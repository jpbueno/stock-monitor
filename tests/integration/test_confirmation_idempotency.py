from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

from stock_monitor.confirmations import (
    ConfirmationEnvelope,
    ConfirmationKind,
    ConfirmationParseError,
)
from stock_monitor.journal import (
    IdempotencyConflict,
    Journal,
    JournalTransaction,
    MigrationCorruption,
)
from stock_monitor.ledger import LedgerSignal
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.reconciliation import (
    ActionStatus,
    ResolvedSignalPlan,
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
)
from stock_monitor.risk import SessionCalendarResolver
from tests.support import policy_fixture


ROOT = Path(__file__).resolve().parents[2]
SESSION = date(2026, 8, 14)


def envelope(
    *,
    message_id: str = "message:1",
    text: str = "SKIPPED SPY",
    message_time: str = "2026-08-14T10:20:00-04:00",
    received_at: str = "2026-08-14T10:20:01-04:00",
) -> ConfirmationEnvelope:
    return ConfirmationEnvelope(
        message_id=message_id,
        message_time=datetime.fromisoformat(message_time),
        received_at=datetime.fromisoformat(received_at),
        text=text,
        session_date=SESSION,
    )


class ConfirmationIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "journal.sqlite3"
        self.journal = Journal.open(self.path)
        self.addCleanup(self.journal.close)
        self.calendar = SessionCalendarResolver(
            (load_current_market_calendar(ROOT, as_of=SESSION),)
        )
        self.plans = UnavailableSignalPlanResolver()
        self.entry_authorities = UnavailableActualEntryAuthorityResolver()

    def ingest(self, item: ConfirmationEnvelope):
        return ingest_confirmation(
            self.journal,
            item,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
            entry_authorities=self.entry_authorities,
        )

    def test_lf_batch_gets_stable_zero_based_ordinals_and_typed_readback(self) -> None:
        item = envelope(
            text=(
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:10 ET\nSKIPPED SPY"
            )
        )
        result = self.ingest(item)

        self.assertFalse(result.duplicate)
        self.assertEqual(tuple(action.ordinal for action in result.actions), (0, 1))
        self.assertEqual(
            tuple(action.kind for action in result.actions),
            (ConfirmationKind.ACCOUNT_CHECK, ConfirmationKind.SKIPPED),
        )
        self.assertEqual(self.journal.count("raw_messages"), 1)
        self.assertEqual(self.journal.count("execution_events"), 2)
        self.assertEqual(self.journal.count("outbox"), 2)
        with self.journal.transaction() as transaction:
            stored = transaction.read_confirmation_result(message_id=item.message_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.message_id, item.message_id)
        self.assertEqual(stored.received_at, item.received_at)
        self.assertEqual(
            tuple(action.event_row_id for action in stored.actions),
            tuple(action.event_row_id for action in result.actions),
        )

    def test_exact_retry_is_noop_and_returns_stored_identity(self) -> None:
        item = envelope()
        first = self.ingest(item)
        before = {
            table: self.journal.count(table)
            for table in ("raw_messages", "execution_events", "outbox")
        }

        second = self.ingest(item)

        self.assertTrue(second.duplicate)
        self.assertEqual(second.raw_row_id, first.raw_row_id)
        self.assertEqual(second.state_digest, first.state_digest)
        self.assertEqual(second.actions, first.actions)
        self.assertEqual(
            before,
            {
                table: self.journal.count(table)
                for table in ("raw_messages", "execution_events", "outbox")
            },
        )

    def test_maximum_supported_batch_keeps_next_ingest_well_below_writer_timeout(
        self,
    ) -> None:
        maximum = envelope(
            message_id="message:max-batch",
            text="\n".join("SKIPPED SPY" for _ in range(64)),
        )
        result = self.ingest(maximum)
        self.assertEqual(len(result.actions), 64)

        started = perf_counter()
        self.ingest(
            envelope(
                message_id="message:after-max-batch",
                text="SKIPPED QQQ",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        elapsed = perf_counter() - started

        self.assertLess(elapsed, 2.0)

    def test_oversized_batch_fails_before_any_journal_write(self) -> None:
        with self.assertRaisesRegex(
            ConfirmationParseError,
            "CONFIRMATION_BATCH_TOO_LARGE",
        ):
            envelope(
                message_id="message:oversized-batch",
                text="\n".join("SKIPPED SPY" for _ in range(65)),
            )

        self.assertEqual(self.journal.count("raw_messages"), 0)
        self.assertEqual(self.journal.count("execution_events"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_extra_execution_destination_corrupts_confirmation_readback(self) -> None:
        item = envelope()
        first = self.ingest(item)
        self.journal.append_outbox(
            idempotency_key="unrelated:destination:v1",
            origin_report_id=None,
            origin_execution_event_id=first.actions[0].event_row_id,
            destination="AUDIT_COPY",
            payload_text="unrelated",
            created_at=item.received_at,
        )

        with self.assertRaisesRegex(
            MigrationCorruption,
            "CONFIRMATION_ACKNOWLEDGEMENT_COUNT_INVALID",
        ):
            self.ingest(item)
        self.assertEqual(self.journal.count("outbox"), 2)

    def test_duplicate_with_changed_destination_conflicts(self) -> None:
        item = envelope()
        self.ingest(item)

        with self.assertRaises(IdempotencyConflict):
            ingest_confirmation(
                self.journal,
                item,
                plans=self.plans,
                calendar=self.calendar,
                policy=policy_fixture(),
                entry_authorities=self.entry_authorities,
                destination="DIFFERENT_DESTINATION",
            )

    def test_same_stable_id_with_changed_text_message_or_receipt_conflicts(self) -> None:
        self.ingest(envelope())
        conflicts = (
            envelope(text="SKIPPED QQQ"),
            envelope(message_time="2026-08-14T10:20:02-04:00", received_at="2026-08-14T10:20:03-04:00"),
            envelope(received_at="2026-08-14T10:20:02-04:00"),
        )
        for changed in conflicts:
            with self.subTest(changed=changed), self.assertRaises(IdempotencyConflict):
                self.ingest(changed)

    def test_same_text_with_different_ids_is_distinct(self) -> None:
        first = self.ingest(envelope(message_id="message:1"))
        second = self.ingest(envelope(message_id="message:2"))

        self.assertNotEqual(first.raw_row_id, second.raw_row_id)
        self.assertNotEqual(first.actions[0].event_id, second.actions[0].event_id)
        self.assertEqual(self.journal.count("raw_messages"), 2)

    def test_ambiguous_line_makes_whole_message_one_pending_event(self) -> None:
        result = self.ingest(envelope(text="SKIPPED SPY\nmaybe BOUGHT QQQ"))

        self.assertEqual(len(result.actions), 1)
        self.assertEqual(result.actions[0].status, ActionStatus.PENDING_CLARIFICATION)
        self.assertEqual(self.journal.count("execution_events"), 1)
        self.assertEqual(self.journal.count("actual_positions"), 0)

    def test_failure_after_event_rolls_back_raw_event_and_outbox(self) -> None:
        original = JournalTransaction.append_outbox

        def fail_after_write(transaction, **kwargs):
            original(transaction, **kwargs)
            raise RuntimeError("injected after outbox")

        with patch.object(JournalTransaction, "append_outbox", fail_after_write):
            with self.assertRaisesRegex(RuntimeError, "injected after outbox"):
                self.ingest(envelope())

        for table in ("raw_messages", "execution_events", "outbox"):
            self.assertEqual(self.journal.count(table), 0)

    def test_issued_plan_alone_cannot_mark_buy_compliant(self) -> None:
        signal = LedgerSignal(
            signal_id="sig-1",
            symbol="SPY",
            role="PRIMARY",
            publication_session=SESSION,
            maximum_entry=Decimal("100.25"),
            recommended_stop=Decimal("97.50"),
            target=Decimal("105.75"),
            planned_shares=5,
            tick_size=Decimal("0.01"),
            trigger_price=Decimal("100.00"),
        )

        class ExactPlanResolver:
            def resolve(
                self,
                *,
                symbol: str,
                economic_at: datetime,
                query_cutoff: datetime,
            ):
                del query_cutoff
                if symbol != "SPY" or economic_at.date() != SESSION:
                    return None
                return ResolvedSignalPlan(
                    signal=signal,
                    report_id="report:test",
                    publication_rank=1,
                    publication_source_digest="a" * 64,
                )

        self.plans = ExactPlanResolver()
        item = envelope(
            text=(
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET; "
                "BID 100.24 ASK 100.25; STOP SET @ 97.50"
            )
        )
        with patch(
            "stock_monitor.reconciliation.is_issued_ledger_signal",
            return_value=True,
        ):
            result = self.ingest(item)

        self.assertEqual(
            result.actions[0].status,
            ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        )
        self.assertIn(
            "AUTHORITY_CONTEXT_UNVERIFIED",
            result.actions[0].reason_codes,
        )


if __name__ == "__main__":
    unittest.main()
