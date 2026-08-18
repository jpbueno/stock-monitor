from __future__ import annotations

import tempfile
import unittest
import hashlib
import json
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import (
    Journal,
    JournalError,
    InvalidJournalValue,
    MigrationCorruption,
    is_verified_journal_action_source,
    is_verified_journal_replay_source,
    is_verified_journal_window_source,
)
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
)
from stock_monitor.risk import SessionCalendarResolver
from tests.support import policy_fixture


ROOT = Path(__file__).resolve().parents[2]
SESSION = date(2026, 8, 14)


def envelope(
    message_id: str,
    text: str,
    *,
    message_time: str,
    received_at: str,
) -> ConfirmationEnvelope:
    return ConfirmationEnvelope(
        message_id=message_id,
        text=text,
        message_time=datetime.fromisoformat(message_time),
        received_at=datetime.fromisoformat(received_at),
        session_date=SESSION,
    )


class JournalConfirmationSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "journal.sqlite3"
        self.journal = Journal.open(self.path)
        self.addCleanup(self.journal.close)
        self.calendar = SessionCalendarResolver(
            (load_current_market_calendar(ROOT, as_of=SESSION),)
        )

    def ingest(self, item: ConfirmationEnvelope):
        return ingest_confirmation(
            self.journal,
            item,
            plans=UnavailableSignalPlanResolver(),
            calendar=self.calendar,
            policy=policy_fixture(),
            entry_authorities=UnavailableActualEntryAuthorityResolver(),
        )

    def test_action_source_is_exact_identity_sealed_and_restart_reissues(self) -> None:
        item = envelope(
            "message:buy",
            (
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET; "
                "BID 100.24 ASK 100.25; STOP SET @ 97.50"
            ),
            message_time="2026-08-14T10:15:00-04:00",
            received_at="2026-08-14T10:15:01-04:00",
        )
        result = self.ingest(item)

        with self.journal.transaction() as transaction:
            source = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id
            )

        self.assertTrue(is_verified_journal_action_source(source))
        self.assertEqual(source.message_id, item.message_id)
        self.assertEqual(source.action_ordinal, 0)
        self.assertEqual(source.raw_text, item.text)
        self.assertEqual(source.received_at, item.received_at)
        self.assertEqual(source.parent_order_id, None)
        self.assertFalse(is_verified_journal_action_source(replace(source)))

        self.journal.close()
        self.journal = Journal.open(self.path)
        with self.journal.transaction() as transaction:
            restarted = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id
            )
        self.assertTrue(is_verified_journal_action_source(restarted))
        self.assertIsNot(source, restarted)
        self.assertEqual(source.source_digest, restarted.source_digest)
        object.__setattr__(source, "symbol", "QQQ")
        self.assertFalse(is_verified_journal_action_source(source))

        import stock_monitor.journal as journal_module

        for name in (
            "_register_journal_source",
            "_register_verified_action_source",
            "_register_verified_window_source",
            "_register_verified_replay_source",
        ):
            self.assertFalse(hasattr(journal_module, name))
        object.__setattr__(
            restarted.row_references[0],
            "row_digest",
            "0" * 64,
        )
        self.assertFalse(is_verified_journal_action_source(restarted))

    def test_closing_journal_revokes_issued_source_authorities(self) -> None:
        result = self.ingest(
            envelope(
                "message:close-revokes",
                "SKIPPED SPY",
                message_time="2026-08-14T10:20:00-04:00",
                received_at="2026-08-14T10:20:01-04:00",
            )
        )
        with self.journal.transaction() as transaction:
            action = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id
            )
            replay = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat(
                    "2026-08-14T10:21:00-04:00"
                )
            )
        self.assertTrue(is_verified_journal_action_source(action))
        self.assertTrue(is_verified_journal_replay_source(replay))

        self.journal.close()

        self.assertFalse(is_verified_journal_action_source(action))
        self.assertFalse(is_verified_journal_replay_source(replay))

    def test_dirty_transaction_cannot_issue_source_before_commit(self) -> None:
        with self.journal.transaction() as transaction:
            raw_id, _ = transaction.append_raw_message(
                "message:dirty",
                datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                "SKIPPED SPY",
            )
            event_id, _ = transaction.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                compliance_result="COMPLIANT",
                reconciliation_state="CLEAR",
                details={
                    "version": 1,
                    "domain_kind": "SKIPPED",
                    "event_time_basis": "MESSAGE_TIME_OBSERVATION",
                    "missing_fields": [],
                    "normalized": {},
                    "reason_codes": [],
                    "source": {
                        "received_at": "2026-08-14T14:20:01.000000Z",
                        "type": "ROBINHOOD_MANUAL_CONFIRMATION",
                    },
                },
            )
            with self.assertRaisesRegex(JournalError, "post-commit"):
                transaction.read_action_source(execution_event_id=event_id)

    def test_source_read_seals_transaction_against_later_write(self) -> None:
        result = self.ingest(
            envelope(
                "message:sealed",
                "SKIPPED SPY",
                message_time="2026-08-14T10:20:00-04:00",
                received_at="2026-08-14T10:20:01-04:00",
            )
        )
        with self.journal.transaction() as transaction:
            transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id
            )
            with self.assertRaisesRegex(JournalError, "sealed read-only"):
                transaction.append_raw_message(
                    "message:forbidden",
                    datetime.fromisoformat("2026-08-14T10:21:00-04:00"),
                    "SKIPPED QQQ",
                )

    def test_incomplete_batch_or_missing_acknowledgement_cannot_issue(self) -> None:
        message_id = "message:incomplete"
        destination = "CODEX_TASK"
        from stock_monitor.domain import stable_execution_event_identity

        stable_event_id, _ = stable_execution_event_identity(message_id, 0)
        acknowledgement_key = "confirmation:" + hashlib.sha256(
            b"stock-monitor/confirmation-outbox/v1\x00"
            + stable_event_id.encode("utf-8")
            + b"\x00"
            + destination.encode("utf-8")
        ).hexdigest()
        with self.journal.transaction() as transaction:
            raw_id, _ = transaction.append_raw_message(
                message_id,
                datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                "SKIPPED SPY\nSKIPPED QQQ",
            )
            event_id, _ = transaction.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                symbol="SPY",
                event_time=datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                compliance_result="COMPLIANT",
                reconciliation_state="CLEAR",
                details={
                    "version": 1,
                    "acknowledgement": {
                        "destination": destination,
                        "idempotency_key": acknowledgement_key,
                    },
                    "domain_kind": "SKIPPED",
                    "event_time_basis": "MESSAGE_TIME_OBSERVATION",
                    "missing_fields": [],
                    "normalized": {
                        "asset_id": None,
                        "delta": None,
                        "fill_group_planned_shares": None,
                        "occ_symbol": None,
                        "open_interest": None,
                        "parent_order_id": None,
                        "pending_orders": None,
                        "reason_sha256": None,
                        "signed_shares": None,
                        "unlogged_positions": None,
                        "volume": None,
                    },
                    "reason_codes": [],
                    "source": {
                        "received_at": "2026-08-14T14:20:01.000000Z",
                        "type": "ROBINHOOD_MANUAL_CONFIRMATION",
                    },
                },
            )
            payload = json.dumps(
                {
                    "event_id": stable_event_id,
                    "kind": "SKIPPED",
                    "ordinal": 0,
                    "reason_codes": [],
                    "status": "COMPLIANT",
                    "version": 1,
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            transaction.append_outbox(
                idempotency_key=acknowledgement_key,
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination=destination,
                payload_text=payload,
                created_at=datetime.fromisoformat(
                    "2026-08-14T10:20:01-04:00"
                ),
            )

        with self.assertRaisesRegex(MigrationCorruption, "ordinals are incomplete"):
            with self.journal.transaction() as transaction:
                transaction.read_confirmation_result(message_id=message_id)
        with self.assertRaisesRegex(MigrationCorruption, "ordinals are incomplete"):
            with self.journal.transaction() as transaction:
                transaction.read_action_source(execution_event_id=event_id)
        with self.assertRaisesRegex(MigrationCorruption, "ordinals are incomplete"):
            with self.journal.transaction() as transaction:
                transaction.read_actual_replay(
                    query_cutoff=datetime.fromisoformat(
                        "2026-08-14T10:21:00-04:00"
                    )
                )

    def test_window_binds_every_cursor_strictly_between_exact_endpoints(self) -> None:
        check = self.ingest(
            envelope(
                "message:check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:10:01-04:00",
            )
        )
        between = self.ingest(
            envelope(
                "message:between",
                "RECONCILE PENDING ORDERS 1 AT 10:11 ET",
                message_time="2026-08-14T10:11:00-04:00",
                received_at="2026-08-14T10:11:01-04:00",
            )
        )
        buy = self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )

        with self.journal.transaction() as transaction:
            window = transaction.read_account_check_window(
                account_check_event_id=check.actions[0].event_row_id,
                terminal_event_id=buy.actions[0].event_row_id,
            )

        self.assertTrue(is_verified_journal_window_source(window))
        self.assertEqual(window.after_cursor, check.actions[0].event_row_id)
        self.assertEqual(window.through_cursor, buy.actions[0].event_row_id)
        self.assertEqual(
            tuple(action.execution_event_id for action in window.between_actions),
            (between.actions[0].event_row_id,),
        )
        self.assertEqual(window.expected_between_count, 1)
        self.assertFalse(is_verified_journal_window_source(replace(window)))

    def test_replay_binds_exact_rows_known_by_cutoff(self) -> None:
        first = self.ingest(
            envelope(
                "message:first",
                "SKIPPED SPY",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:10:01-04:00",
            )
        )
        second = self.ingest(
            envelope(
                "message:second",
                "SKIPPED QQQ",
                message_time="2026-08-14T10:11:00-04:00",
                received_at="2026-08-14T10:11:01-04:00",
            )
        )
        with self.journal.transaction() as transaction:
            replay = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat("2026-08-14T10:10:30-04:00")
            )
        self.assertTrue(is_verified_journal_replay_source(replay))
        self.assertEqual(
            tuple(action.execution_event_id for action in replay.actions),
            (first.actions[0].event_row_id,),
        )
        self.assertEqual(replay.source_through_cursor, second.actions[0].event_row_id)
        self.assertFalse(is_verified_journal_replay_source(replace(replay)))

    def test_later_cursor_cannot_regress_message_or_receipt_knowledge_time(self) -> None:
        self.ingest(
            envelope(
                "message:known-first",
                "SKIPPED SPY",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:15:00-04:00",
            )
        )
        regressed = envelope(
            "message:regressed",
            "SKIPPED QQQ",
            message_time="2026-08-14T10:11:00-04:00",
            received_at="2026-08-14T10:12:00-04:00",
        )
        with self.assertRaisesRegex(
            InvalidJournalValue,
            "CONFIRMATION_RECEIPT_TIME_REGRESSION",
        ):
            self.ingest(regressed)
        self.assertEqual(self.journal.count("raw_messages"), 1)

    def test_replay_rejects_orphan_raw_confirmation_source(self) -> None:
        with self.journal.transaction() as transaction:
            transaction.append_raw_message(
                "message:orphan",
                datetime.fromisoformat("2026-08-14T10:15:00-04:00"),
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
            )

        with self.assertRaisesRegex(
            MigrationCorruption,
            "INCOMPLETE_CONFIRMATION_RAW_SOURCE",
        ):
            with self.journal.transaction() as transaction:
                transaction.read_actual_replay(
                    query_cutoff=datetime.fromisoformat(
                        "2026-08-14T10:16:00-04:00"
                    )
                )

    def test_account_window_rejects_late_historical_check_endpoint(self) -> None:
        self.ingest(
            envelope(
                "message:effective-check",
                "ACCOUNT CHECK settled_cash 4000 pending_orders 0 unlogged_positions 0 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        late = self.ingest(
            envelope(
                "message:historical-check",
                "ACCOUNT CHECK settled_cash 4321 pending_orders 0 unlogged_positions 0 "
                "AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        buy = self.ingest(
            envelope(
                "message:after-check-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:30 ET",
                message_time="2026-08-14T10:31:00-04:00",
                received_at="2026-08-14T10:31:01-04:00",
            )
        )
        with self.assertRaisesRegex(
            InvalidJournalValue,
            "ACCOUNT_CHECK_EFFECTIVE_ORDER_AMBIGUOUS",
        ):
            with self.journal.transaction() as transaction:
                transaction.read_account_check_window(
                    account_check_event_id=late.actions[0].event_row_id,
                    terminal_event_id=buy.actions[0].event_row_id,
                )

    def test_account_window_rejects_late_received_preterminal_check_fact(self) -> None:
        first = self.ingest(
            envelope(
                "message:window-first-check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:30-04:00",
                received_at="2026-08-14T10:10:31-04:00",
            )
        )
        buy = self.ingest(
            envelope(
                "message:window-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; BID 99.99 ASK 100; "
                "STOP SET @ 98",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:window-late-check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 1 "
                "unlogged_positions 0 AT 2026-08-14T10:12:00-04:00",
                message_time="2026-08-14T10:16:00-04:00",
                received_at="2026-08-14T10:16:01-04:00",
            )
        )

        with self.assertRaisesRegex(
            InvalidJournalValue,
            "ACCOUNT_CHECK_LATE_FACT_INVALIDATES_WINDOW",
        ):
            with self.journal.transaction() as transaction:
                transaction.read_account_check_window(
                    account_check_event_id=first.actions[0].event_row_id,
                    terminal_event_id=buy.actions[0].event_row_id,
                )

    def test_account_window_rejects_any_late_received_preterminal_invalidator(self) -> None:
        first = self.ingest(
            envelope(
                "message:window-fee-check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:30-04:00",
                received_at="2026-08-14T10:10:31-04:00",
            )
        )
        buy = self.ingest(
            envelope(
                "message:window-fee-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; BID 99.99 ASK 100; "
                "STOP SET @ 98",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:window-late-fee",
                "FEE SPY 0.03 AT 2026-08-14T10:12:00-04:00",
                message_time="2026-08-14T10:16:00-04:00",
                received_at="2026-08-14T10:16:01-04:00",
            )
        )

        with self.assertRaisesRegex(
            InvalidJournalValue,
            "ACCOUNT_CHECK_LATE_FACT_INVALIDATES_WINDOW",
        ):
            with self.journal.transaction() as transaction:
                transaction.read_account_check_window(
                    account_check_event_id=first.actions[0].event_row_id,
                    terminal_event_id=buy.actions[0].event_row_id,
                )

    def test_account_window_rejects_later_uncertain_account_fact(self) -> None:
        first = self.ingest(
            envelope(
                "message:window-pending-check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:30-04:00",
                received_at="2026-08-14T10:10:31-04:00",
            )
        )
        buy = self.ingest(
            envelope(
                "message:window-pending-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; "
                "BID 99.99 ASK 100; STOP SET @ 98",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:window-late-pending",
                "I may have bought QQQ around 10:12",
                message_time="2026-08-14T10:16:00-04:00",
                received_at="2026-08-14T10:16:01-04:00",
            )
        )

        with self.assertRaisesRegex(
            InvalidJournalValue,
            "ACCOUNT_CHECK_LATE_FACT_INVALIDATES_WINDOW",
        ):
            with self.journal.transaction() as transaction:
                transaction.read_account_check_window(
                    account_check_event_id=first.actions[0].event_row_id,
                    terminal_event_id=buy.actions[0].event_row_id,
                )


if __name__ == "__main__":
    unittest.main()
