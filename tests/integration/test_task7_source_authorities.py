from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import stock_monitor.ledger as ledger_module
import stock_monitor.risk as risk_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import Journal, is_verified_journal_replay_source
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    is_verified_actual_ledger_state,
    replay_actual,
)
from stock_monitor.risk import RiskBlock, SessionCalendarResolver
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


class Task7SourceAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.journal = Journal.open(Path(temporary.name) / "journal.sqlite3")
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

    def build_replay(self):
        buy = self.ingest(
            envelope(
                "message:authority-buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        fee = self.ingest(
            envelope(
                "message:authority-fee",
                "FEE SPY 0.03 AT 10:16 ET",
                message_time="2026-08-14T10:16:30-04:00",
                received_at="2026-08-14T10:16:31-04:00",
            )
        )
        sale = self.ingest(
            envelope(
                "message:authority-sale",
                "SOLD SPY 1 shares @ 101 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        check = self.ingest(
            envelope(
                "message:authority-check",
                "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
                "unlogged_positions 0 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        cutoff = datetime.fromisoformat("2026-08-17T10:00:00-04:00")
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(query_cutoff=cutoff)
        state = replay_actual(
            source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )
        return buy, fee, sale, check, cutoff, source, state

    def test_settlement_replay_uses_full_source_highwater_and_strategy_only(self) -> None:
        buy, fee, sale, check, cutoff, source, state = self.build_replay()

        settlement = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )

        self.assertTrue(settlement.source_verified)
        self.assertEqual(
            [posting.kind for posting in settlement.postings],
            ["BUY", "FEE", "SALE"],
        )
        self.assertEqual(
            [posting.source_event_id for posting in settlement.postings],
            [
                buy.actions[0].event_id,
                fee.actions[0].event_id,
                sale.actions[0].event_id,
            ],
        )
        self.assertEqual(settlement.settled_cash(cutoff), Decimal("4900.97"))
        self.assertEqual(
            settlement.replay_authority.through_execution_cursor,
            check.actions[0].event_row_id,
        )
        self.assertGreater(
            settlement.replay_authority.through_execution_cursor,
            max(posting.cursor for posting in settlement.postings),
        )
        self.assertEqual(
            settlement.replay_authority.journal_source_digest,
            source.source_digest,
        )
        self.assertFalse(replace(settlement).source_verified)
        with self.assertRaisesRegex(
            RiskBlock,
            "JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED",
        ):
            risk_module._issue_settlement_replay(
                replace(source),
                state,
                self.calendar,
            )

        mutated_posting = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        object.__setattr__(
            mutated_posting.postings[0],
            "amount",
            Decimal("999"),
        )
        self.assertFalse(mutated_posting.source_verified)

        mutated_authority = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        object.__setattr__(
            mutated_authority.replay_authority,
            "journal_source_digest",
            "0" * 64,
        )
        self.assertFalse(mutated_authority.source_verified)

        malformed_nested = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        object.__setattr__(malformed_nested, "postings", (object(),))
        self.assertFalse(malformed_nested.source_verified)

    def test_actual_projection_cohort_separates_projection_and_source_terminals(self) -> None:
        self.assertFalse(hasattr(ledger_module, "_register_actual_projection_cohort"))
        buy, _fee, sale, check, _cutoff, source, state = self.build_replay()

        cohort = ledger_module._issue_actual_projection_from_journal(source, state)

        self.assertTrue(ledger_module.is_issued_actual_projection_cohort(cohort))
        self.assertEqual(cohort.projection_start_cursor, buy.actions[0].event_row_id)
        self.assertEqual(cohort.projection_terminal_cursor, sale.actions[0].event_row_id)
        self.assertEqual(cohort.source_through_cursor, check.actions[0].event_row_id)
        self.assertGreater(
            cohort.source_through_cursor,
            cohort.projection_terminal_cursor,
        )
        self.assertEqual(len(cohort.positions), 1)
        self.assertEqual(cohort.positions[0].symbol, "SPY")
        self.assertEqual(cohort.positions[0].shares, 1)
        self.assertEqual(cohort.journal_source_digest, source.source_digest)
        self.assertEqual(cohort.actual_state_digest, state.source_digest)
        self.assertFalse(
            ledger_module.is_issued_actual_projection_cohort(replace(cohort))
        )
        with self.assertRaisesRegex(RiskBlock, "ACTUAL_LEDGER_STATE_UNVERIFIED"):
            ledger_module._issue_actual_projection_from_journal(
                source,
                replace(state),
            )

        mutated = ledger_module._issue_actual_projection_from_journal(
            source,
            state,
        )
        object.__setattr__(
            mutated.positions[0],
            "reason_codes",
            ("TAMPERED",),
        )
        self.assertFalse(ledger_module.is_issued_actual_projection_cohort(mutated))

        malformed_nested = replace(cohort, positions=(object(),))
        self.assertFalse(
            ledger_module.is_issued_actual_projection_cohort(malformed_nested)
        )

    def test_dirty_commit_revokes_replay_state_and_derived_authorities(self) -> None:
        _buy, _fee, _sale, _check, _cutoff, source, state = self.build_replay()
        settlement = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        cohort = ledger_module._issue_actual_projection_from_journal(source, state)
        exact_retry = envelope(
            "message:authority-check",
            "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
            "unlogged_positions 0 AT 15:31 ET",
            message_time="2026-08-14T15:31:30-04:00",
            received_at="2026-08-14T15:31:31-04:00",
        )

        with self.journal.transaction():
            self.assertEqual(
                (
                    is_verified_journal_replay_source(source),
                    is_verified_actual_ledger_state(state),
                    settlement.source_verified,
                    ledger_module.is_issued_actual_projection_cohort(cohort),
                ),
                (True, True, True, True),
            )
        self.ingest(exact_retry)
        self.assertEqual(
            (
                is_verified_journal_replay_source(source),
                is_verified_actual_ledger_state(state),
                settlement.source_verified,
                ledger_module.is_issued_actual_projection_cohort(cohort),
            ),
            (True, True, True, True),
        )
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.journal.transaction() as transaction:
                transaction.append_raw_message(
                    "message:rolled-back-replay",
                    datetime.fromisoformat("2026-08-14T15:31:45-04:00"),
                    "SKIPPED QQQ",
                )
                self.assertEqual(
                    (
                        is_verified_journal_replay_source(source),
                        is_verified_actual_ledger_state(state),
                        settlement.source_verified,
                        ledger_module.is_issued_actual_projection_cohort(cohort),
                    ),
                    (False, False, False, False),
                )
                raise RuntimeError("rollback")
        self.assertTrue(is_verified_journal_replay_source(source))
        self.assertTrue(is_verified_actual_ledger_state(state))
        self.assertTrue(settlement.source_verified)
        self.assertTrue(ledger_module.is_issued_actual_projection_cohort(cohort))

        self.ingest(
            envelope(
                "message:new-replay-generation",
                "SKIPPED QQQ",
                message_time="2026-08-14T15:32:00-04:00",
                received_at="2026-08-14T15:32:01-04:00",
            )
        )

        self.assertEqual(
            (
                is_verified_journal_replay_source(source),
                is_verified_actual_ledger_state(state),
                settlement.source_verified,
                ledger_module.is_issued_actual_projection_cohort(cohort),
            ),
            (False, False, False, False),
        )

    def test_second_connection_commit_revokes_replay_and_derived_authorities(
        self,
    ) -> None:
        _buy, _fee, _sale, _check, _cutoff, source, state = self.build_replay()
        settlement = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        cohort = ledger_module._issue_actual_projection_from_journal(source, state)
        second = Journal.open(self.journal.path)
        self.addCleanup(second.close)

        ingest_confirmation(
            second,
            envelope(
                "message:second-connection-generation",
                "SKIPPED QQQ",
                message_time="2026-08-14T15:32:00-04:00",
                received_at="2026-08-14T15:32:01-04:00",
            ),
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
            entry_authorities=self.entry_authorities,
        )

        self.assertEqual(
            (
                is_verified_journal_replay_source(source),
                is_verified_actual_ledger_state(state),
                settlement.source_verified,
                ledger_module.is_issued_actual_projection_cohort(cohort),
            ),
            (False, False, False, False),
        )

    def test_account_evidence_only_source_issues_empty_strategy_authorities(self) -> None:
        self.ingest(
            envelope(
                "message:authority-unrelated-position",
                "RECONCILE UNRELATED POSITION QQQ +2 shares @ 100 AT 10:13 ET",
                message_time="2026-08-14T10:13:30-04:00",
                received_at="2026-08-14T10:13:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:authority-account-fee",
                "FEE SPY 0.03 AT 10:14 ET",
                message_time="2026-08-14T10:14:30-04:00",
                received_at="2026-08-14T10:14:31-04:00",
            )
        )
        check = self.ingest(
            envelope(
                "message:authority-account-only-check",
                "ACCOUNT CHECK settled_cash 4321 pending_orders 0 "
                "unlogged_positions 0 AT 10:15 ET",
                message_time="2026-08-14T10:15:30-04:00",
                received_at="2026-08-14T10:15:31-04:00",
            )
        )
        cutoff = datetime.fromisoformat("2026-08-14T10:16:00-04:00")
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(query_cutoff=cutoff)
        state = replay_actual(
            source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )

        settlement = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )
        cohort = ledger_module._issue_actual_projection_from_journal(source, state)

        self.assertEqual(settlement.postings, ())
        self.assertEqual(settlement.settled_cash(cutoff), Decimal("5000"))
        self.assertEqual(
            settlement.replay_authority.through_execution_cursor,
            check.actions[0].event_row_id,
        )
        self.assertEqual(
            [(position.symbol, position.lineage_kind) for position in state.positions],
            [("QQQ", "UNRELATED_POSITION")],
        )
        self.assertEqual(cohort.positions, ())
        self.assertIsNone(cohort.projection_start_cursor)
        self.assertIsNone(cohort.projection_terminal_cursor)
        self.assertEqual(
            cohort.source_through_cursor,
            check.actions[0].event_row_id,
        )

    def test_descriptive_settlement_preserves_off_session_strategy_truth(self) -> None:
        self.ingest(
            envelope(
                "message:authority-off-session-buy",
                "BOUGHT SPY 1 shares @ 100 AT 09:00 ET",
                message_time="2026-08-14T09:00:30-04:00",
                received_at="2026-08-14T09:00:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:authority-off-session-fee",
                "FEE SPY 0.03 AT 09:01 ET",
                message_time="2026-08-14T09:01:30-04:00",
                received_at="2026-08-14T09:01:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:authority-off-session-sale",
                "SOLD SPY 1 shares @ 101 AT 09:02 ET",
                message_time="2026-08-14T09:02:30-04:00",
                received_at="2026-08-14T09:02:31-04:00",
            )
        )
        cutoff = datetime.fromisoformat("2026-08-17T10:00:00-04:00")
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(query_cutoff=cutoff)
        state = replay_actual(
            source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )

        settlement = risk_module._issue_settlement_replay(
            source,
            state,
            self.calendar,
        )

        self.assertEqual(
            [posting.kind for posting in settlement.postings],
            ["BUY", "FEE", "SALE"],
        )
        self.assertEqual(settlement.settled_cash(cutoff), Decimal("5000.97"))

    def test_stale_source_and_diagnostic_state_cannot_issue_authority(self) -> None:
        _buy, _fee, _sale, _check, cutoff, source, state = self.build_replay()
        self.ingest(
            envelope(
                "message:authority-later-check",
                "ACCOUNT CHECK settled_cash 4900.97 pending_orders 0 "
                "unlogged_positions 0 AT 15:32 ET",
                message_time="2026-08-14T15:32:30-04:00",
                received_at="2026-08-14T15:32:31-04:00",
            )
        )
        later_cutoff = datetime.fromisoformat("2026-08-17T10:01:00-04:00")
        with self.journal.transaction() as transaction:
            later_source = transaction.read_actual_replay(query_cutoff=later_cutoff)
        later_state = replay_actual(
            later_source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED",
        ):
            risk_module._issue_settlement_replay(
                source,
                later_state,
                self.calendar,
            )
        with self.assertRaisesRegex(
            RiskBlock,
            "JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED",
        ):
            ledger_module._issue_actual_projection_from_journal(
                source,
                later_state,
            )

        diagnostic = SessionCalendarResolver.for_diagnostics(
            self.calendar.calendars
        )
        diagnostic_state = replay_actual(
            later_source,
            plans=self.plans,
            calendar=diagnostic,
            policy=policy_fixture(),
        )
        with self.assertRaisesRegex(RiskBlock, "ACTUAL_LEDGER_STATE_UNVERIFIED"):
            risk_module._issue_settlement_replay(
                later_source,
                diagnostic_state,
                self.calendar,
            )
        self.assertEqual(state.query_cutoff, cutoff)

    def test_identical_streams_from_different_journals_cannot_splice(self) -> None:
        items = (
            envelope(
                "message:identical-journal-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            ),
            envelope(
                "message:identical-journal-check",
                "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
                "unlogged_positions 0 AT 10:16 ET",
                message_time="2026-08-14T10:16:30-04:00",
                received_at="2026-08-14T10:16:31-04:00",
            ),
        )
        for item in items:
            self.ingest(item)
        cutoff = datetime.fromisoformat("2026-08-14T10:17:00-04:00")
        with self.journal.transaction() as transaction:
            source_a = transaction.read_actual_replay(query_cutoff=cutoff)
        state_a = replay_actual(
            source_a,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )

        with tempfile.TemporaryDirectory() as directory:
            journal_b = Journal.open(Path(directory) / "journal.sqlite3")
            self.addCleanup(journal_b.close)
            for item in items:
                ingest_confirmation(
                    journal_b,
                    item,
                    plans=self.plans,
                    calendar=self.calendar,
                    policy=policy_fixture(),
                    entry_authorities=self.entry_authorities,
                )
            with journal_b.transaction() as transaction:
                source_b = transaction.read_actual_replay(query_cutoff=cutoff)
            state_b = replay_actual(
                source_b,
                plans=self.plans,
                calendar=self.calendar,
                policy=policy_fixture(),
            )

            self.assertEqual(source_a.source_digest, source_b.source_digest)
            self.assertEqual(state_a.source_digest, state_b.source_digest)
            with self.assertRaisesRegex(
                RiskBlock,
                "ACTUAL_REPLAY_COHORT_MISMATCH",
            ):
                risk_module._issue_settlement_replay(
                    source_a,
                    state_b,
                    self.calendar,
                )
            with self.assertRaisesRegex(
                RiskBlock,
                "ACTUAL_REPLAY_COHORT_MISMATCH",
            ):
                ledger_module._issue_actual_projection_from_journal(
                    source_a,
                    state_b,
                )

    def test_task7_breaker_and_portfolio_entry_authority_remain_fail_closed(self) -> None:
        _buy, _fee, _sale, _check, _cutoff, source, state = self.build_replay()

        with self.assertRaisesRegex(
            RiskBlock,
            "ACTUAL_BREAKER_SOURCE_UNAVAILABLE",
        ):
            risk_module._issue_actual_entry_authorities(
                source=source,
                state=state,
                calendar_resolver=self.calendar,
            )


if __name__ == "__main__":
    unittest.main()
