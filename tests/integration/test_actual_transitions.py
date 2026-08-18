from __future__ import annotations

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import Journal, JournalTransaction
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from stock_monitor.reconciliation import (
    ActionStatus,
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    is_verified_actual_ledger_state,
    plan_actual_transition,
    replay_actual,
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
    session_date: date = SESSION,
) -> ConfirmationEnvelope:
    return ConfirmationEnvelope(
        message_id=message_id,
        text=text,
        message_time=datetime.fromisoformat(message_time),
        received_at=datetime.fromisoformat(received_at),
        session_date=session_date,
    )


class ActualTransitionPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "journal.sqlite3"
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

    def rows(self, sql: str):
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(sql).fetchall()
        finally:
            connection.close()

    def replay(self, cutoff: str):
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat(cutoff)
            )
        return replay_actual(
            source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )

    def test_off_policy_buy_is_durably_posted_projected_and_reconciled(self) -> None:
        result = self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )

        self.assertEqual(
            result.actions[0].status,
            ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        )
        positions = self.rows(
            "SELECT signal_id, symbol, shares, cost_basis_micros, "
            "last_execution_event_id FROM actual_positions"
        )
        self.assertEqual(len(positions), 1)
        self.assertTrue(positions[0][0].startswith("actual:unplanned:"))
        self.assertEqual(positions[0][1:4], ("SPY", 5, 501_250_000))
        self.assertEqual(positions[0][4], result.actions[0].event_row_id)
        self.assertEqual(
            self.rows(
                "SELECT ledger_name, account_name, entry_kind, amount_micros, "
                "shares_delta, unit_price_micros FROM ledger_postings"
            ),
            [("ACTUAL", "SETTLED_CASH", "BUY", -501_250_000, 5, 100_250_000)],
        )
        reconciliation = self.rows(
            "SELECT reconciliation_required, reason, last_execution_event_id "
            "FROM reconciliation_projection"
        )
        self.assertEqual(reconciliation[0][0], 1)
        self.assertIn("SIGNAL_PLAN_UNAVAILABLE", reconciliation[0][1])
        self.assertEqual(reconciliation[0][2], result.actions[0].event_row_id)
        self.assertEqual(self.journal.count("outbox"), 1)

    def test_later_sale_replays_position_and_consumes_fifo_shares(self) -> None:
        buy = self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        sale = self.ingest(
            envelope(
                "message:sale",
                "SOLD SPY 2 shares @ 101 AT 15:31 ET",
                message_time="2026-08-14T15:32:00-04:00",
                received_at="2026-08-14T15:32:01-04:00",
            )
        )

        self.assertEqual(
            self.rows(
                "SELECT shares, cost_basis_micros, last_execution_event_id "
                "FROM actual_positions"
            ),
            [(3, 300_750_000, sale.actions[0].event_row_id)],
        )
        self.assertEqual(
            self.rows(
                "SELECT entry_kind, amount_micros, shares_delta "
                "FROM ledger_postings ORDER BY id"
            ),
            [("BUY", -501_250_000, 5), ("SALE", 202_000_000, -2)],
        )
        event_lineage = self.rows(
            "SELECT signal_id FROM execution_events ORDER BY id"
        )
        self.assertEqual(event_lineage[0], event_lineage[1])
        self.assertNotEqual(buy.actions[0].event_row_id, sale.actions[0].event_row_id)

    def test_retry_is_noop_across_economic_and_projection_tables(self) -> None:
        item = envelope(
            "message:buy",
            "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
            message_time="2026-08-14T10:15:00-04:00",
            received_at="2026-08-14T10:15:01-04:00",
        )
        first = self.ingest(item)
        tables = (
            "raw_messages",
            "execution_events",
            "account_checks",
            "ledger_postings",
            "actual_positions",
            "actual_cash_projection",
            "reconciliation_projection",
            "outbox",
        )
        before = {table: self.journal.count(table) for table in tables}

        second = self.ingest(item)

        self.assertTrue(second.duplicate)
        self.assertEqual(first.actions, second.actions)
        self.assertEqual(before, {table: self.journal.count(table) for table in tables})

    def test_replay_rejects_missing_or_extra_actual_postings(self) -> None:
        item = envelope(
            "message:buy",
            "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
            message_time="2026-08-14T10:15:00-04:00",
            received_at="2026-08-14T10:15:01-04:00",
        )
        import stock_monitor.reconciliation as reconciliation_module

        original_transition = reconciliation_module._transition_actual

        def without_posting(*args, **kwargs):
            transition = original_transition(*args, **kwargs)
            return replace(transition, posting_intents=())

        with patch.object(
            reconciliation_module,
            "_transition_actual",
            side_effect=without_posting,
        ):
            self.ingest(item)
        with self.assertRaisesRegex(ValueError, "ACTUAL_POSTING_CLOSURE"):
            self.replay("2026-08-14T10:16:00-04:00")

        self.journal.close()
        self.path.unlink()
        self.journal = Journal.open(self.path)
        result = self.ingest(item)
        with self.assertRaisesRegex(ValueError, "commit atomically"):
            with self.journal.transaction() as transaction:
                transaction.append_ledger_posting(
                    posting_key="late-extra",
                    ledger_name="ACTUAL",
                    account_name="SETTLED_CASH",
                    entry_kind="BUY",
                    occurred_at=datetime.fromisoformat(
                        "2026-08-14T10:14:00-04:00"
                    ),
                    amount_micros=-1,
                    execution_event_id=result.actions[0].event_row_id,
                    symbol="SPY",
                    shares_delta=1,
                    unit_price_micros=1,
                    details={
                        "source_event_id": result.actions[0].event_id,
                        "version": 1,
                    },
                )

    def test_cache_match_requires_field_by_field_replay_arithmetic(self) -> None:
        original = JournalTransaction.write_actual_cash_projection

        def corrupt_cash(transaction, **kwargs):
            kwargs["estimated_settled_cash_micros"] = 123
            kwargs["user_confirmed_settled_cash_micros"] = 999_999_999
            kwargs["deployed_capital_micros"] = 777
            kwargs["open_planned_risk_micros"] = 666
            return original(transaction, **kwargs)

        with patch.object(
            JournalTransaction,
            "write_actual_cash_projection",
            new=corrupt_cash,
        ):
            self.ingest(
                envelope(
                    "message:buy",
                    "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
                    message_time="2026-08-14T10:15:00-04:00",
                    received_at="2026-08-14T10:15:01-04:00",
                )
            )
        replayed = self.replay("2026-08-14T10:16:00-04:00")
        self.assertFalse(replayed.cache_matches_replay)

    def test_delayed_economic_event_commits_history_and_leaves_cache_stale(self) -> None:
        self.ingest(
            envelope(
                "message:first",
                "BOUGHT SPY 1 shares @ 100 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:delayed",
                "BOUGHT QQQ 1 shares @ 200 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )

        self.assertEqual(self.journal.count("raw_messages"), 2)
        self.assertEqual(self.journal.count("execution_events"), 2)
        self.assertEqual(self.journal.count("ledger_postings"), 2)
        self.assertEqual(self.journal.count("outbox"), 2)
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat("2026-08-14T10:23:00-04:00")
            )
        self.assertTrue(source.projection_stale)

    def test_low_level_manual_append_rejects_receipt_regression(self) -> None:
        self.ingest(
            envelope(
                "message:first",
                "SKIPPED SPY",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:15:00-04:00",
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "CONFIRMATION_RECEIPT_TIME_REGRESSION",
        ):
            with self.journal.transaction() as transaction:
                raw_id, _ = transaction.append_raw_message(
                    "message:regressed",
                    datetime.fromisoformat("2026-08-14T10:11:00-04:00"),
                    "SKIPPED QQQ",
                )
                transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SKIPPED",
                    symbol="QQQ",
                    event_time=datetime.fromisoformat("2026-08-14T10:11:00-04:00"),
                    compliance_result="COMPLIANT",
                    reconciliation_state="CLEAR",
                    details={
                        "source": {
                            "received_at": "2026-08-14T14:12:00.000000Z",
                            "type": "ROBINHOOD_MANUAL_CONFIRMATION",
                        }
                    },
                )

    def test_full_sale_and_stop_fill_close_position_without_rollback(self) -> None:
        self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:sale",
                "SOLD SPY 2 shares @ 101 AT 15:31 ET",
                message_time="2026-08-14T15:32:00-04:00",
                received_at="2026-08-14T15:32:01-04:00",
            )
        )
        self.assertEqual(
            self.rows("SELECT shares, cost_basis_micros FROM actual_positions"),
            [(0, 0)],
        )
        self.assertEqual(
            self.rows(
                "SELECT reconciliation_required, reason "
                "FROM reconciliation_projection"
            ),
            [(0, None)],
        )
        replayed = self.replay("2026-08-14T15:33:00-04:00")
        self.assertEqual(len(replayed.closed_trades), 1)
        self.assertEqual(replayed.closed_trades[0].pnl_micros, 2_000_000)

    def test_repeated_unplanned_symbol_buys_share_lineage_and_sell_fifo(self) -> None:
        self.ingest(
            envelope(
                "message:buy-1",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:buy-2",
                "BOUGHT SPY 1 shares @ 99 AT 10:16 ET",
                message_time="2026-08-14T10:17:00-04:00",
                received_at="2026-08-14T10:17:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:sale",
                "SOLD SPY 1 shares @ 101 AT 15:31 ET",
                message_time="2026-08-14T15:32:00-04:00",
                received_at="2026-08-14T15:32:01-04:00",
            )
        )
        self.assertEqual(self.journal.count("actual_positions"), 1)
        self.assertEqual(
            self.rows("SELECT shares, cost_basis_micros FROM actual_positions"),
            [(2, 199_000_000)],
        )
        self.assertEqual(
            self.rows(
                "SELECT entry_kind, shares_delta FROM ledger_postings ORDER BY id"
            ),
            [("BUY", 2), ("BUY", 1), ("SALE", -1)],
        )

    def test_stop_update_repairs_missing_stop_then_full_fill_closes(self) -> None:
        self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:stop",
                "STOP UPDATED SPY @ 98 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        reason = self.rows("SELECT reason FROM reconciliation_projection")[0][0]
        self.assertNotIn("CONFIRMED_STOP_MISSING", reason)
        self.assertNotIn("STOP_WIDENING_PROHIBITED", reason)
        self.assertEqual(
            self.rows("SELECT user_confirmed_stop_micros FROM actual_positions"),
            [(98_000_000,)],
        )
        self.ingest(
            envelope(
                "message:fill",
                "STOP FILLED SPY 2 shares @ 97.50 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        self.assertEqual(
            self.rows("SELECT shares, cost_basis_micros FROM actual_positions"),
            [(0, 0)],
        )

    def test_fee_and_account_reconciliation_do_not_mix_strategy_and_user_cash(self) -> None:
        self.ingest(
            envelope(
                "message:check",
                "ACCOUNT CHECK settled_cash 4321 pending_orders 0 unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:10:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:fee",
                "FEE SPY 0.03 AT 15:33 ET",
                message_time="2026-08-14T15:33:30-04:00",
                received_at="2026-08-14T15:33:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:cash",
                "RECONCILE CASH +100 REASON deposit AT 15:34 ET",
                message_time="2026-08-14T15:34:30-04:00",
                received_at="2026-08-14T15:34:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T15:35:00-04:00")
        self.assertEqual(replayed.strategy_settled_cash_micros, 4_899_970_000)
        self.assertEqual(replayed.user_confirmed_cash_micros, 4_421_000_000)
        self.assertEqual(replayed.positions[0].linked_fees_micros, 30_000)
        self.assertEqual(
            self.rows(
                "SELECT entry_kind, amount_micros FROM ledger_postings ORDER BY id"
            ),
            [
                ("ACCOUNT_CHECK", 0),
                ("BUY", -100_000_000),
                ("FEE", -30_000),
                ("RECONCILE_CASH", 100_000_000),
            ],
        )

    def test_sale_settlement_is_t_plus_one_and_never_precedes_receipt(self) -> None:
        self.ingest(
            envelope(
                "message:buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:sale",
                "SOLD SPY 1 shares @ 101 AT 2026-08-14T15:31:00-04:00",
                message_time="2026-08-17T09:36:00-04:00",
                received_at="2026-08-17T09:36:01-04:00",
                session_date=date(2026, 8, 17),
            )
        )

        friday = self.replay("2026-08-14T16:00:00-04:00")
        before_receipt = self.replay("2026-08-17T09:35:30-04:00")
        after_receipt = self.replay("2026-08-17T09:36:01-04:00")
        self.assertEqual(friday.strategy_settled_cash_micros, 4_800_000_000)
        self.assertEqual(before_receipt.strategy_settled_cash_micros, 4_800_000_000)
        self.assertEqual(after_receipt.strategy_settled_cash_micros, 4_901_000_000)
        self.assertEqual(after_receipt.positions[0].shares, 1)

    def test_cross_year_calendar_gap_is_active_then_resolves_without_source_change(self) -> None:
        self.ingest(
            envelope(
                "message:year-buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-12-31T10:15:00-05:00",
                received_at="2026-12-31T10:15:01-05:00",
                session_date=date(2026, 12, 31),
            )
        )
        self.ingest(
            envelope(
                "message:year-sale",
                "SOLD SPY 2 shares @ 101 AT 15:31 ET",
                message_time="2026-12-31T15:32:00-05:00",
                received_at="2026-12-31T15:32:01-05:00",
                session_date=date(2026, 12, 31),
            )
        )
        unresolved = self.replay("2026-12-31T16:00:00-05:00")
        self.assertIn("CALENDAR_COVERAGE_MISSING", unresolved.reconciliation_reasons)
        before_digest = tuple(
            posting.row_reference.row_digest for posting in unresolved.settlement_ledger
        )

        calendar_path = self.path.parent / "2027.json"
        calendar_path.write_bytes(
            (
                ROOT
                / "tests"
                / "fixtures"
                / "reference"
                / "calendar-2027.json"
            ).read_bytes()
        )
        calendar_2027 = MarketCalendar.load(
            calendar_path,
            as_of=date(2027, 1, 4),
        )
        # Task 8/release packaging owns the future pinned digest. This test
        # exercises only the resolver boundary by simulating that completed
        # verification without changing today's release manifest.
        import stock_monitor.market_calendar as calendar_module

        calendar_module._register_calendar_authority(
            calendar_module._RELEASE_CALENDARS,
            calendar_2027,
        )
        expanded = SessionCalendarResolver(
            (
                load_current_market_calendar(ROOT, as_of=date(2026, 8, 14)),
                calendar_2027,
            )
        )
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat("2027-01-04T12:00:00-05:00")
            )
        resolved = replay_actual(
            source,
            plans=self.plans,
            calendar=expanded,
            policy=policy_fixture(),
        )
        self.assertNotIn("CALENDAR_COVERAGE_MISSING", resolved.reconciliation_reasons)
        self.assertEqual(resolved.strategy_settled_cash_micros, 5_002_000_000)
        self.assertEqual(
            before_digest,
            tuple(posting.row_reference.row_digest for posting in resolved.settlement_ledger),
        )

    def test_closed_trade_provenance_includes_every_lifecycle_event(self) -> None:
        buy = self.ingest(
            envelope(
                "message:prov-buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        partial = self.ingest(
            envelope(
                "message:prov-partial",
                "SOLD SPY 1 shares @ 101 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        fee = self.ingest(
            envelope(
                "message:prov-fee",
                "FEE SPY 0.03 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        final = self.ingest(
            envelope(
                "message:prov-final",
                "SOLD SPY 1 shares @ 102 AT 15:32 ET",
                message_time="2026-08-14T15:32:30-04:00",
                received_at="2026-08-14T15:32:31-04:00",
            )
        )
        replayed = self.replay("2026-08-17T12:00:00-04:00")
        trade = replayed.closed_trades[0]
        self.assertEqual(
            trade.source_event_ids,
            (
                buy.actions[0].event_id,
                partial.actions[0].event_id,
                fee.actions[0].event_id,
                final.actions[0].event_id,
            ),
        )
        self.assertEqual(trade.fees_micros, 30_000)
        self.assertEqual(trade.pnl_micros, 2_970_000)

    def test_delayed_same_symbol_buy_replays_after_restart_with_source_lineage(self) -> None:
        self.ingest(
            envelope(
                "message:late-first",
                "BOUGHT SPY 1 shares @ 100 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:late-earlier",
                "BOUGHT SPY 1 shares @ 99 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        self.journal.close()
        self.journal = Journal.open(self.path)
        replayed = self.replay("2026-08-14T10:23:00-04:00")
        self.assertEqual(len(replayed.positions), 1)
        self.assertEqual(replayed.positions[0].shares, 2)
        self.assertEqual(replayed.positions[0].cost_basis_micros, 199_000_000)

    def test_delayed_sale_before_later_buy_does_not_consume_future_lot(self) -> None:
        self.ingest(
            envelope(
                "message:future-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:earlier-sale",
                "SOLD SPY 1 shares @ 101 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        replayed = self.replay("2026-08-14T10:23:00-04:00")
        self.assertEqual(replayed.positions[0].shares, 1)
        self.assertIn(
            "DELAYED_EXIT_PRECEDES_POSITION",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows("SELECT entry_kind FROM ledger_postings ORDER BY id"),
            [("BUY",)],
        )

    def test_delayed_fee_before_later_buy_is_account_evidence_only(self) -> None:
        buy = self.ingest(
            envelope(
                "message:pre-lot-fee-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        fee = self.ingest(
            envelope(
                "message:pre-lot-fee",
                "FEE SPY 0.03 AT 2026-08-14T10:00:00-04:00",
                message_time="2026-08-14T10:16:00-04:00",
                received_at="2026-08-14T10:16:01-04:00",
            )
        )

        replayed = self.replay("2026-08-14T10:17:00-04:00")

        self.assertEqual(
            self.rows(
                "SELECT signal_id FROM execution_events WHERE id = "
                f"{fee.actions[0].event_row_id}"
            ),
            [(None,)],
        )
        self.assertEqual(replayed.strategy_settled_cash_micros, 4_900_000_000)
        self.assertEqual(replayed.positions[0].linked_fees_micros, 0)
        self.assertEqual(
            replayed.positions[0].lifecycle_event_ids,
            (buy.actions[0].event_id,),
        )
        self.assertIn(
            "DELAYED_FEE_PRECEDES_POSITION",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows(
                "SELECT account_name, entry_kind, amount_micros "
                "FROM ledger_postings ORDER BY id"
            ),
            [
                ("SETTLED_CASH", "BUY", -100_000_000),
                ("ACCOUNT_EVIDENCE", "FEE", -30_000),
            ],
        )
        self.assertFalse(replayed.cache_matches_replay)

    def test_delayed_stop_before_later_buy_cannot_mutate_future_lot(self) -> None:
        buy = self.ingest(
            envelope(
                "message:pre-lot-stop-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        stop = self.ingest(
            envelope(
                "message:pre-lot-stop",
                "STOP UPDATED SPY @ 98 AT 2026-08-14T10:00:00-04:00",
                message_time="2026-08-14T10:16:00-04:00",
                received_at="2026-08-14T10:16:01-04:00",
            )
        )

        replayed = self.replay("2026-08-14T10:17:00-04:00")

        self.assertEqual(
            self.rows(
                "SELECT signal_id FROM execution_events WHERE id = "
                f"{stop.actions[0].event_row_id}"
            ),
            [(None,)],
        )
        self.assertIsNone(replayed.positions[0].user_stop_micros)
        self.assertEqual(
            replayed.positions[0].lifecycle_event_ids,
            (buy.actions[0].event_id,),
        )
        self.assertIn(
            "DELAYED_STOP_PRECEDES_POSITION",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows("SELECT entry_kind FROM ledger_postings ORDER BY id"),
            [("BUY",)],
        )
        self.assertFalse(replayed.cache_matches_replay)

    def test_untracked_stop_update_persists_reconciliation_without_mutation(
        self,
    ) -> None:
        result = self.ingest(
            envelope(
                "message:untracked-stop",
                "STOP UPDATED QQQ @ 90 AT 10:12 ET",
                message_time="2026-08-14T10:12:30-04:00",
                received_at="2026-08-14T10:12:31-04:00",
            )
        )

        self.assertEqual(
            result.actions[0].status,
            ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        )
        self.assertIn("POSITION_NOT_TRACKED", result.actions[0].reason_codes)
        self.assertEqual(self.journal.count("raw_messages"), 1)
        self.assertEqual(self.journal.count("execution_events"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(self.journal.count("actual_positions"), 0)
        self.assertEqual(self.journal.count("ledger_postings"), 0)

        replayed = self.replay("2026-08-14T10:13:00-04:00")
        self.assertEqual(replayed.positions, ())
        self.assertIn("POSITION_NOT_TRACKED", replayed.reconciliation_reasons)

        before_restart_digest = replayed.source_digest
        self.journal.close()
        self.journal = Journal.open(self.path)
        restarted = self.replay("2026-08-14T10:13:00-04:00")
        self.assertEqual(restarted.source_digest, before_restart_digest)
        self.assertEqual(restarted.positions, ())
        self.assertIn("POSITION_NOT_TRACKED", restarted.reconciliation_reasons)

    def test_delayed_unrelated_reduction_cannot_remove_future_lot(self) -> None:
        added = self.ingest(
            envelope(
                "message:pre-lot-unrelated-add",
                "RECONCILE UNRELATED POSITION QQQ +2 shares @ 100 AT 11:01 ET",
                message_time="2026-08-14T11:01:30-04:00",
                received_at="2026-08-14T11:01:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:pre-lot-unrelated-reduce",
                "RECONCILE UNRELATED POSITION QQQ -1 shares @ 100 "
                "AT 2026-08-14T10:00:00-04:00",
                message_time="2026-08-14T11:02:00-04:00",
                received_at="2026-08-14T11:02:01-04:00",
            )
        )

        replayed = self.replay("2026-08-14T11:03:00-04:00")

        self.assertEqual([(item.symbol, item.shares) for item in replayed.positions], [("QQQ", 2)])
        self.assertEqual(
            replayed.positions[0].lifecycle_event_ids,
            (added.actions[0].event_id,),
        )
        self.assertIn(
            "DELAYED_UNRELATED_POSITION_REDUCTION_PRECEDES_POSITION",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows(
                "SELECT account_name, entry_kind, shares_delta "
                "FROM ledger_postings ORDER BY id"
            ),
            [
                ("ACCOUNT_EVIDENCE", "UNRELATED_POSITION", 2),
                ("ACCOUNT_EVIDENCE", "UNRELATED_POSITION", -1),
            ],
        )
        self.assertFalse(replayed.cache_matches_replay)

    def test_delayed_exit_after_closed_lifecycle_keeps_prior_attribution(self) -> None:
        self.ingest(
            envelope(
                "message:attribution-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:00 ET",
                message_time="2026-08-14T10:01:00-04:00",
                received_at="2026-08-14T10:01:01-04:00",
            )
        )
        first_sale = self.ingest(
            envelope(
                "message:attribution-sale",
                "SOLD SPY 1 shares @ 102 AT 15:00 ET",
                message_time="2026-08-14T15:01:00-04:00",
                received_at="2026-08-14T15:01:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:attribution-delayed",
                "SOLD SPY 1 shares @ 103 AT 2026-08-14T14:00:00-04:00",
                message_time="2026-08-14T15:02:00-04:00",
                received_at="2026-08-14T15:02:01-04:00",
            )
        )
        replayed = self.replay("2026-08-14T15:03:00-04:00")
        self.assertIn(
            "DELAYED_EXIT_ATTRIBUTION_UNRESOLVED",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(len(replayed.closed_trades), 1)
        self.assertEqual(
            replayed.closed_trades[0].source_event_ids[-1],
            first_sale.actions[0].event_id,
        )
        self.assertEqual(
            self.rows("SELECT entry_kind FROM ledger_postings ORDER BY id"),
            [("BUY",), ("SALE",)],
        )

    def test_cash_deficit_remains_exact_and_active_without_false_cache(self) -> None:
        self.ingest(
            envelope(
                "message:deficit",
                "BOUGHT SPY 100 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        replayed = self.replay("2026-08-14T10:16:00-04:00")
        self.assertEqual(replayed.strategy_settled_cash_micros, -5_000_000_000)
        self.assertIn("STRATEGY_CASH_DEFICIT", replayed.reconciliation_reasons)
        self.assertFalse(replayed.cache_matches_replay)

    def test_replay_rejects_forked_or_forged_actual_entry_lineage(self) -> None:
        import stock_monitor.reconciliation as reconciliation_module

        original = reconciliation_module._resolve_actual_entry_lineage

        self.ingest(
            envelope(
                "message:lineage-first",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )

        def fork_second(before, action, event_id, planned_signal_id):
            if action.price == Decimal("99"):
                return reconciliation_module._actual_only_lineage_id(
                    action,
                    event_id,
                    None,
                )
            return original(before, action, event_id, planned_signal_id)

        with patch.object(
            reconciliation_module,
            "_resolve_actual_entry_lineage",
            side_effect=fork_second,
        ):
            self.ingest(
                envelope(
                    "message:lineage-fork",
                    "BOUGHT SPY 1 shares @ 99 AT 10:16 ET",
                    message_time="2026-08-14T10:17:00-04:00",
                    received_at="2026-08-14T10:17:01-04:00",
                )
            )

        with self.assertRaisesRegex(ValueError, "ACTUAL_LINEAGE_MISMATCH"):
            self.replay("2026-08-14T10:18:00-04:00")

        self.journal.close()
        self.path.unlink()
        self.journal = Journal.open(self.path)
        with patch.object(
            reconciliation_module,
            "_resolve_actual_entry_lineage",
            return_value=("sig-forged", "PLANNED_SIGNAL"),
        ):
            self.ingest(
                envelope(
                    "message:lineage-forged",
                    "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                    message_time="2026-08-14T10:15:00-04:00",
                    received_at="2026-08-14T10:15:01-04:00",
                )
            )
        with self.assertRaisesRegex(ValueError, "ACTUAL_LINEAGE_MISMATCH"):
            self.replay("2026-08-14T10:16:00-04:00")

    def test_ambiguous_fee_is_account_evidence_not_strategy_pnl(self) -> None:
        self.ingest(
            envelope(
                "message:unattributed-fee",
                "FEE SPY 0.03 AT 15:33 ET",
                message_time="2026-08-14T15:33:30-04:00",
                received_at="2026-08-14T15:33:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T15:34:00-04:00")
        self.assertEqual(replayed.strategy_settled_cash_micros, 5_000_000_000)
        self.assertIn("FEE_LINEAGE_AMBIGUOUS", replayed.reconciliation_reasons)
        self.assertEqual(
            self.rows(
                "SELECT account_name, entry_kind, amount_micros "
                "FROM ledger_postings"
            ),
            [("ACCOUNT_EVIDENCE", "FEE", -30_000)],
        )

    def test_unrelated_position_is_durable_account_exposure_and_can_clear(self) -> None:
        self.ingest(
            envelope(
                "message:unrelated-add",
                "RECONCILE UNRELATED POSITION QQQ +2 shares @ 100 AT 11:01 ET",
                message_time="2026-08-14T11:01:30-04:00",
                received_at="2026-08-14T11:01:31-04:00",
            )
        )
        added = self.replay("2026-08-14T11:02:00-04:00")
        self.assertEqual([(p.symbol, p.shares) for p in added.positions], [("QQQ", 2)])
        self.assertIn(
            "UNRELATED_POSITION_RECONCILIATION",
            added.reconciliation_reasons,
        )
        self.assertEqual(added.strategy_settled_cash_micros, 5_000_000_000)
        self.assertEqual(
            self.rows(
                "SELECT account_name, entry_kind, amount_micros, shares_delta, "
                "unit_price_micros FROM ledger_postings"
            ),
            [("ACCOUNT_EVIDENCE", "UNRELATED_POSITION", 0, 2, 100_000_000)],
        )

        self.ingest(
            envelope(
                "message:unrelated-clear",
                "RECONCILE UNRELATED POSITION QQQ -2 shares @ 100 AT 11:03 ET",
                message_time="2026-08-14T11:03:30-04:00",
                received_at="2026-08-14T11:03:31-04:00",
            )
        )
        cleared = self.replay("2026-08-14T11:04:00-04:00")
        self.assertEqual(cleared.positions, ())
        self.assertNotIn(
            "UNRELATED_POSITION_RECONCILIATION",
            cleared.reconciliation_reasons,
        )
        self.assertEqual(cleared.strategy_settled_cash_micros, 5_000_000_000)
        self.assertTrue(cleared.cache_matches_replay)

    def test_unrelated_position_cannot_become_strategy_fee_or_sale(self) -> None:
        self.ingest(
            envelope(
                "message:unrelated-isolation",
                "RECONCILE UNRELATED POSITION QQQ +2 shares @ 100 AT 11:01 ET",
                message_time="2026-08-14T11:01:30-04:00",
                received_at="2026-08-14T11:01:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:unrelated-fee",
                "FEE QQQ 0.03 AT 11:02 ET",
                message_time="2026-08-14T11:02:30-04:00",
                received_at="2026-08-14T11:02:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:unrelated-sale",
                "SOLD QQQ 2 shares @ 101 AT 11:03 ET",
                message_time="2026-08-14T11:03:30-04:00",
                received_at="2026-08-14T11:03:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T11:04:00-04:00")
        self.assertEqual(replayed.strategy_settled_cash_micros, 5_000_000_000)
        self.assertEqual([(p.symbol, p.shares) for p in replayed.positions], [("QQQ", 2)])
        self.assertIn("FEE_LINEAGE_AMBIGUOUS", replayed.reconciliation_reasons)
        self.assertIn(
            "NON_STRATEGY_POSITION_LINEAGE",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows(
                "SELECT account_name, entry_kind FROM ledger_postings ORDER BY id"
            ),
            [
                ("ACCOUNT_EVIDENCE", "UNRELATED_POSITION"),
                ("ACCOUNT_EVIDENCE", "FEE"),
            ],
        )

    def test_symbol_only_exit_across_multiple_groups_is_durable_unresolved(self) -> None:
        for suffix, price, parent, at in (
            ("one", "100", "rh:g1", "10:14"),
            ("two", "101", "rh:g2", "10:16"),
        ):
            self.ingest(
                envelope(
                    f"message:group-{suffix}",
                    f"PARTIAL FILL SPY 1 shares @ {price} AT {at} ET; "
                    f"ORDER {parent} TOTAL 2 shares",
                    message_time=f"2026-08-14T{at}:30-04:00",
                    received_at=f"2026-08-14T{at}:31-04:00",
                )
            )
        self.ingest(
            envelope(
                "message:group-exit",
                "SOLD SPY 1 shares @ 102 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T15:32:00-04:00")
        self.assertEqual(sum(position.shares for position in replayed.positions), 2)
        self.assertIn(
            "MULTIPLE_POSITION_LINEAGES_UNRESOLVED",
            replayed.reconciliation_reasons,
        )
        self.assertEqual(
            self.rows("SELECT entry_kind FROM ledger_postings ORDER BY id"),
            [("BUY",), ("BUY",)],
        )

    def test_closing_one_of_two_positions_retains_other_reconciliation(self) -> None:
        for symbol, at in (("SPY", "10:14"), ("QQQ", "10:16")):
            self.ingest(
                envelope(
                    f"message:two-{symbol.lower()}",
                    f"BOUGHT {symbol} 1 shares @ 100 AT {at} ET",
                    message_time=f"2026-08-14T{at}:30-04:00",
                    received_at=f"2026-08-14T{at}:31-04:00",
                )
            )
        self.ingest(
            envelope(
                "message:close-qqq",
                "SOLD QQQ 1 shares @ 101 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T15:32:00-04:00")
        self.assertEqual([(p.symbol, p.shares) for p in replayed.positions], [("SPY", 1)])
        self.assertTrue(replayed.reconciliation_reasons)
        self.assertEqual(
            self.rows("SELECT entry_kind FROM ledger_postings ORDER BY id"),
            [("BUY",), ("BUY",), ("SALE",)],
        )

    def test_negative_account_adjustment_is_exact_and_not_cached_as_zero(self) -> None:
        self.ingest(
            envelope(
                "message:small-check",
                "ACCOUNT CHECK settled_cash 50 pending_orders 0 unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:30-04:00",
                received_at="2026-08-14T10:10:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:negative-cash",
                "RECONCILE CASH -100 REASON correction AT 10:11 ET",
                message_time="2026-08-14T10:11:30-04:00",
                received_at="2026-08-14T10:11:31-04:00",
            )
        )
        replayed = self.replay("2026-08-14T10:12:00-04:00")
        self.assertEqual(replayed.user_confirmed_cash_micros, -50_000_000)
        self.assertIn("ACCOUNT_CASH_NEGATIVE", replayed.reconciliation_reasons)
        self.assertFalse(replayed.cache_matches_replay)

    def test_stop_update_is_included_in_closed_lifecycle_provenance(self) -> None:
        buy = self.ingest(
            envelope(
                "message:stop-prov-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        stop = self.ingest(
            envelope(
                "message:stop-prov-update",
                "STOP UPDATED SPY @ 98 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        sale = self.ingest(
            envelope(
                "message:stop-prov-sale",
                "SOLD SPY 1 shares @ 101 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        replayed = self.replay("2026-08-17T12:00:00-04:00")
        self.assertEqual(
            replayed.closed_trades[0].source_event_ids,
            (
                buy.actions[0].event_id,
                stop.actions[0].event_id,
                sale.actions[0].event_id,
            ),
        )

    def test_delayed_account_observation_commits_without_overwriting_newer_fact(self) -> None:
        self.ingest(
            envelope(
                "message:newer-check",
                "ACCOUNT CHECK settled_cash 4000 pending_orders 0 unlogged_positions 0 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:late-check",
                "ACCOUNT CHECK settled_cash 4321 pending_orders 1 unlogged_positions 1 "
                "AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        self.assertEqual(self.journal.count("account_checks"), 2)
        replayed = self.replay("2026-08-14T10:23:00-04:00")
        self.assertEqual(replayed.user_confirmed_cash_micros, 4_000_000_000)
        self.assertNotIn("PENDING_ORDERS_PRESENT", replayed.reconciliation_reasons)
        self.assertNotIn("UNLOGGED_POSITIONS_PRESENT", replayed.reconciliation_reasons)
        self.assertFalse(replayed.cache_matches_replay)

    def test_new_account_baseline_clears_superseded_cash_reasons(self) -> None:
        self.ingest(
            envelope(
                "message:no-baseline-adjustment",
                "RECONCILE CASH +100 REASON deposit AT 10:10 ET",
                message_time="2026-08-14T10:10:30-04:00",
                received_at="2026-08-14T10:10:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:new-baseline",
                "ACCOUNT CHECK settled_cash 50 pending_orders 0 unlogged_positions 0 AT 10:11 ET",
                message_time="2026-08-14T10:11:30-04:00",
                received_at="2026-08-14T10:11:31-04:00",
            )
        )
        baseline = self.replay("2026-08-14T10:12:00-04:00")
        self.assertEqual(baseline.user_confirmed_cash_micros, 50_000_000)
        self.assertNotIn(
            "ACCOUNT_CASH_BASELINE_UNAVAILABLE",
            baseline.reconciliation_reasons,
        )

        self.ingest(
            envelope(
                "message:cash-negative",
                "RECONCILE CASH -100 REASON correction AT 10:12 ET",
                message_time="2026-08-14T10:12:30-04:00",
                received_at="2026-08-14T10:12:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:cash-positive-check",
                "ACCOUNT CHECK settled_cash 75 pending_orders 0 unlogged_positions 0 AT 10:13 ET",
                message_time="2026-08-14T10:13:30-04:00",
                received_at="2026-08-14T10:13:31-04:00",
            )
        )
        resolved = self.replay("2026-08-14T10:14:00-04:00")
        self.assertEqual(resolved.user_confirmed_cash_micros, 75_000_000)
        self.assertNotIn("ACCOUNT_CASH_NEGATIVE", resolved.reconciliation_reasons)

    def test_delayed_stop_observation_cannot_overwrite_newer_control(self) -> None:
        buy = self.ingest(
            envelope(
                "message:stop-order-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        newer = self.ingest(
            envelope(
                "message:stop-newer",
                "STOP UPDATED SPY @ 99 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        stale = self.ingest(
            envelope(
                "message:stop-stale",
                "STOP UPDATED SPY @ 95 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        self.journal.close()
        self.journal = Journal.open(self.path)
        replayed = self.replay("2026-08-14T10:23:00-04:00")
        self.assertEqual(replayed.positions[0].user_stop_micros, 99_000_000)
        self.assertIn("STALE_STOP_OBSERVATION", replayed.reconciliation_reasons)
        self.assertIn(
            "DELAYED_STOP_PRECEDES_POSITION",
            replayed.reconciliation_reasons,
        )
        self.assertNotIn("STOP_WIDENING_PROHIBITED", replayed.reconciliation_reasons)
        self.assertEqual(
            replayed.positions[0].lifecycle_event_ids,
            (
                buy.actions[0].event_id,
                newer.actions[0].event_id,
            ),
        )
        self.assertEqual(
            self.rows(
                "SELECT signal_id FROM execution_events WHERE id = "
                f"{stale.actions[0].event_row_id}"
            ),
            [(None,)],
        )
        self.assertFalse(replayed.cache_matches_replay)

    def test_aggregate_signed_64_overflow_rolls_back_second_action(self) -> None:
        self.ingest(
            envelope(
                "message:huge-first",
                "BOUGHT SPY 1 shares @ 5000000000000 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        tables = (
            "raw_messages",
            "execution_events",
            "ledger_postings",
            "actual_positions",
            "actual_cash_projection",
            "reconciliation_projection",
            "outbox",
        )
        before = {table: self.journal.count(table) for table in tables}

        with self.assertRaisesRegex(ValueError, "ACTUAL_AGGREGATE_OVERFLOW"):
            self.ingest(
                envelope(
                    "message:huge-second",
                    "BOUGHT QQQ 1 shares @ 5000000000000 AT 10:16 ET",
                    message_time="2026-08-14T10:17:00-04:00",
                    received_at="2026-08-14T10:17:01-04:00",
                )
            )

        self.assertEqual(
            before,
            {table: self.journal.count(table) for table in tables},
        )

    def test_replayed_state_binds_exact_journal_policy_and_identity(self) -> None:
        result = self.ingest(
            envelope(
                "message:state-authority",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(
                query_cutoff=datetime.fromisoformat(
                    "2026-08-14T10:16:00-04:00"
                )
            )
        state = replay_actual(
            source,
            plans=self.plans,
            calendar=self.calendar,
            policy=policy_fixture(),
        )
        self.assertEqual(state.journal_source_digest, source.source_digest)
        self.assertEqual(len(state.policy_digest), 64)
        self.assertTrue(is_verified_actual_ledger_state(state))
        copied = replace(state)
        self.assertFalse(is_verified_actual_ledger_state(copied))
        with self.assertRaisesRegex(ValueError, "ACTUAL_LEDGER_STATE_UNVERIFIED"):
            plan_actual_transition(
                copied,
                source.actions[0],
                plans=self.plans,
                calendar=self.calendar,
                policy=policy_fixture(),
            )
        self.assertEqual(result.actions[0].event_row_id, source.terminal_cursor)

    def test_closed_lifecycle_loss_streak_and_realized_high_water_are_derived(self) -> None:
        self.ingest(
            envelope(
                "message:loss-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:loss-sale",
                "SOLD SPY 1 shares @ 90 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        self.assertEqual(
            self.rows(
                "SELECT consecutive_losses, weekly_high_water_micros, "
                "monthly_high_water_micros FROM actual_cash_projection"
            ),
            [(1, 5_000_000_000, 5_000_000_000)],
        )

        self.ingest(
            envelope(
                "message:profit-buy",
                "BOUGHT QQQ 1 shares @ 100 AT 15:31 ET",
                message_time="2026-08-14T15:31:30-04:00",
                received_at="2026-08-14T15:31:31-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:profit-sale",
                "SOLD QQQ 1 shares @ 120 AT 15:32 ET",
                message_time="2026-08-14T15:32:30-04:00",
                received_at="2026-08-14T15:32:31-04:00",
            )
        )
        self.assertEqual(
            self.rows(
                "SELECT consecutive_losses, weekly_high_water_micros, "
                "monthly_high_water_micros FROM actual_cash_projection"
            ),
            [(0, 5_010_000_000, 5_010_000_000)],
        )
        replayed = self.replay("2026-08-14T15:33:00-04:00")
        self.assertTrue(replayed.cache_matches_replay)

    def test_delayed_closes_use_economic_order_for_breaker_metrics(self) -> None:
        self.ingest(
            envelope(
                "message:metric-spy-buy",
                "BOUGHT SPY 1 shares @ 100 AT 09:40 ET",
                message_time="2026-08-14T09:41:00-04:00",
                received_at="2026-08-14T09:41:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:metric-qqq-buy",
                "BOUGHT QQQ 1 shares @ 100 AT 09:50 ET",
                message_time="2026-08-14T09:51:00-04:00",
                received_at="2026-08-14T09:51:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:metric-profit",
                "SOLD SPY 1 shares @ 120 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:metric-delayed-loss",
                "SOLD QQQ 1 shares @ 90 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        stale = self.replay("2026-08-14T10:22:30-04:00")
        self.assertEqual(
            [(trade.symbol, trade.pnl_micros) for trade in stale.closed_trades],
            [("SPY", 20_000_000), ("QQQ", -10_000_000)],
        )
        self.assertFalse(stale.cache_matches_replay)

        self.ingest(
            envelope(
                "message:metric-refresh",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:23 ET",
                message_time="2026-08-14T10:23:30-04:00",
                received_at="2026-08-14T10:23:31-04:00",
            )
        )
        self.assertEqual(
            self.rows(
                "SELECT consecutive_losses, weekly_high_water_micros, "
                "monthly_high_water_micros FROM actual_cash_projection"
            ),
            [(0, 5_010_000_000, 5_010_000_000)],
        )
        refreshed = self.replay("2026-08-14T10:24:00-04:00")
        # The cash/breaker fields are current, but an account observation cannot
        # manufacture the missing QQQ position tombstone for the skipped close.
        self.assertFalse(refreshed.cache_matches_replay)

    def test_delayed_sale_splits_economic_lifecycles_and_survives_restart(
        self,
    ) -> None:
        first_buy = self.ingest(
            envelope(
                "m1",
                "BOUGHT SPY 1 shares @ 100 AT 09:40 ET",
                message_time="2026-08-14T09:41:00-04:00",
                received_at="2026-08-14T09:41:01-04:00",
            )
        )
        second_buy = self.ingest(
            envelope(
                "m2",
                "BOUGHT SPY 1 shares @ 200 AT 11:00 ET",
                message_time="2026-08-14T11:01:00-04:00",
                received_at="2026-08-14T11:01:01-04:00",
            )
        )
        delayed_sale = self.ingest(
            envelope(
                "m3",
                "SOLD SPY 1 shares @ 110 AT 2026-08-14T10:00:00-04:00",
                message_time="2026-08-14T11:02:00-04:00",
                received_at="2026-08-14T11:02:01-04:00",
            )
        )
        final_sale = self.ingest(
            envelope(
                "m4",
                "SOLD SPY 1 shares @ 190 AT 12:00 ET",
                message_time="2026-08-14T12:01:00-04:00",
                received_at="2026-08-14T12:01:01-04:00",
            )
        )

        replayed = self.replay("2026-08-14T12:02:00-04:00")

        self.assertEqual(replayed.positions, ())
        self.assertEqual(len(replayed.closed_trades), 2)
        first_trade, second_trade = replayed.closed_trades
        self.assertEqual(
            (
                first_trade.symbol,
                first_trade.opened_at,
                first_trade.closed_at,
                first_trade.buy_cost_micros,
                first_trade.gross_sale_micros,
                first_trade.fees_micros,
                first_trade.pnl_micros,
                first_trade.source_event_ids,
            ),
            (
                "SPY",
                datetime.fromisoformat("2026-08-14T09:40:00-04:00"),
                datetime.fromisoformat("2026-08-14T10:00:00-04:00"),
                100_000_000,
                110_000_000,
                0,
                10_000_000,
                (
                    first_buy.actions[0].event_id,
                    delayed_sale.actions[0].event_id,
                ),
            ),
        )
        self.assertEqual(
            (
                second_trade.symbol,
                second_trade.opened_at,
                second_trade.closed_at,
                second_trade.buy_cost_micros,
                second_trade.gross_sale_micros,
                second_trade.fees_micros,
                second_trade.pnl_micros,
                second_trade.source_event_ids,
            ),
            (
                "SPY",
                datetime.fromisoformat("2026-08-14T11:00:00-04:00"),
                datetime.fromisoformat("2026-08-14T12:00:00-04:00"),
                200_000_000,
                190_000_000,
                0,
                -10_000_000,
                (
                    second_buy.actions[0].event_id,
                    final_sale.actions[0].event_id,
                ),
            ),
        )
        self.assertEqual(
            self.rows(
                "SELECT consecutive_losses, weekly_high_water_micros, "
                "monthly_high_water_micros FROM actual_cash_projection"
            ),
            [(1, 5_010_000_000, 5_010_000_000)],
        )
        self.assertTrue(replayed.cache_matches_replay)

        before_restart_digest = replayed.source_digest
        before_restart_trades = replayed.closed_trades
        self.journal.close()
        self.journal = Journal.open(self.path)
        restarted = self.replay("2026-08-14T12:02:00-04:00")
        self.assertEqual(restarted.source_digest, before_restart_digest)
        self.assertEqual(restarted.closed_trades, before_restart_trades)
        self.assertTrue(restarted.cache_matches_replay)

    def test_equal_time_closes_use_receipt_cursor_for_breaker_metrics(self) -> None:
        self.ingest(
            envelope(
                "message:equal-time-spy-buy",
                "BOUGHT SPY 1 shares @ 100 AT 09:40 ET",
                message_time="2026-08-14T09:41:00-04:00",
                received_at="2026-08-14T09:41:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:equal-time-qqq-buy",
                "BOUGHT QQQ 1 shares @ 100 AT 09:50 ET",
                message_time="2026-08-14T09:51:00-04:00",
                received_at="2026-08-14T09:51:01-04:00",
            )
        )
        first_sale = self.ingest(
            envelope(
                "same-sale-0",
                "SOLD SPY 1 shares @ 120 AT 15:30 ET",
                message_time="2026-08-14T15:30:30-04:00",
                received_at="2026-08-14T15:30:31-04:00",
            )
        )
        second_sale = self.ingest(
            envelope(
                "same-sale-13",
                "SOLD QQQ 1 shares @ 90 AT 15:30 ET",
                message_time="2026-08-14T15:30:40-04:00",
                received_at="2026-08-14T15:30:41-04:00",
            )
        )

        replayed = self.replay("2026-08-14T15:31:00-04:00")

        self.assertLess(
            first_sale.actions[0].event_row_id,
            second_sale.actions[0].event_row_id,
        )
        self.assertEqual(
            self.rows(
                "SELECT consecutive_losses, weekly_high_water_micros, "
                "monthly_high_water_micros FROM actual_cash_projection"
            ),
            [(1, 5_020_000_000, 5_020_000_000)],
        )
        self.assertEqual(
            [trade.source_cursor for trade in replayed.closed_trades],
            [
                first_sale.actions[0].event_row_id,
                second_sale.actions[0].event_row_id,
            ],
        )
        self.assertTrue(replayed.cache_matches_replay)

    def test_reverse_effective_multi_action_is_atomic_restart_safe_and_recoverable(self) -> None:
        self.ingest(
            envelope(
                "message:reverse-seed",
                "BOUGHT SPY 1 shares @ 100 AT 10:00 ET",
                message_time="2026-08-14T10:01:00-04:00",
                received_at="2026-08-14T10:01:01-04:00",
            )
        )
        item = envelope(
            "message:reverse-batch",
            "SOLD SPY 1 shares @ 120 AT 15:31 ET\n"
            "BOUGHT QQQ 1 shares @ 100 AT 10:14 ET",
            message_time="2026-08-14T15:32:00-04:00",
            received_at="2026-08-14T15:32:01-04:00",
        )
        before = {
            table: self.journal.count(table)
            for table in (
                "raw_messages",
                "execution_events",
                "ledger_postings",
                "outbox",
            )
        }

        first = self.ingest(item)

        self.assertEqual([action.ordinal for action in first.actions], [0, 1])
        self.assertEqual(
            {table: self.journal.count(table) - count for table, count in before.items()},
            {
                "raw_messages": 1,
                "execution_events": 2,
                "ledger_postings": 2,
                "outbox": 2,
            },
        )
        self.assertEqual(
            self.rows(
                "SELECT action_ordinal, parsed_action, event_time "
                "FROM execution_events WHERE raw_message_id = ? ORDER BY id".replace(
                    "?", str(first.raw_row_id)
                )
            ),
            [
                (0, "SOLD", "2026-08-14T19:31:00.000000Z"),
                (1, "BOUGHT", "2026-08-14T14:14:00.000000Z"),
            ],
        )
        replayed = self.replay("2026-08-14T15:32:30-04:00")
        self.assertEqual([(position.symbol, position.shares) for position in replayed.positions], [("QQQ", 1)])
        self.assertFalse(replayed.cache_matches_replay)

        counts = {
            table: self.journal.count(table)
            for table in (
                "raw_messages",
                "execution_events",
                "ledger_postings",
                "actual_positions",
                "actual_cash_projection",
                "reconciliation_projection",
                "outbox",
            )
        }
        retry = self.ingest(item)
        self.assertTrue(retry.duplicate)
        self.assertEqual(first.actions, retry.actions)
        self.assertEqual(
            counts,
            {table: self.journal.count(table) for table in counts},
        )

        self.journal.close()
        self.journal = Journal.open(self.path)
        restarted = self.replay("2026-08-14T15:32:30-04:00")
        self.assertEqual(restarted.source_digest, replayed.source_digest)
        self.assertEqual(restarted.positions, replayed.positions)
        self.assertFalse(restarted.cache_matches_replay)

        self.ingest(
            envelope(
                "message:reverse-refresh",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 15:33 ET",
                message_time="2026-08-14T15:33:30-04:00",
                received_at="2026-08-14T15:33:31-04:00",
            )
        )
        recovered = self.replay("2026-08-14T15:34:00-04:00")
        # The skipped QQQ buy has no authoritative cache row. A later account
        # observation can refresh cash/reconciliation, not invent position
        # lineage, so the aggregate cache correctly remains stale.
        self.assertFalse(recovered.cache_matches_replay)

    def test_delayed_cash_only_action_revision_can_recover_on_current_observation(self) -> None:
        self.ingest(
            envelope(
                "message:cash-revision-buy",
                "BOUGHT SPY 1 shares @ 100 AT 10:00 ET",
                message_time="2026-08-14T10:01:00-04:00",
                received_at="2026-08-14T10:01:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:cash-revision-fee-newer",
                "FEE SPY 0.03 AT 10:20 ET",
                message_time="2026-08-14T10:21:00-04:00",
                received_at="2026-08-14T10:21:01-04:00",
            )
        )
        self.ingest(
            envelope(
                "message:cash-revision-fee-delayed",
                "FEE SPY 0.02 AT 2026-08-14T10:10:00-04:00",
                message_time="2026-08-14T10:22:00-04:00",
                received_at="2026-08-14T10:22:01-04:00",
            )
        )
        self.assertFalse(self.replay("2026-08-14T10:22:30-04:00").cache_matches_replay)

        self.ingest(
            envelope(
                "message:cash-revision-refresh",
                "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
                "unlogged_positions 0 AT 10:23 ET",
                message_time="2026-08-14T10:23:30-04:00",
                received_at="2026-08-14T10:23:31-04:00",
            )
        )
        recovered = self.replay("2026-08-14T10:24:00-04:00")
        self.assertTrue(recovered.cache_matches_replay)
        self.assertEqual(
            self.rows("SELECT revision FROM actual_cash_projection"),
            [(3,)],
        )


if __name__ == "__main__":
    unittest.main()
