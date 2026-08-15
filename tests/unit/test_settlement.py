from __future__ import annotations

import copy
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

from stock_monitor.domain import stable_execution_event_identity
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from stock_monitor.risk import (
    AccountCheck,
    ExecutionEvent,
    JournalEventWindow,
    RiskBlock,
    SessionCalendarResolver,
    SettlementLedger,
    SettlementPosting,
    account_check_eligible,
    evaluate_account_check_window,
)
import stock_monitor.risk as risk_module
from tests.support import (
    account_check,
    aware_et,
    buy_event,
    calendar_fixture,
    cash_adjustment,
)


def reviewed_calendar() -> MarketCalendar:
    return load_current_market_calendar(
        Path(__file__).resolve().parents[2],
        as_of=date(2026, 8, 14),
    )


def reviewed_next_year_calendar() -> MarketCalendar:
    return MarketCalendar.from_mapping(
        calendar_fixture(2027),
        as_of=date(2027, 1, 4),
    )


def confirmed_buy_action(*, price: Decimal = Decimal("100"), shares: int = 5):
    event_id, idempotency_key = stable_execution_event_identity(
        "message:settlement",
        0,
    )
    return risk_module._issue_confirmed_buy_action(
        event_id=event_id,
        idempotency_key=idempotency_key,
        message_id="message:settlement",
        action_ordinal=0,
        cursor=12,
        symbol="SPY",
        shares=shares,
        price=price,
        at=aware_et(date(2026, 8, 14), "10:14"),
        message_time=aware_et(date(2026, 8, 14), "10:14"),
        received_at=aware_et(date(2026, 8, 14), "10:14"),
        bid=Decimal("99.99"),
        ask=Decimal("100"),
        user_confirmed_stop=Decimal("97.50"),
        source="ROBINHOOD_MANUAL_CONFIRMATION",
        raw_sha256="1" * 64,
        details_sha256="2" * 64,
    )


class SettlementTests(unittest.TestCase):
    def test_account_check_covers_one_parent_order_across_partial_fills(
        self,
    ) -> None:
        check = AccountCheck(
            Decimal("500"),
            0,
            0,
            aware_et(date(2026, 8, 14), "10:00"),
            cursor=10,
        )
        first = ExecutionEvent(
            "PARTIAL_FILL",
            aware_et(date(2026, 8, 14), "10:12"),
            Decimal("100"),
            2,
            cursor=11,
            parent_order_id="robinhood:order:1",
            fill_group_planned_shares=5,
        )
        terminal = ExecutionEvent(
            "PARTIAL_FILL",
            aware_et(date(2026, 8, 14), "10:14"),
            Decimal("99"),
            3,
            cursor=12,
            parent_order_id="robinhood:order:1",
            fill_group_planned_shares=5,
        )

        self.assertTrue(account_check_eligible(check, terminal, (first,)))
        self.assertFalse(
            account_check_eligible(
                check,
                terminal,
                (replace(first, parent_order_id="robinhood:order:2"),),
            )
        )

    def test_task6_field_builders_never_mint_journal_authority(self) -> None:
        action = confirmed_buy_action()
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        window = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=action,
        )

        self.assertFalse(risk_module.is_issued_confirmed_buy_action(action))
        self.assertFalse(risk_module.is_issued_journal_event_window(window))
        decision = evaluate_account_check_window(
            check,
            action.execution_event,
            window,
        )
        self.assertFalse(decision.eligible)
        self.assertIn("EVENT_WINDOW_UNVERIFIED", decision.reason_codes)

    def test_settlement_digest_binds_full_posting_content(self) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        first = base.record_sale(
            price=Decimal("10"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_digest",
            record_cursor=20,
        )
        changed_posting = replace(first.postings[0], amount=Decimal("11"))
        changed = object.__new__(SettlementLedger)
        object.__setattr__(changed, "initial_settled_cash", first.initial_settled_cash)
        object.__setattr__(changed, "initialized_at", first.initialized_at)
        object.__setattr__(changed, "calendar_resolver", first.calendar_resolver)
        object.__setattr__(changed, "postings", (changed_posting,))

        self.assertNotEqual(
            risk_module._settlement_ledger_content_digest(first),
            risk_module._settlement_ledger_content_digest(changed),
        )

    def test_calendar_digest_is_independent_of_resolver_tuple_order(self) -> None:
        first = SessionCalendarResolver.for_diagnostics(
            (reviewed_calendar(), reviewed_next_year_calendar())
        )
        second = SessionCalendarResolver.for_diagnostics(
            (reviewed_next_year_calendar(), reviewed_calendar())
        )

        self.assertEqual(
            risk_module._calendar_digest(first),
            risk_module._calendar_digest(second),
        )

    def test_confirmation_ordinal_overflow_is_a_risk_block(self) -> None:
        event_id, idempotency_key = stable_execution_event_identity(
            "message:ordinal-max",
            0,
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^INVALID_CONFIRMATION_ACTION_ORDINAL$",
        ):
            risk_module._issue_confirmed_buy_action(
                event_id=event_id,
                idempotency_key=idempotency_key,
                message_id="message:ordinal-max",
                action_ordinal=2**63,
                cursor=12,
                symbol="SPY",
                shares=5,
                price=Decimal("100"),
                at=aware_et(date(2026, 8, 14), "10:14"),
                message_time=aware_et(date(2026, 8, 14), "10:14"),
                received_at=aware_et(date(2026, 8, 14), "10:14"),
                bid=Decimal("99.99"),
                ask=Decimal("100"),
                user_confirmed_stop=Decimal("97.50"),
                source="ROBINHOOD_MANUAL_CONFIRMATION",
                raw_sha256="1" * 64,
                details_sha256="2" * 64,
            )

    def test_settlement_canonicalizes_money_and_rejects_derived_balance_overflow(
        self,
    ) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("9223372036854.775000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        recorded = base.record_sale(
            price=Decimal("1.0"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:55"),
        )

        self.assertEqual(base.initial_settled_cash.as_tuple().exponent, -6)
        self.assertEqual(recorded.postings[0].amount.as_tuple().exponent, -6)
        with self.assertRaisesRegex(RiskBlock, "^INVALID_SETTLED_CASH$"):
            recorded.settled_cash(aware_et(date(2026, 8, 17), "09:30"))

    def test_settlement_rejects_duplicate_source_coordinates(self) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        first = base.record_sale(
            price=Decimal("10"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_coordinate_a",
            record_cursor=20,
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_SOURCE_COORDINATE_CONFLICT$",
        ):
            first.record_sale(
                price=Decimal("10"),
                shares=1,
                at=aware_et(date(2026, 8, 14), "15:55"),
                source_event_id="evt_coordinate_b",
                record_cursor=20,
            )

    def test_confirmed_buy_issuer_requires_ordered_source_times(self) -> None:
        event_id, idempotency_key = stable_execution_event_identity(
            "message:missing-source-time",
            0,
        )
        fields = {
            "event_id": event_id,
            "idempotency_key": idempotency_key,
            "message_id": "message:missing-source-time",
            "action_ordinal": 0,
            "cursor": 12,
            "symbol": "SPY",
            "shares": 5,
            "price": Decimal("100"),
            "at": aware_et(date(2026, 8, 14), "10:12"),
            "bid": Decimal("99.99"),
            "ask": Decimal("100"),
            "user_confirmed_stop": Decimal("97.50"),
            "source": "ROBINHOOD_MANUAL_CONFIRMATION",
            "raw_sha256": "a" * 64,
            "details_sha256": "b" * 64,
        }

        with self.assertRaisesRegex(
            RiskBlock,
            "^CONFIRMATION_SOURCE_TIME_INCOMPLETE$",
        ):
            risk_module._issue_confirmed_buy_action(**fields)

    def test_settlement_cutoff_uses_source_received_time_not_economic_time(
        self,
    ) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        recorded = base.record_buy(
            price=Decimal("100"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "10:12"),
            message_time=aware_et(date(2026, 8, 14), "10:20"),
            received_at=aware_et(date(2026, 8, 14), "10:21"),
            source_event_id="evt_delayed_buy",
            record_cursor=20,
        )

        self.assertEqual(
            recorded.settled_cash(aware_et(date(2026, 8, 14), "10:15")),
            Decimal("5000"),
        )
        self.assertEqual(
            recorded.settled_cash(aware_et(date(2026, 8, 14), "10:21")),
            Decimal("4900"),
        )

    def test_delayed_buy_checks_cash_at_source_receipt_cutoff(self) -> None:
        ledger = SettlementLedger(
            Decimal("100"),
            aware_et(date(2026, 8, 14), "09:00"),
            SessionCalendarResolver((reviewed_calendar(),)),
        )
        ledger = ledger.record_buy(
            price=Decimal("60"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "10:14"),
            source_event_id="evt_known",
            record_cursor=20,
            message_time=aware_et(date(2026, 8, 14), "10:14"),
            received_at=aware_et(date(2026, 8, 14), "10:14"),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^INSUFFICIENT_SETTLED_CASH$",
        ):
            ledger.record_buy(
                price=Decimal("60"),
                shares=1,
                at=aware_et(date(2026, 8, 14), "10:12"),
                source_event_id="evt_delayed",
                record_cursor=21,
                message_time=aware_et(date(2026, 8, 14), "10:20"),
                received_at=aware_et(date(2026, 8, 14), "10:21"),
            )

    def test_settlement_source_coordinates_require_monotone_knowledge_time(
        self,
    ) -> None:
        ledger = SettlementLedger(
            Decimal("100"),
            aware_et(date(2026, 8, 14), "09:00"),
            SessionCalendarResolver((reviewed_calendar(),)),
        )
        ledger = ledger.record_buy(
            price=Decimal("60"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "10:14"),
            source_event_id="evt_source_20",
            record_cursor=20,
            message_time=aware_et(date(2026, 8, 14), "10:29"),
            received_at=aware_et(date(2026, 8, 14), "10:30"),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_SOURCE_TIME_OUT_OF_ORDER$",
        ):
            ledger.record_buy(
                price=Decimal("60"),
                shares=1,
                at=aware_et(date(2026, 8, 14), "10:12"),
                source_event_id="evt_source_21",
                record_cursor=21,
                message_time=aware_et(date(2026, 8, 14), "10:19"),
                received_at=aware_et(date(2026, 8, 14), "10:20"),
            )

    def test_delayed_sale_is_unknown_before_source_receipt(self) -> None:
        ledger = SettlementLedger(
            Decimal("100"),
            aware_et(date(2026, 8, 14), "09:00"),
            SessionCalendarResolver((reviewed_calendar(),)),
        ).record_sale(
            price=Decimal("50"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_delayed_sale",
            record_cursor=20,
            message_time=aware_et(date(2026, 8, 17), "10:00"),
            received_at=aware_et(date(2026, 8, 17), "10:01"),
        )

        self.assertEqual(
            ledger.settled_cash(aware_et(date(2026, 8, 17), "09:30")),
            Decimal("100"),
        )
        self.assertEqual(
            ledger.settled_cash(aware_et(date(2026, 8, 17), "10:01")),
            Decimal("150"),
        )

    def test_account_and_posting_money_are_canonical_microdollars(self) -> None:
        check = AccountCheck(
            Decimal("5000"),
            0,
            0,
            aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        posting = SettlementPosting(
            kind="BUY",
            amount=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "10:14"),
            available_on=date(2026, 8, 14),
            posting_id="buy:canonical-money",
            cursor=12,
        )

        self.assertEqual(check.settled_cash.as_tuple().exponent, -6)
        self.assertEqual(posting.amount.as_tuple().exponent, -6)

    def test_live_resolver_rejects_structural_only_calendar(self) -> None:
        structural = MarketCalendar.from_mapping(
            calendar_fixture(),
            as_of=date(2026, 8, 14),
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^CALENDAR_RELEASE_AUTHORITY_UNVERIFIED$",
        ):
            SessionCalendarResolver((structural,))

        project_root = Path(__file__).resolve().parents[2]
        release = load_current_market_calendar(
            project_root,
            as_of=date(2026, 8, 14),
        )
        self.assertTrue(
            SessionCalendarResolver((release,)).is_open(date(2026, 8, 14))
        )

    def test_same_session_clean_check_precedes_buy(self) -> None:
        check = account_check(at="10:10", settled_cash="5000")
        buy = buy_event(at="10:14", price="100", shares=5)

        self.assertTrue(account_check_eligible(check, buy, ()))

    def test_check_must_strictly_precede_buy_in_same_et_session(self) -> None:
        buy = buy_event(at="10:14", price="100", shares=5)
        cases = (
            account_check(at="10:14", settled_cash="5000"),
            account_check(at="10:15", settled_cash="5000"),
            account_check(
                at="15:59",
                settled_cash="5000",
                session_date=date(2026, 8, 13),
            ),
        )
        for check in cases:
            with self.subTest(check_at=check.at):
                self.assertFalse(account_check_eligible(check, buy, ()))

    def test_account_wide_counts_reconciliation_and_cash_must_be_clear(self) -> None:
        buy = buy_event(at="10:14", price="100", shares=5)
        clean = account_check(at="10:10", settled_cash="500")
        cases = (
            replace(clean, pending_orders=1),
            replace(clean, unlogged_positions=1),
            replace(clean, reconciliation_result="RECONCILIATION_REQUIRED"),
            replace(clean, settled_cash=Decimal("499.99")),
        )
        for check in cases:
            with self.subTest(check=check):
                self.assertFalse(account_check_eligible(check, buy, ()))

    def test_intervening_account_adjustment_invalidates_check(self) -> None:
        check = account_check(at="10:10", settled_cash="5000")
        adjustment = cash_adjustment(at="10:12", amount="-100")
        buy = buy_event(at="10:14", price="100", shares=5)

        self.assertFalse(account_check_eligible(check, buy, (adjustment,)))

    def test_caller_constructed_journal_window_remains_diagnostic_only(self) -> None:
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        action = confirmed_buy_action()
        buy = action.execution_event
        complete = JournalEventWindow(
            after_cursor=10,
            through_cursor=12,
            events=(),
            complete=True,
            source="JOURNAL",
        )

        complete_decision = evaluate_account_check_window(check, buy, complete)
        self.assertFalse(complete_decision.eligible)
        self.assertIn("EVENT_WINDOW_UNVERIFIED", complete_decision.reason_codes)
        for window in (
            replace(complete, complete=False),
            replace(complete, source="CALLER"),
            replace(complete, after_cursor=9),
            replace(complete, through_cursor=13),
        ):
            with self.subTest(window=window):
                decision = evaluate_account_check_window(check, buy, window)
                self.assertFalse(decision.eligible)
                self.assertIn("EVENT_WINDOW_UNVERIFIED", decision.reason_codes)

    def test_task6_window_cannot_authorize_account_check(self) -> None:
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        action = confirmed_buy_action()
        buy = action.execution_event
        issued = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=action,
        )

        decision = evaluate_account_check_window(check, buy, issued)
        self.assertFalse(decision.eligible)
        self.assertIn("EVENT_WINDOW_UNVERIFIED", decision.reason_codes)
        reconstructed = JournalEventWindow(
            after_cursor=issued.after_cursor,
            through_cursor=issued.through_cursor,
            events=issued.events,
            complete=issued.complete,
            source=issued.source,
        )
        for forged in (
            copy.copy(issued),
            replace(issued),
            reconstructed,
        ):
            with self.subTest(forged=forged):
                decision = evaluate_account_check_window(check, buy, forged)
                self.assertFalse(decision.eligible)
                self.assertIn(
                    "EVENT_WINDOW_UNVERIFIED",
                    decision.reason_codes,
                )

    def test_diagnostic_window_still_binds_exact_check_and_buy_endpoints(self) -> None:
        check = AccountCheck(
            Decimal("5000"),
            0,
            0,
            aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        action = confirmed_buy_action()
        buy = action.execution_event
        issued = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=action,
        )

        baseline = evaluate_account_check_window(check, buy, issued)
        self.assertFalse(baseline.eligible)
        self.assertIn("EVENT_WINDOW_UNVERIFIED", baseline.reason_codes)
        for forged_check, forged_buy in (
            (replace(check, settled_cash=Decimal("6000")), buy),
            (check, replace(buy, price=Decimal("99"))),
            (check, replace(buy, shares=4)),
        ):
            with self.subTest(check=forged_check, buy=forged_buy):
                result = evaluate_account_check_window(
                    forged_check,
                    forged_buy,
                    issued,
                )
                self.assertFalse(result.eligible)
                self.assertIn("EVENT_WINDOW_ENDPOINT_MISMATCH", result.reason_codes)

    def test_settlement_rebuild_rejects_forged_same_day_sale_availability(self) -> None:
        sale_at = aware_et(date(2026, 8, 14), "15:55")
        forged = SettlementPosting(
            posting_id="sale-1",
            kind="SALE",
            amount=Decimal("100"),
            at=sale_at,
            available_on=date(2026, 8, 14),
            cursor=1,
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_AVAILABILITY_CONFLICT$",
        ):
            SettlementLedger(
                initial_settled_cash=Decimal("5000"),
                initialized_at=aware_et(date(2026, 8, 14), "09:00"),
                calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
                postings=(forged,),
            )

    def test_settlement_posting_replay_is_idempotent_and_conflicts_fail(self) -> None:
        ledger = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        ).record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 8, 14), "15:55"),
        )
        posting = ledger.postings[0]

        replayed = replace(ledger, postings=(posting, posting))
        self.assertEqual(replayed.postings, (posting,))
        conflict = replace(posting, amount=Decimal("101"))
        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_POSTING_IDEMPOTENCY_CONFLICT$",
        ):
            replace(ledger, postings=(posting, conflict))

    def test_settlement_identity_comes_from_stable_source_event_not_payload(self) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        first = base.record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_sale_a",
            record_cursor=20,
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_POSTING_IDEMPOTENCY_CONFLICT$",
        ):
            first.record_sale(
                price=Decimal("10.01"),
                shares=10,
                at=aware_et(date(2026, 8, 14), "15:55"),
                source_event_id="evt_sale_a",
                record_cursor=20,
            )

        distinct = first.record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_sale_b",
            record_cursor=21,
        )
        self.assertEqual(len(distinct.postings), 2)

    def test_settlement_record_order_is_separate_from_economic_time(self) -> None:
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        later_reported_first = base.record_sale(
            price=Decimal("10"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:55"),
            source_event_id="evt_later",
            record_cursor=30,
            message_time=aware_et(date(2026, 8, 14), "15:55"),
            received_at=aware_et(date(2026, 8, 14), "15:56"),
        )

        delayed = later_reported_first.record_sale(
            price=Decimal("9"),
            shares=1,
            at=aware_et(date(2026, 8, 14), "15:50"),
            source_event_id="evt_delayed",
            record_cursor=31,
            message_time=aware_et(date(2026, 8, 14), "15:56"),
            received_at=aware_et(date(2026, 8, 14), "15:57"),
        )

        self.assertEqual(
            tuple(posting.posting_id for posting in delayed.postings),
            ("settlement:evt_later", "settlement:evt_delayed"),
        )

    def test_settlement_rejects_events_outside_reviewed_session_hours(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        base = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "00:00"),
            calendar_resolver=resolver,
        )
        for method, at in (
            (base.record_buy, aware_et(date(2026, 8, 14), "00:01")),
            (base.record_sale, aware_et(date(2026, 8, 14), "23:59")),
        ):
            with self.subTest(at=at), self.assertRaisesRegex(
                RiskBlock,
                "^SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION$",
            ):
                method(price=Decimal("10"), shares=1, at=at)

        forged = SettlementPosting(
            kind="SALE",
            amount=Decimal("10"),
            at=aware_et(date(2026, 8, 14), "23:59"),
            available_on=date(2026, 8, 17),
            posting_id="settlement:evt_after_hours",
            source_event_id="evt_after_hours",
            cursor=1,
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION$",
        ):
            replace(base, postings=(forged,))

    def test_journal_window_uses_cursor_order_and_invalidates_same_time_event(self) -> None:
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        buy = ExecutionEvent(
            kind="BUY",
            at=aware_et(date(2026, 8, 14), "10:14"),
            price=Decimal("100"),
            shares=5,
            cursor=13,
        )
        same_time_adjustment = ExecutionEvent(
            kind="CASH_ADJUSTMENT",
            at=buy.at,
            amount=Decimal("-1"),
            cursor=12,
        )
        same_time = JournalEventWindow(
            after_cursor=10,
            through_cursor=13,
            events=(same_time_adjustment,),
            complete=True,
            source="JOURNAL",
        )

        decision = evaluate_account_check_window(check, buy, same_time)

        self.assertFalse(decision.eligible)
        self.assertIn("INTERVENING_ACCOUNT_EVENT", decision.reason_codes)

        first = replace(same_time_adjustment, kind="QUOTE", at=check.at, cursor=11)
        second = replace(first, at=buy.at, cursor=12)
        for events in ((second, first), (first, replace(second, cursor=11))):
            with self.subTest(events=events):
                window = replace(same_time, events=events)
                result = evaluate_account_check_window(check, buy, window)
                self.assertFalse(result.eligible)
                self.assertIn("EVENT_WINDOW_UNVERIFIED", result.reason_codes)

    def test_friday_and_holiday_sales_settle_on_next_open_session(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        ledger = SettlementLedger(
            initial_settled_cash=Decimal("1000"),
            initialized_at=aware_et(date(2026, 7, 1), "09:00"),
            calendar_resolver=resolver,
        )
        holiday_sale = ledger.record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 7, 2), "15:55"),
        )
        friday_sale = holiday_sale.record_sale(
            price=Decimal("20"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "15:55"),
        )

        self.assertEqual(holiday_sale.postings[-1].available_on, date(2026, 7, 6))
        self.assertEqual(friday_sale.postings[-1].available_on, date(2026, 8, 17))
        self.assertEqual(
            friday_sale.settled_cash(aware_et(date(2026, 8, 14), "16:00")),
            Decimal("1100"),
        )
        self.assertEqual(
            friday_sale.settled_cash(aware_et(date(2026, 8, 17), "09:30")),
            Decimal("1200"),
        )

    def test_unsettled_sale_proceeds_are_not_available_for_purchase(self) -> None:
        ledger = SettlementLedger(
            initial_settled_cash=Decimal("1000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        ).record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 8, 14), "10:00"),
        )

        self.assertEqual(
            ledger.settled_cash(aware_et(date(2026, 8, 14), "10:01")),
            Decimal("1000"),
        )
        self.assertEqual(
            ledger.unsettled_sale_proceeds(
                aware_et(date(2026, 8, 14), "10:01")
            ),
            Decimal("100"),
        )

    def test_buy_posts_immediately_and_cannot_use_unsettled_cash(self) -> None:
        original = SettlementLedger(
            initial_settled_cash=Decimal("1000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        bought = original.record_buy(
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:00"),
        )

        self.assertEqual(
            original.settled_cash(aware_et(date(2026, 8, 14), "10:01")),
            Decimal("1000"),
        )
        self.assertEqual(
            bought.settled_cash(aware_et(date(2026, 8, 14), "10:01")),
            Decimal("500"),
        )
        with self.assertRaisesRegex(RiskBlock, "^INSUFFICIENT_SETTLED_CASH$"):
            bought.record_buy(
                price=Decimal("501"),
                shares=1,
                at=aware_et(date(2026, 8, 14), "10:02"),
            )

    def test_cross_year_sale_remains_unsettled_without_verified_next_year(self) -> None:
        ledger = SettlementLedger(
            initial_settled_cash=Decimal("1000"),
            initialized_at=aware_et(date(2026, 12, 31), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        ).record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 12, 31), "15:55"),
        )

        posting = ledger.postings[-1]
        self.assertIsNone(posting.available_on)
        self.assertEqual(posting.reason_code, "CALENDAR_COVERAGE_MISSING")
        self.assertEqual(
            ledger.settled_cash(aware_et(date(2026, 12, 31), "16:00")),
            Decimal("1000"),
        )
        self.assertIn("CALENDAR_COVERAGE_MISSING", ledger.reason_codes)

    def test_unresolved_cross_year_sale_resolves_after_next_year_is_verified(self) -> None:
        unresolved = SettlementLedger(
            initial_settled_cash=Decimal("1000"),
            initialized_at=aware_et(date(2026, 12, 31), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        ).record_sale(
            price=Decimal("10"),
            shares=10,
            at=aware_et(date(2026, 12, 31), "15:55"),
        )
        resolver = SessionCalendarResolver.for_diagnostics(
            (reviewed_calendar(), reviewed_next_year_calendar())
        )

        resolved = unresolved.resolve_calendar(resolver)

        self.assertIsNone(unresolved.postings[-1].available_on)
        self.assertEqual(resolved.postings[-1].available_on, date(2027, 1, 4))
        self.assertEqual(resolved.reason_codes, ())
        self.assertEqual(
            resolved.settled_cash(aware_et(date(2027, 1, 4), "09:30")),
            Decimal("1100"),
        )

    def test_account_check_cash_is_evidence_not_strategy_ledger_profit(self) -> None:
        profitable_strategy_ledger = SettlementLedger(
            initial_settled_cash=Decimal("5000"),
            initialized_at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        check = account_check(at="10:10", settled_cash="100")
        buy = buy_event(at="10:14", price="100", shares=2)

        self.assertEqual(
            profitable_strategy_ledger.settled_cash(
                aware_et(date(2026, 8, 14), "10:14")
            ),
            Decimal("5000"),
        )
        self.assertFalse(account_check_eligible(check, buy, ()))


if __name__ == "__main__":
    unittest.main()
