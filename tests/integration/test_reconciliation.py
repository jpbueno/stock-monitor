from __future__ import annotations

import unittest
from datetime import date, datetime
from decimal import Decimal

from stock_monitor.confirmations import parse_confirmation, parse_confirmation_or_pending
from stock_monitor.ledger import LedgerSignal
from stock_monitor.reconciliation import (
    ActionStatus,
    ProjectionPosition,
    ProjectionState,
    ResolvedSignalPlan,
    SignalPlanResolver,
    UnavailableSignalPlanResolver,
    assess_confirmation,
)


SESSION = date(2026, 8, 14)


class StaticResolver:
    def __init__(self, signal: LedgerSignal | None) -> None:
        self.signal = signal

    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ):
        del query_cutoff
        if (
            self.signal is not None
            and self.signal.symbol == symbol
            and self.signal.publication_session == economic_at.date()
        ):
            return ResolvedSignalPlan(
                signal=self.signal,
                report_id="report:test",
                publication_rank=1,
                publication_source_digest="a" * 64,
            )
        return None


def signal(
    *,
    role: str = "PRIMARY",
    maximum_entry: str = "100.25",
    planned_shares: int = 5,
) -> LedgerSignal:
    # Direct construction is intentionally not production publication authority.
    entry = Decimal(maximum_entry)
    stop = Decimal("97.50")
    return LedgerSignal(
        signal_id="sig-1",
        symbol="SPY",
        role=role,
        publication_session=SESSION,
        maximum_entry=entry,
        recommended_stop=stop,
        target=entry + Decimal("2") * (entry - stop),
        planned_shares=planned_shares,
        tick_size=Decimal("0.01"),
        trigger_price=Decimal("100.00"),
    )


def full_buy(
    *, price: str = "100.25", bid: str = "100.24", ask: str = "100.25", stop: str = "97.50"
):
    return parse_confirmation(
        f"BOUGHT SPY 5 shares @ {price} AT 10:14 ET; BID {bid} ASK {ask}; STOP SET @ {stop}",
        session_date=SESSION,
    )


class ReconciliationAssessmentTests(unittest.TestCase):
    def test_resolver_contract_is_structural_and_unavailable_fails_closed(self) -> None:
        self.assertIsInstance(StaticResolver(signal()), SignalPlanResolver)

        result = assess_confirmation(
            full_buy(),
            ProjectionState(),
            signal_resolver=UnavailableSignalPlanResolver(),
        )

        self.assertEqual(result.status, ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED)
        self.assertIn("SIGNAL_PLAN_UNAVAILABLE", result.reason_codes)
        self.assertTrue(result.apply_economic_event)

    def test_exact_but_unissued_test_signal_does_not_claim_publication_authority(self) -> None:
        result = assess_confirmation(
            full_buy(),
            ProjectionState(),
            signal_resolver=StaticResolver(signal()),
        )

        self.assertIsNone(result.signal_id)
        self.assertEqual(result.reason_codes, ("SIGNAL_AUTHORITY_UNVERIFIED",))
        self.assertTrue(result.apply_economic_event)

    def test_buy_rule_violations_are_accumulated_without_dropping_exposure(self) -> None:
        cases = (
            (
                full_buy(price="100.26", bid="100.25", ask="100.26"),
                signal(),
                "FILL_ABOVE_MAXIMUM_ENTRY",
            ),
            (full_buy(), signal(role="WATCHLIST_SHADOW"), "SHADOW_FILL"),
            (
                parse_confirmation(
                    "BOUGHT SPY 4 shares @ 100.25 AT 10:14 ET; BID 100.24 ASK 100.25; STOP SET @ 97.50",
                    session_date=SESSION,
                ),
                signal(),
                "SHARE_QUANTITY_MISMATCH",
            ),
            (
                full_buy(bid="99.90", ask="100.25"),
                signal(),
                "CONFIRMED_SPREAD_TOO_WIDE",
            ),
            (
                full_buy(stop="97.49"),
                signal(),
                "CONFIRMED_STOP_WIDER_THAN_RECOMMENDED",
            ),
        )
        for action, plan, reason in cases:
            with self.subTest(reason=reason):
                result = assess_confirmation(
                    action,
                    ProjectionState(),
                    signal_resolver=StaticResolver(plan),
                )
                self.assertIn(reason, result.reason_codes)
                self.assertTrue(result.apply_economic_event)

    def test_degraded_buy_is_preserved_and_marks_all_missing_execution_evidence(self) -> None:
        action = parse_confirmation(
            "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
            session_date=SESSION,
        )
        result = assess_confirmation(
            action,
            ProjectionState(),
            signal_resolver=StaticResolver(signal()),
        )

        self.assertTrue(result.apply_economic_event)
        self.assertIn("CONFIRMED_BID_MISSING", result.reason_codes)
        self.assertIn("CONFIRMED_ASK_MISSING", result.reason_codes)
        self.assertIn("CONFIRMED_STOP_MISSING", result.reason_codes)

    def test_position_addition_and_over_sell_are_visible_reconciliation_events(self) -> None:
        state = ProjectionState(
            positions=(
                ProjectionPosition(
                    signal_id="sig-1",
                    symbol="SPY",
                    shares=5,
                    cost_basis_micros=501_250_000,
                    recommended_stop_micros=97_500_000,
                    user_confirmed_stop_micros=97_500_000,
                    target_micros=105_750_000,
                ),
            )
        )
        addition = assess_confirmation(
            full_buy(), state, signal_resolver=StaticResolver(signal())
        )
        over_sell = assess_confirmation(
            parse_confirmation(
                "SOLD SPY 6 shares @ 101 AT 15:31 ET", session_date=SESSION
            ),
            state,
            signal_resolver=StaticResolver(signal()),
        )

        self.assertIn("POSITION_ADDITIONS_PROHIBITED", addition.reason_codes)
        self.assertIn("OVER_SELL_REPORTED", over_sell.reason_codes)
        self.assertTrue(over_sell.apply_economic_event)

    def test_addition_above_exact_average_cost_is_not_mislabeled_averaging_down(self) -> None:
        state = ProjectionState(
            positions=(
                ProjectionPosition(
                    signal_id="sig-1",
                    symbol="SPY",
                    shares=5,
                    cost_basis_micros=500_000_000,
                    recommended_stop_micros=97_500_000,
                    user_confirmed_stop_micros=97_500_000,
                    target_micros=105_750_000,
                ),
            )
        )
        action = parse_confirmation(
            "BOUGHT SPY 1 shares @ 101 AT 10:14 ET; BID 100.99 ASK 101; STOP SET @ 97.50",
            session_date=SESSION,
        )
        result = assess_confirmation(
            action,
            state,
            signal_resolver=StaticResolver(signal(planned_shares=1, maximum_entry="101")),
        )

        self.assertIn("POSITION_ADDITIONS_PROHIBITED", result.reason_codes)
        self.assertNotIn("AVERAGING_DOWN_PROHIBITED", result.reason_codes)

    def test_wider_stop_is_stored_but_noncompliant(self) -> None:
        state = ProjectionState(
            positions=(
                ProjectionPosition(
                    signal_id="sig-1",
                    symbol="SPY",
                    shares=5,
                    cost_basis_micros=500_000_000,
                    recommended_stop_micros=98_000_000,
                    user_confirmed_stop_micros=98_000_000,
                    target_micros=105_000_000,
                ),
            )
        )
        result = assess_confirmation(
            parse_confirmation(
                "STOP UPDATED SPY @ 97.99 AT 15:32 ET", session_date=SESSION
            ),
            state,
            signal_resolver=StaticResolver(signal()),
        )

        self.assertEqual(result.status, ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED)
        self.assertIn("STOP_WIDENING_PROHIBITED", result.reason_codes)
        self.assertTrue(result.apply_economic_event)

    def test_partial_fill_needs_explicit_parent_and_matching_total(self) -> None:
        plain = assess_confirmation(
            parse_confirmation(
                "PARTIAL FILL SPY 2 shares @ 100.25 AT 10:14 ET",
                session_date=SESSION,
            ),
            ProjectionState(),
            signal_resolver=StaticResolver(signal()),
        )
        mismatched = assess_confirmation(
            parse_confirmation(
                "PARTIAL FILL SPY 2 shares @ 100.25 AT 10:14 ET; ORDER order:1 TOTAL 6 shares",
                session_date=SESSION,
            ),
            ProjectionState(),
            signal_resolver=StaticResolver(signal()),
        )

        self.assertIn("PARENT_ORDER_UNVERIFIED", plain.reason_codes)
        self.assertIn("FILL_GROUP_QUANTITY_MISMATCH", mismatched.reason_codes)

    def test_ambiguous_text_never_mutates_economic_state(self) -> None:
        pending = parse_confirmation_or_pending(
            "sold a bit of SPY", session_date=SESSION
        )
        result = assess_confirmation(
            pending,
            ProjectionState(),
            signal_resolver=UnavailableSignalPlanResolver(),
        )

        self.assertEqual(result.status, ActionStatus.PENDING_CLARIFICATION)
        self.assertFalse(result.apply_economic_event)

    def test_account_check_with_unknown_activity_forces_reconciliation(self) -> None:
        action = parse_confirmation(
            "ACCOUNT CHECK settled_cash 5000 pending_orders 1 unlogged_positions 2 AT 10:10 ET",
            session_date=SESSION,
        )
        result = assess_confirmation(
            action,
            ProjectionState(),
            signal_resolver=UnavailableSignalPlanResolver(),
        )

        self.assertIn("PENDING_ORDERS_PRESENT", result.reason_codes)
        self.assertIn("UNLOGGED_POSITIONS_PRESENT", result.reason_codes)

    def test_clear_account_check_and_exact_spread_threshold_are_rule_clear(self) -> None:
        clear = assess_confirmation(
            parse_confirmation(
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET",
                session_date=SESSION,
            ),
            ProjectionState(),
            signal_resolver=UnavailableSignalPlanResolver(),
        )
        threshold = assess_confirmation(
            full_buy(price="100.12", bid="99.875", ask="100.125"),
            ProjectionState(),
            signal_resolver=StaticResolver(signal()),
        )

        self.assertEqual(clear.status, ActionStatus.COMPLIANT)
        self.assertNotIn("CONFIRMED_SPREAD_TOO_WIDE", threshold.reason_codes)
        self.assertEqual(threshold.reason_codes, ("SIGNAL_AUTHORITY_UNVERIFIED",))

    def test_valid_tightening_and_non_oversell_exit_boundaries_are_rule_clear(self) -> None:
        state = ProjectionState(
            positions=(
                ProjectionPosition(
                    signal_id="sig-1",
                    symbol="SPY",
                    shares=5,
                    cost_basis_micros=500_000_000,
                    recommended_stop_micros=98_000_000,
                    user_confirmed_stop_micros=98_000_000,
                    target_micros=105_000_000,
                ),
            )
        )
        actions = (
            "STOP UPDATED SPY @ 98.01 AT 15:32 ET",
            "SOLD SPY 5 shares @ 101 AT 15:31 ET",
            "SOLD SPY 2 shares @ 101 AT 15:31 ET",
            "STOP FILLED SPY 5 shares @ 97 AT 10:01 ET",
        )
        for text in actions:
            with self.subTest(text=text):
                result = assess_confirmation(
                    parse_confirmation(text, session_date=SESSION),
                    state,
                    signal_resolver=StaticResolver(signal()),
                )
                self.assertEqual(result.status, ActionStatus.COMPLIANT)

    def test_non_entry_action_classifications_are_explicit(self) -> None:
        cases = (
            ("SKIPPED SPY", ActionStatus.COMPLIANT, False),
            (
                "FEE SPY 0.03 AT 15:33 ET",
                ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
                True,
            ),
            (
                "RECONCILE CASH -10 REASON correction AT 11:00 ET",
                ActionStatus.COMPLIANT,
                True,
            ),
            (
                "RECONCILE UNRELATED POSITION SPY +2 shares @ 100 AT 11:01 ET",
                ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
                True,
            ),
            (
                "OPTION PAPER WINDOW START AT 10:00 ET",
                ActionStatus.COMPLIANT,
                False,
            ),
            (
                "OPTION PAPER MARK AAPL260918C00150000 BID 2 ASK 2.10 AT 15:40 ET",
                ActionStatus.COMPLIANT,
                False,
            ),
        )
        for text, status, applies in cases:
            with self.subTest(text=text):
                result = assess_confirmation(
                    parse_confirmation(text, session_date=SESSION),
                    ProjectionState(),
                    signal_resolver=UnavailableSignalPlanResolver(),
                )
                self.assertEqual(result.status, status)
                self.assertEqual(result.apply_economic_event, applies)


if __name__ == "__main__":
    unittest.main()
