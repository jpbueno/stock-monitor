from __future__ import annotations

import copy
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

from stock_monitor.risk import (
    MAX_HOLD_SESSIONS,
    MarketMark,
    Position,
    SessionCalendarResolver,
    build_market_mark,
    evaluate_position,
    evaluate_position_addition,
)
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from tests.support import aware_et, calendar_fixture, policy_fixture
import stock_monitor.risk as risk_module


def reviewed_calendar() -> MarketCalendar:
    return load_current_market_calendar(
        Path(__file__).resolve().parents[2],
        as_of=date(2026, 8, 14),
    )


def position(*, shares: int = 5, entry: str = "100") -> Position:
    return Position(
        signal_id="sig-1",
        symbol="SPY",
        entry=Decimal(entry),
        shares=shares,
        initial_stop=Decimal("98"),
        recommended_stop=Decimal("98"),
        user_confirmed_stop=Decimal("98"),
        target=Decimal("104"),
        tick_size=Decimal("0.01"),
        entered_session=date(2026, 8, 3),
    )


def mark(
    price: str,
    *,
    current: Position | None = None,
    holding_sessions: int = 5,
    previous_low: str | None = None,
    current_low: str | None = None,
    atr14: str | None = None,
    event_exit_required: bool = False,
    thesis_invalidated: bool = False,
) -> MarketMark:
    current = current or position()
    resolver = SessionCalendarResolver((reviewed_calendar(),))
    mark_session = resolver.add_sessions(
        current.entered_session,
        holding_sessions - 1,
    )
    mark_at = aware_et(mark_session, "15:30")
    previous_value = Decimal(previous_low) if previous_low is not None else None
    current_value = Decimal(current_low) if current_low is not None else None
    atr_value = Decimal(atr14) if atr14 is not None else None
    event_context = risk_module._issue_position_event_context(
        position=current,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        at=mark_at,
        cursor=1,
        start_cursor=1,
        event_count=0,
        calendar_resolver=resolver,
        price=Decimal(price),
        previous_session_low=previous_value,
        current_session_low=current_value,
        atr14=atr_value,
    )
    return build_market_mark(
        current,
        price=Decimal(price),
        at=mark_at,
        calendar_resolver=resolver,
        previous_session_low=(
            previous_value
        ),
        current_session_low=(
            current_value
        ),
        atr14=atr_value,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        event_context_verified=True,
        position_event_context=event_context,
    )


class PositionManagementTests(unittest.TestCase):
    def test_position_action_canonicalizes_hostile_zero_r_multiple(self) -> None:
        action = risk_module.PositionAction(
            status="PROVISIONAL_HOLD",
            reason_codes=(),
            recommended_stop=Decimal("98"),
            user_confirmed_stop=Decimal("98"),
            published_target=Decimal("104"),
            shares_to_exit=0,
            remaining_shares=5,
            r_multiple=Decimal("-0E-999999"),
        )

        self.assertEqual(action.r_multiple, Decimal("0.000000"))
        self.assertEqual(action.r_multiple.as_tuple().exponent, -6)
        self.assertEqual(action.r_multiple.as_tuple().sign, 0)

    def test_position_thresholds_use_exact_r_before_display_rounding(self) -> None:
        current = position()

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark("103.999999", current=current),
            policy_fixture(),
        )

        self.assertEqual(action.r_multiple, Decimal("1.999999"))
        self.assertNotEqual(action.status, "PROVISIONAL_EXIT")
        self.assertNotIn("TWO_R_REACHED", action.reason_codes)

    def test_position_action_rejects_unbounded_r_multiple(self) -> None:
        with self.assertRaisesRegex(
            risk_module.RiskBlock,
            "^INVALID_R_MULTIPLE$",
        ):
            risk_module.PositionAction(
                status="PROVISIONAL_HOLD",
                reason_codes=(),
                recommended_stop=Decimal("98"),
                user_confirmed_stop=Decimal("98"),
                published_target=Decimal("104"),
                shares_to_exit=0,
                remaining_shares=5,
                r_multiple=Decimal("1e999999"),
            )

    def test_diagnostic_calendar_cannot_issue_operational_position_context(
        self,
    ) -> None:
        diagnostic = SessionCalendarResolver.for_diagnostics(
            (
                MarketCalendar.from_mapping(
                    calendar_fixture(),
                    as_of=date(2026, 8, 14),
                ),
            )
        )
        with self.assertRaisesRegex(
            risk_module.RiskBlock,
            "^CALENDAR_RELEASE_AUTHORITY_UNVERIFIED$",
        ):
            risk_module._issue_position_event_context(
                position=position(),
                event_exit_required=False,
                thesis_invalidated=False,
                at=aware_et(date(2026, 8, 14), "15:30"),
                cursor=1,
                start_cursor=1,
                event_count=0,
                calendar_resolver=diagnostic,
                price=Decimal("100"),
            )

    def test_break_even_recommendation_rounds_down_to_position_tick(self) -> None:
        current = position(entry="100.005")

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark("102.01", current=current),
            policy_fixture(),
        )

        self.assertEqual(action.status, "PROVISIONAL_TIGHTEN_STOP")
        self.assertEqual(action.recommended_stop, Decimal("100.00"))

    def test_early_close_uses_review_boundary_and_rejects_outside_window(
        self,
    ) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        current = replace(
            position(),
            entered_session=date(2026, 11, 25),
        )
        review_at = aware_et(date(2026, 11, 27), "12:30")
        context = risk_module._issue_position_event_context(
            position=current,
            event_exit_required=False,
            thesis_invalidated=False,
            at=review_at,
            cursor=1,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("100"),
        )
        reviewed = build_market_mark(
            current,
            price=Decimal("100"),
            at=review_at,
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=context,
        )

        self.assertEqual(
            evaluate_position(current, reviewed, policy_fixture()).status,
            "POSITION_UNVERIFIED",
        )
        for clock in ("12:29", "13:01"):
            with self.subTest(clock=clock), self.assertRaisesRegex(
                risk_module.RiskBlock,
                "^POSITION_REVIEW_OUTSIDE_WINDOW$",
            ):
                risk_module._issue_position_event_context(
                    position=current,
                    event_exit_required=False,
                    thesis_invalidated=False,
                    at=aware_et(date(2026, 11, 27), clock),
                    cursor=1,
                    start_cursor=1,
                    event_count=0,
                    calendar_resolver=resolver,
                    price=Decimal("100"),
                )

    def test_issued_mark_cannot_be_reused_for_a_different_position_revision(
        self,
    ) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        fresh = replace(position(), entered_session=date(2026, 8, 14))
        event_context = risk_module._issue_position_event_context(
            position=fresh,
            event_exit_required=False,
            thesis_invalidated=False,
            at=aware_et(date(2026, 8, 14), "15:30"),
            cursor=1,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("100"),
        )
        fresh_mark = build_market_mark(
            fresh,
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=event_context,
        )

        action = evaluate_position(position(), fresh_mark, policy_fixture())

        self.assertEqual(action.status, "POSITION_UNVERIFIED")
        self.assertIn("POSITION_CONTEXT_UNVERIFIED", action.reason_codes)

    def test_pre_review_mark_and_stale_event_context_cannot_drive_action(
        self,
    ) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        complete_context = risk_module._issue_position_event_context(
            position=position(),
            event_exit_required=False,
            thesis_invalidated=False,
            at=aware_et(date(2026, 8, 14), "15:30"),
            cursor=1,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("102"),
        )
        pre_review_context = replace(
            complete_context,
            at=aware_et(date(2026, 8, 14), "09:00"),
        )
        pre_review = build_market_mark(
            position(),
            price=Decimal("102"),
            at=aware_et(date(2026, 8, 14), "09:00"),
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=pre_review_context,
        )
        stale_context_mark = build_market_mark(
            position(),
            price=Decimal("102"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=pre_review_context,
        )

        self.assertEqual(
            evaluate_position(position(), pre_review, policy_fixture()).status,
            "POSITION_UNVERIFIED",
        )
        self.assertEqual(
            evaluate_position(
                position(),
                stale_context_mark,
                policy_fixture(),
            ).status,
            "POSITION_UNVERIFIED",
        )

    def test_missing_stop_does_not_suppress_mandatory_full_exits(self) -> None:
        current = replace(position(), user_confirmed_stop=None)
        for current_mark, reason in (
            (
                mark("100", current=current, event_exit_required=True),
                "EVENT_EXIT_REQUIRED",
            ),
            (
                mark(
                    "100",
                    current=current,
                    holding_sessions=MAX_HOLD_SESSIONS,
                ),
                "MAX_HOLD_SESSIONS_REACHED",
            ),
            (mark("97", current=current), "RECOMMENDED_STOP_REACHED"),
        ):
            with self.subTest(reason=reason):
                action = risk_module.evaluate_position_diagnostic(
                    current,
                    current_mark,
                    policy_fixture(),
                )
                self.assertEqual(action.status, "PROVISIONAL_EXIT")
                self.assertEqual(action.shares_to_exit, current.shares)
                self.assertIn(reason, action.reason_codes)
                self.assertIn("STOP_UNVERIFIED", action.reason_codes)

    def test_intraday_low_cross_without_ordered_stop_fill_fails_closed(self) -> None:
        action = risk_module.evaluate_position_diagnostic(
            position(),
            mark(
                "100",
                previous_low="99",
                current_low="97",
                atr14="2",
            ),
            policy_fixture(),
        )

        self.assertEqual(action.status, "RECONCILIATION_REQUIRED")
        self.assertIn("STOP_EXECUTION_UNVERIFIED", action.reason_codes)

    def test_tighter_confirmed_stop_cross_requires_ordered_execution_evidence(
        self,
    ) -> None:
        current = replace(position(), user_confirmed_stop=Decimal("99"))

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark(
                "100",
                current=current,
                previous_low="100",
                current_low="99",
                atr14="2",
            ),
            policy_fixture(),
        )

        self.assertEqual(action.status, "RECONCILIATION_REQUIRED")
        self.assertIn("STOP_EXECUTION_UNVERIFIED", action.reason_codes)

    def test_price_crossing_tighter_stop_requires_ordered_execution_evidence(
        self,
    ) -> None:
        current = replace(position(), user_confirmed_stop=Decimal("99"))

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark("98.50", current=current),
            policy_fixture(),
        )

        self.assertEqual(action.status, "RECONCILIATION_REQUIRED")
        self.assertIn("STOP_EXECUTION_UNVERIFIED", action.reason_codes)

    def test_wider_user_stop_stays_separate_and_never_widens_recommendation(self) -> None:
        current = replace(
            position(),
            recommended_stop=Decimal("99"),
            user_confirmed_stop=Decimal("97"),
        )

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark("100.50", current=current),
            policy_fixture(),
        )

        self.assertEqual(action.status, "PROVISIONAL_HOLD")
        self.assertEqual(action.recommended_stop, Decimal("99"))
        self.assertEqual(action.user_confirmed_stop, Decimal("97"))
        self.assertIn("USER_STOP_WIDER_THAN_RECOMMENDED", action.reason_codes)

    def test_averaging_down_and_every_other_addition_are_prohibited(self) -> None:
        current = position()

        down = evaluate_position_addition(current, Decimal("99"), 1)
        up = evaluate_position_addition(current, Decimal("101"), 1)

        self.assertFalse(down.allowed)
        self.assertEqual(
            down.reason_codes,
            ("POSITION_ADDITIONS_PROHIBITED", "AVERAGING_DOWN_PROHIBITED"),
        )
        self.assertFalse(up.allowed)
        self.assertEqual(up.reason_codes, ("POSITION_ADDITIONS_PROHIBITED",))

    def test_one_r_uses_actual_fill_and_tightens_only_to_entry(self) -> None:
        current = position(entry="101")

        below_actual_one_r = risk_module.evaluate_position_diagnostic(
            current,
            mark("103.99", current=current),
            policy_fixture(),
        )
        at_actual_one_r = risk_module.evaluate_position_diagnostic(
            current,
            mark("104", current=current),
            policy_fixture(),
        )

        self.assertEqual(below_actual_one_r.status, "PROVISIONAL_HOLD")
        self.assertEqual(at_actual_one_r.status, "PROVISIONAL_TIGHTEN_STOP")
        self.assertEqual(at_actual_one_r.recommended_stop, Decimal("101"))
        self.assertEqual(at_actual_one_r.published_target, Decimal("104"))

    def test_one_r_rounding_noop_is_hold_not_tighten(self) -> None:
        current = replace(
            position(entry="100.005"),
            recommended_stop=Decimal("100.00"),
            user_confirmed_stop=Decimal("100.00"),
        )

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark("102.01", current=current),
            policy_fixture(),
        )

        self.assertEqual(action.status, "PROVISIONAL_HOLD")
        self.assertEqual(action.recommended_stop, current.recommended_stop)
        self.assertNotIn("ONE_R_REACHED", action.reason_codes)

    def test_persisted_position_money_is_canonical_microdollars(self) -> None:
        current = position()
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        review_session = date(2026, 8, 14)
        event_context = risk_module._issue_position_event_context(
            position=current,
            event_exit_required=False,
            thesis_invalidated=False,
            at=aware_et(review_session, "15:30"),
            cursor=1,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("100"),
            previous_session_low=Decimal("99"),
            current_session_low=Decimal("99.50"),
            atr14=Decimal("2"),
        )
        current_mark = MarketMark(
            price=Decimal("100"),
            at=aware_et(review_session, "15:30"),
            holding_sessions=5,
            previous_session_low=Decimal("99"),
            current_session_low=Decimal("99.50"),
            atr14=Decimal("2"),
        )
        action = risk_module.PositionAction(
            status="PROVISIONAL_HOLD",
            reason_codes=(),
            recommended_stop=Decimal("98"),
            user_confirmed_stop=Decimal("98"),
            published_target=Decimal("104"),
            shares_to_exit=0,
            remaining_shares=current.shares,
            r_multiple=Decimal("1"),
        )
        values = (
            current.entry,
            current.initial_stop,
            current.recommended_stop,
            current.user_confirmed_stop,
            current.target,
            current.tick_size,
            event_context.price,
            event_context.previous_session_low,
            event_context.current_session_low,
            event_context.atr14,
            current_mark.price,
            current_mark.previous_session_low,
            current_mark.current_session_low,
            current_mark.atr14,
            action.recommended_stop,
            action.user_confirmed_stop,
            action.published_target,
        )

        self.assertTrue(
            all(
                value is not None and value.as_tuple().exponent == -6
                for value in values
            ),
        )

    def test_two_r_whole_share_actions_for_one_two_and_three_shares(self) -> None:
        expected_exit_shares = {1: 1, 2: 1, 3: 1}
        expected_remaining = {1: 0, 2: 1, 3: 2}
        for shares in (1, 2, 3):
            with self.subTest(shares=shares):
                action = risk_module.evaluate_position_diagnostic(
                    position(shares=shares),
                    mark(
                        "104",
                        current=position(shares=shares),
                        previous_low="103",
                        current_low="103.50",
                        atr14="2",
                    ),
                    policy_fixture(),
                )
                self.assertEqual(action.status, "PROVISIONAL_EXIT")
                self.assertEqual(action.shares_to_exit, expected_exit_shares[shares])
                self.assertEqual(action.remaining_shares, expected_remaining[shares])
                self.assertEqual(action.published_target, Decimal("104"))
                if shares == 1:
                    self.assertEqual(action.recommended_stop, Decimal("98"))
                else:
                    self.assertEqual(action.recommended_stop, Decimal("102.80"))

    def test_two_r_exits_full_position_when_trail_would_not_tighten(self) -> None:
        current = replace(position(shares=3), recommended_stop=Decimal("103"))

        action = risk_module.evaluate_position_diagnostic(
            current,
            mark(
                "104",
                current=current,
                previous_low="103",
                current_low="103.50",
                atr14="2",
            ),
            policy_fixture(),
        )

        self.assertEqual(action.shares_to_exit, 3)
        self.assertEqual(action.remaining_shares, 0)
        self.assertEqual(action.recommended_stop, Decimal("103"))

    def test_event_thesis_and_ten_session_boundaries_exit(self) -> None:
        cases = (
            (mark("100", event_exit_required=True), "EVENT_EXIT_REQUIRED"),
            (mark("100", thesis_invalidated=True), "THESIS_INVALIDATED"),
            (
                mark("100", holding_sessions=MAX_HOLD_SESSIONS),
                "MAX_HOLD_SESSIONS_REACHED",
            ),
        )
        for current_mark, reason_code in cases:
            with self.subTest(reason_code=reason_code):
                action = risk_module.evaluate_position_diagnostic(
                    position(),
                    current_mark,
                    policy_fixture(),
                )
                self.assertEqual(action.status, "PROVISIONAL_EXIT")
                self.assertEqual(action.shares_to_exit, 5)
                self.assertIn(reason_code, action.reason_codes)

        day_nine = risk_module.evaluate_position_diagnostic(
            position(),
            mark("100", holding_sessions=MAX_HOLD_SESSIONS - 1),
            policy_fixture(),
        )
        self.assertEqual(day_nine.status, "PROVISIONAL_HOLD")

    def test_gap_through_recommended_stop_exits_full_position(self) -> None:
        action = risk_module.evaluate_position_diagnostic(
            position(),
            mark("97"),
            policy_fixture(),
        )

        self.assertEqual(action.status, "PROVISIONAL_EXIT")
        self.assertEqual(action.shares_to_exit, 5)
        self.assertIn("RECOMMENDED_STOP_REACHED", action.reason_codes)

    def test_unverified_event_or_session_context_fails_closed(self) -> None:
        unverified = replace(mark("100"), context_verified=False)

        action = evaluate_position(position(), unverified, policy_fixture())

        self.assertEqual(action.status, "POSITION_UNVERIFIED")
        self.assertEqual(action.reason_codes, ("POSITION_CONTEXT_UNVERIFIED",))
        self.assertEqual(action.shares_to_exit, 0)

        unverified_count = replace(mark("100"), holding_sessions_verified=False)
        count_action = evaluate_position(
            position(),
            unverified_count,
            policy_fixture(),
        )
        self.assertEqual(count_action.status, "POSITION_UNVERIFIED")

    def test_missing_user_stop_does_not_suppress_one_or_two_r_actions(self) -> None:
        current = replace(position(), user_confirmed_stop=None)

        one_r = risk_module.evaluate_position_diagnostic(
            current,
            mark("102", current=current),
            policy_fixture(),
        )
        two_r = risk_module.evaluate_position_diagnostic(
            current,
            mark("104", current=current),
            policy_fixture(),
        )

        self.assertEqual(one_r.status, "PROVISIONAL_TIGHTEN_STOP")
        self.assertEqual(one_r.recommended_stop, current.entry)
        self.assertIn("ONE_R_REACHED", one_r.reason_codes)
        self.assertIn("STOP_UNVERIFIED", one_r.reason_codes)
        self.assertEqual(two_r.status, "PROVISIONAL_EXIT")
        self.assertEqual(
            two_r.r_multiple.as_tuple(),
            Decimal("2.000000").as_tuple(),
        )
        self.assertEqual(two_r.shares_to_exit, current.shares)
        self.assertIn("TWO_R_REACHED", two_r.reason_codes)
        self.assertIn("STOP_UNVERIFIED", two_r.reason_codes)

    def test_market_mark_builder_derives_inclusive_sessions_from_reviewed_calendar(self) -> None:
        current = position()
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        event_context = risk_module._issue_position_event_context(
            position=current,
            event_exit_required=False,
            thesis_invalidated=False,
            at=aware_et(date(2026, 8, 14), "15:30"),
            cursor=1,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("100"),
        )

        current_mark = build_market_mark(
            current,
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=event_context,
        )

        self.assertEqual(current_mark.holding_sessions, MAX_HOLD_SESSIONS)
        self.assertTrue(current_mark.holding_sessions_verified)
        action = risk_module.evaluate_position_diagnostic(
            current,
            current_mark,
            policy_fixture(),
        )
        self.assertIn("MAX_HOLD_SESSIONS_REACHED", action.reason_codes)

    def test_caller_cannot_self_attest_holding_session_authority(self) -> None:
        forged = MarketMark(
            price=Decimal("104"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            holding_sessions=MAX_HOLD_SESSIONS,
            holding_sessions_verified=True,
        )

        action = evaluate_position(position(), forged, policy_fixture())

        self.assertEqual(action.status, "POSITION_UNVERIFIED")
        self.assertEqual(action.reason_codes, ("POSITION_CONTEXT_UNVERIFIED",))

    def test_market_mark_authority_is_identity_and_fingerprint_bound(self) -> None:
        authentic = mark("100", holding_sessions=MAX_HOLD_SESSIONS)

        for forged in (
            copy.copy(authentic),
            replace(authentic, holding_sessions=1),
            replace(authentic, event_exit_required=False),
        ):
            with self.subTest(forged=forged):
                action = evaluate_position(position(), forged, policy_fixture())
                self.assertEqual(action.status, "POSITION_UNVERIFIED")
                self.assertEqual(
                    action.reason_codes,
                    ("POSITION_CONTEXT_UNVERIFIED",),
                )

    def test_raw_event_context_true_cannot_self_authorize_market_mark(self) -> None:
        raw = build_market_mark(
            position(),
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
        )

        action = evaluate_position(position(), raw, policy_fixture())

        self.assertEqual(action.status, "POSITION_UNVERIFIED")

    def test_task6_field_only_event_context_cannot_authorize_market_mark(self) -> None:
        current = position()
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        event_context = risk_module._issue_position_event_context(
            position=current,
            event_exit_required=False,
            thesis_invalidated=False,
            at=aware_et(date(2026, 8, 14), "15:30"),
            cursor=20,
            start_cursor=1,
            event_count=0,
            calendar_resolver=resolver,
            price=Decimal("100"),
        )

        authentic = build_market_mark(
            current,
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "15:30"),
            calendar_resolver=resolver,
            event_exit_required=False,
            thesis_invalidated=False,
            event_context_verified=True,
            position_event_context=event_context,
        )

        action = evaluate_position(current, authentic, policy_fixture())

        self.assertFalse(
            risk_module.is_issued_position_event_context(event_context)
        )
        self.assertFalse(risk_module.is_issued_market_mark(authentic))
        self.assertEqual(action.status, "POSITION_UNVERIFIED")

    def test_explicit_diagnostic_evaluator_preserves_position_formula(self) -> None:
        current = position()
        current_mark = mark(
            "104",
            current=current,
            holding_sessions=MAX_HOLD_SESSIONS,
        )

        action = risk_module.evaluate_position_diagnostic(
            current,
            current_mark,
            policy_fixture(),
        )

        self.assertEqual(action.status, "PROVISIONAL_EXIT")
        self.assertIn("MAX_HOLD_SESSIONS_REACHED", action.reason_codes)


if __name__ == "__main__":
    unittest.main()
