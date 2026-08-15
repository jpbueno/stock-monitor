from __future__ import annotations

import copy
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import stock_monitor.risk as risk_module
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from stock_monitor.risk import (
    BreakerState,
    ClosedTrade,
    EquityPoint,
    LOSS_PAUSE_SESSIONS,
    PairedBreakerState,
    RiskBlock,
    SessionCalendarResolver,
    combine_breaker_states,
    evaluate_breakers,
    evaluate_paired_breakers,
)
from tests.support import aware_et, calendar_fixture


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


def equity(
    day: date,
    value: str,
    *,
    at: str | None = None,
    cursor: int | None = None,
    ordinal: int = 0,
    source_id: str | None = None,
) -> EquityPoint:
    effective_at = aware_et(day, at) if at is not None else None
    return EquityPoint(
        session_date=day,
        equity=Decimal(value),
        at=effective_at,
        cursor=cursor,
        ordinal=ordinal,
        source_id=source_id,
        message_time=effective_at,
        received_at=effective_at,
    )


def close(
    day: date,
    pnl: str,
    signal_id: str = "sig",
    *,
    equity_after: str = "5000",
    at: str | None = None,
    cursor: int | None = None,
    source_id: str | None = None,
) -> ClosedTrade:
    effective_at = aware_et(day, at) if at is not None else None
    return ClosedTrade(
        session_date=day,
        pnl=Decimal(pnl),
        signal_id=signal_id,
        equity_after=Decimal(equity_after),
        at=effective_at,
        cursor=cursor,
        source_id=source_id,
        message_time=effective_at,
        received_at=effective_at,
    )


def complete_flat_history(
    resolver: SessionCalendarResolver,
    start: date,
    through: date,
) -> tuple[EquityPoint, ...]:
    points: list[EquityPoint] = []
    current = start
    cursor = 1
    while current <= through:
        if resolver.is_open(current):
            session = resolver.session(current)
            points.append(
                EquityPoint(
                    current,
                    Decimal("5000"),
                    at=aware_et(current, session.close_time.strftime("%H:%M")),
                    cursor=cursor,
                    source_id=f"equity:{current.isoformat()}",
                    message_time=aware_et(
                        current,
                        session.close_time.strftime("%H:%M"),
                    ),
                    received_at=aware_et(
                        current,
                        session.close_time.strftime("%H:%M"),
                    ),
                )
            )
            cursor += 1
        current = current.fromordinal(current.toordinal() + 1)
    return tuple(points)


class CircuitBreakerTests(unittest.TestCase):
    def test_persisted_breaker_money_is_canonical_microdollars(self) -> None:
        state = BreakerState(
            as_of=date(2026, 8, 14),
            live_entries_paused=False,
            reason_codes=(),
            consecutive_losses=0,
            loss_trigger_session=None,
            loss_pause_through=None,
            loss_resume_session=None,
            weekly_high_water=Decimal("5000"),
            weekly_drawdown=Decimal("0"),
            weekly_pause_through=None,
            monthly_high_water=Decimal("5000"),
            monthly_drawdown=Decimal("0"),
            monthly_pause_through=None,
        )

        values = (
            state.weekly_high_water,
            state.weekly_drawdown,
            state.monthly_high_water,
            state.monthly_drawdown,
        )
        self.assertTrue(
            all(
                value is not None and value.as_tuple().exponent == -6
                for value in values
            )
        )

    def test_breaker_history_attests_complete_queries_at_exact_cutoff(
        self,
    ) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        points = complete_flat_history(
            resolver,
            date(2026, 8, 3),
            date(2026, 8, 13),
        )
        late = (
            *points[:-1],
            replace(
                points[-1],
                received_at=aware_et(date(2026, 8, 14), "09:00"),
            ),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^BREAKER_HISTORY_LOOKAHEAD$",
        ):
            risk_module._issue_breaker_history_authority(
                ledger_name="CANONICAL",
                equity=late,
                closes=(),
                through_session=date(2026, 8, 13),
                terminal_cursor=late[-1].cursor,
                calendar_resolver=resolver,
                query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                equity_expected_count=len(late),
                close_expected_count=0,
                close_stream_through_cursor=0,
            )
    def test_task6_breaker_history_builder_is_diagnostic_only(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        points = complete_flat_history(
            resolver,
            date(2026, 8, 3),
            date(2026, 8, 13),
        )

        history = risk_module._issue_breaker_history_authority(
            ledger_name="CANONICAL",
            equity=points,
            closes=(),
            through_session=date(2026, 8, 13),
            terminal_cursor=points[-1].cursor,
            calendar_resolver=resolver,
        )

        self.assertFalse(
            risk_module.is_issued_breaker_history_authority(history)
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^BREAKER_HISTORY_AUTHORITY_UNVERIFIED$",
        ):
            risk_module.evaluate_authorized_breakers(history)

    def test_authorized_breaker_facts_must_be_inside_reviewed_session(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        day = date(2026, 8, 13)
        premarket_points = (
            equity(
                day,
                "5000",
                at="04:00",
                cursor=1,
                source_id="equity:premarket",
            ),
            equity(
                day,
                "5100",
                at="16:00",
                cursor=2,
                source_id="equity:terminal",
            ),
        )
        terminal_points = (
            equity(
                day,
                "5000",
                at="16:00",
                cursor=1,
                source_id="equity:terminal",
            ),
        )
        after_hours_close = (
            close(
                day,
                "-1",
                "sig-after-hours",
                at="16:01",
                cursor=1,
                source_id="close:after-hours",
            ),
        )

        for points, trades in (
            (premarket_points, ()),
            (terminal_points, after_hours_close),
        ):
            with self.subTest(trades=trades), self.assertRaisesRegex(
                RiskBlock,
                "^BREAKER_POINT_OUTSIDE_MARKET_SESSION$",
            ):
                risk_module._issue_breaker_history_authority(
                    ledger_name="ACTUAL",
                    equity=points,
                    closes=trades,
                    through_session=day,
                    terminal_cursor=points[-1].cursor,
                    calendar_resolver=resolver,
                )

    def test_authorized_history_separates_source_order_from_close_economic_order(
        self,
    ) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        points = complete_flat_history(
            resolver,
            date(2026, 8, 3),
            date(2026, 8, 13),
        )
        source_order = (
            close(
                date(2026, 8, 13),
                "-1",
                "sig-later",
                equity_after="4998",
                at="15:59",
                cursor=11,
                source_id="close:reported-first",
            ),
            close(
                date(2026, 8, 12),
                "-1",
                "sig-delayed",
                equity_after="4999",
                at="15:59",
                cursor=12,
                source_id="close:reported-late",
            ),
        )

        history = risk_module._issue_breaker_history_authority(
            ledger_name="ACTUAL",
            equity=points,
            closes=source_order,
            through_session=date(2026, 8, 13),
            terminal_cursor=points[-1].cursor,
            calendar_resolver=resolver,
        )
        state = evaluate_breakers(
            tuple(sorted(history.equity, key=lambda point: point.at)),
            tuple(sorted(history.closes, key=lambda trade: trade.at)),
            history.calendar_resolver,
        )

        self.assertEqual(state.consecutive_losses, 2)
        self.assertEqual(history.closes, source_order)
        self.assertFalse(
            risk_module.is_issued_breaker_history_authority(history)
        )

    def test_complete_terminal_strategy_history_stays_diagnostic_in_task6(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        opening_day = date(2026, 8, 3)
        through_day = date(2026, 8, 13)
        points = complete_flat_history(resolver, opening_day, through_day)

        diagnostic = evaluate_breakers(points, (), resolver)
        self.assertFalse(risk_module.is_issued_breaker_state(diagnostic))
        history = risk_module._issue_breaker_history_authority(
            ledger_name="CANONICAL",
            equity=points,
            closes=(),
            through_session=through_day,
            terminal_cursor=points[-1].cursor,
            calendar_resolver=resolver,
        )
        self.assertFalse(
            risk_module.is_issued_breaker_history_authority(history)
        )
        for forged in (
            history,
            copy.copy(history),
            replace(history, terminal_cursor=1),
        ):
            with self.subTest(forged=forged):
                with self.assertRaisesRegex(
                    RiskBlock,
                    "^BREAKER_HISTORY_AUTHORITY_UNVERIFIED$",
                ):
                    risk_module.evaluate_authorized_breakers(forged)

    def test_truncated_or_nonterminal_breaker_history_cannot_authorize_entry(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        through_day = date(2026, 8, 13)
        for points, reason in (
            (
                (
                    equity(
                        through_day,
                        "4900",
                        at="16:00",
                        cursor=1,
                        source_id="equity:terminal",
                    ),
                ),
                "BREAKER_EQUITY_INCOMPLETE",
            ),
            (
                (
                    equity(
                        through_day,
                        "5000",
                        at="10:00",
                        cursor=1,
                        source_id="equity:terminal",
                    ),
                ),
                "BREAKER_TERMINAL_SESSION_INCOMPLETE",
            ),
        ):
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(RiskBlock, f"^{reason}$"):
                    risk_module._issue_breaker_history_authority(
                        ledger_name="CANONICAL",
                        equity=points,
                        closes=(),
                        through_session=through_day,
                        terminal_cursor=1,
                        calendar_resolver=resolver,
                    )

    def test_paired_breaker_requires_distinct_canonical_and_actual_lineage(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        points = complete_flat_history(
            resolver,
            date(2026, 8, 3),
            date(2026, 8, 13),
        )
        canonical_history = risk_module._issue_breaker_history_authority(
            ledger_name="CANONICAL",
            equity=points,
            closes=(),
            through_session=date(2026, 8, 13),
            terminal_cursor=points[-1].cursor,
            calendar_resolver=resolver,
        )
        actual_history = risk_module._issue_breaker_history_authority(
            ledger_name="ACTUAL",
            equity=points,
            closes=(),
            through_session=date(2026, 8, 13),
            terminal_cursor=points[-1].cursor,
            calendar_resolver=resolver,
        )
        calendar_digest = risk_module._calendar_digest(resolver)
        canonical = replace(
            evaluate_breakers(canonical_history.equity, (), resolver),
            ledger_name="CANONICAL",
            history_digest=canonical_history.source_digest,
            calendar_digest=calendar_digest,
        )
        actual = replace(
            evaluate_breakers(actual_history.equity, (), resolver),
            ledger_name="ACTUAL",
            history_digest=actual_history.source_digest,
            calendar_digest=calendar_digest,
        )

        paired = combine_breaker_states(
            canonical,
            actual,
            as_of=date(2026, 8, 13),
        )
        self.assertFalse(risk_module.is_issued_paired_breaker_state(paired))
        for first, second in (
            (canonical, canonical),
            (actual, actual),
            (actual, canonical),
        ):
            with self.subTest(first=first, second=second):
                confused = combine_breaker_states(
                    first,
                    second,
                    as_of=date(2026, 8, 13),
                )
                self.assertFalse(
                    risk_module.is_issued_paired_breaker_state(confused)
                )

    def test_authorized_history_requires_every_open_session_terminal_equity(self) -> None:
        resolver = SessionCalendarResolver((reviewed_calendar(),))
        sparse = (
            equity(
                date(2026, 8, 3),
                "5000",
                at="16:00",
                cursor=1,
                source_id="equity:2026-08-03",
            ),
            equity(
                date(2026, 8, 13),
                "5000",
                at="16:00",
                cursor=2,
                source_id="equity:2026-08-13",
            ),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^BREAKER_SESSION_COVERAGE_INCOMPLETE$",
        ):
            risk_module._issue_breaker_history_authority(
                ledger_name="CANONICAL",
                equity=sparse,
                closes=(),
                through_session=date(2026, 8, 13),
                terminal_cursor=2,
                calendar_resolver=resolver,
            )

    def test_three_losses_pause_next_five_sessions_and_resume_on_sixth(self) -> None:
        calendar = reviewed_calendar()
        closes = (
            close(date(2026, 8, 6), "-10", "a"),
            close(date(2026, 8, 7), "-10", "b"),
            close(date(2026, 8, 10), "-10", "c"),
        )
        paused_sessions = tuple(
            calendar.add_sessions(date(2026, 8, 10), offset)
            for offset in range(1, LOSS_PAUSE_SESSIONS + 1)
        )
        for day in paused_sessions:
            with self.subTest(day=day):
                state = evaluate_breakers(
                    (
                        equity(date(2026, 8, 5), "5000"),
                        equity(day, "5000"),
                    ),
                    closes,
                    calendar,
                )
                self.assertTrue(state.live_entries_paused)
                self.assertIn("CONSECUTIVE_LOSS_LIMIT", state.reason_codes)
                self.assertEqual(state.loss_pause_through, date(2026, 8, 17))
        resume = calendar.add_sessions(
            date(2026, 8, 10),
            LOSS_PAUSE_SESSIONS + 1,
        )
        state = evaluate_breakers(
            (
                equity(date(2026, 8, 5), "5000"),
                equity(resume, "5000"),
            ),
            closes,
            calendar,
        )
        self.assertFalse(state.live_entries_paused)
        self.assertEqual(state.loss_resume_session, resume)

    def test_non_loss_resets_consecutive_count(self) -> None:
        closes = (
            close(date(2026, 8, 5), "-10", "a"),
            close(date(2026, 8, 6), "-10", "b"),
            close(date(2026, 8, 7), "0", "c"),
            close(date(2026, 8, 10), "-10", "d"),
            close(date(2026, 8, 11), "-10", "e"),
        )

        state = evaluate_breakers(
            (
                equity(date(2026, 8, 4), "5000"),
                equity(date(2026, 8, 12), "5000"),
            ),
            closes,
            reviewed_calendar(),
        )

        self.assertEqual(state.consecutive_losses, 2)
        self.assertNotIn("CONSECUTIVE_LOSS_LIMIT", state.reason_codes)

    def test_duplicate_logical_close_cannot_manufacture_loss_streak(self) -> None:
        duplicate = close(date(2026, 8, 10), "-10", "same-signal")

        with self.assertRaisesRegex(
            RiskBlock,
            "^DUPLICATE_CLOSED_TRADE_SIGNAL$",
        ):
            evaluate_breakers(
                (equity(date(2026, 8, 11), "5000"),),
                (duplicate, duplicate, duplicate),
                reviewed_calendar(),
            )

    def test_close_without_post_close_equity_authority_fails_closed(self) -> None:
        state = evaluate_breakers(
            (
                equity(
                    date(2026, 8, 10),
                    "5000",
                    at="10:00",
                    cursor=1,
                ),
            ),
            (
                ClosedTrade(
                    session_date=date(2026, 8, 11),
                    pnl=Decimal("-100"),
                    signal_id="x",
                    at=aware_et(date(2026, 8, 11), "15:00"),
                    cursor=2,
                ),
            ),
            reviewed_calendar(),
        )

        self.assertTrue(state.live_entries_paused)
        self.assertIn("BREAKER_EQUITY_INCOMPLETE", state.reason_codes)

    def test_period_hwm_uses_last_authoritative_opening_equity(self) -> None:
        weekly = evaluate_breakers(
            (
                equity(date(2026, 8, 7), "5000"),
                equity(date(2026, 8, 10), "4900"),
            ),
            (),
            reviewed_calendar(),
        )
        monthly = evaluate_breakers(
            (
                equity(date(2026, 7, 31), "5000"),
                equity(date(2026, 8, 3), "4750"),
            ),
            (),
            reviewed_calendar(),
        )

        self.assertEqual(weekly.weekly_drawdown, Decimal("100"))
        self.assertIn("WEEKLY_DRAWDOWN_LIMIT", weekly.reason_codes)
        self.assertEqual(monthly.monthly_drawdown, Decimal("250"))
        self.assertIn("MONTHLY_DRAWDOWN_LIMIT", monthly.reason_codes)

    def test_each_new_loss_after_three_extends_five_session_pause(self) -> None:
        closes = tuple(
            close(day, "-1", f"loss-{index}")
            for index, day in enumerate(
                (
                    date(2026, 8, 3),
                    date(2026, 8, 4),
                    date(2026, 8, 5),
                    date(2026, 8, 6),
                    date(2026, 8, 7),
                    date(2026, 8, 10),
                ),
                start=1,
            )
        )

        state = evaluate_breakers(
            (equity(date(2026, 8, 14), "5000"),),
            closes,
            reviewed_calendar(),
        )

        self.assertEqual(state.loss_trigger_session, date(2026, 8, 10))
        self.assertEqual(state.loss_pause_through, date(2026, 8, 17))
        self.assertTrue(state.live_entries_paused)

    def test_weekly_drawdown_is_inclusive_and_resets_at_monday_boundary(self) -> None:
        calendar = reviewed_calendar()
        friday = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(date(2026, 8, 14), "4900"),
            ),
            (),
            calendar,
        )
        next_monday = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(date(2026, 8, 14), "4900"),
                equity(date(2026, 8, 17), "4900"),
            ),
            (),
            calendar,
        )

        self.assertTrue(friday.live_entries_paused)
        self.assertEqual(friday.weekly_drawdown, Decimal("100"))
        self.assertIn("WEEKLY_DRAWDOWN_LIMIT", friday.reason_codes)
        self.assertFalse(next_monday.live_entries_paused)
        self.assertEqual(next_monday.weekly_drawdown, Decimal("0"))

    def test_monthly_drawdown_is_inclusive_and_resets_at_month_boundary(self) -> None:
        calendar = reviewed_calendar()
        august = evaluate_breakers(
            (
                equity(date(2026, 8, 3), "5000"),
                equity(date(2026, 8, 14), "4750"),
            ),
            (),
            calendar,
        )
        september = evaluate_breakers(
            (
                equity(date(2026, 8, 3), "5000"),
                equity(date(2026, 8, 14), "4750"),
                equity(date(2026, 9, 1), "4750"),
            ),
            (),
            calendar,
        )

        self.assertTrue(august.live_entries_paused)
        self.assertEqual(august.monthly_drawdown, Decimal("250"))
        self.assertIn("MONTHLY_DRAWDOWN_LIMIT", august.reason_codes)
        self.assertFalse(september.live_entries_paused)
        self.assertEqual(september.monthly_drawdown, Decimal("0"))

    def test_high_water_updates_and_just_below_threshold_does_not_pause(self) -> None:
        state = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(date(2026, 8, 11), "5050"),
                equity(date(2026, 8, 12), "4950.01"),
            ),
            (),
            reviewed_calendar(),
        )

        self.assertEqual(state.weekly_high_water, Decimal("5050"))
        self.assertEqual(state.weekly_drawdown, Decimal("99.99"))
        self.assertFalse(state.live_entries_paused)

    def test_period_drawdown_pause_latches_after_equity_recovers(self) -> None:
        calendar = reviewed_calendar()
        weekly = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(date(2026, 8, 11), "4900"),
                equity(date(2026, 8, 12), "5000"),
            ),
            (),
            calendar,
        )
        monthly = evaluate_breakers(
            (
                equity(date(2026, 8, 3), "5000"),
                equity(date(2026, 8, 4), "4750"),
                equity(date(2026, 8, 14), "5000"),
            ),
            (),
            calendar,
        )

        self.assertIn("WEEKLY_DRAWDOWN_LIMIT", weekly.reason_codes)
        self.assertEqual(weekly.weekly_drawdown, Decimal("100"))
        self.assertIn("MONTHLY_DRAWDOWN_LIMIT", monthly.reason_codes)
        self.assertEqual(monthly.monthly_drawdown, Decimal("250"))

    def test_multiple_ordered_points_in_one_session_preserve_intraday_high_water(self) -> None:
        day = date(2026, 8, 14)
        points = (
            equity(day, "5000", at="10:00", cursor=1),
            equity(day, "5100", at="12:00", cursor=2),
            equity(day, "5000", at="15:30", cursor=3),
        )

        state = evaluate_breakers(points, (), reviewed_calendar())

        self.assertEqual(state.weekly_high_water, Decimal("5100"))
        self.assertEqual(state.weekly_drawdown, Decimal("100"))
        self.assertTrue(state.live_entries_paused)

    def test_duplicate_or_out_of_order_equity_authority_is_rejected(self) -> None:
        day = date(2026, 8, 14)
        first = equity(day, "5000", at="10:00", cursor=1)
        second = equity(day, "5010", at="11:00", cursor=2)

        with self.assertRaisesRegex(RiskBlock, "^EQUITY_HISTORY_OUT_OF_ORDER$"):
            evaluate_breakers((second, first), (), reviewed_calendar())
        with self.assertRaisesRegex(RiskBlock, "^DUPLICATE_EQUITY_AUTHORITY$"):
            evaluate_breakers((first, first), (), reviewed_calendar())

    def test_pair_combiner_applies_strictest_pause_and_continues_canonical(self) -> None:
        calendar = reviewed_calendar()
        canonical = evaluate_breakers(
            (
                equity(date(2026, 8, 3), "5000"),
                equity(date(2026, 8, 14), "4750"),
            ),
            (),
            calendar,
        )
        actual = evaluate_breakers(
            (equity(date(2026, 8, 14), "5000"),),
            (),
            calendar,
        )

        paired = combine_breaker_states(canonical, actual, as_of=date(2026, 8, 14))

        self.assertTrue(paired.live_entries_paused)
        self.assertTrue(paired.canonical_observations_continue)
        self.assertIn("CANONICAL:MONTHLY_DRAWDOWN_LIMIT", paired.reason_codes)
        self.assertFalse(actual.live_entries_paused)

        evaluated = evaluate_paired_breakers(
            canonical_equity=(
                equity(date(2026, 8, 3), "5000"),
                equity(date(2026, 8, 14), "4750"),
            ),
            canonical_closes=(),
            actual_equity=(equity(date(2026, 8, 14), "5000"),),
            actual_closes=(),
            calendar=calendar,
            as_of=date(2026, 8, 14),
        )
        self.assertEqual(evaluated, paired)

    def test_pair_combiner_fails_closed_on_as_of_mismatch(self) -> None:
        calendar = reviewed_calendar()
        canonical = evaluate_breakers(
            (equity(date(2026, 8, 14), "5000"),),
            (),
            calendar,
        )
        actual = evaluate_breakers(
            (equity(date(2026, 8, 13), "5000"),),
            (),
            calendar,
        )

        paired = combine_breaker_states(canonical, actual, as_of=date(2026, 8, 14))

        self.assertTrue(paired.live_entries_paused)
        self.assertEqual(paired.reason_codes, ("BREAKER_AS_OF_MISMATCH",))

    def test_paired_breaker_cannot_hide_a_paused_child(self) -> None:
        day = date(2026, 8, 14)
        clear = evaluate_breakers((equity(day, "5000"),), (), reviewed_calendar())
        paused = replace(
            clear,
            live_entries_paused=True,
            reason_codes=("WEEKLY_DRAWDOWN_LIMIT",),
        )

        with self.assertRaisesRegex(RiskBlock, "^INVALID_BREAKER_STATE$"):
            PairedBreakerState(
                as_of=day,
                live_entries_paused=False,
                reason_codes=(),
                canonical=paused,
                actual=clear,
            )

    def test_breaker_authority_is_identity_and_fingerprint_bound(self) -> None:
        day = date(2026, 8, 14)
        authentic = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(day, "4900"),
            ),
            (),
            reviewed_calendar(),
        )
        self.assertTrue(authentic.live_entries_paused)

        forged_values = (
            copy.copy(authentic),
            replace(
                authentic,
                live_entries_paused=False,
                reason_codes=(),
                weekly_drawdown=Decimal("0"),
            ),
        )
        for forged in forged_values:
            with self.subTest(forged=forged):
                diagnostic = combine_breaker_states(
                    forged,
                    authentic,
                    as_of=day,
                )
                self.assertFalse(
                    risk_module.is_issued_paired_breaker_state(diagnostic)
                )

        paired = combine_breaker_states(authentic, authentic, as_of=day)
        self.assertFalse(risk_module.is_issued_paired_breaker_state(paired))
        replaced = combine_breaker_states(
            replace(paired.canonical),
            paired.actual,
            as_of=day,
        )
        self.assertFalse(
            risk_module.is_issued_paired_breaker_state(replaced)
        )

    def test_history_beginning_after_first_close_fails_closed(self) -> None:
        day = date(2026, 8, 10)

        state = evaluate_breakers(
            (),
            (close(day, "-100", "first", equity_after="4900"),),
            reviewed_calendar(),
        )

        self.assertTrue(state.live_entries_paused)
        self.assertIn("BREAKER_EQUITY_INCOMPLETE", state.reason_codes)

    def test_breaker_reason_sequences_are_defensively_frozen(self) -> None:
        state = evaluate_breakers(
            (
                equity(date(2026, 8, 10), "5000"),
                equity(date(2026, 8, 14), "4900"),
            ),
            (),
            reviewed_calendar(),
        )
        source_reasons = list(state.reason_codes)

        copied = replace(
            state,
            reason_codes=source_reasons,  # type: ignore[arg-type]
        )
        source_reasons.append("MUTATED")

        self.assertEqual(copied.reason_codes, ("WEEKLY_DRAWDOWN_LIMIT",))

    def test_loss_pause_fails_closed_when_calendar_coverage_ends(self) -> None:
        closes = (
            close(date(2026, 12, 28), "-1", "a"),
            close(date(2026, 12, 29), "-1", "b"),
            close(date(2026, 12, 30), "-1", "c"),
        )

        state = evaluate_breakers(
            (equity(date(2026, 12, 31), "5000"),),
            closes,
            reviewed_calendar(),
        )

        self.assertTrue(state.live_entries_paused)
        self.assertIn("CALENDAR_COVERAGE_MISSING", state.reason_codes)

    def test_loss_pause_crosses_year_with_verified_calendar_resolver(self) -> None:
        resolver = SessionCalendarResolver.for_diagnostics(
            (reviewed_calendar(), reviewed_next_year_calendar())
        )
        closes = (
            close(date(2026, 12, 28), "-1", "a"),
            close(date(2026, 12, 29), "-1", "b"),
            close(date(2026, 12, 30), "-1", "c"),
        )

        paused = evaluate_breakers(
            (
                equity(date(2026, 12, 24), "5000"),
                equity(date(2027, 1, 7), "5000"),
            ),
            closes,
            resolver,
        )
        resumed = evaluate_breakers(
            (
                equity(date(2026, 12, 24), "5000"),
                equity(date(2027, 1, 8), "5000"),
            ),
            closes,
            resolver,
        )

        self.assertTrue(paused.live_entries_paused)
        self.assertEqual(paused.loss_pause_through, date(2027, 1, 7))
        self.assertFalse(resumed.live_entries_paused)
        self.assertEqual(resumed.loss_resume_session, date(2027, 1, 8))


if __name__ == "__main__":
    unittest.main()
