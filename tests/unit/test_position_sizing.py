from __future__ import annotations

import copy
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, fields, replace
from datetime import date, timedelta
from decimal import MAX_PREC, Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

import stock_monitor.risk as risk_module
from stock_monitor.risk import (
    EquityPoint,
    LongPlanRequest,
    PortfolioState,
    RiskBlock,
    SessionCalendarResolver,
    combine_breaker_states,
    evaluate_breakers,
    plan_long,
    size_long,
)
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from tests.support import aware_et, calendar_fixture, policy_fixture, seeded_ledgers
from tests.unit.test_ranking import candidate


def authorized_state(**overrides: object) -> PortfolioState:
    calendar = load_current_market_calendar(
        Path(__file__).resolve().parents[2],
        as_of=date(2026, 8, 14),
    )
    resolver = SessionCalendarResolver((calendar,))
    points_list: list[EquityPoint] = []
    current = date(2026, 8, 3)
    cursor = 1
    while current <= date(2026, 8, 13):
        if resolver.is_open(current):
            session = resolver.session(current)
            points_list.append(
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
        current += timedelta(days=1)
    points = tuple(points_list)
    calendar_digest = risk_module._calendar_digest(resolver)
    canonical = replace(
        evaluate_breakers(points, (), resolver),
        ledger_name="CANONICAL",
        history_digest="c" * 64,
        calendar_digest=calendar_digest,
    )
    actual = replace(
        evaluate_breakers(points, (), resolver),
        ledger_name="ACTUAL",
        history_digest="a" * 64,
        calendar_digest=calendar_digest,
    )
    paired = combine_breaker_states(canonical, actual, as_of=date(2026, 8, 13))
    values: dict[str, object] = {
        "settled_cash": Decimal("5000"),
        "deployed": Decimal("0"),
        "open_risk": Decimal("0"),
        "open_position_count": 0,
        "entries_this_session": 0,
        "settlement_verified": True,
        "breaker_states": (paired,),
        "calendar_resolver": resolver,
    }
    values.update(overrides)
    return PortfolioState(**values)  # type: ignore[arg-type]


def authorized_plan(request: LongPlanRequest):
    policy = policy_fixture()
    basis = authorized_state()
    return risk_module.plan_long_diagnostic(
        request,
        basis,
        policy,
    )


@contextmanager
def issued_actual_entry_authority() -> Iterator[
    risk_module.PortfolioRiskAuthority
]:
    """Issue one exact authority through the typed production boundary."""
    import stock_monitor.ledger as ledger_module

    signal = seeded_ledgers().signals[0]
    as_of = aware_et(signal.publication_session, "10:14")
    state = authorized_state()
    resolver = state.calendar_resolver
    assert resolver is not None
    breaker = state.breaker_states[0]
    cohort = ledger_module.VerifiedLedgerReplayCohort(
        ledger_name="ACTUAL",
        references=(),
        expected_count=0,
        start_cursor=None,
        terminal_cursor=None,
        query_cutoff=as_of,
        source_digest="a" * 64,
    )
    with patch.object(
        ledger_module,
        "is_issued_verified_replay_cohort",
        return_value=True,
    ):
        pair = ledger_module.LedgerPair(
            signals=(signal,),
            verified_replay_cohorts=(cohort,),
        )
    settlement = risk_module.SettlementLedger(
        Decimal("5000"),
        aware_et(signal.publication_session, "10:10"),
        resolver,
    )
    refresh = risk_module.ActualBreakerRefreshAuthority(
        as_of=as_of,
        through_execution_cursor=12,
        through_close_cursor=0,
        paired_breaker=breaker,
        calendar_digest=risk_module._calendar_digest(resolver),
        source_digest="b" * 64,
    )
    request = LongPlanRequest(
        Decimal("100"),
        signal.recommended_stop,
        signal.tick_size,
        signal.publication_session,
        symbol=signal.symbol,
        published_target=signal.target,
    )
    with (
        patch.object(
            risk_module.SettlementLedger,
            "source_verified",
            new=property(lambda _self: True),
        ),
        patch.object(
            risk_module,
            "is_issued_paired_breaker_state",
            return_value=True,
        ),
        patch.object(
            risk_module,
            "is_issued_actual_breaker_refresh_authority",
            return_value=True,
        ),
    ):
        authority = risk_module._issue_portfolio_risk_authority(
            request=request,
            ledger_pair=pair,
            ledger_name="ACTUAL",
            breaker_state=breaker,
            calendar_resolver=resolver,
            policy=policy_fixture(),
            scope="ACTUAL_ENTRY",
            as_of=as_of,
            settlement_ledger=settlement,
            settled_at=as_of,
            actual_breaker_refresh=refresh,
        )
        yield authority


class SizingTests(unittest.TestCase):
    def test_portfolio_authority_digest_binds_request_and_capacity_state(
        self,
    ) -> None:
        state = authorized_state()
        resolver = state.calendar_resolver
        assert resolver is not None
        policy = policy_fixture()
        spy = LongPlanRequest(
            Decimal("100"),
            Decimal("97.50"),
            Decimal("0.01"),
            date(2026, 8, 14),
            symbol="SPY",
            published_target=Decimal("105"),
        )
        authority = risk_module.PortfolioRiskAuthority(
            request=spy,
            portfolio_state=state,
            scope="ACTUAL_ENTRY",
            as_of=aware_et(spy.session_date, "10:14"),
            ledger_name="ACTUAL",
            projection_through_cursor=None,
            settlement_through_cursor=None,
            projection_digest="1" * 64,
            settlement_source="ACTUAL_SETTLEMENT_LEDGER",
            settlement_digest="2" * 64,
            policy_digest=risk_module._policy_digest(policy),
            calendar_digest=risk_module._calendar_digest(resolver),
            breaker_refresh_digest="4" * 64,
            breaker_refresh_through_execution_cursor=12,
            breaker_refresh_through_close_cursor=0,
        )
        qqq = replace(
            authority,
            request=LongPlanRequest(
                Decimal("50"),
                Decimal("48.75"),
                Decimal("0.01"),
                date(2026, 8, 14),
                symbol="QQQ",
                published_target=Decimal("52.50"),
            ),
        )
        no_cash = replace(
            authority,
            portfolio_state=replace(state, settled_cash=Decimal("0")),
        )

        self.assertEqual(
            len(
                {
                    risk_module._portfolio_authority_digest(value)
                    for value in (authority, qqq, no_cash)
                }
            ),
            3,
        )

    def test_plan_issuer_recomputes_content_from_exact_authority(self) -> None:
        with issued_actual_entry_authority() as authority:
            request = authority.request
            policy = policy_fixture()
            forged = risk_module.LongPlanDecision(
                eligible=True,
                reason_codes=(),
                plan=risk_module.PositionPlan(
                    1,
                    Decimal("100"),
                    Decimal("2.50"),
                ),
                target=Decimal("105"),
                request=request,
                authority_scope="ACTUAL_ENTRY",
                authority_digest=risk_module._portfolio_authority_digest(
                    authority
                ),
                as_of=authority.as_of,
                portfolio_authority=authority,
            )

            with self.assertRaisesRegex(
                RiskBlock,
                "^PLAN_DECISION_CONTENT_MISMATCH$",
            ):
                risk_module._issue_long_plan_decision(
                    forged,
                    authority,
                    policy,
                )

    def test_actual_entry_sizes_prebuy_projection_through_terminal_buy(
        self,
    ) -> None:
        with issued_actual_entry_authority() as authority:
            decision = plan_long(
                authority.request,
                authority.portfolio_state,
                policy_fixture(),
                portfolio_authority=authority,
            )

        self.assertIsNone(authority.projection_through_cursor)
        self.assertIsNone(authority.settlement_through_cursor)
        self.assertEqual(
            authority.breaker_refresh_through_execution_cursor,
            12,
        )
        self.assertTrue(decision.eligible)

    def test_persisted_sizing_money_is_canonical_microdollars(self) -> None:
        request = LongPlanRequest(
            Decimal("100"),
            Decimal("97.50"),
            Decimal("0.01"),
            date(2026, 8, 14),
            symbol="SPY",
            published_target=Decimal("105"),
        )
        plan = size_long(
            Decimal("100"),
            Decimal("97.50"),
            Decimal("5000"),
            Decimal("0"),
            Decimal("0"),
        )
        state = PortfolioState(
            settled_cash=Decimal("5000"),
            deployed=Decimal("0"),
            open_risk=Decimal("0"),
            open_position_count=0,
            entries_this_session=0,
        )

        values = (
            request.entry,
            request.stop,
            request.tick_size,
            request.published_target,
            plan.exposure,
            plan.planned_risk,
            state.settled_cash,
            state.deployed,
            state.open_risk,
        )
        self.assertTrue(
            all(
                value is not None and value.as_tuple().exponent == -6
                for value in values
            )
        )

    def test_long_plan_decision_retains_exact_portfolio_authority(
        self,
    ) -> None:
        self.assertIn(
            "portfolio_authority",
            {field.name for field in fields(risk_module.LongPlanDecision)},
        )

    def test_actual_plan_decision_is_bound_to_exact_authority_digest(
        self,
    ) -> None:
        request = LongPlanRequest(
            Decimal("100"),
            Decimal("97.50"),
            Decimal("0.01"),
            date(2026, 8, 14),
        )
        basis = authorized_state()
        resolver = basis.calendar_resolver
        assert resolver is not None
        as_of = aware_et(request.session_date, "10:14")
        authority = risk_module.PortfolioRiskAuthority(
            request=request,
            portfolio_state=basis,
            scope="ACTUAL_ENTRY",
            as_of=as_of,
            ledger_name="ACTUAL",
            projection_through_cursor=12,
            settlement_through_cursor=12,
            projection_digest="1" * 64,
            settlement_source="ACTUAL_SETTLEMENT_LEDGER",
            settlement_digest="2" * 64,
            policy_digest="3" * 64,
            calendar_digest=risk_module._calendar_digest(resolver),
            breaker_refresh_digest="4" * 64,
            breaker_refresh_through_execution_cursor=12,
            breaker_refresh_through_close_cursor=7,
        )
        forged = risk_module.LongPlanDecision(
            True,
            (),
            size_long(
                Decimal("100"),
                Decimal("97.50"),
                Decimal("5000"),
                Decimal("0"),
                Decimal("0"),
            ),
            Decimal("105"),
            request,
            "ACTUAL_ENTRY",
            "f" * 64,
            as_of,
        )

        with (
            patch.object(
                risk_module,
                "is_issued_portfolio_risk_authority",
                return_value=True,
            ),
            self.assertRaisesRegex(
                RiskBlock,
                "^PORTFOLIO_AUTHORITY_UNVERIFIED$",
            ),
        ):
            risk_module._issue_long_plan_decision(
                forged,
                authority,
                policy_fixture(),
            )

    def test_portfolio_replay_cohort_cutoff_must_equal_authority_as_of(
        self,
    ) -> None:
        import stock_monitor.ledger as ledger_module

        request = LongPlanRequest(
            Decimal("100"),
            Decimal("97.50"),
            Decimal("0.01"),
            date(2026, 8, 14),
        )
        basis = authorized_state()
        resolver = basis.calendar_resolver
        assert resolver is not None
        cohort = ledger_module.VerifiedLedgerReplayCohort(
            ledger_name="CANONICAL",
            references=(),
            expected_count=0,
            start_cursor=None,
            terminal_cursor=None,
            query_cutoff=aware_et(request.session_date, "08:44"),
            source_digest="a" * 64,
        )
        with (
            patch.object(
                ledger_module,
                "is_issued_verified_replay_cohort",
                return_value=True,
            ),
            patch.object(
                risk_module,
                "is_issued_breaker_state",
                return_value=True,
            ),
        ):
            pair = ledger_module.LedgerPair(
                signals=seeded_ledgers().signals,
                verified_replay_cohorts=(cohort,),
            )
            with self.assertRaisesRegex(
                RiskBlock,
                "^PORTFOLIO_REPLAY_CUTOFF_MISMATCH$",
            ):
                risk_module._issue_portfolio_risk_authority(
                    request=request,
                    ledger_pair=pair,
                    ledger_name="CANONICAL",
                    breaker_state=basis.breaker_states[0].canonical,
                    calendar_resolver=resolver,
                    policy=policy_fixture(),
                    scope="CANONICAL_PUBLICATION",
                    as_of=aware_et(request.session_date, "08:45"),
                )

    def test_pair_retains_exact_verified_replay_cohort(self) -> None:
        import stock_monitor.ledger as ledger_module

        cohort = ledger_module.VerifiedLedgerReplayCohort(
            ledger_name="CANONICAL",
            references=(),
            expected_count=0,
            start_cursor=None,
            terminal_cursor=None,
            query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
            source_digest="a" * 64,
        )
        with patch.object(
            ledger_module,
            "is_issued_verified_replay_cohort",
            return_value=True,
        ):
            pair = ledger_module.LedgerPair(
                signals=seeded_ledgers().signals,
                verified_replay_cohorts=(cohort,),
            )

        self.assertIs(pair.canonical_replay_cohort, cohort)

    def test_entry_gate_expires_finished_breaker_windows_but_not_incomplete_history(
        self,
    ) -> None:
        basis = authorized_state()
        resolver = basis.calendar_resolver
        assert resolver is not None
        weekly = evaluate_breakers(
            (
                EquityPoint(date(2026, 8, 10), Decimal("5000")),
                EquityPoint(date(2026, 8, 14), Decimal("4900")),
            ),
            (),
            resolver,
        )
        monthly = evaluate_breakers(
            (
                EquityPoint(date(2026, 8, 3), Decimal("5000")),
                EquityPoint(date(2026, 8, 14), Decimal("4750")),
                EquityPoint(date(2026, 8, 28), Decimal("4750")),
                EquityPoint(date(2026, 8, 31), Decimal("4750")),
            ),
            (),
            resolver,
        )
        losses = tuple(
            risk_module.ClosedTrade(
                day,
                Decimal("-1"),
                signal_id=signal_id,
                equity_after=Decimal("5000"),
            )
            for day, signal_id in (
                (date(2026, 8, 6), "a"),
                (date(2026, 8, 7), "b"),
                (date(2026, 8, 10), "c"),
            )
        )
        consecutive = evaluate_breakers(
            (
                EquityPoint(date(2026, 8, 5), Decimal("5000")),
                EquityPoint(date(2026, 8, 17), Decimal("5000")),
            ),
            losses,
            resolver,
        )

        for breaker, entry_session in (
            (weekly, date(2026, 8, 17)),
            (monthly, date(2026, 9, 1)),
            (consecutive, date(2026, 8, 18)),
        ):
            with self.subTest(entry_session=entry_session):
                request = LongPlanRequest(
                    Decimal("100"),
                    Decimal("97.50"),
                    Decimal("0.01"),
                    entry_session,
                )
                decision = risk_module.plan_long_diagnostic(
                    request,
                    authorized_state(
                        breaker_states=(breaker,),
                        calendar_resolver=resolver,
                    ),
                    policy_fixture(),
                )
                self.assertTrue(decision.eligible)

        incomplete = evaluate_breakers(
            (EquityPoint(date(2026, 8, 14), Decimal("4900")),),
            (),
            resolver,
        )
        blocked = risk_module.plan_long_diagnostic(
            LongPlanRequest(
                Decimal("100"),
                Decimal("97.50"),
                Decimal("0.01"),
                date(2026, 8, 17),
            ),
            authorized_state(
                breaker_states=(incomplete,),
                calendar_resolver=resolver,
            ),
            policy_fixture(),
        )
        self.assertFalse(blocked.eligible)
        self.assertIn("ACTIVE_CIRCUIT_BREAKER", blocked.reason_codes)
    def test_scaled_zero_is_canonicalized_before_decimal_precision_math(self) -> None:
        scaled_zero = Decimal(f"0E-{MAX_PREC}")

        plan = size_long(
            Decimal("100"),
            Decimal("99"),
            Decimal("5000"),
            scaled_zero,
            Decimal("0"),
        )

        self.assertEqual(plan.quantity, 10)

    def test_persisted_integer_boundaries_reject_values_above_int64(self) -> None:
        from stock_monitor.risk import ExecutionEvent

        with self.assertRaisesRegex(RiskBlock, "^INVALID_EXECUTION_SHARES$"):
            ExecutionEvent(
                kind="BUY",
                at=aware_et(date(2026, 8, 14), "10:14"),
                price=Decimal("100"),
                shares=2**63,
            )

    def test_raw_portfolio_flags_and_raw_breakers_never_mint_plan_authority(self) -> None:
        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        raw = authorized_state()

        decision = plan_long(request, raw, policy)

        self.assertFalse(decision.eligible)
        self.assertIn("PORTFOLIO_AUTHORITY_UNVERIFIED", decision.reason_codes)
        self.assertFalse(risk_module.is_issued_long_plan_decision(decision))

    def test_empty_projection_cannot_issue_portfolio_risk_authority(self) -> None:
        from stock_monitor.ledger import LedgerPair

        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        basis = authorized_state()
        with self.assertRaisesRegex(
            RiskBlock,
            "^PORTFOLIO_PROJECTION_UNVERIFIED$",
        ):
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=LedgerPair(signals=seeded_ledgers().signals),
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "08:45"),
            )

        diagnostic = risk_module.plan_long_diagnostic(request, basis, policy)
        self.assertTrue(diagnostic.eligible)
        self.assertFalse(risk_module.is_issued_long_plan_decision(diagnostic))

    def test_portfolio_authority_rejects_unverified_replay_and_lookahead(self) -> None:
        from stock_monitor.ledger import LedgerPair

        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        basis = authorized_state()
        pair = LedgerPair(signals=seeded_ledgers().signals)
        pair.record_canonical_fill(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "09:36"),
        )
        self.assertFalse(pair.replay_verified)
        with self.assertRaisesRegex(
            RiskBlock,
            "^PORTFOLIO_LOOKAHEAD$",
        ):
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=pair,
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "08:45"),
            )
        with self.assertRaisesRegex(
            RiskBlock,
            "^PORTFOLIO_PROJECTION_UNVERIFIED$",
        ):
            prior_pair = LedgerPair(signals=seeded_ledgers().signals)
            prior_pair.record_canonical_fill(
                "sig-1",
                Decimal("100"),
                5,
                aware_et(date(2026, 8, 14), "08:00"),
            )
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=prior_pair,
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "08:45"),
            )

        known_late = LedgerPair(signals=seeded_ledgers().signals)
        known_late.record_canonical_fill(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "08:00"),
        )
        delayed_source_event = replace(
            known_late.events[0],
            message_time=aware_et(date(2026, 8, 14), "09:00"),
            received_at=aware_et(date(2026, 8, 14), "09:01"),
        )
        delayed_source_pair = LedgerPair.rebuild(
            known_late.signals,
            (delayed_source_event,),
        )
        with self.assertRaisesRegex(RiskBlock, "^PORTFOLIO_LOOKAHEAD$"):
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=delayed_source_pair,
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "08:45"),
            )

    def test_portfolio_authority_cutoff_must_match_publication_session(self) -> None:
        from stock_monitor.ledger import LedgerPair

        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        basis = authorized_state()
        for cutoff in (
            aware_et(date(2026, 8, 13), "08:45"),
            aware_et(date(2026, 8, 17), "08:45"),
        ):
            with self.subTest(cutoff=cutoff), self.assertRaisesRegex(
                RiskBlock,
                "^PORTFOLIO_CUTOFF_SESSION_MISMATCH$",
            ):
                risk_module._issue_portfolio_risk_authority(
                    request=request,
                    ledger_pair=LedgerPair(signals=seeded_ledgers().signals),
                    ledger_name="CANONICAL",
                    breaker_state=basis.breaker_states[0].canonical,
                    calendar_resolver=basis.calendar_resolver,
                    policy=policy,
                    scope="CANONICAL_PUBLICATION",
                    as_of=cutoff,
                )
        with self.assertRaisesRegex(
            RiskBlock,
            "^PUBLICATION_CUTOFF_MISMATCH$",
        ):
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=LedgerPair(signals=seeded_ledgers().signals),
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "09:00"),
            )

    def test_actual_only_breaker_pause_does_not_stop_canonical_publication(self) -> None:
        from stock_monitor.ledger import LedgerPair

        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        basis = authorized_state()
        clear_pair = basis.breaker_states[0]
        self.assertIsInstance(clear_pair, risk_module.PairedBreakerState)
        resolver = basis.calendar_resolver
        self.assertIsNotNone(resolver)
        points: list[EquityPoint] = []
        current = date(2026, 8, 3)
        cursor = 1
        while current <= date(2026, 8, 13):
            if resolver.is_open(current):
                session = resolver.session(current)
                points.append(
                    EquityPoint(
                        current,
                        Decimal("4900")
                        if current == date(2026, 8, 13)
                        else Decimal("5000"),
                        at=aware_et(
                            current,
                            session.close_time.strftime("%H:%M"),
                        ),
                        cursor=cursor,
                        source_id=f"actual-equity:{current.isoformat()}",
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
            current += timedelta(days=1)
        paused_actual = replace(
            evaluate_breakers(tuple(points), (), resolver),
            ledger_name="ACTUAL",
            history_digest="b" * 64,
            calendar_digest=risk_module._calendar_digest(resolver),
        )
        strictest = combine_breaker_states(
            clear_pair.canonical,
            paused_actual,
            as_of=date(2026, 8, 13),
        )
        self.assertTrue(strictest.live_entries_paused)

        decision = risk_module.plan_long_diagnostic(
            request,
            PortfolioState(
                settled_cash=Decimal("5000"),
                deployed=Decimal("0"),
                open_risk=Decimal("0"),
                open_position_count=0,
                entries_this_session=0,
                settlement_verified=True,
                breaker_states=(strictest.canonical,),
                calendar_resolver=resolver,
            ),
            policy,
        )

        self.assertTrue(decision.eligible)

    def test_canonical_capacity_ignores_unrelated_actual_event_stream(self) -> None:
        from stock_monitor.ledger import LedgerPair

        policy = policy_fixture()
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        basis = authorized_state()
        pair = LedgerPair(signals=seeded_ledgers().signals)
        pair.record_actual_buy(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )
        self.assertFalse(pair.canonical_replay_verified)
        self.assertFalse(pair.actual_replay_verified)

        with self.assertRaisesRegex(
            RiskBlock,
            "^PORTFOLIO_PROJECTION_UNVERIFIED$",
        ):
            risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=pair,
                ledger_name="CANONICAL",
                breaker_state=basis.breaker_states[0].canonical,
                calendar_resolver=basis.calendar_resolver,
                policy=policy,
                scope="CANONICAL_PUBLICATION",
                as_of=aware_et(request.session_date, "08:45"),
            )

    def test_quantity_respects_both_exposure_and_risk(self) -> None:
        plan = size_long(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            settled_cash=Decimal("5000"),
            deployed=Decimal("0"),
            open_risk=Decimal("0"),
        )

        self.assertEqual(plan.quantity, 10)
        self.assertEqual(plan.exposure, Decimal("1000"))
        self.assertEqual(plan.planned_risk, Decimal("25.00"))

    def test_cent_boundaries_never_breach_locked_caps(self) -> None:
        for entry_cents in range(1001, 1026):
            entry = Decimal(entry_cents) / Decimal("100")
            for distance_cents in range(1, 26):
                distance = Decimal(distance_cents) / Decimal("100")
                plan = size_long(
                    entry=entry,
                    stop=entry - distance,
                    settled_cash=Decimal("5000"),
                    deployed=Decimal("0"),
                    open_risk=Decimal("0"),
                )
                self.assertIs(type(plan.quantity), int)
                self.assertGreaterEqual(plan.quantity, 1)
                self.assertLessEqual(plan.exposure, Decimal("1000"))
                self.assertLessEqual(plan.planned_risk, Decimal("25"))

    def test_nonpositive_distance_and_zero_quantity_have_explicit_codes(self) -> None:
        cases = (
            (
                {
                    "entry": Decimal("100"),
                    "stop": Decimal("100"),
                    "settled_cash": Decimal("5000"),
                    "deployed": Decimal("0"),
                    "open_risk": Decimal("0"),
                },
                "NON_POSITIVE_STOP_DISTANCE",
            ),
            (
                {
                    "entry": Decimal("1000.01"),
                    "stop": Decimal("999.01"),
                    "settled_cash": Decimal("5000"),
                    "deployed": Decimal("0"),
                    "open_risk": Decimal("0"),
                },
                "QUANTITY_BELOW_ONE",
            ),
            (
                {
                    "entry": Decimal("100"),
                    "stop": Decimal("99"),
                    "settled_cash": Decimal("5000"),
                    "deployed": Decimal("1000"),
                    "open_risk": Decimal("0"),
                },
                "EXPOSURE_CAP_REACHED",
            ),
            (
                {
                    "entry": Decimal("100"),
                    "stop": Decimal("99"),
                    "settled_cash": Decimal("5000"),
                    "deployed": Decimal("0"),
                    "open_risk": Decimal("50"),
                },
                "COMBINED_RISK_CAP_REACHED",
            ),
        )
        for kwargs, reason_code in cases:
            with self.subTest(reason_code=reason_code):
                with self.assertRaisesRegex(RiskBlock, f"^{reason_code}$") as raised:
                    size_long(**kwargs)
                self.assertEqual(raised.exception.reason_code, reason_code)

    def test_primitive_rejects_boolean_and_nonfinite_decimals(self) -> None:
        for entry in (True, Decimal("NaN"), Decimal("Infinity")):
            with self.subTest(entry=entry):
                with self.assertRaisesRegex(RiskBlock, "^INVALID_ENTRY$"):
                    size_long(
                        entry=entry,  # type: ignore[arg-type]
                        stop=Decimal("99"),
                        settled_cash=Decimal("5000"),
                        deployed=Decimal("0"),
                        open_risk=Decimal("0"),
                    )

    def test_operand_sized_precision_is_independent_of_ambient_context(self) -> None:
        with localcontext() as context:
            context.prec = 6
            plan = size_long(
                entry=Decimal("100.000001"),
                stop=Decimal("99.000001"),
                settled_cash=Decimal("5000"),
                deployed=Decimal("0"),
                open_risk=Decimal("0"),
            )

        self.assertEqual(plan.quantity, 9)
        self.assertEqual(plan.exposure, Decimal("900.000009"))
        self.assertEqual(plan.planned_risk, Decimal("9.000000"))

    def test_risk_boundaries_reject_unpersistable_submicro_money(self) -> None:
        with self.assertRaisesRegex(RiskBlock, "^INVALID_ENTRY$"):
            size_long(
                entry=Decimal("100.0000001"),
                stop=Decimal("99"),
                settled_cash=Decimal("5000"),
                deployed=Decimal("0"),
                open_risk=Decimal("0"),
            )
        with self.assertRaisesRegex(RiskBlock, "^INVALID_TICK_SIZE$"):
            LongPlanRequest(
                entry=Decimal("100"),
                stop=Decimal("99"),
                tick_size=Decimal("0.0000001"),
                session_date=date(2026, 8, 14),
            )

    def test_context_defaults_fail_closed_without_settlement_or_breaker_authority(self) -> None:
        decision = plan_long(
            LongPlanRequest(
                entry=Decimal("100"),
                stop=Decimal("97.50"),
                tick_size=Decimal("0.01"),
                session_date=date(2026, 8, 14),
            ),
            PortfolioState(
                settled_cash=Decimal("5000"),
                deployed=Decimal("0"),
                open_risk=Decimal("0"),
                open_position_count=0,
                entries_this_session=0,
            ),
            policy_fixture(),
        )

        self.assertFalse(decision.eligible)
        self.assertIn("SETTLEMENT_AUTHORITY_UNVERIFIED", decision.reason_codes)
        self.assertIn("BREAKER_AUTHORITY_UNVERIFIED", decision.reason_codes)

    def test_context_api_owns_tick_and_portfolio_capacity(self) -> None:
        request = LongPlanRequest(
            entry=Decimal("100.01"),
            stop=Decimal("97.51"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        policy = policy_fixture()
        cases = (
            (
                PortfolioState(
                    settled_cash=Decimal("5000"),
                    deployed=Decimal("0"),
                    open_risk=Decimal("0"),
                    open_position_count=2,
                    entries_this_session=0,
                ),
                "POSITION_LIMIT_REACHED",
            ),
            (
                PortfolioState(
                    settled_cash=Decimal("5000"),
                    deployed=Decimal("0"),
                    open_risk=Decimal("0"),
                    open_position_count=0,
                    entries_this_session=1,
                ),
                "SESSION_ENTRY_LIMIT_REACHED",
            ),
            (
                PortfolioState(
                    settled_cash=None,
                    deployed=Decimal("0"),
                    open_risk=Decimal("0"),
                    open_position_count=0,
                    entries_this_session=0,
                ),
                "MISSING_SETTLEMENT_STATE",
            ),
        )
        for state, reason_code in cases:
            with self.subTest(reason_code=reason_code):
                decision = plan_long(request, state, policy)
                self.assertFalse(decision.eligible)
                self.assertIn(reason_code, decision.reason_codes)
                self.assertIsNone(decision.plan)
                self.assertIsNone(decision.target)

    def test_context_api_computes_tick_aligned_target_at_least_exactly_two_r(self) -> None:
        request = LongPlanRequest(
            entry=Decimal("10.03"),
            stop=Decimal("9.99"),
            tick_size=Decimal("0.03"),
            session_date=date(2026, 8, 14),
        )
        # Entry is deliberately not tick aligned and therefore fails closed.
        blocked = authorized_plan(request)
        self.assertEqual(blocked.reason_codes, ("ENTRY_NOT_TICK_ALIGNED",))

        aligned = LongPlanRequest(
            entry=Decimal("10.05"),
            stop=Decimal("9.96"),
            tick_size=Decimal("0.03"),
            session_date=date(2026, 8, 14),
        )
        decision = authorized_plan(aligned)
        self.assertTrue(decision.eligible)
        self.assertEqual(decision.target, Decimal("10.23"))
        self.assertEqual(decision.target % aligned.tick_size, Decimal("0.00"))
        self.assertGreaterEqual(
            decision.target - aligned.entry,
            Decimal("2") * (aligned.entry - aligned.stop),
        )

    def test_context_objects_are_frozen(self) -> None:
        state = PortfolioState(
            settled_cash=Decimal("5000"),
            deployed=Decimal("0"),
            open_risk=Decimal("0"),
            open_position_count=0,
            entries_this_session=0,
        )
        with self.assertRaises(FrozenInstanceError):
            state.deployed = Decimal("100")  # type: ignore[misc]

    def test_context_rejects_untyped_breaker_authority(self) -> None:
        with self.assertRaisesRegex(RiskBlock, "^INVALID_BREAKER_STATE$"):
            PortfolioState(
                settled_cash=Decimal("5000"),
                deployed=Decimal("0"),
                open_risk=Decimal("0"),
                open_position_count=0,
                entries_this_session=0,
                breaker_states=(object(),),
            )

    def test_context_request_consumes_task5_price_contract_without_repricing(self) -> None:
        scored = candidate("AAPL")
        request = LongPlanRequest.from_scored_candidate(scored)

        decision = authorized_plan(request)

        self.assertEqual(request.symbol, "AAPL")
        self.assertEqual(request.entry, scored.maximum_permitted_entry)
        self.assertEqual(request.stop, scored.recommended_stop)
        self.assertEqual(decision.target, scored.target_price)

    def test_context_api_fails_closed_on_unverified_entry_authority(self) -> None:
        request = LongPlanRequest(
            entry=Decimal("100"),
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=date(2026, 8, 14),
        )
        cases = (
            (
                {"settlement_verified": False},
                "SETTLEMENT_AUTHORITY_UNVERIFIED",
            ),
            ({"reconciliation_required": True}, "RECONCILIATION_REQUIRED"),
            ({"stop_unverified": True}, "STOP_UNVERIFIED"),
        )
        for overrides, reason_code in cases:
            values = {
                "settled_cash": Decimal("5000"),
                "deployed": Decimal("0"),
                "open_risk": Decimal("0"),
                "open_position_count": 0,
                "entries_this_session": 0,
                **overrides,
            }
            with self.subTest(reason_code=reason_code):
                decision = plan_long(
                    request,
                    PortfolioState(**values),
                    policy_fixture(),
                )
                self.assertFalse(decision.eligible)
                self.assertIn(reason_code, decision.reason_codes)

    def test_context_api_rejects_closed_or_uncovered_entry_session(self) -> None:
        for session_date, reason in (
            (date(2026, 8, 15), "ENTRY_SESSION_CLOSED"),
            (date(2027, 1, 4), "CALENDAR_COVERAGE_MISSING"),
        ):
            with self.subTest(session_date=session_date):
                request = LongPlanRequest(
                    entry=Decimal("100"),
                    stop=Decimal("97.50"),
                    tick_size=Decimal("0.01"),
                    session_date=session_date,
                )
                decision = plan_long(
                    request,
                    authorized_state(),
                    policy_fixture(),
                )
                self.assertFalse(decision.eligible)
                self.assertIn(reason, decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
