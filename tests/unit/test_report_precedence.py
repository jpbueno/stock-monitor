"""Locked close-review precedence and wording tests."""

from __future__ import annotations

import itertools
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_monitor.reports import (
    ClosePosition,
    CloseState,
    PremarketCandidate,
    PremarketShadow,
    PremarketState,
    ReportSource,
    ScoreComponent,
    UnverifiedClosePosition,
    render_close_report,
    render_premarket_report,
)
from stock_monitor.workflows import (
    CandidateSummary,
    CloseSnapshot,
    PremarketSnapshot,
    ReportEvidence,
    SessionWindow,
    WorkflowContext,
    WorkflowResult,
    run_close,
)


ET = ZoneInfo("America/New_York")


def primary_candidate(symbol: str = "NVDA") -> PremarketCandidate:
    return PremarketCandidate(
        symbol=symbol,
        role="PRIMARY",
        setup="PULLBACK_RECLAIM",
        score_components=(
            ScoreComponent("Trend and market regime", 25, 25),
            ScoreComponent("Relative strength", 20, 20),
            ScoreComponent("Setup quality", 20, 20),
            ScoreComponent("Volume confirmation", 12, 15),
            ScoreComponent("Verified catalyst/context", 9, 10),
            ScoreComponent("Liquidity and execution", 9, 10),
        ),
        trigger=Decimal("181.20"),
        maximum_entry=Decimal("181.40"),
        recommended_stop=Decimal("178.90"),
        target=Decimal("186.40"),
        shares=5,
        planned_risk=Decimal("12.50"),
        provider="ALPACA",
        feed="SIP",
        observed_at=datetime(2026, 8, 14, 8, 44, tzinfo=ET),
        invalidations=("trigger not reached",),
        sources=(ReportSource("Issuer", "https://investor.nvidia.com/"),),
    )


def shadow_candidate(symbol: str = "AMD") -> PremarketShadow:
    return PremarketShadow(
        symbol=symbol,
        role="WATCHLIST_SHADOW",
        score=Decimal("84"),
        setup="PULLBACK_RECLAIM",
        trigger=Decimal("100"),
    )


def close_state(**changes: object) -> CloseState:
    values: dict[str, object] = {
        "session_date": date(2026, 8, 14),
        "generated_at": datetime(2026, 8, 14, 15, 30, tzinfo=ET),
        "reason_codes": ("POSITION_REVIEW_COMPLETE",),
        "positions": (
            ClosePosition(
                symbol="NVDA",
                shares=5,
                mark=Decimal("184.25"),
                estimated_unrealized_pl=Decimal("14.25"),
                r_multiple=Decimal("1.14"),
                recommended_stop=Decimal("181.40"),
                user_confirmed_stop=Decimal("180.00"),
                target=Decimal("186.40"),
                holding_days=3,
                provider="ALPACA",
                feed="IEX",
                observed_at=datetime(2026, 8, 14, 15, 29, tzinfo=ET),
                upcoming_events=("Earnings after planned exit",),
                evidence=(
                    ReportSource(
                        "Issuer calendar",
                        "https://investor.nvidia.com/calendar",
                    ),
                ),
                action="HOLD",
                reason_codes=("POSITION_REVIEW_COMPLETE",),
            ),
        ),
        "observation_ids": ("obs-close", "obs-position"),
        "state_hash": "a" * 64,
        "reconciliation_required": False,
        "position_verified": True,
        "stop_verified": True,
        "data_available": True,
        "exit_due": False,
        "tighten_stop_due": False,
    }
    values.update(changes)
    return CloseState(**values)  # type: ignore[arg-type]


class ReportPrecedenceTests(unittest.TestCase):
    def test_candidate_role_and_material_types_cannot_cross(self) -> None:
        primary = primary_candidate()
        shadow = shadow_candidate()

        self.assertEqual(CandidateSummary("NVDA", "PRIMARY", primary).material, primary)
        self.assertEqual(
            CandidateSummary("AMD", "WATCHLIST_SHADOW", shadow).material,
            shadow,
        )
        with self.assertRaises(ValueError):
            CandidateSummary("AMD", "PRIMARY", shadow)
        with self.assertRaises(ValueError):
            CandidateSummary("NVDA", "WATCHLIST_SHADOW", primary)
        with self.assertRaises(ValueError):
            replace(primary, role="WATCHLIST_SHADOW")

    def test_watchlist_shadow_has_no_sizing_fields(self) -> None:
        shadow = shadow_candidate()
        state = PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="CANDIDATES",
            reason_codes=("WATCHLIST_SHADOW_AVAILABLE",),
            observation_ids=("obs-shadow",),
            state_hash="b" * 64,
            candidates=(primary_candidate(), shadow),
        )

        report = render_premarket_report(state)

        self.assertIn("AMD - WATCHLIST_SHADOW", report.body)
        self.assertIn("Entry trigger: `$100.00`", report.body)
        self.assertEqual(report.body.count("N/A - WATCHLIST ONLY"), 5)
        for field in (
            "maximum_entry",
            "recommended_stop",
            "target",
            "shares",
            "planned_risk",
        ):
            self.assertFalse(hasattr(shadow, field), field)

    def test_candidate_aggregates_require_one_primary_and_at_most_two_shadows(
        self,
    ) -> None:
        primary = primary_candidate()
        second_primary = primary_candidate("AAPL")
        shadows = tuple(
            shadow_candidate(symbol) for symbol in ("AMD", "MSFT", "META")
        )
        invalid_materials = (
            (shadows[0],),
            (primary, second_primary),
            (primary, *shadows),
        )
        for materials in invalid_materials:
            summaries = tuple(
                CandidateSummary(item.symbol, item.role, item) for item in materials
            )
            with self.subTest(materials=materials):
                with self.assertRaises(ValueError):
                    PremarketState(
                        session_date=date(2026, 8, 14),
                        generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                        outcome="CANDIDATES",
                        reason_codes=("QUALIFIED_PRIMARY_AVAILABLE",),
                        observation_ids=("obs-candidates",),
                        state_hash="d" * 64,
                        candidates=materials,
                    )
                with self.assertRaises(ValueError):
                    PremarketSnapshot(summaries, False)
                with self.assertRaises(ValueError):
                    WorkflowResult(
                        outcome="CANDIDATES",
                        message="PLAN ONLY",
                        exit_code=0,
                        reason_codes=("PAPER_PLAN_ONLY",),
                        candidates=summaries,
                    )

        with self.assertRaises(ValueError):
            WorkflowResult(
                outcome="CANDIDATES",
                message="PLAN ONLY",
                exit_code=0,
                reason_codes=("PAPER_PLAN_ONLY",),
            )

    def test_candidate_aggregates_require_unique_symbols(self) -> None:
        materials = (primary_candidate("NVDA"), shadow_candidate("NVDA"))
        summaries = tuple(
            CandidateSummary(item.symbol, item.role, item) for item in materials
        )

        with self.assertRaises(ValueError):
            PremarketState(
                session_date=date(2026, 8, 14),
                generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                outcome="CANDIDATES",
                reason_codes=("QUALIFIED_PRIMARY_AVAILABLE",),
                observation_ids=("obs-candidates",),
                state_hash="d" * 64,
                candidates=materials,
            )
        with self.assertRaises(ValueError):
            PremarketSnapshot(summaries, False)
        with self.assertRaises(ValueError):
            WorkflowResult(
                outcome="CANDIDATES",
                message="PLAN ONLY",
                exit_code=0,
                reason_codes=("PAPER_PLAN_ONLY",),
                candidates=summaries,
            )

    def test_candidate_aggregates_require_primary_first(self) -> None:
        materials = (shadow_candidate("AMD"), primary_candidate("NVDA"))
        summaries = tuple(
            CandidateSummary(item.symbol, item.role, item) for item in materials
        )

        with self.assertRaises(ValueError):
            PremarketState(
                session_date=date(2026, 8, 14),
                generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                outcome="CANDIDATES",
                reason_codes=("QUALIFIED_PRIMARY_AVAILABLE",),
                observation_ids=("obs-candidates",),
                state_hash="d" * 64,
                candidates=materials,
            )
        with self.assertRaises(ValueError):
            PremarketSnapshot(summaries, False)
        with self.assertRaises(ValueError):
            WorkflowResult(
                outcome="CANDIDATES",
                message="PLAN ONLY",
                exit_code=0,
                reason_codes=("PAPER_PLAN_ONLY",),
                candidates=summaries,
            )

    def test_unverified_close_position_never_fabricates_market_values(self) -> None:
        position = UnverifiedClosePosition(
            symbol="AAPL",
            shares=4,
            exact_cost_basis=Decimal("225.10"),
            status="POSITION_UNVERIFIED",
            reason_codes=("PLAN_LINEAGE_UNAVAILABLE",),
        )
        state = close_state(
            positions=(position,),
            position_verified=False,
            reason_codes=("PLAN_LINEAGE_UNAVAILABLE",),
        )

        report = render_close_report(state)

        self.assertIn("Exact cost basis: `$225.10`", report.body)
        self.assertIn("PLAN LINEAGE UNAVAILABLE", report.body)
        for forbidden in (
            "$0.00",
            "Estimated mark:",
            "Estimated unrealized P/L:",
            "R multiple:",
            "Recommended stop:",
            "User-confirmed stop:",
            "First target:",
        ):
            self.assertNotIn(forbidden, report.body)

    def test_verified_close_position_renders_its_action_and_reasons(self) -> None:
        state = close_state()

        report = render_close_report(state)

        self.assertIn("Action: `HOLD`", report.body)
        self.assertIn("POSITION REVIEW COMPLETE", report.body)

    def test_mixed_position_precedence_includes_data_unavailable(self) -> None:
        verified_exit = replace(
            close_state().positions[0],
            action="EXIT",
            reason_codes=("MAX_HOLD_SESSIONS_REACHED",),
        )
        cases = (
            ("DATA_UNAVAILABLE", "DATA UNAVAILABLE"),
            ("STOP_UNVERIFIED", "STOP UNVERIFIED"),
            ("POSITION_UNVERIFIED", "POSITION UNVERIFIED"),
            ("RECONCILIATION_REQUIRED", "RECONCILIATION REQUIRED"),
        )
        for status, expected in cases:
            with self.subTest(status=status):
                unverified = UnverifiedClosePosition(
                    symbol="AAPL",
                    shares=4,
                    exact_cost_basis=Decimal("225.10"),
                    status=status,
                    reason_codes=(f"{status}_REASON",),
                )
                report = render_close_report(
                    close_state(positions=(verified_exit, unverified))
                )
                self.assertEqual(report.outcome, expected)

    def test_close_snapshot_accepts_data_unavailable_projection(self) -> None:
        position = UnverifiedClosePosition(
            symbol="AAPL",
            shares=4,
            exact_cost_basis=Decimal("225.10"),
            status="DATA_UNAVAILABLE",
            reason_codes=("SIP_MARK_UNAVAILABLE",),
        )

        snapshot = CloseSnapshot("DATA_UNAVAILABLE", (position,))

        self.assertEqual(snapshot.positions, (position,))

    def test_data_unavailable_snapshot_returns_exit_three(self) -> None:
        position = UnverifiedClosePosition(
            symbol="AAPL",
            shares=4,
            exact_cost_basis=Decimal("225.10"),
            status="DATA_UNAVAILABLE",
            reason_codes=("SIP_MARK_UNAVAILABLE",),
        )

        class Adapter:
            def validate_configuration(self) -> None:
                return None

            def market_session(self, day: date) -> SessionWindow:
                return SessionWindow(day, datetime.min.time())

            def verify_universe(self, day: date) -> None:
                del day

            def provider_smoke(self) -> None:
                return None

            def verify_sources(self) -> None:
                return None

            def report_evidence(self) -> ReportEvidence:
                return ReportEvidence(("obs-close",), "c" * 64)

            def close_snapshot(self, day: date) -> CloseSnapshot:
                del day
                return CloseSnapshot("HOLD", (position,))

        result = run_close(
            WorkflowContext(
                adapter=Adapter(),
                publisher=None,
                scheduler=None,
                now=datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            )
        )

        self.assertEqual((result.outcome, result.exit_code), ("DATA_UNAVAILABLE", 3))
        self.assertIn("DATA UNAVAILABLE", result.message)

    def test_report_sources_reject_sensitive_and_obfuscated_query_names(self) -> None:
        poisoned_names = (
            "token",
            "access_token",
            "api_key",
            "apikey",
            "authorization",
            "credential",
            "key_id",
            "password",
            "secret",
            "signature",
            "api-key",
            "x.api.key",
            "key-id",
            "client-secret",
            "auth-token",
            "request-signature",
        )
        for name in poisoned_names:
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError,
                "credential",
            ):
                ReportSource(
                    "Issuer filing",
                    f"https://example.com/filing?{name}=canary",
                )

    def test_each_close_state_has_the_locked_outcome(self) -> None:
        cases = (
            (
                {"reconciliation_required": True},
                "RECONCILIATION REQUIRED",
            ),
            ({"position_verified": False}, "POSITION UNVERIFIED"),
            ({"stop_verified": False}, "STOP UNVERIFIED"),
            (
                {"exit_due": True},
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            (
                {"tighten_stop_due": True},
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            ({}, "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE"),
        )
        for changes, expected in cases:
            with self.subTest(expected=expected):
                report = render_close_report(close_state(**changes))
                self.assertEqual(report.outcome, expected)
                self.assertIn(f"`{expected}`", report.body)

    def test_every_pair_of_conflicts_selects_the_higher_precedence(self) -> None:
        conditions = (
            ("reconciliation_required", True, "RECONCILIATION REQUIRED"),
            ("position_verified", False, "POSITION UNVERIFIED"),
            ("stop_verified", False, "STOP UNVERIFIED"),
            (
                "exit_due",
                True,
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            (
                "tighten_stop_due",
                True,
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
            ),
        )
        for higher, lower in itertools.combinations(conditions, 2):
            with self.subTest(higher=higher[0], lower=lower[0]):
                report = render_close_report(
                    close_state(**{higher[0]: higher[1], lower[0]: lower[1]})
                )
                self.assertEqual(report.outcome, higher[2])

    def test_reconciliation_precedes_exit_and_unverified_stop(self) -> None:
        report = render_close_report(
            close_state(
                reconciliation_required=True,
                exit_due=True,
                stop_verified=False,
            )
        )

        self.assertIn("RECONCILIATION REQUIRED", report.body)
        self.assertNotIn("PROVISIONAL EXIT", report.outcome)

    def test_recommended_and_user_confirmed_stops_are_separate(self) -> None:
        report = render_close_report(close_state(tighten_stop_due=True))

        self.assertIn("Recommended stop: `$181.40`", report.body)
        self.assertIn("User-confirmed stop: `$180.00`", report.body)
        self.assertIn("provisional pending current Robinhood verification", report.body)
        self.assertIn("cannot change or replace the protective stop", report.body)

    def test_unverified_stop_is_never_filled_from_the_recommendation(self) -> None:
        state = close_state(stop_verified=False)
        state = replace(
            state,
            positions=(replace(state.positions[0], user_confirmed_stop=None),),
        )

        report = render_close_report(state)

        self.assertEqual(report.outcome, "STOP UNVERIFIED")
        self.assertIn("Recommended stop: `$181.40`", report.body)
        self.assertIn("User-confirmed stop: `NOT CONFIRMED`", report.body)


if __name__ == "__main__":
    unittest.main()
