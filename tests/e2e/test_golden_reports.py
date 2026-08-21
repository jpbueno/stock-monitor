"""Golden output coverage for deterministic user-facing reports."""

from __future__ import annotations

import unittest
import hashlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from stock_monitor.reports import (
    PremarketCandidate,
    PremarketState,
    ReportMetric,
    ReportSource,
    ScoreComponent,
    ValidationState,
    render_premarket_report,
    render_validation_report,
)


GOLDEN_ROOT = Path(__file__).parents[1] / "fixtures" / "golden_reports"
ET = ZoneInfo("America/New_York")


class GoldenReportTests(unittest.TestCase):
    maxDiff = None

    def assert_golden(self, fixture_name: str, body: str) -> None:
        expected = (GOLDEN_ROOT / fixture_name).read_text(encoding="utf-8")
        self.assertEqual(body, expected)

    def test_premarket_candidate_report_is_a_deterministic_plan(self) -> None:
        state = PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="CANDIDATES",
            reason_codes=("QUALIFIED_PRIMARY_AVAILABLE",),
            candidates=(
                PremarketCandidate(
                    symbol="NVDA",
                    role="PRIMARY",
                    setup="PULLBACK_RECLAIM",
                    score_components=(
                        ScoreComponent("Trend and market regime", 25, 25),
                        ScoreComponent("Relative strength", 20, 20),
                        ScoreComponent("Setup quality", 20, 20),
                        ScoreComponent("Volume confirmation", 11, 15),
                        ScoreComponent("Verified catalyst/context", 10, 10),
                        ScoreComponent("Liquidity and execution", 5, 10),
                    ),
                    trigger=Decimal("181.20"),
                    maximum_entry=Decimal("181.40"),
                    recommended_stop=Decimal("178.90"),
                    target=Decimal("186.40"),
                    shares=5,
                    planned_risk=Decimal("12.50"),
                    provider="ALPACA",
                    feed="SIP",
                    observed_at=datetime(2026, 8, 13, 15, 59, tzinfo=ET),
                    invalidations=(
                        "close below EMA20",
                        "Robinhood spread above 0.25%",
                    ),
                    sources=(
                        ReportSource(
                            "Issuer results",
                            "https://investor.nvidia.com/example",
                        ),
                        ReportSource(
                            "SEC filing",
                            "https://www.sec.gov/Archives/example",
                        ),
                    ),
                ),
            ),
            observation_ids=("obs-market", "obs-evidence"),
            state_hash="1" * 64,
        )

        report = render_premarket_report(state)
        replayed = render_premarket_report(state)

        self.assertEqual(report.kind, "PREMARKET")
        self.assertEqual(report.outcome, "CANDIDATES")
        self.assert_golden("premarket-candidate.md", report.body)
        self.assertEqual(report, replayed)
        self.assertEqual(
            report.content_sha256,
            hashlib.sha256(
                (GOLDEN_ROOT / "premarket-candidate.md").read_bytes()
            ).hexdigest(),
        )

    def test_premarket_no_trade_report_names_the_failed_gate(self) -> None:
        state = PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="NO TRADE",
            reason_codes=("NO_CANDIDATE_PASSED_ALL_GATES",),
            observation_ids=("obs-market",),
            state_hash="2" * 64,
        )

        report = render_premarket_report(state)

        self.assert_golden("premarket-no-trade.md", report.body)

    def test_premarket_data_failure_report_fails_closed(self) -> None:
        state = PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="NO NEW TRADE - DATA UNAVAILABLE",
            reason_codes=("ALPACA_ENTITLEMENT_UNAVAILABLE",),
            observation_ids=("obs-entitlement",),
            state_hash="3" * 64,
        )

        report = render_premarket_report(state)

        self.assert_golden("premarket-data-failure.md", report.body)

    def test_phase1_validation_report_is_golden(self) -> None:
        state = ValidationState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 16, 5, tzinfo=ET),
            mode="PHASE 1",
            outcome="PHASE1_IN_PROGRESS",
            metrics=(
                ReportMetric("Closed primary trades", "7/20"),
                ReportMetric("Elapsed", "12 days / 4 weeks minimum"),
                ReportMetric("Mean net R", "+0.18"),
                ReportMetric("Maximum drawdown", "$84.25 / $250.00"),
                ReportMetric("Rule adherence", "96.0% / 90.0% minimum"),
            ),
            reason_codes=("MINIMUM_SAMPLE_NOT_MET",),
            observation_ids=("obs-phase1",),
            state_hash="4" * 64,
        )

        report = render_validation_report(state)

        self.assert_golden("validation-phase1.md", report.body)

    def test_diagnostic_replay_report_keeps_its_bias_warning(self) -> None:
        state = ValidationState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 16, 5, tzinfo=ET),
            mode="DIAGNOSTIC REPLAY",
            outcome="REPLAY_DIAGNOSTIC_ONLY",
            metrics=(
                ReportMetric("Included sessions", "812"),
                ReportMetric("Excluded sessions", "448"),
                ReportMetric("Point-in-time coverage", "64.44%"),
            ),
            reason_codes=("CURRENT_LIST_SURVIVORSHIP_BIAS",),
            observation_ids=("obs-replay-seal",),
            state_hash="5" * 64,
        )

        report = render_validation_report(state)

        self.assert_golden("validation-replay.md", report.body)

    def test_phase2_paper_report_keeps_live_options_locked(self) -> None:
        state = ValidationState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 16, 5, tzinfo=ET),
            mode="PHASE 2 PAPER",
            outcome="PHASE2_BLOCKED",
            metrics=(
                ReportMetric("Closed paper option trades", "3/20"),
                ReportMetric("Elapsed", "8 days / 4 weeks minimum"),
                ReportMetric("Maximum drawdown", "$41.00 / $250.00"),
                ReportMetric("Rule adherence", "100.0% / 90.0% minimum"),
            ),
            reason_codes=("LIVE_OPTIONS_NOT_AUTHORIZED",),
            observation_ids=("obs-phase2",),
            state_hash="6" * 64,
        )

        report = render_validation_report(state)

        self.assert_golden("validation-phase2-paper.md", report.body)


if __name__ == "__main__":
    unittest.main()
