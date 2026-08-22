"""Recorded end-to-end workflow scenarios for the manual-only monitor."""

from __future__ import annotations

import hashlib
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from stock_monitor.workflows import (
    PublishedWorkflow,
    RecordedScenarioAdapter,
    WorkflowContext,
    WorkflowDataError,
    WorkflowResult,
    run_close,
    run_premarket,
)


SCENARIOS = Path(__file__).parents[1] / "fixtures" / "scenarios"


class _CapturingPublisher:
    def __init__(self) -> None:
        self.results: list[WorkflowResult] = []

    def publish(
        self,
        *,
        kind: str,
        session_date,
        generated_at: datetime,
        result: WorkflowResult,
    ) -> PublishedWorkflow:
        self.results.append(result)
        return PublishedWorkflow(
            report_id=f"report-{kind.lower()}",
            report_row_id=1,
            report_path=f"{session_date.isoformat()}/{kind.lower()}.md",
        )


def _run(name: str, *, close: bool = False) -> tuple[WorkflowResult, _CapturingPublisher]:
    adapter = RecordedScenarioAdapter.load(SCENARIOS / name)
    publisher = _CapturingPublisher()
    context = WorkflowContext(
        adapter=adapter,
        publisher=publisher,
        scheduler=None,
        now=adapter.now,
    )
    result = run_close(context) if close else run_premarket(context)
    return result, publisher


class RecordedScenarioTests(unittest.TestCase):
    def test_unknown_uppercase_data_error_is_replaced_by_safe_fallback(self):
        wrapped = RecordedScenarioAdapter.load(SCENARIOS / "eligible.json")

        class PoisonedAdapter:
            def __getattr__(self, name: str):
                return getattr(wrapped, name)

            def provider_smoke(self) -> None:
                raise WorkflowDataError("UPPERCASE_SECRET_CANARY")

        publisher = _CapturingPublisher()
        result = run_premarket(
            WorkflowContext(
                adapter=PoisonedAdapter(),
                publisher=publisher,
                scheduler=None,
                now=wrapped.now,
            )
        )

        self.assertEqual(result.reason_codes, ("DATA_UNAVAILABLE",))
        self.assertNotIn("UPPERCASE_SECRET_CANARY", result.message)
        self.assertNotIn("UPPERCASE_SECRET_CANARY", str(result.safe_fields()))

    def test_missing_configuration_fails_closed_without_publishing(self):
        result, publisher = _run("missing-configuration.json")

        self.assertEqual(result.exit_code, 2)
        self.assertEqual(result.outcome, "CONFIGURATION_REQUIRED")
        self.assertIn("CONFIGURATION REQUIRED", result.message)
        self.assertNotIn("PRIMARY", result.message)
        self.assertEqual(publisher.results, [])

    def test_closed_market_is_a_successful_no_trade_report(self):
        result, publisher = _run("closed-market.json")

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.outcome, "NO_TRADE")
        self.assertEqual(result.reason_codes, ("MARKET_CLOSED",))
        self.assertEqual(len(publisher.results), 1)

    def test_stale_universe_is_data_unavailable_without_candidate(self):
        result, publisher = _run("stale-universe.json")

        self.assertEqual(result.exit_code, 3)
        self.assertEqual(result.outcome, "DATA_UNAVAILABLE")
        self.assertEqual(result.candidates, ())
        self.assertNotIn("PRIMARY", result.message)
        self.assertEqual(len(publisher.results), 1)

    def test_stale_calendar_is_data_unavailable_without_candidate(self):
        result, publisher = _run("stale-calendar.json")

        self.assertEqual(result.exit_code, 3)
        self.assertEqual(result.outcome, "DATA_UNAVAILABLE")
        self.assertEqual(result.reason_codes, ("STALE_CALENDAR",))
        self.assertEqual(result.candidates, ())
        self.assertEqual(len(publisher.results), 1)

    def test_provider_and_source_failures_never_emit_candidates(self):
        for fixture in ("provider-failure.json", "source-failure.json"):
            with self.subTest(fixture=fixture):
                result, _ = _run(fixture)
                self.assertEqual(result.exit_code, 3)
                self.assertEqual(result.outcome, "DATA_UNAVAILABLE")
                self.assertEqual(result.candidates, ())
                self.assertNotIn("PRIMARY", result.message)

    def test_no_candidates_and_active_breaker_are_no_trade(self):
        expected = {
            "no-candidates.json": "NO_CANDIDATES",
            "active-breaker.json": "ACTIVE_BREAKER",
        }
        for fixture, reason in expected.items():
            with self.subTest(fixture=fixture):
                result, _ = _run(fixture)
                self.assertEqual(result.exit_code, 0)
                self.assertEqual(result.outcome, "NO_TRADE")
                self.assertIn(reason, result.reason_codes)

    def test_eligible_fixture_emits_paper_plan_only(self):
        result, publisher = _run("eligible.json")

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.outcome, "CANDIDATES")
        self.assertEqual(tuple(item.symbol for item in result.candidates), ("AAPL",))
        self.assertIsNotNone(result.report)
        assert result.report is not None
        self.assertEqual(result.message, result.report.body)
        self.assertIn("PLAN ONLY", result.message)
        self.assertIn("AAPL - PRIMARY", result.message)
        self.assertIn("Planned risk: `$25.00`", result.message)
        self.assertIn("[Issuer evidence]", result.message)
        self.assertIn("`FIXTURE`", result.message)
        self.assertEqual(result.execution_mode, "FIXTURE")
        self.assertEqual(len(publisher.results), 1)

    def test_reconciliation_close_is_nonzero_and_contains_no_candidate(self):
        result, publisher = _run("reconciliation.json", close=True)

        self.assertEqual(result.exit_code, 5)
        self.assertEqual(result.outcome, "RECONCILIATION_REQUIRED")
        self.assertEqual(result.candidates, ())
        self.assertEqual(len(publisher.results), 1)

    def test_normal_close_uses_rendered_position_risk_and_evidence_material(self):
        result, publisher = _run("normal-close.json", close=True)

        self.assertIsNotNone(result.report)
        assert result.report is not None
        self.assertEqual(result.message, result.report.body)
        self.assertIn("AAPL", result.message)
        self.assertIn("Estimated unrealized P/L: `+$35.00`", result.message)
        self.assertIn("R multiple: `1.40R`", result.message)
        self.assertIn("[Issuer calendar]", result.message)
        self.assertIn("`FIXTURE`", result.message)
        self.assertEqual(result.execution_mode, "FIXTURE")
        self.assertEqual(len(publisher.results), 1)

    def test_legacy_verified_hold_fixture_keeps_its_original_report_bytes(self):
        result, _ = _run("normal-close.json", close=True)

        assert result.report is not None
        expected_sha256 = (
            "099110826146583330d9c3ae43729ac215a42e18e96a0d866cb974fb81338317"
        )
        self.assertNotIn("- Action:", result.message)
        self.assertNotIn("- Position reasons:", result.message)
        self.assertEqual(
            hashlib.sha256(result.message.encode("utf-8")).hexdigest(),
            expected_sha256,
        )
        self.assertEqual(result.report.content_sha256, expected_sha256)

    def test_unverified_close_fails_closed(self):
        result, _ = _run("unverified-position.json", close=True)

        self.assertEqual(result.exit_code, 4)
        self.assertEqual(result.outcome, "POSITION_UNVERIFIED")
        self.assertIn("pending current Robinhood verification", result.message)

    def test_result_rejects_candidates_on_nonzero_exit(self):
        result, _ = _run("eligible.json")

        with self.assertRaises(ValueError):
            replace(result, exit_code=3)

    def test_fixture_rejects_unexpected_fields(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "unsafe.json"
            path.write_text(
                '{"now":"2026-08-14T08:15:00-04:00","account":{},'
                '"provider_fixture":"READY","evidence_fixture":"READY",'
                '"expected_outcome":"NO_TRADE","secret":"must-not-load"}',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                RecordedScenarioAdapter.load(path)


if __name__ == "__main__":
    unittest.main()
