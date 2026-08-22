from __future__ import annotations

import io
import json
import os
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from contextlib import redirect_stdout

import stock_monitor.cli as cli
from stock_monitor.provider_smoke import ProviderSmokeResult
from stock_monitor.workflows import WorkflowResult


ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
CANARY = "PROVIDER_EXCEPTION_CANARY_MUST_NOT_PRINT"


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 8, 22, 13, 0, tzinfo=UTC)
        return value if tz is None else value.astimezone(tz)


def _environment(home: Path) -> dict[str, str]:
    return {
        "APCA_API_KEY_ID": "CANARY_KEY_MUST_NOT_PRINT",
        "APCA_API_SECRET_KEY": "CANARY_SECRET_MUST_NOT_PRINT",
        "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
        "STOCK_MONITOR_HOME": str(home),
    }


class ProviderCliTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.environment = _environment(self.home)

    def test_provider_smoke_emits_only_safe_json_and_result_exit(self) -> None:
        result = ProviderSmokeResult(
            status="READY",
            exit_code=0,
            authentication_ok=True,
            historical_sip_ok=True,
            latest_iex_fresh=True,
            observed_at=NOW,
            reason_codes=(),
        )
        output = io.StringIO()
        with patch("stock_monitor.cli.run_provider_smoke", return_value=result), redirect_stdout(
            output
        ):
            code = cli.run(("provider", "smoke", "--json"), environ=self.environment)

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), result.safe_fields())
        combined = output.getvalue()
        self.assertNotIn(self.environment["APCA_API_KEY_ID"], combined)
        self.assertNotIn(self.environment["APCA_API_SECRET_KEY"], combined)

    def test_expected_provider_failure_returns_exit_three(self) -> None:
        result = ProviderSmokeResult(
            status="BLOCKED_CONNECTIVITY",
            exit_code=3,
            authentication_ok=False,
            historical_sip_ok=False,
            latest_iex_fresh=False,
            observed_at=NOW,
            reason_codes=("CONNECTIVITY_UNAVAILABLE",),
        )
        output = io.StringIO()
        with patch("stock_monitor.cli.run_provider_smoke", return_value=result), redirect_stdout(
            output
        ):
            code = cli.run(("provider", "smoke", "--json"), environ=self.environment)

        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue()), result.safe_fields())

    def test_provider_cli_unexpected_canary_exception_is_generic_exit_ten(self) -> None:
        output = io.StringIO()
        with patch(
            "stock_monitor.cli.run_provider_smoke",
            side_effect=RuntimeError(CANARY),
        ), redirect_stdout(output):
            code = cli.run(("provider", "smoke", "--json"), environ=self.environment)

        self.assertEqual(code, 10)
        combined = output.getvalue()
        self.assertIn("INTERNAL ERROR", combined)
        self.assertNotIn(CANARY, combined)
        self.assertNotIn(self.environment["APCA_API_KEY_ID"], combined)
        self.assertNotIn(self.environment["APCA_API_SECRET_KEY"], combined)

    def test_main_delegates_provider_smoke_instead_of_existing_exit_five(self) -> None:
        result = ProviderSmokeResult(
            status="READY",
            exit_code=0,
            authentication_ok=True,
            historical_sip_ok=True,
            latest_iex_fresh=True,
            observed_at=NOW,
            reason_codes=(),
        )
        output = io.StringIO()
        with patch(
            "stock_monitor.cli.run_provider_smoke",
            return_value=result,
        ), patch.dict(os.environ, self.environment, clear=True), redirect_stdout(output):
            code = cli.main(("provider", "smoke", "--json"))

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), result.safe_fields())

    def test_verify_evidence_loads_release_without_network_or_journal(self) -> None:
        output = io.StringIO()
        with patch.object(cli, "datetime", _FrozenDateTime), patch.object(
            cli,
            "run_provider_smoke",
            side_effect=AssertionError("provider must not be called"),
        ), patch(
            "stock_monitor.journal.Journal.open",
            side_effect=AssertionError("journal must not be opened"),
        ), redirect_stdout(output):
            code = cli.run(
                ("verify", "evidence", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(
            set(payload),
            {
                "status",
                "release_sha256",
                "universe_checksum",
                "reviewed_at",
                "review_by",
                "symbols",
            },
        )
        self.assertEqual(payload["status"], "VERIFIED")
        self.assertEqual(
            payload["symbols"],
            ["AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"],
        )
        self.assertEqual(payload["reviewed_at"], "2026-08-22T00:40:00Z")
        self.assertEqual(payload["review_by"], "2026-08-23T00:39:58Z")

    def test_verify_evidence_failure_is_redacted_exit_three(self) -> None:
        from stock_monitor.evidence import EvidenceRegistryError

        output = io.StringIO()
        with patch.object(cli, "datetime", _FrozenDateTime), patch(
            "stock_monitor.evidence.load_current_evidence_release",
            side_effect=EvidenceRegistryError(CANARY),
        ), redirect_stdout(output):
            code = cli.run(
                ("verify", "evidence", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 3)
        combined = output.getvalue()
        self.assertIn("DATA UNAVAILABLE", combined)
        self.assertNotIn(CANARY, combined)
        self.assertNotIn(self.environment["APCA_API_KEY_ID"], combined)
        self.assertNotIn(self.environment["APCA_API_SECRET_KEY"], combined)

    def test_canonical_adapter_opens_only_the_shallow_provider_graph(self) -> None:
        from stock_monitor.provider_adapter import ProviderWorkflowAdapter

        settings = object()
        journal = object()
        adapter = object()
        with patch.object(
            ProviderWorkflowAdapter,
            "open",
            return_value=adapter,
        ) as opened:
            result = cli._open_canonical_adapter(
                settings,
                journal,
                now=NOW,
            )

        self.assertIs(result, adapter)
        opened.assert_called_once_with(
            settings=settings,
            journal=journal,
            now=NOW,
        )

    def test_run_without_fixture_selects_only_canonical_dispatch(self) -> None:
        output = io.StringIO()
        adapter = object()
        observed_contexts = []

        def canonical_run(context):
            observed_contexts.append(context)
            return WorkflowResult(
                outcome="MARKET_CLOSED_NOOP",
                message="MARKET CLOSED NOOP",
                exit_code=0,
                reason_codes=("MARKET_CLOSED",),
                execution_mode="CANONICAL",
            )

        with patch.object(cli, "datetime", _FrozenDateTime), patch(
            "stock_monitor.cli._open_canonical_adapter",
            return_value=adapter,
        ) as opened, patch(
            "stock_monitor.cli.run_canonical_premarket",
            side_effect=canonical_run,
        ), patch(
            "stock_monitor.cli.RecordedScenarioAdapter.load",
            side_effect=AssertionError("fixture adapter must not be loaded"),
        ), redirect_stdout(output):
            code = cli.run(
                ("run", "premarket", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["execution_mode"], "CANONICAL")
        self.assertEqual(payload["outcome"], "MARKET_CLOSED_NOOP")
        self.assertEqual(len(observed_contexts), 1)
        self.assertIs(observed_contexts[0].adapter, adapter)
        self.assertIsNone(observed_contexts[0].scheduler)
        self.assertEqual(opened.call_count, 1)

    def test_scheduled_no_fixture_uses_only_canonical_scheduler(self) -> None:
        output = io.StringIO()
        adapter = object()
        observed = []

        def scheduled(kind, now, context):
            observed.append((kind, now, context))
            return WorkflowResult(
                outcome="NOT_DUE_NOOP",
                message="NOT DUE NOOP",
                exit_code=0,
                reason_codes=("NOT_DUE",),
                execution_mode="CANONICAL",
            )

        with patch.object(cli, "datetime", _FrozenDateTime), patch(
            "stock_monitor.cli._open_canonical_adapter",
            return_value=adapter,
        ), patch(
            "stock_monitor.scheduled.run_canonical_scheduled",
            side_effect=scheduled,
        ), patch(
            "stock_monitor.scheduled.run_scheduled",
            side_effect=AssertionError("fixture scheduler must not be used"),
        ), redirect_stdout(output):
            code = cli.run(
                ("run", "premarket", "--scheduled", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())["outcome"], "NOT_DUE_NOOP")
        self.assertEqual(len(observed), 1)
        self.assertIs(observed[0][2].adapter, adapter)
        self.assertIsNotNone(observed[0][2].scheduler)


if __name__ == "__main__":
    unittest.main()
