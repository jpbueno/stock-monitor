from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import ANY, patch

import stock_monitor.cli as cli
from stock_monitor.evidence_release_workflow import (
    EvidenceCandidateSummary,
    EvidenceInstallSummary,
    EvidenceProposalSummary,
    EvidenceWorkflowError,
)
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

    def test_evidence_parser_accepts_only_the_guarded_command_grammar(self) -> None:
        digest = "a" * 64
        review_input = self.home / "review.json"
        parser = cli.build_parser()

        prepare = parser.parse_args(("evidence", "prepare", "--json"))
        inspect = parser.parse_args(
            (
                "evidence",
                "inspect",
                "--proposal",
                digest,
                "--review-input",
                str(review_input),
                "--json",
            )
        )
        install = parser.parse_args(
            ("evidence", "install", "--candidate", digest, "--json")
        )

        self.assertEqual(
            (prepare.command, prepare.evidence_command),
            ("evidence", "prepare"),
        )
        self.assertEqual(inspect.proposal, digest)
        self.assertEqual(inspect.review_input, review_input)
        self.assertEqual(install.candidate, digest)

    def test_evidence_parser_rejects_noncanonical_digests_and_relative_review_input(
        self,
    ) -> None:
        parser = cli.build_parser()
        cases = (
            (
                "evidence",
                "inspect",
                "--proposal",
                "A" * 64,
                "--review-input",
                str(self.home / "review.json"),
            ),
            (
                "evidence",
                "inspect",
                "--proposal",
                "a" * 63,
                "--review-input",
                str(self.home / "review.json"),
            ),
            (
                "evidence",
                "inspect",
                "--proposal",
                "a" * 64,
                "--review-input",
                "review.json",
            ),
            ("evidence", "install", "--candidate", "g" * 64),
        )

        for arguments in cases:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    parser.parse_args(arguments)
                self.assertEqual(raised.exception.code, 2)

    def test_evidence_prepare_emits_only_the_allowlisted_summary(self) -> None:
        summary = EvidenceProposalSummary(
            status="PREPARED",
            proposal_sha256="a" * 64,
            universe_sha256="b" * 64,
            parent_release_sha256="c" * 64,
            symbols=("AAPL", "NVDA"),
            reason_codes=(),
            proposal_path=self.home / CANARY / "proposal.json",
        )
        output = io.StringIO()

        with patch.object(cli, "datetime", _FrozenDateTime), patch(
            "stock_monitor.evidence_release_workflow.prepare_evidence_proposal",
            return_value=summary,
        ) as prepared, redirect_stdout(output):
            code = cli.run(
                ("evidence", "prepare", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "parent_release_sha256": "c" * 64,
                "proposal_sha256": "a" * 64,
                "reason_codes": [],
                "status": "PREPARED",
                "symbols": ["AAPL", "NVDA"],
                "universe_sha256": "b" * 64,
            },
        )
        prepared.assert_called_once()
        call = prepared.call_args.kwargs
        self.assertEqual(call["project_root"], ROOT)
        self.assertEqual(
            call["state_root"],
            self.home.resolve() / ".stock-monitor",
        )
        self.assertTrue(callable(call["collect"]))
        self.assertTrue(callable(call["collect_sec"]))
        combined = output.getvalue()
        self.assertNotIn(CANARY, combined)
        self.assertNotIn(self.environment["SEC_USER_AGENT"], combined)
        self.assertNotIn(self.environment["APCA_API_KEY_ID"], combined)
        self.assertNotIn(self.environment["APCA_API_SECRET_KEY"], combined)

    def test_evidence_prepare_blocked_preserves_safe_digest_in_one_json(self) -> None:
        summary = EvidenceProposalSummary(
            status="PREPARED_BLOCKED",
            proposal_sha256="a" * 64,
            universe_sha256="b" * 64,
            parent_release_sha256="c" * 64,
            symbols=("AAPL",),
            reason_codes=("SOURCE_COLLECTION_FAILED",),
            proposal_path=self.home / "proposal.json",
        )
        output = io.StringIO()

        with patch.object(cli, "datetime", _FrozenDateTime), patch(
            "stock_monitor.evidence_release_workflow.prepare_evidence_proposal",
            return_value=summary,
        ), redirect_stdout(output):
            code = cli.run(
                ("evidence", "prepare", "--json"),
                environ=self.environment,
            )

        self.assertEqual(code, 3)
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "parent_release_sha256": "c" * 64,
                "proposal_sha256": "a" * 64,
                "reason_codes": ["SOURCE_COLLECTION_FAILED"],
                "status": "PREPARED_BLOCKED",
                "symbols": ["AAPL"],
                "universe_sha256": "b" * 64,
            },
        )

    def test_evidence_inspect_is_network_free_and_emits_exact_safe_json(self) -> None:
        review_path = self.home / f"{CANARY}-review.json"
        summary = EvidenceCandidateSummary(
            status="AWAITING_DIGEST_APPROVAL",
            candidate_sha256="d" * 64,
            proposal_sha256="a" * 64,
            review_input_sha256="e" * 64,
            release_sha256="f" * 64,
            universe_sha256="b" * 64,
            reviewed_at=datetime(2026, 8, 24, 15, 0, tzinfo=UTC),
            review_by=datetime(2026, 8, 25, 14, 59, tzinfo=UTC),
            symbols=("AAPL", "NVDA"),
            coverage=(
                ("AAPL", "BINARY_EVENT", "UNKNOWN"),
                ("NVDA", "BINARY_EVENT", "UNKNOWN"),
            ),
            reason_codes=("RELEVANT_COVERAGE_UNKNOWN",),
            candidate_path=self.home / CANARY / "candidate.json",
        )
        output = io.StringIO()

        with patch(
            "stock_monitor.evidence_release_workflow.inspect_evidence_candidate",
            return_value=summary,
        ) as inspected, patch(
            "stock_monitor.providers.http.HttpGetClient",
            side_effect=AssertionError("HTTP client must not be constructed"),
        ) as http_client, patch(
            "stock_monitor.providers.evidence_sources.EvidenceSourceClient",
            side_effect=AssertionError("source client must not be constructed"),
        ) as source_client, patch(
            "stock_monitor.providers.sec.SecClient",
            side_effect=AssertionError("SEC client must not be constructed"),
        ) as sec_client, redirect_stdout(output):
            code = cli.run(
                (
                    "evidence",
                    "inspect",
                    "--proposal",
                    "a" * 64,
                    "--review-input",
                    str(review_path),
                    "--json",
                ),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "candidate_sha256": "d" * 64,
                "coverage": [
                    ["AAPL", "BINARY_EVENT", "UNKNOWN"],
                    ["NVDA", "BINARY_EVENT", "UNKNOWN"],
                ],
                "proposal_sha256": "a" * 64,
                "reason_codes": ["RELEVANT_COVERAGE_UNKNOWN"],
                "release_sha256": "f" * 64,
                "review_by": "2026-08-25T14:59:00Z",
                "review_input_sha256": "e" * 64,
                "reviewed_at": "2026-08-24T15:00:00Z",
                "status": "AWAITING_DIGEST_APPROVAL",
                "symbols": ["AAPL", "NVDA"],
                "universe_sha256": "b" * 64,
            },
        )
        inspected.assert_called_once_with(
            project_root=ROOT,
            state_root=self.home.resolve() / ".stock-monitor",
            proposal_sha256="a" * 64,
            review_input_path=review_path,
            as_of=ANY,
        )
        http_client.assert_not_called()
        source_client.assert_not_called()
        sec_client.assert_not_called()
        combined = output.getvalue()
        self.assertNotIn(CANARY, combined)
        self.assertNotIn(self.environment["SEC_USER_AGENT"], combined)

    def test_evidence_install_is_network_free_and_emits_exact_safe_json(self) -> None:
        summary = EvidenceInstallSummary(
            status="INSTALLED",
            candidate_sha256="d" * 64,
            release_sha256="f" * 64,
            installed_at=datetime(2026, 8, 24, 16, 0, tzinfo=UTC),
            symbols=("AAPL", "NVDA"),
        )
        output = io.StringIO()

        with patch(
            "stock_monitor.evidence_release_workflow.install_evidence_candidate",
            return_value=summary,
        ) as installed, patch(
            "stock_monitor.providers.http.HttpGetClient",
            side_effect=AssertionError("HTTP client must not be constructed"),
        ) as http_client, patch(
            "stock_monitor.providers.evidence_sources.EvidenceSourceClient",
            side_effect=AssertionError("source client must not be constructed"),
        ) as source_client, patch(
            "stock_monitor.providers.sec.SecClient",
            side_effect=AssertionError("SEC client must not be constructed"),
        ) as sec_client, redirect_stdout(output):
            code = cli.run(
                (
                    "evidence",
                    "install",
                    "--candidate",
                    "d" * 64,
                    "--json",
                ),
                environ=self.environment,
            )

        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "candidate_sha256": "d" * 64,
                "installed_at": "2026-08-24T16:00:00Z",
                "release_sha256": "f" * 64,
                "status": "INSTALLED",
                "symbols": ["AAPL", "NVDA"],
            },
        )
        installed.assert_called_once_with(
            project_root=ROOT,
            state_root=self.home.resolve() / ".stock-monitor",
            candidate_sha256="d" * 64,
            as_of=ANY,
        )
        http_client.assert_not_called()
        source_client.assert_not_called()
        sec_client.assert_not_called()

    def test_evidence_workflow_failures_use_command_specific_redacted_exits(
        self,
    ) -> None:
        review_path = self.home / "review.json"
        cases = (
            (
                "stock_monitor.evidence_release_workflow.prepare_evidence_proposal",
                ("evidence", "prepare", "--json"),
                3,
                "DATA UNAVAILABLE\nNo candidate or action was produced.\n",
            ),
            (
                "stock_monitor.evidence_release_workflow.inspect_evidence_candidate",
                (
                    "evidence",
                    "inspect",
                    "--proposal",
                    "a" * 64,
                    "--review-input",
                    str(review_path),
                    "--json",
                ),
                3,
                "DATA UNAVAILABLE\nNo candidate or action was produced.\n",
            ),
            (
                "stock_monitor.evidence_release_workflow.install_evidence_candidate",
                (
                    "evidence",
                    "install",
                    "--candidate",
                    "d" * 64,
                    "--json",
                ),
                4,
                "VERIFICATION BLOCKED\nNo candidate or action was produced.\n",
            ),
        )

        for target, arguments, expected_code, expected_output in cases:
            with self.subTest(arguments=arguments):
                output = io.StringIO()
                with patch(
                    target,
                    side_effect=EvidenceWorkflowError(CANARY),
                ), redirect_stdout(output):
                    code = cli.run(arguments, environ=self.environment)
                self.assertEqual(code, expected_code)
                self.assertEqual(output.getvalue(), expected_output)
                self.assertNotIn(CANARY, output.getvalue())
                self.assertNotIn(
                    self.environment["APCA_API_SECRET_KEY"],
                    output.getvalue(),
                )

    def test_evidence_unexpected_failure_remains_generic_and_redacted(self) -> None:
        output = io.StringIO()
        with patch(
            "stock_monitor.evidence_release_workflow.inspect_evidence_candidate",
            side_effect=RuntimeError(CANARY),
        ), redirect_stdout(output):
            code = cli.run(
                (
                    "evidence",
                    "inspect",
                    "--proposal",
                    "a" * 64,
                    "--review-input",
                    str(self.home / f"{CANARY}.json"),
                    "--json",
                ),
                environ=self.environment,
            )

        self.assertEqual(code, 10)
        self.assertEqual(
            output.getvalue(),
            "INTERNAL ERROR\nNo candidate or action was produced.\n",
        )
        self.assertNotIn(CANARY, output.getvalue())

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
