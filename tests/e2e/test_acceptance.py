"""Recorded-fixture acceptance matrix through the public safe launcher."""

from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).parents[2]
LAUNCHER = ROOT / "scripts" / "run_monitor.sh"
SCENARIOS = ROOT / "tests" / "fixtures" / "scenarios"

_MATRIX = (
    ("eligible", "premarket", "eligible.json", "CANDIDATES"),
    ("no_trade", "premarket", "no-candidates.json", "NO_TRADE"),
    ("early_close", "close", "early-close.json", "HOLD"),
    ("normal_close", "close", "normal-close.json", "HOLD"),
    (
        "reconciliation",
        "close",
        "reconciliation.json",
        "RECONCILIATION_REQUIRED",
    ),
    ("data_failure", "premarket", "provider-failure.json", "DATA_UNAVAILABLE"),
)


def _environment(home: Path) -> dict[str, str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "STOCK_MONITOR_HOME": str(home),
        "APCA_API_KEY_ID": "acceptance-fixture-key",
        "APCA_API_SECRET_KEY": "acceptance-fixture-secret",
        "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
    }
    environment.pop("PYTHONPATH", None)
    return environment


def run_recorded_acceptance_matrix(
    root: Path,
) -> tuple[dict[str, int], dict[str, dict[str, object]]]:
    outcomes: dict[str, int] = {}
    documents: dict[str, dict[str, object]] = {}
    for name, workflow, fixture, expected_outcome in _MATRIX:
        home = root / name
        home.mkdir()
        completed = subprocess.run(
            [
                str(LAUNCHER),
                "run",
                workflow,
                "--fixture",
                str(SCENARIOS / fixture),
                "--json",
            ],
            cwd=home,
            env=_environment(home),
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.stderr:
            raise AssertionError(f"{name} wrote stderr: {completed.stderr}")
        try:
            document = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise AssertionError(
                f"{name} did not return one JSON report: {completed.stdout!r}"
            ) from error
        if not isinstance(document, dict):
            raise AssertionError(f"{name} did not return a JSON object")
        if document.get("outcome") != expected_outcome:
            raise AssertionError(
                f"{name} returned {document.get('outcome')!r}, "
                f"expected {expected_outcome!r}"
            )
        if document.get("exit_code") != completed.returncode:
            raise AssertionError(f"{name} process/result exits disagree")
        outcomes[name] = completed.returncode
        documents[name] = document
    return outcomes, documents


class AcceptanceTests(unittest.TestCase):
    def test_scheduled_prompts_use_only_the_stable_unattended_launcher(
        self,
    ) -> None:
        content = (ROOT / "docs" / "scheduled-prompts.md").read_text(
            encoding="utf-8"
        )
        stable_launcher = (
            "/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/"
            "scripts/run_monitor_unattended.sh"
        )

        self.assertNotIn("/.worktrees/", content)
        self.assertEqual(content.count(stable_launcher), 5)
        self.assertNotIn("scripts/run_monitor.sh", content)

    def test_operator_docs_define_the_literal_private_environment(self) -> None:
        content = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "README.md", ROOT / "docs" / "operations.md")
        )
        folded = " ".join(content.casefold().split())

        self.assertIn("exactly four literal, non-empty values", folded)
        for name in (
            "APCA_API_KEY_ID",
            "APCA_API_SECRET_KEY",
            "SEC_USER_AGENT",
            "STOCK_MONITOR_HOME",
        ):
            with self.subTest(name=name):
                self.assertIn(name, content)
        self.assertIn("mode 0400 or 0600", folded)
        self.assertIn("market-data get boundary", folded)
        self.assertIn("not a provider-side read-only property", folded)
        self.assertNotIn("read-only alpaca", folded)
        self.assertNotIn("<read-only-market-data", folded)
        for secret_name in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
            with self.subTest(secret_name=secret_name):
                self.assertNotRegex(content, rf"{secret_name}\s*=")

    def test_schedule_activation_is_current_sequential_and_fail_closed(
        self,
    ) -> None:
        operations = (ROOT / "docs" / "operations.md").read_text(
            encoding="utf-8"
        )
        prompts = (ROOT / "docs" / "scheduled-prompts.md").read_text(
            encoding="utf-8"
        )
        combined = f"{operations}\n{prompts}"
        folded = " ".join(combined.casefold().split())

        for command in (
            "verify universe --json",
            "verify evidence --json",
            "verify calendar --json",
            "provider smoke --json",
        ):
            with self.subTest(command=command):
                self.assertIn(command, combined)
        for phrase in (
            "active Phase 1 validation authority",
            "real manual premarket",
            "real manual close",
            "reviewed scheduled-mode premarket smoke",
            "reviewed scheduled-mode close smoke",
            "current through the next wake",
            "PHASE1_BLOCKED_SCHEDULED_SMOKE",
            "zero external schedules",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase.casefold(), folded)

        for window in (
            "08:45:00 <= start < 09:00:00 ET",
            "12:30:00 <= start < 12:45:00 ET",
            "15:30:00 <= start < 15:45:00 ET",
        ):
            with self.subTest(window=window):
                self.assertIn(window, operations)
                self.assertIn(window, prompts)
        self.assertIn("no backfill", folded)
        self.assertIn("never request options data", folded)

        ordered_gates = (
            "1. Review the current universe",
            "2. Run provider smoke",
            "3. Create or read back the reviewed Phase 1 window",
            "4. Complete and review the real manual premarket",
            "5. While there are still zero external schedules",
            "6. Only after all five gates pass",
        )
        locations = tuple(operations.index(gate) for gate in ordered_gates)
        self.assertEqual(locations, tuple(sorted(locations)))

    def test_recorded_fixture_acceptance_matrix_has_exact_exit_outcomes(self) -> None:
        with TemporaryDirectory() as temporary:
            outcomes, _ = run_recorded_acceptance_matrix(Path(temporary))

        self.assertEqual(
            outcomes,
            {
                "eligible": 0,
                "no_trade": 0,
                "early_close": 0,
                "normal_close": 0,
                "reconciliation": 5,
                "data_failure": 3,
            },
        )

    def test_nonzero_fixture_outcomes_never_invent_candidates(self) -> None:
        with TemporaryDirectory() as temporary:
            outcomes, documents = run_recorded_acceptance_matrix(Path(temporary))

        for name, exit_code in outcomes.items():
            if exit_code == 0:
                continue
            with self.subTest(name=name):
                self.assertEqual(documents[name].get("candidates"), [])
                self.assertNotIn("PRIMARY", json.dumps(documents[name]))

    def test_operator_docs_lock_the_manual_only_activation_contract(self) -> None:
        documents = {
            "README": ROOT / "README.md",
            "operations": ROOT / "docs" / "operations.md",
            "scheduled prompts": ROOT / "docs" / "scheduled-prompts.md",
        }
        content = {
            name: path.read_text(encoding="utf-8")
            for name, path in documents.items()
        }
        combined = "\n".join(content.values())
        required = (
            "educational",
            "no guarantee",
            "$250",
            "T+1",
            "2–10 trading days",
            "Phase 1",
            "Phase 2",
            (
                "/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/"
                "scripts/run_monitor_unattended.sh"
            ),
            "provider smoke",
            "reconciliation",
            "export",
            "never access Robinhood",
            "never place an order",
            "nonzero",
        )
        folded = combined.casefold()
        for phrase in required:
            with self.subTest(phrase=phrase):
                self.assertIn(phrase.casefold(), folded)
        self.assertIn(
            "run premarket --scheduled --json",
            content["scheduled prompts"],
        )
        self.assertGreaterEqual(
            content["scheduled prompts"].count("run close --scheduled --json"),
            2,
        )
        self.assertIn("content-addressed fixture", folded)
        self.assertIn("canonical operator journal", folded)


if __name__ == "__main__":
    unittest.main()
