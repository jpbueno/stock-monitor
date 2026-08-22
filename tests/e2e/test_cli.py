"""End-to-end tests for the stable, secret-safe command-line surface."""

from __future__ import annotations

import json
import hashlib
import io
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from contextlib import redirect_stdout
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from stock_monitor.cli import build_parser, main
from stock_monitor.workflows import WorkflowReconciliationError


ROOT = Path(__file__).parents[2]
SCENARIOS = ROOT / "tests" / "fixtures" / "scenarios"


def _env(home: Path, *, credentials: bool = True) -> dict[str, str]:
    result = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(ROOT / "src"),
        "STOCK_MONITOR_HOME": str(home),
    }
    if credentials:
        result.update(
            {
                "APCA_API_KEY_ID": "CANARY_KEY_MUST_NOT_PRINT",
                "APCA_API_SECRET_KEY": "CANARY_SECRET_MUST_NOT_PRINT",
                "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
            }
        )
    return result


def _run(home: Path, *arguments: str, credentials: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-W", "error", "-m", "stock_monitor", *arguments],
        cwd=ROOT,
        env=_env(home, credentials=credentials),
        text=True,
        capture_output=True,
        check=False,
    )


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name)

    def test_parser_covers_every_locked_command(self):
        cases = (
            ("db", "init"),
            ("verify", "universe"),
            ("verify", "calendar"),
            ("provider", "smoke"),
            ("run", "premarket", "--fixture", str(SCENARIOS / "eligible.json")),
            ("run", "close", "--fixture", str(SCENARIOS / "normal-close.json")),
            (
                "confirm",
                "--message-id",
                "msg-1",
                "--message-time",
                "2026-08-14T10:00:00-04:00",
                "--text",
                "SKIPPED AAPL",
            ),
            ("phase1", "status"),
            ("phase1", "start", "--session", "2026-08-21"),
            ("replay", "diagnostic", "--fixture", str(SCENARIOS / "eligible.json")),
            ("replay", "point-in-time", "--fixture", str(SCENARIOS / "eligible.json")),
            ("option-paper", "start", "--fixture", str(SCENARIOS / "eligible.json")),
            ("option-paper", "rank", "--fixture", str(SCENARIOS / "eligible.json")),
            ("option-paper", "status", "--fixture", str(SCENARIOS / "eligible.json")),
            ("export",),
        )
        parser = build_parser()
        for arguments in cases:
            with self.subTest(arguments=arguments):
                namespace = parser.parse_args(arguments)
                self.assertIsNotNone(namespace.command)

    def _run_phase1_start_at(
        self,
        session_text: str,
        observed_at: datetime,
    ) -> tuple[int, dict[str, object] | str]:
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, requested_timezone=None):
                if requested_timezone is None:
                    return observed_at
                return observed_at.astimezone(requested_timezone)

        output = io.StringIO()
        with patch("stock_monitor.cli.datetime", FixedDateTime), patch.dict(
            os.environ,
            _env(self.home),
            clear=True,
        ), redirect_stdout(output):
            code = main(
                ("phase1", "start", "--session", session_text, "--json")
            )
        text = output.getvalue()
        if code == 0:
            return code, json.loads(text)
        return code, text

    def test_phase1_start_reads_back_allowlisted_idempotent_json(self):
        first_code, first = self._run_phase1_start_at(
            "2026-08-21",
            datetime(2026, 8, 21, 20, 1, tzinfo=ZoneInfo("UTC")),
        )
        retry_code, retry = self._run_phase1_start_at(
            "2026-08-21",
            datetime(2026, 8, 21, 21, 1, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual(first_code, 0)
        self.assertEqual(retry_code, 0)
        self.assertIsInstance(first, dict)
        self.assertIsInstance(retry, dict)
        assert isinstance(first, dict)
        assert isinstance(retry, dict)
        self.assertEqual(
            set(first),
            {
                "calendar_digest",
                "duplicate",
                "source_digest",
                "started_session",
                "starting_capital",
                "window_id",
            },
        )
        self.assertEqual(first["starting_capital"], "5000.00")
        self.assertFalse(first["duplicate"])
        self.assertTrue(retry["duplicate"])
        self.assertEqual(first["window_id"], retry["window_id"])
        self.assertEqual(first["source_digest"], retry["source_digest"])

    def test_phase1_start_requires_strict_literal_date(self):
        for session_text in (
            "20260821",
            "2026-W34-5",
            " 2026-08-21",
            "2026-08-21 ",
            "2026-02-30",
        ):
            with self.subTest(session_text=session_text):
                completed = _run(
                    self.home / hashlib.sha256(session_text.encode()).hexdigest(),
                    "phase1",
                    "start",
                    "--session",
                    session_text,
                    "--json",
                )
                self.assertEqual(completed.returncode, 2)
                self.assertEqual(
                    completed.stdout,
                    "CONFIGURATION REQUIRED\n"
                    "No candidate or action was produced.\n",
                )
                self.assertEqual(completed.stderr, "")

    def test_phase1_start_maps_closed_unclosed_and_conflicting_windows_safely(self):
        unclosed_code, unclosed = self._run_phase1_start_at(
            "2026-08-21",
            datetime(2026, 8, 21, 19, 59, tzinfo=ZoneInfo("UTC")),
        )
        closed_code, closed = self._run_phase1_start_at(
            "2026-08-22",
            datetime(2026, 8, 22, 21, 0, tzinfo=ZoneInfo("UTC")),
        )
        first_code, _ = self._run_phase1_start_at(
            "2026-08-20",
            datetime(2026, 8, 21, 21, 0, tzinfo=ZoneInfo("UTC")),
        )
        conflict_code, conflict = self._run_phase1_start_at(
            "2026-08-21",
            datetime(2026, 8, 21, 21, 0, tzinfo=ZoneInfo("UTC")),
        )

        expected = "VERIFICATION BLOCKED\nNo candidate or action was produced.\n"
        self.assertEqual((unclosed_code, unclosed), (4, expected))
        self.assertEqual((closed_code, closed), (4, expected))
        self.assertEqual(first_code, 0)
        self.assertEqual((conflict_code, conflict), (4, expected))

    def test_calendar_json_exposes_exact_current_session_facts(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, timezone=None):
                value = cls(2026, 8, 14, 10, 0, tzinfo=ZoneInfo("America/New_York"))
                return value if timezone is None else value.astimezone(timezone)

        output = io.StringIO()
        with patch("stock_monitor.cli.datetime", FixedDateTime), patch.dict(
            os.environ, _env(self.home), clear=True
        ), redirect_stdout(output):
            code = main(("verify", "calendar", "--json"))

        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "status": "VERIFIED",
                "kind": "CALENDAR",
                "year": 2026,
                "session_date": "2026-08-14",
                "is_open": True,
                "open_time": "09:30",
                "review_time": "15:30",
                "close_time": "16:00",
                "is_early_close": False,
                "timezone": "America/New_York",
            },
        )

    def test_calendar_json_exposes_reviewed_early_close_facts(self):
        from stock_monitor.market_calendar import MarketSession

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, timezone=None):
                value = cls(
                    2026,
                    11,
                    27,
                    10,
                    0,
                    tzinfo=ZoneInfo("America/New_York"),
                )
                return value if timezone is None else value.astimezone(timezone)

        output = io.StringIO()
        calendar = type(
            "ReviewedCalendar",
            (),
            {
                "year": 2026,
                "timezone": ZoneInfo("America/New_York"),
                "is_open": lambda self, day: day == date(2026, 11, 27),
                "session": lambda self, day: MarketSession(
                    session_date=day,
                    open_time=time(9, 30),
                    close_time=time(13, 0),
                    review_time=time(12, 30),
                    timezone=ZoneInfo("America/New_York"),
                    is_early_close=True,
                ),
            },
        )()
        with patch("stock_monitor.cli.datetime", FixedDateTime), patch(
            "stock_monitor.market_calendar.load_current_market_calendar",
            return_value=calendar,
        ), patch.dict(os.environ, _env(self.home), clear=True), redirect_stdout(output):
            code = main(("verify", "calendar", "--json"))

        document = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(document["session_date"], "2026-11-27")
        self.assertTrue(document["is_open"])
        self.assertTrue(document["is_early_close"])
        self.assertEqual(document["open_time"], "09:30")
        self.assertEqual(document["review_time"], "12:30")
        self.assertEqual(document["close_time"], "13:00")

    def test_missing_credentials_returns_configuration_exit_without_candidate(self):
        completed = _run(self.home, "run", "premarket", credentials=False)

        self.assertEqual(completed.returncode, 2)
        self.assertIn("CONFIGURATION REQUIRED", completed.stdout)
        self.assertNotIn("PRIMARY", completed.stdout)
        self.assertEqual(completed.stderr, "")

    def test_fixture_json_is_allowlisted_and_never_prints_canary_secrets(self):
        completed = _run(
            self.home,
            "run",
            "premarket",
            "--fixture",
            str(SCENARIOS / "eligible.json"),
            "--json",
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        document = json.loads(completed.stdout)
        self.assertEqual(document["outcome"], "CANDIDATES")
        self.assertEqual(document["execution_mode"], "FIXTURE")
        self.assertEqual(document["candidates"], [{"role": "PRIMARY", "symbol": "AAPL"}])
        combined = completed.stdout + completed.stderr
        self.assertNotIn("CANARY_KEY_MUST_NOT_PRINT", combined)
        self.assertNotIn("CANARY_SECRET_MUST_NOT_PRINT", combined)
        self.assertNotIn("provider_fixture", completed.stdout)

    def test_same_session_fixtures_use_distinct_content_addressed_sandboxes(self):
        eligible = SCENARIOS / "eligible.json"
        close = SCENARIOS / "normal-close.json"

        first = _run(
            self.home,
            "run",
            "premarket",
            "--fixture",
            str(eligible),
            "--json",
        )
        second = _run(
            self.home,
            "run",
            "close",
            "--fixture",
            str(close),
            "--json",
        )

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        first_document = json.loads(first.stdout)
        second_document = json.loads(second.stdout)
        self.assertEqual(first_document["execution_mode"], "FIXTURE")
        self.assertEqual(second_document["execution_mode"], "FIXTURE")
        first_digest = hashlib.sha256(eligible.read_bytes()).hexdigest()
        second_digest = hashlib.sha256(close.read_bytes()).hexdigest()
        first_root = self.home / ".stock-monitor" / "fixtures" / first_digest
        second_root = self.home / ".stock-monitor" / "fixtures" / second_digest
        self.assertTrue((first_root / "journal.sqlite3").is_file())
        self.assertTrue((second_root / "journal.sqlite3").is_file())
        self.assertNotEqual(first_root, second_root)
        self.assertFalse((self.home / ".stock-monitor" / "journal.sqlite3").exists())
        for document, fixture_root in (
            (first_document, first_root),
            (second_document, second_root),
        ):
            report_path = Path(str(document["report_path"]))
            self.assertTrue(report_path.is_file())
            self.assertTrue(
                report_path.resolve().is_relative_to(fixture_root.resolve())
            )
            self.assertIn("FIXTURE", report_path.read_text(encoding="utf-8"))

    def test_fixture_digest_symlink_is_rejected_before_canonical_journal_open(self):
        import sqlite3

        fixture = SCENARIOS / "eligible.json"
        initialized = _run(self.home, "db", "init", "--json")
        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        state_root = self.home / ".stock-monitor"
        fixtures_root = state_root / "fixtures"
        fixtures_root.mkdir()
        digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
        (fixtures_root / digest).symlink_to(state_root, target_is_directory=True)

        completed = _run(
            self.home,
            "run",
            "premarket",
            "--fixture",
            str(fixture),
            "--json",
        )

        self.assertEqual(completed.returncode, 5, completed.stderr)
        self.assertIn("PAPER ONLY", completed.stdout)
        connection = sqlite3.connect(state_root / "journal.sqlite3")
        try:
            counts = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                for table in (
                    "source_observations",
                    "report_claims",
                    "reports",
                    "outbox",
                )
            }
        finally:
            connection.close()
        self.assertEqual(counts, {table: (0,) for table in counts})

    def test_explicit_scheduled_flag_uses_the_durable_schedule_path(self):
        fixture = SCENARIOS / "eligible.json"

        first = _run(
            self.home,
            "run",
            "premarket",
            "--fixture",
            str(fixture),
            "--scheduled",
            "--json",
        )
        second = _run(
            self.home,
            "run",
            "premarket",
            "--fixture",
            str(fixture),
            "--scheduled",
            "--json",
        )

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(json.loads(first.stdout)["outcome"], "CANDIDATES")
        self.assertEqual(second.returncode, 0, second.stderr)
        second_document = json.loads(second.stdout)
        self.assertEqual(second_document["outcome"], "ALREADY_EMITTED_NOOP")
        self.assertEqual(second_document["execution_mode"], "FIXTURE")
        digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
        journal_path = (
            self.home / ".stock-monitor" / "fixtures" / digest / "journal.sqlite3"
        )
        import sqlite3

        connection = sqlite3.connect(journal_path)
        try:
            scheduled_count = connection.execute(
                "SELECT COUNT(*) FROM scheduled_runs"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(scheduled_count, (1,))

    def test_declared_workflow_failures_map_to_exact_exit_codes(self):
        cases = (
            ("premarket", "stale-universe.json", 3, "DATA_UNAVAILABLE"),
            ("close", "reconciliation.json", 5, "RECONCILIATION_REQUIRED"),
        )
        for workflow, fixture, expected_code, outcome in cases:
            with self.subTest(fixture=fixture):
                completed = _run(
                    self.home / fixture,
                    "run",
                    workflow,
                    "--fixture",
                    str(SCENARIOS / fixture),
                    "--json",
                )
                self.assertEqual(completed.returncode, expected_code, completed.stderr)
                self.assertEqual(json.loads(completed.stdout)["outcome"], outcome)
                self.assertNotIn("PRIMARY", completed.stdout)

    def test_duplicate_confirmation_is_idempotent(self):
        arguments = (
            "confirm",
            "--message-id",
            "msg-skipped-aapl",
            "--message-time",
            "2026-08-14T10:00:00-04:00",
            "--text",
            "SKIPPED AAPL",
            "--json",
        )

        first = _run(self.home, *arguments)
        second = _run(self.home, *arguments)

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertFalse(json.loads(first.stdout)["duplicate"])
        self.assertTrue(json.loads(second.stdout)["duplicate"])

    def test_export_creates_decimal_safe_csv_files(self):
        initialized = _run(self.home, "db", "init", "--json")
        exported = _run(self.home, "export", "--json")

        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        self.assertEqual(exported.returncode, 0, exported.stderr)
        paths = tuple(Path(item) for item in json.loads(exported.stdout)["paths"])
        self.assertTrue(paths)
        self.assertTrue(all(path.is_file() for path in paths))

    def test_live_option_request_is_rejected_at_paper_boundary(self):
        completed = _run(
            self.home,
            "option-paper",
            "start",
            "--fixture",
            str(SCENARIOS / "eligible.json"),
            "--live",
        )

        self.assertEqual(completed.returncode, 5)
        self.assertIn("PAPER ONLY", completed.stdout)
        self.assertEqual(completed.stderr, "")

    def test_authority_rich_task9_fixture_commands_fail_closed(self):
        commands = (
            ("replay", "diagnostic"),
            ("replay", "point-in-time"),
            ("option-paper", "start"),
            ("option-paper", "rank"),
            ("option-paper", "status"),
        )
        for command in commands:
            with self.subTest(command=command):
                completed = _run(
                    self.home / "-".join(command),
                    *command,
                    "--fixture",
                    str(SCENARIOS / "eligible.json"),
                )
                self.assertEqual(completed.returncode, 5)
                self.assertIn("PAPER ONLY", completed.stdout)
                self.assertNotIn("PRIMARY", completed.stdout)
                self.assertEqual(completed.stderr, "")

    def test_launcher_resolves_repo_from_spaced_external_directory(self):
        external = self.home / "external path with spaces"
        state_home = self.home / "operator state with spaces"
        external.mkdir()
        environment = _env(state_home)
        environment.pop("PYTHONPATH", None)

        completed = subprocess.run(
            [
                str(ROOT / "scripts" / "run_monitor.sh"),
                "run",
                "premarket",
                "--fixture",
                str(SCENARIOS / "eligible.json"),
                "--json",
            ],
            cwd=external,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)["outcome"], "CANDIDATES")
        combined = completed.stdout + completed.stderr
        self.assertNotIn("CANARY_KEY_MUST_NOT_PRINT", combined)
        self.assertNotIn("CANARY_SECRET_MUST_NOT_PRINT", combined)

    def test_unexpected_errors_map_to_ten_without_a_traceback(self):
        with patch("stock_monitor.cli._dispatch", side_effect=RuntimeError("canary detail")):
            with patch.dict(os.environ, _env(self.home), clear=True):
                with patch("sys.stdout") as stdout:
                    code = main(("db", "init"))

        self.assertEqual(code, 10)
        rendered = "".join(str(call) for call in stdout.method_calls)
        self.assertNotIn("canary detail", rendered)

    def test_reconciliation_exception_maps_to_locked_exit_five(self):
        output = io.StringIO()
        with patch(
            "stock_monitor.cli._dispatch",
            side_effect=WorkflowReconciliationError("canary reconciliation detail"),
        ), patch.dict(os.environ, _env(self.home), clear=True), redirect_stdout(output):
            code = main(("db", "init"))

        self.assertEqual(code, 5)
        self.assertEqual(
            output.getvalue(),
            "RECONCILIATION REQUIRED\nNo action was authorized.\n",
        )
        self.assertNotIn("canary reconciliation detail", output.getvalue())


if __name__ == "__main__":
    unittest.main()
