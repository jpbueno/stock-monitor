"""Acceptance-level audit that secrets never reach operator-visible artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from stock_monitor.domain import money_to_micros
from stock_monitor.journal import Journal
from stock_monitor.reports import PremarketState, archive_report, render_premarket_report


ROOT = Path(__file__).parents[2]
LAUNCHER = ROOT / "scripts" / "run_monitor.sh"
SCENARIOS = ROOT / "tests" / "fixtures" / "scenarios"
ET = ZoneInfo("America/New_York")
CANARIES = (
    "CANARY_KEY_123",
    "CANARY_SECRET_456",
    "CANARY_NESTED_789",
)


def _environment(home: Path) -> dict[str, str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "STOCK_MONITOR_HOME": str(home),
        "APCA_API_KEY_ID": f"prefix-{CANARIES[0]}-suffix",
        "APCA_API_SECRET_KEY": json.dumps(
            {
                "credentials": [
                    {
                        "secret": CANARIES[1],
                        "nested": {"password": CANARIES[2]},
                    }
                ]
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
    }
    environment.pop("PYTHONPATH", None)
    return environment


def _run(home: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(LAUNCHER), *arguments],
        cwd=home,
        env=_environment(home),
        text=True,
        capture_output=True,
        check=False,
    )


def _seed_nested_canaries(journal_path: Path) -> None:
    at = datetime(2026, 8, 14, 14, 5, tzinfo=timezone.utc)
    with Journal.open(journal_path) as journal:
        raw_message_id, _ = journal.append_raw_message(
            "secret-audit-message",
            at,
            "SKIPPED AAPL; APCA_API_SECRET_KEY="
            f'{{"credential":{{"password":"{CANARIES[1]}"}}}}',
        )
        journal.append_execution_event(
            raw_message_id=raw_message_id,
            action_ordinal=0,
            parsed_action="SKIPPED",
            signal_id="secret-audit-signal",
            symbol="AAPL",
            shares=1,
            price_micros=money_to_micros(Decimal("100")),
            event_time=at,
            compliance_result="COMPLIANT",
            reconciliation_state="CLEAR",
            details={
                "credentials": [
                    {
                        "api_secret_key": CANARIES[1],
                        "children": [{"password": CANARIES[2]}],
                    }
                ],
                "safe_note": "secret audit fixture",
            },
        )


def _archive_safe_report(reports_root: Path) -> Path:
    report = render_premarket_report(
        PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="NO TRADE",
            reason_codes=("NO_CANDIDATES",),
            observation_ids=("secret-audit-observation",),
            state_hash=hashlib.sha256(b"secret-audit-state").hexdigest(),
        )
    )
    return archive_report(report, reports_root).path


class SecretRedactionTests(unittest.TestCase):
    def test_canary_secrets_never_appear_in_stdout_stderr_archive_or_csv(self) -> None:
        with TemporaryDirectory() as temporary:
            home = Path(temporary)
            completed = [
                _run(home, "provider", "smoke", "--json"),
                _run(home, "db", "init", "--json"),
                _run(
                    home,
                    "run",
                    "premarket",
                    "--fixture",
                    str(SCENARIOS / "eligible.json"),
                    "--json",
                ),
            ]
            self.assertEqual(completed[1].returncode, 0, completed[1].stderr)
            self.assertEqual(completed[2].returncode, 0, completed[2].stderr)

            state_root = home / ".stock-monitor"
            _seed_nested_canaries(state_root / "journal.sqlite3")
            archive_path = _archive_safe_report(home / "reports")
            exported = _run(home, "export", "--json")
            completed.append(exported)
            self.assertEqual(exported.returncode, 0, exported.stderr)

            csv_paths = tuple(
                sorted((state_root / "exports").glob("*.csv"))
            )
            self.assertTrue(csv_paths)
            self.assertTrue(archive_path.is_file())
            combined = "\n".join(
                [
                    *(item.stdout + item.stderr for item in completed),
                    archive_path.read_text(encoding="utf-8"),
                    *(path.read_text(encoding="utf-8") for path in csv_paths),
                ]
            )
            for canary in CANARIES:
                with self.subTest(canary=canary):
                    self.assertNotIn(canary, combined)


if __name__ == "__main__":
    unittest.main()
