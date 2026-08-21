from __future__ import annotations

import io
import json
import os
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from contextlib import redirect_stdout

import stock_monitor.cli as cli


ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
CANARY = "PROVIDER_EXCEPTION_CANARY_MUST_NOT_PRINT"


@dataclass(frozen=True)
class _Result:
    status: str
    exit_code: int
    authentication_ok: bool
    historical_sip_ok: bool
    latest_iex_fresh: bool
    observed_at: datetime
    reason_codes: tuple[str, ...]

    def safe_fields(self) -> dict[str, object]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "observed_at": self.observed_at.isoformat(),
            "checks": {
                "authentication": self.authentication_ok,
                "historical_sip": self.historical_sip_ok,
                "latest_iex_fresh": self.latest_iex_fresh,
            },
            "reason_codes": list(self.reason_codes),
        }


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
        result = _Result(
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
        result = _Result(
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
        result = _Result(
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
            create=True,
        ), patch.dict(os.environ, self.environment, clear=True), redirect_stdout(output):
            code = cli.main(("provider", "smoke", "--json"))

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), result.safe_fields())


if __name__ == "__main__":
    unittest.main()
