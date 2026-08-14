"""Dependency-free helpers shared by Stock Monitor tests."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from stock_monitor.policy import Policy


FIXTURE_ROOT = Path(__file__).parent / "fixtures"
REFERENCE_FIXTURE_ROOT = FIXTURE_ROOT / "reference"


def load_json(relative: str) -> object:
    return json.loads((FIXTURE_ROOT / relative).read_text(encoding="utf-8"))


def aware_et(session_date: date, hhmm: str) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    return datetime.combine(
        session_date,
        time(hour, minute),
        ZoneInfo("America/New_York"),
    )


def isolated_env(
    root: Path,
    overrides: Mapping[str, str] | None = None,
) -> dict[str, str]:
    result = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(Path.cwd() / "src"),
        "STOCK_MONITOR_HOME": str(root),
    }
    result.update(overrides or {})
    return result


def policy_fixture(**overrides: object) -> Policy:
    values: dict[str, object] = {
        "capital": "5000",
        "max_live_exposure": "1000",
        "max_position_risk": "25",
        "max_combined_risk": "50",
        "max_positions": 2,
        "max_entries_per_session": 1,
        "min_score": 80,
        "max_monthly_drawdown": "250",
        "max_weekly_drawdown": "100",
        "universe_max_age_days": 31,
        "live_quote_max_age_seconds": 300,
        "disagreement_tolerance": "0.005",
    }
    values.update(overrides)
    for name in (
        "capital",
        "max_live_exposure",
        "max_position_risk",
        "max_combined_risk",
        "max_monthly_drawdown",
        "max_weekly_drawdown",
        "disagreement_tolerance",
    ):
        values[name] = Decimal(str(values[name]))
    return Policy(**values)  # type: ignore[arg-type]


def calendar_fixture() -> dict[str, object]:
    """Return a mutable copy of the reviewed 2026 calendar fixture."""
    raw = json.loads(
        (REFERENCE_FIXTURE_ROOT / "calendar-2026.json").read_text(
            encoding="utf-8"
        )
    )
    if not isinstance(raw, dict):
        raise TypeError("calendar fixture must contain a JSON object")
    return deepcopy(raw)


def universe_fixture() -> dict[str, object]:
    """Return a mutable copy of the reviewed universe fixture."""
    raw = json.loads(
        (REFERENCE_FIXTURE_ROOT / "universe-2026-08-14.json").read_text(
            encoding="utf-8"
        )
    )
    if not isinstance(raw, dict):
        raise TypeError("universe fixture must contain a JSON object")
    return deepcopy(raw)
