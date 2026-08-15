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
PROVIDER_FIXTURE_ROOT = FIXTURE_ROOT / "providers"
EVIDENCE_FIXTURE_ROOT = FIXTURE_ROOT / "evidence"
PORTFOLIO_FIXTURE_ROOT = FIXTURE_ROOT / "portfolios"


class FixtureTransport:
    """Deterministic single-hop HTTP transport backed by a JSON manifest."""

    def __init__(self, fixture: str | Path) -> None:
        path = Path(fixture)
        if not path.is_absolute():
            candidate = FIXTURE_ROOT / path
            path = candidate if candidate.is_file() else Path(fixture)
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or not isinstance(
            document.get("responses"), list
        ):
            raise TypeError("transport fixture must contain a responses list")
        self._responses = deepcopy(document["responses"])
        self.requested_urls: list[str] = []
        self.requested_headers: list[dict[str, str]] = []

    def get(self, url: str, headers: Mapping[str, str]):
        from stock_monitor.providers.http import HttpResponse

        self.requested_urls.append(url)
        self.requested_headers.append(dict(headers))
        if not self._responses:
            raise AssertionError(f"unexpected fixture request: {url}")
        raw = self._responses.pop(0)
        if not isinstance(raw, dict):
            raise TypeError("fixture response must be an object")
        expected_url = raw.get("url")
        if expected_url != url:
            raise AssertionError(
                f"fixture expected {expected_url!r}, received {url!r}"
            )
        body_value = raw.get("body", "")
        if isinstance(body_value, str):
            body = body_value.encode("utf-8")
        else:
            body = json.dumps(
                body_value,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        headers_value = raw.get("headers", {"Content-Type": "application/json"})
        if isinstance(headers_value, dict):
            response_headers = tuple(
                (str(name), str(value)) for name, value in headers_value.items()
            )
        elif isinstance(headers_value, list):
            response_headers = tuple(
                (str(item[0]), str(item[1])) for item in headers_value
            )
        else:
            raise TypeError("fixture headers must be an object or pair list")
        return HttpResponse(
            status=int(raw.get("status", 200)),
            headers=response_headers,
            body=body,
            url=url,
        )

    @property
    def remaining_responses(self) -> int:
        return len(self._responses)


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


def calendar_fixture(year: int = 2026) -> dict[str, object]:
    """Return a mutable copy of one reviewed calendar fixture."""
    raw = json.loads(
        (REFERENCE_FIXTURE_ROOT / f"calendar-{year}.json").read_text(
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


def credentials():
    """Return canary-only Alpaca credentials without importing providers early."""
    from stock_monitor.providers.alpaca import AlpacaCredentials

    return AlpacaCredentials("fixture-key-id", "fixture-secret-key")


def reference_fixture(name: str) -> dict[str, object]:
    raw = json.loads(
        (PROVIDER_FIXTURE_ROOT / "reference" / name).read_text(encoding="utf-8")
    )
    if not isinstance(raw, dict):
        raise TypeError("reference fixture must contain a JSON object")
    return deepcopy(raw)


def seeded_ledgers(canonical_entry: Decimal = Decimal("100")):
    """Return the deterministic Task 6 ledger pair without eager imports."""
    from stock_monitor.ledger import (
        LedgerPair,
        LedgerSignal,
    )

    raw = json.loads(
        (PORTFOLIO_FIXTURE_ROOT / "seeded-ledgers.json").read_text(
            encoding="utf-8"
        )
    )
    if not isinstance(raw, dict) or not isinstance(raw.get("signals"), list):
        raise TypeError("seeded-ledgers fixture must contain a signals list")
    signals = []
    for item in raw["signals"]:
        if not isinstance(item, dict):
            raise TypeError("seeded-ledgers signal must be an object")
        entry = (
            canonical_entry
            if item["signal_id"] == "sig-1"
            else Decimal(str(item["maximum_entry"]))
        )
        signal = LedgerSignal(
                signal_id=str(item["signal_id"]),
                symbol=str(item["symbol"]),
                role=str(item["role"]),
                publication_session=date.fromisoformat(
                    str(item["publication_session"])
                ),
                maximum_entry=entry,
                recommended_stop=Decimal(str(item["recommended_stop"])),
                target=Decimal(str(item["target"])),
                planned_shares=int(item["planned_shares"]),
                tick_size=Decimal(str(item["tick_size"])),
                trigger_price=Decimal(str(item["trigger_price"])),
            )
        signals.append(signal)
    pair = LedgerPair(signals=tuple(signals))
    pair.record_canonical_fill(
        signal_id="sig-1",
        price=canonical_entry,
        shares=5,
        at=aware_et(date(2026, 8, 14), "10:14"),
    )
    return pair


def account_check(
    *,
    at: str,
    settled_cash: str,
    pending_orders: int = 0,
    unlogged_positions: int = 0,
    session_date: date = date(2026, 8, 14),
):
    """Build a same-session account check for risk tests."""
    from stock_monitor.risk import AccountCheck

    return AccountCheck(
        settled_cash=Decimal(settled_cash),
        pending_orders=pending_orders,
        unlogged_positions=unlogged_positions,
        at=aware_et(session_date, at),
    )


def buy_event(
    *,
    at: str,
    price: str,
    shares: int,
    session_date: date = date(2026, 8, 14),
):
    """Build a confirmed buy event for risk tests."""
    from stock_monitor.risk import ExecutionEvent

    return ExecutionEvent(
        kind="BUY",
        at=aware_et(session_date, at),
        price=Decimal(price),
        shares=shares,
    )


def cash_adjustment(
    *,
    at: str,
    amount: str,
    session_date: date = date(2026, 8, 14),
):
    """Build an account-wide cash adjustment for ordering tests."""
    from stock_monitor.risk import ExecutionEvent

    return ExecutionEvent(
        kind="CASH_ADJUSTMENT",
        at=aware_et(session_date, at),
        amount=Decimal(amount),
    )
