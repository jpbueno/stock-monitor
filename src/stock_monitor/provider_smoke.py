"""Stateless, GET-only provider readiness projection."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from .config import Settings
from .market_calendar import (
    CalendarCoverageError,
    CalendarError,
    latest_completed_session_window,
    load_current_market_calendar,
)
from .providers.alpaca import (
    HISTORICAL_SIP_RELEASE_DELAY,
    AlpacaCredentials,
    AlpacaMarketData,
    EntitlementSmoke,
    TimeWindow,
)
from .providers.http import EgressPolicy, HttpGetClient


_ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class ProviderSmokeResult:
    status: str
    exit_code: int
    authentication_ok: bool
    historical_sip_ok: bool
    latest_iex_fresh: bool
    observed_at: datetime
    reason_codes: tuple[str, ...]

    @classmethod
    def from_entitlement(
        cls,
        smoke: EntitlementSmoke,
        *,
        observed_at: datetime | None = None,
    ) -> "ProviderSmokeResult":
        ready = bool(
            smoke.status == "READY"
            and smoke.authentication_ok
            and smoke.historical_sip_ok
            and smoke.latest_iex_fresh
        )
        return cls(
            status="READY" if ready else smoke.status,
            exit_code=0 if ready else 3,
            authentication_ok=smoke.authentication_ok,
            historical_sip_ok=smoke.historical_sip_ok,
            latest_iex_fresh=smoke.latest_iex_fresh,
            observed_at=smoke.observed_at if observed_at is None else observed_at,
            reason_codes=smoke.failures,
        )

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


def run_provider_smoke(
    settings: Settings,
    *,
    now: Callable[[], datetime],
) -> ProviderSmokeResult:
    observed_at = now()
    try:
        local_date = observed_at.astimezone(_ET).date()
        calendar = load_current_market_calendar(
            settings.project_root,
            as_of=local_date,
        )
        try:
            completed_session = latest_completed_session_window(
                calendar,
                observed_at=observed_at,
                release_delay=HISTORICAL_SIP_RELEASE_DELAY,
            )
        except CalendarCoverageError:
            prior_date = date(local_date.year - 1, 12, 31)
            prior_calendar = load_current_market_calendar(
                settings.project_root,
                as_of=prior_date,
            )
            completed_session = latest_completed_session_window(
                prior_calendar,
                observed_at=observed_at,
                release_delay=HISTORICAL_SIP_RELEASE_DELAY,
            )
        completed = TimeWindow(
            datetime.combine(
                completed_session.session_date,
                time.min,
                tzinfo=completed_session.closed_at.tzinfo,
            ),
            completed_session.closed_at,
        )
    except CalendarError:
        return ProviderSmokeResult.from_entitlement(
            EntitlementSmoke(
                authentication_ok=False,
                historical_sip_ok=False,
                latest_iex_fresh=False,
                status="BLOCKED_COMPLETED_SESSION",
                observed_at=observed_at,
                failures=("COMPLETED_SESSION_RELEASE_UNAVAILABLE",),
            )
        )
    policy = EgressPolicy(("data.alpaca.markets",))
    provider = AlpacaMarketData(
        HttpGetClient(policy),
        AlpacaCredentials(
            settings.alpaca_api_key_id,
            settings.alpaca_api_secret_key,
        ),
        base_url=settings.sources.alpaca_market_data_url,
        now=now,
        cache=None,
    )
    return ProviderSmokeResult.from_entitlement(
        provider.smoke(completed_session=completed),
        observed_at=observed_at,
    )


__all__ = ["ProviderSmokeResult", "run_provider_smoke"]
