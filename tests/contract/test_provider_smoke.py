from __future__ import annotations

import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from stock_monitor.config import load_settings
import stock_monitor.market_calendar as market_calendar_module
from stock_monitor.market_calendar import (
    CalendarError,
    MarketCalendar,
    load_current_market_calendar,
)
from stock_monitor.provider_smoke import ProviderSmokeResult, run_provider_smoke
from stock_monitor.providers.alpaca import (
    AlpacaCredentials,
    AlpacaMarketData,
    ProviderDataError,
    ProviderMalformedError,
    ProviderStaleError,
    TimeWindow,
)
from stock_monitor.providers.http import (
    EgressPolicy,
    HttpResponse,
    HttpStatusError,
    HttpTransportError,
    ProviderResponseError,
    get_with_redirects,
)
from tests.support import calendar_fixture


ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
COMPLETED = TimeWindow(
    datetime(2026, 8, 13, 4, 0, tzinfo=UTC),
    datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
)


class RoutingTransport:
    def __init__(self, responder) -> None:
        self.responder = responder
        self.requested_urls: list[str] = []

    def get(self, url: str, headers) -> HttpResponse:
        self.requested_urls.append(url)
        response = self.responder(url)
        if isinstance(response, Exception):
            raise response
        status, body = response
        return HttpResponse(
            status=status,
            headers=(("Content-Type", "application/json"),),
            body=body.encode("utf-8"),
            url=url,
        )


def _fresh_iex(timestamp: str = "2026-08-14T12:59:00Z") -> str:
    return (
        '{"quotes":{"SPY":{"t":"'
        + timestamp
        + '",'
        '"bp":"651.9","ap":"652.1"}},"next_page_token":null}'
    )


def _completed_sip(timestamp: str = "2026-08-13T04:00:00Z") -> str:
    return (
        '{"bars":{"SPY":[{"t":"'
        + timestamp
        + '",'
        '"o":1,"h":1,"l":1,"c":1,"v":1}]},"next_page_token":null}'
    )


def _environment(home: Path) -> dict[str, str]:
    return {
        "APCA_API_KEY_ID": "fixture-key-id",
        "APCA_API_SECRET_KEY": "fixture-secret-key",
        "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
        "STOCK_MONITOR_HOME": str(home),
    }


class ProviderSmokeContractTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.settings = load_settings(ROOT, _environment(self.home))

    def test_ready_requires_same_invocation_fresh_iex_and_delayed_sip(self) -> None:
        transport = RoutingTransport(
            lambda url: (200, _completed_sip())
            if urlsplit(url).path.endswith("/bars")
            else (200, _fresh_iex())
        )

        with patch("stock_monitor.provider_smoke.HttpGetClient", return_value=transport):
            result = run_provider_smoke(self.settings, now=lambda: NOW)

        self.assertEqual((result.status, result.exit_code), ("READY", 0))
        self.assertEqual(
            set(result.safe_fields()),
            {"status", "exit_code", "observed_at", "checks", "reason_codes"},
        )
        self.assertEqual(len(transport.requested_urls), 2)
        self.assertEqual(
            {urlsplit(url).hostname for url in transport.requested_urls},
            {"data.alpaca.markets"},
        )
        sip_url = next(
            url for url in transport.requested_urls if urlsplit(url).path.endswith("/bars")
        )
        sip_query = parse_qs(urlsplit(sip_url).query)
        self.assertEqual(sip_query["feed"], ["sip"])
        self.assertEqual(sip_query["start"], ["2026-08-13T04:00:00Z"])
        self.assertEqual(sip_query["end"], ["2026-08-13T20:00:00Z"])
        self.assertFalse((self.home / ".stock-monitor").exists())
        self.assertFalse((self.home / "reports").exists())

    def test_ready_accepts_live_latest_shape_and_bounded_clock_skew(self) -> None:
        live_latest = (
            '{"quotes":{"SPY":{"t":"2026-08-14T13:00:00.024230Z",'
            '"bp":"651.9","ap":"652.1"}}}'
        )
        transport = RoutingTransport(
            lambda url: (200, _completed_sip())
            if urlsplit(url).path.endswith("/bars")
            else (200, live_latest)
        )

        with patch("stock_monitor.provider_smoke.HttpGetClient", return_value=transport):
            result = run_provider_smoke(self.settings, now=lambda: NOW)

        self.assertEqual((result.status, result.exit_code), ("READY", 0))
        self.assertTrue(result.authentication_ok)
        self.assertTrue(result.historical_sip_ok)
        self.assertTrue(result.latest_iex_fresh)

    def test_expected_typed_failures_have_exact_safe_results(self) -> None:
        def smoke(responder, *, completed_session=COMPLETED) -> ProviderSmokeResult:
            provider = AlpacaMarketData(
                RoutingTransport(responder),
                AlpacaCredentials("fixture-key-id", "fixture-secret-key"),
                now=lambda: NOW,
                cache=None,
            )
            return ProviderSmokeResult.from_entitlement(
                provider.smoke(completed_session=completed_session)
            )

        cases = (
            (
                "authentication",
                lambda _: (401, '{"message":"unauthorized"}'),
                COMPLETED,
                (
                    "BLOCKED_AUTHENTICATION",
                    3,
                    False,
                    False,
                    False,
                    ("AUTHENTICATION_UNAVAILABLE",),
                ),
            ),
            (
                "connectivity",
                lambda _: HttpTransportError("canary transport detail"),
                COMPLETED,
                (
                    "BLOCKED_CONNECTIVITY",
                    3,
                    False,
                    False,
                    False,
                    ("CONNECTIVITY_UNAVAILABLE",),
                ),
            ),
            (
                "availability",
                lambda _: (503, '{"message":"unavailable"}'),
                COMPLETED,
                (
                    "BLOCKED_AVAILABILITY",
                    3,
                    False,
                    False,
                    False,
                    ("PROVIDER_AVAILABILITY_UNAVAILABLE",),
                ),
            ),
            (
                "malformed",
                lambda _: (200, "not-json"),
                COMPLETED,
                (
                    "BLOCKED_MALFORMED_RESPONSE",
                    3,
                    True,
                    False,
                    False,
                    ("MALFORMED_PROVIDER_RESPONSE",),
                ),
            ),
            (
                "stale_iex",
                lambda url: (200, _completed_sip())
                if urlsplit(url).path.endswith("/bars")
                else (
                    200,
                    '{"quotes":{"SPY":{"t":"2026-08-14T12:54:59Z",'
                    '"bp":1,"ap":1.01}},"next_page_token":null}',
                ),
                COMPLETED,
                (
                    "BLOCKED_IEX_FRESHNESS",
                    3,
                    True,
                    True,
                    False,
                    ("IEX_QUOTE_STALE",),
                ),
            ),
            (
                "incomplete_cohort",
                lambda url: (200, _completed_sip())
                if urlsplit(url).path.endswith("/bars")
                else (200, '{"quotes":{},"next_page_token":null}'),
                COMPLETED,
                (
                    "BLOCKED_INCOMPLETE_COHORT",
                    3,
                    True,
                    True,
                    False,
                    ("PROVIDER_COHORT_INCOMPLETE",),
                ),
            ),
            (
                "sip_entitlement",
                lambda url: (403, '{"message":"sip forbidden"}')
                if urlsplit(url).path.endswith("/bars")
                else (200, _fresh_iex()),
                COMPLETED,
                (
                    "BLOCKED_ENTITLEMENT",
                    3,
                    True,
                    False,
                    True,
                    ("HISTORICAL_SIP_ENTITLEMENT_UNAVAILABLE",),
                ),
            ),
            (
                "completed_session",
                lambda _: (200, _fresh_iex()),
                None,
                (
                    "BLOCKED_COMPLETED_SESSION",
                    3,
                    True,
                    False,
                    True,
                    ("COMPLETED_SESSION_RELEASE_UNAVAILABLE",),
                ),
            ),
            (
                "malformed_sip",
                lambda url: (200, "not-json")
                if urlsplit(url).path.endswith("/bars")
                else (200, _fresh_iex()),
                COMPLETED,
                (
                    "BLOCKED_MALFORMED_RESPONSE",
                    3,
                    True,
                    False,
                    True,
                    ("MALFORMED_PROVIDER_RESPONSE",),
                ),
            ),
            (
                "incomplete_sip",
                lambda url: (200, '{"bars":{},"next_page_token":null}')
                if urlsplit(url).path.endswith("/bars")
                else (200, _fresh_iex()),
                COMPLETED,
                (
                    "BLOCKED_INCOMPLETE_COHORT",
                    3,
                    True,
                    False,
                    True,
                    ("PROVIDER_COHORT_INCOMPLETE",),
                ),
            ),
        )
        for name, responder, completed_session, expected in cases:
            with self.subTest(name=name):
                result = smoke(responder, completed_session=completed_session)
                self.assertEqual(
                    (
                        result.status,
                        result.exit_code,
                        result.authentication_ok,
                        result.historical_sip_ok,
                        result.latest_iex_fresh,
                        result.reason_codes,
                    ),
                    expected,
                )
                self.assertEqual(result.observed_at, NOW)
                self.assertEqual(
                    set(result.safe_fields()),
                    {"status", "exit_code", "observed_at", "checks", "reason_codes"},
                )
                self.assertNotIn("canary", json.dumps(result.safe_fields()))

    def test_http_status_malformed_and_stale_failures_are_typed(self) -> None:
        status_transport = RoutingTransport(
            lambda _: (401, '{"message":"credential canary"}')
        )
        with self.assertRaises(HttpStatusError) as raised:
            get_with_redirects(
                status_transport,
                EgressPolicy(("data.alpaca.markets",)),
                "https://data.alpaca.markets/v2/stocks/quotes/latest?symbols=SPY&feed=iex",
                {},
                max_attempts=1,
            )
        self.assertEqual(raised.exception.status, 401)

        malformed = AlpacaMarketData(
            RoutingTransport(lambda _: (200, "not-json")),
            AlpacaCredentials("fixture-key-id", "fixture-secret-key"),
            now=lambda: NOW,
        )
        with self.assertRaises(ProviderMalformedError):
            malformed.latest_iex_quotes(("SPY",))

        stale = AlpacaMarketData(
            RoutingTransport(
                lambda _: (
                    200,
                    '{"quotes":{"SPY":{"t":"2026-08-14T12:54:59Z",'
                    '"bp":1,"ap":1.01}},"next_page_token":null}',
                )
            ),
            AlpacaCredentials("fixture-key-id", "fixture-secret-key"),
            now=lambda: NOW,
        )
        with self.assertRaises(ProviderStaleError):
            stale.latest_iex_quotes(("SPY",))

        self.assertTrue(issubclass(ProviderStaleError, ProviderDataError))
        self.assertTrue(issubclass(ProviderStaleError, ProviderResponseError))
        self.assertFalse(issubclass(ProviderStaleError, ProviderMalformedError))

    def test_provider_clock_advances_while_observed_at_stays_at_invocation_start(
        self,
    ) -> None:
        instants = iter(
            (
                datetime(2026, 8, 14, 13, 0, 0, tzinfo=UTC),
                datetime(2026, 8, 14, 13, 0, 0, 100_000, tzinfo=UTC),
                datetime(2026, 8, 14, 13, 0, 2, tzinfo=UTC),
                datetime(2026, 8, 14, 13, 0, 3, tzinfo=UTC),
                datetime(2026, 8, 14, 13, 0, 4, tzinfo=UTC),
            )
        )
        calls: list[datetime] = []

        def clock() -> datetime:
            value = next(instants)
            calls.append(value)
            return value

        transport = RoutingTransport(
            lambda url: (200, _completed_sip())
            if urlsplit(url).path.endswith("/bars")
            else (200, _fresh_iex("2026-08-14T13:00:01Z"))
        )
        with patch("stock_monitor.provider_smoke.HttpGetClient", return_value=transport):
            result = run_provider_smoke(self.settings, now=clock)

        self.assertEqual((result.status, result.exit_code), ("READY", 0))
        self.assertEqual(result.observed_at, calls[0])
        self.assertEqual(len(calls), 5)

    def test_new_year_premarket_uses_verified_prior_year_completed_session(
        self,
    ) -> None:
        current = MarketCalendar.from_mapping(
            calendar_fixture(2027),
            as_of=date(2027, 1, 4),
            expected_year=2027,
        )
        market_calendar_module._register_calendar_authority(
            market_calendar_module._RELEASE_CALENDARS,
            current,
        )
        prior = load_current_market_calendar(ROOT, as_of=date(2026, 8, 14))
        observed_at = datetime(2027, 1, 4, 13, 0, tzinfo=UTC)
        transport = RoutingTransport(
            lambda url: (200, _completed_sip("2026-12-31T05:00:00Z"))
            if urlsplit(url).path.endswith("/bars")
            else (200, _fresh_iex("2027-01-04T12:59:00Z"))
        )
        with patch(
            "stock_monitor.provider_smoke.load_current_market_calendar",
            side_effect=(current, prior),
        ) as loader, patch(
            "stock_monitor.provider_smoke.HttpGetClient",
            return_value=transport,
        ):
            result = run_provider_smoke(self.settings, now=lambda: observed_at)

        self.assertEqual((result.status, result.exit_code), ("READY", 0))
        self.assertEqual(loader.call_count, 2)
        self.assertEqual(loader.call_args_list[1].kwargs["as_of"], date(2026, 12, 31))
        sip_url = next(
            url for url in transport.requested_urls if urlsplit(url).path.endswith("/bars")
        )
        query = parse_qs(urlsplit(sip_url).query)
        self.assertEqual(query["start"], ["2026-12-31T05:00:00Z"])
        self.assertEqual(query["end"], ["2026-12-31T21:00:00Z"])

    def test_new_year_fails_closed_when_prior_release_is_unavailable(self) -> None:
        current = MarketCalendar.from_mapping(
            calendar_fixture(2027),
            as_of=date(2027, 1, 4),
            expected_year=2027,
        )
        market_calendar_module._register_calendar_authority(
            market_calendar_module._RELEASE_CALENDARS,
            current,
        )
        observed_at = datetime(2027, 1, 4, 13, 0, tzinfo=UTC)
        with patch(
            "stock_monitor.provider_smoke.load_current_market_calendar",
            side_effect=(current, CalendarError("prior release canary")),
        ) as loader, patch("stock_monitor.provider_smoke.HttpGetClient") as client:
            result = run_provider_smoke(self.settings, now=lambda: observed_at)

        self.assertEqual(
            (
                result.status,
                result.exit_code,
                result.authentication_ok,
                result.historical_sip_ok,
                result.latest_iex_fresh,
                result.reason_codes,
            ),
            (
                "BLOCKED_COMPLETED_SESSION",
                3,
                False,
                False,
                False,
                ("COMPLETED_SESSION_RELEASE_UNAVAILABLE",),
            ),
        )
        self.assertEqual(loader.call_count, 2)
        client.assert_not_called()

    def test_calendar_release_failure_is_safe_and_does_not_touch_network(self) -> None:
        with patch(
            "stock_monitor.provider_smoke.load_current_market_calendar",
            side_effect=CalendarError("release canary"),
        ), patch("stock_monitor.provider_smoke.HttpGetClient") as client:
            result = run_provider_smoke(self.settings, now=lambda: NOW)

        self.assertEqual(result.exit_code, 3)
        self.assertEqual(
            result.reason_codes,
            ("COMPLETED_SESSION_RELEASE_UNAVAILABLE",),
        )
        client.assert_not_called()
        self.assertNotIn("canary", json.dumps(result.safe_fields()))


if __name__ == "__main__":
    unittest.main()
