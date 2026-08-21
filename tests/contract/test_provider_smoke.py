from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from stock_monitor.config import load_settings
from stock_monitor.market_calendar import CalendarError
from stock_monitor.provider_smoke import ProviderSmokeResult, run_provider_smoke
from stock_monitor.providers.alpaca import (
    AlpacaCredentials,
    AlpacaMarketData,
    ProviderMalformedError,
    ProviderStaleError,
    TimeWindow,
)
from stock_monitor.providers.http import (
    EgressPolicy,
    HttpResponse,
    HttpStatusError,
    HttpTransportError,
    get_with_redirects,
)


ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
COMPLETED = TimeWindow(
    datetime(2026, 8, 13, 13, 30, tzinfo=UTC),
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


def _fresh_iex() -> str:
    return (
        '{"quotes":{"SPY":{"t":"2026-08-14T12:59:00Z",'
        '"bp":"651.9","ap":"652.1"}},"next_page_token":null}'
    )


def _completed_sip() -> str:
    return (
        '{"bars":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
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
        self.assertEqual(parse_qs(urlsplit(sip_url).query)["feed"], ["sip"])
        self.assertFalse((self.home / ".stock-monitor").exists())
        self.assertFalse((self.home / "reports").exists())

    def test_expected_typed_failures_are_safe_exit_three(self) -> None:
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
                "AUTHENTICATION_UNAVAILABLE",
            ),
            (
                "connectivity",
                lambda _: HttpTransportError("canary transport detail"),
                COMPLETED,
                "CONNECTIVITY_UNAVAILABLE",
            ),
            (
                "availability",
                lambda _: (503, '{"message":"unavailable"}'),
                COMPLETED,
                "PROVIDER_AVAILABILITY_UNAVAILABLE",
            ),
            (
                "malformed",
                lambda _: (200, "not-json"),
                COMPLETED,
                "MALFORMED_PROVIDER_RESPONSE",
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
                "IEX_QUOTE_STALE",
            ),
            (
                "incomplete_cohort",
                lambda url: (200, _completed_sip())
                if urlsplit(url).path.endswith("/bars")
                else (200, '{"quotes":{},"next_page_token":null}'),
                COMPLETED,
                "PROVIDER_COHORT_INCOMPLETE",
            ),
            (
                "sip_entitlement",
                lambda url: (403, '{"message":"sip forbidden"}')
                if urlsplit(url).path.endswith("/bars")
                else (200, _fresh_iex()),
                COMPLETED,
                "HISTORICAL_SIP_ENTITLEMENT_UNAVAILABLE",
            ),
            (
                "completed_session",
                lambda _: (200, _fresh_iex()),
                None,
                "COMPLETED_SESSION_RELEASE_UNAVAILABLE",
            ),
        )
        for name, responder, completed_session, reason in cases:
            with self.subTest(name=name):
                result = smoke(responder, completed_session=completed_session)
                self.assertEqual(result.exit_code, 3)
                self.assertIn(reason, result.reason_codes)
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
