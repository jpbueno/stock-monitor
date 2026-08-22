from __future__ import annotations

import copy
import gc
import unittest
import weakref
from array import array
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType, ModuleType, SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import stock_monitor.providers.alpaca as alpaca_module
from stock_monitor.providers.alpaca import (
    AlpacaCredentials,
    AlpacaMarketData,
    ProviderDataError,
    ProviderIncompleteError,
    ProviderMalformedError,
    TimeWindow,
    is_ingestible_provider_fetch_cohort,
    is_ingestible_provider_option_chain,
    is_issued_normalized_market_fact,
    is_issued_provider_option_chain,
    is_issued_provider_fetch_cohort,
    is_issued_provider_fetch_page_bundle,
    normalized_market_facts_share_owner,
    provider_fetch_cohorts_share_owner,
    read_provider_fetch_bundle,
    recompute_alpaca_page_metadata,
)
from stock_monitor.providers.http import HttpResponse, HttpTransportError
from tests.support import FixtureTransport, credentials


NOW = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
WINDOW = TimeWindow(
    datetime(2026, 5, 1, tzinfo=UTC),
    datetime(2026, 8, 13, 20, tzinfo=UTC),
)
SMOKE_WINDOW = TimeWindow(
    datetime(2026, 8, 13, 4, 0, tzinfo=UTC),
    datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
)
_GLOBAL_ROUTE_TABLE: dict[str, object] = {}
_GLOBAL_ROUTE_HOLDER = ModuleType("alpaca_contract_route_holder")


class RoutingTransport:
    def __init__(self, responder) -> None:
        self.responder = responder
        self.requested_urls: list[str] = []
        self.requested_headers: list[dict[str, str]] = []

    def get(self, url: str, headers: dict[str, str]) -> HttpResponse:
        self.requested_urls.append(url)
        self.requested_headers.append(dict(headers))
        status, body = self.responder(url)
        return HttpResponse(
            status=status,
            headers=(("Content-Type", "application/json"),),
            body=body.encode("utf-8"),
            url=url,
        )


class AlpacaContractTests(unittest.TestCase):
    @staticmethod
    def _two_page_daily_bar_responder(
        *,
        terminal: bool = True,
    ):
        first_body = (
            '{"bars":{"SPY":[{"t":"2026-08-12T20:00:00Z",'
            '"o":1,"h":1,"l":1,"c":1,"v":1}]},'
            '"next_page_token":"page-2"}'
        )
        second_body = (
            '{"bars":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
            '"o":2,"h":2,"l":2,"c":2,"v":2}]}'
            + (',"next_page_token":null}' if terminal else '}')
        )

        def responder(url: str) -> tuple[int, str]:
            token = parse_qs(urlsplit(url).query).get("page_token")
            return (200, second_body if token == ["page-2"] else first_body)

        return responder, (first_body.encode("utf-8"), second_body.encode("utf-8"))

    @staticmethod
    def _two_page_paginated_cases():
        return (
            (
                "daily bars",
                '{"bars":{"SPY":[{"t":"2026-08-12T20:00:00Z",'
                '"o":1,"h":1,"l":1,"c":1,"v":1}]},'
                '"next_page_token":"page-2"}',
                '{"bars":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
                '"o":2,"h":2,"l":2,"c":2,"v":2}]},'
                '"next_page_token":null}',
                lambda client, sink: client.daily_bars(
                    ("SPY",), WINDOW, page_sink=sink
                ),
            ),
            (
                "minute bars",
                '{"bars":{"SPY":[{"t":"2026-08-13T19:59:00Z",'
                '"o":1,"h":1,"l":1,"c":1,"v":1}]},'
                '"next_page_token":"page-2"}',
                '{"bars":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
                '"o":2,"h":2,"l":2,"c":2,"v":2}]},'
                '"next_page_token":null}',
                lambda client, sink: client.historical_minute_bars(
                    ("SPY",), WINDOW, page_sink=sink
                ),
            ),
            (
                "trades",
                '{"trades":{"SPY":[{"t":"2026-08-13T19:59:00Z",'
                '"p":1,"s":1,"i":1}]},'
                '"next_page_token":"page-2"}',
                '{"trades":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
                '"p":2,"s":1,"i":2}]},'
                '"next_page_token":null}',
                lambda client, sink: client.historical_trades(
                    ("SPY",), WINDOW, page_sink=sink
                ),
            ),
            (
                "quotes",
                '{"quotes":{"SPY":[{"t":"2026-08-13T19:59:00Z",'
                '"bp":1,"ap":2,"i":1}]},'
                '"next_page_token":"page-2"}',
                '{"quotes":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
                '"bp":2,"ap":3,"i":2}]},'
                '"next_page_token":null}',
                lambda client, sink: client.historical_quotes(
                    ("SPY",), WINDOW, page_sink=sink
                ),
            ),
            (
                "option chain",
                '{"snapshots":{"SPY260918C00650000":{'
                '"latestQuote":{"t":"2026-08-14T12:58:00Z",'
                '"bp":"1.00","ap":"1.05"}}},'
                '"next_page_token":"page-2"}',
                '{"snapshots":{"SPY260918C00660000":{'
                '"latestQuote":{"t":"2026-08-14T12:59:00Z",'
                '"bp":"0.90","ap":"0.95"}}},'
                '"next_page_token":null}',
                lambda client, sink: client.option_chain(
                    "SPY", page_sink=sink
                ),
            ),
        )

    def test_page_sink_cannot_replace_request_dependencies_between_pages(
        self,
    ) -> None:
        for name, first_body, second_body, invoke in (
            self._two_page_paginated_cases()
        ):
            with self.subTest(api=name):
                first_transport = RoutingTransport(
                    lambda _url, body=first_body: (200, body)
                )
                replacement_transport = RoutingTransport(
                    lambda _url, body=second_body: (200, body)
                )
                client = AlpacaMarketData(
                    first_transport,
                    credentials(),
                    now=lambda: NOW,
                )
                pages = []

                def mutating_sink(page) -> None:
                    pages.append(page)
                    client._transport = replacement_transport

                with self.assertRaisesRegex(
                    ProviderMalformedError,
                    "request dependencies",
                ):
                    invoke(client, mutating_sink)

                self.assertEqual(len(first_transport.requested_urls), 1)
                self.assertEqual(replacement_transport.requested_urls, [])
                self.assertEqual(len(pages), 1)
                self.assertTrue(
                    is_issued_provider_fetch_page_bundle(pages[0])
                )
                self.assertEqual(client._issued_fetch_manifests, {})

    def test_page_sink_cannot_replace_instance_request_dispatch(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )
        original_dispatch = client._get_json_for_request
        exposed_secrets = []

        def replacement_dispatch(url, dependencies):
            exposed_secrets.append(dependencies.credential_secret_key)
            return original_dispatch(url, dependencies)

        def mutating_sink(_page) -> None:
            client._get_json_for_request = replacement_dispatch

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ):
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_secrets, [])

    def test_page_sink_cannot_replace_class_request_verifier(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )
        original_verifier = AlpacaMarketData._require_current_request_dependencies

        def mutating_sink(_page) -> None:
            AlpacaMarketData._require_current_request_dependencies = (
                lambda _self, _dependencies: None
            )

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ):
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            AlpacaMarketData._require_current_request_dependencies = (
                original_verifier
            )

        self.assertEqual(len(transport.requested_urls), 1)

    def test_page_sink_cannot_replace_module_request_dispatch(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )
        original_dispatch = alpaca_module.get_with_redirects
        exposed_headers = []

        def replacement_dispatch(*args, **kwargs):
            exposed_headers.append(dict(args[3]))
            return original_dispatch(*args, **kwargs)

        def mutating_sink(_page) -> None:
            alpaca_module.get_with_redirects = replacement_dispatch

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ):
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            alpaca_module.get_with_redirects = original_dispatch

        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_mutate_current_time_callable_code(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)

        def now() -> datetime:
            return NOW

        def replacement_now() -> datetime:
            return NOW + timedelta(hours=1)

        original_code = now.__code__
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=now,
        )

        def mutating_sink(_page) -> None:
            now.__code__ = replacement_now.__code__

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ):
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            now.__code__ = original_code

        self.assertEqual(len(transport.requested_urls), 1)

    def test_page_sink_cannot_replace_cache_put_callback(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)

        class RecordingCache:
            def __init__(self) -> None:
                self.calls = 0

            def put(self, _observation, _payload) -> None:
                self.calls += 1

        cache = RecordingCache()
        original_put = RecordingCache.put

        def replacement_put(self, _observation, _payload) -> None:
            self.calls += 1

        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
            cache=cache,  # type: ignore[arg-type]
        )

        def mutating_sink(_page) -> None:
            RecordingCache.put = replacement_put

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ):
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            RecordingCache.put = original_put

        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(cache.calls, 1)

    def test_page_sink_cannot_swap_mutable_transport_handler(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class MutableHandlerTransport:
            def __init__(self) -> None:
                self.handler = original_handler
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.handler(url, headers)

        transport = MutableHandlerTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            transport.handler = replacement_handler

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_mutate_inherited_transport_get_code(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        def handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        class BaseTransport:
            def __init__(self) -> None:
                self.handler = handler
                self.calls = 0
                self.requested_urls: list[str] = []
                self.exposed_headers: list[dict[str, str]] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.handler(url, headers)

        class InheritedTransport(BaseTransport):
            pass

        def replacement_get(
            self,
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            self.exposed_headers.append(dict(headers))
            return self.handler(url, headers)

        transport = InheritedTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )
        original_code = BaseTransport.get.__code__

        def mutating_sink(_page) -> None:
            BaseTransport.get.__code__ = replacement_get.__code__

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ) as raised:
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            BaseTransport.get.__code__ = original_code

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(transport.exposed_headers, [])

    def test_page_sink_cannot_swap_mutable_cache_handler(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)
        replacement_payloads = []

        def original_handler(_observation, payload: bytes) -> None:
            cache.persisted_payloads.append(payload)

        def replacement_handler(_observation, payload: bytes) -> None:
            replacement_payloads.append(payload)

        class MutableHandlerCache:
            def __init__(self) -> None:
                self.handler = original_handler
                self.calls = 0
                self.persisted_payloads: list[bytes] = []

            def put(self, observation, payload: bytes) -> None:
                self.calls += 1
                self.handler(observation, payload)

        cache = MutableHandlerCache()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
            cache=cache,  # type: ignore[arg-type]
        )

        def mutating_sink(_page) -> None:
            cache.handler = replacement_handler

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(cache.calls, 1)
        self.assertEqual(len(cache.persisted_payloads), 1)
        self.assertEqual(replacement_payloads, [])

    def test_page_sink_cannot_swap_callable_clock_delegate_or_state(
        self,
    ) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        class ClockState:
            def __init__(self, value: datetime) -> None:
                self.value = value
                self.uses = 0

        def original_delegate(state: ClockState) -> datetime:
            state.uses += 1
            return state.value

        replacement_calls = []

        def replacement_delegate(state: ClockState) -> datetime:
            replacement_calls.append(state)
            return state.value + timedelta(hours=1)

        class MutableClock:
            def __init__(self, state: ClockState) -> None:
                self.delegate = original_delegate
                self.state = state
                self.calls = 0
                self.returned_values: list[datetime] = []

            def __call__(self) -> datetime:
                self.calls += 1
                value = self.delegate(self.state)
                self.returned_values.append(value)
                return value

        for mutation in ("delegate", "state"):
            with self.subTest(mutation=mutation):
                transport = RoutingTransport(responder)
                original_state = ClockState(NOW)
                forged_state = ClockState(NOW + timedelta(hours=1))
                clock = MutableClock(original_state)
                client = AlpacaMarketData(
                    transport,
                    credentials(),
                    now=clock,
                )

                def mutating_sink(_page) -> None:
                    if mutation == "delegate":
                        clock.delegate = replacement_delegate
                    else:
                        clock.state = forged_state

                with self.assertRaisesRegex(
                    ProviderMalformedError,
                    "request dependencies",
                ) as raised:
                    client.daily_bars(
                        ("SPY",),
                        WINDOW,
                        page_sink=mutating_sink,
                    )

                self.assertIsNone(raised.exception.__context__)
                self.assertIsNone(raised.exception.__cause__)
                self.assertEqual(len(transport.requested_urls), 1)
                self.assertEqual(clock.calls, 2)
                self.assertEqual(original_state.uses, 2)
                self.assertEqual(forged_state.uses, 0)
                self.assertEqual(replacement_calls, [])

    def test_page_sink_cannot_swap_nested_namespace_handler(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class NestedConfigTransport:
            def __init__(self) -> None:
                self.config = SimpleNamespace(handler=original_handler)
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.config.handler(url, headers)

        transport = NestedConfigTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            transport.config.handler = replacement_handler

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_cyclic_route_table_entry(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        for route_kind in ("dict", "list"):
            with self.subTest(route_kind=route_kind):
                exposed_headers = []

                def replacement_handler(
                    url: str,
                    headers: dict[str, str],
                ) -> HttpResponse:
                    exposed_headers.append(dict(headers))
                    return original_handler(url, headers)

                if route_kind == "dict":
                    routes = {"active": original_handler}
                    routes["cycle"] = routes
                else:
                    routes = [original_handler]
                    routes.append(routes)

                class RouteTableTransport:
                    def __init__(self) -> None:
                        self.routes = routes
                        self.calls = 0
                        self.requested_urls: list[str] = []

                    def get(
                        self,
                        url: str,
                        headers: dict[str, str],
                    ) -> HttpResponse:
                        self.calls += 1
                        self.requested_urls.append(url)
                        handler = (
                            self.routes["active"]
                            if isinstance(self.routes, dict)
                            else self.routes[0]
                        )
                        return handler(url, headers)

                transport = RouteTableTransport()
                client = AlpacaMarketData(
                    transport,
                    credentials(),
                    now=lambda: NOW,
                )

                def mutating_sink(_page) -> None:
                    if isinstance(transport.routes, dict):
                        transport.routes["active"] = replacement_handler
                    else:
                        transport.routes[0] = replacement_handler

                with self.assertRaisesRegex(
                    ProviderMalformedError,
                    "request dependencies",
                ) as raised:
                    client.daily_bars(
                        ("SPY",),
                        WINDOW,
                        page_sink=mutating_sink,
                    )

                self.assertIsNone(raised.exception.__context__)
                self.assertIsNone(raised.exception.__cause__)
                self.assertEqual(transport.calls, 1)
                self.assertEqual(len(transport.requested_urls), 1)
                self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_deque_route_entry(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class DequeRouteTransport:
            def __init__(self) -> None:
                self.routes = deque([original_handler])
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.routes[0](url, headers)

        transport = DequeRouteTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            transport.routes[0] = replacement_handler

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_mapping_proxy_route_entry(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class ProxyRouteTransport:
            def __init__(self, routes: MappingProxyType) -> None:
                self.routes = routes
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.routes["handler"](url, headers)

        backing = {"handler": original_handler}
        transport = ProxyRouteTransport(MappingProxyType(backing))
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            backing["handler"] = replacement_handler

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_array_route_selector(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class ArrayRouteTransport:
            def __init__(self, selector: array, handlers: tuple) -> None:
                self.selector = selector
                self.handlers = handlers
                self.calls = 0
                self.requested_urls: list[str] = []
            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return self.handlers[self.selector[0]](url, headers)

        selector = array("b", [0])
        transport = ArrayRouteTransport(
            selector,
            (original_handler, replacement_handler),
        )
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            selector[0] = 1

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "request dependencies",
        ) as raised:
            client.daily_bars(
                ("SPY",),
                WINDOW,
                page_sink=mutating_sink,
            )

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_module_global_route_entry(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class GlobalRouteTransport:
            def __init__(self) -> None:
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                handler = _GLOBAL_ROUTE_TABLE["handler"]
                assert callable(handler)
                return handler(url, headers)

        _GLOBAL_ROUTE_TABLE["handler"] = original_handler
        transport = GlobalRouteTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            _GLOBAL_ROUTE_TABLE["handler"] = replacement_handler

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ) as raised:
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            _GLOBAL_ROUTE_TABLE.clear()

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_page_sink_cannot_swap_module_holder_handler(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        exposed_headers = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        def replacement_handler(
            url: str,
            headers: dict[str, str],
        ) -> HttpResponse:
            exposed_headers.append(dict(headers))
            return original_handler(url, headers)

        class ModuleHolderTransport:
            def __init__(self) -> None:
                self.calls = 0
                self.requested_urls: list[str] = []

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.calls += 1
                self.requested_urls.append(url)
                return _GLOBAL_ROUTE_HOLDER.handler(url, headers)

        _GLOBAL_ROUTE_HOLDER.handler = original_handler
        transport = ModuleHolderTransport()
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        def mutating_sink(_page) -> None:
            _GLOBAL_ROUTE_HOLDER.handler = replacement_handler

        try:
            with self.assertRaisesRegex(
                ProviderMalformedError,
                "request dependencies",
            ) as raised:
                client.daily_bars(
                    ("SPY",),
                    WINDOW,
                    page_sink=mutating_sink,
                )
        finally:
            del _GLOBAL_ROUTE_HOLDER.handler

        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(transport.calls, 1)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(exposed_headers, [])

    def test_trusted_transport_can_increment_field_named_state(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        class StatefulTransport(RoutingTransport):
            def __init__(self) -> None:
                super().__init__(responder)
                self.state = 0

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                self.state += 1
                return super().get(url, headers)

        transport = StatefulTransport()
        try:
            cohort = AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(("SPY",), WINDOW)
        except ProviderMalformedError as error:
            self.fail(f"trusted transport state was rejected: {error}")

        self.assertTrue(is_issued_provider_fetch_cohort(cohort))
        self.assertEqual(transport.state, 2)
        self.assertEqual(len(transport.requested_urls), 2)

    def test_request_graph_does_not_evaluate_lazy_annotations(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        annotation_calls = []

        def original_handler(
            url: str,
            _headers: dict[str, str],
        ) -> HttpResponse:
            status, body = responder(url)
            return HttpResponse(
                status=status,
                headers=(("Content-Type", "application/json"),),
                body=body.encode("utf-8"),
                url=url,
            )

        if not hasattr(original_handler, "__annotate__"):
            self.skipTest("lazy function annotations require Python 3.14")

        def lazy_annotations(format_code: int) -> dict[str, str]:
            annotation_calls.append(format_code)
            return {"return": "HttpResponse"}

        original_handler.__annotations__ = None
        original_handler.__annotate__ = lazy_annotations

        class HandlerTransport:
            def __init__(self) -> None:
                self.handler = original_handler

            def get(
                self,
                url: str,
                headers: dict[str, str],
            ) -> HttpResponse:
                return self.handler(url, headers)

        cohort = AlpacaMarketData(
            HandlerTransport(),
            credentials(),
            now=lambda: NOW,
        ).daily_bars(("SPY",), WINDOW)

        self.assertTrue(is_issued_provider_fetch_cohort(cohort))
        self.assertEqual(annotation_calls, [])

    def test_stateful_transport_remains_api_compatible(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        class StatefulTransport(RoutingTransport):
            def __init__(self) -> None:
                super().__init__(responder)
                self.calls = 0

            def get(self, url: str, headers: dict[str, str]) -> HttpResponse:
                self.calls += 1
                return super().get(url, headers)

        transport = StatefulTransport()
        try:
            cohort = AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(("SPY",), WINDOW)
        except ProviderMalformedError as error:
            self.fail(f"stateful transport was rejected: {error}")

        self.assertTrue(is_issued_provider_fetch_cohort(cohort))
        self.assertEqual(transport.calls, 2)

    def test_page_sink_cannot_substitute_equal_receipt_components(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()

        def replace_page(_client, bundle) -> None:
            object.__setattr__(bundle, "page", replace(bundle.page))

        def replace_payload(_client, bundle) -> None:
            object.__setattr__(
                bundle,
                "payload",
                memoryview(bundle.payload).tobytes(),
            )

        def replace_observation(client, bundle) -> None:
            replacement = replace(bundle.observation)
            client._observations[replacement.observation_id] = replacement
            object.__setattr__(bundle, "observation", replacement)

        for name, mutation in (
            ("page", replace_page),
            ("payload", replace_payload),
            ("observation", replace_observation),
        ):
            with self.subTest(component=name):
                transport = RoutingTransport(responder)
                client = AlpacaMarketData(
                    transport,
                    credentials(),
                    now=lambda: NOW,
                )
                pages = []

                def mutating_sink(page) -> None:
                    pages.append(page)
                    mutation(client, page)

                with self.assertRaisesRegex(
                    ProviderMalformedError,
                    "page authority",
                ):
                    client.daily_bars(
                        ("SPY",),
                        WINDOW,
                        page_sink=mutating_sink,
                    )

                self.assertEqual(len(transport.requested_urls), 1)
                self.assertEqual(len(pages), 1)
                self.assertFalse(
                    is_issued_provider_fetch_page_bundle(pages[0])
                )

    def test_completed_fetch_authority_releases_receipts_and_registry(
        self,
    ) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        client = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        )
        cohort = client.daily_bars(("SPY",), WINDOW)
        bundle = read_provider_fetch_bundle(cohort)
        page = bundle.pages[0]
        manifest = bundle.manifest
        page_identity = id(page)
        page_reference = weakref.ref(page)
        manifest_reference = weakref.ref(manifest)

        del bundle, cohort, manifest, page
        gc.collect()

        self.assertIsNone(manifest_reference())
        self.assertIsNone(page_reference())
        self.assertEqual(client._issued_fetch_manifests, {})
        self.assertNotIn(
            page_identity,
            alpaca_module._ISSUED_PROVIDER_FETCH_PAGE_BUNDLES,
        )

        client_reference = weakref.ref(client)
        del client
        gc.collect()
        self.assertIsNone(client_reference())

    def test_valid_page_is_disclosed_before_later_page_failure(self) -> None:
        responder, payloads = self._two_page_daily_bar_responder(
            terminal=False
        )
        transport = RoutingTransport(responder)
        pages = []
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        with self.assertRaises(ProviderIncompleteError):
            client.daily_bars(("SPY",), WINDOW, page_sink=pages.append)

        self.assertEqual(len(transport.requested_urls), 2)
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0].payload, payloads[0])
        self.assertEqual(pages[0].page.page_ordinal, 1)
        self.assertIsNone(pages[0].page.request_page_token)
        self.assertEqual(pages[0].page.next_page_token, "page-2")
        self.assertTrue(is_issued_provider_fetch_page_bundle(pages[0]))
        self.assertEqual(client._issued_fetch_manifests, {})
        with self.assertRaisesRegex(ValueError, "authority"):
            read_provider_fetch_bundle(pages[0])

    def test_valid_page_is_disclosed_before_later_transport_failure(self) -> None:
        responder, payloads = self._two_page_daily_bar_responder()

        def failing_responder(url: str) -> tuple[int, str]:
            if parse_qs(urlsplit(url).query).get("page_token") == ["page-2"]:
                raise HttpTransportError("transport callback canary")
            return responder(url)

        transport = RoutingTransport(failing_responder)
        pages = []
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: NOW,
        )

        with self.assertRaises(HttpTransportError):
            client.daily_bars(("SPY",), WINDOW, page_sink=pages.append)

        self.assertGreaterEqual(len(transport.requested_urls), 2)
        self.assertTrue(
            all(
                parse_qs(urlsplit(url).query).get("page_token")
                == ["page-2"]
                for url in transport.requested_urls[1:]
            )
        )
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0].payload, payloads[0])
        self.assertTrue(is_issued_provider_fetch_page_bundle(pages[0]))
        self.assertEqual(client._issued_fetch_manifests, {})

    def test_complete_pagination_discloses_exact_terminal_pages_once(self) -> None:
        responder, payloads = self._two_page_daily_bar_responder()
        pages = []
        cohort = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        ).daily_bars(("SPY",), WINDOW, page_sink=pages.append)

        bundle = read_provider_fetch_bundle(cohort)
        self.assertEqual(tuple(page.payload for page in pages), payloads)
        self.assertEqual(tuple(pages), bundle.pages)
        self.assertTrue(
            all(
                disclosed is terminal
                for disclosed, terminal in zip(
                    pages,
                    bundle.pages,
                    strict=True,
                )
            )
        )
        self.assertEqual(len({id(page) for page in pages}), 2)
        self.assertTrue(
            all(
                is_issued_provider_fetch_page_bundle(page)
                for page in pages
            )
        )

    def test_page_sink_copy_and_tamper_cannot_mint_page_authority(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        copied_pages = []

        def copy_sink(page) -> None:
            copied_pages.append(copy.copy(page))
            copied_pages.append(replace(page))

        cohort = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        ).daily_bars(("SPY",), WINDOW, page_sink=copy_sink)
        self.assertTrue(is_issued_provider_fetch_cohort(cohort))
        self.assertEqual(len(copied_pages), 4)
        self.assertTrue(
            all(
                not is_issued_provider_fetch_page_bundle(page)
                for page in copied_pages
            )
        )

        tamper_transport = RoutingTransport(responder)
        tampered_pages = []

        def tamper_sink(page) -> None:
            tampered_pages.append(page)
            object.__setattr__(page, "payload", b'{"secret":"changed"}')

        with self.assertRaisesRegex(
            ProviderMalformedError,
            "page authority",
        ):
            AlpacaMarketData(
                tamper_transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(("SPY",), WINDOW, page_sink=tamper_sink)
        self.assertEqual(len(tamper_transport.requested_urls), 1)
        self.assertEqual(len(tampered_pages), 1)
        self.assertFalse(
            is_issued_provider_fetch_page_bundle(tampered_pages[0])
        )

    def test_page_sink_exception_is_redacted_and_stops_pagination(self) -> None:
        responder, _payloads = self._two_page_daily_bar_responder()
        transport = RoutingTransport(responder)
        pages = []

        def failing_sink(page) -> None:
            pages.append(page)
            raise RuntimeError("fixture-secret-key callback canary")

        with self.assertRaises(ProviderDataError) as raised:
            AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(("SPY",), WINDOW, page_sink=failing_sink)
        self.assertNotIn("fixture-secret-key", str(raised.exception))
        self.assertIsNone(raised.exception.__context__)
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(len(transport.requested_urls), 1)
        self.assertEqual(len(pages), 1)
        self.assertTrue(is_issued_provider_fetch_page_bundle(pages[0]))

    def test_malformed_option_page_is_not_disclosed(self) -> None:
        body = (
            '{"snapshots":{"SPY261332C00650000":{'
            '"latestQuote":{"t":"2026-08-14T12:58:00Z",'
            '"bp":"1.00","ap":"1.05"}}},'
            '"next_page_token":null}'
        )
        pages = []

        with self.assertRaises(ProviderMalformedError):
            AlpacaMarketData(
                RoutingTransport(lambda _: (200, body)),
                credentials(),
                now=lambda: NOW,
            ).option_chain("SPY", page_sink=pages.append)

        self.assertEqual(pages, [])

    def test_invalid_page_sink_is_rejected_before_provider_access(self) -> None:
        transport = RoutingTransport(lambda _: (200, "{}"))
        with self.assertRaises(TypeError):
            AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(("SPY",), WINDOW, page_sink=object())
        self.assertEqual(transport.requested_urls, [])

    def test_replay_only_scope_cannot_be_reminted_as_ingestible(self) -> None:
        client = AlpacaMarketData(
            RoutingTransport(
                lambda _: (
                    200,
                    '{"quotes":{"SPY":['
                    '{"t":"2026-05-01T19:59:59Z","bp":1,"ap":1.01}'
                    ']},"next_page_token":null}',
                )
            ),
            credentials(),
            now=lambda: NOW,
        )
        cohort = client.historical_quotes(("SPY",), WINDOW)
        manifest = read_provider_fetch_bundle(cohort).manifest
        preexisting_twin = alpaca_module._issue_provider_fetch_cohort(
            owner=client,
            manifest=manifest,
            values=cohort,
        )

        alpaca_module._mark_provider_fetch_cohort_replay_only(cohort)
        reminted = alpaca_module._issue_provider_fetch_cohort(
            owner=client,
            manifest=manifest,
            values=cohort,
        )

        self.assertTrue(is_issued_provider_fetch_cohort(reminted))
        self.assertFalse(is_ingestible_provider_fetch_cohort(cohort))
        self.assertFalse(is_ingestible_provider_fetch_cohort(preexisting_twin))
        self.assertFalse(is_ingestible_provider_fetch_cohort(reminted))

    def test_replay_only_option_chain_cannot_be_reminted_as_ingestible(
        self,
    ) -> None:
        client = AlpacaMarketData(
            FixtureTransport("providers/alpaca/option-snapshots.json"),
            credentials(),
            now=lambda: NOW,
        )
        chain = client.option_chain("SPY")
        manifest = read_provider_fetch_bundle(chain).manifest
        preexisting_twin = alpaca_module._issue_provider_option_chain(
            owner=client,
            manifest=manifest,
            snapshots=chain,
        )
        self.assertTrue(is_ingestible_provider_option_chain(chain))
        self.assertTrue(is_ingestible_provider_option_chain(preexisting_twin))

        alpaca_module._mark_provider_option_chain_replay_only(chain)
        reminted = alpaca_module._issue_provider_option_chain(
            owner=client,
            manifest=manifest,
            snapshots=chain,
        )

        self.assertTrue(is_issued_provider_option_chain(reminted))
        self.assertFalse(is_ingestible_provider_option_chain(chain))
        self.assertFalse(
            is_ingestible_provider_option_chain(preexisting_twin)
        )
        self.assertFalse(is_ingestible_provider_option_chain(reminted))

    def test_intraday_bar_page_metadata_recomputes_from_exact_raw_page(self) -> None:
        payload = (
            b'{"bars":{"AAPL":[{"c":"20.40","h":"20.50",'
            b'"l":"20.30","o":"20.35","t":"2026-08-17T14:00:00Z",'
            b'"v":1000}]},"next_page_token":null}'
        )
        retrieved_at = datetime(2026, 8, 17, 20, 20, tzinfo=UTC)

        metadata = recompute_alpaca_page_metadata(
            payload=payload,
            request_url=(
                "https://data.alpaca.markets/v2/stocks/bars?"
                "adjustment=split&end=2026-08-17T20%3A00%3A00Z&feed=sip&"
                "start=2026-08-17T13%3A30%3A00Z&symbols=AAPL&timeframe=1Min"
            ),
            source_type="ALPACA_INTRADAY_BARS",
            retrieved_at=retrieved_at,
        )

        self.assertEqual(
            metadata.source_time,
            datetime(2026, 8, 17, 14, 0, tzinfo=UTC),
        )
        self.assertEqual(metadata.delay_seconds, 22_800)
        self.assertEqual(len(metadata.payload_sha256), 64)

    def test_incomplete_pagination_rejects_entire_cohort(self) -> None:
        transport = FixtureTransport("providers/alpaca/partial-page.json")
        window = TimeWindow(
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 8, 13, 20, tzinfo=UTC),
        )
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: NOW,
            ).daily_bars(["SPY", "QQQ"], window)

    def test_daily_bars_are_split_adjusted_sip_and_paginated_to_explicit_null(self) -> None:
        transport = FixtureTransport("providers/alpaca/complete-bars.json")
        client = AlpacaMarketData(transport, credentials(), now=lambda: NOW)

        bars = client.daily_bars(["qqq", "SPY"], WINDOW)

        self.assertEqual(tuple(bars), ("QQQ", "SPY"))
        self.assertEqual(bars["SPY"][0].close, Decimal("652.10"))
        self.assertEqual(bars["SPY"][0].volume, 70_000_000)
        self.assertEqual(bars["SPY"][0].feed, "sip")
        self.assertEqual(bars["SPY"][0].adjustment, "split")
        self.assertEqual(bars["QQQ"][0].timestamp.tzinfo, UTC)
        self.assertTrue(
            all(
                is_issued_provider_fetch_page_bundle(page)
                for page in read_provider_fetch_bundle(bars).pages
            )
        )
        self.assertEqual(transport.remaining_responses, 0)
        for url in transport.requested_urls:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["feed"], ["sip"])
            self.assertEqual(query["adjustment"], ["split"])
        sent = transport.requested_headers[0]
        self.assertEqual(sent["APCA-API-KEY-ID"], "fixture-key-id")
        self.assertEqual(sent["APCA-API-SECRET-KEY"], "fixture-secret-key")
        self.assertEqual(sent["Accept-Encoding"], "identity")

    def test_historical_quotes_require_sip_and_complete_symbol_cohort(self) -> None:
        transport = FixtureTransport("providers/alpaca/complete-quotes.json")
        quotes = AlpacaMarketData(
            transport, credentials(), now=lambda: NOW
        ).historical_quotes(["SPY", "QQQ"], WINDOW)

        self.assertEqual(tuple(quotes), ("QQQ", "SPY"))
        self.assertEqual(quotes["SPY"][0].bid, Decimal("651.90"))
        self.assertEqual(quotes["SPY"][0].ask, Decimal("652.10"))
        self.assertEqual(quotes["SPY"][0].feed, "sip")
        query = parse_qs(urlsplit(transport.requested_urls[0]).query)
        self.assertEqual(query["feed"], ["sip"])

    def test_latest_quotes_are_explicit_iex_fresh_and_complete(self) -> None:
        transport = FixtureTransport("providers/alpaca/latest-iex.json")
        quotes = AlpacaMarketData(
            transport, credentials(), now=lambda: NOW
        ).latest_iex_quotes(["SPY", "QQQ"])

        self.assertEqual(tuple(quotes), ("QQQ", "SPY"))
        self.assertTrue(all(quote.feed == "iex" for quote in quotes.values()))
        self.assertTrue(all(quote.age_seconds <= 300 for quote in quotes.values()))
        query = parse_qs(urlsplit(transport.requested_urls[0]).query)
        self.assertEqual(query["feed"], ["iex"])

    def test_stale_or_wrong_feed_latest_quote_blocks_entire_result(self) -> None:
        bodies = (
            '{"quotes":{"SPY":{"t":"2026-08-14T12:54:59Z","bp":"651.9","ap":"652.1"}},"next_page_token":null}',
            '{"quotes":{"SPY":{"t":"2026-08-14T12:59:00Z","bp":"651.9","ap":"652.1","feed":"sip"}},"next_page_token":null}',
        )
        for body in bodies:
            with self.subTest(body=body):
                transport = RoutingTransport(lambda _: (200, body))
                with self.assertRaises(ProviderDataError):
                    AlpacaMarketData(
                        transport, credentials(), now=lambda: NOW
                    ).latest_iex_quotes(["SPY"])

    def test_historical_window_must_end_at_least_sixteen_minutes_ago(self) -> None:
        transport = RoutingTransport(lambda _: (200, "{}"))
        too_fresh = TimeWindow(
            datetime(2026, 8, 13, tzinfo=UTC),
            datetime(2026, 8, 14, 12, 44, 1, tzinfo=UTC),
        )
        with self.assertRaises(ProviderDataError):
            AlpacaMarketData(
                transport, credentials(), now=lambda: NOW
            ).daily_bars(["SPY"], too_fresh)
        self.assertEqual(transport.requested_urls, [])

    def test_historical_observations_must_stay_inside_requested_window(self) -> None:
        bar_bodies = (
            '{"bars":{"SPY":[{"t":"2026-04-30T23:59:59Z","o":1,"h":1,"l":1,"c":1,"v":1}]},"next_page_token":null}',
            '{"bars":{"SPY":[{"t":"2026-08-13T20:00:01Z","o":1,"h":1,"l":1,"c":1,"v":1}]},"next_page_token":null}',
        )
        for body in bar_bodies:
            with self.subTest(kind="bar", body=body):
                transport = RoutingTransport(lambda _: (200, body))
                with self.assertRaises(ProviderDataError):
                    AlpacaMarketData(
                        transport, credentials(), now=lambda: NOW
                    ).daily_bars(["SPY"], WINDOW)

        quote_body = (
            '{"quotes":{"SPY":[{"t":"2026-08-13T20:00:01Z",'
            '"bp":"1","ap":"1.01"}]},"next_page_token":null}'
        )
        transport = RoutingTransport(lambda _: (200, quote_body))
        with self.assertRaises(ProviderDataError):
            AlpacaMarketData(
                transport, credentials(), now=lambda: NOW
            ).historical_quotes(["SPY"], WINDOW)

    def test_duplicate_page_tokens_and_missing_symbols_fail_closed(self) -> None:
        page = (
            '{"bars":{"SPY":[{"t":"2026-08-13T20:00:00Z",'
            '"o":"650","h":"653","l":"648","c":"652","v":1}]},'
            '"next_page_token":"same"}'
        )
        transport = RoutingTransport(lambda _: (200, page))
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(
                transport, credentials(), now=lambda: NOW
            ).daily_bars(["SPY"], WINDOW)
        self.assertEqual(len(transport.requested_urls), 2)

    def test_daily_bar_cohort_requires_identical_aligned_timestamps(self) -> None:
        body = (
            '{"bars":{'
            '"SPY":[{"t":"2026-08-13T20:00:00Z","o":1,"h":1,"l":1,"c":1,"v":1}],'
            '"QQQ":[{"t":"2026-08-12T20:00:00Z","o":1,"h":1,"l":1,"c":1,"v":1}]'
            '},"next_page_token":null}'
        )
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(
                RoutingTransport(lambda _: (200, body)),
                credentials(),
                now=lambda: NOW,
            ).daily_bars(["SPY", "QQQ"], WINDOW)

    def test_terminal_daily_bar_must_reach_the_expected_completed_session(self) -> None:
        body = (
            '{"bars":{'
            '"SPY":[{"t":"2026-05-01T20:00:00Z",'
            '"o":1,"h":1,"l":1,"c":1,"v":1}],'
            '"QQQ":[{"t":"2026-05-01T20:00:00Z",'
            '"o":1,"h":1,"l":1,"c":1,"v":1}]'
            '},"next_page_token":null}'
        )
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(
                RoutingTransport(lambda _: (200, body)),
                credentials(),
                now=lambda: NOW,
            ).daily_bars(["SPY", "QQQ"], WINDOW)

    def test_daily_bar_terminal_uses_the_authentic_eastern_session_date(self) -> None:
        body = (
            '{"bars":{'
            '"SPY":[{"t":"2026-08-13T04:00:00Z",'
            '"o":1,"h":1,"l":1,"c":1,"v":1}],'
            '"QQQ":[{"t":"2026-08-13T04:00:00Z",'
            '"o":1,"h":1,"l":1,"c":1,"v":1}]'
            '},"next_page_token":null}'
        )

        bars = AlpacaMarketData(
            RoutingTransport(lambda _: (200, body)),
            credentials(),
            now=lambda: NOW,
        ).daily_bars(["SPY", "QQQ"], WINDOW)

        self.assertEqual(bars["SPY"][-1].timestamp, datetime(2026, 8, 13, 4, tzinfo=UTC))
        self.assertEqual(bars["QQQ"][-1].timestamp, datetime(2026, 8, 13, 4, tzinfo=UTC))

    def test_terminal_quotes_allow_quiet_or_empty_symbol_cohorts(self) -> None:
        cases = (
            (
                "quiet",
                (
                    '{"quotes":{'
                    '"SPY":[{"t":"2026-05-01T19:59:59Z","bp":1,"ap":1.01}],'
                    '"QQQ":[{"t":"2026-05-01T19:59:58Z","bp":1,"ap":1.01}]'
                    '},"next_page_token":null}'
                ),
                ("SPY", "QQQ"),
                (1, 1),
            ),
            (
                "empty",
                '{"quotes":{"AAPL":[]},"next_page_token":null}',
                ("AAPL",),
                (0,),
            ),
        )
        for label, body, symbols, expected_counts in cases:
            with self.subTest(label=label):
                cohort = AlpacaMarketData(
                    RoutingTransport(lambda _: (200, body)),
                    credentials(),
                    now=lambda: NOW,
                ).historical_quotes(symbols, WINDOW)

                ordered_symbols = tuple(sorted(symbols))
                self.assertEqual(tuple(cohort), ordered_symbols)
                self.assertEqual(
                    tuple(len(cohort[symbol]) for symbol in ordered_symbols),
                    expected_counts,
                )
                self.assertTrue(is_issued_provider_fetch_cohort(cohort))
                self.assertTrue(is_ingestible_provider_fetch_cohort(cohort))

    def test_symbols_are_validated_before_url_construction(self) -> None:
        transport = RoutingTransport(lambda _: (200, "{}"))
        poisoned = ("SPY&feed=iex", "../SPY", "", "BRK/B")
        for symbol in poisoned:
            with self.subTest(symbol=symbol), self.assertRaises(ValueError):
                AlpacaMarketData(
                    transport, credentials(), now=lambda: NOW
                ).latest_iex_quotes([symbol])
        self.assertEqual(transport.requested_urls, [])

    def test_option_snapshot_is_indicative_and_never_claims_open_interest(self) -> None:
        transport = FixtureTransport("providers/alpaca/option-snapshots.json")
        snapshots = AlpacaMarketData(
            transport, credentials(), now=lambda: NOW
        ).option_chain("SPY")

        self.assertEqual(len(snapshots), 1)
        self.assertTrue(is_issued_provider_option_chain(snapshots))
        snapshot = snapshots[0]
        self.assertEqual(snapshot.occ_symbol, "SPY260918C00650000")
        self.assertEqual(snapshot.feed, "indicative")
        self.assertIsNone(snapshot.open_interest)
        self.assertEqual(snapshot.daily_volume, 234)
        self.assertNotIn("contracts", transport.requested_urls[0])
        self.assertIn("/v1beta1/options/snapshots/SPY", transport.requested_urls[0])
        self.assertIs(
            read_provider_fetch_bundle(snapshots).manifest,
            read_provider_fetch_bundle(snapshot).manifest,
        )
        copied_chain = copy.copy(snapshots)
        self.assertFalse(is_issued_provider_option_chain(copied_chain))
        with self.assertRaisesRegex(ValueError, "authority"):
            read_provider_fetch_bundle(copied_chain)

    def test_option_chain_authority_revokes_when_page_observation_changes(
        self,
    ) -> None:
        mutations = (
            ("source_timestamp", NOW - timedelta(seconds=1)),
            ("feed", "sip"),
            ("delay_seconds", 1),
            ("content_hash", "0" * 64),
        )
        for field_name, replacement in mutations:
            with self.subTest(field_name=field_name):
                chain = AlpacaMarketData(
                    FixtureTransport(
                        "providers/alpaca/option-snapshots.json"
                    ),
                    credentials(),
                    now=lambda: NOW,
                ).option_chain("SPY")
                observation = read_provider_fetch_bundle(chain).pages[
                    0
                ].observation
                self.assertTrue(is_issued_provider_option_chain(chain))
                self.assertTrue(is_ingestible_provider_option_chain(chain))

                object.__setattr__(
                    observation,
                    field_name,
                    replacement,
                )

                self.assertFalse(is_issued_provider_option_chain(chain))
                self.assertFalse(is_ingestible_provider_option_chain(chain))
                with self.assertRaisesRegex(ValueError, "authority"):
                    read_provider_fetch_bundle(chain)

    def test_option_snapshot_is_bound_to_exact_raw_provider_item(self) -> None:
        client = AlpacaMarketData(
            FixtureTransport("providers/alpaca/option-snapshots.json"),
            credentials(),
            now=lambda: NOW,
        )
        snapshot = client.option_chain("SPY")[0]

        self.assertTrue(is_issued_normalized_market_fact(snapshot))
        source = alpaca_module._normalized_market_fact_source(snapshot)
        bundle = read_provider_fetch_bundle(snapshot)
        self.assertEqual(source.kind, "OPTION_SNAPSHOT")
        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(source.feed, "indicative")
        self.assertEqual(source.page_ordinal, 1)
        self.assertEqual(source.source_item_ordinal, 1)
        self.assertEqual(
            source.source_item_path,
            "$.snapshots.SPY260918C00650000",
        )
        self.assertEqual(source.fetch_manifest.collection, "snapshots")
        self.assertEqual(source.fetch_manifest.requested_symbols, ("SPY",))
        self.assertIs(bundle.manifest, source.fetch_manifest)
        self.assertEqual(len(bundle.pages), 1)
        self.assertIn(b'"SPY260918C00650000"', bundle.pages[0].payload)

        copied = copy.copy(snapshot)
        self.assertFalse(is_issued_normalized_market_fact(copied))
        with self.assertRaisesRegex(ValueError, "authority"):
            read_provider_fetch_bundle(copied)

        forged = replace(snapshot, ask=Decimal("99.99"))
        with self.assertRaisesRegex(ProviderDataError, "raw provider item"):
            alpaca_module._issue_market_fact_from_fetch(
                forged,
                owner=client,
                fetch_manifest=source.fetch_manifest,
                page_ordinal=source.page_ordinal,
                source_item_ordinal=source.source_item_ordinal,
                source_item_path=source.source_item_path,
            )
        self.assertFalse(is_issued_normalized_market_fact(forged))

    def test_option_snapshot_tamper_invalidates_raw_bundle_authority(self) -> None:
        snapshot = AlpacaMarketData(
            FixtureTransport("providers/alpaca/option-snapshots.json"),
            credentials(),
            now=lambda: NOW,
        ).option_chain("SPY")[0]
        self.assertTrue(is_issued_normalized_market_fact(snapshot))

        object.__setattr__(snapshot, "ask", Decimal("99.99"))

        self.assertFalse(is_issued_normalized_market_fact(snapshot))
        with self.assertRaisesRegex(ValueError, "authority"):
            read_provider_fetch_bundle(snapshot)

    def test_option_snapshot_pagination_retains_exact_owner_and_manifest(self) -> None:
        first_body = (
            '{"snapshots":{"SPY260918C00650000":{'
            '"latestQuote":{"t":"2026-08-14T12:58:00Z",'
            '"bp":"1.00","ap":"1.05"},'
            '"greeks":{"delta":"0.35"},"dailyBar":{"v":150}}},'
            '"next_page_token":"page-2"}'
        )
        second_body = (
            '{"snapshots":{"SPY260918C00660000":{'
            '"latestQuote":{"t":"2026-08-14T12:59:00Z",'
            '"bp":"0.90","ap":"0.95"},'
            '"greeks":{"delta":"0.33"},"dailyBar":{"v":125}}},'
            '"next_page_token":null}'
        )

        def responder(url: str) -> tuple[int, str]:
            token = parse_qs(urlsplit(url).query).get("page_token")
            return (200, second_body if token == ["page-2"] else first_body)

        first_client = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        )
        snapshots = first_client.option_chain("SPY")
        self.assertEqual(
            tuple(snapshot.occ_symbol for snapshot in snapshots),
            ("SPY260918C00650000", "SPY260918C00660000"),
        )
        first_source = alpaca_module._normalized_market_fact_source(
            snapshots[0]
        )
        second_source = alpaca_module._normalized_market_fact_source(
            snapshots[1]
        )
        self.assertIs(first_source.fetch_manifest, second_source.fetch_manifest)
        self.assertEqual((first_source.page_ordinal, second_source.page_ordinal), (1, 2))
        self.assertEqual(
            (first_source.source_item_ordinal, second_source.source_item_ordinal),
            (1, 1),
        )
        bundle = read_provider_fetch_bundle(snapshots[0])
        self.assertEqual(
            tuple(page.payload for page in bundle.pages),
            (first_body.encode("utf-8"), second_body.encode("utf-8")),
        )
        self.assertEqual(
            tuple(page.page.request_page_token for page in bundle.pages),
            (None, "page-2"),
        )
        self.assertTrue(
            normalized_market_facts_share_owner(snapshots[0], snapshots[1])
        )
        self.assertTrue(provider_fetch_cohorts_share_owner(snapshots))

        other_snapshot = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        ).option_chain("SPY")[0]
        self.assertFalse(
            normalized_market_facts_share_owner(snapshots[0], other_snapshot)
        )
        other_chain = AlpacaMarketData(
            RoutingTransport(responder),
            credentials(),
            now=lambda: NOW,
        ).option_chain("SPY")
        self.assertFalse(
            provider_fetch_cohorts_share_owner(snapshots, other_chain)
        )

    def test_empty_option_response_is_not_an_ambiguous_success(self) -> None:
        transport = RoutingTransport(
            lambda _: (200, '{"snapshots":{},"next_page_token":null}')
        )
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(
                transport, credentials(), now=lambda: NOW
            ).option_chain("SPY")

    def test_zero_option_bid_is_preserved_for_later_liquidity_rejection(self) -> None:
        body = (
            '{"snapshots":{"SPY260918C00650000":{'
            '"latestQuote":{"t":"2026-08-14T12:59:00Z","bp":"0","ap":"0.10"},'
            '"greeks":{"delta":"0.35"},"dailyBar":{"v":0}}},'
            '"next_page_token":null}'
        )
        snapshot = AlpacaMarketData(
            RoutingTransport(lambda _: (200, body)),
            credentials(),
            now=lambda: NOW,
        ).option_chain("SPY")[0]
        self.assertEqual(snapshot.bid, Decimal("0"))

    def test_future_option_timestamp_and_invalid_occ_date_fail_as_provider_data(self) -> None:
        bodies = (
            (
                '{"snapshots":{"SPY260918C00650000":{"latestQuote":{'
                '"t":"2026-08-14T13:00:00.000001Z","bp":"1","ap":"1.01"}}},'
                '"next_page_token":null}'
            ),
            (
                '{"snapshots":{"SPY261332C00650000":{"latestQuote":{'
                '"t":"2026-08-14T12:59:00Z","bp":"1","ap":"1.01"}}},'
                '"next_page_token":null}'
            ),
        )
        for body in bodies:
            with self.subTest(body=body), self.assertRaises(ProviderDataError):
                AlpacaMarketData(
                    RoutingTransport(lambda _: (200, body)),
                    credentials(),
                    now=lambda: NOW,
                ).option_chain("SPY")

    def test_iex_freshness_does_not_truncate_fractional_age_or_future_time(self) -> None:
        timestamps = (
            "2026-08-14T12:54:59.999999Z",
            "2026-08-14T13:00:00.000001Z",
        )
        for timestamp in timestamps:
            body = (
                '{"quotes":{"SPY":{"t":"'
                + timestamp
                + '","bp":"1","ap":"1.01"}},"next_page_token":null}'
            )
            with self.subTest(timestamp=timestamp), self.assertRaises(
                ProviderDataError
            ):
                AlpacaMarketData(
                    RoutingTransport(lambda _: (200, body)),
                    credentials(),
                    now=lambda: NOW,
                ).latest_iex_quotes(["SPY"])

    def test_smoke_reports_auth_sip_and_iex_separately_without_raising(self) -> None:
        def responder(url: str) -> tuple[int, str]:
            query = parse_qs(urlsplit(url).query)
            if urlsplit(url).path.endswith("/bars"):
                return 403, '{"message":"subscription does not permit SIP"}'
            if query.get("feed") == ["iex"]:
                return 200, (
                    '{"quotes":{"SPY":{"t":"2026-08-14T12:59:00Z",'
                    '"bp":"651.9","ap":"652.1"}},"next_page_token":null}'
                )
            return 401, '{"message":"unauthorized"}'

        transport = RoutingTransport(responder)
        result = AlpacaMarketData(
            transport, credentials(), now=lambda: NOW
        ).smoke(completed_session=SMOKE_WINDOW)

        self.assertTrue(result.authentication_ok)
        self.assertFalse(result.historical_sip_ok)
        self.assertTrue(result.latest_iex_fresh)
        self.assertEqual(result.status, "BLOCKED_ENTITLEMENT")
        self.assertTrue(
            all(urlsplit(url).hostname == "data.alpaca.markets" for url in transport.requested_urls)
        )

    def test_smoke_uses_an_explicit_known_completed_market_session(self) -> None:
        weekend_now = datetime(2026, 8, 15, 13, 0, tzinfo=UTC)

        def responder(url: str) -> tuple[int, str]:
            if urlsplit(url).path.endswith("/bars"):
                return 200, (
                    '{"bars":{"SPY":[{"t":"2026-08-13T04:00:00Z",'
                    '"o":1,"h":1,"l":1,"c":1,"v":1}]},'
                    '"next_page_token":null}'
                )
            return 200, (
                '{"quotes":{"SPY":{"t":"2026-08-15T12:59:00Z",'
                '"bp":1,"ap":1.01}},"next_page_token":null}'
            )

        transport = RoutingTransport(responder)
        result = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: weekend_now,
        ).smoke(completed_session=SMOKE_WINDOW)

        self.assertTrue(result.historical_sip_ok)
        bar_url = next(url for url in transport.requested_urls if urlsplit(url).path.endswith("/bars"))
        query = parse_qs(urlsplit(bar_url).query)
        self.assertEqual(query["start"], ["2026-08-13T04:00:00Z"])
        self.assertEqual(query["end"], ["2026-08-13T20:00:00Z"])

    def test_smoke_never_claims_authentication_during_provider_availability_failure(self) -> None:
        transport = RoutingTransport(lambda _: (503, '{"message":"unavailable"}'))
        result = AlpacaMarketData(
            transport, credentials(), now=lambda: NOW
        ).smoke()
        self.assertFalse(result.authentication_ok)
        self.assertFalse(result.historical_sip_ok)
        self.assertFalse(result.latest_iex_fresh)
        self.assertEqual(result.status, "BLOCKED_AVAILABILITY")

    def test_smoke_keeps_transport_connectivity_distinct(self) -> None:
        class OfflineTransport:
            def get(self, url: str, headers) -> HttpResponse:
                raise HttpTransportError("fixture transport offline")

        result = AlpacaMarketData(
            OfflineTransport(),
            credentials(),
            now=lambda: NOW,
        ).smoke()
        self.assertFalse(result.authentication_ok)
        self.assertEqual(result.status, "BLOCKED_CONNECTIVITY")

    def test_smoke_distinguishes_auth_availability_and_malformed_success(self) -> None:
        cases = (
            (401, '{"message":"unauthorized"}', False, "BLOCKED_AUTHENTICATION"),
            (403, '{"message":"forbidden"}', False, "BLOCKED_AUTHENTICATION"),
            (429, '{"message":"rate limited"}', False, "BLOCKED_AVAILABILITY"),
            (503, '{"message":"unavailable"}', False, "BLOCKED_AVAILABILITY"),
            (
                200,
                '{"quotes":{},"next_page_token":null}',
                True,
                "BLOCKED_INCOMPLETE_COHORT",
            ),
        )
        for status, body, authenticated, expected in cases:
            with self.subTest(status=status):
                result = AlpacaMarketData(
                    RoutingTransport(
                        lambda _, response_status=status, response_body=body: (
                            response_status,
                            response_body,
                        )
                    ),
                    credentials(),
                    now=lambda: NOW,
                ).smoke()
                self.assertIs(result.authentication_ok, authenticated)
                self.assertEqual(result.status, expected)

    def test_credentials_are_immutable_and_never_render_secret_values(self) -> None:
        value = AlpacaCredentials("key-canary", "secret-canary")
        rendered = repr(value)
        self.assertNotIn("key-canary", rendered)
        self.assertNotIn("secret-canary", rendered)
        dependencies = AlpacaMarketData(
            RoutingTransport(lambda _url: (200, "{}")),
            value,
            now=lambda: NOW,
        )._capture_request_dependencies()
        rendered_dependencies = repr(dependencies)
        self.assertNotIn("key-canary", rendered_dependencies)
        self.assertNotIn("secret-canary", rendered_dependencies)
        with self.assertRaises(AttributeError):
            value.key_id = "replacement"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
