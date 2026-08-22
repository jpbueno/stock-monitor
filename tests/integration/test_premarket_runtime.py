from __future__ import annotations

import tempfile
import unittest
import json
import sqlite3
import gc
import weakref
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from stock_monitor.journal import InvalidJournalValue, Journal
import stock_monitor.evidence as evidence_module
import stock_monitor.provider_workflows as provider_workflows_module
import stock_monitor.risk as risk_module
from stock_monitor.market_calendar import CalendarError, load_current_market_calendar
from stock_monitor.policy import Policy
from stock_monitor.provider_workflows import (
    CanonicalMaterialError,
    PremarketWorkflowCoordinator,
)
from stock_monitor.premarket_runtime import (
    PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
    PremarketRuntimeCollector,
    PremarketRuntimeRiskResolver,
    _premarket_collection_window,
    _persist_provider_page,
)
from stock_monitor.providers.alpaca import (
    AlpacaMarketData,
    TimeWindow,
    read_provider_fetch_bundle,
)
from stock_monitor.providers.http import (
    EgressPolicy,
    HttpResponse,
    HttpTransportError,
)
from stock_monitor.providers.reference import ReferenceClient
from stock_monitor.risk import SessionCalendarResolver
from stock_monitor.universe import load_current_universe
from stock_monitor.evidence import load_current_evidence_release
from tests.support import credentials, policy_fixture
from tests.unit import _task5_fixtures as task5_fixtures


ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")


def _provider_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


class _RuntimeMarketTransport:
    def __init__(
        self,
        *,
        session_date: date,
        collected_at: datetime,
        history_sessions: tuple[date, ...],
        previous_session: date,
        previous_close: time,
        price_scale: Decimal = Decimal("1"),
        missing_previous_symbol: str | None = None,
    ) -> None:
        templates = task5_fixtures.universe_candidate_contexts()
        symbols = tuple(context.record.symbol for context in templates)
        # Keep the fake transport's sealed dependency graph limited to the
        # primitive values it serves. Retaining CandidateContext authorities
        # also retains weak calendar registries whose GC cleanup is unrelated
        # to an HTTP request and can otherwise look like transport tampering.
        self._bar_templates = {
            symbol: tuple(
                (
                    bar.close,
                    bar.high,
                    bar.low,
                    bar.open,
                    bar.volume,
                )
                for bar in templates[0].bars_by_symbol[symbol]
            )
            for symbol in symbols
        }
        self._previous_quotes = {
            context.record.symbol: (
                context.previous_session_quote.bid,
                context.previous_session_quote.ask,
            )
            for context in templates
        }
        self._latest_quotes = {
            context.record.symbol: (
                context.latest_iex_quote.bid,
                context.latest_iex_quote.ask,
            )
            for context in templates
        }
        self._session_date = session_date
        self._collected_at = collected_at
        self._history_sessions = history_sessions
        self._previous_session = previous_session
        self._previous_close = previous_close
        self._price_scale = price_scale
        self._missing_previous_symbol = missing_previous_symbol
        self.requested_urls: list[str] = []

    def get(self, url: str, _headers: object) -> HttpResponse:
        self.requested_urls.append(url)
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        symbols = tuple(query["symbols"][0].split(","))
        if parsed.path == "/v2/stocks/bars":
            document = {
                "bars": {
                    symbol: [
                        {
                            "c": str(template[0] * self._price_scale),
                            "h": str(template[1] * self._price_scale),
                            "l": str(template[2] * self._price_scale),
                            "o": str(template[3] * self._price_scale),
                            "t": _provider_time(
                                datetime.combine(day, time(16), ET)
                            ),
                            "v": template[4],
                        }
                        for day, template in zip(
                            self._history_sessions,
                            self._bar_templates[symbol],
                            strict=True,
                        )
                    ]
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes":
            quote_at = datetime.combine(
                self._previous_session,
                self._previous_close,
                ET,
            ) - timedelta(minutes=2)
            document = {
                "quotes": {
                    symbol: []
                    if symbol == self._missing_previous_symbol
                    else [
                        {
                            "ap": str(
                                self._previous_quotes[symbol][1]
                                * self._price_scale
                            ),
                            "bp": str(
                                self._previous_quotes[symbol][0]
                                * self._price_scale
                            ),
                            "i": 100,
                            "t": _provider_time(
                                quote_at - timedelta(minutes=2)
                            ),
                        },
                        {
                            "ap": str(
                                self._previous_quotes[symbol][1]
                                * self._price_scale
                            ),
                            "bp": str(
                                self._previous_quotes[symbol][0]
                                * self._price_scale
                            ),
                            "i": 101,
                            "t": _provider_time(quote_at),
                        }
                    ]
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes/latest":
            quote_at = self._collected_at - timedelta(minutes=2)
            document = {
                "quotes": {
                    symbol: {
                        "ap": str(
                            self._latest_quotes[symbol][1]
                            * self._price_scale
                        ),
                        "bp": str(
                            self._latest_quotes[symbol][0]
                            * self._price_scale
                        ),
                        "i": 202,
                        "t": _provider_time(quote_at),
                    }
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        else:
            raise AssertionError(f"unexpected market-data URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return HttpResponse(
            200,
            (("Content-Type", "application/json"),),
            body,
            url,
        )


class _RuntimeReferenceTransport:
    def __init__(
        self,
        *,
        fail_on: int | None = None,
    ) -> None:
        self.requested_urls: list[str] = []
        self._fail_on = fail_on

    def get(self, url: str, _headers: object) -> HttpResponse:
        self.requested_urls.append(url)
        if (
            self._fail_on is not None
            and len(self.requested_urls) >= self._fail_on
        ):
            raise HttpTransportError("offline reference failure")
        body, content_type = task5_fixtures._HALT_SOURCE_RESPONSES[url]
        return HttpResponse(
            200,
            (("Content-Type", content_type),),
            body,
            url,
        )


class PremarketRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.journal = Journal.open(self.root / "journal.sqlite3")
        self.addCleanup(self.journal.close)

    def _fresh_inputs(self):
        from tests.integration.test_provider_workflows import (
            CanonicalPremarketSourceBindingTests,
        )

        fixture_owner = object.__new__(CanonicalPremarketSourceBindingTests)
        fixture_owner.temporary_root = self.root
        (
            project_root,
            evidence_sha256,
            scoped_authorities,
            clear_authorities,
            scoped_sources,
        ) = fixture_owner._fresh_open_project()
        patchers = (
            mock.patch.object(
                evidence_module,
                "CURRENT_EVIDENCE_RELEASE_SHA256",
                evidence_sha256,
            ),
            mock.patch.dict(
                evidence_module._SCOPED_REFERENCE_AUTHORITIES,
                scoped_authorities,
            ),
            mock.patch.dict(
                evidence_module._CLEAR_COVERAGE_AUTHORITIES,
                clear_authorities,
            ),
            mock.patch.object(
                provider_workflows_module,
                "_SCOPED_REFERENCE_SOURCES",
                scoped_sources,
            ),
        )
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        session_date = date(2026, 8, 24)
        decision_at = datetime(2026, 8, 24, 8, 45, tzinfo=ET)
        requested_at = datetime(2026, 8, 24, 8, 50, tzinfo=ET)
        collected_at = datetime(2026, 8, 24, 8, 52, tzinfo=ET)
        calendar = load_current_market_calendar(
            project_root,
            as_of=session_date,
        )
        universe = load_current_universe(project_root, as_of=session_date)
        evidence_release = load_current_evidence_release(
            project_root,
            as_of=decision_at,
            universe=universe,
        )
        return (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            universe,
            evidence_release,
        )

    def _start_validation_window(
        self,
        *,
        project_root: Path,
        calendar,
        session_date: date,
    ) -> SessionCalendarResolver:
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        previous = window.calendar_resolver.session(window.previous_session)
        close_at = datetime.combine(
            window.previous_session,
            previous.close_time,
            previous.timezone,
        )
        self.journal.start_phase1_validation_window(
            window_id="6" * 64,
            started_session=window.previous_session,
            starting_capital=Decimal("5000"),
            started_at=close_at,
            received_at=close_at,
            calendar_resolver=window.calendar_resolver,
        )
        return window.calendar_resolver

    def _runtime_coordinator(
        self,
        *,
        project_root: Path,
        calendar,
        session_date: date,
        collected_at: datetime,
        price_scale: Decimal = Decimal("1"),
        missing_previous_symbol: str | None = None,
    ) -> PremarketWorkflowCoordinator:
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        market_transport = _RuntimeMarketTransport(
            session_date=session_date,
            collected_at=collected_at,
            history_sessions=window.history_sessions,
            previous_session=window.previous_session,
            previous_close=window.calendar_resolver.session(
                window.previous_session
            ).close_time,
            price_scale=price_scale,
            missing_previous_symbol=missing_previous_symbol,
        )
        reference_transport = _RuntimeReferenceTransport()
        clock = lambda: collected_at
        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=AlpacaMarketData(
                market_transport,
                credentials(),
                now=clock,
            ),
            reference_client=ReferenceClient(
                reference_transport,
                EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
                allowed_urls=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                source_roles=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                now=clock,
            ),
            clock=clock,
        )
        archive_root = self.root / f"reports-{price_scale}"
        archive_root.mkdir()
        return PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=project_root,
            report_archive_root=archive_root,
            collector=collector,
            risk_resolver=PremarketRuntimeRiskResolver(
                policy=policy_fixture(),
                project_root=project_root,
            ),
        )

    def test_constructors_are_network_free_and_dependencies_are_explicit(self) -> None:
        transport = mock.Mock()
        now = lambda: datetime(2026, 8, 24, 12, 52, tzinfo=UTC)
        alpaca = AlpacaMarketData(transport, credentials(), now=now)
        reviewed_sources = {
            **PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
            "https://www.nyse.com/trade/hours-calendars": "PRIMARY_CALENDAR",
            "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": (
                "CROSS_CHECK_CALENDAR"
            ),
        }
        reference = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
            allowed_urls=reviewed_sources,
            source_roles=reviewed_sources,
            now=now,
        )
        project_root = self.root / "project"
        project_root.mkdir()

        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=alpaca,
            reference_client=reference,
            clock=now,
        )
        resolver = PremarketRuntimeRiskResolver(
            policy=policy_fixture(),
            project_root=project_root,
        )

        self.assertIs(collector.journal, self.journal)
        self.assertIsInstance(resolver.policy, Policy)
        self.assertEqual(
            set(PREMARKET_OPERATIONAL_REFERENCE_SOURCES.values()),
            {
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
            },
        )
        transport.get.assert_not_called()

    def test_collection_window_uses_exact_regular_and_early_session_closes(
        self,
    ) -> None:
        calendar = load_current_market_calendar(
            ROOT,
            as_of=date(2026, 8, 22),
        )
        resolver = SessionCalendarResolver((calendar,))
        for label, session_date, previous_date, expected_start, expected_end in (
            (
                "regular",
                date(2026, 8, 24),
                date(2026, 8, 21),
                time(15, 55),
                time(16, 0),
            ),
            (
                "early-close",
                date(2026, 11, 30),
                date(2026, 11, 27),
                time(12, 55),
                time(13, 0),
            ),
        ):
            with self.subTest(label=label):
                window = _premarket_collection_window(
                    project_root=ROOT,
                    calendar=calendar,
                    session_date=session_date,
                )

                self.assertEqual(window.calendar_resolver.calendars, resolver.calendars)
                self.assertEqual(window.previous_session, previous_date)
                self.assertEqual(len(window.history_sessions), 60)
                self.assertEqual(len(window.hold_sessions), 10)
                self.assertEqual(
                    window.quote_window.start.astimezone(ET).time(),
                    expected_start,
                )
                self.assertEqual(
                    window.quote_window.end.astimezone(ET).time(),
                    expected_end,
                )

    def test_missing_adjacent_release_fails_closed_before_collection(self) -> None:
        calendar = load_current_market_calendar(
            ROOT,
            as_of=date(2026, 8, 22),
        )

        with self.assertRaises(CalendarError):
            _premarket_collection_window(
                project_root=ROOT,
                calendar=calendar,
                session_date=date(2026, 1, 5),
            )

    def test_provider_page_is_durable_before_the_next_credentialed_get(
        self,
    ) -> None:
        first = {
            "bars": {
                "SPY": [
                    {
                        "t": "2026-08-12T20:00:00Z",
                        "o": 1,
                        "h": 1,
                        "l": 1,
                        "c": 1,
                        "v": 1,
                    }
                ]
            },
            "next_page_token": "page-2",
        }
        second = {
            "bars": {
                "SPY": [
                    {
                        "t": "2026-08-13T20:00:00Z",
                        "o": 2,
                        "h": 2,
                        "l": 2,
                        "c": 2,
                        "v": 2,
                    }
                ]
            },
            "next_page_token": None,
        }
        durable_counts = []
        journal_path = str(self.root / "journal.sqlite3")

        class Transport:
            def __init__(inner_self) -> None:
                inner_self.journal_path = journal_path

            def get(inner_self, url, _headers):
                connection = sqlite3.connect(inner_self.journal_path)
                try:
                    durable_counts.append(
                        connection.execute(
                            "SELECT COUNT(*) FROM source_observations"
                        ).fetchone()[0]
                    )
                finally:
                    connection.close()
                page_two = parse_qs(urlsplit(url).query).get("page_token") == [
                    "page-2"
                ]
                body = json.dumps(
                    second if page_two else first,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                return HttpResponse(
                    200,
                    (("Content-Type", "application/json"),),
                    body,
                    url,
                )

        bindings = []
        client = AlpacaMarketData(
            Transport(),
            credentials(),
            now=lambda: datetime(2026, 8, 14, 13, tzinfo=UTC),
        )
        cohort = client.daily_bars(
            ("SPY",),
            TimeWindow(
                datetime(2026, 8, 12, 20, tzinfo=UTC),
                datetime(2026, 8, 13, 20, tzinfo=UTC),
            ),
            page_sink=lambda page: bindings.append(
                _persist_provider_page(self.journal, page)
            ),
        )

        self.assertEqual(durable_counts, [0, 1])
        self.assertEqual(len(bindings), 2)
        bundle = read_provider_fetch_bundle(cohort)
        self.assertTrue(
            all(
                binding.source is page
                for binding, page in zip(bindings, bundle.pages, strict=True)
            )
        )
        receipts = self.journal.read_source_observation_receipts(
            tuple(binding.receipt.row_id for binding in bindings)
        )
        self.assertEqual(
            tuple(receipt.source_payload for receipt in receipts),
            tuple(page.payload for page in bundle.pages),
        )

    def test_collector_returns_exact_terminal_cohorts_references_and_contexts(
        self,
    ) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            universe,
            evidence_release,
        ) = self._fresh_inputs()
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        previous_schedule = window.calendar_resolver.session(
            window.previous_session
        )
        market_transport = _RuntimeMarketTransport(
            session_date=session_date,
            collected_at=collected_at,
            history_sessions=window.history_sessions,
            previous_session=window.previous_session,
            previous_close=previous_schedule.close_time,
        )
        reference_transport = _RuntimeReferenceTransport()

        class TerminalClock:
            def __init__(inner_self) -> None:
                inner_self.calls = 0

            def __call__(inner_self):
                inner_self.calls += 1
                return collected_at

        terminal_clock = TerminalClock()
        provider_clock = lambda: collected_at

        reviewed_sources = {
            **PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
            "https://www.nyse.com/trade/hours-calendars": "PRIMARY_CALENDAR",
            "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": (
                "CROSS_CHECK_CALENDAR"
            ),
        }
        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=AlpacaMarketData(
                market_transport,
                credentials(),
                now=provider_clock,
            ),
            reference_client=ReferenceClient(
                reference_transport,
                EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
                allowed_urls=reviewed_sources,
                source_roles=reviewed_sources,
                now=provider_clock,
            ),
            clock=terminal_clock,
        )
        required_symbols = tuple(
            record.symbol for record in universe.records if record.enabled
        )

        collection = collector.collect(
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
            calendar=calendar,
            universe=universe,
            evidence_release=evidence_release,
            required_symbols=required_symbols,
        )

        self.assertIsNone(collection.failure_reason)
        self.assertEqual(collection.collected_at, collected_at)
        self.assertEqual(len(collection.provider_cohorts), 3)
        self.assertEqual(len(collection.reference_sources), 3)
        self.assertEqual(len(collection.contexts), len(required_symbols))
        self.assertEqual(len(collection.persisted_bindings), 6)
        by_role = {
            provider_workflows_module._binding_source_role(
                binding.source,
                invoke_owner_predicate=False,
            ): binding
            for binding in collection.persisted_bindings
        }
        self.assertEqual(
            set(by_role),
            {
                "ALPACA_DAILY_BARS",
                "ALPACA_HISTORICAL_QUOTES",
                "ALPACA_LATEST_QUOTES",
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
            },
        )
        self.assertEqual(
            {
                role
                for role, binding in by_role.items()
                if binding.decision_basis == "OPERATIONAL_HEALTH_ONLY"
            },
            {
                "ALPACA_LATEST_QUOTES",
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
            },
        )
        quote_url = next(
            url
            for url in market_transport.requested_urls
            if urlsplit(url).path == "/v2/stocks/quotes"
        )
        quote_query = parse_qs(urlsplit(quote_url).query)
        self.assertEqual(
            datetime.fromisoformat(quote_query["start"][0].replace("Z", "+00:00"))
            .astimezone(ET)
            .time(),
            time(15, 55),
        )
        self.assertEqual(
            datetime.fromisoformat(quote_query["end"][0].replace("Z", "+00:00"))
            .astimezone(ET)
            .time(),
            time(16, 0),
        )
        self.assertEqual(
            tuple(reference_transport.requested_urls),
            tuple(PREMARKET_OPERATIONAL_REFERENCE_SOURCES),
        )
        self.assertTrue(
            all(
                binding.receipt.retrieved_at <= collection.collected_at
                for binding in collection.persisted_bindings
            )
        )
        self.assertEqual(terminal_clock.calls, 1)
        self.assertTrue(
            all(
                context.as_of == decision_at
                and context.operational_as_of == collected_at
                for context in collection.contexts
            )
        )
        self.assertTrue(
            all(
                context.previous_session_quote.timestamp.astimezone(ET).time()
                == time(15, 58)
                for context in collection.contexts
            )
        )

    def test_provider_failure_returns_every_page_persisted_before_failure(
        self,
    ) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            universe,
            evidence_release,
        ) = self._fresh_inputs()
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        successful = _RuntimeMarketTransport(
            session_date=session_date,
            collected_at=collected_at,
            history_sessions=window.history_sessions,
            previous_session=window.previous_session,
            previous_close=window.calendar_resolver.session(
                window.previous_session
            ).close_time,
        )

        class FailOnSecondPage:
            def __init__(inner_self) -> None:
                inner_self.calls = 0

            def get(inner_self, url, headers):
                inner_self.calls += 1
                if inner_self.calls == 2:
                    raise HttpTransportError("offline provider failure")
                response = successful.get(url, headers)
                document = json.loads(response.body)
                document["next_page_token"] = "page-2"
                body = json.dumps(
                    document,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                return HttpResponse(
                    response.status,
                    response.headers,
                    body,
                    response.url,
                )

        transport = FailOnSecondPage()
        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: collected_at,
            ),
            reference_client=ReferenceClient(
                _RuntimeReferenceTransport(),
                EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
                allowed_urls=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                source_roles=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                now=lambda: collected_at,
            ),
            clock=lambda: collected_at,
        )
        required_symbols = tuple(
            record.symbol for record in universe.records if record.enabled
        )

        collection = collector.collect(
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
            calendar=calendar,
            universe=universe,
            evidence_release=evidence_release,
            required_symbols=required_symbols,
        )

        self.assertEqual(collection.failure_reason, "PROVIDER_CHECK_FAILED")
        self.assertEqual(collection.collected_at, collected_at)
        self.assertEqual(collection.provider_cohorts, ())
        self.assertEqual(len(collection.persisted_bindings), 1)
        self.assertEqual(
            collection.persisted_bindings[0].source.page.source_type,
            "ALPACA_DAILY_BARS",
        )
        self.assertGreaterEqual(transport.calls, 2)
        self.assertEqual(
            len(
                self.journal.read_source_observation_receipts(
                    (collection.persisted_bindings[0].receipt.row_id,)
                )
            ),
            1,
        )

    def test_empty_previous_quote_is_explicit_data_unavailable_context(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            universe,
            evidence_release,
        ) = self._fresh_inputs()
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=AlpacaMarketData(
                _RuntimeMarketTransport(
                    session_date=session_date,
                    collected_at=collected_at,
                    history_sessions=window.history_sessions,
                    previous_session=window.previous_session,
                    previous_close=window.calendar_resolver.session(
                        window.previous_session
                    ).close_time,
                    missing_previous_symbol="AAPL",
                ),
                credentials(),
                now=lambda: collected_at,
            ),
            reference_client=ReferenceClient(
                _RuntimeReferenceTransport(),
                EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
                allowed_urls=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                source_roles=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                now=lambda: collected_at,
            ),
            clock=lambda: collected_at,
        )
        required_symbols = tuple(
            record.symbol for record in universe.records if record.enabled
        )

        collection = collector.collect(
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
            calendar=calendar,
            universe=universe,
            evidence_release=evidence_release,
            required_symbols=required_symbols,
        )

        self.assertIsNone(collection.failure_reason)
        aapl = next(
            context
            for context in collection.contexts
            if context.record.symbol == "AAPL"
        )
        self.assertIsNone(aapl.previous_session_quote)

    def test_empty_previous_quote_dispatches_zero_candidate_data_unavailable(
        self,
    ) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        coordinator = self._runtime_coordinator(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
            collected_at=collected_at,
            missing_previous_symbol="AAPL",
        )

        material = coordinator.premarket_material(
            session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
        )

        self.assertEqual(
            material.report.outcome,
            "NO NEW TRADE - DATA UNAVAILABLE",
        )
        self.assertIn("`DATA_UNAVAILABLE`", material.report.body)
        self.assertEqual(material.snapshot.candidates, ())
        self.assertIsNone(material.primary_plan)
        self.assertIsNone(material.publication_decision)

    def test_reference_failure_hands_off_provider_and_prior_reference_pages(
        self,
    ) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            universe,
            evidence_release,
        ) = self._fresh_inputs()
        window = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        market_transport = _RuntimeMarketTransport(
            session_date=session_date,
            collected_at=collected_at,
            history_sessions=window.history_sessions,
            previous_session=window.previous_session,
            previous_close=window.calendar_resolver.session(
                window.previous_session
            ).close_time,
        )
        reference_transport = _RuntimeReferenceTransport(fail_on=2)
        collector = PremarketRuntimeCollector(
            journal=self.journal,
            project_root=project_root,
            alpaca_market_data=AlpacaMarketData(
                market_transport,
                credentials(),
                now=lambda: collected_at,
            ),
            reference_client=ReferenceClient(
                reference_transport,
                EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
                allowed_urls=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                source_roles=PREMARKET_OPERATIONAL_REFERENCE_SOURCES,
                now=lambda: collected_at,
            ),
            clock=lambda: collected_at,
        )
        required_symbols = tuple(
            record.symbol for record in universe.records if record.enabled
        )

        collection = collector.collect(
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
            calendar=calendar,
            universe=universe,
            evidence_release=evidence_release,
            required_symbols=required_symbols,
        )

        self.assertEqual(collection.failure_reason, "SOURCE_CHECK_FAILED")
        self.assertEqual(len(collection.provider_cohorts), 3)
        self.assertEqual(len(collection.reference_sources), 1)
        self.assertEqual(len(collection.persisted_bindings), 4)
        expected_urls = tuple(PREMARKET_OPERATIONAL_REFERENCE_SOURCES)
        self.assertEqual(reference_transport.requested_urls[0], expected_urls[0])
        self.assertTrue(
            all(
                url == expected_urls[1]
                for url in reference_transport.requested_urls[1:]
            )
        )
        self.assertNotIn(expected_urls[2], reference_transport.requested_urls)

    def test_risk_resolver_binds_validation_breaker_to_exact_journal_call(
        self,
    ) -> None:
        (
            project_root,
            session_date,
            decision_at,
            retrieved_at,
            _collected_at,
            calendar,
            universe,
            evidence_release,
        ) = self._fresh_inputs()
        calendar_resolver = self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        resolver = PremarketRuntimeRiskResolver(
            policy=policy_fixture(),
            project_root=project_root,
        )
        breaker = resolver.validation_breaker(
            journal=self.journal,
            session_date=session_date,
            decision_at=decision_at,
            calendar=calendar,
            calendar_resolver=calendar_resolver,
        )

        resolution = resolver.resolve(
            journal=self.journal,
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=retrieved_at,
            calendar=calendar,
            universe=universe,
            evidence_release=evidence_release,
            ranked_candidates=(),
            validation_breaker=breaker,
        )

        self.assertIs(resolution.breaker_state, breaker)
        self.assertIsNone(resolution.primary_plan)
        forged = replace(breaker)
        with self.assertRaises(CanonicalMaterialError):
            resolver.resolve(
                journal=self.journal,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                ranked_candidates=(),
                validation_breaker=forged,
            )
        other = Journal.open(self.root / "other.sqlite3")
        self.addCleanup(other.close)
        with self.assertRaises(CanonicalMaterialError):
            resolver.resolve(
                journal=other,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                ranked_candidates=(),
                validation_breaker=breaker,
            )

        self.journal.append_source_observation_receipt(
            payload=b"stale-validation-generation",
            source_uri="stock-monitor://test/stale-validation-generation",
            source_type="TEST_AUTHORITY_CHANGE",
            provider="stock-monitor-test",
            feed="test",
            source_time=decision_at,
            retrieved_at=decision_at,
            provider_sequence=None,
            delay_seconds=0,
            health_result="OK",
            details={},
        )
        with self.assertRaises(CanonicalMaterialError):
            resolver.resolve(
                journal=self.journal,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                ranked_candidates=(),
                validation_breaker=breaker,
            )

        mutated = resolver.validation_breaker(
            journal=self.journal,
            session_date=session_date,
            decision_at=decision_at,
            calendar=calendar,
            calendar_resolver=calendar_resolver,
        )
        object.__setattr__(mutated, "history_digest", "0" * 64)
        with self.assertRaises(CanonicalMaterialError):
            resolver.resolve(
                journal=self.journal,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                ranked_candidates=(),
                validation_breaker=mutated,
            )

    def test_validation_breaker_never_bootstraps_a_missing_window(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            _requested_at,
            _collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        calendar_resolver = _premarket_collection_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        ).calendar_resolver
        resolver = PremarketRuntimeRiskResolver(
            policy=policy_fixture(),
            project_root=project_root,
        )
        before = self.journal._connection.total_changes

        with self.assertRaises(InvalidJournalValue):
            resolver.validation_breaker(
                journal=self.journal,
                session_date=session_date,
                decision_at=decision_at,
                calendar=calendar,
                calendar_resolver=calendar_resolver,
            )

        self.assertEqual(self.journal._connection.total_changes, before)
        self.assertEqual(
            self.journal._connection.execute(
                "SELECT COUNT(*) FROM phase1_validation_windows"
            ).fetchone()[0],
            0,
        )

    def test_active_breaker_precedes_ranking_and_portfolio_sizing(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        coordinator = self._runtime_coordinator(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
            collected_at=collected_at,
        )

        with mock.patch.object(
            risk_module,
            "breaker_pauses_entry",
            return_value=True,
        ):
            material = coordinator.premarket_material(
                session_date,
                decision_at=decision_at,
                retrieved_at=requested_at,
            )

        self.assertEqual(material.report.outcome, "NO TRADE")
        self.assertIn("`ACTIVE_BREAKER`", material.report.body)
        self.assertEqual(material.snapshot.candidates, ())
        self.assertIsNone(material.primary_plan)
        self.assertIsNone(material.publication_decision)

    def test_validation_breaker_binding_is_removed_after_collection(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            _requested_at,
            _collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        calendar_resolver = self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        resolver = PremarketRuntimeRiskResolver(
            policy=policy_fixture(),
            project_root=project_root,
        )
        breaker = resolver.validation_breaker(
            journal=self.journal,
            session_date=session_date,
            decision_at=decision_at,
            calendar=calendar,
            calendar_resolver=calendar_resolver,
        )
        identity = id(breaker)
        breaker_reference = weakref.ref(breaker)
        self.assertIn(identity, resolver._bindings)

        del breaker
        gc.collect()

        self.assertIsNone(breaker_reference())
        self.assertNotIn(identity, resolver._bindings)

    def test_exact_ranked_primary_uses_canonical_portfolio_capacity(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        coordinator = self._runtime_coordinator(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
            collected_at=collected_at,
        )

        material = coordinator.premarket_material(
            session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
        )

        self.assertEqual(material.report.outcome, "CANDIDATES")
        self.assertTrue(
            risk_module.is_issued_long_plan_decision(material.primary_plan)
        )
        self.assertTrue(material.primary_plan.eligible)
        self.assertIsNotNone(material.publication_decision)
        self.assertEqual(material.snapshot.candidates[0].role, "PRIMARY")
        self.assertGreater(material.snapshot.candidates[0].material.shares, 0)
        self.assertTrue(
            all(
                item.role == "WATCHLIST_SHADOW"
                and not hasattr(item.material, "shares")
                for item in material.snapshot.candidates[1:]
            )
        )

    def test_no_primary_capacity_retains_exact_ineligible_decision(self) -> None:
        (
            project_root,
            session_date,
            decision_at,
            requested_at,
            collected_at,
            calendar,
            _universe,
            _evidence_release,
        ) = self._fresh_inputs()
        self._start_validation_window(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
        )
        coordinator = self._runtime_coordinator(
            project_root=project_root,
            calendar=calendar,
            session_date=session_date,
            collected_at=collected_at,
            price_scale=Decimal("100"),
        )

        material = coordinator.premarket_material(
            session_date,
            decision_at=decision_at,
            retrieved_at=requested_at,
        )

        self.assertEqual(material.report.outcome, "NO TRADE")
        self.assertIn("`NO_PRIMARY_CAPACITY`", material.report.body)
        self.assertIsNone(material.primary_plan)
        self.assertIsNone(material.publication_decision)
        self.assertEqual(material.snapshot.candidates, ())
        capacity = provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS[
            id(material.composition_authority)
        ].risk_children
        decisions = tuple(
            child
            for child in capacity
            if type(child) is risk_module.LongPlanDecision
        )
        self.assertEqual(len(decisions), 1)
        self.assertTrue(risk_module.is_issued_long_plan_decision(decisions[0]))
        self.assertFalse(decisions[0].eligible)
        self.assertEqual(decisions[0].reason_codes, ("QUANTITY_BELOW_ONE",))


if __name__ == "__main__":
    unittest.main()
