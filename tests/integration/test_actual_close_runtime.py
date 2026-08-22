"""Integration contract for the concrete GET-only actual-close collector."""

from __future__ import annotations

from collections.abc import Callable
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from stock_monitor.journal import Journal
from stock_monitor.providers.alpaca import AlpacaMarketData
from stock_monitor.providers.http import (
    EgressPolicy,
    HttpResponse,
    ProviderResponseError,
)
from stock_monitor.providers.reference import ReferenceClient
from tests.integration.test_signal_lifecycle import _calendar
from tests.support import aware_et, credentials, policy_fixture


_REFERENCE_SOURCES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
        "PRIMARY_HALT_FEED"
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
        "TRADER_ALERT_HALT"
    ),
    "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": (
        "CROSS_CHECK_CALENDAR"
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": (
        "OPERATIONAL_STATUS"
    ),
    "https://www.nyse.com/trade/hours-calendars": "PRIMARY_CALENDAR",
}


class _NoIoTransport:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get(self, url: str, headers: object) -> object:
        del headers
        self.calls.append(url)
        raise AssertionError("runtime constructor performed a GET")


def _utc_text(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


class _CloseTransport:
    def __init__(
        self,
        *,
        session_date: date,
        observed_at: datetime,
    ) -> None:
        self.session_date = session_date
        self.observed_at = observed_at
        self.requested_urls: list[str] = []

    def _market_document(self, url: str) -> object:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        symbol = query["symbols"][0]
        if parsed.path == "/v2/stocks/bars":
            start = datetime.fromisoformat(
                query["start"][0].replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(
                query["end"][0].replace("Z", "+00:00")
            )
            if query["timeframe"] == ["1Day"]:
                resolver = _calendar()
                sessions = [resolver.previous_session(self.session_date)]
                while len(sessions) < 14:
                    sessions.append(resolver.previous_session(sessions[-1]))
                sessions.reverse()
                values = []
                for ordinal, session_day in enumerate(sessions):
                    schedule = resolver.session(session_day)
                    at = datetime.combine(
                        session_day,
                        schedule.close_time,
                        schedule.timezone,
                    )
                    close = Decimal("20") + Decimal(ordinal) / Decimal("10")
                    values.append(
                        {
                            "c": str(close),
                            "h": str(close + Decimal("0.20")),
                            "l": str(close - Decimal("0.20")),
                            "o": str(close - Decimal("0.05")),
                            "t": _utc_text(at),
                            "v": 1_000_000 + ordinal,
                        }
                    )
                self._daily_window = (start, end)
            else:
                values = [
                    {
                        "c": "21.00",
                        "h": "21.10",
                        "l": "20.90",
                        "o": "20.95",
                        "t": _utc_text(end - timedelta(minutes=1)),
                        "v": 50_000,
                    },
                    {
                        "c": "21.05",
                        "h": "21.15",
                        "l": "20.92",
                        "o": "21.00",
                        "t": _utc_text(end),
                        "v": 45_000,
                    },
                ]
                self._minute_window = (start, end)
            return {"bars": {symbol: values}, "next_page_token": None}
        if parsed.path == "/v2/stocks/quotes":
            start = datetime.fromisoformat(
                query["start"][0].replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(
                query["end"][0].replace("Z", "+00:00")
            )
            if parsed.path.endswith("/latest"):
                raise AssertionError("latest quote path was misclassified")
            self._quote_window = (start, end)
            return {
                "quotes": {
                    symbol: [
                        {
                            "ap": "21.02",
                            "bp": "21.00",
                            "i": 7,
                            "t": _utc_text(end),
                        }
                    ]
                },
                "next_page_token": None,
            }
        if parsed.path == "/v2/stocks/quotes/latest":
            return {
                "quotes": {
                    symbol: {
                        "ap": "21.03",
                        "bp": "21.01",
                        "i": 8,
                        "t": _utc_text(self.observed_at),
                    }
                },
                "next_page_token": None,
            }
        raise AssertionError(f"unexpected Alpaca URL: {url}")

    @staticmethod
    def _reference_response(url: str) -> tuple[bytes, str]:
        if url.endswith("feed=tradehalts"):
            return (
                b'<?xml version="1.0"?><rss version="2.0" '
                b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
                b"<ndaq:numItems>0</ndaq:numItems></channel></rss>",
                "application/xml",
            )
        if "feed=currentheadlines" in url:
            return (
                b'<?xml version="1.0"?><rss version="2.0" '
                b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
                b"<title>Nasdaq Equity Trader Alerts</title>"
                b"<ndaq:numItems>0</ndaq:numItems></channel></rss>",
                "application/xml",
            )
        if "/api/notifications/public/alerts" in url:
            return b"[]", "application/json"
        if "Trader.aspx?id=Calendar" in url:
            return b"<html><body>Nasdaq calendar</body></html>", "text/html"
        raise AssertionError(f"unexpected reference URL: {url}")

    def get(self, url: str, headers: object) -> HttpResponse:
        del headers
        self.requested_urls.append(url)
        if url.startswith("https://data.alpaca.markets/"):
            body = json.dumps(
                self._market_document(url),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            content_type = "application/json"
        else:
            body, content_type = self._reference_response(url)
        return HttpResponse(
            200,
            (("Content-Type", content_type),),
            body,
            url,
        )


class _PartialQuoteFailureTransport(_CloseTransport):
    def __init__(self, *, journal_path: Path, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.journal_path = journal_path
        self.first_quote_was_durable = False
        self.observed_source_count = 0

    def _market_document(self, url: str) -> object:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path == "/v2/stocks/quotes" and "page_token" not in query:
            document = super()._market_document(url)
            assert isinstance(document, dict)
            return {**document, "next_page_token": "quote-page-2"}
        return super()._market_document(url)

    def get(self, url: str, headers: object) -> HttpResponse:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if (
            parsed.path == "/v2/stocks/quotes"
            and query.get("page_token") == ["quote-page-2"]
        ):
            self.requested_urls.append(url)
            connection = sqlite3.connect(self.journal_path)
            try:
                self.observed_source_count = connection.execute(
                    "SELECT COUNT(*) FROM source_observations"
                ).fetchone()[0]
            finally:
                connection.close()
            self.first_quote_was_durable = self.observed_source_count >= 3
            raise ProviderResponseError("injected terminal quote-page failure")
        return super().get(url, headers)


class _ReferenceFailureTransport(_CloseTransport):
    def __init__(self, *, failed_url: str, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.failed_url = failed_url

    def get(self, url: str, headers: object) -> HttpResponse:
        if url == self.failed_url:
            del headers
            self.requested_urls.append(url)
            return HttpResponse(
                503,
                (("Content-Type", "application/json"),),
                b"{}",
                url,
            )
        return super().get(url, headers)


class _ReferenceContentTransport(_CloseTransport):
    def __init__(
        self,
        *,
        reference_url: str,
        body: bytes,
        content_type: str,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self.reference_url = reference_url
        self.body = body
        self.content_type = content_type

    def _reference_response(self, url: str) -> tuple[bytes, str]:
        if url == self.reference_url:
            return self.body, self.content_type
        return super()._reference_response(url)


class ActualCloseRuntimeTests(unittest.TestCase):
    @staticmethod
    def _collector(
        journal: Journal,
        transport: object,
        *,
        source_now: datetime,
        terminal_at: datetime,
        terminal_clock: Callable[[], datetime] | None = None,
    ):
        from stock_monitor.actual_close_runtime import ActualCloseRuntimeCollector

        egress = EgressPolicy(
            ("data.alpaca.markets", "www.nasdaqtrader.com", "www.nyse.com")
        )
        alpaca = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: source_now,
        )
        references = ReferenceClient(
            transport,
            egress,
            allowed_urls=_REFERENCE_SOURCES,
            source_roles=_REFERENCE_SOURCES,
            now=lambda: source_now,
        )
        return ActualCloseRuntimeCollector(
            alpaca_market_data=alpaca,
            reference_client=references,
            calendar_resolver=_calendar(),
            policy=policy_fixture(),
            plans=journal.phase1_signal_plan_resolver(),
            clock=(lambda: terminal_at) if terminal_clock is None else terminal_clock,
        )

    def test_runtime_module_exposes_the_collector(self) -> None:
        spec = importlib.util.find_spec("stock_monitor.actual_close_runtime")

        self.assertIsNotNone(spec)
        from stock_monitor import actual_close_runtime

        self.assertTrue(
            hasattr(actual_close_runtime, "ActualCloseRuntimeCollector")
        )

    def test_constructor_binds_exact_shared_dependencies_without_io(self) -> None:
        from stock_monitor.actual_close_runtime import ActualCloseRuntimeCollector

        transport = _NoIoTransport()
        egress = EgressPolicy(
            ("data.alpaca.markets", "www.nasdaqtrader.com", "www.nyse.com")
        )
        alpaca = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: datetime(2026, 8, 14, 20, 0, tzinfo=UTC),
        )
        references = ReferenceClient(
            transport,
            egress,
            allowed_urls=_REFERENCE_SOURCES,
            source_roles=_REFERENCE_SOURCES,
            now=lambda: datetime(2026, 8, 14, 20, 0, tzinfo=UTC),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                plans = journal.phase1_signal_plan_resolver()
                try:
                    collector = ActualCloseRuntimeCollector(
                        alpaca_market_data=alpaca,
                        reference_client=references,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=plans,
                        clock=lambda: datetime(2026, 8, 14, 20, 1, tzinfo=UTC),
                    )
                except TypeError as error:
                    self.fail(f"collector constructor is unavailable: {error}")

                self.assertFalse(hasattr(collector, "transaction"))
                self.assertFalse(hasattr(collector, "order"))
                self.assertFalse(hasattr(collector, "trade"))
                self.assertIs(collector.plans, plans)

        self.assertEqual(transport.calls, [])

    def test_collects_exact_regular_session_windows_and_durable_review(self) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        mark_cutoff = aware_et(session_date, "15:14")
        source_now = aware_et(session_date, "15:31")
        terminal_at = aware_et(session_date, "15:32")
        transport = _CloseTransport(
            session_date=session_date,
            observed_at=aware_et(session_date, "15:30"),
        )
        terminal_clock_urls: list[tuple[str, ...]] = []

        def terminal_clock() -> datetime:
            terminal_clock_urls.append(tuple(transport.requested_urls))
            return terminal_at

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                collector = self._collector(
                    journal,
                    transport,
                    source_now=source_now,
                    terminal_at=terminal_at,
                    terminal_clock=terminal_clock,
                )
                try:
                    collection = collector.collect_close_sources(
                        journal=journal,
                        symbols=("AAPL",),
                        session_date=session_date,
                        review_at=review_at,
                        mark_cutoff=mark_cutoff,
                        command_started_at=source_now,
                    )
                except AttributeError as error:
                    self.fail(f"close collection is unavailable: {error}")

                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=collection.collected_at,
                )

                self.assertEqual(collection.collected_at, terminal_at)
                self.assertEqual(review.retrieved_at, terminal_at)
                self.assertEqual(review.mark_cutoff, mark_cutoff)
                self.assertEqual(
                    {
                        (binding.symbol, binding.source_role)
                        for binding in review.bindings
                    },
                    {
                        *(("AAPL", role) for role in (
                            "SIP_DAILY_BAR",
                            "SIP_MINUTE_BAR",
                            "SIP_QUOTE",
                            "IEX_FRESHNESS",
                            "EVENT_EVIDENCE",
                        )),
                        *((None, role) for role in (
                            "PRIMARY_HALT_FEED",
                            "TRADER_ALERT_HALT",
                            "OPERATIONAL_STATUS",
                            "CROSS_CHECK_CALENDAR",
                        )),
                    },
                )
                event_binding = next(
                    binding
                    for binding in review.bindings
                    if binding.source_role == "EVENT_EVIDENCE"
                )
                self.assertEqual(event_binding.failure_code, "SOURCE_UNAVAILABLE")
                self.assertIsNone(event_binding.receipt)
                self.assertTrue(
                    all(
                        receipt.retrieved_at <= terminal_at
                        for receipt in review.receipts
                    )
                )
                market_receipts = tuple(
                    receipt
                    for receipt in review.receipts
                    if receipt.provider == "alpaca"
                )
                self.assertEqual(len(market_receipts), 4)
                self.assertTrue(
                    all(
                        receipt.provider_sequence is None
                        for receipt in market_receipts
                    )
                )
                global_receipts = tuple(
                    receipt
                    for receipt in review.receipts
                    if receipt.source_type == "OFFICIAL_REFERENCE"
                )
                self.assertEqual(len(global_receipts), 4)
                self.assertEqual(
                    {receipt.provider for receipt in global_receipts},
                    {"Nasdaq", "New York Stock Exchange"},
                )
                for receipt in global_receipts:
                    details = json.loads(receipt.details_json)
                    self.assertEqual(
                        set(details),
                        {
                            "accession",
                            "issuer_cik",
                            "source_observation_id",
                            "source_role",
                            "symbol",
                            "timestamp_source",
                        },
                    )
                    self.assertIsNone(details["accession"])
                    self.assertIsNone(details["issuer_cik"])
                    self.assertIsNone(details["symbol"])
                    self.assertEqual(details["timestamp_source"], "UNAVAILABLE")
                    self.assertEqual(receipt.feed, "UNAVAILABLE")
                    self.assertEqual(receipt.source_time, receipt.retrieved_at)

        resolver = _calendar()
        prior = [resolver.previous_session(session_date)]
        while len(prior) < 14:
            prior.append(resolver.previous_session(prior[-1]))
        prior.reverse()
        first = resolver.session(prior[0])
        last = resolver.session(prior[-1])
        current = resolver.session(session_date)
        self.assertEqual(
            transport._daily_window,
            (
                datetime.combine(
                    prior[0], first.open_time, first.timezone
                ).astimezone(UTC),
                datetime.combine(
                    prior[-1], last.close_time, last.timezone
                ).astimezone(UTC),
            ),
        )
        self.assertEqual(
            transport._minute_window,
            (
                datetime.combine(
                    session_date,
                    current.open_time,
                    current.timezone,
                ).astimezone(UTC),
                mark_cutoff.astimezone(UTC),
            ),
        )
        self.assertEqual(
            transport._quote_window,
            (
                (mark_cutoff - timedelta(minutes=5)).astimezone(UTC),
                mark_cutoff.astimezone(UTC),
            ),
        )
        self.assertFalse(
            any(
                "trades" in url
                or "options" in url
                or "sec.gov" in url
                for url in transport.requested_urls
            )
        )
        self.assertEqual(len(terminal_clock_urls), 1)
        self.assertEqual(len(terminal_clock_urls[0]), 8)
        self.assertIn("/v2/stocks/quotes/latest?", terminal_clock_urls[0][-1])

    def test_uses_reviewed_early_close_windows(self) -> None:
        session_date = date(2026, 11, 27)
        review_at = aware_et(session_date, "12:30")
        mark_cutoff = aware_et(session_date, "12:14")
        source_now = aware_et(session_date, "12:31")
        terminal_at = aware_et(session_date, "12:32")
        transport = _CloseTransport(
            session_date=session_date,
            observed_at=review_at,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                collection = self._collector(
                    journal,
                    transport,
                    source_now=source_now,
                    terminal_at=terminal_at,
                ).collect_close_sources(
                    journal=journal,
                    symbols=("AAPL",),
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=mark_cutoff,
                    command_started_at=source_now,
                )
                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=terminal_at,
                )

        schedule = _calendar().session(session_date)
        self.assertEqual(review.mark_cutoff, mark_cutoff)
        self.assertEqual(
            transport._minute_window,
            (
                datetime.combine(
                    session_date,
                    schedule.open_time,
                    schedule.timezone,
                ).astimezone(UTC),
                mark_cutoff.astimezone(UTC),
            ),
        )
        self.assertEqual(
            transport._quote_window,
            (
                (mark_cutoff - timedelta(minutes=5)).astimezone(UTC),
                mark_cutoff.astimezone(UTC),
            ),
        )

    def test_partial_provider_pages_are_durable_but_role_is_failure_only(self) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        source_now = aware_et(session_date, "15:31")
        terminal_at = aware_et(session_date, "15:32")

        with tempfile.TemporaryDirectory() as temporary_directory:
            journal_path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(journal_path) as journal:
                transport = _PartialQuoteFailureTransport(
                    journal_path=journal_path,
                    session_date=session_date,
                    observed_at=review_at,
                )
                collection = self._collector(
                    journal,
                    transport,
                    source_now=source_now,
                    terminal_at=terminal_at,
                ).collect_close_sources(
                    journal=journal,
                    symbols=("AAPL",),
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=aware_et(session_date, "15:14"),
                    command_started_at=source_now,
                )
                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=terminal_at,
                )
                all_receipts = journal.read_source_observation_receipts(
                    tuple(range(1, journal.count("source_observations") + 1))
                )

        quote_binding = next(
            binding
            for binding in review.bindings
            if binding.symbol == "AAPL" and binding.source_role == "SIP_QUOTE"
        )
        self.assertTrue(
            transport.first_quote_was_durable,
            transport.observed_source_count,
        )
        self.assertEqual(quote_binding.failure_code, "SOURCE_UNAVAILABLE")
        self.assertIsNone(quote_binding.receipt)
        partial_quote_receipts = tuple(
            receipt
            for receipt in all_receipts
            if receipt.source_type == "ALPACA_HISTORICAL_QUOTES"
        )
        self.assertEqual(len(partial_quote_receipts), 1)
        self.assertNotIn(partial_quote_receipts[0].row_id, {
            receipt.row_id for receipt in review.receipts
        })
        self.assertTrue(
            any("page_token=quote-page-2" in url for url in transport.requested_urls)
        )
        self.assertTrue(
            transport.requested_urls[-1].startswith(
                "https://data.alpaca.markets/v2/stocks/quotes/latest?"
            )
        )

    def test_reference_failure_is_one_global_failure_and_other_gets_continue(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        source_now = aware_et(session_date, "15:31")
        terminal_at = aware_et(session_date, "15:32")
        failed_url = (
            "https://www.nyse.com/api/notifications/public/alerts?2=3"
        )
        transport = _ReferenceFailureTransport(
            failed_url=failed_url,
            session_date=session_date,
            observed_at=review_at,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                collection = self._collector(
                    journal,
                    transport,
                    source_now=source_now,
                    terminal_at=terminal_at,
                ).collect_close_sources(
                    journal=journal,
                    symbols=("AAPL",),
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=aware_et(session_date, "15:14"),
                    command_started_at=source_now,
                )
                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=terminal_at,
                )

        operational = next(
            binding
            for binding in review.bindings
            if binding.source_role == "OPERATIONAL_STATUS"
        )
        self.assertEqual(operational.failure_code, "SOURCE_UNAVAILABLE")
        self.assertIsNone(operational.receipt)
        self.assertEqual(
            sum(
                binding.failure_code is not None
                for binding in review.bindings
                if binding.symbol is None
            ),
            1,
        )
        self.assertEqual(len(review.receipts), 7)
        self.assertTrue(
            transport.requested_urls[-1].startswith(
                "https://data.alpaca.markets/v2/stocks/quotes/latest?"
            )
        )

    def test_rejects_noncanonical_symbols_and_wrong_command_date_before_get(
        self,
    ) -> None:
        from stock_monitor.provider_workflows import CanonicalMaterialError

        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        cases = (
            (("aapl",), aware_et(session_date, "15:31")),
            (("AAPL ",), aware_et(session_date, "15:31")),
            (("AAPL",), aware_et(date(2026, 8, 15), "00:01")),
        )
        for symbols, command_started_at in cases:
            with self.subTest(symbols=symbols, command_started_at=command_started_at):
                transport = _CloseTransport(
                    session_date=session_date,
                    observed_at=review_at,
                )
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        collector = self._collector(
                            journal,
                            transport,
                            source_now=aware_et(session_date, "15:31"),
                            terminal_at=aware_et(session_date, "15:32"),
                        )
                        with self.assertRaisesRegex(
                            CanonicalMaterialError,
                            "symbol scope|timing",
                        ):
                            collector.collect_close_sources(
                                journal=journal,
                                symbols=symbols,
                                session_date=session_date,
                                review_at=review_at,
                                mark_cutoff=aware_et(session_date, "15:14"),
                                command_started_at=command_started_at,
                            )
                        self.assertEqual(
                            journal.count("source_observations"),
                            0,
                        )
                        self.assertEqual(journal.count("actual_close_reviews"), 0)
                self.assertEqual(transport.requested_urls, [])

    def test_empty_symbol_scope_collects_only_global_references(self) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        terminal_at = aware_et(session_date, "15:32")
        transport = _CloseTransport(
            session_date=session_date,
            observed_at=review_at,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                collection = self._collector(
                    journal,
                    transport,
                    source_now=aware_et(session_date, "15:31"),
                    terminal_at=terminal_at,
                ).collect_close_sources(
                    journal=journal,
                    symbols=(),
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=aware_et(session_date, "15:14"),
                    command_started_at=aware_et(session_date, "15:31"),
                )
                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=terminal_at,
                )

        self.assertEqual(
            {(binding.symbol, binding.source_role) for binding in review.bindings},
            {(None, role) for role in (
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
                "CROSS_CHECK_CALENDAR",
            )},
        )
        self.assertEqual(len(review.receipts), 4)
        self.assertEqual(len(transport.requested_urls), 4)
        self.assertFalse(
            any(
                url.startswith("https://data.alpaca.markets/")
                for url in transport.requested_urls
            )
        )

    def test_existing_exact_event_evidence_is_bound_from_terminal_actual_replay(
        self,
    ) -> None:
        from tests.integration.test_actual_close_composition import (
            ActualCloseCompositionTests,
        )

        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        source_now = aware_et(session_date, "16:31")
        terminal_at = aware_et(session_date, "16:32")
        fixture = ActualCloseCompositionTests(methodName="runTest")
        fixture.setUp()
        try:
            with tempfile.TemporaryDirectory() as temporary_directory:
                with Journal.open(
                    Path(temporary_directory) / "journal.sqlite3"
                ) as journal:
                    signal_source, _recorded_through = (
                        fixture._seed_linked_open_position(journal)
                    )
                    fixture._persist_clear_event_evidence(
                        journal,
                        signal_source,
                        review_at,
                    )
                    current_signal = journal._read_phase1_signal_source(
                        signal_source.signal_id,
                        query_cutoff=terminal_at,
                    )
                    evidence_source = journal.read_phase1_signal_evidence_source(
                        signal_source.signal_id,
                        review_at=review_at,
                        query_cutoff=terminal_at,
                        calendar_resolver=_calendar(),
                        exact_signal_source=current_signal,
                    )
                    expected_row_ids = set(
                        evidence_source.source_observation_row_ids
                    )
                    transport = _CloseTransport(
                        session_date=session_date,
                        observed_at=review_at,
                    )
                    collection = self._collector(
                        journal,
                        transport,
                        source_now=source_now,
                        terminal_at=terminal_at,
                    ).collect_close_sources(
                        journal=journal,
                        symbols=(signal_source.symbol,),
                        session_date=session_date,
                        review_at=review_at,
                        mark_cutoff=aware_et(session_date, "15:14"),
                        command_started_at=source_now,
                    )
                    review = journal.read_actual_close_review_source(
                        collection.review_id,
                        query_cutoff=terminal_at,
                    )
        finally:
            fixture.doCleanups()

        event_bindings = tuple(
            binding
            for binding in review.bindings
            if binding.symbol == "AAPL"
            and binding.source_role == "EVENT_EVIDENCE"
        )
        self.assertEqual(
            {
                binding.receipt.row_id
                for binding in event_bindings
                if binding.receipt is not None
            },
            expected_row_ids,
        )
        self.assertTrue(event_bindings)
        self.assertTrue(
            all(binding.failure_code is None for binding in event_bindings)
        )

    def test_adverse_or_malformed_global_content_is_persisted_then_failed(self) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        primary_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        trader_url = (
            "https://www.nasdaqtrader.com/rss.aspx?"
            "categorylist=2&feed=currentheadlines"
        )
        operational_url = (
            "https://www.nyse.com/api/notifications/public/alerts?2=3"
        )
        cases = (
            (
                "PRIMARY_HALT_FEED",
                primary_url,
                (
                    b'<?xml version="1.0"?><rss version="2.0" '
                    b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
                    b"<ndaq:numItems>1</ndaq:numItems>"
                    b"<item><ndaq:IssueSymbol>AAPL</ndaq:IssueSymbol></item>"
                    b"</channel></rss>"
                ),
                "application/xml",
            ),
            (
                "TRADER_ALERT_HALT",
                trader_url,
                b"<rss>",
                "application/xml",
            ),
            (
                "OPERATIONAL_STATUS",
                operational_url,
                b"[2]",
                "application/json",
            ),
        )
        for role, url, body, content_type in cases:
            with self.subTest(role=role):
                transport = _ReferenceContentTransport(
                    reference_url=url,
                    body=body,
                    content_type=content_type,
                    session_date=session_date,
                    observed_at=review_at,
                )
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        collection = self._collector(
                            journal,
                            transport,
                            source_now=aware_et(session_date, "15:31"),
                            terminal_at=aware_et(session_date, "15:32"),
                        ).collect_close_sources(
                            journal=journal,
                            symbols=("AAPL",),
                            session_date=session_date,
                            review_at=review_at,
                            mark_cutoff=aware_et(session_date, "15:14"),
                            command_started_at=aware_et(session_date, "15:31"),
                        )
                        review = journal.read_actual_close_review_source(
                            collection.review_id,
                            query_cutoff=collection.collected_at,
                        )
                        all_receipts = journal.read_source_observation_receipts(
                            tuple(
                                range(
                                    1,
                                    journal.count("source_observations") + 1,
                                )
                            )
                        )

                binding = next(
                    binding
                    for binding in review.bindings
                    if binding.symbol is None and binding.source_role == role
                )
                self.assertEqual(binding.failure_code, "SOURCE_UNAVAILABLE")
                self.assertIsNone(binding.receipt)
                persisted = tuple(
                    receipt
                    for receipt in all_receipts
                    if json.loads(receipt.details_json).get("source_role") == role
                )
                self.assertEqual(len(persisted), 1)
                self.assertNotIn(
                    persisted[0].row_id,
                    {receipt.row_id for receipt in review.receipts},
                )

    def test_wrong_disclosure_type_fails_market_roles_without_losing_pages(
        self,
    ) -> None:
        import stock_monitor.actual_close_runtime as runtime_module

        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        transport = _CloseTransport(
            session_date=session_date,
            observed_at=review_at,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                with mock.patch.object(
                    runtime_module,
                    "read_provider_fetch_bundle",
                    return_value=object(),
                ):
                    collection = self._collector(
                        journal,
                        transport,
                        source_now=aware_et(session_date, "15:31"),
                        terminal_at=aware_et(session_date, "15:32"),
                    ).collect_close_sources(
                        journal=journal,
                        symbols=("AAPL",),
                        session_date=session_date,
                        review_at=review_at,
                        mark_cutoff=aware_et(session_date, "15:14"),
                        command_started_at=aware_et(session_date, "15:31"),
                    )
                review = journal.read_actual_close_review_source(
                    collection.review_id,
                    query_cutoff=collection.collected_at,
                )
                self.assertEqual(journal.count("source_observations"), 8)

        market_bindings = tuple(
            binding
            for binding in review.bindings
            if binding.symbol == "AAPL"
            and binding.source_role != "EVENT_EVIDENCE"
        )
        self.assertEqual(len(market_bindings), 4)
        self.assertTrue(
            all(
                binding.failure_code == "SOURCE_UNAVAILABLE"
                for binding in market_bindings
            )
        )
        self.assertEqual(len(review.receipts), 4)

    def test_terminal_clock_cannot_precede_command_or_persisted_receipts(self) -> None:
        from stock_monitor.provider_workflows import CanonicalMaterialError

        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        cases = (
            (
                "command",
                aware_et(session_date, "15:31"),
                aware_et(session_date, "15:30"),
            ),
            (
                "receipt",
                aware_et(session_date, "15:30"),
                aware_et(session_date, "15:30") + timedelta(seconds=30),
            ),
        )
        for boundary, command_started_at, terminal_at in cases:
            with self.subTest(boundary=boundary):
                transport = _CloseTransport(
                    session_date=session_date,
                    observed_at=review_at,
                )
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        collector = self._collector(
                            journal,
                            transport,
                            source_now=aware_et(session_date, "15:31"),
                            terminal_at=terminal_at,
                        )
                        with self.assertRaises(CanonicalMaterialError):
                            collector.collect_close_sources(
                                journal=journal,
                                symbols=("AAPL",),
                                session_date=session_date,
                                review_at=review_at,
                                mark_cutoff=aware_et(session_date, "15:14"),
                                command_started_at=command_started_at,
                            )
                        self.assertEqual(journal.count("actual_close_reviews"), 0)
                        self.assertEqual(journal.count("source_observations"), 8)


if __name__ == "__main__":
    unittest.main()
