from __future__ import annotations

import copy
import gc
import hashlib
import inspect
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import stock_monitor.journal as journal_module
import stock_monitor.ledger as ledger_module
import stock_monitor.evidence as evidence_module
import stock_monitor.phase1 as phase1_module
import stock_monitor.providers.alpaca as alpaca_module
import stock_monitor.risk as risk_module
import stock_monitor.screening as screening_module
import stock_monitor.validation as validation_module
import tests.unit._task5_fixtures as task5_fixture_module
import tests.unit.test_evidence as evidence_test_module
from stock_monitor.evidence import DateRange, EvidenceSourceBinding
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import (
    Journal,
    is_verified_journal_action_source,
    report_archive_relative_path,
    stable_report_id,
)
from stock_monitor.ledger import LedgerSignal
from stock_monitor.providers.cache import SourceDocument
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    replay_actual,
)
from stock_monitor.risk import (
    LongPlanRequest,
    RiskBlock,
    SessionCalendarResolver,
    plan_long,
)
from stock_monitor.providers.http import HttpResponse
from tests.integration.test_signal_lifecycle import (
    _calendar,
    _issued_candidates,
    _issued_publication,
    _pin_provider_cohort_pages,
    _publish,
    _signal,
)
from tests.support import FixtureTransport, aware_et, credentials, policy_fixture
from tests.unit._task5_fixtures import (
    TEST_UNIVERSE,
    candidate_context,
    evidence,
    universe_candidate_contexts,
)
from tests.unit.test_position_sizing import authorized_plan
from tests.unit.test_phase1_promotion import window as diagnostic_phase1_window
from tests.unit.test_evidence import (
    AS_OF as _EVIDENCE_AS_OF,
    binding_document as reviewed_binding_document,
    coverage_document as reviewed_coverage_document,
    decision as reviewed_evidence_fixture,
    etf_record as reviewed_etf_evidence_record,
    iso as reviewed_evidence_iso,
    record as reviewed_evidence_record,
    registry_record_document as reviewed_registry_record_document,
)


_SESSION = date(2026, 8, 14)
_WINDOW_ID = "1" * 64
_ET_ZONE = ZoneInfo("America/New_York")
_PROVIDER_PAGE_FIXTURES: dict[str, dict[str, object]] = {}


def _provider_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _session_hold_sessions(session_date: date) -> tuple[date, ...]:
    return tuple(_calendar().add_sessions(session_date, offset) for offset in range(10))


def _session_reviewed_evidence(
    *,
    session_date: date,
    sequence: int,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
    review_at: datetime | None = None,
    adverse: bool = False,
    binary_event_coverage: str | None = None,
):
    """Issue exact, session-unique reviewed evidence from raw source documents."""
    as_of = aware_et(session_date, "08:45") if review_at is None else review_at
    retrieved_at = as_of - timedelta(hours=1)
    is_etf = subject_kind == "ETF"
    coverage = (
        "NOT_APPLICABLE" if is_etf else "CONFIRMED_CLEAR"
        if binary_event_coverage is None
        else binary_event_coverage
    )
    if binary_event_coverage is not None:
        coverage = binary_event_coverage
    record = task5_fixture_module._reviewed_record(
        sequence=sequence,
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        event_type="fund sponsor notice" if is_etf else "material agreement",
        published_at=as_of - timedelta(days=5, hours=2),
        retrieved_at=retrieved_at,
        adverse_tags=("restatement",) if adverse else (),
    )
    primary_binding = task5_fixture_module._source_binding(
        record,
        subject_kind=subject_kind,
        healthy=True,
    )
    coverage_identifier = (
        f"coverage-{symbol.lower()}-{session_date.isoformat()}-{sequence:03d}"
    )
    attestations = task5_fixture_module._coverage_attestations(
        identifier=coverage_identifier,
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        binary_event_coverage=coverage,
        etf_action_coverage=(
            "CONFIRMED_CLEAR" if is_etf else "NOT_APPLICABLE"
        ),
        checked_at=retrieved_at,
        healthy=True,
    )
    coverage_payload = {
        "attestations": [
            task5_fixture_module._coverage_document(value)
            for value in attestations
        ],
        "kind": "REVIEWED_EVIDENCE_COVERAGE",
        "schema_version": 1,
        "source_observation_id": coverage_identifier,
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    coverage_body = json.dumps(
        coverage_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    coverage_document = SourceDocument(
        url="https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        published_at=retrieved_at,
        retrieved_at=retrieved_at,
        content_hash=hashlib.sha256(coverage_body).hexdigest(),
        body=coverage_body,
        source_observation_id=coverage_identifier,
        publisher="Nasdaq",
        source_type="OFFICIAL_REFERENCE",
        timestamp_source="PRIMARY_METADATA",
        source_role="CROSS_CHECK_CALENDAR",
    )
    coverage_binding = EvidenceSourceBinding.from_document(
        coverage_document,
        symbol=symbol,
        issuer_cik=issuer_cik,
        checked_at=retrieved_at,
        valid_until=retrieved_at + timedelta(hours=24),
        healthy=True,
    )
    bindings = tuple(
        sorted(
            (primary_binding, coverage_binding),
            key=lambda value: value.source_observation_id,
        )
    )
    registry_document = {
        "coverage_attestations": [
            task5_fixture_module._coverage_document(value)
            for value in attestations
        ],
        "kind": "REVIEWED_EVIDENCE_BUNDLE",
        "records": [
            {
                **task5_fixture_module._record_document(record),
                "content_hash": record.content_hash,
            }
        ],
        "registry_id": (
            f"phase1-{subject_kind.lower()}-{symbol.lower()}-"
            f"{session_date.isoformat()}-{sequence:03d}"
        ),
        "reviewed_at": task5_fixture_module._iso_timestamp(
            as_of - timedelta(minutes=5)
        ),
        "schema_version": 2,
        "source_bindings": [
            task5_fixture_module._binding_document(value)
            for value in bindings
        ],
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    registry_payload = json.dumps(
        registry_document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    release_sha256 = hashlib.sha256(registry_payload).hexdigest()
    with tempfile.TemporaryDirectory() as directory:
        project_root = Path(directory)
        registry_path = project_root / "data" / "evidence"
        registry_path.mkdir(parents=True)
        (registry_path / "current.json").write_bytes(registry_payload)
        with mock.patch.object(
            evidence_module,
            "CURRENT_EVIDENCE_REGISTRY_SHA256",
            release_sha256,
        ):
            bundle = evidence_module.load_current_evidence_bundle(
                project_root,
                as_of=as_of,
                source_documents={
                    value.source_observation_id: value.document
                    for value in bindings
                },
            )
    hold_sessions = _session_hold_sessions(session_date)
    decision = evidence_module.classify_evidence(
        (record,),
        DateRange(hold_sessions[0], hold_sessions[-1]),
        symbol=symbol,
        issuer_cik=issuer_cik,
        source_bindings=bindings,
        as_of=as_of,
        subject_kind=subject_kind,
        coverage_attestations=attestations,
        reviewed_bundle=bundle,
    )
    return bundle, decision


class _CandidateAlpacaTransport:
    def __init__(self, contexts: tuple[object, ...]) -> None:
        self._bars = dict(contexts[0].bars_by_symbol)
        self._previous_quotes = {
            context.record.symbol: context.previous_session_quote
            for context in contexts
        }
        self._latest_quotes = {
            context.record.symbol: context.latest_iex_quote
            for context in contexts
        }
        self.bodies_by_url: dict[str, bytes] = {}

    def get(self, url: str, headers: object) -> HttpResponse:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        symbols = tuple(query["symbols"][0].split(","))
        if parsed.path == "/v2/stocks/bars":
            document = {
                "bars": {
                    symbol: [
                        {
                            "c": str(bar.close),
                            "h": str(bar.high),
                            "l": str(bar.low),
                            "o": str(bar.open),
                            "t": _provider_timestamp(bar.timestamp),
                            "v": bar.volume,
                        }
                        for bar in self._bars[symbol]
                    ]
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes/latest":
            document = {
                "quotes": {
                    symbol: {
                        "ap": str(self._latest_quotes[symbol].ask),
                        "bp": str(self._latest_quotes[symbol].bid),
                        "i": self._latest_quotes[symbol].sequence,
                        "t": _provider_timestamp(
                            self._latest_quotes[symbol].timestamp
                        ),
                    }
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes":
            document = {
                "next_page_token": None,
                "quotes": {
                    symbol: [
                        {
                            "ap": str(self._previous_quotes[symbol].ask),
                            "bp": str(self._previous_quotes[symbol].bid),
                            "i": self._previous_quotes[symbol].sequence,
                            "t": _provider_timestamp(
                                self._previous_quotes[symbol].timestamp
                            ),
                        }
                    ]
                    for symbol in symbols
                },
            }
        else:
            raise AssertionError(f"unexpected Alpaca fixture URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.bodies_by_url[url] = body
        return HttpResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            url=url,
        )


class _AuthorityLifecycleTransport:
    def __init__(
        self,
        *,
        symbol: str,
        trade_price: Decimal,
        bid: Decimal,
        ask: Decimal,
        trade_at: datetime | None = None,
        quote_at: datetime | None = None,
        emit_quote: bool = True,
    ) -> None:
        self._symbol = symbol
        self._trade_price = trade_price
        self._bid = bid
        self._ask = ask
        self._trade_at = (
            aware_et(_SESSION, "09:36").astimezone(UTC)
            if trade_at is None
            else trade_at.astimezone(UTC)
        )
        self._quote_at = (
            aware_et(_SESSION, "09:37").astimezone(UTC)
            if quote_at is None
            else quote_at.astimezone(UTC)
        )
        self._emit_quote = emit_quote
        self.bodies_by_url: dict[str, bytes] = {}

    def get(self, url: str, headers: object) -> HttpResponse:
        parsed = urlsplit(url)
        requested = tuple(parse_qs(parsed.query)["symbols"][0].split(","))
        if requested != (self._symbol,):
            raise AssertionError("unexpected lifecycle symbol request")
        if parsed.path == "/v2/stocks/trades":
            document = {
                "next_page_token": None,
                "trades": {
                    self._symbol: [
                        {
                            "i": 101,
                            "p": str(self._trade_price),
                            "s": 100,
                            "t": _provider_timestamp(self._trade_at),
                        }
                    ]
                },
            }
        elif parsed.path == "/v2/stocks/quotes":
            document = {
                "next_page_token": None,
                "quotes": {
                    self._symbol: (
                        [
                            {
                                "ap": str(self._ask),
                                "bp": str(self._bid),
                                "i": 202,
                                "t": _provider_timestamp(self._quote_at),
                            }
                        ]
                        if self._emit_quote
                        else []
                    )
                },
            }
        else:
            raise AssertionError(f"unexpected lifecycle URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.bodies_by_url[url] = body
        return HttpResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            url=url,
        )


def _issued_provider_candidates(
    count: int,
    *,
    forge_aapl_latest: bool = False,
):
    raw_contexts = tuple(universe_candidate_contexts())
    transport = _CandidateAlpacaTransport(raw_contexts)
    now = raw_contexts[0].as_of.astimezone(UTC)
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: now,
    )
    bar_symbols = tuple(sorted(raw_contexts[0].bars_by_symbol))
    raw_bars = tuple(
        bar
        for values in raw_contexts[0].bars_by_symbol.values()
        for bar in values
    )
    bar_window = alpaca_module.TimeWindow(
        min(bar.timestamp for bar in raw_bars),
        max(bar.timestamp for bar in raw_bars),
    )
    provider_bars = client.daily_bars(bar_symbols, bar_window)
    candidate_symbols = tuple(
        sorted(context.record.symbol for context in raw_contexts)
    )
    prior_times = tuple(
        context.previous_session_quote.timestamp for context in raw_contexts
    )
    quote_window = alpaca_module.TimeWindow(
        min(prior_times) - timedelta(minutes=1),
        max(prior_times) + timedelta(minutes=2),
    )
    provider_previous = client.historical_quotes(
        candidate_symbols,
        quote_window,
    )
    provider_latest = client.latest_iex_quotes(candidate_symbols)
    contexts: list[object] = []
    for context in raw_contexts:
        is_etf = context.record.product_type == "etf"
        latest_quote = provider_latest[context.record.symbol]
        if forge_aapl_latest and context.record.symbol == "AAPL":
            latest_quote = replace(
                latest_quote,
                bid=latest_quote.bid + Decimal("0.000001"),
            )
        contexts.append(
            replace(
                context,
                bars_by_symbol=provider_bars,
                previous_session_quote=provider_previous[
                    context.record.symbol
                ][-1],
                latest_iex_quote=latest_quote,
                evidence=evidence(
                    subject_kind="ETF" if is_etf else "STOCK",
                    symbol=context.record.symbol,
                    issuer_cik=None if is_etf else "0000000000",
                    age_days=5,
                    event_type=(
                        "fund sponsor notice"
                        if is_etf
                        else "material agreement"
                    ),
                    binary_event_coverage=(
                        "NOT_APPLICABLE" if is_etf else "CONFIRMED_CLEAR"
                    ),
                    etf_action_coverage=(
                        "CONFIRMED_CLEAR" if is_etf else "NOT_APPLICABLE"
                    ),
                ),
            )
        )
    cohort = screening_module.build_base_eligible_cohort(
        tuple(contexts),
        universe=TEST_UNIVERSE,
    )
    candidates = tuple(
        screening_module.to_scored_candidate(context)
        for context in cohort.contexts
        if screening_module.score_candidate(context).publishable
        and screening_module.detect_setup(context).eligible
    )
    ranked = screening_module.rank_candidates(candidates)
    selected = (
        (next(candidate for candidate in ranked if candidate.symbol == "AAPL"),)
        if count == 1
        else ranked[:count]
    )

    _PROVIDER_PAGE_FIXTURES.clear()
    for candidate in selected:
        authority = screening_module._issued_scored_candidate_authority(
            candidate
        )
        assert authority is not None
        for source in authority.normalized_market_fact_sources:
            for page in source.fetch_manifest.pages:
                body = transport.bodies_by_url[page.request_url]
                document = json.loads(body)
                collection = document[source.fetch_manifest.collection]
                timestamps = tuple(
                    datetime.fromisoformat(
                        str(item["t"]).replace("Z", "+00:00")
                    )
                    for values in collection.values()
                    for item in (values if isinstance(values, list) else (values,))
                )
                _PROVIDER_PAGE_FIXTURES[page.source_observation_id] = {
                    "body": body,
                    "feed": source.feed,
                    "page": page,
                    "retrieved_at": now,
                    "source_time": max(timestamps),
                }
    return selected


def _session_candidate_contexts(
    session_date: date,
    *,
    sequence: int,
) -> tuple[object, ...]:
    resolver = _calendar()
    market_calendar = resolver.calendars[0]
    previous_session = resolver.previous_session(session_date)
    history_sessions = [previous_session]
    while len(history_sessions) < 60:
        history_sessions.append(
            resolver.previous_session(history_sessions[-1])
        )
    history_sessions.reverse()
    raw_contexts = tuple(universe_candidate_contexts())
    raw_bars = raw_contexts[0].bars_by_symbol
    bars_by_symbol = {
        symbol: tuple(
            replace(
                bar,
                timestamp=aware_et(history_session, "16:00").astimezone(UTC),
                source_observation_id=(
                    f"{symbol}-{history_session.isoformat()}-{sequence:03d}"
                ),
            )
            for history_session, bar in zip(history_sessions, values, strict=True)
        )
        for symbol, values in raw_bars.items()
    }
    as_of = aware_et(session_date, "08:45")
    hold_sessions = _session_hold_sessions(session_date)
    attestation = screening_module.build_market_session_attestation(
        market_calendar,
        session_date,
        as_of,
    )
    contexts: list[object] = []
    for context in raw_contexts:
        symbol = context.record.symbol
        is_etf = context.record.product_type == "etf"
        issuer_cik = None if is_etf else "0000000000"
        with mock.patch.object(task5_fixture_module, "RUN_AT", as_of):
            instrument_status = task5_fixture_module.reviewed_instrument_status(
                symbol,
                context.record.listing_venue,
            )
        _bundle, reviewed = _session_reviewed_evidence(
            session_date=session_date,
            sequence=sequence,
            subject_kind="ETF" if is_etf else "STOCK",
            symbol=symbol,
            issuer_cik=issuer_cik,
        )
        contexts.append(
            replace(
                context,
                bars_by_symbol=bars_by_symbol,
                previous_session_quote=replace(
                    context.previous_session_quote,
                    timestamp=aware_et(previous_session, "15:58"),
                    source_observation_id=(
                        f"previous-sip-quote-{symbol.lower()}-"
                        f"{session_date.isoformat()}"
                    ),
                ),
                latest_iex_quote=replace(
                    context.latest_iex_quote,
                    timestamp=as_of - timedelta(seconds=120),
                    source_observation_id=(
                        f"latest-iex-quote-{symbol.lower()}-"
                        f"{session_date.isoformat()}"
                    ),
                ),
                instrument_status=instrument_status,
                evidence=reviewed,
                issuer_cik=issuer_cik,
                session_date=session_date,
                previous_session_date=previous_session,
                as_of=as_of,
                hold_sessions=hold_sessions,
                session_attestation=attestation,
                market_calendar=market_calendar,
            )
        )
    return tuple(contexts)


def _issued_provider_candidates_for_session(
    session_date: date,
    *,
    sequence: int,
    count: int = 1,
):
    raw_contexts = _session_candidate_contexts(
        session_date,
        sequence=sequence,
    )
    transport = _CandidateAlpacaTransport(raw_contexts)
    now = aware_et(session_date, "08:45").astimezone(UTC)
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: now,
    )
    bar_symbols = tuple(sorted(raw_contexts[0].bars_by_symbol))
    raw_bars = tuple(
        bar
        for values in raw_contexts[0].bars_by_symbol.values()
        for bar in values
    )
    provider_bars = client.daily_bars(
        bar_symbols,
        alpaca_module.TimeWindow(
            min(bar.timestamp for bar in raw_bars),
            max(bar.timestamp for bar in raw_bars),
        ),
    )
    candidate_symbols = tuple(
        sorted(context.record.symbol for context in raw_contexts)
    )
    prior_times = tuple(
        context.previous_session_quote.timestamp for context in raw_contexts
    )
    provider_previous = client.historical_quotes(
        candidate_symbols,
        alpaca_module.TimeWindow(
            min(prior_times) - timedelta(minutes=1),
            max(prior_times) + timedelta(minutes=2),
        ),
    )
    provider_latest = client.latest_iex_quotes(candidate_symbols)
    contexts = tuple(
        replace(
            context,
            bars_by_symbol=provider_bars,
            previous_session_quote=provider_previous[context.record.symbol][-1],
            latest_iex_quote=provider_latest[context.record.symbol],
        )
        for context in raw_contexts
    )
    cohort = screening_module.build_base_eligible_cohort(
        contexts,
        universe=TEST_UNIVERSE,
    )
    if cohort.status != "READY":
        raise AssertionError(cohort.reason_codes)
    candidates = tuple(
        screening_module.to_scored_candidate(context)
        for context in cohort.contexts
        if screening_module.score_candidate(context).publishable
        and screening_module.detect_setup(context).eligible
    )
    ranked = screening_module.rank_candidates(candidates)
    selected = (
        (next(candidate for candidate in ranked if candidate.symbol == "AAPL"),)
        if count == 1
        else ranked[:count]
    )
    _PROVIDER_PAGE_FIXTURES.clear()
    for candidate in selected:
        authority = screening_module._issued_scored_candidate_authority(candidate)
        assert authority is not None
        for source in authority.normalized_market_fact_sources:
            for page in source.fetch_manifest.pages:
                body = transport.bodies_by_url[page.request_url]
                document = json.loads(body)
                collection = document[source.fetch_manifest.collection]
                timestamps = tuple(
                    datetime.fromisoformat(
                        str(item["t"]).replace("Z", "+00:00")
                    )
                    for values in collection.values()
                    for item in (
                        values if isinstance(values, list) else (values,)
                    )
                )
                _PROVIDER_PAGE_FIXTURES[page.source_observation_id] = {
                    "body": body,
                    "feed": source.feed,
                    "page": page,
                    "retrieved_at": now,
                    "source_time": max(timestamps),
                }
    return selected


def _start_window(journal: Journal, *, window_id: str = _WINDOW_ID) -> None:
    journal.start_phase1_validation_window(
        window_id=window_id,
        started_session=date(2026, 8, 13),
        starting_capital=Decimal("5000"),
        started_at=aware_et(date(2026, 8, 13), "16:00"),
        received_at=aware_et(date(2026, 8, 13), "16:00"),
        calendar_resolver=_calendar(),
    )


def _canonical_instant(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _append_publication_source_pin(
    journal: Journal,
    *,
    external_id: str,
    ordinal: int,
) -> tuple[int, str]:
    provider_page = _PROVIDER_PAGE_FIXTURES.get(external_id)
    if provider_page is None:
        payload = json.dumps(
            {"raw_source": external_id, "version": 1},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        source_uri = (
            "https://phase1.invalid/task5/"
            + hashlib.sha256(external_id.encode()).hexdigest()
        )
        source_type = "TASK5_CANDIDATE_EVIDENCE"
        provider = "TASK5_AUTHORITY"
        feed = None
        source_time = aware_et(date(2026, 8, 13), "16:00")
        retrieved_at = source_time
        provider_sequence = int(hashlib.sha256(external_id.encode()).hexdigest()[:8], 16)
    else:
        payload = provider_page["body"]
        page = provider_page["page"]
        assert isinstance(payload, bytes)
        source_uri = page.request_url
        source_type = page.source_type
        provider = "alpaca"
        feed = str(provider_page["feed"])
        source_time = provider_page["source_time"]
        retrieved_at = provider_page["retrieved_at"]
        provider_sequence = ordinal
    assert isinstance(payload, bytes)
    assert isinstance(source_time, datetime)
    assert isinstance(retrieved_at, datetime)
    delay_seconds = int((retrieved_at - source_time).total_seconds())
    details = {"source_observation_id": external_id}
    row_id, _duplicate = journal.append_source_observation(
        payload=payload,
        source_uri=source_uri,
        source_type=source_type,
        provider=provider,
        feed=feed,
        source_time=source_time,
        retrieved_at=retrieved_at,
        provider_sequence=provider_sequence,
        delay_seconds=delay_seconds,
        health_result="OK",
        details=details,
    )
    observation_material = journal_module._canonical_json(
        {
            "delay_seconds": delay_seconds,
            "details": json.loads(journal_module._canonical_details(details)),
            "feed": feed,
            "health_result": "OK",
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "provider": provider,
            "provider_sequence": provider_sequence,
            "retrieved_at": journal_module._canonical_timestamp(retrieved_at),
            "source_time": journal_module._canonical_timestamp(source_time),
            "source_type": source_type,
            "source_uri": source_uri,
        }
    )
    return row_id, hashlib.sha256(observation_material.encode()).hexdigest()


def _publish_session_primary(
    journal: Journal,
    *,
    session_date: date,
    sequence: int,
):
    cutoff = aware_et(session_date, "08:45")
    previous_session = _calendar().previous_session(session_date)
    candidates = _issued_provider_candidates_for_session(
        session_date,
        sequence=sequence,
        count=1,
    )
    with mock.patch.object(journal_module, "_utc_now", return_value=cutoff):
        claim = journal.claim_report(session_date, "MORNING")
    if claim.status not in {"ACQUIRED", "RECOVERED_EXPIRED"}:
        raise AssertionError(f"unexpected report claim status: {claim.status}")

    def issue_current(publication_source=None):
        replay_source = journal._read_phase1_canonical_replay_source(
            query_cutoff=cutoff,
            publication_predecessor=True,
            predecessor_publication_source=publication_source,
            calendar_resolver=_calendar(),
            policy=policy_fixture(),
        )
        replay = ledger_module._issue_canonical_ledger_replay_from_phase1_source(
            replay_source
        )
        history_source = journal._read_phase1_breaker_history_source(
            ledger_name="CANONICAL",
            through_session=previous_session,
            query_cutoff=cutoff,
        )
        history = risk_module._issue_breaker_history_from_phase1_source(
            history_source,
            calendar_resolver=_calendar(),
        )
        breaker = risk_module.evaluate_authorized_breakers(history)
        primary = screening_module.rank_candidates(candidates)[0]
        request = LongPlanRequest.from_scored_candidate(primary)
        policy = policy_fixture()
        portfolio = risk_module._issue_portfolio_risk_authority(
            request=request,
            ledger_pair=replay.ledger_pair,
            ledger_name="CANONICAL",
            breaker_state=breaker,
            calendar_resolver=_calendar(),
            policy=policy,
            scope="CANONICAL_PUBLICATION",
            as_of=cutoff,
            phase1_canonical_replay=replay,
        )
        plan = plan_long(
            request,
            portfolio.portfolio_state,
            policy,
            portfolio_authority=portfolio,
        )
        decision = screening_module._issue_portfolio_bound_publication_decision(
            candidates,
            primary_plan_decision=plan,
        )
        return decision, plan

    decision, plan = issue_current()
    manifest = screening_module._publication_observation_manifest(decision)
    pins = tuple(
        _append_publication_source_pin(
            journal,
            external_id=external_id,
            ordinal=ordinal,
        )
        for ordinal, external_id in enumerate(
            manifest.source_observation_ids,
            start=1,
        )
    )
    decision, plan = issue_current()
    state_sha256 = journal_module.phase1_publication_state_sha256(
        decision,
        plan,
    )
    body = json.dumps(
        {
            "phase1_publication": journal_module._phase1_publication_state_manifest(
                decision,
                plan,
            ),
            "published_at": _canonical_instant(cutoff),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    ordered_sha256s = tuple(
        digest for _row_id, digest in sorted(pins, key=lambda item: (item[1], item[0]))
    )
    report_id = stable_report_id(
        "MORNING",
        session_date,
        ordered_sha256s,
        state_sha256,
    )
    with mock.patch.object(journal_module, "_utc_now", return_value=cutoff):
        finalized = journal.finalize_report(
            claim_id=claim.claim_id,
            claim_token=claim.claim_token or "",
            body=body,
            state_sha256=state_sha256,
            observation_ids=tuple(row_id for row_id, _digest in pins),
            archive_relative_path=report_archive_relative_path(
                "MORNING",
                session_date,
                report_id,
            ),
            created_at=cutoff,
            outbox_destination="CODEX_TASK",
            outbox_payload="phase1 session publication",
        )
    publication_source = journal.read_phase1_publication_source(
        finalized.report_id
    )
    decision, plan = issue_current(publication_source)
    stored = journal.publish_phase1_report(
        publication_source=publication_source,
        decision=decision,
        primary_plan_decision=plan,
        validation_window_id=_WINDOW_ID,
        calendar_resolver=_calendar(),
        received_at=cutoff,
    )
    return journal._read_phase1_signal_source(
        stored.signal_id,
        query_cutoff=cutoff,
    )


def _empty_authority_chain(journal: Journal):
    cutoff = aware_et(_SESSION, "08:45")
    replay_source = journal._read_phase1_canonical_replay_source(
        query_cutoff=cutoff,
    )
    replay = ledger_module._issue_canonical_ledger_replay_from_phase1_source(
        replay_source
    )
    history_source = journal._read_phase1_breaker_history_source(
        ledger_name="CANONICAL",
        through_session=date(2026, 8, 13),
        query_cutoff=cutoff,
    )
    history = risk_module._issue_breaker_history_from_phase1_source(
        history_source,
        calendar_resolver=_calendar(),
    )
    breaker = risk_module.evaluate_authorized_breakers(history)
    request = LongPlanRequest(
        entry=Decimal("100"),
        stop=Decimal("97.50"),
        tick_size=Decimal("0.01"),
        session_date=_SESSION,
        symbol="SPY",
        published_target=Decimal("105"),
    )
    portfolio = risk_module._issue_portfolio_risk_authority(
        request=request,
        ledger_pair=replay.ledger_pair,
        ledger_name="CANONICAL",
        breaker_state=breaker,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
        scope="CANONICAL_PUBLICATION",
        as_of=cutoff,
        phase1_canonical_replay=replay,
    )
    return replay_source, replay, history_source, history, breaker, portfolio


def _append_unrelated_source(journal: Journal, *, suffix: str) -> None:
    observed_at = aware_et(_SESSION, "15:55")
    journal.append_source_observation(
        payload=(f'{{"kind":"unrelated","suffix":"{suffix}"}}').encode(),
        source_uri=f"https://example.invalid/{suffix}",
        source_type="MARKET_DATA",
        provider="fixture",
        feed="SIP",
        source_time=observed_at,
        retrieved_at=observed_at + timedelta(seconds=1),
        provider_sequence=999,
        delay_seconds=0,
        health_result="OK",
        details={"symbol": "QQQ"},
    )


def _append_completed_entry_observations(
    journal: Journal,
    signal_source: object,
    *,
    session_date: date | None = None,
    reverse_cohort_order: bool = False,
    simultaneous_trade_quote: bool = False,
    empty_quote: bool = False,
    request_end: datetime | None = None,
    trade_price: Decimal | None = None,
    quote_bid: Decimal | None = None,
    quote_ask: Decimal | None = None,
) -> tuple[str, str | None, datetime]:
    effective_session = (
        signal_source.publication_session
        if session_date is None
        else session_date
    )
    signal_id = signal_source.signal_id
    maximum_entry = risk_module.money_from_micros(
        signal_source.maximum_entry_micros
    )
    tick_size = risk_module.money_from_micros(
        signal_source.tick_size_micros
    )
    transport = _AuthorityLifecycleTransport(
        symbol=signal_source.symbol,
        trade_price=(
            risk_module.money_from_micros(
                signal_source.trigger_price_micros
            )
            if trade_price is None
            else trade_price
        ),
        bid=(maximum_entry - tick_size if quote_bid is None else quote_bid),
        ask=maximum_entry if quote_ask is None else quote_ask,
        trade_at=aware_et(effective_session, "09:36"),
        quote_at=(
            aware_et(effective_session, "09:36")
            if simultaneous_trade_quote
            else aware_et(effective_session, "09:37")
        ),
        emit_quote=not empty_quote,
    )
    retrieved_at = aware_et(effective_session, "16:20").astimezone(UTC)
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: retrieved_at,
    )
    full_session = alpaca_module.TimeWindow(
        aware_et(effective_session, "09:30").astimezone(UTC),
        (
            aware_et(effective_session, "16:00")
            if request_end is None
            else request_end
        ).astimezone(UTC),
    )
    trade_cohort = client.historical_trades(
        (signal_source.symbol,),
        full_session,
    )
    quote_cohort = client.historical_quotes(
        (signal_source.symbol,),
        full_session,
    )
    trade_rows = _pin_provider_cohort_pages(
        journal,
        trade_cohort,
        transport,
    )
    quote_rows = _pin_provider_cohort_pages(
        journal,
        quote_cohort,
        transport,
    )
    cohorts = (
        (quote_cohort, trade_cohort)
        if reverse_cohort_order
        else (trade_cohort, quote_cohort)
    )
    ingested = journal.ingest_phase1_session_cohorts(
        signal_id,
        cohorts,
        core_source_row_ids=(*trade_rows, *quote_rows),
        calendar_resolver=_calendar(),
    )
    self_by_kind = {
        journal.read_phase1_observation(
            observation_id,
            query_cutoff=retrieved_at,
        ).observation_kind: observation_id
        for observation_id in ingested.observation_ids
    }
    trade_observation_id = self_by_kind["TRADE"]
    quote_observation_id = self_by_kind.get("QUOTE")
    completed_at = aware_et(effective_session, "16:21")
    journal.complete_phase1_session(
        signal_id=signal_id,
        session_date=effective_session,
        cohort_through_ordinal=len(ingested.observation_ids),
        expected_observation_count=len(ingested.observation_ids),
        received_through=retrieved_at,
        completed_at=completed_at,
        calendar_resolver=_calendar(),
    )
    return trade_observation_id, quote_observation_id, completed_at


def _confirmation_action_source(
    journal: Journal,
    signal_source: object,
    *,
    event_kind: str,
    after: datetime,
):
    message_time = after + timedelta(seconds=1)
    received_at = after + timedelta(seconds=2)
    if event_kind == "LIVE_CONFIRM":
        maximum_entry = risk_module.money_from_micros(
            signal_source.maximum_entry_micros
        )
        tick_size = risk_module.money_from_micros(
            signal_source.tick_size_micros
        )
        recommended_stop = risk_module.money_from_micros(
            signal_source.recommended_stop_micros
        )
        text = (
            f"BOUGHT {signal_source.symbol} "
            f"{max(1, signal_source.planned_shares)} shares "
            f"@ {maximum_entry} AT 09:37 ET; "
            f"BID {maximum_entry - tick_size} ASK {maximum_entry}; "
            f"STOP SET @ {recommended_stop}"
        )
        expected_domain_kind = "BOUGHT"
    elif event_kind == "LIVE_SKIP":
        text = f"SKIPPED {signal_source.symbol}"
        expected_domain_kind = "SKIPPED"
    else:
        raise AssertionError("unsupported Phase 1 confirmation event kind")
    result = ingest_confirmation(
        journal,
        ConfirmationEnvelope(
            message_id=(
                f"phase1-confirmation:{signal_source.signal_id}:{event_kind}"
            ),
            message_time=message_time,
            received_at=received_at,
            text=text,
            session_date=signal_source.publication_session,
        ),
        plans=UnavailableSignalPlanResolver(),
        calendar=_calendar(),
        policy=policy_fixture(),
        entry_authorities=UnavailableActualEntryAuthorityResolver(),
    )
    with journal.transaction() as transaction:
        source = transaction.read_action_source(
            execution_event_id=result.actions[0].event_row_id,
        )
    assert is_verified_journal_action_source(source)
    assert source.domain_kind == expected_domain_kind
    assert source.symbol == signal_source.symbol
    return source, received_at + timedelta(seconds=1)


def _reread_confirmation_action_source(
    journal: Journal,
    source: object,
):
    with journal.transaction() as transaction:
        return transaction.read_action_source(
            execution_event_id=source.execution_event_id,
        )


def _seed_completed_authority_fill(journal: Journal):
    _publish(journal)
    replay_source = journal._read_phase1_canonical_replay_source(
        query_cutoff=aware_et(_SESSION, "08:45"),
    )
    signal_source = replay_source.signal_sources[0]
    trigger_id, quote_id, completed_at = _append_completed_entry_observations(
        journal,
        signal_source,
    )
    authority = journal.record_phase1_entry(
        signal_source.signal_id,
        confirmation_action_source=None,
        trigger_observation_id=trigger_id,
        quote_observation_id=quote_id,
        calendar_resolver=_calendar(),
        recorded_at=completed_at,
    )
    return authority, completed_at


def _published_signal_source(
    journal: Journal,
    *,
    candidates: tuple[object, ...] | None = None,
):
    _publish(journal, candidates_override=candidates)
    replay_source = journal._read_phase1_canonical_replay_source(
        query_cutoff=aware_et(_SESSION, "08:45"),
    )
    return replay_source.signal_sources[0]


def _position_for_reviewed_evidence(
    *,
    symbol: str = "EXM",
) -> risk_module.Position:
    return risk_module.Position(
        signal_id=f"2026-08-14:{symbol}",
        symbol=symbol,
        entry=Decimal("100"),
        shares=4,
        initial_stop=Decimal("97.50"),
        recommended_stop=Decimal("97.50"),
        user_confirmed_stop=Decimal("97.50"),
        target=Decimal("105"),
        tick_size=Decimal("0.01"),
        entered_session=_SESSION,
        ledger_name="CANONICAL",
    )


def _reviewed_position_evidence_context(
    records: tuple[object, ...],
    **fixture_overrides: object,
):
    options = dict(fixture_overrides)
    subject_kind = options.pop("subject_kind", "STOCK")
    review_at = options.pop("review_at", _EVIDENCE_AS_OF)
    symbol_override = options.get("symbol")
    assert isinstance(subject_kind, str)
    assert isinstance(review_at, datetime)
    (
        values,
        symbol,
        issuer_cik,
        bindings,
        attestations,
        reviewed_bundle,
    ) = reviewed_evidence_fixture(
        records,
        return_context=True,
        authority_as_of=review_at,
        subject_kind=subject_kind,
        **options,
    )
    if symbol_override is not None:
        assert symbol == symbol_override
    position = _position_for_reviewed_evidence(symbol=symbol)
    terminal = _calendar().add_sessions(
        position.entered_session,
        risk_module.MAX_HOLD_SESSIONS - 1,
    )
    decision = evidence_module.classify_evidence(
        values,
        evidence_module.DateRange(
            review_at.astimezone(_ET_ZONE).date(),
            terminal,
        ),
        symbol=symbol,
        issuer_cik=issuer_cik,
        source_bindings=bindings,
        as_of=review_at,
        subject_kind=subject_kind,
        coverage_attestations=attestations,
        reviewed_bundle=reviewed_bundle,
    )
    return position, reviewed_bundle, decision, review_at


def _reviewed_registry_payload(reviewed_bundle: object) -> bytes:
    document = {
        "coverage_attestations": [
            reviewed_coverage_document(value)
            for value in reviewed_bundle.coverage_attestations
        ],
        "kind": "REVIEWED_EVIDENCE_BUNDLE",
        "records": [
            reviewed_registry_record_document(value)
            for value in reviewed_bundle.records
        ],
        "registry_id": reviewed_bundle.registry_id,
        "reviewed_at": reviewed_evidence_iso(reviewed_bundle.reviewed_at),
        "schema_version": 2,
        "source_bindings": [
            reviewed_binding_document(value)
            for value in reviewed_bundle.source_bindings
        ],
        "subject": {
            "issuer_cik": reviewed_bundle.issuer_cik,
            "subject_kind": reviewed_bundle.subject_kind,
            "symbol": reviewed_bundle.symbol,
        },
    }
    payload = json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert hashlib.sha256(payload).hexdigest() == reviewed_bundle.content_hash
    return payload


def _pin_reviewed_evidence_material(
    journal: Journal,
    reviewed_bundle: object,
) -> tuple[int, tuple[int, ...]]:
    registry_payload = _reviewed_registry_payload(reviewed_bundle)
    registry_row_id, _ = journal.append_source_observation(
        payload=registry_payload,
        source_uri=(
            "urn:stock-monitor:reviewed-evidence-registry:"
            f"{reviewed_bundle.registry_id}"
        ),
        source_type="REVIEWED_EVIDENCE_REGISTRY",
        provider="operator-reviewed",
        feed=None,
        source_time=reviewed_bundle.reviewed_at,
        retrieved_at=reviewed_bundle.reviewed_at,
        provider_sequence=None,
        delay_seconds=0,
        health_result="REVIEWED",
        details={
            "content_hash": reviewed_bundle.content_hash,
            "registry_id": reviewed_bundle.registry_id,
        },
    )
    binding_row_ids: list[int] = []
    for binding in reviewed_bundle.source_bindings:
        document = binding.document
        source_time = document.published_at or document.retrieved_at
        row_id, _ = journal.append_source_observation(
            payload=document.body,
            source_uri=document.url,
            source_type=document.source_type,
            provider=document.publisher,
            feed=document.timestamp_source,
            source_time=source_time,
            retrieved_at=document.retrieved_at,
            provider_sequence=None,
            delay_seconds=int(
                (document.retrieved_at - source_time).total_seconds()
            ),
            health_result="OK" if binding.healthy else "UNHEALTHY",
            details={
                "accession": document.accession,
                "issuer_cik": binding.issuer_cik,
                "source_observation_id": document.source_observation_id,
                "source_role": document.source_role,
                "symbol": binding.symbol,
                "timestamp_source": document.timestamp_source,
            },
        )
        binding_row_ids.append(row_id)
    return registry_row_id, tuple(binding_row_ids)


def _persist_signal_evidence(
    journal: Journal,
    *,
    adverse: bool,
    review_at: datetime = _EVIDENCE_AS_OF,
    signal_source: object | None = None,
    binary_event_coverage: str = "CONFIRMED_CLEAR",
):
    if signal_source is None:
        signal_source = _published_signal_source(journal)
    evidence_sequence = 100_000 + review_at.date().timetuple().tm_yday
    bundle, decision = _session_reviewed_evidence(
        session_date=signal_source.publication_session,
        sequence=evidence_sequence,
        subject_kind=signal_source.subject_kind,
        symbol=signal_source.symbol,
        issuer_cik=signal_source.issuer_cik,
        review_at=review_at,
        adverse=adverse,
        binary_event_coverage=binary_event_coverage,
    )
    registry_row_id, binding_row_ids = _pin_reviewed_evidence_material(
        journal,
        bundle,
    )
    signal_source = journal._read_phase1_signal_source(
        signal_source.signal_id,
        query_cutoff=review_at,
    )
    authority = risk_module._issue_phase1_signal_evidence_authority(
        signal_source,
        bundle,
        decision,
        review_at=review_at,
        calendar_resolver=_calendar(),
    )
    stored = journal.record_phase1_signal_evidence(
        authority=authority,
        registry_source_row_id=registry_row_id,
        source_observation_row_ids=binding_row_ids,
    )
    return signal_source, authority, stored, review_at


class _AuthorityExitTransport:
    def __init__(
        self,
        *,
        symbol: str,
        session_date: date,
        daily_sessions: tuple[date, ...],
        previous_session_low: Decimal,
        bid: Decimal,
        ask: Decimal,
        execution_open: Decimal,
        execution_high: Decimal,
        execution_low: Decimal,
        execution_close: Decimal,
        emit_quote: bool = True,
        execution_bars: tuple[
            tuple[datetime, Decimal, Decimal, Decimal, Decimal, int], ...
        ] | None = None,
        quote_ticks: tuple[
            tuple[datetime, Decimal, Decimal, int], ...
        ] | None = None,
    ) -> None:
        self._symbol = symbol
        self._session_date = session_date
        self._daily_sessions = daily_sessions
        self._previous_session_low = previous_session_low
        self._bid = bid
        self._ask = ask
        self._execution_open = execution_open
        self._execution_high = execution_high
        self._execution_low = execution_low
        self._execution_close = execution_close
        self._emit_quote = emit_quote
        self._execution_bars = execution_bars
        self._quote_ticks = quote_ticks
        self.bodies_by_url: dict[str, bytes] = {}

    def get(self, url: str, headers: object) -> HttpResponse:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        requested = tuple(query["symbols"][0].split(","))
        if requested != (self._symbol,):
            raise AssertionError("unexpected exit-review symbol request")
        if parsed.path == "/v2/stocks/bars":
            timeframe = query["timeframe"][0]
            if timeframe == "1Day":
                lows = [Decimal("21.00")] * len(self._daily_sessions)
                if self._daily_sessions[-1] == self._session_date:
                    lows[-2] = self._previous_session_low
                else:
                    lows[-1] = self._previous_session_low
                values = [
                    (
                        {
                            "c": str(
                                self._execution_bars[-1][4]
                                if self._execution_bars
                                else self._execution_close
                            ),
                            "h": str(
                                max(item[2] for item in self._execution_bars)
                                if self._execution_bars
                                else self._execution_high
                            ),
                            "l": str(
                                min(item[3] for item in self._execution_bars)
                                if self._execution_bars
                                else self._execution_low
                            ),
                            "o": str(
                                self._execution_bars[0][1]
                                if self._execution_bars
                                else self._execution_open
                            ),
                            "t": _provider_timestamp(aware_et(day, "09:30")),
                            "v": (
                                sum(item[5] for item in self._execution_bars)
                                if self._execution_bars
                                else 2_000
                            ),
                        }
                        if day == self._session_date
                        else {
                            "c": str(low + Decimal("0.25")),
                            "h": str(low + Decimal("0.50")),
                            "l": str(low),
                            "o": str(low + Decimal("0.25")),
                            "t": _provider_timestamp(aware_et(day, "09:30")),
                            "v": 10_000 + index,
                        }
                    )
                    for index, (day, low) in enumerate(
                        zip(self._daily_sessions, lows, strict=True),
                        start=1,
                    )
                ]
            elif timeframe == "1Min":
                values = (
                    [
                        {
                            "c": str(close),
                            "h": str(high),
                            "l": str(low),
                            "o": str(open_price),
                            "t": _provider_timestamp(at),
                            "v": volume,
                        }
                        for at, open_price, high, low, close, volume in (
                            self._execution_bars or ()
                        )
                    ]
                    if self._execution_bars is not None
                    else [
                        {
                            "c": str(self._execution_close),
                            "h": str(self._execution_high),
                            "l": str(self._execution_low),
                            "o": str(self._execution_open),
                            "t": _provider_timestamp(
                                aware_et(self._session_date, "16:00")
                            ),
                            "v": 2_000,
                        }
                    ]
                )
            else:
                raise AssertionError("unexpected exit-review bar timeframe")
            document = {
                "bars": {self._symbol: values},
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes":
            document = {
                "next_page_token": None,
                "quotes": {
                    self._symbol: (
                        [
                            {
                                "ap": str(ask),
                                "bp": str(bid),
                                "i": sequence,
                                "t": _provider_timestamp(at),
                            }
                            for at, bid, ask, sequence in self._quote_ticks
                        ]
                        if self._quote_ticks is not None
                        else [
                            {
                                "ap": str(self._ask),
                                "bp": str(self._bid),
                                "i": 901,
                                "t": _provider_timestamp(
                                    aware_et(self._session_date, "15:59")
                                    + timedelta(seconds=59)
                                ),
                            }
                        ]
                        if self._emit_quote
                        else []
                    )
                },
            }
        else:
            raise AssertionError(f"unexpected exit-review URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.bodies_by_url[url] = body
        return HttpResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            url=url,
        )


def _open_sessions_ending(day: date, count: int) -> tuple[date, ...]:
    sessions = [day]
    while len(sessions) < count:
        sessions.append(_calendar().previous_session(sessions[-1]))
    return tuple(reversed(sessions))


def _issued_exit_review_cohorts(
    *,
    symbol: str,
    session_date: date,
    bid: Decimal,
    ask: Decimal,
    previous_session_low: Decimal,
    execution_open: Decimal,
    execution_high: Decimal,
    execution_low: Decimal,
    execution_close: Decimal,
    emit_quote: bool = True,
    execution_bars: tuple[
        tuple[datetime, Decimal, Decimal, Decimal, Decimal, int], ...
    ] | None = None,
    quote_ticks: tuple[
        tuple[datetime, Decimal, Decimal, int], ...
    ] | None = None,
    retrieved_at: datetime | None = None,
):
    daily_sessions = _open_sessions_ending(session_date, 14)
    transport = _AuthorityExitTransport(
        symbol=symbol,
        session_date=session_date,
        daily_sessions=daily_sessions,
        previous_session_low=previous_session_low,
        bid=bid,
        ask=ask,
        execution_open=execution_open,
        execution_high=execution_high,
        execution_low=execution_low,
        execution_close=execution_close,
        emit_quote=emit_quote,
        execution_bars=execution_bars,
        quote_ticks=quote_ticks,
    )
    effective_retrieved_at = (
        aware_et(session_date, "16:20").astimezone(UTC)
        if retrieved_at is None
        else retrieved_at.astimezone(UTC)
    )
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: effective_retrieved_at,
    )
    daily_window = alpaca_module.TimeWindow(
        aware_et(daily_sessions[0], "09:30").astimezone(UTC),
        aware_et(session_date, "16:00").astimezone(UTC),
    )
    execution_window = alpaca_module.TimeWindow(
        aware_et(session_date, "09:30").astimezone(UTC),
        aware_et(session_date, "16:00").astimezone(UTC),
    )
    daily = client.daily_bars((symbol,), daily_window)
    execution = client.historical_minute_bars((symbol,), execution_window)
    quotes = client.historical_quotes((symbol,), execution_window)
    assert alpaca_module.provider_fetch_cohorts_share_owner(
        daily,
        execution,
        quotes,
    )
    return daily, execution, quotes, transport, effective_retrieved_at


def _phase1_equity_authorities(
    journal: Journal,
    *,
    emit_quote: bool = True,
    actual_strategy_activity: bool = False,
):
    _authority, completed_at = _seed_completed_authority_fill(journal)
    if actual_strategy_activity:
        actual_actions = (
            (
                "phase1-equity-actual-buy",
                f"BOUGHT {_signal().symbol} 2 shares @ 20 AT 10:14 ET",
            ),
            (
                "phase1-equity-actual-sale",
                f"SOLD {_signal().symbol} 1 shares @ 21 AT 15:31 ET",
            ),
            (
                "phase1-equity-actual-fee",
                f"FEE {_signal().symbol} 0.03 AT 15:33 ET",
            ),
        )
        for ordinal, (message_id, text) in enumerate(
            actual_actions,
            start=1,
        ):
            message_time = completed_at + timedelta(seconds=ordinal * 2 - 1)
            ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id=message_id,
                    message_time=message_time,
                    received_at=message_time + timedelta(seconds=1),
                    text=text,
                    session_date=_SESSION,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
    daily, _execution, quotes, transport, retrieved_at = (
        _issued_exit_review_cohorts(
            symbol=_signal().symbol,
            session_date=_SESSION,
            bid=Decimal("21.00"),
            ask=Decimal("21.01"),
            previous_session_low=Decimal("20.75"),
            execution_open=Decimal("21.00"),
            execution_high=Decimal("21.05"),
            execution_low=Decimal("20.95"),
            execution_close=Decimal("21.00"),
            emit_quote=emit_quote,
        )
    )
    daily_rows = _pin_provider_cohort_pages(journal, daily, transport)
    quote_rows = _pin_provider_cohort_pages(journal, quotes, transport)
    query_cutoff = max(completed_at, retrieved_at) + timedelta(minutes=1)
    journal.ingest_phase1_equity_mark_cohorts(
        _SESSION,
        quote_cohort=quotes,
        daily_bar_cohort=daily,
        core_source_row_ids=(*daily_rows, *quote_rows),
        calendar_resolver=_calendar(),
        recorded_at=query_cutoff,
    )
    canonical_source = journal.read_phase1_equity_mark_source(
        ledger_name="CANONICAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    actual_source = journal.read_phase1_equity_mark_source(
        ledger_name="ACTUAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    canonical = risk_module._issue_phase1_equity_point_from_source(
        canonical_source,
        calendar_resolver=_calendar(),
    )
    actual = risk_module._issue_phase1_equity_point_from_source(
        actual_source,
        calendar_resolver=_calendar(),
    )
    return canonical_source, actual_source, canonical, actual, query_cutoff


def _read_provider_exit_review_source(
    journal: Journal,
    *,
    signal_source: object,
    session_date: date,
    bid: Decimal,
    ask: Decimal,
    previous_session_low: Decimal,
    execution_open: Decimal,
    execution_high: Decimal,
    execution_low: Decimal,
    execution_close: Decimal,
    adverse_evidence: bool,
    persist_evidence: bool = True,
    binary_event_coverage: str = "CONFIRMED_CLEAR",
    evidence_review_at: datetime | None = None,
    execution_bars: tuple[
        tuple[datetime, Decimal, Decimal, Decimal, Decimal, int], ...
    ] | None = None,
    quote_ticks: tuple[
        tuple[datetime, Decimal, Decimal, int], ...
    ] | None = None,
    provider_retrieved_at: datetime | None = None,
):
    review_at = aware_et(session_date, "15:59") + timedelta(seconds=59)
    if persist_evidence:
        _persist_signal_evidence(
            journal,
            adverse=adverse_evidence,
            review_at=(review_at if evidence_review_at is None else evidence_review_at),
            signal_source=signal_source,
            binary_event_coverage=binary_event_coverage,
        )
    daily, execution, quotes, transport, _retrieved_at = (
        _issued_exit_review_cohorts(
            symbol=signal_source.symbol,
            session_date=session_date,
            bid=bid,
            ask=ask,
            previous_session_low=previous_session_low,
            execution_open=execution_open,
            execution_high=execution_high,
            execution_low=execution_low,
            execution_close=execution_close,
            execution_bars=execution_bars,
            quote_ticks=quote_ticks,
            retrieved_at=provider_retrieved_at,
        )
    )
    daily_rows = _pin_provider_cohort_pages(journal, daily, transport)
    execution_rows = _pin_provider_cohort_pages(journal, execution, transport)
    quote_rows = _pin_provider_cohort_pages(journal, quotes, transport)
    ingested = journal.ingest_phase1_exit_review_cohorts(
        signal_source.signal_id,
        daily_bar_cohort=daily,
        execution_bar_cohort=execution,
        quote_cohort=quotes,
        core_source_row_ids=(*daily_rows, *execution_rows, *quote_rows),
        calendar_resolver=_calendar(),
    )
    source = journal.read_phase1_exit_review_source(
        signal_source.signal_id,
        review_session=session_date,
        query_cutoff=ingested.query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    if persist_evidence:
        assert risk_module.is_issued_phase1_position_evidence_authority(
            source.position_evidence
        )
    else:
        assert source.position_evidence is None
    assert alpaca_module.provider_fetch_cohorts_share_owner(
        source.daily_bar_cohort,
        source.execution_bar_cohort,
        source.quote_cohort,
    )
    return source, ingested.query_cutoff


def _prepare_provider_typed_exit(
    journal: Journal,
    **options: object,
):
    source, query_cutoff = _read_provider_exit_review_source(
        journal,
        **options,
    )
    authority = risk_module._issue_phase1_position_exit_authority_from_source(
        source,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    return source, authority, query_cutoff


def _prepare_same_session_batch_exit(journal: Journal):
    exit_session = date(2026, 8, 17)
    target_at = aware_et(exit_session, "14:00")
    later_stop_at = aware_et(exit_session, "15:00")
    review_cutoff = aware_et(exit_session, "15:59") + timedelta(seconds=59)
    signal_source = journal._read_phase1_signal_source(
        _signal().signal_id,
        query_cutoff=review_cutoff,
    )
    return _prepare_provider_typed_exit(
        journal,
        signal_source=signal_source,
        session_date=exit_session,
        bid=Decimal("21.49"),
        ask=Decimal("21.50"),
        previous_session_low=Decimal("21.00"),
        execution_open=Decimal("21.40"),
        execution_high=Decimal("21.50"),
        execution_low=Decimal("20.90"),
        execution_close=Decimal("21.00"),
        adverse_evidence=False,
        evidence_review_at=target_at - timedelta(seconds=2),
        execution_bars=(
            (
                target_at,
                Decimal("21.40"),
                Decimal("21.50"),
                Decimal("21.30"),
                Decimal("21.49"),
                1_000,
            ),
            (
                later_stop_at,
                Decimal("21.00"),
                Decimal("21.05"),
                Decimal("20.90"),
                Decimal("21.00"),
                1_000,
            ),
            (
                aware_et(exit_session, "16:00"),
                Decimal("21.00"),
                Decimal("21.02"),
                Decimal("20.98"),
                Decimal("21.00"),
                1_000,
            ),
        ),
        quote_ticks=(
            (
                target_at - timedelta(seconds=1),
                Decimal("21.49"),
                Decimal("21.50"),
                901,
            ),
            (
                later_stop_at - timedelta(seconds=1),
                Decimal("20.94"),
                Decimal("20.96"),
                902,
            ),
            (
                review_cutoff,
                Decimal("21.49"),
                Decimal("21.50"),
                903,
            ),
        ),
    )


def _record_provider_typed_exit(
    journal: Journal,
    **options: object,
):
    source, authority, query_cutoff = _prepare_provider_typed_exit(
        journal,
        **options,
    )
    stored = journal.record_phase1_canonical_exit(
        exit_authority=authority,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    return source, authority, stored, query_cutoff


def _seed_not_triggered_adherence_material(
    journal: Journal,
    *,
    actual_hard_breach: bool = False,
) -> tuple[str, datetime]:
    _publish(journal)
    signal_source = journal._read_phase1_signal_source(
        _signal().signal_id,
        query_cutoff=aware_et(_SESSION, "08:45"),
    )
    trigger = risk_module.money_from_micros(
        signal_source.trigger_price_micros
    )
    tick = risk_module.money_from_micros(signal_source.tick_size_micros)
    _trade_id, _quote_id, _completed_at = (
        _append_completed_entry_observations(
            journal,
            signal_source,
            trade_price=trigger - tick,
        )
    )
    deadline_session = _calendar().add_sessions(_SESSION, 1)
    query_cutoff = aware_et(deadline_session, "08:45")
    terminal = journal.record_phase1_unentered_terminal(
        signal_source.signal_id,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    assert terminal.to_status == "NOT_TRIGGERED"
    if actual_hard_breach:
        for ordinal, (at, text) in enumerate(
            (
                (
                    "10:14",
                    f"BOUGHT {_signal().symbol} 1 shares @ 20 AT 10:14 ET",
                ),
                (
                    "15:31",
                    f"SOLD {_signal().symbol} 1 shares @ 21 AT 15:31 ET",
                ),
            ),
            start=1,
        ):
            message_time = aware_et(_SESSION, at)
            ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id=f"phase1-not-triggered-hard-{ordinal}",
                    message_time=message_time,
                    received_at=message_time + timedelta(microseconds=1),
                    text=text,
                    session_date=_SESSION,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
    canonical_source = journal.read_phase1_equity_mark_source(
        ledger_name="CANONICAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    actual_source = journal.read_phase1_equity_mark_source(
        ledger_name="ACTUAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    canonical = risk_module._issue_phase1_equity_point_from_source(
        canonical_source,
        calendar_resolver=_calendar(),
    )
    actual = risk_module._issue_phase1_equity_point_from_source(
        actual_source,
        calendar_resolver=_calendar(),
    )
    journal.record_phase1_session_mark(
        canonical_authority=canonical,
        actual_authority=actual,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    return signal_source.signal_id, query_cutoff


def _seed_closed_primary_adherence_material(
    journal: Journal,
    *,
    actual_hard_breach: bool = False,
) -> tuple[str, datetime]:
    (
        _entry_source,
        _entry_actual_source,
        entry_point,
        entry_actual_point,
        entry_cutoff,
    ) = _phase1_equity_authorities(journal)
    journal.record_phase1_session_mark(
        canonical_authority=entry_point,
        actual_authority=entry_actual_point,
        recorded_at=entry_cutoff,
        calendar_resolver=_calendar(),
    )
    exit_session = date(2026, 8, 17)
    review_cutoff = aware_et(exit_session, "15:59") + timedelta(seconds=59)
    signal_source = journal._read_phase1_signal_source(
        _signal().signal_id,
        query_cutoff=review_cutoff,
    )
    _source, _authority, _stored, query_cutoff = _record_provider_typed_exit(
        journal,
        signal_source=signal_source,
        session_date=exit_session,
        bid=Decimal("21.00"),
        ask=Decimal("21.01"),
        previous_session_low=Decimal("20.80"),
        execution_open=Decimal("21.00"),
        execution_high=Decimal("21.05"),
        execution_low=Decimal("20.95"),
        execution_close=Decimal("21.00"),
        adverse_evidence=True,
    )
    if actual_hard_breach:
        for ordinal, (at, text) in enumerate(
            (
                (
                    "10:14",
                    f"BOUGHT {_signal().symbol} 1 shares @ 20 AT 10:14 ET",
                ),
                (
                    "15:31",
                    f"SOLD {_signal().symbol} 1 shares @ 21 AT 15:31 ET",
                ),
            ),
            start=1,
        ):
            message_time = aware_et(exit_session, at)
            ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id=f"phase1-adherence-hard-{ordinal}",
                    message_time=message_time,
                    received_at=message_time + timedelta(microseconds=1),
                    text=text,
                    session_date=exit_session,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
    canonical_source = journal.read_phase1_equity_mark_source(
        ledger_name="CANONICAL",
        session_date=exit_session,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    actual_source = journal.read_phase1_equity_mark_source(
        ledger_name="ACTUAL",
        session_date=exit_session,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    canonical = risk_module._issue_phase1_equity_point_from_source(
        canonical_source,
        calendar_resolver=_calendar(),
    )
    actual = risk_module._issue_phase1_equity_point_from_source(
        actual_source,
        calendar_resolver=_calendar(),
    )
    journal.record_phase1_session_mark(
        canonical_authority=canonical,
        actual_authority=actual,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    return signal_source.signal_id, query_cutoff


def _seed_shadow_adherence_material(
    journal: Journal,
) -> tuple[str, datetime]:
    _publish(journal, candidates_override=_issued_candidates(2))
    replay_source = journal._read_phase1_canonical_replay_source(
        query_cutoff=aware_et(_SESSION, "08:45"),
    )
    shadow_source = next(
        source
        for source in replay_source.signal_sources
        if source.role == "WATCHLIST_SHADOW"
    )
    trigger_id, quote_id, completed_at = _append_completed_entry_observations(
        journal,
        shadow_source,
    )
    authority = journal.record_phase1_entry(
        shadow_source.signal_id,
        confirmation_action_source=None,
        trigger_observation_id=trigger_id,
        quote_observation_id=quote_id,
        calendar_resolver=_calendar(),
        recorded_at=completed_at,
    )
    assert ledger_module.is_issued_shadow_fill_disposition_authority(authority)
    query_cutoff = aware_et(_calendar().add_sessions(_SESSION, 1), "08:45")
    canonical_source = journal.read_phase1_equity_mark_source(
        ledger_name="CANONICAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    actual_source = journal.read_phase1_equity_mark_source(
        ledger_name="ACTUAL",
        session_date=_SESSION,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    canonical = risk_module._issue_phase1_equity_point_from_source(
        canonical_source,
        calendar_resolver=_calendar(),
    )
    actual = risk_module._issue_phase1_equity_point_from_source(
        actual_source,
        calendar_resolver=_calendar(),
    )
    journal.record_phase1_session_mark(
        canonical_authority=canonical,
        actual_authority=actual,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    return shadow_source.signal_id, query_cutoff


def _record_cash_only_session_mark(
    journal: Journal,
    *,
    session_date: date,
    query_cutoff: datetime,
) -> None:
    canonical_source = journal.read_phase1_equity_mark_source(
        ledger_name="CANONICAL",
        session_date=session_date,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    actual_source = journal.read_phase1_equity_mark_source(
        ledger_name="ACTUAL",
        session_date=session_date,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    canonical = risk_module._issue_phase1_equity_point_from_source(
        canonical_source,
        calendar_resolver=_calendar(),
    )
    actual = risk_module._issue_phase1_equity_point_from_source(
        actual_source,
        calendar_resolver=_calendar(),
    )
    journal.record_phase1_session_mark(
        canonical_authority=canonical,
        actual_authority=actual,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )


def _seed_session_closed_primary(
    journal: Journal,
    *,
    session_date: date,
    sequence: int,
) -> tuple[str, datetime]:
    signal_source = _publish_session_primary(
        journal,
        session_date=session_date,
        sequence=sequence,
    )
    trigger_id, quote_id, completed_at = _append_completed_entry_observations(
        journal,
        signal_source,
        session_date=session_date,
    )
    entry = journal.record_phase1_entry(
        signal_source.signal_id,
        confirmation_action_source=None,
        trigger_observation_id=trigger_id,
        quote_observation_id=quote_id,
        calendar_resolver=_calendar(),
        recorded_at=completed_at,
    )
    assert ledger_module.is_issued_paper_entry_authority(entry)
    signal_source = journal._read_phase1_signal_source(
        signal_source.signal_id,
        query_cutoff=completed_at,
    )
    _source, _authority, _stored, exit_cutoff = _record_provider_typed_exit(
        journal,
        signal_source=signal_source,
        session_date=session_date,
        bid=Decimal("21.00"),
        ask=Decimal("21.01"),
        previous_session_low=Decimal("20.80"),
        execution_open=Decimal("21.00"),
        execution_high=Decimal("21.05"),
        execution_low=Decimal("20.95"),
        execution_close=Decimal("21.00"),
        adverse_evidence=True,
        provider_retrieved_at=aware_et(session_date, "16:30"),
    )
    query_cutoff = exit_cutoff + timedelta(minutes=1)
    _record_cash_only_session_mark(
        journal,
        session_date=session_date,
        query_cutoff=query_cutoff,
    )
    stored = journal.record_phase1_adherence(
        signal_source.signal_id,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    assert len(stored.check_ids) == 10
    return signal_source.signal_id, query_cutoff


def _seed_session_not_triggered_primary(
    journal: Journal,
    *,
    session_date: date,
    sequence: int,
) -> tuple[str, datetime]:
    signal_source = _publish_session_primary(
        journal,
        session_date=session_date,
        sequence=sequence,
    )
    trigger = risk_module.money_from_micros(signal_source.trigger_price_micros)
    tick = risk_module.money_from_micros(signal_source.tick_size_micros)
    _append_completed_entry_observations(
        journal,
        signal_source,
        session_date=session_date,
        trade_price=trigger - tick,
    )
    query_cutoff = aware_et(_calendar().add_sessions(session_date, 1), "08:45")
    terminal = journal.record_phase1_unentered_terminal(
        signal_source.signal_id,
        recorded_at=query_cutoff,
        calendar_resolver=_calendar(),
    )
    assert terminal.to_status == "NOT_TRIGGERED"
    _record_cash_only_session_mark(
        journal,
        session_date=session_date,
        query_cutoff=query_cutoff,
    )
    stored = journal.record_phase1_adherence(
        signal_source.signal_id,
        query_cutoff=query_cutoff,
        calendar_resolver=_calendar(),
        policy=policy_fixture(),
    )
    assert len(stored.check_ids) == 10
    return signal_source.signal_id, query_cutoff


def _publication_authority_chain(journal: Journal, *, candidate_count: int):
    replay_source, replay, history_source, history, breaker, _portfolio = (
        _empty_authority_chain(journal)
    )
    candidates = _issued_provider_candidates(candidate_count)
    primary = screening_module.rank_candidates(candidates)[0]
    request = LongPlanRequest.from_scored_candidate(primary)
    policy = policy_fixture()
    portfolio = risk_module._issue_portfolio_risk_authority(
        request=request,
        ledger_pair=replay.ledger_pair,
        ledger_name="CANONICAL",
        breaker_state=breaker,
        calendar_resolver=_calendar(),
        policy=policy,
        scope="CANONICAL_PUBLICATION",
        as_of=aware_et(_SESSION, "08:45"),
        phase1_canonical_replay=replay,
    )
    plan = risk_module.plan_long(
        request,
        portfolio.portfolio_state,
        policy,
        portfolio_authority=portfolio,
    )
    decision = screening_module._issue_portfolio_bound_publication_decision(
        candidates,
        primary_plan_decision=plan,
    )
    return (
        replay_source,
        replay,
        history_source,
        history,
        breaker,
        portfolio,
        candidates,
        plan,
        decision,
    )


def _issued_candidates_with_shared_provider_page():
    contexts = []
    for context in universe_candidate_contexts():
        is_etf = context.record.product_type == "etf"
        bars_by_symbol = {
            symbol: tuple(
                replace(bar, source_observation_id="obs-shared-provider-page")
                for bar in bars
            )
            for symbol, bars in context.bars_by_symbol.items()
        }
        contexts.append(
            replace(
                context,
                bars_by_symbol=bars_by_symbol,
                previous_session_quote=replace(
                    context.previous_session_quote,
                    source_observation_id=(
                        f"previous-sip-quote:{context.record.symbol}"
                    ),
                ),
                latest_iex_quote=replace(
                    context.latest_iex_quote,
                    source_observation_id=(
                        f"latest-iex-quote:{context.record.symbol}"
                    ),
                ),
                evidence=evidence(
                    subject_kind="ETF" if is_etf else "STOCK",
                    symbol=context.record.symbol,
                    issuer_cik=None if is_etf else "0000000000",
                    age_days=5,
                    event_type=(
                        "fund sponsor notice" if is_etf else "material agreement"
                    ),
                    binary_event_coverage=(
                        "NOT_APPLICABLE" if is_etf else "CONFIRMED_CLEAR"
                    ),
                    etf_action_coverage=(
                        "CONFIRMED_CLEAR" if is_etf else "NOT_APPLICABLE"
                    ),
                ),
            )
        )
    cohort = screening_module.build_base_eligible_cohort(
        tuple(contexts),
        universe=TEST_UNIVERSE,
    )
    candidates = tuple(
        screening_module.to_scored_candidate(context)
        for context in cohort.contexts
        if screening_module.score_candidate(context).publishable
        and screening_module.detect_setup(context).eligible
    )
    return screening_module.rank_candidates(candidates)[:2]


class Phase1AuthorityAdapterTests(unittest.TestCase):
    def test_actual_equity_economic_cash_includes_unsettled_sale_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                actions = (
                    (
                        "equity-account-check",
                        "ACCOUNT CHECK settled_cash 4321 pending_orders 0 "
                        "unlogged_positions 0 AT 09:40 ET",
                        "09:40",
                    ),
                    (
                        "equity-buy",
                        "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                        "10:15",
                    ),
                    (
                        "equity-unrelated-position",
                        "RECONCILE UNRELATED POSITION QQQ +2 shares @ 100 "
                        "AT 11:01 ET",
                        "11:01",
                    ),
                    (
                        "equity-sale",
                        "SOLD SPY 1 shares @ 101 AT 15:31 ET",
                        "15:32",
                    ),
                    (
                        "equity-fee",
                        "FEE SPY 0.03 AT 15:33 ET",
                        "15:33",
                    ),
                    (
                        "equity-external-cash",
                        "RECONCILE CASH +100 REASON deposit AT 15:34 ET",
                        "15:34",
                    ),
                )
                for message_id, text, hhmm in actions:
                    message_time = aware_et(_SESSION, hhmm)
                    ingest_confirmation(
                        journal,
                        ConfirmationEnvelope(
                            message_id=message_id,
                            message_time=message_time,
                            received_at=message_time + timedelta(seconds=1),
                            text=text,
                            session_date=_SESSION,
                        ),
                        plans=UnavailableSignalPlanResolver(),
                        calendar=_calendar(),
                        policy=policy_fixture(),
                        entry_authorities=(
                            UnavailableActualEntryAuthorityResolver()
                        ),
                    )
                query_cutoff = aware_et(_SESSION, "15:35")
                with journal.transaction() as transaction:
                    source = transaction.read_actual_replay(
                        query_cutoff=query_cutoff,
                    )
                actual_replay = replay_actual(
                    source,
                    plans=UnavailableSignalPlanResolver(),
                    calendar=_calendar(),
                    policy=policy_fixture(),
                )

                self.assertEqual(
                    actual_replay.strategy_settled_cash_micros,
                    4_799_970_000,
                )
                self.assertEqual(
                    risk_module._phase1_actual_economic_cash_from_verified_replay(
                        source,
                        actual_replay,
                    ),
                    Decimal("4900.970000"),
                )
                strategy_positions = (
                    risk_module._phase1_actual_strategy_positions_from_verified_replay(
                        source,
                        actual_replay,
                    )
                )
                self.assertEqual(
                    tuple(
                        (position.symbol, position.shares)
                        for position in strategy_positions
                    ),
                    (("SPY", 1),),
                )
                self.assertEqual(
                    tuple(
                        (position.symbol, position.lineage_kind)
                        for position in actual_replay.positions
                    ),
                    (
                        ("SPY", "ACTUAL_EVENT"),
                        ("QQQ", "UNRELATED_POSITION"),
                    ),
                )
                self.assertEqual(
                    tuple(
                        (posting.account_name, posting.entry_kind)
                        for posting in source.postings
                    ),
                    (
                        ("ACCOUNT_EVIDENCE", "ACCOUNT_CHECK"),
                        ("SETTLED_CASH", "BUY"),
                        ("ACCOUNT_EVIDENCE", "UNRELATED_POSITION"),
                        ("SETTLED_CASH", "SALE"),
                        ("STRATEGY_FEES", "FEE"),
                        ("ACCOUNT_EVIDENCE", "RECONCILE_CASH"),
                    ),
                )
                for forged_source, forged_state in (
                    (copy.copy(source), actual_replay),
                    (source, copy.copy(actual_replay)),
                    (source, replace(actual_replay)),
                ):
                    with self.subTest(
                        forged_source=forged_source,
                        forged_state=forged_state,
                    ):
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "PHASE1_ACTUAL_ECONOMIC_CASH_SOURCE_MISMATCH",
                        ):
                            risk_module._phase1_actual_economic_cash_from_verified_replay(
                                forged_source,
                                forged_state,
                            )

    def test_equity_mark_source_contract_is_replay_bound_and_moneyless(
        self,
    ) -> None:
        required_journal_surface = (
            "Phase1EquityPositionMarkSource",
            "Phase1EquityMarkSource",
            "is_verified_phase1_equity_mark_source",
        )
        for name in required_journal_surface:
            self.assertTrue(
                hasattr(journal_module, name),
                f"Task 8 equity authority requires Journal {name}",
            )
        for name in (
            "ingest_phase1_equity_mark_cohorts",
            "read_phase1_equity_mark_source",
            "record_phase1_session_mark",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 equity authority requires Journal.{name}",
            )

        source_fields = set(
            journal_module.Phase1EquityMarkSource.__dataclass_fields__
        )
        self.assertTrue(
            {
                "validation_window_id",
                "ledger_name",
                "session_date",
                "point_at",
                "query_cutoff",
                "calendar_digest",
                "canonical_replay_source",
                "canonical_replay",
                "actual_replay_source",
                "actual_replay",
                "position_marks",
                "expected_position_count",
                "expected_mark_count",
                "mark_terminal_cursor",
                "mark_source_highwater",
                "row_references",
                "source_digest",
            }.issubset(source_fields)
        )
        self.assertTrue(
            {
                "symbol",
                "method",
                "quote_cohort",
                "daily_bar_cohort",
                "quote_facts",
                "daily_bar_facts",
                "selected_observation",
                "selected_provider_fact_source",
                "derived_price_micros",
                "mark_at",
                "mark_ordinal",
                "source_cursor",
                "quote_terminal_cursor",
                "quote_source_highwater",
                "daily_bar_terminal_cursor",
                "daily_bar_source_highwater",
                "expected_quote_fact_count",
                "expected_daily_bar_fact_count",
                "row_references",
                "source_digest",
            }.issubset(
                set(
                    journal_module.Phase1EquityPositionMarkSource.__dataclass_fields__
                )
            )
        )
        forbidden_caller_economics = {
            "cash",
            "cash_micros",
            "shares",
            "positions",
            "positions_value",
            "positions_value_micros",
            "equity",
            "equity_micros",
            "external_cash_flow",
            "external_cash_flow_micros",
        }
        self.assertFalse(source_fields & forbidden_caller_economics)

        issuer_parameters = inspect.signature(
            risk_module._issue_phase1_equity_point_from_source
        ).parameters
        self.assertEqual(
            tuple(issuer_parameters),
            ("source", "calendar_resolver"),
        )
        writer_parameters = inspect.signature(
            Journal.record_phase1_session_mark
        ).parameters
        self.assertEqual(
            tuple(writer_parameters),
            (
                "self",
                "canonical_authority",
                "actual_authority",
                "recorded_at",
                "calendar_resolver",
            ),
        )
        self.assertFalse(
            set(writer_parameters) & forbidden_caller_economics
        )

    def test_journal_provider_reissuers_require_registered_source_only(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(alpaca_module, "is_ingestible_provider_fetch_cohort"),
            "provider cohorts need a live-fetch versus replay-only scope",
        )
        for name in (
            "_phase1_reissue_exit_provider_cohorts",
            "_phase1_reissue_equity_provider_cohorts",
        ):
            helper = getattr(journal_module, name)
            self.assertEqual(
                tuple(inspect.signature(helper).parameters),
                ("source",),
                f"{name} must not accept caller pages, URLs, symbols, or windows",
            )
            with self.assertRaises(
                (
                    journal_module.InvalidJournalValue,
                    TypeError,
                    ValueError,
                )
            ):
                helper(SimpleNamespace())

    def test_journal_reissued_provider_cohorts_are_replay_only_for_ingest(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                (
                    equity_source,
                    _actual_source,
                    _canonical,
                    _actual,
                    _equity_cutoff,
                ) = _phase1_equity_authorities(first_journal)
                equity_mark = equity_source.position_marks[0]
                equity_cohorts = (
                    equity_mark.quote_cohort,
                    equity_mark.daily_bar_cohort,
                )

                exit_session = date(2026, 8, 17)
                exit_cutoff = aware_et(exit_session, "15:59") + timedelta(
                    seconds=59
                )
                signal_source = first_journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=exit_cutoff,
                )
                exit_source, _query_cutoff = _read_provider_exit_review_source(
                    first_journal,
                    signal_source=signal_source,
                    session_date=exit_session,
                    bid=Decimal("21.49"),
                    ask=Decimal("21.50"),
                    previous_session_low=Decimal("21.00"),
                    execution_open=Decimal("21.40"),
                    execution_high=Decimal("21.50"),
                    execution_low=Decimal("21.30"),
                    execution_close=Decimal("21.49"),
                    adverse_evidence=False,
                )
                exit_cohorts = (
                    exit_source.daily_bar_cohort,
                    exit_source.execution_bar_cohort,
                    exit_source.quote_cohort,
                )
                for cohort in (*equity_cohorts, *exit_cohorts):
                    with self.subTest(cohort=cohort):
                        self.assertTrue(
                            alpaca_module.is_issued_provider_fetch_cohort(cohort)
                        )
                        self.assertFalse(
                            alpaca_module.is_ingestible_provider_fetch_cohort(
                                cohort
                            )
                        )

                for journal in (first_journal, second_journal):
                    with self.subTest(journal=journal, ingest="equity"):
                        with self.assertRaisesRegex(
                            journal_module.InvalidJournalValue,
                            "live-fetch provider cohorts",
                        ):
                            journal.ingest_phase1_equity_mark_cohorts(
                                _SESSION,
                                quote_cohort=equity_cohorts[0],
                                daily_bar_cohort=equity_cohorts[1],
                                core_source_row_ids=(),
                                calendar_resolver=_calendar(),
                            )
                    with self.subTest(journal=journal, ingest="exit"):
                        with self.assertRaisesRegex(
                            journal_module.InvalidJournalValue,
                            "live-fetch provider cohorts",
                        ):
                            journal.ingest_phase1_exit_review_cohorts(
                                _signal().signal_id,
                                daily_bar_cohort=exit_cohorts[0],
                                execution_bar_cohort=exit_cohorts[1],
                                quote_cohort=exit_cohorts[2],
                                core_source_row_ids=(),
                                calendar_resolver=_calendar(),
                            )
                    with self.subTest(journal=journal, ingest="lifecycle"):
                        with self.assertRaisesRegex(
                            journal_module.InvalidJournalValue,
                            "live-fetch provider cohorts",
                        ):
                            journal.ingest_phase1_session_cohorts(
                                _signal().signal_id,
                                (exit_cohorts[1], exit_cohorts[2]),
                                core_source_row_ids=(),
                                calendar_resolver=_calendar(),
                            )

    def test_equity_authority_rejects_field_only_and_unverified_sources(
        self,
    ) -> None:
        diagnostic = phase1_module.mark_equity(
            Decimal("5000"),
            (),
            {},
            ledger_name="CANONICAL",
            at=aware_et(date(2026, 8, 17), "16:00"),
        )
        self.assertFalse(
            risk_module.is_issued_phase1_equity_point_authority(diagnostic)
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
        ):
            risk_module._issue_phase1_equity_point_from_source(
                SimpleNamespace(
                    ledger_name="CANONICAL",
                    cash_micros=5_000_000_000,
                    equity_micros=5_000_000_000,
                ),
                calendar_resolver=_calendar(),
            )

    def test_equity_mark_ingest_requires_same_owner_complete_quote_and_exact_retry(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "ingest_phase1_equity_mark_cohorts"),
            "Task 8 equity authority requires typed provider ingestion",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                daily, _execution, quotes, transport, _retrieved_at = (
                    _issued_exit_review_cohorts(
                        symbol=_signal().symbol,
                        session_date=_SESSION,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.75"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                    )
                )
                (
                    _other_daily,
                    _other_execution,
                    other_quotes,
                    other_transport,
                    _other_retrieved_at,
                ) = _issued_exit_review_cohorts(
                    symbol=_signal().symbol,
                    session_date=_SESSION,
                    bid=Decimal("20.99"),
                    ask=Decimal("21.00"),
                    previous_session_low=Decimal("20.75"),
                    execution_open=Decimal("21.00"),
                    execution_high=Decimal("21.05"),
                    execution_low=Decimal("20.95"),
                    execution_close=Decimal("21.00"),
                )
                daily_rows = _pin_provider_cohort_pages(
                    journal,
                    daily,
                    transport,
                )
                quote_rows = _pin_provider_cohort_pages(
                    journal,
                    quotes,
                    transport,
                )
                other_quote_rows = _pin_provider_cohort_pages(
                    journal,
                    other_quotes,
                    other_transport,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "owner|cohort",
                ):
                    journal.ingest_phase1_equity_mark_cohorts(
                        _SESSION,
                        quote_cohort=other_quotes,
                        daily_bar_cohort=daily,
                        core_source_row_ids=(
                            *daily_rows,
                            *other_quote_rows,
                        ),
                        calendar_resolver=_calendar(),
                    )

                sealed_at = aware_et(_SESSION, "16:25").astimezone(UTC)
                first = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=quotes,
                    daily_bar_cohort=daily,
                    core_source_row_ids=(*daily_rows, *quote_rows),
                    calendar_resolver=_calendar(),
                    recorded_at=sealed_at,
                )
                retry = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=quotes,
                    daily_bar_cohort=daily,
                    core_source_row_ids=(*daily_rows, *quote_rows),
                    calendar_resolver=_calendar(),
                    recorded_at=sealed_at + timedelta(minutes=1),
                )
                self.assertFalse(first.duplicate)
                self.assertTrue(retry.duplicate)

                invalidated = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=other_quotes,
                    daily_bar_cohort=_other_daily,
                    core_source_row_ids=(
                        *_pin_provider_cohort_pages(
                            journal,
                            _other_daily,
                            other_transport,
                        ),
                        *other_quote_rows,
                    ),
                    calendar_resolver=_calendar(),
                    recorded_at=sealed_at + timedelta(minutes=2),
                )
                self.assertFalse(invalidated.duplicate)
                self.assertTrue(invalidated.invalidated)
                self.assertIsNotNone(invalidated.invalidation_id)
                self.assertEqual(
                    invalidated.invalidated_at,
                    sealed_at + timedelta(minutes=2),
                )

            empty_path = Path(temporary_directory) / "empty-quote.db"
            with Journal.open(empty_path) as journal:
                _start_window(journal)
                daily, _execution, empty_quotes, transport, _retrieved_at = (
                    _issued_exit_review_cohorts(
                        symbol=_signal().symbol,
                        session_date=_SESSION,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.75"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                        emit_quote=False,
                    )
                )
                daily_rows = _pin_provider_cohort_pages(
                    journal,
                    daily,
                    transport,
                )
                quote_rows = _pin_provider_cohort_pages(
                    journal,
                    empty_quotes,
                    transport,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "quote|cohort",
                ):
                    journal.ingest_phase1_equity_mark_cohorts(
                        _SESSION,
                        quote_cohort=None,  # type: ignore[arg-type]
                        daily_bar_cohort=daily,
                        core_source_row_ids=daily_rows,
                        calendar_resolver=_calendar(),
                    )
                stored = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=empty_quotes,
                    daily_bar_cohort=daily,
                    core_source_row_ids=(*daily_rows, *quote_rows),
                    calendar_resolver=_calendar(),
                )
                self.assertFalse(stored.duplicate)
                self.assertEqual(tuple(empty_quotes[_signal().symbol]), ())

    def test_late_equity_mark_evidence_records_stable_invalidation(self) -> None:
        result_fields = set(
            journal_module.Phase1EquityMarkIngestResult.__dataclass_fields__
        )
        self.assertTrue(
            {
                "invalidated",
                "invalidation_id",
                "invalidated_at",
            }.issubset(result_fields),
            "late mark evidence needs a durable append-only invalidation result",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                daily, _execution, quotes, transport, _retrieved_at = (
                    _issued_exit_review_cohorts(
                        symbol=_signal().symbol,
                        session_date=_SESSION,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.75"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                    )
                )
                novel_daily, _novel_execution, novel_quotes, novel_transport, _ = (
                    _issued_exit_review_cohorts(
                        symbol=_signal().symbol,
                        session_date=_SESSION,
                        bid=Decimal("20.99"),
                        ask=Decimal("21.00"),
                        previous_session_low=Decimal("20.75"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                    )
                )
                original_rows = (
                    *_pin_provider_cohort_pages(journal, daily, transport),
                    *_pin_provider_cohort_pages(journal, quotes, transport),
                )
                novel_rows = (
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_daily,
                        novel_transport,
                    ),
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_quotes,
                        novel_transport,
                    ),
                )
                sealed_at = aware_et(_SESSION, "16:25").astimezone(UTC)
                retry_at = aware_et(_SESSION, "16:30").astimezone(UTC)
                discovered_at = aware_et(_SESSION, "16:35").astimezone(UTC)
                duplicate_discovery_at = aware_et(
                    _SESSION,
                    "16:40",
                ).astimezone(UTC)
                first = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=quotes,
                    daily_bar_cohort=daily,
                    core_source_row_ids=original_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=sealed_at,
                )
                exact_retry = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=quotes,
                    daily_bar_cohort=daily,
                    core_source_row_ids=original_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=retry_at,
                )
                self.assertFalse(first.duplicate)
                self.assertFalse(first.invalidated)
                self.assertIsNone(first.invalidation_id)
                self.assertIsNone(first.invalidated_at)
                self.assertTrue(exact_retry.duplicate)
                self.assertFalse(exact_retry.invalidated)
                self.assertIsNone(exact_retry.invalidation_id)
                self.assertIsNone(exact_retry.invalidated_at)

                invalidated = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=novel_quotes,
                    daily_bar_cohort=novel_daily,
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=discovered_at,
                )
                self.assertFalse(invalidated.duplicate)
                self.assertTrue(invalidated.invalidated)
                self.assertIsNotNone(invalidated.invalidation_id)
                self.assertEqual(invalidated.invalidated_at, discovered_at)

                duplicate_invalidation = (
                    journal.ingest_phase1_equity_mark_cohorts(
                        _SESSION,
                        quote_cohort=novel_quotes,
                        daily_bar_cohort=novel_daily,
                        core_source_row_ids=novel_rows,
                        calendar_resolver=_calendar(),
                        recorded_at=duplicate_discovery_at,
                    )
                )
                self.assertTrue(duplicate_invalidation.duplicate)
                self.assertTrue(duplicate_invalidation.invalidated)
                self.assertEqual(
                    duplicate_invalidation.invalidation_id,
                    invalidated.invalidation_id,
                )
                self.assertEqual(
                    duplicate_invalidation.invalidated_at,
                    invalidated.invalidated_at,
                )

            with Journal.open(path) as journal:
                restarted_retry = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=novel_quotes,
                    daily_bar_cohort=novel_daily,
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=duplicate_discovery_at + timedelta(minutes=5),
                )
                self.assertTrue(restarted_retry.duplicate)
                self.assertTrue(restarted_retry.invalidated)
                self.assertEqual(
                    restarted_retry.invalidation_id,
                    invalidated.invalidation_id,
                )
                self.assertEqual(
                    restarted_retry.invalidated_at,
                    invalidated.invalidated_at,
                )

    def test_late_equity_mark_invalidation_preserves_historical_source_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                (
                    original_source,
                    _actual_source,
                    original_authority,
                    _actual_authority,
                    historical_cutoff,
                ) = _phase1_equity_authorities(journal)
                (
                    novel_daily,
                    _novel_execution,
                    novel_quotes,
                    novel_transport,
                    _novel_retrieved_at,
                ) = _issued_exit_review_cohorts(
                    symbol=_signal().symbol,
                    session_date=_SESSION,
                    bid=Decimal("20.99"),
                    ask=Decimal("21.00"),
                    previous_session_low=Decimal("20.75"),
                    execution_open=Decimal("21.00"),
                    execution_high=Decimal("21.05"),
                    execution_low=Decimal("20.95"),
                    execution_close=Decimal("21.00"),
                )
                novel_rows = (
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_daily,
                        novel_transport,
                    ),
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_quotes,
                        novel_transport,
                    ),
                )
                pre_invalidation_source = (
                    journal.read_phase1_equity_mark_source(
                        ledger_name="CANONICAL",
                        session_date=_SESSION,
                        query_cutoff=historical_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                pre_invalidation_authority = (
                    risk_module._issue_phase1_equity_point_from_source(
                        pre_invalidation_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_equity_mark_source(
                        pre_invalidation_source
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        pre_invalidation_authority
                    )
                )
                invalidated_at = historical_cutoff + timedelta(minutes=5)
                result = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=novel_quotes,
                    daily_bar_cohort=novel_daily,
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=invalidated_at,
                )
                self.assertTrue(result.invalidated)
                self.assertEqual(result.invalidated_at, invalidated_at)
                self.assertFalse(
                    journal_module.is_verified_phase1_equity_mark_source(
                        pre_invalidation_source
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_equity_point_authority(
                        pre_invalidation_authority
                    )
                )

                historical_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=historical_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                historical_authority = (
                    risk_module._issue_phase1_equity_point_from_source(
                        historical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertEqual(
                    historical_authority.point,
                    original_authority.point,
                )
                self.assertEqual(
                    historical_authority.authority_digest,
                    original_authority.authority_digest,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "PHASE1_EQUITY_MARK_LATE_EVIDENCE",
                ):
                    journal.read_phase1_equity_mark_source(
                        ledger_name="CANONICAL",
                        session_date=_SESSION,
                        query_cutoff=invalidated_at,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

            self.assertFalse(
                journal_module.is_verified_phase1_equity_mark_source(
                    original_source
                )
            )
            self.assertFalse(
                risk_module.is_issued_phase1_equity_point_authority(
                    original_authority
                )
            )
            with Journal.open(path) as journal:
                restarted_historical_source = (
                    journal.read_phase1_equity_mark_source(
                        ledger_name="CANONICAL",
                        session_date=_SESSION,
                        query_cutoff=historical_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                restarted_historical_authority = (
                    risk_module._issue_phase1_equity_point_from_source(
                        restarted_historical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNot(
                    restarted_historical_authority,
                    historical_authority,
                )
                self.assertEqual(
                    restarted_historical_authority.point,
                    historical_authority.point,
                )
                self.assertEqual(
                    restarted_historical_authority.authority_digest,
                    historical_authority.authority_digest,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "PHASE1_EQUITY_MARK_LATE_EVIDENCE",
                ):
                    journal.read_phase1_equity_mark_source(
                        ledger_name="CANONICAL",
                        session_date=_SESSION,
                        query_cutoff=invalidated_at,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

    def test_late_equity_mark_invalidation_revokes_current_breaker_on_restart(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                (
                    _canonical_source,
                    _actual_source,
                    canonical,
                    actual,
                    historical_cutoff,
                ) = _phase1_equity_authorities(journal)
                journal.record_phase1_session_mark(
                    canonical_authority=canonical,
                    actual_authority=actual,
                    recorded_at=historical_cutoff,
                    calendar_resolver=_calendar(),
                )
                (
                    novel_daily,
                    _novel_execution,
                    novel_quotes,
                    novel_transport,
                    _novel_retrieved_at,
                ) = _issued_exit_review_cohorts(
                    symbol=_signal().symbol,
                    session_date=_SESSION,
                    bid=Decimal("20.99"),
                    ask=Decimal("21.00"),
                    previous_session_low=Decimal("20.75"),
                    execution_open=Decimal("21.00"),
                    execution_high=Decimal("21.05"),
                    execution_low=Decimal("20.95"),
                    execution_close=Decimal("21.00"),
                )
                novel_rows = (
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_daily,
                        novel_transport,
                    ),
                    *_pin_provider_cohort_pages(
                        journal,
                        novel_quotes,
                        novel_transport,
                    ),
                )
                historical_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=historical_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                historical = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        historical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                invalidated_at = historical_cutoff + timedelta(minutes=5)
                result = journal.ingest_phase1_equity_mark_cohorts(
                    _SESSION,
                    quote_cohort=novel_quotes,
                    daily_bar_cohort=novel_daily,
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=invalidated_at,
                )
                self.assertTrue(result.invalidated)
                self.assertFalse(
                    journal_module.is_verified_phase1_breaker_history_source(
                        historical_source
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_breaker_history_authority(historical)
                )

                reissued_historical_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=historical_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                reissued_historical = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        reissued_historical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNot(reissued_historical, historical)
                self.assertEqual(reissued_historical, historical)
                self.assertEqual(
                    reissued_historical.source_digest,
                    historical.source_digest,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "PHASE1_EQUITY_MARK_LATE_EVIDENCE",
                ):
                    journal.read_phase1_breaker_history(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=invalidated_at,
                        calendar_resolver=_calendar(),
                    )

            self.assertFalse(
                journal_module.is_verified_phase1_breaker_history_source(
                    reissued_historical_source
                )
            )
            self.assertFalse(
                risk_module.is_issued_breaker_history_authority(
                    reissued_historical
                )
            )
            with Journal.open(path) as journal:
                restarted_historical_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=historical_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                restarted_historical = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        restarted_historical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNot(restarted_historical, reissued_historical)
                self.assertEqual(restarted_historical, historical)
                self.assertEqual(
                    restarted_historical.source_digest,
                    historical.source_digest,
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "PHASE1_EQUITY_MARK_LATE_EVIDENCE",
                ):
                    journal.read_phase1_breaker_history(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=invalidated_at,
                        calendar_resolver=_calendar(),
                    )

    def test_equity_paired_writer_rolls_back_second_ledger_insert(self) -> None:
        self.assertTrue(
            hasattr(Journal, "record_phase1_session_mark"),
            "Task 8 equity points must be persisted as one paired write",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                (
                    _canonical_source,
                    _actual_source,
                    canonical,
                    actual,
                    query_cutoff,
                ) = _phase1_equity_authorities(journal)
                before = (
                    journal.count("phase1_equity_points"),
                    journal.count("phase1_equity_point_marks"),
                )
                journal._connection.execute(
                    "CREATE TEMP TRIGGER fail_actual_equity_point "
                    "BEFORE INSERT ON phase1_equity_points "
                    "WHEN NEW.ledger_name = 'ACTUAL' "
                    "BEGIN SELECT RAISE(ABORT, "
                    "'injected second-ledger equity failure'); END"
                )
                with self.assertRaises(journal_module.JournalError) as raised:
                    journal.record_phase1_session_mark(
                        canonical_authority=canonical,
                        actual_authority=actual,
                        recorded_at=query_cutoff,
                        calendar_resolver=_calendar(),
                    )
                causes: list[str] = []
                error: BaseException | None = raised.exception
                while error is not None:
                    causes.append(str(error))
                    error = error.__cause__
                self.assertIn(
                    "injected second-ledger equity failure",
                    " | ".join(causes),
                )
                self.assertEqual(
                    (
                        journal.count("phase1_equity_points"),
                        journal.count("phase1_equity_point_marks"),
                    ),
                    before,
                )
                self.assertEqual(
                    journal._connection.execute(
                        "SELECT COUNT(*) FROM phase1_equity_points "
                        "WHERE session_date = ? AND source_cursor > 1",
                        (_SESSION.isoformat(),),
                    ).fetchone()[0],
                    0,
                )

    def test_idle_session_issues_and_pair_persists_replay_only_equity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            query_cutoff = aware_et(_SESSION, "16:01")
            with Journal.open(path) as journal:
                _start_window(journal)
                canonical_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                actual_source = journal.read_phase1_equity_mark_source(
                    ledger_name="ACTUAL",
                    session_date=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(canonical_source.position_marks, ())
                self.assertEqual(actual_source.position_marks, ())
                self.assertEqual(canonical_source.expected_position_count, 0)
                self.assertEqual(actual_source.expected_position_count, 0)
                self.assertEqual(canonical_source.expected_mark_count, 0)
                self.assertEqual(actual_source.expected_mark_count, 0)

                canonical = risk_module._issue_phase1_equity_point_from_source(
                    canonical_source,
                    calendar_resolver=_calendar(),
                )
                actual = risk_module._issue_phase1_equity_point_from_source(
                    actual_source,
                    calendar_resolver=_calendar(),
                )
                for authority in (canonical, actual):
                    with self.subTest(ledger_name=authority.ledger_name):
                        self.assertEqual(
                            authority.point.cash,
                            Decimal("5000.000000"),
                        )
                        self.assertEqual(
                            authority.point.positions_value,
                            Decimal("0.000000"),
                        )
                        self.assertEqual(
                            authority.point.equity,
                            Decimal("5000.000000"),
                        )
                        self.assertEqual(authority.point.mark_sources, ())
                        self.assertTrue(
                            risk_module.is_issued_phase1_equity_point_authority(
                                authority
                            )
                        )
                journal.record_phase1_session_mark(
                    canonical_authority=canonical,
                    actual_authority=actual,
                    recorded_at=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(journal.count("phase1_equity_points"), 4)
                self.assertEqual(journal.count("phase1_equity_point_marks"), 0)

            self.assertFalse(
                risk_module.is_issued_phase1_equity_point_authority(canonical)
            )
            self.assertFalse(
                risk_module.is_issued_phase1_equity_point_authority(actual)
            )
            with Journal.open(path) as journal:
                for ledger_name in ("CANONICAL", "ACTUAL"):
                    with self.subTest(restarted_ledger=ledger_name):
                        source = journal.read_phase1_equity_mark_source(
                            ledger_name=ledger_name,
                            session_date=_SESSION,
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                            policy=policy_fixture(),
                        )
                        restarted = (
                            risk_module._issue_phase1_equity_point_from_source(
                                source,
                                calendar_resolver=_calendar(),
                            )
                        )
                        self.assertEqual(
                            restarted.point.equity,
                            Decimal("5000.000000"),
                        )
                        self.assertEqual(restarted.point.mark_sources, ())

    def test_actual_equity_marks_off_policy_strategy_and_unsettled_sale(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                (
                    _canonical_source,
                    actual_source,
                    _canonical,
                    actual,
                    _query_cutoff,
                ) = _phase1_equity_authorities(
                    journal,
                    actual_strategy_activity=True,
                )
                self.assertEqual(actual_source.expected_position_count, 1)
                self.assertEqual(actual_source.expected_mark_count, 1)
                self.assertEqual(
                    tuple(
                        position.lineage_kind
                        for position in actual_source.actual_replay.positions
                    ),
                    ("ACTUAL_EVENT",),
                )
                self.assertEqual(
                    actual_source.actual_replay.strategy_settled_cash_micros,
                    4_959_970_000,
                )
                self.assertEqual(actual.point.cash, Decimal("4980.970000"))
                self.assertEqual(
                    actual.point.positions_value,
                    Decimal("21.000000"),
                )
                self.assertEqual(actual.point.equity, Decimal("5001.970000"))
                self.assertEqual(
                    actual.point.mark_sources,
                    ((_signal().symbol, "CONSOLIDATED_BID"),),
                )

    def test_equity_authority_prefers_sip_bid_then_completed_daily_close(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "ingest_phase1_equity_mark_cohorts"),
            "Task 8 equity authority requires provider-backed mark ingestion",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            quote_path = Path(temporary_directory) / "quote.db"
            fallback_path = Path(temporary_directory) / "fallback.db"
            with Journal.open(quote_path) as journal:
                (
                    canonical_source,
                    _actual_source,
                    canonical,
                    actual,
                    query_cutoff,
                ) = _phase1_equity_authorities(journal)
                selected_mark = canonical_source.position_marks[0]
                self.assertEqual(selected_mark.method, "CONSOLIDATED_BID")
                self.assertTrue(selected_mark.quote_facts)
                self.assertTrue(selected_mark.daily_bar_facts)
                self.assertTrue(
                    any(
                        item is selected_mark.selected_observation
                        for item in selected_mark.quote_facts
                    )
                )
                self.assertTrue(
                    alpaca_module.provider_fetch_cohorts_share_owner(
                        selected_mark.quote_cohort,
                        selected_mark.daily_bar_cohort,
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        canonical
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(actual)
                )
                self.assertEqual(canonical.point.cash, Decimal("4039.790000"))
                self.assertEqual(
                    canonical.point.positions_value,
                    Decimal("987.000000"),
                )
                self.assertEqual(
                    canonical.point.equity,
                    Decimal("5026.790000"),
                )
                self.assertEqual(
                    canonical.point.mark_sources,
                    (("AAPL", "CONSOLIDATED_BID"),),
                )
                self.assertEqual(actual.point.cash, Decimal("5000.000000"))
                self.assertEqual(actual.point.positions_value, Decimal("0.000000"))
                self.assertEqual(actual.point.equity, Decimal("5000.000000"))
                self.assertEqual(actual.point.mark_sources, ())
                journal.record_phase1_session_mark(
                    canonical_authority=canonical,
                    actual_authority=actual,
                    recorded_at=query_cutoff,
                    calendar_resolver=_calendar(),
                )

            with Journal.open(fallback_path) as journal:
                canonical_source, _actual_source, canonical, actual, cutoff = (
                    _phase1_equity_authorities(
                        journal,
                        emit_quote=False,
                    )
                )
                selected_mark = canonical_source.position_marks[0]
                self.assertEqual(
                    selected_mark.method,
                    "CLOSE_MINUS_0.10_PERCENT",
                )
                self.assertEqual(selected_mark.quote_facts, ())
                self.assertTrue(selected_mark.daily_bar_facts)
                self.assertTrue(
                    any(
                        item is selected_mark.selected_observation
                        for item in selected_mark.daily_bar_facts
                    )
                )
                self.assertEqual(
                    canonical.point.positions_value,
                    Decimal("986.013000"),
                )
                self.assertEqual(
                    canonical.point.equity,
                    Decimal("5025.803000"),
                )
                self.assertEqual(
                    canonical.point.mark_sources,
                    (("AAPL", "CLOSE_MINUS_0.10_PERCENT"),),
                )
                journal.record_phase1_session_mark(
                    canonical_authority=canonical,
                    actual_authority=actual,
                    recorded_at=cutoff,
                    calendar_resolver=_calendar(),
                )

    def test_equity_authority_rejects_copy_subset_lookahead_and_mutation(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "read_phase1_equity_mark_source"),
            "Task 8 equity authority requires an owner-current mark reader",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, _actual_source, authority, _actual, _cutoff = (
                    _phase1_equity_authorities(journal)
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_equity_point_authority(
                        copy.copy(authority)
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_equity_point_authority(
                        replace(authority)
                    )
                )
                bad_sources = (
                    copy.copy(source),
                    replace(source),
                    replace(source, position_marks=()),
                    replace(
                        source,
                        position_marks=(
                            *source.position_marks,
                            source.position_marks[0],
                        ),
                    ),
                    replace(
                        source,
                        session_date=_calendar().add_sessions(
                            source.session_date,
                            1,
                        ),
                    ),
                    replace(
                        source,
                        query_cutoff=source.point_at - timedelta(seconds=1),
                    ),
                )
                for bad_source in bad_sources:
                    with self.subTest(bad_source=bad_source):
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                        ):
                            risk_module._issue_phase1_equity_point_from_source(
                                bad_source,
                                calendar_resolver=_calendar(),
                            )

                mutation_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=source.query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                object.__setattr__(
                    mutation_source.position_marks[0],
                    "derived_price_micros",
                    mutation_source.position_marks[0].derived_price_micros + 1,
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                ):
                    risk_module._issue_phase1_equity_point_from_source(
                        mutation_source,
                        calendar_resolver=_calendar(),
                    )

                nested_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=source.query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                object.__setattr__(
                    nested_source.position_marks[0].selected_observation,
                    "bid",
                    nested_source.position_marks[0].selected_observation.bid
                    + Decimal("0.01"),
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                ):
                    risk_module._issue_phase1_equity_point_from_source(
                        nested_source,
                        calendar_resolver=_calendar(),
                    )

                stale_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=source.query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                stale_authority = (
                    risk_module._issue_phase1_equity_point_from_source(
                        stale_source,
                        calendar_resolver=_calendar(),
                    )
                )
                _append_unrelated_source(journal, suffix="equity-stale")
                self.assertFalse(
                    risk_module.is_issued_phase1_equity_point_authority(
                        stale_authority
                    )
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                ):
                    risk_module._issue_phase1_equity_point_from_source(
                        stale_source,
                        calendar_resolver=_calendar(),
                    )

    def test_equity_authority_rejects_cross_journal_and_reissues_on_restart(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "record_phase1_session_mark"),
            "Task 8 equity authority requires typed point persistence",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                (
                    first_source,
                    first_actual_source,
                    first,
                    first_actual,
                    cutoff,
                ) = _phase1_equity_authorities(first_journal)
                (
                    second_source,
                    second_actual_source,
                    _second,
                    _second_actual,
                    second_cutoff,
                ) = _phase1_equity_authorities(second_journal)
                self.assertEqual(cutoff, second_cutoff)
                cross_journal = replace(
                    first_source,
                    canonical_replay_source=(
                        second_source.canonical_replay_source
                    ),
                    canonical_replay=second_source.canonical_replay,
                )
                cross_provider = replace(
                    first_source,
                    position_marks=second_source.position_marks,
                )
                for forged in (cross_journal, cross_provider):
                    with self.subTest(forged=forged):
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                        ):
                            risk_module._issue_phase1_equity_point_from_source(
                                forged,
                                calendar_resolver=_calendar(),
                            )
                deep_cross_actual = first_journal.read_phase1_equity_mark_source(
                    ledger_name="ACTUAL",
                    session_date=_SESSION,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(
                    first_actual_source.actual_replay_source,
                    second_actual_source.actual_replay_source,
                    "the adversary uses value-identical replay material",
                )
                object.__setattr__(
                    deep_cross_actual,
                    "actual_replay_source",
                    second_actual_source.actual_replay_source,
                )
                object.__setattr__(
                    deep_cross_actual,
                    "actual_replay",
                    second_actual_source.actual_replay,
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_equity_mark_source(
                        deep_cross_actual
                    )
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED",
                ):
                    risk_module._issue_phase1_equity_point_from_source(
                        deep_cross_actual,
                        calendar_resolver=_calendar(),
                    )
                first_journal.record_phase1_session_mark(
                    canonical_authority=first,
                    actual_authority=first_actual,
                    recorded_at=cutoff,
                    calendar_resolver=_calendar(),
                )

            self.assertFalse(
                risk_module.is_issued_phase1_equity_point_authority(first)
            )
            self.assertFalse(
                risk_module.is_issued_phase1_equity_point_authority(
                    first_actual
                )
            )
            with Journal.open(first_path) as journal:
                second_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=_SESSION,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                second_actual_source = journal.read_phase1_equity_mark_source(
                    ledger_name="ACTUAL",
                    session_date=_SESSION,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                restarted = risk_module._issue_phase1_equity_point_from_source(
                    second_source,
                    calendar_resolver=_calendar(),
                )
                restarted_actual = (
                    risk_module._issue_phase1_equity_point_from_source(
                        second_actual_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNot(first, restarted)
                self.assertIsNot(first_actual, restarted_actual)
                self.assertEqual(first.point, restarted.point)
                self.assertEqual(first_actual.point, restarted_actual.point)
                self.assertEqual(first.authority_digest, restarted.authority_digest)
                self.assertEqual(
                    first_actual.authority_digest,
                    restarted_actual.authority_digest,
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        restarted
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        restarted_actual
                    )
                )

    def test_breaker_history_rederives_every_nonbaseline_equity_point(
        self,
    ) -> None:
        breaker_fields = set(
            journal_module.Phase1BreakerHistorySource.__dataclass_fields__
        )
        self.assertTrue(
            {
                "equity_mark_sources",
                "equity_authorities",
                "expected_mark_source_count",
                "mark_source_highwater",
            }.issubset(breaker_fields),
            "breaker history must retain the exact replay/mark authority "
            "behind every nonbaseline equity point",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                (
                    _canonical_source,
                    _actual_source,
                    canonical,
                    actual,
                    query_cutoff,
                ) = _phase1_equity_authorities(journal)
                journal.record_phase1_session_mark(
                    canonical_authority=canonical,
                    actual_authority=actual,
                    recorded_at=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                canonical_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=query_cutoff,
                    )
                )
                actual_source = journal._read_phase1_breaker_history_source(
                    ledger_name="ACTUAL",
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                )
                self.assertEqual(canonical_source.expected_equity_count, 2)
                self.assertEqual(canonical_source.expected_mark_source_count, 1)
                self.assertEqual(len(canonical_source.equity_mark_sources), 1)
                self.assertEqual(len(canonical_source.equity_authorities), 1)
                self.assertEqual(
                    canonical_source.equity_mark_sources[0].query_cutoff,
                    canonical_source.equity_points[1].received_at,
                )
                self.assertEqual(
                    canonical_source.equity_authorities[0].query_cutoff,
                    canonical_source.equity_points[1].received_at,
                )
                self.assertEqual(
                    canonical_source.equity_points[1].point_id,
                    hashlib.sha256(
                        (
                            "stock-monitor/phase1-session-equity-point/v1\x00"
                            + canonical_source.validation_window_id
                            + "\x00CANONICAL\x00"
                            + canonical_source.equity_points[1].session_date.isoformat()
                            + "\x00"
                            + canonical_source.equity_authorities[0].authority_digest
                        ).encode("utf-8")
                    ).hexdigest(),
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        canonical_source.equity_authorities[0]
                    )
                )
                self.assertEqual(actual_source.expected_equity_count, 2)
                self.assertEqual(actual_source.expected_mark_source_count, 1)
                self.assertEqual(
                    actual_source.equity_mark_sources[0].query_cutoff,
                    actual_source.equity_points[1].received_at,
                )
                self.assertEqual(
                    actual_source.equity_authorities[0].query_cutoff,
                    actual_source.equity_points[1].received_at,
                )
                self.assertEqual(
                    actual_source.equity_points[1].point_id,
                    hashlib.sha256(
                        (
                            "stock-monitor/phase1-session-equity-point/v1\x00"
                            + actual_source.validation_window_id
                            + "\x00ACTUAL\x00"
                            + actual_source.equity_points[1].session_date.isoformat()
                            + "\x00"
                            + actual_source.equity_authorities[0].authority_digest
                        ).encode("utf-8")
                    ).hexdigest(),
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_equity_point_authority(
                        actual_source.equity_authorities[0]
                    )
                )

                canonical_history = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        canonical_source,
                        calendar_resolver=_calendar(),
                    )
                )
                actual_history = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        actual_source,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertEqual(
                    canonical_history.equity[-1].equity,
                    canonical.point.equity,
                )
                self.assertEqual(
                    actual_history.equity[-1].equity,
                    actual.point.equity,
                )
                copied_authority_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=query_cutoff,
                    )
                )
                object.__setattr__(
                    copied_authority_source,
                    "equity_authorities",
                    (
                        copy.copy(
                            copied_authority_source.equity_authorities[0]
                        ),
                    ),
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_breaker_history_source(
                        copied_authority_source
                    ),
                    "compare=False derived values require the risk adapter's "
                    "identity check",
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_BREAKER_EQUITY_AUTHORITY_MISMATCH",
                ):
                    risk_module._issue_breaker_history_from_phase1_source(
                        copied_authority_source,
                        calendar_resolver=_calendar(),
                    )

                cross_authority_source = (
                    journal._read_phase1_breaker_history_source(
                        ledger_name="CANONICAL",
                        through_session=_SESSION,
                        query_cutoff=query_cutoff,
                    )
                )
                object.__setattr__(
                    cross_authority_source,
                    "equity_authorities",
                    actual_source.equity_authorities,
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_BREAKER_EQUITY_AUTHORITY_MISMATCH",
                ):
                    risk_module._issue_breaker_history_from_phase1_source(
                        cross_authority_source,
                        calendar_resolver=_calendar(),
                    )

                missing_mark = replace(
                    canonical_source,
                    equity_mark_sources=(),
                    equity_authorities=(),
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_breaker_history_source(
                        missing_mark
                    )
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_BREAKER_HISTORY_SOURCE_UNVERIFIED",
                ):
                    risk_module._issue_breaker_history_from_phase1_source(
                        missing_mark,
                        calendar_resolver=_calendar(),
                    )

    def test_publication_manifest_accepts_one_provider_page_for_many_facts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                chain = _publication_authority_chain(
                    journal,
                    candidate_count=2,
                )
                candidates = chain[-3]
                decision = chain[-1]
                self.assertEqual(len(candidates), 2)
                manifest = screening_module._publication_observation_manifest(
                    decision
                )
                candidate_source_sets = tuple(
                    frozenset(source_ids)
                    for _symbol, source_ids in (
                        manifest.candidate_source_observation_ids
                    )
                )
                shared_ids = set.intersection(
                    *(set(source_ids) for source_ids in candidate_source_sets)
                )

                self.assertTrue(shared_ids)
                for source_id in shared_ids:
                    self.assertEqual(
                        manifest.source_observation_ids.count(source_id),
                        1,
                    )
                    self.assertTrue(
                        all(
                            source_id in source_ids
                            for source_ids in candidate_source_sets
                        )
                    )
                self.assertTrue(
                    any(
                        sum(
                            source.source_observation_id == source_id
                            for _candidate_symbol, source in (
                                manifest.normalized_market_fact_sources
                            )
                        )
                        > 1
                        for source_id in shared_ids
                    )
                )

    def test_report_without_exact_candidate_source_pins_cannot_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                publication_source, decision, plan, _lineage = (
                    _issued_publication(
                        journal,
                        candidate_count=2,
                        pin_mode="empty",
                    )
                )

                self.assertEqual(publication_source.observation_ids, ())
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "observation manifest",
                ):
                    journal.publish_phase1_report(
                        publication_source=publication_source,
                        decision=decision,
                        primary_plan_decision=plan,
                        validation_window_id=_WINDOW_ID,
                        calendar_resolver=_calendar(),
                        received_at=aware_et(_SESSION, "08:45"),
                    )
                self.assertEqual(journal.count("phase1_signals"), 0)

    def test_two_name_replay_authenticates_shadow_but_projects_only_primary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                publication_source, decision, plan, _lineage = (
                    _issued_publication(journal, candidate_count=2)
                )
                stored = journal.publish_phase1_report(
                    publication_source=publication_source,
                    decision=decision,
                    primary_plan_decision=plan,
                    validation_window_id=_WINDOW_ID,
                    calendar_resolver=_calendar(),
                    received_at=aware_et(_SESSION, "08:45"),
                )
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )

                self.assertEqual(len(stored.signal_ids), 2)
                self.assertEqual(
                    tuple(source.role for source in replay_source.signal_sources),
                    ("PRIMARY", "WATCHLIST_SHADOW"),
                )
                self.assertEqual(
                    tuple(
                        source.planned_shares
                        for source in replay_source.signal_sources
                    ),
                    (plan.plan.quantity, 0),
                )
                replay = (
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                        replay_source
                    )
                )
                self.assertEqual(
                    tuple(signal.role for signal in replay.ledger_pair.signals),
                    ("PRIMARY",),
                )
                self.assertEqual(replay.ledger_pair.canonical.open_positions, ())
                shadow_source = replay_source.signal_sources[1]
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_SIGNAL_NOT_TRADABLE",
                ):
                    ledger_module._issue_ledger_signal_from_phase1_source(
                        shadow_source
                    )

    def test_two_name_replay_reissues_new_identity_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            cutoff = aware_et(_SESSION, "08:45")
            with Journal.open(path) as first_journal:
                _start_window(first_journal)
                publication_source, decision, plan, _lineage = (
                    _issued_publication(first_journal, candidate_count=2)
                )
                first_journal.publish_phase1_report(
                    publication_source=publication_source,
                    decision=decision,
                    primary_plan_decision=plan,
                    validation_window_id=_WINDOW_ID,
                    calendar_resolver=_calendar(),
                    received_at=cutoff,
                )
                first_source = (
                    first_journal._read_phase1_canonical_replay_source(
                        query_cutoff=cutoff,
                    )
                )
                first_replay = (
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                        first_source
                    )
                )
                first_primary = first_replay.ledger_pair.signals[0]

            self.assertFalse(
                journal_module.is_verified_phase1_canonical_replay_source(
                    first_source
                )
            )
            self.assertFalse(first_replay.source_verified)
            self.assertFalse(
                ledger_module.is_issued_verified_replay_cohort(
                    first_replay.cohort
                )
            )
            self.assertFalse(first_replay.ledger_pair.canonical_replay_verified)
            self.assertFalse(ledger_module.is_issued_ledger_signal(first_primary))

            with Journal.open(path) as second_journal:
                second_source = (
                    second_journal._read_phase1_canonical_replay_source(
                        query_cutoff=cutoff,
                    )
                )
                second_replay = (
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                        second_source
                    )
                )
                second_primary = second_replay.ledger_pair.signals[0]

                for first, second in (
                    (first_source, second_source),
                    (first_replay, second_replay),
                    (first_replay.cohort, second_replay.cohort),
                    (first_replay.ledger_pair, second_replay.ledger_pair),
                    (first_primary, second_primary),
                ):
                    self.assertIsNot(first, second)
                self.assertEqual(first_source, second_source)
                self.assertEqual(first_replay.cohort, second_replay.cohort)
                self.assertEqual(
                    first_replay.source_digest,
                    second_replay.source_digest,
                )
                self.assertEqual(
                    first_replay.canonical_cash,
                    second_replay.canonical_cash,
                )
                self.assertEqual(
                    first_replay.settled_buying_power,
                    second_replay.settled_buying_power,
                )
                self.assertEqual(
                    first_replay.ledger_pair.canonical,
                    second_replay.ledger_pair.canonical,
                )
                self.assertEqual(first_primary, second_primary)
                self.assertTrue(second_replay.source_verified)
                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(
                        second_replay.cohort
                    )
                )
                self.assertTrue(
                    second_replay.ledger_pair.canonical_replay_verified
                )
                self.assertTrue(
                    ledger_module.is_issued_ledger_signal(second_primary)
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_canonical_replay_source(
                        copy.copy(second_source)
                    )
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_canonical_replay_source(
                        replace(second_source)
                    )
                )

    def test_publication_decision_retains_complete_candidate_source_manifest(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(screening_module, "_publication_observation_manifest"),
            "publication authority must retain exact CandidateContext sources",
        )
        for candidate_count in (1, 2):
            with self.subTest(candidate_count=candidate_count):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _start_window(journal)
                        chain = _publication_authority_chain(
                            journal,
                            candidate_count=candidate_count,
                        )
                        candidates, _plan, decision = chain[-3:]
                        manifest = (
                            screening_module._publication_observation_manifest(
                                decision
                            )
                        )

                        self.assertEqual(
                            tuple(
                                symbol
                                for symbol, _source_ids in (
                                    manifest.candidate_source_observation_ids
                                )
                            ),
                            tuple(
                                candidate.symbol
                                for candidate in screening_module.rank_candidates(
                                    candidates
                                )
                            ),
                        )
                        self.assertEqual(
                            manifest.source_observation_ids,
                            tuple(
                                sorted(
                                    {
                                        source_id
                                        for _symbol, source_ids in (
                                            manifest.candidate_source_observation_ids
                                        )
                                        for source_id in source_ids
                                    }
                                )
                            ),
                        )
                        self.assertTrue(manifest.source_observation_ids)
                        self.assertEqual(
                            len(manifest.candidate_context_digests),
                            candidate_count,
                        )
                        self.assertEqual(
                            manifest.candidate_subjects,
                            tuple(
                                (
                                    candidate.symbol,
                                    (
                                        "ETF"
                                        if candidate.symbol in {"QQQ", "XLK"}
                                        else "STOCK"
                                    ),
                                    (
                                        None
                                        if candidate.symbol in {"QQQ", "XLK"}
                                        else "0000000000"
                                    ),
                                )
                                for candidate in screening_module.rank_candidates(
                                    candidates
                                )
                            ),
                        )
                        self.assertTrue(
                            manifest.normalized_market_fact_sources
                        )
                        self.assertTrue(manifest.provider_fetch_manifests)
                        self.assertTrue(
                            all(
                                source.fetch_manifest in (
                                    manifest.provider_fetch_manifests
                                )
                                for _candidate_symbol, source in (
                                    manifest.normalized_market_fact_sources
                                )
                            )
                        )
                        self.assertEqual(len(manifest.manifest_digest), 64)
                        for candidate in candidates:
                            self.assertTrue(
                                any(
                                    source_id.startswith(
                                        f"evidence-{candidate.symbol.lower()}-"
                                    )
                                    for source_id in manifest.source_observation_ids
                                )
                            )

                        with self.assertRaisesRegex(
                            screening_module.ScreeningError,
                            "publication decision authority",
                        ):
                            screening_module._publication_observation_manifest(
                                copy.copy(decision)
                            )

    def test_publication_rejects_forged_same_id_fact_and_cross_client_splice(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                chain = _publication_authority_chain(
                    journal,
                    candidate_count=1,
                )
                genuine_candidate = chain[-3][0]
                plan = chain[-2]
                genuine_authority = (
                    screening_module._issued_scored_candidate_authority(
                        genuine_candidate
                    )
                )
                assert genuine_authority is not None

                forged_candidate = _issued_provider_candidates(
                    1,
                    forge_aapl_latest=True,
                )[0]
                forged_authority = (
                    screening_module._issued_scored_candidate_authority(
                        forged_candidate
                    )
                )
                assert forged_authority is not None
                genuine_quote = genuine_authority.context.latest_iex_quote
                forged_quote = forged_authority.context.latest_iex_quote
                self.assertEqual(
                    genuine_quote.source_observation_id,
                    forged_quote.source_observation_id,
                )
                self.assertNotEqual(genuine_quote.bid, forged_quote.bid)
                self.assertFalse(
                    alpaca_module.is_issued_normalized_market_fact(
                        forged_quote
                    )
                )
                self.assertEqual(
                    forged_authority.normalized_market_fact_sources,
                    (),
                )
                with self.assertRaisesRegex(
                    screening_module.ScreeningError,
                    "provider authority owner",
                ):
                    screening_module._issue_portfolio_bound_publication_decision(
                        (forged_candidate,),
                        primary_plan_decision=plan,
                    )

                other_client_candidates = _issued_provider_candidates(3)
                other_client_shadow = next(
                    candidate
                    for candidate in other_client_candidates
                    if candidate.total_score < genuine_candidate.total_score
                )
                with self.assertRaisesRegex(
                    screening_module.ScreeningError,
                    "cross provider owners",
                ):
                    screening_module._issue_portfolio_bound_publication_decision(
                        (genuine_candidate, other_client_shadow),
                        primary_plan_decision=plan,
                    )

    def test_alpaca_normalized_fact_authority_binds_raw_item_and_terminal_fetch(
        self,
    ) -> None:
        now = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        window = alpaca_module.TimeWindow(
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 8, 13, 20, tzinfo=UTC),
        )
        first_client = alpaca_module.AlpacaMarketData(
            FixtureTransport("providers/alpaca/complete-bars.json"),
            credentials(),
            now=lambda: now,
        )
        first_cohort = first_client.daily_bars(("QQQ", "SPY"), window)
        first_bar = first_cohort["SPY"][0]
        source = alpaca_module._normalized_market_fact_source(first_bar)
        fetch_bundle = alpaca_module.read_provider_fetch_bundle(first_bar)

        self.assertTrue(
            alpaca_module.is_issued_provider_fetch_cohort(first_cohort)
        )
        self.assertIs(
            alpaca_module._provider_fetch_cohort_manifest(first_cohort),
            source.fetch_manifest,
        )
        self.assertTrue(
            alpaca_module.is_issued_normalized_market_fact(first_bar)
        )
        self.assertEqual(source.kind, "BAR")
        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(
            source.source_observation_id,
            first_bar.source_observation_id,
        )
        self.assertGreater(source.source_item_ordinal, 0)
        self.assertTrue(source.source_item_path.startswith("$.bars.SPY["))
        self.assertEqual(len(source.page_payload_sha256), 64)
        self.assertEqual(len(source.normalized_fields_digest), 64)
        self.assertTrue(source.fetch_manifest.terminal)
        self.assertIs(fetch_bundle.manifest, source.fetch_manifest)
        self.assertEqual(
            tuple(item.page for item in fetch_bundle.pages),
            source.fetch_manifest.pages,
        )
        self.assertTrue(
            all(
                item.observation.observation_id
                == item.page.source_observation_id
                and hashlib.sha256(item.payload).hexdigest()
                == item.page.payload_sha256
                for item in fetch_bundle.pages
            )
        )
        self.assertIsNone(
            source.fetch_manifest.pages[-1].next_page_token
        )
        self.assertEqual(
            tuple(page.page_ordinal for page in source.fetch_manifest.pages),
            tuple(range(1, len(source.fetch_manifest.pages) + 1)),
        )
        for prior, successor in zip(
            source.fetch_manifest.pages,
            source.fetch_manifest.pages[1:],
            strict=False,
        ):
            self.assertEqual(
                successor.request_page_token,
                prior.next_page_token,
            )

        forged = replace(first_bar, close=first_bar.close + Decimal("1"))
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(copy.copy(first_bar))
        )
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(forged)
        )
        with self.assertRaisesRegex(ValueError, "authority"):
            alpaca_module._normalized_market_fact_source(forged)

        caller_manifest = replace(
            source.fetch_manifest,
            manifest_digest="a" * 64,
        )
        caller_fact = replace(
            first_bar,
            close=first_bar.close + Decimal("2"),
        )
        with self.assertRaisesRegex(
            alpaca_module.ProviderDataError,
            "fetch authority",
        ):
            alpaca_module._issue_market_fact_from_fetch(
                caller_fact,
                owner=object(),
                fetch_manifest=caller_manifest,
                page_ordinal=source.page_ordinal,
                source_item_ordinal=source.source_item_ordinal,
                source_item_path=source.source_item_path,
            )
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(caller_fact)
        )

        forged_from_genuine_fetch = replace(
            first_bar,
            close=first_bar.close + Decimal("3"),
        )
        with self.assertRaisesRegex(
            alpaca_module.ProviderDataError,
            "raw provider item",
        ):
            alpaca_module._issue_market_fact_from_fetch(
                forged_from_genuine_fetch,
                owner=first_client,
                fetch_manifest=source.fetch_manifest,
                page_ordinal=source.page_ordinal,
                source_item_ordinal=source.source_item_ordinal,
                source_item_path=source.source_item_path,
            )
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(
                forged_from_genuine_fetch
            )
        )

        self.assertFalse(
            hasattr(alpaca_module, "_issue_normalized_market_fact"),
            "a field-only normalized-fact registrar must not exist",
        )

        second_client = alpaca_module.AlpacaMarketData(
            FixtureTransport("providers/alpaca/complete-bars.json"),
            credentials(),
            now=lambda: now,
        )
        second_bar = second_client.daily_bars(("QQQ", "SPY"), window)["SPY"][0]
        self.assertEqual(
            first_bar.source_observation_id,
            second_bar.source_observation_id,
        )
        self.assertFalse(
            alpaca_module.normalized_market_facts_share_owner(
                first_bar,
                second_bar,
            )
        )

        for label, mutate in (
            (
                "source",
                lambda value: object.__setattr__(
                    value,
                    "normalized_fields_digest",
                    "f" * 64,
                ),
            ),
            (
                "page",
                lambda value: object.__setattr__(
                    value.fetch_manifest.pages[0],
                    "payload_sha256",
                    "e" * 64,
                ),
            ),
            (
                "manifest",
                lambda value: object.__setattr__(
                    value.fetch_manifest,
                    "request_digest",
                    "d" * 64,
                ),
            ),
        ):
            with self.subTest(mutation=label):
                mutation_client = alpaca_module.AlpacaMarketData(
                    FixtureTransport("providers/alpaca/complete-bars.json"),
                    credentials(),
                    now=lambda: now,
                )
                mutation_bar = mutation_client.daily_bars(
                    ("QQQ", "SPY"),
                    window,
                )["SPY"][0]
                mutation_source = (
                    alpaca_module._normalized_market_fact_source(mutation_bar)
                )
                mutate(mutation_source)
                self.assertFalse(
                    alpaca_module.is_issued_normalized_market_fact(
                        mutation_bar
                    )
                )

    def test_alpaca_historical_trade_is_terminal_provider_issued_fact(self) -> None:
        from stock_monitor.providers.http import HttpResponse

        body = json.dumps(
            {
                "next_page_token": None,
                "trades": {
                    "AAPL": [
                        {
                            "i": 701,
                            "p": "20.40",
                            "s": 100,
                            "t": "2026-08-13T19:59:00Z",
                        }
                    ]
                },
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

        class TradeTransport:
            def get(self, url: str, headers: object) -> HttpResponse:
                return HttpResponse(
                    status=200,
                    headers=(("Content-Type", "application/json"),),
                    body=body,
                    url=url,
                )

        now = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        window = alpaca_module.TimeWindow(
            datetime(2026, 8, 13, 19, 55, tzinfo=UTC),
            datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        )
        client = alpaca_module.AlpacaMarketData(
            TradeTransport(),
            credentials(),
            now=lambda: now,
        )
        trade = client.historical_trades(("AAPL",), window)["AAPL"][0]
        source = alpaca_module._normalized_market_fact_source(trade)

        self.assertEqual(trade.price, Decimal("20.40"))
        self.assertEqual(trade.size, 100)
        self.assertEqual(trade.sequence, 701)
        self.assertEqual(source.kind, "TRADE")
        self.assertEqual(source.source_item_path, "$.trades.AAPL[0]")
        self.assertEqual(source.fetch_manifest.collection, "trades")
        self.assertTrue(source.fetch_manifest.terminal)
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(copy.copy(trade))
        )
        self.assertFalse(
            alpaca_module.is_issued_normalized_market_fact(
                replace(trade, price=Decimal("20.41"))
            )
        )
        with self.assertRaisesRegex(
            alpaca_module.ProviderDataError,
            "complete raw provider item cohort",
        ):
            alpaca_module._issue_provider_fetch_cohort(
                owner=client,
                manifest=source.fetch_manifest,
                values={"AAPL": ()},
            )

    def test_alpaca_terminal_trade_fetch_authority_allows_empty_and_quiet_symbols(
        self,
    ) -> None:
        from stock_monitor.providers.http import HttpResponse

        class TradeTransport:
            def __init__(self, body: bytes) -> None:
                self._body = body

            def get(self, url: str, headers: object) -> HttpResponse:
                return HttpResponse(
                    status=200,
                    headers=(("Content-Type", "application/json"),),
                    body=self._body,
                    url=url,
                )

        now = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        window = alpaca_module.TimeWindow(
            datetime(2026, 8, 13, 19, 0, tzinfo=UTC),
            datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        )
        for label, raw_trades, expected_count in (
            ("empty", [], 0),
            (
                "quiet",
                [
                    {
                        "i": 701,
                        "p": "20.40",
                        "s": 100,
                        "t": "2026-08-13T19:10:00Z",
                    }
                ],
                1,
            ),
        ):
            with self.subTest(cohort=label):
                body = json.dumps(
                    {
                        "next_page_token": None,
                        "trades": {"AAPL": raw_trades},
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                client = alpaca_module.AlpacaMarketData(
                    TradeTransport(body),
                    credentials(),
                    now=lambda: now,
                )
                cohort = client.historical_trades(("AAPL",), window)

                self.assertEqual(tuple(cohort), ("AAPL",))
                self.assertEqual(len(cohort["AAPL"]), expected_count)
                self.assertTrue(
                    alpaca_module.is_issued_provider_fetch_cohort(cohort)
                )
                manifest = alpaca_module._provider_fetch_cohort_manifest(
                    cohort
                )
                bundle = alpaca_module.read_provider_fetch_bundle(cohort)
                self.assertEqual(manifest.collection, "trades")
                self.assertIs(bundle.manifest, manifest)
                self.assertEqual(len(bundle.pages), 1)
                self.assertEqual(bundle.pages[0].payload, body)
                self.assertEqual(
                    bundle.pages[0].observation.observation_id,
                    manifest.pages[0].source_observation_id,
                )
                self.assertEqual(manifest.requested_symbols, ("AAPL",))
                self.assertTrue(manifest.terminal)
                self.assertIsNone(manifest.pages[-1].next_page_token)
                self.assertFalse(
                    alpaca_module.is_issued_provider_fetch_cohort(
                        copy.copy(cohort)
                    )
                )
                with self.assertRaisesRegex(ValueError, "authority"):
                    alpaca_module._provider_fetch_cohort_manifest(
                        dict(cohort)
                    )
                with self.assertRaisesRegex(ValueError, "authority"):
                    alpaca_module.read_provider_fetch_bundle(dict(cohort))

    def test_provider_fetch_cohort_set_requires_one_exact_client_owner(self) -> None:
        full_session = alpaca_module.TimeWindow(
            aware_et(_SESSION, "09:30").astimezone(UTC),
            aware_et(_SESSION, "16:00").astimezone(UTC),
        )
        retrieved_at = aware_et(_SESSION, "16:20").astimezone(UTC)

        def client() -> alpaca_module.AlpacaMarketData:
            transport = _AuthorityLifecycleTransport(
                symbol="AAPL",
                trade_price=Decimal("20.40"),
                bid=Decimal("20.41"),
                ask=Decimal("20.42"),
                emit_quote=False,
            )
            return alpaca_module.AlpacaMarketData(
                transport,
                credentials(),
                now=lambda: retrieved_at,
            )

        first = client()
        second = client()
        first_trade = first.historical_trades(("AAPL",), full_session)
        first_empty_quote = first.historical_quotes(("AAPL",), full_session)
        second_empty_quote = second.historical_quotes(("AAPL",), full_session)

        self.assertTrue(
            alpaca_module.provider_fetch_cohorts_share_owner(
                first_trade,
                first_empty_quote,
            )
        )
        self.assertFalse(
            alpaca_module.provider_fetch_cohorts_share_owner(
                first_trade,
                second_empty_quote,
            )
        )
        self.assertFalse(
            alpaca_module.provider_fetch_cohorts_share_owner(
                first_trade,
                copy.copy(first_empty_quote),
            )
        )
        self.assertFalse(alpaca_module.provider_fetch_cohorts_share_owner())
        self.assertFalse(
            alpaca_module.provider_fetch_cohorts_share_owner(
                first_trade,
                object(),
            )
        )

    def test_alpaca_page_metadata_recomputes_identity_time_and_delay_from_raw(
        self,
    ) -> None:
        from stock_monitor.providers.http import HttpResponse

        body = json.dumps(
            {
                "bars": {
                    "SPY": [
                        {
                            "c": "100",
                            "h": "101",
                            "l": "98",
                            "o": "99",
                            "t": "2026-08-13T20:00:00Z",
                            "v": 1000,
                        }
                    ]
                },
                "next_page_token": None,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

        class BarTransport:
            def get(self, url: str, headers: object) -> HttpResponse:
                return HttpResponse(
                    status=200,
                    headers=(("Content-Type", "application/json"),),
                    body=body,
                    url=url,
                )

        retrieved_at = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        window = alpaca_module.TimeWindow(
            datetime(2026, 8, 13, 19, 0, tzinfo=UTC),
            datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        )
        client = alpaca_module.AlpacaMarketData(
            BarTransport(),
            credentials(),
            now=lambda: retrieved_at,
        )
        bar = client.daily_bars(("SPY",), window)["SPY"][0]
        source = alpaca_module._normalized_market_fact_source(bar)
        page = source.fetch_manifest.pages[0]

        metadata = alpaca_module.recompute_alpaca_page_metadata(
            payload=body,
            request_url=page.request_url,
            source_type=page.source_type,
            retrieved_at=retrieved_at,
        )

        self.assertEqual(
            metadata.source_observation_id,
            page.source_observation_id,
        )
        self.assertEqual(
            metadata.source_time,
            datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        )
        self.assertEqual(metadata.delay_seconds, 61_200)
        self.assertEqual(
            metadata.payload_sha256,
            hashlib.sha256(body).hexdigest(),
        )
        shifted = alpaca_module.recompute_alpaca_page_metadata(
            payload=body,
            request_url=page.request_url,
            source_type=page.source_type,
            retrieved_at=retrieved_at + timedelta(seconds=1),
        )
        self.assertNotEqual(
            shifted.source_observation_id,
            metadata.source_observation_id,
        )
        self.assertEqual(shifted.delay_seconds, metadata.delay_seconds + 1)
        with self.assertRaisesRegex(
            alpaca_module.ProviderDataError,
            "future",
        ):
            alpaca_module.recompute_alpaca_page_metadata(
                payload=body,
                request_url=page.request_url,
                source_type=page.source_type,
                retrieved_at=datetime(2026, 8, 13, 19, 59, tzinfo=UTC),
            )

    def test_public_breaker_reader_retains_exact_source_across_collection(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                history = journal.read_phase1_breaker_history(
                    ledger_name="CANONICAL",
                    through_session=date(2026, 8, 13),
                    query_cutoff=aware_et(date(2026, 8, 13), "16:00"),
                    calendar_resolver=_calendar(),
                )

                gc.collect()

                self.assertTrue(
                    risk_module.is_issued_breaker_history_authority(history)
                )
                state = risk_module.evaluate_authorized_breakers(history)
                self.assertTrue(risk_module.is_issued_breaker_state(state))

    def test_position_evidence_authority_derives_clear_stock_and_etf_truth(
        self,
    ) -> None:
        expected_sessions = (
            date(2026, 8, 14),
            date(2026, 8, 17),
            date(2026, 8, 18),
            date(2026, 8, 19),
            date(2026, 8, 20),
            date(2026, 8, 21),
            date(2026, 8, 24),
            date(2026, 8, 25),
            date(2026, 8, 26),
            date(2026, 8, 27),
        )
        def issue(source: object, records: tuple[object, ...], **options: object):
            options.setdefault("subject_kind", source.subject_kind)
            options.setdefault("issuer_cik", source.issuer_cik)
            position, bundle, decision, review_at = (
                _reviewed_position_evidence_context(
                    records,
                    symbol=source.symbol,
                    **options,
                )
            )
            signal_evidence = (
                risk_module._issue_phase1_signal_evidence_authority(
                    source,
                    bundle,
                    decision,
                    review_at=review_at,
                    calendar_resolver=_calendar(),
                )
            )
            position_evidence = (
                risk_module._issue_phase1_position_evidence_authority(
                    signal_evidence,
                    position=position,
                )
            )
            return signal_evidence, position_evidence

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "stock.db") as journal:
                stock_source = _published_signal_source(journal)
                clear_signal, clear = issue(stock_source, ())
                self.assertTrue(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        clear_signal
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_position_evidence_authority(
                        clear
                    )
                )
                self.assertEqual(clear.status, "CLEAR")
                self.assertIs(clear.event_exit_required, False)
                self.assertIs(clear.thesis_invalidated, False)
                self.assertEqual(clear.remaining_sessions, expected_sessions)
                self.assertEqual(
                    clear.terminal_hold_session,
                    expected_sessions[-1],
                )
                self.assertEqual(
                    clear.source_observation_ids,
                    ("coverage-aapl",),
                )
                manifest = risk_module.phase1_signal_evidence_manifest(
                    clear_signal
                )
                self.assertIsInstance(manifest, bytes)
                self.assertEqual(
                    hashlib.sha256(manifest).hexdigest(),
                    clear_signal.source_digest,
                )

                event_record = reviewed_evidence_record(
                    symbol=stock_source.symbol,
                    issuer_cik=stock_source.issuer_cik,
                    event_date=date(2026, 8, 18),
                    event_kind="BINARY_EVENT",
                )
                _event_signal, event = issue(stock_source, (event_record,))
                self.assertEqual(event.status, "EXIT_REQUIRED")
                self.assertIs(event.event_exit_required, True)
                self.assertIs(event.thesis_invalidated, False)
                self.assertEqual(
                    event.relevant_events,
                    ((date(2026, 8, 18), "material agreement"),),
                )

                adverse_record = reviewed_evidence_record(
                    symbol=stock_source.symbol,
                    issuer_cik=stock_source.issuer_cik,
                    adverse_tags=("restatement",),
                )
                _adverse_signal, adverse = issue(
                    stock_source,
                    (adverse_record,),
                )
                self.assertEqual(adverse.status, "EXIT_REQUIRED")
                self.assertIs(adverse.event_exit_required, False)
                self.assertIs(adverse.thesis_invalidated, True)
                self.assertEqual(adverse.adverse_tags, ("restatement",))

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "etf.db") as journal:
                etf_source = _published_signal_source(
                    journal,
                    candidates=(_issued_candidates(3)[0],),
                )
                etf_record = reviewed_etf_evidence_record(
                    symbol=etf_source.symbol,
                    event_date=date(2026, 8, 18),
                    event_kind="ETF_ACTION",
                    event_type="fund sponsor notice",
                )
                _etf_signal, etf = issue(
                    etf_source,
                    (etf_record,),
                    subject_kind="ETF",
                    issuer_cik=None,
                    binary_event_coverage="NOT_APPLICABLE",
                    etf_action_coverage="CONFIRMED_CLEAR",
                )
                self.assertEqual(etf.subject_kind, "ETF")
                self.assertEqual(etf.symbol, "QQQ")
                self.assertIs(etf.event_exit_required, True)
                self.assertIs(etf.thesis_invalidated, False)
                self.assertEqual(
                    etf.relevant_events[0][0],
                    date(2026, 8, 18),
                )

    def test_position_evidence_authority_preserves_unresolved_truth(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "stock.db") as journal:
                source = _published_signal_source(journal)
                stale_record = reviewed_evidence_record(
                    symbol=source.symbol,
                    issuer_cik=source.issuer_cik,
                    published_at=_EVIDENCE_AS_OF - timedelta(days=3),
                    retrieved_at=_EVIDENCE_AS_OF - timedelta(days=2),
                )
                cases = (
                    ((), {"binary_event_coverage": "UNKNOWN"}),
                    ((), {"coverage_healthy": False}),
                    ((stale_record,), {}),
                    (
                        (
                            reviewed_evidence_record(
                                symbol=source.symbol,
                                issuer_cik=source.issuer_cik,
                                classification_ambiguous=True,
                            ),
                        ),
                        {},
                    ),
                )
                for case_index, (records, options) in enumerate(cases):
                    with self.subTest(options=options, records=records):
                        position, bundle, decision, review_at = (
                            _reviewed_position_evidence_context(
                                records,
                                symbol=source.symbol,
                                subject_kind=source.subject_kind,
                                issuer_cik=source.issuer_cik,
                                **options,
                            )
                        )
                        signal_evidence = (
                            risk_module._issue_phase1_signal_evidence_authority(
                                source,
                                bundle,
                                decision,
                                review_at=review_at,
                                calendar_resolver=_calendar(),
                            )
                        )
                        authority = (
                            risk_module._issue_phase1_position_evidence_authority(
                                signal_evidence,
                                position=position,
                            )
                        )
                        self.assertTrue(
                            risk_module.is_issued_phase1_position_evidence_authority(
                                authority
                            )
                        )
                        self.assertEqual(authority.status, "UNRESOLVED")
                        self.assertIsNone(authority.event_exit_required)
                        if case_index == 0:
                            self.assertIs(authority.thesis_invalidated, False)
                        else:
                            self.assertIsNone(authority.thesis_invalidated)
                        self.assertTrue(authority.reason_codes)

    def test_position_evidence_authority_is_identity_bound_and_reissued(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "stock.db"
            with Journal.open(path) as first_journal:
                first_source = _published_signal_source(first_journal)
                first_context = _reviewed_position_evidence_context(
                    (),
                    symbol=first_source.symbol,
                    subject_kind=first_source.subject_kind,
                    issuer_cik=first_source.issuer_cik,
                )
                position, bundle, decision, review_at = first_context
                first_signal = (
                    risk_module._issue_phase1_signal_evidence_authority(
                        first_source,
                        bundle,
                        decision,
                        review_at=review_at,
                        calendar_resolver=_calendar(),
                    )
                )
                first = risk_module._issue_phase1_position_evidence_authority(
                    first_signal,
                    position=position,
                )
                second_context = _reviewed_position_evidence_context(
                    (),
                    symbol=first_source.symbol,
                    subject_kind=first_source.subject_kind,
                    issuer_cik=first_source.issuer_cik,
                )
                with self.assertRaisesRegex(RiskBlock, "EVIDENCE"):
                    risk_module._issue_phase1_signal_evidence_authority(
                        first_source,
                        bundle,
                        second_context[2],
                        review_at=review_at,
                        calendar_resolver=_calendar(),
                    )
                self.assertFalse(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        copy.copy(first_signal)
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_evidence_authority(
                        copy.copy(first)
                    )
                )
                with self.assertRaises((TypeError, RiskBlock)):
                    risk_module._issue_phase1_signal_evidence_authority(
                        object(),
                        object(),
                        object(),
                        review_at=review_at,
                        calendar_resolver=_calendar(),
                    )

            with Journal.open(path) as second_journal:
                second_source = second_journal._read_phase1_signal_source(
                    first_source.signal_id,
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                second_context = _reviewed_position_evidence_context(
                    (),
                    symbol=second_source.symbol,
                    subject_kind=second_source.subject_kind,
                    issuer_cik=second_source.issuer_cik,
                )
                position, bundle, decision, review_at = second_context
                second_signal = (
                    risk_module._issue_phase1_signal_evidence_authority(
                        second_source,
                        bundle,
                        decision,
                        review_at=review_at,
                        calendar_resolver=_calendar(),
                    )
                )
                second = risk_module._issue_phase1_position_evidence_authority(
                    second_signal,
                    position=position,
                )
                self.assertIsNot(first_signal, second_signal)
                self.assertEqual(first_signal, second_signal)
                self.assertEqual(
                    first_signal.source_digest,
                    second_signal.source_digest,
                )
                self.assertIsNot(first, second)
                self.assertEqual(first, second)
                self.assertEqual(first.source_digest, second.source_digest)
                self.assertFalse(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        first_signal
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_evidence_authority(
                        first
                    )
                )

                nested_bundle = second_context[1]
                object.__setattr__(nested_bundle, "symbol", "BAD")
                self.assertFalse(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        second_signal
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_evidence_authority(
                        second
                    )
                )

        self.assertFalse(
            hasattr(risk_module, "register_phase1_signal_evidence_authority")
        )
        self.assertFalse(
            hasattr(risk_module, "register_phase1_position_evidence_authority")
        )

    def test_journal_signal_evidence_reissues_from_exact_persisted_raw_bindings(
        self,
    ) -> None:
        for owner, name in (
            (Journal, "record_phase1_signal_evidence"),
            (Journal, "read_phase1_signal_evidence_source"),
            (Journal, "read_phase1_signal_evidence"),
            (
                journal_module,
                "is_verified_phase1_signal_evidence_source",
            ),
            (
                risk_module,
                "_issue_phase1_signal_evidence_authority_from_source",
            ),
            (
                evidence_module,
                "_issue_reviewed_bundle_from_phase1_source",
            ),
        ):
            self.assertTrue(
                hasattr(owner, name),
                f"Task 8 durable evidence contract requires {name}",
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as first_journal:
                signal_source, _ephemeral, stored, review_at = (
                    _persist_signal_evidence(first_journal, adverse=True)
                )
                self.assertFalse(stored.duplicate)
                first_source = first_journal.read_phase1_signal_evidence_source(
                    signal_source.signal_id,
                    review_at=review_at,
                    query_cutoff=review_at,
                    calendar_resolver=_calendar(),
                )
                first_authority = first_journal.read_phase1_signal_evidence(
                    signal_source.signal_id,
                    review_at=review_at,
                    query_cutoff=review_at,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_signal_evidence_source(
                        first_source
                    )
                )
                self.assertIsInstance(first_source.registry_payload, bytes)
                self.assertEqual(
                    hashlib.sha256(first_source.registry_payload).hexdigest(),
                    first_source.release_sha256,
                )
                self.assertEqual(
                    tuple(
                        document.source_observation_id
                        for document in first_source.source_documents
                    ),
                    tuple(
                        sorted(
                            document.source_observation_id
                            for document in first_source.source_documents
                        )
                    ),
                )
                self.assertTrue(
                    evidence_module._is_reviewed_bundle(
                        first_source.reviewed_bundle
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        first_authority
                    )
                )
                self.assertEqual(first_authority.status, "EXIT_REQUIRED")
                self.assertEqual(
                    first_source.manifest_bytes,
                    risk_module.phase1_signal_evidence_manifest(
                        first_authority
                    ),
                )
                self.assertEqual(
                    hashlib.sha256(first_source.manifest_bytes).hexdigest(),
                    first_source.manifest_digest,
                )
                self.assertEqual(
                    first_source.manifest_digest,
                    first_authority.source_digest,
                )
                self.assertEqual(
                    tuple(
                        binding.document.source_observation_id
                        for binding in first_source.reviewed_bundle.source_bindings
                    ),
                    tuple(
                        sorted(first_authority.source_observation_ids)
                    ),
                )

            self.assertFalse(
                journal_module.is_verified_phase1_signal_evidence_source(
                    first_source
                )
            )
            self.assertFalse(
                risk_module.is_issued_phase1_signal_evidence_authority(
                    first_authority
                )
            )

            with Journal.open(path) as second_journal:
                second_source = (
                    second_journal.read_phase1_signal_evidence_source(
                        signal_source.signal_id,
                        review_at=review_at,
                        query_cutoff=review_at,
                        calendar_resolver=_calendar(),
                    )
                )
                second_authority = (
                    second_journal.read_phase1_signal_evidence(
                        signal_source.signal_id,
                        review_at=review_at,
                        query_cutoff=review_at,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNot(first_source, second_source)
                self.assertIsNot(first_authority, second_authority)
                self.assertIsNot(
                    first_source.reviewed_bundle,
                    second_source.reviewed_bundle,
                )
                self.assertEqual(first_source, second_source)
                self.assertEqual(first_authority, second_authority)
                self.assertEqual(
                    first_source.source_digest,
                    second_source.source_digest,
                )
                self.assertEqual(
                    first_authority.source_digest,
                    second_authority.source_digest,
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_signal_evidence_source(
                        second_source
                    )
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        second_authority
                    )
                )

        self.assertFalse(hasattr(Journal, "append_phase1_signal_evidence"))
        self.assertFalse(hasattr(Journal, "register_phase1_signal_evidence"))
        self.assertFalse(
            hasattr(evidence_module, "load_released_evidence_bundle"),
            "raw registry bytes plus a caller-supplied hash must not be a registrar",
        )

    def test_durable_signal_evidence_rejects_copy_subset_lookahead_and_splice(
        self,
    ) -> None:
        for owner, name in (
            (Journal, "record_phase1_signal_evidence"),
            (Journal, "read_phase1_signal_evidence_source"),
            (
                journal_module,
                "is_verified_phase1_signal_evidence_source",
            ),
            (
                risk_module,
                "_issue_phase1_signal_evidence_authority_from_source",
            ),
            (
                evidence_module,
                "_issue_reviewed_bundle_from_phase1_source",
            ),
        ):
            self.assertTrue(
                hasattr(owner, name),
                f"Task 8 durable evidence contract requires {name}",
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                first_signal, _authority, _stored, review_at = (
                    _persist_signal_evidence(first_journal, adverse=False)
                )
                second_signal, _other, _second_stored, _ = (
                    _persist_signal_evidence(second_journal, adverse=False)
                )
                source = first_journal.read_phase1_signal_evidence_source(
                    first_signal.signal_id,
                    review_at=review_at,
                    query_cutoff=review_at,
                    calendar_resolver=_calendar(),
                )
                other_signal_source = second_journal._read_phase1_signal_source(
                    second_signal.signal_id,
                    query_cutoff=review_at,
                )
                forged_sources = (
                    copy.copy(source),
                    replace(source),
                    replace(
                        source,
                        row_references=source.row_references[:-1],
                    ),
                    replace(
                        source,
                        registry_payload=source.registry_payload + b" ",
                    ),
                    replace(source, release_sha256="0" * 64),
                    replace(
                        source,
                        source_documents=source.source_documents[:-1],
                    ),
                    replace(
                        source,
                        query_cutoff=(
                            source.query_cutoff + timedelta(microseconds=1)
                        ),
                    ),
                    replace(source, signal_source=other_signal_source),
                )
                for forged in forged_sources:
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_signal_evidence_source(
                                forged
                            )
                        )
                        with self.assertRaises(
                            (RiskBlock, validation_module.ValidationError)
                        ):
                            risk_module._issue_phase1_signal_evidence_authority_from_source(
                                forged,
                                calendar_resolver=_calendar(),
                            )

                with self.assertRaises(journal_module.InvalidJournalValue):
                    first_journal.read_phase1_signal_evidence_source(
                        first_signal.signal_id,
                        review_at=review_at,
                        query_cutoff=review_at - timedelta(microseconds=1),
                        calendar_resolver=_calendar(),
                    )

    def test_typed_terminal_writer_derives_invalidation_from_persisted_evidence(
        self,
    ) -> None:
        for owner, name in (
            (Journal, "record_phase1_signal_evidence"),
            (Journal, "read_phase1_signal_evidence"),
            (Journal, "record_phase1_unentered_terminal"),
        ):
            self.assertTrue(
                hasattr(owner, name),
                f"Task 8 typed terminal contract requires {name}",
            )
        terminal_signature = inspect.signature(
            Journal.record_phase1_unentered_terminal
        )
        for forbidden in ("authority", "event_kind", "kind", "status"):
            self.assertNotIn(forbidden, terminal_signature.parameters)

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_source, _ephemeral, _stored, review_at = (
                    _persist_signal_evidence(journal, adverse=True)
                )
                authority = journal.read_phase1_signal_evidence(
                    signal_source.signal_id,
                    review_at=review_at,
                    query_cutoff=review_at,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_signal_evidence_authority(
                        authority
                    )
                )
                before = journal.count("phase1_signal_events")
                result = journal.record_phase1_unentered_terminal(
                    signal_source.signal_id,
                    recorded_at=review_at,
                    calendar_resolver=_calendar(),
                )
                self.assertFalse(result.duplicate)
                self.assertEqual(result.event_kind, "INVALIDATE")
                self.assertEqual(result.to_status, "INVALIDATED")
                self.assertIsNone(result.session_completion_id)
                self.assertEqual(journal.count("phase1_signal_events"), before + 1)
                with self.assertRaises(TypeError):
                    journal.record_phase1_unentered_terminal(
                        signal_source.signal_id,
                        event_kind="EXPIRE",
                        recorded_at=review_at,
                        calendar_resolver=_calendar(),
                    )

    def test_typed_terminal_writer_derives_each_timely_completion_disposition(
        self,
    ) -> None:
        for name in (
            "record_phase1_unentered_terminal",
            "read_phase1_expiry_deadline_source",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 typed terminal contract requires {name}",
            )
        signature = inspect.signature(Journal.record_phase1_unentered_terminal)
        for forbidden in ("authority", "event_kind", "kind", "status"):
            self.assertNotIn(forbidden, signature.parameters)

        cases = (
            (
                "NOT_TRIGGERED",
                "FINALIZE_NOT_TRIGGERED",
                "NOT_TRIGGERED",
            ),
            (
                "NOT_FILLED_LIMIT",
                "FINALIZE_NOT_FILLED",
                "NOT_FILLED_LIMIT",
            ),
            (
                "UNRESOLVED",
                "FINALIZE_UNRESOLVED",
                "UNRESOLVED",
            ),
        )
        deadline_session = _calendar().add_sessions(_SESSION, 1)
        recorded_at = aware_et(deadline_session, "08:45")
        for role in ("PRIMARY", "WATCHLIST_SHADOW"):
            for simulated_status, event_kind, to_status in cases:
                with self.subTest(
                    role=role,
                    simulated_status=simulated_status,
                ), tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _publish(
                            journal,
                            candidates_override=_issued_candidates(2),
                        )
                        replay = journal._read_phase1_canonical_replay_source(
                            query_cutoff=aware_et(_SESSION, "08:45"),
                        )
                        signal_source = next(
                            source
                            for source in replay.signal_sources
                            if source.role == role
                        )
                        trigger = risk_module.money_from_micros(
                            signal_source.trigger_price_micros
                        )
                        limit = risk_module.money_from_micros(
                            signal_source.maximum_entry_micros
                        )
                        tick = risk_module.money_from_micros(
                            signal_source.tick_size_micros
                        )
                        trade_id, quote_id, completion_at = (
                            _append_completed_entry_observations(
                                journal,
                                signal_source,
                                trade_price=(
                                    trigger - tick
                                    if simulated_status == "NOT_TRIGGERED"
                                    else trigger
                                ),
                                quote_bid=limit,
                                quote_ask=(
                                    limit + tick
                                    if simulated_status == "NOT_FILLED_LIMIT"
                                    else limit
                                ),
                                empty_quote=(
                                    simulated_status == "UNRESOLVED"
                                ),
                            )
                        )
                        observation_ids = (
                            (trade_id,)
                            if quote_id is None
                            else (trade_id, quote_id)
                        )
                        observations = tuple(
                            journal.read_phase1_observation(
                                observation_id,
                                query_cutoff=completion_at,
                            )
                            for observation_id in observation_ids
                        )
                        simulated = (
                            ledger_module._phase1_entry_result_from_observations(
                                observations,
                                trigger=trigger,
                                limit=limit,
                            )
                        )
                        self.assertEqual(
                            simulated.status.value,
                            simulated_status,
                        )
                        result = journal.record_phase1_unentered_terminal(
                            signal_source.signal_id,
                            recorded_at=recorded_at,
                            calendar_resolver=_calendar(),
                        )
                        self.assertFalse(result.duplicate)
                        self.assertEqual(result.event_kind, event_kind)
                        self.assertEqual(result.to_status, to_status)
                        self.assertIsNotNone(result.session_completion_id)
                        self.assertNotEqual(result.event_kind, "EXPIRE")

                    with Journal.open(path) as restarted:
                        duplicate = (
                            restarted.record_phase1_unentered_terminal(
                                signal_source.signal_id,
                                recorded_at=recorded_at,
                                calendar_resolver=_calendar(),
                            )
                        )
                        self.assertTrue(duplicate.duplicate)
                        self.assertEqual(duplicate.event_kind, event_kind)
                        self.assertEqual(duplicate.to_status, to_status)
                        self.assertEqual(
                            duplicate.source_digest,
                            result.source_digest,
                        )
                        with self.assertRaises(
                            (RiskBlock, journal_module.InvalidJournalValue)
                        ):
                            restarted.read_phase1_expiry_deadline_source(
                                signal_source.signal_id,
                                observed_at=recorded_at,
                                query_cutoff=recorded_at,
                                calendar_resolver=_calendar(),
                            )

    def test_terminal_precedence_cannot_hide_invalidation_as_expiry(self) -> None:
        for name in (
            "record_phase1_signal_evidence",
            "read_phase1_expiry_deadline_source",
            "record_phase1_unentered_terminal",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 terminal precedence requires {name}",
            )

        deadline_session = _calendar().add_sessions(_SESSION, 1)
        deadline = aware_et(deadline_session, "08:45")
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_source, _authority, _stored, _review_at = (
                    _persist_signal_evidence(journal, adverse=True)
                )
                with self.assertRaises(
                    (RiskBlock, journal_module.InvalidJournalValue)
                ):
                    journal.read_phase1_expiry_deadline_source(
                        signal_source.signal_id,
                        observed_at=deadline,
                        query_cutoff=deadline,
                        calendar_resolver=_calendar(),
                    )
                result = journal.record_phase1_unentered_terminal(
                    signal_source.signal_id,
                    recorded_at=deadline,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(result.event_kind, "INVALIDATE")
                self.assertEqual(result.to_status, "INVALIDATED")
                self.assertIsNone(result.session_completion_id)

    def test_typed_terminal_writer_derives_expiry_only_from_current_deadline(
        self,
    ) -> None:
        for owner, name in (
            (Journal, "read_phase1_expiry_deadline_source"),
            (Journal, "record_phase1_unentered_terminal"),
            (
                journal_module,
                "is_verified_phase1_expiry_deadline_source",
            ),
        ):
            self.assertTrue(
                hasattr(owner, name),
                f"Task 8 typed expiry contract requires {name}",
            )

        deadline_session = _calendar().add_sessions(_SESSION, 1)
        deadline = aware_et(deadline_session, "08:45")
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as journal, Journal.open(
                second_path
            ) as other_journal:
                signal_source = _published_signal_source(journal)
                other_signal = _published_signal_source(other_journal)
                source = journal.read_phase1_expiry_deadline_source(
                    signal_source.signal_id,
                    observed_at=deadline,
                    query_cutoff=deadline,
                    calendar_resolver=_calendar(),
                )
                other_source = other_journal.read_phase1_expiry_deadline_source(
                    other_signal.signal_id,
                    observed_at=deadline,
                    query_cutoff=deadline,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_expiry_deadline_source(
                        source
                    )
                )
                self.assertEqual(source.deadline_session, deadline_session)
                self.assertEqual(source.deadline_at, deadline)
                self.assertEqual(source.observed_at, deadline)
                self.assertEqual(source.query_cutoff, deadline)
                self.assertEqual(
                    source.expiry_evidence.signal_id,
                    signal_source.signal_id,
                )
                self.assertFalse(source.expiry_evidence is None)

                forged_sources = (
                    copy.copy(source),
                    replace(source),
                    replace(
                        source,
                        row_references=source.row_references[:-1],
                    ),
                    replace(
                        source,
                        query_cutoff=deadline + timedelta(microseconds=1),
                    ),
                    replace(
                        source,
                        signal_source=other_source.signal_source,
                    ),
                )
                for forged in forged_sources:
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_expiry_deadline_source(
                                forged
                            )
                        )

                for invalid_at in (
                    deadline - timedelta(microseconds=1),
                    aware_et(deadline_session, "09:35")
                    + timedelta(microseconds=1),
                ):
                    with self.subTest(invalid_at=invalid_at), self.assertRaises(
                        (RiskBlock, journal_module.InvalidJournalValue)
                    ):
                        journal.read_phase1_expiry_deadline_source(
                            signal_source.signal_id,
                            observed_at=invalid_at,
                            query_cutoff=invalid_at,
                            calendar_resolver=_calendar(),
                        )

                before = journal.count("phase1_signal_events")
                result = journal.record_phase1_unentered_terminal(
                    signal_source.signal_id,
                    recorded_at=deadline,
                    calendar_resolver=_calendar(),
                )
                self.assertFalse(result.duplicate)
                self.assertEqual(result.event_kind, "EXPIRE")
                self.assertEqual(result.to_status, "EXPIRED")
                self.assertIsNone(result.session_completion_id)
                self.assertEqual(journal.count("phase1_signal_events"), before + 1)

    def test_concurrent_expiry_retry_cannot_append_a_second_terminal_event(
        self,
    ) -> None:
        for name in (
            "read_phase1_expiry_deadline_source",
            "record_phase1_unentered_terminal",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 concurrent expiry contract requires {name}",
            )

        deadline_session = _calendar().add_sessions(_SESSION, 1)
        deadline = aware_et(deadline_session, "08:45")
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_source = _published_signal_source(journal)

            def expire_once(_: int) -> str:
                try:
                    with Journal.open(path) as journal:
                        source = journal.read_phase1_expiry_deadline_source(
                            signal_source.signal_id,
                            observed_at=deadline,
                            query_cutoff=deadline,
                            calendar_resolver=_calendar(),
                        )
                        result = journal.record_phase1_unentered_terminal(
                            signal_source.signal_id,
                            recorded_at=deadline,
                            calendar_resolver=_calendar(),
                        )
                        return "REJECTED" if result.duplicate else "CREATED"
                except (
                    journal_module.IdempotencyConflict,
                    journal_module.InvalidJournalValue,
                    RiskBlock,
                ):
                    return "REJECTED"

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = tuple(executor.map(expire_once, range(2)))
            self.assertEqual(sorted(outcomes), ["CREATED", "REJECTED"])
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("phase1_signal_events"), 2)

    def test_raw_exit_tuple_cannot_persist_canonical_rows(self) -> None:
        for name in (
            "ingest_phase1_exit_review_cohorts",
            "read_phase1_exit_review_source",
            "record_phase1_canonical_exit",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 provider-backed exit contract requires {name}",
            )
        self.assertEqual(
            tuple(
                inspect.signature(
                    Journal.record_phase1_canonical_exit
                ).parameters
            ),
            ("self", "exit_authority", "recorded_at", "calendar_resolver"),
        )
        for forbidden in (
            "append_phase1_observation",
            "append_phase1_exit_review",
            "record_phase1_exit",
            "record_phase1_partial_exit",
        ):
            self.assertFalse(
                hasattr(Journal, forbidden),
                f"raw exit registrar must remain absent: {forbidden}",
            )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                    journal.count("phase1_closed_trades"),
                )
                exit_at = aware_et(date(2026, 8, 17), "15:50")
                with self.assertRaises((TypeError, journal_module.InvalidJournalValue)):
                    journal.record_phase1_canonical_exit(  # type: ignore[attr-defined]
                        signal_id="2026-08-14:SPY",
                        shares=5,
                        price=Decimal("105.00"),
                        fee=Decimal("1.00"),
                        event_time=exit_at,
                        received_at=exit_at + timedelta(seconds=1),
                        reason="TWO_R_REACHED",
                    )
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                        journal.count("phase1_closed_trades"),
                    ),
                    before,
                )

    def test_profit_target_taken_prevents_repeated_two_r_partial_exit(
        self,
    ) -> None:
        untaken = risk_module.Position(
            signal_id="2026-08-14:SPY",
            symbol="SPY",
            entry=Decimal("100.00"),
            shares=10,
            initial_stop=Decimal("97.50"),
            recommended_stop=Decimal("97.50"),
            user_confirmed_stop=Decimal("97.50"),
            target=Decimal("105.00"),
            tick_size=Decimal("0.01"),
            entered_session=_SESSION,
            ledger_name="CANONICAL",
        )
        first_mark = risk_module.MarketMark(
            price=Decimal("105.00"),
            at=aware_et(date(2026, 8, 17), "15:30"),
            holding_sessions=2,
            previous_session_low=Decimal("104.00"),
            current_session_low=Decimal("104.00"),
            atr14=Decimal("1.00"),
        )

        first = risk_module.evaluate_position_diagnostic(
            untaken,
            first_mark,
            policy_fixture(),
        )

        self.assertEqual(first.status, "PROVISIONAL_EXIT")
        self.assertEqual(first.shares_to_exit, 5)
        self.assertEqual(first.remaining_shares, 5)
        self.assertEqual(first.recommended_stop, Decimal("103.90"))
        self.assertIn("TWO_R_REACHED", first.reason_codes)

        taken = replace(
            untaken,
            shares=first.remaining_shares,
            recommended_stop=first.recommended_stop,
            user_confirmed_stop=first.recommended_stop,
            profit_target_taken=True,
        )
        later_mark = risk_module.MarketMark(
            price=Decimal("105.50"),
            at=aware_et(date(2026, 8, 18), "15:30"),
            holding_sessions=3,
            previous_session_low=Decimal("105.10"),
            current_session_low=Decimal("105.20"),
            atr14=Decimal("1.00"),
        )

        later = risk_module.evaluate_position_diagnostic(
            taken,
            later_mark,
            policy_fixture(),
        )

        self.assertFalse(untaken.profit_target_taken)
        self.assertTrue(taken.profit_target_taken)
        self.assertNotEqual(
            risk_module._position_revision_digest(untaken),
            risk_module._position_revision_digest(taken),
        )
        self.assertEqual(later.status, "PROVISIONAL_HOLD")
        self.assertEqual(later.shares_to_exit, 0)
        self.assertEqual(later.remaining_shares, 5)
        self.assertNotIn("TWO_R_REACHED", later.reason_codes)

    def test_exit_review_mark_uses_quote_bid_or_completed_bar_haircut(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 17), "15:50")
        received_at = review_at + timedelta(seconds=1)
        close_at = aware_et(date(2026, 8, 17), "16:00")
        close_received_at = close_at + timedelta(seconds=1)
        bar_source_at = aware_et(date(2026, 8, 17), "09:30")
        quote = SimpleNamespace(
            observation_id="quote:AAPL:2026-08-17:review",
            observation_kind="QUOTE",
            feed="sip",
            fresh=True,
            bid_micros=21_490_000,
            ask_micros=21_500_000,
            close_micros=None,
            source_time=review_at,
            received_at=received_at,
        )
        bar = SimpleNamespace(
            observation_id="bar:AAPL:2026-08-17:close",
            observation_kind="BAR",
            fresh=True,
            bid_micros=None,
            ask_micros=None,
            close_micros=21_500_000,
            source_time=bar_source_at,
            received_at=close_received_at,
        )
        cases = (
            (
                quote,
                "SIP_QUOTE_BID",
                21_490_000,
                Decimal("21.49"),
                review_at,
                received_at,
            ),
        )

        for (
            observation,
            method,
            expected_micros,
            expected_price,
            mark_at,
            cutoff,
        ) in cases:
            with self.subTest(method=method):
                selected, price = (
                    risk_module._phase1_review_mark_from_observations(
                        (observation,),
                        observation_id=observation.observation_id,
                        method=method,
                        price_micros=expected_micros,
                        at=mark_at,
                        session_date=date(2026, 8, 17),
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )

                self.assertIs(selected, observation)
                self.assertEqual(price, expected_price)

        synthetic = SimpleNamespace(
            observation_id="mark:AAPL:2026-08-17:synthetic",
            observation_kind="MARK",
            feed="sip",
            fresh=True,
            bid_micros=21_490_000,
            ask_micros=21_500_000,
            close_micros=None,
            source_time=review_at,
            received_at=received_at,
        )
        with self.assertRaisesRegex(RiskBlock, "PHASE1_EXIT_MARK_MISMATCH"):
            risk_module._phase1_review_mark_from_observations(
                (synthetic,),
                observation_id=synthetic.observation_id,
                method="SIP_QUOTE_BID",
                price_micros=21_490_000,
                at=review_at,
                session_date=date(2026, 8, 17),
                query_cutoff=received_at,
                calendar_resolver=_calendar(),
            )
        for invalid_feed in ("iex", None):
            nonconsolidated = copy.copy(quote)
            nonconsolidated.feed = invalid_feed
            with self.subTest(feed=invalid_feed), self.assertRaisesRegex(
                RiskBlock,
                "PHASE1_EXIT_MARK_MISMATCH",
            ):
                risk_module._phase1_review_mark_from_observations(
                    (nonconsolidated,),
                    observation_id=nonconsolidated.observation_id,
                    method="SIP_QUOTE_BID",
                    price_micros=21_490_000,
                    at=review_at,
                    session_date=date(2026, 8, 17),
                    query_cutoff=received_at,
                    calendar_resolver=_calendar(),
                )

    def test_daily_close_mark_requires_issued_one_day_bar_cohort(self) -> None:
        session_date = date(2026, 8, 17)
        window = alpaca_module.TimeWindow(
            aware_et(session_date, "09:30").astimezone(UTC),
            aware_et(session_date, "16:00").astimezone(UTC),
        )
        retrieved_at = aware_et(session_date, "16:20").astimezone(UTC)

        class BarTransport:
            def get(self, url: str, headers: object) -> HttpResponse:
                query = parse_qs(urlsplit(url).query)
                timeframe = query["timeframe"][0]
                timestamp = (
                    "2026-08-17T13:30:00Z"
                    if timeframe == "1Day"
                    else "2026-08-17T20:00:00Z"
                )
                body = json.dumps(
                    {
                        "bars": {
                            "AAPL": [
                                {
                                    "c": "21.50",
                                    "h": "21.60",
                                    "l": "21.30",
                                    "o": "21.40",
                                    "t": timestamp,
                                    "v": 1000,
                                }
                            ]
                        },
                        "next_page_token": None,
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                return HttpResponse(
                    status=200,
                    headers=(("Content-Type", "application/json"),),
                    body=body,
                    url=url,
                )

        client = alpaca_module.AlpacaMarketData(
            BarTransport(),
            credentials(),
            now=lambda: retrieved_at,
        )
        daily_cohort = client.daily_bars(("AAPL",), window)
        minute_cohort = client.historical_minute_bars(("AAPL",), window)
        self.assertTrue(
            alpaca_module.is_issued_provider_fetch_cohort(daily_cohort)
        )
        self.assertTrue(
            alpaca_module.is_issued_provider_fetch_cohort(minute_cohort)
        )

        def observation(cohort: object) -> object:
            fact = cohort["AAPL"][0]
            source = alpaca_module._normalized_market_fact_source(fact)
            return SimpleNamespace(
                observation_id=(
                    "journal:" + source.normalized_fields_digest
                ),
                observation_kind="BAR",
                symbol="AAPL",
                fresh=True,
                bid_micros=None,
                ask_micros=None,
                close_micros=21_500_000,
                source_time=fact.timestamp,
                received_at=retrieved_at,
                provider_source_observation_id=(
                    source.source_observation_id
                ),
                source_item_ordinal=source.source_item_ordinal,
                source_item_path=source.source_item_path,
                page_payload_sha256=source.page_payload_sha256,
                normalized_fields_digest=source.normalized_fields_digest,
            )

        daily_observation = observation(daily_cohort)
        selected, price = risk_module._phase1_review_mark_from_observations(
            (daily_observation,),
            observation_id=daily_observation.observation_id,
            method="DAILY_BAR_CLOSE_HAIRCUT",
            price_micros=21_478_500,
            at=aware_et(session_date, "16:00"),
            session_date=session_date,
            query_cutoff=retrieved_at,
            calendar_resolver=_calendar(),
            daily_bar_cohort=daily_cohort,
        )
        self.assertIs(selected, daily_observation)
        self.assertEqual(price, Decimal("21.4785"))

        minute_observation = observation(minute_cohort)
        for invalid_cohort in (
            minute_cohort,
            copy.copy(daily_cohort),
        ):
            with self.subTest(invalid_cohort=invalid_cohort), self.assertRaisesRegex(
                RiskBlock,
                "PHASE1_EXIT_MARK_MISMATCH",
            ):
                risk_module._phase1_review_mark_from_observations(
                    (minute_observation,),
                    observation_id=minute_observation.observation_id,
                    method="DAILY_BAR_CLOSE_HAIRCUT",
                    price_micros=21_478_500,
                    at=aware_et(session_date, "16:00"),
                    session_date=session_date,
                    query_cutoff=retrieved_at,
                    calendar_resolver=_calendar(),
                    daily_bar_cohort=invalid_cohort,
                )

        incomplete_bar = copy.copy(daily_observation)
        incomplete_review_at = aware_et(date(2026, 8, 17), "15:59")
        incomplete_bar.received_at = incomplete_review_at + timedelta(seconds=1)
        with self.assertRaisesRegex(RiskBlock, "PHASE1_EXIT_MARK_MISMATCH"):
            risk_module._phase1_review_mark_from_observations(
                (incomplete_bar,),
                observation_id=incomplete_bar.observation_id,
                method="DAILY_BAR_CLOSE_HAIRCUT",
                price_micros=21_478_500,
                at=incomplete_review_at,
                session_date=date(2026, 8, 17),
                query_cutoff=incomplete_bar.received_at,
                calendar_resolver=_calendar(),
                daily_bar_cohort=daily_cohort,
            )

    def test_partial_projection_state_is_bound_into_ledger_event_digest(
        self,
    ) -> None:
        signal = LedgerSignal(
            signal_id="2026-08-14:SPY",
            symbol="SPY",
            role="PRIMARY",
            publication_session=_SESSION,
            maximum_entry=Decimal("100.00"),
            recommended_stop=Decimal("97.50"),
            target=Decimal("105.00"),
            planned_shares=10,
            tick_size=Decimal("0.01"),
            trigger_price=Decimal("100.00"),
        )
        event = ledger_module.LedgerEvent(
            ledger_name="CANONICAL",
            signal_id=signal.signal_id,
            lot=ledger_module.LedgerLot(
                price=signal.maximum_entry,
                shares=5,
                at=aware_et(_SESSION, "09:37"),
            ),
            user_confirmed_stop=None,
            decision=ledger_module.ComplianceDecision(
                "COMPLIANT",
                True,
                False,
                (),
            ),
            event_id="canonical:remaining:5",
            recommended_stop=Decimal("103.90"),
            profit_target_taken=True,
        )

        projected = ledger_module._apply_position_event((), signal, event)
        untaken = replace(event, profit_target_taken=False)

        self.assertEqual(len(projected), 1)
        self.assertEqual(projected[0].shares, 5)
        self.assertEqual(projected[0].recommended_stop, Decimal("103.90"))
        self.assertTrue(projected[0].profit_target_taken)
        self.assertNotEqual(
            ledger_module._ledger_event_content_digest(event),
            ledger_module._ledger_event_content_digest(untaken),
        )

    def test_replay_derives_open_remainder_stop_and_target_state_from_partial(
        self,
    ) -> None:
        signal = LedgerSignal(
            signal_id="2026-08-14:SPY",
            symbol="SPY",
            role="PRIMARY",
            publication_session=_SESSION,
            maximum_entry=Decimal("100.00"),
            recommended_stop=Decimal("97.50"),
            target=Decimal("105.00"),
            planned_shares=10,
            tick_size=Decimal("0.01"),
            trigger_price=Decimal("100.00"),
        )
        partial = SimpleNamespace(
            row_id=3,
            signal_id=signal.signal_id,
            event_kind="PARTIAL_EXIT",
            shares=5,
            price_micros=104_900_000,
            recommended_stop_micros=103_900_000,
        )

        stop, target_taken = ledger_module._phase1_open_position_state(
            signal,
            (partial,),
            remaining_shares=5,
        )

        self.assertEqual(stop, Decimal("103.90"))
        self.assertTrue(target_taken)
        for invalid in (
            SimpleNamespace(
                **{
                    **vars(partial),
                    "recommended_stop_micros": 97_500_000,
                }
            ),
            SimpleNamespace(**{**vars(partial), "shares": 4}),
        ):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                RiskBlock,
                "PHASE1_PARTIAL_EXIT_STATE_MISMATCH",
            ):
                ledger_module._phase1_open_position_state(
                    signal,
                    (invalid,),
                    remaining_shares=5,
                )

    def test_raw_phase1_window_evaluation_remains_diagnostic_only(self) -> None:
        self.assertTrue(
            hasattr(validation_module, "is_issued_promotion_decision"),
            "Task 8 must add an identity/source-bound promotion verifier",
        )
        self.assertTrue(
            hasattr(
                validation_module,
                "_issue_phase1_promotion_from_journal_source",
            ),
            "Task 8 must add the narrow Journal-source promotion issuer",
        )
        diagnostic = validation_module.evaluate_phase1(
            diagnostic_phase1_window()
        )

        self.assertTrue(diagnostic.passed)
        self.assertFalse(
            validation_module.is_issued_promotion_decision(diagnostic)
        )
        self.assertFalse(
            hasattr(validation_module, "register_promotion_decision")
        )
        self.assertFalse(
            hasattr(validation_module, "_register_promotion_decision")
        )

    def test_promotion_source_contract_binds_complete_window_material(self) -> None:
        self.assertTrue(
            hasattr(journal_module, "Phase1PublishedSignalDispositionSource"),
            "promotion requires exact terminal-disposition source rows",
        )
        self.assertTrue(
            hasattr(journal_module, "Phase1AdherenceCheckSource"),
            "promotion requires exact fixed-checklist source rows",
        )
        required_fields = {
            "validation_window_id",
            "started_session",
            "through_session",
            "starting_capital_micros",
            "started_at",
            "received_at",
            "calendar_digest",
            "expected_open_sessions",
            "signal_sources",
            "disposition_sources",
            "canonical_history",
            "actual_history",
            "adherence_check_sources",
            "adherence_review_sources",
            "query_cutoff",
            "signal_terminal_cursor",
            "signal_source_highwater",
            "lifecycle_terminal_cursor",
            "lifecycle_source_highwater",
            "adherence_terminal_cursor",
            "adherence_source_highwater",
            "expected_signal_count",
            "expected_disposition_count",
            "expected_adherence_count",
            "row_references",
            "source_digest",
        }
        self.assertTrue(
            required_fields.issubset(
                journal_module.Phase1ValidationWindowSource.__dataclass_fields__
            )
        )
        self.assertTrue(hasattr(Journal, "read_phase1_validation_window_source"))
        self.assertTrue(hasattr(Journal, "read_phase1_promotion_decision"))

    def test_adherence_review_contract_derives_atomic_fixed_checklist(self) -> None:
        self.assertTrue(hasattr(journal_module, "Phase1AdherenceReviewSource"))
        self.assertTrue(
            hasattr(journal_module, "Phase1AdherenceCheckEvidenceSource")
        )
        self.assertTrue(hasattr(journal_module, "Phase1AdherenceCheckSource"))
        self.assertTrue(hasattr(Journal, "record_phase1_adherence"))
        self.assertEqual(
            tuple(inspect.signature(Journal.record_phase1_adherence).parameters),
            (
                "self",
                "signal_id",
                "query_cutoff",
                "calendar_resolver",
                "policy",
            ),
        )
        self.assertFalse(hasattr(Journal, "append_phase1_adherence_check"))
        self.assertFalse(hasattr(Journal, "register_phase1_adherence"))
        review_fields = {
            "signal_source",
            "disposition_source",
            "canonical_replay_source",
            "breaker_history_source",
            "actual_action_sources",
            "check_evidence_sources",
            "query_cutoff",
            "calendar_digest",
            "policy_digest",
            "row_references",
            "source_digest",
        }
        self.assertTrue(
            review_fields.issubset(
                journal_module.Phase1AdherenceReviewSource.__dataclass_fields__
            )
        )
        evidence_fields = {
            "check_name",
            "failure_codes",
            "hard_breach_codes",
            "row_references",
            "source_digest",
        }
        self.assertTrue(
            evidence_fields.issubset(
                journal_module.Phase1AdherenceCheckEvidenceSource.__dataclass_fields__
            )
        )
        persisted_fields = {
            "row_id",
            "check_id",
            "validation_window_id",
            "signal_id",
            "check_name",
            "applicable",
            "passed",
            "hard_breach",
            "failure_codes",
            "hard_breach_codes",
            "evidence_digest",
            "authority_digest",
            "evaluated_at",
            "received_at",
            "source_digest",
            "row_reference",
            "row_references",
        }
        self.assertTrue(
            persisted_fields.issubset(
                journal_module.Phase1AdherenceCheckSource.__dataclass_fields__
            )
        )

    def test_adherence_writer_reissues_exact_ten_without_raw_selector(
        self,
    ) -> None:
        self.assertTrue(hasattr(Journal, "read_phase1_adherence_review_source"))
        self.assertTrue(hasattr(Journal, "record_phase1_adherence"))
        self.assertTrue(hasattr(journal_module, "StoredPhase1Adherence"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                first_signal_id, first_cutoff = (
                    _seed_not_triggered_adherence_material(first_journal)
                )
                second_signal_id, second_cutoff = (
                    _seed_not_triggered_adherence_material(second_journal)
                )
                self.assertEqual(first_signal_id, second_signal_id)
                self.assertEqual(first_cutoff, second_cutoff)
                first_source = (
                    first_journal.read_phase1_adherence_review_source(
                        first_signal_id,
                        query_cutoff=first_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                second_source = (
                    second_journal.read_phase1_adherence_review_source(
                        second_signal_id,
                        query_cutoff=second_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                first_authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        first_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertTrue(
                    validation_module.is_issued_phase1_adherence_authority(
                        first_authority
                    )
                )
                self.assertEqual(
                    tuple(check.check_name for check in first_authority.checks),
                    validation_module._PHASE1_ADHERENCE_CHECK_NAMES,
                )
                self.assertEqual(
                    sum(check.applicable for check in first_authority.checks),
                    5,
                )
                self.assertTrue(
                    all(
                        check.passed == check.applicable
                        and not check.hard_breach
                        for check in first_authority.checks
                    )
                )

                stored = first_journal.record_phase1_adherence(
                    first_signal_id,
                    query_cutoff=first_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertFalse(stored.duplicate)
                self.assertEqual(len(stored.check_ids), 10)
                self.assertEqual(
                    stored.authority_digest,
                    first_authority.authority_digest,
                )
                duplicate = first_journal.record_phase1_adherence(
                    first_signal_id,
                    query_cutoff=first_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertTrue(duplicate.duplicate)
                self.assertEqual(duplicate, replace(stored, duplicate=True))
                with self.assertRaises(TypeError):
                    first_journal.record_phase1_adherence(
                        first_signal_id,
                        query_cutoff=first_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        checks=(),
                    )
                self.assertFalse(
                    validation_module.is_issued_phase1_adherence_authority(
                        first_authority
                    )
                )

                current_source = (
                    first_journal.read_phase1_adherence_review_source(
                        first_signal_id,
                        query_cutoff=first_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                current_authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        current_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertIsNot(current_authority, first_authority)
                self.assertEqual(current_authority, first_authority)
                self.assertEqual(
                    current_authority.authority_digest,
                    stored.authority_digest,
                )
                forged_sources = (
                    copy.copy(current_source),
                    replace(current_source),
                    replace(current_source, check_evidence_sources=()),
                    replace(
                        current_source,
                        check_evidence_sources=(
                            *current_source.check_evidence_sources,
                            current_source.check_evidence_sources[-1],
                        ),
                    ),
                    replace(
                        current_source,
                        canonical_replay_source=(
                            second_source.canonical_replay_source
                        ),
                    ),
                )
                for forged in forged_sources:
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_adherence_review_source(
                                forged
                            )
                        )
                        with self.assertRaisesRegex(
                            validation_module.ValidationError,
                            "PHASE1_ADHERENCE_REVIEW_SOURCE_UNVERIFIED",
                        ):
                            validation_module._issue_phase1_adherence_from_journal_source(
                                forged,
                                calendar_resolver=_calendar(),
                                policy=policy_fixture(),
                            )
                self.assertFalse(
                    validation_module.is_issued_phase1_adherence_authority(
                        copy.copy(current_authority)
                    )
                )
                self.assertFalse(
                    validation_module.is_issued_phase1_adherence_authority(
                        replace(current_authority)
                    )
                )

            self.assertFalse(
                validation_module.is_issued_phase1_adherence_authority(
                    current_authority
                )
            )
            with Journal.open(first_path) as restarted:
                restarted_source = (
                    restarted.read_phase1_adherence_review_source(
                        first_signal_id,
                        query_cutoff=first_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                restarted_authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        restarted_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertIsNot(restarted_authority, current_authority)
                self.assertEqual(restarted_authority, current_authority)
                self.assertEqual(
                    restarted_authority.authority_digest,
                    stored.authority_digest,
                )

    def test_adherence_closed_primary_applies_all_ten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.db") as journal:
                signal_id, query_cutoff = (
                    _seed_closed_primary_adherence_material(journal)
                )
                source = journal.read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertTrue(
                    validation_module.is_issued_phase1_adherence_authority(
                        authority
                    )
                )
                self.assertEqual(len(authority.checks), 10)
                self.assertTrue(all(check.applicable for check in authority.checks))
                self.assertTrue(all(check.passed for check in authority.checks))
                self.assertTrue(
                    all(
                        not check.hard_breach
                        and check.failure_codes == ()
                        and check.hard_breach_codes == ()
                        for check in authority.checks
                    )
                )
                stored = journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertFalse(stored.duplicate)
                self.assertEqual(len(stored.check_ids), 10)
                self.assertEqual(journal.count("phase1_adherence_checks"), 10)

    def test_adherence_unbound_actual_entry_propagates_hard_breach(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.db") as journal:
                signal_id, query_cutoff = (
                    _seed_closed_primary_adherence_material(
                        journal,
                        actual_hard_breach=True,
                    )
                )
                source = journal.read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertEqual(len(authority.checks), 10)
                self.assertTrue(all(check.applicable for check in authority.checks))
                completeness_check = next(
                    check
                    for check in authority.checks
                    if check.check_name == "RECORD_COMPLETENESS"
                )
                self.assertFalse(completeness_check.passed)
                self.assertTrue(completeness_check.hard_breach)
                self.assertEqual(
                    completeness_check.failure_codes,
                    ("RECORD_COMPLETENESS_VIOLATION",),
                )
                self.assertEqual(
                    completeness_check.hard_breach_codes,
                    ("RISK_LIMIT_BREACH",),
                )
                stored = journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertFalse(stored.duplicate)
                self.assertEqual(len(stored.check_ids), 10)
                self.assertEqual(journal.count("phase1_adherence_checks"), 10)

    def test_all_not_triggered_global_actual_breach_has_applicable_carrier(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.db") as journal:
                signal_id, query_cutoff = (
                    _seed_not_triggered_adherence_material(
                        journal,
                        actual_hard_breach=True,
                    )
                )
                source = journal.read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                checks = {check.check_name: check for check in authority.checks}
                carrier = checks["RECORD_COMPLETENESS"]
                self.assertTrue(carrier.applicable)
                self.assertFalse(carrier.passed)
                self.assertTrue(carrier.hard_breach)
                self.assertEqual(
                    carrier.failure_codes,
                    ("RECORD_COMPLETENESS_VIOLATION",),
                )
                self.assertEqual(
                    carrier.hard_breach_codes,
                    ("RISK_LIMIT_BREACH",),
                )
                self.assertFalse(
                    checks["POSITION_SIZE_EXPOSURE_AND_RISK"].applicable
                )
                self.assertEqual(
                    checks["POSITION_SIZE_EXPOSURE_AND_RISK"].failure_codes,
                    (),
                )
                self.assertEqual(
                    checks[
                        "POSITION_SIZE_EXPOSURE_AND_RISK"
                    ].hard_breach_codes,
                    (),
                )
                self.assertTrue(
                    all(
                        check.failure_codes == ()
                        and check.hard_breach_codes == ()
                        for check in authority.checks
                        if not check.applicable
                    )
                )
                stored = journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(len(stored.check_ids), 10)
                self.assertTrue(
                    hasattr(Journal, "read_phase1_promotion_decision"),
                    "promotion must consume this persisted hard breach",
                )
                decision = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(decision)
                )
                self.assertFalse(decision.passed)
                self.assertIn("RISK_LIMIT_BREACH", decision.reason_codes)

    def test_shadow_adherence_persists_ten_inapplicable_checks_and_restarts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_id, query_cutoff = _seed_shadow_adherence_material(journal)
                source = journal.read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertEqual(len(authority.checks), 10)
                self.assertTrue(
                    all(
                        not check.applicable
                        and not check.passed
                        and not check.hard_breach
                        and check.failure_codes == ()
                        and check.hard_breach_codes == ()
                        for check in authority.checks
                    )
                )
                stored = journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(len(stored.check_ids), 10)
                self.assertEqual(journal.count("phase1_adherence_checks"), 10)

            self.assertFalse(
                validation_module.is_issued_phase1_adherence_authority(authority)
            )
            with Journal.open(path) as restarted:
                restarted_source = restarted.read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                restarted_authority = (
                    validation_module._issue_phase1_adherence_from_journal_source(
                        restarted_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertIsNot(restarted_authority, authority)
                self.assertEqual(restarted_authority, authority)
                self.assertTrue(
                    validation_module.is_issued_phase1_adherence_authority(
                        restarted_authority
                    )
                )

    def test_session_parameterized_provider_harness_persists_mixed_primaries(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.db") as journal:
                _start_window(journal)
                closed_id, _closed_cutoff = _seed_session_closed_primary(
                    journal,
                    session_date=date(2026, 8, 14),
                    sequence=1,
                )
                not_triggered_id, query_cutoff = (
                    _seed_session_not_triggered_primary(
                        journal,
                        session_date=date(2026, 8, 17),
                        sequence=2,
                    )
                )
                self.assertNotEqual(closed_id, not_triggered_id)
                self.assertEqual(journal.count("phase1_signals"), 2)
                self.assertEqual(journal.count("phase1_closed_trades"), 1)
                self.assertEqual(journal.count("phase1_adherence_checks"), 20)
                self.assertEqual(journal.count("phase1_equity_points"), 6)
                for signal_id, expected_applicable in (
                    (closed_id, 10),
                    (not_triggered_id, 5),
                ):
                    current = journal.read_phase1_adherence_review_source(
                        signal_id,
                        query_cutoff=query_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                    authority = validation_module._issue_phase1_adherence_from_journal_source(
                        current,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                    self.assertEqual(
                        sum(check.applicable for check in authority.checks),
                        expected_applicable,
                    )
                    self.assertTrue(
                        all(
                            check.failure_codes == ()
                            and check.hard_breach_codes == ()
                            for check in authority.checks
                            if not check.applicable
                        )
                    )
                source = journal.read_phase1_validation_window_source(
                    _WINDOW_ID,
                    through_session=date(2026, 8, 17),
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(source.expected_signal_count, 2)
                self.assertEqual(source.expected_disposition_count, 2)
                self.assertEqual(source.expected_adherence_count, 20)
                self.assertEqual(
                    source.expected_open_sessions,
                    (
                        date(2026, 8, 13),
                        date(2026, 8, 14),
                        date(2026, 8, 17),
                    ),
                )
                self.assertEqual(len(source.adherence_review_sources), 2)
                decision = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=date(2026, 8, 17),
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(decision)
                )
                self.assertFalse(decision.passed)
                self.assertIn(
                    "MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET",
                    decision.reason_codes,
                )

    def test_terminal_primary_and_informational_shadow_promote_as_total_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal, candidates_override=_issued_candidates(2))
                publication_cutoff = aware_et(_SESSION, "08:45")
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=publication_cutoff,
                )
                primary = next(
                    source
                    for source in replay_source.signal_sources
                    if source.role == "PRIMARY"
                )
                shadow = next(
                    source
                    for source in replay_source.signal_sources
                    if source.role == "WATCHLIST_SHADOW"
                )
                primary_trigger = risk_module.money_from_micros(
                    primary.trigger_price_micros
                )
                primary_tick = risk_module.money_from_micros(
                    primary.tick_size_micros
                )
                _append_completed_entry_observations(
                    journal,
                    primary,
                    trade_price=primary_trigger - primary_tick,
                )
                shadow_trigger, shadow_quote, shadow_completed = (
                    _append_completed_entry_observations(journal, shadow)
                )
                shadow_authority = journal.record_phase1_entry(
                    shadow.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=shadow_trigger,
                    quote_observation_id=shadow_quote,
                    calendar_resolver=_calendar(),
                    recorded_at=shadow_completed,
                )
                self.assertTrue(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        shadow_authority
                    )
                )
                query_cutoff = aware_et(
                    _calendar().add_sessions(_SESSION, 1),
                    "08:45",
                )
                primary_terminal = journal.record_phase1_unentered_terminal(
                    primary.signal_id,
                    recorded_at=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(primary_terminal.to_status, "NOT_TRIGGERED")
                _record_cash_only_session_mark(
                    journal,
                    session_date=_SESSION,
                    query_cutoff=query_cutoff,
                )
                for signal_id in (primary.signal_id, shadow.signal_id):
                    stored = journal.record_phase1_adherence(
                        signal_id,
                        query_cutoff=query_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                    self.assertEqual(len(stored.check_ids), 10)
                self.assertEqual(journal.count("phase1_canonical_postings"), 0)
                self.assertEqual(journal.count("phase1_closed_trades"), 0)
                source = journal.read_phase1_validation_window_source(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                by_status = {
                    disposition.status: disposition
                    for disposition in source.disposition_sources
                }
                self.assertEqual(
                    set(by_status),
                    {"NOT_TRIGGERED", "SHADOW_FILLED_INFORMATIONAL"},
                )
                shadow_disposition = by_status["SHADOW_FILLED_INFORMATIONAL"]
                self.assertIs(
                    shadow_disposition.shadow_fill_source.signal_source,
                    shadow_disposition.signal_source,
                )
                _shadow_disposition, shadow_trade = (
                    validation_module._phase1_validation_disposition_material(
                        shadow_disposition
                    )
                )
                self.assertEqual(shadow_trade.role, "WATCHLIST_SHADOW")
                self.assertTrue(shadow_trade.triggered)
                self.assertTrue(shadow_trade.filled)
                self.assertFalse(shadow_trade.closed)
                self.assertIsNone(shadow_trade.net_r)
                decision = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(decision)
                )
                self.assertEqual(decision.closed_primary_trades, 0)
                self.assertFalse(decision.passed)

            with Journal.open(path) as restarted:
                restarted_source = restarted.read_phase1_validation_window_source(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                restarted_shadow = next(
                    disposition
                    for disposition in restarted_source.disposition_sources
                    if disposition.status == "SHADOW_FILLED_INFORMATIONAL"
                )
                self.assertIs(
                    restarted_shadow.shadow_fill_source.signal_source,
                    restarted_shadow.signal_source,
                )
                restarted_decision = restarted.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(restarted_decision, decision)
                self.assertIsNot(restarted_decision, decision)

    def test_expired_and_invalidated_promotion_proofs_reuse_outer_signal_identity(
        self,
    ) -> None:
        cases = ("EXPIRED", "INVALIDATED")
        for expected_status in cases:
            with (
                self.subTest(status=expected_status),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "journal.db"
                with Journal.open(path) as journal:
                    if expected_status == "EXPIRED":
                        signal_source = _published_signal_source(journal)
                        proof_at = aware_et(
                            _calendar().add_sessions(_SESSION, 1),
                            "08:45",
                        )
                    else:
                        signal_source, _authority, _stored, proof_at = (
                            _persist_signal_evidence(journal, adverse=True)
                        )
                    terminal = journal.record_phase1_unentered_terminal(
                        signal_source.signal_id,
                        recorded_at=proof_at,
                        calendar_resolver=_calendar(),
                    )
                    self.assertEqual(terminal.to_status, expected_status)
                    query_cutoff = proof_at + timedelta(minutes=1)
                    disposition = (
                        journal.read_phase1_published_signal_disposition_source(
                            signal_source.signal_id,
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                        )
                    )
                    proof = (
                        disposition.expiry_deadline_source
                        if expected_status == "EXPIRED"
                        else disposition.signal_evidence_source
                    )
                    self.assertIsNotNone(proof)
                    self.assertEqual(
                        disposition.signal_source.query_cutoff,
                        query_cutoff,
                    )
                    self.assertEqual(proof.query_cutoff, proof_at)
                    self.assertTrue(
                        proof.signal_source is disposition.signal_source,
                        "terminal proof must reuse the outer signal identity",
                    )
                    if expected_status == "EXPIRED":
                        early_signal = journal._read_phase1_signal_source(
                            signal_source.signal_id,
                            query_cutoff=signal_source.received_at,
                        )
                        with self.assertRaisesRegex(
                            journal_module.InvalidJournalValue,
                            "nested signal source is unverified",
                        ):
                            journal._phase1_nested_signal_sources(
                                signal_source.signal_id,
                                query_cutoff=proof_at,
                                exact_signal_source=early_signal,
                            )
                    if expected_status == "INVALIDATED":
                        with self.assertRaisesRegex(
                            journal_module.InvalidJournalValue,
                            "nested signal source is unverified",
                        ):
                            journal._phase1_nested_signal_sources(
                                signal_source.signal_id,
                                query_cutoff=proof_at,
                                exact_signal_source=replace(
                                    disposition.signal_source
                                ),
                            )
                        issue_authority = (
                            risk_module._issue_phase1_signal_evidence_authority_from_source
                        )
                        authority = issue_authority(
                            proof,
                            calendar_resolver=_calendar(),
                        )
                        self.assertTrue(
                            risk_module.is_issued_phase1_signal_evidence_authority(
                                authority
                            )
                        )
                        self.assertIs(
                            authority.signal_source,
                            disposition.signal_source,
                        )
                    promoted, trade = (
                        validation_module._phase1_validation_disposition_material(
                            disposition
                        )
                    )
                    self.assertEqual(promoted.status.value, expected_status)
                    self.assertFalse(trade.closed)

                with Journal.open(path) as restarted:
                    disposition = (
                        restarted.read_phase1_published_signal_disposition_source(
                            signal_source.signal_id,
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                        )
                    )
                    proof = (
                        disposition.expiry_deadline_source
                        if expected_status == "EXPIRED"
                        else disposition.signal_evidence_source
                    )
                    self.assertIsNotNone(proof)
                    self.assertEqual(
                        disposition.signal_source.query_cutoff,
                        query_cutoff,
                    )
                    self.assertEqual(proof.query_cutoff, proof_at)
                    self.assertTrue(
                        proof.signal_source is disposition.signal_source,
                        "restarted proof must reuse the outer signal identity",
                    )
                    validation_module._phase1_validation_disposition_material(
                        disposition
                    )

    def test_promotion_passes_twenty_source_backed_primaries_over_28_days(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.db") as journal:
                _start_window(journal)
                sessions = tuple(
                    _calendar().add_sessions(_SESSION, offset)
                    for offset in range(20)
                )
                cutoffs: list[datetime] = []
                for sequence, session_date in enumerate(sessions, start=1):
                    _signal_id, cutoff = _seed_session_closed_primary(
                        journal,
                        session_date=session_date,
                        sequence=sequence,
                    )
                    cutoffs.append(cutoff)

                nineteen = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=sessions[-2],
                    query_cutoff=cutoffs[-2],
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(nineteen)
                )
                self.assertFalse(nineteen.passed)
                self.assertEqual(nineteen.closed_primary_trades, 19)
                self.assertIn(
                    "MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET",
                    nineteen.reason_codes,
                )

                source = journal.read_phase1_validation_window_source(
                    _WINDOW_ID,
                    through_session=sessions[-1],
                    query_cutoff=cutoffs[-1],
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_validation_window_source(
                        source
                    )
                )
                self.assertEqual(source.expected_signal_count, 20)
                self.assertEqual(source.expected_disposition_count, 20)
                self.assertEqual(source.expected_adherence_count, 200)
                self.assertEqual(len(source.adherence_review_sources), 20)
                self.assertEqual(len(source.expected_open_sessions), 21)
                self.assertEqual(source.expected_open_sessions[0], date(2026, 8, 13))
                self.assertEqual(source.expected_open_sessions[-1], sessions[-1])
                self.assertGreaterEqual(
                    (sessions[-1] - date(2026, 8, 13)).days,
                    28,
                )
                decision = validation_module._issue_phase1_promotion_from_journal_source(
                    source,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(decision)
                )
                self.assertTrue(decision.passed)
                self.assertEqual(decision.status.value, "PASSED")
                self.assertEqual(decision.reason_codes, ())
                self.assertEqual(decision.closed_primary_trades, 20)
                self.assertGreater(decision.mean_net_r, Decimal("0"))
                self.assertGreaterEqual(decision.adherence, Decimal("0.90"))

                for forged in (
                    replace(
                        source,
                        adherence_check_sources=(
                            source.adherence_check_sources[:-1]
                        ),
                    ),
                    replace(
                        source,
                        adherence_review_sources=(
                            source.adherence_review_sources[:-1]
                        ),
                    ),
                ):
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_validation_window_source(
                                forged
                            )
                        )
                        with self.assertRaises(
                            validation_module.ValidationError
                        ):
                            validation_module._issue_phase1_promotion_from_journal_source(
                                forged,
                                calendar_resolver=_calendar(),
                            )

    def test_promotion_authority_reissues_new_identity_after_restart(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "read_phase1_validation_window_source")
        )
        self.assertTrue(hasattr(Journal, "read_phase1_promotion_decision"))
        self.assertTrue(
            hasattr(
                validation_module,
                "_issue_phase1_promotion_from_journal_source",
            )
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            through_session = _SESSION
            with Journal.open(path) as first_journal:
                signal_id, cutoff = _seed_not_triggered_adherence_material(
                    first_journal
                )
                stored = first_journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(len(stored.check_ids), 10)
                first_source = (
                    first_journal.read_phase1_validation_window_source(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )
                first_decision = first_journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=through_session,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_validation_window_source(
                        first_source
                    )
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(
                        first_decision
                    )
                )

            self.assertFalse(
                journal_module.is_verified_phase1_validation_window_source(
                    first_source
                )
            )
            self.assertFalse(
                validation_module.is_issued_promotion_decision(first_decision)
            )

            with Journal.open(path) as second_journal:
                second_source = (
                    second_journal.read_phase1_validation_window_source(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )
                second_decision = (
                    second_journal.read_phase1_promotion_decision(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )

                self.assertIsNot(first_source, second_source)
                self.assertIsNot(first_decision, second_decision)
                self.assertEqual(first_source, second_source)
                self.assertEqual(first_decision, second_decision)
                self.assertEqual(
                    first_source.source_digest,
                    second_source.source_digest,
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_validation_window_source(
                        second_source
                    )
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(
                        second_decision
                    )
                )
                for forged in (
                    copy.copy(second_source),
                    replace(second_source),
                    replace(second_source, signal_sources=()),
                    replace(second_source, disposition_sources=()),
                    replace(
                        second_source,
                        adherence_check_sources=(
                            second_source.adherence_check_sources[:-1]
                        ),
                    ),
                    replace(
                        second_source,
                        row_references=second_source.row_references[:-1],
                    ),
                    replace(
                        second_source,
                        query_cutoff=(
                            second_source.query_cutoff
                            + timedelta(microseconds=1)
                        ),
                    ),
                ):
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_validation_window_source(
                                forged
                            )
                        )
                        with self.assertRaises(
                            (validation_module.ValidationError, RiskBlock)
                        ):
                            validation_module._issue_phase1_promotion_from_journal_source(
                                forged,
                                calendar_resolver=_calendar(),
                            )
                self.assertFalse(
                    validation_module.is_issued_promotion_decision(
                        copy.copy(second_decision)
                    )
                )
                self.assertFalse(
                    validation_module.is_issued_promotion_decision(
                        replace(second_decision)
                    )
                )
                with self.assertRaises(journal_module.InvalidJournalValue):
                    second_journal.read_phase1_validation_window_source(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff - timedelta(microseconds=1),
                        calendar_resolver=_calendar(),
                    )

    def test_promotion_authority_rejects_cross_journal_window_splice(self) -> None:
        self.assertTrue(
            hasattr(Journal, "read_phase1_validation_window_source")
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            through_session = _SESSION
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                first_signal_id, first_cutoff = (
                    _seed_not_triggered_adherence_material(first_journal)
                )
                second_signal_id, second_cutoff = (
                    _seed_not_triggered_adherence_material(second_journal)
                )
                self.assertEqual(first_cutoff, second_cutoff)
                cutoff = first_cutoff
                first_journal.record_phase1_adherence(
                    first_signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                second_journal.record_phase1_adherence(
                    second_signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                first_source = (
                    first_journal.read_phase1_validation_window_source(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )
                second_source = (
                    second_journal.read_phase1_validation_window_source(
                        _WINDOW_ID,
                        through_session=through_session,
                        query_cutoff=cutoff,
                        calendar_resolver=_calendar(),
                    )
                )
                splice = replace(
                    first_source,
                    actual_history=second_source.actual_history,
                )

                self.assertFalse(
                    journal_module.is_verified_phase1_validation_window_source(
                        splice
                    )
                )
                with self.assertRaises(
                    (validation_module.ValidationError, RiskBlock)
                ):
                    validation_module._issue_phase1_promotion_from_journal_source(
                        splice,
                        calendar_resolver=_calendar(),
                    )

    def test_promotion_rechecks_current_adherence_after_late_actual_entry(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_id, historical_cutoff = (
                    _seed_not_triggered_adherence_material(journal)
                )
                journal.record_phase1_adherence(
                    signal_id,
                    query_cutoff=historical_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                historical = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=historical_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(historical)
                )
                self.assertNotIn("RISK_LIMIT_BREACH", historical.reason_codes)

                action_session = _calendar().add_sessions(_SESSION, 1)
                for ordinal, (at, text) in enumerate(
                    (
                        (
                            "10:14",
                            f"BOUGHT {_signal().symbol} 1 shares @ 20 AT 10:14 ET",
                        ),
                        (
                            "15:31",
                            f"SOLD {_signal().symbol} 1 shares @ 21 AT 15:31 ET",
                        ),
                    ),
                    start=1,
                ):
                    message_time = aware_et(action_session, at)
                    ingest_confirmation(
                        journal,
                        ConfirmationEnvelope(
                            message_id=f"phase1-promotion-late-{ordinal}",
                            message_time=message_time,
                            received_at=message_time + timedelta(microseconds=1),
                            text=text,
                            session_date=action_session,
                        ),
                        plans=UnavailableSignalPlanResolver(),
                        calendar=_calendar(),
                        policy=policy_fixture(),
                        entry_authorities=(
                            UnavailableActualEntryAuthorityResolver()
                        ),
                    )
                current_cutoff = aware_et(action_session, "15:32")
                historical_again = journal.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=historical_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertIsNot(historical_again, historical)
                self.assertEqual(historical_again, historical)
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(
                        historical_again
                    )
                )
                current_source = journal.read_phase1_validation_window_source(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=current_cutoff,
                    calendar_resolver=_calendar(),
                )
                with self.assertRaisesRegex(
                    validation_module.ValidationError,
                    "PHASE1_VALIDATION_ADHERENCE_MISMATCH",
                ):
                    validation_module._issue_phase1_promotion_from_journal_source(
                        current_source,
                        calendar_resolver=_calendar(),
                    )
                with self.assertRaisesRegex(
                    validation_module.ValidationError,
                    "PHASE1_VALIDATION_ADHERENCE_MISMATCH",
                ):
                    journal.read_phase1_promotion_decision(
                        _WINDOW_ID,
                        through_session=_SESSION,
                        query_cutoff=current_cutoff,
                        calendar_resolver=_calendar(),
                    )

            with Journal.open(path) as restarted:
                restarted_historical = restarted.read_phase1_promotion_decision(
                    _WINDOW_ID,
                    through_session=_SESSION,
                    query_cutoff=historical_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(restarted_historical, historical)
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(
                        restarted_historical
                    )
                )
                with self.assertRaisesRegex(
                    validation_module.ValidationError,
                    "PHASE1_VALIDATION_ADHERENCE_MISMATCH",
                ):
                    restarted.read_phase1_promotion_decision(
                        _WINDOW_ID,
                        through_session=_SESSION,
                        query_cutoff=current_cutoff,
                        calendar_resolver=_calendar(),
                    )

    def test_publication_coordinator_issues_complete_one_and_two_name_cohorts(
        self,
    ) -> None:
        for candidate_count in (1, 2):
            with self.subTest(candidate_count=candidate_count):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _start_window(journal)
                        chain = _publication_authority_chain(
                            journal,
                            candidate_count=candidate_count,
                        )
                        candidates, plan, decision = chain[-3:]

                        self.assertTrue(
                            screening_module.is_issued_publication_decision(
                                decision
                            )
                        )
                        self.assertTrue(
                            screening_module.is_issued_publication_decision_for_plan(
                                decision,
                                plan,
                            )
                        )
                        self.assertEqual(
                            tuple(item.candidate for item in decision.candidates),
                            screening_module.rank_candidates(candidates),
                        )
                        self.assertEqual(
                            tuple(item.role for item in decision.candidates),
                            (
                                "PRIMARY",
                                *("WATCHLIST_SHADOW",) * (candidate_count - 1),
                            ),
                        )

    def test_empty_authority_chain_reissues_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as first_journal:
                _start_window(first_journal)
                first = _empty_authority_chain(first_journal)
                (
                    first_replay_source,
                    first_replay,
                    first_history_source,
                    first_history,
                    first_breaker,
                    first_portfolio,
                ) = first
                self.assertTrue(first_replay.source_verified)
                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(
                        first_replay.cohort
                    )
                )
                self.assertTrue(first_replay.ledger_pair.canonical_replay_verified)
                self.assertTrue(
                    risk_module.is_issued_breaker_history_authority(first_history)
                )
                self.assertTrue(risk_module.is_issued_breaker_state(first_breaker))
                self.assertTrue(
                    risk_module.is_issued_portfolio_risk_authority(first_portfolio)
                )

            self.assertFalse(
                journal_module.is_verified_phase1_canonical_replay_source(
                    first_replay_source
                )
            )
            self.assertFalse(
                journal_module.is_verified_phase1_breaker_history_source(
                    first_history_source
                )
            )
            self.assertFalse(first_replay.source_verified)
            self.assertFalse(
                ledger_module.is_issued_verified_replay_cohort(first_replay.cohort)
            )
            self.assertFalse(first_replay.ledger_pair.canonical_replay_verified)
            self.assertFalse(
                risk_module.is_issued_breaker_history_authority(first_history)
            )
            self.assertFalse(risk_module.is_issued_breaker_state(first_breaker))
            self.assertFalse(
                risk_module.is_issued_portfolio_risk_authority(first_portfolio)
            )

            with Journal.open(path) as second_journal:
                second = _empty_authority_chain(second_journal)
                (
                    second_replay_source,
                    second_replay,
                    second_history_source,
                    second_history,
                    second_breaker,
                    second_portfolio,
                ) = second
                for old, new in zip(first, second, strict=True):
                    self.assertIsNot(old, new)
                self.assertEqual(
                    first_replay_source.source_digest,
                    second_replay_source.source_digest,
                )
                self.assertEqual(first_replay.canonical_cash, second_replay.canonical_cash)
                self.assertEqual(
                    first_replay.settled_buying_power,
                    second_replay.settled_buying_power,
                )
                self.assertEqual(first_replay.cohort, second_replay.cohort)
                self.assertEqual(
                    first_history_source.source_digest,
                    second_history_source.source_digest,
                )
                self.assertEqual(first_history, second_history)
                self.assertEqual(first_breaker, second_breaker)
                self.assertEqual(
                    first_portfolio.projection_digest,
                    second_portfolio.projection_digest,
                )
                self.assertEqual(
                    first_portfolio.settlement_digest,
                    second_portfolio.settlement_digest,
                )
                self.assertTrue(
                    risk_module.is_issued_portfolio_risk_authority(
                        second_portfolio
                    )
                )

    def test_portfolio_rejects_cross_journal_and_cross_window_splices(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                _start_window(first_journal, window_id="1" * 64)
                _start_window(second_journal, window_id="2" * 64)
                first = _empty_authority_chain(first_journal)
                second = _empty_authority_chain(second_journal)
                first_replay = first[1]
                second_breaker = second[4]
                request = LongPlanRequest(
                    entry=Decimal("100"),
                    stop=Decimal("97.50"),
                    tick_size=Decimal("0.01"),
                    session_date=_SESSION,
                    symbol="SPY",
                    published_target=Decimal("105"),
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_SOURCE_LINEAGE_MISMATCH",
                ):
                    risk_module._issue_portfolio_risk_authority(
                        request=request,
                        ledger_pair=first_replay.ledger_pair,
                        ledger_name="CANONICAL",
                        breaker_state=second_breaker,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        scope="CANONICAL_PUBLICATION",
                        as_of=aware_et(_SESSION, "08:45"),
                        phase1_canonical_replay=first_replay,
                    )

    def test_publication_coordinator_rejects_empty_or_raw_cohorts(
        self,
    ) -> None:
        issued_primary = screening_module.to_scored_candidate(
            candidate_context(evidence=evidence(age_days=5))
        )
        diagnostic_plan = authorized_plan(
            LongPlanRequest.from_scored_candidate(issued_primary)
        )
        with self.assertRaisesRegex(
            screening_module.ScreeningError,
            "one through three",
        ):
            screening_module._issue_portfolio_bound_publication_decision(
                (),
                primary_plan_decision=diagnostic_plan,
            )
        with self.assertRaisesRegex(
            screening_module.ScreeningError,
            "identity-issued",
        ):
            screening_module._issue_portfolio_bound_publication_decision(
                (
                    issued_primary,
                    replace(issued_primary, symbol="QQQ"),
                    replace(issued_primary, symbol="XLK"),
                ),
                primary_plan_decision=diagnostic_plan,
            )
        self.assertFalse(hasattr(Journal, "publish_phase1_signal"))

    def test_no_field_only_registrar_can_mint_phase1_authority(self) -> None:
        forbidden = {
            ledger_module: (
                "register_ledger_signal",
                "_register_ledger_signal",
                "register_paper_entry_authority",
                "_register_paper_entry_authority",
                "register_verified_replay_cohort",
                "_register_verified_replay_cohort",
            ),
            risk_module: (
                "register_breaker_history_authority",
                "_register_breaker_history_authority",
                "register_portfolio_risk_authority",
                "_register_portfolio_risk_authority",
            ),
        }
        for module, names in forbidden.items():
            for name in names:
                with self.subTest(module=module.__name__, name=name):
                    self.assertFalse(hasattr(module, name))

        direct = _signal()
        self.assertFalse(ledger_module.is_issued_ledger_signal(direct))
        self.assertFalse(hasattr(Journal, "append_phase1_equity_point"))
        self.assertFalse(hasattr(Journal, "register_phase1_equity_point"))

    def test_restart_reissues_new_signal_identity_with_identical_value(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            cutoff = aware_et(_SESSION, "10:00")
            with Journal.open(path) as journal:
                _publish(journal)
                first = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                self.assertTrue(ledger_module.is_issued_ledger_signal(first))

            self.assertFalse(ledger_module.is_issued_ledger_signal(first))
            with Journal.open(path) as journal:
                second = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                self.assertIsNot(first, second)
                self.assertEqual(first, second)
                self.assertTrue(ledger_module.is_issued_ledger_signal(second))
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(copy.copy(second))
                )
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(replace(second))
                )

    def test_entry_adapter_rejects_cross_journal_source_signal_splice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                _, cutoff = _seed_completed_authority_fill(first_journal)
                _, second_cutoff = _seed_completed_authority_fill(second_journal)
                self.assertEqual(second_cutoff, cutoff)
                first_signal = first_journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                second_signal = second_journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                first_source = first_journal._read_phase1_entry_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )

                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_SIGNAL_SOURCE_MISMATCH",
                ):
                    ledger_module._issue_paper_entry_authority_from_phase1_source(
                        first_source,
                        signal=second_signal,
                        calendar_resolver=_calendar(),
                    )
                issued = (
                    ledger_module._issue_paper_entry_authority_from_phase1_source(
                        first_source,
                        signal=first_signal,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertTrue(
                    ledger_module.is_issued_paper_entry_authority(issued)
                )
                self.assertTrue(ledger_module.is_issued_ledger_signal(first_signal))
                self.assertTrue(ledger_module.is_issued_ledger_signal(second_signal))

    def test_direct_live_entry_paths_issue_exactly_one_canonical_buy(self) -> None:
        self.assertTrue(
            hasattr(Journal, "record_phase1_entry"),
            "Task 8 must expose the one typed entry writer",
        )
        self.assertFalse(hasattr(Journal, "record_phase1_paper_fill"))
        self.assertNotIn(
            "event_kind",
            inspect.signature(Journal.record_phase1_entry).parameters,
        )
        for event_kind in ("LIVE_CONFIRM", "LIVE_SKIP"):
            with self.subTest(event_kind=event_kind):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _publish(journal)
                        initial_source = (
                            journal._read_phase1_canonical_replay_source(
                                query_cutoff=aware_et(_SESSION, "08:45"),
                            )
                        )
                        signal_source = initial_source.signal_sources[0]
                        trigger_id, quote_id, completed_at = (
                            _append_completed_entry_observations(
                                journal,
                                signal_source,
                            )
                        )
                        action_source, recorded_at = _confirmation_action_source(
                            journal,
                            signal_source,
                            event_kind=event_kind,
                            after=completed_at,
                        )
                        self.assertFalse(
                            is_verified_journal_action_source(
                                replace(action_source)
                            )
                        )
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "PHASE1_CONFIRMATION_SOURCE_UNVERIFIED",
                        ):
                            journal.record_phase1_entry(
                                signal_source.signal_id,
                                confirmation_action_source=replace(action_source),
                                trigger_observation_id=trigger_id,
                                quote_observation_id=quote_id,
                                calendar_resolver=_calendar(),
                                recorded_at=recorded_at,
                            )
                        with tempfile.TemporaryDirectory() as other_directory:
                            with Journal.open(
                                Path(other_directory) / "other.db"
                            ) as other_journal:
                                cross_source, _ = _confirmation_action_source(
                                    other_journal,
                                    signal_source,
                                    event_kind=event_kind,
                                    after=completed_at,
                                )
                                with self.assertRaisesRegex(
                                    RiskBlock,
                                    "PHASE1_CONFIRMATION_SOURCE_UNVERIFIED",
                                ):
                                    journal.record_phase1_entry(
                                        signal_source.signal_id,
                                        confirmation_action_source=cross_source,
                                        trigger_observation_id=trigger_id,
                                        quote_observation_id=quote_id,
                                        calendar_resolver=_calendar(),
                                        recorded_at=recorded_at,
                                    )

                        authority = journal.record_phase1_entry(
                            signal_source.signal_id,
                            confirmation_action_source=action_source,
                            trigger_observation_id=trigger_id,
                            quote_observation_id=quote_id,
                            calendar_resolver=_calendar(),
                            recorded_at=recorded_at,
                        )
                        replay_source = (
                            journal._read_phase1_canonical_replay_source(
                                query_cutoff=recorded_at,
                            )
                        )
                        replay = (
                            ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                                replay_source
                            )
                        )

                        self.assertTrue(
                            ledger_module.is_issued_paper_entry_authority(
                                authority
                            )
                        )
                        self.assertEqual(len(replay_source.entry_sources), 1)
                        self.assertEqual(
                            tuple(
                                posting.entry_kind
                                for posting in replay_source.postings
                            ).count("BUY"),
                            1,
                        )
                        self.assertEqual(
                            tuple(
                                event.event_kind
                                for event in replay_source.lifecycle_events
                            ).count(event_kind),
                            1,
                        )
                        self.assertEqual(
                            replay.ledger_pair.canonical.open_positions[0].shares,
                            signal_source.planned_shares,
                        )

    def test_paper_to_live_metadata_transition_cannot_duplicate_buy(self) -> None:
        self.assertTrue(hasattr(Journal, "record_phase1_entry"))
        for event_kind in ("LIVE_CONFIRM", "LIVE_SKIP"):
            with self.subTest(event_kind=event_kind):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _publish(journal)
                        initial_source = (
                            journal._read_phase1_canonical_replay_source(
                                query_cutoff=aware_et(_SESSION, "08:45"),
                            )
                        )
                        signal_source = initial_source.signal_sources[0]
                        trigger_id, quote_id, completed_at = (
                            _append_completed_entry_observations(
                                journal,
                                signal_source,
                            )
                        )
                        authority = journal.record_phase1_entry(
                            signal_source.signal_id,
                            confirmation_action_source=None,
                            trigger_observation_id=trigger_id,
                            quote_observation_id=quote_id,
                            calendar_resolver=_calendar(),
                            recorded_at=completed_at,
                        )
                        action_source, recorded_at = _confirmation_action_source(
                            journal,
                            signal_source,
                            event_kind=event_kind,
                            after=completed_at,
                        )
                        before = journal.count("phase1_canonical_postings")
                        with self.assertRaises(
                            journal_module.InvalidJournalValue
                        ):
                            journal.record_phase1_entry(
                                signal_source.signal_id,
                                confirmation_action_source=action_source,
                                trigger_observation_id=trigger_id,
                                quote_observation_id=quote_id,
                                calendar_resolver=_calendar(),
                                recorded_at=recorded_at,
                            )
                        self.assertEqual(
                            journal.count("phase1_canonical_postings"),
                            before,
                        )

                        action_source = _reread_confirmation_action_source(
                            journal,
                            action_source,
                        )
                        updated_authority = journal.record_phase1_entry(
                            signal_source.signal_id,
                            confirmation_action_source=action_source,
                            trigger_observation_id=None,
                            quote_observation_id=None,
                            calendar_resolver=_calendar(),
                            recorded_at=recorded_at,
                        )
                        replay_source = (
                            journal._read_phase1_canonical_replay_source(
                                query_cutoff=recorded_at,
                            )
                        )

                        self.assertFalse(
                            ledger_module.is_issued_paper_entry_authority(authority)
                        )
                        self.assertTrue(
                            ledger_module.is_issued_paper_entry_authority(
                                updated_authority
                            )
                        )
                        self.assertEqual(len(replay_source.entry_sources), 1)
                        self.assertEqual(
                            tuple(
                                posting.entry_kind
                                for posting in replay_source.postings
                            ).count("BUY"),
                            1,
                        )
                        self.assertEqual(
                            tuple(
                                event.event_kind
                                for event in replay_source.lifecycle_events
                            ).count(event_kind),
                            1,
                        )

    def test_watchlist_shadow_records_only_informational_fill_authority(self) -> None:
        self.assertTrue(hasattr(Journal, "record_phase1_entry"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                publication_source, decision, plan, _lineage = (
                    _issued_publication(journal, candidate_count=2)
                )
                journal.publish_phase1_report(
                    publication_source=publication_source,
                    decision=decision,
                    primary_plan_decision=plan,
                    validation_window_id=_WINDOW_ID,
                    calendar_resolver=_calendar(),
                    received_at=aware_et(_SESSION, "08:45"),
                )
                initial_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                shadow_source = initial_source.signal_sources[1]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        shadow_source,
                    )
                )
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                    journal.count("phase1_closed_trades"),
                )
                authority = journal.record_phase1_entry(
                    shadow_source.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                self.assertTrue(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        authority
                    )
                )
                self.assertFalse(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        replace(authority)
                    )
                )
                self.assertEqual(authority.signal_id, shadow_source.signal_id)
                self.assertEqual(authority.trigger_observation_id, trigger_id)
                self.assertEqual(authority.quote_observation_id, quote_id)
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                        journal.count("phase1_closed_trades"),
                    ),
                    (before[0] + 2, before[1], before[2]),
                )

                for event_kind in ("LIVE_CONFIRM", "LIVE_SKIP"):
                    with self.subTest(event_kind=event_kind):
                        action_source, recorded_at = (
                            _confirmation_action_source(
                                journal,
                                shadow_source,
                                event_kind=event_kind,
                                after=completed_at,
                            )
                        )
                        phase1_counts = (
                            journal.count("phase1_signal_events"),
                            journal.count("phase1_canonical_postings"),
                            journal.count("phase1_closed_trades"),
                        )
                        action_source = _reread_confirmation_action_source(
                            journal,
                            action_source,
                        )
                        with self.assertRaises(
                            journal_module.InvalidJournalValue
                        ):
                            journal.record_phase1_entry(
                                shadow_source.signal_id,
                                confirmation_action_source=action_source,
                                trigger_observation_id=None,
                                quote_observation_id=None,
                                calendar_resolver=_calendar(),
                                recorded_at=recorded_at,
                            )
                        self.assertEqual(
                            (
                                journal.count("phase1_signal_events"),
                                journal.count("phase1_canonical_postings"),
                                journal.count("phase1_closed_trades"),
                            ),
                            phase1_counts,
                        )

                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=recorded_at,
                )
                replay = (
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                        replay_source
                    )
                )
                self.assertEqual(replay_source.entry_sources, ())
                self.assertEqual(replay.ledger_pair.canonical.open_positions, ())
                self.assertEqual(
                    tuple(
                        event.event_kind
                        for event in replay_source.lifecycle_events
                        if event.signal_id == shadow_source.signal_id
                    ),
                    ("PUBLISHED", "TRIGGER_OBSERVED", "SHADOW_FILL"),
                )
                source = journal._read_phase1_shadow_fill_source(
                    shadow_source.signal_id,
                    query_cutoff=recorded_at,
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_SHADOW_FILL_SOURCE_UNVERIFIED",
                ):
                    ledger_module._issue_shadow_fill_disposition_from_phase1_source(
                        replace(source),
                        calendar_resolver=_calendar(),
                    )

            self.assertFalse(
                ledger_module.is_issued_shadow_fill_disposition_authority(authority)
            )
            with Journal.open(path) as restarted:
                restarted_authority = restarted.read_phase1_shadow_fill(
                    shadow_source.signal_id,
                    query_cutoff=recorded_at,
                    calendar_resolver=_calendar(),
                )
                self.assertIsNot(restarted_authority, authority)
                self.assertEqual(restarted_authority, authority)
                self.assertTrue(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        restarted_authority
                    )
                )

    def test_raw_shadow_fill_disposition_cannot_mint_authority(self) -> None:
        self.assertTrue(
            hasattr(ledger_module, "ShadowFillDispositionAuthority"),
            "Task 8 must expose a typed informational shadow-fill result",
        )
        raw = ledger_module.ShadowFillDispositionAuthority(
            signal_id="2026-08-14:QQQ",
            lifecycle_event_id="phase1-shadow-fill:2026-08-14:QQQ",
            trigger_observation_id="trade:QQQ:2026-08-14:trigger",
            quote_observation_id="quote:QQQ:2026-08-14:fill",
            trigger_at=aware_et(_SESSION, "09:36"),
            filled_at=aware_et(_SESSION, "09:37"),
            fill_price=Decimal("100.10"),
            source_digest="a" * 64,
            session_complete_digest="b" * 64,
            calendar_digest="c" * 64,
            lifecycle_cursor=1,
            action_ordinal=2,
        )

        self.assertFalse(
            ledger_module.is_issued_shadow_fill_disposition_authority(raw)
        )
        self.assertFalse(
            ledger_module.is_issued_shadow_fill_disposition_authority(
                copy.copy(raw)
            )
        )
        self.assertFalse(
            hasattr(ledger_module, "register_shadow_fill_disposition_authority")
        )
        self.assertFalse(
            hasattr(ledger_module, "_register_shadow_fill_disposition_authority")
        )

    def test_entry_adapter_rejects_copied_subset_and_lookahead_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _, completion_at = _seed_completed_authority_fill(journal)
                signal = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=completion_at,
                )
                source = journal._read_phase1_entry_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=completion_at,
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_entry_source(source)
                )
                for forged in (
                    copy.copy(source),
                    replace(source),
                    replace(source, observations=source.observations[:-1]),
                ):
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_entry_source(forged)
                        )
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "PHASE1_ENTRY_SOURCE_UNVERIFIED",
                        ):
                            ledger_module._issue_paper_entry_authority_from_phase1_source(
                                forged,
                                signal=signal,
                                calendar_resolver=_calendar(),
                            )

                with self.assertRaisesRegex(RiskBlock, "PHASE1_SOURCE_LOOKAHEAD"):
                    journal._read_phase1_entry_source(  # type: ignore[attr-defined]
                        _signal().signal_id,
                        query_cutoff=completion_at - timedelta(microseconds=1),
                    )

    def test_entry_adapter_rejects_diagnostic_calendar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _, cutoff = _seed_completed_authority_fill(journal)
                signal = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                source = journal._read_phase1_entry_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                diagnostic = SessionCalendarResolver.for_diagnostics(
                    _calendar().calendars
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "CALENDAR_RELEASE_AUTHORITY_UNVERIFIED",
                ):
                    ledger_module._issue_paper_entry_authority_from_phase1_source(
                        source,
                        signal=signal,
                        calendar_resolver=diagnostic,
                    )

    def test_commit_revokes_signal_entry_cohort_and_pair_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, cutoff = _seed_completed_authority_fill(journal)
                signal = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=cutoff,
                )
                replay = journal.read_phase1_canonical_replay(  # type: ignore[attr-defined]
                    query_cutoff=cutoff,
                )
                self.assertTrue(ledger_module.is_issued_ledger_signal(signal))
                self.assertTrue(
                    ledger_module.is_issued_paper_entry_authority(authority)
                )
                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(replay.cohort)
                )
                self.assertTrue(replay.ledger_pair.canonical_replay_verified)

                _append_unrelated_source(journal, suffix="new-generation")

                self.assertFalse(ledger_module.is_issued_ledger_signal(signal))
                self.assertFalse(
                    ledger_module.is_issued_paper_entry_authority(authority)
                )
                self.assertFalse(
                    ledger_module.is_issued_verified_replay_cohort(replay.cohort)
                )
                self.assertFalse(replay.ledger_pair.canonical_replay_verified)

    def test_exit_execution_uses_exact_bar_and_fresh_quote_conservatively(
        self,
    ) -> None:
        observed_at = aware_et(date(2026, 8, 17), "15:50")

        def observation(
            observation_id: str,
            kind: str,
            *,
            ordinal: int,
            fresh: bool = True,
            bid: int | None = None,
            ask: int | None = None,
            open_price: int | None = None,
            high: int | None = None,
            low: int | None = None,
            close: int | None = None,
        ) -> SimpleNamespace:
            return SimpleNamespace(
                observation_id=observation_id,
                stream_id=f"{kind.lower()}:SIP:AAPL",
                feed="SIP",
                observation_kind=kind,
                source_time=(
                    observed_at - timedelta(seconds=1)
                    if kind == "QUOTE"
                    else observed_at
                ),
                received_at=observed_at + timedelta(seconds=1),
                cohort_ordinal=ordinal,
                fresh=fresh,
                trade_price_micros=None,
                bid_micros=bid,
                ask_micros=ask,
                open_micros=open_price,
                high_micros=high,
                low_micros=low,
                close_micros=close,
            )

        quote = observation(
            "quote:AAPL:exit",
            "QUOTE",
            ordinal=1,
            bid=21_490_000,
            ask=21_500_000,
        )
        target_bar = observation(
            "bar:AAPL:exit",
            "BAR",
            ordinal=2,
            open_price=21_400_000,
            high=21_500_000,
            low=21_300_000,
            close=21_490_000,
        )
        result, spread_observation_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (quote, target_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
            )
        )

        self.assertEqual(result.exit_reason, risk_module.ExitReason.TARGET)
        self.assertEqual(result.fill_price, Decimal("21.468510"))
        self.assertEqual(result.observation_id, target_bar.observation_id)
        self.assertEqual(spread_observation_id, quote.observation_id)

        same_time_forced, precedence_quote_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (quote, target_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
                forced_exit_reason=risk_module.ExitReason.EVENT_EXIT_REQUIRED,
                forced_expected_exit=Decimal("21.00"),
                forced_triggered_at=observed_at,
            )
        )
        self.assertEqual(
            same_time_forced.exit_reason,
            risk_module.ExitReason.UNRESOLVED,
        )
        self.assertEqual(
            same_time_forced.reason_codes,
            ("FORCED_EXIT_PRICE_OUTSIDE_BAR",),
        )
        self.assertIsNone(precedence_quote_id)

        stale_quote = copy.copy(quote)
        stale_quote.fresh = False
        unresolved, unresolved_spread_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (stale_quote, target_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
            )
        )
        self.assertEqual(
            unresolved.exit_reason,
            risk_module.ExitReason.UNRESOLVED,
        )
        self.assertEqual(unresolved.reason_codes, ("INVALID_EXIT_SPREAD",))
        self.assertIsNone(unresolved_spread_id)

        ambiguous_bar = copy.copy(target_bar)
        ambiguous_bar.low_micros = 19_500_000
        stop_first, stop_spread_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (quote, ambiguous_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
            )
        )
        self.assertEqual(
            stop_first.exit_reason,
            risk_module.ExitReason.STOP_FIRST_CONSERVATIVE,
        )
        self.assertEqual(stop_first.fill_price, Decimal("19.860120"))
        self.assertEqual(stop_spread_id, quote.observation_id)

        repeated_target, repeated_quote_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (quote, target_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
                target_enabled=False,
            )
        )
        self.assertEqual(repeated_target.exit_reason, risk_module.ExitReason.NO_EXIT)
        self.assertIsNone(repeated_quote_id)

        earlier_target = copy.copy(target_bar)
        earlier_target.observation_id = "bar:AAPL:early-target"
        earlier_target.source_time = aware_et(date(2026, 8, 17), "15:40")
        earlier_target.received_at = earlier_target.source_time + timedelta(
            seconds=1
        )
        earlier_target.bid_micros = 21_490_000
        earlier_target.ask_micros = 21_500_000
        forced_bar = copy.copy(target_bar)
        forced_bar.observation_id = "bar:AAPL:forced"
        forced_bar.source_time = aware_et(date(2026, 8, 17), "15:50")
        forced_bar.received_at = forced_bar.source_time + timedelta(seconds=1)
        forced_bar.open_micros = 21_000_000
        forced_bar.high_micros = 21_100_000
        forced_bar.low_micros = 20_900_000
        forced_bar.close_micros = 21_000_000
        forced_bar.bid_micros = 20_990_000
        forced_bar.ask_micros = 21_010_000
        forced, forced_spread_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (earlier_target, forced_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
                forced_exit_reason=risk_module.ExitReason.EVENT_EXIT_REQUIRED,
                forced_expected_exit=Decimal("21.00"),
                forced_triggered_at=forced_bar.source_time,
            )
        )
        self.assertEqual(
            forced.exit_reason,
            risk_module.ExitReason.EVENT_EXIT_REQUIRED,
        )
        self.assertEqual(forced.exited_at, forced_bar.source_time)
        self.assertEqual(forced.observation_id, forced_bar.observation_id)
        self.assertEqual(forced_spread_id, forced_bar.observation_id)

        earlier_stop = copy.copy(earlier_target)
        earlier_stop.observation_id = "bar:AAPL:early-stop"
        earlier_stop.high_micros = 20_100_000
        earlier_stop.low_micros = 19_800_000
        earlier_stop.open_micros = 20_000_000
        earlier_stop.close_micros = 19_900_000
        earlier_stop.bid_micros = 19_870_000
        earlier_stop.ask_micros = 19_890_000
        protective, protective_spread_id = (
            risk_module._phase1_exit_execution_result_from_observations(
                (earlier_stop, forced_bar),
                stop=Decimal("19.88"),
                target=Decimal("21.49"),
                forced_exit_reason=risk_module.ExitReason.EVENT_EXIT_REQUIRED,
                forced_expected_exit=Decimal("21.00"),
                forced_triggered_at=forced_bar.source_time,
            )
        )
        self.assertEqual(protective.exit_reason, risk_module.ExitReason.STOP)
        self.assertEqual(protective.exited_at, earlier_stop.source_time)
        self.assertEqual(protective_spread_id, earlier_stop.observation_id)

        hold_position = risk_module.Position(
            signal_id="2026-08-14:AAPL",
            symbol="AAPL",
            entry=Decimal("100"),
            shares=10,
            initial_stop=Decimal("97.50"),
            recommended_stop=Decimal("97.50"),
            user_confirmed_stop=Decimal("97.50"),
            target=Decimal("105"),
            tick_size=Decimal("0.01"),
            entered_session=_SESSION,
            ledger_name="CANONICAL",
        )
        end_mark = risk_module.MarketMark(
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 17), "15:59"),
            holding_sessions=2,
            previous_session_low=Decimal("99"),
            current_session_low=Decimal("99"),
            atr14=Decimal("1"),
        )
        intraday_stop = copy.copy(target_bar)
        intraday_stop.observation_id = "bar:AAPL:10:00-stop"
        intraday_stop.source_time = aware_et(date(2026, 8, 17), "10:00")
        intraday_stop.received_at = intraday_stop.source_time + timedelta(
            seconds=1
        )
        intraday_stop.open_micros = 100_000_000
        intraday_stop.high_micros = 101_000_000
        intraday_stop.low_micros = 97_000_000
        intraday_stop.close_micros = 100_000_000
        intraday_stop.bid_micros = 97_490_000
        intraday_stop.ask_micros = 97_510_000
        self.assertEqual(
            risk_module.evaluate_position_diagnostic(
                hold_position,
                end_mark,
                policy_fixture(),
            ).status,
            "PROVISIONAL_HOLD",
        )
        recovered_action, recovered_result, recovered_spread_id = (
            risk_module._phase1_position_exit_decision_from_observations(
                position=hold_position,
                mark=end_mark,
                observations=(intraday_stop,),
                policy=policy_fixture(),
            )
        )
        self.assertEqual(recovered_action.status, "PROVISIONAL_EXIT")
        self.assertEqual(recovered_action.shares_to_exit, 10)
        self.assertIn("RECOMMENDED_STOP_REACHED", recovered_action.reason_codes)
        self.assertEqual(recovered_result.exit_reason, risk_module.ExitReason.STOP)
        self.assertEqual(recovered_result.fill_price, Decimal("97.402500"))
        self.assertEqual(recovered_spread_id, intraday_stop.observation_id)

    def test_exit_execution_batches_target_partial_then_later_tightened_stop(
        self,
    ) -> None:
        target_at = aware_et(date(2026, 8, 17), "14:00")

        def bar(
            observation_id: str,
            *,
            at: datetime,
            open_price: int,
            high: int,
            low: int,
            close: int,
            bid: int,
            ask: int,
        ) -> SimpleNamespace:
            return SimpleNamespace(
                observation_id=observation_id,
                stream_id="bar:SIP:AAPL",
                feed="SIP",
                observation_kind="BAR",
                source_time=at,
                received_at=at + timedelta(seconds=1),
                cohort_ordinal=1,
                fresh=True,
                trade_price_micros=None,
                bid_micros=bid,
                ask_micros=ask,
                open_micros=open_price,
                high_micros=high,
                low_micros=low,
                close_micros=close,
            )

        position = risk_module.Position(
            signal_id="2026-08-14:AAPL",
            symbol="AAPL",
            entry=Decimal("20.41"),
            shares=47,
            initial_stop=Decimal("19.88"),
            recommended_stop=Decimal("19.88"),
            user_confirmed_stop=Decimal("19.88"),
            target=Decimal("21.49"),
            tick_size=Decimal("0.01"),
            entered_session=_SESSION,
            ledger_name="CANONICAL",
        )
        mark = risk_module.MarketMark(
            price=Decimal("21.49"),
            at=aware_et(date(2026, 8, 17), "15:59"),
            holding_sessions=2,
            previous_session_low=Decimal("21.00"),
            # The completed-session mark includes the later 20.90 stop bar.
            # The first target action must nevertheless derive its tightened
            # stop only from observations known through the 14:00 target.
            current_session_low=Decimal("20.90"),
            atr14=Decimal("0.48"),
        )
        target_bar = bar(
            "bar:AAPL:target",
            at=target_at,
            open_price=21_400_000,
            high=21_500_000,
            low=21_300_000,
            close=21_490_000,
            bid=21_490_000,
            ask=21_500_000,
        )
        later_stop = bar(
            "bar:AAPL:later-stop",
            at=target_at + timedelta(minutes=30),
            open_price=21_000_000,
            high=21_050_000,
            low=20_900_000,
            close=21_000_000,
            bid=20_940_000,
            ask=20_960_000,
        )

        steps = risk_module._phase1_position_exit_steps_from_observations(
            position=position,
            mark=mark,
            observations=(target_bar, later_stop),
            policy=policy_fixture(),
        )

        self.assertEqual(
            tuple(step.event_kind for step in steps),
            ("PARTIAL_EXIT", "CLOSE"),
        )
        self.assertEqual(steps[0].action.shares_to_exit, 23)
        self.assertEqual(steps[0].action.remaining_shares, 24)
        self.assertEqual(steps[0].action.recommended_stop, Decimal("20.95"))
        self.assertEqual(
            steps[0].execution_result.exit_reason,
            risk_module.ExitReason.TARGET,
        )
        self.assertEqual(steps[1].position.shares, 24)
        self.assertTrue(steps[1].position.profit_target_taken)
        self.assertEqual(
            steps[1].position.recommended_stop,
            Decimal("20.95"),
        )
        self.assertEqual(steps[1].action.shares_to_exit, 24)
        self.assertEqual(steps[1].action.remaining_shares, 0)
        self.assertEqual(
            steps[1].execution_result.exit_reason,
            risk_module.ExitReason.STOP,
        )
        self.assertLess(
            steps[0].execution_result.exited_at,
            steps[1].execution_result.exited_at,
        )

        partial_only = risk_module._phase1_position_exit_steps_from_observations(
            position=position,
            mark=mark,
            observations=(target_bar,),
            policy=policy_fixture(),
        )
        self.assertEqual(
            tuple(step.event_kind for step in partial_only),
            ("PARTIAL_EXIT",),
        )

        same_bar = copy.copy(target_bar)
        same_bar.low_micros = 19_700_000
        same_bar.bid_micros = 19_870_000
        same_bar.ask_micros = 19_890_000
        stop_first = risk_module._phase1_position_exit_steps_from_observations(
            position=position,
            mark=mark,
            observations=(same_bar,),
            policy=policy_fixture(),
        )
        self.assertEqual(tuple(step.event_kind for step in stop_first), ("CLOSE",))
        self.assertEqual(
            stop_first[0].execution_result.exit_reason,
            risk_module.ExitReason.STOP_FIRST_CONSERVATIVE,
        )
        self.assertEqual(stop_first[0].action.shares_to_exit, 47)

    def test_provider_exit_review_issues_complete_same_session_batch(
        self,
    ) -> None:
        exit_session = date(2026, 8, 17)
        target_at = aware_et(exit_session, "14:00")
        later_stop_at = aware_et(exit_session, "15:00")
        execution_bars = (
            (
                target_at,
                Decimal("21.40"),
                Decimal("21.50"),
                Decimal("21.30"),
                Decimal("21.49"),
                1_000,
            ),
            (
                later_stop_at,
                Decimal("21.00"),
                Decimal("21.05"),
                Decimal("20.90"),
                Decimal("21.00"),
                1_000,
            ),
            (
                aware_et(exit_session, "16:00"),
                Decimal("21.00"),
                Decimal("21.02"),
                Decimal("20.98"),
                Decimal("21.00"),
                1_000,
            ),
        )
        quote_ticks = (
            (
                target_at - timedelta(seconds=1),
                Decimal("21.49"),
                Decimal("21.50"),
                901,
            ),
            (
                later_stop_at - timedelta(seconds=1),
                Decimal("20.94"),
                Decimal("20.96"),
                902,
            ),
            (
                aware_et(exit_session, "15:59") + timedelta(seconds=59),
                Decimal("21.49"),
                Decimal("21.50"),
                903,
            ),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                review_cutoff = aware_et(exit_session, "15:59") + timedelta(
                    seconds=59
                )
                signal_source = journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=review_cutoff,
                )
                source, authority, _query_cutoff = (
                    _prepare_provider_typed_exit(
                        journal,
                        signal_source=signal_source,
                        session_date=exit_session,
                        bid=Decimal("21.49"),
                        ask=Decimal("21.50"),
                        previous_session_low=Decimal("21.00"),
                        execution_open=Decimal("21.40"),
                        execution_high=Decimal("21.50"),
                        execution_low=Decimal("20.90"),
                        execution_close=Decimal("21.00"),
                        adverse_evidence=False,
                        evidence_review_at=(
                            target_at - timedelta(seconds=2)
                        ),
                        execution_bars=execution_bars,
                        quote_ticks=quote_ticks,
                    )
                )

                self.assertTrue(
                    journal_module.is_verified_phase1_exit_review_source(source)
                )
                self.assertTrue(
                    risk_module.is_issued_phase1_position_exit_authority(
                        authority
                    )
                )
                self.assertEqual(
                    tuple(step.event_kind for step in authority.steps),
                    ("PARTIAL_EXIT", "CLOSE"),
                )
                self.assertEqual(
                    authority.steps[0].execution_result.fill_price,
                    Decimal("21.468510"),
                )
                self.assertEqual(
                    authority.steps[0].action.recommended_stop,
                    Decimal("20.94"),
                )
                self.assertEqual(authority.steps[0].action.remaining_shares, 24)
                self.assertEqual(authority.steps[1].position.shares, 24)
                self.assertTrue(authority.steps[1].position.profit_target_taken)
                self.assertEqual(
                    authority.steps[1].execution_result.exit_reason,
                    risk_module.ExitReason.STOP,
                )
                self.assertEqual(
                    authority.steps[1].execution_result.fill_price,
                    Decimal("20.919060"),
                )

                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(
                        copy.copy(authority)
                    )
                )
                suffix = replace(authority, steps=authority.steps[:1])
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(suffix)
                )
                nested = (
                    risk_module._issue_phase1_position_exit_authority_from_source(
                        source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                object.__setattr__(
                    nested.steps[1].action,
                    "shares_to_exit",
                    nested.steps[1].action.shares_to_exit - 1,
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(nested)
                )

    def test_exit_steps_rederive_from_verified_material_and_explicit_position(
        self,
    ) -> None:
        market_source_fields = tuple(
            journal_module.Phase1ExitReviewMarketSource.__dataclass_fields__
        )
        self.assertIn("signal_evidence_source", market_source_fields)
        self.assertNotIn("canonical_replay_source", market_source_fields)
        self.assertNotIn("position_evidence", market_source_fields)
        self.assertNotIn("position", market_source_fields)
        self.assertTrue(
            hasattr(
                risk_module,
                "_derive_phase1_position_exit_steps_from_verified_source",
            ),
            "restart replay needs one shared source-backed exit simulation",
        )
        parameters = inspect.signature(
            risk_module._derive_phase1_position_exit_steps_from_verified_source
        ).parameters
        self.assertEqual(
            tuple(parameters),
            ("source", "position", "calendar_resolver", "policy"),
        )
        self.assertNotIn("canonical_replay_source", parameters)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                source, authority, _query_cutoff = (
                    _prepare_same_session_batch_exit(journal)
                )
                market_source = source.market_source
                self.assertTrue(
                    journal_module.is_verified_phase1_exit_review_market_source(
                        market_source
                    )
                )
                rederived = (
                    risk_module._derive_phase1_position_exit_steps_from_verified_source(
                        market_source,
                        position=authority.position,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                self.assertEqual(rederived, authority.steps)
                self.assertIsNot(rederived, authority.steps)
                self.assertTrue(
                    all(
                        restarted is not warm
                        for restarted, warm in zip(
                            rederived,
                            authority.steps,
                            strict=True,
                        )
                    )
                )
                with self.assertRaises(RiskBlock):
                    risk_module._derive_phase1_position_exit_steps_from_verified_source(
                        market_source,
                        position=replace(
                            authority.position,
                            shares=authority.position.shares - 1,
                        ),
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EXIT_REVIEW_MARKET_SOURCE_UNVERIFIED",
                ):
                    risk_module._derive_phase1_position_exit_steps_from_verified_source(
                        copy.copy(market_source),
                        position=authority.position,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                with mock.patch.object(
                    risk_module,
                    "_policy_digest",
                    return_value="0" * 64,
                ), self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EXIT_POLICY_MISMATCH",
                ):
                    risk_module._derive_phase1_position_exit_steps_from_verified_source(
                        market_source,
                        position=authority.position,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EXIT_REVIEW_MARKET_SOURCE_UNVERIFIED",
                ):
                    risk_module._derive_phase1_position_exit_steps_from_verified_source(
                        source,
                        position=authority.position,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

    def test_same_session_exit_batch_persists_atomically_and_restarts(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "record_phase1_canonical_exit"),
            "Task 8 must persist every issued exit step in one transaction",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                source, authority, query_cutoff = (
                    _prepare_same_session_batch_exit(journal)
                )
                self.assertEqual(
                    tuple(step.event_kind for step in authority.steps),
                    ("PARTIAL_EXIT", "CLOSE"),
                )
                stored = journal.record_phase1_canonical_exit(
                    exit_authority=authority,
                    recorded_at=query_cutoff,
                    calendar_resolver=_calendar(),
                )

                self.assertEqual(len(stored.lifecycle_event_ids), 2)
                self.assertEqual(len(stored.posting_keys), 4)
                self.assertIsNotNone(stored.closed_trade_id)
                self.assertFalse(stored.duplicate)
                events = tuple(
                    tuple(row)
                    for row in journal._connection.execute(
                        "SELECT lifecycle_event_id, event_kind, shares, "
                        "price_micros, recommended_stop_micros "
                        "FROM phase1_signal_events "
                        "WHERE signal_id = ? AND event_kind IN "
                        "('PARTIAL_EXIT', 'CLOSE') ORDER BY event_ordinal",
                        (_signal().signal_id,),
                    ).fetchall()
                )
                self.assertEqual(
                    events,
                    (
                        (
                            stored.lifecycle_event_ids[0],
                            "PARTIAL_EXIT",
                            23,
                            21_468_510,
                            20_940_000,
                        ),
                        (
                            stored.lifecycle_event_ids[1],
                            "CLOSE",
                            24,
                            20_919_060,
                            None,
                        ),
                    ),
                )
                postings = tuple(
                    tuple(row)
                    for row in journal._connection.execute(
                        "SELECT posting_key, lifecycle_event_id, entry_kind, "
                        "amount_micros, shares_delta, unit_price_micros "
                        "FROM phase1_canonical_postings "
                        "WHERE signal_id = ? AND entry_kind IN ('SALE', 'FEE') "
                        "ORDER BY id",
                        (_signal().signal_id,),
                    ).fetchall()
                )
                self.assertEqual(
                    tuple(row[0] for row in postings),
                    stored.posting_keys,
                )
                self.assertEqual(
                    tuple(row[1:] for row in postings),
                    (
                        (
                            stored.lifecycle_event_ids[0],
                            "SALE",
                            493_775_730,
                            -23,
                            21_468_510,
                        ),
                        (
                            stored.lifecycle_event_ids[0],
                            "FEE",
                            -1_000_000,
                            None,
                            None,
                        ),
                        (
                            stored.lifecycle_event_ids[1],
                            "SALE",
                            502_057_440,
                            -24,
                            20_919_060,
                        ),
                        (
                            stored.lifecycle_event_ids[1],
                            "FEE",
                            -1_000_000,
                            None,
                            None,
                        ),
                    ),
                )
                closed_trade = journal._connection.execute(
                    "SELECT trade_id, lifecycle_event_id, shares, "
                    "entry_value_micros, exit_value_micros, fee_micros, "
                    "pnl_micros FROM phase1_closed_trades WHERE signal_id = ?",
                    (_signal().signal_id,),
                ).fetchone()
                self.assertIsNotNone(closed_trade)
                self.assertEqual(
                    tuple(closed_trade),
                    (
                        stored.closed_trade_id,
                        stored.lifecycle_event_ids[1],
                        47,
                        960_210_000,
                        995_833_170,
                        2_000_000,
                        33_623_170,
                    ),
                )
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(
                    replay.ledger_pair.canonical.open_positions,
                    (),
                )
                self.assertEqual(replay.canonical_cash, Decimal("5033.623170"))
                self.assertEqual(
                    replay.settled_buying_power,
                    Decimal("4037.79"),
                )
                self.assertEqual(replay.realized_pnl, Decimal("33.623170"))
                self.assertEqual(len(replay.closed_trades), 1)
                replay_digest = replay.source_digest
                closed_trades = replay.closed_trades
                self.assertFalse(
                    journal_module.is_verified_phase1_exit_review_source(source)
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(
                        authority
                    )
                )

            with Journal.open(path) as restarted:
                restarted_replay = restarted.read_phase1_canonical_replay(
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertIsNot(restarted_replay, replay)
                self.assertEqual(restarted_replay.source_digest, replay_digest)
                self.assertEqual(
                    restarted_replay.canonical_cash,
                    Decimal("5033.623170"),
                )
                self.assertEqual(
                    restarted_replay.settled_buying_power,
                    Decimal("4037.79"),
                )
                self.assertEqual(
                    restarted_replay.realized_pnl,
                    Decimal("33.623170"),
                )
                self.assertEqual(restarted_replay.closed_trades, closed_trades)
                self.assertEqual(
                    restarted_replay.ledger_pair.canonical.open_positions,
                    (),
                )

    def test_same_session_exit_batch_rolls_back_second_step_failure(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(Journal, "record_phase1_canonical_exit"),
            "Task 8 must persist every issued exit step in one transaction",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                _source, authority, query_cutoff = (
                    _prepare_same_session_batch_exit(journal)
                )
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                    journal.count("phase1_closed_trades"),
                )
                journal._connection.execute(
                    "CREATE TEMP TRIGGER fail_second_exit_sale "
                    "BEFORE INSERT ON phase1_canonical_postings "
                    "WHEN NEW.entry_kind = 'SALE' AND "
                    "(SELECT COUNT(*) FROM phase1_canonical_postings "
                    "WHERE signal_id = NEW.signal_id "
                    "AND entry_kind = 'SALE') >= 1 "
                    "BEGIN SELECT RAISE(ABORT, "
                    "'injected second-step failure'); END"
                )
                with self.assertRaises(journal_module.JournalError) as raised:
                    journal.record_phase1_canonical_exit(
                        exit_authority=authority,
                        recorded_at=query_cutoff,
                        calendar_resolver=_calendar(),
                    )
                causes: list[str] = []
                error: BaseException | None = raised.exception
                while error is not None:
                    causes.append(str(error))
                    error = error.__cause__
                self.assertIn(
                    "injected second-step failure",
                    " | ".join(causes),
                    "the writer must reach the injected second SALE before "
                    "the transaction rollback is credited",
                )
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                        journal.count("phase1_closed_trades"),
                    ),
                    before,
                )
                self.assertEqual(
                    journal._connection.execute(
                        "SELECT COUNT(*) FROM phase1_signal_events "
                        "WHERE signal_id = ? AND event_kind IN "
                        "('PARTIAL_EXIT', 'CLOSE')",
                        (_signal().signal_id,),
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    journal._connection.execute(
                        "SELECT COUNT(*) FROM phase1_canonical_postings "
                        "WHERE signal_id = ? AND entry_kind IN ('SALE', 'FEE')",
                        (_signal().signal_id,),
                    ).fetchone()[0],
                    0,
                )

    def test_entry_execution_recomputes_the_first_exact_trigger_quote_pair(
        self,
    ) -> None:
        first_trade_at = aware_et(_SESSION, "09:36")

        def observation(
            observation_id: str,
            kind: str,
            *,
            ordinal: int,
            seconds: int,
            fresh: bool = True,
            trade_price: int | None = None,
            bid: int | None = None,
            ask: int | None = None,
        ) -> SimpleNamespace:
            observed_at = first_trade_at + timedelta(seconds=seconds)
            return SimpleNamespace(
                observation_id=observation_id,
                stream_id=f"{kind.lower()}:SIP:AAPL",
                feed="SIP",
                observation_kind=kind,
                source_time=observed_at,
                received_at=observed_at + timedelta(seconds=1),
                provider_sequence=7,
                cohort_ordinal=ordinal,
                fresh=fresh,
                trade_price_micros=trade_price,
                bid_micros=bid,
                ask_micros=ask,
                open_micros=None,
                high_micros=None,
                low_micros=None,
                close_micros=None,
            )

        first_trade = observation(
            "trade:AAPL:first",
            "TRADE",
            ordinal=1,
            seconds=0,
            trade_price=100_000_000,
        )
        first_quote = observation(
            "quote:AAPL:first",
            "QUOTE",
            ordinal=2,
            seconds=1,
            bid=100_000_000,
            ask=100_050_000,
        )
        later_trade = observation(
            "trade:AAPL:later",
            "TRADE",
            ordinal=3,
            seconds=2,
            trade_price=101_000_000,
        )
        later_quote = observation(
            "quote:AAPL:later",
            "QUOTE",
            ordinal=4,
            seconds=3,
            bid=100_050_000,
            ask=100_080_000,
        )
        result = ledger_module._phase1_entry_result_from_observations(
            (first_trade, first_quote, later_trade, later_quote),
            trigger=Decimal("100"),
            limit=Decimal("100.10"),
        )

        self.assertEqual(result.status.value, "TRIGGERED_PAPER")
        self.assertEqual(result.fill_price, Decimal("100.100000"))
        self.assertEqual(
            result.trigger_observation_id,
            first_trade.observation_id,
        )
        self.assertEqual(
            result.quote_observation_id,
            first_quote.observation_id,
        )

        stale_quote = copy.copy(first_quote)
        stale_quote.fresh = False
        stale = ledger_module._phase1_entry_result_from_observations(
            (first_trade, stale_quote, later_trade, later_quote),
            trigger=Decimal("100"),
            limit=Decimal("100.10"),
        )
        self.assertEqual(stale.status.value, "UNRESOLVED")
        self.assertEqual(stale.reason_codes, ("STALE_OBSERVATION",))

        crossed_quote = copy.copy(first_quote)
        crossed_quote.bid_micros = 100_090_000
        crossed_quote.ask_micros = 100_080_000
        crossed = ledger_module._phase1_entry_result_from_observations(
            (first_trade, crossed_quote, later_trade, later_quote),
            trigger=Decimal("100"),
            limit=Decimal("100.10"),
        )
        self.assertEqual(crossed.status.value, "UNRESOLVED")
        self.assertEqual(
            crossed.reason_codes,
            ("INVALID_POST_TRIGGER_QUOTE",),
        )

        duplicate_sequence = copy.copy(first_quote)
        duplicate_sequence.cohort_ordinal = first_trade.cohort_ordinal
        ambiguous = ledger_module._phase1_entry_result_from_observations(
            (first_trade, duplicate_sequence, later_trade, later_quote),
            trigger=Decimal("100"),
            limit=Decimal("100.10"),
        )
        self.assertEqual(ambiguous.status.value, "UNRESOLVED")
        self.assertEqual(
            ambiguous.reason_codes,
            ("DUPLICATE_NORMALIZED_SEQUENCE",),
        )

        simultaneous_quote = copy.copy(first_quote)
        simultaneous_quote.cohort_ordinal = 1
        simultaneous_quote.source_time = first_trade.source_time
        simultaneous_trade = copy.copy(first_trade)
        simultaneous_trade.cohort_ordinal = 2
        for caller_order in (
            (simultaneous_trade, simultaneous_quote),
            (simultaneous_quote, simultaneous_trade),
        ):
            with self.subTest(caller_order=caller_order):
                simultaneous = (
                    ledger_module._phase1_entry_result_from_observations(
                        caller_order,
                        trigger=Decimal("100"),
                        limit=Decimal("100.10"),
                    )
                )
                self.assertEqual(
                    simultaneous.status.value,
                    "UNRESOLVED",
                )
                self.assertEqual(
                    simultaneous.trigger_observation_id,
                    simultaneous_trade.observation_id,
                )
                self.assertIsNone(simultaneous.quote_observation_id)
                self.assertEqual(
                    simultaneous.reason_codes,
                    ("MISSING_POST_TRIGGER_QUOTE",),
                )

    def test_provider_entry_cohort_order_and_empty_quote_fail_closed(self) -> None:
        def source_backed_result(
            *,
            reverse_cohort_order: bool = False,
            simultaneous_trade_quote: bool = False,
            empty_quote: bool = False,
        ):
            with tempfile.TemporaryDirectory() as temporary_directory:
                path = Path(temporary_directory) / "journal.db"
                with Journal.open(path) as journal:
                    _publish(journal)
                    replay_source = (
                        journal._read_phase1_canonical_replay_source(
                            query_cutoff=aware_et(_SESSION, "08:45"),
                        )
                    )
                    signal_source = replay_source.signal_sources[0]
                    trigger = risk_module.money_from_micros(
                        signal_source.trigger_price_micros
                    )
                    limit = risk_module.money_from_micros(
                        signal_source.maximum_entry_micros
                    )
                    trade_id, quote_id, completed_at = (
                        _append_completed_entry_observations(
                            journal,
                            signal_source,
                            reverse_cohort_order=reverse_cohort_order,
                            simultaneous_trade_quote=simultaneous_trade_quote,
                            empty_quote=empty_quote,
                        )
                    )
                    observation_ids = (
                        (trade_id,)
                        if quote_id is None
                        else (trade_id, quote_id)
                    )
                    observations = tuple(
                        sorted(
                            (
                                journal.read_phase1_observation(
                                    observation_id,
                                    query_cutoff=completed_at,
                                )
                                for observation_id in observation_ids
                            ),
                            key=lambda item: item.cohort_ordinal,
                        )
                    )
                    completion = (
                        journal._read_phase1_session_completion_source(
                            signal_id=signal_source.signal_id,
                            session_date=_SESSION,
                            query_cutoff=completed_at,
                        )
                    )
                    result = (
                        ledger_module._phase1_entry_result_from_observations(
                            observations,
                            trigger=trigger,
                            limit=limit,
                        )
                    )
                    semantics = tuple(
                        (
                            item.observation_id,
                            item.observation_kind,
                            item.source_time,
                            item.received_at,
                            item.cohort_ordinal,
                            item.fresh,
                            item.trade_price_micros,
                            item.bid_micros,
                            item.ask_micros,
                            item.source_digest,
                        )
                        for item in observations
                    )
                    return result, semantics, completion.source_digest

        ordered, ordered_semantics, ordered_completion = source_backed_result()
        reversed_result, reversed_semantics, reversed_completion = (
            source_backed_result(reverse_cohort_order=True)
        )
        self.assertEqual(ordered.status.value, "TRIGGERED_PAPER")
        self.assertEqual(reversed_result, ordered)
        self.assertEqual(reversed_semantics, ordered_semantics)
        self.assertEqual(reversed_completion, ordered_completion)

        simultaneous, simultaneous_semantics, _ = source_backed_result(
            simultaneous_trade_quote=True,
        )
        self.assertEqual(
            tuple(item[1] for item in simultaneous_semantics),
            ("QUOTE", "TRADE"),
        )
        self.assertEqual(simultaneous.status.value, "UNRESOLVED")
        self.assertEqual(
            simultaneous.reason_codes,
            ("MISSING_POST_TRIGGER_QUOTE",),
        )

        empty, empty_semantics, _ = source_backed_result(empty_quote=True)
        self.assertEqual(tuple(item[1] for item in empty_semantics), ("TRADE",))
        self.assertEqual(empty.status.value, "UNRESOLVED")
        self.assertEqual(
            empty.reason_codes,
            ("MISSING_POST_TRIGGER_QUOTE",),
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "full release-calendar session",
                ):
                    _append_completed_entry_observations(
                        journal,
                        replay_source.signal_sources[0],
                        request_end=aware_et(_SESSION, "10:02"),
                    )
                self.assertEqual(journal.count("phase1_observations"), 0)
                self.assertEqual(
                    journal.count("phase1_observation_fetch_manifests"),
                    0,
                )

    def test_full_exit_cash_truth_is_not_substituted_from_compatibility_pair(self) -> None:
        for name in (
            "ingest_phase1_exit_review_cohorts",
            "read_phase1_exit_review_source",
            "record_phase1_canonical_exit",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 provider-backed exit contract requires {name}",
            )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                exit_session = date(2026, 8, 17)
                signal_source = journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=aware_et(exit_session, "15:59")
                    + timedelta(seconds=59),
                )
                _source, _authority, _stored, completed_at = (
                    _record_provider_typed_exit(
                        journal,
                        signal_source=signal_source,
                        session_date=exit_session,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.80"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                        adverse_evidence=True,
                    )
                )
                replay = journal.read_phase1_canonical_replay(  # type: ignore[attr-defined]
                    query_cutoff=completed_at,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                settled = journal.read_phase1_canonical_replay(
                    query_cutoff=aware_et(date(2026, 8, 18), "09:30"),
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )

                self.assertEqual(replay.ledger_pair.canonical.open_positions, ())
                self.assertEqual(replay.ledger_pair.canonical.cash, Decimal("5000"))
                self.assertEqual(
                    _authority.execution_result.fill_price,
                    Decimal("20.979000"),
                )
                self.assertEqual(replay.canonical_cash, Decimal("5024.803000"))
                self.assertEqual(
                    replay.settled_buying_power,
                    Decimal("4038.79"),
                )
                self.assertEqual(
                    settled.settled_buying_power,
                    Decimal("5024.803000"),
                )
                self.assertEqual(replay.realized_pnl, Decimal("24.803000"))
                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(replay.cohort)
                )
                self.assertFalse(
                    ledger_module.is_issued_verified_replay_cohort(
                        copy.copy(replay.cohort)
                    )
                )

    def test_partial_exit_restart_retains_tightened_stop_and_target_state(
        self,
    ) -> None:
        for name in (
            "ingest_phase1_exit_review_cohorts",
            "read_phase1_exit_review_source",
            "record_phase1_canonical_exit",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 provider-backed exit contract requires {name}",
            )
        self.assertTrue(
            hasattr(risk_module, "Phase1PositionExitAuthority")
        )
        self.assertTrue(
            hasattr(
                risk_module,
                "_issue_phase1_position_exit_authority_from_source",
            )
        )
        self.assertTrue(
            hasattr(
                risk_module,
                "is_issued_phase1_position_exit_authority",
            )
        )
        self.assertIn(
            "steps",
            risk_module.Phase1PositionExitAuthority.__dataclass_fields__,
            "exit authority must bind every conservative simulated fill",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            partial_session = date(2026, 8, 17)
            final_session = date(2026, 8, 18)
            with Journal.open(path) as first_journal:
                _seed_completed_authority_fill(first_journal)
                partial_signal_source = (
                    first_journal._read_phase1_signal_source(
                        _signal().signal_id,
                        query_cutoff=aware_et(partial_session, "15:59")
                        + timedelta(seconds=59),
                    )
                )
                (
                    first_exit_source,
                    first_exit_authority,
                    _stored,
                    partial_completed_at,
                ) = _record_provider_typed_exit(
                    first_journal,
                    signal_source=partial_signal_source,
                    session_date=partial_session,
                    bid=Decimal("21.49"),
                    ask=Decimal("21.50"),
                    previous_session_low=Decimal("21.00"),
                    execution_open=Decimal("21.40"),
                    execution_high=Decimal("21.50"),
                    execution_low=Decimal("21.30"),
                    execution_close=Decimal("21.49"),
                    adverse_evidence=False,
                )
                first_replay = first_journal.read_phase1_canonical_replay(
                    query_cutoff=partial_completed_at,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                first_position = (
                    first_replay.ledger_pair.canonical.open_positions[0]
                )
                partial_replay_source = (
                    first_journal._read_phase1_canonical_replay_source(
                        query_cutoff=partial_completed_at,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                partial_event = next(
                    event for event in partial_replay_source.lifecycle_events
                    if event.event_kind == "PARTIAL_EXIT"
                )

                self.assertFalse(
                    journal_module.is_verified_phase1_exit_review_source(
                        first_exit_source
                    ),
                    "the successful writer must revoke its consumed source",
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(
                        first_exit_authority
                    ),
                    "the successful writer must revoke its consumed authority",
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_evidence_authority(
                        first_exit_source.position_evidence
                    )
                )
                self.assertEqual(first_exit_authority.action.shares_to_exit, 23)
                self.assertEqual(
                    first_exit_authority.execution_result.fill_price,
                    Decimal("21.468510"),
                )
                self.assertEqual(
                    first_exit_authority.action.remaining_shares,
                    24,
                )
                self.assertEqual(
                    first_exit_authority.action.recommended_stop,
                    Decimal("20.95"),
                )
                self.assertEqual(partial_event.to_status, "TRIGGERED_PAPER")
                self.assertEqual(
                    partial_event.recommended_stop_micros,
                    20_950_000,
                )
                self.assertEqual(first_position.shares, 24)
                self.assertEqual(
                    first_position.recommended_stop,
                    Decimal("20.95"),
                )
                self.assertTrue(first_position.profit_target_taken)
                self.assertEqual(first_replay.closed_trades, ())
                self.assertEqual(
                    first_replay.canonical_cash,
                    Decimal("4532.565730"),
                )
                self.assertEqual(
                    first_replay.settled_buying_power,
                    Decimal("4038.79"),
                )
                self.assertEqual(
                    first_replay.ledger_pair.canonical.cash,
                    Decimal("4509.68"),
                )

            self.assertFalse(
                journal_module.is_verified_phase1_exit_review_source(
                    first_exit_source
                )
            )
            self.assertFalse(
                risk_module.is_issued_phase1_position_exit_authority(
                    first_exit_authority
                )
            )
            self.assertFalse(first_replay.source_verified)

            with Journal.open(path) as second_journal:
                restarted_replay = second_journal.read_phase1_canonical_replay(
                    query_cutoff=partial_completed_at,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                restarted_position = (
                    restarted_replay.ledger_pair.canonical.open_positions[0]
                )

                self.assertIsNot(first_replay, restarted_replay)
                self.assertEqual(
                    first_replay.source_digest,
                    restarted_replay.source_digest,
                )
                self.assertEqual(first_position, restarted_position)
                self.assertEqual(restarted_position.shares, 24)
                self.assertEqual(
                    restarted_position.recommended_stop,
                    Decimal("20.95"),
                )
                self.assertTrue(restarted_position.profit_target_taken)
                restarted_task6_position = risk_module.Position(
                    signal_id=restarted_position.signal_id,
                    symbol=restarted_position.symbol,
                    entry=restarted_position.entry,
                    shares=restarted_position.shares,
                    initial_stop=_signal().recommended_stop,
                    recommended_stop=restarted_position.recommended_stop,
                    user_confirmed_stop=restarted_position.recommended_stop,
                    target=restarted_position.target,
                    tick_size=restarted_position.tick_size,
                    entered_session=_SESSION,
                    ledger_name="CANONICAL",
                    profit_target_taken=restarted_position.profit_target_taken,
                )
                later_above_target = risk_module.evaluate_position_diagnostic(
                    restarted_task6_position,
                    risk_module.MarketMark(
                        price=Decimal("21.60"),
                        at=aware_et(final_session, "15:30"),
                        holding_sessions=3,
                        previous_session_low=Decimal("21.20"),
                        current_session_low=Decimal("21.30"),
                        atr14=Decimal("0.50"),
                    ),
                    policy_fixture(),
                )
                self.assertEqual(later_above_target.status, "PROVISIONAL_HOLD")
                self.assertEqual(later_above_target.shares_to_exit, 0)
                self.assertNotIn(
                    "TWO_R_REACHED",
                    later_above_target.reason_codes,
                )

                (
                    final_exit_source,
                    final_exit_authority,
                    _stored,
                    final_completed_at,
                ) = _record_provider_typed_exit(
                    second_journal,
                    signal_source=(
                        second_journal._read_phase1_signal_source(
                            _signal().signal_id,
                            query_cutoff=aware_et(final_session, "15:59")
                            + timedelta(seconds=59),
                        )
                    ),
                    session_date=final_session,
                    bid=Decimal("20.95"),
                    ask=Decimal("20.96"),
                    previous_session_low=Decimal("20.90"),
                    execution_open=Decimal("21.00"),
                    execution_high=Decimal("21.05"),
                    execution_low=Decimal("20.90"),
                    execution_close=Decimal("21.00"),
                    adverse_evidence=False,
                    persist_evidence=False,
                )
                final_replay = second_journal.read_phase1_canonical_replay(
                    query_cutoff=final_completed_at,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                final_replay_source = (
                    second_journal._read_phase1_canonical_replay_source(
                        query_cutoff=final_completed_at,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                final_event = next(
                    event for event in final_replay_source.lifecycle_events
                    if event.event_kind == "CLOSE"
                )

                self.assertEqual(final_exit_authority.action.shares_to_exit, 24)
                self.assertEqual(
                    final_exit_authority.execution_result.fill_price,
                    Decimal("20.929050"),
                )
                self.assertEqual(final_exit_authority.action.remaining_shares, 0)
                self.assertIsNone(final_event.recommended_stop_micros)
                self.assertEqual(final_event.to_status, "CLOSED")
                self.assertEqual(
                    final_replay.ledger_pair.canonical.open_positions,
                    (),
                )
                self.assertEqual(
                    final_replay.canonical_cash,
                    Decimal("5033.862930"),
                )
                self.assertEqual(
                    final_replay.settled_buying_power,
                    Decimal("4531.565730"),
                )
                self.assertEqual(
                    final_replay.realized_pnl,
                    Decimal("33.862930"),
                )
                self.assertEqual(len(final_replay.closed_trades), 1)

    def test_exit_authority_rejects_copy_splice_lookahead_and_oversell(
        self,
    ) -> None:
        for name in (
            "ingest_phase1_exit_review_cohorts",
            "read_phase1_exit_review_source",
            "record_phase1_canonical_exit",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 provider-backed exit contract requires {name}",
            )
        self.assertTrue(
            hasattr(journal_module, "is_verified_phase1_exit_review_source")
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            unknown_path = Path(temporary_directory) / "unknown.db"
            exit_session = date(2026, 8, 17)
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal, Journal.open(unknown_path) as unknown_journal:
                _seed_completed_authority_fill(first_journal)
                _seed_completed_authority_fill(second_journal)
                _seed_completed_authority_fill(unknown_journal)
                review_cutoff = aware_et(exit_session, "15:59") + timedelta(
                    seconds=59
                )
                first_signal_source = first_journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=review_cutoff,
                )
                second_signal_source = second_journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=review_cutoff,
                )
                unknown_signal_source = (
                    unknown_journal._read_phase1_signal_source(
                        _signal().signal_id,
                        query_cutoff=review_cutoff,
                    )
                )
                exit_options = {
                    "session_date": exit_session,
                    "bid": Decimal("21.49"),
                    "ask": Decimal("21.50"),
                    "previous_session_low": Decimal("21.00"),
                    "execution_open": Decimal("21.40"),
                    "execution_high": Decimal("21.50"),
                    "execution_low": Decimal("21.30"),
                    "execution_close": Decimal("21.49"),
                    "adverse_evidence": False,
                }
                first_source, first_authority, completed_at = (
                    _prepare_provider_typed_exit(
                        first_journal,
                        signal_source=first_signal_source,
                        **exit_options,
                    )
                )
                second_source, _second_authority, _second_completed_at = (
                    _prepare_provider_typed_exit(
                        second_journal,
                        signal_source=second_signal_source,
                        **exit_options,
                    )
                )
                unknown_source, _unknown_completed_at = (
                    _read_provider_exit_review_source(
                        unknown_journal,
                        signal_source=unknown_signal_source,
                        binary_event_coverage="UNKNOWN",
                        **exit_options,
                    )
                )

                self.assertTrue(
                    journal_module.is_verified_phase1_exit_review_source(
                        first_source
                    )
                )
                self.assertTrue(
                    journal_module.is_verified_phase1_exit_review_source(
                        unknown_source
                    )
                )
                self.assertEqual(unknown_source.position_evidence.status, "UNRESOLVED")
                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_EXIT_EVENT_EVIDENCE_UNRESOLVED",
                ):
                    risk_module._issue_phase1_position_exit_authority_from_source(
                        unknown_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

                for legacy_scalar in (
                    "previous_session_low_micros",
                    "current_session_low_micros",
                    "atr14_micros",
                    "event_exit_required",
                    "thesis_invalidated",
                ):
                    self.assertFalse(
                        hasattr(first_source, legacy_scalar),
                        f"exit source must not authorize {legacy_scalar}",
                    )

                symbol = first_source.symbol
                daily_values = first_source.daily_bar_cohort[symbol]
                subset_daily = replace(
                    first_source.daily_bar_cohort,
                    _entries=((symbol, daily_values[:-1]),),
                )
                tampered_last_bar = replace(
                    daily_values[-1],
                    low=daily_values[-1].low + Decimal("0.01"),
                )
                same_id_tampered_daily = replace(
                    first_source.daily_bar_cohort,
                    _entries=(
                        (symbol, (*daily_values[:-1], tampered_last_bar)),
                    ),
                )
                forged_signal_evidence = copy.copy(
                    first_source.position_evidence.signal_evidence
                )
                object.__setattr__(
                    forged_signal_evidence,
                    "event_exit_required",
                    True,
                )
                forged_position_evidence = copy.copy(
                    first_source.position_evidence
                )
                object.__setattr__(
                    forged_position_evidence,
                    "signal_evidence",
                    forged_signal_evidence,
                )
                stale_signal_evidence = copy.copy(
                    first_source.position_evidence.signal_evidence
                )
                object.__setattr__(
                    stale_signal_evidence,
                    "review_at",
                    first_source.position_evidence.review_at
                    - timedelta(minutes=1),
                )
                stale_position_evidence = copy.copy(
                    first_source.position_evidence
                )
                object.__setattr__(
                    stale_position_evidence,
                    "signal_evidence",
                    stale_signal_evidence,
                )

                forged_sources = (
                    copy.copy(first_source),
                    replace(first_source),
                    replace(
                        first_source,
                        canonical_replay_source=(
                            second_source.canonical_replay_source
                        ),
                    ),
                    replace(
                        first_source,
                        position_evidence=second_source.position_evidence,
                    ),
                    replace(
                        first_source,
                        daily_bar_cohort=second_source.daily_bar_cohort,
                    ),
                    replace(first_source, daily_bar_cohort=subset_daily),
                    replace(
                        first_source,
                        daily_bar_cohort=same_id_tampered_daily,
                    ),
                    replace(
                        first_source,
                        position_evidence=forged_position_evidence,
                    ),
                    replace(
                        first_source,
                        position_evidence=stale_position_evidence,
                    ),
                )
                for forged in forged_sources:
                    with self.subTest(forged=forged):
                        self.assertFalse(
                            journal_module.is_verified_phase1_exit_review_source(
                                forged
                            )
                        )
                        with self.assertRaises(RiskBlock):
                            risk_module._issue_phase1_position_exit_authority_from_source(
                                forged,
                                calendar_resolver=_calendar(),
                                policy=policy_fixture(),
                            )

                self.assertFalse(
                    alpaca_module.provider_fetch_cohorts_share_owner(
                        second_source.daily_bar_cohort,
                        first_source.execution_bar_cohort,
                        first_source.quote_cohort,
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(
                        copy.copy(first_authority)
                    )
                )
                with self.assertRaises(
                    (journal_module.InvalidJournalValue, RiskBlock)
                ):
                    second_journal.record_phase1_canonical_exit(
                        exit_authority=first_authority,
                        recorded_at=completed_at,
                        calendar_resolver=_calendar(),
                    )
                with self.assertRaises(
                    (journal_module.InvalidJournalValue, RiskBlock)
                ):
                    first_journal.read_phase1_exit_review_source(
                        _signal().signal_id,
                        review_session=exit_session,
                        query_cutoff=completed_at - timedelta(microseconds=1),
                        calendar_resolver=_calendar(),
                    )

                oversell = (
                    risk_module._issue_phase1_position_exit_authority_from_source(
                        first_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                object.__setattr__(
                    oversell.action,
                    "shares_to_exit",
                    oversell.position.shares + 1,
                )
                self.assertFalse(
                    risk_module.is_issued_phase1_position_exit_authority(
                        oversell
                    )
                )
                with self.assertRaises(
                    (journal_module.InvalidJournalValue, RiskBlock)
                ):
                    first_journal.record_phase1_canonical_exit(
                        exit_authority=oversell,
                        recorded_at=completed_at,
                        calendar_resolver=_calendar(),
                    )

    def test_breaker_history_and_state_bind_complete_source_and_nested_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            through_session = date(2026, 8, 13)
            cutoff = aware_et(through_session, "16:00") + timedelta(seconds=1)
            with Journal.open(path) as journal:
                _publish(journal)
                source = journal._read_phase1_breaker_history_source(  # type: ignore[attr-defined]
                    ledger_name="CANONICAL",
                    through_session=through_session,
                    query_cutoff=cutoff,
                )
                history = risk_module._issue_breaker_history_from_phase1_source(
                    source,
                    calendar_resolver=_calendar(),
                )
                state = risk_module.evaluate_authorized_breakers(history)

                self.assertTrue(
                    risk_module.is_issued_breaker_history_authority(history)
                )
                self.assertTrue(risk_module.is_issued_breaker_state(state))
                self.assertFalse(
                    risk_module.is_issued_breaker_history_authority(
                        copy.copy(history)
                    )
                )
                self.assertFalse(
                    risk_module.is_issued_breaker_state(copy.copy(state))
                )

                nested_mutation = (
                    risk_module._issue_breaker_history_from_phase1_source(
                        source,
                        calendar_resolver=_calendar(),
                    )
                )
                object.__setattr__(
                    nested_mutation.equity[0],
                    "equity",
                    Decimal("4999"),
                )
                self.assertFalse(
                    risk_module.is_issued_breaker_history_authority(
                        nested_mutation
                    )
                )

                diagnostic = SessionCalendarResolver.for_diagnostics(
                    _calendar().calendars
                )
                with self.assertRaisesRegex(
                    RiskBlock,
                    "CALENDAR_RELEASE_AUTHORITY_UNVERIFIED",
                ):
                    risk_module._issue_breaker_history_from_phase1_source(
                        source,
                        calendar_resolver=diagnostic,
                    )

    def test_portfolio_authority_uses_phase1_cash_and_deep_fingerprint(self) -> None:
        for name in (
            "ingest_phase1_exit_review_cohorts",
            "read_phase1_exit_review_source",
            "record_phase1_canonical_exit",
        ):
            self.assertTrue(
                hasattr(Journal, name),
                f"Task 8 provider-backed exit contract requires {name}",
            )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            exit_session = date(2026, 8, 17)
            entry_session = date(2026, 8, 18)
            cutoff = aware_et(entry_session, "08:45")
            with Journal.open(path) as journal:
                (
                    _entry_mark_source,
                    _entry_actual_mark_source,
                    entry_mark,
                    entry_actual_mark,
                    entry_mark_cutoff,
                ) = _phase1_equity_authorities(journal)
                journal.record_phase1_session_mark(
                    canonical_authority=entry_mark,
                    actual_authority=entry_actual_mark,
                    recorded_at=entry_mark_cutoff,
                    calendar_resolver=_calendar(),
                )
                signal_source = journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=aware_et(exit_session, "15:59")
                    + timedelta(seconds=59),
                )
                _source, _exit_authority, _stored_exit, exit_mark_cutoff = (
                    _record_provider_typed_exit(
                        journal,
                        signal_source=signal_source,
                        session_date=exit_session,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.80"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                        adverse_evidence=True,
                    )
                )
                self.assertTrue(hasattr(Journal, "record_phase1_session_mark"))
                exit_mark_source = journal.read_phase1_equity_mark_source(
                    ledger_name="CANONICAL",
                    session_date=exit_session,
                    query_cutoff=exit_mark_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                exit_actual_mark_source = (
                    journal.read_phase1_equity_mark_source(
                        ledger_name="ACTUAL",
                        session_date=exit_session,
                        query_cutoff=exit_mark_cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                exit_mark = risk_module._issue_phase1_equity_point_from_source(
                    exit_mark_source,
                    calendar_resolver=_calendar(),
                )
                exit_actual_mark = (
                    risk_module._issue_phase1_equity_point_from_source(
                        exit_actual_mark_source,
                        calendar_resolver=_calendar(),
                    )
                )
                journal.record_phase1_session_mark(
                    canonical_authority=exit_mark,
                    actual_authority=exit_actual_mark,
                    recorded_at=exit_mark_cutoff,
                    calendar_resolver=_calendar(),
                )
                request = LongPlanRequest(
                    entry=Decimal("100"),
                    stop=Decimal("97.50"),
                    tick_size=Decimal("0.01"),
                    session_date=entry_session,
                    symbol="SPY",
                    published_target=Decimal("105"),
                )
                authority = (
                    journal.read_phase1_canonical_portfolio_authority(  # type: ignore[attr-defined]
                        request=request,
                        as_of=cutoff,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                )
                replay = journal.read_phase1_canonical_replay(  # type: ignore[attr-defined]
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(replay.ledger_pair.canonical.cash, Decimal("5000"))
                self.assertEqual(
                    authority.portfolio_state.settled_cash,
                    Decimal("5024.803000"),
                )
                self.assertTrue(
                    risk_module.is_issued_portfolio_risk_authority(authority)
                )
                self.assertFalse(
                    risk_module.is_issued_portfolio_risk_authority(
                        copy.copy(authority)
                    )
                )

                object.__setattr__(
                    authority.portfolio_state,
                    "settled_cash",
                    Decimal("5000"),
                )
                self.assertFalse(
                    risk_module.is_issued_portfolio_risk_authority(authority)
                )


if __name__ == "__main__":
    unittest.main()
