from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, localcontext
from functools import lru_cache
from pathlib import Path
from typing import Any
from unittest import mock
from zoneinfo import ZoneInfo

from stock_monitor import evidence as evidence_module
from stock_monitor.evidence import (
    DateRange,
    EvidenceCoverageAttestation,
    EvidenceDecision,
    EvidenceRecord,
    EvidenceSourceBinding,
    classify_evidence,
)
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
from stock_monitor.providers.cache import SourceDocument
from stock_monitor.providers.http import EgressPolicy, HttpResponse
from stock_monitor.providers.reference import (
    InstrumentStatusDecision,
    ReferenceClient,
    classify_instrument_status,
)
from stock_monitor.universe import load_current_universe


ET = ZoneInfo("America/New_York")
SESSION_DATE = date(2026, 8, 14)
RUN_AT = datetime.combine(SESSION_DATE, time(8, 45), ET)
FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "market"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MARKET_CALENDAR = load_current_market_calendar(
    PROJECT_ROOT,
    as_of=SESSION_DATE,
)
PRIMARY_SYMBOL = "AAPL"
SECONDARY_SYMBOL = "AMD"
TEST_UNIVERSE = load_current_universe(PROJECT_ROOT, as_of=SESSION_DATE)
_UNIVERSE_RECORDS = TEST_UNIVERSE.records


@dataclass(frozen=True, slots=True)
class BarFixture:
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    feed: str = "sip"
    adjustment: str = "split"
    source_observation_id: str = "bar-fixture"


@dataclass(frozen=True, slots=True)
class QuoteFixture:
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    feed: str
    sequence: int | None = 1
    age_seconds: int = 0
    source_observation_id: str = "quote-fixture"


@dataclass(frozen=True, slots=True)
class RecordFixture:
    symbol: str = PRIMARY_SYMBOL
    product_type: str = "common_stock"
    listing_venue: str = "NASDAQ"
    benchmark: str = "SPY"
    sector_etf: str | None = "XLK"
    enabled: bool = True
    leveraged: bool = False
    inverse: bool = False
    free_float: int | None = 50_000_000
    tick_size: Decimal = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class InstrumentStatusFixture:
    symbol: str = PRIMARY_SYMBOL
    halt_status: str = "CLEAR"
    block_reason: str | None = None
    as_of: datetime = RUN_AT
    valid_until: datetime | None = RUN_AT + timedelta(minutes=5)
    source_observation_ids: tuple[str, ...] = ("halt-fixture",)


_HALT_SOURCE_RESPONSES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
        b'<?xml version="1.0"?><rss version="2.0" '
        b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
        b"<ndaq:numItems>0</ndaq:numItems></channel></rss>",
        "application/xml",
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": (
        b"[]",
        "application/json",
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
        b'<?xml version="1.0"?><rss version="2.0" '
        b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
        b"<title>Nasdaq Equity Trader Alerts</title>"
        b"<ndaq:numItems>0</ndaq:numItems></channel></rss>",
        "application/xml",
    ),
}
_HALT_SOURCE_ROLES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
        "PRIMARY_HALT_FEED"
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": (
        "OPERATIONAL_STATUS"
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
        "TRADER_ALERT_HALT"
    ),
}


class _FixtureReferenceTransport:
    def get(self, url: str, headers: object) -> HttpResponse:
        body, content_type = _HALT_SOURCE_RESPONSES[url]
        return HttpResponse(
            200,
            (("Content-Type", content_type),),
            body,
            url,
        )


def reviewed_instrument_status(
    symbol: str,
    listing_venue: str,
) -> InstrumentStatusDecision:
    client = ReferenceClient(
        _FixtureReferenceTransport(),
        EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
        allowed_urls=_HALT_SOURCE_ROLES,
        source_roles=_HALT_SOURCE_ROLES,
        now=lambda: RUN_AT,
    )
    snapshots = {
        "primary_halt_feed": client.parse_halt_feed(
            client.fetch(
                "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
            )
        ),
        "operational_status": client.parse_halt_feed(
            client.fetch(
                "https://www.nyse.com/api/notifications/public/alerts?2=3"
            )
        ),
        "cross_check_halt_feed": client.parse_halt_feed(
            client.fetch(
                "https://www.nasdaqtrader.com/"
                "rss.aspx?categorylist=2&feed=currentheadlines"
            )
        ),
    }
    return classify_instrument_status(
        symbol,
        listing_venue,
        snapshots,
        as_of=RUN_AT,
    )


def session_dates(count: int = 60, end: date = date(2026, 8, 13)) -> tuple[date, ...]:
    values: list[date] = []
    current = end
    while len(values) < count:
        if MARKET_CALENDAR.is_open(current):
            values.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(values))


def hold_sessions(start: date = SESSION_DATE) -> tuple[date, ...]:
    return tuple(MARKET_CALENDAR.add_sessions(start, offset) for offset in range(10))


def trending_bars(
    symbol: str,
    *,
    start: Decimal = Decimal("19.00"),
    slope: Decimal = Decimal("0.02"),
    volume: int = 5_000_000,
) -> tuple[BarFixture, ...]:
    values: list[BarFixture] = []
    dates = session_dates()
    for index, day in enumerate(dates):
        with localcontext() as context:
            context.prec = 50
            close = start + slope * Decimal(index)
        if index == 55:
            close = values[-1].close - Decimal("0.01")
        daily_volume = 4_000_000 if index == 55 else volume
        if index == 59:
            daily_volume = 7_000_000
        values.append(
            BarFixture(
                symbol=symbol,
                timestamp=datetime.combine(day, time(16), ET).astimezone(UTC),
                open=close - Decimal("0.02"),
                high=close + Decimal("0.20"),
                low=close - Decimal("0.20"),
                close=close,
                volume=daily_volume,
                source_observation_id=f"{symbol}-{day.isoformat()}",
            )
        )
    return tuple(values)


def descending_bars(symbol: str) -> tuple[BarFixture, ...]:
    return trending_bars(
        symbol,
        start=Decimal("25.00"),
        slope=Decimal("-0.05"),
    )


def constant_bars(
    symbol: str,
    *,
    close: Decimal = Decimal("100"),
    volume: int = 1_000_000,
    half_range: Decimal = Decimal("1"),
) -> tuple[BarFixture, ...]:
    return tuple(
        BarFixture(
            symbol=symbol,
            timestamp=datetime.combine(day, time(16), ET).astimezone(UTC),
            open=close,
            high=close + half_range,
            low=close - half_range,
            close=close,
            volume=volume,
            source_observation_id=f"{symbol}-{day.isoformat()}",
        )
        for day in session_dates()
    )


def breakout_bars(symbol: str = PRIMARY_SYMBOL) -> tuple[BarFixture, ...]:
    return constant_bars(
        symbol,
        close=Decimal("20"),
        volume=5_000_000,
        half_range=Decimal("0.20"),
    )


def load_bar_fixture(relative: str) -> tuple[BarFixture, ...]:
    document = json.loads((FIXTURE_ROOT / relative).read_text(encoding="utf-8"))
    if not isinstance(document, list):
        raise TypeError("bar fixture must be a list")
    values: list[BarFixture] = []
    for raw in document:
        if not isinstance(raw, dict):
            raise TypeError("bar fixture row must be an object")
        values.append(
            BarFixture(
                symbol=str(raw["symbol"]),
                timestamp=datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00")),
                open=Decimal(str(raw["o"])),
                high=Decimal(str(raw["h"])),
                low=Decimal(str(raw["l"])),
                close=Decimal(str(raw["c"])),
                volume=int(raw["v"]),
                adjustment=str(raw.get("adjustment", "split")),
                source_observation_id=str(raw.get("id", "fixture")),
            )
        )
    return tuple(values)


def previous_quote(
    *,
    symbol: str = PRIMARY_SYMBOL,
    spread_percent: Decimal = Decimal("0.0015"),
) -> QuoteFixture:
    midpoint = Decimal("20")
    with localcontext() as context:
        context.prec = 50
        half_spread = midpoint * spread_percent / Decimal("2")
    return QuoteFixture(
        symbol=symbol,
        timestamp=datetime(2026, 8, 13, 15, 58, tzinfo=ET),
        bid=midpoint - half_spread,
        ask=midpoint + half_spread,
        feed="sip",
        source_observation_id="previous-sip-quote",
    )


def latest_iex_quote(
    *,
    symbol: str = PRIMARY_SYMBOL,
    age_seconds: int = 120,
) -> QuoteFixture:
    return QuoteFixture(
        symbol=symbol,
        timestamp=RUN_AT - timedelta(seconds=age_seconds),
        bid=Decimal("20.00"),
        ask=Decimal("20.02"),
        feed="iex",
        age_seconds=age_seconds,
        source_observation_id="latest-iex-quote",
    )


def _iso_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _record_document(value: EvidenceRecord) -> dict[str, object]:
    return {
        "accession": value.accession,
        "adverse_tags": list(value.adverse_tags),
        "classification_ambiguous": value.classification_ambiguous,
        "conflicts": list(value.conflicts),
        "event_date": value.event_date.isoformat() if value.event_date else None,
        "event_kind": value.event_kind,
        "event_type": value.event_type,
        "fact": value.fact,
        "issuer_cik": value.issuer_cik,
        "primary_url": value.primary_url,
        "published_at": _iso_timestamp(value.published_at),
        "publisher": value.publisher,
        "record_id": value.record_id,
        "retrieved_at": _iso_timestamp(value.retrieved_at),
        "source_observation_ids": list(value.source_observation_ids),
        "symbol": value.symbol,
    }


def _primary_evidence_body(
    value: EvidenceRecord,
    *,
    subject_kind: str,
) -> bytes:
    payload = {
        "kind": "REVIEWED_PRIMARY_EVIDENCE",
        "records": [_record_document(value)],
        "schema_version": 1,
        "source_observation_id": value.source_observation_ids[0],
        "subject": {
            "issuer_cik": value.issuer_cik,
            "subject_kind": subject_kind,
            "symbol": value.symbol,
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _reviewed_record(
    *,
    sequence: int,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
    event_type: str,
    published_at: datetime,
    retrieved_at: datetime,
    adverse_tags: tuple[str, ...] = (),
    ambiguities: tuple[str, ...] = (),
    conflicts: tuple[str, ...] = (),
    event_date: date | None = None,
    event_kind: str | None = None,
) -> EvidenceRecord:
    identifier = f"evidence-{symbol.lower()}-{sequence:03d}"
    accession = (
        f"{issuer_cik}-26-{sequence:06d}" if issuer_cik is not None else None
    )
    if issuer_cik is None:
        primary_url = (
            "https://www.ssga.com/us/en/intermediary/etfs/"
            f"reviewed-{symbol.lower()}-notice-{sequence}"
        )
        publisher = "State Street Global Advisors"
    else:
        primary_url = (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{int(issuer_cik)}/{accession.replace('-', '')}/filing.htm"
        )
        publisher = "U.S. Securities and Exchange Commission"
    provisional = EvidenceRecord(
        record_id=f"record-{symbol.lower()}-{sequence:03d}",
        symbol=symbol,
        issuer_cik=issuer_cik,
        primary_url=primary_url,
        publisher=publisher,
        published_at=published_at,
        retrieved_at=retrieved_at,
        event_type=event_type,
        fact="The reviewed primary source records a qualifying event.",
        content_hash="0" * 64,
        source_observation_ids=(identifier,),
        accession=accession,
        adverse_tags=adverse_tags,
        conflicts=conflicts,
        classification_ambiguous=bool(ambiguities),
        event_date=event_date,
        event_kind=event_kind,
    )
    body = _primary_evidence_body(provisional, subject_kind=subject_kind)
    return replace(provisional, content_hash=hashlib.sha256(body).hexdigest())


def _source_binding(
    value: EvidenceRecord,
    *,
    subject_kind: str,
    healthy: bool,
) -> EvidenceSourceBinding:
    body = _primary_evidence_body(value, subject_kind=subject_kind)
    if value.issuer_cik is None:
        source_type = "OFFICIAL_REFERENCE"
        timestamp_source = "PRIMARY_METADATA"
        source_role = (
            f"ISSUER_IR:{value.symbol}"
            if value.event_type == "fund sponsor notice"
            else f"CORPORATE_ACTION:{value.symbol}"
        )
    else:
        source_type = "SEC_ARCHIVE"
        timestamp_source = "SEC_FILING_METADATA"
        source_role = None
    document = SourceDocument(
        url=value.primary_url,
        published_at=value.published_at,
        retrieved_at=value.retrieved_at,
        content_hash=hashlib.sha256(body).hexdigest(),
        body=body,
        source_observation_id=value.source_observation_ids[0],
        publisher=value.publisher,
        source_type=source_type,
        timestamp_source=timestamp_source,
        accession=value.accession,
        source_role=source_role,
    )
    return EvidenceSourceBinding.from_document(
        document,
        symbol=value.symbol,
        issuer_cik=value.issuer_cik,
        checked_at=value.retrieved_at,
        valid_until=value.retrieved_at + timedelta(hours=24),
        healthy=healthy,
    )


def _coverage_attestations(
    *,
    identifier: str,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
    binary_event_coverage: str,
    etf_action_coverage: str,
    coverage_start: date,
    coverage_end: date,
    checked_at: datetime,
    healthy: bool,
) -> tuple[EvidenceCoverageAttestation, ...]:
    return tuple(
        EvidenceCoverageAttestation(
            subject_kind=subject_kind,
            symbol=symbol,
            issuer_cik=issuer_cik,
            coverage_kind=kind,
            coverage=coverage,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            source_observation_ids=(identifier,),
            checked_at=checked_at,
            valid_until=checked_at + timedelta(hours=24),
            healthy=healthy,
            complete=True,
            conflicts=(),
        )
        for kind, coverage in (
            ("BINARY_EVENT", binary_event_coverage),
            ("ETF_ACTION", etf_action_coverage),
        )
    )


def _coverage_document(value: EvidenceCoverageAttestation) -> dict[str, object]:
    return {
        "checked_at": _iso_timestamp(value.checked_at),
        "complete": value.complete,
        "conflicts": list(value.conflicts),
        "coverage": value.coverage,
        "coverage_end": value.coverage_end.isoformat(),
        "coverage_kind": value.coverage_kind,
        "coverage_start": value.coverage_start.isoformat(),
        "healthy": value.healthy,
        "issuer_cik": value.issuer_cik,
        "source_observation_ids": list(value.source_observation_ids),
        "subject_kind": value.subject_kind,
        "symbol": value.symbol,
        "valid_until": _iso_timestamp(value.valid_until),
    }


def _coverage_binding(
    *,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
    binary_event_coverage: str,
    etf_action_coverage: str,
    coverage_start: date,
    coverage_end: date,
    checked_at: datetime,
    healthy: bool,
) -> tuple[EvidenceSourceBinding, tuple[EvidenceCoverageAttestation, ...]]:
    identifier = f"coverage-{symbol.lower()}"
    attestations = _coverage_attestations(
        identifier=identifier,
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        binary_event_coverage=binary_event_coverage,
        etf_action_coverage=etf_action_coverage,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        checked_at=checked_at,
        healthy=healthy,
    )
    payload = {
        "attestations": [_coverage_document(value) for value in attestations],
        "kind": "REVIEWED_EVIDENCE_COVERAGE",
        "schema_version": 1,
        "source_observation_id": identifier,
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    document = SourceDocument(
        url="https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        published_at=checked_at,
        retrieved_at=checked_at,
        content_hash=hashlib.sha256(body).hexdigest(),
        body=body,
        source_observation_id=identifier,
        publisher="Nasdaq",
        source_type="OFFICIAL_REFERENCE",
        timestamp_source="PRIMARY_METADATA",
        source_role="CROSS_CHECK_CALENDAR",
    )
    binding = EvidenceSourceBinding.from_document(
        document,
        symbol=symbol,
        issuer_cik=issuer_cik,
        checked_at=checked_at,
        valid_until=checked_at + timedelta(hours=24),
        healthy=healthy,
    )
    return binding, attestations


def _binding_document(value: EvidenceSourceBinding) -> dict[str, object]:
    document = value.document
    return {
        "accession": document.accession,
        "checked_at": _iso_timestamp(value.checked_at),
        "content_hash": document.content_hash,
        "healthy": value.healthy,
        "issuer_cik": value.issuer_cik,
        "primary_url": document.url,
        "published_at": (
            _iso_timestamp(document.published_at)
            if document.published_at is not None
            else None
        ),
        "publisher": document.publisher,
        "retrieved_at": _iso_timestamp(document.retrieved_at),
        "source_observation_id": document.source_observation_id,
        "source_role": document.source_role,
        "source_type": document.source_type,
        "symbol": value.symbol,
        "timestamp_source": document.timestamp_source,
        "valid_until": _iso_timestamp(value.valid_until),
    }


@lru_cache(maxsize=None)
def evidence(
    *,
    subject_kind: str = "STOCK",
    symbol: str = PRIMARY_SYMBOL,
    issuer_cik: str | None = "0000000000",
    age_days: int = 31,
    event_type: str = "material agreement",
    retrieved_at: datetime | None = RUN_AT - timedelta(hours=1),
    block_reason: str | None = None,
    binary_events: tuple[tuple[date, str | None], ...] = (),
    etf_actions: tuple[tuple[date, str | None], ...] = (),
    binary_event_coverage: str = "CONFIRMED_CLEAR",
    etf_action_coverage: str = "NOT_APPLICABLE",
    health: str = "HEALTHY",
    adverse_tags: tuple[str, ...] = (),
    ambiguities: tuple[str, ...] = (),
    conflicts: tuple[str, ...] = (),
) -> EvidenceDecision:
    if block_reason is not None:
        raise ValueError("Task 5 evidence fixtures derive block reasons from raw facts")
    observed_at = retrieved_at or RUN_AT - timedelta(hours=1)
    healthy = health == "HEALTHY"
    event_specs = tuple((event[0], "BINARY_EVENT") for event in binary_events) + tuple(
        (event[0], "ETF_ACTION") for event in etf_actions
    )
    first_event = event_specs[0] if event_specs else (None, None)
    records = [
        _reviewed_record(
            sequence=1,
            subject_kind=subject_kind,
            symbol=symbol,
            issuer_cik=issuer_cik,
            event_type=event_type,
            published_at=RUN_AT - timedelta(days=age_days, hours=2),
            retrieved_at=observed_at,
            adverse_tags=adverse_tags,
            ambiguities=ambiguities,
            conflicts=conflicts,
            event_date=first_event[0],
            event_kind=first_event[1],
        )
    ]
    for sequence, (event_date, event_kind) in enumerate(event_specs[1:], start=2):
        records.append(
            _reviewed_record(
                sequence=sequence,
                subject_kind=subject_kind,
                symbol=symbol,
                issuer_cik=issuer_cik,
                event_type=event_type,
                published_at=RUN_AT - timedelta(days=age_days, hours=2),
                retrieved_at=observed_at,
                event_date=event_date,
                event_kind=event_kind,
            )
        )
    bindings = [
        _source_binding(value, subject_kind=subject_kind, healthy=healthy)
        for value in records
    ]
    coverage_binding, attestations = _coverage_binding(
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        binary_event_coverage=binary_event_coverage,
        etf_action_coverage=etf_action_coverage,
        coverage_start=hold_sessions()[0],
        coverage_end=hold_sessions()[-1],
        checked_at=observed_at,
        healthy=healthy,
    )
    bindings.append(coverage_binding)
    ordered_bindings = tuple(
        sorted(bindings, key=lambda value: value.source_observation_id)
    )
    registry_document = {
        "coverage_attestations": [
            _coverage_document(value) for value in attestations
        ],
        "kind": "REVIEWED_EVIDENCE_BUNDLE",
        "records": [
            {**_record_document(value), "content_hash": value.content_hash}
            for value in records
        ],
        "registry_id": f"task5-{subject_kind.lower()}-{symbol.lower()}",
        "reviewed_at": _iso_timestamp(RUN_AT),
        "schema_version": 3,
        "source_bindings": [
            _binding_document(value) for value in ordered_bindings
        ],
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    payload = json.dumps(
        registry_document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256(payload).hexdigest()
    with tempfile.TemporaryDirectory() as directory:
        project_root = Path(directory)
        registry_path = project_root / "data" / "evidence" / "legacy"
        registry_path.mkdir(parents=True)
        (registry_path / "subjectless.json").write_bytes(payload)
        with mock.patch.object(
            evidence_module,
            "CURRENT_EVIDENCE_REGISTRY_SHA256",
            digest,
        ):
            authority = evidence_module.load_current_evidence_bundle(
                project_root,
                as_of=RUN_AT,
                source_documents={
                    value.source_observation_id: value.document
                    for value in ordered_bindings
                },
            )
    return classify_evidence(
        tuple(records),
        DateRange(hold_sessions()[0], hold_sessions()[-1]),
        symbol=symbol,
        issuer_cik=issuer_cik,
        source_bindings=ordered_bindings,
        as_of=RUN_AT,
        subject_kind=subject_kind,
        coverage_attestations=attestations,
        reviewed_bundle=authority,
    )


def _cohort_instrument_bars() -> dict[str, tuple[BarFixture, ...]]:
    primary = trending_bars(PRIMARY_SYMBOL)
    secondary_values = list(trending_bars(SECONDARY_SYMBOL))
    five_session_start = secondary_values[-6]
    close = five_session_start.close + Decimal("0.10")
    secondary_values[-6] = replace(
        five_session_start,
        open=close - Decimal("0.02"),
        high=close + Decimal("0.20"),
        low=close - Decimal("0.20"),
        close=close,
    )
    return {
        PRIMARY_SYMBOL: primary,
        SECONDARY_SYMBOL: tuple(secondary_values),
        "NVDA": trending_bars(
            "NVDA", slope=Decimal("0.013"), volume=6_000_000
        ),
        "QQQ": trending_bars(
            "QQQ", slope=Decimal("0.04"), volume=6_000_000
        ),
        "SPY": trending_bars(
            "SPY", slope=Decimal("0.01"), volume=6_000_000
        ),
        "VTI": trending_bars(
            "VTI", slope=Decimal("0.012"), volume=6_000_000
        ),
        "XLK": trending_bars(
            "XLK", slope=Decimal("0.015"), volume=6_000_000
        ),
    }


def _raw_candidate_context(symbol: str):
    from stock_monitor.screening import (
        CandidateContext,
        build_market_session_attestation,
    )

    instruments = _cohort_instrument_bars()
    record = TEST_UNIVERSE.by_symbol[symbol]
    is_etf = record.product_type == "etf"
    issuer_cik = None if is_etf else "0000000000"
    sessions = hold_sessions()
    session_attestation = build_market_session_attestation(
        MARKET_CALENDAR,
        session_date=SESSION_DATE,
        as_of=RUN_AT,
    )
    return CandidateContext(
        record=record,
        bars_by_symbol=instruments,
        previous_session_quote=previous_quote(symbol=symbol),
        latest_iex_quote=latest_iex_quote(symbol=symbol),
        instrument_status=reviewed_instrument_status(
            symbol,
            record.listing_venue,
        ),
        evidence=evidence(
            subject_kind="ETF" if is_etf else "STOCK",
            symbol=symbol,
            issuer_cik=issuer_cik,
            event_type=("fund sponsor notice" if is_etf else "material agreement"),
            binary_event_coverage=(
                "NOT_APPLICABLE" if is_etf else "CONFIRMED_CLEAR"
            ),
            etf_action_coverage=(
                "CONFIRMED_CLEAR" if is_etf else "NOT_APPLICABLE"
            ),
        ),
        issuer_cik=issuer_cik,
        initial_listing_date=date(2025, 1, 1),
        listing_date_status="VERIFIED",
        session_date=SESSION_DATE,
        previous_session_date=date(2026, 8, 13),
        as_of=RUN_AT,
        hold_sessions=sessions,
        session_attestation=session_attestation,
        market_calendar=MARKET_CALENDAR,
        rumor_dependent=False,
        relative_strength_cohort=None,
    )


def universe_candidate_contexts() -> tuple[Any, ...]:
    return tuple(_raw_candidate_context(record.symbol) for record in _UNIVERSE_RECORDS)


def candidate_context(**overrides: Any):
    from stock_monitor.screening import build_base_eligible_cohort

    contexts = list(universe_candidate_contexts())
    if overrides:
        candidate_index = next(
            index
            for index, item in enumerate(contexts)
            if item.record.symbol == PRIMARY_SYMBOL
        )
        contexts[candidate_index] = replace(contexts[candidate_index], **overrides)
    decision = build_base_eligible_cohort(
        tuple(contexts),
        universe=TEST_UNIVERSE,
    )
    if decision.status != "READY":
        raise AssertionError(f"Task 5 cohort fixture is invalid: {decision.reason_codes}")
    return next(
        item for item in decision.contexts if item.record.symbol == PRIMARY_SYMBOL
    )


def with_record(context: Any, **overrides: Any):
    record = replace(context.record, **overrides)
    if record.product_type == "etf":
        return replace(
            context,
            record=record,
            issuer_cik=None,
            evidence=evidence(
                subject_kind="ETF",
                symbol=record.symbol,
                issuer_cik=None,
                event_type="fund sponsor notice",
                binary_event_coverage="NOT_APPLICABLE",
                etf_action_coverage="CONFIRMED_CLEAR",
            ),
        )
    return replace(context, record=record)


def with_candidate_bars(context: Any, bars: tuple[BarFixture, ...]):
    mapping = dict(context.bars_by_symbol)
    mapping[context.record.symbol] = bars
    return replace(context, bars_by_symbol=mapping)
