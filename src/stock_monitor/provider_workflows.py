"""Owner-bound material transport for provider-backed monitor workflows.

This module deliberately stops before publication.  It seals the exact
Journal-backed inputs that a canonical coordinator has already composed so the
workflow layer can reject copied, stale, cross-Journal, or mutated material
before it acquires a durable report claim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo


_LOWER_SHA256 = frozenset("0123456789abcdef")
_MATERIAL_AUTHORITY_LOCK = threading.Lock()
_NEW_YORK = ZoneInfo("America/New_York")
_CANONICAL_REASON = re.compile(r"- `([A-Z][A-Z0-9_]{0,63})`\Z")
_CANONICAL_DATA_REASONS = frozenset(
    {
        "DATA_UNAVAILABLE",
        "PROVIDER_CHECK_FAILED",
        "SOURCE_CHECK_FAILED",
        "STALE_CALENDAR",
        "STALE_UNIVERSE",
    }
)
_PREMARKET_OUTCOME_REASONS = {
    "CANDIDATES": frozenset(
        {("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED")}
    ),
    "NO TRADE": frozenset(
        {
            ("ACTIVE_BREAKER",),
            ("MARKET_CLOSED",),
            ("NO_CANDIDATES",),
        }
    ),
    "NO NEW TRADE - DATA UNAVAILABLE": frozenset(
        (reason,) for reason in _CANONICAL_DATA_REASONS
    ),
}
_CLOSE_BRANCHES = (
    (
        "RECONCILIATION_REQUIRED",
        "RECONCILIATION REQUIRED",
        "RECONCILIATION_REQUIRED",
        5,
    ),
    (
        "POSITION_UNVERIFIED",
        "POSITION UNVERIFIED",
        "POSITION_UNVERIFIED",
        4,
    ),
    ("STOP_UNVERIFIED", "STOP UNVERIFIED", "STOP_UNVERIFIED", 4),
    ("DATA_UNAVAILABLE", "DATA UNAVAILABLE", "DATA_UNAVAILABLE", 3),
    (
        "EXIT",
        "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
        "EXIT",
        0,
    ),
    (
        "TIGHTEN_STOP",
        "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
        "TIGHTEN_STOP",
        0,
    ),
    (
        "HOLD",
        "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
        "HOLD",
        0,
    ),
)
_CLOSE_BRANCH_PROJECTIONS = {
    status: (report_outcome, workflow_outcome, exit_code)
    for status, report_outcome, workflow_outcome, exit_code in _CLOSE_BRANCHES
}
_COORDINATOR_CLOSE_REASONS = frozenset(
    {"MANUAL_VERIFICATION_REQUIRED", "NO_ACTUAL_POSITIONS"}
)
_CLOSE_OUTCOME_EXIT_CODES = {
    report_outcome: exit_code
    for _status, report_outcome, _workflow_outcome, exit_code in _CLOSE_BRANCHES
}
_ALPACA_RECEIPT_CONTRACTS = {
    "ALPACA_DAILY_BARS": (
        "/v2/stocks/bars",
        "sip",
        ("symbols", "timeframe", "start", "end", "adjustment", "feed", "limit"),
        {"timeframe": "1Day", "adjustment": "split", "feed": "sip", "limit": "10000"},
    ),
    "ALPACA_INTRADAY_BARS": (
        "/v2/stocks/bars",
        "sip",
        ("symbols", "timeframe", "start", "end", "adjustment", "feed", "limit"),
        {"timeframe": "1Min", "adjustment": "split", "feed": "sip", "limit": "10000"},
    ),
    "ALPACA_HISTORICAL_QUOTES": (
        "/v2/stocks/quotes",
        "sip",
        ("symbols", "start", "end", "feed", "limit"),
        {"feed": "sip", "limit": "10000"},
    ),
    "ALPACA_LATEST_QUOTES": (
        "/v2/stocks/quotes/latest",
        "iex",
        ("symbols", "feed"),
        {"feed": "iex"},
    ),
}
_OFFICIAL_REFERENCE_SOURCES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": frozenset(
        {"New York Stock Exchange", "www.nyse.com"}
    ),
    "https://www.nyse.com/trade/hours-calendars": frozenset(
        {"New York Stock Exchange", "www.nyse.com"}
    ),
}
_OFFICIAL_REFERENCE_ROLES = {
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
_SEC_PUBLISHER = "U.S. Securities and Exchange Commission"
_SEC_RECEIPT_FEEDS = {
    "SEC_ARCHIVE": frozenset(
        {"SEC_ACCEPTANCE_METADATA", "SEC_FILING_METADATA"}
    ),
    "SEC_SUBMISSIONS": frozenset(
        {
            "SEC_ACCEPTANCE_METADATA",
            "SEC_SUBMISSIONS_METADATA",
            "UNAVAILABLE",
        }
    ),
}
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}\Z")
_CIK = re.compile(r"[0-9]{10}\Z")
_SAFE_SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SCOPED_REFERENCE_ROLE = re.compile(
    r"(?:ISSUER_IR|CORPORATE_ACTION):([A-Z][A-Z0-9.-]{0,14})\Z"
)
_OFFICIAL_REFERENCE_DETAIL_KEYS = frozenset(
    {
        "accession",
        "issuer_cik",
        "source_observation_id",
        "source_role",
        "symbol",
        "timestamp_source",
    }
)
_SEC_ARCHIVE_PATH = re.compile(
    r"/Archives/edgar/data/[1-9][0-9]*/[0-9]{18}/"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z"
)
_SEC_SUBMISSIONS_PATH = re.compile(r"/submissions/CIK[0-9]{10}\.json\Z")
_REVIEWED_REGISTRY_URI = re.compile(
    r"urn:stock-monitor:reviewed-evidence-registry:"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z"
)
_REVIEWED_ARTIFACT_URI = re.compile(
    r"stock-monitor://reviewed/"
    r"(?P<role>[a-z0-9][a-z0-9-]{0,63})/"
    r"(?P<digest>[0-9a-f]{64})\Z"
)
_UTC_QUERY_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{6})?Z\Z"
)
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-F]{2})")


class CanonicalMaterialError(ValueError):
    """Canonical workflow material is malformed or lacks current authority."""


@dataclass(frozen=True, slots=True)
class PremarketSourceBinding:
    """Requested decision role for one exact receipt/source-object pair.

    This caller-authored value is only a request.  It gains authority only
    through :func:`issue_canonical_premarket_source_binding_authority`.
    """

    receipt: object
    source: object
    decision_basis: str
    disclosure: object | None = None

    def __post_init__(self) -> None:
        from .journal import SourceObservationReceipt

        if type(self.receipt) is not SourceObservationReceipt:
            raise CanonicalMaterialError(
                "premarket source binding requires an exact receipt"
            )
        if self.source is None:
            raise CanonicalMaterialError(
                "premarket source binding requires an exact source object"
            )
        if self.decision_basis not in {
            "ECONOMIC_INPUT",
            "OPERATIONAL_HEALTH_ONLY",
        }:
            raise CanonicalMaterialError(
                "premarket source binding decision basis is invalid"
            )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketSourceBindingAuthority:
    """Journal-owner-bound capability for exact premarket source objects."""

    decision_at: datetime
    retrieved_at: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    decision_basis: tuple[tuple[int, str, str], ...]
    binding_digest: str

    def __post_init__(self) -> None:
        decision_at = _require_time(self.decision_at, "premarket decision time")
        retrieved_at = _require_time(self.retrieved_at, "premarket retrieval time")
        _validate_premarket_times(
            decision_at.astimezone(_NEW_YORK).date(),
            decision_at,
            retrieved_at,
        )
        _require_receipt_manifest(self.receipt_manifest)
        _require_premarket_decision_basis(self.decision_basis)
        if tuple(item[0] for item in self.decision_basis) != tuple(
            item[0] for item in self.receipt_manifest
        ):
            raise CanonicalMaterialError(
                "premarket decision basis conflicts with its receipt manifest"
            )
        _require_digest(self.binding_digest, "premarket source binding digest")


@dataclass(frozen=True, slots=True)
class _PremarketSourceBindingCandidate:
    authority_reference: ReferenceType[object]
    authority_fingerprint: object
    journal_reference: ReferenceType[object]
    journal_generation: int
    receipt_candidates: tuple[object, ...]
    bindings: tuple[PremarketSourceBinding, ...]
    binding_manifest: tuple[object, ...]


_PREMARKET_SOURCE_BINDING_LOCK = threading.Lock()
_ISSUED_PREMARKET_SOURCE_BINDINGS: dict[
    int,
    _PremarketSourceBindingCandidate,
] = {}


def _freeze_scoped_reference_sources() -> frozenset[
    tuple[str, str | None, str, str]
]:
    """Snapshot the reviewed subject authorities into callback-free primitives."""
    from . import evidence as evidence_module

    authorities = evidence_module._SCOPED_REFERENCE_AUTHORITIES
    if type(authorities) is not dict or not authorities:
        raise CanonicalMaterialError(
            "reviewed scoped reference authorities are malformed"
        )
    frozen: set[tuple[str, str | None, str, str]] = set()
    for role, authority in authorities.items():
        if (
            type(role) is not str
            or _SCOPED_REFERENCE_ROLE.fullmatch(role) is None
            or type(authority) is not tuple
            or len(authority) != 2
        ):
            raise CanonicalMaterialError(
                "reviewed scoped reference authorities are malformed"
            )
        issuer_cik, sources = authority
        invalid_issuer_cik = issuer_cik is not None and (
            type(issuer_cik) is not str
            or _CIK.fullmatch(issuer_cik) is None
        )
        if (
            invalid_issuer_cik
            or type(sources) is not frozenset
            or not sources
        ):
            raise CanonicalMaterialError(
                "reviewed scoped reference authorities are malformed"
            )
        for source in sources:
            if (
                type(source) is not tuple
                or len(source) != 2
                or type(source[0]) is not str
                or type(source[1]) is not str
                or not source[0]
                or not source[1]
            ):
                raise CanonicalMaterialError(
                    "reviewed scoped reference authorities are malformed"
                )
            frozen.add((role, issuer_cik, source[0], source[1]))
    return frozenset(frozen)


_SCOPED_REFERENCE_SOURCES = _freeze_scoped_reference_sources()


class CanonicalWorkflowAdapter(Protocol):
    """Compose exact provider-backed material at fixed economic cutoffs."""

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalPremarketMaterial: ...

    def close_material(
        self,
        session_date: date,
        *,
        review_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalCloseMaterial: ...


def _require_digest(value: object, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or not set(value).issubset(_LOWER_SHA256)
    ):
        raise CanonicalMaterialError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_session(value: object) -> date:
    if type(value) is not date:
        raise CanonicalMaterialError("canonical material session must be an exact date")
    return value


def _require_time(value: object, label: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise CanonicalMaterialError(f"{label} must be timezone-aware")
    if type(value.tzinfo) not in {timezone, ZoneInfo}:
        raise CanonicalMaterialError(
            f"{label} timezone implementation is not immutable"
        )
    try:
        offset = value.utcoffset()
    except Exception as error:
        raise CanonicalMaterialError(f"{label} timezone is invalid") from error
    if offset is None:
        raise CanonicalMaterialError(f"{label} must be timezone-aware")
    return value


def _require_same_new_york_session(
    value: datetime,
    session_date: date,
    label: str,
) -> None:
    if value.astimezone(_NEW_YORK).date() != session_date:
        raise CanonicalMaterialError(
            f"{label} must fall in the material New York session"
        )


def _new_york_clock(value: datetime) -> time:
    local = value.astimezone(_NEW_YORK)
    return time(local.hour, local.minute, local.second, local.microsecond)


def _validate_premarket_times(
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
) -> None:
    _require_same_new_york_session(
        decision_at,
        session_date,
        "premarket decision time",
    )
    _require_same_new_york_session(
        retrieved_at,
        session_date,
        "premarket retrieval time",
    )
    if _new_york_clock(decision_at) != time(8, 45):
        raise CanonicalMaterialError(
            "premarket economic decision must be exactly 08:45 New York time"
        )
    if decision_at > retrieved_at:
        raise CanonicalMaterialError(
            "premarket retrieval cannot precede its economic decision"
        )


def _validate_close_times(
    session_date: date,
    review_at: datetime,
    query_cutoff: datetime,
    retrieved_at: datetime,
) -> None:
    for value, label in (
        (review_at, "close review time"),
        (query_cutoff, "close query cutoff"),
        (retrieved_at, "close retrieval time"),
    ):
        _require_same_new_york_session(value, session_date, label)
    if _new_york_clock(review_at) not in {time(12, 30), time(15, 30)}:
        raise CanonicalMaterialError(
            "close economic review must be exactly 12:30 or 15:30 New York time"
        )
    if not review_at <= query_cutoff <= retrieved_at:
        raise CanonicalMaterialError(
            "close cutoff must be between review and retrieval"
        )


def _invalid_receipt_source() -> None:
    raise CanonicalMaterialError("canonical receipt source identity is invalid")


def _exact_https_url(value: object, hostname: str):
    if (
        type(value) is not str
        or not value
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
    ):
        _invalid_receipt_source()
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        _invalid_receipt_source()
    if (
        parsed.scheme != "https"
        or parsed.netloc != hostname
        or parsed.hostname != hostname
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or urlunsplit(parsed) != value
    ):
        _invalid_receipt_source()
    return parsed


def _canonical_query_pairs(query: str) -> tuple[tuple[str, str], ...]:
    if not query or _INVALID_PERCENT_ESCAPE.search(query):
        _invalid_receipt_source()
    try:
        pairs = tuple(
            parse_qsl(
                query,
                keep_blank_values=True,
                strict_parsing=True,
            )
        )
    except ValueError:
        _invalid_receipt_source()
    if (
        not pairs
        or len({key for key, _value in pairs}) != len(pairs)
        or urlencode(pairs) != query
    ):
        _invalid_receipt_source()
    return pairs


def _alpaca_query_timestamp(value: str) -> datetime:
    if _UTC_QUERY_TIMESTAMP.fullmatch(value) is None:
        _invalid_receipt_source()
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        _invalid_receipt_source()
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        _invalid_receipt_source()
    return parsed


def _validate_alpaca_receipt_source(
    *,
    source_uri: object,
    source_type: str,
    provider: object,
    feed: object,
) -> None:
    contract = _ALPACA_RECEIPT_CONTRACTS[source_type]
    path, required_feed, required_names, fixed_values = contract
    if provider != "alpaca" or feed != required_feed:
        _invalid_receipt_source()
    parsed = _exact_https_url(source_uri, "data.alpaca.markets")
    if parsed.path != path:
        _invalid_receipt_source()
    pairs = _canonical_query_pairs(parsed.query)
    names = tuple(key for key, _value in pairs)
    if names not in {required_names, (*required_names, "page_token")}:
        _invalid_receipt_source()
    values = dict(pairs)
    if any(values.get(key) != expected for key, expected in fixed_values.items()):
        _invalid_receipt_source()
    symbols = tuple(values.get("symbols", "").split(","))
    if (
        not symbols
        or len(symbols) > 200
        or tuple(sorted(set(symbols))) != symbols
        or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
    ):
        _invalid_receipt_source()
    if "start" in values:
        start = _alpaca_query_timestamp(values["start"])
        end = _alpaca_query_timestamp(values["end"])
        if start > end:
            _invalid_receipt_source()
    page_token = values.get("page_token")
    if page_token is not None and (
        not page_token
        or len(page_token) > 1024
        or any(ord(character) < 33 or ord(character) > 126 for character in page_token)
    ):
        _invalid_receipt_source()


def _validate_sec_receipt_source(
    *,
    source_uri: object,
    source_type: str,
    provider: object,
    feed: object,
) -> None:
    if provider != _SEC_PUBLISHER or feed not in _SEC_RECEIPT_FEEDS[source_type]:
        _invalid_receipt_source()
    if source_type == "SEC_ARCHIVE":
        parsed = _exact_https_url(source_uri, "www.sec.gov")
        valid_path = _SEC_ARCHIVE_PATH.fullmatch(parsed.path)
    else:
        parsed = _exact_https_url(source_uri, "data.sec.gov")
        valid_path = _SEC_SUBMISSIONS_PATH.fullmatch(parsed.path)
    if valid_path is None or parsed.query:
        _invalid_receipt_source()


def _official_reference_details(
    value: object,
    *,
    required: bool,
) -> tuple[str, str | None, str | None, str] | None:
    if type(value) is not str:
        _invalid_receipt_source()
    try:
        details = json.loads(value)
        canonical = json.dumps(
            details,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        _invalid_receipt_source()
    if type(details) is not dict or canonical != value:
        _invalid_receipt_source()
    identity_keys = {
        "accession",
        "issuer_cik",
        "source_role",
        "symbol",
        "timestamp_source",
    }
    if not required and not identity_keys.intersection(details):
        return None
    if set(details) != _OFFICIAL_REFERENCE_DETAIL_KEYS:
        _invalid_receipt_source()
    source_role = details["source_role"]
    issuer_cik = details["issuer_cik"]
    symbol = details["symbol"]
    timestamp_source = details["timestamp_source"]
    if (
        details["accession"] is not None
        or type(details["source_observation_id"]) is not str
        or _SAFE_SOURCE_ID.fullmatch(details["source_observation_id"]) is None
        or type(source_role) is not str
        or not source_role
        or (
            issuer_cik is not None
            and (
                type(issuer_cik) is not str
                or _CIK.fullmatch(issuer_cik) is None
            )
        )
        or (
            symbol is not None
            and (
                type(symbol) is not str
                or _SYMBOL.fullmatch(symbol) is None
            )
        )
        or type(timestamp_source) is not str
        or not timestamp_source
    ):
        _invalid_receipt_source()
    return source_role, issuer_cik, symbol, timestamp_source


def _validate_canonical_receipt_source(
    *,
    source_uri: object,
    source_type: object,
    provider: object,
    feed: object,
    health_result: object,
    details_json: object,
) -> None:
    if (
        type(source_type) is not str
        or type(provider) is not str
        or not provider
        or (feed is not None and type(feed) is not str)
        or type(health_result) is not str
    ):
        _invalid_receipt_source()
    if source_type in _ALPACA_RECEIPT_CONTRACTS:
        if health_result != "OK":
            _invalid_receipt_source()
        _validate_alpaca_receipt_source(
            source_uri=source_uri,
            source_type=source_type,
            provider=provider,
            feed=feed,
        )
        return
    if source_type in _SEC_RECEIPT_FEEDS:
        if health_result not in {"OK", "UNHEALTHY"}:
            _invalid_receipt_source()
        _validate_sec_receipt_source(
            source_uri=source_uri,
            source_type=source_type,
            provider=provider,
            feed=feed,
        )
        return
    if source_type == "OFFICIAL_REFERENCE":
        if (
            type(source_uri) is not str
            or health_result not in {"OK", "UNHEALTHY"}
        ):
            _invalid_receipt_source()
        providers = (
            _OFFICIAL_REFERENCE_SOURCES.get(source_uri)
            if type(source_uri) is str
            else None
        )
        if providers is not None:
            if provider not in providers or feed not in {
                "PRIMARY_METADATA",
                "UNAVAILABLE",
            }:
                _invalid_receipt_source()
            details = _official_reference_details(
                details_json,
                required=False,
            )
            if details is not None and details != (
                _OFFICIAL_REFERENCE_ROLES[source_uri],
                None,
                None,
                feed,
            ):
                _invalid_receipt_source()
            return
        details = _official_reference_details(details_json, required=True)
        if details is None:
            _invalid_receipt_source()
        source_role, issuer_cik, symbol, timestamp_source = details
        scoped = _SCOPED_REFERENCE_ROLE.fullmatch(source_role)
        if (
            feed != "PRIMARY_METADATA"
            or timestamp_source != feed
            or scoped is None
            or scoped.group(1) != symbol
            or (source_role, issuer_cik, source_uri, provider)
            not in _SCOPED_REFERENCE_SOURCES
        ):
            _invalid_receipt_source()
        return
    if source_type == "REVIEWED_EVIDENCE_REGISTRY":
        if (
            type(source_uri) is not str
            or _REVIEWED_REGISTRY_URI.fullmatch(source_uri) is None
            or provider != "operator-reviewed"
            or feed is not None
            or health_result != "REVIEWED"
        ):
            _invalid_receipt_source()
        return
    if source_type == "REVIEWED_ARTIFACT":
        if (
            type(source_uri) is not str
            or _REVIEWED_ARTIFACT_URI.fullmatch(source_uri) is None
            or provider != "operator-reviewed"
            or feed is not None
            or health_result != "REVIEWED"
        ):
            _invalid_receipt_source()
        return
    _invalid_receipt_source()


def canonical_source_receipts(value: object) -> tuple[object, ...]:
    """Return the sole deterministic ordering for canonical source receipts."""
    from .journal import SourceObservationReceipt

    if type(value) is not tuple or not value or any(
        type(receipt) is not SourceObservationReceipt for receipt in value
    ):
        raise CanonicalMaterialError(
            "canonical material requires exact source observation receipts"
        )
    receipts = value
    for receipt in receipts:
        if type(receipt.row_id) is not int or receipt.row_id < 1:
            raise CanonicalMaterialError("canonical receipt row identity is invalid")
        _require_digest(
            receipt.observation_sha256,
            "source observation digest",
        )
        _require_digest(receipt.payload_sha256, "source payload digest")
        _require_digest(receipt.source_digest, "source receipt digest")
        _validate_canonical_receipt_source(
            source_uri=receipt.source_uri,
            source_type=receipt.source_type,
            provider=receipt.provider,
            feed=receipt.feed,
            health_result=receipt.health_result,
            details_json=receipt.details_json,
        )
        source_time = _require_time(receipt.source_time, "receipt source time")
        retrieved_at = _require_time(
            receipt.retrieved_at,
            "receipt retrieval time",
        )
        if source_time > retrieved_at:
            raise CanonicalMaterialError(
                "canonical receipt source time cannot follow retrieval"
            )
    if (
        len({id(receipt) for receipt in receipts}) != len(receipts)
        or len({receipt.row_id for receipt in receipts}) != len(receipts)
        or len({receipt.observation_sha256 for receipt in receipts}) != len(receipts)
    ):
        raise CanonicalMaterialError("canonical source receipts must be unique")
    return tuple(
        sorted(
            receipts,
            key=lambda receipt: (receipt.observation_sha256, receipt.row_id),
        )
    )


def _receipt_set(value: object) -> tuple[object, ...]:
    return canonical_source_receipts(value)


def _require_canonical_receipt_order(value: object) -> tuple[object, ...]:
    receipts = canonical_source_receipts(value)
    if any(
        current is not expected
        for current, expected in zip(value, receipts, strict=True)
    ):
        raise CanonicalMaterialError(
            "canonical source receipts must use canonical order"
        )
    return receipts


def _validate_receipt_envelope(
    receipts: tuple[object, ...],
    *,
    kind: str,
    economic_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime | None = None,
    decision_basis: tuple[tuple[int, str, str], ...] = (),
) -> None:
    basis_by_row = {
        row_id: (role, basis)
        for row_id, role, basis in decision_basis
    }
    for receipt in receipts:
        if receipt.retrieved_at > retrieved_at:
            raise CanonicalMaterialError(
                f"canonical {kind.lower()} receipt exceeds its retrieval envelope"
            )
        if kind == "PREMARKET":
            role_basis = basis_by_row.get(receipt.row_id)
            is_operational = role_basis is not None and role_basis[1] == (
                "OPERATIONAL_HEALTH_ONLY"
            )
            if is_operational and role_basis[0] not in {
                "ALPACA_LATEST_QUOTES",
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
            }:
                raise CanonicalMaterialError(
                    "canonical premarket operational role is invalid"
                )
            if not is_operational and receipt.source_time > economic_at:
                raise CanonicalMaterialError(
                    "canonical premarket receipt exceeds its economic cutoff"
                )
            continue
        if query_cutoff is None:
            raise CanonicalMaterialError("canonical close query cutoff is unavailable")
        if receipt.source_time > economic_at:
            raise CanonicalMaterialError(
                "canonical close receipt exceeds its economic cutoff"
            )
        if receipt.retrieved_at > query_cutoff:
            raise CanonicalMaterialError(
                "canonical close receipt exceeds its query cutoff"
            )


def _require_premarket_decision_basis(
    value: object,
) -> tuple[tuple[int, str, str], ...]:
    if type(value) is not tuple or not value:
        raise CanonicalMaterialError("premarket decision basis is invalid")
    row_ids: set[int] = set()
    for item in value:
        if (
            type(item) is not tuple
            or len(item) != 3
            or type(item[0]) is not int
            or item[0] < 1
            or item[0] in row_ids
            or type(item[1]) is not str
            or not item[1]
            or item[2] not in {
                "ECONOMIC_INPUT",
                "OPERATIONAL_HEALTH_ONLY",
            }
        ):
            raise CanonicalMaterialError("premarket decision basis is invalid")
        row_ids.add(item[0])
    return value


def _is_issued_provider_fetch_page_bundle(value: object) -> bool:
    """Use the public Alpaca page capability when that provider slice is present."""
    from .providers import alpaca as alpaca_module

    predicate = getattr(
        alpaca_module,
        "is_issued_provider_fetch_page_bundle",
        None,
    )
    return bool(callable(predicate) and predicate(value))


def _reviewed_binding_identity(
    source: object,
    *,
    invoke_owner_predicate: bool = True,
) -> tuple[str, str, date | datetime] | None:
    from . import evidence as evidence_module
    from . import market_calendar as calendar_module
    from . import universe as universe_module

    if type(source) is calendar_module.MarketCalendar:
        if invoke_owner_predicate and not (
            calendar_module.is_release_verified_market_calendar(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed calendar authority is unavailable"
            )
        pin = calendar_module._RELEASE_MANIFEST_SHA256.get(source.year)
        if type(pin) is not str:
            raise CanonicalMaterialError("premarket reviewed calendar pin is missing")
        return "calendar", pin, source.reviewed_at
    if type(source) is universe_module.UniverseSnapshot:
        if invoke_owner_predicate and not (
            universe_module.is_verified_universe_snapshot(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed universe authority is unavailable"
            )
        pin = source._release_pin
        if type(pin) is not str:
            raise CanonicalMaterialError("premarket reviewed universe pin is missing")
        return "universe", pin, source.reviewed_at
    if type(source) is evidence_module.ReviewedEvidenceRelease:
        if invoke_owner_predicate and not (
            evidence_module.is_verified_evidence_release(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed evidence release authority is unavailable"
            )
        return "evidence-release", source.release_sha256, source.reviewed_at
    if type(source) is evidence_module.ReviewedEvidenceBundle:
        if invoke_owner_predicate and not evidence_module._is_reviewed_bundle(
            source
        ):
            raise CanonicalMaterialError(
                "premarket reviewed evidence child authority is unavailable"
            )
        if type(source.symbol) is not str:
            raise CanonicalMaterialError(
                "premarket reviewed evidence child subject is unavailable"
            )
        return (
            f"evidence-{source.symbol.lower()}",
            source.content_hash,
            source.reviewed_at,
        )
    return None


def _document_disclosure_is_current(disclosure: object, document: object) -> bool:
    from .providers.cache import SourceDocument
    from .providers.reference import ReferenceClient
    from .providers.sec import SecClient

    if type(document) is not SourceDocument:
        return False
    if type(disclosure) is SecClient:
        return bool(
            disclosure._documents.get(document.source_observation_id) is document
        )
    if type(disclosure) is ReferenceClient:
        issued = disclosure._documents.get(document.source_observation_id)
        if issued is None or issued[0] is not document:
            return False
        try:
            from .providers import reference as reference_module

            current = reference_module._canonical_digest(
                reference_module._document_fingerprint(document)
            )
        except (TypeError, ValueError):
            return False
        return bool(current == issued[1])
    return False


def _binding_source_role(
    source: object,
    *,
    invoke_owner_predicate: bool,
) -> str:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    reviewed = _reviewed_binding_identity(
        source,
        invoke_owner_predicate=invoke_owner_predicate,
    )
    if reviewed is not None:
        return reviewed[0]
    if type(source) is ProviderFetchPageBundle:
        return source.page.source_type
    if type(source) is SourceDocument:
        return source.source_role or source.source_type
    raise CanonicalMaterialError("premarket source binding type is unsupported")


def _binding_is_operational_only(source: object) -> bool:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    if type(source) is ProviderFetchPageBundle:
        return source.page.source_type == "ALPACA_LATEST_QUOTES"
    if type(source) is SourceDocument:
        return source.source_role in {
            "PRIMARY_HALT_FEED",
            "TRADER_ALERT_HALT",
            "OPERATIONAL_STATUS",
        }
    return False


def _verify_premarket_binding_source(
    binding: PremarketSourceBinding,
    *,
    reviewed_documents: tuple[object, ...],
    invoke_provider_predicate: bool,
) -> tuple[str, object]:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    receipt = binding.receipt
    source = binding.source
    reviewed = _reviewed_binding_identity(
        source,
        invoke_owner_predicate=invoke_provider_predicate,
    )
    if reviewed is not None:
        role, expected_pin, expected_source_time = reviewed
        match = _REVIEWED_ARTIFACT_URI.fullmatch(receipt.source_uri)
        source_time_matches = (
            receipt.source_time == expected_source_time
            if type(expected_source_time) is datetime
            else receipt.source_time.astimezone(_NEW_YORK).date()
            == expected_source_time
        )
        if (
            receipt.source_type != "REVIEWED_ARTIFACT"
            or receipt.provider != "operator-reviewed"
            or receipt.feed is not None
            or receipt.health_result != "REVIEWED"
            or match is None
            or match.group("role") != role
            or match.group("digest") != expected_pin
            or receipt.payload_sha256 != expected_pin
            or hashlib.sha256(receipt.source_payload).hexdigest() != expected_pin
            or not source_time_matches
            or binding.decision_basis != "ECONOMIC_INPUT"
            or receipt.delay_seconds
            != int((receipt.retrieved_at - receipt.source_time).total_seconds())
        ):
            raise CanonicalMaterialError(
                "premarket reviewed artifact bytes or pin are inconsistent"
            )
        return role, (
            "REVIEWED_ARTIFACT",
            role,
            expected_pin,
            type(source).__qualname__,
        )

    if type(source) is ProviderFetchPageBundle:
        if (
            binding.disclosure is not None
            or (invoke_provider_predicate and not _is_issued_provider_fetch_page_bundle(source))
        ):
            raise CanonicalMaterialError(
                "premarket provider page authority is unavailable; "
                "caller-authored disclosures cannot authorize completeness"
            )
        observation = source.observation
        page = source.page
        if (
            receipt.source_payload != source.payload
            or receipt.payload_sha256 != page.payload_sha256
            or hashlib.sha256(source.payload).hexdigest() != page.payload_sha256
            or page.source_observation_id != observation.observation_id
            or page.request_url != observation.url
            or page.source_type != observation.source_type
            or receipt.source_uri != page.request_url
            or receipt.source_type != page.source_type
            or receipt.provider != "alpaca"
            or receipt.feed != observation.feed
            or receipt.source_time != observation.source_timestamp
            or receipt.retrieved_at != observation.retrieved_at
            or receipt.provider_sequence != page.page_ordinal
            or receipt.delay_seconds != observation.delay_seconds
            or receipt.health_result != "OK"
        ):
            raise CanonicalMaterialError(
                "premarket provider receipt does not bind its exact page"
            )
        return page.source_type, (
            "PROVIDER_PAGE",
            _canonical_digest_value(page),
            hashlib.sha256(source.payload).hexdigest(),
            _canonical_digest_value(observation),
        )

    if type(source) is SourceDocument:
        if not any(document is source for document in reviewed_documents) and not (
            _document_disclosure_is_current(binding.disclosure, source)
        ):
            raise CanonicalMaterialError(
                "premarket source document authority is unavailable"
            )
        expected_source_time = source.published_at or source.retrieved_at
        if (
            receipt.source_payload != source.body
            or receipt.payload_sha256 != source.content_hash
            or hashlib.sha256(source.body).hexdigest() != source.content_hash
            or receipt.source_uri != source.url
            or receipt.source_type != source.source_type
            or receipt.provider != source.publisher
            or receipt.feed != source.timestamp_source
            or receipt.source_time != expected_source_time
            or receipt.retrieved_at != source.retrieved_at
            or receipt.delay_seconds
            != int((source.retrieved_at - expected_source_time).total_seconds())
            or receipt.health_result not in {"OK", "UNHEALTHY"}
        ):
            raise CanonicalMaterialError(
                "premarket source receipt does not bind its exact document"
            )
        return source.source_role or source.source_type, (
            "SOURCE_DOCUMENT",
            _canonical_digest_value(source),
        )
    raise CanonicalMaterialError("premarket source binding type is unsupported")


def _ordered_premarket_bindings(
    value: object,
) -> tuple[PremarketSourceBinding, ...]:
    if type(value) is not tuple or not value or any(
        type(binding) is not PremarketSourceBinding for binding in value
    ):
        raise CanonicalMaterialError(
            "premarket source bindings must be a nonempty exact tuple"
        )
    bindings = value
    receipts = canonical_source_receipts(
        tuple(binding.receipt for binding in bindings)
    )
    by_receipt = {id(binding.receipt): binding for binding in bindings}
    if len(by_receipt) != len(bindings):
        raise CanonicalMaterialError("premarket source receipts must be unique")
    return tuple(by_receipt[id(receipt)] for receipt in receipts)


def _premarket_binding_manifest(
    bindings: tuple[PremarketSourceBinding, ...],
    *,
    invoke_provider_predicate: bool,
) -> tuple[tuple[object, ...], ...]:
    from . import evidence as evidence_module
    from .providers.cache import SourceDocument

    releases = tuple(
        binding.source
        for binding in bindings
        if type(binding.source) is evidence_module.ReviewedEvidenceRelease
    )
    if len(releases) > 1:
        raise CanonicalMaterialError("premarket evidence release is duplicated")
    expected_bundles: tuple[object, ...] = ()
    reviewed_documents: tuple[object, ...] = ()
    if releases:
        release = releases[0]
        expected_bundles = tuple(release.by_symbol.values())
        reviewed_documents = tuple(
            source_binding.document
            for bundle in expected_bundles
            for source_binding in bundle.source_bindings
        )
        supplied_bundles = tuple(
            binding.source
            for binding in bindings
            if type(binding.source) is evidence_module.ReviewedEvidenceBundle
        )
        if (
            len(supplied_bundles) != len(expected_bundles)
            or any(
                sum(supplied is expected for supplied in supplied_bundles) != 1
                for expected in expected_bundles
            )
        ):
            raise CanonicalMaterialError(
                "premarket evidence release children are incomplete"
            )
        supplied_documents = tuple(
            binding.source
            for binding in bindings
            if type(binding.source) is SourceDocument
        )
        if (
            len(supplied_documents) != len(reviewed_documents)
            or any(
                sum(supplied is expected for supplied in supplied_documents) != 1
                for expected in reviewed_documents
            )
        ):
            raise CanonicalMaterialError(
                "premarket evidence source documents are incomplete"
            )

    manifest: list[tuple[object, ...]] = []
    for binding in bindings:
        role, source_fingerprint = _verify_premarket_binding_source(
            binding,
            reviewed_documents=reviewed_documents,
            invoke_provider_predicate=invoke_provider_predicate,
        )
        manifest.append(
            (
                binding.receipt.row_id,
                binding.receipt.observation_sha256,
                role,
                binding.decision_basis,
                source_fingerprint,
            )
        )
    return tuple(manifest)


def _validate_premarket_binding_chronology(
    bindings: tuple[PremarketSourceBinding, ...],
    *,
    decision_at: datetime,
    retrieved_at: datetime,
) -> None:
    for binding in bindings:
        receipt = binding.receipt
        if receipt.retrieved_at > retrieved_at or receipt.source_time > receipt.retrieved_at:
            raise CanonicalMaterialError(
                "premarket source receipt exceeds its retrieval envelope"
            )
        operational = _binding_is_operational_only(binding.source)
        if operational != (
            binding.decision_basis == "OPERATIONAL_HEALTH_ONLY"
        ):
            raise CanonicalMaterialError(
                "premarket operational source must be health-only"
            )
        if (
            binding.decision_basis == "ECONOMIC_INPUT"
            and receipt.source_time > decision_at
        ):
            raise CanonicalMaterialError(
                "premarket economic input exceeds its economic cutoff"
            )


def _premarket_decision_basis(
    bindings: tuple[PremarketSourceBinding, ...],
) -> tuple[tuple[int, str, str], ...]:
    return tuple(
        (
            binding.receipt.row_id,
            _binding_source_role(
                binding.source,
                invoke_owner_predicate=False,
            ),
            binding.decision_basis,
        )
        for binding in bindings
    )


def _is_current_premarket_source_binding_without_callbacks(
    authority: object,
    candidate: _PremarketSourceBindingCandidate | None = None,
) -> bool:
    from . import journal as journal_module

    if type(authority) is not CanonicalPremarketSourceBindingAuthority:
        return False
    if candidate is None:
        with _PREMARKET_SOURCE_BINDING_LOCK:
            candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    journal = None if candidate is None else candidate.journal_reference()
    try:
        fingerprint = _value_fingerprint(authority)
        binding_manifest = _premarket_binding_manifest(
            candidate.bindings if candidate is not None else (),
            invoke_provider_predicate=False,
        )
    except Exception:
        return False
    return bool(
        candidate is not None
        and candidate.authority_reference() is authority
        and journal is not None
        and not getattr(journal, "_closed", True)
        and getattr(journal, "_source_generation", None)
        == candidate.journal_generation
        and candidate.authority_fingerprint == fingerprint
        and candidate.binding_manifest == binding_manifest
        and _canonical_receipt_manifest(
            tuple(binding.receipt for binding in candidate.bindings)
        )
        == authority.receipt_manifest
        and _premarket_decision_basis(candidate.bindings)
        == authority.decision_basis
        and _canonical_sha256(
            "stock-monitor/premarket-source-bindings/v1",
            binding_manifest,
        )
        == authority.binding_digest
        and all(
            journal_module._is_current_journal_authority_candidate_without_callbacks(
                receipt_candidate
            )
            for receipt_candidate in candidate.receipt_candidates
        )
    )


def issue_canonical_premarket_source_binding_authority(
    *,
    journal: object,
    decision_at: datetime,
    retrieved_at: datetime,
    bindings: tuple[PremarketSourceBinding, ...],
) -> CanonicalPremarketSourceBindingAuthority:
    """Bind exact source objects to exact current receipts from one Journal."""
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError(
            "premarket source binding requires an open Journal owner"
        )
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(
        decision_at.astimezone(_NEW_YORK).date(),
        decision_at,
        retrieved_at,
    )
    ordered = _ordered_premarket_bindings(bindings)
    _validate_premarket_binding_chronology(
        ordered,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
    )
    # Provider/document verification may consult mutable owners.  Exhaust it
    # before capturing Journal candidates for the callback-free final seal.
    _premarket_binding_manifest(ordered, invoke_provider_predicate=True)
    receipt_candidates = _current_receipt_candidates(
        journal,
        tuple(binding.receipt for binding in ordered),
    )
    if receipt_candidates is None:
        raise CanonicalMaterialError(
            "premarket source receipts lack one current Journal owner"
        )
    binding_manifest = _premarket_binding_manifest(
        ordered,
        invoke_provider_predicate=False,
    )
    decision_basis = _premarket_decision_basis(ordered)
    authority = CanonicalPremarketSourceBindingAuthority(
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        receipt_manifest=_canonical_receipt_manifest(
            tuple(binding.receipt for binding in ordered)
        ),
        decision_basis=decision_basis,
        binding_digest=_canonical_sha256(
            "stock-monitor/premarket-source-bindings/v1",
            binding_manifest,
        ),
    )
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("premarket Journal generation is invalid")
    identity = id(authority)

    def discard(dead: ReferenceType[object]) -> None:
        with _PREMARKET_SOURCE_BINDING_LOCK:
            current = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(identity)
            if current is not None and current.authority_reference is dead:
                _ISSUED_PREMARKET_SOURCE_BINDINGS.pop(identity, None)

    candidate = _PremarketSourceBindingCandidate(
        authority_reference=ref(authority, discard),
        authority_fingerprint=_value_fingerprint(authority),
        journal_reference=ref(journal),
        journal_generation=generation,
        receipt_candidates=receipt_candidates,
        bindings=ordered,
        binding_manifest=binding_manifest,
    )
    with _PREMARKET_SOURCE_BINDING_LOCK:
        _ISSUED_PREMARKET_SOURCE_BINDINGS[identity] = candidate
    if not _is_current_premarket_source_binding_without_callbacks(
        authority,
        candidate,
    ):
        with _PREMARKET_SOURCE_BINDING_LOCK:
            if _ISSUED_PREMARKET_SOURCE_BINDINGS.get(identity) is candidate:
                _ISSUED_PREMARKET_SOURCE_BINDINGS.pop(identity, None)
        raise CanonicalMaterialError(
            "premarket source binding changed during issuance"
        )
    return authority


def is_issued_canonical_premarket_source_binding_authority(
    authority: object,
    *,
    journal: object,
) -> bool:
    """Return whether an exact source binding remains current for one owner."""
    with _PREMARKET_SOURCE_BINDING_LOCK:
        candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    if candidate is None or candidate.journal_reference() is not journal:
        return False
    try:
        _premarket_binding_manifest(
            candidate.bindings,
            invoke_provider_predicate=True,
        )
    except Exception:
        return False
    with _PREMARKET_SOURCE_BINDING_LOCK:
        refreshed = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    return bool(
        refreshed is candidate
        and _is_current_premarket_source_binding_without_callbacks(
            authority,
            candidate,
        )
    )


def _premarket_reviewed_bindings(
    authority: object,
) -> tuple[PremarketSourceBinding, ...]:
    with _PREMARKET_SOURCE_BINDING_LOCK:
        candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    if not _is_current_premarket_source_binding_without_callbacks(
        authority,
        candidate,
    ):
        raise CanonicalMaterialError(
            "premarket source binding authority is unavailable"
        )
    assert candidate is not None
    return tuple(
        binding
        for binding in candidate.bindings
        if binding.receipt.health_result == "REVIEWED"
    )


def _validate_report(
    report: object,
    *,
    kind: str,
    session_date: date,
    state_hash: str,
    receipts: tuple[object, ...],
) -> None:
    from .reports import Report, is_issued_report

    if type(report) is not Report or not is_issued_report(report):
        raise CanonicalMaterialError(
            "canonical material requires an exact renderer-issued report"
        )
    if (
        report.kind != kind
        or report.session_date != session_date
        or report.state_hash != state_hash
    ):
        raise CanonicalMaterialError(
            "canonical report identity conflicts with its material"
        )
    observation_ids = tuple(receipt.observation_sha256 for receipt in receipts)
    if report.observation_ids != observation_ids:
        raise CanonicalMaterialError(
            "canonical report evidence conflicts with its source receipts"
        )


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _canonical_digest_value(value: object) -> object:
    """Return a deterministic JSON value for the bounded material DTO graph."""
    value_type = type(value)
    if value is None or value_type in {bool, int, str}:
        return value
    if value_type is bytes:
        return {
            "type": "bytes-sha256",
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if value_type is Decimal:
        decimal = value.as_tuple()
        return {
            "type": "decimal",
            "sign": decimal.sign,
            "digits": "".join(str(digit) for digit in decimal.digits),
            "exponent": str(decimal.exponent),
        }
    if value_type is datetime:
        return {"type": "datetime", "value": _canonical_timestamp(value)}
    if value_type is date:
        return {"type": "date", "value": value.isoformat()}
    if value_type is time:
        return {
            "type": "time",
            "value": value.isoformat(timespec="microseconds"),
            "fold": value.fold,
        }
    if value_type is tuple:
        return {
            "type": "tuple",
            "items": [_canonical_digest_value(item) for item in value],
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "type": f"{value_type.__module__}.{value_type.__qualname__}",
            "fields": {
                field.name: _canonical_digest_value(
                    object.__getattribute__(value, field.name)
                )
                for field in fields(value_type)
            },
        }
    raise CanonicalMaterialError(
        "canonical digest material contains an unsupported value"
    )


def _canonical_sha256(domain: str, payload: object) -> str:
    document = {
        "domain": domain,
        "payload": payload,
    }
    try:
        encoded = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CanonicalMaterialError("canonical digest material is invalid") from error
    return hashlib.sha256(encoded).hexdigest()


def canonical_source_digest(source_receipts: tuple[object, ...]) -> str:
    """Derive one digest from the canonical persisted-receipt ordering."""
    receipts = _receipt_set(source_receipts)
    manifest = tuple(
        (
            {
                "row_id": receipt.row_id,
                "observation_sha256": _require_digest(
                    receipt.observation_sha256,
                    "source observation digest",
                ),
                "payload_sha256": _require_digest(
                    receipt.payload_sha256,
                    "source payload digest",
                ),
                "source_digest": _require_digest(
                    receipt.source_digest,
                    "source receipt digest",
                ),
            }
            for receipt in receipts
        ),
    )
    return _canonical_sha256(
        "stock-monitor/canonical-source-material/v1",
        manifest,
    )


def _publication_decision_digest(decision: object | None) -> str | None:
    if decision is None:
        return None
    from . import screening as screening_module

    if type(decision) is not screening_module.PublicationDecision:
        raise CanonicalMaterialError("canonical publication decision type is invalid")
    try:
        return screening_module._publication_decision_fingerprint(decision)
    except Exception as error:
        raise CanonicalMaterialError(
            "canonical publication decision digest is unavailable"
        ) from error


def _long_plan_digest(plan: object | None) -> str | None:
    if plan is None:
        return None
    from . import risk as risk_module

    if type(plan) is not risk_module.LongPlanDecision:
        raise CanonicalMaterialError("canonical primary plan type is invalid")
    try:
        fingerprint = risk_module._long_plan_fingerprint(plan)
        payload = _canonical_digest_value(fingerprint)
    except Exception as error:
        if isinstance(error, CanonicalMaterialError):
            raise
        raise CanonicalMaterialError(
            "canonical primary plan digest is unavailable"
        ) from error
    return _canonical_sha256(
        "stock-monitor/canonical-primary-plan/v1",
        payload,
    )


def _canonical_receipt_manifest(
    source_receipts: tuple[object, ...],
) -> tuple[tuple[int, str, str, str], ...]:
    receipts = _receipt_set(source_receipts)
    return tuple(
        (
            receipt.row_id,
            receipt.observation_sha256,
            receipt.payload_sha256,
            receipt.source_digest,
        )
        for receipt in receipts
    )


def _require_receipt_manifest(
    value: object,
) -> tuple[tuple[int, str, str, str], ...]:
    if type(value) is not tuple or not value:
        raise CanonicalMaterialError(
            "composition authority receipt manifest is invalid"
        )
    manifest: list[tuple[int, str, str, str]] = []
    for item in value:
        if (
            type(item) is not tuple
            or len(item) != 4
            or type(item[0]) is not int
            or item[0] < 1
        ):
            raise CanonicalMaterialError(
                "composition authority receipt manifest is invalid"
            )
        row_id, observation_sha256, payload_sha256, source_digest = item
        manifest.append(
            (
                row_id,
                _require_digest(
                    observation_sha256,
                    "composition receipt observation digest",
                ),
                _require_digest(
                    payload_sha256,
                    "composition receipt payload digest",
                ),
                _require_digest(
                    source_digest,
                    "composition receipt source digest",
                ),
            )
        )
    exact = tuple(manifest)
    if (
        len({item[0] for item in exact}) != len(exact)
        or len({item[1] for item in exact}) != len(exact)
        or exact != tuple(sorted(exact, key=lambda item: (item[1], item[0])))
    ):
        raise CanonicalMaterialError(
            "composition authority receipt manifest is not canonical"
        )
    return exact


def _snapshot_digest(snapshot: object) -> str:
    from .workflows import PremarketSnapshot

    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket composition requires an exact normalized snapshot"
        )
    return _canonical_sha256(
        "stock-monitor/canonical-premarket-snapshot/v1",
        _canonical_digest_value(snapshot),
    )


def _positions_digest(positions: object) -> str:
    from .reports import ClosePosition, UnverifiedClosePosition

    if type(positions) is not tuple or any(
        type(position) not in {ClosePosition, UnverifiedClosePosition}
        for position in positions
    ):
        raise CanonicalMaterialError(
            "close composition requires exact report projections"
        )
    return _canonical_sha256(
        "stock-monitor/canonical-close-positions/v1",
        _canonical_digest_value(positions),
    )


def _validation_window_id(value: object) -> str:
    if type(value) is not str or not value or "\x00" in value:
        raise CanonicalMaterialError(
            "premarket material requires a validation window identity"
        )
    return value


def _validate_reason_tuple(value: object, label: str) -> tuple[str, ...]:
    if (
        type(value) is not tuple
        or not value
        or any(
            type(reason) is not str
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", reason) is None
            for reason in value
        )
        or len(set(value)) != len(value)
    ):
        raise CanonicalMaterialError(f"{label} are invalid")
    return value


def _canonical_close_projection(
    actual_state: object,
    positions: tuple[object, ...],
) -> tuple[str, str, int, tuple[str, ...]]:
    """Derive the only close outcome, workflow exit, and ordered reasons."""
    from .reconciliation import ActualLedgerState
    from .reports import ClosePosition, UnverifiedClosePosition

    if type(actual_state) is not ActualLedgerState:
        raise CanonicalMaterialError(
            "close projection requires an exact actual state"
        )
    _positions_digest(positions)
    reconciliation_reasons = actual_state.reconciliation_reasons
    if type(reconciliation_reasons) is not tuple:
        raise CanonicalMaterialError(
            "close actual reconciliation reasons are invalid"
        )
    if reconciliation_reasons:
        _validate_reason_tuple(
            reconciliation_reasons,
            "close actual reconciliation reasons",
        )
        if _COORDINATOR_CLOSE_REASONS.intersection(reconciliation_reasons):
            raise CanonicalMaterialError(
                "close child reasons contain a coordinator-only reason"
            )

    present = set()
    ordered_reasons = list(reconciliation_reasons)
    if reconciliation_reasons:
        present.add("RECONCILIATION_REQUIRED")
    for position in positions:
        position_reasons = position.reason_codes
        if position_reasons:
            _validate_reason_tuple(
                position_reasons,
                "close position reasons",
            )
            if _COORDINATOR_CLOSE_REASONS.intersection(position_reasons):
                raise CanonicalMaterialError(
                    "close child reasons contain a coordinator-only reason"
                )
            ordered_reasons.extend(position_reasons)
        if type(position) is UnverifiedClosePosition:
            present.add(position.status)
            continue
        if type(position) is not ClosePosition:
            raise CanonicalMaterialError(
                "close projection contains an unsupported position"
            )
        if position.user_confirmed_stop is None:
            present.add("STOP_UNVERIFIED")
        present.add(position.action)

    dominant = "HOLD"
    for status, *_rest in _CLOSE_BRANCHES:
        if status in present:
            dominant = status
            break
    report_outcome, workflow_outcome, exit_code = (
        _CLOSE_BRANCH_PROJECTIONS[dominant]
    )
    if (
        not reconciliation_reasons
        and not actual_state.positions
        and not positions
    ):
        ordered_reasons.append("NO_ACTUAL_POSITIONS")
    if dominant in {"EXIT", "TIGHTEN_STOP", "HOLD"}:
        ordered_reasons.append("MANUAL_VERIFICATION_REQUIRED")
    reasons = tuple(dict.fromkeys(ordered_reasons))
    _validate_reason_tuple(reasons, "close canonical reasons")
    return report_outcome, workflow_outcome, exit_code, reasons


def _validate_premarket_outcome_reasons(
    outcome: object,
    reason_codes: object,
) -> tuple[str, ...]:
    reasons = _validate_reason_tuple(
        reason_codes,
        "premarket composition reasons",
    )
    if type(outcome) is not str or reasons not in _PREMARKET_OUTCOME_REASONS.get(
        outcome,
        frozenset(),
    ):
        raise CanonicalMaterialError(
            "premarket outcome and reasons are not an exact canonical branch"
        )
    return reasons


@dataclass(frozen=True, slots=True)
class _PremarketCompositionEnvelope:
    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    validation_window_id: str
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    snapshot_digest: str
    publication_decision_digest: str | None
    primary_plan_digest: str | None
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    source_binding_digest: str | None = None
    decision_basis: tuple[tuple[int, str, str], ...] = ()
    calendar_release_sha256: str | None = None
    universe_release_sha256: str | None = None
    evidence_release_sha256: str | None = None
    phase1_replay_digest: str | None = None


@dataclass(frozen=True, slots=True)
class _CompositionAuthorityCandidate:
    authority_reference: ReferenceType[object]
    authority_fingerprint: object
    envelope: object
    identity_children: tuple[object, ...]
    journal_reference: ReferenceType[object] | None = None
    journal_generation: int | None = None
    source_binding_candidate: object | None = None
    phase1_candidates: tuple[object, ...] = ()


_PREMARKET_COMPOSITION_LOCK = threading.Lock()
_CLOSE_COMPOSITION_LOCK = threading.Lock()
_ISSUED_PREMARKET_COMPOSITIONS: dict[int, _CompositionAuthorityCandidate] = {}
_ISSUED_CLOSE_COMPOSITIONS: dict[int, _CompositionAuthorityCandidate] = {}


def _premarket_composition_identity_children(
    *,
    snapshot: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
) -> tuple[object, ...]:
    receipts = _receipt_set(source_receipts)
    return (
        snapshot,
        *receipts,
        publication_decision,
        primary_plan,
    )


def _composition_digest(domain: str, values: dict[str, object]) -> str:
    return _canonical_sha256(domain, values)


def _premarket_composition_envelope(
    *,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    validation_window_id: str,
    source_receipts: tuple[object, ...],
    snapshot: object,
    publication_decision: object | None,
    primary_plan: object | None,
    outcome: str,
    reason_codes: tuple[str, ...],
    source_binding_digest: str | None = None,
    decision_basis: tuple[tuple[int, str, str], ...] = (),
    calendar_release_sha256: str | None = None,
    universe_release_sha256: str | None = None,
    evidence_release_sha256: str | None = None,
    phase1_replay_digest: str | None = None,
) -> _PremarketCompositionEnvelope:
    from .workflows import PremarketSnapshot

    session_date = _require_session(session_date)
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(session_date, decision_at, retrieved_at)
    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket composition requires an exact normalized snapshot"
        )
    receipts = _receipt_set(source_receipts)
    _validate_receipt_envelope(
        receipts,
        kind="PREMARKET",
        economic_at=decision_at,
        retrieved_at=retrieved_at,
        decision_basis=decision_basis,
    )
    has_candidates = bool(snapshot.candidates)
    if has_candidates != (
        publication_decision is not None and primary_plan is not None
    ):
        raise CanonicalMaterialError(
            "premarket candidate state requires its decision and primary plan"
        )
    reasons = _validate_premarket_outcome_reasons(outcome, reason_codes)
    if (
        has_candidates != (outcome == "CANDIDATES")
        or snapshot.breaker_active != (reasons == ("ACTIVE_BREAKER",))
    ):
        raise CanonicalMaterialError(
            "premarket composition outcome contradicts its snapshot"
        )
    window_id = _validation_window_id(validation_window_id)
    receipt_manifest = _canonical_receipt_manifest(receipts)
    source_digest = canonical_source_digest(receipts)
    snapshot_digest = _snapshot_digest(snapshot)
    decision_digest = _publication_decision_digest(publication_decision)
    plan_digest = _long_plan_digest(primary_plan)
    extended_values = (
        source_binding_digest,
        calendar_release_sha256,
        universe_release_sha256,
        evidence_release_sha256,
        phase1_replay_digest,
    )
    if any(value is not None for value in extended_values):
        if any(value is None for value in extended_values):
            raise CanonicalMaterialError(
                "premarket composition source authority is incomplete"
            )
        for value, label in zip(
            extended_values,
            (
                "premarket source binding digest",
                "premarket calendar release digest",
                "premarket universe release digest",
                "premarket evidence release digest",
                "premarket Phase 1 replay digest",
            ),
            strict=True,
        ):
            _require_digest(value, label)
        _require_premarket_decision_basis(decision_basis)
        if tuple(item[0] for item in decision_basis) != tuple(
            item[0] for item in receipt_manifest
        ):
            raise CanonicalMaterialError(
                "premarket composition decision basis conflicts with receipts"
            )
    elif decision_basis:
        raise CanonicalMaterialError(
            "premarket composition decision basis lacks source authority"
        )
    values: dict[str, object] = {
        "version": 1,
        "session_date": session_date.isoformat(),
        "decision_at": _canonical_timestamp(decision_at),
        "retrieved_at": _canonical_timestamp(retrieved_at),
        "validation_window_id": window_id,
        "receipt_manifest": receipt_manifest,
        "source_digest": source_digest,
        "snapshot_digest": snapshot_digest,
        "publication_decision_digest": decision_digest,
        "primary_plan_digest": plan_digest,
        "outcome": outcome,
        "reason_codes": reasons,
        "source_binding_digest": source_binding_digest,
        "decision_basis": decision_basis,
        "calendar_release_sha256": calendar_release_sha256,
        "universe_release_sha256": universe_release_sha256,
        "evidence_release_sha256": evidence_release_sha256,
        "phase1_replay_digest": phase1_replay_digest,
    }
    return _PremarketCompositionEnvelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=window_id,
        receipt_manifest=receipt_manifest,
        source_digest=source_digest,
        snapshot_digest=snapshot_digest,
        publication_decision_digest=decision_digest,
        primary_plan_digest=plan_digest,
        outcome=outcome,
        reason_codes=reasons,
        composition_digest=_composition_digest(
            "stock-monitor/canonical-premarket-composition/v1",
            values,
        ),
        source_binding_digest=source_binding_digest,
        decision_basis=decision_basis,
        calendar_release_sha256=calendar_release_sha256,
        universe_release_sha256=universe_release_sha256,
        evidence_release_sha256=evidence_release_sha256,
        phase1_replay_digest=phase1_replay_digest,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketCompositionAuthority:
    """Task 10 hook for a complete premarket composition capability.

    This value is intentionally not self-authenticating.  Only the public
    issuer registers one exact owner-bound capability; a syntactically valid
    caller-built copy never passes its issuance predicate.
    """

    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    validation_window_id: str
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    snapshot_digest: str
    publication_decision_digest: str | None
    primary_plan_digest: str | None
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    source_binding_digest: str | None = None
    decision_basis: tuple[tuple[int, str, str], ...] = ()
    calendar_release_sha256: str | None = None
    universe_release_sha256: str | None = None
    evidence_release_sha256: str | None = None
    phase1_replay_digest: str | None = None

    def __post_init__(self) -> None:
        session_date = _require_session(self.session_date)
        decision_at = _require_time(
            self.decision_at,
            "premarket composition decision time",
        )
        retrieved_at = _require_time(
            self.retrieved_at,
            "premarket composition retrieval time",
        )
        _validate_premarket_times(session_date, decision_at, retrieved_at)
        _validation_window_id(self.validation_window_id)
        _require_receipt_manifest(self.receipt_manifest)
        for value, label in (
            (self.source_digest, "premarket composition source digest"),
            (self.snapshot_digest, "premarket composition snapshot digest"),
            (self.composition_digest, "premarket composition digest"),
        ):
            _require_digest(value, label)
        decision_digest = self.publication_decision_digest
        plan_digest = self.primary_plan_digest
        if (decision_digest is None) != (plan_digest is None):
            raise CanonicalMaterialError(
                "premarket composition plan and decision digests must be paired"
            )
        if decision_digest is not None:
            _require_digest(
                decision_digest,
                "premarket composition publication-decision digest",
            )
            _require_digest(plan_digest, "premarket composition primary-plan digest")
        reasons = _validate_premarket_outcome_reasons(
            self.outcome,
            self.reason_codes,
        )
        if (self.outcome == "CANDIDATES") != (decision_digest is not None):
            raise CanonicalMaterialError(
                "premarket composition candidate authority is inconsistent"
            )
        if reasons == ("ACTIVE_BREAKER",) and self.outcome != "NO TRADE":
            raise CanonicalMaterialError(
                "premarket breaker composition is inconsistent"
            )
        extended_values = (
            self.source_binding_digest,
            self.calendar_release_sha256,
            self.universe_release_sha256,
            self.evidence_release_sha256,
            self.phase1_replay_digest,
        )
        if any(value is not None for value in extended_values):
            if any(value is None for value in extended_values):
                raise CanonicalMaterialError(
                    "premarket composition source authority is incomplete"
                )
            for value, label in zip(
                extended_values,
                (
                    "premarket source binding digest",
                    "premarket calendar release digest",
                    "premarket universe release digest",
                    "premarket evidence release digest",
                    "premarket Phase 1 replay digest",
                ),
                strict=True,
            ):
                _require_digest(value, label)
            _require_premarket_decision_basis(self.decision_basis)
        elif self.decision_basis:
            raise CanonicalMaterialError(
                "premarket composition decision basis lacks source authority"
            )


def _is_issued_premarket_composition_authority(
    authority: object,
    *,
    envelope: _PremarketCompositionEnvelope,
    identity_children: tuple[object, ...],
) -> bool:
    """Verify one exact registered Task 10 composition envelope."""
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or type(envelope) is not _PremarketCompositionEnvelope
        or type(identity_children) is not tuple
    ):
        return False
    try:
        authority_fingerprint = _value_fingerprint(authority)
        envelope_fingerprint = _value_fingerprint(envelope)
    except Exception:
        return False
    if (
        authority.session_date != envelope.session_date
        or authority.decision_at != envelope.decision_at
        or authority.retrieved_at != envelope.retrieved_at
        or authority.validation_window_id != envelope.validation_window_id
        or authority.receipt_manifest != envelope.receipt_manifest
        or authority.source_digest != envelope.source_digest
        or authority.snapshot_digest != envelope.snapshot_digest
        or authority.publication_decision_digest
        != envelope.publication_decision_digest
        or authority.primary_plan_digest != envelope.primary_plan_digest
        or authority.outcome != envelope.outcome
        or authority.reason_codes != envelope.reason_codes
        or authority.composition_digest != envelope.composition_digest
        or authority.source_binding_digest != envelope.source_binding_digest
        or authority.decision_basis != envelope.decision_basis
        or authority.calendar_release_sha256
        != envelope.calendar_release_sha256
        or authority.universe_release_sha256
        != envelope.universe_release_sha256
        or authority.evidence_release_sha256
        != envelope.evidence_release_sha256
        or authority.phase1_replay_digest != envelope.phase1_replay_digest
    ):
        return False
    with _PREMARKET_COMPOSITION_LOCK:
        candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
        if (
            type(candidate) is not _CompositionAuthorityCandidate
            or type(candidate.authority_reference) is not ReferenceType
            or candidate.authority_reference() is not authority
            or type(candidate.envelope) is not _PremarketCompositionEnvelope
            or type(candidate.identity_children) is not tuple
            or (
                authority.source_binding_digest is None
                and len(candidate.identity_children) != len(identity_children)
            )
            or len(candidate.identity_children) < len(identity_children)
            or any(
                current is not expected
                for current, expected in zip(
                    identity_children,
                    candidate.identity_children,
                    strict=False,
                )
            )
        ):
            return False
        try:
            candidate_envelope_fingerprint = _value_fingerprint(
                candidate.envelope
            )
        except Exception:
            return False
        extended_current = True
        if authority.source_binding_digest is not None:
            journal = (
                None
                if candidate.journal_reference is None
                else candidate.journal_reference()
            )
            source_candidate = candidate.source_binding_candidate
            source_authority_index = len(identity_children)
            source_authority = (
                None
                if source_authority_index >= len(candidate.identity_children)
                else candidate.identity_children[source_authority_index]
            )
            from . import journal as journal_module

            extended_current = bool(
                journal is not None
                and not getattr(journal, "_closed", True)
                and getattr(journal, "_source_generation", None)
                == candidate.journal_generation
                and type(source_candidate) is _PremarketSourceBindingCandidate
                and _is_current_premarket_source_binding_without_callbacks(
                    source_authority,
                    source_candidate,
                )
                and all(
                    journal_module._is_current_journal_authority_candidate_without_callbacks(
                        phase1_candidate
                    )
                    for phase1_candidate in candidate.phase1_candidates
                )
            )
        return bool(
            candidate.authority_fingerprint == authority_fingerprint
            and candidate_envelope_fingerprint == envelope_fingerprint
            and extended_current
            and _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority)) is candidate
        )


def _premarket_composition_extended_values(
    authority: object,
) -> dict[str, object]:
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or authority.source_binding_digest is None
    ):
        return {}
    return {
        "source_binding_digest": authority.source_binding_digest,
        "decision_basis": authority.decision_basis,
        "calendar_release_sha256": authority.calendar_release_sha256,
        "universe_release_sha256": authority.universe_release_sha256,
        "evidence_release_sha256": authority.evidence_release_sha256,
        "phase1_replay_digest": authority.phase1_replay_digest,
    }


def issue_canonical_premarket_composition_authority(
    *,
    journal: object,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    validation_window_id: str,
    source_binding_authority: object,
    snapshot: object,
    publication_decision: object | None,
    primary_plan: object | None,
    outcome: str,
    reason_codes: tuple[str, ...],
    calendar: object,
    universe: object,
    evidence_release: object,
    phase1_replay_children: tuple[object, ...],
) -> CanonicalPremarketCompositionAuthority:
    """Seal reviewed releases, receipt bindings, and replay children together."""
    from . import evidence as evidence_module
    from . import journal as journal_module
    from . import market_calendar as calendar_module
    from . import universe as universe_module
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError(
            "premarket composition requires an open Journal owner"
        )
    session_date = _require_session(session_date)
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(session_date, decision_at, retrieved_at)
    if (
        type(phase1_replay_children) is not tuple
        or len({id(child) for child in phase1_replay_children})
        != len(phase1_replay_children)
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children are invalid"
        )

    with _PREMARKET_SOURCE_BINDING_LOCK:
        source_candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(
            id(source_binding_authority)
        )
    if (
        type(source_binding_authority)
        is not CanonicalPremarketSourceBindingAuthority
        or type(source_candidate) is not _PremarketSourceBindingCandidate
        or source_candidate.journal_reference() is not journal
        or source_binding_authority.decision_at != decision_at
        or source_binding_authority.retrieved_at != retrieved_at
        or not _is_current_premarket_source_binding_without_callbacks(
            source_binding_authority,
            source_candidate,
        )
    ):
        raise CanonicalMaterialError(
            "premarket source binding authority has the wrong owner or is not current"
        )

    # Exhaust provider predicates and reviewed-release validators before the
    # final Journal generation/candidate capture.
    _premarket_binding_manifest(
        source_candidate.bindings,
        invoke_provider_predicate=True,
    )
    calendar_identity = _reviewed_binding_identity(calendar)
    universe_identity = _reviewed_binding_identity(universe)
    evidence_identity = _reviewed_binding_identity(evidence_release)
    if (
        type(calendar) is not calendar_module.MarketCalendar
        or type(universe) is not universe_module.UniverseSnapshot
        or type(evidence_release) is not evidence_module.ReviewedEvidenceRelease
        or calendar_identity is None
        or universe_identity is None
        or evidence_identity is None
        or calendar.year != session_date.year
        or evidence_release.universe_sha256 != universe_identity[1]
    ):
        raise CanonicalMaterialError(
            "premarket reviewed calendar, universe, or evidence release is inconsistent"
        )
    bound_sources = tuple(binding.source for binding in source_candidate.bindings)
    required_reviewed_children = (
        calendar,
        universe,
        evidence_release,
        *tuple(evidence_release.by_symbol.values()),
    )
    if any(
        sum(source is child for source in bound_sources) != 1
        for child in required_reviewed_children
    ):
        raise CanonicalMaterialError(
            "premarket reviewed release binding is incomplete"
        )

    expected_phase1_children = tuple(
        bundle._phase1_source
        for bundle in evidence_release.by_symbol.values()
        if bundle._phase1_source is not None
    )
    if (
        len(phase1_replay_children) != len(expected_phase1_children)
        or any(
            supplied is not expected
            for supplied, expected in zip(
                phase1_replay_children,
                expected_phase1_children,
                strict=True,
            )
        )
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children conflict with reviewed evidence"
        )

    phase1_candidates = tuple(
        journal_module._journal_any_source_authority_candidate(child)
        for child in phase1_replay_children
    )
    if any(candidate is None for candidate in phase1_candidates):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay authority is unavailable"
        )
    exact_phase1_candidates = tuple(
        candidate for candidate in phase1_candidates if candidate is not None
    )
    if exact_phase1_candidates and (
        journal_module._current_journal_source_authority_owner(
            exact_phase1_candidates
        )
        is not journal
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children cross Journal owners"
        )

    # Reacquire the source candidate after every authority callback/check.
    with _PREMARKET_SOURCE_BINDING_LOCK:
        refreshed_source_candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(
            id(source_binding_authority)
        )
    if (
        refreshed_source_candidate is not source_candidate
        or not _is_current_premarket_source_binding_without_callbacks(
            source_binding_authority,
            source_candidate,
        )
    ):
        raise CanonicalMaterialError(
            "premarket source binding changed during composition"
        )

    receipts = tuple(binding.receipt for binding in source_candidate.bindings)
    phase1_digest = _canonical_sha256(
        "stock-monitor/premarket-phase1-replay/v1",
        tuple(_value_fingerprint(child) for child in phase1_replay_children),
    )
    envelope = _premarket_composition_envelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=validation_window_id,
        source_receipts=receipts,
        snapshot=snapshot,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        outcome=outcome,
        reason_codes=reason_codes,
        source_binding_digest=source_binding_authority.binding_digest,
        decision_basis=source_binding_authority.decision_basis,
        calendar_release_sha256=calendar_identity[1],
        universe_release_sha256=universe_identity[1],
        evidence_release_sha256=evidence_identity[1],
        phase1_replay_digest=phase1_digest,
    )
    authority = CanonicalPremarketCompositionAuthority(
        session_date=envelope.session_date,
        decision_at=envelope.decision_at,
        retrieved_at=envelope.retrieved_at,
        validation_window_id=envelope.validation_window_id,
        receipt_manifest=envelope.receipt_manifest,
        source_digest=envelope.source_digest,
        snapshot_digest=envelope.snapshot_digest,
        publication_decision_digest=envelope.publication_decision_digest,
        primary_plan_digest=envelope.primary_plan_digest,
        outcome=envelope.outcome,
        reason_codes=envelope.reason_codes,
        composition_digest=envelope.composition_digest,
        source_binding_digest=envelope.source_binding_digest,
        decision_basis=envelope.decision_basis,
        calendar_release_sha256=envelope.calendar_release_sha256,
        universe_release_sha256=envelope.universe_release_sha256,
        evidence_release_sha256=envelope.evidence_release_sha256,
        phase1_replay_digest=envelope.phase1_replay_digest,
    )
    base_children = _premarket_composition_identity_children(
        snapshot=snapshot,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
    )
    identity_children = (
        *base_children,
        source_binding_authority,
        calendar,
        universe,
        evidence_release,
        *tuple(evidence_release.by_symbol.values()),
        *phase1_replay_children,
    )
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("premarket Journal generation is invalid")
    identity = id(authority)

    def discard(dead: ReferenceType[object]) -> None:
        with _PREMARKET_COMPOSITION_LOCK:
            current = _ISSUED_PREMARKET_COMPOSITIONS.get(identity)
            if current is not None and current.authority_reference is dead:
                _ISSUED_PREMARKET_COMPOSITIONS.pop(identity, None)

    candidate = _CompositionAuthorityCandidate(
        authority_reference=ref(authority, discard),
        authority_fingerprint=_value_fingerprint(authority),
        envelope=envelope,
        identity_children=identity_children,
        journal_reference=ref(journal),
        journal_generation=generation,
        source_binding_candidate=source_candidate,
        phase1_candidates=exact_phase1_candidates,
    )
    with _PREMARKET_COMPOSITION_LOCK:
        _ISSUED_PREMARKET_COMPOSITIONS[identity] = candidate
    if not _is_issued_premarket_composition_authority(
        authority,
        envelope=envelope,
        identity_children=base_children,
    ):
        with _PREMARKET_COMPOSITION_LOCK:
            if _ISSUED_PREMARKET_COMPOSITIONS.get(identity) is candidate:
                _ISSUED_PREMARKET_COMPOSITIONS.pop(identity, None)
        raise CanonicalMaterialError(
            "premarket composition changed during issuance"
        )
    return authority


def is_issued_canonical_premarket_composition_authority(
    authority: object,
) -> bool:
    """Return whether one exact Task 10 composition capability is current."""
    with _PREMARKET_COMPOSITION_LOCK:
        candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or type(candidate) is not _CompositionAuthorityCandidate
        or type(candidate.envelope) is not _PremarketCompositionEnvelope
    ):
        return False
    base_length = 1 + len(candidate.envelope.receipt_manifest) + 2
    if authority.source_binding_digest is None:
        return _is_issued_premarket_composition_authority(
            authority,
            envelope=candidate.envelope,
            identity_children=candidate.identity_children,
        )
    source_authority = candidate.identity_children[base_length]
    journal = (
        None
        if candidate.journal_reference is None
        else candidate.journal_reference()
    )
    if journal is None or not (
        is_issued_canonical_premarket_source_binding_authority(
            source_authority,
            journal=journal,
        )
    ):
        return False
    with _PREMARKET_COMPOSITION_LOCK:
        refreshed = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
    if refreshed is not candidate:
        return False
    return _is_issued_premarket_composition_authority(
        authority,
        envelope=candidate.envelope,
        identity_children=candidate.identity_children[:base_length],
    )


def canonical_premarket_state_hash(
    *,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    snapshot: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
    validation_window_id: str,
    outcome: str,
    reason_codes: tuple[str, ...],
    composition_authority: object | None = None,
) -> str:
    """Derive the report state hash from every premarket semantic input."""
    from .workflows import PremarketSnapshot

    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket state hash requires an exact normalized snapshot"
        )
    receipts = _receipt_set(source_receipts)
    extended = _premarket_composition_extended_values(composition_authority)
    envelope = _premarket_composition_envelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=validation_window_id,
        source_receipts=receipts,
        snapshot=snapshot,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        outcome=outcome,
        reason_codes=reason_codes,
        **extended,
    )
    if not _is_issued_premarket_composition_authority(
        composition_authority,
        envelope=envelope,
        identity_children=_premarket_composition_identity_children(
            snapshot=snapshot,
            source_receipts=receipts,
            publication_decision=publication_decision,
            primary_plan=primary_plan,
        ),
    ):
        raise CanonicalMaterialError(
            "premarket composition authority is unavailable"
        )
    payload = {
        "version": 1,
        "session_date": envelope.session_date.isoformat(),
        "decision_at": _canonical_timestamp(envelope.decision_at),
        "retrieved_at": _canonical_timestamp(envelope.retrieved_at),
        "validation_window_id": envelope.validation_window_id,
        "receipt_manifest": envelope.receipt_manifest,
        "source_digest": envelope.source_digest,
        "snapshot_digest": envelope.snapshot_digest,
        "publication_decision_digest": envelope.publication_decision_digest,
        "primary_plan_digest": envelope.primary_plan_digest,
        "outcome": envelope.outcome,
        "reason_codes": envelope.reason_codes,
        "composition_digest": envelope.composition_digest,
        "source_binding_digest": envelope.source_binding_digest,
        "decision_basis": envelope.decision_basis,
        "calendar_release_sha256": envelope.calendar_release_sha256,
        "universe_release_sha256": envelope.universe_release_sha256,
        "evidence_release_sha256": envelope.evidence_release_sha256,
        "phase1_replay_digest": envelope.phase1_replay_digest,
    }
    return _canonical_sha256(
        "stock-monitor/canonical-premarket-state/v1",
        payload,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalCloseCompositionAuthority:
    """Task 11 hook for exact close decision composition in every branch.

    The foundation intentionally exposes no public issuer.  Until Task 11 can
    bind every projected position (including an empty projection) to its exact
    market/context/action sources, canonical close material remains unavailable.
    """

    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    actual_state_digest: str
    actual_replay_source_digest: str
    positions_digest: str
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str

    def __post_init__(self) -> None:
        session_date = _require_session(self.session_date)
        review_at = _require_time(self.review_at, "close composition review time")
        retrieved_at = _require_time(
            self.retrieved_at,
            "close composition retrieval time",
        )
        query_cutoff = _require_time(
            self.query_cutoff,
            "close composition query cutoff",
        )
        _validate_close_times(
            session_date,
            review_at,
            query_cutoff,
            retrieved_at,
        )
        _require_receipt_manifest(self.receipt_manifest)
        for value, label in (
            (self.source_digest, "close composition source digest"),
            (self.actual_state_digest, "close composition actual-state digest"),
            (
                self.actual_replay_source_digest,
                "close composition replay-source digest",
            ),
            (self.positions_digest, "close composition positions digest"),
            (self.composition_digest, "close composition digest"),
        ):
            _require_digest(value, label)
        if type(self.outcome) is not str or self.outcome not in (
            _CLOSE_OUTCOME_EXIT_CODES
        ):
            raise CanonicalMaterialError("close composition outcome is invalid")
        _validate_reason_tuple(self.reason_codes, "close composition reasons")


def _is_issued_close_composition_authority(
    authority: object,
    *,
    envelope: object,
    identity_children: tuple[object, ...],
) -> bool:
    """Verify a complete Task 11 envelope; the registry is empty for now."""
    if (
        type(authority) is not CanonicalCloseCompositionAuthority
        or type(envelope) is not _CloseCompositionEnvelope
        or type(identity_children) is not tuple
    ):
        return False
    try:
        authority_fingerprint = _value_fingerprint(authority)
        envelope_fingerprint = _value_fingerprint(envelope)
    except Exception:
        return False
    if (
        authority.session_date != envelope.session_date
        or authority.review_at != envelope.review_at
        or authority.retrieved_at != envelope.retrieved_at
        or authority.query_cutoff != envelope.query_cutoff
        or authority.receipt_manifest != envelope.receipt_manifest
        or authority.source_digest != envelope.source_digest
        or authority.actual_state_digest != envelope.actual_state_digest
        or authority.actual_replay_source_digest
        != envelope.actual_replay_source_digest
        or authority.positions_digest != envelope.positions_digest
        or authority.outcome != envelope.outcome
        or authority.reason_codes != envelope.reason_codes
        or authority.composition_digest != envelope.composition_digest
    ):
        return False
    with _CLOSE_COMPOSITION_LOCK:
        candidate = _ISSUED_CLOSE_COMPOSITIONS.get(id(authority))
        if (
            type(candidate) is not _CompositionAuthorityCandidate
            or type(candidate.authority_reference) is not ReferenceType
            or candidate.authority_reference() is not authority
            or type(candidate.envelope) is not _CloseCompositionEnvelope
            or type(candidate.identity_children) is not tuple
            or len(candidate.identity_children) != len(identity_children)
            or any(
                current is not expected
                for current, expected in zip(
                    identity_children,
                    candidate.identity_children,
                    strict=True,
                )
            )
        ):
            return False
        try:
            candidate_envelope_fingerprint = _value_fingerprint(
                candidate.envelope
            )
        except Exception:
            return False
        return bool(
            candidate.authority_fingerprint == authority_fingerprint
            and candidate_envelope_fingerprint == envelope_fingerprint
            and _ISSUED_CLOSE_COMPOSITIONS.get(id(authority)) is candidate
        )


def _actual_state_digest(state: object) -> str:
    from . import reconciliation as reconciliation_module

    if type(state) is not reconciliation_module.ActualLedgerState:
        raise CanonicalMaterialError("close state hash requires exact actual state")
    try:
        digest = reconciliation_module._actual_state_digest(state)
    except Exception as error:
        raise CanonicalMaterialError(
            "close actual-state digest is unavailable"
        ) from error
    if digest != state.source_digest:
        raise CanonicalMaterialError("close actual-state digest is inconsistent")
    return digest


@dataclass(frozen=True, slots=True)
class _CloseCompositionEnvelope:
    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    actual_state_digest: str
    actual_replay_source_digest: str
    positions_digest: str
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str


def _close_composition_identity_children(
    *,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
) -> tuple[object, ...]:
    receipts = _receipt_set(source_receipts)
    return (
        actual_state,
        actual_replay_source,
        *positions,
        *receipts,
    )


def _close_composition_envelope(
    *,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    outcome: str,
    reason_codes: tuple[str, ...],
) -> _CloseCompositionEnvelope:
    from .journal import JournalActualReplaySource
    from .reconciliation import ActualLedgerState

    session_date = _require_session(session_date)
    review_at = _require_time(review_at, "close review time")
    retrieved_at = _require_time(retrieved_at, "close retrieval time")
    query_cutoff = _require_time(query_cutoff, "close query cutoff")
    _validate_close_times(
        session_date,
        review_at,
        query_cutoff,
        retrieved_at,
    )
    if (
        type(actual_state) is not ActualLedgerState
        or type(actual_replay_source) is not JournalActualReplaySource
        or actual_state.query_cutoff != query_cutoff
        or actual_replay_source.query_cutoff != query_cutoff
    ):
        raise CanonicalMaterialError(
            "close composition requires exact replay state and source"
        )
    receipts = _receipt_set(source_receipts)
    _validate_receipt_envelope(
        receipts,
        kind="CLOSE",
        economic_at=review_at,
        query_cutoff=query_cutoff,
        retrieved_at=retrieved_at,
    )
    position_digest = _positions_digest(positions)
    reasons = _validate_reason_tuple(reason_codes, "close composition reasons")
    if type(outcome) is not str or outcome not in _CLOSE_OUTCOME_EXIT_CODES:
        raise CanonicalMaterialError("close composition outcome is invalid")
    expected_outcome, _workflow_outcome, _exit_code, expected_reasons = (
        _canonical_close_projection(actual_state, positions)
    )
    if outcome != expected_outcome or reasons != expected_reasons:
        raise CanonicalMaterialError(
            "close outcome and reasons contradict the canonical matrix"
        )
    receipt_manifest = _canonical_receipt_manifest(receipts)
    source_digest = canonical_source_digest(receipts)
    state_digest = _actual_state_digest(actual_state)
    replay_digest = _require_digest(
        actual_replay_source.source_digest,
        "close replay-source digest",
    )
    values: dict[str, object] = {
        "version": 1,
        "session_date": session_date.isoformat(),
        "review_at": _canonical_timestamp(review_at),
        "retrieved_at": _canonical_timestamp(retrieved_at),
        "query_cutoff": _canonical_timestamp(query_cutoff),
        "receipt_manifest": receipt_manifest,
        "source_digest": source_digest,
        "actual_state_digest": state_digest,
        "actual_replay_source_digest": replay_digest,
        "positions_digest": position_digest,
        "outcome": outcome,
        "reason_codes": reasons,
    }
    return _CloseCompositionEnvelope(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        receipt_manifest=receipt_manifest,
        source_digest=source_digest,
        actual_state_digest=state_digest,
        actual_replay_source_digest=replay_digest,
        positions_digest=position_digest,
        outcome=outcome,
        reason_codes=reasons,
        composition_digest=_composition_digest(
            "stock-monitor/canonical-close-composition/v1",
            values,
        ),
    )


def canonical_close_state_hash(
    *,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    outcome: str,
    reason_codes: tuple[str, ...],
    composition_authority: object | None = None,
) -> str:
    """Derive the report state hash from every actual-close semantic input."""
    receipts = _receipt_set(source_receipts)
    envelope = _close_composition_envelope(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
        outcome=outcome,
        reason_codes=reason_codes,
    )
    if not _is_issued_close_composition_authority(
        composition_authority,
        envelope=envelope,
        identity_children=_close_composition_identity_children(
            actual_state=actual_state,
            actual_replay_source=actual_replay_source,
            positions=positions,
            source_receipts=receipts,
        ),
    ):
        raise CanonicalMaterialError("close composition authority is unavailable")
    payload = {
        "version": 1,
        "session_date": envelope.session_date.isoformat(),
        "review_at": _canonical_timestamp(envelope.review_at),
        "retrieved_at": _canonical_timestamp(envelope.retrieved_at),
        "query_cutoff": _canonical_timestamp(envelope.query_cutoff),
        "receipt_manifest": envelope.receipt_manifest,
        "source_digest": envelope.source_digest,
        "actual_state_digest": envelope.actual_state_digest,
        "actual_replay_source_digest": envelope.actual_replay_source_digest,
        "positions_digest": envelope.positions_digest,
        "outcome": envelope.outcome,
        "reason_codes": envelope.reason_codes,
        "composition_digest": envelope.composition_digest,
    }
    return _canonical_sha256(
        "stock-monitor/canonical-close-state/v1",
        payload,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketMaterial:
    """Immutable non-fixture premarket report material."""

    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    snapshot: object
    report: object
    source_receipts: tuple[object, ...]
    publication_decision: object | None
    primary_plan: object | None
    validation_window_id: str
    state_hash: str
    source_digest: str
    material_digest: str
    composition_authority: object | None = None

    def __post_init__(self) -> None:
        from .workflows import PremarketSnapshot

        session_date = _require_session(self.session_date)
        decision_at = _require_time(self.decision_at, "premarket decision time")
        retrieved_at = _require_time(self.retrieved_at, "premarket retrieval time")
        _validate_premarket_times(session_date, decision_at, retrieved_at)
        if type(self.snapshot) is not PremarketSnapshot:
            raise CanonicalMaterialError(
                "premarket material requires an exact normalized snapshot"
            )
        if type(self.composition_authority) is not (
            CanonicalPremarketCompositionAuthority
        ):
            raise CanonicalMaterialError(
                "premarket composition authority is unavailable"
            )
        receipts = _require_canonical_receipt_order(self.source_receipts)
        _validate_receipt_envelope(
            receipts,
            kind="PREMARKET",
            economic_at=decision_at,
            retrieved_at=retrieved_at,
            decision_basis=self.composition_authority.decision_basis,
        )
        state_hash = _require_digest(self.state_hash, "premarket state hash")
        _require_digest(self.source_digest, "premarket source digest")
        _require_digest(self.material_digest, "premarket material digest")
        _validation_window_id(self.validation_window_id)
        has_primary = bool(self.snapshot.candidates)
        if has_primary != (
            self.publication_decision is not None and self.primary_plan is not None
        ):
            raise CanonicalMaterialError(
                "premarket candidate material requires its decision and primary plan"
            )
        _validate_report(
            self.report,
            kind="PREMARKET",
            session_date=session_date,
            state_hash=state_hash,
            receipts=receipts,
        )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalCloseMaterial:
    """Immutable non-fixture actual-close report material."""

    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    actual_state: object
    actual_replay_source: object
    report: object
    positions: tuple[object, ...]
    source_receipts: tuple[object, ...]
    state_hash: str
    source_digest: str
    material_digest: str
    composition_authority: object | None = None

    def __post_init__(self) -> None:
        from .journal import JournalActualReplaySource
        from .reconciliation import ActualLedgerState
        from .reports import ClosePosition, UnverifiedClosePosition

        session_date = _require_session(self.session_date)
        review_at = _require_time(self.review_at, "close review time")
        retrieved_at = _require_time(self.retrieved_at, "close retrieval time")
        query_cutoff = _require_time(self.query_cutoff, "close query cutoff")
        _validate_close_times(
            session_date,
            review_at,
            query_cutoff,
            retrieved_at,
        )
        if (
            type(self.actual_state) is not ActualLedgerState
            or type(self.actual_replay_source) is not JournalActualReplaySource
            or self.actual_state.query_cutoff != query_cutoff
            or self.actual_replay_source.query_cutoff != query_cutoff
        ):
            raise CanonicalMaterialError(
                "close material requires exact actual replay state and source"
            )
        if type(self.positions) is not tuple or any(
            type(position) not in {ClosePosition, UnverifiedClosePosition}
            for position in self.positions
        ):
            raise CanonicalMaterialError(
                "close positions must contain exact report projections"
            )
        if type(self.composition_authority) is not (
            CanonicalCloseCompositionAuthority
        ):
            raise CanonicalMaterialError(
                "close composition authority is unavailable"
            )
        receipts = _require_canonical_receipt_order(self.source_receipts)
        _validate_receipt_envelope(
            receipts,
            kind="CLOSE",
            economic_at=review_at,
            query_cutoff=query_cutoff,
            retrieved_at=retrieved_at,
        )
        state_hash = _require_digest(self.state_hash, "close state hash")
        _require_digest(self.source_digest, "close source digest")
        _require_digest(self.material_digest, "close material digest")
        _validate_report(
            self.report,
            kind="CLOSE",
            session_date=session_date,
            state_hash=state_hash,
            receipts=receipts,
        )
        _validate_close_report_projection(
            actual_state=self.actual_state,
            positions=self.positions,
            report=self.report,
            retrieved_at=retrieved_at,
            composition_authority=self.composition_authority,
        )


CanonicalMaterial = CanonicalPremarketMaterial | CanonicalCloseMaterial


def _report_reason_codes(report: object) -> tuple[str, ...]:
    from .reports import Report

    if type(report) is not Report:
        raise CanonicalMaterialError("canonical report type is invalid")
    lines = report.body.splitlines()
    try:
        start = lines.index("## Reasons") + 1
        end = next(
            index
            for index in range(start, len(lines))
            if lines[index].startswith("## ")
        )
    except (ValueError, StopIteration) as error:
        raise CanonicalMaterialError(
            "canonical report reason block is malformed"
        ) from error
    reasons: list[str] = []
    for line in lines[start:end]:
        if not line:
            continue
        match = _CANONICAL_REASON.fullmatch(line)
        if match is None:
            raise CanonicalMaterialError(
                "canonical report reason block is malformed"
            )
        reasons.append(match.group(1))
    if not reasons or len(reasons) != len(set(reasons)):
        raise CanonicalMaterialError("canonical report reasons are incomplete")
    return tuple(reasons)


def _composition_envelope_for_material(
    material: CanonicalMaterial,
) -> _PremarketCompositionEnvelope | _CloseCompositionEnvelope:
    """Rebuild the complete semantic envelope before authority issuance."""
    reasons = _report_reason_codes(material.report)
    if type(material) is CanonicalPremarketMaterial:
        return _premarket_composition_envelope(
            session_date=material.session_date,
            decision_at=material.decision_at,
            retrieved_at=material.retrieved_at,
            validation_window_id=material.validation_window_id,
            source_receipts=material.source_receipts,
            snapshot=material.snapshot,
            publication_decision=material.publication_decision,
            primary_plan=material.primary_plan,
            outcome=material.report.outcome,
            reason_codes=reasons,
            **_premarket_composition_extended_values(
                material.composition_authority
            ),
        )
    if type(material) is CanonicalCloseMaterial:
        return _close_composition_envelope(
            session_date=material.session_date,
            review_at=material.review_at,
            retrieved_at=material.retrieved_at,
            query_cutoff=material.query_cutoff,
            actual_state=material.actual_state,
            actual_replay_source=material.actual_replay_source,
            positions=material.positions,
            source_receipts=material.source_receipts,
            outcome=material.report.outcome,
            reason_codes=reasons,
        )
    raise CanonicalMaterialError("canonical composition material type is invalid")


def _composition_identity_children_without_callbacks(
    material: CanonicalMaterial,
) -> tuple[object, ...]:
    """Return already-validated exact children without invoking source code."""
    if type(material) is CanonicalPremarketMaterial:
        return (
            material.snapshot,
            *material.source_receipts,
            material.publication_decision,
            material.primary_plan,
        )
    return (
        material.actual_state,
        material.actual_replay_source,
        *material.positions,
        *material.source_receipts,
    )


def _composition_is_current_without_callbacks(
    material: CanonicalMaterial,
    envelope: object,
) -> bool:
    children = _composition_identity_children_without_callbacks(material)
    if type(material) is CanonicalPremarketMaterial:
        return bool(
            type(envelope) is _PremarketCompositionEnvelope
            and _is_issued_premarket_composition_authority(
                material.composition_authority,
                envelope=envelope,
                identity_children=children,
            )
        )
    return bool(
        type(material) is CanonicalCloseMaterial
        and type(envelope) is _CloseCompositionEnvelope
        and _is_issued_close_composition_authority(
            material.composition_authority,
            envelope=envelope,
            identity_children=children,
        )
    )


def _report_projection_lines(report: object, header: str) -> tuple[str, ...]:
    lines = report.body.splitlines()
    try:
        index = lines.index(header)
    except ValueError as error:
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        ) from error
    if index + 1 >= len(lines) or lines[index + 1] != "":
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        )
    projection = tuple(lines[index + 2 :])
    if any(line.startswith("## ") for line in projection):
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        )
    return projection


def _expected_projection_lines(
    values: tuple[object, ...],
    renderer: object,
) -> tuple[str, ...]:
    if not values:
        return ("None.",)
    lines: list[str] = []
    for ordinal, value in enumerate(values, start=1):
        if ordinal > 1:
            lines.append("")
        lines.extend(renderer(value, ordinal))
    return tuple(lines)


def _validate_generated_at(report: object, retrieved_at: datetime) -> None:
    expected = f"- Generated at: `{retrieved_at.isoformat(timespec='seconds')}`"
    if report.body.splitlines().count(expected) != 1:
        raise CanonicalMaterialError(
            "canonical report generation time conflicts with retrieval"
        )


def _validate_premarket_report_projection(
    *,
    snapshot: object,
    report: object,
    retrieved_at: datetime,
    composition_authority: object,
) -> None:
    from . import reports as reports_module

    materials: list[object] = []
    for candidate in snapshot.candidates:
        if candidate.material is None:
            raise CanonicalMaterialError(
                "canonical candidate lacks an exact report projection"
            )
        materials.append(candidate.material)
    expected = _expected_projection_lines(
        tuple(materials),
        reports_module._render_candidate,
    )
    if _report_projection_lines(report, "## Candidates") != expected:
        raise CanonicalMaterialError(
            "canonical premarket report projection contradicts material"
        )
    _validate_generated_at(report, retrieved_at)
    reasons = _report_reason_codes(report)
    if reasons not in _PREMARKET_OUTCOME_REASONS.get(
        report.outcome,
        frozenset(),
    ):
        raise CanonicalMaterialError(
            "canonical premarket report is not an exact outcome/reason branch"
        )
    if (
        type(composition_authority) is not CanonicalPremarketCompositionAuthority
        or composition_authority.outcome != report.outcome
        or composition_authority.reason_codes != reasons
    ):
        raise CanonicalMaterialError(
            "canonical premarket report conflicts with composition authority"
        )
    if snapshot.breaker_active != ("ACTIVE_BREAKER" in reasons):
        raise CanonicalMaterialError(
            "canonical premarket report contradicts its snapshot"
        )
    if snapshot.candidates:
        if (
            snapshot.breaker_active
            or report.outcome != "CANDIDATES"
            or reasons
            != ("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED")
        ):
            raise CanonicalMaterialError(
                "canonical premarket report contradicts its snapshot"
            )
        return
    if report.outcome not in {"NO TRADE", "NO NEW TRADE - DATA UNAVAILABLE"}:
        raise CanonicalMaterialError(
            "canonical premarket report contradicts its snapshot"
        )


def _validate_close_report_projection(
    *,
    actual_state: object,
    positions: tuple[object, ...],
    report: object,
    retrieved_at: datetime,
    composition_authority: object,
) -> None:
    from . import reports as reports_module

    expected = _expected_projection_lines(
        positions,
        reports_module._render_close_position,
    )
    if _report_projection_lines(report, "## Positions") != expected:
        raise CanonicalMaterialError(
            "canonical close report projection contradicts material"
        )
    _validate_generated_at(report, retrieved_at)
    reasons = _report_reason_codes(report)
    if (
        type(composition_authority) is not CanonicalCloseCompositionAuthority
        or composition_authority.outcome != report.outcome
        or composition_authority.reason_codes != reasons
        or report.outcome not in _CLOSE_OUTCOME_EXIT_CODES
    ):
        raise CanonicalMaterialError(
            "canonical close report conflicts with composition authority"
        )
    expected_outcome, _workflow_outcome, _exit_code, expected_reasons = (
        _canonical_close_projection(actual_state, positions)
    )
    if report.outcome != expected_outcome or reasons != expected_reasons:
        raise CanonicalMaterialError(
            "canonical close report contradicts the exact close matrix"
        )


def canonical_material_digest(material: object) -> str:
    """Derive the complete transport digest, excluding its own digest field."""
    from .reports import Report

    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        raise CanonicalMaterialError("canonical material digest type is invalid")
    if type(material.report) is not Report:
        raise CanonicalMaterialError("canonical material report type is invalid")
    source_digest = canonical_source_digest(material.source_receipts)
    if source_digest != material.source_digest:
        raise CanonicalMaterialError(
            "canonical material source digest is inconsistent"
        )
    common: dict[str, object] = {
        "version": 1,
        "kind": material.report.kind,
        "session_date": material.session_date.isoformat(),
        "retrieved_at": _canonical_timestamp(material.retrieved_at),
        "state_hash": material.state_hash,
        "source_digest": source_digest,
        "report": {
            "report_id": material.report.report_id,
            "content_sha256": material.report.content_sha256,
            "outcome": material.report.outcome,
            "observation_ids": list(material.report.observation_ids),
        },
    }
    if type(material) is CanonicalPremarketMaterial:
        common.update(
            {
                "decision_at": _canonical_timestamp(material.decision_at),
                "validation_window_id": material.validation_window_id,
                "snapshot": _canonical_digest_value(material.snapshot),
                "publication_decision_digest": _publication_decision_digest(
                    material.publication_decision
                ),
                "primary_plan_digest": _long_plan_digest(material.primary_plan),
                "composition_authority": _canonical_digest_value(
                    material.composition_authority
                ),
            }
        )
    else:
        common.update(
            {
                "review_at": _canonical_timestamp(material.review_at),
                "query_cutoff": _canonical_timestamp(material.query_cutoff),
                "actual_state_digest": _actual_state_digest(material.actual_state),
                "actual_replay_source_digest": (
                    material.actual_replay_source.source_digest
                ),
                "positions": _canonical_digest_value(material.positions),
                "composition_authority": _canonical_digest_value(
                    material.composition_authority
                ),
            }
        )
    return _canonical_sha256(
        "stock-monitor/canonical-workflow-material/v1",
        common,
    )


def _validate_derived_material(material: CanonicalMaterial) -> None:
    expected_source_digest = canonical_source_digest(material.source_receipts)
    reasons = _report_reason_codes(material.report)
    if type(material) is CanonicalPremarketMaterial:
        expected_state_hash = canonical_premarket_state_hash(
            session_date=material.session_date,
            decision_at=material.decision_at,
            retrieved_at=material.retrieved_at,
            snapshot=material.snapshot,
            source_receipts=material.source_receipts,
            publication_decision=material.publication_decision,
            primary_plan=material.primary_plan,
            validation_window_id=material.validation_window_id,
            outcome=material.report.outcome,
            reason_codes=reasons,
            composition_authority=material.composition_authority,
        )
        _validate_premarket_report_projection(
            snapshot=material.snapshot,
            report=material.report,
            retrieved_at=material.retrieved_at,
            composition_authority=material.composition_authority,
        )
    else:
        expected_state_hash = canonical_close_state_hash(
            session_date=material.session_date,
            review_at=material.review_at,
            retrieved_at=material.retrieved_at,
            query_cutoff=material.query_cutoff,
            actual_state=material.actual_state,
            actual_replay_source=material.actual_replay_source,
            positions=material.positions,
            source_receipts=material.source_receipts,
            outcome=material.report.outcome,
            reason_codes=reasons,
            composition_authority=material.composition_authority,
        )
        _validate_close_report_projection(
            actual_state=material.actual_state,
            positions=material.positions,
            report=material.report,
            retrieved_at=material.retrieved_at,
            composition_authority=material.composition_authority,
        )
    _validate_report(
        material.report,
        kind=(
            "PREMARKET"
            if type(material) is CanonicalPremarketMaterial
            else "CLOSE"
        ),
        session_date=material.session_date,
        state_hash=expected_state_hash,
        receipts=material.source_receipts,
    )
    if (
        material.source_digest != expected_source_digest
        or material.state_hash != expected_state_hash
        or material.material_digest != canonical_material_digest(material)
    ):
        raise CanonicalMaterialError(
            "canonical material digest derivation changed during issuance"
        )


def _value_fingerprint(value: object, active: set[int] | None = None) -> object:
    """Return a callback-free structural seal for supported immutable values."""
    value_type = type(value)
    if value is None or value_type in {bool, int, str, bytes}:
        return (value_type.__name__, value)
    if value_type is Decimal:
        decimal = value.as_tuple()
        return ("Decimal", decimal.sign, decimal.digits, decimal.exponent)
    if value_type is date:
        return ("date", value.isoformat())
    if value_type is datetime:
        _require_time(value, "canonical fingerprint time")
        return ("datetime", value.astimezone(UTC).isoformat(timespec="microseconds"))
    if value_type is time:
        return ("time", value.isoformat(timespec="microseconds"), value.fold)
    if value_type is Path or isinstance(value, Path):
        return ("path", os.fspath(value))

    if active is None:
        active = set()
    identity = id(value)
    if identity in active:
        raise CanonicalMaterialError("canonical material contains a cycle")
    if value_type is tuple:
        active.add(identity)
        try:
            return ("tuple", tuple(_value_fingerprint(item, active) for item in value))
        finally:
            active.remove(identity)
    if is_dataclass(value) and not isinstance(value, type):
        active.add(identity)
        try:
            return (
                "dataclass",
                value_type.__module__,
                value_type.__qualname__,
                tuple(
                    (field.name, _value_fingerprint(getattr(value, field.name), active))
                    for field in fields(value)
                ),
            )
        finally:
            active.remove(identity)
    # Opaque child authorities are identity-bound.  Their owning issuer must
    # verify their own current seal before calling the material issuer.
    return ("opaque", value_type.__module__, value_type.__qualname__, identity)


def _material_fingerprint(material: CanonicalMaterial) -> object:
    return _value_fingerprint(material)


@dataclass(frozen=True, slots=True)
class _MaterialAuthority:
    material_reference: ReferenceType[object]
    material_fingerprint: object
    journal_reference: ReferenceType[object]
    journal_generation: int
    archive_root: Path
    receipt_candidates: tuple[object, ...]
    identity_children: tuple[object, ...]
    domain_authority: object
    composition_envelope: object


_ISSUED_CANONICAL_MATERIALS: dict[int, _MaterialAuthority] = {}


def _material_identity_children(material: CanonicalMaterial) -> tuple[object, ...]:
    if type(material) is CanonicalPremarketMaterial:
        return (
            material.snapshot,
            material.report,
            material.source_receipts,
            *material.source_receipts,
            material.publication_decision,
            material.primary_plan,
            material.composition_authority,
        )
    return (
        material.actual_state,
        material.actual_replay_source,
        material.composition_authority,
        material.report,
        material.positions,
        *material.positions,
        material.source_receipts,
        *material.source_receipts,
    )


@dataclass(frozen=True, slots=True)
class _PremarketDomainAuthority:
    decision_record: object
    observation_manifest: object
    plan_source_candidates: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _CloseDomainAuthority:
    replay_candidate: object
    state_candidate: object


def _canonical_archive_root(value: object) -> Path:
    expected_type = type(Path())
    if type(value) is not expected_type:
        raise CanonicalMaterialError(
            "canonical archive root must be an exact pathlib path"
        )
    root = Path(os.path.abspath(value))
    if root != value or root.is_symlink() or not root.is_dir():
        raise CanonicalMaterialError("canonical archive root is unverified")
    return root


def _current_receipt_candidates(
    journal: object,
    receipts: tuple[object, ...],
) -> tuple[object, ...] | None:
    from . import journal as journal_module

    candidates = tuple(
        journal_module._source_observation_receipt_authority_candidate(receipt)
        for receipt in receipts
    )
    if any(candidate is None for candidate in candidates):
        return None
    exact_candidates = tuple(
        candidate for candidate in candidates if candidate is not None
    )
    if (
        journal_module._current_journal_source_authority_owner(exact_candidates)
        is not journal
    ):
        return None
    if any(
        not journal_module._is_current_journal_authority_candidate_without_callbacks(
            candidate
        )
        for candidate in exact_candidates
    ):
        return None
    return exact_candidates


def _premarket_economic_external_source_ids(
    composition_authority: object,
) -> tuple[str, ...]:
    """Read exact external identities from bound objects, never Journal details."""
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    with _PREMARKET_COMPOSITION_LOCK:
        composition_candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(
            id(composition_authority)
        )
    source_candidate = (
        None
        if composition_candidate is None
        else composition_candidate.source_binding_candidate
    )
    if type(source_candidate) is not _PremarketSourceBindingCandidate:
        raise CanonicalMaterialError(
            "canonical premarket source binding authority is unavailable"
        )
    identifiers: list[str] = []
    for binding in source_candidate.bindings:
        if binding.decision_basis != "ECONOMIC_INPUT":
            continue
        if type(binding.source) is ProviderFetchPageBundle:
            identifiers.append(binding.source.page.source_observation_id)
        elif type(binding.source) is SourceDocument:
            identifiers.append(binding.source.source_observation_id)
    if len(identifiers) != len(set(identifiers)):
        raise CanonicalMaterialError(
            "canonical premarket external source identity is duplicated"
        )
    return tuple(identifiers)


def _capture_premarket_domain_authority(
    material: CanonicalPremarketMaterial,
    journal: object,
) -> _PremarketDomainAuthority | None:
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "premarket composition authority is unavailable"
        )
    if not material.snapshot.candidates:
        if material.publication_decision is not None or material.primary_plan is not None:
            raise CanonicalMaterialError(
                "no-candidate material cannot carry publication authority"
            )
        return None

    from . import journal as journal_module
    from . import risk as risk_module
    from . import screening as screening_module

    decision = material.publication_decision
    plan = material.primary_plan
    if (
        type(decision) is not screening_module.PublicationDecision
        or type(plan) is not risk_module.LongPlanDecision
        or not risk_module.is_issued_long_plan_decision(plan)
        or not screening_module.is_issued_publication_decision_for_plan(
            decision,
            plan,
        )
    ):
        raise CanonicalMaterialError(
            "candidate material requires its exact issued decision and plan"
        )
    snapshot_roles = tuple(
        (candidate.symbol, candidate.role)
        for candidate in material.snapshot.candidates
    )
    decision_roles = tuple(
        (candidate.symbol, candidate.role) for candidate in decision.candidates
    )
    if (
        snapshot_roles != decision_roles
        or plan.as_of != material.decision_at
        or plan.request is None
        or plan.request.session_date != material.session_date
        or plan.request.symbol != snapshot_roles[0][0]
    ):
        raise CanonicalMaterialError(
            "candidate snapshot, decision, and primary plan are inconsistent"
        )
    observation_manifest = screening_module._publication_observation_manifest(
        decision
    )
    external_source_ids = _premarket_economic_external_source_ids(
        material.composition_authority
    )
    manifest_source_ids = observation_manifest.source_observation_ids
    if any(
        external_source_ids.count(source_id) != 1
        for source_id in manifest_source_ids
    ):
        raise CanonicalMaterialError(
            "canonical source receipts do not cover the publication manifest"
        )
    with screening_module._ISSUED_PUBLICATION_DECISIONS_LOCK:
        decision_record = screening_module._ISSUED_PUBLICATION_DECISIONS.get(
            id(decision)
        )
    if (
        decision_record is None
        or decision_record[0]() is not decision
        or decision_record[2]() is not plan
        or decision_record[3] is not observation_manifest
    ):
        raise CanonicalMaterialError(
            "publication decision registry identity is unverified"
        )

    portfolio = plan.portfolio_authority
    bound_sources = risk_module._phase1_bound_sources(portfolio)
    if not bound_sources:
        raise CanonicalMaterialError(
            "canonical primary plan lacks Journal source lineage"
        )
    source_candidates = tuple(
        journal_module._journal_any_source_authority_candidate(source)
        for source, _source_kind in bound_sources
    )
    if any(candidate is None for candidate in source_candidates):
        raise CanonicalMaterialError(
            "canonical primary plan source lineage is unverified"
        )
    exact_candidates = tuple(
        candidate for candidate in source_candidates if candidate is not None
    )
    if (
        journal_module._current_journal_source_authority_owner(exact_candidates)
        is not journal
    ):
        raise CanonicalMaterialError(
            "canonical primary plan and material cross Journal owners"
        )
    authority = _PremarketDomainAuthority(
        decision_record=decision_record,
        observation_manifest=observation_manifest,
        plan_source_candidates=exact_candidates,
    )
    if not _premarket_domain_is_current_without_callbacks(material, authority):
        raise CanonicalMaterialError(
            "canonical publication authority changed during issuance"
        )
    return authority


def _premarket_domain_is_current_without_callbacks(
    material: CanonicalPremarketMaterial,
    authority: _PremarketDomainAuthority | None,
) -> bool:
    if not material.snapshot.candidates:
        return bool(
            authority is None
            and material.publication_decision is None
            and material.primary_plan is None
        )
    if authority is None:
        return False
    from . import journal as journal_module
    from . import risk as risk_module
    from . import screening as screening_module

    decision = material.publication_decision
    plan = material.primary_plan
    if (
        type(decision) is not screening_module.PublicationDecision
        or type(plan) is not risk_module.LongPlanDecision
        or authority.decision_record[0]() is not decision
        or authority.decision_record[2]() is not plan
        or authority.decision_record[3] is not authority.observation_manifest
        or plan.portfolio_authority is None
        or not risk_module._is_current_portfolio_risk_authority_without_callbacks(
            plan.portfolio_authority
        )
        or not risk_module._is_current_risk_authority_without_callbacks(
            risk_module._LONG_PLAN_AUTHORITIES,
            plan,
            exact_type=risk_module.LongPlanDecision,
            children=(plan.portfolio_authority,),
        )
        or any(
            not journal_module._is_current_journal_authority_candidate_without_callbacks(
                candidate
            )
            for candidate in authority.plan_source_candidates
        )
    ):
        return False
    try:
        fingerprint = screening_module._publication_decision_fingerprint(decision)
    except Exception:
        return False
    with screening_module._ISSUED_PUBLICATION_DECISIONS_LOCK:
        current = screening_module._ISSUED_PUBLICATION_DECISIONS.get(id(decision))
        return bool(
            current is authority.decision_record
            and current[0]() is decision
            and current[1] == fingerprint
            and current[2]() is plan
            and current[3] is authority.observation_manifest
        )


def _capture_close_domain_authority(
    material: CanonicalCloseMaterial,
    journal: object,
) -> _CloseDomainAuthority:
    from . import journal as journal_module
    from . import reconciliation as reconciliation_module

    state = material.actual_state
    replay_source = material.actual_replay_source
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "close composition authority is unavailable"
        )
    if not reconciliation_module.is_verified_actual_ledger_state_for_source(
        state,
        replay_source,
    ):
        raise CanonicalMaterialError(
            "close material actual replay authority is unverified"
        )
    replay_candidate = (
        journal_module._journal_replay_source_authority_candidate(replay_source)
    )
    if replay_candidate is None or (
        journal_module._current_journal_source_authority_owner((replay_candidate,))
        is not journal
    ):
        raise CanonicalMaterialError(
            "close material actual replay has the wrong Journal owner"
        )
    state_candidate = reconciliation_module._actual_ledger_state_authority_candidate(
        state,
        replay_source,
    )
    authority = _CloseDomainAuthority(
        replay_candidate=replay_candidate,
        state_candidate=state_candidate,
    )
    if state_candidate is None or not _close_domain_is_current_without_callbacks(
        material,
        authority,
    ):
        raise CanonicalMaterialError(
            "close material actual replay changed during issuance"
        )
    return authority


def _close_domain_is_current_without_callbacks(
    material: CanonicalCloseMaterial,
    authority: _CloseDomainAuthority,
) -> bool:
    from . import journal as journal_module
    from . import reconciliation as reconciliation_module

    return bool(
        authority.replay_candidate[1] is material.actual_replay_source
        and authority.state_candidate[0] is material.actual_state
        and authority.state_candidate[1] is material.actual_replay_source
        and journal_module._is_current_journal_authority_candidate_without_callbacks(
            authority.replay_candidate
        )
        and reconciliation_module._is_current_actual_ledger_state_authority_candidate_without_callbacks(
            authority.state_candidate
        )
    )


def _capture_domain_authority(
    material: CanonicalMaterial,
    journal: object,
) -> object:
    if type(material) is CanonicalPremarketMaterial:
        return _capture_premarket_domain_authority(material, journal)
    return _capture_close_domain_authority(material, journal)


def _domain_is_current_without_callbacks(
    material: CanonicalMaterial,
    authority: object,
) -> bool:
    if type(material) is CanonicalPremarketMaterial:
        if authority is not None and type(authority) is not _PremarketDomainAuthority:
            return False
        return _premarket_domain_is_current_without_callbacks(material, authority)
    return type(authority) is _CloseDomainAuthority and (
        _close_domain_is_current_without_callbacks(material, authority)
    )


def _register_material(
    material: CanonicalMaterial,
    *,
    journal: object,
    report_archive_root: Path,
) -> CanonicalMaterial:
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError("canonical material requires an open Journal")
    root = _canonical_archive_root(report_archive_root)
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None:
        raise CanonicalMaterialError(
            "canonical source receipts lack one current Journal owner"
        )
    domain_authority = _capture_domain_authority(material, journal)
    # Domain verification may execute source-currentness callbacks.  Refresh
    # the receipt batch before entering the callback-free final seal.
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None:
        raise CanonicalMaterialError(
            "canonical source receipts changed during domain verification"
        )
    # Domain verification above exhausts Journal/source callbacks.  Recompute
    # every semantic digest and report projection before the final structural
    # seal so no mutation during those callbacks can be blessed.
    _validate_derived_material(material)
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "canonical composition authority changed during issuance"
        )
    fingerprint = _material_fingerprint(material)
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("canonical Journal generation is invalid")
    identity = id(material)

    def discard(dead: ReferenceType[object]) -> None:
        with _MATERIAL_AUTHORITY_LOCK:
            current = _ISSUED_CANONICAL_MATERIALS.get(identity)
            if current is not None and current.material_reference is dead:
                _ISSUED_CANONICAL_MATERIALS.pop(identity, None)

    authority = _MaterialAuthority(
        ref(material, discard),
        fingerprint,
        ref(journal),
        generation,
        root,
        candidates,
        _material_identity_children(material),
        domain_authority,
        composition_envelope,
    )
    with _MATERIAL_AUTHORITY_LOCK:
        _ISSUED_CANONICAL_MATERIALS[identity] = authority
    if not _is_current_canonical_material_without_callbacks(
        material,
        authority=authority,
    ):
        with _MATERIAL_AUTHORITY_LOCK:
            if _ISSUED_CANONICAL_MATERIALS.get(identity) is authority:
                _ISSUED_CANONICAL_MATERIALS.pop(identity, None)
        raise CanonicalMaterialError(
            "canonical material changed while its authority was issued"
        )
    return material


def issue_canonical_premarket_material(
    *,
    journal: object,
    report_archive_root: Path,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    snapshot: object,
    report: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
    validation_window_id: str,
    composition_authority: object | None = None,
    state_hash: str | None = None,
    source_digest: str | None = None,
    material_digest: str | None = None,
) -> CanonicalPremarketMaterial:
    """Issue one exact owner-bound premarket material capability."""
    receipts = _receipt_set(source_receipts)
    expected_source_digest = canonical_source_digest(receipts)
    report_reasons = _report_reason_codes(report)
    expected_state_hash = canonical_premarket_state_hash(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation_window_id,
        outcome=report.outcome,
        reason_codes=report_reasons,
        composition_authority=composition_authority,
    )
    for supplied, expected, label in (
        (state_hash, expected_state_hash, "canonical state digest"),
        (source_digest, expected_source_digest, "canonical source digest"),
    ):
        if supplied is not None and (
            _require_digest(supplied, label) != expected
        ):
            raise CanonicalMaterialError(f"{label} conflicts with derived digest")
    _validate_report(
        report,
        kind="PREMARKET",
        session_date=session_date,
        state_hash=expected_state_hash,
        receipts=receipts,
    )
    _validate_premarket_report_projection(
        snapshot=snapshot,
        report=report,
        retrieved_at=retrieved_at,
        composition_authority=composition_authority,
    )
    provisional = CanonicalPremarketMaterial(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        report=report,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation_window_id,
        state_hash=expected_state_hash,
        source_digest=expected_source_digest,
        material_digest="0" * 64,
        composition_authority=composition_authority,
    )
    expected_material_digest = canonical_material_digest(provisional)
    if material_digest is not None and (
        _require_digest(material_digest, "canonical material digest")
        != expected_material_digest
    ):
        raise CanonicalMaterialError(
            "canonical material digest conflicts with derived digest"
        )
    material = replace(
        provisional,
        material_digest=expected_material_digest,
    )
    return _register_material(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )


def issue_canonical_close_material(
    *,
    journal: object,
    report_archive_root: Path,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    composition_authority: object | None = None,
    report: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    state_hash: str | None = None,
    source_digest: str | None = None,
    material_digest: str | None = None,
) -> CanonicalCloseMaterial:
    """Issue one exact owner-bound actual-close material capability."""
    receipts = _receipt_set(source_receipts)
    expected_source_digest = canonical_source_digest(receipts)
    report_reasons = _report_reason_codes(report)
    expected_state_hash = canonical_close_state_hash(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
        outcome=report.outcome,
        reason_codes=report_reasons,
        composition_authority=composition_authority,
    )
    for supplied, expected, label in (
        (state_hash, expected_state_hash, "canonical state digest"),
        (source_digest, expected_source_digest, "canonical source digest"),
    ):
        if supplied is not None and (
            _require_digest(supplied, label) != expected
        ):
            raise CanonicalMaterialError(f"{label} conflicts with derived digest")
    _validate_report(
        report,
        kind="CLOSE",
        session_date=session_date,
        state_hash=expected_state_hash,
        receipts=receipts,
    )
    _validate_close_report_projection(
        actual_state=actual_state,
        positions=positions,
        report=report,
        retrieved_at=retrieved_at,
        composition_authority=composition_authority,
    )
    provisional = CanonicalCloseMaterial(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        composition_authority=composition_authority,
        report=report,
        positions=positions,
        source_receipts=receipts,
        state_hash=expected_state_hash,
        source_digest=expected_source_digest,
        material_digest="0" * 64,
    )
    expected_material_digest = canonical_material_digest(provisional)
    if material_digest is not None and (
        _require_digest(material_digest, "canonical material digest")
        != expected_material_digest
    ):
        raise CanonicalMaterialError(
            "canonical material digest conflicts with derived digest"
        )
    material = replace(
        provisional,
        material_digest=expected_material_digest,
    )
    return _register_material(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )


def _material_authority(
    material: object,
    *,
    journal: object,
    report_archive_root: Path,
) -> _MaterialAuthority | None:
    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        return None
    try:
        root = _canonical_archive_root(report_archive_root)
        fingerprint = _material_fingerprint(material)
    except Exception:
        return None
    with _MATERIAL_AUTHORITY_LOCK:
        authority = _ISSUED_CANONICAL_MATERIALS.get(id(material))
    if (
        authority is None
        or authority.material_reference() is not material
        or authority.journal_reference() is not journal
        or authority.archive_root != root
        or authority.material_fingerprint != fingerprint
        or len(_material_identity_children(material))
        != len(authority.identity_children)
        or any(
            current is not expected
            for current, expected in zip(
                _material_identity_children(material),
                authority.identity_children,
                strict=True,
            )
        )
        or not _domain_is_current_without_callbacks(
            material,
            authority.domain_authority,
        )
        or not _composition_is_current_without_callbacks(
            material,
            authority.composition_envelope,
        )
    ):
        return None
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None or len(candidates) != len(authority.receipt_candidates):
        return None
    if any(
        not (
            current[0] is expected[0]
            and current[1] is expected[1]
            and current[2] is expected[2]
            and current[3:5] == expected[3:5]
            and current[5] is expected[5]
            and current[7] is expected[7]
        )
        for current, expected in zip(
            candidates,
            authority.receipt_candidates,
            strict=True,
        )
    ):
        return None
    return authority


def _is_current_canonical_material_without_callbacks(
    material: object,
    *,
    authority: _MaterialAuthority | None = None,
) -> bool:
    from . import journal as journal_module
    from .reports import is_issued_report

    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        return False
    if authority is None:
        with _MATERIAL_AUTHORITY_LOCK:
            authority = _ISSUED_CANONICAL_MATERIALS.get(id(material))
    journal = None if authority is None else authority.journal_reference()
    try:
        fingerprint = _material_fingerprint(material)
    except Exception:
        return False
    if (
        authority is None
        or authority.material_reference() is not material
        or journal is None
        or getattr(journal, "_closed", True)
        or getattr(journal, "_source_generation", None)
        != authority.journal_generation
        or authority.material_fingerprint != fingerprint
        or len(_material_identity_children(material))
        != len(authority.identity_children)
        or any(
            current is not expected
            for current, expected in zip(
                _material_identity_children(material),
                authority.identity_children,
                strict=True,
            )
        )
        or not is_issued_report(material.report)
        or not _domain_is_current_without_callbacks(
            material,
            authority.domain_authority,
        )
        or any(
            not journal_module._is_current_journal_authority_candidate_without_callbacks(
                candidate
            )
            for candidate in authority.receipt_candidates
        )
        or not _composition_is_current_without_callbacks(
            material,
            authority.composition_envelope,
        )
    ):
        return False
    with _MATERIAL_AUTHORITY_LOCK:
        return _ISSUED_CANONICAL_MATERIALS.get(id(material)) is authority


def is_issued_canonical_material(
    material: object,
    *,
    journal: object,
    report_archive_root: Path,
) -> bool:
    """Return whether material is exact, unchanged, current, and owner-bound."""
    authority = _material_authority(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )
    return authority is not None and _is_current_canonical_material_without_callbacks(
        material,
        authority=authority,
    )


__all__ = [
    "CanonicalCloseCompositionAuthority",
    "CanonicalCloseMaterial",
    "CanonicalMaterialError",
    "CanonicalPremarketCompositionAuthority",
    "CanonicalPremarketMaterial",
    "CanonicalPremarketSourceBindingAuthority",
    "CanonicalWorkflowAdapter",
    "PremarketSourceBinding",
    "canonical_close_state_hash",
    "canonical_material_digest",
    "canonical_premarket_state_hash",
    "canonical_source_digest",
    "canonical_source_receipts",
    "is_issued_canonical_material",
    "is_issued_canonical_premarket_composition_authority",
    "is_issued_canonical_premarket_source_binding_authority",
    "issue_canonical_close_material",
    "issue_canonical_premarket_material",
    "issue_canonical_premarket_composition_authority",
    "issue_canonical_premarket_source_binding_authority",
]
