"""Reviewed primary-source evidence and fail-closed classification decisions."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import stat
import threading
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from urllib.parse import parse_qsl, urlsplit

from stock_monitor.domain import require_aware_timestamp
from stock_monitor.providers.cache import SourceDocument
from stock_monitor.universe import UniverseSnapshot, is_verified_universe_snapshot


POSITIVE_EVENT_TYPES = (
    "financial results/guidance",
    "material agreement",
    "product/regulatory milestone",
    "capital allocation",
    "management/governance",
    "acquisition/disposition",
)
ETF_POSITIVE_EVENT_TYPES = (
    "fund sponsor notice",
    "index provider notice",
)
ADVERSE_TAGS = (
    "lowered guidance",
    "restatement",
    "default/bankruptcy",
    "enforcement action",
    "product recall/regulatory rejection",
    "going-concern",
    "dilutive financing",
)
CURRENT_EVIDENCE_REGISTRY_SHA256 = (
    "5c97d3c09117b2618944e8934896564abef0310f35b9ca31bdf8d0694f7714e4"
)
CURRENT_EVIDENCE_RELEASE_SHA256 = CURRENT_EVIDENCE_REGISTRY_SHA256
_EVENT_KINDS = frozenset({"BINARY_EVENT", "ETF_ACTION"})
_BINARY_COVERAGE = frozenset(
    {"CONFIRMED_CLEAR", "NOT_APPLICABLE", "OVERLAP", "UNKNOWN", "CONFLICT"}
)
_ETF_COVERAGE = _BINARY_COVERAGE
_SUBJECT_KINDS = frozenset({"STOCK", "ETF"})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}\Z")
_CIK = re.compile(r"[0-9]{10}\Z")
_ACCESSION = re.compile(r"([0-9]{10})-([0-9]{2})-([0-9]{6})\Z")
_ARCHIVE_FILENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SCOPED_REFERENCE_ROLE = re.compile(
    r"(?:ISSUER_IR|CORPORATE_ACTION):([A-Z][A-Z0-9.-]{0,14})\Z"
)
_MAX_REGISTRY_BYTES = 1_048_576
_MAX_RELEASE_BYTES = 1_048_576
_MAX_SOURCE_BODY_BYTES = 67_108_864
_MAX_SOURCE_ARTIFACT_BYTES = 90_000_000
_SEC_PUBLISHER = "U.S. Securities and Exchange Commission"
_REFERENCE_ROLE_URLS = {
    "CROSS_CHECK_CALENDAR": (
        "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        "Nasdaq",
    ),
    "OPERATIONAL_STATUS": (
        "https://www.nyse.com/api/notifications/public/alerts?2=3",
        "New York Stock Exchange",
    ),
    "PRIMARY_CALENDAR": (
        "https://www.nyse.com/trade/hours-calendars",
        "New York Stock Exchange",
    ),
    "TRADER_ALERT_HALT": (
        "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
        "Nasdaq",
    ),
}
_MISSING = object()
_REVIEWED_AUTHORITY = object()
_REVIEWED_RELEASE_AUTHORITY = object()
_REVIEWED_ISSUANCE_LOCK = threading.Lock()
_REVIEWED_ISSUANCES: dict[
    int,
    tuple[weakref.ReferenceType[object], str, str, str, str],
] = {}
_REVIEWED_RELEASE_ISSUANCE_LOCK = threading.Lock()
_REVIEWED_RELEASE_ISSUANCES: dict[
    int,
    tuple[weakref.ReferenceType[object], str, str],
] = {}
_SENSITIVE_QUERY_PARTS = (
    "authorization",
    "credential",
    "password",
    "secret",
    "signature",
    "token",
)
_SENSITIVE_QUERY_SUFFIXES = (
    "apikey",
    "authorization",
    "credential",
    "keyid",
    "password",
    "secret",
    "signature",
    "token",
)


class EvidenceRegistryError(RuntimeError):
    """A reviewed evidence registry failed integrity or schema verification."""


class EvidenceUnavailableError(RuntimeError):
    """Required reviewed evidence authority or subject context is unavailable."""


def _utc(value: datetime, name: str) -> datetime:
    return require_aware_timestamp(value, name).astimezone(UTC)


def _parse_timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceRegistryError(f"registry {name} is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _utc(parsed, name)
    except (TypeError, ValueError):
        raise EvidenceRegistryError(f"registry {name} is malformed") from None


def _parse_optional_timestamp(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    return _parse_timestamp(value, name)


def _parse_date(value: object, name: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise EvidenceRegistryError(f"registry {name} is malformed")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise EvidenceRegistryError(f"registry {name} is malformed") from None


def _subject(symbol: object, issuer_cik: object) -> tuple[str, str | None]:
    if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
        raise ValueError("evidence symbol is malformed")
    if issuer_cik is not None and (
        not isinstance(issuer_cik, str) or not _CIK.fullmatch(issuer_cik)
    ):
        raise ValueError("evidence issuer CIK must be ten decimal digits")
    return symbol, issuer_cik


def _primary_url(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or any(ord(character) <= 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("evidence primary URL is malformed")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("evidence primary URL is malformed") from None
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError("evidence primary URL must be credential-free HTTPS")
    try:
        query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        raise ValueError("evidence primary URL query is malformed") from None
    for name, _ in query:
        folded = name.casefold()
        compact = re.sub(r"[^a-z0-9]", "", folded)
        if folded.endswith("key") or any(
            part in folded for part in _SENSITIVE_QUERY_PARTS
        ) or any(compact.endswith(part) for part in _SENSITIVE_QUERY_SUFFIXES):
            raise ValueError("evidence primary URL contains credential material")
    return value


def _text(value: object, name: str, *, maximum: int = 1_000) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or not value.isprintable()
        or "\n" in value
        or "\r" in value
        or len(value) > maximum
    ):
        raise ValueError(f"evidence {name} is malformed")
    return value


@dataclass(frozen=True, slots=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("evidence date range requires dates")
        if self.start > self.end:
            raise ValueError("evidence date range start must not follow end")

    def contains(self, value: date) -> bool:
        return self.start <= value <= self.end


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    record_id: str
    symbol: str
    issuer_cik: str | None
    primary_url: str
    publisher: str
    published_at: datetime
    retrieved_at: datetime
    event_type: str | None
    fact: str
    content_hash: str
    source_observation_ids: tuple[str, ...]
    accession: str | None = None
    adverse_tags: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()
    classification_ambiguous: bool = False
    event_date: date | None = None
    event_kind: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.record_id, str) or not _IDENTIFIER.fullmatch(
            self.record_id
        ):
            raise ValueError("evidence record ID is malformed")
        symbol, issuer_cik = _subject(self.symbol, self.issuer_cik)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "issuer_cik", issuer_cik)
        object.__setattr__(self, "primary_url", _primary_url(self.primary_url))
        object.__setattr__(
            self,
            "publisher",
            _text(self.publisher, "publisher", maximum=256),
        )
        published_at = _utc(self.published_at, "evidence publication time")
        retrieved_at = _utc(self.retrieved_at, "evidence retrieval time")
        if published_at > retrieved_at:
            raise ValueError("evidence publication cannot follow retrieval")
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "retrieved_at", retrieved_at)
        if self.event_type is not None:
            object.__setattr__(
                self,
                "event_type",
                _text(self.event_type, "event type"),
            )
        object.__setattr__(self, "fact", _text(self.fact, "fact"))
        if not isinstance(self.content_hash, str) or not _SHA256.fullmatch(
            self.content_hash
        ):
            raise ValueError("evidence content hash must be lowercase SHA-256")
        identifiers = tuple(self.source_observation_ids)
        if (
            not identifiers
            or len(identifiers) != len(set(identifiers))
            or any(
                not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)
                for value in identifiers
            )
        ):
            raise ValueError("evidence source observation IDs are malformed")
        object.__setattr__(self, "source_observation_ids", identifiers)
        if self.accession is not None:
            _text(self.accession, "accession", maximum=64)
        adverse = tuple(self.adverse_tags)
        if len(adverse) != len(set(adverse)) or any(
            value not in ADVERSE_TAGS for value in adverse
        ):
            raise ValueError("evidence adverse tags are outside the closed taxonomy")
        object.__setattr__(self, "adverse_tags", adverse)
        conflicts = tuple(self.conflicts)
        if len(conflicts) != len(set(conflicts)):
            raise ValueError("evidence conflicts are malformed")
        for conflict in conflicts:
            _text(conflict, "conflict")
        object.__setattr__(self, "conflicts", conflicts)
        if type(self.classification_ambiguous) is not bool:
            raise TypeError("evidence ambiguity flag must be boolean")
        if (self.event_date is None) != (self.event_kind is None):
            raise ValueError("evidence dated event requires both date and kind")
        if self.event_date is not None and type(self.event_date) is not date:
            raise TypeError("evidence event date must be a date")
        if self.event_kind is not None and self.event_kind not in _EVENT_KINDS:
            raise ValueError("evidence event kind is unsupported")


def _validate_source_identity(
    document: SourceDocument,
    *,
    symbol: str,
    issuer_cik: str | None,
) -> None:
    """Bind a source envelope to one reviewed provider/role and subject."""
    parsed = urlsplit(document.url)
    if document.source_type == "SEC_ARCHIVE":
        if (
            issuer_cik is None
            or document.publisher != _SEC_PUBLISHER
            or document.timestamp_source != "SEC_FILING_METADATA"
            or document.source_role is not None
            or document.published_at is None
            or document.accession is None
        ):
            raise ValueError("SEC evidence source identity is malformed")
        accession = _ACCESSION.fullmatch(document.accession)
        components = parsed.path.split("/")
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www.sec.gov"
            or parsed.query
            or accession is None
            or accession.group(1) != issuer_cik
            or len(components) != 7
            or components[:5]
            != ["", "Archives", "edgar", "data", str(int(issuer_cik))]
            or components[5] != document.accession.replace("-", "")
            or not _ARCHIVE_FILENAME.fullmatch(components[6])
            or components[6] in {".", ".."}
        ):
            raise ValueError("SEC archive evidence is outside its issuer/accession")
        return
    if document.source_type == "SEC_SUBMISSIONS":
        expected_url = f"https://data.sec.gov/submissions/CIK{issuer_cik}.json"
        if (
            issuer_cik is None
            or document.url != expected_url
            or document.publisher != _SEC_PUBLISHER
            or document.timestamp_source != "SEC_SUBMISSIONS_METADATA"
            or document.source_role is not None
            or document.accession is not None
        ):
            raise ValueError("SEC submissions evidence source identity is malformed")
        return
    if document.source_type != "OFFICIAL_REFERENCE":
        raise ValueError("evidence source type is not reviewed")
    role = document.source_role
    if role in _REFERENCE_ROLE_URLS:
        expected_url, expected_publisher = _REFERENCE_ROLE_URLS[role]
        timestamp_source_is_valid = document.timestamp_source == "PRIMARY_METADATA"
        if role == "OPERATIONAL_STATUS":
            timestamp_source_is_valid = document.timestamp_source in {
                "PRIMARY_METADATA",
                "UNAVAILABLE",
            }
        if (
            document.url != expected_url
            or document.publisher != expected_publisher
            or not timestamp_source_is_valid
            or document.accession is not None
        ):
            raise ValueError("official evidence source role identity is malformed")
        return
    scoped = _SCOPED_REFERENCE_ROLE.fullmatch(role or "")
    if (
        scoped is None
        or scoped.group(1) != symbol
        or document.timestamp_source != "PRIMARY_METADATA"
        or document.accession is not None
    ):
        raise ValueError("scoped evidence source role is not bound to its subject")


@dataclass(frozen=True, slots=True)
class EvidenceSourceBinding:
    """Subject-scoped health binding backed by verified source bytes."""

    document: SourceDocument
    symbol: str
    issuer_cik: str | None
    checked_at: datetime
    valid_until: datetime
    healthy: bool

    def __post_init__(self) -> None:
        if not isinstance(self.document, SourceDocument):
            raise TypeError("evidence binding requires a verified source document")
        symbol, issuer_cik = _subject(self.symbol, self.issuer_cik)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "issuer_cik", issuer_cik)
        _validate_source_identity(
            self.document,
            symbol=symbol,
            issuer_cik=issuer_cik,
        )
        checked_at = _utc(self.checked_at, "evidence source health check")
        valid_until = _utc(self.valid_until, "evidence source health validity")
        if (
            checked_at < self.document.retrieved_at
            or valid_until < checked_at
            or valid_until - checked_at > timedelta(hours=24)
        ):
            raise ValueError("evidence source health validity is malformed")
        object.__setattr__(self, "checked_at", checked_at)
        object.__setattr__(self, "valid_until", valid_until)
        if type(self.healthy) is not bool:
            raise TypeError("evidence source health must be explicit")

    @classmethod
    def from_document(
        cls,
        document: SourceDocument,
        *,
        symbol: str,
        issuer_cik: str | None,
        checked_at: datetime,
        valid_until: datetime,
        healthy: bool,
    ) -> EvidenceSourceBinding:
        return cls(
            document=document,
            symbol=symbol,
            issuer_cik=issuer_cik,
            checked_at=checked_at,
            valid_until=valid_until,
            healthy=healthy,
        )

    @property
    def source_observation_id(self) -> str:
        return self.document.source_observation_id

    @property
    def primary_url(self) -> str:
        return self.document.url

    @property
    def publisher(self) -> str:
        return self.document.publisher

    @property
    def content_hash(self) -> str:
        return self.document.content_hash

    @property
    def retrieved_at(self) -> datetime:
        return self.document.retrieved_at


@dataclass(frozen=True, slots=True)
class EvidenceCoverageAttestation:
    subject_kind: str
    symbol: str
    issuer_cik: str | None
    coverage_kind: str
    coverage: str
    coverage_start: date
    coverage_end: date
    source_observation_ids: tuple[str, ...]
    checked_at: datetime
    valid_until: datetime
    healthy: bool
    complete: bool
    conflicts: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.subject_kind not in _SUBJECT_KINDS:
            raise ValueError("evidence subject kind is unsupported")
        symbol, issuer_cik = _subject(self.symbol, self.issuer_cik)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "issuer_cik", issuer_cik)
        if (self.subject_kind == "STOCK" and issuer_cik is None) or (
            self.subject_kind == "ETF" and issuer_cik is not None
        ):
            raise ValueError("evidence coverage subject/CIK combination is invalid")
        if self.coverage_kind not in _EVENT_KINDS:
            raise ValueError("evidence coverage kind is unsupported")
        allowed = (
            _BINARY_COVERAGE
            if self.coverage_kind == "BINARY_EVENT"
            else _ETF_COVERAGE
        )
        if self.coverage not in allowed:
            raise ValueError("evidence coverage state is unsupported")
        if (
            type(self.coverage_start) is not date
            or type(self.coverage_end) is not date
            or self.coverage_start > self.coverage_end
        ):
            raise ValueError("evidence coverage date range is malformed")
        identifiers = tuple(self.source_observation_ids)
        if (
            not identifiers
            or len(identifiers) != len(set(identifiers))
            or any(
                not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)
                for value in identifiers
            )
        ):
            raise ValueError("evidence coverage provenance is malformed")
        object.__setattr__(self, "source_observation_ids", identifiers)
        checked_at = _utc(self.checked_at, "evidence coverage check")
        valid_until = _utc(self.valid_until, "evidence coverage validity")
        if (
            valid_until < checked_at
            or valid_until - checked_at > timedelta(hours=24)
        ):
            raise ValueError("evidence coverage validity is malformed")
        object.__setattr__(self, "checked_at", checked_at)
        object.__setattr__(self, "valid_until", valid_until)
        if type(self.healthy) is not bool or type(self.complete) is not bool:
            raise TypeError("evidence coverage health must be explicit")
        conflicts = tuple(self.conflicts)
        if len(conflicts) != len(set(conflicts)):
            raise ValueError("evidence coverage conflicts are malformed")
        for conflict in conflicts:
            _text(conflict, "coverage conflict")
        object.__setattr__(self, "conflicts", conflicts)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class EvidenceDecision:
    subject_kind: str
    symbol: str
    issuer_cik: str | None
    as_of: datetime
    qualifying_records: tuple[EvidenceRecord, ...]
    adverse_tags: tuple[str, ...]
    ambiguities: tuple[str, ...]
    conflicts: tuple[str, ...]
    binary_events: tuple[tuple[date, str | None], ...]
    etf_actions: tuple[tuple[date, str | None], ...]
    binary_event_coverage: str
    etf_action_coverage: str
    health: str
    retrieved_at: datetime | None
    source_observation_ids: tuple[str, ...]
    block_reason: str | None
    registry_id: str | None = None
    registry_content_hash: str | None = None
    _authority: object = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _decision_digest: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _reviewed_bundle: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )


def _decision_fingerprint(value: EvidenceDecision) -> str:
    if (
        type(value) is not EvidenceDecision
        or type(value.subject_kind) is not str
        or type(value.symbol) is not str
        or (value.issuer_cik is not None and type(value.issuer_cik) is not str)
        or type(value.as_of) is not datetime
        or type(value.qualifying_records) is not tuple
        or any(type(record) is not EvidenceRecord for record in value.qualifying_records)
        or any(
            type(items) is not tuple or any(type(item) is not str for item in items)
            for items in (
                value.adverse_tags,
                value.ambiguities,
                value.conflicts,
                value.source_observation_ids,
            )
        )
        or type(value.binary_events) is not tuple
        or type(value.etf_actions) is not tuple
        or type(value.binary_event_coverage) is not str
        or type(value.etf_action_coverage) is not str
        or type(value.health) is not str
        or (value.retrieved_at is not None and type(value.retrieved_at) is not datetime)
        or (value.block_reason is not None and type(value.block_reason) is not str)
        or type(value.registry_id) is not str
        or type(value.registry_content_hash) is not str
    ):
        raise TypeError("reviewed evidence decision fields are malformed")

    def event_document(values: tuple[tuple[date, str | None], ...]) -> list[list[object]]:
        result: list[list[object]] = []
        for item in values:
            if (
                type(item) is not tuple
                or len(item) != 2
                or type(item[0]) is not date
                or (item[1] is not None and type(item[1]) is not str)
            ):
                raise TypeError("reviewed evidence decision events are malformed")
            result.append([item[0].isoformat(), item[1]])
        return result

    document = {
        "adverse_tags": list(value.adverse_tags),
        "ambiguities": list(value.ambiguities),
        "as_of": _iso_timestamp(value.as_of),
        "binary_event_coverage": value.binary_event_coverage,
        "binary_events": event_document(value.binary_events),
        "block_reason": value.block_reason,
        "conflicts": list(value.conflicts),
        "etf_action_coverage": value.etf_action_coverage,
        "etf_actions": event_document(value.etf_actions),
        "health": value.health,
        "issuer_cik": value.issuer_cik,
        "qualifying_records": [
            {**_record_document(record), "content_hash": record.content_hash}
            for record in value.qualifying_records
        ],
        "registry_content_hash": value.registry_content_hash,
        "registry_id": value.registry_id,
        "retrieved_at": (
            _iso_timestamp(value.retrieved_at)
            if value.retrieved_at is not None
            else None
        ),
        "source_observation_ids": list(value.source_observation_ids),
        "subject_kind": value.subject_kind,
        "symbol": value.symbol,
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _remember_reviewed_authority(
    value: object,
    *,
    kind: str,
    digest: str,
    registry_id: str,
    release_pin: str,
) -> None:
    object_id = id(value)

    def forget(reference: weakref.ReferenceType[object]) -> None:
        with _REVIEWED_ISSUANCE_LOCK:
            current = _REVIEWED_ISSUANCES.get(object_id)
            if current is not None and current[0] is reference:
                _REVIEWED_ISSUANCES.pop(object_id, None)

    reference = weakref.ref(value, forget)
    with _REVIEWED_ISSUANCE_LOCK:
        _REVIEWED_ISSUANCES[object_id] = (
            reference,
            kind,
            digest,
            registry_id,
            release_pin,
        )


def _reviewed_authority_issuance(
    value: object,
    *,
    kind: str,
) -> tuple[str, str, str] | None:
    with _REVIEWED_ISSUANCE_LOCK:
        issuance = _REVIEWED_ISSUANCES.get(id(value))
        if issuance is None or issuance[0]() is not value or issuance[1] != kind:
            return None
        return issuance[2], issuance[3], issuance[4]


def is_reviewed_evidence_decision(value: object) -> bool:
    """Return true only for untampered output from the pinned classifier path."""
    if (
        type(value) is not EvidenceDecision
        or value._authority is not _REVIEWED_AUTHORITY
        or type(value.registry_id) is not str
        or _IDENTIFIER.fullmatch(value.registry_id) is None
        or type(value.registry_content_hash) is not str
        or _SHA256.fullmatch(value.registry_content_hash) is None
        or type(value._decision_digest) is not str
        or _SHA256.fullmatch(value._decision_digest) is None
    ):
        return False
    try:
        issuance = _reviewed_authority_issuance(
            value,
            kind="EVIDENCE_DECISION",
        )
        if issuance is None:
            return False
        expected_digest, expected_registry_id, expected_release_pin = issuance
        return (
            value._decision_digest == expected_digest
            and _decision_fingerprint(value) == expected_digest
            and value.registry_id == expected_registry_id
            and value.registry_content_hash == expected_release_pin
            and _is_reviewed_bundle(value._reviewed_bundle)
            and value._reviewed_bundle.registry_id == value.registry_id
            and value._reviewed_bundle.content_hash
            == value.registry_content_hash
        )
    except (TypeError, ValueError):
        return False


@dataclass(frozen=True, slots=True)
class EvidenceRegistry:
    registry_id: str
    reviewed_at: datetime
    subject_kind: str | None
    symbol: str | None
    issuer_cik: str | None
    records: tuple[EvidenceRecord, ...]
    source_bindings: tuple[EvidenceSourceBinding, ...]
    coverage_attestations: tuple[EvidenceCoverageAttestation, ...]
    content_hash: str


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ReviewedEvidenceBundle:
    """Release-pinned authority accepted by the classification boundary."""

    registry_id: str
    reviewed_at: datetime
    subject_kind: str | None
    symbol: str | None
    issuer_cik: str | None
    records: tuple[EvidenceRecord, ...]
    source_bindings: tuple[EvidenceSourceBinding, ...]
    coverage_attestations: tuple[EvidenceCoverageAttestation, ...]
    content_hash: str
    _authority: object = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _release_pin: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _bundle_digest: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _phase1_source: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ReviewedEvidenceRelease:
    """One externally pinned, universe-complete reviewed evidence release."""

    release_id: str
    release_sha256: str
    universe_sha256: str
    reviewed_at: datetime
    review_by: datetime
    by_symbol: Mapping[str, ReviewedEvidenceBundle]
    _authority: object = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _release_digest: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )


def _binding_map(
    bindings: Sequence[EvidenceSourceBinding],
) -> dict[str, EvidenceSourceBinding]:
    result: dict[str, EvidenceSourceBinding] = {}
    for binding in bindings:
        if not isinstance(binding, EvidenceSourceBinding):
            raise TypeError("evidence source binding has the wrong type")
        if binding.source_observation_id in result:
            raise ValueError("evidence source observation binding is duplicated")
        result[binding.source_observation_id] = binding
    return result


def _verify_bindings(
    records: Sequence[EvidenceRecord],
    bindings: Sequence[EvidenceSourceBinding],
    coverage: Sequence[EvidenceCoverageAttestation],
    *,
    symbol: str,
    issuer_cik: str | None,
) -> dict[str, EvidenceSourceBinding]:
    by_id = _binding_map(bindings)
    referenced: set[str] = set()
    for record in records:
        for identifier in record.source_observation_ids:
            referenced.add(identifier)
            binding = by_id.get(identifier)
            if binding is None:
                raise EvidenceUnavailableError(
                    "evidence record has no verified source binding"
                )
            if (
                binding.primary_url != record.primary_url
                or binding.publisher != record.publisher
                or binding.content_hash != record.content_hash
                or binding.symbol != record.symbol
                or binding.issuer_cik != record.issuer_cik
                or binding.retrieved_at != record.retrieved_at
                or binding.document.published_at != record.published_at
                or binding.document.accession != record.accession
            ):
                raise ValueError("evidence record conflicts with its source binding")
    for attestation in coverage:
        if not isinstance(attestation, EvidenceCoverageAttestation):
            raise TypeError("evidence coverage attestation has the wrong type")
        if attestation.symbol != symbol or attestation.issuer_cik != issuer_cik:
            raise ValueError("evidence coverage belongs to a different subject")
        for identifier in attestation.source_observation_ids:
            referenced.add(identifier)
            binding = by_id.get(identifier)
            if binding is None:
                raise EvidenceUnavailableError(
                    "evidence coverage has no verified source binding"
                )
            if binding.symbol != symbol or binding.issuer_cik != issuer_cik:
                raise ValueError("evidence coverage source belongs to another subject")
            if (
                attestation.checked_at < binding.document.retrieved_at
                or attestation.checked_at > binding.valid_until
            ):
                raise ValueError("evidence coverage conflicts with source validity")
    if set(by_id) != referenced:
        raise ValueError("evidence registry contains unreferenced source bindings")
    return by_id


def classify_evidence(
    records: Sequence[EvidenceRecord],
    hold: DateRange,
    *,
    symbol: object = _MISSING,
    issuer_cik: object = _MISSING,
    source_bindings: object = _MISSING,
    as_of: object = _MISSING,
    subject_kind: object = _MISSING,
    coverage_attestations: object = _MISSING,
    binary_event_coverage: str | None = None,
    etf_action_coverage: str | None = None,
    max_source_age: timedelta = timedelta(hours=24),
    reviewed_bundle: object = _MISSING,
) -> EvidenceDecision:
    """Classify raw reviewed facts without sentiment inference or point scoring."""
    missing_context = (
        symbol is _MISSING
        or issuer_cik is _MISSING
        or source_bindings is _MISSING
        or as_of is _MISSING
        or subject_kind is _MISSING
        or reviewed_bundle is _MISSING
    )
    if missing_context:
        raise EvidenceUnavailableError(
            "reviewed evidence subject, time, bindings, and authority are required"
        )
    if subject_kind not in _SUBJECT_KINDS:
        raise ValueError("evidence subject kind is unsupported")
    if (subject_kind == "STOCK" and issuer_cik is None) or (
        subject_kind == "ETF" and issuer_cik is not None
    ):
        raise EvidenceUnavailableError(
            "reviewed evidence subject kind and issuer CIK are inconsistent"
        )
    try:
        subject_symbol, subject_cik = _subject(symbol, issuer_cik)
        current = _utc(as_of, "evidence as_of")
    except (TypeError, ValueError):
        raise EvidenceUnavailableError("reviewed evidence context is malformed") from None
    if not _is_reviewed_bundle(reviewed_bundle):
        raise EvidenceUnavailableError("pinned reviewed evidence authority is required")
    assert isinstance(reviewed_bundle, ReviewedEvidenceBundle)
    if (
        reviewed_bundle.subject_kind != subject_kind
        or reviewed_bundle.symbol != subject_symbol
        or reviewed_bundle.issuer_cik != subject_cik
    ):
        raise EvidenceUnavailableError(
            "reviewed evidence authority belongs to a different subject"
        )
    if isinstance(source_bindings, (str, bytes)):
        raise EvidenceUnavailableError("reviewed evidence bindings are unavailable")
    try:
        supplied_bindings = tuple(source_bindings)
    except TypeError:
        raise EvidenceUnavailableError("reviewed evidence bindings are unavailable") from None
    if not supplied_bindings or supplied_bindings != reviewed_bundle.source_bindings:
        raise EvidenceUnavailableError(
            "classification bindings do not match reviewed evidence authority"
        )
    if coverage_attestations is _MISSING:
        attestations = reviewed_bundle.coverage_attestations
    else:
        if isinstance(coverage_attestations, (str, bytes)):
            raise EvidenceUnavailableError(
                "reviewed evidence coverage is unavailable"
            )
        try:
            supplied_coverage = tuple(coverage_attestations)
        except TypeError:
            raise EvidenceUnavailableError(
                "reviewed evidence coverage is unavailable"
            ) from None
        if supplied_coverage != reviewed_bundle.coverage_attestations:
            raise EvidenceUnavailableError(
                "classification coverage does not match reviewed evidence authority"
            )
        attestations = supplied_coverage
    if binary_event_coverage is not None or etf_action_coverage is not None:
        raise EvidenceUnavailableError(
            "bare caller-authored evidence coverage is not authoritative"
        )
    if isinstance(records, (str, bytes)) or not isinstance(hold, DateRange):
        raise TypeError("evidence classification inputs are malformed")
    if (
        not isinstance(max_source_age, timedelta)
        or max_source_age <= timedelta(0)
        or max_source_age > timedelta(hours=24)
    ):
        raise ValueError("evidence source age policy is malformed")
    values = tuple(records)
    if any(not isinstance(value, EvidenceRecord) for value in values):
        raise TypeError("evidence records have the wrong type")
    if values != reviewed_bundle.records:
        raise EvidenceUnavailableError(
            "classification records do not match reviewed evidence authority"
        )
    identifiers = [value.record_id for value in values]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("evidence record IDs must be unique")
    if any(
        value.symbol != subject_symbol or value.issuer_cik != subject_cik
        for value in values
    ):
        raise ValueError("evidence record belongs to a different subject")
    coverage_by_kind: dict[str, EvidenceCoverageAttestation] = {}
    for attestation in attestations:
        if not isinstance(attestation, EvidenceCoverageAttestation):
            raise TypeError("evidence coverage attestation has the wrong type")
        if attestation.subject_kind != subject_kind:
            raise ValueError("evidence coverage has the wrong subject kind")
        if attestation.coverage_kind in coverage_by_kind:
            raise ValueError("evidence coverage kind is duplicated")
        coverage_by_kind[attestation.coverage_kind] = attestation
    bindings = _verify_bindings(
        values,
        supplied_bindings,
        attestations,
        symbol=subject_symbol,
        issuer_cik=subject_cik,
    )
    ordered = tuple(
        sorted(
            values,
            key=lambda value: (-value.published_at.timestamp(), value.record_id),
        )
    )
    retrieved_values = [value.retrieved_at for value in ordered]
    retrieved_values.extend(binding.retrieved_at for binding in bindings.values())
    retrieved_at = max(retrieved_values, default=None)
    source_ids = tuple(
        sorted(
            {
                identifier
                for value in ordered
                for identifier in value.source_observation_ids
            }
            | {
                identifier
                for attestation in attestations
                for identifier in attestation.source_observation_ids
            }
        )
    )
    future = reviewed_bundle.reviewed_at > current or any(
        value.published_at > current or value.retrieved_at > current
        for value in ordered
    ) or any(
        binding.retrieved_at > current or binding.checked_at > current
        for binding in bindings.values()
    ) or any(attestation.checked_at > current for attestation in attestations)
    stale = any(
        current - value.retrieved_at > max_source_age for value in ordered
    ) or any(
        current > binding.valid_until
        or current - binding.retrieved_at > max_source_age
        for binding in bindings.values()
    ) or any(current > attestation.valid_until for attestation in attestations)
    unhealthy = any(not binding.healthy for binding in bindings.values()) or any(
        not attestation.healthy for attestation in attestations
    )
    if not bindings or not attestations:
        health = "MISSING"
    elif unhealthy:
        health = "UNAVAILABLE"
    elif stale:
        health = "STALE"
    elif future:
        health = "CONFLICT"
    else:
        health = "HEALTHY"
    qualifying_taxonomy = (
        POSITIVE_EVENT_TYPES if subject_kind == "STOCK" else ETF_POSITIVE_EVENT_TYPES
    )
    ambiguities = tuple(
        value.record_id
        for value in ordered
        if value.classification_ambiguous
        or (value.event_type not in qualifying_taxonomy and not value.adverse_tags)
    )
    conflicts = tuple(
        dict.fromkeys(
            [conflict for value in ordered for conflict in value.conflicts]
            + [
                conflict
                for attestation in attestations
                for conflict in attestation.conflicts
            ]
        )
    )
    adverse_set = {tag for value in ordered for tag in value.adverse_tags}
    adverse = tuple(tag for tag in ADVERSE_TAGS if tag in adverse_set)
    binary_events = tuple(
        sorted(
            {
                (value.event_date, value.event_type)
                for value in ordered
                if value.event_kind == "BINARY_EVENT" and value.event_date is not None
            },
            key=lambda item: (item[0], item[1] or ""),
        )
    )
    etf_actions = tuple(
        sorted(
            {
                (value.event_date, value.event_type)
                for value in ordered
                if value.event_kind == "ETF_ACTION" and value.event_date is not None
            },
            key=lambda item: (item[0], item[1] or ""),
        )
    )
    binary_overlap = any(hold.contains(event_date) for event_date, _ in binary_events)
    etf_overlap = any(hold.contains(event_date) for event_date, _ in etf_actions)
    def attested_coverage(kind: str) -> str:
        attestation = coverage_by_kind.get(kind)
        if attestation is None:
            return "UNKNOWN"
        if attestation.conflicts:
            return "CONFLICT"
        if not attestation.healthy or not attestation.complete:
            return "UNKNOWN"
        return attestation.coverage

    resolved_binary_coverage = (
        "OVERLAP" if binary_overlap else attested_coverage("BINARY_EVENT")
    )
    resolved_etf_coverage = (
        "OVERLAP" if etf_overlap else attested_coverage("ETF_ACTION")
    )
    coverage_missing = set(coverage_by_kind) != _EVENT_KINDS
    coverage_window_incomplete = any(
        attestation.coverage_start > hold.start
        or attestation.coverage_end < hold.end
        for attestation in coverage_by_kind.values()
    )
    product_coverage_invalid = (
        subject_kind == "STOCK"
        and (
            resolved_binary_coverage == "NOT_APPLICABLE"
            or resolved_etf_coverage != "NOT_APPLICABLE"
        )
    ) or (
        subject_kind == "ETF"
        and (
            resolved_binary_coverage != "NOT_APPLICABLE"
            or resolved_etf_coverage == "NOT_APPLICABLE"
        )
    )
    if future:
        block_reason = "EVIDENCE_TIMESTAMP_IN_FUTURE"
    elif stale:
        block_reason = "EVIDENCE_SOURCE_STALE"
    elif unhealthy:
        block_reason = "EVIDENCE_SOURCE_UNAVAILABLE"
    elif coverage_missing:
        block_reason = "EVIDENCE_COVERAGE_ATTESTATION_MISSING"
    elif coverage_window_incomplete:
        block_reason = "EVIDENCE_HOLD_COVERAGE_INCOMPLETE"
    elif product_coverage_invalid:
        block_reason = "EVIDENCE_PRODUCT_COVERAGE_INVALID"
    elif ambiguities:
        block_reason = "AMBIGUOUS_EVIDENCE_CLASSIFICATION"
    elif conflicts:
        block_reason = "EVIDENCE_SOURCE_CONFLICT"
    elif adverse:
        block_reason = "ADVERSE_EVENT"
    elif "CONFLICT" in (resolved_binary_coverage, resolved_etf_coverage):
        block_reason = "EVIDENCE_EVENT_COVERAGE_CONFLICT"
    elif resolved_binary_coverage == "UNKNOWN":
        block_reason = "BINARY_EVENT_STATUS_UNKNOWN"
    elif resolved_etf_coverage == "UNKNOWN":
        block_reason = "ETF_ACTION_STATUS_UNKNOWN"
    elif resolved_binary_coverage == "OVERLAP":
        block_reason = "BINARY_EVENT_DURING_HOLD"
    elif resolved_etf_coverage == "OVERLAP":
        block_reason = "ETF_ACTION_DURING_HOLD"
    else:
        block_reason = None
    qualifying = (
        tuple(value for value in ordered if value.event_type in qualifying_taxonomy)
        if block_reason is None
        else ()
    )
    decision = EvidenceDecision(
        subject_kind=subject_kind,
        symbol=subject_symbol,
        issuer_cik=subject_cik,
        as_of=current,
        qualifying_records=qualifying,
        adverse_tags=adverse,
        ambiguities=ambiguities,
        conflicts=conflicts,
        binary_events=binary_events,
        etf_actions=etf_actions,
        binary_event_coverage=resolved_binary_coverage,
        etf_action_coverage=resolved_etf_coverage,
        health=health,
        retrieved_at=retrieved_at,
        source_observation_ids=source_ids,
        block_reason=block_reason,
        registry_id=reviewed_bundle.registry_id,
        registry_content_hash=reviewed_bundle.content_hash,
    )
    decision_digest = _decision_fingerprint(decision)
    object.__setattr__(decision, "_authority", _REVIEWED_AUTHORITY)
    object.__setattr__(decision, "_decision_digest", decision_digest)
    object.__setattr__(decision, "_reviewed_bundle", reviewed_bundle)
    _remember_reviewed_authority(
        decision,
        kind="EVIDENCE_DECISION",
        digest=decision_digest,
        registry_id=reviewed_bundle.registry_id,
        release_pin=reviewed_bundle.content_hash,
    )
    return decision


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise EvidenceRegistryError("registry contains duplicate fields")
        result[name] = value
    return result


def _tuple_strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise EvidenceRegistryError(f"registry {name} is malformed")
    return tuple(value)


def _iso_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _decode_subject(value: object) -> tuple[str | None, str | None, str | None]:
    if value is None:
        return None, None, None
    if not isinstance(value, Mapping) or set(value) != {
        "issuer_cik",
        "subject_kind",
        "symbol",
    }:
        raise EvidenceRegistryError("registry evidence subject is malformed")
    subject_kind = value["subject_kind"]
    if subject_kind not in _SUBJECT_KINDS:
        raise EvidenceRegistryError("registry evidence subject kind is malformed")
    try:
        symbol, issuer_cik = _subject(value["symbol"], value["issuer_cik"])
    except (TypeError, ValueError):
        raise EvidenceRegistryError("registry evidence subject is malformed") from None
    if (subject_kind == "STOCK" and issuer_cik is None) or (
        subject_kind == "ETF" and issuer_cik is not None
    ):
        raise EvidenceRegistryError("registry evidence subject/CIK is malformed")
    return subject_kind, symbol, issuer_cik


def _subject_document(
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
) -> dict[str, object]:
    return {
        "issuer_cik": issuer_cik,
        "subject_kind": subject_kind,
        "symbol": symbol,
    }


def _decode_record(value: object) -> EvidenceRecord:
    if not isinstance(value, Mapping):
        raise EvidenceRegistryError("registry evidence record is malformed")
    expected = {
        "accession",
        "adverse_tags",
        "classification_ambiguous",
        "conflicts",
        "content_hash",
        "event_date",
        "event_kind",
        "event_type",
        "fact",
        "issuer_cik",
        "primary_url",
        "published_at",
        "publisher",
        "record_id",
        "retrieved_at",
        "source_observation_ids",
        "symbol",
    }
    if set(value) != expected:
        raise EvidenceRegistryError("registry evidence record fields are malformed")
    try:
        return EvidenceRecord(
            record_id=value["record_id"],
            symbol=value["symbol"],
            issuer_cik=value["issuer_cik"],
            primary_url=value["primary_url"],
            publisher=value["publisher"],
            published_at=_parse_timestamp(value["published_at"], "published_at"),
            retrieved_at=_parse_timestamp(value["retrieved_at"], "retrieved_at"),
            event_type=value["event_type"],
            fact=value["fact"],
            content_hash=value["content_hash"],
            source_observation_ids=_tuple_strings(
                value["source_observation_ids"],
                "source_observation_ids",
            ),
            accession=value["accession"],
            adverse_tags=_tuple_strings(value["adverse_tags"], "adverse_tags"),
            conflicts=_tuple_strings(value["conflicts"], "conflicts"),
            classification_ambiguous=value["classification_ambiguous"],
            event_date=_parse_date(value["event_date"], "event_date"),
            event_kind=value["event_kind"],
        )
    except (TypeError, ValueError, KeyError):
        raise EvidenceRegistryError("registry evidence record is malformed") from None


def _binding_fields(schema_version: int) -> set[str]:
    expected = {
        "accession",
        "content_hash",
        "checked_at",
        "healthy",
        "issuer_cik",
        "primary_url",
        "publisher",
        "retrieved_at",
        "source_observation_id",
        "source_role",
        "source_type",
        "symbol",
        "timestamp_source",
        "valid_until",
    }
    if schema_version == 3:
        expected.add("published_at")
    return expected


def _decode_binding(
    value: object,
    source_documents: Mapping[str, SourceDocument] | None,
    *,
    schema_version: int,
) -> EvidenceSourceBinding:
    if not isinstance(value, Mapping):
        raise EvidenceRegistryError("registry source binding is malformed")
    expected = _binding_fields(schema_version)
    if set(value) != expected:
        raise EvidenceRegistryError("registry source binding fields are malformed")
    try:
        identifier = value["source_observation_id"]
        if not isinstance(identifier, str) or source_documents is None:
            raise EvidenceRegistryError(
                "nonempty registry requires verified source documents"
            )
        document = source_documents.get(identifier)
        if not isinstance(document, SourceDocument):
            raise EvidenceRegistryError(
                "registry source document is missing or unverified"
            )
        retrieved_at = _parse_timestamp(value["retrieved_at"], "retrieved_at")
        published_at = (
            _parse_optional_timestamp(value["published_at"], "published_at")
            if schema_version == 3
            else document.published_at
        )
        if (
            document.source_observation_id != identifier
            or document.url != value["primary_url"]
            or document.publisher != value["publisher"]
            or document.content_hash != value["content_hash"]
            or document.retrieved_at != retrieved_at
            or document.published_at != published_at
            or document.source_type != value["source_type"]
            or document.timestamp_source != value["timestamp_source"]
            or document.accession != value["accession"]
            or document.source_role != value["source_role"]
        ):
            raise EvidenceRegistryError("registry source document metadata conflicts")
        return EvidenceSourceBinding.from_document(
            document,
            symbol=value["symbol"],
            issuer_cik=value["issuer_cik"],
            checked_at=_parse_timestamp(value["checked_at"], "checked_at"),
            valid_until=_parse_timestamp(value["valid_until"], "valid_until"),
            healthy=value["healthy"],
        )
    except (TypeError, ValueError, KeyError):
        raise EvidenceRegistryError("registry source binding is malformed") from None


def _decode_coverage(
    value: object,
    *,
    schema_version: int,
) -> EvidenceCoverageAttestation:
    expected = {
        "checked_at",
        "complete",
        "conflicts",
        "coverage",
        "coverage_kind",
        "healthy",
        "issuer_cik",
        "source_observation_ids",
        "subject_kind",
        "symbol",
        "valid_until",
    }
    if schema_version == 3:
        expected.update({"coverage_end", "coverage_start"})
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EvidenceRegistryError("registry evidence coverage is malformed")
    if schema_version != 3:
        raise EvidenceRegistryError(
            "legacy scoped coverage has no reviewed date range"
        )
    try:
        return EvidenceCoverageAttestation(
            subject_kind=value["subject_kind"],
            symbol=value["symbol"],
            issuer_cik=value["issuer_cik"],
            coverage_kind=value["coverage_kind"],
            coverage=value["coverage"],
            coverage_start=_parse_date(
                value["coverage_start"],
                "coverage_start",
            ),  # type: ignore[arg-type]
            coverage_end=_parse_date(
                value["coverage_end"],
                "coverage_end",
            ),  # type: ignore[arg-type]
            source_observation_ids=_tuple_strings(
                value["source_observation_ids"],
                "coverage source_observation_ids",
            ),
            checked_at=_parse_timestamp(value["checked_at"], "coverage checked_at"),
            valid_until=_parse_timestamp(
                value["valid_until"],
                "coverage valid_until",
            ),
            healthy=value["healthy"],
            complete=value["complete"],
            conflicts=_tuple_strings(value["conflicts"], "coverage conflicts"),
        )
    except (TypeError, ValueError, KeyError):
        raise EvidenceRegistryError("registry evidence coverage is malformed") from None


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


def _binding_authority_document(value: EvidenceSourceBinding) -> dict[str, object]:
    if type(value) is not EvidenceSourceBinding or type(value.document) is not SourceDocument:
        raise TypeError("reviewed evidence binding fields are malformed")
    document = value.document
    if (
        type(document.body) is not bytes
        or hashlib.sha256(document.body).hexdigest() != document.content_hash
        or type(value.healthy) is not bool
    ):
        raise ValueError("reviewed evidence binding bytes are malformed")
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


def _bundle_fingerprint(value: ReviewedEvidenceBundle) -> str:
    if (
        type(value) is not ReviewedEvidenceBundle
        or type(value.registry_id) is not str
        or _IDENTIFIER.fullmatch(value.registry_id) is None
        or type(value.reviewed_at) is not datetime
        or (
            value.subject_kind is not None
            and type(value.subject_kind) is not str
        )
        or (value.symbol is not None and type(value.symbol) is not str)
        or (value.issuer_cik is not None and type(value.issuer_cik) is not str)
        or type(value.records) is not tuple
        or any(type(record) is not EvidenceRecord for record in value.records)
        or type(value.source_bindings) is not tuple
        or any(
            type(binding) is not EvidenceSourceBinding
            for binding in value.source_bindings
        )
        or type(value.coverage_attestations) is not tuple
        or any(
            type(attestation) is not EvidenceCoverageAttestation
            for attestation in value.coverage_attestations
        )
        or type(value.content_hash) is not str
        or _SHA256.fullmatch(value.content_hash) is None
    ):
        raise TypeError("reviewed evidence bundle fields are malformed")
    document = {
        "content_hash": value.content_hash,
        "coverage_attestations": [
            _coverage_document(attestation)
            for attestation in value.coverage_attestations
        ],
        "records": [
            {**_record_document(record), "content_hash": record.content_hash}
            for record in value.records
        ],
        "registry_id": value.registry_id,
        "reviewed_at": _iso_timestamp(value.reviewed_at),
        "source_bindings": [
            _binding_authority_document(binding)
            for binding in value.source_bindings
        ],
        "subject": (
            None
            if value.subject_kind is None
            else _subject_document(
                value.subject_kind,
                value.symbol,  # type: ignore[arg-type]
                value.issuer_cik,
            )
        ),
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _is_reviewed_bundle(value: object) -> bool:
    if (
        type(value) is not ReviewedEvidenceBundle
        or value._authority is not _REVIEWED_AUTHORITY
        or type(value._release_pin) is not str
        or value._release_pin != value.content_hash
        or type(value._bundle_digest) is not str
        or _SHA256.fullmatch(value._bundle_digest) is None
    ):
        return False
    try:
        issuance = _reviewed_authority_issuance(
            value,
            kind="EVIDENCE_BUNDLE",
        )
        if issuance is None:
            return False
        if value._phase1_source is not None:
            from .journal import is_verified_phase1_signal_evidence_source

            if not is_verified_phase1_signal_evidence_source(
                value._phase1_source
            ):
                return False
        expected_digest, expected_registry_id, expected_release_pin = issuance
        return (
            value._bundle_digest == expected_digest
            and _bundle_fingerprint(value) == expected_digest
            and value.registry_id == expected_registry_id
            and value.content_hash == expected_release_pin
            and value._release_pin == expected_release_pin
        )
    except (TypeError, ValueError):
        return False


def _release_fingerprint(value: ReviewedEvidenceRelease) -> str:
    if (
        type(value) is not ReviewedEvidenceRelease
        or type(value.release_id) is not str
        or _IDENTIFIER.fullmatch(value.release_id) is None
        or type(value.release_sha256) is not str
        or _SHA256.fullmatch(value.release_sha256) is None
        or type(value.universe_sha256) is not str
        or _SHA256.fullmatch(value.universe_sha256) is None
        or type(value.reviewed_at) is not datetime
        or type(value.review_by) is not datetime
        or type(value.by_symbol) is not type(MappingProxyType({}))
        or any(
            type(symbol) is not str
            or type(bundle) is not ReviewedEvidenceBundle
            for symbol, bundle in value.by_symbol.items()
        )
    ):
        raise TypeError("reviewed evidence release fields are malformed")
    document = {
        "by_symbol": {
            symbol: _bundle_fingerprint(bundle)
            for symbol, bundle in value.by_symbol.items()
        },
        "release_id": value.release_id,
        "release_sha256": value.release_sha256,
        "review_by": _iso_timestamp(value.review_by),
        "reviewed_at": _iso_timestamp(value.reviewed_at),
        "universe_sha256": value.universe_sha256,
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _remember_reviewed_release(
    release: ReviewedEvidenceRelease,
    digest: str,
) -> None:
    identity = id(release)

    def discard(dead_reference: weakref.ReferenceType[object]) -> None:
        with _REVIEWED_RELEASE_ISSUANCE_LOCK:
            current = _REVIEWED_RELEASE_ISSUANCES.get(identity)
            if current is not None and current[0] is dead_reference:
                del _REVIEWED_RELEASE_ISSUANCES[identity]

    reference = weakref.ref(release, discard)
    with _REVIEWED_RELEASE_ISSUANCE_LOCK:
        _REVIEWED_RELEASE_ISSUANCES[identity] = (
            reference,
            digest,
            release.release_sha256,
        )


def is_verified_evidence_release(value: object) -> bool:
    """Return true only for an untampered release issued by its pinned loader."""
    if (
        type(value) is not ReviewedEvidenceRelease
        or value._authority is not _REVIEWED_RELEASE_AUTHORITY
        or type(value._release_digest) is not str
        or _SHA256.fullmatch(value._release_digest) is None
    ):
        return False
    with _REVIEWED_RELEASE_ISSUANCE_LOCK:
        issued = _REVIEWED_RELEASE_ISSUANCES.get(id(value))
        if issued is None or issued[0]() is not value:
            return False
        expected_digest, expected_sha = issued[1:]
    try:
        return (
            value._release_digest == expected_digest
            and value.release_sha256 == expected_sha
            and _release_fingerprint(value) == expected_digest
            and all(_is_reviewed_bundle(bundle) for bundle in value.by_symbol.values())
        )
    except (TypeError, ValueError):
        return False


def _verify_reviewed_source_bodies(
    records: Sequence[EvidenceRecord],
    coverage: Sequence[EvidenceCoverageAttestation],
    bindings: Mapping[str, EvidenceSourceBinding],
    *,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
) -> None:
    del subject_kind, symbol, issuer_cik
    record_sources = {
        identifier
        for record in records
        for identifier in record.source_observation_ids
    }
    coverage_sources = {
        identifier
        for attestation in coverage
        for identifier in attestation.source_observation_ids
    }
    if record_sources & coverage_sources:
        raise EvidenceRegistryError(
            "reviewed evidence facts and coverage require distinct observations"
        )
    for identifier, binding in bindings.items():
        document = binding.document
        if identifier not in record_sources | coverage_sources:
            raise EvidenceRegistryError(
                "reviewed evidence source observation is unreferenced"
            )
        if (
            type(document.body) is not bytes
            or not document.body
            or not hmac.compare_digest(
                hashlib.sha256(document.body).hexdigest(),
                document.content_hash,
            )
        ):
            raise EvidenceRegistryError(
                "reviewed evidence raw source bytes are corrupt"
            )


def _verify_record_source_roles(
    records: Sequence[EvidenceRecord],
    bindings: Mapping[str, EvidenceSourceBinding],
    *,
    subject_kind: str,
) -> None:
    for record in records:
        for identifier in record.source_observation_ids:
            document = bindings[identifier].document
            role = document.source_role
            if subject_kind == "STOCK":
                if document.source_type in {"SEC_ARCHIVE", "SEC_SUBMISSIONS"}:
                    continue
                if role in {
                    f"ISSUER_IR:{record.symbol}",
                    f"CORPORATE_ACTION:{record.symbol}",
                }:
                    continue
                raise EvidenceRegistryError(
                    "stock evidence does not use a subject-scoped primary source"
                )
            expected_role = (
                f"ISSUER_IR:{record.symbol}"
                if record.event_type == "fund sponsor notice"
                else f"CORPORATE_ACTION:{record.symbol}"
                if record.event_type == "index provider notice"
                else None
            )
            if expected_role is not None:
                if role != expected_role:
                    raise EvidenceRegistryError(
                        "ETF notice does not use its product-scoped official role"
                    )
            elif role not in {
                f"ISSUER_IR:{record.symbol}",
                f"CORPORATE_ACTION:{record.symbol}",
            }:
                raise EvidenceRegistryError(
                    "ETF evidence does not use a product-scoped official source"
                )


def _verify_coverage_source_roles(
    coverage: Sequence[EvidenceCoverageAttestation],
    bindings: Mapping[str, EvidenceSourceBinding],
) -> None:
    for attestation in coverage:
        for identifier in attestation.source_observation_ids:
            document = bindings[identifier].document
            if (
                document.source_type != "OFFICIAL_REFERENCE"
                or document.source_role not in _REFERENCE_ROLE_URLS
            ):
                raise EvidenceRegistryError(
                    "event coverage does not use a reviewed coverage-only role"
                )
            if document.timestamp_source == "UNAVAILABLE":
                relevant = (attestation.subject_kind, attestation.coverage_kind) in {
                    ("STOCK", "BINARY_EVENT"),
                    ("ETF", "ETF_ACTION"),
                }
                safe_relevant = (
                    relevant
                    and not attestation.complete
                    and attestation.coverage == "UNKNOWN"
                )
                safe_opposite = (
                    not relevant
                    and attestation.complete
                    and attestation.coverage == "NOT_APPLICABLE"
                )
                if document.source_role != "OPERATIONAL_STATUS" or not (
                    safe_relevant or safe_opposite
                ):
                    raise EvidenceRegistryError(
                        "timestamp-unavailable coverage has an unsafe state"
                    )


def _load_evidence_registry_payload(
    payload: bytes,
    *,
    expected_sha256: str,
    as_of: datetime,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> EvidenceRegistry:
    """Parse exact registry bytes without granting reviewed authority."""
    if not isinstance(expected_sha256, str) or not _SHA256.fullmatch(expected_sha256):
        raise EvidenceRegistryError("registry expected checksum is malformed")
    current = _utc(as_of, "evidence registry as_of")
    if type(payload) is not bytes or not payload or len(payload) > _MAX_REGISTRY_BYTES:
        raise EvidenceRegistryError("reviewed evidence registry size is invalid")
    digest = hashlib.sha256(payload).hexdigest()
    if digest != expected_sha256:
        raise EvidenceRegistryError("reviewed evidence registry checksum mismatch")
    try:
        document = json.loads(payload, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceRegistryError("reviewed evidence registry JSON is malformed") from None
    expected = {
        "coverage_attestations",
        "kind",
        "records",
        "registry_id",
        "reviewed_at",
        "schema_version",
        "source_bindings",
        "subject",
    }
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or type(document["schema_version"]) is not int
        or document["schema_version"] not in {2, 3}
        or document["kind"] != "REVIEWED_EVIDENCE_BUNDLE"
        or not isinstance(document["registry_id"], str)
        or not _IDENTIFIER.fullmatch(document["registry_id"])
        or not isinstance(document["records"], list)
        or not isinstance(document["source_bindings"], list)
        or not isinstance(document["coverage_attestations"], list)
    ):
        raise EvidenceRegistryError("reviewed evidence registry schema is invalid")
    schema_version = document["schema_version"]
    if schema_version == 2 and (
        document["subject"] is not None
        or document["records"]
        or document["source_bindings"]
        or document["coverage_attestations"]
    ):
        raise EvidenceRegistryError(
            "legacy schema-v2 registry must be an unscoped empty seed"
        )
    reviewed_at = _parse_timestamp(document["reviewed_at"], "reviewed_at")
    if reviewed_at > current:
        raise EvidenceRegistryError("reviewed evidence registry is from the future")
    subject_kind, symbol, issuer_cik = _decode_subject(document["subject"])
    records = tuple(_decode_record(value) for value in document["records"])
    bindings = tuple(
        _decode_binding(
            value,
            source_documents,
            schema_version=schema_version,
        )
        for value in document["source_bindings"]
    )
    coverage = tuple(
        _decode_coverage(value, schema_version=schema_version)
        for value in document["coverage_attestations"]
    )
    observation_times = tuple(value.retrieved_at for value in records) + tuple(
        timestamp
        for value in bindings
        for timestamp in (value.retrieved_at, value.checked_at)
    ) + tuple(value.checked_at for value in coverage)
    if any(timestamp > reviewed_at for timestamp in observation_times):
        raise EvidenceRegistryError(
            "reviewed evidence registry predates a source observation"
        )
    if subject_kind is None:
        if records or bindings or coverage:
            raise EvidenceRegistryError(
                "unscoped reviewed evidence registry must be an empty seed"
            )
    else:
        if (
            any(
                value.symbol != symbol or value.issuer_cik != issuer_cik
                for value in records
            )
            or any(
                value.symbol != symbol or value.issuer_cik != issuer_cik
                for value in bindings
            )
            or any(
                value.subject_kind != subject_kind
                or value.symbol != symbol
                or value.issuer_cik != issuer_cik
                for value in coverage
            )
        ):
            raise EvidenceRegistryError(
                "reviewed evidence registry mixes subject identities"
            )
        kinds = tuple(value.coverage_kind for value in coverage)
        if len(kinds) != len(set(kinds)) or set(kinds) != _EVENT_KINDS:
            raise EvidenceRegistryError(
                "reviewed evidence registry coverage is incomplete or duplicated"
            )
    record_ids = tuple(value.record_id for value in records)
    if len(record_ids) != len(set(record_ids)):
        raise EvidenceRegistryError("reviewed evidence registry has duplicate record IDs")
    try:
        by_id = _binding_map(bindings)
        if source_documents is not None and set(source_documents) != set(by_id):
            raise ValueError
        _verify_bindings(
            records,
            bindings,
            coverage,
            symbol=symbol or "",
            issuer_cik=issuer_cik,
        )
        if subject_kind is not None and symbol is not None:
            _verify_record_source_roles(
                records,
                by_id,
                subject_kind=subject_kind,
            )
            _verify_coverage_source_roles(coverage, by_id)
            _verify_reviewed_source_bodies(
                records,
                coverage,
                by_id,
                subject_kind=subject_kind,
                symbol=symbol,
                issuer_cik=issuer_cik,
            )
    except (EvidenceUnavailableError, TypeError, ValueError):
        raise EvidenceRegistryError("reviewed evidence provenance is invalid") from None
    return EvidenceRegistry(
        registry_id=document["registry_id"],
        reviewed_at=reviewed_at,
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        records=records,
        source_bindings=bindings,
        coverage_attestations=coverage,
        content_hash=digest,
    )


def load_evidence_registry(
    path: Path,
    *,
    expected_sha256: str,
    as_of: datetime,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> EvidenceRegistry:
    """Load one operator-reviewed immutable registry pinned by an external digest."""
    try:
        payload = Path(path).read_bytes()
    except OSError as error:
        raise EvidenceRegistryError(
            f"cannot read reviewed evidence registry: {type(error).__name__}"
        ) from None
    return _load_evidence_registry_payload(
        payload,
        expected_sha256=expected_sha256,
        as_of=as_of,
        source_documents=source_documents,
    )


def _read_open_regular_file(
    descriptor: int,
    *,
    maximum_bytes: int,
    name: str,
) -> bytes:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_size <= 0
        or before.st_size > maximum_bytes
    ):
        raise EvidenceRegistryError(f"{name} is not a confined regular file")
    chunks: list[bytes] = []
    remaining = before.st_size
    while remaining:
        chunk = os.read(descriptor, min(remaining, 1_048_576))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    after = os.fstat(descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        remaining
        or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
    ):
        raise EvidenceRegistryError(f"{name} changed while it was read")
    return b"".join(chunks)


def _read_regular_path(
    path: Path,
    *,
    maximum_bytes: int,
    name: str,
) -> bytes:
    candidate = Path(path)
    if candidate.name in {"", ".", ".."}:
        raise EvidenceRegistryError(f"{name} could not be read securely")
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | os.O_NOFOLLOW
        | os.O_DIRECTORY
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    parent_descriptor: int | None = None
    descriptor: int | None = None
    try:
        try:
            parent_descriptor = os.open(candidate.parent, directory_flags)
            descriptor = os.open(
                candidate.name,
                file_flags,
                dir_fd=parent_descriptor,
            )
            return _read_open_regular_file(
                descriptor,
                maximum_bytes=maximum_bytes,
                name=name,
            )
        except OSError:
            raise EvidenceRegistryError(
                f"{name} could not be read securely"
            ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if parent_descriptor is not None:
            os.close(parent_descriptor)


def _confined_parts(value: object, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or value.startswith("/")
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise EvidenceRegistryError(f"{name} is not a confined relative path")
    normalized = PurePosixPath(value)
    if normalized.is_absolute() or normalized.as_posix() != value:
        raise EvidenceRegistryError(f"{name} is not a canonical relative path")
    return normalized.parts


def _read_confined_regular_file(
    root: Path,
    relative: str,
    *,
    maximum_bytes: int,
    name: str,
) -> bytes:
    parts = _confined_parts(relative, name)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | os.O_NOFOLLOW
        | os.O_DIRECTORY
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    descriptors: list[int] = []
    try:
        current = os.open(root, directory_flags)
        descriptors.append(current)
        for component in parts[:-1]:
            current = os.open(
                component,
                directory_flags,
                dir_fd=current,
            )
            descriptors.append(current)
        descriptor = os.open(parts[-1], file_flags, dir_fd=current)
        descriptors.append(descriptor)
        return _read_open_regular_file(
            descriptor,
            maximum_bytes=maximum_bytes,
            name=name,
        )
    except EvidenceRegistryError:
        raise
    except OSError:
        raise EvidenceRegistryError(f"{name} could not be read securely") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _release_child_document(payload: bytes) -> dict[str, object]:
    try:
        document = json.loads(payload, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceRegistryError("reviewed evidence child JSON is malformed") from None
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != 3
        or not isinstance(document.get("source_bindings"), list)
    ):
        raise EvidenceRegistryError(
            "current evidence release requires schema-v3 subject children"
        )
    return document


def _release_binding_metadata(
    payload: bytes,
) -> tuple[Mapping[str, object], ...]:
    document = _release_child_document(payload)
    values: list[Mapping[str, object]] = []
    identifiers: set[str] = set()
    for value in document["source_bindings"]:  # type: ignore[union-attr]
        if not isinstance(value, Mapping) or set(value) != _binding_fields(3):
            raise EvidenceRegistryError("release child source binding is malformed")
        identifier = value.get("source_observation_id")
        if (
            not isinstance(identifier, str)
            or _IDENTIFIER.fullmatch(identifier) is None
            or identifier in identifiers
        ):
            raise EvidenceRegistryError(
                "release child source observation IDs are malformed"
            )
        identifiers.add(identifier)
        values.append(value)
    return tuple(values)


def _decode_source_artifact(
    payload: bytes,
    *,
    expected_sha256: str,
) -> bytes:
    try:
        envelope = json.loads(payload, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceRegistryError(
            "reviewed evidence source artifact JSON is malformed"
        ) from None
    if (
        not isinstance(envelope, dict)
        or set(envelope)
        != {
            "body",
            "content_sha256",
            "encoding",
            "kind",
            "schema_version",
        }
        or type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != 1
        or envelope["kind"] != "RAW_SOURCE_ARTIFACT"
        or envelope["encoding"] != "base64"
        or envelope["content_sha256"] != expected_sha256
        or not isinstance(envelope["body"], str)
        or not envelope["body"].isascii()
    ):
        raise EvidenceRegistryError(
            "reviewed evidence source artifact schema is invalid"
        )
    encoded = envelope["body"]
    try:
        body = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise EvidenceRegistryError(
            "reviewed evidence source artifact encoding is invalid"
        ) from None
    if (
        not body
        or len(body) > _MAX_SOURCE_BODY_BYTES
        or base64.b64encode(body).decode("ascii") != encoded
        or not hmac.compare_digest(
            hashlib.sha256(body).hexdigest(),
            expected_sha256,
        )
    ):
        raise EvidenceRegistryError(
            "reviewed evidence source artifact bytes are invalid"
        )
    return body


def _artifact_source_documents(
    evidence_root: Path,
    metadata: Sequence[Mapping[str, object]],
) -> Mapping[str, SourceDocument]:
    documents: dict[str, SourceDocument] = {}
    for value in metadata:
        identifier = value["source_observation_id"]
        content_hash = value["content_hash"]
        if (
            not isinstance(identifier, str)
            or not isinstance(content_hash, str)
            or _SHA256.fullmatch(content_hash) is None
        ):
            raise EvidenceRegistryError("release source artifact metadata is malformed")
        artifact_payload = _read_confined_regular_file(
            evidence_root,
            f"sources/{content_hash}.json",
            maximum_bytes=_MAX_SOURCE_ARTIFACT_BYTES,
            name="reviewed evidence source artifact",
        )
        body = _decode_source_artifact(
            artifact_payload,
            expected_sha256=content_hash,
        )
        try:
            document = SourceDocument(
                url=value["primary_url"],  # type: ignore[arg-type]
                published_at=_parse_optional_timestamp(
                    value["published_at"],
                    "published_at",
                ),
                retrieved_at=_parse_timestamp(
                    value["retrieved_at"],
                    "retrieved_at",
                ),
                content_hash=content_hash,
                body=body,
                source_observation_id=identifier,
                publisher=value["publisher"],  # type: ignore[arg-type]
                source_type=value["source_type"],  # type: ignore[arg-type]
                timestamp_source=value["timestamp_source"],  # type: ignore[arg-type]
                accession=value["accession"],  # type: ignore[arg-type]
                source_role=value["source_role"],  # type: ignore[arg-type]
            )
        except (TypeError, ValueError, KeyError):
            raise EvidenceRegistryError(
                "release source artifact metadata is malformed"
            ) from None
        documents[identifier] = document
    return MappingProxyType(documents)


def _verify_release_child_freshness(
    registry: EvidenceRegistry,
    *,
    reviewed_at: datetime,
    review_by: datetime,
    as_of: datetime,
) -> None:
    if (
        registry.reviewed_at > reviewed_at
        or reviewed_at - registry.reviewed_at > timedelta(hours=24)
        or as_of - registry.reviewed_at > timedelta(hours=24)
        or any(
            binding.retrieved_at > as_of
            or as_of - binding.retrieved_at > timedelta(hours=24)
            or review_by - binding.retrieved_at > timedelta(hours=24)
            or binding.checked_at > as_of
            or as_of > binding.valid_until
            or review_by > binding.valid_until
            for binding in registry.source_bindings
        )
        or any(
            attestation.checked_at > as_of
            or as_of - attestation.checked_at > timedelta(hours=24)
            or review_by - attestation.checked_at > timedelta(hours=24)
            or as_of > attestation.valid_until
            or review_by > attestation.valid_until
            for attestation in registry.coverage_attestations
        )
    ):
        raise EvidenceRegistryError(
            "reviewed evidence child is stale, expired, or from the future"
        )


def load_evidence_release(
    path: Path,
    *,
    expected_sha256: str,
    as_of: datetime,
    universe: UniverseSnapshot,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> ReviewedEvidenceRelease:
    """Load an exact universe-complete release and issue its reviewed bundles."""
    if (
        not isinstance(expected_sha256, str)
        or _SHA256.fullmatch(expected_sha256) is None
    ):
        raise EvidenceRegistryError("evidence release expected checksum is malformed")
    current = _utc(as_of, "evidence release as_of")
    if not is_verified_universe_snapshot(universe):
        raise EvidenceRegistryError("verified universe authority is required")
    if not (universe.effective_date <= current.date() <= universe.review_by):
        raise EvidenceRegistryError("evidence release as_of conflicts with universe")
    manifest_path = Path(path)
    payload = _read_regular_path(
        manifest_path,
        maximum_bytes=_MAX_RELEASE_BYTES,
        name="reviewed evidence release",
    )
    digest = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(digest, expected_sha256):
        raise EvidenceRegistryError("reviewed evidence release checksum mismatch")
    try:
        document = json.loads(payload, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceRegistryError("reviewed evidence release JSON is malformed") from None
    expected_fields = {
        "kind",
        "release_id",
        "review_by",
        "reviewed_at",
        "schema_version",
        "subjects",
        "universe_sha256",
    }
    if (
        not isinstance(document, dict)
        or set(document) != expected_fields
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["kind"] != "REVIEWED_EVIDENCE_RELEASE"
        or not isinstance(document["release_id"], str)
        or _IDENTIFIER.fullmatch(document["release_id"]) is None
        or not isinstance(document["subjects"], list)
        or not document["subjects"]
    ):
        raise EvidenceRegistryError("reviewed evidence release schema is invalid")
    reviewed_at = _parse_timestamp(document["reviewed_at"], "reviewed_at")
    review_by = _parse_timestamp(document["review_by"], "review_by")
    review_interval = review_by - reviewed_at
    if (
        not reviewed_at <= current < review_by
        or not timedelta(0) < review_interval <= timedelta(hours=24)
    ):
        raise EvidenceRegistryError("reviewed evidence release window is invalid")
    universe_sha = document["universe_sha256"]
    release_pin = universe._release_pin
    if (
        not isinstance(universe_sha, str)
        or _SHA256.fullmatch(universe_sha) is None
        or not isinstance(release_pin, str)
        or not hmac.compare_digest(universe_sha, release_pin)
    ):
        raise EvidenceRegistryError("evidence release universe checksum mismatch")
    supplied_documents: Mapping[str, SourceDocument] | None = source_documents
    if supplied_documents is not None and not isinstance(
        supplied_documents,
        Mapping,
    ):
        raise EvidenceRegistryError("release source documents are malformed")

    eligible = universe.eligible_records()
    expected_symbols = tuple(record.symbol for record in eligible)
    raw_subjects = document["subjects"]
    subjects: list[Mapping[str, object]] = []
    symbols: list[str] = []
    for raw_subject in raw_subjects:
        if not isinstance(raw_subject, Mapping) or set(raw_subject) != {
            "issuer_cik",
            "path",
            "sha256",
            "subject_kind",
            "symbol",
        }:
            raise EvidenceRegistryError("evidence release subject is malformed")
        symbol = raw_subject["symbol"]
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
            raise EvidenceRegistryError("evidence release subject symbol is malformed")
        if raw_subject["path"] != f"subjects/{symbol}.json":
            raise EvidenceRegistryError("evidence release child path is not canonical")
        _confined_parts(raw_subject["path"], "evidence release child path")
        if (
            not isinstance(raw_subject["sha256"], str)
            or _SHA256.fullmatch(raw_subject["sha256"]) is None
        ):
            raise EvidenceRegistryError("evidence release child checksum is malformed")
        symbols.append(symbol)
        subjects.append(raw_subject)
    if tuple(symbols) != expected_symbols or len(set(symbols)) != len(symbols):
        raise EvidenceRegistryError(
            "evidence release does not exactly cover the eligible universe"
        )

    by_symbol: dict[str, ReviewedEvidenceBundle] = {}
    used_observation_ids: set[str] = set()
    evidence_root = manifest_path.parent
    records_by_symbol = {record.symbol: record for record in eligible}
    for subject in subjects:
        symbol = subject["symbol"]
        assert isinstance(symbol, str)
        universe_record = records_by_symbol[symbol]
        expected_kind = (
            "STOCK" if universe_record.product_type == "common_stock" else "ETF"
        )
        if (
            subject["subject_kind"] != expected_kind
            or subject["issuer_cik"] != universe_record.issuer_cik
        ):
            raise EvidenceRegistryError(
                "evidence release subject conflicts with the universe"
            )
        child_path = subject["path"]
        child_sha = subject["sha256"]
        assert isinstance(child_path, str) and isinstance(child_sha, str)
        child_payload = _read_confined_regular_file(
            evidence_root,
            child_path,
            maximum_bytes=_MAX_REGISTRY_BYTES,
            name="reviewed evidence child",
        )
        if not hmac.compare_digest(
            hashlib.sha256(child_payload).hexdigest(),
            child_sha,
        ):
            raise EvidenceRegistryError("reviewed evidence child checksum mismatch")
        metadata = _release_binding_metadata(child_payload)
        identifiers = {
            value["source_observation_id"]
            for value in metadata
            if isinstance(value["source_observation_id"], str)
        }
        if used_observation_ids & identifiers:
            raise EvidenceRegistryError(
                "source observation ID is reused across release subjects"
            )
        used_observation_ids.update(identifiers)
        if supplied_documents is None:
            child_documents = _artifact_source_documents(
                evidence_root,
                metadata,
            )
        else:
            child_documents = {
                identifier: supplied_documents[identifier]
                for identifier in identifiers
                if identifier in supplied_documents
            }
        registry = _load_evidence_registry_payload(
            child_payload,
            expected_sha256=child_sha,
            as_of=current,
            source_documents=child_documents,
        )
        if (
            registry.subject_kind != expected_kind
            or registry.symbol != symbol
            or registry.issuer_cik != universe_record.issuer_cik
        ):
            raise EvidenceRegistryError(
                "reviewed evidence child subject conflicts with its manifest"
            )
        _verify_release_child_freshness(
            registry,
            reviewed_at=reviewed_at,
            review_by=review_by,
            as_of=current,
        )
        by_symbol[symbol] = _issue_reviewed_evidence_bundle(
            registry,
            release_pin=child_sha,
        )
    if supplied_documents is not None and set(supplied_documents) != used_observation_ids:
        raise EvidenceRegistryError(
            "release source documents do not exactly partition across subjects"
        )
    release = ReviewedEvidenceRelease(
        release_id=document["release_id"],
        release_sha256=digest,
        universe_sha256=universe_sha,
        reviewed_at=reviewed_at,
        review_by=review_by,
        by_symbol=MappingProxyType(by_symbol),
    )
    release_digest = _release_fingerprint(release)
    object.__setattr__(release, "_authority", _REVIEWED_RELEASE_AUTHORITY)
    object.__setattr__(release, "_release_digest", release_digest)
    _remember_reviewed_release(release, release_digest)
    return release


def load_current_evidence_release(
    project_root: Path,
    *,
    as_of: datetime,
    universe: UniverseSnapshot,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> ReviewedEvidenceRelease:
    """Resolve only the externally pinned current multi-subject release."""
    return load_evidence_release(
        Path(project_root) / "data" / "evidence" / "current.json",
        expected_sha256=CURRENT_EVIDENCE_RELEASE_SHA256,
        as_of=as_of,
        universe=universe,
        source_documents=source_documents,
    )


def _issue_reviewed_evidence_bundle(
    registry: EvidenceRegistry,
    *,
    release_pin: str,
    phase1_source: object | None = None,
) -> ReviewedEvidenceBundle:
    if (
        not isinstance(registry, EvidenceRegistry)
        or type(release_pin) is not str
        or _SHA256.fullmatch(release_pin) is None
        or registry.content_hash != release_pin
    ):
        raise EvidenceRegistryError("reviewed evidence release pin is invalid")
    bundle = ReviewedEvidenceBundle(
        registry_id=registry.registry_id,
        reviewed_at=registry.reviewed_at,
        subject_kind=registry.subject_kind,
        symbol=registry.symbol,
        issuer_cik=registry.issuer_cik,
        records=registry.records,
        source_bindings=registry.source_bindings,
        coverage_attestations=registry.coverage_attestations,
        content_hash=registry.content_hash,
    )
    bundle_digest = _bundle_fingerprint(bundle)
    object.__setattr__(bundle, "_authority", _REVIEWED_AUTHORITY)
    object.__setattr__(bundle, "_release_pin", release_pin)
    object.__setattr__(bundle, "_bundle_digest", bundle_digest)
    object.__setattr__(bundle, "_phase1_source", phase1_source)
    _remember_reviewed_authority(
        bundle,
        kind="EVIDENCE_BUNDLE",
        digest=bundle_digest,
        registry_id=bundle.registry_id,
        release_pin=release_pin,
    )
    return bundle


def _issue_reviewed_bundle_from_phase1_source(
    source: object,
) -> ReviewedEvidenceBundle:
    """Reissue exact reviewed bytes only from an owner-current Journal source."""
    from .journal import (
        Phase1SignalEvidenceSource,
        is_verified_phase1_signal_evidence_source,
    )

    if not isinstance(source, Phase1SignalEvidenceSource) or not (
        is_verified_phase1_signal_evidence_source(source)
    ):
        raise EvidenceUnavailableError(
            "verified Phase 1 signal evidence source is required"
        )
    if source.reviewed_bundle is not None or source.evidence_decision is not None:
        bundle = source.reviewed_bundle
        decision = source.evidence_decision
        if (
            not _is_reviewed_bundle(bundle)
            or not is_reviewed_evidence_decision(decision)
            or decision._reviewed_bundle is not bundle
            or bundle.content_hash != source.release_sha256
            or bundle.registry_id != source.registry_id
            or bundle._bundle_digest != source.bundle_digest
        ):
            raise EvidenceUnavailableError(
                "Phase 1 reviewed evidence source authority is inconsistent"
            )
        return bundle
    if (source.reviewed_bundle is None) != (source.evidence_decision is None):
        raise EvidenceUnavailableError(
            "Phase 1 reviewed evidence source authority is incomplete"
        )
    documents = tuple(source.source_documents)
    if (
        type(source.registry_payload) is not bytes
        or type(source.release_sha256) is not str
        or source.release_sha256 != source.registry_content_hash
        or hashlib.sha256(source.registry_payload).hexdigest()
        != source.release_sha256
        or not documents
        or any(type(document) is not SourceDocument for document in documents)
        or tuple(document.source_observation_id for document in documents)
        != tuple(
            sorted(document.source_observation_id for document in documents)
        )
        or len({document.source_observation_id for document in documents})
        != len(documents)
        or source.review_at > source.query_cutoff
    ):
        raise EvidenceRegistryError(
            "Phase 1 reviewed evidence raw material is inconsistent"
        )
    registry = _load_evidence_registry_payload(
        source.registry_payload,
        expected_sha256=source.release_sha256,
        as_of=source.review_at,
        source_documents={
            document.source_observation_id: document for document in documents
        },
    )
    bundle = _issue_reviewed_evidence_bundle(
        registry,
        release_pin=source.release_sha256,
        phase1_source=source,
    )
    if (
        bundle.registry_id != source.registry_id
        or bundle.content_hash != source.registry_content_hash
        or bundle._bundle_digest != source.bundle_digest
    ):
        raise EvidenceRegistryError(
            "Phase 1 reviewed evidence source manifest is inconsistent"
        )
    return bundle


def load_current_evidence_bundle(
    project_root: Path,
    *,
    as_of: datetime,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> ReviewedEvidenceBundle:
    """Resolve the pinned legacy subjectless seed for replay compatibility."""
    root = Path(project_root)
    registry = load_evidence_registry(
        root / "data" / "evidence" / "legacy" / "subjectless.json",
        expected_sha256=CURRENT_EVIDENCE_REGISTRY_SHA256,
        as_of=as_of,
        source_documents=source_documents,
    )
    return _issue_reviewed_evidence_bundle(
        registry,
        release_pin=CURRENT_EVIDENCE_REGISTRY_SHA256,
    )


__all__ = [
    "ADVERSE_TAGS",
    "CURRENT_EVIDENCE_RELEASE_SHA256",
    "CURRENT_EVIDENCE_REGISTRY_SHA256",
    "ETF_POSITIVE_EVENT_TYPES",
    "POSITIVE_EVENT_TYPES",
    "DateRange",
    "EvidenceDecision",
    "EvidenceCoverageAttestation",
    "EvidenceRecord",
    "EvidenceRegistry",
    "EvidenceRegistryError",
    "EvidenceSourceBinding",
    "EvidenceUnavailableError",
    "ReviewedEvidenceBundle",
    "ReviewedEvidenceRelease",
    "classify_evidence",
    "is_reviewed_evidence_decision",
    "is_verified_evidence_release",
    "load_current_evidence_bundle",
    "load_current_evidence_release",
    "load_evidence_release",
    "load_evidence_registry",
]
