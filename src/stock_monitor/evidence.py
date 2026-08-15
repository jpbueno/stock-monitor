"""Reviewed primary-source evidence and fail-closed classification decisions."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from stock_monitor.domain import require_aware_timestamp
from stock_monitor.providers.cache import SourceDocument


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
_REVIEWED_ISSUANCE_LOCK = threading.Lock()
_REVIEWED_ISSUANCES: dict[
    int,
    tuple[weakref.ReferenceType[object], str, str, str, str],
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
        if (
            document.url != expected_url
            or document.publisher != expected_publisher
            or document.timestamp_source != "PRIMARY_METADATA"
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


def _decode_binding(
    value: object,
    source_documents: Mapping[str, SourceDocument] | None,
) -> EvidenceSourceBinding:
    if not isinstance(value, Mapping):
        raise EvidenceRegistryError("registry source binding is malformed")
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
        if (
            document.source_observation_id != identifier
            or document.url != value["primary_url"]
            or document.publisher != value["publisher"]
            or document.content_hash != value["content_hash"]
            or document.retrieved_at != retrieved_at
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


def _decode_coverage(value: object) -> EvidenceCoverageAttestation:
    if not isinstance(value, Mapping) or set(value) != {
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
    }:
        raise EvidenceRegistryError("registry evidence coverage is malformed")
    try:
        return EvidenceCoverageAttestation(
            subject_kind=value["subject_kind"],
            symbol=value["symbol"],
            issuer_cik=value["issuer_cik"],
            coverage_kind=value["coverage_kind"],
            coverage=value["coverage"],
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
        "coverage_kind": value.coverage_kind,
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


def _decode_reviewed_source_body(document: SourceDocument) -> dict[str, object]:
    try:
        payload = json.loads(document.body, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceRegistryError("reviewed evidence source body is malformed") from None
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise EvidenceRegistryError("reviewed evidence source schema is invalid")
    return payload


def _verify_reviewed_source_bodies(
    records: Sequence[EvidenceRecord],
    coverage: Sequence[EvidenceCoverageAttestation],
    bindings: Mapping[str, EvidenceSourceBinding],
    *,
    subject_kind: str,
    symbol: str,
    issuer_cik: str | None,
) -> None:
    expected_subject = _subject_document(subject_kind, symbol, issuer_cik)
    records_by_source: dict[str, list[EvidenceRecord]] = {}
    coverage_by_source: dict[str, list[EvidenceCoverageAttestation]] = {}
    for record in records:
        for identifier in record.source_observation_ids:
            records_by_source.setdefault(identifier, []).append(record)
    for attestation in coverage:
        for identifier in attestation.source_observation_ids:
            coverage_by_source.setdefault(identifier, []).append(attestation)
    if set(records_by_source) & set(coverage_by_source):
        raise EvidenceRegistryError(
            "reviewed evidence facts and coverage require distinct observations"
        )
    for identifier, binding in bindings.items():
        payload = _decode_reviewed_source_body(binding.document)
        common = {
            "schema_version": 1,
            "source_observation_id": identifier,
            "subject": expected_subject,
        }
        if identifier in records_by_source:
            expected = {
                **common,
                "kind": "REVIEWED_PRIMARY_EVIDENCE",
                "records": [
                    _record_document(record)
                    for record in sorted(
                        records_by_source[identifier],
                        key=lambda item: item.record_id,
                    )
                ],
            }
        elif identifier in coverage_by_source:
            expected = {
                **common,
                "attestations": [
                    _coverage_document(attestation)
                    for attestation in sorted(
                        coverage_by_source[identifier],
                        key=lambda item: item.coverage_kind,
                    )
                ],
                "kind": "REVIEWED_EVIDENCE_COVERAGE",
            }
        else:
            raise EvidenceRegistryError(
                "reviewed evidence source observation is unreferenced"
            )
        if payload != expected:
            raise EvidenceRegistryError(
                "reviewed evidence source bytes contradict the registry"
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


def load_evidence_registry(
    path: Path,
    *,
    expected_sha256: str,
    as_of: datetime,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> EvidenceRegistry:
    """Load one operator-reviewed immutable registry pinned by an external digest."""
    if not isinstance(expected_sha256, str) or not _SHA256.fullmatch(expected_sha256):
        raise EvidenceRegistryError("registry expected checksum is malformed")
    current = _utc(as_of, "evidence registry as_of")
    try:
        payload = Path(path).read_bytes()
    except OSError as error:
        raise EvidenceRegistryError(
            f"cannot read reviewed evidence registry: {type(error).__name__}"
        ) from None
    if not payload or len(payload) > _MAX_REGISTRY_BYTES:
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
        or document["schema_version"] != 2
        or document["kind"] != "REVIEWED_EVIDENCE_BUNDLE"
        or not isinstance(document["registry_id"], str)
        or not _IDENTIFIER.fullmatch(document["registry_id"])
        or not isinstance(document["records"], list)
        or not isinstance(document["source_bindings"], list)
        or not isinstance(document["coverage_attestations"], list)
    ):
        raise EvidenceRegistryError("reviewed evidence registry schema is invalid")
    reviewed_at = _parse_timestamp(document["reviewed_at"], "reviewed_at")
    if reviewed_at > current:
        raise EvidenceRegistryError("reviewed evidence registry is from the future")
    subject_kind, symbol, issuer_cik = _decode_subject(document["subject"])
    records = tuple(_decode_record(value) for value in document["records"])
    bindings = tuple(
        _decode_binding(value, source_documents)
        for value in document["source_bindings"]
    )
    coverage = tuple(
        _decode_coverage(value) for value in document["coverage_attestations"]
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


def load_current_evidence_bundle(
    project_root: Path,
    *,
    as_of: datetime,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> ReviewedEvidenceBundle:
    """Resolve only the release-pinned current reviewed evidence registry."""
    root = Path(project_root)
    registry = load_evidence_registry(
        root / "data" / "evidence" / "current.json",
        expected_sha256=CURRENT_EVIDENCE_REGISTRY_SHA256,
        as_of=as_of,
        source_documents=source_documents,
    )
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
    release_pin = CURRENT_EVIDENCE_REGISTRY_SHA256
    object.__setattr__(bundle, "_authority", _REVIEWED_AUTHORITY)
    object.__setattr__(bundle, "_release_pin", release_pin)
    object.__setattr__(bundle, "_bundle_digest", bundle_digest)
    _remember_reviewed_authority(
        bundle,
        kind="EVIDENCE_BUNDLE",
        digest=bundle_digest,
        registry_id=bundle.registry_id,
        release_pin=release_pin,
    )
    return bundle


__all__ = [
    "ADVERSE_TAGS",
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
    "classify_evidence",
    "is_reviewed_evidence_decision",
    "load_current_evidence_bundle",
    "load_evidence_registry",
]
