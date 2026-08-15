"""Exact-URL, role-bound official reference retrieval and status decisions."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import weakref
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from stock_monitor.domain import require_aware_timestamp

from .cache import (
    ContentCache,
    SourceDocument,
    SourceHealthAttestation,
    SourceObservation,
    _issue_provider_health_attestation,
)
from .http import EgressPolicy, GetTransport, NetworkPolicyError, get_with_redirects


_BASE_ROLES = frozenset(
    {
        "CROSS_CHECK_CALENDAR",
        "OPERATIONAL_STATUS",
        "PRIMARY_CALENDAR",
        "PRIMARY_HALT_FEED",
        "TRADER_ALERT_HALT",
    }
)
_SCOPED_ROLE = re.compile(r"(?:ISSUER_IR|CORPORATE_ACTION):[A-Z][A-Z0-9.-]{0,14}\Z")
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}\Z")
_VENUE = re.compile(r"[A-Z][A-Z0-9._-]{0,31}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_STATUS_ROLES = {
    "primary": "PRIMARY_CALENDAR",
    "cross_check": "CROSS_CHECK_CALENDAR",
    "operational_status": "OPERATIONAL_STATUS",
    "trader_alert": "TRADER_ALERT_HALT",
}
_HALT_KEYS = (
    "primary_halt_feed",
    "operational_status",
    "cross_check_halt_feed",
)
_HALT_COVERAGE = frozenset({"COMPLETE_ACTIVE_HALTS", "PARTIAL", "UNKNOWN"})
_OFFICIAL_ROLE_URLS = {
    "CROSS_CHECK_CALENDAR": "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
    "OPERATIONAL_STATUS": (
        "https://www.nyse.com/api/notifications/public/alerts?2=3"
    ),
    "PRIMARY_CALENDAR": "https://www.nyse.com/trade/hours-calendars",
    "PRIMARY_HALT_FEED": (
        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
    ),
    "TRADER_ALERT_HALT": (
        "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines"
    ),
}
_STATUS_VALUES = {
    "CROSS_CHECK_CALENDAR": frozenset(
        {"CLOSED", "OPEN", "UNKNOWN", "UNSUPPORTED"}
    ),
    "OPERATIONAL_STATUS": frozenset(
        {
            "EMERGENCY_CLOSED",
            "EMERGENCY_CLOSURE",
            "HALTED",
            "NORMAL",
            "OPEN",
            "OPERATIONAL",
            "UNKNOWN",
            "UNSUPPORTED",
        }
    ),
    "PRIMARY_CALENDAR": frozenset(
        {"CLOSED", "OPEN", "UNKNOWN", "UNSUPPORTED"}
    ),
    "PRIMARY_HALT_FEED": frozenset({"UNSUPPORTED"}),
    "TRADER_ALERT_HALT": frozenset(
        {
            "CLEAR",
            "EMERGENCY_CLOSED",
            "HALTED",
            "NONE",
            "NO_ALERT",
            "UNKNOWN",
            "UNSUPPORTED",
        }
    ),
}
_HALT_EXPECTATIONS = {
    "primary_halt_feed": (
        "PRIMARY_HALT_FEED",
        _OFFICIAL_ROLE_URLS["PRIMARY_HALT_FEED"],
        "ACTIVE_HALTS",
    ),
    "operational_status": (
        "OPERATIONAL_STATUS",
        _OFFICIAL_ROLE_URLS["OPERATIONAL_STATUS"],
        "EXCHANGE_OPERATIONAL_STATUS",
    ),
    "cross_check_halt_feed": (
        "TRADER_ALERT_HALT",
        _OFFICIAL_ROLE_URLS["TRADER_ALERT_HALT"],
        "ACTIVE_HALTS",
    ),
}
_REFERENCE_SNAPSHOT_AUTHORITY = object()
_ISSUED_SNAPSHOT_LOCK = threading.Lock()
_ISSUED_REFERENCE_AUTHORITIES: dict[
    int,
    tuple[weakref.ReferenceType[object], str, str, tuple[str, ...]],
] = {}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_NDAQ_NAMESPACE = "http://www.nasdaqtrader.com/"
_NDAQ_NUM_ITEMS_TAG = "{http://www.nasdaqtrader.com/}numItems"
_NDAQ_ISSUE_SYMBOL_TAG = "{http://www.nasdaqtrader.com/}IssueSymbol"
_TRADER_ALERT_ITEM_TAGS = frozenset(
    {
        "title",
        "pubDate",
        "link",
        "description",
        f"{{{_NDAQ_NAMESPACE}}}NewsCategory",
        f"{{{_NDAQ_NAMESPACE}}}Alert",
        f"{{{_NDAQ_NAMESPACE}}}Markets",
        f"{{{_NDAQ_NAMESPACE}}}WhatYouNeedToKnow",
    }
)
_DECLARED_ITEM_COUNT = re.compile(r"(?:0|[1-9][0-9]{0,4})\Z")


def _utc(value: datetime, name: str) -> datetime:
    return require_aware_timestamp(value, name).astimezone(UTC)


def _role(value: object) -> str:
    if not isinstance(value, str) or (
        value not in _BASE_ROLES and not _SCOPED_ROLE.fullmatch(value)
    ):
        raise ValueError("reference source role is unsupported")
    return value


def _publisher(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or len(value) > 256
        or not value.isascii()
        or not value.isprintable()
    ):
        raise ValueError("reference publisher is malformed")
    return value


def _origin(url: str) -> str:
    parsed = urlsplit(url)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    return f"https://{hostname}"


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("reference document contains duplicate fields")
        result[name] = value
    return result


def _document_payload(document: SourceDocument, kind: str) -> dict[str, object]:
    try:
        import json

        payload = json.loads(document.body, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("reference document body is malformed") from None
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("reference document schema is unsupported")
    if payload.get("kind") != kind:
        raise ValueError("reference document kind is inconsistent")
    return payload


def _document_time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"reference document {name} is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _utc(parsed, f"reference document {name}")
    except ValueError:
        raise ValueError(f"reference document {name} is malformed") from None


def _require_official_role_url(role: str, url: str) -> None:
    expected = _OFFICIAL_ROLE_URLS.get(role)
    if expected is None or url != expected:
        raise ValueError("reference role does not use its exact reviewed official URL")


def _blocked_exchange(
    reason: str,
    *,
    as_of: datetime,
    identifiers: Iterable[str] = (),
    retrieved_at: datetime | None = None,
    valid_until: datetime | None = None,
) -> ExchangeStatusDecision:
    return _issue_exchange_status_decision(
        status="BLOCKED",
        block_reason=reason,
        as_of=as_of,
        valid_until=valid_until,
        source_observation_ids=tuple(sorted(set(identifiers))),
        retrieved_at=retrieved_at,
    )


def _canonical_digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()


def _remember_reference_authority(
    value: object,
    *,
    kind: str,
    digest: str,
    source_pin: tuple[str, ...],
) -> None:
    object_id = id(value)

    def forget(reference: weakref.ReferenceType[object]) -> None:
        with _ISSUED_SNAPSHOT_LOCK:
            current = _ISSUED_REFERENCE_AUTHORITIES.get(object_id)
            if current is not None and current[0] is reference:
                _ISSUED_REFERENCE_AUTHORITIES.pop(object_id, None)

    reference = weakref.ref(value, forget)
    with _ISSUED_SNAPSHOT_LOCK:
        _ISSUED_REFERENCE_AUTHORITIES[object_id] = (
            reference,
            kind,
            digest,
            source_pin,
        )


def _reference_authority_issuance(
    value: object,
    *,
    kind: str,
) -> tuple[str, tuple[str, ...]] | None:
    with _ISSUED_SNAPSHOT_LOCK:
        issuance = _ISSUED_REFERENCE_AUTHORITIES.get(id(value))
        if issuance is None or issuance[0]() is not value or issuance[1] != kind:
            return None
        return issuance[2], issuance[3]


def _document_fingerprint(document: SourceDocument) -> dict[str, object]:
    if type(document) is not SourceDocument:
        raise TypeError("reference snapshot document has the wrong type")
    if hashlib.sha256(document.body).hexdigest() != document.content_hash:
        raise ValueError("reference snapshot document bytes are not hash-bound")
    return {
        "url": document.url,
        "published_at": (
            document.published_at.isoformat(timespec="microseconds")
            if document.published_at is not None
            else None
        ),
        "retrieved_at": document.retrieved_at.isoformat(timespec="microseconds"),
        "content_hash": document.content_hash,
        "body_sha256": hashlib.sha256(document.body).hexdigest(),
        "source_observation_id": document.source_observation_id,
        "publisher": document.publisher,
        "source_type": document.source_type,
        "timestamp_source": document.timestamp_source,
        "accession": document.accession,
        "source_role": document.source_role,
    }


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ReferenceStatusSnapshot:
    role: str
    status: str
    document: SourceDocument
    healthy: bool
    supported: bool = True
    _authority: object = field(init=False, repr=False, compare=False)
    _snapshot_digest: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "role", _role(self.role))
        if (
            not isinstance(self.status, str)
            or not self.status
            or len(self.status) > 64
            or not self.status.isascii()
            or not self.status.isprintable()
        ):
            raise ValueError("reference status is malformed")
        normalized = self.status.upper()
        if normalized not in _STATUS_VALUES[self.role]:
            raise ValueError("reference status is unsupported for its role")
        object.__setattr__(self, "status", normalized)
        if type(self.document) is not SourceDocument:
            raise TypeError("reference status requires an exact source document")
        if (
            self.document.source_type != "OFFICIAL_REFERENCE"
            or self.document.source_role != self.role
        ):
            raise ValueError("reference status is not bound to its configured role")
        _require_official_role_url(self.role, self.document.url)
        if type(self.healthy) is not bool or type(self.supported) is not bool:
            raise TypeError("reference status health and support must be explicit")
        if not self.supported and (self.status != "UNSUPPORTED" or self.healthy):
            raise ValueError("unsupported reference status cannot claim health")
        _document_fingerprint(self.document)


def _status_fingerprint(value: ReferenceStatusSnapshot) -> str:
    if (
        type(value) is not ReferenceStatusSnapshot
        or type(value.role) is not str
        or type(value.status) is not str
        or type(value.healthy) is not bool
        or type(value.supported) is not bool
    ):
        raise TypeError("reference status snapshot has the wrong type")
    return _canonical_digest(
        {
            "kind": "REFERENCE_STATUS_SNAPSHOT",
            "role": value.role,
            "status": value.status,
            "healthy": value.healthy,
            "supported": value.supported,
            "document": _document_fingerprint(value.document),
        }
    )


def _issue_status_snapshot(
    document: SourceDocument,
    *,
    status: str,
    healthy: bool,
    supported: bool,
) -> ReferenceStatusSnapshot:
    if document.source_role is None:
        raise ValueError("reference status document is missing its role")
    value = ReferenceStatusSnapshot(
        role=document.source_role,
        status=status,
        document=document,
        healthy=healthy,
        supported=supported,
    )
    digest = _status_fingerprint(value)
    object.__setattr__(value, "_authority", _REFERENCE_SNAPSHOT_AUTHORITY)
    object.__setattr__(value, "_snapshot_digest", digest)
    _remember_reference_authority(
        value,
        kind="REFERENCE_STATUS_SNAPSHOT",
        digest=digest,
        source_pin=(
            document.source_observation_id,
            document.content_hash,
            document.url,
            document.source_role,
        ),
    )
    return value


def _is_verified_status_snapshot(value: object) -> bool:
    if type(value) is not ReferenceStatusSnapshot:
        return False
    try:
        issuance = _reference_authority_issuance(
            value,
            kind="REFERENCE_STATUS_SNAPSHOT",
        )
        if issuance is None:
            return False
        expected_digest, expected_source_pin = issuance
        return (
            value._authority is _REFERENCE_SNAPSHOT_AUTHORITY
            and _SHA256.fullmatch(value._snapshot_digest) is not None
            and value._snapshot_digest == expected_digest
            and _status_fingerprint(value) == expected_digest
            and (
                value.document.source_observation_id,
                value.document.content_hash,
                value.document.url,
                value.document.source_role,
            )
            == expected_source_pin
        )
    except (AttributeError, TypeError, ValueError):
        return False


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExchangeStatusDecision:
    status: str
    block_reason: str | None
    as_of: datetime
    valid_until: datetime | None
    source_observation_ids: tuple[str, ...]
    retrieved_at: datetime | None
    _authority: object = field(init=False, repr=False, compare=False)
    _decision_digest: str = field(init=False, repr=False, compare=False)


def _exchange_decision_fingerprint(value: ExchangeStatusDecision) -> str:
    if (
        type(value) is not ExchangeStatusDecision
        or type(value.status) is not str
        or (value.block_reason is not None and type(value.block_reason) is not str)
        or type(value.as_of) is not datetime
        or (value.valid_until is not None and type(value.valid_until) is not datetime)
        or type(value.source_observation_ids) is not tuple
        or any(type(item) is not str for item in value.source_observation_ids)
        or (value.retrieved_at is not None and type(value.retrieved_at) is not datetime)
    ):
        raise TypeError("exchange status decision fields are malformed")
    return _canonical_digest(
        {
            "kind": "EXCHANGE_STATUS_DECISION",
            "status": value.status,
            "block_reason": value.block_reason,
            "as_of": value.as_of.isoformat(timespec="microseconds"),
            "valid_until": (
                value.valid_until.isoformat(timespec="microseconds")
                if value.valid_until is not None
                else None
            ),
            "source_observation_ids": list(value.source_observation_ids),
            "retrieved_at": (
                value.retrieved_at.isoformat(timespec="microseconds")
                if value.retrieved_at is not None
                else None
            ),
        }
    )


def _issue_exchange_status_decision(
    *,
    status: str,
    block_reason: str | None,
    as_of: datetime,
    valid_until: datetime | None,
    source_observation_ids: tuple[str, ...],
    retrieved_at: datetime | None,
) -> ExchangeStatusDecision:
    value = ExchangeStatusDecision(
        status=status,
        block_reason=block_reason,
        as_of=as_of,
        valid_until=valid_until,
        source_observation_ids=source_observation_ids,
        retrieved_at=retrieved_at,
    )
    digest = _exchange_decision_fingerprint(value)
    object.__setattr__(value, "_authority", _REFERENCE_SNAPSHOT_AUTHORITY)
    object.__setattr__(value, "_decision_digest", digest)
    _remember_reference_authority(
        value,
        kind="EXCHANGE_STATUS_DECISION",
        digest=digest,
        source_pin=source_observation_ids,
    )
    return value


def is_reviewed_exchange_status_decision(value: object) -> bool:
    """Return true only for untampered output from exchange-status verification."""
    if type(value) is not ExchangeStatusDecision:
        return False
    try:
        issuance = _reference_authority_issuance(
            value,
            kind="EXCHANGE_STATUS_DECISION",
        )
        if issuance is None:
            return False
        expected_digest, expected_source_pin = issuance
        return (
            value._authority is _REFERENCE_SNAPSHOT_AUTHORITY
            and value._decision_digest == expected_digest
            and _exchange_decision_fingerprint(value) == expected_digest
            and value.source_observation_ids == expected_source_pin
        )
    except (AttributeError, TypeError, ValueError):
        return False


def verify_exchange_status(
    observations: Mapping[str, object],
    *,
    as_of: datetime,
) -> ExchangeStatusDecision:
    """Require fresh, role-bound calendar and emergency-status agreement."""
    if not isinstance(observations, Mapping):
        raise TypeError("exchange status observations must be a mapping")
    current = _utc(as_of, "exchange status as_of")
    if any(name not in observations for name in _STATUS_ROLES):
        return _blocked_exchange("REFERENCE_SOURCE_MISSING", as_of=current)
    values: dict[str, ReferenceStatusSnapshot] = {}
    for name, expected_role in _STATUS_ROLES.items():
        value = observations[name]
        if not _is_verified_status_snapshot(value):
            return _blocked_exchange("REFERENCE_SNAPSHOT_UNVERIFIED", as_of=current)
        assert isinstance(value, ReferenceStatusSnapshot)
        values[name] = value
    identifiers = tuple(
        value.document.source_observation_id for value in values.values()
    )
    retrieved = tuple(value.document.retrieved_at for value in values.values())
    latest = max(retrieved)
    validity = min(value + timedelta(hours=24) for value in retrieved)
    if len(set(identifiers)) != len(identifiers) or len(
        {id(value.document) for value in values.values()}
    ) != len(values):
        return _blocked_exchange(
            "REFERENCE_SOURCE_IDENTITY_CONFLICT",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    if any(values[name].role != role for name, role in _STATUS_ROLES.items()):
        return _blocked_exchange(
            "REFERENCE_SOURCE_ROLE_CONFLICT",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    if any(value > current for value in retrieved):
        return _blocked_exchange(
            "REFERENCE_TIMESTAMP_IN_FUTURE",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    if any(current - value > timedelta(hours=24) for value in retrieved):
        return _blocked_exchange(
            "REFERENCE_SOURCE_STALE",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    if any(not value.supported for value in values.values()):
        return _blocked_exchange(
            "REFERENCE_SOURCE_UNSUPPORTED",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    if any(not value.healthy for value in values.values()):
        return _blocked_exchange(
            "REFERENCE_SOURCE_UNHEALTHY",
            as_of=current,
            identifiers=identifiers,
            retrieved_at=latest,
            valid_until=validity,
        )
    primary = values["primary"].status
    cross_check = values["cross_check"].status
    operational = values["operational_status"].status
    alert = values["trader_alert"].status
    common = {
        "as_of": current,
        "valid_until": validity,
        "source_observation_ids": tuple(sorted(set(identifiers))),
        "retrieved_at": latest,
    }
    emergency_values = {"EMERGENCY_CLOSED", "EMERGENCY_CLOSURE", "HALTED"}
    if operational in emergency_values or alert in emergency_values:
        return _issue_exchange_status_decision(
            status="BLOCKED",
            block_reason="EMERGENCY_CLOSURE",
            **common,
        )
    if primary not in {"OPEN", "CLOSED"} or cross_check not in {"OPEN", "CLOSED"}:
        return _issue_exchange_status_decision(
            status="BLOCKED",
            block_reason="CALENDAR_STATUS_UNCERTAIN",
            **common,
        )
    if primary != cross_check:
        return _issue_exchange_status_decision(
            status="BLOCKED",
            block_reason="CALENDAR_SOURCE_CONFLICT",
            **common,
        )
    if operational not in {"OPEN", "NORMAL", "OPERATIONAL"} or alert not in {
        "CLEAR",
        "NONE",
        "NO_ALERT",
    }:
        return _issue_exchange_status_decision(
            status="BLOCKED",
            block_reason="EMERGENCY_STATUS_UNCERTAIN",
            **common,
        )
    return _issue_exchange_status_decision(
        status=f"{primary}_CONFIRMED",
        block_reason=None,
        **common,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class HaltFeedSnapshot:
    configured_url: str
    observed_url: str
    origin: str
    venue: str
    scope: str
    source_observation_id: str
    retrieved_at: datetime
    valid_until: datetime
    healthy: bool
    pagination_complete: bool
    coverage: str
    halted_symbols: tuple[str, ...]
    document: SourceDocument
    supported: bool = True
    _authority: object = field(init=False, repr=False, compare=False)
    _snapshot_digest: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.document) is not SourceDocument:
            raise TypeError("halt snapshot requires an exact source document")
        if (
            self.configured_url != self.observed_url
            or self.observed_url != self.document.url
            or self.document.source_type != "OFFICIAL_REFERENCE"
            or self.document.source_role not in {
                "OPERATIONAL_STATUS",
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
            }
        ):
            raise ValueError("halt snapshot is not bound to a configured source")
        assert self.document.source_role is not None
        _require_official_role_url(self.document.source_role, self.document.url)
        if self.origin != _origin(self.configured_url):
            raise ValueError("halt snapshot origin is inconsistent")
        if not isinstance(self.venue, str) or not _VENUE.fullmatch(self.venue):
            raise ValueError("halt snapshot venue is malformed")
        if self.scope not in {
            "ACTIVE_HALTS",
            "EXCHANGE_OPERATIONAL_STATUS",
            "UNKNOWN",
        }:
            raise ValueError("halt snapshot scope is unsupported")
        if (
            not isinstance(self.source_observation_id, str)
            or not _IDENTIFIER.fullmatch(self.source_observation_id)
            or self.source_observation_id != self.document.source_observation_id
        ):
            raise ValueError("halt snapshot observation ID is inconsistent")
        retrieved_at = _utc(self.retrieved_at, "halt snapshot retrieval time")
        valid_until = _utc(self.valid_until, "halt snapshot validity")
        if (
            retrieved_at != self.document.retrieved_at
            or valid_until < retrieved_at
            or valid_until - retrieved_at > timedelta(minutes=5)
        ):
            raise ValueError("halt snapshot validity is malformed")
        object.__setattr__(self, "retrieved_at", retrieved_at)
        object.__setattr__(self, "valid_until", valid_until)
        if (
            type(self.healthy) is not bool
            or type(self.pagination_complete) is not bool
            or type(self.supported) is not bool
        ):
            raise TypeError("halt snapshot health, pagination, and support must be explicit")
        if self.coverage not in _HALT_COVERAGE:
            raise ValueError("halt snapshot coverage is unsupported")
        symbols = tuple(self.halted_symbols)
        if len(symbols) != len(set(symbols)) or any(
            not isinstance(value, str) or not _SYMBOL.fullmatch(value)
            for value in symbols
        ):
            raise ValueError("halt snapshot symbols are malformed")
        object.__setattr__(self, "halted_symbols", tuple(sorted(symbols)))
        if not self.supported and (
            self.healthy
            or self.pagination_complete
            or self.coverage != "UNKNOWN"
            or self.halted_symbols
        ):
            raise ValueError("unsupported halt snapshot cannot claim coverage")
        _document_fingerprint(self.document)


def _halt_fingerprint(value: HaltFeedSnapshot) -> str:
    if (
        type(value) is not HaltFeedSnapshot
        or type(value.configured_url) is not str
        or type(value.observed_url) is not str
        or type(value.origin) is not str
        or type(value.venue) is not str
        or type(value.scope) is not str
        or type(value.source_observation_id) is not str
        or type(value.retrieved_at) is not datetime
        or type(value.valid_until) is not datetime
        or type(value.healthy) is not bool
        or type(value.pagination_complete) is not bool
        or type(value.coverage) is not str
        or type(value.halted_symbols) is not tuple
        or any(type(symbol) is not str for symbol in value.halted_symbols)
        or type(value.supported) is not bool
    ):
        raise TypeError("halt snapshot has the wrong type")
    return _canonical_digest(
        {
            "kind": "HALT_FEED_SNAPSHOT",
            "configured_url": value.configured_url,
            "observed_url": value.observed_url,
            "origin": value.origin,
            "venue": value.venue,
            "scope": value.scope,
            "source_observation_id": value.source_observation_id,
            "retrieved_at": value.retrieved_at.isoformat(timespec="microseconds"),
            "valid_until": value.valid_until.isoformat(timespec="microseconds"),
            "healthy": value.healthy,
            "pagination_complete": value.pagination_complete,
            "coverage": value.coverage,
            "halted_symbols": list(value.halted_symbols),
            "supported": value.supported,
            "document": _document_fingerprint(value.document),
        }
    )


def _issue_halt_snapshot(
    document: SourceDocument,
    *,
    venue: str,
    scope: str,
    healthy: bool,
    pagination_complete: bool,
    coverage: str,
    halted_symbols: tuple[str, ...],
    supported: bool,
) -> HaltFeedSnapshot:
    value = HaltFeedSnapshot(
        configured_url=document.url,
        observed_url=document.url,
        origin=_origin(document.url),
        venue=venue,
        scope=scope,
        source_observation_id=document.source_observation_id,
        retrieved_at=document.retrieved_at,
        valid_until=document.retrieved_at + timedelta(minutes=5),
        healthy=healthy,
        pagination_complete=pagination_complete,
        coverage=coverage,
        halted_symbols=halted_symbols,
        document=document,
        supported=supported,
    )
    digest = _halt_fingerprint(value)
    object.__setattr__(value, "_authority", _REFERENCE_SNAPSHOT_AUTHORITY)
    object.__setattr__(value, "_snapshot_digest", digest)
    assert document.source_role is not None
    _remember_reference_authority(
        value,
        kind="HALT_FEED_SNAPSHOT",
        digest=digest,
        source_pin=(
            document.source_observation_id,
            document.content_hash,
            document.url,
            document.source_role,
        ),
    )
    return value


def _is_verified_halt_snapshot(value: object) -> bool:
    if type(value) is not HaltFeedSnapshot:
        return False
    try:
        issuance = _reference_authority_issuance(
            value,
            kind="HALT_FEED_SNAPSHOT",
        )
        if issuance is None:
            return False
        expected_digest, expected_source_pin = issuance
        return (
            value._authority is _REFERENCE_SNAPSHOT_AUTHORITY
            and _SHA256.fullmatch(value._snapshot_digest) is not None
            and value._snapshot_digest == expected_digest
            and _halt_fingerprint(value) == expected_digest
            and (
                value.document.source_observation_id,
                value.document.content_hash,
                value.document.url,
                value.document.source_role,
            )
            == expected_source_pin
        )
    except (AttributeError, TypeError, ValueError):
        return False


@dataclass(frozen=True, slots=True, weakref_slot=True)
class InstrumentStatusDecision:
    symbol: str
    halt_status: str
    as_of: datetime
    valid_until: datetime | None
    source_observation_ids: tuple[str, ...]
    block_reason: str | None
    _authority: object = field(init=False, repr=False, compare=False)
    _decision_digest: str = field(init=False, repr=False, compare=False)


def _instrument_decision_fingerprint(value: InstrumentStatusDecision) -> str:
    if (
        type(value) is not InstrumentStatusDecision
        or type(value.symbol) is not str
        or type(value.halt_status) is not str
        or type(value.as_of) is not datetime
        or (value.valid_until is not None and type(value.valid_until) is not datetime)
        or type(value.source_observation_ids) is not tuple
        or any(type(item) is not str for item in value.source_observation_ids)
        or (value.block_reason is not None and type(value.block_reason) is not str)
    ):
        raise TypeError("instrument status decision fields are malformed")
    return _canonical_digest(
        {
            "kind": "INSTRUMENT_STATUS_DECISION",
            "symbol": value.symbol,
            "halt_status": value.halt_status,
            "as_of": value.as_of.isoformat(timespec="microseconds"),
            "valid_until": (
                value.valid_until.isoformat(timespec="microseconds")
                if value.valid_until is not None
                else None
            ),
            "source_observation_ids": list(value.source_observation_ids),
            "block_reason": value.block_reason,
        }
    )


def is_reviewed_instrument_status_decision(value: object) -> bool:
    """Return true only for untampered output from halt-status classification."""
    if type(value) is not InstrumentStatusDecision:
        return False
    try:
        issuance = _reference_authority_issuance(
            value,
            kind="INSTRUMENT_STATUS_DECISION",
        )
        if issuance is None:
            return False
        expected_digest, expected_source_pin = issuance
        return (
            value._authority is _REFERENCE_SNAPSHOT_AUTHORITY
            and value._decision_digest == expected_digest
            and _instrument_decision_fingerprint(value) == expected_digest
            and value.source_observation_ids == expected_source_pin
        )
    except (AttributeError, TypeError, ValueError):
        return False


def _instrument_decision(
    symbol: str,
    status: str,
    reason: str | None,
    current: datetime,
    snapshots: Iterable[HaltFeedSnapshot],
) -> InstrumentStatusDecision:
    values = tuple(snapshots)
    value = InstrumentStatusDecision(
        symbol=symbol,
        halt_status=status,
        as_of=current,
        valid_until=(min(value.valid_until for value in values) if values else None),
        source_observation_ids=tuple(
            sorted({value.source_observation_id for value in values})
        ),
        block_reason=reason,
    )
    digest = _instrument_decision_fingerprint(value)
    object.__setattr__(value, "_authority", _REFERENCE_SNAPSHOT_AUTHORITY)
    object.__setattr__(value, "_decision_digest", digest)
    _remember_reference_authority(
        value,
        kind="INSTRUMENT_STATUS_DECISION",
        digest=digest,
        source_pin=value.source_observation_ids,
    )
    return value


def classify_instrument_status(
    symbol: str,
    listing_venue: str,
    observations: Mapping[str, object],
    *,
    as_of: datetime,
) -> InstrumentStatusDecision:
    """Return CLEAR only from complete, fresh active-halt coverage."""
    if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
        raise ValueError("instrument status symbol is malformed")
    if not isinstance(listing_venue, str) or not _VENUE.fullmatch(listing_venue):
        raise ValueError("instrument listing venue is malformed")
    if not isinstance(observations, Mapping):
        raise TypeError("halt observations must be a mapping")
    current = _utc(as_of, "instrument status as_of")
    provided: list[HaltFeedSnapshot] = []
    by_key: dict[str, HaltFeedSnapshot] = {}
    valid_keys: set[str] = set()
    first_error: str | None = None
    for name in _HALT_KEYS:
        raw = observations.get(name)
        if raw is None:
            first_error = first_error or "HALT_SOURCE_MISSING"
            continue
        if not _is_verified_halt_snapshot(raw):
            first_error = first_error or "HALT_SNAPSHOT_UNVERIFIED"
            continue
        assert isinstance(raw, HaltFeedSnapshot)
        value = raw
        provided.append(value)
        by_key[name] = value
        expected_role, expected_url, expected_scope = _HALT_EXPECTATIONS[name]
        if (
            value.document.source_role != expected_role
            or value.configured_url != expected_url
        ):
            first_error = "HALT_SOURCE_ROLE_CONFLICT"
        elif not value.supported:
            first_error = first_error or "HALT_SOURCE_UNSUPPORTED"
        elif value.scope != expected_scope or value.venue not in {
            listing_venue,
            "ALL_US",
        }:
            first_error = first_error or "HALT_SOURCE_SCOPE_CONFLICT"
        elif value.retrieved_at > current:
            first_error = first_error or "HALT_TIMESTAMP_IN_FUTURE"
        elif current > value.valid_until:
            first_error = first_error or "HALT_SOURCE_STALE"
        elif not value.healthy:
            first_error = first_error or "HALT_SOURCE_UNHEALTHY"
        elif not value.pagination_complete:
            first_error = first_error or "HALT_PAGINATION_INCOMPLETE"
        elif name == "primary_halt_feed" and value.coverage != "COMPLETE_ACTIVE_HALTS":
            first_error = first_error or "HALT_COVERAGE_INCOMPLETE"
        elif name == "operational_status" and value.coverage not in {
            "COMPLETE_ACTIVE_HALTS",
            "UNKNOWN",
        }:
            first_error = first_error or "HALT_COVERAGE_INCOMPLETE"
        elif name == "cross_check_halt_feed" and value.coverage not in {
            "COMPLETE_ACTIVE_HALTS",
            "PARTIAL",
        }:
            first_error = first_error or "HALT_COVERAGE_INCOMPLETE"
        else:
            valid_keys.add(name)
    identity_conflict = (
        len({value.source_observation_id for value in provided}) != len(provided)
        or len({id(value.document) for value in provided}) != len(provided)
    )
    if identity_conflict:
        first_error = "HALT_SOURCE_IDENTITY_CONFLICT"
    valid_halts = tuple(
        by_key[name]
        for name in ("primary_halt_feed", "cross_check_halt_feed")
        if name in valid_keys and symbol in by_key[name].halted_symbols
    )
    operational_alert = (
        "operational_status" in valid_keys
        and by_key["operational_status"].coverage == "UNKNOWN"
    )
    if identity_conflict:
        return _instrument_decision(
            symbol,
            "UNKNOWN",
            "HALT_SOURCE_IDENTITY_CONFLICT",
            current,
            provided,
        )
    if valid_halts and not identity_conflict:
        return _instrument_decision(
            symbol,
            "HALTED",
            "SYMBOL_HALTED",
            current,
            provided,
        )
    if operational_alert:
        return _instrument_decision(
            symbol,
            "HALTED",
            "EXCHANGE_OPERATIONAL_ALERT",
            current,
            provided,
        )
    if first_error is not None or len(provided) != len(_HALT_KEYS):
        return _instrument_decision(
            symbol,
            "UNKNOWN",
            first_error or "HALT_SOURCE_MISSING",
            current,
            provided,
        )
    return _instrument_decision(symbol, "CLEAR", None, current, provided)


def _xml_local_name(tag: object) -> str:
    if not isinstance(tag, str):
        raise ValueError("reference RSS contains a malformed element")
    return tag.rsplit("}", 1)[-1].casefold()


def _rss_items(document: SourceDocument) -> tuple[ET.Element, ...]:
    folded = document.body.upper()
    if b"<!DOCTYPE" in folded or b"<!ENTITY" in folded:
        raise ValueError("reference RSS contains prohibited declarations")
    try:
        root = ET.fromstring(document.body)
    except (ET.ParseError, UnicodeError):
        raise ValueError("reference RSS is malformed") from None
    if root.tag != "rss" or root.attrib != {"version": "2.0"}:
        raise ValueError("reference RSS schema is unsupported")
    channels = tuple(
        child for child in root if _xml_local_name(child.tag) == "channel"
    )
    if len(channels) != 1 or channels[0].tag != "channel":
        raise ValueError("reference RSS must contain exactly one channel")
    channel = channels[0]
    declarations = tuple(
        child for child in channel if _xml_local_name(child.tag) == "numitems"
    )
    if len(declarations) != 1 or declarations[0].tag != _NDAQ_NUM_ITEMS_TAG:
        raise ValueError("reference RSS item count declaration is missing or ambiguous")
    count_element = declarations[0]
    if count_element.attrib or len(count_element):
        raise ValueError("reference RSS item count declaration is malformed")
    declaration = (count_element.text or "").strip()
    if not _DECLARED_ITEM_COUNT.fullmatch(declaration):
        raise ValueError("reference RSS item count declaration is malformed")
    declared_count = int(declaration)
    items = tuple(child for child in channel if _xml_local_name(child.tag) == "item")
    if any(item.tag != "item" for item in items):
        raise ValueError("reference RSS items must use the exact RSS namespace")
    nested_items = tuple(
        element
        for element in root.iter()
        if element is not root and _xml_local_name(element.tag) == "item"
    )
    if len(nested_items) != len(items) or any(
        left is not right for left, right in zip(nested_items, items, strict=True)
    ):
        raise ValueError("reference RSS items must be direct channel children")
    if len(items) > 10_000:
        raise ValueError("reference RSS contains too many items")
    if declared_count != len(items):
        raise ValueError("reference RSS item count does not match its items")
    return items


def _trade_halt_symbols(document: SourceDocument) -> tuple[str, ...]:
    symbols: list[str] = []
    for item in _rss_items(document):
        direct = tuple(
            child for child in item if child.tag == _NDAQ_ISSUE_SYMBOL_TAG
        )
        symbol_like = tuple(
            child
            for child in item.iter()
            if child is not item
            and _xml_local_name(child.tag) in {"issuesymbol", "symbol"}
        )
        if (
            len(direct) != 1
            or len(symbol_like) != 1
            or symbol_like[0] is not direct[0]
            or direct[0].attrib
            or len(direct[0])
        ):
            raise ValueError("trade-halt RSS item has no unique valid symbol")
        symbol = (direct[0].text or "").strip().upper()
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("trade-halt RSS item has no unique valid symbol")
        if symbol in symbols:
            raise ValueError("trade-halt RSS contains a duplicate symbol item")
        symbols.append(symbol)
    return tuple(sorted(set(symbols)))


def _trader_alert_symbols(document: SourceDocument) -> tuple[str, ...]:
    """Validate the reviewed no-symbol alert schema; never infer from prose."""
    for item in _rss_items(document):
        descendants = tuple(child for child in item.iter() if child is not item)
        if any(
            _xml_local_name(child.tag) in {"issuesymbol", "symbol"}
            for child in descendants
        ):
            raise ValueError("Trader Alert RSS has no reviewed symbol field")
        direct_tags = tuple(child.tag for child in item)
        if (
            len(direct_tags) != len(_TRADER_ALERT_ITEM_TAGS)
            or set(direct_tags) != _TRADER_ALERT_ITEM_TAGS
            or any(child.attrib or len(child) for child in item)
        ):
            raise ValueError("Trader Alert RSS item schema is unsupported")
        category = item.find(f"{{{_NDAQ_NAMESPACE}}}NewsCategory")
        if category is None or (category.text or "").strip() != "Equity Trader Alert":
            raise ValueError("Trader Alert RSS category is unsupported")
    return ()


def _nyse_operational_clear(document: SourceDocument) -> bool:
    """Return clear only for the exact current-alert endpoint's empty JSON list."""

    def reject_constant(_: str) -> object:
        raise ValueError("NYSE current-alert JSON contains a non-finite value")

    try:
        payload = json.loads(
            document.body.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("NYSE current-alert JSON is malformed") from None
    if type(payload) is not list:
        raise ValueError("NYSE current-alert JSON must be an exact list")
    return not payload


class ReferenceClient:
    """Fetch only exact operator-reviewed official URLs with configured roles."""

    def __init__(
        self,
        transport: GetTransport,
        policy: EgressPolicy,
        *,
        allowed_urls: Iterable[str],
        source_roles: Mapping[str, str] | None = None,
        cache: ContentCache | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        reviewed = frozenset(allowed_urls)
        if not reviewed:
            raise NetworkPolicyError("reference client needs reviewed exact URLs")
        if not isinstance(source_roles, Mapping) or set(source_roles) != set(reviewed):
            raise NetworkPolicyError("reference URLs require exact configured roles")
        roles = {url: _role(source_roles[url]) for url in reviewed}
        for url in reviewed:
            policy.validate_get(url)
            try:
                _require_official_role_url(roles[url], url)
            except ValueError:
                raise NetworkPolicyError(
                    "reference URL and role are not an exact reviewed official pair"
                ) from None
        self._transport = transport
        self._policy = policy
        self._allowed_urls = reviewed
        self._source_roles = roles
        self._cache = cache
        self._now = now
        self._documents: dict[str, tuple[SourceDocument, str]] = {}
        self._observations: dict[str, SourceObservation] = {}

    def _validate_exact(self, url: str) -> None:
        if url not in self._allowed_urls:
            raise NetworkPolicyError("reference URL is not exactly preconfigured")

    def fetch(
        self,
        url: str,
        *,
        role: str | None = None,
        published_at: datetime | None = None,
        publisher: str | None = None,
        source_type: str = "OFFICIAL_REFERENCE",
    ) -> SourceDocument:
        self._validate_exact(url)
        configured_role = self._source_roles[url]
        requested_role = configured_role if role is None else _role(role)
        if requested_role != configured_role:
            raise NetworkPolicyError("reference URL role is not exactly preconfigured")
        if source_type != "OFFICIAL_REFERENCE":
            raise ValueError("reference source type is unsupported")
        if publisher is not None:
            publisher = _publisher(publisher)
        if published_at is not None:
            published_at = _utc(published_at, "reference publication time")

        def validate_redirect(target: str) -> None:
            self._validate_exact(target)
            if self._source_roles[target] != configured_role:
                raise NetworkPolicyError("reference redirect changes configured role")

        operational_json = configured_role == "OPERATIONAL_STATUS"
        response = get_with_redirects(
            self._transport,
            self._policy,
            url,
            {
                "Accept": (
                    "application/json"
                    if operational_json
                    else "application/json,text/html,text/plain,application/pdf"
                )
            },
            allowed_content_types=(
                ("application/json",)
                if operational_json
                else (
                    "application/json",
                    "application/pdf",
                    "application/xml",
                    "text/html",
                    "text/plain",
                    "text/xml",
                )
            ),
            exact_url_validator=validate_redirect,
        )
        retrieved_at = _utc(self._now(), "reference retrieval time")
        if published_at is not None and published_at > retrieved_at:
            raise ValueError("reference publication cannot follow retrieval")
        resolved_publisher = publisher or _publisher(
            urlsplit(response.url).hostname or "official"
        )
        digest = hashlib.sha256(response.body).hexdigest()
        identity = hashlib.sha256(
            source_type.encode("ascii")
            + b"\0"
            + response.url.encode("ascii")
            + b"\0"
            + retrieved_at.isoformat(timespec="microseconds").encode("ascii")
            + b"\0"
            + response.body
        ).hexdigest()
        feed = configured_role.casefold().replace(":", "-").replace("_", "-")
        observation = SourceObservation(
            observation_id=f"obs-{identity[:24]}",
            url=response.url,
            source_type=source_type,
            source_timestamp=published_at or retrieved_at,
            retrieved_at=retrieved_at,
            feed=feed,
            delay_seconds=(
                max(0, int((retrieved_at - published_at).total_seconds()))
                if published_at is not None
                else 0
            ),
            content_hash=digest,
        )
        self._observations[observation.observation_id] = observation
        if self._cache is not None:
            self._cache.put(observation, response.body)
        document = SourceDocument(
            url=response.url,
            published_at=published_at,
            retrieved_at=retrieved_at,
            content_hash=digest,
            body=response.body,
            source_observation_id=observation.observation_id,
            publisher=resolved_publisher,
            source_type=source_type,
            timestamp_source=("PRIMARY_METADATA" if published_at else "UNAVAILABLE"),
            source_role=configured_role,
        )
        self._documents[document.source_observation_id] = (
            document,
            _canonical_digest(_document_fingerprint(document)),
        )
        return document

    def _require_fetched_document(self, document: SourceDocument) -> SourceDocument:
        if type(document) is not SourceDocument:
            raise TypeError("reference parser requires an exact source document")
        issued = self._documents.get(document.source_observation_id)
        if issued is None or issued[0] is not document:
            raise ValueError("reference parser requires this client's fetched document")
        try:
            current_fingerprint = _canonical_digest(_document_fingerprint(document))
        except (TypeError, ValueError):
            raise ValueError("reference fetched-document binding is invalid") from None
        if current_fingerprint != issued[1] or (
            document.url not in self._allowed_urls
            or document.source_role != self._source_roles[document.url]
            or document.source_type != "OFFICIAL_REFERENCE"
        ):
            raise ValueError("reference fetched-document binding is invalid")
        return document

    def health_attestation(
        self,
        document: SourceDocument,
    ) -> SourceHealthAttestation:
        """Issue cache authority only for this client's successful fetch."""
        value = self._require_fetched_document(document)
        observation = self._observations.get(value.source_observation_id)
        if observation is None:
            raise ValueError("reference fetch has no bound source observation")
        return _issue_provider_health_attestation(observation)

    def parse_status(self, document: SourceDocument) -> ReferenceStatusSnapshot:
        """Parse only proven machine-readable roles; HTML roles are unsupported."""
        value = self._require_fetched_document(document)
        if value.source_role == "OPERATIONAL_STATUS":
            clear = _nyse_operational_clear(value)
            return _issue_status_snapshot(
                value,
                status=("OPERATIONAL" if clear else "EMERGENCY_CLOSED"),
                healthy=True,
                supported=True,
            )
        if value.source_role == "TRADER_ALERT_HALT":
            _rss_items(value)
            return _issue_status_snapshot(
                value,
                status="UNKNOWN",
                healthy=True,
                supported=True,
            )
        return _issue_status_snapshot(
            value,
            status="UNSUPPORTED",
            healthy=False,
            supported=False,
        )

    def parse_halt_feed(self, document: SourceDocument) -> HaltFeedSnapshot:
        """Parse exact Nasdaq RSS feeds and block unsupported HTML sources."""
        value = self._require_fetched_document(document)
        if value.source_role == "PRIMARY_HALT_FEED":
            return _issue_halt_snapshot(
                value,
                venue="ALL_US",
                scope="ACTIVE_HALTS",
                healthy=True,
                pagination_complete=True,
                coverage="COMPLETE_ACTIVE_HALTS",
                halted_symbols=_trade_halt_symbols(value),
                supported=True,
            )
        if value.source_role == "TRADER_ALERT_HALT":
            return _issue_halt_snapshot(
                value,
                venue="ALL_US",
                scope="ACTIVE_HALTS",
                healthy=True,
                pagination_complete=True,
                coverage="PARTIAL",
                halted_symbols=_trader_alert_symbols(value),
                supported=True,
            )
        if value.source_role == "OPERATIONAL_STATUS":
            clear = _nyse_operational_clear(value)
            return _issue_halt_snapshot(
                value,
                venue="ALL_US",
                scope="EXCHANGE_OPERATIONAL_STATUS",
                healthy=True,
                pagination_complete=True,
                coverage=("COMPLETE_ACTIVE_HALTS" if clear else "UNKNOWN"),
                halted_symbols=(),
                supported=True,
            )
        raise ValueError("reference role is not a halt-status source")


__all__ = [
    "ExchangeStatusDecision",
    "HaltFeedSnapshot",
    "InstrumentStatusDecision",
    "ReferenceClient",
    "ReferenceStatusSnapshot",
    "SourceDocument",
    "classify_instrument_status",
    "is_reviewed_exchange_status_decision",
    "is_reviewed_instrument_status_decision",
    "verify_exchange_status",
]
