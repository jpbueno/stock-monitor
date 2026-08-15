"""Content-addressed, integrity-checked local source cache."""

from __future__ import annotations

import hashlib
import fcntl
import json
import os
import re
import stat
import tempfile
import threading
import urllib.parse
import weakref
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator

from stock_monitor.domain import require_aware_timestamp


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
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


class CacheIntegrityError(RuntimeError):
    """Cached bytes or immutable metadata no longer match their hashes."""


class CacheSourceUnavailableError(RuntimeError):
    """Current source health failed, so cached bytes are not permission to proceed."""


def _utc(value: datetime, name: str) -> datetime:
    return require_aware_timestamp(value, name).astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _parse_iso(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise CacheIntegrityError(f"cached {name} is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CacheIntegrityError(f"cached {name} is malformed") from None
    try:
        return _utc(parsed, name)
    except ValueError:
        raise CacheIntegrityError(f"cached {name} is malformed") from None


def _validate_observation_url(url: str) -> str:
    if (
        not isinstance(url, str)
        or not url
        or "\\" in url
        or any(ord(character) <= 32 or ord(character) == 127 for character in url)
    ):
        raise ValueError("source observation URL is malformed")
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError:
        raise ValueError("source observation URL is malformed") from None
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError("source observation URL must be credential-free HTTPS")
    for name, _ in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        folded = name.casefold()
        if folded == "page_token":
            continue
        compact = re.sub(r"[^a-z0-9]", "", folded)
        if folded.endswith("key") or any(
            part in folded for part in _SENSITIVE_QUERY_PARTS
        ) or any(compact.endswith(part) for part in _SENSITIVE_QUERY_SUFFIXES):
            raise ValueError("source observation URL contains credential material")
    return url


def _source_origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    return f"https://{hostname}"


def _entitlement_class(source_type: str, feed: str) -> str:
    if source_type.startswith("ALPACA_"):
        return f"ALPACA_{feed.upper()}"
    if source_type.startswith("SEC_"):
        return "PUBLIC_SEC"
    return "OFFICIAL_REFERENCE"


_REVIEWED_SOURCE_SCOPES = frozenset(
    {
        (
            "ALPACA_DAILY_BARS",
            "https://data.alpaca.markets",
            "sip",
            "ALPACA_SIP",
        ),
        (
            "ALPACA_HISTORICAL_QUOTES",
            "https://data.alpaca.markets",
            "sip",
            "ALPACA_SIP",
        ),
        (
            "ALPACA_LATEST_QUOTES",
            "https://data.alpaca.markets",
            "iex",
            "ALPACA_IEX",
        ),
        (
            "ALPACA_OPTION_SNAPSHOTS",
            "https://data.alpaca.markets",
            "indicative",
            "ALPACA_INDICATIVE",
        ),
        ("SEC_ARCHIVE", "https://www.sec.gov", "sec", "PUBLIC_SEC"),
        (
            "SEC_SUBMISSIONS",
            "https://data.sec.gov",
            "sec",
            "PUBLIC_SEC",
        ),
        (
            "OFFICIAL_REFERENCE",
            "https://www.nasdaqtrader.com",
            "cross-check-calendar",
            "OFFICIAL_REFERENCE",
        ),
        (
            "OFFICIAL_REFERENCE",
            "https://www.nasdaqtrader.com",
            "primary-halt-feed",
            "OFFICIAL_REFERENCE",
        ),
        (
            "OFFICIAL_REFERENCE",
            "https://www.nasdaqtrader.com",
            "trader-alert-halt",
            "OFFICIAL_REFERENCE",
        ),
        (
            "OFFICIAL_REFERENCE",
            "https://www.nyse.com",
            "operational-status",
            "OFFICIAL_REFERENCE",
        ),
        (
            "OFFICIAL_REFERENCE",
            "https://www.nyse.com",
            "primary-calendar",
            "OFFICIAL_REFERENCE",
        ),
    }
)
_HEALTH_ATTESTATION_AUTHORITY = object()
_KNOWN_SOURCE_TYPES = frozenset(scope[0] for scope in _REVIEWED_SOURCE_SCOPES)
_SOURCE_AGE_POLICY = {
    "ALPACA_DAILY_BARS": (timedelta(hours=24), None),
    "ALPACA_HISTORICAL_QUOTES": (timedelta(hours=24), None),
    "ALPACA_LATEST_QUOTES": (timedelta(minutes=5), timedelta(minutes=5)),
    "ALPACA_OPTION_SNAPSHOTS": (timedelta(minutes=5), timedelta(minutes=5)),
    "OFFICIAL_REFERENCE": (timedelta(hours=24), None),
    "SEC_ARCHIVE": (timedelta(hours=24), None),
    "SEC_SUBMISSIONS": (timedelta(hours=24), None),
}


@dataclass(frozen=True, slots=True)
class SourceObservation:
    observation_id: str
    url: str
    source_type: str
    source_timestamp: datetime
    retrieved_at: datetime
    feed: str
    delay_seconds: int
    content_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.observation_id, str) or not _IDENTIFIER.fullmatch(
            self.observation_id
        ):
            raise ValueError("source observation ID is malformed")
        object.__setattr__(self, "url", _validate_observation_url(self.url))
        if (
            not isinstance(self.source_type, str)
            or not self.source_type
            or not self.source_type.isascii()
            or not isinstance(self.feed, str)
            or not self.feed
            or not self.feed.isascii()
        ):
            raise ValueError("source observation labels are malformed")
        source_timestamp = _utc(self.source_timestamp, "source_timestamp")
        retrieved_at = _utc(self.retrieved_at, "retrieved_at")
        if source_timestamp > retrieved_at:
            raise ValueError("source timestamp cannot follow retrieval")
        object.__setattr__(self, "source_timestamp", source_timestamp)
        object.__setattr__(self, "retrieved_at", retrieved_at)
        if type(self.delay_seconds) is not int or self.delay_seconds < 0:
            raise ValueError("source delay must be a non-negative integer")
        if self.content_hash is not None and not _SHA256.fullmatch(self.content_hash):
            raise ValueError("source content hash must be lowercase SHA-256")

    def with_content_hash(self, digest: str) -> SourceObservation:
        if not _SHA256.fullmatch(digest):
            raise ValueError("source content hash must be lowercase SHA-256")
        return replace(self, content_hash=digest)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SourceHealthAttestation:
    """Current, provenance-bearing permission to use one exact source scope."""

    source_observation_id: str
    source_type: str
    origin: str
    feed: str
    entitlement_class: str
    checked_at: datetime
    valid_until: datetime
    healthy: bool
    entitlement_ok: bool
    _authority: object = field(init=False, repr=False, compare=False)
    _attestation_digest: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.source_observation_id, str) or not _IDENTIFIER.fullmatch(
            self.source_observation_id
        ):
            raise ValueError("source health observation ID is malformed")
        for value, name in (
            (self.source_type, "source type"),
            (self.feed, "feed"),
            (self.entitlement_class, "entitlement class"),
        ):
            if (
                not isinstance(value, str)
                or not value
                or len(value) > 128
                or not value.isascii()
                or not value.isprintable()
            ):
                raise ValueError(f"source health {name} is malformed")
        canonical_origin = _validate_observation_url(self.origin)
        parsed = urllib.parse.urlsplit(canonical_origin)
        if parsed.path not in {"", "/"} or parsed.query:
            raise ValueError("source health origin must be an exact HTTPS origin")
        object.__setattr__(self, "origin", _source_origin(canonical_origin))
        checked_at = _utc(self.checked_at, "health checked_at")
        valid_until = _utc(self.valid_until, "health valid_until")
        if valid_until < checked_at:
            raise ValueError("source health validity cannot precede its check")
        if self.source_type not in _KNOWN_SOURCE_TYPES:
            raise ValueError("source health source type is not reviewed")
        expected_entitlement = _entitlement_class(self.source_type, self.feed)
        if self.entitlement_class != expected_entitlement:
            raise ValueError("source health entitlement class is not reviewed")
        if (
            self.source_type,
            self.origin,
            self.feed,
            self.entitlement_class,
        ) not in _REVIEWED_SOURCE_SCOPES:
            raise ValueError("source health scope is not an exact reviewed source")
        normalized_feed = re.sub(r"[^a-z0-9]", "", self.feed.casefold())
        short_reference = self.source_type == "OFFICIAL_REFERENCE" and any(
            token in normalized_feed
            for token in ("halt", "operationalstatus", "traderalert")
        )
        maximum_validity = (
            timedelta(minutes=5)
            if self.source_type.startswith("ALPACA_") or short_reference
            else timedelta(hours=24)
        )
        if valid_until - checked_at > maximum_validity:
            raise ValueError("source health validity exceeds its reviewed scope")
        object.__setattr__(self, "checked_at", checked_at)
        object.__setattr__(self, "valid_until", valid_until)
        if type(self.healthy) is not bool or type(self.entitlement_ok) is not bool:
            raise TypeError("source health flags must be boolean")


_ISSUED_HEALTH_LOCK = threading.Lock()
_ISSUED_HEALTH: dict[
    int,
    tuple[
        weakref.ReferenceType[SourceHealthAttestation],
        str,
        str,
        tuple[str, str, str, str],
    ],
] = {}


@dataclass(frozen=True, slots=True)
class SourceDocument:
    url: str
    published_at: datetime | None
    retrieved_at: datetime
    content_hash: str
    body: bytes
    source_observation_id: str
    publisher: str
    source_type: str
    timestamp_source: str
    accession: str | None = None
    source_role: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "url", _validate_observation_url(self.url))
        retrieved_at = _utc(self.retrieved_at, "retrieved_at")
        object.__setattr__(self, "retrieved_at", retrieved_at)
        if self.published_at is not None:
            published_at = _utc(self.published_at, "published_at")
            if published_at > retrieved_at:
                raise ValueError("source publication cannot follow retrieval")
            object.__setattr__(self, "published_at", published_at)
        if not _SHA256.fullmatch(self.content_hash):
            raise ValueError("source document hash must be lowercase SHA-256")
        if not isinstance(self.body, bytes) or not self.body:
            raise ValueError("source document body must be non-empty bytes")
        if hashlib.sha256(self.body).hexdigest() != self.content_hash:
            raise ValueError("source document bytes do not match their hash")
        for value, name in (
            (self.source_observation_id, "observation ID"),
            (self.publisher, "publisher"),
            (self.source_type, "source type"),
            (self.timestamp_source, "timestamp source"),
        ):
            if not isinstance(value, str) or not value or not value.isascii():
                raise ValueError(f"source document {name} is malformed")
        if self.source_role is not None and (
            not isinstance(self.source_role, str)
            or not self.source_role
            or len(self.source_role) > 128
            or not self.source_role.isascii()
            or not self.source_role.isprintable()
        ):
            raise ValueError("source document role is malformed")


def _observation_document(observation: SourceObservation) -> dict[str, object]:
    return {
        "content_hash": observation.content_hash,
        "delay_seconds": observation.delay_seconds,
        "feed": observation.feed,
        "observation_id": observation.observation_id,
        "retrieved_at": _iso(observation.retrieved_at),
        "source_timestamp": _iso(observation.source_timestamp),
        "source_type": observation.source_type,
        "url": observation.url,
    }


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _health_fingerprint(value: SourceHealthAttestation) -> str:
    if (
        type(value) is not SourceHealthAttestation
        or type(value.source_observation_id) is not str
        or type(value.source_type) is not str
        or type(value.origin) is not str
        or type(value.feed) is not str
        or type(value.entitlement_class) is not str
        or type(value.checked_at) is not datetime
        or type(value.valid_until) is not datetime
        or type(value.healthy) is not bool
        or type(value.entitlement_ok) is not bool
    ):
        raise TypeError("source health attestation has the wrong type")
    return hashlib.sha256(
        _canonical_json(
            {
                "kind": "PROVIDER_HEALTH_ATTESTATION",
                "source_observation_id": value.source_observation_id,
                "source_type": value.source_type,
                "origin": value.origin,
                "feed": value.feed,
                "entitlement_class": value.entitlement_class,
                "checked_at": _iso(value.checked_at),
                "valid_until": _iso(value.valid_until),
                "healthy": value.healthy,
                "entitlement_ok": value.entitlement_ok,
            }
        )
    ).hexdigest()


def _issue_provider_health_attestation(
    observation: SourceObservation,
) -> SourceHealthAttestation:
    if type(observation) is not SourceObservation:
        raise TypeError("provider health requires an exact source observation")
    checked_at = observation.retrieved_at
    value = SourceHealthAttestation(
        source_observation_id=observation.observation_id,
        source_type=observation.source_type,
        origin=_source_origin(observation.url),
        feed=observation.feed,
        entitlement_class=_entitlement_class(
            observation.source_type,
            observation.feed,
        ),
        checked_at=checked_at,
        valid_until=checked_at + timedelta(minutes=5),
        healthy=True,
        entitlement_ok=True,
    )
    digest = _health_fingerprint(value)
    object.__setattr__(value, "_authority", _HEALTH_ATTESTATION_AUTHORITY)
    object.__setattr__(value, "_attestation_digest", digest)
    object_id = id(value)

    def forget(reference: weakref.ReferenceType[SourceHealthAttestation]) -> None:
        with _ISSUED_HEALTH_LOCK:
            current = _ISSUED_HEALTH.get(object_id)
            if current is not None and current[0] is reference:
                _ISSUED_HEALTH.pop(object_id, None)

    reference = weakref.ref(value, forget)
    with _ISSUED_HEALTH_LOCK:
        _ISSUED_HEALTH[object_id] = (
            reference,
            digest,
            observation.observation_id,
            (
                observation.source_type,
                _source_origin(observation.url),
                observation.feed,
                _entitlement_class(observation.source_type, observation.feed),
            ),
        )
    return value


def _is_provider_health_attestation(value: object) -> bool:
    if type(value) is not SourceHealthAttestation:
        return False
    try:
        with _ISSUED_HEALTH_LOCK:
            issuance = _ISSUED_HEALTH.get(id(value))
            if issuance is None or issuance[0]() is not value:
                return False
        expected_digest = issuance[1]
        expected_observation_id = issuance[2]
        expected_scope = issuance[3]
        return (
            value._authority is _HEALTH_ATTESTATION_AUTHORITY
            and _SHA256.fullmatch(value._attestation_digest) is not None
            and value._attestation_digest == expected_digest
            and _health_fingerprint(value) == expected_digest
            and value.source_observation_id == expected_observation_id
            and (
                value.source_type,
                value.origin,
                value.feed,
                value.entitlement_class,
            )
            == expected_scope
        )
    except (AttributeError, TypeError, ValueError):
        return False


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise CacheIntegrityError("cached metadata contains duplicate fields")
        result[name] = value
    return result


def _decode_observation(value: object) -> SourceObservation:
    if not isinstance(value, dict):
        raise CacheIntegrityError("cached observation is malformed")
    expected = {
        "content_hash",
        "delay_seconds",
        "feed",
        "observation_id",
        "retrieved_at",
        "source_timestamp",
        "source_type",
        "url",
    }
    if set(value) != expected:
        raise CacheIntegrityError("cached observation fields are malformed")
    try:
        return SourceObservation(
            observation_id=value["observation_id"],
            url=value["url"],
            source_type=value["source_type"],
            source_timestamp=_parse_iso(value["source_timestamp"], "source timestamp"),
            retrieved_at=_parse_iso(value["retrieved_at"], "retrieval timestamp"),
            feed=value["feed"],
            delay_seconds=value["delay_seconds"],
            content_hash=value["content_hash"],
        )
    except (TypeError, ValueError):
        raise CacheIntegrityError("cached observation is malformed") from None


class ContentCache:
    """Persist immutable payload bytes and their pinned source observation."""

    _registry_guard = threading.Lock()
    _observation_locks: dict[Path, threading.Lock] = {}

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _payload_path(self, digest: str) -> Path:
        return self.root / "sha256" / digest[:2] / f"{digest}.bin"

    def _metadata_path(self, digest: str) -> Path:
        return self.root / "sha256" / digest[:2] / f"{digest}.json"

    def _observation_path(self, observation_id: str) -> Path:
        return self.root / "observations" / observation_id[:2] / f"{observation_id}.json"

    def _observation_lock_path(self, observation_id: str) -> Path:
        return self.root / "locks" / observation_id[:2] / f"{observation_id}.lock"

    @contextmanager
    def _locked_observation(self, observation_id: str) -> Iterator[None]:
        lock_path = self._observation_lock_path(observation_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._registry_guard:
            thread_lock = self._observation_locks.setdefault(
                lock_path,
                threading.Lock(),
            )
        with thread_lock:
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(lock_path, flags, 0o600)
            except OSError as error:
                raise CacheIntegrityError(
                    f"cannot open cache observation lock: {type(error).__name__}"
                ) from None
            try:
                details = os.fstat(descriptor)
                if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
                    raise CacheIntegrityError("cache observation lock is unsafe")
                os.fchmod(descriptor, 0o600)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            raise

    def put(self, observation: SourceObservation, payload: bytes) -> str:
        if not isinstance(observation, SourceObservation):
            raise TypeError("cache observation has the wrong type")
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("cache payload must be non-empty bytes")
        digest = hashlib.sha256(payload).hexdigest()
        if observation.content_hash not in (None, digest):
            raise CacheIntegrityError("source observation content hash mismatch")
        pinned = observation.with_content_hash(digest)
        observation_value = _observation_document(pinned)
        observation_bytes = _canonical_json(observation_value)
        content_metadata = {
            "content_sha256": digest,
            "size_bytes": len(payload),
        }
        content_metadata_bytes = _canonical_json(content_metadata)
        envelope = {
            "metadata": content_metadata,
            "metadata_sha256": hashlib.sha256(content_metadata_bytes).hexdigest(),
            "schema_version": 1,
        }
        payload_path = self._payload_path(digest)
        metadata_path = self._metadata_path(digest)
        observation_envelope = {
            "metadata_sha256": hashlib.sha256(observation_bytes).hexdigest(),
            "observation": observation_value,
            "payload_sha256": digest,
            "schema_version": 1,
        }
        observation_path = self._observation_path(observation.observation_id)
        observation_payload = _canonical_json(observation_envelope)
        with self._locked_observation(observation.observation_id):
            if observation_path.exists():
                try:
                    existing_observation = observation_path.read_bytes()
                except OSError as error:
                    raise CacheIntegrityError(
                        f"cannot verify cached observation: {type(error).__name__}"
                    ) from None
                if existing_observation != observation_payload:
                    raise CacheIntegrityError(
                        "source observation ID is already pinned to different content"
                    )
                restored, existing_payload = self._read_observation_envelope(
                    observation.observation_id
                )
                if restored != pinned or existing_payload != digest:
                    raise CacheIntegrityError("cached observation binding is corrupt")
                self._read_payload(digest)
                self._read_metadata(digest, metadata_path, expected_size=len(payload))
                return digest

            if payload_path.exists():
                self._read_payload(digest)
                if metadata_path.exists():
                    self._read_metadata(
                        digest,
                        metadata_path,
                        expected_size=len(payload),
                    )
            else:
                if metadata_path.exists():
                    raise CacheIntegrityError(
                        "cached metadata exists without its payload"
                    )
                self._atomic_write(payload_path, payload)
            if not metadata_path.exists():
                self._atomic_write(metadata_path, _canonical_json(envelope))
            self._atomic_write(observation_path, observation_payload)
        return digest

    @staticmethod
    def _read_metadata(
        digest: str,
        metadata_path: Path,
        *,
        expected_size: int | None = None,
    ) -> int:
        try:
            metadata_bytes = metadata_path.read_bytes()
        except OSError as error:
            raise CacheIntegrityError(
                f"cannot read cached metadata: {type(error).__name__}"
            ) from None
        try:
            envelope = json.loads(
                metadata_bytes,
                object_pairs_hook=_strict_json_object,
            )
        except (UnicodeError, json.JSONDecodeError):
            raise CacheIntegrityError("cached metadata is malformed") from None
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"metadata", "metadata_sha256", "schema_version"}
            or envelope["schema_version"] != 1
            or not isinstance(envelope["metadata_sha256"], str)
            or not isinstance(envelope["metadata"], dict)
            or set(envelope["metadata"]) != {"content_sha256", "size_bytes"}
        ):
            raise CacheIntegrityError("cached metadata is malformed")
        metadata = envelope["metadata"]
        metadata_bytes = _canonical_json(metadata)
        if hashlib.sha256(metadata_bytes).hexdigest() != envelope["metadata_sha256"]:
            raise CacheIntegrityError("cached metadata hash mismatch")
        size = metadata["size_bytes"]
        if (
            metadata["content_sha256"] != digest
            or type(size) is not int
            or size <= 0
            or (expected_size is not None and size != expected_size)
        ):
            raise CacheIntegrityError("cached content metadata is inconsistent")
        return size

    def _read_payload(self, digest: str) -> bytes:
        try:
            payload = self._payload_path(digest).read_bytes()
        except OSError as error:
            raise CacheIntegrityError(
                f"cannot read cached object: {type(error).__name__}"
            ) from None
        if not payload or hashlib.sha256(payload).hexdigest() != digest:
            raise CacheIntegrityError("cached payload hash mismatch")
        return payload

    def _read_observation_envelope(
        self,
        observation_id: str,
    ) -> tuple[SourceObservation, str]:
        if not isinstance(observation_id, str) or not _IDENTIFIER.fullmatch(
            observation_id
        ):
            raise CacheIntegrityError("cache observation ID is malformed")
        try:
            payload = self._observation_path(observation_id).read_bytes()
        except OSError as error:
            raise CacheIntegrityError(
                f"cannot read cached observation: {type(error).__name__}"
            ) from None
        try:
            envelope = json.loads(
                payload,
                object_pairs_hook=_strict_json_object,
            )
        except (UnicodeError, json.JSONDecodeError):
            raise CacheIntegrityError("cached observation envelope is malformed") from None
        if (
            not isinstance(envelope, dict)
            or set(envelope)
            != {
                "metadata_sha256",
                "observation",
                "payload_sha256",
                "schema_version",
            }
            or envelope["schema_version"] != 1
            or not isinstance(envelope["metadata_sha256"], str)
            or not isinstance(envelope["payload_sha256"], str)
            or not _SHA256.fullmatch(envelope["payload_sha256"])
        ):
            raise CacheIntegrityError("cached observation envelope is malformed")
        observation_bytes = _canonical_json(envelope["observation"])
        if hashlib.sha256(observation_bytes).hexdigest() != envelope["metadata_sha256"]:
            raise CacheIntegrityError("cached observation metadata hash mismatch")
        observation = _decode_observation(envelope["observation"])
        digest = envelope["payload_sha256"]
        if (
            observation.observation_id != observation_id
            or observation.content_hash != digest
        ):
            raise CacheIntegrityError("cached observation binding is inconsistent")
        return observation, digest

    @staticmethod
    def _authorize(
        observation: SourceObservation,
        health: SourceHealthAttestation,
        as_of: datetime,
    ) -> None:
        if not isinstance(health, SourceHealthAttestation):
            raise TypeError("cache health attestation has the wrong type")
        if not _is_provider_health_attestation(health):
            raise CacheSourceUnavailableError(
                "cache health attestation was not issued by a provider boundary"
            )
        current = _utc(as_of, "cache as_of")
        expected = (
            observation.source_type,
            _source_origin(observation.url),
            observation.feed,
            _entitlement_class(observation.source_type, observation.feed),
        )
        actual = (
            health.source_type,
            health.origin,
            health.feed,
            health.entitlement_class,
        )
        if health.source_observation_id != observation.observation_id or actual != expected:
            raise CacheSourceUnavailableError(
                "current source health scope does not match cached evidence"
            )
        maximum_retrieval_age, maximum_source_age = _SOURCE_AGE_POLICY[
            observation.source_type
        ]
        if (
            observation.retrieved_at > current
            or current - observation.retrieved_at > maximum_retrieval_age
            or (
                maximum_source_age is not None
                and current - observation.source_timestamp > maximum_source_age
            )
        ):
            raise CacheSourceUnavailableError(
                "cached source observation is outside its reviewed age policy"
            )
        if (
            health.healthy is not True
            or health.entitlement_ok is not True
            or health.checked_at > current
            or current > health.valid_until
        ):
            raise CacheSourceUnavailableError(
                "current source health or entitlement is unavailable"
            )

    def get(
        self,
        digest: str,
        *,
        health: SourceHealthAttestation,
        as_of: datetime,
    ) -> tuple[SourceObservation, bytes]:
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise CacheIntegrityError("cache key must be lowercase SHA-256")
        self._read_metadata(digest, self._metadata_path(digest))
        observations: list[SourceObservation] = []
        observation_root = self.root / "observations"
        if observation_root.exists():
            for path in sorted(observation_root.glob("*/*.json")):
                observation, pinned_digest = self._read_observation_envelope(
                    path.stem
                )
                if pinned_digest == digest:
                    observations.append(observation)
        if len(observations) != 1:
            raise CacheIntegrityError(
                "cache digest lookup does not identify one source observation"
            )
        observation = observations[0]
        self._authorize(observation, health, as_of)
        payload = self._read_payload(digest)
        self._read_metadata(
            digest,
            self._metadata_path(digest),
            expected_size=len(payload),
        )
        return observation, payload

    def get_observation(
        self,
        observation_id: str,
        *,
        health: SourceHealthAttestation,
        as_of: datetime,
    ) -> tuple[SourceObservation, bytes]:
        observation, digest = self._read_observation_envelope(observation_id)
        self._authorize(observation, health, as_of)
        payload = self._read_payload(digest)
        self._read_metadata(
            digest,
            self._metadata_path(digest),
            expected_size=len(payload),
        )
        return observation, payload


__all__ = [
    "CacheIntegrityError",
    "CacheSourceUnavailableError",
    "ContentCache",
    "SourceHealthAttestation",
    "SourceDocument",
    "SourceObservation",
]
