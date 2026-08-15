"""SEC submissions and Archives retrieval with aggregate fair-access pacing."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

from stock_monitor.domain import require_aware_timestamp

from .cache import (
    ContentCache,
    SourceDocument,
    SourceHealthAttestation,
    SourceObservation,
    _issue_provider_health_attestation,
)
from .http import (
    EgressPolicy,
    GetTransport,
    HttpResponse,
    NetworkPolicyError,
    ProviderResponseError,
    get_with_redirects,
)


_SUBMISSIONS_ORIGIN = "https://data.sec.gov/submissions/"
_ARCHIVES_ORIGIN = "https://www.sec.gov/Archives/"
_EMAIL = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+"
)
_ARCHIVE_PATH = re.compile(r"[A-Za-z0-9._/-]{1,1024}\Z")
_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}\Z")
_PRIMARY_DOCUMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}\Z")


class SecRateLimitError(RuntimeError):
    """The shared SEC pacing state is unsafe or cannot be honored."""


class SecMetadataError(ProviderResponseError):
    """SEC filing acceptance metadata is missing or inconsistent."""


class SecRateGovernor:
    """Serialize SEC request starts across threads and processes."""

    _registry_guard = threading.Lock()
    _thread_locks: dict[Path, threading.Lock] = {}

    def __init__(
        self,
        state_path: Path,
        *,
        minimum_interval: float = 0.110,
        wall_clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if (
            not isinstance(minimum_interval, (int, float))
            or isinstance(minimum_interval, bool)
            or not math.isfinite(float(minimum_interval))
            or float(minimum_interval) < 0.110
            or float(minimum_interval) > 10.0
        ):
            raise ValueError("SEC minimum interval must be from 0.110 through 10 seconds")
        self.state_path = Path(os.path.abspath(Path(state_path).expanduser()))
        self.minimum_interval = float(minimum_interval)
        self._wall_clock = wall_clock
        self._sleeper = sleeper
        with self._registry_guard:
            self._thread_lock = self._thread_locks.setdefault(
                self.state_path,
                threading.Lock(),
            )

    def _now(self) -> float:
        value = float(self._wall_clock())
        if not math.isfinite(value) or value < 0:
            raise SecRateLimitError("SEC rate clock is invalid")
        return value

    def wait(self) -> None:
        """Wait until the next safe request start and persist it under a file lock."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._thread_lock:
            try:
                descriptor = os.open(
                    self.state_path,
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except OSError as error:
                raise SecRateLimitError(
                    f"SEC rate state cannot be opened safely: {type(error).__name__}"
                ) from None
            try:
                details = os.fstat(descriptor)
                if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
                    raise SecRateLimitError("SEC rate state is not a private regular file")
                os.fchmod(descriptor, 0o600)
                with os.fdopen(descriptor, "r+", encoding="ascii", closefd=False) as state:
                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                    try:
                        raw = state.read(128)
                        if len(raw) == 128:
                            raise SecRateLimitError("SEC rate state is oversized")
                        now = self._now()
                        previous: float | None = None
                        if raw:
                            try:
                                previous = float(raw.strip())
                            except ValueError:
                                raise SecRateLimitError("SEC rate state is corrupt") from None
                            if not math.isfinite(previous) or previous < 0:
                                raise SecRateLimitError("SEC rate state is corrupt")
                            if previous > now:
                                raise SecRateLimitError("SEC rate state is in the future")
                        if previous is not None:
                            delay = previous + self.minimum_interval - now
                            if delay > 0:
                                self._sleeper(delay)
                                now = self._now()
                                if now + 0.000001 < previous + self.minimum_interval:
                                    raise SecRateLimitError(
                                        "SEC rate wait did not advance the clock"
                                    )
                        state.seek(0)
                        state.truncate()
                        state.write(f"{now:.9f}\n")
                        state.flush()
                        os.fsync(descriptor)
                    finally:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


class _GovernedTransport:
    def __init__(self, transport: GetTransport, governor: SecRateGovernor) -> None:
        self._transport = transport
        self._governor = governor

    def get(self, url: str, headers: Mapping[str, str]) -> HttpResponse:
        self._governor.wait()
        return self._transport.get(url, headers)


def _utc(value: datetime, name: str) -> datetime:
    return require_aware_timestamp(value, name).astimezone(UTC)


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise SecMetadataError("SEC acceptance timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise SecMetadataError("SEC acceptance timestamp is malformed") from None
    try:
        return _utc(parsed, "SEC acceptance timestamp")
    except ValueError:
        raise SecMetadataError("SEC acceptance timestamp is malformed") from None


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise SecMetadataError("SEC JSON contains duplicate fields")
        result[name] = value
    return result


def _json(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError):
        raise SecMetadataError("SEC submissions response is malformed") from None
    if not isinstance(value, dict):
        raise SecMetadataError("SEC submissions response is malformed")
    return value


def _identity_user_agent(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or value != value.strip()
        or not value.isascii()
        or not value.isprintable()
    ):
        raise ValueError("SEC User-Agent is malformed")
    matches = tuple(_EMAIL.finditer(value))
    identity = value
    for match in reversed(matches):
        identity = identity[: match.start()] + identity[match.end() :]
    if not matches or not any(character.isalpha() for character in identity):
        raise ValueError("SEC User-Agent needs application identity and contact email")
    return value


class SecClient:
    """Retrieve only official SEC submissions metadata and matched archive files."""

    def __init__(
        self,
        *,
        transport: GetTransport,
        cache: ContentCache,
        governor: SecRateGovernor,
        user_agent: str,
        submissions_origin: str = _SUBMISSIONS_ORIGIN,
        archives_origin: str = _ARCHIVES_ORIGIN,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if submissions_origin != _SUBMISSIONS_ORIGIN:
            raise NetworkPolicyError("SEC submissions origin must be exact")
        if archives_origin != _ARCHIVES_ORIGIN:
            raise NetworkPolicyError("SEC Archives origin must be exact")
        if not isinstance(cache, ContentCache) or not isinstance(
            governor, SecRateGovernor
        ):
            raise TypeError("SEC cache or governor has the wrong type")
        self._transport = _GovernedTransport(transport, governor)
        self._cache = cache
        self._user_agent = _identity_user_agent(user_agent)
        self._submissions_origin = submissions_origin
        self._archives_origin = archives_origin
        self._policy = EgressPolicy({"data.sec.gov", "www.sec.gov"})
        self._now = now
        self._archive_metadata: dict[str, tuple[datetime, str]] = {}
        self._archive_directory_metadata: dict[str, tuple[datetime, str]] = {}
        self._documents: dict[str, SourceDocument] = {}
        self._observations: dict[str, SourceObservation] = {}

    @staticmethod
    def _validate_submission_url(url: str) -> None:
        if not url.startswith(_SUBMISSIONS_ORIGIN):
            raise NetworkPolicyError("SEC submissions redirect left the pinned path")
        name = url[len(_SUBMISSIONS_ORIGIN) :]
        if not re.fullmatch(r"CIK[0-9]{10}\.json", name):
            raise NetworkPolicyError("SEC submissions URL is outside the approved shape")

    @staticmethod
    def _validate_archive_url(url: str) -> None:
        if not url.startswith(_ARCHIVES_ORIGIN):
            raise NetworkPolicyError("SEC Archives redirect left the pinned path")
        path = url[len(_ARCHIVES_ORIGIN) :]
        if (
            not _ARCHIVE_PATH.fullmatch(path)
            or not path.startswith("edgar/data/")
            or any(segment in {"", ".", ".."} for segment in path.split("/"))
        ):
            raise NetworkPolicyError("SEC Archives URL is outside the approved shape")

    def _current_time(self) -> datetime:
        return _utc(self._now(), "retrieval time")

    def _headers(self, *, json_response: bool) -> dict[str, str]:
        return {
            "User-Agent": self._user_agent,
            "Accept": "application/json" if json_response else "*/*",
        }

    def _pin(
        self,
        *,
        url: str,
        body: bytes,
        source_type: str,
        published_at: datetime | None,
        timestamp_source: str,
        accession: str | None = None,
    ) -> SourceDocument:
        retrieved_at = self._current_time()
        if published_at is not None and published_at > retrieved_at:
            raise SecMetadataError("SEC acceptance timestamp follows retrieval")
        digest = hashlib.sha256(body).hexdigest()
        identity = hashlib.sha256(
            source_type.encode("ascii")
            + b"\0"
            + url.encode("ascii")
            + b"\0"
            + retrieved_at.isoformat().encode("ascii")
            + b"\0"
            + body
        ).hexdigest()
        observation = SourceObservation(
            observation_id=f"obs-{identity[:24]}",
            url=url,
            source_type=source_type,
            source_timestamp=published_at or retrieved_at,
            retrieved_at=retrieved_at,
            feed="sec",
            delay_seconds=(
                max(0, int((retrieved_at - published_at).total_seconds()))
                if published_at is not None
                else 0
            ),
            content_hash=digest,
        )
        self._observations[observation.observation_id] = observation
        self._cache.put(observation, body)
        document = SourceDocument(
            url=url,
            published_at=published_at,
            retrieved_at=retrieved_at,
            content_hash=digest,
            body=body,
            source_observation_id=observation.observation_id,
            publisher="U.S. Securities and Exchange Commission",
            source_type=source_type,
            timestamp_source=timestamp_source,
            accession=accession,
        )
        self._documents[document.source_observation_id] = document
        return document

    def health_attestation(
        self,
        document: SourceDocument,
    ) -> SourceHealthAttestation:
        """Issue cache authority only for this client's successful SEC fetch."""
        if type(document) is not SourceDocument:
            raise TypeError("SEC health requires an exact source document")
        if self._documents.get(document.source_observation_id) is not document:
            raise ValueError("SEC document was not issued by this client")
        observation = self._observations.get(document.source_observation_id)
        if observation is None:
            raise ValueError("SEC document has no bound source observation")
        return _issue_provider_health_attestation(observation)

    def get_submission(self, cik: str) -> SourceDocument:
        if not isinstance(cik, str) or not cik.isascii() or not cik.isdigit():
            raise ValueError("SEC CIK must contain decimal digits")
        numeric = int(cik)
        if numeric <= 0 or numeric > 9_999_999_999:
            raise ValueError("SEC CIK is outside its supported range")
        padded = f"{numeric:010d}"
        url = f"{self._submissions_origin}CIK{padded}.json"

        def validate_submission_target(target: str) -> None:
            self._validate_submission_url(target)
            if target != url:
                raise NetworkPolicyError(
                    "SEC submissions redirect changed the requested CIK"
                )

        response = get_with_redirects(
            self._transport,
            self._policy,
            url,
            self._headers(json_response=True),
            allowed_content_types=("application/json",),
            exact_url_validator=validate_submission_target,
        )
        document = _json(response.body)
        if str(document.get("cik", "")).lstrip("0") != str(numeric):
            raise SecMetadataError("SEC submissions CIK does not match the request")
        filings = document.get("filings")
        recent = filings.get("recent") if isinstance(filings, dict) else None
        if not isinstance(recent, dict):
            raise SecMetadataError("SEC submissions recent-filing metadata is missing")
        required = (
            recent.get("accessionNumber"),
            recent.get("acceptanceDateTime"),
            recent.get("primaryDocument"),
        )
        if not all(isinstance(value, list) for value in required):
            raise SecMetadataError("SEC submissions filing arrays are malformed")
        accessions, accepted_values, primary_documents = required
        assert isinstance(accessions, list)
        assert isinstance(accepted_values, list)
        assert isinstance(primary_documents, list)
        if not (
            len(accessions) == len(accepted_values) == len(primary_documents)
        ):
            raise SecMetadataError("SEC submissions filing arrays disagree")
        accepted_times: list[datetime] = []
        staged_archives: dict[str, tuple[datetime, str]] = {}
        staged_directories: dict[str, tuple[datetime, str]] = {}
        for accession, accepted_value, primary_document in zip(
            accessions,
            accepted_values,
            primary_documents,
            strict=True,
        ):
            if (
                not isinstance(accession, str)
                or not _ACCESSION.fullmatch(accession)
                or accession[:10] != padded
                or not isinstance(primary_document, str)
                or not _PRIMARY_DOCUMENT.fullmatch(primary_document)
                or primary_document in {".", ".."}
            ):
                raise SecMetadataError("SEC submissions filing identity is malformed")
            accepted = _timestamp(accepted_value)
            accepted_times.append(accepted)
            compact = accession.replace("-", "")
            path = f"edgar/data/{numeric}/{compact}/{primary_document}"
            value = (accepted, accession)
            existing = self._archive_metadata.get(path)
            if existing is not None and existing != value:
                raise SecMetadataError("SEC submissions archive metadata conflicts")
            staged_existing = staged_archives.get(path)
            if staged_existing is not None and staged_existing != value:
                raise SecMetadataError("SEC submissions archive metadata conflicts")
            staged_archives[path] = value
            directory = f"edgar/data/{numeric}/{compact}/"
            existing_directory = self._archive_directory_metadata.get(directory)
            if existing_directory is not None and existing_directory != value:
                raise SecMetadataError("SEC submissions accession metadata conflicts")
            staged_directory = staged_directories.get(directory)
            if staged_directory is not None and staged_directory != value:
                raise SecMetadataError("SEC submissions accession metadata conflicts")
            staged_directories[directory] = value
        published_at = max(accepted_times) if accepted_times else None
        pinned = self._pin(
            url=response.url,
            body=response.body,
            source_type="SEC_SUBMISSIONS",
            published_at=published_at,
            timestamp_source=(
                "SEC_ACCEPTANCE_METADATA" if published_at is not None else "UNAVAILABLE"
            ),
        )
        self._archive_metadata.update(staged_archives)
        self._archive_directory_metadata.update(staged_directories)
        return pinned

    def get_archive(self, path: str) -> SourceDocument:
        if (
            not isinstance(path, str)
            or not _ARCHIVE_PATH.fullmatch(path)
            or not path.startswith("edgar/data/")
            or path.startswith("/")
            or any(segment in {"", ".", ".."} for segment in path.split("/"))
        ):
            raise ValueError("SEC archive path is malformed")
        metadata = self._archive_metadata.get(path)
        if metadata is None:
            metadata = next(
                (
                    value
                    for prefix, value in self._archive_directory_metadata.items()
                    if path.startswith(prefix)
                ),
                None,
            )
        if metadata is None:
            raise SecMetadataError(
                "SEC archive requires matching acceptance metadata from submissions"
            )
        published_at, accession = metadata
        url = self._archives_origin + path

        def validate_archive_target(target: str) -> None:
            self._validate_archive_url(target)
            if target != url:
                raise NetworkPolicyError(
                    "SEC Archives redirect changed the authorized filing identity"
                )

        response = get_with_redirects(
            self._transport,
            self._policy,
            url,
            self._headers(json_response=False),
            allowed_content_types=(
                "application/json",
                "application/pdf",
                "application/xml",
                "text/html",
                "text/plain",
                "text/xml",
            ),
            exact_url_validator=validate_archive_target,
        )
        return self._pin(
            url=response.url,
            body=response.body,
            source_type="SEC_ARCHIVE",
            published_at=published_at,
            timestamp_source="SEC_ACCEPTANCE_METADATA",
            accession=accession,
        )


__all__ = [
    "SecClient",
    "SecMetadataError",
    "SecRateGovernor",
    "SecRateLimitError",
    "SourceDocument",
]
