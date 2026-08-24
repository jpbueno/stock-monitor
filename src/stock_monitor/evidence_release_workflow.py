"""Prepare immutable, unreviewed evidence proposals for human review."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from stock_monitor import evidence as evidence_module
from stock_monitor.domain import require_aware_timestamp
from stock_monitor.evidence import (
    ADVERSE_TAGS,
    ETF_POSITIVE_EVENT_TYPES,
    POSITIVE_EVENT_TYPES,
    EvidenceCoverageAttestation,
    EvidenceRecord,
    EvidenceRegistryError,
    EvidenceSourceBinding,
    load_evidence_release,
)
from stock_monitor.evidence_authorities import (
    EVIDENCE_AUTHORITIES,
    EvidenceAuthority,
)
from stock_monitor.providers.cache import SourceDocument
from stock_monitor.providers.evidence_sources import ProposalSourceObservation
from stock_monitor.universe import UniverseRecord, UniverseSnapshot, load_current_universe


_CURRENT_RELEASE = Path("data/evidence/current.json")
_MAX_PARENT_RELEASE_BYTES = 4_194_304
_MAX_SOURCE_BYTES = 4_194_304
_MAX_REVIEW_INPUT_BYTES = 1_048_576
_MAX_PROPOSAL_BYTES = 1_048_576
_MAX_REVIEW_TEMPLATE_BYTES = 1_048_576
_MAX_SOURCE_ARTIFACT_BYTES = 6_000_000
_MAX_CANDIDATE_FILES = 4_096
_MAX_CANDIDATE_TOTAL_BYTES = 67_108_864
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}\Z")
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_DIRECTORY", 0)
)
_FILE_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_STABLE_STAT_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)
_STABLE_DIRECTORY_IDENTITY_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
)
_SAFE_REASON_CODES = frozenset(
    {
        "SEC_COLLECTOR_UNAVAILABLE",
        "SOURCE_COLLECTION_FAILED",
        "SOURCE_RESULT_INVALID",
    }
)
_EVENT_CLASSES = frozenset({"BINARY_EVENT", "ETF_ACTION"})
_PROPOSAL_FIELDS = {
    "created_at",
    "kind",
    "observations",
    "parent_release_sha256",
    "reviewer_template_sha256",
    "schema_version",
    "source_failures",
    "subjects",
    "universe_sha256",
}
_PROPOSAL_OBSERVATION_FIELDS = {
    "accession",
    "artifact_path",
    "content_sha256",
    "event_class",
    "issuer_cik",
    "observation_id",
    "published_at",
    "publisher",
    "retrieved_at",
    "role",
    "source_type",
    "symbol",
    "timestamp_source",
    "url",
}
_REVIEW_FIELDS = {
    "coverage_end",
    "coverage_start",
    "kind",
    "proposal_sha256",
    "review_by",
    "reviewed_at",
    "schema_version",
    "subjects",
    "universe_sha256",
}
_REVIEW_SUBJECT_FIELDS = {
    "coverage_attestations",
    "event_class",
    "issuer_cik",
    "records",
    "subject_kind",
    "symbol",
}
_REVIEW_RECORD_FIELDS = {
    "adverse_tags",
    "classification_ambiguous",
    "conflicts",
    "event_date",
    "event_kind",
    "event_type",
    "fact",
    "published_at",
    "source_observation_ids",
}
_REVIEW_COVERAGE_FIELDS = {
    "complete",
    "conflicts",
    "coverage",
    "event_class",
    "source_observation_ids",
}
_CANDIDATE_FIELDS = {
    "inventory",
    "kind",
    "parent_release_sha256",
    "proposal_sha256",
    "release_sha256",
    "review_input_sha256",
    "schema_version",
    "universe_sha256",
}
_CANDIDATE_INVENTORY_FIELDS = {"path", "sha256"}


class EvidenceWorkflowError(RuntimeError):
    """The evidence workflow could not safely prepare an immutable proposal."""


@dataclass(frozen=True, slots=True)
class EvidenceProposalSummary:
    status: str
    proposal_sha256: str
    universe_sha256: str
    parent_release_sha256: str
    symbols: tuple[str, ...]
    reason_codes: tuple[str, ...]
    proposal_path: Path


@dataclass(frozen=True, slots=True)
class EvidenceCandidateSummary:
    status: str
    candidate_sha256: str
    proposal_sha256: str
    review_input_sha256: str
    release_sha256: str
    universe_sha256: str
    reviewed_at: datetime
    review_by: datetime
    symbols: tuple[str, ...]
    coverage: tuple[tuple[str, str, str], ...]
    reason_codes: tuple[str, ...]
    candidate_path: Path


@dataclass(frozen=True, slots=True)
class EvidenceInstallSummary:
    status: str
    candidate_sha256: str
    release_sha256: str
    installed_at: datetime
    symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _CollectedObservation:
    observation_id: str
    symbol: str
    issuer_cik: str | None
    url: str
    publisher: str
    role: str
    event_class: str
    retrieved_at: datetime
    published_at: datetime | None
    timestamp_source: str
    content_sha256: str
    source_type: str
    accession: str | None
    body: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class _VerifiedProposal:
    created_at: datetime
    parent_release_sha256: str
    universe_sha256: str
    subjects: tuple[dict[str, object], ...]
    observations: tuple[_CollectedObservation, ...]
    artifact_payloads: dict[str, bytes] = field(repr=False)


@dataclass(frozen=True, slots=True)
class _CompiledCandidate:
    review_input_sha256: str
    reviewed_at: datetime
    review_by: datetime
    release_sha256: str
    release_files: dict[str, bytes] = field(repr=False)
    symbols: tuple[str, ...]
    coverage: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True, slots=True)
class _LoadedCandidate:
    candidate_sha256: str
    parent_release_sha256: str
    release_sha256: str
    universe_sha256: str
    release_files: dict[str, bytes] = field(repr=False)


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError):
        raise EvidenceWorkflowError("evidence proposal is not canonical") from None


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _safe_source_key(authority: EvidenceAuthority) -> str:
    identity = hashlib.sha256(
        b"\0".join(
            (
                authority.symbol.encode("ascii"),
                authority.role.encode("ascii"),
                authority.requested_url.encode("ascii"),
            )
        )
    ).hexdigest()
    return f"source-{identity[:24]}"


def _normalized_time(value: object, name: str) -> datetime:
    try:
        return require_aware_timestamp(value, name).astimezone(UTC)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        raise ValueError("source timestamp is invalid") from None


def _valid_body(body: object, digest: object) -> bytes:
    if (
        not isinstance(body, bytes)
        or not body
        or len(body) > _MAX_SOURCE_BYTES
        or not isinstance(digest, str)
        or _SHA256.fullmatch(digest) is None
        or hashlib.sha256(body).hexdigest() != digest
    ):
        raise ValueError("source body is invalid")
    return body


def _valid_observation_id(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("source observation ID is invalid")
    return value


def _validate_generic_result(
    authority: EvidenceAuthority,
    result: object,
) -> _CollectedObservation:
    if type(result) is not ProposalSourceObservation:
        raise ValueError("generic source result has the wrong type")
    if (
        result.symbol != authority.symbol
        or result.issuer_cik != authority.issuer_cik
        or result.url not in authority.allowed_final_urls
        or result.publisher != authority.publisher
        or result.role != authority.role
        or result.event_class != authority.event_class
        or result.timestamp_source != "UNAVAILABLE"
        or result.published_at is not None
    ):
        raise ValueError("generic source result is outside its authority")
    retrieved_at = _normalized_time(result.retrieved_at, "source retrieved_at")
    body = _valid_body(result.body, result.content_sha256)
    return _CollectedObservation(
        observation_id=_valid_observation_id(result.observation_id),
        symbol=authority.symbol,
        issuer_cik=authority.issuer_cik,
        url=result.url,
        publisher=authority.publisher,
        role=authority.role,
        event_class=authority.event_class,
        retrieved_at=retrieved_at,
        published_at=None,
        timestamp_source="UNAVAILABLE",
        content_sha256=result.content_sha256,
        source_type="OFFICIAL_REFERENCE",
        accession=None,
        body=body,
    )


def _validate_sec_result(
    authority: EvidenceAuthority,
    result: object,
) -> _CollectedObservation:
    if type(result) is not SourceDocument:
        raise ValueError("SEC source result has the wrong type")
    expected_timestamp_source = (
        "SEC_SUBMISSIONS_METADATA"
        if result.published_at is not None
        else "UNAVAILABLE"
    )
    if (
        authority.issuer_cik is None
        or result.url not in authority.allowed_final_urls
        or result.publisher != authority.publisher
        or result.source_type != "SEC_SUBMISSIONS"
        or result.timestamp_source != expected_timestamp_source
        or result.accession is not None
        or result.source_role is not None
    ):
        raise ValueError("SEC source result is outside its authority")
    retrieved_at = _normalized_time(result.retrieved_at, "SEC retrieved_at")
    published_at = (
        _normalized_time(result.published_at, "SEC published_at")
        if result.published_at is not None
        else None
    )
    if published_at is not None and published_at > retrieved_at:
        raise ValueError("SEC source publication follows retrieval")
    body = _valid_body(result.body, result.content_hash)
    return _CollectedObservation(
        observation_id=_valid_observation_id(result.source_observation_id),
        symbol=authority.symbol,
        issuer_cik=authority.issuer_cik,
        url=result.url,
        publisher=authority.publisher,
        role=authority.role,
        event_class=authority.event_class,
        retrieved_at=retrieved_at,
        published_at=published_at,
        timestamp_source=result.timestamp_source,
        content_sha256=result.content_hash,
        source_type="SEC_SUBMISSIONS",
        accession=None,
        body=body,
    )


def _collect_proposal_sources(
    *,
    universe: UniverseSnapshot,
    collect: Callable[[EvidenceAuthority], ProposalSourceObservation],
    collect_sec: Callable[[str], SourceDocument] | None,
) -> tuple[tuple[_CollectedObservation, ...], tuple[dict[str, str], ...]]:
    eligible_symbols = {record.symbol for record in universe.eligible_records()}
    catalog_symbols = {authority.symbol for authority in EVIDENCE_AUTHORITIES}
    if eligible_symbols != catalog_symbols:
        raise EvidenceWorkflowError("evidence authority catalog is incomplete")

    observations: list[_CollectedObservation] = []
    failures: list[dict[str, str]] = []
    identities: dict[str, _CollectedObservation] = {}
    for authority in EVIDENCE_AUTHORITIES:
        reason_code: str | None = None
        result: object
        if authority.role.startswith("SEC_SUBMISSIONS:"):
            if collect_sec is None:
                reason_code = "SEC_COLLECTOR_UNAVAILABLE"
                result = None
            else:
                try:
                    result = collect_sec(authority.issuer_cik or "")
                except Exception:
                    reason_code = "SOURCE_COLLECTION_FAILED"
                    result = None
            validator = _validate_sec_result
        else:
            try:
                result = collect(authority)
            except Exception:
                reason_code = "SOURCE_COLLECTION_FAILED"
                result = None
            validator = _validate_generic_result

        observation: _CollectedObservation | None = None
        if reason_code is None:
            try:
                observation = validator(authority, result)
                prior = identities.get(observation.observation_id)
                if prior is not None and prior != observation:
                    raise ValueError("source observation identity collision")
            except (TypeError, ValueError):
                reason_code = "SOURCE_RESULT_INVALID"

        if reason_code is not None:
            if reason_code not in _SAFE_REASON_CODES:
                raise EvidenceWorkflowError("evidence source failure is unsafe")
            failures.append(
                {
                    "reason_code": reason_code,
                    "source_key": _safe_source_key(authority),
                }
            )
            continue
        assert observation is not None
        identities[observation.observation_id] = observation
        observations.append(observation)

    failures.sort(key=lambda item: item["source_key"])
    return tuple(observations), tuple(failures)


def _subject_document(record: UniverseRecord) -> dict[str, object]:
    return {
        "event_class": (
            "BINARY_EVENT" if record.product_type == "common_stock" else "ETF_ACTION"
        ),
        "issuer_cik": record.issuer_cik,
        "subject_kind": "STOCK" if record.product_type == "common_stock" else "ETF",
        "symbol": record.symbol,
    }


def _review_subject(record: UniverseRecord) -> dict[str, object]:
    subject = _subject_document(record)
    relevant = subject["event_class"]
    opposite = "ETF_ACTION" if relevant == "BINARY_EVENT" else "BINARY_EVENT"
    return {
        "coverage_attestations": [
            {
                "complete": False,
                "coverage": "UNKNOWN",
                "event_class": relevant,
            },
            {
                "complete": True,
                "coverage": "NOT_APPLICABLE",
                "event_class": opposite,
            },
        ],
        "event_class": relevant,
        "issuer_cik": subject["issuer_cik"],
        "records": [],
        "subject_kind": subject["subject_kind"],
        "symbol": subject["symbol"],
    }


def _observation_document(value: _CollectedObservation) -> dict[str, object]:
    return {
        "accession": value.accession,
        "artifact_path": f"artifacts/{value.content_sha256}.json",
        "content_sha256": value.content_sha256,
        "event_class": value.event_class,
        "issuer_cik": value.issuer_cik,
        "observation_id": value.observation_id,
        "published_at": (
            _utc_text(value.published_at) if value.published_at is not None else None
        ),
        "publisher": value.publisher,
        "retrieved_at": _utc_text(value.retrieved_at),
        "role": value.role,
        "source_type": value.source_type,
        "symbol": value.symbol,
        "timestamp_source": value.timestamp_source,
        "url": value.url,
    }


def _artifact_document(value: _CollectedObservation) -> dict[str, object]:
    return {
        "body": base64.b64encode(value.body).decode("ascii"),
        "content_sha256": value.content_sha256,
        "encoding": "base64",
        "kind": "RAW_SOURCE_ARTIFACT",
        "schema_version": 1,
    }


def _build_proposal_documents(
    *,
    current: datetime,
    universe: UniverseSnapshot,
    parent_sha256: str,
    observations: tuple[_CollectedObservation, ...],
    failures: tuple[dict[str, str], ...],
) -> tuple[dict[str, object], dict[str, object], dict[str, bytes]]:
    eligible = tuple(sorted(universe.eligible_records(), key=lambda item: item.symbol))
    template: dict[str, object] = {
        "kind": "EVIDENCE_REVIEW_INPUT_TEMPLATE",
        "schema_version": 1,
        "subjects": [_review_subject(record) for record in eligible],
    }
    template_sha256 = hashlib.sha256(_canonical_bytes(template)).hexdigest()
    artifacts: dict[str, bytes] = {}
    for observation in observations:
        artifact_bytes = _canonical_bytes(_artifact_document(observation))
        existing = artifacts.get(observation.content_sha256)
        if existing is not None and existing != artifact_bytes:
            raise EvidenceWorkflowError("source artifact digest collision")
        artifacts[observation.content_sha256] = artifact_bytes

    proposal: dict[str, object] = {
        "created_at": _utc_text(current),
        "kind": "UNREVIEWED_EVIDENCE_PROPOSAL",
        "observations": [_observation_document(value) for value in observations],
        "parent_release_sha256": parent_sha256,
        "reviewer_template_sha256": template_sha256,
        "schema_version": 1,
        "source_failures": list(failures),
        "subjects": [_subject_document(record) for record in eligible],
        "universe_sha256": universe._release_pin,
    }
    return proposal, template, artifacts


def _validate_input_directory(details: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_mode & 0o022
    ):
        raise EvidenceWorkflowError("evidence input directory is unsafe")


def _read_stable_regular_descriptor(
    descriptor: int,
    *,
    maximum_bytes: int,
    private: bool,
) -> bytes:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_uid != os.getuid()
        or before.st_mode & 0o022
        or (private and stat.S_IMODE(before.st_mode) != 0o600)
        or before.st_size <= 0
        or before.st_size > maximum_bytes
    ):
        raise EvidenceWorkflowError("evidence input file is unsafe")
    chunks: list[bytes] = []
    remaining = before.st_size
    try:
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1_048_576))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    except OSError:
        raise EvidenceWorkflowError("evidence input file could not be read") from None
    after = os.fstat(descriptor)
    if remaining or any(
        getattr(before, field) != getattr(after, field)
        for field in _STABLE_STAT_FIELDS
    ):
        raise EvidenceWorkflowError("evidence input file changed during read")
    return b"".join(chunks)


def _secure_confined_bytes(
    root: Path,
    relative: tuple[str, ...],
    *,
    maximum_bytes: int,
) -> bytes:
    descriptors: list[int] = []
    try:
        root_before = root.lstat()
        current = os.open(root, _DIRECTORY_OPEN_FLAGS)
        descriptors.append(current)
        root_opened = os.fstat(current)
        _validate_input_directory(root_opened)
        if (
            root_before.st_dev != root_opened.st_dev
            or root_before.st_ino != root_opened.st_ino
        ):
            raise EvidenceWorkflowError("evidence input root changed during open")
        for component in relative[:-1]:
            current = os.open(
                component,
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=current,
            )
            descriptors.append(current)
            _validate_input_directory(os.fstat(current))
        leaf = os.open(relative[-1], _FILE_OPEN_FLAGS, dir_fd=current)
        descriptors.append(leaf)
        return _read_stable_regular_descriptor(
            leaf,
            maximum_bytes=maximum_bytes,
            private=False,
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("evidence parent release is unavailable") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _current_release_digest(project_root: Path) -> str:
    payload = _secure_confined_bytes(
        project_root,
        _CURRENT_RELEASE.parts,
        maximum_bytes=_MAX_PARENT_RELEASE_BYTES,
    )
    return hashlib.sha256(payload).hexdigest()


def _validate_root(path: Path, *, private: bool) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise EvidenceWorkflowError("evidence workflow root must be absolute")
    try:
        if path.resolve(strict=True) != path:
            raise EvidenceWorkflowError("evidence workflow root is indirect")
        details = path.lstat()
    except OSError:
        raise EvidenceWorkflowError("evidence workflow root is unavailable") from None
    if (
        not stat.S_ISDIR(details.st_mode)
        or stat.S_ISLNK(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_mode & 0o022
        or (private and stat.S_IMODE(details.st_mode) != 0o700)
    ):
        raise EvidenceWorkflowError("evidence workflow root is unsafe")
    return path


def _proposal_parent(state_root: Path) -> Path:
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    try:
        state_descriptor, parent_descriptor = _open_proposal_parent(state_root)
        fcntl.flock(parent_descriptor, fcntl.LOCK_EX)
        _verify_parent_binding(state_root, state_descriptor, parent_descriptor)
        return state_root / "evidence-proposals"
    finally:
        if parent_descriptor is not None:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)


def _validate_private_directory_details(details: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != 0o700
    ):
        raise EvidenceWorkflowError("evidence proposal directory is unsafe")


def _open_proposal_parent(state_root: Path) -> tuple[int, int]:
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    try:
        root_before = state_root.lstat()
        state_descriptor = os.open(state_root, _DIRECTORY_OPEN_FLAGS)
        root_opened = os.fstat(state_descriptor)
        _validate_private_directory_details(root_opened)
        if (
            root_before.st_dev != root_opened.st_dev
            or root_before.st_ino != root_opened.st_ino
        ):
            raise EvidenceWorkflowError("evidence state root changed during open")
        created = False
        try:
            os.mkdir("evidence-proposals", 0o700, dir_fd=state_descriptor)
            created = True
            os.fsync(state_descriptor)
        except FileExistsError:
            pass
        parent_descriptor = os.open(
            "evidence-proposals",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=state_descriptor,
        )
        if created:
            os.fchmod(parent_descriptor, 0o700)
        _validate_private_directory_details(os.fstat(parent_descriptor))
        return state_descriptor, parent_descriptor
    except EvidenceWorkflowError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise
    except OSError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise EvidenceWorkflowError("evidence proposal storage is unavailable") from None


def _verify_parent_binding(
    state_root: Path,
    state_descriptor: int,
    parent_descriptor: int,
) -> None:
    opened_root = os.fstat(state_descriptor)
    opened_parent = os.fstat(parent_descriptor)
    _validate_private_directory_details(opened_root)
    _validate_private_directory_details(opened_parent)
    try:
        path_root = state_root.lstat()
        linked_parent = os.stat(
            "evidence-proposals",
            dir_fd=state_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError("evidence proposal path binding changed") from None
    if (
        not stat.S_ISDIR(path_root.st_mode)
        or path_root.st_dev != opened_root.st_dev
        or path_root.st_ino != opened_root.st_ino
        or not stat.S_ISDIR(linked_parent.st_mode)
        or linked_parent.st_dev != opened_parent.st_dev
        or linked_parent.st_ino != opened_parent.st_ino
        or linked_parent.st_uid != opened_parent.st_uid
        or stat.S_IMODE(linked_parent.st_mode) != 0o700
    ):
        raise EvidenceWorkflowError("evidence proposal path binding changed")


def _write_proposal_tree(
    *,
    state_root: Path,
    proposal: dict[str, object],
    review_template: dict[str, object],
    artifacts: dict[str, bytes],
) -> tuple[str, Path]:
    proposal_bytes = _canonical_bytes(proposal)
    template_bytes = _canonical_bytes(review_template)
    proposal_sha256 = hashlib.sha256(proposal_bytes).hexdigest()
    expected_files = {
        "proposal.json": proposal_bytes,
        "review-template.json": template_bytes,
    }
    for digest, payload in artifacts.items():
        if _SHA256.fullmatch(digest) is None:
            raise EvidenceWorkflowError("source artifact digest is invalid")
        expected_files[f"artifacts/{digest}.json"] = payload

    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    temporary_name: str | None = None
    temporary_identity: tuple[int, int] | None = None
    published = False
    try:
        state_descriptor, parent_descriptor = _open_proposal_parent(state_root)
        fcntl.flock(parent_descriptor, fcntl.LOCK_EX)
        if _entry_exists(parent_descriptor, proposal_sha256):
            _verify_proposal_tree(
                parent_descriptor,
                proposal_sha256,
                expected_files,
            )
            _verify_parent_binding(
                state_root,
                state_descriptor,
                parent_descriptor,
            )
            return (
                proposal_sha256,
                state_root
                / "evidence-proposals"
                / proposal_sha256
                / "proposal.json",
            )

        temporary_name, temporary_descriptor = _create_private_directory(
            parent_descriptor,
            prefix=f".{proposal_sha256}.",
        )
        try:
            temporary_details = os.fstat(temporary_descriptor)
            temporary_identity = (
                temporary_details.st_dev,
                temporary_details.st_ino,
            )
            os.mkdir("artifacts", 0o700, dir_fd=temporary_descriptor)
            artifacts_descriptor = os.open(
                "artifacts",
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=temporary_descriptor,
            )
            try:
                os.fchmod(artifacts_descriptor, 0o700)
                _validate_private_directory_details(os.fstat(artifacts_descriptor))
                for relative, payload in sorted(expected_files.items()):
                    if relative.startswith("artifacts/"):
                        _atomic_write_at(
                            artifacts_descriptor,
                            relative.removeprefix("artifacts/"),
                            payload,
                        )
                    else:
                        _atomic_write_at(temporary_descriptor, relative, payload)
                os.fsync(artifacts_descriptor)
            finally:
                os.close(artifacts_descriptor)
            os.fsync(temporary_descriptor)
        finally:
            os.close(temporary_descriptor)

        if _entry_exists(parent_descriptor, proposal_sha256):
            _verify_proposal_tree(
                parent_descriptor,
                proposal_sha256,
                expected_files,
            )
            _remove_tree_at(parent_descriptor, temporary_name)
            temporary_name = None
        else:
            os.replace(
                temporary_name,
                proposal_sha256,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            temporary_name = None
            published = True
            destination = os.stat(
                proposal_sha256,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            if temporary_identity != (destination.st_dev, destination.st_ino):
                raise EvidenceWorkflowError("evidence proposal publication changed identity")
            os.fsync(parent_descriptor)

        _verify_proposal_tree(
            parent_descriptor,
            proposal_sha256,
            expected_files,
        )
        _verify_parent_binding(
            state_root,
            state_descriptor,
            parent_descriptor,
        )
        return (
            proposal_sha256,
            state_root
            / "evidence-proposals"
            / proposal_sha256
            / "proposal.json",
        )
    except EvidenceWorkflowError:
        if parent_descriptor is not None and temporary_name is not None:
            _remove_tree_at(parent_descriptor, temporary_name)
        if parent_descriptor is not None and published and temporary_identity is not None:
            _remove_matching_tree_at(
                parent_descriptor,
                proposal_sha256,
                temporary_identity,
            )
        raise
    except Exception:
        if parent_descriptor is not None and temporary_name is not None:
            _remove_tree_at(parent_descriptor, temporary_name)
        if parent_descriptor is not None and published and temporary_identity is not None:
            _remove_matching_tree_at(
                parent_descriptor,
                proposal_sha256,
                temporary_identity,
            )
        raise EvidenceWorkflowError("evidence proposal storage failed") from None
    finally:
        if parent_descriptor is not None:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)


def _entry_exists(directory_descriptor: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        raise EvidenceWorkflowError("evidence proposal entry is unsafe") from None
    return True


def _create_private_directory(
    parent_descriptor: int,
    *,
    prefix: str,
) -> tuple[str, int]:
    for _ in range(128):
        name = f"{prefix}{secrets.token_hex(16)}.tmp"
        created = False
        descriptor: int | None = None
        try:
            os.mkdir(name, 0o700, dir_fd=parent_descriptor)
            created = True
        except FileExistsError:
            continue
        try:
            descriptor = os.open(
                name,
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=parent_descriptor,
            )
            os.fchmod(descriptor, 0o700)
            _validate_private_directory_details(os.fstat(descriptor))
            return name, descriptor
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            if created:
                try:
                    os.rmdir(name, dir_fd=parent_descriptor)
                    os.fsync(parent_descriptor)
                except OSError:
                    pass
            raise
    raise EvidenceWorkflowError("evidence proposal temporary storage is unavailable")


def _atomic_write_at(
    directory_descriptor: int,
    name: str,
    payload: bytes,
) -> None:
    temporary_name = f".{name}.{secrets.token_hex(16)}.tmp"
    descriptor: int | None = None
    temporary_exists = False
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_descriptor,
        )
        temporary_exists = True
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("short proposal write")
            written += count
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
        temporary_exists = False
        os.fsync(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_exists:
            try:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            except OSError:
                pass


def _directory_names(descriptor: int) -> frozenset[str]:
    try:
        with os.scandir(descriptor) as entries:
            return frozenset(entry.name for entry in entries)
    except OSError:
        raise EvidenceWorkflowError("evidence proposal directory could not be read") from None


def _stable_directory(before: os.stat_result, after: os.stat_result) -> None:
    if any(
        getattr(before, field) != getattr(after, field)
        for field in _STABLE_STAT_FIELDS
    ):
        raise EvidenceWorkflowError("evidence proposal directory changed during read")


def _stable_review_input_directory(
    before: os.stat_result,
    after: os.stat_result,
) -> None:
    if any(
        getattr(before, field) != getattr(after, field)
        for field in _STABLE_DIRECTORY_IDENTITY_FIELDS
    ):
        raise EvidenceWorkflowError(
            "evidence review input directory changed during read"
        )


def _verify_review_input_leaf_binding(
    directory_descriptor: int,
    name: str,
    opened_leaf: os.stat_result,
) -> None:
    try:
        linked_leaf = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError(
            "evidence review input path binding changed"
        ) from None
    if (
        linked_leaf.st_dev != opened_leaf.st_dev
        or linked_leaf.st_ino != opened_leaf.st_ino
    ):
        raise EvidenceWorkflowError("evidence review input path binding changed")


def _verify_review_input_directory_binding(
    parent_descriptor: int,
    name: str,
    opened_directory: os.stat_result,
) -> None:
    try:
        linked_directory = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError(
            "evidence review input path binding changed"
        ) from None
    if not stat.S_ISDIR(linked_directory.st_mode) or any(
        getattr(linked_directory, field) != getattr(opened_directory, field)
        for field in _STABLE_DIRECTORY_IDENTITY_FIELDS
    ):
        raise EvidenceWorkflowError("evidence review input path binding changed")


def _read_private_at(
    directory_descriptor: int,
    name: str,
    expected: bytes,
) -> None:
    try:
        descriptor = os.open(name, _FILE_OPEN_FLAGS, dir_fd=directory_descriptor)
    except OSError:
        raise EvidenceWorkflowError("evidence proposal file is unsafe") from None
    try:
        actual = _read_stable_regular_descriptor(
            descriptor,
            maximum_bytes=len(expected),
            private=True,
        )
        if actual != expected:
            raise EvidenceWorkflowError("evidence proposal content collision")
    finally:
        os.close(descriptor)


def _verify_proposal_tree(
    parent_descriptor: int,
    root_name: str,
    expected_files: dict[str, bytes],
) -> None:
    try:
        root_descriptor = os.open(
            root_name,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_descriptor,
        )
    except OSError:
        raise EvidenceWorkflowError("evidence proposal tree is unsafe") from None
    artifacts_descriptor: int | None = None
    try:
        root_before = os.fstat(root_descriptor)
        _validate_private_directory_details(root_before)
        try:
            artifacts_descriptor = os.open(
                "artifacts",
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=root_descriptor,
            )
        except OSError:
            raise EvidenceWorkflowError("evidence proposal tree is unsafe") from None
        artifacts_before = os.fstat(artifacts_descriptor)
        _validate_private_directory_details(artifacts_before)

        root_files = {
            relative: payload
            for relative, payload in expected_files.items()
            if not relative.startswith("artifacts/")
        }
        artifact_files = {
            relative.removeprefix("artifacts/"): payload
            for relative, payload in expected_files.items()
            if relative.startswith("artifacts/")
        }
        expected_root_names = {
            "artifacts",
            "proposal.json",
            "review-template.json",
        }
        expected_artifact_names = frozenset(artifact_files)
        if (
            _directory_names(root_descriptor) != expected_root_names
            or _directory_names(artifacts_descriptor) != expected_artifact_names
        ):
            raise EvidenceWorkflowError("evidence proposal tree content collision")
        for name, expected in root_files.items():
            _read_private_at(root_descriptor, name, expected)
        for name, expected in artifact_files.items():
            _read_private_at(artifacts_descriptor, name, expected)
        if (
            _directory_names(root_descriptor) != expected_root_names
            or _directory_names(artifacts_descriptor) != expected_artifact_names
        ):
            raise EvidenceWorkflowError("evidence proposal tree content collision")
        _stable_directory(root_before, os.fstat(root_descriptor))
        _stable_directory(artifacts_before, os.fstat(artifacts_descriptor))
    finally:
        if artifacts_descriptor is not None:
            os.close(artifacts_descriptor)
        os.close(root_descriptor)


def _remove_tree_at(parent_descriptor: int, name: str) -> None:
    try:
        root_descriptor = os.open(
            name,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_descriptor,
        )
    except OSError:
        return
    try:
        try:
            artifacts_descriptor = os.open(
                "artifacts",
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=root_descriptor,
            )
        except OSError:
            artifacts_descriptor = None
        if artifacts_descriptor is not None:
            try:
                for child in _directory_names(artifacts_descriptor):
                    os.unlink(child, dir_fd=artifacts_descriptor)
                os.fsync(artifacts_descriptor)
            finally:
                os.close(artifacts_descriptor)
            os.rmdir("artifacts", dir_fd=root_descriptor)
        for child in _directory_names(root_descriptor):
            os.unlink(child, dir_fd=root_descriptor)
        os.fsync(root_descriptor)
    except (EvidenceWorkflowError, OSError):
        return
    finally:
        os.close(root_descriptor)
    try:
        os.rmdir(name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    except OSError:
        pass


def _remove_matching_tree_at(
    parent_descriptor: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    try:
        details = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except OSError:
        return
    if (details.st_dev, details.st_ino) == identity:
        _remove_tree_at(parent_descriptor, name)


def prepare_evidence_proposal(
    *,
    project_root: Path,
    state_root: Path,
    as_of: datetime,
    collect: Callable[[EvidenceAuthority], ProposalSourceObservation],
    collect_sec: Callable[[str], SourceDocument] | None = None,
) -> EvidenceProposalSummary:
    """Collect exact official-source bytes into an unreviewed proposal tree."""
    project_root = _validate_root(project_root, private=False)
    state_root = _validate_root(state_root, private=True)
    _proposal_parent(state_root)
    if not callable(collect) or (collect_sec is not None and not callable(collect_sec)):
        raise EvidenceWorkflowError("evidence collectors are invalid")
    try:
        current = require_aware_timestamp(as_of, "proposal as_of").astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        raise EvidenceWorkflowError("evidence proposal time is invalid") from None
    try:
        universe = load_current_universe(project_root, as_of=current.date())
    except (OSError, TypeError, ValueError):
        raise EvidenceWorkflowError("evidence universe is invalid") from None
    parent_sha256 = _current_release_digest(project_root)
    observations, failures = _collect_proposal_sources(
        universe=universe,
        collect=collect,
        collect_sec=collect_sec,
    )
    proposal, template, artifacts = _build_proposal_documents(
        current=current,
        universe=universe,
        parent_sha256=parent_sha256,
        observations=observations,
        failures=failures,
    )
    proposal_sha256, proposal_path = _write_proposal_tree(
        state_root=state_root,
        proposal=proposal,
        review_template=template,
        artifacts=artifacts,
    )
    eligible = tuple(sorted(universe.eligible_records(), key=lambda item: item.symbol))
    reason_codes = tuple(sorted({item["reason_code"] for item in failures}))
    return EvidenceProposalSummary(
        status="PREPARED_BLOCKED" if failures else "PREPARED",
        proposal_sha256=proposal_sha256,
        universe_sha256=universe._release_pin,
        parent_release_sha256=parent_sha256,
        symbols=tuple(record.symbol for record in eligible),
        reason_codes=reason_codes,
        proposal_path=proposal_path,
    )


def _strict_workflow_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise EvidenceWorkflowError("evidence document contains duplicate fields")
        result[name] = value
    return result


def _canonical_document(payload: bytes, name: str) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_strict_workflow_object)
    except EvidenceWorkflowError:
        raise
    except (UnicodeError, json.JSONDecodeError):
        raise EvidenceWorkflowError(f"{name} JSON is malformed") from None
    if not isinstance(value, dict) or _canonical_bytes(value) != payload:
        raise EvidenceWorkflowError(f"{name} is not canonical JSON")
    return value


def _workflow_timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceWorkflowError(f"{name} is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        normalized = require_aware_timestamp(parsed, name).astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        raise EvidenceWorkflowError(f"{name} is malformed") from None
    if _utc_text(normalized) != value:
        raise EvidenceWorkflowError(f"{name} is not canonical UTC")
    return normalized


def _workflow_optional_timestamp(
    value: object,
    name: str,
) -> datetime | None:
    if value is None:
        return None
    return _workflow_timestamp(value, name)


def _workflow_date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise EvidenceWorkflowError(f"{name} is malformed")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise EvidenceWorkflowError(f"{name} is malformed") from None
    if parsed.isoformat() != value:
        raise EvidenceWorkflowError(f"{name} is malformed")
    return parsed


def _read_private_bytes_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int,
) -> bytes:
    try:
        descriptor = os.open(name, _FILE_OPEN_FLAGS, dir_fd=directory_descriptor)
    except OSError:
        raise EvidenceWorkflowError("evidence workflow file is unsafe") from None
    try:
        return _read_stable_regular_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
            private=True,
        )
    finally:
        os.close(descriptor)


def _read_private_absolute_file(path: Path, *, maximum_bytes: int) -> bytes:
    if not isinstance(path, Path) or not path.is_absolute():
        raise EvidenceWorkflowError("evidence review input path must be absolute")
    parts = path.parts
    if not parts or parts[0] != os.sep or any(
        component in {"", ".", ".."} for component in parts[1:]
    ):
        raise EvidenceWorkflowError("evidence review input path is unsafe")
    descriptors: list[int] = []
    directory_details: list[os.stat_result] = []
    directory_bindings: list[tuple[int, str, os.stat_result]] = []
    try:
        current = os.open(os.sep, _DIRECTORY_OPEN_FLAGS)
        descriptors.append(current)
        directory_details.append(os.fstat(current))
        for component in parts[1:-1]:
            parent = current
            current = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=parent)
            descriptors.append(current)
            details = os.fstat(current)
            if not stat.S_ISDIR(details.st_mode):
                raise EvidenceWorkflowError(
                    "evidence review input directory is unsafe"
                )
            directory_details.append(details)
            directory_bindings.append((parent, component, details))
        leaf = os.open(parts[-1], _FILE_OPEN_FLAGS, dir_fd=current)
        descriptors.append(leaf)
        payload = _read_stable_regular_descriptor(
            leaf,
            maximum_bytes=maximum_bytes,
            private=True,
        )
        opened_leaf = os.fstat(leaf)
        _verify_review_input_leaf_binding(current, parts[-1], opened_leaf)
        for before, descriptor in zip(
            directory_details,
            descriptors[: len(directory_details)],
            strict=True,
        ):
            _stable_review_input_directory(before, os.fstat(descriptor))
        _verify_review_input_leaf_binding(current, parts[-1], opened_leaf)
        for parent, component, opened_directory in directory_bindings:
            _verify_review_input_directory_binding(
                parent,
                component,
                opened_directory,
            )
        return payload
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("evidence review input is unavailable") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _expected_review_template(
    subjects: tuple[dict[str, object], ...],
) -> dict[str, object]:
    values: list[dict[str, object]] = []
    for subject in subjects:
        relevant = subject["event_class"]
        opposite = "ETF_ACTION" if relevant == "BINARY_EVENT" else "BINARY_EVENT"
        values.append(
            {
                "coverage_attestations": [
                    {
                        "complete": False,
                        "coverage": "UNKNOWN",
                        "event_class": relevant,
                    },
                    {
                        "complete": True,
                        "coverage": "NOT_APPLICABLE",
                        "event_class": opposite,
                    },
                ],
                "event_class": relevant,
                "issuer_cik": subject["issuer_cik"],
                "records": [],
                "subject_kind": subject["subject_kind"],
                "symbol": subject["symbol"],
            }
        )
    return {
        "kind": "EVIDENCE_REVIEW_INPUT_TEMPLATE",
        "schema_version": 1,
        "subjects": values,
    }


def _decode_artifact(
    payload: bytes,
    *,
    expected_sha256: str,
) -> bytes:
    document = _canonical_document(payload, "evidence proposal artifact")
    if (
        set(document)
        != {"body", "content_sha256", "encoding", "kind", "schema_version"}
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["kind"] != "RAW_SOURCE_ARTIFACT"
        or document["encoding"] != "base64"
        or document["content_sha256"] != expected_sha256
        or not isinstance(document["body"], str)
        or not document["body"].isascii()
    ):
        raise EvidenceWorkflowError("evidence proposal artifact schema is invalid")
    try:
        body = base64.b64decode(document["body"], validate=True)
    except (ValueError, TypeError):
        raise EvidenceWorkflowError("evidence proposal artifact is invalid") from None
    if (
        not body
        or len(body) > _MAX_SOURCE_BYTES
        or base64.b64encode(body).decode("ascii") != document["body"]
        or hashlib.sha256(body).hexdigest() != expected_sha256
    ):
        raise EvidenceWorkflowError("evidence proposal artifact is invalid")
    return body


def _decode_verified_proposal(
    *,
    proposal_payload: bytes,
    template_payload: bytes,
    artifact_payloads: dict[str, bytes],
    proposal_sha256: str,
    universe: UniverseSnapshot,
    parent_release_sha256: str,
) -> _VerifiedProposal:
    if hashlib.sha256(proposal_payload).hexdigest() != proposal_sha256:
        raise EvidenceWorkflowError("evidence proposal checksum mismatch")
    document = _canonical_document(proposal_payload, "evidence proposal")
    if (
        set(document) != _PROPOSAL_FIELDS
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["kind"] != "UNREVIEWED_EVIDENCE_PROPOSAL"
        or document["parent_release_sha256"] != parent_release_sha256
        or document["universe_sha256"] != universe._release_pin
        or not isinstance(document["subjects"], list)
        or not isinstance(document["observations"], list)
        or document["source_failures"] != []
        or not isinstance(document["reviewer_template_sha256"], str)
        or _SHA256.fullmatch(document["reviewer_template_sha256"]) is None
    ):
        raise EvidenceWorkflowError("evidence proposal schema is invalid or partial")
    created_at = _workflow_timestamp(document["created_at"], "proposal created_at")
    eligible = universe.eligible_records()
    expected_subjects = tuple(_subject_document(record) for record in eligible)
    if document["subjects"] != list(expected_subjects):
        raise EvidenceWorkflowError("evidence proposal subjects are invalid")
    template = _canonical_document(template_payload, "evidence review template")
    if (
        hashlib.sha256(template_payload).hexdigest()
        != document["reviewer_template_sha256"]
        or template != _expected_review_template(expected_subjects)
    ):
        raise EvidenceWorkflowError("evidence review template is invalid")

    expected_artifacts: set[str] = set()
    observations: list[_CollectedObservation] = []
    identifiers: set[str] = set()
    raw_observations = document["observations"]
    if len(raw_observations) != len(EVIDENCE_AUTHORITIES):
        raise EvidenceWorkflowError("evidence proposal observations are incomplete")
    for raw, authority in zip(
        raw_observations,
        EVIDENCE_AUTHORITIES,
        strict=True,
    ):
        expected_type = (
            "SEC_SUBMISSIONS"
            if authority.role.startswith("SEC_SUBMISSIONS:")
            else "OFFICIAL_REFERENCE"
        )
        if (
            not isinstance(raw, dict)
            or set(raw) != _PROPOSAL_OBSERVATION_FIELDS
            or raw["symbol"] != authority.symbol
            or raw["issuer_cik"] != authority.issuer_cik
            or not isinstance(raw["url"], str)
            or raw["url"] not in authority.allowed_final_urls
            or raw["publisher"] != authority.publisher
            or raw["role"] != authority.role
            or raw["event_class"] != authority.event_class
            or raw["source_type"] != expected_type
            or raw["accession"] is not None
            or not isinstance(raw["observation_id"], str)
            or _IDENTIFIER.fullmatch(raw["observation_id"]) is None
            or raw["observation_id"] in identifiers
            or not isinstance(raw["content_sha256"], str)
            or _SHA256.fullmatch(raw["content_sha256"]) is None
            or raw["artifact_path"]
            != f"artifacts/{raw['content_sha256']}.json"
        ):
            raise EvidenceWorkflowError("evidence proposal observation is invalid")
        published_at = _workflow_optional_timestamp(
            raw["published_at"],
            "proposal observation published_at",
        )
        retrieved_at = _workflow_timestamp(
            raw["retrieved_at"],
            "proposal observation retrieved_at",
        )
        if published_at is not None and published_at > retrieved_at:
            raise EvidenceWorkflowError("evidence proposal observation time is invalid")
        if expected_type == "OFFICIAL_REFERENCE":
            if published_at is not None or raw["timestamp_source"] != "UNAVAILABLE":
                raise EvidenceWorkflowError(
                    "generic proposal observation metadata is invalid"
                )
        elif not (
            (
                published_at is not None
                and raw["timestamp_source"] == "SEC_SUBMISSIONS_METADATA"
            )
            or (
                published_at is None
                and raw["timestamp_source"] == "UNAVAILABLE"
            )
        ):
            raise EvidenceWorkflowError("SEC proposal observation metadata is invalid")
        content_sha256 = raw["content_sha256"]
        assert isinstance(content_sha256, str)
        artifact_payload = artifact_payloads.get(content_sha256)
        if artifact_payload is None:
            raise EvidenceWorkflowError("evidence proposal artifact is missing")
        body = _decode_artifact(
            artifact_payload,
            expected_sha256=content_sha256,
        )
        identifiers.add(raw["observation_id"])
        expected_artifacts.add(content_sha256)
        observations.append(
            _CollectedObservation(
                observation_id=raw["observation_id"],
                symbol=authority.symbol,
                issuer_cik=authority.issuer_cik,
                url=raw["url"],
                publisher=authority.publisher,
                role=authority.role,
                event_class=authority.event_class,
                retrieved_at=retrieved_at,
                published_at=published_at,
                timestamp_source=raw["timestamp_source"],
                content_sha256=content_sha256,
                source_type=expected_type,
                accession=None,
                body=body,
            )
        )
    if set(artifact_payloads) != expected_artifacts:
        raise EvidenceWorkflowError("evidence proposal artifact inventory is invalid")
    return _VerifiedProposal(
        created_at=created_at,
        parent_release_sha256=parent_release_sha256,
        universe_sha256=universe._release_pin,
        subjects=expected_subjects,
        observations=tuple(observations),
        artifact_payloads=artifact_payloads,
    )


def _load_verified_proposal(
    *,
    state_root: Path,
    proposal_sha256: str,
    universe: UniverseSnapshot,
    parent_release_sha256: str,
) -> _VerifiedProposal:
    if not isinstance(proposal_sha256, str) or _SHA256.fullmatch(
        proposal_sha256
    ) is None:
        raise EvidenceWorkflowError("evidence proposal checksum is malformed")
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    root_descriptor: int | None = None
    artifacts_descriptor: int | None = None
    try:
        state_descriptor, parent_descriptor = _open_proposal_parent(state_root)
        fcntl.flock(parent_descriptor, fcntl.LOCK_SH)
        parent_before = os.fstat(parent_descriptor)
        _verify_parent_binding(state_root, state_descriptor, parent_descriptor)
        try:
            root_descriptor = os.open(
                proposal_sha256,
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=parent_descriptor,
            )
        except OSError:
            raise EvidenceWorkflowError("evidence proposal is unavailable") from None
        root_before = os.fstat(root_descriptor)
        _validate_private_directory_details(root_before)
        linked_root = os.stat(
            proposal_sha256,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            linked_root.st_dev != root_before.st_dev
            or linked_root.st_ino != root_before.st_ino
            or _directory_names(root_descriptor)
            != {"artifacts", "proposal.json", "review-template.json"}
        ):
            raise EvidenceWorkflowError("evidence proposal tree is unsafe")
        proposal_payload = _read_private_bytes_at(
            root_descriptor,
            "proposal.json",
            maximum_bytes=_MAX_PROPOSAL_BYTES,
        )
        template_payload = _read_private_bytes_at(
            root_descriptor,
            "review-template.json",
            maximum_bytes=_MAX_REVIEW_TEMPLATE_BYTES,
        )
        proposal_document = _canonical_document(
            proposal_payload,
            "evidence proposal",
        )
        raw_observations = proposal_document.get("observations")
        if (
            not isinstance(raw_observations, list)
            or len(raw_observations) != len(EVIDENCE_AUTHORITIES)
        ):
            raise EvidenceWorkflowError("evidence proposal schema is invalid")
        digests: set[str] = set()
        for value in raw_observations:
            if (
                not isinstance(value, dict)
                or not isinstance(value.get("content_sha256"), str)
                or _SHA256.fullmatch(value["content_sha256"]) is None
            ):
                raise EvidenceWorkflowError("evidence proposal observation is invalid")
            digests.add(value["content_sha256"])
        try:
            artifacts_descriptor = os.open(
                "artifacts",
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=root_descriptor,
            )
        except OSError:
            raise EvidenceWorkflowError("evidence proposal artifacts are unsafe") from None
        artifacts_before = os.fstat(artifacts_descriptor)
        _validate_private_directory_details(artifacts_before)
        expected_names = {f"{digest}.json" for digest in digests}
        if _directory_names(artifacts_descriptor) != expected_names:
            raise EvidenceWorkflowError("evidence proposal artifact inventory is invalid")
        artifact_payloads = {
            digest: _read_private_bytes_at(
                artifacts_descriptor,
                f"{digest}.json",
                maximum_bytes=_MAX_SOURCE_ARTIFACT_BYTES,
            )
            for digest in sorted(digests)
        }
        if (
            _directory_names(root_descriptor)
            != {"artifacts", "proposal.json", "review-template.json"}
            or _directory_names(artifacts_descriptor) != expected_names
        ):
            raise EvidenceWorkflowError("evidence proposal tree changed during read")
        _stable_directory(root_before, os.fstat(root_descriptor))
        _stable_directory(artifacts_before, os.fstat(artifacts_descriptor))
        _stable_directory(parent_before, os.fstat(parent_descriptor))
        linked_root = os.stat(
            proposal_sha256,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            linked_root.st_dev != root_before.st_dev
            or linked_root.st_ino != root_before.st_ino
        ):
            raise EvidenceWorkflowError("evidence proposal path binding changed")
        _verify_parent_binding(state_root, state_descriptor, parent_descriptor)
        return _decode_verified_proposal(
            proposal_payload=proposal_payload,
            template_payload=template_payload,
            artifact_payloads=artifact_payloads,
            proposal_sha256=proposal_sha256,
            universe=universe,
            parent_release_sha256=parent_release_sha256,
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
            raise EvidenceWorkflowError(
                "evidence proposal could not be read safely"
            ) from None
    finally:
        if artifacts_descriptor is not None:
            os.close(artifacts_descriptor)
        if root_descriptor is not None:
            os.close(root_descriptor)
        if parent_descriptor is not None:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)


def _review_string_tuple(
    value: object,
    name: str,
    *,
    nonempty: bool = False,
) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or (nonempty and not value)
        or any(not isinstance(item, str) for item in value)
        or len(value) != len(set(value))
    ):
        raise EvidenceWorkflowError(f"{name} is malformed")
    return tuple(value)


def _derived_identifier(prefix: str, value: object) -> str:
    return f"{prefix}-{hashlib.sha256(_canonical_bytes(value)).hexdigest()[:24]}"


def _record_release_document(value: EvidenceRecord) -> dict[str, object]:
    return {
        "accession": value.accession,
        "adverse_tags": list(value.adverse_tags),
        "classification_ambiguous": value.classification_ambiguous,
        "conflicts": list(value.conflicts),
        "content_hash": value.content_hash,
        "event_date": value.event_date.isoformat() if value.event_date else None,
        "event_kind": value.event_kind,
        "event_type": value.event_type,
        "fact": value.fact,
        "issuer_cik": value.issuer_cik,
        "primary_url": value.primary_url,
        "published_at": _utc_text(value.published_at),
        "publisher": value.publisher,
        "record_id": value.record_id,
        "retrieved_at": _utc_text(value.retrieved_at),
        "source_observation_ids": list(value.source_observation_ids),
        "symbol": value.symbol,
    }


def _binding_release_document(
    value: EvidenceSourceBinding,
) -> dict[str, object]:
    document = value.document
    return {
        "accession": document.accession,
        "checked_at": _utc_text(value.checked_at),
        "content_hash": document.content_hash,
        "healthy": value.healthy,
        "issuer_cik": value.issuer_cik,
        "primary_url": document.url,
        "published_at": (
            _utc_text(document.published_at)
            if document.published_at is not None
            else None
        ),
        "publisher": document.publisher,
        "retrieved_at": _utc_text(document.retrieved_at),
        "source_observation_id": document.source_observation_id,
        "source_role": document.source_role,
        "source_type": document.source_type,
        "symbol": value.symbol,
        "timestamp_source": document.timestamp_source,
        "valid_until": _utc_text(value.valid_until),
    }


def _coverage_release_document(
    value: EvidenceCoverageAttestation,
) -> dict[str, object]:
    return {
        "checked_at": _utc_text(value.checked_at),
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
        "valid_until": _utc_text(value.valid_until),
    }


def _source_binding(
    observation: _CollectedObservation,
    *,
    reviewed_at: datetime,
    review_by: datetime,
) -> EvidenceSourceBinding:
    try:
        document = SourceDocument(
            url=observation.url,
            published_at=observation.published_at,
            retrieved_at=observation.retrieved_at,
            content_hash=observation.content_sha256,
            body=observation.body,
            source_observation_id=observation.observation_id,
            publisher=observation.publisher,
            source_type=observation.source_type,
            timestamp_source=observation.timestamp_source,
            accession=observation.accession,
            source_role=(
                None
                if observation.source_type == "SEC_SUBMISSIONS"
                else observation.role
            ),
        )
        return EvidenceSourceBinding.from_document(
            document,
            symbol=observation.symbol,
            issuer_cik=observation.issuer_cik,
            checked_at=reviewed_at,
            valid_until=review_by,
            healthy=True,
        )
    except (TypeError, ValueError):
        raise EvidenceWorkflowError(
            "evidence proposal observation cannot form a safe binding"
        ) from None


def _compile_review_candidate(
    *,
    proposal: _VerifiedProposal,
    proposal_sha256: str,
    review_payload: bytes,
    universe: UniverseSnapshot,
    current: datetime,
) -> _CompiledCandidate:
    review_input_sha256 = hashlib.sha256(review_payload).hexdigest()
    document = _canonical_document(review_payload, "evidence review input")
    if (
        set(document) != _REVIEW_FIELDS
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["kind"] != "EVIDENCE_REVIEW_INPUT"
        or document["proposal_sha256"] != proposal_sha256
        or document["universe_sha256"] != proposal.universe_sha256
        or not isinstance(document["subjects"], list)
    ):
        raise EvidenceWorkflowError("evidence review input schema is invalid")
    reviewed_at = _workflow_timestamp(document["reviewed_at"], "reviewed_at")
    review_by = _workflow_timestamp(document["review_by"], "review_by")
    if (
        proposal.created_at > reviewed_at
        or not reviewed_at <= current < review_by
        or not timedelta(0) < review_by - reviewed_at <= timedelta(hours=24)
    ):
        raise EvidenceWorkflowError("evidence review window is invalid")
    coverage_start = _workflow_date(document["coverage_start"], "coverage_start")
    coverage_end = _workflow_date(document["coverage_end"], "coverage_end")
    if (
        coverage_start > coverage_end
        or not coverage_start <= current.date() <= coverage_end
    ):
        raise EvidenceWorkflowError("evidence coverage dates are invalid")

    raw_subjects = document["subjects"]
    eligible = universe.eligible_records()
    if len(raw_subjects) != len(eligible):
        raise EvidenceWorkflowError("evidence review subjects are incomplete")
    observations_by_id = {
        value.observation_id: value for value in proposal.observations
    }
    release_files: dict[str, bytes] = {}
    manifest_subjects: list[dict[str, object]] = []
    coverage_summary: list[tuple[str, str, str]] = []
    used_artifacts: set[str] = set()
    global_fact_ids: set[str] = set()
    global_coverage_ids: set[str] = set()
    global_fact_bodies: set[tuple[str, str]] = set()
    global_coverage_bodies: set[tuple[str, str]] = set()

    for raw_subject, universe_record, proposal_subject in zip(
        raw_subjects,
        eligible,
        proposal.subjects,
        strict=True,
    ):
        if (
            not isinstance(raw_subject, dict)
            or set(raw_subject) != _REVIEW_SUBJECT_FIELDS
            or raw_subject["symbol"] != proposal_subject["symbol"]
            or raw_subject["issuer_cik"] != proposal_subject["issuer_cik"]
            or raw_subject["subject_kind"] != proposal_subject["subject_kind"]
            or raw_subject["event_class"] != proposal_subject["event_class"]
            or not isinstance(raw_subject["records"], list)
            or not isinstance(raw_subject["coverage_attestations"], list)
        ):
            raise EvidenceWorkflowError("evidence review subject is invalid")
        symbol = universe_record.symbol
        issuer_cik = universe_record.issuer_cik
        subject_kind = proposal_subject["subject_kind"]
        relevant = proposal_subject["event_class"]
        assert isinstance(subject_kind, str) and isinstance(relevant, str)
        opposite = "ETF_ACTION" if relevant == "BINARY_EVENT" else "BINARY_EVENT"

        coverage_by_class: dict[
            str,
            tuple[str, bool, tuple[str, ...]],
        ] = {}
        for raw_coverage in raw_subject["coverage_attestations"]:
            if (
                not isinstance(raw_coverage, dict)
                or set(raw_coverage) != _REVIEW_COVERAGE_FIELDS
                or type(raw_coverage["complete"]) is not bool
                or raw_coverage["conflicts"] != []
                or not isinstance(raw_coverage["event_class"], str)
                or raw_coverage["event_class"] not in _EVENT_CLASSES
                or not isinstance(raw_coverage["coverage"], str)
            ):
                raise EvidenceWorkflowError("evidence review coverage is invalid")
            event_class = raw_coverage["event_class"]
            assert isinstance(event_class, str)
            if event_class in coverage_by_class:
                raise EvidenceWorkflowError("evidence review coverage is duplicated")
            source_ids = _review_string_tuple(
                raw_coverage["source_observation_ids"],
                "coverage source observations",
                nonempty=True,
            )
            for identifier in source_ids:
                observation = observations_by_id.get(identifier)
                if (
                    observation is None
                    or observation.symbol != symbol
                    or observation.issuer_cik != issuer_cik
                    or observation.source_type != "OFFICIAL_REFERENCE"
                ):
                    raise EvidenceWorkflowError(
                        "coverage source observation is not proposal-bound"
                    )
            coverage_by_class[event_class] = (
                raw_coverage["coverage"],
                raw_coverage["complete"],
                source_ids,
            )
        if set(coverage_by_class) != _EVENT_CLASSES:
            raise EvidenceWorkflowError("evidence review coverage is incomplete")
        if coverage_by_class[relevant][:2] != ("UNKNOWN", False) or (
            coverage_by_class[opposite][:2] != ("NOT_APPLICABLE", True)
        ):
            raise EvidenceWorkflowError("evidence review coverage state is unsafe")

        records: list[EvidenceRecord] = []
        subject_fact_ids: set[str] = set()
        for raw_record in raw_subject["records"]:
            if (
                not isinstance(raw_record, dict)
                or set(raw_record) != _REVIEW_RECORD_FIELDS
                or type(raw_record["classification_ambiguous"]) is not bool
            ):
                raise EvidenceWorkflowError("evidence review record is invalid")
            source_ids = _review_string_tuple(
                raw_record["source_observation_ids"],
                "record source observations",
                nonempty=True,
            )
            if len(source_ids) != 1:
                raise EvidenceWorkflowError(
                    "evidence review record requires one source observation"
                )
            observation = observations_by_id.get(source_ids[0])
            if (
                observation is None
                or observation.symbol != symbol
                or observation.issuer_cik != issuer_cik
            ):
                raise EvidenceWorkflowError(
                    "record source observation is not proposal-bound"
                )
            published_at = _workflow_timestamp(
                raw_record["published_at"],
                "record published_at",
            )
            if published_at > observation.retrieved_at or (
                observation.published_at is not None
                and published_at != observation.published_at
            ):
                raise EvidenceWorkflowError("record publication time is invalid")
            allowed_types = (
                POSITIVE_EVENT_TYPES
                if subject_kind == "STOCK"
                else ETF_POSITIVE_EVENT_TYPES
            )
            event_type = raw_record["event_type"]
            if event_type is not None and event_type not in allowed_types:
                raise EvidenceWorkflowError("record event type is unsupported")
            adverse_tags = _review_string_tuple(
                raw_record["adverse_tags"],
                "record adverse tags",
            )
            if any(value not in ADVERSE_TAGS for value in adverse_tags):
                raise EvidenceWorkflowError("record adverse tags are unsupported")
            if event_type is None and not adverse_tags:
                raise EvidenceWorkflowError(
                    "record requires a positive event type or adverse tag"
                )
            conflicts = _review_string_tuple(
                raw_record["conflicts"],
                "record conflicts",
            )
            raw_event_date = raw_record["event_date"]
            event_date = (
                None
                if raw_event_date is None
                else _workflow_date(raw_event_date, "record event_date")
            )
            event_kind = raw_record["event_kind"]
            if (event_date is None) != (event_kind is None) or (
                event_kind is not None and event_kind != relevant
            ):
                raise EvidenceWorkflowError("record event kind is invalid")
            record_id = _derived_identifier(
                "record",
                {
                    "proposal_sha256": proposal_sha256,
                    "record": raw_record,
                    "symbol": symbol,
                },
            )
            try:
                record = EvidenceRecord(
                    record_id=record_id,
                    symbol=symbol,
                    issuer_cik=issuer_cik,
                    primary_url=observation.url,
                    publisher=observation.publisher,
                    published_at=published_at,
                    retrieved_at=observation.retrieved_at,
                    event_type=event_type,
                    fact=raw_record["fact"],
                    content_hash=observation.content_sha256,
                    source_observation_ids=source_ids,
                    accession=observation.accession,
                    adverse_tags=adverse_tags,
                    conflicts=conflicts,
                    classification_ambiguous=raw_record[
                        "classification_ambiguous"
                    ],
                    event_date=event_date,
                    event_kind=event_kind,
                )
            except (TypeError, ValueError):
                raise EvidenceWorkflowError("evidence review record is invalid") from None
            if record_id in {value.record_id for value in records}:
                raise EvidenceWorkflowError("evidence review record is duplicated")
            records.append(record)
            subject_fact_ids.update(source_ids)

        subject_coverage_ids = {
            identifier
            for _, _, identifiers in coverage_by_class.values()
            for identifier in identifiers
        }
        fact_bodies = {
            (
                observations_by_id[identifier].url,
                observations_by_id[identifier].content_sha256,
            )
            for identifier in subject_fact_ids
        }
        coverage_bodies = {
            (
                observations_by_id[identifier].url,
                observations_by_id[identifier].content_sha256,
            )
            for identifier in subject_coverage_ids
        }
        if subject_fact_ids & subject_coverage_ids or fact_bodies & coverage_bodies:
            raise EvidenceWorkflowError(
                "evidence facts and coverage require distinct proposal observations"
            )
        global_fact_ids.update(subject_fact_ids)
        global_coverage_ids.update(subject_coverage_ids)
        global_fact_bodies.update(fact_bodies)
        global_coverage_bodies.update(coverage_bodies)

        used_ids = subject_fact_ids | subject_coverage_ids
        used_observations = [observations_by_id[value] for value in sorted(used_ids)]
        if any(
            observation.retrieved_at > reviewed_at
            or review_by - observation.retrieved_at > timedelta(hours=24)
            for observation in used_observations
        ):
            raise EvidenceWorkflowError("evidence review source window is invalid")
        bindings = [
            _source_binding(
                observation,
                reviewed_at=reviewed_at,
                review_by=review_by,
            )
            for observation in used_observations
        ]
        coverage_values: list[EvidenceCoverageAttestation] = []
        for event_class in (relevant, opposite):
            coverage_state, complete, source_ids = coverage_by_class[event_class]
            try:
                value = EvidenceCoverageAttestation(
                    subject_kind=subject_kind,
                    symbol=symbol,
                    issuer_cik=issuer_cik,
                    coverage_kind=event_class,
                    coverage=coverage_state,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                    source_observation_ids=source_ids,
                    checked_at=reviewed_at,
                    valid_until=review_by,
                    healthy=True,
                    complete=complete,
                    conflicts=(),
                )
            except (TypeError, ValueError):
                raise EvidenceWorkflowError("evidence review coverage is invalid") from None
            coverage_values.append(value)
            coverage_summary.append((symbol, event_class, coverage_state))
        registry_id = _derived_identifier(
            f"registry-{symbol.lower()}",
            {
                "proposal_sha256": proposal_sha256,
                "review_input_sha256": review_input_sha256,
                "symbol": symbol,
            },
        )
        child_document: dict[str, object] = {
            "coverage_attestations": [
                _coverage_release_document(value) for value in coverage_values
            ],
            "kind": "REVIEWED_EVIDENCE_BUNDLE",
            "records": [_record_release_document(value) for value in records],
            "registry_id": registry_id,
            "reviewed_at": _utc_text(reviewed_at),
            "schema_version": 3,
            "source_bindings": [
                _binding_release_document(value) for value in bindings
            ],
            "subject": {
                "issuer_cik": issuer_cik,
                "subject_kind": subject_kind,
                "symbol": symbol,
            },
        }
        child_payload = _canonical_bytes(child_document)
        child_sha256 = hashlib.sha256(child_payload).hexdigest()
        release_files[f"subjects/{symbol}.json"] = child_payload
        manifest_subjects.append(
            {
                "issuer_cik": issuer_cik,
                "path": f"subjects/{symbol}.json",
                "sha256": child_sha256,
                "subject_kind": subject_kind,
                "symbol": symbol,
            }
        )
        for observation in used_observations:
            used_artifacts.add(observation.content_sha256)

    if global_fact_ids & global_coverage_ids or (
        global_fact_bodies & global_coverage_bodies
    ):
        raise EvidenceWorkflowError(
            "evidence facts and coverage require distinct proposal source bodies"
        )
    for digest in sorted(used_artifacts):
        release_files[f"sources/{digest}.json"] = proposal.artifact_payloads[digest]
    release_id = _derived_identifier(
        "candidate-release",
        {
            "proposal_sha256": proposal_sha256,
            "review_input_sha256": review_input_sha256,
        },
    )
    release_document: dict[str, object] = {
        "kind": "REVIEWED_EVIDENCE_RELEASE",
        "release_id": release_id,
        "review_by": _utc_text(review_by),
        "reviewed_at": _utc_text(reviewed_at),
        "schema_version": 1,
        "subjects": manifest_subjects,
        "universe_sha256": proposal.universe_sha256,
    }
    release_payload = _canonical_bytes(release_document)
    release_sha256 = hashlib.sha256(release_payload).hexdigest()
    release_files["current.json"] = release_payload
    _validate_compiled_release(
        release_files,
        release_sha256=release_sha256,
        current=current,
        universe=universe,
    )
    return _CompiledCandidate(
        review_input_sha256=review_input_sha256,
        reviewed_at=reviewed_at,
        review_by=review_by,
        release_sha256=release_sha256,
        release_files=release_files,
        symbols=tuple(record.symbol for record in eligible),
        coverage=tuple(sorted(coverage_summary)),
    )


def _validate_compiled_release(
    release_files: dict[str, bytes],
    *,
    release_sha256: str,
    current: datetime,
    universe: UniverseSnapshot,
) -> None:
    try:
        with tempfile.TemporaryDirectory(prefix="evidence-candidate-validation-") as raw:
            root = Path(raw)
            root.chmod(0o700)
            (root / "subjects").mkdir(mode=0o700)
            (root / "sources").mkdir(mode=0o700)
            for relative, payload in sorted(release_files.items()):
                path = root / relative
                descriptor = os.open(
                    path,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                try:
                    os.fchmod(descriptor, 0o600)
                    written = 0
                    while written < len(payload):
                        count = os.write(descriptor, payload[written:])
                        if count <= 0:
                            raise OSError("short candidate validation write")
                        written += count
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            load_evidence_release(
                root / "current.json",
                expected_sha256=release_sha256,
                as_of=current,
                universe=universe,
            )
    except (EvidenceRegistryError, OSError, TypeError, ValueError):
        raise EvidenceWorkflowError(
            "compiled evidence candidate release is invalid"
        ) from None


def _open_candidate_parent(state_root: Path) -> tuple[int, int]:
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    try:
        root_before = state_root.lstat()
        state_descriptor = os.open(state_root, _DIRECTORY_OPEN_FLAGS)
        root_opened = os.fstat(state_descriptor)
        _validate_private_directory_details(root_opened)
        if (
            root_before.st_dev != root_opened.st_dev
            or root_before.st_ino != root_opened.st_ino
        ):
            raise EvidenceWorkflowError("evidence state root changed during open")
        created = False
        try:
            os.mkdir("evidence-candidates", 0o700, dir_fd=state_descriptor)
            created = True
            os.fsync(state_descriptor)
        except FileExistsError:
            pass
        parent_descriptor = os.open(
            "evidence-candidates",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=state_descriptor,
        )
        if created:
            os.fchmod(parent_descriptor, 0o700)
        _validate_private_directory_details(os.fstat(parent_descriptor))
        return state_descriptor, parent_descriptor
    except EvidenceWorkflowError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise
    except OSError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise EvidenceWorkflowError("evidence candidate storage is unavailable") from None


def _open_existing_candidate_parent(state_root: Path) -> tuple[int, int]:
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    try:
        root_before = state_root.lstat()
        state_descriptor = os.open(state_root, _DIRECTORY_OPEN_FLAGS)
        root_opened = os.fstat(state_descriptor)
        _validate_private_directory_details(root_opened)
        if (
            root_before.st_dev != root_opened.st_dev
            or root_before.st_ino != root_opened.st_ino
        ):
            raise EvidenceWorkflowError("evidence state root changed during open")
        parent_descriptor = os.open(
            "evidence-candidates",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=state_descriptor,
        )
        _validate_private_directory_details(os.fstat(parent_descriptor))
        return state_descriptor, parent_descriptor
    except EvidenceWorkflowError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise
    except OSError:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)
        raise EvidenceWorkflowError("evidence candidate storage is unavailable") from None


def _verify_candidate_parent_binding(
    state_root: Path,
    state_descriptor: int,
    parent_descriptor: int,
) -> None:
    opened_root = os.fstat(state_descriptor)
    opened_parent = os.fstat(parent_descriptor)
    _validate_private_directory_details(opened_root)
    _validate_private_directory_details(opened_parent)
    try:
        path_root = state_root.lstat()
        linked_parent = os.stat(
            "evidence-candidates",
            dir_fd=state_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError("evidence candidate path binding changed") from None
    if (
        not stat.S_ISDIR(path_root.st_mode)
        or path_root.st_dev != opened_root.st_dev
        or path_root.st_ino != opened_root.st_ino
        or not stat.S_ISDIR(linked_parent.st_mode)
        or linked_parent.st_dev != opened_parent.st_dev
        or linked_parent.st_ino != opened_parent.st_ino
        or linked_parent.st_uid != opened_parent.st_uid
        or stat.S_IMODE(linked_parent.st_mode) != 0o700
    ):
        raise EvidenceWorkflowError("evidence candidate path binding changed")


def _verify_candidate_tree(
    parent_descriptor: int,
    root_name: str,
    *,
    candidate_payload: bytes,
    release_files: dict[str, bytes],
) -> None:
    descriptors: list[int] = []
    try:
        root = os.open(root_name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_descriptor)
        descriptors.append(root)
        root_before = os.fstat(root)
        _validate_private_directory_details(root_before)
        linked_root = os.stat(
            root_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(linked_root.st_mode)
            or linked_root.st_dev != root_before.st_dev
            or linked_root.st_ino != root_before.st_ino
        ):
            raise EvidenceWorkflowError("evidence candidate path binding changed")
        release = os.open("release", _DIRECTORY_OPEN_FLAGS, dir_fd=root)
        descriptors.append(release)
        release_before = os.fstat(release)
        _validate_private_directory_details(release_before)
        subjects = os.open("subjects", _DIRECTORY_OPEN_FLAGS, dir_fd=release)
        descriptors.append(subjects)
        subjects_before = os.fstat(subjects)
        _validate_private_directory_details(subjects_before)
        sources = os.open("sources", _DIRECTORY_OPEN_FLAGS, dir_fd=release)
        descriptors.append(sources)
        sources_before = os.fstat(sources)
        _validate_private_directory_details(sources_before)

        subject_files = {
            relative.removeprefix("subjects/"): payload
            for relative, payload in release_files.items()
            if relative.startswith("subjects/")
        }
        source_files = {
            relative.removeprefix("sources/"): payload
            for relative, payload in release_files.items()
            if relative.startswith("sources/")
        }
        if (
            _directory_names(root) != {"candidate.json", "release"}
            or _directory_names(release) != {"current.json", "sources", "subjects"}
            or _directory_names(subjects) != set(subject_files)
            or _directory_names(sources) != set(source_files)
        ):
            raise EvidenceWorkflowError("evidence candidate tree content collision")
        _read_private_at(root, "candidate.json", candidate_payload)
        _read_private_at(release, "current.json", release_files["current.json"])
        for name, payload in sorted(subject_files.items()):
            _read_private_at(subjects, name, payload)
        for name, payload in sorted(source_files.items()):
            _read_private_at(sources, name, payload)
        if (
            _directory_names(root) != {"candidate.json", "release"}
            or _directory_names(release) != {"current.json", "sources", "subjects"}
            or _directory_names(subjects) != set(subject_files)
            or _directory_names(sources) != set(source_files)
        ):
            raise EvidenceWorkflowError("evidence candidate tree content collision")
        _stable_directory(root_before, os.fstat(root))
        _stable_directory(release_before, os.fstat(release))
        _stable_directory(subjects_before, os.fstat(subjects))
        _stable_directory(sources_before, os.fstat(sources))
        linked_root = os.stat(
            root_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            linked_root.st_dev != root_before.st_dev
            or linked_root.st_ino != root_before.st_ino
        ):
            raise EvidenceWorkflowError("evidence candidate path binding changed")
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("evidence candidate tree is unsafe") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _candidate_inventory(
    document: dict[str, object],
) -> tuple[tuple[str, str], ...]:
    raw_inventory = document["inventory"]
    if (
        not isinstance(raw_inventory, list)
        or not raw_inventory
        or len(raw_inventory) > _MAX_CANDIDATE_FILES
    ):
        raise EvidenceWorkflowError("evidence candidate inventory is invalid")
    inventory: list[tuple[str, str]] = []
    for item in raw_inventory:
        if not isinstance(item, dict) or set(item) != _CANDIDATE_INVENTORY_FIELDS:
            raise EvidenceWorkflowError("evidence candidate inventory is invalid")
        path = item["path"]
        digest = item["sha256"]
        if (
            not isinstance(path, str)
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
        ):
            raise EvidenceWorkflowError("evidence candidate inventory is invalid")
        if path == "current.json":
            pass
        elif path.startswith("subjects/"):
            name = path.removeprefix("subjects/")
            if (
                not name.endswith(".json")
                or _SYMBOL.fullmatch(name.removesuffix(".json")) is None
            ):
                raise EvidenceWorkflowError("evidence candidate path is invalid")
        elif path.startswith("sources/"):
            name = path.removeprefix("sources/")
            if (
                not name.endswith(".json")
                or _SHA256.fullmatch(name.removesuffix(".json")) is None
            ):
                raise EvidenceWorkflowError("evidence candidate path is invalid")
        else:
            raise EvidenceWorkflowError("evidence candidate path is invalid")
        inventory.append((path, digest))
    paths = tuple(path for path, _ in inventory)
    if (
        paths != tuple(sorted(paths))
        or len(set(paths)) != len(paths)
        or paths.count("current.json") != 1
    ):
        raise EvidenceWorkflowError("evidence candidate inventory is invalid")
    return tuple(inventory)


def _verify_private_child_directory_binding(
    parent_descriptor: int,
    name: str,
    opened: os.stat_result,
) -> None:
    try:
        linked = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError("evidence candidate path binding changed") from None
    if (
        not stat.S_ISDIR(linked.st_mode)
        or linked.st_dev != opened.st_dev
        or linked.st_ino != opened.st_ino
        or linked.st_uid != opened.st_uid
        or linked.st_mode != opened.st_mode
    ):
        raise EvidenceWorkflowError("evidence candidate path binding changed")


def _read_candidate_release_tree(
    candidate_descriptor: int,
    inventory: tuple[tuple[str, str], ...],
) -> dict[str, bytes]:
    descriptors: list[int] = []
    try:
        release = os.open(
            "release",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=candidate_descriptor,
        )
        descriptors.append(release)
        release_before = os.fstat(release)
        _validate_private_directory_details(release_before)
        subjects = os.open("subjects", _DIRECTORY_OPEN_FLAGS, dir_fd=release)
        descriptors.append(subjects)
        subjects_before = os.fstat(subjects)
        _validate_private_directory_details(subjects_before)
        sources = os.open("sources", _DIRECTORY_OPEN_FLAGS, dir_fd=release)
        descriptors.append(sources)
        sources_before = os.fstat(sources)
        _validate_private_directory_details(sources_before)

        subject_names = {
            path.removeprefix("subjects/")
            for path, _ in inventory
            if path.startswith("subjects/")
        }
        source_names = {
            path.removeprefix("sources/")
            for path, _ in inventory
            if path.startswith("sources/")
        }
        if (
            _directory_names(candidate_descriptor) != {"candidate.json", "release"}
            or _directory_names(release)
            != {"current.json", "sources", "subjects"}
            or _directory_names(subjects) != subject_names
            or _directory_names(sources) != source_names
        ):
            raise EvidenceWorkflowError("evidence candidate tree is invalid")

        release_files: dict[str, bytes] = {}
        total_bytes = 0
        for relative, expected_sha256 in inventory:
            if relative == "current.json":
                directory = release
                name = relative
                maximum_bytes = _MAX_PARENT_RELEASE_BYTES
            elif relative.startswith("subjects/"):
                directory = subjects
                name = relative.removeprefix("subjects/")
                maximum_bytes = _MAX_PARENT_RELEASE_BYTES
            else:
                directory = sources
                name = relative.removeprefix("sources/")
                maximum_bytes = _MAX_SOURCE_ARTIFACT_BYTES
            payload = _read_private_bytes_at(
                directory,
                name,
                maximum_bytes=maximum_bytes,
            )
            total_bytes += len(payload)
            if (
                total_bytes > _MAX_CANDIDATE_TOTAL_BYTES
                or hashlib.sha256(payload).hexdigest() != expected_sha256
            ):
                raise EvidenceWorkflowError("evidence candidate file digest mismatch")
            _canonical_document(payload, "evidence candidate release file")
            release_files[relative] = payload

        if (
            _directory_names(candidate_descriptor) != {"candidate.json", "release"}
            or _directory_names(release)
            != {"current.json", "sources", "subjects"}
            or _directory_names(subjects) != subject_names
            or _directory_names(sources) != source_names
        ):
            raise EvidenceWorkflowError("evidence candidate tree is invalid")
        _stable_directory(release_before, os.fstat(release))
        _stable_directory(subjects_before, os.fstat(subjects))
        _stable_directory(sources_before, os.fstat(sources))
        _verify_private_child_directory_binding(
            candidate_descriptor,
            "release",
            release_before,
        )
        _verify_private_child_directory_binding(release, "subjects", subjects_before)
        _verify_private_child_directory_binding(release, "sources", sources_before)
        return release_files
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("evidence candidate tree is unsafe") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _load_candidate(
    state_root: Path,
    candidate_sha256: str,
) -> _LoadedCandidate:
    if not isinstance(candidate_sha256, str) or _SHA256.fullmatch(candidate_sha256) is None:
        raise EvidenceWorkflowError("evidence candidate digest is malformed")
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    candidate_descriptor: int | None = None
    try:
        state_descriptor, parent_descriptor = _open_existing_candidate_parent(state_root)
        fcntl.flock(parent_descriptor, fcntl.LOCK_SH)
        _verify_candidate_parent_binding(
            state_root,
            state_descriptor,
            parent_descriptor,
        )
        candidate_descriptor = os.open(
            candidate_sha256,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_descriptor,
        )
        candidate_before = os.fstat(candidate_descriptor)
        _validate_private_directory_details(candidate_before)
        linked_candidate = os.stat(
            candidate_sha256,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(linked_candidate.st_mode)
            or linked_candidate.st_dev != candidate_before.st_dev
            or linked_candidate.st_ino != candidate_before.st_ino
        ):
            raise EvidenceWorkflowError("evidence candidate path binding changed")
        candidate_payload = _read_private_bytes_at(
            candidate_descriptor,
            "candidate.json",
            maximum_bytes=_MAX_REVIEW_INPUT_BYTES,
        )
        document = _canonical_document(candidate_payload, "evidence candidate")
        if (
            hashlib.sha256(candidate_payload).hexdigest() != candidate_sha256
            or set(document) != _CANDIDATE_FIELDS
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["kind"] != "EVIDENCE_RELEASE_CANDIDATE"
        ):
            raise EvidenceWorkflowError("evidence candidate wrapper is invalid")
        for name in (
            "parent_release_sha256",
            "proposal_sha256",
            "release_sha256",
            "review_input_sha256",
            "universe_sha256",
        ):
            value = document[name]
            if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                raise EvidenceWorkflowError("evidence candidate wrapper is invalid")
        inventory = _candidate_inventory(document)
        release_files = _read_candidate_release_tree(
            candidate_descriptor,
            inventory,
        )
        release_sha256 = document["release_sha256"]
        universe_sha256 = document["universe_sha256"]
        parent_release_sha256 = document["parent_release_sha256"]
        assert isinstance(release_sha256, str)
        assert isinstance(universe_sha256, str)
        assert isinstance(parent_release_sha256, str)
        release_document = _canonical_document(
            release_files["current.json"],
            "evidence candidate release",
        )
        if (
            hashlib.sha256(release_files["current.json"]).hexdigest()
            != release_sha256
            or release_document.get("universe_sha256") != universe_sha256
        ):
            raise EvidenceWorkflowError("evidence candidate release binding is invalid")
        _verify_candidate_tree(
            parent_descriptor,
            candidate_sha256,
            candidate_payload=candidate_payload,
            release_files=release_files,
        )
        _stable_directory(candidate_before, os.fstat(candidate_descriptor))
        linked_candidate = os.stat(
            candidate_sha256,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            linked_candidate.st_dev != candidate_before.st_dev
            or linked_candidate.st_ino != candidate_before.st_ino
        ):
            raise EvidenceWorkflowError("evidence candidate path binding changed")
        _verify_candidate_parent_binding(
            state_root,
            state_descriptor,
            parent_descriptor,
        )
        return _LoadedCandidate(
            candidate_sha256=candidate_sha256,
            parent_release_sha256=parent_release_sha256,
            release_sha256=release_sha256,
            universe_sha256=universe_sha256,
            release_files=release_files,
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("evidence candidate is unavailable") from None
    finally:
        if candidate_descriptor is not None:
            os.close(candidate_descriptor)
        if parent_descriptor is not None:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)


def _validate_candidate_release(
    candidate: _LoadedCandidate,
    *,
    current: datetime,
    universe: UniverseSnapshot,
) -> tuple[str, ...]:
    if candidate.universe_sha256 != universe._release_pin:
        raise EvidenceWorkflowError("evidence candidate universe changed")
    manifest = _canonical_document(
        candidate.release_files["current.json"],
        "evidence candidate release",
    )
    manifest_subjects = manifest.get("subjects")
    if not isinstance(manifest_subjects, list):
        raise EvidenceWorkflowError("evidence candidate release is invalid")
    manifest_subject_paths: set[str] = set()
    for subject in manifest_subjects:
        if not isinstance(subject, dict) or not isinstance(subject.get("path"), str):
            raise EvidenceWorkflowError("evidence candidate release is invalid")
        manifest_subject_paths.add(subject["path"])
    inventory_subject_paths = {
        relative
        for relative in candidate.release_files
        if relative.startswith("subjects/")
    }
    if inventory_subject_paths != manifest_subject_paths:
        raise EvidenceWorkflowError(
            "evidence candidate subjects do not exactly match its manifest"
        )
    bound_sources: set[str] = set()
    for relative, payload in candidate.release_files.items():
        if not relative.startswith("subjects/"):
            continue
        child = _canonical_document(payload, "evidence candidate subject")
        bindings = child.get("source_bindings")
        if not isinstance(bindings, list):
            raise EvidenceWorkflowError("evidence candidate subject is invalid")
        for binding in bindings:
            if not isinstance(binding, dict):
                raise EvidenceWorkflowError("evidence candidate subject is invalid")
            content_hash = binding.get("content_hash")
            if not isinstance(content_hash, str) or _SHA256.fullmatch(content_hash) is None:
                raise EvidenceWorkflowError("evidence candidate subject is invalid")
            bound_sources.add(f"sources/{content_hash}.json")
    inventory_sources = {
        relative
        for relative in candidate.release_files
        if relative.startswith("sources/")
    }
    if inventory_sources != bound_sources:
        raise EvidenceWorkflowError(
            "evidence candidate sources do not exactly match subject bindings"
        )
    _validate_compiled_release(
        candidate.release_files,
        release_sha256=candidate.release_sha256,
        current=current,
        universe=universe,
    )
    return tuple(record.symbol for record in universe.eligible_records())


def _validate_active_directory_details(details: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_mode & 0o022
    ):
        raise EvidenceWorkflowError("active evidence directory is unsafe")


def _verify_active_evidence_binding(
    project_root: Path,
    project_descriptor: int,
    data_descriptor: int,
    evidence_descriptor: int,
) -> None:
    project_opened = os.fstat(project_descriptor)
    data_opened = os.fstat(data_descriptor)
    evidence_opened = os.fstat(evidence_descriptor)
    _validate_active_directory_details(project_opened)
    _validate_active_directory_details(data_opened)
    _validate_active_directory_details(evidence_opened)
    try:
        linked_project = project_root.lstat()
        linked_data = os.stat(
            "data",
            dir_fd=project_descriptor,
            follow_symlinks=False,
        )
        linked_evidence = os.stat(
            "evidence",
            dir_fd=data_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise EvidenceWorkflowError("active evidence path binding changed") from None
    for linked, opened in (
        (linked_project, project_opened),
        (linked_data, data_opened),
        (linked_evidence, evidence_opened),
    ):
        if (
            not stat.S_ISDIR(linked.st_mode)
            or linked.st_dev != opened.st_dev
            or linked.st_ino != opened.st_ino
            or linked.st_uid != opened.st_uid
            or linked.st_mode != opened.st_mode
        ):
            raise EvidenceWorkflowError("active evidence path binding changed")


def _open_active_evidence_root(project_root: Path) -> tuple[int, int, int]:
    project_descriptor: int | None = None
    data_descriptor: int | None = None
    evidence_descriptor: int | None = None
    try:
        project_before = project_root.lstat()
        project_descriptor = os.open(project_root, _DIRECTORY_OPEN_FLAGS)
        project_opened = os.fstat(project_descriptor)
        _validate_active_directory_details(project_opened)
        if (
            project_before.st_dev != project_opened.st_dev
            or project_before.st_ino != project_opened.st_ino
        ):
            raise EvidenceWorkflowError("active evidence path binding changed")
        data_descriptor = os.open(
            "data",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=project_descriptor,
        )
        _validate_active_directory_details(os.fstat(data_descriptor))
        evidence_descriptor = os.open(
            "evidence",
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=data_descriptor,
        )
        _validate_active_directory_details(os.fstat(evidence_descriptor))
        _verify_active_evidence_binding(
            project_root,
            project_descriptor,
            data_descriptor,
            evidence_descriptor,
        )
        return project_descriptor, data_descriptor, evidence_descriptor
    except EvidenceWorkflowError:
        if evidence_descriptor is not None:
            os.close(evidence_descriptor)
        if data_descriptor is not None:
            os.close(data_descriptor)
        if project_descriptor is not None:
            os.close(project_descriptor)
        raise
    except OSError:
        if evidence_descriptor is not None:
            os.close(evidence_descriptor)
        if data_descriptor is not None:
            os.close(data_descriptor)
        if project_descriptor is not None:
            os.close(project_descriptor)
        raise EvidenceWorkflowError("active evidence tree is unavailable") from None


def _open_active_child_directory(
    parent_descriptor: int,
    name: str,
    *,
    create: bool,
) -> tuple[int, os.stat_result]:
    if name not in {"subjects", "sources"}:
        raise EvidenceWorkflowError("active evidence destination is invalid")
    created = False
    try:
        if create:
            try:
                os.mkdir(name, 0o700, dir_fd=parent_descriptor)
                created = True
                os.fsync(parent_descriptor)
            except FileExistsError:
                pass
        descriptor = os.open(
            name,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_descriptor,
        )
    except OSError:
        raise EvidenceWorkflowError("active evidence directory is unsafe") from None
    try:
        if created:
            os.fchmod(descriptor, 0o700)
        opened = os.fstat(descriptor)
        _validate_active_directory_details(opened)
        linked = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(linked.st_mode)
            or linked.st_dev != opened.st_dev
            or linked.st_ino != opened.st_ino
            or linked.st_uid != opened.st_uid
            or linked.st_mode != opened.st_mode
        ):
            raise EvidenceWorkflowError("active evidence path binding changed")
        return descriptor, opened
    except BaseException:
        os.close(descriptor)
        raise


def _read_optional_active_file_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int,
) -> tuple[bytes | None, os.stat_result | None]:
    try:
        descriptor = os.open(
            name,
            _FILE_OPEN_FLAGS,
            dir_fd=directory_descriptor,
        )
    except FileNotFoundError:
        return None, None
    except OSError:
        raise EvidenceWorkflowError("active evidence file is unsafe") from None
    try:
        payload = _read_stable_regular_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
            private=False,
        )
        opened = os.fstat(descriptor)
        try:
            linked = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except OSError:
            raise EvidenceWorkflowError("active evidence path binding changed") from None
        if any(
            getattr(linked, field) != getattr(opened, field)
            for field in _STABLE_STAT_FIELDS
        ):
            raise EvidenceWorkflowError("active evidence path binding changed")
        return payload, opened
    finally:
        os.close(descriptor)


def _read_active_file_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int,
) -> tuple[bytes, os.stat_result]:
    payload, details = _read_optional_active_file_at(
        directory_descriptor,
        name,
        maximum_bytes=maximum_bytes,
    )
    if payload is None or details is None:
        raise EvidenceWorkflowError("active evidence file is missing")
    return payload, details


def _verify_active_destination_state(
    directory_descriptor: int,
    name: str,
    expected: os.stat_result | None,
) -> None:
    try:
        actual = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        if expected is None:
            return
        raise EvidenceWorkflowError("active evidence destination changed") from None
    except OSError:
        raise EvidenceWorkflowError("active evidence destination is unsafe") from None
    if expected is None or any(
        getattr(actual, field) != getattr(expected, field)
        for field in _STABLE_STAT_FIELDS
    ):
        raise EvidenceWorkflowError("active evidence destination changed")


def _atomic_install_write_at(
    directory_descriptor: int,
    name: str,
    payload: bytes,
    *,
    expected: os.stat_result | None,
    before_replace: Callable[[], None] | None = None,
) -> None:
    temporary_name = f".{name}.{secrets.token_hex(16)}.install.tmp"
    descriptor: int | None = None
    temporary_exists = False
    temporary_identity: tuple[int, int] | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_descriptor,
        )
        temporary_exists = True
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("short evidence install write")
            written += count
        os.fsync(descriptor)
        details = os.fstat(descriptor)
        temporary_identity = (details.st_dev, details.st_ino)
        os.close(descriptor)
        descriptor = None
        if before_replace is not None:
            before_replace()
        _verify_active_destination_state(directory_descriptor, name, expected)
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
        temporary_exists = False
        installed = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if temporary_identity != (installed.st_dev, installed.st_ino):
            raise EvidenceWorkflowError("active evidence publication changed identity")
        os.fsync(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_exists:
            try:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            except OSError:
                pass


def _verify_active_release_files(
    evidence_descriptor: int,
    release_files: dict[str, bytes],
) -> None:
    subjects: int | None = None
    sources: int | None = None
    try:
        subjects, subjects_before = _open_active_child_directory(
            evidence_descriptor,
            "subjects",
            create=False,
        )
        sources, sources_before = _open_active_child_directory(
            evidence_descriptor,
            "sources",
            create=False,
        )
        for relative, expected in sorted(release_files.items()):
            if relative == "current.json":
                directory = evidence_descriptor
                name = relative
                maximum_bytes = _MAX_PARENT_RELEASE_BYTES
            elif relative.startswith("subjects/"):
                directory = subjects
                name = relative.removeprefix("subjects/")
                maximum_bytes = _MAX_PARENT_RELEASE_BYTES
            else:
                directory = sources
                name = relative.removeprefix("sources/")
                maximum_bytes = _MAX_SOURCE_ARTIFACT_BYTES
            actual, _ = _read_active_file_at(
                directory,
                name,
                maximum_bytes=maximum_bytes,
            )
            if actual != expected:
                raise EvidenceWorkflowError("installed evidence differs from candidate")
        _stable_review_input_directory(subjects_before, os.fstat(subjects))
        _stable_review_input_directory(sources_before, os.fstat(sources))
        _verify_private_child_directory_binding(
            evidence_descriptor,
            "subjects",
            subjects_before,
        )
        _verify_private_child_directory_binding(
            evidence_descriptor,
            "sources",
            sources_before,
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("installed evidence tree is unsafe") from None
    finally:
        if sources is not None:
            os.close(sources)
        if subjects is not None:
            os.close(subjects)


def _install_release_tree_manifest_last(
    evidence_descriptor: int,
    *,
    release_files: dict[str, bytes],
    active_manifest: bytes,
    active_manifest_details: os.stat_result,
    verify_authority: Callable[[], None],
) -> None:
    subjects: int | None = None
    sources: int | None = None
    try:
        subjects, subjects_before = _open_active_child_directory(
            evidence_descriptor,
            "subjects",
            create=True,
        )
        sources, sources_before = _open_active_child_directory(
            evidence_descriptor,
            "sources",
            create=True,
        )
        subject_files = {
            relative.removeprefix("subjects/"): payload
            for relative, payload in release_files.items()
            if relative.startswith("subjects/")
        }
        source_files = {
            relative.removeprefix("sources/"): payload
            for relative, payload in release_files.items()
            if relative.startswith("sources/")
        }
        subject_states: dict[
            str,
            tuple[bytes | None, os.stat_result | None],
        ] = {}
        source_states: dict[
            str,
            tuple[bytes | None, os.stat_result | None],
        ] = {}
        for name in sorted(subject_files):
            subject_states[name] = _read_optional_active_file_at(
                subjects,
                name,
                maximum_bytes=_MAX_PARENT_RELEASE_BYTES,
            )
        for name, expected_payload in sorted(source_files.items()):
            state = _read_optional_active_file_at(
                sources,
                name,
                maximum_bytes=_MAX_SOURCE_ARTIFACT_BYTES,
            )
            if state[0] is not None and state[0] != expected_payload:
                raise EvidenceWorkflowError("active evidence source collision")
            source_states[name] = state

        for name, payload in sorted(source_files.items()):
            existing_payload, details = source_states[name]
            if existing_payload is None:
                _atomic_install_write_at(
                    sources,
                    name,
                    payload,
                    expected=details,
                )
        for name, payload in sorted(subject_files.items()):
            existing_payload, details = subject_states[name]
            if existing_payload != payload:
                _atomic_install_write_at(
                    subjects,
                    name,
                    payload,
                    expected=details,
                )

        _stable_review_input_directory(subjects_before, os.fstat(subjects))
        _stable_review_input_directory(sources_before, os.fstat(sources))
        _verify_private_child_directory_binding(
            evidence_descriptor,
            "subjects",
            subjects_before,
        )
        _verify_private_child_directory_binding(
            evidence_descriptor,
            "sources",
            sources_before,
        )

        def recheck_active_manifest() -> None:
            rechecked, _ = _read_active_file_at(
                evidence_descriptor,
                "current.json",
                maximum_bytes=_MAX_PARENT_RELEASE_BYTES,
            )
            if rechecked != active_manifest:
                raise EvidenceWorkflowError("candidate parent release changed")
            for name, expected_payload in sorted(subject_files.items()):
                installed_payload, _ = _read_active_file_at(
                    subjects,
                    name,
                    maximum_bytes=_MAX_PARENT_RELEASE_BYTES,
                )
                if installed_payload != expected_payload:
                    raise EvidenceWorkflowError(
                        "active evidence child changed before manifest"
                    )
            for name, expected_payload in sorted(source_files.items()):
                installed_payload, _ = _read_active_file_at(
                    sources,
                    name,
                    maximum_bytes=_MAX_SOURCE_ARTIFACT_BYTES,
                )
                if installed_payload != expected_payload:
                    raise EvidenceWorkflowError(
                        "active evidence source changed before manifest"
                    )
            _stable_review_input_directory(subjects_before, os.fstat(subjects))
            _stable_review_input_directory(sources_before, os.fstat(sources))
            _verify_private_child_directory_binding(
                evidence_descriptor,
                "subjects",
                subjects_before,
            )
            _verify_private_child_directory_binding(
                evidence_descriptor,
                "sources",
                sources_before,
            )
            verify_authority()

        _atomic_install_write_at(
            evidence_descriptor,
            "current.json",
            release_files["current.json"],
            expected=active_manifest_details,
            before_replace=recheck_active_manifest,
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("active evidence installation failed") from None
    finally:
        if sources is not None:
            os.close(sources)
        if subjects is not None:
            os.close(subjects)


def _read_installed_summary(
    *,
    project_root: Path,
    project_descriptor: int,
    data_descriptor: int,
    evidence_descriptor: int,
    candidate: _LoadedCandidate,
    current: datetime,
    expected_symbols: tuple[str, ...],
    status: str,
) -> EvidenceInstallSummary:
    def current_universe() -> UniverseSnapshot:
        if (
            evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256
            != candidate.release_sha256
        ):
            raise EvidenceWorkflowError(
                "candidate release is not independently pinned"
            )
        try:
            refreshed = load_current_universe(
                project_root,
                as_of=current.date(),
            )
        except (OSError, TypeError, ValueError):
            raise EvidenceWorkflowError("evidence universe changed") from None
        if (
            refreshed._release_pin != candidate.universe_sha256
            or tuple(record.symbol for record in refreshed.eligible_records())
            != expected_symbols
        ):
            raise EvidenceWorkflowError("evidence universe changed")
        return refreshed

    _verify_active_evidence_binding(
        project_root,
        project_descriptor,
        data_descriptor,
        evidence_descriptor,
    )
    universe = current_universe()
    _verify_active_release_files(
        evidence_descriptor,
        candidate.release_files,
    )
    try:
        release = evidence_module.load_current_evidence_release(
            project_root,
            as_of=current,
            universe=universe,
        )
    except (EvidenceRegistryError, OSError, TypeError, ValueError):
        raise EvidenceWorkflowError("installed evidence readback failed") from None
    _verify_active_evidence_binding(
        project_root,
        project_descriptor,
        data_descriptor,
        evidence_descriptor,
    )
    _verify_active_release_files(
        evidence_descriptor,
        candidate.release_files,
    )
    current_universe()
    symbols = tuple(release.by_symbol)
    if (
        release.release_sha256 != candidate.release_sha256
        or release.universe_sha256 != candidate.universe_sha256
        or symbols != expected_symbols
    ):
        raise EvidenceWorkflowError("installed evidence readback differs from candidate")
    return EvidenceInstallSummary(
        status=status,
        candidate_sha256=candidate.candidate_sha256,
        release_sha256=candidate.release_sha256,
        installed_at=current,
        symbols=symbols,
    )


def _remove_directory_contents(descriptor: int, *, depth: int = 0) -> None:
    if depth > 8:
        raise OSError("candidate cleanup nesting is unsafe")
    for name in _directory_names(descriptor):
        details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if stat.S_ISDIR(details.st_mode):
            try:
                child = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=descriptor)
            except OSError:
                os.rmdir(name, dir_fd=descriptor)
                continue
            try:
                _remove_directory_contents(child, depth=depth + 1)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=descriptor)
        else:
            os.unlink(name, dir_fd=descriptor)
    os.fsync(descriptor)


def _remove_candidate_tree_at(parent_descriptor: int, name: str) -> None:
    try:
        root = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_descriptor)
    except OSError:
        return
    try:
        _remove_directory_contents(root)
    except (EvidenceWorkflowError, OSError):
        return
    finally:
        os.close(root)
    try:
        os.rmdir(name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    except OSError:
        pass


def _remove_matching_candidate_tree_at(
    parent_descriptor: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    try:
        details = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except OSError:
        return
    if (details.st_dev, details.st_ino) == identity:
        _remove_candidate_tree_at(parent_descriptor, name)


def _write_candidate_tree(
    *,
    state_root: Path,
    proposal_sha256: str,
    proposal: _VerifiedProposal,
    compiled: _CompiledCandidate,
) -> tuple[str, Path]:
    inventory = [
        {
            "path": relative,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        for relative, payload in sorted(compiled.release_files.items())
    ]
    candidate_document: dict[str, object] = {
        "inventory": inventory,
        "kind": "EVIDENCE_RELEASE_CANDIDATE",
        "parent_release_sha256": proposal.parent_release_sha256,
        "proposal_sha256": proposal_sha256,
        "release_sha256": compiled.release_sha256,
        "review_input_sha256": compiled.review_input_sha256,
        "schema_version": 1,
        "universe_sha256": proposal.universe_sha256,
    }
    candidate_payload = _canonical_bytes(candidate_document)
    candidate_sha256 = hashlib.sha256(candidate_payload).hexdigest()
    state_descriptor: int | None = None
    parent_descriptor: int | None = None
    temporary_name: str | None = None
    temporary_identity: tuple[int, int] | None = None
    published = False
    try:
        state_descriptor, parent_descriptor = _open_candidate_parent(state_root)
        fcntl.flock(parent_descriptor, fcntl.LOCK_EX)
        _verify_candidate_parent_binding(
            state_root,
            state_descriptor,
            parent_descriptor,
        )
        if _entry_exists(parent_descriptor, candidate_sha256):
            _verify_candidate_tree(
                parent_descriptor,
                candidate_sha256,
                candidate_payload=candidate_payload,
                release_files=compiled.release_files,
            )
            _verify_candidate_parent_binding(
                state_root,
                state_descriptor,
                parent_descriptor,
            )
            return (
                candidate_sha256,
                state_root
                / "evidence-candidates"
                / candidate_sha256
                / "candidate.json",
            )

        temporary_name, temporary = _create_private_directory(
            parent_descriptor,
            prefix=f".{candidate_sha256}.",
        )
        try:
            temporary_details = os.fstat(temporary)
            temporary_identity = (temporary_details.st_dev, temporary_details.st_ino)
            os.mkdir("release", 0o700, dir_fd=temporary)
            release = os.open("release", _DIRECTORY_OPEN_FLAGS, dir_fd=temporary)
            try:
                os.fchmod(release, 0o700)
                _validate_private_directory_details(os.fstat(release))
                os.mkdir("subjects", 0o700, dir_fd=release)
                os.mkdir("sources", 0o700, dir_fd=release)
                subjects: int | None = None
                sources: int | None = None
                try:
                    subjects = os.open(
                        "subjects",
                        _DIRECTORY_OPEN_FLAGS,
                        dir_fd=release,
                    )
                    sources = os.open(
                        "sources",
                        _DIRECTORY_OPEN_FLAGS,
                        dir_fd=release,
                    )
                    os.fchmod(subjects, 0o700)
                    os.fchmod(sources, 0o700)
                    _validate_private_directory_details(os.fstat(subjects))
                    _validate_private_directory_details(os.fstat(sources))
                    _atomic_write_at(temporary, "candidate.json", candidate_payload)
                    _atomic_write_at(
                        release,
                        "current.json",
                        compiled.release_files["current.json"],
                    )
                    for relative, payload in sorted(compiled.release_files.items()):
                        if relative.startswith("subjects/"):
                            _atomic_write_at(
                                subjects,
                                relative.removeprefix("subjects/"),
                                payload,
                            )
                        elif relative.startswith("sources/"):
                            _atomic_write_at(
                                sources,
                                relative.removeprefix("sources/"),
                                payload,
                            )
                    os.fsync(subjects)
                    os.fsync(sources)
                finally:
                    if sources is not None:
                        os.close(sources)
                    if subjects is not None:
                        os.close(subjects)
                os.fsync(release)
            finally:
                os.close(release)
            os.fsync(temporary)
        finally:
            os.close(temporary)

        if _entry_exists(parent_descriptor, candidate_sha256):
            _verify_candidate_tree(
                parent_descriptor,
                candidate_sha256,
                candidate_payload=candidate_payload,
                release_files=compiled.release_files,
            )
            _remove_candidate_tree_at(parent_descriptor, temporary_name)
            temporary_name = None
        else:
            os.replace(
                temporary_name,
                candidate_sha256,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            temporary_name = None
            published = True
            destination = os.stat(
                candidate_sha256,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            if temporary_identity != (destination.st_dev, destination.st_ino):
                raise EvidenceWorkflowError(
                    "evidence candidate publication changed identity"
                )
            os.fsync(parent_descriptor)
        _verify_candidate_tree(
            parent_descriptor,
            candidate_sha256,
            candidate_payload=candidate_payload,
            release_files=compiled.release_files,
        )
        _verify_candidate_parent_binding(
            state_root,
            state_descriptor,
            parent_descriptor,
        )
        return (
            candidate_sha256,
            state_root
            / "evidence-candidates"
            / candidate_sha256
            / "candidate.json",
        )
    except EvidenceWorkflowError:
        if parent_descriptor is not None and temporary_name is not None:
            _remove_candidate_tree_at(parent_descriptor, temporary_name)
        if parent_descriptor is not None and published and temporary_identity is not None:
            _remove_matching_candidate_tree_at(
                parent_descriptor,
                candidate_sha256,
                temporary_identity,
            )
        raise
    except Exception:
        if parent_descriptor is not None and temporary_name is not None:
            _remove_candidate_tree_at(parent_descriptor, temporary_name)
        if parent_descriptor is not None and published and temporary_identity is not None:
            _remove_matching_candidate_tree_at(
                parent_descriptor,
                candidate_sha256,
                temporary_identity,
            )
        raise EvidenceWorkflowError("evidence candidate storage failed") from None
    finally:
        if parent_descriptor is not None:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)
        if state_descriptor is not None:
            os.close(state_descriptor)


def inspect_evidence_candidate(
    *,
    project_root: Path,
    state_root: Path,
    proposal_sha256: str,
    review_input_path: Path,
    as_of: datetime,
) -> EvidenceCandidateSummary:
    """Compile reviewer input into an immutable, non-authoritative candidate."""
    project_root = _validate_root(project_root, private=False)
    state_root = _validate_root(state_root, private=True)
    try:
        current = require_aware_timestamp(as_of, "candidate as_of").astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        raise EvidenceWorkflowError("evidence candidate time is invalid") from None
    try:
        universe = load_current_universe(project_root, as_of=current.date())
    except (OSError, TypeError, ValueError):
        raise EvidenceWorkflowError("evidence universe is invalid") from None
    parent_release_sha256 = _current_release_digest(project_root)
    proposal = _load_verified_proposal(
        state_root=state_root,
        proposal_sha256=proposal_sha256,
        universe=universe,
        parent_release_sha256=parent_release_sha256,
    )
    review_payload = _read_private_absolute_file(
        review_input_path,
        maximum_bytes=_MAX_REVIEW_INPUT_BYTES,
    )
    compiled = _compile_review_candidate(
        proposal=proposal,
        proposal_sha256=proposal_sha256,
        review_payload=review_payload,
        universe=universe,
        current=current,
    )
    candidate_sha256, candidate_path = _write_candidate_tree(
        state_root=state_root,
        proposal_sha256=proposal_sha256,
        proposal=proposal,
        compiled=compiled,
    )
    return EvidenceCandidateSummary(
        status="AWAITING_DIGEST_APPROVAL",
        candidate_sha256=candidate_sha256,
        proposal_sha256=proposal_sha256,
        review_input_sha256=compiled.review_input_sha256,
        release_sha256=compiled.release_sha256,
        universe_sha256=proposal.universe_sha256,
        reviewed_at=compiled.reviewed_at,
        review_by=compiled.review_by,
        symbols=compiled.symbols,
        coverage=compiled.coverage,
        reason_codes=("RELEVANT_COVERAGE_UNKNOWN",),
        candidate_path=candidate_path,
    )


def install_evidence_candidate(
    *,
    project_root: Path,
    state_root: Path,
    candidate_sha256: str,
    as_of: datetime,
) -> EvidenceInstallSummary:
    """Install an independently pinned candidate into active evidence."""
    project_root = _validate_root(project_root, private=False)
    state_root = _validate_root(state_root, private=True)
    try:
        current = require_aware_timestamp(as_of, "installation as_of").astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        raise EvidenceWorkflowError("evidence installation time is invalid") from None
    candidate = _load_candidate(state_root, candidate_sha256)
    if candidate.release_sha256 != evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256:
        raise EvidenceWorkflowError("candidate release is not independently pinned")
    try:
        universe = load_current_universe(project_root, as_of=current.date())
    except (OSError, TypeError, ValueError):
        raise EvidenceWorkflowError("evidence universe is invalid") from None
    expected_symbols = _validate_candidate_release(
        candidate,
        current=current,
        universe=universe,
    )

    project_descriptor: int | None = None
    data_descriptor: int | None = None
    evidence_descriptor: int | None = None
    try:
        (
            project_descriptor,
            data_descriptor,
            evidence_descriptor,
        ) = _open_active_evidence_root(project_root)
        fcntl.flock(evidence_descriptor, fcntl.LOCK_EX)
        _verify_active_evidence_binding(
            project_root,
            project_descriptor,
            data_descriptor,
            evidence_descriptor,
        )
        active_manifest, active_details = _read_active_file_at(
            evidence_descriptor,
            "current.json",
            maximum_bytes=_MAX_PARENT_RELEASE_BYTES,
        )
        active_sha256 = hashlib.sha256(active_manifest).hexdigest()
        if active_sha256 == candidate.release_sha256:
            return _read_installed_summary(
                project_root=project_root,
                project_descriptor=project_descriptor,
                data_descriptor=data_descriptor,
                evidence_descriptor=evidence_descriptor,
                candidate=candidate,
                current=current,
                expected_symbols=expected_symbols,
                status="ALREADY_INSTALLED",
            )
        if active_sha256 != candidate.parent_release_sha256:
            raise EvidenceWorkflowError("candidate parent release changed")

        def verify_install_authority() -> None:
            if (
                evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256
                != candidate.release_sha256
            ):
                raise EvidenceWorkflowError(
                    "candidate release is not independently pinned"
                )
            try:
                current_universe = load_current_universe(
                    project_root,
                    as_of=current.date(),
                )
            except (OSError, TypeError, ValueError):
                raise EvidenceWorkflowError("evidence universe changed") from None
            if (
                current_universe._release_pin != candidate.universe_sha256
                or tuple(record.symbol for record in current_universe.eligible_records())
                != expected_symbols
            ):
                raise EvidenceWorkflowError("evidence universe changed")
            _verify_active_evidence_binding(
                project_root,
                project_descriptor,
                data_descriptor,
                evidence_descriptor,
            )

        _install_release_tree_manifest_last(
            evidence_descriptor,
            release_files=candidate.release_files,
            active_manifest=active_manifest,
            active_manifest_details=active_details,
            verify_authority=verify_install_authority,
        )
        _verify_active_evidence_binding(
            project_root,
            project_descriptor,
            data_descriptor,
            evidence_descriptor,
        )
        return _read_installed_summary(
            project_root=project_root,
            project_descriptor=project_descriptor,
            data_descriptor=data_descriptor,
            evidence_descriptor=evidence_descriptor,
            candidate=candidate,
            current=current,
            expected_symbols=expected_symbols,
            status="INSTALLED",
        )
    except EvidenceWorkflowError:
        raise
    except OSError:
        raise EvidenceWorkflowError("active evidence installation failed") from None
    finally:
        if evidence_descriptor is not None:
            try:
                fcntl.flock(evidence_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(evidence_descriptor)
        if data_descriptor is not None:
            os.close(data_descriptor)
        if project_descriptor is not None:
            os.close(project_descriptor)


__all__ = [
    "EvidenceCandidateSummary",
    "EvidenceInstallSummary",
    "EvidenceProposalSummary",
    "EvidenceWorkflowError",
    "inspect_evidence_candidate",
    "install_evidence_candidate",
    "prepare_evidence_proposal",
]
