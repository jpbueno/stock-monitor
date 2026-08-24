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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from stock_monitor.domain import require_aware_timestamp
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
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
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
_SAFE_REASON_CODES = frozenset(
    {
        "SEC_COLLECTOR_UNAVAILABLE",
        "SOURCE_COLLECTION_FAILED",
        "SOURCE_RESULT_INVALID",
    }
)


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


__all__ = [
    "EvidenceProposalSummary",
    "EvidenceWorkflowError",
    "prepare_evidence_proposal",
]
