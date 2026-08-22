"""Fail-closed orchestration for manual, paper-only monitor workflows.

The workflow layer consumes normalized facts through a narrow adapter.  It has
no brokerage or order-placement surface; successful candidates are report-only
paper plans that still require manual execution and confirmation.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol
from weakref import ReferenceType, ref

from .domain import require_aware_timestamp
from .reports import (
    ClosePosition,
    CloseState,
    PremarketCandidate,
    PremarketShadow,
    PremarketState,
    Report,
    ReportSource,
    ScoreComponent,
    UnverifiedClosePosition,
    _issued_report_snapshot,
    archive_report,
    render_close_report,
    render_premarket_report,
)


_TOKEN = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.]{0,9}\Z")
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FIXTURE_KEYS = frozenset(
    {"now", "account", "provider_fixture", "evidence_fixture", "expected_outcome"}
)
_ACCOUNT_KEYS = frozenset(
    {
        "configuration",
        "market",
        "review_time",
        "universe",
        "breaker",
        "candidates",
        "close_state",
    }
)
_ACCOUNT_REPORT_KEYS = _ACCOUNT_KEYS | {"close_positions"}
_CANDIDATE_KEYS = frozenset(
    {
        "symbol",
        "role",
        "setup",
        "score_components",
        "trigger",
        "maximum_entry",
        "recommended_stop",
        "target",
        "shares",
        "planned_risk",
        "provider",
        "feed",
        "observed_at",
        "invalidations",
        "sources",
    }
)
_SCORE_KEYS = frozenset({"label", "earned", "available"})
_SOURCE_KEYS = frozenset({"label", "url"})
_CLOSE_POSITION_KEYS = frozenset(
    {
        "symbol",
        "shares",
        "mark",
        "estimated_unrealized_pl",
        "r_multiple",
        "recommended_stop",
        "user_confirmed_stop",
        "target",
        "holding_days",
        "provider",
        "feed",
        "observed_at",
        "upcoming_events",
        "evidence",
    }
)
_CLOSE_STATES = frozenset(
    {
        "HOLD",
        "EXIT",
        "TIGHTEN_STOP",
        "RECONCILIATION_REQUIRED",
        "POSITION_UNVERIFIED",
        "STOP_UNVERIFIED",
        "DATA_UNAVAILABLE",
    }
)
_CLOSE_PRECEDENCE = (
    "RECONCILIATION_REQUIRED",
    "POSITION_UNVERIFIED",
    "STOP_UNVERIFIED",
    "DATA_UNAVAILABLE",
    "EXIT",
    "TIGHTEN_STOP",
    "HOLD",
)
_SAFE_DATA_REASON_CODES = frozenset(
    {
        "DATA_UNAVAILABLE",
        "PROVIDER_CHECK_FAILED",
        "SOURCE_CHECK_FAILED",
        "STALE_CALENDAR",
        "STALE_UNIVERSE",
    }
)
_ISSUED_RECORDED_ADAPTERS_LOCK = threading.Lock()
_ISSUED_RECORDED_ADAPTERS: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object] | None,
        Path | None,
        int | None,
        str | None,
    ],
] = {}
_ISSUED_WORKFLOW_RESULTS_LOCK = threading.Lock()
_ISSUED_WORKFLOW_RESULTS: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
        Path,
        str,
        date,
        datetime,
        tuple[int, ...],
        tuple[str, ...],
    ],
] = {}


class WorkflowError(RuntimeError):
    """Base class for an expected, safe-to-report workflow failure."""


class WorkflowConfigurationError(WorkflowError):
    """Required local configuration is missing or invalid."""


class WorkflowDataError(WorkflowError):
    """Reviewed data or a required provider/source check is unavailable."""


class WorkflowReconciliationError(WorkflowError):
    """Manual account truth has not been reconciled."""


class WorkflowBoundaryError(WorkflowError):
    """A requested action falls outside the paper/manual-only boundary."""


@dataclass(frozen=True, slots=True)
class SessionWindow:
    """Normalized market-session facts needed by workflow scheduling."""

    session_date: date
    review_time: time


@dataclass(frozen=True, slots=True)
class CandidateSummary:
    """Display-safe identity for an already-qualified paper candidate."""

    symbol: str
    role: str
    material: PremarketCandidate | PremarketShadow | None = None

    def __post_init__(self) -> None:
        if _SYMBOL.fullmatch(self.symbol) is None:
            raise ValueError("candidate symbol must be canonical")
        if self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}:
            raise ValueError("candidate role must be PRIMARY or WATCHLIST_SHADOW")
        if self.material is not None:
            if type(self.material) not in {PremarketCandidate, PremarketShadow}:
                raise TypeError("candidate material must be an exact report projection")
            if (self.material.symbol, self.material.role) != (self.symbol, self.role):
                raise ValueError("candidate material identity conflicts with summary")
            expected_type = (
                PremarketCandidate if self.role == "PRIMARY" else PremarketShadow
            )
            if type(self.material) is not expected_type:
                raise ValueError("candidate material type conflicts with its role")


def _validate_candidate_summary_composition(
    candidates: tuple[CandidateSummary, ...],
    *,
    required: bool,
) -> None:
    if type(candidates) is not tuple or any(
        type(candidate) is not CandidateSummary for candidate in candidates
    ):
        raise TypeError("candidates must be an exact CandidateSummary tuple")
    if not candidates:
        if required:
            raise ValueError("candidate outcome requires one primary candidate")
        return
    primary_count = sum(candidate.role == "PRIMARY" for candidate in candidates)
    shadow_count = sum(
        candidate.role == "WATCHLIST_SHADOW" for candidate in candidates
    )
    if primary_count != 1 or shadow_count > 2:
        raise ValueError("candidates require one primary and at most two shadows")
    symbols = tuple(candidate.symbol for candidate in candidates)
    if len(symbols) != len(set(symbols)):
        raise ValueError("candidate symbols must be unique")
    if candidates[0].role != "PRIMARY":
        raise ValueError("primary candidate must be first")


@dataclass(frozen=True, slots=True)
class ReportEvidence:
    """Exact source identities and Journal rows supporting one rendered report."""

    observation_ids: tuple[str, ...]
    state_hash: str
    source_observation_row_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not self.observation_ids or any(not value for value in self.observation_ids):
            raise ValueError("report evidence requires observation identities")
        if _LOWER_SHA256.fullmatch(self.state_hash) is None:
            raise ValueError("report evidence state hash must be a SHA-256 digest")


@dataclass(frozen=True, slots=True)
class PremarketSnapshot:
    """Normalized output of the screening/risk adapters."""

    candidates: tuple[CandidateSummary, ...]
    breaker_active: bool

    def __post_init__(self) -> None:
        _validate_candidate_summary_composition(
            self.candidates,
            required=False,
        )


@dataclass(frozen=True, slots=True)
class CloseSnapshot:
    """Normalized close decision with manual-verification precedence applied."""

    state: str
    positions: tuple[ClosePosition | UnverifiedClosePosition, ...] = ()

    def __post_init__(self) -> None:
        if self.state not in _CLOSE_STATES:
            raise ValueError("unsupported close state")
        if type(self.positions) is not tuple or any(
            type(position) not in {ClosePosition, UnverifiedClosePosition}
            for position in self.positions
        ):
            raise TypeError("close positions must contain exact report projections")


def _effective_close_state(snapshot: CloseSnapshot) -> str:
    states = {snapshot.state}
    for position in snapshot.positions:
        if type(position) is UnverifiedClosePosition:
            states.add(position.status)
        elif position.action != "HOLD":
            states.add(position.action)
    return next(state for state in _CLOSE_PRECEDENCE if state in states)


@dataclass(frozen=True, slots=True)
class PublishedWorkflow:
    """Durable report identity returned by a workflow publisher."""

    report_id: str | None
    report_row_id: int | None
    report_path: str | None
    status: str = "PUBLISHED"

    def __post_init__(self) -> None:
        if self.status not in {"PUBLISHED", "ALREADY_EMITTED", "IN_PROGRESS"}:
            raise ValueError("unsupported publication status")
        complete = (
            self.report_id is not None
            and self.report_row_id is not None
            and self.report_path is not None
        )
        if self.status in {"PUBLISHED", "ALREADY_EMITTED"} and not complete:
            raise ValueError("completed publication requires its report identity")
        if self.status == "IN_PROGRESS" and any(
            value is not None
            for value in (self.report_id, self.report_row_id, self.report_path)
        ):
            raise ValueError("in-progress publication cannot expose an identity")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class WorkflowResult:
    """Secret-safe result and exact process exit status for one workflow."""

    outcome: str
    message: str
    exit_code: int
    reason_codes: tuple[str, ...]
    candidates: tuple[CandidateSummary, ...] = ()
    report: Report | None = None
    source_observation_row_ids: tuple[int, ...] = ()
    execution_mode: str = "CANONICAL"
    report_id: str | None = None
    report_row_id: int | None = None
    report_path: str | None = None

    def __post_init__(self) -> None:
        if _TOKEN.fullmatch(self.outcome) is None:
            raise ValueError("workflow outcome must be canonical")
        if self.exit_code not in {0, 2, 3, 4, 5, 10}:
            raise ValueError("unsupported workflow exit code")
        if any(_TOKEN.fullmatch(code) is None for code in self.reason_codes):
            raise ValueError("workflow reason codes must be canonical")
        _validate_candidate_summary_composition(
            self.candidates,
            required=self.outcome == "CANDIDATES",
        )
        if self.exit_code != 0 and self.candidates:
            raise ValueError("nonzero workflow results cannot contain candidates")
        if self.outcome != "CANDIDATES" and self.candidates:
            raise ValueError("only candidate outcomes may contain candidates")
        if "\x00" in self.message:
            raise ValueError("workflow message contains a prohibited character")
        if self.execution_mode not in {"CANONICAL", "FIXTURE"}:
            raise ValueError("workflow execution mode is unsupported")
        if self.report is not None:
            if not isinstance(self.report, Report):
                raise TypeError("workflow report must be a Report")
            if self.report.body != self.message:
                raise ValueError("workflow message must equal rendered report body")
        if any(type(value) is not int or value < 1 for value in self.source_observation_row_ids):
            raise ValueError("source observation row IDs must be positive integers")
        if len(set(self.source_observation_row_ids)) != len(
            self.source_observation_row_ids
        ):
            raise ValueError("source observation row IDs must not contain duplicates")
        identities = (self.report_id, self.report_row_id, self.report_path)
        if any(value is not None for value in identities) and not all(
            value is not None for value in identities
        ):
            raise ValueError("report identity must be complete")

    def safe_fields(self) -> dict[str, object]:
        """Return the allowlisted JSON projection; never provider payloads/secrets."""
        fields: dict[str, object] = {
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "reason_codes": list(self.reason_codes),
            "message": self.message,
            "execution_mode": self.execution_mode,
            "candidates": [
                {"symbol": candidate.symbol, "role": candidate.role}
                for candidate in self.candidates
            ],
        }
        if self.report_id is not None:
            fields["report_id"] = self.report_id
            fields["report_path"] = self.report_path
        return fields


class WorkflowAdapter(Protocol):
    """Typed boundary around collection, normalization, and decision engines."""

    def validate_configuration(self) -> None: ...

    def market_session(self, day: date) -> SessionWindow | None: ...

    def verify_universe(self, day: date) -> None: ...

    def provider_smoke(self) -> None: ...

    def verify_sources(self) -> None: ...

    def report_evidence(self) -> ReportEvidence: ...

    def premarket_snapshot(self, day: date) -> PremarketSnapshot: ...

    def close_snapshot(self, day: date) -> CloseSnapshot: ...


class WorkflowPublisher(Protocol):
    """Durable, idempotent report/outbox publication boundary."""

    def publish(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
    ) -> PublishedWorkflow: ...

    def heal_finalized(
        self, *, kind: str, session_date: date
    ) -> PublishedWorkflow: ...


class ScheduledWorkflowStore(Protocol):
    """Atomic scheduled-run claim/completion boundary."""

    def start(self, *, kind: str, session_date: date, intended_at: datetime) -> bool: ...

    def status(self, *, kind: str, session_date: date) -> str: ...

    def result(
        self,
        *,
        kind: str,
        session_date: date,
    ) -> WorkflowResult | None: ...

    def complete(
        self,
        *,
        kind: str,
        session_date: date,
        intended_at: datetime,
        finished_at: datetime,
        decision: str,
        result: WorkflowResult,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkflowContext:
    """All explicit authorities required for one workflow run."""

    adapter: WorkflowAdapter
    publisher: WorkflowPublisher | None
    scheduler: ScheduledWorkflowStore | None
    now: datetime

    def __post_init__(self) -> None:
        require_aware_timestamp(self.now, "workflow time")


def run_premarket(context: WorkflowContext) -> WorkflowResult:
    """Run the premarket decision-support workflow without placing an order."""
    configuration = _configuration_result(context)
    if configuration is not None:
        return configuration
    try:
        session = context.adapter.market_session(context.now.date())
    except WorkflowDataError as error:
        reason = _safe_reason(error, "DATA_UNAVAILABLE")
        return _publish(
            context,
            "PREMARKET",
            _premarket_result(
                context,
                context.now.date(),
                "DATA_UNAVAILABLE",
                3,
                (reason,),
            ),
        )
    if session is None:
        return _publish(
            context,
            "PREMARKET",
            _premarket_result(
                context,
                context.now.date(),
                "NO_TRADE",
                0,
                ("MARKET_CLOSED",),
            ),
        )
    try:
        context.adapter.verify_universe(session.session_date)
        context.adapter.provider_smoke()
        context.adapter.verify_sources()
        snapshot = context.adapter.premarket_snapshot(session.session_date)
    except WorkflowDataError as error:
        reason = _safe_reason(error, "DATA_UNAVAILABLE")
        return _publish(
            context,
            "PREMARKET",
            _premarket_result(
                context,
                session.session_date,
                "DATA_UNAVAILABLE",
                3,
                (reason,),
            ),
        )
    if snapshot.breaker_active:
        result = _premarket_result(
            context,
            session.session_date,
            "NO_TRADE",
            0,
            ("ACTIVE_BREAKER",),
        )
    elif not snapshot.candidates:
        result = _premarket_result(
            context,
            session.session_date,
            "NO_TRADE",
            0,
            ("NO_CANDIDATES",),
        )
    else:
        result = _premarket_result(
            context,
            session.session_date,
            "CANDIDATES",
            0,
            ("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED"),
            snapshot.candidates,
        )
    return _publish(context, "PREMARKET", result)


def run_close(context: WorkflowContext) -> WorkflowResult:
    """Run the close review with reconciliation and manual-verification gates."""
    configuration = _configuration_result(context)
    if configuration is not None:
        return configuration
    try:
        session = context.adapter.market_session(context.now.date())
    except WorkflowDataError as error:
        reason = _safe_reason(error, "DATA_UNAVAILABLE")
        return _publish(
            context,
            "CLOSE",
            _close_result(
                context,
                context.now.date(),
                "DATA_UNAVAILABLE",
                3,
                (reason,),
            ),
        )
    if session is None:
        return _publish(
            context,
            "CLOSE",
            _close_result(
                context,
                context.now.date(),
                "NO_TRADE",
                0,
                ("MARKET_CLOSED",),
            ),
        )
    try:
        context.adapter.verify_universe(session.session_date)
        context.adapter.provider_smoke()
        context.adapter.verify_sources()
        snapshot = context.adapter.close_snapshot(session.session_date)
    except WorkflowDataError as error:
        reason = _safe_reason(error, "DATA_UNAVAILABLE")
        return _publish(
            context,
            "CLOSE",
            _close_result(
                context,
                session.session_date,
                "DATA_UNAVAILABLE",
                3,
                (reason,),
            ),
        )

    effective_state = _effective_close_state(snapshot)
    if effective_state == "RECONCILIATION_REQUIRED":
        result = _close_result(
            context,
            session.session_date,
            "RECONCILIATION_REQUIRED",
            5,
            ("RECONCILIATION_REQUIRED",),
            snapshot.positions,
        )
    elif effective_state in {"POSITION_UNVERIFIED", "STOP_UNVERIFIED"}:
        result = _close_result(
            context,
            session.session_date,
            effective_state,
            4,
            (effective_state,),
            snapshot.positions,
        )
    elif effective_state == "DATA_UNAVAILABLE":
        result = _close_result(
            context,
            session.session_date,
            effective_state,
            3,
            (effective_state,),
            snapshot.positions,
        )
    else:
        result = _close_result(
            context,
            session.session_date,
            effective_state,
            0,
            ("MANUAL_VERIFICATION_REQUIRED",),
            snapshot.positions,
        )
    return _publish(context, "CLOSE", result)


class JournalWorkflowPublisher:
    """Use the Journal's immediate claim and atomic report/outbox finalization."""

    def __init__(self, journal: object, report_archive_root: Path) -> None:
        from .journal import Journal

        if not isinstance(journal, Journal):
            raise TypeError("journal must be a Journal")
        if not isinstance(report_archive_root, Path):
            raise TypeError("report archive root must be a pathlib.Path")
        self.journal = journal
        self.report_archive_root = report_archive_root

    def publish(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
        _snapshot_function: object = _issued_report_snapshot,
        _snapshot_code: object = _issued_report_snapshot.__code__,
        _snapshot_globals: object = _issued_report_snapshot.__globals__,
        _snapshot_registry: object = _issued_report_snapshot.__globals__.get(
            "_ISSUED_REPORTS"
        ),
        _snapshot_lock: object = _issued_report_snapshot.__globals__.get(
            "_ISSUED_REPORTS_LOCK"
        ),
        _snapshot_fingerprint: object = _issued_report_snapshot.__globals__.get(
            "_report_fingerprint"
        ),
        _snapshot_fingerprint_code: object = getattr(
            _issued_report_snapshot.__globals__.get("_report_fingerprint"),
            "__code__",
            None,
        ),
        _snapshot_report_type: object = _issued_report_snapshot.__globals__.get(
            "Report"
        ),
        _archive_function: object = archive_report,
        _archive_code: object = archive_report.__code__,
    ) -> PublishedWorkflow:
        def snapshot_dependencies_are_current() -> bool:
            return bool(
                _issued_report_snapshot is _snapshot_function
                and _snapshot_function.__code__ is _snapshot_code
                and _snapshot_function.__globals__ is _snapshot_globals
                and _snapshot_globals.get("_ISSUED_REPORTS")
                is _snapshot_registry
                and _snapshot_globals.get("_ISSUED_REPORTS_LOCK")
                is _snapshot_lock
                and _snapshot_globals.get("_report_fingerprint")
                is _snapshot_fingerprint
                and _snapshot_fingerprint.__code__
                is _snapshot_fingerprint_code
                and _snapshot_globals.get("Report") is _snapshot_report_type
                and archive_report is _archive_function
                and _archive_function.__code__ is _archive_code
            )

        def report_material(value: object) -> tuple[object, ...] | None:
            if (
                type(value) is not Report
                or type(value.report_id) is not str
                or type(value.kind) is not str
                or type(value.session_date) is not date
                or type(value.outcome) is not str
                or type(value.body) is not str
                or type(value.content_sha256) is not str
                or type(value.observation_ids) is not tuple
                or any(type(item) is not str for item in value.observation_ids)
                or type(value.state_hash) is not str
            ):
                return None
            return (
                value.report_id,
                value.kind,
                value.session_date,
                value.outcome,
                value.body,
                value.content_sha256,
                value.observation_ids,
                value.state_hash,
            )

        root = self.report_archive_root
        if type(root) is not type(Path()) or root.is_symlink():
            raise WorkflowError("report archive root is unverified")

        def archive_is_current(report: Report) -> bool:
            if self.report_archive_root is not root or root.is_symlink():
                return False
            expected = root / report.archive_relative_path
            current = root
            for part in report.archive_relative_path.split("/"):
                current = current / part
                if current.is_symlink():
                    return False
            try:
                archived = expected.read_bytes()
            except OSError:
                return False
            return bool(
                expected.is_file()
                and expected.resolve(strict=False)
                == (root.resolve(strict=False) / report.archive_relative_path).resolve(
                    strict=False
                )
                and archived == report.body.encode("utf-8")
                and hashlib.sha256(archived).hexdigest() == report.content_sha256
            )

        if not snapshot_dependencies_are_current():
            raise WorkflowError("report publication dependencies were replaced")
        require_aware_timestamp(generated_at, "report publication time")
        authority = _issued_workflow_publication(
            result,
            self,
            kind,
            session_date,
            generated_at,
        )
        if authority is None:
            raise WorkflowError(
                "durable publication requires a workflow-issued fixture result"
            )
        issued_report, source_observation_row_ids = authority
        report = _snapshot_function(issued_report)
        if report is None:
            raise WorkflowError(
                "durable publication requires a renderer-issued report"
            )
        expected_report_material = report_material(issued_report)
        if (
            not snapshot_dependencies_are_current()
            or expected_report_material is None
            or report_material(report) != expected_report_material
        ):
            raise WorkflowError("renderer-issued report snapshot is unverified")
        if not source_observation_row_ids:
            raise WorkflowError("durable publication requires source observations")
        if report.kind != kind or report.session_date != session_date:
            raise WorkflowError("rendered report identity conflicts with publication")

        claim = self.journal.claim_report(session_date, kind)
        if (
            not snapshot_dependencies_are_current()
            or report_material(report) != expected_report_material
        ):
            raise WorkflowError("report authority changed during its claim")
        if claim.status == "ALREADY_FINALIZED":
            if claim.report_id is None or claim.report_row_id is None:
                raise WorkflowError("finalized report identity is unavailable")
            stored = self.journal.read_report(claim.report_id)
            if (
                stored.body != report.body
                or stored.state_sha256 != report.state_hash
                or stored.report_id != report.report_id
                or frozenset(stored.observation_ids)
                != frozenset(source_observation_row_ids)
            ):
                raise WorkflowError("finalized report conflicts with rendered material")
            _archive_function(report, root)
            if (
                not snapshot_dependencies_are_current()
                or report_material(report) != expected_report_material
                or not archive_is_current(report)
            ):
                raise WorkflowError("report archive verification failed")
            return PublishedWorkflow(
                claim.report_id,
                claim.report_row_id,
                str(root / report.archive_relative_path),
                "ALREADY_EMITTED",
            )
        if claim.status == "IN_PROGRESS":
            return PublishedWorkflow(None, None, None, "IN_PROGRESS")
        if claim.claim_token is None:
            raise WorkflowError("acquired report claim lacks its token")
        # ``generated_at`` is the economic/scenario time represented in the
        # body. Journal creation time is the actual persistence time and must
        # fall within the real claim/finalization interval.
        recorded_at = datetime.now(timezone.utc)
        if (
            not snapshot_dependencies_are_current()
            or report_material(report) != expected_report_material
        ):
            raise WorkflowError("report authority changed before finalization")
        finalized = self.journal.finalize_report(
            claim_id=claim.claim_id,
            claim_token=claim.claim_token,
            body=report.body,
            state_sha256=report.state_hash,
            observation_ids=source_observation_row_ids,
            archive_relative_path=report.archive_relative_path,
            created_at=recorded_at,
            outbox_destination="CODEX_TASK",
            outbox_payload=report.body,
        )
        if finalized.report_id != report.report_id:
            raise WorkflowError("journal report identity conflicts with rendered material")
        if (
            not snapshot_dependencies_are_current()
            or report_material(report) != expected_report_material
        ):
            raise WorkflowError("report authority changed during finalization")
        _archive_function(report, root)
        if (
            not snapshot_dependencies_are_current()
            or report_material(report) != expected_report_material
            or not archive_is_current(report)
        ):
            raise WorkflowError("report archive verification failed")
        return PublishedWorkflow(
            finalized.report_id,
            finalized.report_row_id,
            str(root / report.archive_relative_path),
        )

    def heal_finalized(
        self, *, kind: str, session_date: date
    ) -> PublishedWorkflow:
        del kind, session_date
        raise WorkflowError(
            "scheduled healing requires the exact run_scheduled authority"
        )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class RecordedScenarioAdapter:
    """Strict fixture adapter; expected outcomes never drive decisions."""

    now: datetime
    configuration: str
    market: str
    review_time: time
    universe: str
    provider_fixture: str
    evidence_fixture: str
    breaker: str
    candidates: tuple[CandidateSummary, ...]
    close_state: str
    close_positions: tuple[ClosePosition | UnverifiedClosePosition, ...]
    expected_outcome: str
    evidence: ReportEvidence
    fixture_payload: bytes

    @property
    def execution_mode(self) -> str:
        return "FIXTURE"

    @classmethod
    def load(cls, path: Path) -> RecordedScenarioAdapter:
        if cls is not RecordedScenarioAdapter:
            raise WorkflowBoundaryError(
                "recorded scenario loading requires the exact adapter type"
            )
        if not isinstance(path, Path) or not path.is_file():
            raise ValueError("scenario fixture is unavailable")
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise ValueError("scenario fixture could not be read") from error
        adapter = _recorded_scenario_from_payload(payload)
        identity = id(adapter)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_RECORDED_ADAPTERS_LOCK:
                current = _ISSUED_RECORDED_ADAPTERS.get(identity)
                if current is not None and current[0] is dead:
                    _ISSUED_RECORDED_ADAPTERS.pop(identity, None)

        fingerprint = _recorded_adapter_fingerprint(adapter)
        if fingerprint is None:
            raise WorkflowBoundaryError(
                "recorded scenario adapter could not be fingerprinted"
            )
        with _ISSUED_RECORDED_ADAPTERS_LOCK:
            _ISSUED_RECORDED_ADAPTERS[identity] = (
                ref(adapter, discard),
                fingerprint,
                None,
                None,
                None,
                None,
            )
        return adapter

    def bind_source_observation(
        self, journal: object
    ) -> RecordedScenarioAdapter:
        """Append and bind this issued fixture's exact Journal source row."""
        from .journal import Journal

        if type(self) is not RecordedScenarioAdapter:
            raise WorkflowBoundaryError(
                "fixture source binding requires the exact recorded adapter type"
            )
        if type(journal) is not Journal:
            raise TypeError("fixture source journal must be an exact Journal")
        current_fingerprint = _recorded_adapter_fingerprint(self)
        with _ISSUED_RECORDED_ADAPTERS_LOCK:
            issued = _ISSUED_RECORDED_ADAPTERS.get(id(self))
            if (
                issued is None
                or issued[0]() is not self
                or issued[1] is None
                or current_fingerprint is None
                or issued[1] != current_fingerprint
                or issued[2] is not None
            ):
                raise WorkflowBoundaryError(
                    "fixture source binding requires an issued recorded adapter"
                )
        captured_payload = self.fixture_payload
        captured_now = self.now
        captured_state_hash = self.evidence.state_hash
        fixture_root = _content_addressed_fixture_root(
            journal.path,
            captured_state_hash,
        )
        source_values = _fixture_source_values(captured_payload, captured_now)
        row_id, _ = journal.append_source_observation(**source_values)
        observation_sha256 = _fixture_observation_sha256(source_values)
        with _ISSUED_RECORDED_ADAPTERS_LOCK:
            post_append = _ISSUED_RECORDED_ADAPTERS.get(id(self))
            if post_append is None or post_append[0]() is not self:
                raise WorkflowBoundaryError(
                    "fixture source adapter identity changed during binding"
                )
        fresh = _recorded_scenario_from_payload(captured_payload)
        fresh_fingerprint = _recorded_adapter_fingerprint(fresh)
        if fresh_fingerprint is None or fresh_fingerprint != current_fingerprint:
            raise WorkflowBoundaryError(
                "fixture payload no longer matches its loaded adapter"
            )
        bound = replace(
            fresh,
            evidence=ReportEvidence(
                (observation_sha256,),
                captured_state_hash,
                (row_id,),
            ),
        )
        identity = id(bound)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_RECORDED_ADAPTERS_LOCK:
                current = _ISSUED_RECORDED_ADAPTERS.get(identity)
                if current is not None and current[0] is dead:
                    _ISSUED_RECORDED_ADAPTERS.pop(identity, None)

        bound_fingerprint = _recorded_adapter_fingerprint(bound)
        if bound_fingerprint is None:
            raise WorkflowBoundaryError(
                "bound recorded scenario adapter could not be fingerprinted"
            )
        with _ISSUED_RECORDED_ADAPTERS_LOCK:
            _ISSUED_RECORDED_ADAPTERS[identity] = (
                ref(bound, discard),
                bound_fingerprint,
                ref(journal),
                fixture_root,
                row_id,
                observation_sha256,
            )
        return bound

    def validate_configuration(self) -> None:
        if self.configuration != "READY":
            raise WorkflowConfigurationError("CONFIGURATION_REQUIRED")

    def market_session(self, day: date) -> SessionWindow | None:
        if self.market == "STALE":
            raise WorkflowDataError("STALE_CALENDAR")
        if self.market == "CLOSED":
            return None
        return SessionWindow(day, self.review_time)

    def verify_universe(self, day: date) -> None:
        del day
        if self.universe != "READY":
            raise WorkflowDataError("STALE_UNIVERSE")

    def provider_smoke(self) -> None:
        if self.provider_fixture != "READY":
            raise WorkflowDataError("PROVIDER_CHECK_FAILED")

    def verify_sources(self) -> None:
        if self.evidence_fixture != "READY":
            raise WorkflowDataError("SOURCE_CHECK_FAILED")

    def report_evidence(self) -> ReportEvidence:
        return self.evidence

    def premarket_snapshot(self, day: date) -> PremarketSnapshot:
        del day
        return PremarketSnapshot(self.candidates, self.breaker == "ACTIVE")

    def close_snapshot(self, day: date) -> CloseSnapshot:
        del day
        return CloseSnapshot(self.close_state, self.close_positions)


def _recorded_scenario_from_payload(payload: bytes) -> RecordedScenarioAdapter:
    """Parse immutable fixture bytes without granting adapter authority."""
    if type(payload) is not bytes:
        raise TypeError("scenario fixture payload must be exact bytes")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("scenario fixture could not be read") from error
    table = _strict_mapping(document, _FIXTURE_KEYS, "scenario")
    raw_account = table["account"]
    if not isinstance(raw_account, dict) or frozenset(raw_account) not in {
        _ACCOUNT_KEYS,
        _ACCOUNT_REPORT_KEYS,
    }:
        raise ValueError("account fixture fields are invalid")
    account = raw_account
    now = _aware_datetime(table["now"])
    review_time = _wall_time(account["review_time"])
    candidates_raw = account["candidates"]
    if not isinstance(candidates_raw, list):
        raise ValueError("scenario candidates must be a list")
    candidates = tuple(_recorded_candidate(item) for item in candidates_raw)
    close_positions_raw = account.get("close_positions", [])
    if not isinstance(close_positions_raw, list):
        raise ValueError("close positions must be a list")
    close_positions = tuple(
        _recorded_close_position(item) for item in close_positions_raw
    )
    values = {
        "configuration": table_value(account, "configuration", {"READY", "MISSING"}),
        "market": table_value(account, "market", {"OPEN", "CLOSED", "STALE"}),
        "universe": table_value(account, "universe", {"READY", "STALE"}),
        "provider_fixture": table_value(
            table,
            "provider_fixture",
            {"READY", "FAILED"},
        ),
        "evidence_fixture": table_value(
            table,
            "evidence_fixture",
            {"READY", "FAILED"},
        ),
        "breaker": table_value(account, "breaker", {"CLEAR", "ACTIVE"}),
        "close_state": table_value(account, "close_state", _CLOSE_STATES),
        "expected_outcome": table_value(table, "expected_outcome", None),
    }
    fixture_sha256 = hashlib.sha256(payload).hexdigest()
    return RecordedScenarioAdapter(
        now,
        review_time=review_time,
        candidates=candidates,
        close_positions=close_positions,
        evidence=ReportEvidence((fixture_sha256,), fixture_sha256),
        fixture_payload=payload,
        **values,
    )


_INVALID_STRUCTURE = object()


def _structural_fingerprint(
    value: object,
    active: set[int] | None = None,
) -> tuple[object, ...] | object:
    """Snapshot exact approved types without invoking caller comparison hooks."""
    value_type = type(value)
    if value is None:
        return ("NONE",)
    if value_type is str:
        return ("STR", value)
    if value_type is bool:
        return ("BOOL", value)
    if value_type is int:
        return ("INT", value)
    if value_type is bytes:
        return ("BYTES", value)
    if value_type is Decimal:
        decimal_tuple = value.as_tuple()
        if type(decimal_tuple.exponent) is not int:
            return _INVALID_STRUCTURE
        return (
            "DECIMAL",
            decimal_tuple.sign,
            tuple(decimal_tuple.digits),
            decimal_tuple.exponent,
        )
    if value_type is date:
        return ("DATE", value.year, value.month, value.day)
    if value_type is datetime:
        zone = value.tzinfo
        if type(zone) is not timezone:
            return _INVALID_STRUCTURE
        offset = zone.utcoffset(None)
        name = zone.tzname(None)
        if type(offset) is not timedelta or type(name) is not str:
            return _INVALID_STRUCTURE
        return (
            "DATETIME",
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.fold,
            offset.days,
            offset.seconds,
            offset.microseconds,
            name,
        )
    if value_type is time:
        zone = value.tzinfo
        if zone is None:
            zone_fingerprint: tuple[object, ...] = ("NAIVE",)
        elif type(zone) is timezone:
            offset = zone.utcoffset(None)
            name = zone.tzname(None)
            if type(offset) is not timedelta or type(name) is not str:
                return _INVALID_STRUCTURE
            zone_fingerprint = (
                "TIMEZONE",
                offset.days,
                offset.seconds,
                offset.microseconds,
                name,
            )
        else:
            return _INVALID_STRUCTURE
        return (
            "TIME",
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.fold,
            zone_fingerprint,
        )

    if active is None:
        active = set()
    identity = id(value)
    if identity in active:
        return _INVALID_STRUCTURE
    if value_type is tuple:
        active.add(identity)
        try:
            items: list[tuple[object, ...]] = []
            for item in value:
                item_fingerprint = _structural_fingerprint(item, active)
                if item_fingerprint is _INVALID_STRUCTURE:
                    return _INVALID_STRUCTURE
                assert isinstance(item_fingerprint, tuple)
                items.append(item_fingerprint)
            return ("TUPLE", tuple(items))
        finally:
            active.remove(identity)

    if value_type is CandidateSummary:
        tag = "CANDIDATE_SUMMARY"
        field_names = ("symbol", "role", "material")
    elif value_type is PremarketCandidate:
        tag = "PREMARKET_CANDIDATE"
        field_names = (
            "symbol",
            "role",
            "setup",
            "score_components",
            "trigger",
            "maximum_entry",
            "recommended_stop",
            "target",
            "shares",
            "planned_risk",
            "provider",
            "feed",
            "observed_at",
            "invalidations",
            "sources",
        )
    elif value_type is PremarketShadow:
        tag = "PREMARKET_SHADOW"
        field_names = (
            "symbol",
            "role",
            "score",
            "setup",
            "trigger",
        )
    elif value_type is ScoreComponent:
        tag = "SCORE_COMPONENT"
        field_names = ("label", "earned", "available")
    elif value_type is ReportSource:
        tag = "REPORT_SOURCE"
        field_names = ("label", "url")
    elif value_type is ClosePosition:
        tag = "CLOSE_POSITION"
        field_names = (
            "symbol",
            "shares",
            "mark",
            "estimated_unrealized_pl",
            "r_multiple",
            "recommended_stop",
            "user_confirmed_stop",
            "target",
            "holding_days",
            "provider",
            "feed",
            "observed_at",
            "upcoming_events",
            "evidence",
            "action",
            "reason_codes",
        )
    elif value_type is UnverifiedClosePosition:
        tag = "UNVERIFIED_CLOSE_POSITION"
        field_names = (
            "symbol",
            "shares",
            "exact_cost_basis",
            "status",
            "reason_codes",
        )
    elif value_type is ReportEvidence:
        tag = "REPORT_EVIDENCE"
        field_names = (
            "observation_ids",
            "state_hash",
            "source_observation_row_ids",
        )
    elif value_type is Report:
        tag = "REPORT"
        field_names = (
            "report_id",
            "kind",
            "session_date",
            "outcome",
            "body",
            "content_sha256",
            "observation_ids",
            "state_hash",
        )
    elif value_type is WorkflowResult:
        tag = "WORKFLOW_RESULT"
        field_names = (
            "outcome",
            "message",
            "exit_code",
            "reason_codes",
            "candidates",
            "report",
            "source_observation_row_ids",
            "execution_mode",
            "report_id",
            "report_row_id",
            "report_path",
        )
    elif value_type is RecordedScenarioAdapter:
        tag = "RECORDED_SCENARIO_ADAPTER"
        field_names = (
            "now",
            "configuration",
            "market",
            "review_time",
            "universe",
            "provider_fixture",
            "evidence_fixture",
            "breaker",
            "candidates",
            "close_state",
            "close_positions",
            "expected_outcome",
            "evidence",
            "fixture_payload",
        )
    else:
        return _INVALID_STRUCTURE

    active.add(identity)
    try:
        fields: list[tuple[object, ...]] = []
        for field_name in field_names:
            field_fingerprint = _structural_fingerprint(
                getattr(value, field_name),
                active,
            )
            if field_fingerprint is _INVALID_STRUCTURE:
                return _INVALID_STRUCTURE
            assert isinstance(field_fingerprint, tuple)
            fields.append(field_fingerprint)
        return (tag, tuple(fields))
    finally:
        active.remove(identity)


def _recorded_adapter_fingerprint(
    adapter: RecordedScenarioAdapter,
) -> tuple[object, ...] | None:
    fingerprint = _structural_fingerprint(adapter)
    return fingerprint if isinstance(fingerprint, tuple) else None


def _workflow_result_fingerprint(
    result: WorkflowResult,
) -> tuple[object, ...] | None:
    fingerprint = _structural_fingerprint(result)
    return fingerprint if isinstance(fingerprint, tuple) else None


def _content_addressed_fixture_root(journal_path: Path, state_hash: str) -> Path:
    if _LOWER_SHA256.fullmatch(state_hash) is None:
        raise WorkflowBoundaryError("fixture state identity is invalid")
    normalized_journal = Path(os.path.abspath(journal_path))
    fixture_root = normalized_journal.parent
    fixtures_root = fixture_root.parent
    state_root = fixtures_root.parent
    if (
        normalized_journal.name != "journal.sqlite3"
        or fixture_root.name != state_hash
        or fixtures_root.name != "fixtures"
    ):
        raise WorkflowBoundaryError(
            "fixture journal must use its content-addressed sandbox"
        )
    if any(
        path.is_symlink()
        for path in (state_root, fixtures_root, fixture_root, normalized_journal)
    ):
        raise WorkflowBoundaryError("fixture journal cannot traverse a symlink")
    try:
        if (
            normalized_journal.resolve(strict=True).parent
            != fixture_root.resolve(strict=True)
            or fixture_root.resolve(strict=True).parent
            != fixtures_root.resolve(strict=True)
            or fixtures_root.resolve(strict=True).parent
            != state_root.resolve(strict=True)
        ):
            raise WorkflowBoundaryError("fixture journal escapes its sandbox")
    except OSError as error:
        raise WorkflowBoundaryError("fixture journal is unavailable") from error
    return fixture_root


def _fixture_source_values(payload: bytes, observed_at: datetime) -> dict[str, object]:
    if type(payload) is not bytes or type(observed_at) is not datetime:
        raise TypeError("fixture source plan requires immutable normalized values")
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "payload": payload,
        "source_uri": f"fixture://recorded-scenario/{digest}",
        "source_type": "RECORDED_SCENARIO",
        "provider": "FIXTURE",
        "feed": "RECORDED_SCENARIO",
        "source_time": observed_at,
        "retrieved_at": observed_at,
        "provider_sequence": None,
        "delay_seconds": 0,
        "health_result": "OK",
    }


def _fixture_observation_sha256(values: Mapping[str, object]) -> str:
    payload = values["payload"]
    if not isinstance(payload, bytes):
        raise TypeError("fixture payload must be bytes")
    material = {
        "delay_seconds": values["delay_seconds"],
        "details": {},
        "feed": values["feed"],
        "health_result": values["health_result"],
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "provider": values["provider"],
        "provider_sequence": values["provider_sequence"],
        "retrieved_at": _fixture_timestamp(values["retrieved_at"]),
        "source_time": _fixture_timestamp(values["source_time"]),
        "source_type": values["source_type"],
        "source_uri": values["source_uri"],
    }
    canonical = json.dumps(
        material,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fixture_timestamp(value: object) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("fixture source time must be timezone-aware")
    utc = value.astimezone(timezone.utc)
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}."
        f"{utc.microsecond:06d}Z"
    )


def _bound_fixture_authority(
    context: WorkflowContext,
    evidence: ReportEvidence,
) -> tuple[
    RecordedScenarioAdapter,
    object,
    Path,
    tuple[int, ...],
    tuple[str, ...],
] | None:
    adapter = context.adapter
    publisher = context.publisher
    if (
        type(adapter) is not RecordedScenarioAdapter
        or type(publisher) is not JournalWorkflowPublisher
        or adapter.execution_mode != "FIXTURE"
    ):
        return None
    current_adapter_fingerprint = _recorded_adapter_fingerprint(adapter)
    with _ISSUED_RECORDED_ADAPTERS_LOCK:
        issued = _ISSUED_RECORDED_ADAPTERS.get(id(adapter))
        if (
            issued is None
            or issued[0]() is not adapter
            or issued[1] is None
            or current_adapter_fingerprint is None
            or issued[1] != current_adapter_fingerprint
            or issued[2] is None
            or issued[3] is None
            or issued[4] is None
            or issued[5] is None
        ):
            return None
        journal = issued[2]()
        fixture_root = issued[3]
        row_id = issued[4]
        observation_sha256 = issued[5]
    if journal is None or publisher.journal is not journal:
        return None
    try:
        current_root = _content_addressed_fixture_root(
            journal.path,
            adapter.evidence.state_hash,
        )
    except WorkflowBoundaryError:
        return None
    archive_root = Path(os.path.abspath(publisher.report_archive_root))
    if (
        archive_root != current_root
        or archive_root.is_symlink()
        or fixture_root != current_root
        or evidence != adapter.evidence
        or evidence.source_observation_row_ids != (row_id,)
        or evidence.observation_ids != (observation_sha256,)
    ):
        return None
    return (
        adapter,
        journal,
        current_root,
        (row_id,),
        (observation_sha256,),
    )


def _issued_workflow_publication(
    result: object,
    publisher: JournalWorkflowPublisher,
    kind: str,
    session_date: date,
    generated_at: datetime,
) -> tuple[object, tuple[int, ...]] | None:
    if (
        type(result) is not WorkflowResult
        or type(kind) is not str
        or type(session_date) is not date
        or type(generated_at) is not datetime
    ):
        return None
    current_result_fingerprint = _workflow_result_fingerprint(result)
    with _ISSUED_WORKFLOW_RESULTS_LOCK:
        issued = _ISSUED_WORKFLOW_RESULTS.get(id(result))
        if (
            issued is None
            or issued[0]() is not result
            or issued[1] is None
            or current_result_fingerprint is None
            or issued[1] != current_result_fingerprint
            or issued[6:] != (
                kind,
                session_date,
                generated_at,
                result.source_observation_row_ids,
                result.report.observation_ids if result.report is not None else (),
            )
        ):
            return None
        adapter = issued[2]()
        journal = issued[4]()
        fixture_root = issued[5]
        adapter_fingerprint = issued[3]
    current_adapter_fingerprint = (
        _recorded_adapter_fingerprint(adapter)
        if type(adapter) is RecordedScenarioAdapter
        else None
    )
    if (
        type(adapter) is not RecordedScenarioAdapter
        or adapter_fingerprint is None
        or current_adapter_fingerprint is None
        or adapter_fingerprint != current_adapter_fingerprint
        or journal is None
        or publisher.journal is not journal
        or result.execution_mode != "FIXTURE"
        or result.report is None
        or result.report.observation_ids != issued[10]
        or result.source_observation_row_ids != issued[9]
    ):
        return None
    try:
        current_root = _content_addressed_fixture_root(
            journal.path,
            result.report.state_hash,
        )
    except WorkflowBoundaryError:
        return None
    if (
        current_root != fixture_root
        or Path(os.path.abspath(publisher.report_archive_root)) != current_root
        or publisher.report_archive_root.is_symlink()
    ):
        return None
    return result.report, issued[9]


def _configuration_result(context: WorkflowContext) -> WorkflowResult | None:
    try:
        context.adapter.validate_configuration()
    except WorkflowConfigurationError:
        return _result(
            "CONFIGURATION_REQUIRED",
            2,
            ("CONFIGURATION_REQUIRED",),
            "CONFIGURATION REQUIRED\nNo candidate or position action was produced.",
        )
    return None


def _publish(
    context: WorkflowContext,
    kind: str,
    result: WorkflowResult,
) -> WorkflowResult:
    if context.publisher is None:
        return result
    published = context.publisher.publish(
        kind=kind,
        session_date=context.now.date(),
        generated_at=context.now,
        result=result,
    )
    if published.status == "IN_PROGRESS":
        return WorkflowResult(
            outcome="PUBLICATION_INCOMPLETE",
            message="PUBLICATION INCOMPLETE - DURABLE CLAIM HAS NO REPORT",
            exit_code=10,
            reason_codes=("PUBLICATION_IN_PROGRESS",),
            execution_mode=result.execution_mode,
        )
    if published.status == "ALREADY_EMITTED":
        return WorkflowResult(
            outcome="ALREADY_EMITTED_NOOP",
            message="ALREADY EMITTED - NOOP",
            exit_code=0,
            reason_codes=(published.status,),
            execution_mode=result.execution_mode,
            report_id=published.report_id,
            report_row_id=published.report_row_id,
            report_path=published.report_path,
        )
    return replace(
        result,
        report_id=published.report_id,
        report_row_id=published.report_row_id,
        report_path=published.report_path,
    )


def _result(
    outcome: str,
    exit_code: int,
    reasons: tuple[str, ...],
    message: str,
    candidates: tuple[CandidateSummary, ...] = (),
) -> WorkflowResult:
    return WorkflowResult(outcome, message, exit_code, reasons, candidates)


def _premarket_result(
    context: WorkflowContext,
    session_date: date,
    outcome: str,
    exit_code: int,
    reasons: tuple[str, ...],
    candidates: tuple[CandidateSummary, ...] = (),
) -> WorkflowResult:
    evidence = context.adapter.report_evidence()
    execution_mode = getattr(context.adapter, "execution_mode", "CANONICAL")
    report_reasons = (
        ("FIXTURE", *reasons) if execution_mode == "FIXTURE" else reasons
    )
    materials = tuple(candidate.material for candidate in candidates)
    if any(material is None for material in materials):
        raise WorkflowError("candidate report material is incomplete")
    report = render_premarket_report(
        PremarketState(
            session_date=session_date,
            generated_at=context.now,
            outcome={
                "CANDIDATES": "CANDIDATES",
                "NO_TRADE": "NO TRADE",
                "DATA_UNAVAILABLE": "NO NEW TRADE - DATA UNAVAILABLE",
            }[outcome],
            reason_codes=report_reasons,
            observation_ids=evidence.observation_ids,
            state_hash=evidence.state_hash,
            candidates=tuple(
                material for material in materials if material is not None
            ),
        )
    )
    result = WorkflowResult(
        outcome=outcome,
        message=report.body,
        exit_code=exit_code,
        reason_codes=reasons,
        candidates=candidates,
        report=report,
        source_observation_row_ids=evidence.source_observation_row_ids,
        execution_mode=execution_mode,
    )
    frame = inspect.currentframe()
    try:
        caller = None if frame is None else frame.f_back
        authority = (
            _bound_fixture_authority(context, evidence)
            if caller is not None and caller.f_code is run_premarket.__code__
            else None
        )
    finally:
        del frame
    if authority is not None:
        adapter, journal, fixture_root, row_ids, observation_ids = authority
        identity = id(result)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_WORKFLOW_RESULTS_LOCK:
                current = _ISSUED_WORKFLOW_RESULTS.get(identity)
                if current is not None and current[0] is dead:
                    _ISSUED_WORKFLOW_RESULTS.pop(identity, None)

        result_fingerprint = _workflow_result_fingerprint(result)
        adapter_fingerprint = _recorded_adapter_fingerprint(adapter)
        if result_fingerprint is None or adapter_fingerprint is None:
            raise WorkflowBoundaryError(
                "workflow result authority could not be fingerprinted"
            )
        with _ISSUED_WORKFLOW_RESULTS_LOCK:
            _ISSUED_WORKFLOW_RESULTS[identity] = (
                ref(result, discard),
                result_fingerprint,
                ref(adapter),
                adapter_fingerprint,
                ref(journal),
                fixture_root,
                "PREMARKET",
                session_date,
                context.now,
                row_ids,
                observation_ids,
            )
    return result


def _close_result(
    context: WorkflowContext,
    session_date: date,
    outcome: str,
    exit_code: int,
    reasons: tuple[str, ...],
    positions: tuple[ClosePosition | UnverifiedClosePosition, ...] = (),
) -> WorkflowResult:
    evidence = context.adapter.report_evidence()
    execution_mode = getattr(context.adapter, "execution_mode", "CANONICAL")
    report_reasons = (
        ("FIXTURE", *reasons) if execution_mode == "FIXTURE" else reasons
    )
    report = render_close_report(
        CloseState(
            session_date=session_date,
            generated_at=context.now,
            reason_codes=report_reasons,
            positions=positions,
            observation_ids=evidence.observation_ids,
            state_hash=evidence.state_hash,
            reconciliation_required=outcome == "RECONCILIATION_REQUIRED",
            position_verified=outcome != "POSITION_UNVERIFIED",
            stop_verified=outcome != "STOP_UNVERIFIED",
            data_available=outcome != "DATA_UNAVAILABLE",
            exit_due=outcome == "EXIT",
            tighten_stop_due=outcome == "TIGHTEN_STOP",
        )
    )
    result = WorkflowResult(
        outcome=outcome,
        message=report.body,
        exit_code=exit_code,
        reason_codes=reasons,
        report=report,
        source_observation_row_ids=evidence.source_observation_row_ids,
        execution_mode=execution_mode,
    )
    frame = inspect.currentframe()
    try:
        caller = None if frame is None else frame.f_back
        authority = (
            _bound_fixture_authority(context, evidence)
            if caller is not None and caller.f_code is run_close.__code__
            else None
        )
    finally:
        del frame
    if authority is not None:
        adapter, journal, fixture_root, row_ids, observation_ids = authority
        identity = id(result)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_WORKFLOW_RESULTS_LOCK:
                current = _ISSUED_WORKFLOW_RESULTS.get(identity)
                if current is not None and current[0] is dead:
                    _ISSUED_WORKFLOW_RESULTS.pop(identity, None)

        result_fingerprint = _workflow_result_fingerprint(result)
        adapter_fingerprint = _recorded_adapter_fingerprint(adapter)
        if result_fingerprint is None or adapter_fingerprint is None:
            raise WorkflowBoundaryError(
                "workflow result authority could not be fingerprinted"
            )
        with _ISSUED_WORKFLOW_RESULTS_LOCK:
            _ISSUED_WORKFLOW_RESULTS[identity] = (
                ref(result, discard),
                result_fingerprint,
                ref(adapter),
                adapter_fingerprint,
                ref(journal),
                fixture_root,
                "CLOSE",
                session_date,
                context.now,
                row_ids,
                observation_ids,
            )
    return result


def _safe_reason(error: WorkflowDataError, fallback: str) -> str:
    value = str(error)
    return value if value in _SAFE_DATA_REASON_CODES else fallback


def _recorded_candidate(value: object) -> CandidateSummary:
    table = _strict_mapping(value, _CANDIDATE_KEYS, "candidate")
    scores_raw = table["score_components"]
    if not isinstance(scores_raw, list):
        raise ValueError("candidate score components must be a list")
    scores = tuple(
        ScoreComponent(
            label=str(_strict_mapping(item, _SCORE_KEYS, "score component")["label"]),
            earned=_exact_int(
                _strict_mapping(item, _SCORE_KEYS, "score component")["earned"],
                "score earned",
            ),
            available=_exact_int(
                _strict_mapping(item, _SCORE_KEYS, "score component")["available"],
                "score available",
            ),
        )
        for item in scores_raw
    )
    invalidations_raw = table["invalidations"]
    sources_raw = table["sources"]
    if not isinstance(invalidations_raw, list) or not all(
        isinstance(item, str) for item in invalidations_raw
    ):
        raise ValueError("candidate invalidations must be a text list")
    if not isinstance(sources_raw, list):
        raise ValueError("candidate sources must be a list")
    sources = tuple(
        ReportSource(
            label=str(_strict_mapping(item, _SOURCE_KEYS, "candidate source")["label"]),
            url=str(_strict_mapping(item, _SOURCE_KEYS, "candidate source")["url"]),
        )
        for item in sources_raw
    )
    symbol = str(table["symbol"])
    role = str(table["role"])
    material = PremarketCandidate(
        symbol=symbol,
        role=role,
        setup=str(table["setup"]),
        score_components=scores,
        trigger=_decimal_text(table["trigger"], "candidate trigger"),
        maximum_entry=_decimal_text(table["maximum_entry"], "candidate maximum entry"),
        recommended_stop=_decimal_text(
            table["recommended_stop"], "candidate recommended stop"
        ),
        target=_decimal_text(table["target"], "candidate target"),
        shares=_exact_int(table["shares"], "candidate shares"),
        planned_risk=_decimal_text(table["planned_risk"], "candidate planned risk"),
        provider=str(table["provider"]),
        feed=str(table["feed"]),
        observed_at=_aware_datetime(table["observed_at"]),
        invalidations=tuple(str(item) for item in invalidations_raw),
        sources=sources,
    )
    return CandidateSummary(symbol=symbol, role=role, material=material)


def _recorded_close_position(value: object) -> ClosePosition:
    table = _strict_mapping(value, _CLOSE_POSITION_KEYS, "close position")
    events = table["upcoming_events"]
    evidence = table["evidence"]
    if not isinstance(events, list) or not all(isinstance(item, str) for item in events):
        raise ValueError("close upcoming events must be a text list")
    if not isinstance(evidence, list) or not evidence:
        raise ValueError("close evidence must be a non-empty list")
    stop_raw = table["user_confirmed_stop"]
    stop = (
        None
        if stop_raw is None
        else _decimal_text(stop_raw, "close user-confirmed stop")
    )
    return ClosePosition(
        symbol=str(table["symbol"]),
        shares=_exact_int(table["shares"], "close shares"),
        mark=_decimal_text(table["mark"], "close mark"),
        estimated_unrealized_pl=_decimal_text(
            table["estimated_unrealized_pl"], "close estimated unrealized P/L"
        ),
        r_multiple=_decimal_text(table["r_multiple"], "close R multiple"),
        recommended_stop=_decimal_text(
            table["recommended_stop"], "close recommended stop"
        ),
        user_confirmed_stop=stop,
        target=_decimal_text(table["target"], "close target"),
        holding_days=_exact_int(table["holding_days"], "close holding days"),
        provider=str(table["provider"]),
        feed=str(table["feed"]),
        observed_at=_aware_datetime(table["observed_at"]),
        upcoming_events=tuple(str(item) for item in events),
        evidence=tuple(
            ReportSource(
                label=str(_strict_mapping(item, _SOURCE_KEYS, "close source")["label"]),
                url=str(_strict_mapping(item, _SOURCE_KEYS, "close source")["url"]),
            )
            for item in evidence
        ),
    )


def _exact_int(value: object, label: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{label} must be an integer")
    return value


def _decimal_text(value: object, label: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be decimal text")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{label} is invalid") from error
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def _strict_mapping(
    value: object,
    keys: frozenset[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, dict) or frozenset(value) != keys:
        raise ValueError(f"{label} fixture fields are invalid")
    return value


def table_value(
    table: Mapping[str, object],
    name: str,
    allowed: set[str] | frozenset[str] | None,
) -> str:
    value = table[name]
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"scenario {name} must be non-empty text")
    if allowed is not None and value not in allowed:
        raise ValueError(f"scenario {name} is unsupported")
    if name == "expected_outcome" and _TOKEN.fullmatch(value) is None:
        raise ValueError("scenario expected outcome must be canonical")
    return value


def _aware_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("scenario now must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
        return require_aware_timestamp(parsed, "scenario now")
    except (ValueError, TypeError) as error:
        raise ValueError("scenario now must be an aware ISO timestamp") from error


def _wall_time(value: object) -> time:
    if not isinstance(value, str) or re.fullmatch(r"\d{2}:\d{2}", value) is None:
        raise ValueError("scenario review time must use HH:MM")
    try:
        parsed = time.fromisoformat(value)
    except ValueError as error:
        raise ValueError("scenario review time is invalid") from error
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise ValueError("scenario review time must be a minute wall time")
    return parsed


__all__ = [
    "CandidateSummary",
    "CloseSnapshot",
    "JournalWorkflowPublisher",
    "PremarketSnapshot",
    "PublishedWorkflow",
    "RecordedScenarioAdapter",
    "ReportEvidence",
    "ScheduledWorkflowStore",
    "SessionWindow",
    "WorkflowAdapter",
    "WorkflowBoundaryError",
    "WorkflowConfigurationError",
    "WorkflowContext",
    "WorkflowDataError",
    "WorkflowError",
    "WorkflowPublisher",
    "WorkflowReconciliationError",
    "WorkflowResult",
    "run_close",
    "run_premarket",
]
