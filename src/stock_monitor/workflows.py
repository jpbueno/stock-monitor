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
from zoneinfo import ZoneInfo

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
_NEW_YORK = ZoneInfo("America/New_York")
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
_ISSUED_CANONICAL_RESULTS_LOCK = threading.Lock()
_ISSUED_CANONICAL_RESULTS: dict[int, _CanonicalResultAuthority] = {}


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
        if type(self.breaker_active) is not bool:
            raise TypeError("breaker_active must be an exact boolean")
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


class CanonicalWorkflowAdapter(Protocol):
    """Separate provider-backed adapter with fixed economic and retrieval times."""

    def market_session(self, day: date) -> SessionWindow | None: ...

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


class CanonicalWorkflowPublisher(Protocol):
    """Publisher that also requires exact canonical material authority."""

    def issue_result(
        self,
        *,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
    ) -> WorkflowResult: ...

    def publish(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
    ) -> PublishedWorkflow: ...


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPublicationPlan:
    """Verified, side-effect-free inputs for canonical Journal finalization."""

    workflow_kind: str
    storage_kind: str
    session_date: date
    generated_at: datetime
    economic_at: datetime
    retrieved_at: datetime
    material_digest: str
    source_digest: str
    source_observation_row_ids: tuple[int, ...]
    rendered_report_id: str
    report: Report
    material: object

    def __post_init__(self) -> None:
        expected_storage = {"PREMARKET": "MORNING", "CLOSE": "CLOSE"}
        if (
            self.workflow_kind not in expected_storage
            or self.storage_kind != expected_storage[self.workflow_kind]
        ):
            raise ValueError("canonical publication storage kind is invalid")
        if type(self.session_date) is not date:
            raise TypeError("canonical publication session must be an exact date")
        require_aware_timestamp(self.generated_at, "canonical generation time")
        require_aware_timestamp(self.economic_at, "canonical economic time")
        require_aware_timestamp(self.retrieved_at, "canonical retrieval time")
        if (
            self.generated_at != self.retrieved_at
            or self.economic_at > self.retrieved_at
            or any(
                value.astimezone(_NEW_YORK).date() != self.session_date
                for value in (
                    self.generated_at,
                    self.economic_at,
                    self.retrieved_at,
                )
            )
        ):
            raise ValueError("canonical publication timing is inconsistent")
        if (
            _LOWER_SHA256.fullmatch(self.material_digest) is None
            or _LOWER_SHA256.fullmatch(self.source_digest) is None
            or _LOWER_SHA256.fullmatch(self.rendered_report_id) is None
        ):
            raise ValueError("canonical publication digest is invalid")
        if type(self.source_observation_row_ids) is not tuple or any(
            type(row_id) is not int or row_id < 1
            for row_id in self.source_observation_row_ids
        ):
            raise ValueError("canonical publication source rows are invalid")
        if len(set(self.source_observation_row_ids)) != len(
            self.source_observation_row_ids
        ):
            raise ValueError("canonical publication source rows must be unique")
        if (
            type(self.report) is not Report
            or self.report.report_id != self.rendered_report_id
            or self.report.kind != self.workflow_kind
            or self.report.session_date != self.session_date
        ):
            raise ValueError("canonical rendered report identity is invalid")

    def archive_relative_path(self, stored_report_id: str) -> str:
        """Derive the archive from the durable kind and durable report ID."""
        from .journal import report_archive_relative_path

        if type(stored_report_id) is not str or _LOWER_SHA256.fullmatch(
            stored_report_id
        ) is None:
            raise ValueError("stored report ID must be a SHA-256 digest")
        return report_archive_relative_path(
            self.storage_kind,
            self.session_date,
            stored_report_id,
        )


@dataclass(frozen=True, slots=True)
class _CanonicalResultAuthority:
    result_reference: ReferenceType[object]
    result_fingerprint: tuple[object, ...]
    material_reference: ReferenceType[object]
    publisher_reference: ReferenceType[object]
    journal_reference: ReferenceType[object]
    archive_root: Path


@dataclass(frozen=True, slots=True)
class _CanonicalPublicationPlanAuthority:
    plan_reference: ReferenceType[object]
    plan_seal: tuple[object, ...]
    result_reference: ReferenceType[object]
    result_fingerprint: tuple[object, ...]
    material_reference: ReferenceType[object]
    publisher_reference: ReferenceType[object]
    journal_reference: ReferenceType[object]
    journal_generation: int
    archive_root: Path
    publisher_code: object


_ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK = threading.Lock()
_ISSUED_CANONICAL_PUBLICATION_PLANS: dict[
    int, _CanonicalPublicationPlanAuthority
] = {}


def _canonical_publication_plan_seal(
    plan: object,
) -> tuple[object, ...] | None:
    """Capture a hook-free exact-object seal for a publication plan."""
    if type(plan) is not CanonicalPublicationPlan:
        return None
    values = (
        plan.workflow_kind,
        plan.storage_kind,
        plan.material_digest,
        plan.source_digest,
        plan.source_observation_row_ids,
        plan.rendered_report_id,
    )
    if (
        any(type(value) is not str for value in values[:4])
        or type(plan.source_observation_row_ids) is not tuple
        or type(plan.rendered_report_id) is not str
    ):
        return None
    return (
        *values,
        id(plan.session_date),
        id(plan.generated_at),
        id(plan.economic_at),
        id(plan.retrieved_at),
        id(plan.report),
        id(plan.material),
    )


def _canonical_publication_plan_values_without_callbacks(
    plan: object,
    journal: object,
) -> tuple[object, ...] | None:
    """Return exact persistence values for a still-current issued plan."""
    from .provider_workflows import _is_current_canonical_material_without_callbacks

    seal = _canonical_publication_plan_seal(plan)
    with _ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK:
        authority = _ISSUED_CANONICAL_PUBLICATION_PLANS.get(id(plan))
    if (
        type(plan) is not CanonicalPublicationPlan
        or not isinstance(authority, _CanonicalPublicationPlanAuthority)
        or authority.plan_reference() is not plan
        or authority.plan_seal != seal
        or authority.journal_reference() is not journal
        or getattr(journal, "_closed", True)
        or getattr(journal, "_source_generation", None)
        != authority.journal_generation
    ):
        return None
    result = authority.result_reference()
    material = authority.material_reference()
    publisher = authority.publisher_reference()
    if (
        type(result) is not WorkflowResult
        or material is not plan.material
        or type(publisher) is not CanonicalJournalWorkflowPublisher
        or CanonicalJournalWorkflowPublisher.publish.__code__
        is not authority.publisher_code
        or getattr(publisher, "journal", None) is not journal
        or getattr(publisher, "report_archive_root", None)
        is not authority.archive_root
        or _workflow_result_fingerprint(result) != authority.result_fingerprint
        or result.report is not plan.report
        or not _is_current_canonical_material_without_callbacks(material)
    ):
        return None
    with _ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK:
        if _ISSUED_CANONICAL_PUBLICATION_PLANS.get(id(plan)) is not authority:
            return None
    return (
        plan.workflow_kind,
        plan.storage_kind,
        plan.session_date,
        plan.economic_at,
        plan.retrieved_at,
        plan.material_digest,
        plan.source_digest,
        plan.source_observation_row_ids,
        plan.report.body,
        plan.report.state_hash,
        plan.report,
        result,
        material,
        publisher,
    )


def _canonical_publication_values_are_exact(
    current: object,
    expected: object,
) -> bool:
    """Compare issued plan values without invoking domain-object equality."""
    if (
        type(current) is not tuple
        or type(expected) is not tuple
        or len(current) != 14
        or len(expected) != 14
    ):
        return False
    identity_indices = {2, 3, 4, 10, 11, 12, 13}
    return all(
        (left is right) if index in identity_indices else (left == right)
        for index, (left, right) in enumerate(
            zip(current, expected, strict=True)
        )
    )


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


@dataclass(frozen=True, slots=True)
class CanonicalWorkflowContext:
    """Explicit non-fixture authorities for one provider-backed run."""

    adapter: CanonicalWorkflowAdapter
    publisher: CanonicalWorkflowPublisher
    scheduler: ScheduledWorkflowStore | None
    now: datetime

    def __post_init__(self) -> None:
        require_aware_timestamp(self.now, "canonical workflow time")


def _canonical_noop(outcome: str, reason: str) -> WorkflowResult:
    return WorkflowResult(
        outcome=outcome,
        message=outcome.replace("_", " "),
        exit_code=0,
        reason_codes=(reason,),
        execution_mode="CANONICAL",
    )


def _canonical_data_unavailable(reason: str) -> WorkflowResult:
    return WorkflowResult(
        outcome="DATA_UNAVAILABLE",
        message="DATA UNAVAILABLE - NO CANDIDATE OR ACTION WAS PRODUCED",
        exit_code=3,
        reason_codes=(reason,),
        execution_mode="CANONICAL",
    )


def _run_canonical(
    context: CanonicalWorkflowContext,
    *,
    kind: str,
) -> WorkflowResult:
    """Run one canonical workflow only inside its no-backfill due window."""
    if type(context) is not CanonicalWorkflowContext:
        raise TypeError("canonical workflow requires its exact context")
    if kind not in {"PREMARKET", "CLOSE"}:
        raise ValueError("canonical workflow kind is unsupported")
    now_et = context.now.astimezone(_NEW_YORK)
    try:
        session = context.adapter.market_session(now_et.date())
    except WorkflowDataError as error:
        return _canonical_data_unavailable(
            _safe_reason(error, "DATA_UNAVAILABLE")
        )
    if session is None:
        return _canonical_noop("MARKET_CLOSED_NOOP", "MARKET_CLOSED")
    if type(session) is not SessionWindow or session.session_date != now_et.date():
        raise WorkflowError("canonical market session is unverified")
    wake = session.review_time if kind == "CLOSE" else time(8, 45)
    economic_at = datetime.combine(
        session.session_date,
        wake,
        tzinfo=_NEW_YORK,
    )
    if now_et < economic_at:
        return _canonical_noop("NOT_DUE_NOOP", "NOT_DUE")
    if now_et >= economic_at + timedelta(minutes=15):
        return _canonical_noop("MISSED_RUN_NOOP", "MISSED_RUN")
    try:
        material = (
            context.adapter.close_material(
                session.session_date,
                review_at=economic_at,
                retrieved_at=context.now,
            )
            if kind == "CLOSE"
            else context.adapter.premarket_material(
                session.session_date,
                decision_at=economic_at,
                retrieved_at=context.now,
            )
        )
    except WorkflowDataError as error:
        return _canonical_data_unavailable(
            _safe_reason(error, "DATA_UNAVAILABLE")
        )
    terminal_retrieved_at = getattr(material, "retrieved_at", None)
    try:
        require_aware_timestamp(
            terminal_retrieved_at,
            "canonical terminal retrieval time",
        )
    except (TypeError, ValueError) as error:
        raise WorkflowError(
            "canonical terminal retrieval time is invalid"
        ) from error
    if terminal_retrieved_at < context.now:
        raise WorkflowError(
            "canonical terminal retrieval precedes command start"
        )
    result = context.publisher.issue_result(material=material)
    if type(result) is not WorkflowResult or result.execution_mode != "CANONICAL":
        raise WorkflowError("canonical workflow result is unverified")
    published = context.publisher.publish(
        kind=kind,
        session_date=session.session_date,
        generated_at=terminal_retrieved_at,
        result=result,
        material=material,
    )
    if type(published) is not PublishedWorkflow:
        raise WorkflowError("canonical publication result is unverified")
    if published.status == "IN_PROGRESS":
        return WorkflowResult(
            outcome="PUBLICATION_INCOMPLETE",
            message="PUBLICATION INCOMPLETE - NO REPORT WAS EMITTED",
            exit_code=10,
            reason_codes=("PUBLICATION_INCOMPLETE",),
            execution_mode="CANONICAL",
        )
    return replace(
        result,
        report_id=published.report_id,
        report_row_id=published.report_row_id,
        report_path=published.report_path,
    )


def run_canonical_premarket(
    context: CanonicalWorkflowContext,
) -> WorkflowResult:
    """Run the provider-backed premarket monitor without placing an order."""
    return _run_canonical(context, kind="PREMARKET")


def run_canonical_close(
    context: CanonicalWorkflowContext,
) -> WorkflowResult:
    """Run the provider-backed actual-close monitor without placing an order."""
    return _run_canonical(context, kind="CLOSE")


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


def _canonical_report_reason_codes(report: Report) -> tuple[str, ...]:
    """Read back the exact deterministic reason block from a rendered report."""
    lines = report.body.splitlines()
    try:
        start = lines.index("## Reasons") + 1
        end = next(
            index
            for index in range(start, len(lines))
            if lines[index].startswith("## ")
        )
    except (ValueError, StopIteration) as error:
        raise WorkflowError("canonical report reason block is malformed") from error
    reasons: list[str] = []
    for line in lines[start:end]:
        if not line:
            continue
        match = re.fullmatch(r"- `([A-Z][A-Z0-9_]{0,63})`", line)
        if match is None:
            raise WorkflowError("canonical report reason block is malformed")
        reasons.append(match.group(1))
    if not reasons or len(reasons) != len(set(reasons)):
        raise WorkflowError("canonical report reasons are incomplete")
    return tuple(reasons)


def _canonical_result_projection(
    material: object,
) -> tuple[str, int, tuple[str, ...]]:
    from .provider_workflows import (
        CanonicalCloseCompositionAuthority,
        CanonicalCloseMaterial,
        CanonicalPremarketCompositionAuthority,
        CanonicalPremarketMaterial,
    )

    report = material.report
    reasons = _canonical_report_reason_codes(report)
    if type(material) is CanonicalPremarketMaterial:
        premarket_matrix = {
            ("CANDIDATES", ("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED")): (
                "CANDIDATES",
                0,
            ),
            ("NO TRADE", ("ACTIVE_BREAKER",)): ("NO_TRADE", 4),
            ("NO TRADE", ("MARKET_CLOSED",)): ("NO_TRADE", 0),
            ("NO TRADE", ("NO_CANDIDATES",)): ("NO_TRADE", 0),
        }
        for data_reason in (
            "DATA_UNAVAILABLE",
            "PROVIDER_CHECK_FAILED",
            "SOURCE_CHECK_FAILED",
            "STALE_CALENDAR",
            "STALE_UNIVERSE",
        ):
            premarket_matrix[
                ("NO NEW TRADE - DATA UNAVAILABLE", (data_reason,))
            ] = ("DATA_UNAVAILABLE", 3)
        projection = premarket_matrix.get((report.outcome, reasons))
        authority = material.composition_authority
        if (
            projection is None
            or type(authority) is not CanonicalPremarketCompositionAuthority
            or authority.outcome != report.outcome
            or authority.reason_codes != reasons
        ):
            raise WorkflowError(
                "canonical premarket report outcome/reasons are unsupported"
            )
        has_candidates = bool(material.snapshot.candidates)
        if has_candidates != (projection[0] == "CANDIDATES") or (
            material.snapshot.breaker_active
            != ("ACTIVE_BREAKER" in reasons)
        ):
            raise WorkflowError(
                "canonical premarket report contradicts its normalized snapshot"
            )
        return projection[0], projection[1], reasons
    if type(material) is not CanonicalCloseMaterial:
        raise WorkflowError("canonical result material type is unsupported")
    close_outcomes = {
        "RECONCILIATION REQUIRED": ("RECONCILIATION_REQUIRED", 5),
        "POSITION UNVERIFIED": ("POSITION_UNVERIFIED", 4),
        "STOP UNVERIFIED": ("STOP_UNVERIFIED", 4),
        "DATA UNAVAILABLE": ("DATA_UNAVAILABLE", 3),
        "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE": ("EXIT", 0),
        "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE": (
            "TIGHTEN_STOP",
            0,
        ),
        "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE": ("HOLD", 0),
    }
    projection = close_outcomes.get(report.outcome)
    authority = material.composition_authority
    if (
        projection is None
        or type(authority) is not CanonicalCloseCompositionAuthority
        or authority.outcome != report.outcome
        or authority.reason_codes != reasons
    ):
        raise WorkflowError("canonical close report outcome is unsupported")
    return projection[0], projection[1], reasons


class CanonicalJournalWorkflowPublisher:
    """Prepare owner-bound canonical publication without fixture authority.

    Durable writes remain disabled until the Journal can atomically finalize a
    report and its canonical timing/digest context.  That check happens after
    exact authority preparation but before a claim or archive mutation.
    """

    __slots__ = (
        "journal",
        "report_archive_root",
        "_bound_journal",
        "_bound_archive_root",
        "__weakref__",
    )

    def __init__(self, journal: object, report_archive_root: Path) -> None:
        from .journal import Journal

        if type(journal) is not Journal or getattr(journal, "_closed", True):
            raise TypeError("canonical publisher journal must be an open Journal")
        expected_path_type = type(Path())
        if type(report_archive_root) is not expected_path_type:
            raise TypeError("canonical report archive root must be a pathlib.Path")
        root = Path(os.path.abspath(report_archive_root))
        if (
            root != report_archive_root
            or root.is_symlink()
            or not root.is_dir()
        ):
            raise WorkflowError("canonical report archive root is unverified")
        self.journal = journal
        self.report_archive_root = root
        self._bound_journal = journal
        self._bound_archive_root = root

    def _is_current(self) -> bool:
        return bool(
            type(self) is CanonicalJournalWorkflowPublisher
            and self.journal is self._bound_journal
            and self.report_archive_root is self._bound_archive_root
            and not getattr(self.journal, "_closed", True)
            and self.report_archive_root.is_dir()
            and not self.report_archive_root.is_symlink()
        )

    def _result_material(
        self,
        material: object,
    ) -> tuple[tuple[int, ...], tuple[CandidateSummary, ...]]:
        from .provider_workflows import (
            CanonicalCloseMaterial,
            CanonicalPremarketMaterial,
            is_issued_canonical_material,
        )

        if not self._is_current():
            raise WorkflowError("canonical workflow publisher authority is invalid")
        if type(material) not in {
            CanonicalPremarketMaterial,
            CanonicalCloseMaterial,
        } or not is_issued_canonical_material(
            material,
            journal=self.journal,
            report_archive_root=self.report_archive_root,
        ):
            raise WorkflowError("canonical workflow material authority is invalid")
        row_ids = tuple(receipt.row_id for receipt in material.source_receipts)
        candidates = (
            material.snapshot.candidates
            if type(material) is CanonicalPremarketMaterial
            else ()
        )
        return row_ids, candidates

    def issue_result(
        self,
        *,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
        _result_type: object = WorkflowResult,
    ) -> WorkflowResult:
        """Construct and register a canonical result from exact material."""
        from .provider_workflows import _is_current_canonical_material_without_callbacks

        if WorkflowResult is not _result_type:
            raise WorkflowError("canonical workflow result dependency was replaced")
        row_ids, candidates = self._result_material(material)
        outcome, exit_code, reason_codes = _canonical_result_projection(material)
        try:
            result = _result_type(
                outcome=outcome,
                message=material.report.body,
                exit_code=exit_code,
                reason_codes=reason_codes,
                candidates=candidates,
                report=material.report,
                source_observation_row_ids=row_ids,
                execution_mode="CANONICAL",
            )
        except (TypeError, ValueError) as error:
            raise WorkflowError("canonical workflow result values are invalid") from error
        if type(result) is not WorkflowResult:
            raise WorkflowError("canonical result construction was intercepted")
        fingerprint = _workflow_result_fingerprint(result)
        if fingerprint is None:
            raise WorkflowError("canonical workflow result could not be sealed")
        identity = id(result)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_CANONICAL_RESULTS_LOCK:
                current = _ISSUED_CANONICAL_RESULTS.get(identity)
                if (
                    isinstance(current, _CanonicalResultAuthority)
                    and current.result_reference is dead
                ):
                    _ISSUED_CANONICAL_RESULTS.pop(identity, None)

        authority = _CanonicalResultAuthority(
            result_reference=ref(result, discard),
            result_fingerprint=fingerprint,
            material_reference=ref(material),
            publisher_reference=ref(self),
            journal_reference=ref(self.journal),
            archive_root=self.report_archive_root,
        )
        with _ISSUED_CANONICAL_RESULTS_LOCK:
            _ISSUED_CANONICAL_RESULTS[identity] = authority
        if (
            not _is_current_canonical_material_without_callbacks(material)
            or _workflow_result_fingerprint(result) != fingerprint
        ):
            with _ISSUED_CANONICAL_RESULTS_LOCK:
                if _ISSUED_CANONICAL_RESULTS.get(identity) is authority:
                    _ISSUED_CANONICAL_RESULTS.pop(identity, None)
            raise WorkflowError(
                "canonical workflow authority changed while it was issued"
            )
        return result

    def bind_result(
        self,
        *,
        result: WorkflowResult,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
    ) -> WorkflowResult:
        """Verify a publisher-issued result; never bless a caller-built copy."""
        from .provider_workflows import _is_current_canonical_material_without_callbacks

        if type(result) is not WorkflowResult:
            raise WorkflowError("canonical workflow result authority is invalid")
        expected_row_ids, expected_candidates = self._result_material(material)
        if (
            result.execution_mode != "CANONICAL"
            or result.report is not material.report
            or result.source_observation_row_ids != expected_row_ids
            or any(
                identity is not None
                for identity in (
                    result.report_id,
                    result.report_row_id,
                    result.report_path,
                )
            )
        ):
            raise WorkflowError("canonical workflow result conflicts with material")
        if len(result.candidates) != len(expected_candidates) or any(
            actual is not expected
            for actual, expected in zip(
                result.candidates,
                expected_candidates,
                strict=True,
            )
        ):
            raise WorkflowError("canonical result conflicts with its candidates")
        fingerprint = _workflow_result_fingerprint(result)
        with _ISSUED_CANONICAL_RESULTS_LOCK:
            authority = _ISSUED_CANONICAL_RESULTS.get(id(result))
        if (
            not isinstance(authority, _CanonicalResultAuthority)
            or authority.result_reference() is not result
            or authority.material_reference() is not material
            or authority.publisher_reference() is not self
            or authority.journal_reference() is not self.journal
            or authority.archive_root != self.report_archive_root
            or fingerprint is None
            or fingerprint != authority.result_fingerprint
            or not _is_current_canonical_material_without_callbacks(material)
        ):
            raise WorkflowError("canonical workflow result authority is unverified")
        return result

    def prepare_publication(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
    ) -> CanonicalPublicationPlan:
        """Verify every canonical capability without mutating Journal or archive."""
        from .provider_workflows import (
            CanonicalCloseMaterial,
            CanonicalPremarketMaterial,
            _is_current_canonical_material_without_callbacks,
            is_issued_canonical_material,
        )

        if (
            not self._is_current()
            or type(kind) is not str
            or kind not in {"PREMARKET", "CLOSE"}
            or type(session_date) is not date
            or type(generated_at) is not datetime
            or type(result) is not WorkflowResult
            or type(material)
            not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}
        ):
            raise WorkflowError("canonical publication request is invalid")
        try:
            require_aware_timestamp(generated_at, "canonical publication time")
        except (TypeError, ValueError) as error:
            raise WorkflowError("canonical publication time is invalid") from error
        with _ISSUED_CANONICAL_RESULTS_LOCK:
            authority = _ISSUED_CANONICAL_RESULTS.get(id(result))
        fingerprint = _workflow_result_fingerprint(result)
        if (
            not isinstance(authority, _CanonicalResultAuthority)
            or authority.result_reference() is not result
            or authority.material_reference() is not material
            or authority.publisher_reference() is not self
            or authority.journal_reference() is not self.journal
            or authority.archive_root != self.report_archive_root
            or fingerprint is None
            or fingerprint != authority.result_fingerprint
            or not is_issued_canonical_material(
                material,
                journal=self.journal,
                report_archive_root=self.report_archive_root,
            )
        ):
            raise WorkflowError("canonical publication authority is unverified")

        expected_type = (
            CanonicalPremarketMaterial if kind == "PREMARKET" else CanonicalCloseMaterial
        )
        economic_at = (
            material.decision_at
            if type(material) is CanonicalPremarketMaterial
            else material.review_at
        )
        storage_kind = "MORNING" if kind == "PREMARKET" else "CLOSE"
        if (
            type(material) is not expected_type
            or material.session_date != session_date
            or generated_at != material.retrieved_at
            or result.execution_mode != "CANONICAL"
            or result.report is not material.report
            or material.report.kind != kind
            or material.report.session_date != session_date
        ):
            raise WorkflowError("canonical publication identity conflicts with material")
        plan = CanonicalPublicationPlan(
            workflow_kind=kind,
            storage_kind=storage_kind,
            session_date=session_date,
            generated_at=generated_at,
            economic_at=economic_at,
            retrieved_at=material.retrieved_at,
            material_digest=material.material_digest,
            source_digest=material.source_digest,
            source_observation_row_ids=tuple(
                receipt.row_id for receipt in material.source_receipts
            ),
            rendered_report_id=material.report.report_id,
            report=material.report,
            material=material,
        )
        if (
            not _is_current_canonical_material_without_callbacks(material)
            or _workflow_result_fingerprint(result) != authority.result_fingerprint
        ):
            raise WorkflowError("canonical publication authority changed during seal")
        with _ISSUED_CANONICAL_RESULTS_LOCK:
            if _ISSUED_CANONICAL_RESULTS.get(id(result)) is not authority:
                raise WorkflowError(
                    "canonical publication result authority changed during seal"
                )
        plan_seal = _canonical_publication_plan_seal(plan)
        generation = getattr(self.journal, "_source_generation", None)
        if plan_seal is None or type(generation) is not int or generation < 0:
            raise WorkflowError("canonical publication plan could not be sealed")
        plan_identity = id(plan)

        def discard(dead: ReferenceType[object]) -> None:
            with _ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK:
                current = _ISSUED_CANONICAL_PUBLICATION_PLANS.get(plan_identity)
                if (
                    isinstance(current, _CanonicalPublicationPlanAuthority)
                    and current.plan_reference is dead
                ):
                    _ISSUED_CANONICAL_PUBLICATION_PLANS.pop(plan_identity, None)

        plan_authority = _CanonicalPublicationPlanAuthority(
            plan_reference=ref(plan, discard),
            plan_seal=plan_seal,
            result_reference=ref(result),
            result_fingerprint=authority.result_fingerprint,
            material_reference=ref(material),
            publisher_reference=ref(self),
            journal_reference=ref(self.journal),
            journal_generation=generation,
            archive_root=self.report_archive_root,
            publisher_code=CanonicalJournalWorkflowPublisher.publish.__code__,
        )
        with _ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK:
            _ISSUED_CANONICAL_PUBLICATION_PLANS[plan_identity] = plan_authority
        if _canonical_publication_plan_values_without_callbacks(
            plan, self.journal
        ) is None:
            with _ISSUED_CANONICAL_PUBLICATION_PLANS_LOCK:
                if (
                    _ISSUED_CANONICAL_PUBLICATION_PLANS.get(plan_identity)
                    is plan_authority
                ):
                    _ISSUED_CANONICAL_PUBLICATION_PLANS.pop(plan_identity, None)
            raise WorkflowError(
                "canonical publication plan changed while it was issued"
            )
        return plan

    def publish(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
        material: CanonicalPremarketMaterial | CanonicalCloseMaterial,
        _archive_function: object = archive_report,
        _archive_code: object = archive_report.__code__,
        _archive_globals: object = archive_report.__globals__,
        _archive_global_function: object = archive_report.__globals__.get(
            "archive_report"
        ),
        _report_type: object = Report,
        _report_init_code: object = Report.__init__.__code__,
    ) -> PublishedWorkflow:
        from .journal import (
            JournalError,
            StoredCanonicalReportContext,
            StoredReport,
        )

        initial_plan = self.prepare_publication(
            kind=kind,
            session_date=session_date,
            generated_at=generated_at,
            result=result,
            material=material,
        )
        initial_values = _canonical_publication_plan_values_without_callbacks(
            initial_plan,
            self.journal,
        )
        if initial_values is None:
            raise WorkflowError("canonical publication plan authority is invalid")
        archive_outcome = material.report.outcome

        def require_publication_dependencies() -> None:
            if (
                Report is not _report_type
                or getattr(getattr(_report_type, "__init__", None), "__code__", None)
                is not _report_init_code
            ):
                raise WorkflowError(
                    "canonical report dependency was replaced"
                )
            if (
                archive_report is not _archive_function
                or getattr(_archive_function, "__code__", None)
                is not _archive_code
                or getattr(_archive_function, "__globals__", None)
                is not _archive_globals
                or not isinstance(_archive_globals, dict)
                or _archive_globals.get("archive_report")
                is not _archive_global_function
                or _archive_global_function is not _archive_function
                or _archive_globals.get("Report") is not _report_type
            ):
                raise WorkflowError(
                    "canonical report archive dependency was replaced"
                )

        require_publication_dependencies()

        def stored_expectations(
            expected: tuple[object, ...],
        ) -> tuple[object, ...]:
            report = expected[10]
            if type(report) is not _report_type:
                raise WorkflowError(
                    "canonical report dependency was replaced"
                )
            return (
                *expected[:10],
                report.content_sha256,
                report.observation_ids,
                report.report_id,
            )

        def exact_stored_material(
            stored: object,
            context: object,
            expected: tuple[object, ...],
        ) -> bool:
            if type(stored) is not StoredReport or type(
                context
            ) is not StoredCanonicalReportContext:
                return False
            (
                workflow_kind,
                storage_kind,
                expected_session_date,
                economic_at,
                retrieved_at,
                material_digest,
                source_digest,
                observation_ids,
                body,
                state_sha256,
                content_sha256,
                observation_sha256s,
                _rendered_report_id,
            ) = expected
            return bool(
                getattr(stored, "session_date", None) == expected_session_date
                and getattr(stored, "report_kind", None) == storage_kind
                and getattr(stored, "body", None) == body
                and getattr(stored, "content_sha256", None) == content_sha256
                and getattr(stored, "state_sha256", None) == state_sha256
                and getattr(stored, "observation_ids", None) == observation_ids
                and getattr(stored, "observation_sha256s", None)
                == observation_sha256s
                and getattr(context, "report_row_id", None)
                == getattr(stored, "report_row_id", None)
                and getattr(context, "workflow_kind", None) == workflow_kind
                and getattr(context, "economic_at", None) == economic_at
                and getattr(context, "retrieved_at", None) == retrieved_at
                and getattr(context, "material_digest", None) == material_digest
                and getattr(context, "source_digest", None) == source_digest
            )

        claim = None
        finalized = None
        expected_values = None
        expected_stored_values = None
        try:
            with self.journal.transaction() as transaction:
                # Re-run every callback-bearing owner/source check under the
                # clean write snapshot before the first claim mutation.
                transaction_plan = self.prepare_publication(
                    kind=kind,
                    session_date=session_date,
                    generated_at=generated_at,
                    result=result,
                    material=material,
                )
                expected_values = (
                    _canonical_publication_plan_values_without_callbacks(
                        transaction_plan,
                        self.journal,
                    )
                )
                if expected_values is None:
                    raise WorkflowError(
                        "canonical publication authority changed before claim"
                    )
                require_publication_dependencies()
                expected_stored_values = stored_expectations(expected_values)
                claim = transaction.claim_report(
                    session_date,
                    transaction_plan.storage_kind,
                )
                if claim.status == "IN_PROGRESS":
                    self.journal._clear_sqlite_callbacks_before_authority_commit()
                    require_publication_dependencies()
                    if not _canonical_publication_values_are_exact(
                        _canonical_publication_plan_values_without_callbacks(
                            transaction_plan,
                            self.journal,
                        ),
                        expected_values,
                    ):
                        raise WorkflowError(
                            "canonical publication authority changed during claim"
                        )
                    return PublishedWorkflow(None, None, None, "IN_PROGRESS")
                if claim.status == "ALREADY_FINALIZED":
                    if claim.report_id is None or claim.report_row_id is None:
                        raise WorkflowError(
                            "finalized canonical report identity is unavailable"
                        )
                    try:
                        context = self.journal.read_canonical_report_context(
                            claim.report_id
                        )
                        stored = self.journal.read_report(claim.report_id)
                    except JournalError as error:
                        raise WorkflowError(
                            "finalized report lacks exact canonical context"
                        ) from error
                    if not exact_stored_material(
                        stored, context, expected_stored_values
                    ):
                        raise WorkflowError(
                            "finalized canonical report conflicts with material"
                        )
                else:
                    if claim.status not in {"ACQUIRED", "RECOVERED_EXPIRED"} or (
                        claim.claim_token is None
                    ):
                        raise WorkflowError(
                            "canonical report claim is unavailable"
                        )
                    if not _canonical_publication_values_are_exact(
                        _canonical_publication_plan_values_without_callbacks(
                            transaction_plan,
                            self.journal,
                        ),
                        expected_values,
                    ):
                        raise WorkflowError(
                            "canonical publication authority changed during claim"
                        )
                    require_publication_dependencies()
                    finalized = transaction.finalize_canonical_report(
                        claim_id=claim.claim_id,
                        claim_token=claim.claim_token,
                        publication_plan=transaction_plan,
                    )
                    require_publication_dependencies()
                    if not _canonical_publication_values_are_exact(
                        _canonical_publication_plan_values_without_callbacks(
                            transaction_plan,
                            self.journal,
                        ),
                        expected_values,
                    ):
                        raise WorkflowError(
                            "canonical publication authority changed during finalization"
                        )
                # SQLite may otherwise dispatch raw connection callbacks for
                # COMMIT after the last authority seal.
                self.journal._clear_sqlite_callbacks_before_authority_commit()
                require_publication_dependencies()
                if not _canonical_publication_values_are_exact(
                    _canonical_publication_plan_values_without_callbacks(
                        transaction_plan,
                        self.journal,
                    ),
                    expected_values,
                ):
                    raise WorkflowError(
                        "canonical publication authority changed before commit"
                    )
        except WorkflowError:
            raise
        except JournalError as error:
            raise WorkflowError("canonical Journal publication failed") from error

        if claim is None or expected_stored_values is None:
            raise WorkflowError("canonical publication did not produce a claim")
        require_publication_dependencies()
        durable_report_id = (
            claim.report_id
            if claim.status == "ALREADY_FINALIZED"
            else getattr(getattr(finalized, "report", None), "report_id", None)
        )
        durable_report_row_id = (
            claim.report_row_id
            if claim.status == "ALREADY_FINALIZED"
            else getattr(
                getattr(finalized, "report", None), "report_row_id", None
            )
        )
        if type(durable_report_id) is not str or type(
            durable_report_row_id
        ) is not int:
            raise WorkflowError("canonical report finalization identity is invalid")
        try:
            stored = self.journal.read_report(durable_report_id)
            context = self.journal.read_canonical_report_context(durable_report_id)
        except JournalError as error:
            raise WorkflowError(
                "canonical report readback failed after commit"
            ) from error
        if (
            stored.report_row_id != durable_report_row_id
            or not exact_stored_material(
                stored, context, expected_stored_values
            )
        ):
            raise WorkflowError(
                "canonical report readback conflicts with publication"
            )
        require_publication_dependencies()
        durable_report = _report_type(
            report_id=stored.report_id,
            kind=stored.report_kind,
            session_date=stored.session_date,
            outcome=archive_outcome,
            body=stored.body,
            content_sha256=stored.content_sha256,
            observation_ids=stored.observation_sha256s,
            state_hash=stored.state_sha256,
        )
        require_publication_dependencies()
        if (
            type(durable_report) is not _report_type
            or durable_report.report_id != stored.report_id
            or durable_report.kind != stored.report_kind
            or durable_report.session_date != stored.session_date
            or durable_report.outcome != archive_outcome
            or durable_report.body != stored.body
            or durable_report.content_sha256 != stored.content_sha256
            or durable_report.observation_ids != stored.observation_sha256s
            or durable_report.state_hash != stored.state_sha256
        ):
            raise WorkflowError(
                "canonical durable report reconstruction failed"
            )
        require_publication_dependencies()
        try:
            archived = _archive_function(durable_report, self.report_archive_root)
        except Exception as error:
            raise WorkflowError("canonical report archive failed") from error
        require_publication_dependencies()
        expected_path = self.report_archive_root / stored.archive_relative_path
        try:
            archived_bytes = expected_path.read_bytes()
            root_resolved = self.report_archive_root.resolve(strict=True)
            path_resolved = expected_path.resolve(strict=True)
        except OSError as error:
            raise WorkflowError(
                "canonical report archive verification failed"
            ) from error
        current = self.report_archive_root
        archive_has_symlink = self.report_archive_root.is_symlink()
        for part in stored.archive_relative_path.split("/"):
            current = current / part
            archive_has_symlink = archive_has_symlink or current.is_symlink()
        if (
            getattr(archived, "report_id", None) != stored.report_id
            or getattr(archived, "path", None) != expected_path
            or getattr(archived, "sha256", None) != stored.content_sha256
            or archive_has_symlink
            or not expected_path.is_file()
            or path_resolved != root_resolved / stored.archive_relative_path
            or archived_bytes != stored.body.encode("utf-8")
            or hashlib.sha256(archived_bytes).hexdigest()
            != stored.content_sha256
        ):
            raise WorkflowError("canonical report archive verification failed")
        return PublishedWorkflow(
            stored.report_id,
            stored.report_row_id,
            str(expected_path),
            (
                "ALREADY_EMITTED"
                if claim.status == "ALREADY_FINALIZED"
                else "PUBLISHED"
            ),
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
        try:
            return PremarketSnapshot(self.candidates, self.breaker == "ACTIVE")
        except (TypeError, ValueError) as error:
            raise WorkflowError(
                "workflow-issued fixture premarket snapshot is invalid"
            ) from error

    def close_snapshot(self, day: date) -> CloseSnapshot:
        del day
        try:
            return CloseSnapshot(self.close_state, self.close_positions)
        except (TypeError, ValueError) as error:
            raise WorkflowError(
                "workflow-issued fixture close snapshot is invalid"
            ) from error


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
    "CanonicalJournalWorkflowPublisher",
    "CanonicalPublicationPlan",
    "CanonicalWorkflowAdapter",
    "CanonicalWorkflowContext",
    "CanonicalWorkflowPublisher",
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
    "run_canonical_close",
    "run_canonical_premarket",
    "run_close",
    "run_premarket",
]
