"""Pure deterministic Markdown rendering and immutable report archiving."""

from __future__ import annotations

import hashlib
import inspect
import os
import tempfile
import threading
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qsl, urlsplit
from weakref import ReferenceType, ref

from .domain import require_aware_timestamp, require_positive_decimal, require_positive_int
_PREMARKET_OUTCOMES = frozenset(
    {
        "CANDIDATES",
        "NO TRADE",
        "NO NEW TRADE - DATA UNAVAILABLE",
    }
)
_SCORE_COMPONENTS = (
    "Trend and market regime",
    "Relative strength",
    "Setup quality",
    "Volume confirmation",
    "Verified catalyst/context",
    "Liquidity and execution",
)
_VALIDATION_KINDS = {
    "PHASE 1": "VALIDATION_PHASE1",
    "DIAGNOSTIC REPLAY": "VALIDATION_REPLAY",
    "STRICT REPLAY": "VALIDATION_REPLAY",
    "PHASE 2 PAPER": "VALIDATION_PHASE2_PAPER",
}
_ARCHIVE_LOCK = threading.Lock()
_ISSUED_REPORTS_LOCK = threading.Lock()
_ISSUED_REPORTS: dict[
    int,
    tuple[ReferenceType[Report], tuple[object, ...]],
] = {}
_SENSITIVE_QUERY_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "key_id",
        "password",
        "secret",
        "signature",
        "token",
    }
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


def stable_report_id(
    kind: str,
    session_date: date,
    observation_ids: tuple[str, ...],
    state_hash: str,
) -> str:
    """Compatibility facade without a module-load dependency on Journal."""
    from .journal import stable_report_id as journal_stable_report_id

    return journal_stable_report_id(
        kind,
        session_date,
        observation_ids,
        state_hash,
    )
_CLOSE_OUTCOMES = (
    "RECONCILIATION REQUIRED",
    "POSITION UNVERIFIED",
    "STOP UNVERIFIED",
    "DATA UNAVAILABLE",
    "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
    "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
    "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
)
_CLOSE_ACTIONS = frozenset({"HOLD", "EXIT", "TIGHTEN_STOP"})
_UNVERIFIED_CLOSE_STATUSES = frozenset(
    {
        "POSITION_UNVERIFIED",
        "STOP_UNVERIFIED",
        "DATA_UNAVAILABLE",
        "RECONCILIATION_REQUIRED",
    }
)


@dataclass(frozen=True, slots=True)
class ReportSource:
    """One display-safe primary evidence link."""

    label: str
    url: str

    def __post_init__(self) -> None:
        _single_line(self.label, "source label")
        _safe_https_url(self.url)


@dataclass(frozen=True, slots=True)
class ScoreComponent:
    """One explicit component of the locked 100-point candidate score."""

    label: str
    earned: int
    available: int

    def __post_init__(self) -> None:
        _single_line(self.label, "score component label")
        if type(self.earned) is not int or type(self.available) is not int:
            raise TypeError("score values must be integers")
        if self.available <= 0 or not 0 <= self.earned <= self.available:
            raise ValueError("score values must be within the available points")


@dataclass(frozen=True, slots=True)
class PremarketCandidate:
    """Report-only projection of one already-qualified candidate."""

    symbol: str
    role: Literal["PRIMARY"]
    setup: str
    score_components: tuple[ScoreComponent, ...]
    trigger: Decimal
    maximum_entry: Decimal
    recommended_stop: Decimal
    target: Decimal
    shares: int
    planned_risk: Decimal
    provider: str
    feed: str
    observed_at: datetime
    invalidations: tuple[str, ...]
    sources: tuple[ReportSource, ...]

    def __post_init__(self) -> None:
        _canonical_token(self.symbol, "candidate symbol")
        _canonical_token(self.role, "candidate role")
        if self.role != "PRIMARY":
            raise ValueError("sized candidate role must be PRIMARY")
        _canonical_token(self.setup, "candidate setup")
        if tuple(component.label for component in self.score_components) != (
            _SCORE_COMPONENTS
        ):
            raise ValueError("candidate must contain the six score components in order")
        if sum(component.available for component in self.score_components) != 100:
            raise ValueError("candidate score components must total 100 available points")
        for name, value in (
            ("trigger", self.trigger),
            ("maximum entry", self.maximum_entry),
            ("recommended stop", self.recommended_stop),
            ("target", self.target),
            ("planned risk", self.planned_risk),
        ):
            require_positive_decimal(value, name)
        require_positive_int(self.shares, "candidate shares")
        if not self.recommended_stop < self.maximum_entry < self.target:
            raise ValueError("candidate stop, entry, and target ordering is invalid")
        _canonical_token(self.provider, "candidate provider")
        _canonical_token(self.feed, "candidate feed")
        require_aware_timestamp(self.observed_at, "candidate observation time")
        if not self.invalidations:
            raise ValueError("candidate must name at least one invalidation")
        for invalidation in self.invalidations:
            _single_line(invalidation, "candidate invalidation")
        if not self.sources:
            raise ValueError("candidate must include at least one source")

    @property
    def score(self) -> int:
        return sum(component.earned for component in self.score_components)


@dataclass(frozen=True, slots=True)
class PremarketShadow:
    """Watchlist-only candidate projection with no sizing or plan authority."""

    symbol: str
    role: Literal["WATCHLIST_SHADOW"]
    score: Decimal
    setup: str
    trigger: Decimal

    def __post_init__(self) -> None:
        _canonical_token(self.symbol, "shadow symbol")
        _canonical_token(self.role, "shadow role")
        if self.role != "WATCHLIST_SHADOW":
            raise ValueError("shadow candidate role must be WATCHLIST_SHADOW")
        score = _finite_decimal(self.score, "shadow score")
        if not Decimal("0") <= score <= Decimal("100"):
            raise ValueError("shadow score must be between zero and 100")
        _canonical_token(self.setup, "shadow setup")
        require_positive_decimal(self.trigger, "shadow trigger")


@dataclass(frozen=True, slots=True)
class PremarketState:
    """Audited inputs required to render one premarket report."""

    session_date: date
    generated_at: datetime
    outcome: str
    reason_codes: tuple[str, ...]
    observation_ids: tuple[str, ...]
    state_hash: str
    candidates: tuple[PremarketCandidate | PremarketShadow, ...] = ()

    def __post_init__(self) -> None:
        _session_date(self.session_date)
        require_aware_timestamp(self.generated_at, "report generation time")
        if self.outcome not in _PREMARKET_OUTCOMES:
            raise ValueError("unsupported premarket outcome")
        _reason_codes(self.reason_codes)
        _observation_ids(self.observation_ids)
        _sha256(self.state_hash, "state hash")
        if type(self.candidates) is not tuple or any(
            type(candidate) not in {PremarketCandidate, PremarketShadow}
            for candidate in self.candidates
        ):
            raise TypeError("candidates must contain exact report projection types")
        if self.outcome == "CANDIDATES" and not self.candidates:
            raise ValueError("candidate outcome requires at least one candidate")
        if self.outcome == "CANDIDATES":
            primary_count = sum(
                type(candidate) is PremarketCandidate for candidate in self.candidates
            )
            shadow_count = sum(
                type(candidate) is PremarketShadow for candidate in self.candidates
            )
            if primary_count != 1 or shadow_count > 2:
                raise ValueError(
                    "candidate outcome requires one primary and at most two shadows"
                )
            symbols = tuple(candidate.symbol for candidate in self.candidates)
            if len(symbols) != len(set(symbols)):
                raise ValueError("candidate symbols must be unique")
            if type(self.candidates[0]) is not PremarketCandidate:
                raise ValueError("primary candidate must be first")
        if self.outcome != "CANDIDATES" and self.candidates:
            raise ValueError("non-candidate outcome cannot contain candidates")


@dataclass(frozen=True, slots=True)
class ReportMetric:
    """One preformatted, non-secret validation metric."""

    label: str
    value: str

    def __post_init__(self) -> None:
        _single_line(self.label, "metric label")
        _single_line(self.value, "metric value")


@dataclass(frozen=True, slots=True)
class ValidationState:
    """Audited inputs for Phase 1, replay, or Phase 2 paper reporting."""

    session_date: date
    generated_at: datetime
    mode: str
    outcome: str
    metrics: tuple[ReportMetric, ...]
    reason_codes: tuple[str, ...]
    observation_ids: tuple[str, ...]
    state_hash: str

    def __post_init__(self) -> None:
        _session_date(self.session_date)
        require_aware_timestamp(self.generated_at, "report generation time")
        if self.mode not in _VALIDATION_KINDS:
            raise ValueError("unsupported validation report mode")
        _canonical_token(self.outcome, "validation outcome")
        if not self.metrics:
            raise ValueError("validation report requires at least one metric")
        _reason_codes(self.reason_codes)
        _observation_ids(self.observation_ids)
        _sha256(self.state_hash, "state hash")


@dataclass(frozen=True, slots=True)
class ClosePosition:
    """Report-only close snapshot for one tracked actual position."""

    symbol: str
    shares: int
    mark: Decimal
    estimated_unrealized_pl: Decimal
    r_multiple: Decimal
    recommended_stop: Decimal
    user_confirmed_stop: Decimal | None
    target: Decimal
    holding_days: int
    provider: str
    feed: str
    observed_at: datetime
    upcoming_events: tuple[str, ...]
    evidence: tuple[ReportSource, ...]
    action: Literal["HOLD", "EXIT", "TIGHTEN_STOP"] = "HOLD"
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _canonical_token(self.symbol, "position symbol")
        require_positive_int(self.shares, "position shares")
        require_positive_decimal(self.mark, "position mark")
        _finite_decimal(self.estimated_unrealized_pl, "estimated unrealized P/L")
        _finite_decimal(self.r_multiple, "position R multiple")
        require_positive_decimal(self.recommended_stop, "recommended stop")
        if self.user_confirmed_stop is not None:
            require_positive_decimal(self.user_confirmed_stop, "user-confirmed stop")
        require_positive_decimal(self.target, "position target")
        if type(self.holding_days) is not int or self.holding_days < 0:
            raise ValueError("holding days must be a non-negative integer")
        _canonical_token(self.provider, "position provider")
        _canonical_token(self.feed, "position feed")
        require_aware_timestamp(self.observed_at, "position observation time")
        for event in self.upcoming_events:
            _single_line(event, "upcoming event")
        if type(self.action) is not str or self.action not in _CLOSE_ACTIONS:
            raise ValueError("verified position action is unsupported")
        _projection_reason_codes(self.reason_codes, required=False)


@dataclass(frozen=True, slots=True)
class UnverifiedClosePosition:
    """Known actual exposure without invented market or plan values."""

    symbol: str
    shares: int
    exact_cost_basis: Decimal
    status: Literal[
        "POSITION_UNVERIFIED",
        "STOP_UNVERIFIED",
        "DATA_UNAVAILABLE",
        "RECONCILIATION_REQUIRED",
    ]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _canonical_token(self.symbol, "unverified position symbol")
        require_positive_int(self.shares, "unverified position shares")
        require_positive_decimal(
            self.exact_cost_basis,
            "unverified position exact cost basis",
        )
        if (
            type(self.status) is not str
            or self.status not in _UNVERIFIED_CLOSE_STATUSES
        ):
            raise ValueError("unverified position status is unsupported")
        _projection_reason_codes(self.reason_codes, required=True)


@dataclass(frozen=True, slots=True)
class CloseState:
    """Audited close snapshot with explicit precedence inputs."""

    session_date: date
    generated_at: datetime
    reason_codes: tuple[str, ...]
    positions: tuple[ClosePosition | UnverifiedClosePosition, ...]
    observation_ids: tuple[str, ...]
    state_hash: str
    reconciliation_required: bool = False
    position_verified: bool = True
    stop_verified: bool = True
    data_available: bool = True
    exit_due: bool = False
    tighten_stop_due: bool = False

    def __post_init__(self) -> None:
        _session_date(self.session_date)
        require_aware_timestamp(self.generated_at, "report generation time")
        _reason_codes(self.reason_codes)
        _observation_ids(self.observation_ids)
        _sha256(self.state_hash, "state hash")
        for name in (
            "reconciliation_required",
            "position_verified",
            "stop_verified",
            "data_available",
            "exit_due",
            "tighten_stop_due",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")
        if type(self.positions) is not tuple or any(
            type(position) not in {ClosePosition, UnverifiedClosePosition}
            for position in self.positions
        ):
            raise TypeError("positions must contain exact report projection types")
        if self.stop_verified and any(
            position.user_confirmed_stop is None for position in self.positions
            if type(position) is ClosePosition
        ):
            raise ValueError("verified stop state requires every stop confirmation")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Report:
    """Fully rendered immutable report and its stable audit identities."""

    report_id: str
    kind: str
    session_date: date
    outcome: str
    body: str
    content_sha256: str
    observation_ids: tuple[str, ...]
    state_hash: str

    @property
    def archive_relative_path(self) -> str:
        from .journal import report_archive_relative_path

        return report_archive_relative_path(self.kind, self.session_date, self.report_id)

    @property
    def sha256(self) -> str:
        return self.content_sha256


class ReportArchiveError(OSError):
    """An immutable report archive could not be inspected or published."""


class ReportArchiveConflict(ReportArchiveError):
    """A stable report identity disagrees with content already on disk."""


@dataclass(frozen=True, slots=True)
class ArchivedReport:
    """Verified archive location and content identity."""

    report_id: str
    path: Path
    sha256: str
    duplicate: bool


def is_issued_report(report: object) -> bool:
    """Return whether *report* is the unchanged exact output of a renderer."""
    if type(report) is not Report:
        return False
    with _ISSUED_REPORTS_LOCK:
        current = _ISSUED_REPORTS.get(id(report))
        return bool(
            current is not None
            and current[0]() is report
            and current[1] == _report_fingerprint(report)
        )


def _issued_report_snapshot(report: object) -> Report | None:
    """Copy verified renderer bytes so later caller mutation cannot cross the boundary."""
    if type(report) is not Report:
        return None
    with _ISSUED_REPORTS_LOCK:
        current = _ISSUED_REPORTS.get(id(report))
        if (
            current is None
            or current[0]() is not report
            or current[1] != _report_fingerprint(report)
        ):
            return None
        snapshot = object.__new__(Report)
        for name, value in zip(
            (
                "report_id",
                "kind",
                "session_date",
                "outcome",
                "body",
                "content_sha256",
                "observation_ids",
                "state_hash",
            ),
            current[1],
            strict=True,
        ):
            object.__setattr__(snapshot, name, value)
        return snapshot


def render_premarket_report(state: PremarketState) -> Report:
    """Render one deterministic plan-only premarket report."""
    if not isinstance(state, PremarketState):
        raise TypeError("state must be a PremarketState")
    lines = [
        f"# Stock Monitor Premarket Report - {state.session_date.isoformat()}",
        "",
        f"- Outcome: `{state.outcome}`",
        f"- Generated at: `{state.generated_at.isoformat(timespec='seconds')}`",
        "",
        "> PLAN ONLY. This report never places an order. Enter only after "
        "09:35 ET, after the trigger occurs, and after verifying the current "
        "Robinhood price and spread.",
        "",
        "## Reasons",
        "",
        *[f"- `{reason}`" for reason in state.reason_codes],
        "",
        "## Candidates",
        "",
    ]
    if not state.candidates:
        lines.append("None.")
    for ordinal, candidate in enumerate(state.candidates, start=1):
        if ordinal > 1:
            lines.append("")
        lines.extend(_render_candidate(candidate, ordinal))
    return _report(
        kind="PREMARKET",
        session_date=state.session_date,
        outcome=state.outcome,
        lines=lines,
        observation_ids=state.observation_ids,
        state_hash=state.state_hash,
    )


def render_close_report(state: CloseState) -> Report:
    """Render the dominant close outcome using the locked fail-closed order."""
    if not isinstance(state, CloseState):
        raise TypeError("state must be a CloseState")
    outcome = _close_outcome(state)
    lines = [
        f"# Stock Monitor Close Report - {state.session_date.isoformat()}",
        "",
        f"- Outcome: `{outcome}`",
        f"- Generated at: `{state.generated_at.isoformat(timespec='seconds')}`",
        "",
        "> Every action is provisional pending current Robinhood verification. "
        "This report cannot change or replace the protective stop entered in "
        "Robinhood.",
        "",
        "## Reasons",
        "",
        *[f"- `{reason}`" for reason in state.reason_codes],
        "",
        "## Positions",
        "",
    ]
    if not state.positions:
        lines.append("None.")
    for ordinal, position in enumerate(state.positions, start=1):
        if ordinal > 1:
            lines.append("")
        lines.extend(_render_close_position(position, ordinal))
    return _report(
        kind="CLOSE",
        session_date=state.session_date,
        outcome=outcome,
        lines=lines,
        observation_ids=state.observation_ids,
        state_hash=state.state_hash,
    )


def render_validation_report(state: ValidationState) -> Report:
    """Render deterministic Phase 1, replay, or Phase 2 paper status."""
    if not isinstance(state, ValidationState):
        raise TypeError("state must be a ValidationState")
    warning = (
        "PROCESS VALIDATION ONLY. Results do not promise future returns or "
        "authorize brokerage execution."
    )
    if state.mode == "DIAGNOSTIC REPLAY":
        warning += (
            " Diagnostic replay has current-list survivorship bias and is not "
            "proof of performance."
        )
    if state.mode == "PHASE 2 PAPER":
        warning += (
            " Options remain paper-only until every external promotion gate and "
            "separate user approval is satisfied."
        )
    lines = [
        f"# Stock Monitor {state.mode} Validation Report - "
        f"{state.session_date.isoformat()}",
        "",
        f"- Outcome: `{state.outcome}`",
        f"- Generated at: `{state.generated_at.isoformat(timespec='seconds')}`",
        "",
        f"> {warning}",
        "",
        "## Metrics",
        "",
        *[f"- {metric.label}: `{metric.value}`" for metric in state.metrics],
        "",
        "## Reasons",
        "",
        *[f"- `{reason}`" for reason in state.reason_codes],
    ]
    return _report(
        kind=_VALIDATION_KINDS[state.mode],
        session_date=state.session_date,
        outcome=state.outcome,
        lines=lines,
        observation_ids=state.observation_ids,
        state_hash=state.state_hash,
    )


def archive_report(report: Report, root: Path) -> ArchivedReport:
    """Atomically publish *report*, rejecting every identity/content conflict."""
    from .journal import (
        InvalidJournalValue,
        report_archive_relative_path,
        stable_report_id,
    )

    if not isinstance(report, Report):
        raise TypeError("report must be a Report")
    if not isinstance(root, Path):
        raise TypeError("archive root must be a pathlib.Path")
    body_bytes = report.body.encode("utf-8")
    actual_sha256 = hashlib.sha256(body_bytes).hexdigest()
    if actual_sha256 != report.content_sha256:
        raise ReportArchiveConflict("report content hash does not match its body")
    try:
        expected_report_id = stable_report_id(
            report.kind,
            report.session_date,
            report.observation_ids,
            report.state_hash,
        )
    except (InvalidJournalValue, TypeError) as error:
        raise ReportArchiveConflict("report audit inputs are invalid") from error
    if report.report_id != expected_report_id:
        raise ReportArchiveConflict("report ID does not match its audit inputs")

    target = root / report_archive_relative_path(
        report.kind,
        report.session_date,
        report.report_id,
    )
    _reject_symbolic_link_ancestors(root, target.parent)
    try:
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as error:
        raise ReportArchiveError("report archive directory could not be created") from error
    _reject_symbolic_link_ancestors(root, target.parent)

    with _ARCHIVE_LOCK:
        duplicate = _verify_existing_archive(target, body_bytes, report.report_id)
        if duplicate:
            return ArchivedReport(
                report_id=report.report_id,
                path=target,
                sha256=actual_sha256,
                duplicate=True,
            )
        temporary_path: Path | None = None
        try:
            file_descriptor, raw_path = tempfile.mkstemp(
                prefix=f".{target.name}.",
                suffix=".tmp",
                dir=target.parent,
            )
            temporary_path = Path(raw_path)
            with os.fdopen(file_descriptor, "wb") as stream:
                stream.write(body_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary_path, target)
            except FileExistsError:
                duplicate = _verify_existing_archive(
                    target,
                    body_bytes,
                    report.report_id,
                )
                if not duplicate:
                    raise ReportArchiveConflict(
                        "report archive target disappeared during publication"
                    )
            temporary_path.unlink()
            temporary_path = None
            _sync_directory(target.parent)
        except ReportArchiveConflict:
            raise
        except OSError as error:
            raise ReportArchiveError("report archive could not be published") from error
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass

    return ArchivedReport(
        report_id=report.report_id,
        path=target,
        sha256=actual_sha256,
        duplicate=duplicate,
    )


def _render_candidate(
    candidate: PremarketCandidate | PremarketShadow,
    ordinal: int,
) -> list[str]:
    if type(candidate) is PremarketCandidate:
        return _render_primary_candidate(candidate, ordinal)
    if type(candidate) is PremarketShadow:
        return _render_shadow_candidate(candidate, ordinal)
    raise TypeError("candidate must be an exact report projection")


def _render_primary_candidate(
    candidate: PremarketCandidate,
    ordinal: int,
) -> list[str]:
    lines = [
        f"### {ordinal}. {candidate.symbol} - {candidate.role}",
        "",
        f"- Setup: `{candidate.setup}`",
        f"- Total score: `{candidate.score}/100`",
    ]
    lines.extend(
        f"- {component.label}: `{component.earned}/{component.available}`"
        for component in candidate.score_components
    )
    lines.extend(
        [
            f"- Entry trigger: `{_money(candidate.trigger)}`",
            f"- Maximum permitted entry: `{_money(candidate.maximum_entry)}`",
            f"- Recommended initial stop: `{_money(candidate.recommended_stop)}`",
            f"- First target: `{_money(candidate.target)}`",
            f"- Whole shares: `{candidate.shares}`",
            f"- Planned risk: `{_money(candidate.planned_risk)}`",
            f"- Data: provider `{candidate.provider}`, feed `{candidate.feed}`, "
            f"observed `{candidate.observed_at.isoformat(timespec='seconds')}`",
            f"- Invalidations: {'; '.join(candidate.invalidations)}",
            "- Sources:",
        ]
    )
    lines.extend(f"  - [{source.label}]({source.url})" for source in candidate.sources)
    return lines


def _render_shadow_candidate(
    candidate: PremarketShadow,
    ordinal: int,
) -> list[str]:
    unavailable = "N/A - WATCHLIST ONLY"
    return [
        f"### {ordinal}. {candidate.symbol} - {candidate.role}",
        "",
        f"- Setup: `{candidate.setup}`",
        f"- Total score: `{_plain_decimal(candidate.score)}/100`",
        f"- Entry trigger: `{_money(candidate.trigger)}`",
        f"- Maximum permitted entry: `{unavailable}`",
        f"- Recommended initial stop: `{unavailable}`",
        f"- First target: `{unavailable}`",
        f"- Whole shares: `{unavailable}`",
        f"- Planned risk: `{unavailable}`",
    ]


def _render_close_position(
    position: ClosePosition | UnverifiedClosePosition,
    ordinal: int,
) -> list[str]:
    if type(position) is ClosePosition:
        return _render_verified_close_position(position, ordinal)
    if type(position) is UnverifiedClosePosition:
        return _render_unverified_close_position(position, ordinal)
    raise TypeError("position must be an exact report projection")


def _render_verified_close_position(
    position: ClosePosition,
    ordinal: int,
) -> list[str]:
    confirmed_stop = (
        "NOT CONFIRMED"
        if position.user_confirmed_stop is None
        else _money(position.user_confirmed_stop)
    )
    upcoming_events = (
        "; ".join(position.upcoming_events) if position.upcoming_events else "None known"
    )
    lines = [
        f"### {ordinal}. {position.symbol}",
        "",
        f"- Shares: `{position.shares}`",
        f"- Estimated mark: `{_money(position.mark)}`",
        f"- Estimated unrealized P/L: `{_signed_money(position.estimated_unrealized_pl)}`",
        f"- R multiple: `{_decimal(position.r_multiple)}R`",
        f"- Recommended stop: `{_money(position.recommended_stop)}`",
        f"- User-confirmed stop: `{confirmed_stop}`",
        f"- First target: `{_money(position.target)}`",
        f"- Holding age: `{position.holding_days} trading days`",
        f"- Upcoming events: {upcoming_events}",
    ]
    if position.action != "HOLD" or position.reason_codes:
        lines.extend(
            [
                f"- Action: `{position.action}`",
                "- Position reasons:",
                *(
                    [
                        f"  - `{_human_token(reason)}`"
                        for reason in position.reason_codes
                    ]
                    if position.reason_codes
                    else ["  - None."]
                ),
            ]
        )
    lines.extend(
        [
            f"- Data: provider `{position.provider}`, feed `{position.feed}`, "
            f"observed `{position.observed_at.isoformat(timespec='seconds')}`",
            "- Evidence:",
        ]
    )
    if position.evidence:
        lines.extend(
            f"  - [{source.label}]({source.url})" for source in position.evidence
        )
    else:
        lines.append("  - None.")
    return lines


def _render_unverified_close_position(
    position: UnverifiedClosePosition,
    ordinal: int,
) -> list[str]:
    return [
        f"### {ordinal}. {position.symbol}",
        "",
        f"- Shares: `{position.shares}`",
        f"- Exact cost basis: `{_money(position.exact_cost_basis)}`",
        f"- Status: `{_human_token(position.status)}`",
        "- Position reasons:",
        *[f"  - `{_human_token(reason)}`" for reason in position.reason_codes],
    ]


def _close_outcome(state: CloseState) -> str:
    unverified_statuses = {
        position.status
        for position in state.positions
        if type(position) is UnverifiedClosePosition
    }
    verified_actions = {
        position.action
        for position in state.positions
        if type(position) is ClosePosition
    }
    if (
        state.reconciliation_required
        or "RECONCILIATION_REQUIRED" in unverified_statuses
    ):
        return _CLOSE_OUTCOMES[0]
    if (
        not state.position_verified
        or "POSITION_UNVERIFIED" in unverified_statuses
    ):
        return _CLOSE_OUTCOMES[1]
    if not state.stop_verified or "STOP_UNVERIFIED" in unverified_statuses:
        return _CLOSE_OUTCOMES[2]
    if not state.data_available or "DATA_UNAVAILABLE" in unverified_statuses:
        return _CLOSE_OUTCOMES[3]
    if state.exit_due or "EXIT" in verified_actions:
        return _CLOSE_OUTCOMES[4]
    if state.tighten_stop_due or "TIGHTEN_STOP" in verified_actions:
        return _CLOSE_OUTCOMES[5]
    return _CLOSE_OUTCOMES[6]


def _verify_existing_archive(
    target: Path,
    body_bytes: bytes,
    report_id: str,
) -> bool:
    if target.is_symlink():
        raise ReportArchiveConflict("report archive target cannot be a symbolic link")
    try:
        existing = target.read_bytes()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ReportArchiveError("report archive could not be inspected") from error
    if existing != body_bytes:
        raise ReportArchiveConflict(
            f"report ID conflicts with archived content: {report_id}"
        )
    return True


def _reject_symbolic_link_ancestors(root: Path, directory: Path) -> None:
    normalized_root = Path(os.path.abspath(root))
    normalized_directory = Path(os.path.abspath(directory))
    try:
        relative = normalized_directory.relative_to(normalized_root)
    except ValueError as error:
        raise ReportArchiveConflict("report archive path escapes its root") from error
    current = normalized_root
    for component in ("", *relative.parts):
        if component:
            current /= component
        if current.is_symlink():
            raise ReportArchiveConflict(
                "report archive path cannot traverse a symbolic link"
            )


def _sync_directory(directory: Path) -> None:
    descriptor: int | None = None
    try:
        descriptor = os.open(directory, os.O_RDONLY)
        os.fsync(descriptor)
    except OSError as error:
        raise ReportArchiveError("report archive directory could not be synchronized") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _report(
    *,
    kind: str,
    session_date: date,
    outcome: str,
    lines: list[str],
    observation_ids: tuple[str, ...],
    state_hash: str,
) -> Report:
    from .journal import stable_report_id

    frame = inspect.currentframe()
    try:
        caller = None if frame is None else frame.f_back
        renderer_codes = {
            render_premarket_report.__code__,
            render_close_report.__code__,
            render_validation_report.__code__,
        }
        if caller is None or caller.f_code not in renderer_codes:
            raise RuntimeError("reports can only be issued by a public renderer")
    finally:
        del frame
    body = "\n".join(lines) + "\n"
    report = Report(
        report_id=stable_report_id(kind, session_date, observation_ids, state_hash),
        kind=kind,
        session_date=session_date,
        outcome=outcome,
        body=body,
        content_sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
        observation_ids=observation_ids,
        state_hash=state_hash,
    )
    _register_issued_report(report)
    return report


def _report_fingerprint(report: Report) -> tuple[object, ...] | None:
    if (
        type(report) is not Report
        or type(report.report_id) is not str
        or type(report.kind) is not str
        or type(report.session_date) is not date
        or type(report.outcome) is not str
        or type(report.body) is not str
        or type(report.content_sha256) is not str
        or type(report.observation_ids) is not tuple
        or any(type(value) is not str for value in report.observation_ids)
        or type(report.state_hash) is not str
    ):
        return None
    return (
        report.report_id,
        report.kind,
        report.session_date,
        report.outcome,
        report.body,
        report.content_sha256,
        report.observation_ids,
        report.state_hash,
    )


def _register_issued_report(report: Report) -> None:
    frame = inspect.currentframe()
    try:
        caller = None if frame is None else frame.f_back
        if caller is None or caller.f_code is not _report.__code__:
            raise RuntimeError("report authority can only be issued by a renderer")
    finally:
        del frame
    identity = id(report)

    def discard(dead: ReferenceType[Report]) -> None:
        with _ISSUED_REPORTS_LOCK:
            current = _ISSUED_REPORTS.get(identity)
            if current is not None and current[0] is dead:
                _ISSUED_REPORTS.pop(identity, None)

    with _ISSUED_REPORTS_LOCK:
        fingerprint = _report_fingerprint(report)
        if fingerprint is None:
            raise RuntimeError("renderer produced invalid report authority")
        _ISSUED_REPORTS[identity] = (
            ref(report, discard),
            fingerprint,
        )


def _money(value: Decimal) -> str:
    return f"${_decimal(value)}"


def _signed_money(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}${_decimal(value)}" if value >= 0 else f"-${_decimal(-value)}"


def _decimal(value: Decimal) -> str:
    text = format(value, "f")
    whole, separator, fraction = text.partition(".")
    fraction = fraction.rstrip("0")
    if len(fraction) < 2:
        fraction += "0" * (2 - len(fraction))
    return f"{whole}.{fraction}" if separator or fraction else f"{whole}.00"


def _plain_decimal(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _human_token(value: str) -> str:
    return value.replace("_", " ")


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    return value


def _single_line(value: object, name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty text without surrounding whitespace")
    if "\n" in value or "\r" in value or "\x00" in value:
        raise ValueError(f"{name} must be one line")
    return value


def _canonical_token(value: object, name: str) -> str:
    value = _single_line(value, name)
    if value != value.upper():
        raise ValueError(f"{name} must be uppercase")
    return value


def _session_date(value: object) -> date:
    if type(value) is not date:
        raise TypeError("session date must be a datetime.date")
    return value


def _sha256(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return value


def _reason_codes(values: tuple[str, ...]) -> None:
    if not values:
        raise ValueError("report requires at least one reason code")
    for value in values:
        _canonical_token(value, "reason code")


def _projection_reason_codes(
    values: tuple[str, ...],
    *,
    required: bool,
) -> None:
    if type(values) is not tuple:
        raise TypeError("position reason codes must be a tuple")
    if required and not values:
        raise ValueError("unverified position requires at least one reason code")
    if len(values) != len(set(values)):
        raise ValueError("position reason codes must not contain duplicates")
    for value in values:
        _canonical_token(value, "position reason code")


def _observation_ids(values: tuple[str, ...]) -> None:
    if not values:
        raise ValueError("report requires at least one observation identity")
    for value in values:
        _single_line(value, "observation identity")


def _safe_https_url(value: object) -> str:
    value = _single_line(value, "source URL")
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
    ):
        raise ValueError("source URL must be credential-free HTTPS")
    try:
        query_pairs = parse_qsl(
            parts.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError:
        raise ValueError("source URL query is malformed") from None
    for name, _ in query_pairs:
        folded = name.casefold()
        if folded == "page_token":
            continue
        compact = "".join(character for character in folded if character.isalnum())
        if folded in _SENSITIVE_QUERY_NAMES or any(
            compact.endswith(suffix) for suffix in _SENSITIVE_QUERY_SUFFIXES
        ):
            raise ValueError("source URL cannot contain credentials")
    return value
