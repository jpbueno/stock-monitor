"""Pure Phase 1 prospective-promotion gates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import Enum
from hashlib import sha256
import json
from pathlib import Path
import re
from threading import RLock
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo

from .domain import (
    DomainValidationError,
    MAX_MICRODOLLARS,
    money_from_micros,
    money_to_micros,
)
from .phase1 import EquityPoint, Phase1Error, SignalStatus, max_drawdown


_ZERO = Decimal("0")
_MINIMUM_TRADES = 20
_MINIMUM_DAYS = 28
_MINIMUM_ADHERENCE = Decimal("0.90")
_MAXIMUM_DRAWDOWN = Decimal("250")
_STARTING_CAPITAL = Decimal("5000")
_ET = ZoneInfo("America/New_York")
_PHASE1_ADHERENCE_CHECK_NAMES = (
    "DATA_CALENDAR_UNIVERSE_FRESHNESS",
    "HARD_ELIGIBILITY_GATES",
    "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
    "VALID_TRIGGER_TIMING",
    "ENTRY_AND_SPREAD_COMPLIANCE",
    "POSITION_SIZE_EXPOSURE_AND_RISK",
    "STOP_STATE",
    "EXIT_RULE",
    "CIRCUIT_BREAKER_BEHAVIOR",
    "RECORD_COMPLETENESS",
)
_FINAL_DISPOSITIONS = frozenset(
    {
        SignalStatus.NOT_TRIGGERED,
        SignalStatus.NOT_FILLED_LIMIT,
        SignalStatus.UNRESOLVED,
        SignalStatus.EXPIRED,
        SignalStatus.INVALIDATED,
        SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        SignalStatus.CLOSED,
    }
)
_TRADE_FLAGS_BY_DISPOSITION = {
    SignalStatus.PUBLISHED: frozenset({(False, False, False)}),
    SignalStatus.TRIGGERED_AWAITING_LIMIT: frozenset({(True, False, False)}),
    SignalStatus.TRIGGERED_PAPER: frozenset({(True, True, False)}),
    SignalStatus.LIVE_CONFIRMED: frozenset({(True, True, False)}),
    SignalStatus.SKIPPED_LIVE_TRACKED_PAPER: frozenset({(True, True, False)}),
    SignalStatus.SHADOW_FILLED_INFORMATIONAL: frozenset(
        {(True, True, False)}
    ),
    SignalStatus.NOT_TRIGGERED: frozenset({(False, False, False)}),
    SignalStatus.NOT_FILLED_LIMIT: frozenset({(True, False, False)}),
    SignalStatus.UNRESOLVED: frozenset(
        {(False, False, False), (True, False, False)}
    ),
    SignalStatus.EXPIRED: frozenset(
        {(False, False, False), (True, False, False)}
    ),
    SignalStatus.INVALIDATED: frozenset(
        {(False, False, False), (True, False, False)}
    ),
    SignalStatus.CLOSED: frozenset({(True, True, True)}),
}
_TERMINAL_EVIDENCE_KIND = {
    SignalStatus.NOT_TRIGGERED: "SESSION_COMPLETION",
    SignalStatus.NOT_FILLED_LIMIT: "SESSION_COMPLETION",
    SignalStatus.UNRESOLVED: "SESSION_COMPLETION",
    SignalStatus.EXPIRED: "EXPIRY_DEADLINE",
    SignalStatus.INVALIDATED: "SIGNAL_EVIDENCE",
    SignalStatus.SHADOW_FILLED_INFORMATIONAL: "SHADOW_FILL",
    SignalStatus.CLOSED: "CLOSED_TRADE",
}
_NON_COMPLETION_TERMINALS = frozenset(
    {SignalStatus.EXPIRED, SignalStatus.INVALIDATED}
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ValidationError(ValueError):
    """A prospective-validation input is structurally invalid."""

    def __init__(self, code: str) -> None:
        self.code = code
        self.reason_code = code
        super().__init__(code)


def _phase1_adherence_applicability(
    *,
    role: str,
    triggered: bool,
    filled: bool,
    closed: bool,
) -> tuple[tuple[str, bool], ...]:
    """Return the complete fixed checklist with deterministic applicability."""
    if (
        role not in {"PRIMARY", "WATCHLIST_SHADOW"}
        or any(type(value) is not bool for value in (triggered, filled, closed))
        or (filled and not triggered)
        or (closed and not filled)
    ):
        raise ValidationError("INVALID_ADHERENCE_APPLICABILITY")
    if role == "WATCHLIST_SHADOW":
        return tuple((name, False) for name in _PHASE1_ADHERENCE_CHECK_NAMES)
    always = {
        "DATA_CALENDAR_UNIVERSE_FRESHNESS",
        "HARD_ELIGIBILITY_GATES",
        "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
        "CIRCUIT_BREAKER_BEHAVIOR",
        "RECORD_COMPLETENESS",
    }
    applicable = set(always)
    if triggered:
        applicable.add("VALID_TRIGGER_TIMING")
    if filled:
        applicable.update(
            {
                "ENTRY_AND_SPREAD_COMPLIANCE",
                "POSITION_SIZE_EXPOSURE_AND_RISK",
                "STOP_STATE",
            }
        )
    if closed:
        applicable.add("EXIT_RULE")
    return tuple(
        (name, name in applicable) for name in _PHASE1_ADHERENCE_CHECK_NAMES
    )


def _finite_decimal(value: object, code: str) -> Decimal:
    if (
        type(value) is not Decimal
        or not value.is_finite()
        or value.copy_abs() > Decimal(MAX_MICRODOLLARS)
    ):
        raise ValidationError(code)
    if value.is_zero():
        return Decimal("0")
    return value


def _nonnegative_count(value: object, code: str) -> int:
    if (
        type(value) is not int
        or value < 0
        or value > MAX_MICRODOLLARS
    ):
        raise ValidationError(code)
    return value


@dataclass(frozen=True, slots=True)
class Phase1Trade:
    signal_id: str
    role: str
    triggered: bool
    filled: bool
    closed: bool
    record_complete: bool
    net_r: Decimal | None
    fill_price: Decimal | None = None
    filled_at: datetime | None = None
    source_id: str | None = None
    source_digest: str | None = None

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise ValidationError("INVALID_PHASE1_TRADE")
        if self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}:
            raise ValidationError("INVALID_PHASE1_TRADE_ROLE")
        for value in (
            self.triggered,
            self.filled,
            self.closed,
            self.record_complete,
        ):
            if type(value) is not bool:
                raise ValidationError("INVALID_PHASE1_TRADE_FLAG")
        if self.closed and not self.filled:
            raise ValidationError("CLOSED_TRADE_WAS_NOT_FILLED")
        if self.filled and not self.triggered:
            raise ValidationError("FILLED_TRADE_WAS_NOT_TRIGGERED")
        if self.closed != (self.net_r is not None):
            raise ValidationError("INVALID_PHASE1_NET_R")
        if self.net_r is not None:
            object.__setattr__(
                self,
                "net_r",
                _finite_decimal(self.net_r, "INVALID_PHASE1_NET_R"),
            )
        informational_shadow = (
            self.role == "WATCHLIST_SHADOW"
            and self.triggered
            and self.filled
            and not self.closed
        )
        informational_fields = (
            self.fill_price,
            self.filled_at,
            self.source_id,
            self.source_digest,
        )
        if informational_shadow:
            if (
                type(self.fill_price) is not Decimal
                or not self.fill_price.is_finite()
                or self.fill_price <= _ZERO
                or not isinstance(self.filled_at, datetime)
                or self.filled_at.tzinfo is None
                or self.filled_at.utcoffset() is None
                or type(self.source_id) is not str
                or not self.source_id
                or not self.source_id.isprintable()
                or type(self.source_digest) is not str
                or _SHA256.fullmatch(self.source_digest) is None
            ):
                raise ValidationError("INCOMPLETE_SHADOW_FILL_FACT")
            try:
                canonical_fill = money_from_micros(
                    money_to_micros(self.fill_price)
                )
            except DomainValidationError:
                raise ValidationError("INCOMPLETE_SHADOW_FILL_FACT") from None
            object.__setattr__(self, "fill_price", canonical_fill)
        elif any(value is not None for value in informational_fields):
            raise ValidationError("UNEXPECTED_SHADOW_FILL_FACT")


@dataclass(frozen=True, slots=True)
class PublishedSignalDisposition:
    signal_id: str
    role: str
    status: SignalStatus
    timestamps_complete: bool
    source_evidence_complete: bool
    session_complete: bool
    terminal_evidence_kind: str | None = None
    terminal_evidence_digest: str | None = None
    terminal_source_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise ValidationError("INVALID_PUBLISHED_SIGNAL")
        if self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}:
            raise ValidationError("INVALID_PUBLISHED_SIGNAL_ROLE")
        if not isinstance(self.status, SignalStatus):
            raise ValidationError("INVALID_PUBLISHED_SIGNAL_STATUS")
        for value in (
            self.timestamps_complete,
            self.source_evidence_complete,
            self.session_complete,
        ):
            if type(value) is not bool:
                raise ValidationError("INVALID_PUBLISHED_SIGNAL_COMPLETENESS")
        terminal_fields = (
            self.terminal_evidence_kind,
            self.terminal_evidence_digest,
            self.terminal_source_id,
        )
        expected_kind = _TERMINAL_EVIDENCE_KIND.get(self.status)
        if expected_kind is None:
            if any(value is not None for value in terminal_fields):
                raise ValidationError("INVALID_SIGNAL_TERMINAL_EVIDENCE")
        elif (
            self.terminal_evidence_kind != expected_kind
            or type(self.terminal_evidence_digest) is not str
            or _SHA256.fullmatch(self.terminal_evidence_digest) is None
            or type(self.terminal_source_id) is not str
            or not self.terminal_source_id
            or not self.terminal_source_id.isprintable()
            or (
                self.status in _NON_COMPLETION_TERMINALS
                and self.session_complete
            )
        ):
            raise ValidationError("INVALID_SIGNAL_TERMINAL_EVIDENCE")

    @property
    def complete(self) -> bool:
        expected_kind = _TERMINAL_EVIDENCE_KIND.get(self.status)
        expected_session_state = (
            not self.session_complete
            if self.status in _NON_COMPLETION_TERMINALS
            else self.session_complete
        )
        return (
            self.status in _FINAL_DISPOSITIONS
            and self.timestamps_complete
            and self.source_evidence_complete
            and expected_kind is not None
            and self.terminal_evidence_kind == expected_kind
            and expected_session_state
        )


@dataclass(frozen=True, slots=True)
class AdherenceSummary:
    passed_applicable_checks: int
    total_applicable_checks: int

    def __post_init__(self) -> None:
        passed = _nonnegative_count(
            self.passed_applicable_checks,
            "INVALID_ADHERENCE_COUNTS",
        )
        total = _nonnegative_count(
            self.total_applicable_checks,
            "INVALID_ADHERENCE_COUNTS",
        )
        if total == 0 or passed > total:
            raise ValidationError("INVALID_ADHERENCE_COUNTS")

    @property
    def ratio(self) -> Decimal:
        with localcontext() as context:
            context.prec = max(
                50,
                len(str(self.passed_applicable_checks))
                + len(str(self.total_applicable_checks))
                + 20,
            )
            return Decimal(self.passed_applicable_checks) / Decimal(
                self.total_applicable_checks
            )


@dataclass(frozen=True, slots=True)
class Phase1AdherenceCheckDecision:
    """One fixed-checklist result derived from exact Journal evidence."""

    check_name: str
    applicable: bool
    passed: bool
    hard_breach: bool
    failure_codes: tuple[str, ...]
    hard_breach_codes: tuple[str, ...]
    evidence_digest: str

    def __post_init__(self) -> None:
        if self.check_name not in _PHASE1_ADHERENCE_CHECK_NAMES:
            raise ValidationError("INVALID_PHASE1_ADHERENCE_CHECK")
        if any(
            type(value) is not bool
            for value in (self.applicable, self.passed, self.hard_breach)
        ):
            raise ValidationError("INVALID_PHASE1_ADHERENCE_CHECK")
        for values in (self.failure_codes, self.hard_breach_codes):
            if (
                type(values) is not tuple
                or any(type(code) is not str or not code for code in values)
                or len(set(values)) != len(values)
            ):
                raise ValidationError("INVALID_PHASE1_ADHERENCE_CHECK")
        if (
            type(self.evidence_digest) is not str
            or _SHA256.fullmatch(self.evidence_digest) is None
        ):
            raise ValidationError("INVALID_PHASE1_ADHERENCE_CHECK")
        expected_hard_breach = bool(self.hard_breach_codes)
        expected_passed = self.applicable and not (
            self.failure_codes or self.hard_breach_codes
        )
        if (
            self.hard_breach != expected_hard_breach
            or self.passed != expected_passed
            or (
                not self.applicable
                and (self.failure_codes or self.hard_breach_codes)
            )
        ):
            raise ValidationError("INVALID_PHASE1_ADHERENCE_CHECK")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1AdherenceAuthority:
    """Identity-bound complete adherence checklist for one terminal signal."""

    validation_window_id: str
    signal_id: str
    role: str
    checks: tuple[Phase1AdherenceCheckDecision, ...]
    evaluated_at: datetime
    query_cutoff: datetime
    source_digest: str
    authority_digest: str
    _phase1_source: object | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if (
            type(self.validation_window_id) is not str
            or not self.validation_window_id
            or type(self.signal_id) is not str
            or not self.signal_id
            or self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}
            or type(self.checks) is not tuple
            or any(
                not isinstance(check, Phase1AdherenceCheckDecision)
                for check in self.checks
            )
            or tuple(check.check_name for check in self.checks)
            != _PHASE1_ADHERENCE_CHECK_NAMES
            or not isinstance(self.evaluated_at, datetime)
            or self.evaluated_at.tzinfo is None
            or self.evaluated_at.utcoffset() is None
            or not isinstance(self.query_cutoff, datetime)
            or self.query_cutoff.tzinfo is None
            or self.query_cutoff.utcoffset() is None
            or self.evaluated_at > self.query_cutoff
            or type(self.source_digest) is not str
            or _SHA256.fullmatch(self.source_digest) is None
            or type(self.authority_digest) is not str
            or _SHA256.fullmatch(self.authority_digest) is None
        ):
            raise ValidationError("INVALID_PHASE1_ADHERENCE_AUTHORITY")


_ISSUED_PHASE1_ADHERENCE_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[Phase1AdherenceAuthority],
        tuple[object, ...],
        ReferenceType[object],
    ],
] = {}
_ISSUED_PHASE1_ADHERENCE_AUTHORITIES_LOCK = RLock()


def _phase1_adherence_authority_fingerprint(
    authority: Phase1AdherenceAuthority,
) -> tuple[object, ...]:
    return (
        authority.validation_window_id,
        authority.signal_id,
        authority.role,
        authority.checks,
        authority.evaluated_at,
        authority.query_cutoff,
        authority.source_digest,
        authority.authority_digest,
    )


def is_issued_phase1_adherence_authority(authority: object) -> bool:
    """Return whether *authority* is exact and its Journal source is current."""
    if not isinstance(authority, Phase1AdherenceAuthority):
        return False
    source = authority._phase1_source
    if source is None:
        return False
    try:
        from .journal import is_verified_phase1_adherence_review_source

        source_current = is_verified_phase1_adherence_review_source(source)
        fingerprint = _phase1_adherence_authority_fingerprint(authority)
    except (AttributeError, ImportError, TypeError, ValueError):
        return False
    if not source_current:
        return False
    with _ISSUED_PHASE1_ADHERENCE_AUTHORITIES_LOCK:
        issued = _ISSUED_PHASE1_ADHERENCE_AUTHORITIES.get(id(authority))
        return (
            issued is not None
            and issued[0]() is authority
            and issued[1] == fingerprint
            and issued[2]() is source
        )


def _phase1_terminal_trade_flags(
    disposition_source: object,
) -> tuple[bool, bool, bool]:
    lifecycle_events = tuple(
        getattr(disposition_source, "lifecycle_events", ())
    )
    kinds = tuple(getattr(event, "event_kind", None) for event in lifecycle_events)
    status_value = getattr(disposition_source, "status", None)
    status = (
        status_value.value
        if isinstance(status_value, SignalStatus)
        else status_value
    )
    triggered = any(
        kind
        in {
            "TRIGGER_OBSERVED",
            "PAPER_FILL",
            "LIVE_CONFIRM",
            "LIVE_SKIP",
            "SHADOW_FILL",
            "PARTIAL_EXIT",
            "CLOSE",
        }
        for kind in kinds
    )
    filled = any(
        kind
        in {
            "PAPER_FILL",
            "LIVE_CONFIRM",
            "LIVE_SKIP",
            "SHADOW_FILL",
            "PARTIAL_EXIT",
            "CLOSE",
        }
        for kind in kinds
    )
    closed = status == SignalStatus.CLOSED.value
    return triggered, filled, closed


def _issue_phase1_adherence_from_journal_source(
    source: object,
    *,
    calendar_resolver: object,
    policy: object,
) -> Phase1AdherenceAuthority:
    """Derive all ten checks from one exact terminal Journal review source."""
    try:
        from .journal import (
            Phase1AdherenceReviewSource,
            is_verified_phase1_adherence_review_source,
            phase1_sources_share_owner,
        )
    except ImportError:
        raise ValidationError(
            "PHASE1_ADHERENCE_REVIEW_SOURCE_UNVERIFIED"
        ) from None
    if not isinstance(source, Phase1AdherenceReviewSource) or not (
        is_verified_phase1_adherence_review_source(source)
    ):
        raise ValidationError("PHASE1_ADHERENCE_REVIEW_SOURCE_UNVERIFIED")

    from .policy import Policy
    from .risk import SessionCalendarResolver, _calendar_digest, _policy_digest

    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise ValidationError("PHASE1_ADHERENCE_CALENDAR_UNVERIFIED")
    if not isinstance(policy, Policy):
        raise ValidationError("PHASE1_ADHERENCE_POLICY_UNVERIFIED")
    try:
        policy.validate()
    except Exception:
        raise ValidationError("PHASE1_ADHERENCE_POLICY_UNVERIFIED") from None
    if (
        source.calendar_digest != _calendar_digest(calendar_resolver)
        or source.policy_digest != _policy_digest(policy)
    ):
        raise ValidationError("PHASE1_ADHERENCE_CONFIGURATION_MISMATCH")

    signal_source = source.signal_source
    disposition_source = source.disposition_source
    nested_sources = tuple(
        value
        for value in (
            signal_source,
            disposition_source,
            source.canonical_replay_source,
            source.breaker_history_source,
            *tuple(source.actual_action_sources),
        )
        if value is not None
    )
    if any(
        not phase1_sources_share_owner(source, nested)
        for nested in nested_sources
    ):
        raise ValidationError("PHASE1_ADHERENCE_SOURCE_OWNER_MISMATCH")
    if (
        getattr(disposition_source, "signal_source", None) is not signal_source
    ):
        raise ValidationError("PHASE1_ADHERENCE_DISPOSITION_MISMATCH")

    triggered, filled, closed = _phase1_terminal_trade_flags(
        disposition_source
    )
    applicability = _phase1_adherence_applicability(
        role=signal_source.role,
        triggered=triggered,
        filled=filled,
        closed=closed,
    )
    evidence_sources = tuple(source.check_evidence_sources)
    if (
        len(evidence_sources) != len(_PHASE1_ADHERENCE_CHECK_NAMES)
        or tuple(item.check_name for item in evidence_sources)
        != _PHASE1_ADHERENCE_CHECK_NAMES
        or any(
            not phase1_sources_share_owner(source, item)
            for item in evidence_sources
        )
    ):
        raise ValidationError("PHASE1_ADHERENCE_CHECKLIST_INCOMPLETE")
    checks = tuple(
        Phase1AdherenceCheckDecision(
            check_name=name,
            applicable=applicable,
            passed=applicable
            and not (tuple(evidence.failure_codes) or tuple(evidence.hard_breach_codes)),
            hard_breach=bool(tuple(evidence.hard_breach_codes)),
            failure_codes=tuple(evidence.failure_codes),
            hard_breach_codes=tuple(evidence.hard_breach_codes),
            evidence_digest=evidence.source_digest,
        )
        for (name, applicable), evidence in zip(
            applicability,
            evidence_sources,
            strict=True,
        )
    )
    evaluated_at = source.query_cutoff
    payload = {
        "namespace": "stock-monitor/phase1-adherence-authority/v1",
        "validation_window_id": signal_source.validation_window_id,
        "signal_id": signal_source.signal_id,
        "role": signal_source.role,
        "evaluated_at": evaluated_at.isoformat(),
        "query_cutoff": source.query_cutoff.isoformat(),
        "source_digest": source.source_digest,
        "checks": [
            {
                "check_name": check.check_name,
                "applicable": check.applicable,
                "passed": check.passed,
                "hard_breach": check.hard_breach,
                "failure_codes": list(check.failure_codes),
                "hard_breach_codes": list(check.hard_breach_codes),
                "evidence_digest": check.evidence_digest,
            }
            for check in checks
        ],
    }
    authority = Phase1AdherenceAuthority(
        validation_window_id=signal_source.validation_window_id,
        signal_id=signal_source.signal_id,
        role=signal_source.role,
        checks=checks,
        evaluated_at=evaluated_at,
        query_cutoff=source.query_cutoff,
        source_digest=source.source_digest,
        authority_digest=sha256(
            json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
        _phase1_source=source,
    )
    fingerprint = _phase1_adherence_authority_fingerprint(authority)
    with _ISSUED_PHASE1_ADHERENCE_AUTHORITIES_LOCK:
        _ISSUED_PHASE1_ADHERENCE_AUTHORITIES[id(authority)] = (
            ref(authority),
            fingerprint,
            ref(source),
        )
    return authority


@dataclass(frozen=True, slots=True)
class Phase1Window:
    started_on: date
    as_of: date
    expected_open_sessions: tuple[date, ...]
    trades: tuple[Phase1Trade, ...]
    published_signals: tuple[PublishedSignalDisposition, ...]
    canonical_equity: tuple[EquityPoint, ...]
    actual_equity: tuple[EquityPoint, ...]
    adherence: AdherenceSummary
    hard_breach_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.started_on) is not date or type(self.as_of) is not date:
            raise ValidationError("INVALID_PHASE1_WINDOW_DATE")
        if self.as_of < self.started_on:
            raise ValidationError("INVALID_PHASE1_WINDOW_DATE")
        if (
            type(self.expected_open_sessions) is not tuple
            or not self.expected_open_sessions
            or any(type(session) is not date for session in self.expected_open_sessions)
            or self.expected_open_sessions[0] != self.started_on
            or any(
                later <= earlier
                for earlier, later in zip(
                    self.expected_open_sessions,
                    self.expected_open_sessions[1:],
                    strict=False,
                )
            )
            or any(
                session < self.started_on or session > self.as_of
                for session in self.expected_open_sessions
            )
        ):
            raise ValidationError("INVALID_EXPECTED_OPEN_SESSIONS")
        sequence_contracts = (
            (self.trades, Phase1Trade, "INVALID_PHASE1_TRADES"),
            (
                self.published_signals,
                PublishedSignalDisposition,
                "INVALID_PUBLISHED_SIGNALS",
            ),
            (self.canonical_equity, EquityPoint, "INVALID_CANONICAL_EQUITY"),
            (self.actual_equity, EquityPoint, "INVALID_ACTUAL_EQUITY"),
        )
        for values, expected_type, code in sequence_contracts:
            if type(values) is not tuple or any(
                not isinstance(value, expected_type) for value in values
            ):
                raise ValidationError(code)
        if not isinstance(self.adherence, AdherenceSummary):
            raise ValidationError("INVALID_ADHERENCE_SUMMARY")
        if type(self.hard_breach_codes) is not tuple or any(
            type(code) is not str or not code for code in self.hard_breach_codes
        ):
            raise ValidationError("INVALID_HARD_BREACH_CODES")
        if len(set(self.hard_breach_codes)) != len(self.hard_breach_codes):
            raise ValidationError("DUPLICATE_HARD_BREACH_CODE")
        signal_by_id = {
            signal.signal_id: signal for signal in self.published_signals
        }
        if len(signal_by_id) != len(self.published_signals):
            raise ValidationError("DUPLICATE_PUBLISHED_SIGNAL")
        trade_ids = {trade.signal_id for trade in self.trades}
        if len(trade_ids) != len(self.trades):
            raise ValidationError("DUPLICATE_PHASE1_TRADE")
        for trade in self.trades:
            disposition = signal_by_id.get(trade.signal_id)
            if disposition is None:
                raise ValidationError("TRADE_WITHOUT_PUBLISHED_SIGNAL")
            if disposition.role != trade.role:
                raise ValidationError("TRADE_SIGNAL_ROLE_MISMATCH")
            trade_flags = (trade.triggered, trade.filled, trade.closed)
            if trade_flags not in _TRADE_FLAGS_BY_DISPOSITION[
                disposition.status
            ]:
                raise ValidationError("TRADE_SIGNAL_STATE_MISMATCH")
        for disposition in self.published_signals:
            if disposition.status is not SignalStatus.SHADOW_FILLED_INFORMATIONAL:
                continue
            matches = tuple(
                trade
                for trade in self.trades
                if trade.signal_id == disposition.signal_id
            )
            if (
                len(matches) != 1
                or matches[0].role != "WATCHLIST_SHADOW"
                or not matches[0].triggered
                or not matches[0].filled
                or matches[0].closed
                or not matches[0].record_complete
                or matches[0].net_r is not None
            ):
                raise ValidationError("SHADOW_FILL_TRADE_RECORD_INCOMPLETE")
        if not self.canonical_equity or not self.actual_equity:
            raise ValidationError("MISSING_PHASE1_EQUITY_CURVE")
        if any(
            point.ledger_name != "CANONICAL" for point in self.canonical_equity
        ):
            raise ValidationError("INVALID_CANONICAL_EQUITY")
        if any(point.ledger_name != "ACTUAL" for point in self.actual_equity):
            raise ValidationError("INVALID_ACTUAL_EQUITY")
        for curve in (self.canonical_equity, self.actual_equity):
            if any(point.at is None for point in curve):
                raise ValidationError("MISSING_PHASE1_EQUITY_TIME")
            point_dates = tuple(
                point.at.astimezone(_ET).date()  # type: ignore[union-attr]
                for point in curve
            )
            if any(
                point_date < self.started_on or point_date > self.as_of
                for point_date in point_dates
            ):
                raise ValidationError("EQUITY_POINT_OUTSIDE_PHASE1_WINDOW")
            if point_dates != self.expected_open_sessions:
                raise ValidationError(
                    "PHASE1_EQUITY_SESSION_COVERAGE_INCOMPLETE"
                )
            first = curve[0]
            if (
                point_dates[0] != self.started_on
                or first.cash != _STARTING_CAPITAL
                or first.positions_value != _ZERO
                or first.equity != _STARTING_CAPITAL
                or first.external_cash_flow != _ZERO
            ):
                raise ValidationError("PHASE1_EQUITY_MUST_START_AT_5000")
        try:
            max_drawdown(self.canonical_equity)
            max_drawdown(self.actual_equity)
        except Phase1Error as error:
            raise ValidationError(error.code) from None

    @property
    def elapsed_days(self) -> int:
        return (self.as_of - self.started_on).days


class PromotionStatus(str, Enum):
    PASSED = "PASSED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PromotionDecision:
    passed: bool
    status: PromotionStatus
    reason_codes: tuple[str, ...]
    closed_primary_trades: int
    elapsed_days: int
    mean_net_r: Decimal | None
    adherence: Decimal
    canonical_max_drawdown: Decimal
    actual_max_drawdown: Decimal
    source_digest: str | None = None
    authority_digest: str | None = None
    _phase1_source: object | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if type(self.passed) is not bool or not isinstance(
            self.status, PromotionStatus
        ):
            raise ValidationError("INVALID_PROMOTION_DECISION")
        if self.passed != (self.status is PromotionStatus.PASSED):
            raise ValidationError("INVALID_PROMOTION_DECISION")
        if type(self.reason_codes) is not tuple or any(
            type(code) is not str or not code for code in self.reason_codes
        ):
            raise ValidationError("INVALID_PROMOTION_REASONS")
        if self.passed == bool(self.reason_codes):
            raise ValidationError("INVALID_PROMOTION_DECISION")
        _nonnegative_count(
            self.closed_primary_trades,
            "INVALID_PROMOTION_TRADE_COUNT",
        )
        if self.passed and self.closed_primary_trades < _MINIMUM_TRADES:
            raise ValidationError("INVALID_PROMOTION_DECISION")
        _nonnegative_count(self.elapsed_days, "INVALID_PROMOTION_ELAPSED_DAYS")
        if self.passed and self.elapsed_days < _MINIMUM_DAYS:
            raise ValidationError("INVALID_PROMOTION_DECISION")
        if self.mean_net_r is not None:
            object.__setattr__(
                self,
                "mean_net_r",
                _finite_decimal(self.mean_net_r, "INVALID_PROMOTION_MEAN_R"),
            )
        if self.passed and (
            self.mean_net_r is None or self.mean_net_r <= _ZERO
        ):
            raise ValidationError("INVALID_PROMOTION_DECISION")
        adherence = _finite_decimal(
            self.adherence,
            "INVALID_PROMOTION_ADHERENCE",
        )
        if not _ZERO <= adherence <= Decimal("1"):
            raise ValidationError("INVALID_PROMOTION_ADHERENCE")
        object.__setattr__(self, "adherence", adherence)
        if self.passed and adherence < _MINIMUM_ADHERENCE:
            raise ValidationError("INVALID_PROMOTION_DECISION")
        for attribute in ("canonical_max_drawdown", "actual_max_drawdown"):
            value = getattr(self, attribute)
            if (
                type(value) is not Decimal
                or not value.is_finite()
                or value < _ZERO
            ):
                raise ValidationError("INVALID_PROMOTION_DRAWDOWN")
            try:
                canonical = money_from_micros(money_to_micros(value))
            except DomainValidationError:
                raise ValidationError("INVALID_PROMOTION_DRAWDOWN") from None
            object.__setattr__(self, attribute, canonical)
            if self.passed and canonical > _MAXIMUM_DRAWDOWN:
                raise ValidationError("INVALID_PROMOTION_DECISION")
        authority_digests = (self.source_digest, self.authority_digest)
        if any(value is not None for value in authority_digests) and not all(
            type(value) is str and _SHA256.fullmatch(value) is not None
            for value in authority_digests
        ):
            raise ValidationError("INVALID_PROMOTION_DECISION")


_ISSUED_PROMOTION_DECISIONS: dict[
    int,
    tuple[
        ReferenceType[PromotionDecision],
        tuple[object, ...],
        ReferenceType[object],
    ],
] = {}
_ISSUED_PROMOTION_DECISIONS_LOCK = RLock()


def _promotion_decision_fingerprint(
    decision: PromotionDecision,
) -> tuple[object, ...]:
    return (
        decision.passed,
        decision.status,
        decision.reason_codes,
        decision.closed_primary_trades,
        decision.elapsed_days,
        decision.mean_net_r,
        decision.adherence,
        decision.canonical_max_drawdown,
        decision.actual_max_drawdown,
        decision.source_digest,
        decision.authority_digest,
    )


def is_issued_promotion_decision(decision: object) -> bool:
    """Return true only for an exact, current Journal-source decision identity."""
    if not isinstance(decision, PromotionDecision):
        return False
    source = decision._phase1_source
    if source is None:
        return False
    try:
        fingerprint = _promotion_decision_fingerprint(decision)
        from .journal import is_verified_phase1_validation_window_source

        source_current = is_verified_phase1_validation_window_source(source)
    except Exception:
        return False
    if not source_current:
        return False
    with _ISSUED_PROMOTION_DECISIONS_LOCK:
        issued = _ISSUED_PROMOTION_DECISIONS.get(id(decision))
        return (
            issued is not None
            and issued[0]() is decision
            and issued[1] == fingerprint
            and issued[2]() is source
        )


def _phase1_validation_disposition_material(
    source: object,
) -> tuple[PublishedSignalDisposition, Phase1Trade]:
    signal = source.signal_source
    events = tuple(source.lifecycle_events)
    try:
        status = SignalStatus(source.status)
    except (TypeError, ValueError):
        raise ValidationError("PHASE1_VALIDATION_DISPOSITION_INCOMPLETE") from None
    if (
        status not in _FINAL_DISPOSITIONS
        or not events
        or source.expected_lifecycle_count != len(events)
        or tuple(event.event_ordinal for event in events)
        != tuple(range(len(events)))
        or tuple(event.row_id for event in events)
        != tuple(sorted(event.row_id for event in events))
        or source.lifecycle_terminal_cursor != events[-1].row_id
        or source.lifecycle_source_highwater != events[-1].row_id
        or events[-1].to_status != status.value
        or any(
            event.signal_id != signal.signal_id
            or not (
                event.event_time
                <= event.message_time
                <= event.received_at
                <= source.query_cutoff
            )
            for event in events
        )
    ):
        raise ValidationError("PHASE1_VALIDATION_DISPOSITION_INCOMPLETE")

    proof_fields = {
        "session_completion": source.session_completion,
        "expiry_deadline_source": source.expiry_deadline_source,
        "signal_evidence_source": source.signal_evidence_source,
        "shadow_fill_source": source.shadow_fill_source,
        "closed_trade": source.closed_trade,
    }
    expected_proof = {
        SignalStatus.NOT_TRIGGERED: "session_completion",
        SignalStatus.NOT_FILLED_LIMIT: "session_completion",
        SignalStatus.UNRESOLVED: "session_completion",
        SignalStatus.EXPIRED: "expiry_deadline_source",
        SignalStatus.INVALIDATED: "signal_evidence_source",
        SignalStatus.SHADOW_FILLED_INFORMATIONAL: "shadow_fill_source",
        SignalStatus.CLOSED: "closed_trade",
    }[status]
    if (
        tuple(name for name, value in proof_fields.items() if value is not None)
        != (expected_proof,)
    ):
        raise ValidationError("PHASE1_VALIDATION_TERMINAL_PROOF_MISMATCH")
    proof = proof_fields[expected_proof]
    assert proof is not None
    terminal_event = events[-1]
    if expected_proof == "session_completion":
        terminal_kind = "SESSION_COMPLETION"
        terminal_source_id = proof.completion_id
        terminal_digest = proof.source_digest
        proof_matches = (
            terminal_event.session_completion_id == proof.completion_id
            and proof.signal_id == signal.signal_id
        )
    elif expected_proof == "expiry_deadline_source":
        terminal_kind = "EXPIRY_DEADLINE"
        terminal_source_id = terminal_event.expiry_source_id
        terminal_digest = proof.source_digest
        proof_matches = (
            terminal_source_id is not None
            and proof.signal_source is signal
            and proof.signal_id == signal.signal_id
        )
    elif expected_proof == "signal_evidence_source":
        terminal_kind = "SIGNAL_EVIDENCE"
        terminal_source_id = proof.evidence_id
        terminal_digest = proof.source_digest
        proof_matches = (
            terminal_event.signal_evidence_id == proof.evidence_id
            and proof.signal_source is signal
        )
    elif expected_proof == "shadow_fill_source":
        terminal_kind = "SHADOW_FILL"
        terminal_source_id = proof.lifecycle_event.lifecycle_event_id
        terminal_digest = proof.source_digest
        proof_matches = (
            proof.signal_source is signal
            and proof.lifecycle_event.lifecycle_event_id
            == terminal_event.lifecycle_event_id
        )
    else:
        terminal_kind = "CLOSED_TRADE"
        terminal_source_id = proof.trade_id
        terminal_digest = proof.source_digest
        proof_matches = (
            proof.signal_id == signal.signal_id
            and proof.lifecycle_event_id == terminal_event.lifecycle_event_id
        )
    if (
        not proof_matches
        or type(terminal_source_id) is not str
        or not terminal_source_id
        or type(terminal_digest) is not str
        or _SHA256.fullmatch(terminal_digest) is None
    ):
        raise ValidationError("PHASE1_VALIDATION_TERMINAL_PROOF_MISMATCH")

    triggered, filled, closed = _phase1_terminal_trade_flags(source)
    disposition = PublishedSignalDisposition(
        signal_id=signal.signal_id,
        role=signal.role,
        status=status,
        timestamps_complete=True,
        source_evidence_complete=True,
        session_complete=status not in _NON_COMPLETION_TERMINALS,
        terminal_evidence_kind=terminal_kind,
        terminal_evidence_digest=terminal_digest,
        terminal_source_id=terminal_source_id,
    )
    if not disposition.complete:
        raise ValidationError("PHASE1_VALIDATION_DISPOSITION_INCOMPLETE")

    net_r: Decimal | None = None
    fill_price: Decimal | None = None
    filled_at: datetime | None = None
    trade_source_id: str | None = None
    trade_source_digest: str | None = None
    if status is SignalStatus.CLOSED:
        if proof.initial_risk_micros <= 0:
            raise ValidationError("PHASE1_VALIDATION_TRADE_SOURCE_MISMATCH")
        with localcontext() as context:
            context.prec = 80
            net_r = Decimal(proof.net_r_numerator_micros) / Decimal(
                proof.initial_risk_micros
            )
    elif status is SignalStatus.SHADOW_FILLED_INFORMATIONAL:
        if (
            signal.role != "WATCHLIST_SHADOW"
            or terminal_event.price_micros is None
            or terminal_event.price_micros <= 0
        ):
            raise ValidationError("PHASE1_VALIDATION_TRADE_SOURCE_MISMATCH")
        fill_price = money_from_micros(terminal_event.price_micros)
        filled_at = terminal_event.event_time
        trade_source_id = terminal_source_id
        trade_source_digest = terminal_digest
    return disposition, Phase1Trade(
        signal_id=signal.signal_id,
        role=signal.role,
        triggered=triggered,
        filled=filled,
        closed=closed,
        record_complete=True,
        net_r=net_r,
        fill_price=fill_price,
        filled_at=filled_at,
        source_id=trade_source_id,
        source_digest=trade_source_digest,
    )


def _phase1_validation_equity_curve(
    history: object,
    *,
    ledger_name: str,
    expected_sessions: tuple[date, ...],
) -> tuple[EquityPoint, ...]:
    points = tuple(history.equity_points)
    if (
        history.ledger_name != ledger_name
        or history.expected_equity_count != len(points)
        or tuple(point.session_date for point in points) != expected_sessions
        or tuple(point.source_cursor for point in points)
        != tuple(sorted(point.source_cursor for point in points))
        or history.equity_terminal_cursor != points[-1].source_cursor
    ):
        raise ValidationError("PHASE1_VALIDATION_EQUITY_SOURCE_INCOMPLETE")
    return tuple(
        EquityPoint(
            ledger_name=ledger_name,
            cash=money_from_micros(point.cash_micros),
            positions_value=money_from_micros(
                point.positions_value_micros
            ),
            equity=money_from_micros(point.equity_micros),
            external_cash_flow=money_from_micros(
                point.external_cash_flow_micros
            ),
            at=point.at,
        )
        for point in points
    )


def _issue_phase1_promotion_from_journal_source(
    source: object,
    *,
    calendar_resolver: object,
) -> PromotionDecision:
    """Issue only from one complete owner-current Phase 1 window source."""
    from .journal import (
        Phase1AdherenceCheckSource,
        Phase1AdherenceReviewSource,
        Phase1PublishedSignalDispositionSource,
        Phase1ValidationWindowSource,
        is_verified_phase1_validation_window_source,
        phase1_sources_share_owner,
    )
    from .risk import SessionCalendarResolver, _calendar_digest

    if not isinstance(source, Phase1ValidationWindowSource) or not (
        is_verified_phase1_validation_window_source(source)
    ):
        raise ValidationError("PHASE1_VALIDATION_WINDOW_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise ValidationError("PHASE1_VALIDATION_CALENDAR_UNVERIFIED")
    if source.calendar_digest != _calendar_digest(calendar_resolver):
        raise ValidationError("PHASE1_VALIDATION_CALENDAR_MISMATCH")

    expected_sessions = tuple(
        source.started_session + timedelta(days=offset)
        for offset in range(
            (source.through_session - source.started_session).days + 1
        )
        if calendar_resolver.is_open(
            source.started_session + timedelta(days=offset)
        )
    )
    if (
        not expected_sessions
        or source.expected_open_sessions != expected_sessions
        or source.through_session != expected_sessions[-1]
        or source.starting_capital_micros != 5_000_000_000
        or source.started_at > source.received_at
        or source.received_at > source.query_cutoff
    ):
        raise ValidationError("PHASE1_VALIDATION_WINDOW_SOURCE_INCOMPLETE")

    signal_sources = tuple(source.signal_sources)
    disposition_sources = tuple(source.disposition_sources)
    adherence_sources = tuple(source.adherence_check_sources)
    adherence_review_sources = tuple(source.adherence_review_sources)
    if (
        not signal_sources
        or source.expected_signal_count != len(signal_sources)
        or source.expected_disposition_count != len(disposition_sources)
        or len(disposition_sources) != len(signal_sources)
        or source.expected_adherence_count != len(adherence_sources)
        or len(adherence_sources)
        != len(signal_sources) * len(_PHASE1_ADHERENCE_CHECK_NAMES)
        or len(adherence_review_sources) != len(signal_sources)
        or len({signal.signal_id for signal in signal_sources})
        != len(signal_sources)
        or any(
            signal.validation_window_id != source.validation_window_id
            or signal.publication_session not in expected_sessions
            or not phase1_sources_share_owner(source, signal)
            for signal in signal_sources
        )
        or any(
            not isinstance(item, Phase1PublishedSignalDispositionSource)
            or not phase1_sources_share_owner(source, item)
            for item in disposition_sources
        )
        or any(
            not isinstance(item, Phase1AdherenceCheckSource)
            for item in adherence_sources
        )
        or any(
            not isinstance(item, Phase1AdherenceReviewSource)
            or not phase1_sources_share_owner(source, item)
            or item.query_cutoff != source.query_cutoff
            for item in adherence_review_sources
        )
    ):
        raise ValidationError("PHASE1_VALIDATION_WINDOW_SOURCE_INCOMPLETE")

    expected_signal_cursor = max(signal.row_id for signal in signal_sources)
    lifecycle_events = tuple(
        event
        for disposition in disposition_sources
        for event in disposition.lifecycle_events
    )
    expected_lifecycle_cursor = max(
        (event.row_id for event in lifecycle_events),
        default=None,
    )
    expected_adherence_cursor = max(
        (item.row_id for item in adherence_sources),
        default=None,
    )
    if (
        source.signal_terminal_cursor != expected_signal_cursor
        or source.signal_source_highwater != expected_signal_cursor
        or source.lifecycle_terminal_cursor != expected_lifecycle_cursor
        or source.lifecycle_source_highwater
        != (0 if expected_lifecycle_cursor is None else expected_lifecycle_cursor)
        or source.adherence_terminal_cursor != expected_adherence_cursor
        or source.adherence_source_highwater
        != (0 if expected_adherence_cursor is None else expected_adherence_cursor)
    ):
        raise ValidationError("PHASE1_VALIDATION_WINDOW_CURSOR_INCOMPLETE")

    signal_by_id = {signal.signal_id: signal for signal in signal_sources}
    dispositions: list[PublishedSignalDisposition] = []
    trades: list[Phase1Trade] = []
    disposition_by_signal: dict[str, object] = {}
    for disposition_source in disposition_sources:
        signal = disposition_source.signal_source
        if (
            signal_by_id.get(signal.signal_id) is not signal
            or signal.signal_id in disposition_by_signal
            or disposition_source.query_cutoff > source.query_cutoff
        ):
            raise ValidationError("PHASE1_VALIDATION_DISPOSITION_INCOMPLETE")
        disposition, trade_value = _phase1_validation_disposition_material(
            disposition_source
        )
        dispositions.append(disposition)
        trades.append(trade_value)
        disposition_by_signal[signal.signal_id] = disposition_source
    if set(disposition_by_signal) != set(signal_by_id):
        raise ValidationError("PHASE1_VALIDATION_DISPOSITION_INCOMPLETE")

    from .policy import Policy

    policy = Policy.from_toml(
        Path(__file__).resolve().parents[2] / "config" / "policy.toml"
    )
    current_adherence: dict[str, Phase1AdherenceAuthority] = {}
    for review_source in adherence_review_sources:
        review_signal = review_source.signal_source
        signal_id = review_signal.signal_id
        if (
            signal_id in current_adherence
            or signal_by_id.get(signal_id) is not review_signal
            or disposition_by_signal.get(signal_id)
            is not review_source.disposition_source
        ):
            raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")
        current_authority = _issue_phase1_adherence_from_journal_source(
            review_source,
            calendar_resolver=calendar_resolver,
            policy=policy,
        )
        if not is_issued_phase1_adherence_authority(current_authority):
            raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")
        current_adherence[signal_id] = current_authority
    if set(current_adherence) != set(signal_by_id):
        raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")

    adherence_by_signal: dict[str, list[object]] = {
        signal_id: [] for signal_id in signal_by_id
    }
    for item in adherence_sources:
        if (
            item.validation_window_id != source.validation_window_id
            or item.signal_id not in adherence_by_signal
            or item.received_at > source.query_cutoff
            or item.evaluated_at > item.received_at
        ):
            raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")
        adherence_by_signal[item.signal_id].append(item)

    passed_applicable = 0
    total_applicable = 0
    hard_breach_codes: list[str] = []
    trade_by_signal = {trade.signal_id: trade for trade in trades}
    for signal in signal_sources:
        rows = tuple(adherence_by_signal[signal.signal_id])
        current_checks = current_adherence[signal.signal_id].checks
        trade_value = trade_by_signal[signal.signal_id]
        applicability = _phase1_adherence_applicability(
            role=signal.role,
            triggered=trade_value.triggered,
            filled=trade_value.filled,
            closed=trade_value.closed,
        )
        if (
            len(rows) != len(_PHASE1_ADHERENCE_CHECK_NAMES)
            or tuple(row.check_name for row in rows)
            != _PHASE1_ADHERENCE_CHECK_NAMES
            or len({row.check_id for row in rows}) != len(rows)
            or len({row.authority_digest for row in rows}) != 1
            or len(current_checks) != len(rows)
        ):
            raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")
        for (check_name, applicable), row, current in zip(
            applicability,
            rows,
            current_checks,
            strict=True,
        ):
            failures = tuple(row.failure_codes)
            breaches = tuple(row.hard_breach_codes)
            expected_passed = applicable and not (failures or breaches)
            if (
                row.check_name != check_name
                or row.applicable != applicable
                or row.passed != expected_passed
                or row.hard_breach != bool(breaches)
                or (not applicable and (failures or breaches))
                or _SHA256.fullmatch(row.evidence_digest) is None
                or _SHA256.fullmatch(row.authority_digest) is None
                or _SHA256.fullmatch(row.source_digest) is None
                or (
                    current.check_name,
                    current.applicable,
                    current.passed,
                    current.hard_breach,
                    current.failure_codes,
                    current.hard_breach_codes,
                )
                != (
                    row.check_name,
                    row.applicable,
                    row.passed,
                    row.hard_breach,
                    failures,
                    breaches,
                )
            ):
                raise ValidationError(
                    "PHASE1_VALIDATION_ADHERENCE_MISMATCH"
                )
            if applicable:
                total_applicable += 1
                passed_applicable += int(row.passed)
            hard_breach_codes.extend(breaches)
    if total_applicable == 0:
        raise ValidationError("PHASE1_VALIDATION_ADHERENCE_INCOMPLETE")

    histories = (source.canonical_history, source.actual_history)
    if any(
        not phase1_sources_share_owner(source, history)
        or history.validation_window_id != source.validation_window_id
        or history.through_session != source.through_session
        or history.query_cutoff != source.query_cutoff
        for history in histories
    ):
        raise ValidationError("PHASE1_VALIDATION_EQUITY_SOURCE_MISMATCH")
    canonical_curve = _phase1_validation_equity_curve(
        source.canonical_history,
        ledger_name="CANONICAL",
        expected_sessions=expected_sessions,
    )
    actual_curve = _phase1_validation_equity_curve(
        source.actual_history,
        ledger_name="ACTUAL",
        expected_sessions=expected_sessions,
    )
    canonical_closed_by_id = {
        trade.trade_id: trade for trade in source.canonical_history.closed_trades
    }
    for disposition_source in disposition_sources:
        closed_trade = disposition_source.closed_trade
        if closed_trade is not None and (
            canonical_closed_by_id.get(closed_trade.trade_id) != closed_trade
        ):
            raise ValidationError("PHASE1_VALIDATION_TRADE_SOURCE_MISMATCH")

    window = Phase1Window(
        started_on=source.started_session,
        as_of=source.through_session,
        expected_open_sessions=expected_sessions,
        trades=tuple(trades),
        published_signals=tuple(dispositions),
        canonical_equity=canonical_curve,
        actual_equity=actual_curve,
        adherence=AdherenceSummary(passed_applicable, total_applicable),
        hard_breach_codes=_deduplicate(hard_breach_codes),
    )
    diagnostic = evaluate_phase1(window)
    authority_payload = {
        "namespace": "stock-monitor/phase1-promotion-authority/v1",
        "source_digest": source.source_digest,
        "passed": diagnostic.passed,
        "status": diagnostic.status.value,
        "reason_codes": list(diagnostic.reason_codes),
        "closed_primary_trades": diagnostic.closed_primary_trades,
        "elapsed_days": diagnostic.elapsed_days,
        "mean_net_r": (
            None if diagnostic.mean_net_r is None else str(diagnostic.mean_net_r)
        ),
        "adherence": str(diagnostic.adherence),
        "canonical_max_drawdown": str(diagnostic.canonical_max_drawdown),
        "actual_max_drawdown": str(diagnostic.actual_max_drawdown),
    }
    authority_digest = sha256(
        json.dumps(
            authority_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    decision = PromotionDecision(
        passed=diagnostic.passed,
        status=diagnostic.status,
        reason_codes=diagnostic.reason_codes,
        closed_primary_trades=diagnostic.closed_primary_trades,
        elapsed_days=diagnostic.elapsed_days,
        mean_net_r=diagnostic.mean_net_r,
        adherence=diagnostic.adherence,
        canonical_max_drawdown=diagnostic.canonical_max_drawdown,
        actual_max_drawdown=diagnostic.actual_max_drawdown,
        source_digest=source.source_digest,
        authority_digest=authority_digest,
        _phase1_source=source,
    )
    fingerprint = _promotion_decision_fingerprint(decision)
    with _ISSUED_PROMOTION_DECISIONS_LOCK:
        _ISSUED_PROMOTION_DECISIONS[id(decision)] = (
            ref(decision),
            fingerprint,
            ref(source),
        )
    return decision


def _countable_trade(
    trade: Phase1Trade,
    dispositions: dict[str, PublishedSignalDisposition],
) -> bool:
    disposition = dispositions[trade.signal_id]
    return (
        trade.role == "PRIMARY"
        and trade.triggered
        and trade.filled
        and trade.closed
        and trade.record_complete
        and trade.net_r is not None
        and disposition.role == "PRIMARY"
        and disposition.status is SignalStatus.CLOSED
        and disposition.complete
    )


def _mean_r(trades: Sequence[Phase1Trade]) -> Decimal | None:
    if not trades:
        return None
    values = tuple(trade.net_r for trade in trades)
    assert all(value is not None for value in values)
    typed = tuple(value for value in values if value is not None)
    digit_budget = sum(max(1, len(value.as_tuple().digits)) for value in typed)
    with localcontext() as context:
        context.prec = max(80, digit_budget + 32)
        return sum(typed, Decimal("0")) / Decimal(len(typed))


def _deduplicate(codes: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if code not in seen:
            result.append(code)
            seen.add(code)
    return tuple(result)


def evaluate_phase1(window: Phase1Window) -> PromotionDecision:
    """Apply every fixed Phase 1 promotion threshold without rounding."""
    if not isinstance(window, Phase1Window):
        raise TypeError("evaluate_phase1 requires a Phase1Window")
    dispositions = {
        signal.signal_id: signal for signal in window.published_signals
    }
    counted = tuple(
        trade
        for trade in window.trades
        if _countable_trade(trade, dispositions)
    )
    mean_net_r = _mean_r(counted)
    adherence = window.adherence.ratio
    canonical_drawdown = max_drawdown(window.canonical_equity)
    actual_drawdown = max_drawdown(window.actual_equity)
    reasons: list[str] = list(window.hard_breach_codes)
    if len(counted) < _MINIMUM_TRADES:
        reasons.append("MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET")
    if window.elapsed_days < _MINIMUM_DAYS:
        reasons.append("MINIMUM_ELAPSED_DAYS_NOT_MET")
    if mean_net_r is None or mean_net_r <= _ZERO:
        reasons.append("MEAN_NET_R_NOT_POSITIVE")
    if adherence < _MINIMUM_ADHERENCE:
        reasons.append("ADHERENCE_BELOW_90_PERCENT")
    if canonical_drawdown > _MAXIMUM_DRAWDOWN:
        reasons.append("CANONICAL_DRAWDOWN_BREACH")
    if actual_drawdown > _MAXIMUM_DRAWDOWN:
        reasons.append("ACTUAL_DRAWDOWN_BREACH")
    incomplete_dispositions = tuple(
        signal.signal_id
        for signal in window.published_signals
        if not signal.complete
    )
    if incomplete_dispositions:
        reasons.append("INCOMPLETE_SIGNAL_DISPOSITIONS")
    incomplete_closed_primary = tuple(
        signal.signal_id
        for signal in window.published_signals
        if signal.role == "PRIMARY"
        and signal.status is SignalStatus.CLOSED
        and not any(
            trade.signal_id == signal.signal_id
            and _countable_trade(trade, dispositions)
            for trade in window.trades
        )
    )
    if incomplete_closed_primary:
        reasons.append("CLOSED_PRIMARY_TRADE_RECORD_INCOMPLETE")
    reason_codes = _deduplicate(reasons)
    passed = not reason_codes
    irrevocable_failure = bool(window.hard_breach_codes) or any(
        code
        in {
            "CANONICAL_DRAWDOWN_BREACH",
            "ACTUAL_DRAWDOWN_BREACH",
        }
        for code in reason_codes
    )
    status = (
        PromotionStatus.PASSED
        if passed
        else PromotionStatus.FAILED
        if irrevocable_failure
        else PromotionStatus.IN_PROGRESS
    )
    return PromotionDecision(
        passed=passed,
        status=status,
        reason_codes=reason_codes,
        closed_primary_trades=len(counted),
        elapsed_days=window.elapsed_days,
        mean_net_r=mean_net_r,
        adherence=adherence,
        canonical_max_drawdown=canonical_drawdown,
        actual_max_drawdown=actual_drawdown,
    )


__all__ = [
    "AdherenceSummary",
    "Phase1AdherenceAuthority",
    "Phase1AdherenceCheckDecision",
    "Phase1Trade",
    "Phase1Window",
    "PromotionDecision",
    "PromotionStatus",
    "PublishedSignalDisposition",
    "ValidationError",
    "evaluate_phase1",
    "is_issued_phase1_adherence_authority",
    "is_issued_promotion_decision",
]
