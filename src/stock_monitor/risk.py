"""Pure risk, settlement, position, and circuit-breaker decisions."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from hashlib import sha256
from sys import _getframe
from threading import RLock
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo

from .domain import (
    DomainValidationError,
    MAX_MICRODOLLARS,
    money_from_micros,
    money_to_micros,
    require_aware_timestamp,
    stable_execution_event_identity,
)
from .evidence import (
    DateRange,
    EvidenceDecision,
    ReviewedEvidenceBundle,
    _is_reviewed_bundle,
    classify_evidence,
    is_reviewed_evidence_decision,
)
from .journal import (
    JournalAccountCheckWindowSource,
    JournalActionSource,
    JournalActualReplaySource,
    is_verified_journal_action_source,
    is_verified_journal_replay_source,
    is_verified_journal_window_source,
)
from .market_calendar import (
    CalendarError,
    MarketCalendar,
    is_release_verified_market_calendar,
    is_validated_market_calendar,
)
from .phase1 import (
    EquityPoint as Phase1MarkedEquityPoint,
    ExitReason,
    IntradayObservation,
    ObservationKind,
    PaperExitResult,
    simulate_exit,
    simulate_forced_exit,
)
from .policy import Policy


_ZERO = Decimal("0")
_TWO = Decimal("2")
_R_MULTIPLE_QUANTUM = Decimal("0.000001")
_MAX_LIVE_EXPOSURE = Decimal("1000")
_MAX_POSITION_RISK = Decimal("25")
_MAX_COMBINED_RISK = Decimal("50")
_VALIDATION_CAPITAL = Decimal("5000")
_PORTFOLIO_RISK_FINGERPRINT_DOMAIN = (
    b"stock-monitor/portfolio-risk-authority/v1"
)
_BREAKER_HISTORY_FINGERPRINT_DOMAIN = (
    b"stock-monitor/breaker-history-authority/v1"
)
_ET = ZoneInfo("America/New_York")
_MARK_SESSION_AUTHORITY = object()
_BREAKER_EVALUATION_AUTHORITY = object()
_AUTHORITY_LOCK = RLock()


@dataclass(frozen=True, slots=True, eq=False)
class _RiskAuthorityRecord:
    seal: object
    children: tuple[object, ...]
    journal_binding: tuple[object, str, object] | None = None
    phase1_bindings: tuple[tuple[object, str, object], ...] = ()


_JOURNAL_WINDOW_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_CONFIRMED_BUY_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_POSITION_EVENT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PHASE1_SIGNAL_EVIDENCE_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PHASE1_POSITION_EVIDENCE_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_MARK_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PHASE1_POSITION_EXIT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PHASE1_EQUITY_POINT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_BREAKER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_BREAKER_HISTORY_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PAIRED_BREAKER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_PORTFOLIO_RISK_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
_ACTUAL_BREAKER_REFRESH_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_SETTLEMENT_LEDGER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_JOURNAL_DERIVED_SOURCE_BINDINGS: dict[
    int,
    tuple[ReferenceType[object], ReferenceType[object], str],
] = {}
_PHASE1_DERIVED_SOURCE_BINDINGS: dict[
    int,
    tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
] = {}
_LONG_PLAN_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], object],
] = {}
MAX_HOLD_SESSIONS = 10
CONSECUTIVE_LOSS_LIMIT = 3
LOSS_PAUSE_SESSIONS = 5
MAX_WEEKLY_DRAWDOWN = Decimal("100")
MAX_MONTHLY_DRAWDOWN = Decimal("250")
_ACCOUNT_INVALIDATING_EVENT_KINDS = frozenset(
    {
        "ACCOUNT_CHECK",
        "BUY",
        "BOUGHT",
        "CASH_ADJUSTMENT",
        "DEPOSIT",
        "FEE",
        "PARTIAL_FILL",
        "PENDING_CLARIFICATION",
        "PENDING_ORDER",
        "POSITION_ADJUSTMENT",
        "RECONCILE_CASH",
        "RECONCILE_PENDING_ORDERS",
        "RECONCILE_UNRELATED_POSITION",
        "RECONCILIATION",
        "SELL",
        "SOLD",
        "STOP_FILLED",
        "STOP_UPDATED",
        "WITHDRAWAL",
    }
)


class RiskBlock(ValueError):
    """A deterministic risk invariant blocked a proposed operation."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        self.code = reason_code
        super().__init__(reason_code)


def _decimal_work_precision(*values: Decimal) -> int:
    finite = tuple(
        value
        for value in values
        if type(value) is Decimal and value.is_finite() and value != _ZERO
    )
    if not finite:
        return 50
    parts = tuple(value.as_tuple() for value in finite)
    exponents = tuple(int(part.exponent) for part in parts)
    coefficient_digits = sum(max(1, len(part.digits)) for part in parts)
    exponent_span = max(exponents) - min(exponents)
    nonzero = tuple(value for value in finite if value != _ZERO)
    magnitude = max(abs(value.adjusted()) for value in nonzero) if nonzero else 0
    return max(50, coefficient_digits + exponent_span + magnitude + 32)


def _require_decimal(
    value: object,
    *,
    reason_code: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise RiskBlock(reason_code)
    if positive and value <= _ZERO:
        raise RiskBlock(reason_code)
    if nonnegative and value < _ZERO:
        raise RiskBlock(reason_code)
    return value


def _require_nonnegative_int(value: object, reason_code: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_MICRODOLLARS:
        raise RiskBlock(reason_code)
    return value


def _require_positive_int(value: object, reason_code: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_MICRODOLLARS:
        raise RiskBlock(reason_code)
    return value


def _freeze_reason_codes(value: object, reason_code: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise RiskBlock(reason_code)
    try:
        reasons = tuple(value)  # type: ignore[arg-type]
    except TypeError:
        raise RiskBlock(reason_code) from None
    if any(type(reason) is not str or not reason for reason in reasons):
        raise RiskBlock(reason_code)
    return reasons


def _require_aware(value: object, reason_code: str) -> datetime:
    try:
        return require_aware_timestamp(value, "timestamp")  # type: ignore[arg-type]
    except DomainValidationError:
        raise RiskBlock(reason_code) from None


def _require_money(
    value: object,
    *,
    reason_code: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    result = _require_decimal(
        value,
        reason_code=reason_code,
        positive=positive,
        nonnegative=nonnegative,
    )
    try:
        micros = money_to_micros(result)
    except DomainValidationError:
        raise RiskBlock(reason_code) from None
    return money_from_micros(micros)


def _is_tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(value, tick_size)
        units = value / tick_size
        return units == units.to_integral_value()


def _round_up_to_tick(value: Decimal, tick_size: Decimal) -> Decimal:
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(value, tick_size)
        return (
            (value / tick_size).to_integral_value(rounding=ROUND_CEILING)
            * tick_size
        )


@dataclass(frozen=True, slots=True)
class PositionPlan:
    """Whole-share size and its exact locked exposure/risk arithmetic."""

    quantity: int
    exposure: Decimal
    planned_risk: Decimal

    def __post_init__(self) -> None:
        _require_positive_int(self.quantity, "QUANTITY_BELOW_ONE")
        object.__setattr__(
            self,
            "exposure",
            _require_money(
                self.exposure,
                reason_code="INVALID_EXPOSURE",
                positive=True,
            ),
        )
        object.__setattr__(
            self,
            "planned_risk",
            _require_money(
                self.planned_risk,
                reason_code="INVALID_PLANNED_RISK",
                positive=True,
            ),
        )


@dataclass(frozen=True, slots=True)
class LongPlanRequest:
    """Instrument/session facts needed beyond the locked sizing primitive."""

    entry: Decimal
    stop: Decimal
    tick_size: Decimal
    session_date: date
    symbol: str | None = None
    published_target: Decimal | None = None

    def __post_init__(self) -> None:
        for attribute, code in (
            ("entry", "INVALID_ENTRY"),
            ("stop", "INVALID_STOP"),
            ("tick_size", "INVALID_TICK_SIZE"),
        ):
            object.__setattr__(
                self,
                attribute,
                _require_money(
                    getattr(self, attribute),
                    reason_code=code,
                    positive=True,
                ),
            )
        if type(self.session_date) is not date:
            raise RiskBlock("INVALID_SESSION_DATE")
        if self.symbol is not None and (
            type(self.symbol) is not str or not self.symbol
        ):
            raise RiskBlock("INVALID_SYMBOL")
        if self.published_target is not None:
            object.__setattr__(
                self,
                "published_target",
                _require_money(
                    self.published_target,
                    reason_code="INVALID_PUBLISHED_TARGET",
                    positive=True,
                ),
            )

    @classmethod
    def from_scored_candidate(cls, candidate: object) -> LongPlanRequest:
        """Consume Task 5's validated price contract without repricing it."""
        from .screening import ScoredCandidate

        if not isinstance(candidate, ScoredCandidate):
            raise TypeError("long plan candidate must be a ScoredCandidate")
        if (
            candidate.publication_session is None
            or candidate.maximum_permitted_entry is None
            or candidate.recommended_stop is None
            or candidate.tick_size is None
            or candidate.target_price is None
        ):
            raise RiskBlock("TASK5_PRICE_CONTRACT_INCOMPLETE")
        return cls(
            entry=candidate.maximum_permitted_entry,
            stop=candidate.recommended_stop,
            tick_size=candidate.tick_size,
            session_date=candidate.publication_session,
            symbol=candidate.symbol,
            published_target=candidate.target_price,
        )


@dataclass(frozen=True, slots=True)
class PortfolioState:
    """Explicit portfolio authority consumed by :func:`plan_long`."""

    settled_cash: Decimal | None
    deployed: Decimal
    open_risk: Decimal
    open_position_count: int
    entries_this_session: int
    open_symbols: tuple[str, ...] = ()
    settlement_verified: bool = False
    reconciliation_required: bool = False
    stop_unverified: bool = False
    breaker_states: tuple[BreakerState | PairedBreakerState, ...] = ()
    calendar_resolver: SessionCalendarResolver | None = None

    def __post_init__(self) -> None:
        if self.settled_cash is not None:
            object.__setattr__(
                self,
                "settled_cash",
                _require_money(
                    self.settled_cash,
                    reason_code="INVALID_SETTLED_CASH",
                    nonnegative=True,
                ),
            )
        for attribute, code in (
            ("deployed", "INVALID_DEPLOYED_CAPITAL"),
            ("open_risk", "INVALID_OPEN_RISK"),
        ):
            object.__setattr__(
                self,
                attribute,
                _require_money(
                    getattr(self, attribute),
                    reason_code=code,
                    nonnegative=True,
                ),
            )
        _require_nonnegative_int(
            self.open_position_count,
            "INVALID_OPEN_POSITION_COUNT",
        )
        _require_nonnegative_int(
            self.entries_this_session,
            "INVALID_SESSION_ENTRY_COUNT",
        )
        if (
            type(self.open_symbols) is not tuple
            or any(
                type(symbol) is not str
                or not symbol
                or symbol != symbol.upper()
                for symbol in self.open_symbols
            )
            or len(set(self.open_symbols)) != len(self.open_symbols)
        ):
            raise RiskBlock("INVALID_OPEN_SYMBOLS")
        if type(self.settlement_verified) is not bool:
            raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
        if type(self.reconciliation_required) is not bool:
            raise RiskBlock("INVALID_RECONCILIATION_STATE")
        if type(self.stop_unverified) is not bool:
            raise RiskBlock("INVALID_STOP_VERIFICATION_STATE")
        if type(self.breaker_states) is not tuple or any(
            not isinstance(state, (BreakerState, PairedBreakerState))
            for state in self.breaker_states
        ):
            raise RiskBlock("INVALID_BREAKER_STATE")
        if len(self.breaker_states) > 1:
            raise RiskBlock("INVALID_BREAKER_STATE")
        if self.calendar_resolver is not None and not isinstance(
            self.calendar_resolver,
            SessionCalendarResolver,
        ):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PortfolioRiskAuthority:
    """Replay-derived portfolio capacity bound to one session and policy."""

    request: LongPlanRequest
    portfolio_state: PortfolioState
    scope: str
    as_of: datetime
    ledger_name: str
    projection_through_cursor: int | None
    settlement_through_cursor: int | None
    projection_digest: str
    settlement_source: str
    settlement_digest: str
    policy_digest: str
    calendar_digest: str
    breaker_refresh_digest: str
    breaker_refresh_through_execution_cursor: int | None
    breaker_refresh_through_close_cursor: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.request, LongPlanRequest) or not isinstance(
            self.portfolio_state,
            PortfolioState,
        ):
            raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY")
        if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
            raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY")
        expected_ledger = {
            "CANONICAL_PUBLICATION": "CANONICAL",
            "ACTUAL_ENTRY": "ACTUAL",
        }.get(self.scope)
        if expected_ledger != self.ledger_name:
            raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY_SCOPE")
        _require_aware(self.as_of, "INVALID_PORTFOLIO_CUTOFF")
        for cursor in (
            self.projection_through_cursor,
            self.settlement_through_cursor,
        ):
            if cursor is not None:
                _require_positive_int(cursor, "INVALID_PORTFOLIO_CURSOR")
        refresh_cursors = (
            self.breaker_refresh_through_execution_cursor,
            self.breaker_refresh_through_close_cursor,
        )
        if self.scope == "CANONICAL_PUBLICATION":
            if any(cursor is not None for cursor in refresh_cursors):
                raise RiskBlock("INVALID_BREAKER_REFRESH")
        else:
            if any(cursor is None for cursor in refresh_cursors):
                raise RiskBlock("ACTUAL_BREAKER_REFRESH_UNVERIFIED")
            for cursor in refresh_cursors:
                _require_nonnegative_int(
                    cursor,
                    "INVALID_BREAKER_REFRESH_CURSOR",
                )
        if self.settlement_source not in {
            "CANONICAL_LEDGER",
            "PHASE1_CANONICAL_REPLAY",
            "ACTUAL_SETTLEMENT_LEDGER",
        }:
            raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
        for digest in (
            self.projection_digest,
            self.settlement_digest,
            self.policy_digest,
            self.calendar_digest,
            self.breaker_refresh_digest,
        ):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY")


def _policy_digest(policy: Policy) -> str:
    payload = {
        "version": 1,
        "capital_micros": money_to_micros(policy.capital),
        "max_live_exposure_micros": money_to_micros(policy.max_live_exposure),
        "max_position_risk_micros": money_to_micros(policy.max_position_risk),
        "max_combined_risk_micros": money_to_micros(policy.max_combined_risk),
        "max_positions": policy.max_positions,
        "max_entries_per_session": policy.max_entries_per_session,
        "min_score": policy.min_score,
        "max_monthly_drawdown_micros": money_to_micros(
            policy.max_monthly_drawdown
        ),
        "max_weekly_drawdown_micros": money_to_micros(
            policy.max_weekly_drawdown
        ),
        "universe_max_age_days": policy.universe_max_age_days,
        "live_quote_max_age_seconds": policy.live_quote_max_age_seconds,
        "disagreement_tolerance": format(
            policy.disagreement_tolerance.normalize(),
            "f",
        ),
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _calendar_digest(resolver: SessionCalendarResolver) -> str:
    payload = {
        "version": 1,
        "release_verified": resolver.release_verified,
        "calendars": [
            {
                "year": calendar.year,
                "timezone": getattr(calendar.timezone, "key", None),
                "retrieved_at": calendar.retrieved_at.isoformat(),
                "reviewed_at": calendar.reviewed_at.isoformat(),
                "open_session_count": calendar.open_session_count,
                "closed_dates": [
                    day.isoformat() for day in calendar.closed_dates
                ],
                "early_closes": [
                    {
                        "date": day.isoformat(),
                        "open_time": session.open_time.isoformat(),
                        "close_time": session.close_time.isoformat(),
                        "review_time": session.review_time.isoformat(),
                    }
                    for day, session in sorted(calendar.early_closes.items())
                ],
                "sources": [
                    {
                        "role": source.role,
                        "name": source.name,
                        "url": source.url,
                        "retrieved_at": source.retrieved_at.isoformat(),
                        "reviewed_at": source.reviewed_at.isoformat(),
                        "closed_dates": [
                            day.isoformat() for day in source.closed_dates
                        ],
                        "early_closes": [
                            [day.isoformat(), close.isoformat()]
                            for day, close in source.early_closes
                        ],
                    }
                    for source in calendar.sources
                ],
            }
            for calendar in resolver.calendars
        ],
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _portfolio_risk_fingerprint(
    authority: PortfolioRiskAuthority,
) -> object:
    """Return a hook-free exact structural seal for one portfolio authority."""
    if type(authority) is not PortfolioRiskAuthority:
        raise TypeError("portfolio risk authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        authority,
        domain=_PORTFOLIO_RISK_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualBreakerRefreshAuthority:
    """Task 7 handoff for a complete live-breaker read through one buy.

    Task 6 deliberately provides no issuer.  The typed Journal adapter must
    prove the complete execution and close streams through ``as_of`` before it
    may register one of these values in a later task.
    """

    as_of: datetime
    through_execution_cursor: int
    through_close_cursor: int
    paired_breaker: PairedBreakerState
    calendar_digest: str
    source_digest: str

    def __post_init__(self) -> None:
        _require_aware(self.as_of, "INVALID_BREAKER_REFRESH_CUTOFF")
        _require_nonnegative_int(
            self.through_execution_cursor,
            "INVALID_BREAKER_REFRESH_CURSOR",
        )
        _require_nonnegative_int(
            self.through_close_cursor,
            "INVALID_BREAKER_REFRESH_CURSOR",
        )
        if not isinstance(self.paired_breaker, PairedBreakerState):
            raise RiskBlock("INVALID_BREAKER_REFRESH")
        for digest in (self.calendar_digest, self.source_digest):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise RiskBlock("INVALID_BREAKER_REFRESH")


def _actual_breaker_refresh_fingerprint(
    authority: ActualBreakerRefreshAuthority,
) -> tuple[object, ...]:
    return (
        authority.as_of,
        authority.through_execution_cursor,
        authority.through_close_cursor,
        authority.paired_breaker,
        authority.calendar_digest,
        authority.source_digest,
    )


def is_issued_actual_breaker_refresh_authority(authority: object) -> bool:
    """Return false until a later typed Journal adapter proves completeness."""
    del authority
    return False


def _is_current_portfolio_risk_authority_without_callbacks(
    authority: object,
) -> bool:
    """Pure final authority, binding, and source-identity verification."""
    return _is_current_risk_authority_without_callbacks(
        _PORTFOLIO_RISK_AUTHORITIES,
        authority,
        exact_type=PortfolioRiskAuthority,
    )


def is_issued_portfolio_risk_authority(authority: object) -> bool:
    if type(authority) is not PortfolioRiskAuthority:
        return False
    # Bound-source verifiers may execute SQLite callbacks.  Finish every one
    # before the callback-free exact source, authority, and registry checks.
    if not _phase1_derived_sources_are_current(authority):
        return False
    return _is_current_portfolio_risk_authority_without_callbacks(authority)


def _issue_portfolio_risk_authority(
    *,
    request: LongPlanRequest,
    ledger_pair: object,
    ledger_name: str,
    breaker_state: BreakerState | PairedBreakerState,
    calendar_resolver: SessionCalendarResolver,
    policy: Policy,
    scope: str,
    as_of: datetime,
    settlement_ledger: object | None = None,
    settled_at: datetime | None = None,
    actual_breaker_refresh: ActualBreakerRefreshAuthority | None = None,
    phase1_canonical_replay: object | None = None,
) -> PortfolioRiskAuthority:
    """Trusted coordinator seam; all capacity fields are recomputed here."""
    from .ledger import (
        LedgerPair,
        Phase1CanonicalLedgerReplay,
        _is_current_phase1_canonical_ledger_replay_authority,
        _phase1_bound_sources as _ledger_phase1_bound_sources,
    )

    if not isinstance(ledger_pair, LedgerPair):
        raise RiskBlock("INVALID_PORTFOLIO_PROJECTION")
    phase1_source_bindings: tuple[tuple[object, str], ...] = ()
    if phase1_canonical_replay is not None:
        if not (
            scope == "CANONICAL_PUBLICATION"
            and ledger_name == "CANONICAL"
            and type(phase1_canonical_replay)
            is Phase1CanonicalLedgerReplay
            and phase1_canonical_replay.ledger_pair is ledger_pair
            and phase1_canonical_replay.cohort
            is ledger_pair.canonical_replay_cohort
        ):
            raise RiskBlock("PHASE1_CANONICAL_REPLAY_UNVERIFIED")
        replay_sources = tuple(
            source
            for source, kind in _ledger_phase1_bound_sources(
                phase1_canonical_replay
            )
            if kind == "CANONICAL_REPLAY"
        )
        history_sources = tuple(
            source
            for source, kind in _phase1_bound_sources(breaker_state)
            if kind == "BREAKER_HISTORY"
        )
        if len(replay_sources) != 1 or len(history_sources) != 1:
            raise RiskBlock("PHASE1_SOURCE_LINEAGE_INCOMPLETE")
        replay_source = replay_sources[0]
        history_source = history_sources[0]
        from .journal import phase1_sources_share_owner

        if not (
            phase1_sources_share_owner(replay_source, history_source)
            and replay_source.validation_window_id
            == history_source.validation_window_id
        ):
            raise RiskBlock("PHASE1_SOURCE_LINEAGE_MISMATCH")
        phase1_source_bindings = (
            (replay_source, "CANONICAL_REPLAY"),
            (history_source, "BREAKER_HISTORY"),
        )
    expected_ledger = {
        "CANONICAL_PUBLICATION": "CANONICAL",
        "ACTUAL_ENTRY": "ACTUAL",
    }.get(scope)
    if expected_ledger != ledger_name:
        raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY_SCOPE")
    as_of = _require_aware(as_of, "INVALID_PORTFOLIO_CUTOFF")
    if as_of.astimezone(_ET).date() != request.session_date:
        raise RiskBlock("PORTFOLIO_CUTOFF_SESSION_MISMATCH")
    if (
        scope == "CANONICAL_PUBLICATION"
        and as_of.astimezone(_ET).time().replace(tzinfo=None) != time(8, 45)
    ):
        raise RiskBlock("PUBLICATION_CUTOFF_MISMATCH")
    scoped_events = tuple(
        event
        for event in ledger_pair.events
        if event.ledger_name == ledger_name
    )
    if any(event.source_received_at > as_of for event in scoped_events):
        raise RiskBlock("PORTFOLIO_LOOKAHEAD")
    scoped_replay_verified = (
        ledger_pair.canonical_replay_verified
        if ledger_name == "CANONICAL"
        else ledger_pair.actual_replay_verified
    )
    scoped_replay_cohort = (
        ledger_pair.canonical_replay_cohort
        if ledger_name == "CANONICAL"
        else ledger_pair.actual_replay_cohort
    )
    if not scoped_replay_verified or scoped_replay_cohort is None:
        raise RiskBlock("PORTFOLIO_PROJECTION_UNVERIFIED")
    if scoped_replay_cohort.query_cutoff != as_of:
        raise RiskBlock("PORTFOLIO_REPLAY_CUTOFF_MISMATCH")
    scoped_cursors = tuple(
        event.cursor for event in scoped_events if event.cursor is not None
    )
    if (
        scoped_replay_cohort.expected_count != len(scoped_events)
        or len(scoped_replay_cohort.references) != len(scoped_events)
        or scoped_replay_cohort.start_cursor
        != (scoped_cursors[0] if scoped_cursors else None)
        or scoped_replay_cohort.terminal_cursor
        != (scoped_cursors[-1] if scoped_cursors else None)
    ):
        raise RiskBlock("PORTFOLIO_REPLAY_COHORT_MISMATCH")
    if scope == "CANONICAL_PUBLICATION":
        if actual_breaker_refresh is not None:
            raise RiskBlock("INVALID_BREAKER_REFRESH")
        if not (
            isinstance(breaker_state, BreakerState)
            and is_issued_breaker_state(breaker_state)
            and breaker_state.ledger_name == "CANONICAL"
        ):
            raise RiskBlock("BREAKER_AUTHORITY_UNVERIFIED")
    else:
        if not (
            isinstance(breaker_state, PairedBreakerState)
            and is_issued_paired_breaker_state(breaker_state)
        ):
            raise RiskBlock("BREAKER_AUTHORITY_UNVERIFIED")
        if not (
            is_issued_actual_breaker_refresh_authority(actual_breaker_refresh)
            and actual_breaker_refresh.paired_breaker is breaker_state
            and actual_breaker_refresh.as_of == as_of
        ):
            raise RiskBlock("ACTUAL_BREAKER_REFRESH_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    resolver_digest = _calendar_digest(calendar_resolver)
    if (
        actual_breaker_refresh is not None
        and actual_breaker_refresh.calendar_digest != resolver_digest
    ):
        raise RiskBlock("BREAKER_CALENDAR_COHORT_MISMATCH")
    breaker_calendar_digests = (
        (breaker_state.calendar_digest,)
        if isinstance(breaker_state, BreakerState)
        else (
            breaker_state.canonical.calendar_digest,
            breaker_state.actual.calendar_digest,
        )
    )
    if any(digest != resolver_digest for digest in breaker_calendar_digests):
        raise RiskBlock("BREAKER_CALENDAR_COHORT_MISMATCH")
    policy.validate()
    required_as_of = calendar_resolver.previous_session(request.session_date)
    if breaker_state.as_of != required_as_of:
        raise RiskBlock("BREAKER_AS_OF_MISMATCH")
    if phase1_canonical_replay is not None:
        # Every SQLite/currentness and caller-dispatchable calendar/policy
        # operation is complete.  This final pure replay seal must immediately
        # precede all LedgerPair and cash/risk derivation.
        if not _is_current_phase1_canonical_ledger_replay_authority(
            phase1_canonical_replay,
            replay_source,
        ):
            raise RiskBlock("PHASE1_CANONICAL_REPLAY_UNVERIFIED")
        if (
            replay_source.query_cutoff != phase1_canonical_replay.query_cutoff
            or phase1_canonical_replay.query_cutoff != as_of
            or replay_source.source_digest
            != phase1_canonical_replay.source_digest
            or money_from_micros(replay_source.canonical_cash_micros)
            != phase1_canonical_replay.canonical_cash
            or money_from_micros(replay_source.settled_buying_power_micros)
            != phase1_canonical_replay.settled_buying_power
            or money_from_micros(replay_source.realized_pnl_micros)
            != phase1_canonical_replay.realized_pnl
        ):
            raise RiskBlock("PHASE1_SOURCE_LINEAGE_MISMATCH")
    snapshot = (
        ledger_pair.canonical if ledger_name == "CANONICAL" else ledger_pair.actual
    )
    if ledger_name == "CANONICAL":
        if settlement_ledger is not None or settled_at is not None:
            raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
        if phase1_canonical_replay is None:
            settled_cash = snapshot.cash
            settlement_source = "CANONICAL_LEDGER"
            settlement_payload = {
                "version": 2,
                "source": settlement_source,
                "settled_cash_micros": money_to_micros(settled_cash),
                "as_of": as_of.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
            }
            settlement_through_cursor = None
        else:
            settled_cash = phase1_canonical_replay.settled_buying_power
            settlement_source = "PHASE1_CANONICAL_REPLAY"
            settlement_through_cursor = (
                phase1_canonical_replay.posting_source_terminal_cursor
            )
            settlement_payload = {
                "version": 3,
                "source": settlement_source,
                "settled_buying_power_micros": money_to_micros(settled_cash),
                "canonical_cash_micros": money_to_micros(
                    phase1_canonical_replay.canonical_cash
                ),
                "realized_pnl_micros": money_to_micros(
                    phase1_canonical_replay.realized_pnl
                ),
                "posting_source_terminal_cursor": settlement_through_cursor,
                "replay_source_digest": phase1_canonical_replay.source_digest,
                "as_of": as_of.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
            }
    else:
        if not isinstance(settlement_ledger, SettlementLedger) or settled_at is None:
            raise RiskBlock("SETTLEMENT_AUTHORITY_UNVERIFIED")
        settled_at = _require_aware(settled_at, "INVALID_SETTLEMENT_TIME")
        if settled_at != as_of:
            raise RiskBlock("SETTLEMENT_CUTOFF_MISMATCH")
        if not settlement_ledger.calendar_resolver.release_verified or (
            _calendar_digest(settlement_ledger.calendar_resolver)
            != resolver_digest
        ):
            raise RiskBlock("SETTLEMENT_CALENDAR_COHORT_MISMATCH")
        if settlement_ledger.initialized_at > as_of or any(
            posting.source_received_at > as_of
            for posting in settlement_ledger.postings
        ):
            raise RiskBlock("PORTFOLIO_LOOKAHEAD")
        if not settlement_ledger.source_verified:
            raise RiskBlock("SETTLEMENT_AUTHORITY_UNVERIFIED")
        settled_cash = settlement_ledger.settled_cash(settled_at)
        settlement_source = "ACTUAL_SETTLEMENT_LEDGER"
        settlement_through_cursor = max(
            (
                posting.cursor
                for posting in settlement_ledger.postings
                if posting.cursor is not None
            ),
            default=None,
        )
        settlement_payload = {
            "version": 2,
            "source": settlement_source,
            "settled_cash_micros": money_to_micros(settled_cash),
            "initialized_at": settlement_ledger.initialized_at.astimezone(
                UTC
            ).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "as_of": settled_at.astimezone(UTC).isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z"),
            "posting_ids": [
                posting.posting_id for posting in settlement_ledger.postings
            ],
            "ledger_content_digest": _settlement_ledger_content_digest(
                settlement_ledger
            ),
        }
        if (
            settlement_through_cursor != scoped_replay_cohort.terminal_cursor
            or actual_breaker_refresh is None
            or actual_breaker_refresh.through_execution_cursor
            <= (scoped_replay_cohort.terminal_cursor or 0)
        ):
            raise RiskBlock("PORTFOLIO_REPLAY_COHORT_MISMATCH")
    entries_this_session = sum(
        position.lots[0].at.astimezone(_ET).date() == request.session_date
        for position in snapshot.open_positions
    )
    state = PortfolioState(
        settled_cash=settled_cash,
        deployed=snapshot.deployed_capital,
        open_risk=snapshot.open_planned_risk,
        open_position_count=len(snapshot.open_positions),
        entries_this_session=entries_this_session,
        open_symbols=tuple(
            sorted({position.symbol for position in snapshot.open_positions})
        ),
        settlement_verified=True,
        reconciliation_required=(
            snapshot.reconciliation_required if ledger_name == "ACTUAL" else False
        ),
        stop_unverified=(
            snapshot.stop_unverified if ledger_name == "ACTUAL" else False
        ),
        breaker_states=(breaker_state,),
        calendar_resolver=calendar_resolver,
    )
    from .ledger import _ledger_event_content_digest

    projection_through_cursor = max(
        (
            event.cursor
            for event in scoped_events
            if event.cursor is not None
        ),
        default=None,
    )
    projection_payload = {
        "version": 2,
        "ledger_name": ledger_name,
        "as_of": as_of.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "signal_ids": [signal.signal_id for signal in ledger_pair.signals],
        "event_digests": [
            _ledger_event_content_digest(event) for event in scoped_events
        ],
        "events_applied": snapshot.events_applied,
        "replay_cohort": {
            "expected_count": scoped_replay_cohort.expected_count,
            "start_cursor": scoped_replay_cohort.start_cursor,
            "terminal_cursor": scoped_replay_cohort.terminal_cursor,
            "query_cutoff": scoped_replay_cohort.query_cutoff.astimezone(
                UTC
            ).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "source_digest": scoped_replay_cohort.source_digest,
        },
    }
    projection_digest = sha256(
        json.dumps(
            projection_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    authority = PortfolioRiskAuthority(
        request=request,
        portfolio_state=state,
        scope=scope,
        as_of=as_of,
        ledger_name=ledger_name,
        projection_through_cursor=projection_through_cursor,
        settlement_through_cursor=settlement_through_cursor,
        projection_digest=projection_digest,
        settlement_source=settlement_source,
        settlement_digest=sha256(
            json.dumps(
                settlement_payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
        policy_digest=_policy_digest(policy),
        calendar_digest=resolver_digest,
        breaker_refresh_digest=(
            breaker_state.history_digest
            if isinstance(breaker_state, BreakerState)
            else actual_breaker_refresh.source_digest
        ),
        breaker_refresh_through_execution_cursor=(
            None
            if actual_breaker_refresh is None
            else actual_breaker_refresh.through_execution_cursor
        ),
        breaker_refresh_through_close_cursor=(
            None
            if actual_breaker_refresh is None
            else actual_breaker_refresh.through_close_cursor
        ),
    )
    if (
        phase1_canonical_replay is not None
        and not _is_current_phase1_canonical_ledger_replay_authority(
            phase1_canonical_replay,
            replay_source,
        )
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_UNVERIFIED")
    _install_risk_authority(
        _PORTFOLIO_RISK_AUTHORITIES,
        authority,
        exact_type=PortfolioRiskAuthority,
        phase1_bindings=phase1_source_bindings,
    )
    return authority


@dataclass(frozen=True, slots=True, weakref_slot=True)
class LongPlanDecision:
    """Auditable outcome from context-rich entry-capacity evaluation."""

    eligible: bool
    reason_codes: tuple[str, ...]
    plan: PositionPlan | None
    target: Decimal | None
    request: LongPlanRequest | None = None
    authority_scope: str | None = None
    authority_digest: str | None = None
    as_of: datetime | None = None
    portfolio_authority: PortfolioRiskAuthority | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_PLAN_DECISION"),
        )
        if type(self.eligible) is not bool:
            raise RiskBlock("INVALID_PLAN_DECISION")
        if self.eligible != (self.plan is not None and self.target is not None):
            raise RiskBlock("INVALID_PLAN_DECISION")
        if self.eligible == bool(self.reason_codes):
            raise RiskBlock("INVALID_PLAN_DECISION")
        if self.eligible and not isinstance(self.request, LongPlanRequest):
            raise RiskBlock("INVALID_PLAN_DECISION")
        if self.request is not None and not isinstance(
            self.request,
            LongPlanRequest,
        ):
            raise RiskBlock("INVALID_PLAN_DECISION")
        lineage = (self.authority_scope, self.authority_digest, self.as_of)
        if any(value is not None for value in lineage):
            if self.authority_scope not in {
                "CANONICAL_PUBLICATION",
                "ACTUAL_ENTRY",
            }:
                raise RiskBlock("INVALID_PLAN_DECISION_AUTHORITY")
            if (
                type(self.authority_digest) is not str
                or len(self.authority_digest) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in self.authority_digest
                )
            ):
                raise RiskBlock("INVALID_PLAN_DECISION_AUTHORITY")
            _require_aware(self.as_of, "INVALID_PLAN_DECISION_AUTHORITY")
            if self.portfolio_authority is not None and not isinstance(
                self.portfolio_authority, PortfolioRiskAuthority
            ):
                raise RiskBlock("INVALID_PLAN_DECISION_AUTHORITY")
        elif self.portfolio_authority is not None:
            raise RiskBlock("INVALID_PLAN_DECISION_AUTHORITY")
        if self.target is not None:
            object.__setattr__(
                self,
                "target",
                _require_money(
                    self.target,
                    reason_code="INVALID_PLAN_TARGET",
                    positive=True,
                ),
            )


def _long_plan_fingerprint(
    decision: LongPlanDecision,
) -> tuple[object, ...]:
    return (
        decision.eligible,
        decision.reason_codes,
        decision.plan,
        decision.target,
        decision.request,
        decision.authority_scope,
        decision.authority_digest,
        decision.as_of,
        (
            None
            if decision.portfolio_authority is None
            else _portfolio_authority_digest(decision.portfolio_authority)
        ),
    )


def _portfolio_authority_digest(
    authority: PortfolioRiskAuthority,
) -> str:
    """Return the canonical digest of every scalar authority coordinate."""
    request = authority.request
    state = authority.portfolio_state

    def breaker_payload(
        breaker: BreakerState | PairedBreakerState,
    ) -> dict[str, object]:
        if isinstance(breaker, PairedBreakerState):
            return {
                "kind": "PAIRED",
                "as_of": breaker.as_of.isoformat(),
                "live_entries_paused": breaker.live_entries_paused,
                "reason_codes": list(breaker.reason_codes),
                "canonical_observations_continue": (
                    breaker.canonical_observations_continue
                ),
                "canonical": breaker_payload(breaker.canonical),
                "actual": breaker_payload(breaker.actual),
            }
        return {
            "kind": "SINGLE",
            "as_of": None if breaker.as_of is None else breaker.as_of.isoformat(),
            "live_entries_paused": breaker.live_entries_paused,
            "reason_codes": list(breaker.reason_codes),
            "consecutive_losses": breaker.consecutive_losses,
            "loss_trigger_session": (
                None
                if breaker.loss_trigger_session is None
                else breaker.loss_trigger_session.isoformat()
            ),
            "loss_pause_through": (
                None
                if breaker.loss_pause_through is None
                else breaker.loss_pause_through.isoformat()
            ),
            "loss_resume_session": (
                None
                if breaker.loss_resume_session is None
                else breaker.loss_resume_session.isoformat()
            ),
            "weekly_high_water_micros": (
                None
                if breaker.weekly_high_water is None
                else money_to_micros(breaker.weekly_high_water)
            ),
            "weekly_drawdown_micros": (
                None
                if breaker.weekly_drawdown is None
                else money_to_micros(breaker.weekly_drawdown)
            ),
            "weekly_pause_through": (
                None
                if breaker.weekly_pause_through is None
                else breaker.weekly_pause_through.isoformat()
            ),
            "monthly_high_water_micros": (
                None
                if breaker.monthly_high_water is None
                else money_to_micros(breaker.monthly_high_water)
            ),
            "monthly_drawdown_micros": (
                None
                if breaker.monthly_drawdown is None
                else money_to_micros(breaker.monthly_drawdown)
            ),
            "monthly_pause_through": (
                None
                if breaker.monthly_pause_through is None
                else breaker.monthly_pause_through.isoformat()
            ),
            "canonical_observations_continue": (
                breaker.canonical_observations_continue
            ),
            "ledger_name": breaker.ledger_name,
            "history_digest": breaker.history_digest,
            "calendar_digest": breaker.calendar_digest,
        }

    payload = {
        "version": 3,
        "request": {
            "symbol": request.symbol,
            "session_date": request.session_date.isoformat(),
            "entry_micros": money_to_micros(request.entry),
            "stop_micros": money_to_micros(request.stop),
            "tick_micros": money_to_micros(request.tick_size),
            "published_target_micros": (
                None
                if request.published_target is None
                else money_to_micros(request.published_target)
            ),
        },
        "portfolio_state": {
            "settled_cash_micros": (
                None
                if state.settled_cash is None
                else money_to_micros(state.settled_cash)
            ),
            "deployed_micros": money_to_micros(state.deployed),
            "open_risk_micros": money_to_micros(state.open_risk),
            "open_position_count": state.open_position_count,
            "entries_this_session": state.entries_this_session,
            "open_symbols": list(state.open_symbols),
            "settlement_verified": state.settlement_verified,
            "reconciliation_required": state.reconciliation_required,
            "stop_unverified": state.stop_unverified,
            "breaker_states": [
                breaker_payload(breaker) for breaker in state.breaker_states
            ],
            "calendar_digest": (
                None
                if state.calendar_resolver is None
                else _calendar_digest(state.calendar_resolver)
            ),
        },
        "scope": authority.scope,
        "ledger_name": authority.ledger_name,
        "as_of": authority.as_of.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "projection_through_cursor": authority.projection_through_cursor,
        "settlement_through_cursor": authority.settlement_through_cursor,
        "projection_digest": authority.projection_digest,
        "settlement_source": authority.settlement_source,
        "settlement_digest": authority.settlement_digest,
        "policy_digest": authority.policy_digest,
        "calendar_digest": authority.calendar_digest,
        "breaker_refresh_digest": authority.breaker_refresh_digest,
        "breaker_refresh_through_execution_cursor": (
            authority.breaker_refresh_through_execution_cursor
        ),
        "breaker_refresh_through_close_cursor": (
            authority.breaker_refresh_through_close_cursor
        ),
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _issue_long_plan_decision(
    decision: LongPlanDecision,
    authority: PortfolioRiskAuthority,
    policy: Policy,
) -> LongPlanDecision:
    if (
        type(decision) is not LongPlanDecision
        or type(authority) is not PortfolioRiskAuthority
        or not is_issued_portfolio_risk_authority(authority)
        or decision.portfolio_authority is not authority
        or decision.authority_scope != authority.scope
        or decision.as_of != authority.as_of
        or decision.request is not authority.request
    ):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    if type(policy) is not Policy:
        raise TypeError("long plan policy must be a Policy")
    policy.validate()
    if authority.policy_digest != _policy_digest(policy):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")

    # Exhaust accepted calendar callbacks before reading any capacity scalar
    # used to authorize the decision.  The canonical digest deliberately runs
    # first because it includes calendar authority material after state fields.
    authority_digest = _portfolio_authority_digest(authority)
    calendar_resolver = authority.portfolio_state.calendar_resolver
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    try:
        calendar_resolver.is_open(authority.request.session_date)
    except RiskBlock as error:
        if error.reason_code != "CALENDAR_COVERAGE_MISSING":
            raise
    try:
        calendar_resolver.previous_session(authority.request.session_date)
    except RiskBlock:
        pass
    if not _is_current_portfolio_risk_authority_without_callbacks(authority):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")

    calendars = object.__getattribute__(calendar_resolver, "calendars")
    if type(calendars) is not tuple or any(
        type(calendar) is not MarketCalendar for calendar in calendars
    ):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    diagnostic_calendar = SessionCalendarResolver.for_diagnostics(calendars)
    diagnostic_state = replace(
        authority.portfolio_state,
        calendar_resolver=diagnostic_calendar,
    )
    expected = plan_long_diagnostic(
        authority.request,
        diagnostic_state,
        policy,
    )
    expected = replace(
        expected,
        authority_scope=authority.scope,
        authority_digest=authority_digest,
        as_of=authority.as_of,
        portfolio_authority=authority,
    )
    from . import journal as journal_module

    if not journal_module._source_fingerprint_seals_equal(
        _risk_authority_seal(decision, exact_type=LongPlanDecision),
        _risk_authority_seal(expected, exact_type=LongPlanDecision),
    ):
        raise RiskBlock("PLAN_DECISION_CONTENT_MISMATCH")
    if not _is_current_portfolio_risk_authority_without_callbacks(authority):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    _install_risk_authority(
        _LONG_PLAN_AUTHORITIES,
        decision,
        exact_type=LongPlanDecision,
        children=(authority,),
    )
    return decision


def is_issued_long_plan_decision(decision: object) -> bool:
    if type(decision) is not LongPlanDecision:
        return False
    authority = decision.portfolio_authority
    if (
        type(authority) is not PortfolioRiskAuthority
        or not is_issued_portfolio_risk_authority(authority)
        or not _is_current_portfolio_risk_authority_without_callbacks(
            authority
        )
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _LONG_PLAN_AUTHORITIES,
        decision,
        exact_type=LongPlanDecision,
        children=(authority,),
    )


def _plan_decision(
    request: LongPlanRequest,
    *,
    eligible: bool,
    reason_codes: tuple[str, ...],
    plan: PositionPlan | None,
    target: Decimal | None,
    issue: bool = False,
    authority: PortfolioRiskAuthority | None = None,
    policy: Policy | None = None,
) -> LongPlanDecision:
    if issue and not is_issued_portfolio_risk_authority(authority):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    authority_digest = None
    authority_scope = None
    as_of = None
    decision_authority = None
    if issue:
        assert authority is not None
        authority_scope = authority.scope
        as_of = authority.as_of
        authority_digest = _portfolio_authority_digest(authority)
        decision_authority = authority
    decision = LongPlanDecision(
        eligible,
        reason_codes,
        plan,
        target,
        request,
        authority_scope,
        authority_digest,
        as_of,
        decision_authority,
    )
    if issue:
        assert authority is not None
        if policy is None:
            raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
        return _issue_long_plan_decision(decision, authority, policy)
    return decision


def size_long(
    entry: Decimal,
    stop: Decimal,
    settled_cash: Decimal,
    deployed: Decimal,
    open_risk: Decimal,
) -> PositionPlan:
    """Size one long with fixed validation caps and no ambient authority."""
    entry = _require_money(entry, reason_code="INVALID_ENTRY", positive=True)
    stop = _require_money(stop, reason_code="INVALID_STOP", positive=True)
    settled_cash = _require_money(
        settled_cash,
        reason_code="INVALID_SETTLED_CASH",
        nonnegative=True,
    )
    deployed = _require_money(
        deployed,
        reason_code="INVALID_DEPLOYED_CAPITAL",
        nonnegative=True,
    )
    open_risk = _require_money(
        open_risk,
        reason_code="INVALID_OPEN_RISK",
        nonnegative=True,
    )
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(
            entry,
            stop,
            settled_cash,
            deployed,
            open_risk,
            _MAX_LIVE_EXPOSURE,
            _MAX_POSITION_RISK,
            _MAX_COMBINED_RISK,
        )
        distance = entry - stop
        if distance <= _ZERO:
            raise RiskBlock("NON_POSITIVE_STOP_DISTANCE")
        if deployed >= _MAX_LIVE_EXPOSURE:
            raise RiskBlock("EXPOSURE_CAP_REACHED")
        if open_risk >= _MAX_COMBINED_RISK:
            raise RiskBlock("COMBINED_RISK_CAP_REACHED")
        remaining_exposure = min(
            settled_cash,
            _MAX_LIVE_EXPOSURE - deployed,
        )
        remaining_risk = min(
            _MAX_POSITION_RISK,
            _MAX_COMBINED_RISK - open_risk,
        )
        quantity = int(
            min(
                remaining_exposure / entry,
                remaining_risk / distance,
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
        if quantity < 1:
            raise RiskBlock("QUANTITY_BELOW_ONE")
        exposure = entry * quantity
        planned_risk = distance * quantity
    return PositionPlan(
        quantity=quantity,
        exposure=exposure,
        planned_risk=planned_risk,
    )


def plan_long(
    request: LongPlanRequest,
    portfolio_state: PortfolioState,
    policy: Policy,
    *,
    portfolio_authority: PortfolioRiskAuthority | None = None,
) -> LongPlanDecision:
    """Apply production capacity gates and require complete source authority."""
    return _plan_long(
        request,
        portfolio_state,
        policy,
        portfolio_authority=portfolio_authority,
        require_authority=True,
    )


def plan_long_diagnostic(
    request: LongPlanRequest,
    portfolio_state: PortfolioState,
    policy: Policy,
) -> LongPlanDecision:
    """Evaluate the pure context formula without issuing entry authority."""
    return _plan_long(
        request,
        portfolio_state,
        policy,
        portfolio_authority=None,
        require_authority=False,
    )


def _plan_long(
    request: LongPlanRequest,
    portfolio_state: PortfolioState,
    policy: Policy,
    *,
    portfolio_authority: PortfolioRiskAuthority | None,
    require_authority: bool,
) -> LongPlanDecision:
    if not isinstance(request, LongPlanRequest):
        raise TypeError("long plan request must be a LongPlanRequest")
    if not isinstance(portfolio_state, PortfolioState):
        raise TypeError("portfolio state must be a PortfolioState")
    if not isinstance(policy, Policy):
        raise TypeError("long plan policy must be a Policy")
    policy.validate()

    reasons: list[str] = []
    authority_verified = (
        portfolio_authority is not None
        and is_issued_portfolio_risk_authority(portfolio_authority)
        and portfolio_authority.request is request
        and portfolio_authority.portfolio_state is portfolio_state
        and portfolio_authority.policy_digest == _policy_digest(policy)
        and portfolio_state.calendar_resolver is not None
        and portfolio_authority.calendar_digest
        == _calendar_digest(portfolio_state.calendar_resolver)
    )
    if require_authority and not authority_verified:
        reasons.append("PORTFOLIO_AUTHORITY_UNVERIFIED")
    if not _is_tick_aligned(request.entry, request.tick_size):
        reasons.append("ENTRY_NOT_TICK_ALIGNED")
    if not _is_tick_aligned(request.stop, request.tick_size):
        reasons.append("STOP_NOT_TICK_ALIGNED")
    if portfolio_state.settled_cash is None:
        reasons.append("MISSING_SETTLEMENT_STATE")
    if not portfolio_state.settlement_verified:
        reasons.append("SETTLEMENT_AUTHORITY_UNVERIFIED")
    if portfolio_state.reconciliation_required:
        reasons.append("RECONCILIATION_REQUIRED")
    if portfolio_state.stop_unverified:
        reasons.append("STOP_UNVERIFIED")
    if portfolio_state.open_position_count >= policy.max_positions:
        reasons.append("POSITION_LIMIT_REACHED")
    if (
        request.symbol is not None
        and request.symbol in portfolio_state.open_symbols
    ):
        reasons.append("DUPLICATE_TICKER_EXPOSURE")
    if portfolio_state.entries_this_session >= policy.max_entries_per_session:
        reasons.append("SESSION_ENTRY_LIMIT_REACHED")
    if portfolio_state.deployed >= policy.max_live_exposure:
        reasons.append("EXPOSURE_CAP_REACHED")
    if portfolio_state.open_risk >= policy.max_combined_risk:
        reasons.append("COMBINED_RISK_CAP_REACHED")
    if (
        len(portfolio_state.breaker_states) != 1
        or portfolio_state.calendar_resolver is None
    ):
        reasons.append("BREAKER_AUTHORITY_UNVERIFIED")
    else:
        breaker_state = portfolio_state.breaker_states[0]
        expected_scope = (
            portfolio_authority.scope
            if require_authority
            and authority_verified
            and portfolio_authority is not None
            else None
        )
        if expected_scope == "CANONICAL_PUBLICATION":
            breaker_verified = (
                isinstance(breaker_state, BreakerState)
                and is_issued_breaker_state(breaker_state)
                and breaker_state.ledger_name == "CANONICAL"
            )
        elif expected_scope == "ACTUAL_ENTRY":
            breaker_verified = (
                isinstance(breaker_state, PairedBreakerState)
                and is_issued_paired_breaker_state(breaker_state)
            )
        elif not require_authority and isinstance(breaker_state, BreakerState):
            breaker_verified = True
        elif not require_authority and isinstance(
            breaker_state,
            PairedBreakerState,
        ):
            breaker_verified = True
        else:
            breaker_verified = is_issued_paired_breaker_state(breaker_state)
        if not breaker_verified:
            reasons.append("BREAKER_AUTHORITY_UNVERIFIED")
        try:
            entry_session_open = portfolio_state.calendar_resolver.is_open(
                request.session_date
            )
        except RiskBlock as error:
            if error.reason_code == "CALENDAR_COVERAGE_MISSING":
                reasons.append("CALENDAR_COVERAGE_MISSING")
                entry_session_open = False
            else:
                raise
        if not entry_session_open and "CALENDAR_COVERAGE_MISSING" not in reasons:
            reasons.append("ENTRY_SESSION_CLOSED")
        try:
            required_as_of = portfolio_state.calendar_resolver.previous_session(
                request.session_date
            )
        except RiskBlock:
            reasons.append("BREAKER_AUTHORITY_UNVERIFIED")
        else:
            if breaker_state.as_of != required_as_of:
                reasons.append("BREAKER_AS_OF_MISMATCH")
            if breaker_pauses_entry(breaker_state, request.session_date):
                reasons.append("ACTIVE_CIRCUIT_BREAKER")
    if reasons:
        return _plan_decision(
            request,
            eligible=False,
            reason_codes=tuple(reasons),
            plan=None,
            target=None,
            issue=require_authority and authority_verified,
            authority=(
                portfolio_authority
                if require_authority and authority_verified
                else None
            ),
            policy=policy,
        )

    assert portfolio_state.settled_cash is not None
    try:
        plan = size_long(
            request.entry,
            request.stop,
            portfolio_state.settled_cash,
            portfolio_state.deployed,
            portfolio_state.open_risk,
        )
    except RiskBlock as error:
        return _plan_decision(
            request,
            eligible=False,
            reason_codes=(error.reason_code,),
            plan=None,
            target=None,
            issue=require_authority and authority_verified,
            authority=(
                portfolio_authority
                if require_authority and authority_verified
                else None
            ),
            policy=policy,
        )
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(
            request.entry,
            request.stop,
            request.tick_size,
        )
        target = _round_up_to_tick(
            request.entry + _TWO * (request.entry - request.stop),
            request.tick_size,
        )
    if request.published_target is not None:
        if request.published_target != target:
            return _plan_decision(
                request,
                eligible=False,
                reason_codes=("TASK5_PRICE_CONTRACT_MISMATCH",),
                plan=None,
                target=None,
                issue=require_authority and authority_verified,
                authority=(
                    portfolio_authority
                    if require_authority and authority_verified
                    else None
                ),
                policy=policy,
            )
        target = request.published_target
    return _plan_decision(
        request,
        eligible=True,
        reason_codes=(),
        plan=plan,
        target=target,
        issue=require_authority and authority_verified,
        authority=(
            portfolio_authority
            if require_authority and authority_verified
            else None
        ),
        policy=policy,
    )


@dataclass(frozen=True, slots=True)
class AccountCheck:
    """User-confirmed account-wide settled buying-power evidence."""

    settled_cash: Decimal
    pending_orders: int
    unlogged_positions: int
    at: datetime
    reconciliation_result: str = "CLEAR"
    cursor: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "settled_cash",
            _require_money(
                self.settled_cash,
                reason_code="INVALID_ACCOUNT_SETTLED_CASH",
                nonnegative=True,
            ),
        )
        _require_nonnegative_int(self.pending_orders, "INVALID_PENDING_ORDER_COUNT")
        _require_nonnegative_int(
            self.unlogged_positions,
            "INVALID_UNLOGGED_POSITION_COUNT",
        )
        _require_aware(self.at, "INVALID_ACCOUNT_CHECK_TIME")
        if self.reconciliation_result not in {
            "CLEAR",
            "RECONCILIATION_REQUIRED",
        }:
            raise RiskBlock("INVALID_ACCOUNT_RECONCILIATION_RESULT")
        if self.cursor is not None:
            _require_positive_int(self.cursor, "INVALID_ACCOUNT_CHECK_CURSOR")


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """One immutable account event relevant to settlement eligibility."""

    kind: str
    at: datetime
    price: Decimal | None = None
    shares: int | None = None
    amount: Decimal | None = None
    cursor: int | None = None
    message_time: datetime | None = None
    received_at: datetime | None = None
    parent_order_id: str | None = None
    fill_group_planned_shares: int | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not str or not self.kind or self.kind != self.kind.upper():
            raise RiskBlock("INVALID_EXECUTION_EVENT_KIND")
        _require_aware(self.at, "INVALID_EXECUTION_EVENT_TIME")
        if self.price is not None:
            object.__setattr__(
                self,
                "price",
                _require_money(
                    self.price,
                    reason_code="INVALID_EXECUTION_PRICE",
                    positive=True,
                ),
            )
        if self.shares is not None:
            _require_positive_int(self.shares, "INVALID_EXECUTION_SHARES")
        if self.amount is not None:
            object.__setattr__(
                self,
                "amount",
                _require_money(
                    self.amount,
                    reason_code="INVALID_EXECUTION_AMOUNT",
                ),
            )
        if self.cursor is not None:
            _require_positive_int(self.cursor, "INVALID_EXECUTION_CURSOR")
        if (self.message_time is None) != (self.received_at is None):
            raise RiskBlock("EXECUTION_SOURCE_TIME_INCOMPLETE")
        if self.message_time is not None and self.received_at is not None:
            message_time = _require_aware(
                self.message_time,
                "INVALID_EXECUTION_MESSAGE_TIME",
            )
            received_at = _require_aware(
                self.received_at,
                "INVALID_EXECUTION_RECEIVED_TIME",
            )
            if not self.at <= message_time <= received_at:
                raise RiskBlock("EXECUTION_SOURCE_TIME_OUT_OF_ORDER")
        if self.kind in {"BUY", "BOUGHT", "PARTIAL_FILL"} and (
            self.price is None or self.shares is None
        ):
            raise RiskBlock("INCOMPLETE_BUY_EVENT")
        if (self.parent_order_id is None) != (
            self.fill_group_planned_shares is None
        ):
            raise RiskBlock("EXECUTION_FILL_GROUP_INCOMPLETE")
        if self.parent_order_id is not None:
            if type(self.parent_order_id) is not str or not self.parent_order_id:
                raise RiskBlock("INVALID_EXECUTION_PARENT_ORDER")
            _require_positive_int(
                self.fill_group_planned_shares,
                "INVALID_EXECUTION_FILL_GROUP_SHARES",
            )
            if self.shares is None or self.fill_group_planned_shares < self.shares:
                raise RiskBlock("INVALID_EXECUTION_FILL_GROUP_SHARES")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ConfirmedBuyAction:
    """Exact persisted BOUGHT action issued by the Task 7 Journal adapter.

    BID, ASK, and STOP are fields of the same grammar action as the execution;
    they are deliberately not modeled as an invented event in another cursor
    stream.
    """

    event_id: str
    idempotency_key: str
    message_id: str
    action_ordinal: int
    cursor: int
    symbol: str
    shares: int
    price: Decimal
    at: datetime
    message_time: datetime
    received_at: datetime
    bid: Decimal
    ask: Decimal
    user_confirmed_stop: Decimal | None
    source: str
    raw_sha256: str
    details_sha256: str
    parent_order_id: str | None = None
    fill_group_planned_shares: int | None = None

    def __post_init__(self) -> None:
        if type(self.message_id) is not str or not self.message_id:
            raise RiskBlock("INVALID_CONFIRMATION_MESSAGE_ID")
        _require_nonnegative_int(
            self.action_ordinal,
            "INVALID_CONFIRMATION_ACTION_ORDINAL",
        )
        expected_event_id, expected_idempotency_key = (
            stable_execution_event_identity(
                self.message_id,
                self.action_ordinal,
            )
        )
        if self.event_id != expected_event_id:
            raise RiskBlock("INVALID_CONFIRMATION_EVENT_ID")
        if self.idempotency_key != expected_idempotency_key:
            raise RiskBlock("INVALID_CONFIRMATION_IDEMPOTENCY_KEY")
        _require_positive_int(self.cursor, "INVALID_CONFIRMATION_CURSOR")
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise RiskBlock("INVALID_CONFIRMATION_SYMBOL")
        _require_positive_int(self.shares, "INVALID_EXECUTION_SHARES")
        object.__setattr__(
            self,
            "price",
            _require_money(
                self.price,
                reason_code="INVALID_EXECUTION_PRICE",
                positive=True,
            ),
        )
        event_at = _require_aware(self.at, "INVALID_EXECUTION_EVENT_TIME")
        message_time = _require_aware(
            self.message_time,
            "INVALID_CONFIRMATION_MESSAGE_TIME",
        )
        received_at = _require_aware(
            self.received_at,
            "INVALID_CONFIRMATION_RECEIVED_TIME",
        )
        if not event_at <= message_time <= received_at:
            raise RiskBlock("CONFIRMATION_SOURCE_TIME_OUT_OF_ORDER")
        object.__setattr__(
            self,
            "bid",
            _require_money(
                self.bid,
                reason_code="INVALID_CONFIRMED_BID",
                positive=True,
            ),
        )
        object.__setattr__(
            self,
            "ask",
            _require_money(
                self.ask,
                reason_code="INVALID_CONFIRMED_ASK",
                positive=True,
            ),
        )
        if self.ask < self.bid:
            raise RiskBlock("INVALID_CONFIRMED_SPREAD")
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _require_money(
                    self.user_confirmed_stop,
                    reason_code="INVALID_USER_CONFIRMED_STOP",
                    positive=True,
                ),
            )
        if self.source != "ROBINHOOD_MANUAL_CONFIRMATION":
            raise RiskBlock("INVALID_CONFIRMATION_SOURCE")
        if (self.parent_order_id is None) != (
            self.fill_group_planned_shares is None
        ):
            raise RiskBlock("CONFIRMATION_FILL_GROUP_INCOMPLETE")
        if self.parent_order_id is not None:
            if type(self.parent_order_id) is not str or not self.parent_order_id:
                raise RiskBlock("INVALID_CONFIRMATION_PARENT_ORDER")
            _require_positive_int(
                self.fill_group_planned_shares,
                "INVALID_CONFIRMATION_FILL_GROUP_SHARES",
            )
            if self.fill_group_planned_shares < self.shares:
                raise RiskBlock("INVALID_CONFIRMATION_FILL_GROUP_SHARES")
        for digest in (self.raw_sha256, self.details_sha256):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise RiskBlock("INVALID_CONFIRMATION_DIGEST")

    @property
    def execution_event(self) -> ExecutionEvent:
        return ExecutionEvent(
            kind="BUY",
            at=self.at,
            price=self.price,
            shares=self.shares,
            cursor=self.cursor,
            message_time=self.message_time,
            received_at=self.received_at,
            parent_order_id=self.parent_order_id,
            fill_group_planned_shares=self.fill_group_planned_shares,
        )


def _confirmed_buy_fingerprint(action: ConfirmedBuyAction) -> tuple[object, ...]:
    return (
        action.event_id,
        action.idempotency_key,
        action.message_id,
        action.action_ordinal,
        action.cursor,
        action.symbol,
        action.shares,
        action.price,
        action.at,
        action.message_time,
        action.received_at,
        action.bid,
        action.ask,
        action.user_confirmed_stop,
        action.source,
        action.raw_sha256,
        action.details_sha256,
        action.parent_order_id,
        action.fill_group_planned_shares,
    )


def _issue_confirmed_buy_action(**fields: object) -> ConfirmedBuyAction:
    """Validate diagnostic action fields without granting Journal authority."""
    if fields.get("message_time") is None or fields.get("received_at") is None:
        raise RiskBlock("CONFIRMATION_SOURCE_TIME_INCOMPLETE")
    return ConfirmedBuyAction(**fields)  # type: ignore[arg-type]


def is_issued_confirmed_buy_action(action: object) -> bool:
    if type(action) is not ConfirmedBuyAction:
        return False
    return (
        _journal_derived_source_is_current(action)
        and _journal_derived_source_is_current_without_callbacks(action)
        and _is_current_risk_authority_without_callbacks(
            _CONFIRMED_BUY_AUTHORITIES,
            action,
            exact_type=ConfirmedBuyAction,
        )
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class JournalEventWindow:
    """Typed evidence that a Journal cursor interval was read completely."""

    after_cursor: int
    through_cursor: int
    events: tuple[ExecutionEvent, ...]
    complete: bool
    source: str
    account_check: AccountCheck | None = None
    terminal_action: ConfirmedBuyAction | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.after_cursor, "INVALID_EVENT_WINDOW_CURSOR")
        _require_positive_int(self.through_cursor, "INVALID_EVENT_WINDOW_CURSOR")
        if self.through_cursor <= self.after_cursor:
            raise RiskBlock("INVALID_EVENT_WINDOW_CURSOR")
        if isinstance(self.events, (str, bytes)):
            raise RiskBlock("INVALID_EVENT_WINDOW")
        events = tuple(self.events)
        if any(not isinstance(event, ExecutionEvent) for event in events):
            raise RiskBlock("INVALID_EVENT_WINDOW")
        object.__setattr__(self, "events", events)
        if type(self.complete) is not bool:
            raise RiskBlock("INVALID_EVENT_WINDOW")
        if type(self.source) is not str or not self.source:
            raise RiskBlock("INVALID_EVENT_WINDOW")
        if self.account_check is not None and not isinstance(
            self.account_check,
            AccountCheck,
        ):
            raise RiskBlock("INVALID_EVENT_WINDOW_ENDPOINT")
        if self.terminal_action is not None and not isinstance(
            self.terminal_action,
            ConfirmedBuyAction,
        ):
            raise RiskBlock("INVALID_EVENT_WINDOW_ENDPOINT")

    @property
    def terminal_event(self) -> ExecutionEvent | None:
        if self.terminal_action is None:
            return None
        return self.terminal_action.execution_event


def _authority_fingerprint(
    window: JournalEventWindow,
) -> tuple[object, ...]:
    account_check = window.account_check
    terminal_action = window.terminal_action
    return (
        window.after_cursor,
        window.through_cursor,
        tuple(
            (
                event.kind,
                event.at,
                event.price,
                event.shares,
                event.amount,
                event.cursor,
                event.message_time,
                event.received_at,
                event.parent_order_id,
                event.fill_group_planned_shares,
            )
            for event in window.events
        ),
        window.complete,
        window.source,
        (
            None
            if account_check is None
            else (
                account_check.settled_cash,
                account_check.pending_orders,
                account_check.unlogged_positions,
                account_check.at,
                account_check.reconciliation_result,
                account_check.cursor,
            )
        ),
        (
            None
            if terminal_action is None
            else _confirmed_buy_fingerprint(terminal_action)
        ),
    )


def _register_identity_authority(
    registry: dict[int, tuple[ReferenceType[object], object]],
    value: object,
    fingerprint: object,
) -> None:
    del registry, value, fingerprint
    raise RiskBlock("RISK_AUTHORITY_REGISTRAR_UNAVAILABLE")


def _has_identity_authority(
    registry: dict[int, tuple[ReferenceType[object], object]],
    value: object,
    fingerprint: object,
) -> bool:
    from .journal import _fingerprints_equal

    with _AUTHORITY_LOCK:
        registered = registry.get(id(value))
        return (
            registered is not None
            and registered[0]() is value
            and _fingerprints_equal(registered[1], fingerprint)
        )


def _risk_authority_seal(
    value: object,
    *,
    exact_type: type[object],
) -> object:
    """Build one hook-free exact structural seal for a risk authority."""
    if type(value) is not exact_type:
        raise TypeError("risk authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        value,
        domain=(
            b"stock-monitor/risk-authority/v1\x00"
            + exact_type.__name__.encode("ascii")
        ),
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _register_risk_authority(
    registry: dict[int, tuple[ReferenceType[object], object]],
    value: object,
    *,
    exact_type: type[object],
    children: Sequence[object] = (),
) -> None:
    del registry, value, exact_type, children
    raise RiskBlock("RISK_AUTHORITY_REGISTRAR_UNAVAILABLE")


def _is_current_risk_authority_without_callbacks(
    registry: dict[int, tuple[ReferenceType[object], object]],
    value: object,
    *,
    exact_type: type[object],
    children: Sequence[object] = (),
) -> bool:
    """Pure final root, child, provenance, and registry-record check."""
    if type(value) is not exact_type:
        return False
    expected_children = tuple(children)
    identity = id(value)
    with _AUTHORITY_LOCK:
        registered = registry.get(identity)
        if (
            registered is None
            or registered[0]() is not value
            or type(registered[1]) is not _RiskAuthorityRecord
        ):
            return False
        record = registered[1]
        assert isinstance(record, _RiskAuthorityRecord)
        if len(record.children) != len(expected_children) or any(
            stored is not expected
            for stored, expected in zip(
                record.children,
                expected_children,
                strict=True,
            )
        ):
            return False
        journal_binding = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(identity)
        phase1_binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(identity)
        if record.journal_binding is None:
            if journal_binding is not None:
                return False
        else:
            source, source_kind, _owner = record.journal_binding
            if (
                journal_binding is None
                or journal_binding[0]() is not value
                or journal_binding[1]() is not source
                or journal_binding[2] != source_kind
            ):
                return False
        if not record.phase1_bindings:
            if phase1_binding is not None:
                return False
        else:
            if (
                phase1_binding is None
                or phase1_binding[0]() is not value
                or len(phase1_binding[1]) != len(record.phase1_bindings)
                or any(
                    bound_source is not recorded_source
                    or bound_kind != recorded_kind
                    for (bound_source, bound_kind), (
                        recorded_source,
                        recorded_kind,
                        _recorded_owner,
                    ) in zip(
                        phase1_binding[1],
                        record.phase1_bindings,
                        strict=True,
                    )
                )
            ):
                return False
    if record.journal_binding is not None:
        source, source_kind, recorded_owner = record.journal_binding
        current_owner = _risk_binding_owner_without_callbacks(
            source,
            source_kind,
        )
        if current_owner is not recorded_owner:
            return False
    if any(
        _risk_binding_owner_without_callbacks(source, source_kind)
        is not recorded_owner
        for source, source_kind, recorded_owner in record.phase1_bindings
    ):
        return False
    try:
        seal = _risk_authority_seal(value, exact_type=exact_type)
    except Exception:
        return False
    from . import journal as journal_module

    with _AUTHORITY_LOCK:
        current = registry.get(identity)
        current_journal_binding = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(
            identity
        )
        current_phase1_binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(
            identity
        )
        return (
            current is registered
            and current[0]() is value
            and current[1] is record
            and current_journal_binding is journal_binding
            and current_phase1_binding is phase1_binding
            and journal_module._source_fingerprint_seals_equal(
                record.seal,
                seal,
            )
        )


def _risk_binding_owner_without_callbacks(
    source: object,
    source_kind: str,
) -> object | None:
    """Return the exact owner of one already-current provenance root."""
    if source_kind == "ISSUED_EVENT_WINDOW":
        if type(source) is not JournalEventWindow or not (
            _is_current_risk_authority_without_callbacks(
                _JOURNAL_WINDOW_AUTHORITIES,
                source,
                exact_type=JournalEventWindow,
                children=(source.terminal_action,),
            )
        ):
            return None
        with _AUTHORITY_LOCK:
            registered = _JOURNAL_WINDOW_AUTHORITIES.get(id(source))
            if (
                registered is None
                or registered[0]() is not source
                or type(registered[1]) is not _RiskAuthorityRecord
            ):
                return None
            record = registered[1]
            assert isinstance(record, _RiskAuthorityRecord)
            if record.journal_binding is None:
                return None
            return record.journal_binding[2]
    from . import journal as journal_module

    candidate = journal_module._journal_any_source_authority_candidate(source)
    if candidate is None or not (
        journal_module._is_current_journal_authority_candidate_without_callbacks(
            candidate
        )
    ):
        return None
    return candidate[2]


def _risk_authority_installer_factory(
    allowed_specs: tuple[
        tuple[
            object,
            type[object],
            frozenset[object],
        ],
        ...,
    ],
):
    """Create the only issuer-frame-gated risk authority installer."""
    get_caller_frame = _getframe
    trusted_globals = globals()

    def install(
        registry: dict[int, tuple[ReferenceType[object], object]],
        value: object,
        *,
        exact_type: type[object],
        children: tuple[object, ...] = (),
        journal_binding: tuple[object, str] | None = None,
        phase1_bindings: tuple[tuple[object, str], ...] = (),
    ) -> None:
        caller_frame = get_caller_frame(1)
        caller_code = caller_frame.f_code
        if not any(
            registry is allowed_registry
            and exact_type is allowed_type
            and caller_code in allowed_callers
            for allowed_registry, allowed_type, allowed_callers in allowed_specs
        ) or caller_frame.f_globals is not trusted_globals:
            raise RiskBlock("RISK_AUTHORITY_REGISTRAR_UNAVAILABLE")
        if type(value) is not exact_type or type(children) is not tuple:
            raise RiskBlock("RISK_AUTHORITY_REGISTRATION_INVALID")
        if journal_binding is not None and (
            type(journal_binding) is not tuple
            or len(journal_binding) != 2
            or type(journal_binding[1]) is not str
        ):
            raise RiskBlock("RISK_AUTHORITY_REGISTRATION_INVALID")
        if type(phase1_bindings) is not tuple or any(
            type(binding) is not tuple
            or len(binding) != 2
            or type(binding[1]) is not str
            for binding in phase1_bindings
        ):
            raise RiskBlock("RISK_AUTHORITY_REGISTRATION_INVALID")

        recorded_journal_binding: tuple[object, str, object] | None = None
        if journal_binding is not None:
            source, source_kind = journal_binding
            owner = _risk_binding_owner_without_callbacks(
                source,
                source_kind,
            )
            if owner is None:
                raise RiskBlock("RISK_AUTHORITY_SOURCE_UNVERIFIED")
            recorded_journal_binding = (source, source_kind, owner)

        recorded_phase1_bindings: list[tuple[object, str, object]] = []
        for source, source_kind in phase1_bindings:
            owner = _risk_binding_owner_without_callbacks(
                source,
                source_kind,
            )
            if owner is None:
                raise RiskBlock("RISK_AUTHORITY_SOURCE_UNVERIFIED")
            recorded_phase1_bindings.append((source, source_kind, owner))

        record = _RiskAuthorityRecord(
            seal=_risk_authority_seal(value, exact_type=exact_type),
            children=children,
            journal_binding=recorded_journal_binding,
            phase1_bindings=tuple(recorded_phase1_bindings),
        )
        identity = id(value)

        def discard(dead: ReferenceType[object]) -> None:
            with _AUTHORITY_LOCK:
                current = registry.get(identity)
                if current is not None and current[0] is dead:
                    registry.pop(identity, None)
                journal_current = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(
                    identity
                )
                if journal_current is not None and journal_current[0] is dead:
                    _JOURNAL_DERIVED_SOURCE_BINDINGS.pop(identity, None)
                phase1_current = _PHASE1_DERIVED_SOURCE_BINDINGS.get(identity)
                if phase1_current is not None and phase1_current[0] is dead:
                    _PHASE1_DERIVED_SOURCE_BINDINGS.pop(identity, None)

        reference = ref(value, discard)
        with _AUTHORITY_LOCK:
            if (
                registry.get(identity) is not None
                or _JOURNAL_DERIVED_SOURCE_BINDINGS.get(identity) is not None
                or _PHASE1_DERIVED_SOURCE_BINDINGS.get(identity) is not None
            ):
                raise RiskBlock("RISK_AUTHORITY_ALREADY_ISSUED")
            registry[identity] = (reference, record)
            if recorded_journal_binding is not None:
                source, source_kind, _owner = recorded_journal_binding
                _JOURNAL_DERIVED_SOURCE_BINDINGS[identity] = (
                    reference,
                    ref(source),
                    source_kind,
                )
            if recorded_phase1_bindings:
                _PHASE1_DERIVED_SOURCE_BINDINGS[identity] = (
                    reference,
                    tuple(
                        (source, source_kind)
                        for source, source_kind, _owner
                        in recorded_phase1_bindings
                    ),
                )

    return install


def _registered_risk_authority_children(
    registry: dict[int, tuple[ReferenceType[object], object]],
    value: object,
) -> tuple[object, ...] | None:
    """Return the exact immutable child manifest for one registered root."""
    with _AUTHORITY_LOCK:
        registered = registry.get(id(value))
        if (
            registered is None
            or registered[0]() is not value
            or type(registered[1]) is not _RiskAuthorityRecord
        ):
            return None
        record = registered[1]
        assert isinstance(record, _RiskAuthorityRecord)
        return record.children


def _bind_phase1_derived_sources(
    value: object,
    sources: Sequence[tuple[object, str]],
) -> None:
    del value, sources
    raise RiskBlock("RISK_AUTHORITY_BINDING_UNAVAILABLE")


def _phase1_bound_sources(value: object) -> tuple[tuple[object, str], ...]:
    with _AUTHORITY_LOCK:
        binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(id(value))
        if binding is None or binding[0]() is not value:
            return ()
        return binding[1]


def _phase1_derived_sources_are_current(value: object) -> bool:
    with _AUTHORITY_LOCK:
        binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(id(value))
        if binding is None:
            return True
        if binding[0]() is not value:
            return False
        resolved = binding[1]
    from . import journal as journal_module

    verifier_names = {
        "BREAKER_HISTORY": "is_verified_phase1_breaker_history_source",
        "CANONICAL_REPLAY": "is_verified_phase1_canonical_replay_source",
        "EQUITY_MARK": "is_verified_phase1_equity_mark_source",
        "EXIT_REVIEW": "is_verified_phase1_exit_review_source",
        "EXIT_REVIEW_MARKET": "is_verified_phase1_exit_review_market_source",
        "SIGNAL_EVIDENCE": "is_verified_phase1_signal_evidence_source",
        "SIGNAL_SOURCE": "is_verified_phase1_signal_source",
    }
    for source, kind in resolved:
        verifier = getattr(journal_module, verifier_names.get(kind, ""), None)
        if verifier is None or not verifier(source):
            return False
    return True


def _phase1_derived_sources_are_current_without_callbacks(
    value: object,
) -> bool:
    """Purely recheck all exact Phase 1 bindings after public callbacks."""
    from . import journal as journal_module

    identity = id(value)
    with _AUTHORITY_LOCK:
        binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(identity)
        if (
            binding is None
            or binding[0]() is not value
            or not binding[1]
        ):
            return False
        captured = binding
        sources = tuple(source for source, _kind in binding[1])
    if any(
        not journal_module._is_current_journal_source_authority_without_callbacks(
            source
        )
        for source in sources
    ):
        return False
    with _AUTHORITY_LOCK:
        current = _PHASE1_DERIVED_SOURCE_BINDINGS.get(identity)
        return current is captured and current[0]() is value


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1EquityPointAuthority:
    """One source-bound canonical or actual Phase 1 equity observation."""

    point: Phase1MarkedEquityPoint
    validation_window_id: str
    ledger_name: str
    session_date: date
    point_at: datetime
    query_cutoff: datetime
    replay_source_digest: str
    mark_source_digest: str
    source_digest: str
    authority_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.point, Phase1MarkedEquityPoint)
            or self.ledger_name not in {"CANONICAL", "ACTUAL"}
            or self.point.ledger_name != self.ledger_name
            or type(self.validation_window_id) is not str
            or not _is_sha256_digest(self.validation_window_id)
            or type(self.session_date) is not date
        ):
            raise RiskBlock("INVALID_PHASE1_EQUITY_POINT_AUTHORITY")
        point_at = _require_aware(
            self.point_at,
            "INVALID_PHASE1_EQUITY_POINT_AUTHORITY",
        )
        query_cutoff = _require_aware(
            self.query_cutoff,
            "INVALID_PHASE1_EQUITY_POINT_AUTHORITY",
        )
        if (
            self.point.at != point_at
            or point_at.astimezone(_ET).date() != self.session_date
            or point_at > query_cutoff
        ):
            raise RiskBlock("INVALID_PHASE1_EQUITY_POINT_AUTHORITY")
        for digest in (
            self.replay_source_digest,
            self.mark_source_digest,
            self.source_digest,
            self.authority_digest,
        ):
            if not _is_sha256_digest(digest):
                raise RiskBlock("INVALID_PHASE1_EQUITY_POINT_AUTHORITY")


def _phase1_marked_equity_point_fingerprint(
    point: Phase1MarkedEquityPoint,
) -> tuple[object, ...]:
    return (
        point.ledger_name,
        point.at,
        point.cash,
        point.positions_value,
        point.equity,
        point.external_cash_flow,
        tuple(tuple(item) for item in point.mark_sources),
    )


def _phase1_equity_point_authority_document(
    authority: Phase1EquityPointAuthority,
) -> dict[str, object]:
    point = authority.point

    def instant(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(
            timespec="microseconds",
        ).replace("+00:00", "Z")

    return {
        "authority_kind": "PHASE1_EQUITY_POINT",
        "cash_micros": money_to_micros(point.cash),
        "equity_micros": money_to_micros(point.equity),
        "external_cash_flow_micros": money_to_micros(
            point.external_cash_flow
        ),
        "ledger_name": authority.ledger_name,
        "mark_source_digest": authority.mark_source_digest,
        "mark_sources": [list(item) for item in point.mark_sources],
        "point_at": instant(authority.point_at),
        "positions_value_micros": money_to_micros(point.positions_value),
        "query_cutoff": instant(authority.query_cutoff),
        "replay_source_digest": authority.replay_source_digest,
        "session_date": authority.session_date.isoformat(),
        "source_digest": authority.source_digest,
        "validation_window_id": authority.validation_window_id,
        "version": 1,
    }


def _phase1_equity_point_authority_fingerprint(
    authority: Phase1EquityPointAuthority,
) -> tuple[object, ...]:
    return (
        _phase1_marked_equity_point_fingerprint(authority.point),
        authority.validation_window_id,
        authority.ledger_name,
        authority.session_date,
        authority.point_at,
        authority.query_cutoff,
        authority.replay_source_digest,
        authority.mark_source_digest,
        authority.source_digest,
        authority.authority_digest,
    )


def is_issued_phase1_equity_point_authority(value: object) -> bool:
    """Return whether Journal material issued this exact current identity."""
    if type(value) is not Phase1EquityPointAuthority:
        return False
    try:
        if not _phase1_derived_sources_are_current(value):
            return False
        document_digest = sha256(
            json.dumps(
                _phase1_equity_point_authority_document(value),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
    except Exception:
        return False
    if document_digest != value.authority_digest or not (
        _phase1_derived_sources_are_current_without_callbacks(value)
    ):
        return False
    bound_sources = _phase1_bound_sources(value)
    if len(bound_sources) != 1 or bound_sources[0][1] != "EQUITY_MARK":
        return False
    return _is_current_risk_authority_without_callbacks(
        _PHASE1_EQUITY_POINT_AUTHORITIES,
        value,
        exact_type=Phase1EquityPointAuthority,
        children=(bound_sources[0][0],),
    )


def _is_current_phase1_equity_point_authority_without_callbacks(
    value: object,
    mark_source: object,
) -> bool:
    """Pure final identity/fingerprint check for one already-verified point."""
    from .journal import Phase1EquityMarkSource

    if (
        type(value) is not Phase1EquityPointAuthority
        or type(mark_source) is not Phase1EquityMarkSource
    ):
        return False
    with _AUTHORITY_LOCK:
        binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(id(value))
        if (
            binding is None
            or binding[0]() is not value
            or len(binding[1]) != 1
            or binding[1][0][0] is not mark_source
            or binding[1][0][1] != "EQUITY_MARK"
        ):
            return False
    return (
        value.source_digest == mark_source.source_digest
        and _is_current_risk_authority_without_callbacks(
            _PHASE1_EQUITY_POINT_AUTHORITIES,
            value,
            exact_type=Phase1EquityPointAuthority,
            children=(mark_source,),
        )
    )


def _phase1_verified_actual_equity_replay(
    source: JournalActualReplaySource,
    state: object,
) -> object:
    """Require one exact identity-paired actual replay for an equity mark."""
    from .reconciliation import (
        ActualLedgerState,
        is_verified_actual_ledger_state_for_source,
    )

    if (
        not isinstance(source, JournalActualReplaySource)
        or not isinstance(state, ActualLedgerState)
        or not is_verified_actual_ledger_state_for_source(state, source)
        or state.query_cutoff != source.query_cutoff
        or state.through_cursor != source.terminal_cursor
        or state.journal_source_digest != source.source_digest
        or state.settlement_ledger != source.postings
        or source.expected_action_count != len(source.actions)
        or source.expected_posting_count != len(source.postings)
    ):
        raise RiskBlock("PHASE1_ACTUAL_ECONOMIC_CASH_SOURCE_MISMATCH")
    return state


def _phase1_actual_economic_cash_from_verified_replay(
    source: JournalActualReplaySource,
    state: object,
) -> Decimal:
    """Recover Phase 1 ACTUAL economic cash, including sale receivables.

    Settlement availability answers whether cash can fund a new entry.  An
    equity mark instead includes every already-executed strategy cash leg, so
    a same-day sale remains an asset before its T+1 availability date.  Only
    exact replay-issued strategy BUY/SALE/FEE postings contribute; account
    observations and caller-reconciled/external cash never do.
    """
    _phase1_verified_actual_equity_replay(source, state)

    strategy_sources = _strategy_settlement_sources(source)
    selected_posting_keys = {
        posting.posting_key for posting, _action in strategy_sources
    }
    if len(selected_posting_keys) != len(strategy_sources):
        raise RiskBlock("PHASE1_ACTUAL_ECONOMIC_CASH_SOURCE_MISMATCH")
    for posting in source.postings:
        if posting.account_name == "ACCOUNT_EVIDENCE":
            continue
        if posting.posting_key not in selected_posting_keys:
            raise RiskBlock("PHASE1_ACTUAL_ECONOMIC_CASH_SOURCE_MISMATCH")

    cash_micros = money_to_micros(_VALIDATION_CAPITAL) + sum(
        posting.amount_micros for posting, _action in strategy_sources
    )
    try:
        return money_from_micros(cash_micros)
    except DomainValidationError:
        raise RiskBlock("PHASE1_ACTUAL_ECONOMIC_CASH_OVERFLOW") from None


def _phase1_actual_strategy_positions_from_verified_replay(
    source: JournalActualReplaySource,
    state: object,
) -> tuple[object, ...]:
    """Project strategy lineages while excluding unrelated account inventory."""
    from .phase1 import PaperPosition
    from .reconciliation import ActualLedgerState

    verified = _phase1_verified_actual_equity_replay(source, state)
    assert isinstance(verified, ActualLedgerState)
    allowed_lineages = {"ACTUAL_EVENT", "ACTUAL_GROUP"}
    if any(
        position.lineage_kind
        not in {*allowed_lineages, "UNRELATED_POSITION"}
        for position in verified.positions
    ):
        raise RiskBlock("PHASE1_ACTUAL_POSITION_LINEAGE_UNRESOLVED")
    return tuple(
        PaperPosition(
            signal_id=position.signal_id,
            symbol=position.symbol,
            ledger_name="ACTUAL",
            shares=position.shares,
        )
        for position in verified.positions
        if position.lineage_kind in allowed_lineages
    )


def _issue_phase1_equity_point_from_source(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
) -> Phase1EquityPointAuthority:
    """Issue only from a complete owner-current replay/mark Journal source.

    Journal's persisted source is intentionally moneyless: the adapter must
    recover cash and open holdings from verified canonical/actual replay, then
    derive every mark from raw-bound SIP quote or completed split-adjusted
    daily-bar evidence.  Until Journal exposes that complete DTO, this seam
    fails closed rather than accepting caller-provided monetary values.
    """
    from . import journal as journal_module

    source_type = getattr(journal_module, "Phase1EquityMarkSource", None)
    source_verifier = getattr(
        journal_module,
        "is_verified_phase1_equity_mark_source",
        None,
    )
    if (
        source_type is None
        or not isinstance(source_type, type)
        or not isinstance(source, source_type)
        or not callable(source_verifier)
        or not source_verifier(source)
    ):
        raise RiskBlock("PHASE1_EQUITY_MARK_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if getattr(source, "calendar_digest", None) != _calendar_digest(
        calendar_resolver
    ):
        raise RiskBlock("PHASE1_EQUITY_CALENDAR_MISMATCH")
    required_material = (
        "validation_window_id",
        "ledger_name",
        "session_date",
        "point_at",
        "query_cutoff",
        "canonical_replay_source",
        "canonical_replay",
        "actual_replay_source",
        "actual_replay",
        "position_marks",
        "expected_position_count",
        "expected_mark_count",
        "mark_terminal_cursor",
        "mark_source_highwater",
        "row_references",
        "source_digest",
    )
    if any(not hasattr(source, name) for name in required_material):
        raise RiskBlock("PHASE1_EQUITY_MARK_SOURCE_INCOMPLETE")
    from urllib.parse import parse_qs, urlsplit

    from .ledger import (
        Phase1CanonicalLedgerReplay,
        is_verified_phase1_canonical_ledger_replay_for_source,
    )
    from .phase1 import EquityMark, PaperPosition, mark_equity
    from .providers.alpaca import (
        Bar,
        ProviderFetchCohort,
        Quote,
        _normalized_market_fact_source,
        _provider_fetch_cohort_manifest,
        is_issued_normalized_market_fact,
        is_issued_provider_fetch_cohort,
        provider_fetch_cohorts_share_owner,
        read_provider_fetch_bundle,
    )
    from .reconciliation import (
        ActualLedgerState,
        is_verified_actual_ledger_state_for_source,
    )

    ledger_name = source.ledger_name
    session_date = source.session_date
    point_at = source.point_at
    query_cutoff = source.query_cutoff
    if (
        ledger_name not in {"CANONICAL", "ACTUAL"}
        or type(session_date) is not date
        or not isinstance(point_at, datetime)
        or not isinstance(query_cutoff, datetime)
        or point_at.tzinfo is None
        or point_at.utcoffset() is None
        or query_cutoff.tzinfo is None
        or query_cutoff.utcoffset() is None
        or point_at > query_cutoff
        or point_at.astimezone(_ET).date() != session_date
    ):
        raise RiskBlock("PHASE1_EQUITY_MARK_SOURCE_MISMATCH")
    try:
        session = calendar_resolver.session(session_date)
    except RiskBlock:
        raise RiskBlock("PHASE1_EQUITY_MARK_SOURCE_MISMATCH") from None
    session_open = datetime.combine(
        session_date,
        session.open_time,
        tzinfo=_ET,
    ).astimezone(UTC)
    session_close = datetime.combine(
        session_date,
        session.close_time,
        tzinfo=_ET,
    ).astimezone(UTC)
    if point_at != session_close:
        raise RiskBlock("PHASE1_EQUITY_MARK_SOURCE_MISMATCH")

    canonical_source = source.canonical_replay_source
    canonical_replay = source.canonical_replay
    actual_source = source.actual_replay_source
    actual_replay = source.actual_replay
    phase1_sources_share_owner = getattr(
        journal_module,
        "phase1_sources_share_owner",
        None,
    )
    if (
        type(canonical_replay) is not Phase1CanonicalLedgerReplay
        or not is_verified_phase1_canonical_ledger_replay_for_source(
            canonical_replay,
            canonical_source,
        )
        or canonical_replay.source_digest
        != getattr(canonical_source, "source_digest", None)
        or canonical_replay.query_cutoff != query_cutoff
        or not isinstance(actual_source, JournalActualReplaySource)
        or not isinstance(actual_replay, ActualLedgerState)
        or not is_verified_actual_ledger_state_for_source(
            actual_replay,
            actual_source,
        )
        or actual_replay.query_cutoff != query_cutoff
        or actual_source.query_cutoff != query_cutoff
        or not callable(phase1_sources_share_owner)
        or not phase1_sources_share_owner(source, canonical_source)
        or source.validation_window_id
        != getattr(canonical_source, "validation_window_id", None)
    ):
        raise RiskBlock("PHASE1_EQUITY_REPLAY_SOURCE_MISMATCH")

    if ledger_name == "CANONICAL":
        cash = canonical_replay.canonical_cash
        replay_source_digest = canonical_source.source_digest
        raw_positions = canonical_replay.ledger_pair.canonical.open_positions
        positions = tuple(
            PaperPosition(
                signal_id=position.signal_id,
                symbol=position.symbol,
                ledger_name="CANONICAL",
                shares=position.shares,
            )
            for position in raw_positions
        )
    else:
        cash = _phase1_actual_economic_cash_from_verified_replay(
            actual_source,
            actual_replay,
        )
        replay_source_digest = actual_source.source_digest
        positions = _phase1_actual_strategy_positions_from_verified_replay(
            actual_source,
            actual_replay,
        )
    position_symbols = tuple(sorted({position.symbol for position in positions}))
    mark_sources = tuple(source.position_marks)
    if (
        source.expected_position_count != len(positions)
        or source.expected_mark_count != len(position_symbols)
        or len(mark_sources) != len(position_symbols)
        or tuple(getattr(mark, "symbol", None) for mark in mark_sources)
        != position_symbols
        or tuple(getattr(mark, "mark_ordinal", None) for mark in mark_sources)
        != tuple(range(1, len(mark_sources) + 1))
        or len(
            {
                getattr(mark, "source_cursor", None)
                for mark in mark_sources
            }
        )
        != len(mark_sources)
        or any(
            type(getattr(mark, "source_cursor", None)) is not int
            or mark.source_cursor <= 0
            for mark in mark_sources
        )
        or source.mark_terminal_cursor
        != (mark_sources[-1].source_cursor if mark_sources else None)
        or source.mark_source_highwater
        < (mark_sources[-1].source_cursor if mark_sources else 0)
    ):
        raise RiskBlock("PHASE1_EQUITY_POSITION_MARK_SET_MISMATCH")

    required_mark_material = (
        "quote_cohort",
        "daily_bar_cohort",
        "quote_facts",
        "daily_bar_facts",
        "selected_observation",
        "selected_provider_fact_source",
        "derived_price_micros",
        "mark_at",
        "mark_ordinal",
        "source_cursor",
        "quote_terminal_cursor",
        "quote_source_highwater",
        "daily_bar_terminal_cursor",
        "daily_bar_source_highwater",
        "expected_quote_fact_count",
        "expected_daily_bar_fact_count",
        "source_digest",
    )
    if any(
        not hasattr(mark, name)
        for mark in mark_sources
        for name in required_mark_material
    ):
        raise RiskBlock("PHASE1_EQUITY_POSITION_MARK_SOURCE_INCOMPLETE")
    cohorts = tuple(
        cohort
        for mark in mark_sources
        for cohort in (mark.quote_cohort, mark.daily_bar_cohort)
    )
    if cohorts and (
        any(not isinstance(cohort, ProviderFetchCohort) for cohort in cohorts)
        or any(not is_issued_provider_fetch_cohort(cohort) for cohort in cohorts)
        or not provider_fetch_cohorts_share_owner(*cohorts)
    ):
        raise RiskBlock("PHASE1_EQUITY_PROVIDER_COHORT_UNVERIFIED")
    marks: dict[str, EquityMark] = {}
    mark_source_documents: list[dict[str, object]] = []
    for mark_source in mark_sources:
        quote_cohort = mark_source.quote_cohort
        daily_bar_cohort = mark_source.daily_bar_cohort
        quote_facts = tuple(mark_source.quote_facts)
        daily_bar_facts = tuple(mark_source.daily_bar_facts)
        selected_observation = mark_source.selected_observation
        method = mark_source.method
        if (
            not isinstance(quote_cohort, ProviderFetchCohort)
            or not isinstance(daily_bar_cohort, ProviderFetchCohort)
            or not is_issued_provider_fetch_cohort(quote_cohort)
            or not is_issued_provider_fetch_cohort(daily_bar_cohort)
            or not provider_fetch_cohorts_share_owner(
                quote_cohort,
                daily_bar_cohort,
            )
            or type(mark_source.quote_facts) is not tuple
            or type(mark_source.daily_bar_facts) is not tuple
            or mark_source.expected_quote_fact_count != len(quote_facts)
            or mark_source.expected_daily_bar_fact_count
            != len(daily_bar_facts)
            or not daily_bar_facts
            or type(mark_source.quote_source_highwater) is not int
            or mark_source.quote_source_highwater < 0
            or type(mark_source.daily_bar_source_highwater) is not int
            or mark_source.daily_bar_source_highwater <= 0
            or type(mark_source.daily_bar_terminal_cursor) is not int
            or mark_source.daily_bar_terminal_cursor <= 0
            or mark_source.daily_bar_source_highwater
            < mark_source.daily_bar_terminal_cursor
            or (
                (mark_source.quote_terminal_cursor is None)
                != (len(quote_facts) == 0)
            )
            or (
                mark_source.quote_terminal_cursor is not None
                and (
                    type(mark_source.quote_terminal_cursor) is not int
                    or mark_source.quote_terminal_cursor <= 0
                    or mark_source.quote_source_highwater
                    < mark_source.quote_terminal_cursor
                )
            )
        ):
            raise RiskBlock("PHASE1_EQUITY_PROVIDER_COHORT_UNVERIFIED")
        try:
            exact_quote_facts = tuple(quote_cohort[mark_source.symbol])
            exact_daily_bar_facts = tuple(
                daily_bar_cohort[mark_source.symbol]
            )
            quote_manifest = _provider_fetch_cohort_manifest(quote_cohort)
            daily_bar_manifest = _provider_fetch_cohort_manifest(
                daily_bar_cohort
            )
            quote_bundle = read_provider_fetch_bundle(quote_cohort)
            daily_bar_bundle = read_provider_fetch_bundle(daily_bar_cohort)
            quote_fact_sources = tuple(
                _normalized_market_fact_source(fact)
                for fact in quote_facts
            )
            daily_bar_fact_sources = tuple(
                _normalized_market_fact_source(fact)
                for fact in daily_bar_facts
            )
            selected_fact_source = _normalized_market_fact_source(
                selected_observation
            )
        except (KeyError, TypeError, ValueError):
            raise RiskBlock("PHASE1_EQUITY_PROVIDER_FACT_UNVERIFIED") from None
        if (
            len(exact_quote_facts) != len(quote_facts)
            or any(
                exact is not retained
                for exact, retained in zip(
                    exact_quote_facts,
                    quote_facts,
                    strict=True,
                )
            )
            or len(exact_daily_bar_facts) != len(daily_bar_facts)
            or any(
                exact is not retained
                for exact, retained in zip(
                    exact_daily_bar_facts,
                    daily_bar_facts,
                    strict=True,
                )
            )
            or quote_bundle.manifest is not quote_manifest
            or daily_bar_bundle.manifest is not daily_bar_manifest
            or quote_manifest.collection != "quotes"
            or daily_bar_manifest.collection != "bars"
            or mark_source.symbol not in quote_manifest.requested_symbols
            or mark_source.symbol not in daily_bar_manifest.requested_symbols
            or any(
                not isinstance(fact, Quote)
                or not is_issued_normalized_market_fact(fact)
                or fact.symbol != mark_source.symbol
                or fact.feed.lower() != "sip"
                or fact_source.kind != "QUOTE"
                or fact_source.symbol != mark_source.symbol
                or fact_source.feed.lower() != "sip"
                or fact_source.fetch_manifest is not quote_manifest
                for fact, fact_source in zip(
                    quote_facts,
                    quote_fact_sources,
                    strict=True,
                )
            )
            or any(
                not isinstance(fact, Bar)
                or not is_issued_normalized_market_fact(fact)
                or fact.symbol != mark_source.symbol
                or fact.feed.lower() != "sip"
                or fact.adjustment.lower() != "split"
                or fact_source.kind != "BAR"
                or fact_source.symbol != mark_source.symbol
                or fact_source.feed.lower() != "sip"
                or fact_source.fetch_manifest is not daily_bar_manifest
                for fact, fact_source in zip(
                    daily_bar_facts,
                    daily_bar_fact_sources,
                    strict=True,
                )
            )
            or not is_issued_normalized_market_fact(selected_observation)
            or mark_source.selected_provider_fact_source
            is not selected_fact_source
            or mark_source.mark_at > point_at
            or mark_source.mark_at.astimezone(_ET).date() != session_date
            or any(
                page.observation.retrieved_at < session_close
                or page.observation.retrieved_at > query_cutoff
                for bundle in (quote_bundle, daily_bar_bundle)
                for page in bundle.pages
            )
        ):
            raise RiskBlock("PHASE1_EQUITY_PROVIDER_FACT_MISMATCH")
        try:
            quote_queries = tuple(
                parse_qs(
                    urlsplit(page.request_url).query,
                    keep_blank_values=True,
                    strict_parsing=True,
                )
                for page in quote_manifest.pages
            )
            daily_bar_queries = tuple(
                parse_qs(
                    urlsplit(page.request_url).query,
                    keep_blank_values=True,
                    strict_parsing=True,
                )
                for page in daily_bar_manifest.pages
            )
            quote_starts = {
                query["start"][0] for query in quote_queries
            }
            quote_ends = {query["end"][0] for query in quote_queries}
            quote_feeds = {
                query["feed"][0].lower() for query in quote_queries
            }
            daily_bar_starts = {
                query["start"][0] for query in daily_bar_queries
            }
            daily_bar_ends = {
                query["end"][0] for query in daily_bar_queries
            }
            daily_bar_feeds = {
                query["feed"][0].lower() for query in daily_bar_queries
            }
            daily_bar_timeframes = {
                query["timeframe"][0] for query in daily_bar_queries
            }
            daily_bar_adjustments = {
                query["adjustment"][0].lower()
                for query in daily_bar_queries
            }
            quote_request_start = datetime.fromisoformat(
                next(iter(quote_starts)).replace("Z", "+00:00")
            )
            quote_request_end = datetime.fromisoformat(
                next(iter(quote_ends)).replace("Z", "+00:00")
            )
            daily_bar_request_start = datetime.fromisoformat(
                next(iter(daily_bar_starts)).replace("Z", "+00:00")
            )
            daily_bar_request_end = datetime.fromisoformat(
                next(iter(daily_bar_ends)).replace("Z", "+00:00")
            )
            if (
                len(quote_starts) != 1
                or len(quote_ends) != 1
                or quote_feeds != {"sip"}
                or len(daily_bar_starts) != 1
                or len(daily_bar_ends) != 1
                or daily_bar_feeds != {"sip"}
                or daily_bar_timeframes != {"1Day"}
                or daily_bar_adjustments != {"split"}
                or {
                    urlsplit(page.request_url).path
                    for page in quote_manifest.pages
                }
                != {"/v2/stocks/quotes"}
                or {
                    page.source_type for page in quote_manifest.pages
                }
                != {"ALPACA_HISTORICAL_QUOTES"}
                or {
                    urlsplit(page.request_url).path
                    for page in daily_bar_manifest.pages
                }
                != {"/v2/stocks/bars"}
                or {
                    page.source_type for page in daily_bar_manifest.pages
                }
                != {"ALPACA_DAILY_BARS"}
            ):
                raise ValueError
        except (KeyError, ValueError, IndexError, StopIteration):
            raise RiskBlock("PHASE1_EQUITY_PROVIDER_FACT_MISMATCH") from None

        current_daily_bars = tuple(
            fact
            for fact in daily_bar_facts
            if fact.timestamp.astimezone(_ET).date() == session_date
        )
        usable_quotes = tuple(
            fact
            for fact in quote_facts
            if session_open <= fact.timestamp <= point_at
            and 0 <= (point_at - fact.timestamp).total_seconds() <= 60
            and fact.bid > _ZERO
            and fact.ask >= fact.bid
        )
        selected_quote = (
            max(
                usable_quotes,
                key=lambda item: (
                    item.timestamp,
                    -1 if item.sequence is None else item.sequence,
                ),
            )
            if usable_quotes
            else None
        )
        if (
            quote_request_start != session_open
            or quote_request_end != session_close
            or daily_bar_request_start > session_open
            or daily_bar_request_end != session_close
            or len(current_daily_bars) != 1
        ):
            raise RiskBlock("PHASE1_EQUITY_PROVIDER_FACT_MISMATCH")

        if selected_quote is not None:
            if (
                method != "CONSOLIDATED_BID"
                or selected_observation is not selected_quote
                or selected_fact_source.fetch_manifest is not quote_manifest
                or selected_observation.timestamp != mark_source.mark_at
                or mark_source.derived_price_micros
                != money_to_micros(selected_observation.bid)
            ):
                raise RiskBlock("PHASE1_EQUITY_MARK_PRIORITY_MISMATCH")
            marks[mark_source.symbol] = EquityMark(
                at=selected_observation.timestamp,
                bid=selected_observation.bid,
                ask=selected_observation.ask,
                completed_close=None,
            )
        else:
            selected_daily_bar = current_daily_bars[0]
            if (
                method != "CLOSE_MINUS_0.10_PERCENT"
                or selected_observation is not selected_daily_bar
                or selected_fact_source.fetch_manifest
                is not daily_bar_manifest
                or mark_source.mark_at != point_at
                or mark_source.derived_price_micros
                != (
                    money_to_micros(selected_daily_bar.close) * 999
                )
                // 1000
            ):
                raise RiskBlock("PHASE1_EQUITY_MARK_PRIORITY_MISMATCH")
            marks[mark_source.symbol] = EquityMark(
                at=point_at,
                bid=None,
                ask=None,
                completed_close=selected_daily_bar.close,
            )
        mark_source_documents.append(
            {
                "symbol": mark_source.symbol,
                "method": method,
                "derived_price_micros": mark_source.derived_price_micros,
                "quote_manifest_digest": quote_manifest.manifest_digest,
                "daily_bar_manifest_digest": (
                    daily_bar_manifest.manifest_digest
                ),
                "selected_normalized_fields_digest": (
                    selected_fact_source.normalized_fields_digest
                ),
                "source_digest": mark_source.source_digest,
            }
        )

    try:
        point = mark_equity(
            cash,
            positions,
            marks,
            ledger_name=ledger_name,
            at=point_at,
        )
    except Exception as error:
        raise RiskBlock("PHASE1_EQUITY_MARK_DERIVATION_FAILED") from error
    mark_source_digest = sha256(
        json.dumps(
            {
                "namespace": "stock-monitor/phase1-equity-mark-sources/v1",
                "marks": mark_source_documents,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    authority = Phase1EquityPointAuthority(
        point=point,
        validation_window_id=source.validation_window_id,
        ledger_name=ledger_name,
        session_date=session_date,
        point_at=point_at,
        query_cutoff=query_cutoff,
        replay_source_digest=replay_source_digest,
        mark_source_digest=mark_source_digest,
        source_digest=source.source_digest,
        authority_digest="0" * 64,
    )
    authority = replace(
        authority,
        authority_digest=sha256(
            json.dumps(
                _phase1_equity_point_authority_document(authority),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
    )
    _install_risk_authority(
        _PHASE1_EQUITY_POINT_AUTHORITIES,
        authority,
        exact_type=Phase1EquityPointAuthority,
        children=(source,),
        phase1_bindings=((source, "EQUITY_MARK"),),
    )
    return authority


def _inherit_phase1_derived_sources(value: object, parent: object) -> None:
    del value, parent
    raise RiskBlock("RISK_AUTHORITY_BINDING_UNAVAILABLE")


def _bind_journal_derived_source(
    value: object,
    source: object,
    source_kind: str,
) -> None:
    del value, source, source_kind
    raise RiskBlock("RISK_AUTHORITY_BINDING_UNAVAILABLE")


def _journal_derived_source_is_current(value: object) -> bool:
    with _AUTHORITY_LOCK:
        binding = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(id(value))
        if binding is None:
            return True
        if binding[0]() is not value:
            return False
        source = binding[1]()
        source_kind = binding[2]
    if source is None:
        return False
    if source_kind == "JOURNAL_WINDOW_SOURCE":
        return is_verified_journal_window_source(source)
    if source_kind == "JOURNAL_REPLAY_SOURCE":
        return is_verified_journal_replay_source(source)
    if source_kind == "ISSUED_EVENT_WINDOW":
        return is_issued_journal_event_window(source)
    return False


def _journal_derived_source_is_current_without_callbacks(
    value: object,
) -> bool:
    """Purely recheck one exact Journal binding after public callbacks."""
    from . import journal as journal_module

    identity = id(value)
    with _AUTHORITY_LOCK:
        binding = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(identity)
        if binding is None or binding[0]() is not value:
            return False
        captured = binding
        source = binding[1]()
        source_kind = binding[2]
    if source is None:
        return False
    if source_kind == "ISSUED_EVENT_WINDOW":
        source_is_current = (
            type(source) is JournalEventWindow
            and _is_current_risk_authority_without_callbacks(
                _JOURNAL_WINDOW_AUTHORITIES,
                source,
                exact_type=JournalEventWindow,
                children=(source.terminal_action,),
            )
        )
    else:
        source_is_current = (
            journal_module._is_current_journal_source_authority_without_callbacks(
                source
            )
        )
    if not source_is_current:
        return False
    with _AUTHORITY_LOCK:
        current = _JOURNAL_DERIVED_SOURCE_BINDINGS.get(identity)
        return current is captured and current[0]() is value


def _issue_journal_event_window(
    *,
    after_cursor: int,
    through_cursor: int,
    events: Sequence[ExecutionEvent],
    account_check: AccountCheck,
    terminal_action: ConfirmedBuyAction,
) -> JournalEventWindow:
    """Validate a diagnostic interval; Task 7 must authenticate its rows."""
    if not isinstance(account_check, AccountCheck):
        raise RiskBlock("INVALID_EVENT_WINDOW_ENDPOINT")
    if not isinstance(terminal_action, ConfirmedBuyAction):
        raise RiskBlock("INVALID_EVENT_WINDOW_ENDPOINT")
    if (
        account_check.cursor != after_cursor
        or terminal_action.cursor != through_cursor
    ):
        raise RiskBlock("EVENT_WINDOW_ENDPOINT_MISMATCH")
    window = JournalEventWindow(
        after_cursor=after_cursor,
        through_cursor=through_cursor,
        events=tuple(events),
        complete=True,
        source="JOURNAL",
        account_check=account_check,
        terminal_action=terminal_action,
    )
    return window


def _is_sha256_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parsed_journal_action(
    source: JournalActionSource,
) -> object:
    """Reparse and cross-check one already identity-sealed Journal action."""
    from .confirmations import (
        ConfirmationKind,
        ParsedConfirmation,
        PendingConfirmation,
        parse_confirmation_batch_or_pending,
    )

    if not is_verified_journal_action_source(source):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_UNVERIFIED")
    if (
        sha256(source.raw_text.encode("utf-8")).hexdigest()
        != source.raw_sha256
        or sha256(source.details_json.encode("utf-8")).hexdigest()
        != source.details_sha256
        or not _is_sha256_digest(source.source_digest)
        or not _is_sha256_digest(source.acknowledgement_payload_sha256)
        or any(
            not _is_sha256_digest(reference.row_digest)
            for reference in source.row_references
        )
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_DIGEST_INVALID")
    expected_event_id, expected_idempotency_key = stable_execution_event_identity(
        source.message_id,
        source.action_ordinal,
    )
    if (
        source.event_id != expected_event_id
        or source.idempotency_key != expected_idempotency_key
        or not source.event_time <= source.message_time <= source.received_at
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")
    try:
        details = json.loads(source.details_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID") from error
    details_source = details.get("source") if isinstance(details, dict) else None
    if (
        not isinstance(details_source, dict)
        or details_source.get("type") != "ROBINHOOD_MANUAL_CONFIRMATION"
        or type(details_source.get("received_at")) is not str
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")
    try:
        details_received_at = datetime.fromisoformat(
            details_source["received_at"].replace("Z", "+00:00")
        )
    except ValueError as error:
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID") from error
    if details_received_at != source.received_at:
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")

    parsed = parse_confirmation_batch_or_pending(
        source.raw_text,
        session_date=source.message_time.astimezone(_ET).date(),
    )
    if isinstance(parsed, PendingConfirmation):
        if source.action_ordinal != 0:
            raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")
        action: ParsedConfirmation | PendingConfirmation = parsed
        expected_kind = ConfirmationKind.PENDING_CLARIFICATION.value
        expected_at = source.message_time
        expected_symbol = None
        expected_shares = None
        expected_price = None
        expected_bid = None
        expected_ask = None
        expected_stop = None
        expected_parent = None
        expected_group_shares = None
    else:
        if source.action_ordinal >= len(parsed):
            raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")
        action = parsed[source.action_ordinal]
        expected_kind = action.kind.value
        expected_at = (
            source.message_time if action.event_time is None else action.event_time
        )
        expected_symbol = action.symbol
        expected_shares = action.quantity
        expected_price = (
            None if action.price is None else money_to_micros(action.price)
        )
        expected_bid = None if action.bid is None else money_to_micros(action.bid)
        expected_ask = None if action.ask is None else money_to_micros(action.ask)
        expected_stop = None if action.stop is None else money_to_micros(action.stop)
        expected_parent = action.parent_order_id
        expected_group_shares = action.fill_group_planned_shares
    if (
        source.storage_action != expected_kind
        or source.domain_kind != expected_kind
        or source.event_time != expected_at
        or source.symbol != expected_symbol
        or source.shares != expected_shares
        or source.price_micros != expected_price
        or source.bid_micros != expected_bid
        or source.ask_micros != expected_ask
        or source.recommended_stop_micros is not None
        or source.user_confirmed_stop_micros != expected_stop
        or source.parent_order_id != expected_parent
        or source.fill_group_planned_shares != expected_group_shares
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")
    return action


def _execution_event_from_journal_action(
    source: JournalActionSource,
    parsed: object,
) -> ExecutionEvent:
    from .confirmations import ParsedConfirmation

    price: Decimal | None = None
    shares: int | None = None
    amount: Decimal | None = None
    if isinstance(parsed, ParsedConfirmation):
        price = parsed.price
        shares = parsed.quantity
        amount = parsed.amount
    return ExecutionEvent(
        kind=source.domain_kind,
        at=source.event_time,
        price=price,
        shares=shares,
        amount=amount,
        cursor=source.execution_event_id,
        message_time=source.message_time,
        received_at=source.received_at,
        parent_order_id=source.parent_order_id,
        fill_group_planned_shares=source.fill_group_planned_shares,
    )


def _issue_account_buy_authority(
    source: JournalAccountCheckWindowSource,
) -> tuple[AccountCheck, ConfirmedBuyAction, JournalEventWindow]:
    """Issue exact risk authority from one identity-verified Journal window."""
    from .confirmations import ConfirmationKind, ParsedConfirmation

    if not isinstance(source, JournalAccountCheckWindowSource) or not (
        is_verified_journal_window_source(source)
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_UNVERIFIED")
    actions = (
        source.account_check_action,
        *source.between_actions,
        source.terminal_action,
    )
    if any(not is_verified_journal_action_source(action) for action in actions):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_UNVERIFIED")
    between_cursors = tuple(
        action.execution_event_id for action in source.between_actions
    )
    if (
        source.after_cursor != source.account_check_action.execution_event_id
        or source.through_cursor != source.terminal_action.execution_event_id
        or source.through_cursor <= source.after_cursor
        or source.source_high_water_cursor < source.through_cursor
        or source.expected_between_count != len(source.between_actions)
        or between_cursors != tuple(sorted(set(between_cursors)))
        or any(
            not source.after_cursor < cursor < source.through_cursor
            for cursor in between_cursors
        )
        or not _is_sha256_digest(source.source_digest)
        or any(
            not _is_sha256_digest(reference.row_digest)
            for reference in source.row_references
        )
    ):
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_INVALID")

    references: dict[tuple[str, int], object] = {}
    for action_source in actions:
        for reference in action_source.row_references:
            key = (reference.table, reference.row_id)
            prior = references.get(key)
            if prior is not None and prior != reference:
                raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_DIGEST_INVALID")
            references[key] = reference
    expected_references = tuple(
        sorted(
            references.values(),
            key=lambda reference: (reference.table, reference.row_id),
        )
    )
    if source.row_references != expected_references:
        raise RiskBlock("JOURNAL_ACCOUNT_WINDOW_SOURCE_DIGEST_INVALID")

    parsed_check = _parsed_journal_action(source.account_check_action)
    check_source = source.account_check_action.account_check
    if (
        not isinstance(parsed_check, ParsedConfirmation)
        or parsed_check.kind is not ConfirmationKind.ACCOUNT_CHECK
        or source.account_check_action.event_role != "OBSERVATION"
        or check_source is None
        or check_source.execution_event_id != source.after_cursor
        or check_source.raw_message_id
        != source.account_check_action.raw_message_id
        or check_source.confirmed_at != source.account_check_action.event_time
        or check_source.settled_cash_micros
        != money_to_micros(parsed_check.settled_cash)
        or check_source.pending_order_count != parsed_check.pending_orders
        or check_source.unlogged_position_count != parsed_check.unlogged_positions
    ):
        raise RiskBlock("ACCOUNT_CHECK_ENDPOINT_INVALID")
    account_check = AccountCheck(
        settled_cash=money_from_micros(check_source.settled_cash_micros),
        pending_orders=check_source.pending_order_count,
        unlogged_positions=check_source.unlogged_position_count,
        at=check_source.confirmed_at,
        reconciliation_result=check_source.reconciliation_result,
        cursor=check_source.execution_event_id,
    )

    parsed_terminal = _parsed_journal_action(source.terminal_action)
    terminal = source.terminal_action
    if (
        not isinstance(parsed_terminal, ParsedConfirmation)
        or parsed_terminal.kind is not ConfirmationKind.BUY
        or terminal.domain_kind != "BOUGHT"
        or terminal.event_role != "ECONOMIC"
        or terminal.account_check is not None
    ):
        raise RiskBlock("ACCOUNT_BUY_TERMINAL_ACTION_INVALID")
    if (
        terminal.symbol is None
        or terminal.shares is None
        or terminal.price_micros is None
        or terminal.bid_micros is None
        or terminal.ask_micros is None
        or terminal.user_confirmed_stop_micros is None
        or parsed_terminal.missing_fields
    ):
        raise RiskBlock("ACCOUNT_BUY_CONFIRMATION_INCOMPLETE")
    terminal_action = ConfirmedBuyAction(
        event_id=terminal.event_id,
        idempotency_key=terminal.idempotency_key,
        message_id=terminal.message_id,
        action_ordinal=terminal.action_ordinal,
        cursor=terminal.execution_event_id,
        symbol=terminal.symbol,
        shares=terminal.shares,
        price=money_from_micros(terminal.price_micros),
        at=terminal.event_time,
        message_time=terminal.message_time,
        received_at=terminal.received_at,
        bid=money_from_micros(terminal.bid_micros),
        ask=money_from_micros(terminal.ask_micros),
        user_confirmed_stop=money_from_micros(
            terminal.user_confirmed_stop_micros
        ),
        source="ROBINHOOD_MANUAL_CONFIRMATION",
        raw_sha256=terminal.raw_sha256,
        details_sha256=terminal.details_sha256,
        parent_order_id=terminal.parent_order_id,
        fill_group_planned_shares=terminal.fill_group_planned_shares,
    )
    events = tuple(
        _execution_event_from_journal_action(
            action_source,
            _parsed_journal_action(action_source),
        )
        for action_source in source.between_actions
    )
    window = JournalEventWindow(
        after_cursor=source.after_cursor,
        through_cursor=source.through_cursor,
        events=events,
        complete=True,
        source="JOURNAL",
        account_check=account_check,
        terminal_action=terminal_action,
    )
    _install_risk_authority(
        _CONFIRMED_BUY_AUTHORITIES,
        terminal_action,
        exact_type=ConfirmedBuyAction,
        journal_binding=(source, "JOURNAL_WINDOW_SOURCE"),
    )
    _install_risk_authority(
        _JOURNAL_WINDOW_AUTHORITIES,
        window,
        exact_type=JournalEventWindow,
        children=(terminal_action,),
        journal_binding=(source, "JOURNAL_WINDOW_SOURCE"),
    )
    return account_check, terminal_action, window


def is_issued_journal_event_window(window: object) -> bool:
    """Return whether *window* is the exact unmodified adapter-issued object."""
    if type(window) is not JournalEventWindow:
        return False
    terminal_action = window.terminal_action
    if (
        terminal_action is None
        or type(terminal_action) is not ConfirmedBuyAction
        or not _journal_derived_source_is_current(window)
        or not is_issued_confirmed_buy_action(terminal_action)
        or not _journal_derived_source_is_current_without_callbacks(window)
        or not _journal_derived_source_is_current_without_callbacks(
            terminal_action
        )
        or not _is_current_risk_authority_without_callbacks(
            _CONFIRMED_BUY_AUTHORITIES,
            terminal_action,
            exact_type=ConfirmedBuyAction,
        )
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _JOURNAL_WINDOW_AUTHORITIES,
        window,
        exact_type=JournalEventWindow,
        children=(terminal_action,),
    )


@dataclass(frozen=True, slots=True)
class AccountCheckDecision:
    eligible: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(
                self.reason_codes,
                "INVALID_ACCOUNT_CHECK_DECISION",
            ),
        )
        if type(self.eligible) is not bool or self.eligible == bool(self.reason_codes):
            raise RiskBlock("INVALID_ACCOUNT_CHECK_DECISION")


def _same_fill_group_event(
    event: ExecutionEvent,
    terminal_buy: ExecutionEvent,
) -> bool:
    return (
        terminal_buy.parent_order_id is not None
        and event.parent_order_id == terminal_buy.parent_order_id
        and event.fill_group_planned_shares
        == terminal_buy.fill_group_planned_shares
        and event.kind in {"BUY", "BOUGHT", "PARTIAL_FILL"}
    )


def _account_check_reasons(
    check: AccountCheck,
    buy: ExecutionEvent,
    intervening_events: Sequence[ExecutionEvent],
) -> tuple[str, ...]:
    if not isinstance(check, AccountCheck):
        raise TypeError("account check must be an AccountCheck")
    if not isinstance(buy, ExecutionEvent):
        raise TypeError("buy must be an ExecutionEvent")
    if isinstance(intervening_events, (str, bytes)):
        raise TypeError("intervening events must be a sequence")
    events = tuple(intervening_events)
    if any(not isinstance(event, ExecutionEvent) for event in events):
        raise TypeError("intervening events contain an invalid value")

    reasons: list[str] = []
    if buy.kind not in {"BUY", "BOUGHT", "PARTIAL_FILL"}:
        reasons.append("EVENT_IS_NOT_BUY")
    if check.at >= buy.at:
        reasons.append("ACCOUNT_CHECK_NOT_BEFORE_BUY")
    if check.at.astimezone(_ET).date() != buy.at.astimezone(_ET).date():
        reasons.append("ACCOUNT_CHECK_SESSION_MISMATCH")
    if check.reconciliation_result != "CLEAR":
        reasons.append("ACCOUNT_CHECK_RECONCILIATION_REQUIRED")
    if check.pending_orders != 0:
        reasons.append("PENDING_ORDERS_PRESENT")
    if check.unlogged_positions != 0:
        reasons.append("UNLOGGED_POSITIONS_PRESENT")
    if buy.price is not None and buy.shares is not None:
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(
                check.settled_cash,
                buy.price,
            )
            required_cash = buy.price * (
                buy.fill_group_planned_shares
                if buy.fill_group_planned_shares is not None
                else buy.shares
            )
        if check.settled_cash < required_cash:
            reasons.append("INSUFFICIENT_ACCOUNT_SETTLED_CASH")
    if any(
        check.at < event.at < buy.at
        and event.kind in _ACCOUNT_INVALIDATING_EVENT_KINDS
        and not _same_fill_group_event(event, buy)
        for event in events
    ):
        reasons.append("INTERVENING_ACCOUNT_EVENT")
    if any(
        check.at < event.at < buy.at
        and event.kind == "PENDING_CLARIFICATION"
        for event in events
    ):
        reasons.append("PENDING_ACCOUNT_EVENT")
    return tuple(reasons)


def account_check_eligible(
    check: AccountCheck,
    buy: ExecutionEvent,
    intervening_events: Sequence[ExecutionEvent],
) -> bool:
    """Pure compatibility helper; caller asserts the event sequence is complete."""
    return not _account_check_reasons(check, buy, intervening_events)


def evaluate_account_check_window(
    check: AccountCheck,
    buy: ExecutionEvent,
    window: JournalEventWindow,
) -> AccountCheckDecision:
    """Require a complete Journal-issued cursor window for production authority."""
    if not isinstance(window, JournalEventWindow):
        raise TypeError("account event window must be a JournalEventWindow")
    event_cursors = tuple(event.cursor for event in window.events)
    diagnostic_window_complete = (
        window.complete
        and window.source == "JOURNAL"
        and check.cursor is not None
        and buy.cursor is not None
        and window.after_cursor == check.cursor
        and window.through_cursor == buy.cursor
        and all(
            event.cursor is not None
            and window.after_cursor < event.cursor < window.through_cursor
            for event in window.events
        )
        and event_cursors == tuple(sorted(event_cursors))
        and len(event_cursors) == len(set(event_cursors))
        and window.account_check == check
        and window.terminal_event == buy
    )
    reasons: list[str] = []
    if not is_issued_journal_event_window(window):
        reasons.append("EVENT_WINDOW_UNVERIFIED")
    if not diagnostic_window_complete:
        reasons.append("EVENT_WINDOW_INCOMPLETE")
    if window.account_check != check or window.terminal_event != buy:
        reasons.append("EVENT_WINDOW_ENDPOINT_MISMATCH")
    reasons.extend(_account_check_reasons(check, buy, window.events))
    if any(event.kind == "PENDING_CLARIFICATION" for event in window.events):
        reasons.append("PENDING_ACCOUNT_EVENT")
    if any(
        event.kind in _ACCOUNT_INVALIDATING_EVENT_KINDS
        and not _same_fill_group_event(event, buy)
        for event in window.events
    ):
        reasons.append("INTERVENING_ACCOUNT_EVENT")
    unique_reasons = tuple(dict.fromkeys(reasons))
    return AccountCheckDecision(not unique_reasons, unique_reasons)


@dataclass(frozen=True, slots=True)
class SessionCalendarResolver:
    """Resolve open sessions across explicitly verified yearly calendars."""

    calendars: tuple[MarketCalendar, ...]
    _require_release: bool = field(default=True, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.calendars, (str, bytes)):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        calendars = tuple(self.calendars)
        if not calendars or any(
            not isinstance(calendar, MarketCalendar) for calendar in calendars
        ):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        if any(not is_validated_market_calendar(calendar) for calendar in calendars):
            raise RiskBlock("CALENDAR_AUTHORITY_UNVERIFIED")
        if self._require_release and any(
            not is_release_verified_market_calendar(calendar)
            for calendar in calendars
        ):
            raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
        if type(self._require_release) is not bool:
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        years = tuple(calendar.year for calendar in calendars)
        if len(years) != len(set(years)):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        object.__setattr__(
            self,
            "calendars",
            tuple(sorted(calendars, key=lambda calendar: calendar.year)),
        )

    @classmethod
    def for_diagnostics(
        cls,
        calendars: Sequence[MarketCalendar],
    ) -> SessionCalendarResolver:
        """Use validated manifests for pure arithmetic without live authority."""
        return cls(tuple(calendars), _require_release=False)

    @property
    def release_verified(self) -> bool:
        return self._require_release and all(
            is_release_verified_market_calendar(calendar)
            for calendar in self.calendars
        )

    def _calendar(self, day: date) -> MarketCalendar:
        for calendar in self.calendars:
            if calendar.year == day.year:
                return calendar
        raise RiskBlock("CALENDAR_COVERAGE_MISSING")

    def is_open(self, day: date) -> bool:
        if type(day) is not date:
            raise RiskBlock("INVALID_SETTLEMENT_DATE")
        try:
            return self._calendar(day).is_open(day)
        except CalendarError:
            raise RiskBlock("CALENDAR_COVERAGE_MISSING") from None

    def session(self, day: date):
        """Return the exact reviewed session schedule for *day*."""
        if type(day) is not date:
            raise RiskBlock("INVALID_SESSION_DATE")
        try:
            return self._calendar(day).session(day)
        except CalendarError as error:
            if self._calendar(day).is_open(day):
                raise RiskBlock("CALENDAR_COVERAGE_MISSING") from error
            raise RiskBlock("ENTRY_SESSION_CLOSED") from error

    def add_sessions(self, start: date, count: int) -> date:
        if type(start) is not date or type(count) is not int or count < 0:
            raise RiskBlock("INVALID_SETTLEMENT_OFFSET")
        self._calendar(start)
        current = start
        remaining = count
        while remaining:
            current += timedelta(days=1)
            if self.is_open(current):
                remaining -= 1
        return current

    def previous_session(self, start: date) -> date:
        """Return the latest reviewed open session strictly before *start*."""
        if type(start) is not date:
            raise RiskBlock("INVALID_SESSION_DATE")
        current = start - timedelta(days=1)
        while not self.is_open(current):
            current -= timedelta(days=1)
        return current

    def count_sessions(self, start: date, end: date) -> int:
        """Count open sessions inclusively between two reviewed dates."""
        if type(start) is not date or type(end) is not date or end < start:
            raise RiskBlock("INVALID_SESSION_RANGE")
        if not self.is_open(start) or not self.is_open(end):
            raise RiskBlock("SESSION_RANGE_ENDPOINT_CLOSED")
        current = start
        count = 0
        while current <= end:
            if self.is_open(current):
                count += 1
            current += timedelta(days=1)
        return count


@dataclass(frozen=True, slots=True)
class SettlementReplayAuthority:
    """Exact Journal/state cohort behind one descriptive settlement replay."""

    query_cutoff: datetime
    through_execution_cursor: int | None
    physical_source_highwater_cursor: int | None
    journal_source_digest: str
    actual_state_digest: str
    calendar_digest: str
    policy_digest: str
    expected_action_count: int
    expected_posting_count: int
    strategy_posting_count: int

    def __post_init__(self) -> None:
        _require_aware(self.query_cutoff, "INVALID_SETTLEMENT_REPLAY_AUTHORITY")
        for cursor in (
            self.through_execution_cursor,
            self.physical_source_highwater_cursor,
        ):
            if cursor is not None:
                _require_positive_int(
                    cursor,
                    "INVALID_SETTLEMENT_REPLAY_AUTHORITY",
                )
        if (
            self.through_execution_cursor is not None
            and self.physical_source_highwater_cursor is not None
            and self.through_execution_cursor
            > self.physical_source_highwater_cursor
        ):
            raise RiskBlock("INVALID_SETTLEMENT_REPLAY_AUTHORITY")
        for digest in (
            self.journal_source_digest,
            self.actual_state_digest,
            self.calendar_digest,
            self.policy_digest,
        ):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in digest
                )
            ):
                raise RiskBlock("INVALID_SETTLEMENT_REPLAY_AUTHORITY")
        for count in (
            self.expected_action_count,
            self.expected_posting_count,
            self.strategy_posting_count,
        ):
            _require_nonnegative_int(
                count,
                "INVALID_SETTLEMENT_REPLAY_AUTHORITY",
            )
        if self.strategy_posting_count > self.expected_posting_count:
            raise RiskBlock("INVALID_SETTLEMENT_REPLAY_AUTHORITY")


@dataclass(frozen=True, slots=True)
class SettlementPosting:
    kind: str
    amount: Decimal
    at: datetime
    available_on: date | None
    reason_code: str | None = None
    posting_id: str = ""
    cursor: int | None = None
    ordinal: int = 0
    source_event_id: str | None = None
    message_time: datetime | None = None
    received_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"BUY", "FEE", "SALE"}:
            raise RiskBlock("INVALID_SETTLEMENT_POSTING")
        object.__setattr__(
            self,
            "amount",
            _require_money(
                self.amount,
                reason_code="INVALID_SETTLEMENT_AMOUNT",
                positive=True,
            ),
        )
        _require_aware(self.at, "INVALID_SETTLEMENT_TIME")
        if self.available_on is not None and type(self.available_on) is not date:
            raise RiskBlock("INVALID_SETTLEMENT_DATE")
        if (self.available_on is None) != (self.reason_code is not None):
            raise RiskBlock("INVALID_SETTLEMENT_POSTING")
        if type(self.posting_id) is not str or not self.posting_id:
            raise RiskBlock("INVALID_SETTLEMENT_POSTING_ID")
        if self.source_event_id is not None:
            if type(self.source_event_id) is not str or not self.source_event_id:
                raise RiskBlock("INVALID_SETTLEMENT_POSTING_ID")
            if self.posting_id != f"settlement:{self.source_event_id}":
                raise RiskBlock("INVALID_SETTLEMENT_POSTING_ID")
        if self.cursor is not None:
            _require_positive_int(self.cursor, "INVALID_SETTLEMENT_CURSOR")
        _require_nonnegative_int(self.ordinal, "INVALID_SETTLEMENT_ORDINAL")
        if (self.message_time is None) != (self.received_at is None):
            raise RiskBlock("SETTLEMENT_SOURCE_TIME_INCOMPLETE")
        if self.message_time is not None and self.received_at is not None:
            message_time = _require_aware(
                self.message_time,
                "INVALID_SETTLEMENT_MESSAGE_TIME",
            )
            received_at = _require_aware(
                self.received_at,
                "INVALID_SETTLEMENT_RECEIVED_TIME",
            )
            if not self.at <= message_time <= received_at:
                raise RiskBlock("SETTLEMENT_SOURCE_TIME_OUT_OF_ORDER")

    @property
    def source_received_at(self) -> datetime:
        """Return knowledge time; diagnostics without source rows use ``at``."""
        return self.received_at if self.received_at is not None else self.at


def _settlement_posting_id(
    kind: str,
    amount: Decimal,
    at: datetime,
    source_event_id: str | None,
) -> str:
    if source_event_id is not None:
        return f"settlement:{source_event_id}"
    content = json.dumps(
        {
            "version": 1,
            "kind": kind,
            "amount_micros": money_to_micros(amount),
            "at": at.astimezone(UTC).isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z"),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"settlement:{sha256(content.encode('utf-8')).hexdigest()}"


def _settlement_order_key(
    posting: SettlementPosting,
) -> tuple[object, ...]:
    if posting.cursor is not None:
        return (1, posting.cursor, posting.ordinal, posting.posting_id)
    return (
        0,
        posting.at.astimezone(UTC),
        posting.ordinal,
        posting.posting_id,
    )


def _validate_settlement_event_time(
    calendar_resolver: SessionCalendarResolver,
    at: datetime,
) -> date:
    local = at.astimezone(_ET)
    session_date = local.date()
    if not calendar_resolver.is_open(session_date):
        raise RiskBlock("SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION")
    try:
        session = calendar_resolver.session(session_date)
    except RiskBlock:
        raise RiskBlock("SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION") from None
    local_time = local.time().replace(tzinfo=None)
    if not session.open_time <= local_time <= session.close_time:
        raise RiskBlock("SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION")
    return session_date


def _normalize_settlement_source_times(
    at: datetime,
    message_time: datetime | None,
    received_at: datetime | None,
) -> tuple[datetime, datetime]:
    if message_time is None:
        message_time = at
    if received_at is None:
        received_at = message_time
    message_time = _require_aware(
        message_time,
        "INVALID_SETTLEMENT_MESSAGE_TIME",
    )
    received_at = _require_aware(
        received_at,
        "INVALID_SETTLEMENT_RECEIVED_TIME",
    )
    if not at <= message_time <= received_at:
        raise RiskBlock("SETTLEMENT_SOURCE_TIME_OUT_OF_ORDER")
    return message_time, received_at


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SettlementLedger:
    """Immutable strategy-cash estimate; account checks remain separate evidence."""

    initial_settled_cash: Decimal
    initialized_at: datetime
    calendar_resolver: SessionCalendarResolver
    postings: tuple[SettlementPosting, ...] = ()
    replay_authority: SettlementReplayAuthority | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "initial_settled_cash",
            _require_money(
                self.initial_settled_cash,
                reason_code="INVALID_INITIAL_SETTLED_CASH",
                nonnegative=True,
            ),
        )
        _require_aware(self.initialized_at, "INVALID_SETTLEMENT_TIME")
        if not isinstance(self.calendar_resolver, SessionCalendarResolver):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        if self.replay_authority is not None and not isinstance(
            self.replay_authority,
            SettlementReplayAuthority,
        ):
            raise RiskBlock("INVALID_SETTLEMENT_REPLAY_AUTHORITY")
        supplied_postings = tuple(self.postings)
        if any(
            not isinstance(posting, SettlementPosting)
            for posting in supplied_postings
        ):
            raise RiskBlock("INVALID_SETTLEMENT_POSTING")
        postings: list[SettlementPosting] = []
        by_id: dict[str, SettlementPosting] = {}
        previous_key: tuple[object, ...] | None = None
        source_coordinates: set[tuple[int, int]] = set()
        previous_source_message_time: datetime | None = None
        previous_source_received_at: datetime | None = None
        for posting in supplied_postings:
            if posting.at < self.initialized_at:
                raise RiskBlock("SETTLEMENT_EVENT_BEFORE_INITIALIZATION")
            session_date = posting.at.astimezone(_ET).date()
            if self.replay_authority is None:
                session_date = _validate_settlement_event_time(
                    self.calendar_resolver,
                    posting.at,
                )
            if posting.kind in {"BUY", "FEE"}:
                expected_available_on = session_date
                expected_reason = None
            else:
                try:
                    expected_available_on = self.calendar_resolver.add_sessions(
                        session_date,
                        1,
                    )
                    expected_reason = None
                except RiskBlock as error:
                    if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                        raise
                    expected_available_on = None
                    expected_reason = "CALENDAR_COVERAGE_MISSING"
            if (
                posting.available_on != expected_available_on
                or posting.reason_code != expected_reason
            ):
                raise RiskBlock("SETTLEMENT_AVAILABILITY_CONFLICT")
            prior = by_id.get(posting.posting_id)
            if prior is not None:
                if prior != posting:
                    raise RiskBlock("SETTLEMENT_POSTING_IDEMPOTENCY_CONFLICT")
                continue
            if posting.cursor is not None:
                coordinate = (posting.cursor, posting.ordinal)
                if coordinate in source_coordinates:
                    raise RiskBlock("SETTLEMENT_SOURCE_COORDINATE_CONFLICT")
                source_coordinates.add(coordinate)
                if posting.message_time is not None and posting.received_at is not None:
                    if (
                        previous_source_message_time is not None
                        and posting.message_time < previous_source_message_time
                    ) or (
                        previous_source_received_at is not None
                        and posting.received_at < previous_source_received_at
                    ):
                        raise RiskBlock("SETTLEMENT_SOURCE_TIME_OUT_OF_ORDER")
                    previous_source_message_time = posting.message_time
                    previous_source_received_at = posting.received_at
            order_key = _settlement_order_key(posting)
            if previous_key is not None and order_key <= previous_key:
                raise RiskBlock("SETTLEMENT_POSTINGS_OUT_OF_ORDER")
            postings.append(posting)
            by_id[posting.posting_id] = posting
            previous_key = order_key
        object.__setattr__(self, "postings", tuple(postings))

    @property
    def source_verified(self) -> bool:
        try:
            if (
                not isinstance(
                    self.calendar_resolver,
                    SessionCalendarResolver,
                )
                or not self.calendar_resolver.release_verified
                or not _journal_derived_source_is_current(self)
            ):
                return False
        except Exception:
            return False
        return (
            _journal_derived_source_is_current_without_callbacks(self)
            and _is_current_risk_authority_without_callbacks(
                _SETTLEMENT_LEDGER_AUTHORITIES,
                self,
                exact_type=SettlementLedger,
            )
        )

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                posting.reason_code
                for posting in self.postings
                if posting.reason_code is not None
            )
        )

    def _amount(self, price: Decimal, shares: int) -> Decimal:
        price = _require_money(
            price,
            reason_code="INVALID_SETTLEMENT_PRICE",
            positive=True,
        )
        shares = _require_positive_int(shares, "INVALID_SETTLEMENT_SHARES")
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(
                price,
                Decimal(shares),
            )
            amount = price * shares
        return _require_money(
            amount,
            reason_code="INVALID_SETTLEMENT_AMOUNT",
            positive=True,
        )

    def _append_posting(
        self,
        *,
        kind: str,
        amount: Decimal,
        at: datetime,
        available_on: date | None,
        reason_code: str | None,
        source_event_id: str | None,
        record_cursor: int | None,
        action_ordinal: int,
        message_time: datetime | None,
        received_at: datetime | None,
    ) -> SettlementLedger:
        if (source_event_id is None) != (record_cursor is None):
            raise RiskBlock("SETTLEMENT_SOURCE_IDENTITY_INCOMPLETE")
        if source_event_id is not None and (
            type(source_event_id) is not str or not source_event_id
        ):
            raise RiskBlock("INVALID_SETTLEMENT_POSTING_ID")
        if record_cursor is not None:
            _require_positive_int(record_cursor, "INVALID_SETTLEMENT_CURSOR")
        _require_nonnegative_int(action_ordinal, "INVALID_SETTLEMENT_ORDINAL")
        message_time, received_at = _normalize_settlement_source_times(
            at,
            message_time,
            received_at,
        )
        posting_id = _settlement_posting_id(
            kind,
            amount,
            at,
            source_event_id,
        )
        matching = tuple(
            posting
            for posting in self.postings
            if posting.posting_id == posting_id
        )
        if matching:
            expected = replace(
                matching[0],
                kind=kind,
                amount=amount,
                at=at,
                available_on=available_on,
                reason_code=reason_code,
                cursor=record_cursor,
                ordinal=action_ordinal,
                source_event_id=source_event_id,
                message_time=message_time,
                received_at=received_at,
            )
            if expected == matching[0]:
                return self
            raise RiskBlock("SETTLEMENT_POSTING_IDEMPOTENCY_CONFLICT")
        ordinal = (
            action_ordinal
            if record_cursor is not None
            else 1
            + max(
                (
                    posting.ordinal
                    for posting in self.postings
                    if posting.cursor is None
                ),
                default=-1,
            )
        )
        posting = SettlementPosting(
            kind=kind,
            amount=amount,
            at=at,
            available_on=available_on,
            reason_code=reason_code,
            posting_id=posting_id,
            cursor=record_cursor,
            ordinal=ordinal,
            source_event_id=source_event_id,
            message_time=message_time,
            received_at=received_at,
        )
        return replace(self, postings=(*self.postings, posting))

    def record_buy(
        self,
        *,
        price: Decimal,
        shares: int,
        at: datetime,
        source_event_id: str | None = None,
        record_cursor: int | None = None,
        action_ordinal: int = 0,
        message_time: datetime | None = None,
        received_at: datetime | None = None,
    ) -> SettlementLedger:
        at = _require_aware(at, "INVALID_SETTLEMENT_TIME")
        if at < self.initialized_at:
            raise RiskBlock("SETTLEMENT_EVENT_BEFORE_INITIALIZATION")
        _validate_settlement_event_time(self.calendar_resolver, at)
        message_time, received_at = _normalize_settlement_source_times(
            at,
            message_time,
            received_at,
        )
        amount = self._amount(price, shares)
        if self.settled_cash(received_at) < amount:
            raise RiskBlock("INSUFFICIENT_SETTLED_CASH")
        return self._append_posting(
            kind="BUY",
            amount=amount,
            at=at,
            available_on=at.astimezone(_ET).date(),
            reason_code=None,
            source_event_id=source_event_id,
            record_cursor=record_cursor,
            action_ordinal=action_ordinal,
            message_time=message_time,
            received_at=received_at,
        )

    def record_sale(
        self,
        *,
        price: Decimal,
        shares: int,
        at: datetime,
        source_event_id: str | None = None,
        record_cursor: int | None = None,
        action_ordinal: int = 0,
        message_time: datetime | None = None,
        received_at: datetime | None = None,
    ) -> SettlementLedger:
        at = _require_aware(at, "INVALID_SETTLEMENT_TIME")
        if at < self.initialized_at:
            raise RiskBlock("SETTLEMENT_EVENT_BEFORE_INITIALIZATION")
        sale_day = _validate_settlement_event_time(self.calendar_resolver, at)
        amount = self._amount(price, shares)
        try:
            available_on = self.calendar_resolver.add_sessions(sale_day, 1)
            reason_code = None
        except RiskBlock as error:
            if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                raise
            available_on = None
            reason_code = error.reason_code
        return self._append_posting(
            kind="SALE",
            amount=amount,
            at=at,
            available_on=available_on,
            reason_code=reason_code,
            source_event_id=source_event_id,
            record_cursor=record_cursor,
            action_ordinal=action_ordinal,
            message_time=message_time,
            received_at=received_at,
        )

    def resolve_calendar(
        self,
        calendar_resolver: SessionCalendarResolver,
    ) -> SettlementLedger:
        """Rebuild unresolved T+1 dates after new reviewed coverage exists."""
        if not isinstance(calendar_resolver, SessionCalendarResolver):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        postings: list[SettlementPosting] = []
        for posting in self.postings:
            if (
                posting.kind != "SALE"
                or posting.reason_code != "CALENDAR_COVERAGE_MISSING"
            ):
                postings.append(posting)
                continue
            sale_day = posting.at.astimezone(_ET).date()
            try:
                available_on = calendar_resolver.add_sessions(sale_day, 1)
            except RiskBlock as error:
                if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                    raise
                postings.append(posting)
            else:
                postings.append(
                    replace(
                        posting,
                        available_on=available_on,
                        reason_code=None,
                    )
                )
        return replace(
            self,
            calendar_resolver=calendar_resolver,
            postings=tuple(postings),
        )

    def settled_cash(self, at: datetime) -> Decimal:
        at = _require_aware(at, "INVALID_SETTLEMENT_TIME")
        if at < self.initialized_at:
            raise RiskBlock("SETTLEMENT_TIME_BEFORE_INITIALIZATION")
        as_of_day = at.astimezone(_ET).date()
        values = [self.initial_settled_cash]
        for posting in self.postings:
            if posting.kind in {"BUY", "FEE"} and posting.source_received_at <= at:
                values.append(-posting.amount)
            elif (
                posting.kind == "SALE"
                and posting.source_received_at <= at
                and posting.available_on is not None
                and posting.available_on <= as_of_day
            ):
                values.append(posting.amount)
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(*values)
            total = sum(values, _ZERO)
        return _require_money(
            total,
            reason_code="INVALID_SETTLED_CASH",
        )

    def unsettled_sale_proceeds(self, at: datetime) -> Decimal:
        at = _require_aware(at, "INVALID_SETTLEMENT_TIME")
        as_of_day = at.astimezone(_ET).date()
        values = tuple(
            posting.amount
            for posting in self.postings
            if posting.kind == "SALE"
            and posting.source_received_at <= at
            and (
                posting.available_on is None
                or posting.available_on > as_of_day
            )
        )
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(*values)
            total = sum(values, _ZERO)
        return _require_money(
            total,
            reason_code="INVALID_UNSETTLED_PROCEEDS",
            nonnegative=True,
        )


def _settlement_ledger_fingerprint(
    ledger: SettlementLedger,
) -> tuple[object, ...]:
    replay_authority = getattr(ledger, "replay_authority", None)
    return (
        money_to_micros(ledger.initial_settled_cash),
        ledger.initialized_at.astimezone(UTC).isoformat(timespec="microseconds"),
        _calendar_digest(ledger.calendar_resolver),
        tuple(
            (
                posting.kind,
                money_to_micros(posting.amount),
                posting.at.astimezone(UTC).isoformat(timespec="microseconds"),
                posting.available_on,
                posting.reason_code,
                posting.posting_id,
                posting.cursor,
                posting.ordinal,
                posting.source_event_id,
                (
                    None
                    if posting.message_time is None
                    else posting.message_time.astimezone(UTC).isoformat(
                        timespec="microseconds"
                    )
                ),
                (
                    None
                    if posting.received_at is None
                    else posting.received_at.astimezone(UTC).isoformat(
                        timespec="microseconds"
                    )
                ),
            )
            for posting in ledger.postings
        ),
        (
            None
            if replay_authority is None
            else _settlement_replay_authority_fingerprint(
                replay_authority
            )
        ),
    )


def _settlement_replay_authority_fingerprint(
    authority: SettlementReplayAuthority,
) -> tuple[object, ...]:
    return (
        authority.query_cutoff.astimezone(UTC).isoformat(
            timespec="microseconds"
        ),
        authority.through_execution_cursor,
        authority.physical_source_highwater_cursor,
        authority.journal_source_digest,
        authority.actual_state_digest,
        authority.calendar_digest,
        authority.policy_digest,
        authority.expected_action_count,
        authority.expected_posting_count,
        authority.strategy_posting_count,
    )


def _settlement_ledger_content_digest(ledger: SettlementLedger) -> str:
    """Hash the full canonical settlement projection, not only posting IDs."""
    if not isinstance(ledger, SettlementLedger):
        raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
    replay_authority = getattr(ledger, "replay_authority", None)
    payload = {
        "version": 2,
        "initial_settled_cash_micros": money_to_micros(
            ledger.initial_settled_cash
        ),
        "initialized_at": ledger.initialized_at.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "calendar_digest": _calendar_digest(ledger.calendar_resolver),
        "postings": [
            {
                "posting_id": posting.posting_id,
                "source_event_id": posting.source_event_id,
                "kind": posting.kind,
                "amount_micros": money_to_micros(posting.amount),
                "at": posting.at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "message_time": (
                    None
                    if posting.message_time is None
                    else posting.message_time.astimezone(UTC).isoformat(
                        timespec="microseconds"
                    ).replace("+00:00", "Z")
                ),
                "received_at": (
                    None
                    if posting.received_at is None
                    else posting.received_at.astimezone(UTC).isoformat(
                        timespec="microseconds"
                    ).replace("+00:00", "Z")
                ),
                "available_on": (
                    None
                    if posting.available_on is None
                    else posting.available_on.isoformat()
                ),
                "reason_code": posting.reason_code,
                "cursor": posting.cursor,
                "ordinal": posting.ordinal,
            }
            for posting in ledger.postings
        ],
        "replay_authority": (
            None
            if replay_authority is None
            else {
                "query_cutoff": replay_authority.query_cutoff.astimezone(
                    UTC
                ).isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "through_execution_cursor": (
                    replay_authority.through_execution_cursor
                ),
                "physical_source_highwater_cursor": (
                    replay_authority.physical_source_highwater_cursor
                ),
                "journal_source_digest": (
                    replay_authority.journal_source_digest
                ),
                "actual_state_digest": (
                    replay_authority.actual_state_digest
                ),
                "calendar_digest": replay_authority.calendar_digest,
                "policy_digest": replay_authority.policy_digest,
                "expected_action_count": (
                    replay_authority.expected_action_count
                ),
                "expected_posting_count": (
                    replay_authority.expected_posting_count
                ),
                "strategy_posting_count": (
                    replay_authority.strategy_posting_count
                ),
            }
        ),
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _issue_settlement_ledger_from_account_window(
    *,
    event_window: JournalEventWindow,
    calendar_resolver: SessionCalendarResolver,
) -> SettlementLedger:
    """Issue a pre-buy settlement snapshot from one complete Journal window."""
    if not is_issued_journal_event_window(event_window):
        raise RiskBlock("EVENT_WINDOW_UNVERIFIED")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    terminal = event_window.terminal_event
    decision = evaluate_account_check_window(
        event_window.account_check,
        terminal,
        event_window,
    )
    if not decision.eligible:
        raise RiskBlock("SETTLEMENT_AUTHORITY_UNVERIFIED")
    session_date = terminal.at.astimezone(_ET).date()
    if not calendar_resolver.is_open(session_date):
        raise RiskBlock("ENTRY_SESSION_CLOSED")
    ledger = SettlementLedger(
        initial_settled_cash=event_window.account_check.settled_cash,
        initialized_at=event_window.account_check.at,
        calendar_resolver=calendar_resolver,
    )
    _install_risk_authority(
        _SETTLEMENT_LEDGER_AUTHORITIES,
        ledger,
        exact_type=SettlementLedger,
        journal_binding=(event_window, "ISSUED_EVENT_WINDOW"),
    )
    return ledger


def _validate_journal_actual_replay_cohort(
    source: JournalActualReplaySource,
    state: object,
    calendar_resolver: SessionCalendarResolver,
) -> None:
    """Validate exact source/state/calendar identities before issuing bridges."""
    from .reconciliation import (
        ActualLedgerState,
        is_verified_actual_ledger_state,
        is_verified_actual_ledger_state_for_source,
    )

    if not is_verified_journal_replay_source(source):
        raise RiskBlock("JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED")
    if not isinstance(state, ActualLedgerState) or not is_verified_actual_ledger_state(
        state
    ):
        raise RiskBlock("ACTUAL_LEDGER_STATE_UNVERIFIED")
    if not is_verified_actual_ledger_state_for_source(state, source):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    calendar_digest = _calendar_digest(calendar_resolver)
    if (
        state.query_cutoff != source.query_cutoff
        or state.through_cursor != source.terminal_cursor
        or source.through_execution_cursor != source.terminal_cursor
        or state.journal_source_digest != source.source_digest
        or state.calendar_digest != calendar_digest
        or not state.calendar_release_verified
        or state.policy_digest is None
        or state.settlement_ledger != source.postings
        or source.expected_action_count != len(source.actions)
        or source.expected_posting_count != len(source.postings)
    ):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")


def _strategy_settlement_sources(
    source: JournalActualReplaySource,
) -> tuple[tuple[object, JournalActionSource], ...]:
    actions_by_cursor = {
        action.execution_event_id: action for action in source.actions
    }
    selected: list[tuple[object, JournalActionSource]] = []
    for posting in source.postings:
        expected_account = {
            "BUY": "SETTLED_CASH",
            "FEE": "STRATEGY_FEES",
            "SALE": "SETTLED_CASH",
        }.get(posting.entry_kind)
        expected_sign = {
            "BUY": -1,
            "FEE": -1,
            "SALE": 1,
        }.get(posting.entry_kind)
        if (
            posting.ledger_name != "ACTUAL"
            or expected_account is None
            or posting.account_name != expected_account
            or posting.execution_event_id is None
            or posting.account_check_id is not None
            or (posting.amount_micros > 0) - (posting.amount_micros < 0)
            != expected_sign
        ):
            continue
        action = actions_by_cursor.get(posting.execution_event_id)
        if action is None or action.domain_kind not in {
            "BOUGHT",
            "FEE",
            "PARTIAL_FILL",
            "SOLD",
            "STOP_FILLED",
        }:
            raise RiskBlock("ACTUAL_SETTLEMENT_SOURCE_MISMATCH")
        if (
            posting.entry_kind == "BUY"
            and action.domain_kind not in {"BOUGHT", "PARTIAL_FILL"}
        ) or (
            posting.entry_kind == "FEE" and action.domain_kind != "FEE"
        ) or (
            posting.entry_kind == "SALE"
            and action.domain_kind not in {"SOLD", "STOP_FILLED"}
        ):
            raise RiskBlock("ACTUAL_SETTLEMENT_SOURCE_MISMATCH")
        selected.append((posting, action))
    return tuple(selected)


def _issue_settlement_replay(
    source: JournalActualReplaySource,
    state: object,
    calendar_resolver: SessionCalendarResolver,
) -> SettlementLedger:
    """Issue descriptive actual cash from one exact Journal replay cohort."""
    _validate_journal_actual_replay_cohort(
        source,
        state,
        calendar_resolver,
    )
    from .reconciliation import ActualLedgerState

    assert isinstance(state, ActualLedgerState)
    selected = _strategy_settlement_sources(source)
    postings: list[SettlementPosting] = []
    for posting_source, action in selected:
        amount = money_from_micros(abs(posting_source.amount_micros))
        kind = posting_source.entry_kind
        session_date = posting_source.occurred_at.astimezone(_ET).date()
        if kind in {"BUY", "FEE"}:
            available_on = session_date
            reason_code = None
        else:
            try:
                available_on = calendar_resolver.add_sessions(session_date, 1)
                reason_code = None
            except RiskBlock as error:
                if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                    raise
                available_on = None
                reason_code = error.reason_code
        postings.append(
            SettlementPosting(
                kind=kind,
                amount=amount,
                at=posting_source.occurred_at,
                available_on=available_on,
                reason_code=reason_code,
                posting_id=f"settlement:{action.event_id}",
                cursor=action.execution_event_id,
                ordinal=action.action_ordinal,
                source_event_id=action.event_id,
                message_time=action.message_time,
                received_at=action.received_at,
            )
        )
    cutoff_day = source.query_cutoff.astimezone(_ET).date()
    initial_cash_micros = state.strategy_settled_cash_micros
    for posting in postings:
        if posting.source_received_at > source.query_cutoff:
            raise RiskBlock("ACTUAL_SETTLEMENT_LOOKAHEAD")
        amount_micros = money_to_micros(posting.amount)
        if posting.kind in {"BUY", "FEE"}:
            initial_cash_micros += amount_micros
        elif (
            posting.available_on is not None
            and posting.available_on <= cutoff_day
        ):
            initial_cash_micros -= amount_micros
    if not 0 <= initial_cash_micros <= MAX_MICRODOLLARS:
        raise RiskBlock("INVALID_INITIAL_SETTLED_CASH")
    initialized_at = min(
        (posting.at for posting in postings),
        default=min(
            (action.event_time for action in source.actions),
            default=source.query_cutoff,
        ),
    )
    replay_authority = SettlementReplayAuthority(
        query_cutoff=source.query_cutoff,
        through_execution_cursor=source.through_execution_cursor,
        physical_source_highwater_cursor=source.source_through_cursor,
        journal_source_digest=source.source_digest,
        actual_state_digest=state.source_digest,
        calendar_digest=_calendar_digest(calendar_resolver),
        policy_digest=state.policy_digest,
        expected_action_count=source.expected_action_count,
        expected_posting_count=source.expected_posting_count,
        strategy_posting_count=len(postings),
    )
    ledger = SettlementLedger(
        initial_settled_cash=money_from_micros(initial_cash_micros),
        initialized_at=initialized_at,
        calendar_resolver=calendar_resolver,
        postings=tuple(postings),
        replay_authority=replay_authority,
    )
    _install_risk_authority(
        _SETTLEMENT_LEDGER_AUTHORITIES,
        ledger,
        exact_type=SettlementLedger,
        journal_binding=(source, "JOURNAL_REPLAY_SOURCE"),
    )
    return ledger


def _issue_actual_entry_authorities(
    *,
    source: JournalActualReplaySource,
    state: object,
    calendar_resolver: SessionCalendarResolver,
) -> None:
    """Task 7 validates replay inputs but cannot issue paired entry authority."""
    _validate_journal_actual_replay_cohort(
        source,
        state,
        calendar_resolver,
    )
    raise RiskBlock("ACTUAL_BREAKER_SOURCE_UNAVAILABLE")


@dataclass(frozen=True, slots=True)
class Position:
    """Frozen live or canonical position; ``entry`` is the actual fill."""

    signal_id: str
    symbol: str
    entry: Decimal
    shares: int
    initial_stop: Decimal
    recommended_stop: Decimal
    user_confirmed_stop: Decimal | None
    target: Decimal
    tick_size: Decimal
    entered_session: date
    ledger_name: str = "ACTUAL"
    profit_target_taken: bool = False

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise RiskBlock("INVALID_POSITION_SIGNAL")
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise RiskBlock("INVALID_POSITION_SYMBOL")
        object.__setattr__(
            self,
            "entry",
            _require_money(
                self.entry,
                reason_code="INVALID_POSITION_ENTRY",
                positive=True,
            ),
        )
        _require_positive_int(self.shares, "INVALID_POSITION_SHARES")
        for attribute, code in (
            ("initial_stop", "INVALID_INITIAL_STOP"),
            ("recommended_stop", "INVALID_RECOMMENDED_STOP"),
            ("target", "INVALID_POSITION_TARGET"),
            ("tick_size", "INVALID_TICK_SIZE"),
        ):
            object.__setattr__(
                self,
                attribute,
                _require_money(
                    getattr(self, attribute),
                    reason_code=code,
                    positive=True,
                ),
            )
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _require_money(
                    self.user_confirmed_stop,
                    reason_code="INVALID_USER_CONFIRMED_STOP",
                    positive=True,
                ),
            )
        if self.initial_stop >= self.entry:
            raise RiskBlock("NON_POSITIVE_INITIAL_RISK")
        if self.recommended_stop < self.initial_stop:
            raise RiskBlock("RECOMMENDED_STOP_WIDENED")
        if not _is_tick_aligned(self.initial_stop, self.tick_size):
            raise RiskBlock("INITIAL_STOP_NOT_TICK_ALIGNED")
        if not _is_tick_aligned(self.recommended_stop, self.tick_size):
            raise RiskBlock("RECOMMENDED_STOP_NOT_TICK_ALIGNED")
        if not _is_tick_aligned(self.target, self.tick_size):
            raise RiskBlock("TARGET_NOT_TICK_ALIGNED")
        if type(self.entered_session) is not date:
            raise RiskBlock("INVALID_ENTRY_SESSION")
        if self.ledger_name not in {"ACTUAL", "CANONICAL"}:
            raise RiskBlock("INVALID_POSITION_LEDGER")
        if type(self.profit_target_taken) is not bool:
            raise RiskBlock("INVALID_PROFIT_TARGET_STATE")


def _position_revision_digest(position: Position) -> str:
    payload = {
        "version": 1,
        "ledger_name": position.ledger_name,
        "signal_id": position.signal_id,
        "symbol": position.symbol,
        "entry_micros": money_to_micros(position.entry),
        "shares": position.shares,
        "initial_stop_micros": money_to_micros(position.initial_stop),
        "recommended_stop_micros": money_to_micros(position.recommended_stop),
        "user_confirmed_stop_micros": (
            None
            if position.user_confirmed_stop is None
            else money_to_micros(position.user_confirmed_stop)
        ),
        "target_micros": money_to_micros(position.target),
        "tick_size_micros": money_to_micros(position.tick_size),
        "entered_session": position.entered_session.isoformat(),
        "profit_target_taken": position.profit_target_taken,
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _phase1_evidence_sessions(
    *,
    start: date,
    terminal: date,
    calendar_resolver: SessionCalendarResolver,
) -> tuple[date, ...]:
    if (
        type(start) is not date
        or type(terminal) is not date
        or start > terminal
        or not calendar_resolver.is_open(start)
        or not calendar_resolver.is_open(terminal)
    ):
        raise RiskBlock("PHASE1_EVIDENCE_WINDOW_INVALID")
    result: list[date] = []
    current = start
    while current <= terminal:
        if calendar_resolver.is_open(current):
            result.append(current)
        current += timedelta(days=1)
    if not result or result[0] != start or result[-1] != terminal:
        raise RiskBlock("PHASE1_EVIDENCE_WINDOW_INVALID")
    return tuple(result)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1SignalEvidenceAuthority:
    """Exact reviewed event/thesis truth for one persisted Phase 1 signal."""

    signal_source: object = field(repr=False, compare=False)
    reviewed_bundle: ReviewedEvidenceBundle = field(repr=False, compare=False)
    evidence_decision: EvidenceDecision = field(repr=False, compare=False)
    signal_id: str
    validation_window_id: str
    symbol: str
    role: str
    publication_session: date
    subject_kind: str
    issuer_cik: str | None
    review_at: datetime
    terminal_hold_session: date
    remaining_sessions: tuple[date, ...]
    event_exit_required: bool | None
    thesis_invalidated: bool | None
    status: str
    reason_codes: tuple[str, ...]
    relevant_events: tuple[tuple[date, str | None], ...]
    adverse_tags: tuple[str, ...]
    source_observation_ids: tuple[str, ...]
    registry_id: str
    registry_content_hash: str
    bundle_digest: str
    decision_digest: str
    signal_source_digest: str
    calendar_digest: str
    source_digest: str

    def __post_init__(self) -> None:
        if (
            type(self.signal_id) is not str
            or not self.signal_id
            or type(self.validation_window_id) is not str
            or not _is_sha256_digest(self.validation_window_id)
            or type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
            or self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}
            or type(self.publication_session) is not date
            or self.subject_kind not in {"STOCK", "ETF"}
            or (
                self.subject_kind == "STOCK"
                and (
                    type(self.issuer_cik) is not str
                    or len(self.issuer_cik) != 10
                    or not self.issuer_cik.isdigit()
                )
            )
            or (self.subject_kind == "ETF" and self.issuer_cik is not None)
        ):
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        _require_aware(
            self.review_at,
            "INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY",
        )
        sessions = tuple(self.remaining_sessions)
        object.__setattr__(self, "remaining_sessions", sessions)
        if (
            not sessions
            or any(type(day) is not date for day in sessions)
            or sessions != tuple(sorted(set(sessions)))
            or sessions[-1] != self.terminal_hold_session
            or sessions[0] != self.review_at.astimezone(_ET).date()
        ):
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        flags = (self.event_exit_required, self.thesis_invalidated)
        if any(
            value is not None and type(value) is not bool
            for value in flags
        ):
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        if (
            (self.status == "CLEAR" and flags != (False, False))
            or (
                self.status == "EXIT_REQUIRED"
                and True not in flags
            )
            or (
                self.status == "UNRESOLVED"
                and (True in flags or None not in flags)
            )
            or self.status not in {"CLEAR", "EXIT_REQUIRED", "UNRESOLVED"}
        ):
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        for attribute in (
            "reason_codes",
            "adverse_tags",
            "source_observation_ids",
        ):
            values = tuple(getattr(self, attribute))
            object.__setattr__(self, attribute, values)
            if (
                len(values) != len(set(values))
                or any(type(value) is not str or not value for value in values)
            ):
                raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        events = tuple(self.relevant_events)
        object.__setattr__(self, "relevant_events", events)
        if any(
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not date
            or item[0] < sessions[0]
            or item[0] > sessions[-1]
            or (item[1] is not None and type(item[1]) is not str)
            for item in events
        ):
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        for digest in (
            self.registry_content_hash,
            self.bundle_digest,
            self.decision_digest,
            self.signal_source_digest,
            self.calendar_digest,
            self.source_digest,
        ):
            if not _is_sha256_digest(digest):
                raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")
        if type(self.registry_id) is not str or not self.registry_id:
            raise RiskBlock("INVALID_PHASE1_SIGNAL_EVIDENCE_AUTHORITY")


def _phase1_signal_evidence_document(
    authority: Phase1SignalEvidenceAuthority,
) -> dict[str, object]:
    def instant(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    return {
        "adverse_tags": list(authority.adverse_tags),
        "bundle_digest": authority.bundle_digest,
        "calendar_digest": authority.calendar_digest,
        "decision_digest": authority.decision_digest,
        "event_exit_required": authority.event_exit_required,
        "issuer_cik": authority.issuer_cik,
        "publication_session": authority.publication_session.isoformat(),
        "reason_codes": list(authority.reason_codes),
        "registry_content_hash": authority.registry_content_hash,
        "registry_id": authority.registry_id,
        "relevant_events": [
            [day.isoformat(), event_type]
            for day, event_type in authority.relevant_events
        ],
        "remaining_sessions": [
            day.isoformat() for day in authority.remaining_sessions
        ],
        "review_at": instant(authority.review_at),
        "role": authority.role,
        "signal_id": authority.signal_id,
        "signal_source_digest": authority.signal_source_digest,
        "source_observation_ids": list(authority.source_observation_ids),
        "status": authority.status,
        "subject_kind": authority.subject_kind,
        "symbol": authority.symbol,
        "terminal_hold_session": authority.terminal_hold_session.isoformat(),
        "thesis_invalidated": authority.thesis_invalidated,
        "validation_window_id": authority.validation_window_id,
        "version": 1,
    }


def _phase1_signal_evidence_bytes(
    authority: Phase1SignalEvidenceAuthority,
) -> bytes:
    return json.dumps(
        _phase1_signal_evidence_document(authority),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _phase1_signal_evidence_fingerprint(
    authority: Phase1SignalEvidenceAuthority,
) -> tuple[object, ...]:
    return (
        authority.signal_id,
        authority.validation_window_id,
        authority.symbol,
        authority.role,
        authority.publication_session,
        authority.subject_kind,
        authority.issuer_cik,
        authority.review_at,
        authority.terminal_hold_session,
        authority.remaining_sessions,
        authority.event_exit_required,
        authority.thesis_invalidated,
        authority.status,
        authority.reason_codes,
        authority.relevant_events,
        authority.adverse_tags,
        authority.source_observation_ids,
        authority.registry_id,
        authority.registry_content_hash,
        authority.bundle_digest,
        authority.decision_digest,
        authority.signal_source_digest,
        authority.calendar_digest,
        authority.source_digest,
    )


def _build_phase1_signal_evidence_authority(
    signal_source: object,
    reviewed_bundle: object,
    decision: object,
    *,
    review_at: datetime,
    calendar_resolver: SessionCalendarResolver,
) -> Phase1SignalEvidenceAuthority:
    """Reclassify one exact reviewed bundle for a persisted signal horizon."""
    from .journal import (
        Phase1SignalSource,
        is_verified_phase1_signal_source,
    )

    if not isinstance(signal_source, Phase1SignalSource) or not (
        is_verified_phase1_signal_source(signal_source)
    ):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if not _is_reviewed_bundle(reviewed_bundle) or not (
        is_reviewed_evidence_decision(decision)
    ):
        raise RiskBlock("PHASE1_REVIEWED_EVIDENCE_UNVERIFIED")
    assert isinstance(reviewed_bundle, ReviewedEvidenceBundle)
    assert isinstance(decision, EvidenceDecision)
    if decision._reviewed_bundle is not reviewed_bundle:
        raise RiskBlock("PHASE1_EVIDENCE_BUNDLE_DECISION_SPLICE")
    review_at = _require_aware(
        review_at,
        "INVALID_PHASE1_EVIDENCE_REVIEW_TIME",
    )
    subject_kind = getattr(signal_source, "subject_kind", None)
    issuer_cik = getattr(signal_source, "issuer_cik", None)
    calendar_digest = _calendar_digest(calendar_resolver)
    review_session = review_at.astimezone(_ET).date()
    terminal = calendar_resolver.add_sessions(
        signal_source.publication_session,
        MAX_HOLD_SESSIONS - 1,
    )
    remaining_sessions = _phase1_evidence_sessions(
        start=review_session,
        terminal=terminal,
        calendar_resolver=calendar_resolver,
    )
    if (
        subject_kind not in {"STOCK", "ETF"}
        or reviewed_bundle.subject_kind != subject_kind
        or reviewed_bundle.symbol != signal_source.symbol
        or reviewed_bundle.issuer_cik != issuer_cik
        or decision.subject_kind != subject_kind
        or decision.symbol != signal_source.symbol
        or decision.issuer_cik != issuer_cik
        or decision.as_of != review_at
        or signal_source.calendar_digest != calendar_digest
        or signal_source.received_at > review_at
        or review_at > signal_source.query_cutoff
    ):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_LINEAGE_MISMATCH")
    exact_decision = classify_evidence(
        reviewed_bundle.records,
        DateRange(review_session, terminal),
        symbol=signal_source.symbol,
        issuer_cik=issuer_cik,
        source_bindings=reviewed_bundle.source_bindings,
        as_of=review_at,
        subject_kind=subject_kind,
        coverage_attestations=reviewed_bundle.coverage_attestations,
        reviewed_bundle=reviewed_bundle,
    )
    if (
        not is_reviewed_evidence_decision(exact_decision)
        or decision != exact_decision
        or decision._decision_digest != exact_decision._decision_digest
    ):
        raise RiskBlock("PHASE1_EVIDENCE_DECISION_WINDOW_MISMATCH")

    event_values = (
        decision.binary_events
        if subject_kind == "STOCK"
        else decision.etf_actions
    )
    relevant_events = tuple(
        item
        for item in event_values
        if review_session <= item[0] <= terminal
    )
    relevant_coverage = (
        decision.binary_event_coverage
        if subject_kind == "STOCK"
        else decision.etf_action_coverage
    )
    evidence_integrity_clear = (
        decision.health == "HEALTHY"
        and not decision.ambiguities
        and not decision.conflicts
        and decision.block_reason
        not in {
            "EVIDENCE_COVERAGE_ATTESTATION_MISSING",
            "EVIDENCE_PRODUCT_COVERAGE_INVALID",
            "EVIDENCE_EVENT_COVERAGE_CONFLICT",
        }
    )
    if not evidence_integrity_clear:
        event_exit_required: bool | None = None
        thesis_invalidated: bool | None = None
    else:
        if relevant_events:
            event_exit_required = True
        elif relevant_coverage == "CONFIRMED_CLEAR":
            event_exit_required = False
        else:
            event_exit_required = None
        thesis_invalidated = bool(decision.adverse_tags)

    if True in (event_exit_required, thesis_invalidated):
        status = "EXIT_REQUIRED"
    elif (event_exit_required, thesis_invalidated) == (False, False):
        status = "CLEAR"
    else:
        status = "UNRESOLVED"
    reasons: list[str] = []
    if event_exit_required is True:
        reasons.append("EVENT_EXIT_REQUIRED")
    if thesis_invalidated is True:
        reasons.append("THESIS_INVALIDATED")
    if status == "UNRESOLVED":
        reasons.append(decision.block_reason or "EVIDENCE_STATUS_UNRESOLVED")
    reason_codes = tuple(dict.fromkeys(reasons))
    bundle_digest = reviewed_bundle._bundle_digest
    decision_digest = decision._decision_digest
    if not _is_sha256_digest(bundle_digest) or not _is_sha256_digest(
        decision_digest
    ):
        raise RiskBlock("PHASE1_REVIEWED_EVIDENCE_UNVERIFIED")
    authority = Phase1SignalEvidenceAuthority(
        signal_source=signal_source,
        reviewed_bundle=reviewed_bundle,
        evidence_decision=decision,
        signal_id=signal_source.signal_id,
        validation_window_id=signal_source.validation_window_id,
        symbol=signal_source.symbol,
        role=signal_source.role,
        publication_session=signal_source.publication_session,
        subject_kind=subject_kind,
        issuer_cik=issuer_cik,
        review_at=review_at,
        terminal_hold_session=terminal,
        remaining_sessions=remaining_sessions,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        status=status,
        reason_codes=reason_codes,
        relevant_events=relevant_events,
        adverse_tags=decision.adverse_tags,
        source_observation_ids=decision.source_observation_ids,
        registry_id=reviewed_bundle.registry_id,
        registry_content_hash=reviewed_bundle.content_hash,
        bundle_digest=bundle_digest,
        decision_digest=decision_digest,
        signal_source_digest=signal_source.source_digest,
        calendar_digest=calendar_digest,
        source_digest="0" * 64,
    )
    source_digest = sha256(_phase1_signal_evidence_bytes(authority)).hexdigest()
    authority = replace(authority, source_digest=source_digest)
    return authority


def _issue_phase1_signal_evidence_authority(
    signal_source: object,
    reviewed_bundle: object,
    decision: object,
    *,
    review_at: datetime,
    calendar_resolver: SessionCalendarResolver,
) -> Phase1SignalEvidenceAuthority:
    authority = _build_phase1_signal_evidence_authority(
        signal_source,
        reviewed_bundle,
        decision,
        review_at=review_at,
        calendar_resolver=calendar_resolver,
    )
    _install_risk_authority(
        _PHASE1_SIGNAL_EVIDENCE_AUTHORITIES,
        authority,
        exact_type=Phase1SignalEvidenceAuthority,
        children=(reviewed_bundle, decision, signal_source),
        phase1_bindings=((signal_source, "SIGNAL_SOURCE"),),
    )
    return authority


def _issue_phase1_signal_evidence_authority_from_source(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
) -> Phase1SignalEvidenceAuthority:
    """Reissue reviewed signal truth from one exact persisted Journal source."""
    from .evidence import (
        EvidenceRegistryError,
        EvidenceUnavailableError,
        _issue_reviewed_bundle_from_phase1_source,
    )
    from .journal import (
        Phase1SignalEvidenceSource,
        is_verified_phase1_signal_evidence_source,
        phase1_sources_share_owner,
    )

    if not isinstance(source, Phase1SignalEvidenceSource) or not (
        is_verified_phase1_signal_evidence_source(source)
    ):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    signal_source = source.signal_source
    if (
        source.calendar_digest != _calendar_digest(calendar_resolver)
        or source.calendar_digest != signal_source.calendar_digest
        or source.review_at > source.query_cutoff
        or signal_source.query_cutoff < source.query_cutoff
        or not phase1_sources_share_owner(source, signal_source)
    ):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_SOURCE_LINEAGE_MISMATCH")
    try:
        reviewed_bundle = _issue_reviewed_bundle_from_phase1_source(source)
    except (EvidenceRegistryError, EvidenceUnavailableError) as error:
        raise RiskBlock("PHASE1_REVIEWED_EVIDENCE_UNVERIFIED") from error
    decision = source.evidence_decision
    if not is_reviewed_evidence_decision(decision):
        raise RiskBlock("PHASE1_REVIEWED_EVIDENCE_UNVERIFIED")
    authority = _build_phase1_signal_evidence_authority(
        signal_source,
        reviewed_bundle,
        decision,
        review_at=source.review_at,
        calendar_resolver=calendar_resolver,
    )
    try:
        historical_manifest = json.loads(source.manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_MANIFEST_MISMATCH") from error
    if not isinstance(historical_manifest, dict):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_MANIFEST_MISMATCH")
    historical_signal_digest = historical_manifest.get(
        "signal_source_digest"
    )
    if not _is_sha256_digest(historical_signal_digest):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_MANIFEST_MISMATCH")
    expected_historical_manifest = _phase1_signal_evidence_document(authority)
    expected_historical_manifest["signal_source_digest"] = (
        historical_signal_digest
    )
    expected_historical_bytes = json.dumps(
        expected_historical_manifest,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if (
        source.registry_id != authority.registry_id
        or source.registry_content_hash != authority.registry_content_hash
        or source.release_sha256 != authority.registry_content_hash
        or source.bundle_digest != authority.bundle_digest
        or source.decision_digest != authority.decision_digest
        or source.manifest_bytes != expected_historical_bytes
        or sha256(source.manifest_bytes).hexdigest()
        != source.manifest_digest
    ):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_MANIFEST_MISMATCH")
    _install_risk_authority(
        _PHASE1_SIGNAL_EVIDENCE_AUTHORITIES,
        authority,
        exact_type=Phase1SignalEvidenceAuthority,
        children=(reviewed_bundle, decision, signal_source),
        phase1_bindings=((source, "SIGNAL_EVIDENCE"),),
    )
    return authority


def is_issued_phase1_signal_evidence_authority(value: object) -> bool:
    if type(value) is not Phase1SignalEvidenceAuthority:
        return False
    try:
        # Source, evidence, and datetime verifiers may dispatch callbacks.
        # Complete them before the pure source/child/root authority checks.
        if not (
            _phase1_derived_sources_are_current(value)
            and _is_reviewed_bundle(value.reviewed_bundle)
            and is_reviewed_evidence_decision(value.evidence_decision)
            and value.evidence_decision._reviewed_bundle
            is value.reviewed_bundle
            and value.bundle_digest == value.reviewed_bundle._bundle_digest
            and value.decision_digest
            == value.evidence_decision._decision_digest
            and value.signal_source_digest
            == getattr(value.signal_source, "source_digest", None)
            and sha256(_phase1_signal_evidence_bytes(value)).hexdigest()
            == value.source_digest
        ):
            return False
        if not _phase1_derived_sources_are_current_without_callbacks(value):
            return False
        return _is_current_risk_authority_without_callbacks(
            _PHASE1_SIGNAL_EVIDENCE_AUTHORITIES,
            value,
            exact_type=Phase1SignalEvidenceAuthority,
            children=(
                value.reviewed_bundle,
                value.evidence_decision,
                value.signal_source,
            ),
        )
    except Exception:
        return False


def phase1_signal_evidence_manifest(
    authority: object,
) -> bytes:
    """Return immutable canonical persistence bytes for one current authority."""
    if not is_issued_phase1_signal_evidence_authority(authority):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_AUTHORITY_UNVERIFIED")
    assert isinstance(authority, Phase1SignalEvidenceAuthority)
    return _phase1_signal_evidence_bytes(authority)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1PositionEvidenceAuthority:
    """Position-revision wrapper over one exact signal evidence authority."""

    signal_evidence: Phase1SignalEvidenceAuthority = field(
        repr=False,
        compare=True,
    )
    position: Position
    position_digest: str
    source_digest: str

    @property
    def subject_kind(self) -> str:
        return self.signal_evidence.subject_kind

    @property
    def symbol(self) -> str:
        return self.signal_evidence.symbol

    @property
    def issuer_cik(self) -> str | None:
        return self.signal_evidence.issuer_cik

    @property
    def review_at(self) -> datetime:
        return self.signal_evidence.review_at

    @property
    def entered_session(self) -> date:
        return self.position.entered_session

    @property
    def terminal_hold_session(self) -> date:
        return self.signal_evidence.terminal_hold_session

    @property
    def remaining_sessions(self) -> tuple[date, ...]:
        return self.signal_evidence.remaining_sessions

    @property
    def event_exit_required(self) -> bool | None:
        return self.signal_evidence.event_exit_required

    @property
    def thesis_invalidated(self) -> bool | None:
        return self.signal_evidence.thesis_invalidated

    @property
    def status(self) -> str:
        return self.signal_evidence.status

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return self.signal_evidence.reason_codes

    @property
    def relevant_events(self) -> tuple[tuple[date, str | None], ...]:
        return self.signal_evidence.relevant_events

    @property
    def adverse_tags(self) -> tuple[str, ...]:
        return self.signal_evidence.adverse_tags

    @property
    def source_observation_ids(self) -> tuple[str, ...]:
        return self.signal_evidence.source_observation_ids

    @property
    def registry_id(self) -> str:
        return self.signal_evidence.registry_id

    @property
    def registry_content_hash(self) -> str:
        return self.signal_evidence.registry_content_hash

    @property
    def bundle_digest(self) -> str:
        return self.signal_evidence.bundle_digest

    @property
    def decision_digest(self) -> str:
        return self.signal_evidence.decision_digest

    @property
    def calendar_digest(self) -> str:
        return self.signal_evidence.calendar_digest


def _phase1_position_evidence_fingerprint(
    authority: Phase1PositionEvidenceAuthority,
) -> tuple[object, ...]:
    return (
        _phase1_signal_evidence_fingerprint(authority.signal_evidence),
        authority.position,
        authority.position_digest,
        authority.source_digest,
    )


def _issue_phase1_position_evidence_authority(
    signal_evidence: object,
    *,
    position: Position,
) -> Phase1PositionEvidenceAuthority:
    if not is_issued_phase1_signal_evidence_authority(signal_evidence):
        raise RiskBlock("PHASE1_SIGNAL_EVIDENCE_AUTHORITY_UNVERIFIED")
    assert isinstance(signal_evidence, Phase1SignalEvidenceAuthority)
    if not isinstance(position, Position):
        raise TypeError("position evidence authority requires a Position")
    if (
        signal_evidence.role != "PRIMARY"
        or position.ledger_name != "CANONICAL"
        or position.signal_id != signal_evidence.signal_id
        or position.symbol != signal_evidence.symbol
        or position.entered_session != signal_evidence.publication_session
    ):
        raise RiskBlock("PHASE1_POSITION_EVIDENCE_LINEAGE_MISMATCH")
    position_digest = _position_revision_digest(position)
    payload = {
        "entered_session": position.entered_session.isoformat(),
        "position_digest": position_digest,
        "signal_evidence_digest": signal_evidence.source_digest,
        "version": 1,
    }
    source_digest = sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    authority = Phase1PositionEvidenceAuthority(
        signal_evidence=signal_evidence,
        position=position,
        position_digest=position_digest,
        source_digest=source_digest,
    )
    _install_risk_authority(
        _PHASE1_POSITION_EVIDENCE_AUTHORITIES,
        authority,
        exact_type=Phase1PositionEvidenceAuthority,
        children=(signal_evidence,),
        phase1_bindings=_phase1_bound_sources(signal_evidence),
    )
    return authority


def is_issued_phase1_position_evidence_authority(value: object) -> bool:
    if type(value) is not Phase1PositionEvidenceAuthority:
        return False
    try:
        if not (
            type(value.signal_evidence) is Phase1SignalEvidenceAuthority
            and is_issued_phase1_signal_evidence_authority(
                value.signal_evidence
            )
            and value.position_digest
            == _position_revision_digest(value.position)
            and _phase1_derived_sources_are_current(value)
        ):
            return False
        if not (
            _phase1_derived_sources_are_current_without_callbacks(value)
            and _is_current_risk_authority_without_callbacks(
                _PHASE1_SIGNAL_EVIDENCE_AUTHORITIES,
                value.signal_evidence,
                exact_type=Phase1SignalEvidenceAuthority,
                children=(
                    value.signal_evidence.reviewed_bundle,
                    value.signal_evidence.evidence_decision,
                    value.signal_evidence.signal_source,
                ),
            )
        ):
            return False
        return _is_current_risk_authority_without_callbacks(
            _PHASE1_POSITION_EVIDENCE_AUTHORITIES,
            value,
            exact_type=Phase1PositionEvidenceAuthority,
            children=(value.signal_evidence,),
        )
    except Exception:
        return False


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PositionEventContext:
    """Ordered event/thesis review facts issued by a later Journal adapter."""

    event_exit_required: bool | None
    thesis_invalidated: bool | None
    evidence_status: str
    at: datetime
    cursor: int
    start_cursor: int
    event_count: int
    ledger_name: str
    signal_id: str
    symbol: str
    position_digest: str
    source_digest: str
    calendar_digest: str
    price: Decimal
    previous_session_low: Decimal | None = None
    current_session_low: Decimal | None = None
    atr14: Decimal | None = None

    def __post_init__(self) -> None:
        if any(
            value is not None and type(value) is not bool
            for value in (
                self.event_exit_required,
                self.thesis_invalidated,
            )
        ):
            raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        if self.evidence_status not in {
            "CLEAR",
            "EXIT_REQUIRED",
            "UNRESOLVED",
            "UNAVAILABLE",
        }:
            raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        if (
            self.evidence_status == "CLEAR"
            and (self.event_exit_required, self.thesis_invalidated)
            != (False, False)
        ) or (
            self.evidence_status == "EXIT_REQUIRED"
            and True
            not in (self.event_exit_required, self.thesis_invalidated)
        ) or (
            self.evidence_status in {"UNRESOLVED", "UNAVAILABLE"}
            and (self.event_exit_required, self.thesis_invalidated)
            == (False, False)
        ):
            raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        _require_aware(self.at, "INVALID_POSITION_EVENT_CONTEXT")
        _require_positive_int(self.cursor, "INVALID_POSITION_EVENT_CURSOR")
        _require_positive_int(
            self.start_cursor,
            "INVALID_POSITION_EVENT_CURSOR",
        )
        if self.start_cursor > self.cursor:
            raise RiskBlock("INVALID_POSITION_EVENT_CURSOR")
        _require_nonnegative_int(
            self.event_count,
            "INVALID_POSITION_EVENT_COUNT",
        )
        if self.ledger_name not in {"ACTUAL", "CANONICAL"}:
            raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        if (
            type(self.signal_id) is not str
            or not self.signal_id
            or type(self.symbol) is not str
            or not self.symbol
        ):
            raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        for digest in (
            self.position_digest,
            self.source_digest,
            self.calendar_digest,
        ):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise RiskBlock("INVALID_POSITION_EVENT_CONTEXT")
        object.__setattr__(
            self,
            "price",
            _require_money(
                self.price,
                reason_code="INVALID_MARK_PRICE",
                positive=True,
            ),
        )
        trailing_values = (
            self.previous_session_low,
            self.current_session_low,
            self.atr14,
        )
        if any(value is not None for value in trailing_values) and not all(
            value is not None for value in trailing_values
        ):
            raise RiskBlock("INCOMPLETE_TRAILING_STOP_CONTEXT")
        for attribute in (
            "previous_session_low",
            "current_session_low",
            "atr14",
        ):
            value = getattr(self, attribute)
            if value is not None:
                object.__setattr__(
                    self,
                    attribute,
                    _require_money(
                        value,
                        reason_code="INVALID_TRAILING_STOP_CONTEXT",
                        positive=True,
                    ),
                )


def _position_event_context_fingerprint(
    context: PositionEventContext,
) -> tuple[object, ...]:
    return (
        context.event_exit_required,
        context.thesis_invalidated,
        context.evidence_status,
        context.at,
        context.cursor,
        context.start_cursor,
        context.event_count,
        context.ledger_name,
        context.signal_id,
        context.symbol,
        context.position_digest,
        context.source_digest,
        context.calendar_digest,
        context.price,
        context.previous_session_low,
        context.current_session_low,
        context.atr14,
    )


def _issue_position_event_context(
    *,
    position: Position,
    event_exit_required: bool | None,
    thesis_invalidated: bool | None,
    evidence_status: str | None = None,
    at: datetime,
    cursor: int,
    start_cursor: int,
    event_count: int,
    calendar_resolver: SessionCalendarResolver,
    price: Decimal,
    previous_session_low: Decimal | None = None,
    current_session_low: Decimal | None = None,
    atr14: Decimal | None = None,
) -> PositionEventContext:
    """Build a diagnostic position-event context without granting authority.

    Task 6 has no persisted source-row cohort capable of proving that the
    supplied event/thesis facts are complete through the review watermark.
    A later journal adapter may verify those rows and register the exact DTO;
    this field-only compatibility builder intentionally never does so.
    """
    if not isinstance(position, Position):
        raise TypeError("position event context requires a Position")
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    at = _require_aware(at, "INVALID_POSITION_EVENT_CONTEXT")
    session_date = at.astimezone(_ET).date()
    try:
        session = calendar_resolver.session(session_date)
    except RiskBlock:
        raise RiskBlock("POSITION_REVIEW_OUTSIDE_WINDOW") from None
    review_clock = at.astimezone(_ET).time().replace(tzinfo=None)
    if not session.review_time <= review_clock <= session.close_time:
        raise RiskBlock("POSITION_REVIEW_OUTSIDE_WINDOW")
    calendar_digest = _calendar_digest(calendar_resolver)
    position_digest = _position_revision_digest(position)
    if evidence_status is None:
        if True in (event_exit_required, thesis_invalidated):
            evidence_status = "EXIT_REQUIRED"
        elif (event_exit_required, thesis_invalidated) == (False, False):
            evidence_status = "CLEAR"
        else:
            evidence_status = "UNRESOLVED"
    source_payload = {
        "version": 1,
        "ledger_name": position.ledger_name,
        "signal_id": position.signal_id,
        "symbol": position.symbol,
        "position_digest": position_digest,
        "event_exit_required": event_exit_required,
        "thesis_invalidated": thesis_invalidated,
        "evidence_status": evidence_status,
        "complete_through": at.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "start_cursor": start_cursor,
        "through_cursor": cursor,
        "event_count": event_count,
        "price_micros": money_to_micros(price),
        "previous_session_low_micros": (
            None
            if previous_session_low is None
            else money_to_micros(previous_session_low)
        ),
        "current_session_low_micros": (
            None
            if current_session_low is None
            else money_to_micros(current_session_low)
        ),
        "atr14_micros": None if atr14 is None else money_to_micros(atr14),
        "calendar_digest": calendar_digest,
    }
    source_digest = sha256(
        json.dumps(
            source_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    context = PositionEventContext(
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        evidence_status=evidence_status,
        at=at,
        cursor=cursor,
        start_cursor=start_cursor,
        event_count=event_count,
        ledger_name=position.ledger_name,
        signal_id=position.signal_id,
        symbol=position.symbol,
        position_digest=position_digest,
        source_digest=source_digest,
        calendar_digest=calendar_digest,
        price=price,
        previous_session_low=previous_session_low,
        current_session_low=current_session_low,
        atr14=atr14,
    )
    return context


def is_issued_position_event_context(context: object) -> bool:
    if type(context) is not PositionEventContext:
        return False
    if not (
        _phase1_derived_sources_are_current(context)
        and _phase1_derived_sources_are_current_without_callbacks(context)
    ):
        return False
    bound_sources = _phase1_bound_sources(context)
    if len(bound_sources) != 1:
        return False
    return _is_current_risk_authority_without_callbacks(
        _POSITION_EVENT_AUTHORITIES,
        context,
        exact_type=PositionEventContext,
        children=(bound_sources[0][0],),
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class MarketMark:
    """Validated mark plus explicit session and event-exit authority.

    ``holding_sessions`` is one-based and includes the entry session; a value of
    ten therefore triggers the fixed ten-session exit.
    """

    price: Decimal
    at: datetime
    holding_sessions: int
    previous_session_low: Decimal | None = None
    current_session_low: Decimal | None = None
    atr14: Decimal | None = None
    event_exit_required: bool | None = False
    thesis_invalidated: bool | None = False
    event_evidence_status: str = "CLEAR"
    context_verified: bool = True
    holding_sessions_verified: bool = False
    ledger_name: str | None = None
    signal_id: str | None = None
    symbol: str | None = None
    position_digest: str | None = None
    event_context_digest: str | None = None
    calendar_digest: str | None = None
    review_cursor: int | None = None
    _session_authority: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "price",
            _require_money(
                self.price,
                reason_code="INVALID_MARK_PRICE",
                positive=True,
            ),
        )
        _require_aware(self.at, "INVALID_MARK_TIME")
        _require_positive_int(self.holding_sessions, "INVALID_HOLDING_SESSION_COUNT")
        trailing_values = (
            self.previous_session_low,
            self.current_session_low,
            self.atr14,
        )
        if any(value is not None for value in trailing_values) and not all(
            value is not None for value in trailing_values
        ):
            raise RiskBlock("INCOMPLETE_TRAILING_STOP_CONTEXT")
        for attribute in (
            "previous_session_low",
            "current_session_low",
            "atr14",
        ):
            value = getattr(self, attribute)
            if value is not None:
                object.__setattr__(
                    self,
                    attribute,
                    _require_money(
                        value,
                        reason_code="INVALID_TRAILING_STOP_CONTEXT",
                        positive=True,
                    ),
                )
        if any(
            value is not None and type(value) is not bool
            for value in (
                self.event_exit_required,
                self.thesis_invalidated,
            )
        ) or any(
            type(value) is not bool
            for value in (
                self.context_verified,
                self.holding_sessions_verified,
            )
        ):
            raise RiskBlock("INVALID_POSITION_CONTEXT")
        if self.event_evidence_status not in {
            "CLEAR",
            "EXIT_REQUIRED",
            "UNRESOLVED",
            "UNAVAILABLE",
        }:
            raise RiskBlock("INVALID_POSITION_CONTEXT")
        lineage = (
            self.ledger_name,
            self.signal_id,
            self.symbol,
            self.position_digest,
            self.event_context_digest,
            self.calendar_digest,
            self.review_cursor,
        )
        if any(value is not None for value in lineage):
            if (
                self.ledger_name not in {"ACTUAL", "CANONICAL"}
                or type(self.signal_id) is not str
                or not self.signal_id
                or type(self.symbol) is not str
                or not self.symbol
                or self.review_cursor is None
            ):
                raise RiskBlock("INVALID_MARK_LINEAGE")
            _require_positive_int(self.review_cursor, "INVALID_MARK_LINEAGE")
            for digest in (
                self.position_digest,
                self.event_context_digest,
                self.calendar_digest,
            ):
                if (
                    type(digest) is not str
                    or len(digest) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in digest
                    )
                ):
                    raise RiskBlock("INVALID_MARK_LINEAGE")


def _market_mark_fingerprint(mark: MarketMark) -> tuple[object, ...]:
    return (
        mark.price,
        mark.at,
        mark.holding_sessions,
        mark.previous_session_low,
        mark.current_session_low,
        mark.atr14,
        mark.event_exit_required,
        mark.thesis_invalidated,
        mark.event_evidence_status,
        mark.context_verified,
        mark.holding_sessions_verified,
        mark.ledger_name,
        mark.signal_id,
        mark.symbol,
        mark.position_digest,
        mark.event_context_digest,
        mark.calendar_digest,
        mark.review_cursor,
    )


def is_issued_market_mark(mark: object) -> bool:
    if type(mark) is not MarketMark:
        return False
    children = _registered_risk_authority_children(_MARK_AUTHORITIES, mark)
    if (
        children is None
        or len(children) != 1
        or type(children[0]) is not PositionEventContext
        or not is_issued_position_event_context(children[0])
        or not _is_current_risk_authority_without_callbacks(
            _POSITION_EVENT_AUTHORITIES,
            children[0],
            exact_type=PositionEventContext,
            children=tuple(
                source
                for source, _kind in _phase1_bound_sources(children[0])
            ),
        )
        or not _phase1_derived_sources_are_current(mark)
    ):
        return False
    if _phase1_bound_sources(mark) and not (
        _phase1_derived_sources_are_current_without_callbacks(mark)
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _MARK_AUTHORITIES,
        mark,
        exact_type=MarketMark,
        children=children,
    )


def build_market_mark(
    position: Position,
    *,
    price: Decimal,
    at: datetime,
    calendar_resolver: SessionCalendarResolver,
    event_exit_required: bool | None,
    thesis_invalidated: bool | None,
    event_context_verified: bool,
    event_evidence_status: str | None = None,
    previous_session_low: Decimal | None = None,
    current_session_low: Decimal | None = None,
    atr14: Decimal | None = None,
    position_event_context: PositionEventContext | None = None,
) -> MarketMark:
    """Build a review mark with resolver-derived one-based holding sessions."""
    if not isinstance(position, Position):
        raise TypeError("market mark builder requires a Position")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise TypeError("market mark builder requires calendar authority")
    at = _require_aware(at, "INVALID_MARK_TIME")
    mark_session = at.astimezone(_ET).date()
    release_verified = calendar_resolver.release_verified
    try:
        review_session = calendar_resolver.session(mark_session)
    except RiskBlock:
        review_session = None
    review_clock = at.astimezone(_ET).time().replace(tzinfo=None)
    in_review_window = (
        review_session is not None
        and review_session.review_time
        <= review_clock
        <= review_session.close_time
    )
    holding_sessions = calendar_resolver.count_sessions(
        position.entered_session,
        mark_session,
    )
    issued_event_context = is_issued_position_event_context(
        position_event_context
    )
    if event_evidence_status is None:
        if True in (event_exit_required, thesis_invalidated):
            event_evidence_status = "EXIT_REQUIRED"
        elif (event_exit_required, thesis_invalidated) == (False, False):
            event_evidence_status = "CLEAR"
        else:
            event_evidence_status = "UNRESOLVED"
    if issued_event_context:
        assert position_event_context is not None
        if (
            position_event_context.at != at
            or position_event_context.at.astimezone(_ET).date() != mark_session
            or position_event_context.ledger_name != position.ledger_name
            or position_event_context.signal_id != position.signal_id
            or position_event_context.symbol != position.symbol
            or position_event_context.position_digest
            != _position_revision_digest(position)
            or position_event_context.calendar_digest
            != _calendar_digest(calendar_resolver)
            or position_event_context.event_exit_required
            is not event_exit_required
            or position_event_context.thesis_invalidated
            is not thesis_invalidated
            or position_event_context.evidence_status
            != event_evidence_status
            or position_event_context.price != price
            or position_event_context.previous_session_low
            != previous_session_low
            or position_event_context.current_session_low != current_session_low
            or position_event_context.atr14 != atr14
        ):
            raise RiskBlock("POSITION_EVENT_CONTEXT_MISMATCH")
    operational = (
        release_verified
        and in_review_window
        and event_context_verified
        and issued_event_context
    )
    mark = MarketMark(
        price=price,
        at=at,
        holding_sessions=holding_sessions,
        previous_session_low=previous_session_low,
        current_session_low=current_session_low,
        atr14=atr14,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        event_evidence_status=event_evidence_status,
        context_verified=operational,
        holding_sessions_verified=release_verified,
        ledger_name=position.ledger_name if operational else None,
        signal_id=position.signal_id if operational else None,
        symbol=position.symbol if operational else None,
        position_digest=(
            _position_revision_digest(position) if operational else None
        ),
        event_context_digest=(
            position_event_context.source_digest if operational else None
        ),
        calendar_digest=(
            _calendar_digest(calendar_resolver) if operational else None
        ),
        review_cursor=(position_event_context.cursor if operational else None),
    )
    if operational:
        assert position_event_context is not None
        _install_risk_authority(
            _MARK_AUTHORITIES,
            mark,
            exact_type=MarketMark,
            children=(position_event_context,),
        )
    return mark


@dataclass(frozen=True, slots=True)
class PositionAction:
    status: str
    reason_codes: tuple[str, ...]
    recommended_stop: Decimal
    user_confirmed_stop: Decimal | None
    published_target: Decimal
    shares_to_exit: int
    remaining_shares: int
    r_multiple: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_POSITION_ACTION"),
        )
        if self.status not in {
            "POSITION_UNVERIFIED",
            "RECONCILIATION_REQUIRED",
            "PROVISIONAL_EXIT",
            "PROVISIONAL_HOLD",
            "PROVISIONAL_TIGHTEN_STOP",
            "STOP_UNVERIFIED",
        }:
            raise RiskBlock("INVALID_POSITION_ACTION")
        object.__setattr__(
            self,
            "recommended_stop",
            _require_money(
                self.recommended_stop,
                reason_code="INVALID_RECOMMENDED_STOP",
                positive=True,
            ),
        )
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _require_money(
                    self.user_confirmed_stop,
                    reason_code="INVALID_USER_CONFIRMED_STOP",
                    positive=True,
                ),
            )
        object.__setattr__(
            self,
            "published_target",
            _require_money(
                self.published_target,
                reason_code="INVALID_POSITION_TARGET",
                positive=True,
            ),
        )
        _require_nonnegative_int(self.shares_to_exit, "INVALID_EXIT_SHARES")
        _require_nonnegative_int(self.remaining_shares, "INVALID_REMAINING_SHARES")
        r_multiple = _require_decimal(
            self.r_multiple,
            reason_code="INVALID_R_MULTIPLE",
        )
        if abs(r_multiple) > Decimal(MAX_MICRODOLLARS):
            raise RiskBlock("INVALID_R_MULTIPLE")
        if r_multiple == _ZERO:
            display_r_multiple = Decimal("0.000000")
        else:
            with localcontext() as decimal_context:
                decimal_context.prec = 50
                display_r_multiple = r_multiple.quantize(
                    _R_MULTIPLE_QUANTUM,
                    rounding=ROUND_FLOOR,
                )
        object.__setattr__(self, "r_multiple", display_r_multiple)


_PHASE1_EXIT_FEE_SCHEDULE_VERSION = "PHASE1_US_EQUITY_EXIT_V1"
_PHASE1_EXIT_FEE_MICROS = 1_000_000


def _phase1_exit_fee_schedule_digest() -> str:
    payload = {
        "namespace": "stock-monitor/phase1-exit-fee-schedule/v1",
        "payload": {
            "fee_micros": _PHASE1_EXIT_FEE_MICROS,
            "version": _PHASE1_EXIT_FEE_SCHEDULE_VERSION,
        },
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _position_action_fingerprint(action: PositionAction) -> tuple[object, ...]:
    return (
        action.status,
        action.reason_codes,
        action.recommended_stop,
        action.user_confirmed_stop,
        action.published_target,
        action.shares_to_exit,
        action.remaining_shares,
        action.r_multiple,
    )


def _paper_exit_result_fingerprint(
    result: PaperExitResult,
) -> tuple[object, ...]:
    return (
        result.exit_reason.value,
        result.fill_price,
        result.exited_at,
        result.observation_id,
        result.reason_codes,
    )


def _phase1_position_exit_authority_payload(
    *,
    position: Position,
    event_context: PositionEventContext,
    mark: MarketMark,
    steps: Sequence[Phase1PositionExitStep],
    mark_observation_id: str,
    position_evidence_digest: str | None,
    fee_schedule_version: str,
    fee_schedule_digest: str,
    validation_window_id: str,
    query_cutoff: datetime,
    source_digest: str,
) -> dict[str, object]:
    def timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    def optional_money(value: Decimal | None) -> int | None:
        return None if value is None else money_to_micros(value)

    return {
        "version": 2,
        "mark_observation_id": mark_observation_id,
        "position_evidence_digest": position_evidence_digest,
        "validation_window_id": validation_window_id,
        "query_cutoff": timestamp(query_cutoff),
        "source_digest": source_digest,
        "position_revision_digest": _position_revision_digest(position),
        "event_context_digest": event_context.source_digest,
        "mark": {
            "price_micros": money_to_micros(mark.price),
            "at": timestamp(mark.at),
            "holding_sessions": mark.holding_sessions,
            "previous_session_low_micros": optional_money(
                mark.previous_session_low
            ),
            "current_session_low_micros": optional_money(
                mark.current_session_low
            ),
            "atr14_micros": optional_money(mark.atr14),
            "event_exit_required": mark.event_exit_required,
            "thesis_invalidated": mark.thesis_invalidated,
            "event_evidence_status": mark.event_evidence_status,
            "review_cursor": mark.review_cursor,
        },
        "steps": [
            {
                "ordinal": ordinal,
                "position_revision_digest": _position_revision_digest(
                    step.position
                ),
                "event_kind": step.event_kind,
                "action": {
                    "status": step.action.status,
                    "reason_codes": list(step.action.reason_codes),
                    "recommended_stop_micros": money_to_micros(
                        step.action.recommended_stop
                    ),
                    "user_confirmed_stop_micros": optional_money(
                        step.action.user_confirmed_stop
                    ),
                    "published_target_micros": money_to_micros(
                        step.action.published_target
                    ),
                    "shares_to_exit": step.action.shares_to_exit,
                    "remaining_shares": step.action.remaining_shares,
                    "r_multiple": str(step.action.r_multiple),
                },
                "execution_result": {
                    "exit_reason": step.execution_result.exit_reason.value,
                    "fill_price_micros": optional_money(
                        step.execution_result.fill_price
                    ),
                    "exited_at": (
                        None
                        if step.execution_result.exited_at is None
                        else timestamp(step.execution_result.exited_at)
                    ),
                    "observation_id": step.execution_result.observation_id,
                    "reason_codes": list(step.execution_result.reason_codes),
                    "quote_observation_id": (
                        step.execution_quote_observation_id
                    ),
                },
                "fee_micros": money_to_micros(step.fee),
            }
            for ordinal, step in enumerate(steps, start=1)
        ],
        "fee_schedule_version": fee_schedule_version,
        "fee_schedule_digest": fee_schedule_digest,
    }


def _phase1_position_exit_authority_digest(
    authority: Phase1PositionExitAuthority,
) -> str:
    payload = _phase1_position_exit_authority_payload(
        position=authority.position,
        event_context=authority.event_context,
        mark=authority.mark,
        steps=authority.steps,
        mark_observation_id=authority.mark_observation_id,
        position_evidence_digest=authority.position_evidence_digest,
        fee_schedule_version=authority.fee_schedule_version,
        fee_schedule_digest=authority.fee_schedule_digest,
        validation_window_id=authority.validation_window_id,
        query_cutoff=authority.query_cutoff,
        source_digest=authority.source_digest,
    )
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1PositionExitAuthority:
    """Owner-current complete ordered exit batch from one Journal review."""

    position: Position
    event_context: PositionEventContext
    mark: MarketMark
    steps: tuple[Phase1PositionExitStep, ...]
    mark_observation_id: str
    position_evidence_digest: str | None
    fee_schedule_version: str
    fee_schedule_digest: str
    validation_window_id: str
    query_cutoff: datetime
    source_digest: str
    authority_digest: str

    def __post_init__(self) -> None:
        steps = tuple(self.steps)
        object.__setattr__(self, "steps", steps)
        if (
            not isinstance(self.position, Position)
            or not isinstance(self.event_context, PositionEventContext)
            or not isinstance(self.mark, MarketMark)
            or not 1 <= len(steps) <= 2
            or any(not isinstance(step, Phase1PositionExitStep) for step in steps)
            or steps[0].position != self.position
            or self.position.ledger_name != "CANONICAL"
            or type(self.mark_observation_id) is not str
            or not self.mark_observation_id
        ):
            raise RiskBlock("INVALID_PHASE1_EXIT_AUTHORITY")
        if (
            self.fee_schedule_version != _PHASE1_EXIT_FEE_SCHEDULE_VERSION
            or self.fee_schedule_digest != _phase1_exit_fee_schedule_digest()
            or (
                self.position_evidence_digest is not None
                and not _is_sha256_digest(self.position_evidence_digest)
            )
            or not _is_sha256_digest(self.validation_window_id)
        ):
            raise RiskBlock("PHASE1_EXIT_FEE_SCHEDULE_MISMATCH")
        _require_aware(self.query_cutoff, "INVALID_PHASE1_EXIT_AUTHORITY")
        if not _is_sha256_digest(self.source_digest) or not _is_sha256_digest(
            self.authority_digest
        ):
            raise RiskBlock("INVALID_PHASE1_EXIT_AUTHORITY")
        if (
            self.event_context.signal_id != self.position.signal_id
            or self.mark.signal_id != self.position.signal_id
            or self.mark.position_digest
            != _position_revision_digest(self.position)
        ):
            raise RiskBlock("PHASE1_EXIT_ACTION_MISMATCH")
        if len(steps) == 2:
            first, second = steps
            if (
                first.event_kind != "PARTIAL_EXIT"
                or second.event_kind != "CLOSE"
                or second.position.signal_id != first.position.signal_id
                or second.position.symbol != first.position.symbol
                or second.position.entry != first.position.entry
                or second.position.initial_stop != first.position.initial_stop
                or second.position.target != first.position.target
                or second.position.tick_size != first.position.tick_size
                or second.position.entered_session != first.position.entered_session
                or second.position.ledger_name != first.position.ledger_name
                or second.position.shares != first.action.remaining_shares
                or second.position.recommended_stop
                != first.action.recommended_stop
                or second.position.profit_target_taken is not True
                or second.execution_result.exited_at
                <= first.execution_result.exited_at
            ):
                raise RiskBlock("PHASE1_EXIT_BATCH_MISMATCH")

    @property
    def action(self) -> PositionAction:
        """Compatibility view of the first ordered exit step."""
        return self.steps[0].action

    @property
    def execution_result(self) -> PaperExitResult:
        return self.steps[0].execution_result

    @property
    def execution_quote_observation_id(self) -> str:
        return self.steps[0].execution_quote_observation_id

    @property
    def event_kind(self) -> str:
        return self.steps[0].event_kind

    @property
    def fee(self) -> Decimal:
        return self.steps[0].fee

    @property
    def total_fee(self) -> Decimal:
        return sum((step.fee for step in self.steps), _ZERO)


def _phase1_position_exit_authority_fingerprint(
    authority: Phase1PositionExitAuthority,
) -> tuple[object, ...]:
    return (
        _position_revision_digest(authority.position),
        _position_event_context_fingerprint(authority.event_context),
        _market_mark_fingerprint(authority.mark),
        tuple(
            (
                _position_revision_digest(step.position),
                _position_action_fingerprint(step.action),
                _paper_exit_result_fingerprint(step.execution_result),
                step.execution_quote_observation_id,
                step.event_kind,
                step.fee,
            )
            for step in authority.steps
        ),
        authority.mark_observation_id,
        authority.position_evidence_digest,
        authority.fee_schedule_version,
        authority.fee_schedule_digest,
        authority.validation_window_id,
        authority.query_cutoff,
        authority.source_digest,
        authority.authority_digest,
    )


def is_issued_phase1_position_exit_authority(authority: object) -> bool:
    if type(authority) is not Phase1PositionExitAuthority:
        return False
    children = _registered_risk_authority_children(
        _PHASE1_POSITION_EXIT_AUTHORITIES,
        authority,
    )
    if (
        children is None
        or len(children) != 3
        or children[0] is not authority.event_context
        or children[1] is not authority.mark
        or type(children[0]) is not PositionEventContext
        or type(children[1]) is not MarketMark
        or not _phase1_derived_sources_are_current(authority)
        or not is_issued_position_event_context(authority.event_context)
        or not is_issued_market_mark(authority.mark)
    ):
        return False
    try:
        digest = _phase1_position_exit_authority_digest(authority)
    except Exception:
        return False
    if digest != authority.authority_digest or not (
        _phase1_derived_sources_are_current_without_callbacks(authority)
    ):
        return False
    context_sources = _phase1_bound_sources(authority.event_context)
    if len(context_sources) != 1 or not (
        _is_current_risk_authority_without_callbacks(
            _POSITION_EVENT_AUTHORITIES,
            authority.event_context,
            exact_type=PositionEventContext,
            children=(context_sources[0][0],),
        )
        and _is_current_risk_authority_without_callbacks(
            _MARK_AUTHORITIES,
            authority.mark,
            exact_type=MarketMark,
            children=(authority.event_context,),
        )
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _PHASE1_POSITION_EXIT_AUTHORITIES,
        authority,
        exact_type=Phase1PositionExitAuthority,
        children=children,
    )


@dataclass(frozen=True, slots=True)
class PositionAdditionDecision:
    allowed: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(
                self.reason_codes,
                "INVALID_POSITION_ADDITION_DECISION",
            ),
        )
        if type(self.allowed) is not bool or self.allowed or not self.reason_codes:
            raise RiskBlock("INVALID_POSITION_ADDITION_DECISION")


def evaluate_position_addition(
    position: Position,
    price: Decimal,
    shares: int,
) -> PositionAdditionDecision:
    """Reject all additions, identifying averaging down separately."""
    if not isinstance(position, Position):
        raise TypeError("position addition requires a Position")
    price = _require_money(
        price,
        reason_code="INVALID_EXECUTION_PRICE",
        positive=True,
    )
    _require_positive_int(shares, "INVALID_EXECUTION_SHARES")
    reasons = ["POSITION_ADDITIONS_PROHIBITED"]
    if price < position.entry:
        reasons.append("AVERAGING_DOWN_PROHIBITED")
    return PositionAdditionDecision(False, tuple(reasons))


def _position_action(
    position: Position,
    mark: MarketMark,
    *,
    status: str,
    reasons: Sequence[str],
    recommended_stop: Decimal,
    shares_to_exit: int,
    r_multiple: Decimal,
) -> PositionAction:
    return PositionAction(
        status=status,
        reason_codes=tuple(dict.fromkeys(reasons)),
        recommended_stop=recommended_stop,
        user_confirmed_stop=position.user_confirmed_stop,
        published_target=position.target,
        shares_to_exit=shares_to_exit,
        remaining_shares=position.shares - shares_to_exit,
        r_multiple=r_multiple,
    )


def evaluate_position(
    position: Position,
    mark: MarketMark,
    policy: Policy,
) -> PositionAction:
    """Evaluate an adapter-authorized close mark without mutating state."""
    _validate_position_evaluation(position, mark, policy)
    r_multiple = _position_r_multiple(position, mark)
    position_revision_matches = (
        mark.ledger_name == position.ledger_name
        and mark.signal_id == position.signal_id
        and mark.symbol == position.symbol
        and mark.position_digest == _position_revision_digest(position)
    )
    if (
        not mark.context_verified
        or not mark.holding_sessions_verified
        or not is_issued_market_mark(mark)
        or not position_revision_matches
    ):
        reasons = (
            ("POSITION_REVISION_MISMATCH",)
            if is_issued_market_mark(mark) and not position_revision_matches
            else ("POSITION_CONTEXT_UNVERIFIED",)
        )
        return _position_action(
            position,
            mark,
            status="POSITION_UNVERIFIED",
            reasons=reasons,
            recommended_stop=position.recommended_stop,
            shares_to_exit=0,
            r_multiple=r_multiple,
        )
    return _evaluate_position_formula(position, mark, r_multiple)


def evaluate_position_diagnostic(
    position: Position,
    mark: MarketMark,
    policy: Policy,
) -> PositionAction:
    """Run the pure Task 6 formula without granting operational authority.

    The returned recommendation is suitable for deterministic calculation
    tests and operator diagnostics only.  Production callers must use
    :func:`evaluate_position`, which requires source-backed mark issuance.
    """
    _validate_position_evaluation(position, mark, policy)
    return _evaluate_position_formula(
        position,
        mark,
        _position_r_multiple(position, mark),
    )


def _validate_position_evaluation(
    position: Position,
    mark: MarketMark,
    policy: Policy,
) -> None:
    if not isinstance(position, Position):
        raise TypeError("position evaluation requires a Position")
    if not isinstance(mark, MarketMark):
        raise TypeError("position evaluation requires a MarketMark")
    if not isinstance(policy, Policy):
        raise TypeError("position evaluation requires a Policy")
    policy.validate()


def _position_r_multiple(position: Position, mark: MarketMark) -> Decimal:
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(
            position.entry,
            position.initial_stop,
            mark.price,
        )
        initial_risk = position.entry - position.initial_stop
        return (mark.price - position.entry) / initial_risk


def _evaluate_position_formula(
    position: Position,
    mark: MarketMark,
    r_multiple: Decimal,
) -> PositionAction:
    compliance_reasons: list[str] = []
    if position.user_confirmed_stop is None:
        compliance_reasons.append("STOP_UNVERIFIED")
    elif position.user_confirmed_stop < position.recommended_stop:
        compliance_reasons.append("USER_STOP_WIDER_THAN_RECOMMENDED")
    effective_protective_stop = max(
        position.recommended_stop,
        (
            position.user_confirmed_stop
            if position.user_confirmed_stop is not None
            else position.recommended_stop
        ),
    )

    exit_reasons: list[str] = []
    if mark.thesis_invalidated:
        exit_reasons.append("THESIS_INVALIDATED")
    if mark.event_exit_required:
        exit_reasons.append("EVENT_EXIT_REQUIRED")
    if mark.holding_sessions >= MAX_HOLD_SESSIONS:
        exit_reasons.append("MAX_HOLD_SESSIONS_REACHED")
    if mark.price <= position.recommended_stop:
        exit_reasons.append("RECOMMENDED_STOP_REACHED")
    if exit_reasons:
        return _position_action(
            position,
            mark,
            status="PROVISIONAL_EXIT",
            reasons=(*exit_reasons, *compliance_reasons),
            recommended_stop=position.recommended_stop,
            shares_to_exit=position.shares,
            r_multiple=r_multiple,
        )

    if mark.price > position.recommended_stop and (
        mark.price <= effective_protective_stop
        or (
            mark.current_session_low is not None
            and mark.current_session_low <= effective_protective_stop
        )
    ):
        return _position_action(
            position,
            mark,
            status="RECONCILIATION_REQUIRED",
            reasons=("STOP_EXECUTION_UNVERIFIED", *compliance_reasons),
            recommended_stop=position.recommended_stop,
            shares_to_exit=0,
            r_multiple=r_multiple,
        )

    if r_multiple >= _TWO and not position.profit_target_taken:
        recommended_stop = position.recommended_stop
        shares_to_exit = position.shares
        if (
            position.shares >= 2
            and mark.previous_session_low is not None
            and mark.current_session_low is not None
            and mark.atr14 is not None
        ):
            with localcontext() as decimal_context:
                decimal_context.prec = _decimal_work_precision(
                    mark.previous_session_low,
                    mark.current_session_low,
                    mark.atr14,
                    position.tick_size,
                )
                raw_trailing_stop = (
                    min(mark.previous_session_low, mark.current_session_low)
                    - Decimal("0.10") * mark.atr14
                )
                trailing_stop = (
                    (raw_trailing_stop / position.tick_size).to_integral_value(
                        rounding=ROUND_FLOOR
                    )
                    * position.tick_size
                )
            if (
                trailing_stop > position.recommended_stop
                and trailing_stop < mark.price
            ):
                recommended_stop = trailing_stop
                shares_to_exit = position.shares // 2
        return _position_action(
            position,
            mark,
            status="PROVISIONAL_EXIT",
            reasons=("TWO_R_REACHED", *compliance_reasons),
            recommended_stop=recommended_stop,
            shares_to_exit=shares_to_exit,
            r_multiple=r_multiple,
        )

    if r_multiple >= Decimal("1") and position.entry > position.recommended_stop:
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(
                position.entry,
                position.tick_size,
            )
            break_even_stop = (
                (position.entry / position.tick_size).to_integral_value(
                    rounding=ROUND_FLOOR
                )
                * position.tick_size
            )
        if break_even_stop > position.recommended_stop:
            return _position_action(
                position,
                mark,
                status="PROVISIONAL_TIGHTEN_STOP",
                reasons=("ONE_R_REACHED", *compliance_reasons),
                recommended_stop=break_even_stop,
                shares_to_exit=0,
                r_multiple=r_multiple,
            )
    if position.user_confirmed_stop is None:
        return _position_action(
            position,
            mark,
            status="STOP_UNVERIFIED",
            reasons=("STOP_UNVERIFIED",),
            recommended_stop=position.recommended_stop,
            shares_to_exit=0,
            r_multiple=r_multiple,
        )
    return _position_action(
        position,
        mark,
        status="PROVISIONAL_HOLD",
        reasons=compliance_reasons,
        recommended_stop=position.recommended_stop,
        shares_to_exit=0,
        r_multiple=r_multiple,
    )


def _phase1_exit_execution_result_from_observations(
    observations: Sequence[object],
    *,
    stop: Decimal,
    target: Decimal,
    forced_exit_reason: ExitReason | None = None,
    forced_expected_exit: Decimal | None = None,
    forced_triggered_at: datetime | None = None,
    target_enabled: bool = True,
) -> tuple[PaperExitResult, str | None]:
    """Translate exact Journal BAR/QUOTE facts into one conservative result."""

    def optional_money(value: object, code: str) -> Decimal | None:
        if value is None:
            return None
        if type(value) is not int:
            raise RiskBlock(code)
        try:
            return money_from_micros(value)
        except DomainValidationError:
            raise RiskBlock(code) from None

    items = tuple(observations)
    quotes = tuple(
        item for item in items if getattr(item, "observation_kind", None) == "QUOTE"
    )
    bars = tuple(
        item for item in items if getattr(item, "observation_kind", None) == "BAR"
    )
    normalized_bars: list[IntradayObservation] = []
    spread_sources: dict[str, str] = {}
    for sequence, bar in enumerate(bars, start=1):
        bar_at = getattr(bar, "source_time", None)
        bar_received_at = getattr(bar, "received_at", None)
        bar_observation_id = getattr(bar, "observation_id", None)
        bar_stream_id = getattr(bar, "stream_id", None)
        bar_feed = getattr(bar, "feed", None)
        bar_fresh = getattr(bar, "fresh", None)
        if (
            type(bar_observation_id) is not str
            or not bar_observation_id
            or type(bar_stream_id) is not str
            or not bar_stream_id
            or type(bar_feed) is not str
            or not bar_feed
            or type(bar_fresh) is not bool
            or not isinstance(bar_at, datetime)
            or not isinstance(bar_received_at, datetime)
        ):
            raise RiskBlock("PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH")

        bid = optional_money(
            getattr(bar, "bid_micros", None),
            "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
        )
        ask = optional_money(
            getattr(bar, "ask_micros", None),
            "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
        )
        spread_observation_id: str | None = None
        if bid is not None or ask is not None:
            if bid is None or ask is None or ask < bid:
                bid = None
                ask = None
            else:
                spread_observation_id = bar_observation_id
        else:
            eligible_quotes = tuple(
                quote
                for quote in quotes
                if (
                    getattr(quote, "feed", None) == bar_feed
                    and isinstance(getattr(quote, "source_time", None), datetime)
                    and getattr(quote, "source_time") <= bar_at
                    and getattr(quote, "source_time").astimezone(_ET).date()
                    == bar_at.astimezone(_ET).date()
                    and (
                        bar_at - getattr(quote, "source_time")
                    ).total_seconds()
                    <= 60
                )
            )
            if eligible_quotes:
                selected_quote = max(
                    eligible_quotes,
                    key=lambda item: (
                        getattr(item, "source_time"),
                        getattr(item, "cohort_ordinal", 0),
                    ),
                )
                quote_bid = optional_money(
                    getattr(selected_quote, "bid_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                )
                quote_ask = optional_money(
                    getattr(selected_quote, "ask_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                )
                if (
                    getattr(selected_quote, "fresh", None) is True
                    and quote_bid is not None
                    and quote_ask is not None
                    and quote_ask >= quote_bid
                ):
                    quote_observation_id = getattr(
                        selected_quote,
                        "observation_id",
                        None,
                    )
                    if (
                        type(quote_observation_id) is not str
                        or not quote_observation_id
                    ):
                        raise RiskBlock(
                            "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH"
                        )
                    bid = quote_bid
                    ask = quote_ask
                    spread_observation_id = quote_observation_id

        normalized_bars.append(
            IntradayObservation(
                observation_id=bar_observation_id,
                stream_id=bar_stream_id,
                feed=bar_feed,
                kind=ObservationKind.BAR,
                at=bar_at,
                received_at=bar_received_at,
                sequence=sequence,
                fresh=bar_fresh,
                bid=bid,
                ask=ask,
                open_price=optional_money(
                    getattr(bar, "open_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                ),
                high=optional_money(
                    getattr(bar, "high_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                ),
                low=optional_money(
                    getattr(bar, "low_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                ),
                close_price=optional_money(
                    getattr(bar, "close_micros", None),
                    "PHASE1_EXIT_EXECUTION_OBSERVATION_MISMATCH",
                ),
                session_open=(
                    bar_at.astimezone(_ET).time().replace(tzinfo=None)
                    == time(9, 30)
                ),
            )
        )
        if spread_observation_id is not None:
            spread_sources[bar_observation_id] = spread_observation_id

    if forced_exit_reason is None and (
        forced_expected_exit is not None or forced_triggered_at is not None
    ):
        raise RiskBlock("PHASE1_EXIT_EXECUTION_MODE_MISMATCH")
    if forced_exit_reason is not None and (
        forced_expected_exit is None or forced_triggered_at is None
    ):
        raise RiskBlock("PHASE1_EXIT_EXECUTION_MODE_MISMATCH")
    if type(target_enabled) is not bool:
        raise RiskBlock("PHASE1_EXIT_EXECUTION_MODE_MISMATCH")
    normalized = tuple(normalized_bars)
    if forced_exit_reason is None:
        result = simulate_exit(
            stop,
            target,
            normalized,
            target_enabled=target_enabled,
        )
    else:
        assert forced_expected_exit is not None
        assert forced_triggered_at is not None
        forced_result = simulate_forced_exit(
            forced_expected_exit,
            normalized,
            exit_reason=forced_exit_reason,
            triggered_at=forced_triggered_at,
        )
        protective_through = (
            forced_result.exited_at
            if forced_result.exited_at is not None
            else forced_triggered_at
        )
        protective_result = simulate_exit(
            stop,
            target,
            tuple(
                observation
                for observation in normalized
                if observation.at <= protective_through
            ),
            target_enabled=False,
        )
        result = (
            protective_result
            if protective_result.exit_reason is not ExitReason.NO_EXIT
            else forced_result
        )
    return (
        result,
        (
            None
            if result.observation_id is None
            else spread_sources.get(result.observation_id)
        ),
    )


def _phase1_position_exit_decision_from_observations(
    *,
    position: Position,
    mark: MarketMark,
    observations: Sequence[object],
    policy: Policy,
) -> tuple[PositionAction, PaperExitResult, str | None]:
    """Derive action and fill in market chronology, before close-mark gating."""
    _validate_position_evaluation(position, mark, policy)
    forced_reason = None
    for required, candidate in (
        (mark.thesis_invalidated, ExitReason.THESIS_INVALIDATED),
        (mark.event_exit_required, ExitReason.EVENT_EXIT_REQUIRED),
        (
            mark.holding_sessions >= MAX_HOLD_SESSIONS,
            ExitReason.MAX_HOLD_SESSIONS_REACHED,
        ),
    ):
        if required:
            forced_reason = candidate
            break
    execution_result, spread_observation_id = (
        _phase1_exit_execution_result_from_observations(
            observations,
            stop=position.recommended_stop,
            target=position.target,
            forced_exit_reason=forced_reason,
            forced_expected_exit=(mark.price if forced_reason is not None else None),
            forced_triggered_at=(mark.at if forced_reason is not None else None),
            target_enabled=(
                not position.profit_target_taken and forced_reason is None
            ),
        )
    )
    base_action = _evaluate_position_formula(
        position,
        mark,
        _position_r_multiple(position, mark),
    )
    if execution_result.exit_reason in {
        ExitReason.STOP,
        ExitReason.GAP_STOP,
        ExitReason.STOP_FIRST_CONSERVATIVE,
    }:
        action = _position_action(
            position,
            mark,
            status="PROVISIONAL_EXIT",
            reasons=("RECOMMENDED_STOP_REACHED",),
            recommended_stop=position.recommended_stop,
            shares_to_exit=position.shares,
            r_multiple=_position_r_multiple(position, mark),
        )
    elif execution_result.exit_reason is ExitReason.TARGET:
        assert execution_result.exited_at is not None
        target_time_lows = tuple(
            money_from_micros(low_micros)
            for observation in observations
            if getattr(observation, "observation_kind", None) == "BAR"
            and isinstance(getattr(observation, "source_time", None), datetime)
            and getattr(observation, "source_time") <= execution_result.exited_at
            and type(
                low_micros := getattr(observation, "low_micros", None)
            ) is int
            and low_micros > 0
        )
        if not target_time_lows:
            raise RiskBlock("PHASE1_EXIT_EXECUTION_UNRESOLVED")
        target_mark = replace(
            mark,
            price=position.target,
            current_session_low=min(target_time_lows),
            event_exit_required=False,
            thesis_invalidated=False,
            holding_sessions=min(
                mark.holding_sessions,
                MAX_HOLD_SESSIONS - 1,
            ),
        )
        action = _evaluate_position_formula(
            position,
            target_mark,
            _position_r_multiple(position, target_mark),
        )
    else:
        action = base_action
    return action, execution_result, spread_observation_id


@dataclass(frozen=True, slots=True)
class Phase1PositionExitStep:
    """One ordered canonical exit mutation and its exact conservative fill."""

    position: Position
    action: PositionAction
    execution_result: PaperExitResult
    execution_quote_observation_id: str
    event_kind: str
    fee: Decimal

    def __post_init__(self) -> None:
        if (
            not isinstance(self.position, Position)
            or not isinstance(self.action, PositionAction)
            or not isinstance(self.execution_result, PaperExitResult)
            or self.position.ledger_name != "CANONICAL"
            or self.event_kind not in {"PARTIAL_EXIT", "CLOSE"}
            or type(self.execution_quote_observation_id) is not str
            or not self.execution_quote_observation_id
        ):
            raise RiskBlock("INVALID_PHASE1_EXIT_STEP")
        object.__setattr__(
            self,
            "fee",
            _require_money(
                self.fee,
                reason_code="INVALID_PHASE1_EXIT_FEE",
                positive=True,
            ),
        )
        if (
            self.fee != money_from_micros(_PHASE1_EXIT_FEE_MICROS)
            or self.action.status != "PROVISIONAL_EXIT"
            or self.action.shares_to_exit <= 0
            or self.action.shares_to_exit + self.action.remaining_shares
            != self.position.shares
            or self.action.published_target != self.position.target
            or self.execution_result.exit_reason
            in {ExitReason.NO_EXIT, ExitReason.UNRESOLVED}
            or self.execution_result.fill_price is None
            or self.execution_result.fill_price <= _ZERO
            or self.execution_result.exited_at is None
            or not self.execution_result.observation_id
        ):
            raise RiskBlock("PHASE1_EXIT_STEP_MISMATCH")
        if self.event_kind == "PARTIAL_EXIT":
            if (
                self.execution_result.exit_reason is not ExitReason.TARGET
                or self.action.remaining_shares <= 0
                or self.action.recommended_stop
                <= self.position.recommended_stop
                or self.position.profit_target_taken
            ):
                raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
        elif self.action.remaining_shares != 0:
            raise RiskBlock("PHASE1_CLOSE_STATE_MISMATCH")


def _phase1_position_exit_steps_from_observations(
    *,
    position: Position,
    mark: MarketMark,
    observations: Sequence[object],
    policy: Policy,
) -> tuple[Phase1PositionExitStep, ...]:
    """Derive the complete ordered exit batch from one full-session review.

    A first +2R target may create a partial exit and a tighter stop.  Only
    observations strictly later than that target may then prove a close of the
    remainder; a same-bar target/stop ambiguity remains stop-first.
    """
    action, result, quote_observation_id = (
        _phase1_position_exit_decision_from_observations(
            position=position,
            mark=mark,
            observations=observations,
            policy=policy,
        )
    )
    if (
        action.status != "PROVISIONAL_EXIT"
        or action.shares_to_exit <= 0
        or result.exit_reason in {ExitReason.NO_EXIT, ExitReason.UNRESOLVED}
        or quote_observation_id is None
    ):
        raise RiskBlock("PHASE1_EXIT_EXECUTION_UNRESOLVED")
    fee = money_from_micros(_PHASE1_EXIT_FEE_MICROS)
    event_kind = "CLOSE" if action.remaining_shares == 0 else "PARTIAL_EXIT"
    first = Phase1PositionExitStep(
        position=position,
        action=action,
        execution_result=result,
        execution_quote_observation_id=quote_observation_id,
        event_kind=event_kind,
        fee=fee,
    )
    if event_kind == "CLOSE":
        return (first,)
    if result.exit_reason is not ExitReason.TARGET:
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
    assert result.exited_at is not None
    remaining_position = replace(
        position,
        shares=action.remaining_shares,
        recommended_stop=action.recommended_stop,
        user_confirmed_stop=(
            position.user_confirmed_stop
            if position.user_confirmed_stop is not None
            and position.user_confirmed_stop >= action.recommended_stop
            else None
        ),
        profit_target_taken=True,
    )
    later_observations = tuple(
        observation
        for observation in observations
        if isinstance(getattr(observation, "source_time", None), datetime)
        and getattr(observation, "source_time") > result.exited_at
    )
    if not later_observations:
        return (first,)
    later_result, later_quote_observation_id = (
        _phase1_exit_execution_result_from_observations(
            later_observations,
            stop=remaining_position.recommended_stop,
            target=remaining_position.target,
            target_enabled=False,
        )
    )
    if later_result.exit_reason is ExitReason.NO_EXIT:
        return (first,)
    if (
        later_result.exit_reason is ExitReason.UNRESOLVED
        or later_quote_observation_id is None
        or later_result.exit_reason
        not in {
            ExitReason.STOP,
            ExitReason.GAP_STOP,
            ExitReason.STOP_FIRST_CONSERVATIVE,
        }
    ):
        raise RiskBlock("PHASE1_EXIT_EXECUTION_UNRESOLVED")
    later_action = _position_action(
        remaining_position,
        mark,
        status="PROVISIONAL_EXIT",
        reasons=("RECOMMENDED_STOP_REACHED",),
        recommended_stop=remaining_position.recommended_stop,
        shares_to_exit=remaining_position.shares,
        r_multiple=_position_r_multiple(remaining_position, mark),
    )
    close = Phase1PositionExitStep(
        position=remaining_position,
        action=later_action,
        execution_result=later_result,
        execution_quote_observation_id=later_quote_observation_id,
        event_kind="CLOSE",
        fee=fee,
    )
    if close.execution_result.exited_at <= first.execution_result.exited_at:
        raise RiskBlock("PHASE1_EXIT_EXECUTION_ORDER_MISMATCH")
    return first, close


def _phase1_review_mark_from_observations(
    observations: Sequence[object],
    *,
    observation_id: str,
    method: str,
    price_micros: int,
    at: datetime,
    session_date: date,
    query_cutoff: datetime,
    calendar_resolver: SessionCalendarResolver,
    daily_bar_cohort: object | None = None,
) -> tuple[object, Decimal]:
    """Recompute a review mark from an exact provider-normalized fact."""
    if isinstance(observations, (str, bytes)):
        raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
    items = tuple(observations)
    matches = tuple(
        item
        for item in items
        if getattr(item, "observation_id", None) == observation_id
    )
    if (
        len(matches) != 1
        or type(observation_id) is not str
        or not observation_id
        or method not in {
            "SIP_QUOTE_BID",
            "DAILY_BAR_CLOSE_HAIRCUT",
        }
        or type(price_micros) is not int
        or price_micros <= 0
        or type(session_date) is not date
        or not isinstance(calendar_resolver, SessionCalendarResolver)
        or not calendar_resolver.release_verified
    ):
        raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
    try:
        normalized_at = _require_aware(at, "PHASE1_EXIT_MARK_MISMATCH")
        normalized_cutoff = _require_aware(
            query_cutoff,
            "PHASE1_EXIT_MARK_MISMATCH",
        )
    except RiskBlock:
        raise
    observation = matches[0]
    try:
        session = calendar_resolver.session(session_date)
    except RiskBlock:
        raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH") from None
    source_time = getattr(observation, "source_time", None)
    received_at = getattr(observation, "received_at", None)
    if (
        not isinstance(source_time, datetime)
        or not isinstance(received_at, datetime)
        or source_time.tzinfo is None
        or source_time.utcoffset() is None
        or received_at.tzinfo is None
        or received_at.utcoffset() is None
        or type(getattr(observation, "fresh", None)) is not bool
        or not observation.fresh
        or source_time.astimezone(_ET).date() != session_date
        or normalized_at.astimezone(_ET).date() != session_date
        or source_time > normalized_at
        or normalized_at > received_at
        or received_at > normalized_cutoff
        or not session.open_time
        <= normalized_at.astimezone(_ET).time().replace(tzinfo=None)
        <= session.close_time
    ):
        raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
    if method == "SIP_QUOTE_BID":
        bid_micros = getattr(observation, "bid_micros", None)
        ask_micros = getattr(observation, "ask_micros", None)
        if (
            getattr(observation, "observation_kind", None) != "QUOTE"
            or getattr(observation, "feed", None) != "sip"
            or source_time != normalized_at
            or type(bid_micros) is not int
            or type(ask_micros) is not int
            or bid_micros <= 0
            or ask_micros < bid_micros
            or price_micros != bid_micros
            or daily_bar_cohort is not None
        ):
            raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
    else:
        from urllib.parse import parse_qs, urlsplit

        from .providers.alpaca import (
            Bar,
            ProviderFetchCohort,
            _normalized_market_fact_source,
            _provider_fetch_cohort_manifest,
            is_issued_provider_fetch_cohort,
        )

        close_micros = getattr(observation, "close_micros", None)
        if (
            getattr(observation, "observation_kind", None) != "BAR"
            or type(close_micros) is not int
            or close_micros <= 0
            or normalized_at.astimezone(_ET).time().replace(tzinfo=None)
            != session.close_time
            or price_micros != (close_micros * 999) // 1000
            or not isinstance(daily_bar_cohort, ProviderFetchCohort)
            or not is_issued_provider_fetch_cohort(daily_bar_cohort)
        ):
            raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
        assert isinstance(daily_bar_cohort, ProviderFetchCohort)
        try:
            manifest = _provider_fetch_cohort_manifest(daily_bar_cohort)
            symbol = getattr(observation, "symbol")
            facts = tuple(daily_bar_cohort[symbol])
            session_facts = tuple(
                fact
                for fact in facts
                if isinstance(fact, Bar)
                and fact.timestamp.astimezone(_ET).date() == session_date
            )
            if len(session_facts) != 1:
                raise ValueError
            fact = session_facts[0]
            fact_source = _normalized_market_fact_source(fact)
            page_queries = tuple(
                (
                    page,
                    parse_qs(
                        urlsplit(page.request_url).query,
                        keep_blank_values=True,
                        strict_parsing=True,
                    ),
                    urlsplit(page.request_url),
                )
                for page in manifest.pages
            )
            request_starts = {
                query["start"][0] for _page, query, _parsed in page_queries
            }
            request_ends = {
                query["end"][0] for _page, query, _parsed in page_queries
            }
            if len(request_starts) != 1 or len(request_ends) != 1:
                raise ValueError
            request_start = datetime.fromisoformat(
                next(iter(request_starts)).replace("Z", "+00:00")
            )
            request_end = datetime.fromisoformat(
                next(iter(request_ends)).replace("Z", "+00:00")
            )
            session_open = datetime.combine(
                session_date,
                session.open_time,
                tzinfo=_ET,
            ).astimezone(UTC)
            session_close = datetime.combine(
                session_date,
                session.close_time,
                tzinfo=_ET,
            ).astimezone(UTC)
        except (AttributeError, KeyError, TypeError, ValueError, IndexError):
            raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH") from None
        if (
            manifest.collection != "bars"
            or manifest.requested_symbols != tuple(
                sorted(manifest.requested_symbols)
            )
            or symbol not in manifest.requested_symbols
            or any(
                parsed.path != "/v2/stocks/bars"
                or page.source_type != "ALPACA_DAILY_BARS"
                or query.get("timeframe") != ["1Day"]
                or query.get("adjustment") != ["split"]
                or query.get("feed") != ["sip"]
                or query.get("symbols")
                != [",".join(manifest.requested_symbols)]
                for page, query, parsed in page_queries
            )
            or request_start > session_open
            or request_end < session_close
            or fact.feed != "sip"
            or fact.adjustment != "split"
            or fact.close != money_from_micros(close_micros)
            or fact.timestamp != source_time
            or getattr(
                observation,
                "provider_source_observation_id",
                None,
            )
            != fact_source.source_observation_id
            or getattr(observation, "source_item_ordinal", None)
            != fact_source.source_item_ordinal
            or getattr(observation, "source_item_path", None)
            != fact_source.source_item_path
            or getattr(observation, "page_payload_sha256", None)
            != fact_source.page_payload_sha256
            or getattr(observation, "normalized_fields_digest", None)
            != fact_source.normalized_fields_digest
        ):
            raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH")
    try:
        return observation, money_from_micros(price_micros)
    except DomainValidationError:
        raise RiskBlock("PHASE1_EXIT_MARK_MISMATCH") from None


@dataclass(frozen=True, slots=True)
class _Phase1ExitMarketMaterial:
    observations: tuple[object, ...]
    mark_observation: object
    mark_price: Decimal
    mark_at: datetime
    previous_session_low: Decimal
    current_session_low: Decimal
    atr14: Decimal


def _phase1_exit_provider_authorities_are_current(source: object) -> bool:
    """Purely recheck the exact provider graph behind one exit review."""
    from .journal import (
        Phase1ExitReviewMarketSource,
        _is_current_phase1_source_authority_without_callbacks,
    )
    from .providers.alpaca import (
        Bar,
        ProviderFetchCohort,
        Quote,
        is_issued_normalized_market_fact,
        is_issued_provider_fetch_cohort,
        provider_fetch_cohorts_share_owner,
    )

    if type(source) is not Phase1ExitReviewMarketSource or not (
        _is_current_phase1_source_authority_without_callbacks(source)
    ):
        return False
    try:
        symbol = object.__getattribute__(source, "symbol")
        cohorts = (
            object.__getattribute__(source, "daily_bar_cohort"),
            object.__getattribute__(source, "execution_bar_cohort"),
            object.__getattribute__(source, "quote_cohort"),
        )
        provider_fact_groups = tuple(
            tuple(cohort[symbol]) for cohort in cohorts
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False
    if (
        type(symbol) is not str
        or not symbol
        or any(type(cohort) is not ProviderFetchCohort for cohort in cohorts)
        or any(not is_issued_provider_fetch_cohort(cohort) for cohort in cohorts)
        or not provider_fetch_cohorts_share_owner(*cohorts)
        or any(
            type(fact) is not expected_type
            or not is_issued_normalized_market_fact(fact)
            for facts, expected_type in zip(
                provider_fact_groups,
                (Bar, Bar, Quote),
                strict=True,
            )
            for fact in facts
        )
    ):
        return False
    return True


def _phase1_exit_market_material_matches_current_provider(
    source: object,
    material: _Phase1ExitMarketMaterial,
) -> bool:
    """Compare derived market scalars with one fresh callback-free provider read."""
    from .indicators import wilder_atr

    if type(material) is not _Phase1ExitMarketMaterial or not (
        _phase1_exit_provider_authorities_are_current(source)
    ):
        return False
    try:
        symbol = object.__getattribute__(source, "symbol")
        daily_bars = tuple(
            object.__getattribute__(source, "daily_bar_cohort")[symbol]
        )
        execution_bars = tuple(
            object.__getattribute__(source, "execution_bar_cohort")[symbol]
        )
        quotes = tuple(
            object.__getattribute__(source, "quote_cohort")[symbol]
        )
        stored_quotes = object.__getattribute__(source, "quote_facts")
        stored_daily = object.__getattribute__(source, "daily_bar_facts")
        source_observations = object.__getattribute__(source, "observations")
        if (
            type(stored_quotes) is not tuple
            or type(stored_daily) is not tuple
            or type(source_observations) is not tuple
            or len(daily_bars) != 14
            or not execution_bars
            or len(quotes) != len(stored_quotes)
            or len(daily_bars) != len(stored_daily)
        ):
            return False
        previous_low = daily_bars[-2].low
        current_low = min(bar.low for bar in execution_bars)
        atr14 = wilder_atr(daily_bars, 14).quantize(
            Decimal("0.000001"),
            rounding=ROUND_CEILING,
        )
        if quotes:
            provider_quote, mark_observation = max(
                zip(quotes, stored_quotes, strict=True),
                key=lambda item: (
                    item[0].timestamp,
                    -1 if item[0].sequence is None else item[0].sequence,
                ),
            )
            mark_price = provider_quote.bid
            mark_at = provider_quote.timestamp
        else:
            mark_observation = stored_daily[-1]
            close_micros = money_to_micros(daily_bars[-1].close)
            mark_price = money_from_micros((close_micros * 999) // 1000)
            mark_at = material.mark_at
    except (AttributeError, DomainValidationError, TypeError, ValueError):
        return False
    if not _phase1_exit_provider_authorities_are_current(source):
        return False
    return (
        len(material.observations) == len(source_observations)
        and all(
            actual is expected
            for actual, expected in zip(
                material.observations,
                source_observations,
                strict=True,
            )
        )
        and material.mark_observation is mark_observation
        and material.mark_price == mark_price
        and material.mark_at is mark_at
        and material.previous_session_low == previous_low
        and material.current_session_low == current_low
        and material.atr14 == atr14
    )


def _phase1_exit_review_market_material(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
) -> _Phase1ExitMarketMaterial:
    """Revalidate exact persisted provider cohorts and recompute market facts."""
    from urllib.parse import parse_qs, urlsplit

    from .indicators import wilder_atr
    from .providers.alpaca import (
        Bar,
        ProviderFetchCohort,
        Quote,
        _normalized_market_fact_source,
        _provider_fetch_cohort_manifest,
        is_issued_provider_fetch_cohort,
        provider_fetch_cohorts_share_owner,
        read_provider_fetch_bundle,
    )

    roles = (
        (
            "DAILY_BAR",
            getattr(source, "daily_bar_cohort", None),
            tuple(getattr(source, "daily_bar_facts", ())),
            "bars",
            "ALPACA_DAILY_BARS",
            "1Day",
        ),
        (
            "EXECUTION_BAR",
            getattr(source, "execution_bar_cohort", None),
            tuple(getattr(source, "execution_bar_facts", ())),
            "bars",
            "ALPACA_INTRADAY_BARS",
            "1Min",
        ),
        (
            "QUOTE",
            getattr(source, "quote_cohort", None),
            tuple(getattr(source, "quote_facts", ())),
            "quotes",
            "ALPACA_HISTORICAL_QUOTES",
            None,
        ),
    )
    cohorts = tuple(item[1] for item in roles)
    if (
        any(not isinstance(item, ProviderFetchCohort) for item in cohorts)
        or any(not is_issued_provider_fetch_cohort(item) for item in cohorts)
        or not provider_fetch_cohorts_share_owner(*cohorts)
    ):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_UNVERIFIED")
    symbol = getattr(source, "symbol", None)
    review_session = getattr(source, "review_session", None)
    query_cutoff = getattr(source, "query_cutoff", None)
    if (
        type(symbol) is not str
        or not symbol
        or type(review_session) is not date
        or not isinstance(query_cutoff, datetime)
    ):
        raise RiskBlock("PHASE1_EXIT_SOURCE_LINEAGE_MISMATCH")
    query_cutoff = _require_aware(
        query_cutoff,
        "PHASE1_EXIT_SOURCE_LINEAGE_MISMATCH",
    )
    try:
        review_schedule = calendar_resolver.session(review_session)
    except RiskBlock:
        raise RiskBlock("PHASE1_EXIT_SOURCE_LINEAGE_MISMATCH") from None
    review_open = datetime.combine(
        review_session,
        review_schedule.open_time,
        tzinfo=_ET,
    ).astimezone(UTC)
    review_close = datetime.combine(
        review_session,
        review_schedule.close_time,
        tzinfo=_ET,
    ).astimezone(UTC)
    if query_cutoff < review_close:
        raise RiskBlock("PHASE1_EXIT_REVIEW_BEFORE_SESSION_CLOSE")

    semantic_manifest_digests: list[str] = []
    provider_facts_by_role: dict[str, tuple[object, ...]] = {}
    stored_facts_by_role: dict[str, tuple[object, ...]] = {}
    for purpose, cohort, stored_facts, collection, source_type, timeframe in roles:
        assert isinstance(cohort, ProviderFetchCohort)
        try:
            manifest = _provider_fetch_cohort_manifest(cohort)
            bundle = read_provider_fetch_bundle(cohort)
            provider_facts = tuple(cohort[symbol])
        except (KeyError, TypeError, ValueError):
            raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_UNVERIFIED") from None
        if (
            manifest.collection != collection
            or manifest.requested_symbols != (symbol,)
            or manifest.terminal is not True
            or not manifest.pages
            or len(bundle.pages) != len(manifest.pages)
            or any(page.source_type != source_type for page in manifest.pages)
        ):
            raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_MISMATCH")
        page_queries: list[dict[str, list[str]]] = []
        for page_bundle in bundle.pages:
            parsed = urlsplit(page_bundle.page.request_url)
            try:
                query = parse_qs(
                    parsed.query,
                    keep_blank_values=True,
                    strict_parsing=True,
                )
            except ValueError:
                raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_MISMATCH") from None
            if (
                parsed.scheme != "https"
                or parsed.netloc != "data.alpaca.markets"
                or parsed.path
                != ("/v2/stocks/bars" if collection == "bars" else "/v2/stocks/quotes")
                or query.get("symbols") != [symbol]
                or query.get("feed") != ["sip"]
                or (
                    timeframe is not None
                    and (
                        query.get("timeframe") != [timeframe]
                        or query.get("adjustment") != ["split"]
                    )
                )
                or (
                    timeframe is None
                    and (
                        "timeframe" in query or "adjustment" in query
                    )
                )
            ):
                raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_MISMATCH")
            page_queries.append(query)
        try:
            starts = {query["start"][0] for query in page_queries}
            ends = {query["end"][0] for query in page_queries}
            if len(starts) != 1 or len(ends) != 1:
                raise ValueError
            request_start = datetime.fromisoformat(
                next(iter(starts)).replace("Z", "+00:00")
            )
            request_end = datetime.fromisoformat(
                next(iter(ends)).replace("Z", "+00:00")
            )
        except (KeyError, ValueError, IndexError):
            raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_MISMATCH") from None
        if purpose in {"EXECUTION_BAR", "QUOTE"}:
            if request_start != review_open or request_end != review_close:
                raise RiskBlock("PHASE1_EXIT_SESSION_COVERAGE_INCOMPLETE")
            role_close = review_close
        else:
            if not provider_facts or any(
                not isinstance(fact, Bar) for fact in provider_facts
            ):
                raise RiskBlock("PHASE1_EXIT_DAILY_HISTORY_INCOMPLETE")
            daily_end_session = provider_facts[-1].timestamp.astimezone(_ET).date()
            daily_end_schedule = calendar_resolver.session(daily_end_session)
            role_close = datetime.combine(
                daily_end_session,
                daily_end_schedule.close_time,
                tzinfo=_ET,
            ).astimezone(UTC)
            first_daily_session = provider_facts[0].timestamp.astimezone(_ET).date()
            first_daily_schedule = calendar_resolver.session(first_daily_session)
            expected_daily_start = datetime.combine(
                first_daily_session,
                first_daily_schedule.open_time,
                tzinfo=_ET,
            ).astimezone(UTC)
            if request_start != expected_daily_start or request_end != role_close:
                raise RiskBlock("PHASE1_EXIT_DAILY_HISTORY_INCOMPLETE")
        if any(
            page.observation.retrieved_at < role_close
            or page.observation.retrieved_at > query_cutoff
            for page in bundle.pages
        ):
            raise RiskBlock("PHASE1_EXIT_PROVIDER_RECEIPT_TIME_MISMATCH")
        semantic_manifest_digests.append(
            sha256(
                json.dumps(
                    {
                        "namespace": (
                            "stock-monitor/phase1-exit-semantic-manifest/v1"
                        ),
                        "purpose": purpose,
                        "collection": manifest.collection,
                        "requested_symbols": list(manifest.requested_symbols),
                        "request_start": request_start.astimezone(UTC).isoformat(
                            timespec="microseconds"
                        ).replace("+00:00", "Z"),
                        "request_end": request_end.astimezone(UTC).isoformat(
                            timespec="microseconds"
                        ).replace("+00:00", "Z"),
                        "pages": [
                            {
                                "page_ordinal": page.page.page_ordinal,
                                "source_type": page.page.source_type,
                                "request_url": page.page.request_url,
                                "request_page_token": (
                                    page.page.request_page_token
                                ),
                                "next_page_token": page.page.next_page_token,
                                "payload_sha256": page.page.payload_sha256,
                            }
                            for page in bundle.pages
                        ],
                    },
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        )

        if len(provider_facts) != len(stored_facts):
            raise RiskBlock("PHASE1_EXIT_PROVIDER_FACT_SET_MISMATCH")
        page_observations = {
            page.page.source_observation_id: page.observation
            for page in bundle.pages
        }
        for ordinal, (provider_fact, stored_fact) in enumerate(
            zip(provider_facts, stored_facts, strict=True),
            start=1,
        ):
            try:
                fact_source = _normalized_market_fact_source(provider_fact)
                page_observation = page_observations[
                    fact_source.source_observation_id
                ]
            except (KeyError, ValueError):
                raise RiskBlock("PHASE1_EXIT_PROVIDER_FACT_SET_MISMATCH") from None
            is_bar = isinstance(provider_fact, Bar)
            is_quote = isinstance(provider_fact, Quote)
            expected_kind = "BAR" if purpose != "QUOTE" else "QUOTE"
            expected_values = (
                (
                    money_to_micros(provider_fact.open),
                    money_to_micros(provider_fact.high),
                    money_to_micros(provider_fact.low),
                    money_to_micros(provider_fact.close),
                    provider_fact.volume,
                    None,
                    None,
                    provider_fact.adjustment,
                    None,
                )
                if is_bar
                else (
                    None,
                    None,
                    None,
                    None,
                    None,
                    money_to_micros(provider_fact.bid),
                    money_to_micros(provider_fact.ask),
                    None,
                    provider_fact.sequence,
                )
            )
            if (
                (purpose != "QUOTE" and not is_bar)
                or (purpose == "QUOTE" and not is_quote)
                or getattr(stored_fact, "purpose", None) != purpose
                or getattr(stored_fact, "fact_ordinal", None) != ordinal
                or getattr(stored_fact, "observation_kind", None) != expected_kind
                or getattr(stored_fact, "symbol", None) != symbol
                or str(getattr(stored_fact, "feed", "")).lower() != "sip"
                or getattr(stored_fact, "source_time", None)
                != provider_fact.timestamp
                or getattr(stored_fact, "received_at", None)
                != page_observation.retrieved_at
                or getattr(stored_fact, "provider_sequence", None)
                != expected_values[8]
                or getattr(stored_fact, "provider_source_observation_id", None)
                != fact_source.source_observation_id
                or getattr(stored_fact, "page_ordinal", None)
                != fact_source.page_ordinal
                or getattr(stored_fact, "source_item_ordinal", None)
                != fact_source.source_item_ordinal
                or getattr(stored_fact, "source_item_path", None)
                != fact_source.source_item_path
                or getattr(stored_fact, "page_payload_sha256", None)
                != fact_source.page_payload_sha256
                or getattr(stored_fact, "normalized_fields_digest", None)
                != fact_source.normalized_fields_digest
                or (
                    getattr(stored_fact, "open_micros", None),
                    getattr(stored_fact, "high_micros", None),
                    getattr(stored_fact, "low_micros", None),
                    getattr(stored_fact, "close_micros", None),
                    getattr(stored_fact, "volume", None),
                    getattr(stored_fact, "bid_micros", None),
                    getattr(stored_fact, "ask_micros", None),
                    getattr(stored_fact, "adjustment", None),
                )
                != expected_values[:8]
                or getattr(stored_fact, "fresh", None) is not True
                or type(getattr(stored_fact, "source_observation_id", None))
                is not int
                or getattr(stored_fact, "source_observation_id") <= 0
            ):
                raise RiskBlock("PHASE1_EXIT_PROVIDER_FACT_SET_MISMATCH")
        provider_facts_by_role[purpose] = provider_facts
        stored_facts_by_role[purpose] = stored_facts

    review_id_payload = {
        "namespace": "stock-monitor/phase1-exit-review/v1",
        "payload": {
            "signal_id": getattr(source, "signal_id", None),
            "review_session": review_session.isoformat(),
            "semantic_manifest_digests": semantic_manifest_digests,
        },
    }
    computed_review_id = sha256(
        json.dumps(
            review_id_payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    all_stored_facts = tuple(
        fact
        for purpose in ("DAILY_BAR", "EXECUTION_BAR", "QUOTE")
        for fact in stored_facts_by_role[purpose]
    )
    observations = tuple(getattr(source, "observations", ()))
    expected_observations = tuple(
        sorted(
            (
                *stored_facts_by_role["EXECUTION_BAR"],
                *stored_facts_by_role["QUOTE"],
            ),
            key=lambda item: getattr(item, "cohort_ordinal", 0),
        )
    )
    all_ids = tuple(getattr(item, "observation_id", None) for item in all_stored_facts)
    if (
        getattr(source, "review_id", None) != computed_review_id
        or getattr(source, "expected_manifest_count", None) != 3
        or getattr(source, "expected_fact_count", None) != len(all_stored_facts)
        or len(set(all_ids)) != len(all_ids)
        or observations != expected_observations
        or tuple(getattr(item, "cohort_ordinal", None) for item in observations)
        != tuple(range(1, len(observations) + 1))
        or not observations
        or getattr(source, "source_observation_highwater", 0)
        < max(getattr(item, "source_observation_id") for item in all_stored_facts)
    ):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_FACT_SET_MISMATCH")

    daily_bars = provider_facts_by_role["DAILY_BAR"]
    execution_bars = provider_facts_by_role["EXECUTION_BAR"]
    if len(daily_bars) != 14 or not execution_bars:
        raise RiskBlock("PHASE1_EXIT_DAILY_HISTORY_INCOMPLETE")
    daily_sessions = tuple(
        bar.timestamp.astimezone(_ET).date() for bar in daily_bars
    )
    expected_daily_end = (
        review_session
        if query_cutoff >= review_close
        else calendar_resolver.previous_session(review_session)
    )
    expected_sessions = [expected_daily_end]
    while len(expected_sessions) < 14:
        expected_sessions.append(
            calendar_resolver.previous_session(expected_sessions[-1])
        )
    expected_sessions.reverse()
    if daily_sessions != tuple(expected_sessions):
        raise RiskBlock("PHASE1_EXIT_DAILY_HISTORY_INCOMPLETE")
    # Calendar methods above are caller-dispatchable.  Recheck the exact
    # Journal/provider graph after the final callback and before copying any
    # market scalar into an operational decision.
    if not _phase1_exit_provider_authorities_are_current(source):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_UNVERIFIED")
    if expected_daily_end == review_session:
        daily_current = daily_bars[-1]
        if (
            daily_current.open != execution_bars[0].open
            or daily_current.high != max(bar.high for bar in execution_bars)
            or daily_current.low != min(bar.low for bar in execution_bars)
            or daily_current.close != execution_bars[-1].close
            or daily_current.volume != sum(bar.volume for bar in execution_bars)
        ):
            raise RiskBlock("PHASE1_EXIT_DAILY_INTRADAY_AGGREGATE_MISMATCH")
        previous_low = daily_bars[-2].low
    else:
        previous_low = daily_bars[-1].low
    current_low = min(bar.low for bar in execution_bars)
    raw_atr14 = wilder_atr(daily_bars, 14)
    atr14 = raw_atr14.quantize(Decimal("0.000001"), rounding=ROUND_CEILING)

    quote_pairs = tuple(
        zip(
            provider_facts_by_role["QUOTE"],
            stored_facts_by_role["QUOTE"],
            strict=True,
        )
    )
    eligible_quotes = tuple(
        (provider_quote, stored_quote)
        for provider_quote, stored_quote in quote_pairs
        if (
            review_open <= provider_quote.timestamp <= review_close
            and (review_close - provider_quote.timestamp).total_seconds() <= 60
        )
    )
    if eligible_quotes:
        provider_quote, mark_observation = max(
            eligible_quotes,
            key=lambda item: (
                item[0].timestamp,
                -1 if item[0].sequence is None else item[0].sequence,
            ),
        )
        mark_price = provider_quote.bid
        mark_at = provider_quote.timestamp
    else:
        if expected_daily_end != review_session:
            raise RiskBlock("PHASE1_EXIT_REVIEW_MARK_UNAVAILABLE")
        mark_observation = stored_facts_by_role["DAILY_BAR"][-1]
        close_micros = money_to_micros(daily_bars[-1].close)
        mark_price = money_from_micros((close_micros * 999) // 1000)
        mark_at = review_close
    material = _Phase1ExitMarketMaterial(
        observations=observations,
        mark_observation=mark_observation,
        mark_price=mark_price,
        mark_at=mark_at,
        previous_session_low=previous_low,
        current_session_low=current_low,
        atr14=atr14,
    )
    if not _phase1_exit_market_material_matches_current_provider(
        source,
        material,
    ):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_UNVERIFIED")
    return material


@dataclass(frozen=True, slots=True)
class Phase1DerivedPositionExit:
    """Replay-independent recomputation of one exact ordered exit batch."""

    event_context: PositionEventContext
    mark: MarketMark
    steps: tuple[Phase1PositionExitStep, ...]
    mark_observation_id: str
    position_evidence_digest: str | None


def _derive_phase1_position_exit_material_from_verified_source(
    source: object,
    *,
    position: Position,
    calendar_resolver: SessionCalendarResolver,
    policy: Policy,
) -> Phase1DerivedPositionExit:
    """Recompute exit facts from raw-bound material and an explicit position.

    This is the common replay boundary.  It deliberately has no canonical
    replay input: callers first reconstruct the position revision they intend
    to verify, then this adapter independently reissues signal-level evidence,
    binds it to that revision, and reruns the complete paper-exit simulation.
    """
    from .journal import (
        Phase1ExitReviewMarketSource,
        is_verified_phase1_exit_review_market_source,
        phase1_sources_share_owner,
    )

    if not isinstance(source, Phase1ExitReviewMarketSource) or not (
        is_verified_phase1_exit_review_market_source(source)
    ):
        raise RiskBlock("PHASE1_EXIT_REVIEW_MARKET_SOURCE_UNVERIFIED")
    if not isinstance(position, Position):
        raise TypeError("Phase 1 exit replay requires a Position")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if not isinstance(policy, Policy):
        raise RiskBlock("INVALID_POLICY")
    policy.validate()

    calendar_digest = _calendar_digest(calendar_resolver)
    signal_source = source.signal_source
    try:
        planned_shares = signal_source.planned_shares
        if position.profit_target_taken:
            legal_position_revision = (
                position.shares == planned_shares - (planned_shares // 2)
                and position.recommended_stop > position.initial_stop
                and position.recommended_stop % position.tick_size == _ZERO
            )
        else:
            legal_position_revision = (
                position.shares == planned_shares
                and position.recommended_stop == position.initial_stop
            )
        position_matches_signal = (
            position.ledger_name == "CANONICAL"
            and position.signal_id == source.signal_id
            and position.symbol == source.symbol
            and position.entry
            == money_from_micros(signal_source.maximum_entry_micros)
            and position.initial_stop
            == money_from_micros(signal_source.recommended_stop_micros)
            and position.target == money_from_micros(signal_source.target_micros)
            and position.tick_size
            == money_from_micros(signal_source.tick_size_micros)
            and position.entered_session == signal_source.publication_session
            and position.user_confirmed_stop is None
            and legal_position_revision
        )
    except (AttributeError, DomainValidationError):
        position_matches_signal = False
    if _policy_digest(policy) != signal_source.policy_digest:
        raise RiskBlock("PHASE1_EXIT_POLICY_MISMATCH")
    if (
        source.calendar_digest != calendar_digest
        or source.signal_id != signal_source.signal_id
        or source.validation_window_id != signal_source.validation_window_id
        or source.symbol != signal_source.symbol
        or signal_source.role != "PRIMARY"
        or not phase1_sources_share_owner(source, signal_source)
        or not position_matches_signal
    ):
        raise RiskBlock("PHASE1_EXIT_POSITION_MISMATCH")

    market = _phase1_exit_review_market_material(
        source,
        calendar_resolver=calendar_resolver,
    )
    signal_evidence_source = source.signal_evidence_source
    position_evidence: Phase1PositionEvidenceAuthority | None
    if signal_evidence_source is None:
        event_exit_required: bool | None = None
        thesis_invalidated: bool | None = None
        evidence_status = "UNAVAILABLE"
        position_evidence = None
        position_evidence_digest = None
    else:
        signal_evidence = _issue_phase1_signal_evidence_authority_from_source(
            signal_evidence_source,
            calendar_resolver=calendar_resolver,
        )
        evidence_signal_source = signal_evidence.signal_source
        if (
            signal_evidence.signal_id != source.signal_id
            or signal_evidence.validation_window_id
            != source.validation_window_id
            or signal_evidence.symbol != source.symbol
            or getattr(evidence_signal_source, "row_id", None)
            != getattr(signal_source, "row_id", None)
            or getattr(evidence_signal_source, "row_sha256", None)
            != getattr(signal_source, "row_sha256", None)
            or getattr(
                evidence_signal_source,
                "publication_source_digest",
                None,
            )
            != getattr(signal_source, "publication_source_digest", None)
            or not phase1_sources_share_owner(source, evidence_signal_source)
            or signal_evidence.calendar_digest != calendar_digest
            or signal_evidence.review_at > source.query_cutoff
            or signal_evidence.review_at.astimezone(_ET).date()
            != source.review_session
        ):
            raise RiskBlock("PHASE1_EXIT_EVENT_EVIDENCE_MISMATCH")
        position_evidence = _issue_phase1_position_evidence_authority(
            signal_evidence,
            position=position,
        )
        event_exit_required = position_evidence.event_exit_required
        thesis_invalidated = position_evidence.thesis_invalidated
        evidence_status = position_evidence.status
        position_evidence_digest = position_evidence.source_digest

    all_market_facts = (
        *tuple(source.daily_bar_facts),
        *tuple(source.execution_bar_facts),
        *tuple(source.quote_facts),
    )
    fact_cursors = tuple(
        getattr(item, "source_cursor", None) for item in all_market_facts
    )
    if not fact_cursors or any(
        type(cursor) is not int or cursor <= 0 for cursor in fact_cursors
    ):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_FACT_SET_MISMATCH")
    event_context = _issue_position_event_context(
        position=position,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        evidence_status=evidence_status,
        at=market.mark_at,
        cursor=max(fact_cursors),
        start_cursor=min(fact_cursors),
        event_count=(
            0
            if position_evidence is None
            else len(position_evidence.source_observation_ids)
        ),
        calendar_resolver=calendar_resolver,
        price=market.mark_price,
        previous_session_low=market.previous_session_low,
        current_session_low=market.current_session_low,
        atr14=market.atr14,
    )
    _install_risk_authority(
        _POSITION_EVENT_AUTHORITIES,
        event_context,
        exact_type=PositionEventContext,
        children=(source,),
        phase1_bindings=((source, "EXIT_REVIEW_MARKET"),),
    )
    mark = build_market_mark(
        position,
        price=market.mark_price,
        at=market.mark_at,
        calendar_resolver=calendar_resolver,
        previous_session_low=market.previous_session_low,
        current_session_low=market.current_session_low,
        atr14=market.atr14,
        event_exit_required=event_exit_required,
        thesis_invalidated=thesis_invalidated,
        event_evidence_status=evidence_status,
        event_context_verified=True,
        position_event_context=event_context,
    )
    steps = _phase1_position_exit_steps_from_observations(
        position=position,
        mark=mark,
        observations=market.observations,
        policy=policy,
    )
    observation_ids = {
        getattr(item, "observation_id", None) for item in market.observations
    }
    if any(
        step.execution_result.observation_id not in observation_ids
        or step.execution_quote_observation_id not in observation_ids
        or step.execution_result.exited_at is None
        or step.execution_result.exited_at > source.query_cutoff
        for step in steps
    ):
        raise RiskBlock("PHASE1_EXIT_EXECUTION_UNRESOLVED")

    partial_steps = tuple(
        step for step in steps if step.event_kind == "PARTIAL_EXIT"
    )
    first_step = steps[0]
    mechanically_independent_full = (
        len(steps) == 1
        and first_step.event_kind == "CLOSE"
        and (
            first_step.execution_result.exit_reason
            in {
                ExitReason.STOP,
                ExitReason.GAP_STOP,
                ExitReason.STOP_FIRST_CONSERVATIVE,
            }
            or "MAX_HOLD_SESSIONS_REACHED" in first_step.action.reason_codes
            or (
                first_step.execution_result.exit_reason is ExitReason.TARGET
                and position.shares == 1
            )
        )
    )
    if partial_steps and evidence_status != "CLEAR":
        raise RiskBlock("PHASE1_EXIT_EVENT_EVIDENCE_UNRESOLVED")
    if (
        evidence_status in {"UNRESOLVED", "UNAVAILABLE"}
        and not mechanically_independent_full
    ):
        raise RiskBlock("PHASE1_EXIT_EVENT_EVIDENCE_UNRESOLVED")
    evidence_dependent = bool(partial_steps) or any(
        step.execution_result.exit_reason
        in {
            ExitReason.EVENT_EXIT_REQUIRED,
            ExitReason.THESIS_INVALIDATED,
        }
        for step in steps
    )
    if (
        evidence_dependent
        and (
            position_evidence is None
            or position_evidence.review_at
            > first_step.execution_result.exited_at
        )
    ):
        raise RiskBlock("PHASE1_EXIT_EVIDENCE_LOOKAHEAD")
    if (
        source.fee_schedule_version != _PHASE1_EXIT_FEE_SCHEDULE_VERSION
        or source.fee_schedule_digest != _phase1_exit_fee_schedule_digest()
        or source.fee_micros != _PHASE1_EXIT_FEE_MICROS
    ):
        raise RiskBlock("PHASE1_EXIT_FEE_SCHEDULE_MISMATCH")
    mark_observation_id = getattr(
        market.mark_observation,
        "observation_id",
        None,
    )
    if type(mark_observation_id) is not str or not mark_observation_id:
        raise RiskBlock("PHASE1_EXIT_REVIEW_MARK_UNAVAILABLE")
    return Phase1DerivedPositionExit(
        event_context=event_context,
        mark=mark,
        steps=steps,
        mark_observation_id=mark_observation_id,
        position_evidence_digest=position_evidence_digest,
    )


def _derive_phase1_position_exit_steps_from_verified_source(
    source: object,
    *,
    position: Position,
    calendar_resolver: SessionCalendarResolver,
    policy: Policy,
) -> tuple[Phase1PositionExitStep, ...]:
    """Return exact exit steps for restart replay from raw-bound material."""
    return _derive_phase1_position_exit_material_from_verified_source(
        source,
        position=position,
        calendar_resolver=calendar_resolver,
        policy=policy,
    ).steps


def _issue_phase1_position_exit_authority_from_source(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
    policy: Policy,
) -> Phase1PositionExitAuthority:
    """Recompute the complete canonical exit batch from one exact review."""
    from .journal import (
        Phase1ExitReviewSource,
        _is_current_phase1_source_authority_without_callbacks,
        is_verified_phase1_exit_review_source,
        phase1_sources_share_owner,
    )
    from .ledger import _issue_canonical_ledger_replay_from_phase1_source

    if not isinstance(source, Phase1ExitReviewSource) or not (
        is_verified_phase1_exit_review_source(source)
    ):
        raise RiskBlock("PHASE1_EXIT_REVIEW_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if not isinstance(policy, Policy):
        raise RiskBlock("INVALID_POLICY")
    policy.validate()
    calendar_digest = _calendar_digest(calendar_resolver)
    market_source = source.market_source
    replay_source = source.canonical_replay_source
    signal_source = source.signal_source
    if (
        source.calendar_digest != calendar_digest
        or market_source.calendar_digest != calendar_digest
        or replay_source.query_cutoff != source.query_cutoff
        or market_source.query_cutoff != source.query_cutoff
        or replay_source.publication_predecessor
        or source.signal_source is not market_source.signal_source
        or source.signal_id != signal_source.signal_id
        or source.validation_window_id != signal_source.validation_window_id
        or source.validation_window_id != replay_source.validation_window_id
        or source.symbol != signal_source.symbol
        or signal_source.role != "PRIMARY"
        or not phase1_sources_share_owner(source, market_source)
        or not phase1_sources_share_owner(source, replay_source)
        or not phase1_sources_share_owner(source, signal_source)
    ):
        raise RiskBlock("PHASE1_EXIT_SOURCE_LINEAGE_MISMATCH")
    replay = _issue_canonical_ledger_replay_from_phase1_source(replay_source)
    projected = tuple(
        item
        for item in replay.ledger_pair.canonical.open_positions
        if item.signal_id == source.signal_id
    )
    if len(projected) != 1:
        raise RiskBlock("PHASE1_EXIT_POSITION_MISMATCH")
    projected_position = projected[0]
    try:
        initial_stop = money_from_micros(
            signal_source.recommended_stop_micros
        )
    except (AttributeError, DomainValidationError):
        raise RiskBlock("PHASE1_EXIT_POSITION_MISMATCH") from None
    entered_session = projected_position.lots[0].at.astimezone(_ET).date()
    position = Position(
        signal_id=source.signal_id,
        symbol=source.symbol,
        entry=projected_position.entry,
        shares=projected_position.shares,
        initial_stop=initial_stop,
        recommended_stop=projected_position.recommended_stop,
        user_confirmed_stop=None,
        target=projected_position.target,
        tick_size=projected_position.tick_size,
        entered_session=entered_session,
        ledger_name="CANONICAL",
        profit_target_taken=projected_position.profit_target_taken,
    )
    if (
        projected_position.symbol != source.symbol
        or projected_position.ledger_name != "CANONICAL"
        or signal_source.maximum_entry_micros
        != money_to_micros(projected_position.entry)
        or signal_source.target_micros
        != money_to_micros(projected_position.target)
        or signal_source.tick_size_micros
        != money_to_micros(projected_position.tick_size)
    ):
        raise RiskBlock("PHASE1_EXIT_POSITION_MISMATCH")

    derived = _derive_phase1_position_exit_material_from_verified_source(
        market_source,
        position=position,
        calendar_resolver=calendar_resolver,
        policy=policy,
    )
    position_evidence = source.position_evidence
    if (
        (position_evidence is None)
        != (derived.position_evidence_digest is None)
        or (
            position_evidence is not None
            and (
                not is_issued_phase1_position_evidence_authority(
                    position_evidence
                )
                or position_evidence.position != position
                or position_evidence.source_digest
                != derived.position_evidence_digest
            )
        )
    ):
        raise RiskBlock("PHASE1_EXIT_EVENT_EVIDENCE_MISMATCH")
    event_context = derived.event_context
    mark = derived.mark
    steps = derived.steps
    mark_observation_id = derived.mark_observation_id
    position_evidence_digest = derived.position_evidence_digest
    digest_payload = _phase1_position_exit_authority_payload(
        position=position,
        event_context=event_context,
        mark=mark,
        steps=steps,
        mark_observation_id=mark_observation_id,
        position_evidence_digest=position_evidence_digest,
        fee_schedule_version=source.fee_schedule_version,
        fee_schedule_digest=source.fee_schedule_digest,
        validation_window_id=source.validation_window_id,
        query_cutoff=source.query_cutoff,
        source_digest=source.source_digest,
    )
    authority_digest = sha256(
        json.dumps(
            digest_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    authority = Phase1PositionExitAuthority(
        position=position,
        event_context=event_context,
        mark=mark,
        steps=steps,
        mark_observation_id=mark_observation_id,
        position_evidence_digest=position_evidence_digest,
        fee_schedule_version=source.fee_schedule_version,
        fee_schedule_digest=source.fee_schedule_digest,
        validation_window_id=source.validation_window_id,
        query_cutoff=source.query_cutoff,
        source_digest=source.source_digest,
        authority_digest=authority_digest,
    )
    mark_observation_matches = tuple(
        fact
        for fact in (
            *tuple(market_source.daily_bar_facts),
            *tuple(market_source.quote_facts),
        )
        if getattr(fact, "observation_id", None) == mark_observation_id
    )
    if len(mark_observation_matches) != 1:
        raise RiskBlock("PHASE1_EXIT_REVIEW_MARK_UNAVAILABLE")
    final_market_material = _Phase1ExitMarketMaterial(
        observations=tuple(market_source.observations),
        mark_observation=mark_observation_matches[0],
        mark_price=mark.price,
        mark_at=mark.at,
        previous_session_low=mark.previous_session_low,
        current_session_low=mark.current_session_low,
        atr14=mark.atr14,
    )
    # All calendar, Journal-currentness, policy, and derived-evidence callbacks
    # are complete.  This final pass is deliberately callback-free and proves
    # both exact source identities plus every provider-derived market scalar
    # immediately before the authority becomes visible.
    if (
        not _is_current_phase1_source_authority_without_callbacks(source)
        or not _phase1_exit_market_material_matches_current_provider(
            market_source,
            final_market_material,
        )
    ):
        raise RiskBlock("PHASE1_EXIT_PROVIDER_COHORT_UNVERIFIED")
    _install_risk_authority(
        _PHASE1_POSITION_EXIT_AUTHORITIES,
        authority,
        exact_type=Phase1PositionExitAuthority,
        children=(event_context, mark, source),
        phase1_bindings=((source, "EXIT_REVIEW"),),
    )
    return authority


@dataclass(frozen=True, slots=True)
class EquityPoint:
    """One valid end-of-session or ordered-close equity observation."""

    session_date: date
    equity: Decimal
    at: datetime | None = None
    cursor: int | None = None
    ordinal: int = 0
    source_id: str | None = None
    message_time: datetime | None = None
    received_at: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise RiskBlock("INVALID_EQUITY_SESSION")
        object.__setattr__(
            self,
            "equity",
            _require_money(
                self.equity,
                reason_code="INVALID_EQUITY",
                nonnegative=True,
            ),
        )
        _validate_authority_fields(
            self.session_date,
            self.at,
            self.cursor,
            self.ordinal,
            "EQUITY",
        )
        if self.source_id is not None and (
            type(self.source_id) is not str or not self.source_id
        ):
            raise RiskBlock("INVALID_EQUITY_SOURCE_ID")
        _validate_source_times(
            self.at,
            self.message_time,
            self.received_at,
            "EQUITY",
        )


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    """One ordered realized result; zero profit resets the loss count."""

    session_date: date
    pnl: Decimal
    signal_id: str = ""
    at: datetime | None = None
    cursor: int | None = None
    ordinal: int = 0
    equity_after: Decimal | None = None
    source_id: str | None = None
    message_time: datetime | None = None
    received_at: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise RiskBlock("INVALID_CLOSE_SESSION")
        object.__setattr__(
            self,
            "pnl",
            _require_money(self.pnl, reason_code="INVALID_REALIZED_PNL"),
        )
        if type(self.signal_id) is not str or not self.signal_id:
            raise RiskBlock("INVALID_CLOSED_TRADE_SIGNAL")
        if self.equity_after is not None:
            object.__setattr__(
                self,
                "equity_after",
                _require_money(
                    self.equity_after,
                    reason_code="INVALID_POST_CLOSE_EQUITY",
                    nonnegative=True,
                ),
            )
        _validate_authority_fields(
            self.session_date,
            self.at,
            self.cursor,
            self.ordinal,
            "CLOSE",
        )
        if self.source_id is not None and (
            type(self.source_id) is not str or not self.source_id
        ):
            raise RiskBlock("INVALID_CLOSE_SOURCE_ID")
        _validate_source_times(
            self.at,
            self.message_time,
            self.received_at,
            "CLOSE",
        )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class BreakerHistoryAuthority:
    """Exact complete strategy-ledger history issued by a persistence adapter."""

    ledger_name: str
    equity: tuple[EquityPoint, ...]
    closes: tuple[ClosedTrade, ...]
    start_cursor: int
    terminal_cursor: int
    close_start_cursor: int | None
    close_terminal_cursor: int | None
    through_session: date
    calendar_resolver: SessionCalendarResolver
    source_digest: str
    query_cutoff: datetime
    equity_expected_count: int
    close_expected_count: int
    close_stream_through_cursor: int
    validation_window_id: str
    window_start_session: date
    window_start_source_id: str

    def __post_init__(self) -> None:
        if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
            raise RiskBlock("INVALID_BREAKER_LEDGER")
        if type(self.equity) is not tuple or any(
            not isinstance(point, EquityPoint) for point in self.equity
        ):
            raise RiskBlock("INVALID_BREAKER_HISTORY")
        if type(self.closes) is not tuple or any(
            not isinstance(trade, ClosedTrade) for trade in self.closes
        ):
            raise RiskBlock("INVALID_BREAKER_HISTORY")
        _require_positive_int(self.start_cursor, "INVALID_BREAKER_CURSOR")
        _require_positive_int(self.terminal_cursor, "INVALID_BREAKER_CURSOR")
        if self.terminal_cursor < self.start_cursor:
            raise RiskBlock("INVALID_BREAKER_CURSOR")
        if (self.close_start_cursor is None) != (
            self.close_terminal_cursor is None
        ):
            raise RiskBlock("INVALID_BREAKER_CURSOR")
        if self.close_start_cursor is not None:
            _require_positive_int(
                self.close_start_cursor,
                "INVALID_BREAKER_CURSOR",
            )
            _require_positive_int(
                self.close_terminal_cursor,
                "INVALID_BREAKER_CURSOR",
            )
            if self.close_terminal_cursor < self.close_start_cursor:
                raise RiskBlock("INVALID_BREAKER_CURSOR")
        if type(self.through_session) is not date:
            raise RiskBlock("INVALID_BREAKER_AS_OF")
        query_cutoff = _require_aware(
            self.query_cutoff,
            "INVALID_BREAKER_QUERY_CUTOFF",
        )
        _require_nonnegative_int(
            self.equity_expected_count,
            "INVALID_BREAKER_HISTORY_COUNT",
        )
        _require_nonnegative_int(
            self.close_expected_count,
            "INVALID_BREAKER_HISTORY_COUNT",
        )
        _require_nonnegative_int(
            self.close_stream_through_cursor,
            "INVALID_BREAKER_CURSOR",
        )
        if (
            self.equity_expected_count != len(self.equity)
            or self.close_expected_count != len(self.closes)
        ):
            raise RiskBlock("BREAKER_HISTORY_COUNT_MISMATCH")
        if any(
            fact.received_at is None or fact.received_at > query_cutoff
            for fact in (*self.equity, *self.closes)
        ):
            raise RiskBlock("BREAKER_HISTORY_LOOKAHEAD")
        if (
            type(self.validation_window_id) is not str
            or len(self.validation_window_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.validation_window_id
            )
            or type(self.window_start_session) is not date
            or type(self.window_start_source_id) is not str
            or not self.window_start_source_id
            or not self.equity
            or self.window_start_session != self.equity[0].session_date
            or self.window_start_source_id != self.equity[0].source_id
        ):
            raise RiskBlock("INVALID_BREAKER_VALIDATION_WINDOW")
        if (
            self.close_terminal_cursor is not None
            and self.close_terminal_cursor > self.close_stream_through_cursor
        ):
            raise RiskBlock("INVALID_BREAKER_CURSOR")
        if not isinstance(self.calendar_resolver, SessionCalendarResolver):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        if (
            type(self.source_digest) is not str
            or len(self.source_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.source_digest)
        ):
            raise RiskBlock("INVALID_BREAKER_SOURCE_DIGEST")


def _breaker_history_fingerprint(
    history: BreakerHistoryAuthority,
) -> object:
    """Return a hook-free exact structural seal for one breaker history."""
    if type(history) is not BreakerHistoryAuthority:
        raise TypeError("breaker history authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        history,
        domain=_BREAKER_HISTORY_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _is_current_breaker_history_authority_without_callbacks(
    history: object,
) -> bool:
    """Pure final history, binding, and source-identity verification."""
    return _is_current_risk_authority_without_callbacks(
        _BREAKER_HISTORY_AUTHORITIES,
        history,
        exact_type=BreakerHistoryAuthority,
    )


def is_issued_breaker_history_authority(history: object) -> bool:
    from .journal import Phase1BreakerHistorySource

    if type(history) is not BreakerHistoryAuthority:
        return False
    with _AUTHORITY_LOCK:
        binding = _PHASE1_DERIVED_SOURCE_BINDINGS.get(id(history))
        if (
            binding is None
            or binding[0]() is not history
            or len(binding[1]) != 1
            or binding[1][0][1] != "BREAKER_HISTORY"
            or type(binding[1][0][0]) is not Phase1BreakerHistorySource
        ):
            return False
        source = binding[1][0][0]
    # Source and calendar verification may execute SQLite or accepted resolver
    # callbacks.  Finish all of them before the hook-free authority seal and
    # exact registry/binding recheck.
    if not _phase1_derived_sources_are_current(history):
        return False
    try:
        calendar_digest = _calendar_digest(history.calendar_resolver)
    except Exception:
        return False
    if (
        calendar_digest != source.calendar_digest
        or history.source_digest != source.source_digest
    ):
        return False
    return _is_current_breaker_history_authority_without_callbacks(history)


def _issue_breaker_history_authority(
    *,
    ledger_name: str,
    equity: Sequence[EquityPoint],
    closes: Sequence[ClosedTrade],
    through_session: date,
    terminal_cursor: int,
    calendar_resolver: SessionCalendarResolver,
    query_cutoff: datetime | None = None,
    equity_expected_count: int | None = None,
    close_expected_count: int | None = None,
    close_stream_through_cursor: int | None = None,
) -> BreakerHistoryAuthority:
    """Validate a diagnostic history without authenticating source rows.

    Task 8 must add a persistence-backed issuer that proves the complete equity
    and close query cohorts.  This helper deliberately leaves its result
    unissued.
    """
    points = tuple(equity)
    trades = tuple(closes)
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if not points or points[0].equity != _VALIDATION_CAPITAL:
        raise RiskBlock("BREAKER_EQUITY_INCOMPLETE")
    if any(
        point.at is None
        or point.cursor is None
        or point.source_id is None
        or point.message_time is None
        or point.received_at is None
        for point in points
    ) or any(
        trade.at is None
        or trade.cursor is None
        or trade.source_id is None
        or trade.message_time is None
        or trade.received_at is None
        for trade in trades
    ):
        raise RiskBlock("BREAKER_HISTORY_SOURCE_INCOMPLETE")
    received_times = tuple(
        fact.received_at for fact in (*points, *trades)
    )
    if query_cutoff is None:
        query_cutoff = max(received_times)
    query_cutoff = _require_aware(
        query_cutoff,
        "INVALID_BREAKER_QUERY_CUTOFF",
    )
    if any(received_at > query_cutoff for received_at in received_times):
        raise RiskBlock("BREAKER_HISTORY_LOOKAHEAD")
    if equity_expected_count is None:
        equity_expected_count = len(points)
    if close_expected_count is None:
        close_expected_count = len(trades)
    _require_nonnegative_int(
        equity_expected_count,
        "INVALID_BREAKER_HISTORY_COUNT",
    )
    _require_nonnegative_int(
        close_expected_count,
        "INVALID_BREAKER_HISTORY_COUNT",
    )
    if (
        equity_expected_count != len(points)
        or close_expected_count != len(trades)
    ):
        raise RiskBlock("BREAKER_HISTORY_COUNT_MISMATCH")
    for fact in (*points, *trades):
        assert fact.at is not None
        try:
            fact_session = calendar_resolver.session(fact.session_date)
        except RiskBlock as error:
            raise RiskBlock("BREAKER_POINT_OUTSIDE_MARKET_SESSION") from error
        fact_clock = fact.at.astimezone(_ET).time().replace(tzinfo=None)
        if not fact_session.open_time <= fact_clock <= fact_session.close_time:
            raise RiskBlock("BREAKER_POINT_OUTSIDE_MARKET_SESSION")
    equity_cursors = tuple(point.cursor for point in points)
    close_cursors = tuple(trade.cursor for trade in trades)
    if close_stream_through_cursor is None:
        close_stream_through_cursor = (
            close_cursors[-1] if close_cursors else 0
        )
    _require_nonnegative_int(
        close_stream_through_cursor,
        "INVALID_BREAKER_CURSOR",
    )
    if (
        equity_cursors != tuple(sorted(equity_cursors))
        or len(equity_cursors) != len(set(equity_cursors))
        or len({point.source_id for point in points}) != len(points)
        or equity_cursors[0] is None
        or equity_cursors[-1] != terminal_cursor
    ):
        raise RiskBlock("BREAKER_HISTORY_CURSOR_INCOMPLETE")
    if trades and (
        close_cursors != tuple(sorted(close_cursors))
        or len(close_cursors) != len(set(close_cursors))
        or len({trade.source_id for trade in trades}) != len(trades)
    ):
        raise RiskBlock("BREAKER_HISTORY_CURSOR_INCOMPLETE")
    if trades and close_cursors[-1] > close_stream_through_cursor:
        raise RiskBlock("BREAKER_HISTORY_CURSOR_INCOMPLETE")
    try:
        session = calendar_resolver.session(through_session)
    except RiskBlock:
        raise
    terminal_points = tuple(
        point
        for point in points
        if point.session_date == through_session
        and point.at.astimezone(_ET).time().replace(tzinfo=None)
        == session.close_time
    )
    if len(terminal_points) != 1:
        raise RiskBlock("BREAKER_TERMINAL_SESSION_INCOMPLETE")
    terminal_by_session: dict[date, EquityPoint] = {}
    for point in points:
        try:
            point_session = calendar_resolver.session(point.session_date)
        except RiskBlock as exc:
            raise RiskBlock("BREAKER_SESSION_COVERAGE_INCOMPLETE") from exc
        point_time = point.at.astimezone(_ET).time().replace(tzinfo=None)
        if point_time == point_session.close_time:
            if point.session_date in terminal_by_session:
                raise RiskBlock("BREAKER_SESSION_COVERAGE_INCOMPLETE")
            terminal_by_session[point.session_date] = point
    opening_session = points[0].session_date
    expected_sessions: list[date] = []
    current = opening_session
    while current <= through_session:
        try:
            if calendar_resolver.is_open(current):
                expected_sessions.append(current)
        except RiskBlock:
            raise RiskBlock("BREAKER_SESSION_COVERAGE_INCOMPLETE") from None
        current += timedelta(days=1)
    if tuple(sorted(terminal_by_session)) != tuple(expected_sessions):
        raise RiskBlock("BREAKER_SESSION_COVERAGE_INCOMPLETE")
    assert points[0].source_id is not None
    validation_payload = {
        "version": 1,
        "ledger_name": ledger_name,
        "window_start_session": points[0].session_date.isoformat(),
        "window_start_source_id": points[0].source_id,
        "window_start_cursor": points[0].cursor,
    }
    validation_window_id = sha256(
        json.dumps(
            validation_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    source_payload = {
        "version": 2,
        "ledger_name": ledger_name,
        "through_session": through_session.isoformat(),
        "query_cutoff": query_cutoff.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "equity_expected_count": equity_expected_count,
        "close_expected_count": close_expected_count,
        "equity_stream_through_cursor": terminal_cursor,
        "close_stream_through_cursor": close_stream_through_cursor,
        "validation_window_id": validation_window_id,
        "equity": [
            {
                "source_id": point.source_id,
                "session_date": point.session_date.isoformat(),
                "equity_micros": money_to_micros(point.equity),
                "at": point.at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "message_time": point.message_time.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "received_at": point.received_at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "cursor": point.cursor,
                "ordinal": point.ordinal,
            }
            for point in points
        ],
        "closes": [
            {
                "source_id": trade.source_id,
                "signal_id": trade.signal_id,
                "session_date": trade.session_date.isoformat(),
                "pnl_micros": money_to_micros(trade.pnl),
                "equity_after_micros": (
                    None
                    if trade.equity_after is None
                    else money_to_micros(trade.equity_after)
                ),
                "at": trade.at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "message_time": trade.message_time.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "received_at": trade.received_at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "cursor": trade.cursor,
                "ordinal": trade.ordinal,
            }
            for trade in trades
        ],
    }
    source_digest = sha256(
        json.dumps(
            source_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    history = BreakerHistoryAuthority(
        ledger_name=ledger_name,
        equity=points,
        closes=trades,
        start_cursor=equity_cursors[0],  # type: ignore[arg-type]
        terminal_cursor=terminal_cursor,
        close_start_cursor=(close_cursors[0] if close_cursors else None),
        close_terminal_cursor=(close_cursors[-1] if close_cursors else None),
        through_session=through_session,
        calendar_resolver=calendar_resolver,
        source_digest=source_digest,
        query_cutoff=query_cutoff,
        equity_expected_count=equity_expected_count,
        close_expected_count=close_expected_count,
        close_stream_through_cursor=close_stream_through_cursor,
        validation_window_id=validation_window_id,
        window_start_session=points[0].session_date,
        window_start_source_id=points[0].source_id,
    )
    return history


def _phase1_breaker_equity_authorities_are_current(
    source: object,
    mark_sources: tuple[object, ...],
    authorities: tuple[object, ...],
) -> bool:
    """Purely recheck the complete exact mark/authority cohort."""
    from .journal import (
        Phase1BreakerHistorySource,
        Phase1EquityMarkSource,
    )

    if (
        type(source) is not Phase1BreakerHistorySource
        or len(mark_sources) != len(authorities)
    ):
        return False
    return all(
        type(mark_source) is Phase1EquityMarkSource
        and type(authority) is Phase1EquityPointAuthority
        and _is_current_phase1_equity_point_authority_without_callbacks(
            authority,
            mark_source,
        )
        for mark_source, authority in zip(
            mark_sources,
            authorities,
            strict=True,
        )
    )


def _issue_breaker_history_from_phase1_source(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
) -> BreakerHistoryAuthority:
    """Issue breaker history only from one complete owner-current Phase 1 read."""
    from .journal import (
        Phase1BreakerHistorySource,
        Phase1EquityMarkSource,
        _is_current_phase1_source_authority_without_callbacks,
        is_verified_phase1_breaker_history_source,
        phase1_sources_share_owner,
    )

    if not isinstance(source, Phase1BreakerHistorySource) or not (
        is_verified_phase1_breaker_history_source(source)
    ):
        raise RiskBlock("PHASE1_BREAKER_HISTORY_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    if _calendar_digest(calendar_resolver) != source.calendar_digest:
        raise RiskBlock("PHASE1_CALENDAR_SOURCE_MISMATCH")
    persisted_points = tuple(source.equity_points)
    equity_mark_sources = tuple(source.equity_mark_sources)
    equity_authorities = tuple(source.equity_authorities)
    if (
        source.expected_mark_source_count != len(equity_mark_sources)
        or len(equity_authorities) != len(equity_mark_sources)
        or len(equity_mark_sources) != max(0, len(persisted_points) - 1)
        or (
            bool(equity_mark_sources)
            and not phase1_sources_share_owner(
                source,
                *equity_mark_sources,
            )
        )
    ):
        raise RiskBlock("PHASE1_BREAKER_EQUITY_AUTHORITY_INCOMPLETE")
    for point, mark_source, authority in zip(
        persisted_points[1:],
        equity_mark_sources,
        equity_authorities,
        strict=True,
    ):
        expected_point_id = sha256(
            (
                "stock-monitor/phase1-session-equity-point/v1\x00"
                + source.validation_window_id
                + "\x00"
                + source.ledger_name
                + "\x00"
                + point.session_date.isoformat()
                + "\x00"
                + getattr(authority, "authority_digest", "")
            ).encode("utf-8")
        ).hexdigest()
        bound_sources = tuple(
            bound_source
            for bound_source, kind in _phase1_bound_sources(authority)
            if kind == "EQUITY_MARK"
        )
        if (
            not isinstance(authority, Phase1EquityPointAuthority)
            or not isinstance(mark_source, Phase1EquityMarkSource)
            or not is_issued_phase1_equity_point_authority(authority)
            or bound_sources != (mark_source,)
            or authority.source_digest != mark_source.source_digest
            or authority.validation_window_id != source.validation_window_id
            or authority.ledger_name != source.ledger_name
            or authority.session_date != point.session_date
            or authority.point_at != point.at
            or authority.query_cutoff != point.received_at
            or mark_source.query_cutoff != point.received_at
            or authority.query_cutoff > source.query_cutoff
            or point.point_id != expected_point_id
            or authority.mark_source_digest != point.mark_source_digest
            or money_to_micros(authority.point.cash) != point.cash_micros
            or money_to_micros(authority.point.positions_value)
            != point.positions_value_micros
            or money_to_micros(authority.point.equity) != point.equity_micros
            or money_to_micros(authority.point.external_cash_flow)
            != point.external_cash_flow_micros
        ):
            raise RiskBlock("PHASE1_BREAKER_EQUITY_AUTHORITY_MISMATCH")

    # Validate every calendar/session rule against the durable point values
    # first.  Operational authority values are copied only after all of those
    # caller-dispatchable callbacks have completed.
    persisted_value_points = tuple(
        EquityPoint(
            session_date=point.session_date,
            equity=money_from_micros(point.equity_micros),
            at=point.at,
            cursor=point.source_cursor,
            source_id=point.point_id,
            message_time=point.message_time,
            received_at=point.received_at,
        )
        for point in persisted_points
    )
    terminal_equity_by_session = {
        point.session_date: point
        for point in persisted_value_points
        if point.at is not None
        and point.at.astimezone(_ET).time().replace(tzinfo=None)
        == calendar_resolver.session(point.session_date).close_time
    }
    trades = tuple(
        ClosedTrade(
            session_date=trade.session_date,
            pnl=money_from_micros(trade.pnl_micros),
            signal_id=trade.signal_id,
            at=trade.at,
            cursor=trade.row_id,
            equity_after=(
                terminal_equity_by_session[trade.session_date].equity
                if trade.session_date in terminal_equity_by_session
                and terminal_equity_by_session[trade.session_date].at is not None
                and terminal_equity_by_session[trade.session_date].at >= trade.at
                else None
            ),
            source_id=trade.trade_id,
            message_time=trade.message_time,
            received_at=trade.received_at,
        )
        for trade in source.closed_trades
    )
    if (
        source.expected_equity_count != len(persisted_value_points)
        or source.expected_close_count != len(trades)
        or not persisted_value_points
        or source.equity_terminal_cursor != persisted_value_points[-1].cursor
        or source.close_terminal_cursor
        != (trades[-1].cursor if trades else None)
        or source.window_start_session
        != persisted_value_points[0].session_date
        or source.window_start_source_id != persisted_value_points[0].source_id
        or any(
            point.validation_window_id != source.validation_window_id
            or point.ledger_name != source.ledger_name
            for point in source.equity_points
        )
        or any(
            trade.validation_window_id != source.validation_window_id
            or trade.ledger_name != source.ledger_name
            for trade in source.closed_trades
        )
    ):
        raise RiskBlock("PHASE1_BREAKER_HISTORY_SOURCE_MISMATCH")
    diagnostic = _issue_breaker_history_authority(
        ledger_name=source.ledger_name,
        equity=persisted_value_points,
        closes=trades,
        through_session=source.through_session,
        terminal_cursor=source.equity_terminal_cursor,
        calendar_resolver=calendar_resolver,
        query_cutoff=source.query_cutoff,
        equity_expected_count=source.expected_equity_count,
        close_expected_count=source.expected_close_count,
        close_stream_through_cursor=source.close_source_highwater,
    )
    if (
        not _is_current_phase1_source_authority_without_callbacks(source)
        or not _phase1_breaker_equity_authorities_are_current(
            source,
            equity_mark_sources,
            equity_authorities,
        )
    ):
        raise RiskBlock("PHASE1_BREAKER_EQUITY_AUTHORITY_INCOMPLETE")
    points = tuple(
        EquityPoint(
            session_date=point.session_date,
            equity=(
                money_from_micros(point.equity_micros)
                if index == 0
                else equity_authorities[index - 1].point.equity
            ),
            at=point.at,
            cursor=point.source_cursor,
            source_id=point.point_id,
            message_time=point.message_time,
            received_at=point.received_at,
        )
        for index, point in enumerate(persisted_points)
    )
    history = replace(
        diagnostic,
        equity=points,
        source_digest=source.source_digest,
        validation_window_id=source.validation_window_id,
        window_start_session=source.window_start_session,
        window_start_source_id=source.window_start_source_id,
    )
    if (
        not _phase1_breaker_equity_authorities_are_current(
            source,
            equity_mark_sources,
            equity_authorities,
        )
        or any(
            point.equity != authority.point.equity
            for point, authority in zip(
                history.equity[1:],
                equity_authorities,
                strict=True,
            )
        )
    ):
        raise RiskBlock("PHASE1_BREAKER_EQUITY_AUTHORITY_INCOMPLETE")
    _install_risk_authority(
        _BREAKER_HISTORY_AUTHORITIES,
        history,
        exact_type=BreakerHistoryAuthority,
        phase1_bindings=((source, "BREAKER_HISTORY"),),
    )
    return history


@dataclass(frozen=True, slots=True, weakref_slot=True)
class BreakerState:
    """Per-ledger breaker result evaluated from immutable ordered series."""

    as_of: date | None
    live_entries_paused: bool
    reason_codes: tuple[str, ...]
    consecutive_losses: int
    loss_trigger_session: date | None
    loss_pause_through: date | None
    loss_resume_session: date | None
    weekly_high_water: Decimal | None
    weekly_drawdown: Decimal | None
    weekly_pause_through: date | None
    monthly_high_water: Decimal | None
    monthly_drawdown: Decimal | None
    monthly_pause_through: date | None
    canonical_observations_continue: bool = True
    ledger_name: str | None = None
    history_digest: str | None = None
    calendar_digest: str | None = None
    _evaluation_authority: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_BREAKER_STATE"),
        )
        if self.as_of is not None and type(self.as_of) is not date:
            raise RiskBlock("INVALID_BREAKER_AS_OF")
        if type(self.live_entries_paused) is not bool:
            raise RiskBlock("INVALID_BREAKER_STATE")
        if self.live_entries_paused != bool(self.reason_codes):
            raise RiskBlock("INVALID_BREAKER_STATE")
        _require_nonnegative_int(self.consecutive_losses, "INVALID_LOSS_COUNT")
        if type(self.canonical_observations_continue) is not bool:
            raise RiskBlock("INVALID_BREAKER_STATE")
        lineage = (self.ledger_name, self.history_digest, self.calendar_digest)
        if any(value is not None for value in lineage):
            if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
                raise RiskBlock("INVALID_BREAKER_LINEAGE")
            for digest in (self.history_digest, self.calendar_digest):
                if (
                    type(digest) is not str
                    or len(digest) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in digest
                    )
                ):
                    raise RiskBlock("INVALID_BREAKER_LINEAGE")
        for attribute, code in (
            ("weekly_high_water", "INVALID_WEEKLY_HIGH_WATER"),
            ("weekly_drawdown", "INVALID_WEEKLY_DRAWDOWN"),
            ("monthly_high_water", "INVALID_MONTHLY_HIGH_WATER"),
            ("monthly_drawdown", "INVALID_MONTHLY_DRAWDOWN"),
        ):
            value = getattr(self, attribute)
            if value is not None:
                object.__setattr__(
                    self,
                    attribute,
                    _require_money(
                        value,
                        reason_code=code,
                        nonnegative=True,
                    ),
                )

    @property
    def paused(self) -> bool:
        return self.live_entries_paused


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PairedBreakerState:
    """Strictest canonical/actual pause used for new live entries."""

    as_of: date
    live_entries_paused: bool
    reason_codes: tuple[str, ...]
    canonical: BreakerState
    actual: BreakerState
    canonical_observations_continue: bool = True
    _evaluation_authority: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_BREAKER_STATE"),
        )
        if type(self.as_of) is not date:
            raise RiskBlock("INVALID_BREAKER_AS_OF")
        if type(self.live_entries_paused) is not bool:
            raise RiskBlock("INVALID_BREAKER_STATE")
        if self.live_entries_paused != bool(self.reason_codes):
            raise RiskBlock("INVALID_BREAKER_STATE")
        if not isinstance(self.canonical, BreakerState) or not isinstance(
            self.actual,
            BreakerState,
        ):
            raise RiskBlock("INVALID_BREAKER_STATE")
        if self.canonical_observations_continue is not True:
            raise RiskBlock("INVALID_BREAKER_STATE")
        if self.canonical.as_of != self.as_of or self.actual.as_of != self.as_of:
            expected_paused = True
            expected_reasons = ("BREAKER_AS_OF_MISMATCH",)
        else:
            expected_paused = (
                self.canonical.live_entries_paused
                or self.actual.live_entries_paused
            )
            expected_reasons = tuple(
                [
                    f"CANONICAL:{reason}"
                    for reason in self.canonical.reason_codes
                ]
                + [f"ACTUAL:{reason}" for reason in self.actual.reason_codes]
            )
        if (
            self.live_entries_paused is not expected_paused
            or self.reason_codes != expected_reasons
        ):
            raise RiskBlock("INVALID_BREAKER_STATE")
    @property
    def paused(self) -> bool:
        return self.live_entries_paused


def breaker_pauses_entry(
    state: BreakerState | PairedBreakerState,
    entry_session: date,
) -> bool:
    """Evaluate typed breaker expiry against the proposed entry session."""
    if type(entry_session) is not date:
        raise RiskBlock("INVALID_SESSION_DATE")
    if isinstance(state, PairedBreakerState):
        if not state.live_entries_paused:
            return False
        if (
            state.canonical.as_of != state.as_of
            or state.actual.as_of != state.as_of
        ):
            return True
        return breaker_pauses_entry(
            state.canonical,
            entry_session,
        ) or breaker_pauses_entry(state.actual, entry_session)
    if not isinstance(state, BreakerState):
        raise TypeError("entry breaker must be a BreakerState value")
    if not state.live_entries_paused:
        return False

    temporary_windows = {
        "CONSECUTIVE_LOSS_LIMIT": state.loss_pause_through,
        "WEEKLY_DRAWDOWN_LIMIT": state.weekly_pause_through,
        "MONTHLY_DRAWDOWN_LIMIT": state.monthly_pause_through,
    }
    if any(reason not in temporary_windows for reason in state.reason_codes):
        return True
    return any(
        pause_through is None or entry_session <= pause_through
        for reason, pause_through in temporary_windows.items()
        if reason in state.reason_codes
    )


def _breaker_fingerprint(state: BreakerState) -> tuple[object, ...]:
    return (
        state.as_of,
        state.live_entries_paused,
        state.reason_codes,
        state.consecutive_losses,
        state.loss_trigger_session,
        state.loss_pause_through,
        state.loss_resume_session,
        state.weekly_high_water,
        state.weekly_drawdown,
        state.weekly_pause_through,
        state.monthly_high_water,
        state.monthly_drawdown,
        state.monthly_pause_through,
        state.canonical_observations_continue,
        state.ledger_name,
        state.history_digest,
        state.calendar_digest,
    )


def _is_current_breaker_state_without_callbacks(state: object) -> bool:
    if type(state) is not BreakerState:
        return False
    children = _registered_risk_authority_children(
        _BREAKER_AUTHORITIES,
        state,
    )
    if (
        children is None
        or len(children) != 1
        or type(children[0]) is not BreakerHistoryAuthority
        or not _phase1_derived_sources_are_current_without_callbacks(state)
        or not _is_current_breaker_history_authority_without_callbacks(
            children[0]
        )
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _BREAKER_AUTHORITIES,
        state,
        exact_type=BreakerState,
        children=children,
    )


def is_issued_breaker_state(state: object) -> bool:
    if type(state) is not BreakerState:
        return False
    children = _registered_risk_authority_children(
        _BREAKER_AUTHORITIES,
        state,
    )
    if (
        children is None
        or len(children) != 1
        or type(children[0]) is not BreakerHistoryAuthority
        or not _phase1_derived_sources_are_current(state)
        or not is_issued_breaker_history_authority(children[0])
    ):
        return False
    return _is_current_breaker_state_without_callbacks(state)


def _paired_breaker_fingerprint(
    state: PairedBreakerState,
) -> tuple[object, ...]:
    return (
        state.as_of,
        state.live_entries_paused,
        state.reason_codes,
        _breaker_fingerprint(state.canonical),
        _breaker_fingerprint(state.actual),
        state.canonical_observations_continue,
    )


def is_issued_paired_breaker_state(state: object) -> bool:
    if type(state) is not PairedBreakerState:
        return False
    if (
        type(state.canonical) is not BreakerState
        or type(state.actual) is not BreakerState
        or not is_issued_breaker_state(state.canonical)
        or not is_issued_breaker_state(state.actual)
        or not _phase1_derived_sources_are_current(state)
    ):
        return False
    if not (
        _phase1_derived_sources_are_current_without_callbacks(state)
        and _is_current_breaker_state_without_callbacks(state.canonical)
        and _is_current_breaker_state_without_callbacks(state.actual)
    ):
        return False
    return _is_current_risk_authority_without_callbacks(
        _PAIRED_BREAKER_AUTHORITIES,
        state,
        exact_type=PairedBreakerState,
        children=(state.canonical, state.actual),
    )


def _validate_authority_fields(
    session_date: date,
    at: datetime | None,
    cursor: int | None,
    ordinal: int,
    prefix: str,
) -> None:
    if at is not None:
        at = _require_aware(at, f"INVALID_{prefix}_TIME")
        if at.astimezone(_ET).date() != session_date:
            raise RiskBlock(f"{prefix}_TIME_SESSION_MISMATCH")
    if cursor is not None:
        _require_positive_int(cursor, f"INVALID_{prefix}_CURSOR")
    _require_nonnegative_int(ordinal, f"INVALID_{prefix}_ORDINAL")


def _validate_source_times(
    effective_at: datetime | None,
    message_time: datetime | None,
    received_at: datetime | None,
    prefix: str,
) -> None:
    if (message_time is None) != (received_at is None):
        raise RiskBlock(f"{prefix}_SOURCE_TIME_INCOMPLETE")
    if message_time is None or received_at is None:
        return
    if effective_at is None:
        raise RiskBlock(f"{prefix}_SOURCE_TIME_INCOMPLETE")
    message_time = _require_aware(
        message_time,
        f"INVALID_{prefix}_MESSAGE_TIME",
    )
    received_at = _require_aware(
        received_at,
        f"INVALID_{prefix}_RECEIVED_TIME",
    )
    if not effective_at <= message_time <= received_at:
        raise RiskBlock(f"{prefix}_SOURCE_TIME_OUT_OF_ORDER")


def _authority_key(value: EquityPoint | ClosedTrade) -> tuple[datetime, int, int]:
    effective_at = (
        value.at.astimezone(UTC)
        if value.at is not None
        else datetime.combine(value.session_date, time.max, UTC)
    )
    return (effective_at, value.cursor if value.cursor is not None else 0, value.ordinal)


def _validate_breaker_session(
    calendar: MarketCalendar | SessionCalendarResolver,
    day: date,
) -> None:
    try:
        if not calendar.is_open(day):
            raise RiskBlock("BREAKER_POINT_OUTSIDE_MARKET_SESSION")
    except CalendarError:
        raise RiskBlock("CALENDAR_COVERAGE_MISSING") from None


def _month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def _period_high_water_and_drawdown(
    points: Sequence[EquityPoint],
) -> tuple[Decimal, Decimal]:
    running_high = points[0].equity
    maximum_drawdown = _ZERO
    for point in points:
        running_high = max(running_high, point.equity)
        with localcontext() as decimal_context:
            decimal_context.prec = _decimal_work_precision(
                running_high,
                point.equity,
            )
            drawdown = running_high - point.equity
        maximum_drawdown = max(maximum_drawdown, drawdown)
    return running_high, maximum_drawdown


def _period_equity_points(
    points: Sequence[EquityPoint],
    *,
    period_start: date,
    as_of: date,
) -> tuple[EquityPoint, ...]:
    in_period = tuple(
        point
        for point in points
        if period_start <= point.session_date <= as_of
    )
    prior = tuple(point for point in points if point.session_date < period_start)
    if prior:
        return (prior[-1], *in_period)
    return in_period


def _calculate_breakers(
    equity: Sequence[EquityPoint],
    closes: Sequence[ClosedTrade],
    calendar: MarketCalendar | SessionCalendarResolver,
) -> BreakerState:
    """Evaluate fixed inclusive breakers for one ledger only."""
    if isinstance(equity, (str, bytes)) or isinstance(closes, (str, bytes)):
        raise TypeError("breaker histories must be sequences")
    equity_points = tuple(equity)
    closed_trades = tuple(closes)
    if any(not isinstance(point, EquityPoint) for point in equity_points):
        raise TypeError("equity history contains an invalid value")
    if any(not isinstance(trade, ClosedTrade) for trade in closed_trades):
        raise TypeError("close history contains an invalid value")
    if not isinstance(calendar, (MarketCalendar, SessionCalendarResolver)):
        raise TypeError("breaker evaluation requires reviewed calendar authority")
    equity_dates = tuple(point.session_date for point in equity_points)
    close_dates = tuple(trade.session_date for trade in closed_trades)
    close_signal_ids = tuple(trade.signal_id for trade in closed_trades)
    if len(close_signal_ids) != len(set(close_signal_ids)):
        raise RiskBlock("DUPLICATE_CLOSED_TRADE_SIGNAL")
    equity_keys = tuple(_authority_key(point) for point in equity_points)
    close_keys = tuple(_authority_key(trade) for trade in closed_trades)
    if equity_keys != tuple(sorted(equity_keys)):
        raise RiskBlock("EQUITY_HISTORY_OUT_OF_ORDER")
    if len(equity_keys) != len(set(equity_keys)):
        raise RiskBlock("DUPLICATE_EQUITY_AUTHORITY")
    if close_keys != tuple(sorted(close_keys)):
        raise RiskBlock("CLOSE_HISTORY_OUT_OF_ORDER")
    if len(close_keys) != len(set(close_keys)):
        raise RiskBlock("DUPLICATE_CLOSE_AUTHORITY")
    for day in (*equity_dates, *close_dates):
        _validate_breaker_session(calendar, day)
    observed_dates = (*equity_dates, *close_dates)
    as_of = max(observed_dates) if observed_dates else None

    combined_equity: list[EquityPoint] = list(equity_points)
    equity_by_key = {_authority_key(point): point for point in equity_points}
    incomplete_close_equity = False
    opening_equity_incomplete = bool(observed_dates) and (
        not equity_points
        or equity_points[0].equity != _VALIDATION_CAPITAL
        or (
            bool(closed_trades)
            and _authority_key(equity_points[0])
            >= min(_authority_key(trade) for trade in closed_trades)
        )
    )
    for trade in closed_trades:
        if trade.equity_after is None:
            incomplete_close_equity = True
            continue
        close_equity = EquityPoint(
            session_date=trade.session_date,
            equity=trade.equity_after,
            at=trade.at,
            cursor=trade.cursor,
            ordinal=trade.ordinal,
        )
        key = _authority_key(close_equity)
        existing = equity_by_key.get(key)
        if existing is not None:
            if existing.equity != close_equity.equity:
                raise RiskBlock("CLOSE_EQUITY_CONFLICT")
            continue
        equity_by_key[key] = close_equity
        combined_equity.append(close_equity)
    combined_equity_points = tuple(
        sorted(combined_equity, key=_authority_key)
    )

    consecutive_losses = 0
    loss_trigger: date | None = None
    for trade in closed_trades:
        if trade.pnl < _ZERO:
            consecutive_losses += 1
            if consecutive_losses >= CONSECUTIVE_LOSS_LIMIT:
                loss_trigger = trade.session_date
        else:
            consecutive_losses = 0

    reasons: list[str] = []
    if incomplete_close_equity or opening_equity_incomplete:
        reasons.append("BREAKER_EQUITY_INCOMPLETE")
    loss_pause_through: date | None = None
    loss_resume: date | None = None
    if loss_trigger is not None and as_of is not None:
        try:
            loss_pause_through = calendar.add_sessions(
                loss_trigger,
                LOSS_PAUSE_SESSIONS,
            )
            loss_resume = calendar.add_sessions(
                loss_trigger,
                LOSS_PAUSE_SESSIONS + 1,
            )
        except (CalendarError, RiskBlock) as error:
            if isinstance(error, RiskBlock) and error.reason_code != "CALENDAR_COVERAGE_MISSING":
                raise
            reasons.extend(
                ("CONSECUTIVE_LOSS_LIMIT", "CALENDAR_COVERAGE_MISSING")
            )
        else:
            if loss_trigger <= as_of < loss_resume:
                reasons.append("CONSECUTIVE_LOSS_LIMIT")

    weekly_high_water: Decimal | None = None
    weekly_drawdown: Decimal | None = None
    weekly_pause_through: date | None = None
    monthly_high_water: Decimal | None = None
    monthly_drawdown: Decimal | None = None
    monthly_pause_through: date | None = None
    if as_of is not None:
        week_start = as_of - timedelta(days=as_of.weekday())
        week_points = _period_equity_points(
            combined_equity_points,
            period_start=week_start,
            as_of=as_of,
        )
        if week_points:
            weekly_high_water, weekly_drawdown = _period_high_water_and_drawdown(
                week_points
            )
            if weekly_drawdown >= MAX_WEEKLY_DRAWDOWN:
                reasons.append("WEEKLY_DRAWDOWN_LIMIT")
                weekly_pause_through = week_start + timedelta(days=6)

        month_points = _period_equity_points(
            combined_equity_points,
            period_start=date(as_of.year, as_of.month, 1),
            as_of=as_of,
        )
        if month_points:
            monthly_high_water, monthly_drawdown = _period_high_water_and_drawdown(
                month_points
            )
            if monthly_drawdown >= MAX_MONTHLY_DRAWDOWN:
                reasons.append("MONTHLY_DRAWDOWN_LIMIT")
                monthly_pause_through = _month_end(as_of)
    unique_reasons = tuple(dict.fromkeys(reasons))
    state = BreakerState(
        as_of=as_of,
        live_entries_paused=bool(unique_reasons),
        reason_codes=unique_reasons,
        consecutive_losses=consecutive_losses,
        loss_trigger_session=loss_trigger,
        loss_pause_through=loss_pause_through,
        loss_resume_session=loss_resume,
        weekly_high_water=weekly_high_water,
        weekly_drawdown=weekly_drawdown,
        weekly_pause_through=weekly_pause_through,
        monthly_high_water=monthly_high_water,
        monthly_drawdown=monthly_drawdown,
        monthly_pause_through=monthly_pause_through,
    )
    return state


def evaluate_breakers(
    equity: Sequence[EquityPoint],
    closes: Sequence[ClosedTrade],
    calendar: MarketCalendar | SessionCalendarResolver,
) -> BreakerState:
    """Pure diagnostic calculation; it never mints entry authority."""
    return _calculate_breakers(equity, closes, calendar)


def evaluate_authorized_breakers(
    history: BreakerHistoryAuthority,
) -> BreakerState:
    """Recompute a breaker state from an exact adapter-issued history."""
    if not is_issued_breaker_history_authority(history):
        raise RiskBlock("BREAKER_HISTORY_AUTHORITY_UNVERIFIED")
    state = _calculate_breakers(
        tuple(sorted(history.equity, key=_authority_key)),
        tuple(sorted(history.closes, key=_authority_key)),
        history.calendar_resolver,
    )
    if state.as_of != history.through_session:
        raise RiskBlock("BREAKER_TERMINAL_SESSION_INCOMPLETE")
    state = replace(
        state,
        ledger_name=history.ledger_name,
        history_digest=history.source_digest,
        calendar_digest=_calendar_digest(history.calendar_resolver),
    )
    _install_risk_authority(
        _BREAKER_AUTHORITIES,
        state,
        exact_type=BreakerState,
        children=(history,),
        phase1_bindings=_phase1_bound_sources(history),
    )
    return state


def combine_breaker_states(
    canonical: BreakerState,
    actual: BreakerState,
    *,
    as_of: date,
) -> PairedBreakerState:
    """Apply the strictest same-snapshot canonical/actual pause."""
    if not isinstance(canonical, BreakerState) or not isinstance(
        actual,
        BreakerState,
    ):
        raise TypeError("paired breakers require BreakerState values")
    if type(as_of) is not date:
        raise RiskBlock("INVALID_BREAKER_AS_OF")
    authoritative_children = is_issued_breaker_state(
        canonical
    ) and is_issued_breaker_state(actual)
    if authoritative_children and (
        canonical.ledger_name != "CANONICAL"
        or actual.ledger_name != "ACTUAL"
        or canonical.history_digest == actual.history_digest
        or canonical.calendar_digest != actual.calendar_digest
    ):
        raise RiskBlock("BREAKER_LEDGER_ROLE_MISMATCH")
    if canonical.as_of != as_of or actual.as_of != as_of:
        paired = PairedBreakerState(
            as_of=as_of,
            live_entries_paused=True,
            reason_codes=("BREAKER_AS_OF_MISMATCH",),
            canonical=canonical,
            actual=actual,
        )
    else:
        reasons = tuple(
            [f"CANONICAL:{reason}" for reason in canonical.reason_codes]
            + [f"ACTUAL:{reason}" for reason in actual.reason_codes]
        )
        paired = PairedBreakerState(
            as_of=as_of,
            live_entries_paused=canonical.live_entries_paused
            or actual.live_entries_paused,
            reason_codes=reasons,
            canonical=canonical,
            actual=actual,
        )
    if authoritative_children:
        phase1_sources = (
            *_phase1_bound_sources(canonical),
            *_phase1_bound_sources(actual),
        )
        _install_risk_authority(
            _PAIRED_BREAKER_AUTHORITIES,
            paired,
            exact_type=PairedBreakerState,
            children=(canonical, actual),
            phase1_bindings=phase1_sources,
        )
    return paired


def evaluate_paired_breakers(
    *,
    canonical_equity: Sequence[EquityPoint],
    canonical_closes: Sequence[ClosedTrade],
    actual_equity: Sequence[EquityPoint],
    actual_closes: Sequence[ClosedTrade],
    calendar: MarketCalendar | SessionCalendarResolver,
    as_of: date,
) -> PairedBreakerState:
    """Evaluate both ledger series and bind their strictest result to *as_of*."""
    canonical = evaluate_breakers(canonical_equity, canonical_closes, calendar)
    actual = evaluate_breakers(actual_equity, actual_closes, calendar)
    return combine_breaker_states(canonical, actual, as_of=as_of)


_install_risk_authority = _risk_authority_installer_factory(
    (
        (
            _LONG_PLAN_AUTHORITIES,
            LongPlanDecision,
            frozenset({_issue_long_plan_decision.__code__}),
        ),
        (
            _PHASE1_EQUITY_POINT_AUTHORITIES,
            Phase1EquityPointAuthority,
            frozenset({_issue_phase1_equity_point_from_source.__code__}),
        ),
        (
            _CONFIRMED_BUY_AUTHORITIES,
            ConfirmedBuyAction,
            frozenset({_issue_account_buy_authority.__code__}),
        ),
        (
            _JOURNAL_WINDOW_AUTHORITIES,
            JournalEventWindow,
            frozenset({_issue_account_buy_authority.__code__}),
        ),
        (
            _PHASE1_SIGNAL_EVIDENCE_AUTHORITIES,
            Phase1SignalEvidenceAuthority,
            frozenset(
                {
                    _issue_phase1_signal_evidence_authority.__code__,
                    _issue_phase1_signal_evidence_authority_from_source.__code__,
                }
            ),
        ),
        (
            _PHASE1_POSITION_EVIDENCE_AUTHORITIES,
            Phase1PositionEvidenceAuthority,
            frozenset({_issue_phase1_position_evidence_authority.__code__}),
        ),
        (
            _MARK_AUTHORITIES,
            MarketMark,
            frozenset({build_market_mark.__code__}),
        ),
        (
            _POSITION_EVENT_AUTHORITIES,
            PositionEventContext,
            frozenset(
                {
                    _derive_phase1_position_exit_material_from_verified_source.__code__,
                }
            ),
        ),
        (
            _PHASE1_POSITION_EXIT_AUTHORITIES,
            Phase1PositionExitAuthority,
            frozenset(
                {_issue_phase1_position_exit_authority_from_source.__code__}
            ),
        ),
        (
            _BREAKER_AUTHORITIES,
            BreakerState,
            frozenset({evaluate_authorized_breakers.__code__}),
        ),
        (
            _PAIRED_BREAKER_AUTHORITIES,
            PairedBreakerState,
            frozenset({combine_breaker_states.__code__}),
        ),
        (
            _PORTFOLIO_RISK_AUTHORITIES,
            PortfolioRiskAuthority,
            frozenset({_issue_portfolio_risk_authority.__code__}),
        ),
        (
            _SETTLEMENT_LEDGER_AUTHORITIES,
            SettlementLedger,
            frozenset(
                {
                    _issue_settlement_ledger_from_account_window.__code__,
                    _issue_settlement_replay.__code__,
                }
            ),
        ),
        (
            _BREAKER_HISTORY_AUTHORITIES,
            BreakerHistoryAuthority,
            frozenset({_issue_breaker_history_from_phase1_source.__code__}),
        ),
    )
)
del _risk_authority_installer_factory
del _getframe


__all__ = [
    "AccountCheck",
    "AccountCheckDecision",
    "ActualBreakerRefreshAuthority",
    "BreakerState",
    "ClosedTrade",
    "CONSECUTIVE_LOSS_LIMIT",
    "EquityPoint",
    "ExecutionEvent",
    "JournalEventWindow",
    "LOSS_PAUSE_SESSIONS",
    "LongPlanDecision",
    "LongPlanRequest",
    "MAX_MONTHLY_DRAWDOWN",
    "PortfolioState",
    "PortfolioRiskAuthority",
    "PositionPlan",
    "MAX_HOLD_SESSIONS",
    "MAX_WEEKLY_DRAWDOWN",
    "MarketMark",
    "PairedBreakerState",
    "Phase1EquityPointAuthority",
    "Phase1PositionEvidenceAuthority",
    "Phase1PositionExitAuthority",
    "Phase1PositionExitStep",
    "Phase1SignalEvidenceAuthority",
    "Position",
    "PositionEventContext",
    "PositionAction",
    "PositionAdditionDecision",
    "RiskBlock",
    "SessionCalendarResolver",
    "SettlementLedger",
    "SettlementPosting",
    "account_check_eligible",
    "breaker_pauses_entry",
    "combine_breaker_states",
    "build_market_mark",
    "evaluate_breakers",
    "evaluate_paired_breakers",
    "evaluate_account_check_window",
    "evaluate_position",
    "evaluate_position_addition",
    "evaluate_position_diagnostic",
    "is_issued_journal_event_window",
    "is_issued_actual_breaker_refresh_authority",
    "is_issued_portfolio_risk_authority",
    "is_issued_long_plan_decision",
    "is_issued_breaker_state",
    "is_issued_market_mark",
    "is_issued_paired_breaker_state",
    "is_issued_phase1_equity_point_authority",
    "is_issued_phase1_position_exit_authority",
    "is_issued_phase1_position_evidence_authority",
    "is_issued_phase1_signal_evidence_authority",
    "is_issued_position_event_context",
    "phase1_signal_evidence_manifest",
    "plan_long",
    "plan_long_diagnostic",
    "size_long",
]
