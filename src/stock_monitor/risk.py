"""Pure risk, settlement, position, and circuit-breaker decisions."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from hashlib import sha256
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
from .market_calendar import (
    CalendarError,
    MarketCalendar,
    is_release_verified_market_calendar,
    is_validated_market_calendar,
)
from .policy import Policy


_ZERO = Decimal("0")
_TWO = Decimal("2")
_R_MULTIPLE_QUANTUM = Decimal("0.000001")
_MAX_LIVE_EXPOSURE = Decimal("1000")
_MAX_POSITION_RISK = Decimal("25")
_MAX_COMBINED_RISK = Decimal("50")
_VALIDATION_CAPITAL = Decimal("5000")
_ET = ZoneInfo("America/New_York")
_MARK_SESSION_AUTHORITY = object()
_BREAKER_EVALUATION_AUTHORITY = object()
_AUTHORITY_LOCK = RLock()
_JOURNAL_WINDOW_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_CONFIRMED_BUY_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_POSITION_EVENT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_MARK_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_BREAKER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_BREAKER_HISTORY_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_PAIRED_BREAKER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_PORTFOLIO_RISK_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_ACTUAL_BREAKER_REFRESH_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_SETTLEMENT_LEDGER_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_LONG_PLAN_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
MAX_HOLD_SESSIONS = 10
CONSECUTIVE_LOSS_LIMIT = 3
LOSS_PAUSE_SESSIONS = 5
MAX_WEEKLY_DRAWDOWN = Decimal("100")
MAX_MONTHLY_DRAWDOWN = Decimal("250")
_ACCOUNT_INVALIDATING_EVENT_KINDS = frozenset(
    {
        "BUY",
        "BOUGHT",
        "CASH_ADJUSTMENT",
        "DEPOSIT",
        "FEE",
        "PARTIAL_FILL",
        "PENDING_ORDER",
        "POSITION_ADJUSTMENT",
        "RECONCILE_CASH",
        "RECONCILE_PENDING_ORDERS",
        "RECONCILE_UNRELATED_POSITION",
        "RECONCILIATION",
        "SELL",
        "SOLD",
        "STOP_FILLED",
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
) -> tuple[object, ...]:
    return (
        authority.request,
        authority.portfolio_state,
        authority.scope,
        authority.as_of,
        authority.ledger_name,
        authority.projection_through_cursor,
        authority.settlement_through_cursor,
        authority.projection_digest,
        authority.settlement_source,
        authority.settlement_digest,
        authority.policy_digest,
        authority.calendar_digest,
        authority.breaker_refresh_digest,
        authority.breaker_refresh_through_execution_cursor,
        authority.breaker_refresh_through_close_cursor,
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
    return isinstance(
        authority,
        ActualBreakerRefreshAuthority,
    ) and _has_identity_authority(
        _ACTUAL_BREAKER_REFRESH_AUTHORITIES,
        authority,
        _actual_breaker_refresh_fingerprint(authority),
    )


def is_issued_portfolio_risk_authority(authority: object) -> bool:
    return isinstance(authority, PortfolioRiskAuthority) and _has_identity_authority(
        _PORTFOLIO_RISK_AUTHORITIES,
        authority,
        _portfolio_risk_fingerprint(authority),
    )


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
) -> PortfolioRiskAuthority:
    """Trusted coordinator seam; all capacity fields are recomputed here."""
    from .ledger import LedgerPair

    if not isinstance(ledger_pair, LedgerPair):
        raise RiskBlock("INVALID_PORTFOLIO_PROJECTION")
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
    snapshot = (
        ledger_pair.canonical if ledger_name == "CANONICAL" else ledger_pair.actual
    )
    if ledger_name == "CANONICAL":
        if settlement_ledger is not None or settled_at is not None:
            raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
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
    _register_identity_authority(
        _PORTFOLIO_RISK_AUTHORITIES,
        authority,
        _portfolio_risk_fingerprint(authority),
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
        decision.portfolio_authority,
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
        not is_issued_portfolio_risk_authority(authority)
        or decision.portfolio_authority is not authority
        or decision.authority_scope != authority.scope
        or decision.as_of != authority.as_of
        or decision.request is not authority.request
        or decision.authority_digest != _portfolio_authority_digest(authority)
    ):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    if not isinstance(policy, Policy):
        raise TypeError("long plan policy must be a Policy")
    policy.validate()
    if authority.policy_digest != _policy_digest(policy):
        raise RiskBlock("PORTFOLIO_AUTHORITY_UNVERIFIED")
    expected = plan_long_diagnostic(
        authority.request,
        authority.portfolio_state,
        policy,
    )
    if (
        decision.eligible != expected.eligible
        or decision.reason_codes != expected.reason_codes
        or decision.plan != expected.plan
        or decision.target != expected.target
    ):
        raise RiskBlock("PLAN_DECISION_CONTENT_MISMATCH")
    _register_identity_authority(
        _LONG_PLAN_AUTHORITIES,
        decision,
        _long_plan_fingerprint(decision),
    )
    return decision


def is_issued_long_plan_decision(decision: object) -> bool:
    return isinstance(decision, LongPlanDecision) and _has_identity_authority(
        _LONG_PLAN_AUTHORITIES,
        decision,
        _long_plan_fingerprint(decision),
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
    return isinstance(action, ConfirmedBuyAction) and _has_identity_authority(
        _CONFIRMED_BUY_AUTHORITIES,
        action,
        _confirmed_buy_fingerprint(action),
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
    return (
        window.after_cursor,
        window.through_cursor,
        window.events,
        window.complete,
        window.source,
        window.account_check,
        window.terminal_action,
    )


def _register_identity_authority(
    registry: dict[int, tuple[ReferenceType[object], tuple[object, ...]]],
    value: object,
    fingerprint: tuple[object, ...],
) -> None:
    identity = id(value)

    def discard(dead: ReferenceType[object]) -> None:
        with _AUTHORITY_LOCK:
            current = registry.get(identity)
            if current is not None and current[0] is dead:
                registry.pop(identity, None)

    reference = ref(value, discard)
    with _AUTHORITY_LOCK:
        registry[identity] = (reference, fingerprint)


def _has_identity_authority(
    registry: dict[int, tuple[ReferenceType[object], tuple[object, ...]]],
    value: object,
    fingerprint: tuple[object, ...],
) -> bool:
    with _AUTHORITY_LOCK:
        registered = registry.get(id(value))
        return (
            registered is not None
            and registered[0]() is value
            and registered[1] == fingerprint
        )


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


def is_issued_journal_event_window(window: object) -> bool:
    """Return whether *window* is the exact unmodified adapter-issued object."""
    return isinstance(window, JournalEventWindow) and _has_identity_authority(
        _JOURNAL_WINDOW_AUTHORITIES,
        window,
        _authority_fingerprint(window),
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
        if self.kind not in {"BUY", "SALE"}:
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
            session_date = _validate_settlement_event_time(
                self.calendar_resolver,
                posting.at,
            )
            if posting.kind == "BUY":
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
        return _has_identity_authority(
            _SETTLEMENT_LEDGER_AUTHORITIES,
            self,
            _settlement_ledger_fingerprint(self),
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
            if posting.kind == "BUY" and posting.source_received_at <= at:
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
    return (
        ledger.initial_settled_cash,
        ledger.initialized_at,
        ledger.calendar_resolver,
        ledger.postings,
    )


def _settlement_ledger_content_digest(ledger: SettlementLedger) -> str:
    """Hash the full canonical settlement projection, not only posting IDs."""
    if not isinstance(ledger, SettlementLedger):
        raise RiskBlock("INVALID_SETTLEMENT_AUTHORITY")
    payload = {
        "version": 1,
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
    _register_identity_authority(
        _SETTLEMENT_LEDGER_AUTHORITIES,
        ledger,
        _settlement_ledger_fingerprint(ledger),
    )
    return ledger


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
    }
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PositionEventContext:
    """Ordered event/thesis review facts issued by a later Journal adapter."""

    event_exit_required: bool
    thesis_invalidated: bool
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
        if type(self.event_exit_required) is not bool or type(
            self.thesis_invalidated
        ) is not bool:
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
    event_exit_required: bool,
    thesis_invalidated: bool,
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
    source_payload = {
        "version": 1,
        "ledger_name": position.ledger_name,
        "signal_id": position.signal_id,
        "symbol": position.symbol,
        "position_digest": position_digest,
        "event_exit_required": event_exit_required,
        "thesis_invalidated": thesis_invalidated,
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
    return isinstance(context, PositionEventContext) and _has_identity_authority(
        _POSITION_EVENT_AUTHORITIES,
        context,
        _position_event_context_fingerprint(context),
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
    event_exit_required: bool = False
    thesis_invalidated: bool = False
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
        for value in (
            self.event_exit_required,
            self.thesis_invalidated,
            self.context_verified,
            self.holding_sessions_verified,
        ):
            if type(value) is not bool:
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
    return isinstance(mark, MarketMark) and _has_identity_authority(
        _MARK_AUTHORITIES,
        mark,
        _market_mark_fingerprint(mark),
    )


def build_market_mark(
    position: Position,
    *,
    price: Decimal,
    at: datetime,
    calendar_resolver: SessionCalendarResolver,
    event_exit_required: bool,
    thesis_invalidated: bool,
    event_context_verified: bool,
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
        _register_identity_authority(
            _MARK_AUTHORITIES,
            mark,
            _market_mark_fingerprint(mark),
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

    if r_multiple >= _TWO:
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
) -> tuple[object, ...]:
    return (
        history.ledger_name,
        history.equity,
        history.closes,
        history.start_cursor,
        history.terminal_cursor,
        history.close_start_cursor,
        history.close_terminal_cursor,
        history.through_session,
        history.calendar_resolver,
        history.source_digest,
        history.query_cutoff,
        history.equity_expected_count,
        history.close_expected_count,
        history.close_stream_through_cursor,
        history.validation_window_id,
        history.window_start_session,
        history.window_start_source_id,
    )


def is_issued_breaker_history_authority(history: object) -> bool:
    return isinstance(history, BreakerHistoryAuthority) and _has_identity_authority(
        _BREAKER_HISTORY_AUTHORITIES,
        history,
        _breaker_history_fingerprint(history),
    )


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


def is_issued_breaker_state(state: object) -> bool:
    return isinstance(state, BreakerState) and _has_identity_authority(
        _BREAKER_AUTHORITIES,
        state,
        _breaker_fingerprint(state),
    )


def _paired_breaker_fingerprint(
    state: PairedBreakerState,
) -> tuple[object, ...]:
    return (
        state.as_of,
        state.live_entries_paused,
        state.reason_codes,
        state.canonical,
        state.actual,
        state.canonical_observations_continue,
    )


def is_issued_paired_breaker_state(state: object) -> bool:
    return isinstance(state, PairedBreakerState) and _has_identity_authority(
        _PAIRED_BREAKER_AUTHORITIES,
        state,
        _paired_breaker_fingerprint(state),
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
    _register_identity_authority(
        _BREAKER_AUTHORITIES,
        state,
        _breaker_fingerprint(state),
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
        _register_identity_authority(
            _PAIRED_BREAKER_AUTHORITIES,
            paired,
            _paired_breaker_fingerprint(paired),
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
    "is_issued_position_event_context",
    "plan_long",
    "plan_long_diagnostic",
    "size_long",
]
