"""Disjoint in-memory canonical and actual ledger projections.

Events are the rebuild authority.  The frozen ledger snapshots are disposable
projections; durable Journal adapters are intentionally deferred to later tasks.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal, ROUND_CEILING, localcontext
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
)
from .phase1 import (
    IntradayObservation,
    ObservationKind,
    PaperEntryResult,
    SignalStatus,
    simulate_entry,
)
from .risk import (
    AccountCheck,
    ActualBreakerRefreshAuthority,
    BreakerState,
    ConfirmedBuyAction,
    ExecutionEvent,
    JournalEventWindow,
    LongPlanDecision,
    PairedBreakerState,
    PortfolioRiskAuthority,
    RiskBlock,
    SessionCalendarResolver,
    combine_breaker_states,
    breaker_pauses_entry,
    evaluate_account_check_window,
    is_issued_journal_event_window,
    is_issued_confirmed_buy_action,
    is_issued_actual_breaker_refresh_authority,
    is_issued_long_plan_decision,
    is_issued_paired_breaker_state,
    is_issued_portfolio_risk_authority,
)


_ZERO = Decimal("0")
_TWO = Decimal("2")
_CAPITAL = Decimal("5000")
_MAX_EXPOSURE = Decimal("1000")
_MAX_POSITION_RISK = Decimal("25")
_MAX_COMBINED_RISK = Decimal("50")
_MAX_POSITIONS = 2
_MAX_ENTRIES_PER_SESSION = 1
_MAX_SPREAD = Decimal("0.0025")
_ET = ZoneInfo("America/New_York")
_EVENT_AUTHORITY_LOCK = RLock()
_VERIFIED_LEDGER_BATCH_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_VERIFIED_REPLAY_COHORT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_ACTUAL_PROJECTION_COHORT_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
    ],
] = {}
_ISSUED_LEDGER_SIGNALS: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_EXECUTION_QUOTE_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_ACTUAL_BUY_CONTEXT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_PAPER_ENTRY_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_SHADOW_FILL_DISPOSITION_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
] = {}
_PHASE1_SOURCE_BINDINGS: dict[
    int,
    tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
] = {}


def _precision(*values: Decimal) -> int:
    finite = tuple(
        value
        for value in values
        if type(value) is Decimal and value.is_finite() and value != _ZERO
    )
    if not finite:
        return 50
    parts = tuple(value.as_tuple() for value in finite)
    exponents = tuple(int(part.exponent) for part in parts)
    digits = sum(max(1, len(part.digits)) for part in parts)
    span = max(exponents) - min(exponents)
    nonzero = tuple(value for value in finite if value != _ZERO)
    magnitude = max(abs(value.adjusted()) for value in nonzero) if nonzero else 0
    return max(50, digits + span + magnitude + 32)


def _money(
    value: object,
    code: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise RiskBlock(code)
    if positive and value <= _ZERO:
        raise RiskBlock(code)
    if nonnegative and value < _ZERO:
        raise RiskBlock(code)
    try:
        micros = money_to_micros(value)
    except DomainValidationError:
        raise RiskBlock(code) from None
    return money_from_micros(micros)


def _positive_int(value: object, code: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_MICRODOLLARS:
        raise RiskBlock(code)
    return value


def _nonnegative_int(value: object, code: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_MICRODOLLARS:
        raise RiskBlock(code)
    return value


def _freeze_reason_codes(value: object, code: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise RiskBlock(code)
    try:
        reason_codes = tuple(value)  # type: ignore[arg-type]
    except TypeError:
        raise RiskBlock(code) from None
    if any(type(reason) is not str or not reason for reason in reason_codes):
        raise RiskBlock(code)
    return reason_codes


def _aware(value: object, code: str) -> datetime:
    try:
        return require_aware_timestamp(value, "timestamp")  # type: ignore[arg-type]
    except DomainValidationError:
        raise RiskBlock(code) from None


def _tick_aligned(value: Decimal, tick: Decimal) -> bool:
    with localcontext() as context:
        context.prec = _precision(value, tick)
        units = value / tick
        return units == units.to_integral_value()


def _target(entry: Decimal, stop: Decimal, tick: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = _precision(entry, stop, tick)
        raw = entry + _TWO * (entry - stop)
        return (
            (raw / tick).to_integral_value(rounding=ROUND_CEILING) * tick
        )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class LedgerSignal:
    signal_id: str
    symbol: str
    role: str
    publication_session: date
    maximum_entry: Decimal
    recommended_stop: Decimal
    target: Decimal
    planned_shares: int
    tick_size: Decimal
    trigger_price: Decimal | None = None

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise RiskBlock("INVALID_SIGNAL_ID")
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise RiskBlock("INVALID_SIGNAL_SYMBOL")
        if self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}:
            raise RiskBlock("INVALID_SIGNAL_ROLE")
        if type(self.publication_session) is not date:
            raise RiskBlock("INVALID_SIGNAL_SESSION")
        object.__setattr__(
            self,
            "maximum_entry",
            _money(self.maximum_entry, "INVALID_SIGNAL_ENTRY", positive=True),
        )
        object.__setattr__(
            self,
            "recommended_stop",
            _money(self.recommended_stop, "INVALID_SIGNAL_STOP", positive=True),
        )
        object.__setattr__(
            self,
            "target",
            _money(self.target, "INVALID_SIGNAL_TARGET", positive=True),
        )
        object.__setattr__(
            self,
            "tick_size",
            _money(self.tick_size, "INVALID_SIGNAL_TICK", positive=True),
        )
        object.__setattr__(
            self,
            "trigger_price",
            _money(
                self.maximum_entry
                if self.trigger_price is None
                else self.trigger_price,
                "INVALID_SIGNAL_TRIGGER",
                positive=True,
            ),
        )
        _positive_int(self.planned_shares, "INVALID_SIGNAL_SHARES")
        if self.recommended_stop >= self.maximum_entry:
            raise RiskBlock("NON_POSITIVE_STOP_DISTANCE")
        for value, code in (
            (self.trigger_price, "SIGNAL_TRIGGER_NOT_TICK_ALIGNED"),
            (self.maximum_entry, "SIGNAL_ENTRY_NOT_TICK_ALIGNED"),
            (self.recommended_stop, "SIGNAL_STOP_NOT_TICK_ALIGNED"),
            (self.target, "SIGNAL_TARGET_NOT_TICK_ALIGNED"),
        ):
            if not _tick_aligned(value, self.tick_size):
                raise RiskBlock(code)
        if self.trigger_price > self.maximum_entry:
            raise RiskBlock("SIGNAL_TRIGGER_ABOVE_MAXIMUM_ENTRY")
        if self.target != _target(
            self.maximum_entry,
            self.recommended_stop,
            self.tick_size,
        ):
            raise RiskBlock("SIGNAL_TARGET_NOT_EXACT_TWO_R")

    @classmethod
    def from_scored_candidate(
        cls,
        candidate: object,
        *,
        role: str,
        planned_shares: int,
    ) -> LedgerSignal:
        from .screening import ScoredCandidate

        if not isinstance(candidate, ScoredCandidate):
            raise TypeError("ledger signal candidate must be a ScoredCandidate")
        if role == "PRIMARY":
            raise RiskBlock("PUBLICATION_AUTHORITY_UNVERIFIED")
        if (
            candidate.publication_session is None
            or candidate.maximum_permitted_entry is None
            or candidate.recommended_stop is None
            or candidate.target_price is None
            or candidate.tick_size is None
        ):
            raise RiskBlock("TASK5_PRICE_CONTRACT_INCOMPLETE")
        return cls(
            signal_id=f"{candidate.publication_session.isoformat()}:{candidate.symbol}",
            symbol=candidate.symbol,
            role=role,
            publication_session=candidate.publication_session,
            maximum_entry=candidate.maximum_permitted_entry,
            recommended_stop=candidate.recommended_stop,
            target=candidate.target_price,
            planned_shares=planned_shares,
            tick_size=candidate.tick_size,
            trigger_price=candidate.trigger_price,
        )

    @classmethod
    def from_publication_decision(
        cls,
        decision: object,
        *,
        rank: int,
        plan_decision: LongPlanDecision,
    ) -> LedgerSignal:
        """Consume sealed Task 5 role output and sealed Task 6 sizing output."""
        from .screening import (
            PublicationDecision,
            is_issued_publication_decision,
        )

        if not isinstance(decision, PublicationDecision) or not (
            is_issued_publication_decision(decision)
        ):
            raise RiskBlock("PUBLICATION_AUTHORITY_UNVERIFIED")
        _positive_int(rank, "INVALID_PUBLICATION_RANK")
        publication = next(
            (item for item in decision.candidates if item.rank == rank),
            None,
        )
        if publication is None:
            raise RiskBlock("UNKNOWN_PUBLICATION_CANDIDATE")
        if publication.role != "PRIMARY" or decision.primary != publication:
            raise RiskBlock("PUBLICATION_CANDIDATE_NOT_PRIMARY")
        if not is_issued_long_plan_decision(plan_decision) or not (
            plan_decision.eligible
        ):
            raise RiskBlock("POSITION_PLAN_AUTHORITY_UNVERIFIED")
        if (
            plan_decision.authority_scope != "CANONICAL_PUBLICATION"
            or plan_decision.as_of is None
            or plan_decision.as_of.astimezone(_ET).date()
            != publication.candidate.publication_session
            or plan_decision.as_of.astimezone(_ET).time().replace(tzinfo=None)
            != time(8, 45)
        ):
            raise RiskBlock("POSITION_PLAN_AUTHORITY_SCOPE_MISMATCH")
        request = plan_decision.request
        plan = plan_decision.plan
        candidate = publication.candidate
        if request is None or plan is None or any(
            (
                request.symbol != candidate.symbol,
                request.session_date != candidate.publication_session,
                request.entry != candidate.maximum_permitted_entry,
                request.stop != candidate.recommended_stop,
                request.tick_size != candidate.tick_size,
                request.published_target != candidate.target_price,
                plan_decision.target != candidate.target_price,
            )
        ):
            raise RiskBlock("POSITION_PLAN_CANDIDATE_MISMATCH")
        signal = cls(
            signal_id=(
                f"{candidate.publication_session.isoformat()}:{candidate.symbol}"
            ),
            symbol=candidate.symbol,
            role=publication.role,
            publication_session=candidate.publication_session,
            maximum_entry=candidate.maximum_permitted_entry,
            recommended_stop=candidate.recommended_stop,
            target=candidate.target_price,
            planned_shares=plan.quantity,
            tick_size=candidate.tick_size,
            trigger_price=candidate.trigger_price,
        )
        identity = id(signal)

        def discard(dead: ReferenceType[object]) -> None:
            with _EVENT_AUTHORITY_LOCK:
                current = _ISSUED_LEDGER_SIGNALS.get(identity)
                if current is not None and current[0] is dead:
                    _ISSUED_LEDGER_SIGNALS.pop(identity, None)

        reference = ref(signal, discard)
        with _EVENT_AUTHORITY_LOCK:
            _ISSUED_LEDGER_SIGNALS[identity] = (
                reference,
                _ledger_signal_fingerprint(signal),
            )
        return signal


def _ledger_signal_fingerprint(signal: LedgerSignal) -> tuple[object, ...]:
    return (
        signal.signal_id,
        signal.symbol,
        signal.role,
        signal.publication_session,
        signal.maximum_entry,
        signal.recommended_stop,
        signal.target,
        signal.planned_shares,
        signal.tick_size,
        signal.trigger_price,
    )


def is_issued_ledger_signal(signal: object) -> bool:
    if not isinstance(signal, LedgerSignal):
        return False
    try:
        fingerprint = _ledger_signal_fingerprint(signal)
    except Exception:
        return False
    with _EVENT_AUTHORITY_LOCK:
        issued = _ISSUED_LEDGER_SIGNALS.get(id(signal))
        registered = (
            issued is not None
            and issued[0]() is signal
            and issued[1] == fingerprint
        )
    return registered and _phase1_sources_are_current(signal)


def _ledger_signal_digest(signal: LedgerSignal) -> str:
    payload = {
        "version": 2,
        "signal_id": signal.signal_id,
        "symbol": signal.symbol,
        "role": signal.role,
        "publication_session": signal.publication_session.isoformat(),
        "maximum_entry_micros": money_to_micros(signal.maximum_entry),
        "recommended_stop_micros": money_to_micros(signal.recommended_stop),
        "target_micros": money_to_micros(signal.target),
        "planned_shares": signal.planned_shares,
        "tick_size_micros": money_to_micros(signal.tick_size),
        "trigger_price_micros": money_to_micros(signal.trigger_price),
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
class PaperEntryAuthority:
    """Task 8 handoff for one complete canonical trigger/limit lineage.

    Task 6 deliberately exposes no issuer.  A later lifecycle adapter must
    verify the stored observation cohort and register the exact value before
    :meth:`LedgerPair.record_authorized_canonical_fill` can consume it.
    """

    signal_id: str
    signal_digest: str
    lifecycle_event_id: str
    trigger_observation_id: str
    trigger_stream_id: str
    trigger_feed: str
    trigger_at: datetime
    trigger_received_at: datetime
    trigger_sequence: int | None
    trigger_source_cursor: int
    trigger_source_ordinal: int
    trigger_stream_through_cursor: int
    trigger_cohort_ordinal: int
    trigger_price: Decimal
    quote_observation_id: str
    quote_stream_id: str
    quote_feed: str
    quote_at: datetime
    quote_received_at: datetime
    quote_sequence: int | None
    quote_source_cursor: int
    quote_source_ordinal: int
    quote_stream_through_cursor: int
    quote_cohort_ordinal: int
    bid: Decimal
    ask: Decimal
    source_digest: str
    session_complete_digest: str
    cohort_through_ordinal: int
    cohort_received_through: datetime
    canonical_event_id: str
    lifecycle_cursor: int
    action_ordinal: int
    calendar_digest: str

    def __post_init__(self) -> None:
        for value in (
            self.signal_id,
            self.lifecycle_event_id,
            self.trigger_observation_id,
            self.trigger_stream_id,
            self.trigger_feed,
            self.quote_observation_id,
            self.quote_stream_id,
            self.quote_feed,
            self.canonical_event_id,
        ):
            if type(value) is not str or not value:
                raise RiskBlock("INVALID_PAPER_ENTRY_AUTHORITY")
        for digest in (
            self.signal_digest,
            self.source_digest,
            self.session_complete_digest,
            self.calendar_digest,
        ):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise RiskBlock("INVALID_PAPER_ENTRY_AUTHORITY")
        trigger_at = _aware(self.trigger_at, "INVALID_PAPER_TRIGGER_TIME")
        trigger_received_at = _aware(
            self.trigger_received_at,
            "INVALID_PAPER_TRIGGER_TIME",
        )
        quote_at = _aware(self.quote_at, "INVALID_PAPER_QUOTE_TIME")
        quote_received_at = _aware(
            self.quote_received_at,
            "INVALID_PAPER_QUOTE_TIME",
        )
        cohort_received_through = _aware(
            self.cohort_received_through,
            "INVALID_PAPER_COHORT_RECEIPT_TIME",
        )
        if trigger_at.astimezone(_ET).time().replace(tzinfo=None) <= time(9, 35):
            raise RiskBlock("PAPER_TRIGGER_NOT_AFTER_0935")
        if (
            quote_at < trigger_at
            or quote_at.astimezone(_ET).date()
            != trigger_at.astimezone(_ET).date()
            or self.quote_cohort_ordinal <= self.trigger_cohort_ordinal
        ):
            raise RiskBlock("PAPER_QUOTE_NOT_AFTER_TRIGGER")
        if (
            trigger_received_at < trigger_at
            or quote_received_at < quote_at
            or quote_received_at < trigger_received_at
        ):
            raise RiskBlock("PAPER_OBSERVATION_RECEIPT_PRECEDES_EVENT")
        if cohort_received_through < max(
            trigger_received_at,
            quote_received_at,
        ):
            raise RiskBlock("PAPER_COHORT_RECEIPT_INCOMPLETE")
        for sequence, code in (
            (self.trigger_sequence, "INVALID_PAPER_TRIGGER_SEQUENCE"),
            (self.quote_sequence, "INVALID_PAPER_QUOTE_SEQUENCE"),
        ):
            if sequence is not None and (
                type(sequence) is not int
                or sequence < 0
                or sequence > MAX_MICRODOLLARS
            ):
                raise RiskBlock(code)
        _positive_int(
            self.trigger_source_cursor,
            "INVALID_PAPER_TRIGGER_CURSOR",
        )
        _positive_int(
            self.quote_source_cursor,
            "INVALID_PAPER_QUOTE_CURSOR",
        )
        _positive_int(
            self.trigger_stream_through_cursor,
            "INVALID_PAPER_TRIGGER_CURSOR",
        )
        _positive_int(
            self.quote_stream_through_cursor,
            "INVALID_PAPER_QUOTE_CURSOR",
        )
        if (
            self.trigger_stream_through_cursor < self.trigger_source_cursor
            or self.quote_stream_through_cursor < self.quote_source_cursor
        ):
            raise RiskBlock("PAPER_STREAM_COHORT_INCOMPLETE")
        for ordinal, code in (
            (self.trigger_source_ordinal, "INVALID_PAPER_TRIGGER_ORDINAL"),
            (self.quote_source_ordinal, "INVALID_PAPER_QUOTE_ORDINAL"),
            (self.trigger_cohort_ordinal, "INVALID_PAPER_COHORT_ORDINAL"),
            (self.quote_cohort_ordinal, "INVALID_PAPER_COHORT_ORDINAL"),
            (self.cohort_through_ordinal, "INVALID_PAPER_COHORT_ORDINAL"),
        ):
            _nonnegative_int(ordinal, code)
        if self.cohort_through_ordinal < self.quote_cohort_ordinal:
            raise RiskBlock("INVALID_PAPER_COHORT_ORDINAL")
        object.__setattr__(
            self,
            "trigger_price",
            _money(
                self.trigger_price,
                "INVALID_PAPER_TRIGGER_PRICE",
                positive=True,
            ),
        )
        object.__setattr__(
            self,
            "bid",
            _money(self.bid, "INVALID_PAPER_BID", positive=True),
        )
        object.__setattr__(
            self,
            "ask",
            _money(self.ask, "INVALID_PAPER_ASK", positive=True),
        )
        if self.ask < self.bid:
            raise RiskBlock("INVALID_PAPER_SPREAD")
        _positive_int(self.lifecycle_cursor, "INVALID_PAPER_SOURCE_CURSOR")
        _nonnegative_int(
            self.action_ordinal,
            "INVALID_PAPER_ACTION_ORDINAL",
        )


def _paper_entry_fingerprint(
    authority: PaperEntryAuthority,
) -> tuple[object, ...]:
    return (
        authority.signal_id,
        authority.signal_digest,
        authority.lifecycle_event_id,
        authority.trigger_observation_id,
        authority.trigger_stream_id,
        authority.trigger_feed,
        authority.trigger_at,
        authority.trigger_received_at,
        authority.trigger_sequence,
        authority.trigger_source_cursor,
        authority.trigger_source_ordinal,
        authority.trigger_stream_through_cursor,
        authority.trigger_cohort_ordinal,
        authority.trigger_price,
        authority.quote_observation_id,
        authority.quote_stream_id,
        authority.quote_feed,
        authority.quote_at,
        authority.quote_received_at,
        authority.quote_sequence,
        authority.quote_source_cursor,
        authority.quote_source_ordinal,
        authority.quote_stream_through_cursor,
        authority.quote_cohort_ordinal,
        authority.bid,
        authority.ask,
        authority.source_digest,
        authority.session_complete_digest,
        authority.cohort_through_ordinal,
        authority.cohort_received_through,
        authority.canonical_event_id,
        authority.lifecycle_cursor,
        authority.action_ordinal,
        authority.calendar_digest,
    )


def is_issued_paper_entry_authority(authority: object) -> bool:
    """Return false until Task 8 verifies and registers source lineage."""
    if not isinstance(authority, PaperEntryAuthority):
        return False
    try:
        fingerprint = _paper_entry_fingerprint(authority)
    except Exception:
        return False
    return _has_ledger_authority(
        _PAPER_ENTRY_AUTHORITIES,
        authority,
        fingerprint,
    ) and _phase1_sources_are_current(authority)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ShadowFillDispositionAuthority:
    """Price/time-only informational fill for one watchlist shadow."""

    signal_id: str
    lifecycle_event_id: str
    trigger_observation_id: str
    quote_observation_id: str
    trigger_at: datetime
    filled_at: datetime
    fill_price: Decimal
    source_digest: str
    session_complete_digest: str
    calendar_digest: str
    lifecycle_cursor: int
    action_ordinal: int

    def __post_init__(self) -> None:
        for value in (
            self.signal_id,
            self.lifecycle_event_id,
            self.trigger_observation_id,
            self.quote_observation_id,
        ):
            if type(value) is not str or not value:
                raise RiskBlock("INVALID_SHADOW_FILL_DISPOSITION")
        trigger_at = _aware(
            self.trigger_at,
            "INVALID_SHADOW_FILL_DISPOSITION",
        )
        filled_at = _aware(
            self.filled_at,
            "INVALID_SHADOW_FILL_DISPOSITION",
        )
        if (
            trigger_at.astimezone(_ET).time().replace(tzinfo=None)
            <= time(9, 35)
            or filled_at < trigger_at
            or filled_at.astimezone(_ET).date()
            != trigger_at.astimezone(_ET).date()
        ):
            raise RiskBlock("INVALID_SHADOW_FILL_DISPOSITION")
        object.__setattr__(
            self,
            "fill_price",
            _money(
                self.fill_price,
                "INVALID_SHADOW_FILL_DISPOSITION",
                positive=True,
            ),
        )
        for digest in (
            self.source_digest,
            self.session_complete_digest,
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
                raise RiskBlock("INVALID_SHADOW_FILL_DISPOSITION")
        _positive_int(
            self.lifecycle_cursor,
            "INVALID_SHADOW_FILL_DISPOSITION",
        )
        _nonnegative_int(
            self.action_ordinal,
            "INVALID_SHADOW_FILL_DISPOSITION",
        )


def _shadow_fill_disposition_fingerprint(
    authority: ShadowFillDispositionAuthority,
) -> tuple[object, ...]:
    return (
        authority.signal_id,
        authority.lifecycle_event_id,
        authority.trigger_observation_id,
        authority.quote_observation_id,
        authority.trigger_at,
        authority.filled_at,
        authority.fill_price,
        authority.source_digest,
        authority.session_complete_digest,
        authority.calendar_digest,
        authority.lifecycle_cursor,
        authority.action_ordinal,
    )


def is_issued_shadow_fill_disposition_authority(authority: object) -> bool:
    if not isinstance(authority, ShadowFillDispositionAuthority):
        return False
    try:
        fingerprint = _shadow_fill_disposition_fingerprint(authority)
    except Exception:
        return False
    return _has_ledger_authority(
        _SHADOW_FILL_DISPOSITION_AUTHORITIES,
        authority,
        fingerprint,
    ) and _phase1_sources_are_current(authority)


@dataclass(frozen=True, slots=True)
class LedgerLot:
    price: Decimal
    shares: int
    at: datetime
    parent_order_id: str | None = None
    total_cost_micros: int = field(init=False)

    def __post_init__(self) -> None:
        price = _money(self.price, "INVALID_LEDGER_PRICE", positive=True)
        shares = _positive_int(self.shares, "INVALID_LEDGER_SHARES")
        _aware(self.at, "INVALID_LEDGER_TIME")
        if self.parent_order_id is not None and (
            type(self.parent_order_id) is not str or not self.parent_order_id
        ):
            raise RiskBlock("INVALID_PARENT_ORDER_ID")
        total_cost_micros = money_to_micros(price) * shares
        if total_cost_micros > MAX_MICRODOLLARS:
            raise RiskBlock("INVALID_LEDGER_LOT_COST")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "total_cost_micros", total_cost_micros)


@dataclass(frozen=True, slots=True)
class LedgerPosition:
    signal_id: str
    symbol: str
    ledger_name: str
    recommended_stop: Decimal
    user_confirmed_stop: Decimal | None
    target: Decimal
    tick_size: Decimal
    lots: tuple[LedgerLot, ...]
    reconciled: bool
    reason_codes: tuple[str, ...] = ()
    profit_target_taken: bool = False

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise RiskBlock("INVALID_POSITION_SIGNAL")
        if type(self.symbol) is not str or not self.symbol:
            raise RiskBlock("INVALID_POSITION_SYMBOL")
        if self.ledger_name not in {"ACTUAL", "CANONICAL"}:
            raise RiskBlock("INVALID_LEDGER_NAME")
        recommended_stop = _money(
            self.recommended_stop,
            "INVALID_RECOMMENDED_STOP",
            positive=True,
        )
        object.__setattr__(self, "recommended_stop", recommended_stop)
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _money(
                self.user_confirmed_stop,
                "INVALID_USER_CONFIRMED_STOP",
                positive=True,
                ),
            )
        object.__setattr__(
            self,
            "target",
            _money(self.target, "INVALID_POSITION_TARGET", positive=True),
        )
        object.__setattr__(
            self,
            "tick_size",
            _money(self.tick_size, "INVALID_TICK_SIZE", positive=True),
        )
        lots = tuple(self.lots)
        if not lots or any(not isinstance(lot, LedgerLot) for lot in lots):
            raise RiskBlock("INVALID_POSITION_LOTS")
        if tuple(lot.at for lot in lots) != tuple(sorted(lot.at for lot in lots)):
            raise RiskBlock("POSITION_LOTS_OUT_OF_ORDER")
        _positive_int(
            sum(lot.shares for lot in lots),
            "INVALID_POSITION_SHARES",
        )
        if sum(lot.total_cost_micros for lot in lots) > MAX_MICRODOLLARS:
            raise RiskBlock("INVALID_POSITION_EXPOSURE")
        object.__setattr__(self, "lots", lots)
        if type(self.reconciled) is not bool:
            raise RiskBlock("INVALID_RECONCILIATION_STATE")
        if type(self.profit_target_taken) is not bool:
            raise RiskBlock("INVALID_PROFIT_TARGET_STATE")
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_POSITION_REASONS"),
        )
        _money(
            self.planned_risk,
            "INVALID_POSITION_RISK",
            nonnegative=True,
        )

    @property
    def shares(self) -> int:
        return sum(lot.shares for lot in self.lots)

    @property
    def exposure(self) -> Decimal:
        return money_from_micros(self.cost_basis_micros)

    @property
    def cost_basis_micros(self) -> int:
        value = sum(lot.total_cost_micros for lot in self.lots)
        if value > MAX_MICRODOLLARS:
            raise RiskBlock("INVALID_POSITION_EXPOSURE")
        return value

    @property
    def entry(self) -> Decimal:
        exposure = self.exposure
        shares = self.shares
        with localcontext() as context:
            context.prec = _precision(exposure, Decimal(shares))
            return exposure / shares

    @property
    def planned_risk(self) -> Decimal:
        if self.ledger_name == "CANONICAL":
            effective_stop = self.recommended_stop
        elif (
            self.user_confirmed_stop is None
            or not _tick_aligned(self.user_confirmed_stop, self.tick_size)
            or any(
                self.user_confirmed_stop >= lot.price for lot in self.lots
            )
        ):
            effective_stop = _ZERO
        else:
            effective_stop = min(
                self.recommended_stop,
                self.user_confirmed_stop,
            )
        effective_stop_micros = money_to_micros(effective_stop)
        risk_micros = 0
        for lot in self.lots:
            distance_micros = max(
                0,
                money_to_micros(lot.price) - effective_stop_micros,
            )
            risk_micros += distance_micros * lot.shares
            if risk_micros > MAX_MICRODOLLARS:
                raise RiskBlock("INVALID_POSITION_RISK")
        return money_from_micros(risk_micros)


@dataclass(frozen=True, slots=True)
class ComplianceDecision:
    status: str
    compliant: bool
    reconciliation_required: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(
                self.reason_codes,
                "INVALID_COMPLIANCE_DECISION",
            ),
        )
        if self.status not in {
            "COMPLIANT",
            "NONCOMPLIANT_RECONCILIATION_REQUIRED",
        }:
            raise RiskBlock("INVALID_COMPLIANCE_DECISION")
        if type(self.compliant) is not bool or type(
            self.reconciliation_required
        ) is not bool:
            raise RiskBlock("INVALID_COMPLIANCE_DECISION")
        if self.compliant == self.reconciliation_required:
            raise RiskBlock("INVALID_COMPLIANCE_DECISION")
        if self.compliant == bool(self.reason_codes):
            raise RiskBlock("INVALID_COMPLIANCE_DECISION")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class LedgerEvent:
    ledger_name: str
    signal_id: str
    lot: LedgerLot
    user_confirmed_stop: Decimal | None
    decision: ComplianceDecision
    event_id: str
    cursor: int | None = None
    ordinal: int = 0
    authority_basis: str | None = None
    signal_digest: str | None = None
    message_time: datetime | None = None
    received_at: datetime | None = None
    recommended_stop: Decimal | None = None
    profit_target_taken: bool = False

    def __post_init__(self) -> None:
        if self.ledger_name not in {"ACTUAL", "CANONICAL"}:
            raise RiskBlock("INVALID_LEDGER_NAME")
        if type(self.signal_id) is not str or not self.signal_id:
            raise RiskBlock("INVALID_SIGNAL_ID")
        if not isinstance(self.lot, LedgerLot):
            raise RiskBlock("INVALID_LEDGER_LOT")
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _money(
                    self.user_confirmed_stop,
                    "INVALID_USER_CONFIRMED_STOP",
                    positive=True,
                ),
            )
        if self.recommended_stop is not None:
            object.__setattr__(
                self,
                "recommended_stop",
                _money(
                    self.recommended_stop,
                    "INVALID_RECOMMENDED_STOP",
                    positive=True,
                ),
            )
        if type(self.profit_target_taken) is not bool:
            raise RiskBlock("INVALID_PROFIT_TARGET_STATE")
        if not isinstance(self.decision, ComplianceDecision):
            raise RiskBlock("INVALID_COMPLIANCE_DECISION")
        if type(self.event_id) is not str or not self.event_id:
            raise RiskBlock("INVALID_LEDGER_EVENT_ID")
        if self.cursor is not None:
            _positive_int(self.cursor, "INVALID_LEDGER_EVENT_CURSOR")
        _nonnegative_int(self.ordinal, "INVALID_LEDGER_EVENT_ORDINAL")
        if self.authority_basis is not None and (
            type(self.authority_basis) is not str
            or len(self.authority_basis) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.authority_basis
            )
        ):
            raise RiskBlock("INVALID_LEDGER_EVENT_AUTHORITY_BASIS")
        if self.signal_digest is not None and (
            type(self.signal_digest) is not str
            or len(self.signal_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.signal_digest
            )
        ):
            raise RiskBlock("INVALID_LEDGER_EVENT_SIGNAL_DIGEST")
        if (self.message_time is None) != (self.received_at is None):
            raise RiskBlock("LEDGER_EVENT_SOURCE_TIME_INCOMPLETE")
        if self.message_time is not None and self.received_at is not None:
            message_time = _aware(
                self.message_time,
                "INVALID_LEDGER_EVENT_MESSAGE_TIME",
            )
            received_at = _aware(
                self.received_at,
                "INVALID_LEDGER_EVENT_RECEIVED_TIME",
            )
            if not self.lot.at <= message_time <= received_at:
                raise RiskBlock("LEDGER_EVENT_SOURCE_TIME_OUT_OF_ORDER")

    @property
    def source_received_at(self) -> datetime:
        """Return source knowledge time; diagnostic events fall back to fill time."""
        return self.received_at if self.received_at is not None else self.lot.at


@dataclass(frozen=True, slots=True, weakref_slot=True)
class VerifiedLedgerEventBatch:
    """Journal-adapter re-verification of persisted event content hashes."""

    references: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if isinstance(self.references, (str, bytes)):
            raise RiskBlock("INVALID_VERIFIED_LEDGER_EVENT_BATCH")
        references = tuple(tuple(reference) for reference in self.references)
        if any(
            len(reference) != 2
            or type(reference[0]) is not str
            or not reference[0]
            or type(reference[1]) is not str
            or len(reference[1]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in reference[1]
            )
            for reference in references
        ):
            raise RiskBlock("INVALID_VERIFIED_LEDGER_EVENT_BATCH")
        if len(references) != len({reference[0] for reference in references}):
            raise RiskBlock("DUPLICATE_VERIFIED_LEDGER_EVENT")
        object.__setattr__(self, "references", references)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class VerifiedLedgerReplayCohort:
    """Complete per-ledger Journal query authority issued by Task 7/8.

    Task 6 deliberately exposes no field-only issuer.  A future typed adapter
    must bind the exact query watermark and register the resulting object only
    after checking the persisted rows and signal lineage.
    """

    ledger_name: str
    references: tuple[tuple[str, str, str], ...]
    expected_count: int
    start_cursor: int | None
    terminal_cursor: int | None
    query_cutoff: datetime
    source_digest: str

    def __post_init__(self) -> None:
        if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
            raise RiskBlock("INVALID_REPLAY_COHORT")
        references = tuple(tuple(reference) for reference in self.references)
        if any(
            len(reference) != 3
            or type(reference[0]) is not str
            or not reference[0]
            or any(
                type(digest) is not str
                or len(digest) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in digest
                )
                for digest in reference[1:]
            )
            for reference in references
        ):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        if len(references) != len({reference[0] for reference in references}):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        expected_count = _nonnegative_int(
            self.expected_count,
            "INVALID_REPLAY_COHORT",
        )
        if expected_count != len(references):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        if (self.start_cursor is None) != (self.terminal_cursor is None):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        if self.start_cursor is not None and self.terminal_cursor is not None:
            _positive_int(self.start_cursor, "INVALID_REPLAY_COHORT")
            _positive_int(self.terminal_cursor, "INVALID_REPLAY_COHORT")
            if self.start_cursor > self.terminal_cursor:
                raise RiskBlock("INVALID_REPLAY_COHORT")
        elif expected_count:
            raise RiskBlock("INVALID_REPLAY_COHORT")
        _aware(self.query_cutoff, "INVALID_REPLAY_COHORT")
        if (
            type(self.source_digest) is not str
            or len(self.source_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.source_digest
            )
        ):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        object.__setattr__(self, "references", references)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualProjectionCohort:
    """Open actual positions bound to an exact Task 7 Journal replay."""

    positions: tuple[object, ...]
    projection_start_cursor: int | None
    projection_terminal_cursor: int | None
    source_through_cursor: int | None
    physical_source_highwater_cursor: int | None
    query_cutoff: datetime
    journal_source_digest: str
    actual_state_digest: str
    calendar_digest: str
    policy_digest: str
    expected_action_count: int
    expected_posting_count: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "positions", tuple(self.positions))
        if (self.projection_start_cursor is None) != (
            self.projection_terminal_cursor is None
        ):
            raise RiskBlock("INVALID_ACTUAL_PROJECTION_COHORT")
        for cursor in (
            self.projection_start_cursor,
            self.projection_terminal_cursor,
            self.source_through_cursor,
            self.physical_source_highwater_cursor,
        ):
            if cursor is not None:
                _positive_int(cursor, "INVALID_ACTUAL_PROJECTION_COHORT")
        if (
            self.projection_start_cursor is not None
            and self.projection_terminal_cursor is not None
            and self.projection_start_cursor > self.projection_terminal_cursor
        ):
            raise RiskBlock("INVALID_ACTUAL_PROJECTION_COHORT")
        if (
            self.projection_terminal_cursor is not None
            and self.source_through_cursor is not None
            and self.projection_terminal_cursor > self.source_through_cursor
        ):
            raise RiskBlock("INVALID_ACTUAL_PROJECTION_COHORT")
        if (
            self.source_through_cursor is not None
            and self.physical_source_highwater_cursor is not None
            and self.source_through_cursor
            > self.physical_source_highwater_cursor
        ):
            raise RiskBlock("INVALID_ACTUAL_PROJECTION_COHORT")
        _aware(self.query_cutoff, "INVALID_ACTUAL_PROJECTION_COHORT")
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
                raise RiskBlock("INVALID_ACTUAL_PROJECTION_COHORT")
        _nonnegative_int(
            self.expected_action_count,
            "INVALID_ACTUAL_PROJECTION_COHORT",
        )
        _nonnegative_int(
            self.expected_posting_count,
            "INVALID_ACTUAL_PROJECTION_COHORT",
        )


@dataclass(frozen=True, slots=True)
class CanonicalLedger:
    open_positions: tuple[LedgerPosition, ...] = ()
    events_applied: int = 0
    breaker_state: BreakerState | None = None

    def __post_init__(self) -> None:
        positions = tuple(self.open_positions)
        if any(not isinstance(item, LedgerPosition) for item in positions):
            raise RiskBlock("INVALID_CANONICAL_LEDGER")
        object.__setattr__(self, "open_positions", positions)
        _nonnegative_int(self.events_applied, "INVALID_CANONICAL_LEDGER")
        if self.breaker_state is not None and not isinstance(
            self.breaker_state,
            BreakerState,
        ):
            raise RiskBlock("INVALID_BREAKER_STATE")
        _money(
            self.deployed_capital,
            "INVALID_DEPLOYED_CAPITAL",
            nonnegative=True,
        )
        _money(
            self.open_planned_risk,
            "INVALID_OPEN_RISK",
            nonnegative=True,
        )
        _money(self.cash, "INVALID_LEDGER_CASH")

    @property
    def deployed_capital(self) -> Decimal:
        return _sum_money(
            (position.exposure for position in self.open_positions),
            "INVALID_DEPLOYED_CAPITAL",
        )

    @property
    def open_planned_risk(self) -> Decimal:
        return _sum_money(
            (position.planned_risk for position in self.open_positions),
            "INVALID_OPEN_RISK",
        )

    @property
    def cash(self) -> Decimal:
        deployed = self.deployed_capital
        with localcontext() as context:
            context.prec = _precision(_CAPITAL, deployed)
            cash = _CAPITAL - deployed
        return _money(cash, "INVALID_LEDGER_CASH")


@dataclass(frozen=True, slots=True)
class ActualLedger:
    open_positions: tuple[LedgerPosition, ...] = ()
    reconciliation_required: bool = False
    reason_codes: tuple[str, ...] = ()
    stop_unverified: bool = False
    events_applied: int = 0
    breaker_state: BreakerState | None = None

    def __post_init__(self) -> None:
        positions = tuple(self.open_positions)
        if any(not isinstance(item, LedgerPosition) for item in positions):
            raise RiskBlock("INVALID_ACTUAL_LEDGER")
        object.__setattr__(self, "open_positions", positions)
        if type(self.reconciliation_required) is not bool or type(
            self.stop_unverified
        ) is not bool:
            raise RiskBlock("INVALID_ACTUAL_LEDGER")
        object.__setattr__(
            self,
            "reason_codes",
            _freeze_reason_codes(self.reason_codes, "INVALID_ACTUAL_LEDGER"),
        )
        if self.reconciliation_required != bool(self.reason_codes):
            raise RiskBlock("INVALID_ACTUAL_LEDGER")
        _nonnegative_int(self.events_applied, "INVALID_ACTUAL_LEDGER")
        if self.breaker_state is not None and not isinstance(
            self.breaker_state,
            BreakerState,
        ):
            raise RiskBlock("INVALID_BREAKER_STATE")
        _money(
            self.deployed_capital,
            "INVALID_DEPLOYED_CAPITAL",
            nonnegative=True,
        )
        _money(
            self.open_planned_risk,
            "INVALID_OPEN_RISK",
            nonnegative=True,
        )
        _money(self.cash, "INVALID_LEDGER_CASH")

    @property
    def deployed_capital(self) -> Decimal:
        return _sum_money(
            (position.exposure for position in self.open_positions),
            "INVALID_DEPLOYED_CAPITAL",
        )

    @property
    def open_planned_risk(self) -> Decimal:
        return _sum_money(
            (position.planned_risk for position in self.open_positions),
            "INVALID_OPEN_RISK",
        )

    @property
    def cash(self) -> Decimal:
        deployed = self.deployed_capital
        with localcontext() as context:
            context.prec = _precision(_CAPITAL, deployed)
            cash = _CAPITAL - deployed
        return _money(cash, "INVALID_LEDGER_CASH")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExecutionQuoteEvidence:
    symbol: str
    bid: Decimal
    ask: Decimal
    observed_at: datetime
    confirmed_at: datetime
    cursor: int
    source: str

    def __post_init__(self) -> None:
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise RiskBlock("INVALID_EXECUTION_QUOTE_SYMBOL")
        object.__setattr__(
            self,
            "bid",
            _money(self.bid, "INVALID_CONFIRMED_BID", positive=True),
        )
        object.__setattr__(
            self,
            "ask",
            _money(self.ask, "INVALID_CONFIRMED_ASK", positive=True),
        )
        if self.ask < self.bid:
            raise RiskBlock("INVALID_CONFIRMED_SPREAD")
        _aware(self.observed_at, "INVALID_EXECUTION_QUOTE_TIME")
        _aware(self.confirmed_at, "INVALID_EXECUTION_QUOTE_TIME")
        if self.observed_at > self.confirmed_at:
            raise RiskBlock("INVALID_EXECUTION_QUOTE_TIME")
        _positive_int(self.cursor, "INVALID_EXECUTION_QUOTE_CURSOR")
        if type(self.source) is not str or not self.source:
            raise RiskBlock("INVALID_EXECUTION_QUOTE_SOURCE")


def _execution_quote_fingerprint(
    evidence: ExecutionQuoteEvidence,
) -> tuple[object, ...]:
    return (
        evidence.symbol,
        evidence.bid,
        evidence.ask,
        evidence.observed_at,
        evidence.confirmed_at,
        evidence.cursor,
        evidence.source,
    )


def _has_ledger_authority(
    registry: dict[int, tuple[ReferenceType[object], tuple[object, ...]]],
    value: object,
    fingerprint: tuple[object, ...],
) -> bool:
    with _EVENT_AUTHORITY_LOCK:
        issued = registry.get(id(value))
        return (
            issued is not None
            and issued[0]() is value
            and issued[1] == fingerprint
        )


def _register_phase1_derived_authority(
    registry: dict[int, tuple[ReferenceType[object], tuple[object, ...]]],
    value: object,
    fingerprint: tuple[object, ...],
) -> None:
    identity = id(value)

    def discard(dead: ReferenceType[object]) -> None:
        with _EVENT_AUTHORITY_LOCK:
            current = registry.get(identity)
            if current is not None and current[0] is dead:
                registry.pop(identity, None)

    reference = ref(value, discard)
    with _EVENT_AUTHORITY_LOCK:
        registry[identity] = (reference, fingerprint)


def _bind_phase1_sources(
    value: object,
    sources: Sequence[tuple[object, str]],
) -> None:
    """Bind an issued value to exact live Journal-owned source identities."""
    frozen_sources = tuple(sources)
    if not frozen_sources:
        return
    identity = id(value)

    def discard(dead: ReferenceType[object]) -> None:
        with _EVENT_AUTHORITY_LOCK:
            current = _PHASE1_SOURCE_BINDINGS.get(identity)
            if current is not None and current[0] is dead:
                _PHASE1_SOURCE_BINDINGS.pop(identity, None)

    value_reference = ref(value, discard)
    with _EVENT_AUTHORITY_LOCK:
        _PHASE1_SOURCE_BINDINGS[identity] = (
            value_reference,
            frozen_sources,
        )


def _phase1_bound_sources(value: object) -> tuple[tuple[object, str], ...]:
    with _EVENT_AUTHORITY_LOCK:
        binding = _PHASE1_SOURCE_BINDINGS.get(id(value))
        if binding is None or binding[0]() is not value:
            return ()
        return binding[1]


def _phase1_sources_are_current(value: object) -> bool:
    with _EVENT_AUTHORITY_LOCK:
        binding = _PHASE1_SOURCE_BINDINGS.get(id(value))
        if binding is None:
            return True
        if binding[0]() is not value:
            return False
        resolved = binding[1]
    from . import journal as journal_module

    verifier_names = {
        "SIGNAL": "is_verified_phase1_signal_source",
        "ENTRY": "is_verified_phase1_entry_source",
        "SHADOW_FILL": "is_verified_phase1_shadow_fill_source",
        "CANONICAL_REPLAY": "is_verified_phase1_canonical_replay_source",
    }
    for source, kind in resolved:
        verifier = getattr(journal_module, verifier_names.get(kind, ""), None)
        if verifier is None or not verifier(source):
            return False
    return True


def _issue_execution_quote_evidence(
    *,
    symbol: str,
    bid: Decimal,
    ask: Decimal,
    observed_at: datetime,
    confirmed_at: datetime,
    cursor: int,
    source: str,
) -> ExecutionQuoteEvidence:
    """Validate diagnostic quote fields without authenticating their source."""
    evidence = ExecutionQuoteEvidence(
        symbol,
        bid,
        ask,
        observed_at,
        confirmed_at,
        cursor,
        source,
    )
    return evidence


def is_issued_execution_quote_evidence(evidence: object) -> bool:
    return isinstance(evidence, ExecutionQuoteEvidence) and _has_ledger_authority(
        _EXECUTION_QUOTE_AUTHORITIES,
        evidence,
        _execution_quote_fingerprint(evidence),
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualBuyContext:
    """Explicit authority required before an actual buy can be compliant."""

    account_check: AccountCheck
    event_window: JournalEventWindow
    bid: Decimal
    ask: Decimal
    user_confirmed_stop: Decimal | None
    breaker_state: PairedBreakerState
    calendar_resolver: SessionCalendarResolver
    portfolio_authority: PortfolioRiskAuthority | None = None
    quote_evidence: ExecutionQuoteEvidence | None = None
    authorized_signal_id: str | None = None
    authorized_symbol: str | None = None
    buy_event: ExecutionEvent | None = None
    event_id: str | None = None
    buy_action: ConfirmedBuyAction | None = None
    plan_decision: LongPlanDecision | None = None
    actual_breaker_refresh: ActualBreakerRefreshAuthority | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.account_check, AccountCheck):
            raise RiskBlock("INVALID_ACCOUNT_CHECK")
        if not isinstance(self.event_window, JournalEventWindow):
            raise RiskBlock("INVALID_EVENT_WINDOW")
        object.__setattr__(
            self,
            "bid",
            _money(self.bid, "INVALID_CONFIRMED_BID", positive=True),
        )
        object.__setattr__(
            self,
            "ask",
            _money(self.ask, "INVALID_CONFIRMED_ASK", positive=True),
        )
        if self.ask < self.bid:
            raise RiskBlock("INVALID_CONFIRMED_SPREAD")
        if self.user_confirmed_stop is not None:
            object.__setattr__(
                self,
                "user_confirmed_stop",
                _money(
                    self.user_confirmed_stop,
                    "INVALID_USER_CONFIRMED_STOP",
                    positive=True,
                ),
            )
        if not isinstance(self.breaker_state, PairedBreakerState):
            raise RiskBlock("INVALID_BREAKER_STATE")
        if not isinstance(self.calendar_resolver, SessionCalendarResolver):
            raise RiskBlock("INVALID_CALENDAR_RESOLVER")
        if self.portfolio_authority is not None and not isinstance(
            self.portfolio_authority,
            PortfolioRiskAuthority,
        ):
            raise RiskBlock("INVALID_PORTFOLIO_AUTHORITY")
        if self.quote_evidence is not None and not isinstance(
            self.quote_evidence,
            ExecutionQuoteEvidence,
        ):
            raise RiskBlock("INVALID_EXECUTION_QUOTE_EVIDENCE")
        for value in (self.authorized_signal_id, self.authorized_symbol, self.event_id):
            if value is not None and (type(value) is not str or not value):
                raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
        if self.buy_event is not None and not isinstance(
            self.buy_event,
            ExecutionEvent,
        ):
            raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
        if self.buy_action is not None and not isinstance(
            self.buy_action,
            ConfirmedBuyAction,
        ):
            raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
        if self.actual_breaker_refresh is not None and not isinstance(
            self.actual_breaker_refresh,
            ActualBreakerRefreshAuthority,
        ):
            raise RiskBlock("INVALID_BREAKER_REFRESH")
        if self.plan_decision is not None:
            if not isinstance(self.plan_decision, LongPlanDecision):
                raise RiskBlock("INVALID_POSITION_PLAN_AUTHORITY")
            plan = self.plan_decision
            request = plan.request
            position_plan = plan.plan
            if (
                not is_issued_long_plan_decision(plan)
                or not plan.eligible
                or self.portfolio_authority is None
                or plan.portfolio_authority is not self.portfolio_authority
                or request is not self.portfolio_authority.request
                or plan.authority_scope != "ACTUAL_ENTRY"
                or plan.as_of != self.portfolio_authority.as_of
                or request is None
                or position_plan is None
                or self.buy_action is None
                or request.symbol != self.buy_action.symbol
                or request.session_date
                != self.buy_action.at.astimezone(_ET).date()
                or not (
                    (
                        self.buy_action.parent_order_id is not None
                        and request.entry >= self.buy_action.price
                    )
                    or (
                        self.buy_action.parent_order_id is None
                        and request.entry == self.buy_action.price
                    )
                )
                or position_plan.quantity
                != (
                    self.buy_action.fill_group_planned_shares
                    if self.buy_action.fill_group_planned_shares is not None
                    else self.buy_action.shares
                )
            ):
                raise RiskBlock("POSITION_PLAN_AUTHORITY_UNVERIFIED")
        if self.portfolio_authority is not None and self.buy_action is not None:
            if (
                self.portfolio_authority.scope != "ACTUAL_ENTRY"
                or self.portfolio_authority.breaker_refresh_through_execution_cursor
                is None
                or self.portfolio_authority.breaker_refresh_through_execution_cursor
                > self.buy_action.cursor
            ):
                raise RiskBlock("ACTUAL_BREAKER_REFRESH_UNVERIFIED")
        if self.actual_breaker_refresh is not None:
            refresh = self.actual_breaker_refresh
            if (
                self.buy_action is None
                or self.portfolio_authority is None
                or not is_issued_actual_breaker_refresh_authority(refresh)
                or refresh.paired_breaker is not self.breaker_state
                or refresh.as_of != self.buy_action.at
                or refresh.through_execution_cursor != self.buy_action.cursor
                or refresh.calendar_digest
                != self.portfolio_authority.calendar_digest
            ):
                raise RiskBlock("ACTUAL_BREAKER_REFRESH_UNVERIFIED")


def _actual_buy_context_fingerprint(
    context: ActualBuyContext,
) -> tuple[object, ...]:
    return (
        context.account_check,
        context.event_window,
        context.bid,
        context.ask,
        context.user_confirmed_stop,
        context.breaker_state,
        context.calendar_resolver,
        context.portfolio_authority,
        context.quote_evidence,
        context.authorized_signal_id,
        context.authorized_symbol,
        context.buy_event,
        context.event_id,
        context.buy_action,
        context.plan_decision,
        context.actual_breaker_refresh,
    )


def is_issued_actual_buy_context(context: object) -> bool:
    return isinstance(context, ActualBuyContext) and _has_ledger_authority(
        _ACTUAL_BUY_CONTEXT_AUTHORITIES,
        context,
        _actual_buy_context_fingerprint(context),
    )


def _issue_actual_buy_context(
    *,
    signal_id: str,
    symbol: str,
    event_id: str,
    buy: ExecutionEvent,
    account_check: AccountCheck,
    event_window: JournalEventWindow,
    quote_evidence: ExecutionQuoteEvidence,
    user_confirmed_stop: Decimal | None,
    breaker_state: PairedBreakerState,
    calendar_resolver: SessionCalendarResolver,
) -> ActualBuyContext:
    """Deprecated diagnostic compatibility seam.

    Separate quote and execution objects cannot prove that BID/ASK/STOP came
    from the same persisted confirmation action, so this path deliberately
    returns an unissued context.
    """
    if type(signal_id) is not str or not signal_id or type(event_id) is not str or not event_id:
        raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
    if type(symbol) is not str or not symbol or symbol != symbol.upper():
        raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
    if not isinstance(buy, ExecutionEvent) or not isinstance(
        account_check,
        AccountCheck,
    ):
        raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
    if buy.price is None or buy.shares is None:
        raise RiskBlock("INCOMPLETE_BUY_EVENT")
    if (
        not is_issued_journal_event_window(event_window)
        or event_window.account_check != account_check
        or event_window.terminal_event != buy
        or not is_issued_execution_quote_evidence(quote_evidence)
        or not is_issued_paired_breaker_state(breaker_state)
        or quote_evidence.symbol != symbol
        or quote_evidence.bid > buy.price
        or quote_evidence.ask < buy.price
        or quote_evidence.observed_at.astimezone(_ET).date()
        != buy.at.astimezone(_ET).date()
        or not quote_evidence.observed_at <= quote_evidence.confirmed_at <= buy.at
        or quote_evidence.cursor <= event_window.after_cursor
        or quote_evidence.cursor >= event_window.through_cursor
        or (buy.at - quote_evidence.observed_at).total_seconds() > 300
    ):
        raise RiskBlock("ACTUAL_BUY_EVIDENCE_MISMATCH")
    context = ActualBuyContext(
        account_check=account_check,
        event_window=event_window,
        bid=quote_evidence.bid,
        ask=quote_evidence.ask,
        user_confirmed_stop=user_confirmed_stop,
        breaker_state=breaker_state,
        calendar_resolver=calendar_resolver,
        quote_evidence=quote_evidence,
        authorized_signal_id=signal_id,
        authorized_symbol=symbol,
        buy_event=buy,
        event_id=event_id,
    )
    return context


def _issue_actual_buy_context_from_action(
    *,
    signal_id: str,
    action: ConfirmedBuyAction,
    account_check: AccountCheck,
    event_window: JournalEventWindow,
    breaker_state: PairedBreakerState,
    calendar_resolver: SessionCalendarResolver,
) -> ActualBuyContext:
    """Assemble a diagnostic exact-action context without source authority."""
    if type(signal_id) is not str or not signal_id:
        raise RiskBlock("INVALID_ACTUAL_BUY_AUTHORITY")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    from .risk import _calendar_digest

    calendar_digest = _calendar_digest(calendar_resolver)
    if (
        not isinstance(action, ConfirmedBuyAction)
        or not isinstance(event_window, JournalEventWindow)
        or event_window.account_check != account_check
        or event_window.terminal_action is not action
        or not isinstance(breaker_state, PairedBreakerState)
        or breaker_state.canonical.calendar_digest != calendar_digest
        or breaker_state.actual.calendar_digest != calendar_digest
    ):
        raise RiskBlock("ACTUAL_BUY_EVIDENCE_MISMATCH")
    context = ActualBuyContext(
        account_check=account_check,
        event_window=event_window,
        bid=action.bid,
        ask=action.ask,
        user_confirmed_stop=action.user_confirmed_stop,
        breaker_state=breaker_state,
        calendar_resolver=calendar_resolver,
        authorized_signal_id=signal_id,
        authorized_symbol=action.symbol,
        buy_event=action.execution_event,
        event_id=action.event_id,
        buy_action=action,
    )
    return context


def _sum_money(
    values: Sequence[Decimal] | object,
    reason_code: str,
) -> Decimal:
    materialized = tuple(values)  # type: ignore[arg-type]
    try:
        total_micros = sum(money_to_micros(value) for value in materialized)
        return money_from_micros(total_micros)
    except DomainValidationError:
        raise RiskBlock(reason_code) from None


def _signal_by_id(
    signals: Sequence[LedgerSignal],
    signal_id: str,
) -> LedgerSignal:
    for signal in signals:
        if signal.signal_id == signal_id:
            return signal
    raise RiskBlock("UNKNOWN_SIGNAL")


def _copy_lot(lot: LedgerLot) -> LedgerLot:
    return LedgerLot(
        price=lot.price,
        shares=lot.shares,
        at=lot.at,
        parent_order_id=lot.parent_order_id,
    )


def _sort_projection_lots(
    positions: Sequence[LedgerPosition],
) -> tuple[LedgerPosition, ...]:
    return tuple(
        replace(
            position,
            lots=tuple(
                sorted(
                    position.lots,
                    key=lambda lot: lot.at.astimezone(UTC),
                )
            ),
        )
        for position in positions
    )


def _ledger_event_id(
    ledger_name: str,
    signal_id: str,
    at: datetime,
    cursor: int | None,
) -> str:
    if cursor is not None:
        return f"{ledger_name.lower()}:journal:{cursor}"
    content = "|".join(
        (ledger_name, signal_id, at.astimezone(UTC).isoformat())
    )
    return f"{ledger_name.lower()}:{sha256(content.encode('utf-8')).hexdigest()}"


def _ledger_event_order_key(
    event: LedgerEvent,
) -> tuple[datetime, int, int, str]:
    return (
        event.lot.at.astimezone(UTC),
        event.cursor if event.cursor is not None else 0,
        event.ordinal,
        event.event_id,
    )


def _normalize_ledger_events(
    events: Sequence[LedgerEvent],
) -> tuple[LedgerEvent, ...]:
    normalized: list[LedgerEvent] = []
    by_id: dict[str, LedgerEvent] = {}
    previous_source_key: dict[str, tuple[int, int]] = {}
    previous_source_message_time: dict[str, datetime] = {}
    previous_source_received_at: dict[str, datetime] = {}
    source_coordinates: set[tuple[str, int, int]] = set()
    for event in tuple(events):
        if not isinstance(event, LedgerEvent):
            raise RiskBlock("INVALID_LEDGER_EVENTS")
        prior = by_id.get(event.event_id)
        if prior is not None:
            if prior != event:
                raise RiskBlock("LEDGER_EVENT_IDEMPOTENCY_CONFLICT")
            continue
        if event.cursor is not None:
            coordinate = (event.ledger_name, event.cursor, event.ordinal)
            if coordinate in source_coordinates:
                raise RiskBlock("LEDGER_SOURCE_COORDINATE_CONFLICT")
            source_key = (event.cursor, event.ordinal)
            prior_source_key = previous_source_key.get(event.ledger_name)
            if prior_source_key is not None and source_key <= prior_source_key:
                raise RiskBlock("LEDGER_EVENTS_OUT_OF_ORDER")
            if event.message_time is not None and event.received_at is not None:
                prior_message_time = previous_source_message_time.get(
                    event.ledger_name
                )
                prior_received_at = previous_source_received_at.get(
                    event.ledger_name
                )
                if (
                    prior_message_time is not None
                    and event.message_time < prior_message_time
                ) or (
                    prior_received_at is not None
                    and event.received_at < prior_received_at
                ):
                    raise RiskBlock("LEDGER_EVENT_SOURCE_TIME_OUT_OF_ORDER")
                previous_source_message_time[event.ledger_name] = event.message_time
                previous_source_received_at[event.ledger_name] = event.received_at
            previous_source_key[event.ledger_name] = source_key
            source_coordinates.add(coordinate)
        by_id[event.event_id] = event
        normalized.append(event)
    return tuple(normalized)


def _ledger_projection_order_key(
    event: LedgerEvent,
) -> tuple[str, datetime, int, int, str]:
    return (
        event.ledger_name,
        event.lot.at.astimezone(UTC),
        event.cursor if event.cursor is not None else 0,
        event.ordinal,
        event.event_id,
    )


def _ledger_event_content_digest(event: LedgerEvent) -> str:
    return _canonical_digest(
        "stock-monitor/ledger-event/v1",
        {
            "ledger_name": event.ledger_name,
            "signal_id": event.signal_id,
            "lot": {
                "price_micros": money_to_micros(event.lot.price),
                "shares": event.lot.shares,
                "total_cost_micros": event.lot.total_cost_micros,
                "at": _canonical_timestamp(event.lot.at),
                "parent_order_id": event.lot.parent_order_id,
            },
            "signal_digest": event.signal_digest,
            "user_confirmed_stop_micros": (
                None
                if event.user_confirmed_stop is None
                else money_to_micros(event.user_confirmed_stop)
            ),
            "recommended_stop_micros": (
                None
                if event.recommended_stop is None
                else money_to_micros(event.recommended_stop)
            ),
            "profit_target_taken": event.profit_target_taken,
            "decision": {
                "status": event.decision.status,
                "compliant": event.decision.compliant,
                "reconciliation_required": event.decision.reconciliation_required,
                "reason_codes": list(event.decision.reason_codes),
            },
            "event_id": event.event_id,
            "cursor": event.cursor,
            "ordinal": event.ordinal,
            "authority_basis": event.authority_basis,
            "message_time": (
                None
                if event.message_time is None
                else _canonical_timestamp(event.message_time)
            ),
            "received_at": (
                None
                if event.received_at is None
                else _canonical_timestamp(event.received_at)
            ),
        },
    )


def _canonical_timestamp(value: datetime) -> str:
    return (
        value.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _canonical_digest(namespace: str, payload: object) -> str:
    canonical = json.dumps(
        {"namespace": namespace, "payload": payload},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def _execution_event_payload(event: ExecutionEvent) -> dict[str, object]:
    return {
        "kind": event.kind,
        "at": _canonical_timestamp(event.at),
        "price_micros": (
            None if event.price is None else money_to_micros(event.price)
        ),
        "shares": event.shares,
        "amount_micros": (
            None if event.amount is None else money_to_micros(event.amount)
        ),
        "cursor": event.cursor,
        "message_time": (
            None
            if event.message_time is None
            else _canonical_timestamp(event.message_time)
        ),
        "received_at": (
            None
            if event.received_at is None
            else _canonical_timestamp(event.received_at)
        ),
        "parent_order_id": event.parent_order_id,
        "fill_group_planned_shares": event.fill_group_planned_shares,
    }


def _breaker_payload(state: BreakerState) -> dict[str, object]:
    def day(value: date | None) -> str | None:
        return None if value is None else value.isoformat()

    def money(value: Decimal | None) -> int | None:
        return None if value is None else money_to_micros(value)

    return {
        "as_of": day(state.as_of),
        "paused": state.live_entries_paused,
        "reasons": list(state.reason_codes),
        "consecutive_losses": state.consecutive_losses,
        "loss_trigger": day(state.loss_trigger_session),
        "loss_pause_through": day(state.loss_pause_through),
        "loss_resume": day(state.loss_resume_session),
        "weekly_high_water_micros": money(state.weekly_high_water),
        "weekly_drawdown_micros": money(state.weekly_drawdown),
        "weekly_pause_through": day(state.weekly_pause_through),
        "monthly_high_water_micros": money(state.monthly_high_water),
        "monthly_drawdown_micros": money(state.monthly_drawdown),
        "monthly_pause_through": day(state.monthly_pause_through),
        "ledger_name": state.ledger_name,
        "history_digest": state.history_digest,
        "calendar_digest": state.calendar_digest,
    }


def _calendar_payload(resolver: SessionCalendarResolver) -> list[object]:
    return [
        {
            "year": calendar.year,
            "retrieved_at": calendar.retrieved_at.isoformat(),
            "reviewed_at": calendar.reviewed_at.isoformat(),
            "closed_dates": [day.isoformat() for day in calendar.closed_dates],
            "early_closes": [
                {
                    "day": day.isoformat(),
                    "open": session.open_time.isoformat(),
                    "close": session.close_time.isoformat(),
                    "review": session.review_time.isoformat(),
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
                }
                for source in calendar.sources
            ],
        }
        for calendar in resolver.calendars
    ]


def _verified_batch_fingerprint(
    batch: VerifiedLedgerEventBatch,
) -> tuple[object, ...]:
    return (batch.references,)


def _replay_cohort_fingerprint(
    cohort: VerifiedLedgerReplayCohort,
) -> tuple[object, ...]:
    return (
        cohort.ledger_name,
        cohort.references,
        cohort.expected_count,
        cohort.start_cursor,
        cohort.terminal_cursor,
        cohort.query_cutoff,
        cohort.source_digest,
    )


def _actual_projection_position_fingerprint(position: object) -> tuple[object, ...]:
    return (
        getattr(position, "signal_id"),
        getattr(position, "symbol"),
        getattr(position, "lineage_kind"),
        getattr(position, "signal_digest"),
        tuple(
            (
                lot.source_event_id,
                lot.source_cursor,
                lot.remaining_shares,
                lot.unit_cost_micros,
                lot.acquired_at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ),
                lot.received_at.astimezone(UTC).isoformat(
                    timespec="microseconds"
                ),
                lot.parent_order_id,
            )
            for lot in getattr(position, "lots")
        ),
        getattr(position, "recommended_stop_micros"),
        getattr(position, "user_stop_micros"),
        getattr(position, "target_micros"),
        getattr(position, "tick_micros"),
        getattr(position, "cumulative_buy_cost_micros"),
        getattr(position, "cumulative_sale_proceeds_micros"),
        getattr(position, "linked_fees_micros"),
        tuple(getattr(position, "reason_codes")),
        tuple(getattr(position, "lifecycle_event_ids")),
    )


def _actual_projection_cohort_fingerprint(
    cohort: ActualProjectionCohort,
) -> tuple[object, ...]:
    return (
        tuple(
            _actual_projection_position_fingerprint(position)
            for position in cohort.positions
        ),
        cohort.projection_start_cursor,
        cohort.projection_terminal_cursor,
        cohort.source_through_cursor,
        cohort.physical_source_highwater_cursor,
        cohort.query_cutoff.astimezone(UTC).isoformat(timespec="microseconds"),
        cohort.journal_source_digest,
        cohort.actual_state_digest,
        cohort.calendar_digest,
        cohort.policy_digest,
        cohort.expected_action_count,
        cohort.expected_posting_count,
    )


def is_issued_actual_projection_cohort(cohort: object) -> bool:
    if not isinstance(cohort, ActualProjectionCohort):
        return False
    try:
        fingerprint = _actual_projection_cohort_fingerprint(cohort)
    except Exception:
        return False
    with _EVENT_AUTHORITY_LOCK:
        issued = _ACTUAL_PROJECTION_COHORT_AUTHORITIES.get(id(cohort))
        if (
            issued is None
            or issued[0]() is not cohort
            or issued[1] != fingerprint
        ):
            return False
        source = issued[2]()
    if source is None:
        return False
    from .journal import is_verified_journal_replay_source

    return is_verified_journal_replay_source(source)


def _issue_actual_projection_from_journal(
    source: object,
    state: object,
) -> ActualProjectionCohort:
    """Issue open positions from one exact verified Journal/state cohort."""
    from .journal import (
        JournalActualReplaySource,
        is_verified_journal_replay_source,
    )
    from .reconciliation import (
        ActualLedgerState,
        ActualPositionState,
        is_verified_actual_ledger_state,
        is_verified_actual_ledger_state_for_source,
    )

    if not isinstance(source, JournalActualReplaySource) or not (
        is_verified_journal_replay_source(source)
    ):
        raise RiskBlock("JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED")
    if not isinstance(state, ActualLedgerState) or not is_verified_actual_ledger_state(
        state
    ):
        raise RiskBlock("ACTUAL_LEDGER_STATE_UNVERIFIED")
    if not is_verified_actual_ledger_state_for_source(state, source):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    if (
        state.query_cutoff != source.query_cutoff
        or state.through_cursor != source.terminal_cursor
        or source.through_execution_cursor != source.terminal_cursor
        or state.journal_source_digest != source.source_digest
        or state.settlement_ledger != source.postings
        or source.expected_action_count != len(source.actions)
        or source.expected_posting_count != len(source.postings)
        or not state.calendar_release_verified
        or state.calendar_digest is None
        or state.policy_digest is None
    ):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    if any(not isinstance(position, ActualPositionState) for position in state.positions):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    strategy_positions = tuple(
        position
        for position in state.positions
        if position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
    )
    actions_by_id = {action.event_id: action for action in source.actions}
    lifecycle_ids = tuple(
        dict.fromkeys(
            (
                *(
                    event_id
                    for position in strategy_positions
                    for event_id in position.lifecycle_event_ids
                ),
                *(
                    event_id
                    for trade in state.closed_trades
                    for event_id in trade.source_event_ids
                ),
            )
        )
    )
    if any(event_id not in actions_by_id for event_id in lifecycle_ids):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    lifecycle_cursors = tuple(
        sorted(actions_by_id[event_id].execution_event_id for event_id in lifecycle_ids)
    )
    cohort = ActualProjectionCohort(
        positions=strategy_positions,
        projection_start_cursor=(
            lifecycle_cursors[0] if lifecycle_cursors else None
        ),
        projection_terminal_cursor=(
            lifecycle_cursors[-1] if lifecycle_cursors else None
        ),
        source_through_cursor=source.through_execution_cursor,
        physical_source_highwater_cursor=source.source_through_cursor,
        query_cutoff=source.query_cutoff,
        journal_source_digest=source.source_digest,
        actual_state_digest=state.source_digest,
        calendar_digest=state.calendar_digest,
        policy_digest=state.policy_digest,
        expected_action_count=source.expected_action_count,
        expected_posting_count=source.expected_posting_count,
    )
    identity = id(cohort)

    def discard(dead: ReferenceType[object]) -> None:
        with _EVENT_AUTHORITY_LOCK:
            current = _ACTUAL_PROJECTION_COHORT_AUTHORITIES.get(identity)
            if current is not None and current[0] is dead:
                _ACTUAL_PROJECTION_COHORT_AUTHORITIES.pop(identity, None)

    reference = ref(cohort, discard)
    with _EVENT_AUTHORITY_LOCK:
        _ACTUAL_PROJECTION_COHORT_AUTHORITIES[identity] = (
            reference,
            _actual_projection_cohort_fingerprint(cohort),
            ref(source),
        )
    return cohort


def is_issued_verified_replay_cohort(cohort: object) -> bool:
    """Return false until Task 7/8 registers a source-row-verified cohort."""
    if not isinstance(cohort, VerifiedLedgerReplayCohort):
        return False
    try:
        fingerprint = _replay_cohort_fingerprint(cohort)
    except Exception:
        return False
    return _has_ledger_authority(
        _VERIFIED_REPLAY_COHORT_AUTHORITIES,
        cohort,
        fingerprint,
    ) and _phase1_sources_are_current(cohort)


def _replay_cohort_authorizes_stream(
    cohorts: Sequence[VerifiedLedgerReplayCohort],
    ledger_name: str,
    events: Sequence[LedgerEvent],
    signals: Sequence[LedgerSignal],
) -> bool:
    candidates = tuple(
        cohort
        for cohort in cohorts
        if cohort.ledger_name == ledger_name
        and is_issued_verified_replay_cohort(cohort)
    )
    if len(candidates) != 1:
        return False
    cohort = candidates[0]
    stream = tuple(event for event in events if event.ledger_name == ledger_name)
    if len(stream) != cohort.expected_count:
        return False
    by_signal = {signal.signal_id: signal for signal in signals}
    references: list[tuple[str, str, str]] = []
    for event in stream:
        signal = by_signal.get(event.signal_id)
        if (
            signal is None
            or event.signal_digest is None
            or event.message_time is None
            or event.received_at is None
            or event.cursor is None
            or event.signal_digest != _ledger_signal_digest(signal)
            or event.received_at > cohort.query_cutoff
        ):
            return False
        references.append(
            (
                event.event_id,
                _ledger_event_content_digest(event),
                event.signal_digest,
            )
        )
    cursors = tuple(event.cursor for event in stream)
    if stream:
        if (
            cohort.start_cursor != cursors[0]
            or cohort.terminal_cursor != cursors[-1]
        ):
            return False
    elif cohort.start_cursor is not None or cohort.terminal_cursor is not None:
        return False
    return tuple(references) == cohort.references


def _register_verified_batch(batch: VerifiedLedgerEventBatch) -> None:
    identity = id(batch)

    def discard(dead: ReferenceType[object]) -> None:
        with _EVENT_AUTHORITY_LOCK:
            current = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(identity)
            if current is not None and current[0] is dead:
                _VERIFIED_LEDGER_BATCH_AUTHORITIES.pop(identity, None)

    reference = ref(batch, discard)
    with _EVENT_AUTHORITY_LOCK:
        _VERIFIED_LEDGER_BATCH_AUTHORITIES[identity] = (
            reference,
            _verified_batch_fingerprint(batch),
        )


def _is_issued_verified_batch(batch: object) -> bool:
    if not isinstance(batch, VerifiedLedgerEventBatch):
        return False
    with _EVENT_AUTHORITY_LOCK:
        registered = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(id(batch))
        return (
            registered is not None
            and registered[0]() is batch
            and registered[1] == _verified_batch_fingerprint(batch)
        )


def _issue_verified_ledger_event_batch(
    events: Sequence[LedgerEvent],
) -> VerifiedLedgerEventBatch:
    """Reject event-only reissuance; source rows must be independently verified."""
    del events
    raise RiskBlock("LEDGER_EVENT_SOURCE_EVIDENCE_REQUIRED")


def _issue_live_verified_ledger_event_batch(
    event: LedgerEvent,
    context: ActualBuyContext,
) -> VerifiedLedgerEventBatch:
    """Bind one in-process event to its exact issued source evidence."""
    if (
        not isinstance(event, LedgerEvent)
        or event.ledger_name != "ACTUAL"
        or not is_issued_actual_buy_context(context)
        or context.buy_action is None
        or event.event_id != context.buy_action.event_id
        or event.signal_id != context.authorized_signal_id
        or event.signal_digest is None
        or event.message_time is None
        or event.received_at is None
        or event.lot.price != context.buy_action.price
        or event.lot.shares != context.buy_action.shares
        or event.lot.at != context.buy_action.at
        or event.lot.parent_order_id != context.buy_action.parent_order_id
        or event.user_confirmed_stop != context.buy_action.user_confirmed_stop
        or event.authority_basis
        != _actual_buy_source_fingerprint(
            signal_id=event.signal_id,
            lot=event.lot,
            context=context,
            event_id=event.event_id,
        )
    ):
        raise RiskBlock("LEDGER_EVENT_SOURCE_EVIDENCE_REQUIRED")
    batch = VerifiedLedgerEventBatch(
        ((event.event_id, _ledger_event_content_digest(event)),)
    )
    _register_verified_batch(batch)
    return batch


def _issue_paper_verified_ledger_event_batch(
    event: LedgerEvent,
    authority: PaperEntryAuthority,
) -> VerifiedLedgerEventBatch:
    if (
        not isinstance(event, LedgerEvent)
        or event.ledger_name != "CANONICAL"
        or not is_issued_paper_entry_authority(authority)
        or event.event_id != authority.canonical_event_id
        or event.signal_id != authority.signal_id
        or event.signal_digest != authority.signal_digest
        or event.message_time is None
        or event.received_at is None
        or event.lot.at != authority.quote_at
        or event.authority_basis != authority.source_digest
    ):
        raise RiskBlock("LEDGER_EVENT_SOURCE_EVIDENCE_REQUIRED")
    batch = VerifiedLedgerEventBatch(
        ((event.event_id, _ledger_event_content_digest(event)),)
    )
    _register_verified_batch(batch)
    return batch


def _batch_authorizes_event(
    batch: VerifiedLedgerEventBatch | None,
    event: LedgerEvent,
) -> bool:
    return (
        _is_issued_verified_batch(batch)
        and event.authority_basis is not None
        and event.message_time is not None
        and event.received_at is not None
        and (event.event_id, _ledger_event_content_digest(event))
        in batch.references  # type: ignore[union-attr]
    )


def _actual_buy_source_fingerprint(
    *,
    signal_id: str,
    lot: LedgerLot,
    context: ActualBuyContext,
    event_id: str,
) -> str:
    action = context.buy_action
    plan_decision = context.plan_decision
    return _canonical_digest(
        "stock-monitor/actual-buy-source/v1",
        {
            "signal_id": signal_id,
            "event_id": event_id,
            "lot": {
                "price_micros": money_to_micros(lot.price),
                "shares": lot.shares,
                "at": _canonical_timestamp(lot.at),
                "parent_order_id": lot.parent_order_id,
            },
            "account_check": {
                "settled_cash_micros": money_to_micros(
                    context.account_check.settled_cash
                ),
                "pending_orders": context.account_check.pending_orders,
                "unlogged_positions": context.account_check.unlogged_positions,
                "at": _canonical_timestamp(context.account_check.at),
                "reconciliation_result": context.account_check.reconciliation_result,
                "cursor": context.account_check.cursor,
            },
            "window": {
                "after_cursor": context.event_window.after_cursor,
                "through_cursor": context.event_window.through_cursor,
                "events": [
                    _execution_event_payload(event)
                    for event in context.event_window.events
                ],
                "source": context.event_window.source,
            },
            "confirmation": (
                None
                if action is None
                else {
                    "event_id": action.event_id,
                    "idempotency_key": action.idempotency_key,
                    "message_id": action.message_id,
                    "action_ordinal": action.action_ordinal,
                    "cursor": action.cursor,
                    "symbol": action.symbol,
                    "shares": action.shares,
                    "price_micros": money_to_micros(action.price),
                    "at": _canonical_timestamp(action.at),
                    "message_time": _canonical_timestamp(action.message_time),
                    "received_at": _canonical_timestamp(action.received_at),
                    "bid_micros": money_to_micros(action.bid),
                    "ask_micros": money_to_micros(action.ask),
                    "stop_micros": (
                        None
                        if action.user_confirmed_stop is None
                        else money_to_micros(action.user_confirmed_stop)
                    ),
                    "source": action.source,
                    "raw_sha256": action.raw_sha256,
                    "details_sha256": action.details_sha256,
                    "parent_order_id": action.parent_order_id,
                    "fill_group_planned_shares": (
                        action.fill_group_planned_shares
                    ),
                }
            ),
            "bid_micros": money_to_micros(context.bid),
            "ask_micros": money_to_micros(context.ask),
            "stop_micros": (
                None
                if context.user_confirmed_stop is None
                else money_to_micros(context.user_confirmed_stop)
            ),
            "breaker": {
                "as_of": context.breaker_state.as_of.isoformat(),
                "paused": context.breaker_state.live_entries_paused,
                "reasons": list(context.breaker_state.reason_codes),
                "canonical": _breaker_payload(context.breaker_state.canonical),
                "actual": _breaker_payload(context.breaker_state.actual),
            },
            "actual_breaker_refresh": (
                None
                if context.actual_breaker_refresh is None
                else {
                    "as_of": _canonical_timestamp(
                        context.actual_breaker_refresh.as_of
                    ),
                    "through_execution_cursor": (
                        context.actual_breaker_refresh.through_execution_cursor
                    ),
                    "through_close_cursor": (
                        context.actual_breaker_refresh.through_close_cursor
                    ),
                    "calendar_digest": (
                        context.actual_breaker_refresh.calendar_digest
                    ),
                    "source_digest": (
                        context.actual_breaker_refresh.source_digest
                    ),
                }
            ),
            "calendar": _calendar_payload(context.calendar_resolver),
            "plan": (
                None
                if plan_decision is None
                else {
                    "eligible": plan_decision.eligible,
                    "authority_scope": plan_decision.authority_scope,
                    "authority_digest": plan_decision.authority_digest,
                    "as_of": (
                        None
                        if plan_decision.as_of is None
                        else _canonical_timestamp(plan_decision.as_of)
                    ),
                    "request": (
                        None
                        if plan_decision.request is None
                        else {
                            "symbol": plan_decision.request.symbol,
                            "session_date": plan_decision.request.session_date.isoformat(),
                            "entry_micros": money_to_micros(
                                plan_decision.request.entry
                            ),
                            "stop_micros": money_to_micros(
                                plan_decision.request.stop
                            ),
                            "tick_micros": money_to_micros(
                                plan_decision.request.tick_size
                            ),
                            "published_target_micros": (
                                None
                                if plan_decision.request.published_target is None
                                else money_to_micros(
                                    plan_decision.request.published_target
                                )
                            ),
                        }
                    ),
                    "quantity": (
                        None
                        if plan_decision.plan is None
                        else plan_decision.plan.quantity
                    ),
                }
            ),
        },
    )


def _apply_position_event(
    positions: tuple[LedgerPosition, ...],
    signal: LedgerSignal,
    event: LedgerEvent,
) -> tuple[LedgerPosition, ...]:
    existing_index = next(
        (
            index
            for index, position in enumerate(positions)
            if position.signal_id == signal.signal_id
        ),
        None,
    )
    lot = _copy_lot(event.lot)
    if existing_index is None:
        recommended_stop = (
            signal.recommended_stop
            if event.recommended_stop is None
            else event.recommended_stop
        )
        if recommended_stop < signal.recommended_stop:
            raise RiskBlock("RECOMMENDED_STOP_WIDENED")
        position = LedgerPosition(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            ledger_name=event.ledger_name,
            recommended_stop=recommended_stop,
            user_confirmed_stop=event.user_confirmed_stop,
            target=signal.target,
            tick_size=signal.tick_size,
            lots=(lot,),
            reconciled=event.decision.compliant,
            reason_codes=event.decision.reason_codes,
            profit_target_taken=event.profit_target_taken,
        )
        return (*positions, position)
    existing = positions[existing_index]
    recommended_stop = (
        existing.recommended_stop
        if event.recommended_stop is None
        else event.recommended_stop
    )
    if recommended_stop < existing.recommended_stop:
        raise RiskBlock("RECOMMENDED_STOP_WIDENED")
    projected_lots = tuple(
        sorted(
            (*existing.lots, lot),
            key=lambda projected: projected.at.astimezone(UTC),
        )
    )
    updated = replace(
        existing,
        lots=projected_lots,
        recommended_stop=recommended_stop,
        user_confirmed_stop=(
            event.user_confirmed_stop
            if event.user_confirmed_stop is not None
            else existing.user_confirmed_stop
        ),
        reconciled=existing.reconciled and event.decision.compliant,
        reason_codes=tuple(
            dict.fromkeys((*existing.reason_codes, *event.decision.reason_codes))
        ),
        profit_target_taken=(
            existing.profit_target_taken or event.profit_target_taken
        ),
    )
    return positions[:existing_index] + (updated,) + positions[existing_index + 1 :]


def _validate_projection_money(
    ledger: CanonicalLedger | ActualLedger,
) -> None:
    # Validate the persisted aggregate columns first.  A single oversized lot
    # necessarily makes the deployed-capital projection unpersistable, and the
    # aggregate reason is the stable boundary reported by the ledger API.
    _money(
        ledger.deployed_capital,
        "INVALID_DEPLOYED_CAPITAL",
        nonnegative=True,
    )
    _money(
        ledger.open_planned_risk,
        "INVALID_OPEN_RISK",
        nonnegative=True,
    )
    _money(ledger.cash, "INVALID_LEDGER_CASH")
    for position in ledger.open_positions:
        _money(
            position.exposure,
            "INVALID_POSITION_EXPOSURE",
            nonnegative=True,
        )
        _money(
            position.planned_risk,
            "INVALID_POSITION_RISK",
            nonnegative=True,
        )


def _actual_projection_matches_portfolio_state(
    actual: ActualLedger,
    authority: PortfolioRiskAuthority,
    session_date: date,
    signal_id: str,
    parent_order_id: str | None,
) -> bool:
    """Bind an ACTUAL plan to the receiving pair's pre-fill projection."""
    state = authority.portfolio_state
    positions = actual.open_positions
    if parent_order_id is not None:
        group_positions = tuple(
            position
            for position in positions
            if position.signal_id == signal_id
        )
        if len(group_positions) > 1:
            return False
        if group_positions:
            group = group_positions[0]
            if (
                not group.reconciled
                or group.reason_codes
                or group.user_confirmed_stop is None
                or any(
                    lot.parent_order_id != parent_order_id
                    for lot in group.lots
                )
            ):
                return False
            positions = tuple(
                position for position in positions if position is not group
            )
    deployed = _sum_money(
        (position.exposure for position in positions),
        "INVALID_DEPLOYED_CAPITAL",
    )
    open_risk = _sum_money(
        (position.planned_risk for position in positions),
        "INVALID_OPEN_RISK",
    )
    entries_this_session = sum(
        position.lots[0].at.astimezone(_ET).date() == session_date
        for position in positions
    )
    return (
        state.deployed == deployed
        and state.open_risk == open_risk
        and state.open_position_count == len(positions)
        and state.entries_this_session == entries_this_session
        and state.open_symbols
        == tuple(sorted({position.symbol for position in positions}))
        and state.reconciliation_required == actual.reconciliation_required
        and state.stop_unverified
        == any(position.user_confirmed_stop is None for position in positions)
    )


def _apply_ledger_event_unchecked(
    signals: Sequence[LedgerSignal],
    canonical: CanonicalLedger,
    actual: ActualLedger,
    event: LedgerEvent,
) -> tuple[CanonicalLedger, ActualLedger]:
    signal = _signal_by_id(signals, event.signal_id)
    if event.ledger_name == "CANONICAL":
        positions = _apply_position_event(canonical.open_positions, signal, event)
        projected = replace(
                canonical,
                open_positions=positions,
                events_applied=canonical.events_applied + 1,
        )
        _validate_projection_money(projected)
        return (projected, actual)
    positions = _apply_position_event(actual.open_positions, signal, event)
    reasons = tuple(dict.fromkeys((*actual.reason_codes, *event.decision.reason_codes)))
    stop_unverified = any(
        position.user_confirmed_stop is None for position in positions
    )
    projected = replace(
            actual,
            open_positions=positions,
            reconciliation_required=bool(reasons),
            reason_codes=reasons,
            stop_unverified=stop_unverified,
            events_applied=actual.events_applied + 1,
    )
    _validate_projection_money(projected)
    return (canonical, projected)


def _required_actual_reason_codes(
    signal: LedgerSignal,
    lot: LedgerLot,
    user_confirmed_stop: Decimal | None,
    actual: ActualLedger,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not is_issued_ledger_signal(signal):
        reasons.append("UNPLANNED_SIGNAL")
        reasons.append("PUBLICATION_AUTHORITY_UNVERIFIED")
    existing_same = next(
        (
            position
            for position in actual.open_positions
            if position.signal_id == signal.signal_id
        ),
        None,
    )
    if lot.price > signal.maximum_entry:
        reasons.append("FILL_ABOVE_MAXIMUM_ENTRY")
    if lot.price <= signal.recommended_stop:
        reasons.append("NON_POSITIVE_STOP_DISTANCE")
    if signal.role != "PRIMARY":
        reasons.append("SHADOW_FILL")
    if lot.at.astimezone(_ET).date() != signal.publication_session:
        reasons.append("ENTRY_SESSION_MISMATCH")
    if existing_same is None:
        if lot.shares != signal.planned_shares and (
            lot.parent_order_id is None or lot.shares > signal.planned_shares
        ):
            reasons.append("SHARE_QUANTITY_MISMATCH")
    else:
        same_parent_fill = (
            lot.parent_order_id is not None
            and all(
                prior.parent_order_id == lot.parent_order_id
                for prior in existing_same.lots
            )
        )
        if same_parent_fill:
            if existing_same.shares + lot.shares > signal.planned_shares:
                reasons.append("SHARE_QUANTITY_MISMATCH")
        else:
            reasons.append("POSITION_ADDITIONS_PROHIBITED")
            if lot.price < existing_same.entry:
                reasons.append("AVERAGING_DOWN_PROHIBITED")
    if any(
        position.signal_id != signal.signal_id
        and position.symbol == signal.symbol
        for position in actual.open_positions
    ):
        reasons.append("DUPLICATE_TICKER_EXPOSURE")
    reasons.extend(_actual_stop_reason_codes(signal, lot, user_confirmed_stop))
    if actual.reconciliation_required:
        reasons.append("PRIOR_RECONCILIATION_REQUIRED")
    return tuple(reasons)


def _actual_stop_reason_codes(
    signal: LedgerSignal,
    lot: LedgerLot,
    user_confirmed_stop: Decimal | None,
) -> tuple[str, ...]:
    if user_confirmed_stop is None:
        return ("STOP_UNVERIFIED",)
    reasons: list[str] = []
    if not _tick_aligned(user_confirmed_stop, signal.tick_size):
        reasons.append("USER_STOP_NOT_TICK_ALIGNED")
    if user_confirmed_stop < signal.recommended_stop:
        reasons.append("USER_STOP_WIDER_THAN_RECOMMENDED")
    if user_confirmed_stop >= lot.price:
        reasons.append("USER_STOP_NOT_BELOW_FILL")
    return tuple(reasons)


def apply_ledger_event(
    signals: Sequence[LedgerSignal],
    canonical: CanonicalLedger,
    actual: ActualLedger,
    event: LedgerEvent,
    *,
    verified_event_batch: VerifiedLedgerEventBatch | None = None,
) -> tuple[CanonicalLedger, ActualLedger]:
    """Validate and project one frozen authority event into one disjoint side."""
    if not isinstance(canonical, CanonicalLedger) or not isinstance(
        actual,
        ActualLedger,
    ):
        raise RiskBlock("INVALID_LEDGER_PROJECTION")
    if not isinstance(event, LedgerEvent):
        raise RiskBlock("INVALID_LEDGER_EVENTS")
    signal = _signal_by_id(signals, event.signal_id)
    if event.ledger_name == "CANONICAL":
        authorized_canonical_event = _batch_authorizes_event(
            verified_event_batch,
            event,
        ) and is_issued_ledger_signal(signal) and (
            event.signal_digest == _ledger_signal_digest(signal)
        )
        expected_decision = (
            ComplianceDecision("COMPLIANT", True, False, ())
            if authorized_canonical_event
            else ComplianceDecision(
                "NONCOMPLIANT_RECONCILIATION_REQUIRED",
                False,
                True,
                tuple(
                    dict.fromkeys(
                        (
                            "PAPER_ENTRY_AUTHORITY_UNVERIFIED",
                            *(
                                ("RESTART_AUTHORITY_UNVERIFIED",)
                                if event.authority_basis is not None
                                else ()
                            ),
                        )
                    )
                ),
            )
        )
        if event.decision != expected_decision:
            raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
        if event.user_confirmed_stop is not None:
            raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
        if signal.role != "PRIMARY":
            raise RiskBlock("CANONICAL_SHADOW_FILL_PROHIBITED")
        if event.lot.at.astimezone(_ET).date() != signal.publication_session:
            raise RiskBlock("ENTRY_SESSION_MISMATCH")
        if event.lot.price != signal.maximum_entry:
            raise RiskBlock("CANONICAL_FILL_AUTHORITY_UNVERIFIED")
        phase1_remainder_projection = (
            0 < event.lot.shares < signal.planned_shares
            and any(
                kind == "CANONICAL_REPLAY"
                for _source, kind in _phase1_bound_sources(event)
            )
            and _phase1_sources_are_current(event)
        )
        if (
            event.lot.shares != signal.planned_shares
            and not phase1_remainder_projection
        ):
            raise RiskBlock("SHARE_QUANTITY_MISMATCH")
        if any(
            position.signal_id == signal.signal_id
            for position in canonical.open_positions
        ):
            raise RiskBlock("POSITION_ADDITIONS_PROHIBITED")
        if any(
            position.signal_id != signal.signal_id
            and position.symbol == signal.symbol
            for position in canonical.open_positions
        ):
            raise RiskBlock("DUPLICATE_TICKER_EXPOSURE")
        if len(canonical.open_positions) >= _MAX_POSITIONS:
            raise RiskBlock("POSITION_LIMIT_REACHED")
        entries = sum(
            position.lots[0].at.astimezone(_ET).date()
            == signal.publication_session
            for position in canonical.open_positions
        )
        if entries >= _MAX_ENTRIES_PER_SESSION:
            raise RiskBlock("SESSION_ENTRY_LIMIT_REACHED")
        projected, unchanged_actual = _apply_ledger_event_unchecked(
            signals,
            canonical,
            actual,
            event,
        )
        projected_position = next(
            position
            for position in projected.open_positions
            if position.signal_id == signal.signal_id
        )
        if projected_position.planned_risk > _MAX_POSITION_RISK:
            raise RiskBlock("POSITION_RISK_LIMIT_BREACHED")
        if projected.deployed_capital > _MAX_EXPOSURE:
            raise RiskBlock("LIVE_EXPOSURE_LIMIT_BREACHED")
        if projected.open_planned_risk > _MAX_COMBINED_RISK:
            raise RiskBlock("COMBINED_RISK_LIMIT_BREACHED")
        return projected, unchanged_actual

    authorized_actual_event = _batch_authorizes_event(
        verified_event_batch,
        event,
    )
    if authorized_actual_event and not is_issued_ledger_signal(signal):
        authorized_actual_event = False
    if authorized_actual_event and (
        event.signal_digest != _ledger_signal_digest(signal)
    ):
        authorized_actual_event = False
    if event.authority_basis is not None and not authorized_actual_event:
        required = _required_actual_reason_codes(
            signal,
            event.lot,
            event.user_confirmed_stop,
            actual,
        )
        reasons = tuple(
            dict.fromkeys(
                (
                    "AUTHORITY_CONTEXT_UNVERIFIED",
                    "RESTART_AUTHORITY_UNVERIFIED",
                    *event.decision.reason_codes,
                    *required,
                )
            )
        )
        downgraded = replace(
            event,
            decision=ComplianceDecision(
                "NONCOMPLIANT_RECONCILIATION_REQUIRED",
                False,
                True,
                reasons,
            ),
        )
        projected_canonical, projected_actual = _apply_ledger_event_unchecked(
            signals,
            canonical,
            actual,
            downgraded,
        )
        projected_position = next(
            position
            for position in projected_actual.open_positions
            if position.signal_id == signal.signal_id
        )
        cap_reasons: list[str] = []
        if projected_position.planned_risk > _MAX_POSITION_RISK:
            cap_reasons.append("POSITION_RISK_LIMIT_BREACHED")
        if projected_actual.deployed_capital > _MAX_EXPOSURE:
            cap_reasons.append("LIVE_EXPOSURE_LIMIT_BREACHED")
        if projected_actual.open_planned_risk > _MAX_COMBINED_RISK:
            cap_reasons.append("COMBINED_RISK_LIMIT_BREACHED")
        if len(projected_actual.open_positions) > _MAX_POSITIONS:
            cap_reasons.append("POSITION_LIMIT_BREACHED")
        if cap_reasons:
            downgraded = replace(
                downgraded,
                decision=replace(
                    downgraded.decision,
                    reason_codes=tuple(
                        dict.fromkeys((*reasons, *cap_reasons))
                    ),
                ),
            )
            projected_canonical, projected_actual = _apply_ledger_event_unchecked(
                signals,
                canonical,
                actual,
                downgraded,
            )
        return projected_canonical, projected_actual
    if authorized_actual_event:
        if "AUTHORITY_CONTEXT_UNVERIFIED" in event.decision.reason_codes:
            raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
    elif (
        event.decision.compliant
        or "AUTHORITY_CONTEXT_UNVERIFIED" not in event.decision.reason_codes
    ):
        raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
    required = _required_actual_reason_codes(
        signal,
        event.lot,
        event.user_confirmed_stop,
        actual,
    )
    if any(reason not in event.decision.reason_codes for reason in required):
        raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
    projected_canonical, projected_actual = _apply_ledger_event_unchecked(
        signals,
        canonical,
        actual,
        event,
    )
    projected_position = next(
        position
        for position in projected_actual.open_positions
        if position.signal_id == signal.signal_id
    )
    cap_reasons: list[str] = []
    if projected_position.planned_risk > _MAX_POSITION_RISK:
        cap_reasons.append("POSITION_RISK_LIMIT_BREACHED")
    if projected_actual.deployed_capital > _MAX_EXPOSURE:
        cap_reasons.append("LIVE_EXPOSURE_LIMIT_BREACHED")
    if projected_actual.open_planned_risk > _MAX_COMBINED_RISK:
        cap_reasons.append("COMBINED_RISK_LIMIT_BREACHED")
    if len(projected_actual.open_positions) > _MAX_POSITIONS:
        cap_reasons.append("POSITION_LIMIT_BREACHED")
    if any(reason not in event.decision.reason_codes for reason in cap_reasons):
        raise RiskBlock("LEDGER_EVENT_DECISION_CONFLICT")
    return projected_canonical, projected_actual


class LedgerPair:
    """Compatibility façade replacing frozen projections after each transition."""

    __slots__ = (
        "_signals",
        "_canonical",
        "_actual",
        "_events",
        "_canonical_replay_cohort",
        "_actual_replay_cohort",
        "_canonical_replay_verified",
        "_actual_replay_verified",
        "_replay_verified",
        "_sealed",
    )

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("LedgerPair projections are read-only")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        *,
        signals: Sequence[LedgerSignal],
        canonical: CanonicalLedger | None = None,
        actual: ActualLedger | None = None,
        events: Sequence[LedgerEvent] = (),
        verified_event_batch: VerifiedLedgerEventBatch | None = None,
        verified_replay_cohorts: Sequence[VerifiedLedgerReplayCohort] = (),
    ) -> None:
        if isinstance(signals, (str, bytes)):
            raise RiskBlock("INVALID_LEDGER_SIGNALS")
        copied_signals = tuple(signals)
        copied_events = _normalize_ledger_events(tuple(events))
        cohorts = tuple(verified_replay_cohorts)
        if any(
            not isinstance(cohort, VerifiedLedgerReplayCohort)
            for cohort in cohorts
        ):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        if len(tuple(cohort.ledger_name for cohort in cohorts)) != len(
            {cohort.ledger_name for cohort in cohorts}
        ):
            raise RiskBlock("INVALID_REPLAY_COHORT")
        issued_empty_canonical_replay = (
            not copied_signals
            and not copied_events
            and len(cohorts) == 1
            and cohorts[0].ledger_name == "CANONICAL"
            and cohorts[0].expected_count == 0
            and is_issued_verified_replay_cohort(cohorts[0])
        )
        if (
            any(not isinstance(signal, LedgerSignal) for signal in copied_signals)
            or (not copied_signals and not issued_empty_canonical_replay)
        ):
            raise RiskBlock("INVALID_LEDGER_SIGNALS")
        identities = tuple(signal.signal_id for signal in copied_signals)
        if len(identities) != len(set(identities)):
            raise RiskBlock("DUPLICATE_SIGNAL_ID")
        if canonical is not None and not isinstance(canonical, CanonicalLedger):
            raise RiskBlock("INVALID_CANONICAL_LEDGER")
        if actual is not None and not isinstance(actual, ActualLedger):
            raise RiskBlock("INVALID_ACTUAL_LEDGER")
        object.__setattr__(self, "_signals", copied_signals)
        starting_canonical = CanonicalLedger(
            breaker_state=canonical.breaker_state if canonical is not None else None
        )
        starting_actual = ActualLedger(
            breaker_state=actual.breaker_state if actual is not None else None
        )
        projected_canonical = starting_canonical
        projected_actual = starting_actual
        for event in copied_events:
            projected_canonical, projected_actual = apply_ledger_event(
                copied_signals,
                projected_canonical,
                projected_actual,
                event,
                verified_event_batch=verified_event_batch,
            )
        projected_canonical = replace(
            projected_canonical,
            open_positions=_sort_projection_lots(
                projected_canonical.open_positions
            ),
        )
        projected_actual = replace(
            projected_actual,
            open_positions=_sort_projection_lots(projected_actual.open_positions),
        )
        if copied_events:
            if (
                canonical is not None
                and (canonical.open_positions or canonical.events_applied)
                and canonical != projected_canonical
            ):
                raise RiskBlock("CANONICAL_PROJECTION_MISMATCH")
            if (
                actual is not None
                and (
                    actual.open_positions
                    or actual.events_applied
                    or actual.reconciliation_required
                    or actual.reason_codes
                    or actual.stop_unverified
                )
                and actual != projected_actual
            ):
                raise RiskBlock("ACTUAL_PROJECTION_MISMATCH")
            resolved_canonical = projected_canonical
            resolved_actual = projected_actual
        else:
            if canonical is not None and (
                canonical.open_positions or canonical.events_applied
            ):
                raise RiskBlock("CANONICAL_PROJECTION_WITHOUT_EVENTS")
            if actual is not None and (
                actual.open_positions
                or actual.events_applied
                or actual.reconciliation_required
                or actual.reason_codes
                or actual.stop_unverified
            ):
                raise RiskBlock("ACTUAL_PROJECTION_WITHOUT_EVENTS")
            resolved_canonical = starting_canonical
            resolved_actual = starting_actual
        object.__setattr__(self, "_canonical", resolved_canonical)
        object.__setattr__(self, "_actual", resolved_actual)
        object.__setattr__(self, "_events", copied_events)
        canonical_events = tuple(
            event for event in copied_events if event.ledger_name == "CANONICAL"
        )
        actual_events = tuple(
            event for event in copied_events if event.ledger_name == "ACTUAL"
        )
        canonical_replay_verified = _replay_cohort_authorizes_stream(
            cohorts,
            "CANONICAL",
            canonical_events,
            copied_signals,
        )
        actual_replay_verified = _replay_cohort_authorizes_stream(
            cohorts,
            "ACTUAL",
            actual_events,
            copied_signals,
        )
        canonical_replay_cohort = (
            next(
                cohort
                for cohort in cohorts
                if cohort.ledger_name == "CANONICAL"
            )
            if canonical_replay_verified
            else None
        )
        actual_replay_cohort = (
            next(
                cohort for cohort in cohorts if cohort.ledger_name == "ACTUAL"
            )
            if actual_replay_verified
            else None
        )
        object.__setattr__(
            self,
            "_canonical_replay_cohort",
            canonical_replay_cohort,
        )
        object.__setattr__(
            self,
            "_actual_replay_cohort",
            actual_replay_cohort,
        )
        object.__setattr__(
            self,
            "_canonical_replay_verified",
            canonical_replay_verified,
        )
        object.__setattr__(
            self,
            "_actual_replay_verified",
            actual_replay_verified,
        )
        object.__setattr__(
            self,
            "_replay_verified",
            canonical_replay_verified and actual_replay_verified,
        )
        object.__setattr__(self, "_sealed", True)

    @property
    def signals(self) -> tuple[LedgerSignal, ...]:
        return self._signals

    @property
    def events(self) -> tuple[LedgerEvent, ...]:
        return self._events

    @property
    def canonical(self) -> CanonicalLedger:
        return self._canonical

    @property
    def actual(self) -> ActualLedger:
        return self._actual

    @property
    def replay_verified(self) -> bool:
        return self.canonical_replay_verified and self.actual_replay_verified

    @property
    def canonical_replay_verified(self) -> bool:
        cohort = self._canonical_replay_cohort
        if cohort is None:
            return False
        if not _phase1_bound_sources(cohort):
            return self._canonical_replay_verified
        return (
            self._canonical_replay_verified
            and _replay_cohort_authorizes_stream(
                (cohort,),
                "CANONICAL",
                tuple(
                    event
                    for event in self.events
                    if event.ledger_name == "CANONICAL"
                ),
                self.signals,
            )
        )

    @property
    def canonical_replay_cohort(self) -> VerifiedLedgerReplayCohort | None:
        return self._canonical_replay_cohort

    @property
    def actual_replay_verified(self) -> bool:
        cohort = self._actual_replay_cohort
        if cohort is None:
            return False
        if not _phase1_bound_sources(cohort):
            return self._actual_replay_verified
        return (
            self._actual_replay_verified
            and _replay_cohort_authorizes_stream(
                (cohort,),
                "ACTUAL",
                tuple(
                    event
                    for event in self.events
                    if event.ledger_name == "ACTUAL"
                ),
                self.signals,
            )
        )

    @property
    def actual_replay_cohort(self) -> VerifiedLedgerReplayCohort | None:
        return self._actual_replay_cohort

    @classmethod
    def rebuild(
        cls,
        signals: Sequence[LedgerSignal],
        events: Sequence[LedgerEvent],
        *,
        canonical_breaker_state: BreakerState | None = None,
        actual_breaker_state: BreakerState | None = None,
        verified_event_batch: VerifiedLedgerEventBatch | None = None,
        verified_replay_cohorts: Sequence[VerifiedLedgerReplayCohort] = (),
    ) -> LedgerPair:
        return cls(
            signals=signals,
            canonical=CanonicalLedger(breaker_state=canonical_breaker_state),
            actual=ActualLedger(breaker_state=actual_breaker_state),
            events=events,
            verified_event_batch=verified_event_batch,
            verified_replay_cohorts=verified_replay_cohorts,
        )

    @property
    def paired_breaker_state(self) -> PairedBreakerState | None:
        canonical = self.canonical.breaker_state
        actual = self.actual.breaker_state
        if canonical is None or actual is None or canonical.as_of is None:
            return None
        return combine_breaker_states(canonical, actual, as_of=canonical.as_of)

    @property
    def new_live_entries_paused(self) -> bool:
        paired = self.paired_breaker_state
        return (
            self.actual.reconciliation_required
            or self.actual.stop_unverified
            or paired is None
            or not is_issued_paired_breaker_state(paired)
            or paired.live_entries_paused
        )

    def _append(
        self,
        event: LedgerEvent,
        *,
        verified_event_batch: VerifiedLedgerEventBatch | None = None,
    ) -> None:
        normalized = _normalize_ledger_events((*self._events, event))
        if len(normalized) == len(self._events):
            return
        canonical, actual = apply_ledger_event(
            self.signals,
            self.canonical,
            self.actual,
            event,
            verified_event_batch=verified_event_batch,
        )
        object.__setattr__(self, "_canonical", canonical)
        object.__setattr__(self, "_actual", actual)
        object.__setattr__(self, "_events", normalized)
        if event.ledger_name == "CANONICAL":
            object.__setattr__(self, "_canonical_replay_cohort", None)
            object.__setattr__(
                self,
                "_canonical_replay_verified",
                False,
            )
        else:
            object.__setattr__(self, "_actual_replay_cohort", None)
            object.__setattr__(
                self,
                "_actual_replay_verified",
                False,
            )
        object.__setattr__(
            self,
            "_replay_verified",
            self._canonical_replay_verified and self._actual_replay_verified,
        )

    def record_canonical_fill(
        self,
        signal_id: str,
        price: Decimal,
        shares: int,
        at: datetime,
    ) -> None:
        signal = _signal_by_id(self.signals, signal_id)
        lot = LedgerLot(price=price, shares=shares, at=at)
        if signal.role != "PRIMARY":
            raise RiskBlock("CANONICAL_SHADOW_FILL_PROHIBITED")
        if lot.at.astimezone(_ET).date() != signal.publication_session:
            raise RiskBlock("ENTRY_SESSION_MISMATCH")
        if lot.price > signal.maximum_entry:
            raise RiskBlock("FILL_ABOVE_MAXIMUM_ENTRY")
        # Task 8 owns authoritative intraday paper-fill simulation.  Until then,
        # a lower caller price cannot make the validation projection optimistic.
        lot = replace(lot, price=signal.maximum_entry)
        if lot.shares != signal.planned_shares:
            raise RiskBlock("SHARE_QUANTITY_MISMATCH")
        if any(
            position.signal_id == signal.signal_id
            for position in self.canonical.open_positions
        ):
            raise RiskBlock("POSITION_ADDITIONS_PROHIBITED")
        if any(
            position.signal_id != signal.signal_id
            and position.symbol == signal.symbol
            for position in self.canonical.open_positions
        ):
            raise RiskBlock("DUPLICATE_TICKER_EXPOSURE")
        if len(self.canonical.open_positions) >= _MAX_POSITIONS:
            raise RiskBlock("POSITION_LIMIT_REACHED")
        entries = sum(
            position.lots[0].at.astimezone(_ET).date()
            == signal.publication_session
            for position in self.canonical.open_positions
        )
        if entries >= _MAX_ENTRIES_PER_SESSION:
            raise RiskBlock("SESSION_ENTRY_LIMIT_REACHED")
        decision = ComplianceDecision(
            "NONCOMPLIANT_RECONCILIATION_REQUIRED",
            False,
            True,
            ("PAPER_ENTRY_AUTHORITY_UNVERIFIED",),
        )
        event = LedgerEvent(
            ledger_name="CANONICAL",
            signal_id=signal_id,
            lot=lot,
            user_confirmed_stop=None,
            decision=decision,
            event_id=_ledger_event_id("CANONICAL", signal_id, lot.at, None),
            ordinal=sum(existing.lot.at == lot.at for existing in self.events),
            signal_digest=_ledger_signal_digest(signal),
            message_time=lot.at,
            received_at=lot.at,
        )
        projected, _ = apply_ledger_event(
            self.signals,
            self.canonical,
            self.actual,
            event,
        )
        projected_position = next(
            position
            for position in projected.open_positions
            if position.signal_id == signal_id
        )
        if projected_position.planned_risk > _MAX_POSITION_RISK:
            raise RiskBlock("POSITION_RISK_LIMIT_BREACHED")
        if projected.deployed_capital > _MAX_EXPOSURE:
            raise RiskBlock("LIVE_EXPOSURE_LIMIT_BREACHED")
        if projected.open_planned_risk > _MAX_COMBINED_RISK:
            raise RiskBlock("COMBINED_RISK_LIMIT_BREACHED")
        self._append(event)

    def record_authorized_canonical_fill(
        self,
        authority: PaperEntryAuthority,
    ) -> None:
        """Consume Task 8's complete trigger/quote authority without scalars."""
        if not is_issued_paper_entry_authority(authority):
            raise RiskBlock("PAPER_ENTRY_AUTHORITY_UNVERIFIED")
        signal = _signal_by_id(self.signals, authority.signal_id)
        if not is_issued_ledger_signal(signal) or (
            authority.signal_digest != _ledger_signal_digest(signal)
        ):
            raise RiskBlock("PUBLICATION_AUTHORITY_UNVERIFIED")
        if (
            signal.role != "PRIMARY"
            or authority.trigger_at.astimezone(_ET).date()
            != signal.publication_session
            or authority.quote_at.astimezone(_ET).date()
            != signal.publication_session
            or authority.trigger_price < signal.trigger_price
            or authority.ask > signal.maximum_entry
        ):
            raise RiskBlock("PAPER_ENTRY_EVIDENCE_MISMATCH")
        lot = LedgerLot(
            price=signal.maximum_entry,
            shares=signal.planned_shares,
            at=authority.quote_at,
        )
        event = LedgerEvent(
            ledger_name="CANONICAL",
            signal_id=signal.signal_id,
            lot=lot,
            user_confirmed_stop=None,
            decision=ComplianceDecision("COMPLIANT", True, False, ()),
            event_id=authority.canonical_event_id,
            cursor=authority.lifecycle_cursor,
            ordinal=authority.action_ordinal,
            authority_basis=authority.source_digest,
            signal_digest=_ledger_signal_digest(signal),
            message_time=authority.quote_received_at,
            received_at=authority.quote_received_at,
        )
        batch = _issue_paper_verified_ledger_event_batch(event, authority)
        self._append(event, verified_event_batch=batch)

    def _decision_and_event(
        self,
        *,
        signal: LedgerSignal,
        lot: LedgerLot,
        user_confirmed_stop: Decimal | None,
        authority_reasons: Sequence[str],
        cursor: int | None = None,
        event_id: str | None = None,
        authority_basis: str | None = None,
        message_time: datetime | None = None,
        received_at: datetime | None = None,
    ) -> tuple[ComplianceDecision, LedgerEvent]:
        reasons = list(authority_reasons)
        if not is_issued_ledger_signal(signal):
            reasons.extend(
                ("UNPLANNED_SIGNAL", "PUBLICATION_AUTHORITY_UNVERIFIED")
            )
        existing_same = next(
            (
                position
                for position in self.actual.open_positions
                if position.signal_id == signal.signal_id
            ),
            None,
        )
        if lot.price > signal.maximum_entry:
            reasons.append("FILL_ABOVE_MAXIMUM_ENTRY")
        if lot.price <= signal.recommended_stop:
            reasons.append("NON_POSITIVE_STOP_DISTANCE")
        if signal.role != "PRIMARY":
            reasons.append("SHADOW_FILL")
        if lot.at.astimezone(_ET).date() != signal.publication_session:
            reasons.append("ENTRY_SESSION_MISMATCH")
        if existing_same is None:
            if lot.shares != signal.planned_shares and (
                lot.parent_order_id is None or lot.shares > signal.planned_shares
            ):
                reasons.append("SHARE_QUANTITY_MISMATCH")
        else:
            same_parent_fill = (
                lot.parent_order_id is not None
                and all(
                    prior.parent_order_id == lot.parent_order_id
                    for prior in existing_same.lots
                )
            )
            if same_parent_fill:
                if existing_same.shares + lot.shares > signal.planned_shares:
                    reasons.append("SHARE_QUANTITY_MISMATCH")
            else:
                reasons.append("POSITION_ADDITIONS_PROHIBITED")
                if lot.price < existing_same.entry:
                    reasons.append("AVERAGING_DOWN_PROHIBITED")
        if any(
            position.signal_id != signal.signal_id
            and position.symbol == signal.symbol
            for position in self.actual.open_positions
        ):
            reasons.append("DUPLICATE_TICKER_EXPOSURE")
        reasons.extend(
            _actual_stop_reason_codes(signal, lot, user_confirmed_stop)
        )
        if self.actual.reconciliation_required:
            reasons.append("PRIOR_RECONCILIATION_REQUIRED")

        provisional = ComplianceDecision(
            "NONCOMPLIANT_RECONCILIATION_REQUIRED",
            False,
            True,
            tuple(dict.fromkeys((*reasons, "PROJECTION_PENDING"))),
        )
        event = LedgerEvent(
            ledger_name="ACTUAL",
            signal_id=signal.signal_id,
            lot=lot,
            user_confirmed_stop=user_confirmed_stop,
            decision=provisional,
            event_id=(
                event_id
                if event_id is not None
                else _ledger_event_id(
                    "ACTUAL",
                    signal.signal_id,
                    lot.at,
                    cursor,
                )
            ),
            cursor=cursor,
            ordinal=sum(existing.lot.at == lot.at for existing in self.events),
            authority_basis=authority_basis,
            signal_digest=_ledger_signal_digest(signal),
            message_time=(lot.at if message_time is None else message_time),
            received_at=(
                lot.at
                if received_at is None and message_time is None
                else message_time
                if received_at is None
                else received_at
            ),
        )
        _, projected = _apply_ledger_event_unchecked(
            self.signals,
            self.canonical,
            self.actual,
            event,
        )
        projected_position = next(
            position
            for position in projected.open_positions
            if position.signal_id == signal.signal_id
        )
        if projected_position.planned_risk > _MAX_POSITION_RISK:
            reasons.append("POSITION_RISK_LIMIT_BREACHED")
        if projected.deployed_capital > _MAX_EXPOSURE:
            reasons.append("LIVE_EXPOSURE_LIMIT_BREACHED")
        if projected.open_planned_risk > _MAX_COMBINED_RISK:
            reasons.append("COMBINED_RISK_LIMIT_BREACHED")
        if len(projected.open_positions) > _MAX_POSITIONS:
            reasons.append("POSITION_LIMIT_BREACHED")
        if existing_same is None:
            same_session_entries = sum(
                position.lots[0].at.astimezone(_ET).date()
                == lot.at.astimezone(_ET).date()
                for position in projected.open_positions
            )
            if same_session_entries > _MAX_ENTRIES_PER_SESSION:
                reasons.append("SESSION_ENTRY_LIMIT_BREACHED")
        reason_codes = tuple(dict.fromkeys(reasons))
        compliant = not reason_codes
        decision = ComplianceDecision(
            "COMPLIANT"
            if compliant
            else "NONCOMPLIANT_RECONCILIATION_REQUIRED",
            compliant,
            not compliant,
            reason_codes,
        )
        return decision, replace(event, decision=decision)

    def record_actual_buy(
        self,
        signal_id: str,
        price: Decimal,
        shares: int,
        at: datetime,
        *,
        parent_order_id: str | None = None,
    ) -> ComplianceDecision:
        """Record real exposure conservatively without inventing authority."""
        signal = _signal_by_id(self.signals, signal_id)
        lot = LedgerLot(
            price=price,
            shares=shares,
            at=at,
            parent_order_id=parent_order_id,
        )
        decision, event = self._decision_and_event(
            signal=signal,
            lot=lot,
            user_confirmed_stop=None,
            authority_reasons=(
                "AUTHORITY_CONTEXT_UNVERIFIED",
                "ACCOUNT_AUTHORITY_UNVERIFIED",
                "BREAKER_AUTHORITY_UNVERIFIED",
            ),
        )
        self._append(event)
        return decision


    def record_actual_buy_with_context(
        self,
        *,
        signal_id: str,
        price: Decimal,
        shares: int,
        at: datetime,
        context: ActualBuyContext,
        event_id: str | None = None,
    ) -> ComplianceDecision:
        """Validate bound account/spread/stop/breaker authority, then record."""
        if not isinstance(context, ActualBuyContext):
            raise TypeError("actual buy context must be an ActualBuyContext")
        if event_id is not None and (type(event_id) is not str or not event_id):
            raise RiskBlock("INVALID_LEDGER_EVENT_ID")
        signal = _signal_by_id(self.signals, signal_id)
        lot = LedgerLot(
            price=price,
            shares=shares,
            at=at,
            parent_order_id=(
                None
                if context.buy_action is None
                else context.buy_action.parent_order_id
            ),
        )
        source_fingerprint = (
            _actual_buy_source_fingerprint(
                signal_id=signal_id,
                lot=lot,
                context=context,
                event_id=event_id,
            )
            if event_id is not None
            else None
        )
        prior = next(
            (
                existing
                for existing in self.events
                if event_id is not None and existing.event_id == event_id
            ),
            None,
        )
        if prior is not None:
            if (
                prior.ledger_name == "ACTUAL"
                and prior.signal_id == signal_id
                and prior.lot == lot
                and prior.user_confirmed_stop == context.user_confirmed_stop
                and prior.authority_basis == source_fingerprint
            ):
                return prior.decision
            raise RiskBlock("LEDGER_EVENT_IDEMPOTENCY_CONFLICT")
        buy = (
            context.buy_action.execution_event
            if context.buy_action is not None
            else ExecutionEvent(
                kind="BUY",
                at=lot.at,
                price=lot.price,
                shares=lot.shares,
                cursor=context.event_window.through_cursor,
            )
        )
        check = evaluate_account_check_window(
            context.account_check,
            buy,
            context.event_window,
        )
        context_evidence_matches = (
            event_id is not None
            and context.authorized_signal_id == signal_id
            and context.authorized_symbol == signal.symbol
            and context.buy_event == buy
            and context.event_id == event_id
            and context.buy_action is not None
            and context.buy_action.event_id == event_id
            and context.buy_action.symbol == signal.symbol
            and context.buy_action.price == lot.price
            and context.buy_action.shares == lot.shares
            and context.buy_action.at == lot.at
            and context.buy_action.parent_order_id == lot.parent_order_id
            and context.buy_action.bid == context.bid
            and context.buy_action.ask == context.ask
            and context.buy_action.user_confirmed_stop
            == context.user_confirmed_stop
        )
        plan = context.plan_decision
        plan_request = None if plan is None else plan.request
        position_plan = None if plan is None else plan.plan
        plan_authorized = (
            plan is not None
            and is_issued_long_plan_decision(plan)
            and plan.eligible
            and plan.portfolio_authority is context.portfolio_authority
            and context.portfolio_authority is not None
            and plan_request is context.portfolio_authority.request
            and plan.authority_scope == "ACTUAL_ENTRY"
            and plan.as_of == context.portfolio_authority.as_of
            and plan_request is not None
            and position_plan is not None
            and _actual_projection_matches_portfolio_state(
                self.actual,
                context.portfolio_authority,
                plan_request.session_date,
                signal.signal_id,
                lot.parent_order_id,
            )
            and plan_request.symbol == signal.symbol
            and plan_request.session_date == signal.publication_session
            and (
                (
                    lot.parent_order_id is not None
                    and plan_request.entry == signal.maximum_entry
                    and lot.price <= plan_request.entry
                )
                or (
                    lot.parent_order_id is None
                    and plan_request.entry == lot.price
                )
            )
            and plan_request.stop == signal.recommended_stop
            and plan_request.tick_size == signal.tick_size
            and plan_request.published_target == signal.target
            and plan.target == signal.target
            and position_plan.quantity == signal.planned_shares
            and (
                (
                    context.buy_action.parent_order_id is not None
                    and context.buy_action.fill_group_planned_shares
                    == signal.planned_shares
                    and lot.parent_order_id
                    == context.buy_action.parent_order_id
                    and lot.shares <= signal.planned_shares
                )
                or (
                    context.buy_action.parent_order_id is None
                    and context.buy_action.fill_group_planned_shares is None
                    and lot.parent_order_id is None
                    and lot.shares == signal.planned_shares
                )
            )
        )
        context_authorized = (
            context_evidence_matches
            and plan_authorized
            and is_issued_actual_buy_context(context)
            and is_issued_journal_event_window(context.event_window)
            and is_issued_paired_breaker_state(context.breaker_state)
            and is_issued_confirmed_buy_action(context.buy_action)
            and is_issued_ledger_signal(signal)
            and context.portfolio_authority is not None
            and is_issued_portfolio_risk_authority(
                context.portfolio_authority
            )
            and context.portfolio_authority.scope == "ACTUAL_ENTRY"
            and context.portfolio_authority.ledger_name == "ACTUAL"
            and (
                context.portfolio_authority.as_of == lot.at
                or (
                    lot.parent_order_id is not None
                    and context.portfolio_authority.as_of < lot.at
                )
            )
            and context.portfolio_authority.portfolio_state.breaker_states
            == (context.breaker_state,)
            and context.portfolio_authority.portfolio_state.calendar_resolver
            is context.calendar_resolver
            and context.actual_breaker_refresh is not None
            and is_issued_actual_breaker_refresh_authority(
                context.actual_breaker_refresh
            )
            and context.actual_breaker_refresh.paired_breaker
            is context.breaker_state
            and context.actual_breaker_refresh.as_of == lot.at
            and context.actual_breaker_refresh.through_execution_cursor
            == context.buy_action.cursor
            and context.actual_breaker_refresh.calendar_digest
            == context.portfolio_authority.calendar_digest
            and context.portfolio_authority.breaker_refresh_through_execution_cursor
            <= context.actual_breaker_refresh.through_execution_cursor
            and context.portfolio_authority.breaker_refresh_through_close_cursor
            is not None
        )
        reasons = list(check.reason_codes)
        if not context_evidence_matches:
            reasons.insert(0, "ACTUAL_BUY_EVIDENCE_MISMATCH")
        if not context_authorized:
            reasons.insert(0, "AUTHORITY_CONTEXT_UNVERIFIED")
            if not plan_authorized:
                reasons.insert(0, "POSITION_PLAN_AUTHORITY_UNVERIFIED")
            if not (
                context.portfolio_authority is not None
                and is_issued_portfolio_risk_authority(
                    context.portfolio_authority
                )
                and context.portfolio_authority.scope == "ACTUAL_ENTRY"
                and (
                    context.portfolio_authority.as_of == lot.at
                    or (
                        lot.parent_order_id is not None
                        and context.portfolio_authority.as_of < lot.at
                    )
                )
                and context.buy_action is not None
                and context.actual_breaker_refresh is not None
                and is_issued_actual_breaker_refresh_authority(
                    context.actual_breaker_refresh
                )
                and context.actual_breaker_refresh.paired_breaker
                is context.breaker_state
                and context.actual_breaker_refresh.as_of == lot.at
                and context.actual_breaker_refresh.through_execution_cursor
                == context.buy_action.cursor
                and context.actual_breaker_refresh.calendar_digest
                == context.portfolio_authority.calendar_digest
                and context.portfolio_authority.breaker_refresh_through_execution_cursor
                <= context.actual_breaker_refresh.through_execution_cursor
                and context.portfolio_authority.breaker_refresh_through_close_cursor
                is not None
            ):
                reasons.insert(0, "ACTUAL_BREAKER_REFRESH_UNVERIFIED")
        entry_day = lot.at.astimezone(_ET).date()
        entry_clock = lot.at.astimezone(_ET).time().replace(tzinfo=None)
        if entry_clock <= time(9, 35):
            reasons.append("ENTRY_NOT_AFTER_0935")
        try:
            session = context.calendar_resolver.session(entry_day)
        except RiskBlock as error:
            reasons.append(error.reason_code)
        else:
            if not session.open_time <= entry_clock <= session.close_time:
                reasons.append("ENTRY_OUTSIDE_REGULAR_SESSION")
        try:
            required_as_of = context.calendar_resolver.previous_session(
                lot.at.astimezone(_ET).date()
            )
        except RiskBlock:
            reasons.append("BREAKER_AUTHORITY_UNVERIFIED")
        else:
            if context.breaker_state.as_of != required_as_of:
                reasons.append("BREAKER_AS_OF_MISMATCH")
        if breaker_pauses_entry(context.breaker_state, entry_day):
            reasons.append("ACTIVE_CIRCUIT_BREAKER")
        with localcontext() as decimal_context:
            decimal_context.prec = _precision(context.bid, context.ask)
            midpoint = (context.ask + context.bid) / _TWO
            spread = (context.ask - context.bid) / midpoint
        if spread > _MAX_SPREAD:
            reasons.append("CONFIRMED_SPREAD_TOO_WIDE")
        if not context.bid <= lot.price <= context.ask:
            reasons.append("FILL_OUTSIDE_CONFIRMED_MARKET")
        decision, event = self._decision_and_event(
            signal=signal,
            lot=lot,
            user_confirmed_stop=context.user_confirmed_stop,
            authority_reasons=reasons,
            cursor=context.event_window.through_cursor,
            event_id=event_id,
            authority_basis=source_fingerprint,
            message_time=(
                context.buy_action.message_time
                if context.buy_action is not None
                and context.buy_action.at == lot.at
                else lot.at
            ),
            received_at=(
                context.buy_action.received_at
                if context.buy_action is not None
                and context.buy_action.at == lot.at
                else lot.at
            ),
        )
        if context_authorized:
            verified_batch = _issue_live_verified_ledger_event_batch(
                event,
                context,
            )
            self._append(event, verified_event_batch=verified_batch)
        else:
            self._append(event)
        return decision


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase1CanonicalLedgerReplay:
    """Task 8 cash/exit truth plus the Task 6 open-lot compatibility bridge."""

    ledger_pair: LedgerPair
    cohort: VerifiedLedgerReplayCohort
    canonical_cash: Decimal
    settled_buying_power: Decimal
    realized_pnl: Decimal
    postings: tuple[object, ...]
    closed_trades: tuple[object, ...]
    projection_terminal_cursor: int | None
    lifecycle_source_terminal_cursor: int | None
    posting_source_terminal_cursor: int | None
    query_cutoff: datetime
    source_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_pair, LedgerPair) or not isinstance(
            self.cohort,
            VerifiedLedgerReplayCohort,
        ):
            raise RiskBlock("INVALID_PHASE1_CANONICAL_REPLAY")
        for name in ("canonical_cash", "settled_buying_power"):
            object.__setattr__(
                self,
                name,
                _money(
                    getattr(self, name),
                    "INVALID_PHASE1_CANONICAL_REPLAY",
                    nonnegative=True,
                ),
            )
        object.__setattr__(
            self,
            "realized_pnl",
            _money(self.realized_pnl, "INVALID_PHASE1_CANONICAL_REPLAY"),
        )
        object.__setattr__(self, "postings", tuple(self.postings))
        object.__setattr__(self, "closed_trades", tuple(self.closed_trades))
        for cursor in (
            self.projection_terminal_cursor,
            self.lifecycle_source_terminal_cursor,
            self.posting_source_terminal_cursor,
        ):
            if cursor is not None:
                _positive_int(cursor, "INVALID_PHASE1_CANONICAL_REPLAY")
        _aware(self.query_cutoff, "INVALID_PHASE1_CANONICAL_REPLAY")
        if (
            type(self.source_digest) is not str
            or len(self.source_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.source_digest)
        ):
            raise RiskBlock("INVALID_PHASE1_CANONICAL_REPLAY")

    @property
    def source_verified(self) -> bool:
        return bool(_phase1_bound_sources(self)) and _phase1_sources_are_current(
            self
        )


def _phase1_signal_source_coordinates(source: object) -> tuple[object, ...]:
    return (
        getattr(source, "row_id"),
        getattr(source, "row_sha256"),
        getattr(source, "signal_id"),
        getattr(source, "validation_window_id"),
        getattr(source, "source_digest"),
    )


def _construct_phase1_signal(
    source: object,
    *,
    binding_source: object,
    binding_kind: str,
) -> LedgerSignal:
    signal = LedgerSignal(
        signal_id=getattr(source, "signal_id"),
        symbol=getattr(source, "symbol"),
        role=getattr(source, "role"),
        publication_session=getattr(source, "publication_session"),
        maximum_entry=money_from_micros(getattr(source, "maximum_entry_micros")),
        recommended_stop=money_from_micros(
            getattr(source, "recommended_stop_micros")
        ),
        target=money_from_micros(getattr(source, "target_micros")),
        planned_shares=getattr(source, "planned_shares"),
        tick_size=money_from_micros(getattr(source, "tick_size_micros")),
        trigger_price=money_from_micros(getattr(source, "trigger_price_micros")),
    )
    _register_phase1_derived_authority(
        _ISSUED_LEDGER_SIGNALS,
        signal,
        _ledger_signal_fingerprint(signal),
    )
    _bind_phase1_sources(signal, ((binding_source, binding_kind),))
    return signal


def _issue_ledger_signal_from_phase1_source(source: object) -> LedgerSignal:
    """Reissue one signal only from an exact owner-current Task 8 row source."""
    from .journal import Phase1SignalSource, is_verified_phase1_signal_source

    if not isinstance(source, Phase1SignalSource) or not (
        is_verified_phase1_signal_source(source)
    ):
        raise RiskBlock("PHASE1_SIGNAL_SOURCE_UNVERIFIED")
    publication = source.publication_source
    if (
        source.received_at > source.query_cutoff
        or source.published_at > source.received_at
        or publication.received_at > source.query_cutoff
    ):
        raise RiskBlock("PHASE1_SOURCE_LOOKAHEAD")
    if (
        source.publication_report_row_id != publication.report_row_id
        or source.publication_report_id != publication.report_id
        or source.publication_source_digest != publication.source_digest
        or source.publication_state_digest != publication.state_sha256
        or source.publication_content_digest != publication.body_sha256
        or source.publication_observation_set_digest
        != publication.observation_set_sha256
        or source.publication_session != publication.session_date
        or source.published_at != publication.published_at
        or source.publication_rank not in {1, 2, 3}
        or (source.publication_rank == 1) != (source.role == "PRIMARY")
    ):
        raise RiskBlock("PHASE1_SIGNAL_SOURCE_MISMATCH")
    if source.role != "PRIMARY" or source.planned_shares <= 0:
        raise RiskBlock("PHASE1_SIGNAL_NOT_TRADABLE")
    return _construct_phase1_signal(
        source,
        binding_source=source,
        binding_kind="SIGNAL",
    )


def _signal_matches_phase1_source(signal: LedgerSignal, source: object) -> bool:
    from .journal import phase1_sources_share_owner

    for bound_source, kind in _phase1_bound_sources(signal):
        if kind != "SIGNAL":
            continue
        try:
            same_row = _phase1_signal_source_coordinates(
                bound_source
            ) == _phase1_signal_source_coordinates(source)
        except Exception:
            return False
        return same_row and phase1_sources_share_owner(bound_source, source)
    return False


def _phase1_entry_result_from_observations(
    observations: Sequence[object],
    *,
    trigger: Decimal,
    limit: Decimal,
) -> PaperEntryResult:
    """Recompute the first canonical trigger/fill from exact cohort order."""

    def optional_money(value: object) -> Decimal | None:
        if value is None:
            return None
        if type(value) is not int:
            raise RiskBlock("PHASE1_ENTRY_OBSERVATION_MISMATCH")
        try:
            return money_from_micros(value)
        except DomainValidationError:
            raise RiskBlock("PHASE1_ENTRY_OBSERVATION_MISMATCH") from None

    normalized: list[IntradayObservation] = []
    for source in tuple(observations):
        kind_value = getattr(source, "observation_kind", None)
        try:
            kind = ObservationKind(kind_value)
        except (TypeError, ValueError):
            raise RiskBlock("PHASE1_ENTRY_OBSERVATION_KIND_MISMATCH") from None
        observed_at = getattr(source, "source_time", None)
        received_at = getattr(source, "received_at", None)
        sequence = getattr(source, "cohort_ordinal", None)
        fresh = getattr(source, "fresh", None)
        if (
            not isinstance(observed_at, datetime)
            or not isinstance(received_at, datetime)
            or type(sequence) is not int
            or type(fresh) is not bool
        ):
            raise RiskBlock("PHASE1_ENTRY_OBSERVATION_MISMATCH")
        normalized.append(
            IntradayObservation(
                observation_id=getattr(source, "observation_id", None),
                stream_id=getattr(source, "stream_id", None),
                feed=getattr(source, "feed", None),
                kind=kind,
                at=observed_at,
                received_at=received_at,
                sequence=sequence,
                fresh=fresh,
                trade_price=optional_money(
                    getattr(source, "trade_price_micros", None)
                ),
                bid=optional_money(getattr(source, "bid_micros", None)),
                ask=optional_money(getattr(source, "ask_micros", None)),
                open_price=optional_money(
                    getattr(source, "open_micros", None)
                ),
                high=optional_money(getattr(source, "high_micros", None)),
                low=optional_money(getattr(source, "low_micros", None)),
                close_price=optional_money(
                    getattr(source, "close_micros", None)
                ),
                session_open=(
                    kind is ObservationKind.BAR
                    and observed_at.astimezone(_ET).time().replace(tzinfo=None)
                    == time(9, 30)
                ),
            )
        )
    return simulate_entry(trigger, limit, tuple(normalized))


def _paper_entry_from_source_material(
    source: object,
    *,
    signal: LedgerSignal,
    calendar_digest: str,
    binding_source: object,
    binding_kind: str,
) -> PaperEntryAuthority:
    observations = tuple(getattr(source, "observations"))
    completion = getattr(source, "completion")
    lifecycle = getattr(source, "lifecycle_event")
    posting = getattr(source, "buy_posting")
    expected_count = getattr(source, "expected_observation_count")
    query_cutoff = getattr(source, "query_cutoff")
    if (
        expected_count != len(observations)
        or completion.expected_observation_count != expected_count
        or completion.cohort_through_ordinal
        != (observations[-1].cohort_ordinal if observations else 0)
        or completion.completed_at > query_cutoff
        or any(observation.received_at > completion.received_through for observation in observations)
        or tuple(observation.cohort_ordinal for observation in observations)
        != tuple(sorted(observation.cohort_ordinal for observation in observations))
        or len({observation.observation_id for observation in observations})
        != len(observations)
        or any(observation.signal_id != signal.signal_id for observation in observations)
        or completion.signal_id != signal.signal_id
        or completion.session_date != signal.publication_session
        or lifecycle.signal_id != signal.signal_id
        or posting.signal_id != signal.signal_id
        or tuple(
            sorted(
                (
                    stream_id,
                    max(
                        observation.source_cursor
                        for observation in observations
                        if observation.stream_id == stream_id
                    ),
                )
                for stream_id in {observation.stream_id for observation in observations}
            )
        )
        != tuple(sorted(getattr(source, "observation_stream_highwaters")))
    ):
        raise RiskBlock("PHASE1_OBSERVATION_COHORT_INCOMPLETE")
    by_id = {observation.observation_id: observation for observation in observations}
    trigger = by_id.get(lifecycle.trigger_observation_id)
    quote = by_id.get(lifecycle.quote_observation_id)
    simulated = _phase1_entry_result_from_observations(
        observations,
        trigger=signal.trigger_price,
        limit=signal.maximum_entry,
    )
    entry_status_by_event = {
        "PAPER_FILL": "TRIGGERED_PAPER",
        "LIVE_CONFIRM": "LIVE_CONFIRMED",
        "LIVE_SKIP": "SKIPPED_LIVE_TRACKED_PAPER",
    }
    if (
        trigger is None
        or quote is None
        or trigger.observation_kind != "TRADE"
        or quote.observation_kind != "QUOTE"
        or not trigger.fresh
        or not quote.fresh
        or trigger.trade_price_micros is None
        or quote.bid_micros is None
        or quote.ask_micros is None
        or quote.ask_micros < quote.bid_micros
        or quote.cohort_ordinal <= trigger.cohort_ordinal
        or quote.source_time < trigger.source_time
        or simulated.status is not SignalStatus.TRIGGERED_PAPER
        or simulated.fill_price != signal.maximum_entry
        or simulated.trigger_observation_id != trigger.observation_id
        or simulated.quote_observation_id != quote.observation_id
        or simulated.trigger_at != trigger.source_time
        or simulated.filled_at != quote.source_time
        or lifecycle.event_kind not in entry_status_by_event
        or lifecycle.from_status != "TRIGGERED_AWAITING_LIMIT"
        or lifecycle.to_status
        != entry_status_by_event.get(lifecycle.event_kind)
        or lifecycle.signal_id != signal.signal_id
        or lifecycle.shares != signal.planned_shares
        or lifecycle.price_micros != money_to_micros(signal.maximum_entry)
        or lifecycle.received_at > query_cutoff
        or posting.entry_kind != "BUY"
        or posting.signal_id != signal.signal_id
        or posting.lifecycle_event_id != lifecycle.lifecycle_event_id
        or posting.shares_delta != signal.planned_shares
        or posting.unit_price_micros != money_to_micros(signal.maximum_entry)
        or posting.amount_micros
        != -(money_to_micros(signal.maximum_entry) * signal.planned_shares)
        or posting.received_at > query_cutoff
        or money_from_micros(trigger.trade_price_micros) < signal.trigger_price
        or money_from_micros(quote.ask_micros) > signal.maximum_entry
    ):
        raise RiskBlock("PHASE1_ENTRY_SOURCE_MISMATCH")
    authority = PaperEntryAuthority(
        signal_id=signal.signal_id,
        signal_digest=_ledger_signal_digest(signal),
        lifecycle_event_id=lifecycle.lifecycle_event_id,
        trigger_observation_id=trigger.observation_id,
        trigger_stream_id=trigger.stream_id,
        trigger_feed=trigger.feed,
        trigger_at=trigger.source_time,
        trigger_received_at=trigger.received_at,
        trigger_sequence=trigger.provider_sequence,
        trigger_source_cursor=trigger.source_cursor,
        trigger_source_ordinal=trigger.source_ordinal,
        trigger_stream_through_cursor=trigger.stream_through_cursor,
        trigger_cohort_ordinal=trigger.cohort_ordinal,
        trigger_price=money_from_micros(trigger.trade_price_micros),
        quote_observation_id=quote.observation_id,
        quote_stream_id=quote.stream_id,
        quote_feed=quote.feed,
        quote_at=quote.source_time,
        quote_received_at=quote.received_at,
        quote_sequence=quote.provider_sequence,
        quote_source_cursor=quote.source_cursor,
        quote_source_ordinal=quote.source_ordinal,
        quote_stream_through_cursor=quote.stream_through_cursor,
        quote_cohort_ordinal=quote.cohort_ordinal,
        bid=money_from_micros(quote.bid_micros),
        ask=money_from_micros(quote.ask_micros),
        source_digest=getattr(source, "source_digest"),
        session_complete_digest=completion.source_digest,
        cohort_through_ordinal=completion.cohort_through_ordinal,
        cohort_received_through=completion.received_through,
        canonical_event_id=lifecycle.lifecycle_event_id,
        lifecycle_cursor=lifecycle.row_id,
        action_ordinal=lifecycle.event_ordinal,
        calendar_digest=calendar_digest,
    )
    _register_phase1_derived_authority(
        _PAPER_ENTRY_AUTHORITIES,
        authority,
        _paper_entry_fingerprint(authority),
    )
    _bind_phase1_sources(authority, ((binding_source, binding_kind),))
    return authority


def _issue_paper_entry_authority_from_phase1_source(
    source: object,
    *,
    signal: LedgerSignal,
    calendar_resolver: SessionCalendarResolver,
) -> PaperEntryAuthority:
    from .journal import Phase1EntrySource, is_verified_phase1_entry_source
    from .risk import _calendar_digest

    if not isinstance(source, Phase1EntrySource) or not (
        is_verified_phase1_entry_source(source)
    ):
        raise RiskBlock("PHASE1_ENTRY_SOURCE_UNVERIFIED")
    if not is_issued_ledger_signal(signal) or not _signal_matches_phase1_source(
        signal,
        source.signal_source,
    ):
        raise RiskBlock("PHASE1_SIGNAL_SOURCE_MISMATCH")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    calendar_digest = _calendar_digest(calendar_resolver)
    if source.calendar_digest != calendar_digest:
        raise RiskBlock("PHASE1_CALENDAR_SOURCE_MISMATCH")
    return _paper_entry_from_source_material(
        source,
        signal=signal,
        calendar_digest=calendar_digest,
        binding_source=source,
        binding_kind="ENTRY",
    )


def _issue_shadow_fill_disposition_from_phase1_source(
    source: object,
    *,
    calendar_resolver: SessionCalendarResolver,
) -> ShadowFillDispositionAuthority:
    """Recompute one price/time-only shadow disposition from a sealed cohort."""
    from .journal import (
        Phase1ShadowFillSource,
        is_verified_phase1_shadow_fill_source,
    )
    from .risk import _calendar_digest

    if not isinstance(source, Phase1ShadowFillSource) or not (
        is_verified_phase1_shadow_fill_source(source)
    ):
        raise RiskBlock("PHASE1_SHADOW_FILL_SOURCE_UNVERIFIED")
    if not isinstance(calendar_resolver, SessionCalendarResolver):
        raise RiskBlock("INVALID_CALENDAR_RESOLVER")
    if not calendar_resolver.release_verified:
        raise RiskBlock("CALENDAR_RELEASE_AUTHORITY_UNVERIFIED")
    calendar_digest = _calendar_digest(calendar_resolver)
    signal = source.signal_source
    observations = tuple(source.observations)
    completion = source.completion
    trigger_event = source.trigger_event
    lifecycle = source.lifecycle_event
    if (
        signal.role != "WATCHLIST_SHADOW"
        or signal.planned_shares != 0
        or source.calendar_digest != calendar_digest
        or source.query_cutoff < completion.completed_at
        or source.expected_observation_count != len(observations)
        or completion.expected_observation_count != len(observations)
        or completion.signal_id != signal.signal_id
        or completion.session_date != signal.publication_session
        or completion.cohort_through_ordinal
        != (observations[-1].cohort_ordinal if observations else 0)
        or source.observation_source_highwater
        != max((item.source_cursor for item in observations), default=0)
        or tuple(item.cohort_ordinal for item in observations)
        != tuple(sorted(item.cohort_ordinal for item in observations))
        or len({item.observation_id for item in observations})
        != len(observations)
        or any(item.signal_id != signal.signal_id for item in observations)
        or any(item.received_at > completion.received_through for item in observations)
        or completion.received_through > completion.completed_at
        or completion.completed_at > source.query_cutoff
        or tuple(
            sorted(
                (
                    stream_id,
                    max(
                        item.source_cursor
                        for item in observations
                        if item.stream_id == stream_id
                    ),
                )
                for stream_id in {item.stream_id for item in observations}
            )
        )
        != tuple(sorted(source.observation_stream_highwaters))
    ):
        raise RiskBlock("PHASE1_SHADOW_FILL_COHORT_INCOMPLETE")
    by_id = {item.observation_id: item for item in observations}
    trigger = by_id.get(lifecycle.trigger_observation_id)
    quote = by_id.get(lifecycle.quote_observation_id)
    simulated = _phase1_entry_result_from_observations(
        observations,
        trigger=money_from_micros(signal.trigger_price_micros),
        limit=money_from_micros(signal.maximum_entry_micros),
    )
    if (
        trigger is None
        or quote is None
        or trigger.observation_kind != "TRADE"
        or quote.observation_kind != "QUOTE"
        or not trigger.fresh
        or not quote.fresh
        or trigger.trade_price_micros is None
        or quote.bid_micros is None
        or quote.ask_micros is None
        or quote.ask_micros < quote.bid_micros
        or quote.cohort_ordinal <= trigger.cohort_ordinal
        or quote.source_time <= trigger.source_time
        or simulated.status is not SignalStatus.TRIGGERED_PAPER
        or simulated.fill_price
        != money_from_micros(signal.maximum_entry_micros)
        or simulated.trigger_observation_id != trigger.observation_id
        or simulated.quote_observation_id != quote.observation_id
        or simulated.trigger_at != trigger.source_time
        or simulated.filled_at != quote.source_time
        or trigger_event.signal_id != signal.signal_id
        or trigger_event.event_kind != "TRIGGER_OBSERVED"
        or trigger_event.from_status != "PUBLISHED"
        or trigger_event.to_status != "TRIGGERED_AWAITING_LIMIT"
        or trigger_event.trigger_observation_id != trigger.observation_id
        or trigger_event.quote_observation_id is not None
        or trigger_event.event_time != trigger.source_time
        or lifecycle.signal_id != signal.signal_id
        or lifecycle.event_kind != "SHADOW_FILL"
        or lifecycle.from_status != "TRIGGERED_AWAITING_LIMIT"
        or lifecycle.to_status != "SHADOW_FILLED_INFORMATIONAL"
        or lifecycle.trigger_observation_id != trigger.observation_id
        or lifecycle.quote_observation_id != quote.observation_id
        or lifecycle.event_time != quote.source_time
        or lifecycle.price_micros != signal.maximum_entry_micros
        or lifecycle.shares is not None
        or lifecycle.recommended_stop_micros is not None
        or lifecycle.event_ordinal != trigger_event.event_ordinal + 1
        or trigger_event.received_at > source.query_cutoff
        or lifecycle.received_at > source.query_cutoff
    ):
        raise RiskBlock("PHASE1_SHADOW_FILL_SOURCE_MISMATCH")
    authority = ShadowFillDispositionAuthority(
        signal_id=signal.signal_id,
        lifecycle_event_id=lifecycle.lifecycle_event_id,
        trigger_observation_id=trigger.observation_id,
        quote_observation_id=quote.observation_id,
        trigger_at=trigger.source_time,
        filled_at=quote.source_time,
        fill_price=money_from_micros(signal.maximum_entry_micros),
        source_digest=source.source_digest,
        session_complete_digest=completion.source_digest,
        calendar_digest=calendar_digest,
        lifecycle_cursor=lifecycle.row_id,
        action_ordinal=lifecycle.event_ordinal,
    )
    _register_phase1_derived_authority(
        _SHADOW_FILL_DISPOSITION_AUTHORITIES,
        authority,
        _shadow_fill_disposition_fingerprint(authority),
    )
    _bind_phase1_sources(authority, ((source, "SHADOW_FILL"),))
    return authority


def _canonical_event_from_paper_authority(
    signal: LedgerSignal,
    authority: PaperEntryAuthority,
    *,
    remaining_shares: int | None = None,
) -> LedgerEvent:
    shares = signal.planned_shares if remaining_shares is None else remaining_shares
    _positive_int(shares, "INVALID_POSITION_SHARES")
    if shares > signal.planned_shares:
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
    return LedgerEvent(
        ledger_name="CANONICAL",
        signal_id=signal.signal_id,
        lot=LedgerLot(
            price=signal.maximum_entry,
            shares=shares,
            at=authority.quote_at,
        ),
        user_confirmed_stop=None,
        decision=ComplianceDecision("COMPLIANT", True, False, ()),
        event_id=(
            authority.canonical_event_id
            if shares == signal.planned_shares
            else f"{authority.canonical_event_id}:remaining:{shares}"
        ),
        cursor=authority.lifecycle_cursor,
        ordinal=authority.action_ordinal,
        authority_basis=authority.source_digest,
        signal_digest=_ledger_signal_digest(signal),
        message_time=authority.quote_received_at,
        received_at=authority.quote_received_at,
    )


def _phase1_open_position_state(
    signal: LedgerSignal,
    lifecycle_events: Sequence[object],
    *,
    remaining_shares: int,
) -> tuple[Decimal, bool]:
    """Fold the one allowed partial exit into the compatibility open lot."""
    if not isinstance(signal, LedgerSignal):
        raise TypeError("signal must be a LedgerSignal")
    if (
        type(remaining_shares) is not int
        or remaining_shares <= 0
        or remaining_shares > signal.planned_shares
        or isinstance(lifecycle_events, (str, bytes))
    ):
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
    partials = tuple(
        event
        for event in tuple(lifecycle_events)
        if getattr(event, "signal_id", None) == signal.signal_id
        and getattr(event, "event_kind", None) == "PARTIAL_EXIT"
    )
    exited_shares = signal.planned_shares - remaining_shares
    if exited_shares == 0:
        if partials:
            raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
        return signal.recommended_stop, False
    if len(partials) != 1:
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
    partial = partials[0]
    shares = getattr(partial, "shares", None)
    price_micros = getattr(partial, "price_micros", None)
    stop_micros = getattr(partial, "recommended_stop_micros", None)
    if (
        type(getattr(partial, "row_id", None)) is not int
        or partial.row_id <= 0
        or type(shares) is not int
        or shares != exited_shares
        or type(price_micros) is not int
        or price_micros <= 0
        or type(stop_micros) is not int
        or stop_micros <= 0
    ):
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
    try:
        exit_price = money_from_micros(price_micros)
        recommended_stop = money_from_micros(stop_micros)
    except DomainValidationError:
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH") from None
    if (
        recommended_stop <= signal.recommended_stop
        or recommended_stop >= exit_price
        or not _tick_aligned(recommended_stop, signal.tick_size)
    ):
        raise RiskBlock("PHASE1_PARTIAL_EXIT_STATE_MISMATCH")
    return recommended_stop, True


def _issue_canonical_ledger_replay_from_phase1_source(
    source: object,
) -> Phase1CanonicalLedgerReplay:
    """Issue Task 6 compatibility state while retaining complete Task 8 truth."""
    from .journal import (
        Phase1CanonicalReplaySource,
        is_verified_phase1_canonical_replay_source,
    )
    from .risk import ClosedTrade

    if not isinstance(source, Phase1CanonicalReplaySource) or not (
        is_verified_phase1_canonical_replay_source(source)
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_UNVERIFIED")
    lifecycle_terminal = (
        source.lifecycle_events[-1].row_id if source.lifecycle_events else None
    )
    posting_terminal = source.postings[-1].row_id if source.postings else None
    source_signal_ids = tuple(item.signal_id for item in source.signal_sources)
    primary_signal_sources = tuple(
        item for item in source.signal_sources if item.role == "PRIMARY"
    )
    shadow_signal_ids = {
        item.signal_id
        for item in source.signal_sources
        if item.role == "WATCHLIST_SHADOW"
    }
    entry_signal_ids = tuple(
        item.signal_source.signal_id for item in source.entry_sources
    )
    if (
        source.expected_lifecycle_count != len(source.lifecycle_events)
        or source.expected_posting_count != len(source.postings)
        or source.expected_closed_trade_count != len(source.closed_trades)
        or source.lifecycle_terminal_cursor != lifecycle_terminal
        or source.posting_terminal_cursor != posting_terminal
        or (
            lifecycle_terminal is not None
            and (
                source.lifecycle_source_highwater is None
                or source.lifecycle_source_highwater < lifecycle_terminal
            )
        )
        or (
            posting_terminal is not None
            and (
                source.posting_source_highwater is None
                or source.posting_source_highwater < posting_terminal
            )
        )
        or len(source_signal_ids) != len(set(source_signal_ids))
        or any(
            (item.role == "PRIMARY" and item.planned_shares <= 0)
            or (item.role == "WATCHLIST_SHADOW" and item.planned_shares != 0)
            or item.role not in {"PRIMARY", "WATCHLIST_SHADOW"}
            for item in source.signal_sources
        )
        or len(entry_signal_ids) != len(set(entry_signal_ids))
        or any(signal_id in shadow_signal_ids for signal_id in entry_signal_ids)
        or any(
            item.validation_window_id != source.validation_window_id
            or item.query_cutoff > source.query_cutoff
            for item in source.signal_sources
        )
        or any(item.query_cutoff > source.query_cutoff for item in source.entry_sources)
        or any(
            item.signal_id not in source_signal_ids
            for item in (
                *source.lifecycle_events,
                *source.postings,
                *source.closed_trades,
            )
        )
        or any(posting.signal_id in shadow_signal_ids for posting in source.postings)
        or any(trade.signal_id in shadow_signal_ids for trade in source.closed_trades)
        or any(
            event.signal_id in shadow_signal_ids
            and event.event_kind
            in {"PAPER_FILL", "LIVE_CONFIRM", "LIVE_SKIP", "CLOSE"}
            for event in source.lifecycle_events
        )
        or any(
            item.validation_window_id != source.validation_window_id
            for item in source.closed_trades
        )
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
    if any(
        getattr(item, "received_at", source.query_cutoff) > source.query_cutoff
        for item in (*source.lifecycle_events, *source.postings, *source.closed_trades)
    ):
        raise RiskBlock("PHASE1_SOURCE_LOOKAHEAD")
    recomputed_cash_micros = source.starting_capital_micros + sum(
        posting.amount_micros for posting in source.postings
    )
    cutoff_session = source.query_cutoff.astimezone(_ET).date()
    recomputed_settled_micros = source.starting_capital_micros + sum(
        posting.amount_micros
        for posting in source.postings
        if posting.settlement_available_session <= cutoff_session
    )
    if (
        recomputed_cash_micros != source.canonical_cash_micros
        or recomputed_settled_micros != source.settled_buying_power_micros
        or sum(trade.pnl_micros for trade in source.closed_trades)
        != source.realized_pnl_micros
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_ARITHMETIC_MISMATCH")
    signals = tuple(
        _construct_phase1_signal(
            signal_source,
            binding_source=source,
            binding_kind="CANONICAL_REPLAY",
        )
        for signal_source in primary_signal_sources
    )
    by_signal = {signal.signal_id: signal for signal in signals}
    if len(by_signal) != len(signals):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
    remaining_shares: dict[str, int] = {signal.signal_id: 0 for signal in signals}
    for posting in source.postings:
        if posting.entry_kind in {"BUY", "SALE"}:
            if posting.signal_id not in remaining_shares or posting.shares_delta is None:
                raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
            remaining_shares[posting.signal_id] += posting.shares_delta
    if any(
        remaining < 0 or remaining > by_signal[signal_id].planned_shares
        for signal_id, remaining in remaining_shares.items()
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
    buy_signal_ids = tuple(
        posting.signal_id
        for posting in source.postings
        if posting.entry_kind == "BUY"
    )
    if (
        len(buy_signal_ids) != len(set(buy_signal_ids))
        or set(buy_signal_ids) != set(entry_signal_ids)
    ):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
    events: list[LedgerEvent] = []
    for entry_source in source.entry_sources:
        signal = by_signal.get(entry_source.signal_source.signal_id)
        if signal is None:
            raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_MISMATCH")
        remaining = remaining_shares.get(signal.signal_id, 0)
        if remaining == 0:
            continue
        recommended_stop, profit_target_taken = _phase1_open_position_state(
            signal,
            source.lifecycle_events,
            remaining_shares=remaining,
        )
        authority = _paper_entry_from_source_material(
            entry_source,
            signal=signal,
            calendar_digest=entry_source.calendar_digest,
            binding_source=source,
            binding_kind="CANONICAL_REPLAY",
        )
        event = _canonical_event_from_paper_authority(
            signal,
            authority,
            remaining_shares=remaining,
        )
        if profit_target_taken:
            event = replace(
                event,
                recommended_stop=recommended_stop,
                profit_target_taken=True,
            )
        _bind_phase1_sources(event, ((source, "CANONICAL_REPLAY"),))
        events.append(event)
    ordered_events = tuple(sorted(events, key=_ledger_projection_order_key))
    batch = VerifiedLedgerEventBatch(
        tuple(
            (event.event_id, _ledger_event_content_digest(event))
            for event in ordered_events
        )
    )
    _register_verified_batch(batch)
    references = tuple(
        (
            event.event_id,
            _ledger_event_content_digest(event),
            event.signal_digest,
        )
        for event in ordered_events
        if event.signal_digest is not None
    )
    cursors = tuple(event.cursor for event in ordered_events if event.cursor is not None)
    cohort = VerifiedLedgerReplayCohort(
        ledger_name="CANONICAL",
        references=references,
        expected_count=len(ordered_events),
        start_cursor=(cursors[0] if cursors else None),
        terminal_cursor=(cursors[-1] if cursors else None),
        query_cutoff=source.query_cutoff,
        source_digest=source.source_digest,
    )
    _register_phase1_derived_authority(
        _VERIFIED_REPLAY_COHORT_AUTHORITIES,
        cohort,
        _replay_cohort_fingerprint(cohort),
    )
    _bind_phase1_sources(cohort, ((source, "CANONICAL_REPLAY"),))
    pair = LedgerPair(
        signals=signals,
        events=ordered_events,
        verified_event_batch=batch,
        verified_replay_cohorts=(cohort,),
    )
    closed_trades = tuple(
        ClosedTrade(
            session_date=trade.session_date,
            pnl=money_from_micros(trade.pnl_micros),
            signal_id=trade.signal_id,
            at=trade.at,
            cursor=trade.row_id,
            source_id=trade.trade_id,
            message_time=trade.message_time,
            received_at=trade.received_at,
        )
        for trade in source.closed_trades
    )
    replay = Phase1CanonicalLedgerReplay(
        ledger_pair=pair,
        cohort=cohort,
        canonical_cash=money_from_micros(source.canonical_cash_micros),
        settled_buying_power=money_from_micros(
            source.settled_buying_power_micros
        ),
        realized_pnl=money_from_micros(source.realized_pnl_micros),
        postings=source.postings,
        closed_trades=closed_trades,
        projection_terminal_cursor=(cursors[-1] if cursors else None),
        lifecycle_source_terminal_cursor=source.lifecycle_terminal_cursor,
        posting_source_terminal_cursor=source.posting_terminal_cursor,
        query_cutoff=source.query_cutoff,
        source_digest=source.source_digest,
    )
    _bind_phase1_sources(replay, ((source, "CANONICAL_REPLAY"),))
    return replay


__all__ = [
    "ActualBuyContext",
    "ActualLedger",
    "CanonicalLedger",
    "ComplianceDecision",
    "LedgerEvent",
    "LedgerLot",
    "LedgerPair",
    "LedgerPosition",
    "LedgerSignal",
    "PaperEntryAuthority",
    "Phase1CanonicalLedgerReplay",
    "ShadowFillDispositionAuthority",
    "VerifiedLedgerEventBatch",
    "VerifiedLedgerReplayCohort",
    "apply_ledger_event",
    "is_issued_paper_entry_authority",
    "is_issued_shadow_fill_disposition_authority",
    "is_issued_verified_replay_cohort",
]
