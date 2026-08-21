"""Disjoint in-memory canonical and actual ledger projections.

Events are the rebuild authority.  The frozen ledger snapshots are disposable
projections; durable Journal adapters are intentionally deferred to later tasks.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal, ROUND_CEILING, localcontext
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
    ClosedTrade,
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
from . import journal as _journal_authority_module
from . import reconciliation as _reconciliation_authority_module


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


@dataclass(frozen=True, slots=True)
class _Phase1DerivedAuthorityRecord:
    """One inseparable seal, provenance manifest, and child manifest."""

    seal: object
    bindings: tuple[tuple[object, str], ...]
    children: tuple[object, ...]
    semantic_verifier: Callable[..., bool]


_VERIFIED_LEDGER_BATCH_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        object,
        tuple[LedgerEvent, ...],
        Callable[..., bool],
    ],
] = {}
_VERIFIED_REPLAY_COHORT_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
] = {}
_ACTUAL_PROJECTION_COHORT_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        object,
        ReferenceType[object],
        object,
        tuple[object, ...],
        object,
        object,
        Callable[..., bool],
    ],
] = {}
_ISSUED_LEDGER_SIGNALS: dict[
    int,
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
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
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
] = {}
_SHADOW_FILL_DISPOSITION_AUTHORITIES: dict[
    int,
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
] = {}
_PHASE1_SOURCE_BINDINGS: dict[
    int,
    tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
] = {}
_PHASE1_CANONICAL_LEDGER_REPLAY_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        object,
        ReferenceType[object],
        tuple[object, ...],
        object,
        Callable[..., object],
    ],
] = {}
_PHASE1_CANONICAL_LEDGER_REPLAY_FINGERPRINT_DOMAIN = (
    b"stock-monitor/phase1-canonical-ledger-replay/v1"
)
_LEDGER_SIGNAL_FINGERPRINT_DOMAIN = (
    b"stock-monitor/ledger-signal-authority/v1"
)
_PAPER_ENTRY_FINGERPRINT_DOMAIN = b"stock-monitor/paper-entry-authority/v1"
_SHADOW_FILL_FINGERPRINT_DOMAIN = b"stock-monitor/shadow-fill-authority/v1"
_ACTUAL_PROJECTION_FINGERPRINT_DOMAIN = (
    b"stock-monitor/actual-projection-cohort-authority/v1"
)
_VERIFIED_REPLAY_COHORT_FINGERPRINT_DOMAIN = (
    b"stock-monitor/verified-ledger-replay-cohort-authority/v1"
)
_VERIFIED_LEDGER_BATCH_FINGERPRINT_DOMAIN = (
    b"stock-monitor/verified-ledger-event-batch-authority/v1"
)


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
        if cls is not LedgerSignal:
            raise RiskBlock("PUBLICATION_SIGNAL_ISSUER_UNVERIFIED")
        from .screening import (
            PublicationDecision,
            is_issued_publication_decision,
        )

        if type(decision) is not PublicationDecision or not (
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
        _register_phase1_derived_authority(
            _ISSUED_LEDGER_SIGNALS,
            signal,
            sources=(),
            children=(decision, plan_decision),
        )
        return signal


def _ledger_signal_fingerprint(signal: LedgerSignal) -> object:
    if type(signal) is not LedgerSignal:
        raise TypeError("ledger signal authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        signal,
        domain=_LEDGER_SIGNAL_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _publication_signal_children_are_current(
    children: tuple[object, ...],
) -> bool:
    """Verify the exact publication decision/plan pair before final sealing."""
    if type(children) is not tuple or len(children) != 2:
        return False
    decision, plan_decision = children
    from .screening import (
        PublicationDecision,
        is_issued_publication_decision_for_plan,
    )

    return (
        type(decision) is PublicationDecision
        and type(plan_decision) is LongPlanDecision
        and is_issued_publication_decision_for_plan(decision, plan_decision)
    )


def is_issued_ledger_signal(signal: object) -> bool:
    if type(signal) is not LedgerSignal:
        return False
    candidate = _phase1_derived_authority_candidate(
        _ISSUED_LEDGER_SIGNALS,
        signal,
    )
    if candidate is None:
        return False
    record = candidate[1]
    resolved = candidate[2]
    if resolved:
        if (
            len(resolved) != 1
            or resolved[0][1] not in {"SIGNAL", "CANONICAL_REPLAY"}
            or len(record.children) != 0
            or not _phase1_authority_sources_are_current(resolved)
        ):
            return False
    elif not _publication_signal_children_are_current(record.children):
        return False
    return _is_current_phase1_derived_authority(
        _ISSUED_LEDGER_SIGNALS,
        signal,
        candidate,
        _ledger_signal_fingerprint,
    )


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
) -> object:
    if type(authority) is not PaperEntryAuthority:
        raise TypeError("paper entry authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        authority,
        domain=_PAPER_ENTRY_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def is_issued_paper_entry_authority(authority: object) -> bool:
    """Return false until Task 8 verifies and registers source lineage."""
    if type(authority) is not PaperEntryAuthority:
        return False
    candidate = _phase1_derived_authority_candidate(
        _PAPER_ENTRY_AUTHORITIES,
        authority,
    )
    record = None if candidate is None else candidate[1]
    signal = (
        None
        if record is None or len(record.children) != 1
        else record.children[0]
    )
    if (
        candidate is None
        or len(candidate[2]) != 1
        or candidate[2][0][1] not in {"ENTRY", "CANONICAL_REPLAY"}
        or type(signal) is not LedgerSignal
        or not is_issued_ledger_signal(signal)
        or not _phase1_authority_sources_are_current(candidate[2])
    ):
        return False
    return _is_current_phase1_derived_authority(
        _PAPER_ENTRY_AUTHORITIES,
        authority,
        candidate,
        _paper_entry_fingerprint,
    )


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
) -> object:
    if type(authority) is not ShadowFillDispositionAuthority:
        raise TypeError("shadow fill authority type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        authority,
        domain=_SHADOW_FILL_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def is_issued_shadow_fill_disposition_authority(authority: object) -> bool:
    if type(authority) is not ShadowFillDispositionAuthority:
        return False
    candidate = _phase1_derived_authority_candidate(
        _SHADOW_FILL_DISPOSITION_AUTHORITIES,
        authority,
    )
    record = None if candidate is None else candidate[1]
    source = (
        None
        if candidate is None or len(candidate[2]) != 1
        else candidate[2][0][0]
    )
    if (
        candidate is None
        or len(candidate[2]) != 1
        or candidate[2][0][1] != "SHADOW_FILL"
        or record is None
        or len(record.children) != 1
        or record.children[0] is not getattr(source, "signal_source", None)
        or not _phase1_authority_sources_are_current(candidate[2])
    ):
        return False
    return _is_current_phase1_derived_authority(
        _SHADOW_FILL_DISPOSITION_AUTHORITIES,
        authority,
        candidate,
        _shadow_fill_disposition_fingerprint,
    )


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


_Phase1DerivedRegistry = dict[
    int,
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
]
_Phase1DerivedCandidate = tuple[
    tuple[ReferenceType[object], _Phase1DerivedAuthorityRecord],
    _Phase1DerivedAuthorityRecord,
    tuple[tuple[object, str], ...],
]


def _phase1_derived_authority_candidate(
    registry: _Phase1DerivedRegistry,
    value: object,
) -> _Phase1DerivedCandidate | None:
    """Capture exact registry and source-binding records without callbacks."""
    identity = id(value)
    with _EVENT_AUTHORITY_LOCK:
        issued = registry.get(identity)
        if (
            issued is None
            or issued[0]() is not value
            or type(issued[1]) is not _Phase1DerivedAuthorityRecord
        ):
            return None
        record = issued[1]
        if (
            type(record.bindings) is not tuple
            or type(record.children) is not tuple
        ):
            return None
        resolved = record.bindings
        if any(
            type(item) is not tuple
            or len(item) != 2
            or type(item[1]) is not str
            for item in resolved
        ):
            return None
        return issued, record, resolved


def _phase1_authority_sources_are_current(
    resolved: tuple[tuple[object, str], ...],
) -> bool:
    """Complete callback-bearing Journal verification before final sealing."""
    from . import journal as journal_module

    verifier_names = {
        "SIGNAL": "is_verified_phase1_signal_source",
        "ENTRY": "is_verified_phase1_entry_source",
        "SHADOW_FILL": "is_verified_phase1_shadow_fill_source",
        "CANONICAL_REPLAY": "is_verified_phase1_canonical_replay_source",
    }
    for source, kind in resolved:
        verifier_name = verifier_names.get(kind)
        verifier = (
            None
            if verifier_name is None
            else getattr(journal_module, verifier_name, None)
        )
        if verifier is None or not verifier(source):
            return False
    return True


def _is_current_phase1_derived_authority(
    registry: _Phase1DerivedRegistry,
    value: object,
    candidate: _Phase1DerivedCandidate,
    fingerprint_factory: Callable[..., object],
) -> bool:
    """Finish with a fresh hook-free seal and exact registry identities."""
    issued, record, resolved = candidate
    captured_fingerprint = record.seal
    captured_children = record.children
    captured_semantic_verifier = record.semantic_verifier
    try:
        if not captured_semantic_verifier(value, resolved, captured_children):
            return False
        fingerprint = fingerprint_factory(value)
    except Exception:
        return False
    from . import journal as journal_module

    identity = id(value)
    with _EVENT_AUTHORITY_LOCK:
        current = registry.get(identity)
        if (
            current is not issued
            or current[0]() is not value
            or current[1] is not record
            or current[1].seal is not captured_fingerprint
            or current[1].bindings is not resolved
            or current[1].children is not captured_children
            or current[1].semantic_verifier is not captured_semantic_verifier
        ):
            return False
        if any(
            current_source is not captured_source
            or current_kind != captured_kind
            for (current_source, current_kind), (
                captured_source,
                captured_kind,
            ) in zip(current[1].bindings, resolved, strict=True)
        ):
            return False
        return journal_module._source_fingerprint_seals_equal(
            captured_fingerprint,
            fingerprint,
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


def _make_hook_free_ledger_event_digest(
    *,
    sha256_factory: Callable[..., object],
    decimal_type: type[Decimal],
    date_type: type[date],
    datetime_type: type[datetime],
    utc_value: object,
    safe_tz_types: tuple[type[object], ...],
    event_type: type[LedgerEvent],
    lot_type: type[LedgerLot],
    decision_type: type[ComplianceDecision],
) -> tuple[Callable[[LedgerEvent], str], Callable[[object], str | None]]:
    """Build the event digest without resolving any mutable module globals."""

    def quote_string(value: str) -> str:
        pieces = ['"']
        escapes = {
            '"': '\\"',
            "\\": "\\\\",
            "\b": "\\b",
            "\f": "\\f",
            "\n": "\\n",
            "\r": "\\r",
            "\t": "\\t",
        }
        for character in value:
            escaped = escapes.get(character)
            if escaped is not None:
                pieces.append(escaped)
                continue
            codepoint = ord(character)
            if codepoint < 0x20 or codepoint > 0x7F:
                if codepoint <= 0xFFFF:
                    pieces.append(f"\\u{codepoint:04x}")
                else:
                    adjusted = codepoint - 0x10000
                    high = 0xD800 + (adjusted >> 10)
                    low = 0xDC00 + (adjusted & 0x3FF)
                    pieces.append(f"\\u{high:04x}\\u{low:04x}")
            else:
                pieces.append(character)
        pieces.append('"')
        return "".join(pieces)

    def encode_json(value: object) -> str:
        if value is None:
            return "null"
        if type(value) is bool:
            return "true" if value else "false"
        if type(value) is int:
            return str(value)
        if type(value) is str:
            return quote_string(value)
        if type(value) is list:
            return "[" + ",".join(encode_json(item) for item in value) + "]"
        if type(value) is dict:
            keys = tuple(value)
            if any(type(key) is not str for key in keys):
                raise TypeError("canonical digest keys must be exact strings")
            return "{" + ",".join(
                quote_string(key) + ":" + encode_json(value[key])
                for key in sorted(keys)
            ) + "}"
        raise TypeError("canonical digest leaf type is unverified")

    def safe_datetime(value: object) -> bool:
        return (
            type(value) is datetime_type
            and any(type(value.tzinfo) is candidate for candidate in safe_tz_types)
        )

    def timestamp(value: object) -> str:
        if not safe_datetime(value):
            raise TypeError("canonical digest datetime is unverified")
        return (
            value.astimezone(utc_value)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

    def money_micros(value: object) -> int:
        if type(value) is not decimal_type:
            raise TypeError("canonical digest money is unverified")
        scaled = value.scaleb(6)
        integral = scaled.to_integral_value()
        if scaled.as_tuple() != integral.as_tuple():
            raise TypeError("canonical digest money is not exact micros")
        return int(integral)

    def digest(event: LedgerEvent) -> str:
        if type(event) is not event_type:
            raise TypeError("ledger event type is unverified")
        lot = object.__getattribute__(event, "lot")
        decision = object.__getattribute__(event, "decision")
        if (
            type(lot) is not lot_type
            or type(decision) is not decision_type
            or type(lot.shares) is not int
            or type(lot.total_cost_micros) is not int
            or lot.total_cost_micros != money_micros(lot.price) * lot.shares
            or type(decision.reason_codes) is not tuple
            or any(type(reason) is not str for reason in decision.reason_codes)
        ):
            raise TypeError("ledger event child type is unverified")
        payload = {
            "ledger_name": event.ledger_name,
            "signal_id": event.signal_id,
            "lot": {
                "price_micros": money_micros(lot.price),
                "shares": lot.shares,
                "total_cost_micros": lot.total_cost_micros,
                "at": timestamp(lot.at),
                "parent_order_id": lot.parent_order_id,
            },
            "signal_digest": event.signal_digest,
            "user_confirmed_stop_micros": (
                None
                if event.user_confirmed_stop is None
                else money_micros(event.user_confirmed_stop)
            ),
            "recommended_stop_micros": (
                None
                if event.recommended_stop is None
                else money_micros(event.recommended_stop)
            ),
            "profit_target_taken": event.profit_target_taken,
            "decision": {
                "status": decision.status,
                "compliant": decision.compliant,
                "reconciliation_required": decision.reconciliation_required,
                "reason_codes": list(decision.reason_codes),
            },
            "event_id": event.event_id,
            "cursor": event.cursor,
            "ordinal": event.ordinal,
            "authority_basis": event.authority_basis,
            "message_time": (
                None if event.message_time is None else timestamp(event.message_time)
            ),
            "received_at": (
                None if event.received_at is None else timestamp(event.received_at)
            ),
        }
        encoded = encode_json(
            {
                "namespace": "stock-monitor/ledger-event/v1",
                "payload": payload,
            }
        ).encode("utf-8")
        return sha256_factory(encoded).hexdigest()

    def signal_source_digest(source: object) -> str | None:
        session = getattr(source, "publication_session", None)
        if (
            type(session) is not date_type
            or type(getattr(source, "signal_id", None)) is not str
            or type(getattr(source, "symbol", None)) is not str
            or type(getattr(source, "role", None)) is not str
            or any(
                type(getattr(source, field_name, None)) is not int
                for field_name in (
                    "maximum_entry_micros",
                    "recommended_stop_micros",
                    "target_micros",
                    "planned_shares",
                    "tick_size_micros",
                    "trigger_price_micros",
                )
            )
        ):
            return None
        payload = {
            "version": 2,
            "signal_id": source.signal_id,
            "symbol": source.symbol,
            "role": source.role,
            "publication_session": session.isoformat(),
            "maximum_entry_micros": source.maximum_entry_micros,
            "recommended_stop_micros": source.recommended_stop_micros,
            "target_micros": source.target_micros,
            "planned_shares": source.planned_shares,
            "tick_size_micros": source.tick_size_micros,
            "trigger_price_micros": source.trigger_price_micros,
        }
        encoded = encode_json(payload).encode("utf-8")
        return sha256_factory(encoded).hexdigest()

    return digest, signal_source_digest


def _make_phase1_derived_semantic_validators(
    *,
    decimal_type: type[Decimal],
    signal_digest_from_source: Callable[[object], str | None],
    signal_type: type[LedgerSignal],
    paper_type: type[PaperEntryAuthority],
    shadow_type: type[ShadowFillDispositionAuthority],
    replay_cohort_type: type[VerifiedLedgerReplayCohort],
    event_type: type[LedgerEvent],
    lot_type: type[LedgerLot],
    decision_type: type[ComplianceDecision],
    date_type: type[date],
    datetime_type: type[datetime],
    safe_tz_types: tuple[type[object], ...],
    event_digest: Callable[[LedgerEvent], str],
) -> tuple[
    Callable[[object, tuple[tuple[object, str], ...], tuple[object, ...]], bool],
    Callable[[object, tuple[tuple[object, str], ...], tuple[object, ...]], bool],
    Callable[[object, tuple[tuple[object, str], ...], tuple[object, ...]], bool],
    Callable[[object, tuple[tuple[object, str], ...], tuple[object, ...]], bool],
]:
    """Capture callback-free source-to-authority semantic rederivation."""

    def same_str(value: object, expected: object) -> bool:
        return type(value) is str and type(expected) is str and value == expected

    def same_int(value: object, expected: object) -> bool:
        return type(value) is int and type(expected) is int and value == expected

    def same_optional_int(value: object, expected: object) -> bool:
        return (value is None and expected is None) or same_int(value, expected)

    def same_date_identity(value: object, expected: object) -> bool:
        return (
            type(value) is date_type
            and type(expected) is date_type
            and value is expected
        )

    def safe_datetime(value: object) -> bool:
        return (
            type(value) is datetime_type
            and any(type(value.tzinfo) is candidate for candidate in safe_tz_types)
        )

    def same_datetime_identity(value: object, expected: object) -> bool:
        return (
            safe_datetime(value)
            and safe_datetime(expected)
            and value is expected
        )

    def expected_money(micros: object) -> Decimal | None:
        if type(micros) is not int:
            return None
        return decimal_type(micros).scaleb(-6)

    def same_money(value: object, micros: object) -> bool:
        expected = expected_money(micros)
        return (
            expected is not None
            and type(value) is decimal_type
            and value.as_tuple() == expected.as_tuple()
        )

    def money_micros(value: object) -> int | None:
        if type(value) is not decimal_type:
            return None
        scaled = value.scaleb(6)
        integral = scaled.to_integral_value()
        if scaled.as_tuple() != integral.as_tuple():
            return None
        return int(integral)

    def same_decimal(value: object, expected: object) -> bool:
        return (
            type(value) is decimal_type
            and type(expected) is decimal_type
            and value == expected
        )

    def signal_semantics(
        value: object,
        sources: tuple[tuple[object, str], ...],
        children: tuple[object, ...],
    ) -> bool:
        if (
            type(value) is not signal_type
            or type(value.signal_id) is not str
            or type(value.symbol) is not str
            or type(value.role) is not str
            or type(value.publication_session) is not date_type
            or type(value.maximum_entry) is not decimal_type
            or type(value.recommended_stop) is not decimal_type
            or type(value.target) is not decimal_type
            or type(value.planned_shares) is not int
            or type(value.tick_size) is not decimal_type
            or type(value.trigger_price) is not decimal_type
        ):
            return False
        if len(sources) == 1:
            if len(children) != 0:
                return False
            root_source, binding_kind = sources[0]
            if binding_kind == "CANONICAL_REPLAY":
                signal_sources = getattr(root_source, "signal_sources", ())
                if type(signal_sources) is not tuple:
                    return False
                matching_sources = tuple(
                    candidate
                    for candidate in signal_sources
                    if type(getattr(candidate, "signal_id", None)) is str
                    and candidate.signal_id == value.signal_id
                    and type(getattr(candidate, "role", None)) is str
                    and candidate.role == "PRIMARY"
                )
                if len(matching_sources) != 1:
                    return False
                source = matching_sources[0]
            else:
                source = root_source
            return (
                same_str(
                    value.signal_id,
                    getattr(source, "signal_id", None),
                )
                and same_str(value.symbol, getattr(source, "symbol", None))
                and same_str(value.role, getattr(source, "role", None))
                and same_date_identity(
                    value.publication_session,
                    getattr(source, "publication_session", None),
                )
                and same_money(
                    value.maximum_entry,
                    getattr(source, "maximum_entry_micros", None),
                )
                and same_money(
                    value.recommended_stop,
                    getattr(source, "recommended_stop_micros", None),
                )
                and same_money(
                    value.target,
                    getattr(source, "target_micros", None),
                )
                and same_int(
                    value.planned_shares,
                    getattr(source, "planned_shares", None),
                )
                and same_money(
                    value.tick_size,
                    getattr(source, "tick_size_micros", None),
                )
                and same_money(
                    value.trigger_price,
                    getattr(source, "trigger_price_micros", None),
                )
            )
        if len(sources) != 0 or len(children) != 2:
            return False
        decision, plan_decision = children
        publication = getattr(decision, "primary", None)
        candidate = getattr(publication, "candidate", None)
        plan = getattr(plan_decision, "plan", None)
        if publication is None or candidate is None or plan is None:
            return False
        session = getattr(candidate, "publication_session", None)
        candidate_symbol = getattr(candidate, "symbol", None)
        return (
            type(session) is date_type
            and type(candidate_symbol) is str
            and same_str(
                value.signal_id,
                f"{session.isoformat()}:{candidate_symbol}",
            )
            and same_str(value.symbol, candidate_symbol)
            and same_str(value.role, getattr(publication, "role", None))
            and same_date_identity(value.publication_session, session)
            and same_decimal(
                value.maximum_entry,
                getattr(candidate, "maximum_permitted_entry", None),
            )
            and same_decimal(
                value.recommended_stop,
                getattr(candidate, "recommended_stop", None),
            )
            and same_decimal(
                value.target,
                getattr(candidate, "target_price", None),
            )
            and same_int(value.planned_shares, getattr(plan, "quantity", None))
            and same_decimal(
                value.tick_size,
                getattr(candidate, "tick_size", None),
            )
            and same_decimal(
                value.trigger_price,
                getattr(candidate, "trigger_price", None),
            )
        )

    def paper_semantics(
        value: object,
        sources: tuple[tuple[object, str], ...],
        children: tuple[object, ...],
    ) -> bool:
        if (
            type(value) is not paper_type
            or len(sources) != 1
            or len(children) != 1
            or type(children[0]) is not signal_type
            or any(
                type(getattr(value, field_name)) is not str
                for field_name in (
                    "signal_id",
                    "signal_digest",
                    "lifecycle_event_id",
                    "trigger_observation_id",
                    "trigger_stream_id",
                    "trigger_feed",
                    "quote_observation_id",
                    "quote_stream_id",
                    "quote_feed",
                    "source_digest",
                    "session_complete_digest",
                    "canonical_event_id",
                    "calendar_digest",
                )
            )
            or any(
                not safe_datetime(getattr(value, field_name))
                for field_name in (
                    "trigger_at",
                    "trigger_received_at",
                    "quote_at",
                    "quote_received_at",
                    "cohort_received_through",
                )
            )
            or any(
                type(getattr(value, field_name)) is not int
                for field_name in (
                    "trigger_source_cursor",
                    "trigger_source_ordinal",
                    "trigger_stream_through_cursor",
                    "trigger_cohort_ordinal",
                    "quote_source_cursor",
                    "quote_source_ordinal",
                    "quote_stream_through_cursor",
                    "quote_cohort_ordinal",
                    "cohort_through_ordinal",
                    "lifecycle_cursor",
                    "action_ordinal",
                )
            )
            or any(
                sequence is not None and type(sequence) is not int
                for sequence in (value.trigger_sequence, value.quote_sequence)
            )
            or any(
                type(getattr(value, field_name)) is not decimal_type
                for field_name in ("trigger_price", "bid", "ask")
            )
        ):
            return False
        root_source, binding_kind = sources[0]
        if binding_kind == "CANONICAL_REPLAY":
            entry_sources = getattr(root_source, "entry_sources", ())
            if type(entry_sources) is not tuple:
                return False
            matching_sources = tuple(
                candidate
                for candidate in entry_sources
                if getattr(candidate, "signal_source", None) is not None
                and type(
                    getattr(
                        getattr(candidate, "signal_source", None),
                        "signal_id",
                        None,
                    )
                ) is str
                and candidate.signal_source.signal_id == value.signal_id
            )
            if len(matching_sources) != 1:
                return False
            source = matching_sources[0]
        else:
            source = root_source
        signal = children[0]
        observations = getattr(source, "observations", ())
        completion = getattr(source, "completion", None)
        lifecycle = getattr(source, "lifecycle_event", None)
        if completion is None or lifecycle is None or type(observations) is not tuple:
            return False
        by_id = {
            getattr(item, "observation_id", None): item for item in observations
        }
        trigger = by_id.get(getattr(lifecycle, "trigger_observation_id", None))
        quote = by_id.get(getattr(lifecycle, "quote_observation_id", None))
        signal_source = getattr(source, "signal_source", None)
        if trigger is None or quote is None or signal_source is None:
            return False
        return (
            same_str(value.signal_id, signal.signal_id)
            and same_str(
                value.signal_digest,
                signal_digest_from_source(signal_source),
            )
            and same_str(
                value.lifecycle_event_id,
                lifecycle.lifecycle_event_id,
            )
            and same_str(
                value.trigger_observation_id,
                trigger.observation_id,
            )
            and same_str(value.trigger_stream_id, trigger.stream_id)
            and same_str(value.trigger_feed, trigger.feed)
            and same_datetime_identity(value.trigger_at, trigger.source_time)
            and same_datetime_identity(
                value.trigger_received_at,
                trigger.received_at,
            )
            and same_optional_int(
                value.trigger_sequence,
                trigger.provider_sequence,
            )
            and same_int(value.trigger_source_cursor, trigger.source_cursor)
            and same_int(value.trigger_source_ordinal, trigger.source_ordinal)
            and same_int(
                value.trigger_stream_through_cursor,
                trigger.stream_through_cursor,
            )
            and same_int(value.trigger_cohort_ordinal, trigger.cohort_ordinal)
            and same_money(value.trigger_price, trigger.trade_price_micros)
            and same_str(value.quote_observation_id, quote.observation_id)
            and same_str(value.quote_stream_id, quote.stream_id)
            and same_str(value.quote_feed, quote.feed)
            and same_datetime_identity(value.quote_at, quote.source_time)
            and same_datetime_identity(
                value.quote_received_at,
                quote.received_at,
            )
            and same_optional_int(value.quote_sequence, quote.provider_sequence)
            and same_int(value.quote_source_cursor, quote.source_cursor)
            and same_int(value.quote_source_ordinal, quote.source_ordinal)
            and same_int(
                value.quote_stream_through_cursor,
                quote.stream_through_cursor,
            )
            and same_int(value.quote_cohort_ordinal, quote.cohort_ordinal)
            and same_money(value.bid, quote.bid_micros)
            and same_money(value.ask, quote.ask_micros)
            and same_str(
                value.source_digest,
                getattr(source, "source_digest", None),
            )
            and same_str(
                value.session_complete_digest,
                completion.source_digest,
            )
            and same_int(
                value.cohort_through_ordinal,
                completion.cohort_through_ordinal,
            )
            and same_datetime_identity(
                value.cohort_received_through,
                completion.received_through,
            )
            and same_str(
                value.canonical_event_id,
                lifecycle.lifecycle_event_id,
            )
            and same_int(value.lifecycle_cursor, lifecycle.row_id)
            and same_int(value.action_ordinal, lifecycle.event_ordinal)
            and same_str(
                value.calendar_digest,
                getattr(source, "calendar_digest", None),
            )
        )

    def shadow_semantics(
        value: object,
        sources: tuple[tuple[object, str], ...],
        children: tuple[object, ...],
    ) -> bool:
        if (
            type(value) is not shadow_type
            or len(sources) != 1
            or any(
                type(getattr(value, field_name)) is not str
                for field_name in (
                    "signal_id",
                    "lifecycle_event_id",
                    "trigger_observation_id",
                    "quote_observation_id",
                    "source_digest",
                    "session_complete_digest",
                    "calendar_digest",
                )
            )
            or any(
                not safe_datetime(getattr(value, field_name))
                for field_name in ("trigger_at", "filled_at")
            )
            or type(value.fill_price) is not decimal_type
            or type(value.lifecycle_cursor) is not int
            or type(value.action_ordinal) is not int
        ):
            return False
        source = sources[0][0]
        signal = getattr(source, "signal_source", None)
        if len(children) != 1 or children[0] is not signal:
            return False
        observations = getattr(source, "observations", ())
        completion = getattr(source, "completion", None)
        lifecycle = getattr(source, "lifecycle_event", None)
        if completion is None or lifecycle is None or type(observations) is not tuple:
            return False
        by_id = {
            getattr(item, "observation_id", None): item for item in observations
        }
        trigger = by_id.get(getattr(lifecycle, "trigger_observation_id", None))
        quote = by_id.get(getattr(lifecycle, "quote_observation_id", None))
        if trigger is None or quote is None or signal is None:
            return False
        return (
            same_str(value.signal_id, signal.signal_id)
            and same_str(
                value.lifecycle_event_id,
                lifecycle.lifecycle_event_id,
            )
            and same_str(
                value.trigger_observation_id,
                trigger.observation_id,
            )
            and same_str(value.quote_observation_id, quote.observation_id)
            and same_datetime_identity(value.trigger_at, trigger.source_time)
            and same_datetime_identity(value.filled_at, quote.source_time)
            and same_money(value.fill_price, signal.maximum_entry_micros)
            and same_str(
                value.source_digest,
                getattr(source, "source_digest", None),
            )
            and same_str(
                value.session_complete_digest,
                completion.source_digest,
            )
            and same_str(
                value.calendar_digest,
                getattr(source, "calendar_digest", None),
            )
            and same_int(value.lifecycle_cursor, lifecycle.row_id)
            and same_int(value.action_ordinal, lifecycle.event_ordinal)
        )

    def replay_cohort_semantics(
        value: object,
        sources: tuple[tuple[object, str], ...],
        children: tuple[object, ...],
    ) -> bool:
        if (
            type(value) is not replay_cohort_type
            or len(sources) != 1
            or any(type(event) is not event_type for event in children)
        ):
            return False
        source = sources[0][0]
        expected_references: list[tuple[str, str, str]] = []
        cursors: list[int] = []
        for event in children:
            lot = object.__getattribute__(event, "lot")
            decision = object.__getattribute__(event, "decision")
            if (
                type(event.ledger_name) is not str
                or type(event.signal_id) is not str
                or type(lot) is not lot_type
                or type(lot.price) is not decimal_type
                or type(lot.shares) is not int
                or not safe_datetime(lot.at)
                or (
                    lot.parent_order_id is not None
                    and type(lot.parent_order_id) is not str
                )
                or type(lot.total_cost_micros) is not int
                or lot.total_cost_micros
                != money_micros(lot.price) * lot.shares
                or (
                    event.user_confirmed_stop is not None
                    and type(event.user_confirmed_stop) is not decimal_type
                )
                or type(decision) is not decision_type
                or type(decision.status) is not str
                or type(decision.compliant) is not bool
                or type(decision.reconciliation_required) is not bool
                or type(decision.reason_codes) is not tuple
                or any(type(reason) is not str for reason in decision.reason_codes)
                or type(event.event_id) is not str
                or (event.cursor is not None and type(event.cursor) is not int)
                or type(event.ordinal) is not int
                or (
                    event.authority_basis is not None
                    and type(event.authority_basis) is not str
                )
                or type(event.signal_digest) is not str
                or not safe_datetime(event.message_time)
                or not safe_datetime(event.received_at)
                or (
                    event.recommended_stop is not None
                    and type(event.recommended_stop) is not decimal_type
                )
                or type(event.profit_target_taken) is not bool
            ):
                return False
            expected_references.append(
                (event.event_id, event_digest(event), event.signal_digest)
            )
            if event.cursor is not None:
                cursors.append(event.cursor)
        if (
            type(value.ledger_name) is not str
            or value.ledger_name != "CANONICAL"
            or type(value.references) is not tuple
            or len(value.references) != len(expected_references)
            or any(
                type(reference) is not tuple
                or len(reference) != 3
                or any(type(item) is not str for item in reference)
                or not same_str(reference[0], expected[0])
                or not same_str(reference[1], expected[1])
                or not same_str(reference[2], expected[2])
                for reference, expected in zip(
                    value.references,
                    expected_references,
                    strict=True,
                )
            )
            or not same_int(value.expected_count, len(children))
            or not same_optional_int(
                value.start_cursor,
                cursors[0] if cursors else None,
            )
            or not same_optional_int(
                value.terminal_cursor,
                cursors[-1] if cursors else None,
            )
            or not same_datetime_identity(
                value.query_cutoff,
                getattr(source, "query_cutoff", None),
            )
            or not same_str(
                value.source_digest,
                getattr(source, "source_digest", None),
            )
        ):
            return False
        return True

    return (
        signal_semantics,
        paper_semantics,
        shadow_semantics,
        replay_cohort_semantics,
    )


def _make_phase1_derived_authority_registrar(
    *,
    frame_getter: Callable[..., object],
    trusted_globals: dict[str, object],
    risk_block: type[RiskBlock],
    authority_lock: RLock,
    reference_factory: Callable[..., ReferenceType[object]],
    binding_registry: dict[
        int,
        tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
    ],
    record_type: type[_Phase1DerivedAuthorityRecord],
    source_verifier: Callable[[tuple[tuple[object, str], ...]], bool],
    policies: tuple[
        tuple[
            _Phase1DerivedRegistry,
            type[object],
            Callable[..., object],
            tuple[object, ...],
            frozenset[str],
            object | None,
            Callable[[tuple[object, ...]], bool] | None,
            Callable[
                [object, tuple[tuple[object, str], ...], tuple[object, ...]],
                bool,
            ],
        ],
        ...,
    ],
) -> Callable[..., None]:
    """Capture immutable issuer code identities in one construction closure."""

    def register(
        registry: _Phase1DerivedRegistry,
        value: object,
        _untrusted_fingerprint: object | None = None,
        *,
        sources: tuple[tuple[object, str], ...] = (),
        children: tuple[object, ...] = (),
    ) -> None:
        del _untrusted_fingerprint
        policy = next(
            (candidate for candidate in policies if registry is candidate[0]),
            None,
        )
        if policy is None:
            raise risk_block("PHASE1_DERIVED_AUTHORITY_ISSUER_UNVERIFIED")
        (
            trusted_registry,
            exact_type,
            fingerprint_factory,
            issuer_codes,
            allowed_kinds,
            child_issuer_code,
            child_verifier,
            semantic_verifier,
        ) = policy
        caller_frame = frame_getter(1)
        if (
            caller_frame.f_globals is not trusted_globals
            or not (
                any(caller_frame.f_code is code for code in issuer_codes)
                or caller_frame.f_code is child_issuer_code
            )
        ):
            raise risk_block("PHASE1_DERIVED_AUTHORITY_ISSUER_UNVERIFIED")
        if (
            type(value) is not exact_type
            or type(sources) is not tuple
            or type(children) is not tuple
        ):
            raise risk_block("PHASE1_DERIVED_AUTHORITY_SOURCE_UNVERIFIED")
        if caller_frame.f_code is child_issuer_code:
            if (
                len(sources) != 0
                or child_verifier is None
                or not child_verifier(children)
            ):
                raise risk_block("PHASE1_DERIVED_AUTHORITY_SOURCE_UNVERIFIED")
        elif (
            len(sources) != 1
            or any(
                type(item) is not tuple
                or len(item) != 2
                or item[1] not in allowed_kinds
                for item in sources
            )
            or not source_verifier(sources)
        ):
            raise risk_block("PHASE1_DERIVED_AUTHORITY_SOURCE_UNVERIFIED")
        try:
            semantically_exact = semantic_verifier(value, sources, children)
        except Exception:
            semantically_exact = False
        if not semantically_exact:
            raise risk_block("PHASE1_DERIVED_AUTHORITY_CONTENT_UNVERIFIED")
        record = record_type(
            seal=fingerprint_factory(value),
            bindings=sources,
            children=children,
            semantic_verifier=semantic_verifier,
        )
        identity = id(value)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current = trusted_registry.get(identity)
                if current is not None and current[0] is dead:
                    trusted_registry.pop(identity, None)

        reference = reference_factory(value, discard)
        with authority_lock:
            if (
                trusted_registry.get(identity) is not None
                or binding_registry.get(identity) is not None
            ):
                raise risk_block("PHASE1_DERIVED_AUTHORITY_ALREADY_ISSUED")
            trusted_registry[identity] = (reference, record)

    return register


def _make_phase1_source_binder(
    *,
    frame_getter: Callable[..., object],
    trusted_globals: dict[str, object],
    risk_block: type[RiskBlock],
    authority_lock: RLock,
    reference_factory: Callable[..., ReferenceType[object]],
    binding_registry: dict[
        int,
        tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
    ],
    event_type: type[LedgerEvent],
    issuer_code: object,
    source_verifier: Callable[[tuple[tuple[object, str], ...]], bool],
) -> Callable[[object, Sequence[tuple[object, str]]], None]:
    """Capture the sole replay-event binder issuer in a construction closure."""

    def bind(
        value: object,
        sources: Sequence[tuple[object, str]],
    ) -> None:
        caller_frame = frame_getter(1)
        if (
            caller_frame.f_globals is not trusted_globals
            or caller_frame.f_code is not issuer_code
            or type(value) is not event_type
            or type(sources) is not tuple
            or len(sources) != 1
            or type(sources[0]) is not tuple
            or len(sources[0]) != 2
            or sources[0][1] != "CANONICAL_REPLAY"
        ):
            raise risk_block("PHASE1_SOURCE_BINDING_ISSUER_UNVERIFIED")
        frozen_sources = sources
        if not source_verifier(frozen_sources):
            raise risk_block("PHASE1_SOURCE_BINDING_ISSUER_UNVERIFIED")
        identity = id(value)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current = binding_registry.get(identity)
                if current is not None and current[0] is dead:
                    binding_registry.pop(identity, None)

        value_reference = reference_factory(value, discard)
        with authority_lock:
            if binding_registry.get(identity) is not None:
                raise risk_block("PHASE1_SOURCE_BINDING_ALREADY_ISSUED")
            binding_registry[identity] = (
                value_reference,
                frozen_sources,
            )

    return bind


def _phase1_bound_sources(value: object) -> tuple[tuple[object, str], ...]:
    if type(value) is LedgerSignal:
        registry: _Phase1DerivedRegistry | None = _ISSUED_LEDGER_SIGNALS
    elif type(value) is PaperEntryAuthority:
        registry = _PAPER_ENTRY_AUTHORITIES
    elif type(value) is ShadowFillDispositionAuthority:
        registry = _SHADOW_FILL_DISPOSITION_AUTHORITIES
    elif type(value) is VerifiedLedgerReplayCohort:
        registry = _VERIFIED_REPLAY_COHORT_AUTHORITIES
    else:
        registry = None
    with _EVENT_AUTHORITY_LOCK:
        if registry is not None:
            issued = registry.get(id(value))
            if (
                issued is None
                or issued[0]() is not value
                or type(issued[1]) is not _Phase1DerivedAuthorityRecord
            ):
                return ()
            return issued[1].bindings
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
) -> object:
    if type(batch) is not VerifiedLedgerEventBatch:
        raise TypeError("verified ledger batch type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        batch,
        domain=_VERIFIED_LEDGER_BATCH_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _replay_cohort_fingerprint(
    cohort: VerifiedLedgerReplayCohort,
) -> object:
    if type(cohort) is not VerifiedLedgerReplayCohort:
        raise TypeError("verified replay cohort type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        cohort,
        domain=_VERIFIED_REPLAY_COHORT_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _actual_projection_cohort_fingerprint(
    cohort: ActualProjectionCohort,
) -> object:
    if type(cohort) is not ActualProjectionCohort:
        raise TypeError("actual projection cohort type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        cohort,
        domain=_ACTUAL_PROJECTION_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def is_issued_actual_projection_cohort(cohort: object) -> bool:
    if type(cohort) is not ActualProjectionCohort:
        return False
    identity = id(cohort)
    with _EVENT_AUTHORITY_LOCK:
        issued = _ACTUAL_PROJECTION_COHORT_AUTHORITIES.get(identity)
        if (
            issued is None
            or issued[0]() is not cohort
        ):
            return False
        source = issued[2]()
        state = issued[3]
        registered_positions = issued[4]
        journal_candidate = issued[5]
        state_candidate = issued[6]
        content_verifier = issued[7]
    if source is None:
        return False
    from .journal import JournalActualReplaySource
    from .reconciliation import (
        ActualLedgerState,
        is_verified_actual_ledger_state_for_source,
    )

    if (
        type(source) is not JournalActualReplaySource
        or type(state) is not ActualLedgerState
        or not is_verified_actual_ledger_state_for_source(state, source)
    ):
        return False
    try:
        fingerprint = content_verifier(
            cohort,
            source,
            state,
            _verification_candidates=(
                journal_candidate,
                state_candidate,
            ),
        )
        cohort_positions = object.__getattribute__(cohort, "positions")
        state_positions = object.__getattribute__(state, "positions")
        strategy_positions = tuple(
            position
            for position in state_positions
            if position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        )
    except Exception:
        return False
    if (
        cohort_positions is not registered_positions
        or type(state_positions) is not tuple
        or len(strategy_positions) != len(registered_positions)
        or any(
            state_position is not registered_position
            for state_position, registered_position in zip(
                strategy_positions,
                registered_positions,
                strict=True,
            )
        )
    ):
        return False
    from . import journal as journal_module

    captured_fingerprint = issued[1]
    with _EVENT_AUTHORITY_LOCK:
        current = _ACTUAL_PROJECTION_COHORT_AUTHORITIES.get(identity)
        return (
            current is issued
            and current[0]() is cohort
            and current[1] is captured_fingerprint
            and current[2]() is source
            and current[3] is state
            and current[4] is registered_positions
            and current[5] is journal_candidate
            and current[6] is state_candidate
            and current[7] is content_verifier
            and journal_module._source_fingerprint_seals_equal(
                captured_fingerprint,
                fingerprint,
            )
        )


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
    # Constructors are module-global callback surfaces.  Re-establish the
    # exact source/state authorities after construction, then enter the
    # callback-free semantic/seal/install phase with no later virtual work.
    if (
        not is_verified_journal_replay_source(source)
        or not is_verified_actual_ledger_state(state)
        or not is_verified_actual_ledger_state_for_source(state, source)
    ):
        raise RiskBlock("ACTUAL_REPLAY_COHORT_MISMATCH")
    _install_actual_projection_authority(cohort, source, state)
    return cohort


def is_issued_verified_replay_cohort(cohort: object) -> bool:
    """Return false until Task 7/8 registers a source-row-verified cohort."""
    if type(cohort) is not VerifiedLedgerReplayCohort:
        return False
    candidate = _phase1_derived_authority_candidate(
        _VERIFIED_REPLAY_COHORT_AUTHORITIES,
        cohort,
    )
    record = None if candidate is None else candidate[1]
    if (
        candidate is None
        or len(candidate[2]) != 1
        or candidate[2][0][1] != "CANONICAL_REPLAY"
        or record is None
        or not _phase1_authority_sources_are_current(candidate[2])
    ):
        return False
    return _is_current_phase1_derived_authority(
        _VERIFIED_REPLAY_COHORT_AUTHORITIES,
        cohort,
        candidate,
        _replay_cohort_fingerprint,
    )


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


def _make_verified_batch_registrar(
    *,
    frame_getter: Callable[..., object],
    trusted_globals: dict[str, object],
    risk_block: type[RiskBlock],
    authority_lock: RLock,
    reference_factory: Callable[..., ReferenceType[object]],
    registry: dict[
        int,
        tuple[
            ReferenceType[object],
            object,
            tuple[LedgerEvent, ...],
            Callable[..., bool],
        ],
    ],
    batch_type: type[VerifiedLedgerEventBatch],
    event_type: type[LedgerEvent],
    lot_type: type[LedgerLot],
    decision_type: type[ComplianceDecision],
    decimal_type: type[Decimal],
    datetime_type: type[datetime],
    safe_tz_types: tuple[type[object], ...],
    fingerprint_factory: Callable[[VerifiedLedgerEventBatch], object],
    event_digest: Callable[[LedgerEvent], str],
    paper_issuer_code: object,
    paper_authority_type: type[PaperEntryAuthority],
    signal_type: type[LedgerSignal],
    paper_authority_verifier: Callable[[object], bool],
    authority_candidate_factory: Callable[..., object],
    authority_candidate_recheck: Callable[..., bool],
    paper_registry: _Phase1DerivedRegistry,
    signal_registry: _Phase1DerivedRegistry,
    paper_fingerprint_factory: Callable[..., object],
    signal_fingerprint_factory: Callable[..., object],
    issuer_codes: tuple[object, ...],
) -> Callable[..., None]:
    """Capture immutable exact batch issuer code identities once."""

    def safe_datetime(value: object) -> bool:
        return (
            type(value) is datetime_type
            and any(type(value.tzinfo) is candidate for candidate in safe_tz_types)
        )

    def same_decimal(value: object, expected: object) -> bool:
        return (
            type(value) is decimal_type
            and type(expected) is decimal_type
            and value.as_tuple() == expected.as_tuple()
        )

    def money_micros(value: object) -> int | None:
        if type(value) is not decimal_type:
            return None
        scaled = value.scaleb(6)
        integral = scaled.to_integral_value()
        if scaled.as_tuple() != integral.as_tuple():
            return None
        return int(integral)

    def paper_authority_candidates(
        authority: object,
    ) -> tuple[object, object, object] | None:
        if type(authority) is not paper_authority_type:
            return None
        authority_candidate = authority_candidate_factory(
            paper_registry,
            authority,
        )
        if authority_candidate is None:
            return None
        authority_record = authority_candidate[1]
        if (
            type(authority_record.children) is not tuple
            or len(authority_record.children) != 1
            or type(authority_record.children[0]) is not signal_type
        ):
            return None
        signal = authority_record.children[0]
        signal_candidate = authority_candidate_factory(
            signal_registry,
            signal,
        )
        if signal_candidate is None:
            return None
        return authority_candidate, signal, signal_candidate

    def paper_event_is_exact(
        event: object,
        authority: object,
        signal: object,
    ) -> bool:
        if (
            type(event) is not event_type
            or type(authority) is not paper_authority_type
            or type(signal) is not signal_type
            or type(authority.signal_id) is not str
            or type(authority.signal_digest) is not str
            or type(authority.canonical_event_id) is not str
            or type(authority.lifecycle_cursor) is not int
            or type(authority.action_ordinal) is not int
            or type(authority.source_digest) is not str
            or not safe_datetime(authority.quote_at)
            or not safe_datetime(authority.quote_received_at)
            or type(signal.signal_id) is not str
            or type(signal.maximum_entry) is not decimal_type
            or type(signal.planned_shares) is not int
        ):
            return False
        lot = object.__getattribute__(event, "lot")
        decision = object.__getattribute__(event, "decision")
        expected_unit_micros = money_micros(signal.maximum_entry)
        return (
            type(lot) is lot_type
            and type(decision) is decision_type
            and type(event.ledger_name) is str
            and event.ledger_name == "CANONICAL"
            and type(event.signal_id) is str
            and event.signal_id == authority.signal_id
            and event.signal_id == signal.signal_id
            and same_decimal(lot.price, signal.maximum_entry)
            and type(lot.shares) is int
            and lot.shares == signal.planned_shares
            and lot.at is authority.quote_at
            and lot.parent_order_id is None
            and type(lot.total_cost_micros) is int
            and type(expected_unit_micros) is int
            and lot.total_cost_micros
            == expected_unit_micros * lot.shares
            and event.user_confirmed_stop is None
            and type(decision.status) is str
            and decision.status == "COMPLIANT"
            and decision.compliant is True
            and decision.reconciliation_required is False
            and type(decision.reason_codes) is tuple
            and len(decision.reason_codes) == 0
            and type(event.event_id) is str
            and event.event_id == authority.canonical_event_id
            and type(event.cursor) is int
            and event.cursor == authority.lifecycle_cursor
            and type(event.ordinal) is int
            and event.ordinal == authority.action_ordinal
            and type(event.authority_basis) is str
            and event.authority_basis == authority.source_digest
            and type(event.signal_digest) is str
            and event.signal_digest == authority.signal_digest
            and event.message_time is authority.quote_received_at
            and event.received_at is authority.quote_received_at
            and event.recommended_stop is None
            and event.profit_target_taken is False
        )

    def event_is_hook_free(event: object) -> bool:
        if type(event) is not event_type:
            return False
        lot = object.__getattribute__(event, "lot")
        decision = object.__getattribute__(event, "decision")
        return (
            type(event.ledger_name) is str
            and type(event.signal_id) is str
            and type(lot) is lot_type
            and type(lot.price) is decimal_type
            and type(lot.shares) is int
            and safe_datetime(lot.at)
            and (
                lot.parent_order_id is None
                or type(lot.parent_order_id) is str
            )
            and type(lot.total_cost_micros) is int
            and (
                event.user_confirmed_stop is None
                or type(event.user_confirmed_stop) is decimal_type
            )
            and type(decision) is decision_type
            and type(decision.status) is str
            and type(decision.compliant) is bool
            and type(decision.reconciliation_required) is bool
            and type(decision.reason_codes) is tuple
            and all(type(reason) is str for reason in decision.reason_codes)
            and type(event.event_id) is str
            and (event.cursor is None or type(event.cursor) is int)
            and type(event.ordinal) is int
            and (
                event.authority_basis is None
                or type(event.authority_basis) is str
            )
            and (event.signal_digest is None or type(event.signal_digest) is str)
            and (
                event.message_time is None or safe_datetime(event.message_time)
            )
            and (event.received_at is None or safe_datetime(event.received_at))
            and (
                event.recommended_stop is None
                or type(event.recommended_stop) is decimal_type
            )
            and type(event.profit_target_taken) is bool
        )

    def content_is_current(
        batch: object,
        events: object,
    ) -> bool:
        if (
            type(batch) is not batch_type
            or type(events) is not tuple
            or any(not event_is_hook_free(event) for event in events)
        ):
            return False
        expected_references = tuple(
            (event.event_id, event_digest(event)) for event in events
        )
        return (
            type(batch.references) is tuple
            and len(batch.references) == len(expected_references)
            and all(
                type(reference) is tuple
                and len(reference) == 2
                and type(reference[0]) is str
                and type(reference[1]) is str
                and reference[0] == expected[0]
                and reference[1] == expected[1]
                for reference, expected in zip(
                    batch.references,
                    expected_references,
                    strict=True,
                )
            )
        )

    def register(
        batch: object,
        *,
        events: tuple[LedgerEvent, ...] = (),
        paper_authority: object = None,
    ) -> None:
        caller_frame = frame_getter(1)
        if (
            caller_frame.f_globals is not trusted_globals
            or caller_frame.f_code not in issuer_codes
        ):
            raise risk_block("VERIFIED_LEDGER_BATCH_ISSUER_UNVERIFIED")
        paper_candidates: tuple[object, object, object] | None = None
        if caller_frame.f_code is paper_issuer_code:
            if (
                type(events) is not tuple
                or len(events) != 1
                or type(paper_authority) is not paper_authority_type
                or paper_authority_verifier(paper_authority) is not True
            ):
                raise risk_block("VERIFIED_LEDGER_BATCH_SOURCE_UNVERIFIED")
            paper_candidates = paper_authority_candidates(paper_authority)
            if paper_candidates is None:
                raise risk_block("VERIFIED_LEDGER_BATCH_SOURCE_UNVERIFIED")
            authority_candidate, signal, signal_candidate = paper_candidates
            if (
                not authority_candidate_recheck(
                    paper_registry,
                    paper_authority,
                    authority_candidate,
                    paper_fingerprint_factory,
                )
                or not authority_candidate_recheck(
                    signal_registry,
                    signal,
                    signal_candidate,
                    signal_fingerprint_factory,
                )
                or not paper_event_is_exact(
                    events[0],
                    paper_authority,
                    signal,
                )
            ):
                raise risk_block("VERIFIED_LEDGER_BATCH_SOURCE_UNVERIFIED")
        elif paper_authority is not None:
            raise risk_block("VERIFIED_LEDGER_BATCH_SOURCE_UNVERIFIED")
        if not content_is_current(batch, events):
            raise risk_block("VERIFIED_LEDGER_BATCH_CONTENT_UNVERIFIED")
        fingerprint = fingerprint_factory(batch)
        if paper_candidates is not None:
            authority_candidate, signal, signal_candidate = paper_candidates
            if (
                not authority_candidate_recheck(
                    paper_registry,
                    paper_authority,
                    authority_candidate,
                    paper_fingerprint_factory,
                )
                or not authority_candidate_recheck(
                    signal_registry,
                    signal,
                    signal_candidate,
                    signal_fingerprint_factory,
                )
            ):
                raise risk_block("VERIFIED_LEDGER_BATCH_SOURCE_UNVERIFIED")
        identity = id(batch)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current = registry.get(identity)
                if current is not None and current[0] is dead:
                    registry.pop(identity, None)

        reference = reference_factory(batch, discard)
        with authority_lock:
            if registry.get(identity) is not None:
                raise risk_block("VERIFIED_LEDGER_BATCH_ALREADY_ISSUED")
            registry[identity] = (
                reference,
                fingerprint,
                events,
                content_is_current,
            )

    return register


def _make_actual_projection_authority_installer(
    *,
    frame_getter: Callable[..., object],
    trusted_globals: dict[str, object],
    risk_block: type[RiskBlock],
    authority_lock: RLock,
    reference_factory: Callable[..., ReferenceType[object]],
    registry: dict[
        int,
        tuple[
            ReferenceType[object],
            object,
            ReferenceType[object],
            object,
            tuple[object, ...],
            object,
            object,
            Callable[..., object],
        ],
    ],
    cohort_type: type[ActualProjectionCohort],
    source_type: type[object],
    state_type: type[object],
    action_type: type[object],
    position_type: type[object],
    closed_trade_type: type[object],
    datetime_type: type[datetime],
    safe_tz_types: tuple[type[object], ...],
    fingerprint_factory: Callable[[ActualProjectionCohort], object],
    journal_candidate_factory: Callable[[object], object | None],
    journal_candidate_recheck: Callable[[object], bool],
    state_candidate_factory: Callable[[object, object], object | None],
    state_candidate_recheck: Callable[[object], bool],
    issuer_code: object,
) -> Callable[..., object]:
    """Build one typed actual-projection grant behind its exact issuer frame."""

    def install(
        cohort: ActualProjectionCohort,
        source: object,
        state: object,
        *,
        _verification_candidates: object = None,
    ) -> object:
        verification_mode = _verification_candidates is not None
        if verification_mode:
            if (
                type(_verification_candidates) is not tuple
                or len(_verification_candidates) != 2
            ):
                raise risk_block(
                    "ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED"
                )
            journal_candidate, state_candidate = _verification_candidates
        else:
            caller_frame = frame_getter(1)
            if (
                caller_frame.f_globals is not trusted_globals
                or caller_frame.f_code is not issuer_code
            ):
                raise risk_block(
                    "ACTUAL_PROJECTION_COHORT_ISSUER_UNVERIFIED"
                )
            journal_candidate = journal_candidate_factory(source)
            state_candidate = state_candidate_factory(state, source)
        if (
            type(cohort) is not cohort_type
            or type(source) is not source_type
            or type(state) is not state_type
            or journal_candidate is None
            or state_candidate is None
        ):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        positions = object.__getattribute__(cohort, "positions")
        state_positions = object.__getattribute__(state, "positions")
        if (
            type(positions) is not tuple
            or any(
                cursor is not None and type(cursor) is not int
                for cursor in (
                    cohort.projection_start_cursor,
                    cohort.projection_terminal_cursor,
                    cohort.source_through_cursor,
                    cohort.physical_source_highwater_cursor,
                )
            )
            or type(cohort.query_cutoff) is not datetime_type
            or not any(
                type(cohort.query_cutoff.tzinfo) is candidate
                for candidate in safe_tz_types
            )
            or any(
                type(digest) is not str
                for digest in (
                    cohort.journal_source_digest,
                    cohort.actual_state_digest,
                    cohort.calendar_digest,
                    cohort.policy_digest,
                )
            )
            or type(cohort.expected_action_count) is not int
            or type(cohort.expected_posting_count) is not int
        ):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        actions = object.__getattribute__(source, "actions")
        state_closed_trades = object.__getattribute__(state, "closed_trades")
        source_through_execution_cursor = object.__getattribute__(
            source,
            "through_execution_cursor",
        )
        source_physical_highwater = object.__getattribute__(
            source,
            "source_through_cursor",
        )
        source_query_cutoff = object.__getattribute__(source, "query_cutoff")
        source_digest = object.__getattribute__(source, "source_digest")
        source_expected_action_count = object.__getattribute__(
            source,
            "expected_action_count",
        )
        source_expected_posting_count = object.__getattribute__(
            source,
            "expected_posting_count",
        )
        state_digest = object.__getattribute__(state, "source_digest")
        state_calendar_digest = object.__getattribute__(state, "calendar_digest")
        state_policy_digest = object.__getattribute__(state, "policy_digest")
        if (
            type(state_positions) is not tuple
            or type(state_closed_trades) is not tuple
            or type(actions) is not tuple
            or any(
                cursor is not None and type(cursor) is not int
                for cursor in (
                    source_through_execution_cursor,
                    source_physical_highwater,
                )
            )
            or type(source_query_cutoff) is not datetime_type
            or not any(
                type(source_query_cutoff.tzinfo) is candidate
                for candidate in safe_tz_types
            )
            or type(source_digest) is not str
            or type(source_expected_action_count) is not int
            or type(source_expected_posting_count) is not int
            or type(state_digest) is not str
            or type(state_calendar_digest) is not str
            or type(state_policy_digest) is not str
            or any(
                type(action) is not action_type
                or type(action.event_id) is not str
                or type(action.execution_event_id) is not int
                for action in actions
            )
            or any(
                type(position) is not position_type
                or type(position.lineage_kind) is not str
                or type(position.lifecycle_event_ids) is not tuple
                or any(
                    type(event_id) is not str
                    for event_id in position.lifecycle_event_ids
                )
                for position in state_positions
            )
            or any(
                type(trade) is not closed_trade_type
                or type(trade.source_event_ids) is not tuple
                or any(
                    type(event_id) is not str
                    for event_id in trade.source_event_ids
                )
                for trade in state_closed_trades
            )
        ):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        strategy_positions = tuple(
            position
            for position in state_positions
            if position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        )
        actions_by_id = {
            action.event_id: action for action in actions
        }
        if len(actions_by_id) != len(actions):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
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
                        for trade in state_closed_trades
                        for event_id in trade.source_event_ids
                    ),
                )
            )
        )
        if any(event_id not in actions_by_id for event_id in lifecycle_ids):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        lifecycle_cursors = tuple(
            sorted(
                actions_by_id[event_id].execution_event_id
                for event_id in lifecycle_ids
            )
        )
        if (
            len(positions) != len(strategy_positions)
            or any(
                position is not state_position
                for position, state_position in zip(
                    positions,
                    strategy_positions,
                    strict=True,
                )
            )
            or cohort.projection_start_cursor
            != (lifecycle_cursors[0] if lifecycle_cursors else None)
            or cohort.projection_terminal_cursor
            != (lifecycle_cursors[-1] if lifecycle_cursors else None)
            or cohort.source_through_cursor
            != source_through_execution_cursor
            or cohort.physical_source_highwater_cursor
            != source_physical_highwater
            or cohort.query_cutoff is not source_query_cutoff
            or cohort.journal_source_digest != source_digest
            or cohort.actual_state_digest != state_digest
            or cohort.calendar_digest != state_calendar_digest
            or cohort.policy_digest != state_policy_digest
            or cohort.expected_action_count
            != source_expected_action_count
            or cohort.expected_posting_count
            != source_expected_posting_count
        ):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        fingerprint = fingerprint_factory(cohort)
        if (
            not journal_candidate_recheck(journal_candidate)
            or not state_candidate_recheck(state_candidate)
        ):
            raise risk_block("ACTUAL_PROJECTION_COHORT_CONTENT_UNVERIFIED")
        if verification_mode:
            return fingerprint
        identity = id(cohort)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current = registry.get(identity)
                if current is not None and current[0] is dead:
                    registry.pop(identity, None)

        cohort_reference = reference_factory(cohort, discard)
        source_reference = reference_factory(source)
        with authority_lock:
            if registry.get(identity) is not None:
                raise risk_block("ACTUAL_PROJECTION_COHORT_ALREADY_ISSUED")
            registry[identity] = (
                cohort_reference,
                fingerprint,
                source_reference,
                state,
                positions,
                journal_candidate,
                state_candidate,
                install,
            )
        return fingerprint

    return install


def _make_canonical_replay_authority_installer(
    *,
    frame_getter: Callable[..., object],
    trusted_globals: dict[str, object],
    risk_block: type[RiskBlock],
    authority_lock: RLock,
    reference_factory: Callable[..., ReferenceType[object]],
    binding_registry: dict[
        int,
        tuple[ReferenceType[object], tuple[tuple[object, str], ...]],
    ],
    registry: dict[
        int,
        tuple[
            ReferenceType[object],
            object,
            ReferenceType[object],
            tuple[object, ...],
        ],
    ],
    replay_type: type[Phase1CanonicalLedgerReplay],
    source_type: type[object],
    signal_source_type: type[object],
    entry_source_type: type[object],
    lifecycle_source_type: type[object],
    posting_source_type: type[object],
    closed_trade_source_type: type[object],
    pair_type: type[LedgerPair],
    signal_type: type[LedgerSignal],
    paper_type: type[PaperEntryAuthority],
    event_type: type[LedgerEvent],
    lot_type: type[LedgerLot],
    decision_type: type[ComplianceDecision],
    batch_type: type[VerifiedLedgerEventBatch],
    cohort_type: type[VerifiedLedgerReplayCohort],
    position_type: type[LedgerPosition],
    canonical_type: type[CanonicalLedger],
    actual_type: type[ActualLedger],
    closed_trade_type: type[ClosedTrade],
    decimal_type: type[Decimal],
    date_type: type[date],
    datetime_type: type[datetime],
    utc_value: object,
    safe_tz_types: tuple[type[object], ...],
    signal_semantic_verifier: Callable[..., bool],
    paper_semantic_verifier: Callable[..., bool],
    event_digest: Callable[[LedgerEvent], str],
    fingerprint_factory: Callable[[Phase1CanonicalLedgerReplay], object],
    source_candidate_factory: Callable[[object], object | None],
    source_candidate_recheck: Callable[[object], bool],
    issuer_code: object,
) -> Callable[..., object]:
    """Atomically bind one typed canonical replay and its exact child graph."""

    def same_money(value: object, expected: object) -> bool:
        return (
            type(value) is decimal_type
            and type(expected) is decimal_type
            and value.as_tuple() == expected.as_tuple()
        )

    def same_str(value: object, expected: object) -> bool:
        return type(value) is str and type(expected) is str and value == expected

    def same_int(value: object, expected: object) -> bool:
        return type(value) is int and type(expected) is int and value == expected

    def same_optional_int(value: object, expected: object) -> bool:
        return (value is None and expected is None) or same_int(value, expected)

    def safe_datetime(value: object) -> bool:
        return (
            type(value) is datetime_type
            and any(type(value.tzinfo) is candidate for candidate in safe_tz_types)
        )

    def money_from_source(micros: object) -> Decimal | None:
        if type(micros) is not int:
            return None
        return decimal_type(micros).scaleb(-6)

    def money_micros(value: object) -> int | None:
        if type(value) is not decimal_type:
            return None
        scaled = value.scaleb(6)
        integral = scaled.to_integral_value()
        if scaled.as_tuple() != integral.as_tuple():
            return None
        return int(integral)

    def install(
        replay: Phase1CanonicalLedgerReplay,
        source: object,
        *,
        signals: tuple[LedgerSignal, ...],
        event_manifests: tuple[tuple[object, ...], ...],
        batch: VerifiedLedgerEventBatch,
        cohort: VerifiedLedgerReplayCohort,
        _verification_source_candidate: object = None,
    ) -> object:
        verification_mode = _verification_source_candidate is not None
        if verification_mode:
            source_candidate = _verification_source_candidate
        else:
            caller_frame = frame_getter(1)
            if (
                caller_frame.f_globals is not trusted_globals
                or caller_frame.f_code is not issuer_code
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_ISSUER_UNVERIFIED")
            source_candidate = source_candidate_factory(source)
        if (
            source_candidate is None
            or type(replay) is not replay_type
            or type(source) is not source_type
            or type(signals) is not tuple
            or any(type(signal) is not signal_type for signal in signals)
            or type(event_manifests) is not tuple
            or any(
                type(manifest) is not tuple or len(manifest) != 6
                for manifest in event_manifests
            )
            or (
                not verification_mode
                and type(batch) is not batch_type
            )
            or (verification_mode and batch is not None)
            or type(cohort) is not cohort_type
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_ISSUER_UNVERIFIED")
        source_signal_sources = object.__getattribute__(source, "signal_sources")
        source_entry_sources = object.__getattribute__(source, "entry_sources")
        source_lifecycle_events = object.__getattribute__(
            source,
            "lifecycle_events",
        )
        source_postings = object.__getattribute__(source, "postings")
        source_closed_trades = object.__getattribute__(source, "closed_trades")
        source_query_cutoff = object.__getattribute__(source, "query_cutoff")
        source_digest = object.__getattribute__(source, "source_digest")
        source_canonical_cash_micros = object.__getattribute__(
            source,
            "canonical_cash_micros",
        )
        source_settled_buying_power_micros = object.__getattribute__(
            source,
            "settled_buying_power_micros",
        )
        source_realized_pnl_micros = object.__getattribute__(
            source,
            "realized_pnl_micros",
        )
        source_lifecycle_terminal_cursor = object.__getattribute__(
            source,
            "lifecycle_terminal_cursor",
        )
        source_posting_terminal_cursor = object.__getattribute__(
            source,
            "posting_terminal_cursor",
        )
        if (
            type(source_signal_sources) is not tuple
            or type(source_entry_sources) is not tuple
            or type(source_lifecycle_events) is not tuple
            or type(source_postings) is not tuple
            or type(source_closed_trades) is not tuple
            or not safe_datetime(source_query_cutoff)
            or type(source_digest) is not str
            or type(source_canonical_cash_micros) is not int
            or type(source_settled_buying_power_micros) is not int
            or type(source_realized_pnl_micros) is not int
            or (
                source_lifecycle_terminal_cursor is not None
                and type(source_lifecycle_terminal_cursor) is not int
            )
            or (
                source_posting_terminal_cursor is not None
                and type(source_posting_terminal_cursor) is not int
            )
            or any(
                type(signal_source) is not signal_source_type
                or type(signal_source.signal_id) is not str
                or type(signal_source.role) is not str
                or type(signal_source.planned_shares) is not int
                for signal_source in source_signal_sources
            )
            or any(
                type(entry_source) is not entry_source_type
                or type(entry_source.signal_source) is not signal_source_type
                or type(entry_source.signal_source.signal_id) is not str
                for entry_source in source_entry_sources
            )
            or any(
                type(lifecycle) is not lifecycle_source_type
                or type(lifecycle.signal_id) is not str
                or type(lifecycle.event_kind) is not str
                or type(lifecycle.row_id) is not int
                or (
                    lifecycle.shares is not None
                    and type(lifecycle.shares) is not int
                )
                or (
                    lifecycle.price_micros is not None
                    and type(lifecycle.price_micros) is not int
                )
                or (
                    lifecycle.recommended_stop_micros is not None
                    and type(lifecycle.recommended_stop_micros) is not int
                )
                for lifecycle in source_lifecycle_events
            )
            or any(
                type(posting) is not posting_source_type
                or type(posting.signal_id) is not str
                or type(posting.entry_kind) is not str
                or (
                    posting.shares_delta is not None
                    and type(posting.shares_delta) is not int
                )
                for posting in source_postings
            )
            or any(
                type(trade) is not closed_trade_source_type
                or type(trade.session_date) is not date_type
                or type(trade.pnl_micros) is not int
                or type(trade.signal_id) is not str
                or not safe_datetime(trade.at)
                or type(trade.row_id) is not int
                or type(trade.trade_id) is not str
                or not safe_datetime(trade.message_time)
                or not safe_datetime(trade.received_at)
                for trade in source_closed_trades
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_SOURCE_UNVERIFIED")
        events = tuple(manifest[0] for manifest in event_manifests)
        if (
            any(type(event) is not event_type for event in events)
            or any(
                not signal_semantic_verifier(
                    signal,
                    ((source, "CANONICAL_REPLAY"),),
                    (),
                )
                for signal in signals
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        signal_by_id = {signal.signal_id: signal for signal in signals}
        primary_source_by_id = {
            signal_source.signal_id: signal_source
            for signal_source in source_signal_sources
            if signal_source.role == "PRIMARY"
        }
        entry_signal_ids = tuple(
            entry_source.signal_source.signal_id
            for entry_source in source_entry_sources
        )
        remaining_by_signal_id = {
            signal_id: 0 for signal_id in primary_source_by_id
        }
        if (
            len(signal_by_id) != len(signals)
            or len(primary_source_by_id) != len(signals)
            or set(signal_by_id) != set(primary_source_by_id)
            or len(entry_signal_ids) != len(set(entry_signal_ids))
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        buy_signal_ids: list[str] = []
        for posting in source_postings:
            if posting.entry_kind not in {"BUY", "SALE"}:
                continue
            if (
                posting.signal_id not in remaining_by_signal_id
                or type(posting.shares_delta) is not int
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
            remaining_by_signal_id[posting.signal_id] += posting.shares_delta
            if posting.entry_kind == "BUY":
                buy_signal_ids.append(posting.signal_id)
        if (
            len(buy_signal_ids) != len(set(buy_signal_ids))
            or set(buy_signal_ids) != set(entry_signal_ids)
            or any(
                remaining < 0
                or remaining > signal_by_id[signal_id].planned_shares
                for signal_id, remaining in remaining_by_signal_id.items()
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")

        def expected_open_state(
            signal: LedgerSignal,
            remaining: int,
        ) -> tuple[Decimal, bool] | None:
            exited_shares = signal.planned_shares - remaining
            partials = tuple(
                lifecycle
                for lifecycle in source_lifecycle_events
                if lifecycle.signal_id == signal.signal_id
                and lifecycle.event_kind == "PARTIAL_EXIT"
            )
            if exited_shares == 0:
                return None if partials else (signal.recommended_stop, False)
            if len(partials) != 1:
                return None
            partial = partials[0]
            stop_micros = partial.recommended_stop_micros
            price_micros = partial.price_micros
            signal_stop_micros = money_micros(signal.recommended_stop)
            tick_micros = money_micros(signal.tick_size)
            if (
                type(partial.shares) is not int
                or partial.shares != exited_shares
                or type(stop_micros) is not int
                or type(price_micros) is not int
                or type(signal_stop_micros) is not int
                or type(tick_micros) is not int
                or tick_micros <= 0
                or stop_micros <= signal_stop_micros
                or stop_micros >= price_micros
                or stop_micros % tick_micros != 0
            ):
                return None
            return (decimal_type(stop_micros).scaleb(-6), True)

        expected_event_references: list[tuple[str, str]] = []
        expected_cohort_references: list[tuple[str, str, str]] = []
        cursors: list[int] = []
        manifest_signal_ids: list[str] = []
        for manifest in event_manifests:
            (
                event,
                signal,
                authority,
                remaining_shares,
                recommended_stop,
                profit_target_taken,
            ) = manifest
            if (
                type(event) is not event_type
                or type(signal) is not signal_type
                or not any(signal is candidate for candidate in signals)
                or type(authority) is not paper_type
                or type(remaining_shares) is not int
                or remaining_shares <= 0
                or type(recommended_stop) is not decimal_type
                or type(profit_target_taken) is not bool
                or not paper_semantic_verifier(
                    authority,
                    ((source, "CANONICAL_REPLAY"),),
                    (signal,),
                )
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
            lot = object.__getattribute__(event, "lot")
            decision = object.__getattribute__(event, "decision")
            expected_event_id = (
                authority.canonical_event_id
                if remaining_shares == signal.planned_shares
                else (
                    f"{authority.canonical_event_id}:remaining:"
                    f"{remaining_shares}"
                )
            )
            expected_recommended_stop = (
                recommended_stop if profit_target_taken else None
            )
            authoritative_remaining = remaining_by_signal_id.get(
                signal.signal_id
            )
            authoritative_open_state = (
                None
                if authoritative_remaining is None
                else expected_open_state(signal, authoritative_remaining)
            )
            if (
                authoritative_remaining is None
                or authoritative_remaining <= 0
                or remaining_shares != authoritative_remaining
                or authoritative_open_state is None
                or not same_money(
                    recommended_stop,
                    authoritative_open_state[0],
                )
                or profit_target_taken is not authoritative_open_state[1]
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
            if (
                type(lot) is not lot_type
                or type(decision) is not decision_type
                or type(event.ledger_name) is not str
                or event.ledger_name != "CANONICAL"
                or type(event.signal_id) is not str
                or event.signal_id != signal.signal_id
                or not same_money(lot.price, signal.maximum_entry)
                or type(lot.shares) is not int
                or lot.shares != remaining_shares
                or lot.at is not authority.quote_at
                or lot.parent_order_id is not None
                or type(lot.total_cost_micros) is not int
                or lot.total_cost_micros
                != money_micros(lot.price) * lot.shares
                or event.user_confirmed_stop is not None
                or type(decision.status) is not str
                or decision.status != "COMPLIANT"
                or decision.compliant is not True
                or decision.reconciliation_required is not False
                or type(decision.reason_codes) is not tuple
                or len(decision.reason_codes) != 0
                or type(event.event_id) is not str
                or event.event_id != expected_event_id
                or type(event.cursor) is not int
                or event.cursor != authority.lifecycle_cursor
                or type(event.ordinal) is not int
                or event.ordinal != authority.action_ordinal
                or type(event.authority_basis) is not str
                or event.authority_basis != authority.source_digest
                or type(event.signal_digest) is not str
                or event.signal_digest != authority.signal_digest
                or event.message_time is not authority.quote_received_at
                or event.received_at is not authority.quote_received_at
                or event.profit_target_taken is not profit_target_taken
                or (
                    expected_recommended_stop is None
                    and event.recommended_stop is not None
                )
                or (
                    expected_recommended_stop is not None
                    and not same_money(
                        event.recommended_stop,
                        expected_recommended_stop,
                    )
                )
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
            digest = event_digest(event)
            expected_event_references.append((event.event_id, digest))
            expected_cohort_references.append(
                (event.event_id, digest, event.signal_digest)
            )
            cursors.append(event.cursor)
            manifest_signal_ids.append(signal.signal_id)
        canonical_event_order = tuple(
            sorted(
                events,
                key=lambda event: (
                    event.ledger_name,
                    event.lot.at.astimezone(utc_value),
                    event.cursor,
                    event.ordinal,
                    event.event_id,
                ),
            )
        )
        expected_open_signal_ids = {
            signal_id
            for signal_id, remaining in remaining_by_signal_id.items()
            if remaining > 0
        }
        if (
            len(manifest_signal_ids) != len(set(manifest_signal_ids))
            or set(manifest_signal_ids) != expected_open_signal_ids
            or any(
                event is not canonical_event
                for event, canonical_event in zip(
                    events,
                    canonical_event_order,
                    strict=True,
                )
            )
            or any(
                prior >= current
                for prior, current in zip(cursors, cursors[1:])
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        batch_matches = (
            type(batch) is batch_type
            and type(batch.references) is tuple
            and len(batch.references) == len(expected_event_references)
            and all(
                type(reference) is tuple
                and len(reference) == 2
                and type(reference[0]) is str
                and type(reference[1]) is str
                and reference[0] == expected[0]
                and reference[1] == expected[1]
                for reference, expected in zip(
                    batch.references,
                    expected_event_references,
                    strict=True,
                )
            )
        )
        if (
            (not verification_mode and not batch_matches)
            or type(cohort.references) is not tuple
            or len(cohort.references) != len(expected_cohort_references)
            or any(
                type(reference) is not tuple
                or len(reference) != 3
                or any(type(item) is not str for item in reference)
                or reference[0] != expected[0]
                or reference[1] != expected[1]
                or reference[2] != expected[2]
                for reference, expected in zip(
                    cohort.references,
                    expected_cohort_references,
                    strict=True,
                )
            )
            or type(cohort.ledger_name) is not str
            or cohort.ledger_name != "CANONICAL"
            or type(cohort.expected_count) is not int
            or cohort.expected_count != len(events)
            or not same_optional_int(
                cohort.start_cursor,
                cursors[0] if cursors else None,
            )
            or not same_optional_int(
                cohort.terminal_cursor,
                cursors[-1] if cursors else None,
            )
            or cohort.query_cutoff is not source_query_cutoff
            or type(cohort.source_digest) is not str
            or not same_str(
                cohort.source_digest,
                source_digest,
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        pair = object.__getattribute__(replay, "ledger_pair")
        pair_signals = object.__getattribute__(pair, "_signals")
        pair_events = object.__getattribute__(pair, "_events")
        pair_cohort = object.__getattribute__(pair, "_canonical_replay_cohort")
        canonical = object.__getattribute__(pair, "_canonical")
        actual = object.__getattribute__(pair, "_actual")
        canonical_positions = object.__getattribute__(canonical, "open_positions")
        if (
            type(pair) is not pair_type
            or type(pair_signals) is not tuple
            or len(pair_signals) != len(signals)
            or any(
                pair_signal is not signal
                for pair_signal, signal in zip(
                    pair_signals,
                    signals,
                    strict=True,
                )
            )
            or type(pair_events) is not tuple
            or len(pair_events) != len(events)
            or any(
                pair_event is not event
                for pair_event, event in zip(
                    pair_events,
                    events,
                    strict=True,
                )
            )
            or pair_cohort is not cohort
            or object.__getattribute__(pair, "_actual_replay_cohort") is not None
            or object.__getattribute__(pair, "_canonical_replay_verified")
            is not True
            or object.__getattribute__(pair, "_actual_replay_verified") is not False
            or object.__getattribute__(pair, "_replay_verified") is not False
            or object.__getattribute__(pair, "_sealed") is not True
            or type(canonical) is not canonical_type
            or type(actual) is not actual_type
            or type(canonical_positions) is not tuple
            or len(canonical_positions) != len(events)
            or not same_int(canonical.events_applied, len(events))
            or canonical.breaker_state is not None
            or type(actual.open_positions) is not tuple
            or len(actual.open_positions) != 0
            or not same_int(actual.events_applied, 0)
            or actual.reconciliation_required is not False
            or type(actual.reason_codes) is not tuple
            or len(actual.reason_codes) != 0
            or actual.stop_unverified is not False
            or actual.breaker_state is not None
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        for manifest in event_manifests:
            event, signal = manifest[:2]
            matching_positions = tuple(
                position
                for position in canonical_positions
                if type(position) is position_type
                and type(position.signal_id) is str
                and position.signal_id == signal.signal_id
            )
            if len(matching_positions) != 1:
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
            position = matching_positions[0]
            expected_stop = (
                signal.recommended_stop
                if event.recommended_stop is None
                else event.recommended_stop
            )
            if (
                type(position.symbol) is not str
                or position.symbol != signal.symbol
                or type(position.ledger_name) is not str
                or position.ledger_name != "CANONICAL"
                or not same_money(position.recommended_stop, expected_stop)
                or position.user_confirmed_stop is not None
                or not same_money(position.target, signal.target)
                or not same_money(position.tick_size, signal.tick_size)
                or type(position.lots) is not tuple
                or len(position.lots) != 1
                or type(position.lots[0]) is not lot_type
                or not same_money(position.lots[0].price, event.lot.price)
                or type(position.lots[0].shares) is not int
                or position.lots[0].shares != event.lot.shares
                or position.lots[0].at is not event.lot.at
                or position.lots[0].parent_order_id is not None
                or type(position.lots[0].total_cost_micros) is not int
                or position.lots[0].total_cost_micros
                != money_micros(position.lots[0].price)
                * position.lots[0].shares
                or event.lot.total_cost_micros
                != position.lots[0].total_cost_micros
                or position.reconciled is not True
                or type(position.reason_codes) is not tuple
                or len(position.reason_codes) != 0
                or position.profit_target_taken is not event.profit_target_taken
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        postings = object.__getattribute__(replay, "postings")
        closed_trades = object.__getattribute__(replay, "closed_trades")
        if (
            type(postings) is not tuple
            or type(source_postings) is not tuple
            or len(postings) != len(source_postings)
            or any(
                posting is not source_posting
                for posting, source_posting in zip(
                    postings,
                    source_postings,
                    strict=True,
                )
            )
            or type(closed_trades) is not tuple
            or type(source_closed_trades) is not tuple
            or len(closed_trades) != len(source_closed_trades)
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        for trade, source_trade in zip(
            closed_trades,
            source_closed_trades,
            strict=True,
        ):
            expected_pnl = money_from_source(
                getattr(source_trade, "pnl_micros", None)
            )
            if (
                type(trade) is not closed_trade_type
                or type(trade.session_date) is not date_type
                or trade.session_date is not getattr(
                    source_trade,
                    "session_date",
                    None,
                )
                or not same_money(trade.pnl, expected_pnl)
                or type(trade.signal_id) is not str
                or trade.signal_id != getattr(source_trade, "signal_id", None)
                or not safe_datetime(trade.at)
                or trade.at is not getattr(source_trade, "at", None)
                or type(trade.cursor) is not int
                or trade.cursor != getattr(source_trade, "row_id", None)
                or type(trade.ordinal) is not int
                or trade.ordinal != 0
                or trade.equity_after is not None
                or type(trade.source_id) is not str
                or trade.source_id != getattr(source_trade, "trade_id", None)
                or not safe_datetime(trade.message_time)
                or trade.message_time is not getattr(
                    source_trade,
                    "message_time",
                    None,
                )
                or not safe_datetime(trade.received_at)
                or trade.received_at is not getattr(
                    source_trade,
                    "received_at",
                    None,
                )
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_CHILD_UNVERIFIED")
        canonical_cash = money_from_source(
            source_canonical_cash_micros
        )
        settled_buying_power = money_from_source(
            source_settled_buying_power_micros
        )
        realized_pnl = money_from_source(
            source_realized_pnl_micros
        )
        expected_projection_terminal = cursors[-1] if cursors else None
        if (
            object.__getattribute__(replay, "cohort") is not cohort
            or not same_money(replay.canonical_cash, canonical_cash)
            or not same_money(
                replay.settled_buying_power,
                settled_buying_power,
            )
            or not same_money(replay.realized_pnl, realized_pnl)
            or not same_optional_int(
                replay.projection_terminal_cursor,
                expected_projection_terminal,
            )
            or not same_optional_int(
                replay.lifecycle_source_terminal_cursor,
                source_lifecycle_terminal_cursor,
            )
            or not same_optional_int(
                replay.posting_source_terminal_cursor,
                source_posting_terminal_cursor,
            )
            or not safe_datetime(replay.query_cutoff)
            or replay.query_cutoff is not source_query_cutoff
            or type(replay.source_digest) is not str
            or not same_str(
                replay.source_digest,
                source_digest,
            )
        ):
            raise risk_block("PHASE1_CANONICAL_REPLAY_CONTENT_UNVERIFIED")
        children = (
            pair,
            cohort,
            postings,
            closed_trades,
            signals,
            events,
            event_manifests,
        )
        bindings = ((source, "CANONICAL_REPLAY"),)
        fingerprint = fingerprint_factory(replay)
        if not source_candidate_recheck(source_candidate):
            raise risk_block("PHASE1_CANONICAL_REPLAY_SOURCE_UNVERIFIED")
        if verification_mode:
            return fingerprint
        identity = id(replay)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current_binding = binding_registry.get(identity)
                if current_binding is not None and current_binding[0] is dead:
                    binding_registry.pop(identity, None)
                current_authority = registry.get(identity)
                if current_authority is not None and current_authority[0] is dead:
                    registry.pop(identity, None)

        replay_reference = reference_factory(replay, discard)
        source_reference = reference_factory(source)
        with authority_lock:
            if (
                binding_registry.get(identity) is not None
                or registry.get(identity) is not None
            ):
                raise risk_block("PHASE1_CANONICAL_REPLAY_ALREADY_ISSUED")
            binding_registry[identity] = (replay_reference, bindings)
            registry[identity] = (
                replay_reference,
                fingerprint,
                source_reference,
                children,
                source_candidate,
                install,
            )
        return fingerprint

    return install


def _is_issued_verified_batch(batch: object) -> bool:
    if type(batch) is not VerifiedLedgerEventBatch:
        return False
    identity = id(batch)
    with _EVENT_AUTHORITY_LOCK:
        registered = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(identity)
        if registered is None or registered[0]() is not batch:
            return False
        captured_fingerprint = registered[1]
        events = registered[2]
        content_is_current = registered[3]
    try:
        if not content_is_current(batch, events):
            return False
        fingerprint = _verified_batch_fingerprint(batch)
    except Exception:
        return False
    from . import journal as journal_module

    with _EVENT_AUTHORITY_LOCK:
        current = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(identity)
        return (
            current is registered
            and current[0]() is batch
            and current[1] is captured_fingerprint
            and current[2] is events
            and current[3] is content_is_current
            and journal_module._source_fingerprint_seals_equal(
                captured_fingerprint,
                fingerprint,
            )
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
    _register_verified_batch(batch, events=(event,))
    return batch


def _issue_paper_verified_ledger_event_batch(
    event: LedgerEvent,
    authority: PaperEntryAuthority,
) -> VerifiedLedgerEventBatch:
    """Reject raw paper events; the ledger method constructs them internally."""
    del event, authority
    raise RiskBlock("LEDGER_EVENT_SOURCE_EVIDENCE_REQUIRED")


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
        batch = VerifiedLedgerEventBatch(
            ((event.event_id, _ledger_event_content_digest(event)),)
        )
        _register_verified_batch(
            batch,
            events=(event,),
            paper_authority=authority,
        )
        with _EVENT_AUTHORITY_LOCK:
            registered_batch = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(id(batch))
        if registered_batch is None or registered_batch[0]() is not batch:
            raise RiskBlock("VERIFIED_LEDGER_BATCH_CONTENT_UNVERIFIED")
        try:
            self._append(event, verified_event_batch=batch)
        finally:
            with _EVENT_AUTHORITY_LOCK:
                if (
                    _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(id(batch))
                    is registered_batch
                ):
                    _VERIFIED_LEDGER_BATCH_AUTHORITIES.pop(id(batch), None)

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
        bindings = _phase1_bound_sources(self)
        return (
            len(bindings) == 1
            and bindings[0][1] == "CANONICAL_REPLAY"
            and is_verified_phase1_canonical_ledger_replay_for_source(
                self,
                bindings[0][0],
            )
        )


def _phase1_canonical_ledger_replay_fingerprint(
    replay: Phase1CanonicalLedgerReplay,
) -> object:
    """Seal every replay field plus the complete ten-slot LedgerPair graph."""
    if type(replay) is not Phase1CanonicalLedgerReplay:
        raise TypeError("Phase 1 canonical ledger replay type is unverified")
    from . import journal as journal_module

    return journal_module._source_fingerprint_seal(
        replay,
        domain=_PHASE1_CANONICAL_LEDGER_REPLAY_FINGERPRINT_DOMAIN,
        root_mode=journal_module._MERKLE_OPAQUE_STRUCTURAL,
    )


def _is_current_phase1_canonical_ledger_replay_authority(
    replay: object,
    source: object,
) -> bool:
    """Pure final authority check after Journal currentness was established."""
    from . import journal as journal_module

    Phase1CanonicalReplaySource = journal_module.Phase1CanonicalReplaySource

    if type(replay) is not Phase1CanonicalLedgerReplay or (
        type(source) is not Phase1CanonicalReplaySource
    ):
        return False
    identity = id(replay)
    with _EVENT_AUTHORITY_LOCK:
        issued = _PHASE1_CANONICAL_LEDGER_REPLAY_AUTHORITIES.get(identity)
        binding = _PHASE1_SOURCE_BINDINGS.get(identity)
        if (
            issued is None
            or issued[0]() is not replay
            or issued[2]() is not source
            or type(issued[3]) is not tuple
            or len(issued[3]) != 7
            or binding is None
            or binding[0]() is not replay
            or len(binding[1]) != 1
            or binding[1][0][0] is not source
            or binding[1][0][1] != "CANONICAL_REPLAY"
        ):
            return False
        captured_issued = issued
        captured_fingerprint = issued[1]
        captured_children = issued[3]
        source_candidate = issued[4]
        content_verifier = issued[5]
    replay_children = (
        object.__getattribute__(replay, "ledger_pair"),
        object.__getattribute__(replay, "cohort"),
        object.__getattribute__(replay, "postings"),
        object.__getattribute__(replay, "closed_trades"),
    )
    if any(
        child is not captured_child
        for child, captured_child in zip(
            replay_children,
            captured_children[:4],
            strict=True,
        )
    ):
        return False
    pair, cohort, _postings, _closed_trades, signals, events, manifests = (
        captured_children
    )
    try:
        pair_signals = object.__getattribute__(pair, "_signals")
        pair_events = object.__getattribute__(pair, "_events")
        pair_cohort = object.__getattribute__(pair, "_canonical_replay_cohort")
    except (AttributeError, TypeError):
        return False
    if (
        type(pair) is not LedgerPair
        or type(cohort) is not VerifiedLedgerReplayCohort
        or type(signals) is not tuple
        or type(events) is not tuple
        or type(manifests) is not tuple
        or type(pair_signals) is not tuple
        or len(pair_signals) != len(signals)
        or any(
            pair_signal is not signal
            for pair_signal, signal in zip(pair_signals, signals, strict=True)
        )
        or type(pair_events) is not tuple
        or len(pair_events) != len(events)
        or any(
            pair_event is not event
            for pair_event, event in zip(pair_events, events, strict=True)
        )
        or pair_cohort is not cohort
        or len(manifests) != len(events)
        or any(
            type(manifest) is not tuple
            or len(manifest) != 6
            or manifest[0] is not event
            for manifest, event in zip(manifests, events, strict=True)
        )
    ):
        return False
    try:
        fingerprint = content_verifier(
            replay,
            source,
            signals=signals,
            event_manifests=manifests,
            batch=None,
            cohort=cohort,
            _verification_source_candidate=source_candidate,
        )
    except Exception:
        return False
    with _EVENT_AUTHORITY_LOCK:
        current = _PHASE1_CANONICAL_LEDGER_REPLAY_AUTHORITIES.get(identity)
        current_binding = _PHASE1_SOURCE_BINDINGS.get(identity)
        return (
            current is captured_issued
            and current[0]() is replay
            and current[1] is captured_fingerprint
            and current[2]() is source
            and current[3] is captured_children
            and current[4] is source_candidate
            and current[5] is content_verifier
            and current_binding is binding
            and current_binding[0]() is replay
            and len(current_binding[1]) == 1
            and current_binding[1][0][0] is source
            and current_binding[1][0][1] == "CANONICAL_REPLAY"
            and journal_module._source_fingerprint_seals_equal(
                captured_fingerprint,
                fingerprint,
            )
        )


def is_verified_phase1_canonical_ledger_replay_for_source(
    replay: object,
    source: object,
) -> bool:
    """Verify one exact issued replay against its exact current Journal source."""
    from .journal import (
        Phase1CanonicalReplaySource,
        is_verified_phase1_canonical_replay_source,
    )

    if type(replay) is not Phase1CanonicalLedgerReplay or (
        type(source) is not Phase1CanonicalReplaySource
    ):
        return False
    # Journal currentness may invoke SQLite callbacks, so it must precede the
    # final callback-free replay fingerprint and registry comparison.
    if not is_verified_phase1_canonical_replay_source(source):
        return False
    return _is_current_phase1_canonical_ledger_replay_authority(
        replay,
        source,
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
    """Convert source fields without minting provenance or authority."""
    del binding_source, binding_kind
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
    signal = _construct_phase1_signal(
        source,
        binding_source=source,
        binding_kind="SIGNAL",
    )
    _register_phase1_derived_authority(
        _ISSUED_LEDGER_SIGNALS,
        signal,
        sources=((source, "SIGNAL"),),
    )
    return signal


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
    """Convert validated-looking fields without minting any authority."""
    del binding_source, binding_kind
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
    authority = _paper_entry_from_source_material(
        source,
        signal=signal,
        calendar_digest=calendar_digest,
        binding_source=source,
        binding_kind="ENTRY",
    )
    _register_phase1_derived_authority(
        _PAPER_ENTRY_AUTHORITIES,
        authority,
        sources=((source, "ENTRY"),),
        children=(signal,),
    )
    return authority


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
        sources=((source, "SHADOW_FILL"),),
        children=(signal,),
    )
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
    for signal in signals:
        _register_phase1_derived_authority(
            _ISSUED_LEDGER_SIGNALS,
            signal,
            sources=((source, "CANONICAL_REPLAY"),),
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
    event_manifests: list[tuple[object, ...]] = []
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
        _register_phase1_derived_authority(
            _PAPER_ENTRY_AUTHORITIES,
            authority,
            sources=((source, "CANONICAL_REPLAY"),),
            children=(signal,),
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
        event_manifests.append(
            (
                event,
                signal,
                authority,
                remaining,
                recommended_stop,
                profit_target_taken,
            )
        )
    ordered_events = tuple(sorted(events, key=_ledger_projection_order_key))
    manifests_by_event_identity = {
        id(manifest[0]): manifest for manifest in event_manifests
    }
    ordered_event_manifests = tuple(
        manifests_by_event_identity[id(event)] for event in ordered_events
    )
    batch = VerifiedLedgerEventBatch(
        tuple(
            (event.event_id, _ledger_event_content_digest(event))
            for event in ordered_events
        )
    )
    _register_verified_batch(batch, events=ordered_events)
    with _EVENT_AUTHORITY_LOCK:
        registered_batch = _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(id(batch))
    if registered_batch is None or registered_batch[0]() is not batch:
        raise RiskBlock("VERIFIED_LEDGER_BATCH_CONTENT_UNVERIFIED")
    try:
        references = tuple(
            (
                event.event_id,
                _ledger_event_content_digest(event),
                event.signal_digest,
            )
            for event in ordered_events
            if event.signal_digest is not None
        )
        cursors = tuple(
            event.cursor
            for event in ordered_events
            if event.cursor is not None
        )
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
            sources=((source, "CANONICAL_REPLAY"),),
            children=ordered_events,
        )
        pair = LedgerPair(
            signals=signals,
            events=ordered_events,
            verified_event_batch=batch,
            verified_replay_cohorts=(cohort,),
        )
    finally:
        with _EVENT_AUTHORITY_LOCK:
            if (
                _VERIFIED_LEDGER_BATCH_AUTHORITIES.get(id(batch))
                is registered_batch
            ):
                _VERIFIED_LEDGER_BATCH_AUTHORITIES.pop(id(batch), None)
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
    # The source verifier may execute SQLite currentness checks.  Finish that
    # callback-capable boundary before sealing the derived replay, then install
    # the source binding and exact replay authority together with no later
    # callback-bearing operation.
    if not is_verified_phase1_canonical_replay_source(source):
        raise RiskBlock("PHASE1_CANONICAL_REPLAY_SOURCE_UNVERIFIED")
    _install_canonical_replay_authority(
        replay,
        source,
        signals=signals,
        event_manifests=ordered_event_manifests,
        batch=batch,
        cohort=cohort,
    )
    return replay


# Freeze the exact original issuer code objects and trusted registry/helper
# identities.  The construction factories and frame accessor are deleted so a
# caller cannot manufacture a new registrar with its own code object.
(
    _ledger_event_content_digest,
    _phase1_signal_source_digest,
) = _make_hook_free_ledger_event_digest(
    sha256_factory=sha256,
    decimal_type=Decimal,
    date_type=date,
    datetime_type=datetime,
    utc_value=UTC,
    safe_tz_types=(ZoneInfo, type(UTC)),
    event_type=LedgerEvent,
    lot_type=LedgerLot,
    decision_type=ComplianceDecision,
)
(
    _signal_semantics,
    _paper_semantics,
    _shadow_semantics,
    _replay_cohort_semantics,
) = _make_phase1_derived_semantic_validators(
    decimal_type=Decimal,
    signal_digest_from_source=_phase1_signal_source_digest,
    signal_type=LedgerSignal,
    paper_type=PaperEntryAuthority,
    shadow_type=ShadowFillDispositionAuthority,
    replay_cohort_type=VerifiedLedgerReplayCohort,
    event_type=LedgerEvent,
    lot_type=LedgerLot,
    decision_type=ComplianceDecision,
    date_type=date,
    datetime_type=datetime,
    safe_tz_types=(ZoneInfo, type(UTC)),
    event_digest=_ledger_event_content_digest,
)
_register_phase1_derived_authority = _make_phase1_derived_authority_registrar(
    frame_getter=_getframe,
    trusted_globals=globals(),
    risk_block=RiskBlock,
    authority_lock=_EVENT_AUTHORITY_LOCK,
    reference_factory=ref,
    binding_registry=_PHASE1_SOURCE_BINDINGS,
    record_type=_Phase1DerivedAuthorityRecord,
    source_verifier=_phase1_authority_sources_are_current,
    policies=(
        (
            _ISSUED_LEDGER_SIGNALS,
            LedgerSignal,
            _ledger_signal_fingerprint,
            (
                _issue_ledger_signal_from_phase1_source.__code__,
                _issue_canonical_ledger_replay_from_phase1_source.__code__,
            ),
            frozenset({"SIGNAL", "CANONICAL_REPLAY"}),
            LedgerSignal.from_publication_decision.__func__.__code__,
            _publication_signal_children_are_current,
            _signal_semantics,
        ),
        (
            _PAPER_ENTRY_AUTHORITIES,
            PaperEntryAuthority,
            _paper_entry_fingerprint,
            (
                _issue_paper_entry_authority_from_phase1_source.__code__,
                _issue_canonical_ledger_replay_from_phase1_source.__code__,
            ),
            frozenset({"ENTRY", "CANONICAL_REPLAY"}),
            None,
            None,
            _paper_semantics,
        ),
        (
            _SHADOW_FILL_DISPOSITION_AUTHORITIES,
            ShadowFillDispositionAuthority,
            _shadow_fill_disposition_fingerprint,
            (_issue_shadow_fill_disposition_from_phase1_source.__code__,),
            frozenset({"SHADOW_FILL"}),
            None,
            None,
            _shadow_semantics,
        ),
        (
            _VERIFIED_REPLAY_COHORT_AUTHORITIES,
            VerifiedLedgerReplayCohort,
            _replay_cohort_fingerprint,
            (_issue_canonical_ledger_replay_from_phase1_source.__code__,),
            frozenset({"CANONICAL_REPLAY"}),
            None,
            None,
            _replay_cohort_semantics,
        ),
    ),
)
_bind_phase1_sources = _make_phase1_source_binder(
    frame_getter=_getframe,
    trusted_globals=globals(),
    risk_block=RiskBlock,
    authority_lock=_EVENT_AUTHORITY_LOCK,
    reference_factory=ref,
    binding_registry=_PHASE1_SOURCE_BINDINGS,
    event_type=LedgerEvent,
    issuer_code=_issue_canonical_ledger_replay_from_phase1_source.__code__,
    source_verifier=_phase1_authority_sources_are_current,
)
_register_verified_batch = _make_verified_batch_registrar(
    frame_getter=_getframe,
    trusted_globals=globals(),
    risk_block=RiskBlock,
    authority_lock=_EVENT_AUTHORITY_LOCK,
    reference_factory=ref,
    registry=_VERIFIED_LEDGER_BATCH_AUTHORITIES,
    batch_type=VerifiedLedgerEventBatch,
    event_type=LedgerEvent,
    lot_type=LedgerLot,
    decision_type=ComplianceDecision,
    decimal_type=Decimal,
    datetime_type=datetime,
    safe_tz_types=(ZoneInfo, type(UTC)),
    fingerprint_factory=_verified_batch_fingerprint,
    event_digest=_ledger_event_content_digest,
    paper_issuer_code=LedgerPair.record_authorized_canonical_fill.__code__,
    paper_authority_type=PaperEntryAuthority,
    signal_type=LedgerSignal,
    paper_authority_verifier=is_issued_paper_entry_authority,
    authority_candidate_factory=_phase1_derived_authority_candidate,
    authority_candidate_recheck=_is_current_phase1_derived_authority,
    paper_registry=_PAPER_ENTRY_AUTHORITIES,
    signal_registry=_ISSUED_LEDGER_SIGNALS,
    paper_fingerprint_factory=_paper_entry_fingerprint,
    signal_fingerprint_factory=_ledger_signal_fingerprint,
    issuer_codes=(
        _issue_live_verified_ledger_event_batch.__code__,
        LedgerPair.record_authorized_canonical_fill.__code__,
        _issue_canonical_ledger_replay_from_phase1_source.__code__,
    ),
)
_install_actual_projection_authority = (
    _make_actual_projection_authority_installer(
        frame_getter=_getframe,
        trusted_globals=globals(),
        risk_block=RiskBlock,
        authority_lock=_EVENT_AUTHORITY_LOCK,
        reference_factory=ref,
        registry=_ACTUAL_PROJECTION_COHORT_AUTHORITIES,
        cohort_type=ActualProjectionCohort,
        source_type=_journal_authority_module.JournalActualReplaySource,
        state_type=_reconciliation_authority_module.ActualLedgerState,
        action_type=_journal_authority_module.JournalActionSource,
        position_type=_reconciliation_authority_module.ActualPositionState,
        closed_trade_type=(
            _reconciliation_authority_module.ActualClosedTrade
        ),
        datetime_type=datetime,
        safe_tz_types=(ZoneInfo, type(UTC)),
        fingerprint_factory=_actual_projection_cohort_fingerprint,
        journal_candidate_factory=(
            _journal_authority_module._journal_replay_source_authority_candidate
        ),
        journal_candidate_recheck=(
            _journal_authority_module._is_current_journal_authority_candidate_without_callbacks
        ),
        state_candidate_factory=(
            _reconciliation_authority_module._actual_ledger_state_authority_candidate
        ),
        state_candidate_recheck=(
            _reconciliation_authority_module._is_current_actual_ledger_state_authority_candidate_without_callbacks
        ),
        issuer_code=_issue_actual_projection_from_journal.__code__,
    )
)
_install_canonical_replay_authority = (
    _make_canonical_replay_authority_installer(
        frame_getter=_getframe,
        trusted_globals=globals(),
        risk_block=RiskBlock,
        authority_lock=_EVENT_AUTHORITY_LOCK,
        reference_factory=ref,
        binding_registry=_PHASE1_SOURCE_BINDINGS,
        registry=_PHASE1_CANONICAL_LEDGER_REPLAY_AUTHORITIES,
        replay_type=Phase1CanonicalLedgerReplay,
        source_type=_journal_authority_module.Phase1CanonicalReplaySource,
        signal_source_type=_journal_authority_module.Phase1SignalSource,
        entry_source_type=_journal_authority_module.Phase1EntrySource,
        lifecycle_source_type=(
            _journal_authority_module.Phase1LifecycleEventSource
        ),
        posting_source_type=(
            _journal_authority_module.Phase1CanonicalPostingSource
        ),
        closed_trade_source_type=(
            _journal_authority_module.Phase1ClosedTradeSource
        ),
        pair_type=LedgerPair,
        signal_type=LedgerSignal,
        paper_type=PaperEntryAuthority,
        event_type=LedgerEvent,
        lot_type=LedgerLot,
        decision_type=ComplianceDecision,
        batch_type=VerifiedLedgerEventBatch,
        cohort_type=VerifiedLedgerReplayCohort,
        position_type=LedgerPosition,
        canonical_type=CanonicalLedger,
        actual_type=ActualLedger,
        closed_trade_type=ClosedTrade,
        decimal_type=Decimal,
        date_type=date,
        datetime_type=datetime,
        utc_value=UTC,
        safe_tz_types=(ZoneInfo, type(UTC)),
        signal_semantic_verifier=_signal_semantics,
        paper_semantic_verifier=_paper_semantics,
        event_digest=_ledger_event_content_digest,
        fingerprint_factory=_phase1_canonical_ledger_replay_fingerprint,
        source_candidate_factory=(
            _journal_authority_module._phase1_source_authority_candidate
        ),
        source_candidate_recheck=(
            _journal_authority_module._is_current_journal_authority_candidate_without_callbacks
        ),
        issuer_code=(
            _issue_canonical_ledger_replay_from_phase1_source.__code__
        ),
    )
)
del (
    _make_hook_free_ledger_event_digest,
    _make_phase1_derived_semantic_validators,
    _make_phase1_derived_authority_registrar,
    _make_phase1_source_binder,
    _make_verified_batch_registrar,
    _make_actual_projection_authority_installer,
    _make_canonical_replay_authority_installer,
    _signal_semantics,
    _paper_semantics,
    _shadow_semantics,
    _replay_cohort_semantics,
    _phase1_signal_source_digest,
    _journal_authority_module,
    _reconciliation_authority_module,
    _getframe,
)


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
    "is_verified_phase1_canonical_ledger_replay_for_source",
]
