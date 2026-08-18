"""Pure Phase 1 lifecycle, paper-fill, and equity diagnostics.

These reducers deliberately issue no durable authority.  Journal adapters own
calendar release checks, complete source cohorts, persistence, and restart-safe
authority issuance; this module only evaluates already-normalized facts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from zoneinfo import ZoneInfo

from .domain import (
    DomainValidationError,
    MAX_MICRODOLLARS,
    MIN_MICRODOLLARS,
    money_from_micros,
    money_to_micros,
    require_aware_timestamp,
)


_ET = ZoneInfo("America/New_York")
_ZERO = Decimal("0")
_REGULAR_OPEN = time(9, 30)
_ENTRY_START = time(9, 35)
_REGULAR_CLOSE = time(16)
_EXPIRY_DEADLINE = time(8, 45)
_TERMINAL_FINALIZERS = frozenset()


class Phase1Error(ValueError):
    """A Phase 1 fact or transition is structurally unsafe."""

    def __init__(self, code: str) -> None:
        self.code = code
        self.reason_code = code
        super().__init__(code)


def _aware(value: object, code: str) -> datetime:
    try:
        return require_aware_timestamp(value, "timestamp")  # type: ignore[arg-type]
    except DomainValidationError:
        raise Phase1Error(code) from None


def _money(
    value: object,
    code: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise Phase1Error(code)
    if positive and value <= _ZERO:
        raise Phase1Error(code)
    if nonnegative and value < _ZERO:
        raise Phase1Error(code)
    try:
        return money_from_micros(money_to_micros(value))
    except DomainValidationError:
        raise Phase1Error(code) from None


def _optional_money(
    value: object | None,
    code: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal | None:
    if value is None:
        return None
    return _money(value, code, positive=positive, nonnegative=nonnegative)


def _nonnegative_int(value: object, code: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_MICRODOLLARS:
        raise Phase1Error(code)
    return value


def _positive_int(value: object, code: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_MICRODOLLARS:
        raise Phase1Error(code)
    return value


class SignalStatus(str, Enum):
    PUBLISHED = "PUBLISHED"
    TRIGGERED_AWAITING_LIMIT = "TRIGGERED_AWAITING_LIMIT"
    TRIGGERED_PAPER = "TRIGGERED_PAPER"
    SHADOW_FILLED_INFORMATIONAL = "SHADOW_FILLED_INFORMATIONAL"
    LIVE_CONFIRMED = "LIVE_CONFIRMED"
    SKIPPED_LIVE_TRACKED_PAPER = "SKIPPED_LIVE_TRACKED_PAPER"
    NOT_TRIGGERED = "NOT_TRIGGERED"
    NOT_FILLED_LIMIT = "NOT_FILLED_LIMIT"
    UNRESOLVED = "UNRESOLVED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    CLOSED = "CLOSED"


class SignalEventKind(str, Enum):
    TRIGGER_OBSERVED = "TRIGGER_OBSERVED"
    PAPER_FILL = "PAPER_FILL"
    SHADOW_FILL = "SHADOW_FILL"
    LIVE_CONFIRM = "LIVE_CONFIRM"
    LIVE_SKIP = "LIVE_SKIP"
    FINALIZE_NOT_TRIGGERED = "FINALIZE_NOT_TRIGGERED"
    FINALIZE_NOT_FILLED = "FINALIZE_NOT_FILLED"
    FINALIZE_UNRESOLVED = "FINALIZE_UNRESOLVED"
    EXPIRE = "EXPIRE"
    INVALIDATE = "INVALIDATE"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    CLOSE = "CLOSE"


_TERMINAL_FINALIZERS = frozenset(
    {
        SignalEventKind.FINALIZE_NOT_TRIGGERED,
        SignalEventKind.FINALIZE_NOT_FILLED,
        SignalEventKind.FINALIZE_UNRESOLVED,
    }
)
_ENTRY_EVENTS = frozenset(
    {
        SignalEventKind.PAPER_FILL,
        SignalEventKind.SHADOW_FILL,
        SignalEventKind.LIVE_CONFIRM,
        SignalEventKind.LIVE_SKIP,
    }
)
_CANONICAL_ENTRY_EVENTS = frozenset(
    {
        SignalEventKind.PAPER_FILL,
        SignalEventKind.LIVE_CONFIRM,
        SignalEventKind.LIVE_SKIP,
    }
)
_EXIT_EVENTS = frozenset(
    {
        SignalEventKind.PARTIAL_EXIT,
        SignalEventKind.CLOSE,
    }
)


@dataclass(frozen=True, slots=True)
class SignalExpiryDeadlineEvidence:
    """Diagnostic value describing an elapsed next-premarket deadline.

    Durable authority belongs to Journal's owner-current expiry source.  This
    value only makes the pure lifecycle distinction explicit: an expiry is
    caused by an elapsed calendar deadline, never by pretending that the
    publication session's observation cohort was complete.
    """

    signal_id: str
    publication_session: date
    deadline_session: date
    deadline_at: datetime
    observed_at: datetime
    calendar_digest: str
    source_id: str

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise Phase1Error("INVALID_EXPIRY_SIGNAL_ID")
        if (
            type(self.publication_session) is not date
            or type(self.deadline_session) is not date
            or self.deadline_session <= self.publication_session
        ):
            raise Phase1Error("INVALID_EXPIRY_SESSION")
        deadline_at = _aware(
            self.deadline_at,
            "INVALID_EXPIRY_DEADLINE_TIME",
        )
        observed_at = _aware(
            self.observed_at,
            "INVALID_EXPIRY_OBSERVED_TIME",
        )
        object.__setattr__(self, "deadline_at", deadline_at)
        object.__setattr__(self, "observed_at", observed_at)
        deadline_et = deadline_at.astimezone(_ET)
        if (
            deadline_et.date() != self.deadline_session
            or deadline_et.time().replace(tzinfo=None) != _EXPIRY_DEADLINE
        ):
            raise Phase1Error("INVALID_EXPIRY_DEADLINE_TIME")
        observed_et = observed_at.astimezone(_ET)
        if (
            observed_et.date() != self.deadline_session
            or observed_at < deadline_at
            or observed_et.time().replace(tzinfo=None) > _ENTRY_START
        ):
            raise Phase1Error("INVALID_EXPIRY_OBSERVED_TIME")
        if (
            type(self.calendar_digest) is not str
            or len(self.calendar_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.calendar_digest
            )
        ):
            raise Phase1Error("INVALID_EXPIRY_CALENDAR_DIGEST")
        if type(self.source_id) is not str or not self.source_id:
            raise Phase1Error("INVALID_EXPIRY_SOURCE_ID")


@dataclass(frozen=True, slots=True)
class SignalEvent:
    event_id: str
    kind: SignalEventKind
    at: datetime
    source_id: str
    session_complete: bool = False
    exit_observation_id: str | None = None
    exit_authority_digest: str | None = None
    shares: int | None = None
    price: Decimal | None = None
    recommended_stop: Decimal | None = None
    expiry_evidence: SignalExpiryDeadlineEvidence | None = None

    def __post_init__(self) -> None:
        if type(self.event_id) is not str or not self.event_id:
            raise Phase1Error("INVALID_SIGNAL_EVENT_ID")
        if not isinstance(self.kind, SignalEventKind):
            raise Phase1Error("INVALID_SIGNAL_EVENT_KIND")
        object.__setattr__(self, "at", _aware(self.at, "INVALID_SIGNAL_EVENT_TIME"))
        if type(self.source_id) is not str or not self.source_id:
            raise Phase1Error("MISSING_SIGNAL_EVENT_SOURCE")
        if type(self.session_complete) is not bool:
            raise Phase1Error("INVALID_SESSION_COMPLETE_FLAG")
        if self.session_complete and self.kind not in _TERMINAL_FINALIZERS:
            raise Phase1Error("UNEXPECTED_SESSION_COMPLETE_EVIDENCE")
        if self.kind is SignalEventKind.EXPIRE:
            if not isinstance(
                self.expiry_evidence,
                SignalExpiryDeadlineEvidence,
            ):
                raise Phase1Error("EXPIRY_DEADLINE_EVIDENCE_REQUIRED")
        elif self.expiry_evidence is not None:
            raise Phase1Error("UNEXPECTED_EXPIRY_DEADLINE_EVIDENCE")
        exit_values = (
            self.exit_observation_id,
            self.exit_authority_digest,
            self.shares,
            self.price,
        )
        if self.kind in _EXIT_EVENTS:
            if any(value is None for value in exit_values):
                raise Phase1Error("INCOMPLETE_EXIT_EVENT_EVIDENCE")
            if (
                type(self.exit_observation_id) is not str
                or not self.exit_observation_id
                or type(self.exit_authority_digest) is not str
                or len(self.exit_authority_digest) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in self.exit_authority_digest
                )
            ):
                raise Phase1Error("INVALID_EXIT_EVENT_EVIDENCE")
            _positive_int(self.shares, "INVALID_EXIT_EVENT_EVIDENCE")
            object.__setattr__(
                self,
                "price",
                _money(
                    self.price,
                    "INVALID_EXIT_EVENT_EVIDENCE",
                    positive=True,
                ),
            )
            if self.kind is SignalEventKind.PARTIAL_EXIT:
                if self.recommended_stop is None:
                    raise Phase1Error("INCOMPLETE_RECOMMENDED_STOP")
                object.__setattr__(
                    self,
                    "recommended_stop",
                    _money(
                        self.recommended_stop,
                        "INVALID_RECOMMENDED_STOP",
                        positive=True,
                    ),
                )
            elif self.recommended_stop is not None:
                raise Phase1Error("UNEXPECTED_RECOMMENDED_STOP")
        elif any(
            value is not None
            for value in (*exit_values, self.recommended_stop)
        ):
            raise Phase1Error("UNEXPECTED_EXIT_EVENT_EVIDENCE")


@dataclass(frozen=True, slots=True)
class Signal:
    signal_id: str
    symbol: str
    role: str
    publication_session: date
    published_at: datetime
    status: SignalStatus = SignalStatus.PUBLISHED
    history: tuple[SignalEvent, ...] = ()

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise Phase1Error("INVALID_SIGNAL_ID")
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise Phase1Error("INVALID_SIGNAL_SYMBOL")
        if self.role not in {"PRIMARY", "WATCHLIST_SHADOW"}:
            raise Phase1Error("INVALID_SIGNAL_ROLE")
        if type(self.publication_session) is not date:
            raise Phase1Error("INVALID_PUBLICATION_SESSION")
        published_at = _aware(self.published_at, "INVALID_PUBLICATION_TIME")
        object.__setattr__(self, "published_at", published_at)
        published_et = published_at.astimezone(_ET)
        if (
            published_et.date() != self.publication_session
            or published_et.time().replace(tzinfo=None) > time(9, 35)
        ):
            raise Phase1Error("INVALID_PUBLICATION_TIME")
        if not isinstance(self.status, SignalStatus):
            raise Phase1Error("INVALID_SIGNAL_STATUS")
        if type(self.history) is not tuple or any(
            not isinstance(item, SignalEvent) for item in self.history
        ):
            raise Phase1Error("INVALID_SIGNAL_HISTORY")
        expected = _replay_signal_history(self, self.history)
        if expected != self.status:
            raise Phase1Error("SIGNAL_HISTORY_STATUS_MISMATCH")


_TRANSITIONS: dict[
    SignalStatus,
    dict[SignalEventKind, SignalStatus],
] = {
    SignalStatus.PUBLISHED: {
        SignalEventKind.TRIGGER_OBSERVED: SignalStatus.TRIGGERED_AWAITING_LIMIT,
        SignalEventKind.FINALIZE_NOT_TRIGGERED: SignalStatus.NOT_TRIGGERED,
        SignalEventKind.FINALIZE_UNRESOLVED: SignalStatus.UNRESOLVED,
        SignalEventKind.EXPIRE: SignalStatus.EXPIRED,
        SignalEventKind.INVALIDATE: SignalStatus.INVALIDATED,
    },
    SignalStatus.TRIGGERED_AWAITING_LIMIT: {
        SignalEventKind.PAPER_FILL: SignalStatus.TRIGGERED_PAPER,
        SignalEventKind.SHADOW_FILL: SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        SignalEventKind.LIVE_CONFIRM: SignalStatus.LIVE_CONFIRMED,
        SignalEventKind.LIVE_SKIP: SignalStatus.SKIPPED_LIVE_TRACKED_PAPER,
        SignalEventKind.FINALIZE_NOT_FILLED: SignalStatus.NOT_FILLED_LIMIT,
        SignalEventKind.FINALIZE_UNRESOLVED: SignalStatus.UNRESOLVED,
        SignalEventKind.EXPIRE: SignalStatus.EXPIRED,
        SignalEventKind.INVALIDATE: SignalStatus.INVALIDATED,
    },
    SignalStatus.TRIGGERED_PAPER: {
        SignalEventKind.LIVE_CONFIRM: SignalStatus.LIVE_CONFIRMED,
        SignalEventKind.LIVE_SKIP: SignalStatus.SKIPPED_LIVE_TRACKED_PAPER,
        SignalEventKind.PARTIAL_EXIT: SignalStatus.TRIGGERED_PAPER,
        SignalEventKind.CLOSE: SignalStatus.CLOSED,
    },
    SignalStatus.LIVE_CONFIRMED: {
        SignalEventKind.PARTIAL_EXIT: SignalStatus.LIVE_CONFIRMED,
        SignalEventKind.CLOSE: SignalStatus.CLOSED,
    },
    SignalStatus.SKIPPED_LIVE_TRACKED_PAPER: {
        SignalEventKind.PARTIAL_EXIT: SignalStatus.SKIPPED_LIVE_TRACKED_PAPER,
        SignalEventKind.CLOSE: SignalStatus.CLOSED,
    },
}


def _validate_transition_event(
    signal: Signal,
    event: SignalEvent,
    *,
    previous_at: datetime,
) -> None:
    if event.at < previous_at:
        raise Phase1Error("EVENT_TIME_REGRESSION")
    if event.kind is SignalEventKind.TRIGGER_OBSERVED:
        event_et = event.at.astimezone(_ET)
        if (
            event_et.date() != signal.publication_session
            or event_et.time().replace(tzinfo=None) <= _ENTRY_START
            or event_et.time().replace(tzinfo=None) > _REGULAR_CLOSE
        ):
            raise Phase1Error("INVALID_TRIGGER_TIME")
    if event.kind in _ENTRY_EVENTS:
        event_et = event.at.astimezone(_ET)
        event_clock = event_et.time().replace(tzinfo=None)
        if (
            event_et.date() != signal.publication_session
            or event_clock <= _ENTRY_START
            or event_clock > _REGULAR_CLOSE
        ):
            raise Phase1Error("INVALID_ENTRY_EVENT_TIME")
    if event.kind in _TERMINAL_FINALIZERS:
        if not event.session_complete:
            raise Phase1Error("SESSION_COMPLETE_EVIDENCE_REQUIRED")
        event_et = event.at.astimezone(_ET)
        # A release-verified calendar adapter must additionally prove that the
        # date is the next open session; pure code owns the premarket bound.
        if (
            event_et.date() <= signal.publication_session
            or event_et.time().replace(tzinfo=None) > _ENTRY_START
        ):
            raise Phase1Error("FINALIZATION_NOT_NEXT_PREMARKET")
    if event.kind is SignalEventKind.EXPIRE:
        evidence = event.expiry_evidence
        if not isinstance(evidence, SignalExpiryDeadlineEvidence):
            raise Phase1Error("EXPIRY_DEADLINE_EVIDENCE_REQUIRED")
        if evidence.signal_id != signal.signal_id:
            raise Phase1Error("EXPIRY_SIGNAL_MISMATCH")
        if evidence.publication_session != signal.publication_session:
            raise Phase1Error("EXPIRY_SESSION_MISMATCH")
        if evidence.source_id != event.source_id:
            raise Phase1Error("EXPIRY_SOURCE_MISMATCH")
        if evidence.observed_at != event.at:
            raise Phase1Error("EXPIRY_EVENT_TIME_MISMATCH")
    if (
        signal.role == "WATCHLIST_SHADOW"
        and event.kind in _CANONICAL_ENTRY_EVENTS
    ):
        raise Phase1Error("SHADOW_CANONICAL_FILL_PROHIBITED")
    if (
        event.kind is SignalEventKind.SHADOW_FILL
        and signal.role != "WATCHLIST_SHADOW"
    ):
        raise Phase1Error("SHADOW_FILL_ROLE_MISMATCH")


def _next_signal_status(
    signal: Signal,
    current: SignalStatus,
    event: SignalEvent,
) -> SignalStatus:
    target = _TRANSITIONS.get(current, {}).get(event.kind)
    if target is None:
        raise Phase1Error("ILLEGAL_SIGNAL_TRANSITION")
    return target


def _replay_signal_history(
    signal: Signal,
    history: tuple[SignalEvent, ...],
) -> SignalStatus:
    status = SignalStatus.PUBLISHED
    previous_at = signal.published_at
    event_ids: set[str] = set()
    for lifecycle_event in history:
        if lifecycle_event.event_id in event_ids:
            raise Phase1Error("DUPLICATE_SIGNAL_EVENT")
        _validate_transition_event(signal, lifecycle_event, previous_at=previous_at)
        status = _next_signal_status(signal, status, lifecycle_event)
        previous_at = lifecycle_event.at
        event_ids.add(lifecycle_event.event_id)
    return status


def advance_signal(signal: Signal, event: SignalEvent) -> Signal:
    """Return the immutable result of one explicit lifecycle transition."""
    if not isinstance(signal, Signal):
        raise TypeError("advance_signal requires a Signal")
    if not isinstance(event, SignalEvent):
        raise TypeError("advance_signal requires a SignalEvent")
    if event.event_id in {item.event_id for item in signal.history}:
        raise Phase1Error("DUPLICATE_SIGNAL_EVENT")
    previous_at = signal.history[-1].at if signal.history else signal.published_at
    _validate_transition_event(signal, event, previous_at=previous_at)
    target = _next_signal_status(signal, signal.status, event)
    return replace(signal, status=target, history=(*signal.history, event))


class ObservationKind(str, Enum):
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    BAR = "BAR"


@dataclass(frozen=True, slots=True)
class IntradayObservation:
    """One normalized cohort fact.

    ``sequence`` is authenticated normalized cohort order, not a provider's
    incomparable trade- or quote-stream sequence number.
    """

    observation_id: str
    stream_id: str
    feed: str
    kind: ObservationKind
    at: datetime
    received_at: datetime
    sequence: int | None
    fresh: bool
    trade_price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    open_price: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close_price: Decimal | None = None
    session_open: bool = False

    def __post_init__(self) -> None:
        for value in (self.observation_id, self.stream_id, self.feed):
            if type(value) is not str or not value:
                raise Phase1Error("INVALID_OBSERVATION_IDENTITY")
        if not isinstance(self.kind, ObservationKind):
            raise Phase1Error("INVALID_OBSERVATION_KIND")
        observed_at = _aware(self.at, "INVALID_OBSERVATION_TIME")
        received_at = _aware(self.received_at, "INVALID_OBSERVATION_RECEIPT_TIME")
        object.__setattr__(self, "at", observed_at)
        object.__setattr__(self, "received_at", received_at)
        if received_at < observed_at:
            raise Phase1Error("OBSERVATION_RECEIPT_PRECEDES_EVENT")
        if self.sequence is not None:
            _nonnegative_int(self.sequence, "INVALID_OBSERVATION_SEQUENCE")
        if type(self.fresh) is not bool or type(self.session_open) is not bool:
            raise Phase1Error("INVALID_OBSERVATION_FLAG")
        for attribute in (
            "trade_price",
            "bid",
            "ask",
            "open_price",
            "high",
            "low",
            "close_price",
        ):
            value = getattr(self, attribute)
            object.__setattr__(
                self,
                attribute,
                _optional_money(
                    value,
                    "INVALID_OBSERVATION_PRICE",
                    nonnegative=True,
                ),
            )
        if self.session_open and self.kind is not ObservationKind.BAR:
            raise Phase1Error("INVALID_SESSION_OPEN_OBSERVATION")
        if self.kind is ObservationKind.TRADE:
            if self.trade_price is None or self.trade_price <= _ZERO:
                raise Phase1Error("INVALID_TRADE_OBSERVATION")
            if any(
                value is not None
                for value in (
                    self.bid,
                    self.ask,
                    self.open_price,
                    self.high,
                    self.low,
                    self.close_price,
                )
            ):
                raise Phase1Error("OBSERVATION_KIND_FIELD_MISMATCH")
        elif self.kind is ObservationKind.QUOTE:
            if any(
                value is not None
                for value in (
                    self.trade_price,
                    self.open_price,
                    self.high,
                    self.low,
                    self.close_price,
                )
            ):
                raise Phase1Error("OBSERVATION_KIND_FIELD_MISMATCH")
        elif self.kind is ObservationKind.BAR:
            prices = (
                self.open_price,
                self.high,
                self.low,
                self.close_price,
            )
            if any(value is None or value <= _ZERO for value in prices):
                raise Phase1Error("INVALID_BAR_OBSERVATION")
            assert all(value is not None for value in prices)
            if (
                self.high < max(self.open_price, self.close_price)
                or self.low > min(self.open_price, self.close_price)
                or self.high < self.low
            ):
                raise Phase1Error("INVALID_BAR_OBSERVATION")
            if self.trade_price is not None:
                raise Phase1Error("OBSERVATION_KIND_FIELD_MISMATCH")


@dataclass(frozen=True, slots=True)
class PaperEntryResult:
    status: SignalStatus
    fill_price: Decimal | None
    filled_at: datetime | None
    trigger_at: datetime | None = None
    trigger_observation_id: str | None = None
    quote_observation_id: str | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, SignalStatus) or self.status not in {
            SignalStatus.NOT_TRIGGERED,
            SignalStatus.NOT_FILLED_LIMIT,
            SignalStatus.TRIGGERED_PAPER,
            SignalStatus.UNRESOLVED,
        }:
            raise Phase1Error("INVALID_PAPER_ENTRY_STATUS")
        if self.fill_price is not None:
            object.__setattr__(
                self,
                "fill_price",
                _money(self.fill_price, "INVALID_PAPER_FILL", positive=True),
            )
        for attribute in ("filled_at", "trigger_at"):
            value = getattr(self, attribute)
            if value is not None:
                object.__setattr__(
                    self,
                    attribute,
                    _aware(value, "INVALID_PAPER_ENTRY_TIME"),
                )
        for attribute in ("trigger_observation_id", "quote_observation_id"):
            value = getattr(self, attribute)
            if value is not None and (type(value) is not str or not value):
                raise Phase1Error("INVALID_PAPER_ENTRY_OBSERVATION_ID")
        if self.status is SignalStatus.NOT_TRIGGERED and any(
            value is not None
            for value in (
                self.trigger_at,
                self.trigger_observation_id,
                self.quote_observation_id,
            )
        ):
            raise Phase1Error("UNEXPECTED_PAPER_ENTRY_METADATA")
        if self.status is SignalStatus.NOT_FILLED_LIMIT and (
            self.trigger_at is None or self.trigger_observation_id is None
        ):
            raise Phase1Error("INCOMPLETE_PAPER_ENTRY_TRIGGER")
        if (
            self.status is SignalStatus.NOT_FILLED_LIMIT
            and self.quote_observation_id is not None
        ):
            raise Phase1Error("UNEXPECTED_PAPER_ENTRY_METADATA")
        if self.status is SignalStatus.UNRESOLVED:
            has_trigger_at = self.trigger_at is not None
            has_trigger_id = self.trigger_observation_id is not None
            if has_trigger_at != has_trigger_id or (
                self.quote_observation_id is not None
                and not (has_trigger_at and has_trigger_id)
            ):
                raise Phase1Error("INCOMPLETE_PAPER_ENTRY_TRIGGER")
        if self.status is SignalStatus.TRIGGERED_PAPER:
            if (
                self.fill_price is None
                or self.filled_at is None
                or self.trigger_at is None
                or not self.trigger_observation_id
                or not self.quote_observation_id
            ):
                raise Phase1Error("INCOMPLETE_PAPER_FILL")
            if self.filled_at < self.trigger_at:
                raise Phase1Error("INVALID_PAPER_ENTRY_TIME_ORDER")
        elif self.fill_price is not None or self.filled_at is not None:
            raise Phase1Error("UNEXPECTED_PAPER_FILL")
        if type(self.reason_codes) is not tuple or any(
            type(code) is not str or not code for code in self.reason_codes
        ) or len(set(self.reason_codes)) != len(self.reason_codes):
            raise Phase1Error("INVALID_PAPER_ENTRY_REASONS")
        if self.status is SignalStatus.UNRESOLVED and not self.reason_codes:
            raise Phase1Error("INCOMPLETE_PAPER_ENTRY_REASONS")
        if (
            self.status is SignalStatus.TRIGGERED_PAPER
            and self.reason_codes
        ):
            raise Phase1Error("UNEXPECTED_PAPER_ENTRY_REASONS")


def _normalized_cohort(
    observations: Sequence[IntradayObservation],
) -> tuple[tuple[IntradayObservation, ...] | None, str | None]:
    if isinstance(observations, (str, bytes)):
        raise TypeError("observations must be a sequence")
    items = tuple(observations)
    if any(not isinstance(item, IntradayObservation) for item in items):
        raise TypeError("observations must contain IntradayObservation values")
    if not items:
        return None, "MISSING_OBSERVATIONS"
    if any(not item.fresh for item in items):
        return None, "STALE_OBSERVATION"
    if any(item.sequence is None for item in items):
        return None, "MISSING_NORMALIZED_SEQUENCE"
    sequences = tuple(item.sequence for item in items)
    if len(set(sequences)) != len(sequences):
        return None, "DUPLICATE_NORMALIZED_SEQUENCE"
    if len({item.observation_id for item in items}) != len(items):
        return None, "DUPLICATE_OBSERVATION_IDENTITY"
    if len({item.feed for item in items}) != 1:
        return None, "MIXED_OBSERVATION_FEED"
    ordered = tuple(sorted(items, key=lambda item: item.sequence))  # type: ignore[arg-type]
    if tuple(item.sequence for item in ordered) != tuple(
        range(1, len(ordered) + 1)
    ):
        return None, "NONCONTIGUOUS_NORMALIZED_SEQUENCE"
    if any(
        later.at < earlier.at
        for earlier, later in zip(ordered, ordered[1:], strict=False)
    ):
        return None, "NORMALIZED_SEQUENCE_TIME_CONFLICT"
    if len({item.at.astimezone(_ET).date() for item in ordered}) != 1:
        return None, "MIXED_OBSERVATION_SESSION"
    return ordered, None


def simulate_entry(
    trigger: Decimal,
    limit: Decimal,
    observations: Sequence[IntradayObservation],
) -> PaperEntryResult:
    """Diagnose trigger/limit outcome from a complete normalized cohort."""
    trigger = _money(trigger, "INVALID_ENTRY_PRICE", positive=True)
    limit = _money(limit, "INVALID_ENTRY_PRICE", positive=True)
    if limit < trigger:
        raise Phase1Error("INVALID_ENTRY_PRICE")
    ordered, cohort_error = _normalized_cohort(observations)
    if ordered is None:
        return PaperEntryResult(
            SignalStatus.UNRESOLVED,
            None,
            None,
            reason_codes=(cohort_error or "INVALID_OBSERVATION_COHORT",),
        )
    trigger_observation: IntradayObservation | None = None
    saw_valid_quote = False
    for observation in ordered:
        clock = observation.at.astimezone(_ET).time().replace(tzinfo=None)
        if clock > _REGULAR_CLOSE:
            continue
        if trigger_observation is None:
            if (
                clock > _ENTRY_START
                and observation.kind is ObservationKind.TRADE
                and observation.trade_price is not None
                and observation.trade_price >= trigger
            ):
                trigger_observation = observation
            continue
        if observation.kind is not ObservationKind.QUOTE:
            continue
        if observation.at < trigger_observation.at:
            return PaperEntryResult(
                SignalStatus.UNRESOLVED,
                None,
                None,
                trigger_at=trigger_observation.at,
                trigger_observation_id=trigger_observation.observation_id,
                reason_codes=("POST_TRIGGER_ORDER_AMBIGUOUS",),
            )
        if (
            observation.bid is None
            or observation.ask is None
            or observation.bid <= _ZERO
            or observation.ask <= _ZERO
            or observation.ask < observation.bid
        ):
            return PaperEntryResult(
                SignalStatus.UNRESOLVED,
                None,
                None,
                trigger_at=trigger_observation.at,
                trigger_observation_id=trigger_observation.observation_id,
                reason_codes=("INVALID_POST_TRIGGER_QUOTE",),
            )
        saw_valid_quote = True
        if observation.ask <= limit:
            return PaperEntryResult(
                SignalStatus.TRIGGERED_PAPER,
                limit,
                observation.at,
                trigger_at=trigger_observation.at,
                trigger_observation_id=trigger_observation.observation_id,
                quote_observation_id=observation.observation_id,
            )
    if trigger_observation is None:
        return PaperEntryResult(SignalStatus.NOT_TRIGGERED, None, None)
    return PaperEntryResult(
        SignalStatus.NOT_FILLED_LIMIT
        if saw_valid_quote
        else SignalStatus.UNRESOLVED,
        None,
        None,
        trigger_at=trigger_observation.at,
        trigger_observation_id=trigger_observation.observation_id,
        reason_codes=()
        if saw_valid_quote
        else ("MISSING_POST_TRIGGER_QUOTE",),
    )


class ExitReason(str, Enum):
    NO_EXIT = "NO_EXIT"
    STOP = "STOP"
    TARGET = "TARGET"
    STOP_FIRST_CONSERVATIVE = "STOP_FIRST_CONSERVATIVE"
    GAP_STOP = "GAP_STOP"
    EVENT_EXIT_REQUIRED = "EVENT_EXIT_REQUIRED"
    THESIS_INVALIDATED = "THESIS_INVALIDATED"
    MAX_HOLD_SESSIONS_REACHED = "MAX_HOLD_SESSIONS_REACHED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class PaperExitResult:
    exit_reason: ExitReason
    fill_price: Decimal | None
    exited_at: datetime | None
    observation_id: str | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.exit_reason, ExitReason):
            raise Phase1Error("INVALID_PAPER_EXIT_REASON")
        if self.fill_price is not None:
            object.__setattr__(
                self,
                "fill_price",
                _money(self.fill_price, "INVALID_PAPER_EXIT_PRICE", nonnegative=True),
            )
        if self.exited_at is not None:
            object.__setattr__(
                self,
                "exited_at",
                _aware(self.exited_at, "INVALID_PAPER_EXIT_TIME"),
            )
        if self.observation_id is not None and (
            type(self.observation_id) is not str or not self.observation_id
        ):
            raise Phase1Error("INVALID_PAPER_EXIT_OBSERVATION_ID")
        is_exit = self.exit_reason not in {
            ExitReason.NO_EXIT,
            ExitReason.UNRESOLVED,
        }
        if is_exit and (
            self.fill_price is None
            or self.exited_at is None
            or not self.observation_id
        ):
            raise Phase1Error("INCOMPLETE_PAPER_EXIT")
        if not is_exit and (
            self.fill_price is not None
            or self.exited_at is not None
            or self.observation_id is not None
        ):
            raise Phase1Error("UNEXPECTED_PAPER_EXIT")
        if type(self.reason_codes) is not tuple or any(
            type(code) is not str or not code for code in self.reason_codes
        ) or len(set(self.reason_codes)) != len(self.reason_codes):
            raise Phase1Error("INVALID_PAPER_EXIT_REASONS")
        if (
            self.exit_reason is not ExitReason.UNRESOLVED
            and self.reason_codes
        ):
            raise Phase1Error("UNEXPECTED_PAPER_EXIT_REASONS")
        if self.exit_reason is ExitReason.UNRESOLVED and not self.reason_codes:
            raise Phase1Error("INCOMPLETE_PAPER_EXIT_REASONS")


def _adverse_exit_fill(
    expected: Decimal,
    bid: Decimal,
    ask: Decimal,
) -> Decimal:
    expected_micros = money_to_micros(expected)
    spread_micros = money_to_micros(ask) - money_to_micros(bid)
    percentage_slippage = (expected_micros + 999) // 1000
    spread_slippage = (spread_micros + 1) // 2
    return money_from_micros(
        max(0, expected_micros - max(percentage_slippage, spread_slippage))
    )


def simulate_exit(
    stop: Decimal,
    target: Decimal,
    observations: Sequence[IntradayObservation],
    *,
    target_enabled: bool = True,
) -> PaperExitResult:
    """Resolve conservative exit price/time ambiguity, never exit quantity.

    The Task 6 position evaluator remains the authority for partial/remainder
    quantities, trailing stops, event/thesis exits, and the ten-session action.
    The Journal adapter must combine its authenticated action with this result.
    That adapter also owns verified early-close cutoffs; this pure guard enforces
    only the standard 09:30 through 16:00 ET session boundary.
    """
    stop = _money(stop, "INVALID_EXIT_BOUNDARY", positive=True)
    target = _money(target, "INVALID_EXIT_BOUNDARY", positive=True)
    if target <= stop or type(target_enabled) is not bool:
        raise Phase1Error("INVALID_EXIT_BOUNDARY")
    ordered, cohort_error = _normalized_cohort(observations)
    if ordered is None:
        return PaperExitResult(
            ExitReason.UNRESOLVED,
            None,
            None,
            reason_codes=(cohort_error or "INVALID_OBSERVATION_COHORT",),
        )
    for observation in ordered:
        if observation.kind is not ObservationKind.BAR:
            continue
        clock = observation.at.astimezone(_ET).time().replace(tzinfo=None)
        if not _REGULAR_OPEN <= clock <= _REGULAR_CLOSE:
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("EXIT_BAR_OUTSIDE_REGULAR_SESSION",),
            )
        if observation.session_open and clock != _REGULAR_OPEN:
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("INVALID_SESSION_OPEN_BAR_TIME",),
            )
        assert observation.open_price is not None
        assert observation.high is not None
        assert observation.low is not None
        gap_stop = observation.session_open and observation.open_price <= stop
        hit_stop = observation.low <= stop
        hit_target = target_enabled and observation.high >= target
        if not (gap_stop or hit_stop or hit_target):
            continue
        if (
            observation.bid is None
            or observation.ask is None
            or observation.bid <= _ZERO
            or observation.ask <= _ZERO
            or observation.ask < observation.bid
        ):
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("INVALID_EXIT_SPREAD",),
            )
        if gap_stop:
            reason = ExitReason.GAP_STOP
            expected = observation.open_price
        elif hit_stop and hit_target:
            reason = ExitReason.STOP_FIRST_CONSERVATIVE
            expected = stop
        elif hit_stop:
            reason = ExitReason.STOP
            expected = stop
        else:
            reason = ExitReason.TARGET
            expected = target
        return PaperExitResult(
            reason,
            _adverse_exit_fill(expected, observation.bid, observation.ask),
            observation.at,
            observation.observation_id,
        )
    return PaperExitResult(ExitReason.NO_EXIT, None, None)


def simulate_forced_exit(
    expected_exit: Decimal,
    observations: Sequence[IntradayObservation],
    *,
    exit_reason: ExitReason,
    triggered_at: datetime,
) -> PaperExitResult:
    """Price one non-boundary exit from the first authenticated eligible bar."""
    expected_exit = _money(
        expected_exit,
        "INVALID_FORCED_EXIT_PRICE",
        positive=True,
    )
    if exit_reason not in {
        ExitReason.EVENT_EXIT_REQUIRED,
        ExitReason.THESIS_INVALIDATED,
        ExitReason.MAX_HOLD_SESSIONS_REACHED,
    }:
        raise Phase1Error("INVALID_FORCED_EXIT_REASON")
    triggered_at = _aware(triggered_at, "INVALID_FORCED_EXIT_TIME")
    ordered, cohort_error = _normalized_cohort(observations)
    if ordered is None:
        return PaperExitResult(
            ExitReason.UNRESOLVED,
            None,
            None,
            reason_codes=(cohort_error or "INVALID_OBSERVATION_COHORT",),
        )
    for observation in ordered:
        if (
            observation.kind is not ObservationKind.BAR
            or observation.at < triggered_at
        ):
            continue
        clock = observation.at.astimezone(_ET).time().replace(tzinfo=None)
        if not _REGULAR_OPEN <= clock <= _REGULAR_CLOSE:
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("EXIT_BAR_OUTSIDE_REGULAR_SESSION",),
            )
        assert observation.low is not None
        assert observation.high is not None
        if not observation.low <= expected_exit <= observation.high:
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("FORCED_EXIT_PRICE_OUTSIDE_BAR",),
            )
        if (
            observation.bid is None
            or observation.ask is None
            or observation.bid <= _ZERO
            or observation.ask <= _ZERO
            or observation.ask < observation.bid
        ):
            return PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                reason_codes=("INVALID_EXIT_SPREAD",),
            )
        return PaperExitResult(
            exit_reason,
            _adverse_exit_fill(expected_exit, observation.bid, observation.ask),
            observation.at,
            observation.observation_id,
        )
    return PaperExitResult(
        ExitReason.UNRESOLVED,
        None,
        None,
        reason_codes=("MISSING_FORCED_EXIT_BAR",),
    )


@dataclass(frozen=True, slots=True)
class PaperPosition:
    signal_id: str
    symbol: str
    ledger_name: str
    shares: int

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise Phase1Error("INVALID_PAPER_POSITION")
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.upper()
        ):
            raise Phase1Error("INVALID_PAPER_POSITION")
        if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
            raise Phase1Error("INVALID_EQUITY_LEDGER")
        _positive_int(self.shares, "INVALID_PAPER_POSITION_SHARES")


@dataclass(frozen=True, slots=True)
class EquityMark:
    at: datetime
    bid: Decimal | None
    ask: Decimal | None
    completed_close: Decimal | None
    fresh: bool = True
    consolidated: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "at", _aware(self.at, "INVALID_EQUITY_MARK_TIME"))
        for attribute in ("bid", "ask", "completed_close"):
            object.__setattr__(
                self,
                attribute,
                _optional_money(
                    getattr(self, attribute),
                    "INVALID_EQUITY_MONEY",
                    nonnegative=True,
                ),
            )
        if type(self.fresh) is not bool or type(self.consolidated) is not bool:
            raise Phase1Error("INVALID_EQUITY_MARK_FLAG")


@dataclass(frozen=True, slots=True)
class EquityPoint:
    ledger_name: str
    at: datetime | None
    cash: Decimal
    positions_value: Decimal
    equity: Decimal
    external_cash_flow: Decimal = Decimal("0")
    mark_sources: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.ledger_name not in {"CANONICAL", "ACTUAL"}:
            raise Phase1Error("INVALID_EQUITY_LEDGER")
        if self.at is not None:
            object.__setattr__(self, "at", _aware(self.at, "INVALID_EQUITY_TIME"))
        for attribute in ("cash", "equity"):
            object.__setattr__(
                self,
                attribute,
                _money(
                    getattr(self, attribute),
                    "INVALID_EQUITY_MONEY",
                    nonnegative=self.ledger_name == "CANONICAL",
                ),
            )
        object.__setattr__(
            self,
            "positions_value",
            _money(
                self.positions_value,
                "INVALID_EQUITY_MONEY",
                nonnegative=True,
            ),
        )
        object.__setattr__(
            self,
            "external_cash_flow",
            _money(self.external_cash_flow, "INVALID_EQUITY_MONEY"),
        )
        if money_to_micros(self.cash) + money_to_micros(
            self.positions_value
        ) != money_to_micros(self.equity):
            raise Phase1Error("EQUITY_COMPONENT_MISMATCH")
        if type(self.mark_sources) is not tuple or any(
            type(item) is not tuple
            or len(item) != 2
            or any(type(part) is not str or not part for part in item)
            for item in self.mark_sources
        ):
            raise Phase1Error("INVALID_EQUITY_MARK_SOURCES")


def _mark_price(mark: EquityMark) -> tuple[int, str]:
    if (
        mark.fresh
        and mark.consolidated
        and mark.bid is not None
        and mark.ask is not None
        and mark.bid > _ZERO
        and mark.ask > _ZERO
        and mark.ask >= mark.bid
    ):
        return money_to_micros(mark.bid), "CONSOLIDATED_BID"
    if mark.completed_close is None or mark.completed_close <= _ZERO:
        raise Phase1Error("MISSING_CONSERVATIVE_MARK")
    close_micros = money_to_micros(mark.completed_close)
    return (close_micros * 999) // 1000, "CLOSE_MINUS_0.10_PERCENT"


def mark_equity(
    cash: Decimal,
    positions: Sequence[PaperPosition],
    marks: Mapping[str, EquityMark],
    *,
    ledger_name: str | None = None,
    at: datetime | None = None,
    external_cash_flow: Decimal = Decimal("0"),
) -> EquityPoint:
    """Mark strategy cash and open positions without account-wide cash flows."""
    cash = _money(cash, "INVALID_EQUITY_MONEY")
    external_cash_flow = _money(
        external_cash_flow,
        "INVALID_EQUITY_MONEY",
    )
    if isinstance(positions, (str, bytes)):
        raise TypeError("positions must be a sequence")
    copied_positions = tuple(positions)
    if any(not isinstance(position, PaperPosition) for position in copied_positions):
        raise TypeError("positions must contain PaperPosition values")
    if not isinstance(marks, Mapping) or any(
        type(symbol) is not str or not isinstance(mark, EquityMark)
        for symbol, mark in marks.items()
    ):
        raise TypeError("marks must map symbols to EquityMark values")
    ledgers = {position.ledger_name for position in copied_positions}
    if len(ledgers) > 1:
        raise Phase1Error("MIXED_EQUITY_LEDGERS")
    inferred_ledger = next(iter(ledgers), "CANONICAL")
    effective_ledger = inferred_ledger if ledger_name is None else ledger_name
    if effective_ledger not in {"CANONICAL", "ACTUAL"}:
        raise Phase1Error("INVALID_EQUITY_LEDGER")
    if ledgers and effective_ledger != inferred_ledger:
        raise Phase1Error("MIXED_EQUITY_LEDGERS")
    if effective_ledger == "CANONICAL" and cash < _ZERO:
        raise Phase1Error("INVALID_EQUITY_MONEY")
    if any(position.symbol not in marks for position in copied_positions):
        raise Phase1Error("MISSING_POSITION_MARK")
    if at is None:
        if copied_positions:
            at = max(marks[position.symbol].at for position in copied_positions)
    if at is not None:
        at = _aware(at, "INVALID_EQUITY_TIME")
    position_micros = 0
    mark_sources: list[tuple[str, str]] = []
    shares_by_symbol: dict[str, int] = {}
    for position in copied_positions:
        shares_by_symbol[position.symbol] = (
            shares_by_symbol.get(position.symbol, 0) + position.shares
        )
    for symbol in sorted(shares_by_symbol):
        mark = marks.get(symbol)
        if mark is None:
            raise Phase1Error("MISSING_POSITION_MARK")
        if at is not None and mark.at > at:
            raise Phase1Error("EQUITY_MARK_AFTER_POINT")
        if (
            at is not None
            and mark.at.astimezone(_ET).date()
            != at.astimezone(_ET).date()
        ):
            raise Phase1Error("EQUITY_MARK_SESSION_MISMATCH")
        price_micros, source = _mark_price(mark)
        extended = price_micros * shares_by_symbol[symbol]
        if extended > MAX_MICRODOLLARS:
            raise Phase1Error("EQUITY_OVERFLOW")
        position_micros += extended
        if position_micros > MAX_MICRODOLLARS:
            raise Phase1Error("EQUITY_OVERFLOW")
        mark_sources.append((symbol, source))
    equity_micros = money_to_micros(cash) + position_micros
    if not MIN_MICRODOLLARS <= equity_micros <= MAX_MICRODOLLARS:
        raise Phase1Error("EQUITY_OVERFLOW")
    return EquityPoint(
        ledger_name=effective_ledger,
        at=at,
        cash=cash,
        positions_value=money_from_micros(position_micros),
        equity=money_from_micros(equity_micros),
        external_cash_flow=external_cash_flow,
        mark_sources=tuple(mark_sources),
    )


def max_drawdown(points: Sequence[EquityPoint]) -> Decimal:
    """Return peak-to-trough drawdown after excluding external cash flows."""
    if isinstance(points, (str, bytes)):
        raise TypeError("points must be a sequence")
    curve = tuple(points)
    if any(not isinstance(point, EquityPoint) for point in curve):
        raise TypeError("points must contain EquityPoint values")
    if not curve:
        return money_from_micros(0)
    if len({point.ledger_name for point in curve}) != 1:
        raise Phase1Error("MIXED_EQUITY_LEDGERS")
    timestamps = tuple(point.at for point in curve)
    if any(value is None for value in timestamps) and any(
        value is not None for value in timestamps
    ):
        raise Phase1Error("EQUITY_TIME_ORDER")
    if all(value is not None for value in timestamps) and any(
        later <= earlier
        for earlier, later in zip(timestamps, timestamps[1:], strict=False)
    ):
        raise Phase1Error("EQUITY_TIME_ORDER")
    cumulative_flow = 0
    high_water: int | None = None
    maximum_drawdown = 0
    for point in curve:
        cumulative_flow += money_to_micros(point.external_cash_flow)
        adjusted = money_to_micros(point.equity) - cumulative_flow
        if not MIN_MICRODOLLARS <= adjusted <= MAX_MICRODOLLARS:
            raise Phase1Error("EQUITY_OVERFLOW")
        high_water = adjusted if high_water is None else max(high_water, adjusted)
        maximum_drawdown = max(maximum_drawdown, high_water - adjusted)
    if maximum_drawdown > MAX_MICRODOLLARS:
        raise Phase1Error("EQUITY_OVERFLOW")
    return money_from_micros(maximum_drawdown)


__all__ = [
    "EquityMark",
    "EquityPoint",
    "ExitReason",
    "IntradayObservation",
    "ObservationKind",
    "PaperEntryResult",
    "PaperExitResult",
    "PaperPosition",
    "Phase1Error",
    "Signal",
    "SignalEvent",
    "SignalEventKind",
    "SignalExpiryDeadlineEvidence",
    "SignalStatus",
    "advance_signal",
    "mark_equity",
    "max_drawdown",
    "simulate_entry",
    "simulate_exit",
]
