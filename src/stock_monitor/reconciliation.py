"""Pure confirmation assessment and Task 7 actual-ledger transition contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import Enum
from sys import _getframe
from threading import RLock
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from weakref import ReferenceType, WeakKeyDictionary, ref
from zoneinfo import ZoneInfo

from . import journal as _journal_authority_module
from . import market_calendar as _market_calendar_authority_module
from .confirmations import (
    ConfirmationEnvelope,
    ConfirmationKind,
    ParsedConfirmation,
    PendingConfirmation,
    parse_confirmation_batch_or_pending,
)
from .domain import (
    MAX_MICRODOLLARS,
    MIN_MICRODOLLARS,
    money_to_micros,
    stable_execution_event_identity,
)
from .journal import (
    IdempotencyConflict,
    Journal,
    JournalActionSource,
    JournalActualReplaySource,
    JournalPostingSource,
    StoredIngestionResult,
    is_verified_journal_action_source,
    is_verified_journal_replay_source,
)
from .market_calendar import MarketCalendar
from .policy import Policy
from .risk import (
    RiskBlock,
    SessionCalendarResolver,
    _calendar_digest,
    _policy_digest,
)

if TYPE_CHECKING:
    from .ledger import LedgerSignal


_ET = ZoneInfo("America/New_York")
_MAX_SPREAD = Decimal("0.0025")
_ACTUAL_PROJECTION_DOMAIN_KINDS = frozenset(
    {
        "ACCOUNT_CHECK",
        "BOUGHT",
        "FEE",
        "PARTIAL_FILL",
        "RECONCILE_CASH",
        "RECONCILE_PENDING_ORDERS",
        "RECONCILE_UNRELATED_POSITION",
        "SOLD",
        "STOP_FILLED",
        "STOP_UPDATED",
    }
)
_ACCOUNT_VALUE_DOMAIN_KINDS = frozenset({"ACCOUNT_CHECK", "RECONCILE_CASH"})


class ActionStatus(str, Enum):
    COMPLIANT = "COMPLIANT"
    NONCOMPLIANT_RECONCILIATION_REQUIRED = (
        "NONCOMPLIANT_RECONCILIATION_REQUIRED"
    )
    PENDING_CLARIFICATION = "PENDING_CLARIFICATION"


@dataclass(frozen=True, slots=True)
class ResolvedSignalPlan:
    signal: LedgerSignal
    report_id: str
    publication_rank: int
    publication_source_digest: str

    def __post_init__(self) -> None:
        from .ledger import LedgerSignal

        if type(self.signal) is not LedgerSignal:
            raise ValueError("INVALID_RESOLVED_SIGNAL_PLAN")
        if type(self.report_id) is not str or not self.report_id:
            raise ValueError("INVALID_RESOLVED_SIGNAL_PLAN")
        _require_int(self.publication_rank, minimum=1)
        if (
            type(self.publication_source_digest) is not str
            or len(self.publication_source_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.publication_source_digest
            )
        ):
            raise ValueError("INVALID_RESOLVED_SIGNAL_PLAN")


@runtime_checkable
class SignalPlanResolver(Protocol):
    """Resolve only an exact in-process Task 5/6 plan for one session."""

    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> ResolvedSignalPlan | None:
        ...


@dataclass(frozen=True, slots=True)
class UnavailableSignalPlanResolver:
    """Production-safe default until Task 8 persists structured signals."""

    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> None:
        del symbol, economic_at, query_cutoff
        return None


@runtime_checkable
class ActualEntryAuthorityResolver(Protocol):
    """Task 8-backed authority lookup; unavailable during Task 7 activation."""

    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> object | None:
        ...


@dataclass(frozen=True, slots=True)
class UnavailableActualEntryAuthorityResolver:
    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> None:
        del symbol, economic_at, query_cutoff
        return None


@dataclass(frozen=True, slots=True)
class ProjectionPosition:
    signal_id: str
    symbol: str
    shares: int
    cost_basis_micros: int
    recommended_stop_micros: int | None
    user_confirmed_stop_micros: int | None
    target_micros: int | None
    parent_order_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.signal_id) is not str or not self.signal_id:
            raise ValueError("INVALID_POSITION_SIGNAL_ID")
        if type(self.symbol) is not str or not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("INVALID_POSITION_SYMBOL")
        _require_int(self.shares, minimum=0)
        _require_int(self.cost_basis_micros, minimum=0)
        for value in (
            self.recommended_stop_micros,
            self.user_confirmed_stop_micros,
            self.target_micros,
        ):
            if value is not None:
                _require_int(value, minimum=1)
        if type(self.parent_order_id) not in {str, type(None)}:
            raise ValueError("INVALID_PARENT_ORDER_ID")
        if self.parent_order_id == "":
            raise ValueError("INVALID_PARENT_ORDER_ID")


@dataclass(frozen=True, slots=True)
class ProjectionState:
    """Immutable assessment input; never a persistence authority."""

    positions: tuple[ProjectionPosition, ...] = ()
    reconciliation_required: bool = False
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        positions = tuple(self.positions)
        if any(not isinstance(position, ProjectionPosition) for position in positions):
            raise ValueError("INVALID_PROJECTION_STATE")
        if len(positions) != len({position.signal_id for position in positions}):
            raise ValueError("DUPLICATE_POSITION_SIGNAL")
        if type(self.reconciliation_required) is not bool:
            raise ValueError("INVALID_PROJECTION_STATE")
        reasons = _reason_codes(self.reason_codes)
        if self.reconciliation_required != bool(reasons):
            raise ValueError("INVALID_PROJECTION_STATE")
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class ActualLot:
    source_event_id: str
    source_cursor: int
    remaining_shares: int
    unit_cost_micros: int
    acquired_at: datetime
    received_at: datetime
    parent_order_id: str | None = None


@dataclass(frozen=True, slots=True)
class ActualPositionState:
    signal_id: str
    symbol: str
    lineage_kind: str
    signal_digest: str | None
    lots: tuple[ActualLot, ...]
    recommended_stop_micros: int | None
    user_stop_micros: int | None
    target_micros: int | None
    tick_micros: int | None
    cumulative_buy_cost_micros: int
    cumulative_sale_proceeds_micros: int
    linked_fees_micros: int
    reason_codes: tuple[str, ...]
    lifecycle_event_ids: tuple[str, ...] = ()

    @property
    def shares(self) -> int:
        return sum(lot.remaining_shares for lot in self.lots)

    @property
    def cost_basis_micros(self) -> int:
        return sum(
            lot.remaining_shares * lot.unit_cost_micros for lot in self.lots
        )


@dataclass(frozen=True, slots=True)
class ActualClosedTrade:
    signal_id: str
    symbol: str
    opened_at: datetime
    closed_at: datetime
    source_cursor: int
    source_ordinal: int
    buy_cost_micros: int
    gross_sale_micros: int
    fees_micros: int
    pnl_micros: int
    source_event_ids: tuple[str, ...]
    source_digest: str


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualLedgerState:
    query_cutoff: datetime
    through_cursor: int | None
    positions: tuple[ActualPositionState, ...]
    closed_trades: tuple[ActualClosedTrade, ...]
    settlement_ledger: tuple[JournalPostingSource, ...]
    strategy_settled_cash_micros: int
    user_confirmed_cash_micros: int | None
    reconciliation_reasons: tuple[str, ...]
    source_digest: str
    cache_matches_replay: bool
    calendar_digest: str | None = None
    calendar_release_verified: bool = False
    journal_source_digest: str | None = None
    policy_digest: str | None = None


@dataclass(frozen=True, slots=True)
class PostingIntent:
    posting_key: str
    account_name: str
    entry_kind: str
    amount_micros: int
    shares_delta: int | None
    unit_price_micros: int | None
    occurred_at: datetime
    details: tuple[tuple[str, object], ...]


@dataclass(frozen=True, slots=True)
class ActualTransition:
    before_digest: str
    source_event_id: str
    decision: ActionAssessment
    posting_intents: tuple[PostingIntent, ...]
    after_state: ActualLedgerState
    projection_authority_kind: str
    state_digest: str


_ACTUAL_STATE_AUTHORITY_LOCK = RLock()
_ACTUAL_STATE_AUTHORITIES: dict[
    int,
    tuple[object, ...],
] = {}
_ACTUAL_LEDGER_STATE_FINGERPRINT_DOMAIN = (
    b"stock-monitor/actual-ledger-state/v1"
)


@dataclass(frozen=True, slots=True)
class _IncrementalIngestionCheckpoint:
    owner: ReferenceType[object]
    source_generation: int
    total_changes: int
    data_version: int
    calendar_digest: str
    policy_digest: str
    query_cutoff: datetime
    terminal_cursor: int
    state: ActualLedgerState
    cache_economic_highwater: datetime | None
    account_value_effective_at: datetime | None
    stop_effective_at: tuple[tuple[str, datetime], ...]


_INGESTION_CHECKPOINT_LOCK = RLock()
_INGESTION_CHECKPOINTS: WeakKeyDictionary[
    Journal,
    tuple[object, ...],
] = WeakKeyDictionary()


def _ingestion_checkpoint_fingerprint(
    checkpoint: _IncrementalIngestionCheckpoint,
) -> tuple[object, ...]:
    return (
        checkpoint.source_generation,
        checkpoint.total_changes,
        checkpoint.data_version,
        checkpoint.calendar_digest,
        checkpoint.policy_digest,
        checkpoint.query_cutoff,
        checkpoint.terminal_cursor,
        _actual_state_digest(checkpoint.state),
        checkpoint.state.source_digest,
        checkpoint.cache_economic_highwater,
        checkpoint.account_value_effective_at,
        checkpoint.stop_effective_at,
    )


def _load_incremental_ingestion_checkpoint(
    journal: Journal,
    *,
    identity: tuple[int, int, int | None],
    query_cutoff: datetime,
    calendar: SessionCalendarResolver,
    policy: Policy,
) -> _IncrementalIngestionCheckpoint | None:
    with _INGESTION_CHECKPOINT_LOCK:
        issued = _INGESTION_CHECKPOINTS.get(journal)
    if issued is None:
        return None
    checkpoint, fingerprint = issued
    try:
        if (
            checkpoint.owner() is not journal
            or fingerprint != _ingestion_checkpoint_fingerprint(checkpoint)
            or identity
            != (
                checkpoint.source_generation,
                checkpoint.data_version,
                checkpoint.terminal_cursor,
            )
            or checkpoint.calendar_digest != _calendar_digest(calendar)
            or checkpoint.policy_digest != _policy_digest(policy)
            or checkpoint.state.calendar_digest != checkpoint.calendar_digest
            or checkpoint.state.policy_digest != checkpoint.policy_digest
            or checkpoint.state.through_cursor != checkpoint.terminal_cursor
            or query_cutoff < checkpoint.query_cutoff
            or query_cutoff.astimezone(_ET).date()
            != checkpoint.query_cutoff.astimezone(_ET).date()
        ):
            return None
    except Exception:
        return None
    return checkpoint


def _store_incremental_ingestion_checkpoint(
    journal: Journal,
    checkpoint: _IncrementalIngestionCheckpoint,
) -> None:
    del journal, checkpoint
    raise RiskBlock("INCREMENTAL_INGESTION_CHECKPOINT_ISSUER_UNVERIFIED")


def _make_incremental_ingestion_checkpoint_api(
    *,
    trusted_globals: dict[str, object],
    issuer_function: object,
    frame_getter: object,
    checkpoint_type: type[_IncrementalIngestionCheckpoint],
    journal_type: type[Journal],
    state_type: type[ActualLedgerState],
    calendar_type: type[SessionCalendarResolver],
    policy_type: type[Policy],
    policy_validate: object,
    private_state_builder: object,
    state_digest: object,
    calendar_digest: object,
    policy_digest: object,
    journal_data_version: object,
    checkpoint_lock: RLock,
    registry: WeakKeyDictionary[Journal, tuple[object, ...]],
    seal_factory: object,
    seal_equal: object,
    opaque_structural_mode: str,
) -> tuple[object, object]:
    """Capture the only checkpoint loader/mint path behind one issuer frame."""

    required = (
        issuer_function,
        frame_getter,
        policy_validate,
        private_state_builder,
        state_digest,
        calendar_digest,
        policy_digest,
        journal_data_version,
        seal_factory,
        seal_equal,
    )
    if any(not callable(value) for value in required):
        raise TypeError("incremental checkpoint dependency is invalid")

    function_type = type(_make_incremental_ingestion_checkpoint_api)
    issuer_code = issuer_function.__code__
    missing = object()
    bindings: list[tuple[dict[str, object], str, object]] = []
    classes: list[tuple[type[object], tuple[tuple[str, object], ...]]] = []
    seen_bindings: set[tuple[int, str]] = set()
    seen_functions: set[int] = set()
    seen_classes: set[int] = set()
    pending = [
        issuer_function,
        private_state_builder,
        state_digest,
        calendar_digest,
        policy_digest,
        policy_validate,
        journal_data_version,
        seal_factory,
        seal_equal,
    ]
    while pending:
        dependency = pending.pop()
        if type(dependency) is not function_type or id(dependency) in seen_functions:
            continue
        seen_functions.add(id(dependency))
        namespace = dependency.__globals__
        for name in dependency.__code__.co_names:
            if name not in namespace:
                continue
            value = namespace[name]
            key = (id(namespace), name)
            if key not in seen_bindings:
                seen_bindings.add(key)
                bindings.append((namespace, name, value))
            if (
                type(value) is function_type
                and type(value.__module__) is str
                and value.__module__.startswith("stock_monitor.")
            ):
                pending.append(value)
            elif (
                type(value) is type
                and value is not journal_type
                and type(value.__module__) is str
                and value.__module__.startswith("stock_monitor.")
                and id(value) not in seen_classes
            ):
                seen_classes.add(id(value))
                classes.append((value, tuple(vars(value).items())))
    bindings.extend(
        (
            (trusted_globals, "_private_incremental_state", private_state_builder),
            (trusted_globals, "_actual_state_digest", state_digest),
            (trusted_globals, "_calendar_digest", calendar_digest),
            (trusted_globals, "_policy_digest", policy_digest),
        )
    )
    binding_manifest = tuple(bindings)
    class_manifest = tuple(classes)
    checkpoint_domain = b"stock-monitor/incremental-ingestion-checkpoint/v2"
    object_getattribute = object.__getattribute__
    weak_reference_type = ReferenceType
    timezone_type = type(UTC)
    zoneinfo_type = ZoneInfo

    def dependencies_are_current() -> bool:
        if any(
            namespace.get(name, missing) is not expected
            for namespace, name, expected in binding_manifest
        ):
            return False
        for dependency_type, expected_items in class_manifest:
            current = vars(dependency_type)
            if len(current) != len(expected_items) or any(
                current.get(name, missing) is not expected
                for name, expected in expected_items
            ):
                return False
        return True

    def timestamp_parts(value: object) -> tuple[int, ...] | None:
        if type(value) is not datetime:
            return None
        timezone = object_getattribute(value, "tzinfo")
        if type(timezone) not in {timezone_type, zoneinfo_type}:
            return None
        try:
            utc_value = datetime.astimezone(value, UTC)
            local_value = datetime.astimezone(value, _ET)
        except Exception:
            return None
        return (
            object_getattribute(utc_value, "year"),
            object_getattribute(utc_value, "month"),
            object_getattribute(utc_value, "day"),
            object_getattribute(utc_value, "hour"),
            object_getattribute(utc_value, "minute"),
            object_getattribute(utc_value, "second"),
            object_getattribute(utc_value, "microsecond"),
            object_getattribute(local_value, "year"),
            object_getattribute(local_value, "month"),
            object_getattribute(local_value, "day"),
        )

    def connection_total_changes(journal: Journal) -> int | None:
        try:
            connection = object_getattribute(journal, "_connection")
            if type(connection) is not sqlite3.Connection:
                return None
            total_changes = object_getattribute(connection, "total_changes")
        except Exception:
            return None
        if type(total_changes) is not int or total_changes < 0:
            return None
        return total_changes

    def fingerprint(checkpoint: _IncrementalIngestionCheckpoint) -> object:
        return seal_factory(  # type: ignore[operator]
            checkpoint,
            domain=checkpoint_domain,
            root_mode=opaque_structural_mode,
        )

    def checkpoint_shape_is_exact(
        checkpoint: object,
        *,
        journal: Journal,
    ) -> bool:
        if type(checkpoint) is not checkpoint_type:
            return False
        owner = object_getattribute(checkpoint, "owner")
        if type(owner) is not weak_reference_type or owner() is not journal:
            return False
        scalar_fields = (
            object_getattribute(checkpoint, "source_generation"),
            object_getattribute(checkpoint, "total_changes"),
            object_getattribute(checkpoint, "data_version"),
            object_getattribute(checkpoint, "terminal_cursor"),
        )
        if any(type(value) is not int or value < 0 for value in scalar_fields):
            return False
        if scalar_fields[-1] < 1:
            return False
        for name in ("calendar_digest", "policy_digest"):
            digest = object_getattribute(checkpoint, name)
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                return False
        if type(object_getattribute(checkpoint, "state")) is not state_type:
            return False
        if timestamp_parts(object_getattribute(checkpoint, "query_cutoff")) is None:
            return False
        for name in ("cache_economic_highwater", "account_value_effective_at"):
            value = object_getattribute(checkpoint, name)
            if value is not None and timestamp_parts(value) is None:
                return False
        stop_effective = object_getattribute(checkpoint, "stop_effective_at")
        if type(stop_effective) is not tuple:
            return False
        return all(
            type(item) is tuple
            and len(item) == 2
            and type(item[0]) is str
            and bool(item[0])
            and timestamp_parts(item[1]) is not None
            for item in stop_effective
        )

    def loader(
        journal: Journal,
        *,
        identity: tuple[int, int, int | None],
        query_cutoff: datetime,
        calendar: SessionCalendarResolver,
        policy: Policy,
    ) -> _IncrementalIngestionCheckpoint | None:
        if (
            type(journal) is not journal_type
            or type(identity) is not tuple
            or len(identity) != 3
            or type(identity[0]) is not int
            or type(identity[1]) is not int
            or type(identity[2]) not in {int, type(None)}
            or timestamp_parts(query_cutoff) is None
            or not isinstance(calendar, calendar_type)
            or type(policy) is not policy_type
            or not dependencies_are_current()
        ):
            return None
        try:
            policy_validate(policy)  # type: ignore[operator]
            current_calendar_digest = calendar_digest(calendar)  # type: ignore[operator]
            current_policy_digest = policy_digest(policy)  # type: ignore[operator]
            current_data_version = journal_data_version(journal)  # type: ignore[operator]
        except Exception:
            return None
        current_total_changes = connection_total_changes(journal)
        if (
            current_total_changes is None
            or type(current_data_version) is not int
            or current_data_version != identity[1]
            or type(current_calendar_digest) is not str
            or type(current_policy_digest) is not str
            or not dependencies_are_current()
        ):
            return None
        with checkpoint_lock:
            issued = registry.get(journal)
        if type(issued) is not tuple or len(issued) != 8:
            return None
        checkpoint, expected_seal = issued[0], issued[1]
        if not checkpoint_shape_is_exact(checkpoint, journal=journal):
            return None
        checkpoint_cutoff = timestamp_parts(checkpoint.query_cutoff)
        requested_cutoff = timestamp_parts(query_cutoff)
        if checkpoint_cutoff is None or requested_cutoff is None:
            return None
        state = checkpoint.state
        try:
            if (
                issued[2]() is not journal
                or issued[3] != current_total_changes
                or issued[4] != identity[1]
                or issued[5] != identity[2]
                or issued[6] != current_calendar_digest
                or issued[7] != current_policy_digest
                or checkpoint.total_changes != current_total_changes
                or checkpoint.data_version != identity[1]
                or checkpoint.terminal_cursor != identity[2]
                or checkpoint.calendar_digest != current_calendar_digest
                or checkpoint.policy_digest != current_policy_digest
                or state.calendar_digest != checkpoint.calendar_digest
                or state.policy_digest != checkpoint.policy_digest
                or state.through_cursor != checkpoint.terminal_cursor
                or state.query_cutoff is not checkpoint.query_cutoff
                or type(state.settlement_ledger) is not tuple
                or len(state.settlement_ledger) != 0
                or state.journal_source_digest is not None
                or state.cache_matches_replay is not False
                or type(state.source_digest) is not str
                or state_digest(state) != state.source_digest  # type: ignore[operator]
                or requested_cutoff[:7] < checkpoint_cutoff[:7]
                or requested_cutoff[7:] != checkpoint_cutoff[7:]
            ):
                return None
            current_seal = fingerprint(checkpoint)
        except Exception:
            return None
        if (
            not dependencies_are_current()
            or connection_total_changes(journal) != current_total_changes
            or not seal_equal(expected_seal, current_seal)  # type: ignore[operator]
        ):
            return None
        with checkpoint_lock:
            current = registry.get(journal)
            return checkpoint if current is issued else None

    def installer(
        journal: Journal,
        *,
        identity: tuple[int, int, int | None],
        query_cutoff: datetime,
        terminal_cursor: int,
        actual_state: ActualLedgerState,
        calendar: SessionCalendarResolver,
        policy: Policy,
        cache_economic_highwater: datetime | None,
        account_value_effective_at: datetime | None,
        stop_effective_at: tuple[tuple[str, datetime], ...],
    ) -> None:
        caller = frame_getter(1)  # type: ignore[operator]
        if caller.f_code is not issuer_code or caller.f_globals is not trusted_globals:
            raise RiskBlock("INCREMENTAL_INGESTION_CHECKPOINT_ISSUER_UNVERIFIED")
        if (
            type(journal) is not journal_type
            or type(identity) is not tuple
            or len(identity) != 3
            or type(identity[0]) is not int
            or type(identity[1]) is not int
            or type(identity[2]) is not int
            or type(terminal_cursor) is not int
            or terminal_cursor < 1
            or identity[2] != terminal_cursor
            or type(actual_state) is not state_type
            or timestamp_parts(query_cutoff) is None
            or not isinstance(calendar, calendar_type)
            or type(policy) is not policy_type
            or type(stop_effective_at) is not tuple
            or not dependencies_are_current()
        ):
            raise ValueError("INCREMENTAL_INGESTION_CHECKPOINT_UNVERIFIED")
        policy_validate(policy)  # type: ignore[operator]
        current_calendar_digest = calendar_digest(calendar)  # type: ignore[operator]
        current_policy_digest = policy_digest(policy)  # type: ignore[operator]
        checkpoint_state = private_state_builder(  # type: ignore[operator]
            actual_state,
            query_cutoff=query_cutoff,
            through_cursor=terminal_cursor,
        )
        if (
            type(checkpoint_state) is not state_type
            or checkpoint_state.query_cutoff is not query_cutoff
            or checkpoint_state.through_cursor != terminal_cursor
            or checkpoint_state.calendar_digest != current_calendar_digest
            or checkpoint_state.policy_digest != current_policy_digest
            or type(checkpoint_state.settlement_ledger) is not tuple
            or len(checkpoint_state.settlement_ledger) != 0
            or checkpoint_state.journal_source_digest is not None
            or checkpoint_state.cache_matches_replay is not False
            or type(checkpoint_state.source_digest) is not str
            or state_digest(checkpoint_state) != checkpoint_state.source_digest  # type: ignore[operator]
            or not dependencies_are_current()
        ):
            raise ValueError("INCREMENTAL_INGESTION_CHECKPOINT_UNVERIFIED")
        current_data_version = journal_data_version(journal)  # type: ignore[operator]
        current_total_changes = connection_total_changes(journal)
        if (
            type(current_data_version) is not int
            or current_data_version != identity[1]
            or current_total_changes is None
        ):
            raise ValueError("INCREMENTAL_INGESTION_CHECKPOINT_STALE")
        checkpoint = checkpoint_type(
            owner=ref(journal),
            source_generation=identity[0],
            total_changes=current_total_changes,
            data_version=identity[1],
            calendar_digest=current_calendar_digest,
            policy_digest=current_policy_digest,
            query_cutoff=query_cutoff,
            terminal_cursor=terminal_cursor,
            state=checkpoint_state,
            cache_economic_highwater=cache_economic_highwater,
            account_value_effective_at=account_value_effective_at,
            stop_effective_at=stop_effective_at,
        )
        if not checkpoint_shape_is_exact(checkpoint, journal=journal):
            raise ValueError("INCREMENTAL_INGESTION_CHECKPOINT_UNVERIFIED")
        checkpoint_seal = fingerprint(checkpoint)
        issued = (
            checkpoint,
            checkpoint_seal,
            ref(journal),
            current_total_changes,
            identity[1],
            terminal_cursor,
            current_calendar_digest,
            current_policy_digest,
        )
        if (
            not dependencies_are_current()
            or connection_total_changes(journal) != current_total_changes
        ):
            raise ValueError("INCREMENTAL_INGESTION_CHECKPOINT_STALE")
        with checkpoint_lock:
            current = registry.get(journal)
            if current is not None:
                if (
                    type(current) is not tuple
                    or len(current) != 8
                    or current[2]() is not journal
                    or type(current[3]) is not int
                    or type(current[5]) is not int
                    or current[3] >= current_total_changes
                    or current[5] >= terminal_cursor
                ):
                    raise RiskBlock("INCREMENTAL_INGESTION_CHECKPOINT_OVERWRITE")
            registry[journal] = issued

    return loader, installer


_ActualLedgerStateAuthorityIssued = tuple[object, ...]
_ActualLedgerStateAuthorityCandidate = tuple[
    ActualLedgerState,
    JournalActualReplaySource,
    _ActualLedgerStateAuthorityIssued,
    object,
    object,
    object,
]


def _make_actual_ledger_state_authority_candidate_api(
    *,
    authority_lock: RLock,
    registry: dict[int, _ActualLedgerStateAuthorityIssued],
    state_type: type[ActualLedgerState],
    source_type: type[JournalActualReplaySource],
    seal_factory: object,
    seal_equal: object,
    fingerprint_domain: bytes,
    opaque_structural_mode: str,
    semantic_rechecker: object,
) -> tuple[object, object, object]:
    """Capture immutable state-authority primitives behind deleted setup."""

    if (
        not callable(seal_factory)
        or not callable(seal_equal)
        or not callable(semantic_rechecker)
    ):
        raise TypeError("actual ledger state seal dependency is invalid")

    def fingerprint(state: ActualLedgerState) -> object:
        if type(state) is not state_type:
            raise TypeError("actual ledger state type is unverified")
        return seal_factory(  # type: ignore[operator]
            state,
            domain=fingerprint_domain,
            root_mode=opaque_structural_mode,
        )

    def candidate(
        state: object,
        source: object,
    ) -> _ActualLedgerStateAuthorityCandidate | None:
        if type(state) is not state_type or type(source) is not source_type:
            return None
        with authority_lock:
            issued = registry.get(id(state))
            if (
                issued is None
                or type(issued) is not tuple
                or len(issued) != 6
                or issued[0]() is not state
                or issued[2]() is not source
                or issued[4] is not semantic_rechecker
            ):
                return None
            return (
                state,
                source,
                issued,
                issued[1],
                issued[3],
                issued[5],
            )

    def is_current_without_callbacks(
        captured: object,
    ) -> bool:
        if type(captured) is not tuple or len(captured) != 6:
            return False
        (
            state,
            source,
            issued,
            expected_fingerprint,
            source_candidate,
            material,
        ) = captured
        if (
            type(state) is not state_type
            or type(source) is not source_type
            or type(issued) is not tuple
            or len(issued) != 6
            or issued[0]() is not state
            or issued[1] is not expected_fingerprint
            or issued[2]() is not source
            or issued[3] is not source_candidate
            or issued[4] is not semantic_rechecker
            or issued[5] is not material
        ):
            return False
        try:
            if not semantic_rechecker(  # type: ignore[operator]
                state,
                source,
                source_candidate,
                expected_fingerprint,
                material,
            ):
                return False
            current_fingerprint = fingerprint(state)
        except Exception:
            return False
        with authority_lock:
            current = registry.get(id(state))
            return (
                current is issued
                and current[0]() is state
                and current[1] is expected_fingerprint
                and current[2]() is source
                and current[3] is source_candidate
                and current[4] is semantic_rechecker
                and current[5] is material
                and seal_equal(  # type: ignore[operator]
                    expected_fingerprint,
                    current_fingerprint,
                )
            )

    return fingerprint, candidate, is_current_without_callbacks

def _is_current_actual_ledger_state_authority_for_source(
    state: object,
    source: object,
) -> bool:
    """Pure exact registry/source check after Journal currentness completed."""
    candidate = _actual_ledger_state_authority_candidate(state, source)
    return candidate is not None and (
        _is_current_actual_ledger_state_authority_candidate_without_callbacks(
            candidate
        )
    )


def is_verified_actual_ledger_state(state: object) -> bool:
    """Return whether replay issued this exact immutable derived state."""
    if type(state) is not ActualLedgerState:
        return False
    with _ACTUAL_STATE_AUTHORITY_LOCK:
        issued = _ACTUAL_STATE_AUTHORITIES.get(id(state))
        source = None if issued is None else issued[2]()
    if type(source) is not JournalActualReplaySource or not (
        is_verified_journal_replay_source(source)
    ):
        return False
    return _is_current_actual_ledger_state_authority_for_source(state, source)


def is_verified_actual_ledger_state_for_source(
    state: object,
    source: object,
) -> bool:
    """Bind a replay-issued state to its exact in-process Journal source."""
    if type(state) is not ActualLedgerState or (
        type(source) is not JournalActualReplaySource
    ):
        return False
    # Journal currentness may invoke SQLite callbacks.  Complete it before the
    # final callback-free state seal and exact registry comparison.
    if not is_verified_journal_replay_source(source):
        return False
    return _is_current_actual_ledger_state_authority_for_source(state, source)


@dataclass(frozen=True, slots=True)
class ActionAssessment:
    status: ActionStatus
    reason_codes: tuple[str, ...]
    apply_economic_event: bool
    signal_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, ActionStatus):
            raise ValueError("INVALID_ACTION_STATUS")
        reasons = _reason_codes(self.reason_codes)
        object.__setattr__(self, "reason_codes", reasons)
        if type(self.apply_economic_event) is not bool:
            raise ValueError("INVALID_ACTION_ASSESSMENT")
        if self.status is ActionStatus.COMPLIANT and reasons:
            raise ValueError("INVALID_ACTION_ASSESSMENT")
        if (
            self.status is ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED
            and not reasons
        ):
            raise ValueError("INVALID_ACTION_ASSESSMENT")
        if self.status is ActionStatus.PENDING_CLARIFICATION and (
            self.apply_economic_event or not reasons
        ):
            raise ValueError("INVALID_ACTION_ASSESSMENT")
        if self.signal_id is not None and (
            type(self.signal_id) is not str or not self.signal_id
        ):
            raise ValueError("INVALID_ACTION_SIGNAL_ID")


@dataclass(frozen=True, slots=True)
class IngestionActionResult:
    ordinal: int
    event_row_id: int
    event_id: str
    kind: ConfirmationKind
    status: ActionStatus
    reason_codes: tuple[str, ...]
    authority_event_row_id: int | None
    outbox_id: int

    def __post_init__(self) -> None:
        _require_int(self.ordinal, minimum=0)
        _require_int(self.event_row_id, minimum=1)
        _require_int(self.outbox_id, minimum=1)
        if type(self.event_id) is not str or not self.event_id:
            raise ValueError("INVALID_INGESTION_EVENT_ID")
        if not isinstance(self.kind, ConfirmationKind) or not isinstance(
            self.status,
            ActionStatus,
        ):
            raise ValueError("INVALID_INGESTION_RESULT")
        object.__setattr__(self, "reason_codes", _reason_codes(self.reason_codes))
        if self.authority_event_row_id is not None:
            _require_int(self.authority_event_row_id, minimum=1)


@dataclass(frozen=True, slots=True)
class IngestionResult:
    raw_row_id: int
    message_id: str
    duplicate: bool
    actions: tuple[IngestionActionResult, ...]
    state_digest: str

    def __post_init__(self) -> None:
        _require_int(self.raw_row_id, minimum=1)
        if type(self.message_id) is not str or not self.message_id:
            raise ValueError("INVALID_INGESTION_MESSAGE_ID")
        if type(self.duplicate) is not bool:
            raise ValueError("INVALID_INGESTION_RESULT")
        actions = tuple(self.actions)
        if not actions or any(
            not isinstance(action, IngestionActionResult) for action in actions
        ):
            raise ValueError("INVALID_INGESTION_RESULT")
        if tuple(action.ordinal for action in actions) != tuple(range(len(actions))):
            raise ValueError("INVALID_INGESTION_ORDINALS")
        if (
            type(self.state_digest) is not str
            or len(self.state_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.state_digest)
        ):
            raise ValueError("INVALID_INGESTION_DIGEST")
        object.__setattr__(self, "actions", actions)


def _require_int(value: object, *, minimum: int) -> int:
    if type(value) is not int or not minimum <= value <= MAX_MICRODOLLARS:
        raise ValueError("INVALID_INTEGER")
    return value


def _reason_codes(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError("INVALID_REASON_CODES")
    result = tuple(dict.fromkeys(values))  # type: ignore[arg-type]
    if any(type(reason) is not str or not reason for reason in result):
        raise ValueError("INVALID_REASON_CODES")
    return result


def _position_for_symbol(
    state: ProjectionState,
    symbol: str,
) -> ProjectionPosition | None:
    return next(
        (position for position in state.positions if position.symbol == symbol),
        None,
    )


def _is_strategy_position(position: ProjectionPosition | ActualPositionState) -> bool:
    return not position.signal_id.startswith("actual:unrelated:")


def _entry_reasons(
    action: ParsedConfirmation,
    state: ProjectionState,
    resolver: SignalPlanResolver,
    query_cutoff: datetime,
) -> tuple[list[str], LedgerSignal | None]:
    from .ledger import is_issued_ledger_signal

    assert action.symbol is not None
    assert action.event_time is not None
    assert action.quantity is not None
    assert action.price is not None
    reasons: list[str] = []
    session_date = action.event_time.astimezone(_ET).date()
    plan = resolver.resolve(
        symbol=action.symbol,
        economic_at=action.event_time,
        query_cutoff=query_cutoff,
    )
    resolved = None if plan is None else plan.signal
    authority_signal: LedgerSignal | None = None
    if resolved is None:
        reasons.append("SIGNAL_PLAN_UNAVAILABLE")
    else:
        if not isinstance(plan, ResolvedSignalPlan):
            raise TypeError("signal resolver returned an invalid value")
        if (
            resolved.symbol != action.symbol
            or resolved.publication_session != session_date
        ):
            reasons.append("SIGNAL_PLAN_MISMATCH")
        if not is_issued_ledger_signal(resolved):
            reasons.append("SIGNAL_AUTHORITY_UNVERIFIED")
        else:
            authority_signal = resolved
        if resolved.role != "PRIMARY":
            reasons.append("SHADOW_FILL")
        if action.price > resolved.maximum_entry:
            reasons.append("FILL_ABOVE_MAXIMUM_ENTRY")
        if action.kind is ConfirmationKind.PARTIAL_FILL:
            if action.parent_order_id is None:
                reasons.append("PARENT_ORDER_UNVERIFIED")
            elif action.fill_group_planned_shares != resolved.planned_shares:
                reasons.append("FILL_GROUP_QUANTITY_MISMATCH")
            if action.quantity > resolved.planned_shares:
                reasons.append("SHARE_QUANTITY_MISMATCH")
        elif action.quantity != resolved.planned_shares:
            reasons.append("SHARE_QUANTITY_MISMATCH")
        if action.stop is not None:
            if action.stop < resolved.recommended_stop:
                reasons.append("CONFIRMED_STOP_WIDER_THAN_RECOMMENDED")
            if action.stop >= action.price:
                reasons.append("NON_POSITIVE_CONFIRMED_STOP_DISTANCE")

    if action.bid is None:
        reasons.append("CONFIRMED_BID_MISSING")
    if action.ask is None:
        reasons.append("CONFIRMED_ASK_MISSING")
    if action.stop is None:
        reasons.append("CONFIRMED_STOP_MISSING")
    if action.bid is not None and action.ask is not None:
        with localcontext() as context:
            context.prec = 40
            midpoint = (action.ask + action.bid) / Decimal("2")
            spread = (action.ask - action.bid) / midpoint
        if spread > _MAX_SPREAD:
            reasons.append("CONFIRMED_SPREAD_TOO_WIDE")
        if not action.bid <= action.price <= action.ask:
            reasons.append("FILL_OUTSIDE_CONFIRMED_MARKET")
    local_time = action.event_time.astimezone(_ET).time().replace(tzinfo=None)
    if (local_time.hour, local_time.minute, local_time.second) <= (9, 35, 0):
        reasons.append("ENTRY_NOT_AFTER_0935")
    existing = _position_for_symbol(state, action.symbol)
    if existing is not None:
        same_group = (
            action.kind is ConfirmationKind.PARTIAL_FILL
            and action.parent_order_id is not None
            and existing.parent_order_id == action.parent_order_id
        )
        if not same_group:
            reasons.append("POSITION_ADDITIONS_PROHIBITED")
            if money_to_micros(action.price) * existing.shares < existing.cost_basis_micros:
                reasons.append("AVERAGING_DOWN_PROHIBITED")
    return reasons, authority_signal


def assess_confirmation(
    action: ParsedConfirmation | PendingConfirmation,
    state: ProjectionState,
    *,
    signal_resolver: SignalPlanResolver,
    query_cutoff: datetime | None = None,
) -> ActionAssessment:
    """Assess policy without granting any source or persistence authority."""
    if not isinstance(state, ProjectionState):
        raise TypeError("state must be a ProjectionState")
    if not isinstance(signal_resolver, SignalPlanResolver):
        raise TypeError("signal resolver must implement SignalPlanResolver")
    if isinstance(action, PendingConfirmation):
        return ActionAssessment(
            ActionStatus.PENDING_CLARIFICATION,
            (action.reason_code,),
            False,
        )
    if not isinstance(action, ParsedConfirmation):
        raise TypeError("action must be a parsed or pending confirmation")
    if query_cutoff is None:
        query_cutoff = action.event_time or datetime.max.replace(tzinfo=UTC)

    reasons: list[str] = []
    signal_id: str | None = None
    apply_economic_event = action.kind in {
        ConfirmationKind.BUY,
        ConfirmationKind.PARTIAL_FILL,
        ConfirmationKind.SOLD,
        ConfirmationKind.STOP_FILLED,
        ConfirmationKind.STOP_UPDATED,
        ConfirmationKind.RECONCILE_CASH,
        ConfirmationKind.RECONCILE_UNRELATED_POSITION,
        ConfirmationKind.FEE,
    }
    if action.kind in {ConfirmationKind.BUY, ConfirmationKind.PARTIAL_FILL}:
        reasons, resolved = _entry_reasons(
            action,
            state,
            signal_resolver,
            query_cutoff,
        )
        signal_id = None if resolved is None else resolved.signal_id
    elif action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}:
        assert action.symbol is not None and action.quantity is not None
        matching = tuple(
            position for position in state.positions if position.symbol == action.symbol
        )
        strategy_matching = tuple(
            position for position in matching if _is_strategy_position(position)
        )
        if not matching:
            reasons.append("POSITION_NOT_TRACKED")
        elif not strategy_matching:
            reasons.append("NON_STRATEGY_POSITION_LINEAGE")
        elif len(matching) != 1 or len(strategy_matching) != 1:
            reasons.append("MULTIPLE_POSITION_LINEAGES_UNRESOLVED")
        else:
            position = strategy_matching[0]
            signal_id = position.signal_id
            if action.quantity > position.shares:
                reasons.append("OVER_SELL_REPORTED")
    elif action.kind is ConfirmationKind.STOP_UPDATED:
        assert action.symbol is not None and action.stop is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.symbol and _is_strategy_position(position)
        )
        if not matching:
            reasons.append("POSITION_NOT_TRACKED")
        elif len(matching) != 1:
            reasons.append("MULTIPLE_POSITION_LINEAGES_UNRESOLVED")
        else:
            position = matching[0]
            signal_id = position.signal_id
            prior = (
                position.user_confirmed_stop_micros
                if position.user_confirmed_stop_micros is not None
                else position.recommended_stop_micros
            )
            if prior is not None and int(action.stop * 1_000_000) < prior:
                reasons.append("STOP_WIDENING_PROHIBITED")
    elif action.kind is ConfirmationKind.ACCOUNT_CHECK:
        if action.pending_orders:
            reasons.append("PENDING_ORDERS_PRESENT")
        if action.unlogged_positions:
            reasons.append("UNLOGGED_POSITIONS_PRESENT")
    elif action.kind is ConfirmationKind.RECONCILE_PENDING_ORDERS:
        if action.pending_orders:
            reasons.append("PENDING_ORDERS_PRESENT")
    elif action.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION:
        assert action.symbol is not None and action.signed_shares is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.symbol
            and position.signal_id == _unrelated_position_lineage_id(action.symbol)
        )
        if action.signed_shares > 0:
            reasons.append("UNRELATED_POSITION_RECONCILIATION")
        elif len(matching) != 1:
            reasons.append("UNRELATED_POSITION_RECONCILIATION")
        elif abs(action.signed_shares) > matching[0].shares:
            reasons.extend(
                (
                    "UNRELATED_POSITION_RECONCILIATION",
                    "UNRELATED_POSITION_QUANTITY_MISMATCH",
                )
            )
        elif abs(action.signed_shares) < matching[0].shares:
            reasons.append("UNRELATED_POSITION_RECONCILIATION")
        signal_id = _unrelated_position_lineage_id(action.symbol)
    elif action.kind is ConfirmationKind.RECONCILE_CASH:
        if (
            state.reconciliation_required
            and not state.positions
            and not set(state.reason_codes)
            <= {
                "ACCOUNT_CASH_BASELINE_UNAVAILABLE",
                "ACCOUNT_CASH_NEGATIVE",
            }
        ):
            reasons.append("ACCOUNT_CASH_RECONCILIATION")
    elif action.kind is ConfirmationKind.FEE:
        assert action.asset_id is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.asset_id and _is_strategy_position(position)
        )
        if len(matching) == 1:
            signal_id = matching[0].signal_id
        else:
            reasons.append("FEE_LINEAGE_AMBIGUOUS")
    elif action.kind in {
        ConfirmationKind.OPTION_REVIEW,
        ConfirmationKind.OPTION_OPEN,
        ConfirmationKind.OPTION_MARK,
        ConfirmationKind.OPTION_CLOSE,
        ConfirmationKind.OPTION_WINDOW_START,
    }:
        apply_economic_event = False
    prior_reconciliation_remains = state.reconciliation_required
    if action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}:
        assert action.symbol is not None and action.quantity is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.symbol and _is_strategy_position(position)
        )
        if (
            len(state.positions) == 1
            and len(matching) == 1
            and action.quantity >= matching[0].shares
        ):
            prior_reconciliation_remains = False
    elif (
        action.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION
        and action.symbol is not None
        and action.signed_shares is not None
        and action.signed_shares < 0
    ):
        matching = tuple(
            position
            for position in state.positions
            if position.signal_id == _unrelated_position_lineage_id(action.symbol)
        )
        if (
            len(matching) == 1
            and abs(action.signed_shares) == matching[0].shares
            and set(state.reason_codes) <= {"UNRELATED_POSITION_RECONCILIATION"}
        ):
            prior_reconciliation_remains = False
    elif (
        action.kind is ConfirmationKind.ACCOUNT_CHECK
        and not action.pending_orders
        and not action.unlogged_positions
        and set(state.reason_codes)
        <= {
            "ACCOUNT_CASH_BASELINE_UNAVAILABLE",
            "ACCOUNT_CASH_NEGATIVE",
        }
    ):
        prior_reconciliation_remains = False
    elif (
        action.kind is ConfirmationKind.RECONCILE_CASH
        and action.amount is not None
        and not state.positions
        and set(state.reason_codes) <= {"ACCOUNT_CASH_NEGATIVE"}
    ):
        prior_reconciliation_remains = False
    if prior_reconciliation_remains:
        reasons.append("PRIOR_RECONCILIATION_REQUIRED")

    unique = tuple(dict.fromkeys(reasons))
    return ActionAssessment(
        ActionStatus.COMPLIANT
        if not unique
        else ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        unique,
        apply_economic_event,
        signal_id,
    )


def _with_settlement_calendar_assessment(
    assessment: ActionAssessment,
    action: ParsedConfirmation,
    calendar: SessionCalendarResolver,
) -> ActionAssessment:
    if action.kind not in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}:
        return assessment
    assert action.event_time is not None
    local = action.event_time.astimezone(_ET)
    reason: str | None = None
    try:
        if not calendar.is_open(local.date()):
            reason = "SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION"
        else:
            session = calendar.session(local.date())
            local_time = local.time().replace(tzinfo=None)
            if not session.open_time <= local_time <= session.close_time:
                reason = "SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION"
            else:
                calendar.add_sessions(local.date(), 1)
    except RiskBlock as error:
        reason = (
            "CALENDAR_COVERAGE_MISSING"
            if error.reason_code == "CALENDAR_COVERAGE_MISSING"
            else "SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION"
        )
    if reason is None or reason in assessment.reason_codes:
        return assessment
    reasons = (*assessment.reason_codes, reason)
    return ActionAssessment(
        ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        reasons,
        assessment.apply_economic_event,
        assessment.signal_id,
    )


def _with_actual_timing_assessment(
    assessment: ActionAssessment,
    action: ParsedConfirmation,
    state: ActualLedgerState,
) -> ActionAssessment:
    if action.event_time is None:
        return assessment
    matching: tuple[ActualPositionState, ...]
    required_shares: int | None = None
    reason: str | None = None
    if action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}:
        assert action.symbol is not None and action.quantity is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.symbol
        )
        if not matching:
            delayed_closed = any(
                trade.symbol == action.symbol
                and action.event_time < trade.closed_at
                for trade in state.closed_trades
            )
            reason = (
                "DELAYED_EXIT_ATTRIBUTION_UNRESOLVED"
                if delayed_closed
                else None
            )
        else:
            required_shares = action.quantity
            reason = "DELAYED_EXIT_PRECEDES_POSITION"
    elif action.kind is ConfirmationKind.FEE:
        assert action.asset_id is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.asset_id
            and position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        )
        if len(matching) != 1:
            return assessment
        required_shares = 1
        reason = "DELAYED_FEE_PRECEDES_POSITION"
    elif action.kind is ConfirmationKind.STOP_UPDATED:
        assert action.symbol is not None
        matching = tuple(
            position
            for position in state.positions
            if position.symbol == action.symbol
            and position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        )
        if len(matching) != 1:
            return assessment
        required_shares = 1
        reason = "DELAYED_STOP_PRECEDES_POSITION"
    elif (
        action.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION
        and action.signed_shares is not None
        and action.signed_shares < 0
    ):
        assert action.symbol is not None
        matching = tuple(
            position
            for position in state.positions
            if position.signal_id == _unrelated_position_lineage_id(action.symbol)
        )
        if len(matching) != 1:
            return assessment
        required_shares = abs(action.signed_shares)
        reason = "DELAYED_UNRELATED_POSITION_REDUCTION_PRECEDES_POSITION"
    else:
        return assessment

    if required_shares is not None:
        eligible_shares, has_future_lot = _position_timing_eligibility(
            matching,
            action.event_time,
        )
        if not has_future_lot or eligible_shares >= required_shares:
            return assessment
    if reason is None:
        return assessment
    if reason in assessment.reason_codes:
        return assessment
    return ActionAssessment(
        ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        (*assessment.reason_codes, reason),
        assessment.apply_economic_event,
        (
            None
            if reason
            in {
                "DELAYED_FEE_PRECEDES_POSITION",
                "DELAYED_STOP_PRECEDES_POSITION",
            }
            else assessment.signal_id
        ),
    )


def _position_timing_eligibility(
    positions: tuple[ActualPositionState, ...],
    event_time: datetime,
) -> tuple[int, bool]:
    """Return shares economically present and whether later lots also exist."""
    eligible_shares = sum(
        lot.remaining_shares
        for position in positions
        for lot in position.lots
        if lot.acquired_at <= event_time
    )
    has_future_lot = any(
        lot.acquired_at > event_time
        for position in positions
        for lot in position.lots
    )
    return eligible_shares, has_future_lot


def _with_actual_cash_assessment(
    assessment: ActionAssessment,
    action: ParsedConfirmation,
    state: ActualLedgerState,
) -> ActionAssessment:
    if action.kind is not ConfirmationKind.RECONCILE_CASH:
        return assessment
    reason: str | None = None
    if state.user_confirmed_cash_micros is None:
        reason = "ACCOUNT_CASH_BASELINE_UNAVAILABLE"
    else:
        assert action.amount is not None
        if state.user_confirmed_cash_micros + money_to_micros(action.amount) < 0:
            reason = "ACCOUNT_CASH_NEGATIVE"
    if reason is None or reason in assessment.reason_codes:
        return assessment
    return ActionAssessment(
        ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        (*assessment.reason_codes, reason),
        assessment.apply_economic_event,
        assessment.signal_id,
    )


def _with_stale_stop_assessment(
    assessment: ActionAssessment,
    action: ParsedConfirmation,
    latest_effective_at: datetime | None,
) -> ActionAssessment:
    if (
        action.kind is not ConfirmationKind.STOP_UPDATED
        or latest_effective_at is None
        or action.event_time is None
        or action.event_time >= latest_effective_at
    ):
        return assessment
    reasons = tuple(
        dict.fromkeys(
            (
                *(
                    reason
                    for reason in assessment.reason_codes
                    if reason != "STOP_WIDENING_PROHIBITED"
                ),
                "STALE_STOP_OBSERVATION",
            )
        )
    )
    return ActionAssessment(
        ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
        reasons,
        assessment.apply_economic_event,
        assessment.signal_id,
    )


def _actual_only_lineage_id(
    action: ParsedConfirmation,
    event_id: str,
    planned_signal_id: str | None,
) -> tuple[str, str]:
    if planned_signal_id is not None:
        return planned_signal_id, "PLANNED_SIGNAL"
    assert action.symbol is not None
    if (
        action.kind is ConfirmationKind.PARTIAL_FILL
        and action.parent_order_id is not None
        and action.fill_group_planned_shares is not None
    ):
        material = (
            "stock-monitor/actual-partial-lineage/v1\x00"
            + action.symbol
            + "\x00"
            + action.parent_order_id
            + "\x00"
            + str(action.fill_group_planned_shares)
        ).encode("utf-8")
        return "actual:group:" + hashlib.sha256(material).hexdigest(), "ACTUAL_GROUP"
    material = (
        "stock-monitor/actual-unplanned-lineage/v1\x00" + event_id
    ).encode("utf-8")
    return "actual:unplanned:" + hashlib.sha256(material).hexdigest(), "ACTUAL_EVENT"


def _unrelated_position_lineage_id(symbol: str) -> str:
    material = (
        "stock-monitor/actual-unrelated-lineage/v1\x00" + symbol
    ).encode("ascii")
    return "actual:unrelated:" + hashlib.sha256(material).hexdigest()


def _resolve_actual_entry_lineage(
    before: ActualLedgerState,
    action: ParsedConfirmation,
    event_id: str,
    planned_signal_id: str | None,
) -> tuple[str, str]:
    if planned_signal_id is not None or action.kind is ConfirmationKind.PARTIAL_FILL:
        return _actual_only_lineage_id(action, event_id, planned_signal_id)
    assert action.symbol is not None
    existing = tuple(
        position
        for position in before.positions
        if position.symbol == action.symbol
        and position.lineage_kind == "ACTUAL_EVENT"
    )
    if len(existing) == 1:
        return existing[0].signal_id, existing[0].lineage_kind
    return _actual_only_lineage_id(action, event_id, None)


def _actual_state_digest(state: ActualLedgerState) -> str:
    payload = {
        "version": 1,
        "query_cutoff": _canonical_timestamp(state.query_cutoff),
        "through_cursor": state.through_cursor,
        "positions": [
            {
                "signal_id": position.signal_id,
                "symbol": position.symbol,
                "lineage_kind": position.lineage_kind,
                "signal_digest": position.signal_digest,
                "lots": [
                    {
                        "source_event_id": lot.source_event_id,
                        "source_cursor": lot.source_cursor,
                        "remaining_shares": lot.remaining_shares,
                        "unit_cost_micros": lot.unit_cost_micros,
                        "acquired_at": _canonical_timestamp(lot.acquired_at),
                        "received_at": _canonical_timestamp(lot.received_at),
                        "parent_order_id": lot.parent_order_id,
                    }
                    for lot in position.lots
                ],
                "recommended_stop_micros": position.recommended_stop_micros,
                "user_stop_micros": position.user_stop_micros,
                "target_micros": position.target_micros,
                "tick_micros": position.tick_micros,
                "cumulative_buy_cost_micros": position.cumulative_buy_cost_micros,
                "cumulative_sale_proceeds_micros": position.cumulative_sale_proceeds_micros,
                "linked_fees_micros": position.linked_fees_micros,
                "reason_codes": list(position.reason_codes),
                "lifecycle_event_ids": list(position.lifecycle_event_ids),
            }
            for position in state.positions
        ],
        "closed_trades": [
            {
                "signal_id": trade.signal_id,
                "symbol": trade.symbol,
                "opened_at": _canonical_timestamp(trade.opened_at),
                "closed_at": _canonical_timestamp(trade.closed_at),
                "source_cursor": trade.source_cursor,
                "source_ordinal": trade.source_ordinal,
                "buy_cost_micros": trade.buy_cost_micros,
                "gross_sale_micros": trade.gross_sale_micros,
                "fees_micros": trade.fees_micros,
                "pnl_micros": trade.pnl_micros,
                "source_event_ids": list(trade.source_event_ids),
                "source_digest": trade.source_digest,
            }
            for trade in state.closed_trades
        ],
        "strategy_settled_cash_micros": state.strategy_settled_cash_micros,
        "user_confirmed_cash_micros": state.user_confirmed_cash_micros,
        "reconciliation_reasons": list(state.reconciliation_reasons),
        "calendar_digest": state.calendar_digest,
        "calendar_release_verified": state.calendar_release_verified,
        "journal_source_digest": state.journal_source_digest,
        "policy_digest": state.policy_digest,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _new_actual_state(
    *,
    query_cutoff: datetime,
    through_cursor: int | None,
    positions: tuple[ActualPositionState, ...],
    closed_trades: tuple[ActualClosedTrade, ...],
    settlement_ledger: tuple[JournalPostingSource, ...],
    strategy_settled_cash_micros: int,
    user_confirmed_cash_micros: int | None,
    reconciliation_reasons: tuple[str, ...],
    cache_matches_replay: bool,
    calendar_digest: str | None = None,
    calendar_release_verified: bool = False,
    journal_source_digest: str | None = None,
    policy_digest: str | None = None,
) -> ActualLedgerState:
    values: list[int] = [strategy_settled_cash_micros]
    if user_confirmed_cash_micros is not None:
        values.append(user_confirmed_cash_micros)
    aggregate_basis = 0
    for position in positions:
        values.extend(
            (
                position.cumulative_buy_cost_micros,
                position.cumulative_sale_proceeds_micros,
                position.linked_fees_micros,
            )
        )
        position_basis = 0
        for lot in position.lots:
            lot_basis = lot.remaining_shares * lot.unit_cost_micros
            values.append(lot_basis)
            position_basis += lot_basis
        values.append(position_basis)
        aggregate_basis += position_basis
    values.append(aggregate_basis)
    for trade in closed_trades:
        _require_int(trade.source_cursor, minimum=1)
        _require_int(trade.source_ordinal, minimum=0)
        values.extend(
            (
                trade.buy_cost_micros,
                trade.gross_sale_micros,
                trade.fees_micros,
                trade.pnl_micros,
            )
        )
    if any(
        type(value) is not int
        or not MIN_MICRODOLLARS <= value <= MAX_MICRODOLLARS
        for value in values
    ):
        raise ValueError("ACTUAL_AGGREGATE_OVERFLOW")
    provisional = ActualLedgerState(
        query_cutoff=query_cutoff,
        through_cursor=through_cursor,
        positions=tuple(sorted(positions, key=lambda item: item.signal_id)),
        closed_trades=closed_trades,
        settlement_ledger=settlement_ledger,
        strategy_settled_cash_micros=strategy_settled_cash_micros,
        user_confirmed_cash_micros=user_confirmed_cash_micros,
        reconciliation_reasons=tuple(dict.fromkeys(reconciliation_reasons)),
        source_digest="0" * 64,
        cache_matches_replay=cache_matches_replay,
        calendar_digest=calendar_digest,
        calendar_release_verified=calendar_release_verified,
        journal_source_digest=journal_source_digest,
        policy_digest=policy_digest,
    )
    return replace(provisional, source_digest=_actual_state_digest(provisional))


def _empty_actual_state(
    query_cutoff: datetime,
    policy: Policy,
    calendar: SessionCalendarResolver,
) -> ActualLedgerState:
    return _new_actual_state(
        query_cutoff=query_cutoff,
        through_cursor=None,
        positions=(),
        closed_trades=(),
        settlement_ledger=(),
        strategy_settled_cash_micros=money_to_micros(policy.capital),
        user_confirmed_cash_micros=None,
        reconciliation_reasons=(),
        cache_matches_replay=True,
        calendar_digest=_calendar_digest(calendar),
        calendar_release_verified=calendar.release_verified,
        journal_source_digest=None,
        policy_digest=_policy_digest(policy),
    )


def _private_incremental_state(
    state: ActualLedgerState,
    *,
    query_cutoff: datetime,
    through_cursor: int | None,
) -> ActualLedgerState:
    """Strip public replay provenance from a private computation checkpoint."""
    return _new_actual_state(
        query_cutoff=query_cutoff,
        through_cursor=through_cursor,
        positions=state.positions,
        closed_trades=state.closed_trades,
        settlement_ledger=(),
        strategy_settled_cash_micros=state.strategy_settled_cash_micros,
        user_confirmed_cash_micros=state.user_confirmed_cash_micros,
        reconciliation_reasons=state.reconciliation_reasons,
        cache_matches_replay=False,
        calendar_digest=state.calendar_digest,
        calendar_release_verified=state.calendar_release_verified,
        journal_source_digest=None,
        policy_digest=state.policy_digest,
    )


def _projection_state_from_actual(state: ActualLedgerState) -> ProjectionState:
    return ProjectionState(
        positions=tuple(
            ProjectionPosition(
                signal_id=position.signal_id,
                symbol=position.symbol,
                shares=position.shares,
                cost_basis_micros=position.cost_basis_micros,
                recommended_stop_micros=position.recommended_stop_micros,
                user_confirmed_stop_micros=position.user_stop_micros,
                target_micros=position.target_micros,
                parent_order_id=(
                    position.lots[0].parent_order_id
                    if position.lots
                    and all(
                        lot.parent_order_id == position.lots[0].parent_order_id
                        for lot in position.lots
                    )
                    else None
                ),
            )
            for position in state.positions
        ),
        reconciliation_required=bool(state.reconciliation_reasons),
        reason_codes=state.reconciliation_reasons,
    )


def _actual_lineage_kind(signal_id: str) -> str:
    if signal_id.startswith("actual:group:"):
        return "ACTUAL_GROUP"
    if signal_id.startswith("actual:"):
        return "ACTUAL_EVENT"
    return "PLANNED_SIGNAL"


def _actual_open_risk_micros(state: ActualLedgerState) -> int:
    total = 0
    for position in state.positions:
        stops = tuple(
            stop
            for stop in (
                position.recommended_stop_micros,
                position.user_stop_micros,
            )
            if stop is not None
        )
        if not stops:
            risk = position.cost_basis_micros
        else:
            conservative_stop = min(stops)
            risk = sum(
                max(0, lot.unit_cost_micros - conservative_stop)
                * lot.remaining_shares
                for lot in position.lots
            )
        total += risk
        if total > MAX_MICRODOLLARS:
            raise ValueError("ACTUAL_RISK_OVERFLOW")
    return total


def _actual_performance_metrics(
    state: ActualLedgerState,
    policy: Policy,
) -> tuple[int, int, int]:
    capital = money_to_micros(policy.capital)
    cutoff_date = state.query_cutoff.astimezone(_ET).date()
    cutoff_week = cutoff_date.isocalendar()[:2]
    cutoff_month = (cutoff_date.year, cutoff_date.month)
    weekly_baseline = capital
    monthly_baseline = capital
    running_equity = capital
    weekly_high: int | None = None
    monthly_high: int | None = None
    consecutive_losses = 0
    for trade in sorted(
        state.closed_trades,
        key=lambda item: (
            item.closed_at.astimezone(UTC),
            item.source_cursor,
            item.source_ordinal,
            item.source_event_ids[-1] if item.source_event_ids else "",
            item.signal_id,
        ),
    ):
        trade_date = trade.closed_at.astimezone(_ET).date()
        trade_week = trade_date.isocalendar()[:2]
        trade_month = (trade_date.year, trade_date.month)
        if trade_week < cutoff_week:
            weekly_baseline += trade.pnl_micros
        if trade_month < cutoff_month:
            monthly_baseline += trade.pnl_micros
        running_equity += trade.pnl_micros
        if not MIN_MICRODOLLARS <= running_equity <= MAX_MICRODOLLARS:
            raise ValueError("ACTUAL_AGGREGATE_OVERFLOW")
        if trade_week == cutoff_week:
            weekly_high = (
                running_equity
                if weekly_high is None
                else max(weekly_high, running_equity)
            )
        if trade_month == cutoff_month:
            monthly_high = (
                running_equity
                if monthly_high is None
                else max(monthly_high, running_equity)
            )
        consecutive_losses = consecutive_losses + 1 if trade.pnl_micros < 0 else 0
    weekly_high = max(weekly_baseline, weekly_high or weekly_baseline)
    monthly_high = max(monthly_baseline, monthly_high or monthly_baseline)
    if any(
        not MIN_MICRODOLLARS <= value <= MAX_MICRODOLLARS
        for value in (weekly_high, monthly_high)
    ):
        raise ValueError("ACTUAL_AGGREGATE_OVERFLOW")
    return consecutive_losses, weekly_high, monthly_high


def _posting_key(event_id: str, leg: str) -> str:
    digest = hashlib.sha256(
        b"stock-monitor/actual-posting/v1\x00"
        + event_id.encode("utf-8")
        + b"\x00"
        + leg.encode("ascii")
    ).hexdigest()
    return f"actual:{digest}"


def _transition_actual(
    before: ActualLedgerState,
    action: ParsedConfirmation,
    assessment: ActionAssessment,
    *,
    event_id: str,
    event_cursor: int,
    event_ordinal: int,
    signal_id: str | None,
    lineage_kind: str | None,
    message_time: datetime,
    received_at: datetime,
    query_cutoff: datetime,
    calendar: SessionCalendarResolver,
    policy: Policy,
) -> ActualTransition:
    positions = list(before.positions)
    closed = list(before.closed_trades)
    postings: list[PostingIntent] = []
    strategy_cash = before.strategy_settled_cash_micros
    user_cash = before.user_confirmed_cash_micros
    event_time = action.event_time or message_time
    event_reasons = tuple(
        reason
        for reason in assessment.reason_codes
        if reason != "PRIOR_RECONCILIATION_REQUIRED"
    )
    prior_position_reasons = {
        reason
        for position in before.positions
        for reason in position.reason_codes
    }
    global_reasons = [
        reason
        for reason in before.reconciliation_reasons
        if reason not in prior_position_reasons
    ]
    if action.kind is ConfirmationKind.ACCOUNT_CHECK:
        global_reasons = [
            reason
            for reason in global_reasons
            if reason
            not in {
                "ACCOUNT_CASH_BASELINE_UNAVAILABLE",
                "ACCOUNT_CASH_NEGATIVE",
            }
        ]
        if not action.pending_orders:
            global_reasons = [
                reason
                for reason in global_reasons
                if reason != "PENDING_ORDERS_PRESENT"
            ]
        if not action.unlogged_positions:
            global_reasons = [
                reason
                for reason in global_reasons
                if reason != "UNLOGGED_POSITIONS_PRESENT"
            ]
    if (
        action.kind is ConfirmationKind.RECONCILE_PENDING_ORDERS
        and not action.pending_orders
    ):
        global_reasons = [
            reason
            for reason in global_reasons
            if reason != "PENDING_ORDERS_PRESENT"
        ]
    if (
        action.kind is ConfirmationKind.RECONCILE_CASH
        and before.user_confirmed_cash_micros is not None
    ):
        global_reasons = [
            reason
            for reason in global_reasons
            if reason != "ACCOUNT_CASH_BASELINE_UNAVAILABLE"
        ]
        assert action.amount is not None
        if (
            before.user_confirmed_cash_micros + money_to_micros(action.amount)
            >= 0
        ):
            global_reasons = [
                reason
                for reason in global_reasons
                if reason != "ACCOUNT_CASH_NEGATIVE"
            ]

    if action.kind in {ConfirmationKind.BUY, ConfirmationKind.PARTIAL_FILL}:
        assert action.symbol is not None
        assert action.quantity is not None
        assert action.price is not None
        assert signal_id is not None and lineage_kind is not None
        unit_cost = money_to_micros(action.price)
        total_cost = unit_cost * action.quantity
        if total_cost > MAX_MICRODOLLARS:
            raise ValueError("ACTUAL_POSTING_OVERFLOW")
        existing_index = next(
            (
                index
                for index, position in enumerate(positions)
                if position.signal_id == signal_id
            ),
            None,
        )
        lot = ActualLot(
            source_event_id=event_id,
            source_cursor=event_cursor,
            remaining_shares=action.quantity,
            unit_cost_micros=unit_cost,
            acquired_at=event_time,
            received_at=received_at,
            parent_order_id=action.parent_order_id,
        )
        if existing_index is None:
            position = ActualPositionState(
                signal_id=signal_id,
                symbol=action.symbol,
                lineage_kind=lineage_kind,
                signal_digest=None,
                lots=(lot,),
                recommended_stop_micros=None,
                user_stop_micros=(
                    None if action.stop is None else money_to_micros(action.stop)
                ),
                target_micros=None,
                tick_micros=None,
                cumulative_buy_cost_micros=total_cost,
                cumulative_sale_proceeds_micros=0,
                linked_fees_micros=0,
                reason_codes=event_reasons,
                lifecycle_event_ids=(event_id,),
            )
            positions.append(position)
        else:
            prior = positions[existing_index]
            positions[existing_index] = replace(
                prior,
                lots=(*prior.lots, lot),
                cumulative_buy_cost_micros=(
                    prior.cumulative_buy_cost_micros + total_cost
                ),
                reason_codes=tuple(
                    dict.fromkeys((*prior.reason_codes, *event_reasons))
                ),
                lifecycle_event_ids=(*prior.lifecycle_event_ids, event_id),
            )
        strategy_cash -= total_cost
        if strategy_cash < 0:
            event_reasons = (*event_reasons, "STRATEGY_CASH_DEFICIT")
            position_index = next(
                index
                for index, candidate in enumerate(positions)
                if candidate.signal_id == signal_id
            )
            positions[position_index] = replace(
                positions[position_index],
                reason_codes=tuple(
                    dict.fromkeys(
                        (*positions[position_index].reason_codes, *event_reasons)
                    )
                ),
            )
        postings.append(
            PostingIntent(
                posting_key=_posting_key(event_id, "BUY"),
                account_name="SETTLED_CASH",
                entry_kind="BUY",
                amount_micros=-total_cost,
                shares_delta=action.quantity,
                unit_price_micros=unit_cost,
                occurred_at=event_time,
                details=(("source_event_id", event_id), ("version", 1)),
            )
        )
    elif action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}:
        assert action.symbol is not None
        assert action.quantity is not None
        assert action.price is not None
        matching = [
            (index, position)
            for index, position in enumerate(positions)
            if position.symbol == action.symbol
            and position.shares > 0
            and position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        ]
        if len(matching) == 1:
            index, position = matching[0]
            requested = action.quantity
            eligible_shares = sum(
                lot.remaining_shares
                for lot in position.lots
                if lot.acquired_at <= event_time
            )
            matched = min(requested, eligible_shares)
            remaining_to_consume = matched
            remaining_lots: list[ActualLot] = []
            for lot in sorted(
                position.lots,
                key=lambda item: (
                    item.acquired_at.astimezone(UTC),
                    item.source_cursor,
                    item.source_event_id,
                ),
            ):
                consumed = (
                    0
                    if lot.acquired_at > event_time
                    else min(lot.remaining_shares, remaining_to_consume)
                )
                remaining_to_consume -= consumed
                if consumed < lot.remaining_shares:
                    remaining_lots.append(
                        replace(
                            lot,
                            remaining_shares=lot.remaining_shares - consumed,
                        )
                    )
            unit_price = money_to_micros(action.price)
            gross = unit_price * matched
            if gross > MAX_MICRODOLLARS:
                raise ValueError("ACTUAL_POSTING_OVERFLOW")
            sale_session = event_time.astimezone(_ET).date()
            try:
                available_on = calendar.add_sessions(sale_session, 1)
            except RiskBlock as error:
                if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                    raise
                available_on = None
            if (
                available_on is not None
                and received_at <= query_cutoff
                and available_on <= query_cutoff.astimezone(_ET).date()
            ):
                strategy_cash += gross
                if strategy_cash > MAX_MICRODOLLARS:
                    raise ValueError("ACTUAL_CASH_OVERFLOW")
            if matched:
                postings.append(
                PostingIntent(
                    posting_key=_posting_key(event_id, "SALE"),
                    account_name="SETTLED_CASH",
                    entry_kind="SALE",
                    amount_micros=gross,
                    shares_delta=-matched,
                    unit_price_micros=unit_price,
                    occurred_at=event_time,
                    details=(("source_event_id", event_id), ("version", 1)),
                    )
                )
            sale_total = position.cumulative_sale_proceeds_micros + gross
            if matched == 0:
                pass
            elif remaining_lots:
                if matched == eligible_shares:
                    # A delayed close can reveal that the remaining, later-dated
                    # lots belong to a new economic lifecycle.  Keep prior
                    # receipt-attributed fees/stops with the lifecycle they were
                    # recorded against; only the future buy sources cross the
                    # boundary, and no control survives without its provenance.
                    survivor_source_ids = {
                        lot.source_event_id for lot in remaining_lots
                    }
                    survivor_lifecycle_ids = tuple(
                        source_event_id
                        for source_event_id in position.lifecycle_event_ids
                        if source_event_id in survivor_source_ids
                    )
                    trade_source_ids = (
                        *(
                            source_event_id
                            for source_event_id in position.lifecycle_event_ids
                            if source_event_id not in survivor_source_ids
                        ),
                        event_id,
                    )
                    survivor_buy_cost = sum(
                        lot.remaining_shares * lot.unit_cost_micros
                        for lot in remaining_lots
                    )
                    closed_buy_cost = (
                        position.cumulative_buy_cost_micros
                        - survivor_buy_cost
                    )
                    _require_int(survivor_buy_cost, minimum=0)
                    _require_int(closed_buy_cost, minimum=0)
                    trade_digest = hashlib.sha256(
                        json.dumps(
                            {
                                "source_cursor": event_cursor,
                                "source_event_ids": trade_source_ids,
                                "source_ordinal": event_ordinal,
                                "version": 2,
                            },
                            ensure_ascii=True,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest()
                    closed.append(
                        ActualClosedTrade(
                            signal_id=position.signal_id,
                            symbol=position.symbol,
                            opened_at=min(
                                lot.acquired_at
                                for lot in position.lots
                                if lot.acquired_at <= event_time
                            ),
                            closed_at=event_time,
                            source_cursor=event_cursor,
                            source_ordinal=event_ordinal,
                            buy_cost_micros=closed_buy_cost,
                            gross_sale_micros=sale_total,
                            fees_micros=position.linked_fees_micros,
                            pnl_micros=(
                                sale_total
                                - closed_buy_cost
                                - position.linked_fees_micros
                            ),
                            source_event_ids=trade_source_ids,
                            source_digest=trade_digest,
                        )
                    )
                    positions[index] = replace(
                        position,
                        lots=tuple(remaining_lots),
                        user_stop_micros=None,
                        cumulative_buy_cost_micros=survivor_buy_cost,
                        cumulative_sale_proceeds_micros=0,
                        linked_fees_micros=0,
                        lifecycle_event_ids=survivor_lifecycle_ids,
                    )
                else:
                    positions[index] = replace(
                        position,
                        lots=tuple(remaining_lots),
                        cumulative_sale_proceeds_micros=sale_total,
                        lifecycle_event_ids=(
                            *position.lifecycle_event_ids,
                            event_id,
                        ),
                    )
            else:
                positions.pop(index)
                trade_source_ids = (
                    (*position.lifecycle_event_ids, event_id)
                    if position.lifecycle_event_ids
                    else tuple(
                        [lot.source_event_id for lot in position.lots]
                        + [event_id]
                    )
                )
                trade_digest = hashlib.sha256(
                    json.dumps(
                        {
                            "source_cursor": event_cursor,
                            "source_event_ids": trade_source_ids,
                            "source_ordinal": event_ordinal,
                            "version": 2,
                        },
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest()
                closed.append(
                    ActualClosedTrade(
                        signal_id=position.signal_id,
                        symbol=position.symbol,
                        opened_at=min(lot.acquired_at for lot in position.lots),
                        closed_at=event_time,
                        source_cursor=event_cursor,
                        source_ordinal=event_ordinal,
                        buy_cost_micros=position.cumulative_buy_cost_micros,
                        gross_sale_micros=sale_total,
                        fees_micros=position.linked_fees_micros,
                        pnl_micros=(
                            sale_total
                            - position.cumulative_buy_cost_micros
                            - position.linked_fees_micros
                        ),
                        source_event_ids=trade_source_ids,
                        source_digest=trade_digest,
                    )
                )
    elif action.kind is ConfirmationKind.STOP_UPDATED:
        assert action.symbol is not None and action.stop is not None
        matching = [
            index
            for index, position in enumerate(positions)
            if position.symbol == action.symbol
            and position.lineage_kind in {"ACTUAL_EVENT", "ACTUAL_GROUP"}
        ]
        if len(matching) == 1:
            index = matching[0]
            prior_position = positions[index]
            if "DELAYED_STOP_PRECEDES_POSITION" in event_reasons:
                pass
            elif "STALE_STOP_OBSERVATION" in event_reasons:
                positions[index] = replace(
                    prior_position,
                    lifecycle_event_ids=(
                        *prior_position.lifecycle_event_ids,
                        event_id,
                    ),
                )
            else:
                retained_reasons = tuple(
                    reason
                    for reason in prior_position.reason_codes
                    if reason
                    not in {
                        "CONFIRMED_STOP_MISSING",
                        "STOP_WIDENING_PROHIBITED",
                    }
                )
                positions[index] = replace(
                    prior_position,
                    user_stop_micros=money_to_micros(action.stop),
                    reason_codes=tuple(
                        dict.fromkeys(
                            (*retained_reasons, *event_reasons)
                        )
                    ),
                    lifecycle_event_ids=(
                        *prior_position.lifecycle_event_ids,
                        event_id,
                    ),
                )
    elif action.kind is ConfirmationKind.ACCOUNT_CHECK:
        assert action.settled_cash is not None
        user_cash = money_to_micros(action.settled_cash)
        postings.append(
            PostingIntent(
                posting_key=_posting_key(event_id, "ACCOUNT_CHECK"),
                account_name="ACCOUNT_EVIDENCE",
                entry_kind="ACCOUNT_CHECK",
                amount_micros=0,
                shares_delta=None,
                unit_price_micros=None,
                occurred_at=event_time,
                details=(("source_event_id", event_id), ("version", 1)),
            )
        )
    elif action.kind is ConfirmationKind.FEE:
        assert action.amount is not None
        fee_micros = money_to_micros(action.amount)
        matching = [
            index
            for index, position in enumerate(positions)
            if signal_id is not None and position.signal_id == signal_id
        ]
        attributed = (
            len(matching) == 1
            and "DELAYED_FEE_PRECEDES_POSITION" not in event_reasons
        )
        if attributed:
            strategy_cash -= fee_micros
            index = matching[0]
            positions[index] = replace(
                positions[index],
                linked_fees_micros=(
                    positions[index].linked_fees_micros + fee_micros
                ),
                lifecycle_event_ids=(
                    *positions[index].lifecycle_event_ids,
                    event_id,
                ),
            )
        postings.append(
            PostingIntent(
                posting_key=_posting_key(event_id, "FEE"),
                account_name=(
                    "STRATEGY_FEES" if attributed else "ACCOUNT_EVIDENCE"
                ),
                entry_kind="FEE",
                amount_micros=-fee_micros,
                shares_delta=None,
                unit_price_micros=None,
                occurred_at=event_time,
                details=(("source_event_id", event_id), ("version", 1)),
            )
        )
    elif action.kind is ConfirmationKind.RECONCILE_CASH:
        assert action.amount is not None
        adjustment = money_to_micros(action.amount)
        if user_cash is not None:
            user_cash += adjustment
            if user_cash < 0:
                event_reasons = (*event_reasons, "ACCOUNT_CASH_NEGATIVE")
        else:
            event_reasons = (*event_reasons, "ACCOUNT_CASH_BASELINE_UNAVAILABLE")
        postings.append(
            PostingIntent(
                posting_key=_posting_key(event_id, "RECONCILE_CASH"),
                account_name="ACCOUNT_EVIDENCE",
                entry_kind="RECONCILE_CASH",
                amount_micros=adjustment,
                shares_delta=None,
                unit_price_micros=None,
                occurred_at=event_time,
                details=(("source_event_id", event_id), ("version", 1)),
            )
        )
    elif action.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION:
        assert action.symbol is not None
        assert action.signed_shares is not None
        assert action.price is not None
        unrelated_signal_id = _unrelated_position_lineage_id(action.symbol)
        if signal_id != unrelated_signal_id:
            raise ValueError("ACTUAL_LINEAGE_MISMATCH")
        matching = next(
            (
                index
                for index, position in enumerate(positions)
                if position.signal_id == unrelated_signal_id
            ),
            None,
        )
        signed_shares = action.signed_shares
        unit_price = money_to_micros(action.price)
        if signed_shares > 0:
            lot = ActualLot(
                source_event_id=event_id,
                source_cursor=event_cursor,
                remaining_shares=signed_shares,
                unit_cost_micros=unit_price,
                acquired_at=event_time,
                received_at=received_at,
            )
            if matching is None:
                positions.append(
                    ActualPositionState(
                        signal_id=unrelated_signal_id,
                        symbol=action.symbol,
                        lineage_kind="UNRELATED_POSITION",
                        signal_digest=None,
                        lots=(lot,),
                        recommended_stop_micros=None,
                        user_stop_micros=None,
                        target_micros=None,
                        tick_micros=None,
                        cumulative_buy_cost_micros=0,
                        cumulative_sale_proceeds_micros=0,
                        linked_fees_micros=0,
                        reason_codes=("UNRELATED_POSITION_RECONCILIATION",),
                        lifecycle_event_ids=(event_id,),
                    )
                )
            else:
                position = positions[matching]
                positions[matching] = replace(
                    position,
                    lots=(*position.lots, lot),
                    lifecycle_event_ids=(*position.lifecycle_event_ids, event_id),
                )
        elif (
            matching is not None
            and abs(signed_shares) <= positions[matching].shares
        ):
            position = positions[matching]
            eligible_shares, _ = _position_timing_eligibility(
                (position,),
                event_time,
            )
            remaining_to_remove = min(
                abs(signed_shares),
                eligible_shares,
            )
            removed_total = remaining_to_remove
            remaining_lots: list[ActualLot] = []
            for lot in sorted(
                position.lots,
                key=lambda item: (
                    item.acquired_at.astimezone(UTC),
                    item.source_cursor,
                    item.source_event_id,
                ),
            ):
                removed = (
                    0
                    if lot.acquired_at > event_time
                    else min(remaining_to_remove, lot.remaining_shares)
                )
                remaining_to_remove -= removed
                if removed < lot.remaining_shares:
                    remaining_lots.append(
                        replace(lot, remaining_shares=lot.remaining_shares - removed)
                    )
            if removed_total == 0:
                pass
            elif remaining_lots:
                positions[matching] = replace(
                    position,
                    lots=tuple(remaining_lots),
                    lifecycle_event_ids=(*position.lifecycle_event_ids, event_id),
                )
            else:
                positions.pop(matching)
        postings.append(
            PostingIntent(
                posting_key=_posting_key(event_id, "UNRELATED_POSITION"),
                account_name="ACCOUNT_EVIDENCE",
                entry_kind="UNRELATED_POSITION",
                amount_micros=0,
                shares_delta=signed_shares,
                unit_price_micros=unit_price,
                occurred_at=event_time,
                details=(("source_event_id", event_id), ("version", 1)),
            )
        )

    active_reasons = tuple(
        dict.fromkeys(
            (
                *global_reasons,
                *(
                    reason
                    for position in positions
                    for reason in position.reason_codes
                ),
            )
        )
    )
    if action.kind in {
        ConfirmationKind.RECONCILE_UNRELATED_POSITION,
        ConfirmationKind.RECONCILE_PENDING_ORDERS,
    } or (
        action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}
        and "OVER_SELL_REPORTED" in event_reasons
    ):
        active_reasons = tuple(dict.fromkeys((*active_reasons, *event_reasons)))
    if any(
        reason
        in {
            "CALENDAR_COVERAGE_MISSING",
            "SETTLEMENT_EVENT_OUTSIDE_MARKET_SESSION",
            "DELAYED_EXIT_PRECEDES_POSITION",
            "DELAYED_EXIT_ATTRIBUTION_UNRESOLVED",
            "DELAYED_FEE_PRECEDES_POSITION",
            "DELAYED_STOP_PRECEDES_POSITION",
            "DELAYED_UNRELATED_POSITION_REDUCTION_PRECEDES_POSITION",
            "STALE_STOP_OBSERVATION",
        }
        for reason in event_reasons
    ):
        active_reasons = tuple(dict.fromkeys((*active_reasons, *event_reasons)))
    if action.kind in {
        ConfirmationKind.ACCOUNT_CHECK,
        ConfirmationKind.FEE,
        ConfirmationKind.RECONCILE_CASH,
        ConfirmationKind.RECONCILE_PENDING_ORDERS,
        ConfirmationKind.STOP_UPDATED,
    } or (
        action.kind in {ConfirmationKind.SOLD, ConfirmationKind.STOP_FILLED}
        and any(
            reason
            in {
                "POSITION_NOT_TRACKED",
                "MULTIPLE_POSITION_LINEAGES_UNRESOLVED",
                "NON_STRATEGY_POSITION_LINEAGE",
            }
            for reason in event_reasons
        )
    ):
        active_reasons = tuple(dict.fromkeys((*active_reasons, *event_reasons)))
    after = _new_actual_state(
        query_cutoff=received_at,
        through_cursor=event_cursor,
        positions=tuple(positions),
        closed_trades=tuple(closed),
        settlement_ledger=before.settlement_ledger,
        strategy_settled_cash_micros=strategy_cash,
        user_confirmed_cash_micros=user_cash,
        reconciliation_reasons=active_reasons,
        cache_matches_replay=True,
        calendar_digest=before.calendar_digest,
        calendar_release_verified=before.calendar_release_verified,
        journal_source_digest=before.journal_source_digest,
        policy_digest=before.policy_digest,
    )
    return ActualTransition(
        before_digest=before.source_digest,
        source_event_id=event_id,
        decision=assessment,
        posting_intents=tuple(postings),
        after_state=after,
        projection_authority_kind="IMMUTABLE_EXECUTION_REPLAY",
        state_digest=after.source_digest,
    )


def _transition_with_account_observation(
    transition: ActualTransition,
    before: ActualLedgerState,
) -> ActualTransition:
    current = transition.after_state
    account_reason_codes = {
        "ACCOUNT_CASH_BASELINE_UNAVAILABLE",
        "ACCOUNT_CASH_NEGATIVE",
        "PENDING_ORDERS_PRESENT",
        "UNLOGGED_POSITIONS_PRESENT",
    }
    restored_reasons = tuple(
        dict.fromkeys(
            (
                *(
                    reason
                    for reason in current.reconciliation_reasons
                    if reason not in account_reason_codes
                ),
                *(
                    reason
                    for reason in before.reconciliation_reasons
                    if reason in account_reason_codes
                ),
            )
        )
    )
    after = _new_actual_state(
        query_cutoff=current.query_cutoff,
        through_cursor=current.through_cursor,
        positions=current.positions,
        closed_trades=current.closed_trades,
        settlement_ledger=current.settlement_ledger,
        strategy_settled_cash_micros=current.strategy_settled_cash_micros,
        user_confirmed_cash_micros=before.user_confirmed_cash_micros,
        reconciliation_reasons=restored_reasons,
        cache_matches_replay=current.cache_matches_replay,
        calendar_digest=current.calendar_digest,
        calendar_release_verified=current.calendar_release_verified,
        journal_source_digest=current.journal_source_digest,
        policy_digest=current.policy_digest,
    )
    return replace(
        transition,
        after_state=after,
        state_digest=after.source_digest,
    )


def _parsed_action_from_source(source: JournalActionSource) -> ParsedConfirmation | None:
    parsed = parse_confirmation_batch_or_pending(
        source.raw_text,
        session_date=source.message_time.astimezone(_ET).date(),
    )
    if isinstance(parsed, PendingConfirmation):
        return None
    if source.action_ordinal >= len(parsed):
        raise ValueError("JOURNAL_ACTION_ORDINAL_INCOMPLETE")
    return parsed[source.action_ordinal]


def _validate_actual_posting_closure(
    observed: tuple[JournalPostingSource, ...],
    expected: tuple[tuple[JournalActionSource, ParsedConfirmation, PostingIntent], ...],
) -> None:
    if len(observed) != len(expected):
        raise ValueError("ACTUAL_POSTING_CLOSURE_MISMATCH")
    by_key = {posting.posting_key: posting for posting in observed}
    if len(by_key) != len(observed):
        raise ValueError("ACTUAL_POSTING_CLOSURE_MISMATCH")
    for action_source, action, intent in expected:
        posting = by_key.get(intent.posting_key)
        expected_details = dict(intent.details)
        try:
            stored_details = json.loads(posting.details_json) if posting else None
        except json.JSONDecodeError as error:
            raise ValueError("ACTUAL_POSTING_CLOSURE_MISMATCH") from error
        expected_execution_event_id = (
            None
            if action.kind is ConfirmationKind.ACCOUNT_CHECK
            else action_source.execution_event_id
        )
        expected_account_check_id = (
            action_source.account_check.row_id
            if action.kind is ConfirmationKind.ACCOUNT_CHECK
            and action_source.account_check is not None
            else None
        )
        if (
            posting is None
            or posting.ledger_name != "ACTUAL"
            or posting.account_name != intent.account_name
            or posting.entry_kind != intent.entry_kind
            or posting.execution_event_id != expected_execution_event_id
            or posting.account_check_id != expected_account_check_id
            or posting.symbol != action.symbol
            or posting.amount_micros != intent.amount_micros
            or posting.shares_delta != intent.shares_delta
            or posting.unit_price_micros != intent.unit_price_micros
            or posting.occurred_at != intent.occurred_at
            or stored_details != expected_details
        ):
            raise ValueError("ACTUAL_POSTING_CLOSURE_MISMATCH")


def _cache_matches_actual_replay(
    source: JournalActualReplaySource,
    state: ActualLedgerState,
    *,
    policy: Policy,
    position_history: dict[str, list[JournalActionSource]],
    cash_history: list[tuple[JournalActionSource, tuple[PostingIntent, ...]]],
    reconciliation_history: list[JournalActionSource],
) -> bool:
    if state.strategy_settled_cash_micros < 0 or (
        state.user_confirmed_cash_micros is not None
        and state.user_confirmed_cash_micros < 0
    ):
        return False
    expected_projection_through = max(
        (
            action.execution_event_id
            for action in source.actions
            if action.domain_kind in _ACTUAL_PROJECTION_DOMAIN_KINDS
        ),
        default=None,
    )
    if source.projection_through_cursor != expected_projection_through:
        return False
    rows_by_table: dict[str, list[dict[str, object]]] = {}
    for row in source.projection_rows:
        rows_by_table.setdefault(row.table, []).append(dict(row.values))

    position_rows = rows_by_table.get("actual_positions", [])
    if len(position_rows) != len(position_history):
        return False
    positions_by_signal = {position.signal_id: position for position in state.positions}
    closed_by_signal = {trade.signal_id: trade for trade in state.closed_trades}
    observed_positions = {
        str(row.get("signal_id")): row for row in position_rows
    }
    if len(observed_positions) != len(position_rows):
        return False
    if any(signal_id not in position_history for signal_id in positions_by_signal):
        return False
    for signal_id, history in position_history.items():
        latest = history[-1]
        row = observed_positions.get(signal_id)
        if row is None:
            return False
        position = positions_by_signal.get(signal_id)
        if position is None:
            trade = closed_by_signal.get(signal_id)
            expected_symbol = latest.symbol if trade is None else trade.symbol
            expected = {
                "signal_id": signal_id,
                "symbol": expected_symbol,
                "shares": 0,
                "cost_basis_micros": 0,
                "recommended_stop_micros": None,
                "user_confirmed_stop_micros": None,
                "target_micros": None,
            }
        else:
            expected = {
                "signal_id": signal_id,
                "symbol": position.symbol,
                "shares": position.shares,
                "cost_basis_micros": position.cost_basis_micros,
                "recommended_stop_micros": position.recommended_stop_micros,
                "user_confirmed_stop_micros": position.user_stop_micros,
                "target_micros": position.target_micros,
            }
        expected.update(
            {
                "last_execution_event_id": latest.execution_event_id,
                "updated_at": _canonical_timestamp(latest.received_at),
                "revision": len(history),
            }
        )
        if any(row.get(name) != value for name, value in expected.items()):
            return False

    cash_rows = rows_by_table.get("actual_cash_projection", [])
    if cash_history:
        if len(cash_rows) != 1:
            return False
        latest_action, latest_intents = cash_history[-1]
        latest_key = latest_intents[-1].posting_key
        posting_by_key = {posting.posting_key: posting for posting in source.postings}
        latest_posting = posting_by_key.get(latest_key)
        if latest_posting is None:
            return False
        consecutive_losses, weekly_high_water, monthly_high_water = (
            _actual_performance_metrics(state, policy)
        )
        cash_expected = {
            "estimated_settled_cash_micros": state.strategy_settled_cash_micros,
            "user_confirmed_settled_cash_micros": state.user_confirmed_cash_micros,
            "deployed_capital_micros": sum(
                position.cost_basis_micros for position in state.positions
            ),
            "open_planned_risk_micros": _actual_open_risk_micros(state),
            "consecutive_losses": consecutive_losses,
            "weekly_high_water_micros": weekly_high_water,
            "monthly_high_water_micros": monthly_high_water,
            "last_ledger_posting_id": latest_posting.row_id,
            "updated_at": _canonical_timestamp(latest_action.received_at),
            "revision": len(cash_history),
        }
        if any(
            cash_rows[0].get(name) != value
            for name, value in cash_expected.items()
        ):
            return False
    elif cash_rows:
        return False

    reconciliation_rows = rows_by_table.get("reconciliation_projection", [])
    if reconciliation_history:
        if len(reconciliation_rows) != 1:
            return False
        latest = reconciliation_history[-1]
        reasons = state.reconciliation_reasons
        reconciliation_expected = {
            "reconciliation_required": int(bool(reasons)),
            "reason": ",".join(reasons) if reasons else None,
            "last_execution_event_id": latest.execution_event_id,
            "updated_at": _canonical_timestamp(latest.received_at),
            "revision": len(reconciliation_history),
        }
        if any(
            reconciliation_rows[0].get(name) != value
            for name, value in reconciliation_expected.items()
        ):
            return False
    elif reconciliation_rows:
        return False
    return not source.projection_stale


def _assess_actual_action(
    action: ParsedConfirmation,
    state: ActualLedgerState,
    *,
    signal_resolver: SignalPlanResolver,
    query_cutoff: datetime,
    calendar: SessionCalendarResolver,
    stop_effective_at: dict[str, datetime],
) -> ActionAssessment:
    assessment = assess_confirmation(
        action,
        _projection_state_from_actual(state),
        signal_resolver=signal_resolver,
        query_cutoff=query_cutoff,
    )
    assessment = _with_settlement_calendar_assessment(
        assessment,
        action,
        calendar,
    )
    assessment = _with_actual_cash_assessment(
        assessment,
        action,
        state,
    )
    assessment = _with_stale_stop_assessment(
        assessment,
        action,
        (
            None
            if assessment.signal_id is None
            else stop_effective_at.get(assessment.signal_id)
        ),
    )
    assessment = _with_actual_timing_assessment(
        assessment,
        action,
        state,
    )
    if (
        action.kind in {ConfirmationKind.BUY, ConfirmationKind.PARTIAL_FILL}
        and "AUTHORITY_CONTEXT_UNVERIFIED" not in assessment.reason_codes
    ):
        assessment = ActionAssessment(
            ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED,
            (*assessment.reason_codes, "AUTHORITY_CONTEXT_UNVERIFIED"),
            assessment.apply_economic_event,
            assessment.signal_id,
        )
    return assessment


def _replay_actual_source(
    source: JournalActualReplaySource,
    *,
    plans: SignalPlanResolver,
    calendar: SessionCalendarResolver,
    policy: Policy,
    require_verified: bool,
) -> ActualLedgerState:
    if require_verified and not is_verified_journal_replay_source(source):
        raise ValueError("JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED")
    if not isinstance(plans, SignalPlanResolver):
        raise TypeError("plans must implement SignalPlanResolver")
    if not isinstance(calendar, SessionCalendarResolver):
        raise TypeError("calendar must be a SessionCalendarResolver")
    if not isinstance(policy, Policy):
        raise TypeError("policy must be a Policy")
    state = _empty_actual_state(source.query_cutoff, policy, calendar)
    expected_postings: list[
        tuple[JournalActionSource, ParsedConfirmation, PostingIntent]
    ] = []
    position_history: dict[str, list[JournalActionSource]] = {}
    cash_history: list[
        tuple[JournalActionSource, tuple[PostingIntent, ...]]
    ] = []
    reconciliation_history: list[JournalActionSource] = []
    materialized: list[tuple[JournalActionSource, ParsedConfirmation]] = []
    for action_source in source.actions:
        parsed = _parsed_action_from_source(action_source)
        if parsed is not None:
            materialized.append((action_source, parsed))
    materialized.sort(
        key=lambda item: (
            item[0].execution_event_id,
            item[0].action_ordinal,
            item[0].event_id,
        )
    )
    account_value_effective_at: datetime | None = None
    stop_effective_at: dict[str, datetime] = {}
    cache_economic_highwater: datetime | None = None
    for action_source, parsed in materialized:
        event_time = (
            action_source.message_time
            if parsed.event_time is None
            else parsed.event_time
        )
        cache_write_allowed = (
            cache_economic_highwater is None
            or event_time >= cache_economic_highwater
        )
        # Task 7 has no persisted signal-plan authority.  Callers may supply a
        # resolver for ingestion diagnostics, but it is not replay evidence.
        # Authoritative replay therefore uses the same fail-closed context for
        # every caller until Task 8 persists and authenticates plan lineage.
        assessment = _assess_actual_action(
            parsed,
            state,
            signal_resolver=UnavailableSignalPlanResolver(),
            query_cutoff=action_source.received_at,
            calendar=calendar,
            stop_effective_at=stop_effective_at,
        )
        if parsed.kind not in {
            ConfirmationKind.BUY,
            ConfirmationKind.PARTIAL_FILL,
            ConfirmationKind.SOLD,
            ConfirmationKind.STOP_FILLED,
            ConfirmationKind.STOP_UPDATED,
            ConfirmationKind.ACCOUNT_CHECK,
            ConfirmationKind.FEE,
            ConfirmationKind.RECONCILE_CASH,
            ConfirmationKind.RECONCILE_UNRELATED_POSITION,
            ConfirmationKind.RECONCILE_PENDING_ORDERS,
        }:
            continue
        signal_id = action_source.signal_id
        lineage_kind = (
            None if signal_id is None else _actual_lineage_kind(signal_id)
        )
        if parsed.kind in {ConfirmationKind.BUY, ConfirmationKind.PARTIAL_FILL}:
            derived_signal_id, derived_lineage_kind = _resolve_actual_entry_lineage(
                state,
                parsed,
                action_source.event_id,
                None,
            )
            if signal_id != derived_signal_id:
                raise ValueError("ACTUAL_LINEAGE_MISMATCH")
            lineage_kind = derived_lineage_kind
        elif parsed.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION:
            assert parsed.symbol is not None
            derived_signal_id = _unrelated_position_lineage_id(parsed.symbol)
            if signal_id != derived_signal_id:
                raise ValueError("ACTUAL_LINEAGE_MISMATCH")
            lineage_kind = "UNRELATED_POSITION"
        elif assessment.signal_id is not None and signal_id != assessment.signal_id:
            raise ValueError("ACTUAL_LINEAGE_MISMATCH")
        transition = _transition_actual(
            state,
            parsed,
            assessment,
            event_id=action_source.event_id,
            event_cursor=action_source.execution_event_id,
            event_ordinal=action_source.action_ordinal,
            signal_id=signal_id,
            lineage_kind=lineage_kind,
            message_time=action_source.message_time,
            received_at=action_source.received_at,
            policy=policy,
            query_cutoff=source.query_cutoff,
            calendar=calendar,
        )
        if parsed.kind.value in _ACCOUNT_VALUE_DOMAIN_KINDS:
            assert parsed.event_time is not None
            if (
                account_value_effective_at is not None
                and parsed.event_time < account_value_effective_at
            ):
                transition = _transition_with_account_observation(
                    transition,
                    state,
                )
            else:
                account_value_effective_at = parsed.event_time
        if (
            parsed.kind is ConfirmationKind.STOP_UPDATED
            and assessment.signal_id is not None
            and parsed.event_time is not None
            and "STALE_STOP_OBSERVATION" not in assessment.reason_codes
            and "DELAYED_STOP_PRECEDES_POSITION"
            not in assessment.reason_codes
        ):
            stop_effective_at[assessment.signal_id] = parsed.event_time
        expected_postings.extend(
            (action_source, parsed, intent)
            for intent in transition.posting_intents
        )
        if (
            cache_write_allowed
            and
            parsed.kind
            in {
                ConfirmationKind.BUY,
                ConfirmationKind.PARTIAL_FILL,
                ConfirmationKind.SOLD,
                ConfirmationKind.STOP_FILLED,
                ConfirmationKind.STOP_UPDATED,
                ConfirmationKind.RECONCILE_UNRELATED_POSITION,
            }
            and action_source.signal_id is not None
        ):
            position_history.setdefault(action_source.signal_id, []).append(
                action_source
            )
        if (
            cache_write_allowed
            and transition.posting_intents
            and transition.after_state.strategy_settled_cash_micros >= 0
            and (
                transition.after_state.user_confirmed_cash_micros is None
                or transition.after_state.user_confirmed_cash_micros >= 0
            )
        ):
            cash_history.append((action_source, transition.posting_intents))
        if cache_write_allowed and parsed.kind.value in {
            "ACCOUNT_CHECK",
            "BOUGHT",
            "FEE",
            "PARTIAL_FILL",
            "RECONCILE_CASH",
            "RECONCILE_PENDING_ORDERS",
            "RECONCILE_UNRELATED_POSITION",
            "SOLD",
            "STOP_FILLED",
            "STOP_UPDATED",
        }:
            reconciliation_history.append(action_source)
        state = transition.after_state
        if (
            cache_write_allowed
            and parsed.kind.value in _ACTUAL_PROJECTION_DOMAIN_KINDS
        ):
            cache_economic_highwater = event_time
    _validate_actual_posting_closure(
        source.postings,
        tuple(expected_postings),
    )
    replayed = _new_actual_state(
        query_cutoff=source.query_cutoff,
        through_cursor=source.terminal_cursor,
        positions=state.positions,
        closed_trades=state.closed_trades,
        settlement_ledger=source.postings,
        strategy_settled_cash_micros=state.strategy_settled_cash_micros,
        user_confirmed_cash_micros=state.user_confirmed_cash_micros,
        reconciliation_reasons=state.reconciliation_reasons,
        cache_matches_replay=False,
        calendar_digest=_calendar_digest(calendar),
        calendar_release_verified=calendar.release_verified,
        journal_source_digest=source.source_digest,
        policy_digest=_policy_digest(policy),
    )
    return replace(
        replayed,
        cache_matches_replay=_cache_matches_actual_replay(
            source,
            replayed,
            policy=policy,
            position_history=position_history,
            cash_history=cash_history,
            reconciliation_history=reconciliation_history,
        ),
    )


def _make_actual_ledger_state_authority_api(
    *,
    trusted_globals: dict[str, object],
    issuer_code: object,
    frame_getter: object,
    replay_builder: object,
    authority_lock: RLock,
    registry: dict[int, tuple[object, ...]],
    state_type: type[ActualLedgerState],
    position_type: type[ActualPositionState],
    lot_type: type[ActualLot],
    closed_trade_type: type[ActualClosedTrade],
    source_type: type[JournalActualReplaySource],
    resolver_protocol: type[SignalPlanResolver],
    unavailable_resolver_type: type[UnavailableSignalPlanResolver],
    calendar_resolver_type: type[SessionCalendarResolver],
    market_calendar_type: type[MarketCalendar],
    policy_type: type[Policy],
    policy_validate: object,
    validated_calendar_registry: dict[int, tuple[object, ...]],
    release_calendar_registry: dict[int, tuple[object, ...]],
    calendar_authority_lock: RLock,
    calendar_fingerprint: object,
    journal_fingerprints_equal: object,
    journal_source_verifier: object,
    journal_candidate_factory: object,
    journal_candidate_recheck: object,
    seal_factory: object,
    seal_equal: object,
    opaque_structural_mode: str,
) -> tuple[object, object]:
    """Build the callback-first, closure-captured actual-state mint seam."""

    required_callables = (
        frame_getter,
        replay_builder,
        policy_validate,
        calendar_fingerprint,
        journal_fingerprints_equal,
        journal_source_verifier,
        journal_candidate_factory,
        journal_candidate_recheck,
        seal_factory,
        seal_equal,
    )
    if any(not callable(value) for value in required_callables):
        raise TypeError("actual ledger state authority dependency is invalid")

    function_type = type(_make_actual_ledger_state_authority_api)
    missing = object()
    binding_manifest: list[tuple[dict[str, object], str, object]] = []
    class_manifest: list[tuple[type[object], tuple[tuple[str, object], ...]]] = []
    seen_bindings: set[tuple[int, str]] = set()
    seen_functions: set[int] = set()
    seen_classes: set[int] = set()
    pending = [
        replay_builder,
        policy_validate,
        calendar_fingerprint,
        journal_fingerprints_equal,
        journal_source_verifier,
        journal_candidate_factory,
        journal_candidate_recheck,
        seal_factory,
        seal_equal,
    ]

    def enqueue_class_callables(dependency_type: type[object]) -> None:
        if id(dependency_type) in seen_classes:
            return
        seen_classes.add(id(dependency_type))
        expected_items = tuple(vars(dependency_type).items())
        class_manifest.append((dependency_type, expected_items))
        for _name, member in expected_items:
            if type(member) is function_type:
                pending.append(member)
            elif type(member) in {classmethod, staticmethod}:
                pending.append(member.__func__)
            elif type(member) is property:
                pending.extend(
                    function
                    for function in (member.fget, member.fset, member.fdel)
                    if function is not None
                )

    # Replay dispatches through these exact class methods and properties.  Seed
    # their code and globals explicitly: a self-restoring class-method patch is
    # otherwise invisible to a manifest rooted only at module-level helpers.
    enqueue_class_callables(calendar_resolver_type)
    enqueue_class_callables(market_calendar_type)

    while pending:
        dependency = pending.pop()
        if type(dependency) is not function_type or id(dependency) in seen_functions:
            continue
        seen_functions.add(id(dependency))
        dependency_globals = dependency.__globals__
        for name in dependency.__code__.co_names:
            if name not in dependency_globals:
                continue
            value = dependency_globals[name]
            binding_key = (id(dependency_globals), name)
            if binding_key not in seen_bindings:
                seen_bindings.add(binding_key)
                binding_manifest.append((dependency_globals, name, value))
            if (
                type(value) is function_type
                and type(value.__module__) is str
                and value.__module__.startswith("stock_monitor.")
            ):
                pending.append(value)
            elif (
                type(value) is type
                and type(value.__module__) is str
                and value.__module__.startswith("stock_monitor.")
                and id(value) not in seen_classes
            ):
                seen_classes.add(id(value))
                class_manifest.append((value, tuple(vars(value).items())))

    binding_manifest.append(
        (trusted_globals, "_replay_actual_source", replay_builder)
    )
    binding_manifest_tuple = tuple(binding_manifest)
    class_manifest_tuple = tuple(class_manifest)
    policy_fields = (
        ("capital", Decimal),
        ("max_live_exposure", Decimal),
        ("max_position_risk", Decimal),
        ("max_combined_risk", Decimal),
        ("max_positions", int),
        ("max_entries_per_session", int),
        ("min_score", int),
        ("max_monthly_drawdown", Decimal),
        ("max_weekly_drawdown", Decimal),
        ("universe_max_age_days", int),
        ("live_quote_max_age_seconds", int),
        ("disagreement_tolerance", Decimal),
    )
    calendar_domain = b"stock-monitor/actual-state-calendar-material/v1"
    policy_domain = b"stock-monitor/actual-state-policy-material/v1"
    state_domain = _ACTUAL_LEDGER_STATE_FINGERPRINT_DOMAIN
    object_getattribute = object.__getattribute__
    object_new = object.__new__
    object_setattr = object.__setattr__
    weak_reference = ref
    timezone_type = type(UTC)
    timedelta_type = timedelta
    zoneinfo_type = ZoneInfo

    def dependencies_are_current() -> bool:
        for namespace, name, expected in binding_manifest_tuple:
            if namespace.get(name, missing) is not expected:
                return False
        for dependency_type, expected_items in class_manifest_tuple:
            current = vars(dependency_type)
            if len(current) != len(expected_items) or any(
                current.get(name, missing) is not expected
                for name, expected in expected_items
            ):
                return False
        return True

    def fingerprint(value: object, *, domain: bytes) -> object:
        return seal_factory(  # type: ignore[operator]
            value,
            domain=domain,
            root_mode=opaque_structural_mode,
        )

    def calendar_authorities_are_current(
        safe_calendar: object,
        captured: object,
        *,
        require_release: bool,
    ) -> bool:
        if (
            type(safe_calendar) is not calendar_resolver_type
            or type(require_release) is not bool
            or object_getattribute(safe_calendar, "_require_release")
            is not require_release
            or type(captured) is not tuple
        ):
            return False
        calendars = object_getattribute(safe_calendar, "calendars")
        if type(calendars) is not tuple or len(calendars) != len(captured):
            return False
        for item, authority in zip(calendars, captured, strict=True):
            if (
                type(item) is not market_calendar_type
                or type(authority) is not tuple
                or len(authority) != 4
                or authority[0] is not item
            ):
                return False
            expected_seal, validated_record, release_record = authority[1:]
            try:
                # The opaque seal runs before the legacy calendar fingerprint,
                # so an injected container/leaf cannot dispatch equality hooks.
                if not seal_equal(  # type: ignore[operator]
                    expected_seal,
                    fingerprint(item, domain=calendar_domain),
                ):
                    return False
                current_calendar_fingerprint = calendar_fingerprint(item)  # type: ignore[operator]
                if (
                    type(validated_record) is not tuple
                    or len(validated_record) != 2
                    or validated_record[0]() is not item
                    or not journal_fingerprints_equal(  # type: ignore[operator]
                        validated_record[1],
                        current_calendar_fingerprint,
                    )
                    or (
                        require_release
                        and (
                            type(release_record) is not tuple
                            or len(release_record) != 2
                            or release_record[0]() is not item
                            or not journal_fingerprints_equal(  # type: ignore[operator]
                                release_record[1],
                                current_calendar_fingerprint,
                            )
                        )
                    )
                ):
                    return False
            except Exception:
                return False
            with calendar_authority_lock:
                if (
                    validated_calendar_registry.get(id(item))
                    is not validated_record
                    or (
                        require_release
                        and release_calendar_registry.get(id(item))
                        is not release_record
                    )
                ):
                    return False
            try:
                if not seal_equal(  # type: ignore[operator]
                    expected_seal,
                    fingerprint(item, domain=calendar_domain),
                ):
                    return False
            except Exception:
                return False
        try:
            return safe_calendar.release_verified is require_release
        except Exception:
            return False

    def material_is_current(material: object) -> bool:
        if type(material) is not tuple or len(material) != 5:
            return False
        (
            safe_calendar,
            calendar_seal,
            safe_policy,
            policy_seal,
            calendar_authorities,
        ) = material
        if (
            type(safe_calendar) is not calendar_resolver_type
            or type(safe_policy) is not policy_type
        ):
            return False
        require_release = object_getattribute(
            safe_calendar,
            "_require_release",
        )
        if type(require_release) is not bool:
            return False
        try:
            return bool(
                seal_equal(  # type: ignore[operator]
                    calendar_seal,
                    fingerprint(safe_calendar, domain=calendar_domain),
                )
                and seal_equal(  # type: ignore[operator]
                    policy_seal,
                    fingerprint(safe_policy, domain=policy_domain),
                )
                and calendar_authorities_are_current(
                    safe_calendar,
                    calendar_authorities,
                    require_release=require_release,
                )
            )
        except Exception:
            return False

    def datetime_value(value: object) -> tuple[object, ...] | None:
        if type(value) is not datetime:
            return None
        timezone = object_getattribute(value, "tzinfo")
        if type(timezone) not in {timezone_type, zoneinfo_type}:
            return None
        try:
            offset = datetime.utcoffset(value)
        except Exception:
            return None
        if type(offset) is not timedelta_type:
            return None
        zone_key: str | None = None
        if type(timezone) is zoneinfo_type:
            zone_key = object_getattribute(timezone, "key")
            if type(zone_key) is not str:
                return None
        return (
            b"datetime",
            object_getattribute(value, "year"),
            object_getattribute(value, "month"),
            object_getattribute(value, "day"),
            object_getattribute(value, "hour"),
            object_getattribute(value, "minute"),
            object_getattribute(value, "second"),
            object_getattribute(value, "microsecond"),
            object_getattribute(value, "fold"),
            object_getattribute(offset, "days"),
            object_getattribute(offset, "seconds"),
            object_getattribute(offset, "microseconds"),
            zone_key,
        )

    def scalar_or_tuple(value: object) -> object:
        if value is None or type(value) in {bool, int, str, bytes}:
            return value
        if type(value) is datetime:
            normalized = datetime_value(value)
            return missing if normalized is None else normalized
        if type(value) is tuple:
            normalized_items: list[object] = []
            for item in value:
                normalized = scalar_or_tuple(item)
                if normalized is missing:
                    return missing
                normalized_items.append(normalized)
            return tuple(normalized_items)
        return missing

    def lot_value(value: object) -> tuple[object, ...] | None:
        if type(value) is not lot_type:
            return None
        normalized = tuple(
            scalar_or_tuple(object_getattribute(value, name))
            for name in (
                "source_event_id",
                "source_cursor",
                "remaining_shares",
                "unit_cost_micros",
                "acquired_at",
                "received_at",
                "parent_order_id",
            )
        )
        if any(item is missing for item in normalized):
            return None
        return (lot_type, *normalized)

    def position_value(value: object) -> tuple[object, ...] | None:
        if type(value) is not position_type:
            return None
        lots = object_getattribute(value, "lots")
        if type(lots) is not tuple:
            return None
        normalized_lots = tuple(lot_value(item) for item in lots)
        if any(item is None for item in normalized_lots):
            return None
        normalized = tuple(
            scalar_or_tuple(object_getattribute(value, name))
            for name in (
                "signal_id",
                "symbol",
                "lineage_kind",
                "signal_digest",
                "recommended_stop_micros",
                "user_stop_micros",
                "target_micros",
                "tick_micros",
                "cumulative_buy_cost_micros",
                "cumulative_sale_proceeds_micros",
                "linked_fees_micros",
                "reason_codes",
                "lifecycle_event_ids",
            )
        )
        if any(item is missing for item in normalized):
            return None
        return (position_type, normalized_lots, *normalized)

    def closed_trade_value(value: object) -> tuple[object, ...] | None:
        if type(value) is not closed_trade_type:
            return None
        normalized = tuple(
            scalar_or_tuple(object_getattribute(value, name))
            for name in (
                "signal_id",
                "symbol",
                "opened_at",
                "closed_at",
                "source_cursor",
                "source_ordinal",
                "buy_cost_micros",
                "gross_sale_micros",
                "fees_micros",
                "pnl_micros",
                "source_event_ids",
                "source_digest",
            )
        )
        if any(item is missing for item in normalized):
            return None
        return (closed_trade_type, *normalized)

    def state_semantic_value(value: object) -> tuple[object, ...] | None:
        if type(value) is not state_type:
            return None
        positions = object_getattribute(value, "positions")
        closed_trades = object_getattribute(value, "closed_trades")
        if type(positions) is not tuple or type(closed_trades) is not tuple:
            return None
        normalized_positions = tuple(position_value(item) for item in positions)
        normalized_closed = tuple(
            closed_trade_value(item) for item in closed_trades
        )
        if any(item is None for item in normalized_positions) or any(
            item is None for item in normalized_closed
        ):
            return None
        normalized = tuple(
            scalar_or_tuple(object_getattribute(value, name))
            for name in (
                "query_cutoff",
                "through_cursor",
                "strategy_settled_cash_micros",
                "user_confirmed_cash_micros",
                "reconciliation_reasons",
                "source_digest",
                "cache_matches_replay",
                "calendar_digest",
                "calendar_release_verified",
                "journal_source_digest",
                "policy_digest",
            )
        )
        if any(item is missing for item in normalized):
            return None
        # Settlement rows are independently pinned to the exact Journal tuple.
        return (
            state_type,
            normalized_positions,
            normalized_closed,
            b"exact-journal-postings",
            *normalized,
        )

    def derive_expected(
        source: JournalActualReplaySource,
        source_candidate: object,
        material: object,
    ) -> ActualLedgerState | None:
        if (
            type(source) is not source_type
            or not dependencies_are_current()
            or not material_is_current(material)
            or not journal_candidate_recheck(source_candidate)  # type: ignore[operator]
        ):
            return None
        (
            safe_calendar,
            _calendar_seal,
            safe_policy,
            _policy_seal,
            _calendar_authorities,
        ) = material
        try:
            expected = replay_builder(  # type: ignore[operator]
                source,
                plans=unavailable_resolver_type(),
                calendar=safe_calendar,
                policy=safe_policy,
                require_verified=False,
            )
        except Exception:
            return None
        if (
            type(expected) is not state_type
            or expected.settlement_ledger is not source.postings
            or expected.query_cutoff is not source.query_cutoff
            or not dependencies_are_current()
            or not material_is_current(material)
            or not journal_candidate_recheck(source_candidate)  # type: ignore[operator]
        ):
            return None
        return expected

    def semantic_rechecker(
        state: object,
        source: object,
        source_candidate: object,
        expected_state_seal: object,
        material: object,
    ) -> bool:
        if type(state) is not state_type or type(source) is not source_type:
            return False
        expected = derive_expected(source, source_candidate, material)
        if expected is None:
            return False
        try:
            current_state_seal = fingerprint(state, domain=state_domain)
        except Exception:
            return False
        current_semantics = state_semantic_value(state)
        expected_semantics = state_semantic_value(expected)
        return bool(
            state.settlement_ledger is source.postings
            and state.query_cutoff is source.query_cutoff
            and expected.settlement_ledger is source.postings
            and expected.query_cutoff is source.query_cutoff
            and current_semantics is not None
            and expected_semantics is not None
            and seal_equal(  # type: ignore[operator]
                expected_state_seal,
                current_state_seal,
            )
            and journal_fingerprints_equal(  # type: ignore[operator]
                current_semantics,
                expected_semantics,
            )
            and dependencies_are_current()
            and material_is_current(material)
            and journal_candidate_recheck(source_candidate)  # type: ignore[operator]
        )

    def installer(
        source: object,
        *,
        plans: object,
        calendar: object,
        policy: object,
    ) -> ActualLedgerState:
        caller = frame_getter(1)  # type: ignore[operator]
        if caller.f_code is not issuer_code or caller.f_globals is not trusted_globals:
            raise RiskBlock("ACTUAL_LEDGER_STATE_ISSUER_UNVERIFIED")
        if type(source) is not source_type:
            raise TypeError("source must be a JournalActualReplaySource")
        if not isinstance(plans, resolver_protocol):
            raise TypeError("plans must implement SignalPlanResolver")
        if not isinstance(calendar, calendar_resolver_type):
            raise TypeError("calendar must be a SessionCalendarResolver")
        if type(policy) is not policy_type:
            raise TypeError("policy must be an exact Policy")

        policy_validate(policy)  # type: ignore[operator]
        policy_values: dict[str, object] = {}
        for name, required_type in policy_fields:
            value = object_getattribute(policy, name)
            if type(value) is not required_type:
                raise ValueError("ACTUAL_LEDGER_POLICY_UNVERIFIED")
            policy_values[name] = value
        safe_policy = policy_type(**policy_values)
        policy_validate(safe_policy)  # type: ignore[operator]

        release_verified = calendar.release_verified
        if type(release_verified) is not bool:
            raise ValueError("ACTUAL_LEDGER_CALENDAR_UNVERIFIED")
        calendars = object_getattribute(calendar, "calendars")
        if type(calendars) is not tuple or not calendars or any(
            type(item) is not market_calendar_type for item in calendars
        ):
            raise ValueError("ACTUAL_LEDGER_CALENDAR_UNVERIFIED")
        calendars = tuple(calendars)
        with calendar_authority_lock:
            validated_records = tuple(
                validated_calendar_registry.get(id(item)) for item in calendars
            )
            release_records = tuple(
                release_calendar_registry.get(id(item)) for item in calendars
            )
        safe_calendar = object_new(calendar_resolver_type)
        object_setattr(safe_calendar, "calendars", calendars)
        object_setattr(safe_calendar, "_require_release", release_verified)
        calendar_authorities = tuple(
            (
                item,
                fingerprint(item, domain=calendar_domain),
                validated_record,
                release_record,
            )
            for item, validated_record, release_record in zip(
                calendars,
                validated_records,
                release_records,
                strict=True,
            )
        )
        if not calendar_authorities_are_current(
            safe_calendar,
            calendar_authorities,
            require_release=release_verified,
        ):
            raise ValueError("ACTUAL_LEDGER_CALENDAR_UNVERIFIED")

        # The accepted resolver/protocol checks above may dispatch caller code.
        # Journal verification and candidate capture are therefore deliberately
        # last before the callback-free reconstruction phase.
        if not journal_source_verifier(source):  # type: ignore[operator]
            raise ValueError("JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED")
        source_candidate = journal_candidate_factory(source)  # type: ignore[operator]
        if source_candidate is None or not journal_candidate_recheck(  # type: ignore[operator]
            source_candidate
        ):
            raise ValueError("JOURNAL_ACTUAL_REPLAY_SOURCE_UNVERIFIED")
        if not dependencies_are_current() or not calendar_authorities_are_current(
            safe_calendar,
            calendar_authorities,
            require_release=release_verified,
        ):
            raise ValueError("ACTUAL_LEDGER_CONSTRUCTION_DEPENDENCY_UNVERIFIED")

        material = (
            safe_calendar,
            fingerprint(safe_calendar, domain=calendar_domain),
            safe_policy,
            fingerprint(safe_policy, domain=policy_domain),
            calendar_authorities,
        )
        state = derive_expected(source, source_candidate, material)
        if state is None:
            raise ValueError("ACTUAL_LEDGER_STATE_CONTENT_UNVERIFIED")
        if not release_verified:
            return state
        state_seal = fingerprint(state, domain=state_domain)
        if not semantic_rechecker(
            state,
            source,
            source_candidate,
            state_seal,
            material,
        ):
            raise ValueError("ACTUAL_LEDGER_STATE_CONTENT_UNVERIFIED")

        key = id(state)

        def discard(dead: ReferenceType[object]) -> None:
            with authority_lock:
                current = registry.get(key)
                if current is not None and current[0] is dead:
                    registry.pop(key, None)

        issued = (
            weak_reference(state, discard),
            state_seal,
            weak_reference(source),
            source_candidate,
            semantic_rechecker,
            material,
        )
        with authority_lock:
            current = registry.get(key)
            if current is not None:
                raise RiskBlock("ACTUAL_LEDGER_STATE_ALREADY_ISSUED")
            registry[key] = issued
        return state

    return semantic_rechecker, installer


def replay_actual(
    source: JournalActualReplaySource,
    *,
    plans: SignalPlanResolver,
    calendar: SessionCalendarResolver,
    policy: Policy,
) -> ActualLedgerState:
    """Replay receipt-stable ownership with effective-time lot eligibility."""
    return _install_actual_ledger_state_authority(
        source,
        plans=plans,
        calendar=calendar,
        policy=policy,
    )


(
    _actual_state_semantic_rechecker,
    _install_actual_ledger_state_authority,
) = _make_actual_ledger_state_authority_api(
    trusted_globals=globals(),
    issuer_code=replay_actual.__code__,
    frame_getter=_getframe,
    replay_builder=_replay_actual_source,
    authority_lock=_ACTUAL_STATE_AUTHORITY_LOCK,
    registry=_ACTUAL_STATE_AUTHORITIES,
    state_type=ActualLedgerState,
    position_type=ActualPositionState,
    lot_type=ActualLot,
    closed_trade_type=ActualClosedTrade,
    source_type=JournalActualReplaySource,
    resolver_protocol=SignalPlanResolver,
    unavailable_resolver_type=UnavailableSignalPlanResolver,
    calendar_resolver_type=SessionCalendarResolver,
    market_calendar_type=MarketCalendar,
    policy_type=Policy,
    policy_validate=Policy.validate,
    validated_calendar_registry=(
        _market_calendar_authority_module._VALIDATED_CALENDARS
    ),
    release_calendar_registry=(
        _market_calendar_authority_module._RELEASE_CALENDARS
    ),
    calendar_authority_lock=(
        _market_calendar_authority_module._CALENDAR_AUTHORITY_LOCK
    ),
    calendar_fingerprint=(
        _market_calendar_authority_module._calendar_fingerprint
    ),
    journal_fingerprints_equal=_journal_authority_module._fingerprints_equal,
    journal_source_verifier=is_verified_journal_replay_source,
    journal_candidate_factory=(
        _journal_authority_module._journal_replay_source_authority_candidate
    ),
    journal_candidate_recheck=(
        _journal_authority_module._is_current_journal_authority_candidate_without_callbacks
    ),
    seal_factory=_journal_authority_module._source_fingerprint_seal,
    seal_equal=_journal_authority_module._source_fingerprint_seals_equal,
    opaque_structural_mode=(
        _journal_authority_module._MERKLE_OPAQUE_STRUCTURAL
    ),
)
(
    _actual_state_authority_fingerprint,
    _actual_ledger_state_authority_candidate,
    _is_current_actual_ledger_state_authority_candidate_without_callbacks,
) = _make_actual_ledger_state_authority_candidate_api(
    authority_lock=_ACTUAL_STATE_AUTHORITY_LOCK,
    registry=_ACTUAL_STATE_AUTHORITIES,
    state_type=ActualLedgerState,
    source_type=JournalActualReplaySource,
    seal_factory=_journal_authority_module._source_fingerprint_seal,
    seal_equal=_journal_authority_module._source_fingerprint_seals_equal,
    fingerprint_domain=_ACTUAL_LEDGER_STATE_FINGERPRINT_DOMAIN,
    opaque_structural_mode=(
        _journal_authority_module._MERKLE_OPAQUE_STRUCTURAL
    ),
    semantic_rechecker=_actual_state_semantic_rechecker,
)
del (
    _actual_state_semantic_rechecker,
    _make_actual_ledger_state_authority_api,
    _make_actual_ledger_state_authority_candidate_api,
)


def plan_actual_transition(
    before: ActualLedgerState,
    source: JournalActionSource,
    *,
    plans: SignalPlanResolver,
    calendar: SessionCalendarResolver,
    policy: Policy,
) -> ActualTransition:
    """Plan one immutable transition from one exact Journal action source."""
    if not isinstance(before, ActualLedgerState):
        raise TypeError("before must be an ActualLedgerState")
    if not is_verified_actual_ledger_state(before):
        raise ValueError("ACTUAL_LEDGER_STATE_UNVERIFIED")
    if not is_verified_journal_action_source(source):
        raise ValueError("JOURNAL_ACTION_SOURCE_UNVERIFIED")
    if (
        before.calendar_digest != _calendar_digest(calendar)
        or before.policy_digest != _policy_digest(policy)
    ):
        raise ValueError("ACTUAL_LEDGER_STATE_PROVENANCE_MISMATCH")
    # JournalActionSource does not bind the complete predecessor cohort. A
    # numeric cursor can be spliced from another Journal, so Task 7 exposes
    # only complete replay as an operational transition boundary.
    del plans
    raise ValueError("ACTUAL_TRANSITION_SOURCE_COHORT_UNVERIFIED")


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _normalized_details(
    *,
    envelope: ConfirmationEnvelope,
    action: ParsedConfirmation | PendingConfirmation,
    assessment: ActionAssessment,
    acknowledgement_destination: str,
    acknowledgement_idempotency_key: str,
) -> dict[str, object]:
    if isinstance(action, PendingConfirmation):
        domain_kind = ConfirmationKind.PENDING_CLARIFICATION.value
        time_basis = "MESSAGE_TIME_OBSERVATION"
        missing_fields: list[str] = []
        normalized: dict[str, object] = {}
    else:
        domain_kind = action.kind.value
        time_basis = action.event_time_basis
        missing_fields = sorted(action.missing_fields)
        normalized = {
            "amount_decimal": (
                None if action.amount is None else str(action.amount)
            ),
            "asset_id": action.asset_id,
            "delta": None if action.delta is None else str(action.delta),
            "fill_group_planned_shares": action.fill_group_planned_shares,
            "occ_symbol": action.occ_symbol,
            "open_interest": action.open_interest,
            "parent_order_id": action.parent_order_id,
            "pending_orders": action.pending_orders,
            "reason_sha256": (
                None
                if action.reason is None
                else hashlib.sha256(action.reason.encode("utf-8")).hexdigest()
            ),
            "signed_shares": action.signed_shares,
            "unlogged_positions": action.unlogged_positions,
            "volume": action.volume,
        }
    return {
        "acknowledgement": {
            "destination": acknowledgement_destination,
            "idempotency_key": acknowledgement_idempotency_key,
        },
        "version": 1,
        "domain_kind": domain_kind,
        "event_time_basis": time_basis,
        "missing_fields": missing_fields,
        "normalized": normalized,
        "reason_codes": list(assessment.reason_codes),
        "source": {
            "received_at": _canonical_timestamp(envelope.received_at),
            "type": "ROBINHOOD_MANUAL_CONFIRMATION",
        },
    }


def _event_outbox_key(event_id: str, destination: str) -> str:
    digest = hashlib.sha256(
        b"stock-monitor/confirmation-outbox/v1\x00"
        + event_id.encode("utf-8")
        + b"\x00"
        + destination.encode("utf-8")
    ).hexdigest()
    return f"confirmation:{digest}"


def _stored_to_result(
    stored: StoredIngestionResult,
    *,
    duplicate: bool,
) -> IngestionResult:
    actions: list[IngestionActionResult] = []
    for action in stored.actions:
        try:
            kind = ConfirmationKind(action.domain_kind)
            status = ActionStatus(action.status)
        except ValueError as error:
            raise ValueError("STORED_CONFIRMATION_CONTRACT_INVALID") from error
        actions.append(
            IngestionActionResult(
                ordinal=action.ordinal,
                event_row_id=action.event_row_id,
                event_id=action.event_id,
                kind=kind,
                status=status,
                reason_codes=action.reason_codes,
                authority_event_row_id=None,
                outbox_id=action.outbox_id,
            )
        )
    return IngestionResult(
        raw_row_id=stored.raw_row_id,
        message_id=stored.message_id,
        duplicate=duplicate,
        actions=tuple(actions),
        state_digest=stored.state_digest,
    )


def _ingest_confirmation_impl(
    journal: Journal,
    envelope: ConfirmationEnvelope,
    *,
    plans: SignalPlanResolver,
    calendar: SessionCalendarResolver,
    policy: Policy,
    entry_authorities: ActualEntryAuthorityResolver,
    destination: str = "CODEX_TASK",
    _trusted_replay_builder: object,
    _trusted_private_state_builder: object,
    _trusted_checkpoint_loader: object,
    _trusted_checkpoint_installer: object,
) -> IngestionResult:
    """Persist a complete source message and its actions in one transaction."""
    if not isinstance(journal, Journal):
        raise TypeError("journal must be a Journal")
    if not isinstance(envelope, ConfirmationEnvelope):
        raise TypeError("envelope must be a ConfirmationEnvelope")
    if not isinstance(plans, SignalPlanResolver):
        raise TypeError("plans must implement SignalPlanResolver")
    if not isinstance(calendar, SessionCalendarResolver):
        raise TypeError("calendar must be a SessionCalendarResolver")
    if not isinstance(policy, Policy):
        raise TypeError("policy must be a Policy")
    if not isinstance(entry_authorities, ActualEntryAuthorityResolver):
        raise TypeError("entry authorities must implement ActualEntryAuthorityResolver")
    if type(destination) is not str or not destination:
        raise ValueError("INVALID_OUTBOX_DESTINATION")

    parsed = parse_confirmation_batch_or_pending(
        envelope.text,
        session_date=envelope.session_date,
    )
    actions: tuple[ParsedConfirmation | PendingConfirmation, ...]
    actions = (parsed,) if isinstance(parsed, PendingConfirmation) else parsed
    with journal.transaction() as transaction:
        # The optimization is deliberately private and stays inside BEGIN
        # IMMEDIATE.  Any owner generation, SQLite data_version, high-water,
        # calendar, policy, cutoff-date, or content mismatch falls back to the
        # exact full replay before the candidate raw row is inserted.
        prior_identity = transaction._incremental_ingestion_identity()
        checkpoint = _trusted_checkpoint_loader(  # type: ignore[operator]
            journal,
            identity=prior_identity,
            query_cutoff=envelope.received_at,
            calendar=calendar,
            policy=policy,
        )
        if checkpoint is None:
            prior_source = journal._read_actual_replay(
                query_cutoff=envelope.received_at,
                through_execution_cursor=None,
            )
            actual_state = _trusted_replay_builder(  # type: ignore[operator]
                prior_source,
                plans=UnavailableSignalPlanResolver(),
                calendar=calendar,
                policy=policy,
                require_verified=False,
            )
            cache_economic_highwater = max(
                (
                    source_action.event_time
                    for source_action in prior_source.actions
                    if source_action.domain_kind
                    in _ACTUAL_PROJECTION_DOMAIN_KINDS
                ),
                default=None,
            )
            account_value_effective_at = max(
                (
                    source_action.event_time
                    for source_action in prior_source.actions
                    if source_action.domain_kind in _ACCOUNT_VALUE_DOMAIN_KINDS
                ),
                default=None,
            )
            stop_effective_at: dict[str, datetime] = {}
            for source_action in prior_source.actions:
                if (
                    source_action.domain_kind
                    == ConfirmationKind.STOP_UPDATED.value
                    and source_action.signal_id is not None
                ):
                    prior = stop_effective_at.get(source_action.signal_id)
                    if prior is None or source_action.event_time > prior:
                        stop_effective_at[source_action.signal_id] = (
                            source_action.event_time
                        )
            actual_state = _trusted_private_state_builder(  # type: ignore[operator]
                actual_state,
                query_cutoff=envelope.received_at,
                through_cursor=prior_source.terminal_cursor,
            )
        else:
            actual_state = _trusted_private_state_builder(  # type: ignore[operator]
                checkpoint.state,
                query_cutoff=envelope.received_at,
                through_cursor=checkpoint.terminal_cursor,
            )
            cache_economic_highwater = checkpoint.cache_economic_highwater
            account_value_effective_at = checkpoint.account_value_effective_at
            stop_effective_at = dict(checkpoint.stop_effective_at)
        state = _projection_state_from_actual(actual_state)
        raw_row_id, duplicate = transaction.append_raw_message(
            envelope.message_id,
            envelope.message_time,
            envelope.text,
        )
        if duplicate:
            stored = transaction.read_confirmation_result(
                message_id=envelope.message_id
            )
            if stored is None:
                raise IdempotencyConflict(
                    "duplicate confirmation has no completed stored result"
                )
            if stored.received_at != envelope.received_at:
                raise IdempotencyConflict(
                    "confirmation receipt identity conflicts with stored content"
                )
            if any(
                action.outbox_destination != destination
                for action in stored.actions
            ):
                raise IdempotencyConflict(
                    "confirmation destination conflicts with stored content"
                )
            return _stored_to_result(stored, duplicate=True)

        transaction.validate_confirmation_source_order(
            message_time=envelope.message_time,
            received_at=envelope.received_at,
        )

        for ordinal, action in enumerate(actions):
            event_id, _ = stable_execution_event_identity(
                envelope.message_id,
                ordinal,
            )
            acknowledgement_key = _event_outbox_key(event_id, destination)
            if isinstance(action, PendingConfirmation):
                assessment = ActionAssessment(
                    ActionStatus.PENDING_CLARIFICATION,
                    (action.reason_code,),
                    False,
                )
                event_time = envelope.message_time
                storage_action = ConfirmationKind.PENDING_CLARIFICATION.value
                symbol = None
                shares = None
                price_micros = None
                bid_micros = None
                ask_micros = None
                user_stop_micros = None
                event_signal_id = None
                lineage_kind = None
            else:
                # Preserve caller-plan diagnostics in the durable event and
                # acknowledgement, while deriving all actual-ledger state and
                # projections from Task 7's deterministic fail-closed context.
                assessment = _assess_actual_action(
                    action,
                    actual_state,
                    signal_resolver=plans,
                    query_cutoff=envelope.received_at,
                    calendar=calendar,
                    stop_effective_at=stop_effective_at,
                )
                authoritative_assessment = _assess_actual_action(
                    action,
                    actual_state,
                    signal_resolver=UnavailableSignalPlanResolver(),
                    query_cutoff=envelope.received_at,
                    calendar=calendar,
                    stop_effective_at=stop_effective_at,
                )
                event_time = (
                    envelope.message_time
                    if action.event_time is None
                    else action.event_time
                )
                storage_action = action.kind.value
                symbol = action.symbol
                shares = action.quantity
                price_micros = (
                    None if action.price is None else money_to_micros(action.price)
                )
                bid_micros = (
                    None if action.bid is None else money_to_micros(action.bid)
                )
                ask_micros = (
                    None if action.ask is None else money_to_micros(action.ask)
                )
                user_stop_micros = (
                    None if action.stop is None else money_to_micros(action.stop)
                )
                event_signal_id = assessment.signal_id
                lineage_kind = (
                    None
                    if event_signal_id is None
                    else _actual_lineage_kind(event_signal_id)
                )
                if action.kind in {
                    ConfirmationKind.BUY,
                    ConfirmationKind.PARTIAL_FILL,
                }:
                    event_signal_id, lineage_kind = _resolve_actual_entry_lineage(
                        actual_state,
                        action,
                        event_id,
                        None,
                    )
                elif action.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION:
                    assert action.symbol is not None
                    event_signal_id = _unrelated_position_lineage_id(action.symbol)
                    lineage_kind = "UNRELATED_POSITION"
            details = _normalized_details(
                envelope=envelope,
                action=action,
                assessment=assessment,
                acknowledgement_destination=destination,
                acknowledgement_idempotency_key=acknowledgement_key,
            )
            reconciliation_state = (
                "PENDING"
                if assessment.status is ActionStatus.PENDING_CLARIFICATION
                else "REQUIRED"
                if assessment.status
                is ActionStatus.NONCOMPLIANT_RECONCILIATION_REQUIRED
                else "CLEAR"
            )
            event_row_id, event_duplicate = transaction.append_execution_event(
                raw_message_id=raw_row_id,
                action_ordinal=ordinal,
                parsed_action=storage_action,
                event_time=event_time,
                signal_id=event_signal_id,
                symbol=symbol,
                shares=shares,
                price_micros=price_micros,
                bid_micros=bid_micros,
                ask_micros=ask_micros,
                user_confirmed_stop_micros=user_stop_micros,
                compliance_result=assessment.status.value,
                reconciliation_state=reconciliation_state,
                details=details,
            )
            if event_duplicate:
                raise IdempotencyConflict(
                    "new confirmation unexpectedly reused an event identity"
                )
            account_check_row_id: int | None = None
            if (
                isinstance(action, ParsedConfirmation)
                and action.kind is ConfirmationKind.ACCOUNT_CHECK
            ):
                assert action.settled_cash is not None
                assert action.pending_orders is not None
                assert action.unlogged_positions is not None
                account_check_row_id, account_check_duplicate = (
                    transaction.append_account_check(
                    execution_event_id=event_row_id,
                    settled_cash_micros=money_to_micros(action.settled_cash),
                    pending_order_count=action.pending_orders,
                    unlogged_position_count=action.unlogged_positions,
                    confirmed_at=event_time,
                    reconciliation_result=(
                        "CLEAR"
                        if assessment.status is ActionStatus.COMPLIANT
                        else "RECONCILIATION_REQUIRED"
                    ),
                    details={"version": 1},
                    )
                )
                if account_check_duplicate:
                    raise IdempotencyConflict(
                        "new confirmation unexpectedly reused an account check"
                    )

            if isinstance(action, ParsedConfirmation):
                cache_write_allowed = (
                    cache_economic_highwater is None
                    or event_time >= cache_economic_highwater
                )
                transition = _transition_actual(
                    actual_state,
                    action,
                    authoritative_assessment,
                    event_id=event_id,
                    event_cursor=event_row_id,
                    event_ordinal=ordinal,
                    signal_id=event_signal_id,
                    lineage_kind=lineage_kind,
                    message_time=envelope.message_time,
                    received_at=envelope.received_at,
                    query_cutoff=envelope.received_at,
                    calendar=calendar,
                    policy=policy,
                )
                if action.kind.value in _ACCOUNT_VALUE_DOMAIN_KINDS:
                    if (
                        account_value_effective_at is not None
                        and event_time < account_value_effective_at
                    ):
                        transition = _transition_with_account_observation(
                            transition,
                            actual_state,
                        )
                    else:
                        account_value_effective_at = event_time
                if (
                    action.kind is ConfirmationKind.STOP_UPDATED
                    and authoritative_assessment.signal_id is not None
                    and "STALE_STOP_OBSERVATION"
                    not in authoritative_assessment.reason_codes
                    and "DELAYED_STOP_PRECEDES_POSITION"
                    not in authoritative_assessment.reason_codes
                ):
                    stop_effective_at[
                        authoritative_assessment.signal_id
                    ] = event_time
                last_posting_id: int | None = None
                for intent in transition.posting_intents:
                    last_posting_id, posting_duplicate = (
                        transaction.append_ledger_posting(
                            posting_key=intent.posting_key,
                            ledger_name="ACTUAL",
                            account_name=intent.account_name,
                            entry_kind=intent.entry_kind,
                            occurred_at=intent.occurred_at,
                            amount_micros=intent.amount_micros,
                            execution_event_id=(
                                None
                                if action.kind is ConfirmationKind.ACCOUNT_CHECK
                                else event_row_id
                            ),
                            account_check_id=(
                                account_check_row_id
                                if action.kind is ConfirmationKind.ACCOUNT_CHECK
                                else None
                            ),
                            symbol=action.symbol,
                            shares_delta=intent.shares_delta,
                            unit_price_micros=intent.unit_price_micros,
                            details=dict(intent.details),
                        )
                    )
                    if posting_duplicate:
                        raise IdempotencyConflict(
                            "new confirmation unexpectedly reused a posting identity"
                        )

                if cache_write_allowed and action.kind in {
                    ConfirmationKind.BUY,
                    ConfirmationKind.PARTIAL_FILL,
                    ConfirmationKind.SOLD,
                    ConfirmationKind.STOP_FILLED,
                    ConfirmationKind.STOP_UPDATED,
                    ConfirmationKind.RECONCILE_UNRELATED_POSITION,
                } and event_signal_id is not None and action.symbol is not None:
                    current_position = next(
                        (
                            position
                            for position in transition.after_state.positions
                            if position.signal_id == event_signal_id
                        ),
                        None,
                    )
                    transaction.write_actual_position(
                        signal_id=event_signal_id,
                        symbol=action.symbol,
                        shares=(
                            0 if current_position is None else current_position.shares
                        ),
                        cost_basis_micros=(
                            0
                            if current_position is None
                            else current_position.cost_basis_micros
                        ),
                        recommended_stop_micros=(
                            None
                            if current_position is None
                            else current_position.recommended_stop_micros
                        ),
                        user_confirmed_stop_micros=(
                            None
                            if current_position is None
                            else current_position.user_stop_micros
                        ),
                        target_micros=(
                            None
                            if current_position is None
                            else current_position.target_micros
                        ),
                        last_execution_event_id=event_row_id,
                        updated_at=envelope.received_at,
                    )

                if (
                    cache_write_allowed
                    and last_posting_id is not None
                    and transition.after_state.strategy_settled_cash_micros >= 0
                    and (
                        transition.after_state.user_confirmed_cash_micros is None
                        or transition.after_state.user_confirmed_cash_micros >= 0
                    )
                ):
                    deployed = sum(
                        position.cost_basis_micros
                        for position in transition.after_state.positions
                    )
                    (
                        consecutive_losses,
                        weekly_high_water,
                        monthly_high_water,
                    ) = _actual_performance_metrics(
                        transition.after_state,
                        policy,
                    )
                    transaction.write_actual_cash_projection(
                        estimated_settled_cash_micros=(
                            transition.after_state.strategy_settled_cash_micros
                        ),
                        user_confirmed_settled_cash_micros=(
                            transition.after_state.user_confirmed_cash_micros
                        ),
                        deployed_capital_micros=deployed,
                        open_planned_risk_micros=_actual_open_risk_micros(
                            transition.after_state
                        ),
                        consecutive_losses=consecutive_losses,
                        weekly_high_water_micros=weekly_high_water,
                        monthly_high_water_micros=monthly_high_water,
                        last_ledger_posting_id=last_posting_id,
                        updated_at=envelope.received_at,
                    )

                if cache_write_allowed and action.kind.value in {
                    "ACCOUNT_CHECK",
                    "BOUGHT",
                    "FEE",
                    "PARTIAL_FILL",
                    "RECONCILE_CASH",
                    "RECONCILE_PENDING_ORDERS",
                    "RECONCILE_UNRELATED_POSITION",
                    "SOLD",
                    "STOP_FILLED",
                    "STOP_UPDATED",
                }:
                    active_reasons = transition.after_state.reconciliation_reasons
                    transaction.write_reconciliation_projection(
                        reconciliation_required=bool(active_reasons),
                        reason=(
                            ",".join(active_reasons) if active_reasons else None
                        ),
                        last_execution_event_id=event_row_id,
                        updated_at=envelope.received_at,
                    )

                actual_state = transition.after_state
                state = _projection_state_from_actual(actual_state)
                if (
                    action.kind.value in _ACTUAL_PROJECTION_DOMAIN_KINDS
                    and cache_write_allowed
                ):
                    cache_economic_highwater = event_time
            payload = json.dumps(
                {
                    "event_id": event_id,
                    "kind": details["domain_kind"],
                    "ordinal": ordinal,
                    "reason_codes": list(assessment.reason_codes),
                    "status": assessment.status.value,
                    "version": 1,
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            transaction.append_outbox(
                idempotency_key=acknowledgement_key,
                origin_report_id=None,
                origin_execution_event_id=event_row_id,
                destination=destination,
                payload_text=payload,
                created_at=envelope.received_at,
            )

    with journal.transaction() as transaction:
        stored = transaction.read_confirmation_result(message_id=envelope.message_id)
        committed_identity = transaction._incremental_ingestion_identity()
    if stored is None:
        raise RuntimeError("committed confirmation could not be read back")
    terminal_cursor = stored.actions[-1].event_row_id
    if committed_identity[2] == terminal_cursor:
        _trusted_checkpoint_installer(  # type: ignore[operator]
            journal,
            identity=committed_identity,
            query_cutoff=envelope.received_at,
            terminal_cursor=terminal_cursor,
            actual_state=actual_state,
            calendar=calendar,
            policy=policy,
            cache_economic_highwater=cache_economic_highwater,
            account_value_effective_at=account_value_effective_at,
            stop_effective_at=tuple(sorted(stop_effective_at.items())),
        )
    return _stored_to_result(stored, duplicate=False)


(
    _load_incremental_ingestion_checkpoint,
    _install_incremental_ingestion_checkpoint,
) = _make_incremental_ingestion_checkpoint_api(
    trusted_globals=globals(),
    issuer_function=_ingest_confirmation_impl,
    frame_getter=_getframe,
    checkpoint_type=_IncrementalIngestionCheckpoint,
    journal_type=Journal,
    state_type=ActualLedgerState,
    calendar_type=SessionCalendarResolver,
    policy_type=Policy,
    policy_validate=Policy.validate,
    private_state_builder=_private_incremental_state,
    state_digest=_actual_state_digest,
    calendar_digest=_calendar_digest,
    policy_digest=_policy_digest,
    journal_data_version=Journal._source_authority_data_version,
    checkpoint_lock=_INGESTION_CHECKPOINT_LOCK,
    registry=_INGESTION_CHECKPOINTS,
    seal_factory=_journal_authority_module._source_fingerprint_seal,
    seal_equal=_journal_authority_module._source_fingerprint_seals_equal,
    opaque_structural_mode=(
        _journal_authority_module._MERKLE_OPAQUE_STRUCTURAL
    ),
)


def _make_ingest_confirmation_entrypoint(
    *,
    implementation: object,
    replay_builder: object,
    private_state_builder: object,
    checkpoint_loader: object,
    checkpoint_installer: object,
    policy_type: type[Policy],
    policy_validate: object,
) -> object:
    """Bind every private mutable ingestion dependency exactly once."""

    if any(
        not callable(value)
        for value in (
            implementation,
            replay_builder,
            private_state_builder,
            checkpoint_loader,
            checkpoint_installer,
            policy_validate,
        )
    ):
        raise TypeError("ingestion entrypoint dependency is invalid")

    def entrypoint(
        journal: Journal,
        envelope: ConfirmationEnvelope,
        *,
        plans: SignalPlanResolver,
        calendar: SessionCalendarResolver,
        policy: Policy,
        entry_authorities: ActualEntryAuthorityResolver,
        destination: str = "CODEX_TASK",
    ) -> IngestionResult:
        if type(policy) is not policy_type:
            raise TypeError("policy must be an exact Policy")
        policy_validate(policy)  # type: ignore[operator]
        return implementation(  # type: ignore[operator]
            journal,
            envelope,
            plans=plans,
            calendar=calendar,
            policy=policy,
            entry_authorities=entry_authorities,
            destination=destination,
            _trusted_replay_builder=replay_builder,
            _trusted_private_state_builder=private_state_builder,
            _trusted_checkpoint_loader=checkpoint_loader,
            _trusted_checkpoint_installer=checkpoint_installer,
        )

    entrypoint.__name__ = "ingest_confirmation"
    entrypoint.__qualname__ = "ingest_confirmation"
    entrypoint.__doc__ = implementation.__doc__
    return entrypoint


ingest_confirmation = _make_ingest_confirmation_entrypoint(
    implementation=_ingest_confirmation_impl,
    replay_builder=_replay_actual_source,
    private_state_builder=_private_incremental_state,
    checkpoint_loader=_load_incremental_ingestion_checkpoint,
    checkpoint_installer=_install_incremental_ingestion_checkpoint,
    policy_type=Policy,
    policy_validate=Policy.validate,
)
del (
    _ingest_confirmation_impl,
    _install_incremental_ingestion_checkpoint,
    _make_incremental_ingestion_checkpoint_api,
    _make_ingest_confirmation_entrypoint,
    _journal_authority_module,
    _market_calendar_authority_module,
    _getframe,
)


__all__ = [
    "ActionAssessment",
    "ActionStatus",
    "ActualClosedTrade",
    "ActualEntryAuthorityResolver",
    "ActualLedgerState",
    "ActualLot",
    "ActualPositionState",
    "ActualTransition",
    "IngestionActionResult",
    "IngestionResult",
    "PostingIntent",
    "ProjectionPosition",
    "ProjectionState",
    "ResolvedSignalPlan",
    "SignalPlanResolver",
    "UnavailableSignalPlanResolver",
    "UnavailableActualEntryAuthorityResolver",
    "assess_confirmation",
    "ingest_confirmation",
    "is_verified_actual_ledger_state",
    "is_verified_actual_ledger_state_for_source",
    "replay_actual",
]
