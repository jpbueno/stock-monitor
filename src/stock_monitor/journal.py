"""Transactional, append-only SQLite journal."""

from __future__ import annotations

import hashlib
import importlib.resources
import json
import re
import secrets
import sqlite3
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Self
from weakref import ReferenceType, ref

from .domain import money_to_micros, stable_execution_event_identity


APPLICATION_ID = 0x53544B4D
BUSY_TIMEOUT_MILLISECONDS = 5_000
REPORT_ID_PATH_PREFIX_LENGTH = 12
_REPORT_CLAIM_LEASE_SECONDS = 300
_ACTUAL_LEDGER_EVENT_ACTIONS = frozenset(
    {
        "BOUGHT",
        "BUY",
        "FEE",
        "PARTIAL_FILL",
        "RECONCILE_CASH",
        "RECONCILE_UNRELATED_POSITION",
        "SELL",
        "SOLD",
        "STOP_FILLED",
    }
)
_POSITION_MUTATING_ACTIONS = frozenset(
    {
        "BOUGHT",
        "PARTIAL_FILL",
        "RECONCILE_UNRELATED_POSITION",
        "SOLD",
        "STOP_FILLED",
        "STOP_UPDATED",
    }
)
_RECONCILIATION_ACTIONS = frozenset(
    {
        "ACCOUNT_CHECK",
        "BOUGHT",
        "FEE",
        "PARTIAL_FILL",
        "PENDING_CLARIFICATION",
        "RECONCILE_CASH",
        "RECONCILE_PENDING_ORDERS",
        "RECONCILE_UNRELATED_POSITION",
        "SOLD",
        "STOP_FILLED",
        "STOP_UPDATED",
    }
)
_MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{3})_[a-z][a-z0-9_]*\.sql$")
_TABLES = frozenset(
    {
        "schema_migrations",
        "raw_messages",
        "source_observations",
        "execution_events",
        "account_checks",
        "report_claims",
        "reports",
        "report_observations",
        "outbox",
        "outbox_delivery_attempts",
        "scheduled_runs",
        "ledger_postings",
        "actual_positions",
        "actual_cash_projection",
        "reconciliation_projection",
    }
)

_RAW_MESSAGE_COLUMNS = (
    "id",
    "message_id",
    "message_time",
    "raw_text",
    "raw_sha256",
)
_EXECUTION_EVENT_COLUMNS = (
    "id",
    "event_id",
    "raw_message_id",
    "action_ordinal",
    "idempotency_key",
    "signal_id",
    "parsed_action",
    "symbol",
    "shares",
    "price_micros",
    "bid_micros",
    "ask_micros",
    "recommended_stop_micros",
    "user_confirmed_stop_micros",
    "event_time",
    "message_time",
    "compliance_result",
    "reconciliation_state",
    "details_json",
)
_ACCOUNT_CHECK_COLUMNS = (
    "id",
    "check_id",
    "raw_message_id",
    "execution_event_id",
    "settled_cash_micros",
    "pending_order_count",
    "unlogged_position_count",
    "confirmed_at",
    "reconciliation_result",
    "details_json",
)
_LEDGER_POSTING_COLUMNS = (
    "id",
    "posting_key",
    "ledger_name",
    "account_name",
    "entry_kind",
    "execution_event_id",
    "account_check_id",
    "symbol",
    "amount_micros",
    "shares_delta",
    "unit_price_micros",
    "occurred_at",
    "details_json",
)
_OUTBOX_COLUMNS = (
    "id",
    "idempotency_key",
    "origin_report_id",
    "origin_execution_event_id",
    "destination",
    "payload_text",
    "payload_sha256",
    "created_at",
)


class JournalError(Exception):
    """Base class for journal failures safe to expose to callers."""


class JournalBusy(JournalError):
    """The journal remained locked beyond its bounded busy timeout."""


class MigrationDrift(JournalError):
    """An applied migration no longer matches its packaged bytes."""


class MigrationCorruption(JournalError):
    """Migration metadata or database ownership is inconsistent."""


class IdempotencyConflict(JournalError):
    """A stable idempotency identity was reused for different content."""


class InvalidJournalValue(JournalError, ValueError):
    """A caller supplied a value that cannot be stored canonically."""


@dataclass(frozen=True)
class ReportClaim:
    """Result of serializing one report kind for one market session."""

    claim_id: int
    session_date: date
    report_kind: str
    status: str
    claim_token: str | None
    lease_expires_at: datetime
    report_row_id: int | None
    report_id: str | None


@dataclass(frozen=True)
class FinalizedReport:
    """Stable report and delivery identities produced by finalization."""

    report_row_id: int
    report_id: str
    outbox_id: int
    duplicate: bool


@dataclass(frozen=True)
class StoredReport:
    """Immutable report material sufficient to reconstruct its archive."""

    report_row_id: int
    report_id: str
    claim_id: int
    session_date: date
    report_kind: str
    body: str
    archive_relative_path: str
    content_sha256: str
    state_sha256: str
    observation_set_sha256: str
    observation_ids: tuple[int, ...]
    observation_sha256s: tuple[str, ...]
    created_at: datetime
    finalized_at: datetime


@dataclass(frozen=True)
class PendingOutbox:
    """One immutable payload that has no recorded delivered attempt."""

    outbox_id: int
    idempotency_key: str
    origin_report_id: int | None
    origin_execution_event_id: int | None
    destination: str
    payload_text: str
    payload_sha256: str
    created_at: datetime
    next_attempt_ordinal: int


@dataclass(frozen=True, slots=True)
class StoredIngestionAction:
    """Typed readback for one completely persisted confirmation action."""

    ordinal: int
    event_row_id: int
    event_id: str
    storage_action: str
    domain_kind: str
    status: str
    reason_codes: tuple[str, ...]
    outbox_id: int
    outbox_destination: str


@dataclass(frozen=True, slots=True)
class StoredIngestionResult:
    """Typed, content-digested readback for one source message."""

    raw_row_id: int
    message_id: str
    message_time: datetime
    received_at: datetime
    raw_text: str
    raw_sha256: str
    actions: tuple[StoredIngestionAction, ...]
    state_digest: str


@dataclass(frozen=True, slots=True)
class JournalRowReference:
    """Digest of every stored column in one exact Journal row."""

    table: str
    row_id: int
    row_digest: str


@dataclass(frozen=True, slots=True, weakref_slot=True)
class JournalAccountCheckSource:
    """Exact immutable account-check row joined to its execution source."""

    row_id: int
    check_id: str
    raw_message_id: int
    execution_event_id: int
    settled_cash_micros: int
    pending_order_count: int
    unlogged_position_count: int
    confirmed_at: datetime
    reconciliation_result: str
    details_json: str
    row_reference: JournalRowReference


@dataclass(frozen=True, slots=True, weakref_slot=True)
class JournalActionSource:
    """Source-authenticated action reconstructed from raw and event rows."""

    execution_event_id: int
    event_id: str
    raw_message_id: int
    message_id: str
    action_ordinal: int
    idempotency_key: str
    storage_action: str
    domain_kind: str
    signal_id: str | None
    symbol: str | None
    shares: int | None
    price_micros: int | None
    bid_micros: int | None
    ask_micros: int | None
    recommended_stop_micros: int | None
    user_confirmed_stop_micros: int | None
    event_time: datetime
    message_time: datetime
    received_at: datetime
    compliance_result: str
    reconciliation_state: str
    raw_text: str
    raw_sha256: str
    details_json: str
    details_sha256: str
    parent_order_id: str | None
    fill_group_planned_shares: int | None
    event_role: str
    account_check: JournalAccountCheckSource | None
    acknowledgement_outbox_id: int
    acknowledgement_destination: str
    acknowledgement_idempotency_key: str
    acknowledgement_payload_sha256: str
    row_references: tuple[JournalRowReference, ...]
    source_digest: str


@dataclass(frozen=True, slots=True, weakref_slot=True)
class JournalAccountCheckWindowSource:
    """Complete cursor interval from one check through one terminal buy."""

    account_check_action: JournalActionSource
    terminal_action: JournalActionSource
    between_actions: tuple[JournalActionSource, ...]
    after_cursor: int
    through_cursor: int
    source_high_water_cursor: int
    expected_between_count: int
    row_references: tuple[JournalRowReference, ...]
    source_digest: str


@dataclass(frozen=True, slots=True)
class JournalPostingSource:
    row_id: int
    posting_key: str
    ledger_name: str
    account_name: str
    entry_kind: str
    execution_event_id: int | None
    account_check_id: int | None
    symbol: str | None
    amount_micros: int
    shares_delta: int | None
    unit_price_micros: int | None
    occurred_at: datetime
    details_json: str
    row_reference: JournalRowReference


@dataclass(frozen=True, slots=True)
class JournalProjectionRowSource:
    table: str
    row_id: int
    values: tuple[tuple[str, object], ...]
    row_reference: JournalRowReference


@dataclass(frozen=True, slots=True, weakref_slot=True)
class JournalActualReplaySource:
    """Complete bitemporal Journal snapshot through one execution cursor."""

    query_cutoff: datetime
    actions: tuple[JournalActionSource, ...]
    postings: tuple[JournalPostingSource, ...]
    projection_rows: tuple[JournalProjectionRowSource, ...]
    start_cursor: int | None
    terminal_cursor: int | None
    through_execution_cursor: int | None
    source_through_cursor: int | None
    projection_through_cursor: int | None
    projection_stale: bool
    expected_action_count: int
    expected_posting_count: int
    row_references: tuple[JournalRowReference, ...]
    source_digest: str


_JOURNAL_SOURCE_LOCK = threading.RLock()
_ACTION_SOURCE_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
        int,
        int,
    ],
] = {}
_WINDOW_SOURCE_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
        int,
        int,
    ],
] = {}
_REPLAY_SOURCE_AUTHORITIES: dict[
    int,
    tuple[
        ReferenceType[object],
        tuple[object, ...],
        ReferenceType[object],
        int,
        int,
    ],
] = {}


def _row_reference_fingerprint(
    reference: JournalRowReference,
) -> tuple[object, ...]:
    return (reference.table, reference.row_id, reference.row_digest)


def _account_check_source_fingerprint(
    source: JournalAccountCheckSource,
) -> tuple[object, ...]:
    return (
        source.row_id,
        source.check_id,
        source.raw_message_id,
        source.execution_event_id,
        source.settled_cash_micros,
        source.pending_order_count,
        source.unlogged_position_count,
        source.confirmed_at,
        source.reconciliation_result,
        source.details_json,
        _row_reference_fingerprint(source.row_reference),
    )


def _action_source_fingerprint(source: JournalActionSource) -> tuple[object, ...]:
    return (
        source.execution_event_id,
        source.event_id,
        source.raw_message_id,
        source.message_id,
        source.action_ordinal,
        source.idempotency_key,
        source.storage_action,
        source.domain_kind,
        source.signal_id,
        source.symbol,
        source.shares,
        source.price_micros,
        source.bid_micros,
        source.ask_micros,
        source.recommended_stop_micros,
        source.user_confirmed_stop_micros,
        source.event_time,
        source.message_time,
        source.received_at,
        source.compliance_result,
        source.reconciliation_state,
        source.raw_text,
        source.raw_sha256,
        source.details_json,
        source.details_sha256,
        source.parent_order_id,
        source.fill_group_planned_shares,
        source.event_role,
        (
            None
            if source.account_check is None
            else _account_check_source_fingerprint(source.account_check)
        ),
        source.acknowledgement_outbox_id,
        source.acknowledgement_destination,
        source.acknowledgement_idempotency_key,
        source.acknowledgement_payload_sha256,
        tuple(
            _row_reference_fingerprint(reference)
            for reference in source.row_references
        ),
        source.source_digest,
    )


def _window_source_fingerprint(
    source: JournalAccountCheckWindowSource,
) -> tuple[object, ...]:
    return (
        _action_source_fingerprint(source.account_check_action),
        _action_source_fingerprint(source.terminal_action),
        tuple(_action_source_fingerprint(action) for action in source.between_actions),
        source.after_cursor,
        source.through_cursor,
        source.source_high_water_cursor,
        source.expected_between_count,
        tuple(
            _row_reference_fingerprint(reference)
            for reference in source.row_references
        ),
        source.source_digest,
    )


def _posting_source_fingerprint(
    source: JournalPostingSource,
) -> tuple[object, ...]:
    return (
        source.row_id,
        source.posting_key,
        source.ledger_name,
        source.account_name,
        source.entry_kind,
        source.execution_event_id,
        source.account_check_id,
        source.symbol,
        source.amount_micros,
        source.shares_delta,
        source.unit_price_micros,
        source.occurred_at,
        source.details_json,
        _row_reference_fingerprint(source.row_reference),
    )


def _projection_row_source_fingerprint(
    source: JournalProjectionRowSource,
) -> tuple[object, ...]:
    return (
        source.table,
        source.row_id,
        tuple(tuple(value) for value in source.values),
        _row_reference_fingerprint(source.row_reference),
    )


def _replay_source_fingerprint(
    source: JournalActualReplaySource,
) -> tuple[object, ...]:
    return (
        source.query_cutoff,
        tuple(_action_source_fingerprint(action) for action in source.actions),
        tuple(_posting_source_fingerprint(posting) for posting in source.postings),
        tuple(
            _projection_row_source_fingerprint(row)
            for row in source.projection_rows
        ),
        source.start_cursor,
        source.terminal_cursor,
        source.through_execution_cursor,
        source.source_through_cursor,
        source.projection_through_cursor,
        source.projection_stale,
        source.expected_action_count,
        source.expected_posting_count,
        tuple(
            _row_reference_fingerprint(reference)
            for reference in source.row_references
        ),
        source.source_digest,
    )


def _has_current_journal_source_authority(
    registry: dict[
        int,
        tuple[
            ReferenceType[object],
            tuple[object, ...],
            ReferenceType[object],
            int,
            int,
        ],
    ],
    source: object,
    fingerprint: tuple[object, ...],
) -> bool:
    with _JOURNAL_SOURCE_LOCK:
        issued = registry.get(id(source))
        if (
            issued is None
            or issued[0]() is not source
            or issued[1] != fingerprint
        ):
            return False
        owner = issued[2]()
        issued_generation = issued[3]
        issued_data_version = issued[4]
    if owner is None or getattr(owner, "_closed", True):
        return False
    try:
        if (
            getattr(owner, "_transaction_active")
            and getattr(owner, "_transaction_dirty")
        ):
            return False
        current_generation = getattr(owner, "_source_generation")
        current_data_version = owner._source_authority_data_version()
    except Exception:
        return False
    return (
        current_generation == issued_generation
        and current_data_version == issued_data_version
    )


def is_verified_journal_action_source(source: object) -> bool:
    """Return whether Journal issued this exact reconstructed action object."""
    if not isinstance(source, JournalActionSource):
        return False
    try:
        fingerprint = _action_source_fingerprint(source)
    except Exception:
        return False
    return _has_current_journal_source_authority(
        _ACTION_SOURCE_AUTHORITIES,
        source,
        fingerprint,
    )


def is_verified_journal_window_source(source: object) -> bool:
    """Return whether Journal issued this exact complete cursor window."""
    if not isinstance(source, JournalAccountCheckWindowSource):
        return False
    try:
        fingerprint = _window_source_fingerprint(source)
    except Exception:
        return False
    return _has_current_journal_source_authority(
        _WINDOW_SOURCE_AUTHORITIES,
        source,
        fingerprint,
    )


def is_verified_journal_replay_source(source: object) -> bool:
    """Return whether Journal issued this exact replay snapshot."""
    if not isinstance(source, JournalActualReplaySource):
        return False
    try:
        fingerprint = _replay_source_fingerprint(source)
    except Exception:
        return False
    return _has_current_journal_source_authority(
        _REPLAY_SOURCE_AUTHORITIES,
        source,
        fingerprint,
    )


@dataclass(frozen=True)
class _Migration:
    version: int
    name: str
    sql: bytes
    sha256: str


_AppliedMigration = tuple[int, str, str, str, str]


class JournalTransaction:
    """Write operations scoped to one immediate SQLite transaction."""

    def __init__(self, journal: Journal) -> None:
        self._journal = journal
        self._active = True
        self._dirty = False
        self._source_read = False
        self._new_execution_event_ids: set[int] = set()
        self._validated_confirmation_source: tuple[datetime, datetime] | None = None

    def _ensure_active(self) -> None:
        if not self._active or not self._journal._transaction_active:
            raise JournalError("journal transaction is no longer active")

    def _deactivate(self) -> None:
        self._active = False

    def _mark_dirty(self) -> None:
        if self._source_read:
            raise JournalError(
                "source authority transaction is sealed read-only"
            )
        self._dirty = True
        self._journal._transaction_dirty = True

    def _ensure_post_commit_source_read(self) -> None:
        self._ensure_active()
        if self._dirty:
            raise JournalError(
                "source authority requires a post-commit read transaction"
            )
        self._source_read = True

    def append_raw_message(
        self, message_id: str, message_time: datetime, text: str
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._append_raw_message(message_id, message_time, text)

    def append_execution_event(
        self,
        *,
        raw_message_id: int,
        action_ordinal: int,
        parsed_action: str,
        event_time: datetime,
        signal_id: str | None = None,
        symbol: str | None = None,
        shares: int | None = None,
        price_micros: int | None = None,
        bid_micros: int | None = None,
        ask_micros: int | None = None,
        recommended_stop_micros: int | None = None,
        user_confirmed_stop_micros: int | None = None,
        compliance_result: str = "UNASSESSED",
        reconciliation_state: str = "PENDING",
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        row_id, duplicate = self._journal._append_execution_event(
            raw_message_id=raw_message_id,
            action_ordinal=action_ordinal,
            parsed_action=parsed_action,
            event_time=event_time,
            signal_id=signal_id,
            symbol=symbol,
            shares=shares,
            price_micros=price_micros,
            bid_micros=bid_micros,
            ask_micros=ask_micros,
            recommended_stop_micros=recommended_stop_micros,
            user_confirmed_stop_micros=user_confirmed_stop_micros,
            compliance_result=compliance_result,
            reconciliation_state=reconciliation_state,
            details=details,
            prevalidated_confirmation_source=(
                self._validated_confirmation_source
            ),
        )
        if not duplicate:
            self._new_execution_event_ids.add(row_id)
        return row_id, duplicate

    def read_confirmation_result(
        self,
        *,
        message_id: str,
    ) -> StoredIngestionResult | None:
        self._ensure_active()
        return self._journal._read_confirmation_result(message_id=message_id)

    def _incremental_ingestion_identity(self) -> tuple[int, int, int | None]:
        """Return a clean in-transaction identity for a private replay cache."""
        self._ensure_active()
        if self._dirty:
            raise JournalError(
                "incremental ingestion identity requires a clean transaction"
            )
        return self._journal._incremental_ingestion_identity()

    def validate_confirmation_source_order(
        self,
        *,
        message_time: datetime,
        received_at: datetime,
    ) -> None:
        """Reject a new source cursor that regresses knowledge chronology."""
        self._ensure_active()
        self._journal._validate_confirmation_source_order(
            message_time=message_time,
            received_at=received_at,
        )
        self._validated_confirmation_source = (
            _parse_canonical_timestamp(_canonical_timestamp(message_time)),
            _parse_canonical_timestamp(_canonical_timestamp(received_at)),
        )

    def read_action_source(
        self,
        *,
        execution_event_id: int,
    ) -> JournalActionSource:
        self._ensure_post_commit_source_read()
        source = self._journal._read_action_source(
            execution_event_id=execution_event_id,
        )
        owner = ref(self._journal)
        generation = self._journal._source_generation
        data_version = self._journal._source_authority_data_version()
        identity = id(source)

        def discard(dead: ReferenceType[object]) -> None:
            with _JOURNAL_SOURCE_LOCK:
                current = _ACTION_SOURCE_AUTHORITIES.get(identity)
                if current is not None and current[0] is dead:
                    _ACTION_SOURCE_AUTHORITIES.pop(identity, None)

        with _JOURNAL_SOURCE_LOCK:
            _ACTION_SOURCE_AUTHORITIES[identity] = (
                ref(source, discard),
                _action_source_fingerprint(source),
                owner,
                generation,
                data_version,
            )
        return source

    def read_account_check_window(
        self,
        *,
        account_check_event_id: int,
        terminal_event_id: int,
    ) -> JournalAccountCheckWindowSource:
        self._ensure_post_commit_source_read()
        source = self._journal._read_account_check_window(
            account_check_event_id=account_check_event_id,
            terminal_event_id=terminal_event_id,
        )
        owner = ref(self._journal)
        generation = self._journal._source_generation
        data_version = self._journal._source_authority_data_version()
        for action in (
            source.account_check_action,
            *source.between_actions,
            source.terminal_action,
        ):
            identity = id(action)

            def discard_action(
                dead: ReferenceType[object],
                *,
                identity: int = identity,
            ) -> None:
                with _JOURNAL_SOURCE_LOCK:
                    current = _ACTION_SOURCE_AUTHORITIES.get(identity)
                    if current is not None and current[0] is dead:
                        _ACTION_SOURCE_AUTHORITIES.pop(identity, None)

            with _JOURNAL_SOURCE_LOCK:
                _ACTION_SOURCE_AUTHORITIES[identity] = (
                    ref(action, discard_action),
                    _action_source_fingerprint(action),
                    owner,
                    generation,
                    data_version,
                )
        identity = id(source)

        def discard_window(dead: ReferenceType[object]) -> None:
            with _JOURNAL_SOURCE_LOCK:
                current = _WINDOW_SOURCE_AUTHORITIES.get(identity)
                if current is not None and current[0] is dead:
                    _WINDOW_SOURCE_AUTHORITIES.pop(identity, None)

        with _JOURNAL_SOURCE_LOCK:
            _WINDOW_SOURCE_AUTHORITIES[identity] = (
                ref(source, discard_window),
                _window_source_fingerprint(source),
                owner,
                generation,
                data_version,
            )
        return source

    def read_actual_replay(
        self,
        *,
        query_cutoff: datetime,
        through_execution_cursor: int | None = None,
    ) -> JournalActualReplaySource:
        self._ensure_post_commit_source_read()
        source = self._journal._read_actual_replay(
            query_cutoff=query_cutoff,
            through_execution_cursor=through_execution_cursor,
        )
        owner = ref(self._journal)
        generation = self._journal._source_generation
        data_version = self._journal._source_authority_data_version()
        for action in source.actions:
            identity = id(action)

            def discard_action(
                dead: ReferenceType[object],
                *,
                identity: int = identity,
            ) -> None:
                with _JOURNAL_SOURCE_LOCK:
                    current = _ACTION_SOURCE_AUTHORITIES.get(identity)
                    if current is not None and current[0] is dead:
                        _ACTION_SOURCE_AUTHORITIES.pop(identity, None)

            with _JOURNAL_SOURCE_LOCK:
                _ACTION_SOURCE_AUTHORITIES[identity] = (
                    ref(action, discard_action),
                    _action_source_fingerprint(action),
                    owner,
                    generation,
                    data_version,
                )
        identity = id(source)

        def discard_replay(dead: ReferenceType[object]) -> None:
            with _JOURNAL_SOURCE_LOCK:
                current = _REPLAY_SOURCE_AUTHORITIES.get(identity)
                if current is not None and current[0] is dead:
                    _REPLAY_SOURCE_AUTHORITIES.pop(identity, None)

        with _JOURNAL_SOURCE_LOCK:
            _REPLAY_SOURCE_AUTHORITIES[identity] = (
                ref(source, discard_replay),
                _replay_source_fingerprint(source),
                owner,
                generation,
                data_version,
            )
        return source

    def append_source_observation(
        self,
        *,
        payload: bytes,
        source_uri: str,
        source_type: str,
        provider: str,
        feed: str | None,
        source_time: datetime,
        retrieved_at: datetime,
        provider_sequence: int | None,
        delay_seconds: int | None,
        health_result: str,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._append_source_observation(
            payload=payload,
            source_uri=source_uri,
            source_type=source_type,
            provider=provider,
            feed=feed,
            source_time=source_time,
            retrieved_at=retrieved_at,
            provider_sequence=provider_sequence,
            delay_seconds=delay_seconds,
            health_result=health_result,
            details=details,
        )

    def finalize_report(
        self,
        *,
        claim_id: int,
        claim_token: str,
        body: str,
        state_sha256: str,
        observation_ids: Sequence[int],
        archive_relative_path: str,
        created_at: datetime,
        outbox_destination: str,
        outbox_payload: str,
    ) -> FinalizedReport:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._finalize_report(
            claim_id=claim_id,
            claim_token=claim_token,
            body=body,
            state_sha256=state_sha256,
            observation_ids=observation_ids,
            archive_relative_path=archive_relative_path,
            created_at=created_at,
            outbox_destination=outbox_destination,
            outbox_payload=outbox_payload,
        )

    def append_outbox(
        self,
        *,
        idempotency_key: str,
        origin_report_id: int | None,
        origin_execution_event_id: int | None,
        destination: str,
        payload_text: str,
        created_at: datetime,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._append_outbox(
            idempotency_key=idempotency_key,
            origin_report_id=origin_report_id,
            origin_execution_event_id=origin_execution_event_id,
            destination=destination,
            payload_text=payload_text,
            created_at=created_at,
        )

    def record_outbox_delivery_attempt(
        self,
        *,
        outbox_id: int,
        attempt_ordinal: int,
        attempted_at: datetime,
        delivery_status: str,
        external_delivery_id: str | None = None,
        error_class: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._record_outbox_delivery_attempt(
            outbox_id=outbox_id,
            attempt_ordinal=attempt_ordinal,
            attempted_at=attempted_at,
            delivery_status=delivery_status,
            external_delivery_id=external_delivery_id,
            error_class=error_class,
            details=details,
        )

    def start_scheduled_run(
        self,
        *,
        run_key: str,
        run_kind: str,
        session_date: date,
        intended_run_at: datetime,
        started_at: datetime,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._start_scheduled_run(
            run_key=run_key,
            run_kind=run_kind,
            session_date=session_date,
            intended_run_at=intended_run_at,
            started_at=started_at,
        )

    def complete_scheduled_run(
        self,
        *,
        run_id: int,
        finished_at: datetime,
        market_session_decision: str,
        outcome: str,
        report_id: int | None = None,
        report_path: str | None = None,
        error_class: str | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._complete_scheduled_run(
            run_id=run_id,
            finished_at=finished_at,
            market_session_decision=market_session_decision,
            outcome=outcome,
            report_id=report_id,
            report_path=report_path,
            error_class=error_class,
        )

    def append_ledger_posting(
        self,
        *,
        posting_key: str,
        ledger_name: str,
        account_name: str,
        entry_kind: str,
        occurred_at: datetime,
        amount_micros: int,
        execution_event_id: int | None = None,
        account_check_id: int | None = None,
        symbol: str | None = None,
        shares_delta: int | None = None,
        unit_price_micros: int | None = None,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        origin_execution_event_id = execution_event_id
        if origin_execution_event_id is None and account_check_id is not None:
            account_origin = _sql(
                self._journal._connection,
                "SELECT execution_event_id FROM account_checks WHERE id = ?",
                (account_check_id,),
            ).fetchone()
            if account_origin is not None:
                origin_execution_event_id = int(account_origin[0])
        if origin_execution_event_id is not None:
            row = _sql(
                self._journal._connection,
                "SELECT details_json FROM execution_events WHERE id = ?",
                (origin_execution_event_id,),
            ).fetchone()
            if row is not None:
                event_details = _canonical_stored_details(
                    str(row[0]),
                    label="execution event",
                )
                source_details = event_details.get("source")
                if (
                    isinstance(source_details, dict)
                    and source_details.get("type")
                    == "ROBINHOOD_MANUAL_CONFIRMATION"
                    and origin_execution_event_id
                    not in self._new_execution_event_ids
                ):
                    raise InvalidJournalValue(
                        "confirmation posting must commit atomically with its event"
                    )
        self._mark_dirty()
        return self._journal._append_ledger_posting(
            posting_key=posting_key,
            ledger_name=ledger_name,
            account_name=account_name,
            entry_kind=entry_kind,
            occurred_at=occurred_at,
            amount_micros=amount_micros,
            execution_event_id=execution_event_id,
            account_check_id=account_check_id,
            symbol=symbol,
            shares_delta=shares_delta,
            unit_price_micros=unit_price_micros,
            details=details,
        )

    def write_actual_position(
        self,
        *,
        signal_id: str,
        symbol: str,
        shares: int,
        cost_basis_micros: int,
        recommended_stop_micros: int | None,
        user_confirmed_stop_micros: int | None,
        target_micros: int | None,
        last_execution_event_id: int,
        updated_at: datetime,
    ) -> int:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._write_actual_position(
            signal_id=signal_id,
            symbol=symbol,
            shares=shares,
            cost_basis_micros=cost_basis_micros,
            recommended_stop_micros=recommended_stop_micros,
            user_confirmed_stop_micros=user_confirmed_stop_micros,
            target_micros=target_micros,
            last_execution_event_id=last_execution_event_id,
            updated_at=updated_at,
        )

    def write_actual_cash_projection(
        self,
        *,
        estimated_settled_cash_micros: int,
        user_confirmed_settled_cash_micros: int | None,
        deployed_capital_micros: int,
        open_planned_risk_micros: int,
        consecutive_losses: int,
        weekly_high_water_micros: int,
        monthly_high_water_micros: int,
        last_ledger_posting_id: int,
        updated_at: datetime,
    ) -> int:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._write_actual_cash_projection(
            estimated_settled_cash_micros=estimated_settled_cash_micros,
            user_confirmed_settled_cash_micros=user_confirmed_settled_cash_micros,
            deployed_capital_micros=deployed_capital_micros,
            open_planned_risk_micros=open_planned_risk_micros,
            consecutive_losses=consecutive_losses,
            weekly_high_water_micros=weekly_high_water_micros,
            monthly_high_water_micros=monthly_high_water_micros,
            last_ledger_posting_id=last_ledger_posting_id,
            updated_at=updated_at,
        )

    def write_reconciliation_projection(
        self,
        *,
        reconciliation_required: bool,
        reason: str | None,
        last_execution_event_id: int | None,
        updated_at: datetime,
    ) -> int:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._write_reconciliation_projection(
            reconciliation_required=reconciliation_required,
            reason=reason,
            last_execution_event_id=last_execution_event_id,
            updated_at=updated_at,
        )

    def append_account_check(
        self,
        *,
        execution_event_id: int,
        settled_cash_micros: int,
        pending_order_count: int,
        unlogged_position_count: int,
        confirmed_at: datetime,
        reconciliation_result: str,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        self._ensure_active()
        self._mark_dirty()
        return self._journal._append_account_check(
            execution_event_id=execution_event_id,
            settled_cash_micros=settled_cash_micros,
            pending_order_count=pending_order_count,
            unlogged_position_count=unlogged_position_count,
            confirmed_at=confirmed_at,
            reconciliation_result=reconciliation_result,
            details=details,
        )


class Journal:
    """One configured SQLite connection and its durable audit API."""

    def __init__(
        self,
        path: Path,
        connection: sqlite3.Connection,
        migration_directory: Path | None = None,
    ) -> None:
        self.path = path
        self._connection = connection
        self._migration_directory = migration_directory
        self._transaction_active = False
        self._transaction_dirty = False
        self._projection_write_allowed = False
        self._report_claim_write_allowed = False
        self._source_generation = 0
        self._closed = False

    @classmethod
    def open(
        cls, path: Path, *, migration_directory: Path | None = None
    ) -> Self:
        if not isinstance(path, Path):
            raise InvalidJournalValue("journal path must be a pathlib.Path")
        if migration_directory is not None:
            if not isinstance(migration_directory, Path):
                raise InvalidJournalValue(
                    "migration directory must be a pathlib.Path"
                )
            if not migration_directory.is_dir():
                raise MigrationCorruption("migration directory is unavailable")
        parent = path.parent
        try:
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as error:
            raise JournalError("journal state directory could not be created") from error
        if not parent.is_dir():
            raise JournalError("journal state parent is not a directory")

        try:
            connection = sqlite3.connect(
                path,
                timeout=BUSY_TIMEOUT_MILLISECONDS / 1_000,
                isolation_level=None,
            )
        except sqlite3.Error as error:
            raise JournalError("journal database could not be opened") from error

        journal = cls(path, connection, migration_directory)
        try:
            journal._verify_database_ownership_snapshot(connection)
            journal._configure_connection()
            journal.migrate()
        except BaseException:
            connection.close()
            journal._closed = True
            raise
        return journal

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        if self._transaction_active:
            raise JournalError("journal cannot close during an active transaction")
        self._connection.close()
        self._closed = True

    def migrate(self) -> None:
        self._ensure_open()
        migrations = _load_migrations(self._migration_directory)
        expected_schema_sha256s = _derive_expected_schema_sha256s(migrations)
        with self._immediate_connection() as connection:
            self._verify_database_ownership(connection)
            applied = self._read_applied_migrations(connection)
            _verify_applied_migrations(applied, migrations)
            current_version = _pragma_int(connection, "user_version")
            if current_version != len(applied):
                raise MigrationCorruption("database migration version is inconsistent")
            for index, row in enumerate(applied):
                if row[3] != expected_schema_sha256s[index]:
                    raise MigrationDrift(
                        "applied schema differs from packaged migrations"
                    )
            if applied and expected_schema_sha256s[len(applied) - 1] != _schema_sha256(
                connection
            ):
                raise MigrationDrift("database schema differs from packaged migrations")

            recorded = list(applied)
            for migration in migrations[len(applied) :]:
                _require_migration_bookkeeping(connection, tuple(recorded))
                _execute_migration(connection, migration.sql)
                _require_migration_bookkeeping(connection, tuple(recorded))
                _verify_migration_connection_state(connection)
                schema_sha256 = _schema_sha256(connection)
                expected_schema_sha256 = expected_schema_sha256s[
                    migration.version - 1
                ]
                if schema_sha256 != expected_schema_sha256:
                    raise MigrationDrift(
                        "database schema differs from packaged migrations"
                    )
                try:
                    applied_at = _canonical_timestamp(datetime.now(timezone.utc))
                    _sql(connection,
                        "INSERT INTO schema_migrations("
                        "version, name, sha256, schema_sha256, applied_at"
                        ") VALUES (?, ?, ?, ?, ?)",
                        (
                            migration.version,
                            migration.name,
                            migration.sha256,
                            expected_schema_sha256,
                            applied_at,
                        ),
                    )
                    _sql(connection, f"PRAGMA user_version = {migration.version}")
                except sqlite3.Error as error:
                    if _is_busy_error(error):
                        raise JournalBusy("journal is busy") from error
                    raise MigrationCorruption(
                        "migration metadata could not be recorded"
                    ) from error
                recorded.append(
                    (
                        migration.version,
                        migration.name,
                        migration.sha256,
                        expected_schema_sha256,
                        applied_at,
                    )
                )
                _require_migration_bookkeeping(connection, tuple(recorded))

            _require_migration_bookkeeping(connection, tuple(recorded))
            _verify_migration_connection_state(connection)

            if _pragma_int(connection, "application_id") == 0:
                _sql(connection, f"PRAGMA application_id = {APPLICATION_ID}")
            if _pragma_int(connection, "application_id") != APPLICATION_ID:
                raise MigrationCorruption("database application identifier is inconsistent")
            if _sql(connection, "PRAGMA foreign_key_check").fetchone() is not None:
                raise MigrationCorruption("database foreign keys are inconsistent")
            quick_check = _sql(connection, "PRAGMA quick_check").fetchone()
            if quick_check is None or quick_check[0] != "ok":
                raise MigrationCorruption("database integrity check failed")

    @contextmanager
    def transaction(self) -> Iterator[JournalTransaction]:
        self._ensure_open()
        with self._immediate_connection():
            transaction = JournalTransaction(self)
            try:
                yield transaction
            finally:
                transaction._deactivate()

    def append_raw_message(
        self, message_id: str, message_time: datetime, text: str
    ) -> tuple[int, bool]:
        with self.transaction() as transaction:
            return transaction.append_raw_message(message_id, message_time, text)

    def append_execution_event(
        self,
        *,
        raw_message_id: int,
        action_ordinal: int,
        parsed_action: str,
        event_time: datetime,
        signal_id: str | None = None,
        symbol: str | None = None,
        shares: int | None = None,
        price_micros: int | None = None,
        bid_micros: int | None = None,
        ask_micros: int | None = None,
        recommended_stop_micros: int | None = None,
        user_confirmed_stop_micros: int | None = None,
        compliance_result: str = "UNASSESSED",
        reconciliation_state: str = "PENDING",
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        """Append one parsed action outside a larger caller-owned transaction."""
        with self.transaction() as transaction:
            return transaction.append_execution_event(
                raw_message_id=raw_message_id,
                action_ordinal=action_ordinal,
                parsed_action=parsed_action,
                event_time=event_time,
                signal_id=signal_id,
                symbol=symbol,
                shares=shares,
                price_micros=price_micros,
                bid_micros=bid_micros,
                ask_micros=ask_micros,
                recommended_stop_micros=recommended_stop_micros,
                user_confirmed_stop_micros=user_confirmed_stop_micros,
                compliance_result=compliance_result,
                reconciliation_state=reconciliation_state,
                details=details,
            )

    def append_source_observation(
        self,
        *,
        payload: bytes,
        source_uri: str,
        source_type: str,
        provider: str,
        feed: str | None,
        source_time: datetime,
        retrieved_at: datetime,
        provider_sequence: int | None,
        delay_seconds: int | None,
        health_result: str,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        """Append one content-addressed source observation."""
        with self.transaction() as transaction:
            return transaction.append_source_observation(
                payload=payload,
                source_uri=source_uri,
                source_type=source_type,
                provider=provider,
                feed=feed,
                source_time=source_time,
                retrieved_at=retrieved_at,
                provider_sequence=provider_sequence,
                delay_seconds=delay_seconds,
                health_result=health_result,
                details=details,
            )

    def claim_report(
        self,
        session_date: date,
        kind: str,
    ) -> ReportClaim:
        """Acquire, observe, or explicitly recover a report publication claim."""
        stored_date = _canonical_date(session_date)
        report_kind = _require_canonical_report_kind(kind)

        with self._immediate_connection() as connection:
            stored_now = _canonical_timestamp(_utc_now())
            row = _sql(connection,
                "SELECT claim.id, claim.claim_token, claim.status, "
                "claim.lease_expires_at, claim.report_id, report.report_id "
                "FROM report_claims AS claim LEFT JOIN reports AS report "
                "ON report.id = claim.report_id "
                "WHERE claim.session_date = ? AND claim.report_kind = ?",
                (stored_date, report_kind),
            ).fetchone()
            if row is None:
                acquired_at = stored_now
                expires_at = _add_seconds(
                    acquired_at, _REPORT_CLAIM_LEASE_SECONDS
                )
                token = secrets.token_urlsafe(32)
                with self._report_claim_write():
                    cursor = _sql(connection,
                        "INSERT INTO report_claims("
                        "session_date, report_kind, claim_token, status, created_at, "
                        "lease_started_at, lease_expires_at, finalized_at, report_id"
                        ") VALUES (?, ?, ?, 'IN_PROGRESS', ?, ?, ?, NULL, NULL)",
                        (
                            stored_date,
                            report_kind,
                            token,
                            acquired_at,
                            acquired_at,
                            expires_at,
                        ),
                    )
                return ReportClaim(
                    claim_id=int(cursor.lastrowid),
                    session_date=session_date,
                    report_kind=report_kind,
                    status="ACQUIRED",
                    claim_token=token,
                    lease_expires_at=_parse_canonical_timestamp(expires_at),
                    report_row_id=None,
                    report_id=None,
                )

            claim_id = int(row[0])
            stored_status = str(row[2])
            expires_at = str(row[3])
            report_row_id = int(row[4]) if row[4] is not None else None
            report_id = str(row[5]) if row[5] is not None else None
            if stored_status == "FINALIZED":
                if report_row_id is None or report_id is None:
                    raise MigrationCorruption(
                        "finalized report claim lacks its report identity"
                    )
                return ReportClaim(
                    claim_id=claim_id,
                    session_date=session_date,
                    report_kind=report_kind,
                    status="ALREADY_FINALIZED",
                    claim_token=None,
                    lease_expires_at=_parse_canonical_timestamp(expires_at),
                    report_row_id=report_row_id,
                    report_id=report_id,
                )
            if stored_status != "IN_PROGRESS":
                raise MigrationCorruption("report claim has an invalid stored status")
            if stored_now >= expires_at:
                recovered_expires_at = _add_seconds(
                    stored_now, _REPORT_CLAIM_LEASE_SECONDS
                )
                token = secrets.token_urlsafe(32)
                with self._report_claim_write():
                    cursor = _sql(connection,
                        "UPDATE report_claims SET claim_token = ?, lease_started_at = ?, "
                        "lease_expires_at = ? WHERE id = ? AND status = 'IN_PROGRESS' "
                        "AND lease_expires_at <= ?",
                        (
                            token,
                            stored_now,
                            recovered_expires_at,
                            claim_id,
                            stored_now,
                        ),
                    )
                if cursor.rowcount != 1:
                    raise JournalBusy("report claim changed during recovery")
                return ReportClaim(
                    claim_id=claim_id,
                    session_date=session_date,
                    report_kind=report_kind,
                    status="RECOVERED_EXPIRED",
                    claim_token=token,
                    lease_expires_at=_parse_canonical_timestamp(
                        recovered_expires_at
                    ),
                    report_row_id=None,
                    report_id=None,
                )
            return ReportClaim(
                claim_id=claim_id,
                session_date=session_date,
                report_kind=report_kind,
                status="IN_PROGRESS",
                claim_token=None,
                lease_expires_at=_parse_canonical_timestamp(expires_at),
                report_row_id=None,
                report_id=None,
            )

    def finalize_report(
        self,
        *,
        claim_id: int,
        claim_token: str,
        body: str,
        state_sha256: str,
        observation_ids: Sequence[int],
        archive_relative_path: str,
        created_at: datetime,
        outbox_destination: str,
        outbox_payload: str,
    ) -> FinalizedReport:
        """Finalize a report, its evidence pins, and its outbox row atomically."""
        with self.transaction() as transaction:
            return transaction.finalize_report(
                claim_id=claim_id,
                claim_token=claim_token,
                body=body,
                state_sha256=state_sha256,
                observation_ids=observation_ids,
                archive_relative_path=archive_relative_path,
                created_at=created_at,
                outbox_destination=outbox_destination,
                outbox_payload=outbox_payload,
            )

    def read_report(self, report_id: str) -> StoredReport:
        """Read immutable report material for crash-safe archive reconstruction."""
        self._ensure_open()
        report_id = _require_sha256(report_id, "report ID")
        row = _sql(
            self._connection,
            "SELECT report.id, report.report_id, report.claim_id, "
            "report.session_date, report.report_kind, report.body_text, "
            "report.content_sha256, report.state_sha256, "
            "report.observation_set_sha256, report.archive_relative_path, "
            "report.created_at, claim.finalized_at, claim.status, claim.report_id "
            "FROM reports AS report JOIN report_claims AS claim "
            "ON claim.id = report.claim_id WHERE report.report_id = ? COLLATE BINARY",
            (report_id,),
        ).fetchone()
        if row is None:
            raise InvalidJournalValue("report does not exist")
        if str(row[12]) != "FINALIZED" or int(row[13]) != int(row[0]):
            raise MigrationCorruption("report is not linked to a finalized claim")
        pins = _sql(
            self._connection,
            "SELECT pin.source_observation_id, observation.observation_sha256 "
            "FROM report_observations AS pin JOIN source_observations AS observation "
            "ON observation.id = pin.source_observation_id "
            "WHERE pin.report_id = ? ORDER BY pin.observation_ordinal",
            (int(row[0]),),
        ).fetchall()
        observation_ids = tuple(int(pin[0]) for pin in pins)
        observation_sha256s = tuple(str(pin[1]) for pin in pins)
        session_date = date.fromisoformat(str(row[3]))
        expected_content_sha256 = hashlib.sha256(str(row[5]).encode("utf-8")).hexdigest()
        expected_observation_set_sha256 = hashlib.sha256(
            _canonical_json(list(observation_sha256s)).encode("utf-8")
        ).hexdigest()
        expected_report_id = stable_report_id(
            str(row[4]), session_date, observation_sha256s, str(row[7])
        )
        expected_archive_path = report_archive_relative_path(
            str(row[4]), session_date, expected_report_id
        )
        if (
            str(row[1]) != expected_report_id
            or str(row[6]) != expected_content_sha256
            or str(row[8]) != expected_observation_set_sha256
            or str(row[9]) != expected_archive_path
        ):
            raise MigrationCorruption("stored report audit material is inconsistent")
        return StoredReport(
            report_row_id=int(row[0]),
            report_id=str(row[1]),
            claim_id=int(row[2]),
            session_date=session_date,
            report_kind=str(row[4]),
            body=str(row[5]),
            archive_relative_path=str(row[9]),
            content_sha256=str(row[6]),
            state_sha256=str(row[7]),
            observation_set_sha256=str(row[8]),
            observation_ids=observation_ids,
            observation_sha256s=observation_sha256s,
            created_at=_parse_canonical_timestamp(str(row[10])),
            finalized_at=_parse_canonical_timestamp(str(row[11])),
        )

    def append_outbox(
        self,
        *,
        idempotency_key: str,
        origin_report_id: int | None,
        origin_execution_event_id: int | None,
        destination: str,
        payload_text: str,
        created_at: datetime,
    ) -> tuple[int, bool]:
        """Append an immutable payload; delivery remains externally at-least-once."""
        with self.transaction() as transaction:
            return transaction.append_outbox(
                idempotency_key=idempotency_key,
                origin_report_id=origin_report_id,
                origin_execution_event_id=origin_execution_event_id,
                destination=destination,
                payload_text=payload_text,
                created_at=created_at,
            )

    def record_outbox_delivery_attempt(
        self,
        *,
        outbox_id: int,
        attempt_ordinal: int,
        attempted_at: datetime,
        delivery_status: str,
        external_delivery_id: str | None = None,
        error_class: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        """Append one delivery result without mutating the payload."""
        with self.transaction() as transaction:
            return transaction.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=attempt_ordinal,
                attempted_at=attempted_at,
                delivery_status=delivery_status,
                external_delivery_id=external_delivery_id,
                error_class=error_class,
                details=details,
            )

    def pending_outbox(self, *, limit: int = 100) -> tuple[PendingOutbox, ...]:
        """Return payloads for which no delivered attempt has been recorded."""
        self._ensure_open()
        limit = _require_integer(limit, "pending outbox limit", minimum=1)
        if limit > 1_000:
            raise InvalidJournalValue("pending outbox limit exceeds the supported bound")
        rows = _sql(self._connection,
            "SELECT o.id, o.idempotency_key, o.origin_report_id, "
            "o.origin_execution_event_id, o.destination, o.payload_text, "
            "o.payload_sha256, o.created_at, COALESCE(("
            "SELECT MAX(next_attempt.attempt_ordinal) "
            "FROM outbox_delivery_attempts AS next_attempt "
            "WHERE next_attempt.outbox_id = o.id), 0), "
            "EXISTS(SELECT 1 FROM reports AS origin_report "
            "WHERE origin_report.id = o.origin_report_id), "
            "EXISTS(SELECT 1 FROM execution_events AS origin_event "
            "WHERE origin_event.id = o.origin_execution_event_id) "
            "FROM outbox AS o "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM outbox_delivery_attempts AS a "
            "WHERE a.outbox_id = o.id AND a.delivery_status = 'DELIVERED'"
            ") ORDER BY o.id LIMIT ?",
            (limit,),
        ).fetchall()
        validated: list[
            tuple[int, str, int | None, int | None, str, str, str, datetime, int]
        ] = []
        for row in rows:
            if (
                len(row) != 11
                or type(row[0]) is not int
                or row[0] <= 0
                or type(row[1]) is not str
                or not row[1]
                or type(row[4]) is not str
                or not row[4]
                or type(row[5]) is not str
                or type(row[6]) is not str
                or type(row[7]) is not str
                or type(row[8]) is not int
                or row[8] < 0
                or type(row[9]) is not int
                or type(row[10]) is not int
            ):
                raise MigrationCorruption("pending outbox row is malformed")
            origin_report_id = row[2]
            origin_execution_event_id = row[3]
            if (
                origin_report_id is not None
                and (type(origin_report_id) is not int or origin_report_id <= 0)
            ) or (
                origin_execution_event_id is not None
                and (
                    type(origin_execution_event_id) is not int
                    or origin_execution_event_id <= 0
                )
            ):
                raise MigrationCorruption("pending outbox origin is malformed")
            if (origin_report_id is None) == (origin_execution_event_id is None):
                raise MigrationCorruption("pending outbox origin is inconsistent")
            if (
                (origin_report_id is not None and row[9] != 1)
                or (
                    origin_execution_event_id is not None
                    and row[10] != 1
                )
            ):
                raise MigrationCorruption("pending outbox origin is missing")
            payload_sha256 = hashlib.sha256(row[5].encode("utf-8")).hexdigest()
            if row[6] != payload_sha256:
                raise MigrationCorruption("pending outbox payload integrity failed")
            prior_attempt_ordinal = row[8]
            if prior_attempt_ordinal >= 2**63 - 1:
                raise MigrationCorruption(
                    "outbox delivery attempt ordinal is exhausted"
                )
            validated.append(
                (
                    row[0],
                    row[1],
                    origin_report_id,
                    origin_execution_event_id,
                    row[4],
                    row[5],
                    row[6],
                    _parse_canonical_timestamp(row[7]),
                    prior_attempt_ordinal + 1,
                )
            )
        return tuple(PendingOutbox(*values) for values in validated)

    def start_scheduled_run(
        self,
        *,
        run_key: str,
        run_kind: str,
        session_date: date,
        intended_run_at: datetime,
        started_at: datetime,
    ) -> tuple[int, bool]:
        """Persist a start before scheduled work begins."""
        with self.transaction() as transaction:
            return transaction.start_scheduled_run(
                run_key=run_key,
                run_kind=run_kind,
                session_date=session_date,
                intended_run_at=intended_run_at,
                started_at=started_at,
            )

    def complete_scheduled_run(
        self,
        *,
        run_id: int,
        finished_at: datetime,
        market_session_decision: str,
        outcome: str,
        report_id: int | None = None,
        report_path: str | None = None,
        error_class: str | None = None,
    ) -> tuple[int, bool]:
        """Apply the sole permitted completion transition to a scheduled run."""
        with self.transaction() as transaction:
            return transaction.complete_scheduled_run(
                run_id=run_id,
                finished_at=finished_at,
                market_session_decision=market_session_decision,
                outcome=outcome,
                report_id=report_id,
                report_path=report_path,
                error_class=error_class,
            )

    def append_account_check(
        self,
        *,
        execution_event_id: int,
        settled_cash_micros: int,
        pending_order_count: int,
        unlogged_position_count: int,
        confirmed_at: datetime,
        reconciliation_result: str,
        details: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        """Append one user-confirmed account state snapshot."""
        with self.transaction() as transaction:
            return transaction.append_account_check(
                execution_event_id=execution_event_id,
                settled_cash_micros=settled_cash_micros,
                pending_order_count=pending_order_count,
                unlogged_position_count=unlogged_position_count,
                confirmed_at=confirmed_at,
                reconciliation_result=reconciliation_result,
                details=details,
            )

    def _read_confirmation_result(
        self,
        *,
        message_id: str,
    ) -> StoredIngestionResult | None:
        """Re-read one fully persisted ingestion result from immutable rows."""
        self._ensure_open()
        message_id = _require_nonempty_text(message_id, "message ID")
        raw_identity = _sql(
            self._connection,
            "SELECT id FROM raw_messages "
            "WHERE message_id = ? COLLATE BINARY",
            (message_id,),
        ).fetchone()
        if raw_identity is None:
            return None
        sources = self._authenticate_action_source_cohort(
            raw_message_id=int(raw_identity[0])
        )
        first_source = sources[0]
        raw_row_id = first_source.raw_message_id
        message_time = first_source.message_time
        raw_text = first_source.raw_text
        raw_sha256 = first_source.raw_sha256
        actions: list[StoredIngestionAction] = []
        digest_rows: list[dict[str, object]] = []
        for source in sources:
            details = _canonical_stored_details(
                source.details_json,
                label="confirmation",
            )
            reason_codes = details.get("reason_codes")
            if (
                not isinstance(reason_codes, list)
                or any(
                    not isinstance(reason, str) or not reason
                    for reason in reason_codes
                )
            ):
                raise MigrationCorruption(
                    "confirmation details contract is incomplete"
                )
            action = StoredIngestionAction(
                ordinal=source.action_ordinal,
                event_row_id=source.execution_event_id,
                event_id=source.event_id,
                storage_action=source.storage_action,
                domain_kind=source.domain_kind,
                status=source.compliance_result,
                reason_codes=tuple(reason_codes),
                outbox_id=source.acknowledgement_outbox_id,
                outbox_destination=source.acknowledgement_destination,
            )
            actions.append(action)
            digest_rows.append(
                {
                    "event_row_id": action.event_row_id,
                    "event_id": action.event_id,
                    "ordinal": action.ordinal,
                    "storage_action": action.storage_action,
                    "status": action.status,
                    "details_sha256": source.details_sha256,
                    "outbox_id": action.outbox_id,
                    "outbox_payload_sha256": (
                        source.acknowledgement_payload_sha256
                    ),
                }
            )
        received_at = first_source.received_at
        state_digest = hashlib.sha256(
            _canonical_json(
                {
                    "version": 1,
                    "raw_row_id": raw_row_id,
                    "message_id": message_id,
                    "message_time": _canonical_timestamp(message_time),
                    "received_at": _canonical_timestamp(received_at),
                    "raw_sha256": raw_sha256,
                    "actions": digest_rows,
                }
            ).encode("utf-8")
        ).hexdigest()
        return StoredIngestionResult(
            raw_row_id=raw_row_id,
            message_id=message_id,
            message_time=message_time,
            received_at=received_at,
            raw_text=raw_text,
            raw_sha256=raw_sha256,
            actions=tuple(actions),
            state_digest=state_digest,
        )

    def _validate_action_source_bundle(
        self,
        *,
        event_rows: Sequence[Sequence[object]],
        outbox_rows: Sequence[Sequence[object]],
        cursor_span_count: int | None = None,
    ) -> tuple[
        dict[int, tuple[int, ...]],
        dict[tuple[int, str], Sequence[object]],
    ]:
        """Validate raw-message cohort boundaries shared by all source reads."""
        if cursor_span_count is not None and cursor_span_count != len(event_rows):
            raise MigrationCorruption("CONFIRMATION_ACTION_GROUP_INTERLEAVED")

        ordinals_by_raw: dict[int, list[int]] = {}
        receipt_by_raw: dict[int, datetime] = {}
        event_ids: set[int] = set()
        completed_raws: set[int] = set()
        current_raw: int | None = None
        prior_cursor: int | None = None
        for row in event_rows:
            event_cursor = int(row[0])
            raw_message_id = int(row[2])
            if prior_cursor is not None and event_cursor <= prior_cursor:
                raise MigrationCorruption(
                    "CONFIRMATION_ACTION_CURSOR_ORDER_INVALID"
                )
            prior_cursor = event_cursor
            event_ids.add(event_cursor)
            if raw_message_id != current_raw:
                if current_raw is not None:
                    completed_raws.add(current_raw)
                if raw_message_id in completed_raws:
                    raise MigrationCorruption(
                        "CONFIRMATION_ACTION_GROUP_INTERLEAVED"
                    )
                current_raw = raw_message_id
            ordinals_by_raw.setdefault(raw_message_id, []).append(int(row[3]))

            details = _canonical_stored_details(
                str(row[18]),
                label="confirmation",
            )
            source_details = details.get("source")
            if (
                not isinstance(source_details, dict)
                or source_details.get("type")
                != "ROBINHOOD_MANUAL_CONFIRMATION"
                or type(source_details.get("received_at")) is not str
            ):
                raise MigrationCorruption(
                    "confirmation details contract is inconsistent"
                )
            received_at = _parse_canonical_timestamp(
                str(source_details["received_at"])
            )
            prior_receipt = receipt_by_raw.setdefault(
                raw_message_id,
                received_at,
            )
            if received_at != prior_receipt:
                raise MigrationCorruption(
                    "CONFIRMATION_RECEIPT_COHORT_MISMATCH"
                )

        ordinal_tuples_by_raw: dict[int, tuple[int, ...]] = {}
        for raw_message_id, ordinals in ordinals_by_raw.items():
            ordered = tuple(ordinals)
            if tuple(sorted(ordered)) != tuple(range(len(ordered))):
                raise MigrationCorruption(
                    "confirmation action ordinals are incomplete"
                )
            if ordered != tuple(range(len(ordered))):
                raise MigrationCorruption(
                    "CONFIRMATION_ACTION_CURSOR_ORDER_INVALID"
                )
            ordinal_tuples_by_raw[raw_message_id] = ordered

        outbox_count_by_event: dict[int, int] = {}
        outbox_by_key: dict[
            tuple[int, str], Sequence[object]
        ] = {}
        for row in outbox_rows:
            origin = int(row[3])
            if origin not in event_ids:
                raise MigrationCorruption(
                    "confirmation acknowledgement origin is inconsistent"
                )
            outbox_count_by_event[origin] = (
                outbox_count_by_event.get(origin, 0) + 1
            )
            outbox_by_key[(origin, str(row[4]))] = row
        if any(
            outbox_count_by_event.get(event_cursor, 0) != 1
            for event_cursor in event_ids
        ):
            raise MigrationCorruption(
                "CONFIRMATION_ACKNOWLEDGEMENT_COUNT_INVALID"
            )
        return ordinal_tuples_by_raw, outbox_by_key

    def _authenticate_action_source_cohort(
        self,
        *,
        raw_message_id: int,
    ) -> tuple[JournalActionSource, ...]:
        """Authenticate every immutable action belonging to one raw message."""
        from zoneinfo import ZoneInfo

        from .confirmations import (
            ConfirmationParseError,
            parse_confirmation_batch_or_pending,
        )

        raw_message_id = _require_integer(
            raw_message_id,
            "raw message row ID",
            minimum=1,
        )
        raw_row = _sql(
            self._connection,
            "SELECT " + ", ".join(_RAW_MESSAGE_COLUMNS)
            + " FROM raw_messages WHERE id = ?",
            (raw_message_id,),
        ).fetchone()
        if raw_row is None:
            raise MigrationCorruption("execution event raw row is missing")
        cohort_event_rows = _sql(
            self._connection,
            "SELECT " + ", ".join(_EXECUTION_EVENT_COLUMNS)
            + " FROM execution_events WHERE raw_message_id = ? ORDER BY id",
            (raw_message_id,),
        ).fetchall()
        if not cohort_event_rows:
            raise MigrationCorruption("confirmation raw row has no completed actions")
        first_cursor = int(cohort_event_rows[0][0])
        last_cursor = int(cohort_event_rows[-1][0])
        span_row = _sql(
            self._connection,
            "SELECT COUNT(*) FROM execution_events WHERE id >= ? AND id <= ?",
            (first_cursor, last_cursor),
        ).fetchone()
        if span_row is None:
            raise JournalError("execution cohort span query returned no result")
        nested_span_row = _sql(
            self._connection,
            "SELECT 1 FROM execution_events WHERE raw_message_id != ? "
            "GROUP BY raw_message_id "
            "HAVING MIN(id) < ? AND MAX(id) > ? LIMIT 1",
            (raw_message_id, first_cursor, last_cursor),
        ).fetchone()
        if nested_span_row is not None:
            raise MigrationCorruption("CONFIRMATION_ACTION_GROUP_INTERLEAVED")
        account_rows = _sql(
            self._connection,
            "SELECT "
            + ", ".join(f"account.{column}" for column in _ACCOUNT_CHECK_COLUMNS)
            + " FROM account_checks AS account "
            "JOIN execution_events AS event "
            "ON event.id = account.execution_event_id "
            "WHERE event.raw_message_id = ? "
            "ORDER BY account.execution_event_id, account.id",
            (raw_message_id,),
        ).fetchall()
        outbox_rows = _sql(
            self._connection,
            "SELECT "
            + ", ".join(f"outbox.{column}" for column in _OUTBOX_COLUMNS)
            + " FROM outbox AS outbox JOIN execution_events AS event "
            "ON event.id = outbox.origin_execution_event_id "
            "WHERE event.raw_message_id = ? ORDER BY outbox.id",
            (raw_message_id,),
        ).fetchall()
        ordinal_tuples_by_raw, outbox_by_key = (
            self._validate_action_source_bundle(
                event_rows=cohort_event_rows,
                outbox_rows=outbox_rows,
                cursor_span_count=int(span_row[0]),
            )
        )
        account_by_event = {int(row[3]): row for row in account_rows}
        raw_message_time = _parse_canonical_timestamp(str(raw_row[2]))
        try:
            parsed = parse_confirmation_batch_or_pending(
                str(raw_row[3]),
                session_date=raw_message_time.astimezone(
                    ZoneInfo("America/New_York")
                ).date(),
            )
        except ConfirmationParseError as error:
            raise MigrationCorruption(
                "confirmation action ordinals are incomplete"
            ) from error
        sources: list[JournalActionSource] = []
        for cohort_event_row in cohort_event_rows:
            cohort_execution_event_id = int(cohort_event_row[0])
            sources.append(
                self._action_source_from_rows(
                    event_row=cohort_event_row,
                    raw_row=raw_row,
                    persisted_ordinals=ordinal_tuples_by_raw[raw_message_id],
                    account_row=account_by_event.get(cohort_execution_event_id),
                    outbox_by_key=outbox_by_key,
                    parsed_batch=parsed,
                )
            )
        return tuple(sources)

    def _read_action_source(
        self,
        *,
        execution_event_id: int,
        validate_chronology: bool = True,
    ) -> JournalActionSource:
        """Read and authenticate one exact immutable confirmation action."""
        execution_event_id = _require_integer(
            execution_event_id,
            "execution event row ID",
            minimum=1,
        )
        event_row = _sql(
            self._connection,
            "SELECT raw_message_id FROM execution_events WHERE id = ?",
            (execution_event_id,),
        ).fetchone()
        if event_row is None:
            raise InvalidJournalValue("execution event row does not exist")
        sources = self._authenticate_action_source_cohort(
            raw_message_id=int(event_row[0])
        )
        source = next(
            (
                candidate
                for candidate in sources
                if candidate.execution_event_id == execution_event_id
            ),
            None,
        )
        if source is None:
            raise MigrationCorruption("execution event cohort is incomplete")
        if validate_chronology:
            self._verify_confirmation_chronology(
                through_cursor=execution_event_id
            )
        return source

    def _read_action_sources_bulk(
        self,
        *,
        through_cursor: int,
    ) -> tuple[JournalActionSource, ...]:
        """Authenticate a complete execution prefix with bounded SQL reads."""
        from zoneinfo import ZoneInfo

        from .confirmations import parse_confirmation_batch_or_pending

        through_cursor = _require_integer(
            through_cursor,
            "through execution cursor",
            minimum=1,
        )
        event_rows = _sql(
            self._connection,
            "SELECT " + ", ".join(_EXECUTION_EVENT_COLUMNS)
            + " FROM execution_events WHERE id <= ? ORDER BY id",
            (through_cursor,),
        ).fetchall()
        raw_rows = _sql(
            self._connection,
            "SELECT "
            + ", ".join(f"raw.{column}" for column in _RAW_MESSAGE_COLUMNS)
            + " FROM raw_messages AS raw JOIN ("
            "SELECT DISTINCT raw_message_id FROM execution_events WHERE id <= ?"
            ") AS selected ON selected.raw_message_id = raw.id ORDER BY raw.id",
            (through_cursor,),
        ).fetchall()
        account_rows = _sql(
            self._connection,
            "SELECT " + ", ".join(_ACCOUNT_CHECK_COLUMNS)
            + " FROM account_checks WHERE execution_event_id <= ? "
            "ORDER BY execution_event_id, id",
            (through_cursor,),
        ).fetchall()
        outbox_rows = _sql(
            self._connection,
            "SELECT " + ", ".join(_OUTBOX_COLUMNS)
            + " FROM outbox WHERE origin_execution_event_id IS NOT NULL "
            "AND origin_execution_event_id <= ? "
            "ORDER BY origin_execution_event_id, id",
            (through_cursor,),
        ).fetchall()

        raw_by_id = {int(row[0]): row for row in raw_rows}
        account_by_event = {int(row[3]): row for row in account_rows}
        ordinal_tuples_by_raw, outbox_by_key = (
            self._validate_action_source_bundle(
                event_rows=event_rows,
                outbox_rows=outbox_rows,
            )
        )

        parsed_by_raw: dict[int, object] = {}
        sources: list[JournalActionSource] = []
        for event_row in event_rows:
            raw_message_id = int(event_row[2])
            raw_row = raw_by_id.get(raw_message_id)
            if raw_row is None:
                raise MigrationCorruption("execution event raw row is missing")
            parsed = parsed_by_raw.get(raw_message_id)
            if parsed is None:
                raw_message_time = _parse_canonical_timestamp(str(raw_row[2]))
                parsed = parse_confirmation_batch_or_pending(
                    str(raw_row[3]),
                    session_date=raw_message_time.astimezone(
                        ZoneInfo("America/New_York")
                    ).date(),
                )
                parsed_by_raw[raw_message_id] = parsed
            sources.append(
                self._action_source_from_rows(
                    event_row=event_row,
                    raw_row=raw_row,
                    persisted_ordinals=ordinal_tuples_by_raw[raw_message_id],
                    account_row=account_by_event.get(int(event_row[0])),
                    outbox_by_key=outbox_by_key,
                    parsed_batch=parsed,
                )
            )
        return tuple(sources)

    def _action_source_from_rows(
        self,
        *,
        event_row: Sequence[object],
        raw_row: Sequence[object],
        persisted_ordinals: tuple[int, ...],
        account_row: Sequence[object] | None,
        outbox_by_key: Mapping[tuple[int, str], Sequence[object]],
        parsed_batch: object | None,
    ) -> JournalActionSource:
        """Build one action through the shared exact semantic validator."""
        from zoneinfo import ZoneInfo

        from .confirmations import (
            ConfirmationKind,
            ParsedConfirmation,
            PendingConfirmation,
            parse_confirmation_batch_or_pending,
        )

        execution_event_id = _require_integer(
            int(event_row[0]),
            "execution event row ID",
            minimum=1,
        )
        event_reference = _journal_row_reference(
            "execution_events",
            _EXECUTION_EVENT_COLUMNS,
            event_row,
        )
        raw_message_id = int(event_row[2])
        raw_reference = _journal_row_reference(
            "raw_messages",
            _RAW_MESSAGE_COLUMNS,
            raw_row,
        )
        if int(raw_row[0]) != raw_message_id:
            raise MigrationCorruption("execution event raw identity is inconsistent")
        raw_text = str(raw_row[3])
        raw_sha256 = str(raw_row[4])
        if hashlib.sha256(raw_text.encode("utf-8")).hexdigest() != raw_sha256:
            raise MigrationCorruption("raw confirmation hash does not match its text")
        message_id = str(raw_row[1])
        message_time = _parse_canonical_timestamp(str(raw_row[2]))
        if str(event_row[15]) != str(raw_row[2]):
            raise MigrationCorruption("execution event message time conflicts with raw")
        event_time = _parse_canonical_timestamp(str(event_row[14]))
        action_ordinal = int(event_row[3])
        expected_event_id, expected_idempotency_key = stable_execution_event_identity(
            message_id,
            action_ordinal,
        )
        if (
            str(event_row[1]) != expected_event_id
            or str(event_row[4]) != expected_idempotency_key
        ):
            raise MigrationCorruption("execution event identity is inconsistent")

        parsed = parsed_batch
        if parsed is None:
            parsed = parse_confirmation_batch_or_pending(
                raw_text,
                session_date=message_time.astimezone(
                    ZoneInfo("America/New_York")
                ).date(),
            )
        parsed_action: ParsedConfirmation | PendingConfirmation
        if isinstance(parsed, PendingConfirmation):
            expected_action_count = 1
            if action_ordinal != 0:
                raise MigrationCorruption("pending confirmation ordinal is inconsistent")
            parsed_action = parsed
            expected_storage_action = ConfirmationKind.PENDING_CLARIFICATION.value
            expected_domain_kind = expected_storage_action
            expected_event_time = message_time
            expected_event_time_basis = "MESSAGE_TIME_OBSERVATION"
            expected_symbol = None
            expected_shares = None
            expected_price = None
            expected_bid = None
            expected_ask = None
            expected_stop = None
            expected_missing: list[str] = []
            expected_normalized: dict[str, object] = {}
        else:
            expected_action_count = len(parsed)
            if action_ordinal >= len(parsed):
                raise MigrationCorruption("confirmation action ordinal is incomplete")
            parsed_action = parsed[action_ordinal]
            expected_storage_action = parsed_action.kind.value
            expected_domain_kind = parsed_action.kind.value
            expected_event_time = (
                message_time
                if parsed_action.event_time is None
                else parsed_action.event_time.astimezone(timezone.utc)
            )
            expected_event_time_basis = parsed_action.event_time_basis
            expected_symbol = parsed_action.symbol
            expected_shares = parsed_action.quantity
            expected_price = (
                None
                if parsed_action.price is None
                else money_to_micros(parsed_action.price)
            )
            expected_bid = (
                None
                if parsed_action.bid is None
                else money_to_micros(parsed_action.bid)
            )
            expected_ask = (
                None
                if parsed_action.ask is None
                else money_to_micros(parsed_action.ask)
            )
            expected_stop = (
                None
                if parsed_action.stop is None
                else money_to_micros(parsed_action.stop)
            )
            expected_missing = sorted(parsed_action.missing_fields)
            expected_normalized = _expected_confirmation_normalized(parsed_action)

        if persisted_ordinals != tuple(range(expected_action_count)):
            raise MigrationCorruption(
                "confirmation action ordinals are incomplete"
            )

        if (
            str(event_row[6]) != expected_storage_action
            or event_row[7] != expected_symbol
            or event_row[8] != expected_shares
            or event_row[9] != expected_price
            or event_row[10] != expected_bid
            or event_row[11] != expected_ask
            or event_row[12] is not None
            or event_row[13] != expected_stop
            or event_time != expected_event_time
        ):
            raise MigrationCorruption(
                "execution event fields conflict with reparsed raw confirmation"
            )

        details_json = str(event_row[18])
        details = _canonical_stored_details(
            details_json,
            label="confirmation",
        )
        source_details = details.get("source")
        acknowledgement = details.get("acknowledgement")
        reason_codes = details.get("reason_codes")
        if (
            details.get("version") != 1
            or details.get("domain_kind") != expected_domain_kind
            or details.get("event_time_basis") != expected_event_time_basis
            or details.get("missing_fields") != expected_missing
            or details.get("normalized") != expected_normalized
            or not isinstance(source_details, dict)
            or source_details.get("type") != "ROBINHOOD_MANUAL_CONFIRMATION"
            or type(source_details.get("received_at")) is not str
            or not isinstance(acknowledgement, dict)
            or type(acknowledgement.get("destination")) is not str
            or not acknowledgement.get("destination")
            or type(acknowledgement.get("idempotency_key")) is not str
            or not isinstance(reason_codes, list)
            or any(type(reason) is not str or not reason for reason in reason_codes)
            or len(reason_codes) != len(set(reason_codes))
        ):
            raise MigrationCorruption("confirmation details contract is inconsistent")
        received_at = _parse_canonical_timestamp(str(source_details["received_at"]))
        if not event_time <= message_time <= received_at:
            raise MigrationCorruption("confirmation source times are out of order")
        compliance_result = str(event_row[16])
        reconciliation_state = str(event_row[17])
        if (
            (compliance_result == "COMPLIANT" and reason_codes)
            or (
                compliance_result == "NONCOMPLIANT_RECONCILIATION_REQUIRED"
                and not reason_codes
            )
            or (
                compliance_result == "PENDING_CLARIFICATION"
                and (not reason_codes or reconciliation_state != "PENDING")
            )
            or compliance_result
            not in {
                "COMPLIANT",
                "NONCOMPLIANT_RECONCILIATION_REQUIRED",
                "PENDING_CLARIFICATION",
            }
        ):
            raise MigrationCorruption("confirmation assessment fields are inconsistent")

        economic_roles = {
            ConfirmationKind.BUY.value,
            ConfirmationKind.PARTIAL_FILL.value,
            ConfirmationKind.SOLD.value,
            ConfirmationKind.STOP_FILLED.value,
            ConfirmationKind.STOP_UPDATED.value,
            ConfirmationKind.FEE.value,
            ConfirmationKind.RECONCILE_CASH.value,
            ConfirmationKind.RECONCILE_UNRELATED_POSITION.value,
        }
        expected_event_role = (
            "ECONOMIC"
            if expected_domain_kind in economic_roles
            else "OBSERVATION"
        )
        event_role = details.get("event_role", expected_event_role)
        if event_role != expected_event_role:
            raise MigrationCorruption("confirmation event role is inconsistent")

        account_check: JournalAccountCheckSource | None = None
        row_references = [raw_reference, event_reference]
        if expected_domain_kind == ConfirmationKind.ACCOUNT_CHECK.value:
            if account_row is None or not isinstance(parsed_action, ParsedConfirmation):
                raise MigrationCorruption("account-check source row is missing")
            account_reference = _journal_row_reference(
                "account_checks",
                _ACCOUNT_CHECK_COLUMNS,
                account_row,
            )
            account_details_json = str(account_row[9])
            _canonical_stored_details(account_details_json, label="account-check")
            expected_check_id = "chk_" + hashlib.sha256(
                (
                    "stock-monitor/account-check/v1\x00"
                    + expected_event_id
                ).encode("utf-8")
            ).hexdigest()
            if (
                str(account_row[1]) != expected_check_id
                or int(account_row[2]) != raw_message_id
                or int(account_row[3]) != execution_event_id
                or int(account_row[4]) != money_to_micros(parsed_action.settled_cash)
                or int(account_row[5]) != parsed_action.pending_orders
                or int(account_row[6]) != parsed_action.unlogged_positions
                or _parse_canonical_timestamp(str(account_row[7])) != event_time
                or str(account_row[8])
                != (
                    "CLEAR"
                    if compliance_result == "COMPLIANT"
                    else "RECONCILIATION_REQUIRED"
                )
            ):
                raise MigrationCorruption(
                    "account-check row conflicts with reparsed confirmation"
                )
            account_check = JournalAccountCheckSource(
                row_id=int(account_row[0]),
                check_id=str(account_row[1]),
                raw_message_id=int(account_row[2]),
                execution_event_id=int(account_row[3]),
                settled_cash_micros=int(account_row[4]),
                pending_order_count=int(account_row[5]),
                unlogged_position_count=int(account_row[6]),
                confirmed_at=_parse_canonical_timestamp(str(account_row[7])),
                reconciliation_result=str(account_row[8]),
                details_json=account_details_json,
                row_reference=account_reference,
            )
            row_references.append(account_reference)
        elif account_row is not None:
            raise MigrationCorruption("non-account event has an account-check row")

        acknowledgement_destination = str(acknowledgement["destination"])
        acknowledgement_idempotency_key = str(
            acknowledgement["idempotency_key"]
        )
        if acknowledgement_idempotency_key != _confirmation_outbox_key(
            expected_event_id,
            acknowledgement_destination,
        ):
            raise MigrationCorruption(
                "confirmation acknowledgement identity is inconsistent"
            )
        outbox_row = outbox_by_key.get(
            (execution_event_id, acknowledgement_destination)
        )
        if outbox_row is None:
            raise MigrationCorruption(
                "confirmation acknowledgement outbox row is missing"
            )
        expected_payload = _canonical_json(
            {
                "event_id": expected_event_id,
                "kind": expected_domain_kind,
                "ordinal": action_ordinal,
                "reason_codes": reason_codes,
                "status": compliance_result,
                "version": 1,
            }
        )
        expected_payload_sha256 = hashlib.sha256(
            expected_payload.encode("utf-8")
        ).hexdigest()
        if (
            str(outbox_row[1]) != acknowledgement_idempotency_key
            or outbox_row[2] is not None
            or int(outbox_row[3]) != execution_event_id
            or str(outbox_row[4]) != acknowledgement_destination
            or str(outbox_row[5]) != expected_payload
            or str(outbox_row[6]) != expected_payload_sha256
            or _parse_canonical_timestamp(str(outbox_row[7])) != received_at
        ):
            raise MigrationCorruption(
                "confirmation acknowledgement outbox row is inconsistent"
            )
        outbox_reference = _journal_row_reference(
            "outbox",
            _OUTBOX_COLUMNS,
            outbox_row,
        )
        row_references.append(outbox_reference)

        ordered_references = tuple(
            sorted(row_references, key=lambda item: (item.table, item.row_id))
        )
        details_sha256 = hashlib.sha256(details_json.encode("utf-8")).hexdigest()
        source_digest = _journal_bundle_digest(
            "stock-monitor/journal-action-source/v1",
            ordered_references,
            {
                "action_ordinal": action_ordinal,
                "execution_event_id": execution_event_id,
                "message_id": message_id,
            },
        )
        normalized = details["normalized"]
        assert isinstance(normalized, dict)
        source = JournalActionSource(
            execution_event_id=execution_event_id,
            event_id=expected_event_id,
            raw_message_id=raw_message_id,
            message_id=message_id,
            action_ordinal=action_ordinal,
            idempotency_key=expected_idempotency_key,
            storage_action=expected_storage_action,
            domain_kind=expected_domain_kind,
            signal_id=None if event_row[5] is None else str(event_row[5]),
            symbol=expected_symbol,
            shares=expected_shares,
            price_micros=expected_price,
            bid_micros=expected_bid,
            ask_micros=expected_ask,
            recommended_stop_micros=None,
            user_confirmed_stop_micros=expected_stop,
            event_time=event_time,
            message_time=message_time,
            received_at=received_at,
            compliance_result=compliance_result,
            reconciliation_state=reconciliation_state,
            raw_text=raw_text,
            raw_sha256=raw_sha256,
            details_json=details_json,
            details_sha256=details_sha256,
            parent_order_id=normalized.get("parent_order_id"),  # type: ignore[arg-type]
            fill_group_planned_shares=normalized.get(
                "fill_group_planned_shares"
            ),  # type: ignore[arg-type]
            event_role=str(event_role),
            account_check=account_check,
            acknowledgement_outbox_id=int(outbox_row[0]),
            acknowledgement_destination=acknowledgement_destination,
            acknowledgement_idempotency_key=acknowledgement_idempotency_key,
            acknowledgement_payload_sha256=expected_payload_sha256,
            row_references=ordered_references,
            source_digest=source_digest,
        )
        return source

    def _validate_confirmation_source_order(
        self,
        *,
        message_time: datetime,
        received_at: datetime,
    ) -> None:
        stored_message_time = _parse_canonical_timestamp(
            _canonical_timestamp(message_time)
        )
        stored_received_at = _parse_canonical_timestamp(
            _canonical_timestamp(received_at)
        )
        if stored_message_time > stored_received_at:
            raise InvalidJournalValue("CONFIRMATION_SOURCE_TIME_OUT_OF_ORDER")
        prior_rows = _sql(
            self._connection,
            "SELECT message_time, details_json FROM execution_events "
            "ORDER BY id DESC",
        ).fetchall()
        for prior in prior_rows:
            prior_details = _canonical_stored_details(
                str(prior[1]),
                label="execution event",
            )
            prior_source = prior_details.get("source")
            if (
                not isinstance(prior_source, dict)
                or prior_source.get("type")
                != "ROBINHOOD_MANUAL_CONFIRMATION"
            ):
                continue
            if type(prior_source.get("received_at")) is not str:
                raise MigrationCorruption("confirmation source receipt is missing")
            prior_message_time = _parse_canonical_timestamp(str(prior[0]))
            prior_received_at = _parse_canonical_timestamp(
                str(prior_source["received_at"])
            )
            if (
                stored_message_time < prior_message_time
                or stored_received_at < prior_received_at
            ):
                raise InvalidJournalValue("CONFIRMATION_RECEIPT_TIME_REGRESSION")
            return

    def _verify_confirmation_chronology(self, *, through_cursor: int) -> None:
        """Verify nondecreasing source knowledge order through one cursor."""
        rows = _sql(
            self._connection,
            "SELECT id, message_time, details_json FROM execution_events "
            "WHERE id <= ? ORDER BY id",
            (through_cursor,),
        ).fetchall()
        prior_message: datetime | None = None
        prior_receipt: datetime | None = None
        for row in rows:
            message_time = _parse_canonical_timestamp(str(row[1]))
            details = _canonical_stored_details(
                str(row[2]),
                label="confirmation",
            )
            source = details.get("source")
            if (
                not isinstance(source, dict)
                or source.get("type") != "ROBINHOOD_MANUAL_CONFIRMATION"
            ):
                continue
            if type(source.get("received_at")) is not str:
                raise MigrationCorruption("confirmation source receipt is missing")
            received_at = _parse_canonical_timestamp(str(source["received_at"]))
            if (
                prior_message is not None
                and (message_time < prior_message or received_at < prior_receipt)
            ):
                raise MigrationCorruption("CONFIRMATION_RECEIPT_TIME_REGRESSION")
            prior_message = message_time
            prior_receipt = received_at

    def _read_account_check_window(
        self,
        *,
        account_check_event_id: int,
        terminal_event_id: int,
    ) -> JournalAccountCheckWindowSource:
        """Read the exact latest-check interval in one SQLite snapshot."""
        account_check_event_id = _require_integer(
            account_check_event_id,
            "account-check execution cursor",
            minimum=1,
        )
        terminal_event_id = _require_integer(
            terminal_event_id,
            "terminal execution cursor",
            minimum=1,
        )
        if terminal_event_id <= account_check_event_id:
            raise InvalidJournalValue("account-check window endpoints are out of order")
        latest_prior = _sql(
            self._connection,
            "SELECT MAX(execution_event_id) FROM account_checks "
            "WHERE execution_event_id < ?",
            (terminal_event_id,),
        ).fetchone()
        if (
            latest_prior is None
            or latest_prior[0] is None
            or int(latest_prior[0]) != account_check_event_id
        ):
            raise InvalidJournalValue("ACCOUNT_CHECK_ENDPOINT_NOT_LATEST")
        check_order = _sql(
            self._connection,
            "SELECT checkrow.execution_event_id, event.event_time "
            "FROM account_checks AS checkrow "
            "JOIN execution_events AS event "
            "ON event.id = checkrow.execution_event_id "
            "WHERE checkrow.execution_event_id < ? "
            "ORDER BY event.event_time DESC, checkrow.execution_event_id DESC",
            (terminal_event_id,),
        ).fetchall()
        if (
            check_order
            and int(check_order[0][0]) != account_check_event_id
        ):
            raise InvalidJournalValue("ACCOUNT_CHECK_EFFECTIVE_ORDER_AMBIGUOUS")
        account_action = self._read_action_source(
            execution_event_id=account_check_event_id,
            validate_chronology=False,
        )
        terminal_action = self._read_action_source(
            execution_event_id=terminal_event_id,
            validate_chronology=False,
        )
        if account_action.domain_kind != "ACCOUNT_CHECK" or (
            terminal_action.domain_kind not in {"BOUGHT", "PARTIAL_FILL"}
        ):
            raise InvalidJournalValue("account-check window endpoints are invalid")
        between_rows = _sql(
            self._connection,
            "SELECT id FROM execution_events WHERE id > ? AND id < ? ORDER BY id",
            (account_check_event_id, terminal_event_id),
        ).fetchall()
        between_actions = tuple(
            self._read_action_source(
                execution_event_id=int(row[0]),
                validate_chronology=False,
            )
            for row in between_rows
        )
        self._verify_confirmation_chronology(through_cursor=terminal_event_id)
        high_water_row = _sql(
            self._connection,
            "SELECT COALESCE(MAX(id), 0) FROM execution_events",
        ).fetchone()
        if high_water_row is None:
            raise JournalError("execution high-water query returned no result")
        source_high_water_cursor = int(high_water_row[0])
        late_effective_rows = _sql(
            self._connection,
            "SELECT id FROM execution_events "
            "WHERE id > ? ORDER BY id",
            (terminal_event_id,),
        ).fetchall()
        for row in late_effective_rows:
            late_action = self._read_action_source(
                execution_event_id=int(row[0]),
                validate_chronology=False,
            )
            if late_action.domain_kind == "PENDING_CLARIFICATION" or (
                late_action.event_time <= terminal_action.event_time
                and late_action.domain_kind in _RECONCILIATION_ACTIONS
            ):
                raise InvalidJournalValue(
                    "ACCOUNT_CHECK_LATE_FACT_INVALIDATES_WINDOW"
                )
        references_by_key: dict[
            tuple[str, int], JournalRowReference
        ] = {}
        for action in (account_action, *between_actions, terminal_action):
            for reference in action.row_references:
                key = (reference.table, reference.row_id)
                prior = references_by_key.get(key)
                if prior is not None and prior != reference:
                    raise MigrationCorruption("account-check window row digest conflicts")
                references_by_key[key] = reference
        row_references = tuple(
            sorted(
                references_by_key.values(),
                key=lambda item: (item.table, item.row_id),
            )
        )
        source_digest = _journal_bundle_digest(
            "stock-monitor/journal-account-window/v1",
            row_references,
            {
                "after_cursor": account_check_event_id,
                "expected_between_count": len(between_actions),
                "source_high_water_cursor": source_high_water_cursor,
                "through_cursor": terminal_event_id,
            },
        )
        return JournalAccountCheckWindowSource(
            account_check_action=account_action,
            terminal_action=terminal_action,
            between_actions=between_actions,
            after_cursor=account_check_event_id,
            through_cursor=terminal_event_id,
            source_high_water_cursor=source_high_water_cursor,
            expected_between_count=len(between_actions),
            row_references=row_references,
            source_digest=source_digest,
        )

    def _read_actual_replay(
        self,
        *,
        query_cutoff: datetime,
        through_execution_cursor: int | None,
    ) -> JournalActualReplaySource:
        """Read one complete received-by-cutoff confirmation replay snapshot."""
        normalized_cutoff = _parse_canonical_timestamp(
            _canonical_timestamp(query_cutoff)
        )
        orphan_raw = _sql(
            self._connection,
            "SELECT raw.id FROM raw_messages AS raw "
            "LEFT JOIN execution_events AS event ON event.raw_message_id = raw.id "
            "GROUP BY raw.id HAVING COUNT(event.id) = 0 "
            "ORDER BY raw.id LIMIT 1",
        ).fetchone()
        if orphan_raw is not None:
            raise MigrationCorruption("INCOMPLETE_CONFIRMATION_RAW_SOURCE")
        high_water_row = _sql(
            self._connection,
            "SELECT MAX(id) FROM execution_events",
        ).fetchone()
        if high_water_row is None:
            raise JournalError("execution high-water query returned no result")
        source_high_water_cursor = (
            None if high_water_row[0] is None else int(high_water_row[0])
        )
        all_actions: list[JournalActionSource] = []
        if source_high_water_cursor is not None:
            all_actions.extend(
                self._read_action_sources_bulk(
                    through_cursor=source_high_water_cursor
                )
            )
            self._verify_confirmation_chronology(
                through_cursor=source_high_water_cursor
            )
        actions = tuple(
            action
            for action in all_actions
            if action.received_at <= normalized_cutoff
        )
        expected_through = (
            actions[-1].execution_event_id if actions else None
        )
        if through_execution_cursor is not None:
            requested_through = _require_integer(
                through_execution_cursor,
                "through execution cursor",
                minimum=1,
            )
            if requested_through != expected_through:
                raise InvalidJournalValue("REPLAY_CUTOFF_INCOMPLETE")
        bounded_through = expected_through
        action_ids = tuple(action.execution_event_id for action in actions)
        check_ids = tuple(
            action.account_check.row_id
            for action in actions
            if action.account_check is not None
        )

        postings: list[JournalPostingSource] = []
        if action_ids or check_ids:
            predicates: list[str] = []
            parameters: list[int] = []
            if action_ids:
                predicates.append(
                    "execution_event_id IN ("
                    + ",".join("?" for _ in action_ids)
                    + ")"
                )
                parameters.extend(action_ids)
            if check_ids:
                predicates.append(
                    "account_check_id IN ("
                    + ",".join("?" for _ in check_ids)
                    + ")"
                )
                parameters.extend(check_ids)
            posting_rows = _sql(
                self._connection,
                "SELECT " + ", ".join(_LEDGER_POSTING_COLUMNS)
                + " FROM ledger_postings WHERE "
                + " OR ".join(f"({predicate})" for predicate in predicates)
                + " ORDER BY id",
                tuple(parameters),
            ).fetchall()
        else:
            posting_rows = ()
        for row in posting_rows:
            posting_reference = _journal_row_reference(
                "ledger_postings",
                _LEDGER_POSTING_COLUMNS,
                row,
            )
            details_json = str(row[12])
            _canonical_stored_details(details_json, label="ledger posting")
            execution_id = None if row[5] is None else int(row[5])
            account_check_id = None if row[6] is None else int(row[6])
            if (execution_id is None) == (account_check_id is None):
                raise MigrationCorruption("ledger posting origin is inconsistent")
            postings.append(
                JournalPostingSource(
                    row_id=int(row[0]),
                    posting_key=str(row[1]),
                    ledger_name=str(row[2]),
                    account_name=str(row[3]),
                    entry_kind=str(row[4]),
                    execution_event_id=execution_id,
                    account_check_id=account_check_id,
                    symbol=None if row[7] is None else str(row[7]),
                    amount_micros=int(row[8]),
                    shares_delta=None if row[9] is None else int(row[9]),
                    unit_price_micros=None if row[10] is None else int(row[10]),
                    occurred_at=_parse_canonical_timestamp(str(row[11])),
                    details_json=details_json,
                    row_reference=posting_reference,
                )
            )

        projection_rows: list[JournalProjectionRowSource] = []
        projection_through_candidates: list[int] = []
        actions_by_cursor = {
            action.execution_event_id: action for action in all_actions
        }
        for table in (
            "actual_positions",
            "actual_cash_projection",
            "reconciliation_projection",
        ):
            column_rows = _sql(
                self._connection,
                f'PRAGMA table_info("{table}")',
            ).fetchall()
            columns = tuple(str(row[1]) for row in column_rows)
            rows = _sql(
                self._connection,
                f'SELECT * FROM "{table}" ORDER BY id',
            ).fetchall()
            for row in rows:
                values = dict(zip(columns, tuple(row), strict=True))
                origin_cursor: int | None = None
                if table in {"actual_positions", "reconciliation_projection"}:
                    cursor = values.get("last_execution_event_id")
                    if cursor is not None:
                        origin_cursor = int(cursor)
                elif table == "actual_cash_projection":
                    posting_id = values.get("last_ledger_posting_id")
                    if posting_id is not None:
                        posting_origin = _sql(
                            self._connection,
                            "SELECT posting.execution_event_id, "
                            "checkrow.execution_event_id "
                            "FROM ledger_postings AS posting "
                            "LEFT JOIN account_checks AS checkrow "
                            "ON checkrow.id = posting.account_check_id "
                            "WHERE posting.id = ?",
                            (int(posting_id),),
                        ).fetchone()
                        if posting_origin is None:
                            raise MigrationCorruption(
                                "cash projection posting high-water is missing"
                            )
                        cursor = (
                            posting_origin[0]
                            if posting_origin[0] is not None
                            else posting_origin[1]
                        )
                        if cursor is not None:
                            origin_cursor = int(cursor)
                if origin_cursor is None:
                    raise MigrationCorruption(
                        "projection source execution cursor is missing"
                    )
                origin_action = actions_by_cursor.get(origin_cursor)
                if origin_action is None:
                    raise MigrationCorruption(
                        "projection source action is not authenticated"
                    )
                updated_at = values.get("updated_at")
                if type(updated_at) is not str:
                    raise MigrationCorruption("projection update time is missing")
                projection_updated_at = _parse_canonical_timestamp(updated_at)
                if (
                    origin_action.received_at > normalized_cutoff
                    or projection_updated_at > normalized_cutoff
                    or bounded_through is None
                    or origin_cursor > bounded_through
                ):
                    continue
                reference = _journal_row_reference(table, columns, row)
                projection_rows.append(
                    JournalProjectionRowSource(
                        table=table,
                        row_id=int(row[0]),
                        values=tuple(zip(columns, tuple(row), strict=True)),
                        row_reference=reference,
                    )
                )
                projection_through_candidates.append(origin_cursor)
        projection_through_cursor = (
            max(projection_through_candidates)
            if projection_through_candidates
            else None
        )
        last_economic_cursor = max(
            (
                action.execution_event_id
                for action in actions
                if action.event_role == "ECONOMIC"
            ),
            default=None,
        )
        projection_stale = last_economic_cursor is not None and (
            projection_through_cursor is None
            or projection_through_cursor < last_economic_cursor
        )

        references_by_key: dict[
            tuple[str, int], JournalRowReference
        ] = {}
        for reference in (
            *(
                reference
                for action in actions
                for reference in action.row_references
            ),
            *(posting.row_reference for posting in postings),
            *(row.row_reference for row in projection_rows),
        ):
            key = (reference.table, reference.row_id)
            prior = references_by_key.get(key)
            if prior is not None and prior != reference:
                raise MigrationCorruption("actual replay row digest conflicts")
            references_by_key[key] = reference
        row_references = tuple(
            sorted(
                references_by_key.values(),
                key=lambda item: (item.table, item.row_id),
            )
        )
        start_cursor = action_ids[0] if action_ids else None
        terminal_cursor = action_ids[-1] if action_ids else None
        source_digest = _journal_bundle_digest(
            "stock-monitor/journal-actual-replay/v1",
            row_references,
            {
                "expected_action_count": len(actions),
                "expected_posting_count": len(postings),
                "projection_stale": projection_stale,
                "projection_through_cursor": projection_through_cursor,
                "query_cutoff": _canonical_timestamp(normalized_cutoff),
                "source_through_cursor": source_high_water_cursor,
                "start_cursor": start_cursor,
                "terminal_cursor": terminal_cursor,
                "through_execution_cursor": bounded_through,
            },
        )
        return JournalActualReplaySource(
            query_cutoff=normalized_cutoff,
            actions=actions,
            postings=tuple(postings),
            projection_rows=tuple(projection_rows),
            start_cursor=start_cursor,
            terminal_cursor=terminal_cursor,
            through_execution_cursor=bounded_through,
            source_through_cursor=source_high_water_cursor,
            projection_through_cursor=projection_through_cursor,
            projection_stale=projection_stale,
            expected_action_count=len(actions),
            expected_posting_count=len(postings),
            row_references=row_references,
            source_digest=source_digest,
        )

    def _incremental_ingestion_identity(self) -> tuple[int, int, int | None]:
        """Identify one complete snapshot for a private same-process cache."""
        self._ensure_open()
        if not self._transaction_active or self._transaction_dirty:
            raise JournalError(
                "incremental ingestion identity requires a clean transaction"
            )
        orphan_raw = _sql(
            self._connection,
            "SELECT raw.id FROM raw_messages AS raw "
            "LEFT JOIN execution_events AS event ON event.raw_message_id = raw.id "
            "GROUP BY raw.id HAVING COUNT(event.id) = 0 "
            "ORDER BY raw.id LIMIT 1",
        ).fetchone()
        if orphan_raw is not None:
            raise MigrationCorruption("INCOMPLETE_CONFIRMATION_RAW_SOURCE")
        row = _sql(
            self._connection,
            "SELECT MAX(id) FROM execution_events",
        ).fetchone()
        if row is None:
            raise JournalError("execution high-water query returned no result")
        highwater = None if row[0] is None else int(row[0])
        return (
            self._source_generation,
            self._source_authority_data_version(),
            highwater,
        )

    def count(self, table: str) -> int:
        self._ensure_open()
        table = _whitelisted_table(table)
        row = _sql(self._connection, f'SELECT COUNT(*) FROM "{table}"').fetchone()
        if row is None:
            raise JournalError("journal count query returned no result")
        return int(row[0])

    def table_info(self, table: str) -> dict[str, str]:
        self._ensure_open()
        table = _whitelisted_table(table)
        rows = _sql(self._connection, f'PRAGMA table_info("{table}")').fetchall()
        return {str(row[1]): str(row[2]) for row in rows}

    def pragma(self, name: str) -> int | str:
        """Return one non-sensitive, explicitly allow-listed connection setting."""
        self._ensure_open()
        if name == "journal_mode":
            row = _sql(self._connection, "PRAGMA journal_mode").fetchone()
            if row is None:
                raise JournalError("journal pragma returned no result")
            return str(row[0]).lower()
        return _pragma_int(self._connection, name)

    def _append_raw_message(
        self, message_id: str, message_time: datetime, text: str
    ) -> tuple[int, bool]:
        _require_nonempty_text(message_id, "message ID")
        if "\x00" in message_id:
            raise InvalidJournalValue("message ID contains an invalid character")
        if type(text) is not str:
            raise InvalidJournalValue("raw message text must be a string")
        stored_time = _canonical_timestamp(message_time)
        raw_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
        immutable = (stored_time, text, raw_sha256)
        existing = _sql(self._connection,
            "SELECT id, message_time, raw_text, raw_sha256 "
            "FROM raw_messages WHERE message_id = ? COLLATE BINARY",
            (message_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "raw message identity conflicts with stored content"
                )
            return int(existing[0]), True
        cursor = _sql(self._connection,
            "INSERT INTO raw_messages(message_id, message_time, raw_text, raw_sha256) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(message_id) DO NOTHING",
            (message_id, stored_time, text, raw_sha256),
        )
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, message_time, raw_text, raw_sha256 "
            "FROM raw_messages WHERE message_id = ? COLLATE BINARY",
            (message_id,),
        ).fetchone()
        if row is None:
            raise JournalError("raw message insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict("raw message identity conflicts with stored content")
        return int(row[0]), not inserted

    def _append_execution_event(
        self,
        *,
        raw_message_id: int,
        action_ordinal: int,
        parsed_action: str,
        event_time: datetime,
        signal_id: str | None,
        symbol: str | None,
        shares: int | None,
        price_micros: int | None,
        bid_micros: int | None,
        ask_micros: int | None,
        recommended_stop_micros: int | None,
        user_confirmed_stop_micros: int | None,
        compliance_result: str,
        reconciliation_state: str,
        details: Mapping[str, object] | None,
        prevalidated_confirmation_source: tuple[datetime, datetime] | None = None,
    ) -> tuple[int, bool]:
        raw_message_id = _require_integer(
            raw_message_id, "raw message row ID", minimum=1
        )
        action_ordinal = _require_integer(
            action_ordinal, "action ordinal", minimum=0
        )
        parsed_action = _require_nonempty_text(parsed_action, "parsed action")
        compliance_result = _require_nonempty_text(
            compliance_result, "compliance result"
        )
        reconciliation_state = _require_nonempty_text(
            reconciliation_state, "reconciliation state"
        )
        signal_id = _optional_text(signal_id, "signal ID")
        symbol = _optional_text(symbol, "symbol")
        if symbol is not None:
            symbol = symbol.upper()
        shares = _optional_integer(shares, "shares", minimum=1)
        price_micros = _optional_integer(price_micros, "price", minimum=1)
        bid_micros = _optional_integer(bid_micros, "bid", minimum=1)
        ask_micros = _optional_integer(ask_micros, "ask", minimum=1)
        recommended_stop_micros = _optional_integer(
            recommended_stop_micros, "recommended stop", minimum=1
        )
        user_confirmed_stop_micros = _optional_integer(
            user_confirmed_stop_micros, "user-confirmed stop", minimum=1
        )
        stored_event_time = _canonical_timestamp(event_time)
        details_json = _canonical_details(details)

        raw = _sql(self._connection,
            "SELECT message_id, message_time FROM raw_messages WHERE id = ?",
            (raw_message_id,),
        ).fetchone()
        if raw is None:
            raise InvalidJournalValue("raw message row does not exist")
        message_id, stored_message_time = str(raw[0]), str(raw[1])
        event_details = json.loads(details_json)
        source_details = event_details.get("source")
        if (
            isinstance(source_details, dict)
            and source_details.get("type")
            == "ROBINHOOD_MANUAL_CONFIRMATION"
        ):
            received_value = source_details.get("received_at")
            if type(received_value) is not str:
                raise InvalidJournalValue(
                    "confirmation source receipt is missing"
                )
            received_at = _parse_canonical_timestamp(received_value)
            message_time = _parse_canonical_timestamp(stored_message_time)
            if prevalidated_confirmation_source != (message_time, received_at):
                self._validate_confirmation_source_order(
                    message_time=message_time,
                    received_at=received_at,
                )
        if stored_event_time > stored_message_time:
            raise InvalidJournalValue(
                "execution event cannot postdate its authoritative message"
            )
        event_id, idempotency_key = stable_execution_event_identity(
            message_id,
            action_ordinal,
        )
        immutable = (
            event_id,
            raw_message_id,
            action_ordinal,
            idempotency_key,
            signal_id,
            parsed_action,
            symbol,
            shares,
            price_micros,
            bid_micros,
            ask_micros,
            recommended_stop_micros,
            user_confirmed_stop_micros,
            stored_event_time,
            stored_message_time,
            compliance_result,
            reconciliation_state,
            details_json,
        )
        existing = _sql(self._connection,
            "SELECT id, event_id, raw_message_id, action_ordinal, idempotency_key, "
            "signal_id, parsed_action, symbol, shares, price_micros, bid_micros, "
            "ask_micros, recommended_stop_micros, user_confirmed_stop_micros, "
            "event_time, message_time, compliance_result, reconciliation_state, "
            "details_json FROM execution_events "
            "WHERE raw_message_id = ? AND action_ordinal = ?",
            (raw_message_id, action_ordinal),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "execution event identity conflicts with stored content"
                )
            return int(existing[0]), True
        try:
            cursor = _sql(self._connection,
                "INSERT INTO execution_events("
                "event_id, raw_message_id, action_ordinal, idempotency_key, signal_id, "
                "parsed_action, symbol, shares, price_micros, bid_micros, ask_micros, "
                "recommended_stop_micros, user_confirmed_stop_micros, event_time, "
                "message_time, compliance_result, reconciliation_state, details_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "execution event identity conflicts with stored content"
            ) from error
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, event_id, raw_message_id, action_ordinal, idempotency_key, "
            "signal_id, parsed_action, symbol, shares, price_micros, bid_micros, "
            "ask_micros, recommended_stop_micros, user_confirmed_stop_micros, "
            "event_time, message_time, compliance_result, reconciliation_state, "
            "details_json FROM execution_events "
            "WHERE raw_message_id = ? AND action_ordinal = ?",
            (raw_message_id, action_ordinal),
        ).fetchone()
        if row is None:
            collision = _sql(self._connection,
                "SELECT 1 FROM execution_events WHERE idempotency_key = ? COLLATE BINARY",
                (idempotency_key,),
            ).fetchone()
            if collision is not None:
                raise IdempotencyConflict(
                    "execution event idempotency identity conflicts with stored content"
                )
            raise JournalError("execution event insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict(
                "execution event identity conflicts with stored content"
            )
        return int(row[0]), not inserted

    def _append_source_observation(
        self,
        *,
        payload: bytes,
        source_uri: str,
        source_type: str,
        provider: str,
        feed: str | None,
        source_time: datetime,
        retrieved_at: datetime,
        provider_sequence: int | None,
        delay_seconds: int | None,
        health_result: str,
        details: Mapping[str, object] | None,
    ) -> tuple[int, bool]:
        if type(payload) is not bytes:
            raise InvalidJournalValue("source payload must be bytes")
        source_uri = _require_nonempty_text(source_uri, "source URI")
        source_type = _require_nonempty_text(source_type, "source type")
        provider = _require_nonempty_text(provider, "provider")
        feed = _optional_text(feed, "feed")
        stored_source_time = _canonical_timestamp(source_time)
        stored_retrieved_at = _canonical_timestamp(retrieved_at)
        provider_sequence = _optional_integer(
            provider_sequence, "provider sequence", minimum=0
        )
        delay_seconds = _optional_integer(delay_seconds, "source delay", minimum=0)
        health_result = _require_nonempty_text(health_result, "health result")
        details_json = _canonical_details(details)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        observation_material = _canonical_json(
            {
                "delay_seconds": delay_seconds,
                "details": json.loads(details_json),
                "feed": feed,
                "health_result": health_result,
                "payload_sha256": payload_sha256,
                "provider": provider,
                "provider_sequence": provider_sequence,
                "retrieved_at": stored_retrieved_at,
                "source_time": stored_source_time,
                "source_type": source_type,
                "source_uri": source_uri,
            }
        )
        observation_sha256 = hashlib.sha256(
            observation_material.encode("utf-8")
        ).hexdigest()
        immutable = (
            observation_sha256,
            payload_sha256,
            source_uri,
            source_type,
            provider,
            feed,
            stored_source_time,
            stored_retrieved_at,
            provider_sequence,
            delay_seconds,
            health_result,
            details_json,
        )
        existing = _sql(self._connection,
            "SELECT id, observation_sha256, payload_sha256, source_uri, source_type, "
            "provider, feed, source_time, retrieved_at, provider_sequence, "
            "delay_seconds, health_result, details_json FROM source_observations "
            "WHERE observation_sha256 = ? COLLATE BINARY",
            (observation_sha256,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "source observation identity conflicts with stored content"
                )
            return int(existing[0]), True
        try:
            cursor = _sql(self._connection,
                "INSERT INTO source_observations("
                "observation_sha256, payload_sha256, source_uri, source_type, provider, "
                "feed, source_time, retrieved_at, provider_sequence, delay_seconds, "
                "health_result, details_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(observation_sha256) DO NOTHING",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "source observation identity conflicts with stored content"
            ) from error
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, observation_sha256, payload_sha256, source_uri, source_type, "
            "provider, feed, source_time, retrieved_at, provider_sequence, "
            "delay_seconds, health_result, details_json FROM source_observations "
            "WHERE observation_sha256 = ? COLLATE BINARY",
            (observation_sha256,),
        ).fetchone()
        if row is None:
            raise JournalError("source observation insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict(
                "source observation identity conflicts with stored content"
            )
        return int(row[0]), not inserted

    def _finalize_report(
        self,
        *,
        claim_id: int,
        claim_token: str,
        body: str,
        state_sha256: str,
        observation_ids: Sequence[int],
        archive_relative_path: str,
        created_at: datetime,
        outbox_destination: str,
        outbox_payload: str,
    ) -> FinalizedReport:
        claim_id = _require_integer(claim_id, "report claim row ID", minimum=1)
        claim_token = _require_nonempty_text(claim_token, "report claim token")
        body = _require_nonempty_text(body, "report body")
        state_sha256 = _require_sha256(state_sha256, "report state hash")
        archive_relative_path = _canonical_archive_path(archive_relative_path)
        stored_created_at = _canonical_timestamp(created_at)
        outbox_destination = _require_nonempty_text(
            outbox_destination, "outbox destination"
        )
        outbox_payload = _require_nonempty_text(outbox_payload, "outbox payload")
        requested_ids = _canonical_integer_set(
            observation_ids, "source observation row IDs"
        )

        claim = _sql(self._connection,
            "SELECT session_date, report_kind, claim_token, status, lease_started_at, "
            "lease_expires_at, report_id, finalized_at, created_at "
            "FROM report_claims WHERE id = ?",
            (claim_id,),
        ).fetchone()
        if claim is None:
            raise InvalidJournalValue("report claim row does not exist")
        session_date, report_kind = str(claim[0]), str(claim[1])
        if str(claim[2]) != claim_token:
            raise IdempotencyConflict("report claim token is stale")
        stored_status = str(claim[3])
        lease_started_at = str(claim[4])
        lease_expires_at = str(claim[5])
        stored_finalized_at = str(claim[7]) if claim[7] is not None else None
        claim_created_at = str(claim[8])
        if stored_status not in {"IN_PROGRESS", "FINALIZED"}:
            raise MigrationCorruption("report claim has an invalid stored status")
        if stored_status == "FINALIZED" and stored_finalized_at is None:
            raise MigrationCorruption("finalized report claim lacks a timestamp")
        if stored_created_at < claim_created_at:
            raise InvalidJournalValue("report creation time cannot predate its claim")

        observations: list[tuple[int, str, str, str]] = []
        if requested_ids:
            placeholders = ", ".join("?" for _ in requested_ids)
            rows = _sql(self._connection,
                "SELECT id, observation_sha256, source_time, retrieved_at "
                "FROM source_observations "
                f"WHERE id IN ({placeholders})",
                requested_ids,
            ).fetchall()
            if len(rows) != len(requested_ids):
                raise InvalidJournalValue("source observation row does not exist")
            observations = sorted(
                (
                    (int(row[0]), str(row[1]), str(row[2]), str(row[3]))
                    for row in rows
                ),
                key=lambda item: (item[1], item[0]),
            )
            if any(
                source_time > stored_created_at
                or retrieved_at > stored_created_at
                for _, _, source_time, retrieved_at in observations
            ):
                raise InvalidJournalValue(
                    "report cannot pin future source evidence"
                )
        observation_set_sha256 = hashlib.sha256(
            _canonical_json([value for _, value, _, _ in observations]).encode(
                "utf-8"
            )
        ).hexdigest()
        content_sha256 = hashlib.sha256(body.encode("utf-8")).hexdigest()
        report_id = stable_report_id(
            report_kind,
            date.fromisoformat(session_date),
            [value for _, value, _, _ in observations],
            state_sha256,
        )
        expected_archive_relative_path = report_archive_relative_path(
            report_kind, date.fromisoformat(session_date), report_id
        )
        if archive_relative_path != expected_archive_relative_path:
            raise InvalidJournalValue(
                "report archive path does not match its stable identity"
            )
        report_immutable = (
            report_id,
            claim_id,
            session_date,
            report_kind,
            body,
            content_sha256,
            state_sha256,
            observation_set_sha256,
            archive_relative_path,
            stored_created_at,
        )
        outbox_key = "report-delivery:" + hashlib.sha256(
            (report_id + "\x00" + outbox_destination).encode("utf-8")
        ).hexdigest()

        if stored_status == "FINALIZED":
            assert stored_finalized_at is not None
            effective_finalized_at = stored_finalized_at
            if stored_created_at > effective_finalized_at:
                raise InvalidJournalValue(
                    "report creation time cannot follow finalization"
                )
            stored_report_id = int(claim[6]) if claim[6] is not None else None
            if stored_report_id is None:
                raise MigrationCorruption("finalized report claim lacks a report row")
            report_row = _sql(self._connection,
                "SELECT id, report_id, claim_id, session_date, report_kind, body_text, "
                "content_sha256, state_sha256, observation_set_sha256, "
                "archive_relative_path, created_at FROM reports WHERE id = ?",
                (stored_report_id,),
            ).fetchone()
            if report_row is None or tuple(report_row[1:]) != report_immutable:
                raise IdempotencyConflict(
                    "report claim conflicts with finalized report content"
                )
            stored_pins = _sql(self._connection,
                "SELECT source_observation_id FROM report_observations "
                "WHERE report_id = ? ORDER BY observation_ordinal",
                (stored_report_id,),
            ).fetchall()
            if tuple(int(row[0]) for row in stored_pins) != tuple(
                row_id for row_id, _, _, _ in observations
            ):
                raise IdempotencyConflict(
                    "report claim conflicts with finalized observation set"
                )
            outbox_id, outbox_duplicate = self._append_outbox(
                idempotency_key=outbox_key,
                origin_report_id=stored_report_id,
                origin_execution_event_id=None,
                destination=outbox_destination,
                payload_text=outbox_payload,
                created_at=_parse_canonical_timestamp(effective_finalized_at),
            )
            if not outbox_duplicate:
                raise MigrationCorruption("finalized report lacked its outbox row")
            return FinalizedReport(
                report_row_id=stored_report_id,
                report_id=report_id,
                outbox_id=outbox_id,
                duplicate=True,
            )

        effective_finalized_at = _canonical_timestamp(_utc_now())
        if stored_created_at > effective_finalized_at:
            raise InvalidJournalValue(
                "report creation time cannot follow finalization"
            )
        if not lease_started_at <= effective_finalized_at < lease_expires_at:
            raise IdempotencyConflict("report claim lease is not active")
        try:
            cursor = _sql(self._connection,
                "INSERT INTO reports("
                "report_id, claim_id, session_date, report_kind, body_text, "
                "content_sha256, state_sha256, observation_set_sha256, "
                "archive_relative_path, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                report_immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "report identity conflicts with stored content"
            ) from error
        if cursor.rowcount != 1:
            raise IdempotencyConflict("report identity conflicts with stored content")
        report_row_id = int(cursor.lastrowid)
        for ordinal, (observation_id, _, _, _) in enumerate(observations):
            _sql(self._connection,
                "INSERT INTO report_observations("
                "report_id, source_observation_id, observation_ordinal"
                ") VALUES (?, ?, ?)",
                (report_row_id, observation_id, ordinal),
            )
        outbox_id, outbox_duplicate = self._append_outbox(
            idempotency_key=outbox_key,
            origin_report_id=report_row_id,
            origin_execution_event_id=None,
            destination=outbox_destination,
            payload_text=outbox_payload,
            created_at=_parse_canonical_timestamp(effective_finalized_at),
        )
        if outbox_duplicate:
            raise MigrationCorruption("new report collided with an existing outbox row")
        with self._report_claim_write():
            update = _sql(self._connection,
                "UPDATE report_claims SET status = 'FINALIZED', finalized_at = ?, "
                "report_id = ? WHERE id = ? AND claim_token = ? COLLATE BINARY "
                "AND status = 'IN_PROGRESS'",
                (effective_finalized_at, report_row_id, claim_id, claim_token),
            )
        if update.rowcount != 1:
            raise IdempotencyConflict("report claim token is stale")
        return FinalizedReport(
            report_row_id=report_row_id,
            report_id=report_id,
            outbox_id=outbox_id,
            duplicate=False,
        )

    def _append_outbox(
        self,
        *,
        idempotency_key: str,
        origin_report_id: int | None,
        origin_execution_event_id: int | None,
        destination: str,
        payload_text: str,
        created_at: datetime,
    ) -> tuple[int, bool]:
        idempotency_key = _require_nonempty_text(
            idempotency_key, "outbox idempotency key"
        )
        origin_report_id = _optional_integer(
            origin_report_id, "origin report row ID", minimum=1
        )
        origin_execution_event_id = _optional_integer(
            origin_execution_event_id, "origin execution event row ID", minimum=1
        )
        if (origin_report_id is None) == (origin_execution_event_id is None):
            raise InvalidJournalValue("outbox row must have exactly one origin")
        destination = _require_nonempty_text(destination, "outbox destination")
        if type(payload_text) is not str:
            raise InvalidJournalValue("outbox payload must be text")
        payload_sha256 = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        stored_created_at = _canonical_timestamp(created_at)
        immutable = (
            idempotency_key,
            origin_report_id,
            origin_execution_event_id,
            destination,
            payload_text,
            payload_sha256,
            stored_created_at,
        )
        existing = _sql(self._connection,
            "SELECT id, idempotency_key, origin_report_id, origin_execution_event_id, "
            "destination, payload_text, payload_sha256, created_at FROM outbox "
            "WHERE idempotency_key = ? COLLATE BINARY",
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "outbox identity conflicts with stored content"
                )
            return int(existing[0]), True
        if origin_report_id is not None:
            origin_delivery = _sql(
                self._connection,
                "SELECT 1 FROM outbox WHERE origin_report_id = ? "
                "AND destination = ?",
                (origin_report_id, destination),
            ).fetchone()
            if origin_delivery is not None:
                raise IdempotencyConflict(
                    "report delivery already exists for this destination"
                )
        else:
            origin_delivery = _sql(
                self._connection,
                "SELECT 1 FROM outbox WHERE origin_execution_event_id = ? "
                "AND destination = ?",
                (origin_execution_event_id, destination),
            ).fetchone()
            if origin_delivery is not None:
                raise IdempotencyConflict(
                    "execution event delivery already exists for this destination"
                )
        try:
            cursor = _sql(self._connection,
                "INSERT INTO outbox("
                "idempotency_key, origin_report_id, origin_execution_event_id, "
                "destination, payload_text, payload_sha256, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(idempotency_key) DO NOTHING",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "outbox identity conflicts with stored journal state"
            ) from error
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, idempotency_key, origin_report_id, origin_execution_event_id, "
            "destination, payload_text, payload_sha256, created_at FROM outbox "
            "WHERE idempotency_key = ? COLLATE BINARY",
            (idempotency_key,),
        ).fetchone()
        if row is None:
            raise JournalError("outbox insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict("outbox identity conflicts with stored content")
        return int(row[0]), not inserted

    def _record_outbox_delivery_attempt(
        self,
        *,
        outbox_id: int,
        attempt_ordinal: int,
        attempted_at: datetime,
        delivery_status: str,
        external_delivery_id: str | None,
        error_class: str | None,
        details: Mapping[str, object] | None,
    ) -> tuple[int, bool]:
        outbox_id = _require_integer(outbox_id, "outbox row ID", minimum=1)
        attempt_ordinal = _require_integer(
            attempt_ordinal, "delivery attempt ordinal", minimum=1
        )
        stored_attempted_at = _canonical_timestamp(attempted_at)
        delivery_status = _require_nonempty_text(
            delivery_status, "delivery status"
        ).upper()
        if delivery_status not in {"FAILED", "DELIVERED"}:
            raise InvalidJournalValue("delivery status is not supported")
        external_delivery_id = _optional_text(
            external_delivery_id, "external delivery ID"
        )
        error_class = _optional_text(error_class, "delivery error class")
        details_json = _canonical_details(details)
        immutable = (
            outbox_id,
            attempt_ordinal,
            stored_attempted_at,
            delivery_status,
            external_delivery_id,
            error_class,
            details_json,
        )
        existing = _sql(self._connection,
            "SELECT id, outbox_id, attempt_ordinal, attempted_at, delivery_status, "
            "external_delivery_id, error_class, details_json "
            "FROM outbox_delivery_attempts WHERE outbox_id = ? AND attempt_ordinal = ?",
            (outbox_id, attempt_ordinal),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "delivery attempt identity conflicts with stored content"
                )
            return int(existing[0]), True
        outbox = _sql(
            self._connection,
            "SELECT created_at FROM outbox WHERE id = ?",
            (outbox_id,),
        ).fetchone()
        if outbox is None:
            raise InvalidJournalValue("outbox row does not exist")
        prior = _sql(
            self._connection,
            "SELECT attempt_ordinal, attempted_at, delivery_status "
            "FROM outbox_delivery_attempts WHERE outbox_id = ? "
            "ORDER BY attempt_ordinal DESC LIMIT 1",
            (outbox_id,),
        ).fetchone()
        if prior is not None and str(prior[2]) == "DELIVERED":
            raise IdempotencyConflict("outbox delivery is already terminal")
        expected_ordinal = 1 if prior is None else int(prior[0]) + 1
        if attempt_ordinal != expected_ordinal:
            raise InvalidJournalValue("delivery attempt ordinal is not contiguous")
        prior_attempted_at = str(outbox[0]) if prior is None else str(prior[1])
        if stored_attempted_at < prior_attempted_at:
            raise InvalidJournalValue("delivery attempt time is out of order")
        if delivery_status == "DELIVERED":
            if external_delivery_id is None or error_class is not None:
                raise InvalidJournalValue(
                    "delivered attempt requires only an external delivery ID"
                )
        elif external_delivery_id is not None or error_class is None:
            raise InvalidJournalValue(
                "failed attempt requires only a delivery error class"
            )
        try:
            cursor = _sql(self._connection,
                "INSERT INTO outbox_delivery_attempts("
                "outbox_id, attempt_ordinal, attempted_at, delivery_status, "
                "external_delivery_id, error_class, details_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "delivery attempt conflicts with stored journal state"
            ) from error
        return int(cursor.lastrowid), False

    def _start_scheduled_run(
        self,
        *,
        run_key: str,
        run_kind: str,
        session_date: date,
        intended_run_at: datetime,
        started_at: datetime,
    ) -> tuple[int, bool]:
        run_key = _require_nonempty_text(run_key, "scheduled run key")
        run_kind = _canonical_token(run_kind, "scheduled run kind")
        stored_date = _canonical_date(session_date)
        stored_intended_at = _canonical_timestamp(intended_run_at)
        stored_started_at = _canonical_timestamp(started_at)
        immutable = (
            run_key,
            run_kind,
            stored_date,
            stored_intended_at,
            stored_started_at,
        )
        existing = _sql(self._connection,
            "SELECT id, run_key, run_kind, session_date, intended_run_at, started_at "
            "FROM scheduled_runs WHERE run_key = ? COLLATE BINARY",
            (run_key,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "scheduled run identity conflicts with stored content"
                )
            return int(existing[0]), True
        cursor = _sql(self._connection,
            "INSERT INTO scheduled_runs("
            "run_key, run_kind, session_date, intended_run_at, started_at"
            ") VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_key) DO NOTHING",
            immutable,
        )
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, run_key, run_kind, session_date, intended_run_at, started_at "
            "FROM scheduled_runs WHERE run_key = ? COLLATE BINARY",
            (run_key,),
        ).fetchone()
        if row is None:
            raise JournalError("scheduled run start could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict(
                "scheduled run identity conflicts with stored content"
            )
        return int(row[0]), not inserted

    def _complete_scheduled_run(
        self,
        *,
        run_id: int,
        finished_at: datetime,
        market_session_decision: str,
        outcome: str,
        report_id: int | None,
        report_path: str | None,
        error_class: str | None,
    ) -> tuple[int, bool]:
        run_id = _require_integer(run_id, "scheduled run row ID", minimum=1)
        stored_finished_at = _canonical_timestamp(finished_at)
        market_session_decision = _canonical_token(
            market_session_decision, "market-session decision"
        )
        outcome = _canonical_token(outcome, "scheduled run outcome")
        report_id = _optional_integer(report_id, "report row ID", minimum=1)
        if report_path is not None:
            report_path = _canonical_archive_path(report_path)
        error_class = _optional_text(error_class, "scheduled run error class")
        completion = (
            stored_finished_at,
            market_session_decision,
            report_id,
            report_path,
            outcome,
            error_class,
        )
        row = _sql(self._connection,
            "SELECT run_kind, session_date, started_at, finished_at, "
            "market_session_decision, report_id, report_path, outcome, error_class "
            "FROM scheduled_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise InvalidJournalValue("scheduled run row does not exist")
        if stored_finished_at < str(row[2]):
            raise InvalidJournalValue("scheduled run cannot finish before it starts")
        if row[3] is not None:
            if tuple(row[3:]) != completion:
                raise IdempotencyConflict(
                    "scheduled run completion conflicts with stored content"
                )
            return run_id, True
        if (report_id is None) != (report_path is None):
            raise InvalidJournalValue(
                "scheduled report row and archive path must be supplied together"
            )
        if (outcome == "REPORT_EMITTED") != (report_id is not None):
            raise InvalidJournalValue(
                "scheduled report outcome conflicts with its report identity"
            )
        if report_id is not None:
            report = _sql(self._connection,
                "SELECT report.session_date, report.report_kind, "
                "report.archive_relative_path, claim.status, claim.report_id, "
                "claim.finalized_at FROM reports AS report "
                "JOIN report_claims AS claim ON claim.id = report.claim_id "
                "WHERE report.id = ?",
                (report_id,),
            ).fetchone()
            if report is None:
                raise InvalidJournalValue("report row does not exist")
            if tuple(str(value) for value in report[:3]) != (
                str(row[1]),
                str(row[0]),
                report_path,
            ):
                raise InvalidJournalValue(
                    "scheduled report does not match the run identity and path"
                )
            if report[3] != "FINALIZED" or report[4] != report_id:
                raise InvalidJournalValue("scheduled report is not finalized")
            report_finalized_at = report[5]
            if (
                report_finalized_at is None
                or str(report_finalized_at) < str(row[2])
                or str(report_finalized_at) > stored_finished_at
            ):
                raise InvalidJournalValue(
                    "scheduled report finalization falls outside the run interval"
                )
        cursor = _sql(self._connection,
            "UPDATE scheduled_runs SET finished_at = ?, market_session_decision = ?, "
            "report_id = ?, report_path = ?, outcome = ?, error_class = ? "
            "WHERE id = ? AND finished_at IS NULL",
            (*completion, run_id),
        )
        if cursor.rowcount != 1:
            raise IdempotencyConflict("scheduled run was completed concurrently")
        return run_id, False

    def _append_ledger_posting(
        self,
        *,
        posting_key: str,
        ledger_name: str,
        account_name: str,
        entry_kind: str,
        occurred_at: datetime,
        amount_micros: int,
        execution_event_id: int | None,
        account_check_id: int | None,
        symbol: str | None,
        shares_delta: int | None,
        unit_price_micros: int | None,
        details: Mapping[str, object] | None,
    ) -> tuple[int, bool]:
        posting_key = _require_nonempty_text(posting_key, "ledger posting key")
        ledger_name = _require_nonempty_text(ledger_name, "ledger name")
        account_name = _require_nonempty_text(account_name, "ledger account name")
        entry_kind = _require_nonempty_text(entry_kind, "ledger entry kind")
        execution_event_id = _optional_integer(
            execution_event_id, "execution event row ID", minimum=1
        )
        account_check_id = _optional_integer(
            account_check_id, "account check row ID", minimum=1
        )
        symbol = _optional_text(symbol, "symbol")
        if symbol is not None:
            symbol = symbol.upper()
        amount_micros = _require_signed_integer(amount_micros, "posting amount")
        shares_delta = _optional_signed_integer(shares_delta, "posting shares")
        unit_price_micros = _optional_integer(
            unit_price_micros, "posting unit price", minimum=1
        )
        stored_occurred_at = _canonical_timestamp(occurred_at)
        details_json = _canonical_details(details)
        if ledger_name == "ACTUAL":
            if (execution_event_id is None) == (account_check_id is None):
                raise InvalidJournalValue(
                    "ACTUAL ledger posting requires exactly one authoritative origin"
                )
            if execution_event_id is not None:
                origin = _sql(
                    self._connection,
                    "SELECT parsed_action, symbol, event_time "
                    "FROM execution_events WHERE id = ?",
                    (execution_event_id,),
                ).fetchone()
                if origin is not None and str(origin[0]) not in _ACTUAL_LEDGER_EVENT_ACTIONS:
                    raise InvalidJournalValue(
                        "ACTUAL ledger posting requires a cash-mutating event origin"
                    )
                origin_symbol_index = 1
                origin_time_index = 2
            else:
                origin = _sql(
                    self._connection,
                    "SELECT event.symbol, account.confirmed_at "
                    "FROM account_checks AS account "
                    "JOIN execution_events AS event "
                    "ON event.id = account.execution_event_id "
                    "WHERE account.id = ?",
                    (account_check_id,),
                ).fetchone()
                origin_symbol_index = 0
                origin_time_index = 1
            if origin is None:
                raise InvalidJournalValue(
                    "ACTUAL ledger posting origin does not exist"
                )
            origin_symbol = (
                str(origin[origin_symbol_index])
                if origin[origin_symbol_index] is not None
                else None
            )
            if symbol != origin_symbol:
                raise InvalidJournalValue(
                    "ACTUAL ledger posting symbol conflicts with its origin"
                )
            if stored_occurred_at < str(origin[origin_time_index]):
                raise InvalidJournalValue(
                    "ACTUAL ledger posting cannot predate its origin"
                )
        immutable = (
            posting_key,
            ledger_name,
            account_name,
            entry_kind,
            execution_event_id,
            account_check_id,
            symbol,
            amount_micros,
            shares_delta,
            unit_price_micros,
            stored_occurred_at,
            details_json,
        )
        existing = _sql(self._connection,
            "SELECT id, posting_key, ledger_name, account_name, entry_kind, "
            "execution_event_id, account_check_id, symbol, amount_micros, shares_delta, "
            "unit_price_micros, occurred_at, details_json FROM ledger_postings "
            "WHERE posting_key = ? COLLATE BINARY",
            (posting_key,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "ledger posting identity conflicts with stored content"
                )
            return int(existing[0]), True
        try:
            cursor = _sql(self._connection,
                "INSERT INTO ledger_postings("
                "posting_key, ledger_name, account_name, entry_kind, "
                "execution_event_id, account_check_id, symbol, amount_micros, "
                "shares_delta, unit_price_micros, occurred_at, details_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(posting_key) DO NOTHING",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise InvalidJournalValue(
                "ledger posting origin does not exist"
            ) from error
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, posting_key, ledger_name, account_name, entry_kind, "
            "execution_event_id, account_check_id, symbol, amount_micros, shares_delta, "
            "unit_price_micros, occurred_at, details_json FROM ledger_postings "
            "WHERE posting_key = ? COLLATE BINARY",
            (posting_key,),
        ).fetchone()
        if row is None:
            raise JournalError("ledger posting insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict(
                "ledger posting identity conflicts with stored content"
            )
        return int(row[0]), not inserted

    def _write_actual_position(
        self,
        *,
        signal_id: str,
        symbol: str,
        shares: int,
        cost_basis_micros: int,
        recommended_stop_micros: int | None,
        user_confirmed_stop_micros: int | None,
        target_micros: int | None,
        last_execution_event_id: int,
        updated_at: datetime,
    ) -> int:
        signal_id = _require_nonempty_text(signal_id, "signal ID")
        symbol = _require_nonempty_text(symbol, "symbol").upper()
        shares = _require_integer(shares, "position shares", minimum=0)
        cost_basis_micros = _require_integer(
            cost_basis_micros, "position cost basis", minimum=0
        )
        recommended_stop_micros = _optional_integer(
            recommended_stop_micros, "recommended stop", minimum=1
        )
        user_confirmed_stop_micros = _optional_integer(
            user_confirmed_stop_micros, "user-confirmed stop", minimum=1
        )
        target_micros = _optional_integer(target_micros, "position target", minimum=1)
        if shares == 0 and (
            cost_basis_micros != 0
            or recommended_stop_micros is not None
            or user_confirmed_stop_micros is not None
            or target_micros is not None
        ):
            raise InvalidJournalValue(
                "a closed position must clear cost basis, stops, and target"
            )
        last_execution_event_id = _require_integer(
            last_execution_event_id, "execution event row ID", minimum=1
        )
        event = _sql(
            self._connection,
            "SELECT signal_id, symbol, parsed_action, event_time "
            "FROM execution_events WHERE id = ?",
            (last_execution_event_id,),
        ).fetchone()
        if event is None:
            raise InvalidJournalValue("execution event row does not exist")
        if (
            event[0] != signal_id
            or event[1] != symbol
            or str(event[2]) not in _POSITION_MUTATING_ACTIONS
        ):
            raise InvalidJournalValue(
                "position projection requires its matching position event"
            )
        stored_updated_at = _canonical_timestamp(updated_at)
        event_time = str(event[3])
        if event_time > stored_updated_at:
            raise InvalidJournalValue(
                "position projection cannot predate its execution event"
            )
        desired = (
            symbol,
            shares,
            cost_basis_micros,
            recommended_stop_micros,
            user_confirmed_stop_micros,
            target_micros,
            last_execution_event_id,
            stored_updated_at,
        )
        row = _sql(self._connection,
            "SELECT symbol, shares, cost_basis_micros, recommended_stop_micros, "
            "user_confirmed_stop_micros, target_micros, last_execution_event_id, "
            "updated_at, revision, (SELECT event_time FROM execution_events "
            "WHERE id = actual_positions.last_execution_event_id) "
            "FROM actual_positions WHERE signal_id = ? COLLATE BINARY",
            (signal_id,),
        ).fetchone()
        with self._projection_write():
            if row is None:
                _sql(self._connection,
                    "INSERT INTO actual_positions("
                    "signal_id, symbol, shares, cost_basis_micros, recommended_stop_micros, "
                    "user_confirmed_stop_micros, target_micros, "
                    "last_execution_event_id, updated_at, revision"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (signal_id, *desired),
                )
                return 1
            if tuple(row[:8]) == desired:
                return int(row[8])
            if row[0] != symbol:
                raise IdempotencyConflict(
                    "position signal identity conflicts with its stored symbol"
                )
            if last_execution_event_id <= int(row[6]):
                raise IdempotencyConflict(
                    "position projection conflicts with its event identity"
                )
            if row[9] is None:
                raise MigrationCorruption(
                    "position projection source lineage is inconsistent"
                )
            if event_time < str(row[9]) or stored_updated_at < str(row[7]):
                raise IdempotencyConflict(
                    "position projection chronology would move backward"
                )
            revision = int(row[8]) + 1
            _sql(self._connection,
                "UPDATE actual_positions SET shares = ?, cost_basis_micros = ?, "
                "recommended_stop_micros = ?, user_confirmed_stop_micros = ?, "
                "target_micros = ?, last_execution_event_id = ?, updated_at = ?, "
                "revision = ? WHERE signal_id = ? COLLATE BINARY",
                (*desired[1:], revision, signal_id),
            )
            return revision

    def _write_actual_cash_projection(
        self,
        *,
        estimated_settled_cash_micros: int,
        user_confirmed_settled_cash_micros: int | None,
        deployed_capital_micros: int,
        open_planned_risk_micros: int,
        consecutive_losses: int,
        weekly_high_water_micros: int,
        monthly_high_water_micros: int,
        last_ledger_posting_id: int,
        updated_at: datetime,
    ) -> int:
        estimated_settled_cash_micros = _require_integer(
            estimated_settled_cash_micros, "estimated settled cash", minimum=0
        )
        user_confirmed_settled_cash_micros = _optional_integer(
            user_confirmed_settled_cash_micros,
            "user-confirmed settled cash",
            minimum=0,
        )
        deployed_capital_micros = _require_integer(
            deployed_capital_micros, "deployed capital", minimum=0
        )
        open_planned_risk_micros = _require_integer(
            open_planned_risk_micros, "open planned risk", minimum=0
        )
        consecutive_losses = _require_integer(
            consecutive_losses, "consecutive losses", minimum=0
        )
        weekly_high_water_micros = _require_integer(
            weekly_high_water_micros, "weekly high water", minimum=0
        )
        monthly_high_water_micros = _require_integer(
            monthly_high_water_micros, "monthly high water", minimum=0
        )
        last_ledger_posting_id = _require_integer(
            last_ledger_posting_id, "ledger posting row ID", minimum=1
        )
        stored_updated_at = _canonical_timestamp(updated_at)
        posting = _sql(
            self._connection,
            "SELECT ledger_name, occurred_at FROM ledger_postings WHERE id = ?",
            (last_ledger_posting_id,),
        ).fetchone()
        if posting is None or str(posting[0]) != "ACTUAL":
            raise InvalidJournalValue(
                "actual cash projection requires an ACTUAL ledger posting"
            )
        if str(posting[1]) > stored_updated_at:
            raise InvalidJournalValue(
                "actual cash projection cannot predate its ledger posting"
            )
        desired = (
            estimated_settled_cash_micros,
            user_confirmed_settled_cash_micros,
            deployed_capital_micros,
            open_planned_risk_micros,
            consecutive_losses,
            weekly_high_water_micros,
            monthly_high_water_micros,
            last_ledger_posting_id,
            stored_updated_at,
        )
        row = _sql(self._connection,
            "SELECT estimated_settled_cash_micros, "
            "user_confirmed_settled_cash_micros, deployed_capital_micros, "
            "open_planned_risk_micros, consecutive_losses, weekly_high_water_micros, "
            "monthly_high_water_micros, last_ledger_posting_id, updated_at, revision, "
            "(SELECT occurred_at FROM ledger_postings "
            "WHERE id = actual_cash_projection.last_ledger_posting_id) "
            "FROM actual_cash_projection WHERE id = 1"
        ).fetchone()
        with self._projection_write():
            if row is None:
                _sql(self._connection,
                    "INSERT INTO actual_cash_projection VALUES "
                    "(1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    desired,
                )
                return 1
            if tuple(row[:9]) == desired:
                return int(row[9])
            if last_ledger_posting_id <= int(row[7]):
                raise IdempotencyConflict(
                    "cash projection conflicts with its posting identity"
                )
            if row[10] is None:
                raise MigrationCorruption(
                    "cash projection source lineage is inconsistent"
                )
            if str(posting[1]) < str(row[10]) or stored_updated_at < str(row[8]):
                raise IdempotencyConflict(
                    "cash projection chronology would move backward"
                )
            revision = int(row[9]) + 1
            _sql(self._connection,
                "UPDATE actual_cash_projection SET "
                "estimated_settled_cash_micros = ?, "
                "user_confirmed_settled_cash_micros = ?, deployed_capital_micros = ?, "
                "open_planned_risk_micros = ?, consecutive_losses = ?, "
                "weekly_high_water_micros = ?, monthly_high_water_micros = ?, "
                "last_ledger_posting_id = ?, updated_at = ?, revision = ? WHERE id = 1",
                (*desired, revision),
            )
            return revision

    def _write_reconciliation_projection(
        self,
        *,
        reconciliation_required: bool,
        reason: str | None,
        last_execution_event_id: int | None,
        updated_at: datetime,
    ) -> int:
        if type(reconciliation_required) is not bool:
            raise InvalidJournalValue("reconciliation flag must be a boolean")
        reason = _optional_text(reason, "reconciliation reason")
        if reconciliation_required and reason is None:
            raise InvalidJournalValue(
                "reconciliation reason is required when reconciliation is active"
            )
        if not reconciliation_required and reason is not None:
            raise InvalidJournalValue(
                "reconciliation reason must be absent when reconciliation is clear"
            )
        if last_execution_event_id is None:
            raise InvalidJournalValue(
                "reconciliation projection requires an execution event"
            )
        last_execution_event_id = _require_integer(
            last_execution_event_id, "execution event row ID", minimum=1
        )
        event = _sql(
            self._connection,
            "SELECT parsed_action, reconciliation_state, event_time "
            "FROM execution_events WHERE id = ?",
            (last_execution_event_id,),
        ).fetchone()
        required_states = {"REQUIRED", "PENDING"}
        if (
            event is None
            or str(event[0]) not in _RECONCILIATION_ACTIONS
            or (
                reconciliation_required
                and str(event[1]) not in required_states
            )
            or (not reconciliation_required and str(event[1]) != "CLEAR")
        ):
            raise InvalidJournalValue(
                "reconciliation projection requires an authoritative matching event"
            )
        stored_updated_at = _canonical_timestamp(updated_at)
        event_time = str(event[2])
        if event_time > stored_updated_at:
            raise InvalidJournalValue(
                "reconciliation projection cannot predate its execution event"
            )
        desired = (
            int(reconciliation_required),
            reason,
            last_execution_event_id,
            stored_updated_at,
        )
        row = _sql(self._connection,
            "SELECT reconciliation_required, reason, last_execution_event_id, "
            "updated_at, revision, (SELECT event_time FROM execution_events "
            "WHERE id = reconciliation_projection.last_execution_event_id) "
            "FROM reconciliation_projection WHERE id = 1"
        ).fetchone()
        with self._projection_write():
            if row is None:
                _sql(self._connection,
                    "INSERT INTO reconciliation_projection VALUES (1, ?, ?, ?, ?, 1)",
                    desired,
                )
                return 1
            if tuple(row[:4]) == desired:
                return int(row[4])
            stored_event_id = int(row[2])
            if last_execution_event_id <= stored_event_id:
                raise IdempotencyConflict(
                    "reconciliation projection conflicts with its event identity"
                )
            if row[5] is None:
                raise MigrationCorruption(
                    "reconciliation projection source lineage is inconsistent"
                )
            if event_time < str(row[5]) or stored_updated_at < str(row[3]):
                raise IdempotencyConflict(
                    "reconciliation projection chronology would move backward"
                )
            revision = int(row[4]) + 1
            _sql(self._connection,
                "UPDATE reconciliation_projection SET reconciliation_required = ?, "
                "reason = ?, last_execution_event_id = ?, updated_at = ?, revision = ? "
                "WHERE id = 1",
                (*desired, revision),
            )
            return revision

    def _append_account_check(
        self,
        *,
        execution_event_id: int,
        settled_cash_micros: int,
        pending_order_count: int,
        unlogged_position_count: int,
        confirmed_at: datetime,
        reconciliation_result: str,
        details: Mapping[str, object] | None,
    ) -> tuple[int, bool]:
        execution_event_id = _require_integer(
            execution_event_id, "execution event row ID", minimum=1
        )
        settled_cash_micros = _require_integer(
            settled_cash_micros, "settled cash", minimum=0
        )
        pending_order_count = _require_integer(
            pending_order_count, "pending-order count", minimum=0
        )
        unlogged_position_count = _require_integer(
            unlogged_position_count, "unlogged-position count", minimum=0
        )
        stored_confirmed_at = _canonical_timestamp(confirmed_at)
        reconciliation_result = _canonical_token(
            reconciliation_result, "account-check reconciliation result"
        )
        if reconciliation_result not in {"CLEAR", "RECONCILIATION_REQUIRED"}:
            raise InvalidJournalValue(
                "account-check reconciliation result is not supported"
            )
        details_json = _canonical_details(details)
        event = _sql(self._connection,
            "SELECT event_id, raw_message_id, parsed_action, event_time, "
            "reconciliation_state "
            "FROM execution_events "
            "WHERE id = ?",
            (execution_event_id,),
        ).fetchone()
        if event is None:
            raise InvalidJournalValue("execution event row does not exist")
        if str(event[2]) != "ACCOUNT_CHECK":
            raise InvalidJournalValue(
                "account check must reference an ACCOUNT_CHECK execution event"
            )
        if str(event[3]) != stored_confirmed_at:
            raise InvalidJournalValue(
                "account check time must match its execution event"
            )
        raw_message_id = int(event[1])
        check_id = "chk_" + hashlib.sha256(
            ("stock-monitor/account-check/v1\x00" + str(event[0])).encode("utf-8")
        ).hexdigest()
        immutable = (
            check_id,
            raw_message_id,
            execution_event_id,
            settled_cash_micros,
            pending_order_count,
            unlogged_position_count,
            stored_confirmed_at,
            reconciliation_result,
            details_json,
        )
        existing = _sql(self._connection,
            "SELECT id, check_id, raw_message_id, execution_event_id, "
            "settled_cash_micros, pending_order_count, unlogged_position_count, "
            "confirmed_at, reconciliation_result, details_json FROM account_checks "
            "WHERE execution_event_id = ?",
            (execution_event_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[1:]) != immutable:
                raise IdempotencyConflict(
                    "account-check identity conflicts with stored content"
                )
            return int(existing[0]), True
        if reconciliation_result == "CLEAR" and (
            pending_order_count != 0 or unlogged_position_count != 0
        ):
            raise InvalidJournalValue(
                "a clear account check cannot contain unreconciled exposure"
            )
        expected_states = (
            {"CLEAR"}
            if reconciliation_result == "CLEAR"
            else {"REQUIRED", "PENDING"}
        )
        if str(event[4]) not in expected_states:
            raise InvalidJournalValue(
                "account check result conflicts with its execution event state"
            )
        try:
            cursor = _sql(self._connection,
                "INSERT INTO account_checks("
                "check_id, raw_message_id, execution_event_id, settled_cash_micros, "
                "pending_order_count, unlogged_position_count, confirmed_at, "
                "reconciliation_result, details_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                immutable,
            )
        except sqlite3.IntegrityError as error:
            raise IdempotencyConflict(
                "account-check identity conflicts with stored content"
            ) from error
        inserted = cursor.rowcount == 1
        row = _sql(self._connection,
            "SELECT id, check_id, raw_message_id, execution_event_id, "
            "settled_cash_micros, pending_order_count, unlogged_position_count, "
            "confirmed_at, reconciliation_result, details_json FROM account_checks "
            "WHERE execution_event_id = ?",
            (execution_event_id,),
        ).fetchone()
        if row is None:
            collision = _sql(self._connection,
                "SELECT 1 FROM account_checks WHERE check_id = ? COLLATE BINARY",
                (check_id,),
            ).fetchone()
            if collision is not None:
                raise IdempotencyConflict(
                    "account-check identity conflicts with stored content"
                )
            raise JournalError("account check insert could not be verified")
        if tuple(row[1:]) != immutable:
            raise IdempotencyConflict(
                "account-check identity conflicts with stored content"
            )
        return int(row[0]), not inserted

    @contextmanager
    def _projection_write(self) -> Iterator[None]:
        if not self._transaction_active or self._projection_write_allowed:
            raise JournalError("projection writes require an active journal transaction")
        self._transaction_dirty = True
        self._projection_write_allowed = True
        try:
            yield
        finally:
            self._projection_write_allowed = False

    @contextmanager
    def _report_claim_write(self) -> Iterator[None]:
        if not self._transaction_active or self._report_claim_write_allowed:
            raise JournalError("report claim writes require an active journal transaction")
        self._transaction_dirty = True
        self._report_claim_write_allowed = True
        try:
            yield
        finally:
            self._report_claim_write_allowed = False

    @contextmanager
    def _immediate_connection(self) -> Iterator[sqlite3.Connection]:
        if self._transaction_active:
            raise JournalError("nested journal transactions are not supported")
        changes_before = self._connection.total_changes
        try:
            _sql(self._connection, "BEGIN IMMEDIATE")
        except sqlite3.Error as error:
            raise _translate_sqlite_error(error) from error
        self._transaction_active = True
        self._transaction_dirty = False
        try:
            yield self._connection
        except BaseException:
            try:
                self._connection.rollback()
            finally:
                self._transaction_dirty = False
                self._transaction_active = False
            raise
        else:
            try:
                self._connection.commit()
            except sqlite3.Error as error:
                try:
                    self._connection.rollback()
                finally:
                    self._transaction_dirty = False
                    self._transaction_active = False
                raise _translate_sqlite_error(error) from error
            self._transaction_dirty = False
            self._transaction_active = False
            if self._connection.total_changes != changes_before:
                self._source_generation += 1

    def _configure_connection(self) -> None:
        try:
            self._connection.create_function(
                "journal_projection_write_allowed",
                0,
                lambda: int(self._projection_write_allowed),
            )
            self._connection.create_function(
                "journal_report_claim_write_allowed",
                0,
                lambda: int(self._report_claim_write_allowed),
            )
            _sql(self._connection,
                f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MILLISECONDS}"
            )
            _sql(self._connection, "PRAGMA foreign_keys = ON")
            _sql(self._connection, "PRAGMA recursive_triggers = ON")
            _enable_wal_with_bounded_retry(self._connection)
            _sql(self._connection, "PRAGMA synchronous = FULL")
            if _pragma_int(self._connection, "foreign_keys") != 1:
                raise JournalError("journal foreign keys could not be enabled")
            if _pragma_int(self._connection, "recursive_triggers") != 1:
                raise JournalError("journal recursive triggers could not be enabled")
            if _pragma_int(self._connection, "busy_timeout") != BUSY_TIMEOUT_MILLISECONDS:
                raise JournalError("journal busy timeout is inconsistent")
            if _pragma_int(self._connection, "synchronous") != 2:
                raise JournalError("journal synchronous mode is inconsistent")
        except sqlite3.Error as error:
            raise _translate_sqlite_error(error) from error

    def _verify_database_ownership(self, connection: sqlite3.Connection) -> None:
        try:
            application_id = _pragma_int(connection, "application_id")
            user_version = _pragma_int(connection, "user_version")
            objects = _sql(connection,
                "SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' "
                "AND type IN ('table', 'view', 'trigger')"
            ).fetchall()
            if application_id == 0:
                if user_version != 0 or objects:
                    raise MigrationCorruption(
                        "database is not owned by Stock Monitor"
                    )
                return
            if application_id != APPLICATION_ID:
                raise MigrationCorruption(
                    "database application identifier is inconsistent"
                )
            if not _table_exists(connection, "schema_migrations"):
                raise MigrationCorruption("owned database lacks migration metadata")
        except MigrationCorruption:
            raise
        except sqlite3.Error as error:
            if _is_busy_error(error):
                raise JournalBusy("journal is busy") from error
            raise MigrationCorruption(
                "database ownership could not be verified"
            ) from error

    def _verify_database_ownership_snapshot(
        self, connection: sqlite3.Connection
    ) -> None:
        try:
            _sql(connection, "BEGIN")
        except sqlite3.Error as error:
            if _is_busy_error(error):
                raise JournalBusy("journal is busy") from error
            raise MigrationCorruption(
                "database ownership could not be verified"
            ) from error
        try:
            self._verify_database_ownership(connection)
        except BaseException:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            raise
        try:
            connection.rollback()
        except sqlite3.Error as error:
            raise MigrationCorruption(
                "database ownership could not be verified"
            ) from error

    @staticmethod
    def _read_applied_migrations(
        connection: sqlite3.Connection,
    ) -> tuple[_AppliedMigration, ...]:
        if not _table_exists(connection, "schema_migrations"):
            return ()
        try:
            rows = _sql(connection,
                "SELECT version, name, sha256, schema_sha256, applied_at "
                "FROM schema_migrations ORDER BY version"
            ).fetchall()
            applied: list[_AppliedMigration] = []
            for row in rows:
                if (
                    len(row) != 5
                    or type(row[0]) is not int
                    or row[0] <= 0
                    or not isinstance(row[1], str)
                    or not isinstance(row[2], str)
                    or not isinstance(row[3], str)
                    or not isinstance(row[4], str)
                ):
                    raise MigrationCorruption("migration metadata is malformed")
                applied.append((row[0], row[1], row[2], row[3], row[4]))
        except sqlite3.Error as error:
            raise MigrationCorruption("migration metadata could not be read") from error
        return tuple(applied)

    def _ensure_open(self) -> None:
        if self._closed:
            raise JournalError("journal is closed")

    def _source_authority_data_version(self) -> int:
        self._ensure_open()
        row = _sql(self._connection, "PRAGMA data_version").fetchone()
        if row is None or type(row[0]) is not int:
            raise JournalError("journal data version is unavailable")
        return int(row[0])


def _load_migrations(migration_directory: Path | None) -> tuple[_Migration, ...]:
    try:
        root = (
            migration_directory
            if migration_directory is not None
            else importlib.resources.files("stock_monitor.sql")
        )
        entries = [
            entry
            for entry in root.iterdir()
            if entry.is_file() and entry.name.endswith(".sql")
        ]
    except (ImportError, ModuleNotFoundError, OSError) as error:
        raise MigrationCorruption("migration resources could not be read") from error
    migrations: list[_Migration] = []
    versions: set[int] = set()
    for entry in entries:
        match = _MIGRATION_NAME.fullmatch(entry.name)
        if match is None:
            raise MigrationCorruption("packaged migration name is malformed")
        version = int(match.group("version"))
        if version in versions:
            raise MigrationCorruption("packaged migration version is duplicated")
        versions.add(version)
        try:
            sql = entry.read_bytes()
        except OSError as error:
            raise MigrationCorruption("migration resources could not be read") from error
        migrations.append(
            _Migration(version, entry.name, sql, hashlib.sha256(sql).hexdigest())
        )
    migrations.sort(key=lambda migration: migration.version)
    if not migrations:
        raise MigrationCorruption("no packaged migrations were found")
    expected = list(range(1, len(migrations) + 1))
    if [migration.version for migration in migrations] != expected:
        raise MigrationCorruption("packaged migration versions are not contiguous")
    return tuple(migrations)


def _verify_applied_migrations(
    applied: tuple[_AppliedMigration, ...],
    packaged: tuple[_Migration, ...],
) -> None:
    if len(applied) > len(packaged):
        raise MigrationCorruption("database migration version is newer than this package")
    for expected_version, row in enumerate(applied, start=1):
        version, name, sha256, schema_sha256, applied_at = row
        if version != expected_version:
            raise MigrationCorruption("applied migration versions are not contiguous")
        migration = packaged[expected_version - 1]
        if name != migration.name or sha256 != migration.sha256:
            raise MigrationDrift("applied migration differs from packaged migration")
        if len(schema_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in schema_sha256
        ):
            raise MigrationCorruption("applied migration schema hash is malformed")
        if (
            len(applied_at) != 27
            or _canonical_timestamp(_parse_canonical_timestamp(applied_at))
            != applied_at
        ):
            raise MigrationCorruption("applied migration timestamp is malformed")


def _derive_expected_schema_sha256s(
    migrations: tuple[_Migration, ...],
) -> tuple[str, ...]:
    try:
        connection = sqlite3.connect(":memory:", isolation_level=None)
    except sqlite3.Error as error:
        raise MigrationCorruption(
            "packaged migration schema could not be verified"
        ) from error
    try:
        connection.create_function(
            "journal_projection_write_allowed", 0, lambda: 0
        )
        _sql(connection, "PRAGMA foreign_keys = ON")
        _sql(connection, "PRAGMA recursive_triggers = ON")
        _sql(connection, "BEGIN IMMEDIATE")
        expected: list[str] = []
        recorded: list[_AppliedMigration] = []
        for migration in migrations:
            _require_migration_bookkeeping(connection, tuple(recorded))
            _execute_migration(connection, migration.sql)
            _require_migration_bookkeeping(connection, tuple(recorded))
            schema_sha256 = _schema_sha256(connection)
            expected.append(schema_sha256)
            try:
                _sql(
                    connection,
                    "INSERT INTO schema_migrations("
                    "version, name, sha256, schema_sha256, applied_at"
                    ") VALUES (?, ?, ?, ?, ?)",
                    (
                        migration.version,
                        migration.name,
                        migration.sha256,
                        schema_sha256,
                        "2000-01-01T00:00:00.000000Z",
                    ),
                )
                _sql(connection, f"PRAGMA user_version = {migration.version}")
            except sqlite3.Error as error:
                raise MigrationCorruption(
                    "packaged migration metadata is incompatible"
                ) from error
            recorded.append(
                (
                    migration.version,
                    migration.name,
                    migration.sha256,
                    schema_sha256,
                    "2000-01-01T00:00:00.000000Z",
                )
            )
            _require_migration_bookkeeping(connection, tuple(recorded))
        return tuple(expected)
    except (KeyboardInterrupt, SystemExit, MemoryError):
        raise
    except BaseException as error:
        raise MigrationCorruption(
            "packaged migration schema could not be verified"
        ) from error
    finally:
        try:
            if connection.in_transaction:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
        finally:
            try:
                connection.close()
            except sqlite3.Error:
                pass


def _execute_migration(connection: sqlite3.Connection, sql_bytes: bytes) -> None:
    statements = _migration_statements(sql_bytes)
    for statement in statements:
        _validate_migration_statement(statement)
    try:
        for statement in statements:
            _sql(connection, statement)
            if not connection.in_transaction:
                raise MigrationCorruption(
                    "migration escaped its transaction boundary"
                )
    except sqlite3.Error as error:
        if _is_busy_error(error):
            raise JournalBusy("journal is busy") from error
        raise MigrationCorruption("migration SQL could not be applied") from error


def _migration_statements(sql_bytes: bytes) -> tuple[str, ...]:
    try:
        sql = sql_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MigrationCorruption("migration is not valid UTF-8") from error
    if sql.startswith("\ufeff"):
        sql = sql[1:]
    if "\ufeff" in sql or "\x00" in sql:
        raise MigrationCorruption("migration contains an invalid character")
    statement = ""
    statements: list[str] = []
    for character in sql:
        statement += character
        if character == ";" and sqlite3.complete_statement(statement):
            statements.append(statement)
            statement = ""
    if _skip_sql_trivia(statement, 0) != len(statement):
        raise MigrationCorruption("migration ends with an incomplete SQL statement")
    return tuple(statements)


def _skip_sql_trivia(value: str, start: int) -> int:
    index = start
    while True:
        while index < len(value) and value[index] in " \t\r\n\f\v":
            index += 1
        if value.startswith("--", index):
            newline = value.find("\n", index + 2)
            index = len(value) if newline < 0 else newline + 1
            continue
        if value.startswith("/*", index):
            closing = value.find("*/", index + 2)
            if closing < 0:
                raise MigrationCorruption(
                    "migration contains an unterminated SQL comment"
                )
            index = closing + 2
            continue
        return index


def _migration_leading_tokens(statement: str, limit: int = 2) -> tuple[str, ...]:
    tokens: list[str] = []
    index = 0
    while len(tokens) < limit:
        index = _skip_sql_trivia(statement, index)
        match = re.match(r"[A-Za-z]+", statement[index:])
        if match is None:
            break
        tokens.append(match.group(0).upper())
        index += len(match.group(0))
    return tuple(tokens)


def _validate_migration_statement(statement: str) -> None:
    tokens = _migration_leading_tokens(statement)
    if not tokens:
        raise MigrationCorruption("migration contains an empty SQL statement")
    if tokens[0] in {
        "BEGIN",
        "COMMIT",
        "DETACH",
        "END",
        "ATTACH",
        "PRAGMA",
        "RELEASE",
        "ROLLBACK",
        "SAVEPOINT",
        "VACUUM",
    }:
        raise MigrationCorruption("migration contains a forbidden SQL statement")
    if tokens[0] == "CREATE" and len(tokens) > 1 and tokens[1] in {
        "TEMP",
        "TEMPORARY",
    }:
        raise MigrationCorruption("migration may not create temporary objects")


def _require_migration_bookkeeping(
    connection: sqlite3.Connection,
    expected: tuple[_AppliedMigration, ...],
) -> None:
    actual = Journal._read_applied_migrations(connection)
    if actual != expected or _pragma_int(connection, "user_version") != len(expected):
        raise MigrationCorruption("migration bookkeeping changed unexpectedly")


def _verify_migration_connection_state(connection: sqlite3.Connection) -> None:
    if (
        _pragma_int(connection, "foreign_keys") != 1
        or _pragma_int(connection, "recursive_triggers") != 1
        or _pragma_int(connection, "busy_timeout") != BUSY_TIMEOUT_MILLISECONDS
        or _pragma_int(connection, "synchronous") != 2
    ):
        raise MigrationCorruption("migration changed journal connection settings")
    databases = _sql(connection, "PRAGMA database_list").fetchall()
    if any(len(row) < 2 or str(row[1]) not in {"main", "temp"} for row in databases):
        raise MigrationCorruption("migration attached an external database")
    temporary_objects = _sql(
        connection,
        "SELECT 1 FROM temp.sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1",
    ).fetchone()
    if temporary_objects is not None:
        raise MigrationCorruption("migration created a temporary schema object")


def _schema_sha256(connection: sqlite3.Connection) -> str:
    try:
        rows = _sql(connection,
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
    except sqlite3.Error as error:
        raise MigrationCorruption("database schema could not be inspected") from error
    material = json.dumps(
        [tuple(str(value) for value in row) for row in rows],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _utc_now() -> datetime:
    """Return the authority clock; tests may patch this private seam."""
    return datetime.now(timezone.utc)


def _canonical_timestamp(value: datetime) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        raise InvalidJournalValue("timestamp must be a timezone-aware datetime")
    try:
        if value.utcoffset() is None:
            raise InvalidJournalValue("timestamp must be a timezone-aware datetime")
        utc = value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise InvalidJournalValue("timestamp is outside the supported range") from error
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}."
        f"{utc.microsecond:06d}Z"
    )


def _parse_canonical_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except (TypeError, ValueError) as error:
        raise MigrationCorruption("stored timestamp is not canonical") from error
    return parsed.replace(tzinfo=timezone.utc)


def _journal_row_reference(
    table: str,
    columns: Sequence[str],
    row: Sequence[object],
) -> JournalRowReference:
    """Hash the table name, primary key, column names, and every stored value."""
    if len(columns) != len(row) or not row:
        raise MigrationCorruption("journal row shape is inconsistent")
    try:
        row_id = int(row[0])
    except (TypeError, ValueError) as error:
        raise MigrationCorruption("journal row identity is invalid") from error
    payload = {
        "version": 1,
        "table": table,
        "row_id": row_id,
        "columns": list(columns),
        "values": list(row),
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return JournalRowReference(table, row_id, digest)


def _journal_bundle_digest(
    namespace: str,
    references: Sequence[JournalRowReference],
    bounds: Mapping[str, object],
) -> str:
    unique: dict[tuple[str, int], JournalRowReference] = {}
    for reference in references:
        key = (reference.table, reference.row_id)
        prior = unique.get(key)
        if prior is not None and prior != reference:
            raise MigrationCorruption("journal bundle row digest conflicts")
        unique[key] = reference
    ordered = tuple(
        sorted(unique.values(), key=lambda item: (item.table, item.row_id))
    )
    payload = {
        "version": 1,
        "namespace": namespace,
        "bounds": dict(bounds),
        "references": [
            [reference.table, reference.row_id, reference.row_digest]
            for reference in ordered
        ],
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _confirmation_outbox_key(event_id: str, destination: str) -> str:
    digest = hashlib.sha256(
        b"stock-monitor/confirmation-outbox/v1\x00"
        + event_id.encode("utf-8")
        + b"\x00"
        + destination.encode("utf-8")
    ).hexdigest()
    return f"confirmation:{digest}"


def _canonical_stored_details(value: object, *, label: str) -> dict[str, object]:
    if type(value) is not str:
        raise MigrationCorruption(f"{label} details are not text")
    try:
        details = json.loads(value)
    except json.JSONDecodeError as error:
        raise MigrationCorruption(f"{label} details are invalid JSON") from error
    if not isinstance(details, dict) or _canonical_json(details) != value:
        raise MigrationCorruption(f"{label} details are not canonical")
    return details


def _expected_confirmation_normalized(action: object) -> dict[str, object]:
    from .confirmations import ParsedConfirmation

    if not isinstance(action, ParsedConfirmation):
        return {}
    return {
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


def _add_seconds(value: str, seconds: int) -> str:
    try:
        result = _parse_canonical_timestamp(value) + timedelta(seconds=seconds)
    except OverflowError as error:
        raise InvalidJournalValue("timestamp is outside the supported range") from error
    return _canonical_timestamp(result)


def _canonical_date(value: date) -> str:
    if type(value) is not date:
        raise InvalidJournalValue("session date must be a datetime.date")
    return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"


def stable_report_id(
    kind: str,
    session_date: date,
    observation_ids: Sequence[str],
    state_hash: str,
) -> str:
    """Return the Task 10 report identity from audited state inputs."""
    canonical_kind = _require_canonical_report_kind(kind)
    canonical_session = _canonical_date(session_date)
    if isinstance(observation_ids, (str, bytes)) or not isinstance(
        observation_ids, Sequence
    ):
        raise InvalidJournalValue("report observation identities must be a sequence")
    canonical_observations = [
        _require_nonempty_text(value, "report observation identity")
        for value in observation_ids
    ]
    state_hash = _require_sha256(state_hash, "report state hash")
    canonical = json.dumps(
        {
            "kind": canonical_kind,
            "session": canonical_session,
            "observations": sorted(canonical_observations),
            "state": state_hash,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def report_archive_relative_path(
    kind: str, session_date: date, report_id: str
) -> str:
    """Return the deterministic Task 10 archive path for a report identity."""
    canonical_kind = _require_canonical_report_kind(kind).lower()
    canonical_session = _canonical_date(session_date)
    report_id = _require_sha256(report_id, "report ID")
    return (
        f"reports/{canonical_session[:4]}/{canonical_session[5:7]}/"
        f"{canonical_session[8:10]}/{canonical_kind}-{canonical_session}-"
        f"{report_id[:REPORT_ID_PATH_PREFIX_LENGTH]}.md"
    )


def _require_nonempty_text(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise InvalidJournalValue(f"{name} must be a non-empty string")
    return value


def _canonical_token(value: object, name: str) -> str:
    value = _require_nonempty_text(value, name)
    if value != value.strip():
        raise InvalidJournalValue(f"{name} must not contain surrounding whitespace")
    canonical = value.upper()
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", canonical) is None:
        raise InvalidJournalValue(f"{name} is not a canonical token")
    return canonical


def _require_canonical_report_kind(value: object) -> str:
    value = _require_nonempty_text(value, "report kind")
    if value != value.strip() or re.fullmatch(r"[A-Z][A-Z0-9_]*", value) is None:
        raise InvalidJournalValue("report kind is not a canonical uppercase token")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_nonempty_text(value, name)


def _require_integer(value: object, name: str, *, minimum: int) -> int:
    if type(value) is not int or value < minimum or value > 2**63 - 1:
        raise InvalidJournalValue(f"{name} is outside the supported integer range")
    return value


def _optional_integer(
    value: object, name: str, *, minimum: int
) -> int | None:
    if value is None:
        return None
    return _require_integer(value, name, minimum=minimum)


def _require_signed_integer(value: object, name: str) -> int:
    if type(value) is not int or not -(2**63) <= value <= 2**63 - 1:
        raise InvalidJournalValue(f"{name} is outside the supported integer range")
    return value


def _optional_signed_integer(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _require_signed_integer(value, name)


def _require_sha256(value: object, name: str) -> str:
    value = _require_nonempty_text(value, name)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise InvalidJournalValue(f"{name} must be a lowercase SHA-256 digest")
    return value


def _canonical_archive_path(value: object) -> str:
    value = _require_nonempty_text(value, "archive relative path")
    if "\\" in value or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise InvalidJournalValue(
            "archive path contains an invalid character or separator"
        )
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != value
        or path.suffix.lower() != ".md"
    ):
        raise InvalidJournalValue("archive path must be a canonical relative Markdown path")
    return value


def _canonical_integer_set(values: object, name: str) -> tuple[int, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise InvalidJournalValue(f"{name} must be a sequence of integers")
    result = tuple(_require_integer(value, name, minimum=1) for value in values)
    if len(result) != len(set(result)):
        raise InvalidJournalValue(f"{name} must not contain duplicates")
    return result


def _canonical_json(value: object) -> str:
    _validate_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise InvalidJournalValue("details are not canonical JSON data") from error


def _canonical_details(value: object) -> str:
    if value is None:
        return "{}"
    if type(value) is not dict:
        raise InvalidJournalValue("details must be a JSON object")
    return _canonical_json(value)


def _validate_json_value(value: object) -> None:
    if value is None or type(value) in {str, bool}:
        return
    if type(value) is int:
        if not -(2**63) <= value <= 2**63 - 1:
            raise InvalidJournalValue("JSON integer is outside the supported range")
        return
    if type(value) is list:
        for item in value:
            _validate_json_value(item)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise InvalidJournalValue("JSON object keys must be strings")
            if key.lower().endswith("_micros"):
                raise InvalidJournalValue(
                    "money must use a typed integer microdollar column"
                )
            _validate_json_value(item)
        return
    raise InvalidJournalValue("details must not contain floating-point values")


def _whitelisted_table(value: object) -> str:
    if type(value) is not str or value not in _TABLES:
        raise InvalidJournalValue("table is not available through the journal API")
    return value


def _sql(
    connection: sqlite3.Connection,
    statement: str,
    parameters: Sequence[object] = (),
) -> sqlite3.Cursor:
    """Issue SQLite SQL without exposing a brokerage-like API name in our package."""
    method = getattr(connection, "execute")
    return method(statement, parameters)


def _enable_wal_with_bounded_retry(connection: sqlite3.Connection) -> None:
    deadline = time.monotonic() + BUSY_TIMEOUT_MILLISECONDS / 1_000
    while True:
        try:
            mode_row = _sql(connection, "PRAGMA journal_mode = WAL").fetchone()
        except sqlite3.Error as error:
            if not _is_busy_error(error) or time.monotonic() >= deadline:
                raise
        else:
            if mode_row is not None and str(mode_row[0]).lower() == "wal":
                return
            if time.monotonic() >= deadline:
                raise JournalError("journal database did not enter WAL mode")
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        _sql(connection,
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def _pragma_int(connection: sqlite3.Connection, name: str) -> int:
    if name not in {
        "application_id",
        "busy_timeout",
        "foreign_keys",
        "recursive_triggers",
        "synchronous",
        "user_version",
    }:
        raise JournalError("unsupported journal pragma")
    row = _sql(connection, f"PRAGMA {name}").fetchone()
    if row is None:
        raise JournalError("journal pragma returned no result")
    return int(row[0])


def _is_busy_error(error: sqlite3.Error) -> bool:
    code = getattr(error, "sqlite_errorcode", None)
    return code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED} or any(
        marker in str(error).lower() for marker in ("locked", "busy")
    )


def _translate_sqlite_error(error: sqlite3.Error) -> JournalError:
    if _is_busy_error(error):
        return JournalBusy("journal is busy")
    return JournalError("journal database operation failed")
