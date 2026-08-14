"""Transactional, append-only SQLite journal."""

from __future__ import annotations

import hashlib
import importlib.resources
import json
import re
import secrets
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Self


APPLICATION_ID = 0x53544B4D
BUSY_TIMEOUT_MILLISECONDS = 5_000
REPORT_ID_PATH_PREFIX_LENGTH = 12
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

    def _ensure_active(self) -> None:
        if not self._active or not self._journal._transaction_active:
            raise JournalError("journal transaction is no longer active")

    def _deactivate(self) -> None:
        self._active = False

    def append_raw_message(
        self, message_id: str, message_time: datetime, text: str
    ) -> tuple[int, bool]:
        self._ensure_active()
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
        return self._journal._append_execution_event(
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
        self._ensure_active()
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
        finalized_at: datetime | None = None,
        outbox_destination: str,
        outbox_payload: str,
    ) -> FinalizedReport:
        self._ensure_active()
        return self._journal._finalize_report(
            claim_id=claim_id,
            claim_token=claim_token,
            body=body,
            state_sha256=state_sha256,
            observation_ids=observation_ids,
            archive_relative_path=archive_relative_path,
            created_at=created_at,
            finalized_at=finalized_at,
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
        self._projection_write_allowed = False
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
            journal._verify_database_ownership(connection)
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
            try:
                self._connection.rollback()
            finally:
                self._transaction_active = False
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

    def append_execution_event(self, **values: object) -> tuple[int, bool]:
        """Append one parsed action outside a larger caller-owned transaction."""
        with self.transaction() as transaction:
            return transaction.append_execution_event(**values)  # type: ignore[arg-type]

    def append_source_observation(self, **values: object) -> tuple[int, bool]:
        """Append one content-addressed source observation."""
        with self.transaction() as transaction:
            return transaction.append_source_observation(**values)  # type: ignore[arg-type]

    def claim_report(
        self,
        session_date: date,
        kind: str,
        *,
        now: datetime | None = None,
        lease_seconds: int = 300,
    ) -> ReportClaim:
        """Acquire, observe, or explicitly recover a report publication claim."""
        stored_date = _canonical_date(session_date)
        report_kind = _canonical_token(kind, "report kind")
        lease_seconds = _require_integer(
            lease_seconds, "report claim lease", minimum=1
        )
        if lease_seconds > 86_400:
            raise InvalidJournalValue("report claim lease exceeds the supported bound")
        caller_supplied_now = now is not None
        stored_now = _canonical_timestamp(now) if now is not None else None

        with self._immediate_connection() as connection:
            row = _sql(connection,
                "SELECT claim.id, claim.claim_token, claim.status, "
                "claim.lease_expires_at, claim.report_id, report.report_id "
                "FROM report_claims AS claim LEFT JOIN reports AS report "
                "ON report.id = claim.report_id "
                "WHERE claim.session_date = ? AND claim.report_kind = ?",
                (stored_date, report_kind),
            ).fetchone()
            if row is None:
                acquired_at = stored_now or _canonical_timestamp(
                    datetime.now(timezone.utc)
                )
                expires_at = _add_seconds(acquired_at, lease_seconds)
                token = secrets.token_urlsafe(32)
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
            if caller_supplied_now and stored_now is not None and stored_now >= expires_at:
                recovered_expires_at = _add_seconds(stored_now, lease_seconds)
                token = secrets.token_urlsafe(32)
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

    def finalize_report(self, **values: object) -> FinalizedReport:
        """Finalize a report, its evidence pins, and its outbox row atomically."""
        with self.transaction() as transaction:
            return transaction.finalize_report(**values)  # type: ignore[arg-type]

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

    def append_outbox(self, **values: object) -> tuple[int, bool]:
        """Append an immutable payload; delivery remains externally at-least-once."""
        with self.transaction() as transaction:
            return transaction.append_outbox(**values)  # type: ignore[arg-type]

    def record_outbox_delivery_attempt(self, **values: object) -> tuple[int, bool]:
        """Append one delivery result without mutating the payload."""
        with self.transaction() as transaction:
            return transaction.record_outbox_delivery_attempt(  # type: ignore[arg-type]
                **values
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
            "WHERE next_attempt.outbox_id = o.id), 0) FROM outbox AS o "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM outbox_delivery_attempts AS a "
            "WHERE a.outbox_id = o.id AND a.delivery_status = 'DELIVERED'"
            ") ORDER BY o.id LIMIT ?",
            (limit,),
        ).fetchall()
        pending: list[PendingOutbox] = []
        for row in rows:
            prior_attempt_ordinal = int(row[8])
            if prior_attempt_ordinal >= 2**63 - 1:
                raise MigrationCorruption(
                    "outbox delivery attempt ordinal is exhausted"
                )
            pending.append(PendingOutbox(
                outbox_id=int(row[0]),
                idempotency_key=str(row[1]),
                origin_report_id=int(row[2]) if row[2] is not None else None,
                origin_execution_event_id=(
                    int(row[3]) if row[3] is not None else None
                ),
                destination=str(row[4]),
                payload_text=str(row[5]),
                payload_sha256=str(row[6]),
                created_at=_parse_canonical_timestamp(str(row[7])),
                next_attempt_ordinal=prior_attempt_ordinal + 1,
            ))
        return tuple(pending)

    def start_scheduled_run(self, **values: object) -> tuple[int, bool]:
        """Persist a start before scheduled work begins."""
        with self.transaction() as transaction:
            return transaction.start_scheduled_run(**values)  # type: ignore[arg-type]

    def complete_scheduled_run(self, **values: object) -> tuple[int, bool]:
        """Apply the sole permitted completion transition to a scheduled run."""
        with self.transaction() as transaction:
            return transaction.complete_scheduled_run(**values)  # type: ignore[arg-type]

    def append_account_check(self, **values: object) -> tuple[int, bool]:
        """Append one user-confirmed account state snapshot."""
        with self.transaction() as transaction:
            return transaction.append_account_check(**values)  # type: ignore[arg-type]

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
        if stored_event_time > stored_message_time:
            raise InvalidJournalValue(
                "execution event cannot postdate its authoritative message"
            )
        identity = hashlib.sha256(
            b"stock-monitor/execution-event/v1\x00"
            + message_id.encode("utf-8")
            + b"\x00"
            + str(action_ordinal).encode("ascii")
        ).hexdigest()
        event_id = f"evt_{identity}"
        idempotency_key = f"message-action:{identity}"
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
        finalized_at: datetime | None,
        outbox_destination: str,
        outbox_payload: str,
    ) -> FinalizedReport:
        claim_id = _require_integer(claim_id, "report claim row ID", minimum=1)
        claim_token = _require_nonempty_text(claim_token, "report claim token")
        body = _require_nonempty_text(body, "report body")
        state_sha256 = _require_sha256(state_sha256, "report state hash")
        archive_relative_path = _canonical_archive_path(archive_relative_path)
        stored_created_at = _canonical_timestamp(created_at)
        requested_finalized_at = (
            _canonical_timestamp(finalized_at) if finalized_at is not None else None
        )
        outbox_destination = _require_nonempty_text(
            outbox_destination, "outbox destination"
        )
        outbox_payload = _require_nonempty_text(outbox_payload, "outbox payload")
        requested_ids = _canonical_integer_set(
            observation_ids, "source observation row IDs"
        )

        claim = _sql(self._connection,
            "SELECT session_date, report_kind, claim_token, status, lease_started_at, "
            "lease_expires_at, report_id, finalized_at "
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
        if stored_status == "FINALIZED":
            if stored_finalized_at is None:
                raise MigrationCorruption("finalized report claim lacks a timestamp")
            if (
                requested_finalized_at is not None
                and requested_finalized_at != stored_finalized_at
            ):
                raise IdempotencyConflict(
                    "report finalization time conflicts with stored content"
                )
            effective_finalized_at = stored_finalized_at
        else:
            effective_finalized_at = requested_finalized_at or _canonical_timestamp(
                datetime.now(timezone.utc)
            )
        if stored_created_at > effective_finalized_at:
            raise InvalidJournalValue(
                "report creation time cannot follow finalization"
            )

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

        if stored_status != "IN_PROGRESS":
            raise MigrationCorruption("report claim has an invalid stored status")
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
            "SELECT signal_id, symbol, parsed_action FROM execution_events WHERE id = ?",
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
            "updated_at, revision FROM actual_positions WHERE signal_id = ? COLLATE BINARY",
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
        posting = _sql(
            self._connection,
            "SELECT ledger_name FROM ledger_postings WHERE id = ?",
            (last_ledger_posting_id,),
        ).fetchone()
        if posting is None or str(posting[0]) != "ACTUAL":
            raise InvalidJournalValue(
                "actual cash projection requires an ACTUAL ledger posting"
            )
        stored_updated_at = _canonical_timestamp(updated_at)
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
            "monthly_high_water_micros, last_ledger_posting_id, updated_at, revision "
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
            "SELECT parsed_action, reconciliation_state "
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
        desired = (
            int(reconciliation_required),
            reason,
            last_execution_event_id,
            stored_updated_at,
        )
        row = _sql(self._connection,
            "SELECT reconciliation_required, reason, last_execution_event_id, "
            "updated_at, revision FROM reconciliation_projection WHERE id = 1"
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
        self._projection_write_allowed = True
        try:
            yield
        finally:
            self._projection_write_allowed = False

    @contextmanager
    def _immediate_connection(self) -> Iterator[sqlite3.Connection]:
        if self._transaction_active:
            raise JournalError("nested journal transactions are not supported")
        try:
            _sql(self._connection, "BEGIN IMMEDIATE")
        except sqlite3.Error as error:
            raise _translate_sqlite_error(error) from error
        self._transaction_active = True
        try:
            yield self._connection
        except BaseException:
            try:
                self._connection.rollback()
            finally:
                self._transaction_active = False
            raise
        else:
            try:
                self._connection.commit()
            except sqlite3.Error as error:
                try:
                    self._connection.rollback()
                finally:
                    self._transaction_active = False
                raise _translate_sqlite_error(error) from error
            self._transaction_active = False

    def _configure_connection(self) -> None:
        try:
            self._connection.create_function(
                "journal_projection_write_allowed",
                0,
                lambda: int(self._projection_write_allowed),
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
    canonical_kind = _canonical_token(kind, "report kind")
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
    canonical_kind = _canonical_token(kind, "report kind").lower()
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
