from __future__ import annotations

import hashlib
import importlib.resources
import json
import re
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Event, current_thread
from unittest.mock import patch

import stock_monitor.journal as journal_module
from stock_monitor.journal import (
    APPLICATION_ID,
    Journal,
    JournalBusy,
    MigrationCorruption,
    MigrationDrift,
    report_archive_relative_path,
    stable_report_id,
)


class JournalMigrationTests(unittest.TestCase):
    def test_reopening_an_unchanged_database_replays_migrations_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"

            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

    def test_migration_source_can_be_loaded_independently_of_source_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            packaged = importlib.resources.files("stock_monitor.sql").joinpath(
                "001_core.sql"
            )
            (migration_directory / "001_core.sql").write_bytes(packaged.read_bytes())

            with Journal.open(
                root / "journal.db", migration_directory=migration_directory
            ) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

    def test_checksum_drift_is_rejected_without_changing_the_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            migration_path = migration_directory / "001_core.sql"
            packaged = importlib.resources.files("stock_monitor.sql").joinpath(
                "001_core.sql"
            )
            migration_path.write_bytes(packaged.read_bytes())
            database_path = root / "journal.db"

            with Journal.open(
                database_path, migration_directory=migration_directory
            ) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

            migration_path.write_bytes(migration_path.read_bytes() + b"\n-- drift\n")
            with self.assertRaises(MigrationDrift):
                Journal.open(
                    database_path, migration_directory=migration_directory
                )

            with closing(sqlite3.connect(database_path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM schema_migrations"
                    ).fetchone()[0],
                    1,
                )

    def test_forged_self_attested_schema_is_rejected_against_packaged_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            packaged = importlib.resources.files("stock_monitor.sql").joinpath(
                "001_core.sql"
            )
            migration_sha256 = hashlib.sha256(packaged.read_bytes()).hexdigest()
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER, name TEXT, sha256 TEXT, "
                    "schema_sha256 TEXT, applied_at TEXT)"
                )
                rows = connection.execute(
                    "SELECT type, name, tbl_name, COALESCE(sql, '') "
                    "FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' "
                    "ORDER BY type, name"
                ).fetchall()
                material = json.dumps(
                    [tuple(str(value) for value in row) for row in rows],
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                self_attested_sha256 = hashlib.sha256(material).hexdigest()
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (?, ?, ?, ?, ?)",
                    (
                        1,
                        "001_core.sql",
                        migration_sha256,
                        self_attested_sha256,
                        "2026-08-14T14:00:00.000000Z",
                    ),
                )
                connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                connection.execute("PRAGMA user_version = 1")
                connection.commit()

            unexpectedly_opened: Journal | None = None
            try:
                with self.assertRaises(MigrationDrift) as raised:
                    unexpectedly_opened = Journal.open(path)
                self.assertNotIn("001_core.sql", str(raised.exception))
            finally:
                if unexpectedly_opened is not None:
                    unexpectedly_opened.close()

    def test_migration_names_and_versions_must_be_ordered_and_contiguous(self) -> None:
        cases = {
            "empty": {},
            "malformed": {"1_bad.sql": b"SELECT 1;"},
            "duplicate": {
                "001_one.sql": b"SELECT 1;",
                "001_two.sql": b"SELECT 1;",
            },
            "gap": {
                "001_one.sql": b"SELECT 1;",
                "003_three.sql": b"SELECT 3;",
            },
        }
        for case, files in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                migration_directory = root / "migrations"
                migration_directory.mkdir()
                for name, contents in files.items():
                    (migration_directory / name).write_bytes(contents)

                with self.assertRaises(MigrationCorruption):
                    Journal.open(
                        root / "journal.db",
                        migration_directory=migration_directory,
                    )

    def test_bad_ddl_rolls_back_the_entire_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                "CREATE TABLE schema_migrations (version INTEGER);\n"
                "CREATE TABLE incomplete (",
                encoding="utf-8",
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                tables = connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                self.assertEqual(tables, [])
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 0)
                self.assertEqual(connection.execute("PRAGMA application_id").fetchone()[0], 0)

            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

    def test_migration_transaction_control_cannot_escape_atomic_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                "CREATE TABLE schema_migrations (version INTEGER);\n"
                "COMMIT;\n"
                "CREATE TABLE escaped (value TEXT);\n"
                "CREATE TABLE incomplete (",
                encoding="utf-8",
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                tables = connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                self.assertEqual(tables, [])

    def test_bom_prefixed_transaction_control_is_rejected_before_any_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_bytes(
                b"CREATE TABLE schema_migrations (version INTEGER);\n"
                b"\xef\xbb\xbf/* hidden transaction */ COMMIT;\n"
                b"CREATE TABLE escaped (value TEXT);\n"
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                tables = connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                self.assertEqual(tables, [])

    def test_nul_in_migration_is_reported_before_any_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_bytes(
                b"CREATE TABLE schema_migrations (version INTEGER);\n"
                b"\x00COMMIT;\n"
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    ).fetchall(),
                    [],
                )

    def test_migration_cannot_erase_prior_bookkeeping(self) -> None:
        metadata_sql = (
            "CREATE TABLE schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "sha256 TEXT NOT NULL, schema_sha256 TEXT NOT NULL, "
            "applied_at TEXT NOT NULL);"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                metadata_sql + "CREATE TABLE first_object (value TEXT);",
                encoding="utf-8",
            )
            (migration_directory / "002_erase_history.sql").write_text(
                "DELETE FROM schema_migrations;"
                "CREATE TABLE escaped_object (value TEXT);",
                encoding="utf-8",
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 0)
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    ).fetchall(),
                    [],
                )

    def test_migration_cannot_rewrite_prior_applied_time(self) -> None:
        metadata_sql = (
            "CREATE TABLE schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "sha256 TEXT NOT NULL, schema_sha256 TEXT NOT NULL, "
            "applied_at TEXT NOT NULL);"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                metadata_sql + "CREATE TABLE first_object (value TEXT);",
                encoding="utf-8",
            )
            (migration_directory / "002_rewrite_history.sql").write_text(
                "UPDATE schema_migrations "
                "SET applied_at = '1999-01-01T00:00:00.000000Z' "
                "WHERE version = 1;",
                encoding="utf-8",
            )

            with self.assertRaises(MigrationCorruption):
                Journal.open(
                    root / "journal.db", migration_directory=migration_directory
                )

    def test_scratch_verification_never_leaks_a_busy_domain_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch(
                "stock_monitor.journal._execute_migration",
                side_effect=JournalBusy("synthetic scratch failure"),
            ):
                with self.assertRaises(MigrationCorruption):
                    Journal.open(path)

    def test_migration_rejects_external_database_escape_before_execution(self) -> None:
        metadata_sql = (
            "CREATE TABLE schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "sha256 TEXT NOT NULL, schema_sha256 TEXT NOT NULL, "
            "applied_at TEXT NOT NULL);"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            escaped = root / "escaped.db"
            escaped_sql_path = str(escaped).replace("'", "''")
            (migration_directory / "001_core.sql").write_text(
                metadata_sql
                + f"ATTACH DATABASE '{escaped_sql_path}' AS escaped;"
                + "CREATE TABLE escaped.leaked (value TEXT);",
                encoding="utf-8",
            )

            with self.assertRaises(MigrationCorruption):
                Journal.open(
                    root / "journal.db", migration_directory=migration_directory
                )

            self.assertFalse(escaped.exists())

    def test_migration_parser_allows_bom_comments_strings_and_trigger_bodies(self) -> None:
        metadata_sql = (
            "CREATE TABLE schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "sha256 TEXT NOT NULL, schema_sha256 TEXT NOT NULL, "
            "applied_at TEXT NOT NULL);"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                "\ufeff-- BEGIN and ROLLBACK are harmless here\n"
                + metadata_sql
                + "CREATE TABLE audit (value TEXT);"
                + "CREATE TRIGGER audit_insert AFTER INSERT ON audit BEGIN "
                + "INSERT INTO audit(value) VALUES ('COMMIT is data'); END;"
                + "/* trailing SAVEPOINT comment */\n",
                encoding="utf-8",
            )

            with Journal.open(
                root / "journal.db", migration_directory=migration_directory
            ) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

    def test_malformed_migration_metadata_is_reported_and_rolled_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            migration_directory = root / "migrations"
            migration_directory.mkdir()
            (migration_directory / "001_core.sql").write_text(
                "CREATE TABLE schema_migrations (version INTEGER);",
                encoding="utf-8",
            )
            path = root / "journal.db"

            with self.assertRaises(MigrationCorruption):
                Journal.open(path, migration_directory=migration_directory)

            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    ).fetchall(),
                    [],
                )

    def test_malformed_applied_migration_row_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                connection.execute("PRAGMA user_version = 1")
                connection.execute(
                    "CREATE TABLE schema_migrations ("
                    "version, name, sha256, schema_sha256)"
                )
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (?, ?, ?, ?)",
                    ("not-an-integer", "001_core.sql", "a" * 64, "b" * 64),
                )
                connection.commit()

            with self.assertRaises(MigrationCorruption):
                Journal.open(path)

    def test_database_ownership_and_version_mismatches_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)

            foreign_path = root / "foreign.db"
            with closing(sqlite3.connect(foreign_path)) as connection:
                connection.execute("CREATE TABLE foreign_data (value TEXT)")
                connection.commit()
            with self.assertRaises(MigrationCorruption):
                Journal.open(foreign_path)
            with closing(sqlite3.connect(foreign_path)) as connection:
                self.assertEqual(
                    connection.execute("PRAGMA journal_mode").fetchone()[0],
                    "delete",
                )

            wrong_application_path = root / "wrong-application.db"
            with Journal.open(wrong_application_path):
                pass
            with closing(sqlite3.connect(wrong_application_path)) as connection:
                connection.execute("PRAGMA application_id = 123")
            with self.assertRaises(MigrationCorruption):
                Journal.open(wrong_application_path)

            wrong_version_path = root / "wrong-version.db"
            with Journal.open(wrong_version_path):
                pass
            with closing(sqlite3.connect(wrong_version_path)) as connection:
                connection.execute("PRAGMA user_version = 2")
            with self.assertRaises(MigrationCorruption):
                Journal.open(wrong_version_path)

            with closing(sqlite3.connect(wrong_version_path)) as connection:
                self.assertEqual(
                    connection.execute("PRAGMA application_id").fetchone()[0],
                    APPLICATION_ID,
                )

    def test_non_sqlite_file_is_reported_as_migration_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            path.write_bytes(b"not a sqlite database")

            with self.assertRaises(MigrationCorruption):
                Journal.open(path)

    def test_reopen_detects_schema_object_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("DROP TRIGGER raw_messages_no_update")

            with self.assertRaises(MigrationDrift):
                Journal.open(path)

    def test_concurrent_first_open_applies_each_migration_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"

            def open_and_count() -> int:
                with Journal.open(path) as journal:
                    return journal.count("schema_migrations")

            with ThreadPoolExecutor(max_workers=2) as executor:
                counts = tuple(executor.map(lambda _: open_and_count(), range(2)))

            self.assertEqual(counts, (1, 1))
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 1)

    def test_ownership_preflight_uses_one_snapshot_during_first_open(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with closing(sqlite3.connect(path)) as connection:
                mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
                self.assertEqual(mode, ("wal",))

            application_id_read = Event()
            release_preflight = Event()
            real_pragma_int = journal_module._pragma_int
            victim_paused = False

            def pause_after_unowned_read(
                connection: sqlite3.Connection, name: str
            ) -> int:
                nonlocal victim_paused
                value = real_pragma_int(connection, name)
                if (
                    not victim_paused
                    and current_thread().name.startswith("ownership-victim")
                    and name == "application_id"
                ):
                    victim_paused = True
                    self.assertEqual(value, 0)
                    application_id_read.set()
                    self.assertTrue(release_preflight.wait(timeout=10))
                return value

            def open_and_count() -> int:
                with Journal.open(path) as journal:
                    return journal.count("schema_migrations")

            with patch.object(
                journal_module, "_pragma_int", side_effect=pause_after_unowned_read
            ), ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="ownership-victim"
            ) as executor:
                victim = executor.submit(open_and_count)
                self.assertTrue(application_id_read.wait(timeout=10))
                try:
                    with Journal.open(path) as journal:
                        self.assertEqual(journal.count("schema_migrations"), 1)
                finally:
                    release_preflight.set()
                self.assertEqual(victim.result(timeout=10), 1)

    def test_open_retries_a_transient_wal_mode_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            real_sql = journal_module._sql
            wal_attempts = 0

            def busy_once(
                connection: sqlite3.Connection,
                statement: str,
                parameters: tuple[object, ...] = (),
            ) -> sqlite3.Cursor:
                nonlocal wal_attempts
                if statement == "PRAGMA journal_mode = WAL":
                    wal_attempts += 1
                    if wal_attempts == 1:
                        raise sqlite3.OperationalError("database is locked")
                return real_sql(connection, statement, parameters)

            with patch("stock_monitor.journal._sql", side_effect=busy_once):
                with Journal.open(path) as journal:
                    self.assertEqual(journal.pragma("journal_mode"), "wal")

            self.assertEqual(wal_attempts, 2)

    def test_core_schema_anticipates_audit_and_live_projection_persistence_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass

            with closing(sqlite3.connect(path)) as connection:
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                    if not str(row[0]).startswith("sqlite_")
                }

        self.assertEqual(
            tables,
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
            },
        )
        self.assertNotIn("signals", tables)
        self.assertFalse(any("option" in table or "phase1" in table for table in tables))

    def test_core_tables_are_strict_and_expose_required_audit_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass

            with closing(sqlite3.connect(path)) as connection:
                strict_by_table = {
                    str(row[1]): int(row[5])
                    for row in connection.execute("PRAGMA table_list")
                    if not str(row[1]).startswith("sqlite_")
                }
                columns = {
                    table: {
                        str(row[1]): str(row[2]).upper()
                        for row in connection.execute(f'PRAGMA table_info("{table}")')
                    }
                    for table in strict_by_table
                }

        self.assertTrue(strict_by_table)
        self.assertTrue(all(strict_by_table.values()))
        self.assertFalse(
            any(type_name == "REAL" for table in columns.values() for type_name in table.values())
        )
        expected_columns = {
            "source_observations": {
                "observation_sha256",
                "payload_sha256",
                "source_uri",
                "source_type",
                "provider",
                "feed",
                "source_time",
                "retrieved_at",
                "provider_sequence",
                "delay_seconds",
                "health_result",
                "details_json",
            },
            "execution_events": {
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
            },
            "account_checks": {
                "check_id",
                "raw_message_id",
                "execution_event_id",
                "settled_cash_micros",
                "pending_order_count",
                "unlogged_position_count",
                "confirmed_at",
                "reconciliation_result",
                "details_json",
            },
            "report_claims": {
                "session_date",
                "report_kind",
                "claim_token",
                "status",
                "created_at",
                "lease_started_at",
                "lease_expires_at",
                "finalized_at",
                "report_id",
            },
            "reports": {
                "report_id",
                "claim_id",
                "session_date",
                "report_kind",
                "body_text",
                "content_sha256",
                "state_sha256",
                "observation_set_sha256",
                "archive_relative_path",
                "created_at",
            },
            "report_observations": {
                "report_id",
                "source_observation_id",
                "observation_ordinal",
            },
            "outbox": {
                "idempotency_key",
                "origin_report_id",
                "origin_execution_event_id",
                "destination",
                "payload_text",
                "payload_sha256",
                "created_at",
            },
            "outbox_delivery_attempts": {
                "outbox_id",
                "attempt_ordinal",
                "attempted_at",
                "delivery_status",
                "external_delivery_id",
                "error_class",
                "details_json",
            },
            "scheduled_runs": {
                "run_key",
                "run_kind",
                "session_date",
                "intended_run_at",
                "started_at",
                "finished_at",
                "market_session_decision",
                "report_id",
                "report_path",
                "outcome",
                "error_class",
            },
            "ledger_postings": {
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
            },
            "actual_positions": {
                "symbol",
                "shares",
                "cost_basis_micros",
                "recommended_stop_micros",
                "user_confirmed_stop_micros",
                "target_micros",
                "last_execution_event_id",
                "updated_at",
                "revision",
            },
            "actual_cash_projection": {
                "estimated_settled_cash_micros",
                "user_confirmed_settled_cash_micros",
                "deployed_capital_micros",
                "open_planned_risk_micros",
                "consecutive_losses",
                "weekly_high_water_micros",
                "monthly_high_water_micros",
                "last_ledger_posting_id",
                "updated_at",
                "revision",
            },
            "reconciliation_projection": {
                "reconciliation_required",
                "reason",
                "last_execution_event_id",
                "updated_at",
                "revision",
            },
        }
        for table, required in expected_columns.items():
            with self.subTest(table=table):
                self.assertLessEqual(required, columns[table].keys())
        for table in columns.values():
            for name, type_name in table.items():
                if name.endswith("_micros"):
                    self.assertEqual(type_name, "INTEGER")

    def test_audit_hash_columns_require_lowercase_sha256_values(self) -> None:
        hash_fields = {
            "schema_migrations": {"sha256", "schema_sha256"},
            "raw_messages": {"raw_sha256"},
            "source_observations": {"observation_sha256", "payload_sha256"},
            "reports": {
                "content_sha256",
                "state_sha256",
                "observation_set_sha256",
            },
            "outbox": {"payload_sha256"},
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema WHERE type = 'table'"
                    )
                    if row[1] is not None
                }
                for table, fields in hash_fields.items():
                    for field in fields:
                        with self.subTest(table=table, field=field):
                            self.assertIn(f"length({field}) = 64", definitions[table])

                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO source_observations VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            1,
                            "not-a-sha256",
                            "a" * 64,
                            "https://example.test/source",
                            "MARKET_DATA",
                            "fixture",
                            None,
                            "2026-08-14T14:00:00.000000Z",
                            "2026-08-14T14:00:00.000000Z",
                            None,
                            None,
                            "OK",
                            "{}",
                        ),
                    )
    def test_every_immutable_table_rejects_update_delete_and_replace(self) -> None:
        immutable_tables = (
            "schema_migrations",
            "raw_messages",
            "source_observations",
            "execution_events",
            "account_checks",
            "reports",
            "report_observations",
            "outbox",
            "outbox_delivery_attempts",
            "ledger_postings",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.create_function(
                    "journal_report_claim_write_allowed", 0, lambda: 1
                )
                timestamp = "2026-08-14T14:00:00.000000Z"
                connection.execute(
                    "INSERT INTO raw_messages VALUES (?, ?, ?, ?, ?)",
                    (1, "msg-seed", timestamp, "SKIPPED SPY", "a" * 64),
                )
                connection.execute(
                    "INSERT INTO source_observations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "b" * 64,
                        "c" * 64,
                        "https://example.test/source",
                        "MARKET_DATA",
                        "fixture",
                        "SIP",
                        timestamp,
                        timestamp,
                        1,
                        0,
                        "OK",
                        "{}",
                    ),
                )
                connection.execute(
                    "INSERT INTO execution_events VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "evt-seed",
                        1,
                        0,
                        "event-key",
                        None,
                        "ACCOUNT_CHECK",
                        "SPY",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        timestamp,
                        timestamp,
                        "COMPLIANT",
                        "CLEAR",
                        "{}",
                    ),
                )
                connection.execute(
                    "INSERT INTO account_checks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, "check-seed", 1, 1, 5_000_000_000, 0, 0, timestamp, "CLEAR", "{}"),
                )
                connection.execute(
                    "INSERT INTO report_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "2026-08-14",
                        "CLOSE",
                        "claim-token",
                        "IN_PROGRESS",
                        timestamp,
                        timestamp,
                        "2026-08-14T14:05:00.000000Z",
                        None,
                        None,
                    ),
                )
                connection.execute(
                    "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "d" * 64,
                        1,
                        "2026-08-14",
                        "CLOSE",
                        "body",
                        "d" * 64,
                        "e" * 64,
                        "f" * 64,
                        "reports/2026/08/14/close-2026-08-14-dddddddddddd.md",
                        timestamp,
                    ),
                )
                connection.execute(
                    "INSERT INTO report_observations VALUES (?, ?, ?, ?)",
                    (1, 1, 1, 0),
                )
                connection.execute(
                    "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, "outbox-seed", 1, None, "TASK", "payload", "1" * 64, timestamp),
                )
                connection.execute(
                    "UPDATE report_claims SET status = 'FINALIZED', "
                    "finalized_at = ?, report_id = 1 WHERE id = 1",
                    (timestamp,),
                )
                connection.execute(
                    "INSERT INTO outbox_delivery_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, 1, 1, timestamp, "FAILED", None, "NETWORK", "{}"),
                )
                connection.execute(
                    "INSERT INTO ledger_postings VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "posting-seed",
                        "ACTUAL",
                        "CASH",
                        "EXECUTION",
                        None,
                        1,
                        "SPY",
                        -100_000_000,
                        1,
                        100_000_000,
                        timestamp,
                        "{}",
                    ),
                )
                connection.commit()

                for table in immutable_tables:
                    primary_key = "version" if table == "schema_migrations" else "id"
                    with self.subTest(table=table, operation="update"):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                f'UPDATE "{table}" SET "{primary_key}" = "{primary_key}"'
                            )
                    with self.subTest(table=table, operation="delete"):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(f'DELETE FROM "{table}"')
                    with self.subTest(table=table, operation="replace"):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                f'INSERT OR REPLACE INTO "{table}" '
                                f'SELECT * FROM "{table}" WHERE "{primary_key}" = 1'
                            )

    def test_all_audit_relationships_use_restrict_foreign_keys(self) -> None:
        expected_foreign_keys = {
            "execution_events": {("raw_message_id", "raw_messages")},
            "account_checks": {
                ("raw_message_id", "raw_messages"),
                ("execution_event_id", "execution_events"),
            },
            "report_claims": {("report_id", "reports")},
            "reports": {("claim_id", "report_claims")},
            "report_observations": {
                ("report_id", "reports"),
                ("source_observation_id", "source_observations"),
            },
            "outbox": {
                ("origin_report_id", "reports"),
                ("origin_execution_event_id", "execution_events"),
            },
            "outbox_delivery_attempts": {("outbox_id", "outbox")},
            "scheduled_runs": {("report_id", "reports")},
            "ledger_postings": {
                ("execution_event_id", "execution_events"),
                ("account_check_id", "account_checks"),
            },
            "actual_positions": {
                ("last_execution_event_id", "execution_events")
            },
            "actual_cash_projection": {
                ("last_ledger_posting_id", "ledger_postings")
            },
            "reconciliation_projection": {
                ("last_execution_event_id", "execution_events")
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                for table, expected in expected_foreign_keys.items():
                    rows = connection.execute(
                        f'PRAGMA foreign_key_list("{table}")'
                    ).fetchall()
                    with self.subTest(table=table):
                        self.assertEqual(
                            {(str(row[3]), str(row[2])) for row in rows}, expected
                        )
                        self.assertTrue(
                            all(
                                str(row[5]).upper() == "RESTRICT"
                                and str(row[6]).upper() == "RESTRICT"
                                for row in rows
                            )
                        )

    def test_numeric_columns_enforce_integer_storage_and_field_signs(self) -> None:
        numeric_fields = {
            "source_observations": {"provider_sequence", "delay_seconds"},
            "execution_events": {
                "raw_message_id",
                "action_ordinal",
                "shares",
                "price_micros",
                "bid_micros",
                "ask_micros",
                "recommended_stop_micros",
                "user_confirmed_stop_micros",
            },
            "account_checks": {
                "raw_message_id",
                "execution_event_id",
                "settled_cash_micros",
                "pending_order_count",
                "unlogged_position_count",
            },
            "report_claims": {"report_id"},
            "reports": {"claim_id"},
            "report_observations": {
                "report_id",
                "source_observation_id",
                "observation_ordinal",
            },
            "outbox": {"origin_report_id", "origin_execution_event_id"},
            "outbox_delivery_attempts": {"outbox_id", "attempt_ordinal"},
            "scheduled_runs": {"report_id"},
            "ledger_postings": {
                "execution_event_id",
                "account_check_id",
                "amount_micros",
                "shares_delta",
                "unit_price_micros",
            },
            "actual_positions": {
                "shares",
                "cost_basis_micros",
                "recommended_stop_micros",
                "user_confirmed_stop_micros",
                "target_micros",
                "last_execution_event_id",
                "revision",
            },
            "actual_cash_projection": {
                "estimated_settled_cash_micros",
                "user_confirmed_settled_cash_micros",
                "deployed_capital_micros",
                "open_planned_risk_micros",
                "consecutive_losses",
                "weekly_high_water_micros",
                "monthly_high_water_micros",
                "last_ledger_posting_id",
                "revision",
            },
            "reconciliation_projection": {
                "reconciliation_required",
                "last_execution_event_id",
                "revision",
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema WHERE type = 'table'"
                    )
                    if row[1] is not None
                }
                for table, fields in numeric_fields.items():
                    for field in fields:
                        with self.subTest(table=table, field=field):
                            self.assertIn(f"typeof({field})", definitions[table])

                timestamp = "2026-08-14T14:00:00.000000Z"
                invalid_values: tuple[object, ...] = (1.25, -1)
                for index, invalid in enumerate(invalid_values, start=1):
                    with self.subTest(provider_sequence=invalid):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO source_observations VALUES "
                                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    index,
                                    f"{index:x}" * 64,
                                    "a" * 64,
                                    "https://example.test/source",
                                    "MARKET_DATA",
                                    "fixture",
                                    None,
                                    timestamp,
                                    timestamp,
                                    invalid,
                                    0,
                                    "OK",
                                    "{}",
                                ),
                            )

    def test_schema_rejects_noncanonical_dates_and_utc_timestamps(self) -> None:
        timestamp_fields = {
            "source_observations": {"source_time", "retrieved_at"},
            "execution_events": {"event_time", "message_time"},
            "account_checks": {"confirmed_at"},
            "report_claims": {
                "created_at",
                "lease_started_at",
                "lease_expires_at",
                "finalized_at",
            },
            "reports": {"created_at"},
            "outbox": {"created_at"},
            "outbox_delivery_attempts": {"attempted_at"},
            "scheduled_runs": {"intended_run_at", "started_at", "finished_at"},
            "ledger_postings": {"occurred_at"},
            "actual_positions": {"updated_at"},
            "actual_cash_projection": {"updated_at"},
            "reconciliation_projection": {"updated_at"},
        }
        date_fields = {
            "report_claims": {"session_date"},
            "reports": {"session_date"},
            "scheduled_runs": {"session_date"},
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                connection.create_function(
                    "journal_report_claim_write_allowed", 0, lambda: 1
                )
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema WHERE type = 'table'"
                    )
                    if row[1] is not None
                }
                for table, fields in timestamp_fields.items():
                    for field in fields:
                        with self.subTest(table=table, timestamp=field):
                            self.assertIn(f"length({field}) = 27", definitions[table])
                for table, fields in date_fields.items():
                    for field in fields:
                        with self.subTest(table=table, date=field):
                            self.assertIn(f"length({field}) = 10", definitions[table])

                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO source_observations VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            1,
                            "a" * 64,
                            "b" * 64,
                            "https://example.test/source",
                            "MARKET_DATA",
                            "fixture",
                            None,
                            "2026-02-30T14:00:00.000000Z",
                            "2026-08-14T14:00:00.000000Z",
                            None,
                            None,
                            "OK",
                            "{}",
                        ),
                    )
                invalid_components = (
                    "2026-13-01T12:00:00.000000Z",
                    "2026-01-32T12:00:00.000000Z",
                    "2026-01-01T12:60:00.000000Z",
                    "2026-01-01T12:00:60.000000Z",
                )
                for index, invalid in enumerate(invalid_components, start=3):
                    with self.subTest(timestamp=invalid):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO source_observations VALUES "
                                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    index,
                                    f"{index:064x}",
                                    "e" * 64,
                                    "https://example.test/source",
                                    "MARKET_DATA",
                                    "fixture",
                                    None,
                                    invalid,
                                    "2026-08-14T14:00:00.000000Z",
                                    None,
                                    None,
                                    "OK",
                                    "{}",
                                ),
                            )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO report_claims VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            1,
                            "2026-13-01",
                            "CLOSE",
                            "invalid-date-token",
                            "IN_PROGRESS",
                            "2026-08-14T14:00:00.000000Z",
                            "2026-08-14T14:00:00.000000Z",
                            "2026-08-14T14:05:00.000000Z",
                            None,
                            None,
                        ),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO source_observations VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            2,
                            "c" * 64,
                            "d" * 64,
                            "https://example.test/source",
                            "MARKET_DATA",
                            "fixture",
                            None,
                            "2026-08-14T24:00:00.000000Z",
                            "2026-08-14T14:00:00.000000Z",
                            None,
                            None,
                            "OK",
                            "{}",
                        ),
                    )

    def test_schema_accepts_canonical_full_microsecond_instants(self) -> None:
        instant = datetime(2026, 8, 14, 12, 45, 0, 139_771, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=instant
            ), Journal.open(path) as journal:
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")

            self.assertEqual(claim.status, "ACQUIRED")

    def test_outbox_schema_requires_one_origin_and_one_delivered_attempt(self) -> None:
        timestamp = "2026-08-14T14:00:00.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                for row_id, origins in enumerate(((None, None), (1, 1)), start=1):
                    with self.subTest(origins=origins):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    row_id,
                                    f"invalid-origin-{row_id}",
                                    origins[0],
                                    origins[1],
                                    "TASK",
                                    "payload",
                                    "a" * 64,
                                    timestamp,
                                ),
                            )
                connection.execute(
                    "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (3, "valid-origin", None, 1, "TASK", "payload", "b" * 64, timestamp),
                )
                connection.execute(
                    "INSERT INTO outbox_delivery_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, 3, 1, timestamp, "DELIVERED", "one", None, "{}"),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO outbox_delivery_attempts VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?)",
                        (2, 3, 2, timestamp, "DELIVERED", "two", None, "{}"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT OR REPLACE INTO outbox_delivery_attempts VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?)",
                        (4, 3, 4, timestamp, "DELIVERED", "four", None, "{}"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO outbox_delivery_attempts VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?)",
                        (3, 3, 3, timestamp, "PENDING", None, None, "{}"),
                    )

    def test_outbox_schema_is_effectively_once_per_event_destination(self) -> None:
        instant = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        timestamp = "2026-08-14T14:00:00.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                raw_id, _ = journal.append_raw_message(
                    "msg-event-outbox-sql", instant, "SKIPPED SPY"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SKIPPED",
                    event_time=instant,
                    symbol="SPY",
                )
                journal.append_outbox(
                    idempotency_key="event-outbox-sql-primary",
                    origin_report_id=None,
                    origin_execution_event_id=event_id,
                    destination="CODEX_TASK",
                    payload_text="recorded",
                    created_at=instant,
                )

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO outbox("
                        "idempotency_key, origin_report_id, "
                        "origin_execution_event_id, destination, payload_text, "
                        "payload_sha256, created_at"
                        ") VALUES (?, NULL, ?, ?, ?, ?, ?)",
                        (
                            "event-outbox-sql-conflict",
                            event_id,
                            "CODEX_TASK",
                            "duplicate",
                            hashlib.sha256(b"duplicate").hexdigest(),
                            timestamp,
                        ),
                    )

    def test_actual_ledger_and_cash_projection_enforce_origin_chronology_in_sql(self) -> None:
        instant = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        event_time = "2026-08-14T14:00:00.000000Z"
        posting_time = "2026-08-14T14:00:01.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                raw_id, _ = journal.append_raw_message(
                    "msg-actual-sql", instant, "SOLD SPY"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SOLD",
                    event_time=instant,
                    symbol="SPY",
                )

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_projection_write_allowed", 0, lambda: 1
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO ledger_postings("
                        "posting_key, ledger_name, account_name, entry_kind, "
                        "amount_micros, occurred_at, details_json"
                        ") VALUES (?, 'ACTUAL', 'CASH', 'SALE', ?, ?, '{}')",
                        ("actual-sql-orphan", 100_000_000, event_time),
                    )

                cursor = connection.execute(
                    "INSERT INTO ledger_postings("
                    "posting_key, ledger_name, account_name, entry_kind, "
                    "execution_event_id, symbol, amount_micros, occurred_at, details_json"
                    ") VALUES (?, 'ACTUAL', 'CASH', 'SALE', ?, 'SPY', ?, ?, '{}')",
                    ("actual-sql-valid", event_id, 100_000_000, posting_time),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO actual_cash_projection VALUES "
                        "(1, ?, NULL, 0, 0, 0, ?, ?, ?, ?, 1)",
                        (
                            5_100_000_000,
                            5_100_000_000,
                            5_100_000_000,
                            int(cursor.lastrowid),
                            event_time,
                        ),
                    )

    def test_actual_ledger_sql_requires_cash_mutating_event_action(self) -> None:
        instant = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        timestamp = "2026-08-14T14:00:00.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                raw_id, _ = journal.append_raw_message(
                    "msg-skipped-actual-sql", instant, "SKIPPED SPY"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SKIPPED",
                    event_time=instant,
                    symbol="SPY",
                )

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO ledger_postings("
                        "posting_key, ledger_name, account_name, entry_kind, "
                        "execution_event_id, symbol, amount_micros, occurred_at, "
                        "details_json"
                        ") VALUES (?, 'ACTUAL', 'CASH', 'SALE', ?, 'SPY', ?, ?, '{}')",
                        (
                            "actual-sql-from-skipped",
                            event_id,
                            100_000_000,
                            timestamp,
                        ),
                    )

    def test_actual_ledger_event_action_allowlist_matches_sql_trigger(self) -> None:
        expected = frozenset(
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
        self.assertEqual(journal_module._ACTUAL_LEDGER_EVENT_ACTIONS, expected)

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT sql FROM sqlite_schema "
                    "WHERE type = 'trigger' "
                    "AND name = 'ledger_postings_validate_actual_origin'"
                ).fetchone()
        self.assertIsNotNone(row)
        trigger_sql = str(row[0])
        match = re.search(
            r"event\.parsed_action\s+IN\s*\((?P<actions>.*?)\)",
            trigger_sql,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        assert match is not None
        sql_actions = frozenset(re.findall(r"'([A-Z_]+)'", match.group("actions")))
        self.assertEqual(sql_actions, expected)

    def test_controlled_claim_and_scheduled_rows_reject_forbidden_mutations(self) -> None:
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=now
            ) as clock, Journal.open(path) as journal:
                in_progress = journal.claim_report(date(2026, 8, 15), "CLOSE")
                finalized = journal.claim_report(date(2026, 8, 14), "CLOSE")
                assert finalized.claim_token is not None
                session_date = date(2026, 8, 14)
                state_sha256 = "a" * 64
                report_id = stable_report_id(
                    "CLOSE", session_date, (), state_sha256
                )
                clock.return_value = now + timedelta(seconds=1)
                report = journal.finalize_report(
                    claim_id=finalized.claim_id,
                    claim_token=finalized.claim_token,
                    body="# Close\n",
                    state_sha256=state_sha256,
                    observation_ids=(),
                    archive_relative_path=report_archive_relative_path(
                        "CLOSE", session_date, report_id
                    ),
                    created_at=now + timedelta(seconds=1),
                    outbox_destination="TASK",
                    outbox_payload="close",
                )
                run_id, _ = journal.start_scheduled_run(
                    run_key="close-2026-08-14",
                    run_kind="CLOSE",
                    session_date=date(2026, 8, 14),
                    intended_run_at=now,
                    started_at=now,
                )
                journal.complete_scheduled_run(
                    run_id=run_id,
                    finished_at=now + timedelta(seconds=2),
                    market_session_decision="OPEN",
                    outcome="REPORT_EMITTED",
                    report_id=report.report_row_id,
                    report_path=report_archive_relative_path(
                        "CLOSE", session_date, report_id
                    ),
                )

            with closing(sqlite3.connect(path)) as connection:
                forbidden = (
                    (
                        "UPDATE report_claims SET session_date = '2026-08-16' WHERE id = ?",
                        (in_progress.claim_id,),
                    ),
                    (
                        "DELETE FROM report_claims WHERE id = ?",
                        (in_progress.claim_id,),
                    ),
                    (
                        "UPDATE report_claims SET claim_token = claim_token WHERE id = ?",
                        (finalized.claim_id,),
                    ),
                    (
                        "INSERT OR REPLACE INTO report_claims "
                        "SELECT * FROM report_claims WHERE id = ?",
                        (finalized.claim_id,),
                    ),
                    (
                        "UPDATE scheduled_runs SET run_key = 'different' WHERE id = ?",
                        (run_id,),
                    ),
                    ("DELETE FROM scheduled_runs WHERE id = ?", (run_id,)),
                    (
                        "INSERT OR REPLACE INTO scheduled_runs "
                        "SELECT * FROM scheduled_runs WHERE id = ?",
                        (run_id,),
                    ),
                )
                for statement, parameters in forbidden:
                    with self.subTest(statement=statement.split()[0:3]):
                        with self.assertRaises(sqlite3.DatabaseError):
                            connection.execute(statement, parameters)

    def test_report_claim_insert_requires_the_journal_clock_boundary(self) -> None:
        timestamp = "2026-08-14T12:45:00.000000Z"
        expires_at = "2026-08-14T12:50:00.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA recursive_triggers = ON")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "INSERT INTO report_claims("
                        "session_date, report_kind, claim_token, status, created_at, "
                        "lease_started_at, lease_expires_at"
                        ") VALUES (?, ?, ?, 'IN_PROGRESS', ?, ?, ?)",
                        (
                            "2026-08-14",
                            "CLOSE",
                            "forged-insert-token",
                            timestamp,
                            timestamp,
                            expires_at,
                        ),
                    )

    def test_report_claim_recovery_requires_the_journal_clock_boundary(self) -> None:
        lease_start = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=lease_start
            ), Journal.open(path) as journal:
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA recursive_triggers = ON")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE report_claims SET claim_token = ?, "
                        "lease_started_at = ?, lease_expires_at = ? WHERE id = ?",
                        (
                            "forged-recovery-token",
                            "2026-08-14T12:50:00.000000Z",
                            "2026-08-14T12:55:00.000000Z",
                            claim.claim_id,
                        ),
                    )

    def test_scheduled_completion_tokens_are_canonical_in_sql(self) -> None:
        timestamp = "2026-08-14T14:00:00.000000Z"
        finished = "2026-08-14T14:00:01.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.execute(
                    "INSERT INTO scheduled_runs("
                    "run_key, run_kind, session_date, intended_run_at, started_at"
                    ") VALUES (?, ?, ?, ?, ?)",
                    (
                        "lowercase-completion",
                        "CLOSE",
                        "2026-08-14",
                        timestamp,
                        timestamp,
                    ),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE scheduled_runs SET finished_at = ?, "
                        "market_session_decision = 'open', "
                        "outcome = 'report_emitted' WHERE run_key = ?",
                        (finished, "lowercase-completion"),
                    )

    def test_report_links_enforce_claim_identity_and_no_future_evidence(self) -> None:
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        report_time = now + timedelta(seconds=1)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=now
            ), Journal.open(path) as journal:
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")
                future_observation_id, _ = journal.append_source_observation(
                    payload=b"future payload",
                    source_uri="https://example.test/future",
                    source_type="MARKET_DATA",
                    provider="fixture",
                    feed=None,
                    source_time=now,
                    retrieved_at=now + timedelta(days=1),
                    provider_sequence=None,
                    delay_seconds=None,
                    health_result="OK",
                )
                future_source_observation_id, _ = journal.append_source_observation(
                    payload=b"future source payload",
                    source_uri="https://example.test/future-source",
                    source_type="MARKET_DATA",
                    provider="fixture",
                    feed=None,
                    source_time=now + timedelta(days=1),
                    retrieved_at=now,
                    provider_sequence=None,
                    delay_seconds=None,
                    health_result="OK",
                )
                run_id, _ = journal.start_scheduled_run(
                    run_key="close-2026-08-14",
                    run_kind="CLOSE",
                    session_date=date(2026, 8, 14),
                    intended_run_at=now,
                    started_at=now,
                )

            canonical_report_time = report_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_report_claim_write_allowed", 0, lambda: 1
                )
                report_values = (
                    1,
                    "1" * 64,
                    claim.claim_id,
                    "2026-08-14",
                    "CLOSE",
                    "# Close\n",
                    "a" * 64,
                    "b" * 64,
                    "c" * 64,
                    "reports/2026/08/14/close-2026-08-14-111111111111.md",
                    canonical_report_time,
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (*report_values[:3], "2026-08-15", "OPEN", *report_values[5:]),
                    )

                connection.execute(
                    "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    report_values,
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE scheduled_runs SET finished_at = ?, "
                        "market_session_decision = 'OPEN', "
                        "outcome = 'REPORT_EMITTED', report_id = 1, "
                        "report_path = ? WHERE id = ?",
                        (
                            (report_time + timedelta(seconds=1)).strftime(
                                "%Y-%m-%dT%H:%M:%S.%fZ"
                            ),
                            report_values[9],
                            run_id,
                        ),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE report_claims SET status = 'FINALIZED', "
                        "finalized_at = ?, report_id = 1 WHERE id = ?",
                        (
                            (report_time + timedelta(seconds=1)).strftime(
                                "%Y-%m-%dT%H:%M:%S.%fZ"
                            ),
                            claim.claim_id,
                        ),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO report_observations VALUES (?, ?, ?, ?)",
                        (1, 1, future_observation_id, 0),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO report_observations VALUES (?, ?, ?, ?)",
                        (2, 1, future_source_observation_id, 0),
                    )

                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE report_claims SET status = 'FINALIZED', "
                        "finalized_at = ?, report_id = 1 WHERE id = ?",
                        (canonical_report_time, claim.claim_id),
                    )
                connection.execute(
                    "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "report-delivery-1",
                        1,
                        None,
                        "CODEX_TASK",
                        "close",
                        "d" * 64,
                        canonical_report_time,
                    ),
                )
                connection.execute(
                    "UPDATE report_claims SET status = 'FINALIZED', "
                    "finalized_at = ?, report_id = 1 WHERE id = ?",
                    (canonical_report_time, claim.claim_id),
                )

    def test_report_rows_require_sha256_ids_and_identity_compatible_paths(self) -> None:
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        report_time = now + timedelta(seconds=1)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=now
            ), Journal.open(path) as journal:
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")

            canonical_report_time = report_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                invalid_id = (
                    1,
                    "not-a-sha256",
                    claim.claim_id,
                    "2026-08-14",
                    "CLOSE",
                    "# Close\n",
                    "a" * 64,
                    "b" * 64,
                    "c" * 64,
                    "reports/2026/08/14/close.md",
                    canonical_report_time,
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        invalid_id,
                    )

                valid_id = "d" * 64
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            *invalid_id[:1],
                            valid_id,
                            *invalid_id[2:9],
                            "reports/2026/08/14/wrong.md",
                            canonical_report_time,
                        ),
                    )


if __name__ == "__main__":
    unittest.main()
