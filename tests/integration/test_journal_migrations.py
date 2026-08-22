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
    ScheduledRunResultEnvelope,
    report_archive_relative_path,
    stable_report_id,
)


class JournalMigrationTests(unittest.TestCase):
    @staticmethod
    def _seed_projection_chronology(
        path: Path,
        *,
        include_projections: bool = True,
    ) -> tuple[tuple[int, int, int], tuple[int, int, int], str, str]:
        base = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        source_time = base + timedelta(seconds=10)
        delayed_time = base + timedelta(seconds=5)
        updated_at = base + timedelta(seconds=20)
        event_ids: list[int] = []
        with Journal.open(path) as journal:
            for suffix, event_time in (
                ("initial", source_time),
                ("delayed", delayed_time),
                ("equal-time", source_time),
            ):
                raw_id, _ = journal.append_raw_message(
                    f"msg-sql-projection-{suffix}", event_time, "BOUGHT SPY"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="BOUGHT",
                    event_time=event_time,
                    signal_id="signal-sql-chronology",
                    symbol="SPY",
                    shares=1,
                    reconciliation_state="REQUIRED",
                )
                event_ids.append(event_id)

            posting_ids: list[int] = []
            with journal.transaction() as transaction:
                for suffix, occurred_at, event_id in zip(
                    ("initial", "delayed", "equal-time"),
                    (source_time, delayed_time, source_time),
                    event_ids,
                    strict=True,
                ):
                    posting_id, _ = transaction.append_ledger_posting(
                        posting_key=f"sql-projection-{suffix}",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="BUY",
                        occurred_at=occurred_at,
                        amount_micros=-100_000_000,
                        execution_event_id=event_id,
                        symbol="SPY",
                    )
                    posting_ids.append(posting_id)
                if include_projections:
                    transaction.write_actual_position(
                        signal_id="signal-sql-chronology",
                        symbol="SPY",
                        shares=1,
                        cost_basis_micros=100_000_000,
                        recommended_stop_micros=None,
                        user_confirmed_stop_micros=None,
                        target_micros=None,
                        last_execution_event_id=event_ids[0],
                        updated_at=updated_at,
                    )
                    transaction.write_actual_cash_projection(
                        estimated_settled_cash_micros=5_000_000_000,
                        user_confirmed_settled_cash_micros=None,
                        deployed_capital_micros=0,
                        open_planned_risk_micros=0,
                        consecutive_losses=0,
                        weekly_high_water_micros=5_000_000_000,
                        monthly_high_water_micros=5_000_000_000,
                        last_ledger_posting_id=posting_ids[0],
                        updated_at=updated_at,
                    )
                    transaction.write_reconciliation_projection(
                        reconciliation_required=True,
                        reason="INITIAL",
                        last_execution_event_id=event_ids[0],
                        updated_at=updated_at,
                    )
        return (
            (event_ids[0], event_ids[1], event_ids[2]),
            (posting_ids[0], posting_ids[1], posting_ids[2]),
            "2026-08-14T14:00:20.000000Z",
            "2026-08-14T14:00:19.999999Z",
        )

    def test_reopening_an_unchanged_database_replays_migrations_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"

            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 5)
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 5)

    def test_provider_monitoring_migration_is_packaged_hashed_and_applied_fifth(self) -> None:
        packaged = importlib.resources.files("stock_monitor.sql").joinpath(
            "005_provider_monitoring.sql"
        )
        self.assertTrue(packaged.is_file(), "packaged migration 005 is missing")
        expected_sha256 = hashlib.sha256(packaged.read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT version, name, sha256 FROM schema_migrations "
                    "ORDER BY version"
                ).fetchall()
                user_version = connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0]

        self.assertEqual(
            [(int(row[0]), str(row[1])) for row in rows],
            [
                (1, "001_core.sql"),
                (2, "002_phase1.sql"),
                (3, "003_phase2_paper.sql"),
                (4, "004_scheduled_result_envelope.sql"),
                (5, "005_provider_monitoring.sql"),
            ],
        )
        self.assertEqual(str(rows[4][2]), expected_sha256)
        self.assertEqual(user_version, 5)

    def test_provider_monitoring_tables_are_strict_and_have_exact_columns(self) -> None:
        expected_columns = {
            "canonical_report_contexts": (
                "id",
                "report_id",
                "workflow_kind",
                "economic_at",
                "retrieved_at",
                "material_digest",
                "source_digest",
                "record_sha256",
            ),
            "actual_close_reviews": (
                "id",
                "review_id",
                "session_date",
                "review_at",
                "mark_cutoff",
                "query_cutoff",
                "retrieved_at",
                "expected_binding_count",
                "source_digest",
                "record_sha256",
            ),
            "actual_close_source_bindings": (
                "id",
                "review_id",
                "binding_ordinal",
                "symbol",
                "source_role",
                "source_observation_id",
                "failure_code",
                "received_at",
                "record_sha256",
            ),
            "close_recommendations": (
                "id",
                "recommendation_id",
                "review_id",
                "session_date",
                "symbol",
                "recommended_stop_micros",
                "action",
                "reasons_json",
                "source_digest",
                "received_at",
                "record_sha256",
            ),
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass

            with closing(sqlite3.connect(path)) as connection:
                table_rows = {
                    str(row[1]): row
                    for row in connection.execute("PRAGMA table_list")
                }
                actual_columns = {
                    table: tuple(
                        str(row[1])
                        for row in connection.execute(
                            f'PRAGMA table_info("{table}")'
                        )
                    )
                    for table in expected_columns
                }

        for table, columns in expected_columns.items():
            with self.subTest(table=table):
                self.assertIn(table, table_rows)
                self.assertEqual(int(table_rows[table][5]), 1)
                self.assertEqual(actual_columns[table], columns)

    def test_scheduled_report_kind_mapping_is_exact_with_premarket_alias(self) -> None:
        cases = (
            ("PREMARKET", "MORNING", True),
            ("PREMARKET", "CLOSE", False),
            ("CLOSE", "MORNING", False),
            ("CLOSE", "CLOSE", True),
        )
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        finished_at = now + timedelta(seconds=2)
        envelope_json = "{}"
        envelope_sha256 = hashlib.sha256(envelope_json.encode("utf-8")).hexdigest()

        for run_kind, report_kind, allowed in cases:
            with self.subTest(run_kind=run_kind, report_kind=report_kind):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with patch.object(
                        journal_module, "_utc_now", return_value=now
                    ) as clock, Journal.open(path) as journal:
                        claim = journal.claim_report(date(2026, 8, 14), report_kind)
                        assert claim.claim_token is not None
                        state_sha256 = "a" * 64
                        report_id = stable_report_id(
                            report_kind,
                            date(2026, 8, 14),
                            (),
                            state_sha256,
                        )
                        archive_path = report_archive_relative_path(
                            report_kind,
                            date(2026, 8, 14),
                            report_id,
                        )
                        clock.return_value = now + timedelta(seconds=1)
                        report = journal.finalize_report(
                            claim_id=claim.claim_id,
                            claim_token=claim.claim_token,
                            body=f"# {report_kind}\n",
                            state_sha256=state_sha256,
                            observation_ids=(),
                            archive_relative_path=archive_path,
                            created_at=now + timedelta(seconds=1),
                            outbox_destination="TASK",
                            outbox_payload=report_kind,
                        )
                        run_id = int(
                            journal._connection.execute(
                                "INSERT INTO scheduled_runs("
                                "run_key, run_kind, session_date, intended_run_at, "
                                "started_at) VALUES (?, ?, ?, ?, ?)",
                                (
                                    f"{run_kind.lower()}-2026-08-14",
                                    run_kind,
                                    "2026-08-14",
                                    "2026-08-14T12:45:00.000000Z",
                                    "2026-08-14T12:45:00.000000Z",
                                ),
                            ).lastrowid
                        )
                        values = (
                            "2026-08-14T12:45:02.000000Z",
                            report.report_row_id,
                            archive_path,
                            envelope_json,
                            envelope_sha256,
                            run_id,
                        )
                        statement = (
                            "UPDATE scheduled_runs SET finished_at = ?, "
                            "market_session_decision = 'DUE_WAKE', report_id = ?, "
                            "report_path = ?, outcome = 'REPORT_EMITTED', "
                            "result_envelope_json = ?, result_envelope_sha256 = ? "
                            "WHERE id = ?"
                        )
                        if allowed:
                            journal._connection.execute(statement, values)
                            self.assertEqual(
                                journal._connection.execute(
                                    "SELECT finished_at FROM scheduled_runs WHERE id = ?",
                                    (run_id,),
                                ).fetchone(),
                                (
                                    finished_at.strftime(
                                        "%Y-%m-%dT%H:%M:%S.%fZ"
                                    ),
                                ),
                            )
                        else:
                            with self.assertRaises(sqlite3.IntegrityError):
                                journal._connection.execute(statement, values)
                            self.assertEqual(
                                journal._connection.execute(
                                    "SELECT finished_at FROM scheduled_runs WHERE id = ?",
                                    (run_id,),
                                ).fetchone(),
                                (None,),
                            )

    def test_canonical_report_context_keeps_distinct_hash_domains(self) -> None:
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        material_digest = "b" * 64
        source_digest = "c" * 64
        record_sha256 = "d" * 64
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=now
            ) as clock, Journal.open(path) as journal:
                morning_claim = journal.claim_report(date(2026, 8, 14), "MORNING")
                assert morning_claim.claim_token is not None
                morning_report_id = stable_report_id(
                    "MORNING",
                    date(2026, 8, 14),
                    (),
                    state_sha256,
                )
                clock.return_value = now + timedelta(seconds=2)
                morning_report = journal.finalize_report(
                    claim_id=morning_claim.claim_id,
                    claim_token=morning_claim.claim_token,
                    body="# Morning\n",
                    state_sha256=state_sha256,
                    observation_ids=(),
                    archive_relative_path=report_archive_relative_path(
                        "MORNING",
                        date(2026, 8, 14),
                        morning_report_id,
                    ),
                    created_at=now + timedelta(seconds=2),
                    outbox_destination="TASK",
                    outbox_payload="morning",
                )
                journal._connection.execute(
                    "INSERT INTO canonical_report_contexts VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        morning_report.report_row_id,
                        "PREMARKET",
                        "2026-08-14T12:45:00.000000Z",
                        "2026-08-14T12:45:01.000000Z",
                        material_digest,
                        source_digest,
                        record_sha256,
                    ),
                )
                self.assertEqual(
                    journal._connection.execute(
                        "SELECT report.state_sha256, context.material_digest, "
                        "context.source_digest, context.record_sha256 "
                        "FROM canonical_report_contexts AS context "
                        "JOIN reports AS report ON report.id = context.report_id"
                    ).fetchone(),
                    (
                        state_sha256,
                        material_digest,
                        source_digest,
                        record_sha256,
                    ),
                )

                clock.return_value = now + timedelta(seconds=3)
                close_claim = journal.claim_report(date(2026, 8, 14), "CLOSE")
                assert close_claim.claim_token is not None
                close_report_id = stable_report_id(
                    "CLOSE",
                    date(2026, 8, 14),
                    (),
                    "e" * 64,
                )
                close_report = journal.finalize_report(
                    claim_id=close_claim.claim_id,
                    claim_token=close_claim.claim_token,
                    body="# Close\n",
                    state_sha256="e" * 64,
                    observation_ids=(),
                    archive_relative_path=report_archive_relative_path(
                        "CLOSE",
                        date(2026, 8, 14),
                        close_report_id,
                    ),
                    created_at=now + timedelta(seconds=3),
                    outbox_destination="TASK",
                    outbox_payload="close",
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    journal._connection.execute(
                        "INSERT INTO canonical_report_contexts VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            2,
                            close_report.report_row_id,
                            "PREMARKET",
                            "2026-08-14T12:45:00.000000Z",
                            "2026-08-14T12:45:01.000000Z",
                            "f" * 64,
                            "1" * 64,
                            "2" * 64,
                        ),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    journal._connection.execute(
                        "INSERT INTO canonical_report_contexts VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            2,
                            close_report.report_row_id,
                            "CLOSE",
                            "2026-08-14T12:45:00.000000Z",
                            "2026-08-14T12:45:04.000000Z",
                            "f" * 64,
                            "1" * 64,
                            "2" * 64,
                        ),
                    )

    def test_actual_close_schema_binds_receipts_failures_and_nonwidening_stops(
        self,
    ) -> None:
        timestamp = "2026-08-14T19:31:00.000000Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.execute(
                    "INSERT INTO source_observations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "a" * 64,
                        "b" * 64,
                        "https://data.alpaca.markets/v2/stocks/quotes",
                        "ALPACA_HISTORICAL_QUOTES",
                        "Alpaca",
                        "SIP",
                        "2026-08-14T19:14:00.000000Z",
                        timestamp,
                        None,
                        960,
                        "OK",
                        "{}",
                    ),
                )
                connection.execute(
                    "INSERT INTO actual_close_reviews VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "c" * 64,
                        "2026-08-14",
                        "2026-08-14T19:30:00.000000Z",
                        "2026-08-14T19:14:00.000000Z",
                        "2026-08-14T19:30:30.000000Z",
                        timestamp,
                        2,
                        "d" * 64,
                        "e" * 64,
                    ),
                )
                invalid_reviews = (
                    (
                        3,
                        "a" * 64,
                        "2026-08-16",
                        "2026-08-16T19:30:00.000000Z",
                        "2026-08-16T19:31:00.000000Z",
                        "2026-08-16T19:32:00.000000Z",
                        "2026-08-16T19:33:00.000000Z",
                        0,
                        "b" * 64,
                        "c" * 64,
                    ),
                    (
                        4,
                        "b" * 64,
                        "2026-08-17",
                        "2026-08-17T19:30:00.000000Z",
                        "2026-08-17T19:14:00.000000Z",
                        "2026-08-17T19:32:00.000000Z",
                        "2026-08-17T19:33:00.000000Z",
                        -1,
                        "c" * 64,
                        "d" * 64,
                    ),
                )
                for review in invalid_reviews:
                    with self.subTest(review=review[0]):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO actual_close_reviews VALUES "
                                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                review,
                            )
                connection.execute(
                    "INSERT INTO actual_close_source_bindings VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "c" * 64,
                        1,
                        "AAPL",
                        "SIP_QUOTE",
                        1,
                        None,
                        timestamp,
                        "f" * 64,
                    ),
                )
                connection.execute(
                    "INSERT INTO actual_close_source_bindings VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        2,
                        "c" * 64,
                        2,
                        None,
                        "OPERATIONAL_STATUS",
                        None,
                        "REFERENCE_UNAVAILABLE",
                        "2026-08-14T19:32:00.000000Z",
                        "1" * 64,
                    ),
                )

                invalid_bindings = (
                    (
                        3,
                        "c" * 64,
                        3,
                        "AAPL",
                        "SIP_SESSION_BAR",
                        1,
                        "ALSO_FAILED",
                        timestamp,
                        "2" * 64,
                    ),
                    (
                        4,
                        "c" * 64,
                        4,
                        "AAPL",
                        "EVENT_EVIDENCE",
                        None,
                        None,
                        timestamp,
                        "3" * 64,
                    ),
                    (
                        5,
                        "c" * 64,
                        5,
                        "AAPL",
                        "ATTACKER_ASSERTED_ROLE",
                        None,
                        "UNAVAILABLE",
                        timestamp,
                        "4" * 64,
                    ),
                    (
                        6,
                        "c" * 64,
                        6,
                        "AAPL",
                        "IEX_FRESHNESS",
                        1,
                        None,
                        "2026-08-14T19:31:01.000000Z",
                        "5" * 64,
                    ),
                    (
                        7,
                        "c" * 64,
                        7,
                        "AAPL",
                        "SIP_QUOTE",
                        None,
                        "DUPLICATE_ROLE",
                        "2026-08-14T19:32:00.000000Z",
                        "6" * 64,
                    ),
                )
                for binding in invalid_bindings:
                    with self.subTest(binding=binding[0]):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO actual_close_source_bindings VALUES "
                                "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                binding,
                            )

                connection.execute(
                    "INSERT INTO close_recommendations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        "7" * 64,
                        "c" * 64,
                        "2026-08-14",
                        "AAPL",
                        220_000_000,
                        "HOLD",
                        '["POSITION_REVIEW_COMPLETE"]',
                        "8" * 64,
                        "2026-08-14T19:32:00.000000Z",
                        "9" * 64,
                    ),
                )
                invalid_recommendations = (
                    (
                        3,
                        "a" * 64,
                        "c" * 64,
                        "2026-08-14",
                        "MSFT",
                        100_000_000,
                        "SELL_NOW",
                        '["POSITION_REVIEW_COMPLETE"]',
                        "b" * 64,
                        "2026-08-14T19:32:00.000000Z",
                        "c" * 64,
                    ),
                    (
                        4,
                        "b" * 64,
                        "c" * 64,
                        "2026-08-14",
                        "MSFT",
                        100_000_000,
                        "HOLD",
                        "{}",
                        "c" * 64,
                        "2026-08-14T19:32:00.000000Z",
                        "d" * 64,
                    ),
                    (
                        5,
                        "c" * 64,
                        "c" * 64,
                        "2026-08-15",
                        "MSFT",
                        100_000_000,
                        "HOLD",
                        '["POSITION_REVIEW_COMPLETE"]',
                        "d" * 64,
                        "2026-08-15T19:32:00.000000Z",
                        "e" * 64,
                    ),
                    (
                        6,
                        "d" * 64,
                        "c" * 64,
                        "2026-08-14",
                        "MSFT",
                        100_000_000,
                        "HOLD",
                        '["POSITION_REVIEW_COMPLETE"]',
                        "e" * 64,
                        "2026-08-14T19:30:59.999999Z",
                        "f" * 64,
                    ),
                )
                for recommendation in invalid_recommendations:
                    with self.subTest(recommendation=recommendation[0]):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO close_recommendations VALUES "
                                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                recommendation,
                            )
                connection.execute(
                    "INSERT INTO actual_close_reviews VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        2,
                        "1" * 64,
                        "2026-08-15",
                        "2026-08-15T19:30:00.000000Z",
                        "2026-08-15T19:14:00.000000Z",
                        "2026-08-15T19:30:30.000000Z",
                        "2026-08-15T19:31:00.000000Z",
                        0,
                        "2" * 64,
                        "3" * 64,
                    ),
                )
                next_recommendation = (
                    2,
                    "4" * 64,
                    "1" * 64,
                    "2026-08-15",
                    "AAPL",
                    219_990_000,
                    "HOLD",
                    '["POSITION_REVIEW_COMPLETE"]',
                    "5" * 64,
                    "2026-08-15T19:32:00.000000Z",
                    "6" * 64,
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO close_recommendations VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        next_recommendation,
                    )
                connection.execute(
                    "INSERT INTO close_recommendations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (*next_recommendation[:5], 220_010_000, *next_recommendation[6:]),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT recommended_stop_micros "
                        "FROM close_recommendations "
                        "WHERE symbol = 'AAPL' ORDER BY session_date"
                    ).fetchall(),
                    [(220_000_000,), (220_010_000,)],
                )

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
                self.assertEqual(journal.count("schema_migrations"), 5)

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
                connection.execute("PRAGMA user_version = 6")
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

            self.assertEqual(counts, (5, 5))
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("schema_migrations"), 5)

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
                        self.assertEqual(journal.count("schema_migrations"), 5)
                finally:
                    release_preflight.set()
                self.assertEqual(victim.result(timeout=10), 5)

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
                "canonical_report_contexts",
                "actual_close_reviews",
                "actual_close_source_bindings",
                "close_recommendations",
                "outbox",
                "outbox_delivery_attempts",
                "scheduled_runs",
                "ledger_postings",
                "actual_positions",
                "actual_cash_projection",
                "reconciliation_projection",
                "phase1_validation_windows",
                "phase1_source_payloads",
                "phase1_publication_manifests",
                "phase1_publication_fetch_pages",
                "phase1_publication_facts",
                "phase1_signals",
                "phase1_signal_evidence_reviews",
                "phase1_signal_evidence_bindings",
                "phase1_expiry_deadlines",
                "phase1_exit_reviews",
                "phase1_exit_review_manifests",
                "phase1_exit_review_pages",
                "phase1_exit_review_facts",
                "phase1_equity_mark_sets",
                "phase1_equity_mark_manifests",
                "phase1_equity_mark_pages",
                "phase1_equity_mark_facts",
                "phase1_equity_mark_invalidations",
                "phase1_equity_mark_invalidation_pages",
                "phase1_observation_fetch_manifests",
                "phase1_observation_fetch_pages",
                "phase1_session_late_evidence",
                "phase1_session_late_evidence_pages",
                "phase1_observations",
                "phase1_session_completions",
                "phase1_signal_events",
                "phase1_canonical_postings",
                "phase1_equity_points",
                "phase1_equity_point_marks",
                "phase1_closed_trades",
                "phase1_adherence_checks",
                "phase2_windows",
                "phase2_authorizations",
                "phase2_fee_schedules",
                "phase2_option_chain_sets",
                "phase2_option_chain_pages",
                "phase2_underlying_review_sets",
                "phase2_underlying_review_pages",
                "phase2_underlying_review_facts",
                "phase2_contract_snapshots",
                "phase2_contract_selections",
                "phase2_entries",
                "phase2_marks",
                "phase2_exit_reviews",
                "phase2_exits",
                "phase2_fee_records",
                "phase2_equity_points",
                "phase2_window_failures",
                "phase2_window_restarts",
                "phase2_adherence_checks",
                "phase2_gate_decisions",
                "historical_replay_runs",
                "historical_replay_dates",
                "historical_replay_evidence",
                "historical_replay_run_seals",
            },
        )
        self.assertIn("phase1_signals", tables)
        self.assertEqual(
            {table for table in tables if table.startswith("phase2_")},
            {
                "phase2_windows",
                "phase2_authorizations",
                "phase2_fee_schedules",
                "phase2_option_chain_sets",
                "phase2_option_chain_pages",
                "phase2_underlying_review_sets",
                "phase2_underlying_review_pages",
                "phase2_underlying_review_facts",
                "phase2_contract_snapshots",
                "phase2_contract_selections",
                "phase2_entries",
                "phase2_marks",
                "phase2_exit_reviews",
                "phase2_exits",
                "phase2_fee_records",
                "phase2_equity_points",
                "phase2_window_failures",
                "phase2_window_restarts",
                "phase2_adherence_checks",
                "phase2_gate_decisions",
            },
        )
        self.assertEqual(
            {table for table in tables if table.startswith("historical_replay_")},
            {
                "historical_replay_runs",
                "historical_replay_dates",
                "historical_replay_evidence",
                "historical_replay_run_seals",
            },
        )

    def test_phase2_schema_binds_promotion_signal_and_manual_source_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }
        self.assertIn("phase2_windows_validate_start_source", definitions)
        window_source = definitions["phase2_windows_validate_start_source"]
        self.assertIn("option_paper_window_start", window_source)
        self.assertIn("raw_message_id = new.start_raw_message_id", window_source)
        window_definition = definitions["phase2_windows"]
        self.assertIn("promotion_through_session", window_definition)
        authorization_source = definitions[
            "phase2_authorizations_validate_signal_lineage"
        ]
        self.assertIn("signal.role = 'primary'", authorization_source)
        self.assertIn(
            "signal.validation_window_id = window.phase1_validation_window_id",
            authorization_source,
        )
        self.assertIn(
            "signal.published_at > window.promotion_query_cutoff",
            authorization_source,
        )
        self.assertIn("signal.published_at > window.started_at", authorization_source)

        snapshot_definition = definitions["phase2_contract_snapshots"]
        self.assertIn("source_kind = 'provider_indicative'", snapshot_definition)
        self.assertIn("source_item_ordinal is not null", snapshot_definition)
        self.assertIn("open_interest is null", snapshot_definition)
        for optional_provider_field in (
            "delta_micros",
            "bid_micros",
            "ask_micros",
            "daily_volume",
            "observed_at",
        ):
            self.assertIn(
                f"{optional_provider_field} is null or",
                snapshot_definition,
            )
        self.assertIn("source_kind = 'manual_review'", snapshot_definition)
        self.assertIn("execution_event_id is not null", snapshot_definition)
        self.assertIn("open_interest is not null", snapshot_definition)
        self.assertIn("delta_micros is not null", snapshot_definition)
        self.assertIn("bid_micros is not null", snapshot_definition)
        self.assertIn("ask_micros is not null", snapshot_definition)
        self.assertIn("daily_volume is not null", snapshot_definition)
        self.assertIn("observed_at is not null", snapshot_definition)
        self.assertNotIn("tick_size_micros", snapshot_definition)
        selection_source = definitions[
            "phase2_contract_selections_validate_snapshots"
        ]
        for field in (
            "underlying",
            "expiration",
            "strike_micros",
            "delta_micros",
            "bid_micros",
            "ask_micros",
            "daily_volume",
        ):
            self.assertIn(
                f"provider.{field} = manual.{field}",
                selection_source,
            )
        self.assertIn(
            "provider.observed_at <= manual.observed_at",
            selection_source,
        )
        self.assertIn(
            "manual.observed_at < new.selected_at",
            selection_source,
        )
        manual_actions = {
            "phase2_contract_snapshots_validate_manual_source": "option_paper_review",
            "phase2_entries_validate_manual_source": "option_paper_open",
            "phase2_marks_validate_manual_source": "option_paper_mark",
            "phase2_exits_validate_manual_source": "option_paper_close",
            "phase2_window_restarts_validate_start_source": (
                "option_paper_window_start"
            ),
        }
        for trigger, action in manual_actions.items():
            with self.subTest(trigger=trigger):
                definition = definitions[trigger]
                self.assertIn(action, definition)
                self.assertIn("raw_message_id", definition)
        fee_source = definitions["phase2_fee_records_validate_actual_source"]
        self.assertIn("fee_kind = 'exit_actual'", fee_source)
        self.assertIn("parsed_action = 'fee'", fee_source)
        self.assertIn("raw_message_id", fee_source)
        self.assertIn("$.normalized.asset_id", fee_source)
        self.assertNotIn(" as real", fee_source)
        self.assertIn("substr(json_extract", fee_source)
        self.assertNotIn("= '-'", fee_source)
        self.assertIn("exit.exit_id = new.exit_id", fee_source)
        self.assertIn("event.event_time >= exit.exited_at", fee_source)
        self.assertIn(
            "execution_event_id integer unique",
            definitions["phase2_fee_records"],
        )
        mark_definition = definitions["phase2_marks"]
        for source_kind in (
            "manual_mark",
            "invalid_raw",
            "missing_deadline",
        ):
            self.assertIn(source_kind, mark_definition)
        self.assertIn("bid_micros is null", mark_definition)
        self.assertIn("liquidation_value_micros = 0", mark_definition)
        invalid_raw = definitions["phase2_marks_validate_invalid_raw_source"]
        self.assertIn("pending_clarification", invalid_raw)
        self.assertIn(
            "'option paper mark ' || manual.occ_symbol || ' *'",
            invalid_raw,
        )
        manual_mark = definitions["phase2_marks_validate_manual_source"]
        self.assertIn("$.normalized.occ_symbol", manual_mark)
        self.assertIn("manual.occ_symbol", manual_mark)
        missing = definitions["phase2_marks_validate_missing_deadline"]
        self.assertIn("calendar_digest = window.calendar_digest", missing)
        self.assertIn("deadline_at <= new.received_at", missing)
        one_open = definitions["phase2_entries_require_no_open_position"]
        self.assertIn("phase2_entries", one_open)
        self.assertIn("phase2_exits", one_open)
        self.assertNotIn("existing.window_id = new.window_id", one_open)
        exit_definition = definitions["phase2_exits"]
        for reason in ("stop", "target", "max_hold_10_sessions", "dte_21"):
            self.assertIn(reason, exit_definition)
        self.assertNotIn("'manual'", exit_definition)
        self.assertNotIn("'expiry'", exit_definition)
        window_definition = definitions["phase2_windows"]
        self.assertIn(
            "starting_capital_micros = 5000000000",
            window_definition,
        )
        self.assertIn(
            "restart_required",
            definitions["phase2_gate_decisions"],
        )
        self.assertIn(
            "quantity = 1",
            definitions["phase2_contract_selections"],
        )
        entry_definition = definitions["phase2_entries"]
        self.assertIn("quantity = 1", entry_definition)
        self.assertIn("all_in_initial_risk_micros <= 50000000", entry_definition)
        self.assertIn(
            "entry_ask_micros * 100 + entry_fee_micros + reserve_fee_micros",
            entry_definition,
        )
        fee_lineage = definitions["phase2_fee_records_validate_lineage"]
        self.assertIn("new.fee_kind = 'entry'", fee_lineage)
        self.assertIn("entry.entry_fee_micros", fee_lineage)
        self.assertIn("new.fee_kind = 'reserve'", fee_lineage)
        self.assertIn("entry.reserve_fee_micros", fee_lineage)
        archived_fee = definitions["phase2_fee_schedules"]
        self.assertIn("reviewed_bytes", archived_fee)
        self.assertIn("currency = 'usd'", archived_fee)
        self.assertIn("contract_multiplier = 100", archived_fee)
        selection_definition = definitions["phase2_contract_selections"]
        for field in (
            "event_exclusion_source_digest",
            "event_exclusion_authority_digest",
            "event_exclusion_row_references_json",
            "event_exclusion_highwaters_json",
            "selection_portfolio_source_digest",
            "selection_portfolio_query_cutoff",
            "selection_portfolio_row_references_json",
            "selection_portfolio_highwaters_json",
            "fee_schedule_id",
            "fee_schedule_digest",
        ):
            self.assertIn(field, selection_definition)

    def test_every_phase2_authority_table_requires_the_journal_write_gate(
        self,
    ) -> None:
        authority_tables = (
            "phase2_windows",
            "phase2_authorizations",
            "phase2_fee_schedules",
            "phase2_option_chain_sets",
            "phase2_option_chain_pages",
            "phase2_underlying_review_sets",
            "phase2_underlying_review_pages",
            "phase2_underlying_review_facts",
            "phase2_contract_snapshots",
            "phase2_contract_selections",
            "phase2_entries",
            "phase2_marks",
            "phase2_exit_reviews",
            "phase2_exits",
            "phase2_fee_records",
            "phase2_equity_points",
            "phase2_window_failures",
            "phase2_window_restarts",
            "phase2_adherence_checks",
            "phase2_gate_decisions",
            "historical_replay_runs",
            "historical_replay_dates",
            "historical_replay_evidence",
            "historical_replay_run_seals",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger'"
                    )
                    if row[1] is not None
                }
                columns_by_table = {
                    table: tuple(
                        str(row[1])
                        for row in connection.execute(
                            f"PRAGMA table_info({table})"
                        )
                        if str(row[1]) not in {"id", "record_sha256"}
                    )
                    for table in authority_tables
                }

        for table in authority_tables:
            with self.subTest(table=table):
                trigger = definitions.get(
                    f"{table}_require_journal_phase2_writer"
                )
                self.assertIsNotNone(trigger)
                assert trigger is not None
                material_arguments = ", ".join(
                    f"new.{column}" for column in columns_by_table[table]
                )
                self.assertIn(
                    "journal_phase2_write_allowed("
                    f"'{table}', new.record_sha256, {material_arguments})",
                    trigger,
                )

    def test_phase2_schema_persists_complete_chain_and_exit_decision_lineage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        chain = definitions["phase2_option_chain_sets"]
        for field in (
            "authorization_id",
            "underlying",
            "requested_symbols_json",
            "request_digest",
            "manifest_digest",
            "expected_page_count",
            "expected_fact_count",
            "review_candidate_fact_digests_json",
            "expected_manual_review_count",
            "terminal",
            "query_cutoff",
        ):
            self.assertIn(field, chain)
        self.assertIn(
            "json_extract(requested_symbols_json, '$[0]') = underlying",
            chain,
        )
        pages = definitions["phase2_option_chain_pages"]
        for field in (
            "chain_set_id",
            "page_ordinal",
            "source_observation_id",
            "external_source_observation_id",
            "request_url",
            "request_page_token",
            "next_page_token",
            "payload_sha256",
            "source_time",
            "retrieved_at",
        ):
            self.assertIn(field, pages)
        page_lineage = definitions["phase2_option_chain_pages_validate_lineage"]
        self.assertIn("prior.next_page_token = new.request_page_token", page_lineage)
        self.assertIn("new.page_ordinal = 1", page_lineage)

        snapshots = definitions["phase2_contract_snapshots"]
        self.assertIn("chain_set_id", snapshots)
        self.assertIn("source_kind = 'provider_indicative'", snapshots)
        self.assertIn("chain_set_id is not null", snapshots)
        self.assertIn("source_kind = 'manual_review'", snapshots)
        self.assertIn("chain_set_id is not null", snapshots)
        self.assertIn("reviewed_provider_snapshot_id is not null", snapshots)
        self.assertIn(
            "unique(chain_set_id, fetch_page_ordinal, source_item_ordinal, source_item_path)",
            snapshots,
        )
        completeness = definitions[
            "phase2_contract_selections_validate_chain_completeness"
        ]
        self.assertIn("count(*)", completeness)
        self.assertIn("expected_page_count", completeness)
        self.assertIn("expected_fact_count", completeness)
        self.assertIn("next_page_token is null", completeness)
        self.assertIn("provider.chain_set_id", completeness)
        self.assertIn("expected_manual_review_count", completeness)

        manual = definitions["phase2_contract_snapshots_validate_manual_source"]
        self.assertIn("option_paper_review", manual)
        self.assertNotIn("option_paper_open", manual)
        for normalized_field in (
            "occ_symbol",
            "delta",
            "open_interest",
            "volume",
        ):
            self.assertIn(f"$.normalized.{normalized_field}", manual)
        self.assertIn("review_candidate_fact_digests_json", manual)
        selection_definition = definitions["phase2_contract_selections"]
        self.assertIn("manual_review_terminal_cursor", selection_definition)
        self.assertIn("expected_manual_review_count", selection_definition)
        entry = definitions["phase2_entries_validate_manual_source"]
        self.assertIn("option_paper_open", entry)
        self.assertIn("event.action_ordinal = new.action_ordinal", entry)
        self.assertIn("action_source_digest", definitions["phase2_entries"])
        self.assertNotIn("event.id = manual.execution_event_id", entry)

        exit_definition = definitions["phase2_exits"]
        for field in (
            "action_ordinal",
            "action_source_digest",
            "underlying_review_set_id",
            "underlying_source_observation_id",
            "underlying_external_source_observation_id",
            "underlying_fetch_page_ordinal",
            "underlying_source_item_ordinal",
            "underlying_source_item_path",
            "underlying_payload_sha256",
            "underlying_fact_digest",
            "underlying_bar_at",
            "underlying_open_micros",
            "underlying_high_micros",
            "underlying_low_micros",
            "underlying_close_micros",
            "underlying_volume",
            "exit_decision_digest",
        ):
            self.assertIn(field, exit_definition)
        exit_lineage = definitions["phase2_exits_validate_underlying_source"]
        self.assertIn("phase1_source_payloads", exit_lineage)
        self.assertIn("source_observations", exit_lineage)
        self.assertIn("underlying_payload_sha256", exit_lineage)
        review = definitions["phase2_underlying_review_sets"]
        for field in (
            "entry_id",
            "underlying",
            "review_session",
            "timeframe",
            "adjustment",
            "feed",
            "requested_symbols_json",
            "request_digest",
            "manifest_digest",
            "expected_page_count",
            "expected_fact_count",
            "terminal",
            "request_start",
            "request_end",
            "query_cutoff",
        ):
            self.assertIn(field, review)
        self.assertIn("timeframe = '1min'", review)
        self.assertIn("adjustment = 'split'", review)
        self.assertIn("feed = 'sip'", review)
        review_pages = definitions["phase2_underlying_review_pages"]
        for field in (
            "review_set_id",
            "page_ordinal",
            "source_observation_id",
            "request_url",
            "request_page_token",
            "next_page_token",
            "payload_sha256",
        ):
            self.assertIn(field, review_pages)
        underlying_complete = definitions[
            "phase2_exits_validate_underlying_completeness"
        ]
        self.assertIn("expected_page_count", underlying_complete)
        self.assertIn("expected_fact_count", underlying_complete)
        self.assertIn("next_page_token is null", underlying_complete)
        review_facts = definitions["phase2_underlying_review_facts"]
        for field in (
            "review_set_id",
            "source_observation_id",
            "fetch_page_ordinal",
            "source_item_ordinal",
            "source_item_path",
            "payload_sha256",
            "symbol",
            "bar_at",
            "open_micros",
            "high_micros",
            "low_micros",
            "close_micros",
            "volume",
            "fact_digest",
        ):
            self.assertIn(field, review_facts)
        self.assertIn(
            "unique(review_set_id, fetch_page_ordinal, source_item_ordinal, source_item_path)",
            review_facts,
        )
        self.assertIn("underlying_review_fact_id", exit_definition)
        compact_exit_definition = " ".join(exit_definition.split())
        self.assertIn(
            "exit_reason in ('stop', 'target') "
            "and underlying_review_fact_id is not null",
            compact_exit_definition,
        )
        self.assertIn(
            "exit_reason in ('max_hold_10_sessions', 'dte_21') "
            "and underlying_review_fact_id is null",
            compact_exit_definition,
        )
        for predicate in (
            "fact.source_item_ordinal = new.underlying_source_item_ordinal",
            "fact.source_item_path = new.underlying_source_item_path",
            "fact.fact_digest = new.underlying_fact_digest",
            "fact.open_micros = new.underlying_open_micros",
            "fact.high_micros = new.underlying_high_micros",
            "fact.low_micros = new.underlying_low_micros",
            "fact.close_micros = new.underlying_close_micros",
            "fact.volume = new.underlying_volume",
        ):
            self.assertIn(predicate, exit_lineage)

    def test_phase2_schema_scopes_missing_marks_and_derives_genesis_equity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger', 'index')"
                    )
                    if row[1] is not None
                }

        mark = definitions["phase2_marks"]
        self.assertIn("deadline_start_at", mark)
        missing = definitions["phase2_marks_validate_missing_deadline"]
        self.assertIn("event.event_time >= new.deadline_start_at", missing)
        self.assertIn("event.event_time <= new.deadline_at", missing)
        genesis = definitions["phase2_equity_points_validate_start"]
        for predicate in (
            "new.cash_micros = 5000000000",
            "new.position_value_micros = 0",
            "new.equity_micros = 5000000000",
            "new.high_water_micros = 5000000000",
            "new.drawdown_micros = 0",
            "new.session_date = window.started_session",
            "new.at = window.started_at",
            "new.received_at = window.received_at",
        ):
            self.assertIn(predicate, genesis)
        self.assertIn(
            "phase2 start equity already exists",
            definitions["phase2_equity_points_one_start_per_window"],
        )
        unique_start = definitions[
            "phase2_equity_points_unique_start_per_window"
        ]
        self.assertIn("create unique index", unique_start)
        self.assertIn("where point_kind = 'start'", unique_start)

    def test_phase2_option_sale_settlement_gates_the_next_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        exit_definition = definitions["phase2_exits"]
        self.assertIn("settlement_available_session", exit_definition)
        self.assertIn("settlement_calendar_digest", exit_definition)
        self.assertIn(
            "settlement_available_session > substr(exited_at, 1, 10)",
            exit_definition,
        )
        settlement_source = definitions[
            "phase2_exits_validate_settlement_calendar"
        ]
        self.assertIn(
            "new.settlement_calendar_digest = window.calendar_digest",
            settlement_source,
        )
        next_entry = definitions[
            "phase2_entries_require_prior_exit_settlement"
        ]
        self.assertIn(
            "prior_exit.settlement_available_session > substr(new.entered_at, 1, 10)",
            next_entry,
        )
        self.assertNotIn("prior_entry.window_id = new.window_id", next_entry)

    def test_phase2_persists_every_exit_review_including_hold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        reviews = definitions["phase2_exit_reviews"]
        for field in (
            "exit_review_id",
            "window_id",
            "entry_id",
            "underlying_review_set_id",
            "review_session",
            "decision_kind",
            "decision_digest",
            "holding_sessions",
            "dte",
            "query_cutoff",
            "evaluated_at",
            "received_at",
        ):
            self.assertIn(field, reviews)
        for decision in (
            "hold",
            "stop",
            "target",
            "max_hold_10_sessions",
            "dte_21",
        ):
            self.assertIn(decision, reviews)
        compact_reviews = " ".join(reviews.split())
        self.assertIn(
            "decision_kind in ('stop', 'target') and decision_fact_id is not null",
            compact_reviews,
        )
        self.assertIn(
            "decision_kind in ('hold', 'max_hold_10_sessions', 'dte_21') "
            "and decision_fact_id is null",
            compact_reviews,
        )
        self.assertIn("unique(entry_id, review_session)", reviews)
        self.assertIn("exit_review_id", definitions["phase2_exits"])
        required_close = definitions["phase2_exits_validate_review_decision"]
        compact_required_close = " ".join(required_close.split())
        self.assertIn("review.decision_kind = new.exit_reason", required_close)
        self.assertIn(
            "review.decision_fact_id = new.underlying_review_fact_id",
            required_close,
        )
        self.assertIn(
            "review.decision_kind in ('max_hold_10_sessions', 'dte_21') "
            "and review.decision_fact_id is null "
            "and new.underlying_review_fact_id is null",
            compact_required_close,
        )
        self.assertIn("review.review_session = substr(new.exited_at, 1, 10)", required_close)
        self.assertIn("review.decision_kind <> 'hold'", required_close)
        next_review = definitions[
            "phase2_exit_reviews_reject_ignored_required_close"
        ]
        self.assertIn("prior.decision_kind <> 'hold'", next_review)
        self.assertIn("phase2_exits", next_review)
        gate = definitions["phase2_gate_decisions"]
        self.assertIn("expected_exit_review_count", gate)
        self.assertIn("actual_exit_review_count", gate)
        self.assertIn("exit_review_terminal_cursor", gate)
        coverage = definitions["phase2_gate_decisions_validate_exit_reviews"]
        self.assertIn("expected_exit_review_count", coverage)
        self.assertIn("decision_kind <> 'hold'", coverage)

    def test_phase2_gate_adherence_is_fixed_and_source_derived(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        checks = definitions["phase2_adherence_checks"]
        for field in (
            "check_id",
            "window_id",
            "entry_id",
            "check_name",
            "applicable",
            "passed",
            "hard_breach",
            "evidence_row_references_json",
            "evidence_highwaters_json",
            "evidence_digest",
            "authority_digest",
            "evaluated_at",
            "received_at",
            "source_digest",
            "record_sha256",
        ):
            self.assertIn(field, checks)
        for check_name in (
            "QUOTE_FRESHNESS",
            "CONTRACT_LIQUIDITY",
            "SETTLED_FUNDS_ELIGIBILITY",
            "EXPIRATION_WINDOW",
            "EVENT_EXCLUSION",
            "ONE_POSITION_LIMIT",
            "ALL_IN_INITIAL_RISK",
            "ENTRY_EXECUTION",
            "EXIT_EXECUTION",
            "RECORD_COMPLETENESS",
        ):
            self.assertIn(check_name.lower(), checks)
        self.assertIn(
            "journal_phase2_adherence_write_allowed",
            definitions["phase2_adherence_checks_require_derived_writer"],
        )
        gate = definitions["phase2_gate_decisions"]
        for field in (
            "elapsed_days",
            "expected_adherence_count",
            "adherence_terminal_cursor",
            "adherence_source_highwater",
        ):
            self.assertIn(field, gate)
        for passed_boundary in (
            "closed_trade_count >= 20",
            "elapsed_days >= 28",
            "mean_net_r_numerator_micros > 0",
            "max_drawdown_micros <= 250000000",
        ):
            self.assertIn(passed_boundary, gate)
        elapsed_gate = definitions[
            "phase2_gate_decisions_validate_elapsed_days"
        ]
        for predicate in (
            "new.elapsed_days",
            "window.started_session",
            "new.query_cutoff",
            "julianday",
        ):
            self.assertIn(predicate, elapsed_gate)
        source_gate = definitions[
            "phase2_gate_decisions_validate_adherence"
        ]
        for predicate in (
            "new.adherence_passed_count",
            "new.adherence_applicable_count",
            "new.hard_breach",
            "new.expected_adherence_count",
            "new.adherence_terminal_cursor",
            "new.adherence_source_highwater",
            "phase2_adherence_checks",
            "check_row.window_id = new.window_id",
            "check_row.received_at <= new.query_cutoff",
        ):
            self.assertIn(predicate, source_gate)

    def test_historical_replay_schema_requires_three_roles_and_cutoff_lineage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        self.assertIn("historical_replay_runs", definitions)
        self.assertIn("tier = 'strict_point_in_time'", definitions["historical_replay_runs"])
        date_definition = definitions["historical_replay_dates"]
        self.assertIn("expected_role_count = 3", date_definition)
        self.assertIn("expected_evidence_count = 3", date_definition)
        evidence_definition = definitions["historical_replay_evidence"]
        for role in ("universe_membership", "event_state", "source_evidence"):
            self.assertIn(role, evidence_definition)
        self.assertIn("source_kind", evidence_definition)
        self.assertIn("source_item_ordinal", evidence_definition)
        self.assertIn("source_item_path", evidence_definition)
        self.assertIn("authority_digest", evidence_definition)
        self.assertIn("published_at <= retrieved_at", evidence_definition)
        cutoff_trigger = definitions["historical_replay_evidence_validate_cutoff"]
        self.assertIn("retrieved_at <= date_source.report_cutoff", cutoff_trigger)
        self.assertIn("source_time <= new.retrieved_at", cutoff_trigger)

    def test_historical_replay_run_is_sealed_before_it_can_issue_authority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'trigger')"
                    )
                    if row[1] is not None
                }

        seal = definitions["historical_replay_run_seals"]
        for field in (
            "replay_run_id",
            "expected_date_count",
            "actual_date_count",
            "actual_evidence_count",
            "date_terminal_cursor",
            "evidence_terminal_cursor",
            "source_observation_highwater",
            "sealed_at",
            "source_digest",
            "record_sha256",
        ):
            self.assertIn(field, seal)
        self.assertIn(
            "historical_replay_dates_reject_after_seal",
            definitions,
        )
        self.assertIn(
            "historical_replay_evidence_reject_after_seal",
            definitions,
        )
        validate = definitions["historical_replay_run_seals_validate_counts"]
        self.assertIn("count(*)", validate)
        self.assertIn("max(replay_date.id)", validate)
        self.assertIn("max(evidence.id)", validate)
        self.assertIn("expected_date_count", validate)

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
                "result_envelope_json",
                "result_envelope_sha256",
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
            "phase1_signals": {
                "signal_id",
                "validation_window_id",
                "symbol",
                "role",
                "publication_session",
                "maximum_entry_micros",
                "recommended_stop_micros",
                "target_micros",
                "planned_shares",
                "trigger_price_micros",
                "publication_report_id",
                "publication_rank",
                "publication_source_digest",
                "publication_state_digest",
                "calendar_digest",
                "published_at",
                "received_at",
                "record_sha256",
            },
            "phase1_source_payloads": {
                "source_observation_id",
                "payload_sha256",
                "source_payload",
                "recorded_at",
                "record_sha256",
            },
            "phase1_publication_manifests": {
                "publication_report_id",
                "manifest_digest",
                "candidate_source_observation_ids_json",
                "candidate_context_digests_json",
                "source_observation_ids_json",
                "record_sha256",
            },
            "phase1_publication_fetch_pages": {
                "publication_report_id",
                "fetch_manifest_ordinal",
                "fetch_manifest_digest",
                "collection_name",
                "requested_symbols_json",
                "request_digest",
                "page_ordinal",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "terminal",
                "record_sha256",
            },
            "phase1_publication_facts": {
                "publication_report_id",
                "fact_ordinal",
                "candidate_symbol",
                "observation_kind",
                "symbol",
                "feed",
                "external_source_observation_id",
                "page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "page_payload_sha256",
                "normalized_fields_digest",
                "fetch_manifest_digest",
                "record_sha256",
            },
            "phase1_exit_reviews": {
                "review_id",
                "signal_id",
                "review_session",
                "calendar_digest",
                "query_cutoff",
                "expected_manifest_count",
                "expected_fact_count",
                "source_observation_highwater",
                "recorded_at",
                "source_digest",
                "record_sha256",
            },
            "phase1_exit_review_manifests": {
                "review_id",
                "purpose",
                "collection_name",
                "requested_symbols_json",
                "request_start",
                "request_end",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "terminal",
                "received_through",
                "expected_page_count",
                "expected_fact_count",
                "source_digest",
                "record_sha256",
            },
            "phase1_exit_review_pages": {
                "review_id",
                "purpose",
                "page_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "source_digest",
                "record_sha256",
            },
            "phase1_exit_review_facts": {
                "fact_id",
                "review_id",
                "purpose",
                "fact_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "fact_kind",
                "symbol",
                "feed",
                "source_time",
                "received_at",
                "provider_sequence",
                "payload_sha256",
                "normalized_fields_digest",
                "values_json",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_sets": {
                "mark_set_id",
                "session_date",
                "calendar_digest",
                "query_cutoff",
                "sealed_at",
                "requested_symbols_json",
                "expected_manifest_count",
                "expected_fact_count",
                "source_observation_highwater",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_manifests": {
                "mark_set_id",
                "purpose",
                "collection_name",
                "requested_symbols_json",
                "request_start",
                "request_end",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "terminal",
                "received_through",
                "expected_page_count",
                "expected_fact_count",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_pages": {
                "mark_set_id",
                "purpose",
                "page_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_facts": {
                "fact_id",
                "mark_set_id",
                "purpose",
                "fact_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "fact_kind",
                "symbol",
                "feed",
                "source_time",
                "received_at",
                "provider_sequence",
                "payload_sha256",
                "normalized_fields_digest",
                "values_json",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_invalidations": {
                "invalidation_id",
                "mark_set_id",
                "session_date",
                "semantic_manifest_digests_json",
                "received_through",
                "invalidated_at",
                "expected_page_count",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_mark_invalidation_pages": {
                "invalidation_id",
                "purpose",
                "page_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "semantic_manifest_digest",
                "source_digest",
                "record_sha256",
            },
            "phase1_observation_fetch_manifests": {
                "cohort_id",
                "signal_id",
                "session_date",
                "purpose",
                "collection_name",
                "requested_symbols_json",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "terminal",
                "request_start",
                "request_end",
                "calendar_digest",
                "received_through",
                "source_digest",
                "record_sha256",
            },
            "phase1_observation_fetch_pages": {
                "cohort_id",
                "page_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "semantic_page_digest",
                "record_sha256",
            },
            "phase1_session_late_evidence": {
                "late_evidence_id",
                "completion_id",
                "signal_id",
                "session_date",
                "collection_name",
                "request_start",
                "request_end",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "received_through",
                "source_digest",
                "record_sha256",
            },
            "phase1_session_late_evidence_pages": {
                "late_evidence_id",
                "page_ordinal",
                "source_observation_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "semantic_page_digest",
                "record_sha256",
            },
            "phase1_validation_windows": {
                "window_id",
                "started_session",
                "starting_capital_micros",
                "started_at",
                "received_at",
                "calendar_digest",
                "source_digest",
            },
            "phase1_observations": {
                "observation_id",
                "signal_id",
                "source_observation_id",
                "source_item_ordinal",
                "source_item_path",
                "source_payload_sha256",
                "source_payload",
                "stream_id",
                "feed",
                "observation_kind",
                "session_date",
                "source_time",
                "received_at",
                "provider_sequence",
                "source_ordinal",
                "cohort_ordinal",
                "trade_price_micros",
                "bid_micros",
                "ask_micros",
                "fresh",
                "fetch_cohort_id",
                "fetch_page_ordinal",
                "source_digest",
                "details_json",
            },
            "phase1_session_completions": {
                "completion_id",
                "signal_id",
                "session_date",
                "cohort_through_ordinal",
                "expected_observation_count",
                "received_through",
                "completed_at",
                "source_digest",
            },
            "phase1_signal_events": {
                "lifecycle_event_id",
                "signal_id",
                "event_ordinal",
                "event_kind",
                "from_status",
                "to_status",
                "event_time",
                "message_time",
                "received_at",
                "confirmation_execution_event_id",
                "trigger_observation_id",
                "quote_observation_id",
                "session_completion_id",
                "exit_observation_id",
                "exit_authority_digest",
                "shares",
                "price_micros",
                "recommended_stop_micros",
                "source_digest",
                "details_json",
            },
            "phase1_canonical_postings": {
                "posting_key",
                "lifecycle_event_id",
                "signal_id",
                "entry_kind",
                "account_name",
                "amount_micros",
                "shares_delta",
                "unit_price_micros",
                "occurred_at",
                "received_at",
                "settlement_available_session",
                "fee_schedule_version",
                "fee_schedule_digest",
                "source_digest",
                "record_sha256",
                "details_json",
            },
            "phase1_equity_points": {
                "point_id",
                "ledger_name",
                "session_date",
                "equity_micros",
                "cash_micros",
                "positions_value_micros",
                "external_cash_flow_micros",
                "source_cursor",
                "mark_source_digest",
                "at",
                "message_time",
                "received_at",
                "source_digest",
            },
            "phase1_equity_point_marks": {
                "equity_point_id",
                "observation_id",
                "symbol",
                "mark_ordinal",
                "method",
                "derived_price_micros",
                "mark_at",
                "source_digest",
            },
            "phase1_closed_trades": {
                "trade_id",
                "ledger_name",
                "signal_id",
                "lifecycle_event_id",
                "session_date",
                "shares",
                "entry_value_micros",
                "exit_value_micros",
                "fee_micros",
                "pnl_micros",
                "initial_risk_micros",
                "net_r_numerator_micros",
                "at",
                "message_time",
                "received_at",
                "source_digest",
            },
            "phase1_adherence_checks": {
                "check_id",
                "signal_id",
                "check_name",
                "applicable",
                "passed",
                "hard_breach",
                "evaluated_at",
                "received_at",
                "terminal_lifecycle_event_id",
                "evidence_digest",
                "review_source_digest",
                "authority_digest",
                "source_digest",
                "details_json",
                "record_sha256",
            },
            "phase2_windows": {
                "window_id",
                "phase1_validation_window_id",
                "promotion_source_digest",
                "promotion_decision_digest",
                "promotion_signal_ids_json",
                "promotion_through_session",
                "promotion_query_cutoff",
                "start_execution_event_id",
                "start_raw_message_id",
                "started_session",
                "started_at",
                "received_at",
                "starting_capital_micros",
                "calendar_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_authorizations": {
                "authorization_id",
                "window_id",
                "signal_id",
                "signal_source_digest",
                "authorization_digest",
                "authorized_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_fee_schedules": {
                "schedule_id",
                "effective_session",
                "reviewed_at",
                "currency",
                "contract_multiplier",
                "entry_fee_per_contract_micros",
                "exit_fee_per_contract_micros",
                "close_fee_reserve_per_contract_micros",
                "source_sha256",
                "schedule_digest",
                "reviewed_bytes",
                "archived_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_underlying_review_facts": {
                "fact_id",
                "review_set_id",
                "source_observation_id",
                "external_source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "symbol",
                "bar_at",
                "open_micros",
                "high_micros",
                "low_micros",
                "close_micros",
                "volume",
                "fact_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_contract_snapshots": {
                "snapshot_id",
                "authorization_id",
                "source_kind",
                "occ_symbol",
                "underlying",
                "expiration",
                "strike_micros",
                "delta_micros",
                "bid_micros",
                "ask_micros",
                "open_interest",
                "daily_volume",
                "source_observation_id",
                "external_source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "provider_fact_digest",
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
                "action_source_digest",
                "observed_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_contract_selections": {
                "selection_id",
                "authorization_id",
                "provider_snapshot_id",
                "manual_snapshot_id",
                "selection_session",
                "quantity",
                "fee_schedule_id",
                "fee_schedule_digest",
                "event_exclusion_source_digest",
                "event_exclusion_authority_digest",
                "event_exclusion_row_references_json",
                "event_exclusion_highwaters_json",
                "selected_at",
                "received_at",
                "ranking_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_entries": {
                "entry_id",
                "window_id",
                "selection_id",
                "execution_event_id",
                "raw_message_id",
                "quantity",
                "entry_ask_micros",
                "entry_fee_micros",
                "reserve_fee_micros",
                "all_in_initial_risk_micros",
                "fee_schedule_id",
                "fee_schedule_digest",
                "entered_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_marks": {
                "mark_id",
                "window_id",
                "entry_id",
                "session_date",
                "source_kind",
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
                "action_source_digest",
                "bid_micros",
                "ask_micros",
                "liquidation_value_micros",
                "valid",
                "failure_reason",
                "calendar_digest",
                "deadline_at",
                "marked_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_exit_reviews": {
                "exit_review_id",
                "window_id",
                "entry_id",
                "underlying_review_set_id",
                "decision_fact_id",
                "review_session",
                "decision_kind",
                "decision_digest",
                "holding_sessions",
                "dte",
                "query_cutoff",
                "evaluated_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_exits": {
                "exit_id",
                "exit_review_id",
                "window_id",
                "entry_id",
                "execution_event_id",
                "raw_message_id",
                "exit_reason",
                "bid_micros",
                "ask_micros",
                "gross_proceeds_micros",
                "net_pnl_micros",
                "net_r_numerator_micros",
                "initial_risk_micros",
                "underlying_review_set_id",
                "underlying_review_fact_id",
                "underlying_fact_digest",
                "exit_decision_digest",
                "settlement_available_session",
                "settlement_calendar_digest",
                "exited_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_fee_records": {
                "fee_id",
                "window_id",
                "entry_id",
                "exit_id",
                "fee_kind",
                "amount_micros",
                "fee_schedule_id",
                "fee_schedule_digest",
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
                "action_source_digest",
                "recorded_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_equity_points": {
                "point_id",
                "window_id",
                "entry_id",
                "mark_id",
                "exit_id",
                "session_date",
                "point_kind",
                "cash_micros",
                "position_value_micros",
                "equity_micros",
                "high_water_micros",
                "drawdown_micros",
                "at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_window_failures": {
                "failure_id",
                "window_id",
                "session_date",
                "reason_code",
                "mark_id",
                "detected_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_window_restarts": {
                "restart_id",
                "failed_window_id",
                "next_window_id",
                "start_execution_event_id",
                "start_raw_message_id",
                "restarted_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "phase2_gate_decisions": {
                "decision_id",
                "window_id",
                "query_cutoff",
                "status",
                "closed_trade_count",
                "elapsed_days",
                "mean_net_r_numerator_micros",
                "mean_net_r_denominator_micros",
                "adherence_passed_count",
                "adherence_applicable_count",
                "max_drawdown_micros",
                "hard_breach",
                "failure_id",
                "expected_exit_review_count",
                "actual_exit_review_count",
                "exit_review_terminal_cursor",
                "window_row_references_json",
                "window_highwaters_json",
                "window_source_digest",
                "evaluated_at",
                "received_at",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_runs": {
                "replay_run_id",
                "tier",
                "started_session",
                "ended_session",
                "query_cutoff",
                "calendar_digest",
                "policy_digest",
                "expected_date_count",
                "recorded_at",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_dates": {
                "replay_date_id",
                "replay_run_id",
                "session_date",
                "report_cutoff",
                "expected_role_count",
                "expected_evidence_count",
                "case_digest",
                "domain_input_digest",
                "mechanics_digest",
                "completion_digest",
                "completed_at",
                "recorded_at",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_evidence": {
                "replay_evidence_id",
                "replay_date_id",
                "role",
                "evidence_ordinal",
                "subject",
                "source_kind",
                "source_observation_id",
                "external_source_observation_id",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "content_sha256",
                "authority_digest",
                "effective_at",
                "published_at",
                "retrieved_at",
                "recorded_at",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_run_seals": {
                "replay_run_id",
                "expected_date_count",
                "actual_date_count",
                "actual_evidence_count",
                "date_terminal_cursor",
                "evidence_terminal_cursor",
                "source_observation_highwater",
                "sealed_at",
                "source_digest",
                "record_sha256",
            },
        }
        for table, required in expected_columns.items():
            with self.subTest(table=table):
                self.assertLessEqual(required, columns[table].keys())
        for table in columns.values():
            for name, type_name in table.items():
                if name.endswith("_micros"):
                    self.assertEqual(type_name, "INTEGER")
        prohibited_phase2_fields = {
            "broker",
            "brokerage",
            "client_order_id",
            "limit_price",
            "order",
            "order_id",
            "order_type",
            "route",
            "side",
            "stop_price",
            "time_in_force",
        }
        for table, table_columns in columns.items():
            if not table.startswith("phase2_"):
                continue
            for name in table_columns:
                with self.subTest(table=table, prohibited_field=name):
                    self.assertFalse(
                        name in prohibited_phase2_fields
                        or any(
                            token in name.split("_")
                            for token in {"broker", "brokerage", "order", "route"}
                        )
                    )

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
            "scheduled_runs": {"result_envelope_sha256"},
            "canonical_report_contexts": {
                "material_digest",
                "source_digest",
                "record_sha256",
            },
            "actual_close_reviews": {
                "review_id",
                "source_digest",
                "record_sha256",
            },
            "actual_close_source_bindings": {"record_sha256"},
            "close_recommendations": {
                "recommendation_id",
                "source_digest",
                "record_sha256",
            },
            "phase1_signals": {
                "publication_source_digest",
                "publication_state_digest",
                "calendar_digest",
                "record_sha256",
            },
            "phase1_validation_windows": {
                "calendar_digest",
                "source_digest",
            },
            "phase1_observation_fetch_manifests": {
                "cohort_id",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "calendar_digest",
                "source_digest",
                "record_sha256",
            },
            "phase1_observation_fetch_pages": {
                "payload_sha256",
                "semantic_page_digest",
                "record_sha256",
            },
            "phase1_session_late_evidence": {
                "late_evidence_id",
                "request_digest",
                "manifest_digest",
                "semantic_manifest_digest",
                "source_digest",
                "record_sha256",
            },
            "phase1_session_late_evidence_pages": {
                "payload_sha256",
                "semantic_page_digest",
                "record_sha256",
            },
            "phase1_observations": {
                "source_payload_sha256",
                "source_digest",
            },
            "phase1_session_completions": {"source_digest"},
            "phase1_signal_events": {"source_digest"},
            "phase1_canonical_postings": {
                "fee_schedule_digest",
                "source_digest",
                "record_sha256",
            },
            "phase1_equity_points": {
                "mark_source_digest",
                "source_digest",
            },
            "phase1_closed_trades": {"source_digest"},
            "phase1_adherence_checks": {
                "evidence_digest",
                "review_source_digest",
                "authority_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_windows": {
                "window_id",
                "promotion_source_digest",
                "promotion_decision_digest",
                "calendar_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_authorizations": {
                "authorization_id",
                "signal_source_digest",
                "authorization_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_fee_schedules": {
                "source_sha256",
                "schedule_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_underlying_review_facts": {
                "fact_id",
                "payload_sha256",
                "fact_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_contract_snapshots": {
                "snapshot_id",
                "payload_sha256",
                "provider_fact_digest",
                "action_source_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_contract_selections": {
                "selection_id",
                "fee_schedule_digest",
                "event_exclusion_source_digest",
                "event_exclusion_authority_digest",
                "ranking_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_entries": {
                "entry_id",
                "fee_schedule_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_marks": {
                "mark_id",
                "action_source_digest",
                "calendar_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_exit_reviews": {
                "exit_review_id",
                "decision_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_exits": {
                "exit_id",
                "underlying_payload_sha256",
                "underlying_fact_digest",
                "exit_decision_digest",
                "settlement_calendar_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_fee_records": {
                "fee_id",
                "fee_schedule_digest",
                "action_source_digest",
                "source_digest",
                "record_sha256",
            },
            "phase2_equity_points": {
                "point_id",
                "source_digest",
                "record_sha256",
            },
            "phase2_window_failures": {
                "failure_id",
                "source_digest",
                "record_sha256",
            },
            "phase2_window_restarts": {
                "restart_id",
                "source_digest",
                "record_sha256",
            },
            "phase2_gate_decisions": {
                "decision_id",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_runs": {
                "replay_run_id",
                "calendar_digest",
                "policy_digest",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_dates": {
                "replay_date_id",
                "case_digest",
                "domain_input_digest",
                "mechanics_digest",
                "completion_digest",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_evidence": {
                "replay_evidence_id",
                "payload_sha256",
                "content_sha256",
                "authority_digest",
                "source_digest",
                "record_sha256",
            },
            "historical_replay_run_seals": {
                "source_digest",
                "record_sha256",
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

    def test_phase1_schema_enforces_primary_and_lifecycle_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                indexes = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type = 'index' AND tbl_name = 'phase1_signals' "
                        "AND sql IS NOT NULL"
                    )
                }
                event_definition = str(
                    connection.execute(
                        "SELECT sql FROM sqlite_schema "
                        "WHERE type = 'table' AND name = 'phase1_signal_events'"
                    ).fetchone()[0]
                ).lower()

        self.assertTrue(
            any(
                "unique" in sql
                and "publication_session" in sql
                and "where role = 'primary'" in sql
                for sql in indexes.values()
            )
        )
        self.assertIn("check(from_status is null or from_status in", event_definition)
        self.assertIn("check(to_status in", event_definition)

    def test_phase1_closed_trade_lineage_uses_the_close_event_kind(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                trigger_sql = str(
                    connection.execute(
                        "SELECT sql FROM sqlite_schema WHERE type = 'trigger' "
                        "AND name = 'phase1_closed_trades_validate_lineage'"
                    ).fetchone()[0]
                ).upper()

        self.assertIn("EVENT_KIND = 'CLOSE'", trigger_sql)
        self.assertNotIn("EVENT_KIND = 'CLOSED'", trigger_sql)

    def test_phase1_lifecycle_shapes_bind_finalizers_and_exit_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                table_sql = str(
                    connection.execute(
                        "SELECT sql FROM sqlite_schema WHERE type = 'table' "
                        "AND name = 'phase1_signal_events'"
                    ).fetchone()[0]
                ).lower()
                posting_trigger_sql = str(
                    connection.execute(
                        "SELECT sql FROM sqlite_schema WHERE type = 'trigger' "
                        "AND name = 'phase1_canonical_postings_validate_lineage'"
                    ).fetchone()[0]
                ).lower()
                trigger_sql = str(
                    connection.execute(
                        "SELECT sql FROM sqlite_schema WHERE type = 'trigger' "
                        "AND name = 'phase1_signal_events_validate_sequence'"
                    ).fetchone()[0]
                ).lower()

        for field in (
            "session_completion_id",
            "exit_observation_id",
            "exit_authority_digest",
        ):
            with self.subTest(field=field):
                self.assertIn(field, table_sql)
        self.assertIn("event_kind = 'trigger_observed'", table_sql)
        self.assertIn("event_kind = 'paper_fill'", table_sql)
        self.assertIn("'partial_exit', 'close'", table_sql)
        self.assertIn("session_completion_id is not null", table_sql)
        self.assertIn("exit_observation_id is not null", table_sql)
        self.assertIn("exit_authority_digest is not null", table_sql)
        self.assertIn("completion.signal_id = new.signal_id", trigger_sql)
        self.assertIn(
            "completion.session_date = signal.publication_session",
            trigger_sql,
        )
        self.assertIn("review.signal_id = new.signal_id", trigger_sql)
        self.assertIn("fact.fact_kind = 'bar'", trigger_sql)
        self.assertIn("signal.role != 'primary'", trigger_sql)
        self.assertIn("signal.role = 'primary'", posting_trigger_sql)
        self.assertIn("event_kind in ('live_confirm', 'live_skip')", posting_trigger_sql)

    def test_phase1_schema_locks_terminal_marks_observation_shapes_and_net_r(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path)) as connection:
                definitions = {
                    str(row[0]): str(row[1]).lower()
                    for row in connection.execute(
                        "SELECT name, sql FROM sqlite_schema "
                        "WHERE type IN ('table', 'index') AND sql IS NOT NULL"
                    )
                }

        equity_contract = " ".join(
            sql
            for name, sql in definitions.items()
            if name == "phase1_equity_points"
            or name.startswith("phase1_equity_points_")
        )
        observation_contract = definitions["phase1_observations"]
        close_contract = definitions["phase1_closed_trades"]
        self.assertIn(
            "unique(validation_window_id, ledger_name, session_date)",
            equity_contract,
        )
        self.assertIn("observation_kind = 'bar'", observation_contract)
        self.assertIn("open_micros is not null", observation_contract)
        self.assertIn(
            "observation_kind in ('trade', 'quote', 'bar')",
            observation_contract,
        )
        self.assertNotIn("observation_kind = 'mark'", observation_contract)
        self.assertIn("bid_micros is not null", observation_contract)
        self.assertIn("ask_micros is not null", observation_contract)
        self.assertIn("ask_micros >= bid_micros", observation_contract)
        self.assertIn("and close_micros is null", observation_contract)
        self.assertIn("and close_micros is not null", observation_contract)
        self.assertIn("initial_risk_micros", close_contract)
        self.assertIn(
            "net_r_numerator_micros = pnl_micros",
            close_contract,
        )
        self.assertNotIn("equity_micros >= 0", definitions["phase1_equity_points"])
        self.assertNotIn("cash_micros >= 0", definitions["phase1_equity_points"])

    def test_every_immutable_table_rejects_update_delete_and_replace(self) -> None:
        immutable_tables = (
            "schema_migrations",
            "raw_messages",
            "source_observations",
            "execution_events",
            "account_checks",
            "reports",
            "report_observations",
            "canonical_report_contexts",
            "actual_close_reviews",
            "actual_close_source_bindings",
            "close_recommendations",
            "outbox",
            "outbox_delivery_attempts",
            "ledger_postings",
            "phase1_validation_windows",
            "phase1_source_payloads",
            "phase1_publication_manifests",
            "phase1_publication_fetch_pages",
            "phase1_publication_facts",
            "phase1_signals",
            "phase1_signal_evidence_reviews",
            "phase1_signal_evidence_bindings",
            "phase1_expiry_deadlines",
            "phase1_exit_reviews",
            "phase1_exit_review_manifests",
            "phase1_exit_review_pages",
            "phase1_exit_review_facts",
            "phase1_equity_mark_sets",
            "phase1_equity_mark_manifests",
            "phase1_equity_mark_pages",
            "phase1_equity_mark_facts",
            "phase1_equity_mark_invalidations",
            "phase1_equity_mark_invalidation_pages",
            "phase1_observation_fetch_manifests",
            "phase1_observation_fetch_pages",
            "phase1_session_late_evidence",
            "phase1_session_late_evidence_pages",
            "phase1_observations",
            "phase1_session_completions",
            "phase1_signal_events",
            "phase1_canonical_postings",
            "phase1_equity_points",
            "phase1_equity_point_marks",
            "phase1_closed_trades",
            "phase1_adherence_checks",
            "phase2_windows",
            "phase2_authorizations",
            "phase2_fee_schedules",
            "phase2_option_chain_sets",
            "phase2_option_chain_pages",
            "phase2_underlying_review_sets",
            "phase2_underlying_review_pages",
            "phase2_underlying_review_facts",
            "phase2_contract_snapshots",
            "phase2_contract_selections",
            "phase2_entries",
            "phase2_marks",
            "phase2_exit_reviews",
            "phase2_exits",
            "phase2_fee_records",
            "phase2_equity_points",
            "phase2_window_failures",
            "phase2_window_restarts",
            "phase2_gate_decisions",
            "historical_replay_runs",
            "historical_replay_dates",
            "historical_replay_evidence",
            "historical_replay_run_seals",
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

                trigger_names = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'trigger'"
                    )
                }

                for table in immutable_tables:
                    primary_key = "version" if table == "schema_migrations" else "id"
                    with self.subTest(table=table, operation="trigger-matrix"):
                        self.assertIn(f"{table}_no_update", trigger_names)
                        self.assertIn(f"{table}_no_delete", trigger_names)
                        self.assertIn(
                            f"{table}_no_conflicting_insert",
                            trigger_names,
                        )
                    if connection.execute(
                        f'SELECT COUNT(*) FROM "{table}"'
                    ).fetchone()[0] == 0:
                        continue
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
            "canonical_report_contexts": {("report_id", "reports")},
            "actual_close_source_bindings": {
                ("review_id", "actual_close_reviews"),
                ("source_observation_id", "source_observations"),
            },
            "close_recommendations": {
                ("review_id", "actual_close_reviews"),
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
            "phase1_source_payloads": {
                ("source_observation_id", "source_observations"),
            },
            "phase1_publication_manifests": {
                ("publication_report_id", "reports"),
            },
            "phase1_publication_fetch_pages": {
                ("publication_report_id", "phase1_publication_manifests"),
            },
            "phase1_publication_facts": {
                ("publication_report_id", "phase1_publication_manifests"),
                ("publication_report_id", "phase1_publication_fetch_pages"),
                ("external_source_observation_id", "phase1_publication_fetch_pages"),
                ("fetch_manifest_digest", "phase1_publication_fetch_pages"),
                ("page_ordinal", "phase1_publication_fetch_pages"),
            },
            "phase1_signals": {
                ("validation_window_id", "phase1_validation_windows"),
                ("publication_report_id", "reports"),
            },
            "phase1_signal_evidence_reviews": {
                ("signal_id", "phase1_signals"),
                ("registry_source_row_id", "source_observations"),
            },
            "phase1_signal_evidence_bindings": {
                ("evidence_id", "phase1_signal_evidence_reviews"),
                ("source_observation_row_id", "source_observations"),
            },
            "phase1_expiry_deadlines": {
                ("signal_id", "phase1_signals"),
            },
            "phase1_exit_reviews": {
                ("signal_id", "phase1_signals"),
            },
            "phase1_exit_review_manifests": {
                ("review_id", "phase1_exit_reviews"),
            },
            "phase1_exit_review_pages": {
                ("review_id", "phase1_exit_review_manifests"),
                ("purpose", "phase1_exit_review_manifests"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase1_exit_review_facts": {
                ("review_id", "phase1_exit_review_pages"),
                ("purpose", "phase1_exit_review_pages"),
                ("page_ordinal", "phase1_exit_review_pages"),
                ("source_observation_id", "phase1_exit_review_pages"),
            },
            "phase1_equity_mark_manifests": {
                ("mark_set_id", "phase1_equity_mark_sets"),
            },
            "phase1_equity_mark_pages": {
                ("mark_set_id", "phase1_equity_mark_manifests"),
                ("purpose", "phase1_equity_mark_manifests"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase1_equity_mark_facts": {
                ("mark_set_id", "phase1_equity_mark_pages"),
                ("purpose", "phase1_equity_mark_pages"),
                ("page_ordinal", "phase1_equity_mark_pages"),
                ("source_observation_id", "phase1_equity_mark_pages"),
            },
            "phase1_equity_mark_invalidations": {
                ("mark_set_id", "phase1_equity_mark_sets"),
            },
            "phase1_equity_mark_invalidation_pages": {
                (
                    "invalidation_id",
                    "phase1_equity_mark_invalidations",
                ),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase1_observation_fetch_manifests": {
                ("signal_id", "phase1_signals"),
            },
            "phase1_observation_fetch_pages": {
                ("cohort_id", "phase1_observation_fetch_manifests"),
                ("source_observation_id", "source_observations"),
            },
            "phase1_session_late_evidence": {
                ("completion_id", "phase1_session_completions"),
                ("signal_id", "phase1_signals"),
            },
            "phase1_session_late_evidence_pages": {
                ("late_evidence_id", "phase1_session_late_evidence"),
                ("source_observation_id", "source_observations"),
            },
            "phase1_observations": {
                ("signal_id", "phase1_signals"),
                ("source_observation_id", "source_observations"),
                ("fetch_cohort_id", "phase1_observation_fetch_manifests"),
                ("fetch_cohort_id", "phase1_observation_fetch_pages"),
                ("fetch_page_ordinal", "phase1_observation_fetch_pages"),
                ("source_observation_id", "phase1_observation_fetch_pages"),
            },
            "phase1_session_completions": {
                ("signal_id", "phase1_signals"),
            },
            "phase1_signal_events": {
                ("signal_id", "phase1_signals"),
                ("confirmation_execution_event_id", "execution_events"),
                ("trigger_observation_id", "phase1_observations"),
                ("quote_observation_id", "phase1_observations"),
                ("session_completion_id", "phase1_session_completions"),
                ("exit_observation_id", "phase1_exit_review_facts"),
                ("signal_evidence_id", "phase1_signal_evidence_reviews"),
                ("expiry_source_id", "phase1_expiry_deadlines"),
            },
            "phase1_canonical_postings": {
                ("lifecycle_event_id", "phase1_signal_events"),
                ("signal_id", "phase1_signals"),
            },
            "phase1_closed_trades": {
                ("validation_window_id", "phase1_validation_windows"),
                ("signal_id", "phase1_signals"),
                ("lifecycle_event_id", "phase1_signal_events"),
            },
            "phase1_adherence_checks": {
                ("validation_window_id", "phase1_validation_windows"),
                ("signal_id", "phase1_signals"),
                ("terminal_lifecycle_event_id", "phase1_signal_events"),
            },
            "phase1_equity_points": {
                ("validation_window_id", "phase1_validation_windows"),
            },
            "phase1_equity_point_marks": {
                ("equity_point_id", "phase1_equity_points"),
                ("observation_id", "phase1_equity_mark_facts"),
            },
            "phase2_windows": {
                ("phase1_validation_window_id", "phase1_validation_windows"),
                ("start_execution_event_id", "execution_events"),
                ("start_raw_message_id", "raw_messages"),
            },
            "phase2_authorizations": {
                ("window_id", "phase2_windows"),
                ("signal_id", "phase1_signals"),
            },
            "phase2_option_chain_sets": {
                ("authorization_id", "phase2_authorizations"),
            },
            "phase2_option_chain_pages": {
                ("chain_set_id", "phase2_option_chain_sets"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase2_underlying_review_sets": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
            },
            "phase2_underlying_review_pages": {
                ("review_set_id", "phase2_underlying_review_sets"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase2_underlying_review_facts": {
                ("review_set_id", "phase2_underlying_review_sets"),
                ("review_set_id", "phase2_underlying_review_pages"),
                ("fetch_page_ordinal", "phase2_underlying_review_pages"),
                ("source_observation_id", "phase2_underlying_review_pages"),
                ("payload_sha256", "phase2_underlying_review_pages"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "phase2_contract_snapshots": {
                ("authorization_id", "phase2_authorizations"),
                ("chain_set_id", "phase2_option_chain_sets"),
                ("reviewed_provider_snapshot_id", "phase2_contract_snapshots"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
                ("execution_event_id", "execution_events"),
                ("raw_message_id", "raw_messages"),
            },
            "phase2_contract_selections": {
                ("authorization_id", "phase2_authorizations"),
                ("provider_snapshot_id", "phase2_contract_snapshots"),
                ("manual_snapshot_id", "phase2_contract_snapshots"),
                ("fee_schedule_id", "phase2_fee_schedules"),
                ("fee_schedule_digest", "phase2_fee_schedules"),
            },
            "phase2_entries": {
                ("window_id", "phase2_windows"),
                ("selection_id", "phase2_contract_selections"),
                ("execution_event_id", "execution_events"),
                ("raw_message_id", "raw_messages"),
                ("fee_schedule_id", "phase2_fee_schedules"),
                ("fee_schedule_digest", "phase2_fee_schedules"),
            },
            "phase2_marks": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
                ("execution_event_id", "execution_events"),
                ("raw_message_id", "raw_messages"),
            },
            "phase2_exit_reviews": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
                ("underlying_review_set_id", "phase2_underlying_review_sets"),
                ("decision_fact_id", "phase2_underlying_review_facts"),
            },
            "phase2_exits": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
                ("exit_review_id", "phase2_exit_reviews"),
                ("execution_event_id", "execution_events"),
                ("raw_message_id", "raw_messages"),
                ("underlying_review_set_id", "phase2_underlying_review_sets"),
                ("underlying_review_fact_id", "phase2_underlying_review_facts"),
                ("underlying_source_observation_id", "source_observations"),
                ("underlying_source_observation_id", "phase1_source_payloads"),
                ("underlying_payload_sha256", "phase1_source_payloads"),
            },
            "phase2_fee_records": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
                ("exit_id", "phase2_exits"),
                ("execution_event_id", "execution_events"),
                ("raw_message_id", "raw_messages"),
                ("fee_schedule_id", "phase2_fee_schedules"),
                ("fee_schedule_digest", "phase2_fee_schedules"),
            },
            "phase2_equity_points": {
                ("window_id", "phase2_windows"),
                ("entry_id", "phase2_entries"),
                ("mark_id", "phase2_marks"),
                ("exit_id", "phase2_exits"),
            },
            "phase2_window_failures": {
                ("window_id", "phase2_windows"),
                ("mark_id", "phase2_marks"),
            },
            "phase2_window_restarts": {
                ("failed_window_id", "phase2_windows"),
                ("next_window_id", "phase2_windows"),
                ("start_execution_event_id", "execution_events"),
                ("start_raw_message_id", "raw_messages"),
            },
            "phase2_gate_decisions": {
                ("window_id", "phase2_windows"),
                ("failure_id", "phase2_window_failures"),
            },
            "historical_replay_dates": {
                ("replay_run_id", "historical_replay_runs"),
            },
            "historical_replay_evidence": {
                ("replay_date_id", "historical_replay_dates"),
                ("source_observation_id", "source_observations"),
                ("source_observation_id", "phase1_source_payloads"),
                ("payload_sha256", "phase1_source_payloads"),
            },
            "historical_replay_run_seals": {
                ("replay_run_id", "historical_replay_runs"),
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

                fact_foreign_keys = connection.execute(
                    'PRAGMA foreign_key_list("phase1_publication_facts")'
                ).fetchall()
                grouped: dict[int, list[tuple[int, str, str, str]]] = {}
                for row in fact_foreign_keys:
                    grouped.setdefault(int(row[0]), []).append(
                        (int(row[1]), str(row[2]), str(row[3]), str(row[4]))
                    )
                self.assertIn(
                    [
                        (
                            0,
                            "phase1_publication_fetch_pages",
                            "publication_report_id",
                            "publication_report_id",
                        ),
                        (
                            1,
                            "phase1_publication_fetch_pages",
                            "external_source_observation_id",
                            "external_source_observation_id",
                        ),
                        (
                            2,
                            "phase1_publication_fetch_pages",
                            "fetch_manifest_digest",
                            "fetch_manifest_digest",
                        ),
                        (
                            3,
                            "phase1_publication_fetch_pages",
                            "page_ordinal",
                            "page_ordinal",
                        ),
                    ],
                    [sorted(rows) for rows in grouped.values()],
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
            "phase1_validation_windows": {"starting_capital_micros"},
            "phase1_signals": {
                "maximum_entry_micros",
                "recommended_stop_micros",
                "target_micros",
                "planned_shares",
                "trigger_price_micros",
                "publication_rank",
            },
            "phase1_observation_fetch_manifests": {"terminal"},
            "phase1_observation_fetch_pages": {
                "page_ordinal",
                "source_observation_id",
            },
            "phase1_session_late_evidence_pages": {
                "page_ordinal",
                "source_observation_id",
            },
            "phase1_observations": {
                "source_observation_id",
                "source_item_ordinal",
                "provider_sequence",
                "source_ordinal",
                "cohort_ordinal",
                "trade_price_micros",
                "bid_micros",
                "ask_micros",
                "open_micros",
                "high_micros",
                "low_micros",
                "close_micros",
                "volume",
                "fresh",
                "fetch_page_ordinal",
            },
            "phase1_session_completions": {
                "cohort_through_ordinal",
                "expected_observation_count",
            },
            "phase1_signal_events": {
                "event_ordinal",
                "shares",
                "price_micros",
            },
            "phase1_canonical_postings": {
                "amount_micros",
                "shares_delta",
                "unit_price_micros",
            },
            "phase1_equity_points": {
                "equity_micros",
                "cash_micros",
                "positions_value_micros",
                "external_cash_flow_micros",
                "source_cursor",
            },
            "phase1_closed_trades": {
                "shares",
                "entry_value_micros",
                "exit_value_micros",
                "fee_micros",
                "pnl_micros",
                "initial_risk_micros",
                "net_r_numerator_micros",
            },
            "phase1_adherence_checks": {
                "applicable",
                "passed",
                "hard_breach",
            },
            "phase2_windows": {
                "start_execution_event_id",
                "start_raw_message_id",
                "starting_capital_micros",
            },
            "phase2_underlying_review_sets": {
                "expected_page_count",
                "expected_fact_count",
                "terminal",
            },
            "phase2_underlying_review_pages": {
                "page_ordinal",
                "source_observation_id",
            },
            "phase2_underlying_review_facts": {
                "source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "open_micros",
                "high_micros",
                "low_micros",
                "close_micros",
                "volume",
            },
            "phase2_contract_snapshots": {
                "strike_micros",
                "delta_micros",
                "bid_micros",
                "ask_micros",
                "open_interest",
                "daily_volume",
                "source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
            },
            "phase2_contract_selections": {"quantity"},
            "phase2_fee_schedules": {
                "contract_multiplier",
                "entry_fee_per_contract_micros",
                "exit_fee_per_contract_micros",
                "close_fee_reserve_per_contract_micros",
            },
            "phase2_entries": {
                "execution_event_id",
                "raw_message_id",
                "quantity",
                "entry_ask_micros",
                "entry_fee_micros",
                "reserve_fee_micros",
                "all_in_initial_risk_micros",
            },
            "phase2_marks": {
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
                "bid_micros",
                "ask_micros",
                "liquidation_value_micros",
                "valid",
            },
            "phase2_exit_reviews": {
                "holding_sessions",
                "dte",
            },
            "phase2_exits": {
                "execution_event_id",
                "raw_message_id",
                "bid_micros",
                "ask_micros",
                "gross_proceeds_micros",
                "net_pnl_micros",
                "net_r_numerator_micros",
                "initial_risk_micros",
            },
            "phase2_fee_records": {
                "amount_micros",
                "execution_event_id",
                "raw_message_id",
                "action_ordinal",
            },
            "phase2_equity_points": {
                "cash_micros",
                "position_value_micros",
                "equity_micros",
                "high_water_micros",
                "drawdown_micros",
            },
            "phase2_window_restarts": {
                "start_execution_event_id",
                "start_raw_message_id",
            },
            "phase2_gate_decisions": {
                "closed_trade_count",
                "elapsed_days",
                "mean_net_r_numerator_micros",
                "mean_net_r_denominator_micros",
                "adherence_passed_count",
                "adherence_applicable_count",
                "max_drawdown_micros",
                "hard_breach",
                "expected_exit_review_count",
                "actual_exit_review_count",
                "exit_review_terminal_cursor",
            },
            "historical_replay_runs": {"expected_date_count"},
            "historical_replay_dates": {
                "expected_role_count",
                "expected_evidence_count",
            },
            "historical_replay_evidence": {
                "evidence_ordinal",
                "source_observation_id",
                "source_item_ordinal",
            },
            "historical_replay_run_seals": {
                "expected_date_count",
                "actual_date_count",
                "actual_evidence_count",
                "date_terminal_cursor",
                "evidence_terminal_cursor",
                "source_observation_highwater",
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
            "canonical_report_contexts": {"economic_at", "retrieved_at"},
            "actual_close_reviews": {
                "review_at",
                "mark_cutoff",
                "query_cutoff",
                "retrieved_at",
            },
            "actual_close_source_bindings": {"received_at"},
            "close_recommendations": {"received_at"},
            "ledger_postings": {"occurred_at"},
            "actual_positions": {"updated_at"},
            "actual_cash_projection": {"updated_at"},
            "reconciliation_projection": {"updated_at"},
            "phase1_validation_windows": {"started_at", "received_at"},
            "phase1_signals": {"published_at", "received_at"},
            "phase1_observation_fetch_manifests": {
                "request_start",
                "request_end",
                "received_through",
            },
            "phase1_session_late_evidence": {
                "request_start",
                "request_end",
                "received_through",
            },
            "phase1_observations": {"source_time", "received_at"},
            "phase1_session_completions": {"received_through", "completed_at"},
            "phase1_signal_events": {"event_time", "message_time", "received_at"},
            "phase1_canonical_postings": {"occurred_at", "received_at"},
            "phase1_equity_points": {"at", "message_time", "received_at"},
            "phase1_closed_trades": {"at", "message_time", "received_at"},
            "phase1_adherence_checks": {"evaluated_at", "received_at"},
            "phase2_windows": {
                "promotion_query_cutoff",
                "started_at",
                "received_at",
            },
            "phase2_authorizations": {"authorized_at", "received_at"},
            "phase2_fee_schedules": {"reviewed_at", "archived_at"},
            "phase2_contract_snapshots": {"observed_at", "received_at"},
            "phase2_contract_selections": {"selected_at", "received_at"},
            "phase2_entries": {"entered_at", "received_at"},
            "phase2_marks": {"deadline_at", "marked_at", "received_at"},
            "phase2_exits": {"exited_at", "received_at"},
            "phase2_fee_records": {"recorded_at"},
            "phase2_equity_points": {"at", "received_at"},
            "phase2_window_failures": {"detected_at", "received_at"},
            "phase2_window_restarts": {"restarted_at", "received_at"},
            "phase2_gate_decisions": {
                "query_cutoff",
                "evaluated_at",
                "received_at",
            },
            "historical_replay_runs": {"query_cutoff", "recorded_at"},
            "historical_replay_dates": {"report_cutoff", "completed_at"},
            "historical_replay_evidence": {
                "effective_at",
                "published_at",
                "retrieved_at",
            },
        }
        date_fields = {
            "report_claims": {"session_date"},
            "reports": {"session_date"},
            "scheduled_runs": {"session_date"},
            "actual_close_reviews": {"session_date"},
            "close_recommendations": {"session_date"},
            "phase1_validation_windows": {"started_session"},
            "phase1_signals": {"publication_session"},
            "phase1_observation_fetch_manifests": {"session_date"},
            "phase1_session_late_evidence": {"session_date"},
            "phase1_observations": {"session_date"},
            "phase1_session_completions": {"session_date"},
            "phase1_canonical_postings": {"settlement_available_session"},
            "phase1_equity_points": {"session_date"},
            "phase1_closed_trades": {"session_date"},
            "phase2_windows": {
                "promotion_through_session",
                "started_session",
            },
            "phase2_fee_schedules": {"effective_session"},
            "phase2_contract_snapshots": {"expiration"},
            "phase2_contract_selections": {"selection_session"},
            "phase2_marks": {"session_date"},
            "phase2_equity_points": {"session_date"},
            "phase2_window_failures": {"session_date"},
            "historical_replay_runs": {"started_session", "ended_session"},
            "historical_replay_dates": {"session_date"},
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

    def test_actual_position_sql_rejects_source_and_update_time_regressions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            event_ids, _, updated_at, earlier_updated_at = (
                self._seed_projection_chronology(path)
            )
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_projection_write_allowed", 0, lambda: 1
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_positions SET shares = 2, "
                        "cost_basis_micros = 200000000, revision = 2 "
                        "WHERE signal_id = 'signal-sql-chronology'"
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_positions SET shares = 2, "
                        "cost_basis_micros = 200000000, "
                        "last_execution_event_id = ?, updated_at = ?, revision = 3 "
                        "WHERE signal_id = 'signal-sql-chronology'",
                        (event_ids[2], updated_at),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_positions SET id = 999, shares = 2, "
                        "cost_basis_micros = 200000000, "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE signal_id = 'signal-sql-chronology'",
                        (event_ids[2], updated_at),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_positions SET shares = 2, "
                        "cost_basis_micros = 200000000, "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE signal_id = 'signal-sql-chronology'",
                        (event_ids[1], "2026-08-14T14:00:21.000000Z"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_positions SET shares = 2, "
                        "cost_basis_micros = 200000000, "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE signal_id = 'signal-sql-chronology'",
                        (event_ids[2], earlier_updated_at),
                    )
                connection.execute(
                    "UPDATE actual_positions SET shares = 2, "
                    "cost_basis_micros = 200000000, "
                    "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                    "WHERE signal_id = 'signal-sql-chronology'",
                    (event_ids[2], updated_at),
                )

    def test_actual_cash_sql_rejects_source_and_update_time_regressions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            _, posting_ids, updated_at, earlier_updated_at = (
                self._seed_projection_chronology(path)
            )
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_projection_write_allowed", 0, lambda: 1
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_cash_projection SET "
                        "estimated_settled_cash_micros = 5100000000, revision = 2 "
                        "WHERE id = 1"
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_cash_projection SET "
                        "estimated_settled_cash_micros = 5100000000, "
                        "last_ledger_posting_id = ?, updated_at = ?, revision = 3 "
                        "WHERE id = 1",
                        (posting_ids[2], updated_at),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_cash_projection SET "
                        "estimated_settled_cash_micros = 5100000000, "
                        "weekly_high_water_micros = 5100000000, "
                        "monthly_high_water_micros = 5100000000, "
                        "last_ledger_posting_id = ?, updated_at = ?, revision = 2 "
                        "WHERE id = 1",
                        (posting_ids[1], "2026-08-14T14:00:21.000000Z"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE actual_cash_projection SET "
                        "estimated_settled_cash_micros = 5100000000, "
                        "weekly_high_water_micros = 5100000000, "
                        "monthly_high_water_micros = 5100000000, "
                        "last_ledger_posting_id = ?, updated_at = ?, revision = 2 "
                        "WHERE id = 1",
                        (posting_ids[2], earlier_updated_at),
                    )
                connection.execute(
                    "UPDATE actual_cash_projection SET "
                    "estimated_settled_cash_micros = 5100000000, "
                    "weekly_high_water_micros = 5100000000, "
                    "monthly_high_water_micros = 5100000000, "
                    "last_ledger_posting_id = ?, updated_at = ?, revision = 2 "
                    "WHERE id = 1",
                    (posting_ids[2], updated_at),
                )

    def test_reconciliation_sql_rejects_source_and_update_time_regressions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            event_ids, _, updated_at, earlier_updated_at = (
                self._seed_projection_chronology(path)
            )
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_projection_write_allowed", 0, lambda: 1
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE reconciliation_projection SET reason = 'CHANGED', "
                        "revision = 2 WHERE id = 1"
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE reconciliation_projection SET reason = 'CHANGED', "
                        "last_execution_event_id = ?, updated_at = ?, revision = 3 "
                        "WHERE id = 1",
                        (event_ids[2], updated_at),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE reconciliation_projection SET reason = '', "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE id = 1",
                        (event_ids[2], updated_at),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE reconciliation_projection SET reason = 'DELAYED', "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE id = 1",
                        (event_ids[1], "2026-08-14T14:00:21.000000Z"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE reconciliation_projection SET reason = 'EQUAL', "
                        "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                        "WHERE id = 1",
                        (event_ids[2], earlier_updated_at),
                    )
                connection.execute(
                    "UPDATE reconciliation_projection SET reason = 'EQUAL', "
                    "last_execution_event_id = ?, updated_at = ?, revision = 2 "
                    "WHERE id = 1",
                    (event_ids[2], updated_at),
                )

    def test_projection_sql_validates_initial_source_time_and_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            event_ids, posting_ids, updated_at, _ = self._seed_projection_chronology(
                path, include_projections=False
            )
            too_early = "2026-08-14T14:00:09.999999Z"
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_projection_write_allowed", 0, lambda: 1
                )

                position_sql = (
                    "INSERT INTO actual_positions("
                    "signal_id, symbol, shares, cost_basis_micros, "
                    "recommended_stop_micros, user_confirmed_stop_micros, "
                    "target_micros, last_execution_event_id, updated_at, revision"
                    ") VALUES ('signal-sql-chronology', 'SPY', 2, 200000000, "
                    "NULL, NULL, NULL, ?, ?, ?)"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(position_sql, (event_ids[2], too_early, 1))
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(position_sql, (event_ids[2], updated_at, 7))
                connection.execute(position_sql, (event_ids[2], updated_at, 1))

                cash_sql = (
                    "INSERT INTO actual_cash_projection VALUES "
                    "(1, 5100000000, NULL, 0, 0, 0, 5100000000, "
                    "5100000000, ?, ?, ?)"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(cash_sql, (posting_ids[2], too_early, 1))
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(cash_sql, (posting_ids[2], updated_at, 7))
                connection.execute(cash_sql, (posting_ids[2], updated_at, 1))

                reconciliation_sql = (
                    "INSERT INTO reconciliation_projection VALUES "
                    "(1, 1, 'VALID', ?, ?, ?)"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        reconciliation_sql, (event_ids[2], too_early, 1)
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        reconciliation_sql, (event_ids[2], updated_at, 7)
                    )
                connection.execute(
                    reconciliation_sql, (event_ids[2], updated_at, 1)
                )

    def test_projection_history_rejects_delete_and_insert_or_replace(self) -> None:
        cases = {
            "actual_positions": (
                "INSERT OR REPLACE INTO actual_positions("
                "id, signal_id, symbol, shares, cost_basis_micros, "
                "recommended_stop_micros, user_confirmed_stop_micros, "
                "target_micros, last_execution_event_id, updated_at, revision"
                ") VALUES (1, 'signal-sql-chronology', 'SPY', 2, "
                "200000000, NULL, NULL, NULL, ?, ?, 1)",
                "DELETE FROM actual_positions "
                "WHERE signal_id = 'signal-sql-chronology'",
            ),
            "actual_cash_projection": (
                "INSERT OR REPLACE INTO actual_cash_projection VALUES "
                "(1, 5100000000, NULL, 0, 0, 0, 5100000000, "
                "5100000000, ?, ?, 1)",
                "DELETE FROM actual_cash_projection WHERE id = 1",
            ),
            "reconciliation_projection": (
                "INSERT OR REPLACE INTO reconciliation_projection VALUES "
                "(1, 1, 'DELAYED', ?, ?, 1)",
                "DELETE FROM reconciliation_projection WHERE id = 1",
            ),
        }
        for recursive_triggers in (0, 1):
            for table, (replace_sql, delete_sql) in cases.items():
                for operation in ("replace", "delete"):
                    with self.subTest(
                        table=table,
                        operation=operation,
                        recursive_triggers=recursive_triggers,
                    ), tempfile.TemporaryDirectory() as temporary_directory:
                        path = Path(temporary_directory) / "journal.db"
                        event_ids, posting_ids, _, _ = (
                            self._seed_projection_chronology(path)
                        )
                        source_id = (
                            posting_ids[1]
                            if table == "actual_cash_projection"
                            else event_ids[1]
                        )
                        with closing(
                            sqlite3.connect(path, isolation_level=None)
                        ) as connection:
                            connection.execute("PRAGMA foreign_keys = ON")
                            connection.execute(
                                f"PRAGMA recursive_triggers = {recursive_triggers}"
                            )
                            connection.create_function(
                                "journal_projection_write_allowed", 0, lambda: 1
                            )
                            with self.assertRaises(sqlite3.IntegrityError):
                                if operation == "replace":
                                    connection.execute(
                                        replace_sql,
                                        (
                                            source_id,
                                            "2026-08-14T14:00:21.000000Z",
                                        ),
                                    )
                                else:
                                    connection.execute(delete_sql)

    def test_projection_triggers_reject_id_and_reason_reset_when_checks_ignored(
        self,
    ) -> None:
        updated_at = datetime(2026, 8, 14, 14, 0, 20, tzinfo=timezone.utc)
        stored_updated_at = "2026-08-14T14:00:20.000000Z"
        for recursive_triggers in (0, 1):
            for table in ("actual_cash_projection", "reconciliation_projection"):
                with self.subTest(
                    table=table, recursive_triggers=recursive_triggers
                ), tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    event_ids, posting_ids, _, _ = self._seed_projection_chronology(
                        path
                    )
                    with closing(
                        sqlite3.connect(path, isolation_level=None)
                    ) as connection:
                        connection.execute("PRAGMA foreign_keys = ON")
                        connection.execute(
                            f"PRAGMA recursive_triggers = {recursive_triggers}"
                        )
                        connection.execute("PRAGMA ignore_check_constraints = ON")
                        connection.create_function(
                            "journal_projection_write_allowed", 0, lambda: 1
                        )
                        connection.execute("BEGIN IMMEDIATE")
                        try:
                            with self.assertRaises(sqlite3.IntegrityError):
                                if table == "actual_cash_projection":
                                    connection.execute(
                                        "UPDATE actual_cash_projection SET id = 2, "
                                        "estimated_settled_cash_micros = 5100000000, "
                                        "weekly_high_water_micros = 5100000000, "
                                        "monthly_high_water_micros = 5100000000, "
                                        "last_ledger_posting_id = ?, updated_at = ?, "
                                        "revision = 2 WHERE id = 1",
                                        (posting_ids[2], stored_updated_at),
                                    )
                                    connection.execute(
                                        "INSERT OR REPLACE INTO "
                                        "actual_cash_projection VALUES "
                                        "(1, 5000000000, NULL, 0, 0, 0, "
                                        "5000000000, 5000000000, ?, ?, 1)",
                                        (posting_ids[0], stored_updated_at),
                                    )
                                else:
                                    connection.execute(
                                        "UPDATE reconciliation_projection SET "
                                        "id = 2, reason = '', "
                                        "last_execution_event_id = ?, "
                                        "updated_at = ?, revision = 2 WHERE id = 1",
                                        (event_ids[2], stored_updated_at),
                                    )
                                    connection.execute(
                                        "INSERT OR REPLACE INTO "
                                        "reconciliation_projection VALUES "
                                        "(1, 1, 'INITIAL', ?, ?, 1)",
                                        (event_ids[0], stored_updated_at),
                                    )
                        finally:
                            if connection.in_transaction:
                                connection.rollback()

                    with Journal.open(path) as journal:
                        with journal.transaction() as transaction:
                            self.assertEqual(
                                transaction.write_actual_cash_projection(
                                    estimated_settled_cash_micros=5_000_000_000,
                                    user_confirmed_settled_cash_micros=None,
                                    deployed_capital_micros=0,
                                    open_planned_risk_micros=0,
                                    consecutive_losses=0,
                                    weekly_high_water_micros=5_000_000_000,
                                    monthly_high_water_micros=5_000_000_000,
                                    last_ledger_posting_id=posting_ids[0],
                                    updated_at=updated_at,
                                ),
                                1,
                            )
                            self.assertEqual(
                                transaction.write_reconciliation_projection(
                                    reconciliation_required=True,
                                    reason="INITIAL",
                                    last_execution_event_id=event_ids[0],
                                    updated_at=updated_at,
                                ),
                                1,
                            )
                        self.assertEqual(journal.count(table), 1)

    def test_projection_insert_triggers_mirror_check_invariants_when_ignored(
        self,
    ) -> None:
        stored_updated_at = "2026-08-14T14:00:20.000000Z"
        updated_at = datetime(2026, 8, 14, 14, 0, 20, tzinfo=timezone.utc)
        position_sql = (
            "INSERT INTO actual_positions("
            "id, signal_id, symbol, shares, cost_basis_micros, "
            "recommended_stop_micros, user_confirmed_stop_micros, "
            "target_micros, last_execution_event_id, updated_at, revision"
            ") VALUES (:id, :signal_id, :symbol, :shares, :cost_basis, "
            ":recommended_stop, :confirmed_stop, :target, :source_id, "
            ":updated_at, :revision)"
        )
        cash_sql = (
            "INSERT INTO actual_cash_projection VALUES "
            "(:id, :estimated, :confirmed, :deployed, :risk, :losses, "
            ":weekly, :monthly, :source_id, :updated_at, :revision)"
        )
        reconciliation_sql = (
            "INSERT INTO reconciliation_projection VALUES "
            "(:id, :required, :reason, :source_id, :updated_at, :revision)"
        )
        for recursive_triggers in (0, 1):
            with self.subTest(
                control="recursive-mode", recursive_triggers=recursive_triggers
            ), tempfile.TemporaryDirectory() as temporary_directory:
                path = Path(temporary_directory) / "journal.db"
                event_ids, posting_ids, _, _ = self._seed_projection_chronology(
                    path, include_projections=False
                )
                position = {
                    "id": 1,
                    "signal_id": "signal-sql-chronology",
                    "symbol": "SPY",
                    "shares": 2,
                    "cost_basis": 200_000_000,
                    "recommended_stop": None,
                    "confirmed_stop": None,
                    "target": None,
                    "source_id": event_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 1,
                }
                cash = {
                    "id": 1,
                    "estimated": 5_100_000_000,
                    "confirmed": None,
                    "deployed": 0,
                    "risk": 0,
                    "losses": 0,
                    "weekly": 5_100_000_000,
                    "monthly": 5_100_000_000,
                    "source_id": posting_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 1,
                }
                reconciliation = {
                    "id": 1,
                    "required": 1,
                    "reason": "VALID",
                    "source_id": event_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 1,
                }
                cases = {
                    "actual_positions": (
                        position_sql,
                        position,
                        {
                            "id": {"id": 0},
                            "shares": {"shares": -1},
                            "cost_basis": {"cost_basis": -1},
                            "recommended_stop": {"recommended_stop": 0},
                            "confirmed_stop": {"confirmed_stop": 0},
                            "target": {"target": 0},
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                            "closed_position": {"shares": 0},
                        },
                    ),
                    "actual_cash_projection": (
                        cash_sql,
                        cash,
                        {
                            "id": {"id": 0},
                            "estimated": {"estimated": -1},
                            "confirmed": {"confirmed": -1},
                            "deployed": {"deployed": -1},
                            "risk": {"risk": -1},
                            "losses": {"losses": -1},
                            "weekly": {"weekly": -1},
                            "monthly": {"monthly": -1},
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                        },
                    ),
                    "reconciliation_projection": (
                        reconciliation_sql,
                        reconciliation,
                        {
                            "id": {"id": 0},
                            "required": {"required": 2},
                            "active_reason_absent": {"reason": None},
                            "active_reason_empty": {"reason": ""},
                            "clear_reason_present": {
                                "required": 0,
                                "reason": "NOT CLEAR",
                            },
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                        },
                    ),
                }
                with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                    connection.execute("PRAGMA foreign_keys = ON")
                    connection.execute(
                        f"PRAGMA recursive_triggers = {recursive_triggers}"
                    )
                    connection.execute("PRAGMA ignore_check_constraints = ON")
                    connection.create_function(
                        "journal_projection_write_allowed", 0, lambda: 1
                    )
                    for table, (statement, valid, invalid_cases) in cases.items():
                        for case, overrides in invalid_cases.items():
                            with self.subTest(
                                table=table,
                                case=case,
                                recursive_triggers=recursive_triggers,
                            ):
                                values = dict(valid)
                                values.update(overrides)
                                connection.execute("SAVEPOINT invalid_projection")
                                try:
                                    with self.assertRaises(sqlite3.IntegrityError):
                                        connection.execute(statement, values)
                                finally:
                                    connection.execute(
                                        "ROLLBACK TO SAVEPOINT invalid_projection"
                                    )
                                    connection.execute(
                                        "RELEASE SAVEPOINT invalid_projection"
                                    )
                    connection.execute(position_sql, position)
                    connection.execute(cash_sql, cash)
                    connection.execute(reconciliation_sql, reconciliation)

                with Journal.open(path) as journal:
                    with journal.transaction() as transaction:
                        self.assertEqual(
                            transaction.write_actual_position(
                                signal_id="signal-sql-chronology",
                                symbol="SPY",
                                shares=2,
                                cost_basis_micros=200_000_000,
                                recommended_stop_micros=None,
                                user_confirmed_stop_micros=None,
                                target_micros=None,
                                last_execution_event_id=event_ids[2],
                                updated_at=updated_at,
                            ),
                            1,
                        )
                        self.assertEqual(
                            transaction.write_actual_cash_projection(
                                estimated_settled_cash_micros=5_100_000_000,
                                user_confirmed_settled_cash_micros=None,
                                deployed_capital_micros=0,
                                open_planned_risk_micros=0,
                                consecutive_losses=0,
                                weekly_high_water_micros=5_100_000_000,
                                monthly_high_water_micros=5_100_000_000,
                                last_ledger_posting_id=posting_ids[2],
                                updated_at=updated_at,
                            ),
                            1,
                        )
                        self.assertEqual(
                            transaction.write_reconciliation_projection(
                                reconciliation_required=True,
                                reason="VALID",
                                last_execution_event_id=event_ids[2],
                                updated_at=updated_at,
                            ),
                            1,
                        )

    def test_projection_update_triggers_mirror_check_invariants_when_ignored(
        self,
    ) -> None:
        stored_updated_at = "2026-08-14T14:00:20.000000Z"
        updated_at = datetime(2026, 8, 14, 14, 0, 20, tzinfo=timezone.utc)
        clear_at = datetime(2026, 8, 14, 14, 0, 21, tzinfo=timezone.utc)
        stored_clear_at = "2026-08-14T14:00:21.000000Z"
        position_sql = (
            "UPDATE actual_positions SET id = :id, shares = :shares, "
            "cost_basis_micros = :cost_basis, "
            "recommended_stop_micros = :recommended_stop, "
            "user_confirmed_stop_micros = :confirmed_stop, "
            "target_micros = :target, last_execution_event_id = :source_id, "
            "updated_at = :updated_at, revision = :revision "
            "WHERE signal_id = 'signal-sql-chronology'"
        )
        cash_sql = (
            "UPDATE actual_cash_projection SET id = :id, "
            "estimated_settled_cash_micros = :estimated, "
            "user_confirmed_settled_cash_micros = :confirmed, "
            "deployed_capital_micros = :deployed, "
            "open_planned_risk_micros = :risk, consecutive_losses = :losses, "
            "weekly_high_water_micros = :weekly, "
            "monthly_high_water_micros = :monthly, "
            "last_ledger_posting_id = :source_id, updated_at = :updated_at, "
            "revision = :revision WHERE id = 1"
        )
        reconciliation_sql = (
            "UPDATE reconciliation_projection SET id = :id, "
            "reconciliation_required = :required, reason = :reason, "
            "last_execution_event_id = :source_id, updated_at = :updated_at, "
            "revision = :revision WHERE id = 1"
        )
        for recursive_triggers in (0, 1):
            with self.subTest(
                control="recursive-mode", recursive_triggers=recursive_triggers
            ), tempfile.TemporaryDirectory() as temporary_directory:
                path = Path(temporary_directory) / "journal.db"
                event_ids, posting_ids, _, _ = self._seed_projection_chronology(path)
                with Journal.open(path) as journal:
                    raw_id, _ = journal.append_raw_message(
                        f"msg-sql-projection-clear-{recursive_triggers}",
                        clear_at,
                        "ACCOUNT CHECK CLEAR",
                    )
                    clear_event_id, _ = journal.append_execution_event(
                        raw_message_id=raw_id,
                        action_ordinal=0,
                        parsed_action="ACCOUNT_CHECK",
                        event_time=clear_at,
                        reconciliation_state="CLEAR",
                    )
                position = {
                    "id": 1,
                    "shares": 2,
                    "cost_basis": 200_000_000,
                    "recommended_stop": None,
                    "confirmed_stop": None,
                    "target": None,
                    "source_id": event_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 2,
                }
                cash = {
                    "id": 1,
                    "estimated": 5_100_000_000,
                    "confirmed": None,
                    "deployed": 0,
                    "risk": 0,
                    "losses": 0,
                    "weekly": 5_100_000_000,
                    "monthly": 5_100_000_000,
                    "source_id": posting_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 2,
                }
                reconciliation = {
                    "id": 1,
                    "required": 1,
                    "reason": "UPDATED",
                    "source_id": event_ids[2],
                    "updated_at": stored_updated_at,
                    "revision": 2,
                }
                cases = {
                    "actual_positions": (
                        position_sql,
                        position,
                        {
                            "id": {"id": 0},
                            "shares": {"shares": -1},
                            "cost_basis": {"cost_basis": -1},
                            "recommended_stop": {"recommended_stop": 0},
                            "confirmed_stop": {"confirmed_stop": 0},
                            "target": {"target": 0},
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                            "closed_position": {"shares": 0},
                        },
                    ),
                    "actual_cash_projection": (
                        cash_sql,
                        cash,
                        {
                            "id": {"id": 2},
                            "estimated": {"estimated": -1},
                            "confirmed": {"confirmed": -1},
                            "deployed": {"deployed": -1},
                            "risk": {"risk": -1},
                            "losses": {"losses": -1},
                            "weekly": {"weekly": -1},
                            "monthly": {"monthly": -1},
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                        },
                    ),
                    "reconciliation_projection": (
                        reconciliation_sql,
                        reconciliation,
                        {
                            "id": {"id": 2},
                            "required": {"required": 2},
                            "active_reason_absent": {"reason": None},
                            "active_reason_empty": {"reason": ""},
                            "source_id": {"source_id": 0},
                            "timestamp": {"updated_at": "not-a-timestamp"},
                            "revision": {"revision": 0},
                        },
                    ),
                }
                with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                    connection.execute("PRAGMA foreign_keys = ON")
                    connection.execute(
                        f"PRAGMA recursive_triggers = {recursive_triggers}"
                    )
                    connection.execute("PRAGMA ignore_check_constraints = ON")
                    connection.create_function(
                        "journal_projection_write_allowed", 0, lambda: 1
                    )
                    for table, (statement, valid, invalid_cases) in cases.items():
                        for case, overrides in invalid_cases.items():
                            with self.subTest(
                                table=table,
                                case=case,
                                recursive_triggers=recursive_triggers,
                            ):
                                values = dict(valid)
                                values.update(overrides)
                                connection.execute("SAVEPOINT invalid_projection")
                                try:
                                    with self.assertRaises(sqlite3.IntegrityError):
                                        connection.execute(statement, values)
                                finally:
                                    connection.execute(
                                        "ROLLBACK TO SAVEPOINT invalid_projection"
                                    )
                                    connection.execute(
                                        "RELEASE SAVEPOINT invalid_projection"
                                    )
                    connection.execute(position_sql, position)
                    connection.execute(cash_sql, cash)
                    connection.execute(reconciliation_sql, reconciliation)
                    connection.execute(
                        reconciliation_sql,
                        {
                            "id": 1,
                            "required": 0,
                            "reason": None,
                            "source_id": clear_event_id,
                            "updated_at": stored_clear_at,
                            "revision": 3,
                        },
                    )

                with Journal.open(path) as journal:
                    with journal.transaction() as transaction:
                        self.assertEqual(
                            transaction.write_actual_position(
                                signal_id="signal-sql-chronology",
                                symbol="SPY",
                                shares=2,
                                cost_basis_micros=200_000_000,
                                recommended_stop_micros=None,
                                user_confirmed_stop_micros=None,
                                target_micros=None,
                                last_execution_event_id=event_ids[2],
                                updated_at=updated_at,
                            ),
                            2,
                        )
                        self.assertEqual(
                            transaction.write_actual_cash_projection(
                                estimated_settled_cash_micros=5_100_000_000,
                                user_confirmed_settled_cash_micros=None,
                                deployed_capital_micros=0,
                                open_planned_risk_micros=0,
                                consecutive_losses=0,
                                weekly_high_water_micros=5_100_000_000,
                                monthly_high_water_micros=5_100_000_000,
                                last_ledger_posting_id=posting_ids[2],
                                updated_at=updated_at,
                            ),
                            2,
                        )
                        self.assertEqual(
                            transaction.write_reconciliation_projection(
                                reconciliation_required=False,
                                reason=None,
                                last_execution_event_id=clear_event_id,
                                updated_at=clear_at,
                            ),
                            3,
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
                run_id = int(
                    journal._connection.execute(
                        "INSERT INTO scheduled_runs("
                        "run_key, run_kind, session_date, intended_run_at, started_at"
                        ") VALUES (?, ?, ?, ?, ?)",
                        (
                            "close-2026-08-14",
                            "CLOSE",
                            "2026-08-14",
                            "2026-08-14T12:45:00.000000Z",
                            "2026-08-14T12:45:00.000000Z",
                        ),
                    ).lastrowid
                )
                archive_path = report_archive_relative_path(
                    "CLOSE", session_date, report_id
                )
                envelope = ScheduledRunResultEnvelope(
                    outcome="EMITTED",
                    message="# Close\n",
                    exit_code=0,
                    reason_codes=("SCHEDULED_EMITTED",),
                    execution_mode="FIXTURE",
                    candidates=(),
                    report_id=report_id,
                    report_row_id=report.report_row_id,
                    report_path=archive_path,
                    report_body="# Close\n",
                    report_content_sha256=hashlib.sha256(
                        b"# Close\n"
                    ).hexdigest(),
                    report_state_sha256=state_sha256,
                )
                envelope_json, envelope_sha256 = (
                    journal_module._scheduled_result_envelope_storage(envelope)
                )
                journal._connection.execute(
                    "UPDATE scheduled_runs SET finished_at = ?, "
                    "market_session_decision = 'OPEN', report_id = ?, "
                    "report_path = ?, outcome = 'REPORT_EMITTED', "
                    "result_envelope_json = ?, result_envelope_sha256 = ? "
                    "WHERE id = ?",
                    (
                        "2026-08-14T12:45:02.000000Z",
                        report.report_row_id,
                        archive_path,
                        envelope_json,
                        envelope_sha256,
                        run_id,
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

    def test_report_claim_creation_cannot_follow_its_lease_start(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path):
                pass
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA recursive_triggers = ON")
                connection.create_function(
                    "journal_report_claim_write_allowed", 0, lambda: 1
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO report_claims("
                        "session_date, report_kind, claim_token, status, created_at, "
                        "lease_started_at, lease_expires_at"
                        ") VALUES (?, ?, ?, 'IN_PROGRESS', ?, ?, ?)",
                        (
                            "2026-08-14",
                            "CLOSE",
                            "future-created-claim",
                            "2026-08-14T12:46:00.000000Z",
                            "2026-08-14T12:45:00.000000Z",
                            "2026-08-14T12:50:00.000000Z",
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

    def test_report_schema_rejects_creation_before_the_original_claim(self) -> None:
        claim_time = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        report_time = "2026-08-14T12:44:59.999999Z"
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with patch.object(
                journal_module, "_utc_now", return_value=claim_time
            ), Journal.open(path) as journal:
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA recursive_triggers = ON")
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
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
                            report_time,
                        ),
                    )

    def test_scheduled_completion_tokens_are_canonical_in_sql(self) -> None:
        timestamp = "2026-08-14T14:00:00.000000Z"
        finished = "2026-08-14T14:00:01.000000Z"
        envelope_json = "{}"
        envelope_sha256 = hashlib.sha256(envelope_json.encode("utf-8")).hexdigest()
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
                        "outcome = 'report_emitted', result_envelope_json = ?, "
                        "result_envelope_sha256 = ? WHERE run_key = ?",
                        (
                            finished,
                            envelope_json,
                            envelope_sha256,
                            "lowercase-completion",
                        ),
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
                run_id = int(
                    journal._connection.execute(
                        "INSERT INTO scheduled_runs("
                        "run_key, run_kind, session_date, intended_run_at, started_at"
                        ") VALUES (?, ?, ?, ?, ?)",
                        (
                            "close-2026-08-14",
                            "CLOSE",
                            "2026-08-14",
                            "2026-08-14T12:45:00.000000Z",
                            "2026-08-14T12:45:00.000000Z",
                        ),
                    ).lastrowid
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
                envelope_json = "{}"
                envelope_sha256 = hashlib.sha256(
                    envelope_json.encode("utf-8")
                ).hexdigest()
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE scheduled_runs SET finished_at = ?, "
                        "market_session_decision = 'OPEN', "
                        "outcome = 'REPORT_EMITTED', report_id = 1, "
                        "report_path = ?, result_envelope_json = ?, "
                        "result_envelope_sha256 = ? WHERE id = ?",
                        (
                            (report_time + timedelta(seconds=1)).strftime(
                                "%Y-%m-%dT%H:%M:%S.%fZ"
                            ),
                            report_values[9],
                            envelope_json,
                            envelope_sha256,
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
