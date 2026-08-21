from __future__ import annotations

import hashlib
import importlib.resources
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path

from stock_monitor.journal import InvalidJournalValue, Journal


_MIGRATIONS_THROUGH_V3 = (
    "001_core.sql",
    "002_phase1.sql",
    "003_phase2_paper.sql",
)
_STARTED_AT = "2026-08-14T14:00:00.000000Z"
_FINISHED_AT = "2026-08-14T14:00:01.000000Z"
_ENVELOPE_JSON = '{"outcome":"NO_TRADE"}'
_ENVELOPE_SHA256 = hashlib.sha256(_ENVELOPE_JSON.encode("utf-8")).hexdigest()


class ScheduledResultEnvelopeMigrationTests(unittest.TestCase):
    def _create_v3_database(self, root: Path) -> Path:
        migration_directory = root / "migrations-v3"
        migration_directory.mkdir()
        packaged_migrations = importlib.resources.files("stock_monitor.sql")
        for name in _MIGRATIONS_THROUGH_V3:
            packaged = packaged_migrations.joinpath(name)
            (migration_directory / name).write_bytes(packaged.read_bytes())

        path = root / "journal.db"
        with Journal.open(path, migration_directory=migration_directory):
            pass
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                3,
            )
        return path

    def _migrate_to_v4(self, path: Path) -> None:
        with Journal.open(path):
            pass
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                4,
            )

    @staticmethod
    def _insert_started_run(connection: sqlite3.Connection, run_key: str) -> None:
        connection.execute(
            "INSERT INTO scheduled_runs("
            "run_key, run_kind, session_date, intended_run_at, started_at"
            ") VALUES (?, 'CLOSE', '2026-08-14', ?, ?)",
            (run_key, _STARTED_AT, _STARTED_AT),
        )

    @staticmethod
    def _complete_run(connection: sqlite3.Connection, run_key: str) -> None:
        connection.execute(
            "UPDATE scheduled_runs SET "
            "finished_at = ?, market_session_decision = 'OPEN', "
            "outcome = 'NO_TRADE', result_envelope_json = ?, "
            "result_envelope_sha256 = ? WHERE run_key = ?",
            (_FINISHED_AT, _ENVELOPE_JSON, _ENVELOPE_SHA256, run_key),
        )

    @staticmethod
    def _completion_predicate(trigger_sql: str) -> str:
        normalized = " ".join(trigger_sql.lower().split())
        match = re.search(r"\bwhen not \((.*)\) begin\b", normalized)
        if match is None:
            raise AssertionError("completion trigger predicate is unavailable")
        return match.group(1)

    def test_v3_rows_are_not_backfilled_and_started_rows_remain_completable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = self._create_v3_database(Path(temporary_directory))
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                connection.execute(
                    "INSERT INTO scheduled_runs("
                    "run_key, run_kind, session_date, intended_run_at, started_at, "
                    "finished_at, market_session_decision, outcome"
                    ") VALUES ("
                    "'v3-terminal', 'CLOSE', '2026-08-14', ?, ?, ?, "
                    "'OPEN', 'NO_TRADE'"
                    ")",
                    (_STARTED_AT, _STARTED_AT, _FINISHED_AT),
                )
                self._insert_started_run(connection, "v3-started")

            self._migrate_to_v4(path)

            with Journal.open(path) as journal:
                self.assertIsNone(
                    journal.read_scheduled_run_result_envelope(
                        run_key="v3-terminal",
                        run_kind="CLOSE",
                        session_date=date(2026, 8, 14),
                    )
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.read_scheduled_run_result_envelope(
                        run_key="v3-started",
                        run_kind="CLOSE",
                        session_date=date(2026, 8, 14),
                    )

            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                rows = connection.execute(
                    "SELECT run_key, finished_at, result_envelope_json, "
                    "result_envelope_sha256 FROM scheduled_runs ORDER BY run_key"
                ).fetchall()
                self.assertEqual(
                    rows,
                    [
                        ("v3-started", None, None, None),
                        ("v3-terminal", _FINISHED_AT, None, None),
                    ],
                )

                self._complete_run(connection, "v3-started")
                self.assertEqual(
                    connection.execute(
                        "SELECT finished_at, result_envelope_json, "
                        "result_envelope_sha256 FROM scheduled_runs "
                        "WHERE run_key = 'v3-started'"
                    ).fetchone(),
                    (_FINISHED_AT, _ENVELOPE_JSON, _ENVELOPE_SHA256),
                )

                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE scheduled_runs SET result_envelope_json = ?, "
                        "result_envelope_sha256 = ? "
                        "WHERE run_key = 'v3-terminal'",
                        (_ENVELOPE_JSON, _ENVELOPE_SHA256),
                    )
                self.assertEqual(
                    connection.execute(
                        "SELECT result_envelope_json, result_envelope_sha256 "
                        "FROM scheduled_runs WHERE run_key = 'v3-terminal'"
                    ).fetchone(),
                    (None, None),
                )

    def test_v4_completion_trigger_preserves_every_v3_predicate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = self._create_v3_database(Path(temporary_directory))
            with closing(sqlite3.connect(path)) as connection:
                v3_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema WHERE type = 'trigger' "
                    "AND name = 'scheduled_runs_controlled_completion'"
                ).fetchone()[0]

            self._migrate_to_v4(path)

            with closing(sqlite3.connect(path)) as connection:
                v4_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema WHERE type = 'trigger' "
                    "AND name = 'scheduled_runs_controlled_completion'"
                ).fetchone()[0]

            v3_predicate = self._completion_predicate(str(v3_sql))
            v4_predicate = self._completion_predicate(str(v4_sql))
            envelope_predicates = (
                "old.result_envelope_json is null",
                "old.result_envelope_sha256 is null",
                "new.result_envelope_json is not null",
                "new.result_envelope_sha256 is not null",
            )
            stripped_v4_predicate = v4_predicate
            for predicate in envelope_predicates:
                self.assertIn(predicate, stripped_v4_predicate)
                stripped_v4_predicate = stripped_v4_predicate.replace(
                    f" and {predicate}",
                    "",
                    1,
                )
            self.assertEqual(stripped_v4_predicate, v3_predicate)

    def test_v4_start_requires_null_envelope_and_rejects_terminal_insert(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            self._migrate_to_v4(path)
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                rejected_inserts = (
                    (
                        "terminal-insert",
                        _FINISHED_AT,
                        "OPEN",
                        "NO_TRADE",
                        None,
                        None,
                    ),
                    ("json-only", None, None, None, _ENVELOPE_JSON, None),
                    ("hash-only", None, None, None, None, _ENVELOPE_SHA256),
                    (
                        "premature-envelope",
                        None,
                        None,
                        None,
                        _ENVELOPE_JSON,
                        _ENVELOPE_SHA256,
                    ),
                )
                for values in rejected_inserts:
                    with self.subTest(run_key=values[0]):
                        with self.assertRaises(sqlite3.DatabaseError):
                            connection.execute(
                                "INSERT INTO scheduled_runs("
                                "run_key, run_kind, session_date, intended_run_at, "
                                "started_at, finished_at, market_session_decision, "
                                "outcome, result_envelope_json, result_envelope_sha256"
                                ") VALUES (?, 'CLOSE', '2026-08-14', ?, ?, ?, ?, ?, ?, ?)",
                                (values[0], _STARTED_AT, _STARTED_AT, *values[1:]),
                            )

                self._insert_started_run(connection, "valid-start")
                self.assertEqual(
                    connection.execute(
                        "SELECT finished_at, result_envelope_json, "
                        "result_envelope_sha256 FROM scheduled_runs "
                        "WHERE run_key = 'valid-start'"
                    ).fetchone(),
                    (None, None, None),
                )

    def test_v4_completion_requires_an_object_and_lowercase_sha256_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            self._migrate_to_v4(path)
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                self._insert_started_run(connection, "invalid-completion")
                invalid_envelopes = (
                    ("missing-pair", None, None),
                    ("json-only", _ENVELOPE_JSON, None),
                    ("hash-only", None, _ENVELOPE_SHA256),
                    ("invalid-json", "not-json", "a" * 64),
                    ("array-json", "[]", "a" * 64),
                    ("uppercase-hash", _ENVELOPE_JSON, "A" * 64),
                    ("short-hash", _ENVELOPE_JSON, "a" * 63),
                    ("non-hex-hash", _ENVELOPE_JSON, "g" * 64),
                )
                for label, envelope_json, envelope_sha256 in invalid_envelopes:
                    with self.subTest(case=label):
                        with self.assertRaises(sqlite3.DatabaseError):
                            connection.execute(
                                "UPDATE scheduled_runs SET "
                                "finished_at = ?, market_session_decision = 'OPEN', "
                                "outcome = 'NO_TRADE', result_envelope_json = ?, "
                                "result_envelope_sha256 = ? "
                                "WHERE run_key = 'invalid-completion'",
                                (_FINISHED_AT, envelope_json, envelope_sha256),
                            )

                self.assertEqual(
                    connection.execute(
                        "SELECT finished_at, result_envelope_json, "
                        "result_envelope_sha256 FROM scheduled_runs "
                        "WHERE run_key = 'invalid-completion'"
                    ).fetchone(),
                    (None, None, None),
                )

    def test_v4_terminal_envelope_rejects_second_update_and_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            self._migrate_to_v4(path)
            with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                self._insert_started_run(connection, "completed")
                self._complete_run(connection, "completed")

                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE scheduled_runs SET outcome = outcome "
                        "WHERE run_key = 'completed'"
                    )

                changed_json = '{"outcome":"CHANGED"}'
                changed_sha256 = hashlib.sha256(
                    changed_json.encode("utf-8")
                ).hexdigest()
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE scheduled_runs SET result_envelope_json = ?, "
                        "result_envelope_sha256 = ? WHERE run_key = 'completed'",
                        (changed_json, changed_sha256),
                    )

                self.assertEqual(
                    connection.execute(
                        "SELECT result_envelope_json, result_envelope_sha256 "
                        "FROM scheduled_runs WHERE run_key = 'completed'"
                    ).fetchone(),
                    (_ENVELOPE_JSON, _ENVELOPE_SHA256),
                )


if __name__ == "__main__":
    unittest.main()
