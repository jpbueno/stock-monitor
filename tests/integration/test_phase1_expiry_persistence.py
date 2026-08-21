from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import stock_monitor.journal as journal_module
from stock_monitor.journal import Journal
from tests.integration.test_phase1_authorities import (
    _SESSION,
    _calendar,
    _published_signal_source,
    aware_et,
)


class Phase1ExpiryPersistenceTests(unittest.TestCase):
    def test_begin_callback_cannot_persist_mutated_expiry_evidence(self) -> None:
        deadline_session = _calendar().add_sessions(_SESSION, 1)
        deadline = aware_et(deadline_session, "08:45")

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_source = _published_signal_source(journal)
                event_count_before = journal.count("phase1_signal_events")
                observed: dict[str, object] = {
                    "trace_fired": False,
                    "lookup_failed": False,
                }

                def mutate_expiry_evidence(statement: str) -> None:
                    if statement.strip().upper() != "BEGIN IMMEDIATE" or observed[
                        "trace_fired"
                    ]:
                        return
                    observed["trace_fired"] = True
                    with journal_module._JOURNAL_SOURCE_LOCK:
                        candidates = tuple(
                            issued[0]()
                            for issued in journal_module._PHASE1_EXPIRY_DEADLINE_SOURCE_AUTHORITIES.values()
                            if issued[2]() is journal
                        )
                    source = next(
                        (
                            candidate
                            for candidate in candidates
                            if type(candidate)
                            is journal_module.Phase1ExpiryDeadlineSource
                            and candidate.signal_id == signal_source.signal_id
                        ),
                        None,
                    )
                    if source is None:
                        observed["lookup_failed"] = True
                        return
                    observed["source"] = source
                    object.__setattr__(
                        source.expiry_evidence,
                        "source_id",
                        "f" * 64,
                    )

                journal._connection.set_trace_callback(mutate_expiry_evidence)
                rejection: BaseException | None = None
                try:
                    journal.record_phase1_unentered_terminal(
                        signal_source.signal_id,
                        recorded_at=deadline,
                        calendar_resolver=_calendar(),
                    )
                except BaseException as error:
                    rejection = error
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["trace_fired"])
                self.assertFalse(observed["lookup_failed"])
                self.assertIsInstance(
                    rejection,
                    journal_module.IdempotencyConflict,
                )
                self.assertRegex(str(rejection), "changed before persistence")
                self.assertEqual(journal.count("phase1_expiry_deadlines"), 0)
                self.assertEqual(
                    journal.count("phase1_signal_events"),
                    event_count_before,
                )
                linked_rows = journal._connection.execute(
                    "SELECT COUNT(*) FROM phase1_signal_events "
                    "WHERE expiry_source_id IS NOT NULL"
                ).fetchone()
                self.assertIsNotNone(linked_rows)
                self.assertEqual(int(linked_rows[0]), 0)
                mutated_source = observed.get("source")
                self.assertIsNotNone(mutated_source)
                self.assertFalse(
                    journal_module.is_verified_phase1_expiry_deadline_source(
                        mutated_source
                    )
                )

    def test_verifier_callback_cannot_restore_after_forged_snapshot(self) -> None:
        deadline_session = _calendar().add_sessions(_SESSION, 1)
        deadline = aware_et(deadline_session, "08:45")

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                signal_source = _published_signal_source(journal)
                observed: dict[str, object] = {"stage": "WAITING_FOR_BEGIN"}

                def mutate_then_restore(statement: str) -> None:
                    normalized = statement.strip().upper()
                    if (
                        normalized == "BEGIN IMMEDIATE"
                        and observed["stage"] == "WAITING_FOR_BEGIN"
                    ):
                        with journal_module._JOURNAL_SOURCE_LOCK:
                            candidates = tuple(
                                issued[0]()
                                for issued in journal_module._PHASE1_EXPIRY_DEADLINE_SOURCE_AUTHORITIES.values()
                                if issued[2]() is journal
                            )
                        source = next(
                            (
                                candidate
                                for candidate in candidates
                                if type(candidate)
                                is journal_module.Phase1ExpiryDeadlineSource
                                and candidate.signal_id
                                == signal_source.signal_id
                            ),
                            None,
                        )
                        if source is None:
                            observed["stage"] = "LOOKUP_FAILED"
                            return
                        original_source_id = source.expiry_evidence.source_id
                        forged_source_id = (
                            "f" * 64
                            if original_source_id != "f" * 64
                            else "e" * 64
                        )
                        observed.update(
                            {
                                "stage": "WAITING_FOR_VERIFIER_PRAGMA",
                                "source": source,
                                "original_source_id": original_source_id,
                                "forged_source_id": forged_source_id,
                            }
                        )
                        object.__setattr__(
                            source.expiry_evidence,
                            "source_id",
                            forged_source_id,
                        )
                    elif (
                        normalized == "PRAGMA DATA_VERSION"
                        and observed["stage"]
                        == "WAITING_FOR_VERIFIER_PRAGMA"
                    ):
                        source = observed["source"]
                        object.__setattr__(
                            source.expiry_evidence,
                            "source_id",
                            observed["original_source_id"],
                        )
                        observed["stage"] = "RESTORED"

                journal._connection.set_trace_callback(mutate_then_restore)
                try:
                    result = journal.record_phase1_unentered_terminal(
                        signal_source.signal_id,
                        recorded_at=deadline,
                        calendar_resolver=_calendar(),
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertEqual(observed["stage"], "RESTORED")
                self.assertFalse(result.duplicate)
                self.assertEqual(result.event_kind, "EXPIRE")
                original_source_id = observed["original_source_id"]
                forged_source_id = observed["forged_source_id"]
                deadline_rows = journal._connection.execute(
                    "SELECT expiry_source_id FROM phase1_expiry_deadlines"
                ).fetchall()
                self.assertEqual(deadline_rows, [(original_source_id,)])
                self.assertNotEqual(deadline_rows, [(forged_source_id,)])
                terminal_rows = journal._connection.execute(
                    "SELECT expiry_source_id FROM phase1_signal_events "
                    "WHERE expiry_source_id IS NOT NULL"
                ).fetchall()
                self.assertEqual(terminal_rows, [(original_source_id,)])
                restored_source = observed["source"]
                self.assertFalse(
                    journal_module.is_verified_phase1_expiry_deadline_source(
                        restored_source
                    )
                )


if __name__ == "__main__":
    unittest.main()
