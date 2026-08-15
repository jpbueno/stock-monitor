from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from zoneinfo import ZoneInfo

import stock_monitor.journal as journal_module
from stock_monitor.journal import (
    IdempotencyConflict,
    InvalidJournalValue,
    Journal,
    JournalBusy,
    JournalError,
    MigrationCorruption,
    report_archive_relative_path,
    stable_report_id,
)


class JournalTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.db_path = Path(self._temporary_directory.name) / "state" / "journal.db"

    def test_exact_raw_message_replay_is_idempotent(self) -> None:
        at = datetime(2026, 8, 14, 10, 0, tzinfo=ZoneInfo("America/New_York"))

        with Journal.open(self.db_path) as journal:
            first_id, first_duplicate = journal.append_raw_message(
                "msg-1", at, "SKIPPED SPY"
            )
            second_id, second_duplicate = journal.append_raw_message(
                "msg-1", at, "SKIPPED SPY"
            )

            self.assertEqual(first_id, second_id)
            self.assertFalse(first_duplicate)
            self.assertTrue(second_duplicate)
            self.assertEqual(journal.count("raw_messages"), 1)

    def test_money_columns_are_integer_backed(self) -> None:
        with Journal.open(self.db_path) as journal:
            columns = journal.table_info("execution_events")

        self.assertEqual(columns["price_micros"].upper(), "INTEGER")

    def test_open_configures_a_bounded_durable_sqlite_connection(self) -> None:
        journal = Journal.open(self.db_path)

        self.assertTrue(self.db_path.parent.is_dir())
        self.assertEqual(journal.pragma("journal_mode"), "wal")
        self.assertEqual(journal.pragma("foreign_keys"), 1)
        self.assertEqual(journal.pragma("recursive_triggers"), 1)
        self.assertEqual(journal.pragma("busy_timeout"), 5_000)
        self.assertEqual(journal.pragma("synchronous"), 2)
        journal.close()

        with self.assertRaises(JournalError):
            journal.count("raw_messages")

    def test_busy_open_is_translated_and_closes_its_failed_connection(self) -> None:
        with Journal.open(self.db_path):
            pass
        with closing(
            sqlite3.connect(self.db_path, isolation_level=None)
        ) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            with patch("stock_monitor.journal.BUSY_TIMEOUT_MILLISECONDS", 10):
                with self.assertRaises(JournalBusy):
                    Journal.open(self.db_path)
            blocker.rollback()

        with Journal.open(self.db_path) as journal:
            self.assertEqual(journal.count("schema_migrations"), 1)

    def test_raw_message_identity_is_exact_and_content_is_retained(self) -> None:
        at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        equivalent = datetime(
            2026, 8, 14, 10, 0, tzinfo=timezone(timedelta(hours=-4))
        )
        with Journal.open(self.db_path) as journal:
            first_id, _ = journal.append_raw_message("Message-A", at, "SKIPPED SPY")
            replay_id, duplicate = journal.append_raw_message(
                "Message-A", equivalent, "SKIPPED SPY"
            )
            second_id, _ = journal.append_raw_message(
                "message-a", at, "SKIPPED SPY"
            )

            self.assertEqual(first_id, replay_id)
            self.assertTrue(duplicate)
            self.assertNotEqual(first_id, second_id)
            self.assertEqual(journal.count("raw_messages"), 2)
            with self.assertRaises(IdempotencyConflict):
                journal.append_raw_message("Message-A", at, "BOUGHT SPY")
            with self.assertRaises(IdempotencyConflict):
                journal.append_raw_message(
                    "Message-A", at + timedelta(microseconds=1), "SKIPPED SPY"
                )

    def test_raw_message_rejects_noncanonical_timestamp_inputs(self) -> None:
        invalid_times: tuple[object, ...] = (
            datetime(2026, 8, 14, 10, 0),
            date(2026, 8, 14),
            True,
            "2026-08-14T14:00:00Z",
        )
        with Journal.open(self.db_path) as journal:
            for invalid in invalid_times:
                with self.subTest(invalid=type(invalid).__name__):
                    with self.assertRaises(InvalidJournalValue):
                        journal.append_raw_message(
                            "msg-invalid", invalid, "SKIPPED SPY"  # type: ignore[arg-type]
                        )
            self.assertEqual(journal.count("raw_messages"), 0)

    def test_transaction_rolls_back_base_exceptions_and_rejects_nesting(self) -> None:
        at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with self.assertRaises(KeyboardInterrupt):
                with journal.transaction() as transaction:
                    transaction.append_raw_message("msg-rollback", at, "SKIPPED SPY")
                    raise KeyboardInterrupt
            self.assertEqual(journal.count("raw_messages"), 0)

            with self.assertRaises(JournalError):
                with journal.transaction() as transaction:
                    transaction.append_raw_message("msg-nested", at, "SKIPPED QQQ")
                    with journal.transaction():
                        pass
            self.assertEqual(journal.count("raw_messages"), 0)

    def test_transaction_handle_cannot_write_after_its_context_exits(self) -> None:
        at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with journal.transaction() as transaction:
                pass

            with self.assertRaises(JournalError):
                transaction.append_raw_message(
                    "msg-after-commit", at, "SKIPPED SPY"
                )
            self.assertEqual(journal.count("raw_messages"), 0)

    def test_close_during_transaction_raises_domain_error_and_unwinds_safely(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        journal = Journal.open(self.db_path)

        with self.assertRaises(JournalError):
            with journal.transaction() as transaction:
                transaction.append_raw_message(
                    "msg-close-rollback", now, "SKIPPED SPY"
                )
                journal.close()

        self.assertEqual(journal.count("raw_messages"), 0)
        journal.append_raw_message("msg-after-close-rejection", now, "SKIPPED QQQ")
        journal.close()
        journal.close()

        with Journal.open(self.db_path) as reopened:
            self.assertEqual(reopened.count("raw_messages"), 1)

    def test_caught_close_error_leaves_active_transaction_usable(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with journal.transaction() as transaction:
                transaction.append_raw_message(
                    "msg-before-close-rejection", now, "SKIPPED SPY"
                )
                with self.assertRaises(JournalError):
                    journal.close()
                transaction.append_raw_message(
                    "msg-after-close-rejection", now, "SKIPPED QQQ"
                )

            self.assertEqual(journal.count("raw_messages"), 2)

    def test_public_transaction_convenience_signatures_are_explicit_and_typed(
        self,
    ) -> None:
        method_names = (
            "append_execution_event",
            "append_source_observation",
            "finalize_report",
            "append_outbox",
            "record_outbox_delivery_attempt",
            "start_scheduled_run",
            "complete_scheduled_run",
            "append_account_check",
        )
        for method_name in method_names:
            with self.subTest(method=method_name):
                journal_signature = inspect.signature(getattr(Journal, method_name))
                transaction_signature = inspect.signature(
                    getattr(journal_module.JournalTransaction, method_name)
                )
                journal_parameters = tuple(journal_signature.parameters.values())[1:]
                transaction_parameters = tuple(
                    transaction_signature.parameters.values()
                )[1:]
                self.assertEqual(journal_parameters, transaction_parameters)
                self.assertEqual(
                    journal_signature.return_annotation,
                    transaction_signature.return_annotation,
                )
                self.assertNotIn(
                    inspect.Parameter.VAR_KEYWORD,
                    {parameter.kind for parameter in journal_parameters},
                )

    def test_invalid_public_wrapper_arguments_fail_before_opening_transaction(
        self,
    ) -> None:
        with Journal.open(self.db_path) as journal, patch.object(
            journal, "transaction"
        ) as transaction:
            with self.assertRaises(TypeError):
                journal.append_outbox()
            with self.assertRaises(TypeError):
                journal.append_outbox(unknown="value")  # type: ignore[call-arg]
            with self.assertRaises(TypeError):
                journal.append_outbox({})  # type: ignore[arg-type]
            transaction.assert_not_called()

    def test_explicit_public_wrapper_accepts_a_keyword_mapping(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-wrapper-mapping", now, "SKIPPED SPY"
            )
            values = {
                "raw_message_id": raw_id,
                "action_ordinal": 0,
                "parsed_action": "SKIPPED",
                "event_time": now,
                "symbol": "SPY",
            }
            event_id, duplicate = journal.append_execution_event(**values)

            self.assertGreater(event_id, 0)
            self.assertFalse(duplicate)
            self.assertEqual(journal.count("execution_events"), 1)

    def test_execution_events_use_stable_zero_based_action_ordinals(self) -> None:
        at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-actions", at, "SKIPPED SPY; SKIPPED QQQ"
            )
            with journal.transaction() as transaction:
                first_id, first_duplicate = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SKIPPED",
                    event_time=at,
                    symbol="SPY",
                )
                second_id, _ = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=1,
                    parsed_action="SKIPPED",
                    event_time=at,
                    symbol="QQQ",
                )
            with journal.transaction() as transaction:
                replay_id, replay_duplicate = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="SKIPPED",
                    event_time=at,
                    symbol="SPY",
                )

            self.assertNotEqual(first_id, second_id)
            self.assertEqual(first_id, replay_id)
            self.assertFalse(first_duplicate)
            self.assertTrue(replay_duplicate)
            self.assertEqual(journal.count("execution_events"), 2)
            with self.assertRaises(IdempotencyConflict):
                with journal.transaction() as transaction:
                    transaction.append_execution_event(
                        raw_message_id=raw_id,
                        action_ordinal=0,
                        parsed_action="BOUGHT",
                        event_time=at,
                        symbol="SPY",
                    )

    def test_execution_event_cannot_postdate_its_authoritative_message(self) -> None:
        message_time = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        future_event_time = message_time + timedelta(days=1)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-future-event", message_time, "BOUGHT SPY"
            )
            with self.assertRaises(InvalidJournalValue):
                journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="BOUGHT",
                    event_time=future_event_time,
                    signal_id="signal-future",
                    symbol="SPY",
                    shares=1,
                    price_micros=100_000_000,
                )

        with closing(sqlite3.connect(self.db_path, isolation_level=None)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA recursive_triggers = ON")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO execution_events("
                    "event_id, raw_message_id, action_ordinal, idempotency_key, "
                    "signal_id, parsed_action, symbol, shares, price_micros, "
                    "event_time, message_time, compliance_result, "
                    "reconciliation_state, details_json"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "evt_future",
                        raw_id,
                        0,
                        "message-action:future",
                        "signal-future",
                        "BOUGHT",
                        "SPY",
                        1,
                        100_000_000,
                        "2026-08-15T14:00:00.000000Z",
                        "2026-08-14T14:00:00.000000Z",
                        "ALLOWED",
                        "CLEAR",
                        "{}",
                    ),
                )

    def test_money_inputs_reject_nonintegers_invalid_signs_and_overflow(self) -> None:
        at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-money", at, "BOUGHT SPY")
            invalid_values: tuple[object, ...] = (True, 1.5, 0, -1, 2**63)
            for invalid in invalid_values:
                with self.subTest(price_micros=invalid):
                    with self.assertRaises(InvalidJournalValue):
                        journal.append_execution_event(
                            raw_message_id=raw_id,
                            action_ordinal=0,
                            parsed_action="BOUGHT",
                            event_time=at,
                            symbol="SPY",
                            price_micros=invalid,  # type: ignore[arg-type]
                        )
            self.assertEqual(journal.count("execution_events"), 0)

    def test_source_observation_distinguishes_payload_and_observation_identity(self) -> None:
        source_at = datetime(2026, 8, 14, 13, 59, tzinfo=timezone.utc)
        retrieved_at = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        values = {
            "payload": b'{"price":"100.00"}',
            "source_uri": "https://example.test/quotes/SPY",
            "source_type": "MARKET_QUOTE",
            "provider": "fixture",
            "feed": "SIP",
            "source_time": source_at,
            "retrieved_at": retrieved_at,
            "provider_sequence": 7,
            "delay_seconds": 60,
            "health_result": "OK",
            "details": {"symbol": "SPY", "eligible": True},
        }
        with Journal.open(self.db_path) as journal:
            first_id, first_duplicate = journal.append_source_observation(**values)
            replay_id, replay_duplicate = journal.append_source_observation(**values)
            distinct_id, _ = journal.append_source_observation(
                **{**values, "source_uri": "https://example.test/quotes/QQQ"}
            )

            self.assertEqual(first_id, replay_id)
            self.assertFalse(first_duplicate)
            self.assertTrue(replay_duplicate)
            self.assertNotEqual(first_id, distinct_id)
            self.assertEqual(journal.count("source_observations"), 2)

    def test_details_json_cannot_hide_money_or_accept_the_wrong_shape(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        values = {
            "payload": b"payload",
            "source_uri": "https://example.test/source",
            "source_type": "HEALTH",
            "provider": "fixture",
            "feed": None,
            "source_time": now,
            "retrieved_at": now,
            "provider_sequence": None,
            "delay_seconds": None,
            "health_result": "OK",
        }
        with Journal.open(self.db_path) as journal:
            with self.assertRaises(InvalidJournalValue):
                journal.append_source_observation(
                    **values, details={"last_price_micros": 100_000_000}
                )
            with self.assertRaises(InvalidJournalValue):
                journal.append_source_observation(
                    **values, details=[]  # type: ignore[arg-type]
                )
            self.assertEqual(journal.count("source_observations"), 0)

    def test_report_claims_serialize_and_recover_expired_leases(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as first, Journal.open(self.db_path) as second:
            with patch.object(journal_module, "_utc_now", return_value=now):
                acquired = first.claim_report(session_date, "CLOSE")
            with patch.object(
                journal_module,
                "_utc_now",
                return_value=now + timedelta(seconds=299),
            ):
                in_progress = second.claim_report(session_date, "CLOSE")
            with patch.object(
                journal_module,
                "_utc_now",
                return_value=now + timedelta(seconds=300),
            ):
                recovered = second.claim_report(session_date, "CLOSE")

            self.assertEqual(acquired.status, "ACQUIRED")
            self.assertIsNotNone(acquired.claim_token)
            self.assertEqual(in_progress.status, "IN_PROGRESS")
            self.assertIsNone(in_progress.claim_token)
            self.assertEqual(recovered.status, "RECOVERED_EXPIRED")
            self.assertNotEqual(acquired.claim_token, recovered.claim_token)
            self.assertEqual(first.count("report_claims"), 1)

            with self.assertRaises(InvalidJournalValue):
                first.claim_report(session_date, "CLOSE ")

    def test_report_claim_uses_internal_clock_and_recovers_expired_lease(self) -> None:
        session_date = date(2026, 8, 14)
        lease_start = datetime(2030, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with patch.object(
                journal_module, "_utc_now", return_value=lease_start, create=True
            ):
                acquired = journal.claim_report(session_date, "CLOSE")
            with patch.object(
                journal_module,
                "_utc_now",
                return_value=lease_start + timedelta(seconds=299),
                create=True,
            ):
                in_progress = journal.claim_report(session_date, "CLOSE")
            with patch.object(
                journal_module,
                "_utc_now",
                return_value=lease_start + timedelta(seconds=300),
                create=True,
            ):
                recovered = journal.claim_report(session_date, "CLOSE")

        self.assertEqual(acquired.status, "ACQUIRED")
        self.assertEqual(in_progress.status, "IN_PROGRESS")
        self.assertEqual(recovered.status, "RECOVERED_EXPIRED")
        self.assertNotEqual(acquired.claim_token, recovered.claim_token)

    def test_report_claim_authority_clock_is_not_a_public_argument(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with self.assertRaises(TypeError):
                journal.claim_report(session_date, "CLOSE", now=now)
            with self.assertRaises(TypeError):
                journal.claim_report(session_date, "CLOSE", lease_seconds=1)

    def test_report_claim_samples_authority_clock_after_write_lock(self) -> None:
        instant = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            def clock_after_lock() -> datetime:
                self.assertTrue(journal._transaction_active)
                return instant

            with patch.object(
                journal_module, "_utc_now", side_effect=clock_after_lock
            ):
                claim = journal.claim_report(date(2026, 8, 14), "CLOSE")

        self.assertEqual(claim.status, "ACQUIRED")

    def test_report_finalization_uses_only_the_internal_authority_clock(self) -> None:
        session_date = date(2026, 8, 14)
        lease_start = datetime.now(timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        values = {
            "body": "# Close\n",
            "state_sha256": state_sha256,
            "observation_ids": (),
            "archive_relative_path": report_archive_relative_path(
                "CLOSE", session_date, report_id
            ),
            "created_at": lease_start,
            "outbox_destination": "CODEX_TASK",
            "outbox_payload": "close",
        }
        with Journal.open(self.db_path) as journal:
            with patch.object(
                journal_module, "_utc_now", return_value=lease_start, create=True
            ):
                claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None

            with self.assertRaises(TypeError):
                journal.finalize_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    finalized_at=lease_start + timedelta(seconds=1),
                    **values,
                )

            with patch.object(
                journal_module,
                "_utc_now",
                return_value=lease_start + timedelta(seconds=300),
                create=True,
            ):
                with self.assertRaises(IdempotencyConflict):
                    journal.finalize_report(
                        claim_id=claim.claim_id,
                        claim_token=claim.claim_token,
                        **values,
                    )

    def test_report_finalization_samples_clock_after_audit_material(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, 1, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        order: list[str] = []
        real_canonical_json = journal_module._canonical_json

        def record_json(value: object) -> str:
            order.append("audit-material")
            return real_canonical_json(value)

        def record_clock() -> datetime:
            order.append("authority-clock")
            return now

        with patch.object(
            journal_module, "_canonical_json", side_effect=record_json
        ), patch.object(
            journal_module, "_utc_now", side_effect=record_clock
        ), Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            order.clear()
            journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=report_archive_relative_path(
                    "CLOSE", session_date, report_id
                ),
                created_at=now,
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )

        self.assertEqual(order[-1], "authority-clock")

    def test_two_connections_racing_for_a_report_claim_get_one_owner(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path):
            pass
        barrier = Barrier(2)

        def claim() -> str:
            with Journal.open(self.db_path) as journal:
                barrier.wait()
                return journal.claim_report(session_date, "CLOSE").status

        with patch.object(journal_module, "_utc_now", return_value=now):
            with ThreadPoolExecutor(max_workers=2) as executor:
                statuses = tuple(executor.map(lambda _: claim(), range(2)))

        self.assertEqual(sorted(statuses), ["ACQUIRED", "IN_PROGRESS"])

    def test_two_connections_racing_to_recover_one_expired_claim_get_one_owner(self) -> None:
        session_date = date(2026, 8, 14)
        lease_start = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with patch.object(
            journal_module, "_utc_now", return_value=lease_start
        ), Journal.open(self.db_path) as journal:
            journal.claim_report(session_date, "CLOSE")
        barrier = Barrier(2)

        def recover() -> str:
            with Journal.open(self.db_path) as journal:
                barrier.wait()
                return journal.claim_report(session_date, "CLOSE").status

        with patch.object(
            journal_module,
            "_utc_now",
            return_value=lease_start + timedelta(seconds=300),
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                statuses = tuple(executor.map(lambda _: recover(), range(2)))

        self.assertEqual(sorted(statuses), ["IN_PROGRESS", "RECOVERED_EXPIRED"])

    def test_report_finalization_requires_the_current_token_and_active_lease(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        with patch.object(
            journal_module, "_utc_now", return_value=now
        ) as clock, Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            values = {
                "claim_id": claim.claim_id,
                "claim_token": claim.claim_token,
                "body": "# Close\n",
                "state_sha256": state_sha256,
                "observation_ids": (),
                "archive_relative_path": report_archive_relative_path(
                    "CLOSE", session_date, report_id
                ),
                "outbox_destination": "CODEX_TASK",
                "outbox_payload": "close",
            }

            clock.return_value = now + timedelta(seconds=301)
            with self.assertRaises(IdempotencyConflict):
                journal.finalize_report(
                    **values, created_at=now
                )

            recovered = journal.claim_report(session_date, "CLOSE")
            assert recovered.claim_token is not None
            with self.assertRaises(IdempotencyConflict):
                journal.finalize_report(
                    **values, created_at=now + timedelta(seconds=301)
                )

            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    **{
                        **values,
                        "claim_token": recovered.claim_token,
                        "archive_relative_path": "reports/bad\x00path.md",
                    },
                    created_at=now + timedelta(seconds=301),
                )

            finalized = journal.finalize_report(
                **{**values, "claim_token": recovered.claim_token},
                created_at=now + timedelta(seconds=301),
            )
            self.assertFalse(finalized.duplicate)

    def test_report_finalization_uses_internal_clock_for_lease_authority(self) -> None:
        session_date = date(2026, 8, 14)
        lease_start = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        values = {
            "body": "# Close\n",
            "state_sha256": state_sha256,
            "observation_ids": (),
            "archive_relative_path": report_archive_relative_path(
                "CLOSE", session_date, report_id
            ),
            "outbox_destination": "CODEX_TASK",
            "outbox_payload": "close",
        }
        with patch.object(
            journal_module, "_utc_now", return_value=lease_start
        ) as clock, Journal.open(self.db_path) as journal:
            stale = journal.claim_report(session_date, "CLOSE")
            assert stale.claim_token is not None
            clock.return_value = lease_start + timedelta(seconds=300)
            with self.assertRaises(IdempotencyConflict):
                journal.finalize_report(
                    claim_id=stale.claim_id,
                    claim_token=stale.claim_token,
                    created_at=lease_start + timedelta(seconds=1),
                    **values,
                )

            recovered = journal.claim_report(session_date, "CLOSE")
            assert recovered.claim_token is not None
            clock.return_value = lease_start + timedelta(seconds=301)
            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    claim_id=recovered.claim_id,
                    claim_token=recovered.claim_token,
                    created_at=lease_start + timedelta(seconds=302),
                    **values,
                )

            finalized = journal.finalize_report(
                claim_id=recovered.claim_id,
                claim_token=recovered.claim_token,
                created_at=lease_start + timedelta(seconds=1),
                **values,
            )
            self.assertFalse(finalized.duplicate)

    def test_report_finalization_rejects_the_exact_lease_expiry(self) -> None:
        session_date = date(2026, 8, 14)
        lease_start = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        with patch.object(
            journal_module, "_utc_now", return_value=lease_start
        ) as clock, Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            clock.return_value = lease_start + timedelta(seconds=300)
            with self.assertRaises(IdempotencyConflict):
                journal.finalize_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    body="# Close\n",
                    state_sha256=state_sha256,
                    observation_ids=(),
                    archive_relative_path=report_archive_relative_path(
                        "CLOSE", session_date, report_id
                    ),
                    created_at=lease_start,
                    outbox_destination="CODEX_TASK",
                    outbox_payload="close",
                )

    def test_report_cannot_predate_its_original_claim(self) -> None:
        session_date = date(2026, 8, 14)
        claim_time = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        with patch.object(
            journal_module, "_utc_now", return_value=claim_time
        ) as clock, Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            clock.return_value = claim_time + timedelta(seconds=1)

            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    body="# Close\n",
                    state_sha256=state_sha256,
                    observation_ids=(),
                    archive_relative_path=report_archive_relative_path(
                        "CLOSE", session_date, report_id
                    ),
                    created_at=claim_time - timedelta(microseconds=1),
                    outbox_destination="CODEX_TASK",
                    outbox_payload="close",
                )

            self.assertEqual(journal.count("reports"), 0)
            self.assertEqual(journal.count("outbox"), 0)

            finalized = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=report_archive_relative_path(
                    "CLOSE", session_date, report_id
                ),
                created_at=claim_time,
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )
            self.assertFalse(finalized.duplicate)

    def test_report_finalization_atomically_pins_observations_and_outbox(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        observation_values = {
            "payload": b"payload",
            "source_uri": "https://example.test/source-a",
            "source_type": "HEALTH",
            "provider": "fixture",
            "feed": None,
            "source_time": now,
            "retrieved_at": now,
            "provider_sequence": None,
            "delay_seconds": None,
            "health_result": "OK",
        }
        with patch.object(
            journal_module, "_utc_now", return_value=now + timedelta(seconds=1)
        ), Journal.open(self.db_path) as journal:
            first_observation, _ = journal.append_source_observation(
                **observation_values
            )
            second_observation, _ = journal.append_source_observation(
                **{**observation_values, "source_uri": "https://example.test/source-b"}
            )
            future_observation, _ = journal.append_source_observation(
                **{
                    **observation_values,
                    "source_uri": "https://example.test/future",
                    "retrieved_at": now + timedelta(seconds=2),
                }
            )
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            values = {
                "claim_id": claim.claim_id,
                "claim_token": claim.claim_token,
                "body": "# Close report\n\nNO TRADE\n",
                "state_sha256": "a" * 64,
                "observation_ids": (second_observation, first_observation),
                "created_at": now + timedelta(seconds=1),
                "outbox_destination": "CODEX_TASK",
                "outbox_payload": "CLOSE: NO TRADE",
            }
            with closing(sqlite3.connect(self.db_path)) as connection:
                observation_sha256s = tuple(
                    str(row[0])
                    for row in connection.execute(
                        "SELECT observation_sha256 FROM source_observations "
                        "WHERE id IN (?, ?) ORDER BY observation_sha256",
                        (first_observation, second_observation),
                    )
                )
            report_id = stable_report_id(
                "CLOSE", session_date, observation_sha256s, values["state_sha256"]
            )
            values["archive_relative_path"] = report_archive_relative_path(
                "CLOSE", session_date, report_id
            )

            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    **{**values, "observation_ids": (future_observation,)}
                )
            self.assertEqual(journal.count("reports"), 0)

            with self.assertRaises(RuntimeError):
                with journal.transaction() as transaction:
                    transaction.finalize_report(**values)
                    raise RuntimeError("injected rollback")
            self.assertEqual(journal.count("reports"), 0)
            self.assertEqual(journal.count("report_observations"), 0)
            self.assertEqual(journal.count("outbox"), 0)

            finalized = journal.finalize_report(**values)
            replay = journal.finalize_report(
                **{**values, "observation_ids": tuple(reversed(values["observation_ids"]))}
            )

            self.assertFalse(finalized.duplicate)
            self.assertTrue(replay.duplicate)
            self.assertEqual(finalized.report_id, replay.report_id)
            self.assertEqual(journal.count("reports"), 1)
            self.assertEqual(journal.count("report_observations"), 2)
            self.assertEqual(journal.count("outbox"), 1)
            self.assertEqual(
                journal.claim_report(session_date, "CLOSE").status,
                "ALREADY_FINALIZED",
            )

    def test_report_id_and_archive_path_match_the_task10_contract(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            observation_ids = []
            for suffix in ("a", "b"):
                observation_id, _ = journal.append_source_observation(
                    payload=f"payload-{suffix}".encode(),
                    source_uri=f"https://example.test/source-{suffix}",
                    source_type="MARKET_DATA",
                    provider="fixture",
                    feed=None,
                    source_time=now,
                    retrieved_at=now,
                    provider_sequence=None,
                    delay_seconds=None,
                    health_result="OK",
                )
                observation_ids.append(observation_id)
            with closing(sqlite3.connect(self.db_path)) as connection:
                observation_sha256s = tuple(
                    str(row[0])
                    for row in connection.execute(
                        "SELECT observation_sha256 FROM source_observations "
                        "ORDER BY observation_sha256"
                    )
                )
            canonical = json.dumps(
                {
                    "kind": "CLOSE",
                    "session": session_date.isoformat(),
                    "observations": sorted(observation_sha256s),
                    "state": state_sha256,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            expected_report_id = hashlib.sha256(canonical.encode()).hexdigest()
            expected_path = (
                "reports/2026/08/14/close-2026-08-14-"
                f"{expected_report_id[:12]}.md"
            )
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None

            finalized = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=tuple(reversed(observation_ids)),
                archive_relative_path=expected_path,
                created_at=now + timedelta(seconds=1),
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )

            self.assertEqual(finalized.report_id, expected_report_id)

    def test_report_identity_helpers_reject_noncanonical_kind(self) -> None:
        session_date = date(2026, 8, 14)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)

        invalid_kinds: tuple[object, ...] = (
            "close",
            "Close",
            " CLOSE",
            "CLOSE ",
            "CLOSÉ",
            "1CLOSE",
            "CLOSE-DAY",
            "",
            None,
        )
        for invalid_kind in invalid_kinds:
            with self.subTest(invalid_kind=invalid_kind):
                with self.assertRaises(InvalidJournalValue):
                    stable_report_id(  # type: ignore[arg-type]
                        invalid_kind, session_date, (), state_sha256
                    )
                with self.assertRaises(InvalidJournalValue):
                    report_archive_relative_path(  # type: ignore[arg-type]
                        invalid_kind, session_date, report_id
                    )

        canonical_id = stable_report_id(
            "CLOSE_2", session_date, (), state_sha256
        )
        self.assertIn(
            "/close_2-2026-08-14-",
            report_archive_relative_path("CLOSE_2", session_date, canonical_id),
        )

    def test_report_claim_requires_canonical_kind(self) -> None:
        with Journal.open(self.db_path) as journal:
            for invalid_kind in ("close", "Close", " CLOSE", "CLOSE ", "CLOSÉ"):
                with self.subTest(invalid_kind=invalid_kind):
                    with self.assertRaises(InvalidJournalValue):
                        journal.claim_report(date(2026, 8, 14), invalid_kind)
            self.assertEqual(journal.count("report_claims"), 0)

    def test_report_finalization_rejects_a_nondeterministic_archive_path(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None

            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    body="# Close\n",
                    state_sha256="a" * 64,
                    observation_ids=(),
                    archive_relative_path="reports/2026/08/14/close.md",
                    created_at=now + timedelta(seconds=1),
                    outbox_destination="CODEX_TASK",
                    outbox_payload="close",
                )

    def test_report_rejects_source_publication_after_its_created_at(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            observation_id, _ = journal.append_source_observation(
                payload=b"future publication",
                source_uri="https://example.test/future-publication",
                source_type="MARKET_DATA",
                provider="fixture",
                feed=None,
                source_time=now + timedelta(seconds=2),
                retrieved_at=now,
                provider_sequence=None,
                delay_seconds=None,
                health_result="OK",
            )
            with closing(sqlite3.connect(self.db_path)) as connection:
                observation_sha256 = str(
                    connection.execute(
                        "SELECT observation_sha256 FROM source_observations WHERE id = ?",
                        (observation_id,),
                    ).fetchone()[0]
                )
            report_id = stable_report_id(
                "CLOSE", session_date, (observation_sha256,), state_sha256
            )
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None

            with self.assertRaises(InvalidJournalValue):
                journal.finalize_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    body="# Close\n",
                    state_sha256=state_sha256,
                    observation_ids=(observation_id,),
                    archive_relative_path=report_archive_relative_path(
                        "CLOSE", session_date, report_id
                    ),
                    created_at=now + timedelta(seconds=1),
                    outbox_destination="CODEX_TASK",
                    outbox_payload="close",
                )

    def test_finalized_report_can_be_read_after_restart_for_archive_recovery(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        body = "# Close\n\nNO TRADE\n"
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            observation_id, _ = journal.append_source_observation(
                payload=b"recovery evidence",
                source_uri="https://example.test/recovery",
                source_type="MARKET_DATA",
                provider="fixture",
                feed=None,
                source_time=now,
                retrieved_at=now,
                provider_sequence=None,
                delay_seconds=None,
                health_result="OK",
            )
            with closing(sqlite3.connect(self.db_path)) as connection:
                observation_sha256 = str(
                    connection.execute(
                        "SELECT observation_sha256 FROM source_observations WHERE id = ?",
                        (observation_id,),
                    ).fetchone()[0]
                )
            report_id = stable_report_id(
                "CLOSE", session_date, (observation_sha256,), state_sha256
            )
            archive_path = report_archive_relative_path(
                "CLOSE", session_date, report_id
            )
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            finalized = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body=body,
                state_sha256=state_sha256,
                observation_ids=(observation_id,),
                archive_relative_path=archive_path,
                created_at=now + timedelta(seconds=1),
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )

        with Journal.open(self.db_path) as journal:
            existing = journal.claim_report(session_date, "CLOSE")
            self.assertEqual(existing.status, "ALREADY_FINALIZED")
            self.assertEqual(existing.report_row_id, finalized.report_row_id)
            self.assertEqual(existing.report_id, finalized.report_id)

            stored = journal.read_report(existing.report_id)
            self.assertEqual(stored.report_row_id, finalized.report_row_id)
            self.assertEqual(stored.report_id, report_id)
            self.assertEqual(stored.body, body)
            self.assertEqual(stored.archive_relative_path, archive_path)
            self.assertEqual(
                stored.content_sha256, hashlib.sha256(body.encode()).hexdigest()
            )
            self.assertEqual(stored.state_sha256, state_sha256)
            self.assertEqual(stored.observation_ids, (observation_id,))
            self.assertEqual(stored.observation_sha256s, (observation_sha256,))

    def test_outbox_recovers_pending_delivery_with_append_only_attempts(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-outbox", now, "SKIPPED SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            outbox_id, duplicate = journal.append_outbox(
                idempotency_key="event-delivery-msg-outbox-0",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="Recorded SKIPPED SPY",
                created_at=now,
            )

            self.assertFalse(duplicate)
            self.assertEqual([item.outbox_id for item in journal.pending_outbox()], [outbox_id])
            failed_id, failed_duplicate = journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=1,
                attempted_at=now + timedelta(seconds=1),
                delivery_status="FAILED",
                error_class="NETWORK",
            )
            replay_id, replay_duplicate = journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=1,
                attempted_at=now + timedelta(seconds=1),
                delivery_status="FAILED",
                error_class="NETWORK",
            )
            self.assertEqual(failed_id, replay_id)
            self.assertFalse(failed_duplicate)
            self.assertTrue(replay_duplicate)
            self.assertEqual([item.outbox_id for item in journal.pending_outbox()], [outbox_id])

            journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=2,
                attempted_at=now + timedelta(seconds=2),
                delivery_status="DELIVERED",
                external_delivery_id="delivery-1",
            )
            self.assertEqual(journal.pending_outbox(), ())
            with self.assertRaises(IdempotencyConflict):
                journal.record_outbox_delivery_attempt(
                    outbox_id=outbox_id,
                    attempt_ordinal=3,
                    attempted_at=now + timedelta(seconds=3),
                    delivery_status="DELIVERED",
                    external_delivery_id="delivery-2",
                )

    def test_report_outbox_is_effectively_once_per_destination(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=report_archive_relative_path(
                    "CLOSE", session_date, report_id
                ),
                created_at=now + timedelta(seconds=1),
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )

            with self.assertRaises(IdempotencyConflict):
                journal.append_outbox(
                    idempotency_key="different-report-delivery-key",
                    origin_report_id=report.report_row_id,
                    origin_execution_event_id=None,
                    destination="CODEX_TASK",
                    payload_text="duplicate close",
                    created_at=now + timedelta(seconds=2),
                )
            self.assertEqual(journal.count("outbox"), 1)

    def test_execution_event_outbox_is_effectively_once_per_destination(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-event-outbox", now, "SKIPPED SPY"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            first_id, first_duplicate = journal.append_outbox(
                idempotency_key="event-outbox-primary",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="recorded",
                created_at=now,
            )
            replay_id, replay_duplicate = journal.append_outbox(
                idempotency_key="event-outbox-primary",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="recorded",
                created_at=now,
            )

            self.assertEqual(first_id, replay_id)
            self.assertFalse(first_duplicate)
            self.assertTrue(replay_duplicate)
            with self.assertRaises(IdempotencyConflict):
                journal.append_outbox(
                    idempotency_key="event-outbox-conflicting-key",
                    origin_report_id=None,
                    origin_execution_event_id=event_id,
                    destination="CODEX_TASK",
                    payload_text="duplicate delivery",
                    created_at=now,
                )
            self.assertEqual(journal.count("outbox"), 1)

            journal.append_outbox(
                idempotency_key="event-outbox-other-destination",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="AUDIT_EXPORT",
                payload_text="recorded",
                created_at=now,
            )
            second_raw_id, _ = journal.append_raw_message(
                "msg-event-outbox-two", now, "SKIPPED QQQ"
            )
            second_event_id, _ = journal.append_execution_event(
                raw_message_id=second_raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="QQQ",
            )
            journal.append_outbox(
                idempotency_key="event-outbox-other-event",
                origin_report_id=None,
                origin_execution_event_id=second_event_id,
                destination="CODEX_TASK",
                payload_text="recorded",
                created_at=now,
            )
            self.assertEqual(journal.count("outbox"), 3)

    def test_pending_outbox_exposes_next_attempt_after_restart(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-restart", now, "SKIPPED SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            outbox_id, _ = journal.append_outbox(
                idempotency_key="event-restart",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="restart",
                created_at=now,
            )
            journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=1,
                attempted_at=now + timedelta(seconds=1),
                delivery_status="FAILED",
                error_class="NETWORK",
            )

        with Journal.open(self.db_path) as journal:
            pending = journal.pending_outbox()
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].next_attempt_ordinal, 2)
            journal.record_outbox_delivery_attempt(
                outbox_id=pending[0].outbox_id,
                attempt_ordinal=pending[0].next_attempt_ordinal,
                attempted_at=now + timedelta(seconds=2),
                delivery_status="DELIVERED",
                external_delivery_id="delivery-restart",
            )
            self.assertEqual(journal.pending_outbox(), ())

    def test_pending_outbox_validates_report_and_event_origins_before_return(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        finalized_at = now + timedelta(seconds=1)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        with patch.object(journal_module, "_utc_now", return_value=now) as clock, Journal.open(
            self.db_path
        ) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            clock.return_value = finalized_at
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=report_archive_relative_path(
                    "CLOSE", session_date, report_id
                ),
                created_at=finalized_at,
                outbox_destination="CODEX_TASK",
                outbox_payload="report pending",
            )
            raw_id, _ = journal.append_raw_message(
                "msg-valid-pending", now, "SKIPPED SPY"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            event_outbox_id, _ = journal.append_outbox(
                idempotency_key="valid-unicode-pending",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="recorded ✓",
                created_at=finalized_at,
            )
            with closing(sqlite3.connect(self.db_path, isolation_level=None)) as connection:
                empty_payload_cursor = connection.execute(
                    "INSERT INTO outbox("
                    "idempotency_key, origin_report_id, origin_execution_event_id, "
                    "destination, payload_text, payload_sha256, created_at"
                    ") VALUES (?, NULL, ?, ?, '', ?, ?)",
                    (
                        "valid-empty-pending",
                        event_id,
                        "AUDIT_EXPORT",
                        hashlib.sha256(b"").hexdigest(),
                        "2026-08-14T14:00:01.000000Z",
                    ),
                )
                empty_payload_outbox_id = int(empty_payload_cursor.lastrowid)

            pending = journal.pending_outbox()
            self.assertEqual(
                {item.outbox_id for item in pending},
                {report.outbox_id, event_outbox_id, empty_payload_outbox_id},
            )
            self.assertEqual(
                {item.origin_report_id for item in pending if item.origin_report_id},
                {report.report_row_id},
            )
            self.assertEqual(
                {
                    item.origin_execution_event_id
                    for item in pending
                    if item.origin_execution_event_id
                },
                {event_id},
            )

    def test_pending_outbox_rejects_payload_hash_corruption_without_exposure(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        timestamp = "2026-08-14T14:00:00.000000Z"
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-corrupt-pending", now, "SKIPPED SPY"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            valid_id, _ = journal.append_outbox(
                idempotency_key="valid-before-corrupt",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="safe",
                created_at=now,
            )
            with closing(sqlite3.connect(self.db_path, isolation_level=None)) as connection:
                connection.execute(
                    "INSERT INTO outbox("
                    "idempotency_key, origin_report_id, origin_execution_event_id, "
                    "destination, payload_text, payload_sha256, created_at"
                    ") VALUES (?, NULL, ?, ?, ?, ?, ?)",
                    (
                        "corrupt-after-valid",
                        event_id,
                        "AUDIT_EXPORT",
                        "do not expose",
                        "0" * 64,
                        timestamp,
                    ),
                )

            with self.assertRaises(MigrationCorruption):
                journal.pending_outbox()
            self.assertGreater(valid_id, 0)

    def test_pending_outbox_rejects_orphaned_origin_without_exposure(self) -> None:
        payload = "orphaned"
        with Journal.open(self.db_path) as journal:
            with closing(sqlite3.connect(self.db_path, isolation_level=None)) as connection:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute(
                    "INSERT INTO outbox("
                    "idempotency_key, origin_report_id, origin_execution_event_id, "
                    "destination, payload_text, payload_sha256, created_at"
                    ") VALUES (?, NULL, ?, ?, ?, ?, ?)",
                    (
                        "orphaned-pending",
                        999_999,
                        "CODEX_TASK",
                        payload,
                        hashlib.sha256(payload.encode()).hexdigest(),
                        "2026-08-14T14:00:00.000000Z",
                    ),
                )

            with self.assertRaises(MigrationCorruption):
                journal.pending_outbox()

    def test_pending_outbox_rejects_invalid_origin_shape_even_if_checks_are_ignored(
        self,
    ) -> None:
        payload = "invalid origin shape"
        payload_sha256 = hashlib.sha256(payload.encode()).hexdigest()
        for suffix, report_origin, event_origin in (
            ("neither", None, None),
            ("both", 1, 1),
        ):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "journal.db"
                with Journal.open(path) as journal:
                    with closing(sqlite3.connect(path, isolation_level=None)) as connection:
                        connection.execute("PRAGMA foreign_keys = OFF")
                        connection.execute("PRAGMA ignore_check_constraints = ON")
                        connection.execute(
                            "INSERT INTO outbox("
                            "idempotency_key, origin_report_id, "
                            "origin_execution_event_id, destination, payload_text, "
                            "payload_sha256, created_at"
                            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                f"invalid-origin-{suffix}",
                                report_origin,
                                event_origin,
                                "CODEX_TASK",
                                payload,
                                payload_sha256,
                                "2026-08-14T14:00:00.000000Z",
                            ),
                        )

                    with self.assertRaises(MigrationCorruption):
                        journal.pending_outbox()

    def test_outbox_attempts_are_contiguous_well_formed_and_terminal(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-attempts", now, "SKIPPED SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )
            outbox_id, _ = journal.append_outbox(
                idempotency_key="attempt-contract",
                origin_report_id=None,
                origin_execution_event_id=event_id,
                destination="CODEX_TASK",
                payload_text="recorded",
                created_at=now,
            )

            invalid_attempts = (
                {
                    "attempt_ordinal": 2,
                    "attempted_at": now,
                    "delivery_status": "FAILED",
                    "error_class": "NETWORK",
                },
                {
                    "attempt_ordinal": 1,
                    "attempted_at": now - timedelta(microseconds=1),
                    "delivery_status": "FAILED",
                    "error_class": "NETWORK",
                },
                {
                    "attempt_ordinal": 1,
                    "attempted_at": now,
                    "delivery_status": "DELIVERED",
                },
                {
                    "attempt_ordinal": 1,
                    "attempted_at": now,
                    "delivery_status": "FAILED",
                    "external_delivery_id": "unexpected",
                },
            )
            for values in invalid_attempts:
                with self.subTest(values=values):
                    with self.assertRaises(InvalidJournalValue):
                        journal.record_outbox_delivery_attempt(
                            outbox_id=outbox_id,
                            **values,
                        )

            journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=1,
                attempted_at=now + timedelta(seconds=1),
                delivery_status="FAILED",
                error_class="NETWORK",
            )
            with self.assertRaises(InvalidJournalValue):
                journal.record_outbox_delivery_attempt(
                    outbox_id=outbox_id,
                    attempt_ordinal=3,
                    attempted_at=now + timedelta(seconds=2),
                    delivery_status="FAILED",
                    error_class="NETWORK",
                )
            journal.record_outbox_delivery_attempt(
                outbox_id=outbox_id,
                attempt_ordinal=2,
                attempted_at=now + timedelta(seconds=2),
                delivery_status="DELIVERED",
                external_delivery_id="delivery-2",
            )
            with self.assertRaises(IdempotencyConflict):
                journal.record_outbox_delivery_attempt(
                    outbox_id=outbox_id,
                    attempt_ordinal=3,
                    attempted_at=now + timedelta(seconds=3),
                    delivery_status="FAILED",
                    error_class="NETWORK",
                )

    def test_scheduled_runs_preserve_starts_and_complete_only_once(self) -> None:
        session_date = date(2026, 8, 14)
        intended = datetime(2026, 8, 14, 19, 30, tzinfo=timezone.utc)
        started = intended + timedelta(seconds=2)
        with Journal.open(self.db_path) as journal:
            run_id, duplicate = journal.start_scheduled_run(
                run_key="close-2026-08-14",
                run_kind="CLOSE",
                session_date=session_date,
                intended_run_at=intended,
                started_at=started,
            )
            replay_id, replay_duplicate = journal.start_scheduled_run(
                run_key="close-2026-08-14",
                run_kind="close",
                session_date=session_date,
                intended_run_at=intended,
                started_at=started,
            )
            self.assertEqual(run_id, replay_id)
            self.assertFalse(duplicate)
            self.assertTrue(replay_duplicate)
            self.assertEqual(journal.count("scheduled_runs"), 1)

            completed_id, completed_duplicate = journal.complete_scheduled_run(
                run_id=run_id,
                finished_at=started + timedelta(seconds=10),
                market_session_decision="OPEN_NORMAL",
                outcome="NO_OP_ALREADY_EMITTED",
            )
            replay_completed_id, replay_completed_duplicate = (
                journal.complete_scheduled_run(
                    run_id=run_id,
                    finished_at=started + timedelta(seconds=10),
                    market_session_decision="OPEN_NORMAL",
                    outcome="NO_OP_ALREADY_EMITTED",
                )
            )
            self.assertEqual(completed_id, replay_completed_id)
            self.assertFalse(completed_duplicate)
            self.assertTrue(replay_completed_duplicate)
            with self.assertRaises(IdempotencyConflict):
                journal.complete_scheduled_run(
                    run_id=run_id,
                    finished_at=started + timedelta(seconds=11),
                    market_session_decision="OPEN_NORMAL",
                    outcome="REPORT_EMITTED",
                )

            missing_report_run_id, _ = journal.start_scheduled_run(
                run_key="missing-report-2026-08-14",
                run_kind="CLOSE",
                session_date=session_date,
                intended_run_at=intended,
                started_at=started,
            )
            with self.assertRaises(InvalidJournalValue):
                journal.complete_scheduled_run(
                    run_id=missing_report_run_id,
                    finished_at=started + timedelta(seconds=10),
                    market_session_decision="OPEN_NORMAL",
                    outcome="REPORT_EMITTED",
                )

    def test_scheduled_completion_report_must_match_the_run_identity_and_path(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        archive_path = report_archive_relative_path(
            "CLOSE", session_date, report_id
        )
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=now + timedelta(seconds=1),
        ), Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=archive_path,
                created_at=now + timedelta(seconds=1),
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )
            run_id, _ = journal.start_scheduled_run(
                run_key="premarket-2026-08-14",
                run_kind="PREMARKET",
                session_date=session_date,
                intended_run_at=now,
                started_at=now,
            )
            with self.assertRaises(InvalidJournalValue):
                journal.complete_scheduled_run(
                    run_id=run_id,
                    finished_at=now + timedelta(seconds=2),
                    market_session_decision="OPEN",
                    outcome="REPORT_EMITTED",
                    report_id=report.report_row_id,
                    report_path=archive_path,
                )

            close_run_id, _ = journal.start_scheduled_run(
                run_key="close-2026-08-14",
                run_kind="CLOSE",
                session_date=session_date,
                intended_run_at=now,
                started_at=now,
            )
            with self.assertRaises(InvalidJournalValue):
                journal.complete_scheduled_run(
                    run_id=close_run_id,
                    finished_at=now + timedelta(seconds=2),
                    market_session_decision="OPEN",
                    outcome="REPORT_EMITTED",
                    report_id=report.report_row_id,
                    report_path="reports/2026/08/14/wrong.md",
                )
            journal.complete_scheduled_run(
                run_id=close_run_id,
                finished_at=now + timedelta(seconds=2),
                market_session_decision="OPEN",
                outcome="REPORT_EMITTED",
                report_id=report.report_row_id,
                report_path=archive_path,
            )

    def test_scheduled_emission_requires_finalization_within_the_run(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        finalized_at = now + timedelta(seconds=10)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", session_date, (), state_sha256)
        archive_path = report_archive_relative_path(
            "CLOSE", session_date, report_id
        )
        with patch.object(
            journal_module, "_utc_now", return_value=now
        ) as clock, Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE")
            assert claim.claim_token is not None
            clock.return_value = finalized_at
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=archive_path,
                created_at=now + timedelta(seconds=1),
                outbox_destination="CODEX_TASK",
                outbox_payload="close",
            )
            run_id, _ = journal.start_scheduled_run(
                run_key="close-impossible-chronology",
                run_kind="CLOSE",
                session_date=session_date,
                intended_run_at=now,
                started_at=now,
            )

            with self.assertRaises(InvalidJournalValue):
                journal.complete_scheduled_run(
                    run_id=run_id,
                    finished_at=now + timedelta(seconds=2),
                    market_session_decision="OPEN",
                    outcome="REPORT_EMITTED",
                    report_id=report.report_row_id,
                    report_path=archive_path,
                )

        with closing(sqlite3.connect(self.db_path, isolation_level=None)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA recursive_triggers = ON")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE scheduled_runs SET finished_at = ?, "
                    "market_session_decision = 'OPEN', outcome = 'REPORT_EMITTED', "
                    "report_id = ?, report_path = ? WHERE id = ?",
                    (
                        "2026-08-14T12:45:02.000000Z",
                        report.report_row_id,
                        archive_path,
                        run_id,
                    ),
                )

    def test_event_posting_projection_and_outbox_commit_as_one_unit(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)

        def write_unit(journal: Journal) -> None:
            with journal.transaction() as transaction:
                raw_id, _ = transaction.append_raw_message(
                    "msg-buy", now, "BOUGHT SPY 1 shares @ 100"
                )
                event_id, _ = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="BOUGHT",
                    event_time=now,
                    signal_id="signal-spy",
                    symbol="SPY",
                    shares=1,
                    price_micros=100_000_000,
                )
                posting_id, _ = transaction.append_ledger_posting(
                    posting_key="msg-buy:0:cash",
                    ledger_name="ACTUAL",
                    account_name="CASH",
                    entry_kind="BUY",
                    occurred_at=now,
                    amount_micros=-100_000_000,
                    execution_event_id=event_id,
                    symbol="SPY",
                    shares_delta=1,
                    unit_price_micros=100_000_000,
                )
                transaction.write_actual_position(
                    signal_id="signal-spy",
                    symbol="SPY",
                    shares=1,
                    cost_basis_micros=100_000_000,
                    recommended_stop_micros=97_500_000,
                    user_confirmed_stop_micros=None,
                    target_micros=105_000_000,
                    last_execution_event_id=event_id,
                    updated_at=now,
                )
                transaction.write_actual_cash_projection(
                    estimated_settled_cash_micros=4_900_000_000,
                    user_confirmed_settled_cash_micros=None,
                    deployed_capital_micros=100_000_000,
                    open_planned_risk_micros=2_500_000,
                    consecutive_losses=0,
                    weekly_high_water_micros=5_000_000_000,
                    monthly_high_water_micros=5_000_000_000,
                    last_ledger_posting_id=posting_id,
                    updated_at=now,
                )
                transaction.write_reconciliation_projection(
                    reconciliation_required=True,
                    reason="STOP UNVERIFIED",
                    last_execution_event_id=event_id,
                    updated_at=now,
                )
                transaction.append_outbox(
                    idempotency_key="msg-buy:0:recorded",
                    origin_report_id=None,
                    origin_execution_event_id=event_id,
                    destination="CODEX_TASK",
                    payload_text="BUY recorded; STOP UNVERIFIED",
                    created_at=now,
                )

        with Journal.open(self.db_path) as journal:
            with self.assertRaises(RuntimeError):
                with journal.transaction() as outer:
                    raw_id, _ = outer.append_raw_message(
                        "msg-rollback-unit", now, "BOUGHT SPY"
                    )
                    event_id, _ = outer.append_execution_event(
                        raw_message_id=raw_id,
                        action_ordinal=0,
                        parsed_action="BOUGHT",
                        event_time=now,
                        symbol="SPY",
                        shares=1,
                        price_micros=100_000_000,
                    )
                    posting_id, _ = outer.append_ledger_posting(
                        posting_key="rollback-posting",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="BUY",
                        occurred_at=now,
                        amount_micros=-100_000_000,
                        execution_event_id=event_id,
                        symbol="SPY",
                    )
                    outer.write_actual_cash_projection(
                        estimated_settled_cash_micros=4_900_000_000,
                        user_confirmed_settled_cash_micros=None,
                        deployed_capital_micros=100_000_000,
                        open_planned_risk_micros=2_500_000,
                        consecutive_losses=0,
                        weekly_high_water_micros=5_000_000_000,
                        monthly_high_water_micros=5_000_000_000,
                        last_ledger_posting_id=posting_id,
                        updated_at=now,
                    )
                    raise RuntimeError("injected rollback")
            self.assertEqual(journal.count("raw_messages"), 0)
            self.assertEqual(journal.count("ledger_postings"), 0)
            self.assertEqual(journal.count("actual_cash_projection"), 0)

            write_unit(journal)
            self.assertEqual(journal.count("raw_messages"), 1)
            self.assertEqual(journal.count("execution_events"), 1)
            self.assertEqual(journal.count("ledger_postings"), 1)
            self.assertEqual(journal.count("actual_positions"), 1)
            self.assertEqual(journal.count("actual_cash_projection"), 1)
            self.assertEqual(journal.count("reconciliation_projection"), 1)
            self.assertEqual(journal.count("outbox"), 1)

            resolution_raw_id, _ = journal.append_raw_message(
                "msg-reconciled", now + timedelta(seconds=1), "RECONCILED SPY"
            )
            resolution_event_id, _ = journal.append_execution_event(
                raw_message_id=resolution_raw_id,
                action_ordinal=0,
                parsed_action="ACCOUNT_CHECK",
                event_time=now + timedelta(seconds=1),
                reconciliation_state="CLEAR",
            )
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.write_reconciliation_projection(
                        reconciliation_required=False,
                        reason=None,
                        last_execution_event_id=None,
                        updated_at=now + timedelta(seconds=1),
                    )
            with journal.transaction() as transaction:
                transaction.write_reconciliation_projection(
                    reconciliation_required=False,
                    reason=None,
                    last_execution_event_id=resolution_event_id,
                    updated_at=now + timedelta(seconds=1),
                )
            sale_raw_id, _ = journal.append_raw_message(
                "msg-sold", now + timedelta(seconds=2), "SOLD SPY 1 shares"
            )
            sale_event_id, _ = journal.append_execution_event(
                raw_message_id=sale_raw_id,
                action_ordinal=0,
                parsed_action="SOLD",
                event_time=now + timedelta(seconds=2),
                signal_id="signal-spy",
                symbol="SPY",
                shares=1,
                price_micros=101_000_000,
            )
            with journal.transaction() as transaction:
                transaction.write_actual_position(
                    signal_id="signal-spy",
                    symbol="SPY",
                    shares=0,
                    cost_basis_micros=0,
                    recommended_stop_micros=None,
                    user_confirmed_stop_micros=None,
                    target_micros=None,
                    last_execution_event_id=sale_event_id,
                    updated_at=now + timedelta(seconds=2),
                )

        with closing(sqlite3.connect(self.db_path)) as connection:
            with self.assertRaises(sqlite3.DatabaseError):
                connection.execute(
                    "UPDATE actual_positions SET shares = 2 WHERE symbol = 'SPY'"
                )

    def test_actual_positions_are_per_signal_and_require_matching_mutation_events(
        self,
    ) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-two-lots", now, "BOUGHT SPY for two signals"
            )
            with journal.transaction() as transaction:
                first_event_id, _ = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="BOUGHT",
                    event_time=now,
                    signal_id="signal-one",
                    symbol="SPY",
                    shares=1,
                )
                second_event_id, _ = transaction.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=1,
                    parsed_action="BOUGHT",
                    event_time=now,
                    signal_id="signal-two",
                    symbol="SPY",
                    shares=2,
                )
                transaction.write_actual_position(
                    signal_id="signal-one",
                    symbol="SPY",
                    shares=1,
                    cost_basis_micros=100_000_000,
                    recommended_stop_micros=None,
                    user_confirmed_stop_micros=None,
                    target_micros=None,
                    last_execution_event_id=first_event_id,
                    updated_at=now,
                )
                transaction.write_actual_position(
                    signal_id="signal-two",
                    symbol="SPY",
                    shares=2,
                    cost_basis_micros=200_000_000,
                    recommended_stop_micros=None,
                    user_confirmed_stop_micros=None,
                    target_micros=None,
                    last_execution_event_id=second_event_id,
                    updated_at=now,
                )

            self.assertEqual(journal.count("actual_positions"), 2)
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.write_actual_position(
                        signal_id="signal-one",
                        symbol="SPY",
                        shares=2,
                        cost_basis_micros=200_000_000,
                        recommended_stop_micros=None,
                        user_confirmed_stop_micros=None,
                        target_micros=None,
                        last_execution_event_id=second_event_id,
                        updated_at=now,
                    )

    def test_actual_position_source_and_update_chronology_never_regress(self) -> None:
        base = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        source_time = base + timedelta(seconds=10)
        delayed_time = base + timedelta(seconds=5)
        updated_at = base + timedelta(seconds=20)
        with Journal.open(self.db_path) as journal:
            event_ids: list[int] = []
            for suffix, event_time in (
                ("initial", source_time),
                ("delayed", delayed_time),
                ("equal-time", source_time),
            ):
                raw_id, _ = journal.append_raw_message(
                    f"msg-position-{suffix}", event_time, "BOUGHT SPY"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="BOUGHT",
                    event_time=event_time,
                    signal_id="signal-position-time",
                    symbol="SPY",
                    shares=1,
                )
                event_ids.append(event_id)

            def write_position(
                event_id: int, *, shares: int, projection_time: datetime
            ) -> int:
                with journal.transaction() as transaction:
                    return transaction.write_actual_position(
                        signal_id="signal-position-time",
                        symbol="SPY",
                        shares=shares,
                        cost_basis_micros=shares * 100_000_000,
                        recommended_stop_micros=None,
                        user_confirmed_stop_micros=None,
                        target_micros=None,
                        last_execution_event_id=event_id,
                        updated_at=projection_time,
                    )

            with self.assertRaises(InvalidJournalValue):
                write_position(
                    event_ids[0], shares=1, projection_time=source_time - timedelta(microseconds=1)
                )
            self.assertEqual(
                write_position(event_ids[0], shares=1, projection_time=updated_at), 1
            )
            with self.assertRaises(IdempotencyConflict):
                write_position(
                    event_ids[1], shares=2, projection_time=updated_at + timedelta(seconds=1)
                )
            with self.assertRaises(IdempotencyConflict):
                write_position(
                    event_ids[2], shares=2, projection_time=updated_at - timedelta(microseconds=1)
                )
            self.assertEqual(
                write_position(event_ids[2], shares=2, projection_time=updated_at), 2
            )
            self.assertEqual(
                write_position(event_ids[2], shares=2, projection_time=updated_at), 2
            )

    def test_actual_cash_source_and_update_chronology_never_regress(self) -> None:
        base = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        source_time = base + timedelta(seconds=10)
        delayed_time = base + timedelta(seconds=5)
        updated_at = base + timedelta(seconds=20)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-cash-chronology", base, "RECONCILE CASH"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="RECONCILE_CASH",
                event_time=base,
                reconciliation_state="CLEAR",
            )
            posting_ids: list[int] = []
            with journal.transaction() as transaction:
                for suffix, occurred_at in (
                    ("initial", source_time),
                    ("delayed", delayed_time),
                    ("equal-time", source_time),
                ):
                    posting_id, _ = transaction.append_ledger_posting(
                        posting_key=f"cash-chronology-{suffix}",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="RECONCILIATION",
                        occurred_at=occurred_at,
                        amount_micros=0,
                        execution_event_id=event_id,
                    )
                    posting_ids.append(posting_id)

            def write_cash(
                posting_id: int, *, cash: int, projection_time: datetime
            ) -> int:
                with journal.transaction() as transaction:
                    return transaction.write_actual_cash_projection(
                        estimated_settled_cash_micros=cash,
                        user_confirmed_settled_cash_micros=None,
                        deployed_capital_micros=0,
                        open_planned_risk_micros=0,
                        consecutive_losses=0,
                        weekly_high_water_micros=cash,
                        monthly_high_water_micros=cash,
                        last_ledger_posting_id=posting_id,
                        updated_at=projection_time,
                    )

            self.assertEqual(
                write_cash(posting_ids[0], cash=5_000_000_000, projection_time=updated_at),
                1,
            )
            with self.assertRaises(IdempotencyConflict):
                write_cash(
                    posting_ids[1],
                    cash=5_100_000_000,
                    projection_time=updated_at + timedelta(seconds=1),
                )
            with self.assertRaises(IdempotencyConflict):
                write_cash(
                    posting_ids[2],
                    cash=5_100_000_000,
                    projection_time=updated_at - timedelta(microseconds=1),
                )
            self.assertEqual(
                write_cash(posting_ids[2], cash=5_100_000_000, projection_time=updated_at),
                2,
            )
            self.assertEqual(
                write_cash(posting_ids[2], cash=5_100_000_000, projection_time=updated_at),
                2,
            )

    def test_reconciliation_source_and_update_chronology_never_regress(self) -> None:
        base = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        source_time = base + timedelta(seconds=10)
        delayed_time = base + timedelta(seconds=5)
        updated_at = base + timedelta(seconds=20)
        with Journal.open(self.db_path) as journal:
            event_ids: list[int] = []
            for suffix, event_time in (
                ("initial", source_time),
                ("delayed", delayed_time),
                ("equal-time", source_time),
            ):
                raw_id, _ = journal.append_raw_message(
                    f"msg-reconciliation-{suffix}", event_time, "RECONCILE CASH"
                )
                event_id, _ = journal.append_execution_event(
                    raw_message_id=raw_id,
                    action_ordinal=0,
                    parsed_action="RECONCILE_CASH",
                    event_time=event_time,
                    reconciliation_state="REQUIRED",
                )
                event_ids.append(event_id)

            def write_reconciliation(
                event_id: int, *, reason: str, projection_time: datetime
            ) -> int:
                with journal.transaction() as transaction:
                    return transaction.write_reconciliation_projection(
                        reconciliation_required=True,
                        reason=reason,
                        last_execution_event_id=event_id,
                        updated_at=projection_time,
                    )

            with self.assertRaises(InvalidJournalValue):
                write_reconciliation(
                    event_ids[0],
                    reason="INITIAL",
                    projection_time=source_time - timedelta(microseconds=1),
                )
            self.assertEqual(
                write_reconciliation(
                    event_ids[0], reason="INITIAL", projection_time=updated_at
                ),
                1,
            )
            with self.assertRaises(IdempotencyConflict):
                write_reconciliation(
                    event_ids[1],
                    reason="DELAYED",
                    projection_time=updated_at + timedelta(seconds=1),
                )
            with self.assertRaises(IdempotencyConflict):
                write_reconciliation(
                    event_ids[2],
                    reason="EQUAL",
                    projection_time=updated_at - timedelta(microseconds=1),
                )
            self.assertEqual(
                write_reconciliation(
                    event_ids[2], reason="EQUAL", projection_time=updated_at
                ),
                2,
            )
            self.assertEqual(
                write_reconciliation(
                    event_ids[2], reason="EQUAL", projection_time=updated_at
                ),
                2,
            )

    def test_actual_cash_projection_requires_an_actual_ledger_posting(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with journal.transaction() as transaction:
                posting_id, _ = transaction.append_ledger_posting(
                    posting_key="canonical-cash",
                    ledger_name="CANONICAL",
                    account_name="CASH",
                    entry_kind="OPENING_BALANCE",
                    occurred_at=now,
                    amount_micros=5_000_000_000,
                )

            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.write_actual_cash_projection(
                        estimated_settled_cash_micros=5_000_000_000,
                        user_confirmed_settled_cash_micros=None,
                        deployed_capital_micros=0,
                        open_planned_risk_micros=0,
                        consecutive_losses=0,
                        weekly_high_water_micros=5_000_000_000,
                        monthly_high_water_micros=5_000_000_000,
                        last_ledger_posting_id=posting_id,
                        updated_at=now,
                    )

    def test_actual_ledger_posting_requires_exactly_one_origin(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.append_ledger_posting(
                        posting_key="actual-orphan",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="OPENING_BALANCE",
                        occurred_at=now,
                        amount_micros=5_000_000_000,
                    )

            with journal.transaction() as transaction:
                canonical_id, duplicate = transaction.append_ledger_posting(
                    posting_key="canonical-opening",
                    ledger_name="CANONICAL",
                    account_name="CASH",
                    entry_kind="OPENING_BALANCE",
                    occurred_at=now,
                    amount_micros=5_000_000_000,
                )
            self.assertGreater(canonical_id, 0)
            self.assertFalse(duplicate)

    def test_actual_ledger_posting_enforces_event_symbol_and_time(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-ledger-event", now, "BOUGHT SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="BOUGHT",
                event_time=now,
                symbol="SPY",
            )
            for posting_key, symbol, occurred_at in (
                ("actual-wrong-symbol", "QQQ", now),
                ("actual-missing-symbol", None, now),
                ("actual-before-event", "SPY", now - timedelta(microseconds=1)),
            ):
                with self.subTest(posting_key=posting_key):
                    with self.assertRaises(InvalidJournalValue):
                        with journal.transaction() as transaction:
                            transaction.append_ledger_posting(
                                posting_key=posting_key,
                                ledger_name="ACTUAL",
                                account_name="CASH",
                                entry_kind="BUY",
                                occurred_at=occurred_at,
                                amount_micros=-100_000_000,
                                execution_event_id=event_id,
                                symbol=symbol,
                            )

            with journal.transaction() as transaction:
                posting_id, duplicate = transaction.append_ledger_posting(
                    posting_key="actual-t-plus-one",
                    ledger_name="ACTUAL",
                    account_name="CASH",
                    entry_kind="SETTLEMENT",
                    occurred_at=now + timedelta(days=1),
                    amount_micros=-100_000_000,
                    execution_event_id=event_id,
                    symbol="SPY",
                )
            self.assertGreater(posting_id, 0)
            self.assertFalse(duplicate)

    def test_skipped_event_cannot_drive_actual_cash_projection(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-skipped-cash", now, "SKIPPED SPY"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
            )

            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    posting_id, _ = transaction.append_ledger_posting(
                        posting_key="actual-from-skipped",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="SALE",
                        occurred_at=now,
                        amount_micros=100_000_000,
                        execution_event_id=event_id,
                        symbol="SPY",
                    )
                    transaction.write_actual_cash_projection(
                        estimated_settled_cash_micros=5_100_000_000,
                        user_confirmed_settled_cash_micros=None,
                        deployed_capital_micros=0,
                        open_planned_risk_micros=0,
                        consecutive_losses=0,
                        weekly_high_water_micros=5_100_000_000,
                        monthly_high_water_micros=5_100_000_000,
                        last_ledger_posting_id=posting_id,
                        updated_at=now,
                    )

            self.assertEqual(journal.count("ledger_postings"), 0)
            self.assertEqual(journal.count("actual_cash_projection"), 0)

    def test_actual_ledger_accepts_task7_cash_mutating_event_actions(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        actions = (
            "BOUGHT",
            "BUY",
            "SOLD",
            "SELL",
            "STOP_FILLED",
            "PARTIAL_FILL",
            "RECONCILE_CASH",
            "RECONCILE_UNRELATED_POSITION",
            "FEE",
        )
        with Journal.open(self.db_path) as journal:
            for ordinal, action in enumerate(actions):
                with self.subTest(action=action):
                    symbol = None if action in {"RECONCILE_CASH", "FEE"} else "SPY"
                    raw_id, _ = journal.append_raw_message(
                        f"msg-actual-{action.lower()}", now, action
                    )
                    event_id, _ = journal.append_execution_event(
                        raw_message_id=raw_id,
                        action_ordinal=0,
                        parsed_action=action,
                        event_time=now,
                        symbol=symbol,
                    )
                    occurred_at = (
                        now + timedelta(days=1)
                        if action in {"SOLD", "SELL", "STOP_FILLED"}
                        else now
                    )
                    with journal.transaction() as transaction:
                        posting_id, duplicate = transaction.append_ledger_posting(
                            posting_key=f"actual-{ordinal}-{action.lower()}",
                            ledger_name="ACTUAL",
                            account_name="CASH",
                            entry_kind=action,
                            occurred_at=occurred_at,
                            amount_micros=0,
                            execution_event_id=event_id,
                            symbol=symbol,
                        )
                    self.assertGreater(posting_id, 0)
                    self.assertFalse(duplicate)

            self.assertEqual(journal.count("ledger_postings"), len(actions))

    def test_actual_ledger_rejects_non_cash_mutating_event_actions(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        actions = (
            "SKIPPED",
            "ACCOUNT_CHECK",
            "STOP_UPDATED",
            "RECONCILE_PENDING_ORDERS",
            "OPTION_PAPER_WINDOW_START",
            "OPTION_PAPER_OPEN",
            "OPTION_PAPER_MARK",
            "OPTION_PAPER_CLOSE",
            "PENDING",
        )
        with Journal.open(self.db_path) as journal:
            for ordinal, action in enumerate(actions):
                with self.subTest(action=action):
                    raw_id, _ = journal.append_raw_message(
                        f"msg-noncash-{ordinal}", now, action
                    )
                    event_id, _ = journal.append_execution_event(
                        raw_message_id=raw_id,
                        action_ordinal=0,
                        parsed_action=action,
                        event_time=now,
                        symbol="SPY",
                    )
                    with self.assertRaises(InvalidJournalValue):
                        with journal.transaction() as transaction:
                            transaction.append_ledger_posting(
                                posting_key=f"actual-noncash-{ordinal}",
                                ledger_name="ACTUAL",
                                account_name="CASH",
                                entry_kind="SALE",
                                occurred_at=now,
                                amount_micros=100_000_000,
                                execution_event_id=event_id,
                                symbol="SPY",
                            )

            self.assertEqual(journal.count("ledger_postings"), 0)

    def test_actual_ledger_posting_enforces_account_check_symbol_and_time(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-ledger-account-check", now, "ACCOUNT CHECK SPY"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="ACCOUNT_CHECK",
                event_time=now,
                symbol="SPY",
                reconciliation_state="CLEAR",
            )
            account_check_id, _ = journal.append_account_check(
                execution_event_id=event_id,
                settled_cash_micros=5_000_000_000,
                pending_order_count=0,
                unlogged_position_count=0,
                confirmed_at=now,
                reconciliation_result="CLEAR",
            )
            for posting_key, symbol, occurred_at in (
                ("actual-check-wrong-symbol", "QQQ", now),
                ("actual-check-missing-symbol", None, now),
                ("actual-check-before", "SPY", now - timedelta(microseconds=1)),
            ):
                with self.subTest(posting_key=posting_key):
                    with self.assertRaises(InvalidJournalValue):
                        with journal.transaction() as transaction:
                            transaction.append_ledger_posting(
                                posting_key=posting_key,
                                ledger_name="ACTUAL",
                                account_name="CASH",
                                entry_kind="ACCOUNT_CHECK",
                                occurred_at=occurred_at,
                                amount_micros=0,
                                account_check_id=account_check_id,
                                symbol=symbol,
                            )
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.append_ledger_posting(
                        posting_key="actual-check-both-origins",
                        ledger_name="ACTUAL",
                        account_name="CASH",
                        entry_kind="ACCOUNT_CHECK",
                        occurred_at=now,
                        amount_micros=0,
                        execution_event_id=event_id,
                        account_check_id=account_check_id,
                        symbol="SPY",
                    )
            with journal.transaction() as transaction:
                posting_id, _ = transaction.append_ledger_posting(
                    posting_key="actual-check-valid",
                    ledger_name="ACTUAL",
                    account_name="CASH",
                    entry_kind="ACCOUNT_CHECK",
                    occurred_at=now,
                    amount_micros=0,
                    account_check_id=account_check_id,
                    symbol="SPY",
                )
            self.assertGreater(posting_id, 0)

    def test_actual_cash_projection_cannot_predate_its_posting(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        posting_time = now + timedelta(seconds=1)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-cash-time", now, "SOLD SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SOLD",
                event_time=now,
                symbol="SPY",
            )
            with journal.transaction() as transaction:
                posting_id, _ = transaction.append_ledger_posting(
                    posting_key="actual-cash-time",
                    ledger_name="ACTUAL",
                    account_name="CASH",
                    entry_kind="SALE",
                    occurred_at=posting_time,
                    amount_micros=100_000_000,
                    execution_event_id=event_id,
                    symbol="SPY",
                )

            projection = {
                "estimated_settled_cash_micros": 5_100_000_000,
                "user_confirmed_settled_cash_micros": None,
                "deployed_capital_micros": 0,
                "open_planned_risk_micros": 0,
                "consecutive_losses": 0,
                "weekly_high_water_micros": 5_100_000_000,
                "monthly_high_water_micros": 5_100_000_000,
                "last_ledger_posting_id": posting_id,
            }
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.write_actual_cash_projection(
                        **projection, updated_at=now
                    )
            with journal.transaction() as transaction:
                revision = transaction.write_actual_cash_projection(
                    **projection, updated_at=posting_time
                )
            self.assertEqual(revision, 1)

    def test_reconciliation_projection_rejects_non_authoritative_events(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message("msg-skip", now, "SKIPPED SPY")
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SKIPPED",
                event_time=now,
                symbol="SPY",
                reconciliation_state="CLEAR",
            )
            with self.assertRaises(InvalidJournalValue):
                with journal.transaction() as transaction:
                    transaction.write_reconciliation_projection(
                        reconciliation_required=False,
                        reason=None,
                        last_execution_event_id=event_id,
                        updated_at=now,
                    )

    def test_clear_account_check_cannot_hide_unreconciled_exposure(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-account-pending", now, "ACCOUNT CHECK pending order"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="ACCOUNT_CHECK",
                event_time=now,
                reconciliation_state="CLEAR",
            )
            with self.assertRaises(InvalidJournalValue):
                journal.append_account_check(
                    execution_event_id=event_id,
                    settled_cash_micros=5_000_000_000,
                    pending_order_count=1,
                    unlogged_position_count=0,
                    confirmed_at=now,
                    reconciliation_result="CLEAR",
                )

    def test_account_checks_are_exact_append_only_integer_records(self) -> None:
        now = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            raw_id, _ = journal.append_raw_message(
                "msg-account", now, "ACCOUNT CHECK settled_cash 5000"
            )
            event_id, _ = journal.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="ACCOUNT_CHECK",
                event_time=now,
                reconciliation_state="CLEAR",
            )
            values = {
                "execution_event_id": event_id,
                "settled_cash_micros": 5_000_000_000,
                "pending_order_count": 0,
                "unlogged_position_count": 0,
                "confirmed_at": now,
                "reconciliation_result": "CLEAR",
            }
            with self.assertRaises(InvalidJournalValue):
                journal.append_account_check(
                    **{**values, "confirmed_at": now + timedelta(seconds=1)}
                )
            first_id, first_duplicate = journal.append_account_check(**values)
            replay_id, replay_duplicate = journal.append_account_check(**values)

            self.assertEqual(first_id, replay_id)
            self.assertFalse(first_duplicate)
            self.assertTrue(replay_duplicate)
            with self.assertRaises(IdempotencyConflict):
                journal.append_account_check(
                    **{**values, "pending_order_count": 1}
                )
            with self.assertRaises(InvalidJournalValue):
                journal.append_account_check(
                    **{**values, "settled_cash_micros": True}
                )

    def test_table_helpers_reject_unlisted_identifiers(self) -> None:
        with Journal.open(self.db_path) as journal:
            with self.assertRaises(InvalidJournalValue):
                journal.count('raw_messages"; DELETE FROM raw_messages; --')
            with self.assertRaises(InvalidJournalValue):
                journal.table_info("sqlite_schema")


if __name__ == "__main__":
    unittest.main()
