from __future__ import annotations

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

from stock_monitor.journal import (
    IdempotencyConflict,
    InvalidJournalValue,
    Journal,
    JournalBusy,
    JournalError,
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

    def test_report_claims_serialize_and_require_explicit_time_for_recovery(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as first, Journal.open(self.db_path) as second:
            acquired = first.claim_report(
                session_date, "CLOSE", now=now, lease_seconds=300
            )
            in_progress = second.claim_report(
                session_date,
                "close",
                now=now + timedelta(seconds=299),
                lease_seconds=300,
            )
            no_clock_takeover = second.claim_report(session_date, "CLOSE")
            recovered = second.claim_report(
                session_date,
                "CLOSE",
                now=now + timedelta(seconds=300),
                lease_seconds=300,
            )

            self.assertEqual(acquired.status, "ACQUIRED")
            self.assertIsNotNone(acquired.claim_token)
            self.assertEqual(in_progress.status, "IN_PROGRESS")
            self.assertIsNone(in_progress.claim_token)
            self.assertEqual(no_clock_takeover.status, "IN_PROGRESS")
            self.assertEqual(recovered.status, "RECOVERED_EXPIRED")
            self.assertNotEqual(acquired.claim_token, recovered.claim_token)
            self.assertEqual(first.count("report_claims"), 1)

            with self.assertRaises(InvalidJournalValue):
                first.claim_report(session_date, "CLOSE ", now=now)

    def test_two_connections_racing_for_a_report_claim_get_one_owner(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path):
            pass
        barrier = Barrier(2)

        def claim() -> str:
            with Journal.open(self.db_path) as journal:
                barrier.wait()
                return journal.claim_report(
                    session_date, "CLOSE", now=now, lease_seconds=300
                ).status

        with ThreadPoolExecutor(max_workers=2) as executor:
            statuses = tuple(executor.map(lambda _: claim(), range(2)))

        self.assertEqual(sorted(statuses), ["ACQUIRED", "IN_PROGRESS"])

    def test_report_finalization_requires_the_current_token_and_active_lease(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        with Journal.open(self.db_path) as journal:
            claim = journal.claim_report(
                session_date, "CLOSE", now=now, lease_seconds=300
            )
            assert claim.claim_token is not None
            values = {
                "claim_id": claim.claim_id,
                "claim_token": claim.claim_token,
                "body": "# Close\n",
                "state_sha256": "a" * 64,
                "observation_ids": (),
                "archive_relative_path": "reports/2026/08/14/close.md",
                "outbox_destination": "CODEX_TASK",
                "outbox_payload": "close",
            }

            with self.assertRaises(IdempotencyConflict):
                journal.finalize_report(
                    **values, created_at=now - timedelta(microseconds=1)
                )

            recovered = journal.claim_report(
                session_date,
                "CLOSE",
                now=now + timedelta(seconds=300),
                lease_seconds=300,
            )
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
        with Journal.open(self.db_path) as journal:
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
            claim = journal.claim_report(
                session_date, "CLOSE", now=now, lease_seconds=300
            )
            assert claim.claim_token is not None
            values = {
                "claim_id": claim.claim_id,
                "claim_token": claim.claim_token,
                "body": "# Close report\n\nNO TRADE\n",
                "state_sha256": "a" * 64,
                "observation_ids": (second_observation, first_observation),
                "archive_relative_path": (
                    "reports/2026/08/14/close-2026-08-14-audit.md"
                ),
                "created_at": now + timedelta(seconds=1),
                "outbox_destination": "CODEX_TASK",
                "outbox_payload": "CLOSE: NO TRADE",
            }

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

    def test_scheduled_completion_report_must_match_the_run_identity_and_path(self) -> None:
        session_date = date(2026, 8, 14)
        now = datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc)
        archive_path = "reports/2026/08/14/close.md"
        with Journal.open(self.db_path) as journal:
            claim = journal.claim_report(session_date, "CLOSE", now=now)
            assert claim.claim_token is not None
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="# Close\n",
                state_sha256="a" * 64,
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
                parsed_action="RECONCILED",
                event_time=now + timedelta(seconds=1),
                symbol="SPY",
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
                symbol="SPY",
                shares=1,
                price_micros=101_000_000,
            )
            with journal.transaction() as transaction:
                transaction.write_actual_position(
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
