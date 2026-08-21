from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

import stock_monitor.confirmations as confirmation_module
import stock_monitor.journal as journal_module
import stock_monitor.ledger as ledger_module
import stock_monitor.reconciliation as reconciliation_module
import stock_monitor.risk as risk_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.domain import stable_execution_event_identity
from stock_monitor.journal import (
    Journal,
    MigrationCorruption,
    is_verified_journal_replay_source,
)
from stock_monitor.ledger import LedgerSignal
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.policy import ConfigurationError
from stock_monitor.reconciliation import (
    ResolvedSignalPlan,
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    is_verified_actual_ledger_state,
    replay_actual,
)
from stock_monitor.risk import SessionCalendarResolver
from tests.support import policy_fixture


ROOT = Path(__file__).resolve().parents[2]
SESSION = date(2026, 8, 14)


def envelope(
    message_id: str,
    text: str = "SKIPPED SPY",
    *,
    message_time: datetime | None = None,
    received_at: datetime | None = None,
) -> ConfirmationEnvelope:
    message_time = message_time or datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
    received_at = received_at or message_time + timedelta(seconds=1)
    return ConfirmationEnvelope(
        message_id=message_id,
        message_time=message_time,
        received_at=received_at,
        text=text,
        session_date=SESSION,
    )


class StructuralPlanResolver:
    def __init__(self) -> None:
        self.signal = LedgerSignal(
            signal_id="sig-review",
            symbol="SPY",
            role="PRIMARY",
            publication_session=SESSION,
            maximum_entry=Decimal("100"),
            recommended_stop=Decimal("97.50"),
            target=Decimal("105"),
            planned_shares=1,
            tick_size=Decimal("0.01"),
            trigger_price=Decimal("99.99"),
        )

    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> ResolvedSignalPlan | None:
        del query_cutoff
        if symbol != "SPY" or economic_at.date() != SESSION:
            return None
        return ResolvedSignalPlan(
            signal=self.signal,
            report_id="report:review",
            publication_rank=1,
            publication_source_digest="a" * 64,
        )


class ExplodingPlanResolver:
    def resolve(
        self,
        *,
        symbol: str,
        economic_at: datetime,
        query_cutoff: datetime,
    ) -> ResolvedSignalPlan | None:
        del symbol, economic_at, query_cutoff
        raise AssertionError("authoritative replay consulted ephemeral plan context")


class Task7FinalReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "journal.sqlite3"
        self.journal = Journal.open(self.path)
        self.addCleanup(self.journal.close)
        self.calendar = SessionCalendarResolver(
            (load_current_market_calendar(ROOT, as_of=SESSION),)
        )
        self.policy = policy_fixture()
        self.plans = UnavailableSignalPlanResolver()
        self.entry_authorities = UnavailableActualEntryAuthorityResolver()

    def ingest(
        self,
        item: ConfirmationEnvelope,
        *,
        journal: Journal | None = None,
        plans: object | None = None,
        calendar: SessionCalendarResolver | None = None,
        policy: object | None = None,
    ):
        return ingest_confirmation(
            journal or self.journal,
            item,
            plans=self.plans if plans is None else plans,
            calendar=self.calendar if calendar is None else calendar,
            policy=self.policy if policy is None else policy,
            entry_authorities=self.entry_authorities,
        )

    def append_skipped_action(
        self,
        transaction,
        *,
        raw_row_id: int,
        message_id: str,
        ordinal: int,
        symbol: str,
        message_time: datetime,
        received_at: datetime,
        destination: str = "CODEX_TASK",
    ) -> int:
        event_id, _ = stable_execution_event_identity(message_id, ordinal)
        acknowledgement_key = journal_module._confirmation_outbox_key(
            event_id,
            destination,
        )
        details = {
            "acknowledgement": {
                "destination": destination,
                "idempotency_key": acknowledgement_key,
            },
            "domain_kind": "SKIPPED",
            "event_role": "OBSERVATION",
            "event_time_basis": "MESSAGE_TIME_OBSERVATION",
            "missing_fields": [],
            "normalized": {
                "amount_decimal": None,
                "asset_id": None,
                "delta": None,
                "fill_group_planned_shares": None,
                "occ_symbol": None,
                "open_interest": None,
                "parent_order_id": None,
                "pending_orders": None,
                "reason_sha256": None,
                "signed_shares": None,
                "unlogged_positions": None,
                "volume": None,
            },
            "reason_codes": [],
            "source": {
                "received_at": received_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "type": "ROBINHOOD_MANUAL_CONFIRMATION",
            },
            "version": 1,
        }
        event_row_id, _ = transaction.append_execution_event(
            raw_message_id=raw_row_id,
            action_ordinal=ordinal,
            parsed_action="SKIPPED",
            event_time=message_time,
            symbol=symbol,
            compliance_result="COMPLIANT",
            reconciliation_state="CLEAR",
            details=details,
        )
        payload = json.dumps(
            {
                "event_id": event_id,
                "kind": "SKIPPED",
                "ordinal": ordinal,
                "reason_codes": [],
                "status": "COMPLIANT",
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
            created_at=received_at,
        )
        return event_row_id

    def append_account_check_without_account_row(
        self,
        transaction,
        *,
        raw_row_id: int,
        message_id: str,
        ordinal: int,
        message_time: datetime,
        event_time: datetime,
        received_at: datetime,
        destination: str = "CODEX_TASK",
    ) -> int:
        event_id, _ = stable_execution_event_identity(message_id, ordinal)
        acknowledgement_key = journal_module._confirmation_outbox_key(
            event_id,
            destination,
        )
        event_row_id, _ = transaction.append_execution_event(
            raw_message_id=raw_row_id,
            action_ordinal=ordinal,
            parsed_action="ACCOUNT_CHECK",
            event_time=event_time,
            compliance_result="COMPLIANT",
            reconciliation_state="CLEAR",
            details={
                "acknowledgement": {
                    "destination": destination,
                    "idempotency_key": acknowledgement_key,
                },
                "domain_kind": "ACCOUNT_CHECK",
                "event_role": "OBSERVATION",
                "event_time_basis": "EXPLICIT",
                "missing_fields": [],
                "normalized": {
                    "amount_decimal": None,
                    "asset_id": None,
                    "delta": None,
                    "fill_group_planned_shares": None,
                    "occ_symbol": None,
                    "open_interest": None,
                    "parent_order_id": None,
                    "pending_orders": 0,
                    "reason_sha256": None,
                    "signed_shares": None,
                    "unlogged_positions": 0,
                    "volume": None,
                },
                "reason_codes": [],
                "source": {
                    "received_at": received_at.isoformat(
                        timespec="microseconds"
                    ).replace("+00:00", "Z"),
                    "type": "ROBINHOOD_MANUAL_CONFIRMATION",
                },
                "version": 1,
            },
        )
        payload = json.dumps(
            {
                "event_id": event_id,
                "kind": "ACCOUNT_CHECK",
                "ordinal": ordinal,
                "reason_codes": [],
                "status": "COMPLIANT",
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
            created_at=received_at,
        )
        return event_row_id

    def assert_source_paths_reject(
        self,
        *,
        event_row_id: int,
        cutoff: datetime,
        error: str,
        include_cold_ingest: bool = False,
    ) -> None:
        def read_single() -> object:
            with self.journal.transaction() as transaction:
                return transaction.read_action_source(
                    execution_event_id=event_row_id
                )

        def read_bulk() -> object:
            with self.journal.transaction() as transaction:
                return transaction.read_actual_replay(query_cutoff=cutoff)

        for label, operation in (
            ("single", read_single),
            ("bulk", read_bulk),
        ):
            with self.subTest(path=label):
                with self.assertRaisesRegex(MigrationCorruption, error):
                    operation()

        self.journal.close()
        self.journal = Journal.open(self.path)
        for label, operation in (
            ("restart-single", read_single),
            ("restart-bulk", read_bulk),
        ):
            with self.subTest(path=label):
                with self.assertRaisesRegex(MigrationCorruption, error):
                    operation()
        if include_cold_ingest:
            with self.subTest(path="cold-next-ingest"):
                with self.assertRaisesRegex(MigrationCorruption, error):
                    self.ingest(
                        envelope(
                            "message:cohort-cold-next",
                            message_time=cutoff + timedelta(minutes=1),
                            received_at=cutoff + timedelta(
                                minutes=1,
                                seconds=1,
                            ),
                        )
                    )

    def test_authoritative_replay_is_independent_of_ephemeral_plan_resolver(
        self,
    ) -> None:
        self.ingest(
            envelope(
                "message:resolver-independent",
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                message_time=datetime(2026, 8, 14, 14, 15, tzinfo=UTC),
                received_at=datetime(2026, 8, 14, 14, 15, 1, tzinfo=UTC),
            )
        )
        cutoff = datetime(2026, 8, 14, 14, 16, tzinfo=UTC)
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(query_cutoff=cutoff)

        unavailable = replay_actual(
            source,
            plans=UnavailableSignalPlanResolver(),
            calendar=self.calendar,
            policy=self.policy,
        )
        structural = replay_actual(
            source,
            plans=StructuralPlanResolver(),
            calendar=self.calendar,
            policy=self.policy,
        )
        with patch(
            "stock_monitor.ledger.is_issued_ledger_signal",
            return_value=True,
        ):
            issued = replay_actual(
                source,
                plans=StructuralPlanResolver(),
                calendar=self.calendar,
                policy=self.policy,
            )
        resolver_not_consulted = replay_actual(
            source,
            plans=ExplodingPlanResolver(),
            calendar=self.calendar,
            policy=self.policy,
        )

        states = (unavailable, structural, issued, resolver_not_consulted)
        self.assertEqual(
            {state.reconciliation_reasons for state in states},
            {unavailable.reconciliation_reasons},
        )
        self.assertEqual(
            {state.positions[0].reason_codes for state in states},
            {unavailable.positions[0].reason_codes},
        )
        self.assertEqual(
            {state.source_digest for state in states},
            {unavailable.source_digest},
        )
        self.assertIn(
            "SIGNAL_PLAN_UNAVAILABLE",
            unavailable.positions[0].reason_codes,
        )
        self.assertIn(
            "AUTHORITY_CONTEXT_UNVERIFIED",
            unavailable.positions[0].reason_codes,
        )

        settlements = tuple(
            risk_module._issue_settlement_replay(source, state, self.calendar)
            for state in states
        )
        cohorts = tuple(
            ledger_module._issue_actual_projection_from_journal(source, state)
            for state in states
        )
        self.assertEqual(
            {item.replay_authority.actual_state_digest for item in settlements},
            {unavailable.source_digest},
        )
        self.assertEqual(
            {item.actual_state_digest for item in cohorts},
            {unavailable.source_digest},
        )

    def test_ingestion_retains_plan_diagnostics_but_checkpoints_actual_only(
        self,
    ) -> None:
        item = envelope(
            "message:diagnostic-plan-only",
            (
                "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; "
                "BID 99.99 ASK 100; STOP SET @ 97.50"
            ),
            message_time=datetime(2026, 8, 14, 14, 15, tzinfo=UTC),
            received_at=datetime(2026, 8, 14, 14, 15, 1, tzinfo=UTC),
        )
        with patch(
            "stock_monitor.ledger.is_issued_ledger_signal",
            return_value=True,
        ):
            result = self.ingest(item, plans=StructuralPlanResolver())

        self.assertNotIn(
            "SIGNAL_PLAN_UNAVAILABLE",
            result.actions[0].reason_codes,
        )
        self.assertIn(
            "AUTHORITY_CONTEXT_UNVERIFIED",
            result.actions[0].reason_codes,
        )
        with self.journal.transaction() as transaction:
            source = transaction.read_actual_replay(
                query_cutoff=item.received_at,
            )
        state = replay_actual(
            source,
            plans=ExplodingPlanResolver(),
            calendar=self.calendar,
            policy=self.policy,
        )

        self.assertIn(
            "SIGNAL_PLAN_UNAVAILABLE",
            state.positions[0].reason_codes,
        )
        self.assertTrue(state.cache_matches_replay)

    def test_one_maximum_envelope_validates_source_chronology_once(self) -> None:
        original = Journal._validate_confirmation_source_order
        calls: list[tuple[datetime, datetime]] = []
        item = envelope(
            "message:one-envelope-validation",
            "\n".join("SKIPPED SPY" for _ in range(64)),
        )

        def counted(
            journal: Journal,
            *,
            message_time: datetime,
            received_at: datetime,
        ) -> None:
            calls.append((message_time, received_at))
            original(
                journal,
                message_time=message_time,
                received_at=received_at,
            )

        with patch.object(Journal, "_validate_confirmation_source_order", counted):
            result = self.ingest(item)

        self.assertEqual(len(result.actions), 64)
        self.assertEqual(calls, [(item.message_time, item.received_at)])

    def test_maximum_envelope_typed_readback_authenticates_one_cohort(
        self,
    ) -> None:
        item = envelope(
            "message:maximum-envelope-typed-readback",
            "\n".join("SKIPPED SPY" for _ in range(64)),
        )
        result = self.ingest(item)
        single_action_reads: list[int] = []
        semantic_builds: list[int] = []
        raw_parses: list[str] = []
        sql_statements: list[str] = []
        original_single_read = Journal._read_action_source
        original_build = Journal._action_source_from_rows
        original_parse = confirmation_module.parse_confirmation_batch_or_pending
        original_sql = journal_module._sql

        def counted_single_read(
            journal: Journal,
            *,
            execution_event_id: int,
            validate_chronology: bool = True,
        ):
            single_action_reads.append(execution_event_id)
            return original_single_read(
                journal,
                execution_event_id=execution_event_id,
                validate_chronology=validate_chronology,
            )

        def counted_build(journal: Journal, **kwargs):
            semantic_builds.append(int(kwargs["event_row"][0]))
            return original_build(journal, **kwargs)

        def counted_parse(text: str, *, session_date: date):
            raw_parses.append(text)
            return original_parse(text, session_date=session_date)

        def counted_sql(connection, statement, parameters=()):
            if connection is self.journal._connection:
                sql_statements.append(statement)
            return original_sql(connection, statement, parameters)

        with (
            patch.object(Journal, "_read_action_source", counted_single_read),
            patch.object(Journal, "_action_source_from_rows", counted_build),
            patch.object(
                confirmation_module,
                "parse_confirmation_batch_or_pending",
                counted_parse,
            ),
            patch.object(journal_module, "_sql", counted_sql),
        ):
            with self.journal.transaction() as transaction:
                stored = transaction.read_confirmation_result(
                    message_id=item.message_id
                )

        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.raw_row_id, result.raw_row_id)
        self.assertEqual(stored.message_id, result.message_id)
        self.assertEqual(stored.state_digest, result.state_digest)
        self.assertEqual(
            tuple(
                (
                    action.ordinal,
                    action.event_row_id,
                    action.event_id,
                    action.domain_kind,
                    action.status,
                    action.reason_codes,
                    action.outbox_id,
                )
                for action in stored.actions
            ),
            tuple(
                (
                    action.ordinal,
                    action.event_row_id,
                    action.event_id,
                    action.kind.value,
                    action.status.value,
                    action.reason_codes,
                    action.outbox_id,
                )
                for action in result.actions
            ),
        )
        self.assertEqual(
            (
                len(single_action_reads),
                len(semantic_builds),
                len(raw_parses),
            ),
            (0, 64, 1),
            f"cohort_sql_count={len(sql_statements)}",
        )
        self.assertEqual(raw_parses, [item.text])
        self.assertLessEqual(len(sql_statements), 10)

    def test_incremental_ingestion_avoids_historical_action_reconstruction(
        self,
    ) -> None:
        base = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
        last_result = None
        for batch in range(16):
            message_time = base + timedelta(minutes=batch)
            last_result = self.ingest(
                envelope(
                    f"message:checkpoint-seed:{batch}",
                    "\n".join("SKIPPED SPY" for _ in range(64)),
                    message_time=message_time,
                    received_at=message_time + timedelta(seconds=1),
                )
            )
        assert last_result is not None
        old_highwater = last_result.actions[-1].event_row_id
        self.assertGreaterEqual(old_highwater, 1024)

        historical_calls: list[int] = []
        original = Journal._read_action_source

        def counted(
            journal: Journal,
            *,
            execution_event_id: int,
            validate_chronology: bool = True,
        ):
            if execution_event_id <= old_highwater:
                historical_calls.append(execution_event_id)
            return original(
                journal,
                execution_event_id=execution_event_id,
                validate_chronology=validate_chronology,
            )

        started = perf_counter()
        with patch.object(Journal, "_read_action_source", counted):
            self.ingest(
                envelope(
                    "message:checkpoint-next",
                    message_time=base + timedelta(minutes=16),
                    received_at=base + timedelta(minutes=16, seconds=1),
                )
            )
        elapsed = perf_counter() - started

        self.assertEqual(historical_calls, [])
        self.assertLess(elapsed, 3.0)

    def test_cold_restart_bulk_authenticates_large_replay_inside_ingestion(
        self,
    ) -> None:
        base = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
        for batch in range(16):
            message_time = base + timedelta(minutes=batch)
            self.ingest(
                envelope(
                    f"message:cold-restart-seed:{batch}",
                    "\n".join("SKIPPED SPY" for _ in range(64)),
                    message_time=message_time,
                    received_at=message_time + timedelta(seconds=1),
                )
            )
        old_highwater = self.journal.count("execution_events")
        self.assertGreaterEqual(old_highwater, 1024)

        next_item = envelope(
            "message:cold-restart-next",
            message_time=base + timedelta(minutes=16),
            received_at=base + timedelta(minutes=16, seconds=1),
        )
        with self.journal.transaction() as transaction:
            warm_source = transaction.read_actual_replay(
                query_cutoff=next_item.received_at,
            )
        warm_state = replay_actual(
            warm_source,
            plans=self.plans,
            calendar=self.calendar,
            policy=self.policy,
        )
        self.assertTrue(
            reconciliation_module.is_verified_actual_ledger_state_for_source(
                warm_state,
                warm_source,
            )
        )

        self.journal.close()
        self.journal = Journal.open(self.path)
        historical_calls: list[int] = []
        sql_statements: list[str] = []
        original_action_read = Journal._read_action_source

        def counted_action_read(
            journal: Journal,
            *,
            execution_event_id: int,
            validate_chronology: bool = True,
        ):
            if execution_event_id <= old_highwater:
                historical_calls.append(execution_event_id)
            return original_action_read(
                journal,
                execution_event_id=execution_event_id,
                validate_chronology=validate_chronology,
            )

        started = perf_counter()
        self.journal._connection.set_trace_callback(sql_statements.append)
        try:
            with patch.object(
                Journal,
                "_read_action_source",
                counted_action_read,
            ):
                result = self.ingest(next_item)
        finally:
            self.journal._connection.set_trace_callback(None)
        elapsed = perf_counter() - started

        self.assertEqual(len(result.actions), 1)
        self.assertEqual(
            self.journal.count("execution_events"),
            old_highwater + 1,
        )
        self.assertFalse(
            historical_calls,
            f"historical_calls={len(historical_calls)}; "
            f"sql_count={len(sql_statements)}; elapsed={elapsed:.6f}",
        )
        self.assertLessEqual(len(sql_statements), 80)
        self.assertLess(elapsed, 3.0)

    def test_large_replay_respects_sqlite_minimum_variable_limit(self) -> None:
        base = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
        for batch in range(16):
            message_time = base + timedelta(minutes=batch)
            self.ingest(
                envelope(
                    f"message:sqlite-variable-limit:{batch}",
                    "\n".join("SKIPPED SPY" for _ in range(64)),
                    message_time=message_time,
                    received_at=message_time + timedelta(seconds=1),
                )
            )

        previous_limit = self.journal._connection.setlimit(
            sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
            999,
        )
        try:
            with self.journal.transaction() as transaction:
                source = transaction.read_actual_replay(
                    query_cutoff=base + timedelta(minutes=16),
                )
        finally:
            self.journal._connection.setlimit(
                sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                previous_limit,
            )

        self.assertEqual(source.expected_action_count, 1024)
        self.assertEqual(len(source.actions), 1024)

    def test_split_receipt_batch_is_rejected_as_one_atomic_cohort(self) -> None:
        message_id = "message:split-receipt"
        message_time = datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
        first_receipt = message_time + timedelta(seconds=1)
        second_receipt = message_time + timedelta(seconds=2)
        with self.journal.transaction() as transaction:
            raw_row_id, _ = transaction.append_raw_message(
                message_id,
                message_time,
                "SKIPPED SPY\nSKIPPED QQQ",
            )
            event_row_id = self.append_skipped_action(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=0,
                symbol="SPY",
                message_time=message_time,
                received_at=first_receipt,
            )
            self.append_skipped_action(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=1,
                symbol="QQQ",
                message_time=message_time,
                received_at=second_receipt,
            )

        for label, cutoff in (
            ("between-receipts", first_receipt + timedelta(microseconds=1)),
            ("after-batch", second_receipt + timedelta(seconds=1)),
        ):
            with self.subTest(cutoff=label):
                self.assert_source_paths_reject(
                    event_row_id=event_row_id,
                    cutoff=cutoff,
                    error="CONFIRMATION_RECEIPT_COHORT_MISMATCH",
                )

    def test_valid_batch_crosses_replay_cutoff_as_one_atomic_cohort(self) -> None:
        item = envelope(
            "message:atomic-cutoff",
            "SKIPPED SPY\nSKIPPED QQQ",
        )
        result = self.ingest(item)

        def replay(cutoff: datetime):
            with self.journal.transaction() as transaction:
                return transaction.read_actual_replay(query_cutoff=cutoff)

        before = replay(item.message_time)
        at_receipt = replay(item.received_at)
        self.assertEqual(before.actions, ())
        self.assertEqual(
            tuple(action.execution_event_id for action in at_receipt.actions),
            tuple(action.event_row_id for action in result.actions),
        )
        self.assertEqual(
            {action.received_at for action in at_receipt.actions},
            {item.received_at},
        )

        self.journal.close()
        self.journal = Journal.open(self.path)
        restarted_before = replay(item.message_time)
        restarted_at_receipt = replay(item.received_at)
        self.assertEqual(restarted_before.actions, ())
        self.assertEqual(restarted_at_receipt, at_receipt)

    def test_reversed_action_ordinals_are_rejected_in_cursor_order(self) -> None:
        message_id = "message:reversed-ordinals"
        message_time = datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
        received_at = message_time + timedelta(seconds=1)
        with self.journal.transaction() as transaction:
            raw_row_id, _ = transaction.append_raw_message(
                message_id,
                message_time,
                "SKIPPED SPY\nSKIPPED QQQ",
            )
            self.append_skipped_action(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=1,
                symbol="QQQ",
                message_time=message_time,
                received_at=received_at,
            )
            event_row_id = self.append_skipped_action(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=0,
                symbol="SPY",
                message_time=message_time,
                received_at=received_at,
            )

        self.assert_source_paths_reject(
            event_row_id=event_row_id,
            cutoff=received_at + timedelta(seconds=1),
            error="CONFIRMATION_ACTION_CURSOR_ORDER_INVALID",
        )

    def test_interleaved_raw_batches_are_rejected_as_cursor_groups(self) -> None:
        message_time = datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
        received_at = message_time + timedelta(seconds=1)
        with self.journal.transaction() as transaction:
            raw_a, _ = transaction.append_raw_message(
                "message:interleaved-a",
                message_time,
                "SKIPPED SPY\nSKIPPED QQQ",
            )
            raw_b, _ = transaction.append_raw_message(
                "message:interleaved-b",
                message_time,
                "SKIPPED DIA",
            )
            event_row_id = self.append_skipped_action(
                transaction,
                raw_row_id=raw_a,
                message_id="message:interleaved-a",
                ordinal=0,
                symbol="SPY",
                message_time=message_time,
                received_at=received_at,
            )
            nested_event_row_id = self.append_skipped_action(
                transaction,
                raw_row_id=raw_b,
                message_id="message:interleaved-b",
                ordinal=0,
                symbol="DIA",
                message_time=message_time,
                received_at=received_at,
            )
            self.append_skipped_action(
                transaction,
                raw_row_id=raw_a,
                message_id="message:interleaved-a",
                ordinal=1,
                symbol="QQQ",
                message_time=message_time,
                received_at=received_at,
            )

        def read_nested_single() -> object:
            with self.journal.transaction() as transaction:
                return transaction.read_action_source(
                    execution_event_id=nested_event_row_id
                )

        with self.subTest(path="nested-single"):
            with self.assertRaisesRegex(
                MigrationCorruption,
                "CONFIRMATION_ACTION_GROUP_INTERLEAVED",
            ):
                read_nested_single()
        self.assert_source_paths_reject(
            event_row_id=event_row_id,
            cutoff=received_at + timedelta(seconds=1),
            error="CONFIRMATION_ACTION_GROUP_INTERLEAVED",
        )
        with self.subTest(path="nested-restart-single"):
            with self.assertRaisesRegex(
                MigrationCorruption,
                "CONFIRMATION_ACTION_GROUP_INTERLEAVED",
            ):
                read_nested_single()

    def test_single_source_authenticates_every_sibling_account_row(self) -> None:
        message_id = "message:missing-sibling-account-row"
        message_time = datetime(2026, 8, 14, 14, 21, tzinfo=UTC)
        account_time = datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
        received_at = message_time + timedelta(seconds=1)
        with self.journal.transaction() as transaction:
            raw_row_id, _ = transaction.append_raw_message(
                message_id,
                message_time,
                "SKIPPED SPY\nACCOUNT CHECK settled_cash 5000 "
                "pending_orders 0 unlogged_positions 0 AT 10:20 ET",
            )
            skipped_event_row_id = self.append_skipped_action(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=0,
                symbol="SPY",
                message_time=message_time,
                received_at=received_at,
            )
            self.append_account_check_without_account_row(
                transaction,
                raw_row_id=raw_row_id,
                message_id=message_id,
                ordinal=1,
                message_time=message_time,
                event_time=account_time,
                received_at=received_at,
            )

        def read_skipped_single() -> object:
            with self.journal.transaction() as transaction:
                return transaction.read_action_source(
                    execution_event_id=skipped_event_row_id
                )

        def read_bulk() -> object:
            with self.journal.transaction() as transaction:
                return transaction.read_actual_replay(
                    query_cutoff=received_at + timedelta(seconds=1)
                )

        with self.subTest(path="single"):
            with self.assertRaisesRegex(
                MigrationCorruption,
                "account-check source row is missing",
            ):
                read_skipped_single()
        with self.subTest(path="bulk-control"):
            with self.assertRaisesRegex(
                MigrationCorruption,
                "account-check source row is missing",
            ):
                read_bulk()

        self.journal.close()
        self.journal = Journal.open(self.path)
        with self.subTest(path="restart-single"):
            with self.assertRaisesRegex(
                MigrationCorruption,
                "account-check source row is missing",
            ):
                read_skipped_single()

    def test_extra_execution_outbox_rejects_source_and_cold_ingest(self) -> None:
        item = envelope("message:extra-execution-outbox")
        result = self.ingest(item)
        action = result.actions[0]
        event_id, _ = stable_execution_event_identity(item.message_id, 0)
        extra_destination = "SECONDARY_TASK"
        extra_key = journal_module._confirmation_outbox_key(
            event_id,
            extra_destination,
        )
        payload = json.dumps(
            {
                "event_id": event_id,
                "kind": "SKIPPED",
                "ordinal": 0,
                "reason_codes": [],
                "status": "COMPLIANT",
                "version": 1,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self.journal.transaction() as transaction:
            transaction.append_outbox(
                idempotency_key=extra_key,
                origin_report_id=None,
                origin_execution_event_id=action.event_row_id,
                destination=extra_destination,
                payload_text=payload,
                created_at=item.received_at,
            )

        self.assert_source_paths_reject(
            event_row_id=action.event_row_id,
            cutoff=item.received_at + timedelta(seconds=1),
            error="CONFIRMATION_ACKNOWLEDGEMENT_COUNT_INVALID",
            include_cold_ingest=True,
        )

    def test_incremental_checkpoint_invalidation_and_rollback_matrix(self) -> None:
        def scenario(
            name: str,
            mutate,
            *,
            expect_full_replay: bool,
            policy=None,
            calendar=None,
            restart: bool = False,
        ) -> None:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / f"{name}.sqlite3"
                journal = Journal.open(path)
                try:
                    first = self.ingest(
                        envelope(f"message:{name}:seed"),
                        journal=journal,
                    )
                    self.assertGreater(first.actions[-1].event_row_id, 0)
                    mutate(journal, path)
                    if restart:
                        journal.close()
                        journal = Journal.open(path)
                    calls: list[datetime] = []
                    original = Journal._read_actual_replay

                    def counted(
                        owner: Journal,
                        *,
                        query_cutoff: datetime,
                        through_execution_cursor: int | None,
                    ):
                        calls.append(query_cutoff)
                        return original(
                            owner,
                            query_cutoff=query_cutoff,
                            through_execution_cursor=through_execution_cursor,
                        )

                    with patch.object(Journal, "_read_actual_replay", counted):
                        self.ingest(
                            envelope(
                                f"message:{name}:next",
                                message_time=datetime(
                                    2026, 8, 14, 14, 21, tzinfo=UTC
                                ),
                                received_at=datetime(
                                    2026, 8, 14, 14, 21, 1, tzinfo=UTC
                                ),
                            ),
                            journal=journal,
                            policy=policy,
                            calendar=calendar,
                        )
                    self.assertEqual(bool(calls), expect_full_replay, name)
                finally:
                    journal.close()

        def no_change(_journal: Journal, _path: Path) -> None:
            return None

        def rollback(journal: Journal, _path: Path) -> None:
            with self.assertRaisesRegex(RuntimeError, "rollback"):
                with journal.transaction() as transaction:
                    transaction.append_raw_message(
                        "message:checkpoint-rollback:orphan",
                        datetime(2026, 8, 14, 14, 20, 30, tzinfo=UTC),
                        "SKIPPED QQQ",
                    )
                    raise RuntimeError("rollback")

        def direct_commit(journal: Journal, _path: Path) -> None:
            journal.claim_report(SESSION, "PREMARKET")

        def cross_connection(journal: Journal, path: Path) -> None:
            other = Journal.open(path)
            try:
                other.claim_report(SESSION, "PREMARKET")
            finally:
                other.close()

        scenarios = (
            ("unchanged", no_change, False, None, None, False),
            ("rollback", rollback, True, None, None, False),
            ("direct", direct_commit, True, None, None, False),
            ("cross", cross_connection, True, None, None, False),
            ("restart", no_change, True, None, None, True),
            (
                "calendar",
                no_change,
                True,
                None,
                SessionCalendarResolver.for_diagnostics(self.calendar.calendars),
                False,
            ),
        )
        for name, mutate, expected, policy, calendar, restart in scenarios:
            with self.subTest(name=name):
                scenario(
                    name,
                    mutate,
                    expect_full_replay=expected,
                    policy=policy,
                    calendar=calendar,
                    restart=restart,
                )

        with self.subTest(name="policy"):
            with tempfile.TemporaryDirectory() as directory:
                journal = Journal.open(Path(directory) / "policy.sqlite3")
                try:
                    self.ingest(
                        envelope("message:policy:seed"),
                        journal=journal,
                    )
                    total_changes = (
                        journal_module._journal_source_authority_total_changes(
                            journal
                        )
                    )
                    with self.assertRaisesRegex(
                        ConfigurationError,
                        "policy field max_positions is fixed",
                    ):
                        self.ingest(
                            envelope(
                                "message:policy:next",
                                message_time=datetime(
                                    2026, 8, 14, 14, 21, tzinfo=UTC
                                ),
                                received_at=datetime(
                                    2026, 8, 14, 14, 21, 1, tzinfo=UTC
                                ),
                            ),
                            journal=journal,
                            policy=policy_fixture(max_positions=3),
                        )
                    self.assertEqual(
                        journal_module._journal_source_authority_total_changes(
                            journal
                        ),
                        total_changes,
                    )
                    with journal.transaction() as transaction:
                        self.assertIsNone(
                            transaction.read_confirmation_result(
                                message_id="message:policy:next"
                            )
                        )
                finally:
                    journal.close()

    def test_typed_readback_rejects_semantically_forged_confirmation(self) -> None:
        message_time = datetime(2026, 8, 14, 14, 20, tzinfo=UTC)
        received_at = message_time + timedelta(seconds=1)
        message_id = "message:forged-readback"
        event_id, _ = stable_execution_event_identity(message_id, 0)
        destination = "CODEX_TASK"
        acknowledgement_key = journal_module._confirmation_outbox_key(
            event_id,
            destination,
        )
        details = {
            "acknowledgement": {
                "destination": destination,
                "idempotency_key": acknowledgement_key,
            },
            "domain_kind": "SOLD",
            "event_role": "ECONOMIC",
            "event_time_basis": "EXPLICIT_ET",
            "missing_fields": [],
            "normalized": {
                "price": "101",
                "quantity": 1,
                "symbol": "SPY",
            },
            "reason_codes": ["POSITION_NOT_TRACKED"],
            "source": {
                "received_at": received_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "type": "ROBINHOOD_MANUAL_CONFIRMATION",
            },
            "version": 1,
        }
        with self.journal.transaction() as transaction:
            raw_id, _ = transaction.append_raw_message(
                message_id,
                message_time,
                "SKIPPED SPY",
            )
            event_row_id, _ = transaction.append_execution_event(
                raw_message_id=raw_id,
                action_ordinal=0,
                parsed_action="SOLD",
                event_time=datetime(2026, 8, 14, 14, 19, tzinfo=UTC),
                symbol="SPY",
                shares=1,
                price_micros=101_000_000,
                compliance_result="NONCOMPLIANT_RECONCILIATION_REQUIRED",
                reconciliation_state="REQUIRED",
                details=details,
            )
            payload = json.dumps(
                {
                    "event_id": event_id,
                    "kind": "SOLD",
                    "ordinal": 0,
                    "reason_codes": ["POSITION_NOT_TRACKED"],
                    "status": "NONCOMPLIANT_RECONCILIATION_REQUIRED",
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
                created_at=received_at,
            )

        with self.journal.transaction() as transaction:
            with self.assertRaises(MigrationCorruption):
                transaction.read_confirmation_result(message_id=message_id)
            with self.assertRaises(MigrationCorruption):
                transaction.read_actual_replay(
                    query_cutoff=received_at + timedelta(seconds=1)
                )

        self.journal.close()
        self.journal = Journal.open(self.path)
        with self.assertRaises(MigrationCorruption):
            self.ingest(
                envelope(
                    "message:after-forged-restart",
                    message_time=message_time + timedelta(minutes=1),
                    received_at=received_at + timedelta(minutes=1),
                )
            )

    def test_report_claim_writes_revoke_all_authorities_before_sql(self) -> None:
        self.ingest(envelope("message:report-dirty-source"))

        def authorities():
            cutoff = datetime(2026, 8, 14, 14, 21, tzinfo=UTC)
            with self.journal.transaction() as transaction:
                source = transaction.read_actual_replay(query_cutoff=cutoff)
            state = replay_actual(
                source,
                plans=self.plans,
                calendar=self.calendar,
                policy=self.policy,
            )
            settlement = risk_module._issue_settlement_replay(
                source,
                state,
                self.calendar,
            )
            cohort = ledger_module._issue_actual_projection_from_journal(
                source,
                state,
            )
            return source, state, settlement, cohort

        def truth(items) -> tuple[bool, bool, bool, bool]:
            source, state, settlement, cohort = items
            return (
                is_verified_journal_replay_source(source),
                is_verified_actual_ledger_state(state),
                settlement.source_verified,
                ledger_module.is_issued_actual_projection_cohort(cohort),
            )

        original_sql = journal_module._sql
        insert_authority = authorities()
        observed_insert: list[tuple[bool, bool, bool, bool]] = []

        def observe_insert(connection, statement, parameters=()):
            if statement.lstrip().startswith("INSERT INTO report_claims"):
                observed_insert.append(truth(insert_authority))
            return original_sql(connection, statement, parameters)

        with patch.object(journal_module, "_sql", observe_insert):
            claim = self.journal.claim_report(SESSION, "PREMARKET")
        self.assertEqual(observed_insert, [(False, False, False, False)])

        recovery_authority = authorities()
        observed_recovery: list[tuple[bool, bool, bool, bool]] = []

        def observe_recovery(connection, statement, parameters=()):
            if statement.lstrip().startswith("UPDATE report_claims SET"):
                observed_recovery.append(truth(recovery_authority))
            return original_sql(connection, statement, parameters)

        assert claim.lease_expires_at is not None
        with (
            patch.object(
                journal_module,
                "_utc_now",
                return_value=claim.lease_expires_at + timedelta(seconds=1),
            ),
            patch.object(journal_module, "_sql", observe_recovery),
        ):
            recovered = self.journal.claim_report(SESSION, "PREMARKET")
        self.assertEqual(recovered.status, "RECOVERED_EXPIRED")
        self.assertEqual(observed_recovery, [(False, False, False, False)])


if __name__ == "__main__":
    unittest.main()
