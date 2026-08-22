"""Atomic Journal and publisher tests for canonical report publication."""

from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import stock_monitor.journal as journal_module
import stock_monitor.provider_workflows as provider_workflows_module
import stock_monitor.workflows as workflows_module
import tests.integration.test_canonical_workflow_foundation as foundation_module
from stock_monitor.journal import (
    IdempotencyConflict,
    Journal,
    JournalError,
    MigrationCorruption,
    report_archive_relative_path,
    stable_report_id,
)
from stock_monitor.provider_workflows import (
    canonical_close_state_hash,
    canonical_premarket_state_hash,
    issue_canonical_close_material,
    issue_canonical_premarket_material,
)
from stock_monitor.workflows import (
    CanonicalJournalWorkflowPublisher,
    PremarketSnapshot,
    WorkflowError,
)

DECISION_AT = foundation_module.DECISION_AT
REVIEW_AT = foundation_module.REVIEW_AT
QUERY_CUTOFF = foundation_module.QUERY_CUTOFF
CLOSE_RETRIEVED_AT = foundation_module.RETRIEVED_AT
EMPTY_CLOSE_REASONS = foundation_module.EMPTY_CLOSE_REASONS


DAY = date(2026, 8, 24)
ECONOMIC_AT = datetime(2026, 8, 24, 12, 45, tzinfo=timezone.utc)
RETRIEVED_AT = ECONOMIC_AT + timedelta(minutes=7)
RECORDED_AT = RETRIEVED_AT + timedelta(seconds=1)


class CanonicalJournalPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.journal = Journal.open(
            Path(self.temporary.name) / "journal.sqlite3"
        )
        self.addCleanup(self.journal.close)
        self.receipt = self.journal.append_source_observation_receipt(
            payload=b'{"symbol":"AAPL"}',
            source_uri="https://data.alpaca.markets/v2/stocks/bars",
            source_type="ALPACA_DAILY_BARS",
            provider="Alpaca",
            feed="SIP",
            source_time=ECONOMIC_AT - timedelta(minutes=20),
            retrieved_at=ECONOMIC_AT - timedelta(minutes=1),
            provider_sequence=1,
            delay_seconds=900,
            health_result="OK",
            details={"page_ordinal": 1, "source_role": "DAILY_BARS"},
        )

    def _values(self, *, workflow_kind: str = "PREMARKET") -> dict[str, object]:
        storage_kind = "MORNING" if workflow_kind == "PREMARKET" else "CLOSE"
        state_sha256 = "a" * 64
        report_id = stable_report_id(
            storage_kind,
            DAY,
            (self.receipt.observation_sha256,),
            state_sha256,
        )
        return {
            "body": "# Canonical report\n",
            "state_sha256": state_sha256,
            "observation_ids": (self.receipt.row_id,),
            "archive_relative_path": report_archive_relative_path(
                storage_kind, DAY, report_id
            ),
            "created_at": RECORDED_AT,
            "outbox_destination": "CODEX_TASK",
            "outbox_payload": "# Canonical report\n",
            "workflow_kind": workflow_kind,
            "economic_at": ECONOMIC_AT,
            "retrieved_at": RETRIEVED_AT,
            "material_digest": "b" * 64,
            "source_digest": "c" * 64,
        }

    def test_claim_report_and_canonical_finalize_commit_one_complete_cohort(self) -> None:
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ):
            with self.journal.transaction() as transaction:
                claim = transaction.claim_report(DAY, "MORNING")
                self.assertEqual(claim.status, "ACQUIRED")
                assert claim.claim_token is not None
                finalized = self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **self._values(),
                )

        self.assertFalse(finalized.report.duplicate)
        self.assertEqual(finalized.context.workflow_kind, "PREMARKET")
        self.assertEqual(self.journal.count("report_claims"), 1)
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("report_observations"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 1)
        self.assertEqual(
            self.journal.read_canonical_report_context(
                finalized.report.report_id
            ),
            finalized.context,
        )

    def test_context_failure_rolls_back_claim_report_pins_outbox_and_context(self) -> None:
        self.journal._connection.execute(
            "CREATE TRIGGER injected_context_failure "
            "BEFORE INSERT ON canonical_report_contexts BEGIN "
            "SELECT RAISE(ABORT, 'injected canonical context failure'); END"
        )
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ), self.assertRaises(IdempotencyConflict):
            with self.journal.transaction() as transaction:
                claim = transaction.claim_report(DAY, "MORNING")
                assert claim.claim_token is not None
                self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **self._values(),
                )

        for table in (
            "report_claims",
            "reports",
            "report_observations",
            "outbox",
            "canonical_report_contexts",
        ):
            with self.subTest(table=table):
                self.assertEqual(self.journal.count(table), 0)

    def test_exact_context_retry_succeeds_but_mismatch_is_rejected(self) -> None:
        values = self._values(workflow_kind="CLOSE")
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ):
            with self.journal.transaction() as transaction:
                claim = transaction.claim_report(DAY, "CLOSE")
                assert claim.claim_token is not None
                finalized = self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **values,
                )
            with self.journal.transaction():
                replay = self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **values,
                )
            with self.assertRaises(IdempotencyConflict), self.journal.transaction():
                self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **{**values, "material_digest": "d" * 64},
                )

        self.assertFalse(finalized.report.duplicate)
        self.assertTrue(replay.report.duplicate)
        self.assertEqual(replay.context, finalized.context)
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 1)

    def test_canonical_finalize_rejects_context_free_finalized_report(self) -> None:
        values = self._values(workflow_kind="CLOSE")
        legacy = {
            key: values[key]
            for key in (
                "body",
                "state_sha256",
                "observation_ids",
                "archive_relative_path",
                "created_at",
                "outbox_destination",
                "outbox_payload",
            )
        }
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ):
            claim = self.journal.claim_report(DAY, "CLOSE")
            assert claim.claim_token is not None
            self.journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                **legacy,
            )
            with self.assertRaisesRegex(
                IdempotencyConflict, "canonical context"
            ), self.journal.transaction():
                self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **values,
                )

        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_public_canonical_finalize_requires_an_exact_issued_plan(self) -> None:
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ), self.assertRaisesRegex(
            IdempotencyConflict, "plan authority"
        ):
            with self.journal.transaction() as transaction:
                claim = transaction.claim_report(DAY, "MORNING")
                assert claim.claim_token is not None
                transaction.finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    publication_plan=object(),
                )
        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_transaction_claim_cannot_write_after_source_authority_read(self) -> None:
        with self.journal.transaction() as transaction:
            transaction.read_actual_replay(query_cutoff=RECORDED_AT)
            with self.assertRaisesRegex(JournalError, "sealed read-only"):
                transaction.claim_report(DAY, "MORNING")
        self.assertEqual(self.journal.count("report_claims"), 0)

    def test_failed_recovery_restores_prior_expired_token_and_lease(self) -> None:
        lease_start = RECORDED_AT
        with patch.object(
            journal_module, "_utc_now", return_value=lease_start
        ):
            original = self.journal.claim_report(DAY, "CLOSE")
        assert original.claim_token is not None
        before = self.journal._connection.execute(
            "SELECT claim_token, lease_started_at, lease_expires_at "
            "FROM report_claims WHERE id = ?",
            (original.claim_id,),
        ).fetchone()
        self.journal._connection.execute(
            "CREATE TRIGGER injected_recovery_context_failure "
            "BEFORE INSERT ON canonical_report_contexts BEGIN "
            "SELECT RAISE(ABORT, 'injected recovery failure'); END"
        )
        values = self._values(workflow_kind="CLOSE")
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=lease_start + timedelta(seconds=300),
        ), self.assertRaises(IdempotencyConflict):
            with self.journal.transaction() as transaction:
                recovered = transaction.claim_report(DAY, "CLOSE")
                self.assertEqual(recovered.status, "RECOVERED_EXPIRED")
                assert recovered.claim_token is not None
                self.journal._finalize_canonical_report(
                    claim_id=recovered.claim_id,
                    claim_token=recovered.claim_token,
                    **values,
                )
        after = self.journal._connection.execute(
            "SELECT claim_token, lease_started_at, lease_expires_at "
            "FROM report_claims WHERE id = ?",
            (original.claim_id,),
        ).fetchone()
        self.assertEqual(after, before)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_context_reader_recomputes_the_record_hash(self) -> None:
        with patch.object(
            journal_module, "_utc_now", return_value=RECORDED_AT
        ):
            with self.journal.transaction() as transaction:
                claim = transaction.claim_report(DAY, "MORNING")
                assert claim.claim_token is not None
                finalized = self.journal._finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    **self._values(),
                )
        self.journal._connection.execute(
            "DROP TRIGGER canonical_report_contexts_no_update"
        )
        self.journal._connection.execute(
            "UPDATE canonical_report_contexts SET record_sha256 = ? WHERE id = ?",
            ("d" * 64, finalized.context.context_row_id),
        )
        with self.assertRaisesRegex(MigrationCorruption, "record hash"):
            self.journal.read_canonical_report_context(
                finalized.report.report_id
            )


class CanonicalPublisherPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "reports"
        self.root.mkdir()
        self.journal = Journal.open(
            Path(self.temporary.name) / "publisher.sqlite3"
        )
        self.addCleanup(self.journal.close)

    @contextmanager
    def _issued_premarket(self):
        receipt = foundation_module.CanonicalWorkflowFoundationTests._receipt(self)
        snapshot = PremarketSnapshot((), False)
        values = {
            "session_date": DAY,
            "decision_at": DECISION_AT,
            "retrieved_at": DECISION_AT + timedelta(minutes=7),
            "validation_window_id": "phase1-window",
            "source_receipts": (receipt,),
            "snapshot": snapshot,
            "publication_decision": None,
            "primary_plan": None,
            "outcome": "NO TRADE",
            "reason_codes": ("NO_CANDIDATES",),
        }
        envelope = provider_workflows_module._premarket_composition_envelope(
            **values
        )
        authority = (
            foundation_module.CanonicalWorkflowFoundationTests._premarket_authority_from_envelope(
                envelope
            )
        )
        children = (
            provider_workflows_module._premarket_composition_identity_children(
                snapshot=snapshot,
                source_receipts=(receipt,),
                publication_decision=None,
                primary_plan=None,
            )
        )
        with foundation_module.CanonicalWorkflowFoundationTests._installed_composition_candidate(
            self,
            authority=authority,
            envelope=envelope,
            identity_children=children,
            registry=provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS,
            lock=provider_workflows_module._PREMARKET_COMPOSITION_LOCK,
        ):
            state_hash = canonical_premarket_state_hash(
                **values,
                composition_authority=authority,
            )
            report = foundation_module.CanonicalWorkflowFoundationTests._premarket_report(
                receipt,
                snapshot=snapshot,
                state_hash=state_hash,
            )
            material = issue_canonical_premarket_material(
                journal=self.journal,
                report_archive_root=self.root,
                report=report,
                composition_authority=authority,
                **{
                    key: value
                    for key, value in values.items()
                    if key not in {"outcome", "reason_codes"}
                },
            )
            publisher = CanonicalJournalWorkflowPublisher(
                self.journal, self.root
            )
            result = publisher.issue_result(material=material)
            yield publisher, material, result

    @contextmanager
    def _issued_close(self):
        receipt = foundation_module.CanonicalWorkflowFoundationTests._receipt(
            self,
            suffix="canonical-close",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        replay_source, actual_state = (
            foundation_module.CanonicalWorkflowFoundationTests._actual(self)
        )
        values = {
            "session_date": DAY,
            "review_at": REVIEW_AT,
            "retrieved_at": CLOSE_RETRIEVED_AT,
            "query_cutoff": QUERY_CUTOFF,
            "actual_state": actual_state,
            "actual_replay_source": replay_source,
            "positions": (),
            "source_receipts": (receipt,),
            "outcome": "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
            "reason_codes": EMPTY_CLOSE_REASONS,
        }
        envelope = provider_workflows_module._close_composition_envelope(
            **values
        )
        authority = (
            foundation_module.CanonicalWorkflowFoundationTests._close_authority_from_envelope(
                envelope
            )
        )
        children = provider_workflows_module._close_composition_identity_children(
            actual_state=actual_state,
            actual_replay_source=replay_source,
            positions=(),
            source_receipts=(receipt,),
        )
        with foundation_module.CanonicalWorkflowFoundationTests._installed_composition_candidate(
            self,
            authority=authority,
            envelope=envelope,
            identity_children=children,
            registry=provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS,
            lock=provider_workflows_module._CLOSE_COMPOSITION_LOCK,
        ):
            state_hash = canonical_close_state_hash(
                **values,
                composition_authority=authority,
            )
            report = foundation_module.CanonicalWorkflowFoundationTests._close_report(
                receipt,
                state_hash=state_hash,
            )
            material = issue_canonical_close_material(
                journal=self.journal,
                report_archive_root=self.root,
                report=report,
                composition_authority=authority,
                **{
                    key: value
                    for key, value in values.items()
                    if key not in {"outcome", "reason_codes"}
                },
            )
            publisher = CanonicalJournalWorkflowPublisher(
                self.journal, self.root
            )
            result = publisher.issue_result(material=material)
            yield publisher, material, result

    @contextmanager
    def _publication_clock(self):
        with patch.object(
            journal_module,
            "_utc_now",
            return_value=DECISION_AT + timedelta(minutes=8),
        ):
            yield

    def _assert_trace_mutation_rolls_back(self, statement_fragment: str) -> None:
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            mutated = False

            def mutate(statement: str) -> None:
                nonlocal mutated
                if not mutated and statement_fragment in statement:
                    mutated = True
                    object.__setattr__(
                        result,
                        "message",
                        result.message + "mutated",
                    )

            self.journal._connection.set_trace_callback(mutate)
            try:
                with self.assertRaises(WorkflowError):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=result,
                        material=material,
                    )
            finally:
                self.journal._connection.set_trace_callback(None)
            self.assertTrue(mutated)
        for table in (
            "report_claims",
            "reports",
            "report_observations",
            "outbox",
            "canonical_report_contexts",
        ):
            with self.subTest(fragment=statement_fragment, table=table):
                self.assertEqual(self.journal.count(table), 0)

    def test_premarket_publishes_durable_morning_identity_and_archive(self) -> None:
        with self._issued_premarket() as (publisher, material, result), patch.object(
            journal_module,
            "_utc_now",
            return_value=DECISION_AT + timedelta(minutes=8),
        ):
            published = publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )

        assert published.report_id is not None
        stored = self.journal.read_report(published.report_id)
        context = self.journal.read_canonical_report_context(published.report_id)
        self.assertEqual(stored.report_kind, "MORNING")
        self.assertEqual(context.workflow_kind, "PREMARKET")
        self.assertNotEqual(stored.report_id, material.report.report_id)
        self.assertEqual(Path(published.report_path).read_text(), stored.body)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 1)

    def test_close_publishes_the_exact_close_identity_and_context(self) -> None:
        with self._issued_close() as (publisher, material, result), patch.object(
            journal_module,
            "_utc_now",
            return_value=CLOSE_RETRIEVED_AT + timedelta(minutes=1),
        ):
            published = publisher.publish(
                kind="CLOSE",
                session_date=DAY,
                generated_at=CLOSE_RETRIEVED_AT,
                result=result,
                material=material,
            )
        assert published.report_id is not None
        stored = self.journal.read_report(published.report_id)
        context = self.journal.read_canonical_report_context(published.report_id)
        self.assertEqual(stored.report_kind, "CLOSE")
        self.assertEqual(stored.report_id, material.report.report_id)
        self.assertEqual(context.workflow_kind, "CLOSE")
        self.assertEqual(context.economic_at, REVIEW_AT.astimezone(timezone.utc))

    def test_copies_and_cross_owner_fail_before_any_claim(self) -> None:
        with self._issued_premarket() as (publisher, material, result):
            for candidate_material, candidate_result in (
                (replace(material), result),
                (material, replace(result)),
            ):
                with self.subTest(
                    material_copy=candidate_material is not material,
                    result_copy=candidate_result is not result,
                ), self.assertRaises(WorkflowError):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=candidate_result,
                        material=candidate_material,
                    )

        self.assertEqual(self.journal.count("report_claims"), 0)

    def test_claim_select_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back("FROM report_claims AS claim")

    def test_claim_insert_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back("INSERT INTO report_claims(")

    def test_report_insert_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back("INSERT INTO reports(")

    def test_report_pin_insert_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back(
            "INSERT INTO report_observations("
        )

    def test_outbox_insert_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back("INSERT INTO outbox(")

    def test_context_insert_callback_mutation_rolls_back(self) -> None:
        self._assert_trace_mutation_rolls_back(
            "INSERT INTO canonical_report_contexts("
        )

    def test_archive_code_mutated_during_sql_is_rejected_before_commit(self) -> None:
        def intercepted_archive(_report, root):
            (root / "archive-was-invoked").write_text("unsafe")
            raise AssertionError("mutated archive function was invoked")

        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            archive_function = workflows_module.archive_report
            original_code = archive_function.__code__
            mutated = False

            def mutate(statement: str) -> None:
                nonlocal mutated
                if (
                    not mutated
                    and "INSERT INTO canonical_report_contexts(" in statement
                ):
                    mutated = True
                    archive_function.__code__ = intercepted_archive.__code__

            self.journal._connection.set_trace_callback(mutate)
            try:
                with self.assertRaisesRegex(
                    WorkflowError, "archive dependency was replaced"
                ):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=result,
                        material=material,
                    )
            finally:
                archive_function.__code__ = original_code
                self.journal._connection.set_trace_callback(None)
            self.assertTrue(mutated)

        self.assertFalse((self.root / "archive-was-invoked").exists())
        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_report_constructor_replaced_during_sql_is_rejected_before_commit(
        self,
    ) -> None:
        class InterceptedReport:
            def __init__(intercepted, **_values: object) -> None:
                del intercepted
                (self.root / "report-constructor-was-invoked").write_text(
                    "unsafe"
                )

        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            original_report_type = workflows_module.Report
            mutated = False

            def mutate(statement: str) -> None:
                nonlocal mutated
                if (
                    not mutated
                    and "INSERT INTO canonical_report_contexts(" in statement
                ):
                    mutated = True
                    workflows_module.Report = InterceptedReport

            self.journal._connection.set_trace_callback(mutate)
            try:
                with self.assertRaisesRegex(
                    WorkflowError, "report dependency was replaced"
                ):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=result,
                        material=material,
                    )
            finally:
                workflows_module.Report = original_report_type
                self.journal._connection.set_trace_callback(None)
            self.assertTrue(mutated)

        self.assertFalse(
            (self.root / "report-constructor-was-invoked").exists()
        )
        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_commit_trace_callback_is_removed_before_final_authority_seal(self) -> None:
        def intercepted_archive(_report, _root):
            raise AssertionError("COMMIT callback changed the archive function")

        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            archive_function = workflows_module.archive_report
            original_code = archive_function.__code__
            commit_called = False

            def observe(statement: str) -> None:
                nonlocal commit_called
                if statement.strip().upper() == "COMMIT":
                    commit_called = True
                    archive_function.__code__ = intercepted_archive.__code__

            self.journal._connection.set_trace_callback(observe)
            try:
                published = publisher.publish(
                    kind="PREMARKET",
                    session_date=DAY,
                    generated_at=DECISION_AT + timedelta(minutes=7),
                    result=result,
                    material=material,
                )
                self.assertIs(archive_function.__code__, original_code)
            finally:
                archive_function.__code__ = original_code
                self.journal._connection.set_trace_callback(None)
            self.assertEqual(published.status, "PUBLISHED")
            self.assertFalse(commit_called)

    def test_successful_commit_revokes_the_old_material_and_result(self) -> None:
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
            with self.assertRaises(WorkflowError):
                publisher.prepare_publication(
                    kind="PREMARKET",
                    session_date=DAY,
                    generated_at=DECISION_AT + timedelta(minutes=7),
                    result=result,
                    material=material,
                )

    def test_separately_committed_claim_invalidates_an_issued_plan(self) -> None:
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            plan = publisher.prepare_publication(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
            claim = self.journal.claim_report(DAY, "MORNING")
            assert claim.claim_token is not None
            with self.assertRaisesRegex(
                IdempotencyConflict, "plan authority"
            ):
                self.journal.finalize_canonical_report(
                    claim_id=claim.claim_id,
                    claim_token=claim.claim_token,
                    publication_plan=plan,
                )
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_active_claim_returns_identity_free_in_progress(self) -> None:
        with self._publication_clock():
            self.journal.claim_report(DAY, "MORNING")
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            published = publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
        self.assertEqual(published.status, "IN_PROGRESS")
        self.assertIsNone(published.report_id)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_in_progress_claim_select_mutation_is_rejected(self) -> None:
        with self._publication_clock():
            self.journal.claim_report(DAY, "MORNING")
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            mutated = False

            def mutate(statement: str) -> None:
                nonlocal mutated
                if not mutated and "FROM report_claims AS claim" in statement:
                    mutated = True
                    object.__setattr__(result, "message", result.message + "mutated")

            self.journal._connection.set_trace_callback(mutate)
            try:
                with self.assertRaises(WorkflowError):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=result,
                        material=material,
                    )
            finally:
                self.journal._connection.set_trace_callback(None)
            self.assertTrue(mutated)
        self.assertEqual(self.journal.count("report_claims"), 1)
        self.assertEqual(self.journal.count("reports"), 0)

    def test_recovery_update_callback_mutation_rolls_back_old_lease(self) -> None:
        with self._publication_clock():
            original = self.journal.claim_report(DAY, "MORNING")
        before = self.journal._connection.execute(
            "SELECT claim_token, lease_started_at, lease_expires_at "
            "FROM report_claims WHERE id = ?",
            (original.claim_id,),
        ).fetchone()
        with self._issued_premarket() as (publisher, material, result), patch.object(
            journal_module,
            "_utc_now",
            return_value=DECISION_AT + timedelta(minutes=13),
        ):
            mutated = False

            def mutate(statement: str) -> None:
                nonlocal mutated
                if not mutated and "UPDATE report_claims SET claim_token" in statement:
                    mutated = True
                    object.__setattr__(result, "message", result.message + "mutated")

            self.journal._connection.set_trace_callback(mutate)
            try:
                with self.assertRaises(WorkflowError):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=DAY,
                        generated_at=DECISION_AT + timedelta(minutes=7),
                        result=result,
                        material=material,
                    )
            finally:
                self.journal._connection.set_trace_callback(None)
            self.assertTrue(mutated)
        after = self.journal._connection.execute(
            "SELECT claim_token, lease_started_at, lease_expires_at "
            "FROM report_claims WHERE id = ?",
            (original.claim_id,),
        ).fetchone()
        self.assertEqual(after, before)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 0)

    def test_missing_archive_is_healed_by_fresh_exact_material(self) -> None:
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            first = publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
        assert first.report_path is not None
        Path(first.report_path).unlink()

        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            healed = publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
        self.assertEqual(healed.status, "ALREADY_EMITTED")
        self.assertEqual(healed.report_id, first.report_id)
        self.assertTrue(Path(first.report_path).is_file())
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 1)

    def test_archive_conflict_after_commit_is_not_overwritten_and_can_heal(self) -> None:
        with self._issued_premarket() as (publisher, material, result):
            durable_id = stable_report_id(
                "MORNING",
                DAY,
                material.report.observation_ids,
                material.report.state_hash,
            )
            conflict_path = self.root / report_archive_relative_path(
                "MORNING", DAY, durable_id
            )
            conflict_path.parent.mkdir(parents=True)
            conflict_path.write_bytes(b"conflicting archive")
            with self._publication_clock(), self.assertRaisesRegex(
                WorkflowError, "archive failed"
            ):
                publisher.publish(
                    kind="PREMARKET",
                    session_date=DAY,
                    generated_at=DECISION_AT + timedelta(minutes=7),
                    result=result,
                    material=material,
                )

        self.assertEqual(conflict_path.read_bytes(), b"conflicting archive")
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("canonical_report_contexts"), 1)
        conflict_path.unlink()
        with self._issued_premarket() as (publisher, material, result), (
            self._publication_clock()
        ):
            healed = publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
        self.assertEqual(healed.status, "ALREADY_EMITTED")
        self.assertEqual(conflict_path.read_bytes(), material.report.body.encode())


if __name__ == "__main__":
    unittest.main()
