from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import stock_monitor.journal as journal_module
import stock_monitor.scheduled as scheduled_module
import stock_monitor.workflows as workflows_module
from stock_monitor.journal import (
    InvalidJournalValue,
    Journal,
    ScheduledRunResultEnvelope,
    report_archive_relative_path,
    stable_report_id,
)
from stock_monitor.scheduled import JournalScheduledRunStore, RunKind, run_scheduled
from stock_monitor.workflows import (
    CandidateSummary,
    JournalWorkflowPublisher,
    PublishedWorkflow,
    RecordedScenarioAdapter,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    run_premarket,
)


SCENARIOS = Path(__file__).parents[1] / "fixtures" / "scenarios"
ET = ZoneInfo("America/New_York")
SESSION_DATE = date(2026, 8, 14)
INTENDED = datetime(2026, 8, 14, 15, 30, tzinfo=ET)


def _envelope(
    *,
    outcome: str = "CONFIGURATION_REQUIRED",
    message: str = "CONFIGURATION REQUIRED",
    exit_code: int = 2,
    reason_codes: tuple[str, ...] = ("CONFIGURATION_REQUIRED",),
    candidates: tuple[tuple[str, str], ...] = (),
    report_id: str | None = None,
    report_row_id: int | None = None,
    report_path: str | None = None,
) -> ScheduledRunResultEnvelope:
    return ScheduledRunResultEnvelope(
        outcome=outcome,
        message=message,
        exit_code=exit_code,
        reason_codes=reason_codes,
        execution_mode="FIXTURE",
        candidates=candidates,
        report_id=report_id,
        report_row_id=report_row_id,
        report_path=report_path,
    )


class ScheduledAuthorityBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)

    def _journal(self, name: str = "journal.sqlite3") -> Journal:
        journal = Journal.open(Path(self.temporary.name) / name)
        self.addCleanup(journal.close)
        journal.migrate()
        return journal

    def _context(self, fixture: str) -> WorkflowContext:
        adapter = RecordedScenarioAdapter.load(SCENARIOS / fixture)
        fixture_root = (
            Path(self.temporary.name)
            / ".stock-monitor"
            / "fixtures"
            / adapter.evidence.state_hash
        )
        journal = Journal.open(fixture_root / "journal.sqlite3")
        self.addCleanup(journal.close)
        journal.migrate()
        adapter = adapter.bind_source_observation(journal)
        return WorkflowContext(
            adapter=adapter,
            publisher=JournalWorkflowPublisher(journal, fixture_root),
            scheduler=JournalScheduledRunStore(journal),
            now=adapter.now,
        )

    @staticmethod
    def _seed_started_run(
        journal: Journal,
        *,
        run_key: str,
        kind: str = "CLOSE",
        intended: datetime = INTENDED,
    ) -> int:
        stored = intended.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        cursor = journal._connection.execute(
            "INSERT INTO scheduled_runs("
            "run_key, run_kind, session_date, intended_run_at, started_at"
            ") VALUES (?, ?, ?, ?, ?)",
            (run_key, kind, intended.date().isoformat(), stored, stored),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _seed_completed_result(
        journal: Journal,
        *,
        run_key: str,
        envelope: ScheduledRunResultEnvelope,
        kind: str = "CLOSE",
    ) -> int:
        run_id = ScheduledAuthorityBoundaryTests._seed_started_run(
            journal,
            run_key=run_key,
            kind=kind,
        )
        stored_json, stored_sha256 = (
            journal_module._scheduled_result_envelope_storage(envelope)
        )
        journal._connection.execute(
            "UPDATE scheduled_runs SET finished_at = ?, "
            "market_session_decision = 'DUE_WAKE', outcome = 'NOOP', "
            "result_envelope_json = ?, result_envelope_sha256 = ? WHERE id = ?",
            (
                "2026-08-14T19:30:01.000000Z",
                stored_json,
                stored_sha256,
                run_id,
            ),
        )
        return run_id

    def test_direct_scheduled_store_start_and_complete_are_denied(self) -> None:
        journal = self._journal()
        scheduler = JournalScheduledRunStore(journal)

        with self.assertRaises(WorkflowError):
            scheduler.start(
                kind="CLOSE",
                session_date=SESSION_DATE,
                intended_at=INTENDED,
            )
        self.assertEqual(journal.count("scheduled_runs"), 0)

        self._seed_started_run(
            journal,
            run_key="stock-monitor:2026-08-14:CLOSE",
        )
        with self.assertRaises(WorkflowError):
            scheduler.complete(
                kind="CLOSE",
                session_date=SESSION_DATE,
                intended_at=INTENDED,
                finished_at=INTENDED + timedelta(seconds=1),
                decision="DUE_WAKE",
                result=WorkflowResult(
                    outcome="CONFIGURATION_REQUIRED",
                    message="CONFIGURATION REQUIRED",
                    exit_code=2,
                    reason_codes=("CONFIGURATION_REQUIRED",),
                    execution_mode="FIXTURE",
                ),
            )
        self.assertEqual(
            journal._connection.execute(
                "SELECT finished_at FROM scheduled_runs "
                "WHERE run_key = 'stock-monitor:2026-08-14:CLOSE'"
            ).fetchone(),
            (None,),
        )

    def test_direct_journal_scheduled_writers_are_denied(self) -> None:
        journal = self._journal()
        with self.assertRaises(InvalidJournalValue):
            journal.start_scheduled_run(
                run_key="direct-journal-start",
                run_kind="CLOSE",
                session_date=SESSION_DATE,
                intended_run_at=INTENDED,
                started_at=INTENDED,
            )
        self.assertEqual(journal.count("scheduled_runs"), 0)

        run_id = self._seed_started_run(
            journal,
            run_key="direct-journal-complete",
        )
        with self.assertRaises(InvalidJournalValue):
            journal.complete_scheduled_run(
                run_id=run_id,
                finished_at=INTENDED + timedelta(seconds=1),
                market_session_decision="DUE_WAKE",
                outcome="NOOP",
                result_envelope=_envelope(),
            )
        self.assertEqual(
            journal._connection.execute(
                "SELECT finished_at FROM scheduled_runs WHERE id = ?",
                (run_id,),
            ).fetchone(),
            (None,),
        )

    def test_report_completion_cannot_store_a_forged_body(self) -> None:
        journal = self._journal()
        now = datetime(2026, 8, 14, 19, 30, tzinfo=timezone.utc)
        state_sha256 = "a" * 64
        report_id = stable_report_id("CLOSE", SESSION_DATE, (), state_sha256)
        report_path = report_archive_relative_path(
            "CLOSE",
            SESSION_DATE,
            report_id,
        )
        with patch.object(journal_module, "_utc_now", return_value=now):
            claim = journal.claim_report(SESSION_DATE, "CLOSE")
            assert claim.claim_token is not None
            report = journal.finalize_report(
                claim_id=claim.claim_id,
                claim_token=claim.claim_token,
                body="HONEST REPORT BODY",
                state_sha256=state_sha256,
                observation_ids=(),
                archive_relative_path=report_path,
                created_at=now,
                outbox_destination="CODEX_TASK",
                outbox_payload="HONEST REPORT BODY",
            )
        run_id = self._seed_started_run(
            journal,
            run_key="forged-report-body",
            intended=now,
        )

        with self.assertRaises(InvalidJournalValue):
            journal.complete_scheduled_run(
                run_id=run_id,
                finished_at=now + timedelta(seconds=1),
                market_session_decision="DUE_WAKE",
                outcome="REPORT_EMITTED",
                report_id=report.report_row_id,
                report_path=report_path,
                result_envelope=_envelope(
                    outcome="EMITTED",
                    message="FORGED REPORT BODY",
                    exit_code=0,
                    reason_codes=("SCHEDULED_EMITTED",),
                    report_id=report_id,
                    report_row_id=report.report_row_id,
                    report_path=report_path,
                ),
            )
        self.assertEqual(
            journal._connection.execute(
                "SELECT finished_at FROM scheduled_runs WHERE id = ?",
                (run_id,),
            ).fetchone(),
            (None,),
        )

    def test_trusted_completion_rejects_a_mutated_report_body(self) -> None:
        context = self._context("eligible.json")
        assert context.scheduler is not None
        original_replace = workflows_module.replace

        def forge(value: object, *args: object, **kwargs: object) -> object:
            result = original_replace(value, *args, **kwargs)
            if type(result) is WorkflowResult and result.report_id is not None:
                object.__setattr__(result, "message", "FORGED REPORT BODY")
            return result

        with patch.object(workflows_module, "replace", side_effect=forge):
            with self.assertRaises((InvalidJournalValue, WorkflowError)):
                run_scheduled(
                    RunKind.PREMARKET,
                    datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                    context,
                )

        try:
            status = context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            )
        except InvalidJournalValue:
            status = None
        self.assertIn(status, {None, "REPORT_FINALIZED"})

    def test_begin_trace_publisher_root_mutation_revokes_completion(self) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        wrong_root = Path(self.temporary.name) / "trace-wrong-root"
        observed = {"fired": False}

        def mutate(statement: str) -> None:
            if statement.startswith("BEGIN") and not observed["fired"]:
                observed["fired"] = True
                context.publisher.report_archive_root = wrong_root

        context.publisher.journal._connection.set_trace_callback(mutate)
        try:
            with self.assertRaises((InvalidJournalValue, WorkflowError)):
                run_scheduled(
                    RunKind.PREMARKET,
                    datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                    context,
                )
        finally:
            context.publisher.journal._connection.set_trace_callback(None)

        self.assertTrue(observed["fired"])
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            ),
            "IN_PROGRESS",
        )
        self.assertEqual(tuple(wrong_root.rglob("*.md")), ())

    def test_begin_trace_mutation_cannot_complete_a_raw_result(self) -> None:
        journal = self._journal()
        scheduler = JournalScheduledRunStore(journal)
        self._seed_started_run(
            journal,
            run_key="stock-monitor:2026-08-14:CLOSE",
        )
        result = WorkflowResult(
            outcome="CONFIGURATION_REQUIRED",
            message="ORIGINAL",
            exit_code=2,
            reason_codes=("CONFIGURATION_REQUIRED",),
            execution_mode="FIXTURE",
        )
        observed = {"fired": False}

        def mutate(statement: str) -> None:
            if statement.startswith("BEGIN") and not observed["fired"]:
                observed["fired"] = True
                object.__setattr__(result, "message", "FORGED")

        journal._connection.set_trace_callback(mutate)
        try:
            with self.assertRaises(WorkflowError):
                scheduler.complete(
                    kind="CLOSE",
                    session_date=SESSION_DATE,
                    intended_at=INTENDED,
                    finished_at=INTENDED + timedelta(seconds=1),
                    decision="DUE_WAKE",
                    result=result,
                )
        finally:
            journal._connection.set_trace_callback(None)
        self.assertEqual(
            journal._connection.execute(
                "SELECT finished_at FROM scheduled_runs "
                "WHERE run_key = 'stock-monitor:2026-08-14:CLOSE'"
            ).fetchone(),
            (None,),
        )

    def test_candidates_without_a_publisher_never_complete(self) -> None:
        context = self._context("eligible.json")
        context = replace(context, publisher=None)
        assert context.scheduler is not None

        result = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            context,
        )

        self.assertEqual(result.exit_code, 10)
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            ),
            "IN_PROGRESS",
        )

    def test_raw_healer_receipt_cannot_turn_a_retry_green(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        assert context.publisher is not None
        assert first.report_id is not None
        assert first.report_row_id is not None
        assert first.report_path is not None
        Path(first.report_path).unlink()
        raw = PublishedWorkflow(
            report_id=first.report_id,
            report_row_id=first.report_row_id,
            report_path=first.report_path,
            status="ALREADY_EMITTED",
        )

        with patch.object(context.publisher, "heal_finalized", return_value=raw):
            retried = run_scheduled(
                RunKind.CLOSE,
                datetime(2026, 8, 14, 15, 31, tzinfo=ET),
                context,
            )

        self.assertEqual(retried.exit_code, 10)
        self.assertFalse(Path(first.report_path).exists())

    def test_wrong_root_and_cross_journal_healers_are_rejected(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        assert context.publisher is not None
        assert first.report_id is not None
        assert first.report_row_id is not None
        assert first.report_path is not None
        Path(first.report_path).unlink()
        wrong_root = Path(self.temporary.name) / "wrong-root"
        context.publisher.report_archive_root = wrong_root

        retried = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 31, tzinfo=ET),
            context,
        )
        self.assertEqual(retried.exit_code, 10)
        self.assertFalse(wrong_root.exists())

        assert context.scheduler is not None
        stored_result = context.scheduler.result(
            kind="CLOSE",
            session_date=SESSION_DATE,
        )
        assert stored_result is not None
        assert stored_result.report_path is not None
        other_journal = self._journal("other.sqlite3")
        other_root = Path(self.temporary.name) / "other-root"
        cross_publisher = JournalWorkflowPublisher(
            other_journal,
            other_root,
        )
        raw = PublishedWorkflow(
            first.report_id,
            first.report_row_id,
            str(other_root / stored_result.report_path),
            "ALREADY_EMITTED",
        )
        with patch.object(cross_publisher, "heal_finalized", return_value=raw):
            cross_retry = run_scheduled(
                RunKind.CLOSE,
                datetime(2026, 8, 14, 15, 32, tzinfo=ET),
                replace(context, publisher=cross_publisher),
            )
        self.assertEqual(cross_retry.exit_code, 10)

    def test_symlink_archive_root_cannot_rebind_a_healer_receipt(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        assert context.publisher is not None
        assert context.scheduler is not None
        assert first.report_id is not None
        assert first.report_row_id is not None
        assert first.report_path is not None
        Path(first.report_path).unlink()
        stored = context.scheduler.result(
            kind="CLOSE",
            session_date=SESSION_DATE,
        )
        assert stored is not None
        assert stored.report_path is not None
        real_root = context.publisher.report_archive_root
        alias = Path(self.temporary.name) / "archive-alias"
        alias.symlink_to(real_root, target_is_directory=True)
        context.publisher.report_archive_root = alias
        raw = PublishedWorkflow(
            first.report_id,
            first.report_row_id,
            str(alias / stored.report_path),
            "ALREADY_EMITTED",
        )

        with patch.object(context.publisher, "heal_finalized", return_value=raw):
            retried = run_scheduled(
                RunKind.CLOSE,
                datetime(2026, 8, 14, 15, 31, tzinfo=ET),
                context,
            )

        self.assertEqual(retried.exit_code, 10)

    def test_finalized_report_crash_gap_stays_fail_closed(self) -> None:
        context = self._context("eligible.json")
        assert context.scheduler is not None
        self._seed_started_run(
            context.publisher.journal,  # type: ignore[union-attr]
            run_key="stock-monitor:2026-08-14:PREMARKET",
            kind="PREMARKET",
            intended=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
        )
        published = run_premarket(replace(context, scheduler=None))
        assert published.report_path is not None
        Path(published.report_path).unlink()

        retried = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 46, tzinfo=ET),
            context,
        )

        self.assertEqual(retried.exit_code, 10)
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            ),
            "REPORT_FINALIZED",
        )
        self.assertFalse(Path(published.report_path).exists())

    def test_self_restoring_decoder_cannot_change_stored_exit_truth(self) -> None:
        journal = self._journal()
        run_key = "stock-monitor:2026-08-14:CLOSE"
        self._seed_completed_result(
            journal,
            run_key=run_key,
            envelope=_envelope(),
        )
        scheduler = JournalScheduledRunStore(journal)
        original_decoder = journal_module._decode_scheduled_result_envelope

        def forged_decoder(*_args: object) -> ScheduledRunResultEnvelope:
            journal_module._decode_scheduled_result_envelope = original_decoder
            return _envelope(
                outcome="ALREADY_COMPLETED_NOOP",
                message="FORGED GREEN",
                exit_code=0,
                reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
            )

        journal_module._decode_scheduled_result_envelope = forged_decoder
        try:
            stored = scheduler.result(kind="CLOSE", session_date=SESSION_DATE)
        finally:
            journal_module._decode_scheduled_result_envelope = original_decoder

        assert stored is not None
        self.assertEqual(stored.exit_code, 2)
        self.assertEqual(stored.message, "CONFIGURATION REQUIRED")

    def test_self_restoring_result_constructors_cannot_change_stored_truth(self) -> None:
        journal = self._journal()
        run_key = "stock-monitor:2026-08-14:PREMARKET"
        self._seed_completed_result(
            journal,
            run_key=run_key,
            envelope=_envelope(
                outcome="CANDIDATES",
                message="ONE CANDIDATE",
                exit_code=0,
                reason_codes=("PAPER_PLAN_ONLY",),
                candidates=(("SPY", "PRIMARY"),),
            ),
            kind="PREMARKET",
        )
        scheduler = JournalScheduledRunStore(journal)
        original_result_type = scheduled_module.WorkflowResult
        original_candidate_type = scheduled_module.CandidateSummary

        def forged_candidate(**_kwargs: object) -> CandidateSummary:
            scheduled_module.CandidateSummary = original_candidate_type
            return original_candidate_type(symbol="QQQ", role="SECONDARY")

        def forged_result(**_kwargs: object) -> WorkflowResult:
            scheduled_module.WorkflowResult = original_result_type
            return original_result_type(
                outcome="ALREADY_COMPLETED_NOOP",
                message="FORGED GREEN",
                exit_code=0,
                reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                execution_mode="FIXTURE",
            )

        scheduled_module.CandidateSummary = forged_candidate
        scheduled_module.WorkflowResult = forged_result
        try:
            stored = scheduler.result(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            )
        finally:
            scheduled_module.CandidateSummary = original_candidate_type
            scheduled_module.WorkflowResult = original_result_type

        assert stored is not None
        self.assertEqual(stored.outcome, "CANDIDATES")
        self.assertEqual(
            tuple((candidate.symbol, candidate.role) for candidate in stored.candidates),
            (("SPY", "PRIMARY"),),
        )


if __name__ == "__main__":
    unittest.main()
