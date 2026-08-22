"""Integration tests for close-wake selection and durable deduplication."""

from __future__ import annotations

import copy
import unittest
import sqlite3
import hashlib
import stock_monitor.workflows as workflows_module
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

from stock_monitor.journal import Journal, stable_report_id
from stock_monitor.reports import (
    PremarketState,
    Report,
    is_issued_report,
    render_premarket_report,
)
from stock_monitor.scheduled import JournalScheduledRunStore, RunKind, run_scheduled
from stock_monitor.workflows import (
    JournalWorkflowPublisher,
    PublishedWorkflow,
    RecordedScenarioAdapter,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    run_close,
    run_premarket,
)


SCENARIOS = Path(__file__).parents[1] / "fixtures" / "scenarios"
ET = ZoneInfo("America/New_York")


class _ReprSpoofSequence:
    def __init__(self, original_repr: str, values: tuple[object, ...]) -> None:
        self.original_repr = original_repr
        self.values = values

    def __repr__(self) -> str:
        return self.original_repr

    def __iter__(self):
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __bool__(self) -> bool:
        return bool(self.values)


class _AlwaysEqual:
    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False

    def __repr__(self) -> str:
        return "CANDIDATES"


class _SameLayoutAdapter(RecordedScenarioAdapter):
    __slots__ = ()

class ScheduledTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self._isolated_context_sequence = 0
        self.journal = Journal.open(Path(self.temporary.name) / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.journal.migrate()

    @staticmethod
    def _seed_scheduled_run(
        journal: Journal,
        *,
        run_key: str,
        run_kind: str,
        intended_at: datetime,
    ) -> int:
        stored = intended_at.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        cursor = journal._connection.execute(
            "INSERT INTO scheduled_runs("
            "run_key, run_kind, session_date, intended_run_at, started_at"
            ") VALUES (?, ?, ?, ?, ?)",
            (
                run_key,
                run_kind,
                intended_at.date().isoformat(),
                stored,
                stored,
            ),
        )
        return int(cursor.lastrowid)

    def test_workflow_publisher_requires_an_explicit_archive_root(self):
        with self.assertRaises(TypeError):
            JournalWorkflowPublisher(self.journal)

    def test_publisher_rejects_a_caller_built_report_before_claiming(self):
        observed_at = datetime.now(timezone.utc)
        observation_id, _ = self.journal.append_source_observation(
            payload=b"forged report evidence",
            source_uri="fixture://scheduled/forged-report",
            source_type="RECORDED_SCENARIO",
            provider="FIXTURE",
            feed="RECORDED_SCENARIO",
            source_time=observed_at,
            retrieved_at=observed_at,
            provider_sequence=None,
            delay_seconds=0,
            health_result="OK",
        )
        row = self.journal._connection.execute(
            "SELECT observation_sha256 FROM source_observations WHERE id = ?",
            (observation_id,),
        ).fetchone()
        assert row is not None
        observation_sha256 = str(row[0])
        body = "# Caller-built candidate report\n"
        report = Report(
            report_id=stable_report_id(
                "PREMARKET",
                date(2026, 8, 14),
                (observation_sha256,),
                "a" * 64,
            ),
            kind="PREMARKET",
            session_date=date(2026, 8, 14),
            outcome="CANDIDATES",
            body=body,
            content_sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            observation_ids=(observation_sha256,),
            state_hash="a" * 64,
        )
        result = WorkflowResult(
            outcome="CANDIDATES",
            message=body,
            exit_code=0,
            reason_codes=("FIXTURE",),
            report=report,
            source_observation_row_ids=(observation_id,),
            execution_mode="FIXTURE",
        )
        archive_root = Path(self.temporary.name) / "forged-archive"

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            JournalWorkflowPublisher(self.journal, archive_root).publish(
                kind="PREMARKET",
                session_date=date(2026, 8, 14),
                generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                result=result,
            )

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(tuple(archive_root.rglob("*.md")), ())

    def test_publisher_rejects_copied_or_mutated_renderer_reports(self):
        observed_at = datetime.now(timezone.utc)
        observation_id, _ = self.journal.append_source_observation(
            payload=b"renderer report evidence",
            source_uri="fixture://scheduled/renderer-report",
            source_type="RECORDED_SCENARIO",
            provider="FIXTURE",
            feed="RECORDED_SCENARIO",
            source_time=observed_at,
            retrieved_at=observed_at,
            provider_sequence=None,
            delay_seconds=0,
            health_result="OK",
        )
        row = self.journal._connection.execute(
            "SELECT observation_sha256 FROM source_observations WHERE id = ?",
            (observation_id,),
        ).fetchone()
        assert row is not None
        observation_sha256 = str(row[0])

        def rendered() -> Report:
            return render_premarket_report(
                PremarketState(
                    session_date=date(2026, 8, 14),
                    generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                    outcome="NO TRADE",
                    reason_codes=("FIXTURE", "NO_CANDIDATES"),
                    observation_ids=(observation_sha256,),
                    state_hash="b" * 64,
                )
            )

        original = rendered()
        mutated = rendered()
        object.__setattr__(mutated, "body", mutated.body + "tampered\n")
        candidates = (copy.copy(original), replace(original), mutated)
        archive_root = Path(self.temporary.name) / "copied-archive"
        publisher = JournalWorkflowPublisher(self.journal, archive_root)
        for report in candidates:
            with self.subTest(report=report):
                result = WorkflowResult(
                    outcome="NO_TRADE",
                    message=report.body,
                    exit_code=0,
                    reason_codes=("FIXTURE", "NO_CANDIDATES"),
                    report=report,
                    source_observation_row_ids=(observation_id,),
                    execution_mode="FIXTURE",
                )
                with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
                    publisher.publish(
                        kind="PREMARKET",
                        session_date=date(2026, 8, 14),
                        generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                        result=result,
                    )
                self.assertEqual(self.journal.count("report_claims"), 0)
                self.assertEqual(self.journal.count("reports"), 0)
                self.assertEqual(self.journal.count("outbox"), 0)
                self.assertEqual(tuple(archive_root.rglob("*.md")), ())

    def test_public_renderer_cannot_mint_a_canonical_workflow_publication(self):
        adapter = RecordedScenarioAdapter.load(SCENARIOS / "eligible.json")
        observed_at = adapter.now
        observation_id, _ = self.journal.append_source_observation(
            payload=adapter.fixture_payload,
            source_uri="fixture://scheduled/public-renderer-bypass",
            source_type="RECORDED_SCENARIO",
            provider="FIXTURE",
            feed="RECORDED_SCENARIO",
            source_time=observed_at,
            retrieved_at=observed_at,
            provider_sequence=None,
            delay_seconds=0,
            health_result="OK",
        )
        row = self.journal._connection.execute(
            "SELECT observation_sha256 FROM source_observations WHERE id = ?",
            (observation_id,),
        ).fetchone()
        assert row is not None
        material = adapter.candidates[0].material
        assert material is not None
        report = render_premarket_report(
            PremarketState(
                session_date=date(2026, 8, 14),
                generated_at=adapter.now,
                outcome="CANDIDATES",
                reason_codes=(
                    "PAPER_PLAN_ONLY",
                    "MANUAL_EXECUTION_REQUIRED",
                ),
                observation_ids=(str(row[0]),),
                state_hash=adapter.evidence.state_hash,
                candidates=(material,),
            )
        )
        result = WorkflowResult(
            outcome="CANDIDATES",
            message=report.body,
            exit_code=0,
            reason_codes=("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED"),
            candidates=adapter.candidates,
            report=report,
            source_observation_row_ids=(observation_id,),
            execution_mode="CANONICAL",
        )
        archive_root = Path(self.temporary.name) / "canonical-bypass"

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            JournalWorkflowPublisher(self.journal, archive_root).publish(
                kind="PREMARKET",
                session_date=date(2026, 8, 14),
                generated_at=adapter.now,
                result=result,
            )

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(tuple(archive_root.rglob("*.md")), ())

    def test_workflow_publisher_pins_evidence_and_returns_existing_archive(self):
        context = self.context("no-candidates.json")

        published = run_premarket(context)

        self.assertIsNotNone(published.report_id)
        assert published.report_id is not None
        stored = self.journal.read_report(published.report_id)
        self.assertEqual(stored.observation_ids, published.source_observation_row_ids)
        assert published.report_path is not None
        path = Path(published.report_path)
        self.assertTrue(path.is_file())
        self.assertEqual(path.read_text(encoding="utf-8"), published.message)

        path.unlink()
        retried = run_premarket(context)

        self.assertEqual(retried.outcome, "ALREADY_EMITTED_NOOP")
        assert retried.report_path is not None
        healed = Path(retried.report_path)
        self.assertTrue(healed.is_file())
        self.assertEqual(healed.read_text(encoding="utf-8"), published.message)

    def test_copied_bound_adapter_cannot_issue_a_durable_workflow_result(self):
        context = self.context("eligible.json")
        copied_context = replace(context, adapter=replace(context.adapter))

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            run_premarket(copied_context)

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_wrong_fixture_archive_root_cannot_issue_before_claim(self):
        context = self.context("eligible.json")
        wrong_root = self.journal.path.parent / "wrong-root"
        wrong_context = replace(
            context,
            publisher=JournalWorkflowPublisher(self.journal, wrong_root),
        )

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            run_premarket(wrong_context)

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(tuple(wrong_root.rglob("*.md")), ())

    def test_adapter_candidate_repr_spoof_cannot_change_published_shares(self):
        context = self.context("eligible.json")
        adapter = context.adapter
        assert isinstance(adapter, RecordedScenarioAdapter)
        original = adapter.candidates[0]
        assert original.material is not None
        forged = replace(
            original,
            material=replace(
                original.material,
                shares=original.material.shares + 7,
            ),
        )
        object.__setattr__(
            adapter,
            "candidates",
            _ReprSpoofSequence(repr(adapter.candidates), (forged,)),
        )

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            run_premarket(context)

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_adapter_scalar_equality_spoof_cannot_bypass_missing_configuration(self):
        context = self.context("missing-configuration.json")
        adapter = context.adapter
        assert isinstance(adapter, RecordedScenarioAdapter)
        object.__setattr__(adapter, "configuration", _AlwaysEqual())

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            run_premarket(context)

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_adapter_subclass_cannot_seed_none_authority_then_swap_to_base(self):
        with self.assertRaisesRegex(WorkflowError, "issued|exact|workflow-issued"):
            adapter = _SameLayoutAdapter.load(
                SCENARIOS / "missing-configuration.json"
            )
            fixture_root = (
                Path(self.temporary.name)
                / ".stock-monitor"
                / "fixtures"
                / adapter.evidence.state_hash
            )
            self.journal.close()
            self.journal = Journal.open(fixture_root / "journal.sqlite3")
            self.addCleanup(self.journal.close)
            self.journal.migrate()
            adapter = adapter.bind_source_observation(self.journal)
            object.__setattr__(adapter, "__class__", RecordedScenarioAdapter)
            object.__setattr__(adapter, "configuration", _AlwaysEqual())
            run_premarket(
                WorkflowContext(
                    adapter=adapter,
                    publisher=JournalWorkflowPublisher(
                        self.journal,
                        fixture_root,
                    ),
                    scheduler=None,
                    now=adapter.now,
                )
            )

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_adapter_bind_rejects_a_same_layout_subclass(self):
        adapter = RecordedScenarioAdapter.load(SCENARIOS / "eligible.json")
        object.__setattr__(adapter, "__class__", _SameLayoutAdapter)

        with self.assertRaisesRegex(WorkflowError, "issued|exact"):
            adapter.bind_source_observation(self.journal)

        self.assertEqual(self.journal.count("source_observations"), 0)

    def test_bind_trace_mutation_cannot_flip_missing_configuration_to_ready(self):
        adapter = RecordedScenarioAdapter.load(
            SCENARIOS / "missing-configuration.json"
        )
        fixture_root = (
            Path(self.temporary.name)
            / ".stock-monitor"
            / "fixtures"
            / adapter.evidence.state_hash
        )
        self.journal.close()
        self.journal = Journal.open(fixture_root / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.journal.migrate()
        trace_fired = False

        def mutate_during_insert(statement: str) -> None:
            nonlocal trace_fired
            if not trace_fired and "INSERT INTO source_observations" in statement:
                trace_fired = True
                object.__setattr__(adapter, "configuration", "READY")

        self.journal._connection.set_trace_callback(mutate_during_insert)
        try:
            bound = adapter.bind_source_observation(self.journal)
        finally:
            self.journal._connection.set_trace_callback(None)

        result = run_premarket(
            WorkflowContext(
                adapter=bound,
                publisher=JournalWorkflowPublisher(self.journal, fixture_root),
                scheduler=None,
                now=bound.now,
            )
        )

        self.assertTrue(trace_fired)
        self.assertEqual(bound.configuration, "MISSING")
        self.assertEqual(result.outcome, "CONFIGURATION_REQUIRED")
        self.assertEqual(result.exit_code, 2)
        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_bind_trace_mutation_cannot_inject_candidate_material(self):
        adapter = RecordedScenarioAdapter.load(
            SCENARIOS / "missing-configuration.json"
        )
        forged_source = RecordedScenarioAdapter.load(SCENARIOS / "eligible.json")
        fixture_root = (
            Path(self.temporary.name)
            / ".stock-monitor"
            / "fixtures"
            / adapter.evidence.state_hash
        )
        self.journal.close()
        self.journal = Journal.open(fixture_root / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.journal.migrate()
        trace_fired = False

        def mutate_during_insert(statement: str) -> None:
            nonlocal trace_fired
            if not trace_fired and "INSERT INTO source_observations" in statement:
                trace_fired = True
                object.__setattr__(adapter, "configuration", "READY")
                object.__setattr__(adapter, "candidates", forged_source.candidates)

        self.journal._connection.set_trace_callback(mutate_during_insert)
        try:
            bound = adapter.bind_source_observation(self.journal)
        finally:
            self.journal._connection.set_trace_callback(None)

        result = run_premarket(
            WorkflowContext(
                adapter=bound,
                publisher=JournalWorkflowPublisher(self.journal, fixture_root),
                scheduler=None,
                now=bound.now,
            )
        )

        self.assertTrue(trace_fired)
        self.assertEqual(bound.configuration, "MISSING")
        self.assertEqual(bound.candidates, ())
        self.assertEqual(result.outcome, "CONFIGURATION_REQUIRED")
        self.assertEqual(result.exit_code, 2)
        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_adapter_close_position_repr_spoof_cannot_change_published_shares(self):
        context = self.context("normal-close.json")
        adapter = context.adapter
        assert isinstance(adapter, RecordedScenarioAdapter)
        original = adapter.close_positions[0]
        forged = replace(original, shares=original.shares + 7)
        object.__setattr__(
            adapter,
            "close_positions",
            _ReprSpoofSequence(repr(adapter.close_positions), (forged,)),
        )

        with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
            run_close(context)

        self.assertEqual(self.journal.count("report_claims"), 0)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_issued_result_candidate_spoof_copy_and_cycle_cannot_publish(self):
        context = self.context("eligible.json")
        captured: list[WorkflowResult] = []

        def capture(
            _publisher: JournalWorkflowPublisher,
            *,
            kind: str,
            session_date: date,
            generated_at: datetime,
            result: WorkflowResult,
        ) -> PublishedWorkflow:
            del kind, session_date, generated_at
            captured.append(result)
            return PublishedWorkflow("a" * 64, 1, "captured.md")

        with patch.object(
            JournalWorkflowPublisher,
            "publish",
            autospec=True,
            side_effect=capture,
        ):
            run_premarket(context)
        issued = captured[0]
        copied = copy.copy(issued)
        scalar_spoof = replace(issued)
        object.__setattr__(scalar_spoof, "outcome", _AlwaysEqual())
        original = issued.candidates[0]
        assert original.material is not None
        forged = replace(
            original,
            material=replace(
                original.material,
                shares=original.material.shares + 7,
            ),
        )
        object.__setattr__(
            issued,
            "candidates",
            _ReprSpoofSequence(repr(issued.candidates), (forged,)),
        )
        cyclic = replace(original)
        object.__setattr__(cyclic, "material", cyclic)

        for candidate_result in (
            copied,
            scalar_spoof,
            issued,
            replace(issued, candidates=(cyclic,)),
        ):
            with self.subTest(candidate_result=candidate_result):
                with self.assertRaisesRegex(WorkflowError, "workflow-issued"):
                    assert context.publisher is not None
                    context.publisher.publish(
                        kind="PREMARKET",
                        session_date=date(2026, 8, 14),
                        generated_at=context.now,
                        result=candidate_result,
                    )
                self.assertEqual(self.journal.count("report_claims"), 0)
                self.assertEqual(self.journal.count("reports"), 0)
                self.assertEqual(self.journal.count("outbox"), 0)

    def test_report_verifier_rejects_scalar_equality_spoof(self):
        report = render_premarket_report(
            PremarketState(
                session_date=date(2026, 8, 14),
                generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                observation_ids=("a" * 64,),
                state_hash="b" * 64,
            )
        )
        object.__setattr__(report, "body", _AlwaysEqual())

        self.assertFalse(is_issued_report(report))

    def context(
        self,
        fixture: str,
        *,
        state_root: Path | None = None,
    ) -> WorkflowContext:
        adapter = RecordedScenarioAdapter.load(SCENARIOS / fixture)
        root = Path(self.temporary.name) if state_root is None else state_root
        fixture_root = (
            root
            / ".stock-monitor"
            / "fixtures"
            / adapter.evidence.state_hash
        )
        self.journal.close()
        self.journal = Journal.open(fixture_root / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.journal.migrate()
        adapter = adapter.bind_source_observation(self.journal)
        return WorkflowContext(
            adapter=adapter,
            publisher=JournalWorkflowPublisher(
                self.journal, fixture_root
            ),
            scheduler=JournalScheduledRunStore(self.journal),
            now=adapter.now,
        )

    def isolated_context(self, fixture: str) -> WorkflowContext:
        self._isolated_context_sequence += 1
        state_root = (
            Path(self.temporary.name)
            / f"scheduled-boundary-{self._isolated_context_sequence}"
        )
        return self.context(fixture, state_root=state_root)

    def run_at(self, wall_clock: str) -> WorkflowResult:
        return run_scheduled(
            RunKind.PREMARKET,
            datetime.fromisoformat(f"2026-08-14T{wall_clock}-04:00"),
            self.isolated_context("eligible.json"),
        )

    def run_close_at(self, wall_clock: str) -> WorkflowResult:
        return run_scheduled(
            RunKind.CLOSE,
            datetime.fromisoformat(f"2026-08-14T{wall_clock}-04:00"),
            self.isolated_context("normal-close.json"),
        )

    def run_early_close_at(self, wall_clock: str) -> WorkflowResult:
        return run_scheduled(
            RunKind.CLOSE,
            datetime.fromisoformat(f"2025-11-28T{wall_clock}-05:00"),
            self.isolated_context("early-close.json"),
        )

    def test_premarket_wake_is_exactly_0845_eastern_for_utc_caller(self):
        context = self.context("eligible.json")

        result = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 12, 45, tzinfo=timezone.utc),
            context,
        )

        self.assertEqual(result.outcome, "CANDIDATES")
        self.assertEqual(self.journal.count("scheduled_runs"), 1)
        self.assertEqual(self.journal.count("reports"), 1)

    def test_premarket_due_window_is_half_open_fifteen_minutes(self):
        self.assertEqual(self.run_at("08:45:00").outcome, "CANDIDATES")
        self.assertEqual(self.run_at("08:59:59").outcome, "CANDIDATES")
        self.assertEqual(self.run_at("09:00:00").outcome, "MISSED_RUN_NOOP")

    def test_premarket_due_window_has_exact_microsecond_boundaries(self):
        before = self.run_at("08:44:59.999999")
        self.assertEqual(before.outcome, "NOT_DUE_NOOP")
        self.assertEqual(self.journal.count("scheduled_runs"), 0)

        self.assertEqual(self.run_at("08:45:00.000000").outcome, "CANDIDATES")
        self.assertEqual(self.run_at("08:59:59.999999").outcome, "CANDIDATES")
        self.assertEqual(self.run_at("09:00:00.000000").outcome, "MISSED_RUN_NOOP")

    def test_premarket_upper_boundary_is_missed_and_never_backfilled(self):
        context = self.context("eligible.json")

        result = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 9, 0, tzinfo=ET),
            context,
        )
        same_day_replay = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 16, 0, tzinfo=ET),
            context,
        )
        next_day_probe = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 15, 8, 44, 59, 999999, tzinfo=ET),
            context,
        )

        self.assertEqual(result.outcome, "MISSED_RUN_NOOP")
        self.assertEqual(same_day_replay.outcome, "ALREADY_COMPLETED_NOOP")
        self.assertEqual(next_day_probe.outcome, "NOT_DUE_NOOP")
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

    def test_delayed_premarket_wake_retains_durable_duplicate_semantics(self):
        context = self.context("eligible.json")

        first = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 59, 59, 999999, tzinfo=ET),
            context,
        )
        duplicate = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 9, 0, tzinfo=ET),
            context,
        )

        self.assertEqual(first.outcome, "CANDIDATES")
        self.assertEqual(duplicate.outcome, "ALREADY_EMITTED_NOOP")
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

    def test_journal_scheduled_status_reader_reports_an_unfinished_claim(self):
        intended_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        run_key = "stock-monitor:2026-08-14:PREMARKET"
        self._seed_scheduled_run(
            self.journal,
            run_key=run_key,
            run_kind="PREMARKET",
            intended_at=intended_at,
        )

        reader = getattr(self.journal, "read_scheduled_run_status", None)
        self.assertTrue(callable(reader), "Journal scheduled status reader is required")
        assert reader is not None
        self.assertEqual(
            reader(
                run_key=run_key,
                run_kind="PREMARKET",
                session_date=intended_at.date(),
            ),
            "IN_PROGRESS",
        )

    def test_scheduled_completion_snapshots_result_before_sql_callbacks(self):
        context = self.context("eligible.json")
        scheduler = context.scheduler
        assert isinstance(scheduler, JournalScheduledRunStore)
        intended_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        original_replace = workflows_module.replace
        captured: list[WorkflowResult] = []
        honest_message: list[str] = []
        trace_fired = False

        def capture_result(value: object, *args: object, **kwargs: object) -> object:
            replaced = original_replace(value, *args, **kwargs)
            if type(replaced) is WorkflowResult and replaced.report_id is not None:
                captured.append(replaced)
                honest_message.append(replaced.message)
            return replaced

        def mutate_after_snapshot(statement: str) -> None:
            nonlocal trace_fired
            if not trace_fired and captured and statement.startswith("BEGIN"):
                trace_fired = True
                object.__setattr__(captured[0], "message", "FORGED")

        self.journal._connection.set_trace_callback(mutate_after_snapshot)
        denied_before_claim = False
        try:
            with patch.object(
                workflows_module,
                "replace",
                side_effect=capture_result,
            ):
                try:
                    run_scheduled(RunKind.PREMARKET, intended_at, context)
                except WorkflowError:
                    denied_before_claim = True
        finally:
            self.journal._connection.set_trace_callback(None)

        if denied_before_claim:
            self.assertFalse(trace_fired)
            self.assertEqual(captured, [])
            self.assertEqual(self.journal.count("scheduled_runs"), 0)
            return
        self.assertTrue(trace_fired)
        self.assertEqual(captured[0].message, "FORGED")
        stored = scheduler.result(kind="PREMARKET", session_date=intended_at.date())
        assert stored is not None
        self.assertEqual(stored.message, honest_message[0])

    def test_crashed_schedule_claim_without_report_is_nonzero_and_not_backfilled(self):
        context = self.context("eligible.json")
        assert context.scheduler is not None
        intended_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        self._seed_scheduled_run(
            self.journal,
            run_key="stock-monitor:2026-08-14:PREMARKET",
            run_kind="PREMARKET",
            intended_at=intended_at,
        )

        result = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 46, tzinfo=ET),
            context,
        )

        self.assertEqual(result.outcome, "SCHEDULE_INCOMPLETE")
        self.assertEqual(result.exit_code, 10)
        self.assertEqual(result.reason_codes, ("SCHEDULE_CLAIM_INCOMPLETE",))
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

    def test_in_progress_report_claim_is_truthful_nonzero_not_already_emitted(self):
        context = self.context("eligible.json")
        self.journal.claim_report(date(2026, 8, 14), "PREMARKET")

        result = run_premarket(context)

        self.assertEqual(result.outcome, "PUBLICATION_INCOMPLETE")
        self.assertEqual(result.exit_code, 10)
        self.assertEqual(result.reason_codes, ("PUBLICATION_IN_PROGRESS",))
        self.assertEqual(result.execution_mode, "FIXTURE")
        self.assertIsNone(result.report_path)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_publication_incomplete_keeps_schedule_claim_unfinished(self):
        context = self.context("eligible.json")
        self.journal.claim_report(date(2026, 8, 14), "PREMARKET")

        first = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            context,
        )
        second = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 46, tzinfo=ET),
            context,
        )

        self.assertEqual(first.outcome, "PUBLICATION_INCOMPLETE")
        self.assertEqual(first.exit_code, 10)
        self.assertEqual(second.outcome, "SCHEDULE_INCOMPLETE")
        self.assertEqual(second.exit_code, 10)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_finalized_report_retry_stays_fail_closed_without_backfill(self):
        context = self.context("eligible.json")
        assert context.scheduler is not None
        intended_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        self._seed_scheduled_run(
            self.journal,
            run_key="stock-monitor:2026-08-14:PREMARKET",
            run_kind="PREMARKET",
            intended_at=intended_at,
        )
        published = run_premarket(replace(context, scheduler=None))
        assert published.report_path is not None
        Path(published.report_path).unlink()

        retried = run_scheduled(
            RunKind.PREMARKET,
            datetime(2026, 8, 14, 8, 46, tzinfo=ET),
            context,
        )

        self.assertEqual(retried.outcome, "SCHEDULE_INCOMPLETE")
        self.assertEqual(retried.exit_code, 10)
        self.assertFalse(Path(published.report_path).exists())
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET", session_date=intended_at.date()
            ),
            "REPORT_FINALIZED",
        )

    def test_early_close_emits_once_across_both_wakes(self):
        context = self.context("early-close.json")

        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2025, 11, 28, 12, 30, tzinfo=ET),
            context,
        )
        second = run_scheduled(
            RunKind.CLOSE,
            datetime(2025, 11, 28, 15, 30, tzinfo=ET),
            context,
        )

        self.assertEqual(first.outcome, "EMITTED")
        self.assertEqual(second.outcome, "ALREADY_EMITTED_NOOP")
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

    def test_close_completion_persists_the_final_outward_result(self):
        context = self.context("normal-close.json")
        assert context.scheduler is not None
        outward = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        stored = context.scheduler.result(
            kind="CLOSE",
            session_date=date(2026, 8, 14),
        )

        assert stored is not None
        self.assertEqual(
            (
                stored.outcome,
                stored.message,
                stored.exit_code,
                stored.reason_codes,
                stored.execution_mode,
                stored.report_id,
                stored.report_row_id,
            ),
            (
                outward.outcome,
                outward.message,
                outward.exit_code,
                outward.reason_codes,
                outward.execution_mode,
                outward.report_id,
                outward.report_row_id,
            ),
        )
        assert stored.report_path is not None
        assert outward.report_path is not None
        self.assertTrue(outward.report_path.endswith(stored.report_path))
        self.assertEqual(stored.outcome, "EMITTED")
        self.assertEqual(stored.reason_codes[0], "SCHEDULED_EMITTED")

    def test_normal_close_ignores_early_wake_then_emits(self):
        context = self.context("normal-close.json")

        early = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 12, 30, tzinfo=ET),
            context,
        )
        due = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )

        self.assertEqual(early.outcome, "NOT_DUE_NOOP")
        self.assertEqual(due.outcome, "EMITTED")
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

    def test_regular_close_due_window_is_half_open_fifteen_minutes(self):
        before = self.run_close_at("15:29:59.999999")
        self.assertEqual(before.outcome, "NOT_DUE_NOOP")
        self.assertEqual(self.journal.count("scheduled_runs"), 0)

        self.assertEqual(self.run_close_at("15:30:00.000000").outcome, "EMITTED")
        self.assertEqual(self.run_close_at("15:44:59.999999").outcome, "EMITTED")
        self.assertEqual(
            self.run_close_at("15:45:00.000000").outcome,
            "MISSED_RUN_NOOP",
        )

    def test_early_close_due_window_is_half_open_fifteen_minutes(self):
        before = self.run_early_close_at("12:29:59.999999")
        self.assertEqual(before.outcome, "NOT_DUE_NOOP")
        self.assertEqual(self.journal.count("scheduled_runs"), 0)

        self.assertEqual(
            self.run_early_close_at("12:30:00.000000").outcome,
            "EMITTED",
        )
        self.assertEqual(
            self.run_early_close_at("12:44:59.999999").outcome,
            "EMITTED",
        )
        self.assertEqual(
            self.run_early_close_at("12:45:00.000000").outcome,
            "MISSED_RUN_NOOP",
        )

    def test_close_uses_nominal_review_time_not_dispatch_time(self):
        result = self.run_close_at("15:44:59")

        self.assertEqual(result.outcome, "EMITTED")
        nominal_timestamp = "- Generated at: `2026-08-14T15:30:00-04:00`"
        dispatch_timestamp = "2026-08-14T15:44:59-04:00"
        self.assertIn(nominal_timestamp, result.message)
        self.assertNotIn(dispatch_timestamp, result.message)
        assert result.report_path is not None
        archived = Path(result.report_path).read_text(encoding="utf-8")
        self.assertIn(nominal_timestamp, archived)
        self.assertNotIn(dispatch_timestamp, archived)

    def test_due_windows_convert_utc_across_daylight_and_standard_time(self):
        summer = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 19, 44, 59, 999999, tzinfo=timezone.utc),
            self.isolated_context("normal-close.json"),
        )
        winter = run_scheduled(
            RunKind.CLOSE,
            datetime(2025, 11, 28, 17, 44, 59, 999999, tzinfo=timezone.utc),
            self.isolated_context("early-close.json"),
        )

        self.assertEqual(summer.outcome, "EMITTED")
        self.assertEqual(winter.outcome, "EMITTED")

    def test_missed_close_is_recorded_but_never_backfilled(self):
        context = self.context("normal-close.json")

        result = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 45, tzinfo=ET),
            context,
        )

        self.assertEqual(result.outcome, "MISSED_RUN_NOOP")
        self.assertEqual(result.candidates, ())
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(self.journal.count("scheduled_runs"), 1)

        replay = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 46, tzinfo=ET),
            context,
        )
        self.assertEqual(replay.outcome, "ALREADY_COMPLETED_NOOP")
        self.assertEqual(replay.reason_codes, ("ALREADY_COMPLETED_NO_REPORT",))
        self.assertEqual(self.journal.count("reports"), 0)

    def test_failed_provider_check_emits_only_fail_closed_report(self):
        context = self.context("close-provider-failure.json")

        result = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 44, 59, 999999, tzinfo=ET),
            context,
        )

        self.assertEqual(result.outcome, "EMITTED")
        self.assertEqual(result.exit_code, 3)
        self.assertEqual(result.candidates, ())
        self.assertNotIn("PRIMARY", result.message)
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)

    def test_finalized_provider_failure_retry_preserves_failure_truth(self):
        context = self.context("close-provider-failure.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        assert first.report_path is not None
        Path(first.report_path).unlink()

        retried = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 31, tzinfo=ET),
            context,
        )

        self.assertEqual(retried.outcome, first.outcome)
        self.assertEqual(retried.exit_code, first.exit_code)
        self.assertEqual(retried.reason_codes, first.reason_codes)
        self.assertEqual(retried.report_id, first.report_id)
        self.assertEqual(retried.report_row_id, first.report_row_id)
        self.assertEqual(retried.report_path, first.report_path)
        self.assertTrue(Path(retried.report_path).is_file())
        self.assertEqual(self.journal.count("reports"), 1)
        self.assertEqual(self.journal.count("outbox"), 1)

    def test_completed_report_retry_requires_exact_healed_identity(self):
        context = self.context("close-provider-failure.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )
        assert context.publisher is not None
        assert first.report_row_id is not None
        assert first.report_path is not None
        forged = PublishedWorkflow(
            report_id="f" * 64,
            report_row_id=first.report_row_id,
            report_path=first.report_path,
            status="ALREADY_EMITTED",
        )

        with patch.object(
            context.publisher,
            "heal_finalized",
            return_value=forged,
        ):
            retried = run_scheduled(
                RunKind.CLOSE,
                datetime(2026, 8, 14, 15, 31, tzinfo=ET),
                context,
            )

        self.assertEqual(retried.outcome, "SCHEDULE_INCOMPLETE")
        self.assertEqual(retried.exit_code, 10)
        self.assertEqual(retried.reason_codes, ("SCHEDULE_RESULT_UNAVAILABLE",))

    def test_scheduled_close_without_a_report_never_claims_emitted(self):
        context = self.context("missing-configuration.json")

        result = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 44, 59, 999999, tzinfo=ET),
            context,
        )

        self.assertEqual(result.outcome, "CONFIGURATION_REQUIRED")
        self.assertEqual(result.exit_code, 2)
        self.assertEqual(result.candidates, ())
        self.assertIsNone(result.report_id)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_missing_configuration_retry_never_becomes_a_green_noop(self):
        context = self.context("missing-configuration.json")
        first = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
            context,
        )

        retried = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 31, tzinfo=ET),
            context,
        )

        self.assertEqual(retried.outcome, first.outcome)
        self.assertEqual(retried.exit_code, first.exit_code)
        self.assertEqual(retried.reason_codes, first.reason_codes)
        self.assertEqual(retried.exit_code, 2)
        self.assertIsNone(retried.report_id)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)

    def test_malformed_stored_result_never_becomes_a_green_noop(self):
        context = self.context("missing-configuration.json")
        assert context.scheduler is not None
        intended_at = datetime(2026, 8, 14, 15, 30, tzinfo=ET)
        self._seed_scheduled_run(
            self.journal,
            run_key="stock-monitor:2026-08-14:CLOSE",
            run_kind="CLOSE",
            intended_at=intended_at,
        )
        stored_json = "{}"
        self.journal._connection.execute(
            "UPDATE scheduled_runs SET finished_at = ?, "
            "market_session_decision = 'DUE_WAKE', outcome = 'NOOP', "
            "result_envelope_json = ?, result_envelope_sha256 = ?",
            (
                "2026-08-14T19:30:01.000000Z",
                stored_json,
                hashlib.sha256(stored_json.encode("utf-8")).hexdigest(),
            ),
        )

        retried = run_scheduled(
            RunKind.CLOSE,
            datetime(2026, 8, 14, 15, 31, tzinfo=ET),
            context,
        )

        self.assertEqual(retried.outcome, "SCHEDULE_INCOMPLETE")
        self.assertEqual(retried.exit_code, 10)
        self.assertEqual(retried.reason_codes, ("SCHEDULE_RESULT_UNAVAILABLE",))

    def test_unverified_and_reconciled_close_states_preserve_exit_codes(self):
        expected = {
            "unverified-position.json": 4,
            "normal-close.json": 0,
        }
        for fixture, exit_code in expected.items():
            with self.subTest(fixture=fixture):
                with TemporaryDirectory() as temporary:
                    adapter = RecordedScenarioAdapter.load(SCENARIOS / fixture)
                    fixture_root = (
                        Path(temporary)
                        / ".stock-monitor"
                        / "fixtures"
                        / adapter.evidence.state_hash
                    )
                    journal = Journal.open(fixture_root / "journal.sqlite3")
                    try:
                        journal.migrate()
                        adapter = adapter.bind_source_observation(journal)
                        context = WorkflowContext(
                            adapter=adapter,
                            publisher=JournalWorkflowPublisher(
                                journal, fixture_root
                            ),
                            scheduler=JournalScheduledRunStore(journal),
                            now=adapter.now,
                        )
                        result = run_scheduled(
                            RunKind.CLOSE,
                            datetime(2026, 8, 14, 15, 30, tzinfo=ET),
                            context,
                        )
                    finally:
                        journal.close()
                self.assertEqual(result.outcome, "EMITTED")
                self.assertEqual(result.exit_code, exit_code)

    def test_naive_schedule_time_fails_closed(self):
        context = self.context("normal-close.json")

        with self.assertRaises(ValueError):
            run_scheduled(
                RunKind.CLOSE,
                datetime(2026, 8, 14, 15, 30),
                context,
            )


if __name__ == "__main__":
    unittest.main()
