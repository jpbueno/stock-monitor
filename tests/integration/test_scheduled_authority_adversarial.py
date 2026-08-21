from __future__ import annotations

import hashlib
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import stock_monitor.journal as journal_module
import stock_monitor.reports as reports_module
import stock_monitor.scheduled as scheduled_module
import stock_monitor.workflows as workflows_module
from stock_monitor.journal import (
    Journal,
    JournalError,
    ScheduledRunResultEnvelope,
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
)


SCENARIOS = Path(__file__).parents[1] / "fixtures" / "scenarios"
ET = ZoneInfo("America/New_York")
SESSION_DATE = date(2026, 8, 14)
PREMARKET_WAKE = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
CLOSE_WAKE = datetime(2026, 8, 14, 15, 30, tzinfo=ET)


def _envelope(
    *,
    outcome: str = "CONFIGURATION_REQUIRED",
    message: str = "CONFIGURATION REQUIRED",
    exit_code: int = 2,
    reason_codes: tuple[str, ...] = ("CONFIGURATION_REQUIRED",),
    candidates: tuple[tuple[str, str], ...] = (),
) -> ScheduledRunResultEnvelope:
    return ScheduledRunResultEnvelope(
        outcome=outcome,
        message=message,
        exit_code=exit_code,
        reason_codes=reason_codes,
        execution_mode="FIXTURE",
        candidates=candidates,
        report_id=None,
        report_row_id=None,
        report_path=None,
    )


class ScheduledAuthorityAdversarialTests(unittest.TestCase):
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
    def _seed_completed_result(
        journal: Journal,
        *,
        run_key: str,
        envelope: ScheduledRunResultEnvelope,
        kind: str,
        intended: datetime,
    ) -> None:
        stored_intended = intended.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        cursor = journal._connection.execute(
            "INSERT INTO scheduled_runs("
            "run_key, run_kind, session_date, intended_run_at, started_at"
            ") VALUES (?, ?, ?, ?, ?)",
            (
                run_key,
                kind,
                intended.date().isoformat(),
                stored_intended,
                stored_intended,
            ),
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
                int(cursor.lastrowid),
            ),
        )

    @staticmethod
    def _closure_cells(function: object) -> dict[str, object]:
        code = getattr(function, "__code__", None)
        closure = getattr(function, "__closure__", None)
        if code is None or closure is None:
            return {}
        return dict(zip(code.co_freevars, (cell.cell_contents for cell in closure)))

    def test_journal_first_import_cannot_expose_scheduled_writer_authority(self) -> None:
        script = textwrap.dedent(
            """
            import tempfile
            from datetime import date, datetime, timedelta, timezone
            from pathlib import Path
            from types import SimpleNamespace

            import stock_monitor.journal as journal_module

            namespace = {
                "__name__": "stock_monitor.scheduled",
                "journal_module": journal_module,
            }
            exec(
                "def _install_scheduled_boundary_from_journal():\\n"
                "    return journal_module._take_scheduled_journal_boundary()\\n",
                namespace,
            )
            journal = None
            try:
                boundary = namespace[
                    "_install_scheduled_boundary_from_journal"
                ]()
                configure, claim, complete, _abandon, _heal, read_envelope = boundary

                from stock_monitor.reports import Report, archive_report
                from stock_monitor.workflows import (
                    CandidateSummary,
                    JournalWorkflowPublisher,
                    PublishedWorkflow,
                    WorkflowError,
                    WorkflowResult,
                )

                configure(
                    candidate=CandidateSummary,
                    workflow_result=WorkflowResult,
                    publisher=JournalWorkflowPublisher,
                    published=PublishedWorkflow,
                    report=Report,
                    archive=archive_report,
                    workflow_error=WorkflowError,
                )
                with tempfile.TemporaryDirectory() as temporary:
                    journal = journal_module.Journal.open(
                        Path(temporary) / "journal.sqlite3"
                    )
                    journal.migrate()
                    intended = datetime(
                        2026, 8, 14, 15, 30, tzinfo=timezone.utc
                    )
                    duplicate, authority = claim(
                        SimpleNamespace(journal=journal),
                        kind="CLOSE",
                        session_date=date(2026, 8, 14),
                        intended_at=intended,
                        publisher=None,
                    )
                    if duplicate or authority is None:
                        raise RuntimeError("scheduled authority was not minted")
                    complete(
                        authority,
                        SimpleNamespace(journal=journal),
                        finished_at=intended + timedelta(seconds=1),
                        decision="DUE_WAKE",
                        result=WorkflowResult(
                            outcome="ALREADY_COMPLETED_NOOP",
                            message="FORGED GREEN",
                            exit_code=0,
                            reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                            execution_mode="FIXTURE",
                        ),
                    )
                    stored = read_envelope(
                        journal,
                        run_key="stock-monitor:2026-08-14:CLOSE",
                        run_kind="CLOSE",
                        session_date=date(2026, 8, 14),
                    )
                    print(
                        "EXFILTRATED"
                        if stored is not None and stored.message == "FORGED GREEN"
                        else "DENIED"
                    )
            except Exception as error:
                print("DENIED", type(error).__name__)
            finally:
                if journal is not None:
                    journal.close()
            """
        )
        completed = subprocess.run(
            [sys.executable, "-Werror", "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("DENIED", completed.stdout, completed.stdout)
        self.assertNotIn("EXFILTRATED", completed.stdout, completed.stdout)

    def test_public_run_scheduled_closure_cannot_complete_a_forged_result(self) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        cells = self._closure_cells(scheduled_module.run_scheduled)
        claim = cells.get("claim")
        complete = cells.get("complete")
        if not callable(claim) or not callable(complete):
            return

        try:
            duplicate, authority = claim(
                context.scheduler,
                kind="PREMARKET",
                session_date=SESSION_DATE,
                intended_at=PREMARKET_WAKE,
                publisher=None,
            )
            if duplicate or authority is None:
                return
            complete(
                authority,
                context.scheduler,
                finished_at=PREMARKET_WAKE + timedelta(seconds=1),
                decision="DUE_WAKE",
                result=WorkflowResult(
                    outcome="ALREADY_COMPLETED_NOOP",
                    message="FORGED GREEN",
                    exit_code=0,
                    reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                    execution_mode="FIXTURE",
                ),
            )
        except (JournalError, WorkflowError):
            return

        try:
            stored = context.scheduler.result(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            )
        except JournalError:
            stored = None
        self.assertTrue(
            stored is None or stored.message != "FORGED GREEN",
            "public closure cells exposed a working completion capability",
        )

    def test_nested_decoder_dependencies_cannot_forge_stored_truth(self) -> None:
        journal = self._journal()
        run_key = "stock-monitor:2026-08-14:CLOSE"
        self._seed_completed_result(
            journal,
            run_key=run_key,
            envelope=_envelope(),
            kind="CLOSE",
            intended=CLOSE_WAKE,
        )
        row = journal._connection.execute(
            "SELECT result_envelope_json FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        assert row is not None
        stored_json = str(row[0])
        original_loads = journal_module.json.loads
        original_canonical_json = journal_module._canonical_json
        forged_payload = original_loads(stored_json)
        forged_payload.update(
            {
                "outcome": "ALREADY_COMPLETED_NOOP",
                "message": "FORGED GREEN",
                "exit_code": 0,
                "reason_codes": ["ALREADY_COMPLETED_NO_REPORT"],
            }
        )

        def forged_loads(_value: object) -> object:
            journal_module.json.loads = original_loads
            return forged_payload

        def forged_canonical_json(_value: object) -> str:
            journal_module._canonical_json = original_canonical_json
            return stored_json

        journal_module.json.loads = forged_loads
        journal_module._canonical_json = forged_canonical_json
        try:
            try:
                stored = JournalScheduledRunStore(journal).result(
                    kind="CLOSE",
                    session_date=SESSION_DATE,
                )
            except JournalError:
                return
        finally:
            journal_module.json.loads = original_loads
            journal_module._canonical_json = original_canonical_json

        assert stored is not None
        self.assertEqual(stored.exit_code, 2)
        self.assertEqual(stored.message, "CONFIGURATION REQUIRED")

    def test_nested_json_raw_decode_cannot_forge_store_result_truth(self) -> None:
        journal = self._journal()
        run_key = "stock-monitor:2026-08-14:CLOSE"
        self._seed_completed_result(
            journal,
            run_key=run_key,
            envelope=_envelope(),
            kind="CLOSE",
            intended=CLOSE_WAKE,
        )
        row = journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        assert row is not None
        stored_json = str(row[0])
        stored_sha256 = str(row[1])
        forged_payload = journal_module.json.loads(stored_json)
        forged_payload.update(
            {
                "outcome": "ALREADY_COMPLETED_NOOP",
                "message": "FORGED GREEN",
                "exit_code": 0,
                "reason_codes": ["ALREADY_COMPLETED_NO_REPORT"],
            }
        )
        decoder = journal_module.json._default_decoder
        encoder_type = journal_module.json.JSONEncoder
        original_raw_decode = decoder.raw_decode
        original_iterencode = encoder_type.iterencode

        def forged_iterencode(
            _encoder: object,
            _value: object,
            _one_shot: bool = False,
        ) -> object:
            del _encoder, _value, _one_shot
            encoder_type.iterencode = original_iterencode
            return iter((stored_json,))

        def forged_raw_decode(_value: str, idx: int = 0) -> object:
            del idx
            decoder.raw_decode = original_raw_decode
            encoder_type.iterencode = forged_iterencode
            return forged_payload, len(_value)

        decoder.raw_decode = forged_raw_decode
        try:
            stored = JournalScheduledRunStore(journal).result(
                kind="CLOSE",
                session_date=SESSION_DATE,
            )
        except JournalError:
            stored = None
        finally:
            decoder.raw_decode = original_raw_decode
            encoder_type.iterencode = original_iterencode

        row_after = journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        self.assertIs(decoder.raw_decode, original_raw_decode)
        self.assertIs(encoder_type.iterencode, original_iterencode)
        self.assertEqual(row_after, (stored_json, stored_sha256))
        self.assertTrue(
            stored is None
            or (
                stored.exit_code == 2
                and stored.message == "CONFIGURATION REQUIRED"
            ),
            "nested JSON dispatch forged an unchanged stored result",
        )

    def test_nested_json_raw_decode_cannot_forge_duplicate_run_truth(self) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        first = run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        self.assertEqual(first.exit_code, 2)
        run_key = "stock-monitor:2026-08-14:PREMARKET"
        row = context.scheduler.journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        assert row is not None
        stored_json = str(row[0])
        stored_sha256 = str(row[1])
        forged_payload = journal_module.json.loads(stored_json)
        forged_payload.update(
            {
                "outcome": "ALREADY_COMPLETED_NOOP",
                "message": "FORGED GREEN",
                "exit_code": 0,
                "reason_codes": ["ALREADY_COMPLETED_NO_REPORT"],
            }
        )
        decoder = journal_module.json._default_decoder
        encoder_type = journal_module.json.JSONEncoder
        original_raw_decode = decoder.raw_decode
        original_iterencode = encoder_type.iterencode

        def forged_iterencode(
            _encoder: object,
            _value: object,
            _one_shot: bool = False,
        ) -> object:
            del _encoder, _value, _one_shot
            encoder_type.iterencode = original_iterencode
            return iter((stored_json,))

        def forged_raw_decode(_value: str, idx: int = 0) -> object:
            del idx
            decoder.raw_decode = original_raw_decode
            encoder_type.iterencode = forged_iterencode
            return forged_payload, len(_value)

        decoder.raw_decode = forged_raw_decode
        try:
            try:
                duplicate = run_scheduled(
                    RunKind.PREMARKET,
                    PREMARKET_WAKE + timedelta(minutes=1),
                    context,
                )
            except (JournalError, WorkflowError):
                duplicate = None
        finally:
            decoder.raw_decode = original_raw_decode
            encoder_type.iterencode = original_iterencode

        row_after = context.scheduler.journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        self.assertIs(decoder.raw_decode, original_raw_decode)
        self.assertIs(encoder_type.iterencode, original_iterencode)
        self.assertEqual(row_after, (stored_json, stored_sha256))
        self.assertTrue(
            duplicate is None
            or (
                duplicate.outcome == first.outcome
                and duplicate.exit_code == first.exit_code
                and duplicate.message == first.message
            ),
            "nested JSON dispatch forged duplicate scheduled truth",
        )

    def test_self_restoring_sql_cannot_forge_duplicate_run_truth(self) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        first = run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        self.assertEqual(first.exit_code, 2)
        run_key = "stock-monitor:2026-08-14:PREMARKET"
        durable_before = context.scheduler.journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        assert durable_before is not None
        forged_json, forged_sha256 = (
            journal_module._scheduled_result_envelope_storage(
                _envelope(
                    outcome="ALREADY_COMPLETED_NOOP",
                    message="FORGED GREEN",
                    exit_code=0,
                    reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                )
            )
        )
        original_sql = journal_module._sql
        observed = {"status": False, "result": False}

        class ForgedCursor:
            def __init__(self, row: tuple[object, ...]) -> None:
                self._row = row

            def fetchone(self) -> tuple[object, ...]:
                return self._row

        def forged_sql(
            connection: object,
            statement: str,
            parameters: object = (),
        ) -> object:
            cursor = original_sql(connection, statement, parameters)
            if "claim.status" in statement:
                observed["status"] = True
            if "result_envelope_json" not in statement:
                return cursor
            observed["result"] = True
            row = cursor.fetchone()
            assert row is not None
            forged_row = list(row)
            forged_row[6] = forged_json
            forged_row[7] = forged_sha256
            journal_module._sql = original_sql
            return ForgedCursor(tuple(forged_row))

        journal_module._sql = forged_sql
        try:
            try:
                duplicate = run_scheduled(
                    RunKind.PREMARKET,
                    PREMARKET_WAKE + timedelta(minutes=1),
                    context,
                )
            except (JournalError, WorkflowError):
                duplicate = None
        finally:
            journal_module._sql = original_sql

        durable_after = context.scheduler.journal._connection.execute(
            "SELECT result_envelope_json, result_envelope_sha256 "
            "FROM scheduled_runs WHERE run_key = ?",
            (run_key,),
        ).fetchone()
        self.assertIs(journal_module._sql, original_sql)
        self.assertEqual(durable_after, durable_before)
        self.assertTrue(
            duplicate is None
            or (
                duplicate.outcome == first.outcome
                and duplicate.exit_code == first.exit_code
                and duplicate.message == first.message
            ),
            "mutable SQL readback forged duplicate scheduled truth",
        )

    def test_self_restoring_workflow_code_cannot_persist_forged_results(
        self,
    ) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        compromised: list[str] = []
        cases = (
            (RunKind.PREMARKET, PREMARKET_WAKE, workflows_module.run_premarket),
            (RunKind.CLOSE, CLOSE_WAKE, workflows_module.run_close),
        )
        for kind, wake, workflow in cases:
            original_code = workflow.__code__
            function_globals = workflow.__globals__
            code_key = "_ADVERSARIAL_WORKFLOW_ORIGINAL_CODE"
            target_key = "_ADVERSARIAL_WORKFLOW_TARGET"
            function_globals[code_key] = original_code
            function_globals[target_key] = workflow
            exec(
                "def _adversarial_workflow_once(context):\n"
                "    del context\n"
                "    _ADVERSARIAL_WORKFLOW_TARGET.__code__ = "
                "_ADVERSARIAL_WORKFLOW_ORIGINAL_CODE\n"
                "    return WorkflowResult(\n"
                "        outcome='CANDIDATES',\n"
                "        message='FORGED WORKFLOW',\n"
                "        exit_code=0,\n"
                "        reason_codes=('PAPER_PLAN_ONLY',),\n"
                "        candidates=(CandidateSummary('QQQ', 'PRIMARY'),),\n"
                "        execution_mode='FIXTURE',\n"
                "    )\n",
                function_globals,
            )
            forged = function_globals.pop("_adversarial_workflow_once")
            workflow.__code__ = forged.__code__
            try:
                try:
                    outward = run_scheduled(kind, wake, context)
                except (JournalError, WorkflowError):
                    outward = None
            finally:
                workflow.__code__ = original_code
                function_globals.pop(code_key, None)
                function_globals.pop(target_key, None)
            try:
                stored = context.scheduler.result(
                    kind=kind.value,
                    session_date=SESSION_DATE,
                )
            except JournalError:
                stored = None
            if (
                outward is not None
                and outward.message == "FORGED WORKFLOW"
            ) or (
                stored is not None
                and stored.message == "FORGED WORKFLOW"
            ):
                compromised.append(kind.value)
            self.assertIs(workflow.__code__, original_code)

        self.assertEqual(
            compromised,
            [],
            "self-restoring workflow code persisted forged results",
        )

    def test_self_restoring_report_snapshot_cannot_persist_forged_body(
        self,
    ) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        original_snapshot = workflows_module._issued_report_snapshot
        forged_body = "# FORGED PUBLISHED BODY\n"

        def forged_snapshot(issued_report: object) -> object:
            workflows_module._issued_report_snapshot = original_snapshot
            report = original_snapshot(issued_report)
            assert report is not None
            return replace(
                report,
                body=forged_body,
                content_sha256=hashlib.sha256(
                    forged_body.encode("utf-8")
                ).hexdigest(),
            )

        workflows_module._issued_report_snapshot = forged_snapshot
        try:
            try:
                outward = workflows_module.run_premarket(
                    replace(context, scheduler=None)
                )
            except WorkflowError:
                outward = None
        finally:
            workflows_module._issued_report_snapshot = original_snapshot

        self.assertIs(
            workflows_module._issued_report_snapshot,
            original_snapshot,
        )
        if outward is None or outward.report_id is None:
            return
        stored = context.publisher.journal.read_report(outward.report_id)
        self.assertEqual(
            stored.body,
            outward.message,
            "self-restoring report snapshot persisted forged report bytes",
        )

    def test_completion_select_root_mutation_revokes_scheduled_write(self) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        wrong_root = Path(self.temporary.name) / "select-wrong-root"
        observed = {"fired": False}

        def mutate(statement: str) -> None:
            if (
                statement.startswith(
                    "SELECT run_kind, session_date, started_at, finished_at"
                )
                and not observed["fired"]
            ):
                observed["fired"] = True
                context.publisher.report_archive_root = wrong_root

        connection = context.publisher.journal._connection
        connection.set_trace_callback(mutate)
        try:
            with self.assertRaises((JournalError, WorkflowError)):
                run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        finally:
            connection.set_trace_callback(None)

        self.assertTrue(observed["fired"])
        self.assertIsNone(
            connection.execute(
                "SELECT finished_at FROM scheduled_runs "
                "WHERE run_key = 'stock-monitor:2026-08-14:PREMARKET'"
            ).fetchone()[0]
        )

    def test_completion_select_archive_deletion_revokes_scheduled_write(self) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        archive_root = context.publisher.report_archive_root
        observed = {"fired": False, "path": None}

        def delete_archive(statement: str) -> None:
            if (
                statement.startswith(
                    "SELECT run_kind, session_date, started_at, finished_at"
                )
                and not observed["fired"]
            ):
                paths = tuple(archive_root.rglob("*.md"))
                if paths:
                    observed["fired"] = True
                    observed["path"] = paths[0]
                    paths[0].unlink()

        connection = context.publisher.journal._connection
        connection.set_trace_callback(delete_archive)
        try:
            with self.assertRaises((JournalError, WorkflowError)):
                run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        finally:
            connection.set_trace_callback(None)

        self.assertTrue(observed["fired"])
        self.assertIsNotNone(observed["path"])
        assert isinstance(observed["path"], Path)
        self.assertFalse(observed["path"].exists())
        self.assertIsNone(
            connection.execute(
                "SELECT finished_at FROM scheduled_runs "
                "WHERE run_key = 'stock-monitor:2026-08-14:PREMARKET'"
            ).fetchone()[0]
        )

    def test_completion_commit_archive_deletion_fails_then_duplicate_heals(
        self,
    ) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        archive_root = context.publisher.report_archive_root
        observed = {"armed": False, "fired": False, "path": None}

        def delete_on_completion_commit(statement: str) -> None:
            if statement.startswith("UPDATE scheduled_runs SET finished_at"):
                observed["armed"] = True
            elif statement == "COMMIT" and observed["armed"] and not observed["fired"]:
                paths = tuple(archive_root.rglob("*.md"))
                if paths:
                    observed["fired"] = True
                    observed["path"] = paths[0]
                    paths[0].unlink()

        connection = context.publisher.journal._connection
        connection.set_trace_callback(delete_on_completion_commit)
        try:
            with self.assertRaises((JournalError, WorkflowError)):
                run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        finally:
            connection.set_trace_callback(None)

        self.assertTrue(observed["fired"])
        self.assertIsInstance(observed["path"], Path)
        archive_path = observed["path"]
        assert isinstance(archive_path, Path)
        self.assertFalse(archive_path.exists())
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            ),
            "REPORT_EMITTED",
        )

        retried = run_scheduled(
            RunKind.PREMARKET,
            PREMARKET_WAKE + timedelta(minutes=1),
            context,
        )
        self.assertEqual(retried.outcome, "ALREADY_EMITTED_NOOP")
        self.assertEqual(retried.exit_code, 0)
        self.assertTrue(archive_path.is_file())

    def test_completion_commit_root_mutation_fails_then_duplicate_recovers(
        self,
    ) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        original_root = context.publisher.report_archive_root
        wrong_root = Path(self.temporary.name) / "commit-wrong-root"
        observed = {"armed": False, "fired": False}

        def mutate_on_completion_commit(statement: str) -> None:
            if statement.startswith("UPDATE scheduled_runs SET finished_at"):
                observed["armed"] = True
            elif statement == "COMMIT" and observed["armed"] and not observed["fired"]:
                observed["fired"] = True
                context.publisher.report_archive_root = wrong_root

        connection = context.publisher.journal._connection
        connection.set_trace_callback(mutate_on_completion_commit)
        try:
            with self.assertRaises((JournalError, WorkflowError)):
                run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
        finally:
            connection.set_trace_callback(None)

        self.assertTrue(observed["fired"])
        self.assertIs(context.publisher.report_archive_root, wrong_root)
        self.assertEqual(
            context.scheduler.status(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            ),
            "REPORT_EMITTED",
        )
        context.publisher.report_archive_root = original_root
        retried = run_scheduled(
            RunKind.PREMARKET,
            PREMARKET_WAKE + timedelta(minutes=1),
            context,
        )
        self.assertEqual(retried.outcome, "ALREADY_EMITTED_NOOP")
        self.assertEqual(retried.exit_code, 0)
        assert retried.report_path is not None
        self.assertTrue(Path(retried.report_path).is_file())

    def test_self_restoring_adapter_decision_method_cannot_publish_no_trade(
        self,
    ) -> None:
        context = self._context("eligible.json")
        assert context.scheduler is not None
        adapter_type = workflows_module.RecordedScenarioAdapter
        original_snapshot = adapter_type.premarket_snapshot

        def forged_snapshot(
            adapter: object,
            day: date,
        ) -> object:
            adapter_type.premarket_snapshot = original_snapshot
            snapshot = original_snapshot(adapter, day)
            return workflows_module.PremarketSnapshot(
                candidates=(),
                breaker_active=snapshot.breaker_active,
            )

        adapter_type.premarket_snapshot = forged_snapshot
        try:
            try:
                outward = run_scheduled(
                    RunKind.PREMARKET,
                    PREMARKET_WAKE,
                    context,
                )
            except (JournalError, WorkflowError):
                outward = None
        finally:
            adapter_type.premarket_snapshot = original_snapshot

        try:
            stored = context.scheduler.result(
                kind="PREMARKET",
                session_date=SESSION_DATE,
            )
        except JournalError:
            stored = None
        self.assertIs(adapter_type.premarket_snapshot, original_snapshot)
        self.assertFalse(
            outward is not None
            and outward.outcome == "NO_TRADE"
            and outward.exit_code == 0,
            "self-restoring adapter decision suppressed an honest candidate",
        )
        self.assertFalse(
            stored is not None
            and stored.outcome == "NO_TRADE"
            and stored.exit_code == 0,
            "adapter decision forgery became durable scheduled truth",
        )

    def test_exact_candidate_constructor_cannot_forge_stored_candidates(self) -> None:
        journal = self._journal()
        self._seed_completed_result(
            journal,
            run_key="stock-monitor:2026-08-14:PREMARKET",
            envelope=_envelope(
                outcome="CANDIDATES",
                message="ONE CANDIDATE",
                exit_code=0,
                reason_codes=("PAPER_PLAN_ONLY",),
                candidates=(("SPY", "PRIMARY"),),
            ),
            kind="PREMARKET",
            intended=PREMARKET_WAKE,
        )
        original_init = CandidateSummary.__init__

        def forged_init(self: CandidateSummary, **_values: object) -> None:
            CandidateSummary.__init__ = original_init
            original_init(self, symbol="QQQ", role="SECONDARY")

        CandidateSummary.__init__ = forged_init
        try:
            try:
                stored = JournalScheduledRunStore(journal).result(
                    kind="PREMARKET",
                    session_date=SESSION_DATE,
                )
            except JournalError:
                return
        finally:
            CandidateSummary.__init__ = original_init

        assert stored is not None
        self.assertEqual(
            tuple((candidate.symbol, candidate.role) for candidate in stored.candidates),
            (("SPY", "PRIMARY"),),
        )

    def test_exact_workflow_result_constructor_cannot_persist_forged_green(self) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        original_init = WorkflowResult.__init__

        def forged_init(self: WorkflowResult, *_args: object, **_values: object) -> None:
            WorkflowResult.__init__ = original_init
            original_init(
                self,
                outcome="ALREADY_COMPLETED_NOOP",
                message="FORGED GREEN",
                exit_code=0,
                reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                execution_mode="FIXTURE",
            )

        WorkflowResult.__init__ = forged_init
        try:
            try:
                run_scheduled(RunKind.PREMARKET, PREMARKET_WAKE, context)
            except (JournalError, WorkflowError):
                return
        finally:
            WorkflowResult.__init__ = original_init

        stored = context.scheduler.result(
            kind="PREMARKET",
            session_date=SESSION_DATE,
        )
        self.assertTrue(
            stored is None or stored.message != "FORGED GREEN",
            "a self-restoring result constructor changed durable scheduled truth",
        )

    def test_healer_report_constructor_cannot_archive_a_forged_body(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(RunKind.CLOSE, CLOSE_WAKE, context)
        assert first.report_path is not None
        archive_path = Path(first.report_path)
        archive_path.unlink()
        forged_body = "# FORGED HEALER BODY\n"
        original_init = reports_module.Report.__init__

        def forged_init(
            self: reports_module.Report,
            *_args: object,
            **values: object,
        ) -> None:
            reports_module.Report.__init__ = original_init
            forged_values = dict(values)
            forged_values["body"] = forged_body
            forged_values["content_sha256"] = hashlib.sha256(
                forged_body.encode("utf-8")
            ).hexdigest()
            original_init(self, **forged_values)

        reports_module.Report.__init__ = forged_init
        try:
            try:
                retried = run_scheduled(
                    RunKind.CLOSE,
                    CLOSE_WAKE + timedelta(minutes=1),
                    context,
                )
            except (JournalError, WorkflowError):
                return
        finally:
            reports_module.Report.__init__ = original_init

        compromised = retried.exit_code == 0 and (
            not archive_path.is_file()
            or archive_path.read_text(encoding="utf-8") != first.message
        )
        self.assertFalse(compromised, "healer archived constructor-forged bytes")

    def test_healer_published_constructor_cannot_forge_its_receipt(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(RunKind.CLOSE, CLOSE_WAKE, context)
        assert first.report_path is not None
        Path(first.report_path).unlink()
        forged_path = str(Path(self.temporary.name) / "wrong-root" / "forged.md")
        original_init = PublishedWorkflow.__init__

        def forged_init(
            self: PublishedWorkflow,
            *_args: object,
            **_values: object,
        ) -> None:
            PublishedWorkflow.__init__ = original_init
            original_init(
                self,
                "f" * 64,
                999_999,
                forged_path,
                "ALREADY_EMITTED",
            )

        PublishedWorkflow.__init__ = forged_init
        try:
            try:
                retried = run_scheduled(
                    RunKind.CLOSE,
                    CLOSE_WAKE + timedelta(minutes=1),
                    context,
                )
            except (JournalError, WorkflowError):
                return
        finally:
            PublishedWorkflow.__init__ = original_init

        receipt_matches = (
            retried.report_id,
            retried.report_row_id,
            retried.report_path,
        ) == (first.report_id, first.report_row_id, first.report_path)
        self.assertTrue(
            retried.exit_code != 0 or receipt_matches,
            "healer returned a green receipt with forged report identity",
        )

    def test_healer_archive_function_code_cannot_claim_an_unwritten_archive(self) -> None:
        context = self._context("normal-close.json")
        first = run_scheduled(RunKind.CLOSE, CLOSE_WAKE, context)
        assert first.report_path is not None
        archive_path = Path(first.report_path)
        archive_path.unlink()
        archive_function = reports_module.archive_report
        original_code = archive_function.__code__
        function_globals = archive_function.__globals__
        code_key = "_ADVERSARIAL_ARCHIVE_ORIGINAL_CODE"
        path_key = "_ADVERSARIAL_ARCHIVE_EXPECTED_PATH"
        function_globals[code_key] = original_code
        function_globals[path_key] = archive_path
        exec(
            "def _adversarial_archive_once(report, root):\n"
            "    del root\n"
            "    archive_report.__code__ = _ADVERSARIAL_ARCHIVE_ORIGINAL_CODE\n"
            "    return ArchivedReport(\n"
            "        report.report_id,\n"
            "        _ADVERSARIAL_ARCHIVE_EXPECTED_PATH,\n"
            "        report.content_sha256,\n"
            "        False,\n"
            "    )\n",
            function_globals,
        )
        forged_function = function_globals.pop("_adversarial_archive_once")
        archive_function.__code__ = forged_function.__code__
        try:
            try:
                retried = run_scheduled(
                    RunKind.CLOSE,
                    CLOSE_WAKE + timedelta(minutes=1),
                    context,
                )
            except (JournalError, WorkflowError):
                return
        finally:
            archive_function.__code__ = original_code
            function_globals.pop(code_key, None)
            function_globals.pop(path_key, None)

        self.assertFalse(
            retried.exit_code == 0 and not archive_path.is_file(),
            "healer accepted a green receipt for an archive that was never written",
        )

    def test_report_completion_cannot_return_a_path_outside_bound_root(self) -> None:
        context = self._context("eligible.json")
        assert context.publisher is not None
        assert context.scheduler is not None
        wrong_path = str(Path(self.temporary.name) / "wrong-root" / "forged.md")
        original_replace = workflows_module.replace

        def forged_replace(
            value: object,
            *args: object,
            **values: object,
        ) -> object:
            result = original_replace(value, *args, **values)
            if type(result) is WorkflowResult and result.report_id is not None:
                object.__setattr__(result, "report_path", wrong_path)
            return result

        with patch.object(workflows_module, "replace", side_effect=forged_replace):
            try:
                outward = run_scheduled(
                    RunKind.PREMARKET,
                    PREMARKET_WAKE,
                    context,
                )
            except (JournalError, WorkflowError):
                return

        stored = context.scheduler.result(
            kind="PREMARKET",
            session_date=SESSION_DATE,
        )
        assert stored is not None
        assert stored.report_path is not None
        expected_path = str(
            (context.publisher.report_archive_root / stored.report_path).absolute()
        )
        self.assertEqual(outward.report_path, expected_path)

    def test_exact_envelope_constructor_cannot_turn_exit_two_durably_green(
        self,
    ) -> None:
        context = self._context("missing-configuration.json")
        assert context.scheduler is not None
        original_init = ScheduledRunResultEnvelope.__init__

        def forged_init(
            self: ScheduledRunResultEnvelope,
            *_args: object,
            **_values: object,
        ) -> None:
            ScheduledRunResultEnvelope.__init__ = original_init
            original_init(
                self,
                outcome="ALREADY_COMPLETED_NOOP",
                message="FORGED GREEN",
                exit_code=0,
                reason_codes=("ALREADY_COMPLETED_NO_REPORT",),
                execution_mode="FIXTURE",
                candidates=(),
                report_id=None,
                report_row_id=None,
                report_path=None,
            )

        ScheduledRunResultEnvelope.__init__ = forged_init
        try:
            try:
                outward = run_scheduled(
                    RunKind.PREMARKET,
                    PREMARKET_WAKE,
                    context,
                )
            except (JournalError, WorkflowError):
                return
        finally:
            ScheduledRunResultEnvelope.__init__ = original_init

        stored = context.scheduler.result(
            kind="PREMARKET",
            session_date=SESSION_DATE,
        )
        assert stored is not None
        self.assertEqual(outward.exit_code, 2)
        self.assertEqual(stored.exit_code, 2)
        self.assertNotEqual(stored.message, "FORGED GREEN")

    def test_bootstrap_is_absent_and_one_shot_in_both_import_orders(self) -> None:
        script = textwrap.dedent(
            """
            import builtins
            import sys

            order = sys.argv[1]
            captured = {}
            original_import = builtins.__import__

            def observing_import(name, globals=None, locals=None, fromlist=(), level=0):
                scheduled = sys.modules.get("stock_monitor.scheduled")
                if scheduled is not None:
                    accept = getattr(
                        scheduled,
                        "_accept_scheduled_journal_boundary",
                        None,
                    )
                    token = getattr(
                        scheduled,
                        "_scheduled_journal_bootstrap_token",
                        None,
                    )
                    if callable(accept) and token is not None:
                        captured.setdefault("accept", accept)
                        captured.setdefault("token", token)
                return original_import(name, globals, locals, fromlist, level)

            builtins.__import__ = observing_import
            try:
                if order == "journal-first":
                    import stock_monitor.journal as journal_module
                    import stock_monitor.scheduled as scheduled_module
                elif order == "scheduled-first":
                    import stock_monitor.scheduled as scheduled_module
                    import stock_monitor.journal as journal_module
                else:
                    raise AssertionError("unsupported import order")
            finally:
                builtins.__import__ = original_import

            scheduled_attributes = (
                "_scheduled_journal_bootstrap_token",
                "_accept_scheduled_journal_boundary",
                "_install_scheduled_boundary_from_journal",
            )
            journal_attributes = (
                "_take_scheduled_journal_boundary",
                "_scheduled_boundary_values",
                "_scheduled_boundary_accept",
                "_scheduled_boundary_token",
                "_scheduled_boundary_module",
                "_make_scheduled_journal_boundary",
            )
            leaked = [
                f"scheduled.{name}"
                for name in scheduled_attributes
                if hasattr(scheduled_module, name)
            ] + [
                f"journal.{name}"
                for name in journal_attributes
                if hasattr(journal_module, name)
            ]
            if leaked:
                raise AssertionError(f"bootstrap attributes leaked: {leaked!r}")

            accept = captured.get("accept")
            token = captured.get("token")
            if (accept is None) != (token is None):
                raise AssertionError("bootstrap capture was incomplete")
            if accept is not None:
                try:
                    accept(token, ())
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("bootstrap accepted a second invocation")
            print("BOOTSTRAP_CLOSED", order)
            """
        )

        for order in ("journal-first", "scheduled-first"):
            with self.subTest(order=order):
                completed = subprocess.run(
                    [sys.executable, "-Werror", "-c", script, order],
                    cwd=Path(__file__).resolve().parents[2],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(
                    completed.stdout.strip(),
                    f"BOOTSTRAP_CLOSED {order}",
                )

    def test_borrowed_runner_code_with_alternate_globals_cannot_write(self) -> None:
        context = self._context("missing-configuration.json")
        assert context.publisher is not None
        public_runner = scheduled_module.run_scheduled
        public_code = getattr(public_runner, "__code__", None)
        public_closure = getattr(public_runner, "__closure__", None)
        if public_code is None or public_closure is None:
            return
        cells = self._closure_cells(public_runner)
        implementation = cells.get("run_implementation")
        implementation_code = getattr(implementation, "__code__", None)
        if implementation_code is None:
            return

        alternate_globals = dict(public_runner.__globals__)
        alternate_globals["__name__"] = "adversarial.borrowed_scheduled"
        borrowed_implementation = types.FunctionType(
            implementation_code,
            alternate_globals,
            getattr(implementation, "__name__", "borrowed_implementation"),
            getattr(implementation, "__defaults__", None),
            getattr(implementation, "__closure__", None),
        )
        borrowed_implementation.__kwdefaults__ = getattr(
            implementation,
            "__kwdefaults__",
            None,
        )

        def closure_cell(value: object) -> object:
            def capture() -> object:
                return value

            assert capture.__closure__ is not None
            return capture.__closure__[0]

        borrowed_cells = tuple(
            closure_cell(
                borrowed_implementation
                if name == "run_implementation"
                else cell.cell_contents
            )
            for name, cell in zip(public_code.co_freevars, public_closure)
        )
        borrowed_runner = types.FunctionType(
            public_code,
            alternate_globals,
            public_runner.__name__,
            public_runner.__defaults__,
            borrowed_cells,
        )
        borrowed_runner.__kwdefaults__ = public_runner.__kwdefaults__

        with self.assertRaises((JournalError, WorkflowError)):
            borrowed_runner(
                RunKind.PREMARKET,
                PREMARKET_WAKE,
                context,
            )
        self.assertEqual(context.publisher.journal.count("scheduled_runs"), 0)


if __name__ == "__main__":
    unittest.main()
