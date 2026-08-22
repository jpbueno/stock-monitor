"""Market-calendar-selected scheduling with durable one-shot claims."""

from __future__ import annotations


def _make_scheduled_journal_bootstrap() -> tuple[object, object]:
    token = object()
    consumed = False

    def accept(candidate: object, values: object) -> None:
        nonlocal consumed
        if consumed or candidate is not token:
            raise RuntimeError("scheduled Journal bootstrap is unavailable")
        installer = globals().get("_install_scheduled_boundary_from_journal")
        if not callable(installer):
            raise RuntimeError("scheduled Journal installer is unavailable")
        consumed = True
        installer(values)

    return token, accept


(
    _scheduled_journal_bootstrap_token,
    _accept_scheduled_journal_boundary,
) = _make_scheduled_journal_bootstrap()
del _make_scheduled_journal_bootstrap

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from inspect import currentframe
from pathlib import Path
from zoneinfo import ZoneInfo

from .domain import DomainValidationError, require_aware_timestamp
from .workflows import (
    CandidateSummary,
    JournalWorkflowPublisher,
    PublishedWorkflow,
    Report,
    ScheduledWorkflowStore,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    archive_report,
    run_close,
    run_premarket,
)


_ET = ZoneInfo("America/New_York")
_DUE_WINDOW = timedelta(minutes=15)


class RunKind(str, Enum):
    """The only scheduled, read-only monitor workflows."""

    PREMARKET = "PREMARKET"
    CLOSE = "CLOSE"


class JournalScheduledRunStore:
    """Serialize scheduled starts/completions through Journal transactions."""

    def __init__(self, journal: object) -> None:
        from .journal import Journal

        if not isinstance(journal, Journal):
            raise TypeError("journal must be a Journal")
        self.journal = journal

    def start(
        self,
        *,
        kind: str,
        session_date: date,
        intended_at: datetime,
    ) -> bool:
        del kind, session_date, intended_at
        raise WorkflowError(
            "scheduled claims require the exact run_scheduled authority"
        )

    def status(self, *, kind: str, session_date: date) -> str:
        """Return whether an existing durable claim has a recorded completion."""
        return self.journal.read_scheduled_run_status(
            run_key=_run_key(kind, session_date),
            run_kind=kind,
            session_date=session_date,
        )

    def result(
        self,
        *,
        kind: str,
        session_date: date,
    ) -> WorkflowResult | None:
        """Reconstruct only the canonical, secret-safe terminal result."""
        envelope = self.journal.read_scheduled_run_result_envelope(
            run_key=_run_key(kind, session_date),
            run_kind=kind,
            session_date=session_date,
        )
        if envelope is None:
            return None
        return WorkflowResult(
            outcome=envelope.outcome,
            message=envelope.message,
            exit_code=envelope.exit_code,
            reason_codes=envelope.reason_codes,
            candidates=tuple(
                CandidateSummary(symbol=symbol, role=role)
                for symbol, role in envelope.candidates
            ),
            execution_mode=envelope.execution_mode,
            report_id=envelope.report_id,
            report_row_id=envelope.report_row_id,
            report_path=envelope.report_path,
        )

    def complete(
        self,
        *,
        kind: str,
        session_date: date,
        intended_at: datetime,
        finished_at: datetime,
        decision: str,
        result: WorkflowResult,
    ) -> None:
        del kind, session_date, intended_at, finished_at, decision, result
        raise WorkflowError(
            "scheduled completion requires the exact run_scheduled authority"
        )


def _run_scheduled_with_authority(
    kind: RunKind,
    now: datetime,
    context: WorkflowContext,
    *,
    claim_run: object,
    complete_run: object,
    abandon_run: object,
    heal_publication: object,
    run_kind_type: type[RunKind],
    store_type: type[JournalScheduledRunStore],
    workflow_error_type: type[WorkflowError],
    result_class: type[WorkflowResult],
    result_initializer: object,
    result_builder: object,
    close_workflow: object,
    premarket_workflow: object,
    replace_value: object,
    require_timestamp: object,
    domain_error_type: type[DomainValidationError],
    eastern_timezone: ZoneInfo,
    noop_result: object,
    incomplete_result: object,
    unavailable_result: object,
    read_stored_result: object,
    read_stored_status: object,
    healed_matches: object,
    datetime_type: type[datetime],
    time_type: type[time],
    _DUE_WINDOW: timedelta,
    utc_timezone: timezone,
    execution_mode: object,
    workflow_dependencies_current: object,
) -> WorkflowResult:
    """Run exactly at the selected wake, deduplicate, and never backfill."""
    if result_class.__init__ is not result_initializer:
        raise workflow_error_type("scheduled result constructor was replaced")
    if not workflow_dependencies_current(context):
        raise workflow_error_type("scheduled workflow authority was replaced")
    if type(kind) is not run_kind_type:
        raise TypeError("scheduled kind must be a RunKind")
    try:
        require_timestamp(now, "scheduled run time")
    except domain_error_type as error:
        raise ValueError(str(error)) from error
    if context.scheduler is None:
        raise ValueError("scheduled workflow requires a durable scheduler")

    now_et = now.astimezone(eastern_timezone)
    session = context.adapter.market_session(now_et.date())
    if session is None:
        return noop_result(
            "MARKET_CLOSED_NOOP", "MARKET_CLOSED", execution_mode(context)
        )
    wake = session.review_time if kind is run_kind_type.CLOSE else time_type(8, 45)
    intended_at = datetime_type.combine(
        session.session_date,
        wake,
        tzinfo=eastern_timezone,
    )
    if now_et < intended_at:
        return noop_result("NOT_DUE_NOOP", "NOT_DUE", execution_mode(context))

    if type(context.scheduler) is not store_type:
        raise workflow_error_type(
            "scheduled workflow requires the exact Journal store"
        )
    duplicate, completion_authority = claim_run(
        context.scheduler,
        kind=kind.value,
        session_date=session.session_date,
        intended_at=intended_at,
        publisher=context.publisher,
    )
    if duplicate:
        stored_status = read_stored_status(
            context.scheduler,
            kind=kind.value,
            session_date=session.session_date,
        )
        if stored_status == "IN_PROGRESS":
            return incomplete_result(execution_mode(context))
        if stored_status == "REPORT_FINALIZED":
            return incomplete_result(execution_mode(context))
        stored_result = read_stored_result(
            context.scheduler,
            kind=kind.value,
            session_date=session.session_date,
        )
        if stored_result is None:
            return unavailable_result(execution_mode(context))
        if stored_status == "REPORT_EMITTED":
            if context.publisher is None:
                return incomplete_result(execution_mode(context))
            healed = heal_publication(
                context.scheduler,
                context.publisher,
                kind=kind.value,
                session_date=session.session_date,
                stored=stored_result,
            )
            if (
                healed is None
                or not healed_matches(
                    context.publisher,
                    healed,
                    stored_result,
                )
            ):
                return unavailable_result(execution_mode(context))
            if stored_result.exit_code != 0:
                return result_builder(
                    outcome=stored_result.outcome,
                    message=stored_result.message,
                    exit_code=stored_result.exit_code,
                    reason_codes=stored_result.reason_codes,
                    candidates=stored_result.candidates,
                    execution_mode=stored_result.execution_mode,
                    report_id=stored_result.report_id,
                    report_row_id=stored_result.report_row_id,
                    report_path=healed.report_path,
                )
            return result_builder(
                outcome="ALREADY_EMITTED_NOOP",
                message="ALREADY EMITTED - ARCHIVE VERIFIED",
                exit_code=0,
                reason_codes=("FINALIZED_REPORT_ARCHIVE_VERIFIED",),
                execution_mode=stored_result.execution_mode,
                report_id=healed.report_id,
                report_row_id=healed.report_row_id,
                report_path=healed.report_path,
            )
        if stored_result.exit_code != 0:
            return stored_result
        return noop_result(
            "ALREADY_COMPLETED_NOOP",
            "ALREADY_COMPLETED_NO_REPORT",
            stored_result.execution_mode,
        )
    if now_et >= intended_at + _DUE_WINDOW:
        missed = noop_result(
            "MISSED_RUN_NOOP",
            "MISSED_RUN",
            execution_mode(context),
        )
        complete_run(
            context.scheduler,
            authority=completion_authority,
            finished_at=datetime_type.now(utc_timezone),
            decision="MISSED_RUN",
            result=missed,
        )
        return missed

    run_context = replace_value(context, now=intended_at)
    if not workflow_dependencies_current(run_context):
        abandon_run(context.scheduler, completion_authority)
        raise workflow_error_type("scheduled workflow authority was replaced")
    result = (
        close_workflow(run_context)
        if kind is run_kind_type.CLOSE
        else premarket_workflow(run_context)
    )
    if (
        result_class.__init__ is not result_initializer
        or not workflow_dependencies_current(run_context)
    ):
        abandon_run(context.scheduler, completion_authority)
        raise workflow_error_type("scheduled workflow authority changed during the run")
    if result.outcome == "PUBLICATION_INCOMPLETE":
        abandon_run(context.scheduler, completion_authority)
        return result
    if result.outcome == "CANDIDATES" and context.publisher is None:
        abandon_run(context.scheduler, completion_authority)
        return incomplete_result(execution_mode(context))
    outward = result
    if kind is run_kind_type.CLOSE and result.report_id is not None:
        outward = result_builder(
            outcome="EMITTED",
            message=result.message,
            exit_code=result.exit_code,
            reason_codes=("SCHEDULED_EMITTED", *result.reason_codes),
            candidates=result.candidates,
            report=result.report,
            source_observation_row_ids=result.source_observation_row_ids,
            execution_mode=result.execution_mode,
            report_id=result.report_id,
            report_row_id=result.report_row_id,
            report_path=result.report_path,
        )
    complete_run(
        context.scheduler,
        authority=completion_authority,
        finished_at=datetime_type.now(utc_timezone),
        decision="DUE_WAKE",
        result=outward,
    )
    return outward


def _run_key(kind: str, session_date: date) -> str:
    return f"stock-monitor:{session_date.isoformat()}:{kind}"


def _noop(outcome: str, reason: str, execution_mode: str) -> WorkflowResult:
    return WorkflowResult(
        outcome=outcome,
        message=outcome.replace("_", " "),
        exit_code=0,
        reason_codes=(reason,),
        execution_mode=execution_mode,
    )


def _schedule_incomplete(execution_mode: str) -> WorkflowResult:
    return WorkflowResult(
        outcome="SCHEDULE_INCOMPLETE",
        message="SCHEDULE INCOMPLETE - DURABLE CLAIM HAS NO COMPLETED REPORT",
        exit_code=10,
        reason_codes=("SCHEDULE_CLAIM_INCOMPLETE",),
        execution_mode=execution_mode,
    )


def _schedule_result_unavailable(execution_mode: str) -> WorkflowResult:
    return WorkflowResult(
        outcome="SCHEDULE_INCOMPLETE",
        message="SCHEDULE INCOMPLETE - STORED RESULT IS UNAVAILABLE",
        exit_code=10,
        reason_codes=("SCHEDULE_RESULT_UNAVAILABLE",),
        execution_mode=execution_mode,
    )


def _read_stored_result(
    scheduler: ScheduledWorkflowStore,
    *,
    kind: str,
    session_date: date,
) -> WorkflowResult | None:
    from .journal import JournalError

    try:
        result = scheduler.result(kind=kind, session_date=session_date)
    except JournalError:
        return None
    if result is not None and type(result) is not WorkflowResult:
        return None
    return result


def _healed_publication_matches_stored_result(
    publisher: object,
    healed: PublishedWorkflow,
    stored: WorkflowResult,
) -> bool:
    if (
        type(healed) is not PublishedWorkflow
        or healed.report_id != stored.report_id
        or healed.report_row_id != stored.report_row_id
        or type(healed.report_path) is not str
        or type(stored.report_path) is not str
    ):
        return False
    if type(publisher) is JournalWorkflowPublisher:
        expected = (publisher.report_archive_root / stored.report_path).absolute()
        return Path(healed.report_path).absolute() == expected
    return healed.report_path == stored.report_path


def _execution_mode(context: WorkflowContext) -> str:
    return getattr(context.adapter, "execution_mode", "CANONICAL")


def _install_scheduled_boundary_from_journal(values: object) -> None:
    from . import journal as journal_module

    if type(values) is not tuple or len(values) != 6:
        raise RuntimeError("scheduled Journal boundary is malformed")
    configure, claim, complete, abandon, heal, read_envelope = values
    result_type = WorkflowResult
    result_initializer = result_type.__init__
    candidate_type = CandidateSummary
    run_kind_type = RunKind
    store_type = JournalScheduledRunStore
    workflow_error_type = WorkflowError
    close_workflow = run_close
    premarket_workflow = run_premarket
    replace_value = replace
    require_timestamp = require_aware_timestamp
    domain_error_type = DomainValidationError
    eastern_timezone = _ET
    datetime_type = datetime
    time_type = time
    due_window = _DUE_WINDOW
    utc_timezone = timezone.utc
    journal_error_type = journal_module.JournalError
    read_status_method = journal_module.Journal.read_scheduled_run_status
    object_new = object.__new__
    object_setattr = object.__setattr__
    run_key_function = _run_key
    run_implementation = _run_scheduled_with_authority
    runner_code = run_implementation.__code__
    scheduled_globals = globals()
    wrapper_code: object | None = None
    inspect_frame = currentframe
    workflow_function_type = type(close_workflow)
    workflow_globals = close_workflow.__globals__
    workflow_functions = (
        (close_workflow, close_workflow.__code__, workflow_globals),
        (premarket_workflow, premarket_workflow.__code__, workflow_globals),
    )
    workflow_dependency_records: list[
        tuple[dict[str, object], str, object, object | None]
    ] = []
    visited_dependencies: set[tuple[int, str]] = set()
    visited_functions: set[int] = set()

    def capture_workflow_dependencies(function: object) -> None:
        if type(function) is not workflow_function_type:
            return
        identity = id(function)
        if identity in visited_functions:
            return
        visited_functions.add(identity)
        namespace = function.__globals__
        for name in function.__code__.co_names:
            if name not in namespace:
                continue
            key = (id(namespace), name)
            dependency = namespace[name]
            dependency_code = (
                dependency.__code__
                if type(dependency) is workflow_function_type
                else None
            )
            if key not in visited_dependencies:
                visited_dependencies.add(key)
                workflow_dependency_records.append(
                    (namespace, name, dependency, dependency_code)
                )
            if (
                dependency_code is not None
                and type(dependency.__globals__.get("__name__")) is str
                and dependency.__globals__["__name__"].startswith("stock_monitor.")
            ):
                capture_workflow_dependencies(dependency)

    capture_workflow_dependencies(close_workflow)
    capture_workflow_dependencies(premarket_workflow)
    capture_workflow_dependencies(JournalWorkflowPublisher.publish)
    adapter_type = workflow_globals.get("RecordedScenarioAdapter")
    adapter_method_names = (
        "validate_configuration",
        "market_session",
        "verify_universe",
        "provider_smoke",
        "verify_sources",
        "report_evidence",
        "premarket_snapshot",
        "close_snapshot",
    )
    adapter_methods = (
        tuple(
            (
                name,
                getattr(adapter_type, name),
                getattr(adapter_type, name).__code__,
            )
            for name in adapter_method_names
        )
        if type(adapter_type) is type
        else ()
    )
    publisher_method = JournalWorkflowPublisher.publish
    publisher_method_code = publisher_method.__code__

    def workflow_dependencies_current(context: WorkflowContext) -> bool:
        if any(
            function.__code__ is not code
            or function.__globals__ is not namespace
            for function, code, namespace in workflow_functions
        ):
            return False
        for namespace, name, expected, expected_code in workflow_dependency_records:
            current = namespace.get(name)
            if current is not expected:
                return False
            if expected_code is not None and expected.__code__ is not expected_code:
                return False
        adapter = getattr(context, "adapter", None)
        if type(adapter) is adapter_type and any(
            getattr(adapter_type, name) is not method
            or method.__code__ is not code
            for name, method, code in adapter_methods
        ):
            return False
        publisher = getattr(context, "publisher", None)
        if type(publisher) is JournalWorkflowPublisher and (
            JournalWorkflowPublisher.publish is not publisher_method
            or publisher_method.__code__ is not publisher_method_code
        ):
            return False
        return True

    def construct_candidate(symbol: str, role: str) -> CandidateSummary:
        candidate = object_new(candidate_type)
        object_setattr(candidate, "symbol", symbol)
        object_setattr(candidate, "role", role)
        object_setattr(candidate, "material", None)
        return candidate

    def construct_result(
        *,
        outcome: str,
        message: str,
        exit_code: int,
        reason_codes: tuple[str, ...],
        candidates: tuple[CandidateSummary, ...] = (),
        report: Report | None = None,
        source_observation_row_ids: tuple[int, ...] = (),
        execution_mode: str,
        report_id: str | None = None,
        report_row_id: int | None = None,
        report_path: str | None = None,
    ) -> WorkflowResult:
        result = object_new(result_type)
        for name, value in (
            ("outcome", outcome),
            ("message", message),
            ("exit_code", exit_code),
            ("reason_codes", reason_codes),
            ("candidates", candidates),
            ("report", report),
            ("source_observation_row_ids", source_observation_row_ids),
            ("execution_mode", execution_mode),
            ("report_id", report_id),
            ("report_row_id", report_row_id),
            ("report_path", report_path),
        ):
            object_setattr(result, name, value)
        return result

    def execution_mode(context: WorkflowContext) -> str:
        value = getattr(context.adapter, "execution_mode", "CANONICAL")
        if type(value) is not str:
            raise workflow_error_type("scheduled execution mode is unverified")
        return value

    def noop_result(
        outcome: str,
        reason: str,
        mode: str,
    ) -> WorkflowResult:
        return construct_result(
            outcome=outcome,
            message=outcome.replace("_", " "),
            exit_code=0,
            reason_codes=(reason,),
            execution_mode=mode,
        )

    def incomplete_result(mode: str) -> WorkflowResult:
        return construct_result(
            outcome="SCHEDULE_INCOMPLETE",
            message="SCHEDULE INCOMPLETE - DURABLE CLAIM HAS NO COMPLETED REPORT",
            exit_code=10,
            reason_codes=("SCHEDULE_CLAIM_INCOMPLETE",),
            execution_mode=mode,
        )

    def unavailable_result(mode: str) -> WorkflowResult:
        return construct_result(
            outcome="SCHEDULE_INCOMPLETE",
            message="SCHEDULE INCOMPLETE - STORED RESULT IS UNAVAILABLE",
            exit_code=10,
            reason_codes=("SCHEDULE_RESULT_UNAVAILABLE",),
            execution_mode=mode,
        )

    def read_stored_status(
        store: JournalScheduledRunStore,
        *,
        kind: str,
        session_date: date,
    ) -> str:
        return read_status_method(
            store.journal,
            run_key=run_key_function(kind, session_date),
            run_kind=kind,
            session_date=session_date,
        )

    def read_result(
        store: JournalScheduledRunStore,
        *,
        kind: str,
        session_date: date,
    ) -> WorkflowResult | None:
        envelope = read_envelope(
            store.journal,
            run_key=run_key_function(kind, session_date),
            run_kind=kind,
            session_date=session_date,
        )
        if envelope is None:
            return None
        return construct_result(
            outcome=envelope.outcome,
            message=envelope.message,
            exit_code=envelope.exit_code,
            reason_codes=envelope.reason_codes,
            candidates=tuple(
                construct_candidate(symbol, role)
                for symbol, role in envelope.candidates
            ),
            execution_mode=envelope.execution_mode,
            report_id=envelope.report_id,
            report_row_id=envelope.report_row_id,
            report_path=envelope.report_path,
        )

    JournalScheduledRunStore.result = read_result
    JournalScheduledRunStore.status = read_stored_status

    def read_stored_result(
        store: JournalScheduledRunStore,
        *,
        kind: str,
        session_date: date,
    ) -> WorkflowResult | None:
        try:
            result = read_result(store, kind=kind, session_date=session_date)
        except journal_error_type:
            return None
        if result is not None and type(result) is not result_type:
            return None
        return result

    def healed_matches(
        publisher: object,
        healed: object,
        stored: WorkflowResult,
    ) -> bool:
        if (
            type(publisher) is not JournalWorkflowPublisher
            or type(healed) is not PublishedWorkflow
            or healed.status != "ALREADY_EMITTED"
            or healed.report_id != stored.report_id
            or healed.report_row_id != stored.report_row_id
            or type(stored.report_path) is not str
            or type(healed.report_path) is not str
        ):
            return False
        expected = str((publisher.report_archive_root / stored.report_path).absolute())
        return healed.report_path == expected

    def require_store_runner() -> None:
        frame = inspect_frame()
        store_frame = None if frame is None else frame.f_back
        runner_frame = None if store_frame is None else store_frame.f_back
        public_frame = None if runner_frame is None else runner_frame.f_back
        try:
            if (
                store_frame is None
                or runner_frame is None
                or public_frame is None
                or wrapper_code is None
                or runner_frame.f_code is not runner_code
                or public_frame.f_code is not wrapper_code
                or runner_frame.f_globals is not scheduled_globals
                or public_frame.f_globals is not scheduled_globals
            ):
                raise workflow_error_type(
                    "scheduled writes require the exact live run_scheduled stack"
                )
        finally:
            del frame, store_frame, runner_frame, public_frame

    def protected_store_start(
        store: JournalScheduledRunStore,
        *,
        kind: str,
        session_date: date,
        intended_at: datetime,
        publisher: object = None,
    ) -> tuple[bool, object | None]:
        require_store_runner()
        return claim(
            store,
            kind=kind,
            session_date=session_date,
            intended_at=intended_at,
            publisher=publisher,
        )

    def protected_store_complete(
        store: JournalScheduledRunStore,
        *,
        authority: object = None,
        finished_at: datetime,
        decision: str,
        result: WorkflowResult,
        kind: str | None = None,
        session_date: date | None = None,
        intended_at: datetime | None = None,
    ) -> None:
        del kind, session_date, intended_at
        require_store_runner()
        complete(
            authority,
            store,
            finished_at=finished_at,
            decision=decision,
            result=result,
        )

    def protected_store_abandon(
        store: JournalScheduledRunStore,
        authority: object,
    ) -> None:
        del store
        require_store_runner()
        abandon(authority)

    def protected_run_scheduled(
        kind: RunKind,
        now: datetime,
        context: WorkflowContext,
    ) -> WorkflowResult:
        return run_implementation(
            kind,
            now,
            context,
            claim_run=protected_store_start,
            complete_run=protected_store_complete,
            abandon_run=protected_store_abandon,
            heal_publication=heal,
            run_kind_type=run_kind_type,
            store_type=store_type,
            workflow_error_type=workflow_error_type,
            result_class=result_type,
            result_initializer=result_initializer,
            result_builder=construct_result,
            close_workflow=close_workflow,
            premarket_workflow=premarket_workflow,
            replace_value=replace_value,
            require_timestamp=require_timestamp,
            domain_error_type=domain_error_type,
            eastern_timezone=eastern_timezone,
            noop_result=noop_result,
            incomplete_result=incomplete_result,
            unavailable_result=unavailable_result,
            read_stored_result=read_stored_result,
            read_stored_status=read_stored_status,
            healed_matches=healed_matches,
            datetime_type=datetime_type,
            time_type=time_type,
            _DUE_WINDOW=due_window,
            utc_timezone=utc_timezone,
            execution_mode=execution_mode,
            workflow_dependencies_current=workflow_dependencies_current,
        )

    configure(
        candidate=candidate_type,
        workflow_result=result_type,
        publisher=JournalWorkflowPublisher,
        published=PublishedWorkflow,
        report=Report,
        archive=archive_report,
        workflow_error=workflow_error_type,
        runner=run_implementation,
        wrapper=protected_run_scheduled,
        scheduled_globals=scheduled_globals,
        store_start=protected_store_start,
        store_complete=protected_store_complete,
        dependencies=(
            ("claim_run", protected_store_start),
            ("complete_run", protected_store_complete),
            ("abandon_run", protected_store_abandon),
            ("heal_publication", heal),
            ("run_kind_type", run_kind_type),
            ("store_type", store_type),
            ("workflow_error_type", workflow_error_type),
            ("result_class", result_type),
            ("result_initializer", result_initializer),
            ("result_builder", construct_result),
            ("close_workflow", close_workflow),
            ("premarket_workflow", premarket_workflow),
            ("replace_value", replace_value),
            ("require_timestamp", require_timestamp),
            ("domain_error_type", domain_error_type),
            ("eastern_timezone", eastern_timezone),
            ("noop_result", noop_result),
            ("incomplete_result", incomplete_result),
            ("unavailable_result", unavailable_result),
            ("read_stored_result", read_stored_result),
            ("read_stored_status", read_stored_status),
            ("healed_matches", healed_matches),
            ("datetime_type", datetime_type),
            ("time_type", time_type),
            ("_DUE_WINDOW", due_window),
            ("utc_timezone", utc_timezone),
            ("execution_mode", execution_mode),
            ("workflow_dependencies_current", workflow_dependencies_current),
        ),
    )
    wrapper_code = protected_run_scheduled.__code__
    JournalScheduledRunStore.start = protected_store_start
    JournalScheduledRunStore.complete = protected_store_complete
    globals()["run_scheduled"] = protected_run_scheduled
    for name in (
        "_scheduled_journal_bootstrap_token",
        "_accept_scheduled_journal_boundary",
        "_install_scheduled_boundary_from_journal",
        "_run_scheduled_with_authority",
        "_noop",
        "_schedule_incomplete",
        "_schedule_result_unavailable",
        "_read_stored_result",
        "_healed_publication_matches_stored_result",
        "_execution_mode",
        "_run_key",
        "_DUE_WINDOW",
    ):
        globals().pop(name, None)


from . import journal as _journal_bootstrap  # noqa: E402

del _journal_bootstrap


__all__ = ["JournalScheduledRunStore", "RunKind", "run_scheduled"]
