"""Stable, secret-safe command-line boundary for Stock Monitor."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import ConfigurationError, Settings, load_settings
from .domain import DomainValidationError, require_aware_timestamp
from .provider_smoke import run_provider_smoke
from .workflows import (
    JournalWorkflowPublisher,
    RecordedScenarioAdapter,
    WorkflowBoundaryError,
    WorkflowContext,
    WorkflowDataError,
    WorkflowReconciliationError,
    WorkflowResult,
    run_close,
    run_premarket,
)


_ET = ZoneInfo("America/New_York")
_SESSION_DATE_LITERAL = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")


class DataCommandError(RuntimeError):
    """A CLI verification/export dependency is unavailable."""


class CommandBoundaryError(RuntimeError):
    """A CLI request is outside an activated paper/manual-only adapter."""


class VerificationCommandError(RuntimeError):
    """A requested state transition failed a closed verification gate."""


def build_parser() -> argparse.ArgumentParser:
    """Build the locked Task 11 command/subcommand grammar."""
    parser = argparse.ArgumentParser(
        prog="stock-monitor",
        description="Local paper/manual-only stock and ETF decision support.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    db = commands.add_parser("db")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    _json_flag(db_commands.add_parser("init"))

    verify = commands.add_parser("verify")
    verify_commands = verify.add_subparsers(dest="verify_command", required=True)
    _json_flag(verify_commands.add_parser("universe"))
    _json_flag(verify_commands.add_parser("calendar"))

    provider = commands.add_parser("provider")
    provider_commands = provider.add_subparsers(dest="provider_command", required=True)
    _json_flag(provider_commands.add_parser("smoke"))

    run = commands.add_parser("run")
    run_commands = run.add_subparsers(dest="run_command", required=True)
    for name in ("premarket", "close"):
        workflow = run_commands.add_parser(name)
        workflow.add_argument("--fixture", type=Path)
        workflow.add_argument(
            "--scheduled",
            action="store_true",
            help="Use the durable exact-wake/no-backfill scheduler path.",
        )
        _json_flag(workflow)

    confirm = commands.add_parser("confirm")
    confirm.add_argument("--message-id", required=True)
    confirm.add_argument("--message-time", required=True)
    confirm.add_argument("--text", required=True)
    _json_flag(confirm)

    phase1 = commands.add_parser("phase1")
    phase1_commands = phase1.add_subparsers(dest="phase1_command", required=True)
    _json_flag(phase1_commands.add_parser("status"))
    phase1_start = phase1_commands.add_parser("start")
    phase1_start.add_argument("--session", required=True)
    _json_flag(phase1_start)

    replay = commands.add_parser("replay")
    replay_commands = replay.add_subparsers(dest="replay_command", required=True)
    for name in ("diagnostic", "point-in-time"):
        replay_command = replay_commands.add_parser(name)
        replay_command.add_argument("--fixture", type=Path, required=True)
        _json_flag(replay_command)

    option = commands.add_parser("option-paper")
    option_commands = option.add_subparsers(dest="option_command", required=True)
    for name in ("start", "rank", "status"):
        option_command = option_commands.add_parser(name)
        option_command.add_argument("--fixture", type=Path, required=True)
        option_command.add_argument(
            "--live",
            action="store_true",
            help="Explicitly rejected: this command is paper-only.",
        )
        _json_flag(option_command)

    _json_flag(commands.add_parser("export"))
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str],
) -> int:
    """Parse, dispatch, and map only declared safe failures to stable exits."""
    try:
        arguments = build_parser().parse_args(argv)
        return _dispatch(arguments, environ)
    except ConfigurationError:
        _print_message("CONFIGURATION REQUIRED\nNo candidate or action was produced.")
        return 2
    except (WorkflowDataError, DataCommandError):
        _print_message("DATA UNAVAILABLE\nNo candidate or action was produced.")
        return 3
    except VerificationCommandError:
        _print_message("VERIFICATION BLOCKED\nNo candidate or action was produced.")
        return 4
    except WorkflowReconciliationError:
        _print_message("RECONCILIATION REQUIRED\nNo action was authorized.")
        return 5
    except (WorkflowBoundaryError, CommandBoundaryError):
        _print_message("PAPER ONLY\nThe requested command is not authorized.")
        return 5
    except Exception:
        # Unexpected exception detail may contain provider payloads or secrets.
        _print_message("INTERNAL ERROR\nNo candidate or action was produced.")
        return 10


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI with the process environment at the executable boundary."""
    return run(argv, environ=os.environ)


def _dispatch(arguments: argparse.Namespace, environ: Mapping[str, str]) -> int:
    settings = load_settings(_project_root(), environ)
    command = arguments.command
    if command == "db":
        return _db_init(settings, arguments.json)
    if command == "verify":
        return _verify(settings, arguments.verify_command, arguments.json)
    if command == "provider":
        if arguments.provider_command != "smoke":
            raise CommandBoundaryError("unsupported provider command")
        result = run_provider_smoke(settings, now=lambda: datetime.now(timezone.utc))
        _emit(result.safe_fields(), arguments.json)
        return result.exit_code
    if command == "run":
        return _run_workflow(settings, arguments)
    if command == "confirm":
        return _confirm(settings, arguments)
    if command == "phase1":
        if arguments.phase1_command == "status":
            return _phase1_status(settings, arguments.json)
        if arguments.phase1_command == "start":
            return _phase1_start(settings, arguments.session, arguments.json)
        raise CommandBoundaryError("unsupported Phase 1 command")
    if command == "replay":
        raise CommandBoundaryError("recorded replay adapter is not activated")
    if command == "option-paper":
        if arguments.live:
            raise CommandBoundaryError("live options are prohibited")
        raise CommandBoundaryError("paper option CLI adapter is not activated")
    if command == "export":
        return _export(settings, arguments.json)
    raise CommandBoundaryError("unsupported command")


def _db_init(settings: Settings, as_json: bool) -> int:
    from .journal import Journal

    with Journal.open(settings.journal_path) as journal:
        count = journal.count("schema_migrations")
    _emit({"status": "INITIALIZED", "migration_count": count}, as_json)
    return 0


def _verify(settings: Settings, kind: str, as_json: bool) -> int:
    today = datetime.now(_ET).date()
    try:
        if kind == "calendar":
            from .market_calendar import load_current_market_calendar

            calendar = load_current_market_calendar(settings.project_root, as_of=today)
            is_open = calendar.is_open(today)
            session = calendar.session(today) if is_open else None
            payload = {
                "status": "VERIFIED",
                "kind": "CALENDAR",
                "year": calendar.year,
                "session_date": today.isoformat(),
                "is_open": is_open,
                "open_time": (
                    session.open_time.strftime("%H:%M") if session is not None else None
                ),
                "review_time": (
                    session.review_time.strftime("%H:%M")
                    if session is not None
                    else None
                ),
                "close_time": (
                    session.close_time.strftime("%H:%M") if session is not None else None
                ),
                "is_early_close": (
                    session.is_early_close if session is not None else False
                ),
                "timezone": str(calendar.timezone),
            }
        elif kind == "universe":
            from .universe import load_current_universe

            universe = load_current_universe(settings.project_root, as_of=today)
            payload = {
                "status": "VERIFIED",
                "kind": "UNIVERSE",
                "record_count": len(universe.records),
            }
        else:
            raise CommandBoundaryError("unsupported verification")
    except (OSError, ValueError) as error:
        raise DataCommandError("reviewed reference data is unavailable") from error
    _emit(payload, as_json)
    return 0


def _run_workflow(settings: Settings, arguments: argparse.Namespace) -> int:
    if arguments.fixture is None:
        raise CommandBoundaryError(
            "live collection requires an explicitly configured workflow adapter"
        )
    adapter = RecordedScenarioAdapter.load(arguments.fixture)
    from .journal import Journal

    fixture_root = _validated_fixture_root(
        settings.state_root,
        adapter.evidence.state_hash,
    )
    with Journal.open(fixture_root / "journal.sqlite3") as journal:
        adapter = adapter.bind_source_observation(journal)
        scheduler = None
        if arguments.scheduled:
            from .scheduled import JournalScheduledRunStore

            scheduler = JournalScheduledRunStore(journal)
        context = WorkflowContext(
            adapter=adapter,
            publisher=JournalWorkflowPublisher(journal, fixture_root),
            scheduler=scheduler,
            now=adapter.now,
        )
        if arguments.scheduled:
            from .scheduled import RunKind, run_scheduled

            kind = (
                RunKind.CLOSE
                if arguments.run_command == "close"
                else RunKind.PREMARKET
            )
            result = run_scheduled(kind, adapter.now, context)
        else:
            result = (
                run_close(context)
                if arguments.run_command == "close"
                else run_premarket(context)
            )
    _emit_result(result, arguments.json)
    return result.exit_code


def _validated_fixture_root(state_root: Path, state_hash: str) -> Path:
    """Create the content-addressed fixture root without traversing symlinks."""
    normalized_state_root = Path(os.path.abspath(state_root))
    fixtures_root = normalized_state_root / "fixtures"
    fixture_root = fixtures_root / state_hash
    if fixture_root.parent != fixtures_root or fixture_root.name != state_hash:
        raise CommandBoundaryError("fixture state path is outside its sandbox")

    paths = (normalized_state_root, fixtures_root, fixture_root)
    if any(path.is_symlink() for path in paths):
        raise CommandBoundaryError("fixture state path cannot traverse a symlink")
    try:
        fixture_root.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise CommandBoundaryError("fixture state path is unavailable") from error
    if any(path.is_symlink() for path in paths):
        raise CommandBoundaryError("fixture state path cannot traverse a symlink")
    try:
        if (
            fixtures_root.resolve(strict=True).parent
            != normalized_state_root.resolve(strict=True)
            or fixture_root.resolve(strict=True).parent
            != fixtures_root.resolve(strict=True)
        ):
            raise CommandBoundaryError("fixture state path escapes its sandbox")
    except OSError as error:
        raise CommandBoundaryError("fixture state path is unavailable") from error
    return fixture_root


def _confirm(settings: Settings, arguments: argparse.Namespace) -> int:
    from .confirmations import ConfirmationEnvelope
    from .journal import Journal
    from .market_calendar import load_current_market_calendar
    from .reconciliation import (
        UnavailableActualEntryAuthorityResolver,
        UnavailableSignalPlanResolver,
        ingest_confirmation,
    )
    from .risk import SessionCalendarResolver

    try:
        message_time = require_aware_timestamp(
            datetime.fromisoformat(arguments.message_time),
            "message time",
        )
    except (ValueError, DomainValidationError) as error:
        raise CommandBoundaryError("confirmation time is invalid") from error
    session_date = message_time.astimezone(_ET).date()
    try:
        calendar = load_current_market_calendar(
            settings.project_root,
            as_of=session_date,
        )
        resolver = SessionCalendarResolver((calendar,))
        envelope = ConfirmationEnvelope(
            message_id=arguments.message_id,
            message_time=message_time,
            received_at=datetime.now(timezone.utc),
            text=arguments.text,
            session_date=session_date,
        )
        with Journal.open(settings.journal_path) as journal:
            with journal.transaction() as transaction:
                stored = transaction.read_confirmation_result(
                    message_id=arguments.message_id,
                )
            if stored is not None:
                if (
                    stored.message_time != message_time.astimezone(timezone.utc)
                    or stored.raw_text != arguments.text
                ):
                    raise CommandBoundaryError(
                        "confirmation message identity conflicts with stored content"
                    )
                _emit(
                    {
                        "message_id": stored.message_id,
                        "duplicate": True,
                        "state_digest": stored.state_digest,
                        "actions": [
                            {
                                "ordinal": action.ordinal,
                                "kind": action.domain_kind,
                                "status": action.status,
                                "reason_codes": list(action.reason_codes),
                            }
                            for action in stored.actions
                        ],
                    },
                    arguments.json,
                )
                return 0
            result = ingest_confirmation(
                journal,
                envelope,
                plans=UnavailableSignalPlanResolver(),
                calendar=resolver,
                policy=settings.policy,
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
    except (OSError, ValueError) as error:
        raise CommandBoundaryError("confirmation could not be authorized") from error
    payload = {
        "message_id": result.message_id,
        "duplicate": result.duplicate,
        "state_digest": result.state_digest,
        "actions": [
            {
                "ordinal": action.ordinal,
                "kind": action.kind.value,
                "status": action.status.value,
                "reason_codes": list(action.reason_codes),
            }
            for action in result.actions
        ],
    }
    _emit(payload, arguments.json)
    return 0


def _strict_session_date(value: object) -> date:
    if (
        type(value) is not str
        or _SESSION_DATE_LITERAL.fullmatch(value) is None
    ):
        raise ConfigurationError("session must be a literal YYYY-MM-DD date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ConfigurationError(
            "session must be a literal YYYY-MM-DD date"
        ) from error
    if parsed.isoformat() != value:
        raise ConfigurationError("session must be a literal YYYY-MM-DD date")
    return parsed


def _phase1_start(settings: Settings, session_text: str, as_json: bool) -> int:
    from .journal import IdempotencyConflict, InvalidJournalValue, Journal
    from .market_calendar import CalendarError, load_current_market_calendar
    from .phase1_bootstrap import bootstrap_phase1
    from .risk import RiskBlock, SessionCalendarResolver

    session_date = _strict_session_date(session_text)
    try:
        calendar = load_current_market_calendar(
            settings.project_root,
            as_of=session_date,
        )
    except CalendarError as error:
        raise DataCommandError(
            "reviewed calendar release is unavailable"
        ) from error

    try:
        resolver = SessionCalendarResolver((calendar,))
        with Journal.open(settings.journal_path) as journal:
            stored = bootstrap_phase1(
                journal,
                session_date=session_date,
                calendar_resolver=resolver,
                received_at=datetime.now(timezone.utc),
            )
    except (IdempotencyConflict, InvalidJournalValue, RiskBlock) as error:
        raise VerificationCommandError(
            "Phase 1 validation window could not be verified"
        ) from error
    _emit(stored.safe_fields(), as_json)
    return 0


def _phase1_status(settings: Settings, as_json: bool) -> int:
    from .journal import Journal

    with Journal.open(settings.journal_path) as journal:
        windows = journal.count("phase1_validation_windows")
        completed = journal.count("phase1_closed_trades")
    _emit(
        {
            "status": "PAPER_VALIDATION",
            "windows": windows,
            "completed_trades": completed,
        },
        as_json,
    )
    return 0


def _export(settings: Settings, as_json: bool) -> int:
    from .exports import ExportError, export_tables
    from .journal import Journal

    try:
        with Journal.open(settings.journal_path) as journal:
            paths = export_tables(journal, settings.state_root / "exports")
    except (ExportError, OSError) as error:
        raise DataCommandError("journal export failed") from error
    _emit({"status": "EXPORTED", "paths": [str(path) for path in paths]}, as_json)
    return 0


def _emit_result(result: WorkflowResult, as_json: bool) -> None:
    _emit(result.safe_fields(), as_json, plain=result.message)


def _emit(
    payload: Mapping[str, object],
    as_json: bool,
    *,
    plain: str | None = None,
) -> None:
    if as_json:
        print(
            json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    elif plain is not None:
        _print_message(plain)
    else:
        _print_message(str(payload.get("status", "OK")))


def _print_message(message: str) -> None:
    print(message)


def _json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


__all__ = ["build_parser", "main", "run"]
