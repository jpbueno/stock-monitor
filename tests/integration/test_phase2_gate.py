from __future__ import annotations

import copy
import gc
import json
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest
from collections.abc import Callable
from contextlib import closing, nullcontext
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from unittest import mock
from weakref import ref
from zoneinfo import ZoneInfo

from stock_monitor import config as config_module
from stock_monitor import evidence as evidence_module
from stock_monitor import journal as journal_module
from stock_monitor import options_paper as options_paper_module
from stock_monitor import validation as validation_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.config import load_fee_schedule
from stock_monitor.journal import (
    IdempotencyConflict,
    InvalidJournalValue,
    Journal,
    JournalError,
    is_verified_journal_action_source,
)
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
)
from stock_monitor.risk import SessionCalendarResolver, _calendar_digest
from stock_monitor.replay import (
    ReplayCase,
    ReplayDomainComponent,
    ReplayDomainResult,
    ReplayDomainStatus,
    ReplayError,
    ReplayRequest,
    _register_historical_replay_source,
    is_verified_historical_replay_source as is_replay_registered_source,
    replay_ambiguous_bar,
    replay_point_in_time,
)
from tests.integration.test_phase1_authorities import (
    _SESSION,
    _calendar,
    _persist_signal_evidence,
    _publish_session_primary,
    _seed_not_triggered_adherence_material,
    _seed_completed_authority_fill,
    _seed_session_closed_primary,
    _start_window,
)
from tests.support import aware_et, policy_fixture
from tests.unit import _task5_fixtures as task5_fixture_module


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _phase2_fee_insert_material(
    schedule: object,
    *,
    archived_at: datetime,
    reviewed_bytes: bytes | None = None,
) -> tuple[str, tuple[object, ...]]:
    if reviewed_bytes is None:
        reviewed_bytes = config_module._read_reviewed_fee_schedule_bytes(schedule)
    source_digest = journal_module._phase2_fee_schedule_source_digest(
        schedule_id=schedule.schedule_id,
        effective_session=schedule.effective_session.isoformat(),
        reviewed_at=journal_module._canonical_timestamp(schedule.reviewed_at),
        currency=schedule.currency,
        contract_multiplier=schedule.contract_multiplier,
        entry_fee_per_contract_micros=(
            schedule.entry_fee_per_contract_micros
        ),
        exit_fee_per_contract_micros=(
            schedule.exit_fee_per_contract_micros
        ),
        close_fee_reserve_per_contract_micros=(
            schedule.close_fee_reserve_per_contract_micros
        ),
        source_sha256=schedule.source_sha256,
        schedule_digest=schedule.digest,
        archived_at=journal_module._canonical_timestamp(archived_at),
    )
    values_without_hash = (
        schedule.schedule_id,
        schedule.effective_session.isoformat(),
        journal_module._canonical_timestamp(schedule.reviewed_at),
        schedule.currency,
        schedule.contract_multiplier,
        schedule.entry_fee_per_contract_micros,
        schedule.exit_fee_per_contract_micros,
        schedule.close_fee_reserve_per_contract_micros,
        schedule.source_sha256,
        schedule.digest,
        reviewed_bytes,
        journal_module._canonical_timestamp(archived_at),
        source_digest,
    )
    row = (
        *values_without_hash,
        journal_module._phase2_fee_schedule_record_digest(values_without_hash),
    )
    statement = (
        "INSERT INTO phase2_fee_schedules("
        + ", ".join(journal_module._PHASE2_FEE_SCHEDULE_COLUMNS[1:])
        + ") VALUES ("
        + ", ".join("?" for _ in row)
        + ")"
    )
    return statement, row


def _seed_passed_phase1_promotion(
    journal: Journal,
    *,
    session_count: int = 20,
) -> tuple[object, tuple[date, ...], datetime]:
    _start_window(journal)
    sessions = tuple(
        _calendar().add_sessions(_SESSION, offset)
        for offset in range(session_count)
    )
    cutoffs = tuple(
        _seed_session_closed_primary(
            journal,
            session_date=session_date,
            sequence=sequence,
        )[1]
        for sequence, session_date in enumerate(sessions, start=1)
    )
    decision = journal.read_phase1_promotion_decision(
        "1" * 64,
        through_session=sessions[-1],
        query_cutoff=cutoffs[-1],
        calendar_resolver=_calendar(),
    )
    assert validation_module.is_issued_promotion_decision(decision)
    assert decision.passed
    return decision, sessions, cutoffs[-1]


def _phase2_window_start_action(
    journal: Journal,
    *,
    session_date: date,
    event_at: datetime,
):
    result = ingest_confirmation(
        journal,
        ConfirmationEnvelope(
            message_id=f"phase2-window-start:{session_date.isoformat()}",
            message_time=event_at + timedelta(seconds=1),
            received_at=event_at + timedelta(seconds=2),
            text=(
                "OPTION PAPER WINDOW START AT "
                + event_at.isoformat(timespec="seconds")
            ),
            session_date=session_date,
        ),
        plans=UnavailableSignalPlanResolver(),
        calendar=_calendar(),
        policy=policy_fixture(),
        entry_authorities=UnavailableActualEntryAuthorityResolver(),
    )
    with journal.transaction() as transaction:
        source = transaction.read_action_source(
            execution_event_id=result.actions[0].event_row_id,
        )
    assert is_verified_journal_action_source(source)
    assert source.domain_kind == "OPTION_PAPER_WINDOW_START"
    return source


def _append_delayed_phase1_actual_hard_evidence(
    journal: Journal,
    *,
    symbol: str,
    economic_session: date,
    received_after: datetime,
) -> datetime:
    last_received = received_after
    actions = (
        (
            "buy",
            "BOUGHT "
            + symbol
            + " 1 shares @ 20 AT "
            + aware_et(economic_session, "10:14").isoformat(
                timespec="seconds"
            ),
        ),
        (
            "sell",
            "SOLD "
            + symbol
            + " 1 shares @ 21 AT "
            + aware_et(economic_session, "15:31").isoformat(
                timespec="seconds"
            ),
        ),
    )
    for ordinal, (label, text) in enumerate(actions, start=1):
        message_time = received_after + timedelta(minutes=ordinal)
        last_received = message_time + timedelta(seconds=1)
        ingest_confirmation(
            journal,
            ConfirmationEnvelope(
                message_id=f"phase2-delayed-hard:{label}",
                message_time=message_time,
                received_at=last_received,
                text=text,
                session_date=message_time.astimezone(
                    ZoneInfo("America/New_York")
                ).date(),
            ),
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
            entry_authorities=UnavailableActualEntryAuthorityResolver(),
        )
    return last_received


class Phase2JournalSourceContractTests(unittest.TestCase):
    @staticmethod
    def _required_phase1_fingerprint_forest() -> Callable[
        [tuple[object, ...]],
        tuple[journal_module._SourceFingerprintSeal, ...],
    ]:
        forest = getattr(
            journal_module,
            "_phase1_source_fingerprint_forest",
            None,
        )
        if not callable(forest):
            raise AssertionError(
                "Phase 1 fingerprint forest helper is unavailable"
            )
        return forest

    @staticmethod
    def _required_phase1_fingerprint_factory(
        source: object,
    ) -> Callable[[], object]:
        factory_type = getattr(
            journal_module,
            "_Phase1SourceFingerprintFactory",
            None,
        )
        if not isinstance(factory_type, type):
            raise AssertionError(
                "Phase 1 fingerprint factory tag is unavailable"
            )
        factory = factory_type(source)
        if type(factory) is not factory_type or not callable(factory):
            raise AssertionError("Phase 1 fingerprint factory tag is malformed")
        return factory

    @staticmethod
    def _required_phase1_forest_describe_node() -> Callable[..., object]:
        describe_node = getattr(
            journal_module,
            "_phase1_forest_describe_node",
            None,
        )
        if not callable(describe_node):
            raise AssertionError(
                "Phase 1 forest node-descriptor helper is unavailable"
            )
        return describe_node

    @staticmethod
    def _synthetic_authority_candidate(
        journal: Journal,
        registry: journal_module._JournalSourceRegistry,
        source: object,
        expected_fingerprint: object,
        fingerprint_factory: Callable[[], object],
    ) -> journal_module._JournalAuthorityCandidate:
        owner_reference = ref(journal)
        total_changes = (
            journal_module._journal_source_authority_total_changes(journal)
        )
        data_version = journal._source_authority_data_version()
        issued = (
            ref(source),
            expected_fingerprint,
            owner_reference,
            total_changes,
            data_version,
        )
        registry[id(source)] = issued
        return (
            registry,
            source,
            journal,
            total_changes,
            data_version,
            expected_fingerprint,
            fingerprint_factory,
            issued,
        )

    def test_variadic_phase1_owner_check_resolves_anchor_once(self) -> None:
        owner = object()
        sources = (object(), object(), object(), object())
        candidates = {
            source: (
                {},
                source,
                owner,
                1,
                1,
                (),
                lambda: (),
            )
            for source in sources
        }
        with mock.patch.object(
            journal_module,
            "_phase1_source_authority_candidate",
            side_effect=lambda source: candidates[source],
        ) as resolve_candidate, mock.patch.object(
            journal_module,
            "_current_journal_source_authority_owner",
            return_value=owner,
        ):
            self.assertTrue(
                journal_module.phase1_sources_share_owner(
                    sources[0],
                    *sources[1:],
                )
            )
        self.assertEqual(
            tuple(call.args[0] for call in resolve_candidate.call_args_list),
            sources,
        )

    def test_variadic_owner_check_recomputes_after_read_scope_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            with journal._phase1_publication_read_scope():
                source = journal._read_phase1_signal_source(
                    signal_id,
                    query_cutoff=cutoff,
                )
                publication = source.publication_source
                self.assertTrue(
                    journal_module.is_verified_phase1_signal_source(source)
                )
                self.assertTrue(
                    journal_module.phase1_sources_share_owner(
                        source,
                        publication,
                        publication,
                    )
                )

                replacement = (
                    "0" * 64
                    if publication.body_sha256 != "0" * 64
                    else "1" * 64
                )
                object.__setattr__(
                    publication,
                    "body_sha256",
                    replacement,
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_publication_source(
                        publication
                    )
                )
                self.assertFalse(
                    journal_module.is_verified_phase1_signal_source(source)
                )
                self.assertFalse(
                    journal_module.phase1_sources_share_owner(
                        source,
                        publication,
                        publication,
                    )
                )

    def test_owner_batch_fingerprints_after_all_data_version_callbacks(
        self,
    ) -> None:
        for mutation_target in ("anchor", "right"):
            with (
                self.subTest(mutation_target=mutation_target),
                tempfile.TemporaryDirectory() as directory,
                Journal.open(Path(directory) / "journal.sqlite3") as journal,
            ):
                signal_id, cutoff = _seed_not_triggered_adherence_material(
                    journal
                )
                source = journal._read_phase1_signal_source(
                    signal_id,
                    query_cutoff=cutoff,
                )
                publication = source.publication_source
                data_version_calls = 0

                def mutate_anchor(statement: str) -> None:
                    nonlocal data_version_calls
                    if statement.strip().upper() != "PRAGMA DATA_VERSION":
                        return
                    data_version_calls += 1
                    target = source if mutation_target == "anchor" else publication
                    replacement = (
                        "0" * 64
                        if target.source_digest != "0" * 64
                        else "1" * 64
                    )
                    object.__setattr__(
                        target,
                        "source_digest",
                        replacement,
                    )

                journal._connection.set_trace_callback(mutate_anchor)
                try:
                    same_owner = journal_module.phase1_sources_share_owner(
                        source,
                        publication,
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                # One owner-currentness read covers the whole batch; its
                # callback cannot mutate either the anchor or a right before
                # the callback-free final fingerprint pass.
                self.assertEqual(data_version_calls, 1)
                self.assertFalse(same_owner)
                self.assertFalse(
                    (
                        journal_module.is_verified_phase1_signal_source(source)
                        if mutation_target == "anchor"
                        else journal_module.is_verified_phase1_publication_source(
                            publication
                        )
                    )
                )

    def test_adherence_review_verifies_each_supplied_root_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "first.sqlite3"
        ) as journal, Journal.open(
            Path(directory) / "second.sqlite3"
        ) as other_journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            other_signal_id, other_cutoff = (
                _seed_not_triggered_adherence_material(other_journal)
            )
            self.assertEqual((other_signal_id, other_cutoff), (signal_id, cutoff))
            calendar = _calendar()
            policy = policy_fixture()
            through_session = journal._phase1_adherence_through_session(
                query_cutoff=cutoff,
                calendar_resolver=calendar,
            )

            disposition = (
                journal._read_phase1_published_signal_disposition_source(
                    signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=calendar,
                    policy=policy,
                )
            )
            canonical = journal._read_phase1_canonical_replay_source(
                query_cutoff=cutoff,
                calendar_resolver=calendar,
                policy=policy,
            )
            breaker = journal._read_phase1_breaker_history_source(
                ledger_name="CANONICAL",
                through_session=through_session,
                query_cutoff=cutoff,
                calendar_resolver=calendar,
                policy=policy,
            )
            with journal.transaction() as transaction:
                actual = transaction.read_actual_replay(query_cutoff=cutoff)

            other_canonical = (
                other_journal._read_phase1_canonical_replay_source(
                    query_cutoff=other_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
            )
            phase1_forest_calls: list[tuple[object, ...]] = []
            direct_phase1_roots: list[object] = []
            replay_roots: list[object] = []
            original_phase1_forest = (
                journal_module._phase1_source_fingerprint_forest
            )
            original_phase1_fingerprint = (
                journal_module._phase1_source_fingerprint
            )
            original_replay_fingerprint = (
                journal_module._replay_source_fingerprint
            )

            def phase1_forest(roots: tuple[object, ...]) -> object:
                phase1_forest_calls.append(roots)
                return original_phase1_forest(roots)

            def phase1_fingerprint(source: object) -> object:
                direct_phase1_roots.append(source)
                return original_phase1_fingerprint(source)

            def replay_fingerprint(source: object) -> object:
                replay_roots.append(source)
                return original_replay_fingerprint(source)

            with (
                mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint_forest",
                    side_effect=phase1_forest,
                ),
                mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint",
                    side_effect=phase1_fingerprint,
                ),
                mock.patch.object(
                    journal_module,
                    "_replay_source_fingerprint",
                    side_effect=replay_fingerprint,
                ),
            ):
                review = journal._read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=calendar,
                    policy=policy,
                    disposition_source=disposition,
                    canonical_replay_source=canonical,
                    breaker_history_source=breaker,
                    actual_replay_source=actual,
                )

            supplied_roots = (disposition, canonical, breaker)
            supplied_batches = tuple(
                roots
                for roots in phase1_forest_calls
                if all(
                    any(item is supplied for item in roots)
                    for supplied in supplied_roots
                )
            )
            self.assertTrue(supplied_batches)
            for roots in supplied_batches:
                self.assertTrue(
                    all(
                        sum(item is supplied for item in roots) == 1
                        for supplied in supplied_roots
                    )
                )
            self.assertEqual(
                sum(item is actual for item in replay_roots),
                1,
            )
            self.assertGreaterEqual(
                sum(item is review for item in direct_phase1_roots),
                1,
            )

            with self.assertRaisesRegex(
                journal_module.InvalidJournalValue,
                "canonical replay is unverified",
            ):
                journal._read_phase1_adherence_review_source(
                    signal_id,
                    query_cutoff=cutoff,
                    calendar_resolver=calendar,
                    policy=policy,
                    disposition_source=disposition,
                    canonical_replay_source=breaker,
                    breaker_history_source=breaker,
                    actual_replay_source=actual,
                )
            for invalid_canonical in (
                other_canonical,
                copy.copy(canonical),
            ):
                with (
                    self.subTest(invalid_canonical=invalid_canonical),
                    self.assertRaisesRegex(
                        journal_module.InvalidJournalValue,
                        "canonical replay is unverified",
                    ),
                ):
                    journal._read_phase1_adherence_review_source(
                        signal_id,
                        query_cutoff=cutoff,
                        calendar_resolver=calendar,
                        policy=policy,
                        disposition_source=disposition,
                        canonical_replay_source=invalid_canonical,
                        breaker_history_source=breaker,
                        actual_replay_source=actual,
                    )

            callback_count = 0

            def mutate_canonical(statement: str) -> None:
                nonlocal callback_count
                if statement.strip().upper() != "PRAGMA DATA_VERSION":
                    return
                callback_count += 1
                if callback_count != 1:
                    return
                object.__setattr__(canonical, "source_digest", "0" * 64)

            journal._connection.set_trace_callback(mutate_canonical)
            try:
                with self.assertRaisesRegex(
                    journal_module.InvalidJournalValue,
                    "canonical replay is unverified",
                ):
                    journal._read_phase1_adherence_review_source(
                        signal_id,
                        query_cutoff=cutoff,
                        calendar_resolver=calendar,
                        policy=policy,
                        disposition_source=disposition,
                        canonical_replay_source=canonical,
                        breaker_history_source=breaker,
                        actual_replay_source=actual,
                    )
            finally:
                journal._connection.set_trace_callback(None)
            self.assertGreaterEqual(callback_count, 1)
            self.assertFalse(
                journal_module.is_verified_phase1_canonical_replay_source(
                    canonical
                )
            )

    def test_individual_verifiers_fingerprint_after_currentness_callbacks(
        self,
    ) -> None:
        def assert_trace_mutation_denied(
            journal: Journal,
            source: object,
            verifier: Callable[[object], bool],
        ) -> None:
            callback_count = 0

            def mutate_source(statement: str) -> None:
                nonlocal callback_count
                if statement.strip().upper() != "PRAGMA DATA_VERSION":
                    return
                callback_count += 1
                replacement = (
                    "0" * 64
                    if source.source_digest != "0" * 64
                    else "1" * 64
                )
                object.__setattr__(source, "source_digest", replacement)

            journal._connection.set_trace_callback(mutate_source)
            try:
                self.assertFalse(verifier(source))
            finally:
                journal._connection.set_trace_callback(None)
            self.assertEqual(callback_count, 1)
            self.assertFalse(verifier(source))

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "phase1.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            signal_source = journal._read_phase1_signal_source(
                signal_id,
                query_cutoff=cutoff,
            )
            assert_trace_mutation_denied(
                journal,
                signal_source,
                journal_module.is_verified_phase1_signal_source,
            )

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "action.sqlite3"
        ) as journal:
            action_source = _phase2_window_start_action(
                journal,
                session_date=date(2026, 8, 18),
                event_at=aware_et(date(2026, 8, 18), "09:45"),
            )
            assert_trace_mutation_denied(
                journal,
                action_source,
                journal_module.is_verified_journal_action_source,
            )

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "replay.sqlite3"
        ) as journal:
            with journal.transaction() as transaction:
                replay_source = transaction.read_actual_replay(
                    query_cutoff=datetime(
                        2026,
                        8,
                        18,
                        16,
                        tzinfo=timezone.utc,
                    )
                )
            assert_trace_mutation_denied(
                journal,
                replay_source,
                journal_module.is_verified_journal_replay_source,
            )

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "phase2.sqlite3"
        ) as journal:
            fee_source = journal.archive_phase2_fee_schedule(
                load_fee_schedule(
                    PROJECT_ROOT
                    / "tests/fixtures/options/reviewed-fees.json"
                ),
                archived_at=datetime(
                    2026,
                    8,
                    18,
                    16,
                    tzinfo=timezone.utc,
                ),
            )
            assert_trace_mutation_denied(
                journal,
                fee_source,
                journal_module.is_verified_phase2_fee_schedule_source,
            )

    def test_phase1_reader_registration_rejects_nested_mutation_during_currentness(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            with journal._phase1_publication_read_scope():
                first = journal._read_phase1_signal_source(
                    signal_id,
                    query_cutoff=cutoff,
                )
                publication = first.publication_source
                self.assertEqual(len(publication.source_observations), 8)
                data_version_calls = 0

                def mutate_nested_source(statement: str) -> None:
                    nonlocal data_version_calls
                    if statement.strip().upper() != "PRAGMA DATA_VERSION":
                        return
                    data_version_calls += 1
                    if data_version_calls == 3:
                        object.__setattr__(
                            publication,
                            "source_observations",
                            (),
                        )

                journal._connection.set_trace_callback(mutate_nested_source)
                try:
                    with self.assertRaises(journal_module.JournalError):
                        journal._read_phase1_signal_source(
                            signal_id,
                            query_cutoff=cutoff,
                        )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertEqual(data_version_calls, 3)
                self.assertFalse(
                    journal_module.is_verified_phase1_publication_source(
                        publication
                    )
                )

    def test_phase1_reader_registration_rejects_nested_source_subclasses(
        self,
    ) -> None:
        self.assertEqual(
            frozenset(
                parent_type
                for parent_type, _dependency_fields in (
                    journal_module._PHASE1_READER_DIRECT_DEPENDENCY_FIELDS
                )
            ),
            journal_module._PHASE1_SOURCE_TYPES,
        )
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            source = journal._read_phase1_signal_source(
                signal_id,
                query_cutoff=cutoff,
            )

            publication = source.publication_source

            class SameLayoutPublication(type(publication)):
                __slots__ = ()

            object.__setattr__(
                publication,
                "__class__",
                SameLayoutPublication,
            )
            with self.assertRaises(journal_module.JournalError):
                journal_module._phase1_reader_nested_authority_candidates(
                    source
                )

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            _signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            replay_source = journal._read_phase1_canonical_replay_source(
                query_cutoff=cutoff,
            )
            copied_signal = copy.copy(replay_source.signal_sources[0])
            object.__setattr__(
                replay_source,
                "signal_sources",
                (copied_signal, *replay_source.signal_sources[1:]),
            )
            with self.assertRaises(journal_module.JournalError):
                journal_module._phase1_reader_nested_authority_candidates(
                    replay_source
                )

    def test_mutable_generation_reset_cannot_revive_stale_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            fee_source = journal.archive_phase2_fee_schedule(
                load_fee_schedule(
                    PROJECT_ROOT
                    / "tests/fixtures/options/reviewed-fees.json"
                ),
                archived_at=datetime(
                    2026,
                    8,
                    18,
                    16,
                    tzinfo=timezone.utc,
                ),
            )
            issued_generation = journal._source_generation
            self.assertTrue(
                journal_module.is_verified_phase2_fee_schedule_source(
                    fee_source
                )
            )

            _phase2_window_start_action(
                journal,
                session_date=date(2026, 8, 18),
                event_at=aware_et(date(2026, 8, 18), "09:45"),
            )
            self.assertGreater(journal._source_generation, issued_generation)
            self.assertFalse(
                journal_module.is_verified_phase2_fee_schedule_source(
                    fee_source
                )
            )

            # The mutable compatibility counter is not an authority token.
            # Restoring it must not revive the stale exact source identity.
            journal._source_generation = issued_generation
            self.assertFalse(
                journal_module.is_verified_phase2_fee_schedule_source(
                    fee_source
                )
            )

    def test_nested_action_subclass_cannot_run_a_final_fingerprint_hook(
        self,
    ) -> None:
        session_date = date(2026, 8, 18)
        event_at = aware_et(session_date, "10:10")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            result = ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id="phase2-fingerprint-hook-account-check",
                    message_time=event_at + timedelta(seconds=1),
                    received_at=event_at + timedelta(seconds=2),
                    text=(
                        "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                        "unlogged_positions 0 AT 10:10 ET"
                    ),
                    session_date=session_date,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
            fee_source = journal.archive_phase2_fee_schedule(
                load_fee_schedule(
                    PROJECT_ROOT
                    / "tests/fixtures/options/reviewed-fees.json"
                ),
                archived_at=datetime(
                    2026,
                    8,
                    18,
                    16,
                    tzinfo=timezone.utc,
                ),
            )
            with journal.transaction() as transaction:
                action_source = transaction.read_action_source(
                    execution_event_id=result.actions[0].event_row_id,
                )
            account_check = action_source.account_check
            self.assertIsNotNone(account_check)
            assert account_check is not None
            hook_armed = False
            hook_fired = False

            class HookedAccountCheck(
                journal_module.JournalAccountCheckSource
            ):
                def __getattribute__(self, name: str) -> object:
                    nonlocal hook_fired
                    if hook_armed and name == "row_id":
                        hook_fired = True
                        replacement = (
                            "0" * 64
                            if fee_source.source_digest != "0" * 64
                            else "1" * 64
                        )
                        object.__setattr__(
                            fee_source,
                            "source_digest",
                            replacement,
                        )
                    return super().__getattribute__(name)

            hooked = HookedAccountCheck(
                **{
                    item.name: getattr(account_check, item.name)
                    for item in fields(
                        journal_module.JournalAccountCheckSource
                    )
                }
            )
            object.__setattr__(action_source, "account_check", hooked)
            self.assertFalse(
                journal_module.is_verified_journal_action_source(action_source)
            )

            hook_armed = True
            self.assertFalse(
                journal_module.phase2_sources_share_owner(
                    fee_source,
                    action_source,
                )
            )
            self.assertFalse(hook_fired)
            self.assertTrue(
                journal_module.is_verified_phase2_fee_schedule_source(
                    fee_source
                )
            )

    def test_datetime_equality_hook_cannot_mutate_an_earlier_batch_source(
        self,
    ) -> None:
        session_date = date(2026, 8, 18)
        event_at = aware_et(session_date, "10:10")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            result = ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id="phase2-datetime-equality-hook",
                    message_time=event_at + timedelta(seconds=1),
                    received_at=event_at + timedelta(seconds=2),
                    text=(
                        "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                        "unlogged_positions 0 AT 10:10 ET"
                    ),
                    session_date=session_date,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
            with journal.transaction() as transaction:
                replay_source = transaction.read_actual_replay(
                    query_cutoff=event_at + timedelta(minutes=1),
                )
            action_source = next(
                action
                for action in replay_source.actions
                if action.execution_event_id == result.actions[0].event_row_id
            )
            self.assertTrue(
                journal_module.phase2_sources_share_owner(
                    action_source,
                    replay_source,
                )
            )
            hook_fired = False

            class HookTimezone(tzinfo):
                def utcoffset(self, value: datetime | None) -> timedelta:
                    nonlocal hook_fired
                    del value
                    hook_fired = True
                    replacement = (
                        "0" * 64
                        if action_source.source_digest != "0" * 64
                        else "1" * 64
                    )
                    object.__setattr__(
                        action_source,
                        "source_digest",
                        replacement,
                    )
                    return timedelta(0)

                def dst(self, value: datetime | None) -> timedelta:
                    del value
                    return timedelta(0)

                def tzname(self, value: datetime | None) -> str:
                    del value
                    return "UTC"

            object.__setattr__(
                replay_source,
                "query_cutoff",
                replay_source.query_cutoff.replace(tzinfo=HookTimezone()),
            )
            self.assertFalse(
                journal_module.phase2_sources_share_owner(
                    action_source,
                    replay_source,
                )
            )
            self.assertFalse(hook_fired)
            self.assertTrue(
                journal_module.is_verified_journal_action_source(action_source)
            )
            self.assertFalse(
                journal_module.is_verified_journal_replay_source(replay_source)
            )

    def test_promotion_rechecks_window_after_nested_currentness_callbacks(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            journal.record_phase1_adherence(
                signal_id,
                query_cutoff=cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            source = journal.read_phase1_validation_window_source(
                "1" * 64,
                through_session=_SESSION,
                query_cutoff=cutoff,
                calendar_resolver=_calendar(),
            )
            callback_count = 0

            def mutate_window(statement: str) -> None:
                nonlocal callback_count
                if statement.strip().upper() != "PRAGMA DATA_VERSION":
                    return
                callback_count += 1
                if callback_count != 2:
                    return
                replacement = (
                    "0" * 64
                    if source.source_digest != "0" * 64
                    else "1" * 64
                )
                object.__setattr__(source, "source_digest", replacement)

            journal._connection.set_trace_callback(mutate_window)
            try:
                with self.assertRaises(
                    validation_module.ValidationError
                ):
                    validation_module._issue_phase1_promotion_from_journal_source(
                        source,
                        calendar_resolver=_calendar(),
                    )
            finally:
                journal._connection.set_trace_callback(None)
            self.assertGreaterEqual(callback_count, 2)
            self.assertFalse(
                journal_module.is_verified_phase1_validation_window_source(
                    source
                )
            )

    def test_source_merkle_seals_handle_depth_5000_iteratively(self) -> None:
        @dataclass(frozen=True, slots=True)
        class DeepNode:
            child: object

        @dataclass(frozen=True, slots=True)
        class OpaqueEnvelope:
            payload: object = field(compare=False)

        root: object = "leaf"
        for _ in range(5_000):
            root = DeepNode(root)

        phase1 = journal_module._phase1_source_fingerprint(root)
        opaque = journal_module._phase2_opaque_identity_fingerprint(
            OpaqueEnvelope(root)
        )
        for seal in (phase1, opaque):
            self.assertIs(type(seal), journal_module._SourceFingerprintSeal)
            self.assertIs(type(seal.digest), bytes)
            self.assertEqual(len(seal.digest), 32)

    def test_source_fingerprint_seals_resist_object_setattr_and_spoofs(
        self,
    ) -> None:
        for attribute, replacement in (
            ("digest", b"0" * 32),
            ("identity_anchors", (object(),)),
        ):
            with self.subTest(attribute=attribute):
                seal = journal_module._phase1_source_fingerprint(
                    ("immutable", attribute)
                )
                original_digest = seal.digest
                original_anchors = seal.identity_anchors
                with self.assertRaises((AttributeError, TypeError)):
                    object.__setattr__(seal, attribute, replacement)
                self.assertEqual(seal.digest, original_digest)
                self.assertIs(seal.identity_anchors, original_anchors)

        reference = journal_module._phase1_source_fingerprint(("reference",))
        try:
            class SpoofedSeal(journal_module._SourceFingerprintSeal):
                pass
        except TypeError:
            return
        spoofed = SpoofedSeal(reference.digest, reference.identity_anchors)
        self.assertFalse(
            journal_module._source_fingerprint_seals_equal(spoofed, spoofed)
        )
        self.assertFalse(journal_module._fingerprints_equal(spoofed, spoofed))

    def test_phase1_fingerprint_forest_matches_independent_root_seals(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class SharedLeaf:
            value: str

        @dataclass(frozen=True, slots=True)
        class LeftRoot:
            child: object

        @dataclass(frozen=True, slots=True)
        class RightRoot:
            child: object

        shared = SharedLeaf("shared")
        left = LeftRoot(shared)
        right = RightRoot(shared)
        for roots in (
            (left, right, left),
            (right, left, left),
        ):
            with self.subTest(order=tuple(type(root).__name__ for root in roots)):
                independent = tuple(
                    journal_module._phase1_source_fingerprint(root)
                    for root in roots
                )
                forest = self._required_phase1_fingerprint_forest()(roots)
                self.assertIs(type(forest), tuple)
                self.assertEqual(len(forest), len(roots))
                for expected, actual in zip(
                    independent,
                    forest,
                    strict=True,
                ):
                    self.assertIs(
                        type(actual),
                        journal_module._SourceFingerprintSeal,
                    )
                    self.assertTrue(
                        journal_module._source_fingerprint_seals_equal(
                            expected,
                            actual,
                        )
                    )
                duplicate_indices = tuple(
                    ordinal
                    for ordinal, root in enumerate(roots)
                    if root is left
                )
                self.assertEqual(len(duplicate_indices), 2)
                self.assertTrue(
                    journal_module._source_fingerprint_seals_equal(
                        forest[duplicate_indices[0]],
                        forest[duplicate_indices[1]],
                    )
                )

    def test_phase1_fingerprint_forest_keeps_root_identity_anchors_isolated(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class OpaqueValue:
            value: str

        @dataclass(frozen=True, slots=True)
        class IdentityRoot:
            at: datetime
            opaque: object = field(compare=False)

        first_at = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        second_at = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        first_opaque = OpaqueValue("same")
        second_opaque = OpaqueValue("same")
        first_root = IdentityRoot(first_at, first_opaque)
        second_root = IdentityRoot(second_at, second_opaque)
        self.assertEqual(first_root, second_root)
        self.assertIsNot(first_at, second_at)
        self.assertIsNot(first_opaque, second_opaque)

        forest = self._required_phase1_fingerprint_forest()(
            (first_root, second_root)
        )
        independent = (
            journal_module._phase1_source_fingerprint(first_root),
            journal_module._phase1_source_fingerprint(second_root),
        )
        for expected, actual in zip(independent, forest, strict=True):
            self.assertTrue(
                journal_module._source_fingerprint_seals_equal(expected, actual)
            )

        first_anchors = forest[0].identity_anchors
        second_anchors = forest[1].identity_anchors
        self.assertIsNot(first_anchors, second_anchors)
        self.assertTrue(any(anchor is first_at for anchor in first_anchors))
        self.assertTrue(any(anchor is first_opaque for anchor in first_anchors))
        self.assertFalse(any(anchor is second_at for anchor in first_anchors))
        self.assertFalse(any(anchor is second_opaque for anchor in first_anchors))
        self.assertTrue(any(anchor is second_at for anchor in second_anchors))
        self.assertTrue(any(anchor is second_opaque for anchor in second_anchors))
        self.assertFalse(any(anchor is first_at for anchor in second_anchors))
        self.assertFalse(any(anchor is first_opaque for anchor in second_anchors))
        self.assertFalse(
            journal_module._source_fingerprint_seals_equal(forest[0], forest[1])
        )

    def test_single_root_phase1_fingerprint_matches_forest_without_calling_forest(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class SlotNode:
            value: object

        @dataclass(frozen=True)
        class DictRoot:
            label: str
            child: object

        @dataclass(frozen=True, slots=True)
        class SharedRoot:
            left: object
            right: object

        @dataclass(frozen=True, slots=True)
        class OpaqueRoot:
            first_at: datetime
            second_at: datetime
            first_opaque: object = field(compare=False)
            second_opaque: object = field(compare=False)
            amount: Decimal = field(compare=False)

        shared = SlotNode("shared")
        repeated_at = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        repeated_opaque = object()
        amount = Decimal("12.50")
        opaque_root = OpaqueRoot(
            repeated_at,
            repeated_at,
            repeated_opaque,
            repeated_opaque,
            amount,
        )
        self.assertIs(opaque_root.first_at, opaque_root.second_at)
        self.assertIs(opaque_root.first_opaque, opaque_root.second_opaque)

        cases = (
            ("slots", SlotNode("slots")),
            ("dict-backed", DictRoot("dict", SlotNode("child"))),
            ("shared-dag", SharedRoot(shared, shared)),
            ("opaque-decimal-duplicate-anchors", opaque_root),
        )
        original_forest = self._required_phase1_fingerprint_forest()

        for case, root in cases:
            with self.subTest(case=case):
                expected = original_forest((root,))[0]
                with mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint_forest",
                    wraps=original_forest,
                ) as forest:
                    actual = journal_module._phase1_source_fingerprint(root)

                self.assertIs(
                    type(actual),
                    journal_module._SourceFingerprintSeal,
                )
                expected_digest = tuple.__getitem__(expected, 0)
                actual_digest = tuple.__getitem__(actual, 0)
                self.assertEqual(actual_digest, expected_digest)
                expected_anchors = tuple.__getitem__(expected, 1)
                actual_anchors = tuple.__getitem__(actual, 1)
                self.assertIs(type(actual_anchors), tuple)
                self.assertEqual(len(actual_anchors), len(expected_anchors))
                self.assertTrue(
                    all(
                        actual_anchor is expected_anchor
                        for actual_anchor, expected_anchor in zip(
                            actual_anchors,
                            expected_anchors,
                            strict=True,
                        )
                    )
                )
                if case == "opaque-decimal-duplicate-anchors":
                    self.assertEqual(
                        sum(anchor is repeated_at for anchor in actual_anchors),
                        1,
                    )
                    self.assertEqual(
                        sum(
                            anchor is repeated_opaque
                            for anchor in actual_anchors
                        ),
                        1,
                    )
                    self.assertEqual(
                        sum(anchor is amount for anchor in actual_anchors),
                        1,
                    )
                self.assertEqual(
                    forest.call_count,
                    0,
                    "single-root fingerprint delegated to forest traversal",
                )

    def test_phase1_fingerprint_forest_visits_shared_dataclasses_once(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class SharedNode:
            child: object

        @dataclass(frozen=True, slots=True)
        class ForestRoot:
            label: int
            child: object

        shared_depth = 32
        root_count = 8
        shared: object = SharedNode("leaf")
        for _ in range(shared_depth - 1):
            shared = SharedNode(shared)
        roots = tuple(ForestRoot(ordinal, shared) for ordinal in range(root_count))
        unique_forest_nodes = shared_depth + root_count + root_count + 1
        describe_node = self._required_phase1_forest_describe_node()

        with (
            mock.patch.object(
                journal_module,
                "_phase1_forest_describe_node",
                wraps=describe_node,
            ) as independent_descriptions,
            mock.patch.object(
                journal_module,
                "fields",
                side_effect=AssertionError(
                    "Phase 1 forest used dynamic dataclass metadata"
                ),
            ),
        ):
            independent = tuple(
                journal_module._phase1_source_fingerprint(root) for root in roots
            )
        independent_visits = independent_descriptions.call_count

        with (
            mock.patch.object(
                journal_module,
                "_phase1_forest_describe_node",
                wraps=describe_node,
            ) as forest_descriptions,
            mock.patch.object(
                journal_module,
                "fields",
                side_effect=AssertionError(
                    "Phase 1 forest used dynamic dataclass metadata"
                ),
            ),
        ):
            forest = self._required_phase1_fingerprint_forest()(roots)
        forest_visits = forest_descriptions.call_count

        for expected, actual in zip(independent, forest, strict=True):
            self.assertTrue(
                journal_module._source_fingerprint_seals_equal(expected, actual)
            )
        self.assertLessEqual(forest_visits, unique_forest_nodes)
        self.assertGreater(independent_visits, unique_forest_nodes * 4)
        self.assertLess(forest_visits * 3, independent_visits)

    def test_owner_bypasses_mutable_phase1_factory_call_below_forest_batch_threshold(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class SharedChild:
            value: str

        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class AuthorityRoot:
            label: int
            child: object

        original_fingerprint = journal_module._phase1_source_fingerprint
        original_forest = self._required_phase1_fingerprint_forest()

        for root_count in range(1, 4):
            with (
                self.subTest(root_count=root_count),
                tempfile.TemporaryDirectory() as directory,
                Journal.open(Path(directory) / "journal.sqlite3") as journal,
            ):
                shared = SharedChild("original")
                roots = tuple(
                    AuthorityRoot(ordinal, shared)
                    for ordinal in range(root_count)
                )
                expected = tuple(
                    journal_module._phase1_source_fingerprint(root)
                    for root in roots
                )
                factories = tuple(
                    self._required_phase1_fingerprint_factory(root)
                    for root in roots
                )
                factory_type = type(factories[0])
                factory_calls: list[object] = []

                def counted_factory_call(factory: object) -> object:
                    factory_calls.append(factory)
                    object.__setattr__(shared, "value", "mutated")
                    source = tuple.__getitem__(factory, 0)
                    return original_fingerprint(source)

                registry: journal_module._JournalSourceRegistry = {}
                candidates = tuple(
                    self._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )

                with mock.patch.object(
                    factory_type,
                    "__call__",
                    new=counted_factory_call,
                ), mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint",
                    wraps=original_fingerprint,
                ) as fingerprint, mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint_forest",
                    wraps=original_forest,
                ) as forest:
                    owner = (
                        journal_module._current_journal_source_authority_owner(
                            candidates
                        )
                    )

                self.assertIs(owner, journal)
                self.assertEqual(factory_calls, [])
                self.assertEqual(shared.value, "original")
                self.assertEqual(fingerprint.call_count, root_count)
                self.assertEqual(forest.call_count, 0)

    def test_owner_batches_four_shared_phase1_roots_into_two_fresh_forests_and_rejects_interpass_mutation(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class MutableChild:
            value: str

        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class AuthorityRoot:
            label: str
            child: object

        shared = MutableChild("original")
        roots = tuple(
            AuthorityRoot(label, shared)
            for label in ("first", "second", "third", "fourth")
        )
        expected = tuple(
            journal_module._phase1_source_fingerprint(root) for root in roots
        )
        phase1_factories = tuple(
            self._required_phase1_fingerprint_factory(root) for root in roots
        )
        original_forest = self._required_phase1_fingerprint_forest()
        forest_calls: list[tuple[object, ...]] = []

        def mutate_between_forest_passes(
            sources: tuple[object, ...],
        ) -> tuple[journal_module._SourceFingerprintSeal, ...]:
            seals = original_forest(sources)
            forest_calls.append(tuple(sources))
            if len(forest_calls) == 1:
                object.__setattr__(shared, "value", "mutated")
            return seals

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            registry: journal_module._JournalSourceRegistry = {}
            candidates = tuple(
                self._synthetic_authority_candidate(
                    journal,
                    registry,
                    source,
                    fingerprint,
                    factory,
                )
                for source, fingerprint, factory in zip(
                    roots,
                    expected,
                    phase1_factories,
                    strict=True,
                )
            )
            with mock.patch.object(
                journal_module,
                "_phase1_source_fingerprint_forest",
                side_effect=mutate_between_forest_passes,
            ):
                owner = journal_module._current_journal_source_authority_owner(
                    candidates
                )

        self.assertIsNone(owner)
        self.assertEqual(len(forest_calls), 2)
        for call_roots in forest_calls:
            self.assertEqual(len(call_roots), len(roots))
            self.assertTrue(
                all(
                    actual is expected_root
                    for actual, expected_root in zip(
                        call_roots,
                        roots,
                        strict=True,
                    )
                )
            )

    def test_owner_second_forest_later_metaclass_cannot_mutate_completed_shared_root(
        self,
    ) -> None:
        attack_state: dict[str, object] = {
            "armed": False,
            "callback_count": 0,
            "fired_on_forest_call": None,
        }
        forest_call_count = 0

        class CallbackMeta(type):
            def __getattribute__(cls, name: str) -> object:
                if name == "__dataclass_fields__" and attack_state["armed"]:
                    attack_state["callback_count"] = (
                        int(attack_state["callback_count"]) + 1
                    )
                    if attack_state["fired_on_forest_call"] is None:
                        attack_state["fired_on_forest_call"] = forest_call_count
                        object.__setattr__(shared, "value", "mutated")
                return type.__getattribute__(cls, name)

        @dataclass(frozen=True, slots=True)
        class MutableChild:
            value: str

        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class EarlierRoot:
            label: str
            child: object

        @dataclass(
            frozen=True,
            slots=True,
            weakref_slot=True,
        )
        class LaterRoot(metaclass=CallbackMeta):
            label: str
            child: object

        shared = MutableChild("original")
        roots = (
            EarlierRoot("earlier", shared),
            EarlierRoot("middle-1", shared),
            EarlierRoot("middle-2", shared),
            LaterRoot("later", shared),
        )
        expected = tuple(
            journal_module._phase1_source_fingerprint(root) for root in roots
        )
        factories = tuple(
            self._required_phase1_fingerprint_factory(root) for root in roots
        )
        original_forest = self._required_phase1_fingerprint_forest()

        def arm_only_during_second_forest(
            sources: tuple[object, ...],
        ) -> tuple[journal_module._SourceFingerprintSeal, ...]:
            nonlocal forest_call_count
            forest_call_count += 1
            attack_state["armed"] = forest_call_count == 2
            try:
                return original_forest(sources)
            finally:
                attack_state["armed"] = False

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            registry: journal_module._JournalSourceRegistry = {}
            candidates = tuple(
                self._synthetic_authority_candidate(
                    journal,
                    registry,
                    source,
                    fingerprint,
                    factory,
                )
                for source, fingerprint, factory in zip(
                    roots,
                    expected,
                    factories,
                    strict=True,
                )
            )
            with mock.patch.object(
                journal_module,
                "_phase1_source_fingerprint_forest",
                side_effect=arm_only_during_second_forest,
            ):
                owner = journal_module._current_journal_source_authority_owner(
                    candidates
                )

            callback_count = int(attack_state["callback_count"])
            self.assertEqual(forest_call_count, 2)
            self.assertEqual(callback_count, 0)
            self.assertIsNone(attack_state["fired_on_forest_call"])
            self.assertEqual(shared.value, "original")
            self.assertIs(owner, journal)

    def test_owner_second_forest_field_compare_descriptor_cannot_mutate_completed_shared_root(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import json
            import sys
            import tempfile
            from dataclasses import Field, dataclass
            from pathlib import Path
            from types import MemberDescriptorType
            from unittest import mock
            from weakref import ref

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import Journal


            @dataclass(frozen=True, slots=True)
            class MutableChild:
                value: str


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class EarlierRoot:
                label: str
                child: object


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class LaterRoot:
                label: str
                child: object


            shared = MutableChild("original")
            roots = (
                EarlierRoot("earlier", shared),
                EarlierRoot("middle-1", shared),
                EarlierRoot("middle-2", shared),
                LaterRoot("later", shared),
            )
            expected = tuple(
                journal_module._phase1_source_fingerprint(root)
                for root in roots
            )
            factories = tuple(
                journal_module._Phase1SourceFingerprintFactory(root)
                for root in roots
            )
            target_field = LaterRoot.__dataclass_fields__["child"]
            field_type = type(target_field)
            if field_type is not Field:
                raise AssertionError("target metadata is not an exact Field")
            original_compare = type.__getattribute__(field_type, "compare")
            if type(original_compare) is not MemberDescriptorType:
                raise AssertionError("Field.compare is not a member descriptor")

            attack_state = {
                "armed": False,
                "callback_count": 0,
                "fired_on_forest_call": None,
            }
            forest_call_count = 0


            class CallbackCompare:
                def __get__(self, instance, owner=None):
                    if instance is None:
                        return self
                    if attack_state["armed"] and instance is target_field:
                        attack_state["callback_count"] += 1
                        if attack_state["fired_on_forest_call"] is None:
                            attack_state["fired_on_forest_call"] = (
                                forest_call_count
                            )
                            object.__setattr__(
                                shared,
                                "value",
                                "mutated",
                            )
                    return MemberDescriptorType.__get__(
                        original_compare,
                        instance,
                        field_type,
                    )


            def candidate(journal, registry, source, fingerprint, factory):
                owner_reference = ref(journal)
                total_changes = (
                    journal_module._journal_source_authority_total_changes(
                        journal
                    )
                )
                data_version = journal._source_authority_data_version()
                issued = (
                    ref(source),
                    fingerprint,
                    owner_reference,
                    total_changes,
                    data_version,
                )
                registry[id(source)] = issued
                return (
                    registry,
                    source,
                    journal,
                    total_changes,
                    data_version,
                    fingerprint,
                    factory,
                    issued,
                )


            original_forest = journal_module._phase1_source_fingerprint_forest


            def arm_only_during_second_forest(sources):
                global forest_call_count
                forest_call_count += 1
                attack_state["armed"] = forest_call_count == 2
                try:
                    return original_forest(sources)
                finally:
                    attack_state["armed"] = False


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )
                setattr(field_type, "compare", CallbackCompare())
                try:
                    with mock.patch.object(
                        journal_module,
                        "_phase1_source_fingerprint_forest",
                        side_effect=arm_only_during_second_forest,
                    ):
                        owner = (
                            journal_module
                            ._current_journal_source_authority_owner(candidates)
                        )
                finally:
                    setattr(field_type, "compare", original_compare)

                print(
                    json.dumps(
                        {
                            "callback_count": attack_state["callback_count"],
                            "fired_on_forest_call": attack_state[
                                "fired_on_forest_call"
                            ],
                            "forest_call_count": forest_call_count,
                            "owner_granted": owner is journal,
                            "shared_value": shared.value,
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertEqual(outcome["forest_call_count"], 2)
        self.assertEqual(outcome["callback_count"], 0)
        self.assertIsNone(outcome["fired_on_forest_call"])
        self.assertEqual(outcome["shared_value"], "original")
        self.assertTrue(outcome["owner_granted"])

    def test_owner_second_forest_decimal_tuple_descriptor_is_never_invoked(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import json
            import sys
            import tempfile
            from dataclasses import dataclass, field
            from decimal import Decimal
            from pathlib import Path
            from unittest import mock

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import Journal
            from tests.integration.test_phase2_gate import (
                Phase2JournalSourceContractTests as Contract,
            )


            attack_state = {
                "armed": False,
                "callback_count": 0,
                "fired_on_forest_call": None,
            }
            forest_call_count = 0


            @dataclass(frozen=True, slots=True)
            class MutableChild:
                value: str


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class EarlierRoot:
                label: str
                child: object


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class LaterRoot:
                label: str
                child: object
                amount: Decimal = field(compare=False)


            shared = MutableChild("original")
            roots = (
                EarlierRoot("earlier", shared),
                EarlierRoot("middle-1", shared),
                EarlierRoot("middle-2", shared),
                LaterRoot("later", shared, Decimal("12.50")),
            )
            expected = tuple(
                journal_module._phase1_source_fingerprint(root)
                for root in roots
            )
            factories = tuple(
                Contract._required_phase1_fingerprint_factory(root)
                for root in roots
            )
            decimal_tuple_type = type(Decimal("0").as_tuple())
            if not issubclass(decimal_tuple_type, tuple):
                raise AssertionError("DecimalTuple is not tuple-backed")
            original_exponent = type.__getattribute__(
                decimal_tuple_type,
                "exponent",
            )


            class CallbackExponent:
                def __get__(self, instance, owner=None):
                    if instance is None:
                        return self
                    if attack_state["armed"]:
                        attack_state["callback_count"] += 1
                        if attack_state["fired_on_forest_call"] is None:
                            attack_state["fired_on_forest_call"] = (
                                forest_call_count
                            )
                            object.__setattr__(
                                shared,
                                "value",
                                "mutated",
                            )
                    return tuple.__getitem__(instance, 2)


            original_forest = Contract._required_phase1_fingerprint_forest()


            def arm_only_during_second_forest(sources):
                global forest_call_count
                forest_call_count += 1
                attack_state["armed"] = forest_call_count == 2
                try:
                    return original_forest(sources)
                finally:
                    attack_state["armed"] = False


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    Contract._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )
                setattr(decimal_tuple_type, "exponent", CallbackExponent())
                try:
                    with mock.patch.object(
                        journal_module,
                        "_phase1_source_fingerprint_forest",
                        side_effect=arm_only_during_second_forest,
                    ):
                        owner = (
                            journal_module
                            ._current_journal_source_authority_owner(candidates)
                        )
                finally:
                    setattr(
                        decimal_tuple_type,
                        "exponent",
                        original_exponent,
                    )

                print(
                    json.dumps(
                        {
                            "callback_count": attack_state["callback_count"],
                            "fired_on_forest_call": attack_state[
                                "fired_on_forest_call"
                            ],
                            "forest_call_count": forest_call_count,
                            "owner_granted": owner is journal,
                            "shared_value": shared.value,
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertEqual(outcome["forest_call_count"], 2)
        self.assertEqual(
            outcome["callback_count"],
            0,
            "DecimalTuple.exponent dispatched a mutable descriptor during "
            "authority verification",
        )
        self.assertIsNone(outcome["fired_on_forest_call"])
        self.assertEqual(outcome["shared_value"], "original")
        self.assertTrue(outcome["owner_granted"])

    def test_owner_later_seal_descriptor_cannot_mutate_completed_shared_root(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import json
            import sys
            import tempfile
            from dataclasses import dataclass
            from pathlib import Path
            from unittest import mock

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import Journal
            from tests.integration.test_phase2_gate import (
                Phase2JournalSourceContractTests as Contract,
            )


            attack_state = {
                "armed": False,
                "callback_count": 0,
                "forest_call_count": 0,
            }
            forest_results = []


            @dataclass(frozen=True, slots=True)
            class MutableChild:
                value: str


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class AuthorityRoot:
                label: str
                child: object


            shared = MutableChild("original")
            roots = tuple(
                AuthorityRoot(label, shared)
                for label in ("earlier", "middle-1", "middle-2", "later")
            )
            expected = tuple(
                journal_module._phase1_source_fingerprint(root)
                for root in roots
            )
            factories = tuple(
                Contract._required_phase1_fingerprint_factory(root)
                for root in roots
            )
            seal_type = journal_module._SourceFingerprintSeal
            original_digest = type.__getattribute__(seal_type, "digest")


            class CallbackDigest:
                def __get__(self, instance, owner=None):
                    if instance is None:
                        return self
                    target = (
                        forest_results[0][-1]
                        if len(forest_results) == 2
                        else None
                    )
                    if attack_state["armed"] and instance is target:
                        attack_state["callback_count"] += 1
                        object.__setattr__(
                            shared,
                            "value",
                            "mutated",
                        )
                    return tuple.__getitem__(instance, 0)


            original_forest = Contract._required_phase1_fingerprint_forest()


            def capture_two_forest_passes(sources):
                result = original_forest(sources)
                forest_results.append(result)
                attack_state["forest_call_count"] += 1
                if len(forest_results) == 2:
                    attack_state["armed"] = True
                return result


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    Contract._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )
                setattr(seal_type, "digest", CallbackDigest())
                try:
                    with mock.patch.object(
                        journal_module,
                        "_phase1_source_fingerprint_forest",
                        side_effect=capture_two_forest_passes,
                    ):
                        owner = (
                            journal_module
                            ._current_journal_source_authority_owner(candidates)
                        )
                finally:
                    attack_state["armed"] = False
                    setattr(seal_type, "digest", original_digest)

                print(
                    json.dumps(
                        {
                            "callback_count": attack_state["callback_count"],
                            "forest_call_count": attack_state[
                                "forest_call_count"
                            ],
                            "owner_granted": owner is journal,
                            "shared_value": shared.value,
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertEqual(outcome["forest_call_count"], 2)
        self.assertEqual(outcome["callback_count"], 0)
        self.assertEqual(outcome["shared_value"], "original")
        self.assertTrue(outcome["owner_granted"])

    def test_owner_second_forest_instance_dict_collision_cannot_mutate_completed_shared_root(
        self,
    ) -> None:
        attack_state: dict[str, object] = {
            "armed": False,
            "callback_count": 0,
            "fired_on_forest_call": None,
        }
        forest_call_count = 0

        @dataclass(frozen=True, slots=True)
        class MutableChild:
            value: str

        class CollidingKey:
            def __hash__(self) -> int:
                return hash("child")

            def __eq__(self, other: object) -> bool:
                if type(other) is not str or other != "child":
                    return False
                if attack_state["armed"]:
                    attack_state["callback_count"] = (
                        int(attack_state["callback_count"]) + 1
                    )
                    if attack_state["fired_on_forest_call"] is None:
                        attack_state["fired_on_forest_call"] = (
                            forest_call_count
                        )
                        object.__setattr__(shared, "value", "mutated")
                return True

        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class EarlierRoot:
            label: str
            child: object

        @dataclass(frozen=True)
        class LaterDictRoot:
            label: str
            child: object

        shared = MutableChild("original")
        later = LaterDictRoot("later", shared)
        roots = (
            EarlierRoot("earlier", shared),
            EarlierRoot("middle-1", shared),
            EarlierRoot("middle-2", shared),
            later,
        )
        expected = tuple(
            journal_module._phase1_source_fingerprint(root) for root in roots
        )
        factories = tuple(
            self._required_phase1_fingerprint_factory(root) for root in roots
        )

        instance_values = object.__getattribute__(later, "__dict__")
        self.assertIs(type(instance_values), dict)
        child = dict.pop(instance_values, "child")
        collision = CollidingKey()
        dict.__setitem__(instance_values, collision, child)
        self.assertTrue(
            any(key is collision for key in dict.keys(instance_values))
        )
        self.assertTrue(
            any(type(key) is not str for key in dict.keys(instance_values))
        )
        original_forest = self._required_phase1_fingerprint_forest()

        def arm_only_during_second_forest(
            sources: tuple[object, ...],
        ) -> tuple[journal_module._SourceFingerprintSeal, ...]:
            nonlocal forest_call_count
            forest_call_count += 1
            attack_state["armed"] = forest_call_count == 2
            try:
                return original_forest(sources)
            finally:
                attack_state["armed"] = False

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            registry: journal_module._JournalSourceRegistry = {}
            candidates = tuple(
                self._synthetic_authority_candidate(
                    journal,
                    registry,
                    source,
                    fingerprint,
                    factory,
                )
                for source, fingerprint, factory in zip(
                    roots,
                    expected,
                    factories,
                    strict=True,
                )
            )
            with mock.patch.object(
                journal_module,
                "_phase1_source_fingerprint_forest",
                side_effect=arm_only_during_second_forest,
            ):
                owner = journal_module._current_journal_source_authority_owner(
                    candidates
                )

        callback_count = int(attack_state["callback_count"])
        self.assertEqual(callback_count, 0)
        self.assertIsNone(attack_state["fired_on_forest_call"])
        self.assertEqual(shared.value, "original")
        self.assertIsNone(owner)

    def test_owner_second_forest_class_namespace_collision_cannot_mutate_completed_shared_root(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import gc
            import json
            import sys
            import tempfile
            import warnings
            from dataclasses import dataclass
            from pathlib import Path
            from unittest import mock

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import Journal
            from tests.integration.test_phase2_gate import (
                Phase2JournalSourceContractTests as Contract,
            )


            attack_state = {
                "armed": False,
                "callback_count": 0,
                "fired_on_forest_call": None,
            }
            forest_call_count = 0


            @dataclass(frozen=True, slots=True)
            class MutableChild:
                value: str


            @dataclass(frozen=True, slots=True, weakref_slot=True)
            class EarlierRoot:
                label: str
                child: object


            shared = MutableChild("original")


            class CollidingKey:
                def __hash__(self):
                    return hash("child")

                def __eq__(self, other):
                    if type(other) is not str or other != "child":
                        return False
                    if attack_state["armed"]:
                        attack_state["callback_count"] += 1
                        if attack_state["fired_on_forest_call"] is None:
                            attack_state["fired_on_forest_call"] = (
                                forest_call_count
                            )
                            object.__setattr__(
                                shared,
                                "value",
                                "mutated",
                            )
                    return True


            collision = CollidingKey()
            namespace = {
                "__module__": __name__,
                "__annotations__": {
                    "label": str,
                    "child": object,
                },
                collision: None,
            }
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                LaterRoot = type("LaterRoot", (), namespace)
            matching_warnings = tuple(
                warning
                for warning in caught
                if type(warning.message) is RuntimeWarning
                and "non-string key" in str(warning.message)
            )
            if len(matching_warnings) != 1:
                raise AssertionError(
                    "class collision fixture did not emit its exact warning"
                )

            LaterRoot = dataclass(frozen=True)(LaterRoot)
            later = LaterRoot("later", shared)
            class_namespace = type.__getattribute__(LaterRoot, "__dict__")
            namespace_dicts = tuple(
                value
                for value in gc.get_referents(class_namespace)
                if type(value) is dict
            )
            if len(namespace_dicts) != 1:
                raise AssertionError(
                    "class mappingproxy backing dictionary is unavailable"
                )
            mutable_namespace = namespace_dicts[0]
            dict.__delitem__(mutable_namespace, collision)

            roots = (
                EarlierRoot("earlier", shared),
                EarlierRoot("middle-1", shared),
                EarlierRoot("middle-2", shared),
                later,
            )
            expected = tuple(
                journal_module._phase1_source_fingerprint(root)
                for root in roots
            )
            factories = tuple(
                Contract._required_phase1_fingerprint_factory(root)
                for root in roots
            )
            dict.__setitem__(mutable_namespace, collision, None)
            if not any(key is collision for key in class_namespace.keys()):
                raise AssertionError("class collision key was not retained")
            original_forest = Contract._required_phase1_fingerprint_forest()


            def arm_only_during_second_forest(sources):
                global forest_call_count
                forest_call_count += 1
                attack_state["armed"] = forest_call_count == 2
                try:
                    return original_forest(sources)
                finally:
                    attack_state["armed"] = False


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    Contract._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )
                with mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint_forest",
                    side_effect=arm_only_during_second_forest,
                ):
                    owner = (
                        journal_module
                        ._current_journal_source_authority_owner(candidates)
                    )

                print(
                    json.dumps(
                        {
                            "callback_count": attack_state["callback_count"],
                            "fired_on_forest_call": attack_state[
                                "fired_on_forest_call"
                            ],
                            "forest_call_count": forest_call_count,
                            "owner_granted": owner is journal,
                            "shared_value": shared.value,
                            "warning_count": len(matching_warnings),
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertEqual(outcome["warning_count"], 1)
        self.assertEqual(outcome["callback_count"], 0)
        self.assertIsNone(outcome["fired_on_forest_call"])
        self.assertEqual(outcome["shared_value"], "original")
        self.assertFalse(outcome["owner_granted"])

    def test_phase2_owner_second_field_descriptor_cannot_mutate_first_candidate(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import json
            import sys
            import tempfile
            from dataclasses import Field
            from datetime import date, datetime, timezone
            from pathlib import Path
            from types import MemberDescriptorType

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import Journal, Phase2FeeScheduleSource
            from tests.integration.test_phase2_gate import (
                Phase2JournalSourceContractTests as Contract,
            )


            def fee_source(ordinal):
                return Phase2FeeScheduleSource(
                    row_id=ordinal,
                    schedule_id=f"schedule-{ordinal}",
                    effective_session=date(2026, 8, ordinal),
                    reviewed_at=datetime(
                        2026,
                        8,
                        ordinal,
                        12,
                        tzinfo=timezone.utc,
                    ),
                    currency="USD",
                    contract_multiplier=100,
                    entry_fee_per_contract_micros=1,
                    exit_fee_per_contract_micros=2,
                    close_fee_reserve_per_contract_micros=3,
                    source_sha256=str(ordinal) * 64,
                    schedule_digest=str(ordinal + 2) * 64,
                    reviewed_bytes=f"schedule-{ordinal}".encode(),
                    archived_at=datetime(
                        2026,
                        8,
                        ordinal,
                        13,
                        tzinfo=timezone.utc,
                    ),
                    row_references=(),
                    source_digest=str(ordinal + 4) * 64,
                )


            sources = (fee_source(1), fee_source(2))
            expected = tuple(
                journal_module._phase2_source_fingerprint(source)
                for source in sources
            )
            target_field = Phase2FeeScheduleSource.__dataclass_fields__[
                "currency"
            ]
            field_type = type(target_field)
            if field_type is not Field:
                raise AssertionError("target metadata is not an exact Field")
            original_compare = type.__getattribute__(field_type, "compare")
            if type(original_compare) is not MemberDescriptorType:
                raise AssertionError("Field.compare is not a member descriptor")

            attack_state = {
                "armed": False,
                "callback_count": 0,
                "factory_call_count": 0,
                "fired_on_factory_call": None,
            }


            class CallbackCompare:
                def __get__(self, instance, owner=None):
                    if instance is None:
                        return self
                    if attack_state["armed"] and instance is target_field:
                        attack_state["callback_count"] += 1
                        if attack_state["fired_on_factory_call"] is None:
                            attack_state["fired_on_factory_call"] = (
                                attack_state["factory_call_count"]
                            )
                            object.__setattr__(
                                sources[0],
                                "currency",
                                "EUR",
                            )
                    return MemberDescriptorType.__get__(
                        original_compare,
                        instance,
                        field_type,
                    )


            def factory_for(source):
                def fingerprint():
                    attack_state["factory_call_count"] += 1
                    attack_state["armed"] = (
                        attack_state["factory_call_count"] == 2
                    )
                    try:
                        return journal_module._phase2_source_fingerprint(source)
                    finally:
                        attack_state["armed"] = False

                return fingerprint


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    Contract._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory_for(source),
                    )
                    for source, fingerprint in zip(
                        sources,
                        expected,
                        strict=True,
                    )
                )
                setattr(field_type, "compare", CallbackCompare())
                try:
                    owner = (
                        journal_module
                        ._current_journal_source_authority_owner(candidates)
                    )
                finally:
                    setattr(field_type, "compare", original_compare)

                print(
                    json.dumps(
                        {
                            **attack_state,
                            "first_currency": sources[0].currency,
                            "owner_granted": owner is journal,
                            "source_types_exact": all(
                                type(source) is Phase2FeeScheduleSource
                                for source in sources
                            ),
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertTrue(outcome["source_types_exact"])
        self.assertEqual(outcome["callback_count"], 0)
        self.assertIsNone(outcome["fired_on_factory_call"])
        self.assertEqual(outcome["first_currency"], "USD")
        self.assertTrue(outcome["owner_granted"])

    def test_phase2_owner_decimal_tuple_descriptor_cannot_mutate_first_candidate(
        self,
    ) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        program = textwrap.dedent(
            r"""
            import json
            import sys
            import tempfile
            from datetime import date, datetime, timezone
            from decimal import Decimal
            from pathlib import Path

            sys.path.insert(0, "src")

            from stock_monitor import journal as journal_module
            from stock_monitor.journal import (
                Journal,
                Phase2OptionChainFactSource,
            )
            from tests.integration.test_phase2_gate import (
                Phase2JournalSourceContractTests as Contract,
            )


            snapshot_field = (
                Phase2OptionChainFactSource.__dataclass_fields__["snapshot"]
            )
            if snapshot_field.compare is not False:
                raise AssertionError("snapshot is not compare-false")


            def option_fact(ordinal):
                return Phase2OptionChainFactSource(
                    row_id=ordinal,
                    snapshot_id=f"snapshot-{ordinal}",
                    authorization_id=f"authorization-{ordinal}",
                    chain_set_id=f"chain-{ordinal}",
                    occ_symbol=f"AAPL26090{ordinal}C00100000",
                    underlying="AAPL",
                    expiration=date(2026, 9, 18),
                    strike_micros=100_000_000,
                    delta_micros=500_000,
                    bid_micros=1_000_000,
                    ask_micros=1_100_000,
                    daily_volume=100,
                    source_observation_row_id=ordinal,
                    external_source_observation_id=f"observation-{ordinal}",
                    fetch_page_ordinal=1,
                    source_item_ordinal=ordinal,
                    source_item_path=f"$.options[{ordinal}]",
                    payload_sha256=str(ordinal) * 64,
                    provider_fact_digest=str(ordinal + 2) * 64,
                    observed_at=datetime(
                        2026,
                        8,
                        ordinal,
                        12,
                        tzinfo=timezone.utc,
                    ),
                    received_at=datetime(
                        2026,
                        8,
                        ordinal,
                        12,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    snapshot=Decimal(f"{ordinal}.25"),
                    row_references=(),
                    source_digest=str(ordinal + 4) * 64,
                )


            sources = (option_fact(1), option_fact(2))
            expected = tuple(
                journal_module._phase2_source_fingerprint(source)
                for source in sources
            )
            decimal_tuple_type = type(Decimal("0").as_tuple())
            original_exponent = type.__getattribute__(
                decimal_tuple_type,
                "exponent",
            )
            attack_state = {
                "armed": False,
                "callback_count": 0,
                "factory_call_count": 0,
                "fired_on_factory_call": None,
            }


            class CallbackExponent:
                def __get__(self, instance, owner=None):
                    if instance is None:
                        return self
                    if attack_state["armed"]:
                        attack_state["callback_count"] += 1
                        if attack_state["fired_on_factory_call"] is None:
                            attack_state["fired_on_factory_call"] = (
                                attack_state["factory_call_count"]
                            )
                            object.__setattr__(
                                sources[0],
                                "underlying",
                                "MSFT",
                            )
                    return tuple.__getitem__(instance, 2)


            def factory_for(source):
                def fingerprint():
                    attack_state["factory_call_count"] += 1
                    attack_state["armed"] = (
                        attack_state["factory_call_count"] == 2
                    )
                    try:
                        return journal_module._phase2_source_fingerprint(source)
                    finally:
                        attack_state["armed"] = False

                return fingerprint


            with tempfile.TemporaryDirectory() as directory, Journal.open(
                Path(directory) / "journal.sqlite3"
            ) as journal:
                registry = {}
                candidates = tuple(
                    Contract._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory_for(source),
                    )
                    for source, fingerprint in zip(
                        sources,
                        expected,
                        strict=True,
                    )
                )
                setattr(
                    decimal_tuple_type,
                    "exponent",
                    CallbackExponent(),
                )
                try:
                    owner = (
                        journal_module
                        ._current_journal_source_authority_owner(candidates)
                    )
                finally:
                    setattr(
                        decimal_tuple_type,
                        "exponent",
                        original_exponent,
                    )

                print(
                    json.dumps(
                        {
                            **attack_state,
                            "first_underlying": sources[0].underlying,
                            "owner_granted": owner is journal,
                            "source_types_exact": all(
                                type(source) is Phase2OptionChainFactSource
                                for source in sources
                            ),
                        },
                        sort_keys=True,
                    )
                )
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-W", "error", "-c", program],
            cwd=repository_root,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        outcome = json.loads(completed.stdout)
        self.assertTrue(outcome["source_types_exact"])
        self.assertEqual(outcome["callback_count"], 0)
        self.assertIsNone(outcome["fired_on_factory_call"])
        self.assertEqual(outcome["first_underlying"], "AAPL")
        self.assertTrue(outcome["owner_granted"])

    def test_owner_batch_routes_non_phase1_candidates_through_legacy_factory(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class Phase1Root:
            ordinal: int
            child: object

        class LegacySource:
            pass

        shared = ("shared",)
        phase1_sources = tuple(
            Phase1Root(ordinal, shared) for ordinal in range(4)
        )
        phase1_expected = tuple(
            journal_module._phase1_source_fingerprint(source)
            for source in phase1_sources
        )
        phase1_factories = tuple(
            self._required_phase1_fingerprint_factory(source)
            for source in phase1_sources
        )
        legacy_source = LegacySource()
        legacy_expected = ("legacy", "fingerprint")
        legacy_factory = mock.Mock(return_value=legacy_expected)
        original_forest = self._required_phase1_fingerprint_forest()

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            registry: journal_module._JournalSourceRegistry = {}
            candidates = tuple(
                self._synthetic_authority_candidate(
                    journal,
                    registry,
                    source,
                    fingerprint,
                    factory,
                )
                for source, fingerprint, factory in zip(
                    phase1_sources,
                    phase1_expected,
                    phase1_factories,
                    strict=True,
                )
            ) + (
                self._synthetic_authority_candidate(
                    journal,
                    registry,
                    legacy_source,
                    legacy_expected,
                    legacy_factory,
                ),
            )
            with mock.patch.object(
                journal_module,
                "_phase1_source_fingerprint_forest",
                wraps=original_forest,
            ) as forest:
                owner = journal_module._current_journal_source_authority_owner(
                    candidates
                )

        self.assertIs(owner, journal)
        self.assertEqual(forest.call_count, 2)
        for call in forest.call_args_list:
            self.assertEqual(len(call.args[0]), 4)
            self.assertTrue(
                all(
                    actual is expected
                    for actual, expected in zip(
                        call.args[0],
                        phase1_sources,
                        strict=True,
                    )
                )
            )
        legacy_factory.assert_called_once_with()

    def test_owner_batch_rejects_malformed_or_cyclic_phase1_forests(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class ValidRoot:
            value: str

        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class CyclicRoot:
            child: object

        class MalformedRoot:
            pass

        valid_roots = tuple(
            ValidRoot(f"valid-{ordinal}") for ordinal in range(3)
        )
        valid_expected = tuple(
            journal_module._phase1_source_fingerprint(valid)
            for valid in valid_roots
        )
        cyclic = CyclicRoot(None)
        object.__setattr__(cyclic, "child", cyclic)

        for case, invalid in (
            ("malformed", MalformedRoot()),
            ("cyclic", cyclic),
        ):
            with (
                self.subTest(case=case),
                tempfile.TemporaryDirectory() as directory,
                Journal.open(Path(directory) / "journal.sqlite3") as journal,
            ):
                registry: journal_module._JournalSourceRegistry = {}
                invalid_expected = journal_module._phase1_source_fingerprint(
                    ("placeholder", case)
                )
                roots = (*valid_roots, invalid)
                expected = (*valid_expected, invalid_expected)
                factories = tuple(
                    self._required_phase1_fingerprint_factory(root)
                    for root in roots
                )
                candidates = tuple(
                    self._synthetic_authority_candidate(
                        journal,
                        registry,
                        source,
                        fingerprint,
                        factory,
                    )
                    for source, fingerprint, factory in zip(
                        roots,
                        expected,
                        factories,
                        strict=True,
                    )
                )
                original_forest = self._required_phase1_fingerprint_forest()
                with mock.patch.object(
                    journal_module,
                    "_phase1_source_fingerprint_forest",
                    wraps=original_forest,
                ) as forest:
                    owner = (
                        journal_module._current_journal_source_authority_owner(
                            candidates
                        )
                    )

                self.assertIsNone(owner)
                self.assertEqual(forest.call_count, 1)
                called_roots = forest.call_args.args[0]
                self.assertEqual(len(called_roots), 4)
                self.assertTrue(
                    all(
                        actual is expected_root
                        for actual, expected_root in zip(
                            called_roots,
                            roots,
                            strict=True,
                        )
                    )
                )

    def test_source_merkle_seals_retain_compact_material(self) -> None:
        @dataclass(frozen=True, slots=True)
        class SharedNode:
            left: object
            right: object

        @dataclass(frozen=True, slots=True)
        class OpaqueEnvelope:
            payload: object = field(compare=False)

        root: object = SharedNode("left", "right")
        for _ in range(512):
            root = SharedNode(root, root)

        seals = (
            journal_module._phase1_source_fingerprint(root),
            journal_module._phase2_opaque_identity_fingerprint(
                OpaqueEnvelope(root)
            ),
        )
        for seal in seals:
            self.assertIs(type(seal), journal_module._SourceFingerprintSeal)
            self.assertIs(type(seal.identity_anchors), tuple)
            self.assertLessEqual(len(seal.identity_anchors), 4)
            retained_bytes = (
                sys.getsizeof(seal)
                + sys.getsizeof(seal.digest)
                + sys.getsizeof(seal.identity_anchors)
            )
            self.assertLess(retained_bytes, 2_048)

    def test_source_merkle_seals_ignore_structural_alias_topology(self) -> None:
        @dataclass(frozen=True, slots=True)
        class SharedNode:
            left: object
            right: object

        @dataclass(frozen=True, slots=True)
        class OpaqueEnvelope:
            payload: object = field(compare=False)

        shared_leaf = SharedNode("left", "right")
        shared = SharedNode(shared_leaf, shared_leaf)
        duplicated = SharedNode(
            SharedNode("left", "right"),
            SharedNode("left", "right"),
        )
        shared_seal = journal_module._phase1_source_fingerprint(shared)
        duplicated_seal = journal_module._phase1_source_fingerprint(duplicated)
        self.assertIs(
            type(shared_seal), journal_module._SourceFingerprintSeal
        )
        self.assertTrue(
            journal_module._fingerprints_equal(
                shared_seal,
                duplicated_seal,
            )
        )

        opaque_root = SharedNode(shared_leaf, shared_leaf)
        envelope = OpaqueEnvelope(opaque_root)
        opaque_shared = journal_module._phase2_opaque_identity_fingerprint(
            envelope
        )
        object.__setattr__(
            opaque_root,
            "right",
            SharedNode("left", "right"),
        )
        opaque_duplicated = (
            journal_module._phase2_opaque_identity_fingerprint(envelope)
        )
        self.assertTrue(
            journal_module._fingerprints_equal(
                opaque_shared,
                opaque_duplicated,
            )
        )

    def test_source_merkle_seals_recompute_and_detect_mutation(self) -> None:
        @dataclass(frozen=True, slots=True)
        class MutableNode:
            value: object

        @dataclass(frozen=True, slots=True)
        class Source:
            nested: object

        nested = MutableNode("original")
        source = Source(nested)
        original = journal_module._phase1_source_fingerprint(source)
        self.assertIs(type(original), journal_module._SourceFingerprintSeal)

        object.__setattr__(nested, "value", "mutated")
        mutated = journal_module._phase1_source_fingerprint(source)
        self.assertFalse(
            journal_module._fingerprints_equal(original, mutated)
        )

        object.__setattr__(nested, "value", "original")
        restored = journal_module._phase1_source_fingerprint(source)
        self.assertTrue(
            journal_module._fingerprints_equal(original, restored)
        )

    def test_source_merkle_seals_bind_exact_types_and_frames(self) -> None:
        @dataclass(frozen=True, slots=True)
        class First:
            left: object
            right: object

        @dataclass(frozen=True, slots=True)
        class Second:
            left: object
            right: object

        Second.__module__ = First.__module__
        Second.__name__ = First.__name__
        Second.__qualname__ = First.__qualname__
        first = journal_module._phase1_source_fingerprint(
            First("same", "value")
        )
        spoofed = journal_module._phase1_source_fingerprint(
            Second("same", "value")
        )
        self.assertIs(type(first), journal_module._SourceFingerprintSeal)
        self.assertFalse(journal_module._fingerprints_equal(first, spoofed))

        collision_pairs = (
            (("ab", "c"), ("a", "bc")),
            ((("a", "b"),), ("a", "b")),
            ((None,), (False,)),
            ((False,), (0,)),
            (("value",), (b"value",)),
            ((date(2026, 1, 2),), ("2026-01-02",)),
            (((),), ()),
        )
        for left, right in collision_pairs:
            with self.subTest(left=left, right=right):
                self.assertFalse(
                    journal_module._fingerprints_equal(
                        journal_module._phase1_source_fingerprint(left),
                        journal_module._phase1_source_fingerprint(right),
                    )
                )

        class SpoofedSeal(journal_module._SourceFingerprintSeal):
            pass

        spoofed_seal = SpoofedSeal(first.digest, first.identity_anchors)
        malformed_seal = journal_module._SourceFingerprintSeal(b"", ())
        for left, right in (
            (spoofed_seal, spoofed_seal),
            ((spoofed_seal,), (spoofed_seal,)),
            (malformed_seal, malformed_seal),
        ):
            with self.subTest(seal_shape=type(left)):
                self.assertFalse(
                    journal_module._fingerprints_equal(left, right)
                )

    def test_source_merkle_seals_identity_bind_datetime_without_hooks(
        self,
    ) -> None:
        class HookTZ(tzinfo):
            def __init__(self) -> None:
                self.calls = 0

            def utcoffset(self, _value: datetime | None) -> timedelta:
                self.calls += 1
                return timedelta(0)

            def dst(self, _value: datetime | None) -> timedelta:
                self.calls += 1
                return timedelta(0)

            def tzname(self, _value: datetime | None) -> str:
                self.calls += 1
                return "HOOK"

        @dataclass(frozen=True, slots=True)
        class Timestamped:
            at: datetime

        hook = HookTZ()
        original_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=hook)
        replacement_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=hook)
        source = Timestamped(original_at)
        original = journal_module._phase1_source_fingerprint(source)
        rebuilt = journal_module._phase1_source_fingerprint(source)
        self.assertIs(type(original), journal_module._SourceFingerprintSeal)
        self.assertTrue(
            journal_module._fingerprints_equal(original, rebuilt)
        )

        object.__setattr__(source, "at", replacement_at)
        replacement = journal_module._phase1_source_fingerprint(source)
        self.assertFalse(
            journal_module._fingerprints_equal(original, replacement)
        )
        self.assertEqual(hook.calls, 0)

    def test_phase2_merkle_seals_bind_compare_false_identity_and_structure(
        self,
    ) -> None:
        @dataclass(frozen=True, slots=True)
        class OpaqueValue:
            value: object

        @dataclass(frozen=True, slots=True)
        class Phase2Envelope:
            label: str
            opaque: object = field(compare=False)

        opaque = OpaqueValue("original")
        source = Phase2Envelope("source", opaque)
        copied_source = copy.copy(source)
        original = journal_module._phase2_source_fingerprint(source)
        copied = journal_module._phase2_source_fingerprint(copied_source)
        opaque_seal = journal_module._phase2_opaque_identity_fingerprint(
            source
        )
        self.assertIs(
            type(opaque_seal), journal_module._SourceFingerprintSeal
        )
        self.assertTrue(journal_module._fingerprints_equal(original, copied))

        equal_but_distinct = Phase2Envelope(
            "source",
            OpaqueValue("original"),
        )
        self.assertFalse(
            journal_module._fingerprints_equal(
                original,
                journal_module._phase2_source_fingerprint(
                    equal_but_distinct
                ),
            )
        )

        object.__setattr__(opaque, "value", "mutated")
        self.assertFalse(
            journal_module._fingerprints_equal(
                original,
                journal_module._phase2_source_fingerprint(source),
            )
        )

    def test_source_merkle_seals_reject_deep_cycles_iteratively(self) -> None:
        @dataclass(frozen=True, slots=True)
        class DeepNode:
            child: object

        @dataclass(frozen=True, slots=True)
        class OpaqueEnvelope:
            payload: object = field(compare=False)

        root = DeepNode(None)
        tail = root
        for _ in range(5_000):
            next_node = DeepNode(None)
            object.__setattr__(tail, "child", next_node)
            tail = next_node
        object.__setattr__(tail, "child", root)

        for fingerprint, source in (
            (journal_module._phase1_source_fingerprint, root),
            (
                journal_module._phase2_opaque_identity_fingerprint,
                OpaqueEnvelope(root),
            ),
        ):
            with self.subTest(fingerprint=fingerprint.__name__):
                with self.assertRaisesRegex(TypeError, "cyclic"):
                    fingerprint(source)

    def test_fingerprint_comparison_handles_deep_shared_tuple_dags(
        self,
    ) -> None:
        left_match: object = ("leaf",)
        right_match: object = ("leaf",)
        right_mismatch: object = ("different",)
        for _ in range(2_000):
            left_match = (left_match, left_match)
            right_match = (right_match, right_match)
            right_mismatch = (right_mismatch, right_mismatch)

        with self.subTest(result="equal"):
            self.assertTrue(
                journal_module._fingerprints_equal(
                    left_match,
                    right_match,
                )
            )
        with self.subTest(result="deep-leaf-mismatch"):
            self.assertFalse(
                journal_module._fingerprints_equal(
                    left_match,
                    right_mismatch,
                )
            )

    def test_owner_batch_discards_each_fingerprint_before_building_next(
        self,
    ) -> None:
        released: list[int] = []

        class Source:
            pass

        class FingerprintProbe:
            def __init__(self, ordinal: int) -> None:
                self.ordinal = ordinal

            def __del__(self) -> None:
                released.append(self.ordinal)

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            sources = (Source(), Source())
            registry: journal_module._JournalSourceRegistry = {}
            owner_reference = journal_module.ref(journal)
            total_changes = (
                journal_module._journal_source_authority_total_changes(
                    journal
                )
            )
            data_version = journal._source_authority_data_version()
            candidates: list[journal_module._JournalAuthorityCandidate] = []

            for ordinal, source in enumerate(sources):
                stored_fingerprint = ("stored", ordinal)
                registry[id(source)] = (
                    journal_module.ref(source),
                    stored_fingerprint,
                    owner_reference,
                    total_changes,
                    data_version,
                )

                def build_fingerprint(
                    ordinal: int = ordinal,
                ) -> tuple[object, ...]:
                    if ordinal:
                        self.assertEqual(released, [0])
                    return (FingerprintProbe(ordinal),)

                candidates.append(
                    (
                        registry,
                        source,
                        journal,
                        total_changes,
                        data_version,
                        stored_fingerprint,
                        build_fingerprint,
                        registry[id(source)],
                    )
                )

            with mock.patch.object(
                journal_module,
                "_fingerprints_equal",
                new=lambda _left, _right: True,
            ):
                self.assertIs(
                    journal_module._current_journal_source_authority_owner(
                        candidates
                    ),
                    journal,
                )

    def test_owner_batch_rejects_stored_seal_identity_replacement(self) -> None:
        class Source:
            pass

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            source = Source()
            registry: journal_module._JournalSourceRegistry = {}
            stored_seal = journal_module._phase1_source_fingerprint(
                ("same", "material")
            )
            replacement_seal = journal_module._phase1_source_fingerprint(
                ("same", "material")
            )
            self.assertIsNot(stored_seal, replacement_seal)
            self.assertTrue(
                journal_module._fingerprints_equal(
                    stored_seal,
                    replacement_seal,
                )
            )
            owner_reference = journal_module.ref(journal)
            total_changes = (
                journal_module._journal_source_authority_total_changes(
                    journal
                )
            )
            data_version = journal._source_authority_data_version()
            registry[id(source)] = (
                journal_module.ref(source),
                stored_seal,
                owner_reference,
                total_changes,
                data_version,
            )
            candidate = (
                registry,
                source,
                journal,
                total_changes,
                data_version,
                stored_seal,
                lambda: replacement_seal,
                registry[id(source)],
            )

            # Simulate a same-identity registry splice after candidate capture.
            registry[id(source)] = (
                journal_module.ref(source),
                replacement_seal,
                owner_reference,
                total_changes,
                data_version,
            )
            self.assertIsNone(
                journal_module._current_journal_source_authority_owner(
                    (candidate,)
                )
            )

    def test_generic_registrar_cannot_mint_any_phase2_source_type(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            for source_type in journal_module._PHASE2_SOURCE_TYPES:
                with self.subTest(source_type=source_type.__name__):
                    raw = object.__new__(source_type)
                    for registrar in (
                        journal_module._register_journal_source_authority,
                        journal_module._remember_journal_source_authority,
                    ):
                        with self.subTest(registrar=registrar.__name__):
                            with self.assertRaisesRegex(
                                JournalError,
                                "persisted Journal reader",
                            ):
                                registrar(
                                    journal_module._PHASE2_SOURCE_AUTHORITIES,
                                    raw,
                                    journal,
                                )

    def test_phase2_inputs_cannot_be_minted_by_the_generic_phase1_registrar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            signal_id, cutoff = _seed_not_triggered_adherence_material(journal)
            journal.record_phase1_adherence(
                signal_id,
                query_cutoff=cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            signal_source = journal._read_phase1_signal_source(
                signal_id,
                query_cutoff=cutoff,
            )
            window_source = journal.read_phase1_validation_window_source(
                "1" * 64,
                through_session=_SESSION,
                query_cutoff=cutoff,
                calendar_resolver=_calendar(),
            )
            cases = (
                (
                    signal_source,
                    journal_module._PHASE1_SIGNAL_SOURCE_AUTHORITIES,
                    journal_module.is_verified_phase1_signal_source,
                ),
                (
                    window_source,
                    journal_module._PHASE1_VALIDATION_WINDOW_SOURCE_AUTHORITIES,
                    journal_module.is_verified_phase1_validation_window_source,
                ),
            )
            for source, registry, verifier in cases:
                with self.subTest(source_type=type(source).__name__):
                    self.assertTrue(verifier(source))
                    for forged in (copy.copy(source), object.__new__(type(source))):
                        with self.assertRaisesRegex(
                            JournalError,
                            "persisted Journal reader",
                        ):
                            journal_module._register_journal_source_authority(
                                registry,
                                forged,
                                journal,
                            )
                        self.assertFalse(verifier(forged))

    def test_phase2_and_historical_source_dtos_have_the_frozen_surface(self) -> None:
        expected_fields = {
            "Phase2WindowSource": (
                "row_id",
                "window_id",
                "validation_window_id",
                "promotion_source",
                "promotion_decision_digest",
                "promotion_signal_ids",
                "promotion_query_cutoff",
                "start_action",
                "started_session",
                "started_at",
                "received_at",
                "starting_capital_micros",
                "calendar_digest",
                "query_cutoff",
                "row_references",
                "source_digest",
            ),
            "Phase2AuthorizationSource": (
                "row_id",
                "authorization_id",
                "window_source",
                "signal_source",
                "authorized_at",
                "received_at",
                "authorization_digest",
                "row_references",
                "source_digest",
            ),
            "Phase2FeeScheduleSource": (
                "row_id",
                "schedule_id",
                "effective_session",
                "reviewed_at",
                "currency",
                "contract_multiplier",
                "entry_fee_per_contract_micros",
                "exit_fee_per_contract_micros",
                "close_fee_reserve_per_contract_micros",
                "source_sha256",
                "schedule_digest",
                "reviewed_bytes",
                "archived_at",
                "row_references",
                "source_digest",
            ),
            "Phase2EventExclusionSource": (
                "signal_source",
                "signal_evidence_source",
                "covered_sessions",
                "event_sessions",
                "reviewed_at",
                "query_cutoff",
                "calendar_digest",
                "evidence_source_row_ids",
                "evidence_source_highwater",
                "expected_evidence_source_count",
                "row_references",
                "source_digest",
                "authority_digest",
            ),
            "Phase2OptionChainPageSource": (
                "row_id",
                "page_id",
                "chain_set_id",
                "page_ordinal",
                "source_observation_row_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "source_time",
                "retrieved_at",
                "raw_payload",
                "row_references",
                "source_digest",
            ),
            "Phase2OptionChainFactSource": (
                "row_id",
                "snapshot_id",
                "authorization_id",
                "chain_set_id",
                "occ_symbol",
                "underlying",
                "expiration",
                "strike_micros",
                "delta_micros",
                "bid_micros",
                "ask_micros",
                "daily_volume",
                "source_observation_row_id",
                "external_source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "provider_fact_digest",
                "observed_at",
                "received_at",
                "snapshot",
                "row_references",
                "source_digest",
            ),
            "Phase2OptionChainSource": (
                "row_id",
                "chain_set_id",
                "authorization_source",
                "underlying",
                "collection_name",
                "requested_symbols",
                "request_digest",
                "manifest_digest",
                "pages",
                "facts",
                "provider_chain",
                "review_candidate_fact_digests",
                "expected_manual_review_count",
                "expected_page_count",
                "expected_fact_count",
                "terminal",
                "query_cutoff",
                "received_at",
                "row_references",
                "source_digest",
            ),
            "Phase2ManualOptionReviewSource": (
                "row_id",
                "snapshot_id",
                "authorization_id",
                "chain_set_id",
                "provider_fact_source",
                "action_source",
                "occ_symbol",
                "underlying",
                "expiration",
                "strike_micros",
                "delta_micros",
                "bid_micros",
                "ask_micros",
                "open_interest",
                "daily_volume",
                "observed_at",
                "received_at",
                "row_references",
                "source_digest",
            ),
            "Phase2SelectionSource": (
                "row_id",
                "selection_id",
                "authorization_source",
                "option_chain_source",
                "provider_fact_source",
                "manual_review_sources",
                "selected_manual_review_source",
                "fee_schedule_source",
                "event_exclusion_source",
                "portfolio_source",
                "selection_session",
                "quantity",
                "selected_at",
                "received_at",
                "ranking_digest",
                "row_references",
                "source_digest",
            ),
            "Phase2OptionOpenSource": (
                "row_id",
                "entry_id",
                "window_id",
                "selection_source",
                "open_action",
                "quantity",
                "entry_ask_micros",
                "entry_fee_micros",
                "reserve_fee_micros",
                "all_in_initial_risk_micros",
                "fee_schedule_source",
                "portfolio_source_digest",
                "entered_at",
                "received_at",
                "row_references",
                "source_digest",
            ),
            "Phase2OpenPositionSource": (
                "entry_id",
                "selection_id",
                "authorization_id",
                "occ_symbol",
                "quantity",
                "entry_ask_micros",
                "entry_fee_micros",
                "reserve_fee_micros",
                "all_in_initial_risk_micros",
                "entered_at",
                "entry_source",
                "row_references",
                "source_digest",
            ),
            "Phase2SettlementSource": (
                "exit_id",
                "gross_proceeds_micros",
                "economic_at",
                "settlement_available_session",
                "settled",
                "row_references",
                "source_digest",
            ),
            "Phase2PortfolioSource": (
                "window_id",
                "as_of",
                "query_cutoff",
                "settled_cash_micros",
                "economic_cash_micros",
                "equity_micros",
                "open_position_source",
                "settlement_sources",
                "unsettled_proceeds_micros",
                "calendar_digest",
                "ledger_row_references",
                "ledger_terminal_cursor",
                "ledger_source_highwater",
                "expected_entry_count",
                "expected_exit_count",
                "expected_fee_count",
                "expected_equity_count",
                "row_references",
                "source_digest",
                "authority_digest",
            ),
            "Phase2UnderlyingReviewPageSource": (
                "row_id",
                "page_id",
                "review_set_id",
                "page_ordinal",
                "source_observation_row_id",
                "external_source_observation_id",
                "source_type",
                "request_url",
                "request_page_token",
                "next_page_token",
                "payload_sha256",
                "source_time",
                "retrieved_at",
                "raw_payload",
                "row_references",
                "source_digest",
            ),
            "Phase2UnderlyingReviewFactSource": (
                "row_id",
                "fact_id",
                "review_set_id",
                "source_observation_row_id",
                "external_source_observation_id",
                "fetch_page_ordinal",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "symbol",
                "bar_at",
                "open_micros",
                "high_micros",
                "low_micros",
                "close_micros",
                "volume",
                "fact_digest",
                "bar",
                "row_references",
                "source_digest",
            ),
            "Phase2UnderlyingReviewSource": (
                "row_id",
                "review_set_id",
                "window_id",
                "entry_source",
                "underlying",
                "review_session",
                "collection_name",
                "timeframe",
                "adjustment",
                "feed",
                "requested_symbols",
                "request_digest",
                "manifest_digest",
                "pages",
                "facts",
                "provider_cohort",
                "expected_page_count",
                "expected_fact_count",
                "terminal",
                "request_start",
                "request_end",
                "query_cutoff",
                "received_at",
                "row_references",
                "source_digest",
            ),
            "Phase2ExitReviewSource": (
                "row_id",
                "exit_review_id",
                "window_id",
                "entry_source",
                "underlying_review_source",
                "decision_fact_source",
                "review_session",
                "decision_kind",
                "decision_digest",
                "decision",
                "holding_sessions",
                "dte",
                "query_cutoff",
                "evaluated_at",
                "received_at",
                "row_references",
                "source_digest",
            ),
            "HistoricalReplayEvidenceSource": (
                "row_id",
                "replay_evidence_id",
                "replay_date_id",
                "role",
                "evidence_ordinal",
                "subject",
                "source_kind",
                "source_observation_row_id",
                "external_source_observation_id",
                "source_item_ordinal",
                "source_item_path",
                "payload_sha256",
                "content_sha256",
                "authority_digest",
                "effective_at",
                "published_at",
                "retrieved_at",
                "row_references",
                "source_digest",
            ),
            "HistoricalReplayDateSource": (
                "row_id",
                "replay_date_id",
                "run_id",
                "session_date",
                "report_cutoff",
                "evidence_sources",
                "expected_role_count",
                "expected_evidence_count",
                "case_digest",
                "domain_input_digest",
                "mechanics_digest",
                "completion_digest",
                "completed_at",
                "row_references",
                "source_digest",
            ),
            "HistoricalReplaySource": (
                "row_id",
                "run_id",
                "tier",
                "query_cutoff",
                "calendar_digest",
                "policy_digest",
                "date_sources",
                "expected_date_count",
                "cursors",
                "highwaters",
                "row_references",
                "source_digest",
            ),
        }

        for name, expected in expected_fields.items():
            source_type = getattr(journal_module, name)
            self.assertEqual(
                tuple(item.name for item in fields(source_type)),
                expected,
            )

    def test_caller_built_phase2_source_objects_have_no_authority(self) -> None:
        verifier_names = (
            "is_verified_phase2_window_source",
            "is_verified_phase2_authorization_source",
            "is_verified_phase2_fee_schedule_source",
            "is_verified_phase2_event_exclusion_source",
            "is_verified_phase2_option_chain_source",
            "is_verified_phase2_manual_option_review_source",
            "is_verified_phase2_selection_source",
            "is_verified_phase2_option_open_source",
            "is_verified_phase2_portfolio_source",
            "is_verified_phase2_underlying_review_source",
            "is_verified_phase2_exit_review_source",
            "is_verified_historical_replay_source",
        )
        for name in verifier_names:
            with self.subTest(verifier=name):
                self.assertFalse(getattr(journal_module, name)(object()))
        self.assertFalse(journal_module.phase2_sources_share_owner(object(), object()))
        self.assertTrue(
            callable(
                getattr(
                    journal_module.HistoricalReplaySource,
                    "_is_current_historical_replay_source",
                    None,
                )
            )
        )
        self.assertTrue(
            callable(
                getattr(
                    journal_module.HistoricalReplayDateSource,
                    "_rederive_historical_replay_case",
                    None,
                )
            )
        )
        self.assertTrue(
            callable(
                getattr(
                    journal_module.Phase2FeeScheduleSource,
                    "_is_current_phase2_fee_schedule_source",
                    None,
                )
            )
        )


class Phase2EventExclusionJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        role, pair, authority = task5_fixture_module._test_coverage_authority(
            "AAPL",
            "0000000000",
        )
        scoped_patcher = mock.patch.dict(
            evidence_module._SCOPED_REFERENCE_AUTHORITIES,
            {role: authority},
        )
        clear_patcher = mock.patch.dict(
            evidence_module._CLEAR_COVERAGE_AUTHORITIES,
            {role: frozenset({pair})},
        )
        scoped_patcher.start()
        clear_patcher.start()
        self.addCleanup(clear_patcher.stop)
        self.addCleanup(scoped_patcher.stop)

    @staticmethod
    def _persist_dated_adverse_evidence(
        journal: Journal,
        *,
        event_session: date,
    ) -> tuple[object, object, object, datetime]:
        original = task5_fixture_module._reviewed_record

        def dated_record(**options: object):
            return original(
                **options,
                event_date=event_session,
                event_kind="BINARY_EVENT",
            )

        with mock.patch.object(
            task5_fixture_module,
            "_reviewed_record",
            side_effect=dated_record,
        ):
            return _persist_signal_evidence(journal, adverse=True)

    def test_reader_issues_clear_latest_persisted_evidence_for_exact_signal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            historical_signal, _authority, _stored, review_at = (
                _persist_signal_evidence(journal, adverse=False)
            )
            signal_source = journal._read_phase1_signal_source(
                historical_signal.signal_id,
                query_cutoff=review_at,
            )

            source = journal.read_phase2_event_exclusion_source(
                signal_source,
                calendar_resolver=_calendar(),
                query_cutoff=review_at,
            )

            self.assertTrue(
                journal_module.is_verified_phase2_event_exclusion_source(source)
            )
            self.assertIs(source.signal_source, signal_source)
            self.assertIs(source.signal_evidence_source.signal_source, signal_source)
            self.assertEqual(
                source.covered_sessions,
                tuple(
                    _calendar().add_sessions(
                        signal_source.publication_session,
                        offset,
                    )
                    for offset in range(10)
                ),
            )
            self.assertEqual(source.event_sessions, ())
            self.assertEqual(source.reviewed_at, review_at)
            self.assertEqual(source.query_cutoff, review_at)
            self.assertEqual(
                source.expected_evidence_source_count,
                len(source.evidence_source_row_ids),
            )
            self.assertEqual(
                source.evidence_source_highwater,
                max(
                    source.signal_evidence_source.registry_source_row_id,
                    *source.evidence_source_row_ids,
                ),
            )

    def test_reader_rederives_horizon_after_phase_switch_calendar_callbacks(
        self,
    ) -> None:
        positive_session_calls = 0

        class PhaseSwitchCalendar(SessionCalendarResolver):
            def add_sessions(self, start: date, count: int) -> date:
                nonlocal positive_session_calls
                if count > 0:
                    positive_session_calls += 1
                    if positive_session_calls > 2:
                        count += 10
                return super().add_sessions(start, count)

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            historical_signal, _authority, _stored, review_at = (
                _persist_signal_evidence(journal, adverse=False)
            )
            signal_source = journal._read_phase1_signal_source(
                historical_signal.signal_id,
                query_cutoff=review_at,
            )
            switched_calendar = PhaseSwitchCalendar(_calendar().calendars)

            with self.assertRaisesRegex(
                InvalidJournalValue,
                "session coverage",
            ):
                journal.read_phase2_event_exclusion_source(
                    signal_source,
                    calendar_resolver=switched_calendar,
                    query_cutoff=review_at,
                )

            self.assertGreater(positive_session_calls, 2)

    def test_reader_preserves_dated_adverse_event_sessions(self) -> None:
        event_session = _calendar().add_sessions(_SESSION, 4)
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            historical_signal, _authority, _stored, review_at = (
                self._persist_dated_adverse_evidence(
                    journal,
                    event_session=event_session,
                )
            )
            signal_source = journal._read_phase1_signal_source(
                historical_signal.signal_id,
                query_cutoff=review_at,
            )

            source = journal.read_phase2_event_exclusion_source(
                signal_source,
                calendar_resolver=_calendar(),
                query_cutoff=review_at,
            )

            self.assertTrue(
                journal_module.is_verified_phase2_event_exclusion_source(source)
            )
            self.assertEqual(source.event_sessions, (event_session,))

    def test_reader_never_falls_back_from_latest_adverse_to_older_clear(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            historical_signal, _authority, _stored, first_review = (
                _persist_signal_evidence(journal, adverse=False)
            )
            second_session = _calendar().add_sessions(_SESSION, 1)
            second_review = aware_et(second_session, "08:45")
            _persist_signal_evidence(
                journal,
                adverse=True,
                review_at=second_review,
                signal_source=historical_signal,
            )
            signal_source = journal._read_phase1_signal_source(
                historical_signal.signal_id,
                query_cutoff=second_review,
            )

            with self.assertRaisesRegex(
                InvalidJournalValue,
                "dated event overlap",
            ):
                journal.read_phase2_event_exclusion_source(
                    signal_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=second_review,
                )

            earlier_signal = journal._read_phase1_signal_source(
                historical_signal.signal_id,
                query_cutoff=first_review,
            )
            earlier = journal.read_phase2_event_exclusion_source(
                earlier_signal,
                calendar_resolver=_calendar(),
                query_cutoff=first_review,
            )
            self.assertEqual(earlier.reviewed_at, first_review)
            self.assertEqual(earlier.event_sessions, ())

    def test_reader_rejects_unknown_conflicted_or_ambiguous_latest_truth(
        self,
    ) -> None:
        for case in ("UNKNOWN", "CONFLICT", "AMBIGUOUS"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    original = task5_fixture_module._reviewed_record

                    def reviewed_record(**options: object):
                        if case == "CONFLICT":
                            options["conflicts"] = ("conflicting-source",)
                        elif case == "AMBIGUOUS":
                            options["ambiguities"] = ("ambiguous-classification",)
                        return original(**options)

                    patcher = (
                        mock.patch.object(
                            task5_fixture_module,
                            "_reviewed_record",
                            side_effect=reviewed_record,
                        )
                        if case != "UNKNOWN"
                        else nullcontext()
                    )
                    with patcher:
                        historical_signal, _authority, _stored, review_at = (
                            _persist_signal_evidence(
                                journal,
                                adverse=False,
                                binary_event_coverage=(
                                    "UNKNOWN" if case == "UNKNOWN" else "CONFIRMED_CLEAR"
                                ),
                            )
                        )
                    signal_source = journal._read_phase1_signal_source(
                        historical_signal.signal_id,
                        query_cutoff=review_at,
                    )

                    with self.assertRaisesRegex(
                        InvalidJournalValue,
                        "unresolved",
                    ):
                        journal.read_phase2_event_exclusion_source(
                            signal_source,
                            calendar_resolver=_calendar(),
                            query_cutoff=review_at,
                        )

    def test_reader_authority_rejects_copy_nested_mutation_and_cross_owner(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(
                Path(directory) / "first.sqlite3"
            ) as journal, Journal.open(
                Path(directory) / "second.sqlite3"
            ) as other:
                historical_signal, _authority, _stored, review_at = (
                    _persist_signal_evidence(journal, adverse=False)
                )
                signal_source = journal._read_phase1_signal_source(
                    historical_signal.signal_id,
                    query_cutoff=review_at,
                )
                source = journal.read_phase2_event_exclusion_source(
                    signal_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=review_at,
                )

                self.assertFalse(
                    journal_module.is_verified_phase2_event_exclusion_source(
                        copy.copy(source)
                    )
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "signal identity",
                ):
                    other.read_phase2_event_exclusion_source(
                        signal_source,
                        calendar_resolver=_calendar(),
                        query_cutoff=review_at,
                    )
                object.__setattr__(
                    source.signal_evidence_source,
                    "source_digest",
                    "f" * 64,
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_event_exclusion_source(source)
                )

    def test_reader_reissues_after_restart_and_revokes_on_later_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                historical_signal, _authority, _stored, review_at = (
                    _persist_signal_evidence(journal, adverse=False)
                )
                signal_source = journal._read_phase1_signal_source(
                    historical_signal.signal_id,
                    query_cutoff=review_at,
                )
                first = journal.read_phase2_event_exclusion_source(
                    signal_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=review_at,
                )
                first_source_digest = first.source_digest
                first_authority_digest = first.authority_digest
                self.assertTrue(
                    journal_module.is_verified_phase2_event_exclusion_source(first)
                )

            self.assertFalse(
                journal_module.is_verified_phase2_event_exclusion_source(first)
            )
            with Journal.open(path) as restarted:
                restarted_signal = restarted._read_phase1_signal_source(
                    historical_signal.signal_id,
                    query_cutoff=review_at,
                )
                second = restarted.read_phase2_event_exclusion_source(
                    restarted_signal,
                    calendar_resolver=_calendar(),
                    query_cutoff=review_at,
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_event_exclusion_source(second)
                )
                self.assertIsNot(first, second)
                self.assertIs(second.signal_source, restarted_signal)
                self.assertEqual(second.source_digest, first_source_digest)
                self.assertEqual(second.authority_digest, first_authority_digest)

                restarted.append_raw_message(
                    "phase2-event-exclusion-revision",
                    review_at + timedelta(seconds=1),
                    "revision",
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_event_exclusion_source(second)
                )

    def test_reader_discards_reconstruction_when_revision_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                historical_signal, _authority, _stored, review_at = (
                    _persist_signal_evidence(journal, adverse=False)
                )
                signal_source = journal._read_phase1_signal_source(
                    historical_signal.signal_id,
                    query_cutoff=review_at,
                )
                original = journal._read_phase1_signal_evidence_source

                def read_then_change(*args: object, **kwargs: object):
                    result = original(*args, **kwargs)
                    with Journal.open(path) as other:
                        other.append_raw_message(
                            "phase2-event-exclusion-race",
                            review_at + timedelta(seconds=1),
                            "race",
                        )
                    return result

                before = len(journal_module._PHASE2_SOURCE_AUTHORITIES)
                with mock.patch.object(
                    journal,
                    "_read_phase1_signal_evidence_source",
                    side_effect=read_then_change,
                ), self.assertRaises(
                    (JournalError, journal_module.MigrationCorruption)
                ):
                    journal.read_phase2_event_exclusion_source(
                        signal_source,
                        calendar_resolver=_calendar(),
                        query_cutoff=review_at,
                    )
                self.assertEqual(
                    len(journal_module._PHASE2_SOURCE_AUTHORITIES),
                    before,
                )


class Phase2PortfolioJournalTests(unittest.TestCase):
    @staticmethod
    def _seed_genesis_window(
        journal: Journal,
    ) -> tuple[object, object, datetime]:
        signal_source, _authority, _stored, promotion_cutoff = (
            _persist_signal_evidence(journal, adverse=False)
        )
        calendar = _calendar()
        promotion_source = journal_module.Phase1ValidationWindowSource(
            validation_window_id=signal_source.validation_window_id,
            started_session=signal_source.publication_session,
            through_session=signal_source.publication_session,
            starting_capital_micros=5_000_000_000,
            started_at=promotion_cutoff,
            received_at=promotion_cutoff,
            calendar_digest=signal_source.calendar_digest,
            expected_open_sessions=(signal_source.publication_session,),
            signal_sources=(signal_source,),
            disposition_sources=(),
            canonical_history=None,  # type: ignore[arg-type]
            actual_history=None,  # type: ignore[arg-type]
            adherence_check_sources=(),
            adherence_review_sources=(),
            query_cutoff=promotion_cutoff,
            signal_terminal_cursor=signal_source.row_id,
            signal_source_highwater=signal_source.row_id,
            lifecycle_terminal_cursor=None,
            lifecycle_source_highwater=0,
            adherence_terminal_cursor=None,
            adherence_source_highwater=0,
            expected_signal_count=1,
            expected_disposition_count=0,
            expected_adherence_count=0,
            row_references=signal_source.row_references,
            source_digest="9" * 64,
        )
        promotion = validation_module.PromotionDecision(
            passed=True,
            status=validation_module.PromotionStatus.PASSED,
            reason_codes=(),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.10"),
            adherence=Decimal("0.90"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("0"),
            source_digest=promotion_source.source_digest,
            authority_digest="a" * 64,
            _phase1_source=promotion_source,
        )
        started_session = calendar.add_sessions(
            signal_source.publication_session,
            1,
        )
        started_at = aware_et(started_session, "08:00")
        start_action = _phase2_window_start_action(
            journal,
            session_date=started_session,
            event_at=started_at,
        )
        received_at = start_action.received_at
        promotion_signal_ids = (signal_source.signal_id,)
        window_id = journal_module._phase2_stable_id(
            "stock-monitor/phase2-window/v1",
            promotion_source.validation_window_id,
            promotion_source.source_digest,
            promotion.authority_digest,
            start_action.source_digest,
            str(start_action.execution_event_id),
        )
        source_references = journal_module._phase2_row_references(
            promotion_source.row_references,
            start_action.row_references,
        )
        source_digest = journal_module._journal_bundle_digest(
            "stock-monitor/phase2-window-source/v1",
            source_references,
            {
                "window_id": window_id,
                "validation_window_id": promotion_source.validation_window_id,
                "promotion_source_digest": promotion_source.source_digest,
                "promotion_decision_digest": promotion.authority_digest,
                "promotion_signal_ids": list(promotion_signal_ids),
                "promotion_through_session": (
                    promotion_source.through_session.isoformat()
                ),
                "promotion_query_cutoff": (
                    journal_module._canonical_timestamp(promotion_cutoff)
                ),
                "start_execution_event_id": start_action.execution_event_id,
                "start_raw_message_id": start_action.raw_message_id,
                "started_session": started_session.isoformat(),
                "started_at": journal_module._canonical_timestamp(started_at),
                "received_at": journal_module._canonical_timestamp(received_at),
                "starting_capital_micros": 5_000_000_000,
                "calendar_digest": signal_source.calendar_digest,
            },
        )
        values_without_hash = (
            window_id,
            promotion_source.validation_window_id,
            promotion_source.source_digest,
            promotion.authority_digest,
            journal_module._canonical_audit_json(list(promotion_signal_ids)),
            promotion_source.through_session.isoformat(),
            journal_module._canonical_timestamp(promotion_cutoff),
            start_action.execution_event_id,
            start_action.raw_message_id,
            started_session.isoformat(),
            journal_module._canonical_timestamp(started_at),
            journal_module._canonical_timestamp(received_at),
            5_000_000_000,
            signal_source.calendar_digest,
            source_digest,
        )
        record_sha256 = journal_module._phase2_plain_record_digest(
            journal_module._PHASE2_WINDOW_COLUMNS,
            values_without_hash,
        )
        journal._connection.create_function(
            "journal_phase2_write_allowed",
            -1,
            lambda *arguments: 1,
        )
        try:
            with journal.transaction() as transaction:
                transaction._mark_dirty()
                journal._connection.execute(
                    "INSERT INTO phase2_windows("
                    + ", ".join(journal_module._PHASE2_WINDOW_COLUMNS[1:])
                    + ") VALUES ("
                    + ", ".join("?" for _ in (*values_without_hash, record_sha256))
                    + ")",
                    (*values_without_hash, record_sha256),
                )
                window_row = journal._connection.execute(
                    "SELECT "
                    + ", ".join(journal_module._PHASE2_WINDOW_COLUMNS)
                    + " FROM phase2_windows WHERE window_id = ? COLLATE BINARY",
                    (window_id,),
                ).fetchone()
                assert window_row is not None
                window_reference = journal_module._journal_row_reference(
                    "phase2_windows",
                    journal_module._PHASE2_WINDOW_COLUMNS,
                    window_row,
                )
                genesis = journal_module._phase2_window_genesis_record(
                    window_reference,
                    window_id=window_id,
                    window_source_digest=source_digest,
                    started_session=started_session,
                    started_at=started_at,
                    received_at=received_at,
                )
                journal._connection.execute(
                    "INSERT INTO phase2_equity_points("
                    + ", ".join(journal_module._PHASE2_EQUITY_POINT_COLUMNS[1:])
                    + ") VALUES ("
                    + ", ".join("?" for _ in genesis)
                    + ")",
                    genesis,
                )
        finally:
            journal._configure_connection()
        with mock.patch.object(
            Journal,
            "read_phase1_promotion_decision",
            return_value=promotion,
        ):
            window_source = journal.read_phase2_window_source(
                window_id,
                query_cutoff=received_at,
                calendar_resolver=calendar,
            )
        assert window_source is not None
        assert journal_module.is_verified_phase2_window_source(window_source)
        return window_source, promotion, received_at

    def test_reader_issues_exact_genesis_only_portfolio(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_source, _promotion, cutoff = self._seed_genesis_window(journal)

            source = journal.read_phase2_portfolio_source(
                window_source,
                calendar_resolver=_calendar(),
                query_cutoff=cutoff,
            )

            self.assertTrue(
                journal_module.is_verified_phase2_portfolio_source(source)
            )
            self.assertEqual(source.window_id, window_source.window_id)
            self.assertEqual(source.as_of, cutoff)
            self.assertEqual(source.query_cutoff, cutoff)
            self.assertEqual(source.settled_cash_micros, 5_000_000_000)
            self.assertEqual(source.economic_cash_micros, 5_000_000_000)
            self.assertEqual(source.equity_micros, 5_000_000_000)
            self.assertIsNone(source.open_position_source)
            self.assertEqual(source.settlement_sources, ())
            self.assertEqual(source.unsettled_proceeds_micros, 0)
            self.assertEqual(source.expected_entry_count, 0)
            self.assertEqual(source.expected_exit_count, 0)
            self.assertEqual(source.expected_fee_count, 0)
            self.assertEqual(source.expected_equity_count, 1)
            self.assertEqual(len(source.ledger_row_references), 1)
            self.assertEqual(
                source.ledger_row_references[0].table,
                "phase2_equity_points",
            )
            self.assertEqual(
                source.ledger_terminal_cursor,
                source.ledger_row_references[0].row_id,
            )
            self.assertEqual(
                source.ledger_source_highwater,
                source.ledger_row_references[0].row_id,
            )

    def test_portfolio_authority_binds_exact_window_identity_transitively(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_a, promotion, cutoff = self._seed_genesis_window(journal)
            with mock.patch.object(
                Journal,
                "read_phase1_promotion_decision",
                return_value=promotion,
            ):
                window_b = journal.read_phase2_window_source(
                    window_a.window_id,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                )
            self.assertIsNotNone(window_b)
            assert window_b is not None
            self.assertIsNot(window_b, window_a)
            self.assertTrue(
                journal_module.is_verified_phase2_window_source(window_a)
            )
            self.assertTrue(
                journal_module.is_verified_phase2_window_source(window_b)
            )
            source = journal.read_phase2_portfolio_source(
                window_a,
                calendar_resolver=_calendar(),
                query_cutoff=cutoff,
            )
            before_tamper = (
                journal_module.phase2_sources_share_owner(window_a, source),
                journal_module.phase2_sources_share_owner(window_b, source),
                journal_module.is_verified_phase2_portfolio_source(source),
                journal_module.phase2_portfolio_source_binds_window(
                    source,
                    window_a,
                ),
                journal_module.phase2_portfolio_source_binds_window(
                    source,
                    window_b,
                ),
            )

            original_digest = window_a.source_digest
            object.__setattr__(window_a, "source_digest", "f" * 64)
            try:
                observed = (
                    journal_module.phase2_sources_share_owner(window_a, source),
                    journal_module.phase2_sources_share_owner(window_b, source),
                    journal_module.is_verified_phase2_window_source(window_a),
                    journal_module.is_verified_phase2_window_source(window_b),
                    journal_module.is_verified_phase2_portfolio_source(source),
                    journal_module.phase2_sources_share_owner(window_b, source),
                    journal_module.phase2_portfolio_source_binds_window(
                        source,
                        window_a,
                    ),
                    journal_module.phase2_portfolio_source_binds_window(
                        source,
                        window_b,
                    ),
                )
            finally:
                object.__setattr__(window_a, "source_digest", original_digest)

            self.assertEqual(
                (*before_tamper, *observed),
                (
                    True,
                    False,
                    True,
                    True,
                    False,
                    False,
                    False,
                    False,
                    True,
                    False,
                    False,
                    False,
                    False,
                ),
            )

    def test_portfolio_retains_window_until_portfolio_is_collected(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_source, _promotion, cutoff = self._seed_genesis_window(journal)
            source = journal.read_phase2_portfolio_source(
                window_source,
                calendar_resolver=_calendar(),
                query_cutoff=cutoff,
            )
            source_identity = id(source)
            source_reference = ref(source)
            window_reference = ref(window_source)

            del window_source
            gc.collect()

            self.assertIsNotNone(window_reference())
            self.assertTrue(
                journal_module.is_verified_phase2_portfolio_source(source)
            )
            self.assertIn(
                source_identity,
                journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES,
            )

            del source
            gc.collect()

            self.assertIsNone(source_reference())
            self.assertIsNone(window_reference())
            self.assertNotIn(
                source_identity,
                journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES,
            )

    def test_portfolio_final_recheck_rejects_main_registry_record_swap(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_source, _promotion, cutoff = self._seed_genesis_window(journal)
            source = journal.read_phase2_portfolio_source(
                window_source,
                calendar_resolver=_calendar(),
                query_cutoff=cutoff,
            )
            original_verifier = (
                journal_module._current_journal_source_authority_owner
            )
            binding = journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES[
                id(source)
            ]

            for target, identity, captured in (
                ("portfolio", id(source), binding.portfolio_issued),
                ("window", id(window_source), binding.window_issued),
            ):
                with self.subTest(target=target):
                    replacement = tuple(list(captured))

                    def swap_after_streamed_verification(candidates):
                        owner = original_verifier(candidates)
                        if owner is not None:
                            with journal_module._JOURNAL_SOURCE_LOCK:
                                journal_module._PHASE2_SOURCE_AUTHORITIES[
                                    identity
                                ] = replacement
                        return owner

                    try:
                        with mock.patch.object(
                            journal_module,
                            "_current_journal_source_authority_owner",
                            side_effect=swap_after_streamed_verification,
                        ):
                            self.assertFalse(
                                journal_module.is_verified_phase2_portfolio_source(
                                    source
                                )
                            )
                    finally:
                        with journal_module._JOURNAL_SOURCE_LOCK:
                            journal_module._PHASE2_SOURCE_AUTHORITIES[
                                identity
                            ] = captured

    def test_reader_binds_account_wide_counts_and_highwaters_in_both_digests(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_source, _promotion, cutoff = self._seed_genesis_window(journal)
            digest_materials: list[tuple[str, object]] = []
            original = journal_module._journal_bundle_digest

            def capture_digest(
                namespace: str,
                references: object,
                material: object,
            ) -> str:
                if namespace.startswith("stock-monitor/phase2-portfolio-"):
                    digest_materials.append((namespace, material))
                return original(namespace, references, material)

            with mock.patch.object(
                journal_module,
                "_journal_bundle_digest",
                side_effect=capture_digest,
            ):
                source = journal.read_phase2_portfolio_source(
                    window_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=cutoff,
                )

            self.assertEqual(
                tuple(namespace for namespace, _material in digest_materials),
                (
                    "stock-monitor/phase2-portfolio-source/v1",
                    "stock-monitor/phase2-portfolio-authority/v1",
                ),
            )
            expected_summaries = [
                {
                    "table": "phase2_windows",
                    "count": 1,
                    "source_highwater": window_source.row_id,
                },
                {
                    "table": "phase2_entries",
                    "count": 0,
                    "source_highwater": 0,
                },
                {
                    "table": "phase2_marks",
                    "count": 0,
                    "source_highwater": 0,
                },
                {
                    "table": "phase2_exits",
                    "count": 0,
                    "source_highwater": 0,
                },
                {
                    "table": "phase2_fee_records",
                    "count": 0,
                    "source_highwater": 0,
                },
                {
                    "table": "phase2_equity_points",
                    "count": 1,
                    "source_highwater": source.ledger_source_highwater,
                },
            ]
            for _namespace, material in digest_materials:
                self.assertIsInstance(material, dict)
                self.assertEqual(material["stream_summaries"], expected_summaries)
                self.assertEqual(
                    material["ledger_terminal_cursor"],
                    source.ledger_terminal_cursor,
                )
                self.assertEqual(
                    material["ledger_source_highwater"],
                    source.ledger_source_highwater,
                )

    def test_reader_rejects_any_non_genesis_account_wide_ledger_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            window_source, _promotion, cutoff = self._seed_genesis_window(journal)
            expected_counts = {
                "phase2_windows": 1,
                "phase2_entries": 0,
                "phase2_marks": 0,
                "phase2_exits": 0,
                "phase2_fee_records": 0,
                "phase2_equity_points": 1,
            }
            original = journal_module._sql

            for table_name in expected_counts:
                with self.subTest(table_name=table_name):
                    def add_visible_row(
                        connection: sqlite3.Connection,
                        statement: str,
                        parameters: tuple[object, ...] = (),
                    ):
                        cursor = original(connection, statement, parameters)
                        if not statement.startswith("SELECT 'phase2_windows'"):
                            return cursor
                        rows = [tuple(row) for row in cursor.fetchall()]
                        changed = [
                            (
                                name,
                                count + 1,
                                max(highwater + 1, 1),
                            )
                            if name == table_name
                            else (name, count, highwater)
                            for name, count, highwater in rows
                        ]
                        replacement = mock.Mock()
                        replacement.fetchall.return_value = changed
                        return replacement

                    with mock.patch.object(
                        journal_module,
                        "_sql",
                        side_effect=add_visible_row,
                    ), self.assertRaisesRegex(
                        InvalidJournalValue,
                        "sole genesis ledger",
                    ):
                        journal.read_phase2_portfolio_source(
                            window_source,
                            calendar_resolver=_calendar(),
                            query_cutoff=cutoff,
                        )

    def test_reader_authority_rejects_copy_cross_owner_raw_and_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with Journal.open(root / "first.sqlite3") as journal, Journal.open(
                root / "second.sqlite3"
            ) as other:
                window_source, _promotion, cutoff = self._seed_genesis_window(
                    journal
                )
                source = journal.read_phase2_portfolio_source(
                    window_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=cutoff,
                )
                copied_source = copy.copy(source)
                raw_source = type(source)(
                    **{
                        item.name: getattr(source, item.name)
                        for item in fields(type(source))
                    }
                )
                raw_window = type(window_source)(
                    **{
                        item.name: getattr(window_source, item.name)
                        for item in fields(type(window_source))
                    }
                )

                self.assertFalse(
                    journal_module.is_verified_phase2_portfolio_source(
                        copied_source
                    )
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_portfolio_source(raw_source)
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "window identity",
                ):
                    journal.read_phase2_portfolio_source(
                        copy.copy(window_source),
                        calendar_resolver=_calendar(),
                        query_cutoff=cutoff,
                    )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "window identity",
                ):
                    journal.read_phase2_portfolio_source(
                        raw_window,
                        calendar_resolver=_calendar(),
                        query_cutoff=cutoff,
                    )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "window identity",
                ):
                    other.read_phase2_portfolio_source(
                        window_source,
                        calendar_resolver=_calendar(),
                        query_cutoff=cutoff,
                    )
                with self.assertRaisesRegex(
                    JournalError,
                    "persisted Journal reader",
                ):
                    journal_module._register_journal_source_authority(
                        journal_module._PHASE2_SOURCE_AUTHORITIES,
                        raw_source,
                        journal,
                    )

                object.__setattr__(
                    source.ledger_row_references[0],
                    "row_digest",
                    "f" * 64,
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_portfolio_source(source)
                )

    def test_reader_reissues_after_restart_and_revokes_on_later_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                window_source, promotion, cutoff = self._seed_genesis_window(journal)
                source = journal.read_phase2_portfolio_source(
                    window_source,
                    calendar_resolver=_calendar(),
                    query_cutoff=cutoff,
                )
                window_id = window_source.window_id
                source_digest = source.source_digest
                authority_digest = source.authority_digest
                self.assertTrue(
                    journal_module.is_verified_phase2_portfolio_source(source)
                )

            self.assertFalse(
                journal_module.is_verified_phase2_portfolio_source(source)
            )
            with Journal.open(path) as restarted, mock.patch.object(
                Journal,
                "read_phase1_promotion_decision",
                return_value=promotion,
            ):
                restarted_window = restarted.read_phase2_window_source(
                    window_id,
                    query_cutoff=cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertIsNotNone(restarted_window)
                assert restarted_window is not None
                restored = restarted.read_phase2_portfolio_source(
                    restarted_window,
                    calendar_resolver=_calendar(),
                    query_cutoff=cutoff,
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_portfolio_source(restored)
                )
                self.assertTrue(
                    journal_module.phase2_portfolio_source_binds_window(
                        restored,
                        restarted_window,
                    )
                )
                self.assertIsNot(restored, source)
                self.assertEqual(restored.source_digest, source_digest)
                self.assertEqual(restored.authority_digest, authority_digest)

                restarted.append_raw_message(
                    "phase2-portfolio-revision",
                    cutoff + timedelta(seconds=1),
                    "revision",
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_portfolio_source(restored)
                )

    def test_reader_discards_reconstruction_when_revision_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                window_source, _promotion, cutoff = self._seed_genesis_window(journal)
                original = journal_module._sql
                revision_changed = False

                def change_after_summary(
                    connection: sqlite3.Connection,
                    statement: str,
                    parameters: tuple[object, ...] = (),
                ):
                    nonlocal revision_changed
                    cursor = original(connection, statement, parameters)
                    if (
                        not revision_changed
                        and statement.startswith("SELECT 'phase2_windows'")
                    ):
                        summary_rows = cursor.fetchall()
                        with Journal.open(path) as other:
                            other.append_raw_message(
                                "phase2-portfolio-race",
                                cutoff + timedelta(seconds=1),
                                "race",
                            )
                        revision_changed = True
                        replacement = mock.Mock()
                        replacement.fetchall.return_value = summary_rows
                        return replacement
                    return cursor

                before = {
                    identity
                    for identity, issued in (
                        journal_module._PHASE2_SOURCE_AUTHORITIES.items()
                    )
                    if type(issued[0]()) is journal_module.Phase2PortfolioSource
                }
                with mock.patch.object(
                    journal_module,
                    "_sql",
                    side_effect=change_after_summary,
                ), self.assertRaises(JournalError):
                    journal.read_phase2_portfolio_source(
                        window_source,
                        calendar_resolver=_calendar(),
                        query_cutoff=cutoff,
                    )
                after = {
                    identity
                    for identity, issued in (
                        journal_module._PHASE2_SOURCE_AUTHORITIES.items()
                    )
                    if type(issued[0]()) is journal_module.Phase2PortfolioSource
                }
                self.assertTrue(revision_changed)
                self.assertEqual(after, before)


class Phase2FeeScheduleJournalTests(unittest.TestCase):
    def test_fee_rows_require_the_source_specific_journal_writer(self) -> None:
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        statement, row = _phase2_fee_insert_material(
            schedule,
            archived_at=archived_at,
        )

        with tempfile.TemporaryDirectory() as directory:
            external_path = Path(directory) / "external.sqlite3"
            with Journal.open(external_path):
                pass
            with closing(sqlite3.connect(external_path)) as connection:
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(statement, row)
            with Journal.open(external_path) as restarted:
                self.assertIsNone(
                    restarted.read_phase2_fee_schedule_source(
                        schedule.schedule_id,
                        query_cutoff=archived_at,
                    )
                )

            internal_path = Path(directory) / "internal.sqlite3"
            with Journal.open(internal_path) as journal:
                with self.assertRaises(sqlite3.DatabaseError):
                    journal._connection.execute(statement, row)
                self.assertIsNone(
                    journal.read_phase2_fee_schedule_source(
                        schedule.schedule_id,
                        query_cutoff=archived_at,
                    )
                )

            mutable_path = Path(directory) / "mutable.sqlite3"
            with Journal.open(mutable_path) as journal:
                self.assertFalse(
                    hasattr(journal, "_phase2_write"),
                    "a caller-visible generic Phase 2 write context is unsafe",
                )
                journal._phase2_write_allowed = True
                with journal.transaction() as transaction:
                    transaction._mark_dirty()
                    with self.assertRaises(sqlite3.DatabaseError):
                        journal._connection.execute(statement, row)

    def test_trace_callback_cannot_splice_unplanned_fee_material_into_writer(
        self,
    ) -> None:
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        _valid_statement, valid_row = _phase2_fee_insert_material(
            schedule,
            archived_at=archived_at,
        )
        forged_document = json.loads(
            config_module._read_reviewed_fee_schedule_bytes(schedule).decode(
                "utf-8"
            )
        )
        forged_document.update(
            {
                "schedule_id": "ATTACK_OPTION_FEES_V1",
                "entry_fee_per_contract_micros": 0,
                "source_sha256": "b" * 64,
            }
        )
        forged_bytes = config_module._canonical_reviewed_fee_bytes(
            forged_document
        )
        forged_schedule = config_module._parse_reviewed_fee_schedule(
            forged_document
        )
        forged_statement, forged_row = _phase2_fee_insert_material(
            forged_schedule,
            archived_at=archived_at,
            reviewed_bytes=forged_bytes,
        )
        self.assertNotEqual(valid_row[-1], forged_row[-1])
        borrowed_hash_row = (*forged_row[:-1], valid_row[-1])

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            gate_results: list[tuple[int, int, int, int]] = []
            splice_errors: list[sqlite3.DatabaseError] = []
            trace_entered: list[bool] = []

            def gate(
                table: str,
                record_sha256: str,
                values_without_hash: tuple[object, ...],
            ) -> int:
                parameters = (
                    table,
                    record_sha256,
                    *values_without_hash,
                )
                row = journal._connection.execute(
                    "SELECT journal_phase2_write_allowed("
                    + ", ".join("?" for _ in parameters)
                    + ")",
                    parameters,
                ).fetchone()
                assert row is not None
                return int(row[0])

            def trace(frame: object, event: str, argument: object) -> object:
                del argument
                if (
                    event != "line"
                    or frame.f_code
                    is not Journal._archive_phase2_fee_schedule.__code__
                    or trace_entered
                    or type(frame.f_locals.get("phase2_write_plan"))
                    is not frozenset
                ):
                    return trace
                trace_entered.append(True)
                gate_results.append(
                    (
                        gate(
                            "phase2_fee_schedules",
                            str(valid_row[-1]),
                            tuple(valid_row[:-1]),
                        ),
                        gate(
                            "historical_replay_runs",
                            str(valid_row[-1]),
                            tuple(valid_row[:-1]),
                        ),
                        gate(
                            "phase2_fee_schedules",
                            str(forged_row[-1]),
                            tuple(valid_row[:-1]),
                        ),
                        gate(
                            "phase2_fee_schedules",
                            str(valid_row[-1]),
                            tuple(forged_row[:-1]),
                        ),
                    )
                )
                try:
                    journal._connection.execute(
                        forged_statement,
                        borrowed_hash_row,
                    )
                except sqlite3.DatabaseError as error:
                    splice_errors.append(error)
                return trace

            previous_trace = sys.gettrace()
            sys.settrace(trace)
            try:
                source = journal.archive_phase2_fee_schedule(
                    schedule,
                    archived_at=archived_at,
                )
            finally:
                sys.settrace(previous_trace)

            self.assertEqual(trace_entered, [True])
            self.assertEqual(gate_results, [(1, 0, 0, 0)])
            self.assertEqual(len(splice_errors), 1)
            self.assertTrue(
                journal_module.is_verified_phase2_fee_schedule_source(source)
            )
            self.assertIsNone(
                journal.read_phase2_fee_schedule_source(
                    forged_schedule.schedule_id,
                    query_cutoff=archived_at,
                )
            )

    def test_journal_subclass_cannot_override_a_phase2_writer(self) -> None:
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        statement, row = _phase2_fee_insert_material(
            schedule,
            archived_at=archived_at,
        )

        class ForgedJournal(Journal):
            def _archive_phase2_fee_schedule(
                self,
                untrusted_schedule: object,
                *,
                archived_at: datetime,
            ) -> None:
                del untrusted_schedule, archived_at
                phase2_write_plan = frozenset(
                    {("phase2_fee_schedules", row[-1])}
                )
                if not phase2_write_plan:
                    raise AssertionError("forged write plan is unexpectedly empty")
                self._connection.execute(statement, row)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path):
                pass
            with self.assertRaisesRegex(JournalError, "subclasses"):
                ForgedJournal.open(path)

            connection = sqlite3.connect(path, isolation_level=None)
            forged = ForgedJournal(path, connection)
            try:
                forged._configure_connection()
                with self.assertRaises(sqlite3.DatabaseError):
                    forged.archive_phase2_fee_schedule(
                        schedule,
                        archived_at=archived_at,
                    )
                count = forged._connection.execute(
                    "SELECT COUNT(*) FROM phase2_fee_schedules"
                ).fetchone()
                self.assertEqual(count, (0,))
            finally:
                forged.close()

    def test_fee_row_converter_cannot_issue_a_nonpersisted_source(self) -> None:
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            journal.archive_phase2_fee_schedule(
                schedule,
                archived_at=archived_at,
            )
            row = journal._connection.execute(
                "SELECT "
                + ", ".join(journal_module._PHASE2_FEE_SCHEDULE_COLUMNS)
                + " FROM phase2_fee_schedules WHERE schedule_id = ?",
                (schedule.schedule_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            forged_row = list(row)
            forged_row[0] = int(row[0]) + 10_000
            forged_row[-1] = journal_module._phase2_fee_schedule_record_digest(
                tuple(forged_row[1:-1])
            )

            forged = journal._phase2_fee_schedule_source_from_row(forged_row)

            self.assertFalse(
                journal_module.is_verified_phase2_fee_schedule_source(forged)
            )
            with self.assertRaises(ValueError):
                config_module._reissue_archived_fee_schedule(forged)

    def test_archived_fee_schedule_reissues_across_restart_and_rejects_conflict(
        self,
    ) -> None:
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        cutoff = datetime(2026, 8, 18, 17, tzinfo=timezone.utc)
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "journal.sqlite3"
            with Journal.open(path) as journal:
                source = journal.archive_phase2_fee_schedule(
                    schedule,
                    archived_at=archived_at,
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_fee_schedule_source(source)
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_fee_schedule_source(
                        copy.copy(source)
                    )
                )
                reissued = config_module._reissue_archived_fee_schedule(source)
                self.assertTrue(config_module.is_reviewed_fee_schedule(reissued))
                self.assertEqual(reissued, schedule)
                self.assertIsNot(reissued, schedule)

                journal.append_raw_message(
                    "phase2-authority-stale",
                    archived_at + timedelta(seconds=1),
                    "unrelated durable write",
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_fee_schedule_source(source)
                )
                with self.assertRaisesRegex(
                    JournalError,
                    "persisted Journal reader",
                ):
                    journal_module._register_journal_source_authority(
                        journal_module._PHASE2_SOURCE_AUTHORITIES,
                        source,
                        journal,
                    )
                self.assertFalse(
                    journal_module.is_verified_phase2_fee_schedule_source(source)
                )

                retry = journal.archive_phase2_fee_schedule(
                    schedule,
                    archived_at=archived_at,
                )
                self.assertEqual(retry, source)
                self.assertIsNot(retry, source)

                changed_path = root / "changed-fees.json"
                changed = json.loads(source.reviewed_bytes)
                changed["entry_fee_per_contract_micros"] += 1
                changed_path.write_text(json.dumps(changed), encoding="utf-8")
                changed_schedule = load_fee_schedule(changed_path)
                with self.assertRaises(IdempotencyConflict):
                    journal.archive_phase2_fee_schedule(
                        changed_schedule,
                        archived_at=archived_at,
                    )

            self.assertFalse(
                journal_module.is_verified_phase2_fee_schedule_source(source)
            )
            with Journal.open(path) as restarted:
                restored = restarted.read_phase2_fee_schedule_source(
                    schedule.schedule_id,
                    query_cutoff=cutoff,
                )
                self.assertIsNotNone(restored)
                assert restored is not None
                self.assertTrue(
                    journal_module.is_verified_phase2_fee_schedule_source(restored)
                )
                restarted_schedule = config_module._reissue_archived_fee_schedule(
                    restored
                )
                self.assertTrue(
                    config_module.is_reviewed_fee_schedule(restarted_schedule)
                )
                self.assertEqual(restarted_schedule, schedule)

    def test_fee_schedule_read_is_cutoff_bound(self) -> None:
        schedule = load_fee_schedule(
            PROJECT_ROOT / "tests/fixtures/options/reviewed-fees.json"
        )
        archived_at = datetime(2026, 8, 18, 16, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            journal.archive_phase2_fee_schedule(schedule, archived_at=archived_at)

            self.assertIsNone(
                journal.read_phase2_fee_schedule_source(
                    schedule.schedule_id,
                    query_cutoff=datetime(
                        2026,
                        8,
                        18,
                        15,
                        59,
                        59,
                        tzinfo=timezone.utc,
                    ),
                )
            )


class Phase2WindowAuthorizationJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        role, pair, authority = task5_fixture_module._test_coverage_authority(
            "AAPL",
            "0000000000",
        )
        scoped_patcher = mock.patch.dict(
            evidence_module._SCOPED_REFERENCE_AUTHORITIES,
            {role: authority},
        )
        clear_patcher = mock.patch.dict(
            evidence_module._CLEAR_COVERAGE_AUTHORITIES,
            {role: frozenset({pair})},
        )
        scoped_patcher.start()
        clear_patcher.start()
        self.addCleanup(clear_patcher.stop)
        self.addCleanup(scoped_patcher.stop)

    def test_window_genesis_material_is_exact_and_content_addressed(
        self,
    ) -> None:
        started_at = aware_et(_SESSION, "08:00")
        received_at = started_at + timedelta(seconds=2)
        window_reference = journal_module.JournalRowReference(
            "phase2_windows",
            7,
            "a" * 64,
        )
        record = journal_module._phase2_window_genesis_record(
            window_reference,
            window_id="b" * 64,
            window_source_digest="c" * 64,
            started_session=_SESSION,
            started_at=started_at,
            received_at=received_at,
        )
        stored = dict(
            zip(
                journal_module._PHASE2_EQUITY_POINT_COLUMNS[1:],
                record,
                strict=True,
            )
        )
        self.assertEqual(stored["window_id"], "b" * 64)
        self.assertIsNone(stored["entry_id"])
        self.assertIsNone(stored["mark_id"])
        self.assertIsNone(stored["exit_id"])
        self.assertEqual(stored["session_date"], _SESSION.isoformat())
        self.assertEqual(stored["point_kind"], "START")
        self.assertEqual(stored["cash_micros"], 5_000_000_000)
        self.assertEqual(stored["position_value_micros"], 0)
        self.assertEqual(stored["equity_micros"], 5_000_000_000)
        self.assertEqual(stored["high_water_micros"], 5_000_000_000)
        self.assertEqual(stored["drawdown_micros"], 0)
        self.assertEqual(
            stored["at"],
            journal_module._canonical_timestamp(started_at),
        )
        self.assertEqual(
            stored["received_at"],
            journal_module._canonical_timestamp(received_at),
        )
        self.assertEqual(
            stored["record_sha256"],
            journal_module._phase2_plain_record_digest(
                journal_module._PHASE2_EQUITY_POINT_COLUMNS,
                record[:-1],
            ),
        )

    def test_authorization_reader_checks_window_at_the_outer_cutoff(
        self,
    ) -> None:
        authorized_at = aware_et(_SESSION, "10:00")
        outer_cutoff = authorized_at + timedelta(minutes=5)
        values_without_hash = (
            "a" * 64,
            "b" * 64,
            "c" * 64,
            "d" * 64,
            "e" * 64,
            journal_module._canonical_timestamp(authorized_at),
            journal_module._canonical_timestamp(authorized_at),
            "f" * 64,
        )
        row = (
            1,
            *values_without_hash,
            journal_module._phase2_plain_record_digest(
                journal_module._PHASE2_AUTHORIZATION_COLUMNS,
                values_without_hash,
            ),
        )

        class WindowReadProbe(RuntimeError):
            pass

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal, mock.patch.object(
            Journal,
            "read_phase2_window_source",
            side_effect=WindowReadProbe,
        ) as read_window:
            with self.assertRaises(WindowReadProbe):
                journal._phase2_authorization_source_from_row(
                    row,
                    query_cutoff=outer_cutoff,
                    calendar_resolver=_calendar(),
                )
        self.assertEqual(
            read_window.call_args.kwargs["query_cutoff"],
            outer_cutoff,
        )

    def test_promotion_semantics_ignore_identity_but_detect_revocation(
        self,
    ) -> None:
        passed = validation_module.PromotionDecision(
            passed=True,
            status=validation_module.PromotionStatus.PASSED,
            reason_codes=(),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.10"),
            adherence=Decimal("0.90"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("0"),
            source_digest="a" * 64,
            authority_digest="b" * 64,
        )
        same_semantics = validation_module.PromotionDecision(
            passed=True,
            status=validation_module.PromotionStatus.PASSED,
            reason_codes=(),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.10"),
            adherence=Decimal("0.90"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("0"),
            source_digest="c" * 64,
            authority_digest="d" * 64,
        )
        revoked = validation_module.PromotionDecision(
            passed=False,
            status=validation_module.PromotionStatus.FAILED,
            reason_codes=("RISK_LIMIT_BREACH",),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.10"),
            adherence=Decimal("0.90"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("1"),
            source_digest="e" * 64,
            authority_digest="f" * 64,
        )

        semantics = journal_module._phase2_promotion_semantics
        self.assertEqual(semantics(passed), semantics(same_semantics))
        self.assertNotEqual(semantics(passed), semantics(revoked))

    def test_window_reader_translates_current_promotion_failure_to_revocation(
        self,
    ) -> None:
        calendar = _calendar()
        signal_id = "5" * 64
        validation_window_id = "1" * 64
        promotion_source_digest = "2" * 64
        promotion_decision_digest = "3" * 64
        promotion_cutoff = aware_et(_SESSION, "16:00")
        started_session = calendar.add_sessions(_SESSION, 1)
        started_at = aware_et(started_session, "08:00")
        received_at = started_at + timedelta(seconds=1)
        promotion_source = journal_module.Phase1ValidationWindowSource(
            validation_window_id=validation_window_id,
            started_session=_SESSION,
            through_session=_SESSION,
            starting_capital_micros=5_000_000_000,
            started_at=aware_et(_SESSION, "09:00"),
            received_at=aware_et(_SESSION, "09:00") + timedelta(seconds=1),
            calendar_digest=_calendar_digest(calendar),
            expected_open_sessions=(_SESSION,),
            signal_sources=(mock.Mock(signal_id=signal_id),),
            disposition_sources=(),
            canonical_history=object(),
            actual_history=object(),
            adherence_check_sources=(),
            adherence_review_sources=(),
            query_cutoff=promotion_cutoff,
            signal_terminal_cursor=None,
            signal_source_highwater=1,
            lifecycle_terminal_cursor=None,
            lifecycle_source_highwater=1,
            adherence_terminal_cursor=None,
            adherence_source_highwater=1,
            expected_signal_count=1,
            expected_disposition_count=0,
            expected_adherence_count=0,
            row_references=(),
            source_digest=promotion_source_digest,
        )
        promotion = validation_module.PromotionDecision(
            passed=True,
            status=validation_module.PromotionStatus.PASSED,
            reason_codes=(),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.10"),
            adherence=Decimal("0.90"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("0"),
            source_digest=promotion_source_digest,
            authority_digest=promotion_decision_digest,
            _phase1_source=promotion_source,
        )
        values_without_hash = (
            "a" * 64,
            validation_window_id,
            promotion_source_digest,
            promotion_decision_digest,
            journal_module._canonical_audit_json([signal_id]),
            _SESSION.isoformat(),
            journal_module._canonical_timestamp(promotion_cutoff),
            1,
            1,
            started_session.isoformat(),
            journal_module._canonical_timestamp(started_at),
            journal_module._canonical_timestamp(received_at),
            5_000_000_000,
            _calendar_digest(calendar),
            "4" * 64,
        )
        row = (
            1,
            *values_without_hash,
            journal_module._phase2_plain_record_digest(
                journal_module._PHASE2_WINDOW_COLUMNS,
                values_without_hash,
            ),
        )
        upstream_error = validation_module.ValidationError(
            "PHASE1_VALIDATION_ADHERENCE_MISMATCH"
        )

        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal, mock.patch.object(
            journal,
            "read_phase1_promotion_decision",
            side_effect=(promotion, upstream_error),
        ):
            with self.assertRaisesRegex(
                InvalidJournalValue,
                "revoked",
            ) as caught:
                journal._phase2_window_source_from_row(
                    row,
                    query_cutoff=received_at + timedelta(minutes=1),
                    calendar_resolver=calendar,
                )
        self.assertIs(caught.exception.__cause__, upstream_error)

    def test_delayed_actual_write_revokes_late_authorities_after_restart(
        self,
    ) -> None:
        with (
            mock.patch.object(validation_module, "_MINIMUM_TRADES", 1),
            mock.patch.object(validation_module, "_MINIMUM_DAYS", 1),
            tempfile.TemporaryDirectory() as directory,
        ):
            path = Path(directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                _start_window(journal)
                _, promotion_cutoff = _seed_session_closed_primary(
                    journal,
                    session_date=_SESSION,
                    sequence=1,
                )
                promotion = journal.read_phase1_promotion_decision(
                    "1" * 64,
                    through_session=_SESSION,
                    query_cutoff=promotion_cutoff,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    validation_module.is_issued_promotion_decision(promotion)
                )
                self.assertTrue(promotion.passed)

                start_session = _calendar().add_sessions(_SESSION, 1)
                start_action = _phase2_window_start_action(
                    journal,
                    session_date=start_session,
                    event_at=aware_et(start_session, "08:00"),
                )
                window = journal.start_phase2_window(
                    promotion_decision=promotion,
                    start_action=start_action,
                    calendar_resolver=_calendar(),
                )
                signal = _publish_session_primary(
                    journal,
                    session_date=start_session,
                    sequence=2,
                )
                authorization = journal.authorize_phase2_signal(
                    window_source=window,
                    signal_source=signal,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_authorization_source(
                        authorization
                    )
                )

                late_cutoff = _append_delayed_phase1_actual_hard_evidence(
                    journal,
                    symbol=(
                        promotion._phase1_source.signal_sources[0].symbol
                    ),
                    economic_session=_SESSION,
                    received_after=(
                        authorization.received_at + timedelta(minutes=5)
                    ),
                )
                historical_window = journal.read_phase2_window_source(
                    window.window_id,
                    query_cutoff=window.received_at,
                    calendar_resolver=_calendar(),
                )
                historical_authorization = (
                    journal.read_phase2_authorization_source(
                        authorization.authorization_id,
                        query_cutoff=authorization.received_at,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNotNone(historical_window)
                self.assertIsNotNone(historical_authorization)
                with self.assertRaisesRegex(InvalidJournalValue, "revoked"):
                    journal.read_phase2_window_source(
                        window.window_id,
                        query_cutoff=late_cutoff,
                        calendar_resolver=_calendar(),
                    )
                with self.assertRaisesRegex(InvalidJournalValue, "revoked"):
                    journal.read_phase2_authorization_source(
                        authorization.authorization_id,
                        query_cutoff=late_cutoff,
                        calendar_resolver=_calendar(),
                    )

            with Journal.open(path) as restarted:
                with self.assertRaisesRegex(InvalidJournalValue, "revoked"):
                    restarted.read_phase2_window_source(
                        window.window_id,
                        query_cutoff=late_cutoff,
                        calendar_resolver=_calendar(),
                    )
                with self.assertRaisesRegex(InvalidJournalValue, "revoked"):
                    restarted.read_phase2_authorization_source(
                        authorization.authorization_id,
                        query_cutoff=late_cutoff,
                        calendar_resolver=_calendar(),
                    )

    def test_window_and_authorization_public_api_is_exposed(self) -> None:
        for method_name in (
            "start_phase2_window",
            "read_phase2_window_source",
            "authorize_phase2_signal",
            "read_phase2_authorization_source",
        ):
            with self.subTest(method_name=method_name):
                self.assertTrue(
                    callable(getattr(Journal, method_name, None)),
                    f"Journal.{method_name} is required",
                )

    def test_twenty_trade_twenty_eight_day_gate_issues_window_and_authorization(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            promotion, sessions, promotion_cutoff = (
                _seed_passed_phase1_promotion(journal)
            )
            self.assertTrue(
                validation_module.is_issued_promotion_decision(promotion)
            )
            self.assertTrue(promotion.passed)
            self.assertEqual(promotion.status.value, "PASSED")
            self.assertEqual(promotion.reason_codes, ())
            self.assertEqual(promotion.closed_primary_trades, 20)
            self.assertEqual(
                promotion.elapsed_days,
                (sessions[-1] - date(2026, 8, 13)).days,
            )
            self.assertGreaterEqual(promotion.elapsed_days, 28)

            start_session = _calendar().add_sessions(sessions[-1], 1)
            start_action = _phase2_window_start_action(
                journal,
                session_date=start_session,
                event_at=aware_et(start_session, "08:00"),
            )
            window = journal.start_phase2_window(
                promotion_decision=promotion,
                start_action=start_action,
                calendar_resolver=_calendar(),
            )
            self.assertTrue(
                journal_module.is_verified_phase2_window_source(window)
            )
            self.assertEqual(window.validation_window_id, "1" * 64)
            self.assertEqual(window.started_session, start_session)
            self.assertEqual(window.started_at, start_action.event_time)
            self.assertEqual(window.received_at, start_action.received_at)
            self.assertEqual(window.query_cutoff, start_action.received_at)
            self.assertEqual(
                window.promotion_source.through_session,
                sessions[-1],
            )
            self.assertEqual(
                window.promotion_query_cutoff,
                promotion_cutoff,
            )
            self.assertEqual(window.starting_capital_micros, 5_000_000_000)
            self.assertEqual(
                window.promotion_signal_ids,
                tuple(
                    source.signal_id
                    for source in promotion._phase1_source.signal_sources
                ),
            )
            genesis = journal._connection.execute(
                "SELECT point_kind, session_date, cash_micros, "
                "position_value_micros, equity_micros, "
                "high_water_micros, drawdown_micros, at, received_at "
                "FROM phase2_equity_points WHERE window_id = ?",
                (window.window_id,),
            ).fetchone()
            self.assertEqual(
                genesis,
                (
                    "START",
                    start_session.isoformat(),
                    5_000_000_000,
                    0,
                    5_000_000_000,
                    5_000_000_000,
                    0,
                    journal_module._canonical_timestamp(
                        start_action.event_time
                    ),
                    journal_module._canonical_timestamp(
                        start_action.received_at
                    ),
                ),
            )

            signal_source = _publish_session_primary(
                journal,
                session_date=start_session,
                sequence=21,
            )
            authorization = journal.authorize_phase2_signal(
                window_source=window,
                signal_source=signal_source,
                calendar_resolver=_calendar(),
            )
            self.assertTrue(
                journal_module.is_verified_phase2_authorization_source(
                    authorization
                )
            )
            authorization_time = max(
                window.query_cutoff,
                signal_source.query_cutoff,
            )
            self.assertEqual(
                authorization.window_source.window_id,
                window.window_id,
            )
            self.assertEqual(
                authorization.signal_source.signal_id,
                signal_source.signal_id,
            )
            self.assertEqual(authorization.authorized_at, authorization_time)
            self.assertEqual(authorization.received_at, authorization_time)
            stored_authorization = journal._connection.execute(
                "SELECT window_id, signal_id, signal_source_digest, "
                "authorization_digest, authorized_at, received_at, "
                "source_digest FROM phase2_authorizations "
                "WHERE authorization_id = ?",
                (authorization.authorization_id,),
            ).fetchone()
            self.assertEqual(
                stored_authorization,
                (
                    window.window_id,
                    signal_source.signal_id,
                    signal_source.source_digest,
                    authorization.authorization_digest,
                    journal_module._canonical_timestamp(authorization_time),
                    journal_module._canonical_timestamp(authorization_time),
                    authorization.source_digest,
                ),
            )
            self.assertEqual(journal.count("phase1_signals"), 21)
            self.assertEqual(journal.count("phase2_windows"), 1)
            self.assertEqual(journal.count("phase2_equity_points"), 1)
            self.assertEqual(journal.count("phase2_authorizations"), 1)

    def test_window_and_authorization_identity_retry_and_restart_with_one_session(
        self,
    ) -> None:
        with (
            mock.patch.object(validation_module, "_MINIMUM_TRADES", 1),
            mock.patch.object(validation_module, "_MINIMUM_DAYS", 1),
            tempfile.TemporaryDirectory() as directory,
        ):
            root = Path(directory)
            path = root / "journal.sqlite3"
            with Journal.open(path) as journal:
                promotion, sessions, promotion_cutoff = (
                    _seed_passed_phase1_promotion(
                        journal,
                        session_count=1,
                    )
                )
                same_instant_action = _phase2_window_start_action(
                    journal,
                    session_date=sessions[-1],
                    event_at=promotion_cutoff,
                )
                self.assertFalse(
                    validation_module.is_issued_promotion_decision(promotion),
                    "the required START write must stale the old promotion",
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.start_phase2_window(
                        promotion_decision=promotion,
                        start_action=same_instant_action,
                        calendar_resolver=_calendar(),
                    )

                start_session = _calendar().add_sessions(sessions[-1], 1)
                start_at = aware_et(start_session, "08:00")
                start_action = _phase2_window_start_action(
                    journal,
                    session_date=start_session,
                    event_at=start_at,
                )
                with Journal.open(root / "other.sqlite3") as other:
                    other_action = _phase2_window_start_action(
                        other,
                        session_date=start_session,
                        event_at=start_at,
                    )
                    with self.assertRaises(InvalidJournalValue):
                        journal.start_phase2_window(
                            promotion_decision=promotion,
                            start_action=other_action,
                            calendar_resolver=_calendar(),
                        )
                for bad_promotion, bad_action in (
                    (copy.copy(promotion), start_action),
                    (promotion, copy.copy(start_action)),
                ):
                    with self.subTest(
                        bad_promotion=bad_promotion is not promotion,
                        bad_action=bad_action is not start_action,
                    ):
                        with self.assertRaises(InvalidJournalValue):
                            journal.start_phase2_window(
                                promotion_decision=bad_promotion,
                                start_action=bad_action,
                                calendar_resolver=_calendar(),
                            )

                real_sql = journal_module._sql

                def fail_genesis_insert_once(
                    connection: sqlite3.Connection,
                    statement: str,
                    parameters: tuple[object, ...] = (),
                ) -> sqlite3.Cursor:
                    if statement.startswith(
                        "INSERT INTO phase2_equity_points("
                    ):
                        raise sqlite3.IntegrityError(
                            "injected Phase 2 genesis failure"
                        )
                    return real_sql(connection, statement, parameters)

                with mock.patch.object(
                    journal_module,
                    "_sql",
                    side_effect=fail_genesis_insert_once,
                ):
                    with self.assertRaisesRegex(
                        IdempotencyConflict,
                        "genesis",
                    ):
                        journal.start_phase2_window(
                            promotion_decision=promotion,
                            start_action=start_action,
                            calendar_resolver=_calendar(),
                        )
                self.assertEqual(journal.count("phase2_windows"), 0)
                self.assertEqual(journal.count("phase2_equity_points"), 0)

                window = journal.start_phase2_window(
                    promotion_decision=promotion,
                    start_action=start_action,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_window_source(window)
                )
                self.assertEqual(window.started_session, start_session)
                self.assertEqual(window.started_at, start_action.event_time)
                self.assertEqual(window.received_at, start_action.received_at)
                self.assertEqual(window.query_cutoff, start_action.received_at)
                self.assertEqual(
                    window.promotion_source.through_session,
                    sessions[-1],
                )
                self.assertEqual(window.starting_capital_micros, 5_000_000_000)
                self.assertEqual(journal.count("phase2_equity_points"), 1)
                genesis = journal._connection.execute(
                    "SELECT point_kind, session_date, cash_micros, "
                    "position_value_micros, equity_micros, "
                    "high_water_micros, drawdown_micros, at, received_at "
                    "FROM phase2_equity_points WHERE window_id = ?",
                    (window.window_id,),
                ).fetchone()
                self.assertEqual(
                    genesis,
                    (
                        "START",
                        start_session.isoformat(),
                        5_000_000_000,
                        0,
                        5_000_000_000,
                        5_000_000_000,
                        0,
                        journal_module._canonical_timestamp(
                            start_action.event_time
                        ),
                        journal_module._canonical_timestamp(
                            start_action.received_at
                        ),
                    ),
                )
                self.assertIn(
                    "phase2_equity_points",
                    {reference.table for reference in window.row_references},
                )
                stored_through = journal._connection.execute(
                    "SELECT promotion_through_session FROM phase2_windows "
                    "WHERE window_id = ?",
                    (window.window_id,),
                ).fetchone()
                self.assertEqual(stored_through, (sessions[-1].isoformat(),))
                self.assertIsNone(
                    journal.read_phase2_window_source(
                        window.window_id,
                        query_cutoff=start_action.received_at
                        - timedelta(microseconds=1),
                        calendar_resolver=_calendar(),
                    )
                )

                retry_window = journal.start_phase2_window(
                    promotion_decision=promotion,
                    start_action=start_action,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(retry_window.window_id, window.window_id)
                self.assertEqual(retry_window.source_digest, window.source_digest)
                self.assertEqual(journal.count("phase2_windows"), 1)
                self.assertEqual(journal.count("phase2_equity_points"), 1)

                signal_source = _publish_session_primary(
                    journal,
                    session_date=start_session,
                    sequence=21,
                )
                self.assertFalse(
                    journal_module.is_verified_phase2_window_source(
                        retry_window
                    ),
                    "the required signal write must stale the old window",
                )
                for bad_window, bad_signal in (
                    (copy.copy(retry_window), signal_source),
                    (retry_window, copy.copy(signal_source)),
                    (
                        retry_window,
                        promotion._phase1_source.signal_sources[0],
                    ),
                ):
                    with self.subTest(
                        bad_window=bad_window is not retry_window,
                        bad_signal=bad_signal.signal_id,
                    ):
                        with self.assertRaises(InvalidJournalValue):
                            journal.authorize_phase2_signal(
                                window_source=bad_window,
                                signal_source=bad_signal,
                                calendar_resolver=_calendar(),
                            )

                with Journal.open(root / "signal-owner.sqlite3") as other:
                    _start_window(other)
                    other_signal = _publish_session_primary(
                        other,
                        session_date=_SESSION,
                        sequence=99,
                    )
                    with self.assertRaises(InvalidJournalValue):
                        journal.authorize_phase2_signal(
                            window_source=retry_window,
                            signal_source=other_signal,
                            calendar_resolver=_calendar(),
                        )

                authorization = journal.authorize_phase2_signal(
                    window_source=retry_window,
                    signal_source=signal_source,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_authorization_source(
                        authorization
                    )
                )
                self.assertEqual(
                    authorization.authorized_at,
                    max(retry_window.query_cutoff, signal_source.query_cutoff),
                )
                self.assertEqual(
                    authorization.received_at,
                    authorization.authorized_at,
                )
                issued = options_paper_module._authorization_for_source(
                    authorization,
                    calendar_resolver=_calendar(),
                )
                self.assertIsNotNone(issued)
                assert issued is not None
                self.assertEqual(
                    issued.source_digest,
                    authorization.authorization_digest,
                )
                self.assertIsNone(
                    journal.read_phase2_authorization_source(
                        authorization.authorization_id,
                        query_cutoff=authorization.received_at
                        - timedelta(microseconds=1),
                        calendar_resolver=_calendar(),
                    )
                )

                retry_authorization = journal.authorize_phase2_signal(
                    window_source=retry_window,
                    signal_source=signal_source,
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(
                    retry_authorization.authorization_id,
                    authorization.authorization_id,
                )
                self.assertEqual(
                    retry_authorization.source_digest,
                    authorization.source_digest,
                )
                self.assertEqual(journal.count("phase2_authorizations"), 1)
                window_id = window.window_id
                authorization_id = authorization.authorization_id
                authorization_cutoff = authorization.received_at

            self.assertFalse(
                journal_module.is_verified_phase2_window_source(window)
            )
            self.assertFalse(
                journal_module.is_verified_phase2_authorization_source(
                    authorization
                )
            )
            with Journal.open(path) as restarted:
                restored_window = restarted.read_phase2_window_source(
                    window_id,
                    query_cutoff=authorization_cutoff,
                    calendar_resolver=_calendar(),
                )
                restored_authorization = (
                    restarted.read_phase2_authorization_source(
                        authorization_id,
                        query_cutoff=authorization_cutoff,
                        calendar_resolver=_calendar(),
                    )
                )
                self.assertIsNotNone(restored_window)
                self.assertIsNotNone(restored_authorization)
                assert restored_window is not None
                assert restored_authorization is not None
                self.assertTrue(
                    journal_module.is_verified_phase2_window_source(
                        restored_window
                    )
                )
                self.assertTrue(
                    journal_module.is_verified_phase2_authorization_source(
                        restored_authorization
                    )
                )
                self.assertEqual(restarted.count("phase2_equity_points"), 1)
                self.assertIn(
                    "phase2_equity_points",
                    {
                        reference.table
                        for reference in restored_window.row_references
                    },
                )
                self.assertEqual(restored_window.source_digest, window.source_digest)
                self.assertEqual(
                    restored_authorization.source_digest,
                    authorization.source_digest,
                )
                restored_issued = options_paper_module._authorization_for_source(
                    restored_authorization,
                    calendar_resolver=_calendar(),
                )
                self.assertIsNotNone(restored_issued)


class HistoricalReplayJournalIntegrationTests(unittest.TestCase):
    def _selector_case(self, session_date: date) -> ReplayCase:
        return ReplayCase(
            session_date=session_date,
            domain_results=tuple(
                ReplayDomainResult(
                    component,
                    ReplayDomainStatus.PASSED,
                )
                for component in ReplayDomainComponent
            ),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )

    def _archive(
        self,
        journal: Journal,
        *,
        run_id: str,
        session_dates: tuple[date, ...],
        query_cutoff: datetime,
    ):
        with mock.patch.object(
            journal_module,
            "_utc_now",
            return_value=query_cutoff + timedelta(minutes=1),
        ):
            return journal.archive_historical_replay_source(
                run_id=run_id,
                session_dates=session_dates,
                query_cutoff=query_cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )

    def test_replay_row_converters_cannot_issue_nonpersisted_sources(self) -> None:
        query_cutoff = aware_et(_SESSION, "16:30")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            _seed_completed_authority_fill(journal)
            self._archive(
                journal,
                run_id="f" * 64,
                session_dates=(_SESSION,),
                query_cutoff=query_cutoff,
            )
            run_row = journal._connection.execute(
                "SELECT "
                + ", ".join(journal_module._HISTORICAL_REPLAY_RUN_COLUMNS)
                + " FROM historical_replay_runs WHERE replay_run_id = ?",
                ("f" * 64,),
            ).fetchone()
            date_row = journal._connection.execute(
                "SELECT "
                + ", ".join(journal_module._HISTORICAL_REPLAY_DATE_COLUMNS)
                + " FROM historical_replay_dates WHERE replay_run_id = ?",
                ("f" * 64,),
            ).fetchone()
            evidence_row = journal._connection.execute(
                "SELECT "
                + ", ".join(journal_module._HISTORICAL_REPLAY_EVIDENCE_COLUMNS)
                + " FROM historical_replay_evidence LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(run_row)
            self.assertIsNotNone(date_row)
            self.assertIsNotNone(evidence_row)
            assert run_row is not None
            assert date_row is not None
            assert evidence_row is not None

            forged_run_row = list(run_row)
            forged_run_row[0] = int(run_row[0]) + 10_000
            forged_run_row[-1] = journal_module._historical_replay_record_digest(
                journal_module._HISTORICAL_REPLAY_RUN_COLUMNS,
                tuple(forged_run_row[1:-1]),
            )
            forged_run = journal._historical_replay_source_from_rows(
                forged_run_row,
                query_cutoff=query_cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            self.assertFalse(
                journal_module.is_verified_historical_replay_source(forged_run)
            )
            self.assertFalse(is_replay_registered_source(forged_run))
            self.assertTrue(
                all(
                    not journal_module.is_verified_historical_replay_date_source(
                        item
                    )
                    for item in forged_run.date_sources
                )
            )
            self.assertTrue(
                all(
                    not journal_module.is_verified_historical_replay_evidence_source(
                        evidence
                    )
                    for item in forged_run.date_sources
                    for evidence in item.evidence_sources
                )
            )

            forged_date_row = list(date_row)
            forged_date_row[0] = int(date_row[0]) + 10_000
            forged_date_row[-1] = journal_module._historical_replay_record_digest(
                journal_module._HISTORICAL_REPLAY_DATE_COLUMNS,
                tuple(forged_date_row[1:-1]),
            )
            forged_date, forged_evidence = (
                journal._historical_replay_date_source_from_row(
                    forged_date_row,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
            )
            self.assertFalse(
                journal_module.is_verified_historical_replay_date_source(
                    forged_date
                )
            )
            self.assertTrue(
                all(
                    not journal_module.is_verified_historical_replay_evidence_source(
                        evidence
                    )
                    for evidence in forged_evidence
                )
            )

            forged_evidence_row = list(evidence_row)
            forged_evidence_row[0] = int(evidence_row[0]) + 10_000
            forged_evidence_row[-1] = (
                journal_module._historical_replay_record_digest(
                    journal_module._HISTORICAL_REPLAY_EVIDENCE_COLUMNS,
                    tuple(forged_evidence_row[1:-1]),
                )
            )
            forged_evidence_source = (
                journal._historical_replay_evidence_source_from_row(
                    forged_evidence_row,
                    report_cutoff=query_cutoff,
                )
            )
            self.assertFalse(
                journal_module.is_verified_historical_replay_evidence_source(
                    forged_evidence_source
                )
            )

    def test_generic_release_pins_stay_incomplete_across_restart(
        self,
    ) -> None:
        query_cutoff = aware_et(_SESSION, "16:30")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            journal = Journal.open(path)
            self.addCleanup(journal.close)
            _seed_completed_authority_fill(journal)

            source = self._archive(
                journal,
                run_id="a" * 64,
                session_dates=(_SESSION,),
                query_cutoff=query_cutoff,
            )
            self.assertEqual(
                tuple(
                    item.role
                    for item in source.date_sources[0].evidence_sources
                ),
                ("SOURCE_EVIDENCE",),
            )
            rederived = source.date_sources[0]._rederive_historical_replay_case()

            self.assertTrue(
                journal_module.is_verified_historical_replay_source(source)
            )
            self.assertTrue(is_replay_registered_source(source))
            self.assertIsInstance(rederived, ReplayCase)
            assert isinstance(rederived, ReplayCase)
            first = replay_point_in_time(
                ReplayRequest(
                    cases=(rederived,),
                    historical_source=source,
                )
            )
            self.assertEqual((first.included_dates, first.excluded_dates), (0, 1))
            self.assertEqual(
                first.results[0].reason_codes,
                (
                    "MISSING_UNIVERSE_MEMBERSHIP",
                    "MISSING_EVENT_STATE",
                ),
            )
            self.assertNotIn("STRICT_POINT_IN_TIME_EVIDENCE", first.labels)

            journal.close()
            self.assertFalse(
                journal_module.is_verified_historical_replay_source(source)
            )
            self.assertFalse(is_replay_registered_source(source))
            restarted = Journal.open(path)
            self.addCleanup(restarted.close)
            restored = restarted.read_historical_replay_source(
                "a" * 64,
                query_cutoff=query_cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            self.assertIsNotNone(restored)
            assert restored is not None
            restored_case = (
                restored.date_sources[0]._rederive_historical_replay_case()
            )
            self.assertIsInstance(restored_case, ReplayCase)
            assert isinstance(restored_case, ReplayCase)
            self.assertEqual(restored_case, rederived)
            self.assertIsNot(restored_case, rederived)
            self.assertTrue(
                journal_module.is_verified_historical_replay_source(restored)
            )
            self.assertTrue(is_replay_registered_source(restored))
            second = replay_point_in_time(
                ReplayRequest(
                    cases=(restored_case,),
                    historical_source=restored,
                )
            )
            self.assertEqual(second.exclusion_counts, first.exclusion_counts)
            self.assertNotIn("STRICT_POINT_IN_TIME_EVIDENCE", second.labels)

    def test_incomplete_roles_remain_excluded_after_late_phase1_material(
        self,
    ) -> None:
        query_cutoff = aware_et(_SESSION, "16:30")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            source = self._archive(
                journal,
                run_id="b" * 64,
                session_dates=(_SESSION,),
                query_cutoff=query_cutoff,
            )
            selector = self._selector_case(_SESSION)
            before = replay_point_in_time(
                ReplayRequest(
                    cases=(selector,),
                    historical_source=source,
                )
            )
            self.assertEqual(
                before.exclusion_counts,
                (
                    ("MISSING_EVENT_STATE", 1),
                    ("MISSING_SOURCE_EVIDENCE", 1),
                    ("MISSING_UNIVERSE_MEMBERSHIP", 1),
                ),
            )

            _seed_completed_authority_fill(journal)
            retried = self._archive(
                journal,
                run_id="b" * 64,
                session_dates=(_SESSION,),
                query_cutoff=query_cutoff,
            )
            self.assertEqual(retried.date_sources[0].evidence_sources, ())
            restored = journal.read_historical_replay_source(
                "b" * 64,
                query_cutoff=query_cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            self.assertIsNotNone(restored)
            assert restored is not None
            self.assertEqual(restored.date_sources[0].evidence_sources, ())
            after = replay_point_in_time(
                ReplayRequest(
                    cases=(selector,),
                    historical_source=restored,
                )
            )
            self.assertEqual(after.exclusion_counts, before.exclusion_counts)

    def test_exact_source_date_coverage_rejects_request_subset(self) -> None:
        sessions = (_SESSION, _calendar().add_sessions(_SESSION, 1))
        query_cutoff = aware_et(sessions[-1], "16:30")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            source = self._archive(
                journal,
                run_id="c" * 64,
                session_dates=sessions,
                query_cutoff=query_cutoff,
            )

            result = replay_point_in_time(
                ReplayRequest(
                    cases=(self._selector_case(_SESSION),),
                    historical_source=source,
                )
            )

            self.assertEqual(result.included_dates, 0)
            self.assertEqual(
                result.exclusion_counts,
                (("HISTORICAL_REPLAY_DATE_COVERAGE_MISMATCH", 1),),
            )

    def test_copy_raw_tamper_and_stale_sources_cannot_mint_authority(self) -> None:
        query_cutoff = aware_et(_SESSION, "16:30")
        with tempfile.TemporaryDirectory() as directory, Journal.open(
            Path(directory) / "journal.sqlite3"
        ) as journal:
            source = self._archive(
                journal,
                run_id="d" * 64,
                session_dates=(_SESSION,),
                query_cutoff=query_cutoff,
            )
            raw = journal_module.HistoricalReplaySource(
                **{
                    item.name: getattr(source, item.name)
                    for item in fields(journal_module.HistoricalReplaySource)
                }
            )
            copied = copy.copy(source)
            for candidate in (raw, copied):
                with self.subTest(candidate=candidate):
                    self.assertFalse(
                        journal_module.is_verified_historical_replay_source(
                            candidate
                        )
                    )
                    self.assertFalse(is_replay_registered_source(candidate))
                    with self.assertRaisesRegex(
                        ReplayError,
                        "UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY",
                    ):
                        _register_historical_replay_source(candidate)

            journal.append_source_observation(
                payload=b'{"late":true}',
                source_uri="https://example.invalid/late-replay-evidence",
                source_type="MARKET_DATA",
                provider="fixture",
                feed="SIP",
                source_time=query_cutoff + timedelta(minutes=1),
                retrieved_at=query_cutoff + timedelta(minutes=2),
                provider_sequence=1,
                delay_seconds=0,
                health_result="OK",
            )
            self.assertFalse(
                journal_module.is_verified_historical_replay_source(source)
            )
            self.assertFalse(is_replay_registered_source(source))

            reissued = journal.read_historical_replay_source(
                "d" * 64,
                query_cutoff=query_cutoff,
                calendar_resolver=_calendar(),
                policy=policy_fixture(),
            )
            self.assertIsNotNone(reissued)
            assert reissued is not None
            object.__setattr__(reissued, "source_digest", "0" * 64)
            self.assertFalse(
                journal_module.is_verified_historical_replay_source(reissued)
            )
            self.assertFalse(is_replay_registered_source(reissued))


if __name__ == "__main__":
    unittest.main()
