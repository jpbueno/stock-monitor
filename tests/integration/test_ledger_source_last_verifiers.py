from __future__ import annotations

import inspect
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from pathlib import Path
from types import FunctionType, SimpleNamespace

import stock_monitor.ledger as ledger_module
import stock_monitor.market_calendar as market_calendar_module
import stock_monitor.reconciliation as reconciliation_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import Journal
from stock_monitor.market_calendar import (
    is_release_verified_market_calendar,
    load_current_market_calendar,
)
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    replay_actual,
)
from stock_monitor.risk import RiskBlock, SessionCalendarResolver
from tests.integration.test_phase1_authorities import (
    _SESSION,
    _append_unrelated_source,
    _append_completed_entry_observations,
    _calendar,
    _issued_candidates,
    _issued_publication,
    _publish,
    _seed_completed_authority_fill,
    _signal,
    _start_window,
    aware_et,
)
from tests.support import policy_fixture


ROOT = Path(__file__).resolve().parents[2]


def _confirmation(
    message_id: str,
    text: str,
    *,
    message_time: str,
    received_at: str,
) -> ConfirmationEnvelope:
    return ConfirmationEnvelope(
        message_id=message_id,
        text=text,
        message_time=datetime.fromisoformat(message_time),
        received_at=datetime.fromisoformat(received_at),
        session_date=_SESSION,
    )


class LedgerSourceLastVerifierTests(unittest.TestCase):
    def _actual_projection(self, journal: Journal):
        calendar = SessionCalendarResolver(
            (load_current_market_calendar(ROOT, as_of=_SESSION),)
        )
        plans = UnavailableSignalPlanResolver()
        entry_authorities = UnavailableActualEntryAuthorityResolver()

        def ingest(item: ConfirmationEnvelope) -> None:
            ingest_confirmation(
                journal,
                item,
                plans=plans,
                calendar=calendar,
                policy=policy_fixture(),
                entry_authorities=entry_authorities,
            )

        ingest(
            _confirmation(
                "message:ledger-verifier-buy",
                "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        ingest(
            _confirmation(
                "message:ledger-verifier-check",
                "ACCOUNT CHECK settled_cash 4800 pending_orders 0 "
                "unlogged_positions 0 AT 10:15 ET",
                message_time="2026-08-14T10:15:30-04:00",
                received_at="2026-08-14T10:15:31-04:00",
            )
        )
        cutoff = datetime.fromisoformat("2026-08-14T10:16:00-04:00")
        with journal.transaction() as transaction:
            source = transaction.read_actual_replay(query_cutoff=cutoff)
        state = replay_actual(
            source,
            plans=plans,
            calendar=calendar,
            policy=policy_fixture(),
        )
        cohort = ledger_module._issue_actual_projection_from_journal(source, state)
        return source, state, cohort

    def test_ledger_signal_mutated_by_source_callback_is_never_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                signal = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                self.assertEqual(signal.planned_shares, 47)
                self.assertTrue(ledger_module.is_issued_ledger_signal(signal))
                observed = {"fired": False}

                def mutate_signal(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(signal, "planned_shares", 147)

                journal._connection.set_trace_callback(mutate_signal)
                try:
                    accepted_during_callback = (
                        ledger_module.is_issued_ledger_signal(signal)
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertEqual(signal.planned_shares, 147)
                self.assertEqual(
                    (
                        accepted_during_callback,
                        ledger_module.is_issued_ledger_signal(signal),
                    ),
                    (False, False),
                )

    def test_shadow_fill_mutated_by_source_callback_is_never_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal, candidates_override=_issued_candidates(2))
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                shadow_source = replay_source.signal_sources[1]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        shadow_source,
                    )
                )
                authority = journal.record_phase1_entry(
                    shadow_source.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                original_ordinal = authority.action_ordinal
                self.assertTrue(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        authority
                    )
                )
                observed = {"fired": False}

                def mutate_authority(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(
                            authority,
                            "action_ordinal",
                            original_ordinal + 1,
                        )

                journal._connection.set_trace_callback(mutate_authority)
                try:
                    accepted_during_callback = (
                        ledger_module.is_issued_shadow_fill_disposition_authority(
                            authority
                        )
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertEqual(authority.action_ordinal, original_ordinal + 1)
                self.assertEqual(
                    (
                        accepted_during_callback,
                        ledger_module.is_issued_shadow_fill_disposition_authority(
                            authority
                        ),
                    ),
                    (False, False),
                )

    def test_paper_entry_mutated_by_source_callback_is_never_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, _completed_at = _seed_completed_authority_fill(journal)
                original_ordinal = authority.action_ordinal
                self.assertTrue(
                    ledger_module.is_issued_paper_entry_authority(authority)
                )
                observed = {"fired": False}

                def mutate_authority(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(
                            authority,
                            "action_ordinal",
                            original_ordinal + 1,
                        )

                journal._connection.set_trace_callback(mutate_authority)
                try:
                    accepted_during_callback = (
                        ledger_module.is_issued_paper_entry_authority(authority)
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertEqual(authority.action_ordinal, original_ordinal + 1)
                self.assertEqual(
                    (
                        accepted_during_callback,
                        ledger_module.is_issued_paper_entry_authority(authority),
                    ),
                    (False, False),
                )

    def test_actual_projection_mutated_by_source_callback_is_never_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _source, state, cohort = self._actual_projection(journal)
                original_count = cohort.expected_action_count
                self.assertTrue(
                    ledger_module.is_issued_actual_projection_cohort(cohort)
                )
                observed = {"fired": False}

                def mutate_cohort(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(
                            cohort,
                            "expected_action_count",
                            original_count + 1,
                        )

                journal._connection.set_trace_callback(mutate_cohort)
                try:
                    accepted_during_callback = (
                        ledger_module.is_issued_actual_projection_cohort(cohort)
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertEqual(
                    cohort.expected_action_count,
                    original_count + 1,
                )
                self.assertEqual(
                    (
                        accepted_during_callback,
                        ledger_module.is_issued_actual_projection_cohort(cohort),
                    ),
                    (False, False),
                )

    def test_actual_projection_requires_exact_position_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _source, state, cohort = self._actual_projection(journal)
                original_position = cohort.positions[0]
                copied_position = replace(original_position)

                self.assertTrue(
                    ledger_module.is_issued_actual_projection_cohort(cohort)
                )
                self.assertIsNot(copied_position, original_position)
                self.assertEqual(copied_position, original_position)
                object.__setattr__(state, "positions", (copied_position,))
                object.__setattr__(cohort, "positions", (copied_position,))

                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(cohort)
                )

    def test_verified_replay_mutated_by_source_callback_is_never_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                cohort = replay.cohort
                original_count = cohort.expected_count
                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(cohort)
                )
                observed = {"fired": False}

                def mutate_cohort(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(
                            cohort,
                            "expected_count",
                            original_count + 1,
                        )

                journal._connection.set_trace_callback(mutate_cohort)
                try:
                    accepted_during_callback = (
                        ledger_module.is_issued_verified_replay_cohort(cohort)
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertEqual(cohort.expected_count, original_count + 1)
                self.assertEqual(
                    (
                        accepted_during_callback,
                        ledger_module.is_issued_verified_replay_cohort(cohort),
                    ),
                    (False, False),
                )

    def test_family_verifiers_keep_callbacks_before_hook_free_final_seals(
        self,
    ) -> None:
        phase1_verifiers = (
            ledger_module.is_issued_ledger_signal,
            ledger_module.is_issued_paper_entry_authority,
            ledger_module.is_issued_shadow_fill_disposition_authority,
            ledger_module.is_issued_verified_replay_cohort,
        )
        for verifier in phase1_verifiers:
            with self.subTest(verifier=verifier.__name__):
                source = inspect.getsource(verifier)
                self.assertLess(
                    source.index("_phase1_authority_sources_are_current"),
                    source.index("_is_current_phase1_derived_authority"),
                )
                self.assertNotIn("_phase1_sources_are_current", source)

        actual_source = inspect.getsource(
            ledger_module.is_issued_actual_projection_cohort
        )
        self.assertLess(
            actual_source.index("is_verified_actual_ledger_state_for_source"),
            actual_source.index("content_verifier("),
        )
        self.assertIn("_source_fingerprint_seals_equal", actual_source)

        fingerprints = (
            ledger_module._ledger_signal_fingerprint,
            ledger_module._paper_entry_fingerprint,
            ledger_module._shadow_fill_disposition_fingerprint,
            ledger_module._actual_projection_cohort_fingerprint,
            ledger_module._replay_cohort_fingerprint,
        )
        for fingerprint in fingerprints:
            with self.subTest(fingerprint=fingerprint.__name__):
                source = inspect.getsource(fingerprint)
                self.assertIn("_source_fingerprint_seal", source)
                self.assertIn("_MERKLE_OPAQUE_STRUCTURAL", source)

    def test_stale_signal_cannot_be_rebound_to_another_current_journal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            first_path = Path(temporary_directory) / "first.db"
            second_path = Path(temporary_directory) / "second.db"
            with Journal.open(first_path) as first_journal, Journal.open(
                second_path
            ) as second_journal:
                _publish(first_journal)
                _publish(second_journal)
                first_signal = first_journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                second_signal = second_journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                second_bindings = ledger_module._phase1_bound_sources(
                    second_signal
                )
                self.assertEqual(len(second_bindings), 1)

                _append_unrelated_source(first_journal, suffix="revoke-first")
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(first_signal)
                )
                self.assertTrue(
                    ledger_module.is_issued_ledger_signal(second_signal)
                )

                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_SOURCE_BINDING_ISSUER_UNVERIFIED",
                ):
                    ledger_module._bind_phase1_sources(
                        first_signal,
                        ((second_bindings[0][0], "SIGNAL"),),
                    )

                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(first_signal)
                )

    def test_mutated_signal_cannot_overwrite_its_original_registry_seal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                signal = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                object.__setattr__(signal, "planned_shares", 147)
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(signal)
                )

                with self.assertRaisesRegex(
                    RiskBlock,
                    "PHASE1_DERIVED_AUTHORITY_ISSUER_UNVERIFIED",
                ):
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._ISSUED_LEDGER_SIGNALS,
                        signal,
                        ledger_module._ledger_signal_fingerprint(signal),
                    )

                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(signal)
                )

    def test_equal_signal_copy_cannot_be_first_minted_by_raw_registrar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                copied = replace(genuine)

                self.assertTrue(ledger_module.is_issued_ledger_signal(genuine))
                self.assertFalse(ledger_module.is_issued_ledger_signal(copied))
                self.assertEqual(copied, genuine)
                self.assertIsNot(copied, genuine)
                try:
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._ISSUED_LEDGER_SIGNALS,
                        copied,
                        ledger_module._ledger_signal_fingerprint(copied),
                    )
                    ledger_module._bind_phase1_sources(
                        copied,
                        ledger_module._phase1_bound_sources(genuine),
                    )
                except RiskBlock:
                    pass
                self.assertFalse(ledger_module.is_issued_ledger_signal(copied))

    def test_equal_paper_entry_copy_cannot_be_first_minted_by_raw_registrar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                genuine, _completed_at = _seed_completed_authority_fill(journal)
                copied = replace(genuine)

                self.assertTrue(
                    ledger_module.is_issued_paper_entry_authority(genuine)
                )
                self.assertFalse(
                    ledger_module.is_issued_paper_entry_authority(copied)
                )
                try:
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._PAPER_ENTRY_AUTHORITIES,
                        copied,
                        ledger_module._paper_entry_fingerprint(copied),
                    )
                    ledger_module._bind_phase1_sources(
                        copied,
                        ledger_module._phase1_bound_sources(genuine),
                    )
                except RiskBlock:
                    pass
                self.assertFalse(
                    ledger_module.is_issued_paper_entry_authority(copied)
                )

    def test_equal_shadow_fill_copy_cannot_be_first_minted_by_raw_registrar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal, candidates_override=_issued_candidates(2))
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                shadow_source = replay_source.signal_sources[1]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        shadow_source,
                    )
                )
                genuine = journal.record_phase1_entry(
                    shadow_source.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                copied = replace(genuine)

                self.assertTrue(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        genuine
                    )
                )
                self.assertFalse(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        copied
                    )
                )
                try:
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._SHADOW_FILL_DISPOSITION_AUTHORITIES,
                        copied,
                        ledger_module._shadow_fill_disposition_fingerprint(
                            copied
                        ),
                    )
                    ledger_module._bind_phase1_sources(
                        copied,
                        ledger_module._phase1_bound_sources(genuine),
                    )
                except RiskBlock:
                    pass
                self.assertFalse(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        copied
                    )
                )

    def test_equal_replay_cohort_copy_cannot_be_first_minted_by_raw_registrar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                genuine = replay.cohort
                copied = replace(genuine)

                self.assertTrue(
                    ledger_module.is_issued_verified_replay_cohort(genuine)
                )
                self.assertFalse(
                    ledger_module.is_issued_verified_replay_cohort(copied)
                )
                try:
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._VERIFIED_REPLAY_COHORT_AUTHORITIES,
                        copied,
                        ledger_module._replay_cohort_fingerprint(copied),
                    )
                    ledger_module._bind_phase1_sources(
                        copied,
                        ledger_module._phase1_bound_sources(genuine),
                    )
                except RiskBlock:
                    pass
                self.assertFalse(
                    ledger_module.is_issued_verified_replay_cohort(copied)
                )

    def test_forged_signal_source_converter_never_mints_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine_signal = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                genuine_source = ledger_module._phase1_bound_sources(
                    genuine_signal
                )[0][0]
                forged_source = replace(genuine_source, symbol="QQQ")

                forged_signal = ledger_module._construct_phase1_signal(
                    forged_source,
                    binding_source=genuine_source,
                    binding_kind="SIGNAL",
                )

                self.assertEqual(forged_signal.symbol, "QQQ")
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(forged_signal)
                )

    def test_raw_verified_batch_registrar_cannot_mint_authority(self) -> None:
        raw = ledger_module.VerifiedLedgerEventBatch(
            (("forged:event", "a" * 64),)
        )
        self.assertFalse(ledger_module._is_issued_verified_batch(raw))

        with self.assertRaisesRegex(
            RiskBlock,
            "VERIFIED_LEDGER_BATCH_ISSUER_UNVERIFIED",
        ):
            ledger_module._register_verified_batch(raw)

        self.assertFalse(ledger_module._is_issued_verified_batch(raw))

    def test_forged_paper_source_converter_never_mints_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                genuine_authority, _completed_at = (
                    _seed_completed_authority_fill(journal)
                )
                genuine_source = ledger_module._phase1_bound_sources(
                    genuine_authority
                )[0][0]
                signal = journal.read_phase1_signal(
                    genuine_authority.signal_id,
                    query_cutoff=genuine_source.query_cutoff,
                )
                forged_source = replace(genuine_source, source_digest="b" * 64)

                forged_authority = ledger_module._paper_entry_from_source_material(
                    forged_source,
                    signal=signal,
                    calendar_digest=genuine_source.calendar_digest,
                    binding_source=genuine_source,
                    binding_kind="ENTRY",
                )

                self.assertEqual(forged_authority.source_digest, "b" * 64)
                self.assertFalse(
                    ledger_module.is_issued_paper_entry_authority(
                        forged_authority
                    )
                )

    def test_unissued_quote_cannot_be_first_minted_by_phase1_registrar(
        self,
    ) -> None:
        quote = ledger_module._issue_execution_quote_evidence(
            symbol="SPY",
            bid=ledger_module.Decimal("499.99"),
            ask=ledger_module.Decimal("500.00"),
            observed_at=datetime.fromisoformat("2026-08-14T10:14:00-04:00"),
            confirmed_at=datetime.fromisoformat("2026-08-14T10:15:00-04:00"),
            cursor=1,
            source="UNVERIFIED_FIXTURE",
        )
        self.assertFalse(
            ledger_module.is_issued_execution_quote_evidence(quote)
        )

        try:
            ledger_module._register_phase1_derived_authority(
                ledger_module._EXECUTION_QUOTE_AUTHORITIES,
                quote,
                ledger_module._execution_quote_fingerprint(quote),
            )
        except RiskBlock:
            pass

        self.assertFalse(
            ledger_module.is_issued_execution_quote_evidence(quote)
        )

    def test_equal_actual_projection_copy_is_not_issued(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _source, _state, genuine = self._actual_projection(journal)
                copied = replace(genuine)

                self.assertTrue(
                    ledger_module.is_issued_actual_projection_cohort(genuine)
                )
                self.assertEqual(copied, genuine)
                self.assertIsNot(copied, genuine)
                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(copied)
                )

    def test_calendar_callback_source_aba_cannot_issue_actual_state_or_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, _genuine_state, _genuine_cohort = self._actual_projection(
                    journal
                )
                action = source.actions[0]
                original_received_at = action.received_at
                forged_received_at = original_received_at + timedelta(seconds=7)
                observed = {"fired": False}

                class MutatingCalendarResolver(SessionCalendarResolver):
                    @property
                    def release_verified(self) -> bool:
                        if not observed["fired"]:
                            observed["fired"] = True
                            object.__setattr__(
                                action,
                                "received_at",
                                forged_received_at,
                            )
                        return super().release_verified

                calendar = MutatingCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                state = None
                try:
                    state = replay_actual(
                        source,
                        plans=UnavailableSignalPlanResolver(),
                        calendar=calendar,
                        policy=policy_fixture(),
                    )
                except (RiskBlock, ValueError):
                    pass
                self.assertTrue(observed["fired"])
                object.__setattr__(action, "received_at", original_received_at)

                accepted = state is not None and (
                    reconciliation_module.is_verified_actual_ledger_state_for_source(
                        state,
                        source,
                    )
                )
                cohort = None
                if state is not None:
                    try:
                        cohort = ledger_module._issue_actual_projection_from_journal(
                            source,
                            state,
                        )
                    except RiskBlock:
                        pass
                cohort_issued = (
                    cohort is not None
                    and ledger_module.is_issued_actual_projection_cohort(cohort)
                )

                self.assertTrue(
                    reconciliation_module.is_verified_journal_replay_source(
                        source
                    )
                )
                self.assertEqual((accepted, cohort_issued), (False, False))

    def test_calendar_trace_mutation_cannot_issue_actual_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                plans = UnavailableSignalPlanResolver()
                policy = policy_fixture()
                entry_authorities = UnavailableActualEntryAuthorityResolver()
                items = (
                    _confirmation(
                        "message:calendar-trace-buy",
                        "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                        message_time="2026-08-14T10:15:00-04:00",
                        received_at="2026-08-14T10:15:01-04:00",
                    ),
                    _confirmation(
                        "message:calendar-trace-fee",
                        "FEE SPY 0.03 AT 10:16 ET",
                        message_time="2026-08-14T10:16:30-04:00",
                        received_at="2026-08-14T10:16:31-04:00",
                    ),
                    _confirmation(
                        "message:calendar-trace-sale",
                        "SOLD SPY 1 shares @ 101 AT 15:30 ET",
                        message_time="2026-08-14T15:30:30-04:00",
                        received_at="2026-08-14T15:30:31-04:00",
                    ),
                    _confirmation(
                        "message:calendar-trace-check",
                        "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
                        "unlogged_positions 0 AT 15:31 ET",
                        message_time="2026-08-14T15:31:30-04:00",
                        received_at="2026-08-14T15:31:31-04:00",
                    ),
                )
                for item in items:
                    ingest_confirmation(
                        journal,
                        item,
                        plans=plans,
                        calendar=calendar,
                        policy=policy,
                        entry_authorities=entry_authorities,
                    )
                cutoff = datetime.fromisoformat("2026-08-17T10:00:00-04:00")
                with journal.transaction() as transaction:
                    source = transaction.read_actual_replay(query_cutoff=cutoff)

                market_calendar = calendar.calendars[0]
                original_closed_dates = market_calendar._closed_date_set
                forged_closed_dates = frozenset(
                    (*original_closed_dates, date(2026, 8, 17))
                )
                observed = {"fired": False}

                def mutate_calendar(statement: str) -> None:
                    if (
                        statement.strip().upper() == "PRAGMA DATA_VERSION"
                        and not observed["fired"]
                    ):
                        observed["fired"] = True
                        object.__setattr__(
                            market_calendar,
                            "_closed_date_set",
                            forged_closed_dates,
                        )

                state = None
                journal._connection.set_trace_callback(mutate_calendar)
                try:
                    try:
                        state = replay_actual(
                            source,
                            plans=plans,
                            calendar=calendar,
                            policy=policy,
                        )
                    except (RiskBlock, ValueError):
                        pass
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(observed["fired"])
                self.assertFalse(
                    is_release_verified_market_calendar(market_calendar)
                )
                accepted = state is not None and (
                    reconciliation_module.is_verified_actual_ledger_state_for_source(
                        state,
                        source,
                    )
                )
                forged_cash = (
                    None if state is None else state.strategy_settled_cash_micros
                )
                object.__setattr__(
                    market_calendar,
                    "_closed_date_set",
                    original_closed_dates,
                )

                self.assertEqual((accepted, forged_cash), (False, None))

    def test_self_restoring_market_calendar_method_cannot_issue_actual_state_or_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                plans = UnavailableSignalPlanResolver()
                policy = policy_fixture()
                entry_authorities = UnavailableActualEntryAuthorityResolver()
                items = (
                    _confirmation(
                        "message:calendar-method-buy",
                        "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                        message_time="2026-08-14T10:15:00-04:00",
                        received_at="2026-08-14T10:15:01-04:00",
                    ),
                    _confirmation(
                        "message:calendar-method-fee",
                        "FEE SPY 0.03 AT 10:16 ET",
                        message_time="2026-08-14T10:16:30-04:00",
                        received_at="2026-08-14T10:16:31-04:00",
                    ),
                    _confirmation(
                        "message:calendar-method-sale",
                        "SOLD SPY 1 shares @ 101 AT 15:30 ET",
                        message_time="2026-08-14T15:30:30-04:00",
                        received_at="2026-08-14T15:30:31-04:00",
                    ),
                    _confirmation(
                        "message:calendar-method-check",
                        "ACCOUNT CHECK settled_cash 4900 pending_orders 0 "
                        "unlogged_positions 0 AT 15:31 ET",
                        message_time="2026-08-14T15:31:30-04:00",
                        received_at="2026-08-14T15:31:31-04:00",
                    ),
                )
                for item in items:
                    ingest_confirmation(
                        journal,
                        item,
                        plans=plans,
                        calendar=calendar,
                        policy=policy,
                        entry_authorities=entry_authorities,
                    )
                cutoff = datetime.fromisoformat("2026-08-17T10:00:00-04:00")
                with journal.transaction() as transaction:
                    source = transaction.read_actual_replay(query_cutoff=cutoff)

                market_calendar = calendar.calendars[0]
                calendar_type = type(market_calendar)
                original_is_open = calendar_type.is_open
                target = date(2026, 8, 17)

                def arm(restoring_call: int) -> dict[str, int]:
                    observed = {"target_calls": 0}

                    def forged_is_open(value: object, day: date) -> bool:
                        if value is market_calendar and day == target:
                            observed["target_calls"] += 1
                            if observed["target_calls"] == restoring_call:
                                calendar_type.is_open = original_is_open
                            return False
                        return original_is_open(value, day)

                    calendar_type.is_open = forged_is_open
                    return observed

                state = None
                state_verified = False
                cohort = None
                cohort_verified = False
                try:
                    install_calls = arm(4)
                    try:
                        state = replay_actual(
                            source,
                            plans=plans,
                            calendar=calendar,
                            policy=policy,
                        )
                    except (RiskBlock, ValueError):
                        pass
                    if state is not None:
                        self.assertIs(calendar_type.is_open, original_is_open)
                    if state is not None:
                        verify_calls = arm(2)
                        state_verified = (
                            reconciliation_module.is_verified_actual_ledger_state_for_source(
                                state,
                                source,
                            )
                        )
                        self.assertIs(calendar_type.is_open, original_is_open)
                        issue_calls = arm(10)
                        try:
                            cohort = ledger_module._issue_actual_projection_from_journal(
                                source,
                                state,
                            )
                        except RiskBlock:
                            pass
                        self.assertIs(calendar_type.is_open, original_is_open)
                        if cohort is not None:
                            cohort_verify_calls = arm(4)
                            cohort_verified = (
                                ledger_module.is_issued_actual_projection_cohort(
                                    cohort
                                )
                            )
                            self.assertIs(
                                calendar_type.is_open,
                                original_is_open,
                            )
                            self.assertEqual(
                                cohort_verify_calls["target_calls"],
                                4,
                            )
                        self.assertEqual(issue_calls["target_calls"], 10)
                        self.assertEqual(verify_calls["target_calls"], 2)
                        self.assertEqual(install_calls["target_calls"], 4)
                finally:
                    calendar_type.is_open = original_is_open

                original_session_type = market_calendar_module.MarketSession
                constructor_calls = {"target_calls": 0}

                def forged_session_constructor(*args: object, **kwargs: object):
                    constructor_calls["target_calls"] += 1
                    forged = replace(
                        original_session_type(*args, **kwargs),
                        open_time=time(10, 0),
                        close_time=time(11, 0),
                    )
                    if constructor_calls["target_calls"] == 2:
                        market_calendar_module.MarketSession = original_session_type
                    return forged

                nested_global_state = None
                market_calendar_module.MarketSession = forged_session_constructor
                try:
                    try:
                        nested_global_state = replay_actual(
                            source,
                            plans=plans,
                            calendar=calendar,
                            policy=policy,
                        )
                    except (RiskBlock, ValueError):
                        pass
                    if nested_global_state is not None:
                        self.assertIs(
                            market_calendar_module.MarketSession,
                            original_session_type,
                        )
                        self.assertEqual(
                            constructor_calls["target_calls"],
                            2,
                        )
                finally:
                    market_calendar_module.MarketSession = original_session_type

                forged_cash = (
                    None if state is None else state.strategy_settled_cash_micros
                )
                self.assertEqual(
                    (
                        state is not None,
                        state_verified,
                        cohort is not None,
                        cohort_verified,
                        forged_cash,
                        nested_global_state is not None,
                    ),
                    (False, False, False, False, None, False),
                )

    def test_replaced_actual_replay_helper_cannot_issue_forged_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, genuine, _cohort = self._actual_projection(journal)
                genuine_position = genuine.positions[0]
                genuine_lot = genuine_position.lots[0]
                forged_lot = replace(
                    genuine_lot,
                    received_at=genuine_lot.received_at + timedelta(seconds=7),
                )
                forged_position = replace(
                    genuine_position,
                    lots=(forged_lot,),
                )
                forged = replace(
                    genuine,
                    positions=(forged_position, *genuine.positions[1:]),
                )
                replay_builder = reconciliation_module._replay_actual_source
                reconciliation_module._replay_actual_source = (
                    lambda *_args, **_kwargs: forged
                )
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                returned = None
                try:
                    returned = replay_actual(
                        source,
                        plans=UnavailableSignalPlanResolver(),
                        calendar=calendar,
                        policy=policy_fixture(),
                    )
                except (RiskBlock, ValueError):
                    pass
                finally:
                    reconciliation_module._replay_actual_source = replay_builder

                self.assertIsNot(returned, forged)
                self.assertFalse(
                    reconciliation_module.is_verified_actual_ledger_state_for_source(
                        forged,
                        source,
                    )
                )

    def test_unvalidated_policy_cannot_issue_actual_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, _state, _cohort = self._actual_projection(journal)
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                unapproved = replace(
                    policy_fixture(),
                    capital=Decimal("6000"),
                )

                with self.assertRaises(ValueError):
                    replay_actual(
                        source,
                        plans=UnavailableSignalPlanResolver(),
                        calendar=calendar,
                        policy=unapproved,
                    )

    def test_ingest_ignores_replaced_actual_replay_helper_before_durable_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                replay_builder = reconciliation_module._replay_actual_source
                observed = {"calls": 0}

                def forged_builder(*args: object, **kwargs: object):
                    observed["calls"] += 1
                    genuine = replay_builder(*args, **kwargs)
                    return replace(
                        genuine,
                        strategy_settled_cash_micros=6_000_000_000,
                    )

                reconciliation_module._replay_actual_source = forged_builder
                try:
                    result = ingest_confirmation(
                        journal,
                        _confirmation(
                            "message:guarded-ingestion-replay",
                            "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                            "unlogged_positions 0 AT 10:15 ET",
                            message_time="2026-08-14T10:15:00-04:00",
                            received_at="2026-08-14T10:15:01-04:00",
                        ),
                        plans=UnavailableSignalPlanResolver(),
                        calendar=SessionCalendarResolver(
                            (
                                load_current_market_calendar(
                                    ROOT,
                                    as_of=_SESSION,
                                ),
                            )
                        ),
                        policy=policy_fixture(),
                        entry_authorities=(
                            UnavailableActualEntryAuthorityResolver()
                        ),
                    )
                finally:
                    reconciliation_module._replay_actual_source = replay_builder

                self.assertEqual(observed["calls"], 0)
                self.assertFalse(result.duplicate)
                self.assertEqual(journal.count("raw_messages"), 1)

    def test_ingest_rejects_unvalidated_policy_before_durable_write(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                with self.assertRaises(ValueError):
                    ingest_confirmation(
                        journal,
                        _confirmation(
                            "message:unvalidated-ingestion-policy",
                            "ACCOUNT CHECK settled_cash 6000 pending_orders 0 "
                            "unlogged_positions 0 AT 10:15 ET",
                            message_time="2026-08-14T10:15:00-04:00",
                            received_at="2026-08-14T10:15:01-04:00",
                        ),
                        plans=UnavailableSignalPlanResolver(),
                        calendar=SessionCalendarResolver(
                            (
                                load_current_market_calendar(
                                    ROOT,
                                    as_of=_SESSION,
                                ),
                            )
                        ),
                        policy=replace(
                            policy_fixture(),
                            capital=Decimal("6000"),
                        ),
                        entry_authorities=(
                            UnavailableActualEntryAuthorityResolver()
                        ),
                    )

                self.assertEqual(journal.count("raw_messages"), 0)
                self.assertEqual(journal.count("actual_cash_projection"), 0)

    def test_raw_incremental_checkpoint_cannot_forge_durable_cash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                plans = UnavailableSignalPlanResolver()
                policy = policy_fixture()
                entry_authorities = UnavailableActualEntryAuthorityResolver()

                def ingest(item: ConfirmationEnvelope) -> None:
                    ingest_confirmation(
                        journal,
                        item,
                        plans=plans,
                        calendar=calendar,
                        policy=policy,
                        entry_authorities=entry_authorities,
                    )

                ingest(
                    _confirmation(
                        "message:checkpoint-forgery-buy",
                        "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                        message_time="2026-08-14T10:15:00-04:00",
                        received_at="2026-08-14T10:15:01-04:00",
                    )
                )
                ingest(
                    _confirmation(
                        "message:checkpoint-forgery-check",
                        "ACCOUNT CHECK settled_cash 4800 pending_orders 0 "
                        "unlogged_positions 0 AT 10:15 ET",
                        message_time="2026-08-14T10:15:30-04:00",
                        received_at="2026-08-14T10:15:31-04:00",
                    )
                )
                genuine = reconciliation_module._INGESTION_CHECKPOINTS[journal][0]
                forged_state = replace(
                    genuine.state,
                    strategy_settled_cash_micros=9_000_000_000,
                )
                forged = replace(genuine, state=forged_state)

                try:
                    reconciliation_module._store_incremental_ingestion_checkpoint(
                        journal,
                        forged,
                    )
                except (RiskBlock, ValueError):
                    pass
                retained = reconciliation_module._INGESTION_CHECKPOINTS[journal][0]

                ingest(
                    _confirmation(
                        "message:checkpoint-forgery-fee",
                        "FEE SPY 0.03 AT 10:16 ET",
                        message_time="2026-08-14T10:16:30-04:00",
                        received_at="2026-08-14T10:16:31-04:00",
                    )
                )
                row = journal._connection.execute(
                    "SELECT estimated_settled_cash_micros "
                    "FROM actual_cash_projection WHERE id = 1"
                ).fetchone()

                self.assertIsNot(retained, forged)
                self.assertEqual(row, (4_799_970_000,))

    def test_replaced_checkpoint_loader_cannot_inject_durable_cash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                plans = UnavailableSignalPlanResolver()
                policy = policy_fixture()
                entry_authorities = UnavailableActualEntryAuthorityResolver()

                def ingest(item: ConfirmationEnvelope) -> None:
                    ingest_confirmation(
                        journal,
                        item,
                        plans=plans,
                        calendar=calendar,
                        policy=policy,
                        entry_authorities=entry_authorities,
                    )

                ingest(
                    _confirmation(
                        "message:checkpoint-loader-buy",
                        "BOUGHT SPY 2 shares @ 100 AT 10:14 ET",
                        message_time="2026-08-14T10:15:00-04:00",
                        received_at="2026-08-14T10:15:01-04:00",
                    )
                )
                ingest(
                    _confirmation(
                        "message:checkpoint-loader-check",
                        "ACCOUNT CHECK settled_cash 4800 pending_orders 0 "
                        "unlogged_positions 0 AT 10:15 ET",
                        message_time="2026-08-14T10:15:30-04:00",
                        received_at="2026-08-14T10:15:31-04:00",
                    )
                )
                genuine = reconciliation_module._INGESTION_CHECKPOINTS[journal][0]
                forged = replace(
                    genuine,
                    state=replace(
                        genuine.state,
                        strategy_settled_cash_micros=9_000_000_000,
                    ),
                )
                original_loader = (
                    reconciliation_module._load_incremental_ingestion_checkpoint
                )
                observed = {"calls": 0}

                def forged_loader(*_args: object, **_kwargs: object):
                    observed["calls"] += 1
                    return forged

                reconciliation_module._load_incremental_ingestion_checkpoint = (
                    forged_loader
                )
                try:
                    ingest(
                        _confirmation(
                            "message:checkpoint-loader-fee",
                            "FEE SPY 0.03 AT 10:16 ET",
                            message_time="2026-08-14T10:16:30-04:00",
                            received_at="2026-08-14T10:16:31-04:00",
                        )
                    )
                finally:
                    reconciliation_module._load_incremental_ingestion_checkpoint = (
                        original_loader
                    )
                row = journal._connection.execute(
                    "SELECT estimated_settled_cash_micros "
                    "FROM actual_cash_projection WHERE id = 1"
                ).fetchone()

                self.assertEqual(observed["calls"], 0)
                self.assertEqual(row, (4_799_970_000,))

    def test_checkpoint_rejects_rolled_back_connection_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                calendar = SessionCalendarResolver(
                    (load_current_market_calendar(ROOT, as_of=_SESSION),)
                )
                policy = policy_fixture()
                ingest_confirmation(
                    journal,
                    _confirmation(
                        "message:checkpoint-rollback-seed",
                        "SKIPPED SPY",
                        message_time="2026-08-14T10:15:00-04:00",
                        received_at="2026-08-14T10:15:01-04:00",
                    ),
                    plans=UnavailableSignalPlanResolver(),
                    calendar=calendar,
                    policy=policy,
                    entry_authorities=UnavailableActualEntryAuthorityResolver(),
                )
                checkpoint = reconciliation_module._INGESTION_CHECKPOINTS[
                    journal
                ][0]

                with self.assertRaisesRegex(RuntimeError, "rollback"):
                    with journal.transaction() as transaction:
                        transaction.append_raw_message(
                            "message:checkpoint-rollback-orphan",
                            datetime.fromisoformat(
                                "2026-08-14T10:15:30-04:00"
                            ),
                            "SKIPPED QQQ",
                        )
                        raise RuntimeError("rollback")

                with journal.transaction() as transaction:
                    identity = transaction._incremental_ingestion_identity()
                    loaded = reconciliation_module._load_incremental_ingestion_checkpoint(
                        journal,
                        identity=identity,
                        query_cutoff=datetime.fromisoformat(
                            "2026-08-14T10:16:00-04:00"
                        ),
                        calendar=calendar,
                        policy=policy,
                    )

                self.assertIs(
                    reconciliation_module._INGESTION_CHECKPOINTS[journal][0],
                    checkpoint,
                )
                self.assertIsNone(loaded)

    def test_replacing_batch_issuer_global_cannot_expand_allowlist(self) -> None:
        raw = ledger_module.VerifiedLedgerEventBatch(
            (("forged:mutable-global", "c" * 64),)
        )
        original = ledger_module._issue_live_verified_ledger_event_batch

        def attacker() -> None:
            ledger_module._register_verified_batch(raw)

        ledger_module._issue_live_verified_ledger_event_batch = attacker
        try:
            try:
                attacker()
            except RiskBlock:
                pass
        finally:
            ledger_module._issue_live_verified_ledger_event_batch = original

        self.assertFalse(ledger_module._is_issued_verified_batch(raw))

    def test_replacing_derived_issuer_global_cannot_expand_allowlist(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                copied = replace(genuine)
                bindings = ledger_module._phase1_bound_sources(genuine)
                original = ledger_module._issue_ledger_signal_from_phase1_source

                def attacker() -> None:
                    ledger_module._register_phase1_derived_authority(
                        ledger_module._ISSUED_LEDGER_SIGNALS,
                        copied,
                        sources=bindings,
                    )

                ledger_module._issue_ledger_signal_from_phase1_source = attacker
                try:
                    try:
                        attacker()
                    except RiskBlock:
                        pass
                finally:
                    ledger_module._issue_ledger_signal_from_phase1_source = (
                        original
                    )

                self.assertFalse(ledger_module.is_issued_ledger_signal(copied))

    def test_replacing_replay_issuer_global_cannot_expand_binder_allowlist(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                genuine_event = replay.ledger_pair.events[0]
                copied_event = replace(genuine_event)
                bindings = ledger_module._phase1_bound_sources(genuine_event)
                original = (
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source
                )

                def attacker() -> None:
                    ledger_module._bind_phase1_sources(copied_event, bindings)

                ledger_module._issue_canonical_ledger_replay_from_phase1_source = (
                    attacker
                )
                try:
                    try:
                        attacker()
                    except RiskBlock:
                        pass
                finally:
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source = (
                        original
                    )

                self.assertEqual(
                    ledger_module._phase1_bound_sources(copied_event),
                    (),
                )

    def test_borrowed_batch_issuer_code_with_attacker_globals_cannot_mint(
        self,
    ) -> None:
        raw = ledger_module.VerifiedLedgerEventBatch(
            (("forged:borrowed-code", "d" * 64),)
        )
        at = datetime.fromisoformat("2026-08-14T10:15:00-04:00")
        event = SimpleNamespace(
            ledger_name="CANONICAL",
            event_id="forged:borrowed-code",
            signal_id="signal:borrowed-code",
            signal_digest="e" * 64,
            message_time=at,
            received_at=at,
            lot=SimpleNamespace(at=at),
            authority_basis="f" * 64,
        )
        authority = SimpleNamespace(
            canonical_event_id=event.event_id,
            signal_id=event.signal_id,
            signal_digest=event.signal_digest,
            quote_at=at,
            source_digest=event.authority_basis,
        )
        attacker_globals = dict(ledger_module.__dict__)
        attacker_globals.update(
            LedgerEvent=object,
            is_issued_paper_entry_authority=lambda _value: True,
            VerifiedLedgerEventBatch=lambda _references: raw,
            _ledger_event_content_digest=lambda _event: "d" * 64,
            _register_verified_batch=ledger_module._register_verified_batch,
        )
        borrowed_issuer = FunctionType(
            ledger_module._issue_paper_verified_ledger_event_batch.__code__,
            attacker_globals,
        )

        try:
            borrowed_issuer(event, authority)
        except RiskBlock:
            pass

        self.assertFalse(ledger_module._is_issued_verified_batch(raw))

    def test_borrowed_derived_issuer_code_with_attacker_globals_cannot_mint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                forged = replace(genuine, symbol="QQQ")
                attacker_globals = dict(ledger_module.__dict__)
                attacker_globals.update(
                    _construct_phase1_signal=(
                        lambda *_args, **_kwargs: forged
                    ),
                    _register_phase1_derived_authority=(
                        ledger_module._register_phase1_derived_authority
                    ),
                )
                borrowed_issuer = FunctionType(
                    ledger_module._issue_ledger_signal_from_phase1_source.__code__,
                    attacker_globals,
                )

                try:
                    borrowed_issuer(source)
                except RiskBlock:
                    pass

                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_borrowed_replay_issuer_code_with_attacker_globals_cannot_bind(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                genuine_authority, completed_at = (
                    _seed_completed_authority_fill(journal)
                )
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(replay)[0][0]
                genuine_signal = replay.ledger_pair.signals[0]
                copied_event = replace(replay.ledger_pair.events[0])
                attacker_globals = dict(ledger_module.__dict__)
                attacker_globals.update(
                    _construct_phase1_signal=(
                        lambda *_args, **_kwargs: genuine_signal
                    ),
                    _register_phase1_derived_authority=(
                        lambda *_args, **_kwargs: None
                    ),
                    _phase1_open_position_state=(
                        lambda *_args, **_kwargs: (
                            genuine_signal.recommended_stop,
                            False,
                        )
                    ),
                    _paper_entry_from_source_material=(
                        lambda *_args, **_kwargs: genuine_authority
                    ),
                    _canonical_event_from_paper_authority=(
                        lambda *_args, **_kwargs: copied_event
                    ),
                    _bind_phase1_sources=ledger_module._bind_phase1_sources,
                )
                borrowed_issuer = FunctionType(
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source.__code__,
                    attacker_globals,
                )

                try:
                    borrowed_issuer(source)
                except Exception:
                    pass

                self.assertFalse(
                    ledger_module._phase1_bound_sources(copied_event)
                )

    def test_publication_classmethod_cannot_grant_attacker_factory_signal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                _source, decision, plan, _lineage = _issued_publication(journal)
                forged = replace(
                    _signal(),
                    signal_id="forged:publication-classmethod",
                    symbol="QQQ",
                )

                def attacker_factory(**_ignored: object):
                    return forged

                try:
                    returned = (
                        ledger_module.LedgerSignal.from_publication_decision.__func__(
                            attacker_factory,
                            decision,
                            rank=1,
                            plan_decision=plan,
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_borrowed_actual_projection_code_cannot_install_forged_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, state, genuine = self._actual_projection(journal)
                forged = replace(
                    genuine,
                    journal_source_digest="f" * 64,
                )
                attacker_globals = dict(ledger_module.__dict__)
                attacker_globals["ActualProjectionCohort"] = (
                    lambda **_ignored: forged
                )
                borrowed_issuer = FunctionType(
                    ledger_module._issue_actual_projection_from_journal.__code__,
                    attacker_globals,
                )

                try:
                    returned = borrowed_issuer(source, state)
                except RiskBlock:
                    returned = None

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(forged)
                )

    def test_publication_classmethod_records_exact_decision_plan_children(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                _source, decision, plan, _lineage = _issued_publication(journal)

                signal = ledger_module.LedgerSignal.from_publication_decision(
                    decision,
                    rank=1,
                    plan_decision=plan,
                )
                candidate = ledger_module._phase1_derived_authority_candidate(
                    ledger_module._ISSUED_LEDGER_SIGNALS,
                    signal,
                )

                self.assertTrue(ledger_module.is_issued_ledger_signal(signal))
                self.assertIsNotNone(candidate)
                assert candidate is not None
                self.assertEqual(len(candidate[1].children), 2)
                self.assertIs(candidate[1].children[0], decision)
                self.assertIs(candidate[1].children[1], plan)

    def test_borrowed_publication_classmethod_code_cannot_install_forged_signal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                _source, decision, plan, _lineage = _issued_publication(journal)
                forged = replace(
                    _signal(),
                    signal_id="forged:borrowed-publication-code",
                    symbol="QQQ",
                )

                def attacker_factory(**_ignored: object):
                    return forged

                attacker_globals = dict(ledger_module.__dict__)
                attacker_globals["LedgerSignal"] = attacker_factory
                borrowed_issuer = FunctionType(
                    ledger_module.LedgerSignal.from_publication_decision.__func__.__code__,
                    attacker_globals,
                )

                try:
                    returned = borrowed_issuer(
                        attacker_factory,
                        decision,
                        rank=1,
                        plan_decision=plan,
                    )
                except RiskBlock:
                    returned = None

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_replacing_ledger_signal_global_cannot_expand_publication_issuer(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                _source, decision, plan, _lineage = _issued_publication(journal)
                signal_type = ledger_module.LedgerSignal
                issuer = signal_type.from_publication_decision.__func__
                forged = replace(
                    _signal(),
                    signal_id="forged:replaced-ledger-signal-global",
                    symbol="QQQ",
                )

                def attacker_factory(**_ignored: object):
                    return forged

                ledger_module.LedgerSignal = attacker_factory
                try:
                    returned = issuer(
                        attacker_factory,
                        decision,
                        rank=1,
                        plan_decision=plan,
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.LedgerSignal = signal_type

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_borrowed_canonical_replay_code_cannot_install_forged_replay(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                genuine_authority, completed_at = (
                    _seed_completed_authority_fill(journal)
                )
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                genuine_signal = genuine.ledger_pair.signals[0]
                genuine_event = genuine.ledger_pair.events[0]
                forged = replace(genuine, source_digest="f" * 64)
                attacker_globals = dict(ledger_module.__dict__)
                attacker_globals.update(
                    _construct_phase1_signal=(
                        lambda *_args, **_kwargs: genuine_signal
                    ),
                    _register_phase1_derived_authority=(
                        lambda *_args, **_kwargs: None
                    ),
                    _phase1_open_position_state=(
                        lambda *_args, **_kwargs: (
                            genuine_signal.recommended_stop,
                            False,
                        )
                    ),
                    _paper_entry_from_source_material=(
                        lambda *_args, **_kwargs: genuine_authority
                    ),
                    _canonical_event_from_paper_authority=(
                        lambda *_args, **_kwargs: genuine_event
                    ),
                    _bind_phase1_sources=lambda *_args, **_kwargs: None,
                    _register_verified_batch=lambda *_args, **_kwargs: None,
                    LedgerPair=lambda **_kwargs: genuine.ledger_pair,
                    Phase1CanonicalLedgerReplay=lambda **_kwargs: forged,
                )
                borrowed_issuer = FunctionType(
                    ledger_module._issue_canonical_ledger_replay_from_phase1_source.__code__,
                    attacker_globals,
                )

                try:
                    returned = borrowed_issuer(source)
                except RiskBlock:
                    returned = None

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        forged,
                        source,
                    )
                )

    def test_replaced_signal_converter_cannot_mint_forged_source_signal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                forged = replace(genuine, planned_shares=147)
                original = ledger_module._construct_phase1_signal

                def fake_converter(*_args: object, **_kwargs: object):
                    ledger_module._construct_phase1_signal = original
                    return forged

                ledger_module._construct_phase1_signal = fake_converter
                try:
                    returned = ledger_module._issue_ledger_signal_from_phase1_source(
                        source
                    )
                except RiskBlock:
                    returned = None
                finally:
                    ledger_module._construct_phase1_signal = original

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_malicious_nested_signal_equality_cannot_mint_authority(
        self,
    ) -> None:
        class AlwaysEqual(str):
            def __eq__(self, _other: object) -> bool:
                return True

            def __ne__(self, _other: object) -> bool:
                return False

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                genuine = journal.read_phase1_signal(
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                forged = replace(genuine)
                object.__setattr__(forged, "symbol", AlwaysEqual("QQQ"))
                original = ledger_module._construct_phase1_signal

                def fake_converter(*_args: object, **_kwargs: object):
                    ledger_module._construct_phase1_signal = original
                    return forged

                ledger_module._construct_phase1_signal = fake_converter
                try:
                    returned = ledger_module._issue_ledger_signal_from_phase1_source(
                        source
                    )
                except RiskBlock:
                    returned = None
                finally:
                    ledger_module._construct_phase1_signal = original

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module.is_issued_ledger_signal(forged))

    def test_custom_tzinfo_is_rejected_before_event_digest_callback(
        self,
    ) -> None:
        observed = {"callbacks": 0}

        class HookTZ(tzinfo):
            def utcoffset(self, _value: datetime | None) -> timedelta:
                observed["callbacks"] += 1
                return timedelta(0)

            def dst(self, _value: datetime | None) -> timedelta:
                observed["callbacks"] += 1
                return timedelta(0)

            def tzname(self, _value: datetime | None) -> str:
                observed["callbacks"] += 1
                return "HOOK"

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                event = replay.ledger_pair.events[0]
                hook_time = datetime(2026, 8, 14, 14, 0, tzinfo=HookTZ())
                forged_lot = object.__new__(ledger_module.LedgerLot)
                object.__setattr__(forged_lot, "price", event.lot.price)
                object.__setattr__(forged_lot, "shares", event.lot.shares)
                object.__setattr__(forged_lot, "at", hook_time)
                object.__setattr__(
                    forged_lot,
                    "parent_order_id",
                    event.lot.parent_order_id,
                )
                object.__setattr__(
                    forged_lot,
                    "total_cost_micros",
                    event.lot.total_cost_micros,
                )
                forged_event = replace(event)
                object.__setattr__(forged_event, "lot", forged_lot)

                with self.assertRaises(TypeError):
                    ledger_module._ledger_event_content_digest(forged_event)

                self.assertEqual(observed["callbacks"], 0)

    def test_replaced_paper_converter_cannot_mint_forged_authority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                genuine, _completed_at = _seed_completed_authority_fill(journal)
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                signal = journal.read_phase1_signal(
                    genuine.signal_id,
                    query_cutoff=source.query_cutoff,
                )
                forged = replace(genuine, source_digest="f" * 64)
                original = ledger_module._paper_entry_from_source_material

                def fake_converter(*_args: object, **_kwargs: object):
                    ledger_module._paper_entry_from_source_material = original
                    return forged

                ledger_module._paper_entry_from_source_material = fake_converter
                try:
                    returned = (
                        ledger_module._issue_paper_entry_authority_from_phase1_source(
                            source,
                            signal=signal,
                            calendar_resolver=_calendar(),
                        )
                    )
                except RiskBlock:
                    returned = None
                finally:
                    ledger_module._paper_entry_from_source_material = original

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_issued_paper_entry_authority(forged)
                )

    def test_self_restoring_shadow_constructor_cannot_mint_forged_authority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal, candidates_override=_issued_candidates(2))
                replay_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                shadow_source = replay_source.signal_sources[1]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(journal, shadow_source)
                )
                genuine = journal.record_phase1_entry(
                    shadow_source.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                forged = replace(genuine, source_digest="f" * 64)
                authority_type = ledger_module.ShadowFillDispositionAuthority

                def fake_constructor(**_kwargs: object):
                    ledger_module.ShadowFillDispositionAuthority = authority_type
                    return forged

                ledger_module.ShadowFillDispositionAuthority = fake_constructor
                try:
                    returned = (
                        ledger_module._issue_shadow_fill_disposition_from_phase1_source(
                            source,
                            calendar_resolver=_calendar(),
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.ShadowFillDispositionAuthority = authority_type

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_issued_shadow_fill_disposition_authority(
                        forged
                    )
                )

    def test_self_restoring_batch_constructor_cannot_mint_forged_batch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                event = replay.ledger_pair.events[0]
                forged = ledger_module.VerifiedLedgerEventBatch(
                    (("forged:self-restoring-batch", "d" * 64),)
                )
                batch_type = ledger_module.VerifiedLedgerEventBatch

                def fake_constructor(_references: object):
                    ledger_module.VerifiedLedgerEventBatch = batch_type
                    return forged

                ledger_module.VerifiedLedgerEventBatch = fake_constructor
                try:
                    returned = ledger_module._issue_paper_verified_ledger_event_batch(
                        event,
                        authority,
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.VerifiedLedgerEventBatch = batch_type

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(ledger_module._is_issued_verified_batch(forged))

    def test_self_restoring_actual_constructor_cannot_mint_forged_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, state, genuine = self._actual_projection(journal)
                forged = replace(genuine, journal_source_digest="f" * 64)
                cohort_type = ledger_module.ActualProjectionCohort

                def fake_constructor(**_kwargs: object):
                    ledger_module.ActualProjectionCohort = cohort_type
                    return forged

                ledger_module.ActualProjectionCohort = fake_constructor
                try:
                    returned = ledger_module._issue_actual_projection_from_journal(
                        source,
                        state,
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.ActualProjectionCohort = cohort_type

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(forged)
                )

    def test_expected_source_equality_hook_cannot_mint_actual_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, state, genuine = self._actual_projection(journal)
                original_digest = source.source_digest
                forged = replace(genuine, journal_source_digest="f" * 64)
                cohort_type = ledger_module.ActualProjectionCohort
                observed = {"hook_calls": 0}

                class RestoreEqual(str):
                    def __ne__(self, _other: object) -> bool:
                        observed["hook_calls"] += 1
                        object.__setattr__(
                            source,
                            "source_digest",
                            original_digest,
                        )
                        return False

                def fake_constructor(**_kwargs: object):
                    object.__setattr__(
                        source,
                        "source_digest",
                        RestoreEqual(original_digest),
                    )
                    ledger_module.ActualProjectionCohort = cohort_type
                    return forged

                ledger_module.ActualProjectionCohort = fake_constructor
                try:
                    returned = ledger_module._issue_actual_projection_from_journal(
                        source,
                        state,
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.ActualProjectionCohort = cohort_type
                    object.__setattr__(source, "source_digest", original_digest)

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertEqual(observed["hook_calls"], 0)
                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(forged)
                )

    def test_exact_source_mutation_cannot_mint_then_revive_actual_cohort(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                source, state, genuine = self._actual_projection(journal)
                original_digest = source.source_digest
                forged_digest = "f" * 64
                forged = replace(
                    genuine,
                    journal_source_digest=forged_digest,
                )
                cohort_type = ledger_module.ActualProjectionCohort

                def fake_constructor(**_kwargs: object):
                    object.__setattr__(
                        source,
                        "source_digest",
                        forged_digest,
                    )
                    ledger_module.ActualProjectionCohort = cohort_type
                    return forged

                ledger_module.ActualProjectionCohort = fake_constructor
                try:
                    returned = ledger_module._issue_actual_projection_from_journal(
                        source,
                        state,
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.ActualProjectionCohort = cohort_type
                    object.__setattr__(source, "source_digest", original_digest)

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_issued_actual_projection_cohort(forged)
                )

    def test_self_restoring_replay_constructor_cannot_mint_forged_root(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                forged = replace(genuine, canonical_cash=Decimal("1"))
                replay_type = ledger_module.Phase1CanonicalLedgerReplay

                def fake_constructor(**_kwargs: object):
                    ledger_module.Phase1CanonicalLedgerReplay = replay_type
                    return forged

                ledger_module.Phase1CanonicalLedgerReplay = fake_constructor
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.Phase1CanonicalLedgerReplay = replay_type

                if returned is not None:
                    self.assertIs(returned, forged)
                self.assertFalse(
                    ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        forged,
                        source,
                    )
                )

    def test_self_restoring_ledger_pair_factory_cannot_mint_forged_graph(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                pair_type = ledger_module.LedgerPair
                forged_pair = object.__new__(pair_type)
                for slot in pair_type.__slots__:
                    object.__setattr__(
                        forged_pair,
                        slot,
                        object.__getattribute__(genuine.ledger_pair, slot),
                    )
                forged_signal = replace(
                    genuine.ledger_pair.signals[0],
                    planned_shares=147,
                )
                object.__setattr__(forged_pair, "_signals", (forged_signal,))

                class RestoringPairFactory(type):
                    def __call__(factory, *_args: object, **_kwargs: object):
                        del factory
                        ledger_module.LedgerPair = pair_type
                        return forged_pair

                class ForgedPairFactory(metaclass=RestoringPairFactory):
                    pass

                ledger_module.LedgerPair = ForgedPairFactory
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.LedgerPair = pair_type

                if returned is not None:
                    self.assertIs(returned.ledger_pair, forged_pair)
                self.assertTrue(
                    returned is None
                    or not ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        returned,
                        source,
                    )
                )

    def test_malicious_nested_pair_lot_leaf_cannot_mint_replay(self) -> None:
        class AlwaysEqual(str):
            def __ne__(self, _other: object) -> bool:
                return False

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                pair_type = ledger_module.LedgerPair
                observed: dict[str, object] = {}

                class RestoringPairFactory(type):
                    def __call__(factory, *args: object, **kwargs: object):
                        del factory
                        base_pair = pair_type(*args, **kwargs)
                        genuine_position = base_pair.canonical.open_positions[0]
                        genuine_lot = genuine_position.lots[0]
                        forged_lot = object.__new__(ledger_module.LedgerLot)
                        object.__setattr__(forged_lot, "price", genuine_lot.price)
                        object.__setattr__(forged_lot, "shares", genuine_lot.shares)
                        object.__setattr__(forged_lot, "at", genuine_lot.at)
                        object.__setattr__(
                            forged_lot,
                            "parent_order_id",
                            AlwaysEqual("forged:parent"),
                        )
                        object.__setattr__(
                            forged_lot,
                            "total_cost_micros",
                            genuine_lot.total_cost_micros,
                        )
                        forged_position = replace(
                            genuine_position,
                            lots=(forged_lot,),
                        )
                        forged_canonical = replace(
                            base_pair.canonical,
                            open_positions=(forged_position,),
                        )
                        forged_pair = object.__new__(pair_type)
                        for slot in pair_type.__slots__:
                            object.__setattr__(
                                forged_pair,
                                slot,
                                object.__getattribute__(base_pair, slot),
                            )
                        object.__setattr__(
                            forged_pair,
                            "_canonical",
                            forged_canonical,
                        )
                        observed["forged_pair"] = forged_pair
                        ledger_module.LedgerPair = pair_type
                        return forged_pair

                class ForgedPairFactory(metaclass=RestoringPairFactory):
                    pass

                ledger_module.LedgerPair = ForgedPairFactory
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module.LedgerPair = pair_type

                if returned is not None:
                    self.assertIs(
                        returned.ledger_pair,
                        observed.get("forged_pair"),
                    )
                self.assertTrue(
                    returned is None
                    or not ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        returned,
                        source,
                    )
                )

    def test_wrong_exact_lot_total_cannot_mint_canonical_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                event_builder = ledger_module._canonical_event_from_paper_authority
                observed: dict[str, object] = {}

                def fake_event_builder(*args: object, **kwargs: object):
                    event = event_builder(*args, **kwargs)
                    genuine_lot = event.lot
                    forged_lot = object.__new__(ledger_module.LedgerLot)
                    object.__setattr__(forged_lot, "price", genuine_lot.price)
                    object.__setattr__(forged_lot, "shares", genuine_lot.shares)
                    object.__setattr__(forged_lot, "at", genuine_lot.at)
                    object.__setattr__(
                        forged_lot,
                        "parent_order_id",
                        genuine_lot.parent_order_id,
                    )
                    object.__setattr__(
                        forged_lot,
                        "total_cost_micros",
                        genuine_lot.total_cost_micros + 1,
                    )
                    forged_event = replace(event, lot=forged_lot)
                    observed["forged_event"] = forged_event
                    ledger_module._canonical_event_from_paper_authority = (
                        event_builder
                    )
                    return forged_event

                ledger_module._canonical_event_from_paper_authority = (
                    fake_event_builder
                )
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module._canonical_event_from_paper_authority = (
                        event_builder
                    )

                if returned is not None:
                    self.assertIs(
                        returned.ledger_pair.events[0],
                        observed.get("forged_event"),
                    )
                self.assertTrue(
                    returned is None
                    or not ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        returned,
                        source,
                    )
                )

    def test_self_restoring_open_position_helper_cannot_mint_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                genuine_signal = genuine.ledger_pair.signals[0]
                forged_stop = (
                    genuine_signal.recommended_stop + genuine_signal.tick_size
                )
                helper = ledger_module._phase1_open_position_state

                def fake_helper(*_args: object, **_kwargs: object):
                    ledger_module._phase1_open_position_state = helper
                    return forged_stop, True

                ledger_module._phase1_open_position_state = fake_helper
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module._phase1_open_position_state = helper

                if returned is not None:
                    self.assertEqual(
                        returned.ledger_pair.events[0].recommended_stop,
                        forged_stop,
                    )
                self.assertTrue(
                    returned is None
                    or not ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        returned,
                        source,
                    )
                )

    def test_mutable_projection_order_key_cannot_define_phase_b_order(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                genuine = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(genuine)[0][0]
                order_key = ledger_module._ledger_projection_order_key
                observed: dict[str, object] = {"calls": 0}

                def fake_order_key(event: object):
                    observed["calls"] = int(observed["calls"]) + 1
                    observed["event"] = event
                    object.__setattr__(event, "cursor", event.cursor + 1)
                    ledger_module._ledger_projection_order_key = order_key
                    return order_key(event)

                ledger_module._ledger_projection_order_key = fake_order_key
                try:
                    returned = (
                        ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                            source
                        )
                    )
                except (RiskBlock, TypeError):
                    returned = None
                finally:
                    ledger_module._ledger_projection_order_key = order_key

                self.assertEqual(observed["calls"], 1)
                self.assertTrue(
                    returned is None
                    or not ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        returned,
                        source,
                    )
                )
                installer_source = inspect.getsource(
                    ledger_module._install_canonical_replay_authority
                )
                self.assertNotIn(
                    "_ledger_projection_order_key",
                    installer_source,
                )
                self.assertIn("canonical_event_order", installer_source)

    def test_paper_batch_rejects_raw_forged_stop_and_profit_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, completed_at = _seed_completed_authority_fill(journal)
                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                signal = replay.ledger_pair.signals[0]
                genuine_event = replay.ledger_pair.events[0]
                forged_event = replace(
                    genuine_event,
                    recommended_stop=(
                        signal.recommended_stop + signal.tick_size
                    ),
                    profit_target_taken=True,
                )

                try:
                    batch = ledger_module._issue_paper_verified_ledger_event_batch(
                        forged_event,
                        authority,
                    )
                except RiskBlock:
                    batch = None

                self.assertTrue(
                    batch is None
                    or not ledger_module._is_issued_verified_batch(batch)
                )

    def test_authorized_paper_fill_uses_only_an_ephemeral_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, _completed_at = _seed_completed_authority_fill(journal)
                candidate = ledger_module._phase1_derived_authority_candidate(
                    ledger_module._PAPER_ENTRY_AUTHORITIES,
                    authority,
                )
                self.assertIsNotNone(candidate)
                assert candidate is not None
                signal = candidate[1].children[0]
                pair = ledger_module.LedgerPair(signals=(signal,))
                before = set(ledger_module._VERIFIED_LEDGER_BATCH_AUTHORITIES)

                pair.record_authorized_canonical_fill(authority)

                self.assertEqual(len(pair.events), 1)
                self.assertEqual(pair.events[0].event_id, authority.canonical_event_id)
                self.assertEqual(
                    set(ledger_module._VERIFIED_LEDGER_BATCH_AUTHORITIES),
                    before,
                )

    def test_canonical_batch_expires_with_exact_replay_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                pair_type = ledger_module.LedgerPair
                observed: dict[str, object] = {}

                def capturing_pair(*args: object, **kwargs: object):
                    observed["batch"] = kwargs.get("verified_event_batch")
                    ledger_module.LedgerPair = pair_type
                    return pair_type(*args, **kwargs)

                ledger_module.LedgerPair = capturing_pair
                try:
                    replay = journal.read_phase1_canonical_replay(
                        query_cutoff=completed_at,
                    )
                finally:
                    ledger_module.LedgerPair = pair_type
                source = ledger_module._phase1_bound_sources(replay)[0][0]
                batch = observed["batch"]
                self.assertFalse(ledger_module._is_issued_verified_batch(batch))

                _append_unrelated_source(
                    journal,
                    suffix="expire-canonical-batch-source",
                )

                self.assertFalse(
                    ledger_module._is_issued_verified_batch(batch)
                )
                self.assertFalse(
                    ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        replay,
                        source,
                    )
                )

    def test_canonical_batch_is_removed_when_pair_construction_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _authority, completed_at = _seed_completed_authority_fill(journal)
                pair_type = ledger_module.LedgerPair
                observed: dict[str, object] = {}

                def failing_pair(*_args: object, **kwargs: object):
                    observed["batch"] = kwargs.get("verified_event_batch")
                    ledger_module.LedgerPair = pair_type
                    raise RiskBlock("SYNTHETIC_PAIR_FAILURE")

                ledger_module.LedgerPair = failing_pair
                try:
                    with self.assertRaisesRegex(
                        RiskBlock,
                        "^SYNTHETIC_PAIR_FAILURE$",
                    ):
                        journal.read_phase1_canonical_replay(
                            query_cutoff=completed_at,
                        )
                finally:
                    ledger_module.LedgerPair = pair_type

                batch = observed["batch"]
                self.assertFalse(ledger_module._is_issued_verified_batch(batch))
                self.assertNotIn(
                    id(batch),
                    ledger_module._VERIFIED_LEDGER_BATCH_AUTHORITIES,
                )

                replay = journal.read_phase1_canonical_replay(
                    query_cutoff=completed_at,
                )
                source = ledger_module._phase1_bound_sources(replay)[0][0]
                self.assertTrue(
                    ledger_module.is_verified_phase1_canonical_ledger_replay_for_source(
                        replay,
                        source,
                    )
                )


if __name__ == "__main__":
    unittest.main()
