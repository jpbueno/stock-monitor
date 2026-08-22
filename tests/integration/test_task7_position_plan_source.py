from __future__ import annotations

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import (
    IdempotencyConflict,
    InvalidJournalValue,
    Journal,
    MigrationCorruption,
    is_verified_actual_position_plan_source,
)
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    replay_actual,
)
from stock_monitor.risk import RiskBlock
from tests.integration.test_phase1_authorities import (
    _append_completed_entry_observations,
    _confirmation_action_source,
)
from tests.integration.test_signal_lifecycle import _calendar, _publish
from tests.support import aware_et, policy_fixture


class ActualPositionPlanSourceTests(unittest.TestCase):
    @staticmethod
    def _actual_snapshot(journal: Journal, cutoff):
        with journal.transaction() as transaction:
            replay_source = transaction.read_actual_replay(
                query_cutoff=cutoff,
            )
        state = replay_actual(
            replay_source,
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
        )
        return replay_source, state

    @staticmethod
    def _ingest(journal: Journal, *, message_id: str, text: str, at):
        return ingest_confirmation(
            journal,
            ConfirmationEnvelope(
                message_id=message_id,
                message_time=at,
                received_at=at + timedelta(seconds=1),
                text=text,
                session_date=at.date(),
            ),
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
            entry_authorities=UnavailableActualEntryAuthorityResolver(),
        )

    def _confirmation_action_with_message_id(
        self,
        journal: Journal,
        signal_source: object,
        *,
        message_id: str,
        event_clock: str,
        at,
    ):
        maximum_entry = Decimal(signal_source.maximum_entry_micros) / Decimal(
            1_000_000
        )
        tick_size = Decimal(signal_source.tick_size_micros) / Decimal(1_000_000)
        recommended_stop = Decimal(
            signal_source.recommended_stop_micros
        ) / Decimal(1_000_000)
        result = self._ingest(
            journal,
            message_id=message_id,
            text=(
                f"BOUGHT {signal_source.symbol} "
                f"{signal_source.planned_shares} shares "
                f"@ {maximum_entry} AT {event_clock} ET; "
                f"BID {maximum_entry - tick_size} ASK {maximum_entry}; "
                f"STOP SET @ {recommended_stop}"
            ),
            at=at,
        )
        with journal.transaction() as transaction:
            action_source = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id,
            )
        return action_source, at + timedelta(seconds=2)

    def _partial_fill_action_source(
        self,
        journal: Journal,
        signal_source: object,
        *,
        message_id: str,
        shares: int,
        parent_order_id: str,
        event_clock: str,
        at,
    ):
        maximum_entry = Decimal(
            signal_source.maximum_entry_micros
        ) / Decimal(1_000_000)
        result = self._ingest(
            journal,
            message_id=message_id,
            text=(
                f"PARTIAL FILL {signal_source.symbol} {shares} shares "
                f"@ {maximum_entry} AT {event_clock} ET; "
                f"ORDER {parent_order_id} TOTAL "
                f"{signal_source.planned_shares} shares"
            ),
            at=at,
        )
        with journal.transaction() as transaction:
            action_source = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id,
            )
        self.assertEqual(action_source.domain_kind, "PARTIAL_FILL")
        return action_source, at + timedelta(seconds=2)

    def _seed_linked_open_position(self, journal: Journal):
        _publish(journal)
        publication_cutoff = aware_et(date(2026, 8, 14), "08:45")
        signal_source = journal._read_phase1_canonical_replay_source(
            query_cutoff=publication_cutoff,
        ).signal_sources[0]
        trigger_id, quote_id, completed_at = (
            _append_completed_entry_observations(journal, signal_source)
        )
        action_source, recorded_at = _confirmation_action_source(
            journal,
            signal_source,
            event_kind="LIVE_CONFIRM",
            after=completed_at,
        )
        journal.record_phase1_entry(
            signal_source.signal_id,
            confirmation_action_source=action_source,
            trigger_observation_id=trigger_id,
            quote_observation_id=quote_id,
            calendar_resolver=_calendar(),
            recorded_at=recorded_at,
        )
        query_cutoff = recorded_at + timedelta(seconds=1)
        replay_source, state = self._actual_snapshot(journal, query_cutoff)
        return replay_source, state, signal_source, query_cutoff

    def test_resolves_one_open_actual_lifecycle_to_one_primary_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                replay_source, state, signal, cutoff = (
                    self._seed_linked_open_position(journal)
                )

                resolution = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol=signal.symbol,
                    query_cutoff=cutoff,
                )

                self.assertEqual(resolution.status, "RESOLVED")
                self.assertIsNotNone(resolution.source)
                source = resolution.source
                assert source is not None
                self.assertIs(source.actual_replay_source, replay_source)
                self.assertIs(source.actual_position_state, state)
                self.assertEqual(
                    source.signal_source.signal_id,
                    signal.signal_id,
                )
                self.assertEqual(source.symbol, signal.symbol)
                self.assertEqual(source.query_cutoff, cutoff)
                self.assertEqual(len(source.matched_actions), 1)
                self.assertEqual(len(source.lifecycle_events), 1)
                self.assertEqual(
                    source.lifecycle_events[0].event_kind,
                    "LIVE_CONFIRM",
                )
                self.assertEqual(
                    source.lifecycle_events[0].to_status,
                    "LIVE_CONFIRMED",
                )
                self.assertEqual(
                    source.signal_source.role,
                    "PRIMARY",
                )
                self.assertEqual(len(source.position_plan_digest), 64)
                self.assertEqual(len(source.source_digest), 64)
                self.assertTrue(source.row_references)

    def test_source_authority_rejects_copies_cross_owner_and_later_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "first.sqlite3") as first:
                replay_source, state, signal, cutoff = (
                    self._seed_linked_open_position(first)
                )
                resolution = first.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol=signal.symbol,
                    query_cutoff=cutoff,
                )
                source = resolution.source
                assert source is not None
                self.assertTrue(
                    is_verified_actual_position_plan_source(source)
                )
                self.assertFalse(
                    is_verified_actual_position_plan_source(replace(source))
                )
                with self.assertRaises(InvalidJournalValue):
                    first.resolve_actual_position_plan_source(
                        actual_replay_source=replace(replay_source),
                        actual_position_state=state,
                        symbol=signal.symbol,
                        query_cutoff=cutoff,
                    )
                with self.assertRaises(InvalidJournalValue):
                    first.resolve_actual_position_plan_source(
                        actual_replay_source=replay_source,
                        actual_position_state=replace(state),
                        symbol=signal.symbol,
                        query_cutoff=cutoff,
                    )
                mutated_resolution = (
                    first.resolve_actual_position_plan_source(
                        actual_replay_source=replay_source,
                        actual_position_state=state,
                        symbol=signal.symbol,
                        query_cutoff=cutoff,
                    )
                )
                mutated_source = mutated_resolution.source
                assert mutated_source is not None
                object.__setattr__(mutated_source, "symbol", "QQQ")
                self.assertFalse(
                    is_verified_actual_position_plan_source(mutated_source)
                )
                self.assertTrue(
                    is_verified_actual_position_plan_source(source)
                )
                with Journal.open(root / "second.sqlite3") as second:
                    with self.assertRaises(InvalidJournalValue):
                        second.resolve_actual_position_plan_source(
                            actual_replay_source=replay_source,
                            actual_position_state=state,
                            symbol=signal.symbol,
                            query_cutoff=cutoff,
                        )

                first.append_source_observation(
                    payload=b'{"kind":"later"}',
                    source_uri="https://example.invalid/later",
                    source_type="TEST",
                    provider="fixture",
                    feed=None,
                    source_time=cutoff + timedelta(seconds=1),
                    retrieved_at=cutoff + timedelta(seconds=2),
                    provider_sequence=None,
                    delay_seconds=None,
                    health_result="OK",
                    details={"version": 1},
                )
                self.assertFalse(
                    is_verified_actual_position_plan_source(source)
                )

    def test_cutoff_mismatch_fails_and_future_plan_link_is_not_used(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                replay_source, state, signal, cutoff = (
                    self._seed_linked_open_position(journal)
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.resolve_actual_position_plan_source(
                        actual_replay_source=replay_source,
                        actual_position_state=state,
                        symbol=signal.symbol,
                        query_cutoff=cutoff + timedelta(microseconds=1),
                    )

            with Journal.open(path.parent / "lookahead.sqlite3") as journal:
                _publish(journal)
                signal_source = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                ).signal_sources[0]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                action_source, recorded_at = _confirmation_action_source(
                    journal,
                    signal_source,
                    event_kind="LIVE_CONFIRM",
                    after=completed_at,
                )
                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=action_source,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=recorded_at,
                )
                before_link = recorded_at - timedelta(microseconds=1)
                early_replay, early_state = self._actual_snapshot(
                    journal,
                    before_link,
                )

                resolution = journal.resolve_actual_position_plan_source(
                    actual_replay_source=early_replay,
                    actual_position_state=early_state,
                    symbol=signal_source.symbol,
                    query_cutoff=before_link,
                )

                self.assertEqual(resolution.status, "UNAVAILABLE")
                self.assertIsNone(resolution.source)

    def test_zero_or_unlinked_open_position_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                empty_cutoff = aware_et(date(2026, 8, 14), "09:35")
                empty_source, empty_state = self._actual_snapshot(
                    journal,
                    empty_cutoff,
                )
                empty = journal.resolve_actual_position_plan_source(
                    actual_replay_source=empty_source,
                    actual_position_state=empty_state,
                    symbol="SPY",
                    query_cutoff=empty_cutoff,
                )
                self.assertEqual(empty.status, "UNAVAILABLE")
                self.assertIsNone(empty.source)

                buy_at = aware_et(date(2026, 8, 14), "10:15")
                self._ingest(
                    journal,
                    message_id="unlinked-buy",
                    text="BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                    at=buy_at,
                )
                cutoff = buy_at + timedelta(seconds=2)
                replay_source, state = self._actual_snapshot(journal, cutoff)
                unlinked = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol="SPY",
                    query_cutoff=cutoff,
                )
                self.assertEqual(unlinked.status, "UNAVAILABLE")
                self.assertIsNone(unlinked.source)

    def test_multiple_open_position_lifecycles_are_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                buy_at = aware_et(date(2026, 8, 14), "10:15")
                self._ingest(
                    journal,
                    message_id="strategy-buy",
                    text="BOUGHT SPY 1 shares @ 100 AT 10:14 ET",
                    at=buy_at,
                )
                unrelated_at = aware_et(date(2026, 8, 14), "11:01")
                self._ingest(
                    journal,
                    message_id="unrelated-buy",
                    text=(
                        "RECONCILE UNRELATED POSITION SPY +1 shares @ 101 "
                        "AT 11:00 ET"
                    ),
                    at=unrelated_at,
                )
                cutoff = unrelated_at + timedelta(seconds=2)
                replay_source, state = self._actual_snapshot(journal, cutoff)

                resolution = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol="SPY",
                    query_cutoff=cutoff,
                )

                self.assertEqual(resolution.status, "AMBIGUOUS")
                self.assertIsNone(resolution.source)

    def test_plan_digest_is_stable_through_partial_close_and_stop_update(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                replay_source, state, signal, cutoff = (
                    self._seed_linked_open_position(journal)
                )
                first = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol=signal.symbol,
                    query_cutoff=cutoff,
                ).source
                assert first is not None

                partial_at = cutoff + timedelta(minutes=1)
                self._ingest(
                    journal,
                    message_id="partial-close",
                    text=f"SOLD {signal.symbol} 1 shares @ 101 AT 15:31 ET",
                    at=partial_at,
                )
                partial_cutoff = partial_at + timedelta(seconds=2)
                partial_replay, partial_state = self._actual_snapshot(
                    journal,
                    partial_cutoff,
                )
                partial = journal.resolve_actual_position_plan_source(
                    actual_replay_source=partial_replay,
                    actual_position_state=partial_state,
                    symbol=signal.symbol,
                    query_cutoff=partial_cutoff,
                ).source
                assert partial is not None
                self.assertEqual(
                    partial.position_plan_digest,
                    first.position_plan_digest,
                )
                self.assertEqual(
                    partial.opening_actual_lifecycle_id,
                    first.opening_actual_lifecycle_id,
                )

                stop_at = partial_at + timedelta(minutes=1)
                self._ingest(
                    journal,
                    message_id="stop-update",
                    text=f"STOP UPDATED {signal.symbol} @ 98 AT 15:32 ET",
                    at=stop_at,
                )
                stop_cutoff = stop_at + timedelta(seconds=2)
                stop_replay, stop_state = self._actual_snapshot(
                    journal,
                    stop_cutoff,
                )
                stopped = journal.resolve_actual_position_plan_source(
                    actual_replay_source=stop_replay,
                    actual_position_state=stop_state,
                    symbol=signal.symbol,
                    query_cutoff=stop_cutoff,
                ).source
                assert stopped is not None
                self.assertEqual(
                    stopped.position_plan_digest,
                    first.position_plan_digest,
                )

    def test_every_partial_fill_requires_an_exact_same_group_plan_binding(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                _publish(journal)
                signal_source = (
                    journal._read_phase1_canonical_replay_source(
                        query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                    ).signal_sources[0]
                )
                self.assertGreater(signal_source.planned_shares, 1)
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                parent_order_id = "robinhood:task7:partial-order"
                first_action, first_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-partial-fill-1",
                        shares=1,
                        parent_order_id=parent_order_id,
                        event_clock="10:14",
                        at=completed_at + timedelta(seconds=1),
                    )
                )
                try:
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=first_action,
                        trigger_observation_id=trigger_id,
                        quote_observation_id=quote_id,
                        calendar_resolver=_calendar(),
                        recorded_at=first_recorded_at,
                    )
                except InvalidJournalValue as error:
                    self.fail(
                        "first PARTIAL_FILL must be a LIVE_CONFIRM anchor: "
                        f"{error}"
                    )
                first_cutoff = first_recorded_at + timedelta(seconds=1)
                first_replay, first_state = self._actual_snapshot(
                    journal,
                    first_cutoff,
                )
                first_source = (
                    journal.resolve_actual_position_plan_source(
                        actual_replay_source=first_replay,
                        actual_position_state=first_state,
                        symbol=signal_source.symbol,
                        query_cutoff=first_cutoff,
                    ).source
                )
                assert first_source is not None
                self.assertEqual(
                    len(getattr(first_source, "plan_bindings", ())),
                    1,
                )

                second_action, second_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-partial-fill-2",
                        shares=signal_source.planned_shares - 1,
                        parent_order_id=parent_order_id,
                        event_clock="10:16",
                        at=first_cutoff + timedelta(seconds=1),
                    )
                )
                missing_replay, missing_state = self._actual_snapshot(
                    journal,
                    second_recorded_at,
                )
                missing = journal.resolve_actual_position_plan_source(
                    actual_replay_source=missing_replay,
                    actual_position_state=missing_state,
                    symbol=signal_source.symbol,
                    query_cutoff=second_recorded_at,
                )
                self.assertEqual(missing.status, "UNAVAILABLE")
                self.assertIsNone(missing.source)

                with self.assertRaises(RiskBlock):
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=replace(second_action),
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=second_recorded_at,
                    )

                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=second_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=second_recorded_at,
                )
                binding_count = journal.count(
                    "actual_position_plan_bindings"
                )
                lifecycle_count = journal.count("phase1_signal_events")
                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=second_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=second_recorded_at,
                )
                self.assertEqual(
                    journal.count("actual_position_plan_bindings"),
                    binding_count,
                )
                self.assertEqual(
                    journal.count("phase1_signal_events"),
                    lifecycle_count,
                )
                with self.assertRaises(IdempotencyConflict):
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=second_action,
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=(
                            second_recorded_at + timedelta(microseconds=1)
                        ),
                    )
                final_cutoff = second_recorded_at + timedelta(seconds=1)
                final_replay, final_state = self._actual_snapshot(
                    journal,
                    final_cutoff,
                )
                final = journal.resolve_actual_position_plan_source(
                    actual_replay_source=final_replay,
                    actual_position_state=final_state,
                    symbol=signal_source.symbol,
                    query_cutoff=final_cutoff,
                )

                self.assertEqual(final.status, "RESOLVED")
                final_source = final.source
                assert final_source is not None
                self.assertEqual(len(final_source.matched_actions), 2)
                self.assertEqual(len(final_source.plan_bindings), 2)
                self.assertEqual(len(final_source.lifecycle_events), 1)
                self.assertEqual(
                    tuple(
                        binding.binding_kind
                        for binding in final_source.plan_bindings
                    ),
                    (
                        "LIVE_CONFIRM_ANCHOR",
                        "PARTIAL_FILL_CONTINUATION",
                    ),
                )
                self.assertEqual(
                    tuple(
                        binding.confirmation_execution_event_id
                        for binding in final_source.plan_bindings
                    ),
                    (
                        first_action.execution_event_id,
                        second_action.execution_event_id,
                    ),
                )
                self.assertEqual(
                    final_source.position_plan_digest,
                    first_source.position_plan_digest,
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    journal._connection.execute(
                        "UPDATE actual_position_plan_bindings "
                        "SET fill_shares = fill_shares WHERE id = 1"
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    journal._connection.execute(
                        "DELETE FROM actual_position_plan_bindings WHERE id = 1"
                    )

                wrong_group_action, wrong_group_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-partial-fill-wrong-group",
                        shares=1,
                        parent_order_id="robinhood:task7:other-order",
                        event_clock="10:18",
                        at=final_cutoff + timedelta(seconds=1),
                    )
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=wrong_group_action,
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=wrong_group_recorded_at,
                    )
                with self.assertRaises(InvalidJournalValue):
                    journal.record_phase1_entry(
                        "wrong-primary-signal",
                        confirmation_action_source=wrong_group_action,
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=wrong_group_recorded_at,
                    )

    def test_later_fill_cannot_be_bound_as_the_lifecycle_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                _publish(journal)
                signal_source = (
                    journal._read_phase1_canonical_replay_source(
                        query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                    ).signal_sources[0]
                )
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                parent_order_id = "robinhood:task7:ordered-fill"
                first_action, _ = self._partial_fill_action_source(
                    journal,
                    signal_source,
                    message_id="task7-ordered-fill-1",
                    shares=1,
                    parent_order_id=parent_order_id,
                    event_clock="10:14",
                    at=completed_at + timedelta(seconds=1),
                )
                later_action, later_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-ordered-fill-2",
                        shares=signal_source.planned_shares - 1,
                        parent_order_id=parent_order_id,
                        event_clock="10:16",
                        at=completed_at + timedelta(seconds=4),
                    )
                )
                self.assertLess(
                    first_action.execution_event_id,
                    later_action.execution_event_id,
                )

                with self.assertRaises(InvalidJournalValue):
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=later_action,
                        trigger_observation_id=trigger_id,
                        quote_observation_id=quote_id,
                        calendar_resolver=_calendar(),
                        recorded_at=later_recorded_at,
                    )

                self.assertEqual(
                    journal.count("actual_position_plan_bindings"),
                    0,
                )
                self.assertEqual(journal.count("phase1_signal_events"), 1)

    def test_partial_fill_binding_cannot_skip_an_earlier_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                _publish(journal)
                signal_source = (
                    journal._read_phase1_canonical_replay_source(
                        query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                    ).signal_sources[0]
                )
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                parent_order_id = "robinhood:task7:cursor-gap"
                first_action, first_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-cursor-gap-1",
                        shares=1,
                        parent_order_id=parent_order_id,
                        event_clock="10:14",
                        at=completed_at + timedelta(seconds=1),
                    )
                )
                self._partial_fill_action_source(
                    journal,
                    signal_source,
                    message_id="task7-cursor-gap-2",
                    shares=1,
                    parent_order_id=parent_order_id,
                    event_clock="10:16",
                    at=completed_at + timedelta(seconds=4),
                )
                later_action, later_recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-cursor-gap-3",
                        shares=signal_source.planned_shares - 2,
                        parent_order_id=parent_order_id,
                        event_clock="10:18",
                        at=completed_at + timedelta(seconds=7),
                    )
                )
                with journal.transaction() as transaction:
                    first_action = transaction.read_action_source(
                        execution_event_id=first_action.execution_event_id,
                    )
                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=first_action,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=first_recorded_at + timedelta(seconds=6),
                )
                with journal.transaction() as transaction:
                    later_action = transaction.read_action_source(
                        execution_event_id=later_action.execution_event_id,
                    )

                with self.assertRaises(InvalidJournalValue):
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=later_action,
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=later_recorded_at + timedelta(seconds=2),
                    )

                self.assertEqual(
                    journal.count("actual_position_plan_bindings"),
                    1,
                )

    def test_paper_entry_accepts_exact_partial_fill_live_metadata_after_reopen(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                _publish(journal)
                signal_source = (
                    journal._read_phase1_canonical_replay_source(
                        query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                    ).signal_sources[0]
                )
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=None,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                partial_action, recorded_at = (
                    self._partial_fill_action_source(
                        journal,
                        signal_source,
                        message_id="task7-paper-partial-live",
                        shares=1,
                        parent_order_id="robinhood:task7:paper-live",
                        event_clock="10:14",
                        at=completed_at + timedelta(seconds=1),
                    )
                )
                try:
                    journal.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=partial_action,
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=recorded_at,
                    )
                except MigrationCorruption as error:
                    self.fail(
                        "paper-first LIVE metadata must accept PARTIAL_FILL: "
                        f"{error}"
                    )
                journal.record_phase1_entry(
                    signal_source.signal_id,
                    confirmation_action_source=partial_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=recorded_at,
                )
                signal_id = signal_source.signal_id
                symbol = signal_source.symbol
                execution_event_id = partial_action.execution_event_id
                self.assertEqual(
                    journal.count("actual_position_plan_bindings"),
                    1,
                )

            with Journal.open(path) as journal:
                with journal.transaction() as transaction:
                    partial_action = transaction.read_action_source(
                        execution_event_id=execution_event_id,
                    )
                journal.record_phase1_entry(
                    signal_id,
                    confirmation_action_source=partial_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=recorded_at,
                )
                cutoff = recorded_at + timedelta(seconds=1)
                replay_source, state = self._actual_snapshot(journal, cutoff)
                resolution = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=state,
                    symbol=symbol,
                    query_cutoff=cutoff,
                )

                self.assertEqual(resolution.status, "RESOLVED")
                source = resolution.source
                assert source is not None
                self.assertEqual(source.matched_actions, (partial_action,))
                self.assertEqual(
                    source.plan_bindings[0].binding_kind,
                    "LIVE_CONFIRM_ANCHOR",
                )


    def test_same_primary_plan_reopen_has_a_new_position_plan_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                replay, state, signal, cutoff = self._seed_linked_open_position(
                    journal
                )
                first_source = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay,
                    actual_position_state=state,
                    symbol=signal.symbol,
                    query_cutoff=cutoff,
                ).source
                assert first_source is not None
                first_action = first_source.matched_actions[0]
                first_lifecycle_id = getattr(
                    first_source.plan_bindings[0],
                    "position_lifecycle_id",
                    None,
                )
                self.assertIsNotNone(first_lifecycle_id)

                close_at = cutoff + timedelta(minutes=1)
                self._ingest(
                    journal,
                    message_id="linked-lifecycle-close",
                    text=(
                        f"SOLD {signal.symbol} {signal.planned_shares} shares "
                        "@ 21 AT 15:31 ET"
                    ),
                    at=close_at,
                )
                closed_cutoff = close_at + timedelta(seconds=2)
                closed_replay, closed_state = self._actual_snapshot(
                    journal,
                    closed_cutoff,
                )
                closed = journal.resolve_actual_position_plan_source(
                    actual_replay_source=closed_replay,
                    actual_position_state=closed_state,
                    symbol=signal.symbol,
                    query_cutoff=closed_cutoff,
                )
                self.assertEqual(closed.status, "UNAVAILABLE")

                reopen_at = closed_cutoff + timedelta(minutes=1)
                reopen_action, recorded_at = (
                    self._confirmation_action_with_message_id(
                        journal,
                        signal,
                        message_id="linked-reopen-buy",
                        event_clock="15:34",
                        at=reopen_at,
                    )
                )
                self.assertNotEqual(
                    reopen_action.signal_id,
                    first_action.signal_id,
                )
                with self.assertRaises(RiskBlock):
                    journal.record_phase1_entry(
                        signal.signal_id,
                        confirmation_action_source=replace(reopen_action),
                        trigger_observation_id=None,
                        quote_observation_id=None,
                        calendar_resolver=_calendar(),
                        recorded_at=recorded_at,
                    )
                journal.record_phase1_entry(
                    signal.signal_id,
                    confirmation_action_source=reopen_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=recorded_at,
                )
                journal.record_phase1_entry(
                    signal.signal_id,
                    confirmation_action_source=reopen_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=recorded_at,
                )
                with journal.transaction() as transaction:
                    closed_chain_action = transaction.read_action_source(
                        execution_event_id=(
                            first_action.execution_event_id
                        ),
                    )
                journal.record_phase1_entry(
                    signal.signal_id,
                    confirmation_action_source=closed_chain_action,
                    trigger_observation_id=(
                        first_source.lifecycle_events[0].trigger_observation_id
                    ),
                    quote_observation_id=(
                        first_source.lifecycle_events[0].quote_observation_id
                    ),
                    calendar_resolver=_calendar(),
                    recorded_at=first_source.plan_bindings[0].recorded_at,
                )
                self.assertEqual(
                    journal.count("actual_position_plan_bindings"),
                    2,
                )
                self.assertFalse(
                    is_verified_actual_position_plan_source(first_source)
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.resolve_actual_position_plan_source(
                        actual_replay_source=replay,
                        actual_position_state=state,
                        symbol=signal.symbol,
                        query_cutoff=cutoff,
                    )
                reopened_cutoff = recorded_at + timedelta(seconds=1)
                reopened_replay, reopened_state = self._actual_snapshot(
                    journal,
                    reopened_cutoff,
                )
                reopened_source = (
                    journal.resolve_actual_position_plan_source(
                        actual_replay_source=reopened_replay,
                        actual_position_state=reopened_state,
                        symbol=signal.symbol,
                        query_cutoff=reopened_cutoff,
                    ).source
                )
                assert reopened_source is not None

                self.assertEqual(
                    reopened_source.signal_source.primary_plan_digest,
                    first_source.signal_source.primary_plan_digest,
                )
                self.assertEqual(
                    reopened_source.matched_actions,
                    (reopen_action,),
                )
                self.assertEqual(
                    len(reopened_source.plan_bindings),
                    1,
                )
                self.assertEqual(
                    reopened_source.plan_bindings[0].binding_ordinal,
                    1,
                )
                self.assertNotEqual(
                    reopened_source.plan_bindings[0].position_lifecycle_id,
                    first_lifecycle_id,
                )
                persisted_chains = journal._connection.execute(
                    "SELECT position_lifecycle_id, binding_ordinal, "
                    "confirmation_execution_event_id "
                    "FROM actual_position_plan_bindings "
                    "ORDER BY id"
                ).fetchall()
                self.assertEqual(
                    tuple((str(row[0]), int(row[1])) for row in persisted_chains),
                    (
                        (first_lifecycle_id, 1),
                        (
                            reopened_source.plan_bindings[
                                0
                            ].position_lifecycle_id,
                            1,
                        ),
                    ),
                )
                self.assertEqual(
                    int(persisted_chains[1][2]),
                    reopen_action.execution_event_id,
                )
                self.assertNotEqual(
                    reopened_source.opening_actual_event_id,
                    first_source.opening_actual_event_id,
                )
                self.assertNotEqual(
                    reopened_source.opening_actual_lifecycle_id,
                    first_source.opening_actual_lifecycle_id,
                )
                self.assertNotEqual(
                    reopened_source.position_plan_digest,
                    first_source.position_plan_digest,
                )


if __name__ == "__main__":
    unittest.main()
