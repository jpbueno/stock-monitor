from __future__ import annotations

import copy
import unittest
from dataclasses import FrozenInstanceError, fields, replace
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

from stock_monitor.domain import stable_execution_event_identity
from stock_monitor.ledger import (
    ActualBuyContext,
    ActualLedger,
    ComplianceDecision,
    LedgerLot,
    LedgerPair,
    LedgerPosition,
    LedgerSignal,
)
from stock_monitor.risk import (
    AccountCheck,
    EquityPoint,
    ExecutionEvent,
    JournalEventWindow,
    RiskBlock,
    SessionCalendarResolver,
    combine_breaker_states,
    evaluate_breakers,
    LongPlanRequest,
    plan_long,
)
from stock_monitor.screening import select_publication_roles, to_scored_candidate
from tests.support import aware_et, calendar_fixture, seeded_ledgers
from tests.support import policy_fixture
from tests.unit.test_position_sizing import authorized_plan, authorized_state
from tests.unit._task5_fixtures import candidate_context, evidence
from tests.unit.test_ranking import candidate
from stock_monitor.market_calendar import MarketCalendar, load_current_market_calendar
import stock_monitor.ledger as ledger_module
import stock_monitor.risk as risk_module


def reviewed_calendar() -> MarketCalendar:
    return load_current_market_calendar(
        Path(__file__).resolve().parents[2],
        as_of=date(2026, 8, 14),
    )


def clean_breakers():
    return authorized_state().breaker_states[0]


def issued_buy_context(
    *,
    message_id: str,
    action_ordinal: int = 0,
    buy_at=None,
    check_at=None,
    stop: Decimal = Decimal("97.50"),
    bid: Decimal = Decimal("99.99"),
    ask: Decimal = Decimal("100"),
    price: Decimal = Decimal("100"),
    shares: int = 5,
    parent_order_id: str | None = None,
    fill_group_planned_shares: int | None = None,
) -> ActualBuyContext:
    buy_at = buy_at or aware_et(date(2026, 8, 14), "10:14")
    check_at = check_at or aware_et(date(2026, 8, 14), "10:10")
    check = AccountCheck(Decimal("5000"), 0, 0, check_at, cursor=10)
    event_id, idempotency_key = stable_execution_event_identity(
        message_id,
        action_ordinal,
    )
    action = risk_module._issue_confirmed_buy_action(
        event_id=event_id,
        idempotency_key=idempotency_key,
        message_id=message_id,
        action_ordinal=action_ordinal,
        cursor=12,
        symbol="SPY",
        shares=shares,
        price=price,
        at=buy_at,
        message_time=buy_at,
        received_at=buy_at,
        bid=bid,
        ask=ask,
        user_confirmed_stop=stop,
        source="ROBINHOOD_MANUAL_CONFIRMATION",
        raw_sha256="1" * 64,
        details_sha256="2" * 64,
        parent_order_id=parent_order_id,
        fill_group_planned_shares=fill_group_planned_shares,
    )
    window = risk_module._issue_journal_event_window(
        after_cursor=10,
        through_cursor=12,
        events=(),
        account_check=check,
        terminal_action=action,
    )
    return ledger_module._issue_actual_buy_context_from_action(
        signal_id="sig-1",
        action=action,
        account_check=check,
        event_window=window,
        breaker_state=clean_breakers(),
        calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
    )


class SeparateLedgerTests(unittest.TestCase):
    def test_partial_fill_handoff_retains_stable_parent_order(self) -> None:
        context = issued_buy_context(
            message_id="message:partial-handoff",
            shares=2,
            parent_order_id="robinhood:order:1",
            fill_group_planned_shares=5,
        )

        self.assertEqual(
            context.buy_action.parent_order_id,
            "robinhood:order:1",
        )
        self.assertEqual(context.buy_action.fill_group_planned_shares, 5)
        self.assertFalse(ledger_module.is_issued_actual_buy_context(context))

    def test_seed_fixture_does_not_register_raw_signal_authority(self) -> None:
        pair = seeded_ledgers()

        self.assertTrue(pair.signals)
        self.assertTrue(
            all(
                not ledger_module.is_issued_ledger_signal(signal)
                for signal in pair.signals
            )
        )
        self.assertFalse(
            hasattr(ledger_module, "_register_issued_ledger_signal")
        )
        self.assertFalse(hasattr(ledger_module, "_register_ledger_authority"))

    def test_unissued_clear_breaker_pair_still_pauses_new_live_entries(
        self,
    ) -> None:
        seed = seeded_ledgers()
        raw = clean_breakers()
        pair = LedgerPair.rebuild(
            seed.signals,
            (),
            canonical_breaker_state=raw.canonical,
            actual_breaker_state=raw.actual,
        )

        self.assertFalse(
            risk_module.is_issued_paired_breaker_state(
                pair.paired_breaker_state
            )
        )
        self.assertTrue(pair.new_live_entries_paused)

    def test_task6_quote_and_buy_context_builders_are_diagnostic_only(
        self,
    ) -> None:
        quote = ledger_module._issue_execution_quote_evidence(
            symbol="SPY",
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            observed_at=aware_et(date(2026, 8, 14), "10:13"),
            confirmed_at=aware_et(date(2026, 8, 14), "10:14"),
            cursor=11,
            source="ROBINHOOD_MANUAL_CONFIRMATION",
        )
        context = issued_buy_context(message_id="message:diagnostic-only")

        self.assertFalse(
            ledger_module.is_issued_execution_quote_evidence(quote)
        )
        self.assertFalse(ledger_module.is_issued_actual_buy_context(context))
        values = (
            quote.bid,
            quote.ask,
            context.bid,
            context.ask,
            context.user_confirmed_stop,
            context.buy_action.price,
            context.buy_action.bid,
            context.buy_action.ask,
            context.buy_action.user_confirmed_stop,
        )
        self.assertTrue(
            all(
                value is not None and value.as_tuple().exponent == -6
                for value in values
            )
        )

    def test_empty_or_caller_constructed_replay_cohort_is_never_verified(
        self,
    ) -> None:
        signals = seeded_ledgers().signals
        empty = LedgerPair(signals=signals)
        self.assertFalse(empty.canonical_replay_verified)
        self.assertFalse(empty.actual_replay_verified)
        self.assertFalse(empty.replay_verified)

        direct = ledger_module.VerifiedLedgerReplayCohort(
            ledger_name="CANONICAL",
            references=(),
            expected_count=0,
            start_cursor=None,
            terminal_cursor=None,
            query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
            source_digest="a" * 64,
        )
        rebuilt = LedgerPair(
            signals=signals,
            verified_replay_cohorts=(direct,),
        )
        self.assertFalse(rebuilt.canonical_replay_verified)

    def test_ledger_event_binds_exact_signal_contract_digest(self) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        pair.record_actual_buy(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )
        event = pair.events[0]

        self.assertEqual(
            event.signal_digest,
            ledger_module._ledger_signal_digest(pair.signals[0]),
        )
        changed = replace(
            pair.signals[0],
            recommended_stop=Decimal("97"),
            target=Decimal("106"),
        )
        self.assertNotEqual(
            event.signal_digest,
            ledger_module._ledger_signal_digest(changed),
        )

    def test_position_cost_basis_is_exact_micros_and_average_is_display_only(
        self,
    ) -> None:
        at = aware_et(date(2026, 8, 14), "10:14")
        lots = (
            ledger_module.LedgerLot(Decimal("100"), 1, at),
            ledger_module.LedgerLot(Decimal("101"), 2, at),
        )
        current = LedgerPosition(
            signal_id="sig-cost",
            symbol="SPY",
            ledger_name="ACTUAL",
            recommended_stop=Decimal("98"),
            user_confirmed_stop=Decimal("98"),
            target=Decimal("104"),
            tick_size=Decimal("0.01"),
            lots=lots,
            reconciled=False,
            reason_codes=("AUTHORITY_CONTEXT_UNVERIFIED",),
        )

        self.assertEqual(lots[0].total_cost_micros, 100_000_000)
        self.assertEqual(lots[1].total_cost_micros, 202_000_000)
        self.assertEqual(current.cost_basis_micros, 302_000_000)
        self.assertEqual(current.exposure, Decimal("302.000000"))
        self.assertEqual(current.shares, 3)
        self.assertEqual(current.entry * 3, current.exposure)

    def test_direct_position_and_ledger_derived_totals_must_fit_int64(self) -> None:
        at = aware_et(date(2026, 8, 14), "10:14")
        with self.assertRaisesRegex(RiskBlock, "^INVALID_LEDGER_LOT_COST$"):
            ledger_module.LedgerLot(Decimal("9000000000000"), 2, at)

        valid_lot = ledger_module.LedgerLot(
            Decimal("5000000000000"),
            1,
            at,
        )
        with self.assertRaisesRegex(RiskBlock, "^INVALID_POSITION_EXPOSURE$"):
            LedgerPosition(
                signal_id="sig-overflow",
                symbol="SPY",
                ledger_name="ACTUAL",
                recommended_stop=Decimal("1"),
                user_confirmed_stop=Decimal("1"),
                target=Decimal("5000000000001"),
                tick_size=Decimal("0.01"),
                lots=(valid_lot, valid_lot),
                reconciled=False,
                reason_codes=("AUTHORITY_CONTEXT_UNVERIFIED",),
            )
        valid_position = LedgerPosition(
            signal_id="sig-large",
            symbol="SPY",
            ledger_name="CANONICAL",
            recommended_stop=Decimal("1"),
            user_confirmed_stop=None,
            target=Decimal("5000000000001"),
            tick_size=Decimal("0.01"),
            lots=(valid_lot,),
            reconciled=False,
            reason_codes=("PAPER_ENTRY_AUTHORITY_UNVERIFIED",),
        )
        with self.assertRaisesRegex(RiskBlock, "^INVALID_DEPLOYED_CAPITAL$"):
            ledger_module.CanonicalLedger(
                open_positions=(valid_position, valid_position),
            )

    def test_ledger_rejects_duplicate_source_coordinates(self) -> None:
        source = LedgerPair(signals=seeded_ledgers().signals)
        source.record_actual_buy(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )
        first = replace(
            source.events[0],
            event_id="actual:coordinate:a",
            cursor=12,
            ordinal=0,
        )
        second = replace(
            source.events[0],
            event_id="actual:coordinate:b",
            cursor=12,
            ordinal=0,
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_SOURCE_COORDINATE_CONFLICT$",
        ):
            LedgerPair.rebuild(source.signals, (first, second))

    def test_same_event_id_with_changed_source_evidence_conflicts(self) -> None:
        first_context = issued_buy_context(message_id="message:source-conflict")
        changed_context = issued_buy_context(
            message_id="message:source-conflict",
            bid=Decimal("99.98"),
        )
        pair = LedgerPair(signals=seeded_ledgers().signals)
        kwargs = {
            "signal_id": "sig-1",
            "price": Decimal("100"),
            "shares": 5,
            "at": first_context.buy_action.at,
            "event_id": first_context.buy_action.event_id,
        }
        pair.record_actual_buy_with_context(
            **kwargs,
            context=first_context,
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_IDEMPOTENCY_CONFLICT$",
        ):
            pair.record_actual_buy_with_context(
                **kwargs,
                context=changed_context,
            )

    def test_ledger_rejects_int64_share_and_derived_money_overflow(self) -> None:
        with self.assertRaisesRegex(RiskBlock, "^INVALID_LEDGER_SHARES$"):
            ledger_module.LedgerLot(
                Decimal("1"),
                2**63,
                aware_et(date(2026, 8, 14), "10:14"),
            )
        pair = LedgerPair(signals=seeded_ledgers().signals)
        with self.assertRaisesRegex(
            RiskBlock,
            "^INVALID_LEDGER_LOT_COST$",
        ):
            pair.record_actual_buy(
                "sig-1",
                Decimal("9000000000000"),
                2,
                aware_et(date(2026, 8, 14), "10:14"),
            )
        self.assertEqual(pair.events, ())
        self.assertEqual(pair.actual.open_positions, ())

    def test_delayed_earlier_partial_fill_is_kept_in_effective_lot_order(
        self,
    ) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        later = aware_et(date(2026, 8, 14), "10:14")
        delayed = aware_et(date(2026, 8, 14), "10:12")

        pair.record_actual_buy("sig-1", Decimal("100"), 2, later)
        pair.record_actual_buy("sig-1", Decimal("99"), 3, delayed)

        self.assertEqual(
            tuple(lot.at for lot in pair.actual.open_positions[0].lots),
            (delayed, later),
        )
        self.assertEqual(len(pair.events), 2)

    def test_legacy_canonical_fill_is_diagnostic_and_cannot_count_phase_one(
        self,
    ) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)

        pair.record_canonical_fill(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertFalse(pair.events[0].decision.compliant)
        self.assertIn(
            "PAPER_ENTRY_AUTHORITY_UNVERIFIED",
            pair.events[0].decision.reason_codes,
        )
        self.assertFalse(pair.canonical_replay_verified)

    def test_direct_paper_entry_authority_cannot_authorize_canonical_fill(
        self,
    ) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        signal = pair.signals[0]
        authority = ledger_module.PaperEntryAuthority(
            signal_id=signal.signal_id,
            signal_digest="1" * 64,
            lifecycle_event_id="lifecycle:triggered",
            trigger_observation_id="trade:1",
            trigger_stream_id="trades:SIP:SPY",
            trigger_feed="SIP",
            trigger_at=aware_et(date(2026, 8, 14), "09:36"),
            trigger_received_at=aware_et(date(2026, 8, 14), "09:36"),
            trigger_sequence=100,
            trigger_source_cursor=10,
            trigger_source_ordinal=0,
            trigger_stream_through_cursor=10,
            trigger_cohort_ordinal=1,
            trigger_price=Decimal("99"),
            quote_observation_id="quote:2",
            quote_stream_id="quotes:SIP:SPY",
            quote_feed="SIP",
            quote_at=aware_et(date(2026, 8, 14), "09:37"),
            quote_received_at=aware_et(date(2026, 8, 14), "09:37"),
            quote_sequence=5,
            quote_source_cursor=20,
            quote_source_ordinal=0,
            quote_stream_through_cursor=20,
            quote_cohort_ordinal=2,
            bid=Decimal("99.98"),
            ask=Decimal("100"),
            source_digest="2" * 64,
            session_complete_digest="3" * 64,
            cohort_through_ordinal=2,
            cohort_received_through=aware_et(
                date(2026, 8, 14),
                "09:38",
            ),
            canonical_event_id="paper:1",
            lifecycle_cursor=30,
            action_ordinal=0,
            calendar_digest="4" * 64,
        )

        self.assertTrue(
            all(
                value.as_tuple().exponent == -6
                for value in (
                    authority.trigger_price,
                    authority.bid,
                    authority.ask,
                )
            )
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^PAPER_ENTRY_AUTHORITY_UNVERIFIED$",
        ):
            pair.record_authorized_canonical_fill(authority)
        same_time_ordered = replace(
            authority,
            quote_at=authority.trigger_at,
            quote_received_at=authority.trigger_received_at,
            trigger_sequence=None,
            quote_sequence=0,
        )
        self.assertFalse(
            ledger_module.is_issued_paper_entry_authority(same_time_ordered)
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^PAPER_QUOTE_NOT_AFTER_TRIGGER$",
        ):
            replace(
                authority,
                quote_at=authority.trigger_at,
                quote_cohort_ordinal=authority.trigger_cohort_ordinal,
            )
        with self.assertRaisesRegex(
            RiskBlock,
            "^INVALID_PAPER_ENTRY_AUTHORITY$",
        ):
            replace(authority, session_complete_digest="")
        with self.assertRaisesRegex(
            RiskBlock,
            "^INVALID_PAPER_QUOTE_SEQUENCE$",
        ):
            replace(authority, quote_sequence=-1)
        with self.assertRaisesRegex(
            RiskBlock,
            "^PAPER_STREAM_COHORT_INCOMPLETE$",
        ):
            replace(authority, quote_stream_through_cursor=19)
        with self.assertRaisesRegex(
            RiskBlock,
            "^PAPER_COHORT_RECEIPT_INCOMPLETE$",
        ):
            replace(
                authority,
                cohort_received_through=aware_et(
                    date(2026, 8, 14),
                    "09:36",
                ),
            )
        self.assertIn("PaperEntryAuthority", ledger_module.__all__)
        self.assertIn(
            "is_issued_paper_entry_authority",
            ledger_module.__all__,
        )
        self.assertFalse(
            hasattr(ledger_module, "_issue_paper_entry_authority")
        )

    def test_task6_only_buy_context_cannot_hide_same_session_breaker_change(
        self,
    ) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        context = issued_buy_context(message_id="message:stale-live-breaker")
        event_id = stable_execution_event_identity(
            "message:stale-live-breaker",
            0,
        )[0]

        decision = pair.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
            event_id=event_id,
        )

        self.assertFalse(decision.compliant)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            decision.reason_codes,
        )
        self.assertTrue(pair.actual.reconciliation_required)
        self.assertEqual(pair.actual.deployed_capital, Decimal("500"))

    def test_actual_portfolio_refresh_terminal_must_match_terminal_buy(self) -> None:
        context = issued_buy_context(message_id="message:refresh-terminal")
        action = context.buy_action
        assert action is not None
        resolver = context.calendar_resolver
        authority = risk_module.PortfolioRiskAuthority(
            request=LongPlanRequest(
                entry=action.price,
                stop=Decimal("97.50"),
                tick_size=Decimal("0.01"),
                session_date=action.at.date(),
            ),
            portfolio_state=authorized_state(),
            scope="ACTUAL_ENTRY",
            as_of=action.at,
            ledger_name="ACTUAL",
            projection_through_cursor=action.cursor - 1,
            settlement_through_cursor=action.cursor - 1,
            projection_digest="1" * 64,
            settlement_source="ACTUAL_SETTLEMENT_LEDGER",
            settlement_digest="2" * 64,
            policy_digest="3" * 64,
            calendar_digest=risk_module._calendar_digest(resolver),
            breaker_refresh_digest="4" * 64,
            breaker_refresh_through_execution_cursor=action.cursor - 1,
            breaker_refresh_through_close_cursor=7,
        )

        prebuy = replace(context, portfolio_authority=authority)
        self.assertFalse(ledger_module.is_issued_actual_buy_context(prebuy))
        stale_refresh = risk_module.ActualBreakerRefreshAuthority(
            as_of=action.at,
            through_execution_cursor=action.cursor - 1,
            through_close_cursor=7,
            paired_breaker=context.breaker_state,
            calendar_digest=risk_module._calendar_digest(resolver),
            source_digest="5" * 64,
        )
        current_refresh = replace(
            stale_refresh,
            through_execution_cursor=action.cursor,
        )
        with patch.object(
            ledger_module,
            "is_issued_actual_breaker_refresh_authority",
            return_value=True,
        ):
            with self.assertRaisesRegex(
                RiskBlock,
                "^ACTUAL_BREAKER_REFRESH_UNVERIFIED$",
            ):
                replace(
                    context,
                    portfolio_authority=authority,
                    actual_breaker_refresh=stale_refresh,
                )
            bound = replace(
                context,
                portfolio_authority=authority,
                actual_breaker_refresh=current_refresh,
            )
        self.assertEqual(
            bound.portfolio_authority.breaker_refresh_through_execution_cursor,
            action.cursor - 1,
        )
        self.assertEqual(
            bound.actual_breaker_refresh.through_execution_cursor,
            action.cursor,
        )
        self.assertEqual(
            bound.actual_breaker_refresh.through_close_cursor,
            7,
        )
        self.assertFalse(ledger_module.is_issued_actual_buy_context(bound))

    def test_canonical_rejects_duplicate_ticker_across_signal_ids(self) -> None:
        pair = seeded_ledgers()

        with self.assertRaisesRegex(
            RiskBlock,
            "^DUPLICATE_TICKER_EXPOSURE$",
        ):
            pair.record_canonical_fill(
                "sig-spy-2",
                Decimal("102"),
                2,
                aware_et(date(2026, 8, 17), "10:00"),
            )

    def test_unissued_primary_signal_cannot_authorize_actual_compliance(self) -> None:
        forged = LedgerSignal(
            signal_id="sig-1",
            symbol="SPY",
            role="PRIMARY",
            publication_session=date(2026, 8, 14),
            maximum_entry=Decimal("100"),
            recommended_stop=Decimal("97.50"),
            target=Decimal("105"),
            planned_shares=5,
            tick_size=Decimal("0.01"),
        )
        self.assertFalse(ledger_module.is_issued_ledger_signal(forged))
        context = issued_buy_context(message_id="message:unissued-signal")
        pair = LedgerPair(signals=(forged,))

        decision = pair.record_actual_buy_with_context(
            signal_id=forged.signal_id,
            price=Decimal("100"),
            shares=5,
            at=context.buy_action.at,
            context=context,
            event_id=context.buy_action.event_id,
        )

        self.assertFalse(decision.compliant)
        self.assertTrue(decision.reconciliation_required)
        self.assertIn("UNPLANNED_SIGNAL", decision.reason_codes)
        self.assertFalse(pair.actual_replay_verified)

    def test_actual_above_limit_fill_does_not_mutate_canonical_fill(self) -> None:
        ledgers = seeded_ledgers(canonical_entry=Decimal("100"))

        decision = ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("101"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertEqual(ledgers.canonical.open_positions[0].entry, Decimal("100"))
        self.assertEqual(ledgers.actual.open_positions[0].entry, Decimal("101"))
        self.assertTrue(ledgers.actual.reconciliation_required)
        self.assertTrue(decision.reconciliation_required)
        self.assertIn("FILL_ABOVE_MAXIMUM_ENTRY", decision.reason_codes)
        self.assertEqual(ledgers.actual.open_positions[0].target, Decimal("105"))
        self.assertEqual(
            ledgers.actual.open_positions[0].planned_risk,
            Decimal("505"),
        )

    def test_contextless_buy_records_exposure_but_never_invents_authority(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        decision = ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertFalse(decision.compliant)
        self.assertEqual(decision.status, "NONCOMPLIANT_RECONCILIATION_REQUIRED")
        self.assertIn("ACCOUNT_AUTHORITY_UNVERIFIED", decision.reason_codes)
        self.assertIn("BREAKER_AUTHORITY_UNVERIFIED", decision.reason_codes)
        self.assertIn("STOP_UNVERIFIED", decision.reason_codes)
        self.assertEqual(ledgers.actual.deployed_capital, Decimal("500"))

    def test_shadow_fill_is_preserved_as_real_off_policy_exposure(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        decision = ledgers.record_actual_buy(
            signal_id="sig-shadow",
            price=Decimal("200"),
            shares=2,
            at=aware_et(date(2026, 8, 14), "10:20"),
        )

        self.assertEqual(ledgers.actual.open_positions[0].symbol, "QQQ")
        self.assertEqual(ledgers.actual.open_positions[0].exposure, Decimal("400"))
        self.assertIn("SHADOW_FILL", decision.reason_codes)
        self.assertEqual(ledgers.canonical.open_positions, ())

    def test_duplicate_ticker_different_signal_remains_two_positions(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        decision = ledgers.record_actual_buy(
            signal_id="sig-spy-2",
            price=Decimal("102"),
            shares=2,
            at=aware_et(date(2026, 8, 17), "10:14"),
        )

        self.assertEqual(
            tuple(item.signal_id for item in ledgers.actual.open_positions),
            ("sig-1", "sig-spy-2"),
        )
        self.assertEqual(
            tuple(item.symbol for item in ledgers.actual.open_positions),
            ("SPY", "SPY"),
        )
        self.assertIn("DUPLICATE_TICKER_EXPOSURE", decision.reason_codes)

    def test_partial_lots_are_frozen_and_averaging_down_is_flagged(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=2,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        decision = ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("99"),
            shares=3,
            at=aware_et(date(2026, 8, 14), "10:15"),
        )

        current = ledgers.actual.open_positions[0]
        self.assertEqual(current.shares, 5)
        self.assertEqual(len(current.lots), 2)
        self.assertEqual(current.entry, Decimal("99.4"))
        self.assertIn("POSITION_ADDITIONS_PROHIBITED", decision.reason_codes)
        self.assertIn("AVERAGING_DOWN_PROHIBITED", decision.reason_codes)
        with self.assertRaises(FrozenInstanceError):
            current.lots[0].shares = 99  # type: ignore[misc]

    def test_snapshots_are_disjoint_and_constructor_defensively_copies(self) -> None:
        source = list(seeded_ledgers().signals)
        ledgers = LedgerPair(signals=source)
        source.clear()
        ledgers.record_canonical_fill(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )
        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertEqual(len(ledgers.signals), 3)
        canonical_position = ledgers.canonical.open_positions[0]
        actual_position = ledgers.actual.open_positions[0]
        self.assertIsNot(canonical_position, actual_position)
        self.assertIsNot(canonical_position.lots, actual_position.lots)
        self.assertIsNot(canonical_position.lots[0], actual_position.lots[0])
        with self.assertRaises(FrozenInstanceError):
            canonical_position.target = Decimal("999")  # type: ignore[misc]

    def test_rebuild_from_frozen_events_is_deterministic_and_non_aliasing(self) -> None:
        ledgers = seeded_ledgers()
        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("101"),
            shares=2,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        rebuilt = LedgerPair.rebuild(ledgers.signals, ledgers.events)

        self.assertEqual(rebuilt.canonical, ledgers.canonical)
        self.assertEqual(rebuilt.actual, ledgers.actual)
        self.assertEqual(rebuilt.events, ledgers.events)
        self.assertIsNot(
            rebuilt.canonical.open_positions[0],
            ledgers.canonical.open_positions[0],
        )
        self.assertIsNot(
            rebuilt.actual.open_positions[0].lots[0],
            ledgers.actual.open_positions[0].lots[0],
        )

        constructor_rebuild = LedgerPair(
            signals=ledgers.signals,
            events=ledgers.events,
        )
        self.assertEqual(constructor_rebuild.canonical, ledgers.canonical)
        self.assertEqual(constructor_rebuild.actual, ledgers.actual)

    def test_rebuild_is_idempotent_for_exact_duplicate_event_identity(self) -> None:
        ledgers = seeded_ledgers()
        event = ledgers.events[0]

        rebuilt = LedgerPair.rebuild(
            ledgers.signals,
            (event, event, event),
        )

        self.assertEqual(rebuilt.canonical.events_applied, 1)
        self.assertEqual(rebuilt.canonical.open_positions[0].shares, 5)
        self.assertEqual(rebuilt.canonical.deployed_capital, Decimal("500"))

    def test_rebuild_rejects_event_identity_conflict_and_out_of_order(self) -> None:
        ledgers = seeded_ledgers()
        canonical = ledgers.events[0]
        conflict = replace(
            canonical,
            lot=replace(canonical.lot, price=Decimal("99")),
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_IDEMPOTENCY_CONFLICT$",
        ):
            LedgerPair.rebuild(ledgers.signals, (canonical, conflict))

        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:15"),
        )
        rebuilt = LedgerPair.rebuild(
            ledgers.signals,
            tuple(reversed(ledgers.events)),
        )
        self.assertEqual(rebuilt.canonical.events_applied, 1)
        self.assertEqual(rebuilt.actual.events_applied, 1)

    def test_per_ledger_source_order_accepts_delayed_economic_facts(self) -> None:
        signals = seeded_ledgers().signals
        pair = LedgerPair(signals=signals)
        pair.record_actual_buy(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )
        pair.record_actual_buy(
            "sig-shadow",
            Decimal("200"),
            2,
            aware_et(date(2026, 8, 14), "10:12"),
        )
        first = replace(
            pair.events[0],
            event_id="actual:source:20",
            cursor=20,
            message_time=aware_et(date(2026, 8, 14), "10:14"),
            received_at=aware_et(date(2026, 8, 14), "10:15"),
        )
        delayed = replace(
            pair.events[1],
            event_id="actual:source:21",
            cursor=21,
            message_time=aware_et(date(2026, 8, 14), "10:15"),
            received_at=aware_et(date(2026, 8, 14), "10:16"),
        )

        rebuilt = LedgerPair.rebuild(signals, (first, delayed))

        self.assertEqual(rebuilt.actual.events_applied, 2)
        self.assertEqual(
            tuple(event.event_id for event in rebuilt.events),
            ("actual:source:20", "actual:source:21"),
        )
        with self.assertRaisesRegex(RiskBlock, "^LEDGER_EVENTS_OUT_OF_ORDER$"):
            LedgerPair.rebuild(
                signals,
                (replace(first, cursor=22), delayed),
            )

    def test_per_ledger_source_order_rejects_decreasing_knowledge_time(
        self,
    ) -> None:
        signals = seeded_ledgers().signals
        pair = LedgerPair(signals=signals)
        pair.record_actual_buy(
            "sig-1",
            Decimal("100"),
            5,
            aware_et(date(2026, 8, 14), "10:14"),
        )
        pair.record_actual_buy(
            "sig-shadow",
            Decimal("200"),
            2,
            aware_et(date(2026, 8, 14), "10:12"),
        )
        first = replace(
            pair.events[0],
            event_id="actual:source:20:knowledge",
            cursor=20,
            message_time=aware_et(date(2026, 8, 14), "10:29"),
            received_at=aware_et(date(2026, 8, 14), "10:30"),
        )
        decreasing = replace(
            pair.events[1],
            event_id="actual:source:21:knowledge",
            cursor=21,
            message_time=aware_et(date(2026, 8, 14), "10:19"),
            received_at=aware_et(date(2026, 8, 14), "10:20"),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_SOURCE_TIME_OUT_OF_ORDER$",
        ):
            LedgerPair.rebuild(signals, (first, decreasing))

    def test_rebuild_revalidates_canonical_event_invariants(self) -> None:
        ledgers = seeded_ledgers()
        event = ledgers.events[0]
        oversized = replace(event, lot=replace(event.lot, shares=50))

        with self.assertRaisesRegex(RiskBlock, "^SHARE_QUANTITY_MISMATCH$"):
            LedgerPair.rebuild(ledgers.signals, (oversized,))

    def test_rebuild_does_not_trust_forged_actual_compliance_decision(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )
        event = ledgers.events[0]
        forged = replace(
            event,
            decision=ComplianceDecision("COMPLIANT", True, False, ()),
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_DECISION_CONFLICT$",
        ):
            LedgerPair.rebuild(ledgers.signals, (forged,))

    def test_event_only_restart_batch_cannot_self_bless_forged_compliance(self) -> None:
        forged = ledger_module.LedgerEvent(
            ledger_name="ACTUAL",
            signal_id="sig-1",
            lot=LedgerLot(
                Decimal("100"),
                5,
                aware_et(date(2026, 8, 14), "10:14"),
            ),
            user_confirmed_stop=Decimal("97.50"),
            decision=ComplianceDecision("COMPLIANT", True, False, ()),
            event_id=stable_execution_event_identity("message:forged", 0)[0],
            cursor=12,
            authority_basis="0" * 64,
        )

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_SOURCE_EVIDENCE_REQUIRED$",
        ):
            ledger_module._issue_verified_ledger_event_batch((forged,))

    def test_persisted_event_digest_uses_canonical_money_and_timestamp_bytes(self) -> None:
        event = ledger_module.LedgerEvent(
            ledger_name="ACTUAL",
            signal_id="sig-1",
            lot=LedgerLot(
                Decimal("100"),
                5,
                aware_et(date(2026, 8, 14), "10:14"),
            ),
            user_confirmed_stop=Decimal("97.5"),
            decision=ComplianceDecision(
                "NONCOMPLIANT_RECONCILIATION_REQUIRED",
                False,
                True,
                ("AUTHORITY_CONTEXT_UNVERIFIED",),
            ),
            event_id=stable_execution_event_identity("message:digest", 0)[0],
            cursor=12,
        )
        rehydrated = replace(
            event,
            lot=replace(event.lot, price=Decimal("100.000000")),
            user_confirmed_stop=Decimal("97.500000"),
        )

        assert event.user_confirmed_stop is not None
        self.assertEqual(event.user_confirmed_stop.as_tuple().exponent, -6)
        self.assertEqual(
            ledger_module._ledger_event_content_digest(event),
            ledger_module._ledger_event_content_digest(rehydrated),
        )

    def test_unverified_restart_preserves_actual_exposure_but_downgrades_it(self) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        context = issued_buy_context(message_id="message:restart-downgrade")
        event_id = stable_execution_event_identity(
            "message:restart-downgrade",
            0,
        )[0]
        first = pair.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
            event_id=event_id,
        )
        self.assertFalse(first.compliant)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            first.reason_codes,
        )

        rebuilt = LedgerPair.rebuild(
            pair.signals,
            tuple(replace(event) for event in pair.events),
        )

        self.assertEqual(rebuilt.actual.deployed_capital, Decimal("500"))
        self.assertTrue(rebuilt.actual.reconciliation_required)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            rebuilt.actual.reason_codes,
        )
        self.assertFalse(rebuilt.replay_verified)

    def test_pair_rejects_injected_snapshot_and_projection_reassignment(self) -> None:
        forged_position = LedgerPosition(
            signal_id="sig-1",
            symbol="SPY",
            ledger_name="CANONICAL",
            recommended_stop=Decimal("97.50"),
            user_confirmed_stop=None,
            target=Decimal("105"),
            tick_size=Decimal("0.01"),
            lots=(
                LedgerLot(
                    Decimal("100"),
                    5,
                    aware_et(date(2026, 8, 14), "10:14"),
                ),
            ),
            reconciled=True,
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^CANONICAL_PROJECTION_WITHOUT_EVENTS$",
        ):
            LedgerPair(
                signals=seeded_ledgers().signals,
                canonical=ledger_module.CanonicalLedger(
                    open_positions=(forged_position,),
                    events_applied=999,
                ),
            )

        pair = LedgerPair(signals=seeded_ledgers().signals)
        with self.assertRaises(AttributeError):
            pair.canonical = ledger_module.CanonicalLedger()
        with self.assertRaises(AttributeError):
            pair.actual = ActualLedger()
        with self.assertRaises(AttributeError):
            pair._events = ()

    def test_caller_constructed_context_remains_non_authoritative(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        window = JournalEventWindow(
            after_cursor=10,
            through_cursor=12,
            events=(),
            complete=True,
            source="JOURNAL",
        )
        context = ActualBuyContext(
            account_check=check,
            event_window=window,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            user_confirmed_stop=Decimal("97.50"),
            breaker_state=clean_breakers(),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )

        decision = ledgers.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
        )

        self.assertFalse(decision.compliant)
        self.assertIn("AUTHORITY_CONTEXT_UNVERIFIED", decision.reason_codes)
        self.assertNotIn("BREAKER_AS_OF_MISMATCH", decision.reason_codes)
        self.assertTrue(ledgers.actual.reconciliation_required)
        self.assertFalse(ledgers.actual.stop_unverified)
        self.assertEqual(
            ledgers.actual.open_positions[0].user_confirmed_stop,
            Decimal("97.50"),
        )

    def test_adapter_context_without_live_breaker_refresh_stays_replayable_but_noncompliant(
        self,
    ) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        context = issued_buy_context(
            message_id="message:msg-1",
        )
        event_id = stable_execution_event_identity("message:msg-1", 0)[0]
        buy = dict(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
            event_id=event_id,
        )

        first = ledgers.record_actual_buy_with_context(**buy)
        second = ledgers.record_actual_buy_with_context(**buy)

        self.assertFalse(first.compliant)
        self.assertEqual(second, first)
        self.assertTrue(ledgers.actual.reconciliation_required)
        self.assertFalse(ledgers.replay_verified)
        self.assertEqual(ledgers.actual.events_applied, 1)
        self.assertEqual(len(ledgers.events), 1)
        copied_events = tuple(replace(event) for event in ledgers.events)
        rebuilt = LedgerPair.rebuild(ledgers.signals, copied_events)
        self.assertEqual(rebuilt.actual.deployed_capital, Decimal("500"))
        self.assertTrue(rebuilt.actual.reconciliation_required)
        self.assertFalse(rebuilt.replay_verified)

        with self.assertRaisesRegex(
            RiskBlock,
            "^LEDGER_EVENT_IDEMPOTENCY_CONFLICT$",
        ):
            ledgers.record_actual_buy_with_context(
                **{**buy, "price": Decimal("99.99")}
            )

    def test_copied_or_recomputed_adapter_context_cannot_authorize_fill(self) -> None:
        check = AccountCheck(
            settled_cash=Decimal("5000"),
            pending_orders=0,
            unlogged_positions=0,
            at=aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        copy_event_id, copy_key = stable_execution_event_identity(
            "message:copy-window",
            0,
        )
        action = risk_module._issue_confirmed_buy_action(
            event_id=copy_event_id,
            idempotency_key=copy_key,
            message_id="message:copy-window",
            action_ordinal=0,
            cursor=12,
            symbol="SPY",
            shares=5,
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "10:14"),
            message_time=aware_et(date(2026, 8, 14), "10:14"),
            received_at=aware_et(date(2026, 8, 14), "10:14"),
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            user_confirmed_stop=Decimal("97.50"),
            source="ROBINHOOD_MANUAL_CONFIRMATION",
            raw_sha256="3" * 64,
            details_sha256="4" * 64,
        )
        issued = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=action,
        )
        base = ActualBuyContext(
            account_check=check,
            event_window=issued,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            user_confirmed_stop=Decimal("97.50"),
            breaker_state=clean_breakers(),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )
        windows = (
            copy.copy(issued),
            replace(issued),
            JournalEventWindow(
                issued.after_cursor,
                issued.through_cursor,
                issued.events,
                issued.complete,
                issued.source,
            ),
        )

        for ordinal, window in enumerate(windows):
            with self.subTest(window=window):
                ledgers = LedgerPair(signals=seeded_ledgers().signals)
                decision = ledgers.record_actual_buy_with_context(
                    signal_id="sig-1",
                    price=Decimal("100"),
                    shares=5,
                    at=aware_et(date(2026, 8, 14), "10:14"),
                    context=replace(base, event_window=window),
                    event_id=f"actual:message:forged:{ordinal}",
                )
                self.assertFalse(decision.compliant)
                self.assertIn(
                    "AUTHORITY_CONTEXT_UNVERIFIED",
                    decision.reason_codes,
                )

    def test_authorized_actual_entry_is_strictly_after_0935_et(self) -> None:
        base_at = aware_et(date(2026, 8, 14), "09:35")
        boundary_context = issued_buy_context(
            message_id="message:boundary",
            buy_at=base_at,
            check_at=aware_et(date(2026, 8, 14), "09:33"),
        )

        at_boundary = LedgerPair(signals=seeded_ledgers().signals)
        rejected = at_boundary.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=base_at,
            context=boundary_context,
            event_id=stable_execution_event_identity("message:boundary", 0)[0],
        )
        accepted = LedgerPair(signals=seeded_ledgers().signals)
        accepted_at = base_at.replace(microsecond=1)
        accepted_context = issued_buy_context(
            message_id="message:boundary",
            action_ordinal=1,
            buy_at=accepted_at,
            check_at=aware_et(date(2026, 8, 14), "09:33"),
        )
        compliant = accepted.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=accepted_at,
            context=accepted_context,
            event_id=stable_execution_event_identity("message:boundary", 1)[0],
        )

        self.assertFalse(rejected.compliant)
        self.assertIn("ENTRY_NOT_AFTER_0935", rejected.reason_codes)
        self.assertFalse(compliant.compliant)
        self.assertNotIn("ENTRY_NOT_AFTER_0935", compliant.reason_codes)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            compliant.reason_codes,
        )

    def test_actual_stop_must_be_tick_aligned_and_between_recommendation_and_fill(self) -> None:
        for ordinal, stop, reason in (
            (1, Decimal("99.999999"), "USER_STOP_NOT_TICK_ALIGNED"),
            (2, Decimal("101"), "USER_STOP_NOT_BELOW_FILL"),
            (3, Decimal("97"), "USER_STOP_WIDER_THAN_RECOMMENDED"),
        ):
            with self.subTest(stop=stop):
                ledgers = LedgerPair(signals=seeded_ledgers().signals)
                context = issued_buy_context(
                    message_id="message:stop",
                    action_ordinal=ordinal,
                    stop=stop,
                )
                decision = ledgers.record_actual_buy_with_context(
                    signal_id="sig-1",
                    price=Decimal("100"),
                    shares=5,
                    at=aware_et(date(2026, 8, 14), "10:14"),
                    context=context,
                    event_id=stable_execution_event_identity(
                        "message:stop",
                        ordinal,
                    )[0],
                )
                self.assertFalse(decision.compliant)
                self.assertIn(reason, decision.reason_codes)

        tight = LedgerPair(signals=seeded_ledgers().signals)
        tight_context = issued_buy_context(
            message_id="message:stop-tight",
            stop=Decimal("99.99"),
        )
        decision = tight.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=tight_context,
            event_id=stable_execution_event_identity("message:stop-tight", 0)[0],
        )
        self.assertFalse(decision.compliant)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            decision.reason_codes,
        )
        self.assertEqual(tight.actual.open_planned_risk, Decimal("12.50"))

    def test_authorized_buy_derives_execution_and_quote_from_issued_evidence(self) -> None:
        context = issued_buy_context(
            message_id="message:evidence",
        )
        buy = context.buy_action
        assert buy is not None

        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        decision = ledgers.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=buy.at,
            context=context,
            event_id=stable_execution_event_identity("message:evidence", 0)[0],
        )
        self.assertFalse(decision.compliant)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            decision.reason_codes,
        )

        for forged in (
            copy.copy(context),
            replace(context),
            replace(context, bid=Decimal("99")),
        ):
            with self.subTest(forged=forged):
                pair = LedgerPair(signals=seeded_ledgers().signals)
                result = pair.record_actual_buy_with_context(
                    signal_id="sig-1",
                    price=Decimal("100"),
                    shares=5,
                    at=buy.at,
                    context=forged,
                    event_id=stable_execution_event_identity(
                        "message:evidence-copy",
                        0,
                    )[0],
                )
                self.assertFalse(result.compliant)
                self.assertIn(
                    "AUTHORITY_CONTEXT_UNVERIFIED",
                    result.reason_codes,
                )

    def test_authorized_buy_uses_one_exact_terminal_confirmation_action(self) -> None:
        check = AccountCheck(
            Decimal("5000"),
            0,
            0,
            aware_et(date(2026, 8, 14), "10:10"),
            cursor=10,
        )
        event_id, idempotency_key = stable_execution_event_identity(
            "message:terminal",
            0,
        )
        action = risk_module._issue_confirmed_buy_action(
            event_id=event_id,
            idempotency_key=idempotency_key,
            message_id="message:terminal",
            action_ordinal=0,
            cursor=12,
            symbol="SPY",
            shares=5,
            price=Decimal("100"),
            at=aware_et(date(2026, 8, 14), "10:14"),
            message_time=aware_et(date(2026, 8, 14), "10:14"),
            received_at=aware_et(date(2026, 8, 14), "10:14"),
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            user_confirmed_stop=Decimal("97.50"),
            source="ROBINHOOD_MANUAL_CONFIRMATION",
            raw_sha256="1" * 64,
            details_sha256="2" * 64,
        )
        window = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=action,
        )
        context = ledger_module._issue_actual_buy_context_from_action(
            signal_id="sig-1",
            action=action,
            account_check=check,
            event_window=window,
            breaker_state=clean_breakers(),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )

        pair = LedgerPair(signals=seeded_ledgers().signals)
        decision = pair.record_actual_buy_with_context(
            signal_id="sig-1",
            price=action.price,
            shares=action.shares,
            at=action.at,
            context=context,
            event_id=action.event_id,
        )
        self.assertFalse(decision.compliant)
        self.assertIn(
            "ACTUAL_BREAKER_REFRESH_UNVERIFIED",
            decision.reason_codes,
        )

        for field, value in (
            ("price", Decimal("99.99")),
            ("shares", 4),
            ("at", aware_et(date(2026, 8, 14), "10:15")),
            (
                "event_id",
                stable_execution_event_identity("message:terminal", 1)[0],
            ),
        ):
            with self.subTest(field=field):
                kwargs = {
                    "signal_id": "sig-1",
                    "price": action.price,
                    "shares": action.shares,
                    "at": action.at,
                    "context": context,
                    "event_id": action.event_id,
                }
                kwargs[field] = value
                other = LedgerPair(signals=seeded_ledgers().signals)
                result = other.record_actual_buy_with_context(**kwargs)
                self.assertFalse(result.compliant)
                self.assertIn("ACTUAL_BUY_EVIDENCE_MISMATCH", result.reason_codes)

        with self.assertRaisesRegex(RiskBlock, "INVALID_CONFIRMATION_SOURCE"):
            bad_event_id, bad_idempotency_key = stable_execution_event_identity(
                "message:bad-source",
                0,
            )
            risk_module._issue_confirmed_buy_action(
                event_id=bad_event_id,
                idempotency_key=bad_idempotency_key,
                message_id="message:bad-source",
                action_ordinal=0,
                cursor=13,
                symbol="SPY",
                shares=5,
                price=Decimal("100"),
                at=action.at,
                message_time=action.message_time,
                received_at=action.received_at,
                bid=Decimal("99.99"),
                ask=Decimal("100"),
                user_confirmed_stop=Decimal("97.50"),
                source="CALLER_ASSERTED",
                raw_sha256="3" * 64,
                details_sha256="4" * 64,
            )

    def test_actual_fill_at_or_below_recommended_stop_is_noncompliant(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        context = ActualBuyContext(
            account_check=AccountCheck(
                settled_cash=Decimal("5000"),
                pending_orders=0,
                unlogged_positions=0,
                at=aware_et(date(2026, 8, 14), "10:10"),
                cursor=10,
            ),
            event_window=JournalEventWindow(
                after_cursor=10,
                through_cursor=12,
                events=(),
                complete=True,
                source="JOURNAL",
            ),
            bid=Decimal("97.49"),
            ask=Decimal("97.50"),
            user_confirmed_stop=Decimal("97.50"),
            breaker_state=clean_breakers(),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )

        decision = ledgers.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("97.50"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
        )

        self.assertFalse(decision.compliant)
        self.assertIn("NON_POSITIVE_STOP_DISTANCE", decision.reason_codes)
        self.assertEqual(ledgers.actual.deployed_capital, Decimal("487.50"))

    def test_confirmed_wider_stop_drives_conservative_actual_risk(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)
        context = ActualBuyContext(
            account_check=AccountCheck(
                settled_cash=Decimal("5000"),
                pending_orders=0,
                unlogged_positions=0,
                at=aware_et(date(2026, 8, 14), "10:10"),
                cursor=10,
            ),
            event_window=JournalEventWindow(
                after_cursor=10,
                through_cursor=12,
                events=(),
                complete=True,
                source="JOURNAL",
            ),
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            user_confirmed_stop=Decimal("90"),
            breaker_state=clean_breakers(),
            calendar_resolver=SessionCalendarResolver((reviewed_calendar(),)),
        )

        decision = ledgers.record_actual_buy_with_context(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
            context=context,
        )

        self.assertIn("USER_STOP_WIDER_THAN_RECOMMENDED", decision.reason_codes)
        self.assertEqual(ledgers.actual.open_planned_risk, Decimal("50"))

    def test_canonical_fill_uses_published_maximum_for_conservative_projection(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        ledgers.record_canonical_fill(
            signal_id="sig-1",
            price=Decimal("99"),
            shares=5,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertEqual(ledgers.canonical.open_positions[0].entry, Decimal("100"))

    def test_missing_breaker_projection_pauses_entry_status(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        self.assertTrue(ledgers.new_live_entries_paused)

    def test_real_exposure_over_hard_cap_is_recorded_and_pauses_entries(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        decision = ledgers.record_actual_buy(
            signal_id="sig-1",
            price=Decimal("100"),
            shares=11,
            at=aware_et(date(2026, 8, 14), "10:14"),
        )

        self.assertEqual(ledgers.actual.deployed_capital, Decimal("1100"))
        self.assertIn("LIVE_EXPOSURE_LIMIT_BREACHED", decision.reason_codes)
        self.assertTrue(ledgers.new_live_entries_paused)

    def test_ledger_money_arithmetic_ignores_ambient_decimal_precision(self) -> None:
        ledgers = LedgerPair(signals=seeded_ledgers().signals)

        with localcontext() as context:
            context.prec = 6
            ledgers.record_actual_buy(
                signal_id="sig-1",
                price=Decimal("99.999999"),
                shares=5,
                at=aware_et(date(2026, 8, 14), "10:14"),
            )
            deployed = ledgers.actual.deployed_capital
            cash = ledgers.actual.cash

        self.assertEqual(deployed, Decimal("499.999995"))
        self.assertEqual(cash, Decimal("4500.000005"))

    def test_nested_reason_sequences_are_defensively_frozen(self) -> None:
        source_reasons = ["MANUAL_REVIEW"]
        decision = ComplianceDecision(
            "NONCOMPLIANT_RECONCILIATION_REQUIRED",
            False,
            True,
            source_reasons,  # type: ignore[arg-type]
        )
        position = LedgerPosition(
            signal_id="sig-1",
            symbol="SPY",
            ledger_name="ACTUAL",
            recommended_stop=Decimal("97.50"),
            user_confirmed_stop=None,
            target=Decimal("105"),
            tick_size=Decimal("0.01"),
            lots=(
                LedgerLot(
                    Decimal("100"),
                    5,
                    aware_et(date(2026, 8, 14), "10:14"),
                ),
            ),
            reconciled=False,
            reason_codes=source_reasons,  # type: ignore[arg-type]
        )
        snapshot = ActualLedger(
            open_positions=(position,),
            reconciliation_required=True,
            reason_codes=source_reasons,  # type: ignore[arg-type]
            stop_unverified=True,
        )

        source_reasons.append("MUTATED")

        self.assertEqual(decision.reason_codes, ("MANUAL_REVIEW",))
        self.assertEqual(position.reason_codes, ("MANUAL_REVIEW",))
        self.assertEqual(snapshot.reason_codes, ("MANUAL_REVIEW",))

    def test_ledger_signal_consumes_task5_contract_and_rejects_submicro_money(self) -> None:
        scored = to_scored_candidate(
            candidate_context(evidence=evidence(age_days=11))
        )
        publication = select_publication_roles(
            (scored,),
            capacity_available=True,
        )
        sizing = authorized_plan(LongPlanRequest.from_scored_candidate(scored))
        self.assertTrue(sizing.eligible)
        self.assertFalse(risk_module.is_issued_long_plan_decision(sizing))
        with self.assertRaisesRegex(
            RiskBlock,
            "^PUBLICATION_AUTHORITY_UNVERIFIED$",
        ):
            LedgerSignal.from_publication_decision(
                publication,
                rank=1,
                plan_decision=sizing,
            )
        signal = LedgerSignal.from_scored_candidate(
            scored,
            role="WATCHLIST_SHADOW",
            planned_shares=sizing.plan.quantity,
        )

        self.assertEqual(signal.maximum_entry, scored.maximum_permitted_entry)
        self.assertEqual(signal.trigger_price, scored.trigger_price)
        self.assertEqual(signal.target, scored.target_price)
        self.assertEqual(signal.planned_shares, sizing.plan.quantity)
        changed_trigger = replace(
            signal,
            trigger_price=signal.trigger_price + signal.tick_size,
        )
        self.assertNotEqual(
            ledger_module._ledger_signal_digest(signal),
            ledger_module._ledger_signal_digest(changed_trigger),
        )
        with self.assertRaisesRegex(RiskBlock, "^INVALID_SIGNAL_ENTRY$"):
            replace(signal, maximum_entry=Decimal("100.0000001"))

        with self.assertRaisesRegex(
            RiskBlock,
            "^PUBLICATION_AUTHORITY_UNVERIFIED$",
        ):
            LedgerSignal.from_scored_candidate(
                scored,
                role="PRIMARY",
                planned_shares=1,
            )

    def test_paper_fill_requires_trade_to_reach_published_trigger(self) -> None:
        scored = to_scored_candidate(
            candidate_context(evidence=evidence(age_days=11))
        )
        sizing = authorized_plan(LongPlanRequest.from_scored_candidate(scored))
        signal = LedgerSignal.from_scored_candidate(
            scored,
            role="WATCHLIST_SHADOW",
            planned_shares=sizing.plan.quantity,
        )
        signal = replace(signal, role="PRIMARY")
        authority = ledger_module.PaperEntryAuthority(
            signal_id=signal.signal_id,
            signal_digest=ledger_module._ledger_signal_digest(signal),
            lifecycle_event_id="lifecycle:triggered",
            trigger_observation_id="trade:below-published-trigger",
            trigger_stream_id=f"trades:SIP:{signal.symbol}",
            trigger_feed="SIP",
            trigger_at=aware_et(signal.publication_session, "09:36"),
            trigger_received_at=aware_et(signal.publication_session, "09:36"),
            trigger_sequence=100,
            trigger_source_cursor=10,
            trigger_source_ordinal=0,
            trigger_stream_through_cursor=10,
            trigger_cohort_ordinal=1,
            trigger_price=Decimal("0.01"),
            quote_observation_id="quote:after-trigger",
            quote_stream_id=f"quotes:SIP:{signal.symbol}",
            quote_feed="SIP",
            quote_at=aware_et(signal.publication_session, "09:37"),
            quote_received_at=aware_et(signal.publication_session, "09:37"),
            quote_sequence=5,
            quote_source_cursor=20,
            quote_source_ordinal=0,
            quote_stream_through_cursor=20,
            quote_cohort_ordinal=2,
            bid=signal.maximum_entry - signal.tick_size,
            ask=signal.maximum_entry,
            source_digest="2" * 64,
            session_complete_digest="3" * 64,
            cohort_through_ordinal=2,
            cohort_received_through=aware_et(
                signal.publication_session,
                "09:38",
            ),
            canonical_event_id="paper:below-published-trigger",
            lifecycle_cursor=30,
            action_ordinal=0,
            calendar_digest="4" * 64,
        )
        pair = LedgerPair(signals=(signal,))

        with (
            patch.object(
                ledger_module,
                "is_issued_paper_entry_authority",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_ledger_signal",
                return_value=True,
            ),
            self.assertRaisesRegex(
                RiskBlock,
                "^PAPER_ENTRY_EVIDENCE_MISMATCH$",
            ),
        ):
            pair.record_authorized_canonical_fill(authority)

    def test_actual_buy_context_carries_exact_actual_entry_plan(self) -> None:
        self.assertIn(
            "plan_decision",
            {field.name for field in fields(ActualBuyContext)},
        )

    def test_actual_buy_context_rejects_wrong_or_ineligible_plan(self) -> None:
        context = issued_buy_context(message_id="message:plan-binding")
        action = context.buy_action
        assert action is not None
        basis = authorized_state()
        resolver = context.calendar_resolver
        authority = risk_module.PortfolioRiskAuthority(
            request=LongPlanRequest(
                entry=action.price,
                stop=Decimal("97.50"),
                tick_size=Decimal("0.01"),
                session_date=action.at.date(),
                symbol=action.symbol,
                published_target=Decimal("105"),
            ),
            portfolio_state=basis,
            scope="ACTUAL_ENTRY",
            as_of=action.at,
            ledger_name="ACTUAL",
            projection_through_cursor=action.cursor,
            settlement_through_cursor=action.cursor,
            projection_digest="1" * 64,
            settlement_source="ACTUAL_SETTLEMENT_LEDGER",
            settlement_digest="2" * 64,
            policy_digest="3" * 64,
            calendar_digest=risk_module._calendar_digest(resolver),
            breaker_refresh_digest="4" * 64,
            breaker_refresh_through_execution_cursor=action.cursor,
            breaker_refresh_through_close_cursor=7,
        )
        valid_plan = risk_module.LongPlanDecision(
            eligible=True,
            reason_codes=(),
            plan=risk_module.size_long(
                action.price,
                Decimal("97.50"),
                Decimal("5000"),
                Decimal("0"),
                Decimal("0"),
            ),
            target=Decimal("105"),
            request=authority.request,
            authority_scope="ACTUAL_ENTRY",
            authority_digest="5" * 64,
            as_of=action.at,
            portfolio_authority=authority,
        )
        wrong_symbol = replace(
            valid_plan,
            request=replace(authority.request, symbol="QQQ"),
        )
        zero_cash_authority = replace(
            authority,
            portfolio_state=replace(
                authority.portfolio_state,
                settled_cash=Decimal("0"),
            ),
        )
        zero_cash = replace(
            valid_plan,
            portfolio_authority=zero_cash_authority,
        )
        ineligible = risk_module.LongPlanDecision(
            eligible=False,
            reason_codes=("INSUFFICIENT_SETTLED_CASH",),
            plan=None,
            target=None,
            request=authority.request,
            authority_scope="ACTUAL_ENTRY",
            authority_digest="6" * 64,
            as_of=action.at,
            portfolio_authority=authority,
        )

        with patch.object(
            ledger_module,
            "is_issued_long_plan_decision",
            return_value=True,
        ):
            for plan in (wrong_symbol, zero_cash, ineligible):
                with self.subTest(plan=plan), self.assertRaisesRegex(
                    RiskBlock,
                    "^POSITION_PLAN_AUTHORITY_UNVERIFIED$",
                ):
                    replace(
                        context,
                        portfolio_authority=authority,
                        plan_decision=plan,
                    )

    def test_actual_entry_plan_must_match_receiving_projection(self) -> None:
        context = issued_buy_context(message_id="message:stale-projection")
        action = context.buy_action
        assert action is not None
        stale_state = replace(
            authorized_state(),
            deployed=Decimal("100"),
            open_risk=Decimal("10"),
            open_position_count=1,
            open_symbols=("QQQ",),
        )
        request = LongPlanRequest(
            entry=action.price,
            stop=Decimal("97.50"),
            tick_size=Decimal("0.01"),
            session_date=action.at.date(),
            symbol=action.symbol,
            published_target=Decimal("105"),
        )
        authority = risk_module.PortfolioRiskAuthority(
            request=request,
            portfolio_state=stale_state,
            scope="ACTUAL_ENTRY",
            as_of=action.at,
            ledger_name="ACTUAL",
            projection_through_cursor=action.cursor,
            settlement_through_cursor=action.cursor,
            projection_digest="1" * 64,
            settlement_source="ACTUAL_SETTLEMENT_LEDGER",
            settlement_digest="2" * 64,
            policy_digest="3" * 64,
            calendar_digest=risk_module._calendar_digest(
                context.calendar_resolver
            ),
            breaker_refresh_digest="4" * 64,
            breaker_refresh_through_execution_cursor=action.cursor,
            breaker_refresh_through_close_cursor=7,
        )
        plan = risk_module.LongPlanDecision(
            eligible=True,
            reason_codes=(),
            plan=risk_module.PositionPlan(
                quantity=5,
                exposure=Decimal("500"),
                planned_risk=Decimal("12.50"),
            ),
            target=Decimal("105"),
            request=request,
            authority_scope="ACTUAL_ENTRY",
            authority_digest="5" * 64,
            as_of=action.at,
            portfolio_authority=authority,
        )

        with patch.object(
            ledger_module,
            "is_issued_long_plan_decision",
            return_value=True,
        ):
            bound = replace(
                context,
                portfolio_authority=authority,
                plan_decision=plan,
            )
            decision = LedgerPair(
                signals=seeded_ledgers().signals
            ).record_actual_buy_with_context(
                signal_id="sig-1",
                price=action.price,
                shares=action.shares,
                at=action.at,
                context=bound,
                event_id=action.event_id,
            )

        self.assertIn(
            "POSITION_PLAN_AUTHORITY_UNVERIFIED",
            decision.reason_codes,
        )

    def test_delayed_partial_fill_same_parent_is_not_an_addition(self) -> None:
        pair = LedgerPair(signals=seeded_ledgers().signals)
        parent = "robinhood:order:1"
        later = aware_et(date(2026, 8, 14), "10:14")
        delayed = aware_et(date(2026, 8, 14), "10:12")

        pair.record_actual_buy(
            "sig-1",
            Decimal("100"),
            2,
            later,
            parent_order_id=parent,
        )
        decision = pair.record_actual_buy(
            "sig-1",
            Decimal("99"),
            3,
            delayed,
            parent_order_id=parent,
        )

        self.assertEqual(
            tuple(lot.parent_order_id for lot in pair.actual.open_positions[0].lots),
            (parent, parent),
        )
        self.assertNotIn("POSITION_ADDITIONS_PROHIBITED", decision.reason_codes)
        self.assertNotIn("AVERAGING_DOWN_PROHIBITED", decision.reason_codes)
        self.assertNotIn("SHARE_QUANTITY_MISMATCH", decision.reason_codes)
        self.assertEqual(pair.actual.open_positions[0].shares, 5)

    def test_partial_fill_parent_change_or_overfill_is_noncompliant(self) -> None:
        first_parent = "robinhood:order:1"
        different_parent = LedgerPair(signals=seeded_ledgers().signals)
        different_parent.record_actual_buy(
            "sig-1",
            Decimal("100"),
            2,
            aware_et(date(2026, 8, 14), "10:12"),
            parent_order_id=first_parent,
        )
        changed = different_parent.record_actual_buy(
            "sig-1",
            Decimal("100"),
            3,
            aware_et(date(2026, 8, 14), "10:14"),
            parent_order_id="robinhood:order:2",
        )
        self.assertIn("POSITION_ADDITIONS_PROHIBITED", changed.reason_codes)

        overfilled = LedgerPair(signals=seeded_ledgers().signals)
        overfilled.record_actual_buy(
            "sig-1",
            Decimal("100"),
            2,
            aware_et(date(2026, 8, 14), "10:12"),
            parent_order_id=first_parent,
        )
        overflow = overfilled.record_actual_buy(
            "sig-1",
            Decimal("100"),
            4,
            aware_et(date(2026, 8, 14), "10:14"),
            parent_order_id=first_parent,
        )
        self.assertIn("SHARE_QUANTITY_MISMATCH", overflow.reason_codes)

    def test_authorized_same_parent_partials_reuse_exact_entry_plan(self) -> None:
        signal = seeded_ledgers().signals[0]
        resolver = authorized_state().calendar_resolver
        assert resolver is not None
        breaker = clean_breakers()
        first_at = aware_et(signal.publication_session, "10:12")
        cohort = ledger_module.VerifiedLedgerReplayCohort(
            ledger_name="ACTUAL",
            references=(),
            expected_count=0,
            start_cursor=None,
            terminal_cursor=None,
            query_cutoff=first_at,
            source_digest="a" * 64,
        )
        with patch.object(
            ledger_module,
            "is_issued_verified_replay_cohort",
            return_value=True,
        ):
            pair = LedgerPair(
                signals=(signal,),
                verified_replay_cohorts=(cohort,),
            )
        settlement = risk_module.SettlementLedger(
            Decimal("500"),
            aware_et(signal.publication_session, "10:00"),
            resolver,
        )
        refresh_one = risk_module.ActualBreakerRefreshAuthority(
            as_of=first_at,
            through_execution_cursor=12,
            through_close_cursor=0,
            paired_breaker=breaker,
            calendar_digest=risk_module._calendar_digest(resolver),
            source_digest="b" * 64,
        )
        request = LongPlanRequest(
            signal.maximum_entry,
            signal.recommended_stop,
            signal.tick_size,
            signal.publication_session,
            symbol=signal.symbol,
            published_target=signal.target,
        )
        policy = policy_fixture()
        with (
            patch.object(
                risk_module.SettlementLedger,
                "source_verified",
                new=property(lambda _self: True),
            ),
            patch.object(
                risk_module,
                "is_issued_paired_breaker_state",
                return_value=True,
            ),
            patch.object(
                risk_module,
                "is_issued_actual_breaker_refresh_authority",
                return_value=True,
            ),
        ):
            authority = risk_module._issue_portfolio_risk_authority(
                request=request,
                ledger_pair=pair,
                ledger_name="ACTUAL",
                breaker_state=breaker,
                calendar_resolver=resolver,
                policy=policy,
                scope="ACTUAL_ENTRY",
                as_of=first_at,
                settlement_ledger=settlement,
                settled_at=first_at,
                actual_breaker_refresh=refresh_one,
            )
            plan = plan_long(
                request,
                authority.portfolio_state,
                policy,
                portfolio_authority=authority,
            )
        self.assertTrue(plan.eligible)
        self.assertEqual(plan.plan.quantity, signal.planned_shares)

        check = AccountCheck(
            Decimal("500"),
            0,
            0,
            aware_et(signal.publication_session, "10:00"),
            cursor=10,
        )
        parent = "robinhood:order:1"

        def action(
            *, message: str, cursor: int, at, price: Decimal, shares: int
        ):
            event_id, idempotency_key = stable_execution_event_identity(
                message,
                0,
            )
            return risk_module._issue_confirmed_buy_action(
                event_id=event_id,
                idempotency_key=idempotency_key,
                message_id=message,
                action_ordinal=0,
                cursor=cursor,
                symbol=signal.symbol,
                shares=shares,
                price=price,
                at=at,
                message_time=at,
                received_at=at,
                bid=price - signal.tick_size,
                ask=price,
                user_confirmed_stop=signal.recommended_stop,
                source="ROBINHOOD_MANUAL_CONFIRMATION",
                raw_sha256="1" * 64,
                details_sha256="2" * 64,
                parent_order_id=parent,
                fill_group_planned_shares=signal.planned_shares,
            )

        first = action(
            message="message:partial:1",
            cursor=12,
            at=first_at,
            price=Decimal("100"),
            shares=2,
        )
        second = action(
            message="message:partial:2",
            cursor=13,
            at=aware_et(signal.publication_session, "10:14"),
            price=Decimal("99"),
            shares=3,
        )
        window_one = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=12,
            events=(),
            account_check=check,
            terminal_action=first,
        )
        window_two = risk_module._issue_journal_event_window(
            after_cursor=10,
            through_cursor=13,
            events=(first.execution_event,),
            account_check=check,
            terminal_action=second,
        )
        refresh_two = replace(
            refresh_one,
            as_of=second.at,
            through_execution_cursor=13,
            source_digest="c" * 64,
        )

        patches = (
            patch.object(
                ledger_module,
                "is_issued_actual_buy_context",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_journal_event_window",
                return_value=True,
            ),
            patch.object(
                risk_module,
                "is_issued_journal_event_window",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_confirmed_buy_action",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_paired_breaker_state",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_actual_breaker_refresh_authority",
                return_value=True,
            ),
            patch.object(
                ledger_module,
                "is_issued_ledger_signal",
                return_value=True,
            ),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            contexts = (
                ActualBuyContext(
                    check,
                    window_one,
                    first.bid,
                    first.ask,
                    first.user_confirmed_stop,
                    breaker,
                    resolver,
                    portfolio_authority=authority,
                    authorized_signal_id=signal.signal_id,
                    authorized_symbol=signal.symbol,
                    buy_event=first.execution_event,
                    event_id=first.event_id,
                    buy_action=first,
                    plan_decision=plan,
                    actual_breaker_refresh=refresh_one,
                ),
                ActualBuyContext(
                    check,
                    window_two,
                    second.bid,
                    second.ask,
                    second.user_confirmed_stop,
                    breaker,
                    resolver,
                    portfolio_authority=authority,
                    authorized_signal_id=signal.signal_id,
                    authorized_symbol=signal.symbol,
                    buy_event=second.execution_event,
                    event_id=second.event_id,
                    buy_action=second,
                    plan_decision=plan,
                    actual_breaker_refresh=refresh_two,
                ),
            )
            first_decision = pair.record_actual_buy_with_context(
                signal_id=signal.signal_id,
                price=first.price,
                shares=first.shares,
                at=first.at,
                context=contexts[0],
                event_id=first.event_id,
            )
            second_decision = pair.record_actual_buy_with_context(
                signal_id=signal.signal_id,
                price=second.price,
                shares=second.shares,
                at=second.at,
                context=contexts[1],
                event_id=second.event_id,
            )

        self.assertTrue(first_decision.compliant)
        self.assertTrue(second_decision.compliant)
        self.assertEqual(pair.actual.open_positions[0].shares, 5)

    def test_shadow_or_no_capacity_candidate_cannot_be_promoted_to_primary(self) -> None:
        primary = to_scored_candidate(
            candidate_context(evidence=evidence(age_days=11))
        )
        shadow = replace(primary, symbol="MSFT")
        sizing = authorized_plan(LongPlanRequest.from_scored_candidate(primary))
        ready = select_publication_roles(
            (primary, shadow),
            capacity_available=True,
        )
        no_capacity = select_publication_roles(
            (primary,),
            capacity_available=False,
        )

        for decision, rank, reason in (
            (ready, 2, "PUBLICATION_AUTHORITY_UNVERIFIED"),
            (no_capacity, 1, "PUBLICATION_AUTHORITY_UNVERIFIED"),
        ):
            with self.subTest(status=decision.status, rank=rank):
                with self.assertRaisesRegex(
                    RiskBlock,
                    f"^{reason}$",
                ):
                    LedgerSignal.from_publication_decision(
                        decision,
                        rank=rank,
                        plan_decision=sizing,
                    )

        with self.assertRaisesRegex(
            RiskBlock,
            "^PUBLICATION_AUTHORITY_UNVERIFIED$",
        ):
            LedgerSignal.from_publication_decision(
                replace(ready),
                rank=1,
                plan_decision=sizing,
            )


if __name__ == "__main__":
    unittest.main()
