from __future__ import annotations

import pickle
import unittest
from copy import copy, deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock

from stock_monitor.evidence import EvidenceDecision
from stock_monitor.market_calendar import (
    MarketCalendar,
    load_current_market_calendar,
)
from stock_monitor.providers import reference as reference_module
from stock_monitor.providers.reference import InstrumentStatusDecision
from stock_monitor.screening import (
    ScreeningError,
    build_market_session_attestation,
    evaluate_eligibility,
)
from tests.unit import _task5_fixtures as task5_fixtures
from tests.unit._task5_fixtures import (
    RUN_AT,
    SESSION_DATE,
    ET,
    InstrumentStatusFixture,
    PRIMARY_SYMBOL,
    SECONDARY_SYMBOL,
    candidate_context,
    constant_bars,
    descending_bars,
    evidence,
    latest_iex_quote,
    previous_quote,
    session_dates,
    with_candidate_bars,
    with_record,
)


class EligibilityTests(unittest.TestCase):
    def test_attestation_rejects_nonrelease_and_copied_calendar_authority(self) -> None:
        project_root = Path(__file__).resolve().parents[2]
        structural = MarketCalendar.load(
            project_root / "data" / "calendars" / "2026.json",
            as_of=date(2026, 8, 14),
        )
        release = load_current_market_calendar(
            project_root,
            as_of=date(2026, 8, 14),
        )
        for calendar in (structural, replace(release)):
            with self.subTest(calendar=calendar), self.assertRaisesRegex(
                ScreeningError,
                "calendar release authority is unverified",
            ):
                build_market_session_attestation(
                    calendar,
                    session_date=date(2026, 8, 14),
                    as_of=RUN_AT,
                )

    def test_market_session_attestation_is_derived_from_reviewed_calendar(self) -> None:
        calendar = load_current_market_calendar(
            Path(__file__).resolve().parents[2],
            as_of=date(2026, 9, 4),
        )
        as_of = RUN_AT.replace(month=9, day=4)

        attestation = build_market_session_attestation(
            calendar,
            session_date=date(2026, 9, 4),
            as_of=as_of,
        )

        self.assertEqual(attestation.previous_session_date, date(2026, 9, 3))
        self.assertEqual(attestation.hold_sessions[0], date(2026, 9, 4))
        self.assertEqual(attestation.hold_sessions[1], date(2026, 9, 8))
        self.assertNotIn(date(2026, 9, 7), attestation.hold_sessions)
        self.assertEqual(
            tuple(source.role for source in calendar.sources),
            ("primary", "cross_check"),
        )

    def test_attestation_builder_rejects_closed_and_cross_year_windows(self) -> None:
        calendar = MarketCalendar.load(
            Path(__file__).resolve().parents[2] / "data" / "calendars" / "2026.json",
            as_of=date(2026, 9, 4),
        )
        with self.assertRaises(ScreeningError):
            build_market_session_attestation(
                calendar,
                session_date=date(2026, 9, 7),
                as_of=RUN_AT.replace(month=9, day=7),
            )
        december_calendar = replace(
            calendar,
            retrieved_at=date(2026, 12, 24),
            reviewed_at=date(2026, 12, 24),
            sources=tuple(
                replace(
                    source,
                    retrieved_at=date(2026, 12, 24),
                    reviewed_at=date(2026, 12, 24),
                )
                for source in calendar.sources
            ),
        )
        with self.assertRaises(ScreeningError):
            build_market_session_attestation(
                december_calendar,
                session_date=date(2026, 12, 24),
                as_of=RUN_AT.replace(month=12, day=24),
            )

    def test_attestation_builder_rejects_tampered_calendar_schedule_state(self) -> None:
        calendar = MarketCalendar.load(
            Path(__file__).resolve().parents[2] / "data" / "calendars" / "2026.json",
            as_of=date(2026, 9, 7),
        )
        holiday = date(2026, 9, 7)
        tampered = replace(
            calendar,
            _closed_date_set=calendar._closed_date_set - {holiday},
        )

        with self.assertRaises(ScreeningError):
            build_market_session_attestation(
                tampered,
                session_date=holiday,
                as_of=RUN_AT.replace(month=9, day=7),
            )

    def test_exact_price_volume_float_and_spread_boundaries_pass(self) -> None:
        context = candidate_context()
        bars = constant_bars(
            PRIMARY_SYMBOL,
            close=Decimal("100"),
            volume=1_000_000,
        )
        context = with_candidate_bars(context, bars)
        context = replace(
            context,
            previous_session_quote=previous_quote(spread_percent=Decimal("0.0025")),
            initial_listing_date=SESSION_DATE - timedelta(days=91),
        )
        context = with_record(context, free_float=50_000_000)

        decision = evaluate_eligibility(context)

        self.assertTrue(decision.eligible)
        self.assertTrue(decision.live_eligible)
        self.assertEqual(decision.average_dollar_volume, Decimal("100000000"))
        self.assertEqual(decision.median_share_volume, Decimal("1000000"))
        self.assertEqual(decision.spread_percent, Decimal("0.0025"))

    def test_each_numeric_gate_fails_immediately_below_its_boundary(self) -> None:
        base = candidate_context()
        price = with_candidate_bars(
            base,
            constant_bars(
                PRIMARY_SYMBOL,
                close=Decimal("9.99"),
                volume=20_000_000,
            ),
        )
        adv = with_candidate_bars(
            base,
            constant_bars(
                PRIMARY_SYMBOL,
                close=Decimal("99.99"),
                volume=1_000_000,
            ),
        )
        share_volume = with_candidate_bars(
            base,
            constant_bars(
                PRIMARY_SYMBOL,
                close=Decimal("200"),
                volume=999_999,
            ),
        )
        float_context = with_record(base, free_float=49_999_999)
        spread = replace(
            base,
            previous_session_quote=previous_quote(
                spread_percent=Decimal("0.002501")
            ),
        )
        cases = (
            (price, "PRICE_BELOW_MINIMUM"),
            (adv, "AVERAGE_DOLLAR_VOLUME_BELOW_MINIMUM"),
            (share_volume, "MEDIAN_SHARE_VOLUME_BELOW_MINIMUM"),
            (float_context, "FREE_FLOAT_BELOW_MINIMUM"),
            (spread, "SPREAD_TOO_WIDE"),
        )
        for context, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_eligibility(context)
                self.assertFalse(decision.eligible)
                self.assertIn(reason, decision.reason_codes)

    def test_ipo_requires_91_days_and_explicit_verified_status(self) -> None:
        passing = replace(
            candidate_context(),
            initial_listing_date=SESSION_DATE - timedelta(days=91),
        )
        recent = replace(
            passing,
            initial_listing_date=SESSION_DATE - timedelta(days=90),
        )
        unverified = replace(
            passing,
            initial_listing_date=None,
            listing_date_status="UNVERIFIED",
        )

        self.assertTrue(evaluate_eligibility(passing).eligible)
        self.assertIn("IPO_TOO_RECENT", evaluate_eligibility(recent).reason_codes)
        unknown = evaluate_eligibility(unverified)
        self.assertEqual(unknown.status, "DATA_UNAVAILABLE")
        self.assertIn("IPO_DATE_UNVERIFIED", unknown.reason_codes)

    def test_etf_is_exempt_from_corporate_free_float(self) -> None:
        context = with_record(
            candidate_context(),
            product_type="etf",
            sector_etf=None,
            free_float=None,
        )
        self.assertTrue(evaluate_eligibility(context).eligible)

    def test_halt_status_must_be_exactly_clear(self) -> None:
        base = candidate_context()
        unknown = replace(
            base,
            instrument_status=InstrumentStatusFixture(halt_status="UNKNOWN"),
        )
        halted = replace(
            base,
            instrument_status=InstrumentStatusFixture(
                halt_status="HALTED", block_reason="SYMBOL_HALTED"
            ),
        )
        self.assertTrue(evaluate_eligibility(base).eligible)
        self.assertEqual(evaluate_eligibility(unknown).status, "DATA_UNAVAILABLE")
        self.assertIn(
            "HALT_STATUS_UNKNOWN", evaluate_eligibility(unknown).reason_codes
        )
        self.assertIn("SYMBOL_HALTED", evaluate_eligibility(halted).reason_codes)

    def test_instrument_status_requires_current_subject_and_nonempty_provenance(self) -> None:
        base = candidate_context()
        stale = replace(
            base,
            instrument_status=replace(
                base.instrument_status,
                as_of=RUN_AT - timedelta(minutes=1),
                valid_until=None,
            ),
        )
        wrong_subject = replace(
            base,
            instrument_status=replace(
                base.instrument_status,
                symbol=SECONDARY_SYMBOL,
            ),
        )
        missing_provenance = replace(
            base,
            instrument_status=replace(
                base.instrument_status, source_observation_ids=()
            ),
        )
        explicitly_valid = replace(
            base,
            instrument_status=replace(
                base.instrument_status,
                as_of=RUN_AT - timedelta(minutes=1),
                valid_until=RUN_AT,
            ),
        )

        self.assertIn(
            "HALT_STATUS_STALE", evaluate_eligibility(stale).reason_codes
        )
        self.assertIn(
            "HALT_STATUS_SYMBOL_MISMATCH",
            evaluate_eligibility(wrong_subject).reason_codes,
        )
        self.assertIn(
            "HALT_STATUS_PROVENANCE_MISSING",
            evaluate_eligibility(missing_provenance).reason_codes,
        )
        explicitly_valid_decision = evaluate_eligibility(explicitly_valid)
        self.assertEqual(explicitly_valid_decision.status, "DATA_UNAVAILABLE")
        self.assertIn(
            "INSTRUMENT_STATUS_UNREVIEWED",
            explicitly_valid_decision.reason_codes,
        )

    def test_only_sealed_classifier_output_can_authorize_clear_status(self) -> None:
        base = candidate_context()
        reviewed = base.instrument_status
        self.assertTrue(
            reference_module.is_reviewed_instrument_status_decision(reviewed)
        )
        self.assertTrue(evaluate_eligibility(base).eligible)
        direct = InstrumentStatusDecision(
            symbol=reviewed.symbol,
            halt_status=reviewed.halt_status,
            as_of=reviewed.as_of,
            valid_until=reviewed.valid_until,
            source_observation_ids=reviewed.source_observation_ids,
            block_reason=reviewed.block_reason,
        )
        for case, forged in (
            ("direct", direct),
            ("replace", replace(reviewed)),
            ("copy", copy(reviewed)),
            ("deepcopy", deepcopy(reviewed)),
            ("pickle", pickle.loads(pickle.dumps(reviewed))),
        ):
            with self.subTest(case=case):
                decision = evaluate_eligibility(
                    replace(base, instrument_status=forged)
                )
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn(
                    "INSTRUMENT_STATUS_UNREVIEWED",
                    decision.reason_codes,
                )

        tampered_context = candidate_context()
        tampered = tampered_context.instrument_status
        object.__setattr__(
            tampered,
            "valid_until",
            RUN_AT + timedelta(minutes=4),
        )
        object.__setattr__(
            tampered,
            "_decision_digest",
            reference_module._instrument_decision_fingerprint(tampered),
        )
        tampered_decision = evaluate_eligibility(tampered_context)
        self.assertEqual(tampered_decision.status, "DATA_UNAVAILABLE")
        self.assertIn(
            "INSTRUMENT_STATUS_UNREVIEWED",
            tampered_decision.reason_codes,
        )

    def test_clear_status_requires_an_unexpired_explicit_validity_window(self) -> None:
        base = candidate_context()
        missing_validity = replace(
            base,
            instrument_status=replace(base.instrument_status, valid_until=None),
        )
        expired = replace(
            base,
            instrument_status=replace(
                base.instrument_status,
                valid_until=RUN_AT - timedelta(microseconds=1),
            ),
        )

        for context in (missing_validity, expired):
            with self.subTest(valid_until=context.instrument_status.valid_until):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn("HALT_STATUS_STALE", decision.reason_codes)

    def test_direct_task4_decision_without_review_authority_fails_closed(self) -> None:
        base = candidate_context()
        status = InstrumentStatusDecision(
            symbol=PRIMARY_SYMBOL,
            halt_status="CLEAR",
            as_of=RUN_AT,
            valid_until=RUN_AT + timedelta(minutes=5),
            source_observation_ids=("halt-primary", "halt-cross-check"),
            block_reason=None,
        )
        evidence_decision = EvidenceDecision(
            subject_kind="STOCK",
            symbol=PRIMARY_SYMBOL,
            issuer_cik="0000000000",
            as_of=RUN_AT,
            qualifying_records=base.evidence.qualifying_records,
            adverse_tags=(),
            ambiguities=(),
            conflicts=(),
            binary_events=(),
            etf_actions=(),
            binary_event_coverage="CONFIRMED_CLEAR",
            etf_action_coverage="NOT_APPLICABLE",
            health="HEALTHY",
            retrieved_at=RUN_AT - timedelta(hours=1),
            source_observation_ids=(
                "evidence-cross-check",
                "evidence-fact",
                "evidence-primary",
            ),
            block_reason=None,
        )

        decision = evaluate_eligibility(
            replace(base, instrument_status=status, evidence=evidence_decision)
        )

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("INSTRUMENT_STATUS_UNREVIEWED", decision.reason_codes)
        self.assertIn("EVIDENCE_DECISION_UNREVIEWED", decision.reason_codes)

    def test_tampered_reviewed_evidence_decision_fails_closed(self) -> None:
        base = candidate_context()
        reviewed = evidence(
            age_days=4,
            retrieved_at=RUN_AT - timedelta(minutes=15),
        )
        tampered = replace(reviewed, registry_id="caller-overridden-registry")

        decision = evaluate_eligibility(replace(base, evidence=tampered))

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("EVIDENCE_DECISION_UNREVIEWED", decision.reason_codes)

    def test_product_flags_otc_and_rumor_are_hard_blocks(self) -> None:
        base = candidate_context()
        cases = (
            (with_record(base, leveraged=True), "LEVERAGED_OR_INVERSE_PRODUCT"),
            (with_record(base, inverse=True), "LEVERAGED_OR_INVERSE_PRODUCT"),
            (with_record(base, listing_venue="OTC"), "OTC_SECURITY"),
            (replace(base, rumor_dependent=True), "RUMOR_DEPENDENT"),
        )
        for context, reason in cases:
            with self.subTest(reason=reason):
                self.assertIn(reason, evaluate_eligibility(context).reason_codes)

    def test_leveraged_and_inverse_flags_must_be_exact_false_booleans(self) -> None:
        base = candidate_context()
        cases = (
            with_record(base, leveraged=1),
            with_record(base, leveraged=0),
            with_record(base, inverse=1),
            with_record(base, inverse=0),
        )

        for context in cases:
            with self.subTest(
                leveraged=context.record.leveraged,
                inverse=context.record.inverse,
            ):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn("PRODUCT_FLAGS_UNVERIFIED", decision.reason_codes)

    def test_iex_300_seconds_and_evidence_24_hours_are_inclusive(self) -> None:
        base = candidate_context()
        passing = replace(
            base,
            latest_iex_quote=latest_iex_quote(age_seconds=300),
            evidence=evidence(retrieved_at=RUN_AT - timedelta(hours=24)),
        )
        stale_quote = replace(
            passing,
            latest_iex_quote=latest_iex_quote(age_seconds=301),
        )
        stale_evidence = replace(
            passing,
            evidence=evidence(retrieved_at=RUN_AT - timedelta(seconds=86_401)),
        )

        self.assertTrue(evaluate_eligibility(passing).eligible)
        self.assertIn("IEX_QUOTE_STALE", evaluate_eligibility(stale_quote).reason_codes)
        self.assertIn(
            "EVIDENCE_SOURCE_STALE",
            evaluate_eligibility(stale_evidence).reason_codes,
        )

    def test_operational_clock_accepts_post_cutoff_iex_and_halt_facts(self) -> None:
        operational_as_of = RUN_AT + timedelta(minutes=7)
        with mock.patch.object(task5_fixtures, "RUN_AT", operational_as_of):
            status = task5_fixtures.reviewed_instrument_status(
                PRIMARY_SYMBOL,
                "NASDAQ",
            )
        context = replace(
            candidate_context(),
            operational_as_of=operational_as_of,
            latest_iex_quote=replace(
                latest_iex_quote(),
                timestamp=operational_as_of - timedelta(seconds=30),
            ),
            instrument_status=status,
        )

        decision = evaluate_eligibility(context)

        self.assertGreater(context.latest_iex_quote.timestamp, context.as_of)
        self.assertGreater(context.instrument_status.as_of, context.as_of)
        self.assertTrue(decision.eligible)
        self.assertNotIn("IEX_QUOTE_FROM_FUTURE", decision.reason_codes)
        self.assertNotIn("HALT_STATUS_STALE", decision.reason_codes)

    def test_operational_clock_rejects_stale_and_future_facts(self) -> None:
        operational_as_of = RUN_AT + timedelta(minutes=7)
        with mock.patch.object(task5_fixtures, "RUN_AT", operational_as_of):
            current_status = task5_fixtures.reviewed_instrument_status(
                PRIMARY_SYMBOL,
                "NASDAQ",
            )
        with mock.patch.object(
            task5_fixtures,
            "RUN_AT",
            operational_as_of + timedelta(seconds=1),
        ):
            future_status = task5_fixtures.reviewed_instrument_status(
                PRIMARY_SYMBOL,
                "NASDAQ",
            )
        economic_context = candidate_context()
        base = replace(
            economic_context,
            operational_as_of=operational_as_of,
            latest_iex_quote=replace(
                latest_iex_quote(),
                timestamp=operational_as_of - timedelta(seconds=30),
            ),
            instrument_status=current_status,
        )
        cases = (
            (
                replace(
                    base,
                    latest_iex_quote=replace(
                        latest_iex_quote(),
                        timestamp=operational_as_of - timedelta(seconds=301),
                    ),
                ),
                "IEX_QUOTE_STALE",
            ),
            (
                replace(
                    base,
                    latest_iex_quote=replace(
                        latest_iex_quote(),
                        timestamp=operational_as_of + timedelta(microseconds=1),
                    ),
                ),
                "IEX_QUOTE_FROM_FUTURE",
            ),
            (
                replace(
                    base,
                    instrument_status=economic_context.instrument_status,
                ),
                "HALT_STATUS_STALE",
            ),
            (
                replace(base, instrument_status=future_status),
                "HALT_STATUS_STALE",
            ),
        )

        for context, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn(reason, decision.reason_codes)

    def test_explicit_operational_clock_must_be_aware_and_not_precede_cutoff(
        self,
    ) -> None:
        earlier = RUN_AT - timedelta(minutes=1)
        with mock.patch.object(task5_fixtures, "RUN_AT", earlier):
            earlier_status = task5_fixtures.reviewed_instrument_status(
                PRIMARY_SYMBOL,
                "NASDAQ",
            )
        economic_context = candidate_context()
        cases = (
            replace(
                economic_context,
                operational_as_of=earlier,
                latest_iex_quote=replace(
                    latest_iex_quote(),
                    timestamp=earlier - timedelta(seconds=30),
                ),
                instrument_status=earlier_status,
            ),
            replace(
                economic_context,
                operational_as_of=RUN_AT.replace(tzinfo=None),
            ),
        )

        for context in cases:
            with self.subTest(operational_as_of=context.operational_as_of):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn(
                    "OPERATIONAL_AS_OF_INVALID",
                    decision.reason_codes,
                )

    def test_stock_binary_and_etf_action_cover_the_inclusive_ten_session_hold(self) -> None:
        base = candidate_context()
        first, last = base.hold_sessions[0], base.hold_sessions[-1]
        stock_first = replace(
            base,
            evidence=evidence(
                binary_events=((first, "earnings"),),
                binary_event_coverage="OVERLAP",
            ),
        )
        stock_last = replace(
            base,
            evidence=evidence(
                binary_events=((last, "earnings"),),
                binary_event_coverage="OVERLAP",
            ),
        )
        stock_after = replace(
            base,
            evidence=evidence(binary_events=((last + timedelta(days=1), "earnings"),)),
        )
        etf = with_record(base, product_type="etf", sector_etf=None, free_float=None)
        etf = replace(
            etf,
            evidence=evidence(
                subject_kind="ETF",
                issuer_cik=None,
                event_type="fund sponsor notice",
                etf_actions=((last, "fund reorganization"),),
                binary_event_coverage="NOT_APPLICABLE",
                etf_action_coverage="OVERLAP",
            ),
        )

        self.assertIn(
            "BINARY_EVENT_DURING_HOLD",
            evaluate_eligibility(stock_first).reason_codes,
        )
        self.assertIn(
            "BINARY_EVENT_DURING_HOLD",
            evaluate_eligibility(stock_last).reason_codes,
        )
        self.assertTrue(evaluate_eligibility(stock_after).eligible)
        self.assertIn(
            "ETF_ACTION_DURING_HOLD", evaluate_eligibility(etf).reason_codes
        )

    def test_product_specific_event_coverage_unknown_or_conflict_is_data_unavailable(self) -> None:
        stock = replace(
            candidate_context(),
            evidence=evidence(binary_event_coverage="UNKNOWN"),
        )
        etf = with_record(
            candidate_context(), product_type="etf", sector_etf=None, free_float=None
        )
        etf = replace(
            etf,
            evidence=evidence(
                subject_kind="ETF",
                issuer_cik=None,
                event_type="fund sponsor notice",
                binary_event_coverage="NOT_APPLICABLE",
                etf_action_coverage="CONFLICT",
            ),
        )

        stock_decision = evaluate_eligibility(stock)
        etf_decision = evaluate_eligibility(etf)

        self.assertEqual(stock_decision.status, "DATA_UNAVAILABLE")
        self.assertIn("BINARY_EVENT_COVERAGE_UNKNOWN", stock_decision.reason_codes)
        self.assertEqual(etf_decision.status, "DATA_UNAVAILABLE")
        self.assertIn("ETF_ACTION_COVERAGE_CONFLICT", etf_decision.reason_codes)

    def test_evidence_requires_exact_subject_as_of_issuer_and_provenance(self) -> None:
        base = candidate_context()
        cases = (
            (
                replace(
                    base,
                    evidence=replace(base.evidence, symbol=SECONDARY_SYMBOL),
                ),
                "EVIDENCE_SUBJECT_MISMATCH",
            ),
            (
                replace(base, evidence=replace(base.evidence, issuer_cik="9999999999")),
                "EVIDENCE_ISSUER_MISMATCH",
            ),
            (
                replace(
                    base,
                    evidence=replace(
                        base.evidence, as_of=RUN_AT - timedelta(seconds=1)
                    ),
                ),
                "EVIDENCE_AS_OF_MISMATCH",
            ),
            (
                replace(
                    base,
                    evidence=replace(base.evidence, source_observation_ids=()),
                ),
                "EVIDENCE_PROVENANCE_MISSING",
            ),
        )
        for context, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn(reason, decision.reason_codes)

    def test_evidence_subject_kind_and_raw_adverse_ambiguity_conflict_fail_closed(self) -> None:
        base = candidate_context()
        cases = (
            (
                replace(
                    base,
                    evidence=evidence(
                        subject_kind="ETF",
                        issuer_cik=None,
                        event_type="fund sponsor notice",
                        binary_event_coverage="NOT_APPLICABLE",
                        etf_action_coverage="CONFIRMED_CLEAR",
                    ),
                ),
                "DATA_UNAVAILABLE",
                "EVIDENCE_SUBJECT_KIND_MISMATCH",
            ),
            (
                replace(
                    base,
                    evidence=evidence(adverse_tags=("lowered guidance",)),
                ),
                "INELIGIBLE",
                "ADVERSE_EVENT",
            ),
            (
                replace(
                    base,
                    evidence=evidence(ambiguities=("ambiguous-1",)),
                ),
                "DATA_UNAVAILABLE",
                "AMBIGUOUS_EVIDENCE_CLASSIFICATION",
            ),
            (
                replace(
                    base,
                    evidence=evidence(conflicts=("conflict-1",)),
                ),
                "DATA_UNAVAILABLE",
                "EVIDENCE_SOURCE_CONFLICT",
            ),
        )
        for context, status, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_eligibility(context)
                self.assertEqual(decision.status, status)
                self.assertIn(reason, decision.reason_codes)

    def test_dual_index_downtrend_is_paper_only_not_a_cohort_exclusion(self) -> None:
        base = candidate_context()
        mapping = dict(base.bars_by_symbol)
        mapping["SPY"] = descending_bars("SPY")
        mapping["QQQ"] = descending_bars("QQQ")
        context = replace(base, bars_by_symbol=mapping)

        decision = evaluate_eligibility(context)

        self.assertTrue(decision.eligible)
        self.assertFalse(decision.live_eligible)
        self.assertTrue(decision.paper_only)
        self.assertIn("DUAL_INDEX_DOWNTREND_PAPER_ONLY", decision.reason_codes)

    def test_all_gate_failures_are_returned_without_short_circuiting(self) -> None:
        context = with_candidate_bars(
            candidate_context(),
            constant_bars(PRIMARY_SYMBOL, close=Decimal("9"), volume=1),
        )
        context = with_record(context, free_float=None)
        context = replace(
            context,
            rumor_dependent=True,
            listing_date_status="UNVERIFIED",
            initial_listing_date=None,
        )

        reasons = evaluate_eligibility(context).reason_codes

        self.assertIn("PRICE_BELOW_MINIMUM", reasons)
        self.assertIn("AVERAGE_DOLLAR_VOLUME_BELOW_MINIMUM", reasons)
        self.assertIn("MEDIAN_SHARE_VOLUME_BELOW_MINIMUM", reasons)
        self.assertIn("FREE_FLOAT_UNVERIFIED", reasons)
        self.assertIn("IPO_DATE_UNVERIFIED", reasons)
        self.assertIn("RUMOR_DEPENDENT", reasons)

    def test_exactly_60_aligned_split_adjusted_completed_bars_are_required(self) -> None:
        base = candidate_context()
        mapping = dict(base.bars_by_symbol)
        mapping[PRIMARY_SYMBOL] = mapping[PRIMARY_SYMBOL][:-1]
        missing = replace(base, bars_by_symbol=mapping)
        mapping = dict(base.bars_by_symbol)
        mapping[PRIMARY_SYMBOL] = tuple(
            replace(bar, adjustment="raw") if index == 59 else bar
            for index, bar in enumerate(mapping[PRIMARY_SYMBOL])
        )
        raw = replace(base, bars_by_symbol=mapping)

        self.assertEqual(evaluate_eligibility(missing).status, "DATA_UNAVAILABLE")
        self.assertIn(
            "BAR_HISTORY_INCOMPLETE", evaluate_eligibility(missing).reason_codes
        )
        self.assertIn("BARS_NOT_SPLIT_ADJUSTED", evaluate_eligibility(raw).reason_codes)

    def test_bar_sessions_must_be_consecutive_verified_market_sessions(self) -> None:
        base = candidate_context()
        sample = base.bars_by_symbol[base.record.symbol]
        friday_index = next(
            index
            for index, (left, right) in enumerate(zip(sample, sample[1:]))
            if left.timestamp.astimezone(ET).weekday() == 4
            and right.timestamp.astimezone(ET).weekday() == 0
        )
        sunday = sample[friday_index].timestamp.astimezone(ET).date() + timedelta(
            days=2
        )
        mapping = {
            symbol: tuple(
                replace(
                    bar,
                    timestamp=datetime.combine(sunday, time(16), ET).astimezone(UTC),
                )
                if index == friday_index
                else bar
                for index, bar in enumerate(bars)
            )
            for symbol, bars in base.bars_by_symbol.items()
        }

        decision = evaluate_eligibility(replace(base, bars_by_symbol=mapping))

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("BAR_SESSION_CALENDAR_MISMATCH", decision.reason_codes)

    def test_every_scoring_bar_must_come_from_the_sip_feed(self) -> None:
        base = candidate_context()
        mapping = dict(base.bars_by_symbol)
        mapping[base.record.symbol] = tuple(
            replace(bar, feed="iex") if index == 30 else bar
            for index, bar in enumerate(mapping[base.record.symbol])
        )

        decision = evaluate_eligibility(replace(base, bars_by_symbol=mapping))

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("BARS_NOT_SIP", decision.reason_codes)

    def test_bar_tail_must_equal_calendar_selected_latest_completed_session(self) -> None:
        context = replace(
            candidate_context(), previous_session_date=date(2026, 8, 12)
        )

        decision = evaluate_eligibility(context)

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn(
            "BAR_HISTORY_NOT_LATEST_COMPLETED_SESSION", decision.reason_codes
        )

    def test_evaluation_rederives_sessions_and_rejects_forged_sunday_tail(self) -> None:
        base = candidate_context()
        forged_dates = (*session_dates(59, end=date(2026, 8, 7)), date(2026, 8, 9))
        mapping = {
            symbol: tuple(
                replace(
                    bar,
                    timestamp=datetime.combine(day, time(16), ET).astimezone(UTC),
                )
                for bar, day in zip(bars, forged_dates)
            )
            for symbol, bars in base.bars_by_symbol.items()
        }
        forged = replace(
            base,
            bars_by_symbol=mapping,
            previous_session_date=date(2026, 8, 9),
            previous_session_quote=replace(
                base.previous_session_quote,
                timestamp=datetime(2026, 8, 9, 15, 58, tzinfo=ET),
            ),
            session_attestation=replace(
                base.session_attestation,
                previous_session_date=date(2026, 8, 9),
            ),
        )

        decision = evaluate_eligibility(forged)

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("CALENDAR_DERIVATION_MISMATCH", decision.reason_codes)

    def test_hold_window_requires_matching_verified_calendar_attestation(self) -> None:
        base = candidate_context()
        weekend_sessions = list(base.hold_sessions)
        weekend_sessions[1] = date(2026, 8, 15)
        weekend_sessions.sort()
        weekends = replace(
            base,
            hold_sessions=tuple(weekend_sessions),
            session_attestation=replace(
                base.session_attestation,
                hold_sessions=tuple(weekend_sessions),
            ),
        )
        mismatched = replace(
            base,
            session_attestation=replace(
                base.session_attestation,
                previous_session_date=date(2026, 8, 12),
            ),
        )

        weekend_decision = evaluate_eligibility(weekends)
        mismatch_decision = evaluate_eligibility(mismatched)

        self.assertEqual(weekend_decision.status, "DATA_UNAVAILABLE")
        self.assertIn("HOLD_WINDOW_NOT_MARKET_SESSIONS", weekend_decision.reason_codes)
        self.assertEqual(mismatch_decision.status, "DATA_UNAVAILABLE")
        self.assertIn("CALENDAR_ATTESTATION_MISMATCH", mismatch_decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
