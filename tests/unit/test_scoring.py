from __future__ import annotations

import unittest
from copy import copy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import MappingProxyType

from stock_monitor import screening as screening_module
from stock_monitor.screening import (
    RelativeStrengthCohort,
    build_base_eligible_cohort,
    midrank_percentile,
    ScreeningError,
    score_candidate,
)
from tests.unit._task5_fixtures import (
    PRIMARY_SYMBOL,
    RUN_AT,
    SECONDARY_SYMBOL,
    TEST_UNIVERSE,
    candidate_context,
    constant_bars,
    evidence,
    universe_candidate_contexts,
    with_candidate_bars,
)


class ScoringTests(unittest.TestCase):
    def test_midrank_percentile_is_deterministic_for_ties_and_one_symbol(self) -> None:
        self.assertEqual(
            midrank_percentile(
                Decimal("2"),
                (Decimal("1"), Decimal("2"), Decimal("2"), Decimal("4")),
            ),
            Decimal("50"),
        )
        self.assertEqual(
            midrank_percentile(Decimal("7"), (Decimal("7"),)), Decimal("50")
        )

    def test_empty_midrank_cohort_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            midrank_percentile(Decimal("1"), ())

    def test_raw_primary_evidence_changes_score_79_to_publishable_84(self) -> None:
        low = candidate_context(evidence=evidence(age_days=31))
        high = candidate_context(evidence=evidence(age_days=11))

        low_score = score_candidate(low)
        high_score = score_candidate(high)

        self.assertEqual(low_score.total, 79)
        self.assertFalse(low_score.publishable)
        self.assertEqual(high_score.total, 84)
        self.assertTrue(high_score.publishable)
        self.assertEqual(low_score.catalyst_context, 0)
        self.assertEqual(high_score.catalyst_context, 5)

    def test_catalyst_day_boundaries_are_10_11_30_and_31(self) -> None:
        expected = {10: 10, 11: 5, 30: 5, 31: 0}
        for days, points in expected.items():
            with self.subTest(days=days):
                context = candidate_context(evidence=evidence(age_days=days))
                self.assertEqual(score_candidate(context).catalyst_context, points)

    def test_category_values_never_exceed_locked_caps(self) -> None:
        score = score_candidate(candidate_context(evidence=evidence(age_days=1)))
        self.assertLessEqual(score.trend_and_regime, 25)
        self.assertLessEqual(score.relative_strength, 20)
        self.assertLessEqual(score.setup_quality, 20)
        self.assertLessEqual(score.volume_confirmation, 15)
        self.assertLessEqual(score.catalyst_context, 10)
        self.assertLessEqual(score.liquidity_execution, 10)
        self.assertLessEqual(score.total, 100)

    def test_missing_relative_strength_cohort_is_data_unavailable(self) -> None:
        context = replace(candidate_context(), relative_strength_cohort=None)
        score = score_candidate(context)
        self.assertEqual(score.status, "DATA_UNAVAILABLE")
        self.assertFalse(score.publishable)
        self.assertIn("RELATIVE_STRENGTH_COHORT_INCOMPLETE", score.reason_codes)

    def test_two_pass_cohort_excludes_policy_ineligible_symbols_before_midrank(self) -> None:
        eligible, second, *remaining = universe_candidate_contexts()
        ineligible = with_candidate_bars(
            second,
            constant_bars(
                SECONDARY_SYMBOL,
                close=Decimal("9"),
                volume=20_000_000,
            ),
        )

        cohort = build_base_eligible_cohort(
            (eligible, ineligible, *remaining), universe=TEST_UNIVERSE
        )

        self.assertEqual(cohort.status, "READY")
        symbols = tuple(item.record.symbol for item in cohort.contexts)
        self.assertIn(PRIMARY_SYMBOL, symbols)
        self.assertNotIn(SECONDARY_SYMBOL, symbols)
        self.assertEqual(len(symbols), len(TEST_UNIVERSE.records) - 1)

    def test_natural_empty_cohort_is_no_trade_but_missing_symbol_is_data_unavailable(self) -> None:
        complete = universe_candidate_contexts()
        ineligible = tuple(
            with_candidate_bars(
                context,
                constant_bars(
                    context.record.symbol,
                    close=Decimal("9"),
                    volume=20_000_000,
                ),
            )
            for context in complete
        )
        natural = build_base_eligible_cohort(
            ineligible, universe=TEST_UNIVERSE
        )
        incomplete = build_base_eligible_cohort(
            complete[:-1], universe=TEST_UNIVERSE
        )

        self.assertEqual(natural.status, "NO_TRADE")
        self.assertEqual(incomplete.status, "DATA_UNAVAILABLE")
        self.assertIn("COHORT_FETCH_INCOMPLETE", incomplete.reason_codes)

    def test_cohort_requires_exact_universe_membership_and_record_provenance(self) -> None:
        complete = universe_candidate_contexts()
        wrong_record = replace(
            complete[0],
            record=replace(complete[0].record, free_float=50_000_001),
        )

        missing = build_base_eligible_cohort(
            complete[:-1], universe=TEST_UNIVERSE
        )
        wrong_provenance = build_base_eligible_cohort(
            (wrong_record, *complete[1:]), universe=TEST_UNIVERSE
        )

        self.assertEqual(missing.status, "DATA_UNAVAILABLE")
        self.assertIn("COHORT_FETCH_INCOMPLETE", missing.reason_codes)
        self.assertEqual(wrong_provenance.status, "DATA_UNAVAILABLE")
        self.assertIn(
            "COHORT_UNIVERSE_PROVENANCE_INVALID",
            wrong_provenance.reason_codes,
        )

    def test_replaced_universe_subset_cannot_authorize_a_cohort(self) -> None:
        context = replace(candidate_context(), relative_strength_cohort=None)
        forged = replace(
            TEST_UNIVERSE,
            records=(context.record,),
            by_symbol=MappingProxyType({context.record.symbol: context.record}),
            sector_mapping=MappingProxyType({context.record.symbol: "XLK"}),
        )

        decision = build_base_eligible_cohort((context,), universe=forged)

        self.assertEqual(decision.status, "DATA_UNAVAILABLE")
        self.assertIn("COHORT_UNIVERSE_UNVERIFIED", decision.reason_codes)

    def test_cohort_builder_does_not_accept_an_implicit_expected_universe(self) -> None:
        context = replace(candidate_context(), relative_strength_cohort=None)

        with self.assertRaises(TypeError):
            build_base_eligible_cohort((context,))

    def test_caller_forged_lower_returns_cannot_promote_79_to_84(self) -> None:
        context = candidate_context(evidence=evidence(age_days=31))
        baseline = score_candidate(context)
        five_value = baseline.five_session_relative_strength
        twenty_value = baseline.twenty_session_relative_strength
        self.assertIsNotNone(five_value)
        self.assertIsNotNone(twenty_value)
        forged = RelativeStrengthCohort(
            five_session_values=(five_value - Decimal("1"),),
            twenty_session_values=(twenty_value - Decimal("1"),),
        )

        promoted = score_candidate(
            replace(context, relative_strength_cohort=forged)
        )

        self.assertEqual(baseline.total, 79)
        self.assertFalse(promoted.publishable)
        self.assertEqual(promoted.status, "DATA_UNAVAILABLE")
        self.assertIn("RELATIVE_STRENGTH_COHORT_UNTRUSTED", promoted.reason_codes)

    def test_copied_authority_and_recomputed_digest_cannot_reseal_cohort(self) -> None:
        baseline_context = candidate_context(evidence=evidence(age_days=31))
        promoted_context = replace(
            baseline_context,
            evidence=evidence(age_days=11),
        )
        cohort = baseline_context.relative_strength_cohort
        self.assertIsNotNone(cohort)
        observations = tuple(
            replace(
                observation,
                context_fingerprint=screening_module._relative_input_fingerprint(
                    promoted_context
                ),
            )
            if observation.symbol == PRIMARY_SYMBOL
            else observation
            for observation in cohort.observations
        )
        forged = replace(cohort, observations=observations)
        object.__setattr__(forged, "_authority", cohort._authority)
        object.__setattr__(
            forged,
            "_integrity",
            screening_module._cohort_integrity(forged),
        )

        promoted = score_candidate(
            replace(
                promoted_context,
                relative_strength_cohort=forged,
            )
        )

        self.assertEqual(score_candidate(baseline_context).total, 79)
        self.assertEqual(promoted.status, "DATA_UNAVAILABLE")
        self.assertFalse(promoted.publishable)
        self.assertIn("RELATIVE_STRENGTH_COHORT_UNTRUSTED", promoted.reason_codes)

    def test_replaced_or_copied_builder_cohort_loses_issuance_identity(self) -> None:
        context = candidate_context()
        cohort = context.relative_strength_cohort
        self.assertIsNotNone(cohort)

        for case, forged in (
            ("replace", replace(cohort)),
            ("copy", copy(cohort)),
        ):
            with self.subTest(case=case):
                score = score_candidate(
                    replace(context, relative_strength_cohort=forged)
                )
                self.assertEqual(score.status, "DATA_UNAVAILABLE")
                self.assertFalse(score.publishable)
                self.assertIn(
                    "RELATIVE_STRENGTH_COHORT_UNTRUSTED",
                    score.reason_codes,
                )

    def test_cohort_private_seal_fields_are_not_public_constructor_inputs(self) -> None:
        context = candidate_context()
        cohort = context.relative_strength_cohort
        self.assertIsNotNone(cohort)

        with self.assertRaises(TypeError):
            RelativeStrengthCohort(
                five_session_values=cohort.five_session_values,
                twenty_session_values=cohort.twenty_session_values,
                observations=cohort.observations,
                expected_symbols=cohort.expected_symbols,
                universe_checksum=cohort.universe_checksum,
                universe_effective_date=cohort.universe_effective_date,
                universe_reviewed_at=cohort.universe_reviewed_at,
                universe_review_by=cohort.universe_review_by,
                _integrity=cohort._integrity,
                _authority=cohort._authority,
            )

    def test_cohort_rejects_non_exact_tuple_and_repr_trick_inputs(self) -> None:
        class TupleSubclass(tuple):
            pass

        class ReprTrap(str):
            def __repr__(self) -> str:
                raise AssertionError("cohort validation must not call caller repr")

        cases = (
            {
                "five_session_values": TupleSubclass((Decimal("1"),)),
                "twenty_session_values": (Decimal("1"),),
            },
            {
                "five_session_values": (Decimal("1"),),
                "twenty_session_values": (Decimal("1"),),
                "expected_symbols": (ReprTrap(PRIMARY_SYMBOL),),
            },
        )
        for values in cases:
            with self.subTest(values=tuple(values)):
                with self.assertRaises(TypeError):
                    RelativeStrengthCohort(**values)

    def test_cohort_is_bound_to_the_exact_bar_snapshot_used_by_the_builder(self) -> None:
        context = candidate_context()
        bars = context.bars_by_symbol[context.record.symbol]
        mutated_bars = (replace(bars[0], volume=bars[0].volume + 1),) + bars[1:]

        score = score_candidate(with_candidate_bars(context, mutated_bars))

        self.assertEqual(score.status, "DATA_UNAVAILABLE")
        self.assertFalse(score.publishable)
        self.assertIn("RELATIVE_STRENGTH_COHORT_UNTRUSTED", score.reason_codes)

    def test_cohort_is_bound_to_full_record_and_quote_snapshot(self) -> None:
        context = candidate_context()
        cases = (
            (
                "tick",
                replace(
                    context,
                    record=replace(context.record, tick_size=Decimal("1")),
                ),
            ),
            (
                "quote",
                replace(
                    context,
                    previous_session_quote=replace(
                        context.previous_session_quote,
                        bid=Decimal("19.99"),
                        ask=Decimal("20.01"),
                    ),
                ),
            ),
        )

        for case, mutated in cases:
            with self.subTest(case=case):
                score = score_candidate(mutated)
                self.assertEqual(score.status, "DATA_UNAVAILABLE")
                self.assertFalse(score.publishable)
                self.assertIn(
                    "RELATIVE_STRENGTH_COHORT_UNTRUSTED",
                    score.reason_codes,
                )

    def test_future_or_stale_evidence_never_earns_catalyst_points(self) -> None:
        stale = replace(
            candidate_context(),
            evidence=evidence(
                age_days=1,
                retrieved_at=RUN_AT - timedelta(hours=25),
            ),
        )
        score = score_candidate(stale)
        self.assertFalse(score.publishable)
        self.assertEqual(score.catalyst_context, 0)
        self.assertEqual(score.status, "DATA_UNAVAILABLE")

    def test_qualifying_fact_must_match_subject_and_decision_provenance(self) -> None:
        base = candidate_context(evidence=evidence(age_days=1))
        fact = base.evidence.qualifying_records[0]
        cases = (
            (
                replace(fact, symbol=SECONDARY_SYMBOL),
                "CATALYST_SUBJECT_MISMATCH",
            ),
            (
                replace(fact, issuer_cik="9999999999"),
                "CATALYST_SUBJECT_MISMATCH",
            ),
            (
                replace(fact, source_observation_ids=("foreign-source",)),
                "CATALYST_PROVENANCE_MISMATCH",
            ),
        )
        for foreign_fact, reason in cases:
            with self.subTest(reason=reason):
                context = replace(
                    base,
                    evidence=replace(
                        base.evidence,
                        qualifying_records=(foreign_fact,),
                    ),
                )
                score = score_candidate(context)
                self.assertEqual(score.catalyst_context, 0)
                self.assertEqual(score.status, "DATA_UNAVAILABLE")
                self.assertFalse(score.publishable)
                self.assertIn(reason, score.reason_codes)

    def test_qualifying_fact_retrieval_must_be_current_and_not_future(self) -> None:
        base = candidate_context(evidence=evidence(age_days=1))
        fact = base.evidence.qualifying_records[0]
        cases = (
            (
                replace(
                    fact,
                    published_at=RUN_AT + timedelta(seconds=1),
                    retrieved_at=RUN_AT + timedelta(seconds=1),
                ),
                "CATALYST_TIMESTAMP_IN_FUTURE",
            ),
            (
                replace(fact, retrieved_at=RUN_AT + timedelta(seconds=1)),
                "CATALYST_TIMESTAMP_IN_FUTURE",
            ),
            (
                replace(fact, retrieved_at=RUN_AT - timedelta(seconds=86_401)),
                "CATALYST_SOURCE_STALE",
            ),
        )
        for invalid_fact, reason in cases:
            with self.subTest(reason=reason):
                context = replace(
                    base,
                    evidence=replace(
                        base.evidence,
                        qualifying_records=(invalid_fact,),
                    ),
                )
                score = score_candidate(context)
                self.assertEqual(score.catalyst_context, 0)
                self.assertEqual(score.status, "DATA_UNAVAILABLE")
                self.assertFalse(score.publishable)
                self.assertIn(reason, score.reason_codes)


if __name__ == "__main__":
    unittest.main()
