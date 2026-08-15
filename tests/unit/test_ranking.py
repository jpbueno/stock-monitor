from __future__ import annotations

import copy
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext

from stock_monitor.screening import (
    ScoredCandidate,
    ScoreCard,
    ScreeningError,
    SetupDecision,
    rank_candidates,
    is_issued_publication_decision,
    select_publication_roles,
    to_scored_candidate,
)
import stock_monitor.screening as screening_module
from tests.unit._task5_fixtures import candidate_context, evidence, previous_quote


def candidate(
    symbol: str,
    *,
    score: int = 90,
    relative_strength: str = "75",
    dollar_volume: str = "200000000",
) -> ScoredCandidate:
    remaining = score
    category_values: list[int] = []
    for cap in (25, 20, 20, 15, 10, 10):
        value = min(cap, remaining)
        category_values.append(value)
        remaining -= value
    score_card = ScoreCard(
        status="PUBLISHABLE" if score >= 80 else "BELOW_MINIMUM_SCORE",
        reason_codes=(),
        trend_and_regime=category_values[0],
        relative_strength=category_values[1],
        setup_quality=category_values[2],
        volume_confirmation=category_values[3],
        catalyst_context=category_values[4],
        liquidity_execution=category_values[5],
        total=score,
        publishable=score >= 80,
        five_session_relative_strength=Decimal("0.01"),
        twenty_session_relative_strength=Decimal("0.02"),
        five_session_relative_strength_percentile=Decimal(relative_strength),
        twenty_session_relative_strength_percentile=Decimal(relative_strength),
    )
    setup = SetupDecision(
        eligible=True,
        status="ELIGIBLE",
        setup_type="PULLBACK_RECLAIM",
        qualifying_setups=("PULLBACK_RECLAIM",),
        reason_codes=(),
        atr14=Decimal("2"),
        resistance=None,
        raw_trigger=Decimal("100.001"),
        raw_stop=Decimal("98.009"),
        trigger_price=Decimal("100.01"),
        stop_price=Decimal("98.00"),
        planned_entry=Decimal("100.12"),
        maximum_permitted_entry=Decimal("100.12"),
        target_price=Decimal("104.36"),
        stop_distance=Decimal("2.12"),
    )
    return ScoredCandidate(
        symbol=symbol,
        total_score=score,
        relative_strength_percentile=Decimal(relative_strength),
        average_dollar_volume=Decimal(dollar_volume),
        publication_session=date(2026, 8, 14),
        raw_trigger=Decimal("100.001"),
        raw_stop=Decimal("98.009"),
        delayed_spread_amount=Decimal("0.02"),
        tick_size=Decimal("0.01"),
        trigger_price=Decimal("100.01"),
        maximum_permitted_entry=Decimal("100.12"),
        recommended_stop=Decimal("98.00"),
        target_price=Decimal("104.36"),
        score_card=score_card,
        setup=setup,
    )


class RankingTests(unittest.TestCase):
    def test_four_way_key_is_score_rs_adv_then_symbol(self) -> None:
        values = (
            candidate("DDD", score=89, relative_strength="99", dollar_volume="999999999"),
            candidate("CCC", relative_strength="74", dollar_volume="900000000"),
            candidate("BBB", relative_strength="75", dollar_volume="100000000"),
            candidate("AAA", relative_strength="75", dollar_volume="100000000"),
            candidate("ZZZ", relative_strength="75", dollar_volume="500000000"),
        )

        ranked = rank_candidates(values)

        self.assertEqual(tuple(item.symbol for item in ranked), ("ZZZ", "AAA", "BBB"))

    def test_ranking_is_exact_under_low_ambient_decimal_precision(self) -> None:
        values = (
            candidate("AAA", relative_strength="75.0000001"),
            candidate("ZZZ", relative_strength="75.0000002"),
        )
        with localcontext() as context:
            context.prec = 6
            ranked = rank_candidates(values)

        self.assertEqual(tuple(item.symbol for item in ranked), ("ZZZ", "AAA"))

    def test_ranking_preserves_decimal_differences_beyond_fifty_digits(self) -> None:
        relative_base = "75." + "0" * 60
        dollar_base = "1" + "0" * 60
        cases = (
            (
                (
                    candidate("AAA", relative_strength=relative_base + "1"),
                    candidate("ZZZ", relative_strength=relative_base + "2"),
                ),
                "relative strength",
            ),
            (
                (
                    candidate("AAA", dollar_volume=dollar_base + "1"),
                    candidate("ZZZ", dollar_volume=dollar_base + "2"),
                ),
                "dollar volume",
            ),
        )

        for values, dimension in cases:
            with self.subTest(dimension=dimension):
                ranked = rank_candidates(values)
                self.assertEqual(
                    tuple(item.symbol for item in ranked),
                    ("ZZZ", "AAA"),
                )

    def test_at_most_three_results_receive_one_primary_and_two_shadows(self) -> None:
        decision = select_publication_roles(
            tuple(candidate(symbol) for symbol in ("DDD", "CCC", "BBB", "AAA")),
            capacity_available=True,
        )
        self.assertEqual(len(decision.candidates), 3)
        self.assertEqual(decision.candidates[0].role, "PRIMARY")
        self.assertEqual(
            tuple(item.role for item in decision.candidates[1:]),
            ("WATCHLIST_SHADOW", "WATCHLIST_SHADOW"),
        )

    def test_rank_two_never_substitutes_when_rank_one_lacks_capacity(self) -> None:
        values = (
            candidate("AAA", score=95),
            candidate("BBB", score=90),
        )

        decision = select_publication_roles(values, capacity_available=False)

        self.assertEqual(decision.status, "NO_PRIMARY_CAPACITY")
        self.assertIsNone(decision.primary)
        self.assertTrue(
            all(item.role == "WATCHLIST_SHADOW" for item in decision.candidates)
        )

    def test_rank_one_capacity_outcome_is_an_explicit_required_input(self) -> None:
        with self.assertRaises(TypeError):
            select_publication_roles((candidate("AAA"),))

    def test_global_no_capacity_also_produces_no_primary(self) -> None:
        decision = select_publication_roles(
            (candidate("AAA"), candidate("BBB")), capacity_available=False
        )
        self.assertEqual(decision.status, "NO_PRIMARY_CAPACITY")
        self.assertIsNone(decision.primary)

    def test_raw_capacity_boolean_cannot_issue_publication_authority(self) -> None:
        scored = to_scored_candidate(
            candidate_context(evidence=evidence(age_days=11))
        )
        issued = select_publication_roles(
            (scored,),
            capacity_available=True,
        )

        self.assertFalse(is_issued_publication_decision(issued))
        self.assertFalse(hasattr(screening_module, "_issue_scored_candidate"))
        self.assertFalse(hasattr(screening_module, "_issue_publication_decision"))
        self.assertFalse(is_issued_publication_decision(copy.copy(issued)))
        self.assertFalse(is_issued_publication_decision(replace(issued)))
        self.assertFalse(
            is_issued_publication_decision(
                replace(
                    issued,
                    status="NO_PRIMARY_CAPACITY",
                    candidates=(
                        replace(
                            issued.candidates[0],
                            role="WATCHLIST_SHADOW",
                        ),
                    ),
                    primary=None,
                )
            )
        )

        direct = candidate("AAA")
        for unissued in (direct, copy.copy(scored), replace(scored)):
            with self.subTest(unissued=unissued):
                decision = select_publication_roles(
                    (unissued,),
                    capacity_available=True,
                )
                self.assertFalse(is_issued_publication_decision(decision))

    def test_scored_candidate_exposes_task6_price_contract_without_portfolio_state(self) -> None:
        context = candidate_context(evidence=evidence(age_days=11))
        candidate_value = to_scored_candidate(context)

        self.assertEqual(candidate_value.publication_session, context.session_date)
        self.assertEqual(candidate_value.raw_trigger, candidate_value.setup.raw_trigger)
        self.assertEqual(candidate_value.raw_stop, candidate_value.setup.raw_stop)
        self.assertEqual(candidate_value.tick_size, Decimal("0.01"))
        self.assertEqual(
            candidate_value.delayed_spread_amount,
            context.previous_session_quote.ask - context.previous_session_quote.bid,
        )
        self.assertEqual(candidate_value.trigger_price, candidate_value.setup.trigger_price)
        self.assertEqual(
            candidate_value.maximum_permitted_entry,
            candidate_value.setup.maximum_permitted_entry,
        )
        self.assertEqual(
            candidate_value.recommended_stop, candidate_value.setup.stop_price
        )
        self.assertEqual(candidate_value.target_price, candidate_value.setup.target_price)

    def test_scored_candidate_spread_is_exact_under_low_ambient_precision(self) -> None:
        quote = replace(
            previous_quote(),
            bid=Decimal("20"),
            ask=Decimal("20.02469135780246913578024691358"),
        )
        context = candidate_context(
            evidence=evidence(age_days=11),
            previous_session_quote=quote,
        )
        with localcontext() as decimal_context:
            decimal_context.prec = 80
            expected = quote.ask - quote.bid

        with localcontext() as decimal_context:
            decimal_context.prec = 6
            candidate_value = to_scored_candidate(context)

        self.assertEqual(candidate_value.delayed_spread_amount, expected)

    def test_price_formulas_preserve_decimals_beyond_fifty_digits(self) -> None:
        audit = candidate("AAA")
        tick = Decimal("1e-70")
        raw_trigger = Decimal(
            "100.123456789012345678901234567890123456789012345678901234567890123456789"
        )
        raw_stop = Decimal(
            "98.009876543210987654321098765432109876543210987654321098765432109876543"
        )
        spread = Decimal("0.020000000000000000000000000000000000000000000000000000000000000000001")
        with localcontext() as decimal_context:
            decimal_context.prec = 240
            trigger = (
                (raw_trigger / tick).to_integral_value(rounding=ROUND_CEILING)
                * tick
            )
            stop = (
                (raw_stop / tick).to_integral_value(rounding=ROUND_FLOOR)
                * tick
            )
            slippage = max(Decimal("0.001") * trigger, spread / Decimal("2"))
            entry = (
                ((trigger + slippage) / tick).to_integral_value(
                    rounding=ROUND_CEILING
                )
                * tick
            )
            stop_distance = entry - stop
            target = (
                ((entry + Decimal("2") * stop_distance) / tick).to_integral_value(
                    rounding=ROUND_CEILING
                )
                * tick
            )
        setup = replace(
            audit.setup,
            raw_trigger=raw_trigger,
            raw_stop=raw_stop,
            trigger_price=trigger,
            stop_price=stop,
            planned_entry=entry,
            maximum_permitted_entry=entry,
            target_price=target,
            stop_distance=stop_distance,
        )

        with localcontext() as decimal_context:
            decimal_context.prec = 6
            result = ScoredCandidate(
                symbol="AAA",
                total_score=audit.total_score,
                relative_strength_percentile=audit.relative_strength_percentile,
                average_dollar_volume=audit.average_dollar_volume,
                publication_session=audit.publication_session,
                raw_trigger=raw_trigger,
                raw_stop=raw_stop,
                delayed_spread_amount=spread,
                tick_size=tick,
                trigger_price=trigger,
                maximum_permitted_entry=entry,
                recommended_stop=stop,
                target_price=target,
                score_card=audit.score_card,
                setup=setup,
            )

        self.assertEqual(result.maximum_permitted_entry, entry)
        self.assertEqual(result.target_price, target)

    def test_empty_ranked_set_is_no_trade(self) -> None:
        decision = select_publication_roles((), capacity_available=True)
        self.assertEqual(decision.status, "NO_TRADE")
        self.assertEqual(decision.candidates, ())
        self.assertFalse(is_issued_publication_decision(decision))

    def test_duplicate_symbols_and_nonpublishable_or_incomplete_contracts_are_rejected(self) -> None:
        valid = candidate("AAA")
        with self.assertRaises(ScreeningError):
            rank_candidates((valid, candidate("AAA", score=91)))
        with self.assertRaises(ScreeningError):
            rank_candidates((candidate("BBB", score=79),))
        with self.assertRaises(ScreeningError):
            replace(valid, target_price=None)
        with self.assertRaises(ScreeningError):
            replace(valid, trigger_price=Decimal("100.015"))

    def test_full_price_contract_requires_a_publishable_score_card(self) -> None:
        valid = candidate("AAA")
        nonpublishable = replace(
            valid.score_card,
            status="BELOW_MINIMUM_SCORE",
            publishable=False,
        )

        with self.assertRaises(ScreeningError):
            replace(valid, score_card=nonpublishable)

    def test_price_contract_requires_exact_audit_boolean_outcomes(self) -> None:
        valid = candidate("AAA")
        malformed = (
            lambda: replace(
                valid,
                score_card=replace(valid.score_card, publishable=1),
            ),
            lambda: replace(
                valid,
                setup=replace(valid.setup, eligible=1),
            ),
        )

        for build in malformed:
            with self.subTest(build=build), self.assertRaises(ScreeningError):
                build()

    def test_full_price_contract_target_must_equal_exact_tick_rounded_two_r(self) -> None:
        valid = candidate("AAA")
        for target in (
            Decimal("104.35"),
            Decimal("104.37"),
            Decimal("200.00"),
        ):
            with self.subTest(target=target), self.assertRaises(ScreeningError):
                replace(
                    valid,
                    target_price=target,
                    setup=replace(valid.setup, target_price=target),
                )

    def test_price_contract_recomputes_trigger_stop_and_maximum_entry_exactly(self) -> None:
        valid = candidate("AAA")
        wrong_stop_setup = replace(
            valid.setup,
            stop_price=Decimal("97.99"),
            stop_distance=Decimal("2.13"),
            target_price=Decimal("104.38"),
        )
        wrong_entry_setup = replace(
            valid.setup,
            planned_entry=Decimal("100.13"),
            maximum_permitted_entry=Decimal("100.13"),
            stop_distance=Decimal("2.13"),
            target_price=Decimal("104.39"),
        )
        malformed_builders = (
            lambda: replace(
                valid,
                trigger_price=Decimal("100.02"),
                setup=replace(valid.setup, trigger_price=Decimal("100.02")),
            ),
            lambda: replace(
                valid,
                recommended_stop=Decimal("97.99"),
                target_price=Decimal("104.38"),
                setup=wrong_stop_setup,
            ),
            lambda: replace(
                valid,
                maximum_permitted_entry=Decimal("100.13"),
                target_price=Decimal("104.39"),
                setup=wrong_entry_setup,
            ),
        )

        for build in malformed_builders:
            with self.subTest(build=build), self.assertRaises(ScreeningError):
                build()

    def test_delayed_spread_must_drive_the_recomputed_maximum_entry(self) -> None:
        valid = candidate("AAA")

        with self.assertRaises(ScreeningError):
            replace(valid, delayed_spread_amount=Decimal("10"))


if __name__ == "__main__":
    unittest.main()
