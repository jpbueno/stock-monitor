from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext

from stock_monitor.indicators import ema, wilder_atr
from stock_monitor.screening import detect_setup
from tests.unit._task5_fixtures import (
    PRIMARY_SYMBOL,
    SECONDARY_SYMBOL,
    breakout_bars,
    candidate_context,
    constant_bars,
    with_candidate_bars,
)


def round_up(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def round_down(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


class SetupTests(unittest.TestCase):
    def test_pullback_reclaim_uses_exact_formula_and_tick_rounding(self) -> None:
        context = candidate_context()
        bars = context.bars_by_symbol[PRIMARY_SYMBOL]
        decision = detect_setup(context)
        with localcontext() as decimal_context:
            decimal_context.prec = 50
            atr = wilder_atr(bars, 14)
            raw_trigger = max(bars[-1].high, bars[-2].high) + Decimal("0.05") * atr
            raw_stop = min(bar.low for bar in bars[-3:]) - Decimal("0.10") * atr
            trigger = round_up(raw_trigger, Decimal("0.01"))
            stop = round_down(raw_stop, Decimal("0.01"))
            spread = (
                context.previous_session_quote.ask
                - context.previous_session_quote.bid
            )
            slippage = max(Decimal("0.001") * trigger, spread / Decimal("2"))
            entry = round_up(trigger + slippage, Decimal("0.01"))
            target = round_up(entry + Decimal("2") * (entry - stop), Decimal("0.01"))

        self.assertTrue(decision.eligible)
        self.assertEqual(decision.setup_type, "PULLBACK_RECLAIM")
        self.assertEqual(decision.atr14, atr)
        self.assertEqual(decision.raw_trigger, raw_trigger)
        self.assertEqual(decision.raw_stop, raw_stop)
        self.assertEqual(decision.trigger_price, trigger)
        self.assertEqual(decision.stop_price, stop)
        self.assertEqual(decision.planned_entry, entry)
        self.assertEqual(decision.target_price, target)
        self.assertGreater(ema(tuple(bar.close for bar in bars), 20), Decimal("0"))

    def test_spread_driven_entry_preserves_decimals_beyond_fifty_digits(self) -> None:
        base = candidate_context()
        tick = Decimal("1e-70")
        quote = replace(
            base.previous_session_quote,
            bid=Decimal("20"),
            ask=Decimal(
                "20.123456789012345678901234567890123456789012345678901234567890123456789"
            ),
        )
        context = replace(
            base,
            record=replace(base.record, tick_size=tick),
            previous_session_quote=quote,
        )

        with localcontext() as decimal_context:
            decimal_context.prec = 6
            decision = detect_setup(context)
        self.assertTrue(decision.eligible)
        with localcontext() as decimal_context:
            decimal_context.prec = 240
            spread = quote.ask - quote.bid
            slippage = max(
                Decimal("0.001") * decision.trigger_price,
                spread / Decimal("2"),
            )
            expected_entry = round_up(decision.trigger_price + slippage, tick)
            expected_stop_distance = expected_entry - decision.stop_price
            expected_target = round_up(
                expected_entry + Decimal("2") * expected_stop_distance,
                tick,
            )

        self.assertEqual(decision.maximum_permitted_entry, expected_entry)
        self.assertEqual(decision.stop_distance, expected_stop_distance)
        self.assertEqual(decision.target_price, expected_target)

    def test_breakout_confirmation_uses_t_minus_20_through_t_minus_2(self) -> None:
        context = with_candidate_bars(candidate_context(), breakout_bars())

        decision = detect_setup(context)

        self.assertTrue(decision.eligible)
        self.assertEqual(decision.setup_type, "BREAKOUT_CONFIRMATION")
        self.assertEqual(decision.resistance, Decimal("20.20"))
        self.assertEqual(decision.trigger_price, Decimal("20.22"))
        self.assertEqual(decision.stop_price, Decimal("19.76"))

    def test_neither_setup_qualifying_is_ineligible(self) -> None:
        context = with_candidate_bars(
            candidate_context(),
            constant_bars(PRIMARY_SYMBOL, half_range=Decimal("0.05")),
        )
        decision = detect_setup(context)
        self.assertFalse(decision.eligible)
        self.assertIn("SETUP_NOT_QUALIFIED", decision.reason_codes)

    def test_invalid_tick_and_nonpositive_price_plan_fail_closed(self) -> None:
        invalid_tick = replace(
            candidate_context(),
            record=replace(candidate_context().record, tick_size=Decimal("0")),
        )
        decision = detect_setup(invalid_tick)
        self.assertFalse(decision.eligible)
        self.assertIn("INVALID_TICK_SIZE", decision.reason_codes)

        base = candidate_context()
        mapping = dict(base.bars_by_symbol)
        bars = list(mapping[PRIMARY_SYMBOL])
        bars[-3] = replace(bars[-3], low=Decimal("0.01"))
        mapping[PRIMARY_SYMBOL] = bars
        nonpositive = detect_setup(replace(base, bars_by_symbol=mapping))
        self.assertFalse(nonpositive.eligible)
        self.assertIn("NONPOSITIVE_STOP_PRICE", nonpositive.reason_codes)

    def test_setup_requires_full_previous_session_quote_contract(self) -> None:
        base = candidate_context()
        wrong_symbol = replace(
            base,
            previous_session_quote=replace(
                base.previous_session_quote, symbol=SECONDARY_SYMBOL
            ),
        )
        wrong_feed = replace(
            base,
            previous_session_quote=replace(base.previous_session_quote, feed="iex"),
        )
        wrong_window = replace(
            base,
            previous_session_quote=replace(
                base.previous_session_quote,
                timestamp=base.previous_session_quote.timestamp.replace(
                    hour=15, minute=54
                ),
            ),
        )
        cases = (
            (wrong_symbol, "PREVIOUS_SESSION_QUOTE_SYMBOL_MISMATCH"),
            (wrong_feed, "PREVIOUS_SESSION_QUOTE_NOT_CONSOLIDATED"),
            (wrong_window, "PREVIOUS_SESSION_QUOTE_OUTSIDE_WINDOW"),
        )
        for context, reason in cases:
            with self.subTest(reason=reason):
                decision = detect_setup(context)
                self.assertFalse(decision.eligible)
                self.assertEqual(decision.status, "DATA_UNAVAILABLE")
                self.assertIn(reason, decision.reason_codes)

    def test_context_snapshots_input_bars_so_future_mutation_cannot_look_ahead(self) -> None:
        base = candidate_context()
        source = list(base.bars_by_symbol[PRIMARY_SYMBOL])
        mapping = dict(base.bars_by_symbol)
        mapping[PRIMARY_SYMBOL] = source
        context = replace(base, bars_by_symbol=mapping)
        before = detect_setup(context)
        source.append(replace(source[-1], close=Decimal("9999")))

        after = detect_setup(context)

        self.assertEqual(before, after)
        self.assertEqual(len(context.bars_by_symbol[PRIMARY_SYMBOL]), 60)


if __name__ == "__main__":
    unittest.main()
