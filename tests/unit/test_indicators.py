from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal, localcontext

from stock_monitor.indicators import (
    IndicatorError,
    average_dollar_volume,
    directional_volume_means,
    ema,
    five_session_return,
    max_relative_volume,
    median_share_volume,
    sma,
    twenty_session_return,
    wilder_atr,
)
from tests.unit._task5_fixtures import load_bar_fixture, trending_bars


class IndicatorTests(unittest.TestCase):
    def test_sma_ema_and_wilder_atr_match_conventional_hand_values(self) -> None:
        values = (Decimal("2"), Decimal("4"), Decimal("6"), Decimal("10"))
        with localcontext() as context:
            context.prec = 50
            expected_sma = Decimal("20") / Decimal("3")

        self.assertEqual(sma(values, 3), expected_sma)
        self.assertEqual(ema(values, 3), Decimal("7"))
        self.assertEqual(
            wilder_atr(load_bar_fixture("bars/atr-hand-calculated.json"), 14),
            Decimal("2.187643"),
        )

    def test_calculations_ignore_a_low_ambient_decimal_precision(self) -> None:
        values = tuple(Decimal(index) / Decimal("7") for index in range(1, 22))
        with localcontext() as context:
            context.prec = 6
            observed = ema(values, 20)
        with localcontext() as context:
            context.prec = 50
            expected = ema(values, 20)
        self.assertEqual(observed, expected)

    def test_insufficient_history_is_rejected(self) -> None:
        bars = trending_bars("AAA")[:13]
        cases = (
            lambda: sma((Decimal("1"),), 2),
            lambda: ema((Decimal("1"),), 2),
            lambda: wilder_atr(bars, 14),
            lambda: five_session_return((Decimal("1"),) * 5),
            lambda: twenty_session_return((Decimal("1"),) * 20),
        )
        for operation in cases:
            with self.subTest(operation=operation), self.assertRaises(IndicatorError):
                operation()

    def test_non_split_adjusted_bars_are_rejected(self) -> None:
        bars = list(trending_bars("AAA"))
        bars[-1] = replace(bars[-1], adjustment="raw")
        for operation in (
            lambda: wilder_atr(bars, 14),
            lambda: average_dollar_volume(bars, 20),
            lambda: median_share_volume(bars, 20),
        ):
            with self.subTest(operation=operation), self.assertRaises(IndicatorError):
                operation()

    def test_returns_and_volume_aggregates_use_completed_tail_windows(self) -> None:
        bars = trending_bars("AAA")
        closes = tuple(bar.close for bar in bars)
        with localcontext() as context:
            context.prec = 50
            expected_five = closes[-1] / closes[-6] - Decimal("1")
            expected_twenty = closes[-1] / closes[-21] - Decimal("1")
            expected_adv = sum(
                bar.close * Decimal(bar.volume) for bar in bars[-20:]
            ) / Decimal("20")

        self.assertEqual(five_session_return(closes), expected_five)
        self.assertEqual(twenty_session_return(closes), expected_twenty)
        self.assertEqual(average_dollar_volume(bars, 20), expected_adv)
        self.assertEqual(median_share_volume(bars, 20), Decimal("5000000"))
        self.assertGreaterEqual(max_relative_volume(bars, 20, 3), Decimal("1.2"))
        up_mean, down_mean = directional_volume_means(bars, 10)
        self.assertIsNotNone(up_mean)
        self.assertIsNotNone(down_mean)
        self.assertGreater(up_mean, down_mean)  # type: ignore[operator]


if __name__ == "__main__":
    unittest.main()

