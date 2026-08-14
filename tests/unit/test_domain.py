from __future__ import annotations

import unittest
from datetime import date, datetime
from decimal import Decimal, localcontext

from stock_monitor.domain import (
    DomainValidationError,
    money_from_micros,
    money_to_micros,
    require_aware_timestamp,
    require_positive_decimal,
    require_positive_int,
)
from tests.support import aware_et


MIN_MICRODOLLARS = -(2**63)
MAX_MICRODOLLARS = 2**63 - 1


class MoneyTests(unittest.TestCase):
    def test_microdollar_round_trip_is_exact(self) -> None:
        self.assertEqual(
            money_from_micros(money_to_micros(Decimal("25.01"))),
            Decimal("25.01"),
        )

    def test_money_with_more_than_six_decimal_places_is_rejected(self) -> None:
        with self.assertRaises(DomainValidationError):
            money_to_micros(Decimal("1.0000001"))

    def test_non_finite_money_is_rejected(self) -> None:
        for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with self.subTest(value=value), self.assertRaises(DomainValidationError):
                money_to_micros(value)

    def test_conversion_is_exact_under_low_decimal_context_precision(self) -> None:
        with localcontext() as context:
            context.prec = 6

            micros = money_to_micros(Decimal("25.010001"))
            restored = money_from_micros(micros)
            with self.assertRaises(DomainValidationError):
                money_to_micros(Decimal("25.0100011"))

        self.assertEqual(micros, 25_010_001)
        self.assertEqual(restored, Decimal("25.010001"))

    def test_large_negative_microdollars_round_trip_without_context_rounding(self) -> None:
        micros = -9_000_000_000_000_000_000
        with localcontext() as context:
            context.prec = 6

            restored = money_from_micros(micros)
            round_trip = money_to_micros(restored)

        self.assertEqual(round_trip, micros)

    def test_sqlite_signed_64_bit_boundaries_round_trip_exactly(self) -> None:
        with localcontext() as context:
            context.prec = 6
            for micros in (MIN_MICRODOLLARS, MAX_MICRODOLLARS):
                with self.subTest(micros=micros):
                    restored = money_from_micros(micros)
                    self.assertEqual(money_to_micros(restored), micros)

    def test_values_outside_sqlite_signed_64_bit_range_are_rejected(self) -> None:
        for micros in (MIN_MICRODOLLARS - 1, MAX_MICRODOLLARS + 1):
            with self.subTest(direction="from", micros=micros):
                with self.assertRaises(DomainValidationError):
                    money_from_micros(micros)

        outside_money = (
            Decimal("-9223372036854.775809"),
            Decimal("9223372036854.775808"),
        )
        with localcontext() as context:
            context.prec = 6
            for value in outside_money:
                with self.subTest(direction="to", value=value):
                    with self.assertRaises(DomainValidationError):
                        money_to_micros(value)

    def test_extreme_decimal_exponents_fail_closed_without_power_expansion(self) -> None:
        extreme_values = (
            Decimal("1E+100000"),
            Decimal("-1E+100000"),
            Decimal("1E-100000"),
            Decimal("-1E-100000"),
        )
        with localcontext() as context:
            context.prec = 6
            for value in extreme_values:
                with self.subTest(value=value), self.assertRaises(
                    DomainValidationError
                ):
                    money_to_micros(value)


class DomainInvariantTests(unittest.TestCase):
    def test_aware_timestamp_is_accepted(self) -> None:
        value = aware_et(date(2026, 8, 14), "09:35")

        self.assertIs(require_aware_timestamp(value, "observed_at"), value)

    def test_naive_timestamp_is_rejected(self) -> None:
        with self.assertRaises(DomainValidationError):
            require_aware_timestamp(datetime(2026, 8, 14, 9, 35), "observed_at")

    def test_decimal_values_must_be_positive(self) -> None:
        self.assertEqual(
            require_positive_decimal(Decimal("0.01"), "price"),
            Decimal("0.01"),
        )
        for value in (Decimal("0"), Decimal("-0.01"), Decimal("NaN")):
            with self.subTest(value=value), self.assertRaises(DomainValidationError):
                require_positive_decimal(value, "price")

    def test_integer_values_must_be_positive_non_booleans(self) -> None:
        self.assertEqual(require_positive_int(1, "shares"), 1)
        for value in (0, -1, True):
            with self.subTest(value=value), self.assertRaises(DomainValidationError):
                require_positive_int(value, "shares")


if __name__ == "__main__":
    unittest.main()
