from __future__ import annotations

import unittest
from datetime import date, datetime
from decimal import Decimal

from stock_monitor.domain import (
    DomainValidationError,
    money_from_micros,
    money_to_micros,
    require_aware_timestamp,
    require_positive_decimal,
    require_positive_int,
)
from tests.support import aware_et


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
