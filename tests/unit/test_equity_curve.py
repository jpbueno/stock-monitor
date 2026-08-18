from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import stock_monitor.risk as risk_module
from stock_monitor.phase1 import (
    EquityMark,
    EquityPoint,
    PaperPosition,
    Phase1Error,
    mark_equity,
    max_drawdown,
)


ET = ZoneInfo("America/New_York")
SESSION = date(2026, 8, 17)


def at(day_offset: int, clock: str = "16:00") -> datetime:
    day = SESSION + timedelta(days=day_offset)
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def point(
    value: str,
    day_offset: int,
    *,
    ledger_name: str = "CANONICAL",
    external_cash_flow: str = "0",
) -> EquityPoint:
    return EquityPoint(
        ledger_name=ledger_name,
        at=at(day_offset),
        cash=Decimal(value),
        positions_value=Decimal("0"),
        equity=Decimal(value),
        external_cash_flow=Decimal(external_cash_flow),
    )


class EquityCurveTests(unittest.TestCase):
    def test_raw_equity_calculation_is_diagnostic_not_phase1_authority(
        self,
    ) -> None:
        diagnostic = mark_equity(
            Decimal("5000"),
            (),
            {},
            ledger_name="CANONICAL",
            at=at(0),
        )

        self.assertFalse(
            risk_module.is_issued_phase1_equity_point_authority(diagnostic)
        )
        self.assertTrue(
            hasattr(risk_module, "_issue_phase1_equity_point_from_source")
        )
        self.assertFalse(
            hasattr(risk_module, "register_phase1_equity_point_authority")
        )
        self.assertFalse(
            hasattr(risk_module, "_register_phase1_equity_point_authority")
        )

    def test_idle_cash_is_part_of_canonical_equity(self) -> None:
        result = mark_equity(
            Decimal("5000"),
            (),
            {},
        )

        self.assertEqual(result.cash, Decimal("5000.000000"))
        self.assertEqual(result.positions_value, Decimal("0.000000"))
        self.assertEqual(result.equity, Decimal("5000.000000"))

    def test_actual_equity_preserves_signed_strategy_cash(self) -> None:
        position = PaperPosition(
            signal_id="sig-deficit",
            symbol="SPY",
            ledger_name="ACTUAL",
            shares=10,
        )
        mark = EquityMark(
            at=at(0),
            bid=Decimal("100"),
            ask=Decimal("100.01"),
            completed_close=Decimal("100"),
        )

        result = mark_equity(
            Decimal("-1100"),
            (position,),
            {"SPY": mark},
            ledger_name="ACTUAL",
            at=at(0),
        )

        self.assertEqual(result.cash, Decimal("-1100.000000"))
        self.assertEqual(result.positions_value, Decimal("1000.000000"))
        self.assertEqual(result.equity, Decimal("-100.000000"))
        with self.assertRaisesRegex(Phase1Error, "INVALID_EQUITY_MONEY"):
            mark_equity(
                Decimal("-1"),
                (),
                {},
                ledger_name="CANONICAL",
                at=at(0),
            )

    def test_consolidated_delayed_bid_is_the_open_position_mark(self) -> None:
        position = PaperPosition(
            signal_id="sig-1",
            symbol="SPY",
            ledger_name="CANONICAL",
            shares=10,
        )
        result = mark_equity(
            Decimal("4000"),
            (position,),
            {
                "SPY": EquityMark(
                    at=at(0),
                    bid=Decimal("99"),
                    ask=Decimal("99.02"),
                    completed_close=Decimal("100"),
                    fresh=True,
                    consolidated=True,
                )
            },
        )

        self.assertEqual(result.positions_value, Decimal("990.000000"))
        self.assertEqual(result.equity, Decimal("4990.000000"))
        self.assertEqual(result.mark_sources, (("SPY", "CONSOLIDATED_BID"),))

    def test_missing_bid_falls_back_to_close_reduced_by_point_one_percent(self) -> None:
        position = PaperPosition(
            signal_id="sig-1",
            symbol="SPY",
            ledger_name="ACTUAL",
            shares=10,
        )
        result = mark_equity(
            Decimal("4000"),
            (position,),
            {
                "SPY": EquityMark(
                    at=at(0),
                    bid=None,
                    ask=None,
                    completed_close=Decimal("100"),
                    fresh=True,
                    consolidated=True,
                )
            },
        )

        self.assertEqual(result.ledger_name, "ACTUAL")
        self.assertEqual(result.positions_value, Decimal("999.000000"))
        self.assertEqual(result.equity, Decimal("4999.000000"))
        self.assertEqual(
            result.mark_sources, (("SPY", "CLOSE_MINUS_0.10_PERCENT"),)
        )

    def test_nonconsolidated_or_stale_bid_uses_completed_close_fallback(self) -> None:
        position = PaperPosition("sig-1", "SPY", "CANONICAL", 1)
        for mark in (
            EquityMark(
                at=at(0),
                bid=Decimal("101"),
                ask=Decimal("101.02"),
                completed_close=Decimal("100"),
                fresh=True,
                consolidated=False,
            ),
            EquityMark(
                at=at(0),
                bid=Decimal("101"),
                ask=Decimal("101.02"),
                completed_close=Decimal("100"),
                fresh=False,
                consolidated=True,
            ),
        ):
            with self.subTest(mark=mark):
                result = mark_equity(Decimal("0"), (position,), {"SPY": mark})
                self.assertEqual(result.equity, Decimal("99.900000"))

    def test_missing_position_mark_fails_closed(self) -> None:
        position = PaperPosition("sig-1", "SPY", "CANONICAL", 1)
        with self.assertRaisesRegex(Phase1Error, "MISSING_POSITION_MARK"):
            mark_equity(Decimal("4000"), (position,), {})

    def test_all_marks_belong_to_the_equity_point_session(self) -> None:
        positions = (
            PaperPosition("sig-1", "SPY", "CANONICAL", 1),
            PaperPosition("sig-2", "QQQ", "CANONICAL", 1),
        )
        marks = {
            "SPY": EquityMark(
                at=at(0),
                bid=Decimal("100"),
                ask=Decimal("100.01"),
                completed_close=Decimal("100"),
            ),
            "QQQ": EquityMark(
                at=at(1),
                bid=Decimal("100"),
                ask=Decimal("100.01"),
                completed_close=Decimal("100"),
            ),
        }

        with self.assertRaisesRegex(
            Phase1Error,
            "EQUITY_MARK_SESSION_MISMATCH",
        ):
            mark_equity(
                Decimal("4800"),
                positions,
                marks,
                at=at(1),
            )

    def test_mixed_canonical_and_actual_positions_are_rejected(self) -> None:
        positions = (
            PaperPosition("sig-1", "SPY", "CANONICAL", 1),
            PaperPosition("sig-2", "QQQ", "ACTUAL", 1),
        )
        marks = {
            symbol: EquityMark(
                at=at(0),
                bid=Decimal("100"),
                ask=Decimal("100.01"),
                completed_close=Decimal("100"),
            )
            for symbol in ("SPY", "QQQ")
        }
        with self.assertRaisesRegex(Phase1Error, "MIXED_EQUITY_LEDGERS"):
            mark_equity(Decimal("4800"), positions, marks)

    def test_external_cash_flows_do_not_create_or_hide_drawdown(self) -> None:
        deposit_curve = (
            point("5000", 0),
            point("6000", 1, external_cash_flow="1000"),
            point("5750", 2),
        )
        withdrawal_curve = (
            point("5000", 0),
            point("4000", 1, external_cash_flow="-1000"),
            point("3999", 2),
        )

        self.assertEqual(max_drawdown(deposit_curve), Decimal("250.000000"))
        self.assertEqual(max_drawdown(withdrawal_curve), Decimal("1.000000"))

    def test_high_water_uses_the_supplied_strict_time_order(self) -> None:
        curve = (point("5000", 1), point("5100", 0))
        with self.assertRaisesRegex(Phase1Error, "EQUITY_TIME_ORDER"):
            max_drawdown(curve)

    def test_money_values_and_position_extensions_stay_in_int64_micros(self) -> None:
        maximum = Decimal("9223372036854.775807")
        accepted = mark_equity(
            maximum,
            (),
            {},
            ledger_name="CANONICAL",
            at=at(0),
        )
        self.assertEqual(accepted.equity, maximum)
        with self.assertRaisesRegex(Phase1Error, "INVALID_EQUITY_MONEY"):
            mark_equity(
                maximum + Decimal("0.000001"),
                (),
                {},
                ledger_name="CANONICAL",
                at=at(0),
            )
        with self.assertRaisesRegex(Phase1Error, "EQUITY_OVERFLOW"):
            mark_equity(
                Decimal("0"),
                (PaperPosition("sig-1", "SPY", "CANONICAL", 2),),
                {
                    "SPY": EquityMark(
                        at=at(0),
                        bid=maximum,
                        ask=maximum,
                        completed_close=maximum,
                    )
                },
            )

    def test_equity_dtos_are_canonical_decimal_and_immutable(self) -> None:
        result = mark_equity(
            Decimal("5000.0"),
            (),
            {},
            ledger_name="CANONICAL",
            at=at(0),
        )
        self.assertEqual(result.equity.as_tuple().exponent, -6)
        with self.assertRaises(FrozenInstanceError):
            result.equity = Decimal("1")  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
