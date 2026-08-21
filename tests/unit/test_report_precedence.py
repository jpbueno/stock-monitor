"""Locked close-review precedence and wording tests."""

from __future__ import annotations

import itertools
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_monitor.reports import (
    ClosePosition,
    CloseState,
    ReportSource,
    render_close_report,
)


ET = ZoneInfo("America/New_York")


def close_state(**changes: object) -> CloseState:
    values: dict[str, object] = {
        "session_date": date(2026, 8, 14),
        "generated_at": datetime(2026, 8, 14, 15, 30, tzinfo=ET),
        "reason_codes": ("POSITION_REVIEW_COMPLETE",),
        "positions": (
            ClosePosition(
                symbol="NVDA",
                shares=5,
                mark=Decimal("184.25"),
                estimated_unrealized_pl=Decimal("14.25"),
                r_multiple=Decimal("1.14"),
                recommended_stop=Decimal("181.40"),
                user_confirmed_stop=Decimal("180.00"),
                target=Decimal("186.40"),
                holding_days=3,
                provider="ALPACA",
                feed="IEX",
                observed_at=datetime(2026, 8, 14, 15, 29, tzinfo=ET),
                upcoming_events=("Earnings after planned exit",),
                evidence=(
                    ReportSource(
                        "Issuer calendar",
                        "https://investor.nvidia.com/calendar",
                    ),
                ),
            ),
        ),
        "observation_ids": ("obs-close", "obs-position"),
        "state_hash": "a" * 64,
        "reconciliation_required": False,
        "position_verified": True,
        "stop_verified": True,
        "exit_due": False,
        "tighten_stop_due": False,
    }
    values.update(changes)
    return CloseState(**values)  # type: ignore[arg-type]


class ReportPrecedenceTests(unittest.TestCase):
    def test_report_sources_reject_sensitive_and_obfuscated_query_names(self) -> None:
        poisoned_names = (
            "token",
            "access_token",
            "api_key",
            "apikey",
            "authorization",
            "credential",
            "key_id",
            "password",
            "secret",
            "signature",
            "api-key",
            "x.api.key",
            "key-id",
            "client-secret",
            "auth-token",
            "request-signature",
        )
        for name in poisoned_names:
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError,
                "credential",
            ):
                ReportSource(
                    "Issuer filing",
                    f"https://example.com/filing?{name}=canary",
                )

    def test_each_close_state_has_the_locked_outcome(self) -> None:
        cases = (
            (
                {"reconciliation_required": True},
                "RECONCILIATION REQUIRED",
            ),
            ({"position_verified": False}, "POSITION UNVERIFIED"),
            ({"stop_verified": False}, "STOP UNVERIFIED"),
            (
                {"exit_due": True},
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            (
                {"tighten_stop_due": True},
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            ({}, "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE"),
        )
        for changes, expected in cases:
            with self.subTest(expected=expected):
                report = render_close_report(close_state(**changes))
                self.assertEqual(report.outcome, expected)
                self.assertIn(f"`{expected}`", report.body)

    def test_every_pair_of_conflicts_selects_the_higher_precedence(self) -> None:
        conditions = (
            ("reconciliation_required", True, "RECONCILIATION REQUIRED"),
            ("position_verified", False, "POSITION UNVERIFIED"),
            ("stop_verified", False, "STOP UNVERIFIED"),
            (
                "exit_due",
                True,
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
            ),
            (
                "tighten_stop_due",
                True,
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
            ),
        )
        for higher, lower in itertools.combinations(conditions, 2):
            with self.subTest(higher=higher[0], lower=lower[0]):
                report = render_close_report(
                    close_state(**{higher[0]: higher[1], lower[0]: lower[1]})
                )
                self.assertEqual(report.outcome, higher[2])

    def test_reconciliation_precedes_exit_and_unverified_stop(self) -> None:
        report = render_close_report(
            close_state(
                reconciliation_required=True,
                exit_due=True,
                stop_verified=False,
            )
        )

        self.assertIn("RECONCILIATION REQUIRED", report.body)
        self.assertNotIn("PROVISIONAL EXIT", report.outcome)

    def test_recommended_and_user_confirmed_stops_are_separate(self) -> None:
        report = render_close_report(close_state(tighten_stop_due=True))

        self.assertIn("Recommended stop: `$181.40`", report.body)
        self.assertIn("User-confirmed stop: `$180.00`", report.body)
        self.assertIn("provisional pending current Robinhood verification", report.body)
        self.assertIn("cannot change or replace the protective stop", report.body)

    def test_unverified_stop_is_never_filled_from_the_recommendation(self) -> None:
        state = close_state(stop_verified=False)
        state = replace(
            state,
            positions=(replace(state.positions[0], user_confirmed_stop=None),),
        )

        report = render_close_report(state)

        self.assertEqual(report.outcome, "STOP UNVERIFIED")
        self.assertIn("Recommended stop: `$181.40`", report.body)
        self.assertIn("User-confirmed stop: `NOT CONFIRMED`", report.body)


if __name__ == "__main__":
    unittest.main()
