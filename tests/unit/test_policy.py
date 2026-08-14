from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from stock_monitor.policy import ConfigurationError, Policy
from tests.support import policy_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class PolicyTests(unittest.TestCase):
    def test_published_policy_values_are_loaded_exactly(self) -> None:
        policy = Policy.from_toml(PROJECT_ROOT / "config" / "policy.toml")

        self.assertEqual(policy.capital, Decimal("5000"))
        self.assertEqual(policy.max_live_exposure, Decimal("1000"))
        self.assertEqual(policy.max_position_risk, Decimal("25"))
        self.assertEqual(policy.max_combined_risk, Decimal("50"))
        self.assertEqual(policy.max_positions, 2)
        self.assertEqual(policy.max_entries_per_session, 1)
        self.assertEqual(policy.min_score, 80)
        self.assertEqual(policy.max_monthly_drawdown, Decimal("250"))
        self.assertEqual(policy.max_weekly_drawdown, Decimal("100"))
        self.assertEqual(policy.universe_max_age_days, 31)
        self.assertEqual(policy.live_quote_max_age_seconds, 300)
        self.assertEqual(policy.disagreement_tolerance, Decimal("0.005"))

    def test_valid_fixture_passes_validation(self) -> None:
        policy_fixture().validate()

    def test_every_fixed_safety_limit_rejects_tampering(self) -> None:
        overrides: dict[str, object] = {
            "max_live_exposure": "1001",
            "max_position_risk": "26",
            "max_combined_risk": "51",
            "max_positions": 3,
            "max_entries_per_session": 2,
            "min_score": 79,
            "max_weekly_drawdown": "101",
            "max_monthly_drawdown": "251",
            "universe_max_age_days": 32,
            "live_quote_max_age_seconds": 301,
            "disagreement_tolerance": "0.006",
        }
        for name, value in overrides.items():
            with self.subTest(name=name), self.assertRaises(ConfigurationError):
                policy_fixture(**{name: value}).validate()

    def test_capital_is_immutable_during_validation(self) -> None:
        with self.assertRaises(ConfigurationError):
            policy_fixture(capital="5001").validate()

    def test_fixed_values_reject_equal_but_wrong_numeric_types(self) -> None:
        policy = policy_fixture()
        tampered_values = {
            "max_position_risk": 25,
            "max_positions": 2.0,
        }
        for name, value in tampered_values.items():
            with self.subTest(name=name), self.assertRaises(ConfigurationError):
                replace(policy, **{name: value}).validate()

    def test_validation_baseline_mapping_cannot_be_mutated(self) -> None:
        baseline = Policy._FIXED_VALUES
        original = dict(baseline)
        try:
            with self.assertRaises(TypeError):
                baseline["max_live_exposure"] = Decimal("1000000")
        finally:
            if isinstance(baseline, dict):
                baseline.clear()
                baseline.update(original)

    def test_validation_does_not_trust_a_rebound_class_alias(self) -> None:
        baseline = Policy._FIXED_VALUES
        try:
            Policy._FIXED_VALUES = {"max_live_exposure": Decimal("1000000")}
            with self.assertRaises(ConfigurationError):
                policy_fixture(max_live_exposure="1000000").validate()
        finally:
            Policy._FIXED_VALUES = baseline

    def test_missing_policy_field_is_a_configuration_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.toml"
            path.write_text("[policy]\ncapital = \"5000\"\n", encoding="utf-8")

            with self.assertRaises(ConfigurationError) as raised:
                Policy.from_toml(path)

        self.assertIn("missing", str(raised.exception).lower())

    def test_malformed_policy_toml_is_a_configuration_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.toml"
            path.write_text("[policy\n", encoding="utf-8")

            with self.assertRaises(ConfigurationError) as raised:
                Policy.from_toml(path)

        self.assertIn("policy.toml", str(raised.exception))

    def test_float_policy_money_is_rejected_as_inexact_configuration(self) -> None:
        original = (PROJECT_ROOT / "config" / "policy.toml").read_text(
            encoding="utf-8"
        )
        malformed = original.replace('capital = "5000"', "capital = 5000.0")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.toml"
            path.write_text(malformed, encoding="utf-8")

            with self.assertRaises(ConfigurationError) as raised:
                Policy.from_toml(path)

        self.assertIn("capital", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
