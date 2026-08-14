from __future__ import annotations

import tomllib
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class BrokerageBoundaryTests(unittest.TestCase):
    def test_runtime_has_no_third_party_dependencies(self) -> None:
        data = tomllib.loads(
            (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )

        self.assertEqual(data["project"]["dependencies"], [])

    def test_project_uses_python_311_src_layout_and_safe_entry_point(self) -> None:
        data = tomllib.loads(
            (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )

        self.assertEqual(data["project"]["requires-python"], ">=3.11")
        self.assertEqual(
            data["project"]["scripts"]["stock-monitor"],
            "stock_monitor.cli:main",
        )
        self.assertEqual(data["tool"]["setuptools"]["package-dir"], {"": "src"})

    def test_source_configuration_excludes_trading_and_robinhood_hosts(self) -> None:
        text = (PROJECT_ROOT / "config" / "sources.toml").read_text(
            encoding="utf-8"
        ).lower()

        self.assertIn("data.alpaca.markets", text)
        self.assertNotIn("paper-api.alpaca.markets", text)
        self.assertNotIn("api.alpaca.markets", text)
        self.assertNotIn("robinhood", text)

    def test_package_defines_no_broker_or_order_module(self) -> None:
        package = PROJECT_ROOT / "src" / "stock_monitor"
        prohibited = {"broker.py", "brokerage.py", "order.py", "orders.py"}

        self.assertTrue(package.is_dir())
        self.assertFalse(prohibited.intersection(path.name for path in package.glob("*.py")))


if __name__ == "__main__":
    unittest.main()
