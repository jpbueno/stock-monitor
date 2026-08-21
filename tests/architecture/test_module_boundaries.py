"""Protect the narrow Task 9-11 dependency graph from accidental coupling."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[2]
PACKAGE = ROOT / "src" / "stock_monitor"


def _stock_monitor_imports(module: str) -> set[str]:
    path = PACKAGE / f"{module}.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(
                alias.name
                for alias in node.names
                if alias.name == "stock_monitor"
                or alias.name.startswith("stock_monitor.")
            )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 1 and node.module:
                imports.add(f"stock_monitor.{node.module}")
            elif node.level == 0 and node.module and (
                node.module == "stock_monitor"
                or node.module.startswith("stock_monitor.")
            ):
                imports.add(node.module)
    return imports


def _top_level_stock_monitor_imports(module: str) -> set[str]:
    path = PACKAGE / f"{module}.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            imports.update(
                alias.name
                for alias in node.names
                if alias.name == "stock_monitor"
                or alias.name.startswith("stock_monitor.")
            )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 1 and node.module:
                imports.add(f"stock_monitor.{node.module}")
            elif node.level == 0 and node.module and (
                node.module == "stock_monitor"
                or node.module.startswith("stock_monitor.")
            ):
                imports.add(node.module)
    return imports


def _forbidden_imports(module: str, forbidden: set[str]) -> set[str]:
    return {
        imported
        for imported in _stock_monitor_imports(module)
        if imported.removeprefix("stock_monitor.").split(".", 1)[0] in forbidden
    }


class ModuleBoundaryTests(unittest.TestCase):
    def test_task9_modules_do_not_eagerly_import_runtime_or_persistence_layers(self) -> None:
        forbidden = {
            "cli",
            "config",
            "exports",
            "journal",
            "providers",
            "reports",
            "scheduled",
            "workflows",
        }
        for module in ("replay", "options_paper"):
            with self.subTest(module=module):
                imported_roots = {
                    imported.removeprefix("stock_monitor.").split(".", 1)[0]
                    for imported in _top_level_stock_monitor_imports(module)
                }
                self.assertTrue(imported_roots.isdisjoint(forbidden))

    def test_replay_remains_fully_independent_of_runtime_layers(self) -> None:
        self.assertEqual(
            _forbidden_imports(
                "replay",
                {
                    "cli",
                    "config",
                    "exports",
                    "journal",
                    "providers",
                    "reports",
                    "scheduled",
                    "workflows",
                },
            ),
            set(),
        )

    def test_report_layer_does_not_reach_provider_or_orchestration_layers(self) -> None:
        self.assertEqual(
            _forbidden_imports(
                "reports",
                {"cli", "providers", "scheduled", "workflows"},
            ),
            set(),
        )

    def test_workflow_layer_only_reaches_lower_level_report_material(self) -> None:
        self.assertEqual(
            _forbidden_imports(
                "workflows",
                {
                    "cli",
                    "config",
                    "exports",
                    "options_paper",
                    "providers",
                    "replay",
                    "scheduled",
                },
            ),
            set(),
        )

    def test_scheduler_depends_only_on_domain_and_workflow_contracts(self) -> None:
        self.assertEqual(
            _top_level_stock_monitor_imports("scheduled"),
            {"stock_monitor.domain", "stock_monitor.workflows"},
        )
        self.assertEqual(
            _stock_monitor_imports("scheduled"),
            {
                "stock_monitor.domain",
                "stock_monitor.journal",
                "stock_monitor.workflows",
            },
        )


if __name__ == "__main__":
    unittest.main()
