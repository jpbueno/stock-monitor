from __future__ import annotations

import shutil
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from pathlib import Path

from stock_monitor.config import ConfigurationError, load_settings


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENVIRONMENT = {
    "APCA_API_KEY_ID": "key-id-canary",
    "APCA_API_SECRET_KEY": "secret-key-canary",
    "SEC_USER_AGENT": "Stock Monitor tests test@example.com",
}


class RecordingEnvironment(Mapping[str, str]):
    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)
        self.accessed: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.accessed.append(key)
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


class SettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.project_root = Path(self.temporary_directory.name) / "project"
        shutil.copytree(PROJECT_ROOT / "config", self.project_root / "config")

    def test_project_and_default_runtime_paths_are_resolved(self) -> None:
        settings = load_settings(self.project_root / ".", ENVIRONMENT)

        self.assertEqual(settings.project_root, self.project_root.resolve())
        self.assertEqual(settings.config_root, self.project_root.resolve() / "config")
        self.assertEqual(settings.state_root, self.project_root.resolve() / ".stock-monitor")
        self.assertEqual(settings.journal_path, settings.state_root / "journal.sqlite3")
        self.assertEqual(settings.cache_root, settings.state_root / "cache")
        self.assertEqual(settings.reports_root, self.project_root.resolve() / "reports")

    def test_relative_runtime_home_is_resolved_from_project_root(self) -> None:
        environ = {**ENVIRONMENT, "STOCK_MONITOR_HOME": "operator-state"}

        settings = load_settings(self.project_root, environ)

        operator_root = self.project_root.resolve() / "operator-state"
        self.assertEqual(settings.state_root, operator_root / ".stock-monitor")
        self.assertEqual(settings.reports_root, operator_root / "reports")

    def test_settings_repr_never_contains_secret_values(self) -> None:
        settings = load_settings(self.project_root, ENVIRONMENT)

        rendered = repr(settings)

        self.assertNotIn(ENVIRONMENT["APCA_API_KEY_ID"], rendered)
        self.assertNotIn(ENVIRONMENT["APCA_API_SECRET_KEY"], rendered)

    def test_loader_reads_only_the_four_approved_environment_variables(self) -> None:
        environ = RecordingEnvironment(
            {
                **ENVIRONMENT,
                "STOCK_MONITOR_HOME": "runtime",
                "UNRELATED_SECRET": "must-not-be-read",
            }
        )

        load_settings(self.project_root, environ)

        self.assertEqual(
            set(environ.accessed),
            {
                "APCA_API_KEY_ID",
                "APCA_API_SECRET_KEY",
                "SEC_USER_AGENT",
                "STOCK_MONITOR_HOME",
            },
        )

    def test_missing_required_environment_value_is_a_safe_configuration_error(self) -> None:
        environ = {
            "APCA_API_KEY_ID": "key-id-canary",
            "APCA_API_SECRET_KEY": "secret-key-canary",
            "SEC_USER_AGENT": "",
        }

        with self.assertRaises(ConfigurationError) as raised:
            load_settings(self.project_root, environ)

        message = str(raised.exception)
        self.assertIn("SEC_USER_AGENT", message)
        self.assertNotIn("key-id-canary", message)
        self.assertNotIn("secret-key-canary", message)

    def test_missing_sources_table_is_rejected(self) -> None:
        (self.project_root / "config" / "sources.toml").write_text(
            "schema_version = 1\n",
            encoding="utf-8",
        )

        with self.assertRaises(ConfigurationError) as raised:
            load_settings(self.project_root, ENVIRONMENT)

        self.assertIn("sources", str(raised.exception).lower())

    def test_malformed_sources_toml_is_rejected_without_secret_disclosure(self) -> None:
        (self.project_root / "config" / "sources.toml").write_text(
            "[sources\n",
            encoding="utf-8",
        )

        with self.assertRaises(ConfigurationError) as raised:
            load_settings(self.project_root, ENVIRONMENT)

        message = str(raised.exception)
        self.assertIn("sources.toml", message)
        self.assertNotIn(ENVIRONMENT["APCA_API_KEY_ID"], message)
        self.assertNotIn(ENVIRONMENT["APCA_API_SECRET_KEY"], message)


if __name__ == "__main__":
    unittest.main()
