from __future__ import annotations

import json
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
        self.sources_text = (
            self.project_root / "config" / "sources.toml"
        ).read_text(encoding="utf-8")

    def _write_sources(self, **replacements: object) -> None:
        lines = self.sources_text.splitlines()
        for name, value in replacements.items():
            prefix = f"{name} = "
            matches = [
                index for index, line in enumerate(lines) if line.startswith(prefix)
            ]
            self.assertEqual(len(matches), 1, name)
            lines[matches[0]] = prefix + json.dumps(value)
        (self.project_root / "config" / "sources.toml").write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )

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

    def test_http_environment_values_reject_whitespace_controls_and_excess_length(self) -> None:
        invalid_values = (
            ("key surrounding whitespace", "APCA_API_KEY_ID", " key-id-canary"),
            (
                "secret CRLF",
                "APCA_API_SECRET_KEY",
                "secret-key-canary\r\ninjected",
            ),
            ("key C1 control", "APCA_API_KEY_ID", "key\x85id"),
            ("key non-ASCII", "APCA_API_KEY_ID", "clé-id"),
            ("secret emoji", "APCA_API_SECRET_KEY", "secret-🔐"),
            (
                "user agent null",
                "SEC_USER_AGENT",
                "Stock Monitor tests test@example.com\x00",
            ),
            (
                "user agent separator",
                "SEC_USER_AGENT",
                "Stock Monitor\u2028tests test@example.com",
            ),
            (
                "user agent non-ASCII",
                "SEC_USER_AGENT",
                "Stock Monitör tests test@example.com",
            ),
            ("key excessive length", "APCA_API_KEY_ID", "k" * 257),
            (
                "user agent excessive length",
                "SEC_USER_AGENT",
                "Stock Monitor " + "x" * 500 + " test@example.com",
            ),
        )
        for case, field, value in invalid_values:
            environ = {**ENVIRONMENT, field: value}
            with self.subTest(case=case):
                with self.assertRaises(ConfigurationError) as raised:
                    load_settings(self.project_root, environ)
                self.assertNotIn(value, str(raised.exception))

    def test_sec_user_agent_requires_application_identity_and_email(self) -> None:
        invalid_user_agents = (
            "test@example.com",
            "first@example.com second@example.com",
            "Stock Monitor Operations",
            "Stock Monitor invalid@example",
            "Stock Monitor a..b@example.com",
        )
        for user_agent in invalid_user_agents:
            environ = {**ENVIRONMENT, "SEC_USER_AGENT": user_agent}
            with self.subTest(user_agent=user_agent):
                with self.assertRaises(ConfigurationError) as raised:
                    load_settings(self.project_root, environ)
                self.assertIn("SEC_USER_AGENT", str(raised.exception))
                self.assertNotIn(user_agent, str(raised.exception))

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

    def test_alpaca_url_is_pinned_to_the_approved_market_data_origin(self) -> None:
        poisoned_urls = (
            "https://paper-api.alpaca.markets",
            "https://api.alpaca.markets",
            "https://data.alpaca.markets/v2/orders",
            "https://data.alpaca.markets?",
            "https://data.alpaca.markets#",
            "https://data.alpaca.markets.evil.example",
            "http://data.alpaca.markets",
            "https://data.alpaca.markets:444",
            "https://user:password@data.alpaca.markets",
        )
        for url in poisoned_urls:
            with self.subTest(url=url):
                self._write_sources(alpaca_market_data_url=url)
                with self.assertRaises(ConfigurationError):
                    load_settings(self.project_root, ENVIRONMENT)

    def test_approved_alpaca_origin_is_canonicalized(self) -> None:
        self._write_sources(
            alpaca_market_data_url="HTTPS://DATA.ALPACA.MARKETS.:443/"
        )

        settings = load_settings(self.project_root, ENVIRONMENT)

        self.assertEqual(
            settings.sources.alpaca_market_data_url,
            "https://data.alpaca.markets",
        )

    def test_source_urls_reject_ip_local_and_prohibited_hosts(self) -> None:
        poisoned_urls = (
            "https://127.0.0.1/submissions/",
            "https://127.1/submissions/",
            "https://0x7f.1/submissions/",
            "https://[::1]/submissions/",
            "https://[::1/submissions/",
            "https://localhost/submissions/",
            "https://api.robinhood.com/submissions/",
            "https://paper-api.alpaca.markets/submissions/",
        )
        for field in ("sec_submissions_url", "sec_archives_url"):
            for url in poisoned_urls:
                with self.subTest(field=field, url=url):
                    self._write_sources(**{field: url})
                    with self.assertRaises(ConfigurationError):
                        load_settings(self.project_root, ENVIRONMENT)

    def test_reference_hosts_are_canonicalized(self) -> None:
        self._write_sources(
            reference_hosts=["WWW.NYSE.COM.", "WWW.NASDAQTRADER.COM"]
        )

        settings = load_settings(self.project_root, ENVIRONMENT)

        self.assertEqual(
            settings.sources.reference_hosts,
            ("www.nyse.com", "www.nasdaqtrader.com"),
        )

    def test_reference_hosts_reject_non_dns_and_prohibited_values(self) -> None:
        poisoned_hosts = (
            "127.0.0.1",
            "127.1",
            "0x7f.1",
            "localhost",
            "api.robinhood.com",
            "paper-api.alpaca.markets",
            "https://www.nyse.com",
            "www.nyse.com/path",
            "www.nyse.com:443",
        )
        for host in poisoned_hosts:
            with self.subTest(host=host):
                self._write_sources(reference_hosts=[host])
                with self.assertRaises(ConfigurationError):
                    load_settings(self.project_root, ENVIRONMENT)


if __name__ == "__main__":
    unittest.main()
