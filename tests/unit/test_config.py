from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from pathlib import Path

import stock_monitor.config as config_module
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

    def _write_fees(self, document: object) -> Path:
        path = self.project_root / "config" / "fees.json"
        path.write_text(
            json.dumps(document, indent=2) + "\n",
            encoding="utf-8",
        )
        return path

    def _reviewed_fees(self, **replacements: object) -> dict[str, object]:
        document: dict[str, object] = {
            "schema_version": 1,
            "status": "reviewed",
            "schedule_id": "TEST_OPTION_FEES_V1",
            "effective_session": "2026-08-18",
            "reviewed_at": "2026-08-18T16:00:00.000000Z",
            "currency": "USD",
            "contract_multiplier": 100,
            "entry_fee_per_contract_micros": 10_000,
            "exit_fee_per_contract_micros": 20_000,
            "close_fee_reserve_per_contract_micros": 30_000,
            "source_sha256": "a" * 64,
        }
        document.update(replacements)
        return document

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

    def test_checked_in_fee_review_marker_blocks_phase2_but_not_phase1_settings(
        self,
    ) -> None:
        loader = getattr(config_module, "load_fee_schedule", None)
        self.assertIsNotNone(loader, "typed Phase 2 fee loader is missing")
        assert loader is not None

        settings = load_settings(self.project_root, ENVIRONMENT)
        with self.assertRaisesRegex(ConfigurationError, "explicit.*review"):
            loader(settings.fees_path)

    def test_reviewed_fee_schedule_loads_as_an_immutable_typed_value(self) -> None:
        loader = getattr(config_module, "load_fee_schedule", None)
        schedule_type = getattr(config_module, "FeeSchedule", None)
        self.assertIsNotNone(loader, "typed Phase 2 fee loader is missing")
        self.assertIsNotNone(schedule_type, "typed Phase 2 fee schedule is missing")
        assert loader is not None and schedule_type is not None
        document = self._reviewed_fees()
        path = self._write_fees(document)

        schedule = loader(path)

        self.assertIsInstance(schedule, schedule_type)
        self.assertEqual(schedule.schedule_id, "TEST_OPTION_FEES_V1")
        self.assertEqual(schedule.effective_session.isoformat(), "2026-08-18")
        self.assertEqual(
            schedule.reviewed_at.isoformat(),
            "2026-08-18T16:00:00+00:00",
        )
        self.assertEqual(schedule.currency, "USD")
        self.assertEqual(schedule.contract_multiplier, 100)
        self.assertEqual(schedule.entry_fee_per_contract_micros, 10_000)
        self.assertEqual(schedule.exit_fee_per_contract_micros, 20_000)
        self.assertEqual(schedule.close_fee_reserve_per_contract_micros, 30_000)
        self.assertEqual(schedule.source_sha256, "a" * 64)
        canonical = json.dumps(
            document,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self.assertEqual(schedule.digest, hashlib.sha256(canonical).hexdigest())
        with self.assertRaises((AttributeError, TypeError)):
            schedule.contract_multiplier = 1

    def test_fee_schedule_rejects_unreviewed_unknown_or_unsafe_values(self) -> None:
        loader = getattr(config_module, "load_fee_schedule", None)
        self.assertIsNotNone(loader, "typed Phase 2 fee loader is missing")
        assert loader is not None
        cases = (
            ("boolean schema version", {"schema_version": True}),
            ("wrong schema version", {"schema_version": 2}),
            ("string schema version", {"schema_version": "1"}),
            ("unreviewed", {"status": "operator_review_required"}),
            ("unknown field", {"unexpected": "value"}),
            ("boolean integer", {"entry_fee_per_contract_micros": True}),
            ("negative fee", {"exit_fee_per_contract_micros": -1}),
            ("zero exit fee", {"exit_fee_per_contract_micros": 0}),
            ("wrong multiplier", {"contract_multiplier": 1}),
            (
                "under-reserved close",
                {
                    "exit_fee_per_contract_micros": 30_000,
                    "close_fee_reserve_per_contract_micros": 20_000,
                },
            ),
            ("noncanonical date", {"effective_session": "2026-8-18"}),
            (
                "noncanonical timestamp",
                {"reviewed_at": "2026-08-18T12:00:00-04:00"},
            ),
            ("unsafe schedule id", {"schedule_id": "option fees v1"}),
            ("wrong currency", {"currency": "EUR"}),
            ("uppercase source hash", {"source_sha256": "A" * 64}),
        )
        for case, replacements in cases:
            with self.subTest(case=case):
                path = self._write_fees(self._reviewed_fees(**replacements))
                with self.assertRaises(ConfigurationError) as raised:
                    loader(path)
                self.assertNotIn("key-id-canary", str(raised.exception))
                self.assertNotIn("secret-key-canary", str(raised.exception))

    def test_fee_schedule_authority_rejects_raw_copied_and_mutated_values(self) -> None:
        loader = getattr(config_module, "load_fee_schedule", None)
        verifier = getattr(config_module, "is_reviewed_fee_schedule", None)
        self.assertIsNotNone(loader, "typed Phase 2 fee loader is missing")
        self.assertIsNotNone(verifier, "fee schedule authority verifier is missing")
        assert loader is not None and verifier is not None
        self.assertIsNone(
            getattr(config_module, "_issue_fee_schedule", None),
            "caller-accessible raw fee schedule registrar must not exist",
        )
        document = self._reviewed_fees()
        path = self._write_fees(document)

        issued = loader(path)

        self.assertTrue(verifier(issued))
        self.assertFalse(verifier(document))
        self.assertFalse(verifier(copy.copy(issued)))
        object.__setattr__(
            issued,
            "entry_fee_per_contract_micros",
            issued.entry_fee_per_contract_micros + 1,
        )
        self.assertFalse(verifier(issued))
        self.assertTrue(verifier(loader(path)))

    def test_archived_fee_reissuer_accepts_only_a_journal_source_capability(
        self,
    ) -> None:
        reissuer = getattr(config_module, "_reissue_archived_fee_schedule", None)
        self.assertIsNotNone(reissuer, "archived fee source reissuer is missing")
        assert reissuer is not None
        reviewed_bytes = json.dumps(
            self._reviewed_fees(),
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        class FakeArchivedSource:
            def __init__(self) -> None:
                self.reviewed_bytes = reviewed_bytes

            def _is_current_phase2_fee_schedule_source(self) -> bool:
                return True

        for raw in (reviewed_bytes, self._reviewed_fees(), FakeArchivedSource()):
            with self.subTest(raw=type(raw).__name__):
                with self.assertRaises(ConfigurationError):
                    reissuer(raw)

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

    def test_sec_origins_are_pinned_to_official_paths_not_only_safe_hosts(self) -> None:
        poisoned = {
            "sec_submissions_url": (
                "https://www.sec.gov/submissions/",
                "https://data.sec.gov/other/",
                "https://data.sec.gov/submissions",
            ),
            "sec_archives_url": (
                "https://data.sec.gov/Archives/",
                "https://www.sec.gov/other/",
                "https://www.sec.gov/Archives",
            ),
        }
        for field, values in poisoned.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    self._write_sources(**{field: value})
                    with self.assertRaises(ConfigurationError):
                        load_settings(self.project_root, ENVIRONMENT)

    def test_sec_origins_load_only_as_exact_official_values(self) -> None:
        settings = load_settings(self.project_root, ENVIRONMENT)
        self.assertEqual(
            settings.sources.sec_submissions_url,
            "https://data.sec.gov/submissions/",
        )
        self.assertEqual(
            settings.sources.sec_archives_url,
            "https://www.sec.gov/Archives/",
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

    def test_reference_urls_are_exact_reviewed_https_urls_on_reference_hosts(self) -> None:
        settings = load_settings(self.project_root, ENVIRONMENT)
        self.assertEqual(
            settings.sources.reference_urls,
            (
                "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
                "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
                "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
                "https://www.nyse.com/api/notifications/public/alerts?2=3",
                "https://www.nyse.com/trade/hours-calendars",
            ),
        )
        self.assertEqual(
            tuple(source.role for source in settings.sources.reference_sources),
            (
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "CROSS_CHECK_CALENDAR",
                "OPERATIONAL_STATUS",
                "PRIMARY_CALENDAR",
            ),
        )
        self.assertEqual(
            tuple(source.url for source in settings.sources.reference_sources),
            settings.sources.reference_urls,
        )

        poisoned = (
            "http://www.nasdaqtrader.com/Trader.aspx?id=TraderAlerts",
            "https://www.nasdaqtrader.com/Trader.aspx?id=TraderAlerts#fragment",
            "https://www.nasdaqtrader.com/Trader.aspx?access_token=canary",
            "https://www.nasdaqtrader.com/Trader.aspx?api-key=canary",
            "https://www.nasdaqtrader.com/Trader.aspx?key.id=canary",
            "https://www.nasdaqtrader.com/Trader.aspx?client-secret=canary",
            "https://www.nasdaqtrader.com//evil.example/path",
            "https://evil.example/Trader.aspx?id=TraderAlerts",
            "https://user:secret@www.nasdaqtrader.com/Trader.aspx?id=TraderAlerts",
        )
        reviewed_urls = list(settings.sources.reference_urls)
        for url in poisoned:
            with self.subTest(url=url):
                self._write_sources(reference_urls=[url, *reviewed_urls[1:]])
                with self.assertRaises(ConfigurationError):
                    load_settings(self.project_root, ENVIRONMENT)

    def test_reference_role_binding_is_complete_unique_and_scoped_roles_need_manifest(self) -> None:
        settings = load_settings(self.project_root, ENVIRONMENT)
        urls = list(settings.sources.reference_urls)
        roles = [source.role for source in settings.sources.reference_sources]
        feeds = [source.feed for source in settings.sources.reference_sources]

        for poisoned_roles in (
            roles[:-1],
            [*roles[:-1], roles[0]],
            [*roles[:-1], "UNREVIEWED_ROLE"],
        ):
            with self.subTest(roles=poisoned_roles):
                self._write_sources(reference_roles=poisoned_roles)
                with self.assertRaises(ConfigurationError):
                    load_settings(self.project_root, ENVIRONMENT)

        self._write_sources(
            reference_hosts=[
                "www.nyse.com",
                "www.nasdaqtrader.com",
                "ir.example.com",
            ],
            reference_urls=[*urls, "https://ir.example.com/"],
            reference_roles=[*roles, "ISSUER_IR:EXM"],
            reference_feeds=[*feeds, "issuer-ir-primary"],
        )
        with self.assertRaises(ConfigurationError):
            load_settings(self.project_root, ENVIRONMENT)

    def test_scoped_reference_role_rejects_unreviewed_subject_origin_pair(self) -> None:
        settings = load_settings(self.project_root, ENVIRONMENT)
        urls = list(settings.sources.reference_urls)
        roles = [source.role for source in settings.sources.reference_sources]
        feeds = [source.feed for source in settings.sources.reference_sources]
        self._write_sources(
            reference_hosts=[
                "www.nyse.com",
                "www.nasdaqtrader.com",
                "attacker.example",
            ],
            reference_urls=[*urls, "https://attacker.example/"],
            reference_roles=[*roles, "ISSUER_IR:AAPL"],
            reference_feeds=[*feeds, "issuer-ir-primary"],
        )

        with self.assertRaises(ConfigurationError):
            load_settings(self.project_root, ENVIRONMENT)

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
