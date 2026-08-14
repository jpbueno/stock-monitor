from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType

from stock_monitor.universe import UniverseError, UniverseSnapshot
from tests.support import load_json, universe_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISHED_UNIVERSE = PROJECT_ROOT / "data" / "universe" / "2026-08-14.json"
SP500_IT_URL = (
    "https://www.spglobal.com/spdji/en/indices/equity/"
    "sp-500-information-technology-sector/"
)
NASDAQ_100_URL = "https://www.nasdaq.com/docs/2026/05/04/NDX.pdf"
NASDAQ_TICK_URL = (
    "https://listingcenter.nasdaq.com/rulebook/nasdaq/rules/"
    "Nasdaq%20Equity%201"
)
NYSE_TICK_URL = "https://www.nyse.com/regulation/rules"


def _canonical_checksum(raw: dict[str, object]) -> str:
    payload = {key: value for key, value in raw.items() if key != "checksum"}
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _resign(raw: dict[str, object]) -> dict[str, object]:
    raw["checksum"] = _canonical_checksum(raw)
    return raw


class UniverseSnapshotTests(unittest.TestCase):
    def _load_mapping(
        self,
        raw: dict[str, object],
        *,
        as_of: date = date(2026, 8, 14),
    ) -> UniverseSnapshot:
        return UniverseSnapshot.from_mapping(raw, as_of=as_of)

    def test_published_universe_matches_reviewed_reference_fixture(self) -> None:
        published = json.loads(PUBLISHED_UNIVERSE.read_text(encoding="utf-8"))

        self.assertEqual(published, universe_fixture())

    def test_loads_current_manually_reviewed_snapshot_and_exact_seed(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )

        self.assertEqual(snapshot.effective_date, date(2026, 8, 14))
        self.assertEqual(snapshot.reviewed_at, date(2026, 8, 14))
        self.assertEqual(snapshot.review_by, date(2026, 9, 14))
        self.assertEqual(
            snapshot.acquisition_method,
            "manual_primary_source_review",
        )
        self.assertEqual(
            tuple(record.symbol for record in snapshot.eligible_records()),
            ("AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"),
        )
        self.assertIsInstance(snapshot.eligible_records(), tuple)

    def test_checksum_is_canonical_json_excluding_only_checksum(self) -> None:
        raw = universe_fixture()

        self.assertEqual(raw["checksum"], _canonical_checksum(raw))

    def test_corrupt_checksum_fixture_fails_closed(self) -> None:
        raw = load_json("reference/corrupt-universe.json")
        self.assertIsInstance(raw, dict)

        with self.assertRaises(UniverseError):
            UniverseSnapshot.from_mapping(raw, as_of=date(2026, 8, 14))

    def test_checksum_corruption_fails_before_record_use(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        first = records[0]
        self.assertIsInstance(first, dict)
        first["symbol"] = "MSFT"

        with self.assertRaises(UniverseError):
            self._load_mapping(raw)

    def test_stock_records_have_exact_membership_and_float_evidence(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )
        expected_float = {
            "AAPL": (14_600_000_000, 14_688_846_235),
            "AMD": (1_600_000_000, 1_615_325_777),
            "NVDA": (23_000_000_000, 23_354_828_293),
        }

        for symbol, (stored, derived) in expected_float.items():
            with self.subTest(symbol=symbol):
                record = snapshot.by_symbol[symbol]
                self.assertEqual(record.product_type, "common_stock")
                self.assertEqual(record.benchmark, "SPY")
                self.assertEqual(record.sector_etf, "XLK")
                self.assertEqual(record.free_float, stored)
                self.assertIsNotNone(record.float_derivation)
                assert record.float_derivation is not None
                self.assertEqual(record.float_derivation.derived_value, derived)
                self.assertEqual(record.float_derivation.stored_value, stored)
                self.assertEqual(
                    tuple(evidence.index for evidence in record.membership_sources),
                    ("sp_500_information_technology", "nasdaq_100"),
                )
                self.assertEqual(
                    tuple(evidence.url for evidence in record.membership_sources),
                    (SP500_IT_URL, NASDAQ_100_URL),
                )
                self.assertEqual(
                    tuple(
                        evidence.source_as_of
                        for evidence in record.membership_sources
                    ),
                    (date(2026, 6, 30), date(2026, 5, 1)),
                )

    def test_exact_float_derivation_operands_are_preserved(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )
        aapl = snapshot.by_symbol["AAPL"].float_derivation
        amd = snapshot.by_symbol["AMD"].float_derivation
        nvda = snapshot.by_symbol["NVDA"].float_derivation
        assert aapl is not None and amd is not None and nvda is not None

        self.assertEqual(
            aapl.operands,
            {
                "shares_outstanding": 14_697_926_000,
                "directors_and_officers_shares": 9_079_765,
            },
        )
        self.assertEqual(aapl.source_as_of, date(2026, 1, 2))
        self.assertEqual(
            amd.operands,
            {
                "nonaffiliate_market_value": 232_300_000_000,
                "share_price": "143.81",
            },
        )
        self.assertEqual(amd.corroborating_shares_outstanding, 1_630_410_843)
        self.assertEqual(
            amd.corroborating_shares_outstanding_as_of,
            date(2026, 1, 30),
        )
        self.assertEqual(
            nvda.operands,
            {
                "shares_outstanding": 24_312_141_810,
                "directors_and_officers_shares": 957_313_517,
            },
        )
        self.assertEqual(nvda.source_as_of, date(2026, 3, 23))
        for derivation in (aapl, amd, nvda):
            self.assertTrue(derivation.rounded_down)
            self.assertEqual(derivation.rounding, "conservative_round_down")

    def test_float_derivations_preserve_fact_specific_operand_dates(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )
        aapl = snapshot.by_symbol["AAPL"].float_derivation
        amd = snapshot.by_symbol["AMD"].float_derivation
        nvda = snapshot.by_symbol["NVDA"].float_derivation
        assert aapl is not None and amd is not None and nvda is not None

        self.assertEqual(
            aapl.operand_source_dates,
            {
                "shares_outstanding": date(2026, 1, 2),
                "directors_and_officers_shares": date(2026, 1, 2),
            },
        )
        self.assertEqual(
            amd.operand_source_dates,
            {
                "nonaffiliate_market_value": date(2025, 6, 28),
                "share_price": date(2025, 6, 27),
            },
        )
        self.assertEqual(
            nvda.operand_source_dates,
            {
                "shares_outstanding": date(2026, 3, 23),
                "directors_and_officers_shares": date(2026, 3, 23),
            },
        )

    def test_etfs_have_explicit_null_sector_mapping_and_sponsor_evidence(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )
        expected_primary_dates = {
            "SPY": date(2026, 8, 14),
            "VTI": date(2026, 4, 28),
            "XLK": date(2026, 8, 14),
            "QQQ": date(2026, 3, 31),
        }

        for symbol, source_as_of in expected_primary_dates.items():
            with self.subTest(symbol=symbol):
                record = snapshot.by_symbol[symbol]
                self.assertEqual(record.product_type, "etf")
                self.assertIsNone(record.sector_etf)
                self.assertIsNone(record.free_float)
                self.assertIsNone(record.float_derivation)
                self.assertFalse(record.leveraged)
                self.assertFalse(record.inverse)
                self.assertEqual(record.source_as_of, source_as_of)
                self.assertGreaterEqual(len(record.sponsor_sources), 1)
                self.assertEqual(
                    record.objective_classification_method,
                    "manual_official_sponsor_objective_review_same_direction",
                )

        self.assertEqual(len(snapshot.by_symbol["VTI"].sponsor_sources), 2)

    def test_benchmark_and_support_roles_are_distinct_and_complete(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )

        self.assertEqual(snapshot.regime_support_symbols, ("SPY", "QQQ"))
        self.assertEqual(snapshot.by_symbol["QQQ"].benchmark, "SPY")
        self.assertIn("regime_benchmark", snapshot.by_symbol["QQQ"].support_roles)
        self.assertEqual(snapshot.by_symbol["SPY"].benchmark, "VTI")
        self.assertIn("market_benchmark", snapshot.by_symbol["SPY"].support_roles)
        self.assertIn("regime_benchmark", snapshot.by_symbol["SPY"].support_roles)
        self.assertIn("spy_benchmark", snapshot.by_symbol["VTI"].support_roles)
        self.assertIn("sector_benchmark", snapshot.by_symbol["XLK"].support_roles)

    def test_tick_sizes_are_exact_positive_decimals_with_reviewed_sources(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )

        for record in snapshot.records:
            with self.subTest(symbol=record.symbol):
                self.assertEqual(record.tick_size, Decimal("0.01"))
                self.assertEqual(
                    record.tick_classification,
                    "reviewed_conditional_price_at_or_above_1_usd",
                )
                self.assertEqual(record.tick_source.source_as_of, date(2026, 8, 14))
                expected_url = (
                    NASDAQ_TICK_URL
                    if record.listing_venue == "NASDAQ"
                    else NYSE_TICK_URL
                )
                self.assertEqual(record.tick_source.url, expected_url)

        self.assertEqual(
            {source.scope for source in snapshot.tick_policy_sources},
            {
                "nasdaq_listed",
                "nyse_listed",
                "sec_subpenny_faq",
                "sec_minimum_increment_postponement",
            },
        )

    def test_static_snapshot_does_not_embed_runtime_liquidity_gates(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)

        for record in records:
            self.assertIsInstance(record, dict)
            for runtime_field in (
                "last_price",
                "average_dollar_volume",
                "median_share_volume",
                "spread",
                "halted",
            ):
                self.assertNotIn(runtime_field, record)

    def test_review_fails_closed_before_effective_date_or_after_review_by(self) -> None:
        for as_of in (date(2026, 8, 13), date(2026, 9, 15)):
            with self.subTest(as_of=as_of), self.assertRaises(UniverseError):
                UniverseSnapshot.load(PUBLISHED_UNIVERSE, as_of=as_of)

    def test_review_metadata_must_be_current_and_manual(self) -> None:
        for field, value in (
            ("reviewed_at", "2026-08-13"),
            ("review_by", "2026-09-15"),
            ("acquisition_method", "automated_scrape"),
        ):
            raw = universe_fixture()
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_duplicate_symbol_is_rejected(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        records.append(records[0])

        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_missing_or_conflicting_stock_provenance_is_rejected(self) -> None:
        cases = (
            ("missing membership", lambda record: record.pop("membership_sources")),
            (
                "membership conflict",
                lambda record: record["membership_sources"][0].update(  # type: ignore[index,union-attr]
                    {"member": False}
                ),
            ),
            ("missing float", lambda record: record.pop("free_float")),
            ("missing float source", lambda record: record.pop("float_source")),
            (
                "source conflict",
                lambda record: record.update(
                    {"source_url": "https://www.sec.gov/Archives/unrelated.htm"}
                ),
            ),
        )
        for case, mutate in cases:
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = records[0]
            self.assertIsInstance(record, dict)
            mutate(record)
            with self.subTest(case=case), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_float_at_or_below_threshold_and_bad_derivation_are_rejected(self) -> None:
        for case, mutation in (
            ("at threshold", {"free_float": 50_000_000}),
            ("below threshold", {"free_float": 49_999_999}),
        ):
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = records[0]
            self.assertIsInstance(record, dict)
            float_source = record["float_source"]
            self.assertIsInstance(float_source, dict)
            record.update(mutation)
            float_source["stored_value"] = mutation["free_float"]
            with self.subTest(case=case), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        record = records[0]
        self.assertIsInstance(record, dict)
        float_source = record["float_source"]
        self.assertIsInstance(float_source, dict)
        float_source["derived_value"] = 14_688_846_236
        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_stock_missing_sector_or_etf_with_stock_fields_is_rejected(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        stock = records[0]
        self.assertIsInstance(stock, dict)
        stock["sector_etf"] = None
        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        etf = next(
            record
            for record in records
            if isinstance(record, dict) and record.get("symbol") == "SPY"
        )
        etf["free_float"] = 1_000_000_000
        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_leveraged_inverse_or_disabled_record_is_rejected(self) -> None:
        for field, value in (
            ("leveraged", True),
            ("inverse", True),
            ("enabled", False),
        ):
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = records[3]
            self.assertIsInstance(record, dict)
            record[field] = value
            with self.subTest(field=field), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_missing_benchmark_or_support_distinction_is_rejected(self) -> None:
        cases = (
            ("missing benchmark", "QQQ", "benchmark"),
            ("missing regime role", "QQQ", "regime_benchmark"),
            ("missing market role", "SPY", "market_benchmark"),
            ("missing spy role", "VTI", "spy_benchmark"),
            ("missing sector role", "XLK", "sector_benchmark"),
        )
        for case, symbol, value in cases:
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = next(
                item
                for item in records
                if isinstance(item, dict) and item.get("symbol") == symbol
            )
            if value == "benchmark":
                record.pop("benchmark")
            else:
                roles = record["support_roles"]
                self.assertIsInstance(roles, list)
                roles.remove(value)
            with self.subTest(case=case), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

        raw = universe_fixture()
        policy = raw["benchmark_policy"]
        self.assertIsInstance(policy, dict)
        policy["regime_support_symbols"] = ["SPY"]
        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_bad_tick_value_source_or_classification_is_rejected(self) -> None:
        cases = (
            ("non string", "tick_size", 0.01),
            ("zero", "tick_size", "0"),
            ("negative", "tick_size", "-0.01"),
            ("not finite", "tick_size", "NaN"),
            (
                "wrong source",
                "tick_source_url",
                "https://www.nyse.com/regulation/rules",
            ),
            ("missing condition", "tick_classification", "unconditional"),
        )
        for case, field, value in cases:
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = records[0]
            self.assertIsInstance(record, dict)
            record[field] = value
            with self.subTest(case=case), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_invalid_url_scheme_credentials_or_host_is_rejected(self) -> None:
        invalid_urls = (
            "http://www.sec.gov/Archives/example.htm",
            "https://user:password@www.sec.gov/Archives/example.htm",
            "https://www.sec.gov.evil.example/Archives/example.htm",
            "https://evil.example/evidence",
        )
        for url in invalid_urls:
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = records[0]
            self.assertIsInstance(record, dict)
            record["source_url"] = url
            float_source = record["float_source"]
            self.assertIsInstance(float_source, dict)
            sources = float_source["sources"]
            self.assertIsInstance(sources, list)
            first_source = sources[0]
            self.assertIsInstance(first_source, dict)
            first_source["url"] = url
            with self.subTest(url=url), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_snapshot_and_nested_collections_are_immutable(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 14),
        )
        record = snapshot.by_symbol["AAPL"]
        assert record.float_derivation is not None

        self.assertIsInstance(snapshot.by_symbol, MappingProxyType)
        self.assertIsInstance(record.float_derivation.operands, MappingProxyType)
        with self.assertRaises(TypeError):
            snapshot.by_symbol["AAPL"] = record  # type: ignore[index]
        with self.assertRaises(TypeError):
            record.float_derivation.operands["shares_outstanding"] = 1  # type: ignore[index]
        with self.assertRaises(FrozenInstanceError):
            record.symbol = "MSFT"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
