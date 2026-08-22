from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from copy import copy
from dataclasses import FrozenInstanceError, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from unittest import mock

from stock_monitor import universe as universe_module
from stock_monitor.universe import (
    CURRENT_UNIVERSE_SHA256,
    SourceEvidence,
    UniverseError,
    UniverseSnapshot,
    is_verified_universe_snapshot,
    load_current_universe,
    load_universe_release,
)
from tests.support import load_json, universe_fixture as _legacy_universe_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISHED_UNIVERSE = PROJECT_ROOT / "data" / "universe" / "2026-08-22.json"
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


_STOCK_IDENTITY_METADATA = {
    "AAPL": {
        "issuer_cik": "0000320193",
        "issuer_cik_source_url": (
            "https://data.sec.gov/submissions/CIK0000320193.json"
        ),
        "initial_listing_date": "1980-12-12",
        "initial_listing_date_kind": "initial_public_trading_date",
        "initial_listing_source_url": (
            "https://investor.apple.com/investor-relations/faq/default.aspx"
        ),
    },
    "AMD": {
        "issuer_cik": "0000002488",
        "issuer_cik_source_url": (
            "https://data.sec.gov/submissions/CIK0000002488.json"
        ),
        "initial_listing_date": "1979-10-15",
        "initial_listing_date_kind": "first_exchange_listing_date",
        "initial_listing_source_url": "https://ir.amd.com/contacts-faq/faq",
    },
    "NVDA": {
        "issuer_cik": "0001045810",
        "issuer_cik_source_url": (
            "https://data.sec.gov/submissions/CIK0001045810.json"
        ),
        "initial_listing_date": "1999-01-22",
        "initial_listing_date_kind": "initial_public_trading_date",
        "initial_listing_source_url": (
            "https://investor.nvidia.com/investor-resources/faqs/default.aspx"
        ),
    },
}
_ETF_LISTING_METADATA = {
    "QQQ": (
        "1999-03-10",
        "exchange_listing_date",
        "https://www.sec.gov/Archives/edgar/data/1067839/"
        "000110465910002985/a09-28465_1485bpos.htm",
        "2010-01-26",
    ),
    "SPY": (
        "1993-01-22",
        "exchange_listing_date",
        "https://www.ssga.com/us/en/intermediary/etfs/"
        "state-street-spdr-sp-500-etf-trust-spy",
        "2026-08-22",
    ),
    "VTI": (
        "2001-05-24",
        "etf_share_class_launch_proxy",
        "https://personal1.vanguard.com/pub/Pdf/p961.pdf",
        "2026-04-28",
    ),
    "XLK": (
        "1998-12-22",
        "exchange_listing_date",
        "https://www.ssga.com/us/en/intermediary/etfs/"
        "state-street-technology-select-sector-spdr-etf-xlk",
        "2026-08-22",
    ),
}


def universe_fixture() -> dict[str, object]:
    """Return synthetic new-release material without rewriting historical data."""
    raw = _legacy_universe_fixture()
    raw["effective_date"] = "2026-08-22"
    raw["reviewed_at"] = "2026-08-22"
    raw["review_by"] = "2026-09-22"
    tick_policy = raw["tick_policy"]
    assert isinstance(tick_policy, dict)
    tick_policy["reviewed_at"] = "2026-08-22"
    tick_sources = tick_policy["sources"]
    assert isinstance(tick_sources, list)
    for source in tick_sources:
        assert isinstance(source, dict)
        source["source_as_of"] = "2026-08-22"
    records = raw["records"]
    assert isinstance(records, list)
    for item in records:
        assert isinstance(item, dict)
        symbol = item["symbol"]
        assert isinstance(symbol, str)
        item["reviewed_at"] = "2026-08-22"
        item["tick_source_as_of"] = "2026-08-22"
        if symbol == "QQQ":
            qqq_url = "https://www.invesco.com/qqq-etf/en/home.html"
            item["source_url"] = qqq_url
            item["source_as_of"] = "2026-08-22"
            item["sponsor_sources"] = [
                {"url": qqq_url, "source_as_of": "2026-08-22"}
            ]
        if item["product_type"] == "common_stock":
            metadata = _STOCK_IDENTITY_METADATA[symbol]
            for name, value in metadata.items():
                item[name] = value
            item["issuer_cik_source_as_of"] = "2026-08-22"
            item["initial_listing_source_as_of"] = "2026-08-22"
        else:
            listing_date, listing_kind, listing_url, listing_source_as_of = (
                _ETF_LISTING_METADATA[symbol]
            )
            item["issuer_cik"] = None
            item["issuer_cik_source_url"] = None
            item["issuer_cik_source_as_of"] = None
            item["initial_listing_date"] = listing_date
            item["initial_listing_date_kind"] = listing_kind
            item["initial_listing_source_url"] = listing_url
            item["initial_listing_source_as_of"] = listing_source_as_of
    return _resign(raw)


class UniverseSnapshotTests(unittest.TestCase):
    def _load_mapping(
        self,
        raw: dict[str, object],
        *,
        as_of: date = date(2026, 8, 22),
    ) -> UniverseSnapshot:
        return UniverseSnapshot.from_mapping(raw, as_of=as_of)

    def test_explicit_release_loader_preserves_a_historical_external_pin(
        self,
    ) -> None:
        path = (
            PROJECT_ROOT
            / "tests"
            / "fixtures"
            / "reference"
            / "universe-2026-08-14.json"
        )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        snapshot = load_universe_release(
            path,
            expected_sha256=digest,
            as_of=date(2026, 8, 14),
        )

        self.assertTrue(is_verified_universe_snapshot(snapshot))
        self.assertEqual(snapshot._release_pin, digest)
        self.assertNotEqual(snapshot._release_pin, CURRENT_UNIVERSE_SHA256)
        with self.assertRaises(UniverseError):
            load_universe_release(
                path,
                expected_sha256="0" * 64,
                as_of=date(2026, 8, 14),
            )

    def test_published_universe_matches_reviewed_reference_fixture(self) -> None:
        published = json.loads(PUBLISHED_UNIVERSE.read_text(encoding="utf-8"))

        self.assertEqual(published, universe_fixture())

    def test_all_instruments_require_identity_and_listing_provenance(self) -> None:
        snapshot = self._load_mapping(universe_fixture())

        self.assertEqual(snapshot.by_symbol["AAPL"].issuer_cik, "0000320193")
        self.assertEqual(snapshot.by_symbol["AMD"].issuer_cik, "0000002488")
        self.assertEqual(snapshot.by_symbol["NVDA"].issuer_cik, "0001045810")
        self.assertEqual(
            snapshot.by_symbol["AMD"].initial_listing_date,
            date(1979, 10, 15),
        )
        self.assertEqual(
            snapshot.by_symbol["AMD"].initial_listing_date_kind,
            "first_exchange_listing_date",
        )
        self.assertIsInstance(
            snapshot.by_symbol["AMD"].issuer_cik_source,
            SourceEvidence,
        )
        self.assertIsInstance(
            snapshot.by_symbol["AMD"].initial_listing_source,
            SourceEvidence,
        )
        self.assertEqual(
            snapshot.by_symbol["VTI"].initial_listing_date_kind,
            "etf_share_class_launch_proxy",
        )
        for symbol in ("QQQ", "SPY", "VTI", "XLK"):
            with self.subTest(symbol=symbol):
                record = snapshot.by_symbol[symbol]
                self.assertIsNone(record.issuer_cik)
                self.assertIsNone(record.issuer_cik_source)
                self.assertLessEqual(record.initial_listing_date, record.reviewed_at)

    def test_identity_and_listing_metadata_fail_closed(self) -> None:
        def swap_primary_filing(record: dict[str, object]) -> None:
            other_issuer_url = (
                "https://www.sec.gov/Archives/edgar/data/2488/"
                "000000248826000018/amd-20251227.htm"
            )
            record["source_url"] = other_issuer_url
            float_source = record["float_source"]
            assert isinstance(float_source, dict)
            sources = float_source["sources"]
            assert isinstance(sources, list)
            primary = sources[0]
            assert isinstance(primary, dict)
            primary["url"] = other_issuer_url

        cases = (
            ("missing CIK", "AAPL", lambda record: record.pop("issuer_cik")),
            (
                "malformed CIK",
                "AAPL",
                lambda record: record.update({"issuer_cik": "320193"}),
            ),
            (
                "CIK source mismatch",
                "AAPL",
                lambda record: record.update(
                    {
                        "issuer_cik_source_url": (
                            "https://data.sec.gov/submissions/CIK0000002488.json"
                        )
                    }
                ),
            ),
            (
                "CIK and submissions source swapped across issuers",
                "AAPL",
                lambda record: record.update(
                    {
                        "issuer_cik": "0000002488",
                        "issuer_cik_source_url": (
                            "https://data.sec.gov/submissions/CIK0000002488.json"
                        ),
                    }
                ),
            ),
            (
                "primary SEC filing swapped across issuers",
                "AAPL",
                swap_primary_filing,
            ),
            (
                "ETF CIK",
                "SPY",
                lambda record: record.update(
                    {
                        "issuer_cik": "0000320193",
                        "issuer_cik_source_url": (
                            "https://data.sec.gov/submissions/CIK0000320193.json"
                        ),
                        "issuer_cik_source_as_of": "2026-08-13",
                    }
                ),
            ),
            (
                "future listing",
                "NVDA",
                lambda record: record.update(
                    {"initial_listing_date": "2026-08-23"}
                ),
            ),
            (
                "wrong historical listing",
                "AAPL",
                lambda record: record.update(
                    {"initial_listing_date": "1980-12-13"}
                ),
            ),
            (
                "wrong issuer listing kind",
                "AMD",
                lambda record: record.update(
                    {"initial_listing_date_kind": "initial_public_trading_date"}
                ),
            ),
            (
                "unapproved listing source",
                "AMD",
                lambda record: record.update(
                    {"initial_listing_source_url": "https://example.com/listing"}
                ),
            ),
            (
                "incompatible listing kind",
                "VTI",
                lambda record: record.update(
                    {"initial_listing_date_kind": "initial_public_trading_date"}
                ),
            ),
        )
        for case, symbol, mutate in cases:
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            record = next(
                item
                for item in records
                if isinstance(item, dict) and item.get("symbol") == symbol
            )
            mutate(record)
            with self.subTest(case=case), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_identity_metadata_is_bound_to_verified_snapshot_fingerprint(self) -> None:
        raw = universe_fixture()
        payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "data" / "universe" / "2026-08-22.json"
            path.parent.mkdir(parents=True)
            path.write_bytes(payload)
            with mock.patch.object(
                universe_module,
                "CURRENT_UNIVERSE_SHA256",
                digest,
            ):
                snapshot = load_current_universe(
                    root,
                    as_of=date(2026, 8, 22),
                )
                changed = replace(
                    snapshot.by_symbol["AAPL"],
                    issuer_cik="0000002488",
                )
                object.__setattr__(
                    snapshot,
                    "records",
                    (changed,) + snapshot.records[1:],
                )
                object.__setattr__(
                    snapshot,
                    "by_symbol",
                    MappingProxyType({**snapshot.by_symbol, "AAPL": changed}),
                )
                object.__setattr__(
                    snapshot,
                    "_snapshot_digest",
                    universe_module._snapshot_fingerprint(snapshot),
                )

                self.assertFalse(is_verified_universe_snapshot(snapshot))

    def test_loads_current_manually_reviewed_snapshot_and_exact_seed(self) -> None:
        snapshot = load_current_universe(
            PROJECT_ROOT,
            as_of=date(2026, 8, 22),
        )

        self.assertEqual(snapshot.effective_date, date(2026, 8, 22))
        self.assertEqual(snapshot.reviewed_at, date(2026, 8, 22))
        self.assertEqual(snapshot.review_by, date(2026, 9, 22))
        self.assertEqual(
            snapshot.acquisition_method,
            "manual_primary_source_review",
        )
        self.assertEqual(
            tuple(record.symbol for record in snapshot.eligible_records()),
            ("AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"),
        )
        self.assertIsInstance(snapshot.eligible_records(), tuple)
        self.assertTrue(is_verified_universe_snapshot(snapshot))

    def test_generic_validators_cannot_mint_production_universe_authority(self) -> None:
        raw = universe_fixture()
        loaded = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 22),
        )
        decoded = UniverseSnapshot.from_mapping(raw, as_of=date(2026, 8, 22))

        self.assertFalse(is_verified_universe_snapshot(loaded))
        self.assertFalse(is_verified_universe_snapshot(decoded))

    def test_current_universe_release_pin_rejects_alternate_reviewed_content(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        raw["records"] = [
            record for record in records if record["symbol"] != "NVDA"
        ]
        benchmark = raw["benchmark_policy"]
        self.assertIsInstance(benchmark, dict)
        sector_mapping = benchmark["sector_mapping"]
        self.assertIsInstance(sector_mapping, dict)
        del sector_mapping["NVDA"]
        payload = json.dumps(_resign(raw), sort_keys=True).encode("utf-8")

        with tempfile.TemporaryDirectory() as directory:
            project_root = Path(directory)
            manifest = project_root / "data" / "universe" / "2026-08-22.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_bytes(payload)
            generic = UniverseSnapshot.load(manifest, as_of=date(2026, 8, 22))
            self.assertFalse(is_verified_universe_snapshot(generic))
            with self.assertRaises(UniverseError):
                load_current_universe(project_root, as_of=date(2026, 8, 22))

        with mock.patch.object(Path, "read_bytes", return_value=payload):
            with self.assertRaises(UniverseError):
                load_current_universe(PROJECT_ROOT, as_of=date(2026, 8, 22))

        self.assertRegex(CURRENT_UNIVERSE_SHA256, r"[0-9a-f]{64}\Z")

    def test_direct_and_replaced_snapshots_are_not_loader_verified(self) -> None:
        snapshot = load_current_universe(
            PROJECT_ROOT,
            as_of=date(2026, 8, 22),
        )
        aapl = snapshot.by_symbol["AAPL"]

        direct = UniverseSnapshot(
            effective_date=snapshot.effective_date,
            reviewed_at=snapshot.reviewed_at,
            review_by=snapshot.review_by,
            acquisition_method=snapshot.acquisition_method,
            checksum=snapshot.checksum,
            records=snapshot.records,
            by_symbol=snapshot.by_symbol,
            regime_support_symbols=snapshot.regime_support_symbols,
            sector_mapping=snapshot.sector_mapping,
            tick_policy_sources=snapshot.tick_policy_sources,
        )
        subset = replace(
            snapshot,
            records=(aapl,),
            by_symbol=MappingProxyType({"AAPL": aapl}),
            sector_mapping=MappingProxyType({"AAPL": "XLK"}),
        )
        changed_tick = replace(aapl, tick_size=Decimal("1"))
        mutated = replace(
            snapshot,
            records=(changed_tick,) + snapshot.records[1:],
            by_symbol=MappingProxyType(
                {**snapshot.by_symbol, "AAPL": changed_tick}
            ),
        )

        self.assertFalse(is_verified_universe_snapshot(direct))
        self.assertFalse(is_verified_universe_snapshot(subset))
        self.assertFalse(is_verified_universe_snapshot(mutated))

    def test_copied_or_mutated_release_cannot_be_resealed_with_private_digest(self) -> None:
        for case, forged in (
            (
                "copy",
                copy(
                    load_current_universe(
                        PROJECT_ROOT,
                        as_of=date(2026, 8, 22),
                    )
                ),
            ),
            (
                "issued-object",
                load_current_universe(
                    PROJECT_ROOT,
                    as_of=date(2026, 8, 22),
                ),
            ),
        ):
            with self.subTest(case=case):
                aapl = forged.by_symbol["AAPL"]
                object.__setattr__(forged, "records", (aapl,))
                object.__setattr__(
                    forged,
                    "by_symbol",
                    MappingProxyType({"AAPL": aapl}),
                )
                object.__setattr__(
                    forged,
                    "sector_mapping",
                    MappingProxyType({"AAPL": "XLK"}),
                )
                object.__setattr__(
                    forged,
                    "_snapshot_digest",
                    universe_module._snapshot_fingerprint(forged),
                )

                self.assertFalse(is_verified_universe_snapshot(forged))

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
            as_of=date(2026, 8, 22),
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
            as_of=date(2026, 8, 22),
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
            as_of=date(2026, 8, 22),
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
            as_of=date(2026, 8, 22),
        )
        expected_primary_dates = {
            "SPY": date(2026, 8, 14),
            "VTI": date(2026, 4, 28),
            "XLK": date(2026, 8, 14),
            "QQQ": date(2026, 8, 22),
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
        self.assertNotEqual(
            snapshot.by_symbol["QQQ"].initial_listing_source.url,
            snapshot.by_symbol["QQQ"].source_url,
        )
        self.assertEqual(
            snapshot.by_symbol["QQQ"].initial_listing_source.source_as_of,
            date(2010, 1, 26),
        )
        self.assertEqual(
            snapshot.by_symbol["VTI"].initial_listing_source.source_as_of,
            date(2026, 4, 28),
        )

    def test_benchmark_and_support_roles_are_distinct_and_complete(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 22),
        )

        self.assertEqual(snapshot.regime_support_symbols, ("SPY", "QQQ"))
        self.assertEqual(snapshot.by_symbol["QQQ"].benchmark, "SPY")
        self.assertIn("regime_benchmark", snapshot.by_symbol["QQQ"].support_roles)
        self.assertEqual(snapshot.by_symbol["SPY"].benchmark, "VTI")
        self.assertIn("market_benchmark", snapshot.by_symbol["SPY"].support_roles)
        self.assertIn("regime_benchmark", snapshot.by_symbol["SPY"].support_roles)
        self.assertIn("spy_benchmark", snapshot.by_symbol["VTI"].support_roles)
        self.assertIn("sector_benchmark", snapshot.by_symbol["XLK"].support_roles)

    def test_sector_mapping_targets_exactly_match_sector_benchmark_roles(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 22),
        )
        mapping_targets = set(snapshot.sector_mapping.values())
        role_holders = {
            record.symbol
            for record in snapshot.records
            if "sector_benchmark" in record.support_roles
        }
        self.assertEqual(mapping_targets, role_holders)

        raw = universe_fixture()
        policy = raw["benchmark_policy"]
        self.assertIsInstance(policy, dict)
        sector_mapping = policy["sector_mapping"]
        self.assertIsInstance(sector_mapping, dict)
        sector_mapping["AAPL"] = "QQQ"
        records = raw["records"]
        self.assertIsInstance(records, list)
        aapl = next(
            record
            for record in records
            if isinstance(record, dict) and record.get("symbol") == "AAPL"
        )
        aapl["sector_etf"] = "QQQ"

        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_tick_sizes_are_exact_positive_decimals_with_reviewed_sources(self) -> None:
        snapshot = UniverseSnapshot.load(
            PUBLISHED_UNIVERSE,
            as_of=date(2026, 8, 22),
        )

        for record in snapshot.records:
            with self.subTest(symbol=record.symbol):
                self.assertEqual(record.tick_size, Decimal("0.01"))
                self.assertEqual(
                    record.tick_classification,
                    "reviewed_conditional_price_at_or_above_1_usd",
                )
                self.assertEqual(record.tick_source.source_as_of, date(2026, 8, 22))
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
        for as_of in (date(2026, 8, 21), date(2026, 9, 23)):
            with self.subTest(as_of=as_of), self.assertRaises(UniverseError):
                UniverseSnapshot.load(PUBLISHED_UNIVERSE, as_of=as_of)

    def test_review_metadata_must_be_current_and_manual(self) -> None:
        for field, value in (
            ("reviewed_at", "2026-08-21"),
            ("review_by", "2026-09-23"),
            ("acquisition_method", "automated_scrape"),
        ):
            raw = universe_fixture()
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(UniverseError):
                self._load_mapping(_resign(raw))

    def test_universe_schema_version_must_be_an_exact_integer(self) -> None:
        for invalid in (True, False):
            raw = universe_fixture()
            raw["schema_version"] = invalid

            with self.subTest(value=invalid), self.assertRaises(UniverseError):
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

    def test_float_at_threshold_is_accepted_and_below_is_rejected(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        record = records[0]
        self.assertIsInstance(record, dict)
        float_source = record["float_source"]
        self.assertIsInstance(float_source, dict)
        record["free_float"] = 50_000_000
        float_source["stored_value"] = 50_000_000

        snapshot = self._load_mapping(_resign(raw))

        self.assertEqual(snapshot.by_symbol["AAPL"].free_float, 50_000_000)

        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        record = records[0]
        self.assertIsInstance(record, dict)
        float_source = record["float_source"]
        self.assertIsInstance(float_source, dict)
        record["free_float"] = 49_999_999
        float_source["stored_value"] = 49_999_999
        with self.assertRaises(UniverseError):
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

    def test_amd_float_cannot_exceed_corroborating_outstanding_shares(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        amd = next(
            record
            for record in records
            if isinstance(record, dict) and record.get("symbol") == "AMD"
        )
        float_source = amd["float_source"]
        self.assertIsInstance(float_source, dict)
        float_source["corroborating_shares_outstanding"] = 1_610_000_000

        with self.assertRaises(UniverseError):
            self._load_mapping(_resign(raw))

    def test_amd_corroborating_date_must_match_float_source_evidence(self) -> None:
        for corroborating_date in ("2026-08-14", "2026-01-29"):
            raw = universe_fixture()
            records = raw["records"]
            self.assertIsInstance(records, list)
            amd = next(
                record
                for record in records
                if isinstance(record, dict) and record.get("symbol") == "AMD"
            )
            float_source = amd["float_source"]
            self.assertIsInstance(float_source, dict)
            float_source["corroborating_shares_outstanding_as_of"] = (
                corroborating_date
            )

            with self.subTest(corroborating_date=corroborating_date):
                with self.assertRaises(UniverseError):
                    self._load_mapping(_resign(raw))

    def test_float_operand_dates_cannot_exceed_evidence_date(self) -> None:
        raw = universe_fixture()
        records = raw["records"]
        self.assertIsInstance(records, list)
        amd = next(
            record
            for record in records
            if isinstance(record, dict) and record.get("symbol") == "AMD"
        )
        float_source = amd["float_source"]
        self.assertIsInstance(float_source, dict)
        operand_dates = float_source["operand_source_dates"]
        self.assertIsInstance(operand_dates, dict)
        operand_dates["share_price"] = "2026-01-31"

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
            as_of=date(2026, 8, 22),
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
