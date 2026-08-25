from __future__ import annotations

import unittest
from dataclasses import replace
from unittest import mock

import stock_monitor.evidence_authorities as evidence_authorities_module
from stock_monitor.evidence_authorities import (
    CLEAR_COVERAGE_AUTHORITY_BUNDLES,
    EVIDENCE_AUTHORITIES,
    EvidenceAuthority,
    authorities_for,
    scoped_reference_authorities,
)


class EvidenceAuthorityPolicyTests(unittest.TestCase):
    def test_every_source_is_exact_subject_scoped_and_fact_only(self) -> None:
        self.assertTrue(EVIDENCE_AUTHORITIES)
        self.assertEqual(CLEAR_COVERAGE_AUTHORITY_BUNDLES, {})
        for source in EVIDENCE_AUTHORITIES:
            self.assertIs(type(source), EvidenceAuthority)
            self.assertEqual(source.purpose, "FACT_DISCOVERY")
            self.assertFalse(source.clear_capable)
            self.assertTrue(source.requested_url.startswith("https://"))
            self.assertIn(source.requested_url, source.allowed_final_urls)
            self.assertEqual(source.role.rsplit(":", 1)[-1], source.symbol)

    def test_retrieval_redirect_policy_contains_only_proven_requested_urls(
        self,
    ) -> None:
        for source in EVIDENCE_AUTHORITIES:
            self.assertEqual(
                source.allowed_final_urls,
                frozenset({source.requested_url}),
            )

        scoped = scoped_reference_authorities()
        self.assertIn(
            (
                "https://investor.apple.com/investor-relations/faq/default.aspx",
                "Apple Inc.",
            ),
            scoped["ISSUER_IR:AAPL"][1],
        )

    def test_catalog_covers_exact_current_universe_without_generic_roles(self) -> None:
        symbols = {"AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"}
        self.assertEqual(
            {source.symbol for source in EVIDENCE_AUTHORITIES},
            symbols,
        )
        for symbol in symbols:
            self.assertGreaterEqual(len(authorities_for(symbol)), 2)
        self.assertNotIn(
            "ISSUER_IR:*",
            {source.role for source in EVIDENCE_AUTHORITIES},
        )
        self.assertNotIn("ISSUER_IR:*", scoped_reference_authorities())

    def test_catalog_matches_the_approved_requested_urls_and_publishers(self) -> None:
        self.assertEqual(
            {
                (source.symbol, source.requested_url, source.publisher)
                for source in EVIDENCE_AUTHORITIES
            },
            {
                (
                    "AAPL",
                    "https://data.sec.gov/submissions/CIK0000320193.json",
                    "U.S. Securities and Exchange Commission",
                ),
                (
                    "AAPL",
                    "https://investor.apple.com/investor-relations/default.aspx",
                    "Apple Inc.",
                ),
                (
                    "AAPL",
                    "https://www.apple.com/newsroom/rss-feed.rss",
                    "Apple Inc.",
                ),
                (
                    "AMD",
                    "https://data.sec.gov/submissions/CIK0000002488.json",
                    "U.S. Securities and Exchange Commission",
                ),
                (
                    "AMD",
                    "https://ir.amd.com/news-events/ir-calendar",
                    "Advanced Micro Devices, Inc.",
                ),
                (
                    "AMD",
                    "https://ir.amd.com/news-events/press-releases/rss",
                    "Advanced Micro Devices, Inc.",
                ),
                (
                    "NVDA",
                    "https://data.sec.gov/submissions/CIK0001045810.json",
                    "U.S. Securities and Exchange Commission",
                ),
                (
                    "NVDA",
                    "https://investor.nvidia.com/rss/Event.aspx?LanguageId=1",
                    "NVIDIA Corporation",
                ),
                (
                    "NVDA",
                    "https://nvidianews.nvidia.com/cats/press_release.xml",
                    "NVIDIA Corporation",
                ),
                (
                    "QQQ",
                    "https://www.invesco.com/qqq-etf/en/home.html",
                    "Invesco",
                ),
                (
                    "QQQ",
                    "https://www.invesco.com/us/en/newsroom.html",
                    "Invesco",
                ),
                (
                    "SPY",
                    "https://www.ssga.com/us/en/intermediary/etfs/"
                    "state-street-spdr-sp-500-etf-trust-spy",
                    "State Street Global Advisors",
                ),
                (
                    "SPY",
                    "https://www.ssga.com/us/en/intermediary/resources/"
                    "authorized-participants",
                    "State Street Global Advisors",
                ),
                (
                    "VTI",
                    "https://investor.vanguard.com/investment-products/etfs/"
                    "profile/vti",
                    "Vanguard",
                ),
                (
                    "VTI",
                    "https://corporate.vanguard.com/content/corporatesite/us/en/"
                    "corp/who-we-are/pressroom/index.html.html",
                    "Vanguard",
                ),
                (
                    "XLK",
                    "https://www.ssga.com/us/en/intermediary/etfs/"
                    "state-street-technology-select-sector-spdr-etf-xlk",
                    "State Street Global Advisors",
                ),
                (
                    "XLK",
                    "https://www.ssga.com/us/en/intermediary/resources/"
                    "authorized-participants",
                    "State Street Global Advisors",
                ),
            },
        )

    def test_sec_sources_use_a_distinct_role_outside_reference_authority(self) -> None:
        scoped = scoped_reference_authorities()
        sec_sources = tuple(
            source
            for source in EVIDENCE_AUTHORITIES
            if source.publisher == "U.S. Securities and Exchange Commission"
        )
        self.assertEqual(len(sec_sources), 3)
        for source in sec_sources:
            self.assertEqual(source.role, f"SEC_SUBMISSIONS:{source.symbol}")
            self.assertNotIn(source.role, scoped)

    def test_clear_coverage_policy_cannot_be_mutated_at_runtime(self) -> None:
        key = ("AAPL", "BINARY_EVENT")
        try:
            with self.assertRaises(TypeError):
                CLEAR_COVERAGE_AUTHORITY_BUNDLES[key] = (  # type: ignore[index]
                    frozenset()
                )
        finally:
            if type(CLEAR_COVERAGE_AUTHORITY_BUNDLES) is dict:
                CLEAR_COVERAGE_AUTHORITY_BUNDLES.pop(key, None)

    def test_credential_like_query_keys_are_rejected(self) -> None:
        for query in ("key=value", "access_key=value"):
            with self.subTest(query=query):
                with self.assertRaises(RuntimeError):
                    evidence_authorities_module._origin(
                        f"https://example.com/official?{query}"
                    )

    def test_sec_role_requires_the_exact_sec_submission_identity(self) -> None:
        issuer_page = next(
            source
            for source in authorities_for("AAPL")
            if source.publisher == "Apple Inc."
        )
        malformed = replace(
            issuer_page,
            publisher="U.S. Securities and Exchange Commission",
            role="SEC_SUBMISSIONS:AAPL",
        )
        with mock.patch.object(
            evidence_authorities_module,
            "EVIDENCE_AUTHORITIES",
            (malformed,),
        ):
            with self.assertRaises(RuntimeError):
                evidence_authorities_module._validate_catalog()


if __name__ == "__main__":
    unittest.main()
