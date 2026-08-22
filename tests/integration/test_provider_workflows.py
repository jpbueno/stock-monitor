"""Adversarial foundation tests for canonical premarket source composition."""

from __future__ import annotations

import gc
import hashlib
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from unittest import mock
from weakref import ref
from zoneinfo import ZoneInfo

import stock_monitor.market_calendar as market_calendar_module
import stock_monitor.provider_workflows as provider_workflows_module
from stock_monitor.evidence import load_current_evidence_release
from stock_monitor.journal import Journal
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.provider_workflows import (
    CanonicalMaterialError,
    PremarketSourceBinding,
    canonical_premarket_state_hash,
    is_issued_canonical_premarket_composition_authority,
    is_issued_canonical_premarket_source_binding_authority,
    issue_canonical_premarket_composition_authority,
    issue_canonical_premarket_source_binding_authority,
)
from stock_monitor.providers.alpaca import (
    AlpacaMarketData,
    read_provider_fetch_bundle,
)
from stock_monitor.universe import load_current_universe
from stock_monitor.workflows import PremarketSnapshot
from tests.support import FixtureTransport, credentials


ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 22)
DECISION_AT = datetime(2026, 8, 22, 8, 45, tzinfo=ET)
RETRIEVED_AT = datetime(2026, 8, 22, 8, 52, tzinfo=ET)


class CanonicalPremarketSourceBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.journal = Journal.open(Path(temporary.name) / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.calendar = load_current_market_calendar(ROOT, as_of=DAY)
        self.universe = load_current_universe(ROOT, as_of=DAY)
        self.evidence = load_current_evidence_release(
            ROOT,
            as_of=DECISION_AT,
            universe=self.universe,
        )

    @staticmethod
    def _reviewed_uri(role: str, payload: bytes) -> str:
        return (
            f"stock-monitor://reviewed/{role}/"
            f"{hashlib.sha256(payload).hexdigest()}"
        )

    def _reviewed_receipt(
        self,
        *,
        role: str,
        payload: bytes,
        source_time: datetime,
        journal: Journal | None = None,
    ):
        target = self.journal if journal is None else journal
        return target.append_source_observation_receipt(
            payload=payload,
            source_uri=self._reviewed_uri(role, payload),
            source_type="REVIEWED_ARTIFACT",
            provider="operator-reviewed",
            feed=None,
            source_time=source_time,
            retrieved_at=DECISION_AT,
            provider_sequence=None,
            delay_seconds=int((DECISION_AT - source_time).total_seconds()),
            health_result="REVIEWED",
            details={"role": role},
        )

    def _reviewed_bindings(self, *, journal: Journal | None = None):
        bindings: list[PremarketSourceBinding] = []
        calendar_payload = (ROOT / "data/calendars/2026.json").read_bytes()
        universe_payload = (ROOT / "data/universe/2026-08-22.json").read_bytes()
        release_payload = (ROOT / "data/evidence/current.json").read_bytes()
        reviewed = (
            (
                "calendar",
                calendar_payload,
                datetime.combine(self.calendar.reviewed_at, datetime.min.time(), ET),
                self.calendar,
            ),
            (
                "universe",
                universe_payload,
                datetime.combine(self.universe.reviewed_at, datetime.min.time(), ET),
                self.universe,
            ),
            (
                "evidence-release",
                release_payload,
                self.evidence.reviewed_at,
                self.evidence,
            ),
        )
        for role, payload, source_time, source in reviewed:
            bindings.append(
                PremarketSourceBinding(
                    receipt=self._reviewed_receipt(
                        role=role,
                        payload=payload,
                        source_time=source_time,
                        journal=journal,
                    ),
                    source=source,
                    decision_basis="ECONOMIC_INPUT",
                )
            )
        for symbol, bundle in self.evidence.by_symbol.items():
            payload = (ROOT / f"data/evidence/subjects/{symbol}.json").read_bytes()
            bindings.append(
                PremarketSourceBinding(
                    receipt=self._reviewed_receipt(
                        role=f"evidence-{symbol.lower()}",
                        payload=payload,
                        source_time=bundle.reviewed_at,
                        journal=journal,
                    ),
                    source=bundle,
                    decision_basis="ECONOMIC_INPUT",
                )
            )
            for source_binding in bundle.source_bindings:
                document = source_binding.document
                details = {
                    "accession": document.accession,
                    "issuer_cik": None,
                    "source_observation_id": document.source_observation_id,
                    "source_role": document.source_role,
                    "symbol": None,
                    "timestamp_source": document.timestamp_source,
                }
                receipt = (self.journal if journal is None else journal).append_source_observation_receipt(
                    payload=document.body,
                    source_uri=document.url,
                    source_type=document.source_type,
                    provider=document.publisher,
                    feed=document.timestamp_source,
                    source_time=document.published_at or document.retrieved_at,
                    retrieved_at=document.retrieved_at,
                    provider_sequence=None,
                    delay_seconds=0,
                    health_result="OK",
                    details=details,
                )
                bindings.append(
                    PremarketSourceBinding(
                        receipt=receipt,
                        source=document,
                        decision_basis="OPERATIONAL_HEALTH_ONLY",
                    )
                )
        target = self.journal if journal is None else journal
        reread = target.read_source_observation_receipts(
            tuple(binding.receipt.row_id for binding in bindings)
        )
        return tuple(
            replace(binding, receipt=receipt)
            for binding, receipt in zip(bindings, reread, strict=True)
        )

    def _authority(self):
        return issue_canonical_premarket_source_binding_authority(
            journal=self.journal,
            decision_at=DECISION_AT,
            retrieved_at=RETRIEVED_AT,
            bindings=self._reviewed_bindings(),
        )

    def test_exact_reviewed_bytes_children_and_owner_issue_one_current_authority(self) -> None:
        original_calendar_predicate = (
            market_calendar_module.is_release_verified_market_calendar
        )
        with mock.patch.object(
            market_calendar_module,
            "is_release_verified_market_calendar",
            wraps=original_calendar_predicate,
        ) as calendar_predicate:
            authority = self._authority()
        self.assertEqual(calendar_predicate.call_count, 1)
        self.assertTrue(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )
        self.assertTrue(
            all(
                binding.receipt.health_result == "REVIEWED"
                and binding.receipt.source_payload
                for binding in provider_workflows_module._premarket_reviewed_bindings(
                    authority
                )
            )
        )
        dummy = b"{}"
        dummy_receipt = self._reviewed_receipt(
            role="calendar",
            payload=dummy,
            source_time=datetime.combine(
                self.calendar.reviewed_at,
                datetime.min.time(),
                ET,
            ),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "bytes or pin"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                bindings=(
                    PremarketSourceBinding(
                        receipt=dummy_receipt,
                        source=self.calendar,
                        decision_basis="ECONOMIC_INPUT",
                    ),
                ),
            )
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )

    def test_late_iex_is_health_only_and_details_id_has_no_authority(self) -> None:
        now = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        client = AlpacaMarketData(
            FixtureTransport("providers/alpaca/latest-iex.json"),
            credentials(),
            now=lambda: now,
        )
        cohort = client.latest_iex_quotes(("QQQ", "SPY"))
        disclosure = read_provider_fetch_bundle(cohort["QQQ"])
        page = disclosure.pages[0]
        receipt = self.journal.append_source_observation_receipt(
            payload=page.payload,
            source_uri=page.page.request_url,
            source_type=page.page.source_type,
            provider="alpaca",
            feed=page.observation.feed,
            source_time=page.observation.source_timestamp,
            retrieved_at=page.observation.retrieved_at,
            provider_sequence=page.page.page_ordinal,
            delay_seconds=page.observation.delay_seconds,
            health_result="OK",
            details={"source_observation_id": "forged-but-ignored"},
        )
        binding = PremarketSourceBinding(
            receipt=receipt,
            source=page,
            disclosure=disclosure,
            decision_basis="OPERATIONAL_HEALTH_ONLY",
        )
        decision_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 14, 9, 0, tzinfo=ET)
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), self.assertRaisesRegex(
            CanonicalMaterialError,
            "caller-authored disclosures",
        ):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(binding,),
            )
        binding = replace(binding, disclosure=None)
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            side_effect=lambda value: value is page,
        ):
            authority = issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(binding,),
            )
        self.assertIn(
            (receipt.row_id, "ALPACA_LATEST_QUOTES", "OPERATIONAL_HEALTH_ONLY"),
            authority.decision_basis,
        )
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), self.assertRaisesRegex(CanonicalMaterialError, "health-only"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(replace(binding, decision_basis="ECONOMIC_INPUT"),),
            )

    def test_cross_owner_copy_mutation_and_callback_fail_closed(self) -> None:
        authority = self._authority()
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                replace(authority),
                journal=self.journal,
            )
        )
        object.__setattr__(authority, "binding_digest", "f" * 64)
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )

        with tempfile.TemporaryDirectory() as other_root:
            other = Journal.open(Path(other_root) / "other.sqlite3")
            self.addCleanup(other.close)
            with self.assertRaisesRegex(CanonicalMaterialError, "owner"):
                issue_canonical_premarket_source_binding_authority(
                    journal=self.journal,
                    decision_at=DECISION_AT,
                    retrieved_at=RETRIEVED_AT,
                    bindings=self._reviewed_bindings(journal=other),
                )

        bindings = self._reviewed_bindings()
        original = provider_workflows_module._verify_premarket_binding_source

        def mutate_owner(binding, **kwargs):
            result = original(binding, **kwargs)
            self.journal.append_source_observation(
                payload=b"callback",
                source_uri=self._reviewed_uri("callback", b"callback"),
                source_type="REVIEWED_ARTIFACT",
                provider="operator-reviewed",
                feed=None,
                source_time=DECISION_AT,
                retrieved_at=DECISION_AT,
                provider_sequence=None,
                delay_seconds=0,
                health_result="REVIEWED",
                details={"role": "callback"},
            )
            return result

        with mock.patch.object(
            provider_workflows_module,
            "_verify_premarket_binding_source",
            side_effect=mutate_owner,
        ), self.assertRaisesRegex(CanonicalMaterialError, "changed|current"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                bindings=bindings,
            )

    def test_composition_seals_binding_decision_basis_and_release_children(self) -> None:
        source_authority = self._authority()
        snapshot = PremarketSnapshot((), False)
        with self.assertRaisesRegex(
            CanonicalMaterialError,
            "conflict with reviewed evidence",
        ):
            issue_canonical_premarket_composition_authority(
                journal=self.journal,
                session_date=DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                validation_window_id="task10-foundation",
                source_binding_authority=source_authority,
                snapshot=snapshot,
                publication_decision=None,
                primary_plan=None,
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                calendar=self.calendar,
                universe=self.universe,
                evidence_release=self.evidence,
                phase1_replay_children=(object(),),
            )
        composition = issue_canonical_premarket_composition_authority(
            journal=self.journal,
            session_date=DAY,
            decision_at=DECISION_AT,
            retrieved_at=RETRIEVED_AT,
            validation_window_id="task10-foundation",
            source_binding_authority=source_authority,
            snapshot=snapshot,
            publication_decision=None,
            primary_plan=None,
            outcome="NO TRADE",
            reason_codes=("NO_CANDIDATES",),
            calendar=self.calendar,
            universe=self.universe,
            evidence_release=self.evidence,
            phase1_replay_children=(),
        )
        self.assertEqual(
            composition.source_binding_digest,
            source_authority.binding_digest,
        )
        self.assertEqual(composition.decision_basis, source_authority.decision_basis)
        self.assertTrue(
            is_issued_canonical_premarket_composition_authority(composition)
        )
        source_candidate = provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS[
            id(source_authority)
        ]
        self.assertEqual(
            len(
                canonical_premarket_state_hash(
                    session_date=DAY,
                    decision_at=DECISION_AT,
                    retrieved_at=RETRIEVED_AT,
                    snapshot=snapshot,
                    source_receipts=tuple(
                        binding.receipt for binding in source_candidate.bindings
                    ),
                    publication_decision=None,
                    primary_plan=None,
                    validation_window_id="task10-foundation",
                    outcome="NO TRADE",
                    reason_codes=("NO_CANDIDATES",),
                    composition_authority=composition,
                )
            ),
            64,
        )
        self.assertFalse(
            is_issued_canonical_premarket_composition_authority(replace(composition))
        )

        identity = id(source_authority)
        reference = ref(source_authority)
        del source_authority
        gc.collect()
        self.assertIsNotNone(reference())
        self.assertIn(identity, provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS)

    def test_authority_registries_release_dead_capabilities(self) -> None:
        source_authority = self._authority()
        source_identity = id(source_authority)
        source_reference = ref(source_authority)
        del source_authority
        gc.collect()
        self.assertIsNone(source_reference())
        self.assertNotIn(
            source_identity,
            provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS,
        )


if __name__ == "__main__":
    unittest.main()
