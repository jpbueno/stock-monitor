"""Durable scheduling for canonical provider-backed workflow contexts."""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
from weakref import ref

import stock_monitor.provider_workflows as provider_workflows
from stock_monitor.journal import Journal
from stock_monitor.provider_workflows import (
    CanonicalPremarketCompositionAuthority,
    canonical_premarket_state_hash,
    issue_canonical_premarket_material,
)
from stock_monitor.reports import (
    PremarketState,
    render_premarket_report,
)
from stock_monitor.scheduled import (
    JournalScheduledRunStore,
    RunKind,
    run_canonical_scheduled,
)
from stock_monitor.workflows import (
    CanonicalJournalWorkflowPublisher,
    CanonicalWorkflowContext,
    PremarketSnapshot,
    SessionWindow,
    WorkflowDataError,
)


ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 21)


class _UnavailableCanonicalAdapter:
    execution_mode = "CANONICAL"

    def __init__(self) -> None:
        self.material_calls: list[tuple[object, ...]] = []

    def market_session(self, day: date) -> SessionWindow | None:
        return SessionWindow(day, time(15, 30))

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> object:
        self.material_calls.append(
            (session_date, decision_at, retrieved_at)
        )
        raise WorkflowDataError("DATA_UNAVAILABLE")

    def close_material(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("close material was not requested")


class _MaterialCanonicalAdapter(_UnavailableCanonicalAdapter):
    def __init__(self, journal: Journal, reports: Path) -> None:
        super().__init__()
        self.journal = journal
        self.reports = reports
        self._authority: object | None = None
        self._candidate: object | None = None

    def cleanup(self) -> None:
        authority = self._authority
        candidate = self._candidate
        if authority is None or candidate is None:
            return
        with provider_workflows._PREMARKET_COMPOSITION_LOCK:
            if (
                provider_workflows._ISSUED_PREMARKET_COMPOSITIONS.get(
                    id(authority)
                )
                is candidate
            ):
                provider_workflows._ISSUED_PREMARKET_COMPOSITIONS.pop(
                    id(authority),
                    None,
                )

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> object:
        self.material_calls.append(
            (session_date, decision_at, retrieved_at)
        )
        receipt = self.journal.append_source_observation_receipt(
            payload=b'{"release":"scheduled"}',
            source_uri=(
                "urn:stock-monitor:reviewed-evidence-registry:"
                "scheduled-canonical"
            ),
            source_type="REVIEWED_EVIDENCE_REGISTRY",
            provider="operator-reviewed",
            feed=None,
            source_time=decision_at,
            retrieved_at=decision_at,
            provider_sequence=None,
            delay_seconds=0,
            health_result="REVIEWED",
            details={"source_role": "REVIEWED_RELEASE"},
        )
        snapshot = PremarketSnapshot((), False)
        values = {
            "session_date": session_date,
            "decision_at": decision_at,
            "retrieved_at": retrieved_at,
            "validation_window_id": "scheduled-phase1-window",
            "source_receipts": (receipt,),
            "snapshot": snapshot,
            "publication_decision": None,
            "primary_plan": None,
            "outcome": "NO TRADE",
            "reason_codes": ("NO_CANDIDATES",),
        }
        envelope = provider_workflows._premarket_composition_envelope(
            **values
        )
        authority = CanonicalPremarketCompositionAuthority(
            session_date=envelope.session_date,
            decision_at=envelope.decision_at,
            retrieved_at=envelope.retrieved_at,
            validation_window_id=envelope.validation_window_id,
            receipt_manifest=envelope.receipt_manifest,
            source_digest=envelope.source_digest,
            snapshot_digest=envelope.snapshot_digest,
            publication_decision_digest=envelope.publication_decision_digest,
            primary_plan_digest=envelope.primary_plan_digest,
            outcome=envelope.outcome,
            reason_codes=envelope.reason_codes,
            composition_digest=envelope.composition_digest,
        )
        children = (
            provider_workflows._premarket_composition_identity_children(
                snapshot=snapshot,
                source_receipts=(receipt,),
                publication_decision=None,
                primary_plan=None,
            )
        )
        candidate = provider_workflows._CompositionAuthorityCandidate(
            authority_reference=ref(authority),
            authority_fingerprint=provider_workflows._value_fingerprint(
                authority
            ),
            envelope=envelope,
            identity_children=children,
        )
        with provider_workflows._PREMARKET_COMPOSITION_LOCK:
            provider_workflows._ISSUED_PREMARKET_COMPOSITIONS[
                id(authority)
            ] = candidate
        self._authority = authority
        self._candidate = candidate
        state_hash = canonical_premarket_state_hash(
            **values,
            composition_authority=authority,
        )
        report = render_premarket_report(
            PremarketState(
                session_date=session_date,
                generated_at=retrieved_at,
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                observation_ids=(receipt.observation_sha256,),
                state_hash=state_hash,
                candidates=(),
            )
        )
        return issue_canonical_premarket_material(
            journal=self.journal,
            report_archive_root=self.reports,
            report=report,
            composition_authority=authority,
            **{
                key: value
                for key, value in values.items()
                if key not in {"outcome", "reason_codes"}
            },
        )


class CanonicalScheduledDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.reports = root / "reports"
        self.reports.mkdir()
        self.journal = Journal.open(root / "journal.sqlite3")
        self.addCleanup(self.journal.close)

    def test_due_canonical_failure_is_claimed_once_and_replayed(self):
        now = datetime(2026, 8, 21, 8, 52, 19, tzinfo=ET)
        adapter = _UnavailableCanonicalAdapter()
        scheduler = JournalScheduledRunStore(self.journal)
        context = CanonicalWorkflowContext(
            adapter=adapter,
            publisher=CanonicalJournalWorkflowPublisher(
                self.journal,
                self.reports,
            ),
            scheduler=scheduler,
            now=now,
        )

        first = run_canonical_scheduled(RunKind.PREMARKET, now, context)
        second = run_canonical_scheduled(RunKind.PREMARKET, now, context)

        self.assertEqual(
            (first.outcome, first.exit_code, first.reason_codes),
            ("DATA_UNAVAILABLE", 3, ("DATA_UNAVAILABLE",)),
        )
        self.assertEqual(second.safe_fields(), first.safe_fields())
        self.assertEqual(
            adapter.material_calls,
            [
                (
                    DAY,
                    datetime(2026, 8, 21, 8, 45, tzinfo=ET),
                    now,
                )
            ],
        )
        self.assertEqual(self.journal.count("scheduled_runs"), 1)
        self.assertEqual(
            scheduler.status(kind="PREMARKET", session_date=DAY),
            "COMPLETED_NO_REPORT",
        )

    def test_canonical_premarket_report_maps_to_durable_morning_and_heals(self):
        now = datetime(2026, 8, 21, 8, 52, 19, tzinfo=ET)
        adapter = _MaterialCanonicalAdapter(self.journal, self.reports)
        self.addCleanup(adapter.cleanup)
        scheduler = JournalScheduledRunStore(self.journal)
        context = CanonicalWorkflowContext(
            adapter=adapter,
            publisher=CanonicalJournalWorkflowPublisher(
                self.journal,
                self.reports,
            ),
            scheduler=scheduler,
            now=now,
        )

        first = run_canonical_scheduled(RunKind.PREMARKET, now, context)

        self.assertEqual((first.outcome, first.exit_code), ("NO_TRADE", 0))
        assert first.report_id is not None
        stored = self.journal.read_report(first.report_id)
        self.assertEqual(stored.report_kind, "MORNING")
        self.assertEqual(
            scheduler.status(kind="PREMARKET", session_date=DAY),
            "REPORT_EMITTED",
        )
        assert first.report_path is not None
        archived = Path(first.report_path)
        archived.unlink()

        second = run_canonical_scheduled(RunKind.PREMARKET, now, context)

        self.assertEqual(
            (second.outcome, second.exit_code, second.reason_codes),
            (
                "ALREADY_EMITTED_NOOP",
                0,
                ("FINALIZED_REPORT_ARCHIVE_VERIFIED",),
            ),
        )
        self.assertTrue(archived.is_file())
        self.assertEqual(adapter.material_calls, [(DAY, datetime(2026, 8, 21, 8, 45, tzinfo=ET), now)])


if __name__ == "__main__":
    unittest.main()
