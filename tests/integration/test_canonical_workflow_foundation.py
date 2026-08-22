"""Fail-closed authority tests for the canonical workflow foundation."""

from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo
from weakref import ref

import stock_monitor.provider_workflows as provider_workflows_module
from stock_monitor.journal import Journal, report_archive_relative_path
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.provider_workflows import (
    CanonicalCloseCompositionAuthority,
    CanonicalCloseMaterial,
    CanonicalMaterialError,
    CanonicalPremarketCompositionAuthority,
    CanonicalPremarketMaterial,
    canonical_close_state_hash,
    canonical_premarket_state_hash,
    canonical_source_digest,
    canonical_source_receipts,
    is_issued_canonical_material,
    issue_canonical_close_material,
    issue_canonical_premarket_material,
)
from stock_monitor.reconciliation import UnavailableSignalPlanResolver, replay_actual
from stock_monitor.reports import (
    ClosePosition,
    CloseState,
    PremarketState,
    UnverifiedClosePosition,
    render_close_report,
    render_premarket_report,
)
from stock_monitor.risk import SessionCalendarResolver
from stock_monitor.workflows import (
    CandidateSummary,
    CanonicalJournalWorkflowPublisher,
    CanonicalPublicationPlan,
    JournalWorkflowPublisher,
    PremarketSnapshot,
    WorkflowError,
    WorkflowResult,
    _canonical_result_projection,
)
from tests.support import policy_fixture
from tests.unit.test_report_precedence import primary_candidate


ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 24)
DECISION_AT = datetime(2026, 8, 24, 8, 45, tzinfo=ET)
REVIEW_AT = datetime(2026, 8, 24, 15, 30, tzinfo=ET)
QUERY_CUTOFF = datetime(2026, 8, 24, 15, 36, tzinfo=ET)
RETRIEVED_AT = datetime(2026, 8, 24, 15, 38, tzinfo=ET)
ROOT = Path(__file__).resolve().parents[2]
EMPTY_CLOSE_REASONS = (
    "NO_ACTUAL_POSITIONS",
    "MANUAL_VERIFICATION_REQUIRED",
)


class _MutatingTimezone(tzinfo):
    """A hostile timezone whose callbacks alter composition authority state."""

    def __init__(self, callback) -> None:
        self.callback = callback
        self.calls = 0

    def utcoffset(self, value):
        del value
        self.calls += 1
        self.callback()
        return timedelta(hours=-4)

    def dst(self, value):
        del value
        return timedelta(0)

    def tzname(self, value):
        del value
        return "HOSTILE"


class CanonicalWorkflowFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "reports"
        self.root.mkdir()
        self.journal = Journal.open(Path(self.temporary.name) / "journal.sqlite3")
        self.addCleanup(self.journal.close)

    def _receipt(
        self,
        *,
        suffix: str = "premarket",
        journal: Journal | None = None,
        source_uri: str | None = None,
        provider: str = "alpaca",
        source_type: str = "ALPACA_DAILY_BARS",
        feed: str | None = "sip",
        source_time: datetime | None = None,
        retrieved_at: datetime | None = None,
        details: dict[str, object] | None = None,
        health_result: str = "OK",
    ):
        target = self.journal if journal is None else journal
        return target.append_source_observation_receipt(
            payload=(f'{{"source":"{suffix}"}}').encode("utf-8"),
            source_uri=(
                "https://data.alpaca.markets/v2/stocks/bars?"
                "symbols=AAPL&timeframe=1Day&"
                "start=2026-08-21T13%3A30%3A00Z&"
                "end=2026-08-24T12%3A45%3A00Z&"
                "adjustment=split&feed=sip&limit=10000"
                if source_uri is None
                else source_uri
            ),
            source_type=source_type,
            provider=provider,
            feed=feed,
            source_time=(
                DECISION_AT - timedelta(minutes=20)
                if source_time is None
                else source_time
            ),
            retrieved_at=(
                DECISION_AT - timedelta(minutes=1)
                if retrieved_at is None
                else retrieved_at
            ),
            provider_sequence=1,
            delay_seconds=900,
            health_result=health_result,
            details=(
                {"page_ordinal": 1, "source_role": suffix.upper()}
                if details is None
                else details
            ),
        )

    @staticmethod
    def _premarket_authority(
        *,
        session_date: date = DAY,
        decision_at: datetime = DECISION_AT,
        retrieved_at: datetime = DECISION_AT + timedelta(minutes=7),
        outcome: str = "NO TRADE",
        reasons: tuple[str, ...] = ("NO_CANDIDATES",),
    ) -> CanonicalPremarketCompositionAuthority:
        return CanonicalPremarketCompositionAuthority(
            session_date=session_date,
            decision_at=decision_at,
            retrieved_at=retrieved_at,
            validation_window_id="phase1-window",
            receipt_manifest=((1, "0" * 64, "1" * 64, "2" * 64),),
            source_digest="1" * 64,
            snapshot_digest="2" * 64,
            publication_decision_digest=None,
            primary_plan_digest=None,
            outcome=outcome,
            reason_codes=reasons,
            composition_digest="3" * 64,
        )

    @staticmethod
    def _close_authority(
        *,
        session_date: date = DAY,
        review_at: datetime = REVIEW_AT,
        retrieved_at: datetime = RETRIEVED_AT,
        query_cutoff: datetime = QUERY_CUTOFF,
        outcome: str = "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
        reasons: tuple[str, ...] = EMPTY_CLOSE_REASONS,
    ) -> CanonicalCloseCompositionAuthority:
        return CanonicalCloseCompositionAuthority(
            session_date=session_date,
            review_at=review_at,
            retrieved_at=retrieved_at,
            query_cutoff=query_cutoff,
            receipt_manifest=((1, "0" * 64, "1" * 64, "2" * 64),),
            source_digest="4" * 64,
            actual_state_digest="5" * 64,
            actual_replay_source_digest="6" * 64,
            positions_digest="7" * 64,
            outcome=outcome,
            reason_codes=reasons,
            composition_digest="8" * 64,
        )

    def _actual(self, *, query_cutoff: datetime = QUERY_CUTOFF):
        with self.journal.transaction() as transaction:
            replay_source = transaction.read_actual_replay(query_cutoff=query_cutoff)
        state = replay_actual(
            replay_source,
            plans=UnavailableSignalPlanResolver(),
            calendar=SessionCalendarResolver(
                (load_current_market_calendar(ROOT, as_of=query_cutoff.date()),)
            ),
            policy=policy_fixture(),
        )
        return replay_source, state

    @staticmethod
    def _premarket_report(
        receipt,
        *,
        snapshot: PremarketSnapshot,
        generated_at: datetime = DECISION_AT + timedelta(minutes=7),
        outcome: str = "NO TRADE",
        reasons: tuple[str, ...] = ("NO_CANDIDATES",),
        state_hash: str = "a" * 64,
        observation_ids: tuple[str, ...] | None = None,
    ):
        return render_premarket_report(
            PremarketState(
                session_date=generated_at.astimezone(ET).date(),
                generated_at=generated_at,
                outcome=outcome,
                reason_codes=reasons,
                observation_ids=(
                    (receipt.observation_sha256,)
                    if observation_ids is None
                    else observation_ids
                ),
                state_hash=state_hash,
                candidates=tuple(
                    candidate.material
                    for candidate in snapshot.candidates
                    if candidate.material is not None
                ),
            )
        )

    @staticmethod
    def _close_report(
        receipt,
        *,
        generated_at: datetime = RETRIEVED_AT,
        reasons: tuple[str, ...] = EMPTY_CLOSE_REASONS,
        state_hash: str = "b" * 64,
        observation_ids: tuple[str, ...] | None = None,
        positions: tuple[ClosePosition | UnverifiedClosePosition, ...] = (),
        **state_overrides: bool,
    ):
        return render_close_report(
            CloseState(
                session_date=generated_at.astimezone(ET).date(),
                generated_at=generated_at,
                reason_codes=reasons,
                positions=positions,
                observation_ids=(
                    (receipt.observation_sha256,)
                    if observation_ids is None
                    else observation_ids
                ),
                state_hash=state_hash,
                **state_overrides,
            )
        )

    @staticmethod
    def _verified_close_position(
        *,
        symbol: str = "AAPL",
        action: str = "HOLD",
        reasons: tuple[str, ...] = ("POSITION_REVIEW_COMPLETE",),
        stop: Decimal | None = Decimal("98.00"),
    ) -> ClosePosition:
        return ClosePosition(
            symbol=symbol,
            shares=2,
            mark=Decimal("102.00"),
            estimated_unrealized_pl=Decimal("4.00"),
            r_multiple=Decimal("1.00"),
            recommended_stop=Decimal("98.00"),
            user_confirmed_stop=stop,
            target=Decimal("108.00"),
            holding_days=3,
            provider="ALPACA",
            feed="IEX",
            observed_at=REVIEW_AT - timedelta(minutes=1),
            upcoming_events=(),
            evidence=(),
            action=action,
            reason_codes=reasons,
        )

    @staticmethod
    def _unverified_close_position(
        *,
        symbol: str = "AAPL",
        status: str = "POSITION_UNVERIFIED",
        reasons: tuple[str, ...] = ("PLAN_LINEAGE_UNAVAILABLE",),
    ) -> UnverifiedClosePosition:
        return UnverifiedClosePosition(
            symbol=symbol,
            shares=2,
            exact_cost_basis=Decimal("100.00"),
            status=status,
            reason_codes=reasons,
        )

    def _premarket_issue_kwargs(
        self,
        receipt,
        *,
        snapshot: PremarketSnapshot | None = None,
        report=None,
        composition_authority: object | None = None,
    ) -> dict[str, object]:
        exact_snapshot = PremarketSnapshot((), False) if snapshot is None else snapshot
        exact_report = (
            self._premarket_report(receipt, snapshot=exact_snapshot)
            if report is None
            else report
        )
        return {
            "journal": self.journal,
            "report_archive_root": self.root,
            "session_date": DAY,
            "decision_at": DECISION_AT,
            "retrieved_at": DECISION_AT + timedelta(minutes=7),
            "snapshot": exact_snapshot,
            "report": exact_report,
            "source_receipts": (receipt,),
            "publication_decision": None,
            "primary_plan": None,
            "validation_window_id": "phase1-window",
            "composition_authority": composition_authority,
        }

    def _close_issue_kwargs(
        self,
        receipt,
        *,
        composition_authority: object | None = None,
        report=None,
    ) -> dict[str, object]:
        replay_source, actual_state = self._actual()
        return {
            "journal": self.journal,
            "report_archive_root": self.root,
            "session_date": DAY,
            "review_at": REVIEW_AT,
            "retrieved_at": RETRIEVED_AT,
            "query_cutoff": QUERY_CUTOFF,
            "actual_state": actual_state,
            "actual_replay_source": replay_source,
            "composition_authority": composition_authority,
            "report": self._close_report(receipt) if report is None else report,
            "positions": (),
            "source_receipts": (receipt,),
        }

    @contextmanager
    def _installed_composition_candidate(
        self,
        *,
        authority: object,
        envelope: object,
        identity_children: tuple[object, ...],
        registry: dict[int, object],
        lock,
    ):
        candidate = provider_workflows_module._CompositionAuthorityCandidate(
            authority_reference=ref(authority),
            authority_fingerprint=provider_workflows_module._value_fingerprint(
                authority
            ),
            envelope=envelope,
            identity_children=identity_children,
        )
        with lock:
            registry[id(authority)] = candidate
        try:
            yield candidate
        finally:
            with lock:
                if registry.get(id(authority)) is candidate:
                    registry.pop(id(authority), None)

    @staticmethod
    def _premarket_authority_from_envelope(envelope):
        return CanonicalPremarketCompositionAuthority(
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

    @staticmethod
    def _close_authority_from_envelope(envelope):
        return CanonicalCloseCompositionAuthority(
            session_date=envelope.session_date,
            review_at=envelope.review_at,
            retrieved_at=envelope.retrieved_at,
            query_cutoff=envelope.query_cutoff,
            receipt_manifest=envelope.receipt_manifest,
            source_digest=envelope.source_digest,
            actual_state_digest=envelope.actual_state_digest,
            actual_replay_source_digest=envelope.actual_replay_source_digest,
            positions_digest=envelope.positions_digest,
            outcome=envelope.outcome,
            reason_codes=envelope.reason_codes,
            composition_digest=envelope.composition_digest,
        )

    def test_public_premarket_issuer_requires_composition_for_every_branch(self) -> None:
        receipt = self._receipt()
        branches = (
            (PremarketSnapshot((), False), "NO TRADE", ("NO_CANDIDATES",)),
            (PremarketSnapshot((), True), "NO TRADE", ("ACTIVE_BREAKER",)),
            (
                PremarketSnapshot((), False),
                "NO NEW TRADE - DATA UNAVAILABLE",
                ("DATA_UNAVAILABLE",),
            ),
        )
        for snapshot, outcome, reasons in branches:
            report = self._premarket_report(
                receipt, snapshot=snapshot, outcome=outcome, reasons=reasons
            )
            for authority in (
                None,
                self._premarket_authority(outcome=outcome, reasons=reasons),
            ):
                with self.subTest(
                    outcome=outcome, forged=authority is not None
                ), self.assertRaisesRegex(
                    CanonicalMaterialError, "composition authority"
                ):
                    issue_canonical_premarket_material(
                        **self._premarket_issue_kwargs(
                            receipt,
                            snapshot=snapshot,
                            report=report,
                            composition_authority=authority,
                        )
                    )

        candidate_projection = primary_candidate("AAPL")
        candidate_snapshot = PremarketSnapshot(
            (CandidateSummary("AAPL", "PRIMARY", candidate_projection),), False
        )
        candidate_report = self._premarket_report(
            receipt,
            snapshot=candidate_snapshot,
            outcome="CANDIDATES",
            reasons=("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED"),
        )
        candidate_authority = CanonicalPremarketCompositionAuthority(
            session_date=DAY,
            decision_at=DECISION_AT,
            retrieved_at=DECISION_AT + timedelta(minutes=7),
            validation_window_id="phase1-window",
            receipt_manifest=((1, "0" * 64, "1" * 64, "2" * 64),),
            source_digest="1" * 64,
            snapshot_digest="2" * 64,
            publication_decision_digest="3" * 64,
            primary_plan_digest="4" * 64,
            outcome="CANDIDATES",
            reason_codes=("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED"),
            composition_digest="5" * 64,
        )
        candidate_kwargs = self._premarket_issue_kwargs(
            receipt,
            snapshot=candidate_snapshot,
            report=candidate_report,
            composition_authority=candidate_authority,
        )
        candidate_kwargs.update(publication_decision=object(), primary_plan=object())
        with self.assertRaisesRegex(
            CanonicalMaterialError,
            "composition authority|publication decision",
        ):
            issue_canonical_premarket_material(**candidate_kwargs)

    def test_public_close_issuer_requires_composition_even_when_empty(self) -> None:
        receipt = self._receipt(
            suffix="close",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        invented = self._close_report(receipt, reasons=("INVENTED_REASON",))
        for report in (self._close_report(receipt), invented):
            for authority in (None, self._close_authority()):
                with self.subTest(
                    invented=report is invented, forged=authority is not None
                ), self.assertRaisesRegex(
                    CanonicalMaterialError,
                    (
                        "close outcome and reasons"
                        if report is invented
                        else "composition authority"
                    ),
                ):
                    issue_canonical_close_material(
                        **self._close_issue_kwargs(
                            receipt,
                            report=report,
                            composition_authority=authority,
                        )
                    )

    def test_emulated_private_verifiers_bind_the_complete_material_envelope(self) -> None:
        premarket_receipt = self._receipt(suffix="private-premarket")
        snapshot = PremarketSnapshot((), False)
        premarket_values = {
            "session_date": DAY,
            "decision_at": DECISION_AT,
            "retrieved_at": DECISION_AT + timedelta(minutes=7),
            "validation_window_id": "phase1-window",
            "source_receipts": (premarket_receipt,),
            "snapshot": snapshot,
            "publication_decision": None,
            "primary_plan": None,
            "outcome": "NO TRADE",
            "reason_codes": ("NO_CANDIDATES",),
        }
        premarket_envelope = (
            provider_workflows_module._premarket_composition_envelope(
                **premarket_values
            )
        )
        exact_premarket_authority = self._premarket_authority_from_envelope(
            premarket_envelope
        )
        wrong_premarket_authorities = (
            replace(
                exact_premarket_authority,
                session_date=DAY + timedelta(days=1),
                decision_at=DECISION_AT + timedelta(days=1),
                retrieved_at=DECISION_AT + timedelta(days=1, minutes=7),
            ),
            replace(
                exact_premarket_authority,
                retrieved_at=DECISION_AT + timedelta(minutes=8),
            ),
            replace(
                exact_premarket_authority,
                receipt_manifest=(
                    (*premarket_envelope.receipt_manifest[0][:3], "f" * 64),
                ),
                source_digest="e" * 64,
            ),
        )
        for wrong_authority in wrong_premarket_authorities:
            children = provider_workflows_module._premarket_composition_identity_children(
                snapshot=snapshot,
                source_receipts=(premarket_receipt,),
                publication_decision=None,
                primary_plan=None,
            )
            with self._installed_composition_candidate(
                authority=wrong_authority,
                envelope=premarket_envelope,
                identity_children=children,
                registry=provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS,
                lock=provider_workflows_module._PREMARKET_COMPOSITION_LOCK,
            ), self.subTest(kind="PREMARKET", mismatch=wrong_authority):
                with self.assertRaisesRegex(
                    CanonicalMaterialError,
                    "composition authority",
                ):
                    canonical_premarket_state_hash(
                        **premarket_values,
                        composition_authority=wrong_authority,
                    )

        exact_premarket_children = (
            provider_workflows_module._premarket_composition_identity_children(
                snapshot=snapshot,
                source_receipts=(premarket_receipt,),
                publication_decision=None,
                primary_plan=None,
            )
        )
        with self._installed_composition_candidate(
            authority=exact_premarket_authority,
            envelope=premarket_envelope,
            identity_children=exact_premarket_children,
            registry=provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS,
            lock=provider_workflows_module._PREMARKET_COMPOSITION_LOCK,
        ):
            premarket_state_hash = canonical_premarket_state_hash(
                **premarket_values,
                composition_authority=exact_premarket_authority,
            )
            premarket_report = self._premarket_report(
                premarket_receipt,
                snapshot=snapshot,
                state_hash=premarket_state_hash,
            )
            premarket_material = issue_canonical_premarket_material(
                journal=self.journal,
                report_archive_root=self.root,
                report=premarket_report,
                composition_authority=exact_premarket_authority,
                **{
                    key: value
                    for key, value in premarket_values.items()
                    if key not in {"outcome", "reason_codes"}
                },
            )
            publisher = CanonicalJournalWorkflowPublisher(self.journal, self.root)
            result = publisher.issue_result(material=premarket_material)
            claims_before = self.journal.count("report_claims")
            with self.assertRaisesRegex(
                WorkflowError,
                "canonical Journal publication",
            ):
                publisher.publish(
                    kind="PREMARKET",
                    session_date=DAY,
                    generated_at=premarket_values["retrieved_at"],
                    result=result,
                    material=premarket_material,
                )
            self.assertEqual(self.journal.count("report_claims"), claims_before)

        close_receipt = self._receipt(
            suffix="private-close",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        replay_source, actual_state = self._actual()
        close_values = {
            "session_date": DAY,
            "review_at": REVIEW_AT,
            "retrieved_at": RETRIEVED_AT,
            "query_cutoff": QUERY_CUTOFF,
            "actual_state": actual_state,
            "actual_replay_source": replay_source,
            "positions": (),
            "source_receipts": (close_receipt,),
            "outcome": "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
            "reason_codes": EMPTY_CLOSE_REASONS,
        }
        close_envelope = provider_workflows_module._close_composition_envelope(
            **close_values
        )
        exact_close_authority = self._close_authority_from_envelope(close_envelope)
        wrong_close_authorities = (
            replace(
                exact_close_authority,
                session_date=DAY + timedelta(days=1),
                review_at=REVIEW_AT + timedelta(days=1),
                retrieved_at=RETRIEVED_AT + timedelta(days=1),
                query_cutoff=QUERY_CUTOFF + timedelta(days=1),
            ),
            replace(
                exact_close_authority,
                retrieved_at=RETRIEVED_AT + timedelta(minutes=1),
            ),
            replace(
                exact_close_authority,
                receipt_manifest=(
                    (*close_envelope.receipt_manifest[0][:3], "f" * 64),
                ),
                source_digest="e" * 64,
            ),
        )
        for wrong_authority in wrong_close_authorities:
            children = provider_workflows_module._close_composition_identity_children(
                actual_state=actual_state,
                actual_replay_source=replay_source,
                positions=(),
                source_receipts=(close_receipt,),
            )
            with self._installed_composition_candidate(
                authority=wrong_authority,
                envelope=close_envelope,
                identity_children=children,
                registry=provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS,
                lock=provider_workflows_module._CLOSE_COMPOSITION_LOCK,
            ), self.subTest(kind="CLOSE", mismatch=wrong_authority):
                with self.assertRaisesRegex(
                    CanonicalMaterialError,
                    "composition authority",
                ):
                    canonical_close_state_hash(
                        **close_values,
                        composition_authority=wrong_authority,
                    )

        exact_close_children = (
            provider_workflows_module._close_composition_identity_children(
                actual_state=actual_state,
                actual_replay_source=replay_source,
                positions=(),
                source_receipts=(close_receipt,),
            )
        )
        with self._installed_composition_candidate(
            authority=exact_close_authority,
            envelope=close_envelope,
            identity_children=exact_close_children,
            registry=provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS,
            lock=provider_workflows_module._CLOSE_COMPOSITION_LOCK,
        ):
            close_state_hash = canonical_close_state_hash(
                **close_values,
                composition_authority=exact_close_authority,
            )
            close_report = self._close_report(
                close_receipt,
                state_hash=close_state_hash,
            )
            close_material = issue_canonical_close_material(
                journal=self.journal,
                report_archive_root=self.root,
                report=close_report,
                composition_authority=exact_close_authority,
                **{
                    key: value
                    for key, value in close_values.items()
                    if key not in {"outcome", "reason_codes"}
                },
            )
            publisher = CanonicalJournalWorkflowPublisher(self.journal, self.root)
            result = publisher.issue_result(material=close_material)
            with self.assertRaisesRegex(
                WorkflowError,
                "canonical Journal publication",
            ):
                publisher.publish(
                    kind="CLOSE",
                    session_date=DAY,
                    generated_at=RETRIEVED_AT,
                    result=result,
                    material=close_material,
                )

    def test_composition_currentness_never_calls_mutable_timezone_code(self) -> None:
        premarket_receipt = self._receipt(suffix="hostile-time-premarket")
        snapshot = PremarketSnapshot((), False)
        premarket_values = {
            "session_date": DAY,
            "decision_at": DECISION_AT,
            "retrieved_at": DECISION_AT + timedelta(minutes=7),
            "validation_window_id": "phase1-window",
            "source_receipts": (premarket_receipt,),
            "snapshot": snapshot,
            "publication_decision": None,
            "primary_plan": None,
            "outcome": "NO TRADE",
            "reason_codes": ("NO_CANDIDATES",),
        }
        premarket_envelope = (
            provider_workflows_module._premarket_composition_envelope(
                **premarket_values
            )
        )
        premarket_authority = self._premarket_authority_from_envelope(
            premarket_envelope
        )
        premarket_children = (
            provider_workflows_module._premarket_composition_identity_children(
                snapshot=snapshot,
                source_receipts=(premarket_receipt,),
                publication_decision=None,
                primary_plan=None,
            )
        )
        with self._installed_composition_candidate(
            authority=premarket_authority,
            envelope=premarket_envelope,
            identity_children=premarket_children,
            registry=provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS,
            lock=provider_workflows_module._PREMARKET_COMPOSITION_LOCK,
        ) as premarket_candidate:
            def remove_premarket_authority() -> None:
                provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS.pop(
                    id(premarket_authority),
                    None,
                )

            hostile = _MutatingTimezone(remove_premarket_authority)
            object.__setattr__(
                premarket_authority,
                "retrieved_at",
                datetime(2026, 8, 24, 8, 52, tzinfo=hostile),
            )
            with self.assertRaisesRegex(
                CanonicalMaterialError,
                "composition authority",
            ):
                canonical_premarket_state_hash(
                    **premarket_values,
                    composition_authority=premarket_authority,
                )
            self.assertEqual(hostile.calls, 0)
            with provider_workflows_module._PREMARKET_COMPOSITION_LOCK:
                self.assertIs(
                    provider_workflows_module._ISSUED_PREMARKET_COMPOSITIONS.get(
                        id(premarket_authority)
                    ),
                    premarket_candidate,
                )

        close_receipt = self._receipt(
            suffix="hostile-time-close",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        replay_source, actual_state = self._actual()
        close_values = {
            "session_date": DAY,
            "review_at": REVIEW_AT,
            "retrieved_at": RETRIEVED_AT,
            "query_cutoff": QUERY_CUTOFF,
            "actual_state": actual_state,
            "actual_replay_source": replay_source,
            "positions": (),
            "source_receipts": (close_receipt,),
            "outcome": "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
            "reason_codes": EMPTY_CLOSE_REASONS,
        }
        close_envelope = provider_workflows_module._close_composition_envelope(
            **close_values
        )
        close_authority = self._close_authority_from_envelope(close_envelope)
        close_children = (
            provider_workflows_module._close_composition_identity_children(
                actual_state=actual_state,
                actual_replay_source=replay_source,
                positions=(),
                source_receipts=(close_receipt,),
            )
        )
        with self._installed_composition_candidate(
            authority=close_authority,
            envelope=close_envelope,
            identity_children=close_children,
            registry=provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS,
            lock=provider_workflows_module._CLOSE_COMPOSITION_LOCK,
        ) as close_candidate:
            def remove_close_authority() -> None:
                provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS.pop(
                    id(close_authority),
                    None,
                )

            hostile = _MutatingTimezone(remove_close_authority)
            object.__setattr__(
                close_authority,
                "retrieved_at",
                datetime(2026, 8, 24, 15, 38, tzinfo=hostile),
            )
            with self.assertRaisesRegex(
                CanonicalMaterialError,
                "composition authority",
            ):
                canonical_close_state_hash(
                    **close_values,
                    composition_authority=close_authority,
                )
            self.assertEqual(hostile.calls, 0)
            with provider_workflows_module._CLOSE_COMPOSITION_LOCK:
                self.assertIs(
                    provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS.get(
                        id(close_authority)
                    ),
                    close_candidate,
                )

    def test_exact_utc_and_zoneinfo_times_are_accepted(self) -> None:
        exact_times = (
            datetime(2026, 8, 24, 12, 45, tzinfo=UTC),
            datetime(2026, 8, 24, 8, 45, tzinfo=ET),
            datetime(
                2026,
                8,
                24,
                8,
                45,
                tzinfo=timezone(timedelta(hours=-4)),
            ),
        )
        for value in exact_times:
            with self.subTest(timezone=type(value.tzinfo).__name__):
                self.assertIs(
                    provider_workflows_module._require_time(value, "exact time"),
                    value,
                )
                self.assertEqual(
                    provider_workflows_module._value_fingerprint(value)[0],
                    "datetime",
                )

    def test_approved_receipt_sources_reach_premarket_and_close_authority_boundaries(
        self,
    ) -> None:
        approved = (
            (
                "PREMARKET",
                "ALPACA_DAILY_BARS",
                "https://data.alpaca.markets/v2/stocks/bars?"
                "symbols=AAPL&timeframe=1Day&"
                "start=2026-08-21T13%3A30%3A00Z&"
                "end=2026-08-24T12%3A45%3A00Z&"
                "adjustment=split&feed=sip&limit=10000",
                "alpaca",
                "sip",
            ),
            (
                "CLOSE",
                "ALPACA_HISTORICAL_QUOTES",
                "https://data.alpaca.markets/v2/stocks/quotes?"
                "symbols=AAPL&start=2026-08-24T19%3A09%3A00Z&"
                "end=2026-08-24T19%3A14%3A00Z&feed=sip&limit=10000",
                "alpaca",
                "sip",
            ),
            (
                "CLOSE",
                "ALPACA_INTRADAY_BARS",
                "https://data.alpaca.markets/v2/stocks/bars?"
                "symbols=AAPL&timeframe=1Min&"
                "start=2026-08-24T19%3A09%3A00Z&"
                "end=2026-08-24T19%3A14%3A00Z&"
                "adjustment=split&feed=sip&limit=10000",
                "alpaca",
                "sip",
            ),
            (
                "CLOSE",
                "ALPACA_LATEST_QUOTES",
                "https://data.alpaca.markets/v2/stocks/quotes/latest?"
                "symbols=AAPL&feed=iex",
                "alpaca",
                "iex",
            ),
            (
                "PREMARKET",
                "SEC_ARCHIVE",
                "https://www.sec.gov/Archives/edgar/data/320193/"
                "000032019326000001/aapl-20260813.htm",
                "U.S. Securities and Exchange Commission",
                "SEC_FILING_METADATA",
            ),
            (
                "PREMARKET",
                "SEC_ARCHIVE",
                "https://www.sec.gov/Archives/edgar/data/320193/"
                "000032019326000001/aapl-20260813.htm",
                "U.S. Securities and Exchange Commission",
                "SEC_ACCEPTANCE_METADATA",
            ),
            (
                "CLOSE",
                "SEC_SUBMISSIONS",
                "https://data.sec.gov/submissions/CIK0000320193.json",
                "U.S. Securities and Exchange Commission",
                "SEC_SUBMISSIONS_METADATA",
            ),
            (
                "CLOSE",
                "SEC_SUBMISSIONS",
                "https://data.sec.gov/submissions/CIK0000320193.json",
                "U.S. Securities and Exchange Commission",
                "SEC_ACCEPTANCE_METADATA",
            ),
            (
                "CLOSE",
                "SEC_SUBMISSIONS",
                "https://data.sec.gov/submissions/CIK0000320193.json",
                "U.S. Securities and Exchange Commission",
                "UNAVAILABLE",
            ),
            (
                "PREMARKET",
                "OFFICIAL_REFERENCE",
                "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
                "Nasdaq",
                "PRIMARY_METADATA",
            ),
            (
                "PREMARKET",
                "OFFICIAL_REFERENCE",
                "https://www.nasdaqtrader.com/rss.aspx?"
                "categorylist=2&feed=currentheadlines",
                "www.nasdaqtrader.com",
                "UNAVAILABLE",
            ),
            (
                "CLOSE",
                "OFFICIAL_REFERENCE",
                "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
                "Nasdaq",
                "PRIMARY_METADATA",
            ),
            (
                "CLOSE",
                "OFFICIAL_REFERENCE",
                "https://www.nyse.com/api/notifications/public/alerts?2=3",
                "New York Stock Exchange",
                "UNAVAILABLE",
            ),
            (
                "PREMARKET",
                "OFFICIAL_REFERENCE",
                "https://www.nyse.com/trade/hours-calendars",
                "www.nyse.com",
                "PRIMARY_METADATA",
            ),
            (
                "PREMARKET",
                "REVIEWED_EVIDENCE_REGISTRY",
                "urn:stock-monitor:reviewed-evidence-registry:release-2026.08.24",
                "operator-reviewed",
                None,
            ),
        )
        receipts: dict[str, object] = {}
        fixed_reference_roles = {
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": "TRADER_ALERT_HALT",
            "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": "CROSS_CHECK_CALENDAR",
            "https://www.nyse.com/api/notifications/public/alerts?2=3": "OPERATIONAL_STATUS",
            "https://www.nyse.com/trade/hours-calendars": "PRIMARY_CALENDAR",
        }
        for ordinal, (kind, source_type, source_uri, provider, feed) in enumerate(
            approved,
            start=1,
        ):
            receipt = self._receipt(
                suffix=f"approved-{ordinal}",
                source_uri=source_uri,
                source_type=source_type,
                provider=provider,
                feed=feed,
                details=(
                    {
                        "accession": None,
                        "issuer_cik": None,
                        "source_observation_id": f"approved-reference-{ordinal}",
                        "source_role": fixed_reference_roles[source_uri],
                        "symbol": None,
                        "timestamp_source": feed,
                    }
                    if source_type == "OFFICIAL_REFERENCE"
                    else None
                ),
                health_result=(
                    "REVIEWED"
                    if source_type == "REVIEWED_EVIDENCE_REGISTRY"
                    else "OK"
                ),
                source_time=(
                    DECISION_AT - timedelta(minutes=20)
                    if kind == "PREMARKET"
                    else REVIEW_AT - timedelta(minutes=20)
                ),
                retrieved_at=(
                    DECISION_AT - timedelta(minutes=1)
                    if kind == "PREMARKET"
                    else QUERY_CUTOFF - timedelta(minutes=1)
                ),
            )
            with self.subTest(kind=kind, source_type=source_type, source_uri=source_uri):
                self.assertEqual(canonical_source_receipts((receipt,)), (receipt,))
            receipts.setdefault(kind, receipt)

        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            issue_canonical_premarket_material(
                **self._premarket_issue_kwargs(receipts["PREMARKET"])
            )
        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            issue_canonical_close_material(
                **self._close_issue_kwargs(receipts["CLOSE"])
            )

    def test_subject_scoped_official_receipts_require_the_exact_reviewed_subject(
        self,
    ) -> None:
        source_uri = "https://investor.apple.com/investor-relations/faq/default.aspx"
        exact_details = {
            "accession": None,
            "issuer_cik": "0000320193",
            "source_observation_id": "issuer-ir-aapl",
            "source_role": "ISSUER_IR:AAPL",
            "symbol": "AAPL",
            "timestamp_source": "PRIMARY_METADATA",
        }
        exact = self._receipt(
            suffix="scoped-reference-exact",
            source_uri=source_uri,
            source_type="OFFICIAL_REFERENCE",
            provider="Apple Inc.",
            feed="PRIMARY_METADATA",
            details=exact_details,
        )
        self.assertEqual(canonical_source_receipts((exact,)), (exact,))

        mutations = (
            {"details": {**exact_details, "source_role": "ISSUER_IR:AMD"}},
            {"details": {**exact_details, "symbol": "AMD"}},
            {"details": {**exact_details, "issuer_cik": "0000002488"}},
            {"provider": "Advanced Micro Devices, Inc."},
            {"feed": "UNAVAILABLE"},
        )
        for ordinal, mutation in enumerate(mutations, start=1):
            values = {
                "source_uri": source_uri,
                "source_type": "OFFICIAL_REFERENCE",
                "provider": "Apple Inc.",
                "feed": "PRIMARY_METADATA",
                "details": exact_details,
                **mutation,
            }
            receipt = self._receipt(
                suffix=f"scoped-reference-swap-{ordinal}",
                **values,
            )
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                CanonicalMaterialError,
                "source identity",
            ):
                canonical_source_receipts((receipt,))

        fixed_uri = "https://www.nyse.com/trade/hours-calendars"
        fixed_details = {
            "accession": None,
            "issuer_cik": None,
            "source_observation_id": "primary-calendar",
            "source_role": "PRIMARY_CALENDAR",
            "symbol": None,
            "timestamp_source": "PRIMARY_METADATA",
        }
        for ordinal, details in enumerate(
            (
                {**fixed_details, "source_role": "OPERATIONAL_STATUS"},
                {**fixed_details, "symbol": "AAPL"},
            ),
            start=1,
        ):
            receipt = self._receipt(
                suffix=f"fixed-reference-swap-{ordinal}",
                source_uri=fixed_uri,
                source_type="OFFICIAL_REFERENCE",
                provider="New York Stock Exchange",
                feed="PRIMARY_METADATA",
                details=details,
            )
            with self.subTest(details=details), self.assertRaisesRegex(
                CanonicalMaterialError,
                "source identity",
            ):
                canonical_source_receipts((receipt,))

    def test_receipt_health_is_bound_to_the_exact_source_vocabulary(self) -> None:
        fixed_details = {
            "accession": None,
            "issuer_cik": None,
            "source_observation_id": "operational-status-unhealthy",
            "source_role": "OPERATIONAL_STATUS",
            "symbol": None,
            "timestamp_source": "UNAVAILABLE",
        }
        unhealthy_reference = self._receipt(
            suffix="unhealthy-reference",
            source_uri="https://www.nyse.com/api/notifications/public/alerts?2=3",
            source_type="OFFICIAL_REFERENCE",
            provider="New York Stock Exchange",
            feed="UNAVAILABLE",
            details=fixed_details,
            health_result="UNHEALTHY",
        )
        unhealthy_sec = self._receipt(
            suffix="unhealthy-sec",
            source_uri="https://data.sec.gov/submissions/CIK0000320193.json",
            source_type="SEC_SUBMISSIONS",
            provider="U.S. Securities and Exchange Commission",
            feed="UNAVAILABLE",
            health_result="UNHEALTHY",
        )
        for receipt in (unhealthy_reference, unhealthy_sec):
            with self.subTest(source_type=receipt.source_type):
                self.assertEqual(canonical_source_receipts((receipt,)), (receipt,))

        reviewed_registry = self._receipt(
            suffix="reviewed-registry-health",
            source_uri=(
                "urn:stock-monitor:reviewed-evidence-registry:"
                "release-2026.08.24"
            ),
            source_type="REVIEWED_EVIDENCE_REGISTRY",
            provider="operator-reviewed",
            feed=None,
            health_result="REVIEWED",
        )
        self.assertEqual(
            canonical_source_receipts((reviewed_registry,)),
            (reviewed_registry,),
        )

        invalid_health_cases = (
            {},
            {
                "source_uri": "https://data.sec.gov/submissions/CIK0000320193.json",
                "source_type": "SEC_SUBMISSIONS",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "UNAVAILABLE",
                "health_result": "REVIEWED",
            },
            {
                "source_uri": (
                    "urn:stock-monitor:reviewed-evidence-registry:"
                    "release-2026.08.24"
                ),
                "source_type": "REVIEWED_EVIDENCE_REGISTRY",
                "provider": "operator-reviewed",
                "feed": None,
                "health_result": "OK",
            },
            {
                "source_uri": "https://www.nyse.com/api/notifications/public/alerts?2=3",
                "source_type": "OFFICIAL_REFERENCE",
                "provider": "New York Stock Exchange",
                "feed": "UNAVAILABLE",
                "details": fixed_details,
                "health_result": "REVIEWED",
            },
        )
        for ordinal, overrides in enumerate(invalid_health_cases, start=1):
            values = {"health_result": "UNHEALTHY", **overrides}
            receipt = self._receipt(
                suffix=f"invalid-health-{ordinal}",
                **values,
            )
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                CanonicalMaterialError,
                "source identity",
            ):
                canonical_source_receipts((receipt,))

    def test_receipt_sources_outside_the_exact_positive_contract_are_rejected(
        self,
    ) -> None:
        valid_alpaca = (
            "https://data.alpaca.markets/v2/stocks/bars?"
            "symbols=AAPL&timeframe=1Day&"
            "start=2026-08-21T13%3A30%3A00Z&"
            "end=2026-08-24T12%3A45%3A00Z&"
            "adjustment=split&feed=sip&limit=10000"
        )
        valid_archive = (
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/aapl-20260813.htm"
        )
        cases = (
            {"source_uri": valid_alpaca.replace("https://", "http://", 1)},
            {"source_uri": valid_alpaca.replace("data.alpaca.markets", "data.alpaca.markets.evil")},
            {"source_uri": valid_alpaca.replace("https://", "https://user@", 1)},
            {"source_uri": valid_alpaca.replace("data.alpaca.markets", "data.alpaca.markets:443")},
            {"source_uri": valid_alpaca.replace("data.alpaca.markets", "data.alpaca.markets:bad")},
            {"source_uri": valid_alpaca + "#fragment"},
            {"source_uri": valid_alpaca.replace("/v2/stocks/bars", "/v2/stocks/bars/extra")},
            {"source_uri": valid_alpaca + "&unknown=1"},
            {"source_uri": valid_alpaca.replace("feed=sip", "feed=iex")},
            {"source_uri": valid_alpaca.replace("timeframe=1Day", "timeframe=1Min")},
            {"source_uri": valid_alpaca.replace("&limit=10000", "")},
            {"source_uri": valid_alpaca, "provider": "ALPACA"},
            {"source_uri": valid_alpaca, "provider": "synthetic"},
            {"source_uri": valid_alpaca, "feed": "SIP"},
            {"source_uri": valid_alpaca, "source_type": "ALPACA_HISTORICAL_TRADES"},
            {"source_uri": valid_alpaca, "source_type": "MOCK_ALPACA_DAILY_BARS"},
            {
                "source_uri": valid_archive.replace("https://", "http://", 1),
                "source_type": "SEC_ARCHIVE",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "SEC_FILING_METADATA",
            },
            {
                "source_uri": valid_archive.replace("www.sec.gov", "www.sec.gov.evil"),
                "source_type": "SEC_ARCHIVE",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "SEC_FILING_METADATA",
            },
            {
                "source_uri": valid_archive + "?download=1",
                "source_type": "SEC_ARCHIVE",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "SEC_FILING_METADATA",
            },
            {
                "source_uri": "https://data.sec.gov/submissions/CIK320193.json",
                "source_type": "SEC_SUBMISSIONS",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "SEC_SUBMISSIONS_METADATA",
            },
            {
                "source_uri": "https://data.sec.gov/submissions/CIK0000320193.json?x=1",
                "source_type": "SEC_SUBMISSIONS",
                "provider": "U.S. Securities and Exchange Commission",
                "feed": "SEC_SUBMISSIONS_METADATA",
            },
            {
                "source_uri": "https://www.nyse.com/trade/hours-calendars/extra",
                "source_type": "OFFICIAL_REFERENCE",
                "provider": "New York Stock Exchange",
                "feed": "PRIMARY_METADATA",
            },
            {
                "source_uri": "https://www.nyse.com/api/notifications/public/alerts?2=4",
                "source_type": "OFFICIAL_REFERENCE",
                "provider": "New York Stock Exchange",
                "feed": "UNAVAILABLE",
            },
            {
                "source_uri": "https://www.nyse.com/trade/hours-calendars",
                "source_type": "OFFICIAL_REFERENCE",
                "provider": "NYSE",
                "feed": "PRIMARY_METADATA",
            },
            {
                "source_uri": "urn:stock-monitor:reviewed-evidence-registry:unsafe/id",
                "source_type": "REVIEWED_EVIDENCE_REGISTRY",
                "provider": "operator-reviewed",
                "feed": None,
            },
            {
                "source_uri": "urn:stock-monitor:reviewed-evidence-registry:safe-id",
                "source_type": "REVIEWED_EVIDENCE_REGISTRY",
                "provider": "operator_reviewed",
                "feed": None,
            },
            {
                "source_uri": "urn:stock-monitor:reviewed-evidence-registry:safe-id",
                "source_type": "REVIEWED_EVIDENCE_REGISTRY",
                "provider": "operator-reviewed",
                "feed": "PRIMARY_METADATA",
            },
            {"source_uri": "https://example.test/source", "source_type": "SYNTHETIC"},
        )
        for ordinal, overrides in enumerate(cases, start=1):
            receipt = self._receipt(suffix=f"unapproved-{ordinal}", **overrides)
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                CanonicalMaterialError,
                "source identity",
            ):
                canonical_source_receipts((receipt,))

    def test_fixture_receipt_provider_source_type_and_feed_are_rejected(self) -> None:
        cases = (
            {"provider": "FIXTURE"},
            {"source_type": "RECORDED_SCENARIO"},
            {"feed": "recorded-scenario"},
        )
        for ordinal, overrides in enumerate(cases, start=1):
            receipt = self._receipt(suffix=f"fixture-{ordinal}", **overrides)
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                CanonicalMaterialError, "source identity"
            ):
                issue_canonical_premarket_material(
                    **self._premarket_issue_kwargs(receipt)
                )
        close_receipt = self._receipt(
            suffix="fixture-close",
            source_type="RECORDED_SCENARIO",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "source identity"):
            issue_canonical_close_material(
                **self._close_issue_kwargs(close_receipt)
            )

    def test_receipts_are_bounded_by_economic_query_and_retrieval_cutoffs(self) -> None:
        premarket_cases = (
            {
                "source_time": DECISION_AT + timedelta(seconds=1),
                "retrieved_at": DECISION_AT + timedelta(seconds=2),
            },
            {
                "source_time": DECISION_AT - timedelta(minutes=1),
                "retrieved_at": DECISION_AT + timedelta(minutes=8),
            },
        )
        for ordinal, overrides in enumerate(premarket_cases, start=1):
            receipt = self._receipt(suffix=f"premarket-time-{ordinal}", **overrides)
            with self.subTest(kind="PREMARKET", overrides=overrides), self.assertRaisesRegex(
                CanonicalMaterialError, "cutoff|retrieval envelope"
            ):
                canonical_premarket_state_hash(
                    session_date=DAY,
                    decision_at=DECISION_AT,
                    retrieved_at=DECISION_AT + timedelta(minutes=7),
                    snapshot=PremarketSnapshot((), False),
                    source_receipts=(receipt,),
                    publication_decision=None,
                    primary_plan=None,
                    validation_window_id="phase1-window",
                    outcome="NO TRADE",
                    reason_codes=("NO_CANDIDATES",),
                    composition_authority=self._premarket_authority(),
                )

        close_cases = (
            {
                "source_time": REVIEW_AT + timedelta(seconds=1),
                "retrieved_at": REVIEW_AT + timedelta(seconds=2),
            },
            {
                "source_time": REVIEW_AT - timedelta(minutes=1),
                "retrieved_at": QUERY_CUTOFF + timedelta(seconds=1),
            },
        )
        replay_source, actual_state = self._actual()
        for ordinal, overrides in enumerate(close_cases, start=1):
            receipt = self._receipt(suffix=f"close-time-{ordinal}", **overrides)
            with self.subTest(kind="CLOSE", overrides=overrides), self.assertRaisesRegex(
                CanonicalMaterialError, "economic cutoff|query cutoff"
            ):
                canonical_close_state_hash(
                    session_date=DAY,
                    review_at=REVIEW_AT,
                    retrieved_at=RETRIEVED_AT,
                    query_cutoff=QUERY_CUTOFF,
                    actual_state=actual_state,
                    actual_replay_source=replay_source,
                    positions=(),
                    source_receipts=(receipt,),
                    outcome="PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                    reason_codes=EMPTY_CLOSE_REASONS,
                    composition_authority=self._close_authority(),
                )

    def test_new_york_session_identity_dst_clocks_and_calendar_gaps_fail_closed(self) -> None:
        receipt = self._receipt()
        next_utc_day = datetime(2026, 8, 25, 1, 0, tzinfo=UTC)
        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            canonical_premarket_state_hash(
                session_date=DAY,
                decision_at=DECISION_AT,
                retrieved_at=next_utc_day,
                snapshot=PremarketSnapshot((), False),
                source_receipts=(receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="phase1-window",
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                composition_authority=self._premarket_authority(
                    retrieved_at=next_utc_day
                ),
            )
        with self.assertRaisesRegex(CanonicalMaterialError, "New York session"):
            canonical_premarket_state_hash(
                session_date=DAY,
                decision_at=datetime(2026, 8, 24, 2, 0, tzinfo=UTC),
                retrieved_at=datetime(2026, 8, 24, 3, 0, tzinfo=UTC),
                snapshot=PremarketSnapshot((), False),
                source_receipts=(receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="phase1-window",
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                composition_authority=self._premarket_authority(),
            )

        winter_day = date(2026, 12, 14)
        winter_decision = datetime(2026, 12, 14, 13, 45, tzinfo=UTC)
        winter_retrieved = winter_decision + timedelta(minutes=7)
        winter_receipt = self._receipt(
            suffix="winter",
            source_time=winter_decision - timedelta(minutes=20),
            retrieved_at=winter_decision - timedelta(minutes=1),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            canonical_premarket_state_hash(
                session_date=winter_day,
                decision_at=winter_decision,
                retrieved_at=winter_retrieved,
                snapshot=PremarketSnapshot((), False),
                source_receipts=(winter_receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="winter-window",
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                composition_authority=self._premarket_authority(
                    session_date=winter_day,
                    decision_at=winter_decision,
                    retrieved_at=winter_retrieved,
                ),
            )
        with self.assertRaisesRegex(CanonicalMaterialError, "08:45"):
            canonical_premarket_state_hash(
                session_date=winter_day,
                decision_at=datetime(2026, 12, 14, 12, 45, tzinfo=UTC),
                retrieved_at=winter_retrieved,
                snapshot=PremarketSnapshot((), False),
                source_receipts=(winter_receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="winter-window",
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                composition_authority=self._premarket_authority(),
            )

        replay_source, actual_state = self._actual()
        close_receipt = self._receipt(
            suffix="close-clock",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "12:30 or 15:30"):
            canonical_close_state_hash(
                session_date=DAY,
                review_at=REVIEW_AT - timedelta(minutes=1),
                retrieved_at=RETRIEVED_AT,
                query_cutoff=QUERY_CUTOFF,
                actual_state=actual_state,
                actual_replay_source=replay_source,
                positions=(),
                source_receipts=(close_receipt,),
                outcome="PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                reason_codes=EMPTY_CLOSE_REASONS,
                composition_authority=self._close_authority(),
            )

        early_review = datetime(2026, 8, 24, 12, 30, tzinfo=ET)
        early_receipt = self._receipt(
            suffix="early-close-clock",
            source_time=early_review - timedelta(minutes=20),
            retrieved_at=early_review + timedelta(minutes=1),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            canonical_close_state_hash(
                session_date=DAY,
                review_at=early_review,
                retrieved_at=RETRIEVED_AT,
                query_cutoff=QUERY_CUTOFF,
                actual_state=actual_state,
                actual_replay_source=replay_source,
                positions=(),
                source_receipts=(early_receipt,),
                outcome="PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                reason_codes=EMPTY_CLOSE_REASONS,
                composition_authority=self._close_authority(review_at=early_review),
            )

        holiday = date(2026, 12, 25)
        holiday_decision = datetime(2026, 12, 25, 8, 45, tzinfo=ET)
        holiday_receipt = self._receipt(
            suffix="holiday",
            source_time=holiday_decision - timedelta(minutes=20),
            retrieved_at=holiday_decision - timedelta(minutes=1),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "composition authority"):
            canonical_premarket_state_hash(
                session_date=holiday,
                decision_at=holiday_decision,
                retrieved_at=holiday_decision + timedelta(minutes=7),
                snapshot=PremarketSnapshot((), False),
                source_receipts=(holiday_receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="holiday-window",
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                composition_authority=self._premarket_authority(
                    session_date=holiday,
                    decision_at=holiday_decision,
                    retrieved_at=holiday_decision + timedelta(minutes=7),
                ),
            )

    def test_receipt_order_is_canonical_and_report_order_must_match_exactly(self) -> None:
        first = self._receipt(suffix="order-first")
        second = self._receipt(suffix="order-second")
        ordered = canonical_source_receipts((second, first))
        expected = tuple(
            sorted(
                (first, second),
                key=lambda receipt: (receipt.observation_sha256, receipt.row_id),
            )
        )
        self.assertEqual(ordered, expected)
        self.assertEqual(
            canonical_source_digest((first, second)),
            canonical_source_digest((second, first)),
        )

        snapshot = PremarketSnapshot((), False)
        report = self._premarket_report(
            ordered[0],
            snapshot=snapshot,
            observation_ids=tuple(
                receipt.observation_sha256 for receipt in ordered
            ),
        )
        material = CanonicalPremarketMaterial(
            session_date=DAY,
            decision_at=DECISION_AT,
            retrieved_at=DECISION_AT + timedelta(minutes=7),
            snapshot=snapshot,
            report=report,
            source_receipts=ordered,
            publication_decision=None,
            primary_plan=None,
            validation_window_id="phase1-window",
            state_hash=report.state_hash,
            source_digest=canonical_source_digest(ordered),
            material_digest="c" * 64,
            composition_authority=self._premarket_authority(),
        )
        self.assertFalse(
            is_issued_canonical_material(
                material,
                journal=self.journal,
                report_archive_root=self.root,
            )
        )

        reversed_report = self._premarket_report(
            ordered[0],
            snapshot=snapshot,
            observation_ids=tuple(
                receipt.observation_sha256 for receipt in reversed(ordered)
            ),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "evidence"):
            replace(material, report=reversed_report)
        with self.assertRaisesRegex(CanonicalMaterialError, "canonical order"):
            replace(material, source_receipts=tuple(reversed(ordered)))

    def test_premarket_breaker_requires_an_exact_bool(self) -> None:
        for value in (0, 1, "false", None):
            with self.subTest(value=value), self.assertRaisesRegex(
                TypeError, "breaker_active"
            ):
                PremarketSnapshot((), value)  # type: ignore[arg-type]

    def test_close_matrix_derives_pairwise_precedence_and_exact_reasons(
        self,
    ) -> None:
        _replay_source, base_state = self._actual()
        branches = (
            (
                "RECONCILIATION_REQUIRED",
                replace(
                    base_state,
                    reconciliation_reasons=(
                        "ACTUAL_RECONCILIATION_REQUIRED",
                        "SHARED_RECONCILIATION_REASON",
                    ),
                ),
                self._unverified_close_position(
                    symbol="AAPL",
                    status="RECONCILIATION_REQUIRED",
                    reasons=(
                        "SHARED_RECONCILIATION_REASON",
                        "POSITION_RECONCILIATION_REQUIRED",
                    ),
                ),
                "RECONCILIATION REQUIRED",
                "RECONCILIATION_REQUIRED",
                5,
            ),
            (
                "POSITION_UNVERIFIED",
                base_state,
                self._unverified_close_position(
                    symbol="AMD",
                    reasons=("PLAN_LINEAGE_UNAVAILABLE",),
                ),
                "POSITION UNVERIFIED",
                "POSITION_UNVERIFIED",
                4,
            ),
            (
                "STOP_UNVERIFIED",
                base_state,
                self._verified_close_position(
                    symbol="MSFT",
                    stop=None,
                    reasons=("STOP_CONFIRMATION_UNAVAILABLE",),
                ),
                "STOP UNVERIFIED",
                "STOP_UNVERIFIED",
                4,
            ),
            (
                "DATA_UNAVAILABLE",
                base_state,
                self._unverified_close_position(
                    symbol="NVDA",
                    status="DATA_UNAVAILABLE",
                    reasons=("SIP_MARK_UNAVAILABLE",),
                ),
                "DATA UNAVAILABLE",
                "DATA_UNAVAILABLE",
                3,
            ),
            (
                "EXIT",
                base_state,
                self._verified_close_position(
                    symbol="QQQ",
                    action="EXIT",
                    reasons=("MAX_HOLD_SESSIONS_REACHED",),
                ),
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
                "EXIT",
                0,
            ),
            (
                "TIGHTEN_STOP",
                base_state,
                self._verified_close_position(
                    symbol="SPY",
                    action="TIGHTEN_STOP",
                    reasons=("TRAILING_STOP_ADVANCE",),
                ),
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
                "TIGHTEN_STOP",
                0,
            ),
            (
                "HOLD",
                base_state,
                self._verified_close_position(
                    symbol="VTI",
                    reasons=("POSITION_REVIEW_COMPLETE",),
                ),
                "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                "HOLD",
                0,
            ),
        )
        for (
            _status,
            actual_state,
            position,
            report_outcome,
            workflow_outcome,
            exit_code,
        ) in branches:
            expected_reasons = tuple(
                dict.fromkeys(
                    (
                        *actual_state.reconciliation_reasons,
                        *position.reason_codes,
                        *(
                            ("MANUAL_VERIFICATION_REQUIRED",)
                            if workflow_outcome in {"EXIT", "TIGHTEN_STOP", "HOLD"}
                            else ()
                        ),
                    )
                )
            )
            with self.subTest(status=_status, solo=True):
                self.assertEqual(
                    provider_workflows_module._canonical_close_projection(
                        actual_state,
                        (position,),
                    ),
                    (
                        report_outcome,
                        workflow_outcome,
                        exit_code,
                        expected_reasons,
                    ),
                )

        for higher_index, higher in enumerate(branches):
            for lower in branches[higher_index + 1 :]:
                actual_state = higher[1]
                positions = (higher[2], lower[2])
                expected_reasons = tuple(
                    dict.fromkeys(
                        (
                            *actual_state.reconciliation_reasons,
                            *higher[2].reason_codes,
                            *lower[2].reason_codes,
                            *(
                                ("MANUAL_VERIFICATION_REQUIRED",)
                                if higher[4] in {"EXIT", "TIGHTEN_STOP", "HOLD"}
                                else ()
                            ),
                        )
                    )
                )
                with self.subTest(higher=higher[0], lower=lower[0]):
                    self.assertEqual(
                        provider_workflows_module._canonical_close_projection(
                            actual_state,
                            positions,
                        ),
                        (higher[3], higher[4], higher[5], expected_reasons),
                    )

        reconciliation_position = branches[0][2]
        self.assertEqual(
            provider_workflows_module._canonical_close_projection(
                base_state,
                (reconciliation_position,),
            )[:3],
            ("RECONCILIATION REQUIRED", "RECONCILIATION_REQUIRED", 5),
        )
        self.assertEqual(
            provider_workflows_module._canonical_close_projection(base_state, ()),
            (
                "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                "HOLD",
                0,
                EMPTY_CLOSE_REASONS,
            ),
        )
        for action in ("HOLD", "EXIT", "TIGHTEN_STOP"):
            for reserved_reason in EMPTY_CLOSE_REASONS:
                with self.subTest(
                    action=action,
                    reserved_reason=reserved_reason,
                ), self.assertRaisesRegex(
                    CanonicalMaterialError,
                    "coordinator-only reason",
                ):
                    provider_workflows_module._canonical_close_projection(
                        base_state,
                        (
                            self._verified_close_position(
                                action=action,
                                reasons=(
                                    reserved_reason,
                                    "POSITION_REVIEW_COMPLETE",
                                ),
                            ),
                        ),
                    )
        for reserved_reason in EMPTY_CLOSE_REASONS:
            with self.subTest(
                source="actual-reconciliation",
                reserved_reason=reserved_reason,
            ), self.assertRaisesRegex(
                CanonicalMaterialError,
                "coordinator-only reason",
            ):
                provider_workflows_module._canonical_close_projection(
                    replace(
                        base_state,
                        reconciliation_reasons=(reserved_reason,),
                    ),
                    (),
                )
            with self.subTest(
                source="unverified-position",
                reserved_reason=reserved_reason,
            ), self.assertRaisesRegex(
                CanonicalMaterialError,
                "coordinator-only reason",
            ):
                provider_workflows_module._canonical_close_projection(
                    base_state,
                    (
                        self._unverified_close_position(
                            reasons=(
                                reserved_reason,
                                "PLAN_LINEAGE_UNAVAILABLE",
                            ),
                        ),
                    ),
                )

    def test_close_envelope_rejects_report_semantics_outside_the_exact_matrix(
        self,
    ) -> None:
        receipt = self._receipt(
            suffix="close-exact-matrix",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        replay_source, actual_state = self._actual()
        position = self._verified_close_position()
        exact_reasons = (
            "POSITION_REVIEW_COMPLETE",
            "MANUAL_VERIFICATION_REQUIRED",
        )
        exact = {
            "session_date": DAY,
            "review_at": REVIEW_AT,
            "retrieved_at": RETRIEVED_AT,
            "query_cutoff": QUERY_CUTOFF,
            "actual_state": actual_state,
            "actual_replay_source": replay_source,
            "positions": (position,),
            "source_receipts": (receipt,),
            "outcome": "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
            "reason_codes": exact_reasons,
        }
        envelope = provider_workflows_module._close_composition_envelope(**exact)
        self.assertEqual(envelope.outcome, exact["outcome"])
        self.assertEqual(envelope.reason_codes, exact_reasons)

        contradictions = (
            {"reason_codes": ("INVENTED_REASON",)},
            {
                "outcome": "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
            },
            {"reason_codes": ("POSITION_REVIEW_COMPLETE",)},
        )
        for contradiction in contradictions:
            with self.subTest(contradiction=contradiction), self.assertRaisesRegex(
                CanonicalMaterialError,
                "close outcome and reasons",
            ):
                provider_workflows_module._close_composition_envelope(
                    **{**exact, **contradiction}
                )

    def test_result_projection_uses_exhaustive_outcome_reason_exit_matrices(self) -> None:
        receipt = self._receipt()
        premarket_cases = (
            (False, "NO TRADE", ("NO_CANDIDATES",), "NO_TRADE", 0),
            (False, "NO TRADE", ("MARKET_CLOSED",), "NO_TRADE", 0),
            (True, "NO TRADE", ("ACTIVE_BREAKER",), "NO_TRADE", 4),
            (
                False,
                "NO NEW TRADE - DATA UNAVAILABLE",
                ("DATA_UNAVAILABLE",),
                "DATA_UNAVAILABLE",
                3,
            ),
        )
        for breaker, report_outcome, reasons, outcome, exit_code in premarket_cases:
            snapshot = PremarketSnapshot((), breaker)
            report = self._premarket_report(
                receipt, snapshot=snapshot, outcome=report_outcome, reasons=reasons
            )
            material = CanonicalPremarketMaterial(
                session_date=DAY,
                decision_at=DECISION_AT,
                retrieved_at=DECISION_AT + timedelta(minutes=7),
                snapshot=snapshot,
                report=report,
                source_receipts=(receipt,),
                publication_decision=None,
                primary_plan=None,
                validation_window_id="phase1-window",
                state_hash=report.state_hash,
                source_digest=canonical_source_digest((receipt,)),
                material_digest="d" * 64,
                composition_authority=self._premarket_authority(
                    outcome=report_outcome, reasons=reasons
                ),
            )
            self.assertEqual(
                _canonical_result_projection(material), (outcome, exit_code, reasons)
            )

        invalid_premarket_reasons = (
            ("NO_CANDIDATES", "MARKET_CLOSED"),
            ("DATA_UNAVAILABLE", "SOURCE_CHECK_FAILED"),
            ("INVENTED_REASON",),
        )
        for reasons in invalid_premarket_reasons:
            report_outcome = (
                "NO NEW TRADE - DATA UNAVAILABLE"
                if "DATA_UNAVAILABLE" in reasons
                else "NO TRADE"
            )
            with self.subTest(reasons=reasons), self.assertRaises(
                (CanonicalMaterialError, WorkflowError)
            ):
                authority = self._premarket_authority(
                    outcome=report_outcome, reasons=reasons
                )
                snapshot = PremarketSnapshot((), False)
                report = self._premarket_report(
                    receipt,
                    snapshot=snapshot,
                    outcome=report_outcome,
                    reasons=reasons,
                )
                material = CanonicalPremarketMaterial(
                    session_date=DAY,
                    decision_at=DECISION_AT,
                    retrieved_at=DECISION_AT + timedelta(minutes=7),
                    snapshot=snapshot,
                    report=report,
                    source_receipts=(receipt,),
                    publication_decision=None,
                    primary_plan=None,
                    validation_window_id="phase1-window",
                    state_hash=report.state_hash,
                    source_digest=canonical_source_digest((receipt,)),
                    material_digest="e" * 64,
                    composition_authority=authority,
                )
                _canonical_result_projection(material)

        close_receipt = self._receipt(
            suffix="close-matrix",
            source_time=REVIEW_AT - timedelta(minutes=20),
            retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
        )
        replay_source, actual_state = self._actual()
        close_cases = (
            (
                replace(
                    actual_state,
                    reconciliation_reasons=("ACTUAL_RECONCILIATION_REQUIRED",),
                ),
                (),
                {"reconciliation_required": True},
                "RECONCILIATION REQUIRED",
                "RECONCILIATION_REQUIRED",
                5,
                ("ACTUAL_RECONCILIATION_REQUIRED",),
            ),
            (
                actual_state,
                (self._unverified_close_position(),),
                {"position_verified": False},
                "POSITION UNVERIFIED",
                "POSITION_UNVERIFIED",
                4,
                ("PLAN_LINEAGE_UNAVAILABLE",),
            ),
            (
                actual_state,
                (
                    self._verified_close_position(
                        stop=None,
                        reasons=("STOP_CONFIRMATION_UNAVAILABLE",),
                    ),
                ),
                {"stop_verified": False},
                "STOP UNVERIFIED",
                "STOP_UNVERIFIED",
                4,
                ("STOP_CONFIRMATION_UNAVAILABLE",),
            ),
            (
                actual_state,
                (
                    self._unverified_close_position(
                        status="DATA_UNAVAILABLE",
                        reasons=("SIP_MARK_UNAVAILABLE",),
                    ),
                ),
                {"data_available": False},
                "DATA UNAVAILABLE",
                "DATA_UNAVAILABLE",
                3,
                ("SIP_MARK_UNAVAILABLE",),
            ),
            (
                actual_state,
                (
                    self._verified_close_position(
                        action="EXIT",
                        reasons=("MAX_HOLD_SESSIONS_REACHED",),
                    ),
                ),
                {"exit_due": True},
                "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
                "EXIT",
                0,
                (
                    "MAX_HOLD_SESSIONS_REACHED",
                    "MANUAL_VERIFICATION_REQUIRED",
                ),
            ),
            (
                actual_state,
                (
                    self._verified_close_position(
                        action="TIGHTEN_STOP",
                        reasons=("TRAILING_STOP_ADVANCE",),
                    ),
                ),
                {"tighten_stop_due": True},
                "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
                "TIGHTEN_STOP",
                0,
                ("TRAILING_STOP_ADVANCE", "MANUAL_VERIFICATION_REQUIRED"),
            ),
            (
                actual_state,
                (self._verified_close_position(),),
                {},
                "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                "HOLD",
                0,
                ("POSITION_REVIEW_COMPLETE", "MANUAL_VERIFICATION_REQUIRED"),
            ),
            (
                actual_state,
                (),
                {},
                "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                "HOLD",
                0,
                EMPTY_CLOSE_REASONS,
            ),
        )
        for (
            case_state,
            positions,
            flags,
            report_outcome,
            outcome,
            exit_code,
            reasons,
        ) in close_cases:
            report = self._close_report(
                close_receipt,
                reasons=reasons,
                positions=positions,
                **flags,
            )
            self.assertEqual(report.outcome, report_outcome)
            material = CanonicalCloseMaterial(
                session_date=DAY,
                review_at=REVIEW_AT,
                retrieved_at=RETRIEVED_AT,
                query_cutoff=QUERY_CUTOFF,
                actual_state=case_state,
                actual_replay_source=replay_source,
                report=report,
                positions=positions,
                source_receipts=(close_receipt,),
                state_hash=report.state_hash,
                source_digest=canonical_source_digest((close_receipt,)),
                material_digest="f" * 64,
                composition_authority=self._close_authority(
                    outcome=report_outcome, reasons=reasons
                ),
            )
            self.assertEqual(
                _canonical_result_projection(material), (outcome, exit_code, reasons)
            )

    def test_publication_plan_maps_storage_and_uses_stored_report_identity(self) -> None:
        receipt = self._receipt()
        report = self._premarket_report(receipt, snapshot=PremarketSnapshot((), False))
        plan = CanonicalPublicationPlan(
            workflow_kind="PREMARKET",
            storage_kind="MORNING",
            session_date=DAY,
            generated_at=DECISION_AT + timedelta(minutes=7),
            economic_at=DECISION_AT,
            retrieved_at=DECISION_AT + timedelta(minutes=7),
            material_digest="1" * 64,
            source_digest="2" * 64,
            source_observation_row_ids=(receipt.row_id,),
            rendered_report_id=report.report_id,
            report=report,
            material=object(),
        )
        stored_report_id = "3" * 64
        self.assertEqual(
            plan.archive_relative_path(stored_report_id),
            report_archive_relative_path("MORNING", DAY, stored_report_id),
        )
        with self.assertRaises(ValueError):
            replace(plan, generated_at=plan.generated_at + timedelta(seconds=1))

        close_report = self._close_report(
            self._receipt(
                suffix="close-plan",
                source_time=REVIEW_AT - timedelta(minutes=20),
                retrieved_at=QUERY_CUTOFF - timedelta(minutes=1),
            )
        )
        close_plan = replace(
            plan,
            workflow_kind="CLOSE",
            storage_kind="CLOSE",
            generated_at=RETRIEVED_AT,
            economic_at=REVIEW_AT,
            retrieved_at=RETRIEVED_AT,
            rendered_report_id=close_report.report_id,
            report=close_report,
        )
        self.assertEqual(
            close_plan.archive_relative_path(stored_report_id),
            report_archive_relative_path("CLOSE", DAY, stored_report_id),
        )

    def test_caller_material_result_copy_cross_owner_and_publish_are_rejected_without_writes(self) -> None:
        receipt = self._receipt()
        snapshot = PremarketSnapshot((), False)
        report = self._premarket_report(receipt, snapshot=snapshot)
        material = CanonicalPremarketMaterial(
            session_date=DAY,
            decision_at=DECISION_AT,
            retrieved_at=DECISION_AT + timedelta(minutes=7),
            snapshot=snapshot,
            report=report,
            source_receipts=(receipt,),
            publication_decision=None,
            primary_plan=None,
            validation_window_id="phase1-window",
            state_hash=report.state_hash,
            source_digest=canonical_source_digest((receipt,)),
            material_digest="9" * 64,
            composition_authority=self._premarket_authority(),
        )
        result = WorkflowResult(
            outcome="NO_TRADE",
            message=report.body,
            exit_code=0,
            reason_codes=("NO_CANDIDATES",),
            report=report,
            source_observation_row_ids=(receipt.row_id,),
            execution_mode="CANONICAL",
        )
        publisher = CanonicalJournalWorkflowPublisher(self.journal, self.root)
        other = Journal.open(Path(self.temporary.name) / "other.sqlite3")
        self.addCleanup(other.close)
        other_publisher = CanonicalJournalWorkflowPublisher(other, self.root)
        claims_before = self.journal.count("report_claims")

        for candidate_publisher, candidate_material, candidate_result in (
            (publisher, material, result),
            (publisher, replace(material), result),
            (publisher, material, replace(result)),
            (other_publisher, material, result),
        ):
            with self.subTest(
                cross_owner=candidate_publisher is other_publisher,
                material_copy=candidate_material is not material,
                result_copy=candidate_result is not result,
            ), self.assertRaises(WorkflowError):
                candidate_publisher.bind_result(
                    result=candidate_result, material=candidate_material
                )

        with self.assertRaises(WorkflowError):
            publisher.publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
                material=material,
            )
        with self.assertRaises(WorkflowError):
            JournalWorkflowPublisher(self.journal, self.root).publish(
                kind="PREMARKET",
                session_date=DAY,
                generated_at=DECISION_AT + timedelta(minutes=7),
                result=result,
            )

        self.assertEqual(self.journal.count("report_claims"), claims_before)
        self.assertEqual(self.journal.count("reports"), 0)
        self.assertEqual(self.journal.count("outbox"), 0)
        self.assertEqual(list(self.root.rglob("*.md")), [])


if __name__ == "__main__":
    unittest.main()
