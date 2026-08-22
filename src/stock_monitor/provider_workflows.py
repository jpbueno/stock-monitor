"""Owner-bound material transport for provider-backed monitor workflows.

This module deliberately stops before publication.  It seals the exact
Journal-backed inputs that a canonical coordinator has already composed so the
workflow layer can reject copied, stale, cross-Journal, or mutated material
before it acquires a durable report claim.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import threading
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo


_LOWER_SHA256 = frozenset("0123456789abcdef")
_MATERIAL_AUTHORITY_LOCK = threading.Lock()
_NEW_YORK = ZoneInfo("America/New_York")
_CANONICAL_REASON = re.compile(r"- `([A-Z][A-Z0-9_]{0,63})`\Z")
_CANONICAL_DATA_REASONS = frozenset(
    {
        "DATA_UNAVAILABLE",
        "PROVIDER_CHECK_FAILED",
        "SOURCE_CHECK_FAILED",
        "STALE_CALENDAR",
        "STALE_UNIVERSE",
    }
)
_PRIMARY_CAPACITY_REASONS = frozenset(
    {
        "COMBINED_RISK_CAP_REACHED",
        "DUPLICATE_TICKER_EXPOSURE",
        "EXPOSURE_CAP_REACHED",
        "POSITION_LIMIT_REACHED",
        "QUANTITY_BELOW_ONE",
        "SESSION_ENTRY_LIMIT_REACHED",
    }
)
_PREMARKET_OUTCOME_REASONS = {
    "CANDIDATES": frozenset(
        {("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED")}
    ),
    "NO TRADE": frozenset(
        {
            ("ACTIVE_BREAKER",),
            ("MARKET_CLOSED",),
            ("NO_CANDIDATES",),
            ("NO_PRIMARY_CAPACITY",),
        }
    ),
    "NO NEW TRADE - DATA UNAVAILABLE": frozenset(
        (reason,) for reason in _CANONICAL_DATA_REASONS
    ),
}
_CLOSE_BRANCHES = (
    (
        "RECONCILIATION_REQUIRED",
        "RECONCILIATION REQUIRED",
        "RECONCILIATION_REQUIRED",
        5,
    ),
    (
        "POSITION_UNVERIFIED",
        "POSITION UNVERIFIED",
        "POSITION_UNVERIFIED",
        4,
    ),
    ("STOP_UNVERIFIED", "STOP UNVERIFIED", "STOP_UNVERIFIED", 4),
    ("DATA_UNAVAILABLE", "DATA UNAVAILABLE", "DATA_UNAVAILABLE", 3),
    (
        "EXIT",
        "PROVISIONAL EXIT - VERIFY CURRENT ROBINHOOD PRICE",
        "EXIT",
        0,
    ),
    (
        "TIGHTEN_STOP",
        "PROVISIONAL TIGHTEN STOP - VERIFY CURRENT ROBINHOOD PRICE",
        "TIGHTEN_STOP",
        0,
    ),
    (
        "HOLD",
        "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
        "HOLD",
        0,
    ),
)
_CLOSE_BRANCH_PROJECTIONS = {
    status: (report_outcome, workflow_outcome, exit_code)
    for status, report_outcome, workflow_outcome, exit_code in _CLOSE_BRANCHES
}
_COORDINATOR_CLOSE_REASONS = frozenset(
    {"MANUAL_VERIFICATION_REQUIRED", "NO_ACTUAL_POSITIONS"}
)
_LEGACY_ACTUAL_ENTRY_CONTEXT_REASONS = frozenset(
    {"SIGNAL_PLAN_UNAVAILABLE", "AUTHORITY_CONTEXT_UNVERIFIED"}
)
_CLOSE_OUTCOME_EXIT_CODES = {
    report_outcome: exit_code
    for _status, report_outcome, _workflow_outcome, exit_code in _CLOSE_BRANCHES
}
_ALPACA_RECEIPT_CONTRACTS = {
    "ALPACA_DAILY_BARS": (
        "/v2/stocks/bars",
        "sip",
        ("symbols", "timeframe", "start", "end", "adjustment", "feed", "limit"),
        {"timeframe": "1Day", "adjustment": "split", "feed": "sip", "limit": "10000"},
    ),
    "ALPACA_INTRADAY_BARS": (
        "/v2/stocks/bars",
        "sip",
        ("symbols", "timeframe", "start", "end", "adjustment", "feed", "limit"),
        {"timeframe": "1Min", "adjustment": "split", "feed": "sip", "limit": "10000"},
    ),
    "ALPACA_HISTORICAL_QUOTES": (
        "/v2/stocks/quotes",
        "sip",
        ("symbols", "start", "end", "feed", "limit"),
        {"feed": "sip", "limit": "10000"},
    ),
    "ALPACA_LATEST_QUOTES": (
        "/v2/stocks/quotes/latest",
        "iex",
        ("symbols", "feed"),
        {"feed": "iex"},
    ),
}
_OFFICIAL_REFERENCE_SOURCES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": frozenset(
        {"Nasdaq", "www.nasdaqtrader.com"}
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": frozenset(
        {"New York Stock Exchange", "www.nyse.com"}
    ),
    "https://www.nyse.com/trade/hours-calendars": frozenset(
        {"New York Stock Exchange", "www.nyse.com"}
    ),
}
_OFFICIAL_REFERENCE_ROLES = {
    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
        "PRIMARY_HALT_FEED"
    ),
    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
        "TRADER_ALERT_HALT"
    ),
    "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": (
        "CROSS_CHECK_CALENDAR"
    ),
    "https://www.nyse.com/api/notifications/public/alerts?2=3": (
        "OPERATIONAL_STATUS"
    ),
    "https://www.nyse.com/trade/hours-calendars": "PRIMARY_CALENDAR",
}
_SEC_PUBLISHER = "U.S. Securities and Exchange Commission"
_SEC_RECEIPT_FEEDS = {
    "SEC_ARCHIVE": frozenset(
        {"SEC_ACCEPTANCE_METADATA", "SEC_FILING_METADATA"}
    ),
    "SEC_SUBMISSIONS": frozenset(
        {
            "SEC_ACCEPTANCE_METADATA",
            "SEC_SUBMISSIONS_METADATA",
            "UNAVAILABLE",
        }
    ),
}
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}\Z")
_CIK = re.compile(r"[0-9]{10}\Z")
_SAFE_SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SCOPED_REFERENCE_ROLE = re.compile(
    r"(?:ISSUER_IR|CORPORATE_ACTION):([A-Z][A-Z0-9.-]{0,14})\Z"
)
_OFFICIAL_REFERENCE_DETAIL_KEYS = frozenset(
    {
        "accession",
        "issuer_cik",
        "source_observation_id",
        "source_role",
        "symbol",
        "timestamp_source",
    }
)
_SEC_ARCHIVE_PATH = re.compile(
    r"/Archives/edgar/data/[1-9][0-9]*/[0-9]{18}/"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z"
)
_SEC_SUBMISSIONS_PATH = re.compile(r"/submissions/CIK[0-9]{10}\.json\Z")
_REVIEWED_REGISTRY_URI = re.compile(
    r"urn:stock-monitor:reviewed-evidence-registry:"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z"
)
_REVIEWED_ARTIFACT_URI = re.compile(
    r"stock-monitor://reviewed/"
    r"(?P<role>[a-z0-9][a-z0-9-]{0,63})/"
    r"(?P<digest>[0-9a-f]{64})\Z"
)
_UTC_QUERY_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{6})?Z\Z"
)
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-F]{2})")


class CanonicalMaterialError(ValueError):
    """Canonical workflow material is malformed or lacks current authority."""


@dataclass(frozen=True, slots=True)
class PremarketSourceBinding:
    """Requested decision role for one exact receipt/source-object pair.

    This caller-authored value is only a request.  It gains authority only
    through :func:`issue_canonical_premarket_source_binding_authority`.
    """

    receipt: object
    source: object
    decision_basis: str
    disclosure: object | None = None

    def __post_init__(self) -> None:
        from .journal import SourceObservationReceipt

        if type(self.receipt) is not SourceObservationReceipt:
            raise CanonicalMaterialError(
                "premarket source binding requires an exact receipt"
            )
        if self.source is None:
            raise CanonicalMaterialError(
                "premarket source binding requires an exact source object"
            )
        if self.decision_basis not in {
            "ECONOMIC_INPUT",
            "OPERATIONAL_HEALTH_ONLY",
        }:
            raise CanonicalMaterialError(
                "premarket source binding decision basis is invalid"
            )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketSourceBindingAuthority:
    """Journal-owner-bound capability for exact premarket source objects."""

    decision_at: datetime
    retrieved_at: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    decision_basis: tuple[tuple[int, str, str], ...]
    binding_digest: str

    def __post_init__(self) -> None:
        decision_at = _require_time(self.decision_at, "premarket decision time")
        retrieved_at = _require_time(self.retrieved_at, "premarket retrieval time")
        _validate_premarket_times(
            decision_at.astimezone(_NEW_YORK).date(),
            decision_at,
            retrieved_at,
        )
        _require_receipt_manifest(self.receipt_manifest)
        _require_premarket_decision_basis(self.decision_basis)
        if tuple(item[0] for item in self.decision_basis) != tuple(
            item[0] for item in self.receipt_manifest
        ):
            raise CanonicalMaterialError(
                "premarket decision basis conflicts with its receipt manifest"
            )
        _require_digest(self.binding_digest, "premarket source binding digest")


@dataclass(frozen=True, slots=True)
class PremarketProviderCollection:
    """Exact injectable output of one bounded premarket collection pass."""

    collected_at: datetime
    provider_cohorts: tuple[object, ...] = ()
    reference_sources: tuple[object, ...] = ()
    contexts: tuple[object, ...] = ()
    persisted_bindings: tuple[PremarketSourceBinding, ...] = ()
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        collected_at = _require_time(
            self.collected_at,
            "premarket collection terminal time",
        )
        for value in (
            self.provider_cohorts,
            self.reference_sources,
            self.contexts,
            self.persisted_bindings,
        ):
            if type(value) is not tuple:
                raise CanonicalMaterialError(
                    "premarket collection values must be exact tuples"
                )
        if self.failure_reason not in {
            None,
            "DATA_UNAVAILABLE",
            "PROVIDER_CHECK_FAILED",
            "SOURCE_CHECK_FAILED",
        }:
            raise CanonicalMaterialError(
                "premarket collection failure reason is unsupported"
            )
        if any(
            type(binding) is not PremarketSourceBinding
            for binding in self.persisted_bindings
        ):
            raise CanonicalMaterialError(
                "premarket persisted bindings are malformed"
            )
        if any(
            binding.receipt.retrieved_at > collected_at
            for binding in self.persisted_bindings
        ):
            raise CanonicalMaterialError(
                "premarket collection predates a persisted source receipt"
            )


@dataclass(frozen=True, slots=True)
class PremarketRiskResolution:
    """Exact risk authority selected after the final source reread."""

    breaker_state: object
    primary_plan: object | None = None
    publication_decision: object | None = None
    capacity_decision: object | None = None

    def __post_init__(self) -> None:
        if (self.primary_plan is None) != (self.publication_decision is None):
            raise CanonicalMaterialError(
                "premarket plan and publication decision must be paired"
            )
        if self.capacity_decision is not None and self.primary_plan is not None:
            raise CanonicalMaterialError(
                "premarket capacity and publication decisions are mutually exclusive"
            )


class PremarketCollectionError(RuntimeError):
    """Provider failure carrying every page already durably persisted."""

    def __init__(self, collection: PremarketProviderCollection) -> None:
        if (
            type(collection) is not PremarketProviderCollection
            or collection.failure_reason is None
        ):
            raise CanonicalMaterialError(
                "premarket collection error requires a failed handoff"
            )
        self.collection = collection
        super().__init__(collection.failure_reason)


@dataclass(frozen=True, slots=True)
class _PremarketValidationContext:
    """Exact Journal-derived Phase 1 authority for one open-session run."""

    breaker_state: object
    history_source: object
    validation_window_id: str
    calendar_resolver: object


@dataclass(frozen=True, slots=True)
class _PremarketSourceBindingCandidate:
    authority_reference: ReferenceType[object]
    authority_fingerprint: object
    journal_reference: ReferenceType[object]
    journal_generation: int
    receipt_candidates: tuple[object, ...]
    bindings: tuple[PremarketSourceBinding, ...]
    binding_manifest: tuple[object, ...]


_PREMARKET_SOURCE_BINDING_LOCK = threading.Lock()
_ISSUED_PREMARKET_SOURCE_BINDINGS: dict[
    int,
    _PremarketSourceBindingCandidate,
] = {}


def _freeze_scoped_reference_sources() -> frozenset[
    tuple[str, str | None, str, str]
]:
    """Snapshot the reviewed subject authorities into callback-free primitives."""
    from . import evidence as evidence_module

    authorities = evidence_module._SCOPED_REFERENCE_AUTHORITIES
    if type(authorities) is not dict or not authorities:
        raise CanonicalMaterialError(
            "reviewed scoped reference authorities are malformed"
        )
    frozen: set[tuple[str, str | None, str, str]] = set()
    for role, authority in authorities.items():
        if (
            type(role) is not str
            or _SCOPED_REFERENCE_ROLE.fullmatch(role) is None
            or type(authority) is not tuple
            or len(authority) != 2
        ):
            raise CanonicalMaterialError(
                "reviewed scoped reference authorities are malformed"
            )
        issuer_cik, sources = authority
        invalid_issuer_cik = issuer_cik is not None and (
            type(issuer_cik) is not str
            or _CIK.fullmatch(issuer_cik) is None
        )
        if (
            invalid_issuer_cik
            or type(sources) is not frozenset
            or not sources
        ):
            raise CanonicalMaterialError(
                "reviewed scoped reference authorities are malformed"
            )
        for source in sources:
            if (
                type(source) is not tuple
                or len(source) != 2
                or type(source[0]) is not str
                or type(source[1]) is not str
                or not source[0]
                or not source[1]
            ):
                raise CanonicalMaterialError(
                    "reviewed scoped reference authorities are malformed"
                )
            frozen.add((role, issuer_cik, source[0], source[1]))
    return frozenset(frozen)


_SCOPED_REFERENCE_SOURCES = _freeze_scoped_reference_sources()


class CanonicalWorkflowAdapter(Protocol):
    """Compose exact provider-backed material at fixed economic cutoffs."""

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalPremarketMaterial: ...

    def close_material(
        self,
        session_date: date,
        *,
        review_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalCloseMaterial: ...


class PremarketWorkflowCoordinator:
    """Injectable canonical premarket collection and composition boundary."""

    __slots__ = (
        "_collector",
        "_journal",
        "_project_root",
        "_report_archive_root",
        "_risk_resolver",
    )

    def __init__(
        self,
        *,
        journal: object,
        project_root: Path,
        report_archive_root: Path,
        collector: object,
        risk_resolver: object,
    ) -> None:
        from .journal import Journal

        if type(journal) is not Journal or getattr(journal, "_closed", True):
            raise CanonicalMaterialError(
                "premarket coordinator requires an open Journal"
            )
        for value, label in (
            (project_root, "project root"),
            (report_archive_root, "report archive root"),
        ):
            if type(value) is not type(Path()) or not value.is_absolute():
                raise CanonicalMaterialError(
                    f"premarket coordinator {label} must be an absolute path"
                )
        if not project_root.is_dir() or project_root.is_symlink():
            raise CanonicalMaterialError(
                "premarket coordinator project root is unavailable"
            )
        _canonical_archive_root(report_archive_root)
        if not callable(getattr(collector, "collect", None)):
            raise CanonicalMaterialError(
                "premarket coordinator collector is unavailable"
            )
        if not callable(getattr(risk_resolver, "resolve", None)) or not callable(
            getattr(risk_resolver, "validation_breaker", None)
        ):
            raise CanonicalMaterialError(
                "premarket coordinator risk resolver is unavailable"
            )
        self._journal = journal
        self._project_root = project_root
        self._report_archive_root = report_archive_root
        self._collector = collector
        self._risk_resolver = risk_resolver

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalPremarketMaterial:
        """Compose one canonical premarket material from stable Journal inputs."""
        from .evidence import EvidenceRegistryError, EvidenceUnavailableError
        from .market_calendar import CalendarError, load_current_market_calendar
        from .universe import UniverseError, load_current_universe
        from .workflows import WorkflowDataError

        session_date = _require_session(session_date)
        decision_at = _require_time(decision_at, "premarket decision time")
        retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
        _validate_premarket_times(session_date, decision_at, retrieved_at)
        try:
            calendar = load_current_market_calendar(
                project_root=self._project_root,
                as_of=session_date,
            )
        except (CalendarError, OSError) as error:
            raise WorkflowDataError("STALE_CALENDAR") from error
        session_open = calendar.is_open(session_date)
        if not session_open:
            raise CanonicalMaterialError(
                "premarket coordinator requires an open session"
            )
        try:
            validation = _resolve_premarket_validation_context(
                journal=self._journal,
                risk_resolver=self._risk_resolver,
                project_root=self._project_root,
                session_date=session_date,
                decision_at=decision_at,
                calendar=calendar,
            )
        except CalendarError as error:
            raise WorkflowDataError("STALE_CALENDAR") from error
        try:
            universe = load_current_universe(
                self._project_root,
                as_of=session_date,
            )
        except (UniverseError, OSError) as error:
            raise WorkflowDataError("STALE_UNIVERSE") from error
        try:
            evidence_release, evidence_failure = (
                _load_coordinator_evidence_release(
                    project_root=self._project_root,
                    decision_at=decision_at,
                    universe=universe,
                )
            )
            bindings = _persist_premarket_reviewed_bindings(
                journal=self._journal,
                project_root=self._project_root,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                retrieved_at=retrieved_at,
            )
        except (EvidenceRegistryError, EvidenceUnavailableError, OSError) as error:
            raise WorkflowDataError("SOURCE_CHECK_FAILED") from error
        if session_open:
            if evidence_failure is not None:
                validation = _resolve_premarket_validation_context(
                    journal=self._journal,
                    risk_resolver=self._risk_resolver,
                    project_root=self._project_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    calendar=calendar,
                    expected=validation,
                )
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(False),
                    publication_decision=None,
                    primary_plan=None,
                    outcome="NO NEW TRADE - DATA UNAVAILABLE",
                    reason_codes=(evidence_failure,),
                    phase1_replay_children=_evidence_phase1_children(
                        evidence_release
                    ),
                )
            required_symbols = tuple(
                record.symbol for record in universe.records if record.enabled
            )
            try:
                collection = self._collector.collect(
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    required_symbols=required_symbols,
                )
            except PremarketCollectionError as error:
                collection = error.collection
            except Exception as error:
                from .providers.http import (
                    NetworkPolicyError,
                    ProviderResponseError,
                )

                if isinstance(error, (NetworkPolicyError, ProviderResponseError)):
                    raise WorkflowDataError("PROVIDER_CHECK_FAILED") from error
                raise
            if type(collection) is not PremarketProviderCollection:
                raise CanonicalMaterialError(
                    "premarket collector returned an invalid result"
                )
            if (
                collection.collected_at < retrieved_at
                or collection.collected_at.astimezone(_NEW_YORK).date()
                != session_date
            ):
                raise CanonicalMaterialError(
                    "premarket collection terminal time is inconsistent"
                )
            retrieved_at = collection.collected_at
            bindings = _reread_premarket_bindings(
                self._journal,
                (*bindings, *collection.persisted_bindings),
            )
            validation = _resolve_premarket_validation_context(
                journal=self._journal,
                risk_resolver=self._risk_resolver,
                project_root=self._project_root,
                session_date=session_date,
                decision_at=decision_at,
                calendar=calendar,
                expected=validation,
            )
            if collection.failure_reason is not None:
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(False),
                    publication_decision=None,
                    primary_plan=None,
                    outcome="NO NEW TRADE - DATA UNAVAILABLE",
                    reason_codes=(collection.failure_reason,),
                    phase1_replay_children=_evidence_phase1_children(
                        evidence_release
                    ),
                )
            _require_complete_premarket_provider_handoff(
                collection,
                required_symbols,
            )
            _require_complete_premarket_reference_handoff(collection)
            ranked = _rank_premarket_contexts(
                collection.contexts,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                bindings=bindings,
            )
            cohort_data_unavailable = ranked is None
            ranked_candidates = () if ranked is None else ranked
            resolution = self._risk_resolver.resolve(
                journal=self._journal,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                ranked_candidates=ranked_candidates,
                validation_breaker=validation.breaker_state,
            )
            validation = _resolve_premarket_validation_context(
                journal=self._journal,
                risk_resolver=self._risk_resolver,
                project_root=self._project_root,
                session_date=session_date,
                decision_at=decision_at,
                calendar=calendar,
                expected=validation,
            )
            breaker_active = _validate_premarket_risk_resolution(
                resolution,
                session_date=session_date,
                decision_at=decision_at,
                ranked_candidates=ranked_candidates,
                validation=validation,
            )
            phase1_children = _premarket_phase1_children(
                evidence_release,
                resolution,
                validation,
            )
            if breaker_active:
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(True),
                    publication_decision=None,
                    primary_plan=None,
                    outcome="NO TRADE",
                    reason_codes=("ACTIVE_BREAKER",),
                    phase1_replay_children=phase1_children,
                    breaker_state=resolution.breaker_state,
                )
            if cohort_data_unavailable:
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(False),
                    publication_decision=None,
                    primary_plan=None,
                    outcome="NO NEW TRADE - DATA UNAVAILABLE",
                    reason_codes=("DATA_UNAVAILABLE",),
                    phase1_replay_children=phase1_children,
                    breaker_state=resolution.breaker_state,
                )
            if not ranked_candidates:
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(False),
                    publication_decision=None,
                    primary_plan=None,
                    outcome="NO TRADE",
                    reason_codes=("NO_CANDIDATES",),
                    phase1_replay_children=phase1_children,
                    breaker_state=resolution.breaker_state,
                )
            if resolution.capacity_decision is not None:
                return _issue_coordinator_premarket_branch(
                    journal=self._journal,
                    report_archive_root=self._report_archive_root,
                    session_date=session_date,
                    decision_at=decision_at,
                    retrieved_at=retrieved_at,
                    validation=validation,
                    bindings=bindings,
                    calendar=calendar,
                    universe=universe,
                    evidence_release=evidence_release,
                    snapshot=_empty_premarket_snapshot(False),
                    publication_decision=None,
                    primary_plan=None,
                    capacity_decision=resolution.capacity_decision,
                    capacity_candidate=ranked_candidates[0],
                    outcome="NO TRADE",
                    reason_codes=("NO_PRIMARY_CAPACITY",),
                    phase1_replay_children=phase1_children,
                    breaker_state=resolution.breaker_state,
                )
            return _issue_coordinator_premarket_branch(
                journal=self._journal,
                report_archive_root=self._report_archive_root,
                session_date=session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                validation=validation,
                bindings=bindings,
                calendar=calendar,
                universe=universe,
                evidence_release=evidence_release,
                snapshot=_premarket_candidate_snapshot(resolution),
                publication_decision=resolution.publication_decision,
                primary_plan=resolution.primary_plan,
                outcome="CANDIDATES",
                reason_codes=(
                    "PAPER_PLAN_ONLY",
                    "MANUAL_EXECUTION_REQUIRED",
                ),
                phase1_replay_children=phase1_children,
                breaker_state=resolution.breaker_state,
            )
        raise CanonicalMaterialError(
            "premarket coordinator session authority changed during composition"
        )


def _resolve_premarket_validation_context(
    *,
    journal: object,
    risk_resolver: object,
    project_root: Path,
    session_date: date,
    decision_at: datetime,
    calendar: object,
    expected: _PremarketValidationContext | None = None,
) -> _PremarketValidationContext:
    """Derive the validation window only from one current breaker-history read."""
    from . import journal as journal_module
    from . import risk as risk_module
    from .journal import Journal, Phase1BreakerHistorySource
    from .market_calendar import MarketCalendar

    if (
        type(journal) is not Journal
        or getattr(journal, "_closed", True)
        or type(project_root) is not type(Path())
        or not project_root.is_absolute()
        or type(calendar) is not MarketCalendar
    ):
        raise CanonicalMaterialError(
            "premarket validation authority context is unavailable"
        )
    issue = getattr(risk_resolver, "validation_breaker", None)
    if not callable(issue):
        raise CanonicalMaterialError(
            "premarket validation authority resolver is unavailable"
        )
    if expected is not None and type(expected) is not _PremarketValidationContext:
        raise CanonicalMaterialError(
            "premarket validation authority context is malformed"
        )
    if expected is None:
        resolver = _premarket_validation_calendar_resolver(
            project_root=project_root,
            session_date=session_date,
            calendar=calendar,
        )
    else:
        resolver = expected.calendar_resolver
    if (
        type(resolver) is not risk_module.SessionCalendarResolver
        or not resolver.release_verified
        or not any(
            candidate is calendar for candidate in resolver.calendars
        )
    ):
        raise CanonicalMaterialError(
            "premarket validation calendar authority is unavailable"
        )
    try:
        previous_session = resolver.previous_session(session_date)
    except risk_module.RiskBlock as error:
        from .market_calendar import CalendarError

        raise CalendarError(
            "premarket validation calendar coverage is missing"
        ) from error
    breaker = issue(
        journal=journal,
        session_date=session_date,
        decision_at=decision_at,
        calendar=calendar,
        calendar_resolver=resolver,
    )
    bindings = risk_module._phase1_bound_sources(breaker)
    if (
        type(breaker) is not risk_module.BreakerState
        or not risk_module.is_issued_breaker_state(breaker)
        or breaker.ledger_name != "CANONICAL"
        or breaker.as_of != previous_session
        or len(bindings) != 1
        or bindings[0][1] != "BREAKER_HISTORY"
        or type(bindings[0][0]) is not Phase1BreakerHistorySource
    ):
        raise CanonicalMaterialError(
            "premarket validation breaker authority is unavailable"
        )
    source = bindings[0][0]
    source_candidate = journal_module._journal_any_source_authority_candidate(
        source
    )
    if (
        source.ledger_name != "CANONICAL"
        or source.through_session != previous_session
        or source.query_cutoff != decision_at
        or source.calendar_digest != risk_module._calendar_digest(resolver)
        or breaker.history_digest != source.source_digest
        or _validation_window_id(source.validation_window_id)
        != source.validation_window_id
        or source_candidate is None
        or journal_module._current_journal_source_authority_owner(
            (source_candidate,)
        )
        is not journal
        or not journal_module._is_current_journal_authority_candidate_without_callbacks(
            source_candidate
        )
        or not risk_module._is_current_breaker_state_without_callbacks(breaker)
    ):
        raise CanonicalMaterialError(
            "premarket validation history has the wrong owner or is not current"
        )
    resolved = _PremarketValidationContext(
        breaker_state=breaker,
        history_source=source,
        validation_window_id=source.validation_window_id,
        calendar_resolver=resolver,
    )
    if expected is not None and (
        type(expected) is not _PremarketValidationContext
        or resolved.calendar_resolver is not expected.calendar_resolver
        or resolved.validation_window_id != expected.validation_window_id
        or resolved.history_source.window_start_session
        != expected.history_source.window_start_session
        or resolved.history_source.window_start_source_id
        != expected.history_source.window_start_source_id
        or resolved.history_source.source_digest
        != expected.history_source.source_digest
    ):
        raise CanonicalMaterialError(
            "premarket validation authority changed during collection"
        )
    return resolved


def _premarket_validation_calendar_resolver(
    *,
    project_root: Path,
    session_date: date,
    calendar: object,
) -> object:
    """Load only release-pinned adjacent coverage needed for the prior session."""
    from . import risk as risk_module
    from .market_calendar import CalendarError, load_current_market_calendar

    resolver = risk_module.SessionCalendarResolver((calendar,))
    try:
        resolver.previous_session(session_date)
        return resolver
    except risk_module.RiskBlock as error:
        if error.reason_code != "CALENDAR_COVERAGE_MISSING":
            raise CalendarError(
                "premarket validation calendar arithmetic failed"
            ) from error
    previous_year = session_date.year - 1
    adjacent = load_current_market_calendar(
        project_root,
        as_of=date(previous_year, 12, 31),
    )
    resolver = risk_module.SessionCalendarResolver((adjacent, calendar))
    try:
        resolver.previous_session(session_date)
    except risk_module.RiskBlock as error:
        raise CalendarError(
            "premarket validation calendar coverage is missing"
        ) from error
    return resolver
@dataclass(frozen=True, slots=True)
class ActualCloseCollection:
    """Durable hand-off from provider collection to close composition."""

    review_id: str
    collected_at: datetime

    def __post_init__(self) -> None:
        _require_digest(self.review_id, "actual close review identity")
        _require_time(self.collected_at, "actual close terminal collection time")


class ActualCloseSourceCollector(Protocol):
    """Collect and persist one complete close-review source cohort.

    The seam is intentionally narrower than an Alpaca client.  Implementations
    may page provider APIs internally, but they return only the identity of the
    exact Journal review that binds every receipt or explicit failure.
    """

    def collect_close_sources(
        self,
        *,
        journal: object,
        symbols: tuple[str, ...],
        session_date: date,
        review_at: datetime,
        mark_cutoff: datetime,
        command_started_at: datetime,
    ) -> ActualCloseCollection: ...


def _require_digest(value: object, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or not set(value).issubset(_LOWER_SHA256)
    ):
        raise CanonicalMaterialError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_session(value: object) -> date:
    if type(value) is not date:
        raise CanonicalMaterialError("canonical material session must be an exact date")
    return value


def _require_time(value: object, label: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise CanonicalMaterialError(f"{label} must be timezone-aware")
    if type(value.tzinfo) not in {timezone, ZoneInfo}:
        raise CanonicalMaterialError(
            f"{label} timezone implementation is not immutable"
        )
    try:
        offset = value.utcoffset()
    except Exception as error:
        raise CanonicalMaterialError(f"{label} timezone is invalid") from error
    if offset is None:
        raise CanonicalMaterialError(f"{label} must be timezone-aware")
    return value


def _require_same_new_york_session(
    value: datetime,
    session_date: date,
    label: str,
) -> None:
    if value.astimezone(_NEW_YORK).date() != session_date:
        raise CanonicalMaterialError(
            f"{label} must fall in the material New York session"
        )


def _new_york_clock(value: datetime) -> time:
    local = value.astimezone(_NEW_YORK)
    return time(local.hour, local.minute, local.second, local.microsecond)


def _validate_premarket_times(
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
) -> None:
    _require_same_new_york_session(
        decision_at,
        session_date,
        "premarket decision time",
    )
    _require_same_new_york_session(
        retrieved_at,
        session_date,
        "premarket retrieval time",
    )
    if _new_york_clock(decision_at) != time(8, 45):
        raise CanonicalMaterialError(
            "premarket economic decision must be exactly 08:45 New York time"
        )
    if decision_at > retrieved_at:
        raise CanonicalMaterialError(
            "premarket retrieval cannot precede its economic decision"
        )


def _validate_close_times(
    session_date: date,
    review_at: datetime,
    query_cutoff: datetime,
    retrieved_at: datetime,
) -> None:
    for value, label in (
        (review_at, "close review time"),
        (query_cutoff, "close query cutoff"),
        (retrieved_at, "close retrieval time"),
    ):
        _require_same_new_york_session(value, session_date, label)
    if _new_york_clock(review_at) not in {time(12, 30), time(15, 30)}:
        raise CanonicalMaterialError(
            "close economic review must be exactly 12:30 or 15:30 New York time"
        )
    if not review_at <= query_cutoff <= retrieved_at:
        raise CanonicalMaterialError(
            "close cutoff must be between review and retrieval"
        )


def _invalid_receipt_source() -> None:
    raise CanonicalMaterialError("canonical receipt source identity is invalid")


def _exact_https_url(value: object, hostname: str):
    if (
        type(value) is not str
        or not value
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
    ):
        _invalid_receipt_source()
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        _invalid_receipt_source()
    if (
        parsed.scheme != "https"
        or parsed.netloc != hostname
        or parsed.hostname != hostname
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or urlunsplit(parsed) != value
    ):
        _invalid_receipt_source()
    return parsed


def _canonical_query_pairs(query: str) -> tuple[tuple[str, str], ...]:
    if not query or _INVALID_PERCENT_ESCAPE.search(query):
        _invalid_receipt_source()
    try:
        pairs = tuple(
            parse_qsl(
                query,
                keep_blank_values=True,
                strict_parsing=True,
            )
        )
    except ValueError:
        _invalid_receipt_source()
    if (
        not pairs
        or len({key for key, _value in pairs}) != len(pairs)
        or urlencode(pairs) != query
    ):
        _invalid_receipt_source()
    return pairs


def _alpaca_query_timestamp(value: str) -> datetime:
    if _UTC_QUERY_TIMESTAMP.fullmatch(value) is None:
        _invalid_receipt_source()
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        _invalid_receipt_source()
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        _invalid_receipt_source()
    return parsed


def _validate_alpaca_receipt_source(
    *,
    source_uri: object,
    source_type: str,
    provider: object,
    feed: object,
) -> None:
    contract = _ALPACA_RECEIPT_CONTRACTS[source_type]
    path, required_feed, required_names, fixed_values = contract
    if provider != "alpaca" or feed != required_feed:
        _invalid_receipt_source()
    parsed = _exact_https_url(source_uri, "data.alpaca.markets")
    if parsed.path != path:
        _invalid_receipt_source()
    pairs = _canonical_query_pairs(parsed.query)
    names = tuple(key for key, _value in pairs)
    if names not in {required_names, (*required_names, "page_token")}:
        _invalid_receipt_source()
    values = dict(pairs)
    if any(values.get(key) != expected for key, expected in fixed_values.items()):
        _invalid_receipt_source()
    symbols = tuple(values.get("symbols", "").split(","))
    if (
        not symbols
        or len(symbols) > 200
        or tuple(sorted(set(symbols))) != symbols
        or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
    ):
        _invalid_receipt_source()
    if "start" in values:
        start = _alpaca_query_timestamp(values["start"])
        end = _alpaca_query_timestamp(values["end"])
        if start > end:
            _invalid_receipt_source()
    page_token = values.get("page_token")
    if page_token is not None and (
        not page_token
        or len(page_token) > 1024
        or any(ord(character) < 33 or ord(character) > 126 for character in page_token)
    ):
        _invalid_receipt_source()


def _validate_sec_receipt_source(
    *,
    source_uri: object,
    source_type: str,
    provider: object,
    feed: object,
) -> None:
    if provider != _SEC_PUBLISHER or feed not in _SEC_RECEIPT_FEEDS[source_type]:
        _invalid_receipt_source()
    if source_type == "SEC_ARCHIVE":
        parsed = _exact_https_url(source_uri, "www.sec.gov")
        valid_path = _SEC_ARCHIVE_PATH.fullmatch(parsed.path)
    else:
        parsed = _exact_https_url(source_uri, "data.sec.gov")
        valid_path = _SEC_SUBMISSIONS_PATH.fullmatch(parsed.path)
    if valid_path is None or parsed.query:
        _invalid_receipt_source()


def _official_reference_details(
    value: object,
    *,
    required: bool,
) -> tuple[str, str | None, str | None, str] | None:
    if type(value) is not str:
        _invalid_receipt_source()
    try:
        details = json.loads(value)
        canonical = json.dumps(
            details,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        _invalid_receipt_source()
    if type(details) is not dict or canonical != value:
        _invalid_receipt_source()
    identity_keys = {
        "accession",
        "issuer_cik",
        "source_role",
        "symbol",
        "timestamp_source",
    }
    if not required and not identity_keys.intersection(details):
        return None
    if set(details) != _OFFICIAL_REFERENCE_DETAIL_KEYS:
        _invalid_receipt_source()
    source_role = details["source_role"]
    issuer_cik = details["issuer_cik"]
    symbol = details["symbol"]
    timestamp_source = details["timestamp_source"]
    if (
        details["accession"] is not None
        or type(details["source_observation_id"]) is not str
        or _SAFE_SOURCE_ID.fullmatch(details["source_observation_id"]) is None
        or type(source_role) is not str
        or not source_role
        or (
            issuer_cik is not None
            and (
                type(issuer_cik) is not str
                or _CIK.fullmatch(issuer_cik) is None
            )
        )
        or (
            symbol is not None
            and (
                type(symbol) is not str
                or _SYMBOL.fullmatch(symbol) is None
            )
        )
        or type(timestamp_source) is not str
        or not timestamp_source
    ):
        _invalid_receipt_source()
    return source_role, issuer_cik, symbol, timestamp_source


def _validate_canonical_receipt_source(
    *,
    source_uri: object,
    source_type: object,
    provider: object,
    feed: object,
    health_result: object,
    details_json: object,
) -> None:
    if (
        type(source_type) is not str
        or type(provider) is not str
        or not provider
        or (feed is not None and type(feed) is not str)
        or type(health_result) is not str
    ):
        _invalid_receipt_source()
    if source_type in _ALPACA_RECEIPT_CONTRACTS:
        if health_result != "OK":
            _invalid_receipt_source()
        _validate_alpaca_receipt_source(
            source_uri=source_uri,
            source_type=source_type,
            provider=provider,
            feed=feed,
        )
        return
    if source_type in _SEC_RECEIPT_FEEDS:
        if health_result not in {"OK", "UNHEALTHY"}:
            _invalid_receipt_source()
        _validate_sec_receipt_source(
            source_uri=source_uri,
            source_type=source_type,
            provider=provider,
            feed=feed,
        )
        return
    if source_type == "OFFICIAL_REFERENCE":
        if (
            type(source_uri) is not str
            or health_result not in {"OK", "UNHEALTHY"}
        ):
            _invalid_receipt_source()
        providers = (
            _OFFICIAL_REFERENCE_SOURCES.get(source_uri)
            if type(source_uri) is str
            else None
        )
        if providers is not None:
            if provider not in providers or feed not in {
                "PRIMARY_METADATA",
                "UNAVAILABLE",
            }:
                _invalid_receipt_source()
            details = _official_reference_details(
                details_json,
                required=False,
            )
            if details is not None and details != (
                _OFFICIAL_REFERENCE_ROLES[source_uri],
                None,
                None,
                feed,
            ):
                _invalid_receipt_source()
            return
        details = _official_reference_details(details_json, required=True)
        if details is None:
            _invalid_receipt_source()
        source_role, issuer_cik, symbol, timestamp_source = details
        scoped = _SCOPED_REFERENCE_ROLE.fullmatch(source_role)
        if (
            feed != "PRIMARY_METADATA"
            or timestamp_source != feed
            or scoped is None
            or scoped.group(1) != symbol
            or (source_role, issuer_cik, source_uri, provider)
            not in _SCOPED_REFERENCE_SOURCES
        ):
            _invalid_receipt_source()
        return
    if source_type == "REVIEWED_EVIDENCE_REGISTRY":
        if (
            type(source_uri) is not str
            or _REVIEWED_REGISTRY_URI.fullmatch(source_uri) is None
            or provider != "operator-reviewed"
            or feed is not None
            or health_result != "REVIEWED"
        ):
            _invalid_receipt_source()
        return
    if source_type == "REVIEWED_ARTIFACT":
        if (
            type(source_uri) is not str
            or _REVIEWED_ARTIFACT_URI.fullmatch(source_uri) is None
            or provider != "operator-reviewed"
            or feed is not None
            or health_result != "REVIEWED"
        ):
            _invalid_receipt_source()
        return
    _invalid_receipt_source()


def canonical_source_receipts(value: object) -> tuple[object, ...]:
    """Return the sole deterministic ordering for canonical source receipts."""
    from .journal import SourceObservationReceipt

    if type(value) is not tuple or not value or any(
        type(receipt) is not SourceObservationReceipt for receipt in value
    ):
        raise CanonicalMaterialError(
            "canonical material requires exact source observation receipts"
        )
    receipts = value
    for receipt in receipts:
        if type(receipt.row_id) is not int or receipt.row_id < 1:
            raise CanonicalMaterialError("canonical receipt row identity is invalid")
        _require_digest(
            receipt.observation_sha256,
            "source observation digest",
        )
        _require_digest(receipt.payload_sha256, "source payload digest")
        _require_digest(receipt.source_digest, "source receipt digest")
        _validate_canonical_receipt_source(
            source_uri=receipt.source_uri,
            source_type=receipt.source_type,
            provider=receipt.provider,
            feed=receipt.feed,
            health_result=receipt.health_result,
            details_json=receipt.details_json,
        )
        source_time = _require_time(receipt.source_time, "receipt source time")
        retrieved_at = _require_time(
            receipt.retrieved_at,
            "receipt retrieval time",
        )
        if source_time > retrieved_at:
            raise CanonicalMaterialError(
                "canonical receipt source time cannot follow retrieval"
            )
    if (
        len({id(receipt) for receipt in receipts}) != len(receipts)
        or len({receipt.row_id for receipt in receipts}) != len(receipts)
        or len({receipt.observation_sha256 for receipt in receipts}) != len(receipts)
    ):
        raise CanonicalMaterialError("canonical source receipts must be unique")
    return tuple(
        sorted(
            receipts,
            key=lambda receipt: (receipt.observation_sha256, receipt.row_id),
        )
    )


def _receipt_set(value: object) -> tuple[object, ...]:
    return canonical_source_receipts(value)


def _require_canonical_receipt_order(value: object) -> tuple[object, ...]:
    receipts = canonical_source_receipts(value)
    if any(
        current is not expected
        for current, expected in zip(value, receipts, strict=True)
    ):
        raise CanonicalMaterialError(
            "canonical source receipts must use canonical order"
        )
    return receipts


def _validate_receipt_envelope(
    receipts: tuple[object, ...],
    *,
    kind: str,
    economic_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime | None = None,
    decision_basis: tuple[tuple[int, str, str], ...] = (),
) -> None:
    basis_by_row = {
        row_id: (role, basis)
        for row_id, role, basis in decision_basis
    }
    for receipt in receipts:
        if receipt.retrieved_at > retrieved_at:
            raise CanonicalMaterialError(
                f"canonical {kind.lower()} receipt exceeds its retrieval envelope"
            )
        if kind == "PREMARKET":
            role_basis = basis_by_row.get(receipt.row_id)
            is_operational = role_basis is not None and role_basis[1] == (
                "OPERATIONAL_HEALTH_ONLY"
            )
            if is_operational and role_basis[0] not in {
                "ALPACA_LATEST_QUOTES",
                "PRIMARY_HALT_FEED",
                "TRADER_ALERT_HALT",
                "OPERATIONAL_STATUS",
            }:
                raise CanonicalMaterialError(
                    "canonical premarket operational role is invalid"
                )
            if not is_operational and receipt.source_time > economic_at:
                raise CanonicalMaterialError(
                    "canonical premarket receipt exceeds its economic cutoff"
                )
            continue
        if query_cutoff is None:
            raise CanonicalMaterialError("canonical close query cutoff is unavailable")
        if receipt.retrieved_at > query_cutoff:
            raise CanonicalMaterialError(
                "canonical close receipt exceeds its query cutoff"
            )
        terminal_health_observation = (
            receipt.source_type == "ALPACA_LATEST_QUOTES"
        )
        if (
            receipt.source_type == "OFFICIAL_REFERENCE"
            and receipt.feed == "UNAVAILABLE"
        ):
            details = _official_reference_details(
                receipt.details_json,
                required=True,
            )
            terminal_health_observation = (
                details is not None
                and details[0]
                in {
                    "PRIMARY_HALT_FEED",
                    "TRADER_ALERT_HALT",
                    "OPERATIONAL_STATUS",
                    "CROSS_CHECK_CALENDAR",
                }
                and details[1] is None
                and details[2] is None
                and details[3] == "UNAVAILABLE"
                and receipt.source_time == receipt.retrieved_at
            )
        if not terminal_health_observation and receipt.source_time > economic_at:
            raise CanonicalMaterialError(
                "canonical close receipt exceeds its economic cutoff"
            )
        if terminal_health_observation and receipt.source_time > query_cutoff:
            raise CanonicalMaterialError(
                "canonical close health receipt exceeds its retrieval envelope"
            )


def _require_premarket_decision_basis(
    value: object,
) -> tuple[tuple[int, str, str], ...]:
    if type(value) is not tuple or not value:
        raise CanonicalMaterialError("premarket decision basis is invalid")
    row_ids: set[int] = set()
    for item in value:
        if (
            type(item) is not tuple
            or len(item) != 3
            or type(item[0]) is not int
            or item[0] < 1
            or item[0] in row_ids
            or type(item[1]) is not str
            or not item[1]
            or item[2] not in {
                "ECONOMIC_INPUT",
                "OPERATIONAL_HEALTH_ONLY",
            }
        ):
            raise CanonicalMaterialError("premarket decision basis is invalid")
        row_ids.add(item[0])
    return value


def _is_issued_provider_fetch_page_bundle(value: object) -> bool:
    """Use the public Alpaca page capability when that provider slice is present."""
    from .providers import alpaca as alpaca_module

    predicate = getattr(
        alpaca_module,
        "is_issued_provider_fetch_page_bundle",
        None,
    )
    return bool(callable(predicate) and predicate(value))


def _reviewed_binding_identity(
    source: object,
    *,
    invoke_owner_predicate: bool = True,
) -> tuple[str, str, date | datetime] | None:
    from . import evidence as evidence_module
    from . import market_calendar as calendar_module
    from . import universe as universe_module

    if type(source) is calendar_module.MarketCalendar:
        if invoke_owner_predicate and not (
            calendar_module.is_release_verified_market_calendar(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed calendar authority is unavailable"
            )
        pin = calendar_module._RELEASE_MANIFEST_SHA256.get(source.year)
        if type(pin) is not str:
            raise CanonicalMaterialError("premarket reviewed calendar pin is missing")
        return "calendar", pin, source.reviewed_at
    if type(source) is universe_module.UniverseSnapshot:
        if invoke_owner_predicate and not (
            universe_module.is_verified_universe_snapshot(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed universe authority is unavailable"
            )
        pin = source._release_pin
        if type(pin) is not str:
            raise CanonicalMaterialError("premarket reviewed universe pin is missing")
        return "universe", pin, source.reviewed_at
    if type(source) is evidence_module.ReviewedEvidenceRelease:
        if invoke_owner_predicate and not (
            evidence_module.is_verified_evidence_release(source)
        ):
            raise CanonicalMaterialError(
                "premarket reviewed evidence release authority is unavailable"
            )
        return "evidence-release", source.release_sha256, source.reviewed_at
    if type(source) is evidence_module.ReviewedEvidenceBundle:
        if invoke_owner_predicate and not evidence_module._is_reviewed_bundle(
            source
        ):
            raise CanonicalMaterialError(
                "premarket reviewed evidence child authority is unavailable"
            )
        if type(source.symbol) is not str:
            raise CanonicalMaterialError(
                "premarket reviewed evidence child subject is unavailable"
            )
        return (
            f"evidence-{source.symbol.lower()}",
            source.content_hash,
            source.reviewed_at,
        )
    return None


def _document_disclosure_is_current(disclosure: object, document: object) -> bool:
    from .providers.cache import SourceDocument
    from .providers.reference import ReferenceClient
    from .providers.sec import SecClient

    if type(document) is not SourceDocument:
        return False
    if type(disclosure) is SecClient:
        return bool(
            disclosure._documents.get(document.source_observation_id) is document
        )
    if type(disclosure) is ReferenceClient:
        issued = disclosure._documents.get(document.source_observation_id)
        if issued is None or issued[0] is not document:
            return False
        try:
            from .providers import reference as reference_module

            current = reference_module._canonical_digest(
                reference_module._document_fingerprint(document)
            )
        except (TypeError, ValueError):
            return False
        return bool(current == issued[1])
    return False


def _binding_source_role(
    source: object,
    *,
    invoke_owner_predicate: bool,
) -> str:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    reviewed = _reviewed_binding_identity(
        source,
        invoke_owner_predicate=invoke_owner_predicate,
    )
    if reviewed is not None:
        return reviewed[0]
    if type(source) is ProviderFetchPageBundle:
        return source.page.source_type
    if type(source) is SourceDocument:
        return source.source_role or source.source_type
    raise CanonicalMaterialError("premarket source binding type is unsupported")


def _binding_is_operational_only(source: object) -> bool:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    if type(source) is ProviderFetchPageBundle:
        return source.page.source_type == "ALPACA_LATEST_QUOTES"
    if type(source) is SourceDocument:
        return source.source_role in {
            "PRIMARY_HALT_FEED",
            "TRADER_ALERT_HALT",
            "OPERATIONAL_STATUS",
        }
    return False


def _verify_premarket_binding_source(
    binding: PremarketSourceBinding,
    *,
    reviewed_documents: tuple[object, ...],
    invoke_provider_predicate: bool,
) -> tuple[str, object]:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    receipt = binding.receipt
    source = binding.source
    reviewed = _reviewed_binding_identity(
        source,
        invoke_owner_predicate=invoke_provider_predicate,
    )
    if reviewed is not None:
        role, expected_pin, expected_source_time = reviewed
        match = _REVIEWED_ARTIFACT_URI.fullmatch(receipt.source_uri)
        source_time_matches = (
            receipt.source_time == expected_source_time
            if type(expected_source_time) is datetime
            else receipt.source_time.astimezone(_NEW_YORK).date()
            == expected_source_time
        )
        if (
            receipt.source_type != "REVIEWED_ARTIFACT"
            or receipt.provider != "operator-reviewed"
            or receipt.feed is not None
            or receipt.health_result != "REVIEWED"
            or match is None
            or match.group("role") != role
            or match.group("digest") != expected_pin
            or receipt.payload_sha256 != expected_pin
            or hashlib.sha256(receipt.source_payload).hexdigest() != expected_pin
            or not source_time_matches
            or binding.decision_basis != "ECONOMIC_INPUT"
            or receipt.delay_seconds
            != int((receipt.retrieved_at - receipt.source_time).total_seconds())
        ):
            raise CanonicalMaterialError(
                "premarket reviewed artifact bytes or pin are inconsistent"
            )
        return role, (
            "REVIEWED_ARTIFACT",
            role,
            expected_pin,
            type(source).__qualname__,
        )

    if type(source) is ProviderFetchPageBundle:
        if (
            binding.disclosure is not None
            or (invoke_provider_predicate and not _is_issued_provider_fetch_page_bundle(source))
        ):
            raise CanonicalMaterialError(
                "premarket provider page authority is unavailable; "
                "caller-authored disclosures cannot authorize completeness"
            )
        observation = source.observation
        page = source.page
        if (
            receipt.source_payload != source.payload
            or receipt.payload_sha256 != page.payload_sha256
            or hashlib.sha256(source.payload).hexdigest() != page.payload_sha256
            or page.source_observation_id != observation.observation_id
            or page.request_url != observation.url
            or page.source_type != observation.source_type
            or receipt.source_uri != page.request_url
            or receipt.source_type != page.source_type
            or receipt.provider != "alpaca"
            or receipt.feed != observation.feed
            or receipt.source_time != observation.source_timestamp
            or receipt.retrieved_at != observation.retrieved_at
            or receipt.provider_sequence != page.page_ordinal
            or receipt.delay_seconds != observation.delay_seconds
            or receipt.health_result != "OK"
        ):
            raise CanonicalMaterialError(
                "premarket provider receipt does not bind its exact page"
            )
        return page.source_type, (
            "PROVIDER_PAGE",
            _canonical_digest_value(page),
            hashlib.sha256(source.payload).hexdigest(),
            _canonical_digest_value(observation),
        )

    if type(source) is SourceDocument:
        if not any(document is source for document in reviewed_documents) and not (
            _document_disclosure_is_current(binding.disclosure, source)
        ):
            raise CanonicalMaterialError(
                "premarket source document authority is unavailable"
            )
        expected_source_time = source.published_at or source.retrieved_at
        if (
            receipt.source_payload != source.body
            or receipt.payload_sha256 != source.content_hash
            or hashlib.sha256(source.body).hexdigest() != source.content_hash
            or receipt.source_uri != source.url
            or receipt.source_type != source.source_type
            or receipt.provider != source.publisher
            or receipt.feed != source.timestamp_source
            or receipt.source_time != expected_source_time
            or receipt.retrieved_at != source.retrieved_at
            or receipt.delay_seconds
            != int((source.retrieved_at - expected_source_time).total_seconds())
            or receipt.health_result not in {"OK", "UNHEALTHY"}
        ):
            raise CanonicalMaterialError(
                "premarket source receipt does not bind its exact document"
            )
        return source.source_role or source.source_type, (
            "SOURCE_DOCUMENT",
            _canonical_digest_value(source),
        )
    raise CanonicalMaterialError("premarket source binding type is unsupported")


def _ordered_premarket_bindings(
    value: object,
) -> tuple[PremarketSourceBinding, ...]:
    if type(value) is not tuple or not value or any(
        type(binding) is not PremarketSourceBinding for binding in value
    ):
        raise CanonicalMaterialError(
            "premarket source bindings must be a nonempty exact tuple"
        )
    bindings = value
    receipts = canonical_source_receipts(
        tuple(binding.receipt for binding in bindings)
    )
    by_receipt = {id(binding.receipt): binding for binding in bindings}
    if len(by_receipt) != len(bindings):
        raise CanonicalMaterialError("premarket source receipts must be unique")
    return tuple(by_receipt[id(receipt)] for receipt in receipts)


def _premarket_binding_manifest(
    bindings: tuple[PremarketSourceBinding, ...],
    *,
    invoke_provider_predicate: bool,
) -> tuple[tuple[object, ...], ...]:
    from . import evidence as evidence_module
    from .providers.cache import SourceDocument

    releases = tuple(
        binding.source
        for binding in bindings
        if type(binding.source) is evidence_module.ReviewedEvidenceRelease
    )
    if len(releases) > 1:
        raise CanonicalMaterialError("premarket evidence release is duplicated")
    expected_bundles: tuple[object, ...] = ()
    reviewed_documents: tuple[object, ...] = ()
    if releases:
        release = releases[0]
        expected_bundles = tuple(release.by_symbol.values())
        reviewed_documents = tuple(
            source_binding.document
            for bundle in expected_bundles
            for source_binding in bundle.source_bindings
        )
        supplied_bundles = tuple(
            binding.source
            for binding in bindings
            if type(binding.source) is evidence_module.ReviewedEvidenceBundle
        )
        if (
            len(supplied_bundles) != len(expected_bundles)
            or any(
                sum(supplied is expected for supplied in supplied_bundles) != 1
                for expected in expected_bundles
            )
        ):
            raise CanonicalMaterialError(
                "premarket evidence release children are incomplete"
            )
        supplied_documents = tuple(
            binding.source
            for binding in bindings
            if type(binding.source) is SourceDocument
            and any(
                binding.source is expected
                for expected in reviewed_documents
            )
        )
        if (
            len(supplied_documents) != len(reviewed_documents)
            or any(
                sum(supplied is expected for supplied in supplied_documents) != 1
                for expected in reviewed_documents
            )
        ):
            raise CanonicalMaterialError(
                "premarket evidence source documents are incomplete"
            )

    manifest: list[tuple[object, ...]] = []
    for binding in bindings:
        role, source_fingerprint = _verify_premarket_binding_source(
            binding,
            reviewed_documents=reviewed_documents,
            invoke_provider_predicate=invoke_provider_predicate,
        )
        manifest.append(
            (
                binding.receipt.row_id,
                binding.receipt.observation_sha256,
                role,
                binding.decision_basis,
                source_fingerprint,
            )
        )
    return tuple(manifest)


def _validate_premarket_binding_chronology(
    bindings: tuple[PremarketSourceBinding, ...],
    *,
    decision_at: datetime,
    retrieved_at: datetime,
) -> None:
    from . import evidence as evidence_module

    reviewed_documents = tuple(
        source_binding.document
        for binding in bindings
        if type(binding.source) is evidence_module.ReviewedEvidenceRelease
        for bundle in binding.source.by_symbol.values()
        for source_binding in bundle.source_bindings
    )
    for binding in bindings:
        receipt = binding.receipt
        if receipt.retrieved_at > retrieved_at or receipt.source_time > receipt.retrieved_at:
            raise CanonicalMaterialError(
                "premarket source receipt exceeds its retrieval envelope"
            )
        operational = _binding_is_operational_only(binding.source) and not any(
            binding.source is document for document in reviewed_documents
        )
        if operational != (
            binding.decision_basis == "OPERATIONAL_HEALTH_ONLY"
        ):
            raise CanonicalMaterialError(
                "premarket operational source must be health-only"
            )
        if (
            binding.decision_basis == "ECONOMIC_INPUT"
            and receipt.source_time > decision_at
        ):
            raise CanonicalMaterialError(
                "premarket economic input exceeds its economic cutoff"
            )


def _premarket_decision_basis(
    bindings: tuple[PremarketSourceBinding, ...],
) -> tuple[tuple[int, str, str], ...]:
    return tuple(
        (
            binding.receipt.row_id,
            _binding_source_role(
                binding.source,
                invoke_owner_predicate=False,
            ),
            binding.decision_basis,
        )
        for binding in bindings
    )


def _is_current_premarket_source_binding_without_callbacks(
    authority: object,
    candidate: _PremarketSourceBindingCandidate | None = None,
) -> bool:
    from . import journal as journal_module

    if type(authority) is not CanonicalPremarketSourceBindingAuthority:
        return False
    if candidate is None:
        with _PREMARKET_SOURCE_BINDING_LOCK:
            candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    journal = None if candidate is None else candidate.journal_reference()
    try:
        fingerprint = _value_fingerprint(authority)
        binding_manifest = _premarket_binding_manifest(
            candidate.bindings if candidate is not None else (),
            invoke_provider_predicate=False,
        )
    except Exception:
        return False
    return bool(
        candidate is not None
        and candidate.authority_reference() is authority
        and journal is not None
        and not getattr(journal, "_closed", True)
        and getattr(journal, "_source_generation", None)
        == candidate.journal_generation
        and candidate.authority_fingerprint == fingerprint
        and candidate.binding_manifest == binding_manifest
        and _canonical_receipt_manifest(
            tuple(binding.receipt for binding in candidate.bindings)
        )
        == authority.receipt_manifest
        and _premarket_decision_basis(candidate.bindings)
        == authority.decision_basis
        and _canonical_sha256(
            "stock-monitor/premarket-source-bindings/v1",
            binding_manifest,
        )
        == authority.binding_digest
        and all(
            journal_module._is_current_journal_authority_candidate_without_callbacks(
                receipt_candidate
            )
            for receipt_candidate in candidate.receipt_candidates
        )
    )


def issue_canonical_premarket_source_binding_authority(
    *,
    journal: object,
    decision_at: datetime,
    retrieved_at: datetime,
    bindings: tuple[PremarketSourceBinding, ...],
) -> CanonicalPremarketSourceBindingAuthority:
    """Bind exact source objects to exact current receipts from one Journal."""
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError(
            "premarket source binding requires an open Journal owner"
        )
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(
        decision_at.astimezone(_NEW_YORK).date(),
        decision_at,
        retrieved_at,
    )
    ordered = _ordered_premarket_bindings(bindings)
    _validate_premarket_binding_chronology(
        ordered,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
    )
    # Provider/document verification may consult mutable owners.  Exhaust it
    # before capturing Journal candidates for the callback-free final seal.
    _premarket_binding_manifest(ordered, invoke_provider_predicate=True)
    receipt_candidates = _current_receipt_candidates(
        journal,
        tuple(binding.receipt for binding in ordered),
    )
    if receipt_candidates is None:
        raise CanonicalMaterialError(
            "premarket source receipts lack one current Journal owner"
        )
    binding_manifest = _premarket_binding_manifest(
        ordered,
        invoke_provider_predicate=False,
    )
    decision_basis = _premarket_decision_basis(ordered)
    authority = CanonicalPremarketSourceBindingAuthority(
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        receipt_manifest=_canonical_receipt_manifest(
            tuple(binding.receipt for binding in ordered)
        ),
        decision_basis=decision_basis,
        binding_digest=_canonical_sha256(
            "stock-monitor/premarket-source-bindings/v1",
            binding_manifest,
        ),
    )
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("premarket Journal generation is invalid")
    identity = id(authority)

    def discard(dead: ReferenceType[object]) -> None:
        with _PREMARKET_SOURCE_BINDING_LOCK:
            current = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(identity)
            if current is not None and current.authority_reference is dead:
                _ISSUED_PREMARKET_SOURCE_BINDINGS.pop(identity, None)

    candidate = _PremarketSourceBindingCandidate(
        authority_reference=ref(authority, discard),
        authority_fingerprint=_value_fingerprint(authority),
        journal_reference=ref(journal),
        journal_generation=generation,
        receipt_candidates=receipt_candidates,
        bindings=ordered,
        binding_manifest=binding_manifest,
    )
    with _PREMARKET_SOURCE_BINDING_LOCK:
        _ISSUED_PREMARKET_SOURCE_BINDINGS[identity] = candidate
    if not _is_current_premarket_source_binding_without_callbacks(
        authority,
        candidate,
    ):
        with _PREMARKET_SOURCE_BINDING_LOCK:
            if _ISSUED_PREMARKET_SOURCE_BINDINGS.get(identity) is candidate:
                _ISSUED_PREMARKET_SOURCE_BINDINGS.pop(identity, None)
        raise CanonicalMaterialError(
            "premarket source binding changed during issuance"
        )
    return authority


def is_issued_canonical_premarket_source_binding_authority(
    authority: object,
    *,
    journal: object,
) -> bool:
    """Return whether an exact source binding remains current for one owner."""
    with _PREMARKET_SOURCE_BINDING_LOCK:
        candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    if candidate is None or candidate.journal_reference() is not journal:
        return False
    try:
        _premarket_binding_manifest(
            candidate.bindings,
            invoke_provider_predicate=True,
        )
    except Exception:
        return False
    with _PREMARKET_SOURCE_BINDING_LOCK:
        refreshed = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    return bool(
        refreshed is candidate
        and _is_current_premarket_source_binding_without_callbacks(
            authority,
            candidate,
        )
    )


def _premarket_reviewed_bindings(
    authority: object,
) -> tuple[PremarketSourceBinding, ...]:
    with _PREMARKET_SOURCE_BINDING_LOCK:
        candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(id(authority))
    if not _is_current_premarket_source_binding_without_callbacks(
        authority,
        candidate,
    ):
        raise CanonicalMaterialError(
            "premarket source binding authority is unavailable"
        )
    assert candidate is not None
    return tuple(
        binding
        for binding in candidate.bindings
        if binding.receipt.health_result == "REVIEWED"
    )


def _empty_premarket_snapshot(breaker_active: bool) -> object:
    from .workflows import PremarketSnapshot

    return PremarketSnapshot((), breaker_active)


def _bound_premarket_external_ids(
    bindings: tuple[PremarketSourceBinding, ...],
) -> tuple[str, ...]:
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    identifiers: list[str] = []
    for binding in bindings:
        if type(binding.source) is ProviderFetchPageBundle:
            identifiers.append(binding.source.page.source_observation_id)
        elif type(binding.source) is SourceDocument:
            identifiers.append(binding.source.source_observation_id)
    if len(identifiers) != len(set(identifiers)):
        raise CanonicalMaterialError(
            "premarket bound external source identity is duplicated"
        )
    return tuple(identifiers)


def _rank_premarket_contexts(
    contexts: tuple[object, ...],
    *,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    calendar: object,
    universe: object,
    evidence_release: object,
    bindings: tuple[PremarketSourceBinding, ...],
) -> tuple[object, ...] | None:
    """Validate one complete release cohort and issue its ranked candidates."""
    from . import evidence as evidence_module
    from . import screening as screening_module
    from .providers import reference as reference_module

    expected_records = tuple(record for record in universe.records if record.enabled)
    if (
        type(contexts) is not tuple
        or len(contexts) != len(expected_records)
        or any(type(context) is not screening_module.CandidateContext for context in contexts)
    ):
        raise CanonicalMaterialError(
            "premarket candidate context cohort is incomplete"
        )
    by_symbol = {context.record.symbol: context for context in contexts}
    if tuple(sorted(by_symbol)) != tuple(sorted(record.symbol for record in expected_records)):
        raise CanonicalMaterialError(
            "premarket candidate context symbols are incomplete"
        )
    bound_ids = _bound_premarket_external_ids(bindings)
    for record in expected_records:
        context = by_symbol[record.symbol]
        bundle = evidence_release.by_symbol.get(record.symbol)
        decision = context.evidence
        status = context.instrument_status
        if (
            context.record is not record
            or context.session_date != session_date
            or context.as_of != decision_at
            or context.operational_as_of != retrieved_at
            or context.market_calendar is not calendar
            or bundle is None
            or not evidence_module.is_reviewed_evidence_decision(decision)
            or decision._reviewed_bundle is not bundle
            or not reference_module.is_reviewed_instrument_status_decision(status)
            or status.symbol != record.symbol
            or status.as_of > retrieved_at
            or any(bound_ids.count(source_id) != 1 for source_id in decision.source_observation_ids)
            or any(bound_ids.count(source_id) != 1 for source_id in status.source_observation_ids)
        ):
            raise CanonicalMaterialError(
                "premarket candidate context authority is inconsistent"
            )
        try:
            facts = screening_module._candidate_market_facts(context)
        except Exception as error:
            raise CanonicalMaterialError(
                "premarket candidate market facts are incomplete"
            ) from error
        for fact in facts:
            source_id = getattr(fact, "source_observation_id", None)
            if (
                bound_ids.count(source_id) != 1
                or not screening_module.is_issued_normalized_market_fact(fact)
            ):
                raise CanonicalMaterialError(
                    "premarket candidate market-fact authority is unavailable"
                )
        if any(
            bar.timestamp > decision_at
            for values in context.bars_by_symbol.values()
            for bar in values
        ) or (
            context.previous_session_quote is not None
            and context.previous_session_quote.timestamp > decision_at
        ):
            raise CanonicalMaterialError(
                "premarket economic market fact exceeds the decision cutoff"
            )
        if (
            context.latest_iex_quote is not None
            and context.latest_iex_quote.timestamp > retrieved_at
        ):
            raise CanonicalMaterialError(
                "premarket operational quote exceeds retrieval"
            )

    cohort = screening_module.build_base_eligible_cohort(
        contexts,
        universe=universe,
    )
    return _rank_premarket_cohort(cohort)


def _rank_premarket_cohort(cohort: object) -> tuple[object, ...] | None:
    from . import screening as screening_module

    if type(cohort) is not screening_module.CohortDecision:
        raise CanonicalMaterialError("premarket cohort decision is malformed")
    if cohort.status == "DATA_UNAVAILABLE":
        return None
    if cohort.status == "NO_TRADE":
        return ()
    if cohort.status != "READY":
        raise CanonicalMaterialError("premarket cohort decision is unsupported")
    candidates = tuple(
        screening_module.to_scored_candidate(context)
        for context in cohort.contexts
        if screening_module.score_candidate(context).publishable
        and screening_module.detect_setup(context).eligible
    )
    return screening_module.rank_candidates(candidates)


def _validate_premarket_risk_resolution(
    resolution: object,
    *,
    session_date: date,
    decision_at: datetime,
    ranked_candidates: tuple[object, ...],
    validation: _PremarketValidationContext,
) -> bool:
    from . import risk as risk_module
    from . import screening as screening_module

    if type(resolution) is not PremarketRiskResolution:
        raise CanonicalMaterialError(
            "premarket risk resolver returned an invalid result"
        )
    breaker = resolution.breaker_state
    if (
        type(breaker) is not risk_module.BreakerState
        or not risk_module.is_issued_breaker_state(breaker)
    ):
        raise CanonicalMaterialError(
            "premarket breaker authority is unavailable"
        )
    breaker_sources = tuple(
        source
        for source, kind in risk_module._phase1_bound_sources(breaker)
        if kind == "BREAKER_HISTORY"
    )
    if (
        len(breaker_sources) != 1
        or breaker_sources[0].validation_window_id
        != validation.validation_window_id
        or breaker_sources[0].source_digest
        != validation.history_source.source_digest
    ):
        raise CanonicalMaterialError(
            "premarket breaker and validation histories conflict"
        )
    breaker_active = risk_module.breaker_pauses_entry(
        breaker,
        session_date,
    )
    decision = resolution.publication_decision
    plan = resolution.primary_plan
    capacity = resolution.capacity_decision
    if breaker_active or not ranked_candidates:
        if decision is not None or plan is not None or capacity is not None:
            raise CanonicalMaterialError(
                "premarket blocked cohort cannot carry a sized plan"
            )
        return breaker_active
    if capacity is not None:
        request = risk_module.LongPlanRequest.from_scored_candidate(
            ranked_candidates[0]
        )
        portfolio = getattr(capacity, "portfolio_authority", None)
        if (
            decision is not None
            or plan is not None
            or type(capacity) is not risk_module.LongPlanDecision
            or not risk_module.is_issued_long_plan_decision(capacity)
            or capacity.eligible
            or capacity.plan is not None
            or capacity.target is not None
            or capacity.request != request
            or capacity.authority_scope != "CANONICAL_PUBLICATION"
            or capacity.as_of != decision_at
            or not capacity.reason_codes
            or not set(capacity.reason_codes).issubset(
                _PRIMARY_CAPACITY_REASONS
            )
            or not risk_module.is_issued_portfolio_risk_authority(portfolio)
            or portfolio is not capacity.portfolio_authority
            or portfolio.request is not capacity.request
            or len(portfolio.portfolio_state.breaker_states) != 1
            or portfolio.portfolio_state.breaker_states[0] is not breaker
        ):
            raise CanonicalMaterialError(
                "premarket capacity decision authority is inconsistent"
            )
        return False
    if (
        not risk_module.is_issued_long_plan_decision(plan)
        or not screening_module.is_issued_publication_decision_for_plan(
            decision,
            plan,
        )
        or tuple(item.candidate for item in decision.candidates)
        != ranked_candidates[:3]
    ):
        raise CanonicalMaterialError(
            "premarket candidate risk authority is inconsistent"
        )
    return False


def _premarket_candidate_snapshot(
    resolution: PremarketRiskResolution,
) -> object:
    """Project one sized primary and at most two unsized ranked shadows."""
    from .reports import (
        PremarketCandidate,
        PremarketShadow,
        ReportSource,
        ScoreComponent,
    )
    from . import screening as screening_module
    from .workflows import CandidateSummary, PremarketSnapshot

    decision = resolution.publication_decision
    plan = resolution.primary_plan
    if decision is None or plan is None or plan.plan is None:
        raise CanonicalMaterialError(
            "premarket candidate projection lacks a primary plan"
        )
    summaries: list[CandidateSummary] = []
    for publication in decision.candidates:
        candidate = publication.candidate
        setup = candidate.setup
        score = candidate.score_card
        if setup is None or setup.setup_type is None or score is None:
            raise CanonicalMaterialError(
                "premarket candidate projection is incomplete"
            )
        if publication.role == "PRIMARY":
            issued = screening_module._issued_scored_candidate_authority(
                candidate
            )
            if issued is None:
                raise CanonicalMaterialError(
                    "premarket primary candidate authority is unavailable"
                )
            context = issued.context
            evidence = context.evidence
            reviewed_bundle = evidence._reviewed_bundle
            links = tuple(
                dict.fromkeys(
                    record.primary_url
                    for record in evidence.qualifying_records
                )
            )
            if not links:
                links = tuple(
                    dict.fromkeys(
                        binding.primary_url
                        for binding in reviewed_bundle.source_bindings
                    )
                )
            sources = tuple(
                ReportSource(label="Reviewed evidence", url=url)
                for url in links
            )
            material = PremarketCandidate(
                symbol=candidate.symbol,
                role="PRIMARY",
                setup=setup.setup_type,
                score_components=(
                    ScoreComponent(
                        "Trend and market regime",
                        score.trend_and_regime,
                        25,
                    ),
                    ScoreComponent(
                        "Relative strength",
                        score.relative_strength,
                        20,
                    ),
                    ScoreComponent(
                        "Setup quality",
                        score.setup_quality,
                        20,
                    ),
                    ScoreComponent(
                        "Volume confirmation",
                        score.volume_confirmation,
                        15,
                    ),
                    ScoreComponent(
                        "Verified catalyst/context",
                        score.catalyst_context,
                        10,
                    ),
                    ScoreComponent(
                        "Liquidity and execution",
                        score.liquidity_execution,
                        10,
                    ),
                ),
                trigger=candidate.trigger_price,
                maximum_entry=candidate.maximum_permitted_entry,
                recommended_stop=candidate.recommended_stop,
                target=candidate.target_price,
                shares=plan.plan.quantity,
                planned_risk=plan.plan.planned_risk,
                provider="ALPACA",
                feed="SIP",
                observed_at=context.previous_session_quote.timestamp,
                invalidations=(
                    "Do not enter above the maximum permitted entry.",
                    "Cancel if reviewed evidence or market-status checks block entry.",
                ),
                sources=sources,
            )
        elif publication.role == "WATCHLIST_SHADOW":
            material = PremarketShadow(
                symbol=candidate.symbol,
                role="WATCHLIST_SHADOW",
                score=Decimal(candidate.total_score),
                setup=setup.setup_type,
                trigger=candidate.trigger_price,
            )
        else:
            raise CanonicalMaterialError(
                "premarket publication role is unsupported"
            )
        summaries.append(
            CandidateSummary(
                symbol=candidate.symbol,
                role=publication.role,
                material=material,
            )
        )
    return PremarketSnapshot(tuple(summaries), False)


def _evidence_phase1_children(evidence_release: object) -> tuple[object, ...]:
    return tuple(
        bundle._phase1_source
        for bundle in evidence_release.by_symbol.values()
        if bundle._phase1_source is not None
    )


def _premarket_phase1_children(
    evidence_release: object,
    resolution: PremarketRiskResolution,
    validation: _PremarketValidationContext,
) -> tuple[object, ...]:
    from . import risk as risk_module

    values = [
        *_evidence_phase1_children(evidence_release),
        validation.history_source,
    ]
    if resolution.capacity_decision is not None:
        values.extend(
            source
            for source, _kind in risk_module._phase1_bound_sources(
                resolution.capacity_decision.portfolio_authority
            )
        )
    values.extend(
        source
        for source, _kind in risk_module._phase1_bound_sources(
            resolution.breaker_state
        )
    )
    if resolution.primary_plan is not None:
        values.extend(
            source
            for source, _kind in risk_module._phase1_bound_sources(
                resolution.primary_plan.portfolio_authority
            )
        )
    unique: list[object] = []
    for value in values:
        if not any(value is present for present in unique):
            unique.append(value)
    return tuple(unique)


def _load_coordinator_evidence_release(
    *,
    project_root: Path,
    decision_at: datetime,
    universe: object,
) -> tuple[object, str | None]:
    """Load current evidence or bind the exact stale release for a safe branch."""
    from .evidence import (
        EvidenceRegistryError,
        load_current_evidence_release,
    )

    try:
        return (
            load_current_evidence_release(
                project_root,
                as_of=decision_at,
                universe=universe,
            ),
            None,
        )
    except EvidenceRegistryError as current_error:
        try:
            payload = (
                project_root / "data" / "evidence" / "current.json"
            ).read_bytes()
            document = json.loads(payload)
            reviewed_text = document["reviewed_at"]
            if type(reviewed_text) is not str or not reviewed_text.endswith("Z"):
                raise ValueError
            reviewed_at = datetime.fromisoformat(
                reviewed_text[:-1] + "+00:00"
            )
            release = load_current_evidence_release(
                project_root,
                as_of=reviewed_at,
                universe=universe,
            )
        except (OSError, KeyError, TypeError, ValueError, EvidenceRegistryError):
            raise current_error
        if not (
            release.reviewed_at <= decision_at < release.review_by
        ):
            return release, "SOURCE_CHECK_FAILED"
        raise current_error


def _reviewed_artifact_binding(
    *,
    journal: object,
    role: str,
    payload: bytes,
    source_time: datetime,
    retrieved_at: datetime,
    source: object,
) -> PremarketSourceBinding:
    digest = hashlib.sha256(payload).hexdigest()
    receipt = journal.append_source_observation_receipt(
        payload=payload,
        source_uri=f"stock-monitor://reviewed/{role}/{digest}",
        source_type="REVIEWED_ARTIFACT",
        provider="operator-reviewed",
        feed=None,
        source_time=source_time,
        retrieved_at=retrieved_at,
        provider_sequence=None,
        delay_seconds=int((retrieved_at - source_time).total_seconds()),
        health_result="REVIEWED",
        details={"role": role},
    )
    return PremarketSourceBinding(
        receipt=receipt,
        source=source,
        decision_basis="ECONOMIC_INPUT",
    )


def _evidence_document_details(
    document: object,
    bundle: object,
) -> dict[str, object]:
    source_role = document.source_role
    scoped = (
        None
        if source_role is None
        else _SCOPED_REFERENCE_ROLE.fullmatch(source_role)
    )
    return {
        "accession": document.accession,
        "issuer_cik": bundle.issuer_cik if scoped is not None else None,
        "source_observation_id": document.source_observation_id,
        "source_role": source_role,
        "symbol": bundle.symbol if scoped is not None else None,
        "timestamp_source": document.timestamp_source,
    }


def _reviewed_document_binding(
    *,
    journal: object,
    document: object,
    bundle: object,
) -> PremarketSourceBinding:
    source_time = document.published_at or document.retrieved_at
    receipt = journal.append_source_observation_receipt(
        payload=document.body,
        source_uri=document.url,
        source_type=document.source_type,
        provider=document.publisher,
        feed=document.timestamp_source,
        source_time=source_time,
        retrieved_at=document.retrieved_at,
        provider_sequence=None,
        delay_seconds=int((document.retrieved_at - source_time).total_seconds()),
        health_result="OK",
        details=_evidence_document_details(document, bundle),
    )
    return PremarketSourceBinding(
        receipt=receipt,
        source=document,
        # These exact bytes are frozen into the reviewed release before the
        # economic cutoff.  Their original source role does not turn the
        # reviewed decision into a late operational input.
        decision_basis="ECONOMIC_INPUT",
    )


def _persist_premarket_reviewed_bindings(
    *,
    journal: object,
    project_root: Path,
    calendar: object,
    universe: object,
    evidence_release: object,
    retrieved_at: datetime,
) -> tuple[PremarketSourceBinding, ...]:
    """Persist exact local release bytes and reread every resulting receipt."""
    reviewed: list[PremarketSourceBinding] = []
    reviewed.append(
        _reviewed_artifact_binding(
            journal=journal,
            role="calendar",
            payload=(
                project_root / "data" / "calendars" / f"{calendar.year:04d}.json"
            ).read_bytes(),
            source_time=datetime.combine(
                calendar.reviewed_at,
                time.min,
                _NEW_YORK,
            ),
            retrieved_at=retrieved_at,
            source=calendar,
        )
    )
    reviewed.append(
        _reviewed_artifact_binding(
            journal=journal,
            role="universe",
            payload=(
                project_root
                / "data"
                / "universe"
                / f"{universe.effective_date.isoformat()}.json"
            ).read_bytes(),
            source_time=datetime.combine(
                universe.reviewed_at,
                time.min,
                _NEW_YORK,
            ),
            retrieved_at=retrieved_at,
            source=universe,
        )
    )
    reviewed.append(
        _reviewed_artifact_binding(
            journal=journal,
            role="evidence-release",
            payload=(project_root / "data" / "evidence" / "current.json").read_bytes(),
            source_time=evidence_release.reviewed_at,
            retrieved_at=retrieved_at,
            source=evidence_release,
        )
    )
    for symbol, bundle in evidence_release.by_symbol.items():
        reviewed.append(
            _reviewed_artifact_binding(
                journal=journal,
                role=f"evidence-{symbol.lower()}",
                payload=(
                    project_root
                    / "data"
                    / "evidence"
                    / "subjects"
                    / f"{symbol}.json"
                ).read_bytes(),
                source_time=bundle.reviewed_at,
                retrieved_at=retrieved_at,
                source=bundle,
            )
        )
        reviewed.extend(
            _reviewed_document_binding(
                journal=journal,
                document=source_binding.document,
                bundle=bundle,
            )
            for source_binding in bundle.source_bindings
        )
    return _reread_premarket_bindings(journal, tuple(reviewed))


def _reread_premarket_bindings(
    journal: object,
    bindings: tuple[PremarketSourceBinding, ...],
) -> tuple[PremarketSourceBinding, ...]:
    reread = journal.read_source_observation_receipts(
        tuple(binding.receipt.row_id for binding in bindings)
    )
    return _ordered_premarket_bindings(
        tuple(
            replace(binding, receipt=receipt)
            for binding, receipt in zip(bindings, reread, strict=True)
        )
    )


def _terminal_premarket_provider_pages(
    cohorts: tuple[object, ...],
    required_symbols: tuple[str, ...],
) -> tuple[object, ...]:
    """Return every page from one exact complete three-role provider owner."""
    from .providers import alpaca as alpaca_module

    if (
        type(cohorts) is not tuple
        or len(cohorts) != 3
        or any(
            type(cohort) is not alpaca_module.ProviderFetchCohort
            or not alpaca_module.is_issued_provider_fetch_cohort(cohort)
            for cohort in cohorts
        )
        or not alpaca_module.provider_fetch_cohorts_share_owner(*cohorts)
    ):
        raise CanonicalMaterialError(
            "premarket provider cohorts lack one exact current owner"
        )
    expected_symbols = tuple(sorted(set(required_symbols)))
    if expected_symbols != required_symbols:
        raise CanonicalMaterialError(
            "premarket required-symbol cohort is not canonical"
        )
    bundles = tuple(
        alpaca_module.read_provider_fetch_bundle(cohort) for cohort in cohorts
    )
    by_source_type: dict[str, object] = {}
    for bundle in bundles:
        if type(bundle) is not alpaca_module.ProviderFetchBundle:
            raise CanonicalMaterialError(
                "premarket provider disclosure is malformed"
            )
        manifest = bundle.manifest
        pages = bundle.pages
        if (
            type(manifest) is not alpaca_module.ProviderFetchManifest
            or manifest.terminal is not True
            or manifest.requested_symbols != expected_symbols
            or type(pages) is not tuple
            or len(pages) != len(manifest.pages)
            or any(
                type(page_bundle) is not alpaca_module.ProviderFetchPageBundle
                or page_bundle.page is not manifest_page
                or not _is_issued_provider_fetch_page_bundle(page_bundle)
                for page_bundle, manifest_page in zip(
                    pages,
                    manifest.pages,
                    strict=True,
                )
            )
        ):
            raise CanonicalMaterialError(
                "premarket provider disclosure is incomplete or unissued"
            )
        source_types = {page.page.source_type for page in pages}
        if len(source_types) != 1:
            raise CanonicalMaterialError(
                "premarket provider disclosure mixes source roles"
            )
        source_type = next(iter(source_types))
        if source_type in by_source_type:
            raise CanonicalMaterialError(
                "premarket provider cohort role is duplicated"
            )
        by_source_type[source_type] = bundle
    if set(by_source_type) != {
        "ALPACA_DAILY_BARS",
        "ALPACA_HISTORICAL_QUOTES",
        "ALPACA_LATEST_QUOTES",
    }:
        raise CanonicalMaterialError(
            "premarket provider cohort roles are incomplete"
        )
    return tuple(
        page
        for source_type in (
            "ALPACA_DAILY_BARS",
            "ALPACA_HISTORICAL_QUOTES",
            "ALPACA_LATEST_QUOTES",
        )
        for page in by_source_type[source_type].pages
    )


def _persist_premarket_provider_bindings(
    *,
    journal: object,
    cohorts: tuple[object, ...],
    required_symbols: tuple[str, ...],
) -> tuple[PremarketSourceBinding, ...]:
    """Test/support sink that persists every exact terminal provider page."""
    page_bundles = _terminal_premarket_provider_pages(
        cohorts,
        required_symbols,
    )

    bindings: list[PremarketSourceBinding] = []
    for page_bundle in page_bundles:
        page = page_bundle.page
        observation = page_bundle.observation
        receipt = journal.append_source_observation_receipt(
            payload=page_bundle.payload,
            source_uri=page.request_url,
            source_type=page.source_type,
            provider="alpaca",
            feed=observation.feed,
            source_time=observation.source_timestamp,
            retrieved_at=observation.retrieved_at,
            provider_sequence=page.page_ordinal,
            delay_seconds=observation.delay_seconds,
            health_result="OK",
            details={
                "source_observation_id": page.source_observation_id,
            },
        )
        bindings.append(
            PremarketSourceBinding(
                receipt=receipt,
                source=page_bundle,
                decision_basis=(
                    "OPERATIONAL_HEALTH_ONLY"
                    if page.source_type == "ALPACA_LATEST_QUOTES"
                    else "ECONOMIC_INPUT"
                ),
            )
        )
    return tuple(bindings)


def _require_complete_premarket_provider_handoff(
    collection: PremarketProviderCollection,
    required_symbols: tuple[str, ...],
) -> None:
    """Bind terminal disclosures to exactly the pages durably handed off."""
    pages = _terminal_premarket_provider_pages(
        collection.provider_cohorts,
        required_symbols,
    )
    handed_pages = tuple(
        binding.source
        for binding in collection.persisted_bindings
        if _binding_source_role(
            binding.source,
            invoke_owner_predicate=False,
        ).startswith("ALPACA_")
    )
    if (
        len(handed_pages) != len(pages)
        or any(
            sum(handed is page for handed in handed_pages) != 1
            for page in pages
        )
    ):
        raise CanonicalMaterialError(
            "premarket persisted provider pages are incomplete"
        )


def _require_complete_premarket_reference_handoff(
    collection: PremarketProviderCollection,
) -> None:
    """Require the three operational documents from one exact client owner."""
    from .providers.cache import SourceDocument
    from .providers.reference import ReferenceClient

    bindings = tuple(
        binding
        for binding in collection.persisted_bindings
        if type(binding.source) is SourceDocument
        and binding.source.source_role
        in {"PRIMARY_HALT_FEED", "TRADER_ALERT_HALT", "OPERATIONAL_STATUS"}
    )
    documents = tuple(binding.source for binding in bindings)
    owners = tuple(binding.disclosure for binding in bindings)
    expected_roles = {
        "PRIMARY_HALT_FEED",
        "TRADER_ALERT_HALT",
        "OPERATIONAL_STATUS",
    }
    if (
        len(bindings) != 3
        or set(document.source_role for document in documents) != expected_roles
        or len({id(owner) for owner in owners}) != 1
        or type(owners[0]) is not ReferenceClient
        or any(
            binding.decision_basis != "OPERATIONAL_HEALTH_ONLY"
            or not _document_disclosure_is_current(
                binding.disclosure,
                binding.source,
            )
            for binding in bindings
        )
        or len(collection.reference_sources) != len(documents)
        or any(
            sum(supplied is document for supplied in collection.reference_sources)
            != 1
            for document in documents
        )
    ):
        raise CanonicalMaterialError(
            "premarket operational reference handoff is incomplete"
        )


def _persist_premarket_reference_bindings(
    *,
    journal: object,
    owner: object,
    documents: tuple[object, ...],
) -> tuple[PremarketSourceBinding, ...]:
    """Test/concrete sink helper for already-fetched operational documents."""
    from .providers.cache import SourceDocument
    from .providers.reference import ReferenceClient

    if (
        type(owner) is not ReferenceClient
        or type(documents) is not tuple
        or any(
            type(document) is not SourceDocument
            or not _document_disclosure_is_current(owner, document)
            for document in documents
        )
    ):
        raise CanonicalMaterialError(
            "premarket operational reference owner is unavailable"
        )
    bindings: list[PremarketSourceBinding] = []
    for document in documents:
        source_time = document.published_at or document.retrieved_at
        receipt = journal.append_source_observation_receipt(
            payload=document.body,
            source_uri=document.url,
            source_type=document.source_type,
            provider=document.publisher,
            feed=document.timestamp_source,
            source_time=source_time,
            retrieved_at=document.retrieved_at,
            provider_sequence=None,
            delay_seconds=int(
                (document.retrieved_at - source_time).total_seconds()
            ),
            health_result="OK",
            details={},
        )
        bindings.append(
            PremarketSourceBinding(
                receipt=receipt,
                source=document,
                disclosure=owner,
                decision_basis="OPERATIONAL_HEALTH_ONLY",
            )
        )
    return tuple(bindings)


def _issue_coordinator_premarket_branch(
    *,
    journal: object,
    report_archive_root: Path,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    validation: _PremarketValidationContext,
    bindings: tuple[PremarketSourceBinding, ...],
    calendar: object,
    universe: object,
    evidence_release: object,
    snapshot: object,
    publication_decision: object | None,
    primary_plan: object | None,
    capacity_decision: object | None = None,
    capacity_candidate: object | None = None,
    outcome: str,
    reason_codes: tuple[str, ...],
    phase1_replay_children: tuple[object, ...],
    breaker_state: object | None = None,
) -> object:
    from .reports import PremarketState, render_premarket_report

    phase1_children: list[object] = []
    for child in phase1_replay_children:
        if not any(child is current for current in phase1_children):
            phase1_children.append(child)
    if not any(
        validation.history_source is current for current in phase1_children
    ):
        phase1_children.append(validation.history_source)
    source_authority = issue_canonical_premarket_source_binding_authority(
        journal=journal,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        bindings=bindings,
    )
    receipts = tuple(binding.receipt for binding in bindings)
    composition = issue_canonical_premarket_composition_authority(
        journal=journal,
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=validation.validation_window_id,
        source_binding_authority=source_authority,
        snapshot=snapshot,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        capacity_decision=capacity_decision,
        capacity_candidate=capacity_candidate,
        outcome=outcome,
        reason_codes=reason_codes,
        calendar=calendar,
        universe=universe,
        evidence_release=evidence_release,
        phase1_replay_children=tuple(phase1_children),
        breaker_state=breaker_state,
        validation_breaker_state=validation.breaker_state,
    )
    state_hash = canonical_premarket_state_hash(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation.validation_window_id,
        outcome=outcome,
        reason_codes=reason_codes,
        composition_authority=composition,
    )
    report = render_premarket_report(
        PremarketState(
            session_date=session_date,
            generated_at=retrieved_at,
            outcome=outcome,
            reason_codes=reason_codes,
            observation_ids=tuple(
                receipt.observation_sha256 for receipt in receipts
            ),
            state_hash=state_hash,
            candidates=tuple(
                candidate.material for candidate in snapshot.candidates
            ),
        )
    )
    return issue_canonical_premarket_material(
        journal=journal,
        report_archive_root=report_archive_root,
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        report=report,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation.validation_window_id,
        composition_authority=composition,
    )


def _validate_report(
    report: object,
    *,
    kind: str,
    session_date: date,
    state_hash: str,
    receipts: tuple[object, ...],
) -> None:
    from .reports import Report, is_issued_report

    if type(report) is not Report or not is_issued_report(report):
        raise CanonicalMaterialError(
            "canonical material requires an exact renderer-issued report"
        )
    if (
        report.kind != kind
        or report.session_date != session_date
        or report.state_hash != state_hash
    ):
        raise CanonicalMaterialError(
            "canonical report identity conflicts with its material"
        )
    observation_ids = tuple(receipt.observation_sha256 for receipt in receipts)
    if report.observation_ids != observation_ids:
        raise CanonicalMaterialError(
            "canonical report evidence conflicts with its source receipts"
        )


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _canonical_digest_value(value: object) -> object:
    """Return a deterministic JSON value for the bounded material DTO graph."""
    value_type = type(value)
    if value is None or value_type in {bool, int, str}:
        return value
    if value_type is bytes:
        return {
            "type": "bytes-sha256",
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if value_type is Decimal:
        decimal = value.as_tuple()
        return {
            "type": "decimal",
            "sign": decimal.sign,
            "digits": "".join(str(digit) for digit in decimal.digits),
            "exponent": str(decimal.exponent),
        }
    if value_type is datetime:
        return {"type": "datetime", "value": _canonical_timestamp(value)}
    if value_type is date:
        return {"type": "date", "value": value.isoformat()}
    if value_type is time:
        return {
            "type": "time",
            "value": value.isoformat(timespec="microseconds"),
            "fold": value.fold,
        }
    if value_type is tuple:
        return {
            "type": "tuple",
            "items": [_canonical_digest_value(item) for item in value],
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "type": f"{value_type.__module__}.{value_type.__qualname__}",
            "fields": {
                field.name: _canonical_digest_value(
                    object.__getattribute__(value, field.name)
                )
                for field in fields(value_type)
            },
        }
    raise CanonicalMaterialError(
        "canonical digest material contains an unsupported value"
    )


def _canonical_sha256(domain: str, payload: object) -> str:
    document = {
        "domain": domain,
        "payload": payload,
    }
    try:
        encoded = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CanonicalMaterialError("canonical digest material is invalid") from error
    return hashlib.sha256(encoded).hexdigest()


def canonical_source_digest(source_receipts: tuple[object, ...]) -> str:
    """Derive one digest from the canonical persisted-receipt ordering."""
    receipts = _receipt_set(source_receipts)
    manifest = tuple(
        (
            {
                "row_id": receipt.row_id,
                "observation_sha256": _require_digest(
                    receipt.observation_sha256,
                    "source observation digest",
                ),
                "payload_sha256": _require_digest(
                    receipt.payload_sha256,
                    "source payload digest",
                ),
                "source_digest": _require_digest(
                    receipt.source_digest,
                    "source receipt digest",
                ),
            }
            for receipt in receipts
        ),
    )
    return _canonical_sha256(
        "stock-monitor/canonical-source-material/v1",
        manifest,
    )


def _publication_decision_digest(decision: object | None) -> str | None:
    if decision is None:
        return None
    from . import screening as screening_module

    if type(decision) is not screening_module.PublicationDecision:
        raise CanonicalMaterialError("canonical publication decision type is invalid")
    try:
        return screening_module._publication_decision_fingerprint(decision)
    except Exception as error:
        raise CanonicalMaterialError(
            "canonical publication decision digest is unavailable"
        ) from error


def _long_plan_digest(plan: object | None) -> str | None:
    if plan is None:
        return None
    from . import risk as risk_module

    if type(plan) is not risk_module.LongPlanDecision:
        raise CanonicalMaterialError("canonical primary plan type is invalid")
    try:
        fingerprint = risk_module._long_plan_fingerprint(plan)
        payload = _canonical_digest_value(fingerprint)
    except Exception as error:
        if isinstance(error, CanonicalMaterialError):
            raise
        raise CanonicalMaterialError(
            "canonical primary plan digest is unavailable"
        ) from error
    return _canonical_sha256(
        "stock-monitor/canonical-primary-plan/v1",
        payload,
    )


def _canonical_receipt_manifest(
    source_receipts: tuple[object, ...],
) -> tuple[tuple[int, str, str, str], ...]:
    receipts = _receipt_set(source_receipts)
    return tuple(
        (
            receipt.row_id,
            receipt.observation_sha256,
            receipt.payload_sha256,
            receipt.source_digest,
        )
        for receipt in receipts
    )


def _require_receipt_manifest(
    value: object,
) -> tuple[tuple[int, str, str, str], ...]:
    if type(value) is not tuple or not value:
        raise CanonicalMaterialError(
            "composition authority receipt manifest is invalid"
        )
    manifest: list[tuple[int, str, str, str]] = []
    for item in value:
        if (
            type(item) is not tuple
            or len(item) != 4
            or type(item[0]) is not int
            or item[0] < 1
        ):
            raise CanonicalMaterialError(
                "composition authority receipt manifest is invalid"
            )
        row_id, observation_sha256, payload_sha256, source_digest = item
        manifest.append(
            (
                row_id,
                _require_digest(
                    observation_sha256,
                    "composition receipt observation digest",
                ),
                _require_digest(
                    payload_sha256,
                    "composition receipt payload digest",
                ),
                _require_digest(
                    source_digest,
                    "composition receipt source digest",
                ),
            )
        )
    exact = tuple(manifest)
    if (
        len({item[0] for item in exact}) != len(exact)
        or len({item[1] for item in exact}) != len(exact)
        or exact != tuple(sorted(exact, key=lambda item: (item[1], item[0])))
    ):
        raise CanonicalMaterialError(
            "composition authority receipt manifest is not canonical"
        )
    return exact


def _snapshot_digest(snapshot: object) -> str:
    from .workflows import PremarketSnapshot

    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket composition requires an exact normalized snapshot"
        )
    return _canonical_sha256(
        "stock-monitor/canonical-premarket-snapshot/v1",
        _canonical_digest_value(snapshot),
    )


def _positions_digest(positions: object) -> str:
    from .reports import ClosePosition, UnverifiedClosePosition

    if type(positions) is not tuple or any(
        type(position) not in {ClosePosition, UnverifiedClosePosition}
        for position in positions
    ):
        raise CanonicalMaterialError(
            "close composition requires exact report projections"
        )
    return _canonical_sha256(
        "stock-monitor/canonical-close-positions/v1",
        _canonical_digest_value(positions),
    )


def _validation_window_id(value: object) -> str:
    if type(value) is not str or not value or "\x00" in value:
        raise CanonicalMaterialError(
            "premarket material requires a validation window identity"
        )
    return value


def _validate_reason_tuple(value: object, label: str) -> tuple[str, ...]:
    if (
        type(value) is not tuple
        or not value
        or any(
            type(reason) is not str
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", reason) is None
            for reason in value
        )
        or len(set(value)) != len(value)
    ):
        raise CanonicalMaterialError(f"{label} are invalid")
    return value


def _canonical_close_projection(
    actual_state: object,
    positions: tuple[object, ...],
    coordinator_reason_codes: tuple[str, ...] = (),
) -> tuple[str, str, int, tuple[str, ...]]:
    """Derive the only close outcome, workflow exit, and ordered reasons."""
    from .reconciliation import ActualLedgerState
    from .reports import ClosePosition, UnverifiedClosePosition

    if type(actual_state) is not ActualLedgerState:
        raise CanonicalMaterialError(
            "close projection requires an exact actual state"
        )
    _positions_digest(positions)
    raw_reconciliation_reasons = actual_state.reconciliation_reasons
    if type(raw_reconciliation_reasons) is not tuple:
        raise CanonicalMaterialError(
            "close actual reconciliation reasons are invalid"
        )
    reconciliation_reasons = tuple(
        reason
        for reason in raw_reconciliation_reasons
        if reason not in _LEGACY_ACTUAL_ENTRY_CONTEXT_REASONS
    )
    if reconciliation_reasons:
        _validate_reason_tuple(
            reconciliation_reasons,
            "close actual reconciliation reasons",
        )
        if _COORDINATOR_CLOSE_REASONS.intersection(reconciliation_reasons):
            raise CanonicalMaterialError(
                "close child reasons contain a coordinator-only reason"
            )

    if (
        type(coordinator_reason_codes) is not tuple
        or len(set(coordinator_reason_codes)) != len(coordinator_reason_codes)
        or any(
            reason not in _CANONICAL_DATA_REASONS
            for reason in coordinator_reason_codes
        )
    ):
        raise CanonicalMaterialError(
            "close coordinator data reasons are invalid"
        )

    present = set()
    ordered_reasons = list(reconciliation_reasons)
    if reconciliation_reasons:
        present.add("RECONCILIATION_REQUIRED")
    if coordinator_reason_codes:
        present.add("DATA_UNAVAILABLE")
        ordered_reasons.extend(coordinator_reason_codes)
    for position in positions:
        position_reasons = position.reason_codes
        if position_reasons:
            _validate_reason_tuple(
                position_reasons,
                "close position reasons",
            )
            if _COORDINATOR_CLOSE_REASONS.intersection(position_reasons):
                raise CanonicalMaterialError(
                    "close child reasons contain a coordinator-only reason"
                )
            ordered_reasons.extend(position_reasons)
        if type(position) is UnverifiedClosePosition:
            present.add(position.status)
            continue
        if type(position) is not ClosePosition:
            raise CanonicalMaterialError(
                "close projection contains an unsupported position"
            )
        if position.user_confirmed_stop is None:
            present.add("STOP_UNVERIFIED")
        present.add(position.action)

    dominant = "HOLD"
    for status, *_rest in _CLOSE_BRANCHES:
        if status in present:
            dominant = status
            break
    report_outcome, workflow_outcome, exit_code = (
        _CLOSE_BRANCH_PROJECTIONS[dominant]
    )
    if (
        not reconciliation_reasons
        and not coordinator_reason_codes
        and not actual_state.positions
        and not positions
    ):
        ordered_reasons.append("NO_ACTUAL_POSITIONS")
    if dominant in {"EXIT", "TIGHTEN_STOP", "HOLD"}:
        ordered_reasons.append("MANUAL_VERIFICATION_REQUIRED")
    reasons = tuple(dict.fromkeys(ordered_reasons))
    _validate_reason_tuple(reasons, "close canonical reasons")
    return report_outcome, workflow_outcome, exit_code, reasons


def _validate_premarket_outcome_reasons(
    outcome: object,
    reason_codes: object,
) -> tuple[str, ...]:
    reasons = _validate_reason_tuple(
        reason_codes,
        "premarket composition reasons",
    )
    if type(outcome) is not str or reasons not in _PREMARKET_OUTCOME_REASONS.get(
        outcome,
        frozenset(),
    ):
        raise CanonicalMaterialError(
            "premarket outcome and reasons are not an exact canonical branch"
        )
    return reasons


@dataclass(frozen=True, slots=True)
class _PremarketCompositionEnvelope:
    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    validation_window_id: str
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    snapshot_digest: str
    publication_decision_digest: str | None
    primary_plan_digest: str | None
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    capacity_decision_digest: str | None = None
    capacity_candidate_digest: str | None = None
    source_binding_digest: str | None = None
    decision_basis: tuple[tuple[int, str, str], ...] = ()
    calendar_release_sha256: str | None = None
    universe_release_sha256: str | None = None
    evidence_release_sha256: str | None = None
    phase1_replay_digest: str | None = None


@dataclass(frozen=True, slots=True)
class _CompositionAuthorityCandidate:
    authority_reference: ReferenceType[object]
    authority_fingerprint: object
    envelope: object
    identity_children: tuple[object, ...]
    journal_reference: ReferenceType[object] | None = None
    journal_generation: int | None = None
    source_binding_candidate: object | None = None
    phase1_candidates: tuple[object, ...] = ()
    risk_children: tuple[object, ...] = ()
    semantic_children: tuple[object, ...] = ()
    semantic_fingerprint: object | None = None


@dataclass(frozen=True, slots=True, weakref_slot=True)
class _ClosePositionAuthorityBundle:
    """Private exact-source chain behind one projected close position."""

    projection: object
    branch: str
    identity_children: tuple[object, ...]
    source_digest: str

    def __post_init__(self) -> None:
        from .reports import ClosePosition, UnverifiedClosePosition

        if type(self.projection) not in {
            ClosePosition,
            UnverifiedClosePosition,
        }:
            raise CanonicalMaterialError(
                "close position authority projection is invalid"
            )
        if (
            type(self.branch) is not str
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", self.branch) is None
            or type(self.identity_children) is not tuple
            or not self.identity_children
        ):
            raise CanonicalMaterialError(
                "close position authority chain is invalid"
            )
        expected = _close_position_authority_digest(
            projection=self.projection,
            branch=self.branch,
            identity_children=self.identity_children,
        )
        if self.source_digest != expected:
            raise CanonicalMaterialError(
                "close position authority digest is inconsistent"
            )


@dataclass(frozen=True, slots=True)
class _ClosePositionAuthorityCandidate:
    bundle_reference: ReferenceType[object]
    coordinator_reference: ReferenceType[object]
    journal_reference: ReferenceType[object]
    journal_generation: int
    bundle_fingerprint: object
    projection: object
    identity_children: tuple[object, ...]
    actual_state: object
    actual_replay_source: object
    query_cutoff: datetime
    calendar_resolver: object
    policy: object


def _close_position_authority_child_material(value: object) -> object:
    """Project one exact authority child into deterministic digest material."""
    for field_name in (
        "source_digest",
        "context_digest",
        "authority_digest",
    ):
        digest = getattr(value, field_name, None)
        if (
            type(digest) is str
            and len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest)
        ):
            value_type = type(value)
            return {
                "type": f"{value_type.__module__}.{value_type.__qualname__}",
                "digest_field": field_name,
                "digest": digest,
            }
    return _canonical_digest_value(value)


def _close_position_authority_digest(
    *,
    projection: object,
    branch: str,
    identity_children: tuple[object, ...],
) -> str:
    return _canonical_sha256(
        "stock-monitor/actual-close-position-authority/v1",
        {
            "branch": branch,
            "projection": _canonical_digest_value(projection),
            "identity_children": [
                _close_position_authority_child_material(child)
                for child in identity_children
            ],
        },
    )


def _issue_close_position_authority_bundle(
    *,
    projection: object,
    branch: str,
    identity_children: tuple[object, ...],
    coordinator: object,
    actual_state: object,
    actual_replay_source: object,
    query_cutoff: datetime,
) -> _ClosePositionAuthorityBundle:
    trusted_frame = None
    frame = inspect.currentframe()
    try:
        candidate_frame = None if frame is None else frame.f_back
        while candidate_frame is not None:
            if (
                candidate_frame.f_code
                is ActualCloseWorkflowCoordinator._project_position.__code__
                and candidate_frame.f_globals is globals()
                and candidate_frame.f_locals.get("self") is coordinator
            ):
                trusted_frame = candidate_frame
                break
            candidate_frame = candidate_frame.f_back
    finally:
        del frame, candidate_frame
    if trusted_frame is None:
        raise CanonicalMaterialError(
            "close position authority issuer is unavailable"
        )
    journal = getattr(coordinator, "_journal", None)
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError(
            "close position authority Journal generation is invalid"
        )
    bundle = _ClosePositionAuthorityBundle(
        projection=projection,
        branch=branch,
        identity_children=identity_children,
        source_digest=_close_position_authority_digest(
            projection=projection,
            branch=branch,
            identity_children=identity_children,
        ),
    )
    identity = id(bundle)

    def discard(dead: ReferenceType[object]) -> None:
        with _CLOSE_POSITION_AUTHORITY_LOCK:
            current = _ISSUED_CLOSE_POSITION_AUTHORITIES.get(identity)
            if current is not None and current.bundle_reference is dead:
                _ISSUED_CLOSE_POSITION_AUTHORITIES.pop(identity, None)

    candidate = _ClosePositionAuthorityCandidate(
        bundle_reference=ref(bundle, discard),
        coordinator_reference=ref(coordinator),
        journal_reference=ref(journal),
        journal_generation=generation,
        bundle_fingerprint=_value_fingerprint(bundle),
        projection=projection,
        identity_children=identity_children,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        query_cutoff=query_cutoff,
        calendar_resolver=getattr(coordinator, "_calendar_resolver", None),
        policy=getattr(coordinator, "_policy", None),
    )
    with _CLOSE_POSITION_AUTHORITY_LOCK:
        if _ISSUED_CLOSE_POSITION_AUTHORITIES.get(identity) is not None:
            raise CanonicalMaterialError(
                "close position authority identity is already registered"
            )
        _ISSUED_CLOSE_POSITION_AUTHORITIES[identity] = candidate
    return bundle


def _close_semantic_fingerprint(
    semantic_children: tuple[object, ...],
) -> object:
    """Seal both exact identities and values for retained close authorities."""
    if type(semantic_children) is not tuple:
        raise CanonicalMaterialError(
            "close semantic authority children are invalid"
        )
    sealed: list[object] = []
    for child in semantic_children:
        if type(child) is _ClosePositionAuthorityBundle:
            sealed.append(
                (
                    "position-authority",
                    id(child),
                    id(child.projection),
                    _value_fingerprint(child.projection),
                    child.branch,
                    child.source_digest,
                    tuple(
                        (id(source), _value_fingerprint(source))
                        for source in child.identity_children
                    ),
                )
            )
        else:
            sealed.append(
                (
                    "authority-child",
                    id(child),
                    _value_fingerprint(child),
                )
            )
    return ("close-semantic-authority/v1", tuple(sealed))


_PREMARKET_COMPOSITION_LOCK = threading.Lock()
_CLOSE_COMPOSITION_LOCK = threading.Lock()
_CLOSE_POSITION_AUTHORITY_LOCK = threading.Lock()
_ISSUED_PREMARKET_COMPOSITIONS: dict[int, _CompositionAuthorityCandidate] = {}
_ISSUED_CLOSE_COMPOSITIONS: dict[int, _CompositionAuthorityCandidate] = {}
_ISSUED_CLOSE_POSITION_AUTHORITIES: dict[
    int,
    _ClosePositionAuthorityCandidate,
] = {}


def _premarket_composition_identity_children(
    *,
    snapshot: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
) -> tuple[object, ...]:
    receipts = _receipt_set(source_receipts)
    return (
        snapshot,
        *receipts,
        publication_decision,
        primary_plan,
    )


def _composition_digest(domain: str, values: dict[str, object]) -> str:
    return _canonical_sha256(domain, values)


def _premarket_composition_envelope(
    *,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    validation_window_id: str,
    source_receipts: tuple[object, ...],
    snapshot: object,
    publication_decision: object | None,
    primary_plan: object | None,
    capacity_decision_digest: str | None = None,
    capacity_candidate_digest: str | None = None,
    outcome: str,
    reason_codes: tuple[str, ...],
    source_binding_digest: str | None = None,
    decision_basis: tuple[tuple[int, str, str], ...] = (),
    calendar_release_sha256: str | None = None,
    universe_release_sha256: str | None = None,
    evidence_release_sha256: str | None = None,
    phase1_replay_digest: str | None = None,
) -> _PremarketCompositionEnvelope:
    from .workflows import PremarketSnapshot

    session_date = _require_session(session_date)
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(session_date, decision_at, retrieved_at)
    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket composition requires an exact normalized snapshot"
        )
    receipts = _receipt_set(source_receipts)
    _validate_receipt_envelope(
        receipts,
        kind="PREMARKET",
        economic_at=decision_at,
        retrieved_at=retrieved_at,
        decision_basis=decision_basis,
    )
    has_candidates = bool(snapshot.candidates)
    if has_candidates != (
        publication_decision is not None and primary_plan is not None
    ):
        raise CanonicalMaterialError(
            "premarket candidate state requires its decision and primary plan"
        )
    reasons = _validate_premarket_outcome_reasons(outcome, reason_codes)
    if (
        has_candidates != (outcome == "CANDIDATES")
        or snapshot.breaker_active != (reasons == ("ACTIVE_BREAKER",))
    ):
        raise CanonicalMaterialError(
            "premarket composition outcome contradicts its snapshot"
        )
    window_id = _validation_window_id(validation_window_id)
    receipt_manifest = _canonical_receipt_manifest(receipts)
    source_digest = canonical_source_digest(receipts)
    snapshot_digest = _snapshot_digest(snapshot)
    decision_digest = _publication_decision_digest(publication_decision)
    plan_digest = _long_plan_digest(primary_plan)
    if capacity_decision_digest is not None:
        _require_digest(
            capacity_decision_digest,
            "premarket capacity-decision digest",
        )
    if capacity_candidate_digest is not None:
        _require_digest(
            capacity_candidate_digest,
            "premarket capacity-candidate digest",
        )
    capacity_blocked = reasons == ("NO_PRIMARY_CAPACITY",)
    capacity_pair_complete = (
        capacity_decision_digest is not None
        and capacity_candidate_digest is not None
    )
    capacity_pair_malformed = (capacity_decision_digest is None) != (
        capacity_candidate_digest is None
    )
    if capacity_blocked != capacity_pair_complete or capacity_pair_malformed:
        raise CanonicalMaterialError(
            "premarket capacity branch lacks its exact candidate and decision digests"
        )
    extended_values = (
        source_binding_digest,
        calendar_release_sha256,
        universe_release_sha256,
        evidence_release_sha256,
        phase1_replay_digest,
    )
    if any(value is not None for value in extended_values):
        if any(value is None for value in extended_values):
            raise CanonicalMaterialError(
                "premarket composition source authority is incomplete"
            )
        for value, label in zip(
            extended_values,
            (
                "premarket source binding digest",
                "premarket calendar release digest",
                "premarket universe release digest",
                "premarket evidence release digest",
                "premarket Phase 1 replay digest",
            ),
            strict=True,
        ):
            _require_digest(value, label)
        _require_premarket_decision_basis(decision_basis)
        if tuple(item[0] for item in decision_basis) != tuple(
            item[0] for item in receipt_manifest
        ):
            raise CanonicalMaterialError(
                "premarket composition decision basis conflicts with receipts"
            )
    elif decision_basis:
        raise CanonicalMaterialError(
            "premarket composition decision basis lacks source authority"
        )
    values: dict[str, object] = {
        "version": 1,
        "session_date": session_date.isoformat(),
        "decision_at": _canonical_timestamp(decision_at),
        "retrieved_at": _canonical_timestamp(retrieved_at),
        "validation_window_id": window_id,
        "receipt_manifest": receipt_manifest,
        "source_digest": source_digest,
        "snapshot_digest": snapshot_digest,
        "publication_decision_digest": decision_digest,
        "primary_plan_digest": plan_digest,
        "capacity_decision_digest": capacity_decision_digest,
        "capacity_candidate_digest": capacity_candidate_digest,
        "outcome": outcome,
        "reason_codes": reasons,
        "source_binding_digest": source_binding_digest,
        "decision_basis": decision_basis,
        "calendar_release_sha256": calendar_release_sha256,
        "universe_release_sha256": universe_release_sha256,
        "evidence_release_sha256": evidence_release_sha256,
        "phase1_replay_digest": phase1_replay_digest,
    }
    return _PremarketCompositionEnvelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=window_id,
        receipt_manifest=receipt_manifest,
        source_digest=source_digest,
        snapshot_digest=snapshot_digest,
        publication_decision_digest=decision_digest,
        primary_plan_digest=plan_digest,
        capacity_decision_digest=capacity_decision_digest,
        capacity_candidate_digest=capacity_candidate_digest,
        outcome=outcome,
        reason_codes=reasons,
        composition_digest=_composition_digest(
            "stock-monitor/canonical-premarket-composition/v1",
            values,
        ),
        source_binding_digest=source_binding_digest,
        decision_basis=decision_basis,
        calendar_release_sha256=calendar_release_sha256,
        universe_release_sha256=universe_release_sha256,
        evidence_release_sha256=evidence_release_sha256,
        phase1_replay_digest=phase1_replay_digest,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketCompositionAuthority:
    """Task 10 hook for a complete premarket composition capability.

    This value is intentionally not self-authenticating.  Only the public
    issuer registers one exact owner-bound capability; a syntactically valid
    caller-built copy never passes its issuance predicate.
    """

    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    validation_window_id: str
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    snapshot_digest: str
    publication_decision_digest: str | None
    primary_plan_digest: str | None
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    capacity_decision_digest: str | None = None
    capacity_candidate_digest: str | None = None
    source_binding_digest: str | None = None
    decision_basis: tuple[tuple[int, str, str], ...] = ()
    calendar_release_sha256: str | None = None
    universe_release_sha256: str | None = None
    evidence_release_sha256: str | None = None
    phase1_replay_digest: str | None = None

    def __post_init__(self) -> None:
        session_date = _require_session(self.session_date)
        decision_at = _require_time(
            self.decision_at,
            "premarket composition decision time",
        )
        retrieved_at = _require_time(
            self.retrieved_at,
            "premarket composition retrieval time",
        )
        _validate_premarket_times(session_date, decision_at, retrieved_at)
        _validation_window_id(self.validation_window_id)
        _require_receipt_manifest(self.receipt_manifest)
        for value, label in (
            (self.source_digest, "premarket composition source digest"),
            (self.snapshot_digest, "premarket composition snapshot digest"),
            (self.composition_digest, "premarket composition digest"),
        ):
            _require_digest(value, label)
        decision_digest = self.publication_decision_digest
        plan_digest = self.primary_plan_digest
        if (decision_digest is None) != (plan_digest is None):
            raise CanonicalMaterialError(
                "premarket composition plan and decision digests must be paired"
            )
        if decision_digest is not None:
            _require_digest(
                decision_digest,
                "premarket composition publication-decision digest",
            )
            _require_digest(plan_digest, "premarket composition primary-plan digest")
        capacity_digest = self.capacity_decision_digest
        if capacity_digest is not None:
            _require_digest(
                capacity_digest,
                "premarket composition capacity-decision digest",
            )
        capacity_candidate_digest = self.capacity_candidate_digest
        if capacity_candidate_digest is not None:
            _require_digest(
                capacity_candidate_digest,
                "premarket composition capacity-candidate digest",
            )
        reasons = _validate_premarket_outcome_reasons(
            self.outcome,
            self.reason_codes,
        )
        if (self.outcome == "CANDIDATES") != (decision_digest is not None):
            raise CanonicalMaterialError(
                "premarket composition candidate authority is inconsistent"
            )
        capacity_pair_complete = (
            capacity_digest is not None
            and capacity_candidate_digest is not None
        )
        capacity_pair_malformed = (capacity_digest is None) != (
            capacity_candidate_digest is None
        )
        if (
            (reasons == ("NO_PRIMARY_CAPACITY",))
            != capacity_pair_complete
            or capacity_pair_malformed
        ):
            raise CanonicalMaterialError(
                "premarket composition capacity authority is inconsistent"
            )
        if reasons == ("ACTIVE_BREAKER",) and self.outcome != "NO TRADE":
            raise CanonicalMaterialError(
                "premarket breaker composition is inconsistent"
            )
        extended_values = (
            self.source_binding_digest,
            self.calendar_release_sha256,
            self.universe_release_sha256,
            self.evidence_release_sha256,
            self.phase1_replay_digest,
        )
        if any(value is not None for value in extended_values):
            if any(value is None for value in extended_values):
                raise CanonicalMaterialError(
                    "premarket composition source authority is incomplete"
                )
            for value, label in zip(
                extended_values,
                (
                    "premarket source binding digest",
                    "premarket calendar release digest",
                    "premarket universe release digest",
                    "premarket evidence release digest",
                    "premarket Phase 1 replay digest",
                ),
                strict=True,
            ):
                _require_digest(value, label)
            _require_premarket_decision_basis(self.decision_basis)
        elif self.decision_basis:
            raise CanonicalMaterialError(
                "premarket composition decision basis lacks source authority"
            )


def _premarket_risk_child_is_current_without_callbacks(child: object) -> bool:
    from . import risk as risk_module

    if type(child) is risk_module.BreakerState:
        return risk_module._is_current_breaker_state_without_callbacks(child)
    if type(child) is risk_module.LongPlanDecision:
        portfolio = child.portfolio_authority
        return bool(
            portfolio is not None
            and risk_module._is_current_portfolio_risk_authority_without_callbacks(
                portfolio
            )
            and risk_module._phase1_derived_sources_are_current_without_callbacks(
                portfolio
            )
            and risk_module._is_current_risk_authority_without_callbacks(
                risk_module._LONG_PLAN_AUTHORITIES,
                child,
                exact_type=risk_module.LongPlanDecision,
                children=(portfolio,),
            )
        )
    return False


def _premarket_semantic_child_is_current_without_callbacks(
    child: object,
    digest: str,
) -> bool:
    from . import screening as screening_module

    if type(child) is not screening_module.ScoredCandidate:
        return False
    try:
        current_digest = screening_module._scored_candidate_fingerprint(child)
    except Exception:
        return False
    with screening_module._ISSUED_SCORED_CANDIDATES_LOCK:
        issued = screening_module._ISSUED_SCORED_CANDIDATES.get(id(child))
        return bool(
            type(issued) is screening_module._IssuedScoredCandidateAuthority
            and issued.reference() is child
            and issued.candidate_digest == digest
            and current_digest == digest
        )


def _is_issued_premarket_composition_authority(
    authority: object,
    *,
    envelope: _PremarketCompositionEnvelope,
    identity_children: tuple[object, ...],
) -> bool:
    """Verify one exact registered Task 10 composition envelope."""
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or type(envelope) is not _PremarketCompositionEnvelope
        or type(identity_children) is not tuple
    ):
        return False
    try:
        authority_fingerprint = _value_fingerprint(authority)
        envelope_fingerprint = _value_fingerprint(envelope)
    except Exception:
        return False
    if (
        authority.session_date != envelope.session_date
        or authority.decision_at != envelope.decision_at
        or authority.retrieved_at != envelope.retrieved_at
        or authority.validation_window_id != envelope.validation_window_id
        or authority.receipt_manifest != envelope.receipt_manifest
        or authority.source_digest != envelope.source_digest
        or authority.snapshot_digest != envelope.snapshot_digest
        or authority.publication_decision_digest
        != envelope.publication_decision_digest
        or authority.primary_plan_digest != envelope.primary_plan_digest
        or authority.capacity_decision_digest
        != envelope.capacity_decision_digest
        or authority.capacity_candidate_digest
        != envelope.capacity_candidate_digest
        or authority.outcome != envelope.outcome
        or authority.reason_codes != envelope.reason_codes
        or authority.composition_digest != envelope.composition_digest
        or authority.source_binding_digest != envelope.source_binding_digest
        or authority.decision_basis != envelope.decision_basis
        or authority.calendar_release_sha256
        != envelope.calendar_release_sha256
        or authority.universe_release_sha256
        != envelope.universe_release_sha256
        or authority.evidence_release_sha256
        != envelope.evidence_release_sha256
        or authority.phase1_replay_digest != envelope.phase1_replay_digest
    ):
        return False
    with _PREMARKET_COMPOSITION_LOCK:
        candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
        if (
            type(candidate) is not _CompositionAuthorityCandidate
            or type(candidate.authority_reference) is not ReferenceType
            or candidate.authority_reference() is not authority
            or type(candidate.envelope) is not _PremarketCompositionEnvelope
            or type(candidate.identity_children) is not tuple
            or (
                authority.source_binding_digest is None
                and len(candidate.identity_children) != len(identity_children)
            )
            or len(candidate.identity_children) < len(identity_children)
            or any(
                current is not expected
                for current, expected in zip(
                    identity_children,
                    candidate.identity_children,
                    strict=False,
                )
            )
        ):
            return False
        try:
            candidate_envelope_fingerprint = _value_fingerprint(
                candidate.envelope
            )
        except Exception:
            return False
        extended_current = True
        if authority.source_binding_digest is not None:
            journal = (
                None
                if candidate.journal_reference is None
                else candidate.journal_reference()
            )
            source_candidate = candidate.source_binding_candidate
            source_authority_index = len(identity_children)
            source_authority = (
                None
                if source_authority_index >= len(candidate.identity_children)
                else candidate.identity_children[source_authority_index]
            )
            from . import journal as journal_module

            extended_current = bool(
                journal is not None
                and not getattr(journal, "_closed", True)
                and getattr(journal, "_source_generation", None)
                == candidate.journal_generation
                and type(source_candidate) is _PremarketSourceBindingCandidate
                and _is_current_premarket_source_binding_without_callbacks(
                    source_authority,
                    source_candidate,
                )
                and all(
                    journal_module._is_current_journal_authority_candidate_without_callbacks(
                        phase1_candidate
                    )
                    for phase1_candidate in candidate.phase1_candidates
                )
                and all(
                    _premarket_risk_child_is_current_without_callbacks(child)
                    for child in candidate.risk_children
                )
                and all(
                    _premarket_semantic_child_is_current_without_callbacks(
                        child,
                        digest,
                    )
                    for child, digest in candidate.semantic_children
                )
            )
        return bool(
            candidate.authority_fingerprint == authority_fingerprint
            and candidate_envelope_fingerprint == envelope_fingerprint
            and extended_current
            and _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority)) is candidate
        )


def _premarket_composition_extended_values(
    authority: object,
) -> dict[str, object]:
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or authority.source_binding_digest is None
    ):
        return {}
    return {
        "capacity_decision_digest": authority.capacity_decision_digest,
        "capacity_candidate_digest": authority.capacity_candidate_digest,
        "source_binding_digest": authority.source_binding_digest,
        "decision_basis": authority.decision_basis,
        "calendar_release_sha256": authority.calendar_release_sha256,
        "universe_release_sha256": authority.universe_release_sha256,
        "evidence_release_sha256": authority.evidence_release_sha256,
        "phase1_replay_digest": authority.phase1_replay_digest,
    }


def issue_canonical_premarket_composition_authority(
    *,
    journal: object,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    validation_window_id: str,
    source_binding_authority: object,
    snapshot: object,
    publication_decision: object | None,
    primary_plan: object | None,
    capacity_decision: object | None = None,
    capacity_candidate: object | None = None,
    outcome: str,
    reason_codes: tuple[str, ...],
    calendar: object,
    universe: object,
    evidence_release: object,
    phase1_replay_children: tuple[object, ...],
    breaker_state: object | None = None,
    validation_breaker_state: object | None = None,
) -> CanonicalPremarketCompositionAuthority:
    """Seal reviewed releases, receipt bindings, and replay children together."""
    from . import evidence as evidence_module
    from . import journal as journal_module
    from . import market_calendar as calendar_module
    from . import risk as risk_module
    from . import screening as screening_module
    from . import universe as universe_module
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError(
            "premarket composition requires an open Journal owner"
        )
    session_date = _require_session(session_date)
    decision_at = _require_time(decision_at, "premarket decision time")
    retrieved_at = _require_time(retrieved_at, "premarket retrieval time")
    _validate_premarket_times(session_date, decision_at, retrieved_at)
    if (
        type(phase1_replay_children) is not tuple
        or len({id(child) for child in phase1_replay_children})
        != len(phase1_replay_children)
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children are invalid"
        )

    with _PREMARKET_SOURCE_BINDING_LOCK:
        source_candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(
            id(source_binding_authority)
        )
    if (
        type(source_binding_authority)
        is not CanonicalPremarketSourceBindingAuthority
        or type(source_candidate) is not _PremarketSourceBindingCandidate
        or source_candidate.journal_reference() is not journal
        or source_binding_authority.decision_at != decision_at
        or source_binding_authority.retrieved_at != retrieved_at
        or not _is_current_premarket_source_binding_without_callbacks(
            source_binding_authority,
            source_candidate,
        )
    ):
        raise CanonicalMaterialError(
            "premarket source binding authority has the wrong owner or is not current"
        )

    # Exhaust provider predicates and reviewed-release validators before the
    # final Journal generation/candidate capture.
    _premarket_binding_manifest(
        source_candidate.bindings,
        invoke_provider_predicate=True,
    )
    calendar_identity = _reviewed_binding_identity(calendar)
    universe_identity = _reviewed_binding_identity(universe)
    evidence_identity = _reviewed_binding_identity(evidence_release)
    if (
        type(calendar) is not calendar_module.MarketCalendar
        or type(universe) is not universe_module.UniverseSnapshot
        or type(evidence_release) is not evidence_module.ReviewedEvidenceRelease
        or calendar_identity is None
        or universe_identity is None
        or evidence_identity is None
        or calendar.year != session_date.year
        or evidence_release.universe_sha256 != universe_identity[1]
    ):
        raise CanonicalMaterialError(
            "premarket reviewed calendar, universe, or evidence release is inconsistent"
        )
    bound_sources = tuple(binding.source for binding in source_candidate.bindings)
    required_reviewed_children = (
        calendar,
        universe,
        evidence_release,
        *tuple(evidence_release.by_symbol.values()),
    )
    if any(
        sum(source is child for source in bound_sources) != 1
        for child in required_reviewed_children
    ):
        raise CanonicalMaterialError(
            "premarket reviewed release binding is incomplete"
        )

    expected_phase1_values = list(
        bundle._phase1_source
        for bundle in evidence_release.by_symbol.values()
        if bundle._phase1_source is not None
    )
    risk_children: list[object] = []
    semantic_children: list[tuple[object, str]] = []
    capacity_candidate_digest: str | None = None
    if validation_breaker_state is not None:
        if (
            type(validation_breaker_state) is not risk_module.BreakerState
            or not risk_module.is_issued_breaker_state(
                validation_breaker_state
            )
        ):
            raise CanonicalMaterialError(
                "premarket validation breaker authority is unavailable"
            )
        validation_sources = tuple(
            source
            for source, kind in risk_module._phase1_bound_sources(
                validation_breaker_state
            )
            if kind == "BREAKER_HISTORY"
        )
        if (
            len(validation_sources) != 1
            or validation_sources[0].validation_window_id
            != validation_window_id
        ):
            raise CanonicalMaterialError(
                "premarket validation window conflicts with breaker history"
            )
        expected_phase1_values.extend(validation_sources)
        risk_children.append(validation_breaker_state)
    if (capacity_decision is None) != (capacity_candidate is None):
        raise CanonicalMaterialError(
            "premarket capacity candidate and decision must be paired"
        )
    if capacity_decision is not None:
        try:
            capacity_request = risk_module.LongPlanRequest.from_scored_candidate(
                capacity_candidate
            )
            capacity_candidate_digest = (
                screening_module._scored_candidate_fingerprint(
                    capacity_candidate
                )
            )
        except Exception as error:
            raise CanonicalMaterialError(
                "premarket capacity candidate authority is unavailable"
            ) from error
        if (
            type(capacity_candidate) is not screening_module.ScoredCandidate
            or not screening_module.is_issued_scored_candidate(
                capacity_candidate
            )
            or capacity_candidate.publication_session != session_date
            or type(capacity_decision) is not risk_module.LongPlanDecision
            or not risk_module.is_issued_long_plan_decision(capacity_decision)
            or capacity_decision.eligible
            or capacity_decision.plan is not None
            or capacity_decision.target is not None
            or capacity_decision.authority_scope != "CANONICAL_PUBLICATION"
            or capacity_decision.as_of != decision_at
            or capacity_decision.request != capacity_request
            or not capacity_decision.reason_codes
            or not set(capacity_decision.reason_codes).issubset(
                _PRIMARY_CAPACITY_REASONS
            )
            or publication_decision is not None
            or primary_plan is not None
            or outcome != "NO TRADE"
            or reason_codes != ("NO_PRIMARY_CAPACITY",)
        ):
            raise CanonicalMaterialError(
                "premarket capacity candidate and decision conflict"
            )
        expected_phase1_values.extend(
            source
            for source, _kind in risk_module._phase1_bound_sources(
                capacity_decision.portfolio_authority
            )
        )
        risk_children.append(capacity_decision)
        semantic_children.append(
            (capacity_candidate, capacity_candidate_digest)
        )
    if breaker_state is not None:
        if not (
            risk_module.is_issued_breaker_state(breaker_state)
            or risk_module.is_issued_paired_breaker_state(breaker_state)
        ):
            raise CanonicalMaterialError(
                "premarket breaker authority is unavailable"
            )
        if snapshot.breaker_active != risk_module.breaker_pauses_entry(
            breaker_state,
            session_date,
        ):
            raise CanonicalMaterialError(
                "premarket snapshot conflicts with breaker authority"
            )
        expected_phase1_values.extend(
            source
            for source, _kind in risk_module._phase1_bound_sources(
                breaker_state
            )
        )
        if not any(breaker_state is child for child in risk_children):
            risk_children.append(breaker_state)
        if primary_plan is not None:
            expected_phase1_values.extend(
                source
                for source, _kind in risk_module._phase1_bound_sources(
                    primary_plan.portfolio_authority
                )
            )
    elif primary_plan is not None or snapshot.breaker_active:
        raise CanonicalMaterialError(
            "premarket risk-bearing composition lacks breaker authority"
        )
    expected_phase1_children: list[object] = []
    for child in expected_phase1_values:
        if not any(child is current for current in expected_phase1_children):
            expected_phase1_children.append(child)
    if (
        len(phase1_replay_children) != len(expected_phase1_children)
        or any(
            supplied is not expected
            for supplied, expected in zip(
                phase1_replay_children,
                expected_phase1_children,
                strict=True,
            )
        )
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children conflict with reviewed evidence"
        )

    phase1_candidates = tuple(
        journal_module._journal_any_source_authority_candidate(child)
        for child in phase1_replay_children
    )
    if any(candidate is None for candidate in phase1_candidates):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay authority is unavailable"
        )
    exact_phase1_candidates = tuple(
        candidate for candidate in phase1_candidates if candidate is not None
    )
    if exact_phase1_candidates and (
        journal_module._current_journal_source_authority_owner(
            exact_phase1_candidates
        )
        is not journal
    ):
        raise CanonicalMaterialError(
            "premarket Phase 1 replay children cross Journal owners"
        )

    # Reacquire the source candidate after every authority callback/check.
    with _PREMARKET_SOURCE_BINDING_LOCK:
        refreshed_source_candidate = _ISSUED_PREMARKET_SOURCE_BINDINGS.get(
            id(source_binding_authority)
        )
    if (
        refreshed_source_candidate is not source_candidate
        or not _is_current_premarket_source_binding_without_callbacks(
            source_binding_authority,
            source_candidate,
        )
    ):
        raise CanonicalMaterialError(
            "premarket source binding changed during composition"
        )

    receipts = tuple(binding.receipt for binding in source_candidate.bindings)
    phase1_digest = _canonical_sha256(
        "stock-monitor/premarket-phase1-replay/v1",
        tuple(_value_fingerprint(child) for child in phase1_replay_children),
    )
    envelope = _premarket_composition_envelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=validation_window_id,
        source_receipts=receipts,
        snapshot=snapshot,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        capacity_decision_digest=_long_plan_digest(capacity_decision),
        capacity_candidate_digest=capacity_candidate_digest,
        outcome=outcome,
        reason_codes=reason_codes,
        source_binding_digest=source_binding_authority.binding_digest,
        decision_basis=source_binding_authority.decision_basis,
        calendar_release_sha256=calendar_identity[1],
        universe_release_sha256=universe_identity[1],
        evidence_release_sha256=evidence_identity[1],
        phase1_replay_digest=phase1_digest,
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
        capacity_decision_digest=envelope.capacity_decision_digest,
        capacity_candidate_digest=envelope.capacity_candidate_digest,
        outcome=envelope.outcome,
        reason_codes=envelope.reason_codes,
        composition_digest=envelope.composition_digest,
        source_binding_digest=envelope.source_binding_digest,
        decision_basis=envelope.decision_basis,
        calendar_release_sha256=envelope.calendar_release_sha256,
        universe_release_sha256=envelope.universe_release_sha256,
        evidence_release_sha256=envelope.evidence_release_sha256,
        phase1_replay_digest=envelope.phase1_replay_digest,
    )
    base_children = _premarket_composition_identity_children(
        snapshot=snapshot,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
    )
    identity_children = (
        *base_children,
        source_binding_authority,
        calendar,
        universe,
        evidence_release,
        *tuple(evidence_release.by_symbol.values()),
        breaker_state,
        validation_breaker_state,
        capacity_decision,
        capacity_candidate,
        *phase1_replay_children,
    )
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("premarket Journal generation is invalid")
    identity = id(authority)

    def discard(dead: ReferenceType[object]) -> None:
        with _PREMARKET_COMPOSITION_LOCK:
            current = _ISSUED_PREMARKET_COMPOSITIONS.get(identity)
            if current is not None and current.authority_reference is dead:
                _ISSUED_PREMARKET_COMPOSITIONS.pop(identity, None)

    candidate = _CompositionAuthorityCandidate(
        authority_reference=ref(authority, discard),
        authority_fingerprint=_value_fingerprint(authority),
        envelope=envelope,
        identity_children=identity_children,
        journal_reference=ref(journal),
        journal_generation=generation,
        source_binding_candidate=source_candidate,
        phase1_candidates=exact_phase1_candidates,
        risk_children=tuple(risk_children),
        semantic_children=tuple(semantic_children),
    )
    with _PREMARKET_COMPOSITION_LOCK:
        _ISSUED_PREMARKET_COMPOSITIONS[identity] = candidate
    if not _is_issued_premarket_composition_authority(
        authority,
        envelope=envelope,
        identity_children=base_children,
    ):
        with _PREMARKET_COMPOSITION_LOCK:
            if _ISSUED_PREMARKET_COMPOSITIONS.get(identity) is candidate:
                _ISSUED_PREMARKET_COMPOSITIONS.pop(identity, None)
        raise CanonicalMaterialError(
            "premarket composition changed during issuance"
        )
    return authority


def is_issued_canonical_premarket_composition_authority(
    authority: object,
) -> bool:
    """Return whether one exact Task 10 composition capability is current."""
    with _PREMARKET_COMPOSITION_LOCK:
        candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
    if (
        type(authority) is not CanonicalPremarketCompositionAuthority
        or type(candidate) is not _CompositionAuthorityCandidate
        or type(candidate.envelope) is not _PremarketCompositionEnvelope
    ):
        return False
    base_length = 1 + len(candidate.envelope.receipt_manifest) + 2
    if authority.source_binding_digest is None:
        return _is_issued_premarket_composition_authority(
            authority,
            envelope=candidate.envelope,
            identity_children=candidate.identity_children,
        )
    source_authority = candidate.identity_children[base_length]
    journal = (
        None
        if candidate.journal_reference is None
        else candidate.journal_reference()
    )
    if journal is None or not (
        is_issued_canonical_premarket_source_binding_authority(
            source_authority,
            journal=journal,
        )
    ):
        return False
    with _PREMARKET_COMPOSITION_LOCK:
        refreshed = _ISSUED_PREMARKET_COMPOSITIONS.get(id(authority))
    if refreshed is not candidate:
        return False
    return _is_issued_premarket_composition_authority(
        authority,
        envelope=candidate.envelope,
        identity_children=candidate.identity_children[:base_length],
    )


def canonical_premarket_state_hash(
    *,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    snapshot: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
    validation_window_id: str,
    outcome: str,
    reason_codes: tuple[str, ...],
    composition_authority: object | None = None,
) -> str:
    """Derive the report state hash from every premarket semantic input."""
    from .workflows import PremarketSnapshot

    if type(snapshot) is not PremarketSnapshot:
        raise CanonicalMaterialError(
            "premarket state hash requires an exact normalized snapshot"
        )
    receipts = _receipt_set(source_receipts)
    extended = _premarket_composition_extended_values(composition_authority)
    envelope = _premarket_composition_envelope(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        validation_window_id=validation_window_id,
        source_receipts=receipts,
        snapshot=snapshot,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        outcome=outcome,
        reason_codes=reason_codes,
        **extended,
    )
    if not _is_issued_premarket_composition_authority(
        composition_authority,
        envelope=envelope,
        identity_children=_premarket_composition_identity_children(
            snapshot=snapshot,
            source_receipts=receipts,
            publication_decision=publication_decision,
            primary_plan=primary_plan,
        ),
    ):
        raise CanonicalMaterialError(
            "premarket composition authority is unavailable"
        )
    payload = {
        "version": 1,
        "session_date": envelope.session_date.isoformat(),
        "decision_at": _canonical_timestamp(envelope.decision_at),
        "retrieved_at": _canonical_timestamp(envelope.retrieved_at),
        "validation_window_id": envelope.validation_window_id,
        "receipt_manifest": envelope.receipt_manifest,
        "source_digest": envelope.source_digest,
        "snapshot_digest": envelope.snapshot_digest,
        "publication_decision_digest": envelope.publication_decision_digest,
        "primary_plan_digest": envelope.primary_plan_digest,
        "capacity_decision_digest": envelope.capacity_decision_digest,
        "capacity_candidate_digest": envelope.capacity_candidate_digest,
        "outcome": envelope.outcome,
        "reason_codes": envelope.reason_codes,
        "composition_digest": envelope.composition_digest,
        "source_binding_digest": envelope.source_binding_digest,
        "decision_basis": envelope.decision_basis,
        "calendar_release_sha256": envelope.calendar_release_sha256,
        "universe_release_sha256": envelope.universe_release_sha256,
        "evidence_release_sha256": envelope.evidence_release_sha256,
        "phase1_replay_digest": envelope.phase1_replay_digest,
    }
    return _canonical_sha256(
        "stock-monitor/canonical-premarket-state/v1",
        payload,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalCloseCompositionAuthority:
    """Task 11 hook for exact close decision composition in every branch.

    The foundation intentionally exposes no public issuer.  Until Task 11 can
    bind every projected position (including an empty projection) to its exact
    market/context/action sources, canonical close material remains unavailable.
    """

    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    actual_state_digest: str
    actual_replay_source_digest: str
    positions_digest: str
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    coordinator_reason_codes: tuple[str, ...] = ()
    review_source_digest: str | None = None
    position_authority_digests: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        session_date = _require_session(self.session_date)
        review_at = _require_time(self.review_at, "close composition review time")
        retrieved_at = _require_time(
            self.retrieved_at,
            "close composition retrieval time",
        )
        query_cutoff = _require_time(
            self.query_cutoff,
            "close composition query cutoff",
        )
        _validate_close_times(
            session_date,
            review_at,
            query_cutoff,
            retrieved_at,
        )
        _require_receipt_manifest(self.receipt_manifest)
        for value, label in (
            (self.source_digest, "close composition source digest"),
            (self.actual_state_digest, "close composition actual-state digest"),
            (
                self.actual_replay_source_digest,
                "close composition replay-source digest",
            ),
            (self.positions_digest, "close composition positions digest"),
            (self.composition_digest, "close composition digest"),
        ):
            _require_digest(value, label)
        if type(self.outcome) is not str or self.outcome not in (
            _CLOSE_OUTCOME_EXIT_CODES
        ):
            raise CanonicalMaterialError("close composition outcome is invalid")
        _validate_reason_tuple(self.reason_codes, "close composition reasons")
        if (
            type(self.coordinator_reason_codes) is not tuple
            or len(set(self.coordinator_reason_codes))
            != len(self.coordinator_reason_codes)
            or any(
                reason not in _CANONICAL_DATA_REASONS
                for reason in self.coordinator_reason_codes
            )
        ):
            raise CanonicalMaterialError(
                "close composition coordinator reasons are invalid"
            )
        if self.review_source_digest is not None:
            _require_digest(
                self.review_source_digest,
                "close composition review-source digest",
            )
        if type(self.position_authority_digests) is not tuple:
            raise CanonicalMaterialError(
                "close position authority digests are invalid"
            )
        for digest in self.position_authority_digests:
            _require_digest(digest, "close position authority digest")


def _is_issued_close_composition_authority(
    authority: object,
    *,
    envelope: object,
    identity_children: tuple[object, ...],
) -> bool:
    """Verify a complete Task 11 envelope and its retained exact sources."""
    from .journal import ActualCloseReviewSource

    if (
        type(authority) is not CanonicalCloseCompositionAuthority
        or type(envelope) is not _CloseCompositionEnvelope
        or type(identity_children) is not tuple
    ):
        return False
    try:
        authority_fingerprint = _value_fingerprint(authority)
        envelope_fingerprint = _value_fingerprint(envelope)
    except Exception:
        return False
    if (
        authority.session_date != envelope.session_date
        or authority.review_at != envelope.review_at
        or authority.retrieved_at != envelope.retrieved_at
        or authority.query_cutoff != envelope.query_cutoff
        or authority.receipt_manifest != envelope.receipt_manifest
        or authority.source_digest != envelope.source_digest
        or authority.actual_state_digest != envelope.actual_state_digest
        or authority.actual_replay_source_digest
        != envelope.actual_replay_source_digest
        or authority.positions_digest != envelope.positions_digest
        or authority.outcome != envelope.outcome
        or authority.reason_codes != envelope.reason_codes
        or authority.composition_digest != envelope.composition_digest
        or authority.coordinator_reason_codes
        != envelope.coordinator_reason_codes
        or authority.review_source_digest != envelope.review_source_digest
        or authority.position_authority_digests
        != envelope.position_authority_digests
    ):
        return False
    with _CLOSE_COMPOSITION_LOCK:
        candidate = _ISSUED_CLOSE_COMPOSITIONS.get(id(authority))
        requires_semantic_sources = authority.review_source_digest is not None
        if (
            type(candidate) is not _CompositionAuthorityCandidate
            or type(candidate.authority_reference) is not ReferenceType
            or candidate.authority_reference() is not authority
            or type(candidate.envelope) is not _CloseCompositionEnvelope
            or type(candidate.identity_children) is not tuple
            or type(candidate.semantic_children) is not tuple
            or (
                requires_semantic_sources
                and (
                    candidate.semantic_fingerprint is None
                    or len(candidate.semantic_children)
                    != len(authority.position_authority_digests) + 1
                    or type(candidate.semantic_children[0])
                    is not ActualCloseReviewSource
                    or candidate.semantic_children[0].source_digest
                    != authority.review_source_digest
                    or any(
                        type(bundle) is not _ClosePositionAuthorityBundle
                        or bundle.projection is not position
                        or bundle.source_digest != digest
                        for bundle, position, digest in zip(
                            candidate.semantic_children[1:],
                            identity_children[
                                2 : 2
                                + len(authority.position_authority_digests)
                            ],
                            authority.position_authority_digests,
                            strict=True,
                        )
                    )
                )
            )
            or len(candidate.identity_children) != len(identity_children)
            or any(
                current is not expected
                for current, expected in zip(
                    identity_children,
                    candidate.identity_children,
                    strict=True,
                )
            )
        ):
            return False
        try:
            candidate_envelope_fingerprint = _value_fingerprint(
                candidate.envelope
            )
            semantic_fingerprint = (
                _close_semantic_fingerprint(candidate.semantic_children)
                if requires_semantic_sources
                else None
            )
        except Exception:
            return False
        return bool(
            candidate.authority_fingerprint == authority_fingerprint
            and candidate_envelope_fingerprint == envelope_fingerprint
            and (
                not requires_semantic_sources
                or semantic_fingerprint == candidate.semantic_fingerprint
            )
            and _ISSUED_CLOSE_COMPOSITIONS.get(id(authority)) is candidate
        )


def _actual_state_digest(state: object) -> str:
    from . import reconciliation as reconciliation_module

    if type(state) is not reconciliation_module.ActualLedgerState:
        raise CanonicalMaterialError("close state hash requires exact actual state")
    try:
        digest = reconciliation_module._actual_state_digest(state)
    except Exception as error:
        raise CanonicalMaterialError(
            "close actual-state digest is unavailable"
        ) from error
    if digest != state.source_digest:
        raise CanonicalMaterialError("close actual-state digest is inconsistent")
    return digest


@dataclass(frozen=True, slots=True)
class _CloseCompositionEnvelope:
    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    receipt_manifest: tuple[tuple[int, str, str, str], ...]
    source_digest: str
    actual_state_digest: str
    actual_replay_source_digest: str
    positions_digest: str
    outcome: str
    reason_codes: tuple[str, ...]
    composition_digest: str
    coordinator_reason_codes: tuple[str, ...] = ()
    review_source_digest: str | None = None
    position_authority_digests: tuple[str, ...] = ()


def _close_composition_identity_children(
    *,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
) -> tuple[object, ...]:
    receipts = _receipt_set(source_receipts)
    return (
        actual_state,
        actual_replay_source,
        *positions,
        *receipts,
    )


def _close_composition_envelope(
    *,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    outcome: str,
    reason_codes: tuple[str, ...],
    coordinator_reason_codes: tuple[str, ...] = (),
    review_source_digest: str | None = None,
    position_authority_digests: tuple[str, ...] = (),
) -> _CloseCompositionEnvelope:
    from .journal import JournalActualReplaySource
    from .reconciliation import ActualLedgerState

    session_date = _require_session(session_date)
    review_at = _require_time(review_at, "close review time")
    retrieved_at = _require_time(retrieved_at, "close retrieval time")
    query_cutoff = _require_time(query_cutoff, "close query cutoff")
    _validate_close_times(
        session_date,
        review_at,
        query_cutoff,
        retrieved_at,
    )
    if (
        type(actual_state) is not ActualLedgerState
        or type(actual_replay_source) is not JournalActualReplaySource
        or actual_state.query_cutoff != query_cutoff
        or actual_replay_source.query_cutoff != query_cutoff
    ):
        raise CanonicalMaterialError(
            "close composition requires exact replay state and source"
        )
    receipts = _receipt_set(source_receipts)
    _validate_receipt_envelope(
        receipts,
        kind="CLOSE",
        economic_at=review_at,
        query_cutoff=query_cutoff,
        retrieved_at=retrieved_at,
    )
    position_digest = _positions_digest(positions)
    reasons = _validate_reason_tuple(reason_codes, "close composition reasons")
    if type(outcome) is not str or outcome not in _CLOSE_OUTCOME_EXIT_CODES:
        raise CanonicalMaterialError("close composition outcome is invalid")
    expected_outcome, _workflow_outcome, _exit_code, expected_reasons = (
        _canonical_close_projection(
            actual_state,
            positions,
            coordinator_reason_codes,
        )
    )
    if outcome != expected_outcome or reasons != expected_reasons:
        raise CanonicalMaterialError(
            "close outcome and reasons contradict the canonical matrix"
        )
    if review_source_digest is not None:
        _require_digest(
            review_source_digest,
            "close composition review-source digest",
        )
    if type(position_authority_digests) is not tuple:
        raise CanonicalMaterialError(
            "close position authority digests are invalid"
        )
    for digest in position_authority_digests:
        _require_digest(digest, "close position authority digest")
    receipt_manifest = _canonical_receipt_manifest(receipts)
    source_digest = canonical_source_digest(receipts)
    state_digest = _actual_state_digest(actual_state)
    replay_digest = _require_digest(
        actual_replay_source.source_digest,
        "close replay-source digest",
    )
    values: dict[str, object] = {
        "version": 1,
        "session_date": session_date.isoformat(),
        "review_at": _canonical_timestamp(review_at),
        "retrieved_at": _canonical_timestamp(retrieved_at),
        "query_cutoff": _canonical_timestamp(query_cutoff),
        "receipt_manifest": receipt_manifest,
        "source_digest": source_digest,
        "actual_state_digest": state_digest,
        "actual_replay_source_digest": replay_digest,
        "positions_digest": position_digest,
        "outcome": outcome,
        "reason_codes": reasons,
        "coordinator_reason_codes": coordinator_reason_codes,
        "review_source_digest": review_source_digest,
        "position_authority_digests": position_authority_digests,
    }
    return _CloseCompositionEnvelope(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        receipt_manifest=receipt_manifest,
        source_digest=source_digest,
        actual_state_digest=state_digest,
        actual_replay_source_digest=replay_digest,
        positions_digest=position_digest,
        outcome=outcome,
        reason_codes=reasons,
        composition_digest=_composition_digest(
            "stock-monitor/canonical-close-composition/v1",
            values,
        ),
        coordinator_reason_codes=coordinator_reason_codes,
        review_source_digest=review_source_digest,
        position_authority_digests=position_authority_digests,
    )


def canonical_close_state_hash(
    *,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    outcome: str,
    reason_codes: tuple[str, ...],
    coordinator_reason_codes: tuple[str, ...] = (),
    review_source_digest: str | None = None,
    position_authority_digests: tuple[str, ...] = (),
    composition_authority: object | None = None,
) -> str:
    """Derive the report state hash from every actual-close semantic input."""
    receipts = _receipt_set(source_receipts)
    envelope = _close_composition_envelope(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
        outcome=outcome,
        reason_codes=reason_codes,
        coordinator_reason_codes=coordinator_reason_codes,
        review_source_digest=review_source_digest,
        position_authority_digests=position_authority_digests,
    )
    if not _is_issued_close_composition_authority(
        composition_authority,
        envelope=envelope,
        identity_children=_close_composition_identity_children(
            actual_state=actual_state,
            actual_replay_source=actual_replay_source,
            positions=positions,
            source_receipts=receipts,
        ),
    ):
        raise CanonicalMaterialError("close composition authority is unavailable")
    payload = {
        "version": 1,
        "session_date": envelope.session_date.isoformat(),
        "review_at": _canonical_timestamp(envelope.review_at),
        "retrieved_at": _canonical_timestamp(envelope.retrieved_at),
        "query_cutoff": _canonical_timestamp(envelope.query_cutoff),
        "receipt_manifest": envelope.receipt_manifest,
        "source_digest": envelope.source_digest,
        "actual_state_digest": envelope.actual_state_digest,
        "actual_replay_source_digest": envelope.actual_replay_source_digest,
        "positions_digest": envelope.positions_digest,
        "outcome": envelope.outcome,
        "reason_codes": envelope.reason_codes,
        "coordinator_reason_codes": envelope.coordinator_reason_codes,
        "review_source_digest": envelope.review_source_digest,
        "position_authority_digests": envelope.position_authority_digests,
        "composition_digest": envelope.composition_digest,
    }
    return _canonical_sha256(
        "stock-monitor/canonical-close-state/v1",
        payload,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketMaterial:
    """Immutable non-fixture premarket report material."""

    session_date: date
    decision_at: datetime
    retrieved_at: datetime
    snapshot: object
    report: object
    source_receipts: tuple[object, ...]
    publication_decision: object | None
    primary_plan: object | None
    validation_window_id: str
    state_hash: str
    source_digest: str
    material_digest: str
    composition_authority: object | None = None

    def __post_init__(self) -> None:
        from .workflows import PremarketSnapshot

        session_date = _require_session(self.session_date)
        decision_at = _require_time(self.decision_at, "premarket decision time")
        retrieved_at = _require_time(self.retrieved_at, "premarket retrieval time")
        _validate_premarket_times(session_date, decision_at, retrieved_at)
        if type(self.snapshot) is not PremarketSnapshot:
            raise CanonicalMaterialError(
                "premarket material requires an exact normalized snapshot"
            )
        if type(self.composition_authority) is not (
            CanonicalPremarketCompositionAuthority
        ):
            raise CanonicalMaterialError(
                "premarket composition authority is unavailable"
            )
        receipts = _require_canonical_receipt_order(self.source_receipts)
        _validate_receipt_envelope(
            receipts,
            kind="PREMARKET",
            economic_at=decision_at,
            retrieved_at=retrieved_at,
            decision_basis=self.composition_authority.decision_basis,
        )
        state_hash = _require_digest(self.state_hash, "premarket state hash")
        _require_digest(self.source_digest, "premarket source digest")
        _require_digest(self.material_digest, "premarket material digest")
        _validation_window_id(self.validation_window_id)
        has_primary = bool(self.snapshot.candidates)
        if has_primary != (
            self.publication_decision is not None and self.primary_plan is not None
        ):
            raise CanonicalMaterialError(
                "premarket candidate material requires its decision and primary plan"
            )
        _validate_report(
            self.report,
            kind="PREMARKET",
            session_date=session_date,
            state_hash=state_hash,
            receipts=receipts,
        )


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalCloseMaterial:
    """Immutable non-fixture actual-close report material."""

    session_date: date
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    actual_state: object
    actual_replay_source: object
    report: object
    positions: tuple[object, ...]
    source_receipts: tuple[object, ...]
    state_hash: str
    source_digest: str
    material_digest: str
    composition_authority: object | None = None
    coordinator_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        from .journal import JournalActualReplaySource
        from .reconciliation import ActualLedgerState
        from .reports import ClosePosition, UnverifiedClosePosition

        session_date = _require_session(self.session_date)
        review_at = _require_time(self.review_at, "close review time")
        retrieved_at = _require_time(self.retrieved_at, "close retrieval time")
        query_cutoff = _require_time(self.query_cutoff, "close query cutoff")
        _validate_close_times(
            session_date,
            review_at,
            query_cutoff,
            retrieved_at,
        )
        if (
            type(self.actual_state) is not ActualLedgerState
            or type(self.actual_replay_source) is not JournalActualReplaySource
            or self.actual_state.query_cutoff != query_cutoff
            or self.actual_replay_source.query_cutoff != query_cutoff
        ):
            raise CanonicalMaterialError(
                "close material requires exact actual replay state and source"
            )
        if type(self.positions) is not tuple or any(
            type(position) not in {ClosePosition, UnverifiedClosePosition}
            for position in self.positions
        ):
            raise CanonicalMaterialError(
                "close positions must contain exact report projections"
            )
        if type(self.composition_authority) is not (
            CanonicalCloseCompositionAuthority
        ):
            raise CanonicalMaterialError(
                "close composition authority is unavailable"
            )
        if (
            type(self.coordinator_reason_codes) is not tuple
            or len(set(self.coordinator_reason_codes))
            != len(self.coordinator_reason_codes)
            or any(
                reason not in _CANONICAL_DATA_REASONS
                for reason in self.coordinator_reason_codes
            )
        ):
            raise CanonicalMaterialError(
                "close material coordinator reasons are invalid"
            )
        receipts = _require_canonical_receipt_order(self.source_receipts)
        _validate_receipt_envelope(
            receipts,
            kind="CLOSE",
            economic_at=review_at,
            query_cutoff=query_cutoff,
            retrieved_at=retrieved_at,
        )
        state_hash = _require_digest(self.state_hash, "close state hash")
        _require_digest(self.source_digest, "close source digest")
        _require_digest(self.material_digest, "close material digest")
        _validate_report(
            self.report,
            kind="CLOSE",
            session_date=session_date,
            state_hash=state_hash,
            receipts=receipts,
        )
        _validate_close_report_projection(
            actual_state=self.actual_state,
            positions=self.positions,
            report=self.report,
            retrieved_at=retrieved_at,
            composition_authority=self.composition_authority,
            coordinator_reason_codes=self.coordinator_reason_codes,
        )


CanonicalMaterial = CanonicalPremarketMaterial | CanonicalCloseMaterial


def _report_reason_codes(report: object) -> tuple[str, ...]:
    from .reports import Report

    if type(report) is not Report:
        raise CanonicalMaterialError("canonical report type is invalid")
    lines = report.body.splitlines()
    try:
        start = lines.index("## Reasons") + 1
        end = next(
            index
            for index in range(start, len(lines))
            if lines[index].startswith("## ")
        )
    except (ValueError, StopIteration) as error:
        raise CanonicalMaterialError(
            "canonical report reason block is malformed"
        ) from error
    reasons: list[str] = []
    for line in lines[start:end]:
        if not line:
            continue
        match = _CANONICAL_REASON.fullmatch(line)
        if match is None:
            raise CanonicalMaterialError(
                "canonical report reason block is malformed"
            )
        reasons.append(match.group(1))
    if not reasons or len(reasons) != len(set(reasons)):
        raise CanonicalMaterialError("canonical report reasons are incomplete")
    return tuple(reasons)


def _composition_envelope_for_material(
    material: CanonicalMaterial,
) -> _PremarketCompositionEnvelope | _CloseCompositionEnvelope:
    """Rebuild the complete semantic envelope before authority issuance."""
    reasons = _report_reason_codes(material.report)
    if type(material) is CanonicalPremarketMaterial:
        return _premarket_composition_envelope(
            session_date=material.session_date,
            decision_at=material.decision_at,
            retrieved_at=material.retrieved_at,
            validation_window_id=material.validation_window_id,
            source_receipts=material.source_receipts,
            snapshot=material.snapshot,
            publication_decision=material.publication_decision,
            primary_plan=material.primary_plan,
            outcome=material.report.outcome,
            reason_codes=reasons,
            **_premarket_composition_extended_values(
                material.composition_authority
            ),
        )
    if type(material) is CanonicalCloseMaterial:
        return _close_composition_envelope(
            session_date=material.session_date,
            review_at=material.review_at,
            retrieved_at=material.retrieved_at,
            query_cutoff=material.query_cutoff,
            actual_state=material.actual_state,
            actual_replay_source=material.actual_replay_source,
            positions=material.positions,
            source_receipts=material.source_receipts,
            outcome=material.report.outcome,
            reason_codes=reasons,
            coordinator_reason_codes=material.coordinator_reason_codes,
            review_source_digest=(
                material.composition_authority.review_source_digest
            ),
            position_authority_digests=(
                material.composition_authority.position_authority_digests
            ),
        )
    raise CanonicalMaterialError("canonical composition material type is invalid")


def _composition_identity_children_without_callbacks(
    material: CanonicalMaterial,
) -> tuple[object, ...]:
    """Return already-validated exact children without invoking source code."""
    if type(material) is CanonicalPremarketMaterial:
        return (
            material.snapshot,
            *material.source_receipts,
            material.publication_decision,
            material.primary_plan,
        )
    return (
        material.actual_state,
        material.actual_replay_source,
        *material.positions,
        *material.source_receipts,
    )


def _composition_is_current_without_callbacks(
    material: CanonicalMaterial,
    envelope: object,
) -> bool:
    children = _composition_identity_children_without_callbacks(material)
    if type(material) is CanonicalPremarketMaterial:
        return bool(
            type(envelope) is _PremarketCompositionEnvelope
            and _is_issued_premarket_composition_authority(
                material.composition_authority,
                envelope=envelope,
                identity_children=children,
            )
        )
    return bool(
        type(material) is CanonicalCloseMaterial
        and type(envelope) is _CloseCompositionEnvelope
        and _is_issued_close_composition_authority(
            material.composition_authority,
            envelope=envelope,
            identity_children=children,
        )
    )


def _report_projection_lines(report: object, header: str) -> tuple[str, ...]:
    lines = report.body.splitlines()
    try:
        index = lines.index(header)
    except ValueError as error:
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        ) from error
    if index + 1 >= len(lines) or lines[index + 1] != "":
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        )
    projection = tuple(lines[index + 2 :])
    if any(line.startswith("## ") for line in projection):
        raise CanonicalMaterialError(
            "canonical report projection block is malformed"
        )
    return projection


def _expected_projection_lines(
    values: tuple[object, ...],
    renderer: object,
) -> tuple[str, ...]:
    if not values:
        return ("None.",)
    lines: list[str] = []
    for ordinal, value in enumerate(values, start=1):
        if ordinal > 1:
            lines.append("")
        lines.extend(renderer(value, ordinal))
    return tuple(lines)


def _validate_generated_at(report: object, retrieved_at: datetime) -> None:
    expected = f"- Generated at: `{retrieved_at.isoformat(timespec='seconds')}`"
    if report.body.splitlines().count(expected) != 1:
        raise CanonicalMaterialError(
            "canonical report generation time conflicts with retrieval"
        )


def _validate_premarket_report_projection(
    *,
    snapshot: object,
    report: object,
    retrieved_at: datetime,
    composition_authority: object,
) -> None:
    from . import reports as reports_module

    materials: list[object] = []
    for candidate in snapshot.candidates:
        if candidate.material is None:
            raise CanonicalMaterialError(
                "canonical candidate lacks an exact report projection"
            )
        materials.append(candidate.material)
    expected = _expected_projection_lines(
        tuple(materials),
        reports_module._render_candidate,
    )
    if _report_projection_lines(report, "## Candidates") != expected:
        raise CanonicalMaterialError(
            "canonical premarket report projection contradicts material"
        )
    _validate_generated_at(report, retrieved_at)
    reasons = _report_reason_codes(report)
    if reasons not in _PREMARKET_OUTCOME_REASONS.get(
        report.outcome,
        frozenset(),
    ):
        raise CanonicalMaterialError(
            "canonical premarket report is not an exact outcome/reason branch"
        )
    if (
        type(composition_authority) is not CanonicalPremarketCompositionAuthority
        or composition_authority.outcome != report.outcome
        or composition_authority.reason_codes != reasons
    ):
        raise CanonicalMaterialError(
            "canonical premarket report conflicts with composition authority"
        )
    if snapshot.breaker_active != ("ACTIVE_BREAKER" in reasons):
        raise CanonicalMaterialError(
            "canonical premarket report contradicts its snapshot"
        )
    if snapshot.candidates:
        if (
            snapshot.breaker_active
            or report.outcome != "CANDIDATES"
            or reasons
            != ("PAPER_PLAN_ONLY", "MANUAL_EXECUTION_REQUIRED")
        ):
            raise CanonicalMaterialError(
                "canonical premarket report contradicts its snapshot"
            )
        return
    if report.outcome not in {"NO TRADE", "NO NEW TRADE - DATA UNAVAILABLE"}:
        raise CanonicalMaterialError(
            "canonical premarket report contradicts its snapshot"
        )


def _validate_close_report_projection(
    *,
    actual_state: object,
    positions: tuple[object, ...],
    report: object,
    retrieved_at: datetime,
    composition_authority: object,
    coordinator_reason_codes: tuple[str, ...] = (),
) -> None:
    from . import reports as reports_module

    expected = _expected_projection_lines(
        positions,
        reports_module._render_close_position,
    )
    if _report_projection_lines(report, "## Positions") != expected:
        raise CanonicalMaterialError(
            "canonical close report projection contradicts material"
        )
    _validate_generated_at(report, retrieved_at)
    reasons = _report_reason_codes(report)
    if (
        type(composition_authority) is not CanonicalCloseCompositionAuthority
        or composition_authority.outcome != report.outcome
        or composition_authority.reason_codes != reasons
        or composition_authority.coordinator_reason_codes
        != coordinator_reason_codes
        or report.outcome not in _CLOSE_OUTCOME_EXIT_CODES
    ):
        raise CanonicalMaterialError(
            "canonical close report conflicts with composition authority"
        )
    expected_outcome, _workflow_outcome, _exit_code, expected_reasons = (
        _canonical_close_projection(
            actual_state,
            positions,
            coordinator_reason_codes,
        )
    )
    if report.outcome != expected_outcome or reasons != expected_reasons:
        raise CanonicalMaterialError(
            "canonical close report contradicts the exact close matrix"
        )


def canonical_material_digest(material: object) -> str:
    """Derive the complete transport digest, excluding its own digest field."""
    from .reports import Report

    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        raise CanonicalMaterialError("canonical material digest type is invalid")
    if type(material.report) is not Report:
        raise CanonicalMaterialError("canonical material report type is invalid")
    source_digest = canonical_source_digest(material.source_receipts)
    if source_digest != material.source_digest:
        raise CanonicalMaterialError(
            "canonical material source digest is inconsistent"
        )
    common: dict[str, object] = {
        "version": 1,
        "kind": material.report.kind,
        "session_date": material.session_date.isoformat(),
        "retrieved_at": _canonical_timestamp(material.retrieved_at),
        "state_hash": material.state_hash,
        "source_digest": source_digest,
        "report": {
            "report_id": material.report.report_id,
            "content_sha256": material.report.content_sha256,
            "outcome": material.report.outcome,
            "observation_ids": list(material.report.observation_ids),
        },
    }
    if type(material) is CanonicalPremarketMaterial:
        common.update(
            {
                "decision_at": _canonical_timestamp(material.decision_at),
                "validation_window_id": material.validation_window_id,
                "snapshot": _canonical_digest_value(material.snapshot),
                "publication_decision_digest": _publication_decision_digest(
                    material.publication_decision
                ),
                "primary_plan_digest": _long_plan_digest(material.primary_plan),
                "composition_authority": _canonical_digest_value(
                    material.composition_authority
                ),
            }
        )
    else:
        common.update(
            {
                "review_at": _canonical_timestamp(material.review_at),
                "query_cutoff": _canonical_timestamp(material.query_cutoff),
                "actual_state_digest": _actual_state_digest(material.actual_state),
                "actual_replay_source_digest": (
                    material.actual_replay_source.source_digest
                ),
                "positions": _canonical_digest_value(material.positions),
                "coordinator_reason_codes": list(
                    material.coordinator_reason_codes
                ),
                "composition_authority": _canonical_digest_value(
                    material.composition_authority
                ),
            }
        )
    return _canonical_sha256(
        "stock-monitor/canonical-workflow-material/v1",
        common,
    )


def _validate_derived_material(material: CanonicalMaterial) -> None:
    expected_source_digest = canonical_source_digest(material.source_receipts)
    reasons = _report_reason_codes(material.report)
    if type(material) is CanonicalPremarketMaterial:
        expected_state_hash = canonical_premarket_state_hash(
            session_date=material.session_date,
            decision_at=material.decision_at,
            retrieved_at=material.retrieved_at,
            snapshot=material.snapshot,
            source_receipts=material.source_receipts,
            publication_decision=material.publication_decision,
            primary_plan=material.primary_plan,
            validation_window_id=material.validation_window_id,
            outcome=material.report.outcome,
            reason_codes=reasons,
            composition_authority=material.composition_authority,
        )
        _validate_premarket_report_projection(
            snapshot=material.snapshot,
            report=material.report,
            retrieved_at=material.retrieved_at,
            composition_authority=material.composition_authority,
        )
    else:
        expected_state_hash = canonical_close_state_hash(
            session_date=material.session_date,
            review_at=material.review_at,
            retrieved_at=material.retrieved_at,
            query_cutoff=material.query_cutoff,
            actual_state=material.actual_state,
            actual_replay_source=material.actual_replay_source,
            positions=material.positions,
            source_receipts=material.source_receipts,
            outcome=material.report.outcome,
            reason_codes=reasons,
            coordinator_reason_codes=material.coordinator_reason_codes,
            review_source_digest=(
                material.composition_authority.review_source_digest
            ),
            position_authority_digests=(
                material.composition_authority.position_authority_digests
            ),
            composition_authority=material.composition_authority,
        )
        _validate_close_report_projection(
            actual_state=material.actual_state,
            positions=material.positions,
            report=material.report,
            retrieved_at=material.retrieved_at,
            composition_authority=material.composition_authority,
            coordinator_reason_codes=material.coordinator_reason_codes,
        )
    _validate_report(
        material.report,
        kind=(
            "PREMARKET"
            if type(material) is CanonicalPremarketMaterial
            else "CLOSE"
        ),
        session_date=material.session_date,
        state_hash=expected_state_hash,
        receipts=material.source_receipts,
    )
    if (
        material.source_digest != expected_source_digest
        or material.state_hash != expected_state_hash
        or material.material_digest != canonical_material_digest(material)
    ):
        raise CanonicalMaterialError(
            "canonical material digest derivation changed during issuance"
        )


def _value_fingerprint(value: object, active: set[int] | None = None) -> object:
    """Return a callback-free structural seal for supported immutable values."""
    value_type = type(value)
    if value is None or value_type in {bool, int, str, bytes}:
        return (value_type.__name__, value)
    if value_type is Decimal:
        decimal = value.as_tuple()
        return ("Decimal", decimal.sign, decimal.digits, decimal.exponent)
    if value_type is date:
        return ("date", value.isoformat())
    if value_type is datetime:
        _require_time(value, "canonical fingerprint time")
        return ("datetime", value.astimezone(UTC).isoformat(timespec="microseconds"))
    if value_type is time:
        return ("time", value.isoformat(timespec="microseconds"), value.fold)
    if value_type is Path or isinstance(value, Path):
        return ("path", os.fspath(value))

    if active is None:
        active = set()
    identity = id(value)
    if identity in active:
        raise CanonicalMaterialError("canonical material contains a cycle")
    if value_type is tuple:
        active.add(identity)
        try:
            return ("tuple", tuple(_value_fingerprint(item, active) for item in value))
        finally:
            active.remove(identity)
    if is_dataclass(value) and not isinstance(value, type):
        active.add(identity)
        try:
            return (
                "dataclass",
                value_type.__module__,
                value_type.__qualname__,
                tuple(
                    (field.name, _value_fingerprint(getattr(value, field.name), active))
                    for field in fields(value)
                ),
            )
        finally:
            active.remove(identity)
    # Opaque child authorities are identity-bound.  Their owning issuer must
    # verify their own current seal before calling the material issuer.
    return ("opaque", value_type.__module__, value_type.__qualname__, identity)


def _material_fingerprint(material: CanonicalMaterial) -> object:
    return _value_fingerprint(material)


@dataclass(frozen=True, slots=True)
class _MaterialAuthority:
    material_reference: ReferenceType[object]
    material_fingerprint: object
    journal_reference: ReferenceType[object]
    journal_generation: int
    archive_root: Path
    receipt_candidates: tuple[object, ...]
    identity_children: tuple[object, ...]
    domain_authority: object
    composition_envelope: object


_ISSUED_CANONICAL_MATERIALS: dict[int, _MaterialAuthority] = {}


def _material_identity_children(material: CanonicalMaterial) -> tuple[object, ...]:
    if type(material) is CanonicalPremarketMaterial:
        return (
            material.snapshot,
            material.report,
            material.source_receipts,
            *material.source_receipts,
            material.publication_decision,
            material.primary_plan,
            material.composition_authority,
        )
    return (
        material.actual_state,
        material.actual_replay_source,
        material.composition_authority,
        material.report,
        material.positions,
        *material.positions,
        material.source_receipts,
        *material.source_receipts,
    )


@dataclass(frozen=True, slots=True)
class _PremarketDomainAuthority:
    decision_record: object
    observation_manifest: object
    plan_source_candidates: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _CloseDomainAuthority:
    replay_candidate: object
    state_candidate: object
    composition_candidate: _CompositionAuthorityCandidate
    semantic_fingerprint: object


def _canonical_archive_root(value: object) -> Path:
    expected_type = type(Path())
    if type(value) is not expected_type:
        raise CanonicalMaterialError(
            "canonical archive root must be an exact pathlib path"
        )
    root = Path(os.path.abspath(value))
    if root != value or root.is_symlink() or not root.is_dir():
        raise CanonicalMaterialError("canonical archive root is unverified")
    return root


def _current_receipt_candidates(
    journal: object,
    receipts: tuple[object, ...],
) -> tuple[object, ...] | None:
    from . import journal as journal_module

    candidates = tuple(
        journal_module._source_observation_receipt_authority_candidate(receipt)
        for receipt in receipts
    )
    if any(candidate is None for candidate in candidates):
        return None
    exact_candidates = tuple(
        candidate for candidate in candidates if candidate is not None
    )
    if (
        journal_module._current_journal_source_authority_owner(exact_candidates)
        is not journal
    ):
        return None
    if any(
        not journal_module._is_current_journal_authority_candidate_without_callbacks(
            candidate
        )
        for candidate in exact_candidates
    ):
        return None
    return exact_candidates


def _premarket_external_source_basis(
    composition_authority: object,
) -> dict[str, str]:
    """Read exact external identities from bound objects, never Journal details."""
    from .providers.alpaca import ProviderFetchPageBundle
    from .providers.cache import SourceDocument

    with _PREMARKET_COMPOSITION_LOCK:
        composition_candidate = _ISSUED_PREMARKET_COMPOSITIONS.get(
            id(composition_authority)
        )
    source_candidate = (
        None
        if composition_candidate is None
        else composition_candidate.source_binding_candidate
    )
    if type(source_candidate) is not _PremarketSourceBindingCandidate:
        raise CanonicalMaterialError(
            "canonical premarket source binding authority is unavailable"
        )
    identifiers: dict[str, str] = {}
    for binding in source_candidate.bindings:
        if type(binding.source) is ProviderFetchPageBundle:
            source_id = binding.source.page.source_observation_id
        elif type(binding.source) is SourceDocument:
            source_id = binding.source.source_observation_id
        else:
            continue
        if source_id in identifiers:
            raise CanonicalMaterialError(
                "canonical premarket external source identity is duplicated"
            )
        identifiers[source_id] = binding.decision_basis
    return identifiers


def _capture_premarket_domain_authority(
    material: CanonicalPremarketMaterial,
    journal: object,
) -> _PremarketDomainAuthority | None:
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "premarket composition authority is unavailable"
        )
    if not material.snapshot.candidates:
        if material.publication_decision is not None or material.primary_plan is not None:
            raise CanonicalMaterialError(
                "no-candidate material cannot carry publication authority"
            )
        return None

    from . import journal as journal_module
    from . import risk as risk_module
    from . import screening as screening_module

    decision = material.publication_decision
    plan = material.primary_plan
    if (
        type(decision) is not screening_module.PublicationDecision
        or type(plan) is not risk_module.LongPlanDecision
        or not risk_module.is_issued_long_plan_decision(plan)
        or not screening_module.is_issued_publication_decision_for_plan(
            decision,
            plan,
        )
    ):
        raise CanonicalMaterialError(
            "candidate material requires its exact issued decision and plan"
        )
    snapshot_roles = tuple(
        (candidate.symbol, candidate.role)
        for candidate in material.snapshot.candidates
    )
    decision_roles = tuple(
        (candidate.symbol, candidate.role) for candidate in decision.candidates
    )
    if (
        snapshot_roles != decision_roles
        or plan.as_of != material.decision_at
        or plan.request is None
        or plan.request.session_date != material.session_date
        or plan.request.symbol != snapshot_roles[0][0]
    ):
        raise CanonicalMaterialError(
            "candidate snapshot, decision, and primary plan are inconsistent"
        )
    observation_manifest = screening_module._publication_observation_manifest(
        decision
    )
    external_source_basis = _premarket_external_source_basis(
        material.composition_authority
    )
    manifest_source_ids = observation_manifest.source_observation_ids
    if any(
        source_id not in external_source_basis
        for source_id in manifest_source_ids
    ):
        raise CanonicalMaterialError(
            "canonical source receipts do not cover the publication manifest"
        )
    for publication in decision.candidates:
        candidate_authority = screening_module._issued_scored_candidate_authority(
            publication.candidate
        )
        if candidate_authority is None:
            raise CanonicalMaterialError(
                "canonical candidate source authority is unavailable"
            )
        context = candidate_authority.context
        operational_ids = {
            context.latest_iex_quote.source_observation_id,
            *context.instrument_status.source_observation_ids,
        }
        if any(
            external_source_basis.get(source_id)
            != (
                "OPERATIONAL_HEALTH_ONLY"
                if source_id in operational_ids
                else "ECONOMIC_INPUT"
            )
            for source_id in candidate_authority.source_observation_ids
        ):
            raise CanonicalMaterialError(
                "canonical source decision basis conflicts with candidate use"
            )
    with screening_module._ISSUED_PUBLICATION_DECISIONS_LOCK:
        decision_record = screening_module._ISSUED_PUBLICATION_DECISIONS.get(
            id(decision)
        )
    if (
        decision_record is None
        or decision_record[0]() is not decision
        or decision_record[2]() is not plan
        or decision_record[3] is not observation_manifest
    ):
        raise CanonicalMaterialError(
            "publication decision registry identity is unverified"
        )

    portfolio = plan.portfolio_authority
    bound_sources = risk_module._phase1_bound_sources(portfolio)
    if not bound_sources:
        raise CanonicalMaterialError(
            "canonical primary plan lacks Journal source lineage"
        )
    source_candidates = tuple(
        journal_module._journal_any_source_authority_candidate(source)
        for source, _source_kind in bound_sources
    )
    if any(candidate is None for candidate in source_candidates):
        raise CanonicalMaterialError(
            "canonical primary plan source lineage is unverified"
        )
    exact_candidates = tuple(
        candidate for candidate in source_candidates if candidate is not None
    )
    if (
        journal_module._current_journal_source_authority_owner(exact_candidates)
        is not journal
    ):
        raise CanonicalMaterialError(
            "canonical primary plan and material cross Journal owners"
        )
    authority = _PremarketDomainAuthority(
        decision_record=decision_record,
        observation_manifest=observation_manifest,
        plan_source_candidates=exact_candidates,
    )
    if not _premarket_domain_is_current_without_callbacks(material, authority):
        raise CanonicalMaterialError(
            "canonical publication authority changed during issuance"
        )
    return authority


def _premarket_domain_is_current_without_callbacks(
    material: CanonicalPremarketMaterial,
    authority: _PremarketDomainAuthority | None,
) -> bool:
    if not material.snapshot.candidates:
        return bool(
            authority is None
            and material.publication_decision is None
            and material.primary_plan is None
        )
    if authority is None:
        return False
    from . import journal as journal_module
    from . import risk as risk_module
    from . import screening as screening_module

    decision = material.publication_decision
    plan = material.primary_plan
    if (
        type(decision) is not screening_module.PublicationDecision
        or type(plan) is not risk_module.LongPlanDecision
        or authority.decision_record[0]() is not decision
        or authority.decision_record[2]() is not plan
        or authority.decision_record[3] is not authority.observation_manifest
        or plan.portfolio_authority is None
        or not risk_module._is_current_portfolio_risk_authority_without_callbacks(
            plan.portfolio_authority
        )
        or not risk_module._is_current_risk_authority_without_callbacks(
            risk_module._LONG_PLAN_AUTHORITIES,
            plan,
            exact_type=risk_module.LongPlanDecision,
            children=(plan.portfolio_authority,),
        )
        or any(
            not journal_module._is_current_journal_authority_candidate_without_callbacks(
                candidate
            )
            for candidate in authority.plan_source_candidates
        )
    ):
        return False
    try:
        fingerprint = screening_module._publication_decision_fingerprint(decision)
    except Exception:
        return False
    with screening_module._ISSUED_PUBLICATION_DECISIONS_LOCK:
        current = screening_module._ISSUED_PUBLICATION_DECISIONS.get(id(decision))
        return bool(
            current is authority.decision_record
            and current[0]() is decision
            and current[1] == fingerprint
            and current[2]() is plan
            and current[3] is authority.observation_manifest
        )


def _is_issued_close_position_authority_bundle(
    bundle: object,
    *,
    review_source: object,
    actual_state: object | None = None,
    actual_replay_source: object | None = None,
    journal: object | None = None,
) -> bool:
    """Rederive one exact coordinator-issued position projection."""
    from . import journal as journal_module
    from . import risk as risk_module
    from .reconciliation import ActualLedgerState, ActualPositionState
    from .reports import ClosePosition, UnverifiedClosePosition

    if type(bundle) is not _ClosePositionAuthorityBundle:
        return False
    with _CLOSE_POSITION_AUTHORITY_LOCK:
        candidate = _ISSUED_CLOSE_POSITION_AUTHORITIES.get(id(bundle))
    if (
        type(candidate) is not _ClosePositionAuthorityCandidate
        or candidate.bundle_reference() is not bundle
        or candidate.coordinator_reference() is None
        or candidate.journal_reference() is None
    ):
        return False
    candidate_journal = candidate.journal_reference()
    exact_state = candidate.actual_state if actual_state is None else actual_state
    exact_replay = (
        candidate.actual_replay_source
        if actual_replay_source is None
        else actual_replay_source
    )
    exact_journal = candidate_journal if journal is None else journal
    if (
        exact_journal is not candidate_journal
        or exact_state is not candidate.actual_state
        or exact_replay is not candidate.actual_replay_source
        or type(exact_state) is not ActualLedgerState
        or type(review_source) is not journal_module.ActualCloseReviewSource
        or getattr(candidate_journal, "_source_generation", None)
        != candidate.journal_generation
        or not candidate_journal.owns_actual_close_review_source(review_source)
        or not journal_module.is_verified_actual_close_review_source(
            review_source
        )
        or bundle.projection is not candidate.projection
        or bundle.identity_children is not candidate.identity_children
        or type(bundle.identity_children) is not tuple
        or len(bundle.identity_children) < 2
        or type(bundle.identity_children[0]) is not ActualPositionState
        or type(bundle.identity_children[1])
        is not journal_module.ActualPositionPlanResolution
        or bundle.projection.symbol != bundle.identity_children[0].symbol
        or bundle.source_digest
        != _close_position_authority_digest(
            projection=bundle.projection,
            branch=bundle.branch,
            identity_children=bundle.identity_children,
        )
    ):
        return False
    actual_position = bundle.identity_children[0]
    resolution = bundle.identity_children[1]
    if (
        not any(position is actual_position for position in exact_state.positions)
        or getattr(exact_replay, "query_cutoff", None) != candidate.query_cutoff
        or getattr(exact_state, "query_cutoff", None) != candidate.query_cutoff
        or review_source.query_cutoff != candidate.query_cutoff
    ):
        return False

    # Exhaust callback-bearing child authority validation before the final
    # registry/fingerprint seal below.
    for child in bundle.identity_children[2:]:
        child_type = type(child)
        if child_type is journal_module.ActualPositionPlanSource:
            current = journal_module.is_verified_actual_position_plan_source(
                child
            )
        elif child_type is journal_module.ActualCloseReviewSource:
            current = (
                child is review_source
                and journal_module.is_verified_actual_close_review_source(child)
            )
        elif child_type is journal_module.Phase1SignalEvidenceSource:
            current = journal_module.is_verified_phase1_signal_evidence_source(
                child
            )
        elif child_type is journal_module.LatestCloseRecommendationSource:
            current = (
                journal_module.is_verified_latest_close_recommendation_source(
                    child
                )
            )
        elif child_type is risk_module.Phase1SignalEvidenceAuthority:
            current = risk_module.is_issued_phase1_signal_evidence_authority(
                child
            )
        elif child_type is risk_module.ActualCloseMarketSource:
            current = risk_module.is_issued_actual_close_market_source(child)
        elif child_type is risk_module.ActualPositionEventContext:
            current = risk_module.is_issued_actual_position_event_context(child)
        elif child_type is risk_module.MarketMark:
            current = risk_module.is_issued_market_mark(child)
        elif child_type is risk_module.PositionAction:
            current = True
        elif child_type is risk_module.ActualCloseDecisionSource:
            current = risk_module.is_issued_actual_close_decision_source(child)
        else:
            return False
        if not current:
            return False

    reconciliation_reasons = _actual_close_reconciliation_reasons(exact_state)
    status_for_block = (
        "RECONCILIATION_REQUIRED"
        if reconciliation_reasons
        else "POSITION_UNVERIFIED"
    )

    def exact_unverified(status: str, reasons: tuple[str, ...]) -> bool:
        try:
            expected = _actual_close_unverified_projection(
                actual_position,
                status=status,
                reason_codes=reasons,
            )
        except Exception:
            return False
        return type(bundle.projection) is UnverifiedClosePosition and (
            bundle.projection == expected
        )

    children = bundle.identity_children
    branch_valid = False
    if bundle.branch == "PLAN_UNAVAILABLE":
        try:
            current_resolution = (
                candidate_journal.resolve_actual_position_plan_source(
                    actual_replay_source=exact_replay,
                    actual_position_state=exact_state,
                    symbol=actual_position.symbol,
                    query_cutoff=candidate.query_cutoff,
                )
            )
        except Exception:
            current_resolution = None
        branch_valid = bool(
            len(children) == 2
            and resolution.status != "RESOLVED"
            and current_resolution == resolution
            and exact_unverified(
                status_for_block,
                (*reconciliation_reasons, *resolution.reason_codes),
            )
        )
    else:
        if (
            resolution.status != "RESOLVED"
            or resolution.source is None
            or len(children) < 4
            or children[2] is not resolution.source
            or children[3] is not review_source
        ):
            return False
        plan_source = children[2]
        if (
            plan_source.actual_replay_source is not exact_replay
            or plan_source.actual_position_state is not exact_state
            or plan_source.query_cutoff != candidate.query_cutoff
            or plan_source.symbol != actual_position.symbol
        ):
            return False

        review_failure_reasons = _actual_close_review_failure_reasons(
            review_source,
            actual_position.symbol,
        )
        if bundle.branch == "REVIEW_SOURCE_UNAVAILABLE":
            status = (
                "RECONCILIATION_REQUIRED"
                if reconciliation_reasons
                else (
                    "POSITION_UNVERIFIED"
                    if "EVENT_EVIDENCE_UNAVAILABLE"
                    in review_failure_reasons
                    else "DATA_UNAVAILABLE"
                )
            )
            branch_valid = bool(
                len(children) == 4
                and review_failure_reasons
                and exact_unverified(
                    status,
                    (*reconciliation_reasons, *review_failure_reasons),
                )
            )
        elif bundle.branch == "EVENT_EVIDENCE_UNAVAILABLE":
            failed = False
            try:
                candidate_journal.read_phase1_signal_evidence_source(
                    plan_source.signal_source.signal_id,
                    review_at=review_source.review_at,
                    query_cutoff=candidate.query_cutoff,
                    calendar_resolver=candidate.calendar_resolver,
                    exact_signal_source=plan_source.signal_source,
                )
            except (journal_module.JournalError, risk_module.RiskBlock):
                failed = True
            branch_valid = bool(
                len(children) == 4
                and not review_failure_reasons
                and failed
                and exact_unverified(
                    status_for_block,
                    (*reconciliation_reasons, "EVENT_EVIDENCE_UNAVAILABLE"),
                )
            )
        elif bundle.branch == "EVENT_AUTHORITY_UNAVAILABLE":
            if len(children) != 5:
                return False
            evidence_source = children[4]
            if (
                type(evidence_source)
                is not journal_module.Phase1SignalEvidenceSource
                or evidence_source.signal_source is not plan_source.signal_source
            ):
                return False
            failed = False
            try:
                risk_module._issue_phase1_signal_evidence_authority_from_source(
                    evidence_source,
                    calendar_resolver=candidate.calendar_resolver,
                )
            except (journal_module.JournalError, risk_module.RiskBlock):
                failed = True
            branch_valid = bool(
                not review_failure_reasons
                and failed
                and exact_unverified(
                    status_for_block,
                    (*reconciliation_reasons, "EVENT_EVIDENCE_UNAVAILABLE"),
                )
            )
        else:
            if len(children) < 6:
                return False
            evidence_source = children[4]
            event_evidence = children[5]
            if (
                type(evidence_source)
                is not journal_module.Phase1SignalEvidenceSource
                or type(event_evidence)
                is not risk_module.Phase1SignalEvidenceAuthority
                or evidence_source.signal_source is not plan_source.signal_source
                or not any(
                    bound_source is evidence_source
                    for bound_source, _kind in risk_module._phase1_bound_sources(
                        event_evidence
                    )
                )
            ):
                return False
            if bundle.branch == "RECOMMENDATION_HISTORY_UNAVAILABLE":
                failed = False
                try:
                    candidate_journal.read_latest_close_recommendation_source(
                        position_plan_source=plan_source,
                        query_cutoff=candidate.query_cutoff,
                    )
                except (journal_module.JournalError, risk_module.RiskBlock):
                    failed = True
                branch_valid = bool(
                    len(children) == 6
                    and failed
                    and exact_unverified(
                        status_for_block,
                        (
                            *reconciliation_reasons,
                            "RECOMMENDATION_HISTORY_UNAVAILABLE",
                        ),
                    )
                )
            else:
                if len(children) < 7:
                    return False
                history_source = children[6]
                if (
                    type(history_source)
                    is not journal_module.LatestCloseRecommendationSource
                    or history_source.position_plan_source is not plan_source
                ):
                    return False
                if bundle.branch == "MARKET_SOURCE_UNAVAILABLE":
                    market_error = None
                    try:
                        risk_module.issue_actual_close_market_source(
                            review_source,
                            plan_source,
                            calendar_resolver=candidate.calendar_resolver,
                        )
                    except risk_module.RiskBlock as error:
                        market_error = error
                    reason = (
                        None
                        if market_error is None
                        else _actual_close_market_failure_reason(
                            review_source,
                            actual_position.symbol,
                            market_error,
                        )
                    )
                    branch_valid = bool(
                        len(children) == 7
                        and reason is not None
                        and exact_unverified(
                            (
                                "RECONCILIATION_REQUIRED"
                                if reconciliation_reasons
                                else "DATA_UNAVAILABLE"
                            ),
                            (*reconciliation_reasons, reason),
                        )
                    )
                else:
                    if len(children) < 8:
                        return False
                    market_source = children[7]
                    if (
                        type(market_source)
                        is not risk_module.ActualCloseMarketSource
                        or market_source.review_source is not review_source
                        or market_source.position_plan_source is not plan_source
                    ):
                        return False
                    if bundle.branch == "POSITION_CONTEXT_UNVERIFIED":
                        failed = False
                        try:
                            risk_module.issue_actual_position_event_context(
                                market_source,
                                plan_source,
                                event_evidence,
                                history_source,
                                calendar_resolver=candidate.calendar_resolver,
                                policy=candidate.policy,
                            )
                        except risk_module.RiskBlock:
                            failed = True
                        branch_valid = bool(
                            len(children) == 8
                            and failed
                            and exact_unverified(
                                status_for_block,
                                (
                                    *reconciliation_reasons,
                                    "POSITION_CONTEXT_UNVERIFIED",
                                ),
                            )
                        )
                    elif bundle.branch == "CLOSE_MARK_UNVERIFIED":
                        if len(children) != 9:
                            return False
                        context = children[8]
                        if (
                            type(context)
                            is not risk_module.ActualPositionEventContext
                            or context.review_source is not review_source
                            or context.position_plan_source is not plan_source
                            or context.latest_recommendation_source
                            is not history_source
                        ):
                            return False
                        failed = False
                        try:
                            risk_module.issue_actual_close_mark(
                                market_source,
                                context,
                            )
                        except risk_module.RiskBlock:
                            failed = True
                        branch_valid = bool(
                            failed
                            and exact_unverified(
                                status_for_block,
                                (
                                    *reconciliation_reasons,
                                    "POSITION_CONTEXT_UNVERIFIED",
                                ),
                            )
                        )
                    elif bundle.branch == "POSITION_EVALUATION_UNVERIFIED":
                        if len(children) != 10:
                            return False
                        context, mark = children[8:10]
                        if (
                            type(context)
                            is not risk_module.ActualPositionEventContext
                            or type(mark) is not risk_module.MarketMark
                            or context.review_source is not review_source
                            or context.position_plan_source is not plan_source
                            or context.latest_recommendation_source
                            is not history_source
                            or risk_module._registered_risk_authority_children(
                                risk_module._MARK_AUTHORITIES,
                                mark,
                            )
                            != (context,)
                        ):
                            return False
                        try:
                            expected_mark = risk_module.issue_actual_close_mark(
                                market_source,
                                context,
                            )
                        except risk_module.RiskBlock:
                            return False
                        if expected_mark != mark:
                            return False
                        failed = False
                        try:
                            risk_module.evaluate_position(
                                context.position,
                                mark,
                                candidate.policy,
                            )
                        except risk_module.RiskBlock:
                            failed = True
                        branch_valid = bool(
                            failed
                            and exact_unverified(
                                status_for_block,
                                (
                                    *reconciliation_reasons,
                                    "POSITION_CONTEXT_UNVERIFIED",
                                ),
                            )
                        )
                    else:
                        if len(children) not in {11, 12}:
                            return False
                        context, mark, action = children[8:11]
                        if (
                            type(context)
                            is not risk_module.ActualPositionEventContext
                            or type(mark) is not risk_module.MarketMark
                            or type(action) is not risk_module.PositionAction
                            or context.review_source is not review_source
                            or context.position_plan_source is not plan_source
                            or context.latest_recommendation_source
                            is not history_source
                        ):
                            return False
                        try:
                            expected_action = risk_module.evaluate_position(
                                context.position,
                                mark,
                                candidate.policy,
                            )
                        except risk_module.RiskBlock:
                            return False
                        if expected_action != action:
                            return False
                        if bundle.branch == "RECONCILIATION_REQUIRED":
                            branch_valid = bool(
                                len(children) == 11
                                and reconciliation_reasons
                                and exact_unverified(
                                    "RECONCILIATION_REQUIRED",
                                    reconciliation_reasons,
                                )
                            )
                        elif bundle.branch in {
                            "POSITION_UNVERIFIED",
                            "RECONCILIATION_REQUIRED",
                        }:
                            branch_valid = bool(
                                len(children) == 11
                                and action.status == bundle.branch
                                and exact_unverified(
                                    action.status,
                                    tuple(action.reason_codes),
                                )
                            )
                        elif bundle.branch == "DECISION_AUTHORITY_UNAVAILABLE":
                            failed = False
                            try:
                                risk_module.issue_actual_close_decision(
                                    context,
                                    mark,
                                    candidate.policy,
                                )
                            except risk_module.RiskBlock:
                                failed = True
                            branch_valid = bool(
                                len(children) == 11
                                and failed
                                and exact_unverified(
                                    "POSITION_UNVERIFIED",
                                    ("POSITION_CONTEXT_UNVERIFIED",),
                                )
                            )
                        elif bundle.branch == "STOP_UNVERIFIED":
                            try:
                                expected_projection = (
                                    _actual_close_verified_projection(
                                        context=context,
                                        market_source=market_source,
                                        mark=mark,
                                        action=action,
                                        decision=None,
                                    )
                                )
                            except Exception:
                                expected_projection = None
                            branch_valid = bool(
                                len(children) == 11
                                and action.status == "STOP_UNVERIFIED"
                                and type(bundle.projection) is ClosePosition
                                and bundle.projection == expected_projection
                            )
                        elif bundle.branch == "VERIFIED_DECISION":
                            if len(children) != 12:
                                return False
                            decision = children[11]
                            try:
                                expected_projection = (
                                    _actual_close_verified_projection(
                                        context=context,
                                        market_source=market_source,
                                        mark=mark,
                                        action=action,
                                        decision=decision,
                                    )
                                )
                            except Exception:
                                expected_projection = None
                            branch_valid = bool(
                                type(decision)
                                is risk_module.ActualCloseDecisionSource
                                and decision.context is context
                                and decision.mark is mark
                                and decision.policy is candidate.policy
                                and decision.position_action is action
                                and decision.review_source is review_source
                                and decision.position_plan_source is plan_source
                                and decision.latest_recommendation_source
                                is history_source
                                and type(bundle.projection) is ClosePosition
                                and bundle.projection == expected_projection
                            )

    if not branch_valid:
        return False
    try:
        fingerprint = _value_fingerprint(bundle)
    except Exception:
        return False
    with _CLOSE_POSITION_AUTHORITY_LOCK:
        current = _ISSUED_CLOSE_POSITION_AUTHORITIES.get(id(bundle))
        return bool(
            current is candidate
            and candidate.bundle_reference() is bundle
            and candidate.journal_reference() is candidate_journal
            and getattr(candidate_journal, "_source_generation", None)
            == candidate.journal_generation
            and candidate.projection is bundle.projection
            and candidate.identity_children is bundle.identity_children
            and candidate.bundle_fingerprint == fingerprint
        )


def _capture_close_domain_authority(
    material: CanonicalCloseMaterial,
    journal: object,
) -> _CloseDomainAuthority:
    from . import journal as journal_module
    from . import reconciliation as reconciliation_module

    state = material.actual_state
    replay_source = material.actual_replay_source
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "close composition authority is unavailable"
        )
    if not reconciliation_module.is_verified_actual_ledger_state_for_source(
        state,
        replay_source,
    ):
        raise CanonicalMaterialError(
            "close material actual replay authority is unverified"
        )
    with _CLOSE_COMPOSITION_LOCK:
        composition_candidate = _ISSUED_CLOSE_COMPOSITIONS.get(
            id(material.composition_authority)
        )
    requires_semantic_sources = (
        material.composition_authority.review_source_digest is not None
    )
    if (
        type(composition_candidate) is not _CompositionAuthorityCandidate
        or composition_candidate.authority_reference()
        is not material.composition_authority
        or (
            requires_semantic_sources
            and len(composition_candidate.semantic_children)
            != len(material.positions) + 1
        )
    ):
        raise CanonicalMaterialError(
            "close material exact source chain is unavailable"
        )
    if requires_semantic_sources:
        review_source = composition_candidate.semantic_children[0]
        position_authorities = composition_candidate.semantic_children[1:]
        if (
            not journal.owns_actual_close_review_source(review_source)
            or review_source.source_digest
            != material.composition_authority.review_source_digest
            or any(
                not _is_issued_close_position_authority_bundle(
                    bundle,
                    review_source=review_source,
                    actual_state=state,
                    actual_replay_source=replay_source,
                    journal=journal,
                )
                for bundle in position_authorities
            )
        ):
            raise CanonicalMaterialError(
                "close material exact source chain is unverified"
            )
    replay_candidate = (
        journal_module._journal_replay_source_authority_candidate(replay_source)
    )
    if replay_candidate is None or (
        journal_module._current_journal_source_authority_owner((replay_candidate,))
        is not journal
    ):
        raise CanonicalMaterialError(
            "close material actual replay has the wrong Journal owner"
        )
    state_candidate = reconciliation_module._actual_ledger_state_authority_candidate(
        state,
        replay_source,
    )
    authority = _CloseDomainAuthority(
        replay_candidate=replay_candidate,
        state_candidate=state_candidate,
        composition_candidate=composition_candidate,
        semantic_fingerprint=_close_semantic_fingerprint(
            composition_candidate.semantic_children
        ),
    )
    if state_candidate is None or not _close_domain_is_current_without_callbacks(
        material,
        authority,
    ):
        raise CanonicalMaterialError(
            "close material actual replay changed during issuance"
        )
    return authority


def _close_domain_is_current_without_callbacks(
    material: CanonicalCloseMaterial,
    authority: _CloseDomainAuthority,
) -> bool:
    from . import journal as journal_module
    from . import reconciliation as reconciliation_module

    try:
        semantic_fingerprint = _close_semantic_fingerprint(
            authority.composition_candidate.semantic_children
        )
    except Exception:
        return False
    with _CLOSE_COMPOSITION_LOCK:
        current_composition = _ISSUED_CLOSE_COMPOSITIONS.get(
            id(material.composition_authority)
        )
    return bool(
        authority.replay_candidate[1] is material.actual_replay_source
        and authority.state_candidate[0] is material.actual_state
        and authority.state_candidate[1] is material.actual_replay_source
        and journal_module._is_current_journal_authority_candidate_without_callbacks(
            authority.replay_candidate
        )
        and reconciliation_module._is_current_actual_ledger_state_authority_candidate_without_callbacks(
            authority.state_candidate
        )
        and authority.composition_candidate.authority_reference()
        is material.composition_authority
        and current_composition is authority.composition_candidate
        and (
            authority.composition_candidate.semantic_fingerprint is None
            or authority.composition_candidate.semantic_fingerprint
            == authority.semantic_fingerprint
        )
        and semantic_fingerprint == authority.semantic_fingerprint
    )


def _capture_domain_authority(
    material: CanonicalMaterial,
    journal: object,
) -> object:
    if type(material) is CanonicalPremarketMaterial:
        return _capture_premarket_domain_authority(material, journal)
    return _capture_close_domain_authority(material, journal)


def _domain_is_current_without_callbacks(
    material: CanonicalMaterial,
    authority: object,
) -> bool:
    if type(material) is CanonicalPremarketMaterial:
        if authority is not None and type(authority) is not _PremarketDomainAuthority:
            return False
        return _premarket_domain_is_current_without_callbacks(material, authority)
    return type(authority) is _CloseDomainAuthority and (
        _close_domain_is_current_without_callbacks(material, authority)
    )


def _register_material(
    material: CanonicalMaterial,
    *,
    journal: object,
    report_archive_root: Path,
) -> CanonicalMaterial:
    from .journal import Journal

    if type(journal) is not Journal or getattr(journal, "_closed", True):
        raise CanonicalMaterialError("canonical material requires an open Journal")
    root = _canonical_archive_root(report_archive_root)
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None:
        raise CanonicalMaterialError(
            "canonical source receipts lack one current Journal owner"
        )
    domain_authority = _capture_domain_authority(material, journal)
    # Domain verification may execute source-currentness callbacks.  Refresh
    # the receipt batch before entering the callback-free final seal.
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None:
        raise CanonicalMaterialError(
            "canonical source receipts changed during domain verification"
        )
    # Domain verification above exhausts Journal/source callbacks.  Recompute
    # every semantic digest and report projection before the final structural
    # seal so no mutation during those callbacks can be blessed.
    _validate_derived_material(material)
    composition_envelope = _composition_envelope_for_material(material)
    if not _composition_is_current_without_callbacks(
        material,
        composition_envelope,
    ):
        raise CanonicalMaterialError(
            "canonical composition authority changed during issuance"
        )
    fingerprint = _material_fingerprint(material)
    generation = getattr(journal, "_source_generation", None)
    if type(generation) is not int or generation < 0:
        raise CanonicalMaterialError("canonical Journal generation is invalid")
    identity = id(material)

    def discard(dead: ReferenceType[object]) -> None:
        with _MATERIAL_AUTHORITY_LOCK:
            current = _ISSUED_CANONICAL_MATERIALS.get(identity)
            if current is not None and current.material_reference is dead:
                _ISSUED_CANONICAL_MATERIALS.pop(identity, None)

    authority = _MaterialAuthority(
        ref(material, discard),
        fingerprint,
        ref(journal),
        generation,
        root,
        candidates,
        _material_identity_children(material),
        domain_authority,
        composition_envelope,
    )
    with _MATERIAL_AUTHORITY_LOCK:
        _ISSUED_CANONICAL_MATERIALS[identity] = authority
    if not _is_current_canonical_material_without_callbacks(
        material,
        authority=authority,
    ):
        with _MATERIAL_AUTHORITY_LOCK:
            if _ISSUED_CANONICAL_MATERIALS.get(identity) is authority:
                _ISSUED_CANONICAL_MATERIALS.pop(identity, None)
        raise CanonicalMaterialError(
            "canonical material changed while its authority was issued"
        )
    return material


def issue_canonical_premarket_material(
    *,
    journal: object,
    report_archive_root: Path,
    session_date: date,
    decision_at: datetime,
    retrieved_at: datetime,
    snapshot: object,
    report: object,
    source_receipts: tuple[object, ...],
    publication_decision: object | None,
    primary_plan: object | None,
    validation_window_id: str,
    composition_authority: object | None = None,
    state_hash: str | None = None,
    source_digest: str | None = None,
    material_digest: str | None = None,
) -> CanonicalPremarketMaterial:
    """Issue one exact owner-bound premarket material capability."""
    receipts = _receipt_set(source_receipts)
    expected_source_digest = canonical_source_digest(receipts)
    report_reasons = _report_reason_codes(report)
    expected_state_hash = canonical_premarket_state_hash(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation_window_id,
        outcome=report.outcome,
        reason_codes=report_reasons,
        composition_authority=composition_authority,
    )
    for supplied, expected, label in (
        (state_hash, expected_state_hash, "canonical state digest"),
        (source_digest, expected_source_digest, "canonical source digest"),
    ):
        if supplied is not None and (
            _require_digest(supplied, label) != expected
        ):
            raise CanonicalMaterialError(f"{label} conflicts with derived digest")
    _validate_report(
        report,
        kind="PREMARKET",
        session_date=session_date,
        state_hash=expected_state_hash,
        receipts=receipts,
    )
    _validate_premarket_report_projection(
        snapshot=snapshot,
        report=report,
        retrieved_at=retrieved_at,
        composition_authority=composition_authority,
    )
    provisional = CanonicalPremarketMaterial(
        session_date=session_date,
        decision_at=decision_at,
        retrieved_at=retrieved_at,
        snapshot=snapshot,
        report=report,
        source_receipts=receipts,
        publication_decision=publication_decision,
        primary_plan=primary_plan,
        validation_window_id=validation_window_id,
        state_hash=expected_state_hash,
        source_digest=expected_source_digest,
        material_digest="0" * 64,
        composition_authority=composition_authority,
    )
    expected_material_digest = canonical_material_digest(provisional)
    if material_digest is not None and (
        _require_digest(material_digest, "canonical material digest")
        != expected_material_digest
    ):
        raise CanonicalMaterialError(
            "canonical material digest conflicts with derived digest"
        )
    material = replace(
        provisional,
        material_digest=expected_material_digest,
    )
    return _register_material(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )


def issue_canonical_close_material(
    *,
    journal: object,
    report_archive_root: Path,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    composition_authority: object | None = None,
    report: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    coordinator_reason_codes: tuple[str, ...] = (),
    state_hash: str | None = None,
    source_digest: str | None = None,
    material_digest: str | None = None,
) -> CanonicalCloseMaterial:
    """Issue one exact owner-bound actual-close material capability."""
    receipts = _receipt_set(source_receipts)
    expected_source_digest = canonical_source_digest(receipts)
    report_reasons = _report_reason_codes(report)
    review_source_digest = (
        composition_authority.review_source_digest
        if type(composition_authority)
        is CanonicalCloseCompositionAuthority
        else None
    )
    position_authority_digests = (
        composition_authority.position_authority_digests
        if type(composition_authority)
        is CanonicalCloseCompositionAuthority
        else ()
    )
    expected_state_hash = canonical_close_state_hash(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
        outcome=report.outcome,
        reason_codes=report_reasons,
        coordinator_reason_codes=coordinator_reason_codes,
        review_source_digest=review_source_digest,
        position_authority_digests=position_authority_digests,
        composition_authority=composition_authority,
    )
    for supplied, expected, label in (
        (state_hash, expected_state_hash, "canonical state digest"),
        (source_digest, expected_source_digest, "canonical source digest"),
    ):
        if supplied is not None and (
            _require_digest(supplied, label) != expected
        ):
            raise CanonicalMaterialError(f"{label} conflicts with derived digest")
    _validate_report(
        report,
        kind="CLOSE",
        session_date=session_date,
        state_hash=expected_state_hash,
        receipts=receipts,
    )
    _validate_close_report_projection(
        actual_state=actual_state,
        positions=positions,
        report=report,
        retrieved_at=retrieved_at,
        composition_authority=composition_authority,
        coordinator_reason_codes=coordinator_reason_codes,
    )
    provisional = CanonicalCloseMaterial(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        composition_authority=composition_authority,
        report=report,
        positions=positions,
        source_receipts=receipts,
        state_hash=expected_state_hash,
        source_digest=expected_source_digest,
        material_digest="0" * 64,
        coordinator_reason_codes=coordinator_reason_codes,
    )
    expected_material_digest = canonical_material_digest(provisional)
    if material_digest is not None and (
        _require_digest(material_digest, "canonical material digest")
        != expected_material_digest
    ):
        raise CanonicalMaterialError(
            "canonical material digest conflicts with derived digest"
        )
    material = replace(
        provisional,
        material_digest=expected_material_digest,
    )
    return _register_material(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )


def _issue_close_composition_authority(
    *,
    session_date: date,
    review_at: datetime,
    retrieved_at: datetime,
    query_cutoff: datetime,
    actual_state: object,
    actual_replay_source: object,
    positions: tuple[object, ...],
    source_receipts: tuple[object, ...],
    review_source: object,
    position_authorities: tuple[_ClosePositionAuthorityBundle, ...],
    outcome: str,
    reason_codes: tuple[str, ...],
    coordinator_reason_codes: tuple[str, ...] = (),
) -> CanonicalCloseCompositionAuthority:
    """Register one exact coordinator-only close composition capability."""
    from .journal import (
        ActualCloseReviewSource,
        is_verified_actual_close_review_source,
    )

    if (
        type(review_source) is not ActualCloseReviewSource
        or not is_verified_actual_close_review_source(review_source)
        or review_source.session_date != session_date
        or review_source.review_at != review_at
        or review_source.query_cutoff != query_cutoff
        or review_source.retrieved_at != retrieved_at
        or type(position_authorities) is not tuple
        or len(position_authorities) != len(positions)
        or len(actual_state.positions) != len(position_authorities)
        or any(
            type(bundle) is not _ClosePositionAuthorityBundle
            or bundle.projection is not position
            or bundle.identity_children[0] is not actual_position
            or not _is_issued_close_position_authority_bundle(
                bundle,
                review_source=review_source,
                actual_state=actual_state,
                actual_replay_source=actual_replay_source,
            )
            for bundle, position, actual_position in zip(
                position_authorities,
                positions,
                actual_state.positions,
                strict=True,
            )
        )
    ):
        raise CanonicalMaterialError(
            "close composition source authority chain is invalid"
        )
    receipts = _receipt_set(source_receipts)
    if (
        len(receipts) != len(review_source.receipts)
        or {id(receipt) for receipt in receipts}
        != {id(receipt) for receipt in review_source.receipts}
    ):
        raise CanonicalMaterialError(
            "close composition review receipts are not exact"
        )
    position_authority_digests = tuple(
        bundle.source_digest for bundle in position_authorities
    )
    envelope = _close_composition_envelope(
        session_date=session_date,
        review_at=review_at,
        retrieved_at=retrieved_at,
        query_cutoff=query_cutoff,
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
        outcome=outcome,
        reason_codes=reason_codes,
        coordinator_reason_codes=coordinator_reason_codes,
        review_source_digest=review_source.source_digest,
        position_authority_digests=position_authority_digests,
    )
    authority = CanonicalCloseCompositionAuthority(
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
        coordinator_reason_codes=envelope.coordinator_reason_codes,
        review_source_digest=envelope.review_source_digest,
        position_authority_digests=envelope.position_authority_digests,
    )
    children = _close_composition_identity_children(
        actual_state=actual_state,
        actual_replay_source=actual_replay_source,
        positions=positions,
        source_receipts=receipts,
    )
    identity = id(authority)

    def discard(dead: ReferenceType[object]) -> None:
        with _CLOSE_COMPOSITION_LOCK:
            current = _ISSUED_CLOSE_COMPOSITIONS.get(identity)
            if current is not None and current.authority_reference is dead:
                _ISSUED_CLOSE_COMPOSITIONS.pop(identity, None)

    candidate = _CompositionAuthorityCandidate(
        authority_reference=ref(authority, discard),
        authority_fingerprint=_value_fingerprint(authority),
        envelope=envelope,
        identity_children=children,
        semantic_children=(review_source, *position_authorities),
        semantic_fingerprint=_close_semantic_fingerprint(
            (review_source, *position_authorities)
        ),
    )
    with _CLOSE_COMPOSITION_LOCK:
        if _ISSUED_CLOSE_COMPOSITIONS.get(identity) is not None:
            raise CanonicalMaterialError(
                "close composition authority identity is already registered"
            )
        _ISSUED_CLOSE_COMPOSITIONS[identity] = candidate
    if not _is_issued_close_composition_authority(
        authority,
        envelope=envelope,
        identity_children=children,
    ):
        with _CLOSE_COMPOSITION_LOCK:
            if _ISSUED_CLOSE_COMPOSITIONS.get(identity) is candidate:
                _ISSUED_CLOSE_COMPOSITIONS.pop(identity, None)
        raise CanonicalMaterialError(
            "close composition authority changed during issuance"
        )
    return authority


def _actual_close_unverified_projection(
    actual_position: object,
    *,
    status: str,
    reason_codes: tuple[str, ...],
) -> object:
    from .domain import money_from_micros
    from .reconciliation import ActualPositionState
    from .reports import UnverifiedClosePosition

    if type(actual_position) is not ActualPositionState:
        raise CanonicalMaterialError(
            "actual close projection requires an exact position"
        )
    reasons = tuple(dict.fromkeys(reason_codes))
    _validate_reason_tuple(reasons, "actual close position reasons")
    return UnverifiedClosePosition(
        symbol=actual_position.symbol,
        shares=actual_position.shares,
        exact_cost_basis=money_from_micros(actual_position.cost_basis_micros),
        status=status,
        reason_codes=reasons,
    )


def _actual_close_reconciliation_reasons(actual_state: object) -> tuple[str, ...]:
    from .reconciliation import ActualLedgerState

    if type(actual_state) is not ActualLedgerState or type(
        actual_state.reconciliation_reasons
    ) is not tuple:
        raise CanonicalMaterialError(
            "actual close reconciliation state is invalid"
        )
    return tuple(
        reason
        for reason in actual_state.reconciliation_reasons
        if reason not in _LEGACY_ACTUAL_ENTRY_CONTEXT_REASONS
    )


def _actual_close_report_sources(market_source: object) -> tuple[object, ...]:
    from .journal import SourceObservationReceipt
    from .reports import ReportSource
    from .risk import ActualCloseMarketSource

    if type(market_source) is not ActualCloseMarketSource:
        raise CanonicalMaterialError("actual close market source is invalid")
    sources: list[ReportSource] = []
    seen: set[str] = set()
    for receipt in market_source.observation_receipts:
        if type(receipt) is not SourceObservationReceipt:
            raise CanonicalMaterialError(
                "actual close market receipt is invalid"
            )
        if receipt.source_uri in seen:
            continue
        seen.add(receipt.source_uri)
        sources.append(
            ReportSource(
                label=f"{receipt.provider} {receipt.source_type}",
                url=receipt.source_uri,
            )
        )
    return tuple(sources)


def _actual_close_verified_projection(
    *,
    context: object,
    market_source: object,
    mark: object,
    action: object,
    decision: object | None,
) -> object:
    from .domain import money_from_micros
    from .reports import ClosePosition
    from .risk import (
        ActualCloseDecisionSource,
        ActualCloseMarketSource,
        ActualPositionEventContext,
        MarketMark,
        PositionAction,
    )

    if (
        type(context) is not ActualPositionEventContext
        or type(market_source) is not ActualCloseMarketSource
        or type(mark) is not MarketMark
        or type(action) is not PositionAction
        or (
            decision is not None
            and type(decision) is not ActualCloseDecisionSource
        )
    ):
        raise CanonicalMaterialError(
            "actual close verified projection inputs are invalid"
        )
    mapped_action = {
        "PROVISIONAL_EXIT": "EXIT",
        "PROVISIONAL_HOLD": "HOLD",
        "PROVISIONAL_TIGHTEN_STOP": "TIGHTEN_STOP",
        "STOP_UNVERIFIED": "HOLD",
    }.get(action.status)
    if mapped_action is None:
        raise CanonicalMaterialError(
            "actual close position action cannot be projected as verified"
        )
    reasons = (
        decision.reason_codes
        if decision is not None
        else tuple(action.reason_codes)
    )
    recommended_stop = (
        money_from_micros(decision.recommended_stop_micros)
        if decision is not None
        else action.recommended_stop
    )
    quote_receipts = tuple(
        receipt
        for receipt in market_source.observation_receipts
        if receipt.source_type == "ALPACA_HISTORICAL_QUOTES"
    )
    if len(quote_receipts) != 1:
        raise CanonicalMaterialError(
            "actual close SIP quote receipt is not exact"
        )
    quote_receipt = quote_receipts[0]
    position = context.position
    return ClosePosition(
        symbol=position.symbol,
        shares=position.shares,
        mark=mark.price,
        estimated_unrealized_pl=(mark.price - position.entry) * position.shares,
        r_multiple=action.r_multiple,
        recommended_stop=recommended_stop,
        user_confirmed_stop=action.user_confirmed_stop,
        target=action.published_target,
        holding_days=context.holding_sessions,
        provider=quote_receipt.provider.upper(),
        feed=quote_receipt.feed.upper(),
        observed_at=market_source.observed_at,
        upcoming_events=(),
        evidence=_actual_close_report_sources(market_source),
        action=mapped_action,
        reason_codes=reasons,
    )


def _actual_close_market_failure_reason(
    review_source: object,
    symbol: str,
    error: Exception,
) -> str:
    from .journal import ActualCloseReviewSource

    if type(review_source) is ActualCloseReviewSource:
        failures = {
            binding.source_role
            for binding in review_source.bindings
            if binding.symbol == symbol and binding.failure_code is not None
        }
        if failures.intersection(
            {"SIP_DAILY_BAR", "SIP_MINUTE_BAR", "SIP_QUOTE"}
        ):
            return "SIP_MARK_UNAVAILABLE"
        if "IEX_FRESHNESS" in failures:
            return "PROVIDER_CHECK_FAILED"
    code = str(error)
    if "IEX" in code:
        return "PROVIDER_CHECK_FAILED"
    return "SIP_MARK_UNAVAILABLE"


def _actual_close_review_failure_reasons(
    review_source: object,
    symbol: str,
) -> tuple[str, ...]:
    from .journal import ActualCloseReviewSource

    if type(review_source) is not ActualCloseReviewSource:
        raise CanonicalMaterialError("actual close review source is invalid")
    reasons: list[str] = []
    if any(
        binding.symbol == symbol
        and binding.source_role == "EVENT_EVIDENCE"
        and binding.failure_code is not None
        for binding in review_source.bindings
    ):
        reasons.append("EVENT_EVIDENCE_UNAVAILABLE")
    return tuple(reasons)


def _actual_close_coordinator_reason_codes(
    review_source: object,
) -> tuple[str, ...]:
    from .journal import ActualCloseReviewSource

    if type(review_source) is not ActualCloseReviewSource:
        raise CanonicalMaterialError("actual close review source is invalid")
    if any(
        binding.symbol is None and binding.failure_code is not None
        for binding in review_source.bindings
    ):
        return ("SOURCE_CHECK_FAILED",)
    return ()


def _actual_close_ledger_fingerprint_value(value: object) -> object:
    """Return callback-free canonical material for the ACTUAL stability seal."""
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is datetime:
        return {"datetime": _canonical_timestamp(value)}
    if type(value) is date:
        return {"date": value.isoformat()}
    if type(value) is Decimal:
        return {"decimal": str(value)}
    if type(value) is bytes:
        return {
            "bytes_length": len(value),
            "bytes_sha256": hashlib.sha256(value).hexdigest(),
        }
    if type(value) is tuple:
        return [
            _actual_close_ledger_fingerprint_value(item)
            for item in value
        ]
    if type(value) is list:
        return [
            _actual_close_ledger_fingerprint_value(item)
            for item in value
        ]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise CanonicalMaterialError(
                "actual close ledger stability mapping is invalid"
            )
        return {
            key: _actual_close_ledger_fingerprint_value(value[key])
            for key in sorted(value)
        }
    if is_dataclass(value) and type(value).__module__.startswith(
        "stock_monitor."
    ):
        return {
            "dataclass": (
                f"{type(value).__module__}.{type(value).__qualname__}"
            ),
            "fields": {
                field.name: _actual_close_ledger_fingerprint_value(
                    getattr(value, field.name)
                )
                for field in fields(type(value))
            },
        }
    raise CanonicalMaterialError(
        "actual close ledger stability value is unsupported"
    )


def _actual_close_ledger_stability_digest(
    replay_source: object,
    actual_state: object,
) -> str:
    """Bind all ACTUAL economics while excluding cutoff-derived digests."""
    from .journal import JournalActualReplaySource
    from .reconciliation import ActualLedgerState

    if (
        type(replay_source) is not JournalActualReplaySource
        or type(actual_state) is not ActualLedgerState
    ):
        raise CanonicalMaterialError(
            "actual close ledger stability inputs are invalid"
        )
    replay_material = {
        field.name: _actual_close_ledger_fingerprint_value(
            getattr(replay_source, field.name)
        )
        for field in fields(JournalActualReplaySource)
        if field.name not in {"query_cutoff", "source_digest"}
    }
    state_material = {
        field.name: _actual_close_ledger_fingerprint_value(
            getattr(actual_state, field.name)
        )
        for field in fields(ActualLedgerState)
        if field.name
        not in {
            "query_cutoff",
            "source_digest",
            "journal_source_digest",
        }
    }
    return hashlib.sha256(
        b"stock-monitor/actual-close-ledger-stability/v1\0"
        + json.dumps(
            {
                "replay": replay_material,
                "state": state_material,
            },
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _actual_close_plan_resolution_stability_digest(
    resolutions: tuple[tuple[str, object], ...],
) -> str:
    """Bind exact per-position plan semantics without cutoff-derived digests."""
    from .journal import (
        ActualPositionPlanResolution,
        ActualPositionPlanSource,
        Phase1SignalSource,
    )

    if (
        type(resolutions) is not tuple
        or tuple(sorted(symbol for symbol, _resolution in resolutions))
        != tuple(symbol for symbol, _resolution in resolutions)
        or len({symbol for symbol, _resolution in resolutions})
        != len(resolutions)
    ):
        raise CanonicalMaterialError(
            "actual close plan stability resolutions are invalid"
        )
    material: list[object] = []
    for symbol, resolution in resolutions:
        if (
            type(symbol) is not str
            or not symbol
            or type(resolution) is not ActualPositionPlanResolution
        ):
            raise CanonicalMaterialError(
                "actual close plan stability resolution is invalid"
            )
        resolution_material: dict[str, object] = {
            "symbol": symbol,
            "status": resolution.status,
            "reason_codes": list(resolution.reason_codes),
        }
        source = resolution.source
        if source is None:
            resolution_material["source"] = None
        else:
            if (
                type(source) is not ActualPositionPlanSource
                or source.symbol != symbol
                or type(source.signal_source) is not Phase1SignalSource
            ):
                raise CanonicalMaterialError(
                    "actual close plan stability source is invalid"
                )
            signal_material = {
                field.name: _actual_close_ledger_fingerprint_value(
                    getattr(source.signal_source, field.name)
                )
                for field in fields(Phase1SignalSource)
                if field.name
                not in {
                    "publication_source",
                    "query_cutoff",
                    "source_digest",
                }
            }
            source_material = {
                field.name: _actual_close_ledger_fingerprint_value(
                    getattr(source, field.name)
                )
                for field in fields(ActualPositionPlanSource)
                if field.name
                not in {
                    "actual_position_state",
                    "actual_replay_source",
                    "query_cutoff",
                    "signal_source",
                    "source_digest",
                }
            }
            source_material["signal_source"] = signal_material
            resolution_material["source"] = source_material
        material.append(resolution_material)
    return hashlib.sha256(
        b"stock-monitor/actual-close-plan-stability/v1\0"
        + json.dumps(
            material,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


class ActualCloseWorkflowCoordinator:
    """Compose canonical ACTUAL close material without brokerage mutation."""

    def __init__(
        self,
        *,
        journal: object,
        report_archive_root: Path,
        source_collector: ActualCloseSourceCollector,
        calendar_resolver: object,
        policy: object,
        plans: object,
    ) -> None:
        from .journal import Journal
        from .risk import Policy, SessionCalendarResolver

        if type(journal) is not Journal or getattr(journal, "_closed", True):
            raise CanonicalMaterialError(
                "actual close coordinator requires an open Journal"
            )
        if not callable(getattr(source_collector, "collect_close_sources", None)):
            raise CanonicalMaterialError(
                "actual close source collector is unavailable"
            )
        if type(calendar_resolver) is not SessionCalendarResolver:
            raise CanonicalMaterialError(
                "actual close calendar authority is invalid"
            )
        if type(policy) is not Policy:
            raise CanonicalMaterialError("actual close policy is invalid")
        policy.validate()
        self._journal = journal
        self._report_archive_root = _canonical_archive_root(
            report_archive_root
        )
        self._source_collector = source_collector
        self._calendar_resolver = calendar_resolver
        self._policy = policy
        self._plans = plans

    def _snapshot(self, query_cutoff: datetime) -> tuple[object, object]:
        from .reconciliation import replay_actual

        try:
            with self._journal.transaction() as transaction:
                replay_source = transaction.read_actual_replay(
                    query_cutoff=query_cutoff,
                )
            actual_state = replay_actual(
                replay_source,
                plans=self._plans,
                calendar=self._calendar_resolver,
                policy=self._policy,
            )
        except Exception as error:
            raise CanonicalMaterialError(
                "actual close replay is unavailable"
            ) from error
        return replay_source, actual_state

    def _plan_resolutions(
        self,
        *,
        replay_source: object,
        actual_state: object,
        query_cutoff: datetime,
    ) -> tuple[tuple[str, object], ...]:
        from .journal import JournalError

        resolutions: list[tuple[str, object]] = []
        try:
            for actual_position in actual_state.positions:
                resolutions.append(
                    (
                        actual_position.symbol,
                        self._journal.resolve_actual_position_plan_source(
                            actual_replay_source=replay_source,
                            actual_position_state=actual_state,
                            symbol=actual_position.symbol,
                            query_cutoff=query_cutoff,
                        ),
                    )
                )
        except JournalError as error:
            raise CanonicalMaterialError(
                "actual close plan resolution is unavailable"
            ) from error
        return tuple(sorted(resolutions, key=lambda value: value[0]))

    def _read_review(
        self,
        collection: ActualCloseCollection,
        *,
        session_date: date,
        review_at: datetime,
        mark_cutoff: datetime,
        query_cutoff: datetime,
        retrieved_at: datetime,
    ) -> object:
        try:
            review_source = self._journal.read_actual_close_review_source(
                collection.review_id,
                query_cutoff=query_cutoff,
            )
        except Exception as error:
            raise CanonicalMaterialError(
                "actual close final review is unavailable"
            ) from error
        if (
            review_source.session_date != session_date
            or review_source.review_at != review_at
            or review_source.mark_cutoff != mark_cutoff
            or review_source.query_cutoff != query_cutoff
            or review_source.retrieved_at != retrieved_at
        ):
            raise CanonicalMaterialError(
                "actual close collection review timing is not exact"
            )
        return review_source

    def _project_position(
        self,
        *,
        replay_source: object,
        actual_state: object,
        actual_position: object,
        resolution: object,
        review_source: object,
        query_cutoff: datetime,
    ) -> tuple[object, object | None, _ClosePositionAuthorityBundle]:
        from .journal import (
            ActualCloseReviewSource,
            ActualPositionPlanResolution,
            JournalError,
        )
        from .risk import (
            RiskBlock,
            evaluate_position,
            issue_actual_close_decision,
            issue_actual_close_mark,
            issue_actual_close_market_source,
            issue_actual_position_event_context,
        )

        if type(review_source) is not ActualCloseReviewSource:
            raise CanonicalMaterialError(
                "actual close projection requires an exact final review"
            )
        review_at = review_source.review_at
        reconciliation_reasons = _actual_close_reconciliation_reasons(
            actual_state
        )
        status_for_block = (
            "RECONCILIATION_REQUIRED"
            if reconciliation_reasons
            else "POSITION_UNVERIFIED"
        )
        if type(resolution) is not ActualPositionPlanResolution:
            raise CanonicalMaterialError(
                "actual close plan resolution is unavailable"
            )

        def unverified(
            *,
            status: str,
            reason_codes: tuple[str, ...],
            branch: str,
            authority_children: tuple[object, ...] = (),
        ) -> tuple[object, None, _ClosePositionAuthorityBundle]:
            projection = _actual_close_unverified_projection(
                actual_position,
                status=status,
                reason_codes=reason_codes,
            )
            return (
                projection,
                None,
                _issue_close_position_authority_bundle(
                    projection=projection,
                    branch=branch,
                    identity_children=(
                        actual_position,
                        resolution,
                        *authority_children,
                    ),
                    coordinator=self,
                    actual_state=actual_state,
                    actual_replay_source=replay_source,
                    query_cutoff=query_cutoff,
                ),
            )

        if resolution.status != "RESOLVED" or resolution.source is None:
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    *resolution.reason_codes,
                ),
                branch="PLAN_UNAVAILABLE",
            )
        plan_source = resolution.source
        review_failure_reasons = _actual_close_review_failure_reasons(
            review_source,
            actual_position.symbol,
        )
        if review_failure_reasons:
            status = (
                "RECONCILIATION_REQUIRED"
                if reconciliation_reasons
                else (
                    "POSITION_UNVERIFIED"
                    if "EVENT_EVIDENCE_UNAVAILABLE"
                    in review_failure_reasons
                    else "DATA_UNAVAILABLE"
                )
            )
            return unverified(
                status=status,
                reason_codes=(
                    *reconciliation_reasons,
                    *review_failure_reasons,
                ),
                branch="REVIEW_SOURCE_UNAVAILABLE",
                authority_children=(plan_source, review_source),
            )
        try:
            evidence_source = self._journal.read_phase1_signal_evidence_source(
                plan_source.signal_source.signal_id,
                review_at=review_at,
                query_cutoff=query_cutoff,
                calendar_resolver=self._calendar_resolver,
                exact_signal_source=plan_source.signal_source,
            )
        except (JournalError, RiskBlock):
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "EVENT_EVIDENCE_UNAVAILABLE",
                ),
                branch="EVENT_EVIDENCE_UNAVAILABLE",
                authority_children=(plan_source, review_source),
            )
        try:
            from .risk import _issue_phase1_signal_evidence_authority_from_source

            event_evidence = _issue_phase1_signal_evidence_authority_from_source(
                evidence_source,
                calendar_resolver=self._calendar_resolver,
            )
        except (JournalError, RiskBlock):
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "EVENT_EVIDENCE_UNAVAILABLE",
                ),
                branch="EVENT_AUTHORITY_UNAVAILABLE",
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                ),
            )
        try:
            history_source = (
                self._journal.read_latest_close_recommendation_source(
                    position_plan_source=plan_source,
                    query_cutoff=query_cutoff,
                )
            )
        except (JournalError, RiskBlock):
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "RECOMMENDATION_HISTORY_UNAVAILABLE",
                ),
                branch="RECOMMENDATION_HISTORY_UNAVAILABLE",
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                    event_evidence,
                ),
            )
        try:
            market_source = issue_actual_close_market_source(
                review_source,
                plan_source,
                calendar_resolver=self._calendar_resolver,
            )
        except RiskBlock as error:
            reason = _actual_close_market_failure_reason(
                review_source,
                actual_position.symbol,
                error,
            )
            return unverified(
                status=(
                    "RECONCILIATION_REQUIRED"
                    if reconciliation_reasons
                    else "DATA_UNAVAILABLE"
                ),
                reason_codes=(*reconciliation_reasons, reason),
                branch="MARKET_SOURCE_UNAVAILABLE",
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                    event_evidence,
                    history_source,
                ),
            )
        try:
            context = issue_actual_position_event_context(
                market_source,
                plan_source,
                event_evidence,
                history_source,
                calendar_resolver=self._calendar_resolver,
                policy=self._policy,
            )
        except RiskBlock:
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "POSITION_CONTEXT_UNVERIFIED",
                ),
                branch="POSITION_CONTEXT_UNVERIFIED",
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                    event_evidence,
                    history_source,
                    market_source,
                ),
            )
        try:
            mark = issue_actual_close_mark(market_source, context)
        except RiskBlock:
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "POSITION_CONTEXT_UNVERIFIED",
                ),
                branch="CLOSE_MARK_UNVERIFIED",
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                    event_evidence,
                    history_source,
                    market_source,
                    context,
                ),
            )
        try:
            action = evaluate_position(context.position, mark, self._policy)
        except RiskBlock:
            return unverified(
                status=status_for_block,
                reason_codes=(
                    *reconciliation_reasons,
                    "POSITION_CONTEXT_UNVERIFIED",
                ),
                authority_children=(
                    plan_source,
                    review_source,
                    evidence_source,
                    event_evidence,
                    history_source,
                    market_source,
                    context,
                    mark,
                ),
                branch="POSITION_EVALUATION_UNVERIFIED",
            )
        exact_chain = (
            plan_source,
            review_source,
            evidence_source,
            event_evidence,
            history_source,
            market_source,
            context,
            mark,
            action,
        )
        if reconciliation_reasons:
            return unverified(
                status="RECONCILIATION_REQUIRED",
                reason_codes=reconciliation_reasons,
                branch="RECONCILIATION_REQUIRED",
                authority_children=exact_chain,
            )
        if action.status in {"POSITION_UNVERIFIED", "RECONCILIATION_REQUIRED"}:
            return unverified(
                status=action.status,
                reason_codes=tuple(action.reason_codes),
                branch=action.status,
                authority_children=exact_chain,
            )
        decision = None
        if action.status != "STOP_UNVERIFIED":
            try:
                decision = issue_actual_close_decision(
                    context,
                    mark,
                    self._policy,
                )
                action = decision.position_action
                exact_chain = (*exact_chain[:-1], action)
            except RiskBlock:
                return unverified(
                    status="POSITION_UNVERIFIED",
                    reason_codes=("POSITION_CONTEXT_UNVERIFIED",),
                    branch="DECISION_AUTHORITY_UNAVAILABLE",
                    authority_children=exact_chain,
                )
        projection = _actual_close_verified_projection(
            context=context,
            market_source=market_source,
            mark=mark,
            action=action,
            decision=decision,
        )
        bundle_children = (
            *exact_chain,
            *((decision,) if decision is not None else ()),
        )
        return (
            projection,
            decision,
            _issue_close_position_authority_bundle(
                projection=projection,
                branch=(
                    "STOP_UNVERIFIED"
                    if decision is None
                    else "VERIFIED_DECISION"
                ),
                identity_children=(
                    actual_position,
                    resolution,
                    *bundle_children,
                ),
                coordinator=self,
                actual_state=actual_state,
                actual_replay_source=replay_source,
                query_cutoff=query_cutoff,
            ),
        )

    def _project_all(
        self,
        *,
        replay_source: object,
        actual_state: object,
        resolutions: tuple[tuple[str, object], ...],
        review_source: object,
        query_cutoff: datetime,
    ) -> tuple[
        tuple[object, ...],
        tuple[object, ...],
        tuple[_ClosePositionAuthorityBundle, ...],
    ]:
        projections: list[object] = []
        decisions: list[object] = []
        authorities: list[_ClosePositionAuthorityBundle] = []
        resolution_by_symbol = dict(resolutions)
        if (
            len(resolution_by_symbol) != len(resolutions)
            or set(resolution_by_symbol)
            != {position.symbol for position in actual_state.positions}
        ):
            raise CanonicalMaterialError(
                "actual close plan resolution cohort is not exact"
            )
        for actual_position in actual_state.positions:
            projection, decision, authority = self._project_position(
                replay_source=replay_source,
                actual_state=actual_state,
                actual_position=actual_position,
                resolution=resolution_by_symbol[actual_position.symbol],
                review_source=review_source,
                query_cutoff=query_cutoff,
            )
            projections.append(projection)
            authorities.append(authority)
            if decision is not None:
                decisions.append(decision)
        return tuple(projections), tuple(decisions), tuple(authorities)

    def close_material(
        self,
        session_date: date,
        *,
        review_at: datetime,
        retrieved_at: datetime,
    ) -> CanonicalCloseMaterial:
        from .reports import (
            ClosePosition,
            CloseState,
            UnverifiedClosePosition,
            render_close_report,
        )

        session_date = _require_session(session_date)
        review_at = _require_time(review_at, "actual close review time")
        command_started_at = _require_time(
            retrieved_at,
            "actual close command start time",
        )
        discovery_cutoff = command_started_at
        _validate_close_times(
            session_date,
            review_at,
            discovery_cutoff,
            command_started_at,
        )
        try:
            schedule = self._calendar_resolver.session(session_date)
        except Exception as error:
            raise CanonicalMaterialError(
                "actual close calendar session is unavailable"
            ) from error
        expected_review = datetime.combine(
            session_date,
            schedule.review_time,
            tzinfo=schedule.timezone,
        )
        if review_at != expected_review:
            raise CanonicalMaterialError(
                "actual close review time conflicts with calendar"
            )
        session_close = datetime.combine(
            session_date,
            schedule.close_time,
            tzinfo=schedule.timezone,
        )
        mark_cutoff = min(review_at - timedelta(minutes=16), session_close)

        discovery_source, discovery_state = self._snapshot(
            discovery_cutoff
        )
        ledger_stability_digest = _actual_close_ledger_stability_digest(
            discovery_source,
            discovery_state,
        )
        plan_stability_digest = (
            _actual_close_plan_resolution_stability_digest(
                self._plan_resolutions(
                    replay_source=discovery_source,
                    actual_state=discovery_state,
                    query_cutoff=discovery_cutoff,
                )
            )
        )
        symbols = tuple(
            sorted(
                {
                    position.symbol
                    for position in discovery_state.positions
                    if position.shares > 0
                }
            )
        )
        try:
            collection = self._source_collector.collect_close_sources(
                journal=self._journal,
                symbols=symbols,
                session_date=session_date,
                review_at=review_at,
                mark_cutoff=mark_cutoff,
                command_started_at=command_started_at,
            )
        except Exception as error:
            # Collection may already have persisted one or more source rows.
            # Always obtain a new ACTUAL authority before surfacing the error
            # so a discovered reconciliation condition is never hidden behind
            # stale pre-write state.
            self._snapshot(discovery_cutoff)
            raise CanonicalMaterialError(
                "actual close collection failed after final replay"
            ) from error
        if type(collection) is not ActualCloseCollection:
            raise CanonicalMaterialError(
                "actual close collector returned an invalid durable review"
            )
        collected_at = _require_time(
            collection.collected_at,
            "actual close terminal collection time",
        )
        _validate_close_times(
            session_date,
            review_at,
            discovery_cutoff,
            collected_at,
        )
        query_cutoff = collected_at
        collected_review = self._read_review(
            collection,
            session_date=session_date,
            review_at=review_at,
            mark_cutoff=mark_cutoff,
            query_cutoff=query_cutoff,
            retrieved_at=collected_at,
        )
        coordinator_reason_codes = _actual_close_coordinator_reason_codes(
            collected_review
        )

        stable_source, stable_state = self._snapshot(query_cutoff)
        if _actual_close_ledger_stability_digest(
            stable_source,
            stable_state,
        ) != ledger_stability_digest:
            raise CanonicalMaterialError(
                "actual close ledger changed during provider collection"
            )
        stable_resolutions = self._plan_resolutions(
            replay_source=stable_source,
            actual_state=stable_state,
            query_cutoff=query_cutoff,
        )
        if _actual_close_plan_resolution_stability_digest(
            stable_resolutions
        ) != plan_stability_digest:
            raise CanonicalMaterialError(
                "actual close plan binding changed during provider collection"
            )
        stable_positions, stable_decisions, _stable_authorities = (
            self._project_all(
            replay_source=stable_source,
            actual_state=stable_state,
            resolutions=stable_resolutions,
            review_source=collected_review,
            query_cutoff=query_cutoff,
            )
        )
        _report_outcome, _workflow_outcome, exit_code, _reasons = (
            _canonical_close_projection(
                stable_state,
                stable_positions,
                coordinator_reason_codes,
            )
        )
        if exit_code == 0 and stable_decisions:
            current_decisions: list[object] = []
            recommendation_review = self._read_review(
                collection,
                session_date=session_date,
                review_at=review_at,
                mark_cutoff=mark_cutoff,
                query_cutoff=query_cutoff,
                retrieved_at=collected_at,
            )
            for expected_decision in stable_decisions:
                current_source, current_state = self._snapshot(query_cutoff)
                if _actual_close_ledger_stability_digest(
                    current_source,
                    current_state,
                ) != ledger_stability_digest:
                    raise CanonicalMaterialError(
                        "actual close ledger changed before recommendation"
                    )
                current_resolutions = self._plan_resolutions(
                    replay_source=current_source,
                    actual_state=current_state,
                    query_cutoff=query_cutoff,
                )
                if _actual_close_plan_resolution_stability_digest(
                    current_resolutions
                ) != plan_stability_digest:
                    raise CanonicalMaterialError(
                        "actual close plan binding changed before recommendation"
                    )
                matching = tuple(
                    position
                    for position in current_state.positions
                    if position.symbol == expected_decision.symbol
                )
                if len(matching) != 1:
                    raise CanonicalMaterialError(
                        "actual close position changed before recommendation"
                    )
                current_resolution = dict(current_resolutions)[
                    expected_decision.symbol
                ]
                _projection, current_decision, _current_authority = (
                    self._project_position(
                    replay_source=current_source,
                    actual_state=current_state,
                    actual_position=matching[0],
                    resolution=current_resolution,
                    review_source=recommendation_review,
                    query_cutoff=query_cutoff,
                    )
                )
                if (
                    current_decision is None
                    or current_decision.source_digest
                    != expected_decision.source_digest
                ):
                    raise CanonicalMaterialError(
                        "actual close decision changed before recommendation"
                    )
                current_decisions.append(current_decision)
            try:
                self._journal.append_close_recommendations(
                    tuple(current_decisions)
                )
            except Exception as error:
                raise CanonicalMaterialError(
                    "actual close recommendation could not be persisted"
                ) from error

        final_source, final_state = self._snapshot(query_cutoff)
        if _actual_close_ledger_stability_digest(
            final_source,
            final_state,
        ) != ledger_stability_digest:
            raise CanonicalMaterialError(
                "actual close ledger changed before final composition"
            )
        final_resolutions = self._plan_resolutions(
            replay_source=final_source,
            actual_state=final_state,
            query_cutoff=query_cutoff,
        )
        if _actual_close_plan_resolution_stability_digest(
            final_resolutions
        ) != plan_stability_digest:
            raise CanonicalMaterialError(
                "actual close plan binding changed before final composition"
            )
        final_review = self._read_review(
            collection,
            session_date=session_date,
            review_at=review_at,
            mark_cutoff=mark_cutoff,
            query_cutoff=query_cutoff,
            retrieved_at=collected_at,
        )
        positions, _final_decisions, position_authorities = self._project_all(
            replay_source=final_source,
            actual_state=final_state,
            resolutions=final_resolutions,
            review_source=final_review,
            query_cutoff=query_cutoff,
        )
        coordinator_reason_codes = _actual_close_coordinator_reason_codes(
            final_review
        )
        outcome, _workflow_outcome, _exit_code, reason_codes = (
            _canonical_close_projection(
                final_state,
                positions,
                coordinator_reason_codes,
            )
        )
        receipts = canonical_source_receipts(final_review.receipts)
        composition_authority = _issue_close_composition_authority(
            session_date=session_date,
            review_at=review_at,
            retrieved_at=collected_at,
            query_cutoff=query_cutoff,
            actual_state=final_state,
            actual_replay_source=final_source,
            positions=positions,
            source_receipts=receipts,
            review_source=final_review,
            position_authorities=position_authorities,
            outcome=outcome,
            reason_codes=reason_codes,
            coordinator_reason_codes=coordinator_reason_codes,
        )
        state_hash = canonical_close_state_hash(
            session_date=session_date,
            review_at=review_at,
            retrieved_at=collected_at,
            query_cutoff=query_cutoff,
            actual_state=final_state,
            actual_replay_source=final_source,
            positions=positions,
            source_receipts=receipts,
            outcome=outcome,
            reason_codes=reason_codes,
            coordinator_reason_codes=coordinator_reason_codes,
            review_source_digest=composition_authority.review_source_digest,
            position_authority_digests=(
                composition_authority.position_authority_digests
            ),
            composition_authority=composition_authority,
        )
        unverified_statuses = {
            position.status
            for position in positions
            if type(position) is UnverifiedClosePosition
        }
        actions = {
            position.action
            for position in positions
            if type(position) is ClosePosition
        }
        close_state = CloseState(
            session_date=session_date,
            generated_at=collected_at,
            reason_codes=reason_codes,
            positions=positions,
            observation_ids=tuple(
                receipt.observation_sha256 for receipt in receipts
            ),
            state_hash=state_hash,
            reconciliation_required=bool(
                _actual_close_reconciliation_reasons(final_state)
            ),
            position_verified="POSITION_UNVERIFIED" not in unverified_statuses,
            stop_verified=(
                "STOP_UNVERIFIED" not in unverified_statuses
                and all(
                    position.user_confirmed_stop is not None
                    for position in positions
                    if type(position) is ClosePosition
                )
            ),
            data_available=(
                not coordinator_reason_codes
                and "DATA_UNAVAILABLE" not in unverified_statuses
            ),
            exit_due="EXIT" in actions,
            tighten_stop_due="TIGHTEN_STOP" in actions,
        )
        report = render_close_report(close_state)
        if report.outcome != outcome:
            raise CanonicalMaterialError(
                "actual close report outcome conflicts with aggregation"
            )
        return issue_canonical_close_material(
            journal=self._journal,
            report_archive_root=self._report_archive_root,
            session_date=session_date,
            review_at=review_at,
            retrieved_at=collected_at,
            query_cutoff=query_cutoff,
            actual_state=final_state,
            actual_replay_source=final_source,
            composition_authority=composition_authority,
            report=report,
            positions=positions,
            source_receipts=receipts,
            coordinator_reason_codes=coordinator_reason_codes,
            state_hash=state_hash,
        )


def _material_authority(
    material: object,
    *,
    journal: object,
    report_archive_root: Path,
) -> _MaterialAuthority | None:
    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        return None
    try:
        root = _canonical_archive_root(report_archive_root)
        fingerprint = _material_fingerprint(material)
    except Exception:
        return None
    with _MATERIAL_AUTHORITY_LOCK:
        authority = _ISSUED_CANONICAL_MATERIALS.get(id(material))
    if (
        authority is None
        or authority.material_reference() is not material
        or authority.journal_reference() is not journal
        or authority.archive_root != root
        or authority.material_fingerprint != fingerprint
        or len(_material_identity_children(material))
        != len(authority.identity_children)
        or any(
            current is not expected
            for current, expected in zip(
                _material_identity_children(material),
                authority.identity_children,
                strict=True,
            )
        )
        or not _domain_is_current_without_callbacks(
            material,
            authority.domain_authority,
        )
        or not _composition_is_current_without_callbacks(
            material,
            authority.composition_envelope,
        )
    ):
        return None
    candidates = _current_receipt_candidates(journal, material.source_receipts)
    if candidates is None or len(candidates) != len(authority.receipt_candidates):
        return None
    if any(
        not (
            current[0] is expected[0]
            and current[1] is expected[1]
            and current[2] is expected[2]
            and current[3:5] == expected[3:5]
            and current[5] is expected[5]
            and current[7] is expected[7]
        )
        for current, expected in zip(
            candidates,
            authority.receipt_candidates,
            strict=True,
        )
    ):
        return None
    return authority


def _is_current_canonical_material_without_callbacks(
    material: object,
    *,
    authority: _MaterialAuthority | None = None,
) -> bool:
    from . import journal as journal_module
    from .reports import is_issued_report

    if type(material) not in {CanonicalPremarketMaterial, CanonicalCloseMaterial}:
        return False
    if authority is None:
        with _MATERIAL_AUTHORITY_LOCK:
            authority = _ISSUED_CANONICAL_MATERIALS.get(id(material))
    journal = None if authority is None else authority.journal_reference()
    try:
        fingerprint = _material_fingerprint(material)
    except Exception:
        return False
    if (
        authority is None
        or authority.material_reference() is not material
        or journal is None
        or getattr(journal, "_closed", True)
        or getattr(journal, "_source_generation", None)
        != authority.journal_generation
        or authority.material_fingerprint != fingerprint
        or len(_material_identity_children(material))
        != len(authority.identity_children)
        or any(
            current is not expected
            for current, expected in zip(
                _material_identity_children(material),
                authority.identity_children,
                strict=True,
            )
        )
        or not is_issued_report(material.report)
        or not _domain_is_current_without_callbacks(
            material,
            authority.domain_authority,
        )
        or any(
            not journal_module._is_current_journal_authority_candidate_without_callbacks(
                candidate
            )
            for candidate in authority.receipt_candidates
        )
        or not _composition_is_current_without_callbacks(
            material,
            authority.composition_envelope,
        )
    ):
        return False
    with _MATERIAL_AUTHORITY_LOCK:
        return _ISSUED_CANONICAL_MATERIALS.get(id(material)) is authority


def is_issued_canonical_material(
    material: object,
    *,
    journal: object,
    report_archive_root: Path,
) -> bool:
    """Return whether material is exact, unchanged, current, and owner-bound."""
    authority = _material_authority(
        material,
        journal=journal,
        report_archive_root=report_archive_root,
    )
    return authority is not None and _is_current_canonical_material_without_callbacks(
        material,
        authority=authority,
    )


__all__ = [
    "ActualCloseCollection",
    "ActualCloseSourceCollector",
    "ActualCloseWorkflowCoordinator",
    "CanonicalCloseCompositionAuthority",
    "CanonicalCloseMaterial",
    "CanonicalMaterialError",
    "CanonicalPremarketCompositionAuthority",
    "CanonicalPremarketMaterial",
    "CanonicalPremarketSourceBindingAuthority",
    "CanonicalWorkflowAdapter",
    "PremarketCollectionError",
    "PremarketProviderCollection",
    "PremarketRiskResolution",
    "PremarketWorkflowCoordinator",
    "PremarketSourceBinding",
    "canonical_close_state_hash",
    "canonical_material_digest",
    "canonical_premarket_state_hash",
    "canonical_source_digest",
    "canonical_source_receipts",
    "is_issued_canonical_material",
    "is_issued_canonical_premarket_composition_authority",
    "is_issued_canonical_premarket_source_binding_authority",
    "issue_canonical_close_material",
    "issue_canonical_premarket_material",
    "issue_canonical_premarket_composition_authority",
    "issue_canonical_premarket_source_binding_authority",
]
