"""Concrete GET-only source and risk adapters for canonical premarket runs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo

from .domain import require_aware_timestamp
from .journal import Journal
from .policy import Policy
from .provider_workflows import (
    CanonicalMaterialError,
    PremarketProviderCollection,
    PremarketSourceBinding,
)
from .providers.alpaca import (
    AlpacaMarketData,
    ProviderFetchPageBundle,
    TimeWindow,
    is_issued_provider_fetch_page_bundle,
)
from .providers.reference import ReferenceClient
from .risk import RiskBlock, SessionCalendarResolver


_NEW_YORK = ZoneInfo("America/New_York")


PREMARKET_OPERATIONAL_REFERENCE_SOURCES: Mapping[str, str] = MappingProxyType(
    {
        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
            "PRIMARY_HALT_FEED"
        ),
        "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
            "TRADER_ALERT_HALT"
        ),
        "https://www.nyse.com/api/notifications/public/alerts?2=3": (
            "OPERATIONAL_STATUS"
        ),
    }
)


def _project_root(value: object) -> Path:
    if type(value) is not type(Path()) or not value.is_absolute():
        raise CanonicalMaterialError(
            "premarket runtime project root must be an absolute path"
        )
    if not value.is_dir() or value.is_symlink():
        raise CanonicalMaterialError(
            "premarket runtime project root is unavailable"
        )
    return value


def _clock(value: object) -> Callable[[], datetime]:
    if not callable(value):
        raise CanonicalMaterialError("premarket runtime clock is unavailable")
    return value


@dataclass(frozen=True, slots=True)
class _PremarketCollectionWindow:
    calendar_resolver: SessionCalendarResolver
    previous_session: date
    history_sessions: tuple[date, ...]
    hold_sessions: tuple[date, ...]
    daily_window: TimeWindow
    quote_window: TimeWindow


@dataclass(frozen=True, slots=True)
class _ValidationBreakerBinding:
    breaker_reference: ReferenceType[object]
    journal_reference: ReferenceType[object]
    session_date: date
    decision_at: datetime
    calendar: object
    calendar_resolver: SessionCalendarResolver
    calendar_digest: str
    history_source: object


def _adjacent_calendar(
    *,
    project_root: Path,
    year: int,
    previous: bool,
):
    from .market_calendar import load_current_market_calendar

    as_of = date(year, 12, 31) if previous else date(year, 1, 31)
    return load_current_market_calendar(project_root, as_of=as_of)


def _premarket_collection_window(
    *,
    project_root: Path,
    calendar: object,
    session_date: date,
) -> _PremarketCollectionWindow:
    """Resolve 60 completed and ten hold sessions from pinned yearly releases."""
    from .market_calendar import (
        CalendarError,
        MarketCalendar,
        is_release_verified_market_calendar,
    )

    project_root = _project_root(project_root)
    if (
        type(calendar) is not MarketCalendar
        or not is_release_verified_market_calendar(calendar)
        or type(session_date) is not date
        or calendar.year != session_date.year
    ):
        raise CalendarError("premarket runtime calendar authority is unavailable")
    resolver = SessionCalendarResolver((calendar,))

    def with_year(year: int, *, previous: bool) -> None:
        nonlocal resolver
        if any(value.year == year for value in resolver.calendars):
            return
        adjacent = _adjacent_calendar(
            project_root=project_root,
            year=year,
            previous=previous,
        )
        resolver = SessionCalendarResolver((*resolver.calendars, adjacent))

    try:
        if not resolver.is_open(session_date):
            raise CalendarError("premarket runtime session is closed")
        try:
            previous_session = resolver.previous_session(session_date)
        except RiskBlock as error:
            if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                raise
            with_year(session_date.year - 1, previous=True)
            previous_session = resolver.previous_session(session_date)

        descending = [previous_session]
        while len(descending) < 60:
            try:
                preceding = resolver.previous_session(descending[-1])
            except RiskBlock as error:
                if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                    raise
                with_year(descending[-1].year - 1, previous=True)
                preceding = resolver.previous_session(descending[-1])
            descending.append(preceding)
        history_sessions = tuple(reversed(descending))

        try:
            hold_sessions = tuple(
                resolver.add_sessions(session_date, offset)
                for offset in range(10)
            )
        except RiskBlock as error:
            if error.reason_code != "CALENDAR_COVERAGE_MISSING":
                raise
            with_year(session_date.year + 1, previous=False)
            hold_sessions = tuple(
                resolver.add_sessions(session_date, offset)
                for offset in range(10)
            )
        previous_schedule = resolver.session(previous_session)
    except RiskBlock as error:
        raise CalendarError(
            "premarket runtime calendar coverage is missing"
        ) from error

    previous_close = datetime.combine(
        previous_session,
        previous_schedule.close_time,
        previous_schedule.timezone,
    )
    daily_start = datetime.combine(
        history_sessions[0],
        time.min,
        _NEW_YORK,
    )
    return _PremarketCollectionWindow(
        calendar_resolver=resolver,
        previous_session=previous_session,
        history_sessions=history_sessions,
        hold_sessions=hold_sessions,
        daily_window=TimeWindow(daily_start, previous_close),
        quote_window=TimeWindow(
            previous_close - timedelta(minutes=5),
            previous_close,
        ),
    )


def _persist_provider_page(
    journal: Journal,
    page_bundle: ProviderFetchPageBundle,
) -> PremarketSourceBinding:
    """Durably bind one exact issued Alpaca page before collection continues."""
    if (
        type(journal) is not Journal
        or getattr(journal, "_closed", True)
        or type(page_bundle) is not ProviderFetchPageBundle
        or not is_issued_provider_fetch_page_bundle(page_bundle)
    ):
        raise CanonicalMaterialError(
            "premarket runtime provider page authority is unavailable"
        )
    page = page_bundle.page
    observation = page_bundle.observation
    if page.source_type not in {
        "ALPACA_DAILY_BARS",
        "ALPACA_HISTORICAL_QUOTES",
        "ALPACA_LATEST_QUOTES",
    }:
        raise CanonicalMaterialError(
            "premarket runtime provider page role is unsupported"
        )
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
        details={"source_observation_id": page.source_observation_id},
    )
    return PremarketSourceBinding(
        receipt=receipt,
        source=page_bundle,
        decision_basis=(
            "OPERATIONAL_HEALTH_ONLY"
            if page.source_type == "ALPACA_LATEST_QUOTES"
            else "ECONOMIC_INPUT"
        ),
    )


class PremarketRuntimeCollector:
    """Collect exact premarket provider/reference inputs without trading APIs."""

    __slots__ = (
        "_alpaca_market_data",
        "_clock",
        "_journal",
        "_project_root",
        "_reference_client",
    )

    def __init__(
        self,
        *,
        journal: Journal,
        project_root: Path,
        alpaca_market_data: AlpacaMarketData,
        reference_client: ReferenceClient,
        clock: Callable[[], datetime],
    ) -> None:
        if type(journal) is not Journal or getattr(journal, "_closed", True):
            raise CanonicalMaterialError(
                "premarket runtime collector requires an open Journal"
            )
        if type(alpaca_market_data) is not AlpacaMarketData:
            raise CanonicalMaterialError(
                "premarket runtime collector requires the Alpaca market-data client"
            )
        if (
            getattr(alpaca_market_data, "_base_url", None)
            != "https://data.alpaca.markets"
            or getattr(
                getattr(alpaca_market_data, "_policy", None),
                "allowed_hosts",
                None,
            )
            != frozenset({"data.alpaca.markets"})
        ):
            raise CanonicalMaterialError(
                "premarket runtime Alpaca client scope is invalid"
            )
        configured_reference_roles = getattr(
            reference_client,
            "_source_roles",
            None,
        )
        if (
            type(reference_client) is not ReferenceClient
            or not isinstance(configured_reference_roles, Mapping)
            or any(
                configured_reference_roles.get(url) != role
                for url, role in PREMARKET_OPERATIONAL_REFERENCE_SOURCES.items()
            )
        ):
            raise CanonicalMaterialError(
                "premarket runtime reference client scope is invalid"
            )
        self._journal = journal
        self._alpaca_market_data = alpaca_market_data
        self._reference_client = reference_client
        self._project_root = _project_root(project_root)
        self._clock = _clock(clock)

    @property
    def journal(self) -> Journal:
        return self._journal

    def _terminal_time(
        self,
        *,
        retrieved_at: datetime,
        bindings: tuple[PremarketSourceBinding, ...],
    ) -> datetime:
        terminal = require_aware_timestamp(
            self._clock(),
            "premarket collection terminal time",
        )
        if terminal < retrieved_at or any(
            binding.receipt.retrieved_at > terminal for binding in bindings
        ):
            raise CanonicalMaterialError(
                "premarket collection clock predates its source receipts"
            )
        return terminal

    def collect(
        self,
        *,
        session_date: date,
        decision_at: datetime,
        retrieved_at: datetime,
        calendar: object,
        universe: object,
        evidence_release: object,
        required_symbols: tuple[str, ...],
    ) -> PremarketProviderCollection:
        """Collect one exact, complete GET-only premarket source cohort."""
        from . import evidence as evidence_module
        from . import provider_workflows as provider_workflows_module
        from . import screening as screening_module
        from .market_calendar import (
            MarketCalendar,
            is_release_verified_market_calendar,
        )
        from .providers.http import NetworkPolicyError, ProviderResponseError
        from .providers.reference import classify_instrument_status
        from .universe import UniverseSnapshot, is_verified_universe_snapshot

        decision_at = require_aware_timestamp(
            decision_at,
            "premarket decision time",
        )
        retrieved_at = require_aware_timestamp(
            retrieved_at,
            "premarket requested retrieval time",
        )
        provider_workflows_module._validate_premarket_times(
            session_date,
            decision_at,
            retrieved_at,
        )
        if (
            type(calendar) is not MarketCalendar
            or not is_release_verified_market_calendar(calendar)
            or calendar.year != session_date.year
            or type(universe) is not UniverseSnapshot
            or not is_verified_universe_snapshot(universe)
            or type(evidence_release)
            is not evidence_module.ReviewedEvidenceRelease
            or not evidence_module.is_verified_evidence_release(evidence_release)
        ):
            raise CanonicalMaterialError(
                "premarket runtime collection authority is unavailable"
            )
        enabled_records = tuple(
            record for record in universe.records if record.enabled
        )
        expected_symbols = tuple(sorted(record.symbol for record in enabled_records))
        universe_identity = provider_workflows_module._reviewed_binding_identity(
            universe
        )
        if (
            universe_identity is None
            or evidence_release.universe_sha256 != universe_identity[1]
            or type(required_symbols) is not tuple
            or required_symbols != expected_symbols
            or tuple(sorted(evidence_release.by_symbol)) != expected_symbols
        ):
            raise CanonicalMaterialError(
                "premarket runtime collection authority is unavailable"
            )

        window = _premarket_collection_window(
            project_root=self._project_root,
            calendar=calendar,
            session_date=session_date,
        )
        # CandidateContext and its session attestation are intentionally
        # single-release authorities.  Adjacent releases may support the
        # validation history, but never silently widen that screening contract.
        if any(
            value.year != calendar.year
            for value in (*window.history_sessions, *window.hold_sessions)
        ):
            terminal = self._terminal_time(
                retrieved_at=retrieved_at,
                bindings=(),
            )
            return PremarketProviderCollection(
                collected_at=terminal,
                failure_reason="DATA_UNAVAILABLE",
            )

        bindings: list[PremarketSourceBinding] = []
        cohorts: list[object] = []
        documents: list[object] = []

        def page_sink(page: ProviderFetchPageBundle) -> None:
            bindings.append(_persist_provider_page(self._journal, page))

        try:
            cohorts.append(
                self._alpaca_market_data.daily_bars(
                    required_symbols,
                    window.daily_window,
                    page_sink=page_sink,
                )
            )
            cohorts.append(
                self._alpaca_market_data.historical_quotes(
                    required_symbols,
                    window.quote_window,
                    page_sink=page_sink,
                )
            )
            cohorts.append(
                self._alpaca_market_data.latest_iex_quote_cohort(
                    required_symbols,
                    page_sink=page_sink,
                )
            )
        except (NetworkPolicyError, ProviderResponseError):
            persisted = tuple(bindings)
            terminal = self._terminal_time(
                retrieved_at=retrieved_at,
                bindings=persisted,
            )
            return PremarketProviderCollection(
                collected_at=terminal,
                provider_cohorts=tuple(cohorts),
                persisted_bindings=persisted,
                failure_reason="PROVIDER_CHECK_FAILED",
            )

        try:
            for url, role in PREMARKET_OPERATIONAL_REFERENCE_SOURCES.items():
                document = self._reference_client.fetch(url, role=role)
                persisted = provider_workflows_module._persist_premarket_reference_bindings(
                    journal=self._journal,
                    owner=self._reference_client,
                    documents=(document,),
                )
                bindings.extend(persisted)
                documents.append(document)
        except CanonicalMaterialError:
            raise
        except (NetworkPolicyError, ProviderResponseError, ValueError):
            persisted = tuple(bindings)
            terminal = self._terminal_time(
                retrieved_at=retrieved_at,
                bindings=persisted,
            )
            return PremarketProviderCollection(
                collected_at=terminal,
                provider_cohorts=tuple(cohorts),
                reference_sources=tuple(documents),
                persisted_bindings=persisted,
                failure_reason="SOURCE_CHECK_FAILED",
            )

        persisted = tuple(bindings)
        terminal = self._terminal_time(
            retrieved_at=retrieved_at,
            bindings=persisted,
        )
        try:
            by_role = {document.source_role: document for document in documents}
            snapshots = {
                "primary_halt_feed": self._reference_client.parse_halt_feed(
                    by_role["PRIMARY_HALT_FEED"]
                ),
                "operational_status": self._reference_client.parse_halt_feed(
                    by_role["OPERATIONAL_STATUS"]
                ),
                "cross_check_halt_feed": self._reference_client.parse_halt_feed(
                    by_role["TRADER_ALERT_HALT"]
                ),
            }
            attestation = screening_module.build_market_session_attestation(
                calendar,
                session_date,
                decision_at,
            )
            hold = evidence_module.DateRange(
                window.hold_sessions[0],
                window.hold_sessions[-1],
            )
            daily, previous_quotes, latest_quotes = cohorts
            contexts = []
            for record in enabled_records:
                bundle = evidence_release.by_symbol[record.symbol]
                evidence = evidence_module.classify_evidence(
                    bundle.records,
                    hold,
                    symbol=record.symbol,
                    issuer_cik=record.issuer_cik,
                    source_bindings=bundle.source_bindings,
                    as_of=decision_at,
                    subject_kind=bundle.subject_kind,
                    coverage_attestations=bundle.coverage_attestations,
                    reviewed_bundle=bundle,
                )
                status = classify_instrument_status(
                    record.symbol,
                    record.listing_venue,
                    snapshots,
                    as_of=terminal,
                )
                previous_values = previous_quotes[record.symbol]
                latest_values = latest_quotes[record.symbol]
                if len(latest_values) != 1:
                    raise CanonicalMaterialError(
                        "premarket runtime quote cohort is incomplete"
                    )
                contexts.append(
                    screening_module.CandidateContext(
                        record=record,
                        bars_by_symbol=daily,
                        previous_session_quote=(
                            None if not previous_values else previous_values[-1]
                        ),
                        latest_iex_quote=latest_values[0],
                        instrument_status=status,
                        evidence=evidence,
                        issuer_cik=record.issuer_cik,
                        initial_listing_date=record.initial_listing_date,
                        listing_date_status="VERIFIED",
                        session_date=session_date,
                        previous_session_date=window.previous_session,
                        as_of=decision_at,
                        operational_as_of=terminal,
                        hold_sessions=window.hold_sessions,
                        session_attestation=attestation,
                        market_calendar=calendar,
                    )
                )
        except CanonicalMaterialError:
            raise
        except (
            evidence_module.EvidenceUnavailableError,
            KeyError,
            TypeError,
            ValueError,
        ):
            return PremarketProviderCollection(
                collected_at=terminal,
                provider_cohorts=tuple(cohorts),
                reference_sources=tuple(documents),
                persisted_bindings=persisted,
                failure_reason="SOURCE_CHECK_FAILED",
            )

        return PremarketProviderCollection(
            collected_at=terminal,
            provider_cohorts=tuple(cohorts),
            reference_sources=tuple(documents),
            contexts=tuple(contexts),
            persisted_bindings=persisted,
        )


class PremarketRuntimeRiskResolver:
    """Resolve canonical breaker and primary-only portfolio authority."""

    __slots__ = (
        "__weakref__",
        "_bindings",
        "_lock",
        "_policy",
        "_project_root",
    )

    def __init__(self, *, policy: Policy, project_root: Path) -> None:
        if type(policy) is not Policy:
            raise CanonicalMaterialError(
                "premarket runtime resolver requires the fixed Policy"
        )
        policy.validate()
        self._policy = policy
        self._project_root = _project_root(project_root)
        self._lock = RLock()
        self._bindings: dict[int, _ValidationBreakerBinding] = {}

    @property
    def policy(self) -> Policy:
        return self._policy

    def validation_breaker(
        self,
        *,
        journal: Journal,
        session_date: date,
        decision_at: datetime,
        calendar: object,
        calendar_resolver: SessionCalendarResolver,
    ) -> object:
        """Issue and remember one exact Journal-owned validation breaker."""
        from . import journal as journal_module
        from . import risk as risk_module
        from .market_calendar import (
            MarketCalendar,
            is_release_verified_market_calendar,
        )

        decision_at = require_aware_timestamp(
            decision_at,
            "premarket decision time",
        )
        from . import provider_workflows as provider_workflows_module

        provider_workflows_module._validate_premarket_times(
            session_date,
            decision_at,
            decision_at,
        )
        if (
            type(journal) is not Journal
            or getattr(journal, "_closed", True)
            or type(calendar) is not MarketCalendar
            or not is_release_verified_market_calendar(calendar)
            or type(calendar_resolver) is not SessionCalendarResolver
            or not calendar_resolver.release_verified
            or not any(value is calendar for value in calendar_resolver.calendars)
            or not calendar_resolver.is_open(session_date)
        ):
            raise CanonicalMaterialError(
                "premarket validation breaker inputs are unavailable"
            )
        previous_session = calendar_resolver.previous_session(session_date)
        history = journal.read_phase1_breaker_history(
            ledger_name="CANONICAL",
            through_session=previous_session,
            query_cutoff=decision_at,
            calendar_resolver=calendar_resolver,
        )
        breaker = risk_module.evaluate_authorized_breakers(history)
        sources = risk_module._phase1_bound_sources(breaker)
        if (
            type(breaker) is not risk_module.BreakerState
            or not risk_module.is_issued_breaker_state(breaker)
            or breaker.ledger_name != "CANONICAL"
            or breaker.as_of != previous_session
            or len(sources) != 1
            or sources[0][1] != "BREAKER_HISTORY"
        ):
            raise CanonicalMaterialError(
                "premarket validation breaker authority is unavailable"
            )
        source = sources[0][0]
        source_candidate = journal_module._journal_any_source_authority_candidate(
            source
        )
        if (
            source_candidate is None
            or journal_module._current_journal_source_authority_owner(
                (source_candidate,)
            )
            is not journal
            or not journal_module._is_current_journal_authority_candidate_without_callbacks(
                source_candidate
            )
        ):
            raise CanonicalMaterialError(
                "premarket validation breaker has the wrong Journal owner"
            )
        calendar_digest = risk_module._calendar_digest(calendar_resolver)
        identity = id(breaker)
        resolver_reference = ref(self)

        def discard(dead: ReferenceType[object]) -> None:
            owner = resolver_reference()
            if owner is None:
                return
            with owner._lock:
                current = owner._bindings.get(identity)
                if current is not None and current.breaker_reference is dead:
                    owner._bindings.pop(identity, None)

        binding = _ValidationBreakerBinding(
            breaker_reference=ref(breaker, discard),
            journal_reference=ref(journal),
            session_date=session_date,
            decision_at=decision_at,
            calendar=calendar,
            calendar_resolver=calendar_resolver,
            calendar_digest=calendar_digest,
            history_source=source,
        )
        with self._lock:
            self._bindings[identity] = binding
        if not self._binding_is_current(breaker, binding):
            with self._lock:
                if self._bindings.get(identity) is binding:
                    self._bindings.pop(identity, None)
            raise CanonicalMaterialError(
                "premarket validation breaker changed during issuance"
            )
        return breaker

    @staticmethod
    def _binding_is_current(
        breaker: object,
        binding: _ValidationBreakerBinding,
    ) -> bool:
        from . import journal as journal_module
        from . import risk as risk_module

        journal = binding.journal_reference()
        if (
            binding.breaker_reference() is not breaker
            or journal is None
            or getattr(journal, "_closed", True)
            or not risk_module.is_issued_breaker_state(breaker)
            or risk_module._calendar_digest(binding.calendar_resolver)
            != binding.calendar_digest
        ):
            return False
        sources = risk_module._phase1_bound_sources(breaker)
        if (
            len(sources) != 1
            or sources[0][0] is not binding.history_source
            or sources[0][1] != "BREAKER_HISTORY"
        ):
            return False
        candidate = journal_module._journal_any_source_authority_candidate(
            binding.history_source
        )
        return bool(
            candidate is not None
            and journal_module._current_journal_source_authority_owner(
                (candidate,)
            )
            is journal
            and journal_module._is_current_journal_authority_candidate_without_callbacks(
                candidate
            )
        )

    def resolve(
        self,
        *,
        journal: Journal,
        session_date: date,
        decision_at: datetime,
        retrieved_at: datetime,
        calendar: object,
        universe: object,
        evidence_release: object,
        ranked_candidates: tuple[object, ...],
        validation_breaker: object,
    ) -> object:
        """Size only rank one from exact canonical Journal capacity."""
        from . import evidence as evidence_module
        from . import provider_workflows as provider_workflows_module
        from . import risk as risk_module
        from . import screening as screening_module
        from .market_calendar import MarketCalendar
        from .universe import UniverseSnapshot, is_verified_universe_snapshot

        decision_at = require_aware_timestamp(
            decision_at,
            "premarket decision time",
        )
        retrieved_at = require_aware_timestamp(
            retrieved_at,
            "premarket retrieval time",
        )
        provider_workflows_module._validate_premarket_times(
            session_date,
            decision_at,
            retrieved_at,
        )
        with self._lock:
            binding = self._bindings.get(id(validation_breaker))
        universe_identity = provider_workflows_module._reviewed_binding_identity(
            universe
        )
        if (
            type(binding) is not _ValidationBreakerBinding
            or binding.breaker_reference() is not validation_breaker
            or binding.journal_reference() is not journal
            or binding.session_date != session_date
            or binding.decision_at != decision_at
            or binding.calendar is not calendar
            or not self._binding_is_current(validation_breaker, binding)
            or type(calendar) is not MarketCalendar
            or type(universe) is not UniverseSnapshot
            or not is_verified_universe_snapshot(universe)
            or universe_identity is None
            or type(evidence_release)
            is not evidence_module.ReviewedEvidenceRelease
            or not evidence_module.is_verified_evidence_release(evidence_release)
            or evidence_release.universe_sha256 != universe_identity[1]
            or type(ranked_candidates) is not tuple
            or len(ranked_candidates) > 3
            or any(
                not screening_module.is_issued_scored_candidate(candidate)
                or candidate.publication_session != session_date
                for candidate in ranked_candidates
            )
        ):
            raise CanonicalMaterialError(
                "premarket risk resolution authority is unavailable"
            )
        ranked = screening_module.rank_candidates(ranked_candidates)
        if len(ranked) != len(ranked_candidates) or any(
            current is not expected
            for current, expected in zip(
                ranked,
                ranked_candidates,
                strict=True,
            )
        ):
            raise CanonicalMaterialError(
                "premarket ranked candidate authority is inconsistent"
            )
        if (
            risk_module.breaker_pauses_entry(validation_breaker, session_date)
            or not ranked
        ):
            return provider_workflows_module.PremarketRiskResolution(
                breaker_state=validation_breaker
            )

        request = risk_module.LongPlanRequest.from_scored_candidate(ranked[0])
        portfolio = journal.read_phase1_canonical_portfolio_authority(
            request=request,
            as_of=decision_at,
            calendar_resolver=binding.calendar_resolver,
            policy=self._policy,
        )
        breaker_states = getattr(portfolio.portfolio_state, "breaker_states", ())
        if (
            not risk_module.is_issued_portfolio_risk_authority(portfolio)
            or portfolio.request is not request
            or portfolio.as_of != decision_at
            or portfolio.scope != "CANONICAL_PUBLICATION"
            or len(breaker_states) != 1
            or not risk_module.is_issued_breaker_state(breaker_states[0])
        ):
            raise CanonicalMaterialError(
                "premarket canonical portfolio authority is unavailable"
            )
        portfolio_breaker = breaker_states[0]
        portfolio_sources = risk_module._phase1_bound_sources(portfolio_breaker)
        validation_source = binding.history_source
        if (
            len(portfolio_sources) != 1
            or portfolio_sources[0][1] != "BREAKER_HISTORY"
            or portfolio_sources[0][0].validation_window_id
            != validation_source.validation_window_id
            or portfolio_sources[0][0].source_digest
            != validation_source.source_digest
        ):
            raise CanonicalMaterialError(
                "premarket portfolio breaker conflicts with validation"
            )
        plan = risk_module.plan_long(
            request,
            portfolio.portfolio_state,
            self._policy,
            portfolio_authority=portfolio,
        )
        if not risk_module.is_issued_long_plan_decision(plan):
            raise CanonicalMaterialError(
                "premarket primary plan authority is unavailable"
            )
        if not plan.eligible:
            if not plan.reason_codes or not set(plan.reason_codes).issubset(
                provider_workflows_module._PRIMARY_CAPACITY_REASONS
            ):
                raise CanonicalMaterialError(
                    "premarket primary capacity outcome is unsupported"
                )
            return provider_workflows_module.PremarketRiskResolution(
                breaker_state=portfolio_breaker,
                capacity_decision=plan,
            )
        publication = screening_module._issue_portfolio_bound_publication_decision(
            ranked,
            primary_plan_decision=plan,
        )
        return provider_workflows_module.PremarketRiskResolution(
            breaker_state=portfolio_breaker,
            primary_plan=plan,
            publication_decision=publication,
        )


__all__ = [
    "PREMARKET_OPERATIONAL_REFERENCE_SOURCES",
    "PremarketRuntimeCollector",
    "PremarketRuntimeRiskResolver",
]
