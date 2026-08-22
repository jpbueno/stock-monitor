"""Concrete GET-only source collector for canonical ACTUAL close reviews."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re
from types import MappingProxyType

from .domain import require_aware_timestamp
from .journal import (
    ActualCloseFailureBinding,
    ActualCloseReceiptBinding,
    Journal,
    JournalError,
    Phase1SignalPlanResolver,
)
from .policy import Policy
from .provider_workflows import (
    ActualCloseCollection,
    CanonicalMaterialError,
)
from .providers.alpaca import (
    AlpacaMarketData,
    ProviderFetchBundle,
    ProviderFetchCohort,
    ProviderFetchPageBundle,
    TimeWindow,
    is_issued_provider_fetch_cohort,
    is_issued_provider_fetch_page_bundle,
    read_provider_fetch_bundle,
)
from .providers.http import NetworkPolicyError, ProviderResponseError
from .providers.reference import ReferenceClient
from .risk import RiskBlock, SessionCalendarResolver


ACTUAL_CLOSE_REFERENCE_SOURCES: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
            "PRIMARY_HALT_FEED",
            "Nasdaq",
        ),
        "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
            "TRADER_ALERT_HALT",
            "Nasdaq",
        ),
        "https://www.nyse.com/api/notifications/public/alerts?2=3": (
            "OPERATIONAL_STATUS",
            "New York Stock Exchange",
        ),
        "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar": (
            "CROSS_CHECK_CALENDAR",
            "Nasdaq",
        ),
    }
)


_MARKET_SOURCE_TYPES: Mapping[str, str] = MappingProxyType(
    {
        "SIP_DAILY_BAR": "ALPACA_DAILY_BARS",
        "SIP_MINUTE_BAR": "ALPACA_INTRADAY_BARS",
        "SIP_QUOTE": "ALPACA_HISTORICAL_QUOTES",
        "IEX_FRESHNESS": "ALPACA_LATEST_QUOTES",
    }
)


_SYMBOL = re.compile(r"[A-Z][A-Z0-9.]{0,9}\Z")


@dataclass(frozen=True, slots=True)
class _MarketAttempt:
    row_ids: tuple[int, ...]
    pages: tuple[ProviderFetchPageBundle, ...]
    cohort: ProviderFetchCohort


def _persist_provider_page(
    journal: Journal,
    page_bundle: ProviderFetchPageBundle,
) -> int:
    """Commit one exact Alpaca page before its request generator can advance."""
    if (
        type(page_bundle) is not ProviderFetchPageBundle
        or not is_issued_provider_fetch_page_bundle(page_bundle)
        or page_bundle.page.source_type not in set(_MARKET_SOURCE_TYPES.values())
    ):
        raise CanonicalMaterialError(
            "actual close provider page authority is unavailable"
        )
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
        provider_sequence=None,
        delay_seconds=observation.delay_seconds,
        health_result="OK",
        details={"source_observation_id": page.source_observation_id},
    )
    return receipt.row_id


def _persist_reference_document(
    *,
    journal: Journal,
    reference_client: ReferenceClient,
    document: object,
    role: str,
    provider: str,
) -> int:
    from .providers.cache import SourceDocument

    if (
        type(document) is not SourceDocument
        or document.source_role != role
        or document.publisher != provider
        or document.source_type != "OFFICIAL_REFERENCE"
    ):
        raise CanonicalMaterialError(
            "actual close reference document identity is invalid"
        )
    reference_client.health_attestation(document)
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
        details={
            "accession": None,
            "issuer_cik": None,
            "source_observation_id": document.source_observation_id,
            "source_role": role,
            "symbol": None,
            "timestamp_source": document.timestamp_source,
        },
    )
    return receipt.row_id


class ActualCloseRuntimeCollector:
    """Collect exact close-review inputs through read-only provider clients."""

    __slots__ = (
        "_alpaca_market_data",
        "_calendar_resolver",
        "_clock",
        "_plans",
        "_policy",
        "_reference_client",
    )

    def __init__(
        self,
        *,
        alpaca_market_data: AlpacaMarketData,
        reference_client: ReferenceClient,
        calendar_resolver: SessionCalendarResolver,
        policy: Policy,
        plans: Phase1SignalPlanResolver,
        clock: Callable[[], datetime],
    ) -> None:
        if (
            type(alpaca_market_data) is not AlpacaMarketData
            or getattr(alpaca_market_data, "_base_url", None)
            != "https://data.alpaca.markets"
            or getattr(
                getattr(alpaca_market_data, "_policy", None),
                "allowed_hosts",
                None,
            )
            != frozenset({"data.alpaca.markets"})
        ):
            raise CanonicalMaterialError(
                "actual close runtime Alpaca client scope is invalid"
            )
        configured_roles = getattr(reference_client, "_source_roles", None)
        if (
            type(reference_client) is not ReferenceClient
            or not isinstance(configured_roles, Mapping)
            or any(
                configured_roles.get(url) != role
                for url, (role, _provider) in ACTUAL_CLOSE_REFERENCE_SOURCES.items()
            )
        ):
            raise CanonicalMaterialError(
                "actual close runtime reference client scope is invalid"
            )
        if (
            type(calendar_resolver) is not SessionCalendarResolver
            or not calendar_resolver.release_verified
        ):
            raise CanonicalMaterialError(
                "actual close runtime calendar authority is invalid"
            )
        if type(policy) is not Policy:
            raise CanonicalMaterialError(
                "actual close runtime policy is invalid"
            )
        policy.validate()
        if (
            type(plans) is not Phase1SignalPlanResolver
            or type(plans.journal) is not Journal
            or getattr(plans.journal, "_closed", True)
        ):
            raise CanonicalMaterialError(
                "actual close runtime plan resolver is invalid"
            )
        if not callable(clock):
            raise CanonicalMaterialError(
                "actual close runtime clock is unavailable"
            )
        self._alpaca_market_data = alpaca_market_data
        self._reference_client = reference_client
        self._calendar_resolver = calendar_resolver
        self._policy = policy
        self._plans = plans
        self._clock = clock

    @property
    def plans(self) -> Phase1SignalPlanResolver:
        return self._plans

    def _collect_market_role(
        self,
        *,
        journal: Journal,
        symbol: str,
        role: str,
        request: Callable[[Callable[[ProviderFetchPageBundle], None]], object],
    ) -> _MarketAttempt | None:
        row_ids: list[int] = []
        pages: list[ProviderFetchPageBundle] = []
        sink_error: list[Exception] = []

        def page_sink(page: ProviderFetchPageBundle) -> None:
            try:
                row_id = _persist_provider_page(journal, page)
            except Exception as error:
                sink_error.append(error)
                raise
            row_ids.append(row_id)
            pages.append(page)

        try:
            cohort = request(page_sink)
        except (NetworkPolicyError, ProviderResponseError):
            if sink_error:
                raise CanonicalMaterialError(
                    "actual close provider page could not be persisted"
                ) from sink_error[0]
            return None
        if sink_error:
            raise CanonicalMaterialError(
                "actual close provider page could not be persisted"
            ) from sink_error[0]
        try:
            disclosure = read_provider_fetch_bundle(cohort)
        except (TypeError, ValueError):
            return None
        if (
            type(disclosure) is not ProviderFetchBundle
            or type(cohort) is not ProviderFetchCohort
            or not is_issued_provider_fetch_cohort(cohort)
            or tuple(cohort) != (symbol,)
            or not pages
            or len(disclosure.pages) != len(pages)
            or any(
                current is not expected
                for current, expected in zip(
                    disclosure.pages,
                    pages,
                    strict=True,
                )
            )
            or any(
                page.page.source_type != _MARKET_SOURCE_TYPES[role]
                for page in pages
            )
        ):
            return None
        return _MarketAttempt(tuple(row_ids), tuple(pages), cohort)

    def _collect_references(
        self,
        *,
        journal: Journal,
        symbols: tuple[str, ...],
    ) -> tuple[
        dict[tuple[str | None, str], tuple[int, ...]],
        set[tuple[str | None, str]],
    ]:
        successes: dict[tuple[str | None, str], tuple[int, ...]] = {}
        failures: set[tuple[str | None, str]] = set()
        symbol_set = set(symbols)
        for url, (role, provider) in ACTUAL_CLOSE_REFERENCE_SOURCES.items():
            scope = (None, role)
            try:
                document = self._reference_client.fetch(
                    url,
                    role=role,
                    publisher=provider,
                )
                row_id = _persist_reference_document(
                    journal=journal,
                    reference_client=self._reference_client,
                    document=document,
                    role=role,
                    provider=provider,
                )
                safe = True
                if role in {"PRIMARY_HALT_FEED", "TRADER_ALERT_HALT"}:
                    snapshot = self._reference_client.parse_halt_feed(document)
                    safe = bool(
                        snapshot.healthy
                        and snapshot.supported
                        and snapshot.pagination_complete
                        and not symbol_set.intersection(snapshot.halted_symbols)
                        and (
                            role != "PRIMARY_HALT_FEED"
                            or snapshot.coverage == "COMPLETE_ACTIVE_HALTS"
                        )
                    )
                elif role == "OPERATIONAL_STATUS":
                    halt = self._reference_client.parse_halt_feed(document)
                    status = self._reference_client.parse_status(document)
                    safe = bool(
                        halt.healthy
                        and halt.supported
                        and halt.pagination_complete
                        and halt.coverage == "COMPLETE_ACTIVE_HALTS"
                        and status.healthy
                        and status.supported
                        and status.status == "OPERATIONAL"
                    )
                if safe:
                    successes[scope] = (row_id,)
                else:
                    failures.add(scope)
            except CanonicalMaterialError:
                raise
            except (NetworkPolicyError, ProviderResponseError, ValueError):
                failures.add(scope)
        return successes, failures

    def _event_evidence_rows(
        self,
        *,
        journal: Journal,
        symbols: tuple[str, ...],
        review_at: datetime,
        query_cutoff: datetime,
    ) -> tuple[
        dict[tuple[str | None, str], tuple[int, ...]],
        set[tuple[str | None, str]],
    ]:
        from .reconciliation import replay_actual

        successes: dict[tuple[str | None, str], tuple[int, ...]] = {}
        failures: set[tuple[str | None, str]] = set()
        try:
            with journal.transaction() as transaction:
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
                "actual close terminal replay is unavailable"
            ) from error
        for symbol in symbols:
            scope = (symbol, "EVENT_EVIDENCE")
            try:
                resolution = journal.resolve_actual_position_plan_source(
                    actual_replay_source=replay_source,
                    actual_position_state=actual_state,
                    symbol=symbol,
                    query_cutoff=query_cutoff,
                )
                if resolution.status != "RESOLVED" or resolution.source is None:
                    failures.add(scope)
                    continue
                plan_source = resolution.source
                evidence_source = journal.read_phase1_signal_evidence_source(
                    plan_source.signal_source.signal_id,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                    calendar_resolver=self._calendar_resolver,
                    exact_signal_source=plan_source.signal_source,
                )
                row_ids = tuple(evidence_source.source_observation_row_ids)
                if not row_ids or len(row_ids) != len(set(row_ids)):
                    failures.add(scope)
                    continue
                successes[scope] = row_ids
            except (JournalError, RiskBlock, ValueError):
                failures.add(scope)
        return successes, failures

    def collect_close_sources(
        self,
        *,
        journal: object,
        symbols: tuple[str, ...],
        session_date: date,
        review_at: datetime,
        mark_cutoff: datetime,
        command_started_at: datetime,
    ) -> ActualCloseCollection:
        """Persist one receipt-or-failure result for every close-review role."""
        if (
            type(journal) is not Journal
            or getattr(journal, "_closed", True)
            or self._plans.journal is not journal
            or type(symbols) is not tuple
            or any(
                type(symbol) is not str
                or _SYMBOL.fullmatch(symbol) is None
                for symbol in symbols
            )
            or tuple(sorted(set(symbols))) != symbols
            or type(session_date) is not date
        ):
            raise CanonicalMaterialError(
                "actual close runtime Journal or symbol scope is invalid"
            )
        review_at = require_aware_timestamp(review_at, "actual close review time")
        mark_cutoff = require_aware_timestamp(
            mark_cutoff,
            "actual close mark cutoff",
        )
        command_started_at = require_aware_timestamp(
            command_started_at,
            "actual close command start",
        )
        try:
            schedule = self._calendar_resolver.session(session_date)
            expected_review = datetime.combine(
                session_date,
                schedule.review_time,
                schedule.timezone,
            )
            session_open = datetime.combine(
                session_date,
                schedule.open_time,
                schedule.timezone,
            )
            session_close = datetime.combine(
                session_date,
                schedule.close_time,
                schedule.timezone,
            )
            expected_mark = min(review_at - timedelta(minutes=16), session_close)
            previous_sessions = [
                self._calendar_resolver.previous_session(session_date)
            ]
            while len(previous_sessions) < 14:
                previous_sessions.append(
                    self._calendar_resolver.previous_session(
                        previous_sessions[-1]
                    )
                )
            previous_sessions.reverse()
            first_schedule = self._calendar_resolver.session(
                previous_sessions[0]
            )
            last_schedule = self._calendar_resolver.session(
                previous_sessions[-1]
            )
        except RiskBlock as error:
            raise CanonicalMaterialError(
                "actual close runtime calendar coverage is unavailable"
            ) from error
        if (
            review_at != expected_review
            or mark_cutoff != expected_mark
            or command_started_at < review_at
            or command_started_at.astimezone(schedule.timezone).date()
            != session_date
        ):
            raise CanonicalMaterialError(
                "actual close runtime timing is inconsistent"
            )
        daily_window = TimeWindow(
            datetime.combine(
                previous_sessions[0],
                first_schedule.open_time,
                first_schedule.timezone,
            ),
            datetime.combine(
                previous_sessions[-1],
                last_schedule.close_time,
                last_schedule.timezone,
            ),
        )
        minute_window = TimeWindow(session_open, mark_cutoff)
        quote_window = TimeWindow(mark_cutoff - timedelta(minutes=5), mark_cutoff)

        successful_rows: dict[
            tuple[str | None, str], tuple[int, ...]
        ] = {}
        failed_scopes: set[tuple[str | None, str]] = set()
        iex_attempts: dict[str, _MarketAttempt] = {}
        for symbol in symbols:
            requests = (
                (
                    "SIP_DAILY_BAR",
                    lambda sink, symbol=symbol: self._alpaca_market_data.daily_bars(
                        (symbol,), daily_window, page_sink=sink
                    ),
                ),
                (
                    "SIP_MINUTE_BAR",
                    lambda sink, symbol=symbol: (
                        self._alpaca_market_data.historical_minute_bars(
                            (symbol,), minute_window, page_sink=sink
                        )
                    ),
                ),
                (
                    "SIP_QUOTE",
                    lambda sink, symbol=symbol: (
                        self._alpaca_market_data.historical_quotes(
                            (symbol,), quote_window, page_sink=sink
                        )
                    ),
                ),
            )
            for role, request in requests:
                attempt = self._collect_market_role(
                    journal=journal,
                    symbol=symbol,
                    role=role,
                    request=request,
                )
                scope = (symbol, role)
                if attempt is None:
                    failed_scopes.add(scope)
                else:
                    successful_rows[scope] = attempt.row_ids

        reference_rows, reference_failures = self._collect_references(
            journal=journal,
            symbols=symbols,
        )
        successful_rows.update(reference_rows)
        failed_scopes.update(reference_failures)

        for symbol in symbols:
            attempt = self._collect_market_role(
                journal=journal,
                symbol=symbol,
                role="IEX_FRESHNESS",
                request=lambda sink, symbol=symbol: (
                    self._alpaca_market_data.latest_iex_quote_cohort(
                        (symbol,), page_sink=sink
                    )
                ),
            )
            scope = (symbol, "IEX_FRESHNESS")
            if attempt is None:
                failed_scopes.add(scope)
            else:
                successful_rows[scope] = attempt.row_ids
                iex_attempts[symbol] = attempt

        collected_at = require_aware_timestamp(
            self._clock(),
            "actual close terminal collection time",
        )
        if (
            collected_at < command_started_at
            or collected_at.astimezone(schedule.timezone).date() != session_date
        ):
            raise CanonicalMaterialError(
                "actual close terminal collection time is inconsistent"
            )
        for symbol, attempt in iex_attempts.items():
            values = attempt.cohort[symbol]
            if (
                len(values) != 1
                or collected_at - values[0].timestamp > timedelta(minutes=5)
                or values[0].timestamp > collected_at
            ):
                scope = (symbol, "IEX_FRESHNESS")
                successful_rows.pop(scope, None)
                failed_scopes.add(scope)

        event_rows, event_failures = self._event_evidence_rows(
            journal=journal,
            symbols=symbols,
            review_at=review_at,
            query_cutoff=collected_at,
        )
        successful_rows.update(event_rows)
        failed_scopes.update(event_failures)

        expected_scopes = {
            *((symbol, role) for symbol in symbols for role in (
                "SIP_DAILY_BAR",
                "SIP_MINUTE_BAR",
                "SIP_QUOTE",
                "IEX_FRESHNESS",
                "EVENT_EVIDENCE",
            )),
            *((None, role) for role, _provider in (
                ACTUAL_CLOSE_REFERENCE_SOURCES.values()
            )),
        }
        if (
            set(successful_rows).intersection(failed_scopes)
            or set(successful_rows).union(failed_scopes) != expected_scopes
        ):
            raise CanonicalMaterialError(
                "actual close scoped source results are incomplete"
            )
        row_scopes = tuple(
            (scope, row_id)
            for scope, row_ids in successful_rows.items()
            for row_id in row_ids
        )
        if len({row_id for _scope, row_id in row_scopes}) != len(row_scopes):
            raise CanonicalMaterialError(
                "actual close receipt rows cannot be reused across scopes"
            )
        receipt_by_id = {
            receipt.row_id: receipt
            for receipt in journal.read_source_observation_receipts(
                tuple(row_id for _scope, row_id in row_scopes)
            )
        }
        if len(receipt_by_id) != len(row_scopes) or any(
            receipt.retrieved_at > collected_at
            for receipt in receipt_by_id.values()
        ):
            raise CanonicalMaterialError(
                "actual close terminal time predates its source receipts"
            )
        receipt_bindings = tuple(
            ActualCloseReceiptBinding(
                scope[0],
                scope[1],
                receipt_by_id[row_id],
            )
            for scope, row_id in row_scopes
        )
        failure_bindings = tuple(
            ActualCloseFailureBinding(
                symbol,
                role,
                "SOURCE_UNAVAILABLE",
                collected_at,
            )
            for symbol, role in sorted(
                failed_scopes,
                key=lambda scope: ("" if scope[0] is None else scope[0], scope[1]),
            )
        )
        try:
            review = journal.append_actual_close_review(
                session_date=session_date,
                review_at=review_at,
                mark_cutoff=mark_cutoff,
                query_cutoff=collected_at,
                retrieved_at=collected_at,
                receipt_bindings=receipt_bindings,
                failure_bindings=failure_bindings,
            )
        except JournalError as error:
            raise CanonicalMaterialError(
                "actual close durable review could not be appended"
            ) from error
        return ActualCloseCollection(
            review_id=review.review_id,
            collected_at=collected_at,
        )


__all__ = ["ActualCloseRuntimeCollector"]
