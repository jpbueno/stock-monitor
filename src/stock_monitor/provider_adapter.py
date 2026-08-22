"""Shallow provider construction for canonical monitor workflows."""

from __future__ import annotations

from datetime import UTC as _UTC
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from urllib.parse import urlsplit as _urlsplit

from . import config as _config
from . import journal as _journal
from . import market_calendar as _market_calendar
from . import risk as _risk
from . import workflows as _workflows
from .providers import alpaca as _alpaca
from .providers import cache as _cache
from .providers import http as _http
from .providers import reference as _reference


def _utc_now() -> _datetime:
    """Return the actual UTC receipt time for provider calls."""

    return _datetime.now(_UTC)


def _aware_utc(value: object) -> _datetime:
    if type(value) is not _datetime or value.tzinfo is None:
        raise ValueError("provider adapter command time must be timezone-aware")
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError):
        offset = None
    if offset is None:
        raise ValueError("provider adapter command time must be timezone-aware")
    return value.astimezone(_UTC)


def _alpaca_host(url: object) -> str:
    if type(url) is not str:
        raise ValueError("Alpaca market-data URL must be text")
    try:
        host = _urlsplit(url).hostname
    except ValueError:
        host = None
    if host is None:
        raise ValueError("Alpaca market-data URL must contain a host")
    return host


def _release_calendar_resolver(
    project_root: object,
    session_date: _date,
) -> object:
    calendar = _market_calendar.load_current_market_calendar(
        project_root,
        as_of=session_date,
    )
    calendars = [calendar]
    remaining = 14
    candidate = session_date
    while remaining:
        candidate -= _timedelta(days=1)
        if candidate.year != session_date.year:
            calendars.insert(
                0,
                _market_calendar.load_current_market_calendar(
                    project_root,
                    as_of=candidate,
                ),
            )
            break
        if calendar.is_open(candidate):
            remaining -= 1
    return _risk.SessionCalendarResolver(tuple(calendars))


class ProviderWorkflowAdapter:
    """Bind configured read-only providers to canonical workflow coordinators."""

    __slots__ = (
        "__alpaca_market_data",
        "__cache",
        "__clock",
        "__egress_policy",
        "__http_client",
        "__journal",
        "__journal_path",
        "__opened_at",
        "__policy",
        "__project_root",
        "__reference_client",
        "__report_archive_root",
    )

    def __init__(self) -> None:
        raise TypeError("use ProviderWorkflowAdapter.open")

    def __setattr__(self, name: str, value: object) -> None:
        del name, value
        raise AttributeError("provider adapter bindings are immutable")

    def __delattr__(self, name: str) -> None:
        del name
        raise AttributeError("provider adapter bindings are immutable")

    @classmethod
    def open(
        cls,
        *,
        settings: _config.Settings,
        journal: _journal.Journal,
        now: _datetime,
    ) -> ProviderWorkflowAdapter:
        """Construct the provider graph without fetching or creating state."""

        if cls is not ProviderWorkflowAdapter:
            raise TypeError("provider adapter subclasses cannot be opened")
        if type(settings) is not _config.Settings:
            raise TypeError("provider adapter requires an exact Settings")
        if type(journal) is not _journal.Journal:
            raise TypeError("provider adapter requires an exact Journal")
        if journal._closed:
            raise ValueError("provider adapter requires an open Journal")
        if journal.path != settings.journal_path:
            raise ValueError("provider adapter Journal conflicts with Settings")
        opened_at = _aware_utc(now)
        clock = _utc_now

        content_cache = _cache.ContentCache(settings.cache_root)
        egress_policy = _http.EgressPolicy(
            (
                _alpaca_host(settings.sources.alpaca_market_data_url),
                *settings.sources.reference_hosts,
            )
        )
        http_client = _http.HttpGetClient(egress_policy)
        alpaca_market_data = _alpaca.AlpacaMarketData(
            http_client,
            _alpaca.AlpacaCredentials(
                key_id=settings.alpaca_api_key_id,
                secret_key=settings.alpaca_api_secret_key,
            ),
            base_url=settings.sources.alpaca_market_data_url,
            now=clock,
            cache=content_cache,
        )
        reference_client = _reference.ReferenceClient(
            http_client,
            egress_policy,
            allowed_urls=settings.sources.reference_urls,
            source_roles={
                source.url: source.role
                for source in settings.sources.reference_sources
            },
            cache=content_cache,
            now=clock,
        )

        adapter = object.__new__(cls)
        for name, value in (
            ("_ProviderWorkflowAdapter__alpaca_market_data", alpaca_market_data),
            ("_ProviderWorkflowAdapter__cache", content_cache),
            ("_ProviderWorkflowAdapter__clock", clock),
            ("_ProviderWorkflowAdapter__egress_policy", egress_policy),
            ("_ProviderWorkflowAdapter__http_client", http_client),
            ("_ProviderWorkflowAdapter__journal", journal),
            ("_ProviderWorkflowAdapter__journal_path", journal.path),
            ("_ProviderWorkflowAdapter__opened_at", opened_at),
            ("_ProviderWorkflowAdapter__policy", settings.policy),
            ("_ProviderWorkflowAdapter__project_root", settings.project_root),
            (
                "_ProviderWorkflowAdapter__reference_client",
                reference_client,
            ),
            (
                "_ProviderWorkflowAdapter__report_archive_root",
                settings.reports_root,
            ),
        ):
            object.__setattr__(adapter, name, value)
        return adapter

    def _require_journal(self) -> None:
        if (
            type(self.__journal) is not _journal.Journal
            or self.__journal._closed
            or self.__journal.path != self.__journal_path
        ):
            raise RuntimeError("provider adapter Journal is unavailable")

    def market_session(self, day: _date) -> _workflows.SessionWindow | None:
        """Return the release-pinned review window for one open session."""

        if type(day) is not _date:
            raise TypeError("market session day must be an exact date")
        try:
            calendar = _market_calendar.load_current_market_calendar(
                self.__project_root,
                as_of=day,
            )
            if not calendar.is_open(day):
                return None
            session = calendar.session(day)
        except _market_calendar.CalendarError as error:
            raise _workflows.WorkflowDataError("STALE_CALENDAR") from error
        return _workflows.SessionWindow(
            session_date=day,
            review_time=session.review_time,
        )

    def premarket_material(
        self,
        session_date: _date,
        *,
        decision_at: _datetime,
        retrieved_at: _datetime,
    ) -> object:
        """Lazily compose provider-backed premarket material."""

        from .premarket_runtime import (
            PremarketRuntimeCollector,
            PremarketRuntimeRiskResolver,
        )
        from .provider_workflows import PremarketWorkflowCoordinator

        self._require_journal()
        collector = PremarketRuntimeCollector(
            journal=self.__journal,
            project_root=self.__project_root,
            alpaca_market_data=self.__alpaca_market_data,
            reference_client=self.__reference_client,
            clock=self.__clock,
        )
        risk_resolver = PremarketRuntimeRiskResolver(
            policy=self.__policy,
            project_root=self.__project_root,
        )
        coordinator = PremarketWorkflowCoordinator(
            journal=self.__journal,
            project_root=self.__project_root,
            report_archive_root=self.__report_archive_root,
            collector=collector,
            risk_resolver=risk_resolver,
        )
        return coordinator.premarket_material(
            session_date,
            decision_at=decision_at,
            retrieved_at=retrieved_at,
        )

    def close_material(
        self,
        session_date: _date,
        *,
        review_at: _datetime,
        retrieved_at: _datetime,
    ) -> object:
        """Lazily compose provider-backed actual-close material."""

        from .actual_close_runtime import ActualCloseRuntimeCollector
        from .provider_workflows import ActualCloseWorkflowCoordinator

        self._require_journal()
        try:
            calendar_resolver = _release_calendar_resolver(
                self.__project_root,
                session_date,
            )
        except (_market_calendar.CalendarError, _risk.RiskBlock) as error:
            raise _workflows.WorkflowDataError("STALE_CALENDAR") from error
        plans = self.__journal.phase1_signal_plan_resolver()
        collector = ActualCloseRuntimeCollector(
            alpaca_market_data=self.__alpaca_market_data,
            reference_client=self.__reference_client,
            calendar_resolver=calendar_resolver,
            policy=self.__policy,
            plans=plans,
            clock=self.__clock,
        )
        coordinator = ActualCloseWorkflowCoordinator(
            journal=self.__journal,
            report_archive_root=self.__report_archive_root,
            source_collector=collector,
            calendar_resolver=calendar_resolver,
            policy=self.__policy,
            plans=plans,
        )
        return coordinator.close_material(
            session_date,
            review_at=review_at,
            retrieved_at=retrieved_at,
        )


__all__ = ["ProviderWorkflowAdapter"]
