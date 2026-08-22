"""Integration contract for the shallow provider workflow adapter."""

from __future__ import annotations

import socket
import sys
import tempfile
import types
import unittest
from contextlib import ExitStack
from datetime import UTC, date, datetime, time
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import stock_monitor.provider_adapter as provider_adapter_module
import stock_monitor.provider_workflows as provider_workflows_module
from stock_monitor.config import Settings, load_settings
from stock_monitor.journal import Journal, Phase1SignalPlanResolver
from stock_monitor.market_calendar import CalendarError
from stock_monitor.provider_adapter import ProviderWorkflowAdapter
from stock_monitor.providers.alpaca import AlpacaMarketData
from stock_monitor.providers.cache import ContentCache
from stock_monitor.providers.http import EgressPolicy, HttpGetClient
from stock_monitor.providers.reference import ReferenceClient
from stock_monitor.risk import SessionCalendarResolver
from stock_monitor.workflows import SessionWindow, WorkflowDataError


ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")
OPENED_AT = datetime(2026, 8, 21, 8, 40, tzinfo=ET)


class _RuntimeDouble:
    instances: list[_RuntimeDouble]

    def __init_subclass__(cls) -> None:
        cls.instances = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        type(self).instances.append(self)


class _PremarketCollector(_RuntimeDouble):
    pass


class _PremarketRiskResolver(_RuntimeDouble):
    pass


class _ActualCloseCollector(_RuntimeDouble):
    pass


class _CoordinatorDouble(_RuntimeDouble):
    result = object()
    calls: list[tuple[tuple[object, ...], dict[str, object]]]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.calls = []
        cls.result = object()


class _PremarketCoordinator(_CoordinatorDouble):
    def premarket_material(self, *args: object, **kwargs: object) -> object:
        type(self).calls.append((args, kwargs))
        return type(self).result


class _ActualCloseCoordinator(_CoordinatorDouble):
    def close_material(self, *args: object, **kwargs: object) -> object:
        type(self).calls.append((args, kwargs))
        return type(self).result


class _Calendar:
    def __init__(
        self,
        year: int,
        *,
        open_days: frozenset[date] | None = None,
        review_time: time = time(15, 30),
    ) -> None:
        self.year = year
        self._open_days = open_days
        self._review_time = review_time

    def is_open(self, day: date) -> bool:
        if day.year != self.year:
            raise CalendarError("calendar year mismatch")
        if self._open_days is not None:
            return day in self._open_days
        return day.weekday() < 5

    def session(self, day: date) -> object:
        if not self.is_open(day):
            raise CalendarError("closed")
        return types.SimpleNamespace(review_time=self._review_time)


class _Resolver:
    instances: list[_Resolver] = []

    def __init__(self, calendars: tuple[object, ...]) -> None:
        self.calendars = tuple(calendars)
        self.release_verified = True
        type(self).instances.append(self)


class ProviderWorkflowAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.operator_root = Path(temporary.name)
        self.settings = load_settings(
            ROOT,
            {
                "APCA_API_KEY_ID": "paper-key-id",
                "APCA_API_SECRET_KEY": "paper-secret-key",
                "SEC_USER_AGENT": "Stock Monitor operator@example.com",
                "STOCK_MONITOR_HOME": str(self.operator_root),
            },
        )
        self.settings.reports_root.mkdir(parents=True)
        self.journal = Journal.open(self.settings.journal_path)
        self.addCleanup(self.journal.close)
        for runtime in (
            _PremarketCollector,
            _PremarketRiskResolver,
            _ActualCloseCollector,
            _PremarketCoordinator,
            _ActualCloseCoordinator,
        ):
            runtime.instances.clear()
        _PremarketCoordinator.calls.clear()
        _ActualCloseCoordinator.calls.clear()
        _Resolver.instances.clear()

    @staticmethod
    def _private(adapter: ProviderWorkflowAdapter, name: str) -> object:
        return getattr(adapter, f"_ProviderWorkflowAdapter__{name}")

    @staticmethod
    def _files(root: Path) -> dict[str, tuple[int, int]]:
        return {
            str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
            for path in root.rglob("*")
            if path.is_file()
        }

    @staticmethod
    def _runtime_modules(stack: ExitStack) -> None:
        premarket = types.ModuleType("stock_monitor.premarket_runtime")
        premarket.PremarketRuntimeCollector = _PremarketCollector
        premarket.PremarketRuntimeRiskResolver = _PremarketRiskResolver
        actual_close = types.ModuleType("stock_monitor.actual_close_runtime")
        actual_close.ActualCloseRuntimeCollector = _ActualCloseCollector
        stack.enter_context(
            mock.patch.dict(
                sys.modules,
                {
                    premarket.__name__: premarket,
                    actual_close.__name__: actual_close,
                },
            )
        )

    def _open(self) -> ProviderWorkflowAdapter:
        return ProviderWorkflowAdapter.open(
            settings=self.settings,
            journal=self.journal,
            now=OPENED_AT,
        )

    def test_open_constructs_exact_shared_get_only_topology_without_io(self) -> None:
        observed = datetime(2026, 8, 21, 12, 40, tzinfo=UTC)

        def clock() -> datetime:
            return observed

        before = self._files(self.operator_root)
        with (
            mock.patch.object(
                Path,
                "mkdir",
                side_effect=AssertionError("open attempted a filesystem write"),
            ) as mkdir,
            mock.patch.object(
                Path,
                "write_bytes",
                side_effect=AssertionError("open attempted a filesystem write"),
            ) as write_bytes,
            mock.patch.object(
                Path,
                "write_text",
                side_effect=AssertionError("open attempted a filesystem write"),
            ) as write_text,
            mock.patch.object(
                HttpGetClient,
                "get",
                autospec=True,
                side_effect=AssertionError("open attempted a GET"),
            ) as get,
            mock.patch.object(
                socket,
                "create_connection",
                side_effect=AssertionError("open attempted network access"),
            ) as connect,
            mock.patch.object(provider_adapter_module, "_utc_now", clock),
            mock.patch.dict(
                sys.modules,
                {
                    "stock_monitor.premarket_runtime": None,
                    "stock_monitor.actual_close_runtime": None,
                },
            ),
        ):
            adapter = self._open()

        self.assertEqual(self._files(self.operator_root), before)
        mkdir.assert_not_called()
        write_bytes.assert_not_called()
        write_text.assert_not_called()
        get.assert_not_called()
        connect.assert_not_called()

        cache = self._private(adapter, "cache")
        policy = self._private(adapter, "egress_policy")
        transport = self._private(adapter, "http_client")
        alpaca = self._private(adapter, "alpaca_market_data")
        references = self._private(adapter, "reference_client")
        self.assertIs(type(cache), ContentCache)
        self.assertEqual(cache.root, self.settings.cache_root)
        self.assertIs(type(policy), EgressPolicy)
        self.assertEqual(
            policy.allowed_hosts,
            frozenset(
                ("data.alpaca.markets", *self.settings.sources.reference_hosts)
            ),
        )
        self.assertIs(type(transport), HttpGetClient)
        self.assertIs(transport._policy, policy)
        self.assertIs(type(alpaca), AlpacaMarketData)
        self.assertIs(alpaca._transport, transport)
        self.assertEqual(
            alpaca._base_url,
            self.settings.sources.alpaca_market_data_url,
        )
        self.assertIs(alpaca._cache, cache)
        self.assertIs(alpaca._now, clock)
        self.assertIs(type(references), ReferenceClient)
        self.assertIs(references._transport, transport)
        self.assertIs(references._policy, policy)
        self.assertIs(references._cache, cache)
        self.assertIs(references._now, clock)
        self.assertEqual(
            references._allowed_urls,
            frozenset(self.settings.sources.reference_urls),
        )
        self.assertEqual(
            references._source_roles,
            {
                source.url: source.role
                for source in self.settings.sources.reference_sources
            },
        )
        self.assertEqual(
            self._private(adapter, "opened_at"),
            OPENED_AT.astimezone(UTC),
        )
        self.assertIs(self._private(adapter, "clock"), clock)

    def test_open_rejects_wrong_closed_and_cross_journal_inputs(self) -> None:
        with self.assertRaises(TypeError):
            ProviderWorkflowAdapter.open(self.settings, self.journal, OPENED_AT)
        with self.assertRaises(TypeError):
            ProviderWorkflowAdapter.open(
                settings=object(), journal=self.journal, now=OPENED_AT
            )
        with self.assertRaises(TypeError):
            ProviderWorkflowAdapter.open(
                settings=self.settings, journal=object(), now=OPENED_AT
            )
        with self.assertRaises(ValueError):
            ProviderWorkflowAdapter.open(
                settings=self.settings,
                journal=self.journal,
                now=datetime(2026, 8, 21, 8, 40),
            )

        closed = Journal.open(self.operator_root / "closed.sqlite3")
        closed.close()
        with self.assertRaises(ValueError):
            ProviderWorkflowAdapter.open(
                settings=self.settings, journal=closed, now=OPENED_AT
            )

        other = Journal.open(self.operator_root / "other.sqlite3")
        self.addCleanup(other.close)
        with self.assertRaises(ValueError):
            ProviderWorkflowAdapter.open(
                settings=self.settings, journal=other, now=OPENED_AT
            )

    def test_market_session_returns_none_for_a_reviewed_closed_day(self) -> None:
        day = date(2026, 8, 22)
        adapter = self._open()
        calendar = _Calendar(2026, open_days=frozenset())
        with mock.patch.object(
            provider_adapter_module._market_calendar,
            "load_current_market_calendar",
            return_value=calendar,
        ) as load:
            self.assertIsNone(adapter.market_session(day))
        load.assert_called_once_with(self.settings.project_root, as_of=day)

    def test_market_session_returns_the_exact_review_window(self) -> None:
        day = date(2026, 8, 21)
        adapter = self._open()
        calendar = _Calendar(
            2026,
            open_days=frozenset({day}),
            review_time=time(12, 30),
        )
        with mock.patch.object(
            provider_adapter_module._market_calendar,
            "load_current_market_calendar",
            return_value=calendar,
        ):
            result = adapter.market_session(day)
        self.assertIs(type(result), SessionWindow)
        self.assertEqual(result, SessionWindow(day, time(12, 30)))

    def test_market_session_normalizes_all_calendar_errors(self) -> None:
        adapter = self._open()
        day = date(2026, 8, 21)
        for failure in (
            mock.Mock(side_effect=CalendarError("stale release")),
            mock.Mock(return_value=_Calendar(2026, open_days=frozenset())),
        ):
            if failure.return_value is not mock.DEFAULT:
                failure.return_value.is_open = mock.Mock(
                    side_effect=CalendarError("invalid session")
                )
            with (
                self.subTest(failure=failure.side_effect),
                mock.patch.object(
                    provider_adapter_module._market_calendar,
                    "load_current_market_calendar",
                    failure,
                ),
                self.assertRaisesRegex(WorkflowDataError, "^STALE_CALENDAR$") as raised,
            ):
                adapter.market_session(day)
            self.assertIsInstance(raised.exception.__cause__, CalendarError)

    def test_premarket_dispatches_exact_private_dependencies_without_get(self) -> None:
        adapter = self._open()
        session_date = date(2026, 8, 21)
        decision_at = datetime(2026, 8, 21, 8, 45, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 21, 8, 49, tzinfo=ET)
        with ExitStack() as stack:
            self._runtime_modules(stack)
            stack.enter_context(
                mock.patch.object(
                    provider_workflows_module,
                    "PremarketWorkflowCoordinator",
                    _PremarketCoordinator,
                )
            )
            get = stack.enter_context(
                mock.patch.object(
                    HttpGetClient,
                    "get",
                    autospec=True,
                    side_effect=AssertionError("constructor performed GET"),
                )
            )
            result = adapter.premarket_material(
                session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
            )

        self.assertIs(result, _PremarketCoordinator.result)
        get.assert_not_called()
        collector = _PremarketCollector.instances[-1]
        risk = _PremarketRiskResolver.instances[-1]
        self.assertEqual(
            collector.kwargs,
            {
                "journal": self.journal,
                "project_root": self.settings.project_root,
                "alpaca_market_data": self._private(adapter, "alpaca_market_data"),
                "reference_client": self._private(adapter, "reference_client"),
                "clock": self._private(adapter, "clock"),
            },
        )
        self.assertEqual(
            risk.kwargs,
            {
                "policy": self.settings.policy,
                "project_root": self.settings.project_root,
            },
        )
        coordinator = _PremarketCoordinator.instances[-1]
        self.assertEqual(
            coordinator.kwargs,
            {
                "journal": self.journal,
                "project_root": self.settings.project_root,
                "report_archive_root": self.settings.reports_root,
                "collector": collector,
                "risk_resolver": risk,
            },
        )
        self.assertEqual(
            _PremarketCoordinator.calls,
            [
                (
                    (session_date,),
                    {"decision_at": decision_at, "retrieved_at": retrieved_at},
                )
            ],
        )
        self.assertNotIn("validation_window_id", coordinator.kwargs)

    def test_close_dispatches_release_verified_resolver_and_exact_dependencies(
        self,
    ) -> None:
        adapter = self._open()
        session_date = date(2026, 8, 21)
        review_at = datetime(2026, 8, 21, 15, 30, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 21, 15, 31, tzinfo=ET)
        with ExitStack() as stack:
            self._runtime_modules(stack)
            stack.enter_context(
                mock.patch.object(
                    provider_workflows_module,
                    "ActualCloseWorkflowCoordinator",
                    _ActualCloseCoordinator,
                    create=True,
                )
            )
            get = stack.enter_context(
                mock.patch.object(
                    HttpGetClient,
                    "get",
                    autospec=True,
                    side_effect=AssertionError("constructor performed GET"),
                )
            )
            result = adapter.close_material(
                session_date,
                review_at=review_at,
                retrieved_at=retrieved_at,
            )

        self.assertIs(result, _ActualCloseCoordinator.result)
        get.assert_not_called()
        collector = _ActualCloseCollector.instances[-1]
        resolver = collector.kwargs["calendar_resolver"]
        self.assertIs(type(resolver), SessionCalendarResolver)
        self.assertTrue(resolver.release_verified)
        self.assertEqual(tuple(calendar.year for calendar in resolver.calendars), (2026,))
        plans = collector.kwargs["plans"]
        self.assertIs(type(plans), Phase1SignalPlanResolver)
        self.assertIs(plans.journal, self.journal)
        self.assertIs(collector.kwargs["policy"], self.settings.policy)
        self.assertEqual(
            collector.kwargs,
            {
                "alpaca_market_data": self._private(adapter, "alpaca_market_data"),
                "reference_client": self._private(adapter, "reference_client"),
                "calendar_resolver": resolver,
                "policy": self.settings.policy,
                "plans": plans,
                "clock": self._private(adapter, "clock"),
            },
        )
        coordinator = _ActualCloseCoordinator.instances[-1]
        self.assertIs(coordinator.kwargs["plans"], plans)
        self.assertIs(
            coordinator.kwargs["policy"],
            collector.kwargs["policy"],
        )
        self.assertEqual(
            coordinator.kwargs,
            {
                "journal": self.journal,
                "report_archive_root": self.settings.reports_root,
                "source_collector": collector,
                "calendar_resolver": resolver,
                "policy": self.settings.policy,
                "plans": plans,
            },
        )
        self.assertEqual(
            _ActualCloseCoordinator.calls,
            [
                (
                    (session_date,),
                    {"review_at": review_at, "retrieved_at": retrieved_at},
                )
            ],
        )

    def test_january_close_loads_previous_year_for_fourteen_prior_sessions(
        self,
    ) -> None:
        adapter = self._open()
        session_date = date(2026, 1, 5)
        review_at = datetime(2026, 1, 5, 15, 30, tzinfo=ET)
        retrieved_at = datetime(2026, 1, 5, 15, 31, tzinfo=ET)
        calendars = {year: _Calendar(year) for year in (2025, 2026)}

        def load(_root: Path, *, as_of: date) -> _Calendar:
            return calendars[as_of.year]

        with ExitStack() as stack:
            self._runtime_modules(stack)
            load_mock = stack.enter_context(
                mock.patch.object(
                    provider_adapter_module._market_calendar,
                    "load_current_market_calendar",
                    side_effect=load,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    provider_adapter_module._risk,
                    "SessionCalendarResolver",
                    _Resolver,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    provider_workflows_module,
                    "ActualCloseWorkflowCoordinator",
                    _ActualCloseCoordinator,
                    create=True,
                )
            )
            adapter.close_material(
                session_date,
                review_at=review_at,
                retrieved_at=retrieved_at,
            )

        resolver = _Resolver.instances[-1]
        self.assertEqual(resolver.calendars, (calendars[2025], calendars[2026]))
        self.assertEqual(
            [call.kwargs["as_of"].year for call in load_mock.call_args_list],
            [2026, 2025],
        )
        self.assertIs(
            _ActualCloseCollector.instances[-1].kwargs["calendar_resolver"],
            resolver,
        )

    def test_public_surface_exposes_only_canonical_read_material_methods(self) -> None:
        adapter = self._open()
        self.assertEqual(provider_adapter_module.__all__, ["ProviderWorkflowAdapter"])
        self.assertEqual(
            {name for name in dir(adapter) if not name.startswith("_")},
            {"close_material", "market_session", "open", "premarket_material"},
        )
        for name in (
            "alpaca_market_data",
            "reference_client",
            "http_client",
            "credentials",
            "account",
            "brokerage",
            "orders",
            "options",
            "robinhood",
            "sec_client",
            "trade",
        ):
            self.assertFalse(hasattr(adapter, name), name)
        rendered = repr(adapter)
        self.assertNotIn(self.settings.alpaca_api_key_id, rendered)
        self.assertNotIn(self.settings.alpaca_api_secret_key, rendered)


if __name__ == "__main__":
    unittest.main()
