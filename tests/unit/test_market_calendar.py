from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, time, timezone
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

import stock_monitor.market_calendar as market_calendar_module
from stock_monitor.market_calendar import (
    CalendarError,
    MarketCalendar,
    is_release_verified_market_calendar,
    is_validated_market_calendar,
    load_current_market_calendar,
)
from tests.support import calendar_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISHED_CALENDAR = PROJECT_ROOT / "data" / "calendars" / "2026.json"
REVIEWED_AS_OF = date(2026, 8, 14)
EXPECTED_CLOSED_DATES = (
    date(2026, 1, 1),
    date(2026, 1, 19),
    date(2026, 2, 16),
    date(2026, 4, 3),
    date(2026, 5, 25),
    date(2026, 6, 19),
    date(2026, 7, 3),
    date(2026, 9, 7),
    date(2026, 11, 26),
    date(2026, 12, 25),
)


class MarketCalendarTests(unittest.TestCase):
    def test_completed_session_selector_crosses_new_year_holiday(self) -> None:
        prior = load_current_market_calendar(
            PROJECT_ROOT,
            as_of=REVIEWED_AS_OF,
        )

        try:
            completed = market_calendar_module.latest_completed_session_window(
                prior,
                observed_at=datetime(2027, 1, 1, 17, 0, tzinfo=timezone.utc),
            )
        except CalendarError:
            completed = None

        self.assertIsNotNone(completed)
        assert completed is not None
        self.assertEqual(completed.session_date, date(2026, 12, 31))
        self.assertEqual(
            completed.closed_at,
            datetime(2026, 12, 31, 16, 0, tzinfo=prior.timezone),
        )

    def test_completed_session_selector_uses_first_session_after_release(self) -> None:
        current = MarketCalendar.from_mapping(
            calendar_fixture(2027),
            as_of=date(2027, 1, 4),
            expected_year=2027,
        )
        market_calendar_module._register_calendar_authority(
            market_calendar_module._RELEASE_CALENDARS,
            current,
        )

        completed = market_calendar_module.latest_completed_session_window(
            current,
            observed_at=datetime(2027, 1, 4, 21, 16, tzinfo=timezone.utc),
        )

        self.assertEqual(getattr(completed, "session_date", None), date(2027, 1, 4))
        self.assertEqual(
            getattr(completed, "closed_at", None),
            datetime(2027, 1, 4, 16, 0, tzinfo=current.timezone),
        )

    def test_structural_and_pinned_release_authority_are_distinct(self) -> None:
        structural = MarketCalendar.load(
            PUBLISHED_CALENDAR,
            as_of=REVIEWED_AS_OF,
        )
        release = load_current_market_calendar(
            PROJECT_ROOT,
            as_of=REVIEWED_AS_OF,
        )

        self.assertTrue(is_validated_market_calendar(structural))
        self.assertFalse(is_release_verified_market_calendar(structural))
        self.assertTrue(is_validated_market_calendar(release))
        self.assertTrue(is_release_verified_market_calendar(release))
        for forged in (
            replace(release),
            replace(
                release,
                _closed_date_set=release._closed_date_set - {date(2026, 12, 25)},
            ),
        ):
            with self.subTest(forged=forged):
                self.assertFalse(is_validated_market_calendar(forged))
                self.assertFalse(is_release_verified_market_calendar(forged))

    def test_direct_calendar_cannot_mint_validation_or_release_authority(self) -> None:
        release = load_current_market_calendar(
            PROJECT_ROOT,
            as_of=REVIEWED_AS_OF,
        )
        forged = MarketCalendar(
            year=release.year,
            timezone=release.timezone,
            retrieved_at=release.retrieved_at,
            reviewed_at=release.reviewed_at,
            sources=release.sources,
            closed_dates=(),
            early_closes=MappingProxyType({}),
            open_session_count=261,
            _regular_open_time=time(0, 0),
            _regular_close_time=time(23, 59),
            _regular_review_time=time(23, 0),
            _closed_date_set=frozenset(),
        )

        self.assertTrue(forged.is_open(date(2026, 12, 25)))
        self.assertFalse(is_validated_market_calendar(forged))
        self.assertFalse(is_release_verified_market_calendar(forged))

    def _load_mapping(
        self,
        raw: dict[str, object],
        *,
        filename: str = "2026.json",
        as_of: date = REVIEWED_AS_OF,
    ) -> MarketCalendar:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / filename
            path.write_text(
                json.dumps(raw, indent=2) + "\n",
                encoding="utf-8",
            )
            return MarketCalendar.load(path, as_of=as_of)

    def test_published_calendar_matches_reviewed_reference_fixture(self) -> None:
        published = json.loads(PUBLISHED_CALENDAR.read_text(encoding="utf-8"))

        self.assertEqual(published, calendar_fixture())

    def test_calendar_uses_reviewed_exchange_sources_and_new_york_time(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        self.assertEqual(calendar.year, 2026)
        self.assertEqual(calendar.timezone.key, "America/New_York")
        self.assertEqual(calendar.retrieved_at, date(2026, 8, 14))
        self.assertEqual(calendar.reviewed_at, date(2026, 8, 14))
        self.assertEqual(
            tuple(source.url for source in calendar.sources),
            (
                "https://www.nyse.com/trade/hours-calendars",
                "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
            ),
        )
        self.assertEqual(calendar.closed_dates, EXPECTED_CLOSED_DATES)
        self.assertEqual(calendar.open_session_count, 251)

    def test_load_defaults_as_of_to_current_new_york_date(self) -> None:
        with patch.object(
            market_calendar_module,
            "_current_new_york_date",
            return_value=REVIEWED_AS_OF,
            create=True,
        ) as current_date:
            try:
                calendar = MarketCalendar.load(PUBLISHED_CALENDAR)
            except TypeError as exc:
                self.fail(f"load does not support an omitted as_of: {exc}")

        self.assertEqual(calendar.reviewed_at, REVIEWED_AS_OF)
        current_date.assert_called_once_with()

    def test_default_as_of_fails_closed_when_manifest_is_future_or_stale(self) -> None:
        for case, current_date in (
            ("future", date(2026, 1, 1)),
            ("stale", date(2026, 9, 15)),
        ):
            with self.subTest(case=case), patch.object(
                market_calendar_module,
                "_current_new_york_date",
                return_value=current_date,
                create=True,
            ):
                try:
                    with self.assertRaises(CalendarError):
                        MarketCalendar.load(PUBLISHED_CALENDAR)
                except TypeError as exc:
                    self.fail(f"load does not support an omitted as_of: {exc}")

    def test_current_date_uses_new_york_at_utc_midnight_boundary(self) -> None:
        utc_instant = datetime(2026, 8, 15, 3, 30, tzinfo=timezone.utc)
        with patch.object(
            market_calendar_module,
            "datetime",
            create=True,
        ) as datetime_type:
            datetime_type.now.side_effect = utc_instant.astimezone
            try:
                current_date = market_calendar_module._current_new_york_date()
            except AttributeError as exc:
                self.fail(f"New York current-date helper is missing: {exc}")

        self.assertEqual(current_date, date(2026, 8, 14))
        requested_timezone = datetime_type.now.call_args.args[0]
        self.assertEqual(requested_timezone.key, "America/New_York")

    def test_regular_session_routes_review_to_1530(self) -> None:
        session = MarketCalendar.load(
            PUBLISHED_CALENDAR,
            as_of=REVIEWED_AS_OF,
        ).session(date(2026, 8, 14))

        self.assertEqual(session.open_time, time(9, 30))
        self.assertEqual(session.close_time, time(16, 0))
        self.assertEqual(session.review_time, time(15, 30))
        self.assertFalse(session.is_early_close)
        self.assertEqual(session.timezone.key, "America/New_York")

    def test_both_early_closes_route_review_to_1230(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        for session_date in (date(2026, 11, 27), date(2026, 12, 24)):
            with self.subTest(session_date=session_date):
                session = calendar.session(session_date)
                self.assertEqual(session.close_time, time(13, 0))
                self.assertEqual(session.review_time, time(12, 30))
                self.assertTrue(session.is_early_close)

    def test_closed_dates_and_weekends_are_not_sessions(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        for session_date in (*EXPECTED_CLOSED_DATES, date(2026, 8, 15)):
            with self.subTest(session_date=session_date):
                self.assertFalse(calendar.is_open(session_date))
                with self.assertRaises(CalendarError):
                    calendar.session(session_date)

    def test_add_sessions_supports_zero_and_skips_weekends_and_closures(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        self.assertEqual(
            calendar.add_sessions(date(2026, 8, 14), 0),
            date(2026, 8, 14),
        )
        self.assertEqual(
            calendar.add_sessions(date(2026, 8, 14), 1),
            date(2026, 8, 17),
        )
        self.assertEqual(
            calendar.add_sessions(date(2026, 7, 2), 1),
            date(2026, 7, 6),
        )

    def test_add_sessions_rejects_negative_non_integer_and_missing_year(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        for invalid_count in (-1, True, 1.0):
            with self.subTest(count=invalid_count), self.assertRaises(CalendarError):
                calendar.add_sessions(date(2026, 8, 14), invalid_count)  # type: ignore[arg-type]
        with self.assertRaises(CalendarError):
            calendar.add_sessions(date(2027, 1, 2), 0)
        with self.assertRaises(CalendarError):
            calendar.add_sessions(date(2026, 12, 31), 1)

    def test_session_and_is_open_fail_closed_outside_manifest_year(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)

        for method in (calendar.session, calendar.is_open):
            with self.subTest(method=method.__name__), self.assertRaises(CalendarError):
                method(date(2027, 1, 2))

    def test_manifest_rejects_wrong_or_missing_year(self) -> None:
        for case, mutation, filename in (
            ("wrong manifest year", {"year": 2025}, "2026.json"),
            ("wrong path year", {}, "2025.json"),
        ):
            raw = calendar_fixture()
            raw.update(mutation)
            with self.subTest(case=case), self.assertRaises(CalendarError):
                self._load_mapping(raw, filename=filename)

        raw = calendar_fixture()
        del raw["year"]
        with self.assertRaises(CalendarError):
            self._load_mapping(raw)

    def test_manifest_rejects_stale_or_unreviewed_source_data(self) -> None:
        mutations = (
            ("review_status", "pending"),
            ("reviewed_at", "2026-08-13"),
        )
        for field, value in mutations:
            raw = calendar_fixture()
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(CalendarError):
                self._load_mapping(raw)

        for source_name, field, value in (
            ("primary", "reviewed_at", "2026-08-13"),
            ("primary", "retrieved_at", "2026-08-13"),
            ("cross_check", "reviewed_at", "2026-08-13"),
            ("cross_check", "retrieved_at", "2026-08-13"),
        ):
            raw = calendar_fixture()
            raw_sources = raw["sources"]
            self.assertIsInstance(raw_sources, dict)
            raw_source = raw_sources[source_name]
            self.assertIsInstance(raw_source, dict)
            raw_source[field] = value
            with self.subTest(source=source_name, field=field), self.assertRaises(
                CalendarError
            ):
                self._load_mapping(raw)

    def test_calendar_freshness_window_is_inclusive_through_day_31(self) -> None:
        calendar = MarketCalendar.load(
            PUBLISHED_CALENDAR,
            as_of=date(2026, 9, 14),
        )

        self.assertEqual(calendar.reviewed_at, REVIEWED_AS_OF)
        with self.assertRaises(CalendarError):
            MarketCalendar.load(
                PUBLISHED_CALENDAR,
                as_of=date(2026, 9, 15),
            )

    def test_calendar_rejects_future_dated_and_stale_as_of_values(self) -> None:
        for case, as_of in (
            ("manifest is future dated", date(2026, 1, 1)),
            ("manifest is stale", date(2026, 12, 31)),
        ):
            with self.subTest(case=case), self.assertRaises(CalendarError):
                MarketCalendar.load(PUBLISHED_CALENDAR, as_of=as_of)

    def test_calendar_rejects_internally_consistent_stale_or_future_reviews(self) -> None:
        for case, source_date in (
            ("stale January review", "2026-01-01"),
            ("future December review", "2026-12-31"),
        ):
            raw = calendar_fixture()
            raw["retrieved_at"] = source_date
            raw["reviewed_at"] = source_date
            sources = raw["sources"]
            self.assertIsInstance(sources, dict)
            for source in sources.values():
                self.assertIsInstance(source, dict)
                source["retrieved_at"] = source_date
                source["reviewed_at"] = source_date
            with self.subTest(case=case), self.assertRaises(CalendarError):
                self._load_mapping(raw, as_of=REVIEWED_AS_OF)

    def test_calendar_rejects_cross_year_as_of_inside_freshness_window(self) -> None:
        raw = calendar_fixture()
        raw["retrieved_at"] = "2026-12-31"
        raw["reviewed_at"] = "2026-12-31"
        sources = raw["sources"]
        self.assertIsInstance(sources, dict)
        for source in sources.values():
            self.assertIsInstance(source, dict)
            source["retrieved_at"] = "2026-12-31"
            source["reviewed_at"] = "2026-12-31"

        with self.assertRaises(CalendarError):
            self._load_mapping(raw, as_of=date(2027, 1, 1))

    def test_calendar_as_of_requires_an_exact_date(self) -> None:
        invalid_values = (
            "2026-08-14",
            datetime(2026, 8, 14),
            True,
        )
        for invalid in invalid_values:
            with self.subTest(api="load", value=invalid), self.assertRaises(
                CalendarError
            ):
                MarketCalendar.load(PUBLISHED_CALENDAR, as_of=invalid)  # type: ignore[arg-type]
            with self.subTest(api="from_mapping", value=invalid), self.assertRaises(
                CalendarError
            ):
                MarketCalendar.from_mapping(  # type: ignore[arg-type]
                    calendar_fixture(),
                    as_of=invalid,
                    expected_year=2026,
                )
        with self.assertRaises(CalendarError):
            MarketCalendar.from_mapping(
                calendar_fixture(),
                as_of=None,  # type: ignore[arg-type]
                expected_year=2026,
            )

    def test_calendar_schema_version_must_be_an_exact_integer(self) -> None:
        for invalid in (True, False):
            raw = calendar_fixture()
            raw["schema_version"] = invalid

            with self.subTest(value=invalid), self.assertRaises(CalendarError):
                self._load_mapping(raw)

    def test_manifest_rejects_exchange_schedule_conflicts(self) -> None:
        raw = calendar_fixture()
        sources = raw["sources"]
        self.assertIsInstance(sources, dict)
        cross_check = sources["cross_check"]
        self.assertIsInstance(cross_check, dict)
        closures = cross_check["closures"]
        self.assertIsInstance(closures, list)
        closures.pop()

        with self.assertRaises(CalendarError):
            self._load_mapping(raw)

    def test_source_label_differences_do_not_create_false_conflicts(self) -> None:
        raw = calendar_fixture()
        sources = raw["sources"]
        self.assertIsInstance(sources, dict)
        cross_check = sources["cross_check"]
        self.assertIsInstance(cross_check, dict)
        closures = cross_check["closures"]
        self.assertIsInstance(closures, list)
        first = closures[0]
        self.assertIsInstance(first, dict)
        first["label"] = "Different official label"

        calendar = self._load_mapping(raw)

        self.assertEqual(calendar.open_session_count, 251)

    def test_manifest_rejects_duplicate_closures_and_early_closes(self) -> None:
        for key in ("closures", "early_closes"):
            raw = calendar_fixture()
            values = raw[key]
            self.assertIsInstance(values, list)
            values.append(values[0])
            with self.subTest(key=key), self.assertRaises(CalendarError):
                self._load_mapping(raw)

    def test_manifest_rejects_weekend_or_closed_early_close(self) -> None:
        for invalid_date in ("2026-11-28", "2026-11-26"):
            raw = calendar_fixture()
            early_closes = raw["early_closes"]
            self.assertIsInstance(early_closes, list)
            early_close = early_closes[0]
            self.assertIsInstance(early_close, dict)
            early_close["date"] = invalid_date
            for source_name in ("primary", "cross_check"):
                sources = raw["sources"]
                self.assertIsInstance(sources, dict)
                source = sources[source_name]
                self.assertIsInstance(source, dict)
                source_early_closes = source["early_closes"]
                self.assertIsInstance(source_early_closes, list)
                source_item = source_early_closes[0]
                self.assertIsInstance(source_item, dict)
                source_item["date"] = invalid_date
            with self.subTest(invalid_date=invalid_date), self.assertRaises(
                CalendarError
            ):
                self._load_mapping(raw)

    def test_manifest_rejects_malformed_times_and_timezone(self) -> None:
        cases = (
            ("timezone", "UTC"),
            ("regular_open", "9:30"),
            ("regular_close", "25:00"),
            ("regular_review", "16:30"),
            ("early_close", "16:00"),
            ("early_review", "13:00"),
        )
        for case, value in cases:
            raw = calendar_fixture()
            if case == "timezone":
                raw["timezone"] = value
            elif case.startswith("regular_"):
                regular = raw["regular_session"]
                self.assertIsInstance(regular, dict)
                regular[case.removeprefix("regular_") + "_time"] = value
            else:
                early_closes = raw["early_closes"]
                self.assertIsInstance(early_closes, list)
                early_close = early_closes[0]
                self.assertIsInstance(early_close, dict)
                early_close[case.removeprefix("early_") + "_time"] = value
            with self.subTest(case=case), self.assertRaises(CalendarError):
                self._load_mapping(raw)

    def test_manifest_rejects_missing_session_count_and_manual_disable(self) -> None:
        raw = calendar_fixture()
        del raw["expected_open_sessions"]
        with self.assertRaises(CalendarError):
            self._load_mapping(raw)

        raw = calendar_fixture()
        raw["expected_open_sessions"] = 250
        with self.assertRaises(CalendarError):
            self._load_mapping(raw)

        raw = calendar_fixture()
        raw["manual_disable_dates"] = ["2026-08-14"]
        with self.assertRaises(CalendarError):
            self._load_mapping(raw)

    def test_calendar_data_structures_are_immutable(self) -> None:
        calendar = MarketCalendar.load(PUBLISHED_CALENDAR, as_of=REVIEWED_AS_OF)
        session = calendar.session(date(2026, 8, 14))

        self.assertIsInstance(calendar.early_closes, MappingProxyType)
        with self.assertRaises(TypeError):
            calendar.early_closes[date(2026, 8, 14)] = session  # type: ignore[index]
        with self.assertRaises(FrozenInstanceError):
            session.close_time = time(15, 0)  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
