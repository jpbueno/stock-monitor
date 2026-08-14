"""Offline validation and session arithmetic for reviewed exchange calendars."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_NEW_YORK = "America/New_York"
_NYSE_URL = "https://www.nyse.com/trade/hours-calendars"
_NASDAQ_URL = "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar"
_TIME_PATTERN = re.compile(r"\d{2}:\d{2}")


class CalendarError(ValueError):
    """A calendar manifest is absent, inconsistent, or not reviewed."""


def _current_new_york_date() -> date:
    """Return today's date in the calendar's authoritative timezone."""
    try:
        timezone = ZoneInfo(_NEW_YORK)
    except ZoneInfoNotFoundError as exc:  # pragma: no cover - platform defect
        raise CalendarError("America/New_York timezone is unavailable") from exc
    return datetime.now(timezone).date()


@dataclass(frozen=True, slots=True)
class CalendarSource:
    """One immutable exchange schedule used by the reviewed manifest."""

    role: str
    name: str
    url: str
    retrieved_at: date
    reviewed_at: date
    closed_dates: tuple[date, ...]
    early_closes: tuple[tuple[date, time], ...]


@dataclass(frozen=True, slots=True)
class MarketSession:
    """The local wall-clock schedule for one open trading session."""

    session_date: date
    open_time: time
    close_time: time
    review_time: time
    timezone: ZoneInfo
    is_early_close: bool


@dataclass(frozen=True, slots=True)
class MarketCalendar:
    """A complete, source-agreed calendar for exactly one year."""

    year: int
    timezone: ZoneInfo
    retrieved_at: date
    reviewed_at: date
    sources: tuple[CalendarSource, ...]
    closed_dates: tuple[date, ...]
    early_closes: Mapping[date, MarketSession]
    open_session_count: int
    _regular_open_time: time = field(repr=False)
    _regular_close_time: time = field(repr=False)
    _regular_review_time: time = field(repr=False)
    _closed_date_set: frozenset[date] = field(repr=False)

    @classmethod
    def load(cls, path: Path, as_of: date | None = None) -> MarketCalendar:
        """Load and fully validate a local JSON manifest without network I/O."""
        if as_of is None:
            as_of = _current_new_york_date()
        if type(as_of) is not date:
            raise CalendarError("calendar as_of must be an exact date")
        if not isinstance(path, Path):
            raise CalendarError("calendar path must be a Path")
        try:
            expected_year = int(path.stem)
        except ValueError as exc:
            raise CalendarError("calendar filename must be a four-digit year") from exc
        if path.stem != f"{expected_year:04d}" or path.suffix != ".json":
            raise CalendarError("calendar filename must be a four-digit year")

        try:
            raw = json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=_unique_object,
                parse_float=_reject_json_float,
                parse_constant=_reject_json_constant,
            )
        except CalendarError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CalendarError("calendar manifest could not be read") from exc
        return cls.from_mapping(
            raw,
            as_of=as_of,
            expected_year=expected_year,
        )

    @classmethod
    def from_mapping(
        cls,
        raw: object,
        as_of: date,
        *,
        expected_year: int | None = None,
    ) -> MarketCalendar:
        """Validate a decoded manifest and return an immutable calendar."""
        if type(as_of) is not date:
            raise CalendarError("calendar as_of must be an exact date")
        table = _mapping(raw, "calendar")
        _exact_keys(
            table,
            {
                "schema_version",
                "year",
                "timezone",
                "retrieved_at",
                "reviewed_at",
                "review_status",
                "regular_session",
                "closures",
                "early_closes",
                "manual_disable_dates",
                "expected_open_sessions",
                "sources",
            },
            "calendar",
        )
        if type(table["schema_version"]) is not int or table["schema_version"] != 1:
            raise CalendarError("unsupported calendar schema version")

        year = _integer(table["year"], "year")
        if year < 1 or year > 9999:
            raise CalendarError("calendar year is invalid")
        if expected_year is not None and year != expected_year:
            raise CalendarError("calendar year does not match its filename")
        if as_of.year != year:
            raise CalendarError("calendar as_of is outside the manifest year")
        if table["timezone"] != _NEW_YORK:
            raise CalendarError("calendar timezone must be America/New_York")
        try:
            timezone = ZoneInfo(_NEW_YORK)
        except ZoneInfoNotFoundError as exc:  # pragma: no cover - platform defect
            raise CalendarError("America/New_York timezone is unavailable") from exc

        retrieved_at = _manifest_date(table["retrieved_at"], "retrieved_at", year)
        reviewed_at = _manifest_date(table["reviewed_at"], "reviewed_at", year)
        if table["review_status"] != "reviewed":
            raise CalendarError("calendar manifest is not reviewed")
        if retrieved_at != reviewed_at:
            raise CalendarError("calendar source data is stale or unreviewed")
        if as_of < retrieved_at or as_of < reviewed_at:
            raise CalendarError("calendar manifest is future dated")
        if (
            (as_of - retrieved_at).days > 31
            or (as_of - reviewed_at).days > 31
        ):
            raise CalendarError("calendar manifest is stale")

        regular = _mapping(table["regular_session"], "regular_session")
        _exact_keys(
            regular,
            {"open_time", "close_time", "review_time"},
            "regular_session",
        )
        regular_open = _wall_time(regular["open_time"], "regular open_time")
        regular_close = _wall_time(regular["close_time"], "regular close_time")
        regular_review = _wall_time(regular["review_time"], "regular review_time")
        if (
            regular_open != time(9, 30)
            or regular_close != time(16, 0)
            or regular_review != time(15, 30)
        ):
            raise CalendarError("regular session times do not match reviewed policy")

        closures = _date_sequence(table["closures"], "closures", year)
        if len(set(closures)) != len(closures):
            raise CalendarError("calendar closures contain duplicates")
        closed_set = frozenset(closures)
        if any(day.weekday() >= 5 for day in closures):
            raise CalendarError("calendar closure falls on a weekend")

        early_schedule = _canonical_early_closes(
            table["early_closes"],
            year=year,
            closed_dates=closed_set,
        )

        manual_disable_dates = _date_sequence(
            table["manual_disable_dates"],
            "manual_disable_dates",
            year,
        )
        if manual_disable_dates:
            raise CalendarError("manual calendar disable is active")

        sources_table = _mapping(table["sources"], "sources")
        _exact_keys(sources_table, {"primary", "cross_check"}, "sources")
        primary = _calendar_source(
            "primary",
            sources_table["primary"],
            expected_name="NYSE",
            expected_url=_NYSE_URL,
            year=year,
            retrieved_at=retrieved_at,
            reviewed_at=reviewed_at,
        )
        cross_check = _calendar_source(
            "cross_check",
            sources_table["cross_check"],
            expected_name="Nasdaq Trader",
            expected_url=_NASDAQ_URL,
            year=year,
            retrieved_at=retrieved_at,
            reviewed_at=reviewed_at,
        )
        canonical_closures = tuple(sorted(closures))
        canonical_early = tuple(
            sorted(
                (day, close_and_review[0])
                for day, close_and_review in early_schedule.items()
            )
        )
        for source in (primary, cross_check):
            if source.closed_dates != canonical_closures:
                raise CalendarError("exchange calendar sources conflict on closures")
            if source.early_closes != canonical_early:
                raise CalendarError("exchange calendar sources conflict on early closes")

        expected_count = _integer(
            table["expected_open_sessions"],
            "expected_open_sessions",
        )
        actual_count = _open_session_count(year, closed_set)
        if expected_count != actual_count:
            raise CalendarError("calendar session count is incomplete")

        early_sessions = {
            day: MarketSession(
                session_date=day,
                open_time=regular_open,
                close_time=close_time,
                review_time=review_time,
                timezone=timezone,
                is_early_close=True,
            )
            for day, (close_time, review_time) in early_schedule.items()
        }
        return cls(
            year=year,
            timezone=timezone,
            retrieved_at=retrieved_at,
            reviewed_at=reviewed_at,
            sources=(primary, cross_check),
            closed_dates=canonical_closures,
            early_closes=MappingProxyType(early_sessions),
            open_session_count=actual_count,
            _regular_open_time=regular_open,
            _regular_close_time=regular_close,
            _regular_review_time=regular_review,
            _closed_date_set=closed_set,
        )

    def is_open(self, day: date) -> bool:
        """Return whether *day* is an open session in this manifest year."""
        self._require_supported_day(day)
        return day.weekday() < 5 and day not in self._closed_date_set

    def session(self, day: date) -> MarketSession:
        """Return an immutable session, rejecting closed or missing dates."""
        if not self.is_open(day):
            raise CalendarError("requested date is not an open market session")
        early = self.early_closes.get(day)
        if early is not None:
            return early
        return MarketSession(
            session_date=day,
            open_time=self._regular_open_time,
            close_time=self._regular_close_time,
            review_time=self._regular_review_time,
            timezone=self.timezone,
            is_early_close=False,
        )

    def add_sessions(self, start: date, count: int) -> date:
        """Advance by nonnegative open sessions, skipping weekends and closures."""
        self._require_supported_day(start)
        if type(count) is not int or count < 0:
            raise CalendarError("negative or non-integer session offsets are unsupported")
        current = start
        remaining = count
        while remaining:
            current += timedelta(days=1)
            if self.is_open(current):
                remaining -= 1
        return current

    def _require_supported_day(self, day: date) -> None:
        if type(day) is not date or day.year != self.year:
            raise CalendarError("requested date is outside the loaded calendar year")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CalendarError(f"calendar JSON contains duplicate key {key}")
        result[key] = value
    return result


def _reject_json_float(value: str) -> None:
    raise CalendarError("calendar JSON must not contain floating-point numbers")


def _reject_json_constant(value: str) -> None:
    raise CalendarError("calendar JSON must not contain non-finite numbers")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) for key in value
    ):
        raise CalendarError(f"{name} must be an object")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise CalendarError(f"{name} must be an array")
    return value


def _exact_keys(
    value: Mapping[str, object],
    required: set[str],
    name: str,
) -> None:
    missing = required - value.keys()
    unknown = value.keys() - required
    if missing or unknown:
        raise CalendarError(f"{name} has missing or unknown fields")


def _integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise CalendarError(f"{name} must be an integer")
    return value


def _manifest_date(value: object, name: str, year: int) -> date:
    if not isinstance(value, str):
        raise CalendarError(f"{name} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise CalendarError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != value or parsed.year != year:
        raise CalendarError(f"{name} is outside the calendar year")
    return parsed


def _wall_time(value: object, name: str) -> time:
    if not isinstance(value, str) or _TIME_PATTERN.fullmatch(value) is None:
        raise CalendarError(f"{name} must use HH:MM")
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise CalendarError(f"{name} is invalid") from exc
    if parsed.tzinfo is not None or parsed.second or parsed.microsecond:
        raise CalendarError(f"{name} must be an unzoned minute")
    return parsed


def _date_sequence(value: object, name: str, year: int) -> tuple[date, ...]:
    items = _sequence(value, name)
    return tuple(
        _manifest_date(item, f"{name} item", year)
        for item in items
    )


def _canonical_early_closes(
    value: object,
    *,
    year: int,
    closed_dates: frozenset[date],
) -> dict[date, tuple[time, time]]:
    result: dict[date, tuple[time, time]] = {}
    for item in _sequence(value, "early_closes"):
        table = _mapping(item, "early close")
        _exact_keys(
            table,
            {"date", "close_time", "review_time"},
            "early close",
        )
        day = _manifest_date(table["date"], "early close date", year)
        close_time = _wall_time(table["close_time"], "early close time")
        review_time = _wall_time(table["review_time"], "early review time")
        if day in result:
            raise CalendarError("calendar early closes contain duplicates")
        if day.weekday() >= 5 or day in closed_dates:
            raise CalendarError("early close is not an open weekday")
        if close_time != time(13, 0) or review_time != time(12, 30):
            raise CalendarError("early close times do not match reviewed policy")
        result[day] = (close_time, review_time)
    return result


def _calendar_source(
    role: str,
    value: object,
    *,
    expected_name: str,
    expected_url: str,
    year: int,
    retrieved_at: date,
    reviewed_at: date,
) -> CalendarSource:
    table = _mapping(value, f"{role} source")
    _exact_keys(
        table,
        {
            "name",
            "url",
            "retrieved_at",
            "reviewed_at",
            "closures",
            "early_closes",
        },
        f"{role} source",
    )
    if table["name"] != expected_name or table["url"] != expected_url:
        raise CalendarError(f"{role} calendar source is not approved")
    _validated_source_url(expected_url)
    source_retrieved = _manifest_date(
        table["retrieved_at"],
        f"{role} retrieved_at",
        year,
    )
    source_reviewed = _manifest_date(
        table["reviewed_at"],
        f"{role} reviewed_at",
        year,
    )
    if source_retrieved != retrieved_at or source_reviewed != reviewed_at:
        raise CalendarError(f"{role} calendar source is stale or unreviewed")

    closures: list[date] = []
    for item in _sequence(table["closures"], f"{role} closures"):
        entry = _mapping(item, f"{role} closure")
        _exact_keys(entry, {"date", "label"}, f"{role} closure")
        if not isinstance(entry["label"], str) or not entry["label"].strip():
            raise CalendarError(f"{role} closure label is missing")
        closures.append(
            _manifest_date(entry["date"], f"{role} closure date", year)
        )
    if len(set(closures)) != len(closures):
        raise CalendarError(f"{role} closures contain duplicates")

    early_closes: dict[date, time] = {}
    for item in _sequence(table["early_closes"], f"{role} early_closes"):
        entry = _mapping(item, f"{role} early close")
        _exact_keys(
            entry,
            {"date", "label", "close_time"},
            f"{role} early close",
        )
        if not isinstance(entry["label"], str) or not entry["label"].strip():
            raise CalendarError(f"{role} early-close label is missing")
        day = _manifest_date(entry["date"], f"{role} early close date", year)
        close_time = _wall_time(
            entry["close_time"],
            f"{role} early close time",
        )
        if day in early_closes:
            raise CalendarError(f"{role} early closes contain duplicates")
        early_closes[day] = close_time

    return CalendarSource(
        role=role,
        name=expected_name,
        url=expected_url,
        retrieved_at=source_retrieved,
        reviewed_at=source_reviewed,
        closed_dates=tuple(sorted(closures)),
        early_closes=tuple(sorted(early_closes.items())),
    )


def _validated_source_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.hostname not in {"www.nyse.com", "www.nasdaqtrader.com"}
        or parsed.fragment
    ):
        raise CalendarError("calendar source URL is invalid")


def _open_session_count(year: int, closed_dates: frozenset[date]) -> int:
    current = date(year, 1, 1)
    end = date(year, 12, 31)
    count = 0
    while current <= end:
        if current.weekday() < 5 and current not in closed_dates:
            count += 1
        current += timedelta(days=1)
    return count


__all__ = [
    "CalendarError",
    "CalendarSource",
    "MarketCalendar",
    "MarketSession",
]
