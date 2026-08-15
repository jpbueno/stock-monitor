"""Read-only Alpaca market-data adapter for SIP history and IEX freshness."""

from __future__ import annotations

import hashlib
import json
import math
import re
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from stock_monitor.domain import require_aware_timestamp

from .cache import (
    ContentCache,
    SourceHealthAttestation,
    SourceObservation,
    _issue_provider_health_attestation,
)
from .http import (
    EgressPolicy,
    GetTransport,
    HttpTransportError,
    ProviderIncompleteError,
    ProviderResponseError,
    get_with_redirects,
)


_BASE_URL = "https://data.alpaca.markets"
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,9}\Z")
_OCC_SYMBOL = re.compile(
    r"(?P<root>[A-Z]{1,6})(?P<date>[0-9]{6})(?P<right>[CP])(?P<strike>[0-9]{8})\Z"
)
_HISTORICAL_DELAY = timedelta(minutes=16)
_LATEST_MAX_AGE_SECONDS = 300
_HISTORICAL_TERMINAL_TOLERANCE = timedelta(minutes=5)
_MAX_PAGES = 100
_NEW_YORK = ZoneInfo("America/New_York")


class ProviderDataError(ProviderResponseError):
    """Market data is malformed, stale, or inconsistent with its requested feed."""


@dataclass(frozen=True, slots=True)
class AlpacaCredentials:
    key_id: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.key_id, self.secret_key):
            if (
                not isinstance(value, str)
                or not value
                or not value.isascii()
                or not value.isprintable()
                or value != value.strip()
                or len(value) > 256
            ):
                raise ValueError("Alpaca credential is malformed")


def _utc(value: datetime, name: str) -> datetime:
    return require_aware_timestamp(value, name).astimezone(UTC)


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ProviderDataError(f"{name} timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ProviderDataError(f"{name} timestamp is malformed") from None
    try:
        return _utc(parsed, f"{name} timestamp")
    except ValueError:
        raise ProviderDataError(f"{name} timestamp is malformed") from None


def _decimal(value: object, name: str, *, positive: bool = True) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ProviderDataError(f"{name} is malformed")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ProviderDataError(f"{name} is malformed") from None
    if not result.is_finite() or (positive and result <= 0):
        raise ProviderDataError(f"{name} is malformed")
    return result


def _integer(value: object, name: str, *, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ProviderDataError(f"{name} is malformed")
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        raise ProviderDataError(f"{name} is malformed") from None
    if str(result) != str(value) and not (
        isinstance(value, str) and value.isdigit() and int(value) == result
    ):
        raise ProviderDataError(f"{name} is malformed")
    if result < 0:
        raise ProviderDataError(f"{name} is malformed")
    return result


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ProviderDataError("provider JSON contains duplicate fields")
        result[name] = value
    return result


def _json_object(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_object_pairs)
    except (UnicodeError, json.JSONDecodeError):
        raise ProviderDataError("provider response is not valid JSON") from None
    if not isinstance(value, dict):
        raise ProviderDataError("provider JSON root must be an object")
    return value


def _format_utc(value: datetime) -> str:
    normalized = _utc(value, "window timestamp")
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class TimeWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        start = _utc(self.start, "window start")
        end = _utc(self.end, "window end")
        if start >= end:
            raise ValueError("market-data window start must precede end")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    feed: str
    adjustment: str
    source_observation_id: str

    @property
    def t(self) -> datetime:
        return self.timestamp


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    feed: str
    sequence: int | None
    age_seconds: int
    source_observation_id: str

    @property
    def t(self) -> datetime:
        return self.timestamp

    @property
    def bp(self) -> Decimal:
        return self.bid

    @property
    def ap(self) -> Decimal:
        return self.ask


@dataclass(frozen=True, slots=True)
class OptionSnapshot:
    occ_symbol: str
    underlying: str
    expiration: date
    strike: Decimal
    delta: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    daily_volume: int | None
    open_interest: None
    feed: str
    observed_at: datetime | None
    source_observation_id: str


@dataclass(frozen=True, slots=True)
class EntitlementSmoke:
    authentication_ok: bool
    historical_sip_ok: bool
    latest_iex_fresh: bool
    status: str
    observed_at: datetime
    failures: tuple[str, ...]


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not values:
        raise ValueError("at least one market-data symbol is required")
    result: list[str] = []
    for value in values:
        if not isinstance(value, str) or not _SYMBOL.fullmatch(value.upper()):
            raise ValueError("market-data symbol is malformed")
        result.append(value.upper())
    if len(result) > 200 or len(result) != len(set(result)):
        raise ValueError("market-data symbols must be unique and bounded")
    return tuple(sorted(result))


class AlpacaMarketData:
    """Market-data-only client; it has no transactional-origin capability."""

    def __init__(
        self,
        transport: GetTransport,
        credentials: AlpacaCredentials,
        *,
        base_url: str = _BASE_URL,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        cache: ContentCache | None = None,
    ) -> None:
        if base_url != _BASE_URL:
            raise ValueError("Alpaca market-data origin must be exact")
        if not isinstance(credentials, AlpacaCredentials):
            raise TypeError("Alpaca credentials have the wrong type")
        self._transport = transport
        self._credentials = credentials
        self._base_url = base_url
        self._policy = EgressPolicy({"data.alpaca.markets"})
        self._now = now
        self._cache = cache
        self._observations: dict[str, SourceObservation] = {}

    def _current_time(self) -> datetime:
        return _utc(self._now(), "current time")

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self._credentials.key_id,
            "APCA-API-SECRET-KEY": self._credentials.secret_key,
            "Accept": "application/json",
        }

    def _get_json(self, url: str) -> tuple[dict[str, object], bytes]:
        response = get_with_redirects(
            self._transport,
            self._policy,
            url,
            self._headers(),
            allowed_content_types=("application/json",),
        )
        return _json_object(response.body), response.body

    def _pin(
        self,
        *,
        url: str,
        payload: bytes,
        source_type: str,
        feed: str,
        source_timestamp: datetime,
    ) -> str:
        retrieved_at = self._current_time()
        safe_timestamp = _utc(source_timestamp, "source timestamp")
        if safe_timestamp > retrieved_at:
            raise ProviderDataError("provider source timestamp is in the future")
        identity = hashlib.sha256(
            source_type.encode("ascii")
            + b"\0"
            + url.encode("ascii")
            + b"\0"
            + retrieved_at.isoformat().encode("ascii")
            + b"\0"
            + payload
        ).hexdigest()
        observation_id = f"obs-{identity[:24]}"
        observation = SourceObservation(
            observation_id=observation_id,
            url=url,
            source_type=source_type,
            source_timestamp=safe_timestamp,
            retrieved_at=retrieved_at,
            feed=feed,
            delay_seconds=max(0, int((retrieved_at - safe_timestamp).total_seconds())),
        )
        self._observations[observation_id] = observation
        if self._cache is not None:
            self._cache.put(observation, payload)
        return observation_id

    def health_attestation(self, observation_id: str) -> SourceHealthAttestation:
        """Issue cache authority only for a successful market-data observation."""
        if not isinstance(observation_id, str) or not observation_id:
            raise ValueError("market-data observation ID is malformed")
        observation = self._observations.get(observation_id)
        if observation is None:
            raise ValueError("market-data observation was not issued by this client")
        return _issue_provider_health_attestation(observation)

    def _validate_historical_window(self, window: TimeWindow) -> None:
        if not isinstance(window, TimeWindow):
            raise TypeError("historical window has the wrong type")
        if self._current_time() - window.end < _HISTORICAL_DELAY:
            raise ProviderDataError(
                "historical SIP window must end at least sixteen minutes ago"
            )

    def _pages(
        self,
        *,
        path: str,
        query: list[tuple[str, str]],
        collection: str,
    ) -> list[tuple[str, dict[str, object], bytes]]:
        pages: list[tuple[str, dict[str, object], bytes]] = []
        token: str | None = None
        seen_tokens: set[str] = set()
        for _ in range(_MAX_PAGES):
            current_query = list(query)
            if token is not None:
                current_query.append(("page_token", token))
            url = f"{self._base_url}{path}?{urllib.parse.urlencode(current_query)}"
            document, payload = self._get_json(url)
            if collection not in document or not isinstance(document[collection], dict):
                raise ProviderIncompleteError(
                    f"provider response is missing the {collection} collection"
                )
            if "next_page_token" not in document:
                raise ProviderIncompleteError("provider pagination did not terminate explicitly")
            pages.append((url, document, payload))
            next_token = document["next_page_token"]
            if next_token is None:
                return pages
            if (
                not isinstance(next_token, str)
                or not next_token
                or len(next_token) > 512
                or not next_token.isascii()
                or not next_token.isprintable()
                or next_token in seen_tokens
            ):
                raise ProviderIncompleteError("provider pagination token is malformed or repeated")
            seen_tokens.add(next_token)
            token = next_token
        raise ProviderIncompleteError("provider pagination exceeded the page limit")

    def daily_bars(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
    ) -> Mapping[str, tuple[Bar, ...]]:
        requested = _symbols(symbols)
        self._validate_historical_window(window)
        query = [
            ("symbols", ",".join(requested)),
            ("timeframe", "1Day"),
            ("start", _format_utc(window.start)),
            ("end", _format_utc(window.end)),
            ("adjustment", "split"),
            ("feed", "sip"),
            ("limit", "10000"),
        ]
        result: dict[str, list[Bar]] = {symbol: [] for symbol in requested}
        for url, document, payload in self._pages(
            path="/v2/stocks/bars",
            query=query,
            collection="bars",
        ):
            raw_collection = document["bars"]
            assert isinstance(raw_collection, dict)
            unknown = set(raw_collection) - set(requested)
            if unknown:
                raise ProviderDataError("provider bars contain an unrequested symbol")
            parsed_page: list[tuple[str, datetime, dict[str, object]]] = []
            for symbol, values in raw_collection.items():
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderDataError("provider bars collection is malformed")
                for value in values:
                    if not isinstance(value, dict):
                        raise ProviderDataError("provider bar is malformed")
                    timestamp = _timestamp(value.get("t"), "bar")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderDataError(
                            "provider bar lies outside the requested window"
                        )
                    parsed_page.append((symbol, timestamp, value))
            source_timestamp = max(
                (item[1] for item in parsed_page),
                default=window.end,
            )
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_DAILY_BARS",
                feed="sip",
                source_timestamp=source_timestamp,
            )
            for symbol, timestamp, value in parsed_page:
                open_price = _decimal(value.get("o"), "bar open")
                high = _decimal(value.get("h"), "bar high")
                low = _decimal(value.get("l"), "bar low")
                close = _decimal(value.get("c"), "bar close")
                volume = _integer(value.get("v"), "bar volume")
                assert volume is not None
                if high < max(open_price, close, low) or low > min(open_price, close, high):
                    raise ProviderDataError("provider bar OHLC values are inconsistent")
                result[symbol].append(
                    Bar(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_price,
                        high=high,
                        low=low,
                        close=close,
                        volume=volume,
                        feed="sip",
                        adjustment="split",
                        source_observation_id=observation_id,
                    )
                )
        return self._complete_bars(result, expected_terminal=window.end)

    @staticmethod
    def _complete_bars(
        values: dict[str, list[Bar]],
        *,
        expected_terminal: datetime,
    ) -> dict[str, tuple[Bar, ...]]:
        completed: dict[str, tuple[Bar, ...]] = {}
        cohort_timestamps: frozenset[datetime] | None = None
        for symbol in sorted(values):
            ordered = tuple(sorted(values[symbol], key=lambda item: item.timestamp))
            if not ordered or len({item.timestamp for item in ordered}) != len(ordered):
                raise ProviderIncompleteError(
                    "provider bars are missing or contain duplicate timestamps"
                )
            if (
                ordered[-1].timestamp.astimezone(_NEW_YORK).date()
                != expected_terminal.astimezone(_NEW_YORK).date()
            ):
                raise ProviderIncompleteError(
                    "provider bars do not reach the expected completed session"
                )
            timestamps = frozenset(item.timestamp for item in ordered)
            if cohort_timestamps is None:
                cohort_timestamps = timestamps
            elif timestamps != cohort_timestamps:
                raise ProviderIncompleteError(
                    "provider bars do not contain an aligned timestamp cohort"
                )
            completed[symbol] = ordered
        return completed

    def historical_quotes(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
    ) -> Mapping[str, tuple[Quote, ...]]:
        requested = _symbols(symbols)
        self._validate_historical_window(window)
        query = [
            ("symbols", ",".join(requested)),
            ("start", _format_utc(window.start)),
            ("end", _format_utc(window.end)),
            ("feed", "sip"),
            ("limit", "10000"),
        ]
        result: dict[str, list[Quote]] = {symbol: [] for symbol in requested}
        for url, document, payload in self._pages(
            path="/v2/stocks/quotes",
            query=query,
            collection="quotes",
        ):
            raw_collection = document["quotes"]
            assert isinstance(raw_collection, dict)
            unknown = set(raw_collection) - set(requested)
            if unknown:
                raise ProviderDataError("provider quotes contain an unrequested symbol")
            parsed_page: list[tuple[str, datetime, dict[str, object]]] = []
            for symbol, values in raw_collection.items():
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderDataError("provider quotes collection is malformed")
                for value in values:
                    if not isinstance(value, dict):
                        raise ProviderDataError("provider quote is malformed")
                    timestamp = _timestamp(value.get("t"), "quote")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderDataError(
                            "provider quote lies outside the requested window"
                        )
                    parsed_page.append((symbol, timestamp, value))
            source_timestamp = max(
                (item[1] for item in parsed_page),
                default=window.end,
            )
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_HISTORICAL_QUOTES",
                feed="sip",
                source_timestamp=source_timestamp,
            )
            now = self._current_time()
            for symbol, timestamp, value in parsed_page:
                result[symbol].append(
                    self._quote(
                        symbol,
                        timestamp,
                        value,
                        feed="sip",
                        now=now,
                        observation_id=observation_id,
                    )
                )
        completed: dict[str, tuple[Quote, ...]] = {}
        for symbol in sorted(result):
            ordered = tuple(sorted(result[symbol], key=lambda item: item.timestamp))
            if not ordered or len({item.timestamp for item in ordered}) != len(ordered):
                raise ProviderIncompleteError(
                    "provider quotes are missing or contain duplicate timestamps"
                )
            if window.end - ordered[-1].timestamp > _HISTORICAL_TERMINAL_TOLERANCE:
                raise ProviderIncompleteError(
                    "provider quotes do not reach the terminal window boundary"
                )
            completed[symbol] = ordered
        return completed

    @staticmethod
    def _quote(
        symbol: str,
        timestamp: datetime,
        value: dict[str, object],
        *,
        feed: str,
        now: datetime,
        observation_id: str,
    ) -> Quote:
        payload_feed = value.get("feed")
        if payload_feed is not None and payload_feed != feed:
            raise ProviderDataError("provider quote feed conflicts with requested feed")
        bid = _decimal(value.get("bp"), "quote bid")
        ask = _decimal(value.get("ap"), "quote ask")
        if ask < bid:
            raise ProviderDataError("provider quote is crossed")
        exact_age = (now - timestamp).total_seconds()
        if exact_age < 0:
            raise ProviderDataError("provider quote timestamp is in the future")
        age = math.ceil(exact_age)
        sequence = _integer(value.get("i"), "quote sequence", optional=True)
        return Quote(
            symbol=symbol,
            timestamp=timestamp,
            bid=bid,
            ask=ask,
            feed=feed,
            sequence=sequence,
            age_seconds=age,
            source_observation_id=observation_id,
        )

    def latest_iex_quotes(self, symbols: Sequence[str]) -> Mapping[str, Quote]:
        requested = _symbols(symbols)
        query = [("symbols", ",".join(requested)), ("feed", "iex")]
        url = (
            f"{self._base_url}/v2/stocks/quotes/latest?"
            + urllib.parse.urlencode(query)
        )
        document, payload = self._get_json(url)
        if "quotes" not in document or not isinstance(document["quotes"], dict):
            raise ProviderIncompleteError("provider latest quotes collection is missing")
        if document.get("next_page_token", None) is not None:
            raise ProviderIncompleteError("latest quote response cannot be paginated")
        raw = document["quotes"]
        assert isinstance(raw, dict)
        if set(raw) != set(requested):
            raise ProviderIncompleteError("provider latest quotes cohort is incomplete")
        parsed: list[tuple[str, datetime, dict[str, object]]] = []
        for symbol in requested:
            value = raw[symbol]
            if not isinstance(value, dict):
                raise ProviderDataError("provider latest quote is malformed")
            parsed.append((symbol, _timestamp(value.get("t"), "latest quote"), value))
        now = self._current_time()
        observation_id = self._pin(
            url=url,
            payload=payload,
            source_type="ALPACA_LATEST_QUOTES",
            feed="iex",
            source_timestamp=max(item[1] for item in parsed),
        )
        result: dict[str, Quote] = {}
        for symbol, timestamp, value in parsed:
            quote = self._quote(
                symbol,
                timestamp,
                value,
                feed="iex",
                now=now,
                observation_id=observation_id,
            )
            if quote.age_seconds > _LATEST_MAX_AGE_SECONDS:
                raise ProviderDataError("IEX freshness quote is stale")
            result[symbol] = quote
        return result

    def option_chain(self, underlying: str) -> tuple[OptionSnapshot, ...]:
        symbol = _symbols([underlying])[0]
        query = [("feed", "indicative"), ("limit", "1000")]
        pages = self._pages(
            path=f"/v1beta1/options/snapshots/{urllib.parse.quote(symbol)}",
            query=query,
            collection="snapshots",
        )
        snapshots: dict[str, OptionSnapshot] = {}
        for url, document, payload in pages:
            raw = document["snapshots"]
            assert isinstance(raw, dict)
            observed_values: list[datetime] = []
            parsed_values: list[tuple[str, dict[str, object], re.Match[str]]] = []
            for occ_symbol, value in raw.items():
                match = _OCC_SYMBOL.fullmatch(occ_symbol) if isinstance(occ_symbol, str) else None
                if match is None or not isinstance(value, dict) or match.group("root") != symbol:
                    raise ProviderDataError("indicative option snapshot is malformed")
                quote = value.get("latestQuote")
                if isinstance(quote, dict) and quote.get("t") is not None:
                    observed_values.append(_timestamp(quote.get("t"), "option quote"))
                parsed_values.append((occ_symbol, value, match))
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_OPTION_SNAPSHOTS",
                feed="indicative",
                source_timestamp=max(observed_values, default=self._current_time()),
            )
            for occ_symbol, value, match in parsed_values:
                if occ_symbol in snapshots:
                    raise ProviderIncompleteError("option snapshot is duplicated across pages")
                quote = value.get("latestQuote")
                greeks = value.get("greeks")
                daily_bar = value.get("dailyBar")
                bid: Decimal | None = None
                ask: Decimal | None = None
                observed_at: datetime | None = None
                if quote is not None:
                    if not isinstance(quote, dict):
                        raise ProviderDataError("indicative option quote is malformed")
                    bid = _decimal(quote.get("bp"), "option bid", positive=False)
                    ask = _decimal(quote.get("ap"), "option ask", positive=False)
                    if bid < 0 or ask < 0:
                        raise ProviderDataError("indicative option quote is negative")
                    if ask < bid:
                        raise ProviderDataError("indicative option quote is crossed")
                    observed_at = _timestamp(quote.get("t"), "option quote")
                delta: Decimal | None = None
                if greeks is not None:
                    if not isinstance(greeks, dict):
                        raise ProviderDataError("indicative option greeks are malformed")
                    delta = _decimal(greeks.get("delta"), "option delta", positive=False)
                    if not Decimal("-1") <= delta <= Decimal("1"):
                        raise ProviderDataError("indicative option delta is outside its bounds")
                volume: int | None = None
                if daily_bar is not None:
                    if not isinstance(daily_bar, dict):
                        raise ProviderDataError("indicative option daily bar is malformed")
                    volume = _integer(
                        daily_bar.get("v"),
                        "option daily volume",
                        optional=True,
                    )
                try:
                    expiration = datetime.strptime(
                        match.group("date"), "%y%m%d"
                    ).date()
                except ValueError:
                    raise ProviderDataError(
                        "indicative option expiration is malformed"
                    ) from None
                strike = Decimal(int(match.group("strike"))) / Decimal("1000")
                snapshots[occ_symbol] = OptionSnapshot(
                    occ_symbol=occ_symbol,
                    underlying=symbol,
                    expiration=expiration,
                    strike=strike,
                    delta=delta,
                    bid=bid,
                    ask=ask,
                    daily_volume=volume,
                    open_interest=None,
                    feed="indicative",
                    observed_at=observed_at,
                    source_observation_id=observation_id,
                )
        if not snapshots:
            raise ProviderIncompleteError("indicative option snapshot response is empty")
        return tuple(snapshots[name] for name in sorted(snapshots))

    def smoke(
        self,
        *,
        completed_session: TimeWindow | None = None,
    ) -> EntitlementSmoke:
        """Check authentication, delayed SIP access, and current IEX freshness separately."""
        now = self._current_time()
        authenticated = False
        iex_fresh = False
        sip_ok = False
        connectivity_failed = False
        availability_failed = False
        failures: list[str] = []
        try:
            self.latest_iex_quotes(["SPY"])
            authenticated = True
            iex_fresh = True
        except (ProviderDataError, ProviderIncompleteError):
            authenticated = True
            failures.append("IEX_FRESHNESS_UNAVAILABLE")
        except ProviderResponseError as error:
            text = str(error)
            if isinstance(error, HttpTransportError):
                connectivity_failed = True
            elif "HTTP 429" in text or re.search(r"HTTP 5[0-9]{2}", text):
                availability_failed = True
            elif "HTTP 401" not in text and "HTTP 403" not in text:
                authenticated = True
            failures.append("IEX_FRESHNESS_UNAVAILABLE")
        if authenticated:
            if not isinstance(completed_session, TimeWindow):
                failures.append("COMPLETED_SESSION_UNAVAILABLE")
            else:
                try:
                    self.daily_bars(["SPY"], completed_session)
                    sip_ok = True
                except ProviderResponseError as error:
                    text = str(error)
                    if isinstance(error, HttpTransportError):
                        connectivity_failed = True
                    elif "HTTP 429" in text or re.search(r"HTTP 5[0-9]{2}", text):
                        availability_failed = True
                    failures.append("HISTORICAL_SIP_UNAVAILABLE")
        else:
            failures.append("AUTHENTICATION_UNAVAILABLE")
        if connectivity_failed:
            status = "BLOCKED_CONNECTIVITY"
        elif availability_failed:
            status = "BLOCKED_AVAILABILITY"
        elif not authenticated:
            status = "BLOCKED_AUTHENTICATION"
        elif not iex_fresh:
            status = "BLOCKED_IEX_FRESHNESS"
        elif not sip_ok:
            status = "BLOCKED_ENTITLEMENT"
        else:
            status = "READY"
        return EntitlementSmoke(
            authentication_ok=authenticated,
            historical_sip_ok=sip_ok,
            latest_iex_fresh=iex_fresh,
            status=status,
            observed_at=now,
            failures=tuple(failures),
        )


__all__ = [
    "AlpacaCredentials",
    "AlpacaMarketData",
    "Bar",
    "EntitlementSmoke",
    "OptionSnapshot",
    "ProviderDataError",
    "ProviderIncompleteError",
    "Quote",
    "TimeWindow",
]
