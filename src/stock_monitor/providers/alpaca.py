"""Read-only Alpaca market-data adapter for SIP history and IEX freshness."""

from __future__ import annotations

import hashlib
import json
import math
import re
import urllib.parse
from array import array
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from gc import get_referents
from threading import RLock
from types import (
    CellType,
    CodeType,
    FunctionType,
    MappingProxyType,
    MemberDescriptorType,
    MethodType,
    ModuleType,
)
from weakref import ReferenceType, ref
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
    HttpStatusError,
    HttpTransportError,
    ProviderDataError,
    ProviderIncompleteError,
    ProviderMalformedError,
    ProviderResponseError,
    get_with_redirects,
)


_BASE_URL = "https://data.alpaca.markets"
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,9}\Z")
_OCC_SYMBOL = re.compile(
    r"(?P<root>[A-Z]{1,6})(?P<date>[0-9]{6})(?P<right>[CP])(?P<strike>[0-9]{8})\Z"
)
HISTORICAL_SIP_RELEASE_DELAY = timedelta(minutes=16)
_LATEST_MAX_AGE_SECONDS = 300
_MAX_PAGES = 100
_NEW_YORK = ZoneInfo("America/New_York")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
_PAGE_SOURCE_CONTRACTS = {
    "ALPACA_DAILY_BARS": ("bars", "/v2/stocks/bars", False),
    "ALPACA_INTRADAY_BARS": ("bars", "/v2/stocks/bars", False),
    "ALPACA_HISTORICAL_QUOTES": (
        "quotes",
        "/v2/stocks/quotes",
        False,
    ),
    "ALPACA_HISTORICAL_TRADES": (
        "trades",
        "/v2/stocks/trades",
        False,
    ),
    "ALPACA_LATEST_QUOTES": (
        "quotes",
        "/v2/stocks/quotes/latest",
        True,
    ),
    "ALPACA_OPTION_SNAPSHOTS": (
        "snapshots",
        "/v1beta1/options/snapshots/",
        True,
    ),
}


class ProviderStaleError(ProviderDataError):
    """A structurally valid current-data observation is too old."""


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
        raise ProviderMalformedError(f"{name} timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ProviderMalformedError(f"{name} timestamp is malformed") from None
    try:
        return _utc(parsed, f"{name} timestamp")
    except ValueError:
        raise ProviderMalformedError(f"{name} timestamp is malformed") from None


def _decimal(value: object, name: str, *, positive: bool = True) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ProviderMalformedError(f"{name} is malformed")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ProviderMalformedError(f"{name} is malformed") from None
    if not result.is_finite() or (positive and result <= 0):
        raise ProviderMalformedError(f"{name} is malformed")
    return result


def _integer(value: object, name: str, *, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ProviderMalformedError(f"{name} is malformed")
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        raise ProviderMalformedError(f"{name} is malformed") from None
    if str(result) != str(value) and not (
        isinstance(value, str) and value.isdigit() and int(value) == result
    ):
        raise ProviderMalformedError(f"{name} is malformed")
    if result < 0:
        raise ProviderMalformedError(f"{name} is malformed")
    return result


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ProviderMalformedError("provider JSON contains duplicate fields")
        result[name] = value
    return result


def _json_object(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_object_pairs)
    except (UnicodeError, json.JSONDecodeError):
        raise ProviderMalformedError("provider response is not valid JSON") from None
    if not isinstance(value, dict):
        raise ProviderMalformedError("provider JSON root must be an object")
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
class AlpacaPageMetadata:
    source_observation_id: str
    source_time: datetime
    retrieved_at: datetime
    delay_seconds: int
    payload_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.source_observation_id) is not str
            or not self.source_observation_id.startswith("obs-")
            or _SHA256_HEX.fullmatch(self.payload_sha256) is None
            or type(self.delay_seconds) is not int
            or self.delay_seconds < 0
        ):
            raise ProviderMalformedError("Alpaca page metadata is malformed")
        source_time = _utc(self.source_time, "source time")
        retrieved_at = _utc(self.retrieved_at, "retrieved time")
        if source_time > retrieved_at:
            raise ProviderMalformedError("provider source timestamp is in the future")
        if int((retrieved_at - source_time).total_seconds()) != self.delay_seconds:
            raise ProviderMalformedError("Alpaca page delay is inconsistent")
        object.__setattr__(self, "source_time", source_time)
        object.__setattr__(self, "retrieved_at", retrieved_at)


@dataclass(frozen=True, slots=True)
class ProviderFetchPage:
    page_ordinal: int
    source_observation_id: str
    source_type: str
    request_url: str
    request_page_token: str | None
    next_page_token: str | None
    payload_sha256: str

    def __post_init__(self) -> None:
        if type(self.page_ordinal) is not int or self.page_ordinal <= 0:
            raise ProviderMalformedError("provider fetch page ordinal is malformed")
        for value in (
            self.source_observation_id,
            self.source_type,
            self.request_url,
        ):
            if type(value) is not str or not value:
                raise ProviderMalformedError("provider fetch page identity is malformed")
        for token in (self.request_page_token, self.next_page_token):
            if token is not None and (type(token) is not str or not token):
                raise ProviderMalformedError("provider fetch page token is malformed")
        if _SHA256_HEX.fullmatch(self.payload_sha256) is None:
            raise ProviderMalformedError("provider fetch page digest is malformed")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ProviderFetchManifest:
    collection: str
    requested_symbols: tuple[str, ...]
    request_digest: str
    pages: tuple[ProviderFetchPage, ...]
    terminal: bool
    manifest_digest: str

    def __post_init__(self) -> None:
        symbols = tuple(self.requested_symbols)
        pages = tuple(self.pages)
        object.__setattr__(self, "requested_symbols", symbols)
        object.__setattr__(self, "pages", pages)
        if (
            self.collection not in {"bars", "quotes", "trades", "snapshots"}
            or not symbols
            or symbols != tuple(sorted(set(symbols)))
            or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
            or _SHA256_HEX.fullmatch(self.request_digest) is None
            or _SHA256_HEX.fullmatch(self.manifest_digest) is None
            or self.terminal is not True
            or not pages
            or tuple(page.page_ordinal for page in pages)
            != tuple(range(1, len(pages) + 1))
            or pages[0].request_page_token is not None
            or pages[-1].next_page_token is not None
            or len({page.source_observation_id for page in pages}) != len(pages)
            or any(
                successor.request_page_token != prior.next_page_token
                or prior.next_page_token is None
                for prior, successor in zip(pages, pages[1:], strict=False)
            )
        ):
            raise ProviderMalformedError("provider fetch manifest is malformed")


@dataclass(frozen=True, slots=True)
class NormalizedMarketFactSource:
    kind: str
    symbol: str
    feed: str
    source_observation_id: str
    page_ordinal: int
    source_item_ordinal: int
    source_item_path: str
    page_payload_sha256: str
    normalized_fields_digest: str
    fetch_manifest: ProviderFetchManifest

    def __post_init__(self) -> None:
        if (
            self.kind not in {"BAR", "QUOTE", "TRADE", "OPTION_SNAPSHOT"}
            or _SYMBOL.fullmatch(self.symbol) is None
            or type(self.feed) is not str
            or not self.feed
            or type(self.source_observation_id) is not str
            or not self.source_observation_id
            or type(self.page_ordinal) is not int
            or self.page_ordinal <= 0
            or type(self.source_item_ordinal) is not int
            or self.source_item_ordinal <= 0
            or type(self.source_item_path) is not str
            or not self.source_item_path
            or _SHA256_HEX.fullmatch(self.page_payload_sha256) is None
            or _SHA256_HEX.fullmatch(self.normalized_fields_digest) is None
            or not isinstance(self.fetch_manifest, ProviderFetchManifest)
        ):
            raise ProviderMalformedError("normalized market fact source is malformed")
        matching_pages = tuple(
            page
            for page in self.fetch_manifest.pages
            if page.page_ordinal == self.page_ordinal
        )
        if (
            len(matching_pages) != 1
            or matching_pages[0].source_observation_id
            != self.source_observation_id
            or matching_pages[0].payload_sha256 != self.page_payload_sha256
        ):
            raise ProviderMalformedError("normalized market fact page is inconsistent")


@dataclass(frozen=True, slots=True, weakref_slot=True)
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


@dataclass(frozen=True, slots=True, weakref_slot=True)
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


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Trade:
    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    feed: str
    sequence: int | None
    source_observation_id: str

    @property
    def t(self) -> datetime:
        return self.timestamp

    @property
    def p(self) -> Decimal:
        return self.price


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ProviderFetchCohort(Mapping[str, tuple[Bar | Quote | Trade, ...]]):
    _entries: tuple[
        tuple[str, tuple[Bar | Quote | Trade, ...]],
        ...,
    ]

    def __post_init__(self) -> None:
        entries = tuple(
            (symbol, tuple(values)) for symbol, values in self._entries
        )
        object.__setattr__(self, "_entries", entries)
        if (
            not entries
            or tuple(symbol for symbol, _values in entries)
            != tuple(sorted({symbol for symbol, _values in entries}))
            or any(
                _SYMBOL.fullmatch(symbol) is None
                or any(
                    not isinstance(value, (Bar, Quote, Trade))
                    or value.symbol != symbol
                    for value in values
                )
                for symbol, values in entries
            )
        ):
            raise ProviderMalformedError("provider fetch cohort is malformed")

    def __getitem__(self, symbol: str) -> tuple[Bar | Quote | Trade, ...]:
        for candidate, values in self._entries:
            if candidate == symbol:
                return values
        raise KeyError(symbol)

    def __iter__(self) -> Iterator[str]:
        return iter(symbol for symbol, _values in self._entries)

    def __len__(self) -> int:
        return len(self._entries)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ProviderFetchPageBundle:
    page: ProviderFetchPage
    payload: bytes
    observation: SourceObservation


@dataclass(frozen=True, slots=True)
class ProviderFetchBundle:
    manifest: ProviderFetchManifest
    pages: tuple[ProviderFetchPageBundle, ...]


@dataclass(frozen=True, slots=True)
class _IssuedNormalizedMarketFact:
    reference: ReferenceType[object]
    fingerprint: str
    source_fingerprint: str
    owner: object
    source: NormalizedMarketFactSource


@dataclass(frozen=True, slots=True)
class _IssuedProviderFetchManifest:
    reference: ReferenceType[ProviderFetchManifest]
    fingerprint: str
    raw_pages: tuple[bytes, ...]
    observations: tuple[SourceObservation, ...]
    observation_fingerprints: tuple[str, ...]
    page_bundles: tuple[ProviderFetchPageBundle, ...]


@dataclass(frozen=True, slots=True)
class _IssuedProviderFetchPageBundle:
    reference: ReferenceType[ProviderFetchPageBundle]
    fingerprint: str
    observation_fingerprint: str
    page: ProviderFetchPage
    payload: bytes
    observation: SourceObservation
    owner: object


@dataclass(frozen=True, slots=True, repr=False)
class _RequestGraphNode:
    value: object
    kind: str
    shape: tuple[str | bytes | int | bool, ...]
    references: tuple[object, ...]
    descend_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True, repr=False)
class _RequestGraphSnapshot:
    roots: tuple[object, ...]
    nodes: tuple[_RequestGraphNode, ...]


@dataclass(slots=True, repr=False)
class _MutableRequestGraphDependency:
    roots: tuple[object, ...]
    snapshot: _RequestGraphSnapshot


@dataclass(frozen=True, slots=True, repr=False)
class _AlpacaRequestDependencies:
    transport: GetTransport
    credentials: AlpacaCredentials = field(repr=False)
    credential_key_id: str = field(repr=False)
    credential_secret_key: str = field(repr=False)
    base_url: str
    policy: EgressPolicy
    policy_allowed_hosts: frozenset[str]
    now: Callable[[], datetime]
    cache: ContentCache | None
    cache_put_function: Callable[[SourceObservation, bytes], object] | None = field(
        repr=False
    )
    observations: dict[str, SourceObservation]
    provider_authority_lock: object
    issued_fetch_manifests: dict[int, _IssuedProviderFetchManifest]
    namespace_dependencies: tuple[
        tuple[Mapping[str, object], tuple[tuple[str, object], ...]],
        ...,
    ] = field(repr=False)
    class_dependencies: tuple[
        tuple[type, tuple[tuple[str, object], ...]],
        ...,
    ] = field(repr=False)
    mro_dependencies: tuple[
        tuple[type, tuple[type, ...]],
        ...,
    ] = field(repr=False)
    instance_dependencies: tuple[
        tuple[dict[str, object], tuple[tuple[str, object], ...]],
        ...,
    ] = field(repr=False)
    graph_dependencies: tuple[
        _MutableRequestGraphDependency,
        ...,
    ] = field(repr=False)
    transport_graph_dependency: (
        _MutableRequestGraphDependency | None
    ) = field(repr=False)
    cache_graph_dependency: (
        _MutableRequestGraphDependency | None
    ) = field(repr=False)
    clock_graph_dependency: (
        _MutableRequestGraphDependency | None
    ) = field(repr=False)
    function_dependencies: tuple[
        tuple[
            FunctionType,
            object,
            object,
            tuple[tuple[str, object], ...] | None,
            object,
            object,
        ],
        ...,
    ] = field(repr=False)
    missing_dependency: object = field(repr=False)
    request_verifier: Callable[..., None] = field(repr=False)
    request_verifier_code: object = field(repr=False)
    request_dispatch: Callable[..., tuple[dict[str, object], bytes]] = field(
        repr=False
    )
    pages_function: Callable[
        ..., Iterator[tuple[str, dict[str, object], bytes]]
    ] = field(repr=False)
    issue_page_function: Callable[..., ProviderFetchPageBundle] = field(
        repr=False
    )
    require_pages_function: Callable[..., None] = field(repr=False)
    transport_get_function: Callable[..., object] = field(repr=False)
    get_with_redirects_function: Callable[..., object] = field(repr=False)
    json_object_function: Callable[[bytes], dict[str, object]] = field(
        repr=False
    )
    sealed_transport: object = field(repr=False)


_REQUEST_GRAPH_MISSING = object()


def _request_graph_global_target_is_mutable(value: object) -> bool:
    value_type = type(value)
    return not (
        value is None
        or value_type
        in (
            bool,
            bytes,
            CodeType,
            complex,
            date,
            datetime,
            Decimal,
            float,
            FunctionType,
            int,
            ModuleType,
            str,
            timedelta,
            ZoneInfo,
        )
        or isinstance(value, type)
    )


def _request_graph_node(value: object) -> _RequestGraphNode:
    value_type = type(value)
    references: list[object] = []
    descend_indices: list[int] = []

    def reference(target: object, *, descend: bool) -> None:
        index = len(references)
        references.append(target)
        if descend:
            descend_indices.append(index)

    if value_type is dict:
        items = tuple(dict.items(value))
        for key, item in items:
            reference(key, descend=True)
            reference(item, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="dict",
            shape=(len(items),),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is list:
        for item in list.__iter__(value):
            reference(item, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="list",
            shape=(len(references),),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is deque:
        for item in deque.__iter__(value):
            reference(item, descend=True)
        max_length = value.maxlen
        return _RequestGraphNode(
            value=value,
            kind="deque",
            shape=(
                max_length is None,
                0 if max_length is None else max_length,
                len(references),
            ),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is MappingProxyType:
        referents = tuple(get_referents(value))
        if len(referents) != 1 or type(referents[0]) is not dict:
            raise ProviderMalformedError(
                "provider request dependency mapping proxy is unsupported"
            )
        reference(referents[0], descend=True)
        return _RequestGraphNode(
            value=value,
            kind="mappingproxy",
            shape=(),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is array:
        return _RequestGraphNode(
            value=value,
            kind="array",
            shape=(
                array.typecode.__get__(value, array),
                array.tobytes(value),
            ),
            references=(),
            descend_indices=(),
        )
    if value_type is tuple:
        for item in tuple.__iter__(value):
            reference(item, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="tuple",
            shape=(len(references),),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type in (set, frozenset):
        iterator = (
            set.__iter__(value)
            if value_type is set
            else frozenset.__iter__(value)
        )
        for item in sorted(iterator, key=id):
            reference(item, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="set" if value_type is set else "frozenset",
            shape=(len(references),),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is bytearray:
        return _RequestGraphNode(
            value=value,
            kind="bytearray",
            shape=tuple(bytearray.__iter__(value)),
            references=(),
            descend_indices=(),
        )
    if value_type is FunctionType:
        code = value.__code__
        function_globals = value.__globals__
        defaults = value.__defaults__
        keyword_defaults = value.__kwdefaults__
        closure = value.__closure__
        attributes = value.__dict__
        function_builtins = value.__builtins__
        reference(code, descend=False)
        reference(function_globals, descend=False)
        for target in (
            defaults,
            keyword_defaults,
            closure,
            attributes,
        ):
            reference(target, descend=target is not None)
        raw_referents = tuple(sorted(get_referents(value), key=id))
        for target in raw_referents:
            reference(
                target,
                descend=(
                    target is not function_globals
                    and target is not function_builtins
                    and (
                        type(target) is FunctionType
                        or _request_graph_global_target_is_mutable(
                            target
                        )
                    )
                ),
            )
        names = tuple(sorted(frozenset(code.co_names)))
        shape: list[str | bytes | int | bool] = [len(names)]
        for name in names:
            target = dict.get(
                function_globals,
                name,
                _REQUEST_GRAPH_MISSING,
            )
            present = target is not _REQUEST_GRAPH_MISSING
            shape.extend((name, present))
            if present:
                reference(
                    target,
                    descend=_request_graph_global_target_is_mutable(
                        target
                    ),
                )
                if type(target) is ModuleType:
                    module_namespace = ModuleType.__getattribute__(
                        target,
                        "__dict__",
                    )
                    reference(module_namespace, descend=False)
                    shape.append(len(names))
                    for attribute_name in names:
                        attribute = dict.get(
                            module_namespace,
                            attribute_name,
                            _REQUEST_GRAPH_MISSING,
                        )
                        attribute_present = (
                            attribute is not _REQUEST_GRAPH_MISSING
                        )
                        shape.extend(
                            (attribute_name, attribute_present)
                        )
                        if attribute_present:
                            reference(attribute, descend=True)
                else:
                    shape.append(0)
        return _RequestGraphNode(
            value=value,
            kind="function",
            shape=tuple(shape),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is MethodType:
        reference(value.__self__, descend=True)
        reference(value.__func__, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="method",
            shape=(),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if value_type is CellType:
        try:
            target = value.cell_contents
        except ValueError:
            present = False
        else:
            present = True
            reference(target, descend=True)
        return _RequestGraphNode(
            value=value,
            kind="cell",
            shape=(present,),
            references=tuple(references),
            descend_indices=tuple(descend_indices),
        )
    if (
        value is None
        or value_type
        in (
            bool,
            bytes,
            CodeType,
            complex,
            date,
            datetime,
            Decimal,
            float,
            int,
            ModuleType,
            str,
            timedelta,
            ZoneInfo,
        )
        or isinstance(value, type)
    ):
        return _RequestGraphNode(
            value=value,
            kind="leaf",
            shape=(),
            references=(),
            descend_indices=(),
        )

    shape = []
    reference(value_type, descend=False)
    try:
        namespace = object.__getattribute__(value, "__dict__")
    except Exception:
        namespace = _REQUEST_GRAPH_MISSING
    has_namespace = type(namespace) is dict
    shape.append(has_namespace)
    if has_namespace:
        reference(namespace, descend=True)

    try:
        hierarchy = tuple(type.__getattribute__(value_type, "__mro__"))
    except Exception:
        hierarchy = ()
    shape.append(len(hierarchy))
    for candidate in hierarchy:
        reference(candidate, descend=False)
        try:
            class_namespace = type.__getattribute__(candidate, "__dict__")
            items = tuple(class_namespace.items())
        except Exception:
            items = ()
        shape.append(len(items))
        for name, descriptor in items:
            shape.append(name)
            reference(descriptor, descend=type(descriptor) is FunctionType)
            functions: tuple[object, ...]
            if type(descriptor) in (staticmethod, classmethod):
                functions = (descriptor.__func__,)
            elif type(descriptor) is property:
                functions = (
                    descriptor.fget,
                    descriptor.fset,
                    descriptor.fdel,
                )
            else:
                functions = ()
            functions = tuple(
                function
                for function in functions
                if type(function) is FunctionType
            )
            shape.append(len(functions))
            for function in functions:
                reference(function, descend=True)
            if type(descriptor) is MemberDescriptorType:
                try:
                    slot_value = descriptor.__get__(value, value_type)
                except AttributeError:
                    slot_present = False
                else:
                    slot_present = True
                    reference(slot_value, descend=True)
                shape.append(slot_present)
            else:
                shape.append(False)
    return _RequestGraphNode(
        value=value,
        kind="object",
        shape=tuple(shape),
        references=tuple(references),
        descend_indices=tuple(descend_indices),
    )


def _capture_request_graph(
    roots: tuple[object, ...],
) -> _RequestGraphSnapshot:
    nodes: list[_RequestGraphNode] = []
    seen: dict[int, object] = {}
    pending = list(reversed(roots))
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            if seen[identity] is value:
                continue
            raise ProviderMalformedError(
                "provider request dependency identity is inconsistent"
            )
        seen[identity] = value
        node = _request_graph_node(value)
        nodes.append(node)
        for index in reversed(node.descend_indices):
            pending.append(node.references[index])
    return _RequestGraphSnapshot(roots=roots, nodes=tuple(nodes))


def _request_graph_shapes_are_exact(
    current: tuple[str | bytes | int | bool, ...],
    expected: tuple[str | bytes | int | bool, ...],
) -> bool:
    return len(current) == len(expected) and all(
        type(current_value) is type(expected_value)
        and current_value == expected_value
        for current_value, expected_value in zip(
            current,
            expected,
            strict=True,
        )
    )


def _request_graph_snapshots_are_exact(
    current: _RequestGraphSnapshot,
    expected: _RequestGraphSnapshot,
) -> bool:
    if len(current.roots) != len(expected.roots) or any(
        current_root is not expected_root
        for current_root, expected_root in zip(
            current.roots,
            expected.roots,
            strict=True,
        )
    ):
        return False
    if len(current.nodes) != len(expected.nodes):
        return False
    for current_node, expected_node in zip(
        current.nodes,
        expected.nodes,
        strict=True,
    ):
        if (
            current_node.value is not expected_node.value
            or current_node.kind != expected_node.kind
            or not _request_graph_shapes_are_exact(
                current_node.shape,
                expected_node.shape,
            )
            or current_node.descend_indices
            != expected_node.descend_indices
            or len(current_node.references)
            != len(expected_node.references)
            or any(
                current_reference is not expected_reference
                for current_reference, expected_reference in zip(
                    current_node.references,
                    expected_node.references,
                    strict=True,
                )
            )
        ):
            return False
    return True


def _request_graph_dependency_is_current(
    dependency: _MutableRequestGraphDependency,
) -> bool:
    try:
        current = _capture_request_graph(dependency.roots)
    except Exception:
        return False
    return _request_graph_snapshots_are_exact(
        current,
        dependency.snapshot,
    )


def _require_current_request_graph(
    dependency: _MutableRequestGraphDependency | None,
) -> None:
    if dependency is not None and not _request_graph_dependency_is_current(
        dependency
    ):
        raise ProviderMalformedError(
            "provider request dependencies changed during pagination"
        )


def _refresh_trusted_request_graph(
    dependency: _MutableRequestGraphDependency | None,
) -> None:
    if dependency is None:
        return
    failed = False
    try:
        snapshot = _capture_request_graph(dependency.roots)
    except Exception:
        failed = True
        snapshot = dependency.snapshot
    if failed:
        raise ProviderMalformedError(
            "provider request dependencies changed during pagination"
        )
    dependency.snapshot = snapshot


@dataclass(frozen=True, slots=True, repr=False)
class _SealedRequestTransport:
    get_function: Callable[..., object]
    graph_dependency: _MutableRequestGraphDependency

    def get(self, url: str, headers: Mapping[str, str]) -> object:
        _require_current_request_graph(self.graph_dependency)
        try:
            return self.get_function(url, headers)
        finally:
            _refresh_trusted_request_graph(self.graph_dependency)


@dataclass(frozen=True, slots=True)
class _IssuedProviderFetchCohort:
    reference: ReferenceType[ProviderFetchCohort]
    fingerprint: str
    owner: object
    manifest: ProviderFetchManifest
    ingestible: bool


_ISSUED_NORMALIZED_MARKET_FACTS: dict[int, _IssuedNormalizedMarketFact] = {}
_ISSUED_NORMALIZED_MARKET_FACTS_LOCK = RLock()
_ISSUED_PROVIDER_FETCH_PAGE_BUNDLES: dict[
    int,
    _IssuedProviderFetchPageBundle,
] = {}
_ISSUED_PROVIDER_FETCH_PAGE_BUNDLES_LOCK = RLock()
_ISSUED_PROVIDER_FETCH_COHORTS: dict[int, _IssuedProviderFetchCohort] = {}
_ISSUED_PROVIDER_FETCH_COHORTS_LOCK = RLock()
_REPLAY_ONLY_PROVIDER_FETCH_SCOPES: dict[
    tuple[int, int],
    tuple[object, ProviderFetchManifest],
] = {}
_REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK = RLock()


def _provider_fetch_scope_is_replay_only(
    owner: object,
    manifest: ProviderFetchManifest,
) -> bool:
    with _REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK:
        scope = _REPLAY_ONLY_PROVIDER_FETCH_SCOPES.get(
            (id(owner), id(manifest))
        )
        return (
            scope is not None
            and scope[0] is owner
            and scope[1] is manifest
        )


def _canonical_digest(namespace: str, payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            {"namespace": namespace, "payload": payload},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _market_fact_payload(
    fact: Bar | Quote | Trade | OptionSnapshot,
) -> dict[str, object]:
    if isinstance(fact, OptionSnapshot):
        return {
            "kind": "OPTION_SNAPSHOT",
            "occ_symbol": fact.occ_symbol,
            "underlying": fact.underlying,
            "expiration": fact.expiration.isoformat(),
            "strike": str(fact.strike),
            "delta": None if fact.delta is None else str(fact.delta),
            "bid": None if fact.bid is None else str(fact.bid),
            "ask": None if fact.ask is None else str(fact.ask),
            "daily_volume": fact.daily_volume,
            "open_interest": fact.open_interest,
            "feed": fact.feed,
            "observed_at": (
                None
                if fact.observed_at is None
                else _format_utc(fact.observed_at)
            ),
            "source_observation_id": fact.source_observation_id,
        }
    timestamp = _format_utc(fact.timestamp)
    if isinstance(fact, Bar):
        return {
            "kind": "BAR",
            "symbol": fact.symbol,
            "timestamp": timestamp,
            "open": str(fact.open),
            "high": str(fact.high),
            "low": str(fact.low),
            "close": str(fact.close),
            "volume": fact.volume,
            "feed": fact.feed,
            "adjustment": fact.adjustment,
            "source_observation_id": fact.source_observation_id,
        }
    if isinstance(fact, Quote):
        return {
            "kind": "QUOTE",
            "symbol": fact.symbol,
            "timestamp": timestamp,
            "bid": str(fact.bid),
            "ask": str(fact.ask),
            "feed": fact.feed,
            "sequence": fact.sequence,
            "age_seconds": fact.age_seconds,
            "source_observation_id": fact.source_observation_id,
        }
    if isinstance(fact, Trade):
        return {
            "kind": "TRADE",
            "symbol": fact.symbol,
            "timestamp": timestamp,
            "price": str(fact.price),
            "size": fact.size,
            "feed": fact.feed,
            "sequence": fact.sequence,
            "source_observation_id": fact.source_observation_id,
        }
    raise TypeError("normalized market fact has the wrong type")


def _market_fact_fingerprint(
    fact: Bar | Quote | Trade | OptionSnapshot,
) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-normalized-market-fact/v1",
        _market_fact_payload(fact),
    )


def _provider_fetch_manifest_payload(
    manifest: ProviderFetchManifest,
) -> dict[str, object]:
    return {
        "collection": manifest.collection,
        "requested_symbols": list(manifest.requested_symbols),
        "request_digest": manifest.request_digest,
        "pages": [
            {
                "page_ordinal": page.page_ordinal,
                "source_observation_id": page.source_observation_id,
                "source_type": page.source_type,
                "request_url": page.request_url,
                "request_page_token": page.request_page_token,
                "next_page_token": page.next_page_token,
                "payload_sha256": page.payload_sha256,
            }
            for page in manifest.pages
        ],
        "terminal": manifest.terminal,
        "manifest_digest": manifest.manifest_digest,
    }


def _provider_fetch_manifest_fingerprint(
    manifest: ProviderFetchManifest,
) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-issued-fetch-manifest/v1",
        _provider_fetch_manifest_payload(manifest),
    )


def _provider_fetch_page_bundle_fingerprint(
    bundle: ProviderFetchPageBundle,
) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-issued-fetch-page-bundle/v1",
        {
            "page": {
                "page_ordinal": bundle.page.page_ordinal,
                "source_observation_id": bundle.page.source_observation_id,
                "source_type": bundle.page.source_type,
                "request_url": bundle.page.request_url,
                "request_page_token": bundle.page.request_page_token,
                "next_page_token": bundle.page.next_page_token,
                "payload_sha256": bundle.page.payload_sha256,
            },
            "payload_sha256": hashlib.sha256(bundle.payload).hexdigest(),
            "observation": _source_observation_fingerprint(
                bundle.observation
            ),
        },
    )


def _provider_fetch_page_bundle_authority(
    bundle: object,
) -> _IssuedProviderFetchPageBundle | None:
    if not isinstance(bundle, ProviderFetchPageBundle):
        return None
    try:
        fingerprint = _provider_fetch_page_bundle_fingerprint(bundle)
    except Exception:
        return None
    with _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES_LOCK:
        issued = _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES.get(id(bundle))
        owner = None if issued is None else issued.owner
        if (
            issued is None
            or issued.reference() is not bundle
            or issued.fingerprint != fingerprint
            or bundle.page is not issued.page
            or bundle.payload is not issued.payload
            or bundle.observation is not issued.observation
            or not isinstance(owner, AlpacaMarketData)
        ):
            return None
        try:
            is_current = (
                _source_observation_fingerprint(bundle.observation)
                == issued.observation_fingerprint
                and owner._observations.get(
                    bundle.page.source_observation_id
                )
                is bundle.observation
                and bundle.page.source_observation_id
                == bundle.observation.observation_id
                and bundle.page.request_url == bundle.observation.url
                and bundle.page.source_type == bundle.observation.source_type
                and hashlib.sha256(bundle.payload).hexdigest()
                == bundle.page.payload_sha256
                and _source_observation_id_for_payload(
                    source_type=bundle.page.source_type,
                    url=bundle.page.request_url,
                    retrieved_at=bundle.observation.retrieved_at,
                    payload=bundle.payload,
                )
                == bundle.page.source_observation_id
            )
        except Exception:
            return None
        if not is_current:
            return None
        return issued


def is_issued_provider_fetch_page_bundle(bundle: object) -> bool:
    """Return whether *bundle* is one exact current provider-issued page."""
    return _provider_fetch_page_bundle_authority(bundle) is not None


def _issue_provider_fetch_page_bundle(
    *,
    owner: object,
    page: ProviderFetchPage,
    payload: bytes,
    observation: SourceObservation,
) -> ProviderFetchPageBundle:
    if not isinstance(owner, AlpacaMarketData):
        raise ProviderMalformedError("provider fetch page owner is unverified")
    bundle = ProviderFetchPageBundle(
        page=page,
        payload=payload,
        observation=observation,
    )
    if (
        owner._observations.get(page.source_observation_id) is not observation
        or page.source_observation_id != observation.observation_id
        or page.request_url != observation.url
        or page.source_type != observation.source_type
        or hashlib.sha256(payload).hexdigest() != page.payload_sha256
        or _source_observation_id_for_payload(
            source_type=page.source_type,
            url=page.request_url,
            retrieved_at=observation.retrieved_at,
            payload=payload,
        )
        != page.source_observation_id
    ):
        raise ProviderMalformedError(
            "provider fetch page does not match its pinned observation"
        )
    identity = id(bundle)

    def discard(dead: ReferenceType[ProviderFetchPageBundle]) -> None:
        with _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES_LOCK:
            current = _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES.pop(identity, None)

    authority = _IssuedProviderFetchPageBundle(
        reference=ref(bundle, discard),
        fingerprint=_provider_fetch_page_bundle_fingerprint(bundle),
        observation_fingerprint=_source_observation_fingerprint(observation),
        page=page,
        payload=payload,
        observation=observation,
        owner=owner,
    )
    with _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES_LOCK:
        _ISSUED_PROVIDER_FETCH_PAGE_BUNDLES[identity] = authority
    return bundle


def _source_observation_fingerprint(observation: SourceObservation) -> str:
    if type(observation) is not SourceObservation:
        raise TypeError("source observation has the wrong type")
    return _canonical_digest(
        "stock-monitor/alpaca-source-observation/v1",
        {
            "observation_id": observation.observation_id,
            "url": observation.url,
            "source_type": observation.source_type,
            "source_timestamp": _format_utc(
                observation.source_timestamp
            ),
            "retrieved_at": _format_utc(observation.retrieved_at),
            "feed": observation.feed,
            "delay_seconds": observation.delay_seconds,
            "content_hash": observation.content_hash,
        },
    )


def _normalized_market_fact_source_fingerprint(
    source: NormalizedMarketFactSource,
) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-normalized-market-fact-source/v1",
        {
            "kind": source.kind,
            "symbol": source.symbol,
            "feed": source.feed,
            "source_observation_id": source.source_observation_id,
            "page_ordinal": source.page_ordinal,
            "source_item_ordinal": source.source_item_ordinal,
            "source_item_path": source.source_item_path,
            "page_payload_sha256": source.page_payload_sha256,
            "normalized_fields_digest": source.normalized_fields_digest,
            "fetch_manifest": _provider_fetch_manifest_payload(
                source.fetch_manifest
            ),
        },
    )


def _normalized_market_fact_authority(
    fact: object,
) -> _IssuedNormalizedMarketFact | None:
    if not isinstance(fact, (Bar, Quote, Trade, OptionSnapshot)):
        return None
    try:
        fingerprint = _market_fact_fingerprint(fact)
    except Exception:
        return None
    with _ISSUED_NORMALIZED_MARKET_FACTS_LOCK:
        issued = _ISSUED_NORMALIZED_MARKET_FACTS.get(id(fact))
        if (
            issued is None
            or issued.reference() is not fact
            or issued.fingerprint != fingerprint
            or issued.source_fingerprint
            != _normalized_market_fact_source_fingerprint(issued.source)
            or _issued_provider_fetch_manifest(
                issued.owner,
                issued.source.fetch_manifest,
            )
            is None
        ):
            return None
        return issued


def is_issued_normalized_market_fact(fact: object) -> bool:
    return _normalized_market_fact_authority(fact) is not None


def _normalized_market_fact_source(fact: object) -> NormalizedMarketFactSource:
    issued = _normalized_market_fact_authority(fact)
    if issued is None:
        raise ValueError("normalized market fact authority is unverified")
    return issued.source


def normalized_market_facts_share_owner(left: object, right: object) -> bool:
    left_authority = _normalized_market_fact_authority(left)
    right_authority = _normalized_market_fact_authority(right)
    return (
        left_authority is not None
        and right_authority is not None
        and left_authority.owner is right_authority.owner
    )


def _provider_fetch_cohort_fingerprint(
    cohort: ProviderFetchCohort,
) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-provider-fetch-cohort/v1",
        {
            "entries": [
                {
                    "symbol": symbol,
                    "facts": [
                        _market_fact_payload(fact) for fact in facts
                    ],
                }
                for symbol, facts in cohort._entries
            ]
        },
    )


def _issue_provider_fetch_cohort(
    *,
    owner: object,
    manifest: ProviderFetchManifest,
    values: Mapping[str, tuple[Bar | Quote | Trade, ...]],
) -> ProviderFetchCohort:
    issued_fetch = _issued_provider_fetch_manifest(owner, manifest)
    if issued_fetch is None:
        raise ProviderMalformedError("provider fetch cohort authority is unverified")
    entries = tuple(
        (symbol, tuple(values[symbol])) for symbol in sorted(values)
    )
    if tuple(symbol for symbol, _facts in entries) != manifest.requested_symbols:
        raise ProviderMalformedError("provider fetch cohort symbols are incomplete")
    actual_by_coordinate: dict[
        tuple[int, int, str],
        Bar | Quote | Trade,
    ] = {}
    for _symbol, facts in entries:
        for fact in facts:
            authority = _normalized_market_fact_authority(fact)
            if (
                authority is None
                or authority.owner is not owner
                or authority.source.fetch_manifest is not manifest
            ):
                raise ProviderMalformedError(
                    "provider fetch cohort fact authority is unverified"
                )
            coordinate = (
                authority.source.page_ordinal,
                authority.source.source_item_ordinal,
                authority.source.source_item_path,
            )
            if coordinate in actual_by_coordinate:
                raise ProviderMalformedError(
                    "provider fetch cohort duplicates a raw provider item"
                )
            actual_by_coordinate[coordinate] = fact
    expected_by_coordinate: dict[
        tuple[int, int, str],
        Bar | Quote | Trade,
    ] = {}
    for page, payload, observation in zip(
        manifest.pages,
        issued_fetch.raw_pages,
        issued_fetch.observations,
        strict=True,
    ):
        for item_ordinal, item_path, _symbol, _value in _raw_provider_items(
            manifest=manifest,
            page=page,
            payload=payload,
        ):
            coordinate = (page.page_ordinal, item_ordinal, item_path)
            expected_by_coordinate[coordinate] = (
                _market_fact_from_raw_provider_item(
                    manifest=manifest,
                    page=page,
                    observation=observation,
                    payload=payload,
                    source_item_ordinal=item_ordinal,
                    source_item_path=item_path,
                )
            )
    if (
        actual_by_coordinate.keys() != expected_by_coordinate.keys()
        or any(
            actual_by_coordinate[coordinate] != expected_fact
            for coordinate, expected_fact in expected_by_coordinate.items()
        )
    ):
        raise ProviderMalformedError(
            "provider fetch cohort is not the complete raw provider item cohort"
        )
    cohort = ProviderFetchCohort(entries)
    identity = id(cohort)

    def discard(dead: ReferenceType[ProviderFetchCohort]) -> None:
        with _ISSUED_PROVIDER_FETCH_COHORTS_LOCK:
            current = _ISSUED_PROVIDER_FETCH_COHORTS.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_PROVIDER_FETCH_COHORTS.pop(identity, None)

    with _REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK:
        authority = _IssuedProviderFetchCohort(
            reference=ref(cohort, discard),
            fingerprint=_provider_fetch_cohort_fingerprint(cohort),
            owner=owner,
            manifest=manifest,
            ingestible=not _provider_fetch_scope_is_replay_only(
                owner,
                manifest,
            ),
        )
        with _ISSUED_PROVIDER_FETCH_COHORTS_LOCK:
            _ISSUED_PROVIDER_FETCH_COHORTS[identity] = authority
    return cohort


def _provider_fetch_cohort_authority(
    cohort: object,
) -> _IssuedProviderFetchCohort | None:
    if not isinstance(cohort, ProviderFetchCohort):
        return None
    try:
        fingerprint = _provider_fetch_cohort_fingerprint(cohort)
    except Exception:
        return None
    with _ISSUED_PROVIDER_FETCH_COHORTS_LOCK:
        issued = _ISSUED_PROVIDER_FETCH_COHORTS.get(id(cohort))
        if (
            issued is None
            or issued.reference() is not cohort
            or issued.fingerprint != fingerprint
            or _issued_provider_fetch_manifest(
                issued.owner,
                issued.manifest,
            )
            is None
        ):
            return None
        for _symbol, facts in cohort._entries:
            for fact in facts:
                fact_authority = _normalized_market_fact_authority(fact)
                if (
                    fact_authority is None
                    or fact_authority.owner is not issued.owner
                    or fact_authority.source.fetch_manifest
                    is not issued.manifest
                ):
                    return None
        return issued


def is_issued_provider_fetch_cohort(cohort: object) -> bool:
    return _provider_fetch_cohort_authority(cohort) is not None


def is_ingestible_provider_fetch_cohort(cohort: object) -> bool:
    """Return whether *cohort* came from a live provider fetch.

    Journal restart readers may reparse retained raw pages into issued cohorts
    so risk adapters can verify exact normalized facts again.  Those replay
    objects are deliberately not eligible as new ingestion evidence.
    """
    authority = _provider_fetch_cohort_authority(cohort)
    return authority is not None and authority.ingestible


def _mark_provider_fetch_cohort_replay_only(
    cohort: object,
) -> ProviderFetchCohort:
    """Irreversibly narrow one issued cohort to restart-read use only."""
    authority = _provider_fetch_cohort_authority(cohort)
    if authority is None or not isinstance(cohort, ProviderFetchCohort):
        raise ProviderMalformedError("provider fetch cohort authority is unverified")
    with _REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK:
        _REPLAY_ONLY_PROVIDER_FETCH_SCOPES[
            (id(authority.owner), id(authority.manifest))
        ] = (authority.owner, authority.manifest)
        with _ISSUED_PROVIDER_FETCH_COHORTS_LOCK:
            current = _ISSUED_PROVIDER_FETCH_COHORTS.get(id(cohort))
            if current is None or current.reference() is not cohort:
                raise ProviderMalformedError(
                    "provider fetch cohort authority changed during narrowing"
                )
            for identity, candidate in tuple(
                _ISSUED_PROVIDER_FETCH_COHORTS.items()
            ):
                if (
                    candidate.owner is authority.owner
                    and candidate.manifest is authority.manifest
                    and candidate.ingestible
                ):
                    _ISSUED_PROVIDER_FETCH_COHORTS[identity] = (
                        _IssuedProviderFetchCohort(
                            reference=candidate.reference,
                            fingerprint=candidate.fingerprint,
                            owner=candidate.owner,
                            manifest=candidate.manifest,
                            ingestible=False,
                        )
                    )
    return cohort


def provider_fetch_cohorts_share_owner(*cohorts: object) -> bool:
    """Return whether every current issued cohort/option chain has one owner."""
    if not cohorts:
        return False
    authorities = tuple(
        _provider_fetch_cohort_authority(cohort)
        or _provider_option_chain_authority(cohort)
        for cohort in cohorts
    )
    if any(authority is None for authority in authorities):
        return False
    owner = authorities[0].owner
    return all(authority.owner is owner for authority in authorities[1:])


def _provider_fetch_cohort_manifest(
    cohort: object,
) -> ProviderFetchManifest:
    issued = _provider_fetch_cohort_authority(cohort)
    if issued is None:
        raise ValueError("provider fetch cohort authority is unverified")
    return issued.manifest


def read_provider_fetch_bundle(value: object) -> ProviderFetchBundle:
    """Return immutable raw evidence only for a current issued fact/cohort."""
    fact_authority = _normalized_market_fact_authority(value)
    cohort_authority = _provider_fetch_cohort_authority(value)
    option_chain_authority = _provider_option_chain_authority(value)
    if fact_authority is not None:
        owner = fact_authority.owner
        manifest = fact_authority.source.fetch_manifest
    elif cohort_authority is not None:
        owner = cohort_authority.owner
        manifest = cohort_authority.manifest
    elif option_chain_authority is not None:
        owner = option_chain_authority.owner
        manifest = option_chain_authority.manifest
    else:
        raise ValueError("provider fetch authority is unverified")
    issued_fetch = _issued_provider_fetch_manifest(owner, manifest)
    if issued_fetch is None:
        raise ValueError("provider fetch authority is unverified")
    return ProviderFetchBundle(
        manifest=manifest,
        pages=issued_fetch.page_bundles,
    )


@dataclass(frozen=True, slots=True, weakref_slot=True)
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


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ProviderOptionChain(Sequence[OptionSnapshot]):
    """Exact complete option-snapshot sequence for one terminal fetch."""

    _snapshots: tuple[OptionSnapshot, ...]

    def __post_init__(self) -> None:
        snapshots = tuple(self._snapshots)
        object.__setattr__(self, "_snapshots", snapshots)
        if (
            not snapshots
            or any(not isinstance(item, OptionSnapshot) for item in snapshots)
            or snapshots
            != tuple(sorted(snapshots, key=lambda item: item.occ_symbol))
            or len({item.occ_symbol for item in snapshots}) != len(snapshots)
            or len({item.underlying for item in snapshots}) != 1
        ):
            raise ProviderMalformedError("provider option chain is malformed")

    def __getitem__(self, index: int | slice):
        return self._snapshots[index]

    def __iter__(self) -> Iterator[OptionSnapshot]:
        return iter(self._snapshots)

    def __len__(self) -> int:
        return len(self._snapshots)


@dataclass(frozen=True, slots=True)
class _IssuedProviderOptionChain:
    reference: ReferenceType[ProviderOptionChain]
    fingerprint: str
    owner: object
    manifest: ProviderFetchManifest
    ingestible: bool


_ISSUED_PROVIDER_OPTION_CHAINS: dict[int, _IssuedProviderOptionChain] = {}
_ISSUED_PROVIDER_OPTION_CHAINS_LOCK = RLock()


def _provider_option_chain_fingerprint(chain: ProviderOptionChain) -> str:
    return _canonical_digest(
        "stock-monitor/alpaca-provider-option-chain/v1",
        [_market_fact_payload(snapshot) for snapshot in chain],
    )


def _issue_provider_option_chain(
    *,
    owner: object,
    manifest: ProviderFetchManifest,
    snapshots: Sequence[OptionSnapshot],
) -> ProviderOptionChain:
    issued_fetch = _issued_provider_fetch_manifest(owner, manifest)
    if issued_fetch is None or manifest.collection != "snapshots":
        raise ProviderMalformedError("provider option chain authority is unverified")
    values = tuple(sorted(snapshots, key=lambda item: item.occ_symbol))
    actual_by_coordinate: dict[tuple[int, int, str], OptionSnapshot] = {}
    for snapshot in values:
        authority = _normalized_market_fact_authority(snapshot)
        if (
            authority is None
            or authority.owner is not owner
            or authority.source.fetch_manifest is not manifest
            or authority.source.kind != "OPTION_SNAPSHOT"
        ):
            raise ProviderMalformedError(
                "provider option chain fact authority is unverified"
            )
        coordinate = (
            authority.source.page_ordinal,
            authority.source.source_item_ordinal,
            authority.source.source_item_path,
        )
        if coordinate in actual_by_coordinate:
            raise ProviderMalformedError(
                "provider option chain duplicates a raw provider item"
            )
        actual_by_coordinate[coordinate] = snapshot
    expected_by_coordinate: dict[tuple[int, int, str], OptionSnapshot] = {}
    for page, payload, observation in zip(
        manifest.pages,
        issued_fetch.raw_pages,
        issued_fetch.observations,
        strict=True,
    ):
        for item_ordinal, item_path, _symbol, _value in _raw_provider_items(
            manifest=manifest,
            page=page,
            payload=payload,
        ):
            coordinate = (page.page_ordinal, item_ordinal, item_path)
            expected = _market_fact_from_raw_provider_item(
                manifest=manifest,
                page=page,
                observation=observation,
                payload=payload,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(expected, OptionSnapshot):
                raise ProviderMalformedError(
                    "provider option chain raw item is malformed"
                )
            expected_by_coordinate[coordinate] = expected
    if (
        actual_by_coordinate.keys() != expected_by_coordinate.keys()
        or any(
            actual_by_coordinate[coordinate] != expected
            for coordinate, expected in expected_by_coordinate.items()
        )
    ):
        raise ProviderMalformedError(
            "provider option chain is not the complete raw snapshot cohort"
        )
    chain = ProviderOptionChain(values)
    identity = id(chain)

    def discard(dead: ReferenceType[ProviderOptionChain]) -> None:
        with _ISSUED_PROVIDER_OPTION_CHAINS_LOCK:
            current = _ISSUED_PROVIDER_OPTION_CHAINS.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_PROVIDER_OPTION_CHAINS.pop(identity, None)

    with _REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK:
        authority = _IssuedProviderOptionChain(
            reference=ref(chain, discard),
            fingerprint=_provider_option_chain_fingerprint(chain),
            owner=owner,
            manifest=manifest,
            ingestible=not _provider_fetch_scope_is_replay_only(
                owner,
                manifest,
            ),
        )
        with _ISSUED_PROVIDER_OPTION_CHAINS_LOCK:
            _ISSUED_PROVIDER_OPTION_CHAINS[identity] = authority
    return chain


def _provider_option_chain_authority(
    chain: object,
) -> _IssuedProviderOptionChain | None:
    if not isinstance(chain, ProviderOptionChain):
        return None
    try:
        fingerprint = _provider_option_chain_fingerprint(chain)
    except Exception:
        return None
    with _ISSUED_PROVIDER_OPTION_CHAINS_LOCK:
        issued = _ISSUED_PROVIDER_OPTION_CHAINS.get(id(chain))
        if (
            issued is None
            or issued.reference() is not chain
            or issued.fingerprint != fingerprint
            or _issued_provider_fetch_manifest(issued.owner, issued.manifest)
            is None
        ):
            return None
        for snapshot in chain:
            fact_authority = _normalized_market_fact_authority(snapshot)
            if (
                fact_authority is None
                or fact_authority.owner is not issued.owner
                or fact_authority.source.fetch_manifest is not issued.manifest
            ):
                return None
        return issued


def is_issued_provider_option_chain(chain: object) -> bool:
    return _provider_option_chain_authority(chain) is not None


def is_ingestible_provider_option_chain(chain: object) -> bool:
    """Return whether *chain* retains live-ingest authority."""
    authority = _provider_option_chain_authority(chain)
    return authority is not None and authority.ingestible


def _mark_provider_option_chain_replay_only(
    chain: object,
) -> ProviderOptionChain:
    """Irreversibly narrow one exact option-fetch scope to replay use."""
    authority = _provider_option_chain_authority(chain)
    if authority is None or not isinstance(chain, ProviderOptionChain):
        raise ProviderMalformedError("provider option chain authority is unverified")
    with _REPLAY_ONLY_PROVIDER_FETCH_SCOPES_LOCK:
        _REPLAY_ONLY_PROVIDER_FETCH_SCOPES[
            (id(authority.owner), id(authority.manifest))
        ] = (authority.owner, authority.manifest)
        with _ISSUED_PROVIDER_OPTION_CHAINS_LOCK:
            current = _ISSUED_PROVIDER_OPTION_CHAINS.get(id(chain))
            if current is None or current.reference() is not chain:
                raise ProviderMalformedError(
                    "provider option chain authority changed during narrowing"
                )
            for identity, candidate in tuple(
                _ISSUED_PROVIDER_OPTION_CHAINS.items()
            ):
                if (
                    candidate.owner is authority.owner
                    and candidate.manifest is authority.manifest
                    and candidate.ingestible
                ):
                    _ISSUED_PROVIDER_OPTION_CHAINS[identity] = (
                        _IssuedProviderOptionChain(
                            reference=candidate.reference,
                            fingerprint=candidate.fingerprint,
                            owner=candidate.owner,
                            manifest=candidate.manifest,
                            ingestible=False,
                        )
                    )
    return chain


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


def _source_observation_id_for_payload(
    *,
    source_type: str,
    url: str,
    retrieved_at: datetime,
    payload: bytes,
) -> str:
    identity = hashlib.sha256(
        source_type.encode("ascii")
        + b"\0"
        + url.encode("ascii")
        + b"\0"
        + retrieved_at.isoformat().encode("ascii")
        + b"\0"
        + payload
    ).hexdigest()
    return f"obs-{identity[:24]}"


def recompute_alpaca_page_metadata(
    *,
    payload: bytes,
    request_url: str,
    source_type: str,
    retrieved_at: datetime,
) -> AlpacaPageMetadata:
    """Purely recompute raw-page identity and chronology for persistence."""
    if not isinstance(payload, bytes) or not payload:
        raise ProviderMalformedError("Alpaca page payload is malformed")
    contract = _PAGE_SOURCE_CONTRACTS.get(source_type)
    if contract is None:
        raise ProviderMalformedError("Alpaca page source type is unsupported")
    collection, expected_path, latest_shape = contract
    try:
        parsed_url = urllib.parse.urlsplit(request_url)
    except (TypeError, ValueError):
        raise ProviderMalformedError("Alpaca page request URL is malformed") from None
    option_underlying: str | None = None
    path_matches = parsed_url.path == expected_path
    if source_type == "ALPACA_OPTION_SNAPSHOTS":
        encoded_underlying = parsed_url.path.removeprefix(expected_path)
        option_underlying = urllib.parse.unquote(encoded_underlying)
        path_matches = (
            parsed_url.path.startswith(expected_path)
            and _SYMBOL.fullmatch(option_underlying) is not None
            and parsed_url.path
            == expected_path + urllib.parse.quote(option_underlying)
        )
    if (
        f"{parsed_url.scheme}://{parsed_url.netloc}" != _BASE_URL
        or not path_matches
        or parsed_url.fragment
    ):
        raise ProviderMalformedError("Alpaca page request URL is inconsistent")
    document = _json_object(payload)
    raw_collection = document.get(collection)
    if not isinstance(raw_collection, dict):
        raise ProviderMalformedError("Alpaca page collection is malformed")
    safe_retrieved_at = _utc(retrieved_at, "retrieved time")
    timestamps: list[datetime] = []
    for symbol in sorted(raw_collection):
        raw_values = raw_collection[symbol]
        if source_type == "ALPACA_OPTION_SNAPSHOTS":
            match = _OCC_SYMBOL.fullmatch(symbol) if isinstance(symbol, str) else None
            if (
                match is None
                or match.group("root") != option_underlying
                or not isinstance(raw_values, dict)
            ):
                raise ProviderMalformedError("Alpaca option snapshot item is malformed")
            quote = raw_values.get("latestQuote")
            if quote is not None:
                if not isinstance(quote, dict):
                    raise ProviderMalformedError(
                        "Alpaca option snapshot quote is malformed"
                    )
                timestamps.append(
                    _timestamp(quote.get("t"), "option quote")
                )
            continue
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
            raise ProviderMalformedError("Alpaca page symbol is malformed")
        if latest_shape:
            values = (raw_values,)
        else:
            if not isinstance(raw_values, list):
                raise ProviderMalformedError("Alpaca page collection is malformed")
            values = tuple(raw_values)
        for value in values:
            if not isinstance(value, dict):
                raise ProviderMalformedError("Alpaca page item is malformed")
            timestamps.append(_timestamp(value.get("t"), collection[:-1]))
    if timestamps:
        source_time = max(timestamps)
    elif source_type == "ALPACA_OPTION_SNAPSHOTS":
        source_time = safe_retrieved_at
    else:
        if latest_shape:
            raise ProviderIncompleteError("latest Alpaca page is empty")
        end_values = urllib.parse.parse_qs(
            parsed_url.query,
            keep_blank_values=True,
            strict_parsing=True,
        ).get("end", [])
        if len(end_values) != 1:
            raise ProviderMalformedError(
                "empty Alpaca page request has no exact end boundary"
            )
        source_time = _timestamp(end_values[0], "request end")
    if source_time > safe_retrieved_at:
        raise ProviderMalformedError("provider source timestamp is in the future")
    return AlpacaPageMetadata(
        source_observation_id=_source_observation_id_for_payload(
            source_type=source_type,
            url=request_url,
            retrieved_at=safe_retrieved_at,
            payload=payload,
        ),
        source_time=source_time,
        retrieved_at=safe_retrieved_at,
        delay_seconds=int((safe_retrieved_at - source_time).total_seconds()),
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _register_provider_fetch_manifest(
    owner: object,
    manifest: ProviderFetchManifest,
    pages: tuple[tuple[str, dict[str, object], bytes, str, str], ...],
    page_bundles: tuple[ProviderFetchPageBundle, ...],
) -> None:
    if not isinstance(owner, AlpacaMarketData):
        raise ProviderMalformedError("provider fetch authority owner is unverified")
    if (
        len(pages) != len(manifest.pages)
        or len(page_bundles) != len(manifest.pages)
    ):
        raise ProviderMalformedError("provider fetch authority pages are incomplete")
    raw_pages: list[bytes] = []
    observations: list[SourceObservation] = []
    for manifest_page, page_bundle, (
        url,
        document,
        payload,
        observation_id,
        source_type,
    ) in zip(manifest.pages, page_bundles, pages, strict=True):
        observation = owner._observations.get(observation_id)
        try:
            reparsed = _json_object(payload)
        except ProviderMalformedError:
            raise ProviderMalformedError(
                "provider fetch authority payload is malformed"
            ) from None
        if (
            manifest_page.source_observation_id != observation_id
            or manifest_page.source_type != source_type
            or manifest_page.request_url != url
            or manifest_page.payload_sha256
            != hashlib.sha256(payload).hexdigest()
            or observation is None
            or observation.url != url
            or observation.source_type != source_type
            or _source_observation_id_for_payload(
                source_type=source_type,
                url=url,
                retrieved_at=observation.retrieved_at,
                payload=payload,
            )
            != observation_id
            or reparsed != document
            or page_bundle.page is not manifest_page
            or page_bundle.payload is not payload
            or page_bundle.observation is not observation
            or (
                authority := _provider_fetch_page_bundle_authority(
                    page_bundle
                )
            )
            is None
            or authority.owner is not owner
        ):
            raise ProviderMalformedError(
                "provider fetch authority does not match pinned raw pages"
            )
        raw_pages.append(payload)
        observations.append(observation)
    manifest_identity = id(manifest)
    manifest_registry = owner._issued_fetch_manifests
    authority_lock = owner._provider_authority_lock

    def discard_manifest(dead: ReferenceType[ProviderFetchManifest]) -> None:
        with authority_lock:
            current = manifest_registry.get(manifest_identity)
            if current is not None and current.reference is dead:
                manifest_registry.pop(manifest_identity, None)

    entry = _IssuedProviderFetchManifest(
        reference=ref(manifest, discard_manifest),
        fingerprint=_provider_fetch_manifest_fingerprint(manifest),
        raw_pages=tuple(raw_pages),
        observations=tuple(observations),
        observation_fingerprints=tuple(
            _source_observation_fingerprint(observation)
            for observation in observations
        ),
        page_bundles=page_bundles,
    )
    with authority_lock:
        manifest_registry[manifest_identity] = entry


def _issued_provider_fetch_manifest(
    owner: object,
    manifest: object,
) -> _IssuedProviderFetchManifest | None:
    if not isinstance(owner, AlpacaMarketData) or not isinstance(
        manifest,
        ProviderFetchManifest,
    ):
        return None
    try:
        fingerprint = _provider_fetch_manifest_fingerprint(manifest)
    except Exception:
        return None
    with owner._provider_authority_lock:
        issued = owner._issued_fetch_manifests.get(id(manifest))
        if (
            issued is None
            or issued.reference() is not manifest
            or issued.fingerprint != fingerprint
            or len(issued.raw_pages) != len(manifest.pages)
            or len(issued.observations) != len(manifest.pages)
            or len(issued.observation_fingerprints) != len(manifest.pages)
            or len(issued.page_bundles) != len(manifest.pages)
        ):
            return None
        for (
            page,
            payload,
            observation,
            observation_fingerprint,
            page_bundle,
        ) in zip(
            manifest.pages,
            issued.raw_pages,
            issued.observations,
            issued.observation_fingerprints,
            issued.page_bundles,
            strict=True,
        ):
            try:
                is_current = (
                    _source_observation_fingerprint(observation)
                    == observation_fingerprint
                )
                if not is_current or (
                    hashlib.sha256(payload).hexdigest()
                    != page.payload_sha256
                    or owner._observations.get(page.source_observation_id)
                    is not observation
                    or observation.url != page.request_url
                    or observation.source_type != page.source_type
                    or _source_observation_id_for_payload(
                        source_type=page.source_type,
                        url=page.request_url,
                        retrieved_at=observation.retrieved_at,
                        payload=payload,
                    )
                    != page.source_observation_id
                    or page_bundle.page is not page
                    or page_bundle.payload is not payload
                    or page_bundle.observation is not observation
                    or (
                        page_authority := _provider_fetch_page_bundle_authority(
                            page_bundle
                        )
                    )
                    is None
                    or page_authority.owner is not owner
                ):
                    return None
            except Exception:
                return None
        return issued


def _provider_fetch_manifest(
    *,
    owner: object,
    collection: str,
    requested_symbols: tuple[str, ...],
    pages: Sequence[
        tuple[str, dict[str, object], bytes, str, str]
    ],
    page_bundles: Sequence[ProviderFetchPageBundle] | None = None,
) -> ProviderFetchManifest:
    values = tuple(pages)
    if not values:
        raise ProviderIncompleteError("provider fetch manifest has no pages")
    supplied_page_bundles = (
        None if page_bundles is None else tuple(page_bundles)
    )
    if supplied_page_bundles is not None and len(supplied_page_bundles) != len(
        values
    ):
        raise ProviderIncompleteError("provider fetch page bundles are incomplete")
    page_sources: list[ProviderFetchPage] = []
    expected_request_token: str | None = None
    for page_ordinal, (
        url,
        document,
        payload,
        observation_id,
        source_type,
    ) in enumerate(values, start=1):
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        raw_request_tokens = query.get("page_token", [])
        request_token = (
            None if not raw_request_tokens else raw_request_tokens[0]
        )
        if (
            len(raw_request_tokens) > 1
            or request_token != expected_request_token
            or "next_page_token" not in document
        ):
            raise ProviderIncompleteError(
                "provider pagination successor chain is inconsistent"
            )
        next_token = document["next_page_token"]
        if next_token is not None and (
            type(next_token) is not str or not next_token
        ):
            raise ProviderIncompleteError("provider pagination token is malformed")
        candidate_page = ProviderFetchPage(
            page_ordinal=page_ordinal,
            source_observation_id=observation_id,
            source_type=source_type,
            request_url=url,
            request_page_token=request_token,
            next_page_token=next_token,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
        if supplied_page_bundles is None:
            page_sources.append(candidate_page)
        else:
            supplied_bundle = supplied_page_bundles[page_ordinal - 1]
            authority = _provider_fetch_page_bundle_authority(
                supplied_bundle
            )
            if (
                authority is None
                or authority.owner is not owner
                or supplied_bundle.page != candidate_page
            ):
                raise ProviderMalformedError(
                    "provider fetch page authority is unverified"
                )
            page_sources.append(supplied_bundle.page)
        expected_request_token = next_token
    if page_sources[-1].next_page_token is not None:
        raise ProviderIncompleteError(
            "provider pagination did not terminate explicitly"
        )
    first_url = urllib.parse.urlsplit(page_sources[0].request_url)
    token_free_query = tuple(
        (name, value)
        for name, value in urllib.parse.parse_qsl(
            first_url.query,
            keep_blank_values=True,
        )
        if name != "page_token"
    )
    request_digest = _canonical_digest(
        "stock-monitor/alpaca-fetch-request/v1",
        {
            "collection": collection,
            "requested_symbols": list(requested_symbols),
            "origin": f"{first_url.scheme}://{first_url.netloc}",
            "path": first_url.path,
            "query": [list(item) for item in token_free_query],
        },
    )
    manifest_payload = {
        "collection": collection,
        "requested_symbols": list(requested_symbols),
        "request_digest": request_digest,
        "pages": [
            {
                "page_ordinal": page.page_ordinal,
                "source_observation_id": page.source_observation_id,
                "source_type": page.source_type,
                "request_url": page.request_url,
                "request_page_token": page.request_page_token,
                "next_page_token": page.next_page_token,
                "payload_sha256": page.payload_sha256,
            }
            for page in page_sources
        ],
        "terminal": True,
    }
    manifest = ProviderFetchManifest(
        collection=collection,
        requested_symbols=requested_symbols,
        request_digest=request_digest,
        pages=tuple(page_sources),
        terminal=True,
        manifest_digest=_canonical_digest(
            "stock-monitor/alpaca-fetch-manifest/v1",
            manifest_payload,
        ),
    )
    if supplied_page_bundles is None:
        if not isinstance(owner, AlpacaMarketData):
            raise ProviderMalformedError(
                "provider fetch page owner is unverified"
            )
        supplied_page_bundles = tuple(
            _issue_provider_fetch_page_bundle(
                owner=owner,
                page=page,
                payload=payload,
                observation=owner._observations[observation_id],
            )
            for page, (
                _url,
                _document,
                payload,
                observation_id,
                _source_type,
            ) in zip(page_sources, values, strict=True)
        )
    _register_provider_fetch_manifest(
        owner,
        manifest,
        values,
        supplied_page_bundles,
    )
    return manifest


def _raw_provider_items(
    *,
    manifest: ProviderFetchManifest,
    page: ProviderFetchPage,
    payload: bytes,
) -> tuple[tuple[int, str, str, dict[str, object]], ...]:
    document = _json_object(payload)
    raw_collection = document.get(manifest.collection)
    if not isinstance(raw_collection, dict):
        raise ProviderMalformedError("provider fetch raw collection is malformed")
    current_ordinal = 0
    result: list[tuple[int, str, str, dict[str, object]]] = []
    latest_quote = page.source_type == "ALPACA_LATEST_QUOTES"
    option_snapshot = page.source_type == "ALPACA_OPTION_SNAPSHOTS"
    if (manifest.collection == "snapshots") != option_snapshot:
        raise ProviderMalformedError("provider fetch snapshot source is inconsistent")
    for symbol in sorted(raw_collection):
        raw_values = raw_collection[symbol]
        if option_snapshot:
            match = _OCC_SYMBOL.fullmatch(symbol) if isinstance(symbol, str) else None
            if match is None or match.group("root") not in manifest.requested_symbols:
                raise ProviderMalformedError(
                    "provider fetch contains an unrequested option snapshot"
                )
        elif not isinstance(symbol, str) or symbol not in manifest.requested_symbols:
            raise ProviderMalformedError("provider fetch contains an unrequested symbol")
        if latest_quote or option_snapshot:
            values = (raw_values,)
        else:
            if not isinstance(raw_values, list):
                raise ProviderMalformedError("provider fetch raw collection is malformed")
            values = tuple(raw_values)
        for item_index, value in enumerate(values):
            if not isinstance(value, dict):
                raise ProviderMalformedError("provider fetch raw item is malformed")
            current_ordinal += 1
            item_path = (
                f"$.{manifest.collection}.{symbol}"
                if latest_quote or option_snapshot
                else f"$.{manifest.collection}.{symbol}[{item_index}]"
            )
            result.append((current_ordinal, item_path, symbol, value))
    return tuple(result)


def _raw_provider_item(
    *,
    manifest: ProviderFetchManifest,
    page: ProviderFetchPage,
    payload: bytes,
    source_item_ordinal: int,
    source_item_path: str,
) -> tuple[str, dict[str, object]]:
    for item_ordinal, item_path, symbol, value in _raw_provider_items(
        manifest=manifest,
        page=page,
        payload=payload,
    ):
        if (
            item_ordinal == source_item_ordinal
            and item_path == source_item_path
        ):
            return symbol, value
    raise ProviderMalformedError(
        "normalized market fact does not match an exact raw provider item"
    )


def _option_snapshot_from_raw_item(
    *,
    symbol: str,
    value: dict[str, object],
    observation: SourceObservation,
    requested_symbols: tuple[str, ...],
) -> OptionSnapshot:
    match = _OCC_SYMBOL.fullmatch(symbol)
    if match is None or match.group("root") not in requested_symbols:
        raise ProviderMalformedError("indicative option snapshot is malformed")
    quote = value.get("latestQuote")
    bid: Decimal | None = None
    ask: Decimal | None = None
    observed_at: datetime | None = None
    if quote is not None:
        if not isinstance(quote, dict):
            raise ProviderMalformedError("indicative option quote is malformed")
        bid = _decimal(quote.get("bp"), "option bid", positive=False)
        ask = _decimal(quote.get("ap"), "option ask", positive=False)
        if bid < 0 or ask < 0:
            raise ProviderMalformedError("indicative option quote is negative")
        if ask < bid:
            raise ProviderMalformedError("indicative option quote is crossed")
        observed_at = _timestamp(quote.get("t"), "option quote")
        if observed_at > observation.retrieved_at:
            raise ProviderMalformedError(
                "provider option timestamp is in the future"
            )
    greeks = value.get("greeks")
    delta: Decimal | None = None
    if greeks is not None:
        if not isinstance(greeks, dict):
            raise ProviderMalformedError("indicative option greeks are malformed")
        delta = _decimal(
            greeks.get("delta"),
            "option delta",
            positive=False,
        )
        if not Decimal("-1") <= delta <= Decimal("1"):
            raise ProviderMalformedError(
                "indicative option delta is outside its bounds"
            )
    daily_bar = value.get("dailyBar")
    volume: int | None = None
    if daily_bar is not None:
        if not isinstance(daily_bar, dict):
            raise ProviderMalformedError(
                "indicative option daily bar is malformed"
            )
        volume = _integer(
            daily_bar.get("v"),
            "option daily volume",
            optional=True,
        )
    try:
        expiration = datetime.strptime(match.group("date"), "%y%m%d").date()
    except ValueError:
        raise ProviderMalformedError(
            "indicative option expiration is malformed"
        ) from None
    return OptionSnapshot(
        occ_symbol=symbol,
        underlying=match.group("root"),
        expiration=expiration,
        strike=Decimal(int(match.group("strike"))) / Decimal("1000"),
        delta=delta,
        bid=bid,
        ask=ask,
        daily_volume=volume,
        open_interest=None,
        feed="indicative",
        observed_at=observed_at,
        source_observation_id=observation.observation_id,
    )


def _market_fact_from_raw_provider_item(
    *,
    manifest: ProviderFetchManifest,
    page: ProviderFetchPage,
    observation: SourceObservation,
    payload: bytes,
    source_item_ordinal: int,
    source_item_path: str,
) -> Bar | Quote | Trade | OptionSnapshot:
    symbol, value = _raw_provider_item(
        manifest=manifest,
        page=page,
        payload=payload,
        source_item_ordinal=source_item_ordinal,
        source_item_path=source_item_path,
    )
    if manifest.collection == "snapshots":
        if page.source_type != "ALPACA_OPTION_SNAPSHOTS":
            raise ProviderMalformedError(
                "provider OPTION_SNAPSHOT source type is inconsistent"
            )
        return _option_snapshot_from_raw_item(
            symbol=symbol,
            value=value,
            observation=observation,
            requested_symbols=manifest.requested_symbols,
        )
    timestamp = _timestamp(value.get("t"), manifest.collection[:-1])
    if manifest.collection == "bars":
        if page.source_type not in {
            "ALPACA_DAILY_BARS",
            "ALPACA_INTRADAY_BARS",
        }:
            raise ProviderMalformedError("provider BAR source type is inconsistent")
        open_price = _decimal(value.get("o"), "bar open")
        high = _decimal(value.get("h"), "bar high")
        low = _decimal(value.get("l"), "bar low")
        close = _decimal(value.get("c"), "bar close")
        volume = _integer(value.get("v"), "bar volume")
        assert volume is not None
        if high < max(open_price, close, low) or low > min(
            open_price,
            close,
            high,
        ):
            raise ProviderMalformedError("provider bar OHLC values are inconsistent")
        return Bar(
            symbol=symbol,
            timestamp=timestamp,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
            feed="sip",
            adjustment="split",
            source_observation_id=page.source_observation_id,
        )
    if manifest.collection == "quotes":
        if page.source_type == "ALPACA_HISTORICAL_QUOTES":
            feed = "sip"
        elif page.source_type == "ALPACA_LATEST_QUOTES":
            feed = "iex"
        else:
            raise ProviderMalformedError("provider QUOTE source type is inconsistent")
        quote = AlpacaMarketData._quote(
            symbol,
            timestamp,
            value,
            feed=feed,
            now=observation.retrieved_at,
            observation_id=page.source_observation_id,
        )
        if (
            page.source_type == "ALPACA_LATEST_QUOTES"
            and quote.age_seconds > _LATEST_MAX_AGE_SECONDS
        ):
            raise ProviderStaleError("IEX freshness quote is stale")
        return quote
    if manifest.collection == "trades":
        if page.source_type != "ALPACA_HISTORICAL_TRADES":
            raise ProviderMalformedError("provider TRADE source type is inconsistent")
        payload_feed = value.get("feed")
        if payload_feed is not None and payload_feed != "sip":
            raise ProviderMalformedError(
                "provider trade feed conflicts with requested feed"
            )
        price = _decimal(value.get("p"), "trade price")
        size = _integer(value.get("s"), "trade size")
        sequence = _integer(value.get("i"), "trade sequence")
        assert size is not None
        assert sequence is not None
        if size <= 0:
            raise ProviderMalformedError("trade size is malformed")
        return Trade(
            symbol=symbol,
            timestamp=timestamp,
            price=price,
            size=size,
            feed="sip",
            sequence=sequence,
            source_observation_id=page.source_observation_id,
        )
    raise ProviderMalformedError("normalized market fact collection is unsupported")


def _issue_market_fact_from_fetch(
    fact: Bar | Quote | Trade | OptionSnapshot | None = None,
    *,
    owner: object,
    fetch_manifest: ProviderFetchManifest,
    page_ordinal: int,
    source_item_ordinal: int,
    source_item_path: str,
) -> Bar | Quote | Trade | OptionSnapshot:
    issued_fetch = _issued_provider_fetch_manifest(owner, fetch_manifest)
    if issued_fetch is None:
        raise ProviderMalformedError("provider fetch authority is unverified")
    try:
        page_index = tuple(
            page.page_ordinal for page in fetch_manifest.pages
        ).index(page_ordinal)
    except ValueError:
        raise ProviderMalformedError("normalized market fact page is missing") from None
    matching_page = fetch_manifest.pages[page_index]
    normalized_fact = _market_fact_from_raw_provider_item(
        manifest=fetch_manifest,
        page=matching_page,
        observation=issued_fetch.observations[page_index],
        payload=issued_fetch.raw_pages[page_index],
        source_item_ordinal=source_item_ordinal,
        source_item_path=source_item_path,
    )
    if fact is not None:
        if fact != normalized_fact:
            raise ProviderMalformedError(
                "normalized market fact does not match the raw provider item"
            )
        raise ProviderMalformedError(
            "caller-supplied normalized market fact registrar is unavailable"
        )
    kind = (
        "BAR"
        if isinstance(normalized_fact, Bar)
        else "QUOTE"
        if isinstance(normalized_fact, Quote)
        else "TRADE"
        if isinstance(normalized_fact, Trade)
        else "OPTION_SNAPSHOT"
    )
    normalized_fields_digest = _canonical_digest(
        "stock-monitor/alpaca-normalized-fields/v1",
        {
            "fact": _market_fact_payload(normalized_fact),
            "page_ordinal": page_ordinal,
            "source_item_ordinal": source_item_ordinal,
            "source_item_path": source_item_path,
        },
    )
    source = NormalizedMarketFactSource(
        kind=kind,
        symbol=(
            normalized_fact.underlying
            if isinstance(normalized_fact, OptionSnapshot)
            else normalized_fact.symbol
        ),
        feed=normalized_fact.feed,
        source_observation_id=normalized_fact.source_observation_id,
        page_ordinal=page_ordinal,
        source_item_ordinal=source_item_ordinal,
        source_item_path=source_item_path,
        page_payload_sha256=matching_page.payload_sha256,
        normalized_fields_digest=normalized_fields_digest,
        fetch_manifest=fetch_manifest,
    )
    identity = id(normalized_fact)

    def discard(dead: ReferenceType[object]) -> None:
        with _ISSUED_NORMALIZED_MARKET_FACTS_LOCK:
            current = _ISSUED_NORMALIZED_MARKET_FACTS.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_NORMALIZED_MARKET_FACTS.pop(identity, None)

    authority = _IssuedNormalizedMarketFact(
        reference=ref(normalized_fact, discard),
        fingerprint=_market_fact_fingerprint(normalized_fact),
        source_fingerprint=_normalized_market_fact_source_fingerprint(source),
        owner=owner,
        source=source,
    )
    with _ISSUED_NORMALIZED_MARKET_FACTS_LOCK:
        _ISSUED_NORMALIZED_MARKET_FACTS[identity] = authority
    return normalized_fact


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
        self._provider_authority_lock = RLock()
        self._issued_fetch_manifests: dict[
            int,
            _IssuedProviderFetchManifest,
        ] = {}

    def _capture_request_dependencies(self) -> _AlpacaRequestDependencies:
        client_class = AlpacaMarketData
        request_verifier = client_class._require_current_request_dependencies
        request_dispatch = client_class._get_json_for_request
        pages_function = client_class._pages
        issue_page_function = client_class._issue_and_disclose_page
        require_pages_function = client_class._require_current_fetch_pages
        transport_get_function = self._transport.get
        cache_put_function = (
            None if self._cache is None else self._cache.put
        )

        graph_dependencies_by_root: dict[
            int,
            tuple[object, _MutableRequestGraphDependency],
        ] = {}

        def graph_dependency(
            root: object | None,
        ) -> _MutableRequestGraphDependency | None:
            if root is None:
                return None
            existing = graph_dependencies_by_root.get(id(root))
            if existing is not None and existing[0] is root:
                return existing[1]
            roots = (root,)
            dependency = _MutableRequestGraphDependency(
                roots=roots,
                snapshot=_capture_request_graph(roots),
            )
            graph_dependencies_by_root[id(root)] = (root, dependency)
            return dependency

        transport_graph_dependency = graph_dependency(self._transport)
        cache_graph_dependency = graph_dependency(self._cache)
        clock_graph_dependency = graph_dependency(self._now)
        assert transport_graph_dependency is not None
        sealed_transport = _SealedRequestTransport(
            get_function=transport_get_function,
            graph_dependency=transport_graph_dependency,
        )

        callable_dependencies: list[object] = [
            transport_get_function,
            self._now,
        ]
        if cache_put_function is not None:
            callable_dependencies.append(cache_put_function)

        dependency_root_types = [
            client_class,
            type(self._transport),
            type(self._policy),
            type(sealed_transport),
        ]
        if self._cache is not None:
            dependency_root_types.append(type(self._cache))
        for callable_dependency in callable_dependencies:
            if isinstance(callable_dependency, FunctionType):
                continue
            if isinstance(callable_dependency, MethodType):
                callable_owner = callable_dependency.__self__
                candidate = (
                    callable_owner
                    if isinstance(callable_owner, type)
                    else type(callable_owner)
                )
            elif isinstance(callable_dependency, type):
                candidate = callable_dependency
            else:
                candidate = type(callable_dependency)
            dependency_root_types.append(candidate)
        dependency_root_types = list(
            dict.fromkeys(dependency_root_types)
        )
        mro_dependencies = tuple(
            (candidate, tuple(candidate.__mro__))
            for candidate in dependency_root_types
        )
        dependency_class_values = [
            candidate
            for _root, hierarchy in mro_dependencies
            for candidate in hierarchy
            if candidate is not object
        ]
        dependency_classes = tuple(
            dict.fromkeys(dependency_class_values)
        )
        class_dependencies = tuple(
            (candidate, tuple(candidate.__dict__.items()))
            for candidate in dependency_classes
        )
        instance_dependencies = tuple(
            (namespace, tuple(namespace.items()))
            for namespace in (self.__dict__,)
        )
        dependency_functions: dict[int, FunctionType] = {}
        for _candidate, items in class_dependencies:
            for _name, value in items:
                functions: tuple[object, ...]
                if isinstance(value, (staticmethod, classmethod)):
                    functions = (value.__func__,)
                elif isinstance(value, property):
                    functions = (value.fget, value.fset, value.fdel)
                else:
                    functions = (value,)
                for function in functions:
                    if isinstance(function, FunctionType):
                        dependency_functions[id(function)] = function
        for value in (
            get_with_redirects,
            _json_object,
            request_verifier,
            request_dispatch,
            pages_function,
            issue_page_function,
            require_pages_function,
        ):
            if isinstance(value, FunctionType):
                dependency_functions[id(value)] = value
        for callable_dependency in callable_dependencies:
            function = (
                callable_dependency.__func__
                if isinstance(callable_dependency, MethodType)
                else callable_dependency
            )
            if isinstance(function, FunctionType):
                dependency_functions[id(function)] = function
        dependency_namespaces: dict[int, dict[str, object]] = {
            id(globals()): globals(),
        }
        for function in tuple(dependency_functions.values()):
            dependency_namespaces[id(function.__globals__)] = (
                function.__globals__
            )
        for namespace in tuple(dependency_namespaces.values()):
            for value in namespace.values():
                if isinstance(value, FunctionType):
                    dependency_functions[id(value)] = value
        namespace_dependencies = tuple(
            (namespace, tuple(namespace.items()))
            for namespace in dependency_namespaces.values()
        )
        function_dependencies = tuple(
            (
                function,
                function.__code__,
                function.__defaults__,
                (
                    None
                    if function.__kwdefaults__ is None
                    else tuple(function.__kwdefaults__.items())
                ),
                function.__globals__,
                function.__closure__,
            )
            for function in dependency_functions.values()
        )
        return _AlpacaRequestDependencies(
            transport=self._transport,
            credentials=self._credentials,
            credential_key_id=self._credentials.key_id,
            credential_secret_key=self._credentials.secret_key,
            base_url=self._base_url,
            policy=self._policy,
            policy_allowed_hosts=self._policy.allowed_hosts,
            now=self._now,
            cache=self._cache,
            cache_put_function=cache_put_function,
            observations=self._observations,
            provider_authority_lock=self._provider_authority_lock,
            issued_fetch_manifests=self._issued_fetch_manifests,
            namespace_dependencies=namespace_dependencies,
            class_dependencies=class_dependencies,
            mro_dependencies=mro_dependencies,
            instance_dependencies=instance_dependencies,
            graph_dependencies=tuple(
                dependency
                for _root, dependency in graph_dependencies_by_root.values()
            ),
            transport_graph_dependency=transport_graph_dependency,
            cache_graph_dependency=cache_graph_dependency,
            clock_graph_dependency=clock_graph_dependency,
            function_dependencies=function_dependencies,
            missing_dependency=object(),
            request_verifier=request_verifier,
            request_verifier_code=request_verifier.__code__,
            request_dispatch=request_dispatch,
            pages_function=pages_function,
            issue_page_function=issue_page_function,
            require_pages_function=require_pages_function,
            transport_get_function=transport_get_function,
            get_with_redirects_function=get_with_redirects,
            json_object_function=_json_object,
            sealed_transport=sealed_transport,
        )

    def _require_current_request_dependencies(
        self,
        dependencies: _AlpacaRequestDependencies,
    ) -> None:
        try:
            current_transport_get = self._transport.get
            expected_transport_get = dependencies.transport_get_function
            transport_get_is_current = (
                current_transport_get is expected_transport_get
                if not isinstance(expected_transport_get, MethodType)
                else (
                    isinstance(current_transport_get, MethodType)
                    and current_transport_get.__self__
                    is expected_transport_get.__self__
                    and current_transport_get.__func__
                    is expected_transport_get.__func__
                )
            )
            current_cache_put = (
                None if self._cache is None else self._cache.put
            )
            expected_cache_put = dependencies.cache_put_function
            cache_put_is_current = (
                current_cache_put is expected_cache_put
                if not isinstance(expected_cache_put, MethodType)
                else (
                    isinstance(current_cache_put, MethodType)
                    and current_cache_put.__self__
                    is expected_cache_put.__self__
                    and current_cache_put.__func__
                    is expected_cache_put.__func__
                )
            )
            current = (
                self._transport is dependencies.transport
                and transport_get_is_current
                and self._credentials is dependencies.credentials
                and self._credentials.key_id
                == dependencies.credential_key_id
                and self._credentials.secret_key
                == dependencies.credential_secret_key
                and self._base_url == dependencies.base_url
                and self._policy is dependencies.policy
                and self._policy.allowed_hosts
                == dependencies.policy_allowed_hosts
                and self._now is dependencies.now
                and self._cache is dependencies.cache
                and cache_put_is_current
                and self._observations is dependencies.observations
                and self._provider_authority_lock
                is dependencies.provider_authority_lock
                and self._issued_fetch_manifests
                is dependencies.issued_fetch_manifests
            )
            for candidate, expected in dependencies.mro_dependencies:
                hierarchy = candidate.__mro__
                if (
                    not current
                    or len(hierarchy) != len(expected)
                    or any(
                        current_class is not expected_class
                        for current_class, expected_class in zip(
                            hierarchy,
                            expected,
                            strict=True,
                        )
                    )
                ):
                    current = False
                    break
            for namespace, expected in dependencies.namespace_dependencies:
                if not current or len(namespace) != len(expected):
                    current = False
                    break
                for name, value in expected:
                    if (
                        namespace.get(
                            name,
                            dependencies.missing_dependency,
                        )
                        is not value
                    ):
                        current = False
                        break
            for candidate, expected in dependencies.class_dependencies:
                namespace = candidate.__dict__
                if not current or len(namespace) != len(expected):
                    current = False
                    break
                for name, value in expected:
                    if (
                        namespace.get(
                            name,
                            dependencies.missing_dependency,
                        )
                        is not value
                    ):
                        current = False
                        break
            for namespace, expected in dependencies.instance_dependencies:
                if not current or len(namespace) != len(expected):
                    current = False
                    break
                for name, value in expected:
                    if (
                        namespace.get(
                            name,
                            dependencies.missing_dependency,
                        )
                        is not value
                    ):
                        current = False
                        break
            for dependency in dependencies.graph_dependencies:
                if not current or not _request_graph_dependency_is_current(
                    dependency
                ):
                    current = False
                    break
            for (
                function,
                code,
                defaults,
                keyword_defaults,
                function_globals,
                closure,
            ) in dependencies.function_dependencies:
                if not current or (
                    function.__code__ is not code
                    or function.__defaults__ is not defaults
                    or function.__globals__ is not function_globals
                    or function.__closure__ is not closure
                ):
                    current = False
                    break
                current_keyword_defaults = function.__kwdefaults__
                if keyword_defaults is None:
                    if current_keyword_defaults is not None:
                        current = False
                        break
                elif (
                    current_keyword_defaults is None
                    or len(current_keyword_defaults)
                    != len(keyword_defaults)
                ):
                    current = False
                    break
                else:
                    for name, value in keyword_defaults:
                        if (
                            current_keyword_defaults.get(
                                name,
                                dependencies.missing_dependency,
                            )
                            is not value
                        ):
                            current = False
                            break
        except Exception:
            current = False
        if not current:
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            ) from None

    def _get_json_for_request(
        self,
        url: str,
        dependencies: _AlpacaRequestDependencies,
    ) -> tuple[dict[str, object], bytes]:
        request_verifier = dependencies.request_verifier
        if request_verifier.__code__ is not dependencies.request_verifier_code:
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, dependencies)
        response = dependencies.get_with_redirects_function(
            dependencies.sealed_transport,
            dependencies.policy,
            url,
            {
                "APCA-API-KEY-ID": dependencies.credential_key_id,
                "APCA-API-SECRET-KEY": (
                    dependencies.credential_secret_key
                ),
                "Accept": "application/json",
            },
            allowed_content_types=("application/json",),
        )
        if request_verifier.__code__ is not dependencies.request_verifier_code:
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, dependencies)
        return (
            dependencies.json_object_function(response.body),
            response.body,
        )

    def _current_time(
        self,
        request_dependencies: _AlpacaRequestDependencies | None = None,
    ) -> datetime:
        now = (
            self._now
            if request_dependencies is None
            else request_dependencies.now
        )
        if request_dependencies is not None:
            _require_current_request_graph(
                request_dependencies.clock_graph_dependency
            )
        current = now()
        if request_dependencies is not None:
            _refresh_trusted_request_graph(
                request_dependencies.clock_graph_dependency
            )
        return _utc(current, "current time")

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
        request_dependencies: _AlpacaRequestDependencies | None = None,
    ) -> str:
        retrieved_at = self._current_time(request_dependencies)
        safe_timestamp = _utc(source_timestamp, "source timestamp")
        if safe_timestamp > retrieved_at:
            raise ProviderMalformedError("provider source timestamp is in the future")
        if source_type in _PAGE_SOURCE_CONTRACTS:
            metadata = recompute_alpaca_page_metadata(
                payload=payload,
                request_url=url,
                source_type=source_type,
                retrieved_at=retrieved_at,
            )
            if (
                source_type != "ALPACA_OPTION_SNAPSHOTS"
                and metadata.source_time != safe_timestamp
            ):
                raise ProviderMalformedError(
                    "provider source timestamp conflicts with its raw page"
                )
            safe_timestamp = metadata.source_time
            observation_id = metadata.source_observation_id
            delay_seconds = metadata.delay_seconds
        else:
            observation_id = _source_observation_id_for_payload(
                source_type=source_type,
                url=url,
                retrieved_at=retrieved_at,
                payload=payload,
            )
            delay_seconds = max(
                0,
                int((retrieved_at - safe_timestamp).total_seconds()),
            )
        observation = SourceObservation(
            observation_id=observation_id,
            url=url,
            source_type=source_type,
            source_timestamp=safe_timestamp,
            retrieved_at=retrieved_at,
            feed=feed,
            delay_seconds=delay_seconds,
        )
        self._observations[observation_id] = observation
        cache_put_function = (
            None
            if request_dependencies is None
            else request_dependencies.cache_put_function
        )
        if request_dependencies is None and self._cache is not None:
            cache_put_function = self._cache.put
        if cache_put_function is not None:
            if request_dependencies is not None:
                _require_current_request_graph(
                    request_dependencies.cache_graph_dependency
                )
            cache_put_function(observation, payload)
            if request_dependencies is not None:
                _refresh_trusted_request_graph(
                    request_dependencies.cache_graph_dependency
                )
        return observation_id

    def health_attestation(self, observation_id: str) -> SourceHealthAttestation:
        """Issue cache authority only for a successful market-data observation."""
        if not isinstance(observation_id, str) or not observation_id:
            raise ValueError("market-data observation ID is malformed")
        observation = self._observations.get(observation_id)
        if observation is None:
            raise ValueError("market-data observation was not issued by this client")
        return _issue_provider_health_attestation(observation)

    def _validate_historical_window(
        self,
        window: TimeWindow,
        request_dependencies: _AlpacaRequestDependencies | None = None,
    ) -> None:
        if not isinstance(window, TimeWindow):
            raise TypeError("historical window has the wrong type")
        if (
            self._current_time(request_dependencies) - window.end
            < HISTORICAL_SIP_RELEASE_DELAY
        ):
            raise ProviderMalformedError(
                "historical SIP window must end at least sixteen minutes ago"
            )

    def _pages(
        self,
        *,
        path: str,
        query: list[tuple[str, str]],
        collection: str,
        request_dependencies: _AlpacaRequestDependencies,
    ) -> Iterator[tuple[str, dict[str, object], bytes]]:
        request_verifier = request_dependencies.request_verifier
        request_dispatch = request_dependencies.request_dispatch
        token: str | None = None
        seen_tokens: set[str] = set()
        for _ in range(_MAX_PAGES):
            if (
                request_verifier.__code__
                is not request_dependencies.request_verifier_code
            ):
                raise ProviderMalformedError(
                    "provider request dependencies changed during pagination"
                )
            request_verifier(self, request_dependencies)
            current_query = list(query)
            if token is not None:
                current_query.append(("page_token", token))
            url = (
                f"{request_dependencies.base_url}{path}?"
                f"{urllib.parse.urlencode(current_query)}"
            )
            document, payload = request_dispatch(
                self,
                url,
                request_dependencies,
            )
            if collection not in document or not isinstance(document[collection], dict):
                raise ProviderIncompleteError(
                    f"provider response is missing the {collection} collection"
                )
            if "next_page_token" not in document:
                raise ProviderIncompleteError("provider pagination did not terminate explicitly")
            next_token = document["next_page_token"]
            if next_token is not None and (
                not isinstance(next_token, str)
                or not next_token
                or len(next_token) > 512
                or not next_token.isascii()
                or not next_token.isprintable()
                or next_token in seen_tokens
            ):
                raise ProviderIncompleteError("provider pagination token is malformed or repeated")
            yield url, document, payload
            if next_token is None:
                return
            seen_tokens.add(next_token)
            token = next_token
        raise ProviderIncompleteError("provider pagination exceeded the page limit")

    @staticmethod
    def _validate_page_sink(
        page_sink: Callable[[ProviderFetchPageBundle], None] | None,
    ) -> Callable[[ProviderFetchPageBundle], None] | None:
        if page_sink is not None and not callable(page_sink):
            raise TypeError("provider page sink must be callable")
        return page_sink

    def _issue_and_disclose_page(
        self,
        *,
        page_ordinal: int,
        url: str,
        document: dict[str, object],
        payload: bytes,
        observation_id: str,
        source_type: str,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None,
        request_dependencies: _AlpacaRequestDependencies,
    ) -> ProviderFetchPageBundle:
        request_verifier = request_dependencies.request_verifier
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        raw_request_tokens = query.get("page_token", [])
        if len(raw_request_tokens) > 1:
            raise ProviderIncompleteError(
                "provider pagination successor chain is inconsistent"
            )
        request_token = (
            None if not raw_request_tokens else raw_request_tokens[0]
        )
        next_token = document.get("next_page_token")
        page = ProviderFetchPage(
            page_ordinal=page_ordinal,
            source_observation_id=observation_id,
            source_type=source_type,
            request_url=url,
            request_page_token=request_token,
            next_page_token=next_token,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
        observation = self._observations.get(observation_id)
        if observation is None:
            raise ProviderMalformedError(
                "provider fetch page observation is unavailable"
            )
        bundle = _issue_provider_fetch_page_bundle(
            owner=self,
            page=page,
            payload=payload,
            observation=observation,
        )
        if page_sink is not None:
            sink_failed = False
            try:
                page_sink(bundle)
            except Exception:
                sink_failed = True
            if sink_failed:
                raise ProviderMalformedError("provider page sink failed")
            if (
                request_verifier.__code__
                is not request_dependencies.request_verifier_code
            ):
                raise ProviderMalformedError(
                    "provider request dependencies changed during pagination"
                )
            request_verifier(self, request_dependencies)
            authority = _provider_fetch_page_bundle_authority(bundle)
            if authority is None or authority.owner is not self:
                raise ProviderMalformedError(
                    "provider fetch page authority changed during disclosure"
                )
        return bundle

    def _require_current_fetch_pages(
        self,
        pages: Sequence[ProviderFetchPageBundle],
        request_dependencies: _AlpacaRequestDependencies,
    ) -> None:
        request_verifier = request_dependencies.request_verifier
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        if any(
            (authority := _provider_fetch_page_bundle_authority(page))
            is None
            or authority.owner is not self
            for page in pages
        ):
            raise ProviderMalformedError(
                "provider fetch page authority changed during disclosure"
            )

    def daily_bars(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> Mapping[str, tuple[Bar, ...]]:
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        pages_function = request_dependencies.pages_function
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        requested = _symbols(symbols)
        self._validate_historical_window(window, request_dependencies)
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
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
        raw_pages = pages_function(
            self,
            path="/v2/stocks/bars",
            query=query,
            collection="bars",
            request_dependencies=request_dependencies,
        )
        pinned_pages: list[
            tuple[str, dict[str, object], bytes, str, str]
        ] = []
        issued_pages: list[ProviderFetchPageBundle] = []
        pending_sources: list[tuple[int, int, str]] = []
        for page_ordinal, (url, document, payload) in enumerate(
            raw_pages,
            start=1,
        ):
            raw_collection = document["bars"]
            assert isinstance(raw_collection, dict)
            unknown = set(raw_collection) - set(requested)
            if unknown:
                raise ProviderMalformedError("provider bars contain an unrequested symbol")
            parsed_page: list[
                tuple[str, datetime, dict[str, object], int, str]
            ] = []
            source_item_ordinal = 0
            for symbol in sorted(raw_collection):
                values = raw_collection[symbol]
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderMalformedError("provider bars collection is malformed")
                for index, value in enumerate(values):
                    if not isinstance(value, dict):
                        raise ProviderMalformedError("provider bar is malformed")
                    timestamp = _timestamp(value.get("t"), "bar")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderMalformedError(
                            "provider bar lies outside the requested window"
                        )
                    source_item_ordinal += 1
                    parsed_page.append(
                        (
                            symbol,
                            timestamp,
                            value,
                            source_item_ordinal,
                            f"$.bars.{symbol}[{index}]",
                        )
                    )
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
                request_dependencies=request_dependencies,
            )
            pinned_pages.append(
                (url, document, payload, observation_id, "ALPACA_DAILY_BARS")
            )
            for (
                symbol,
                timestamp,
                value,
                item_ordinal,
                item_path,
            ) in parsed_page:
                open_price = _decimal(value.get("o"), "bar open")
                high = _decimal(value.get("h"), "bar high")
                low = _decimal(value.get("l"), "bar low")
                close = _decimal(value.get("c"), "bar close")
                volume = _integer(value.get("v"), "bar volume")
                assert volume is not None
                if high < max(open_price, close, low) or low > min(open_price, close, high):
                    raise ProviderMalformedError("provider bar OHLC values are inconsistent")
                bar = Bar(
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
                result[symbol].append(bar)
                pending_sources.append(
                    (page_ordinal, item_ordinal, item_path)
                )
            if any(
                len({bar.timestamp for bar in bars}) != len(bars)
                for bars in result.values()
            ):
                raise ProviderIncompleteError(
                    "provider bars are missing or contain duplicate timestamps"
                )
            issued_pages.append(
                issue_page_function(
                    self,
                    page_ordinal=page_ordinal,
                    url=url,
                    document=document,
                    payload=payload,
                    observation_id=observation_id,
                    source_type="ALPACA_DAILY_BARS",
                    page_sink=page_sink,
                    request_dependencies=request_dependencies,
                )
            )
            require_pages_function(
                self,
                issued_pages,
                request_dependencies,
            )
        self._complete_bars(result, expected_terminal=window.end)
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="bars",
            requested_symbols=requested,
            pages=pinned_pages,
            page_bundles=issued_pages,
        )
        issued_result: dict[str, list[Bar]] = {
            symbol: [] for symbol in requested
        }
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, Bar):
                raise ProviderMalformedError("provider BAR authority is malformed")
            issued_result[fact.symbol].append(fact)
        issued_values = self._complete_bars(
            issued_result,
            expected_terminal=window.end,
        )
        return _issue_provider_fetch_cohort(
            owner=self,
            manifest=fetch_manifest,
            values=issued_values,
        )

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

    def historical_minute_bars(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> Mapping[str, tuple[Bar, ...]]:
        """Return a terminal split-adjusted SIP one-minute bar cohort."""
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        pages_function = request_dependencies.pages_function
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        requested = _symbols(symbols)
        self._validate_historical_window(window, request_dependencies)
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        query = [
            ("symbols", ",".join(requested)),
            ("timeframe", "1Min"),
            ("start", _format_utc(window.start)),
            ("end", _format_utc(window.end)),
            ("adjustment", "split"),
            ("feed", "sip"),
            ("limit", "10000"),
        ]
        result: dict[str, list[Bar]] = {symbol: [] for symbol in requested}
        raw_pages = pages_function(
            self,
            path="/v2/stocks/bars",
            query=query,
            collection="bars",
            request_dependencies=request_dependencies,
        )
        pinned_pages: list[
            tuple[str, dict[str, object], bytes, str, str]
        ] = []
        issued_pages: list[ProviderFetchPageBundle] = []
        pending_sources: list[tuple[int, int, str]] = []
        for page_ordinal, (url, document, payload) in enumerate(
            raw_pages,
            start=1,
        ):
            raw_collection = document["bars"]
            assert isinstance(raw_collection, dict)
            if set(raw_collection) - set(requested):
                raise ProviderMalformedError(
                    "provider bars contain an unrequested symbol"
                )
            parsed_page: list[
                tuple[str, datetime, dict[str, object], int, str]
            ] = []
            source_item_ordinal = 0
            for symbol in sorted(raw_collection):
                values = raw_collection[symbol]
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderMalformedError(
                        "provider bars collection is malformed"
                    )
                for index, value in enumerate(values):
                    if not isinstance(value, dict):
                        raise ProviderMalformedError("provider bar is malformed")
                    timestamp = _timestamp(value.get("t"), "bar")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderMalformedError(
                            "provider bar lies outside the requested window"
                        )
                    source_item_ordinal += 1
                    parsed_page.append(
                        (
                            symbol,
                            timestamp,
                            value,
                            source_item_ordinal,
                            f"$.bars.{symbol}[{index}]",
                        )
                    )
            source_timestamp = max(
                (item[1] for item in parsed_page),
                default=window.end,
            )
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_INTRADAY_BARS",
                feed="sip",
                source_timestamp=source_timestamp,
                request_dependencies=request_dependencies,
            )
            pinned_pages.append(
                (
                    url,
                    document,
                    payload,
                    observation_id,
                    "ALPACA_INTRADAY_BARS",
                )
            )
            for symbol, _timestamp_value, value, item_ordinal, item_path in (
                parsed_page
            ):
                open_price = _decimal(value.get("o"), "bar open")
                high = _decimal(value.get("h"), "bar high")
                low = _decimal(value.get("l"), "bar low")
                close = _decimal(value.get("c"), "bar close")
                volume = _integer(value.get("v"), "bar volume")
                assert volume is not None
                if high < max(open_price, close, low) or low > min(
                    open_price,
                    close,
                    high,
                ):
                    raise ProviderMalformedError(
                        "provider bar OHLC values are inconsistent"
                    )
                result[symbol].append(
                    Bar(
                        symbol=symbol,
                        timestamp=_timestamp_value,
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
                pending_sources.append(
                    (page_ordinal, item_ordinal, item_path)
                )
            if any(
                len({bar.timestamp for bar in bars}) != len(bars)
                for bars in result.values()
            ):
                raise ProviderIncompleteError(
                    "provider bars are missing or contain duplicate timestamps"
                )
            issued_pages.append(
                issue_page_function(
                    self,
                    page_ordinal=page_ordinal,
                    url=url,
                    document=document,
                    payload=payload,
                    observation_id=observation_id,
                    source_type="ALPACA_INTRADAY_BARS",
                    page_sink=page_sink,
                    request_dependencies=request_dependencies,
                )
            )
            require_pages_function(
                self,
                issued_pages,
                request_dependencies,
            )
        completed = self._complete_bars(
            result,
            expected_terminal=window.end,
        )
        if any(
            values[-1].timestamp < window.end - timedelta(minutes=5)
            for values in completed.values()
        ):
            raise ProviderIncompleteError(
                "provider minute bars do not reach the terminal boundary"
            )
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="bars",
            requested_symbols=requested,
            pages=pinned_pages,
            page_bundles=issued_pages,
        )
        issued_result: dict[str, list[Bar]] = {
            symbol: [] for symbol in requested
        }
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, Bar):
                raise ProviderMalformedError("provider BAR authority is malformed")
            issued_result[fact.symbol].append(fact)
        issued_values = self._complete_bars(
            issued_result,
            expected_terminal=window.end,
        )
        return _issue_provider_fetch_cohort(
            owner=self,
            manifest=fetch_manifest,
            values=issued_values,
        )

    def historical_trades(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> Mapping[str, tuple[Trade, ...]]:
        """Return one complete terminal SIP trade fetch with item authority."""
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        pages_function = request_dependencies.pages_function
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        requested = _symbols(symbols)
        self._validate_historical_window(window, request_dependencies)
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        query = [
            ("symbols", ",".join(requested)),
            ("start", _format_utc(window.start)),
            ("end", _format_utc(window.end)),
            ("feed", "sip"),
            ("limit", "10000"),
        ]
        result: dict[str, list[Trade]] = {symbol: [] for symbol in requested}
        raw_pages = pages_function(
            self,
            path="/v2/stocks/trades",
            query=query,
            collection="trades",
            request_dependencies=request_dependencies,
        )
        pinned_pages: list[
            tuple[str, dict[str, object], bytes, str, str]
        ] = []
        issued_pages: list[ProviderFetchPageBundle] = []
        pending_sources: list[tuple[int, int, str]] = []
        for page_ordinal, (url, document, payload) in enumerate(
            raw_pages,
            start=1,
        ):
            raw_collection = document["trades"]
            assert isinstance(raw_collection, dict)
            if set(raw_collection) - set(requested):
                raise ProviderMalformedError(
                    "provider trades contain an unrequested symbol"
                )
            parsed_page: list[
                tuple[str, datetime, dict[str, object], int, str]
            ] = []
            source_item_ordinal = 0
            for symbol in sorted(raw_collection):
                values = raw_collection[symbol]
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderMalformedError(
                        "provider trades collection is malformed"
                    )
                for index, value in enumerate(values):
                    if not isinstance(value, dict):
                        raise ProviderMalformedError("provider trade is malformed")
                    timestamp = _timestamp(value.get("t"), "trade")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderMalformedError(
                            "provider trade lies outside the requested window"
                        )
                    source_item_ordinal += 1
                    parsed_page.append(
                        (
                            symbol,
                            timestamp,
                            value,
                            source_item_ordinal,
                            f"$.trades.{symbol}[{index}]",
                        )
                    )
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_HISTORICAL_TRADES",
                feed="sip",
                source_timestamp=max(
                    (item[1] for item in parsed_page),
                    default=window.end,
                ),
                request_dependencies=request_dependencies,
            )
            pinned_pages.append(
                (
                    url,
                    document,
                    payload,
                    observation_id,
                    "ALPACA_HISTORICAL_TRADES",
                )
            )
            for (
                symbol,
                timestamp,
                value,
                item_ordinal,
                item_path,
            ) in parsed_page:
                payload_feed = value.get("feed")
                if payload_feed is not None and payload_feed != "sip":
                    raise ProviderMalformedError(
                        "provider trade feed conflicts with requested feed"
                    )
                price = _decimal(value.get("p"), "trade price")
                size = _integer(value.get("s"), "trade size")
                sequence = _integer(value.get("i"), "trade sequence")
                assert size is not None
                assert sequence is not None
                if size <= 0:
                    raise ProviderMalformedError("trade size is malformed")
                trade = Trade(
                    symbol=symbol,
                    timestamp=timestamp,
                    price=price,
                    size=size,
                    feed="sip",
                    sequence=sequence,
                    source_observation_id=observation_id,
                )
                result[symbol].append(trade)
                pending_sources.append(
                    (page_ordinal, item_ordinal, item_path)
                )
            if any(
                len(
                    {
                        (trade.timestamp, trade.sequence)
                        for trade in trades
                    }
                )
                != len(trades)
                for trades in result.values()
            ):
                raise ProviderIncompleteError(
                    "provider trades do not form a complete terminal cohort"
                )
            issued_pages.append(
                issue_page_function(
                    self,
                    page_ordinal=page_ordinal,
                    url=url,
                    document=document,
                    payload=payload,
                    observation_id=observation_id,
                    source_type="ALPACA_HISTORICAL_TRADES",
                    page_sink=page_sink,
                    request_dependencies=request_dependencies,
                )
            )
            require_pages_function(
                self,
                issued_pages,
                request_dependencies,
            )
        completed: dict[str, tuple[Trade, ...]] = {}
        for symbol in sorted(result):
            ordered = tuple(
                sorted(
                    result[symbol],
                    key=lambda item: (item.timestamp, item.sequence),
                )
            )
            identities = tuple(
                (item.timestamp, item.sequence) for item in ordered
            )
            if len(set(identities)) != len(identities):
                raise ProviderIncompleteError(
                    "provider trades do not form a complete terminal cohort"
                )
            completed[symbol] = ordered
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="trades",
            requested_symbols=requested,
            pages=pinned_pages,
            page_bundles=issued_pages,
        )
        issued_result: dict[str, list[Trade]] = {
            symbol: [] for symbol in requested
        }
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, Trade):
                raise ProviderMalformedError("provider TRADE authority is malformed")
            issued_result[fact.symbol].append(fact)
        issued_values = {
            symbol: tuple(
                sorted(
                    issued_result[symbol],
                    key=lambda item: (item.timestamp, item.sequence),
                )
            )
            for symbol in sorted(issued_result)
        }
        return _issue_provider_fetch_cohort(
            owner=self,
            manifest=fetch_manifest,
            values=issued_values,
        )

    def historical_quotes(
        self,
        symbols: Sequence[str],
        window: TimeWindow,
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> Mapping[str, tuple[Quote, ...]]:
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        pages_function = request_dependencies.pages_function
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        requested = _symbols(symbols)
        self._validate_historical_window(window, request_dependencies)
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        query = [
            ("symbols", ",".join(requested)),
            ("start", _format_utc(window.start)),
            ("end", _format_utc(window.end)),
            ("feed", "sip"),
            ("limit", "10000"),
        ]
        result: dict[str, list[Quote]] = {symbol: [] for symbol in requested}
        raw_pages = pages_function(
            self,
            path="/v2/stocks/quotes",
            query=query,
            collection="quotes",
            request_dependencies=request_dependencies,
        )
        pinned_pages: list[
            tuple[str, dict[str, object], bytes, str, str]
        ] = []
        issued_pages: list[ProviderFetchPageBundle] = []
        pending_sources: list[tuple[int, int, str]] = []
        for page_ordinal, (url, document, payload) in enumerate(
            raw_pages,
            start=1,
        ):
            raw_collection = document["quotes"]
            assert isinstance(raw_collection, dict)
            unknown = set(raw_collection) - set(requested)
            if unknown:
                raise ProviderMalformedError("provider quotes contain an unrequested symbol")
            parsed_page: list[
                tuple[str, datetime, dict[str, object], int, str]
            ] = []
            source_item_ordinal = 0
            for symbol in sorted(raw_collection):
                values = raw_collection[symbol]
                if not isinstance(symbol, str) or not isinstance(values, list):
                    raise ProviderMalformedError("provider quotes collection is malformed")
                for index, value in enumerate(values):
                    if not isinstance(value, dict):
                        raise ProviderMalformedError("provider quote is malformed")
                    timestamp = _timestamp(value.get("t"), "quote")
                    if not window.start <= timestamp <= window.end:
                        raise ProviderMalformedError(
                            "provider quote lies outside the requested window"
                        )
                    source_item_ordinal += 1
                    parsed_page.append(
                        (
                            symbol,
                            timestamp,
                            value,
                            source_item_ordinal,
                            f"$.quotes.{symbol}[{index}]",
                        )
                    )
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
                request_dependencies=request_dependencies,
            )
            pinned_pages.append(
                (
                    url,
                    document,
                    payload,
                    observation_id,
                    "ALPACA_HISTORICAL_QUOTES",
                )
            )
            now = self._observations[observation_id].retrieved_at
            for (
                symbol,
                timestamp,
                value,
                item_ordinal,
                item_path,
            ) in parsed_page:
                quote = self._quote(
                    symbol,
                    timestamp,
                    value,
                    feed="sip",
                    now=now,
                    observation_id=observation_id,
                )
                result[symbol].append(quote)
                pending_sources.append(
                    (page_ordinal, item_ordinal, item_path)
                )
            if any(
                len({quote.timestamp for quote in quotes}) != len(quotes)
                for quotes in result.values()
            ):
                raise ProviderIncompleteError(
                    "provider quotes contain duplicate timestamps"
                )
            issued_pages.append(
                issue_page_function(
                    self,
                    page_ordinal=page_ordinal,
                    url=url,
                    document=document,
                    payload=payload,
                    observation_id=observation_id,
                    source_type="ALPACA_HISTORICAL_QUOTES",
                    page_sink=page_sink,
                    request_dependencies=request_dependencies,
                )
            )
            require_pages_function(
                self,
                issued_pages,
                request_dependencies,
            )
        completed: dict[str, tuple[Quote, ...]] = {}
        for symbol in sorted(result):
            ordered = tuple(sorted(result[symbol], key=lambda item: item.timestamp))
            if len({item.timestamp for item in ordered}) != len(ordered):
                raise ProviderIncompleteError(
                    "provider quotes contain duplicate timestamps"
                )
            completed[symbol] = ordered
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="quotes",
            requested_symbols=requested,
            pages=pinned_pages,
            page_bundles=issued_pages,
        )
        issued_result: dict[str, list[Quote]] = {
            symbol: [] for symbol in requested
        }
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, Quote):
                raise ProviderMalformedError("provider QUOTE authority is malformed")
            issued_result[fact.symbol].append(fact)
        issued_values = {
            symbol: tuple(
                sorted(
                    issued_result[symbol],
                    key=lambda item: item.timestamp,
                )
            )
            for symbol in sorted(issued_result)
        }
        return _issue_provider_fetch_cohort(
            owner=self,
            manifest=fetch_manifest,
            values=issued_values,
        )

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
            raise ProviderMalformedError("provider quote feed conflicts with requested feed")
        bid = _decimal(value.get("bp"), "quote bid")
        ask = _decimal(value.get("ap"), "quote ask")
        if ask < bid:
            raise ProviderMalformedError("provider quote is crossed")
        exact_age = (now - timestamp).total_seconds()
        if exact_age < 0:
            raise ProviderMalformedError("provider quote timestamp is in the future")
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

    def latest_iex_quote_cohort(
        self,
        symbols: Sequence[str],
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> ProviderFetchCohort:
        """Return one issued one-quote-per-symbol IEX freshness cohort."""
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        request_dispatch = request_dependencies.request_dispatch
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        requested = _symbols(symbols)
        query = [("symbols", ",".join(requested)), ("feed", "iex")]
        url = (
            f"{request_dependencies.base_url}/v2/stocks/quotes/latest?"
            + urllib.parse.urlencode(query)
        )
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        document, payload = request_dispatch(
            self,
            url,
            request_dependencies,
        )
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
                raise ProviderMalformedError("provider latest quote is malformed")
            parsed.append((symbol, _timestamp(value.get("t"), "latest quote"), value))
        observation_id = self._pin(
            url=url,
            payload=payload,
            source_type="ALPACA_LATEST_QUOTES",
            feed="iex",
            source_timestamp=max(item[1] for item in parsed),
            request_dependencies=request_dependencies,
        )
        now = self._observations[observation_id].retrieved_at
        result: dict[str, Quote] = {}
        pending_sources: list[tuple[int, int, str]] = []
        for source_item_ordinal, (symbol, timestamp, value) in enumerate(
            parsed,
            start=1,
        ):
            quote = self._quote(
                symbol,
                timestamp,
                value,
                feed="iex",
                now=now,
                observation_id=observation_id,
            )
            if quote.age_seconds > _LATEST_MAX_AGE_SECONDS:
                raise ProviderStaleError("IEX freshness quote is stale")
            result[symbol] = quote
            pending_sources.append(
                (
                    1,
                    source_item_ordinal,
                    f"$.quotes.{symbol}",
                )
            )
        manifest_document = dict(document)
        manifest_document.setdefault("next_page_token", None)
        page_bundle = issue_page_function(
            self,
            page_ordinal=1,
            url=url,
            document=manifest_document,
            payload=payload,
            observation_id=observation_id,
            source_type="ALPACA_LATEST_QUOTES",
            page_sink=page_sink,
            request_dependencies=request_dependencies,
        )
        require_pages_function(
            self,
            (page_bundle,),
            request_dependencies,
        )
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="quotes",
            requested_symbols=requested,
            pages=(
                (
                    url,
                    manifest_document,
                    payload,
                    observation_id,
                    "ALPACA_LATEST_QUOTES",
                ),
            ),
            page_bundles=(page_bundle,),
        )
        issued_result: dict[str, tuple[Quote, ...]] = {}
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, Quote):
                raise ProviderMalformedError("provider QUOTE authority is malformed")
            issued_result[fact.symbol] = (fact,)
        return _issue_provider_fetch_cohort(
            owner=self,
            manifest=fetch_manifest,
            values=issued_result,
        )

    def latest_iex_quotes(self, symbols: Sequence[str]) -> Mapping[str, Quote]:
        """Preserve the public latest-quote mapping without a duplicate GET."""
        cohort = self.latest_iex_quote_cohort(symbols)
        return {
            symbol: cohort[symbol][0]
            for symbol in cohort
        }

    def option_chain(
        self,
        underlying: str,
        *,
        page_sink: Callable[[ProviderFetchPageBundle], None] | None = None,
    ) -> ProviderOptionChain:
        page_sink = self._validate_page_sink(page_sink)
        request_dependencies = self._capture_request_dependencies()
        request_verifier = request_dependencies.request_verifier
        pages_function = request_dependencies.pages_function
        issue_page_function = request_dependencies.issue_page_function
        require_pages_function = request_dependencies.require_pages_function
        symbol = _symbols([underlying])[0]
        if (
            request_verifier.__code__
            is not request_dependencies.request_verifier_code
        ):
            raise ProviderMalformedError(
                "provider request dependencies changed during pagination"
            )
        request_verifier(self, request_dependencies)
        query = [("feed", "indicative"), ("limit", "1000")]
        raw_pages = pages_function(
            self,
            path=f"/v1beta1/options/snapshots/{urllib.parse.quote(symbol)}",
            query=query,
            collection="snapshots",
            request_dependencies=request_dependencies,
        )
        pinned_pages: list[
            tuple[str, dict[str, object], bytes, str, str]
        ] = []
        issued_pages: list[ProviderFetchPageBundle] = []
        pending_sources: list[tuple[int, int, str]] = []
        seen_occ_symbols: set[str] = set()
        for page_ordinal, (url, document, payload) in enumerate(
            raw_pages,
            start=1,
        ):
            raw = document["snapshots"]
            assert isinstance(raw, dict)
            observed_values: list[datetime] = []
            for source_item_ordinal, occ_symbol in enumerate(
                sorted(raw),
                start=1,
            ):
                value = raw[occ_symbol]
                match = _OCC_SYMBOL.fullmatch(occ_symbol) if isinstance(occ_symbol, str) else None
                if match is None or not isinstance(value, dict) or match.group("root") != symbol:
                    raise ProviderMalformedError("indicative option snapshot is malformed")
                if occ_symbol in seen_occ_symbols:
                    raise ProviderIncompleteError(
                        "option snapshot is duplicated across pages"
                    )
                seen_occ_symbols.add(occ_symbol)
                quote = value.get("latestQuote")
                if isinstance(quote, dict) and quote.get("t") is not None:
                    observed_values.append(_timestamp(quote.get("t"), "option quote"))
                pending_sources.append(
                    (
                        page_ordinal,
                        source_item_ordinal,
                        f"$.snapshots.{occ_symbol}",
                    )
                )
            observation_id = self._pin(
                url=url,
                payload=payload,
                source_type="ALPACA_OPTION_SNAPSHOTS",
                feed="indicative",
                source_timestamp=max(
                    observed_values,
                    default=self._current_time(request_dependencies),
                ),
                request_dependencies=request_dependencies,
            )
            pinned_pages.append(
                (
                    url,
                    document,
                    payload,
                    observation_id,
                    "ALPACA_OPTION_SNAPSHOTS",
                )
            )
            observation = self._observations[observation_id]
            for occ_symbol in sorted(raw):
                value = raw[occ_symbol]
                assert isinstance(occ_symbol, str)
                assert isinstance(value, dict)
                _option_snapshot_from_raw_item(
                    symbol=occ_symbol,
                    value=value,
                    observation=observation,
                    requested_symbols=(symbol,),
                )
            issued_pages.append(
                issue_page_function(
                    self,
                    page_ordinal=page_ordinal,
                    url=url,
                    document=document,
                    payload=payload,
                    observation_id=observation_id,
                    source_type="ALPACA_OPTION_SNAPSHOTS",
                    page_sink=page_sink,
                    request_dependencies=request_dependencies,
                )
            )
            require_pages_function(
                self,
                issued_pages,
                request_dependencies,
            )
        if not pending_sources:
            raise ProviderIncompleteError("indicative option snapshot response is empty")
        fetch_manifest = _provider_fetch_manifest(
            owner=self,
            collection="snapshots",
            requested_symbols=(symbol,),
            pages=pinned_pages,
            page_bundles=issued_pages,
        )
        snapshots: dict[str, OptionSnapshot] = {}
        for page_ordinal, item_ordinal, item_path in pending_sources:
            fact = _issue_market_fact_from_fetch(
                owner=self,
                fetch_manifest=fetch_manifest,
                page_ordinal=page_ordinal,
                source_item_ordinal=item_ordinal,
                source_item_path=item_path,
            )
            if not isinstance(fact, OptionSnapshot):
                raise ProviderMalformedError(
                    "provider OPTION_SNAPSHOT authority is malformed"
                )
            snapshots[fact.occ_symbol] = fact
        return _issue_provider_option_chain(
            owner=self,
            manifest=fetch_manifest,
            snapshots=tuple(snapshots[name] for name in sorted(snapshots)),
        )

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
        failures: list[str] = []

        def add_failure(reason: str) -> None:
            if reason not in failures:
                failures.append(reason)

        def classify(
            error: ProviderResponseError,
            *,
            check: str,
        ) -> None:
            nonlocal authenticated
            if isinstance(error, HttpTransportError):
                add_failure("CONNECTIVITY_UNAVAILABLE")
                return
            if isinstance(error, HttpStatusError):
                if error.status in {408, 425, 429} or 500 <= error.status <= 599:
                    add_failure("PROVIDER_AVAILABILITY_UNAVAILABLE")
                elif check == "iex" and error.status in {401, 403}:
                    add_failure("AUTHENTICATION_UNAVAILABLE")
                elif check == "sip" and error.status == 401:
                    authenticated = False
                    add_failure("AUTHENTICATION_UNAVAILABLE")
                elif check == "sip" and error.status == 403:
                    add_failure("HISTORICAL_SIP_ENTITLEMENT_UNAVAILABLE")
                else:
                    authenticated = True
                    add_failure("MALFORMED_PROVIDER_RESPONSE")
                return
            authenticated = True
            if isinstance(error, ProviderStaleError):
                add_failure("IEX_QUOTE_STALE")
            elif isinstance(error, ProviderIncompleteError):
                add_failure("PROVIDER_COHORT_INCOMPLETE")
            elif isinstance(error, ProviderMalformedError):
                add_failure("MALFORMED_PROVIDER_RESPONSE")
            else:
                add_failure("MALFORMED_PROVIDER_RESPONSE")

        try:
            self.latest_iex_quotes(["SPY"])
            authenticated = True
            iex_fresh = True
        except ProviderResponseError as error:
            classify(error, check="iex")
        if authenticated:
            if not isinstance(completed_session, TimeWindow):
                add_failure("COMPLETED_SESSION_RELEASE_UNAVAILABLE")
            else:
                try:
                    self.daily_bars(["SPY"], completed_session)
                    sip_ok = True
                except ProviderResponseError as error:
                    classify(error, check="sip")
        status = "READY"
        for reason, blocked_status in (
            ("CONNECTIVITY_UNAVAILABLE", "BLOCKED_CONNECTIVITY"),
            ("PROVIDER_AVAILABILITY_UNAVAILABLE", "BLOCKED_AVAILABILITY"),
            ("AUTHENTICATION_UNAVAILABLE", "BLOCKED_AUTHENTICATION"),
            ("MALFORMED_PROVIDER_RESPONSE", "BLOCKED_MALFORMED_RESPONSE"),
            ("PROVIDER_COHORT_INCOMPLETE", "BLOCKED_INCOMPLETE_COHORT"),
            ("IEX_QUOTE_STALE", "BLOCKED_IEX_FRESHNESS"),
            (
                "COMPLETED_SESSION_RELEASE_UNAVAILABLE",
                "BLOCKED_COMPLETED_SESSION",
            ),
            (
                "HISTORICAL_SIP_ENTITLEMENT_UNAVAILABLE",
                "BLOCKED_ENTITLEMENT",
            ),
        ):
            if reason in failures:
                status = blocked_status
                break
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
    "AlpacaPageMetadata",
    "Bar",
    "EntitlementSmoke",
    "HISTORICAL_SIP_RELEASE_DELAY",
    "OptionSnapshot",
    "NormalizedMarketFactSource",
    "ProviderDataError",
    "ProviderFetchCohort",
    "ProviderFetchBundle",
    "ProviderFetchManifest",
    "ProviderFetchPage",
    "ProviderFetchPageBundle",
    "ProviderIncompleteError",
    "ProviderMalformedError",
    "ProviderOptionChain",
    "ProviderStaleError",
    "Quote",
    "TimeWindow",
    "Trade",
    "is_ingestible_provider_fetch_cohort",
    "is_ingestible_provider_option_chain",
    "is_issued_normalized_market_fact",
    "is_issued_provider_fetch_page_bundle",
    "is_issued_provider_option_chain",
    "is_issued_provider_fetch_cohort",
    "normalized_market_facts_share_owner",
    "provider_fetch_cohorts_share_owner",
    "read_provider_fetch_bundle",
    "recompute_alpaca_page_metadata",
]
