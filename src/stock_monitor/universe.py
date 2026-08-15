"""Offline validation for versioned, primary-sourced trading universes."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit
from weakref import ReferenceType, ref


_ACQUISITION_METHOD = "manual_primary_source_review"
_TICK_CLASSIFICATION = "reviewed_conditional_price_at_or_above_1_usd"
_ETF_CLASSIFICATION = (
    "manual_official_sponsor_objective_review_same_direction"
)
_SP500_IT_URL = (
    "https://www.spglobal.com/spdji/en/indices/equity/"
    "sp-500-information-technology-sector/"
)
_NASDAQ_100_URL = "https://www.nasdaq.com/docs/2026/05/04/NDX.pdf"
_NASDAQ_TICK_URL = (
    "https://listingcenter.nasdaq.com/rulebook/nasdaq/rules/"
    "Nasdaq%20Equity%201"
)
_NYSE_TICK_URL = "https://www.nyse.com/regulation/rules"
_SEC_TICK_FAQ_URL = "https://www.sec.gov/divisions/marketreg/subpenny612faq.htm"
_SEC_TICK_POSTPONEMENT_URL = (
    "https://www.sec.gov/newsroom/speeches-statements/"
    "atkins-statement-minimum-pricing-increments-access-fee-caps-061126"
)
_TICK_SOURCE_URLS = {
    "nasdaq_listed": _NASDAQ_TICK_URL,
    "nyse_listed": _NYSE_TICK_URL,
    "sec_subpenny_faq": _SEC_TICK_FAQ_URL,
    "sec_minimum_increment_postponement": _SEC_TICK_POSTPONEMENT_URL,
}
_ALLOWED_HOSTS = frozenset(
    {
        "www.spglobal.com",
        "www.nasdaq.com",
        "www.sec.gov",
        "www.ssga.com",
        "investor.vanguard.com",
        "personal1.vanguard.com",
        "www.invesco.com",
        "listingcenter.nasdaq.com",
        "www.nyse.com",
    }
)
_SPONSOR_HOSTS = frozenset(
    {
        "www.ssga.com",
        "investor.vanguard.com",
        "personal1.vanguard.com",
        "www.invesco.com",
    }
)
_SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9]{0,5}")
_DECIMAL_PATTERN = re.compile(r"(?:0|[1-9]\d*)(?:\.\d+)?")
_CHECKSUM_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_SHA256_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_MINIMUM_FREE_FLOAT = 50_000_000
_VERIFIED_UNIVERSE_AUTHORITY = object()
_MAPPING_PROXY_TYPE = type(MappingProxyType({}))
_CURRENT_UNIVERSE_RELATIVE_PATH = Path("data/universe/2026-08-14.json")
_MAXIMUM_UNIVERSE_BYTES = 1_048_576
CURRENT_UNIVERSE_SHA256 = (
    "e277048b6c0580dc7f82d062f04ac2898f51ff81cb3673e4d79b54f320fba753"
)


class UniverseError(ValueError):
    """A universe snapshot is stale, corrupt, or lacks required provenance."""


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    """A primary or rule source with its evidence date."""

    url: str
    source_as_of: date
    scope: str = ""


@dataclass(frozen=True, slots=True)
class MembershipEvidence:
    """Primary evidence that a stock belongs to a reviewed index."""

    index: str
    url: str
    source_as_of: date
    acquisition_method: str


@dataclass(frozen=True, slots=True)
class FloatDerivation:
    """Exact primary-source inputs supporting a conservative float value."""

    source_as_of: date
    sources: tuple[SourceEvidence, ...]
    formula: str
    operands: Mapping[str, int | str]
    operand_source_dates: Mapping[str, date]
    derived_value: int
    stored_value: int
    rounding: str
    rounded_down: bool
    corroborating_shares_outstanding: int | None = None
    corroborating_shares_outstanding_as_of: date | None = None


@dataclass(frozen=True, slots=True)
class UniverseRecord:
    """One immutable, statically eligible stock or ETF record."""

    symbol: str
    product_type: str
    listing_venue: str
    benchmark: str
    sector_etf: str | None
    support_roles: tuple[str, ...]
    enabled: bool
    leveraged: bool
    inverse: bool
    reviewed_at: date
    tick_size: Decimal
    tick_source: SourceEvidence
    tick_classification: str
    source_url: str
    source_as_of: date
    membership_sources: tuple[MembershipEvidence, ...] = ()
    free_float: int | None = None
    float_derivation: FloatDerivation | None = None
    sponsor_sources: tuple[SourceEvidence, ...] = ()
    objective_classification_method: str | None = None


@dataclass(frozen=True, slots=True, weakref_slot=True)
class UniverseSnapshot:
    """A checksum-verified universe that is valid for a bounded review window."""

    effective_date: date
    reviewed_at: date
    review_by: date
    acquisition_method: str
    checksum: str
    records: tuple[UniverseRecord, ...]
    by_symbol: Mapping[str, UniverseRecord]
    regime_support_symbols: tuple[str, ...]
    sector_mapping: Mapping[str, str]
    tick_policy_sources: tuple[SourceEvidence, ...]
    _authority: object = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _snapshot_digest: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _release_pin: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    @classmethod
    def load(cls, path: Path, as_of: date) -> UniverseSnapshot:
        """Load a local JSON snapshot without performing any network access."""
        if not isinstance(path, Path):
            raise UniverseError("universe path must be a Path")
        try:
            raw = json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=_unique_object,
                parse_float=_reject_json_float,
                parse_constant=_reject_json_constant,
            )
        except UniverseError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise UniverseError("universe manifest could not be read") from exc
        return cls.from_mapping(raw, as_of=as_of)

    @classmethod
    def from_mapping(cls, raw: object, as_of: date) -> UniverseSnapshot:
        """Validate decoded JSON, provenance, checksum, and review freshness."""
        if type(as_of) is not date:
            raise UniverseError("universe as_of must be a date")
        table = _mapping(raw, "universe")
        checksum = table.get("checksum")
        if not isinstance(checksum, str) or _CHECKSUM_PATTERN.fullmatch(checksum) is None:
            raise UniverseError("universe checksum is missing or malformed")
        calculated_checksum = canonical_payload_checksum(table)
        if not hmac.compare_digest(checksum, calculated_checksum):
            raise UniverseError("universe checksum does not match its payload")

        _exact_keys(
            table,
            {
                "schema_version",
                "effective_date",
                "reviewed_at",
                "review_by",
                "acquisition_method",
                "benchmark_policy",
                "tick_policy",
                "records",
                "checksum",
            },
            "universe",
        )
        if type(table["schema_version"]) is not int or table["schema_version"] != 1:
            raise UniverseError("unsupported universe schema version")

        effective_date = _iso_date(table["effective_date"], "effective_date")
        reviewed_at = _iso_date(table["reviewed_at"], "reviewed_at")
        review_by = _iso_date(table["review_by"], "review_by")
        if table["acquisition_method"] != _ACQUISITION_METHOD:
            raise UniverseError("universe was not manually reviewed from primary sources")
        if reviewed_at != effective_date:
            raise UniverseError("universe effective date is not its reviewed date")
        if review_by != reviewed_at + timedelta(days=31):
            raise UniverseError("universe review window is not the approved 31 days")
        if as_of < effective_date or as_of > review_by:
            raise UniverseError("universe snapshot is not effective or is stale")

        benchmark = _benchmark_policy(table["benchmark_policy"])
        tick_sources = _tick_policy(table["tick_policy"], reviewed_at)

        raw_records = _sequence(table["records"], "records")
        if not raw_records:
            raise UniverseError("universe has no records")
        records = tuple(
            _universe_record(
                item,
                reviewed_at=reviewed_at,
                benchmark=benchmark,
                tick_sources=tick_sources,
            )
            for item in raw_records
        )
        symbols = tuple(record.symbol for record in records)
        if len(set(symbols)) != len(symbols):
            raise UniverseError("universe contains duplicate symbols")
        if symbols != tuple(sorted(symbols)):
            raise UniverseError("universe records must be sorted by symbol")

        by_symbol = {record.symbol: record for record in records}
        sector_mapping = benchmark["sector_mapping"]
        stock_symbols = {
            record.symbol
            for record in records
            if record.product_type == "common_stock"
        }
        if set(sector_mapping) != stock_symbols:
            raise UniverseError("sector mapping is missing or contains non-stock symbols")
        for record in records:
            if record.benchmark not in by_symbol:
                raise UniverseError("record benchmark is missing from the universe")
            if record.sector_etf is not None:
                sector_record = by_symbol.get(record.sector_etf)
                if sector_record is None or sector_record.product_type != "etf":
                    raise UniverseError("stock sector ETF is missing from the universe")

        _validate_support_roles(
            records,
            by_symbol,
            benchmark["regime_support_symbols"],
            sector_mapping,
        )
        return cls(
            effective_date=effective_date,
            reviewed_at=reviewed_at,
            review_by=review_by,
            acquisition_method=_ACQUISITION_METHOD,
            checksum=checksum,
            records=records,
            by_symbol=MappingProxyType(by_symbol),
            regime_support_symbols=benchmark["regime_support_symbols"],
            sector_mapping=MappingProxyType(dict(sector_mapping)),
            tick_policy_sources=tick_sources,
        )

    def eligible_records(self) -> tuple[UniverseRecord, ...]:
        """Return the immutable statically eligible allow-list."""
        return tuple(record for record in self.records if record.enabled)


_ISSUED_UNIVERSES: dict[
    int,
    tuple[ReferenceType[UniverseSnapshot], str],
] = {}
_ISSUED_UNIVERSES_LOCK = RLock()


def canonical_payload_checksum(raw: Mapping[str, object]) -> str:
    """Hash canonical JSON after removing only the top-level checksum field."""
    payload = {key: value for key, value in raw.items() if key != "checksum"}
    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise UniverseError("universe payload is not canonical JSON") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def load_current_universe(
    project_root: Path,
    *,
    as_of: date,
) -> UniverseSnapshot:
    """Load only the externally pinned current reviewed universe release."""
    root = Path(project_root)
    manifest = root / _CURRENT_UNIVERSE_RELATIVE_PATH
    try:
        payload = manifest.read_bytes()
    except OSError as exc:
        raise UniverseError("current universe manifest could not be read") from exc
    if not payload or len(payload) > _MAXIMUM_UNIVERSE_BYTES:
        raise UniverseError("current universe manifest size is invalid")
    digest = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(digest, CURRENT_UNIVERSE_SHA256):
        raise UniverseError("current universe release checksum mismatch")
    try:
        raw = json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_float=_reject_json_float,
            parse_constant=_reject_json_constant,
        )
    except UniverseError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise UniverseError("current universe manifest could not be read") from exc
    snapshot = UniverseSnapshot.from_mapping(raw, as_of=as_of)
    object.__setattr__(snapshot, "_authority", _VERIFIED_UNIVERSE_AUTHORITY)
    object.__setattr__(snapshot, "_release_pin", CURRENT_UNIVERSE_SHA256)
    snapshot_digest = _snapshot_fingerprint(snapshot)
    object.__setattr__(snapshot, "_snapshot_digest", snapshot_digest)
    identity = id(snapshot)

    def discard_snapshot(
        dead_reference: ReferenceType[UniverseSnapshot],
    ) -> None:
        with _ISSUED_UNIVERSES_LOCK:
            current = _ISSUED_UNIVERSES.get(identity)
            if current is not None and current[0] is dead_reference:
                del _ISSUED_UNIVERSES[identity]

    snapshot_reference = ref(snapshot, discard_snapshot)
    with _ISSUED_UNIVERSES_LOCK:
        _ISSUED_UNIVERSES[identity] = (snapshot_reference, snapshot_digest)
    return snapshot


def _date_document(value: object, name: str) -> str:
    if type(value) is not date:
        raise TypeError(f"verified universe {name} is malformed")
    return value.isoformat()


def _decimal_document(value: object, name: str) -> dict[str, object]:
    if type(value) is not Decimal or not value.is_finite():
        raise TypeError(f"verified universe {name} is malformed")
    parts = value.as_tuple()
    return {
        "digits": list(parts.digits),
        "exponent": parts.exponent,
        "sign": parts.sign,
    }


def _source_evidence_document(value: object) -> dict[str, object]:
    if (
        type(value) is not SourceEvidence
        or type(value.url) is not str
        or type(value.scope) is not str
    ):
        raise TypeError("verified universe source evidence is malformed")
    return {
        "scope": value.scope,
        "source_as_of": _date_document(value.source_as_of, "source date"),
        "url": value.url,
    }


def _membership_evidence_document(value: object) -> dict[str, object]:
    if (
        type(value) is not MembershipEvidence
        or type(value.index) is not str
        or type(value.url) is not str
        or type(value.acquisition_method) is not str
    ):
        raise TypeError("verified universe membership evidence is malformed")
    return {
        "acquisition_method": value.acquisition_method,
        "index": value.index,
        "source_as_of": _date_document(value.source_as_of, "membership date"),
        "url": value.url,
    }


def _string_mapping_document(value: object, name: str) -> dict[str, str]:
    if type(value) is not _MAPPING_PROXY_TYPE or any(
        type(key) is not str or type(item) is not str
        for key, item in value.items()
    ):
        raise TypeError(f"verified universe {name} is malformed")
    return dict(value)


def _float_derivation_document(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if (
        type(value) is not FloatDerivation
        or type(value.sources) is not tuple
        or any(type(item) is not SourceEvidence for item in value.sources)
        or type(value.formula) is not str
        or type(value.operands) is not _MAPPING_PROXY_TYPE
        or any(
            type(key) is not str or type(item) not in {int, str}
            for key, item in value.operands.items()
        )
        or type(value.operand_source_dates) is not _MAPPING_PROXY_TYPE
        or any(
            type(key) is not str or type(item) is not date
            for key, item in value.operand_source_dates.items()
        )
        or type(value.derived_value) is not int
        or type(value.stored_value) is not int
        or type(value.rounding) is not str
        or type(value.rounded_down) is not bool
        or (
            value.corroborating_shares_outstanding is not None
            and type(value.corroborating_shares_outstanding) is not int
        )
        or (
            value.corroborating_shares_outstanding_as_of is not None
            and type(value.corroborating_shares_outstanding_as_of) is not date
        )
    ):
        raise TypeError("verified universe float derivation is malformed")
    operands = {
        key: {"type": "integer", "value": item}
        if type(item) is int
        else {"type": "string", "value": item}
        for key, item in value.operands.items()
    }
    operand_dates = {
        key: _date_document(item, "float operand date")
        for key, item in value.operand_source_dates.items()
    }
    return {
        "corroborating_shares_outstanding": (
            value.corroborating_shares_outstanding
        ),
        "corroborating_shares_outstanding_as_of": (
            _date_document(
                value.corroborating_shares_outstanding_as_of,
                "float corroboration date",
            )
            if value.corroborating_shares_outstanding_as_of is not None
            else None
        ),
        "derived_value": value.derived_value,
        "formula": value.formula,
        "operand_source_dates": operand_dates,
        "operands": operands,
        "rounded_down": value.rounded_down,
        "rounding": value.rounding,
        "source_as_of": _date_document(value.source_as_of, "float source date"),
        "sources": [_source_evidence_document(item) for item in value.sources],
        "stored_value": value.stored_value,
    }


def _universe_record_document(value: object) -> dict[str, object]:
    if (
        type(value) is not UniverseRecord
        or any(
            type(item) is not str
            for item in (
                value.symbol,
                value.product_type,
                value.listing_venue,
                value.benchmark,
                value.tick_classification,
                value.source_url,
            )
        )
        or (value.sector_etf is not None and type(value.sector_etf) is not str)
        or type(value.support_roles) is not tuple
        or any(type(item) is not str for item in value.support_roles)
        or type(value.enabled) is not bool
        or type(value.leveraged) is not bool
        or type(value.inverse) is not bool
        or type(value.tick_source) is not SourceEvidence
        or type(value.membership_sources) is not tuple
        or any(
            type(item) is not MembershipEvidence
            for item in value.membership_sources
        )
        or (value.free_float is not None and type(value.free_float) is not int)
        or type(value.sponsor_sources) is not tuple
        or any(type(item) is not SourceEvidence for item in value.sponsor_sources)
        or (
            value.objective_classification_method is not None
            and type(value.objective_classification_method) is not str
        )
    ):
        raise TypeError("verified universe record is malformed")
    return {
        "benchmark": value.benchmark,
        "enabled": value.enabled,
        "float_derivation": _float_derivation_document(value.float_derivation),
        "free_float": value.free_float,
        "inverse": value.inverse,
        "leveraged": value.leveraged,
        "listing_venue": value.listing_venue,
        "membership_sources": [
            _membership_evidence_document(item)
            for item in value.membership_sources
        ],
        "objective_classification_method": value.objective_classification_method,
        "product_type": value.product_type,
        "reviewed_at": _date_document(value.reviewed_at, "record review date"),
        "sector_etf": value.sector_etf,
        "source_as_of": _date_document(value.source_as_of, "record source date"),
        "source_url": value.source_url,
        "sponsor_sources": [
            _source_evidence_document(item) for item in value.sponsor_sources
        ],
        "support_roles": list(value.support_roles),
        "symbol": value.symbol,
        "tick_classification": value.tick_classification,
        "tick_size": _decimal_document(value.tick_size, "record tick size"),
        "tick_source": _source_evidence_document(value.tick_source),
    }


def _snapshot_fingerprint(value: UniverseSnapshot) -> str:
    if (
        type(value) is not UniverseSnapshot
        or type(value.acquisition_method) is not str
        or type(value.checksum) is not str
        or type(value.records) is not tuple
        or any(type(record) is not UniverseRecord for record in value.records)
        or type(value.by_symbol) is not _MAPPING_PROXY_TYPE
        or any(
            type(symbol) is not str or type(record) is not UniverseRecord
            for symbol, record in value.by_symbol.items()
        )
        or type(value.regime_support_symbols) is not tuple
        or any(type(symbol) is not str for symbol in value.regime_support_symbols)
        or type(value.sector_mapping) is not _MAPPING_PROXY_TYPE
        or type(value.tick_policy_sources) is not tuple
        or any(
            type(item) is not SourceEvidence for item in value.tick_policy_sources
        )
    ):
        raise TypeError("verified universe snapshot fields are malformed")
    document = {
        "acquisition_method": value.acquisition_method,
        "by_symbol": {
            symbol: _universe_record_document(record)
            for symbol, record in value.by_symbol.items()
        },
        "checksum": value.checksum,
        "effective_date": _date_document(value.effective_date, "effective date"),
        "records": [_universe_record_document(record) for record in value.records],
        "regime_support_symbols": list(value.regime_support_symbols),
        "review_by": _date_document(value.review_by, "review deadline"),
        "reviewed_at": _date_document(value.reviewed_at, "review date"),
        "sector_mapping": _string_mapping_document(
            value.sector_mapping,
            "sector mapping",
        ),
        "tick_policy_sources": [
            _source_evidence_document(item) for item in value.tick_policy_sources
        ],
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _issued_universe_digest(value: UniverseSnapshot) -> str | None:
    with _ISSUED_UNIVERSES_LOCK:
        issued = _ISSUED_UNIVERSES.get(id(value))
        if issued is None or issued[0]() is not value:
            return None
        return issued[1]


def is_verified_universe_snapshot(value: object) -> bool:
    """Return true only for an untampered snapshot from the validated loader."""
    if type(value) is not UniverseSnapshot:
        return False
    issued_digest = _issued_universe_digest(value)
    if (
        issued_digest is None
        or value._authority is not _VERIFIED_UNIVERSE_AUTHORITY
        or value._release_pin != CURRENT_UNIVERSE_SHA256
        or type(value._snapshot_digest) is not str
        or _SHA256_DIGEST_PATTERN.fullmatch(value._snapshot_digest) is None
        or not hmac.compare_digest(value._snapshot_digest, issued_digest)
    ):
        return False
    try:
        return hmac.compare_digest(
            value._snapshot_digest,
            _snapshot_fingerprint(value),
        )
    except (TypeError, ValueError):
        return False


def _universe_record(
    value: object,
    *,
    reviewed_at: date,
    benchmark: Mapping[str, Any],
    tick_sources: tuple[SourceEvidence, ...],
) -> UniverseRecord:
    table = _mapping(value, "universe record")
    product_type = table.get("product_type")
    common_keys = {
        "symbol",
        "product_type",
        "listing_venue",
        "benchmark",
        "sector_etf",
        "support_roles",
        "enabled",
        "leveraged",
        "inverse",
        "reviewed_at",
        "tick_size",
        "tick_source_url",
        "tick_source_as_of",
        "tick_classification",
        "source_url",
        "source_as_of",
    }
    if product_type == "common_stock":
        _exact_keys(
            table,
            common_keys | {"membership_sources", "free_float", "float_source"},
            "stock record",
        )
    elif product_type == "etf":
        _exact_keys(
            table,
            common_keys
            | {"sponsor_sources", "objective_classification_method"},
            "ETF record",
        )
    else:
        raise UniverseError("record product_type is not approved")

    symbol = _symbol(table["symbol"], "symbol")
    listing_venue = _string(table["listing_venue"], "listing_venue")
    if listing_venue not in {"NASDAQ", "NYSE", "NYSE_ARCA"}:
        raise UniverseError("record listing venue is not approved")
    market_benchmark = _symbol(table["benchmark"], "benchmark")
    roles = _string_tuple(table["support_roles"], "support_roles")
    if len(set(roles)) != len(roles) or "eligible" not in roles:
        raise UniverseError("record support roles are missing or duplicated")
    allowed_roles = {
        "eligible",
        "market_benchmark",
        "regime_benchmark",
        "spy_benchmark",
        "sector_benchmark",
    }
    if not set(roles) <= allowed_roles:
        raise UniverseError("record has an unknown support role")

    enabled = _boolean(table["enabled"], "enabled")
    leveraged = _boolean(table["leveraged"], "leveraged")
    inverse = _boolean(table["inverse"], "inverse")
    if not enabled:
        raise UniverseError("disabled universe record blocks the snapshot")
    if leveraged or inverse:
        raise UniverseError("leveraged or inverse products are ineligible")
    record_reviewed_at = _iso_date(table["reviewed_at"], "record reviewed_at")
    if record_reviewed_at != reviewed_at:
        raise UniverseError("record has stale review metadata")

    tick_size = _positive_decimal_string(table["tick_size"], "tick_size")
    if tick_size != Decimal("0.01"):
        raise UniverseError("reviewed tick size must be 0.01 while price is at least 1")
    if table["tick_classification"] != _TICK_CLASSIFICATION:
        raise UniverseError("tick size lacks reviewed conditional classification")
    tick_url = _source_url(table["tick_source_url"], "tick_source_url")
    tick_as_of = _evidence_date(
        table["tick_source_as_of"],
        "tick_source_as_of",
        reviewed_at,
    )
    expected_scope = "nasdaq_listed" if listing_venue == "NASDAQ" else "nyse_listed"
    expected_tick = next(
        (source for source in tick_sources if source.scope == expected_scope),
        None,
    )
    if (
        expected_tick is None
        or tick_url != expected_tick.url
        or tick_as_of != expected_tick.source_as_of
    ):
        raise UniverseError("tick source conflicts with the listing venue")
    tick_source = SourceEvidence(
        url=tick_url,
        source_as_of=tick_as_of,
        scope=expected_scope,
    )

    source_url = _source_url(table["source_url"], "source_url")
    source_as_of = _evidence_date(
        table["source_as_of"],
        "source_as_of",
        reviewed_at,
    )
    sector_value = table["sector_etf"]

    if product_type == "common_stock":
        if not isinstance(sector_value, str):
            raise UniverseError("stock sector ETF mapping is missing")
        sector_etf = _symbol(sector_value, "sector_etf")
        expected_sector = benchmark["sector_mapping"].get(symbol)
        if sector_etf != expected_sector:
            raise UniverseError("stock sector ETF conflicts with benchmark policy")
        if market_benchmark != benchmark["stock_market_benchmark"]:
            raise UniverseError("stock market benchmark conflicts with policy")
        memberships = _membership_sources(
            table["membership_sources"],
            reviewed_at=reviewed_at,
        )
        free_float = _positive_integer(table["free_float"], "free_float")
        if free_float < _MINIMUM_FREE_FLOAT:
            raise UniverseError("stock free float must be at least 50 million shares")
        float_derivation = _float_derivation(
            table["float_source"],
            reviewed_at=reviewed_at,
            free_float=free_float,
            primary_url=source_url,
            primary_as_of=source_as_of,
        )
        return UniverseRecord(
            symbol=symbol,
            product_type=product_type,
            listing_venue=listing_venue,
            benchmark=market_benchmark,
            sector_etf=sector_etf,
            support_roles=roles,
            enabled=enabled,
            leveraged=leveraged,
            inverse=inverse,
            reviewed_at=record_reviewed_at,
            tick_size=tick_size,
            tick_source=tick_source,
            tick_classification=_TICK_CLASSIFICATION,
            source_url=source_url,
            source_as_of=source_as_of,
            membership_sources=memberships,
            free_float=free_float,
            float_derivation=float_derivation,
        )

    if sector_value is not None:
        raise UniverseError("ETF sector mapping must be explicit null")
    expected_benchmark = (
        benchmark["spy_market_benchmark"]
        if symbol == "SPY"
        else benchmark["other_etf_market_benchmark"]
    )
    if market_benchmark != expected_benchmark:
        raise UniverseError("ETF market benchmark conflicts with policy")
    sponsor_sources = _sponsor_sources(
        table["sponsor_sources"],
        reviewed_at=reviewed_at,
    )
    if (source_url, source_as_of) not in {
        (source.url, source.source_as_of) for source in sponsor_sources
    }:
        raise UniverseError("ETF primary source conflicts with sponsor evidence")
    if table["objective_classification_method"] != _ETF_CLASSIFICATION:
        raise UniverseError("ETF objective classification method is missing")
    return UniverseRecord(
        symbol=symbol,
        product_type=product_type,
        listing_venue=listing_venue,
        benchmark=market_benchmark,
        sector_etf=None,
        support_roles=roles,
        enabled=enabled,
        leveraged=leveraged,
        inverse=inverse,
        reviewed_at=record_reviewed_at,
        tick_size=tick_size,
        tick_source=tick_source,
        tick_classification=_TICK_CLASSIFICATION,
        source_url=source_url,
        source_as_of=source_as_of,
        sponsor_sources=sponsor_sources,
        objective_classification_method=_ETF_CLASSIFICATION,
    )


def _benchmark_policy(value: object) -> Mapping[str, Any]:
    table = _mapping(value, "benchmark_policy")
    _exact_keys(
        table,
        {
            "stock_market_benchmark",
            "spy_market_benchmark",
            "other_etf_market_benchmark",
            "regime_support_symbols",
            "sector_mapping",
        },
        "benchmark_policy",
    )
    if (
        table["stock_market_benchmark"] != "SPY"
        or table["spy_market_benchmark"] != "VTI"
        or table["other_etf_market_benchmark"] != "SPY"
    ):
        raise UniverseError("market benchmark policy is not approved")
    regime = _string_tuple(
        table["regime_support_symbols"],
        "regime_support_symbols",
    )
    if regime != ("SPY", "QQQ"):
        raise UniverseError("regime support must distinguish SPY and QQQ")
    sector_raw = _mapping(table["sector_mapping"], "sector_mapping")
    sector_mapping: dict[str, str] = {}
    for raw_symbol, raw_sector in sector_raw.items():
        symbol = _symbol(raw_symbol, "sector mapping symbol")
        sector = _symbol(raw_sector, "sector mapping ETF")
        if symbol in sector_mapping:
            raise UniverseError("sector mapping contains duplicate symbols")
        sector_mapping[symbol] = sector
    return MappingProxyType(
        {
            "stock_market_benchmark": "SPY",
            "spy_market_benchmark": "VTI",
            "other_etf_market_benchmark": "SPY",
            "regime_support_symbols": regime,
            "sector_mapping": MappingProxyType(sector_mapping),
        }
    )


def _tick_policy(value: object, reviewed_at: date) -> tuple[SourceEvidence, ...]:
    table = _mapping(value, "tick_policy")
    _exact_keys(
        table,
        {"classification", "reviewed_at", "sources"},
        "tick_policy",
    )
    if table["classification"] != _TICK_CLASSIFICATION:
        raise UniverseError("tick policy is not reviewed and conditional")
    if _iso_date(table["reviewed_at"], "tick policy reviewed_at") != reviewed_at:
        raise UniverseError("tick policy review is stale")
    sources: list[SourceEvidence] = []
    for item in _sequence(table["sources"], "tick policy sources"):
        source = _mapping(item, "tick policy source")
        _exact_keys(source, {"scope", "url", "source_as_of"}, "tick policy source")
        scope = _string(source["scope"], "tick policy scope")
        expected_url = _TICK_SOURCE_URLS.get(scope)
        if expected_url is None:
            raise UniverseError("tick policy source scope is unknown")
        url = _source_url(source["url"], "tick policy URL")
        source_as_of = _evidence_date(
            source["source_as_of"],
            "tick policy source_as_of",
            reviewed_at,
        )
        if url != expected_url or source_as_of != reviewed_at:
            raise UniverseError("tick policy source is stale or conflicting")
        sources.append(SourceEvidence(url=url, source_as_of=source_as_of, scope=scope))
    if {source.scope for source in sources} != set(_TICK_SOURCE_URLS):
        raise UniverseError("tick policy source set is incomplete")
    if len(sources) != len(_TICK_SOURCE_URLS):
        raise UniverseError("tick policy contains duplicate sources")
    return tuple(sources)


def _membership_sources(
    value: object,
    *,
    reviewed_at: date,
) -> tuple[MembershipEvidence, ...]:
    expected = {
        "sp_500_information_technology": _SP500_IT_URL,
        "nasdaq_100": _NASDAQ_100_URL,
    }
    evidence: list[MembershipEvidence] = []
    for item in _sequence(value, "membership_sources"):
        source = _mapping(item, "membership source")
        _exact_keys(
            source,
            {"index", "url", "source_as_of", "acquisition_method", "member"},
            "membership source",
        )
        index = _string(source["index"], "membership index")
        url = _source_url(source["url"], "membership URL")
        source_as_of = _evidence_date(
            source["source_as_of"],
            "membership source_as_of",
            reviewed_at,
        )
        if expected.get(index) != url:
            raise UniverseError("stock membership provenance conflicts")
        if source["acquisition_method"] != _ACQUISITION_METHOD:
            raise UniverseError("stock membership was not manually reviewed")
        if source["member"] is not True:
            raise UniverseError("stock lacks affirmative index membership evidence")
        evidence.append(
            MembershipEvidence(
                index=index,
                url=url,
                source_as_of=source_as_of,
                acquisition_method=_ACQUISITION_METHOD,
            )
        )
    if tuple(item.index for item in evidence) != tuple(expected):
        raise UniverseError("stock requires S&P and Nasdaq-100 membership evidence")
    return tuple(evidence)


def _float_derivation(
    value: object,
    *,
    reviewed_at: date,
    free_float: int,
    primary_url: str,
    primary_as_of: date,
) -> FloatDerivation:
    table = _mapping(value, "float_source")
    required = {
        "source_as_of",
        "sources",
        "formula",
        "operands",
        "operand_source_dates",
        "derived_value",
        "stored_value",
        "rounding",
        "rounded_down",
    }
    optional = {
        "corroborating_shares_outstanding",
        "corroborating_shares_outstanding_as_of",
    }
    if not required <= table.keys() or not table.keys() <= required | optional:
        raise UniverseError("float derivation has missing or unknown fields")
    if bool(optional & table.keys()) and not optional <= table.keys():
        raise UniverseError("float corroboration metadata is incomplete")

    source_as_of = _evidence_date(
        table["source_as_of"],
        "float source_as_of",
        reviewed_at,
    )
    sources: list[SourceEvidence] = []
    for item in _sequence(table["sources"], "float sources"):
        source = _mapping(item, "float source evidence")
        _exact_keys(source, {"url", "source_as_of"}, "float source evidence")
        url = _source_url(source["url"], "float source URL")
        if not url.startswith("https://www.sec.gov/Archives/edgar/data/"):
            raise UniverseError("stock float source must be a primary SEC filing")
        evidence_as_of = _evidence_date(
            source["source_as_of"],
            "float evidence source_as_of",
            reviewed_at,
        )
        sources.append(SourceEvidence(url=url, source_as_of=evidence_as_of))
    if not sources or len({source.url for source in sources}) != len(sources):
        raise UniverseError("stock float sources are missing or duplicated")
    if (primary_url, primary_as_of) != (sources[0].url, sources[0].source_as_of):
        raise UniverseError("stock primary source conflicts with float derivation")
    if source_as_of != primary_as_of:
        raise UniverseError("stock float source dates conflict")

    formula = _string(table["formula"], "float formula")
    operands_raw = _mapping(table["operands"], "float operands")
    operands: dict[str, int | str]
    if formula == "shares_outstanding - directors_and_officers_shares":
        _exact_keys(
            operands_raw,
            {"shares_outstanding", "directors_and_officers_shares"},
            "float operands",
        )
        shares = _positive_integer(
            operands_raw["shares_outstanding"],
            "shares_outstanding",
        )
        insider = _positive_integer(
            operands_raw["directors_and_officers_shares"],
            "directors_and_officers_shares",
        )
        calculated = shares - insider
        operands = {
            "shares_outstanding": shares,
            "directors_and_officers_shares": insider,
        }
    elif formula == "floor(nonaffiliate_market_value / share_price)":
        _exact_keys(
            operands_raw,
            {"nonaffiliate_market_value", "share_price"},
            "float operands",
        )
        market_value = _positive_integer(
            operands_raw["nonaffiliate_market_value"],
            "nonaffiliate_market_value",
        )
        share_price = _positive_decimal_string(
            operands_raw["share_price"],
            "share_price",
        )
        with localcontext() as context:
            context.prec = max(50, len(str(market_value)) + 20)
            calculated = int(
                (Decimal(market_value) / share_price).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
        operands = {
            "nonaffiliate_market_value": market_value,
            "share_price": str(operands_raw["share_price"]),
        }
    else:
        raise UniverseError("stock float formula is not approved")

    operand_dates_raw = _mapping(
        table["operand_source_dates"],
        "float operand_source_dates",
    )
    if set(operand_dates_raw) != set(operands):
        raise UniverseError("stock float operand dates are incomplete or conflicting")
    operand_source_dates = {
        name: _evidence_date(
            operand_dates_raw[name],
            f"{name} source date",
            source_as_of,
        )
        for name in operands
    }

    derived_value = _positive_integer(table["derived_value"], "derived_value")
    stored_value = _positive_integer(table["stored_value"], "stored_value")
    if derived_value != calculated:
        raise UniverseError("stock float derivation arithmetic does not match")
    if stored_value != free_float or stored_value >= derived_value:
        raise UniverseError("stock float value is not conservatively rounded down")
    if table["rounding"] != "conservative_round_down" or table["rounded_down"] is not True:
        raise UniverseError("stock float lacks round-down metadata")

    corroborating_value: int | None = None
    corroborating_as_of: date | None = None
    if optional <= table.keys():
        corroborating_value = _positive_integer(
            table["corroborating_shares_outstanding"],
            "corroborating_shares_outstanding",
        )
        corroborating_as_of = _evidence_date(
            table["corroborating_shares_outstanding_as_of"],
            "corroborating_shares_outstanding_as_of",
            source_as_of,
        )
        if formula != "floor(nonaffiliate_market_value / share_price)":
            raise UniverseError("unexpected stock float corroboration metadata")
        if corroborating_as_of not in {
            source.source_as_of for source in sources
        }:
            raise UniverseError("stock float corroboration lacks matching source evidence")
    elif formula == "floor(nonaffiliate_market_value / share_price)":
        raise UniverseError("division float derivation lacks outstanding-share evidence")

    if corroborating_value is not None and (
        derived_value > corroborating_value
        or stored_value > corroborating_value
    ):
        raise UniverseError("stock float exceeds corroborating outstanding shares")

    return FloatDerivation(
        source_as_of=source_as_of,
        sources=tuple(sources),
        formula=formula,
        operands=MappingProxyType(operands),
        operand_source_dates=MappingProxyType(operand_source_dates),
        derived_value=derived_value,
        stored_value=stored_value,
        rounding="conservative_round_down",
        rounded_down=True,
        corroborating_shares_outstanding=corroborating_value,
        corroborating_shares_outstanding_as_of=corroborating_as_of,
    )


def _sponsor_sources(
    value: object,
    *,
    reviewed_at: date,
) -> tuple[SourceEvidence, ...]:
    sources: list[SourceEvidence] = []
    for item in _sequence(value, "sponsor_sources"):
        source = _mapping(item, "sponsor source")
        _exact_keys(source, {"url", "source_as_of"}, "sponsor source")
        url = _source_url(source["url"], "sponsor URL")
        if urlsplit(url).hostname not in _SPONSOR_HOSTS:
            raise UniverseError("ETF source is not an official sponsor host")
        source_as_of = _evidence_date(
            source["source_as_of"],
            "sponsor source_as_of",
            reviewed_at,
        )
        sources.append(SourceEvidence(url=url, source_as_of=source_as_of))
    pairs = {(source.url, source.source_as_of) for source in sources}
    if not sources or len(pairs) != len(sources):
        raise UniverseError("ETF sponsor evidence is missing or duplicated")
    return tuple(sources)


def _validate_support_roles(
    records: tuple[UniverseRecord, ...],
    by_symbol: Mapping[str, UniverseRecord],
    regime_support_symbols: tuple[str, ...],
    sector_mapping: Mapping[str, str],
) -> None:
    required_etfs = {"SPY", "QQQ", "VTI", "XLK"}
    if any(
        symbol not in by_symbol or by_symbol[symbol].product_type != "etf"
        for symbol in required_etfs
    ):
        raise UniverseError("required benchmark and support ETFs are missing")
    sector_targets = set(sector_mapping.values())
    if any(
        symbol not in by_symbol or by_symbol[symbol].product_type != "etf"
        for symbol in sector_targets
    ):
        raise UniverseError("sector benchmark target is not an ETF")
    expected_roles = {
        "market_benchmark": {"SPY"},
        "regime_benchmark": set(regime_support_symbols),
        "spy_benchmark": {"VTI"},
        "sector_benchmark": sector_targets,
    }
    for role, expected_symbols in expected_roles.items():
        actual_symbols = {
            record.symbol for record in records if role in record.support_roles
        }
        if actual_symbols != expected_symbols:
            raise UniverseError(f"{role} support distinction is missing or conflicting")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise UniverseError(f"universe JSON contains duplicate key {key}")
        result[key] = value
    return result


def _reject_json_float(value: str) -> None:
    raise UniverseError("universe JSON must not contain floating-point numbers")


def _reject_json_constant(value: str) -> None:
    raise UniverseError("universe JSON must not contain non-finite numbers")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) for key in value
    ):
        raise UniverseError(f"{name} must be an object")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise UniverseError(f"{name} must be an array")
    return value


def _exact_keys(
    value: Mapping[str, object],
    required: set[str],
    name: str,
) -> None:
    missing = required - value.keys()
    unknown = value.keys() - required
    if missing or unknown:
        raise UniverseError(f"{name} has missing or unknown fields")


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise UniverseError(f"{name} must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise UniverseError(f"{name} contains control characters")
    return value


def _symbol(value: object, name: str) -> str:
    symbol = _string(value, name)
    if _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise UniverseError(f"{name} is not a canonical symbol")
    return symbol


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    return tuple(_string(item, f"{name} item") for item in _sequence(value, name))


def _boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise UniverseError(f"{name} must be a boolean")
    return value


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise UniverseError(f"{name} must be a positive integer")
    return value


def _positive_decimal_string(value: object, name: str) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_PATTERN.fullmatch(value) is None:
        raise UniverseError(f"{name} must be a canonical Decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise UniverseError(f"{name} must be a Decimal string") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise UniverseError(f"{name} must be a positive finite Decimal string")
    return parsed


def _iso_date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise UniverseError(f"{name} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise UniverseError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise UniverseError(f"{name} must be a canonical ISO date")
    return parsed


def _evidence_date(value: object, name: str, reviewed_at: date) -> date:
    parsed = _iso_date(value, name)
    if parsed > reviewed_at:
        raise UniverseError(f"{name} is after the snapshot review")
    return parsed


def _source_url(value: object, name: str) -> str:
    url = _string(value, name)
    if any(character.isspace() for character in url):
        raise UniverseError(f"{name} contains whitespace")
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise UniverseError(f"{name} has an invalid port") from exc
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.hostname not in _ALLOWED_HOSTS
        or not parsed.path.startswith("/")
        or parsed.fragment
    ):
        raise UniverseError(f"{name} is not an approved HTTPS source URL")
    return url


__all__ = [
    "CURRENT_UNIVERSE_SHA256",
    "FloatDerivation",
    "MembershipEvidence",
    "SourceEvidence",
    "UniverseError",
    "UniverseRecord",
    "UniverseSnapshot",
    "canonical_payload_checksum",
    "is_verified_universe_snapshot",
    "load_current_universe",
]
