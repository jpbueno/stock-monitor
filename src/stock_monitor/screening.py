"""Pure eligibility, setup, scoring, and ranking decisions for stock monitoring."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields as dataclass_fields, is_dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from threading import RLock
from types import MappingProxyType
from typing import Protocol
from weakref import ReferenceType, ref
from zoneinfo import ZoneInfo

from .evidence import is_reviewed_evidence_decision
from .indicators import (
    BarLike,
    IndicatorError,
    average_dollar_volume,
    directional_volume_means,
    ema,
    five_session_return,
    max_relative_volume,
    median_share_volume,
    sma,
    twenty_session_return,
    wilder_atr,
)
from .market_calendar import (
    CalendarError,
    MarketCalendar,
    is_release_verified_market_calendar,
)
from .providers.reference import is_reviewed_instrument_status_decision
from .providers.alpaca import (
    NormalizedMarketFactSource,
    ProviderFetchManifest,
    _normalized_market_fact_source,
    is_issued_normalized_market_fact,
    normalized_market_facts_share_owner,
)
from .universe import UniverseSnapshot, is_verified_universe_snapshot


_ET = ZoneInfo("America/New_York")
_ZERO = Decimal("0")
_MINIMUM_PRICE = Decimal("10")
_MINIMUM_DOLLAR_VOLUME = Decimal("100000000")
_MINIMUM_SHARE_VOLUME = Decimal("1000000")
_MINIMUM_FREE_FLOAT = 50_000_000
_MAXIMUM_SPREAD = Decimal("0.0025")
_MAXIMUM_IEX_AGE_SECONDS = Decimal("300")
_MAXIMUM_EVIDENCE_AGE_SECONDS = Decimal("86400")
_APPROVED_PRODUCTS = frozenset({"common_stock", "etf"})
_APPROVED_VENUES = frozenset({"NASDAQ", "NYSE", "NYSE_ARCA"})
_SYMBOL = re.compile(r"[A-Z][A-Z0-9]{0,5}\Z")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
_SHA256_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _decimal_work_precision(*values: Decimal) -> int:
    finite = tuple(
        value
        for value in values
        if type(value) is Decimal and value.is_finite()
    )
    if not finite:
        return 50
    parts = tuple(value.as_tuple() for value in finite)
    exponents = tuple(int(part.exponent) for part in parts)
    coefficient_digits = sum(max(1, len(part.digits)) for part in parts)
    exponent_span = max(exponents) - min(exponents)
    magnitude = max(abs(value.adjusted()) for value in finite if value != _ZERO) \
        if any(value != _ZERO for value in finite) else 0
    return max(50, coefficient_digits + exponent_span + magnitude + 16)


class ScreeningError(ValueError):
    """Screening inputs do not form a complete deterministic decision."""


class InstrumentRecordLike(Protocol):
    symbol: str
    product_type: str
    listing_venue: str
    benchmark: str
    sector_etf: str | None
    enabled: bool
    leveraged: bool
    inverse: bool
    free_float: int | None
    tick_size: Decimal


class QuoteLike(Protocol):
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    feed: str


class InstrumentStatusLike(Protocol):
    """Structural seam for Task 4 halt decisions while that contract settles."""

    symbol: str
    halt_status: str
    block_reason: str | None
    as_of: datetime
    valid_until: datetime | None
    source_observation_ids: tuple[str, ...]


class CatalystFactLike(Protocol):
    symbol: str
    issuer_cik: str | None
    event_type: str
    published_at: datetime
    retrieved_at: datetime
    primary_url: str
    publisher: str
    fact: str
    accession: str | None
    source_observation_ids: tuple[str, ...]


class EvidenceDecisionLike(Protocol):
    """Raw, non-scored evidence facts emitted by Task 4 classification."""

    subject_kind: str
    symbol: str
    issuer_cik: str | None
    as_of: datetime
    qualifying_records: tuple[CatalystFactLike, ...]
    adverse_tags: tuple[str, ...]
    ambiguities: tuple[str, ...]
    conflicts: tuple[str, ...]
    binary_events: tuple[tuple[date, str | None], ...]
    etf_actions: tuple[tuple[date, str | None], ...]
    binary_event_coverage: str
    etf_action_coverage: str
    health: str
    retrieved_at: datetime | None
    source_observation_ids: tuple[str, ...]
    block_reason: str | None


class MarketSessionAttestationLike(Protocol):
    status: str
    as_of: datetime
    previous_session_date: date
    hold_sessions: tuple[date, ...]
    calendar_reviewed_at: date
    source_urls: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MarketSessionAttestation:
    """Calendar-owned proof of the previous and ten intended market sessions."""

    status: str
    as_of: datetime
    previous_session_date: date
    hold_sessions: tuple[date, ...]
    calendar_reviewed_at: date
    source_urls: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "hold_sessions", tuple(self.hold_sessions))
        object.__setattr__(self, "source_urls", tuple(self.source_urls))


def build_market_session_attestation(
    calendar: MarketCalendar,
    session_date: date,
    as_of: datetime,
) -> MarketSessionAttestation:
    """Derive the previous and inclusive ten-session window from one reviewed year."""
    if not isinstance(calendar, MarketCalendar):
        raise ScreeningError("market-session attestation requires a MarketCalendar")
    if not is_release_verified_market_calendar(calendar):
        raise ScreeningError("calendar release authority is unverified")
    if type(session_date) is not date:
        raise ScreeningError("publication session must be an exact date")
    current = _aware(as_of)
    if (
        current is None
        or current.astimezone(calendar.timezone).date() != session_date
        or calendar.year != session_date.year
        or getattr(calendar.timezone, "key", None) != "America/New_York"
    ):
        raise ScreeningError("calendar does not cover the publication session")
    if (
        type(calendar.retrieved_at) is not date
        or type(calendar.reviewed_at) is not date
        or calendar.retrieved_at != calendar.reviewed_at
        or calendar.reviewed_at > session_date
        or (session_date - calendar.reviewed_at).days > 31
        or type(calendar.open_session_count) is not int
        or calendar.open_session_count <= 0
    ):
        raise ScreeningError("calendar review metadata is not current")
    sources = tuple(calendar.sources)
    expected_sources = {
        "primary": ("NYSE", "https://www.nyse.com/trade/hours-calendars"),
        "cross_check": (
            "Nasdaq Trader",
            "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        ),
    }
    if len(sources) != 2 or {source.role for source in sources} != set(
        expected_sources
    ):
        raise ScreeningError("calendar source roles are incomplete")
    for source in sources:
        expected_name, expected_url = expected_sources[source.role]
        if (
            source.name != expected_name
            or source.url != expected_url
            or source.retrieved_at != calendar.retrieved_at
            or source.reviewed_at != calendar.reviewed_at
        ):
            raise ScreeningError("calendar source review metadata conflicts")
    closed_dates = tuple(calendar.closed_dates)
    early_closes = tuple(
        sorted(
            (day, session.close_time)
            for day, session in calendar.early_closes.items()
        )
    )
    if (
        closed_dates != tuple(sorted(closed_dates))
        or len(closed_dates) != len(set(closed_dates))
        or any(day.year != calendar.year for day in closed_dates)
        or any(calendar.is_open(day) for day in closed_dates)
        or any(
            tuple(source.closed_dates) != closed_dates
            or tuple(source.early_closes) != early_closes
            for source in sources
        )
    ):
        raise ScreeningError("calendar schedule conflicts with reviewed sources")
    first_day = date(calendar.year, 1, 1)
    last_day = date(calendar.year, 12, 31)
    expected_open_count = 0
    current_day = first_day
    while current_day <= last_day:
        if calendar.is_open(current_day):
            expected_open_count += 1
        current_day += timedelta(days=1)
    if expected_open_count != calendar.open_session_count:
        raise ScreeningError("calendar open-session count conflicts with schedule")
    try:
        if not calendar.is_open(session_date):
            raise ScreeningError("publication date is not a market session")
        previous = session_date - timedelta(days=1)
        while previous.year == calendar.year and not calendar.is_open(previous):
            previous -= timedelta(days=1)
        if previous.year != calendar.year or not calendar.is_open(previous):
            raise ScreeningError("previous market session is outside calendar coverage")
        sessions = tuple(
            calendar.add_sessions(session_date, offset) for offset in range(10)
        )
    except CalendarError as exc:
        raise ScreeningError("ten-session hold exceeds calendar coverage") from exc
    if (
        len(sessions) != 10
        or sessions[0] != session_date
        or any(not calendar.is_open(day) for day in sessions)
        or any(current_day <= prior for prior, current_day in zip(sessions, sessions[1:]))
    ):
        raise ScreeningError("calendar-derived hold sessions are inconsistent")
    return MarketSessionAttestation(
        status="VERIFIED",
        as_of=current,
        previous_session_date=previous,
        hold_sessions=sessions,
        calendar_reviewed_at=calendar.reviewed_at,
        source_urls=tuple(source.url for source in sources),
    )


@dataclass(frozen=True, slots=True)
class RelativeStrengthObservation:
    symbol: str
    five_session_value: Decimal
    twenty_session_value: Decimal
    context_fingerprint: str

    def __post_init__(self) -> None:
        if type(self.symbol) is not str or _SYMBOL.fullmatch(self.symbol) is None:
            raise TypeError("relative-strength observation symbol is malformed")
        for value in (self.five_session_value, self.twenty_session_value):
            if type(value) is not Decimal or not value.is_finite():
                raise TypeError("relative-strength observation must contain Decimals")
        if (
            type(self.context_fingerprint) is not str
            or _SHA256_HEX.fullmatch(self.context_fingerprint) is None
        ):
            raise TypeError("relative-strength observation fingerprint is malformed")


_COHORT_AUTHORITY = object()


@dataclass(frozen=True, slots=True, weakref_slot=True)
class RelativeStrengthCohort:
    five_session_values: tuple[Decimal, ...]
    twenty_session_values: tuple[Decimal, ...]
    observations: tuple[RelativeStrengthObservation, ...] = ()
    expected_symbols: tuple[str, ...] = ()
    universe_checksum: str | None = None
    universe_effective_date: date | None = None
    universe_reviewed_at: date | None = None
    universe_review_by: date | None = None
    _integrity: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _authority: object | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        _validate_cohort_fields(self)


def _validate_cohort_fields(cohort: RelativeStrengthCohort) -> None:
    if type(cohort.five_session_values) is not tuple or type(
        cohort.twenty_session_values
    ) is not tuple:
        raise TypeError("relative-strength values must be exact tuples")
    if len(cohort.five_session_values) != len(cohort.twenty_session_values):
        raise TypeError("relative-strength value series lengths do not match")
    for value in (*cohort.five_session_values, *cohort.twenty_session_values):
        if type(value) is not Decimal or not value.is_finite():
            raise TypeError("relative-strength values must be finite Decimals")
    if type(cohort.observations) is not tuple or any(
        type(value) is not RelativeStrengthObservation
        for value in cohort.observations
    ):
        raise TypeError("relative-strength observations must be an exact tuple")
    if cohort.observations and len(cohort.observations) != len(
        cohort.five_session_values
    ):
        raise TypeError("relative-strength observations and values do not align")
    if type(cohort.expected_symbols) is not tuple or any(
        type(symbol) is not str or _SYMBOL.fullmatch(symbol) is None
        for symbol in cohort.expected_symbols
    ):
        raise TypeError("relative-strength expected symbols must be canonical")
    if len(cohort.expected_symbols) != len(set(cohort.expected_symbols)):
        raise TypeError("relative-strength expected symbols are duplicated")
    observation_symbols = tuple(value.symbol for value in cohort.observations)
    if len(observation_symbols) != len(set(observation_symbols)) or any(
        symbol not in cohort.expected_symbols for symbol in observation_symbols
    ):
        raise TypeError("relative-strength observations are outside the universe")
    if cohort.observations and (
        tuple(value.five_session_value for value in cohort.observations)
        != cohort.five_session_values
        or tuple(value.twenty_session_value for value in cohort.observations)
        != cohort.twenty_session_values
    ):
        raise TypeError("relative-strength observation values do not align")
    provenance = (
        cohort.universe_checksum,
        cohort.universe_effective_date,
        cohort.universe_reviewed_at,
        cohort.universe_review_by,
    )
    present = tuple(value is not None for value in provenance)
    if any(present) and not all(present):
        raise TypeError("relative-strength universe provenance is incomplete")
    if all(present):
        if (
            type(cohort.universe_checksum) is not str
            or _SHA256_CHECKSUM.fullmatch(cohort.universe_checksum) is None
            or type(cohort.universe_effective_date) is not date
            or type(cohort.universe_reviewed_at) is not date
            or type(cohort.universe_review_by) is not date
            or cohort.universe_effective_date > cohort.universe_reviewed_at
            or cohort.universe_reviewed_at > cohort.universe_review_by
        ):
            raise TypeError("relative-strength universe provenance is malformed")
    if cohort.observations and not all(present):
        raise TypeError("relative-strength observations lack universe provenance")


_ISSUED_COHORTS: dict[
    int,
    tuple[ReferenceType[RelativeStrengthCohort], str],
] = {}
_ISSUED_COHORTS_LOCK = RLock()


@dataclass(frozen=True, slots=True)
class CandidateContext:
    """Immutable, pre-publication facts for one universe instrument."""

    record: InstrumentRecordLike
    bars_by_symbol: Mapping[str, Sequence[BarLike]]
    previous_session_quote: QuoteLike | None
    latest_iex_quote: QuoteLike | None
    instrument_status: InstrumentStatusLike | None
    evidence: EvidenceDecisionLike | None
    issuer_cik: str | None
    initial_listing_date: date | None
    listing_date_status: str
    session_date: date
    previous_session_date: date
    as_of: datetime
    hold_sessions: tuple[date, ...]
    session_attestation: MarketSessionAttestation
    market_calendar: MarketCalendar
    rumor_dependent: bool = False
    relative_strength_cohort: RelativeStrengthCohort | None = None
    operational_as_of: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.bars_by_symbol, Mapping):
            raise TypeError("candidate bars must be a symbol mapping")
        copied: dict[str, tuple[BarLike, ...]] = {}
        for raw_symbol, raw_bars in self.bars_by_symbol.items():
            if not isinstance(raw_symbol, str) or not raw_symbol:
                raise TypeError("candidate bar symbol is malformed")
            symbol = raw_symbol.upper()
            if symbol in copied:
                raise TypeError("candidate bar mapping contains duplicate symbols")
            if isinstance(raw_bars, (str, bytes)):
                raise TypeError("candidate bar history must be a sequence")
            copied[symbol] = tuple(raw_bars)
        object.__setattr__(self, "bars_by_symbol", MappingProxyType(copied))
        object.__setattr__(self, "hold_sessions", tuple(self.hold_sessions))


@dataclass(frozen=True, slots=True)
class EligibilityDecision:
    eligible: bool
    base_eligible: bool
    live_eligible: bool
    paper_only: bool
    data_available: bool
    status: str
    reason_codes: tuple[str, ...]
    average_dollar_volume: Decimal | None
    median_share_volume: Decimal | None
    spread_percent: Decimal | None


@dataclass(frozen=True, slots=True)
class SetupDecision:
    eligible: bool
    status: str
    setup_type: str | None
    qualifying_setups: tuple[str, ...]
    reason_codes: tuple[str, ...]
    atr14: Decimal | None
    resistance: Decimal | None
    raw_trigger: Decimal | None
    raw_stop: Decimal | None
    trigger_price: Decimal | None
    stop_price: Decimal | None
    planned_entry: Decimal | None
    maximum_permitted_entry: Decimal | None
    target_price: Decimal | None
    stop_distance: Decimal | None


@dataclass(frozen=True, slots=True)
class CohortDecision:
    status: str
    contexts: tuple[CandidateContext, ...]
    eligibility: tuple[EligibilityDecision, ...]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ScoreCard:
    status: str
    reason_codes: tuple[str, ...]
    trend_and_regime: int
    relative_strength: int
    setup_quality: int
    volume_confirmation: int
    catalyst_context: int
    liquidity_execution: int
    total: int
    publishable: bool
    five_session_relative_strength: Decimal | None
    twenty_session_relative_strength: Decimal | None
    five_session_relative_strength_percentile: Decimal | None
    twenty_session_relative_strength_percentile: Decimal | None

    def __post_init__(self) -> None:
        caps = (
            (self.trend_and_regime, 25),
            (self.relative_strength, 20),
            (self.setup_quality, 20),
            (self.volume_confirmation, 15),
            (self.catalyst_context, 10),
            (self.liquidity_execution, 10),
        )
        if any(type(value) is not int or not 0 <= value <= cap for value, cap in caps):
            raise ScreeningError("score category is outside its locked cap")
        if self.total != sum(value for value, _ in caps) or not 0 <= self.total <= 100:
            raise ScreeningError("score total does not equal its category arithmetic")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ScoredCandidate:
    """Portfolio-agnostic scored setup and its exact Task 6 price contract."""

    symbol: str
    total_score: int
    relative_strength_percentile: Decimal
    average_dollar_volume: Decimal
    publication_session: date | None = None
    raw_trigger: Decimal | None = None
    raw_stop: Decimal | None = None
    delayed_spread_amount: Decimal | None = None
    tick_size: Decimal | None = None
    trigger_price: Decimal | None = None
    maximum_permitted_entry: Decimal | None = None
    recommended_stop: Decimal | None = None
    target_price: Decimal | None = None
    score_card: ScoreCard | None = None
    setup: SetupDecision | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise ScreeningError("scored-candidate symbol must be canonical uppercase")
        if type(self.total_score) is not int or not 0 <= self.total_score <= 100:
            raise ScreeningError("scored-candidate total must be from zero through 100")
        for name in ("relative_strength_percentile", "average_dollar_volume"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ScreeningError(f"scored-candidate {name} must be a finite Decimal")
        if not Decimal("0") <= self.relative_strength_percentile <= Decimal("100"):
            raise ScreeningError("scored-candidate percentile is outside zero through 100")
        if self.average_dollar_volume <= _ZERO:
            raise ScreeningError("scored-candidate dollar volume must be positive")
        price_names = (
            "raw_trigger",
            "raw_stop",
            "delayed_spread_amount",
            "tick_size",
            "trigger_price",
            "maximum_permitted_entry",
            "recommended_stop",
            "target_price",
        )
        price_values = tuple(getattr(self, name) for name in price_names)
        present = tuple(value is not None for value in price_values)
        if any(present) and not all(present):
            raise ScreeningError("scored-candidate price contract is incomplete")
        if all(present):
            if type(self.publication_session) is not date:
                raise ScreeningError("scored-candidate publication session is missing")
            typed = {
                name: value
                for name, value in zip(price_names, price_values)
                if isinstance(value, Decimal)
            }
            if len(typed) != len(price_names) or any(
                not value.is_finite() for value in typed.values()
            ):
                raise ScreeningError("scored-candidate prices must be finite Decimals")
            if any(
                typed[name] <= _ZERO
                for name in price_names
                if name != "delayed_spread_amount"
            ) or typed["delayed_spread_amount"] < _ZERO:
                raise ScreeningError("scored-candidate prices must be positive")
            tick = typed["tick_size"]
            with localcontext() as decimal_context:
                decimal_context.prec = _decimal_work_precision(*typed.values())
                expected_trigger = _round_up(typed["raw_trigger"], tick)
                expected_stop = _round_down(typed["raw_stop"], tick)
                slippage_allowance = max(
                    Decimal("0.001") * expected_trigger,
                    typed["delayed_spread_amount"] / Decimal("2"),
                )
                expected_entry = _round_up(
                    expected_trigger + slippage_allowance,
                    tick,
                )
                for name in (
                    "trigger_price",
                    "maximum_permitted_entry",
                    "recommended_stop",
                    "target_price",
                ):
                    units = typed[name] / tick
                    if units != units.to_integral_value():
                        raise ScreeningError(
                            "scored-candidate rounded price violates tick size"
                        )
                if (
                    typed["trigger_price"] != expected_trigger
                    or typed["recommended_stop"] != expected_stop
                    or typed["maximum_permitted_entry"] != expected_entry
                ):
                    raise ScreeningError(
                        "scored-candidate price contract violates locked formula"
                    )
                if (
                    typed["raw_stop"] >= typed["raw_trigger"]
                    or typed["recommended_stop"] >= typed["maximum_permitted_entry"]
                    or typed["target_price"] <= typed["maximum_permitted_entry"]
                ):
                    raise ScreeningError("scored-candidate price ordering is invalid")
                expected_stop_distance = (
                    typed["maximum_permitted_entry"] - typed["recommended_stop"]
                )
                minimum_target = (
                    typed["maximum_permitted_entry"]
                    + Decimal("2") * expected_stop_distance
                )
                expected_target = _round_up(minimum_target, tick)
                if typed["target_price"] != expected_target:
                    raise ScreeningError(
                        "scored-candidate target violates locked two-R formula"
                    )
            if self.score_card is None or self.setup is None:
                raise ScreeningError("scored-candidate audit decisions are missing")
            if (
                self.score_card.publishable is not True
                or self.score_card.status != "PUBLISHABLE"
                or self.total_score < 80
                or self.score_card.total != self.total_score
                or self.score_card.twenty_session_relative_strength_percentile
                != self.relative_strength_percentile
            ):
                raise ScreeningError("scored-candidate score card is inconsistent")
            if (
                self.setup.eligible is not True
                or self.setup.status != "ELIGIBLE"
                or self.setup.raw_trigger != typed["raw_trigger"]
                or self.setup.raw_stop != typed["raw_stop"]
                or self.setup.trigger_price != typed["trigger_price"]
                or self.setup.planned_entry != typed["maximum_permitted_entry"]
                or self.setup.maximum_permitted_entry
                != typed["maximum_permitted_entry"]
                or self.setup.stop_price != typed["recommended_stop"]
                or self.setup.target_price != typed["target_price"]
                or self.setup.stop_distance != expected_stop_distance
            ):
                raise ScreeningError("scored-candidate setup decision is inconsistent")


@dataclass(frozen=True, slots=True)
class PublicationObservationManifest:
    """Exact Task 5 source material retained by one issued publication."""

    candidate_source_observation_ids: tuple[tuple[str, tuple[str, ...]], ...]
    candidate_context_digests: tuple[tuple[str, str], ...]
    candidate_subjects: tuple[tuple[str, str, str | None], ...]
    normalized_market_fact_sources: tuple[
        tuple[str, NormalizedMarketFactSource], ...
    ]
    provider_fetch_manifests: tuple[ProviderFetchManifest, ...]
    source_observation_ids: tuple[str, ...]
    manifest_digest: str

    def __post_init__(self) -> None:
        candidate_sources = tuple(self.candidate_source_observation_ids)
        candidate_digests = tuple(self.candidate_context_digests)
        candidate_subjects = tuple(self.candidate_subjects)
        normalized_sources = tuple(self.normalized_market_fact_sources)
        fetch_manifests = tuple(self.provider_fetch_manifests)
        source_ids = tuple(self.source_observation_ids)
        object.__setattr__(
            self,
            "candidate_source_observation_ids",
            candidate_sources,
        )
        object.__setattr__(self, "candidate_context_digests", candidate_digests)
        object.__setattr__(self, "candidate_subjects", candidate_subjects)
        object.__setattr__(
            self,
            "normalized_market_fact_sources",
            normalized_sources,
        )
        object.__setattr__(
            self,
            "provider_fetch_manifests",
            fetch_manifests,
        )
        object.__setattr__(self, "source_observation_ids", source_ids)
        symbols = tuple(symbol for symbol, _values in candidate_sources)
        if (
            not candidate_sources
            or symbols != tuple(symbol for symbol, _digest in candidate_digests)
            or symbols
            != tuple(symbol for symbol, _kind, _issuer in candidate_subjects)
            or len(symbols) != len(set(symbols))
            or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
            or any(
                type(values) is not tuple
                or not values
                or values != tuple(sorted(set(values)))
                or any(type(value) is not str or not value for value in values)
                for _symbol, values in candidate_sources
            )
            or any(_SHA256_HEX.fullmatch(digest) is None for _symbol, digest in candidate_digests)
            or any(
                subject_kind not in {"STOCK", "ETF"}
                or (
                    subject_kind == "STOCK"
                    and (
                        type(issuer_cik) is not str
                        or not issuer_cik.isdigit()
                        or len(issuer_cik) != 10
                    )
                )
                or (subject_kind == "ETF" and issuer_cik is not None)
                for _symbol, subject_kind, issuer_cik in candidate_subjects
            )
            or not normalized_sources
            or any(
                candidate_symbol not in symbols
                or not isinstance(source, NormalizedMarketFactSource)
                for candidate_symbol, source in normalized_sources
            )
            or not fetch_manifests
            or any(
                not isinstance(manifest, ProviderFetchManifest)
                for manifest in fetch_manifests
            )
            or len(
                {manifest.manifest_digest for manifest in fetch_manifests}
            )
            != len(fetch_manifests)
            or {
                source.fetch_manifest.manifest_digest
                for _candidate_symbol, source in normalized_sources
            }
            != {manifest.manifest_digest for manifest in fetch_manifests}
            or not source_ids
            or source_ids != tuple(sorted(set(source_ids)))
            or _SHA256_HEX.fullmatch(self.manifest_digest) is None
        ):
            raise ScreeningError("publication observation manifest is malformed")
        expected_ids = tuple(
            sorted(
                {
                    source_id
                    for _symbol, values in candidate_sources
                    for source_id in values
                }
            )
        )
        if expected_ids != source_ids:
            raise ScreeningError("publication observation manifest is incomplete")


@dataclass(frozen=True, slots=True)
class _IssuedScoredCandidateAuthority:
    reference: ReferenceType[ScoredCandidate]
    candidate_digest: str
    context: CandidateContext
    context_digest: str
    source_observation_ids: tuple[str, ...]
    normalized_market_fact_sources: tuple[NormalizedMarketFactSource, ...]


_ISSUED_SCORED_CANDIDATES: dict[int, _IssuedScoredCandidateAuthority] = {}
_ISSUED_SCORED_CANDIDATES_LOCK = RLock()


def _scored_candidate_fingerprint(candidate: ScoredCandidate) -> str:
    document = _canonical_snapshot_value(candidate)
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _candidate_source_observation_ids(
    context: CandidateContext,
) -> tuple[str, ...]:
    """Return every exact raw-page identity used to score one context."""
    if type(context) is not CandidateContext:
        raise ScreeningError("candidate context must be the exact DTO")
    source_ids: set[str] = set()

    def remember(source_id: object) -> None:
        if type(source_id) is not str or not source_id:
            raise ScreeningError("candidate source observation ID is malformed")
        source_ids.add(source_id)

    for symbol in sorted(context.bars_by_symbol):
        for bar in context.bars_by_symbol[symbol]:
            remember(getattr(bar, "source_observation_id", None))
    for quote in (
        context.previous_session_quote,
        context.latest_iex_quote,
    ):
        if quote is None:
            continue
        remember(getattr(quote, "source_observation_id", None))
    if context.instrument_status is not None:
        for source_id in context.instrument_status.source_observation_ids:
            remember(source_id)
    if context.evidence is not None:
        for record in context.evidence.qualifying_records:
            for source_id in record.source_observation_ids:
                remember(source_id)
        for source_id in context.evidence.source_observation_ids:
            remember(source_id)
    if not source_ids:
        raise ScreeningError("candidate source observation manifest is empty")
    return tuple(sorted(source_ids))


def _candidate_market_facts(context: CandidateContext) -> tuple[object, ...]:
    facts: list[object] = []
    for symbol in sorted(context.bars_by_symbol):
        for bar in context.bars_by_symbol[symbol]:
            if getattr(bar, "symbol", None) != symbol:
                raise ScreeningError(
                    "candidate market fact symbol conflicts with its cohort"
                )
            facts.append(bar)
    for quote in (
        context.previous_session_quote,
        context.latest_iex_quote,
    ):
        if quote is not None:
            if getattr(quote, "symbol", None) != str(context.record.symbol).upper():
                raise ScreeningError(
                    "candidate quote symbol conflicts with its instrument"
                )
            facts.append(quote)
    if not facts or len({id(fact) for fact in facts}) != len(facts):
        raise ScreeningError("candidate normalized market facts are incomplete")
    return tuple(facts)


def _candidate_normalized_market_fact_sources(
    context: CandidateContext,
) -> tuple[NormalizedMarketFactSource, ...]:
    facts = _candidate_market_facts(context)
    first = facts[0]
    if not is_issued_normalized_market_fact(first):
        raise ScreeningError(
            "candidate market facts require provider normalization authority"
        )
    sources: list[NormalizedMarketFactSource] = []
    for fact in facts:
        if (
            not is_issued_normalized_market_fact(fact)
            or not normalized_market_facts_share_owner(first, fact)
        ):
            raise ScreeningError(
                "candidate market facts require one provider authority owner"
            )
        source = _normalized_market_fact_source(fact)
        if (
            source.symbol != getattr(fact, "symbol", None)
            or source.feed != getattr(fact, "feed", None)
            or source.source_observation_id
            != getattr(fact, "source_observation_id", None)
            or not source.fetch_manifest.terminal
        ):
            raise ScreeningError(
                "candidate normalized market fact source is inconsistent"
            )
        sources.append(source)
    return tuple(sources)


def _issued_scored_candidate_authority(
    candidate: object,
) -> _IssuedScoredCandidateAuthority | None:
    if not isinstance(candidate, ScoredCandidate):
        return None
    try:
        candidate_digest = _scored_candidate_fingerprint(candidate)
    except Exception:
        return None
    with _ISSUED_SCORED_CANDIDATES_LOCK:
        issued = _ISSUED_SCORED_CANDIDATES.get(id(candidate))
        if (
            issued is None
            or issued.reference() is not candidate
            or issued.candidate_digest != candidate_digest
        ):
            return None
    context = issued.context
    try:
        if (
            issued.context_digest != _relative_input_fingerprint(context)
            or issued.source_observation_ids
            != _candidate_source_observation_ids(context)
            or (
                bool(issued.normalized_market_fact_sources)
                and issued.normalized_market_fact_sources
                != _candidate_normalized_market_fact_sources(context)
            )
            or context.evidence is None
            or not is_reviewed_evidence_decision(context.evidence)
            or context.instrument_status is None
            or not is_reviewed_instrument_status_decision(
                context.instrument_status
            )
            or not is_release_verified_market_calendar(context.market_calendar)
            or context.relative_strength_cohort is None
            or not _trusted_relative_strength_cohort(
                context,
                context.relative_strength_cohort,
            )
        ):
            return None
    except Exception:
        return None
    return issued


def is_issued_scored_candidate(candidate: object) -> bool:
    return _issued_scored_candidate_authority(candidate) is not None


def _provider_fetch_manifest_document(
    manifest: ProviderFetchManifest,
) -> dict[str, object]:
    return {
        "collection": manifest.collection,
        "requested_symbols": list(manifest.requested_symbols),
        "request_digest": manifest.request_digest,
        "terminal": manifest.terminal,
        "manifest_digest": manifest.manifest_digest,
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
    }


def _normalized_market_fact_source_document(
    candidate_symbol: str,
    source: NormalizedMarketFactSource,
) -> dict[str, object]:
    return {
        "candidate_symbol": candidate_symbol,
        "kind": source.kind,
        "symbol": source.symbol,
        "feed": source.feed,
        "source_observation_id": source.source_observation_id,
        "page_ordinal": source.page_ordinal,
        "source_item_ordinal": source.source_item_ordinal,
        "source_item_path": source.source_item_path,
        "page_payload_sha256": source.page_payload_sha256,
        "normalized_fields_digest": source.normalized_fields_digest,
        "fetch_manifest_digest": source.fetch_manifest.manifest_digest,
    }


def _build_publication_observation_manifest(
    candidates: Sequence[ScoredCandidate],
) -> PublicationObservationManifest:
    values = tuple(candidates)
    if not values:
        raise ScreeningError("publication source candidate cohort is empty")
    candidate_sources: list[tuple[str, tuple[str, ...]]] = []
    candidate_digests: list[tuple[str, str]] = []
    candidate_subjects: list[tuple[str, str, str | None]] = []
    normalized_sources: list[tuple[str, NormalizedMarketFactSource]] = []
    fetch_manifests_by_digest: dict[str, ProviderFetchManifest] = {}
    source_ids: set[str] = set()
    owner_anchor: object | None = None
    for candidate in values:
        issued = _issued_scored_candidate_authority(candidate)
        if issued is None:
            raise ScreeningError(
                "publication source requires identity-issued candidates"
            )
        current_normalized_sources = (
            _candidate_normalized_market_fact_sources(issued.context)
        )
        if (
            not issued.normalized_market_fact_sources
            or issued.normalized_market_fact_sources
            != current_normalized_sources
        ):
            raise ScreeningError(
                "publication source requires provider-issued normalized facts"
            )
        candidate_facts = _candidate_market_facts(issued.context)
        if owner_anchor is None:
            owner_anchor = candidate_facts[0]
        if any(
            not normalized_market_facts_share_owner(owner_anchor, fact)
            for fact in candidate_facts
        ):
            raise ScreeningError(
                "publication source candidate facts cross provider owners"
            )
        candidate_sources.append(
            (candidate.symbol, issued.source_observation_ids)
        )
        candidate_digests.append((candidate.symbol, issued.context_digest))
        evidence = issued.context.evidence
        if (
            evidence is None
            or evidence.symbol != candidate.symbol
            or evidence.subject_kind not in {"STOCK", "ETF"}
            or evidence.issuer_cik != issued.context.issuer_cik
            or (
                evidence.subject_kind == "STOCK"
                and evidence.issuer_cik is None
            )
            or (
                evidence.subject_kind == "ETF"
                and evidence.issuer_cik is not None
            )
        ):
            raise ScreeningError(
                "publication source candidate subject is unverified"
            )
        candidate_subjects.append(
            (
                candidate.symbol,
                evidence.subject_kind,
                evidence.issuer_cik,
            )
        )
        for normalized_source in current_normalized_sources:
            normalized_sources.append((candidate.symbol, normalized_source))
            fetch_manifest = normalized_source.fetch_manifest
            prior = fetch_manifests_by_digest.setdefault(
                fetch_manifest.manifest_digest,
                fetch_manifest,
            )
            if prior != fetch_manifest:
                raise ScreeningError(
                    "publication source fetch manifest digest conflicts"
                )
        source_ids.update(issued.source_observation_ids)
    ordered_source_ids = tuple(sorted(source_ids))
    fetch_manifests = tuple(fetch_manifests_by_digest.values())
    manifest_document = {
        "candidate_context_digests": candidate_digests,
        "candidate_source_observation_ids": candidate_sources,
        "candidate_subjects": candidate_subjects,
        "normalized_market_fact_sources": [
            _normalized_market_fact_source_document(symbol, source)
            for symbol, source in normalized_sources
        ],
        "provider_fetch_manifests": [
            _provider_fetch_manifest_document(manifest)
            for manifest in fetch_manifests
        ],
        "source_observation_ids": ordered_source_ids,
        "version": 3,
    }
    manifest_digest = hashlib.sha256(
        json.dumps(
            manifest_document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return PublicationObservationManifest(
        candidate_source_observation_ids=tuple(candidate_sources),
        candidate_context_digests=tuple(candidate_digests),
        candidate_subjects=tuple(candidate_subjects),
        normalized_market_fact_sources=tuple(normalized_sources),
        provider_fetch_manifests=fetch_manifests,
        source_observation_ids=ordered_source_ids,
        manifest_digest=manifest_digest,
    )


@dataclass(frozen=True, slots=True)
class PublicationCandidate:
    rank: int
    role: str
    candidate: ScoredCandidate

    @property
    def symbol(self) -> str:
        return self.candidate.symbol


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PublicationDecision:
    status: str
    candidates: tuple[PublicationCandidate, ...]
    primary: PublicationCandidate | None

    def __post_init__(self) -> None:
        candidates = tuple(self.candidates)
        if any(not isinstance(item, PublicationCandidate) for item in candidates):
            raise ScreeningError("publication candidates are malformed")
        object.__setattr__(self, "candidates", candidates)
        if self.status not in {"READY", "NO_PRIMARY_CAPACITY", "NO_TRADE"}:
            raise ScreeningError("publication status is malformed")
        expected_primary = next(
            (item for item in candidates if item.role == "PRIMARY"),
            None,
        )
        if self.primary != expected_primary:
            raise ScreeningError("publication primary conflicts with roles")


_ISSUED_PUBLICATION_DECISIONS: dict[
    int,
    tuple[
        ReferenceType[PublicationDecision],
        str,
        ReferenceType[object],
        PublicationObservationManifest,
    ],
] = {}
_ISSUED_PUBLICATION_DECISIONS_LOCK = RLock()


def _publication_decision_fingerprint(decision: PublicationDecision) -> str:
    document = _canonical_snapshot_value(decision)
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_issued_publication_decision(decision: object) -> bool:
    if not isinstance(decision, PublicationDecision):
        return False
    try:
        fingerprint = _publication_decision_fingerprint(decision)
    except Exception:
        return False
    with _ISSUED_PUBLICATION_DECISIONS_LOCK:
        issued = _ISSUED_PUBLICATION_DECISIONS.get(id(decision))
        if (
            issued is not None
            and issued[0]() is decision
            and issued[1] == fingerprint
        ):
            primary_plan_decision = issued[2]()
            source_manifest = issued[3]
        else:
            return False
    if primary_plan_decision is None:
        return False
    from .risk import is_issued_long_plan_decision

    try:
        current_manifest = _build_publication_observation_manifest(
            tuple(item.candidate for item in decision.candidates)
        )
    except Exception:
        return False
    return (
        is_issued_long_plan_decision(primary_plan_decision)
        and source_manifest == current_manifest
    )


def is_issued_publication_decision_for_plan(
    decision: object,
    primary_plan_decision: object,
) -> bool:
    """Require the exact portfolio-bound plan used to issue *decision*."""
    if not is_issued_publication_decision(decision):
        return False
    with _ISSUED_PUBLICATION_DECISIONS_LOCK:
        issued = _ISSUED_PUBLICATION_DECISIONS.get(id(decision))
        return issued is not None and issued[2]() is primary_plan_decision


def _publication_observation_manifest(
    decision: PublicationDecision,
) -> PublicationObservationManifest:
    """Reveal source pins only for the exact current issued decision identity."""
    if not is_issued_publication_decision(decision):
        raise ScreeningError("publication decision authority is unverified")
    with _ISSUED_PUBLICATION_DECISIONS_LOCK:
        issued = _ISSUED_PUBLICATION_DECISIONS.get(id(decision))
        if issued is None or issued[0]() is not decision:
            raise ScreeningError("publication decision authority is unverified")
        return issued[3]


def _issue_portfolio_bound_publication_decision(
    candidates: Sequence[ScoredCandidate],
    *,
    primary_plan_decision: object,
) -> PublicationDecision:
    """Issue one complete up-to-three cohort from ranking and capacity authority.

    This is deliberately not a generic registrar: it recomputes ranking and
    roles, requires every Task 5 candidate identity, and binds rank one to the
    exact Task 6 production plan and its canonical portfolio authority.
    """
    if isinstance(candidates, (str, bytes)):
        raise TypeError("publication candidates must be a sequence")
    values = tuple(candidates)
    if not 1 <= len(values) <= 3:
        raise ScreeningError(
            "portfolio-bound publication requires from one through three candidates"
        )
    if any(not is_issued_scored_candidate(candidate) for candidate in values):
        raise ScreeningError(
            "portfolio-bound publication requires identity-issued candidates"
        )
    ranked = rank_candidates(values)
    if len(ranked) != len(values) or {id(candidate) for candidate in ranked} != {
        id(candidate) for candidate in values
    }:
        raise ScreeningError("publication candidate cohort is incomplete")

    from .risk import (
        LongPlanDecision,
        is_issued_long_plan_decision,
        is_issued_portfolio_risk_authority,
    )

    if not isinstance(primary_plan_decision, LongPlanDecision) or not (
        is_issued_long_plan_decision(primary_plan_decision)
    ):
        raise ScreeningError("primary plan authority is unverified")
    authority = primary_plan_decision.portfolio_authority
    if (
        not primary_plan_decision.eligible
        or primary_plan_decision.plan is None
        or primary_plan_decision.request is None
        or primary_plan_decision.authority_scope != "CANONICAL_PUBLICATION"
        or not is_issued_portfolio_risk_authority(authority)
        or authority is not primary_plan_decision.portfolio_authority
        or authority.request is not primary_plan_decision.request
        or authority.portfolio_state
        is not primary_plan_decision.portfolio_authority.portfolio_state
    ):
        raise ScreeningError("primary plan authority is unverified")
    primary = ranked[0]
    request = primary_plan_decision.request
    if (
        primary.publication_session is None
        or primary.maximum_permitted_entry is None
        or primary.recommended_stop is None
        or primary.target_price is None
        or primary.tick_size is None
        or request.symbol != primary.symbol
        or request.session_date != primary.publication_session
        or request.entry != primary.maximum_permitted_entry
        or request.stop != primary.recommended_stop
        or request.tick_size != primary.tick_size
        or request.published_target != primary.target_price
        or primary_plan_decision.target != primary.target_price
    ):
        raise ScreeningError("rank-one candidate and primary plan do not match")

    decision = select_publication_roles(ranked, capacity_available=True)
    if (
        decision.status != "READY"
        or decision.primary is None
        or decision.primary.candidate is not primary
        or tuple(item.rank for item in decision.candidates)
        != tuple(range(1, len(ranked) + 1))
        or tuple(item.role for item in decision.candidates)
        != ("PRIMARY", *("WATCHLIST_SHADOW",) * (len(ranked) - 1))
    ):
        raise ScreeningError("publication role derivation is inconsistent")
    source_manifest = _build_publication_observation_manifest(ranked)
    identity = id(decision)

    def discard(dead: ReferenceType[PublicationDecision]) -> None:
        with _ISSUED_PUBLICATION_DECISIONS_LOCK:
            current = _ISSUED_PUBLICATION_DECISIONS.get(identity)
            if current is not None and current[0] is dead:
                _ISSUED_PUBLICATION_DECISIONS.pop(identity, None)

    with _ISSUED_PUBLICATION_DECISIONS_LOCK:
        _ISSUED_PUBLICATION_DECISIONS[identity] = (
            ref(decision, discard),
            _publication_decision_fingerprint(decision),
            ref(primary_plan_decision),
            source_manifest,
        )
    return decision


def _append(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _aware(value: object) -> datetime | None:
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    try:
        if value.utcoffset() is None:
            return None
    except (OverflowError, ValueError):
        return None
    return value


def _operational_as_of(context: CandidateContext) -> datetime | None:
    economic = _aware(context.as_of)
    value = context.operational_as_of
    operational = economic if value is None else _aware(value)
    if economic is None or operational is None:
        return None
    try:
        if operational.astimezone(UTC) < economic.astimezone(UTC):
            return None
    except (OverflowError, ValueError):
        return None
    return operational


def _elapsed_seconds(later: datetime, earlier: datetime) -> Decimal:
    delta = later.astimezone(UTC) - earlier.astimezone(UTC)
    with localcontext() as context:
        context.prec = 50
        return (
            Decimal(delta.days) * Decimal("86400")
            + Decimal(delta.seconds)
            + Decimal(delta.microseconds) / Decimal("1000000")
        )


def _bar_session_date(bar: BarLike) -> date | None:
    value = _aware(getattr(bar, "timestamp", None))
    return value.astimezone(UTC).date() if value is not None else None


def _required_symbols(context: CandidateContext) -> tuple[str, ...]:
    record = context.record
    values = [getattr(record, "symbol", ""), getattr(record, "benchmark", "")]
    if getattr(record, "product_type", None) == "common_stock":
        values.append(getattr(record, "sector_etf", "") or "")
    values.extend(("SPY", "QQQ"))
    return tuple(dict.fromkeys(str(value).upper() for value in values if value))


def _completed_calendar_sessions(
    calendar: MarketCalendar,
    end: date,
    count: int,
) -> tuple[date, ...]:
    if not isinstance(calendar, MarketCalendar) or not calendar.is_open(end):
        raise ScreeningError("completed-session calendar coverage is unavailable")
    descending = [end]
    current = end
    while len(descending) < count:
        current -= timedelta(days=1)
        if current.year != calendar.year:
            raise ScreeningError("completed-session history crosses calendar coverage")
        if calendar.is_open(current):
            descending.append(current)
    return tuple(reversed(descending))


def _validate_bar_cohort(
    context: CandidateContext,
    data_reasons: list[str],
) -> date | None:
    canonical_dates: tuple[date, ...] | None = None
    latest: date | None = None
    selected_latest = context.previous_session_date
    expected_dates: tuple[date, ...] | None = None
    if type(selected_latest) is not date or selected_latest >= context.session_date:
        _append(data_reasons, "LATEST_COMPLETED_SESSION_UNVERIFIED")
    else:
        try:
            expected_dates = _completed_calendar_sessions(
                context.market_calendar,
                selected_latest,
                60,
            )
        except (CalendarError, ScreeningError):
            _append(data_reasons, "BAR_SESSION_CALENDAR_UNAVAILABLE")
    for symbol in _required_symbols(context):
        bars = context.bars_by_symbol.get(symbol)
        if bars is None:
            _append(data_reasons, "BAR_SYMBOL_MISSING")
            continue
        if len(bars) != 60:
            _append(data_reasons, "BAR_HISTORY_INCOMPLETE")
            continue
        if any(getattr(bar, "symbol", "") != symbol for bar in bars):
            _append(data_reasons, "BAR_SYMBOL_MISMATCH")
        if any(getattr(bar, "adjustment", None) != "split" for bar in bars):
            _append(data_reasons, "BARS_NOT_SPLIT_ADJUSTED")
        if any(str(getattr(bar, "feed", "")).upper() != "SIP" for bar in bars):
            _append(data_reasons, "BARS_NOT_SIP")
        dates = tuple(_bar_session_date(bar) for bar in bars)
        if any(value is None for value in dates):
            _append(data_reasons, "BAR_TIMESTAMP_UNVERIFIED")
            continue
        checked_dates = tuple(value for value in dates if value is not None)
        if expected_dates is not None and checked_dates != expected_dates:
            _append(data_reasons, "BAR_SESSION_CALENDAR_MISMATCH")
        if any(
            current <= previous
            for previous, current in zip(checked_dates, checked_dates[1:])
        ):
            _append(data_reasons, "BAR_SESSION_ORDER_INVALID")
        if checked_dates[-1] >= context.session_date:
            _append(data_reasons, "BAR_SESSION_NOT_COMPLETED")
        if canonical_dates is None:
            canonical_dates = checked_dates
            latest = checked_dates[-1]
        elif checked_dates != canonical_dates:
            _append(data_reasons, "BAR_SERIES_MISALIGNED")
        try:
            average_dollar_volume(bars, 20)
        except IndicatorError:
            _append(data_reasons, "BAR_VALUES_INVALID")
    if latest is not None and latest != selected_latest:
        _append(data_reasons, "BAR_HISTORY_NOT_LATEST_COMPLETED_SESSION")
    return latest


def _quote_spread(
    context: CandidateContext,
    latest_completed: date | None,
    data_reasons: list[str],
) -> Decimal | None:
    quote = context.previous_session_quote
    if quote is None:
        _append(data_reasons, "PREVIOUS_SESSION_QUOTE_MISSING")
        return None
    symbol = getattr(context.record, "symbol", "").upper()
    timestamp = _aware(getattr(quote, "timestamp", None))
    bid = getattr(quote, "bid", None)
    ask = getattr(quote, "ask", None)
    if getattr(quote, "symbol", "").upper() != symbol:
        _append(data_reasons, "PREVIOUS_SESSION_QUOTE_SYMBOL_MISMATCH")
    if str(getattr(quote, "feed", "")).upper() != "SIP":
        _append(data_reasons, "PREVIOUS_SESSION_QUOTE_NOT_CONSOLIDATED")
    if timestamp is None:
        _append(data_reasons, "PREVIOUS_SESSION_QUOTE_TIMESTAMP_UNVERIFIED")
    else:
        local = timestamp.astimezone(_ET)
        if latest_completed is not None and local.date() != latest_completed:
            _append(data_reasons, "PREVIOUS_SESSION_QUOTE_WRONG_SESSION")
        wall = local.timetz().replace(tzinfo=None)
        if wall < time(15, 55) or wall > time(16, 0):
            _append(data_reasons, "PREVIOUS_SESSION_QUOTE_OUTSIDE_WINDOW")
    if (
        not isinstance(bid, Decimal)
        or not isinstance(ask, Decimal)
        or not bid.is_finite()
        or not ask.is_finite()
        or bid <= _ZERO
        or ask <= _ZERO
        or ask < bid
    ):
        _append(data_reasons, "PREVIOUS_SESSION_QUOTE_INVALID")
        return None
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(ask, bid)
        midpoint = (ask + bid) / Decimal("2")
        return (ask - bid) / midpoint


def _validate_iex(context: CandidateContext, data_reasons: list[str]) -> None:
    quote = context.latest_iex_quote
    if quote is None:
        _append(data_reasons, "IEX_QUOTE_MISSING")
        return
    timestamp = _aware(getattr(quote, "timestamp", None))
    as_of = _operational_as_of(context)
    symbol = getattr(context.record, "symbol", "").upper()
    if getattr(quote, "symbol", "").upper() != symbol:
        _append(data_reasons, "IEX_QUOTE_SYMBOL_MISMATCH")
    if str(getattr(quote, "feed", "")).upper() != "IEX":
        _append(data_reasons, "IEX_QUOTE_FEED_INVALID")
    bid = getattr(quote, "bid", None)
    ask = getattr(quote, "ask", None)
    if (
        not isinstance(bid, Decimal)
        or not isinstance(ask, Decimal)
        or not bid.is_finite()
        or not ask.is_finite()
        or bid <= _ZERO
        or ask <= _ZERO
        or ask < bid
    ):
        _append(data_reasons, "IEX_QUOTE_INVALID")
    if timestamp is None or as_of is None:
        _append(data_reasons, "IEX_QUOTE_TIMESTAMP_UNVERIFIED")
        return
    age = _elapsed_seconds(as_of, timestamp)
    if age < _ZERO:
        _append(data_reasons, "IEX_QUOTE_FROM_FUTURE")
    elif age > _MAXIMUM_IEX_AGE_SECONDS:
        _append(data_reasons, "IEX_QUOTE_STALE")


def _validate_hold_sessions(context: CandidateContext, data_reasons: list[str]) -> None:
    sessions = context.hold_sessions
    if (
        len(sessions) != 10
        or any(type(value) is not date for value in sessions)
        or sessions[0] != context.session_date
        or any(current <= previous for previous, current in zip(sessions, sessions[1:]))
    ):
        _append(data_reasons, "HOLD_WINDOW_UNVERIFIED")
    if any(value.weekday() >= 5 for value in sessions if type(value) is date):
        _append(data_reasons, "HOLD_WINDOW_NOT_MARKET_SESSIONS")
    try:
        derived = build_market_session_attestation(
            context.market_calendar,
            context.session_date,
            context.as_of,
        )
    except (AttributeError, ScreeningError, TypeError):
        _append(data_reasons, "CALENDAR_DERIVATION_UNAVAILABLE")
        derived = None
    attestation = context.session_attestation
    try:
        status = attestation.status
        attested_as_of = attestation.as_of
        attested_previous = attestation.previous_session_date
        attested_sessions = tuple(attestation.hold_sessions)
        reviewed_at = attestation.calendar_reviewed_at
        source_urls = tuple(attestation.source_urls)
    except (AttributeError, TypeError):
        _append(data_reasons, "CALENDAR_ATTESTATION_MISSING")
        return
    if derived is not None and attestation != derived:
        _append(data_reasons, "CALENDAR_DERIVATION_MISMATCH")
    if status != "VERIFIED":
        _append(data_reasons, "CALENDAR_ATTESTATION_UNVERIFIED")
    if (
        attested_as_of != context.as_of
        or attested_previous != context.previous_session_date
        or attested_sessions != sessions
    ):
        _append(data_reasons, "CALENDAR_ATTESTATION_MISMATCH")
    if (
        type(reviewed_at) is not date
        or reviewed_at > context.session_date
        or (context.session_date - reviewed_at).days > 31
    ):
        _append(data_reasons, "CALENDAR_ATTESTATION_STALE")
    if (
        len(source_urls) < 2
        or len(source_urls) != len(set(source_urls))
        or any(
            not isinstance(url, str) or not url.startswith("https://")
            for url in source_urls
        )
    ):
        _append(data_reasons, "CALENDAR_ATTESTATION_PROVENANCE_MISSING")


def _validate_evidence(
    context: CandidateContext,
    data_reasons: list[str],
    policy_reasons: list[str],
) -> None:
    decision = context.evidence
    if decision is None:
        _append(data_reasons, "EVIDENCE_SOURCE_STATUS_MISSING")
        return
    if not is_reviewed_evidence_decision(decision):
        _append(data_reasons, "EVIDENCE_DECISION_UNREVIEWED")
    try:
        health = decision.health
        retrieved_at = decision.retrieved_at
        block_reason = decision.block_reason
        binary_events = tuple(decision.binary_events)
        etf_actions = tuple(decision.etf_actions)
        binary_coverage = decision.binary_event_coverage
        etf_coverage = decision.etf_action_coverage
        subject_kind = decision.subject_kind
        evidence_symbol = getattr(
            decision, "symbol", getattr(decision, "subject_symbol", None)
        )
        evidence_issuer = getattr(
            decision, "issuer_cik", getattr(decision, "issuer", None)
        )
        evidence_as_of = decision.as_of
        source_ids = tuple(decision.source_observation_ids)
        qualifying_records = tuple(decision.qualifying_records)
        adverse_tags = tuple(decision.adverse_tags)
        ambiguities = tuple(decision.ambiguities)
        conflicts = tuple(decision.conflicts)
    except (AttributeError, TypeError):
        _append(data_reasons, "EVIDENCE_CONTRACT_INCOMPLETE")
        return
    expected_symbol = str(getattr(context.record, "symbol", "")).upper()
    expected_kind = (
        "STOCK"
        if getattr(context.record, "product_type", None) == "common_stock"
        else "ETF"
        if getattr(context.record, "product_type", None) == "etf"
        else None
    )
    if subject_kind != expected_kind:
        _append(data_reasons, "EVIDENCE_SUBJECT_KIND_MISMATCH")
    if evidence_symbol != expected_symbol:
        _append(data_reasons, "EVIDENCE_SUBJECT_MISMATCH")
    if evidence_issuer != context.issuer_cik:
        _append(data_reasons, "EVIDENCE_ISSUER_MISMATCH")
    if evidence_as_of != context.as_of:
        _append(data_reasons, "EVIDENCE_AS_OF_MISMATCH")
    if (
        not source_ids
        or len(source_ids) != len(set(source_ids))
        or any(not isinstance(value, str) or not value for value in source_ids)
    ):
        _append(data_reasons, "EVIDENCE_PROVENANCE_MISSING")
    if health != "HEALTHY":
        _append(data_reasons, "EVIDENCE_SOURCE_UNAVAILABLE")
    for values in (adverse_tags, ambiguities, conflicts):
        if any(not isinstance(value, str) or not value.strip() for value in values):
            _append(data_reasons, "EVIDENCE_CONTRACT_INCOMPLETE")
    if adverse_tags:
        _append(policy_reasons, "ADVERSE_EVENT")
    if ambiguities:
        _append(data_reasons, "AMBIGUOUS_EVIDENCE_CLASSIFICATION")
    if conflicts:
        _append(data_reasons, "EVIDENCE_SOURCE_CONFLICT")
    for record in qualifying_records:
        for reason in _catalyst_record_reasons(context, decision, record):
            _append(data_reasons, reason)
    retrieved = _aware(retrieved_at)
    as_of = _aware(context.as_of)
    if retrieved is None or as_of is None:
        _append(data_reasons, "EVIDENCE_TIMESTAMP_UNVERIFIED")
    else:
        age = _elapsed_seconds(as_of, retrieved)
        if age < _ZERO:
            _append(data_reasons, "EVIDENCE_TIMESTAMP_IN_FUTURE")
        elif age > _MAXIMUM_EVIDENCE_AGE_SECONDS:
            _append(data_reasons, "EVIDENCE_SOURCE_STALE")
    if block_reason:
        if block_reason in {
            "BINARY_EVENT_DURING_HOLD",
            "ETF_ACTION_DURING_HOLD",
            "ADVERSE_EVENT",
        }:
            _append(policy_reasons, block_reason)
        else:
            _append(data_reasons, str(block_reason))
    product_type = getattr(context.record, "product_type", None)
    if product_type == "common_stock":
        required_coverage = binary_coverage
        prefix = "BINARY_EVENT"
        if etf_coverage != "NOT_APPLICABLE":
            _append(data_reasons, "EVIDENCE_PRODUCT_COVERAGE_MISMATCH")
    else:
        required_coverage = etf_coverage
        prefix = "ETF_ACTION"
        if binary_coverage != "NOT_APPLICABLE":
            _append(data_reasons, "EVIDENCE_PRODUCT_COVERAGE_MISMATCH")
    if required_coverage == "OVERLAP":
        _append(policy_reasons, f"{prefix}_DURING_HOLD")
    elif required_coverage == "UNKNOWN":
        _append(data_reasons, f"{prefix}_COVERAGE_UNKNOWN")
    elif required_coverage == "CONFLICT":
        _append(data_reasons, f"{prefix}_COVERAGE_CONFLICT")
    elif required_coverage != "CONFIRMED_CLEAR":
        _append(data_reasons, f"{prefix}_COVERAGE_UNVERIFIED")
    if len(context.hold_sessions) == 10:
        start = context.hold_sessions[0]
        end = context.hold_sessions[-1]
        events = (
            binary_events
            if getattr(context.record, "product_type", None) == "common_stock"
            else etf_actions
        )
        expected_reason = (
            "BINARY_EVENT_DURING_HOLD"
            if getattr(context.record, "product_type", None) == "common_stock"
            else "ETF_ACTION_DURING_HOLD"
        )
        for event in events:
            if (
                not isinstance(event, tuple)
                or len(event) != 2
                or type(event[0]) is not date
            ):
                _append(data_reasons, "EVENT_COVERAGE_INCOMPLETE")
                continue
            if start <= event[0] <= end:
                _append(policy_reasons, expected_reason)


def _catalyst_record_reasons(
    context: CandidateContext,
    decision: EvidenceDecisionLike,
    record: CatalystFactLike,
) -> tuple[str, ...]:
    reasons: list[str] = []
    try:
        record_symbol = record.symbol
        record_issuer = record.issuer_cik
        published = _aware(record.published_at)
        retrieved = _aware(record.retrieved_at)
        record_source_ids = tuple(record.source_observation_ids)
        decision_source_ids = tuple(decision.source_observation_ids)
        decision_symbol = getattr(
            decision, "symbol", getattr(decision, "subject_symbol", None)
        )
        decision_issuer = getattr(
            decision, "issuer_cik", getattr(decision, "issuer", None)
        )
    except (AttributeError, TypeError):
        return ("CATALYST_CONTRACT_INCOMPLETE",)
    expected_symbol = str(getattr(context.record, "symbol", "")).upper()
    if (
        record_symbol != expected_symbol
        or record_symbol != decision_symbol
        or record_issuer != context.issuer_cik
        or record_issuer != decision_issuer
    ):
        _append(reasons, "CATALYST_SUBJECT_MISMATCH")
    if (
        not record_source_ids
        or len(record_source_ids) != len(set(record_source_ids))
        or any(
            not isinstance(value, str) or not value for value in record_source_ids
        )
        or not set(record_source_ids).issubset(decision_source_ids)
    ):
        _append(reasons, "CATALYST_PROVENANCE_MISMATCH")
    current = _aware(context.as_of)
    if published is None or retrieved is None or current is None:
        _append(reasons, "CATALYST_TIMESTAMP_UNVERIFIED")
    elif published > current or retrieved > current:
        _append(reasons, "CATALYST_TIMESTAMP_IN_FUTURE")
    elif published > retrieved:
        _append(reasons, "CATALYST_TIMESTAMP_CONFLICT")
    elif _elapsed_seconds(current, retrieved) > _MAXIMUM_EVIDENCE_AGE_SECONDS:
        _append(reasons, "CATALYST_SOURCE_STALE")
    return tuple(reasons)


def _dual_index_downtrend(context: CandidateContext) -> bool:
    results: list[bool] = []
    for symbol in ("SPY", "QQQ"):
        bars = context.bars_by_symbol.get(symbol)
        if bars is None or len(bars) != 60:
            return False
        closes = tuple(bar.close for bar in bars)
        try:
            current = sma(closes, 50)
            prior = sma(closes[:-10], 50)
        except IndicatorError:
            return False
        results.append(closes[-1] < current and current < prior)
    return all(results)


def _validate_instrument_status(
    context: CandidateContext,
    data_reasons: list[str],
    policy_reasons: list[str],
) -> None:
    status = context.instrument_status
    if status is None:
        _append(data_reasons, "HALT_STATUS_UNKNOWN")
        return
    if not is_reviewed_instrument_status_decision(status):
        _append(data_reasons, "INSTRUMENT_STATUS_UNREVIEWED")
    expected_symbol = str(getattr(context.record, "symbol", "")).upper()
    try:
        status_symbol = status.symbol
        halt_status = status.halt_status
        block_reason = status.block_reason
        status_as_of = _aware(status.as_of)
        valid_until = _aware(getattr(status, "valid_until", None))
        source_ids = tuple(status.source_observation_ids)
    except (AttributeError, TypeError):
        _append(data_reasons, "HALT_STATUS_CONTRACT_INCOMPLETE")
        return
    if status_symbol != expected_symbol:
        _append(data_reasons, "HALT_STATUS_SYMBOL_MISMATCH")
    if (
        not source_ids
        or len(source_ids) != len(set(source_ids))
        or any(not isinstance(value, str) or not value for value in source_ids)
    ):
        _append(data_reasons, "HALT_STATUS_PROVENANCE_MISSING")
    current = _operational_as_of(context)
    currently_valid = (
        status_as_of is not None
        and current is not None
        and valid_until is not None
        and status_as_of <= current <= valid_until
    )
    if not currently_valid:
        _append(data_reasons, "HALT_STATUS_STALE")
    if halt_status == "HALTED":
        _append(policy_reasons, "SYMBOL_HALTED")
    elif halt_status != "CLEAR":
        _append(data_reasons, "HALT_STATUS_UNKNOWN")
    elif block_reason is not None:
        _append(data_reasons, "HALT_STATUS_CONFLICT")


def evaluate_eligibility(context: CandidateContext) -> EligibilityDecision:
    """Audit every hard gate without short-circuiting the reason list."""
    if not isinstance(context, CandidateContext):
        raise TypeError("eligibility requires a CandidateContext")
    data_reasons: list[str] = []
    policy_reasons: list[str] = []
    pause_reasons: list[str] = []
    record = context.record
    if _operational_as_of(context) is None:
        _append(data_reasons, "OPERATIONAL_AS_OF_INVALID")

    latest_completed = _validate_bar_cohort(context, data_reasons)
    bars = context.bars_by_symbol.get(str(getattr(record, "symbol", "")).upper())
    average: Decimal | None = None
    median: Decimal | None = None
    last_close: Decimal | None = None
    if bars is not None and len(bars) == 60:
        try:
            average = average_dollar_volume(bars, 20)
            median = median_share_volume(bars, 20)
            last_close = bars[-1].close
        except (IndicatorError, AttributeError):
            _append(data_reasons, "BAR_VALUES_INVALID")
    if last_close is not None and last_close < _MINIMUM_PRICE:
        _append(policy_reasons, "PRICE_BELOW_MINIMUM")
    if average is not None and average < _MINIMUM_DOLLAR_VOLUME:
        _append(policy_reasons, "AVERAGE_DOLLAR_VOLUME_BELOW_MINIMUM")
    if median is not None and median < _MINIMUM_SHARE_VOLUME:
        _append(policy_reasons, "MEDIAN_SHARE_VOLUME_BELOW_MINIMUM")

    product_type = getattr(record, "product_type", None)
    if product_type not in _APPROVED_PRODUCTS:
        _append(policy_reasons, "PRODUCT_TYPE_INELIGIBLE")
    if getattr(record, "enabled", None) is not True:
        _append(policy_reasons, "PRODUCT_DISABLED")
    leveraged = getattr(record, "leveraged", None)
    inverse = getattr(record, "inverse", None)
    if type(leveraged) is not bool or type(inverse) is not bool:
        _append(data_reasons, "PRODUCT_FLAGS_UNVERIFIED")
    elif leveraged or inverse:
        _append(policy_reasons, "LEVERAGED_OR_INVERSE_PRODUCT")
    if getattr(record, "listing_venue", None) not in _APPROVED_VENUES:
        _append(policy_reasons, "OTC_SECURITY")
    if product_type == "common_stock":
        free_float = getattr(record, "free_float", None)
        if type(free_float) is not int:
            _append(data_reasons, "FREE_FLOAT_UNVERIFIED")
        elif free_float < _MINIMUM_FREE_FLOAT:
            _append(policy_reasons, "FREE_FLOAT_BELOW_MINIMUM")

    if context.listing_date_status != "VERIFIED" or type(context.initial_listing_date) is not date:
        _append(data_reasons, "IPO_DATE_UNVERIFIED")
    elif (context.session_date - context.initial_listing_date).days < 91:
        _append(policy_reasons, "IPO_TOO_RECENT")

    _validate_instrument_status(context, data_reasons, policy_reasons)

    spread = _quote_spread(context, latest_completed, data_reasons)
    if spread is not None and spread > _MAXIMUM_SPREAD:
        _append(policy_reasons, "SPREAD_TOO_WIDE")
    _validate_iex(context, data_reasons)
    _validate_hold_sessions(context, data_reasons)
    _validate_evidence(context, data_reasons, policy_reasons)
    if context.rumor_dependent is not False:
        _append(policy_reasons, "RUMOR_DEPENDENT")
    if not data_reasons and _dual_index_downtrend(context):
        _append(pause_reasons, "DUAL_INDEX_DOWNTREND_PAPER_ONLY")

    data_available = not data_reasons
    eligible = data_available and not policy_reasons
    paper_only = eligible and bool(pause_reasons)
    live_eligible = eligible and not paper_only
    if not data_available:
        decision_status = "DATA_UNAVAILABLE"
    elif policy_reasons:
        decision_status = "INELIGIBLE"
    elif paper_only:
        decision_status = "ELIGIBLE_PAPER_ONLY"
    else:
        decision_status = "ELIGIBLE"
    return EligibilityDecision(
        eligible=eligible,
        base_eligible=eligible,
        live_eligible=live_eligible,
        paper_only=paper_only,
        data_available=data_available,
        status=decision_status,
        reason_codes=tuple(data_reasons + policy_reasons + pause_reasons),
        average_dollar_volume=average,
        median_share_volume=median,
        spread_percent=spread,
    )


def _round_up(value: Decimal, tick_size: Decimal) -> Decimal:
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(value, tick_size)
        return (
            (value / tick_size).to_integral_value(rounding=ROUND_CEILING)
            * tick_size
        )


def _round_down(value: Decimal, tick_size: Decimal) -> Decimal:
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(value, tick_size)
        return (
            (value / tick_size).to_integral_value(rounding=ROUND_FLOOR)
            * tick_size
        )


def _empty_setup(status: str, *reasons: str) -> SetupDecision:
    return SetupDecision(
        eligible=False,
        status=status,
        setup_type=None,
        qualifying_setups=(),
        reason_codes=tuple(reasons),
        atr14=None,
        resistance=None,
        raw_trigger=None,
        raw_stop=None,
        trigger_price=None,
        stop_price=None,
        planned_entry=None,
        maximum_permitted_entry=None,
        target_price=None,
        stop_distance=None,
    )


def _four_trend_conditions(bars: Sequence[BarLike]) -> tuple[bool, bool, bool, bool]:
    closes = tuple(bar.close for bar in bars)
    current_sma20 = sma(closes, 20)
    prior_sma20 = sma(closes[:-5], 20)
    current_sma50 = sma(closes, 50)
    prior_sma50 = sma(closes[:-10], 50)
    return (
        closes[-1] > current_sma20,
        current_sma20 > prior_sma20,
        closes[-1] > current_sma50,
        current_sma50 > prior_sma50,
    )


def detect_setup(context: CandidateContext) -> SetupDecision:
    """Detect one approved setup using only the frozen completed-session snapshot."""
    if not isinstance(context, CandidateContext):
        raise TypeError("setup detection requires a CandidateContext")
    record = context.record
    symbol = str(getattr(record, "symbol", "")).upper()
    bars = context.bars_by_symbol.get(symbol)
    if bars is None or len(bars) != 60:
        return _empty_setup("DATA_UNAVAILABLE", "BAR_HISTORY_INCOMPLETE")
    validation_reasons: list[str] = []
    _validate_bar_cohort(context, validation_reasons)
    _validate_hold_sessions(context, validation_reasons)
    if validation_reasons:
        return _empty_setup("DATA_UNAVAILABLE", *validation_reasons)
    tick_size = getattr(record, "tick_size", None)
    if (
        not isinstance(tick_size, Decimal)
        or not tick_size.is_finite()
        or tick_size <= _ZERO
    ):
        return _empty_setup("DATA_UNAVAILABLE", "INVALID_TICK_SIZE")
    quote = context.previous_session_quote
    if quote is None:
        return _empty_setup("DATA_UNAVAILABLE", "PREVIOUS_SESSION_QUOTE_MISSING")
    quote_reasons: list[str] = []
    _quote_spread(context, context.previous_session_date, quote_reasons)
    if quote_reasons:
        return _empty_setup("DATA_UNAVAILABLE", *quote_reasons)
    bid = getattr(quote, "bid", None)
    ask = getattr(quote, "ask", None)
    if (
        not isinstance(bid, Decimal)
        or not isinstance(ask, Decimal)
        or not bid.is_finite()
        or not ask.is_finite()
        or bid <= _ZERO
        or ask <= _ZERO
        or ask < bid
    ):
        return _empty_setup("DATA_UNAVAILABLE", "PREVIOUS_SESSION_QUOTE_INVALID")

    try:
        closes = tuple(bar.close for bar in bars)
        trend_conditions = _four_trend_conditions(bars)
        current_ema20 = ema(closes, 20)
        atr14 = wilder_atr(bars, 14)
    except IndicatorError:
        return _empty_setup("DATA_UNAVAILABLE", "BAR_VALUES_INVALID")
    if atr14 <= _ZERO:
        return _empty_setup("INELIGIBLE", "ATR_NONPOSITIVE")

    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(
            tick_size,
            bid,
            ask,
            current_ema20,
            atr14,
            *(bar.close for bar in bars),
            *(bar.high for bar in bars),
            *(bar.low for bar in bars),
        )
        half_atr = Decimal("0.5") * atr14
        pullback = (
            all(trend_conditions)
            and any(
                current_ema20 - half_atr <= bar.low <= current_ema20 + half_atr
                for bar in bars[-3:]
            )
            and closes[-1] > current_ema20
            and closes[-1] > closes[-2]
        )
        resistance = max(bar.high for bar in bars[-21:-2])
        atr_ratio = atr14 / closes[-1]
        breakout = (
            resistance - half_atr <= closes[-1] <= resistance
            and Decimal("0.01") <= atr_ratio <= Decimal("0.05")
        )
        qualifying: list[str] = []
        if pullback:
            qualifying.append("PULLBACK_RECLAIM")
        if breakout:
            qualifying.append("BREAKOUT_CONFIRMATION")
        if not qualifying:
            return _empty_setup("INELIGIBLE", "SETUP_NOT_QUALIFIED")
        selected = qualifying[0]
        if selected == "PULLBACK_RECLAIM":
            raw_trigger = max(bars[-1].high, bars[-2].high) + Decimal("0.05") * atr14
        else:
            raw_trigger = resistance + Decimal("0.05") * atr14
        raw_stop = min(bar.low for bar in bars[-3:]) - Decimal("0.10") * atr14
        if raw_stop <= _ZERO:
            return _empty_setup("INELIGIBLE", "NONPOSITIVE_STOP_PRICE")
        trigger = _round_up(raw_trigger, tick_size)
        stop = _round_down(raw_stop, tick_size)
        if stop <= _ZERO:
            return _empty_setup("INELIGIBLE", "NONPOSITIVE_STOP_PRICE")
        delayed_spread = ask - bid
        slippage = max(Decimal("0.001") * trigger, delayed_spread / Decimal("2"))
        planned_entry = _round_up(trigger + slippage, tick_size)
        stop_distance = planned_entry - stop
        if stop_distance <= _ZERO:
            return _empty_setup("INELIGIBLE", "NONPOSITIVE_STOP_DISTANCE")
        target = _round_up(
            planned_entry + Decimal("2") * stop_distance,
            tick_size,
        )
    return SetupDecision(
        eligible=True,
        status="ELIGIBLE",
        setup_type=selected,
        qualifying_setups=tuple(qualifying),
        reason_codes=(),
        atr14=atr14,
        resistance=resistance if breakout else None,
        raw_trigger=raw_trigger,
        raw_stop=raw_stop,
        trigger_price=trigger,
        stop_price=stop,
        planned_entry=planned_entry,
        maximum_permitted_entry=planned_entry,
        target_price=target,
        stop_distance=stop_distance,
    )


def midrank_percentile(value: Decimal, cohort: Sequence[Decimal]) -> Decimal:
    """Return deterministic midrank percentile, including 50 for one equal item."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ScreeningError("percentile value must be a finite Decimal")
    if isinstance(cohort, (str, bytes)):
        raise ScreeningError("percentile cohort must be a Decimal sequence")
    values = tuple(cohort)
    if not values:
        raise ScreeningError("percentile cohort is empty")
    if any(not isinstance(item, Decimal) or not item.is_finite() for item in values):
        raise ScreeningError("percentile cohort contains an invalid value")
    lower = sum(item < value for item in values)
    equal = sum(item == value for item in values)
    with localcontext() as decimal_context:
        decimal_context.prec = 50
        return (
            (Decimal(lower) + Decimal(equal) / Decimal("2"))
            / Decimal(len(values))
            * Decimal("100")
        )


def _relative_returns(context: CandidateContext) -> tuple[Decimal, Decimal]:
    record = context.record
    symbol = str(getattr(record, "symbol", "")).upper()
    market_symbol = str(getattr(record, "benchmark", "")).upper()
    if getattr(record, "product_type", None) == "common_stock":
        relative_symbol = str(getattr(record, "sector_etf", "") or "").upper()
    else:
        relative_symbol = market_symbol
    instrument = context.bars_by_symbol.get(symbol)
    market = context.bars_by_symbol.get(market_symbol)
    relative = context.bars_by_symbol.get(relative_symbol)
    if instrument is None or market is None or relative is None:
        raise ScreeningError("relative-strength benchmark history is missing")
    instrument_closes = tuple(bar.close for bar in instrument)
    market_closes = tuple(bar.close for bar in market)
    relative_closes = tuple(bar.close for bar in relative)
    with localcontext() as decimal_context:
        decimal_context.prec = 50
        five = five_session_return(instrument_closes) - five_session_return(
            market_closes
        )
        twenty = twenty_session_return(instrument_closes) - twenty_session_return(
            relative_closes
        )
        return five, twenty


def _digest_fields(values: Sequence[object]) -> str:
    digest = hashlib.sha256()
    for value in values:
        if isinstance(value, Decimal):
            encoded = repr(value.as_tuple()).encode("ascii")
        elif isinstance(value, (date, datetime)):
            encoded = value.isoformat().encode("ascii")
        else:
            encoded = repr(value).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _canonical_snapshot_value(value: object) -> object:
    if value is None:
        return {"type": "none"}
    if type(value) is bool:
        return {"type": "boolean", "value": value}
    if type(value) is int:
        return {"type": "integer", "value": value}
    if type(value) is str:
        return {"type": "string", "value": value}
    if type(value) is bytes:
        return {"type": "bytes", "value": value.hex()}
    if type(value) is Decimal:
        if not value.is_finite():
            raise ScreeningError("cohort context contains a non-finite Decimal")
        parts = value.as_tuple()
        return {
            "type": "decimal",
            "sign": parts.sign,
            "digits": list(parts.digits),
            "exponent": parts.exponent,
        }
    if type(value) is datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ScreeningError("cohort context contains a naive timestamp")
        return {"type": "datetime", "value": value.isoformat()}
    if type(value) is date:
        return {"type": "date", "value": value.isoformat()}
    if type(value) is time:
        return {"type": "time", "value": value.isoformat()}
    if isinstance(value, ZoneInfo):
        return {"type": "zoneinfo", "value": value.key}
    if type(value) is tuple:
        return {
            "type": "tuple",
            "value": [_canonical_snapshot_value(item) for item in value],
        }
    if type(value) is frozenset:
        documents = [_canonical_snapshot_value(item) for item in value]
        documents.sort(
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))
        )
        return {"type": "frozenset", "value": documents}
    if isinstance(value, Mapping):
        entries = [
            (
                _canonical_snapshot_value(key),
                _canonical_snapshot_value(item),
            )
            for key, item in value.items()
        ]
        entries.sort(
            key=lambda pair: json.dumps(
                pair[0],
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return {
            "type": "mapping",
            "value": [[key, item] for key, item in entries],
        }
    if is_dataclass(value) and not isinstance(value, type):
        document: dict[str, object] = {}
        for item in dataclass_fields(value):
            if item.name.startswith("_") or (
                type(value) is CandidateContext
                and item.name == "relative_strength_cohort"
            ):
                continue
            document[item.name] = _canonical_snapshot_value(
                getattr(value, item.name)
            )
        return {
            "type": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": document,
        }
    raise ScreeningError("cohort context contains an unsupported value")


def _relative_input_fingerprint(context: CandidateContext) -> str:
    if type(context) is not CandidateContext:
        raise ScreeningError("cohort context must be the exact candidate DTO")
    document = _canonical_snapshot_value(context)
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _cohort_integrity(cohort: RelativeStrengthCohort) -> str:
    if type(cohort) is not RelativeStrengthCohort:
        raise ScreeningError("cohort must be the exact relative-strength DTO")
    document = _canonical_snapshot_value(cohort)
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _issued_cohort_digest(cohort: RelativeStrengthCohort) -> str | None:
    with _ISSUED_COHORTS_LOCK:
        issued = _ISSUED_COHORTS.get(id(cohort))
        if issued is None or issued[0]() is not cohort:
            return None
        return issued[1]


def _trusted_relative_strength_cohort(
    context: CandidateContext,
    cohort: RelativeStrengthCohort,
) -> bool:
    if type(cohort) is not RelativeStrengthCohort:
        return False
    issued_digest = _issued_cohort_digest(cohort)
    if (
        issued_digest is None
        or cohort._authority is not _COHORT_AUTHORITY
        or type(cohort._integrity) is not str
        or _SHA256_HEX.fullmatch(cohort._integrity) is None
        or cohort._integrity != issued_digest
    ):
        return False
    try:
        _validate_cohort_fields(cohort)
        if cohort._integrity != _cohort_integrity(cohort):
            return False
        if not cohort.expected_symbols or not cohort.observations:
            return False
        symbol = str(getattr(context.record, "symbol", ""))
        matches = tuple(
            observation
            for observation in cohort.observations
            if observation.symbol == symbol
        )
        if len(matches) != 1 or symbol not in cohort.expected_symbols:
            return False
        observation = matches[0]
        if observation.context_fingerprint != _relative_input_fingerprint(context):
            return False
        current_returns = _relative_returns(context)
    except (AttributeError, IndicatorError, ScreeningError, TypeError, ValueError):
        return False
    return current_returns == (
        observation.five_session_value,
        observation.twenty_session_value,
    )


def build_base_eligible_cohort(
    contexts: Sequence[CandidateContext],
    *,
    universe: UniverseSnapshot,
) -> CohortDecision:
    """Build percentiles only from a complete checksum-reviewed universe snapshot."""
    if isinstance(contexts, (str, bytes)):
        raise TypeError("candidate cohort must be a sequence")
    if not isinstance(universe, UniverseSnapshot):
        raise TypeError("candidate cohort requires a UniverseSnapshot")
    values = tuple(contexts)
    if any(not isinstance(item, CandidateContext) for item in values):
        raise TypeError("candidate cohort contains a non-context value")
    if not is_verified_universe_snapshot(universe):
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=(),
            reason_codes=("COHORT_UNIVERSE_UNVERIFIED",),
        )
    def canonical_symbol(value: object) -> str:
        if not isinstance(value, str) or _SYMBOL.fullmatch(value) is None:
            raise ScreeningError("cohort symbol must be a canonical uppercase string")
        return value

    actual = tuple(canonical_symbol(item.record.symbol) for item in values)
    if len(actual) != len(set(actual)):
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=(),
            reason_codes=("COHORT_SYMBOL_DUPLICATE",),
        )
    expected = tuple(
        canonical_symbol(record.symbol)
        for record in universe.records
        if "eligible" in record.support_roles
    )
    checksum = universe.checksum
    cohort_sessions = {
        (item.session_date, item.previous_session_date, item.as_of) for item in values
    }
    provenance_valid = (
        isinstance(checksum, str)
        and checksum.startswith("sha256:")
        and len(checksum) == 71
        and all(character in "0123456789abcdef" for character in checksum[7:])
        and type(universe.effective_date) is date
        and type(universe.reviewed_at) is date
        and type(universe.review_by) is date
        and len(cohort_sessions) == 1
        and all(
            universe.effective_date <= item.session_date <= universe.review_by
            and universe.reviewed_at <= item.session_date
            and universe.by_symbol.get(item.record.symbol) == item.record
            for item in values
        )
    )
    if not provenance_valid:
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=(),
            reason_codes=("COHORT_UNIVERSE_PROVENANCE_INVALID",),
        )
    if (
        not expected
        or len(expected) != len(set(expected))
        or len(actual) != len(expected)
        or set(actual) != set(expected)
    ):
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=(),
            reason_codes=("COHORT_FETCH_INCOMPLETE",),
        )
    decisions = tuple(evaluate_eligibility(item) for item in values)
    if any(not decision.data_available for decision in decisions):
        reasons = tuple(
            dict.fromkeys(
                reason
                for decision in decisions
                if not decision.data_available
                for reason in decision.reason_codes
            )
        )
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=decisions,
            reason_codes=("COHORT_DATA_INCOMPLETE",) + reasons,
        )
    eligible_contexts = tuple(
        item
        for item, decision in zip(values, decisions)
        if decision.base_eligible
    )
    if not eligible_contexts:
        return CohortDecision(
            status="NO_TRADE",
            contexts=(),
            eligibility=decisions,
            reason_codes=("NATURAL_COHORT_EMPTY",),
        )
    ordered_eligible = tuple(
        sorted(eligible_contexts, key=lambda item: item.record.symbol)
    )
    try:
        returns = tuple(_relative_returns(item) for item in ordered_eligible)
    except (IndicatorError, ScreeningError):
        return CohortDecision(
            status="DATA_UNAVAILABLE",
            contexts=(),
            eligibility=decisions,
            reason_codes=("RELATIVE_STRENGTH_BENCHMARK_INCOMPLETE",),
        )
    observations = tuple(
        RelativeStrengthObservation(
            symbol=item.record.symbol,
            five_session_value=values_for_symbol[0],
            twenty_session_value=values_for_symbol[1],
            context_fingerprint=_relative_input_fingerprint(item),
        )
        for item, values_for_symbol in zip(ordered_eligible, returns)
    )
    cohort = RelativeStrengthCohort(
        five_session_values=tuple(value[0] for value in returns),
        twenty_session_values=tuple(value[1] for value in returns),
        observations=observations,
        expected_symbols=expected,
        universe_checksum=universe.checksum,
        universe_effective_date=universe.effective_date,
        universe_reviewed_at=universe.reviewed_at,
        universe_review_by=universe.review_by,
    )
    digest = _cohort_integrity(cohort)
    object.__setattr__(cohort, "_authority", _COHORT_AUTHORITY)
    object.__setattr__(cohort, "_integrity", digest)
    identity = id(cohort)

    def discard_cohort(
        dead_reference: ReferenceType[RelativeStrengthCohort],
    ) -> None:
        with _ISSUED_COHORTS_LOCK:
            current = _ISSUED_COHORTS.get(identity)
            if current is not None and current[0] is dead_reference:
                del _ISSUED_COHORTS[identity]

    cohort_reference = ref(cohort, discard_cohort)
    with _ISSUED_COHORTS_LOCK:
        _ISSUED_COHORTS[identity] = (cohort_reference, digest)
    enriched = tuple(
        replace(item, relative_strength_cohort=cohort)
        for item in eligible_contexts
    )
    return CohortDecision(
        status="READY",
        contexts=enriched,
        eligibility=decisions,
        reason_codes=(),
    )


def _regime_condition(context: CandidateContext) -> bool:
    for symbol in ("SPY", "QQQ"):
        bars = context.bars_by_symbol.get(symbol)
        if bars is None or len(bars) != 60:
            continue
        closes = tuple(bar.close for bar in bars)
        try:
            current = sma(closes, 50)
            prior = sma(closes[:-10], 50)
        except IndicatorError:
            continue
        if closes[-1] > current and current > prior:
            return True
    return False


def _relative_strength_points(percentile: Decimal) -> int:
    if percentile >= Decimal("75"):
        return 10
    if percentile >= Decimal("50"):
        return 5
    return 0


def _volume_points(
    bars: Sequence[BarLike],
    average: Decimal | None,
) -> int:
    points = 0
    if average is not None:
        if average >= Decimal("500000000"):
            points += 5
        elif average >= Decimal("250000000"):
            points += 3
        elif average >= _MINIMUM_DOLLAR_VOLUME:
            points += 1
    try:
        if max_relative_volume(bars, 20, 3) >= Decimal("1.2"):
            points += 5
        up_mean, down_mean = directional_volume_means(bars, 10)
        if up_mean is not None and down_mean is not None and up_mean > down_mean:
            points += 5
    except IndicatorError:
        return points
    return points


_STOCK_CATALYST_TYPES = frozenset(
    {
        "financial results/guidance",
        "material agreement",
        "product/regulatory milestone",
        "capital allocation",
        "management/governance",
        "acquisition/disposition",
    }
)
_ETF_CATALYST_TYPES = frozenset(
    {
        "fund sponsor notice",
        "fund-sponsor notice",
        "index provider notice",
        "index-provider notice",
    }
)


def _evidence_is_current_and_clear(
    context: CandidateContext,
    decision: EvidenceDecisionLike,
) -> bool:
    if not is_reviewed_evidence_decision(decision):
        return False
    try:
        product_type = context.record.product_type
        expected_kind = "STOCK" if product_type == "common_stock" else "ETF"
        expected_symbol = context.record.symbol
        retrieved = _aware(decision.retrieved_at)
        current = _aware(context.as_of)
        source_ids = tuple(decision.source_observation_ids)
        raw_flags = (
            tuple(decision.adverse_tags),
            tuple(decision.ambiguities),
            tuple(decision.conflicts),
        )
        if product_type == "common_stock":
            product_coverage_clear = (
                decision.binary_event_coverage == "CONFIRMED_CLEAR"
                and decision.etf_action_coverage == "NOT_APPLICABLE"
            )
        else:
            product_coverage_clear = (
                decision.binary_event_coverage == "NOT_APPLICABLE"
                and decision.etf_action_coverage == "CONFIRMED_CLEAR"
            )
        evidence_symbol = getattr(
            decision, "symbol", getattr(decision, "subject_symbol", None)
        )
        evidence_issuer = getattr(
            decision, "issuer_cik", getattr(decision, "issuer", None)
        )
    except (AttributeError, TypeError):
        return False
    if (
        product_type not in _APPROVED_PRODUCTS
        or decision.subject_kind != expected_kind
        or evidence_symbol != expected_symbol
        or evidence_issuer != context.issuer_cik
        or decision.as_of != context.as_of
        or decision.health != "HEALTHY"
        or decision.block_reason is not None
        or not product_coverage_clear
        or any(raw_flags)
        or not source_ids
        or len(source_ids) != len(set(source_ids))
        or any(not isinstance(value, str) or not value for value in source_ids)
        or retrieved is None
        or current is None
    ):
        return False
    age = _elapsed_seconds(current, retrieved)
    return _ZERO <= age <= _MAXIMUM_EVIDENCE_AGE_SECONDS


def _catalyst_points(context: CandidateContext) -> int:
    decision = context.evidence
    if decision is None or not _evidence_is_current_and_clear(context, decision):
        return 0
    try:
        records = tuple(decision.qualifying_records)
    except (AttributeError, TypeError):
        return 0
    allowed = (
        _STOCK_CATALYST_TYPES
        if getattr(context.record, "product_type", None) == "common_stock"
        else _ETF_CATALYST_TYPES
    )
    best = 0
    for record in records:
        if _catalyst_record_reasons(context, decision, record):
            continue
        try:
            event_type = record.event_type
            published_at = _aware(record.published_at)
            primary_url = record.primary_url
            publisher = record.publisher
            fact = record.fact
            source_ids = tuple(record.source_observation_ids)
        except (AttributeError, TypeError):
            continue
        if (
            event_type not in allowed
            or published_at is None
            or not isinstance(primary_url, str)
            or not primary_url.startswith("https://")
            or not isinstance(publisher, str)
            or not publisher.strip()
            or not isinstance(fact, str)
            or not fact.strip()
            or not source_ids
        ):
            continue
        age_days = (
            context.session_date - published_at.astimezone(_ET).date()
        ).days
        if 0 <= age_days <= 10:
            best = max(best, 10)
        elif 11 <= age_days <= 30:
            best = max(best, 5)
    return best


def _liquidity_points(
    median: Decimal | None,
    spread: Decimal | None,
) -> int:
    points = 0
    if median is not None:
        if median >= Decimal("5000000"):
            points += 5
        elif median >= Decimal("2000000"):
            points += 3
        elif median >= _MINIMUM_SHARE_VOLUME:
            points += 1
    if spread is not None:
        if spread <= Decimal("0.001"):
            points += 5
        elif spread <= Decimal("0.002"):
            points += 3
        elif spread <= _MAXIMUM_SPREAD:
            points += 1
    return points


def score_candidate(context: CandidateContext) -> ScoreCard:
    """Compute six capped categories; hard/data gates can never be narrated away."""
    if not isinstance(context, CandidateContext):
        raise TypeError("scoring requires a CandidateContext")
    eligibility = evaluate_eligibility(context)
    setup = detect_setup(context)
    reasons = list(eligibility.reason_codes)
    for reason in setup.reason_codes:
        _append(reasons, reason)
    symbol = str(context.record.symbol).upper()
    bars = context.bars_by_symbol.get(symbol, ())

    trend = 0
    try:
        trend = sum(5 for passed in _four_trend_conditions(bars) if passed)
        if _regime_condition(context):
            trend += 5
    except IndicatorError:
        _append(reasons, "BAR_VALUES_INVALID")

    five_value: Decimal | None = None
    twenty_value: Decimal | None = None
    five_percentile: Decimal | None = None
    twenty_percentile: Decimal | None = None
    relative_points = 0
    cohort = context.relative_strength_cohort
    cohort_trusted = False
    try:
        if cohort is None:
            raise ScreeningError("relative-strength cohort is missing")
        if not _trusted_relative_strength_cohort(context, cohort):
            _append(reasons, "RELATIVE_STRENGTH_COHORT_UNTRUSTED")
            raise ScreeningError("relative-strength cohort is untrusted")
        cohort_trusted = True
        five_value, twenty_value = _relative_returns(context)
        five_percentile = midrank_percentile(
            five_value, cohort.five_session_values
        )
        twenty_percentile = midrank_percentile(
            twenty_value, cohort.twenty_session_values
        )
        relative_points = _relative_strength_points(
            five_percentile
        ) + _relative_strength_points(twenty_percentile)
    except (IndicatorError, ScreeningError):
        _append(reasons, "RELATIVE_STRENGTH_COHORT_INCOMPLETE")

    setup_points = 20 if setup.eligible else 0
    volume = _volume_points(bars, eligibility.average_dollar_volume)
    catalyst = _catalyst_points(context)
    liquidity = _liquidity_points(
        eligibility.median_share_volume,
        eligibility.spread_percent,
    )
    total = trend + relative_points + setup_points + volume + catalyst + liquidity
    cohort_complete = (
        cohort_trusted
        and cohort is not None
        and bool(cohort.five_session_values)
        and bool(cohort.twenty_session_values)
        and five_percentile is not None
        and twenty_percentile is not None
    )
    if not eligibility.data_available or not cohort_complete:
        status = "DATA_UNAVAILABLE"
    elif not eligibility.eligible or not setup.eligible:
        status = "INELIGIBLE"
    elif total >= 80:
        status = "PUBLISHABLE"
    else:
        status = "BELOW_MINIMUM_SCORE"
    return ScoreCard(
        status=status,
        reason_codes=tuple(reasons),
        trend_and_regime=trend,
        relative_strength=relative_points,
        setup_quality=setup_points,
        volume_confirmation=volume,
        catalyst_context=catalyst,
        liquidity_execution=liquidity,
        total=total,
        publishable=status == "PUBLISHABLE",
        five_session_relative_strength=five_value,
        twenty_session_relative_strength=twenty_value,
        five_session_relative_strength_percentile=five_percentile,
        twenty_session_relative_strength_percentile=twenty_percentile,
    )


def to_scored_candidate(context: CandidateContext) -> ScoredCandidate:
    """Freeze a publishable score and price plan without consulting portfolio state."""
    score = score_candidate(context)
    setup = detect_setup(context)
    eligibility = evaluate_eligibility(context)
    if not score.publishable or not setup.eligible:
        raise ScreeningError("candidate is not publishable")
    if (
        setup.raw_trigger is None
        or setup.raw_stop is None
        or setup.trigger_price is None
        or setup.maximum_permitted_entry is None
        or setup.stop_price is None
        or setup.target_price is None
        or score.twenty_session_relative_strength_percentile is None
        or eligibility.average_dollar_volume is None
        or context.previous_session_quote is None
    ):
        raise ScreeningError("publishable candidate price contract is incomplete")
    ask = context.previous_session_quote.ask
    bid = context.previous_session_quote.bid
    with localcontext() as decimal_context:
        decimal_context.prec = _decimal_work_precision(ask, bid)
        spread = ask - bid
    candidate = ScoredCandidate(
            symbol=str(context.record.symbol).upper(),
            total_score=score.total,
            relative_strength_percentile=(
                score.twenty_session_relative_strength_percentile
            ),
            average_dollar_volume=eligibility.average_dollar_volume,
            publication_session=context.session_date,
            raw_trigger=setup.raw_trigger,
            raw_stop=setup.raw_stop,
            delayed_spread_amount=spread,
            tick_size=context.record.tick_size,
            trigger_price=setup.trigger_price,
            maximum_permitted_entry=setup.maximum_permitted_entry,
            recommended_stop=setup.stop_price,
            target_price=setup.target_price,
            score_card=score,
            setup=setup,
        )
    identity = id(candidate)
    digest = _scored_candidate_fingerprint(candidate)
    context_digest = _relative_input_fingerprint(context)
    source_observation_ids = _candidate_source_observation_ids(context)
    try:
        normalized_market_fact_sources = (
            _candidate_normalized_market_fact_sources(context)
        )
    except ScreeningError:
        normalized_market_fact_sources = ()

    def discard(dead: ReferenceType[ScoredCandidate]) -> None:
        with _ISSUED_SCORED_CANDIDATES_LOCK:
            current = _ISSUED_SCORED_CANDIDATES.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_SCORED_CANDIDATES.pop(identity, None)

    reference = ref(candidate, discard)
    with _ISSUED_SCORED_CANDIDATES_LOCK:
        _ISSUED_SCORED_CANDIDATES[identity] = _IssuedScoredCandidateAuthority(
            reference=reference,
            candidate_digest=digest,
            context=context,
            context_digest=context_digest,
            source_observation_ids=source_observation_ids,
            normalized_market_fact_sources=normalized_market_fact_sources,
        )
    return candidate


def rank_candidates(
    candidates: Sequence[ScoredCandidate],
) -> tuple[ScoredCandidate, ...]:
    """Apply the locked four-way ordering and return no more than three."""
    if isinstance(candidates, (str, bytes)):
        raise TypeError("ranked candidates must be a sequence")
    values = tuple(candidates)
    if any(not isinstance(item, ScoredCandidate) for item in values):
        raise TypeError("ranked candidates contain an invalid value")
    symbols = tuple(item.symbol for item in values)
    if len(symbols) != len(set(symbols)):
        raise ScreeningError("ranked candidates contain duplicate symbols")
    if any(
        item.publication_session is None
        or item.raw_trigger is None
        or item.raw_stop is None
        or item.delayed_spread_amount is None
        or item.tick_size is None
        or item.trigger_price is None
        or item.maximum_permitted_entry is None
        or item.recommended_stop is None
        or item.target_price is None
        or item.score_card is None
        or not item.score_card.publishable
        or item.score_card.status != "PUBLISHABLE"
        or item.setup is None
        or not item.setup.eligible
        for item in values
    ):
        raise ScreeningError("ranking requires complete publishable candidates")
    # Stable passes preserve exact Decimal comparison.  In particular, avoid
    # negating a Decimal for a descending tuple key: unary arithmetic obeys the
    # ambient precision and can collapse distinct high-precision values.
    ranked = sorted(values, key=lambda item: item.symbol)
    ranked.sort(key=lambda item: item.average_dollar_volume, reverse=True)
    ranked.sort(key=lambda item: item.relative_strength_percentile, reverse=True)
    ranked.sort(key=lambda item: item.total_score, reverse=True)
    return tuple(ranked[:3])


def select_publication_roles(
    candidates: Sequence[ScoredCandidate],
    *,
    capacity_available: bool,
) -> PublicationDecision:
    """Provisionally label rank one without minting capacity authority.

    ``capacity_available`` is a caller-supplied diagnostic input.  A later
    coordinator must bind the exact ranked cohort to an issued Task 6 capacity
    decision before a :class:`PublicationDecision` can become authoritative.
    """
    if type(capacity_available) is not bool:
        raise TypeError("rank-one capacity outcome must be boolean")
    ranked = rank_candidates(candidates)
    def finish(decision: PublicationDecision) -> PublicationDecision:
        return decision

    if not ranked:
        return finish(PublicationDecision(status="NO_TRADE", candidates=(), primary=None))
    if not capacity_available:
        shadows = tuple(
            PublicationCandidate(
                rank=index,
                role="WATCHLIST_SHADOW",
                candidate=candidate,
            )
            for index, candidate in enumerate(ranked, start=1)
        )
        return finish(
            PublicationDecision(
                status="NO_PRIMARY_CAPACITY",
                candidates=shadows,
                primary=None,
            )
        )
    publications = tuple(
        PublicationCandidate(
            rank=index,
            role="PRIMARY" if index == 1 else "WATCHLIST_SHADOW",
            candidate=candidate,
        )
        for index, candidate in enumerate(ranked, start=1)
    )
    return finish(
        PublicationDecision(
            status="READY",
            candidates=publications,
            primary=publications[0],
        )
    )


__all__ = [
    "CandidateContext",
    "CatalystFactLike",
    "CohortDecision",
    "EligibilityDecision",
    "EvidenceDecisionLike",
    "InstrumentRecordLike",
    "InstrumentStatusLike",
    "MarketSessionAttestation",
    "MarketSessionAttestationLike",
    "PublicationCandidate",
    "PublicationDecision",
    "PublicationObservationManifest",
    "QuoteLike",
    "RelativeStrengthCohort",
    "ScoredCandidate",
    "ScoreCard",
    "ScreeningError",
    "SetupDecision",
    "build_base_eligible_cohort",
    "build_market_session_attestation",
    "detect_setup",
    "evaluate_eligibility",
    "is_issued_publication_decision",
    "is_issued_publication_decision_for_plan",
    "is_issued_scored_candidate",
    "midrank_percentile",
    "rank_candidates",
    "score_candidate",
    "select_publication_roles",
    "to_scored_candidate",
]
