"""Pure historical replay diagnostics with explicit evidence boundaries."""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from threading import RLock
from weakref import ReferenceType, ref

from .domain import DomainValidationError, money_from_micros, money_to_micros
from .phase1 import (
    ExitReason,
    IntradayObservation,
    SignalStatus,
    simulate_entry,
    simulate_exit,
)


class ReplayTier(str, Enum):
    """The two intentionally separate historical-replay tiers."""

    DIAGNOSTIC = "DIAGNOSTIC"
    POINT_IN_TIME = "POINT_IN_TIME"


class ReplayMechanicsStatus(str, Enum):
    """Whether available price evidence resolves execution mechanics."""

    NO_ACTION = "NO_ACTION"
    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class ReplayMechanicsEvidence(str, Enum):
    DAILY_ONLY = "DAILY_ONLY"
    ORDERED_INTRADAY = "ORDERED_INTRADAY"


class ReplayDomainComponent(str, Enum):
    """Domain reducer exercised for each historical session."""

    DAILY_PRICE = "DAILY_PRICE"
    INDICATOR = "INDICATOR"
    SIZING = "SIZING"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"


class ReplayDomainStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    UNRESOLVED = "UNRESOLVED"


class ReplayEvaluationStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    UNRESOLVED = "UNRESOLVED"


_DIAGNOSTIC_LABELS = (
    "CURRENT_LIST_SURVIVORSHIP_BIAS",
    "CURRENT_MEMBERSHIP_BIAS",
    "DIAGNOSTIC_ONLY_NOT_PERFORMANCE_VALIDATION",
)
_POINT_IN_TIME_LABELS = (
    "POINT_IN_TIME_REPLAY_REQUESTED",
    "NOT_FUTURE_PROFITABILITY_PROOF",
)
_STRICT_POINT_IN_TIME_LABELS = (
    "STRICT_POINT_IN_TIME_EVIDENCE",
    "NOT_FUTURE_PROFITABILITY_PROOF",
)
_HISTORICAL_EVIDENCE_ROLES = (
    "UNIVERSE_MEMBERSHIP",
    "EVENT_STATE",
    "SOURCE_EVIDENCE",
)


class ReplayError(ValueError):
    """One replay input cannot be evaluated without weakening its boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        self.reason_code = code
        super().__init__(code)


_HISTORICAL_REPLAY_SOURCE_LOCK = RLock()
_HistoricalReplaySourceRegistry = dict[
    int,
    tuple[ReferenceType[object], tuple[object, ...]],
]
_HISTORICAL_REPLAY_SOURCE_AUTHORITIES: _HistoricalReplaySourceRegistry = {}
_HISTORICAL_REPLAY_AUTHORITY_METHOD = (
    "_is_current_historical_replay_source"
)


def _historical_source_fingerprint_value(value: object) -> object:
    if isinstance(value, Enum):
        return (type(value).__module__, type(value).__qualname__, value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__module__,
            type(value).__qualname__,
            tuple(
                (
                    item.name,
                    _historical_source_fingerprint_value(
                        getattr(value, item.name)
                    ),
                )
                for item in fields(value)
                if item.compare
            ),
        )
    if type(value) is tuple:
        return tuple(_historical_source_fingerprint_value(item) for item in value)
    if isinstance(value, Mapping):
        return tuple(
            sorted(
                (
                    _historical_source_fingerprint_value(key),
                    _historical_source_fingerprint_value(item),
                )
                for key, item in value.items()
            )
        )
    if type(value) is Decimal:
        return ("Decimal", value.as_tuple())
    if type(value) is bytes:
        return bytes(value)
    if value is None or type(value) in {str, int, bool, date, datetime}:
        return value
    raise TypeError("unsupported historical replay source value")


def _historical_source_fingerprint(source: object) -> tuple[object, ...]:
    fingerprint = _historical_source_fingerprint_value(source)
    if type(fingerprint) is not tuple:
        raise TypeError("historical replay source fingerprint is malformed")
    return fingerprint


def _historical_source_has_current_authority(source: object) -> bool:
    journal_module = sys.modules.get("stock_monitor.journal")
    expected_type = (
        None
        if journal_module is None
        else getattr(journal_module, "HistoricalReplaySource", None)
    )
    if expected_type is None or type(source) is not expected_type:
        return False
    try:
        verifier = getattr(source, _HISTORICAL_REPLAY_AUTHORITY_METHOD)
        current = verifier() if callable(verifier) else None
    except Exception:
        return False
    return type(current) is bool and current


def _register_historical_replay_source(source: object) -> None:
    """Register one exact Journal-verified source without importing Journal."""
    if not _historical_source_has_current_authority(source):
        raise ReplayError("UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY")
    identity = id(source)

    def discard(dead: ReferenceType[object]) -> None:
        with _HISTORICAL_REPLAY_SOURCE_LOCK:
            current = _HISTORICAL_REPLAY_SOURCE_AUTHORITIES.get(identity)
            if current is not None and current[0] is dead:
                _HISTORICAL_REPLAY_SOURCE_AUTHORITIES.pop(identity, None)

    try:
        source_reference = ref(source, discard)
        fingerprint = _historical_source_fingerprint(source)
    except (TypeError, ValueError):
        raise ReplayError("INVALID_HISTORICAL_REPLAY_SOURCE") from None
    with _HISTORICAL_REPLAY_SOURCE_LOCK:
        _HISTORICAL_REPLAY_SOURCE_AUTHORITIES[identity] = (
            source_reference,
            fingerprint,
        )


def is_verified_historical_replay_source(source: object) -> bool:
    """Return whether this exact unmodified Journal-issued source is current."""
    with _HISTORICAL_REPLAY_SOURCE_LOCK:
        registered = _HISTORICAL_REPLAY_SOURCE_AUTHORITIES.get(id(source))
        if registered is None or registered[0]() is not source:
            return False
        expected_fingerprint = registered[1]
    try:
        return _historical_source_has_current_authority(source) and (
            _historical_source_fingerprint(source) == expected_fingerprint
        )
    except Exception:
        return False


def _aware(value: object, code: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ReplayError(code)
    try:
        if value.utcoffset() is None:
            raise ReplayError(code)
    except (OverflowError, ValueError):
        raise ReplayError(code) from None
    return value


@dataclass(frozen=True, slots=True)
class ReplayDateResult:
    session_date: date
    included: bool
    reason_codes: tuple[str, ...] = ()
    evaluation_status: ReplayEvaluationStatus = ReplayEvaluationStatus.PASSED
    domain_results: tuple[ReplayDomainResult, ...] = ()
    mechanics: ReplayMechanicsResult | None = None

    def __post_init__(self) -> None:
        if type(self.session_date) is not date or type(self.included) is not bool:
            raise ReplayError("INVALID_REPLAY_DATE_RESULT")
        if type(self.reason_codes) is not tuple or any(
            type(code) is not str or not code for code in self.reason_codes
        ) or len(set(self.reason_codes)) != len(self.reason_codes):
            raise ReplayError("INVALID_REPLAY_DATE_REASONS")
        if not isinstance(self.evaluation_status, ReplayEvaluationStatus):
            raise ReplayError("INVALID_REPLAY_EVALUATION_STATUS")
        if not self.included and not self.reason_codes:
            raise ReplayError("MISSING_REPLAY_EXCLUSION_REASON")
        if not self.domain_results and self.mechanics is None:
            if (
                self.included
                or self.evaluation_status
                is not ReplayEvaluationStatus.UNRESOLVED
            ):
                raise ReplayError("INVALID_REPLAY_EVALUATION_STATUS")
            return
        if type(self.domain_results) is not tuple or {
            item.component
            for item in self.domain_results
            if isinstance(item, ReplayDomainResult)
        } != set(ReplayDomainComponent) or len(self.domain_results) != len(
            ReplayDomainComponent
        ):
            raise ReplayError("INCOMPLETE_REPLAY_DOMAIN_RESULTS")
        if not isinstance(self.mechanics, ReplayMechanicsResult):
            raise ReplayError("INVALID_REPLAY_MECHANICS")
        expected_status = (
            ReplayEvaluationStatus.UNRESOLVED
            if self.mechanics.status is ReplayMechanicsStatus.UNRESOLVED
            or any(
                item.status is ReplayDomainStatus.UNRESOLVED
                for item in self.domain_results
            )
            else ReplayEvaluationStatus.FAILED
            if any(
                item.status is ReplayDomainStatus.FAILED
                for item in self.domain_results
            )
            else ReplayEvaluationStatus.PASSED
        )
        if self.evaluation_status is not expected_status:
            raise ReplayError("INVALID_REPLAY_EVALUATION_STATUS")


@dataclass(frozen=True, slots=True)
class ReplayMechanicsResult:
    status: ReplayMechanicsStatus
    reason_codes: tuple[str, ...] = ()
    exit_reason: str | None = None
    fill_price: Decimal | None = None
    exited_at: datetime | None = None
    observation_ids: tuple[str, ...] = ()
    entry_fill_price: Decimal | None = None
    reference_price: Decimal | None = None
    evidence_kind: ReplayMechanicsEvidence = (
        ReplayMechanicsEvidence.ORDERED_INTRADAY
    )
    is_exact: bool = True
    labels: tuple[str, ...] = ()
    entry_status: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, ReplayMechanicsStatus):
            raise ReplayError("INVALID_REPLAY_MECHANICS_STATUS")
        if not isinstance(self.evidence_kind, ReplayMechanicsEvidence):
            raise ReplayError("INVALID_REPLAY_MECHANICS_EVIDENCE")
        if type(self.is_exact) is not bool:
            raise ReplayError("INVALID_REPLAY_EXACTNESS")
        for field_name in ("reason_codes", "labels", "observation_ids"):
            values = getattr(self, field_name)
            if type(values) is not tuple or any(
                type(value) is not str or not value for value in values
            ) or len(set(values)) != len(values):
                raise ReplayError("INVALID_REPLAY_MECHANICS_METADATA")
        if self.exit_reason is not None and (
            type(self.exit_reason) is not str or not self.exit_reason
        ):
            raise ReplayError("INVALID_REPLAY_EXIT_REASON")
        for field_name in (
            "fill_price",
            "entry_fill_price",
            "reference_price",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _price(value, "INVALID_REPLAY_MECHANICS_PRICE"),
                )
        if self.exited_at is not None:
            object.__setattr__(
                self,
                "exited_at",
                _aware(self.exited_at, "INVALID_REPLAY_EXIT_TIME"),
            )
        if self.entry_status is not None and self.entry_status not in {
            SignalStatus.NOT_TRIGGERED.value,
            SignalStatus.NOT_FILLED_LIMIT.value,
            SignalStatus.TRIGGERED_PAPER.value,
            SignalStatus.UNRESOLVED.value,
        }:
            raise ReplayError("INVALID_REPLAY_ENTRY_STATUS")
        if self.evidence_kind is ReplayMechanicsEvidence.DAILY_ONLY and (
            self.is_exact
            or "DAILY_ONLY_NON_EXACT_EXECUTION" not in self.labels
        ):
            raise ReplayError("INVALID_REPLAY_EXACTNESS")
        if self.status is ReplayMechanicsStatus.UNRESOLVED:
            if not self.reason_codes:
                raise ReplayError("MISSING_REPLAY_MECHANICS_REASONS")
            if any(
                value is not None
                for value in (
                    self.exit_reason,
                    self.fill_price,
                    self.exited_at,
                    self.reference_price,
                )
            ):
                raise ReplayError("UNEXPECTED_REPLAY_MECHANICS_ECONOMICS")
            if self.is_exact:
                raise ReplayError("INVALID_REPLAY_EXACTNESS")
            return
        if self.reason_codes:
            raise ReplayError("UNEXPECTED_REPLAY_MECHANICS_REASONS")
        if self.status is ReplayMechanicsStatus.NO_ACTION:
            if any(
                value is not None
                for value in (
                    self.exit_reason,
                    self.fill_price,
                    self.exited_at,
                    self.entry_fill_price,
                    self.reference_price,
                )
            ) or self.observation_ids:
                raise ReplayError("UNEXPECTED_REPLAY_MECHANICS_ECONOMICS")
            return
        if not self.exit_reason:
            raise ReplayError("INCOMPLETE_REPLAY_MECHANICS")
        if self.exit_reason == "ENTRY_ONLY":
            if (
                self.entry_fill_price is None
                or not self.observation_ids
                or any(
                    value is not None
                    for value in (
                        self.fill_price,
                        self.exited_at,
                        self.reference_price,
                    )
                )
            ):
                raise ReplayError("INCOMPLETE_REPLAY_MECHANICS")
            return
        if self.fill_price is None or self.reference_price is None:
            raise ReplayError("INCOMPLETE_REPLAY_MECHANICS")
        if self.evidence_kind is ReplayMechanicsEvidence.ORDERED_INTRADAY and (
            self.exited_at is None or not self.observation_ids
        ):
            raise ReplayError("INCOMPLETE_REPLAY_MECHANICS")
        if self.exit_reason == "ENTRY_THEN_STOP_CONSERVATIVE" and (
            self.entry_fill_price is None
        ):
            raise ReplayError("INCOMPLETE_REPLAY_MECHANICS")


@dataclass(frozen=True, slots=True)
class ReplayDomainResult:
    component: ReplayDomainComponent
    status: ReplayDomainStatus
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.component, ReplayDomainComponent):
            raise ReplayError("INVALID_REPLAY_DOMAIN_COMPONENT")
        if not isinstance(self.status, ReplayDomainStatus):
            raise ReplayError("INVALID_REPLAY_DOMAIN_STATUS")
        if type(self.reason_codes) is not tuple or any(
            type(code) is not str or not code for code in self.reason_codes
        ) or len(set(self.reason_codes)) != len(self.reason_codes):
            raise ReplayError("INVALID_REPLAY_DOMAIN_REASONS")
        if self.status is ReplayDomainStatus.PASSED and self.reason_codes:
            raise ReplayError("UNEXPECTED_REPLAY_DOMAIN_REASONS")
        if self.status is not ReplayDomainStatus.PASSED and not self.reason_codes:
            raise ReplayError("MISSING_REPLAY_DOMAIN_REASONS")


@dataclass(frozen=True, slots=True)
class ReplayCase:
    session_date: date
    domain_results: tuple[ReplayDomainResult, ...]
    mechanics: ReplayMechanicsResult

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise ReplayError("INVALID_REPLAY_DATE")
        if type(self.domain_results) is not tuple or any(
            not isinstance(item, ReplayDomainResult)
            for item in self.domain_results
        ):
            raise ReplayError("INVALID_REPLAY_DOMAIN_RESULTS")
        if {item.component for item in self.domain_results} != set(
            ReplayDomainComponent
        ) or len(self.domain_results) != len(ReplayDomainComponent):
            raise ReplayError("INCOMPLETE_REPLAY_DOMAIN_RESULTS")
        if not isinstance(self.mechanics, ReplayMechanicsResult):
            raise ReplayError("INVALID_REPLAY_MECHANICS")
        object.__setattr__(
            self,
            "domain_results",
            tuple(
                sorted(
                    self.domain_results,
                    key=lambda item: list(ReplayDomainComponent).index(
                        item.component
                    ),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    cases: tuple[ReplayCase, ...]
    historical_source: object | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if type(self.cases) is not tuple or any(
            not isinstance(item, ReplayCase) for item in self.cases
        ):
            raise ReplayError("INVALID_REPLAY_CASES")
        if len({item.session_date for item in self.cases}) != len(self.cases):
            raise ReplayError("DUPLICATE_REPLAY_DATE")
        object.__setattr__(
            self,
            "cases",
            tuple(sorted(self.cases, key=lambda item: item.session_date)),
        )


def _price(value: object, code: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ReplayError(code)
    try:
        return money_from_micros(money_to_micros(value))
    except DomainValidationError:
        raise ReplayError(code) from None


def _adverse_fill(
    reference: Decimal,
    bid: Decimal,
    ask: Decimal,
    *,
    buying: bool,
) -> Decimal:
    reference_micros = money_to_micros(reference)
    spread_micros = money_to_micros(ask) - money_to_micros(bid)
    slippage_micros = max(
        (reference_micros + 999) // 1000,
        (spread_micros + 1) // 2,
    )
    return money_from_micros(
        reference_micros + slippage_micros
        if buying
        else max(0, reference_micros - slippage_micros)
    )


def replay_ambiguous_bar(
    entry: Decimal,
    stop: Decimal,
    target: Decimal,
    high: Decimal,
    low: Decimal,
) -> ReplayMechanicsResult:
    """Keep conservative daily-bar economics while labeling them non-exact."""
    entry = _price(entry, "INVALID_REPLAY_BOUNDARY")
    stop = _price(stop, "INVALID_REPLAY_BOUNDARY")
    target = _price(target, "INVALID_REPLAY_BOUNDARY")
    high = _price(high, "INVALID_REPLAY_BAR")
    low = _price(low, "INVALID_REPLAY_BAR")
    if not stop < entry < target or high < low:
        raise ReplayError("INVALID_REPLAY_BOUNDARY")
    if low <= stop and high >= target:
        return ReplayMechanicsResult(
            ReplayMechanicsStatus.RESOLVED,
            exit_reason="STOP_FIRST_CONSERVATIVE",
            fill_price=_adverse_fill(stop, stop, stop, buying=False),
            reference_price=stop,
            evidence_kind=ReplayMechanicsEvidence.DAILY_ONLY,
            is_exact=False,
            labels=(
                "DAILY_ONLY_NON_EXACT_EXECUTION",
                "CONSERVATIVE_SEQUENCE_ASSUMPTION",
            ),
        )
    if low <= stop and high >= entry:
        return ReplayMechanicsResult(
            ReplayMechanicsStatus.RESOLVED,
            exit_reason="ENTRY_THEN_STOP_CONSERVATIVE",
            fill_price=_adverse_fill(stop, stop, stop, buying=False),
            entry_fill_price=_adverse_fill(entry, entry, entry, buying=True),
            reference_price=stop,
            evidence_kind=ReplayMechanicsEvidence.DAILY_ONLY,
            is_exact=False,
            labels=(
                "DAILY_ONLY_NON_EXACT_EXECUTION",
                "CONSERVATIVE_SEQUENCE_ASSUMPTION",
            ),
        )
    return ReplayMechanicsResult(
        ReplayMechanicsStatus.NO_ACTION,
        evidence_kind=ReplayMechanicsEvidence.DAILY_ONLY,
        is_exact=False,
        labels=("DAILY_ONLY_NON_EXACT_EXECUTION",),
    )


def replay_ordered_intraday(
    stop: Decimal,
    target: Decimal,
    observations: Sequence[IntradayObservation],
    *,
    trigger: Decimal | None = None,
    maximum_entry: Decimal | None = None,
) -> ReplayMechanicsResult:
    """Replay deterministic mechanics from normalized intraday evidence."""
    items = tuple(observations)
    if any(not isinstance(item, IntradayObservation) for item in items):
        raise TypeError("observations must contain IntradayObservation values")
    if len({item.stream_id for item in items}) > 1:
        return ReplayMechanicsResult(
            ReplayMechanicsStatus.UNRESOLVED,
            ("MIXED_OBSERVATION_STREAM",),
            evidence_kind=ReplayMechanicsEvidence.ORDERED_INTRADAY,
            is_exact=False,
        )
    if (trigger is None) != (maximum_entry is None):
        raise ReplayError("INCOMPLETE_REPLAY_ENTRY_BOUNDARY")
    if trigger is not None and maximum_entry is not None:
        trigger = _price(trigger, "INVALID_REPLAY_BOUNDARY")
        maximum_entry = _price(maximum_entry, "INVALID_REPLAY_BOUNDARY")
        stop = _price(stop, "INVALID_REPLAY_BOUNDARY")
        target = _price(target, "INVALID_REPLAY_BOUNDARY")
        if not stop < trigger <= maximum_entry < target:
            raise ReplayError("INVALID_REPLAY_BOUNDARY")
        entry_result = simulate_entry(trigger, maximum_entry, items)
        if entry_result.status is SignalStatus.UNRESOLVED:
            return ReplayMechanicsResult(
                ReplayMechanicsStatus.UNRESOLVED,
                entry_result.reason_codes,
                evidence_kind=ReplayMechanicsEvidence.ORDERED_INTRADAY,
                is_exact=False,
                entry_status=entry_result.status.value,
            )
        if entry_result.status in {
            SignalStatus.NOT_TRIGGERED,
            SignalStatus.NOT_FILLED_LIMIT,
        }:
            return ReplayMechanicsResult(
                ReplayMechanicsStatus.NO_ACTION,
                entry_status=entry_result.status.value,
            )
        assert entry_result.status is SignalStatus.TRIGGERED_PAPER
        assert entry_result.fill_price is not None
        assert entry_result.trigger_observation_id is not None
        assert entry_result.quote_observation_id is not None
        ordered = tuple(
            sorted(items, key=lambda item: item.sequence)  # type: ignore[arg-type]
        )
        fill_observation = next(
            item
            for item in ordered
            if item.observation_id == entry_result.quote_observation_id
        )
        assert fill_observation.sequence is not None
        exit_facts = tuple(
            item
            for item in ordered
            if item.sequence is not None
            and item.sequence > fill_observation.sequence
            and item.kind.value == "BAR"
        )
        entry_ids = (
            entry_result.trigger_observation_id,
            entry_result.quote_observation_id,
        )
        if not exit_facts:
            return ReplayMechanicsResult(
                ReplayMechanicsStatus.RESOLVED,
                exit_reason="ENTRY_ONLY",
                observation_ids=entry_ids,
                entry_fill_price=entry_result.fill_price,
                entry_status=entry_result.status.value,
            )
        rebased_exit_facts = tuple(
            replace(item, sequence=index)
            for index, item in enumerate(exit_facts, start=1)
        )
        exit_result = simulate_exit(stop, target, rebased_exit_facts)
        if exit_result.exit_reason is ExitReason.UNRESOLVED:
            return ReplayMechanicsResult(
                ReplayMechanicsStatus.UNRESOLVED,
                exit_result.reason_codes,
                observation_ids=entry_ids,
                entry_fill_price=entry_result.fill_price,
                evidence_kind=ReplayMechanicsEvidence.ORDERED_INTRADAY,
                is_exact=False,
                entry_status=entry_result.status.value,
            )
        if exit_result.exit_reason is ExitReason.NO_EXIT:
            return ReplayMechanicsResult(
                ReplayMechanicsStatus.RESOLVED,
                exit_reason="ENTRY_ONLY",
                observation_ids=entry_ids,
                entry_fill_price=entry_result.fill_price,
                entry_status=entry_result.status.value,
            )
        assert exit_result.fill_price is not None
        assert exit_result.exited_at is not None
        assert exit_result.observation_id is not None
        if exit_result.exit_reason is ExitReason.GAP_STOP:
            reference_price = next(
                item.open_price
                for item in exit_facts
                if item.observation_id == exit_result.observation_id
            )
        elif exit_result.exit_reason in {
            ExitReason.STOP,
            ExitReason.STOP_FIRST_CONSERVATIVE,
        }:
            reference_price = stop
        else:
            reference_price = target
        assert reference_price is not None
        return ReplayMechanicsResult(
            ReplayMechanicsStatus.RESOLVED,
            exit_reason=exit_result.exit_reason.value,
            fill_price=exit_result.fill_price,
            exited_at=exit_result.exited_at,
            observation_ids=(*entry_ids, exit_result.observation_id),
            entry_fill_price=entry_result.fill_price,
            reference_price=reference_price,
            entry_status=entry_result.status.value,
        )

    result = simulate_exit(stop, target, items)
    if result.exit_reason is ExitReason.UNRESOLVED:
        return ReplayMechanicsResult(
            ReplayMechanicsStatus.UNRESOLVED,
            result.reason_codes,
            evidence_kind=ReplayMechanicsEvidence.ORDERED_INTRADAY,
            is_exact=False,
        )
    if result.exit_reason is ExitReason.NO_EXIT:
        return ReplayMechanicsResult(ReplayMechanicsStatus.NO_ACTION)
    assert result.fill_price is not None
    assert result.exited_at is not None
    assert result.observation_id is not None
    if result.exit_reason is ExitReason.GAP_STOP:
        reference_price = next(
            item.open_price
            for item in observations
            if item.observation_id == result.observation_id
        )
    elif result.exit_reason in {
        ExitReason.STOP,
        ExitReason.STOP_FIRST_CONSERVATIVE,
    }:
        reference_price = stop
    else:
        reference_price = target
    assert reference_price is not None
    return ReplayMechanicsResult(
        ReplayMechanicsStatus.RESOLVED,
        exit_reason=result.exit_reason.value,
        fill_price=result.fill_price,
        exited_at=result.exited_at,
        observation_ids=(result.observation_id,),
        reference_price=reference_price,
    )


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Coverage and mandatory disclosures for one replay run."""

    tier: ReplayTier
    total_dates: int
    included_dates: int
    excluded_dates: int
    labels: tuple[str, ...]
    results: tuple[ReplayDateResult, ...] = ()
    exclusion_counts: tuple[tuple[str, int], ...] = ()
    evaluation_counts: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.tier, ReplayTier):
            raise ReplayError("INVALID_REPLAY_TIER")
        if any(
            type(value) is not int or value < 0
            for value in (
                self.total_dates,
                self.included_dates,
                self.excluded_dates,
            )
        ):
            raise ReplayError("INVALID_REPLAY_COUNTS")
        if (
            self.total_dates != self.included_dates + self.excluded_dates
            or type(self.results) is not tuple
            or any(not isinstance(item, ReplayDateResult) for item in self.results)
            or len(self.results) != self.total_dates
            or sum(item.included for item in self.results)
            != self.included_dates
            or len({item.session_date for item in self.results})
            != len(self.results)
        ):
            raise ReplayError("INVALID_REPLAY_COUNTS")
        if type(self.labels) is not tuple or any(
            type(label) is not str or not label for label in self.labels
        ) or len(set(self.labels)) != len(self.labels):
            raise ReplayError("INVALID_REPLAY_LABELS")
        if self.tier is ReplayTier.DIAGNOSTIC:
            if self.labels != _DIAGNOSTIC_LABELS:
                raise ReplayError("INVALID_REPLAY_LABELS")
            if self.excluded_dates:
                raise ReplayError("INVALID_REPLAY_COUNTS")
        elif self.labels not in {
            _POINT_IN_TIME_LABELS,
            _STRICT_POINT_IN_TIME_LABELS,
        } or self.labels == _POINT_IN_TIME_LABELS and self.included_dates:
            raise ReplayError("INVALID_REPLAY_LABELS")
        expected_exclusions: dict[str, int] = {}
        for result in self.results:
            if result.included:
                continue
            for reason in result.reason_codes:
                expected_exclusions[reason] = (
                    expected_exclusions.get(reason, 0) + 1
                )
        if self.exclusion_counts != tuple(sorted(expected_exclusions.items())):
            raise ReplayError("INVALID_REPLAY_EXCLUSION_COUNTS")
        expected_evaluations: dict[str, int] = {}
        for result in self.results:
            key = result.evaluation_status.value
            expected_evaluations[key] = expected_evaluations.get(key, 0) + 1
        if self.evaluation_counts != tuple(sorted(expected_evaluations.items())):
            raise ReplayError("INVALID_REPLAY_EVALUATION_COUNTS")


def _evaluate_case(case: ReplayCase) -> ReplayDateResult:
    reasons = tuple(
        dict.fromkeys(
            (
                *(
                    code
                    for result in case.domain_results
                    for code in result.reason_codes
                ),
                *case.mechanics.reason_codes,
            )
        )
    )
    if (
        case.mechanics.status is ReplayMechanicsStatus.UNRESOLVED
        or any(
            result.status is ReplayDomainStatus.UNRESOLVED
            for result in case.domain_results
        )
    ):
        status = ReplayEvaluationStatus.UNRESOLVED
    elif any(
        result.status is ReplayDomainStatus.FAILED
        for result in case.domain_results
    ):
        status = ReplayEvaluationStatus.FAILED
    else:
        status = ReplayEvaluationStatus.PASSED
    return ReplayDateResult(
        session_date=case.session_date,
        included=True,
        reason_codes=reasons,
        evaluation_status=status,
        domain_results=case.domain_results,
        mechanics=case.mechanics,
    )


def replay_diagnostic(request: ReplayRequest) -> ReplayResult:
    """Evaluate current-list cases while disclosing their diagnostic limit."""
    if not isinstance(request, ReplayRequest):
        raise TypeError("request must be a ReplayRequest")
    results = tuple(_evaluate_case(case) for case in request.cases)
    counts: dict[str, int] = {}
    for result in results:
        counts[result.evaluation_status.value] = (
            counts.get(result.evaluation_status.value, 0) + 1
        )
    return ReplayResult(
        tier=ReplayTier.DIAGNOSTIC,
        total_dates=len(results),
        included_dates=len(results),
        excluded_dates=0,
        labels=_DIAGNOSTIC_LABELS,
        results=results,
        evaluation_counts=tuple(sorted(counts.items())),
    )


def replay_point_in_time(
    request: ReplayRequest,
) -> ReplayResult:
    """Evaluate only cases freshly rederived by an exact Journal source."""
    if not isinstance(request, ReplayRequest):
        raise TypeError("request must be a ReplayRequest")
    source = request.historical_source
    if source is None or not is_verified_historical_replay_source(source):
        authority_reason = (
            "MISSING_HISTORICAL_REPLAY_AUTHORITY"
            if source is None
            else "UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY"
        )
        results = tuple(
            ReplayDateResult(
                session_date=case.session_date,
                included=False,
                reason_codes=(authority_reason,),
                evaluation_status=ReplayEvaluationStatus.UNRESOLVED,
            )
            for case in request.cases
        )
    else:
        date_sources = getattr(source, "date_sources", ())
        if type(date_sources) is not tuple:
            date_sources = ()
        requested_dates = tuple(case.session_date for case in request.cases)
        source_dates = tuple(
            getattr(date_source, "session_date", None)
            for date_source in date_sources
        )
        expected_date_count = getattr(source, "expected_date_count", None)
        coverage_mismatch = (
            type(expected_date_count) is not int
            or expected_date_count != len(requested_dates)
            or len(date_sources) != len(requested_dates)
            or any(type(source_date) is not date for source_date in source_dates)
            or len(set(source_dates)) != len(source_dates)
            or set(source_dates) != set(requested_dates)
        )
        by_date = {
            source_date: date_source
            for source_date, date_source in zip(source_dates, date_sources)
            if type(source_date) is date
        }
        result_items: list[ReplayDateResult] = []
        for requested_case in request.cases:
            if coverage_mismatch:
                result_items.append(
                    ReplayDateResult(
                        session_date=requested_case.session_date,
                        included=False,
                        reason_codes=(
                            "HISTORICAL_REPLAY_DATE_COVERAGE_MISMATCH",
                        ),
                        evaluation_status=(
                            ReplayEvaluationStatus.UNRESOLVED
                        ),
                    )
                )
                continue
            date_source = by_date[requested_case.session_date]
            evidence_sources = getattr(date_source, "evidence_sources", ())
            if type(evidence_sources) is not tuple:
                evidence_sources = ()
            observed_roles = {
                (
                    role.value
                    if isinstance(role, Enum)
                    else role
                )
                for evidence_source in evidence_sources
                if type(
                    role := getattr(evidence_source, "role", None)
                ) is str
                or isinstance(role, Enum)
            }
            missing_role_reasons = tuple(
                f"MISSING_{role}"
                for role in _HISTORICAL_EVIDENCE_ROLES
                if role not in observed_roles
            )
            if missing_role_reasons:
                result_items.append(
                    ReplayDateResult(
                        session_date=requested_case.session_date,
                        included=False,
                        reason_codes=missing_role_reasons,
                        evaluation_status=(
                            ReplayEvaluationStatus.UNRESOLVED
                        ),
                    )
                )
                continue
            try:
                rederive = getattr(
                    date_source,
                    "_rederive_historical_replay_case",
                )
                authoritative_case = (
                    rederive() if callable(rederive) else None
                )
            except Exception:
                authoritative_case = None
            if (
                not isinstance(authoritative_case, ReplayCase)
                or authoritative_case.session_date
                != requested_case.session_date
            ):
                result_items.append(
                    ReplayDateResult(
                        session_date=requested_case.session_date,
                        included=False,
                        reason_codes=(
                            "HISTORICAL_REPLAY_CASE_REDERIVATION_FAILED",
                        ),
                        evaluation_status=(
                            ReplayEvaluationStatus.UNRESOLVED
                        ),
                    )
                )
                continue
            evaluated = _evaluate_case(authoritative_case)
            if authoritative_case != requested_case:
                evaluated = replace(
                    evaluated,
                    included=False,
                    reason_codes=("HISTORICAL_REPLAY_CASE_MISMATCH",),
                )
            result_items.append(evaluated)
        results = tuple(result_items)
    evaluation_counts: dict[str, int] = {}
    exclusion_counts: dict[str, int] = {}
    for result in results:
        key = result.evaluation_status.value
        evaluation_counts[key] = evaluation_counts.get(key, 0) + 1
        if not result.included:
            for reason in result.reason_codes:
                exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1
    included_dates = sum(result.included for result in results)
    return ReplayResult(
        tier=ReplayTier.POINT_IN_TIME,
        total_dates=len(results),
        included_dates=included_dates,
        excluded_dates=len(results) - included_dates,
        labels=(
            _STRICT_POINT_IN_TIME_LABELS
            if results and included_dates == len(results)
            else _POINT_IN_TIME_LABELS
        ),
        results=results,
        exclusion_counts=tuple(sorted(exclusion_counts.items())),
        evaluation_counts=tuple(sorted(evaluation_counts.items())),
    )
