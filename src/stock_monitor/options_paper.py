"""Pure, source-bound Phase 2 long-call selection and paper accounting.

This module deliberately has no persistence, provider, network, or brokerage
behavior.  Journal/config adapters may issue the immutable authorities it
consumes, while every transition here remains a deterministic domain decision.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import Enum
from hashlib import sha256
from threading import RLock
from urllib.parse import parse_qs, urlsplit
from weakref import ReferenceType, ref

from .domain import DomainValidationError, money_from_micros, money_to_micros


_ZERO = Decimal("0")
_MINIMUM_DELTA = Decimal("0.30")
_MAXIMUM_DELTA = Decimal("0.40")
_TARGET_DELTA = Decimal("0.35")
_MAXIMUM_RELATIVE_SPREAD = Decimal("0.10")
_MAXIMUM_INITIAL_RISK_MICROS = 50_000_000
_MINIMUM_OPEN_INTEREST = 1_000
_MINIMUM_DAILY_VOLUME = 100
_MINIMUM_DTE = 30
_MAXIMUM_DTE = 60
_TARGET_DTE = 45
_EVENT_EXCLUSION_SESSIONS = 10
_CONTRACT_MULTIPLIER = 100
_BULLISH_LONG_CALL = "BULLISH_LONG_CALL"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_OCC = re.compile(
    r"(?P<root>[A-Z]{1,6})(?P<expiry>[0-9]{6})"
    r"(?P<right>[CP])(?P<strike>[0-9]{8})\Z"
)


class OptionPaperError(ValueError):
    """A Phase 2 paper-option invariant failed closed."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class PaperOnlyBoundaryError(OptionPaperError):
    """An option action is intentionally outside paper-only Phase 2."""


class OptionWindowStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    RESTART_REQUIRED = "RESTART_REQUIRED"


def _aware(value: object, code: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OptionPaperError(code)
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError):
        offset = None
    if offset is None:
        raise OptionPaperError(code)
    return value


def _sha256(value: object, code: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise OptionPaperError(code)
    return value


def _text(value: object, code: str) -> str:
    if type(value) is not str or not value:
        raise OptionPaperError(code)
    return value


def _decimal(value: object, code: str, *, optional: bool = False) -> Decimal | None:
    if value is None and optional:
        return None
    if type(value) is not Decimal or not value.is_finite():
        raise OptionPaperError(code)
    return value


def _money_micros(
    value: object,
    code: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> int:
    if type(value) is not Decimal or not value.is_finite():
        raise OptionPaperError(code)
    try:
        micros = money_to_micros(value)
    except DomainValidationError:
        raise OptionPaperError(code) from None
    if positive and micros <= 0:
        raise OptionPaperError(code)
    if nonnegative and micros < 0:
        raise OptionPaperError(code)
    return micros


def _canonical_money(
    value: object,
    code: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    return money_from_micros(
        _money_micros(
            value,
            code,
            positive=positive,
            nonnegative=nonnegative,
        )
    )


def _canonical_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


def _digest(*parts: object) -> str:
    material = b"stock-monitor/options-paper/v1"
    for part in parts:
        if isinstance(part, datetime):
            encoded = _canonical_timestamp(part).encode("ascii")
        elif isinstance(part, date):
            encoded = part.isoformat().encode("ascii")
        elif isinstance(part, Enum):
            encoded = str(part.value).encode("utf-8")
        else:
            encoded = str(part).encode("utf-8")
        material += b"\0" + encoded
    return sha256(material).hexdigest()


def _action_occ_symbol(action: object) -> str | None:
    """Read the canonical OCC identity already reparsed by Journal."""
    try:
        details_json = action.details_json
        if (
            type(details_json) is not str
            or sha256(details_json.encode("utf-8")).hexdigest()
            != action.details_sha256
        ):
            return None
        details = json.loads(details_json)
        if (
            not isinstance(details, dict)
            or json.dumps(
                details,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            != details_json
        ):
            return None
        normalized = details.get("normalized")
        if not isinstance(normalized, dict):
            return None
        value = normalized.get("occ_symbol")
        if type(value) is not str or _OCC.fullmatch(value) is None:
            return None
        return value
    except Exception:
        return None


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Phase2Authorization:
    promotion_decision: object
    window_source: object
    signal_source: object
    signal: object
    strategy: str
    issued_at: datetime
    source_digest: str

    def __post_init__(self) -> None:
        _text(self.strategy, "INVALID_PHASE2_AUTHORIZATION")
        _aware(self.issued_at, "INVALID_PHASE2_AUTHORIZATION")
        _sha256(self.source_digest, "INVALID_PHASE2_AUTHORIZATION")


_PHASE2_AUTHORIZATIONS: dict[
    int,
    tuple[
        ReferenceType[Phase2Authorization],
        tuple[object, ...],
        ReferenceType[object],
        ReferenceType[object],
        ReferenceType[object],
        object,
    ],
] = {}
_PHASE2_AUTHORIZATIONS_LOCK = RLock()


def _authorization_fingerprint(
    authorization: Phase2Authorization,
) -> tuple[object, ...]:
    return (
        id(authorization.promotion_decision),
        id(authorization.window_source),
        id(authorization.signal_source),
        id(authorization.signal),
        authorization.strategy,
        authorization.issued_at,
        authorization.source_digest,
    )


def _authorization_material_reason(
    promotion_decision: object,
    window_source: object,
    signal_source: object,
    *,
    signal: object,
    strategy: object,
    issued_at: object,
) -> str | None:
    from .journal import (
        Phase1SignalSource,
        Phase1ValidationWindowSource,
        Phase2WindowSource,
        is_verified_phase1_signal_source,
        is_verified_phase2_window_source,
        phase2_sources_share_owner,
    )
    from .validation import (
        PromotionDecision,
        PromotionStatus,
        is_issued_promotion_decision,
    )
    from .phase1 import Signal, SignalStatus

    if (
        not isinstance(promotion_decision, PromotionDecision)
        or not is_issued_promotion_decision(promotion_decision)
        or not promotion_decision.passed
        or promotion_decision.status is not PromotionStatus.PASSED
    ):
        return "PHASE1_PROMOTION_UNVERIFIED"
    if not isinstance(signal_source, Phase1SignalSource) or not (
        is_verified_phase1_signal_source(signal_source)
    ):
        return "PHASE1_SIGNAL_UNVERIFIED"
    if not isinstance(window_source, Phase2WindowSource) or not (
        is_verified_phase2_window_source(window_source)
    ):
        return "PHASE2_WINDOW_UNVERIFIED"
    if (
        not isinstance(signal, Signal)
        or signal.signal_id != signal_source.signal_id
        or signal.symbol != signal_source.symbol
        or signal.role != signal_source.role
        or signal.publication_session != signal_source.publication_session
        or signal.published_at != signal_source.published_at
        or signal.status is not SignalStatus.PUBLISHED
        or signal.history
    ):
        return "PHASE2_SIGNAL_MISMATCH"
    source = promotion_decision._phase1_source
    if not isinstance(source, Phase1ValidationWindowSource):
        return "PHASE1_PROMOTION_UNVERIFIED"
    if (
        window_source.promotion_source is not source
        or window_source.promotion_decision_digest
        != promotion_decision.authority_digest
        or window_source.validation_window_id != source.validation_window_id
        or window_source.promotion_query_cutoff != source.query_cutoff
        or window_source.promotion_signal_ids
        != tuple(item.signal_id for item in source.signal_sources)
    ):
        return "PHASE2_WINDOW_PROMOTION_MISMATCH"
    if not phase2_sources_share_owner(window_source, signal_source):
        return "PHASE2_SOURCE_OWNER_MISMATCH"
    if signal_source.validation_window_id != window_source.validation_window_id:
        return "PHASE2_VALIDATION_WINDOW_MISMATCH"
    if any(
        signal_source.signal_id == historical_signal_id
        for historical_signal_id in window_source.promotion_signal_ids
    ):
        return "PHASE2_SIGNAL_ALREADY_IN_PROMOTION_WINDOW"
    try:
        issued = _aware(issued_at, "INVALID_PHASE2_AUTHORIZATION")
    except OptionPaperError:
        return "INVALID_PHASE2_AUTHORIZATION"
    if (
        signal_source.published_at <= window_source.promotion_query_cutoff
        or signal_source.received_at <= window_source.promotion_query_cutoff
        or signal_source.published_at <= window_source.started_at
        or signal_source.received_at <= window_source.started_at
        or signal_source.published_at > signal_source.received_at
        or signal_source.received_at > signal_source.query_cutoff
        or window_source.query_cutoff > issued
        or signal_source.query_cutoff > issued
    ):
        return "PHASE2_SIGNAL_NOT_AFTER_PROMOTION"
    if signal_source.role != "PRIMARY":
        return "PHASE2_REQUIRES_PRIMARY_SIGNAL"
    if strategy != _BULLISH_LONG_CALL:
        return "PHASE2_REQUIRES_BULLISH_LONG_CALL"
    return None


def _authorization_digest(
    promotion_decision: object,
    window_source: object,
    signal_source: object,
    signal: object,
    strategy: str,
    issued_at: datetime,
) -> str:
    return _digest(
        "authorization",
        promotion_decision.source_digest,
        promotion_decision.authority_digest,
        window_source.source_digest,
        window_source.window_id,
        window_source.started_at,
        signal_source.source_digest,
        signal_source.signal_id,
        signal_source.validation_window_id,
        signal.signal_id,
        signal.symbol,
        signal.role,
        signal.publication_session,
        signal.published_at,
        strategy,
        issued_at,
    )


def _issue_phase2_authorization(
    *,
    promotion_decision: object,
    window_source: object,
    signal_source: object,
    issued_at: datetime,
    strategy: str = _BULLISH_LONG_CALL,
    signal: object | None = None,
) -> Phase2Authorization:
    """Bind one new post-promotion PRIMARY to the exact passed authority."""
    if signal is None:
        from .phase1 import Signal

        try:
            signal = Signal(
                signal_id=signal_source.signal_id,
                symbol=signal_source.symbol,
                role=signal_source.role,
                publication_session=signal_source.publication_session,
                published_at=signal_source.published_at,
            )
        except (AttributeError, TypeError, ValueError):
            raise OptionPaperError("PHASE1_SIGNAL_UNVERIFIED") from None
    reason = _authorization_material_reason(
        promotion_decision,
        window_source,
        signal_source,
        signal=signal,
        strategy=strategy,
        issued_at=issued_at,
    )
    if reason is not None:
        raise OptionPaperError(reason)
    authorization = Phase2Authorization(
        promotion_decision=promotion_decision,
        window_source=window_source,
        signal_source=signal_source,
        signal=signal,
        strategy=strategy,
        issued_at=issued_at,
        source_digest=_authorization_digest(
            promotion_decision,
            window_source,
            signal_source,
            signal,
            strategy,
            issued_at,
        ),
    )
    identity = id(authorization)
    signal_identity = id(signal)

    def discard(dead: ReferenceType[Phase2Authorization]) -> None:
        with _PHASE2_AUTHORIZATIONS_LOCK:
            current = _PHASE2_AUTHORIZATIONS.get(identity)
            if current is not None and current[0] is dead:
                _PHASE2_AUTHORIZATIONS.pop(identity, None)
                bound = _AUTHORIZED_PHASE2_SIGNALS.get(signal_identity)
                if bound is not None and bound[1]() is None:
                    _AUTHORIZED_PHASE2_SIGNALS.pop(signal_identity, None)

    with _PHASE2_AUTHORIZATIONS_LOCK:
        _PHASE2_AUTHORIZATIONS[identity] = (
            ref(authorization, discard),
            _authorization_fingerprint(authorization),
            ref(promotion_decision),
            ref(window_source),
            ref(signal_source),
            signal,
        )
        _AUTHORIZED_PHASE2_SIGNALS[signal_identity] = (
            signal,
            ref(authorization),
            _signal_fingerprint(signal),
        )
    return authorization


def is_issued_phase2_authorization(authorization: object) -> bool:
    """Return true only for the exact still-current authorization identity."""
    if not isinstance(authorization, Phase2Authorization):
        return False
    try:
        reason = _authorization_material_reason(
            authorization.promotion_decision,
            authorization.window_source,
            authorization.signal_source,
            signal=authorization.signal,
            strategy=authorization.strategy,
            issued_at=authorization.issued_at,
        )
        digest = _authorization_digest(
            authorization.promotion_decision,
            authorization.window_source,
            authorization.signal_source,
            authorization.signal,
            authorization.strategy,
            authorization.issued_at,
        )
        fingerprint = _authorization_fingerprint(authorization)
    except Exception:
        return False
    if reason is not None or authorization.source_digest != digest:
        return False
    with _PHASE2_AUTHORIZATIONS_LOCK:
        issued = _PHASE2_AUTHORIZATIONS.get(id(authorization))
        bound_signal = _AUTHORIZED_PHASE2_SIGNALS.get(id(authorization.signal))
        return (
            issued is not None
            and issued[0]() is authorization
            and issued[1] == fingerprint
            and issued[2]() is authorization.promotion_decision
            and issued[3]() is authorization.window_source
            and issued[4]() is authorization.signal_source
            and issued[5] is authorization.signal
            and bound_signal is not None
            and bound_signal[0] is authorization.signal
            and bound_signal[1]() is authorization
            and bound_signal[2]
            == _signal_fingerprint(authorization.signal)
        )


_AUTHORIZED_PHASE2_SIGNALS: dict[
    int,
    tuple[object, ReferenceType[Phase2Authorization], tuple[object, ...]],
] = {}


def _signal_fingerprint(signal: object) -> tuple[object, ...]:
    return (
        signal.signal_id,
        signal.symbol,
        signal.role,
        signal.publication_session,
        signal.published_at,
        signal.status,
        signal.history,
    )


def is_authorized_phase2_signal(signal: object) -> bool:
    """Return true only for the exact Signal minted by a current authority."""
    from .phase1 import Signal

    if not isinstance(signal, Signal):
        return False
    try:
        fingerprint = _signal_fingerprint(signal)
    except Exception:
        return False
    with _PHASE2_AUTHORIZATIONS_LOCK:
        issued = _AUTHORIZED_PHASE2_SIGNALS.get(id(signal))
        if (
            issued is None
            or issued[0] is not signal
            or issued[2] != fingerprint
        ):
            return False
        authorization = issued[1]()
    return (
        authorization is not None
        and authorization.signal is signal
        and is_issued_phase2_authorization(authorization)
    )


@dataclass(frozen=True, slots=True)
class ProviderOptionFacts:
    """Overlapping Alpaca proposal facts; open interest is manual-only.

    The provider contract intentionally exposes ``open_interest`` as unavailable,
    so Robinhood's reviewed integer lives only on :class:`OptionContract` and is
    independently gated instead of being invented for a false equality check.
    """

    occ_symbol: str
    underlying: str
    expiration: date
    strike: Decimal
    option_type: str
    delta: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    daily_volume: int | None
    observed_at: datetime
    source_observation_id: str

    def __post_init__(self) -> None:
        _text(self.occ_symbol, "INVALID_PROVIDER_OPTION_FACTS")
        _text(self.underlying, "INVALID_PROVIDER_OPTION_FACTS")
        if type(self.expiration) is not date:
            raise OptionPaperError("INVALID_PROVIDER_OPTION_FACTS")
        _decimal(self.strike, "INVALID_PROVIDER_OPTION_FACTS")
        _text(self.option_type, "INVALID_PROVIDER_OPTION_FACTS")
        _decimal(self.delta, "INVALID_PROVIDER_OPTION_FACTS", optional=True)
        _decimal(self.bid, "INVALID_PROVIDER_OPTION_FACTS", optional=True)
        _decimal(self.ask, "INVALID_PROVIDER_OPTION_FACTS", optional=True)
        if self.daily_volume is not None and type(self.daily_volume) is not int:
            raise OptionPaperError("INVALID_PROVIDER_OPTION_FACTS")
        _aware(self.observed_at, "INVALID_PROVIDER_OPTION_FACTS")
        _text(self.source_observation_id, "INVALID_PROVIDER_OPTION_FACTS")


@dataclass(frozen=True, slots=True)
class OptionContract:
    """One provider proposal plus the exact manual Robinhood confirmation."""

    occ_symbol: str
    underlying: str
    expiration: date
    strike: Decimal
    option_type: str
    delta: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    open_interest: int | None
    daily_volume: int | None
    observed_at: datetime
    manual_source_id: str
    provider_facts: ProviderOptionFacts

    def __post_init__(self) -> None:
        _text(self.occ_symbol, "INVALID_OPTION_CONTRACT")
        _text(self.underlying, "INVALID_OPTION_CONTRACT")
        if type(self.expiration) is not date:
            raise OptionPaperError("INVALID_OPTION_CONTRACT")
        _decimal(self.strike, "INVALID_OPTION_CONTRACT")
        _text(self.option_type, "INVALID_OPTION_CONTRACT")
        _decimal(self.delta, "INVALID_OPTION_CONTRACT", optional=True)
        _decimal(self.bid, "INVALID_OPTION_CONTRACT", optional=True)
        _decimal(self.ask, "INVALID_OPTION_CONTRACT", optional=True)
        if self.open_interest is not None and type(self.open_interest) is not int:
            raise OptionPaperError("INVALID_OPTION_CONTRACT")
        if self.daily_volume is not None and type(self.daily_volume) is not int:
            raise OptionPaperError("INVALID_OPTION_CONTRACT")
        _aware(self.observed_at, "INVALID_OPTION_CONTRACT")
        _text(self.manual_source_id, "INVALID_OPTION_CONTRACT")
        if not isinstance(self.provider_facts, ProviderOptionFacts):
            raise OptionPaperError("INVALID_OPTION_CONTRACT")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class _ReviewedOptionChain(Sequence[OptionContract]):
    """Identity-bound complete chain derived from durable provider/review sources."""

    contracts: tuple[OptionContract, ...]
    authorization: Phase2Authorization = field(repr=False, compare=False)
    chain_source: object = field(repr=False, compare=False)
    manual_review_sources: tuple[object, ...] = field(
        repr=False,
        compare=False,
    )
    stage: str
    source_digest: str

    def __post_init__(self) -> None:
        if type(self.contracts) is not tuple or any(
            not isinstance(contract, OptionContract)
            for contract in self.contracts
        ):
            raise OptionPaperError("INVALID_OPTION_CHAIN")
        if not isinstance(self.authorization, Phase2Authorization):
            raise OptionPaperError("INVALID_OPTION_CHAIN")
        if type(self.manual_review_sources) is not tuple:
            raise OptionPaperError("INVALID_OPTION_CHAIN")
        if self.stage not in {"REVIEWED", "ELIGIBLE"}:
            raise OptionPaperError("INVALID_OPTION_CHAIN")
        _sha256(self.source_digest, "INVALID_OPTION_CHAIN")

    def __len__(self) -> int:
        return len(self.contracts)

    def __getitem__(self, index: object) -> object:
        return self.contracts[index]  # type: ignore[index]


_REVIEWED_OPTION_CHAINS: dict[
    int,
    tuple[
        ReferenceType[_ReviewedOptionChain],
        tuple[object, ...],
        object,
        tuple[object, ...],
        _ReviewedOptionChain | None,
    ],
] = {}
_REVIEWED_OPTION_CHAINS_LOCK = RLock()


def _option_contract_fingerprint(contract: OptionContract) -> tuple[object, ...]:
    provider = contract.provider_facts
    return (
        contract.occ_symbol,
        contract.underlying,
        contract.expiration,
        contract.strike,
        contract.option_type,
        contract.delta,
        contract.bid,
        contract.ask,
        contract.open_interest,
        contract.daily_volume,
        contract.observed_at,
        contract.manual_source_id,
        provider.occ_symbol,
        provider.underlying,
        provider.expiration,
        provider.strike,
        provider.option_type,
        provider.delta,
        provider.bid,
        provider.ask,
        provider.daily_volume,
        provider.observed_at,
        provider.source_observation_id,
    )


def _reviewed_chain_fingerprint(
    chain: _ReviewedOptionChain,
) -> tuple[object, ...]:
    return (
        tuple(id(contract) for contract in chain.contracts),
        tuple(_option_contract_fingerprint(contract) for contract in chain.contracts),
        id(chain.authorization),
        id(chain.chain_source),
        tuple(id(source) for source in chain.manual_review_sources),
        chain.stage,
        chain.source_digest,
    )


def _reviewed_chain_sources_are_current(
    chain: _ReviewedOptionChain,
) -> bool:
    try:
        from .journal import (
            Phase2ManualOptionReviewSource,
            Phase2OptionChainSource,
            is_verified_journal_action_source,
            is_verified_phase2_authorization_source,
            is_verified_phase2_manual_option_review_source,
            is_verified_phase2_option_chain_source,
            phase2_sources_share_owner,
        )

        chain_source = chain.chain_source
        authorization = chain.authorization
        if not isinstance(chain_source, Phase2OptionChainSource) or not (
            is_verified_phase2_option_chain_source(chain_source)
        ):
            return False
        authorization_source = chain_source.authorization_source
        if (
            not is_verified_phase2_authorization_source(
                authorization_source
            )
            or authorization_source.window_source
            is not authorization.window_source
            or authorization_source.signal_source
            is not authorization.signal_source
            or authorization_source.authorization_digest
            != authorization.source_digest
            or authorization_source.authorized_at != authorization.issued_at
            or not phase2_sources_share_owner(
                chain_source,
                authorization.window_source,
            )
            or not phase2_sources_share_owner(
                chain_source,
                authorization.signal_source,
            )
        ):
            return False
        if len(chain.manual_review_sources) != len(
            chain_source.review_candidate_fact_digests
        ):
            return False
        for source in chain.manual_review_sources:
            if (
                not isinstance(source, Phase2ManualOptionReviewSource)
                or not is_verified_phase2_manual_option_review_source(source)
                or not phase2_sources_share_owner(chain_source, source)
                or not is_verified_journal_action_source(source.action_source)
                or not phase2_sources_share_owner(
                    chain_source,
                    source.action_source,
                )
            ):
                return False
        return True
    except Exception:
        return False


def _reviewed_chain_is_current(
    chain: object,
    *,
    authorization: Phase2Authorization,
    stage: str,
) -> bool:
    if (
        not isinstance(chain, _ReviewedOptionChain)
        or chain.authorization is not authorization
        or chain.stage != stage
        or not is_issued_phase2_authorization(authorization)
    ):
        return False
    try:
        fingerprint = _reviewed_chain_fingerprint(chain)
    except Exception:
        return False
    with _REVIEWED_OPTION_CHAINS_LOCK:
        issued = _REVIEWED_OPTION_CHAINS.get(id(chain))
        registry_current = (
            issued is not None
            and issued[0]() is chain
            and issued[1] == fingerprint
            and issued[2] is chain.chain_source
            and len(issued[3]) == len(chain.manual_review_sources)
            and all(
                registered is current
                for registered, current in zip(
                    issued[3],
                    chain.manual_review_sources,
                    strict=True,
                )
            )
        )
        parent = None if issued is None else issued[4]
    if not registry_current or not _reviewed_chain_sources_are_current(chain):
        return False
    return stage == "REVIEWED" or (
        parent is not None
        and _reviewed_chain_is_current(
            parent,
            authorization=authorization,
            stage="REVIEWED",
        )
    )


def _provider_fact_is_review_candidate(
    fact: object,
    *,
    authorization: Phase2Authorization,
) -> bool:
    try:
        match = _OCC.fullmatch(fact.occ_symbol)
        if match is None or match.group("right") != "C":
            return False
        if (
            fact.underlying != authorization.signal_source.symbol
            or match.group("root") != fact.underlying
            or datetime.strptime(match.group("expiry"), "%y%m%d").date()
            != fact.expiration
            or int(match.group("strike")) != fact.strike_micros // 1_000
            or fact.strike_micros % 1_000 != 0
            or fact.delta_micros is None
            or not 300_000 <= fact.delta_micros <= 400_000
            or fact.bid_micros is None
            or fact.ask_micros is None
            or fact.bid_micros <= 0
            or fact.ask_micros <= 0
            or fact.bid_micros > fact.ask_micros
            or fact.daily_volume is None
            or fact.daily_volume < _MINIMUM_DAILY_VOLUME
            or fact.observed_at is None
            or fact.observed_at < authorization.issued_at
        ):
            return False
        dte = (
            fact.expiration
            - authorization.signal_source.publication_session
        ).days
        if not _MINIMUM_DTE <= dte <= _MAXIMUM_DTE:
            return False
        with localcontext() as context:
            context.prec = 60
            relative_spread = Decimal(
                fact.ask_micros - fact.bid_micros
            ) / (
                Decimal(fact.ask_micros + fact.bid_micros)
                / Decimal("2")
            )
        return relative_spread <= _MAXIMUM_RELATIVE_SPREAD
    except Exception:
        return False


def _provider_snapshot_matches_fact(snapshot: object, fact: object) -> bool:
    """Bind the opaque provider value to every durable normalized field."""
    try:
        observed_at = snapshot.observed_at
        if observed_at is not None:
            normalized_observed_at = _aware(
                observed_at,
                "OPTION_CHAIN_SOURCE_INCOMPLETE",
            ).astimezone(UTC)
            observed_text = normalized_observed_at.isoformat(
                timespec=(
                    "microseconds"
                    if normalized_observed_at.microsecond
                    else "seconds"
                )
            ).replace("+00:00", "Z")
        else:
            observed_text = None
        snapshot_delta_micros = (
            None
            if snapshot.delta is None
            else int(snapshot.delta * Decimal(1_000_000))
        )
        if (
            snapshot.delta is not None
            and snapshot.delta * Decimal(1_000_000)
            != Decimal(snapshot_delta_micros)
        ):
            return False
        payload = {
            "fact": {
                "kind": "OPTION_SNAPSHOT",
                "occ_symbol": snapshot.occ_symbol,
                "underlying": snapshot.underlying,
                "expiration": snapshot.expiration.isoformat(),
                "strike": str(snapshot.strike),
                "delta": (
                    None if snapshot.delta is None else str(snapshot.delta)
                ),
                "bid": None if snapshot.bid is None else str(snapshot.bid),
                "ask": None if snapshot.ask is None else str(snapshot.ask),
                "daily_volume": snapshot.daily_volume,
                "open_interest": snapshot.open_interest,
                "feed": snapshot.feed,
                "observed_at": observed_text,
                "source_observation_id": snapshot.source_observation_id,
            },
            "page_ordinal": fact.fetch_page_ordinal,
            "source_item_ordinal": fact.source_item_ordinal,
            "source_item_path": fact.source_item_path,
        }
        normalized_fields_digest = sha256(
            json.dumps(
                {
                    "namespace": "stock-monitor/alpaca-normalized-fields/v1",
                    "payload": payload,
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return (
            snapshot.occ_symbol == fact.occ_symbol
            and snapshot.underlying == fact.underlying
            and snapshot.expiration == fact.expiration
            and money_to_micros(snapshot.strike) == fact.strike_micros
            and snapshot_delta_micros == fact.delta_micros
            and (
                None
                if snapshot.bid is None
                else money_to_micros(snapshot.bid)
            )
            == fact.bid_micros
            and (
                None
                if snapshot.ask is None
                else money_to_micros(snapshot.ask)
            )
            == fact.ask_micros
            and snapshot.daily_volume == fact.daily_volume
            and snapshot.open_interest is None
            and snapshot.feed == "indicative"
            and snapshot.observed_at == fact.observed_at
            and snapshot.source_observation_id
            == fact.external_source_observation_id
            and normalized_fields_digest == fact.provider_fact_digest
        )
    except Exception:
        return False


def _derive_reviewed_option_chain(
    chain_source: object,
    manual_review_sources: Sequence[object],
    authorization: Phase2Authorization,
) -> Sequence[OptionContract]:
    """Derive a complete reviewed cohort from exact current Journal sources."""
    from .journal import (
        Phase2ManualOptionReviewSource,
        Phase2OptionChainSource,
        is_verified_journal_action_source,
        is_verified_phase2_authorization_source,
        is_verified_phase2_manual_option_review_source,
        is_verified_phase2_option_chain_fact_source,
        is_verified_phase2_option_chain_page_source,
        is_verified_phase2_option_chain_source,
        phase2_sources_share_owner,
    )

    if not is_issued_phase2_authorization(authorization):
        raise OptionPaperError("PHASE2_AUTHORIZATION_UNVERIFIED")
    if not isinstance(chain_source, Phase2OptionChainSource) or not (
        is_verified_phase2_option_chain_source(chain_source)
    ):
        raise OptionPaperError("OPTION_CHAIN_AUTHORITY_UNVERIFIED")
    authorization_source = chain_source.authorization_source
    if (
        not is_verified_phase2_authorization_source(authorization_source)
        or authorization_source.window_source is not authorization.window_source
        or authorization_source.signal_source is not authorization.signal_source
        or authorization_source.authorization_digest
        != authorization.source_digest
        or authorization_source.authorized_at != authorization.issued_at
        or not phase2_sources_share_owner(
            chain_source,
            authorization.window_source,
        )
        or not phase2_sources_share_owner(
            chain_source,
            authorization.signal_source,
        )
    ):
        raise OptionPaperError("OPTION_CHAIN_AUTHORIZATION_MISMATCH")
    if isinstance(manual_review_sources, (str, bytes)) or not isinstance(
        manual_review_sources,
        Sequence,
    ):
        raise OptionPaperError("OPTION_CHAIN_REVIEW_INCOMPLETE")
    reviews = tuple(manual_review_sources)
    if any(
        not isinstance(source, Phase2ManualOptionReviewSource)
        or not is_verified_phase2_manual_option_review_source(source)
        or not phase2_sources_share_owner(chain_source, source)
        or not is_verified_journal_action_source(source.action_source)
        or not phase2_sources_share_owner(chain_source, source.action_source)
        for source in reviews
    ):
        raise OptionPaperError("OPTION_REVIEW_AUTHORITY_UNVERIFIED")
    pages = chain_source.pages
    facts = chain_source.facts
    if (
        chain_source.collection_name != "snapshots"
        or chain_source.requested_symbols
        != (authorization.signal_source.symbol,)
        or chain_source.underlying != authorization.signal_source.symbol
        or not chain_source.terminal
        or chain_source.expected_page_count != len(pages)
        or chain_source.expected_fact_count != len(facts)
        or chain_source.expected_manual_review_count
        != len(chain_source.review_candidate_fact_digests)
        or tuple(page.page_ordinal for page in pages)
        != tuple(range(1, len(pages) + 1))
        or not pages
        or pages[0].request_page_token is not None
        or pages[-1].next_page_token is not None
        or any(
            page.chain_set_id != chain_source.chain_set_id
            or not is_verified_phase2_option_chain_page_source(page)
            or not phase2_sources_share_owner(chain_source, page)
            or page.retrieved_at > chain_source.query_cutoff
            or (
                index > 0
                and pages[index - 1].next_page_token
                != page.request_page_token
            )
            for index, page in enumerate(pages)
        )
        or chain_source.query_cutoff > chain_source.received_at
    ):
        raise OptionPaperError("OPTION_CHAIN_SOURCE_INCOMPLETE")
    provider_snapshots = tuple(chain_source.provider_chain)
    if len(provider_snapshots) != len(facts):
        raise OptionPaperError("OPTION_CHAIN_SOURCE_INCOMPLETE")
    if any(
        not is_verified_phase2_option_chain_fact_source(fact)
        or not phase2_sources_share_owner(chain_source, fact)
        or fact.chain_set_id != chain_source.chain_set_id
        or fact.authorization_id != authorization_source.authorization_id
        or fact.underlying != chain_source.underlying
        or fact.received_at > chain_source.query_cutoff
        or fact.snapshot is not provider_snapshot
        or fact.fetch_page_ordinal < 1
        or fact.fetch_page_ordinal > len(pages)
        or fact.source_item_ordinal < 1
        or type(fact.source_item_path) is not str
        or not fact.source_item_path
        or fact.source_observation_row_id
        != pages[fact.fetch_page_ordinal - 1].source_observation_row_id
        or fact.external_source_observation_id
        != pages[fact.fetch_page_ordinal - 1].external_source_observation_id
        or fact.payload_sha256
        != pages[fact.fetch_page_ordinal - 1].payload_sha256
        or not _provider_snapshot_matches_fact(provider_snapshot, fact)
        for fact, provider_snapshot in zip(
            facts,
            provider_snapshots,
            strict=True,
        )
    ):
        raise OptionPaperError("OPTION_CHAIN_SOURCE_INCOMPLETE")
    candidate_facts = tuple(
        fact
        for fact in facts
        if _provider_fact_is_review_candidate(
            fact,
            authorization=authorization,
        )
    )
    candidate_digests = tuple(
        fact.provider_fact_digest for fact in candidate_facts
    )
    if (
        candidate_digests != chain_source.review_candidate_fact_digests
        or len(set(candidate_digests)) != len(candidate_digests)
        or len(reviews) != len(candidate_facts)
        or chain_source.expected_manual_review_count != len(reviews)
    ):
        raise OptionPaperError("OPTION_CHAIN_REVIEW_INCOMPLETE")
    reviews_by_fact: dict[int, Phase2ManualOptionReviewSource] = {}
    for review in reviews:
        fact_identity = id(review.provider_fact_source)
        if fact_identity in reviews_by_fact:
            raise OptionPaperError("OPTION_CHAIN_REVIEW_INCOMPLETE")
        reviews_by_fact[fact_identity] = review
    if set(reviews_by_fact) != {id(fact) for fact in candidate_facts}:
        raise OptionPaperError("OPTION_CHAIN_REVIEW_INCOMPLETE")
    contracts: list[OptionContract] = []
    ordered_reviews: list[Phase2ManualOptionReviewSource] = []
    for fact in candidate_facts:
        review = reviews_by_fact[id(fact)]
        if (
            review.provider_fact_source is not fact
            or review.authorization_id != authorization_source.authorization_id
            or review.chain_set_id != chain_source.chain_set_id
            or review.action_source.domain_kind != "OPTION_PAPER_REVIEW"
            or review.action_source.event_time != review.observed_at
            or review.action_source.bid_micros != review.bid_micros
            or review.action_source.ask_micros != review.ask_micros
            or _action_occ_symbol(review.action_source) != review.occ_symbol
            or review.received_at < review.observed_at
            or fact.observed_at is None
            or fact.observed_at > review.observed_at
            or review.occ_symbol != fact.occ_symbol
            or review.underlying != fact.underlying
            or review.expiration != fact.expiration
            or review.strike_micros != fact.strike_micros
            or review.delta_micros != fact.delta_micros
            or review.bid_micros != fact.bid_micros
            or review.ask_micros != fact.ask_micros
            or review.daily_volume != fact.daily_volume
        ):
            raise OptionPaperError("PROVIDER_MANUAL_MISMATCH")
        provider_facts = ProviderOptionFacts(
            occ_symbol=fact.occ_symbol,
            underlying=fact.underlying,
            expiration=fact.expiration,
            strike=money_from_micros(fact.strike_micros),
            option_type="CALL",
            delta=Decimal(fact.delta_micros) / Decimal(1_000_000),
            bid=money_from_micros(fact.bid_micros),
            ask=money_from_micros(fact.ask_micros),
            daily_volume=fact.daily_volume,
            observed_at=fact.observed_at,
            source_observation_id=fact.external_source_observation_id,
        )
        contracts.append(
            OptionContract(
                occ_symbol=review.occ_symbol,
                underlying=review.underlying,
                expiration=review.expiration,
                strike=money_from_micros(review.strike_micros),
                option_type="CALL",
                delta=Decimal(review.delta_micros) / Decimal(1_000_000),
                bid=money_from_micros(review.bid_micros),
                ask=money_from_micros(review.ask_micros),
                open_interest=review.open_interest,
                daily_volume=review.daily_volume,
                observed_at=review.observed_at,
                manual_source_id=review.snapshot_id,
                provider_facts=provider_facts,
            )
        )
        ordered_reviews.append(review)
    reviewed = _ReviewedOptionChain(
        contracts=tuple(contracts),
        authorization=authorization,
        chain_source=chain_source,
        manual_review_sources=tuple(ordered_reviews),
        stage="REVIEWED",
        source_digest=_digest(
            "reviewed-option-chain",
            authorization.source_digest,
            chain_source.source_digest,
            *(source.source_digest for source in ordered_reviews),
        ),
    )
    identity = id(reviewed)

    def discard(dead: ReferenceType[_ReviewedOptionChain]) -> None:
        with _REVIEWED_OPTION_CHAINS_LOCK:
            current = _REVIEWED_OPTION_CHAINS.get(identity)
            if current is not None and current[0] is dead:
                _REVIEWED_OPTION_CHAINS.pop(identity, None)

    with _REVIEWED_OPTION_CHAINS_LOCK:
        _REVIEWED_OPTION_CHAINS[identity] = (
            ref(reviewed, discard),
            _reviewed_chain_fingerprint(reviewed),
            chain_source,
            tuple(ordered_reviews),
            None,
        )
    return reviewed


@dataclass(frozen=True, slots=True, weakref_slot=True)
class OptionSelection:
    authorization: Phase2Authorization
    contract: OptionContract
    eligible_chain: _ReviewedOptionChain = field(repr=False, compare=False)
    fee_schedule: object
    event_exclusion_source: object
    portfolio_source: object
    selection_source: object | None = field(repr=False, compare=False)
    calendar_digest: str
    selected_at: datetime
    quantity: int
    dte: int
    reviewed_ask: Decimal
    entry_fee: Decimal
    reserved_exit_fee: Decimal
    reviewed_initial_risk: Decimal
    source_digest: str
    _calendar_resolver: object = field(compare=False, repr=False)


_OPTION_SELECTIONS: dict[
    int,
    tuple[ReferenceType[OptionSelection], tuple[object, ...]],
] = {}
_OPTION_SELECTIONS_LOCK = RLock()


def _fee_schedule_fields(schedule: object) -> tuple[object, ...] | None:
    try:
        from .config import FeeSchedule, is_reviewed_fee_schedule

        if not isinstance(schedule, FeeSchedule) or not is_reviewed_fee_schedule(
            schedule
        ):
            return None
        fields = (
            schedule.schedule_id,
            schedule.effective_session,
            schedule.reviewed_at,
            schedule.currency,
            schedule.contract_multiplier,
            schedule.entry_fee_per_contract_micros,
            schedule.exit_fee_per_contract_micros,
            schedule.close_fee_reserve_per_contract_micros,
            schedule.source_sha256,
            schedule.digest,
        )
    except (AttributeError, ImportError, TypeError, ValueError):
        return None
    if (
        type(fields[0]) is not str
        or not fields[0]
        or type(fields[1]) is not date
        or not isinstance(fields[2], datetime)
        or fields[2].tzinfo is None
        or fields[3] != "USD"
        or fields[4] != _CONTRACT_MULTIPLIER
        or any(type(value) is not int or value < 0 for value in fields[5:8])
        or fields[7] < fields[6]
        or type(fields[8]) is not str
        or _SHA256.fullmatch(fields[8]) is None
        or type(fields[9]) is not str
        or _SHA256.fullmatch(fields[9]) is None
    ):
        return None
    return fields


def _event_window_reason(
    event_exclusion_source: object,
    *,
    authorization: Phase2Authorization,
    calendar_resolver: object,
) -> str | None:
    from .journal import (
        Phase2EventExclusionSource,
        is_verified_phase1_signal_evidence_source,
        is_verified_phase2_event_exclusion_source,
        phase2_sources_share_owner,
    )
    from .risk import SessionCalendarResolver

    if not isinstance(event_exclusion_source, Phase2EventExclusionSource) or not (
        is_verified_phase2_event_exclusion_source(event_exclusion_source)
    ):
        return "EVENT_EXCLUSION_UNVERIFIED"
    if (
        event_exclusion_source.signal_source is not authorization.signal_source
        or not is_verified_phase1_signal_evidence_source(
            event_exclusion_source.signal_evidence_source
        )
        or not phase2_sources_share_owner(
            event_exclusion_source,
            event_exclusion_source.signal_evidence_source,
        )
        or not phase2_sources_share_owner(
            authorization.window_source,
            event_exclusion_source,
        )
    ):
        return "EVENT_EXCLUSION_SOURCE_MISMATCH"
    if not isinstance(calendar_resolver, SessionCalendarResolver) or not (
        calendar_resolver.release_verified
    ):
        return "PHASE2_CALENDAR_UNVERIFIED"
    from .risk import _calendar_digest

    if _calendar_digest(calendar_resolver) != event_exclusion_source.calendar_digest:
        return "PHASE2_CALENDAR_UNVERIFIED"
    signal_session = authorization.signal_source.publication_session
    try:
        expected = tuple(
            calendar_resolver.add_sessions(signal_session, offset)
            for offset in range(_EVENT_EXCLUSION_SESSIONS)
        )
    except Exception:
        return "EVENT_EXCLUSION_INCOMPLETE"
    if (
        event_exclusion_source.covered_sessions != expected
        or len(event_exclusion_source.covered_sessions)
        != _EVENT_EXCLUSION_SESSIONS
        or tuple(sorted(set(event_exclusion_source.event_sessions)))
        != event_exclusion_source.event_sessions
        or any(
            session not in event_exclusion_source.covered_sessions
            for session in event_exclusion_source.event_sessions
        )
        or event_exclusion_source.reviewed_at
        > event_exclusion_source.query_cutoff
    ):
        return "EVENT_EXCLUSION_INCOMPLETE"
    return None


def _portfolio_reason(
    portfolio_source: object,
    *,
    authorization: Phase2Authorization,
    event_exclusion_source: object,
) -> str | None:
    from .journal import (
        Phase2PortfolioSource,
        is_verified_phase2_portfolio_source,
        phase2_portfolio_source_binds_window,
        phase2_sources_share_owner,
    )

    if not isinstance(portfolio_source, Phase2PortfolioSource) or not (
        is_verified_phase2_portfolio_source(portfolio_source)
    ):
        return "PHASE2_PORTFOLIO_UNVERIFIED"
    if (
        portfolio_source.window_id != authorization.window_source.window_id
        or not phase2_portfolio_source_binds_window(
            portfolio_source,
            authorization.window_source,
        )
        or not phase2_sources_share_owner(
            event_exclusion_source,
            portfolio_source,
        )
    ):
        return "PHASE2_PORTFOLIO_SOURCE_MISMATCH"
    if (
        type(portfolio_source.settled_cash_micros) is not int
        or portfolio_source.settled_cash_micros < 0
        or portfolio_source.as_of > portfolio_source.query_cutoff
        or portfolio_source.query_cutoff < authorization.issued_at
        or portfolio_source.query_cutoff
        < event_exclusion_source.query_cutoff
    ):
        return "PHASE2_PORTFOLIO_CHRONOLOGY_INVALID"
    return None


def _provider_manual_match(contract: OptionContract) -> bool:
    provider = contract.provider_facts
    return (
        provider.occ_symbol == contract.occ_symbol
        and provider.underlying == contract.underlying
        and provider.expiration == contract.expiration
        and provider.strike == contract.strike
        and provider.option_type == contract.option_type
        and provider.delta == contract.delta
        and provider.bid == contract.bid
        and provider.ask == contract.ask
        and provider.daily_volume == contract.daily_volume
    )


def _occ_matches(contract: OptionContract) -> bool:
    match = _OCC.fullmatch(contract.occ_symbol)
    if match is None:
        return False
    try:
        encoded_expiration = datetime.strptime(
            match.group("expiry"),
            "%y%m%d",
        ).date()
    except ValueError:
        return False
    with localcontext() as context:
        context.prec = 60
        encoded_strike = contract.strike * Decimal("1000")
    if encoded_strike != encoded_strike.to_integral_value():
        return False
    strike_integer = int(encoded_strike)
    if not 0 <= strike_integer <= 99_999_999:
        return False
    expected_right = "C" if contract.option_type == "CALL" else "P"
    return (
        match.group("root") == contract.underlying
        and encoded_expiration == contract.expiration
        and match.group("right") == expected_right
        and int(match.group("strike")) == strike_integer
    )


def _relative_spread(contract: OptionContract) -> Decimal | None:
    if contract.bid is None or contract.ask is None:
        return None
    if contract.bid <= _ZERO or contract.ask <= _ZERO or contract.bid > contract.ask:
        return None
    with localcontext() as context:
        context.prec = 60
        midpoint = (contract.bid + contract.ask) / Decimal("2")
        return (contract.ask - contract.bid) / midpoint


def _is_canonical_positive_premium(value: Decimal) -> bool:
    try:
        return _money_micros(
            value,
            "INVALID_OPTION_QUOTE",
            positive=True,
        ) > 0
    except OptionPaperError:
        return False


def option_contract_reason_codes(
    contract: OptionContract,
    *,
    authorization: Phase2Authorization,
    fee_schedule: object,
    event_exclusion_source: object,
    calendar_resolver: object,
    portfolio_source: object,
) -> tuple[str, ...]:
    """Return all deterministic ineligibility codes for one contract."""
    if not isinstance(contract, OptionContract):
        raise OptionPaperError("INVALID_OPTION_CONTRACT")
    if not is_issued_phase2_authorization(authorization):
        raise OptionPaperError("PHASE2_AUTHORIZATION_UNVERIFIED")
    signal = authorization.signal_source
    event_reason = _event_window_reason(
        event_exclusion_source,
        authorization=authorization,
        calendar_resolver=calendar_resolver,
    )
    if event_reason is not None:
        raise OptionPaperError(event_reason)
    portfolio_reason = _portfolio_reason(
        portfolio_source,
        authorization=authorization,
        event_exclusion_source=event_exclusion_source,
    )
    if portfolio_reason is not None:
        raise OptionPaperError(portfolio_reason)
    fee_fields = _fee_schedule_fields(fee_schedule)
    reasons: list[str] = []
    if fee_fields is None:
        reasons.append("FEE_SCHEDULE_UNREVIEWED")
    if not _provider_manual_match(contract):
        reasons.append("PROVIDER_MANUAL_MISMATCH")
    if contract.underlying != signal.symbol:
        reasons.append("UNDERLYING_MISMATCH")
    if contract.option_type != "CALL":
        reasons.append("PHASE2_REQUIRES_BULLISH_LONG_CALL")
    if not _occ_matches(contract):
        reasons.append("INVALID_OCC_SYMBOL")
    dte = (contract.expiration - signal.publication_session).days
    if not _MINIMUM_DTE <= dte <= _MAXIMUM_DTE:
        reasons.append("OPTION_DTE_OUTSIDE_30_60")
    if (
        contract.delta is None
        or not _MINIMUM_DELTA <= contract.delta <= _MAXIMUM_DELTA
    ):
        reasons.append("OPTION_DELTA_OUTSIDE_0_30_0_40")
    if (
        contract.open_interest is None
        or contract.open_interest < _MINIMUM_OPEN_INTEREST
    ):
        reasons.append("OPTION_OPEN_INTEREST_BELOW_1000")
    if (
        contract.daily_volume is None
        or contract.daily_volume < _MINIMUM_DAILY_VOLUME
    ):
        reasons.append("OPTION_DAILY_VOLUME_BELOW_100")
    spread = _relative_spread(contract)
    if (
        spread is None
        or contract.bid is None
        or contract.ask is None
        or not _is_canonical_positive_premium(contract.bid)
        or not _is_canonical_positive_premium(contract.ask)
    ):
        reasons.append("INVALID_OPTION_QUOTE")
    elif spread > _MAXIMUM_RELATIVE_SPREAD:
        reasons.append("OPTION_SPREAD_ABOVE_10_PERCENT")
    if portfolio_source.open_position_source is not None:
        reasons.append("OPTION_POSITION_ALREADY_OPEN")
    if any(
        event_session in event_exclusion_source.covered_sessions
        for event_session in event_exclusion_source.event_sessions
    ):
        reasons.append("EVENT_WITHIN_TEN_SESSIONS")
    if contract.observed_at < authorization.issued_at:
        reasons.append("MANUAL_OPTION_FACTS_PRECEDE_AUTHORIZATION")
    if (
        contract.provider_facts.observed_at < authorization.issued_at
        or contract.provider_facts.observed_at > contract.observed_at
    ):
        reasons.append("PROVIDER_OPTION_FACTS_TIME_INVALID")
    if event_exclusion_source.reviewed_at > contract.observed_at:
        reasons.append("EVENT_EXCLUSION_NOT_REVIEWED_BEFORE_ENTRY")
    if contract.observed_at >= portfolio_source.query_cutoff:
        reasons.append("OPTION_REVIEW_NOT_BEFORE_SELECTION")
    if fee_fields is not None:
        effective_session = fee_fields[1]
        reviewed_at = fee_fields[2]
        if (
            effective_session > signal.publication_session
            or reviewed_at > contract.observed_at
        ):
            reasons.append("FEE_SCHEDULE_NOT_EFFECTIVE")
        if contract.ask is not None:
            try:
                initial_risk_micros = (
                    _money_micros(
                        contract.ask,
                        "INVALID_OPTION_QUOTE",
                        positive=True,
                    )
                    * _CONTRACT_MULTIPLIER
                    + fee_fields[5]
                    + fee_fields[7]
                )
            except OptionPaperError:
                pass
            else:
                if initial_risk_micros > _MAXIMUM_INITIAL_RISK_MICROS:
                    reasons.append("OPTION_INITIAL_RISK_ABOVE_50")
                if initial_risk_micros > portfolio_source.settled_cash_micros:
                    reasons.append("OPTION_SETTLED_CASH_INSUFFICIENT")
    return tuple(dict.fromkeys(reasons))


def eligible_option_contracts(
    chain: Sequence[OptionContract],
    *,
    authorization: Phase2Authorization,
    fee_schedule: object,
    event_exclusion_source: object,
    calendar_resolver: object,
    portfolio_source: object,
) -> Sequence[OptionContract]:
    if not _reviewed_chain_is_current(
        chain,
        authorization=authorization,
        stage="REVIEWED",
    ):
        raise OptionPaperError("OPTION_CHAIN_AUTHORITY_UNVERIFIED")
    contracts = tuple(chain)
    if any(not isinstance(contract, OptionContract) for contract in contracts):
        raise OptionPaperError("INVALID_OPTION_CHAIN")
    symbols = tuple(contract.occ_symbol for contract in contracts)
    if len(symbols) != len(set(symbols)):
        raise OptionPaperError("DUPLICATE_OPTION_CONTRACT")
    if any(not _provider_manual_match(contract) for contract in contracts):
        raise OptionPaperError("PROVIDER_MANUAL_MISMATCH")
    fee_fields = _fee_schedule_fields(fee_schedule)
    eligible_contracts = tuple(
        contract
        for contract in contracts
        if not option_contract_reason_codes(
            contract,
            authorization=authorization,
            fee_schedule=fee_schedule,
            event_exclusion_source=event_exclusion_source,
            calendar_resolver=calendar_resolver,
            portfolio_source=portfolio_source,
        )
    )
    eligible = _ReviewedOptionChain(
        contracts=eligible_contracts,
        authorization=authorization,
        chain_source=chain.chain_source,
        manual_review_sources=chain.manual_review_sources,
        stage="ELIGIBLE",
        source_digest=_digest(
            "eligible-option-chain",
            chain.source_digest,
            "UNREVIEWED" if fee_fields is None else fee_fields[9],
            event_exclusion_source.source_digest,
            portfolio_source.source_digest,
            portfolio_source.authority_digest,
            *(
                contract.occ_symbol
                for contract in eligible_contracts
            ),
        ),
    )
    identity = id(eligible)

    def discard(dead: ReferenceType[_ReviewedOptionChain]) -> None:
        with _REVIEWED_OPTION_CHAINS_LOCK:
            current = _REVIEWED_OPTION_CHAINS.get(identity)
            if current is not None and current[0] is dead:
                _REVIEWED_OPTION_CHAINS.pop(identity, None)

    with _REVIEWED_OPTION_CHAINS_LOCK:
        _REVIEWED_OPTION_CHAINS[identity] = (
            ref(eligible, discard),
            _reviewed_chain_fingerprint(eligible),
            eligible.chain_source,
            eligible.manual_review_sources,
            chain,
        )
    return eligible


def option_sort_key(
    contract: OptionContract,
    signal_session: date,
) -> tuple[object, ...]:
    """Return the complete locked deterministic option ranking key."""
    if not isinstance(contract, OptionContract) or type(signal_session) is not date:
        raise OptionPaperError("INVALID_OPTION_SORT_INPUT")
    spread = _relative_spread(contract)
    if spread is None or contract.delta is None:
        raise OptionPaperError("INELIGIBLE_OPTION_SORT_INPUT")
    dte = (contract.expiration - signal_session).days
    with localcontext() as context:
        context.prec = 60
        delta_distance = abs(contract.delta - _TARGET_DELTA)
    return (
        abs(dte - _TARGET_DTE),
        -contract.expiration.toordinal(),
        delta_distance,
        spread,
        -int(contract.open_interest),  # eligibility proved non-None
        -int(contract.daily_volume),  # eligibility proved non-None
        contract.occ_symbol,
    )


def rank_option_contracts(
    chain: Sequence[OptionContract],
    signal: object,
) -> tuple[OptionContract, ...]:
    """Rank a prevalidated chain for one exact authorized Phase 2 Signal."""
    if not isinstance(chain, _ReviewedOptionChain):
        raise OptionPaperError("OPTION_CHAIN_AUTHORITY_UNVERIFIED")
    authorization = chain.authorization
    if not _reviewed_chain_is_current(
        chain,
        authorization=authorization,
        stage="ELIGIBLE",
    ):
        raise OptionPaperError("OPTION_CHAIN_AUTHORITY_UNVERIFIED")
    if authorization.signal is not signal or not is_authorized_phase2_signal(
        signal
    ):
        raise OptionPaperError("PHASE2_SIGNAL_AUTHORIZATION_UNVERIFIED")
    contracts = tuple(chain)
    if any(not isinstance(contract, OptionContract) for contract in contracts):
        raise OptionPaperError("INVALID_OPTION_CHAIN")
    symbols = tuple(contract.occ_symbol for contract in contracts)
    if len(symbols) != len(set(symbols)):
        raise OptionPaperError("DUPLICATE_OPTION_CONTRACT")
    return tuple(
        sorted(
            contracts,
            key=lambda contract: option_sort_key(
                contract,
                signal.publication_session,
            ),
        )
    )


def _selection_fingerprint(selection: OptionSelection) -> tuple[object, ...]:
    contract = selection.contract
    provider = contract.provider_facts
    return (
        id(selection.authorization),
        id(selection.eligible_chain),
        selection.eligible_chain.source_digest,
        contract.occ_symbol,
        contract.underlying,
        contract.expiration,
        contract.strike,
        contract.option_type,
        contract.delta,
        contract.bid,
        contract.ask,
        contract.open_interest,
        contract.daily_volume,
        contract.observed_at,
        contract.manual_source_id,
        provider.occ_symbol,
        provider.underlying,
        provider.expiration,
        provider.strike,
        provider.option_type,
        provider.delta,
        provider.bid,
        provider.ask,
        provider.daily_volume,
        provider.observed_at,
        provider.source_observation_id,
        id(selection.fee_schedule),
        id(selection.event_exclusion_source),
        id(selection.portfolio_source),
        id(selection.selection_source),
        selection.calendar_digest,
        selection.selected_at,
        selection.quantity,
        selection.dte,
        selection.reviewed_ask,
        selection.entry_fee,
        selection.reserved_exit_fee,
        selection.reviewed_initial_risk,
        selection.source_digest,
        id(selection._calendar_resolver),
    )


def _selection_digest(
    authorization: Phase2Authorization,
    contract: OptionContract,
    eligible_chain: _ReviewedOptionChain,
    fee_schedule: object,
    event_exclusion_source: object,
    portfolio_source: object,
    calendar_digest: str,
    selected_at: datetime,
) -> str:
    fields = _fee_schedule_fields(fee_schedule)
    if fields is None:
        raise OptionPaperError("FEE_SCHEDULE_UNREVIEWED")
    return _digest(
        "selection",
        authorization.source_digest,
        eligible_chain.source_digest,
        contract.occ_symbol,
        contract.expiration,
        contract.strike,
        contract.delta,
        contract.bid,
        contract.ask,
        contract.open_interest,
        contract.daily_volume,
        contract.observed_at,
        contract.manual_source_id,
        contract.provider_facts.source_observation_id,
        fields[9],
        event_exclusion_source.source_digest,
        portfolio_source.source_digest,
        portfolio_source.authority_digest,
        calendar_digest,
        selected_at,
    )


def _authorization_for_source(
    source: object,
    *,
    calendar_resolver: object | None = None,
) -> Phase2Authorization | None:
    """Resolve an already-issued domain authority from its exact Journal source."""
    try:
        from .journal import (
            Phase2AuthorizationSource,
            is_verified_phase2_authorization_source,
            phase2_sources_share_owner,
        )

        if not isinstance(source, Phase2AuthorizationSource) or not (
            is_verified_phase2_authorization_source(source)
        ):
            return None
    except Exception:
        return None
    with _PHASE2_AUTHORIZATIONS_LOCK:
        candidates = tuple(
            issued[0]() for issued in _PHASE2_AUTHORIZATIONS.values()
        )
    matching: list[Phase2Authorization] = []
    for authorization in candidates:
        if not isinstance(authorization, Phase2Authorization):
            continue
        try:
            if (
                is_issued_phase2_authorization(authorization)
                and source.window_source is authorization.window_source
                and source.signal_source is authorization.signal_source
                and source.authorized_at == authorization.issued_at
                and source.authorization_digest == authorization.source_digest
                and phase2_sources_share_owner(
                    source,
                    authorization.window_source,
                )
                and phase2_sources_share_owner(
                    source,
                    authorization.signal_source,
                )
            ):
                matching.append(authorization)
        except Exception:
            continue
    if len(matching) == 1:
        return matching[0]
    if matching or calendar_resolver is None:
        return None
    try:
        from .validation import _issue_phase1_promotion_from_journal_source

        promotion = _issue_phase1_promotion_from_journal_source(
            source.window_source.promotion_source,
            calendar_resolver=calendar_resolver,
        )
        if (
            promotion.authority_digest
            != source.window_source.promotion_decision_digest
            or source.received_at < source.authorized_at
        ):
            return None
        authorization = _issue_phase2_authorization(
            promotion_decision=promotion,
            window_source=source.window_source,
            signal_source=source.signal_source,
            issued_at=source.authorized_at,
        )
    except Exception:
        return None
    if authorization.source_digest != source.authorization_digest:
        return None
    return authorization


def _durable_selection_source_is_current(selection: OptionSelection) -> bool:
    source = selection.selection_source
    if source is None:
        return False
    try:
        from .journal import (
            Phase2SelectionSource,
            is_verified_phase2_authorization_source,
            is_verified_phase2_event_exclusion_source,
            is_verified_phase2_fee_schedule_source,
            is_verified_phase2_manual_option_review_source,
            is_verified_phase2_option_chain_fact_source,
            is_verified_phase2_option_chain_source,
            is_verified_phase2_portfolio_source,
            is_verified_phase2_selection_source,
            phase2_sources_share_owner,
        )

        authorization = selection.authorization
        selected_review = source.selected_manual_review_source
        fee_fields = _fee_schedule_fields(selection.fee_schedule)
        if fee_fields is None:
            return False
        nested_sources = (
            source.authorization_source,
            source.option_chain_source,
            source.provider_fact_source,
            source.selected_manual_review_source,
            source.fee_schedule_source,
            source.event_exclusion_source,
            source.portfolio_source,
            *source.manual_review_sources,
        )
        if (
            not isinstance(source, Phase2SelectionSource)
            or not is_verified_phase2_selection_source(source)
            or not is_verified_phase2_authorization_source(
                source.authorization_source
            )
            or not is_verified_phase2_option_chain_source(
                source.option_chain_source
            )
            or not is_verified_phase2_option_chain_fact_source(
                source.provider_fact_source
            )
            or not is_verified_phase2_fee_schedule_source(
                source.fee_schedule_source
            )
            or not is_verified_phase2_event_exclusion_source(
                source.event_exclusion_source
            )
            or not is_verified_phase2_portfolio_source(source.portfolio_source)
            or any(
                not is_verified_phase2_manual_option_review_source(review)
                for review in source.manual_review_sources
            )
            or any(
                not phase2_sources_share_owner(source, nested)
                for nested in nested_sources
            )
        ):
            return False
        if (
            source.authorization_source.window_source
            is not authorization.window_source
            or source.authorization_source.signal_source
            is not authorization.signal_source
            or source.authorization_source.authorization_digest
            != authorization.source_digest
            or source.authorization_source.authorized_at
            != authorization.issued_at
            or source.option_chain_source is not selection.eligible_chain.chain_source
            or len(source.manual_review_sources)
            != len(selection.eligible_chain.manual_review_sources)
            or any(
                persisted is not current
                for persisted, current in zip(
                    source.manual_review_sources,
                    selection.eligible_chain.manual_review_sources,
                    strict=True,
                )
            )
            or source.event_exclusion_source
            is not selection.event_exclusion_source
            or source.portfolio_source is not selection.portfolio_source
            or source.quantity != selection.quantity
            or source.quantity != 1
            or source.selected_at != selection.selected_at
            or source.received_at < source.selected_at
            or source.selection_session
            != authorization.signal_source.publication_session
            or source.ranking_digest != selection.source_digest
            or selected_review not in source.manual_review_sources
            or source.provider_fact_source
            is not selected_review.provider_fact_source
            or selection.contract.manual_source_id != selected_review.snapshot_id
            or selection.contract.occ_symbol != selected_review.occ_symbol
            or selection.contract.observed_at != selected_review.observed_at
            or selected_review.observed_at >= source.selected_at
            or source.option_chain_source.query_cutoff > selected_review.observed_at
            or source.portfolio_source.query_cutoff > source.selected_at
            or source.fee_schedule_source.schedule_id != fee_fields[0]
            or source.fee_schedule_source.effective_session != fee_fields[1]
            or source.fee_schedule_source.reviewed_at != fee_fields[2]
            or source.fee_schedule_source.currency != fee_fields[3]
            or source.fee_schedule_source.contract_multiplier != fee_fields[4]
            or source.fee_schedule_source.entry_fee_per_contract_micros
            != fee_fields[5]
            or source.fee_schedule_source.exit_fee_per_contract_micros
            != fee_fields[6]
            or source.fee_schedule_source.close_fee_reserve_per_contract_micros
            != fee_fields[7]
            or source.fee_schedule_source.source_sha256 != fee_fields[8]
            or source.fee_schedule_source.schedule_digest != fee_fields[9]
        ):
            return False
        return True
    except Exception:
        return False


def is_issued_option_selection(selection: object) -> bool:
    if not isinstance(selection, OptionSelection):
        return False
    try:
        if not is_issued_phase2_authorization(selection.authorization):
            return False
        if not _reviewed_chain_is_current(
            selection.eligible_chain,
            authorization=selection.authorization,
            stage="ELIGIBLE",
        ) or not any(
            contract is selection.contract
            for contract in selection.eligible_chain.contracts
        ):
            return False
        from .risk import SessionCalendarResolver, _calendar_digest

        if (
            not isinstance(selection._calendar_resolver, SessionCalendarResolver)
            or not selection._calendar_resolver.release_verified
            or _calendar_digest(selection._calendar_resolver)
            != selection.calendar_digest
        ):
            return False
        fee_fields = _fee_schedule_fields(selection.fee_schedule)
        if fee_fields is None:
            return False
        if selection.selection_source is not None and not (
            _durable_selection_source_is_current(selection)
        ):
            return False
        if option_contract_reason_codes(
            selection.contract,
            authorization=selection.authorization,
            fee_schedule=selection.fee_schedule,
            event_exclusion_source=selection.event_exclusion_source,
            calendar_resolver=selection._calendar_resolver,
            portfolio_source=selection.portfolio_source,
        ):
            return False
        if (
            selection.quantity != 1
            or selection.dte
            != (
                selection.contract.expiration
                - selection.authorization.signal_source.publication_session
            ).days
            or selection.selected_at
            != selection.portfolio_source.query_cutoff
            or selection.reviewed_ask != selection.contract.ask
            or selection.entry_fee != money_from_micros(fee_fields[5])
            or selection.reserved_exit_fee != money_from_micros(fee_fields[7])
            or selection.reviewed_initial_risk
            != money_from_micros(
                _money_micros(
                    selection.reviewed_ask,
                    "INVALID_OPTION_SELECTION",
                    positive=True,
                )
                * _CONTRACT_MULTIPLIER
                + fee_fields[5]
                + fee_fields[7]
            )
            or selection.source_digest
            != _selection_digest(
                selection.authorization,
                selection.contract,
                selection.eligible_chain,
                selection.fee_schedule,
                selection.event_exclusion_source,
                selection.portfolio_source,
                selection.calendar_digest,
                selection.selected_at,
            )
        ):
            return False
        fingerprint = _selection_fingerprint(selection)
    except Exception:
        return False
    with _OPTION_SELECTIONS_LOCK:
        issued = _OPTION_SELECTIONS.get(id(selection))
        return (
            issued is not None
            and issued[0]() is selection
            and issued[1] == fingerprint
        )


def select_paper_long_call(
    chain: Sequence[OptionContract],
    *,
    authorization: Phase2Authorization,
    fee_schedule: object,
    event_exclusion_source: object,
    calendar_resolver: object,
    portfolio_source: object,
) -> OptionSelection | None:
    eligible = eligible_option_contracts(
        chain,
        authorization=authorization,
        fee_schedule=fee_schedule,
        event_exclusion_source=event_exclusion_source,
        calendar_resolver=calendar_resolver,
        portfolio_source=portfolio_source,
    )
    ranked = rank_option_contracts(eligible, authorization.signal)
    if not ranked:
        return None
    from .risk import _calendar_digest

    contract = ranked[0]
    fee_fields = _fee_schedule_fields(fee_schedule)
    if fee_fields is None or contract.ask is None:  # already proven by eligibility
        raise OptionPaperError("FEE_SCHEDULE_UNREVIEWED")
    calendar_digest = _calendar_digest(calendar_resolver)
    entry_ask_micros = _money_micros(
        contract.ask,
        "INVALID_OPTION_QUOTE",
        positive=True,
    )
    initial_risk_micros = (
        entry_ask_micros * _CONTRACT_MULTIPLIER
        + fee_fields[5]
        + fee_fields[7]
    )
    selection = OptionSelection(
        authorization=authorization,
        contract=contract,
        eligible_chain=eligible,
        fee_schedule=fee_schedule,
        event_exclusion_source=event_exclusion_source,
        portfolio_source=portfolio_source,
        selection_source=None,
        calendar_digest=calendar_digest,
        selected_at=portfolio_source.query_cutoff,
        quantity=1,
        dte=(
            contract.expiration
            - authorization.signal_source.publication_session
        ).days,
        reviewed_ask=money_from_micros(entry_ask_micros),
        entry_fee=money_from_micros(fee_fields[5]),
        reserved_exit_fee=money_from_micros(fee_fields[7]),
        reviewed_initial_risk=money_from_micros(initial_risk_micros),
        source_digest=_selection_digest(
            authorization,
            contract,
            eligible,
            fee_schedule,
            event_exclusion_source,
            portfolio_source,
            calendar_digest,
            portfolio_source.query_cutoff,
        ),
        _calendar_resolver=calendar_resolver,
    )
    identity = id(selection)

    def discard(dead: ReferenceType[OptionSelection]) -> None:
        with _OPTION_SELECTIONS_LOCK:
            current = _OPTION_SELECTIONS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_SELECTIONS.pop(identity, None)

    with _OPTION_SELECTIONS_LOCK:
        _OPTION_SELECTIONS[identity] = (
            ref(selection, discard),
            _selection_fingerprint(selection),
        )
    return selection


def _reissue_option_selection(
    selection_source: object,
    *,
    calendar_resolver: object,
    portfolio_source: object,
) -> OptionSelection:
    """Rederive one durable selection from exact current Journal sources."""
    try:
        from .config import _reissue_archived_fee_schedule
        from .journal import (
            Phase2SelectionSource,
            is_verified_phase2_event_exclusion_source,
            is_verified_phase2_fee_schedule_source,
            is_verified_phase2_manual_option_review_source,
            is_verified_phase2_option_chain_fact_source,
            is_verified_phase2_option_chain_source,
            is_verified_phase2_portfolio_source,
            is_verified_phase2_selection_source,
            phase2_sources_share_owner,
        )
        from .risk import SessionCalendarResolver, _calendar_digest
    except (AttributeError, ImportError):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_UNAVAILABLE") from None
    if not isinstance(selection_source, Phase2SelectionSource) or not (
        is_verified_phase2_selection_source(selection_source)
    ):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_UNVERIFIED")
    if portfolio_source is not selection_source.portfolio_source or not (
        is_verified_phase2_portfolio_source(portfolio_source)
    ):
        raise OptionPaperError("PHASE2_PORTFOLIO_SOURCE_MISMATCH")
    authorization = _authorization_for_source(
        selection_source.authorization_source,
        calendar_resolver=calendar_resolver,
    )
    if authorization is None:
        raise OptionPaperError("PHASE2_AUTHORIZATION_UNVERIFIED")
    nested_sources = (
        selection_source.authorization_source,
        selection_source.option_chain_source,
        selection_source.provider_fact_source,
        selection_source.selected_manual_review_source,
        selection_source.fee_schedule_source,
        selection_source.event_exclusion_source,
        portfolio_source,
        *selection_source.manual_review_sources,
    )
    if (
        not is_verified_phase2_option_chain_source(
            selection_source.option_chain_source
        )
        or not is_verified_phase2_option_chain_fact_source(
            selection_source.provider_fact_source
        )
        or not is_verified_phase2_fee_schedule_source(
            selection_source.fee_schedule_source
        )
        or not is_verified_phase2_event_exclusion_source(
            selection_source.event_exclusion_source
        )
        or any(
            not is_verified_phase2_manual_option_review_source(review)
            for review in selection_source.manual_review_sources
        )
        or any(
            not phase2_sources_share_owner(selection_source, nested)
            for nested in nested_sources
        )
    ):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_UNVERIFIED")
    if (
        not isinstance(calendar_resolver, SessionCalendarResolver)
        or not calendar_resolver.release_verified
        or _calendar_digest(calendar_resolver)
        != authorization.window_source.calendar_digest
        or portfolio_source.calendar_digest
        != authorization.window_source.calendar_digest
        or selection_source.event_exclusion_source.calendar_digest
        != authorization.window_source.calendar_digest
    ):
        raise OptionPaperError("PHASE2_CALENDAR_UNVERIFIED")
    if (
        selection_source.option_chain_source.authorization_source
        is not selection_source.authorization_source
        or selection_source.event_exclusion_source.signal_source
        is not authorization.signal_source
        or not any(
            fact is selection_source.provider_fact_source
            for fact in selection_source.option_chain_source.facts
        )
    ):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_MISMATCH")
    if not any(
        review is selection_source.selected_manual_review_source
        for review in selection_source.manual_review_sources
    ) or (
        selection_source.selected_manual_review_source.provider_fact_source
        is not selection_source.provider_fact_source
    ):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_MISMATCH")
    if (
        selection_source.quantity != 1
        or selection_source.received_at < selection_source.selected_at
        or selection_source.selection_session
        != authorization.signal_source.publication_session
        or portfolio_source.query_cutoff != selection_source.selected_at
        or portfolio_source.query_cutoff
        < selection_source.event_exclusion_source.query_cutoff
        or selection_source.selected_manual_review_source.observed_at
        >= selection_source.selected_at
        or selection_source.fee_schedule_source.archived_at
        > selection_source.selected_at
    ):
        raise OptionPaperError("OPTION_SELECTION_CHRONOLOGY_INVALID")
    try:
        schedule = calendar_resolver.session(selection_source.selection_session)
        selected_local = selection_source.selected_at.astimezone(
            schedule.timezone
        )
    except Exception:
        raise OptionPaperError("OPTION_SELECTION_CHRONOLOGY_INVALID") from None
    if selected_local.date() != selection_source.selection_session:
        raise OptionPaperError("OPTION_SELECTION_CHRONOLOGY_INVALID")
    reviewed = _derive_reviewed_option_chain(
        selection_source.option_chain_source,
        selection_source.manual_review_sources,
        authorization,
    )
    try:
        fee_schedule = _reissue_archived_fee_schedule(
            selection_source.fee_schedule_source
        )
    except Exception:
        raise OptionPaperError("FEE_SCHEDULE_UNREVIEWED") from None
    candidate = select_paper_long_call(
        reviewed,
        authorization=authorization,
        fee_schedule=fee_schedule,
        event_exclusion_source=selection_source.event_exclusion_source,
        calendar_resolver=calendar_resolver,
        portfolio_source=portfolio_source,
    )
    if candidate is None:
        raise OptionPaperError("OPTION_SELECTION_NO_ELIGIBLE_CONTRACT")
    if (
        candidate.contract.manual_source_id
        != selection_source.selected_manual_review_source.snapshot_id
        or candidate.contract.occ_symbol
        != selection_source.selected_manual_review_source.occ_symbol
        or candidate.source_digest != selection_source.ranking_digest
        or candidate.quantity != selection_source.quantity
        or candidate.selected_at != selection_source.selected_at
    ):
        raise OptionPaperError("OPTION_SELECTION_RANKING_MISMATCH")
    durable = OptionSelection(
        authorization=candidate.authorization,
        contract=candidate.contract,
        eligible_chain=candidate.eligible_chain,
        fee_schedule=candidate.fee_schedule,
        event_exclusion_source=candidate.event_exclusion_source,
        portfolio_source=candidate.portfolio_source,
        selection_source=selection_source,
        calendar_digest=candidate.calendar_digest,
        selected_at=candidate.selected_at,
        quantity=candidate.quantity,
        dte=candidate.dte,
        reviewed_ask=candidate.reviewed_ask,
        entry_fee=candidate.entry_fee,
        reserved_exit_fee=candidate.reserved_exit_fee,
        reviewed_initial_risk=candidate.reviewed_initial_risk,
        source_digest=candidate.source_digest,
        _calendar_resolver=candidate._calendar_resolver,
    )
    identity = id(durable)

    def discard(dead: ReferenceType[OptionSelection]) -> None:
        with _OPTION_SELECTIONS_LOCK:
            current = _OPTION_SELECTIONS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_SELECTIONS.pop(identity, None)

    with _OPTION_SELECTIONS_LOCK:
        _OPTION_SELECTIONS[identity] = (
            ref(durable, discard),
            _selection_fingerprint(durable),
        )
    if not is_issued_option_selection(durable):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_UNVERIFIED")
    return durable


@dataclass(frozen=True, slots=True)
class OptionMark:
    mark_id: str
    occ_symbol: str
    session_date: date
    observed_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    manual_source_id: str

    def __post_init__(self) -> None:
        _text(self.mark_id, "INVALID_OPTION_MARK")
        _text(self.occ_symbol, "INVALID_OPTION_MARK")
        if type(self.session_date) is not date:
            raise OptionPaperError("INVALID_OPTION_MARK")
        _aware(self.observed_at, "INVALID_OPTION_MARK")
        _decimal(self.bid, "INVALID_OPTION_MARK", optional=True)
        _decimal(self.ask, "INVALID_OPTION_MARK", optional=True)
        _text(self.manual_source_id, "INVALID_OPTION_MARK")


@dataclass(frozen=True, slots=True)
class OptionEquityPoint:
    kind: str
    mark_id: str
    session_date: date
    at: datetime
    liquidation_value: Decimal
    equity: Decimal
    high_water: Decimal
    drawdown: Decimal
    valid: bool
    reason_codes: tuple[str, ...]
    source_digest: str


@dataclass(frozen=True, slots=True)
class ClosedPaperOption:
    mark_id: str
    occ_symbol: str
    exited_at: datetime
    exit_bid: Decimal
    exit_ask: Decimal
    actual_exit_fee: Decimal
    net_pnl: Decimal
    net_r: Decimal
    source_digest: str


@dataclass(frozen=True, slots=True, weakref_slot=True)
class PaperOptionPosition:
    window_id: str
    selection: OptionSelection
    portfolio_source: object = field(repr=False, compare=False)
    open_source: object = field(repr=False, compare=False)
    starting_equity: Decimal
    paper_cash: Decimal
    opened_at: datetime
    entry_ask: Decimal
    entry_fee: Decimal
    reserved_exit_fee: Decimal
    initial_risk: Decimal
    status: OptionWindowStatus
    equity_points: tuple[OptionEquityPoint, ...]
    high_water: Decimal
    maximum_drawdown: Decimal
    closed_trade: ClosedPaperOption | None
    source_digest: str
    _calendar_resolver: object = field(compare=False, repr=False)


_OPTION_WINDOWS: dict[
    int,
    tuple[ReferenceType[PaperOptionPosition], tuple[object, ...]],
] = {}
_OPTION_WINDOWS_LOCK = RLock()


def _point_fingerprint(point: OptionEquityPoint) -> tuple[object, ...]:
    return (
        point.kind,
        point.mark_id,
        point.session_date,
        point.at,
        point.liquidation_value,
        point.equity,
        point.high_water,
        point.drawdown,
        point.valid,
        point.reason_codes,
        point.source_digest,
    )


def _closed_fingerprint(
    closed: ClosedPaperOption | None,
) -> tuple[object, ...] | None:
    if closed is None:
        return None
    return (
        closed.mark_id,
        closed.occ_symbol,
        closed.exited_at,
        closed.exit_bid,
        closed.exit_ask,
        closed.actual_exit_fee,
        closed.net_pnl,
        closed.net_r,
        closed.source_digest,
    )


def _window_fingerprint(window: PaperOptionPosition) -> tuple[object, ...]:
    return (
        window.window_id,
        id(window.selection),
        id(window.portfolio_source),
        id(window.open_source),
        window.starting_equity,
        window.paper_cash,
        window.opened_at,
        window.entry_ask,
        window.entry_fee,
        window.reserved_exit_fee,
        window.initial_risk,
        window.status,
        tuple(_point_fingerprint(point) for point in window.equity_points),
        window.high_water,
        window.maximum_drawdown,
        _closed_fingerprint(window.closed_trade),
        window.source_digest,
        id(window._calendar_resolver),
    )


def is_issued_option_window(window: object) -> bool:
    if not isinstance(window, PaperOptionPosition):
        return False
    try:
        from .risk import SessionCalendarResolver, _calendar_digest

        material = _option_open_material(
            window.selection,
            window.portfolio_source,
            window.open_source,
        )
        if (
            window.selection.selection_source is None
            or not _durable_selection_source_is_current(window.selection)
            or window._calendar_resolver is not window.selection._calendar_resolver
            or not isinstance(window._calendar_resolver, SessionCalendarResolver)
            or not window._calendar_resolver.release_verified
            or _calendar_digest(window._calendar_resolver)
            != window.selection.calendar_digest
            or window.window_id != window.open_source.window_id
            or window.starting_equity != money_from_micros(material[0])
            or window.opened_at != material[2]
            or window.entry_ask != money_from_micros(material[3])
            or window.entry_fee != money_from_micros(material[4])
            or window.reserved_exit_fee != money_from_micros(material[5])
            or window.initial_risk != money_from_micros(material[6])
        ):
            return False
        fingerprint = _window_fingerprint(window)
    except Exception:
        return False
    with _OPTION_WINDOWS_LOCK:
        issued = _OPTION_WINDOWS.get(id(window))
        return (
            issued is not None
            and issued[0]() is window
            and issued[1] == fingerprint
        )


def _option_open_material(
    selection: OptionSelection,
    portfolio_source: object,
    open_source: object,
) -> tuple[int, int, datetime, int, int, int, int, str]:
    if (
        not is_issued_option_selection(selection)
        or selection.selection_source is None
        or not _durable_selection_source_is_current(selection)
    ):
        raise OptionPaperError("OPTION_SELECTION_UNVERIFIED")
    try:
        from .journal import (
            Phase2OptionOpenSource,
            Phase2PortfolioSource,
            is_verified_journal_action_source,
            is_verified_phase2_fee_schedule_source,
            is_verified_phase2_option_open_source,
            is_verified_phase2_portfolio_source,
            is_verified_phase2_selection_source,
            phase2_sources_share_owner,
        )
    except (AttributeError, ImportError):
        raise OptionPaperError("OPTION_OPEN_SOURCE_UNAVAILABLE") from None
    selection_source = selection.selection_source
    if not is_verified_phase2_selection_source(selection_source):
        raise OptionPaperError("OPTION_SELECTION_SOURCE_UNVERIFIED")
    if not isinstance(portfolio_source, Phase2PortfolioSource) or not (
        is_verified_phase2_portfolio_source(portfolio_source)
    ):
        raise OptionPaperError("PHASE2_PORTFOLIO_UNVERIFIED")
    if not isinstance(open_source, Phase2OptionOpenSource) or not (
        is_verified_phase2_option_open_source(open_source)
    ):
        raise OptionPaperError("OPTION_OPEN_SOURCE_UNVERIFIED")
    if (
        not is_verified_journal_action_source(open_source.open_action)
        or not is_verified_phase2_fee_schedule_source(
            open_source.fee_schedule_source
        )
        or any(
            not phase2_sources_share_owner(open_source, nested)
            for nested in (
                selection_source,
                portfolio_source,
                open_source.open_action,
                open_source.fee_schedule_source,
                selection.authorization.window_source,
            )
        )
    ):
        raise OptionPaperError("OPTION_OPEN_SOURCE_UNVERIFIED")
    if (
        open_source.selection_source is not selection_source
        or open_source.fee_schedule_source
        is not selection_source.fee_schedule_source
        or open_source.portfolio_source_digest != portfolio_source.source_digest
        or open_source.window_id != selection.authorization.window_source.window_id
        or portfolio_source.window_id != open_source.window_id
    ):
        raise OptionPaperError("OPTION_OPEN_SOURCE_MISMATCH")
    if (
        portfolio_source is selection_source.portfolio_source
        or portfolio_source.query_cutoff <= selection_source.selected_at
    ):
        raise OptionPaperError("PHASE2_PORTFOLIO_STALE_FOR_OPEN")
    if portfolio_source.open_position_source is not None:
        raise OptionPaperError("OPTION_POSITION_ALREADY_OPEN")
    fee_fields = _fee_schedule_fields(selection.fee_schedule)
    if fee_fields is None:
        raise OptionPaperError("FEE_SCHEDULE_UNREVIEWED")
    action = open_source.open_action
    if (
        action.domain_kind != "OPTION_PAPER_OPEN"
        or action.event_role != "OBSERVATION"
        or action.symbol is not None
        or action.event_time != open_source.entered_at
        or action.ask_micros != open_source.entry_ask_micros
        or action.bid_micros is not None
        or _action_occ_symbol(action) != selection.contract.occ_symbol
        or action.received_at > open_source.received_at
        or open_source.received_at < open_source.entered_at
    ):
        raise OptionPaperError("OPTION_OPEN_QUOTE_MISMATCH")
    if (
        open_source.quantity != 1
        or open_source.quantity != selection.quantity
        or type(open_source.entry_ask_micros) is not int
        or open_source.entry_ask_micros <= 0
        or open_source.entry_fee_micros != fee_fields[5]
        or open_source.reserve_fee_micros != fee_fields[7]
    ):
        raise OptionPaperError("OPTION_OPEN_ECONOMICS_MISMATCH")
    initial_risk_micros = (
        open_source.entry_ask_micros * _CONTRACT_MULTIPLIER
        + open_source.entry_fee_micros
        + open_source.reserve_fee_micros
    )
    if (
        open_source.all_in_initial_risk_micros != initial_risk_micros
        or initial_risk_micros > _MAXIMUM_INITIAL_RISK_MICROS
    ):
        raise OptionPaperError("OPTION_INITIAL_RISK_ABOVE_50")
    unsettled_proceeds_micros = portfolio_source.unsettled_proceeds_micros
    if (
        type(unsettled_proceeds_micros) is not int
        or unsettled_proceeds_micros < 0
    ):
        raise OptionPaperError("OPTION_SETTLED_CASH_INSUFFICIENT")
    if unsettled_proceeds_micros > 0:
        raise OptionPaperError("OPTION_PRIOR_SALE_UNSETTLED")
    if (
        type(portfolio_source.settled_cash_micros) is not int
        or type(portfolio_source.economic_cash_micros) is not int
        or type(portfolio_source.equity_micros) is not int
        or min(
            portfolio_source.settled_cash_micros,
            portfolio_source.economic_cash_micros,
            portfolio_source.equity_micros,
        )
        < 0
        or portfolio_source.settled_cash_micros < initial_risk_micros
    ):
        raise OptionPaperError("OPTION_SETTLED_CASH_INSUFFICIENT")
    entry_debit_micros = (
        open_source.entry_ask_micros * _CONTRACT_MULTIPLIER
        + open_source.entry_fee_micros
    )
    paper_cash_micros = portfolio_source.economic_cash_micros - entry_debit_micros
    if paper_cash_micros < 0:
        raise OptionPaperError("OPTION_ECONOMIC_CASH_INSUFFICIENT")
    opened = _aware(open_source.entered_at, "INVALID_OPTION_OPEN_TIME")
    review = selection_source.selected_manual_review_source
    if (
        review.observed_at >= selection_source.selected_at
        or selection_source.selected_at > opened
        or portfolio_source.as_of > portfolio_source.query_cutoff
        or portfolio_source.query_cutoff > opened
    ):
        raise OptionPaperError("OPTION_OPEN_CHRONOLOGY_INVALID")
    try:
        schedule = selection._calendar_resolver.session(
            selection_source.selection_session
        )
        local_opened = opened.astimezone(schedule.timezone)
    except Exception:
        raise OptionPaperError("INVALID_OPTION_OPEN_TIME") from None
    if (
        local_opened.date() != schedule.session_date
        or not schedule.open_time
        <= local_opened.time().replace(tzinfo=None)
        <= schedule.close_time
    ):
        raise OptionPaperError("INVALID_OPTION_OPEN_TIME")
    source_digest = _digest(
        "open",
        selection.source_digest,
        selection_source.source_digest,
        portfolio_source.source_digest,
        portfolio_source.authority_digest,
        open_source.source_digest,
        portfolio_source.equity_micros,
        paper_cash_micros,
        opened,
        open_source.entry_ask_micros,
        open_source.entry_fee_micros,
        open_source.reserve_fee_micros,
        initial_risk_micros,
    )
    return (
        portfolio_source.equity_micros,
        paper_cash_micros,
        opened,
        open_source.entry_ask_micros,
        open_source.entry_fee_micros,
        open_source.reserve_fee_micros,
        initial_risk_micros,
        source_digest,
    )


def open_option_window(
    selection: OptionSelection,
    portfolio_source: object,
    open_source: object,
) -> PaperOptionPosition:
    """Open one fixed-quantity position from exact durable OPEN evidence."""
    material = _option_open_material(selection, portfolio_source, open_source)
    window = PaperOptionPosition(
        window_id=open_source.window_id,
        selection=selection,
        portfolio_source=portfolio_source,
        open_source=open_source,
        starting_equity=money_from_micros(material[0]),
        paper_cash=money_from_micros(material[1]),
        opened_at=material[2],
        entry_ask=money_from_micros(material[3]),
        entry_fee=money_from_micros(material[4]),
        reserved_exit_fee=money_from_micros(material[5]),
        initial_risk=money_from_micros(material[6]),
        status=OptionWindowStatus.ACTIVE,
        equity_points=(),
        high_water=money_from_micros(material[0]),
        maximum_drawdown=Decimal("0"),
        closed_trade=None,
        source_digest=material[7],
        _calendar_resolver=selection._calendar_resolver,
    )
    identity = id(window)

    def discard(dead: ReferenceType[PaperOptionPosition]) -> None:
        with _OPTION_WINDOWS_LOCK:
            current = _OPTION_WINDOWS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_WINDOWS.pop(identity, None)

    with _OPTION_WINDOWS_LOCK:
        _OPTION_WINDOWS[identity] = (
            ref(window, discard),
            _window_fingerprint(window),
        )
    return window


def _mark_order_reason(
    window: PaperOptionPosition,
    mark: OptionMark,
) -> str | None:
    if any(point.mark_id == mark.mark_id for point in window.equity_points):
        return "DUPLICATE_OPTION_MARK"
    if any(
        point.kind == "MARK" and point.session_date == mark.session_date
        for point in window.equity_points
    ):
        return "DUPLICATE_OPTION_MARK_SESSION"
    previous_at = (
        window.equity_points[-1].at
        if window.equity_points
        else window.opened_at
    )
    if mark.observed_at <= previous_at:
        return "OPTION_MARK_OUT_OF_ORDER"
    return None


def _mark_reason_codes(
    window: PaperOptionPosition,
    mark: OptionMark,
    *,
    close: bool,
) -> tuple[str, ...]:
    resolver = window._calendar_resolver
    reasons: list[str] = []
    if mark.occ_symbol != window.selection.contract.occ_symbol:
        reasons.append("OPTION_MARK_CONTRACT_MISMATCH")
    try:
        schedule = resolver.session(mark.session_date)
    except Exception:
        reasons.append("OPTION_MARK_SESSION_CLOSED")
        schedule = None
    if schedule is not None:
        local = mark.observed_at.astimezone(schedule.timezone)
        if local.date() != mark.session_date:
            reasons.append("OPTION_MARK_SESSION_MISMATCH")
        local_clock = local.time().replace(tzinfo=None)
        if close:
            if not schedule.open_time <= local_clock <= schedule.close_time:
                reasons.append("OPTION_CLOSE_OUTSIDE_SESSION")
        else:
            close_minus_five = (
                datetime.combine(mark.session_date, schedule.close_time)
                - timedelta(minutes=5)
            ).time()
            if not schedule.review_time <= local_clock <= close_minus_five:
                reasons.append("OPTION_MARK_OUTSIDE_REVIEW_WINDOW")
    if (
        mark.bid is None
        or mark.ask is None
        or mark.bid <= _ZERO
        or mark.ask <= _ZERO
        or mark.bid > mark.ask
        or not _is_canonical_positive_premium(mark.bid)
        or not _is_canonical_positive_premium(mark.ask)
    ):
        reasons.append("INVALID_OPTION_MARK_QUOTE")
    return tuple(dict.fromkeys(reasons))


def _point(
    *,
    kind: str,
    mark: OptionMark,
    liquidation_micros: int,
    equity_micros: int,
    high_water_micros: int,
    drawdown_micros: int,
    valid: bool,
    reason_codes: tuple[str, ...],
    prior_digest: str,
) -> OptionEquityPoint:
    source_digest = _digest(
        kind,
        prior_digest,
        mark.mark_id,
        mark.occ_symbol,
        mark.session_date,
        mark.observed_at,
        mark.bid,
        mark.ask,
        mark.manual_source_id,
        liquidation_micros,
        equity_micros,
        high_water_micros,
        drawdown_micros,
        valid,
        *reason_codes,
    )
    return OptionEquityPoint(
        kind=kind,
        mark_id=mark.mark_id,
        session_date=mark.session_date,
        at=mark.observed_at,
        liquidation_value=money_from_micros(liquidation_micros),
        equity=money_from_micros(equity_micros),
        high_water=money_from_micros(high_water_micros),
        drawdown=money_from_micros(drawdown_micros),
        valid=valid,
        reason_codes=reason_codes,
        source_digest=source_digest,
    )


def record_option_mark(
    window: PaperOptionPosition,
    mark: OptionMark,
) -> PaperOptionPosition:
    """Record one ordered daily mark; invalid evidence permanently restarts."""
    if not is_issued_option_window(window):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    if not isinstance(mark, OptionMark):
        raise OptionPaperError("INVALID_OPTION_MARK")
    if window.closed_trade is not None or window.status is OptionWindowStatus.CLOSED:
        raise OptionPaperError("OPTION_WINDOW_CLOSED")
    order_reason = _mark_order_reason(window, mark)
    if order_reason is not None:
        raise OptionPaperError(order_reason)
    reasons = _mark_reason_codes(window, mark, close=False)
    valid = not reasons
    reserve_micros = _money_micros(
        window.reserved_exit_fee,
        "INVALID_OPTION_EXIT_RESERVE",
        nonnegative=True,
    )
    if valid:
        assert mark.bid is not None
        liquidation_micros = max(
            0,
            _money_micros(
                mark.bid,
                "INVALID_OPTION_MARK_QUOTE",
                positive=True,
            )
            * _CONTRACT_MULTIPLIER
            - reserve_micros,
        )
    else:
        liquidation_micros = 0
    paper_cash_micros = _money_micros(
        window.paper_cash,
        "INVALID_OPTION_WINDOW_CASH",
        nonnegative=True,
    )
    prior_high_micros = _money_micros(
        window.high_water,
        "INVALID_OPTION_WINDOW_HIGH_WATER",
        nonnegative=True,
    )
    prior_maximum_micros = _money_micros(
        window.maximum_drawdown,
        "INVALID_OPTION_WINDOW_DRAWDOWN",
        nonnegative=True,
    )
    equity_micros = paper_cash_micros + liquidation_micros
    high_water_micros = max(prior_high_micros, equity_micros)
    drawdown_micros = high_water_micros - equity_micros
    maximum_micros = max(prior_maximum_micros, drawdown_micros)
    point = _point(
        kind="MARK",
        mark=mark,
        liquidation_micros=liquidation_micros,
        equity_micros=equity_micros,
        high_water_micros=high_water_micros,
        drawdown_micros=drawdown_micros,
        valid=valid,
        reason_codes=reasons,
        prior_digest=window.source_digest,
    )
    status = (
        OptionWindowStatus.RESTART_REQUIRED
        if not valid or window.status is OptionWindowStatus.RESTART_REQUIRED
        else OptionWindowStatus.ACTIVE
    )
    source_digest = _digest(
        "window-mark",
        window.source_digest,
        point.source_digest,
        status,
        maximum_micros,
    )
    updated = PaperOptionPosition(
        window_id=window.window_id,
        selection=window.selection,
        portfolio_source=window.portfolio_source,
        open_source=window.open_source,
        starting_equity=window.starting_equity,
        paper_cash=window.paper_cash,
        opened_at=window.opened_at,
        entry_ask=window.entry_ask,
        entry_fee=window.entry_fee,
        reserved_exit_fee=window.reserved_exit_fee,
        initial_risk=window.initial_risk,
        status=status,
        equity_points=(*window.equity_points, point),
        high_water=money_from_micros(high_water_micros),
        maximum_drawdown=money_from_micros(maximum_micros),
        closed_trade=None,
        source_digest=source_digest,
        _calendar_resolver=window._calendar_resolver,
    )
    identity = id(updated)

    def discard(dead: ReferenceType[PaperOptionPosition]) -> None:
        with _OPTION_WINDOWS_LOCK:
            current = _OPTION_WINDOWS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_WINDOWS.pop(identity, None)

    with _OPTION_WINDOWS_LOCK:
        _OPTION_WINDOWS[identity] = (
            ref(updated, discard),
            _window_fingerprint(updated),
        )
    return updated


def close_option_window(
    window: PaperOptionPosition,
    mark: OptionMark,
    *,
    actual_exit_fee: Decimal,
) -> PaperOptionPosition:
    """Close at the confirmed bid and replace the reserve with actual fees."""
    if not is_issued_option_window(window):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    if not isinstance(mark, OptionMark):
        raise OptionPaperError("INVALID_OPTION_CLOSE")
    if window.closed_trade is not None or window.status is OptionWindowStatus.CLOSED:
        raise OptionPaperError("OPTION_WINDOW_CLOSED")
    if any(point.mark_id == mark.mark_id for point in window.equity_points):
        raise OptionPaperError("DUPLICATE_OPTION_MARK")
    previous_at = (
        window.equity_points[-1].at
        if window.equity_points
        else window.opened_at
    )
    if mark.observed_at <= previous_at:
        raise OptionPaperError("OPTION_MARK_OUT_OF_ORDER")
    reasons = _mark_reason_codes(window, mark, close=True)
    if reasons:
        raise OptionPaperError("INVALID_OPTION_CLOSE_QUOTE")
    fee_micros = _money_micros(
        actual_exit_fee,
        "INVALID_ACTUAL_EXIT_FEE",
        positive=True,
    )
    assert mark.bid is not None and mark.ask is not None
    exit_bid_micros = _money_micros(
        mark.bid,
        "INVALID_OPTION_CLOSE_QUOTE",
        positive=True,
    )
    exit_ask_micros = _money_micros(
        mark.ask,
        "INVALID_OPTION_CLOSE_QUOTE",
        positive=True,
    )
    paper_cash_micros = _money_micros(
        window.paper_cash,
        "INVALID_OPTION_WINDOW_CASH",
        nonnegative=True,
    )
    final_cash_micros = (
        paper_cash_micros
        + exit_bid_micros * _CONTRACT_MULTIPLIER
        - fee_micros
    )
    starting_micros = _money_micros(
        window.starting_equity,
        "INVALID_OPTION_STARTING_EQUITY",
        positive=True,
    )
    net_pnl_micros = final_cash_micros - starting_micros
    initial_risk_micros = _money_micros(
        window.initial_risk,
        "INVALID_OPTION_INITIAL_RISK",
        positive=True,
    )
    net_r = Decimal(net_pnl_micros) / Decimal(initial_risk_micros)
    prior_high_micros = _money_micros(
        window.high_water,
        "INVALID_OPTION_WINDOW_HIGH_WATER",
        nonnegative=True,
    )
    prior_maximum_micros = _money_micros(
        window.maximum_drawdown,
        "INVALID_OPTION_WINDOW_DRAWDOWN",
        nonnegative=True,
    )
    high_water_micros = max(prior_high_micros, final_cash_micros)
    drawdown_micros = high_water_micros - final_cash_micros
    maximum_micros = max(prior_maximum_micros, drawdown_micros)
    point = _point(
        kind="CLOSE",
        mark=mark,
        liquidation_micros=exit_bid_micros * _CONTRACT_MULTIPLIER,
        equity_micros=final_cash_micros,
        high_water_micros=high_water_micros,
        drawdown_micros=drawdown_micros,
        valid=True,
        reason_codes=(),
        prior_digest=window.source_digest,
    )
    close_digest = _digest(
        "close",
        point.source_digest,
        fee_micros,
        net_pnl_micros,
        net_r,
    )
    closed_trade = ClosedPaperOption(
        mark_id=mark.mark_id,
        occ_symbol=mark.occ_symbol,
        exited_at=mark.observed_at,
        exit_bid=money_from_micros(exit_bid_micros),
        exit_ask=money_from_micros(exit_ask_micros),
        actual_exit_fee=money_from_micros(fee_micros),
        net_pnl=money_from_micros(net_pnl_micros),
        net_r=net_r,
        source_digest=close_digest,
    )
    status = (
        OptionWindowStatus.RESTART_REQUIRED
        if window.status is OptionWindowStatus.RESTART_REQUIRED
        else OptionWindowStatus.CLOSED
    )
    source_digest = _digest(
        "window-close",
        window.source_digest,
        close_digest,
        status,
        final_cash_micros,
        maximum_micros,
    )
    updated = PaperOptionPosition(
        window_id=window.window_id,
        selection=window.selection,
        portfolio_source=window.portfolio_source,
        open_source=window.open_source,
        starting_equity=window.starting_equity,
        paper_cash=money_from_micros(final_cash_micros),
        opened_at=window.opened_at,
        entry_ask=window.entry_ask,
        entry_fee=window.entry_fee,
        reserved_exit_fee=window.reserved_exit_fee,
        initial_risk=window.initial_risk,
        status=status,
        equity_points=(*window.equity_points, point),
        high_water=money_from_micros(high_water_micros),
        maximum_drawdown=money_from_micros(maximum_micros),
        closed_trade=closed_trade,
        source_digest=source_digest,
        _calendar_resolver=window._calendar_resolver,
    )
    identity = id(updated)

    def discard(dead: ReferenceType[PaperOptionPosition]) -> None:
        with _OPTION_WINDOWS_LOCK:
            current = _OPTION_WINDOWS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_WINDOWS.pop(identity, None)

    with _OPTION_WINDOWS_LOCK:
        _OPTION_WINDOWS[identity] = (
            ref(updated, discard),
            _window_fingerprint(updated),
        )
    return updated


@dataclass(frozen=True, slots=True, weakref_slot=True)
class OptionExitDecision:
    """Source-bound diagnostic stating whether one paper close is required."""

    position: PaperOptionPosition = field(repr=False, compare=False)
    underlying_source: object = field(repr=False, compare=False)
    decision_fact_source: object | None = field(repr=False, compare=False)
    review_session: date
    evaluated_at: datetime
    holding_sessions: int
    dte: int
    required_close_reason: str | None
    reason_codes: tuple[str, ...]
    diagnostic_only: bool
    source_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.position, PaperOptionPosition)
            or type(self.review_session) is not date
            or type(self.holding_sessions) is not int
            or self.holding_sessions <= 0
            or type(self.dte) is not int
            or self.required_close_reason
            not in {
                None,
                "STOP",
                "TARGET",
                "MAX_HOLD_10_SESSIONS",
                "DTE_21",
            }
            or type(self.reason_codes) is not tuple
            or any(type(code) is not str or not code for code in self.reason_codes)
            or len(self.reason_codes) != len(set(self.reason_codes))
            or self.diagnostic_only is not True
        ):
            raise OptionPaperError("INVALID_OPTION_EXIT_DECISION")
        _aware(self.evaluated_at, "INVALID_OPTION_EXIT_DECISION")
        _sha256(self.source_digest, "INVALID_OPTION_EXIT_DECISION")


_OPTION_EXIT_DECISIONS: dict[
    int,
    tuple[
        ReferenceType[OptionExitDecision],
        tuple[object, ...],
        ReferenceType[PaperOptionPosition],
        ReferenceType[object],
    ],
] = {}
_OPTION_EXIT_DECISIONS_LOCK = RLock()


def _option_holding_sessions(
    resolver: object,
    *,
    opened_session: date,
    review_session: date,
) -> int:
    if review_session < opened_session:
        raise OptionPaperError("OPTION_EXIT_SOURCE_PRECEDES_OPEN")
    cursor = opened_session
    count = 1
    for _ in range(370):
        if cursor == review_session:
            return count
        try:
            cursor = resolver.add_sessions(cursor, 1)
        except Exception:
            break
        count += 1
        if cursor > review_session:
            break
    raise OptionPaperError("OPTION_EXIT_SOURCE_SESSION_INVALID")


def _option_exit_material(
    position: PaperOptionPosition,
    underlying_source: object,
) -> tuple[
    date,
    datetime,
    int,
    int,
    str | None,
    tuple[str, ...],
    object | None,
    str,
]:
    if not is_issued_option_window(position):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    if position.closed_trade is not None or position.status is OptionWindowStatus.CLOSED:
        raise OptionPaperError("OPTION_WINDOW_CLOSED")
    try:
        from .journal import (
            Phase2UnderlyingReviewSource,
            is_verified_phase2_option_open_source,
            is_verified_phase2_underlying_review_fact_source,
            is_verified_phase2_underlying_review_page_source,
            is_verified_phase2_underlying_review_source,
            phase2_sources_share_owner,
        )
    except (AttributeError, ImportError):
        raise OptionPaperError("OPTION_EXIT_SOURCE_UNAVAILABLE") from None
    if not isinstance(underlying_source, Phase2UnderlyingReviewSource) or not (
        is_verified_phase2_underlying_review_source(underlying_source)
    ):
        raise OptionPaperError("OPTION_EXIT_SOURCE_UNVERIFIED")
    if (
        not is_verified_phase2_option_open_source(underlying_source.entry_source)
        or underlying_source.entry_source is not position.open_source
        or underlying_source.window_id != position.window_id
        or not phase2_sources_share_owner(
            underlying_source,
            position.open_source,
        )
    ):
        raise OptionPaperError("OPTION_EXIT_ENTRY_SOURCE_MISMATCH")
    pages = underlying_source.pages
    facts = underlying_source.facts
    if (
        underlying_source.collection_name != "bars"
        or underlying_source.timeframe != "1Min"
        or underlying_source.adjustment != "split"
        or underlying_source.feed != "sip"
        or underlying_source.requested_symbols
        != (position.selection.authorization.signal_source.symbol,)
        or not underlying_source.terminal
        or not pages
        or not facts
        or underlying_source.expected_page_count != len(pages)
        or underlying_source.expected_fact_count != len(facts)
        or tuple(page.page_ordinal for page in pages)
        != tuple(range(1, len(pages) + 1))
        or pages[0].request_page_token is not None
        or pages[-1].next_page_token is not None
        or any(
            page.review_set_id != underlying_source.review_set_id
            or not is_verified_phase2_underlying_review_page_source(page)
            or not phase2_sources_share_owner(underlying_source, page)
            or page.source_type != "ALPACA_INTRADAY_BARS"
            or page.retrieved_at > underlying_source.query_cutoff
            or (
                index > 0
                and pages[index - 1].next_page_token
                != page.request_page_token
            )
            for index, page in enumerate(pages)
        )
    ):
        raise OptionPaperError("OPTION_EXIT_SOURCE_INCOMPLETE")
    for page in pages:
        try:
            parsed = urlsplit(page.request_url)
            query = parse_qs(parsed.query, keep_blank_values=True)
        except Exception:
            raise OptionPaperError("OPTION_EXIT_SOURCE_SEMANTICS_INVALID") from None
        if (
            parsed.path != "/v2/stocks/bars"
            or query.get("timeframe") != ["1Min"]
            or query.get("adjustment") != ["split"]
            or query.get("feed") != ["sip"]
            or query.get("symbols")
            != [position.selection.authorization.signal_source.symbol]
        ):
            raise OptionPaperError("OPTION_EXIT_SOURCE_SEMANTICS_INVALID")
    try:
        provider_values = tuple(
            underlying_source.provider_cohort[underlying_source.underlying]
        )
    except Exception:
        raise OptionPaperError("OPTION_EXIT_SOURCE_INCOMPLETE") from None
    if len(provider_values) != len(facts) or any(
        fact.bar is not provider_bar
        for fact, provider_bar in zip(facts, provider_values, strict=True)
    ):
        raise OptionPaperError("OPTION_EXIT_SOURCE_INCOMPLETE")
    if any(
        not is_verified_phase2_underlying_review_fact_source(fact)
        or not phase2_sources_share_owner(underlying_source, fact)
        or fact.review_set_id != underlying_source.review_set_id
        or fact.symbol != underlying_source.underlying
        or fact.fetch_page_ordinal < 1
        or fact.fetch_page_ordinal > len(pages)
        or fact.source_observation_row_id
        != pages[fact.fetch_page_ordinal - 1].source_observation_row_id
        or fact.external_source_observation_id
        != pages[fact.fetch_page_ordinal - 1].external_source_observation_id
        or fact.payload_sha256
        != pages[fact.fetch_page_ordinal - 1].payload_sha256
        or fact.bar.symbol != fact.symbol
        or fact.bar.timestamp != fact.bar_at
        or fact.bar.feed != "sip"
        or fact.bar.adjustment != "split"
        or money_to_micros(fact.bar.open) != fact.open_micros
        or money_to_micros(fact.bar.high) != fact.high_micros
        or money_to_micros(fact.bar.low) != fact.low_micros
        or money_to_micros(fact.bar.close) != fact.close_micros
        or fact.bar.volume != fact.volume
        for fact in facts
    ):
        raise OptionPaperError("OPTION_EXIT_SOURCE_INCOMPLETE")
    signal_source = position.selection.authorization.signal_source
    if (
        underlying_source.underlying != signal_source.symbol
        or underlying_source.request_start > underlying_source.request_end
        or underlying_source.request_end > underlying_source.query_cutoff
        or underlying_source.query_cutoff > underlying_source.received_at
    ):
        raise OptionPaperError("OPTION_EXIT_UNDERLYING_MISMATCH")
    resolver = position._calendar_resolver
    try:
        schedule = resolver.session(underlying_source.review_session)
        request_start_local = underlying_source.request_start.astimezone(
            schedule.timezone
        )
        request_end_local = underlying_source.request_end.astimezone(
            schedule.timezone
        )
    except Exception:
        raise OptionPaperError("OPTION_EXIT_SOURCE_SESSION_INVALID")
    review_session = underlying_source.review_session
    try:
        close_minus_five = (
            datetime.combine(review_session, schedule.close_time)
            - timedelta(minutes=5)
        ).time()
    except Exception:
        raise OptionPaperError("OPTION_EXIT_SOURCE_SESSION_INVALID") from None
    opened_session = position.opened_at.astimezone(schedule.timezone).date()
    expected_start = (
        position.opened_at
        if review_session == opened_session
        else datetime.combine(
            review_session,
            schedule.open_time,
            tzinfo=schedule.timezone,
        )
    )
    if (
        request_start_local.date() != review_session
        or request_end_local.date() != review_session
        or underlying_source.request_start != expected_start
        or not schedule.review_time
        <= request_end_local.time().replace(tzinfo=None)
        <= close_minus_five
        or tuple(fact.bar_at for fact in facts)
        != tuple(sorted({fact.bar_at for fact in facts}))
        or any(
            fact.bar_at < underlying_source.request_start
            or fact.bar_at > underlying_source.request_end
            or fact.bar_at.astimezone(schedule.timezone).date() != review_session
            for fact in facts
        )
        or facts[-1].bar_at < underlying_source.request_end - timedelta(minutes=5)
    ):
        raise OptionPaperError("OPTION_EXIT_SOURCE_OUTSIDE_REVIEW_WINDOW")
    holding_sessions = _option_holding_sessions(
        resolver,
        opened_session=opened_session,
        review_session=review_session,
    )
    dte = (position.selection.contract.expiration - review_session).days
    try:
        stop = money_from_micros(signal_source.recommended_stop_micros)
        target = money_from_micros(signal_source.target_micros)
    except Exception:
        raise OptionPaperError("OPTION_EXIT_THRESHOLDS_UNVERIFIED") from None
    if stop <= _ZERO or target <= stop:
        raise OptionPaperError("OPTION_EXIT_THRESHOLDS_UNVERIFIED")
    reasons: list[str] = []
    required: str | None = None
    decision_fact_source = None
    evaluated_at = underlying_source.request_end
    for fact in facts:
        stop_hit = fact.low_micros <= signal_source.recommended_stop_micros
        target_hit = fact.high_micros >= signal_source.target_micros
        if stop_hit:
            required = "STOP"
            decision_fact_source = fact
            evaluated_at = fact.bar_at
            reasons.append("STOP")
            if target_hit:
                reasons.append("SAME_BAR_STOP_TARGET_STOP_FIRST")
            break
        if target_hit:
            required = "TARGET"
            decision_fact_source = fact
            evaluated_at = fact.bar_at
            reasons.append("TARGET")
            break
    if required is None and holding_sessions >= 10:
        required = "MAX_HOLD_10_SESSIONS"
        reasons.append(required)
    elif required is None and dte <= 21:
        required = "DTE_21"
        reasons.append(required)
    source_digest = _digest(
        "option-exit-decision",
        position.source_digest,
        underlying_source.source_digest,
        underlying_source.manifest_digest,
        *(fact.fact_digest for fact in facts),
        review_session,
        evaluated_at,
        holding_sessions,
        dte,
        stop,
        target,
        required,
        None if decision_fact_source is None else decision_fact_source.source_digest,
        *reasons,
    )
    return (
        review_session,
        evaluated_at,
        holding_sessions,
        dte,
        required,
        tuple(reasons),
        decision_fact_source,
        source_digest,
    )


def _option_exit_decision_fingerprint(
    decision: OptionExitDecision,
) -> tuple[object, ...]:
    return (
        id(decision.position),
        id(decision.underlying_source),
        id(decision.decision_fact_source),
        decision.review_session,
        decision.evaluated_at,
        decision.holding_sessions,
        decision.dte,
        decision.required_close_reason,
        decision.reason_codes,
        decision.diagnostic_only,
        decision.source_digest,
    )


def evaluate_option_exit(
    window: PaperOptionPosition,
    underlying_source: object,
) -> OptionExitDecision:
    """Derive one diagnostic close requirement from a complete 1Min source."""
    material = _option_exit_material(window, underlying_source)
    decision = OptionExitDecision(
        position=window,
        underlying_source=underlying_source,
        decision_fact_source=material[6],
        review_session=material[0],
        evaluated_at=material[1],
        holding_sessions=material[2],
        dte=material[3],
        required_close_reason=material[4],
        reason_codes=material[5],
        diagnostic_only=True,
        source_digest=material[7],
    )
    identity = id(decision)

    def discard(dead: ReferenceType[OptionExitDecision]) -> None:
        with _OPTION_EXIT_DECISIONS_LOCK:
            current = _OPTION_EXIT_DECISIONS.get(identity)
            if current is not None and current[0] is dead:
                _OPTION_EXIT_DECISIONS.pop(identity, None)

    with _OPTION_EXIT_DECISIONS_LOCK:
        _OPTION_EXIT_DECISIONS[identity] = (
            ref(decision, discard),
            _option_exit_decision_fingerprint(decision),
            ref(window),
            ref(underlying_source),
        )
    return decision


def is_issued_option_exit_decision(decision: object) -> bool:
    if not isinstance(decision, OptionExitDecision):
        return False
    try:
        material = _option_exit_material(
            decision.position,
            decision.underlying_source,
        )
        fingerprint = _option_exit_decision_fingerprint(decision)
    except Exception:
        return False
    if (
        decision.review_session,
        decision.evaluated_at,
        decision.holding_sessions,
        decision.dte,
        decision.required_close_reason,
        decision.reason_codes,
        decision.decision_fact_source,
        decision.source_digest,
    ) != material:
        return False
    with _OPTION_EXIT_DECISIONS_LOCK:
        issued = _OPTION_EXIT_DECISIONS.get(id(decision))
        return (
            issued is not None
            and issued[0]() is decision
            and issued[1] == fingerprint
            and issued[2]() is decision.position
            and issued[3]() is decision.underlying_source
        )


class OptionPromotionStatus(str, Enum):
    PASSED = "PASSED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    RESTART_REQUIRED = "RESTART_REQUIRED"


@dataclass(frozen=True, slots=True)
class OptionWindow:
    """One immutable diagnostic prospective multi-trade Phase 2 cohort.

    This pure value is never a promotion authority.  Persistence must rederive
    the same facts from durable rows before any later live-phase gate can pass.
    """

    window_id: str
    started_session: date
    through_session: date
    closed_trade_net_rs: tuple[Decimal, ...]
    equity_curve: tuple[Decimal, ...]
    adherence_checks: tuple[bool, ...]
    hard_breach_codes: tuple[str, ...]
    missing_mark_sessions: tuple[date, ...]
    missing_exit_review_sessions: tuple[date, ...]
    missed_required_close_sessions: tuple[date, ...]
    record_complete: bool
    prior_window_id: str | None
    prior_window_source_digest: str | None
    source_digest: str

    def __post_init__(self) -> None:
        _text(self.window_id, "INVALID_OPTION_VALIDATION_WINDOW")
        if (
            type(self.started_session) is not date
            or type(self.through_session) is not date
            or self.through_session < self.started_session
        ):
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        if type(self.closed_trade_net_rs) is not tuple or any(
            type(value) is not Decimal or not value.is_finite()
            for value in self.closed_trade_net_rs
        ):
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        if type(self.equity_curve) is not tuple or not self.equity_curve:
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        canonical_curve = tuple(
            _canonical_money(
                value,
                "INVALID_OPTION_VALIDATION_EQUITY",
            )
            for value in self.equity_curve
        )
        if canonical_curve[0] != Decimal("5000"):
            raise OptionPaperError("OPTION_WINDOW_MUST_START_AT_5000")
        object.__setattr__(self, "equity_curve", canonical_curve)
        if type(self.adherence_checks) is not tuple or any(
            type(value) is not bool for value in self.adherence_checks
        ):
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        if (
            type(self.hard_breach_codes) is not tuple
            or any(
                type(value) is not str or not value
                for value in self.hard_breach_codes
            )
            or len(self.hard_breach_codes) != len(set(self.hard_breach_codes))
        ):
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        for sessions in (
            self.missing_mark_sessions,
            self.missing_exit_review_sessions,
            self.missed_required_close_sessions,
        ):
            if (
                type(sessions) is not tuple
                or any(type(value) is not date for value in sessions)
                or tuple(sorted(set(sessions))) != sessions
            ):
                raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        if type(self.record_complete) is not bool:
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        prior_values = (
            self.prior_window_id,
            self.prior_window_source_digest,
        )
        if any(value is not None for value in prior_values) and not (
            type(self.prior_window_id) is str
            and bool(self.prior_window_id)
            and type(self.prior_window_source_digest) is str
            and _SHA256.fullmatch(self.prior_window_source_digest) is not None
        ):
            raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
        _sha256(self.source_digest, "INVALID_OPTION_VALIDATION_WINDOW")

    @property
    def status(self) -> OptionPromotionStatus:
        return evaluate_option_window(self).status


def _validation_window_digest(
    *,
    window_id: str,
    started_session: date,
    through_session: date,
    closed_trade_net_rs: tuple[Decimal, ...],
    equity_curve: tuple[Decimal, ...],
    adherence_checks: tuple[bool, ...],
    hard_breach_codes: tuple[str, ...],
    missing_mark_sessions: tuple[date, ...],
    missing_exit_review_sessions: tuple[date, ...],
    missed_required_close_sessions: tuple[date, ...],
    record_complete: bool,
    prior_window_id: str | None,
    prior_window_source_digest: str | None,
) -> str:
    return _digest(
        "validation-window",
        window_id,
        started_session,
        through_session,
        *closed_trade_net_rs,
        "equity",
        *equity_curve,
        "adherence",
        *("1" if value else "0" for value in adherence_checks),
        "hard-breaches",
        *hard_breach_codes,
        "missing-marks",
        *missing_mark_sessions,
        "missing-exit-reviews",
        *missing_exit_review_sessions,
        "missed-required-closes",
        *missed_required_close_sessions,
        record_complete,
        prior_window_id,
        prior_window_source_digest,
    )


def _diagnostic_option_validation_window(
    *,
    window_id: str,
    started_session: date,
    through_session: date,
    closed_trade_net_rs: tuple[Decimal, ...],
    equity_curve: tuple[Decimal, ...],
    adherence_checks: tuple[bool, ...],
    hard_breach_codes: tuple[str, ...],
    missing_mark_sessions: tuple[date, ...],
    missing_exit_review_sessions: tuple[date, ...],
    missed_required_close_sessions: tuple[date, ...],
    record_complete: bool,
    prior_window: OptionWindow | None,
) -> OptionWindow:
    """Build a plain diagnostic aggregate with no authority semantics."""
    if prior_window is not None and not _diagnostic_window_is_well_formed(
        prior_window
    ):
        raise OptionPaperError("PRIOR_OPTION_WINDOW_UNVERIFIED")
    prior_window_id = None if prior_window is None else prior_window.window_id
    prior_digest = None if prior_window is None else prior_window.source_digest
    if type(equity_curve) is not tuple:
        raise OptionPaperError("INVALID_OPTION_VALIDATION_WINDOW")
    canonical_equity_curve = tuple(
        _canonical_money(
            value,
            "INVALID_OPTION_VALIDATION_EQUITY",
        )
        for value in equity_curve
    )
    source_digest = _validation_window_digest(
        window_id=window_id,
        started_session=started_session,
        through_session=through_session,
        closed_trade_net_rs=closed_trade_net_rs,
        equity_curve=canonical_equity_curve,
        adherence_checks=adherence_checks,
        hard_breach_codes=hard_breach_codes,
        missing_mark_sessions=missing_mark_sessions,
        missing_exit_review_sessions=missing_exit_review_sessions,
        missed_required_close_sessions=missed_required_close_sessions,
        record_complete=record_complete,
        prior_window_id=prior_window_id,
        prior_window_source_digest=prior_digest,
    )
    return OptionWindow(
        window_id=window_id,
        started_session=started_session,
        through_session=through_session,
        closed_trade_net_rs=closed_trade_net_rs,
        equity_curve=canonical_equity_curve,
        adherence_checks=adherence_checks,
        hard_breach_codes=hard_breach_codes,
        missing_mark_sessions=missing_mark_sessions,
        missing_exit_review_sessions=missing_exit_review_sessions,
        missed_required_close_sessions=missed_required_close_sessions,
        record_complete=record_complete,
        prior_window_id=prior_window_id,
        prior_window_source_digest=prior_digest,
        source_digest=source_digest,
    )


def _diagnostic_window_is_well_formed(window: object) -> bool:
    if not isinstance(window, OptionWindow):
        return False
    try:
        digest = _validation_window_digest(
            window_id=window.window_id,
            started_session=window.started_session,
            through_session=window.through_session,
            closed_trade_net_rs=window.closed_trade_net_rs,
            equity_curve=window.equity_curve,
            adherence_checks=window.adherence_checks,
            hard_breach_codes=window.hard_breach_codes,
            missing_mark_sessions=window.missing_mark_sessions,
            missing_exit_review_sessions=window.missing_exit_review_sessions,
            missed_required_close_sessions=(
                window.missed_required_close_sessions
            ),
            record_complete=window.record_complete,
            prior_window_id=window.prior_window_id,
            prior_window_source_digest=window.prior_window_source_digest,
        )
    except Exception:
        return False
    return digest == window.source_digest


def _ordered_maximum_drawdown(equity_curve: tuple[Decimal, ...]) -> Decimal:
    high_water_micros = _money_micros(
        equity_curve[0],
        "INVALID_OPTION_VALIDATION_EQUITY",
    )
    maximum_micros = 0
    for equity in equity_curve:
        equity_micros = _money_micros(
            equity,
            "INVALID_OPTION_VALIDATION_EQUITY",
        )
        high_water_micros = max(high_water_micros, equity_micros)
        maximum_micros = max(maximum_micros, high_water_micros - equity_micros)
    return money_from_micros(maximum_micros)


@dataclass(frozen=True, slots=True)
class OptionPromotionDecision:
    status: OptionPromotionStatus
    reason_codes: tuple[str, ...]
    closed_primary_trades: int
    elapsed_days: int
    mean_net_r: Decimal | None
    adherence: Decimal
    maximum_drawdown: Decimal
    diagnostic_only: bool
    promotion_authorized: bool


def evaluate_option_window(window: OptionWindow) -> OptionPromotionDecision:
    """Diagnose prospective gates without conferring promotion authority."""
    if not _diagnostic_window_is_well_formed(window):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    closed_count = len(window.closed_trade_net_rs)
    elapsed_days = (window.through_session - window.started_session).days
    if window.closed_trade_net_rs:
        with localcontext() as context:
            context.prec = 60
            mean_net_r = sum(
                window.closed_trade_net_rs,
                start=Decimal("0"),
            ) / Decimal(closed_count)
    else:
        mean_net_r = None
    with localcontext() as context:
        context.prec = 60
        adherence = (
            Decimal(sum(window.adherence_checks))
            / Decimal(len(window.adherence_checks))
            if window.adherence_checks
            else Decimal("0")
        )
    maximum_drawdown = _ordered_maximum_drawdown(window.equity_curve)
    reasons: list[str] = []
    if closed_count < 20:
        reasons.append("MINIMUM_CLOSED_OPTION_TRADES_NOT_MET")
    if elapsed_days < 28:
        reasons.append("MINIMUM_PHASE2_DAYS_NOT_MET")
    if mean_net_r is None or mean_net_r <= _ZERO:
        reasons.append("MEAN_OPTION_NET_R_NOT_POSITIVE")
    if adherence < Decimal("0.9"):
        reasons.append("OPTION_ADHERENCE_BELOW_90_PERCENT")
    if maximum_drawdown > Decimal("250"):
        reasons.append("OPTION_MAX_DRAWDOWN_ABOVE_250")
    reasons.extend(window.hard_breach_codes)
    restart_required = not window.record_complete or any(
        (
            window.missing_mark_sessions,
            window.missing_exit_review_sessions,
            window.missed_required_close_sessions,
        )
    )
    if restart_required:
        restart_reasons: list[str] = []
        if not window.record_complete or window.missing_mark_sessions:
            restart_reasons.append("INCOMPLETE_OPTION_MARKS")
        if window.missing_exit_review_sessions:
            restart_reasons.append("INCOMPLETE_OPTION_EXIT_REVIEWS")
        if window.missed_required_close_sessions:
            restart_reasons.append("MISSED_REQUIRED_OPTION_CLOSE")
        reasons[0:0] = restart_reasons
        status = OptionPromotionStatus.RESTART_REQUIRED
    elif window.hard_breach_codes:
        status = OptionPromotionStatus.FAILED
    elif reasons:
        status = OptionPromotionStatus.IN_PROGRESS
    else:
        status = OptionPromotionStatus.PASSED
    return OptionPromotionDecision(
        status=status,
        reason_codes=tuple(dict.fromkeys(reasons)),
        closed_primary_trades=closed_count,
        elapsed_days=elapsed_days,
        mean_net_r=mean_net_r,
        adherence=adherence,
        maximum_drawdown=maximum_drawdown,
        diagnostic_only=True,
        promotion_authorized=False,
    )


@dataclass(frozen=True, slots=True)
class OptionWindowStart:
    prior_window_id: str
    prior_window_source_digest: str
    started_session: date
    recorded_at: datetime
    source_digest: str


def _diagnostic_option_window_start(
    window: OptionWindow,
    *,
    started_session: date,
    recorded_at: datetime,
) -> OptionWindowStart:
    if not _diagnostic_window_is_well_formed(window):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    if evaluate_option_window(window).status is not (
        OptionPromotionStatus.RESTART_REQUIRED
    ):
        raise OptionPaperError("OPTION_WINDOW_RESTART_NOT_REQUIRED")
    if (
        type(started_session) is not date
        or started_session <= window.through_session
    ):
        raise OptionPaperError("INVALID_OPTION_WINDOW_START")
    recorded = _aware(recorded_at, "INVALID_OPTION_WINDOW_START")
    if recorded.date() != started_session:
        raise OptionPaperError("INVALID_OPTION_WINDOW_START")
    return OptionWindowStart(
        prior_window_id=window.window_id,
        prior_window_source_digest=window.source_digest,
        started_session=started_session,
        recorded_at=recorded,
        source_digest=_digest(
            "diagnostic-window-start",
            window.window_id,
            window.source_digest,
            started_session,
            recorded,
        ),
    )


def _diagnostic_window_start_matches(
    start: object,
    window: OptionWindow,
) -> bool:
    if not isinstance(start, OptionWindowStart):
        return False
    try:
        expected_digest = _digest(
            "diagnostic-window-start",
            window.window_id,
            window.source_digest,
            start.started_session,
            start.recorded_at,
        )
    except Exception:
        return False
    return (
        start.prior_window_id == window.window_id
        and start.prior_window_source_digest == window.source_digest
        and start.source_digest == expected_digest
    )


def start_next_window(
    window: OptionWindow,
    start_event: object | None,
) -> OptionWindow:
    """Start a fresh prospective cohort only from an exact explicit event."""
    if not _diagnostic_window_is_well_formed(window):
        raise OptionPaperError("OPTION_WINDOW_UNVERIFIED")
    if evaluate_option_window(window).status is not (
        OptionPromotionStatus.RESTART_REQUIRED
    ):
        raise OptionPaperError("OPTION_WINDOW_RESTART_NOT_REQUIRED")
    if start_event is None:
        return window
    if not _diagnostic_window_start_matches(start_event, window):
        raise OptionPaperError("OPTION_WINDOW_START_UNVERIFIED")
    return _diagnostic_option_validation_window(
        window_id=(
            f"{window.window_id}:restart:{start_event.source_digest[:12]}"
        ),
        started_session=start_event.started_session,
        through_session=start_event.started_session,
        closed_trade_net_rs=(),
        equity_curve=(Decimal("5000"),),
        adherence_checks=(),
        hard_breach_codes=(),
        missing_mark_sessions=(),
        missing_exit_review_sessions=(),
        missed_required_close_sessions=(),
        record_complete=True,
        prior_window=window,
    )


def _paper_only_boundary(*_args: object, **_kwargs: object) -> None:
    raise PaperOnlyBoundaryError("PAPER_ONLY_OPTION_BOUNDARY")


def exercise_option(*args: object, **kwargs: object) -> None:
    _paper_only_boundary(*args, **kwargs)


def roll_option(*args: object, **kwargs: object) -> None:
    _paper_only_boundary(*args, **kwargs)


def assign_option(*args: object, **kwargs: object) -> None:
    _paper_only_boundary(*args, **kwargs)


def hold_through_expiration(*args: object, **kwargs: object) -> None:
    _paper_only_boundary(*args, **kwargs)


__all__ = [
    "ClosedPaperOption",
    "OptionContract",
    "OptionEquityPoint",
    "OptionExitDecision",
    "OptionMark",
    "OptionPaperError",
    "OptionPromotionStatus",
    "OptionSelection",
    "OptionWindow",
    "OptionPromotionDecision",
    "OptionWindowStart",
    "OptionWindowStatus",
    "PaperOptionPosition",
    "PaperOnlyBoundaryError",
    "Phase2Authorization",
    "ProviderOptionFacts",
    "assign_option",
    "close_option_window",
    "eligible_option_contracts",
    "evaluate_option_exit",
    "evaluate_option_window",
    "exercise_option",
    "hold_through_expiration",
    "is_authorized_phase2_signal",
    "is_issued_option_exit_decision",
    "is_issued_option_selection",
    "is_issued_option_window",
    "is_issued_phase2_authorization",
    "open_option_window",
    "option_contract_reason_codes",
    "option_sort_key",
    "rank_option_contracts",
    "record_option_mark",
    "roll_option",
    "select_paper_long_call",
    "start_next_window",
]
