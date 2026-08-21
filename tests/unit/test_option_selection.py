from __future__ import annotations

import copy
import hashlib
import inspect
import json
import tempfile
import unittest
from unittest import mock
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from weakref import ref
from zoneinfo import ZoneInfo

import stock_monitor.journal as journal_module
import stock_monitor.options_paper as options_module
import stock_monitor.providers.alpaca as alpaca_module
import stock_monitor.validation as validation_module
from stock_monitor.options_paper import (
    OptionContract,
    OptionPaperError,
    Phase2Authorization,
    ProviderOptionFacts,
    _derive_reviewed_option_chain,
    _issue_phase2_authorization,
    eligible_option_contracts,
    is_issued_phase2_authorization,
    rank_option_contracts,
    select_paper_long_call,
)
from stock_monitor.config import load_fee_schedule
from stock_monitor.domain import money_to_micros
from stock_monitor.journal import (
    Journal,
    JournalActionSource,
    Phase1SignalSource,
    Phase1SignalEvidenceSource,
    Phase1ValidationWindowSource,
    Phase2AuthorizationSource,
    Phase2EventExclusionSource,
    Phase2ManualOptionReviewSource,
    Phase2OptionChainFactSource,
    Phase2OptionChainPageSource,
    Phase2OptionChainSource,
    Phase2PortfolioSource,
    Phase2WindowSource,
)
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.providers.alpaca import AlpacaMarketData
from stock_monitor.providers.http import HttpResponse
from stock_monitor.risk import SessionCalendarResolver
from stock_monitor.validation import PromotionDecision, PromotionStatus
from tests.support import credentials


ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[2]
FEE_FIXTURE = ROOT / "tests" / "fixtures" / "options" / "reviewed-fees.json"
SIGNAL_SESSION = date(2026, 8, 18)
PROMOTION_CUTOFF = datetime(2026, 8, 17, 16, 0, tzinfo=ET)


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def _signal_source(
    *,
    row_id: int,
    signal_id: str,
    validation_window_id: str = "phase1-window",
    role: str = "PRIMARY",
    publication_session: date = SIGNAL_SESSION,
    published_at: datetime | None = None,
    received_at: datetime | None = None,
) -> Phase1SignalSource:
    published = published_at or _at(publication_session, 8)
    received = received_at or (published + timedelta(minutes=1))
    digest = f"{row_id:064x}"
    return Phase1SignalSource(
        row_id=row_id,
        row_sha256=digest,
        signal_id=signal_id,
        validation_window_id=validation_window_id,
        symbol="SPY",
        subject_kind="ETF",
        issuer_cik=None,
        role=role,
        publication_session=publication_session,
        maximum_entry_micros=500_000_000,
        recommended_stop_micros=490_000_000,
        target_micros=520_000_000,
        planned_shares=1,
        tick_size_micros=10_000,
        trigger_price_micros=495_000_000,
        publication_report_row_id=row_id,
        publication_report_id=f"report-{row_id}",
        publication_rank=1,
        publication_source_digest="1" * 64,
        publication_state_digest="2" * 64,
        publication_content_digest="3" * 64,
        publication_observation_set_digest="4" * 64,
        publication_decision_digest="5" * 64,
        primary_plan_digest="6" * 64,
        policy_digest="7" * 64,
        calendar_digest="8" * 64,
        published_at=published,
        received_at=received,
        query_cutoff=received + timedelta(minutes=1),
        publication_source=None,  # type: ignore[arg-type]
        row_references=(),
        source_digest=digest,
    )


def _register_phase1(journal: Journal, registry: object, source: object) -> None:
    """Install test-only Phase 1 identity without a production registrar."""
    identity = id(source)

    def discard(dead):
        with journal_module._JOURNAL_SOURCE_LOCK:
            current = registry.get(identity)
            if current is not None and current[0] is dead:
                registry.pop(identity, None)

    with journal_module._JOURNAL_SOURCE_LOCK:
        registry[identity] = (
            ref(source, discard),
            journal_module._phase1_source_fingerprint(
                source,
                read_cache=journal._phase1_fingerprint_read_cache,
            ),
            ref(journal),
            journal_module._journal_source_authority_total_changes(journal),
            journal._source_authority_data_version(),
        )


def _register_signal(journal: Journal, source: Phase1SignalSource) -> None:
    _register_phase1(
        journal,
        journal_module._PHASE1_SIGNAL_SOURCE_AUTHORITIES,
        source,
    )


def _register_phase2(journal: Journal, source: object) -> None:
    """Install a test-only issued identity without reopening production APIs."""
    identity = id(source)

    def discard(dead):
        with journal_module._JOURNAL_SOURCE_LOCK:
            current = journal_module._PHASE2_SOURCE_AUTHORITIES.get(identity)
            if current is not None and current[0] is dead:
                journal_module._PHASE2_SOURCE_AUTHORITIES.pop(identity, None)
            binding = (
                journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES.get(
                    identity
                )
            )
            if binding is not None and binding.portfolio_reference is dead:
                journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES.pop(
                    identity,
                    None,
                )

    with journal_module._JOURNAL_SOURCE_LOCK:
        journal_module._PHASE2_SOURCE_AUTHORITIES[identity] = (
            ref(source, discard),
            journal_module._phase2_source_fingerprint(source),
            ref(journal),
            journal_module._journal_source_authority_total_changes(journal),
            journal._source_authority_data_version(),
        )


def _register_action(journal: Journal, source: JournalActionSource) -> None:
    identity = id(source)
    with journal_module._JOURNAL_SOURCE_LOCK:
        journal_module._ACTION_SOURCE_AUTHORITIES[identity] = (
            ref(source),
            journal_module._action_source_fingerprint(source),
            ref(journal),
            journal_module._journal_source_authority_total_changes(journal),
            journal._source_authority_data_version(),
        )


def _action_source(
    *,
    row_id: int,
    domain_kind: str,
    event_time: datetime,
    bid_micros: int | None = None,
    ask_micros: int | None = None,
    occ_symbol: str | None = None,
) -> JournalActionSource:
    details_json = json.dumps(
        {"normalized": {"occ_symbol": occ_symbol}},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return JournalActionSource(
        execution_event_id=row_id,
        event_id=f"event-{row_id}",
        raw_message_id=row_id,
        message_id=f"message-{row_id}",
        action_ordinal=0,
        idempotency_key=f"idempotency-{row_id}",
        storage_action=domain_kind,
        domain_kind=domain_kind,
        signal_id=None,
        symbol=None if domain_kind.startswith("OPTION_PAPER_") else "SPY",
        shares=None,
        price_micros=None,
        bid_micros=bid_micros,
        ask_micros=ask_micros,
        recommended_stop_micros=None,
        user_confirmed_stop_micros=None,
        event_time=event_time,
        message_time=event_time,
        received_at=event_time + timedelta(seconds=1),
        compliance_result="COMPLIANT",
        reconciliation_state="CLEAN",
        raw_text=domain_kind,
        raw_sha256=f"{row_id:064x}",
        details_json=details_json,
        details_sha256=hashlib.sha256(details_json.encode("utf-8")).hexdigest(),
        parent_order_id=None,
        fill_group_planned_shares=None,
        event_role=(
            "OBSERVATION"
            if domain_kind.startswith("OPTION_PAPER_")
            else "PRIMARY"
        ),
        account_check=None,
        acknowledgement_outbox_id=row_id,
        acknowledgement_destination="LOCAL",
        acknowledgement_idempotency_key=f"ack-{row_id}",
        acknowledgement_payload_sha256="1" * 64,
        row_references=(),
        source_digest=f"{row_id + 1000:064x}",
    )


@contextmanager
def issued_phase2_inputs():
    with tempfile.TemporaryDirectory() as directory:
        journal = Journal.open(Path(directory) / "journal.sqlite3")
        historical_signal = _signal_source(
            row_id=1,
            signal_id="phase1-historical",
            publication_session=date(2026, 8, 14),
            published_at=_at(date(2026, 8, 14), 8),
            received_at=_at(date(2026, 8, 14), 8, 1),
        )
        current_signal = _signal_source(
            row_id=2,
            signal_id="phase2-current-primary",
        )
        resolver = SessionCalendarResolver(
            (
                load_current_market_calendar(
                    ROOT,
                    as_of=SIGNAL_SESSION,
                ),
            )
        )
        from stock_monitor.risk import _calendar_digest

        calendar_digest = _calendar_digest(resolver)
        promotion_window = Phase1ValidationWindowSource(
            validation_window_id="phase1-window",
            started_session=date(2026, 7, 20),
            through_session=date(2026, 8, 14),
            starting_capital_micros=5_000_000_000,
            started_at=_at(date(2026, 7, 20), 8),
            received_at=PROMOTION_CUTOFF,
            calendar_digest=calendar_digest,
            expected_open_sessions=(),
            signal_sources=(historical_signal,),
            disposition_sources=(),
            canonical_history=None,  # type: ignore[arg-type]
            actual_history=None,  # type: ignore[arg-type]
            adherence_check_sources=(),
            adherence_review_sources=(),
            query_cutoff=PROMOTION_CUTOFF,
            signal_terminal_cursor=1,
            signal_source_highwater=1,
            lifecycle_terminal_cursor=None,
            lifecycle_source_highwater=0,
            adherence_terminal_cursor=None,
            adherence_source_highwater=0,
            expected_signal_count=1,
            expected_disposition_count=0,
            expected_adherence_count=0,
            row_references=(),
            source_digest="9" * 64,
        )
        _register_signal(journal, historical_signal)
        _register_signal(journal, current_signal)
        _register_phase1(
            journal,
            journal_module._PHASE1_VALIDATION_WINDOW_SOURCE_AUTHORITIES,
            promotion_window,
        )
        decision = PromotionDecision(
            passed=True,
            status=PromotionStatus.PASSED,
            reason_codes=(),
            closed_primary_trades=20,
            elapsed_days=28,
            mean_net_r=Decimal("0.1"),
            adherence=Decimal("0.9"),
            canonical_max_drawdown=Decimal("0"),
            actual_max_drawdown=Decimal("0"),
            source_digest=promotion_window.source_digest,
            authority_digest="a" * 64,
            _phase1_source=promotion_window,
        )
        with validation_module._ISSUED_PROMOTION_DECISIONS_LOCK:
            validation_module._ISSUED_PROMOTION_DECISIONS[id(decision)] = (
                ref(decision),
                validation_module._promotion_decision_fingerprint(decision),
                ref(promotion_window),
            )
        try:
            start_action = _action_source(
                row_id=10,
                domain_kind="OPTION_PAPER_WINDOW_START",
                event_time=_at(SIGNAL_SESSION, 7, 30),
            )
            _register_action(journal, start_action)
            phase2_window = Phase2WindowSource(
                row_id=10,
                window_id="phase2-window",
                validation_window_id=promotion_window.validation_window_id,
                promotion_source=promotion_window,
                promotion_decision_digest=decision.authority_digest,
                promotion_signal_ids=tuple(
                    source.signal_id
                    for source in promotion_window.signal_sources
                ),
                promotion_query_cutoff=promotion_window.query_cutoff,
                start_action=start_action,
                started_session=SIGNAL_SESSION,
                started_at=start_action.event_time,
                received_at=start_action.received_at,
                starting_capital_micros=5_000_000_000,
                calendar_digest=promotion_window.calendar_digest,
                query_cutoff=start_action.received_at,
                row_references=(),
                source_digest="b" * 64,
            )
            _register_phase2(journal, phase2_window)
            authorization = _issue_phase2_authorization(
                promotion_decision=decision,
                window_source=phase2_window,
                signal_source=current_signal,
                issued_at=current_signal.query_cutoff,
            )
            authorization_source = Phase2AuthorizationSource(
                row_id=11,
                authorization_id="phase2-authorization",
                window_source=phase2_window,
                signal_source=current_signal,
                authorized_at=authorization.issued_at,
                received_at=authorization.issued_at,
                authorization_digest=authorization.source_digest,
                row_references=(),
                source_digest="c" * 64,
            )
            _register_phase2(journal, authorization_source)
            yield SimpleNamespace(
                journal=journal,
                historical_signal=historical_signal,
                signal=current_signal,
                window=promotion_window,
                phase2_window=phase2_window,
                decision=decision,
                authorization=authorization,
                authorization_source=authorization_source,
                resolver=resolver,
            )
        finally:
            with validation_module._ISSUED_PROMOTION_DECISIONS_LOCK:
                validation_module._ISSUED_PROMOTION_DECISIONS.pop(
                    id(decision),
                    None,
                )
            journal.close()


def reviewed_fee_schedule():
    return load_fee_schedule(FEE_FIXTURE)


def event_source(
    inputs: SimpleNamespace,
    *,
    covered_sessions: tuple[date, ...] | None = None,
    event_sessions: tuple[date, ...] = (),
) -> Phase2EventExclusionSource:
    from stock_monitor.risk import _calendar_digest

    calendar_digest = _calendar_digest(inputs.resolver)
    evidence = Phase1SignalEvidenceSource(
        signal_source=inputs.signal,
        reviewed_bundle=None,
        evidence_decision=None,
        evidence_id="phase2-event-evidence",
        registry_payload=b"{}",
        release_sha256="1" * 64,
        source_documents=(),
        manifest_bytes=b"{}",
        manifest_digest="2" * 64,
        registry_source_row_id=1,
        source_observation_row_ids=(1,),
        registry_id="registry",
        registry_content_hash="3" * 64,
        bundle_digest="4" * 64,
        decision_digest="5" * 64,
        review_at=_at(SIGNAL_SESSION, 8, 45),
        query_cutoff=_at(SIGNAL_SESSION, 8, 46),
        calendar_digest=calendar_digest,
        source_observation_highwater=1,
        expected_source_observation_count=1,
        row_references=(),
        source_digest="6" * 64,
    )
    _register_phase1(
        inputs.journal,
        journal_module._PHASE1_SIGNAL_EVIDENCE_SOURCE_AUTHORITIES,
        evidence,
    )
    coverage = covered_sessions or tuple(
        inputs.resolver.add_sessions(SIGNAL_SESSION, offset)
        for offset in range(10)
    )
    source = Phase2EventExclusionSource(
        signal_source=inputs.signal,
        signal_evidence_source=evidence,
        covered_sessions=coverage,
        event_sessions=tuple(sorted(event_sessions)),
        reviewed_at=_at(SIGNAL_SESSION, 9),
        query_cutoff=_at(SIGNAL_SESSION, 9),
        calendar_digest=calendar_digest,
        evidence_source_row_ids=(1,),
        evidence_source_highwater=1,
        expected_evidence_source_count=1,
        row_references=(),
        source_digest=_source_digest(
            f"event-source:{coverage}:{event_sessions}"
        ),
        authority_digest=_source_digest(
            f"event-authority:{coverage}:{event_sessions}"
        ),
    )
    _register_phase2(inputs.journal, source)
    return source


def portfolio_source(
    inputs: SimpleNamespace,
    *,
    settled_cash_micros: int = 5_000_000_000,
    open_position_source: object | None = None,
    query_cutoff: datetime | None = None,
) -> Phase2PortfolioSource:
    from stock_monitor.risk import _calendar_digest

    cutoff = query_cutoff or _at(SIGNAL_SESSION, 10, 5)
    source = Phase2PortfolioSource(
        window_id=inputs.phase2_window.window_id,
        as_of=cutoff,
        query_cutoff=cutoff,
        settled_cash_micros=settled_cash_micros,
        economic_cash_micros=settled_cash_micros,
        equity_micros=settled_cash_micros,
        open_position_source=open_position_source,  # type: ignore[arg-type]
        settlement_sources=(),
        unsettled_proceeds_micros=0,
        calendar_digest=_calendar_digest(inputs.resolver),
        ledger_row_references=(),
        ledger_terminal_cursor=None,
        ledger_source_highwater=0,
        expected_entry_count=0,
        expected_exit_count=0,
        expected_fee_count=0,
        expected_equity_count=0,
        row_references=(),
        source_digest=_source_digest(
            f"portfolio:{settled_cash_micros}:{id(open_position_source)}:{cutoff}"
        ),
        authority_digest=_source_digest(
            f"portfolio-authority:{settled_cash_micros}:{id(open_position_source)}:{cutoff}"
        ),
    )
    _register_portfolio(
        inputs.journal,
        source,
        window_source=inputs.phase2_window,
    )
    return source


def _register_portfolio(
    journal: Journal,
    source: Phase2PortfolioSource,
    *,
    window_source: Phase2WindowSource,
) -> None:
    """Install a test-only portfolio with its exact Window lineage."""
    _register_phase2(journal, source)
    with journal_module._JOURNAL_SOURCE_LOCK:
        portfolio_issued = journal_module._PHASE2_SOURCE_AUTHORITIES[id(source)]
        window_issued = journal_module._PHASE2_SOURCE_AUTHORITIES[
            id(window_source)
        ]
        journal_module._PHASE2_PORTFOLIO_WINDOW_AUTHORITIES[id(source)] = (
            journal_module._Phase2PortfolioWindowAuthority(
                portfolio_reference=portfolio_issued[0],
                window_source=window_source,
                portfolio_issued=portfolio_issued,
                window_issued=window_issued,
            )
        )


def _occ(expiration: date, strike: Decimal, *, right: str = "CALL") -> str:
    marker = "C" if right == "CALL" else "P"
    strike_code = int(strike * Decimal("1000"))
    return f"SPY{expiration:%y%m%d}{marker}{strike_code:08d}"


def option_contract(
    *,
    expiration: date | None = None,
    strike: Decimal = Decimal("400"),
    option_type: str = "CALL",
    delta: Decimal = Decimal("0.35"),
    bid: Decimal | None = Decimal("0.38"),
    ask: Decimal | None = Decimal("0.40"),
    open_interest: int | None = 1_500,
    daily_volume: int | None = 150,
    occ_symbol: str | None = None,
    underlying: str = "SPY",
    provider_overrides: dict[str, object] | None = None,
) -> OptionContract:
    expiry = expiration or (SIGNAL_SESSION + timedelta(days=45))
    symbol = occ_symbol or _occ(expiry, strike, right=option_type)
    observed = _at(SIGNAL_SESSION, 10)
    provider_values: dict[str, object] = {
        "occ_symbol": symbol,
        "underlying": underlying,
        "expiration": expiry,
        "strike": strike,
        "option_type": option_type,
        "delta": delta,
        "bid": bid,
        "ask": ask,
        "daily_volume": daily_volume,
        "observed_at": observed - timedelta(minutes=1),
        "source_observation_id": f"alpaca:{symbol}",
    }
    provider_values.update(provider_overrides or {})
    return OptionContract(
        occ_symbol=symbol,
        underlying=underlying,
        expiration=expiry,
        strike=strike,
        option_type=option_type,
        delta=delta,
        bid=bid,
        ask=ask,
        open_interest=open_interest,
        daily_volume=daily_volume,
        observed_at=observed,
        manual_source_id=f"robinhood-manual:{symbol}",
        provider_facts=ProviderOptionFacts(**provider_values),
    )


def _source_digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def reviewed_chain(inputs: SimpleNamespace, contracts) -> object:
    requested = tuple(contracts)
    snapshots: dict[str, object] = {}
    for contract in requested:
        provider = contract.provider_facts
        value: dict[str, object] = {}
        if provider.bid is not None and provider.ask is not None:
            value["latestQuote"] = {
                "t": provider.observed_at.astimezone(UTC)
                .isoformat()
                .replace("+00:00", "Z"),
                "bp": str(provider.bid),
                "ap": str(provider.ask),
            }
        if provider.delta is not None:
            value["greeks"] = {"delta": str(provider.delta)}
        if provider.daily_volume is not None:
            value["dailyBar"] = {"v": provider.daily_volume}
        snapshots[contract.occ_symbol] = value
    body = json.dumps(
        {"snapshots": snapshots, "next_page_token": None},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    class Transport:
        def get(self, url: str, headers: dict[str, str]) -> HttpResponse:
            return HttpResponse(
                status=200,
                headers=(("Content-Type", "application/json"),),
                body=body,
                url=url,
            )

    provider_chain = AlpacaMarketData(
        Transport(),
        credentials(),
        now=lambda: _at(SIGNAL_SESSION, 9, 59).astimezone(UTC)
        + timedelta(seconds=30),
    ).option_chain("SPY")
    bundle = alpaca_module.read_provider_fetch_bundle(provider_chain)
    page_sources: list[Phase2OptionChainPageSource] = []
    for bundle_page in bundle.pages:
        page = bundle_page.page
        observation = bundle_page.observation
        source = Phase2OptionChainPageSource(
            row_id=100 + page.page_ordinal,
            page_id=_source_digest(f"page:{page.page_ordinal}"),
            chain_set_id="chain-set",
            page_ordinal=page.page_ordinal,
            source_observation_row_id=1000 + page.page_ordinal,
            external_source_observation_id=page.source_observation_id,
            source_type=page.source_type,
            request_url=page.request_url,
            request_page_token=page.request_page_token,
            next_page_token=page.next_page_token,
            payload_sha256=page.payload_sha256,
            source_time=observation.source_timestamp,
            retrieved_at=observation.retrieved_at,
            raw_payload=bundle_page.payload,
            row_references=(),
            source_digest=_source_digest(
                f"page-source:{page.page_ordinal}"
            ),
        )
        _register_phase2(inputs.journal, source)
        page_sources.append(source)
    fact_sources: list[Phase2OptionChainFactSource] = []
    for ordinal, snapshot in enumerate(provider_chain, start=1):
        provider_source = alpaca_module._normalized_market_fact_source(
            snapshot
        )
        fact = Phase2OptionChainFactSource(
            row_id=200 + ordinal,
            snapshot_id=_source_digest(f"provider-snapshot:{snapshot.occ_symbol}"),
            authorization_id=inputs.authorization_source.authorization_id,
            chain_set_id="chain-set",
            occ_symbol=snapshot.occ_symbol,
            underlying=snapshot.underlying,
            expiration=snapshot.expiration,
            strike_micros=money_to_micros(snapshot.strike),
            delta_micros=(
                None
                if snapshot.delta is None
                else int(snapshot.delta * Decimal(1_000_000))
            ),
            bid_micros=(
                None if snapshot.bid is None else money_to_micros(snapshot.bid)
            ),
            ask_micros=(
                None if snapshot.ask is None else money_to_micros(snapshot.ask)
            ),
            daily_volume=snapshot.daily_volume,
            source_observation_row_id=1000 + provider_source.page_ordinal,
            external_source_observation_id=(
                provider_source.source_observation_id
            ),
            fetch_page_ordinal=provider_source.page_ordinal,
            source_item_ordinal=provider_source.source_item_ordinal,
            source_item_path=provider_source.source_item_path,
            payload_sha256=provider_source.page_payload_sha256,
            provider_fact_digest=provider_source.normalized_fields_digest,
            observed_at=snapshot.observed_at,
            received_at=bundle.pages[
                provider_source.page_ordinal - 1
            ].observation.retrieved_at,
            snapshot=snapshot,
            row_references=(),
            source_digest=_source_digest(
                f"fact-source:{snapshot.occ_symbol}"
            ),
        )
        _register_phase2(inputs.journal, fact)
        fact_sources.append(fact)
    candidate_facts = tuple(
        fact
        for fact in fact_sources
        if options_module._provider_fact_is_review_candidate(
            fact,
            authorization=inputs.authorization,
        )
    )
    query_cutoff = max(page.retrieved_at for page in page_sources)
    chain_source = Phase2OptionChainSource(
        row_id=300,
        chain_set_id="chain-set",
        authorization_source=inputs.authorization_source,
        underlying="SPY",
        collection_name="snapshots",
        requested_symbols=("SPY",),
        request_digest=bundle.manifest.request_digest,
        manifest_digest=bundle.manifest.manifest_digest,
        pages=tuple(page_sources),
        facts=tuple(fact_sources),
        provider_chain=provider_chain,
        review_candidate_fact_digests=tuple(
            fact.provider_fact_digest for fact in candidate_facts
        ),
        expected_manual_review_count=len(candidate_facts),
        expected_page_count=len(page_sources),
        expected_fact_count=len(fact_sources),
        terminal=True,
        query_cutoff=query_cutoff,
        received_at=query_cutoff,
        row_references=(),
        source_digest=_source_digest("option-chain-source"),
    )
    _register_phase2(inputs.journal, chain_source)
    by_occ = {contract.occ_symbol: contract for contract in requested}
    manual_sources: list[Phase2ManualOptionReviewSource] = []
    for ordinal, fact in enumerate(candidate_facts, start=1):
        contract = by_occ[fact.occ_symbol]
        observed_at = contract.observed_at
        action = _action_source(
            row_id=400 + ordinal,
            domain_kind="OPTION_PAPER_REVIEW",
            event_time=observed_at,
            bid_micros=money_to_micros(contract.bid),
            ask_micros=money_to_micros(contract.ask),
            occ_symbol=contract.occ_symbol,
        )
        _register_action(inputs.journal, action)
        manual = Phase2ManualOptionReviewSource(
            row_id=400 + ordinal,
            snapshot_id=_source_digest(f"manual:{fact.occ_symbol}"),
            authorization_id=inputs.authorization_source.authorization_id,
            chain_set_id=chain_source.chain_set_id,
            provider_fact_source=fact,
            action_source=action,
            occ_symbol=contract.occ_symbol,
            underlying=contract.underlying,
            expiration=contract.expiration,
            strike_micros=money_to_micros(contract.strike),
            delta_micros=int(contract.delta * Decimal(1_000_000)),
            bid_micros=money_to_micros(contract.bid),
            ask_micros=money_to_micros(contract.ask),
            open_interest=contract.open_interest,
            daily_volume=contract.daily_volume,
            observed_at=observed_at,
            received_at=action.received_at,
            row_references=(),
            source_digest=_source_digest(f"manual-source:{fact.occ_symbol}"),
        )
        _register_phase2(inputs.journal, manual)
        manual_sources.append(manual)
    derived = _derive_reviewed_option_chain(
        chain_source,
        tuple(manual_sources),
        inputs.authorization,
    )
    inputs.chain_source = chain_source
    inputs.manual_review_sources = tuple(manual_sources)
    return derived


def rank_with(inputs: SimpleNamespace, contracts, **overrides: object):
    arguments = {
        "authorization": inputs.authorization,
        "fee_schedule": reviewed_fee_schedule(),
        "event_exclusion_source": event_source(inputs),
        "calendar_resolver": inputs.resolver,
        "portfolio_source": portfolio_source(inputs),
    }
    arguments.update(overrides)
    eligible = eligible_option_contracts(
        reviewed_chain(inputs, contracts),
        **arguments,
    )
    return rank_option_contracts(eligible, inputs.authorization.signal)


class Phase2AuthorizationTests(unittest.TestCase):
    def test_authorization_requires_the_exact_started_phase2_window_source(
        self,
    ) -> None:
        parameters = inspect.signature(_issue_phase2_authorization).parameters

        self.assertIn("window_source", parameters)
        self.assertNotIn("phase2_window", parameters)

    def test_authorization_reissues_from_nested_sources_without_registry_state(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            with options_module._PHASE2_AUTHORIZATIONS_LOCK:
                saved_authorizations = dict(
                    options_module._PHASE2_AUTHORIZATIONS
                )
                saved_signals = dict(options_module._AUTHORIZED_PHASE2_SIGNALS)
                options_module._PHASE2_AUTHORIZATIONS.clear()
                options_module._AUTHORIZED_PHASE2_SIGNALS.clear()
            try:
                with mock.patch.object(
                    validation_module,
                    "_issue_phase1_promotion_from_journal_source",
                    return_value=inputs.decision,
                ) as promotion_reissuer:
                    restored = options_module._authorization_for_source(
                        inputs.authorization_source,
                        calendar_resolver=inputs.resolver,
                    )

                self.assertIsNotNone(restored)
                assert restored is not None
                self.assertIsNot(restored, inputs.authorization)
                self.assertEqual(
                    restored.source_digest,
                    inputs.authorization_source.authorization_digest,
                )
                promotion_reissuer.assert_called_once_with(
                    inputs.phase2_window.promotion_source,
                    calendar_resolver=inputs.resolver,
                )
            finally:
                with options_module._PHASE2_AUTHORIZATIONS_LOCK:
                    options_module._PHASE2_AUTHORIZATIONS.clear()
                    options_module._PHASE2_AUTHORIZATIONS.update(
                        saved_authorizations
                    )
                    options_module._AUTHORIZED_PHASE2_SIGNALS.clear()
                    options_module._AUTHORIZED_PHASE2_SIGNALS.update(
                        saved_signals
                    )

    def test_authorization_binds_exact_current_decision_signal_and_owner(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            self.assertTrue(
                is_issued_phase2_authorization(inputs.authorization)
            )
            self.assertFalse(
                is_issued_phase2_authorization(copy.copy(inputs.authorization))
            )
            self.assertIs(
                inputs.authorization.promotion_decision,
                inputs.decision,
            )
            self.assertIs(inputs.authorization.signal_source, inputs.signal)

            self.assertEqual(
                tuple(inspect.signature(rank_option_contracts).parameters),
                ("chain", "signal"),
            )
            self.assertEqual(
                inputs.authorization.signal.signal_id,
                inputs.signal.signal_id,
            )
            reviewed = reviewed_chain(inputs, (option_contract(),))
            eligible = eligible_option_contracts(
                reviewed,
                authorization=inputs.authorization,
                fee_schedule=reviewed_fee_schedule(),
                event_exclusion_source=event_source(inputs),
                calendar_resolver=inputs.resolver,
                portfolio_source=portfolio_source(inputs),
            )
            for untrusted_signal in (
                copy.copy(inputs.authorization.signal),
                replace(
                    inputs.authorization.signal,
                    signal_id="unregistered-phase2-signal",
                ),
            ):
                with self.subTest(
                    untrusted_signal=untrusted_signal,
                ), self.assertRaisesRegex(
                    OptionPaperError,
                    "PHASE2_SIGNAL_AUTHORIZATION_UNVERIFIED",
                ):
                    rank_option_contracts(eligible, untrusted_signal)

            invalid_pairs = (
                (True, inputs.signal, "PHASE1_PROMOTION_UNVERIFIED"),
                (
                    copy.copy(inputs.decision),
                    inputs.signal,
                    "PHASE1_PROMOTION_UNVERIFIED",
                ),
                (
                    inputs.decision,
                    copy.copy(inputs.signal),
                    "PHASE1_SIGNAL_UNVERIFIED",
                ),
            )
            for decision, signal, reason in invalid_pairs:
                with self.subTest(reason=reason), self.assertRaisesRegex(
                    OptionPaperError,
                    reason,
                ):
                    _issue_phase2_authorization(
                        promotion_decision=decision,
                        window_source=inputs.phase2_window,
                        signal_source=signal,
                        issued_at=inputs.signal.query_cutoff,
                    )

            with issued_phase2_inputs() as foreign:
                with self.assertRaisesRegex(
                    OptionPaperError,
                    "PHASE2_SOURCE_OWNER_MISMATCH",
                ):
                    _issue_phase2_authorization(
                        promotion_decision=inputs.decision,
                        window_source=inputs.phase2_window,
                        signal_source=foreign.signal,
                        issued_at=foreign.signal.query_cutoff,
                    )

    def test_authorization_requires_new_post_promotion_bullish_primary(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            with self.assertRaisesRegex(
                OptionPaperError,
                "PHASE2_SIGNAL_ALREADY_IN_PROMOTION_WINDOW",
            ):
                _issue_phase2_authorization(
                    promotion_decision=inputs.decision,
                    window_source=inputs.phase2_window,
                    signal_source=inputs.historical_signal,
                    issued_at=inputs.signal.query_cutoff,
                )

            cases = (
                (
                    _signal_source(
                        row_id=3,
                        signal_id="wrong-window",
                        validation_window_id="another-window",
                    ),
                    {},
                    "PHASE2_VALIDATION_WINDOW_MISMATCH",
                ),
                (
                    _signal_source(
                        row_id=4,
                        signal_id="at-cutoff",
                        publication_session=PROMOTION_CUTOFF.date(),
                        published_at=PROMOTION_CUTOFF.replace(hour=9),
                        received_at=PROMOTION_CUTOFF,
                    ),
                    {},
                    "PHASE2_SIGNAL_NOT_AFTER_PROMOTION",
                ),
                (
                    _signal_source(
                        row_id=5,
                        signal_id="shadow",
                        role="WATCHLIST_SHADOW",
                    ),
                    {},
                    "PHASE2_REQUIRES_PRIMARY_SIGNAL",
                ),
                (
                    inputs.signal,
                    {"strategy": "BEARISH_LONG_PUT"},
                    "PHASE2_REQUIRES_BULLISH_LONG_CALL",
                ),
            )
            for signal, kwargs, reason in cases:
                with self.subTest(reason=reason):
                    if signal is not inputs.signal:
                        _register_signal(inputs.journal, signal)
                    with self.assertRaisesRegex(OptionPaperError, reason):
                        _issue_phase2_authorization(
                            promotion_decision=inputs.decision,
                            window_source=inputs.phase2_window,
                            signal_source=signal,
                            issued_at=max(
                                inputs.signal.query_cutoff,
                                signal.query_cutoff,
                            ),
                            **kwargs,
                        )


class OptionSelectionTests(unittest.TestCase):
    def test_portfolio_rejects_same_row_window_reissue_substitution(self) -> None:
        cutoff = _at(SIGNAL_SESSION, 10, 5)
        portfolio = Phase2PortfolioSource(
            window_id="phase2-window",
            as_of=cutoff,
            query_cutoff=cutoff,
            settled_cash_micros=5_000_000_000,
            economic_cash_micros=5_000_000_000,
            equity_micros=5_000_000_000,
            open_position_source=None,
            settlement_sources=(),
            unsettled_proceeds_micros=0,
            calendar_digest="a" * 64,
            ledger_row_references=(),
            ledger_terminal_cursor=None,
            ledger_source_highwater=0,
            expected_entry_count=0,
            expected_exit_count=0,
            expected_fee_count=0,
            expected_equity_count=1,
            row_references=(),
            source_digest="b" * 64,
            authority_digest="c" * 64,
        )
        authorization = SimpleNamespace(
            window_source=SimpleNamespace(window_id=portfolio.window_id),
            issued_at=cutoff - timedelta(hours=1),
        )
        exclusion = SimpleNamespace(query_cutoff=cutoff - timedelta(minutes=1))
        with mock.patch.object(
            journal_module,
            "is_verified_phase2_portfolio_source",
            return_value=True,
        ), mock.patch.object(
            journal_module,
            "phase2_sources_share_owner",
            return_value=True,
        ), mock.patch.object(
            journal_module,
            "phase2_portfolio_source_binds_window",
            return_value=False,
        ):
            self.assertEqual(
                options_module._portfolio_reason(
                    portfolio,
                    authorization=authorization,
                    event_exclusion_source=exclusion,
                ),
                "PHASE2_PORTFOLIO_SOURCE_MISMATCH",
            )

    def test_bare_option_contract_sequence_has_no_selection_authority(self) -> None:
        raw_chain = (option_contract(),)

        calls = (
            lambda: eligible_option_contracts(
                raw_chain,
                authorization=object(),  # type: ignore[arg-type]
                fee_schedule=object(),
                event_exclusion_source=object(),
                calendar_resolver=object(),
                portfolio_source=object(),
            ),
            lambda: rank_option_contracts(raw_chain, object()),
        )
        for call in calls:
            with self.subTest(call=call), self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_CHAIN_AUTHORITY_UNVERIFIED",
            ):
                call()

        raw_authorization = Phase2Authorization(
            promotion_decision=object(),
            window_source=object(),
            signal_source=object(),
            signal=object(),
            strategy="BULLISH_LONG_CALL",
            issued_at=_at(SIGNAL_SESSION, 9),
            source_digest="f" * 64,
        )
        caller_built = options_module._ReviewedOptionChain(
            contracts=raw_chain,
            authorization=raw_authorization,
            chain_source=object(),
            manual_review_sources=(object(),),
            stage="ELIGIBLE",
            source_digest="e" * 64,
        )
        with self.assertRaisesRegex(
            OptionPaperError,
            "OPTION_CHAIN_AUTHORITY_UNVERIFIED",
        ):
            rank_option_contracts(caller_built, raw_authorization.signal)

    def test_reviewed_chain_derivation_requires_exact_journal_sources(
        self,
    ) -> None:
        self.assertEqual(
            tuple(
                inspect.signature(
                    options_module._derive_reviewed_option_chain
                ).parameters
            ),
            ("chain_source", "manual_review_sources", "authorization"),
        )
        for name in (
            "Phase2OptionChainSource",
            "Phase2ManualOptionReviewSource",
            "is_verified_phase2_option_chain_source",
            "is_verified_phase2_manual_option_review_source",
        ):
            with self.subTest(name=name):
                self.assertTrue(hasattr(journal_module, name))

    def test_reviewed_chain_rejects_copy_subset_reorder_and_missing_review(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            contracts = (
                option_contract(strike=Decimal("400")),
                option_contract(strike=Decimal("401")),
            )
            reviewed = reviewed_chain(inputs, contracts)
            arguments = {
                "authorization": inputs.authorization,
                "fee_schedule": reviewed_fee_schedule(),
                "event_exclusion_source": event_source(inputs),
                "calendar_resolver": inputs.resolver,
                "portfolio_source": portfolio_source(inputs),
            }
            eligible = eligible_option_contracts(reviewed, **arguments)
            self.assertEqual(len(eligible), 2)

            for invalid in (
                copy.copy(reviewed),
                tuple(reviewed)[1:],
                tuple(reversed(tuple(reviewed))),
                (*tuple(reviewed), option_contract(strike=Decimal("402"))),
            ):
                with self.subTest(invalid=invalid), self.assertRaisesRegex(
                    OptionPaperError,
                    "OPTION_CHAIN_AUTHORITY_UNVERIFIED",
                ):
                    eligible_option_contracts(invalid, **arguments)

            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_CHAIN_REVIEW_INCOMPLETE",
            ):
                _derive_reviewed_option_chain(
                    inputs.chain_source,
                    inputs.manual_review_sources[1:],
                    inputs.authorization,
                )
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_REVIEW_AUTHORITY_UNVERIFIED",
            ):
                _derive_reviewed_option_chain(
                    inputs.chain_source,
                    (
                        copy.copy(inputs.manual_review_sources[0]),
                        inputs.manual_review_sources[1],
                    ),
                    inputs.authorization,
                )

            mismatched = replace(
                inputs.manual_review_sources[0],
                ask_micros=inputs.manual_review_sources[0].ask_micros + 1,
            )
            _register_phase2(inputs.journal, mismatched)
            with self.assertRaisesRegex(
                OptionPaperError,
                "PROVIDER_MANUAL_MISMATCH",
            ):
                _derive_reviewed_option_chain(
                    inputs.chain_source,
                    (mismatched, inputs.manual_review_sources[1]),
                    inputs.authorization,
                )

    def test_reviewed_chain_rejects_post_issue_provider_snapshot_mutation(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            reviewed_chain(inputs, (option_contract(),))
            snapshot = inputs.chain_source.facts[0].snapshot
            object.__setattr__(snapshot, "ask", Decimal("0.41"))

            self.assertFalse(
                journal_module.is_verified_phase2_option_chain_fact_source(
                    inputs.chain_source.facts[0]
                )
            )
            self.assertFalse(
                journal_module.is_verified_phase2_option_chain_source(
                    inputs.chain_source
                )
            )
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_CHAIN_AUTHORITY_UNVERIFIED",
            ):
                _derive_reviewed_option_chain(
                    inputs.chain_source,
                    inputs.manual_review_sources,
                    inputs.authorization,
                )

    def test_selection_accepts_only_durable_event_and_portfolio_authorities(
        self,
    ) -> None:
        expected_keyword_parameters = {
            "authorization",
            "fee_schedule",
            "event_exclusion_source",
            "calendar_resolver",
            "portfolio_source",
        }
        for function in (eligible_option_contracts, select_paper_long_call):
            with self.subTest(function=function.__name__):
                parameters = inspect.signature(function).parameters
                self.assertEqual(
                    set(parameters) - {"chain"},
                    expected_keyword_parameters,
                )
                self.assertNotIn("event_window", parameters)
                self.assertNotIn("open_position", parameters)

        self.assertFalse(hasattr(journal_module, "EventExclusionWindow"))
        for name in (
            "Phase2WindowSource",
            "Phase2EventExclusionSource",
            "Phase2PortfolioSource",
            "is_verified_phase2_window_source",
            "is_verified_phase2_event_exclusion_source",
            "is_verified_phase2_portfolio_source",
            "phase2_sources_share_owner",
        ):
            with self.subTest(name=name):
                self.assertTrue(hasattr(journal_module, name))

    def test_all_inclusive_contract_and_risk_boundaries_are_eligible(self) -> None:
        with issued_phase2_inputs() as inputs:
            contracts = (
                option_contract(
                    expiration=SIGNAL_SESSION + timedelta(days=30),
                    delta=Decimal("0.30"),
                    bid=Decimal("0.38"),
                    ask=Decimal("0.42"),
                    open_interest=1_000,
                    daily_volume=100,
                    strike=Decimal("390"),
                ),
                option_contract(
                    expiration=SIGNAL_SESSION + timedelta(days=60),
                    delta=Decimal("0.40"),
                    bid=Decimal("0.44"),
                    ask=Decimal("0.48"),
                    open_interest=1_000,
                    daily_volume=100,
                    strike=Decimal("410"),
                ),
            )

            reviewed = reviewed_chain(inputs, contracts)
            fee_schedule = reviewed_fee_schedule()
            exclusion = event_source(inputs)
            portfolio = portfolio_source(inputs)
            eligible = eligible_option_contracts(
                reviewed,
                authorization=inputs.authorization,
                fee_schedule=fee_schedule,
                event_exclusion_source=exclusion,
                calendar_resolver=inputs.resolver,
                portfolio_source=portfolio,
            )

            self.assertEqual(
                {contract.occ_symbol for contract in eligible},
                {contract.occ_symbol for contract in contracts},
            )
            selection = select_paper_long_call(
                reviewed,
                authorization=inputs.authorization,
                fee_schedule=fee_schedule,
                event_exclusion_source=exclusion,
                calendar_resolver=inputs.resolver,
                portfolio_source=portfolio,
            )
            self.assertIsNotNone(selection)
            assert selection is not None
            self.assertEqual(selection.quantity, 1)
            self.assertEqual(selection.dte, 60)
            self.assertEqual(selection.reviewed_ask, Decimal("0.48"))
            self.assertEqual(selection.reviewed_initial_risk, Decimal("50"))

    def test_review_mismatch_aborts_the_whole_selection_without_substitution(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            contracts = (
                option_contract(strike=Decimal("400")),
                option_contract(strike=Decimal("401"), delta=Decimal("0.34")),
            )
            reviewed_chain(inputs, contracts)
            mismatched = replace(
                inputs.manual_review_sources[0],
                ask_micros=inputs.manual_review_sources[0].ask_micros + 1,
            )
            _register_phase2(inputs.journal, mismatched)

            with self.assertRaisesRegex(
                OptionPaperError,
                "PROVIDER_MANUAL_MISMATCH",
            ):
                _derive_reviewed_option_chain(
                    inputs.chain_source,
                    (mismatched, inputs.manual_review_sources[1]),
                    inputs.authorization,
                )

    def test_dte_is_derived_and_each_hard_contract_gate_fails_closed(self) -> None:
        with issued_phase2_inputs() as inputs:
            base = option_contract()
            self.assertFalse(hasattr(base.provider_facts, "open_interest"))
            self.assertNotIn("tick_size", OptionContract.__dataclass_fields__)
            self.assertEqual(base.open_interest, 1_500)
            cases = (
                option_contract(expiration=SIGNAL_SESSION + timedelta(days=29)),
                option_contract(expiration=SIGNAL_SESSION + timedelta(days=61)),
                option_contract(delta=Decimal("0.299999")),
                option_contract(delta=Decimal("0.400001")),
                option_contract(open_interest=999),
                option_contract(daily_volume=99),
                option_contract(bid=Decimal("0")),
                option_contract(bid=Decimal("0.37"), ask=Decimal("0.42")),
                option_contract(ask=Decimal("0.480001")),
                option_contract(option_type="PUT"),
            )
            for contract in cases:
                with self.subTest(contract=contract):
                    self.assertEqual(rank_with(inputs, (contract,)), ())

            self.assertFalse(hasattr(base, "dte"))

            # Tick size is not present in either approved source. Exact
            # microdollar quotes remain valid without an invented tick gate.
            ranked = rank_with(
                inputs,
                (
                    option_contract(
                        bid=Decimal("0.381"),
                        ask=Decimal("0.40"),
                    ),
                ),
            )
            self.assertEqual(len(ranked), 1)
            self.assertEqual(ranked[0].bid, Decimal("0.381"))
            self.assertEqual(ranked[0].ask, Decimal("0.40"))

    def test_fee_and_full_ten_session_event_evidence_fail_closed(self) -> None:
        with issued_phase2_inputs() as inputs:
            contract = option_contract()
            copied_fee = copy.copy(reviewed_fee_schedule())
            self.assertEqual(
                rank_with(inputs, (contract,), fee_schedule=copied_fee),
                (),
            )
            incomplete = tuple(
                inputs.resolver.add_sessions(SIGNAL_SESSION, offset)
                for offset in range(9)
            )
            with self.assertRaisesRegex(
                OptionPaperError,
                "EVENT_EXCLUSION_INCOMPLETE",
            ):
                rank_with(
                    inputs,
                    (contract,),
                    event_exclusion_source=event_source(
                        inputs,
                        covered_sessions=incomplete,
                    ),
                )
            blocked_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 9)
            self.assertEqual(
                rank_with(
                    inputs,
                    (contract,),
                    event_exclusion_source=event_source(
                        inputs,
                        event_sessions=(blocked_session,),
                    ),
                ),
                (),
            )

    def test_ranking_uses_every_locked_tie_break_in_order(self) -> None:
        with issued_phase2_inputs() as inputs:
            expiry = SIGNAL_SESSION + timedelta(days=45)
            ladder = (
                option_contract(
                    expiration=expiry,
                    strike=Decimal("406"),
                    delta=Decimal("0.34"),
                    bid=Decimal("0.39"),
                    ask=Decimal("0.40"),
                    open_interest=3_000,
                    daily_volume=300,
                ),
                option_contract(
                    expiration=expiry,
                    strike=Decimal("405"),
                    bid=Decimal("0.38"),
                    ask=Decimal("0.40"),
                    open_interest=3_000,
                    daily_volume=300,
                ),
                option_contract(
                    expiration=expiry,
                    strike=Decimal("404"),
                    bid=Decimal("0.39"),
                    ask=Decimal("0.40"),
                    open_interest=1_000,
                    daily_volume=300,
                ),
                option_contract(
                    expiration=expiry,
                    strike=Decimal("403"),
                    bid=Decimal("0.39"),
                    ask=Decimal("0.40"),
                    open_interest=3_000,
                    daily_volume=100,
                ),
                option_contract(
                    expiration=expiry,
                    strike=Decimal("402"),
                    bid=Decimal("0.39"),
                    ask=Decimal("0.40"),
                    open_interest=3_000,
                    daily_volume=300,
                ),
                option_contract(
                    expiration=expiry,
                    strike=Decimal("401"),
                    bid=Decimal("0.39"),
                    ask=Decimal("0.40"),
                    open_interest=3_000,
                    daily_volume=300,
                ),
                option_contract(
                    expiration=SIGNAL_SESSION + timedelta(days=44),
                    strike=Decimal("399"),
                ),
                option_contract(
                    expiration=SIGNAL_SESSION + timedelta(days=46),
                    strike=Decimal("407"),
                ),
            )

            ranked = rank_with(inputs, ladder)

            self.assertEqual(
                tuple(item.occ_symbol for item in ranked),
                (
                    _occ(expiry, Decimal("401")),
                    _occ(expiry, Decimal("402")),
                    _occ(expiry, Decimal("403")),
                    _occ(expiry, Decimal("404")),
                    _occ(expiry, Decimal("405")),
                    _occ(expiry, Decimal("406")),
                    _occ(
                        SIGNAL_SESSION + timedelta(days=46),
                        Decimal("407"),
                    ),
                    _occ(
                        SIGNAL_SESSION + timedelta(days=44),
                        Decimal("399"),
                    ),
                ),
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
