from __future__ import annotations

import copy
import inspect
import json
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest import mock

import stock_monitor.options_paper as options_module
import stock_monitor.journal as journal_module
import stock_monitor.config as config_module
import stock_monitor.providers.alpaca as alpaca_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.domain import money_to_micros
from stock_monitor.journal import (
    Phase2FeeScheduleSource,
    Phase2OptionOpenSource,
    Phase2OpenPositionSource,
    Phase2SelectionSource,
    Phase2UnderlyingReviewFactSource,
    Phase2UnderlyingReviewPageSource,
    Phase2UnderlyingReviewSource,
)
from stock_monitor.options_paper import (
    OptionExitDecision,
    OptionMark,
    OptionPaperError,
    OptionPromotionDecision,
    OptionPromotionStatus,
    OptionWindow,
    OptionWindowStatus,
    PaperOptionPosition,
    PaperOnlyBoundaryError,
    _diagnostic_option_validation_window,
    _diagnostic_option_window_start,
    assign_option,
    close_option_window,
    evaluate_option_exit,
    evaluate_option_window,
    exercise_option,
    hold_through_expiration,
    is_issued_option_exit_decision,
    is_issued_option_window,
    open_option_window,
    record_option_mark,
    roll_option,
    select_paper_long_call,
    start_next_window,
)
from stock_monitor.providers.alpaca import AlpacaMarketData, TimeWindow
from stock_monitor.providers.http import HttpResponse
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
)
from tests.support import credentials, policy_fixture
from tests.unit.test_option_selection import (
    ET,
    SIGNAL_SESSION,
    _action_source,
    _register_action,
    _register_phase1,
    _register_phase2,
    _register_portfolio,
    _register_signal,
    _source_digest,
    event_source,
    issued_phase2_inputs,
    option_contract,
    portfolio_source,
    reviewed_chain,
    reviewed_fee_schedule,
)


def _fee_source(inputs, schedule) -> Phase2FeeScheduleSource:
    source = Phase2FeeScheduleSource(
        row_id=500,
        schedule_id=schedule.schedule_id,
        effective_session=schedule.effective_session,
        reviewed_at=schedule.reviewed_at,
        currency=schedule.currency,
        contract_multiplier=schedule.contract_multiplier,
        entry_fee_per_contract_micros=(
            schedule.entry_fee_per_contract_micros
        ),
        exit_fee_per_contract_micros=schedule.exit_fee_per_contract_micros,
        close_fee_reserve_per_contract_micros=(
            schedule.close_fee_reserve_per_contract_micros
        ),
        source_sha256=schedule.source_sha256,
        schedule_digest=schedule.digest,
        reviewed_bytes=config_module._read_reviewed_fee_schedule_bytes(
            schedule
        ),
        archived_at=schedule.reviewed_at + timedelta(seconds=1),
        row_references=(),
        source_digest=_source_digest("fee-source"),
    )
    _register_phase2(inputs.journal, source)
    return source


def _durable_selection(inputs, *, contract=None):
    schedule = reviewed_fee_schedule()
    exclusion = event_source(inputs)
    portfolio = portfolio_source(inputs)
    reviewed = reviewed_chain(
        inputs,
        (option_contract() if contract is None else contract,),
    )
    initial = select_paper_long_call(
        reviewed,
        authorization=inputs.authorization,
        fee_schedule=schedule,
        event_exclusion_source=exclusion,
        calendar_resolver=inputs.resolver,
        portfolio_source=portfolio,
    )
    assert initial is not None
    fee_source = _fee_source(inputs, schedule)
    selected_review = next(
        review
        for review in inputs.manual_review_sources
        if review.occ_symbol == initial.contract.occ_symbol
    )
    source = Phase2SelectionSource(
        row_id=501,
        selection_id="selection-1",
        authorization_source=inputs.authorization_source,
        option_chain_source=inputs.chain_source,
        provider_fact_source=selected_review.provider_fact_source,
        manual_review_sources=inputs.manual_review_sources,
        selected_manual_review_source=selected_review,
        fee_schedule_source=fee_source,
        event_exclusion_source=exclusion,
        portfolio_source=portfolio,
        selection_session=SIGNAL_SESSION,
        quantity=1,
        selected_at=initial.selected_at,
        received_at=initial.selected_at + timedelta(seconds=1),
        ranking_digest=initial.source_digest,
        row_references=(),
        source_digest=_source_digest("selection-source"),
    )
    _register_phase2(inputs.journal, source)
    durable = options_module._reissue_option_selection(
        source,
        calendar_resolver=inputs.resolver,
        portfolio_source=portfolio,
    )
    return durable, portfolio, fee_source


@contextmanager
def opened_option_window(
    *,
    contract=None,
    open_ask=Decimal("0.40"),
    open_minute: int = 10,
):
    with issued_phase2_inputs() as inputs:
        selection, _selection_portfolio, fee_source = _durable_selection(
            inputs,
            contract=contract,
        )
        opened_at = datetime(
            SIGNAL_SESSION.year,
            SIGNAL_SESSION.month,
            SIGNAL_SESSION.day,
            10,
            open_minute,
            tzinfo=ET,
        )
        portfolio = portfolio_source(
            inputs,
            query_cutoff=opened_at - timedelta(minutes=1),
        )
        action = _action_source(
            row_id=502,
            domain_kind="OPTION_PAPER_OPEN",
            event_time=opened_at,
            ask_micros=money_to_micros(open_ask),
            occ_symbol=selection.contract.occ_symbol,
        )
        _register_action(inputs.journal, action)
        entry_ask_micros = money_to_micros(open_ask)
        entry_fee_micros = money_to_micros(selection.entry_fee)
        reserve_fee_micros = money_to_micros(selection.reserved_exit_fee)
        open_source = Phase2OptionOpenSource(
            row_id=502,
            entry_id="entry-1",
            window_id=inputs.phase2_window.window_id,
            selection_source=selection.selection_source,
            open_action=action,
            quantity=1,
            entry_ask_micros=entry_ask_micros,
            entry_fee_micros=entry_fee_micros,
            reserve_fee_micros=reserve_fee_micros,
            all_in_initial_risk_micros=(
                entry_ask_micros * 100
                + entry_fee_micros
                + reserve_fee_micros
            ),
            fee_schedule_source=fee_source,
            portfolio_source_digest=portfolio.source_digest,
            entered_at=opened_at,
            received_at=action.received_at,
            row_references=(),
            source_digest=_source_digest("open-source"),
        )
        _register_phase2(inputs.journal, open_source)
        window = open_option_window(
            selection,
            portfolio,
            open_source,
        )
        yield inputs, selection, window


def underlying_bar(
    session_date: date,
    *,
    low: Decimal = Decimal("495"),
    high: Decimal = Decimal("510"),
):
    observed_at = datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        15,
        54,
        tzinfo=ET,
    )
    timestamp = observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    body = json.dumps(
        {
            "bars": {
                "SPY": [
                    {
                        "t": timestamp,
                        "o": "500",
                        "h": str(high),
                        "l": str(low),
                        "c": "500",
                        "v": 1000,
                    }
                ]
            },
            "next_page_token": None,
        },
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

    return AlpacaMarketData(
        Transport(),
        credentials(),
        now=lambda: observed_at.astimezone(UTC) + timedelta(days=1),
    ).daily_bars(
        ("SPY",),
        TimeWindow(
            datetime(
                session_date.year,
                session_date.month,
                session_date.day,
                9,
                30,
                tzinfo=ET,
            ).astimezone(UTC),
            observed_at.astimezone(UTC),
        ),
    )["SPY"][0]


def underlying_source(
    inputs,
    window: PaperOptionPosition,
    session_date: date,
    *,
    low: Decimal = Decimal("495"),
    high: Decimal = Decimal("510"),
    points: tuple[tuple[int, int, Decimal, Decimal], ...] | None = None,
) -> Phase2UnderlyingReviewSource:
    schedule = inputs.resolver.session(session_date)
    values = points or ((15, 30, low, high),)
    raw_bars = []
    for hour, minute, bar_low, bar_high in values:
        bar_at = datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            hour,
            minute,
            tzinfo=schedule.timezone,
        )
        raw_bars.append(
            {
                "t": bar_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                "o": "500",
                "h": str(bar_high),
                "l": str(bar_low),
                "c": "500",
                "v": 1000,
            }
        )
    request_end = datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        values[-1][0],
        values[-1][1],
        tzinfo=schedule.timezone,
    )
    opened_session = window.opened_at.astimezone(schedule.timezone).date()
    request_start = (
        window.opened_at
        if session_date == opened_session
        else datetime.combine(
            session_date,
            schedule.open_time,
            tzinfo=schedule.timezone,
        )
    )
    body = json.dumps(
        {"bars": {"SPY": raw_bars}, "next_page_token": None},
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

    cohort = AlpacaMarketData(
        Transport(),
        credentials(),
        now=lambda: request_end.astimezone(UTC) + timedelta(minutes=16),
    ).historical_minute_bars(
        ("SPY",),
        TimeWindow(request_start.astimezone(UTC), request_end.astimezone(UTC)),
    )
    bundle = alpaca_module.read_provider_fetch_bundle(cohort)
    page_sources = []
    for bundle_page in bundle.pages:
        page = bundle_page.page
        observation = bundle_page.observation
        page_source = Phase2UnderlyingReviewPageSource(
            row_id=600 + page.page_ordinal,
            page_id=_source_digest(f"underlying-page:{page.page_ordinal}"),
            review_set_id=f"review:{session_date}",
            page_ordinal=page.page_ordinal,
            source_observation_row_id=6000 + page.page_ordinal,
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
                f"underlying-page-source:{page.page_ordinal}"
            ),
        )
        _register_phase2(inputs.journal, page_source)
        page_sources.append(page_source)
    fact_sources = []
    for ordinal, bar in enumerate(cohort["SPY"], start=1):
        provider_source = alpaca_module._normalized_market_fact_source(bar)
        fact = Phase2UnderlyingReviewFactSource(
            row_id=700 + ordinal,
            fact_id=_source_digest(f"underlying-fact:{ordinal}"),
            review_set_id=f"review:{session_date}",
            source_observation_row_id=6000 + provider_source.page_ordinal,
            external_source_observation_id=(
                provider_source.source_observation_id
            ),
            fetch_page_ordinal=provider_source.page_ordinal,
            source_item_ordinal=provider_source.source_item_ordinal,
            source_item_path=provider_source.source_item_path,
            payload_sha256=provider_source.page_payload_sha256,
            symbol=bar.symbol,
            bar_at=bar.timestamp,
            open_micros=money_to_micros(bar.open),
            high_micros=money_to_micros(bar.high),
            low_micros=money_to_micros(bar.low),
            close_micros=money_to_micros(bar.close),
            volume=bar.volume,
            fact_digest=provider_source.normalized_fields_digest,
            bar=bar,
            row_references=(),
            source_digest=_source_digest(f"underlying-fact-source:{ordinal}"),
        )
        _register_phase2(inputs.journal, fact)
        fact_sources.append(fact)
    query_cutoff = max(page.retrieved_at for page in page_sources)
    source = Phase2UnderlyingReviewSource(
        row_id=800,
        review_set_id=f"review:{session_date}",
        window_id=window.window_id,
        entry_source=window.open_source,
        underlying="SPY",
        review_session=session_date,
        collection_name="bars",
        timeframe="1Min",
        adjustment="split",
        feed="sip",
        requested_symbols=("SPY",),
        request_digest=bundle.manifest.request_digest,
        manifest_digest=bundle.manifest.manifest_digest,
        pages=tuple(page_sources),
        facts=tuple(fact_sources),
        provider_cohort=cohort,
        expected_page_count=len(page_sources),
        expected_fact_count=len(fact_sources),
        terminal=True,
        request_start=request_start,
        request_end=request_end,
        query_cutoff=query_cutoff,
        received_at=query_cutoff,
        row_references=(),
        source_digest=_source_digest(f"underlying-source:{session_date}"),
    )
    _register_phase2(inputs.journal, source)
    return source


def option_mark(
    inputs,
    *,
    mark_id: str,
    session_date: date,
    hour: int | None = None,
    minute: int | None = None,
    bid: Decimal | None = Decimal("0.50"),
    ask: Decimal | None = Decimal("0.52"),
    occ_symbol: str | None = None,
) -> OptionMark:
    session = inputs.resolver.session(session_date)
    observed_at = datetime.combine(
        session_date,
        session.review_time,
        tzinfo=session.timezone,
    )
    if hour is not None:
        observed_at = observed_at.replace(hour=hour, minute=minute or 0)
    return OptionMark(
        mark_id=mark_id,
        occ_symbol=occ_symbol or option_contract().occ_symbol,
        session_date=session_date,
        observed_at=observed_at,
        bid=bid,
        ask=ask,
        manual_source_id=f"robinhood-mark:{mark_id}",
    )


class OptionAccountingTests(unittest.TestCase):
    def test_open_derives_cash_time_and_quote_from_exact_sources(self) -> None:
        self.assertEqual(
            tuple(inspect.signature(open_option_window).parameters),
            ("selection", "portfolio_source", "open_source"),
        )

    def test_selection_reissues_after_authorization_registry_restart(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            selection, portfolio, _fee_source_value = _durable_selection(inputs)
            source = selection.selection_source
            with options_module._PHASE2_AUTHORIZATIONS_LOCK:
                saved_authorizations = dict(
                    options_module._PHASE2_AUTHORIZATIONS
                )
                saved_signals = dict(options_module._AUTHORIZED_PHASE2_SIGNALS)
                options_module._PHASE2_AUTHORIZATIONS.clear()
                options_module._AUTHORIZED_PHASE2_SIGNALS.clear()
            try:
                with mock.patch.object(
                    options_module,
                    "_authorization_for_source",
                    wraps=options_module._authorization_for_source,
                ) as authorization_reissuer:
                    with mock.patch(
                        "stock_monitor.validation."
                        "_issue_phase1_promotion_from_journal_source",
                        return_value=inputs.decision,
                    ) as promotion_reissuer:
                        restarted = options_module._reissue_option_selection(
                            source,
                            calendar_resolver=inputs.resolver,
                            portfolio_source=portfolio,
                        )

                self.assertIs(restarted.selection_source, source)
                self.assertEqual(restarted.source_digest, source.ranking_digest)
                self.assertIsNot(restarted.authorization, inputs.authorization)
                authorization_reissuer.assert_called()
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

    def test_locked_public_window_signatures_remain_exact(self) -> None:
        signatures = {
            record_option_mark: ("window", "mark"),
            evaluate_option_window: ("window",),
            start_next_window: ("window", "start_event"),
        }
        for function, expected in signatures.items():
            with self.subTest(function=function.__name__):
                self.assertEqual(
                    tuple(inspect.signature(function).parameters),
                    expected,
                )

    def test_open_uses_ask_and_charges_entry_fee_without_spending_reserve(
        self,
    ) -> None:
        with opened_option_window() as (_, selection, window):
            self.assertIsNot(
                window.portfolio_source,
                selection.portfolio_source,
            )
            self.assertGreater(
                window.portfolio_source.query_cutoff,
                selection.selected_at,
            )
            self.assertEqual(selection.reviewed_ask, Decimal("0.40"))
            self.assertEqual(selection.entry_fee, Decimal("1"))
            self.assertEqual(selection.reserved_exit_fee, Decimal("1"))
            self.assertEqual(selection.reviewed_initial_risk, Decimal("42"))
            self.assertEqual(window.paper_cash, Decimal("4959"))
            self.assertEqual(window.entry_ask, Decimal("0.40"))
            self.assertEqual(window.initial_risk, Decimal("42"))
            self.assertEqual(window.high_water, Decimal("5000"))
            self.assertEqual(window.maximum_drawdown, Decimal("0"))
            self.assertEqual(window.status, OptionWindowStatus.ACTIVE)

            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_OPEN_SOURCE_UNVERIFIED",
            ):
                open_option_window(
                    selection,
                    window.portfolio_source,
                    copy.copy(window.open_source),
                )

    def test_open_requires_fresh_exact_flat_global_portfolio(self) -> None:
        with opened_option_window() as (inputs, selection, window):
            stale_source = replace(
                window.open_source,
                portfolio_source_digest=selection.portfolio_source.source_digest,
                source_digest=_source_digest("stale-open-source"),
            )
            _register_phase2(inputs.journal, stale_source)
            with self.assertRaisesRegex(
                OptionPaperError,
                "PHASE2_PORTFOLIO_STALE_FOR_OPEN",
            ):
                open_option_window(
                    selection,
                    selection.portfolio_source,
                    stale_source,
                )

            with self.assertRaisesRegex(
                OptionPaperError,
                "PHASE2_PORTFOLIO_UNVERIFIED",
            ):
                open_option_window(
                    selection,
                    copy.copy(window.portfolio_source),
                    window.open_source,
                )

            existing = Phase2OpenPositionSource(
                entry_id="already-open",
                selection_id=selection.selection_source.selection_id,
                authorization_id=(
                    selection.selection_source.authorization_source.authorization_id
                ),
                occ_symbol=selection.contract.occ_symbol,
                quantity=1,
                entry_ask_micros=money_to_micros(window.entry_ask),
                entry_fee_micros=money_to_micros(window.entry_fee),
                reserve_fee_micros=money_to_micros(window.reserved_exit_fee),
                all_in_initial_risk_micros=money_to_micros(window.initial_risk),
                entered_at=window.opened_at - timedelta(minutes=1),
                entry_source=window.open_source,
                row_references=(),
                source_digest=_source_digest("already-open-position"),
            )
            _register_phase2(inputs.journal, existing)
            busy_portfolio = replace(
                window.portfolio_source,
                open_position_source=existing,
                expected_entry_count=1,
                source_digest=_source_digest("busy-open-portfolio"),
                authority_digest=_source_digest("busy-open-portfolio-authority"),
            )
            _register_portfolio(
                inputs.journal,
                busy_portfolio,
                window_source=inputs.phase2_window,
            )
            busy_source = replace(
                window.open_source,
                portfolio_source_digest=busy_portfolio.source_digest,
                source_digest=_source_digest("busy-open-source"),
            )
            _register_phase2(inputs.journal, busy_source)
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_POSITION_ALREADY_OPEN",
            ):
                open_option_window(selection, busy_portfolio, busy_source)

    def test_open_rejects_unsettled_sale_before_settled_cash_check(self) -> None:
        with opened_option_window() as (inputs, selection, window):
            for settled_cash_micros in (5_000_000_000, 0):
                with self.subTest(settled_cash_micros=settled_cash_micros):
                    unsettled_portfolio = replace(
                        window.portfolio_source,
                        settled_cash_micros=settled_cash_micros,
                        economic_cash_micros=settled_cash_micros,
                        equity_micros=settled_cash_micros,
                        unsettled_proceeds_micros=1,
                        source_digest=_source_digest(
                            f"unsettled-open-portfolio:{settled_cash_micros}"
                        ),
                        authority_digest=_source_digest(
                            "unsettled-open-portfolio-authority:"
                            f"{settled_cash_micros}"
                        ),
                    )
                    _register_portfolio(
                        inputs.journal,
                        unsettled_portfolio,
                        window_source=inputs.phase2_window,
                    )
                    unsettled_source = replace(
                        window.open_source,
                        portfolio_source_digest=(
                            unsettled_portfolio.source_digest
                        ),
                        source_digest=_source_digest(
                            f"unsettled-open-source:{settled_cash_micros}"
                        ),
                    )
                    _register_phase2(inputs.journal, unsettled_source)

                    with self.assertRaisesRegex(
                        OptionPaperError,
                        "OPTION_PRIOR_SALE_UNSETTLED",
                    ):
                        open_option_window(
                            selection,
                            unsettled_portfolio,
                            unsettled_source,
                        )

    def test_open_rechecks_actual_ask_risk_and_review_selection_chronology(
        self,
    ) -> None:
        with opened_option_window(open_ask=Decimal("0.48")) as (
            _,
            _,
            boundary,
        ):
            self.assertEqual(boundary.initial_risk, Decimal("50"))
            self.assertEqual(boundary.paper_cash, Decimal("4951"))

        with self.assertRaisesRegex(
            OptionPaperError,
            "OPTION_INITIAL_RISK_ABOVE_50",
        ):
            with opened_option_window(open_ask=Decimal("0.49")):
                pass

        with self.assertRaisesRegex(
            OptionPaperError,
            "PHASE2_PORTFOLIO_STALE_FOR_OPEN",
        ):
            with opened_option_window(open_minute=4):
                pass

    def test_open_rejects_forged_bid_or_another_contract_occ(self) -> None:
        with opened_option_window() as (inputs, selection, window):
            forged_bid = replace(
                window.open_source.open_action,
                bid_micros=money_to_micros(Decimal("0.39")),
            )
            _register_action(inputs.journal, forged_bid)
            bid_source = replace(window.open_source, open_action=forged_bid)
            _register_phase2(inputs.journal, bid_source)
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_OPEN_QUOTE_MISMATCH",
            ):
                open_option_window(selection, window.portfolio_source, bid_source)

            wrong_occ_action = _action_source(
                row_id=503,
                domain_kind="OPTION_PAPER_OPEN",
                event_time=window.opened_at,
                ask_micros=money_to_micros(window.entry_ask),
                occ_symbol=option_contract(strike=Decimal("401")).occ_symbol,
            )
            _register_action(inputs.journal, wrong_occ_action)
            wrong_occ_source = replace(
                window.open_source,
                open_action=wrong_occ_action,
                received_at=wrong_occ_action.received_at,
            )
            _register_phase2(inputs.journal, wrong_occ_source)
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_OPEN_QUOTE_MISMATCH",
            ):
                open_option_window(
                    selection,
                    window.portfolio_source,
                    wrong_occ_source,
                )

    def test_journal_reparsed_ask_only_open_action_authorizes_exact_occ(
        self,
    ) -> None:
        with issued_phase2_inputs() as inputs:
            contract = option_contract()
            event_time = datetime(2026, 8, 18, 10, 10, tzinfo=ET)
            result = ingest_confirmation(
                inputs.journal,
                ConfirmationEnvelope(
                    message_id="message:actual-option-open",
                    message_time=event_time,
                    received_at=event_time + timedelta(seconds=1),
                    text=(
                        f"OPTION PAPER OPEN {contract.occ_symbol} "
                        "ASK 0.40 AT 10:10 ET"
                    ),
                    session_date=SIGNAL_SESSION,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=inputs.resolver,
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )
            with inputs.journal.transaction() as transaction:
                action = transaction.read_action_source(
                    execution_event_id=result.actions[0].event_row_id
                )
            assert action is not None
            self.assertIsNone(action.symbol)
            self.assertIsNone(action.bid_micros)
            self.assertEqual(action.ask_micros, money_to_micros(Decimal("0.40")))
            self.assertEqual(
                options_module._action_occ_symbol(action),
                contract.occ_symbol,
            )

            # The write invalidated earlier snapshot capabilities. Reissue the
            # same exact test fixtures at the new Journal generation before
            # deriving the durable selection/open source.
            _register_signal(inputs.journal, inputs.historical_signal)
            _register_signal(inputs.journal, inputs.signal)
            _register_phase1(
                inputs.journal,
                journal_module._PHASE1_VALIDATION_WINDOW_SOURCE_AUTHORITIES,
                inputs.window,
            )
            _register_action(inputs.journal, inputs.phase2_window.start_action)
            _register_phase2(inputs.journal, inputs.phase2_window)
            _register_phase2(inputs.journal, inputs.authorization_source)
            selection, _selection_portfolio, fee_source = _durable_selection(
                inputs,
                contract=contract,
            )
            portfolio = portfolio_source(
                inputs,
                query_cutoff=event_time - timedelta(minutes=1),
            )
            open_source = Phase2OptionOpenSource(
                row_id=action.execution_event_id,
                entry_id="entry:actual-open",
                window_id=inputs.phase2_window.window_id,
                selection_source=selection.selection_source,
                open_action=action,
                quantity=1,
                entry_ask_micros=action.ask_micros,
                entry_fee_micros=money_to_micros(selection.entry_fee),
                reserve_fee_micros=money_to_micros(
                    selection.reserved_exit_fee
                ),
                all_in_initial_risk_micros=money_to_micros(Decimal("42")),
                fee_schedule_source=fee_source,
                portfolio_source_digest=portfolio.source_digest,
                entered_at=action.event_time,
                received_at=action.received_at,
                row_references=(),
                source_digest=_source_digest("actual-open-source"),
            )
            _register_phase2(inputs.journal, open_source)

            opened = open_option_window(selection, portfolio, open_source)

            self.assertEqual(opened.entry_ask, Decimal("0.40"))
            self.assertEqual(opened.paper_cash, Decimal("4959"))

    def test_mark_window_boundaries_are_inclusive_and_marks_use_exit_reserve(
        self,
    ) -> None:
        for boundary in ("review", "close-minus-five"):
            with self.subTest(boundary=boundary), opened_option_window() as (
                inputs,
                _,
                window,
            ):
                session_date = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
                schedule = inputs.resolver.session(session_date)
                observed_at = datetime.combine(
                    session_date,
                    (
                        schedule.review_time
                        if boundary == "review"
                        else (
                            datetime.combine(
                                session_date,
                                schedule.close_time,
                            )
                            - timedelta(minutes=5)
                        ).time()
                    ),
                    tzinfo=schedule.timezone,
                )
                mark = replace(
                    option_mark(
                        inputs,
                        mark_id=f"mark-{boundary}",
                        session_date=session_date,
                    ),
                    observed_at=observed_at,
                )

                updated = record_option_mark(
                    window,
                    mark,
                )

                point = updated.equity_points[-1]
                self.assertTrue(point.valid)
                self.assertEqual(point.liquidation_value, Decimal("49"))
                self.assertEqual(point.equity, Decimal("5008"))
                self.assertEqual(updated.high_water, Decimal("5008"))
                self.assertEqual(updated.maximum_drawdown, Decimal("0"))

    def test_missing_nonpositive_crossed_outside_or_wrong_contract_is_zero(
        self,
    ) -> None:
        cases = (
            {"bid": None, "ask": None},
            {"bid": Decimal("0"), "ask": Decimal("0.01")},
            {"bid": Decimal("0.53"), "ask": Decimal("0.52")},
            {"bid": Decimal("0.5000001"), "ask": Decimal("0.52")},
            {"hour": 15, "minute": 29},
            {"hour": 15, "minute": 56},
            {"occ_symbol": "SPY261016C00401000"},
        )
        for index, changes in enumerate(cases):
            with self.subTest(changes=changes), opened_option_window() as (
                inputs,
                _,
                window,
            ):
                session_date = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
                invalid = option_mark(
                    inputs,
                    mark_id=f"invalid-{index}",
                    session_date=session_date,
                    **changes,
                )

                failed = record_option_mark(
                    window,
                    invalid,
                )

                point = failed.equity_points[-1]
                self.assertFalse(point.valid)
                self.assertEqual(point.liquidation_value, Decimal("0"))
                self.assertEqual(point.equity, Decimal("4959"))
                self.assertEqual(failed.maximum_drawdown, Decimal("41"))
                self.assertEqual(
                    failed.status,
                    OptionWindowStatus.RESTART_REQUIRED,
                )

                later_session = inputs.resolver.add_sessions(session_date, 1)
                later = record_option_mark(
                    failed,
                    option_mark(
                        inputs,
                        mark_id=f"later-{index}",
                        session_date=later_session,
                    ),
                )
                self.assertEqual(
                    later.status,
                    OptionWindowStatus.RESTART_REQUIRED,
                )

    def test_marks_are_strictly_ordered_unique_and_state_is_identity_bound(
        self,
    ) -> None:
        with opened_option_window() as (inputs, _, window):
            first_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
            first_mark = option_mark(
                inputs,
                mark_id="mark-1",
                session_date=first_session,
            )
            first = record_option_mark(
                window,
                first_mark,
            )
            second_session = inputs.resolver.add_sessions(first_session, 1)
            cases = (
                (
                    option_mark(
                        inputs,
                        mark_id="mark-1",
                        session_date=second_session,
                    ),
                    "DUPLICATE_OPTION_MARK",
                ),
                (
                    option_mark(
                        inputs,
                        mark_id="mark-same-session",
                        session_date=first_session,
                    ),
                    "DUPLICATE_OPTION_MARK_SESSION",
                ),
                (
                    option_mark(
                        inputs,
                        mark_id="mark-earlier",
                        session_date=SIGNAL_SESSION,
                    ),
                    "OPTION_MARK_OUT_OF_ORDER",
                ),
            )
            for mark, reason in cases:
                with self.subTest(reason=reason), self.assertRaisesRegex(
                    OptionPaperError,
                    reason,
                ):
                    record_option_mark(
                        first,
                        mark,
                    )

            for forged in (
                copy.copy(first),
                replace(first, paper_cash=Decimal("5000")),
            ):
                with self.subTest(forged=forged), self.assertRaisesRegex(
                    OptionPaperError,
                    "OPTION_WINDOW_UNVERIFIED",
                ):
                    record_option_mark(
                        forged,
                        option_mark(
                            inputs,
                            mark_id="mark-next",
                            session_date=second_session,
                        ),
                    )

    def test_high_water_and_drawdown_follow_ordered_marks_and_close(self) -> None:
        with opened_option_window() as (inputs, _, window):
            first_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
            high = record_option_mark(
                window,
                option_mark(
                    inputs,
                    mark_id="mark-high",
                    session_date=first_session,
                    bid=Decimal("0.70"),
                    ask=Decimal("0.72"),
                ),
            )
            self.assertEqual(high.high_water, Decimal("5028"))
            self.assertEqual(high.maximum_drawdown, Decimal("0"))
            close_session = inputs.resolver.add_sessions(first_session, 1)
            closed = close_option_window(
                high,
                option_mark(
                    inputs,
                    mark_id="close-low",
                    session_date=close_session,
                    hour=14,
                    bid=Decimal("0.30"),
                    ask=Decimal("0.60"),
                ),
                actual_exit_fee=Decimal("0.50"),
            )

            self.assertEqual(closed.status, OptionWindowStatus.CLOSED)
            self.assertEqual(closed.paper_cash, Decimal("4988.50"))
            self.assertEqual(closed.high_water, Decimal("5028"))
            self.assertEqual(closed.maximum_drawdown, Decimal("39.50"))
            assert closed.closed_trade is not None
            self.assertEqual(closed.closed_trade.exit_bid, Decimal("0.30"))
            self.assertEqual(closed.closed_trade.exit_ask, Decimal("0.60"))
            self.assertEqual(closed.closed_trade.actual_exit_fee, Decimal("0.50"))
            self.assertEqual(closed.closed_trade.net_pnl, Decimal("-11.50"))
            self.assertEqual(
                closed.closed_trade.net_r,
                Decimal("-11.50") / Decimal("42"),
            )

    def test_actual_exit_fee_replaces_reserve_and_restart_is_permanent(
        self,
    ) -> None:
        with opened_option_window() as (inputs, _, window):
            mark_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
            failed = record_option_mark(
                window,
                option_mark(
                    inputs,
                    mark_id="missing",
                    session_date=mark_session,
                    bid=None,
                    ask=None,
                ),
            )
            close_session = inputs.resolver.add_sessions(mark_session, 1)
            closed = close_option_window(
                failed,
                option_mark(
                    inputs,
                    mark_id="close",
                    session_date=close_session,
                    hour=14,
                    bid=Decimal("0.55"),
                    ask=Decimal("0.60"),
                ),
                actual_exit_fee=Decimal("0.75"),
            )

            self.assertEqual(
                closed.status,
                OptionWindowStatus.RESTART_REQUIRED,
            )
            self.assertEqual(closed.paper_cash, Decimal("5013.25"))
            assert closed.closed_trade is not None
            self.assertEqual(closed.closed_trade.net_pnl, Decimal("13.25"))
            self.assertEqual(
                closed.closed_trade.net_r,
                Decimal("13.25") / Decimal("42"),
            )

            with self.assertRaisesRegex(
                OptionPaperError,
                "INVALID_ACTUAL_EXIT_FEE",
            ):
                close_option_window(
                    window,
                    option_mark(
                        inputs,
                        mark_id="too-precise-fee",
                        session_date=mark_session,
                        hour=14,
                    ),
                    actual_exit_fee=Decimal("0.0000001"),
                )

    def test_close_rejects_zero_actual_exit_fee(self) -> None:
        with opened_option_window() as (inputs, _, window):
            close_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)

            with self.assertRaisesRegex(
                OptionPaperError,
                "INVALID_ACTUAL_EXIT_FEE",
            ):
                close_option_window(
                    window,
                    option_mark(
                        inputs,
                        mark_id="zero-actual-fee",
                        session_date=close_session,
                        hour=14,
                    ),
                    actual_exit_fee=Decimal("0"),
                )

    def test_paper_only_boundary_rejects_every_live_or_lifecycle_verb(
        self,
    ) -> None:
        with opened_option_window() as (_, _, window):
            for verb in (
                exercise_option,
                roll_option,
                assign_option,
                hold_through_expiration,
            ):
                with self.subTest(verb=verb.__name__), self.assertRaises(
                    PaperOnlyBoundaryError,
                ):
                    verb(window)
            self.assertFalse(hasattr(options_module, "submit_live_option_order"))


class OptionExitDecisionTests(unittest.TestCase):
    def test_exit_uses_a_complete_journal_underlying_review_source(self) -> None:
        self.assertEqual(
            tuple(inspect.signature(evaluate_option_exit).parameters),
            ("window", "underlying_source"),
        )
        self.assertTrue(
            hasattr(journal_module, "Phase2UnderlyingReviewSource")
        )
        self.assertTrue(
            hasattr(
                journal_module,
                "is_verified_phase2_underlying_review_source",
            )
        )
        self.assertIn("underlying_source", OptionExitDecision.__dataclass_fields__)
        self.assertNotIn("underlying_fact", OptionExitDecision.__dataclass_fields__)

    def test_options_domain_exposes_no_raw_provider_authority_registrar(self) -> None:
        self.assertFalse(
            hasattr(options_module, "_register_option_underlying_fact")
        )
        self.assertFalse(
            hasattr(options_module, "_register_option_provider_chain")
        )
        for name in (
            "_register_selection",
            "_register_window",
            "_register_validation_window",
            "_issue_diagnostic_option_validation_window",
            "_issue_option_window_start",
            "is_issued_option_validation_window",
        ):
            with self.subTest(name=name):
                self.assertFalse(hasattr(options_module, name))

        forged = PaperOptionPosition(
            window_id="forged-window",
            selection=object(),  # type: ignore[arg-type]
            portfolio_source=object(),
            open_source=object(),
            starting_equity=Decimal("5000"),
            paper_cash=Decimal("999999"),
            opened_at=datetime(2026, 8, 18, 10, tzinfo=ET),
            entry_ask=Decimal("0.01"),
            entry_fee=Decimal("0"),
            reserved_exit_fee=Decimal("0"),
            initial_risk=Decimal("1"),
            status=OptionWindowStatus.CLOSED,
            equity_points=(),
            high_water=Decimal("999999"),
            maximum_drawdown=Decimal("0"),
            closed_trade=None,
            source_digest="f" * 64,
            _calendar_resolver=object(),
        )
        self.assertFalse(is_issued_option_window(forged))

    def test_stop_precedes_target_and_same_bar_ambiguity_is_explicit(self) -> None:
        with opened_option_window() as (inputs, _, window):
            stop = evaluate_option_exit(
                window,
                underlying_source(
                    inputs,
                    window,
                    SIGNAL_SESSION,
                    low=Decimal("489"),
                ),
            )
            target = evaluate_option_exit(
                window,
                underlying_source(
                    inputs,
                    window,
                    SIGNAL_SESSION,
                    high=Decimal("521"),
                ),
            )
            ambiguous = evaluate_option_exit(
                window,
                underlying_source(
                    inputs,
                    window,
                    SIGNAL_SESSION,
                    low=Decimal("489"),
                    high=Decimal("521"),
                ),
            )

            self.assertEqual(stop.required_close_reason, "STOP")
            self.assertEqual(target.required_close_reason, "TARGET")
            self.assertEqual(ambiguous.required_close_reason, "STOP")
            self.assertIn(
                "SAME_BAR_STOP_TARGET_STOP_FIRST",
                ambiguous.reason_codes,
            )
            self.assertTrue(ambiguous.diagnostic_only)

    def test_exit_source_rejects_wrong_market_semantics_and_omitted_prefix(
        self,
    ) -> None:
        with opened_option_window() as (inputs, _, window):
            review_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
            source = underlying_source(
                inputs,
                window,
                review_session,
                points=(
                    (10, 0, Decimal("489"), Decimal("505")),
                    (15, 30, Decimal("495"), Decimal("510")),
                ),
            )
            decision = evaluate_option_exit(window, source)
            self.assertEqual(decision.required_close_reason, "STOP")
            self.assertEqual(
                decision.evaluated_at.astimezone(ET).time(),
                datetime(2026, 8, 19, 10, 0, tzinfo=ET).time(),
            )

            for changes in (
                {"timeframe": "1Day"},
                {"adjustment": "raw"},
                {"feed": "iex"},
                {"collection_name": "quotes"},
            ):
                invalid = replace(source, **changes)
                _register_phase2(inputs.journal, invalid)
                with self.subTest(changes=changes), self.assertRaisesRegex(
                    OptionPaperError,
                    "OPTION_EXIT_SOURCE_INCOMPLETE",
                ):
                    evaluate_option_exit(window, invalid)

            omitted_prefix = replace(
                source,
                facts=(source.facts[-1],),
                expected_fact_count=1,
            )
            _register_phase2(inputs.journal, omitted_prefix)
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_EXIT_SOURCE_INCOMPLETE",
            ):
                evaluate_option_exit(window, omitted_prefix)

            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_EXIT_SOURCE_UNVERIFIED",
            ):
                evaluate_option_exit(window, underlying_bar(review_session))

    def test_ten_sessions_precede_twenty_one_dte_and_hold_is_explicit(self) -> None:
        with opened_option_window() as (inputs, _, window):
            ordinary_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 1)
            hold = evaluate_option_exit(
                window,
                underlying_source(inputs, window, ordinary_session),
            )
            deadline_session = inputs.resolver.add_sessions(SIGNAL_SESSION, 9)
            deadline = evaluate_option_exit(
                window,
                underlying_source(inputs, window, deadline_session),
            )

            self.assertIsNone(hold.required_close_reason)
            self.assertEqual(deadline.holding_sessions, 10)
            self.assertEqual(
                deadline.required_close_reason,
                "MAX_HOLD_10_SESSIONS",
            )

        dte_contract = option_contract(
            expiration=SIGNAL_SESSION + timedelta(days=30),
        )
        with opened_option_window(contract=dte_contract) as (
            inputs,
            _,
            window,
        ):
            review_session = SIGNAL_SESSION + timedelta(days=9)
            while not inputs.resolver.is_open(review_session):
                review_session += timedelta(days=1)
            decision = evaluate_option_exit(
                window,
                underlying_source(inputs, window, review_session),
            )
            self.assertLess(decision.holding_sessions, 10)
            self.assertLessEqual(decision.dte, 21)
            self.assertEqual(decision.required_close_reason, "DTE_21")

    def test_exit_decision_rejects_fact_or_decision_copy_and_never_expires(
        self,
    ) -> None:
        with opened_option_window() as (inputs, _, window):
            source = underlying_source(inputs, window, SIGNAL_SESSION)
            with self.assertRaisesRegex(
                OptionPaperError,
                "OPTION_EXIT_SOURCE_UNVERIFIED",
            ):
                evaluate_option_exit(window, copy.copy(source))

            decision = evaluate_option_exit(window, source)
            self.assertIsInstance(decision, OptionExitDecision)
            self.assertTrue(is_issued_option_exit_decision(decision))
            self.assertFalse(
                is_issued_option_exit_decision(copy.copy(decision))
            )
            close_reasons = {
                "STOP",
                "TARGET",
                "MAX_HOLD_10_SESSIONS",
                "DTE_21",
            }
            self.assertNotIn("EXPIRY", close_reasons)
            self.assertFalse(hasattr(options_module, "OptionWindowEvaluation"))


def option_validation_window(
    *,
    closed_trades: int = 20,
    elapsed_days: int = 28,
    net_r: Decimal = Decimal("0.1"),
    adherence: tuple[bool, ...] = (True,) * 9 + (False,),
    terminal_equity: Decimal = Decimal("4750"),
    hard_breach_codes: tuple[str, ...] = (),
    missing_mark_sessions: tuple[date, ...] = (),
    missing_exit_review_sessions: tuple[date, ...] = (),
    missed_required_close_sessions: tuple[date, ...] = (),
    record_complete: bool = True,
) -> OptionWindow:
    started = date(2026, 7, 20)
    return _diagnostic_option_validation_window(
        window_id="phase2-window",
        started_session=started,
        through_session=started + timedelta(days=elapsed_days),
        closed_trade_net_rs=(net_r,) * closed_trades,
        equity_curve=(Decimal("5000"), terminal_equity),
        adherence_checks=adherence,
        hard_breach_codes=hard_breach_codes,
        missing_mark_sessions=missing_mark_sessions,
        missing_exit_review_sessions=missing_exit_review_sessions,
        missed_required_close_sessions=missed_required_close_sessions,
        record_complete=record_complete,
        prior_window=None,
    )


class OptionProspectiveWindowTests(unittest.TestCase):
    def test_twenty_closes_and_twenty_eight_days_are_inclusive(self) -> None:
        passing = option_validation_window()
        decision = evaluate_option_window(passing)
        self.assertIsInstance(decision, OptionPromotionDecision)
        self.assertEqual(decision.status, OptionPromotionStatus.PASSED)
        self.assertEqual(decision.closed_primary_trades, 20)
        self.assertEqual(decision.elapsed_days, 28)
        self.assertEqual(decision.mean_net_r, Decimal("0.1"))
        self.assertEqual(decision.adherence, Decimal("0.9"))
        self.assertEqual(decision.maximum_drawdown, Decimal("250"))
        self.assertTrue(decision.diagnostic_only)
        self.assertFalse(decision.promotion_authorized)

        cases = (
            (option_validation_window(closed_trades=19), "MINIMUM_CLOSED_OPTION_TRADES_NOT_MET"),
            (option_validation_window(elapsed_days=27), "MINIMUM_PHASE2_DAYS_NOT_MET"),
        )
        for window, reason in cases:
            with self.subTest(reason=reason):
                current = evaluate_option_window(window)
                self.assertEqual(current.status, OptionPromotionStatus.IN_PROGRESS)
                self.assertIn(reason, current.reason_codes)

    def test_expectancy_adherence_drawdown_and_hard_breach_gate_promotion(
        self,
    ) -> None:
        cases = (
            (
                option_validation_window(net_r=Decimal("0")),
                OptionPromotionStatus.IN_PROGRESS,
                "MEAN_OPTION_NET_R_NOT_POSITIVE",
            ),
            (
                option_validation_window(adherence=(True,) * 8 + (False,) * 2),
                OptionPromotionStatus.IN_PROGRESS,
                "OPTION_ADHERENCE_BELOW_90_PERCENT",
            ),
            (
                option_validation_window(terminal_equity=Decimal("4749.999999")),
                OptionPromotionStatus.IN_PROGRESS,
                "OPTION_MAX_DRAWDOWN_ABOVE_250",
            ),
            (
                option_validation_window(
                    hard_breach_codes=("EXERCISE_ATTEMPTED",),
                ),
                OptionPromotionStatus.FAILED,
                "EXERCISE_ATTEMPTED",
            ),
        )
        for window, expected_status, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_option_window(window)
                self.assertEqual(decision.status, expected_status)
                self.assertIn(reason, decision.reason_codes)

    def test_missing_mark_is_permanent_until_exact_explicit_fresh_start(
        self,
    ) -> None:
        missing_session = date(2026, 8, 12)
        failed = option_validation_window(
            missing_mark_sessions=(missing_session,),
        )
        decision = evaluate_option_window(failed)
        self.assertEqual(decision.status, OptionPromotionStatus.RESTART_REQUIRED)
        self.assertIn("INCOMPLETE_OPTION_MARKS", decision.reason_codes)
        self.assertIs(start_next_window(failed, None), failed)

        start = _diagnostic_option_window_start(
            failed,
            started_session=date(2026, 8, 19),
            recorded_at=datetime(2026, 8, 19, 8, 0, tzinfo=ET),
        )
        fresh = start_next_window(failed, copy.copy(start))

        self.assertEqual(
            evaluate_option_window(fresh).status,
            OptionPromotionStatus.IN_PROGRESS,
        )
        self.assertEqual(fresh.closed_trade_net_rs, ())
        self.assertEqual(fresh.missing_mark_sessions, ())
        self.assertEqual(fresh.prior_window_id, failed.window_id)
        self.assertEqual(fresh.prior_window_source_digest, failed.source_digest)
        self.assertEqual(failed.missing_mark_sessions, (missing_session,))
        self.assertFalse(evaluate_option_window(copy.copy(fresh)).promotion_authorized)

    def test_missing_exit_review_or_required_close_forces_fresh_restart(
        self,
    ) -> None:
        missing_review = date(2026, 8, 11)
        missed_close = date(2026, 8, 12)
        failed = option_validation_window(
            missing_exit_review_sessions=(missing_review,),
            missed_required_close_sessions=(missed_close,),
        )

        decision = evaluate_option_window(failed)

        self.assertEqual(
            decision.status,
            OptionPromotionStatus.RESTART_REQUIRED,
        )
        self.assertIn("INCOMPLETE_OPTION_EXIT_REVIEWS", decision.reason_codes)
        self.assertIn("MISSED_REQUIRED_OPTION_CLOSE", decision.reason_codes)
        self.assertIs(start_next_window(failed, None), failed)

        start = _diagnostic_option_window_start(
            failed,
            started_session=date(2026, 8, 19),
            recorded_at=datetime(2026, 8, 19, 8, 0, tzinfo=ET),
        )
        fresh = start_next_window(failed, start)
        self.assertEqual(fresh.missing_exit_review_sessions, ())
        self.assertEqual(fresh.missed_required_close_sessions, ())
        self.assertEqual(
            evaluate_option_window(fresh).status,
            OptionPromotionStatus.IN_PROGRESS,
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
