from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock
from urllib.parse import urlencode

import stock_monitor.evidence as evidence_module
import stock_monitor.journal as journal_module
import stock_monitor.provider_workflows as provider_workflows_module
import stock_monitor.risk as risk_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.indicators import IndicatorError
from stock_monitor.journal import (
    ActualCloseFailureBinding,
    ActualCloseReceiptBinding,
    InvalidJournalValue,
    Journal,
)
from stock_monitor.providers.alpaca import recompute_alpaca_page_metadata
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    replay_actual,
)
from stock_monitor.risk import (
    RiskBlock,
    is_issued_actual_close_market_source,
    is_issued_actual_position_event_context,
    is_issued_market_mark,
    issue_actual_close_mark,
    issue_actual_close_market_source,
    issue_actual_position_event_context,
)
from tests.integration.test_phase1_authorities import (
    _append_completed_entry_observations,
    _persist_signal_evidence,
)
from tests.integration import (
    test_phase1_authorities as phase1_authority_fixture_module,
    test_task7_position_plan_source as position_plan_fixture_module,
)
from tests.integration.test_signal_lifecycle import (
    _calendar,
    _issued_candidates,
    _publish,
)
from tests.support import aware_et, policy_fixture
from tests.unit import _task5_fixtures as task5_fixture_module


_SYMBOL = "AAPL"
_MARKET_ROLE_BY_SOURCE_TYPE = {
    "ALPACA_DAILY_BARS": "SIP_DAILY_BAR",
    "ALPACA_HISTORICAL_QUOTES": "SIP_QUOTE",
    "ALPACA_INTRADAY_BARS": "SIP_MINUTE_BAR",
    "ALPACA_LATEST_QUOTES": "IEX_FRESHNESS",
}
_GLOBAL_CLOSE_ROLES = (
    "PRIMARY_HALT_FEED",
    "TRADER_ALERT_HALT",
    "OPERATIONAL_STATUS",
    "CROSS_CHECK_CALENDAR",
)


def _utc_text(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _payload(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class ActualCloseCompositionTests(unittest.TestCase):
    def setUp(self) -> None:
        scoped: dict[
            str,
            tuple[str | None, frozenset[tuple[str, str]]],
        ] = {}
        clear: dict[str, frozenset[tuple[str, str]]] = {}
        for symbol, issuer_cik in (
            (_SYMBOL, "0000000000"),
            ("QQQ", None),
        ):
            role, pair, authority = task5_fixture_module._test_coverage_authority(
                symbol,
                issuer_cik,
            )
            scoped[role] = authority
            clear[role] = frozenset({pair})
        scoped_patcher = mock.patch.dict(
            evidence_module._SCOPED_REFERENCE_AUTHORITIES,
            scoped,
        )
        clear_patcher = mock.patch.dict(
            evidence_module._CLEAR_COVERAGE_AUTHORITIES,
            clear,
        )
        scoped_patcher.start()
        clear_patcher.start()
        canonical_sources_patcher = mock.patch.object(
            provider_workflows_module,
            "_SCOPED_REFERENCE_SOURCES",
            provider_workflows_module._freeze_scoped_reference_sources(),
        )
        canonical_sources_patcher.start()
        canonical_sec_fixture_patcher = mock.patch.object(
            provider_workflows_module,
            "_SEC_ARCHIVE_PATH",
            provider_workflows_module.re.compile(
                r"/Archives/edgar/data/[0-9]+/[0-9]{18}/"
                r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z"
            ),
        )
        canonical_sec_fixture_patcher.start()
        self.addCleanup(canonical_sec_fixture_patcher.stop)
        self.addCleanup(canonical_sources_patcher.stop)
        self.addCleanup(clear_patcher.stop)
        self.addCleanup(scoped_patcher.stop)

    @staticmethod
    def _confirmation_action_source_at(
        journal: Journal,
        signal_source: object,
        *,
        event_clock: str,
        after: datetime,
    ):
        maximum_entry = risk_module.money_from_micros(
            signal_source.maximum_entry_micros
        )
        tick_size = risk_module.money_from_micros(
            signal_source.tick_size_micros
        )
        recommended_stop = risk_module.money_from_micros(
            signal_source.recommended_stop_micros
        )
        message_time = after + timedelta(seconds=1)
        received_at = after + timedelta(seconds=2)
        result = ingest_confirmation(
            journal,
            ConfirmationEnvelope(
                message_id=(
                    f"actual-close-entry:{signal_source.signal_id}:"
                    f"{event_clock}"
                ),
                message_time=message_time,
                received_at=received_at,
                text=(
                    f"BOUGHT {signal_source.symbol} "
                    f"{signal_source.planned_shares} shares "
                    f"@ {maximum_entry} AT {event_clock} ET; "
                    f"BID {maximum_entry - tick_size} ASK {maximum_entry}; "
                    f"STOP SET @ {recommended_stop}"
                ),
                session_date=signal_source.publication_session,
            ),
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
            entry_authorities=UnavailableActualEntryAuthorityResolver(),
        )
        with journal.transaction() as transaction:
            action_source = transaction.read_action_source(
                execution_event_id=result.actions[0].event_row_id,
            )
        return action_source, received_at + timedelta(seconds=1)

    def _seed_linked_open_position(
        self,
        journal: Journal,
        *,
        entry_clock: str = "09:37",
    ):
        _publish(journal)
        publication_cutoff = aware_et(date(2026, 8, 14), "08:45")
        signal_source = journal._read_phase1_canonical_replay_source(
            query_cutoff=publication_cutoff,
        ).signal_sources[0]
        trigger_id, quote_id, completed_at = (
            _append_completed_entry_observations(journal, signal_source)
        )
        action_source, recorded_at = self._confirmation_action_source_at(
            journal,
            signal_source,
            event_clock=entry_clock,
            after=completed_at,
        )
        journal.record_phase1_entry(
            signal_source.signal_id,
            confirmation_action_source=action_source,
            trigger_observation_id=trigger_id,
            quote_observation_id=quote_id,
            calendar_resolver=_calendar(),
            recorded_at=recorded_at,
        )
        return signal_source, recorded_at + timedelta(seconds=1)

    def _seed_two_linked_open_positions(self, journal: Journal):
        phase1_authority_fixture_module._start_window(journal)
        first_session = date(2026, 8, 14)
        second_session = _calendar().add_sessions(first_session, 1)
        first_signal = phase1_authority_fixture_module._publish_session_primary(
            journal,
            session_date=first_session,
            sequence=701,
        )
        self.assertEqual(first_signal.symbol, _SYMBOL)
        recorded_through = aware_et(first_session, "08:45")

        def open_position(signal_source, *, ordinal: int) -> None:
            nonlocal recorded_through
            trigger_id, quote_id, completed_at = (
                _append_completed_entry_observations(
                    journal,
                    signal_source,
                )
            )
            action_source, recorded_at = self._confirmation_action_source_at(
                journal,
                signal_source,
                event_clock=f"09:{36 + ordinal:02d}",
                after=completed_at + timedelta(seconds=ordinal * 3),
            )
            journal.record_phase1_entry(
                signal_source.signal_id,
                confirmation_action_source=action_source,
                trigger_observation_id=trigger_id,
                quote_observation_id=quote_id,
                calendar_resolver=_calendar(),
                recorded_at=recorded_at,
            )
            recorded_through = max(
                recorded_through,
                recorded_at + timedelta(seconds=1),
            )

        open_position(first_signal, ordinal=1)
        daily, _execution, quotes, transport, provider_retrieved_at = (
            phase1_authority_fixture_module._issued_exit_review_cohorts(
                symbol=first_signal.symbol,
                session_date=first_session,
                bid=Decimal("20.25"),
                ask=Decimal("20.27"),
                previous_session_low=Decimal("19.00"),
                execution_open=Decimal("20.20"),
                execution_high=Decimal("20.35"),
                execution_low=Decimal("20.05"),
                execution_close=Decimal("20.28"),
            )
        )
        daily_rows = phase1_authority_fixture_module._pin_provider_cohort_pages(
            journal,
            daily,
            transport,
        )
        quote_rows = phase1_authority_fixture_module._pin_provider_cohort_pages(
            journal,
            quotes,
            transport,
        )
        mark_cutoff = max(
            recorded_through,
            provider_retrieved_at,
        ) + timedelta(minutes=1)
        journal.ingest_phase1_equity_mark_cohorts(
            first_session,
            quote_cohort=quotes,
            daily_bar_cohort=daily,
            core_source_row_ids=(*daily_rows, *quote_rows),
            calendar_resolver=_calendar(),
            recorded_at=mark_cutoff,
        )
        phase1_authority_fixture_module._record_cash_only_session_mark(
            journal,
            session_date=first_session,
            query_cutoff=mark_cutoff,
        )
        second_candidates = (
            phase1_authority_fixture_module._issued_provider_candidates_for_session(
                second_session,
                sequence=702,
                count=2,
            )
        )
        qqq_candidate = next(
            candidate
            for candidate in second_candidates
            if candidate.symbol == "QQQ"
        )
        with mock.patch.object(
            phase1_authority_fixture_module,
            "_issued_provider_candidates_for_session",
            return_value=(qqq_candidate,),
        ):
            second_signal = (
                phase1_authority_fixture_module._publish_session_primary(
                    journal,
                    session_date=second_session,
                    sequence=702,
                )
            )
        self.assertEqual(second_signal.symbol, "QQQ")
        open_position(second_signal, ordinal=2)
        return (first_signal, second_signal), recorded_through

    def _context_inputs(
        self,
        journal: Journal,
        signal_source: object,
        *,
        review_at: datetime,
        query_cutoff: datetime,
        binary_event_coverage: str = "CONFIRMED_CLEAR",
        review_retrieved_at: datetime | None = None,
        **market_overrides,
    ):
        self._persist_clear_event_evidence(
            journal,
            signal_source,
            review_at,
            binary_event_coverage=binary_event_coverage,
        )
        receipts = self._append_market_receipts(
            journal,
            self._market_specs(
                review_at=review_at,
                **market_overrides,
            ),
        )
        event_evidence = self._final_event_evidence(
            journal,
            signal_source,
            review_at=review_at,
            query_cutoff=query_cutoff,
        )
        review_source = self._append_review_for_receipts(
            journal,
            receipts=receipts,
            review_at=review_at,
            query_cutoff=query_cutoff,
            event_evidence=event_evidence,
            retrieved_at=review_retrieved_at,
        )
        plan_source = self._final_plan(journal, query_cutoff)
        event_evidence = self._final_event_evidence(
            journal,
            signal_source,
            review_at=review_at,
            query_cutoff=query_cutoff,
        )
        history_source = journal.read_latest_close_recommendation_source(
            position_plan_source=plan_source,
            query_cutoff=query_cutoff,
        )
        market_source = issue_actual_close_market_source(
            review_source,
            plan_source,
            calendar_resolver=_calendar(),
        )
        return market_source, plan_source, event_evidence, history_source

    def _append_review_for_receipts(
        self,
        journal: Journal,
        *,
        receipts,
        global_receipts=(),
        review_at: datetime,
        query_cutoff: datetime,
        event_evidence=None,
        event_evidence_row_ids: tuple[int, ...] | None = None,
        retrieved_at: datetime | None = None,
    ):
        receipt_ids = tuple(receipt.row_id for receipt in receipts)
        current_receipts = journal.read_source_observation_receipts(receipt_ids)
        receipt_bindings = [
            ActualCloseReceiptBinding(
                _SYMBOL,
                _MARKET_ROLE_BY_SOURCE_TYPE[receipt.source_type],
                receipt,
            )
            for receipt in current_receipts
        ]
        current_global_receipts = journal.read_source_observation_receipts(
            tuple(receipt.row_id for receipt in global_receipts)
        ) if global_receipts else ()
        for receipt in current_global_receipts:
            details = json.loads(receipt.details_json)
            receipt_bindings.append(
                ActualCloseReceiptBinding(
                    None,
                    details["source_role"],
                    receipt,
                )
            )
        covered_roles = {
            binding.source_role for binding in receipt_bindings
        }
        failures = [
            ActualCloseFailureBinding(
                _SYMBOL,
                role,
                "SOURCE_UNAVAILABLE",
                query_cutoff,
            )
            for role in _MARKET_ROLE_BY_SOURCE_TYPE.values()
            if role not in covered_roles
        ]
        if event_evidence is None:
            failures.append(
                ActualCloseFailureBinding(
                    _SYMBOL,
                    "EVENT_EVIDENCE",
                    "SOURCE_UNAVAILABLE",
                    query_cutoff,
                )
            )
        else:
            evidence_bindings = risk_module._phase1_bound_sources(
                event_evidence
            )
            self.assertEqual(len(evidence_bindings), 1)
            evidence_source = evidence_bindings[0][0]
            evidence_row_ids = (
                evidence_source.source_observation_row_ids
                if event_evidence_row_ids is None
                else event_evidence_row_ids
            )
            evidence_receipts = journal.read_source_observation_receipts(
                evidence_row_ids
            )
            receipt_bindings.extend(
                ActualCloseReceiptBinding(
                    _SYMBOL,
                    "EVENT_EVIDENCE",
                    receipt,
                )
                for receipt in evidence_receipts
            )
        covered_global_roles = {
            binding.source_role
            for binding in receipt_bindings
            if binding.symbol is None
        }
        failures.extend(
            ActualCloseFailureBinding(
                None,
                role,
                "SOURCE_UNAVAILABLE",
                query_cutoff,
            )
            for role in _GLOBAL_CLOSE_ROLES
            if role not in covered_global_roles
        )
        review_session = review_at.astimezone(
            _calendar().session(review_at.date()).timezone
        ).date()
        return journal.append_actual_close_review(
            session_date=review_session,
            review_at=review_at,
            mark_cutoff=review_at - timedelta(minutes=16),
            query_cutoff=query_cutoff,
            retrieved_at=(
                query_cutoff if retrieved_at is None else retrieved_at
            ),
            receipt_bindings=tuple(receipt_bindings),
            failure_bindings=tuple(failures),
        )

    def _issue_market_from_receipts(
        self,
        journal: Journal,
        *,
        receipts,
        review_at: datetime,
        query_cutoff: datetime,
        review_retrieved_at: datetime | None = None,
    ):
        if journal.count("phase1_signals") == 0:
            _signal_source, position_ready_cutoff = (
                self._seed_linked_open_position(journal)
            )
        else:
            with journal.transaction() as transaction:
                replay_source = transaction.read_actual_replay(
                    query_cutoff=aware_et(date(2026, 12, 31), "23:59"),
                )
            position_ready_cutoff = max(
                action.received_at for action in replay_source.actions
            ) + timedelta(seconds=2)
        effective_query_cutoff = query_cutoff
        if query_cutoff >= review_at and all(
            receipt.retrieved_at <= query_cutoff for receipt in receipts
        ):
            effective_query_cutoff = max(
                query_cutoff,
                position_ready_cutoff,
            )
        review_source = self._append_review_for_receipts(
            journal,
            receipts=receipts,
            review_at=review_at,
            query_cutoff=effective_query_cutoff,
            retrieved_at=review_retrieved_at,
        )
        plan_source = self._final_plan(journal, effective_query_cutoff)
        return issue_actual_close_market_source(
            review_source,
            plan_source,
            calendar_resolver=_calendar(),
        )

    @staticmethod
    def _actual_snapshot(journal: Journal, query_cutoff: datetime):
        with journal.transaction() as transaction:
            replay_source = transaction.read_actual_replay(
                query_cutoff=query_cutoff,
            )
        state = replay_actual(
            replay_source,
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
        )
        return replay_source, state

    def _final_plan(self, journal: Journal, query_cutoff: datetime):
        replay_source, state = self._actual_snapshot(journal, query_cutoff)
        resolution = journal.resolve_actual_position_plan_source(
            actual_replay_source=replay_source,
            actual_position_state=state,
            symbol=_SYMBOL,
            query_cutoff=query_cutoff,
        )
        self.assertEqual(resolution.status, "RESOLVED")
        self.assertIsNotNone(resolution.source)
        return resolution.source

    @staticmethod
    def _session_at(day: date, clock) -> datetime:
        return datetime.combine(day, clock, tzinfo=_calendar().session(day).timezone)

    @staticmethod
    def _completed_sessions(day: date) -> tuple[date, ...]:
        resolver = _calendar()
        values = [resolver.previous_session(day)]
        while len(values) < 14:
            values.append(resolver.previous_session(values[-1]))
        values.reverse()
        return tuple(values)

    def _market_specs(
        self,
        *,
        review_at: datetime,
        symbol: str = _SYMBOL,
        quote_offsets: tuple[timedelta, ...] = (timedelta(),),
        sip_bid: str = "20.25",
        sip_ask: str = "20.27",
        iex_bid: str = "999.00",
        iex_ask: str = "999.01",
        iex_offset: timedelta = timedelta(minutes=-1),
        retrieved_offset: timedelta = timedelta(),
        include_sip: bool = True,
        include_iex: bool = True,
        include_daily: bool = True,
        include_intraday: bool = True,
        multipage_sip: bool = False,
        latest_omits_next_page_token: bool = False,
        health_by_type: dict[str, str] | None = None,
        payload_feed_by_type: dict[str, str] | None = None,
        daily_bar_count: int = 14,
        daily_volume: int = 1_000_000,
        intraday_volumes: tuple[int, int] = (50_000, 45_000),
        multipage_intraday: bool = False,
    ) -> tuple[dict[str, object], ...]:
        resolver = _calendar()
        session_day = review_at.astimezone(
            resolver.session(review_at.date()).timezone
        ).date()
        schedule = resolver.session(session_day)
        session_close = datetime.combine(
            session_day,
            schedule.close_time,
            tzinfo=schedule.timezone,
        )
        cutoff = min(review_at - timedelta(minutes=16), session_close)
        quote_start = cutoff - timedelta(minutes=5)
        retrieved_at = review_at + retrieved_offset
        health_by_type = health_by_type or {}
        payload_feed_by_type = payload_feed_by_type or {}
        specs: list[dict[str, object]] = []

        def item(source_type: str, value: dict[str, object]) -> dict[str, object]:
            payload_feed = payload_feed_by_type.get(source_type)
            return (
                value
                if payload_feed is None
                else {**value, "feed": payload_feed}
            )

        def add(
            *,
            source_type: str,
            source_uri: str,
            feed: str,
            document: object,
        ) -> None:
            raw = _payload(document)
            metadata = recompute_alpaca_page_metadata(
                payload=raw,
                request_url=source_uri,
                source_type=source_type,
                retrieved_at=retrieved_at,
            )
            specs.append(
                {
                    "payload": raw,
                    "source_uri": source_uri,
                    "source_type": source_type,
                    "provider": "alpaca",
                    "feed": feed,
                    "source_time": metadata.source_time,
                    "retrieved_at": metadata.retrieved_at,
                    "provider_sequence": None,
                    "delay_seconds": metadata.delay_seconds,
                    "health_result": health_by_type.get(source_type, "OK"),
                    "details": {
                        "source_observation_id": metadata.source_observation_id,
                    },
                }
            )

        completed_sessions = self._completed_sessions(session_day)
        if include_daily:
            first_schedule = resolver.session(completed_sessions[0])
            last_schedule = resolver.session(completed_sessions[-1])
            daily_start = datetime.combine(
                completed_sessions[0],
                first_schedule.open_time,
                tzinfo=first_schedule.timezone,
            )
            daily_end = datetime.combine(
                completed_sessions[-1],
                last_schedule.close_time,
                tzinfo=last_schedule.timezone,
            )
            daily_url = "https://data.alpaca.markets/v2/stocks/bars?" + urlencode(
                (
                    ("symbols", symbol),
                    ("timeframe", "1Day"),
                    ("start", _utc_text(daily_start)),
                    ("end", _utc_text(daily_end)),
                    ("adjustment", "split"),
                    ("feed", "sip"),
                    ("limit", "10000"),
                )
            )
            bars = []
            for ordinal in range(daily_bar_count):
                session_date = completed_sessions[min(ordinal, 13)]
                session = resolver.session(session_date)
                timestamp = datetime.combine(
                    session_date,
                    session.close_time,
                    tzinfo=session.timezone,
                ) + timedelta(minutes=max(0, ordinal - 13))
                close = Decimal("19.00") + Decimal(ordinal) / Decimal("10")
                bars.append(
                    item(
                        "ALPACA_DAILY_BARS",
                        {
                        "c": str(close),
                        "h": str(close + Decimal("0.20")),
                        "l": str(close - Decimal("0.20")),
                        "o": str(close - Decimal("0.05")),
                        "t": _utc_text(timestamp),
                        "v": daily_volume + ordinal,
                        },
                    )
                )
            add(
                source_type="ALPACA_DAILY_BARS",
                source_uri=daily_url,
                feed="sip",
                document={"bars": {symbol: bars}, "next_page_token": None},
            )

        if include_intraday:
            session_open = datetime.combine(
                session_day,
                schedule.open_time,
                tzinfo=schedule.timezone,
            )
            intraday_url = (
                "https://data.alpaca.markets/v2/stocks/bars?"
                + urlencode(
                    (
                        ("symbols", symbol),
                        ("timeframe", "1Min"),
                        ("start", _utc_text(session_open)),
                        ("end", _utc_text(cutoff)),
                        ("adjustment", "split"),
                        ("feed", "sip"),
                        ("limit", "10000"),
                    )
                )
            )
            intraday_bars = (
                item(
                    "ALPACA_INTRADAY_BARS",
                    {
                        "c": "20.30",
                        "h": "20.35",
                        "l": "20.05",
                        "o": "20.20",
                        "t": _utc_text(cutoff - timedelta(minutes=1)),
                        "v": intraday_volumes[0],
                    },
                ),
                item(
                    "ALPACA_INTRADAY_BARS",
                    {
                        "c": "20.28",
                        "h": "20.32",
                        "l": "20.10",
                        "o": "20.30",
                        "t": _utc_text(cutoff),
                        "v": intraday_volumes[1],
                    },
                ),
            )
            if multipage_intraday:
                add(
                    source_type="ALPACA_INTRADAY_BARS",
                    source_uri=intraday_url,
                    feed="sip",
                    document={
                        "bars": {symbol: [intraday_bars[0]]},
                        "next_page_token": "intraday-next-1",
                    },
                )
                add(
                    source_type="ALPACA_INTRADAY_BARS",
                    source_uri=(
                        intraday_url + "&page_token=intraday-next-1"
                    ),
                    feed="sip",
                    document={
                        "bars": {symbol: [intraday_bars[1]]},
                        "next_page_token": None,
                    },
                )
            else:
                add(
                    source_type="ALPACA_INTRADAY_BARS",
                    source_uri=intraday_url,
                    feed="sip",
                    document={
                        "bars": {symbol: list(intraday_bars)},
                        "next_page_token": None,
                    },
                )

        if include_sip:
            quote_url = "https://data.alpaca.markets/v2/stocks/quotes?" + urlencode(
                (
                    ("symbols", symbol),
                    ("start", _utc_text(quote_start)),
                    ("end", _utc_text(cutoff)),
                    ("feed", "sip"),
                    ("limit", "10000"),
                )
            )
            if multipage_sip:
                add(
                    source_type="ALPACA_HISTORICAL_QUOTES",
                    source_uri=quote_url,
                    feed="sip",
                    document={
                        "quotes": {
                            symbol: [
                                item(
                                    "ALPACA_HISTORICAL_QUOTES",
                                    {
                                    "ap": "20.22",
                                    "bp": "20.20",
                                    "i": 1,
                                    "t": _utc_text(
                                        cutoff - timedelta(minutes=4)
                                    ),
                                    },
                                )
                            ]
                        },
                        "next_page_token": "next-1",
                    },
                )
                add(
                    source_type="ALPACA_HISTORICAL_QUOTES",
                    source_uri=quote_url + "&page_token=next-1",
                    feed="sip",
                    document={
                        "quotes": {
                            symbol: [
                                item(
                                    "ALPACA_HISTORICAL_QUOTES",
                                    {
                                    "ap": sip_ask,
                                    "bp": sip_bid,
                                    "i": 2,
                                    "t": _utc_text(cutoff),
                                    },
                                )
                            ]
                        },
                        "next_page_token": None,
                    },
                )
            else:
                quotes = [
                    item(
                        "ALPACA_HISTORICAL_QUOTES",
                        {
                            "ap": sip_ask,
                            "bp": sip_bid,
                            "i": ordinal,
                            "t": _utc_text(cutoff + offset),
                        },
                    )
                    for ordinal, offset in enumerate(quote_offsets, start=1)
                ]
                add(
                    source_type="ALPACA_HISTORICAL_QUOTES",
                    source_uri=quote_url,
                    feed="sip",
                    document={
                        "quotes": {symbol: quotes},
                        "next_page_token": None,
                    },
                )

        if include_iex:
            iex_url = (
                "https://data.alpaca.markets/v2/stocks/quotes/latest?"
                + urlencode((("symbols", symbol), ("feed", "iex")))
            )
            iex_document = {
                "quotes": {
                    symbol: item(
                        "ALPACA_LATEST_QUOTES",
                        {
                            "ap": iex_ask,
                            "bp": iex_bid,
                            "i": 1,
                            "t": _utc_text(review_at + iex_offset),
                        },
                    )
                },
            }
            if not latest_omits_next_page_token:
                iex_document["next_page_token"] = None
            add(
                source_type="ALPACA_LATEST_QUOTES",
                source_uri=iex_url,
                feed="iex",
                document=iex_document,
            )
        return tuple(specs)

    @staticmethod
    def _rewrite_market_spec(
        spec: dict[str, object],
        **overrides: object,
    ) -> dict[str, object]:
        rewritten = {**spec, **overrides}
        metadata = recompute_alpaca_page_metadata(
            payload=rewritten["payload"],
            request_url=rewritten["source_uri"],
            source_type=rewritten["source_type"],
            retrieved_at=rewritten["retrieved_at"],
        )
        rewritten.update(
            source_time=metadata.source_time,
            retrieved_at=metadata.retrieved_at,
            delay_seconds=metadata.delay_seconds,
            details={"source_observation_id": metadata.source_observation_id},
        )
        return rewritten

    @staticmethod
    def _global_close_specs(
        review_at: datetime,
        *,
        retrieved_at: datetime | None = None,
        timestamp_source: str = "PRIMARY_METADATA",
    ) -> tuple[dict[str, object], ...]:
        observed_at = (
            review_at
            if timestamp_source == "PRIMARY_METADATA"
            else retrieved_at
        )
        if observed_at is None:
            raise AssertionError(
                "timestamp-unavailable references need a retrieval time"
            )
        sources = (
            (
                "PRIMARY_HALT_FEED",
                "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
                "Nasdaq",
            ),
            (
                "TRADER_ALERT_HALT",
                "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
                "Nasdaq",
            ),
            (
                "OPERATIONAL_STATUS",
                "https://www.nyse.com/api/notifications/public/alerts?2=3",
                "New York Stock Exchange",
            ),
            (
                "CROSS_CHECK_CALENDAR",
                "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
                "Nasdaq",
            ),
        )
        return tuple(
            {
                "payload": _payload({"role": role, "status": "CLEAR"}),
                "source_uri": source_uri,
                "source_type": "OFFICIAL_REFERENCE",
                "provider": provider,
                "feed": timestamp_source,
                "source_time": observed_at,
                "retrieved_at": observed_at,
                "provider_sequence": None,
                "delay_seconds": 0,
                "health_result": "OK",
                "details": {
                    "accession": None,
                    "issuer_cik": None,
                    "source_observation_id": (
                        f"actual-close-{role.lower().replace('_', '-')}"
                    ),
                    "source_role": role,
                    "symbol": None,
                    "timestamp_source": timestamp_source,
                },
            }
            for role, source_uri, provider in sources
        )

    @staticmethod
    def _persist_clear_event_evidence(
        journal: Journal,
        signal_source: object,
        review_at: datetime,
        *,
        binary_event_coverage: str = "CONFIRMED_CLEAR",
    ) -> None:
        _persist_signal_evidence(
            journal,
            adverse=False,
            review_at=review_at,
            signal_source=signal_source,
            binary_event_coverage=binary_event_coverage,
            source_retrieved_at=review_at,
        )

    @staticmethod
    def _final_event_evidence(
        journal: Journal,
        signal_source: object,
        *,
        review_at: datetime,
        query_cutoff: datetime,
    ):
        current_signal_source = journal._read_phase1_signal_source(
            signal_source.signal_id,
            query_cutoff=query_cutoff,
        )
        evidence_source = journal._read_phase1_signal_evidence_source(
            signal_source.signal_id,
            review_at=review_at,
            query_cutoff=query_cutoff,
            calendar_resolver=_calendar(),
            exact_signal_source=current_signal_source,
        )
        return risk_module._issue_phase1_signal_evidence_authority_from_source(
            evidence_source,
            calendar_resolver=_calendar(),
        )

    def _append_market_receipts(
        self,
        journal: Journal,
        specs: tuple[dict[str, object], ...],
    ):
        row_ids: list[int] = []
        with journal.transaction() as transaction:
            for spec in specs:
                row_id, duplicate = transaction.append_source_observation(**spec)
                if duplicate:
                    raise AssertionError("market receipt fixture unexpectedly duplicated")
                row_ids.append(row_id)
        return journal.read_source_observation_receipts(tuple(row_ids))

    def _issue_source(
        self,
        journal: Journal,
        *,
        review_at: datetime,
        query_cutoff: datetime,
        **market_overrides,
    ):
        receipts = self._append_market_receipts(
            journal,
            self._market_specs(review_at=review_at, **market_overrides),
        )
        source = self._issue_market_from_receipts(
            journal,
            receipts=receipts,
            review_at=review_at,
            query_cutoff=query_cutoff,
        )
        return source, receipts

    def _fixture_close_collector(
        self,
        signal_source: object,
        *,
        before_collection_writes=None,
        during_collection=None,
        collection_duration: timedelta = timedelta(),
        **market_overrides: object,
    ):
        case = self

        class FixtureCollector:
            def __init__(collector_self) -> None:
                collector_self.symbols: list[tuple[str, ...]] = []
                collector_self.generations: list[tuple[int, int]] = []

            def collect_close_sources(
                collector_self,
                *,
                journal: Journal,
                symbols: tuple[str, ...],
                session_date: date,
                review_at: datetime,
                mark_cutoff: datetime,
                command_started_at: datetime,
            ):
                del session_date
                case.assertEqual(mark_cutoff, review_at - timedelta(minutes=16))
                collected_at = command_started_at + collection_duration
                collector_self.symbols.append(symbols)
                before = journal._source_generation
                if before_collection_writes is not None:
                    before_collection_writes(journal)
                case._persist_clear_event_evidence(
                    journal,
                    signal_source,
                    review_at,
                )
                receipts = case._append_market_receipts(
                    journal,
                    case._market_specs(
                        review_at=review_at,
                        **market_overrides,
                    ),
                )
                global_receipts = case._append_market_receipts(
                    journal,
                    case._global_close_specs(review_at),
                )
                if during_collection is not None:
                    during_collection(journal)
                event_evidence = case._final_event_evidence(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=collected_at,
                )
                review = case._append_review_for_receipts(
                    journal,
                    receipts=receipts,
                    global_receipts=global_receipts,
                    review_at=review_at,
                    query_cutoff=collected_at,
                    event_evidence=event_evidence,
                    retrieved_at=collected_at,
                )
                collector_self.generations.append(
                    (before, journal._source_generation)
                )
                return provider_workflows_module.ActualCloseCollection(
                    review_id=review.review_id,
                    collected_at=collected_at,
                )

        return FixtureCollector()

    def _empty_close_collector(
        self,
        *,
        failed_global_roles: tuple[str, ...] = (),
        collection_duration: timedelta = timedelta(),
        global_timestamp_source: str = "PRIMARY_METADATA",
    ):
        case = self

        class EmptyCollector:
            def __init__(collector_self) -> None:
                collector_self.symbols: list[tuple[str, ...]] = []
                collector_self.mark_cutoffs: list[datetime] = []

            def collect_close_sources(
                collector_self,
                *,
                journal: Journal,
                symbols: tuple[str, ...],
                session_date: date,
                review_at: datetime,
                mark_cutoff: datetime,
                command_started_at: datetime,
            ):
                case.assertEqual(symbols, ())
                case.assertEqual(mark_cutoff, review_at - timedelta(minutes=16))
                collected_at = command_started_at + collection_duration
                collector_self.symbols.append(symbols)
                collector_self.mark_cutoffs.append(mark_cutoff)
                specs = tuple(
                    spec
                    for spec in case._global_close_specs(
                        review_at,
                        retrieved_at=collected_at,
                        timestamp_source=global_timestamp_source,
                    )
                    if spec["details"]["source_role"]
                    not in failed_global_roles
                )
                receipts = case._append_market_receipts(journal, specs)
                bindings = tuple(
                    ActualCloseReceiptBinding(
                        None,
                        json.loads(receipt.details_json)["source_role"],
                        receipt,
                    )
                    for receipt in receipts
                )
                failures = tuple(
                    ActualCloseFailureBinding(
                        None,
                        role,
                        "SOURCE_UNAVAILABLE",
                        collected_at,
                    )
                    for role in failed_global_roles
                )
                review = journal.append_actual_close_review(
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=mark_cutoff,
                    query_cutoff=collected_at,
                    retrieved_at=collected_at,
                    receipt_bindings=bindings,
                    failure_bindings=failures,
                )
                return provider_workflows_module.ActualCloseCollection(
                    review_id=review.review_id,
                    collected_at=collected_at,
                )

        return EmptyCollector()

    def _two_position_close_collector(self, signal_sources: tuple[object, ...]):
        case = self

        class TwoPositionCollector:
            def __init__(collector_self) -> None:
                collector_self.collection = None

            def collect_close_sources(
                collector_self,
                *,
                journal: Journal,
                symbols: tuple[str, ...],
                session_date: date,
                review_at: datetime,
                mark_cutoff: datetime,
                command_started_at: datetime,
            ):
                case.assertEqual(symbols, (_SYMBOL, "QQQ"))
                case.assertEqual(mark_cutoff, review_at - timedelta(minutes=16))
                for signal_source in signal_sources:
                    case._persist_clear_event_evidence(
                        journal,
                        signal_source,
                        review_at,
                    )
                receipt_ids_by_symbol = {}
                for signal_source in signal_sources:
                    appended = case._append_market_receipts(
                        journal,
                        case._market_specs(
                            review_at=review_at,
                            symbol=signal_source.symbol,
                        ),
                    )
                    receipt_ids_by_symbol[signal_source.symbol] = tuple(
                        receipt.row_id for receipt in appended
                    )
                appended_globals = case._append_market_receipts(
                    journal,
                    case._global_close_specs(review_at),
                )
                global_receipt_ids = tuple(
                    receipt.row_id for receipt in appended_globals
                )
                receipts_by_symbol = {
                    symbol: journal.read_source_observation_receipts(row_ids)
                    for symbol, row_ids in receipt_ids_by_symbol.items()
                }
                global_receipts = journal.read_source_observation_receipts(
                    global_receipt_ids
                )
                receipt_bindings = []
                for signal_source in signal_sources:
                    symbol = signal_source.symbol
                    receipt_bindings.extend(
                        ActualCloseReceiptBinding(
                            symbol,
                            _MARKET_ROLE_BY_SOURCE_TYPE[receipt.source_type],
                            receipt,
                        )
                        for receipt in receipts_by_symbol[symbol]
                    )
                    event_evidence = case._final_event_evidence(
                        journal,
                        signal_source,
                        review_at=review_at,
                        query_cutoff=command_started_at,
                    )
                    evidence_source = risk_module._phase1_bound_sources(
                        event_evidence
                    )[0][0]
                    evidence_receipts = (
                        journal.read_source_observation_receipts(
                            evidence_source.source_observation_row_ids
                        )
                    )
                    receipt_bindings.extend(
                        ActualCloseReceiptBinding(
                            symbol,
                            "EVENT_EVIDENCE",
                            receipt,
                        )
                        for receipt in evidence_receipts
                    )
                receipt_bindings.extend(
                    ActualCloseReceiptBinding(
                        None,
                        json.loads(receipt.details_json)["source_role"],
                        receipt,
                    )
                    for receipt in global_receipts
                )
                review = journal.append_actual_close_review(
                    session_date=session_date,
                    review_at=review_at,
                    mark_cutoff=mark_cutoff,
                    query_cutoff=command_started_at,
                    retrieved_at=command_started_at,
                    receipt_bindings=tuple(receipt_bindings),
                    failure_bindings=(),
                )
                collector_self.collection = (
                    provider_workflows_module.ActualCloseCollection(
                        review_id=review.review_id,
                        collected_at=command_started_at,
                    )
                )
                return collector_self.collection

        return TwoPositionCollector()

    def test_coordinator_uses_early_close_nominal_sip_cutoff(self) -> None:
        session_date = date(2026, 11, 27)
        review_at = aware_et(session_date, "12:30")
        retrieved_at = aware_et(session_date, "12:38")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                collector = self._empty_close_collector()
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=retrieved_at,
                )

                self.assertEqual(
                    collector.mark_cutoffs,
                    [aware_et(session_date, "12:14")],
                )
                self.assertEqual(material.positions, ())
                self.assertEqual(
                    material.report.outcome,
                    "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                )

    def test_public_evidence_read_reuses_only_exact_current_plan_signal(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                stale_signal, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                self._persist_clear_event_evidence(
                    journal,
                    stale_signal,
                    review_at,
                )
                plan_source = self._final_plan(journal, query_cutoff)
                exact_signal = plan_source.signal_source

                evidence_source = (
                    journal.read_phase1_signal_evidence_source(
                        exact_signal.signal_id,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                        calendar_resolver=_calendar(),
                        exact_signal_source=exact_signal,
                    )
                )
                self.assertIs(evidence_source.signal_source, exact_signal)

                for substituted_signal in (
                    replace(exact_signal),
                    stale_signal,
                ):
                    with self.subTest(
                        substituted_signal=substituted_signal
                    ), self.assertRaisesRegex(
                        InvalidJournalValue,
                        "nested signal source is unverified",
                    ):
                        journal.read_phase1_signal_evidence_source(
                            exact_signal.signal_id,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                            exact_signal_source=substituted_signal,
                        )

                with Journal.open(path) as other_journal:
                    cross_owner_signal = (
                        other_journal._read_phase1_signal_source(
                            exact_signal.signal_id,
                            query_cutoff=query_cutoff,
                        )
                    )
                    with self.assertRaisesRegex(
                        InvalidJournalValue,
                        "nested signal source is unverified",
                    ):
                        journal.read_phase1_signal_evidence_source(
                            exact_signal.signal_id,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                            exact_signal_source=cross_owner_signal,
                        )

    def test_coordinator_maps_authoritative_empty_actual_cohort_to_hold(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "15:38")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                collector = self._empty_close_collector()
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=retrieved_at,
                )

                self.assertEqual(collector.symbols, [()])
                self.assertEqual(material.actual_state.positions, ())
                self.assertEqual(material.positions, ())
                self.assertEqual(material.coordinator_reason_codes, ())
                self.assertEqual(
                    material.report.outcome,
                    "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                )
                self.assertIn("NO_ACTUAL_POSITIONS", material.report.body)
                self.assertIn(
                    "MANUAL_VERIFICATION_REQUIRED",
                    material.report.body,
                )
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_coordinator_rejects_terminal_collection_before_command_start(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        command_started_at = aware_et(session_date, "15:38")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=self._empty_close_collector(
                            collection_duration=-timedelta(minutes=1),
                        ),
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                with self.assertRaisesRegex(
                    provider_workflows_module.CanonicalMaterialError,
                    "cutoff must be between review and retrieval",
                ):
                    coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=command_started_at,
                    )

                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_coordinator_accepts_timestamp_unavailable_global_health_after_review(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        command_started_at = aware_et(session_date, "15:38")
        collected_at = aware_et(session_date, "15:39")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                collector = self._empty_close_collector(
                    collection_duration=timedelta(minutes=1),
                    global_timestamp_source="UNAVAILABLE",
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=command_started_at,
                )

                self.assertEqual(material.query_cutoff, collected_at)
                self.assertEqual(material.retrieved_at, collected_at)
                self.assertEqual(len(material.source_receipts), 4)
                self.assertTrue(
                    all(
                        receipt.feed == "UNAVAILABLE"
                        and receipt.source_time == collected_at
                        and receipt.retrieved_at == collected_at
                        for receipt in material.source_receipts
                    )
                )
                self.assertEqual(
                    material.report.outcome,
                    "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                )

    def test_coordinator_maps_empty_global_source_failure_to_data_unavailable(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "15:38")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                collector = self._empty_close_collector(
                    failed_global_roles=("OPERATIONAL_STATUS",),
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=retrieved_at,
                )

                self.assertEqual(collector.symbols, [()])
                self.assertEqual(material.actual_state.positions, ())
                self.assertEqual(material.positions, ())
                self.assertEqual(
                    material.coordinator_reason_codes,
                    ("SOURCE_CHECK_FAILED",),
                )
                self.assertEqual(material.report.outcome, "DATA UNAVAILABLE")
                self.assertIn("SOURCE_CHECK_FAILED", material.report.body)
                self.assertNotIn("NO_ACTUAL_POSITIONS", material.report.body)
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_coordinator_composes_one_verified_position_from_delayed_sip(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        # The linked-entry fixture seals its provider cohort at 16:21, so the
        # invocation cutoff must follow that durable write.
        retrieved_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _position_ready_at = (
                    self._seed_linked_open_position(journal)
                )
                collector = self._fixture_close_collector(
                    signal_source,
                    sip_bid="20.25",
                    sip_ask="20.27",
                    iex_bid="999.00",
                    iex_ask="999.01",
                )

                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                replay_generations: list[int] = []

                def replay_with_generation(*args, **kwargs):
                    replay_generations.append(journal._source_generation)
                    return replay_actual(*args, **kwargs)

                with mock.patch(
                    "stock_monitor.reconciliation.replay_actual",
                    side_effect=replay_with_generation,
                ):
                    material = coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=retrieved_at,
                    )

                self.assertEqual(collector.symbols, [(_SYMBOL,)])
                self.assertGreater(
                    collector.generations[0][1],
                    collector.generations[0][0],
                )
                self.assertEqual(material.query_cutoff, retrieved_at)
                self.assertEqual(len(material.actual_state.positions), 1)
                self.assertEqual(len(material.positions), 1)
                projection = material.positions[0]
                self.assertEqual(
                    type(projection).__name__,
                    "ClosePosition",
                    repr(projection),
                )
                self.assertEqual(projection.symbol, _SYMBOL)
                self.assertEqual(projection.mark, Decimal("20.25"))
                self.assertEqual(projection.provider, "ALPACA")
                self.assertEqual(projection.feed, "SIP")
                self.assertEqual(projection.action, "HOLD")
                self.assertEqual(
                    len(
                        material.composition_authority.position_authority_digests
                    ),
                    1,
                )
                self.assertRegex(
                    material.composition_authority.review_source_digest,
                    r"\A[0-9a-f]{64}\Z",
                )
                self.assertEqual(
                    material.report.outcome,
                    "PROVISIONAL HOLD - VERIFY CURRENT ROBINHOOD PRICE",
                )
                self.assertIn(
                    "MANUAL_VERIFICATION_REQUIRED",
                    material.report.body,
                )
                self.assertEqual(journal.count("close_recommendations"), 1)
                self.assertGreaterEqual(len(replay_generations), 4)
                self.assertGreater(
                    replay_generations[-1],
                    replay_generations[-2],
                )
                self.assertEqual(
                    replay_generations[-1],
                    journal._source_generation,
                )
                self.assertTrue(
                    provider_workflows_module.is_issued_canonical_material(
                        material,
                        journal=journal,
                        report_archive_root=root,
                    )
                )

    def test_coordinator_uses_terminal_collection_time_after_provider_gets(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        command_started_at = aware_et(session_date, "16:30")
        collected_at = aware_et(session_date, "16:31")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _position_ready_at = (
                    self._seed_linked_open_position(journal)
                )
                collector = self._fixture_close_collector(
                    signal_source,
                    collection_duration=timedelta(minutes=1),
                    retrieved_offset=timedelta(minutes=61),
                    iex_offset=timedelta(minutes=60),
                    iex_bid="999.00",
                    iex_ask="999.01",
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=command_started_at,
                )

                self.assertEqual(material.query_cutoff, collected_at)
                self.assertEqual(material.retrieved_at, collected_at)
                self.assertEqual(
                    material.composition_authority.retrieved_at,
                    collected_at,
                )
                self.assertTrue(
                    any(
                        receipt.retrieved_at > command_started_at
                        for receipt in material.source_receipts
                    )
                )
                self.assertTrue(
                    all(
                        receipt.retrieved_at <= material.retrieved_at
                        for receipt in material.source_receipts
                    )
                )
                projection = material.positions[0]
                self.assertEqual(
                    type(projection).__name__,
                    "ClosePosition",
                    repr(projection),
                )
                self.assertEqual(projection.feed, "SIP")
                self.assertEqual(projection.mark, Decimal("20.25"))
                self.assertNotEqual(projection.mark, Decimal("999.00"))
                for forged_authority in (
                    replace(
                        material.composition_authority,
                        review_source_digest="0" * 64,
                    ),
                    replace(
                        material.composition_authority,
                        position_authority_digests=("0" * 64,),
                    ),
                ):
                    forged_material = replace(
                        material,
                        composition_authority=forged_authority,
                    )
                    self.assertFalse(
                        provider_workflows_module.is_issued_canonical_material(
                            forged_material,
                            journal=journal,
                            report_archive_root=root,
                        )
                    )
                with provider_workflows_module._CLOSE_COMPOSITION_LOCK:
                    candidate = provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS[
                        id(material.composition_authority)
                    ]
                original_semantic_children = candidate.semantic_children
                try:
                    object.__setattr__(
                        candidate,
                        "semantic_children",
                        (
                            replace(original_semantic_children[0]),
                            *original_semantic_children[1:],
                        ),
                    )
                    self.assertFalse(
                        provider_workflows_module.is_issued_canonical_material(
                            material,
                            journal=journal,
                            report_archive_root=root,
                        )
                    )
                finally:
                    object.__setattr__(
                        candidate,
                        "semantic_children",
                        original_semantic_children,
                    )
                bundle = candidate.semantic_children[1]
                original_digest = bundle.source_digest
                try:
                    object.__setattr__(bundle, "source_digest", "0" * 64)
                    self.assertFalse(
                        provider_workflows_module.is_issued_canonical_material(
                            material,
                            journal=journal,
                            report_archive_root=root,
                        )
                    )
                finally:
                    object.__setattr__(
                        bundle,
                        "source_digest",
                        original_digest,
                    )
                self.assertTrue(
                    provider_workflows_module.is_issued_canonical_material(
                        material,
                        journal=journal,
                        report_archive_root=root,
                    )
                )

    def test_close_position_authority_rederives_the_exact_decision_projection(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _position_ready_at = (
                    self._seed_linked_open_position(journal)
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=self._fixture_close_collector(
                            signal_source,
                        ),
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )
                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=retrieved_at,
                )
                with provider_workflows_module._CLOSE_COMPOSITION_LOCK:
                    candidate = provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS[
                        id(material.composition_authority)
                    ]
                review_source, bundle = candidate.semantic_children
                validate = (
                    provider_workflows_module._is_issued_close_position_authority_bundle
                )
                self.assertTrue(
                    validate(bundle, review_source=review_source)
                )

                self.assertFalse(
                    validate(replace(bundle), review_source=review_source)
                )
                self.assertFalse(
                    validate(
                        bundle,
                        review_source=replace(review_source),
                    )
                )
                with Journal.open(root / "wrong-owner.sqlite3") as wrong_owner:
                    self.assertFalse(
                        validate(
                            bundle,
                            review_source=review_source,
                            journal=wrong_owner,
                        )
                    )
                for projection in (
                    replace(
                        bundle.projection,
                        action="EXIT",
                        reason_codes=("FORGED_EXIT",),
                    ),
                    replace(
                        bundle.projection,
                        reason_codes=("FORGED_REASON",),
                    ),
                ):
                    forged = provider_workflows_module._ClosePositionAuthorityBundle(
                        projection=projection,
                        branch=bundle.branch,
                        identity_children=bundle.identity_children,
                        source_digest=(
                            provider_workflows_module._close_position_authority_digest(
                                projection=projection,
                                branch=bundle.branch,
                                identity_children=bundle.identity_children,
                            )
                        ),
                    )
                    self.assertFalse(
                        validate(forged, review_source=review_source)
                    )

                children = bundle.identity_children
                context, mark, action, decision = children[8:12]
                copied_action = replace(action)
                try:
                    object.__setattr__(
                        bundle,
                        "identity_children",
                        (*children[:10], copied_action, decision),
                    )
                    object.__setattr__(
                        bundle,
                        "source_digest",
                        provider_workflows_module._close_position_authority_digest(
                            projection=bundle.projection,
                            branch=bundle.branch,
                            identity_children=bundle.identity_children,
                        ),
                    )
                    self.assertFalse(
                        validate(bundle, review_source=review_source)
                    )
                finally:
                    object.__setattr__(bundle, "identity_children", children)
                    object.__setattr__(
                        bundle,
                        "source_digest",
                        provider_workflows_module._close_position_authority_digest(
                            projection=bundle.projection,
                            branch=bundle.branch,
                            identity_children=children,
                        ),
                    )

                for field_name, forged_child in (
                    ("position_action", replace(action)),
                    ("context", replace(context)),
                    ("mark", replace(mark)),
                ):
                    original_child = getattr(decision, field_name)
                    try:
                        object.__setattr__(
                            decision,
                            field_name,
                            forged_child,
                        )
                        self.assertFalse(
                            validate(bundle, review_source=review_source)
                        )
                    finally:
                        object.__setattr__(
                            decision,
                            field_name,
                            original_child,
                        )
                self.assertTrue(
                    validate(bundle, review_source=review_source)
                )

                original_action = bundle.projection.action
                original_reasons = bundle.projection.reason_codes
                original_digest = bundle.source_digest
                try:
                    object.__setattr__(bundle.projection, "action", "EXIT")
                    object.__setattr__(
                        bundle.projection,
                        "reason_codes",
                        ("FORGED_EXIT",),
                    )
                    object.__setattr__(
                        bundle,
                        "source_digest",
                        provider_workflows_module._close_position_authority_digest(
                            projection=bundle.projection,
                            branch=bundle.branch,
                            identity_children=bundle.identity_children,
                        ),
                    )
                    self.assertFalse(
                        validate(bundle, review_source=review_source)
                    )
                finally:
                    object.__setattr__(
                        bundle.projection,
                        "action",
                        original_action,
                    )
                    object.__setattr__(
                        bundle.projection,
                        "reason_codes",
                        original_reasons,
                    )
                    object.__setattr__(bundle, "source_digest", original_digest)

    def test_stop_unverified_authority_rederives_its_exact_action_projection(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _ready_at = self._seed_linked_open_position(
                    journal,
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=self._fixture_close_collector(
                            signal_source,
                        ),
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )
                original_evaluate = risk_module.evaluate_position

                def stop_unverified(position, mark, policy):
                    action = original_evaluate(position, mark, policy)
                    return replace(
                        action,
                        status="STOP_UNVERIFIED",
                        reason_codes=("STOP_UNVERIFIED",),
                        user_confirmed_stop=None,
                    )

                with mock.patch.object(
                    risk_module,
                    "evaluate_position",
                    side_effect=stop_unverified,
                ):
                    material = coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=retrieved_at,
                    )
                    with provider_workflows_module._CLOSE_COMPOSITION_LOCK:
                        candidate = (
                            provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS[
                                id(material.composition_authority)
                            ]
                        )
                    review_source, bundle = candidate.semantic_children
                    self.assertEqual(bundle.branch, "STOP_UNVERIFIED")
                    self.assertEqual(material.report.outcome, "STOP UNVERIFIED")
                    self.assertEqual(journal.count("close_recommendations"), 0)
                    validate = (
                        provider_workflows_module._is_issued_close_position_authority_bundle
                    )
                    self.assertTrue(
                        validate(bundle, review_source=review_source)
                    )
                    self.assertFalse(
                        validate(replace(bundle), review_source=review_source)
                    )
                    children = bundle.identity_children
                    action = children[10]
                    try:
                        object.__setattr__(
                            bundle,
                            "identity_children",
                            (*children[:10], replace(action)),
                        )
                        object.__setattr__(
                            bundle,
                            "source_digest",
                            provider_workflows_module._close_position_authority_digest(
                                projection=bundle.projection,
                                branch=bundle.branch,
                                identity_children=bundle.identity_children,
                            ),
                        )
                        self.assertFalse(
                            validate(bundle, review_source=review_source)
                        )
                    finally:
                        object.__setattr__(bundle, "identity_children", children)
                        object.__setattr__(
                            bundle,
                            "source_digest",
                            provider_workflows_module._close_position_authority_digest(
                                projection=bundle.projection,
                                branch=bundle.branch,
                                identity_children=children,
                            ),
                        )
                    self.assertTrue(
                        validate(bundle, review_source=review_source)
                    )

    def test_later_stage_unverified_authorities_rederive_the_exact_failure(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")
        original_event_authority = (
            risk_module._issue_phase1_signal_evidence_authority_from_source
        )
        event_authority_calls = 0

        def event_authority_after_collection(*args, **kwargs):
            nonlocal event_authority_calls
            event_authority_calls += 1
            if event_authority_calls == 1:
                return original_event_authority(*args, **kwargs)
            raise RiskBlock("forced event-authority failure")

        cases = (
            (
                "_issue_phase1_signal_evidence_authority_from_source",
                event_authority_after_collection,
                "EVENT_AUTHORITY_UNAVAILABLE",
            ),
            (
                "issue_actual_close_mark",
                RiskBlock("forced close-mark failure"),
                "CLOSE_MARK_UNVERIFIED",
            ),
            (
                "evaluate_position",
                RiskBlock("forced position-evaluation failure"),
                "POSITION_EVALUATION_UNVERIFIED",
            ),
        )

        for attribute, effect, expected_branch in cases:
            with self.subTest(stage=attribute):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    root = Path(temporary_directory)
                    with Journal.open(root / "journal.sqlite3") as journal:
                        signal_source, _ready_at = (
                            self._seed_linked_open_position(journal)
                        )
                        coordinator = (
                            provider_workflows_module.ActualCloseWorkflowCoordinator(
                                journal=journal,
                                report_archive_root=root,
                                source_collector=self._fixture_close_collector(
                                    signal_source,
                                ),
                                calendar_resolver=_calendar(),
                                policy=policy_fixture(),
                                plans=journal.phase1_signal_plan_resolver(),
                            )
                        )
                        with mock.patch.object(
                            risk_module,
                            attribute,
                            side_effect=effect,
                        ):
                            material = coordinator.close_material(
                                session_date,
                                review_at=review_at,
                                retrieved_at=retrieved_at,
                            )
                            with provider_workflows_module._CLOSE_COMPOSITION_LOCK:
                                candidate = provider_workflows_module._ISSUED_CLOSE_COMPOSITIONS[
                                    id(material.composition_authority)
                                ]
                            review_source, bundle = candidate.semantic_children
                            self.assertEqual(bundle.branch, expected_branch)
                            self.assertTrue(
                                provider_workflows_module._is_issued_close_position_authority_bundle(
                                    bundle,
                                    review_source=review_source,
                                )
                            )
                            if (
                                expected_branch
                                == "POSITION_EVALUATION_UNVERIFIED"
                            ):
                                with mock.patch.object(
                                    risk_module,
                                    "issue_actual_close_mark",
                                    side_effect=RiskBlock(
                                        "forced prerequisite failure"
                                    ),
                                ):
                                    self.assertFalse(
                                        provider_workflows_module._is_issued_close_position_authority_bundle(
                                            bundle,
                                            review_source=review_source,
                                        )
                                    )
                        self.assertEqual(
                            material.report.outcome,
                            "POSITION UNVERIFIED",
                        )
                        self.assertEqual(
                            journal.count("close_recommendations"),
                            0,
                        )

    def test_coordinator_keeps_reconciliation_dominant_when_sip_fails(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _position_ready_at = (
                    self._seed_linked_open_position(journal)
                )
                ingest_confirmation(
                    journal,
                    ConfirmationEnvelope(
                        message_id="actual-close:pending-orders",
                        message_time=aware_et(session_date, "16:25"),
                        received_at=(
                            aware_et(session_date, "16:25")
                            + timedelta(seconds=1)
                        ),
                        text=(
                            "ACCOUNT CHECK settled_cash 5000 pending_orders 1 "
                            "unlogged_positions 0 AT 16:25 ET"
                        ),
                        session_date=session_date,
                    ),
                    plans=UnavailableSignalPlanResolver(),
                    calendar=_calendar(),
                    policy=policy_fixture(),
                    entry_authorities=UnavailableActualEntryAuthorityResolver(),
                )
                current_signal_source = journal._read_phase1_signal_source(
                    signal_source.signal_id,
                    query_cutoff=review_at,
                )
                collector = self._fixture_close_collector(
                    current_signal_source,
                    include_sip=False,
                    include_iex=True,
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                material = coordinator.close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=retrieved_at,
                )

                self.assertEqual(collector.symbols, [(_SYMBOL,)])
                self.assertEqual(
                    material.report.outcome,
                    "RECONCILIATION REQUIRED",
                )
                self.assertEqual(len(material.actual_state.positions), 1)
                self.assertEqual(len(material.positions), 1)
                projection = material.positions[0]
                self.assertEqual(
                    projection.status,
                    "RECONCILIATION_REQUIRED",
                )
                self.assertIn(
                    "PENDING_ORDERS_PRESENT",
                    material.report.body,
                )
                self.assertIn(
                    "SIP_MARK_UNAVAILABLE",
                    material.report.body,
                )
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_actual_ledger_mutation_during_collection_fails_closed(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")

        def add_unlinked_position(journal: Journal) -> None:
            ingest_confirmation(
                journal,
                ConfirmationEnvelope(
                    message_id="actual-close:concurrent-unlinked-position",
                    message_time=aware_et(session_date, "16:25"),
                    received_at=(
                        aware_et(session_date, "16:25")
                        + timedelta(seconds=1)
                    ),
                    text=(
                        "BOUGHT QQQ 2 shares @ 100 AT 15:20 ET; "
                        "BID 99.99 ASK 100; STOP SET @ 95"
                    ),
                    session_date=session_date,
                ),
                plans=UnavailableSignalPlanResolver(),
                calendar=_calendar(),
                policy=policy_fixture(),
                entry_authorities=UnavailableActualEntryAuthorityResolver(),
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_source, _position_ready_at = (
                    self._seed_linked_open_position(journal)
                )
                collector = self._fixture_close_collector(
                    signal_source,
                    during_collection=add_unlinked_position,
                    sip_bid="19.90",
                    sip_ask="19.91",
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                with self.assertRaisesRegex(
                    provider_workflows_module.CanonicalMaterialError,
                    "ledger changed during provider collection",
                ):
                    coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=retrieved_at,
                    )

                self.assertEqual(collector.symbols, [(_SYMBOL,)])
                self.assertEqual(journal.count("close_recommendations"), 0)
                _terminal_source, terminal_state = self._actual_snapshot(
                    journal,
                    retrieved_at,
                )
                self.assertEqual(
                    tuple(
                        position.symbol
                        for position in terminal_state.positions
                    ),
                    (_SYMBOL, "QQQ"),
                )

    def test_plan_binding_mutation_during_collection_fails_closed(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        command_started_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                _publish(journal)
                signal_source = (
                    journal._read_phase1_canonical_replay_source(
                        query_cutoff=aware_et(session_date, "08:45"),
                    ).signal_sources[0]
                )
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(
                        journal,
                        signal_source,
                    )
                )
                action_source, recorded_at = self._confirmation_action_source_at(
                    journal,
                    signal_source,
                    event_clock="09:37",
                    after=completed_at,
                )

                def bind_existing_actual_position(owner: Journal) -> None:
                    owner.record_phase1_entry(
                        signal_source.signal_id,
                        confirmation_action_source=action_source,
                        trigger_observation_id=trigger_id,
                        quote_observation_id=quote_id,
                        calendar_resolver=_calendar(),
                        recorded_at=recorded_at,
                    )

                collector = self._fixture_close_collector(
                    signal_source,
                    before_collection_writes=bind_existing_actual_position,
                )
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )

                with self.assertRaisesRegex(
                    provider_workflows_module.CanonicalMaterialError,
                    "plan binding changed during provider collection",
                ):
                    coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=command_started_at,
                    )

                self.assertEqual(journal.count("actual_position_plan_bindings"), 1)
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_two_position_recommendations_are_atomic_and_retryable(
        self,
    ) -> None:
        session_date = date(2026, 8, 17)
        review_at = aware_et(session_date, "15:30")
        command_started_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                signal_sources, _ready_at = self._seed_two_linked_open_positions(
                    journal
                )
                collector = self._two_position_close_collector(signal_sources)
                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=collector,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )
                original_sql = journal_module._sql
                inserts = 0

                def fail_second_recommendation_insert(
                    connection,
                    statement,
                    parameters=(),
                ):
                    nonlocal inserts
                    if statement.startswith(
                        "INSERT INTO close_recommendations("
                    ):
                        inserts += 1
                        if inserts == 2:
                            raise sqlite3.OperationalError(
                                "injected second recommendation failure"
                            )
                    return original_sql(connection, statement, parameters)

                with mock.patch.object(
                    journal_module,
                    "_sql",
                    side_effect=fail_second_recommendation_insert,
                ), self.assertRaisesRegex(
                    provider_workflows_module.CanonicalMaterialError,
                    "recommendation could not be persisted",
                ):
                    coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=command_started_at,
                    )

                self.assertEqual(inserts, 2)
                self.assertEqual(journal.count("close_recommendations"), 0)
                self.assertIsNotNone(collector.collection)

                class ExistingCollection:
                    def collect_close_sources(
                        collector_self,
                        **values,
                    ):
                        del collector_self, values
                        return collector.collection

                retry = provider_workflows_module.ActualCloseWorkflowCoordinator(
                    journal=journal,
                    report_archive_root=root,
                    source_collector=ExistingCollection(),
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                    plans=journal.phase1_signal_plan_resolver(),
                ).close_material(
                    session_date,
                    review_at=review_at,
                    retrieved_at=command_started_at,
                )
                self.assertEqual(
                    tuple(position.symbol for position in retry.positions),
                    (_SYMBOL, "QQQ"),
                )
                self.assertEqual(journal.count("close_recommendations"), 2)

    def test_collection_write_then_error_forces_a_final_actual_replay(
        self,
    ) -> None:
        session_date = date(2026, 8, 14)
        review_at = aware_et(session_date, "15:30")
        retrieved_at = aware_et(session_date, "16:30")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "journal.sqlite3") as journal:
                self._seed_linked_open_position(journal)
                source_count = journal.count("source_observations")

                class FailingCollector:
                    def collect_close_sources(
                        collector_self,
                        *,
                        journal: Journal,
                        symbols: tuple[str, ...],
                        session_date: date,
                        review_at: datetime,
                        mark_cutoff: datetime,
                        command_started_at: datetime,
                    ):
                        del (
                            collector_self,
                            symbols,
                            session_date,
                            mark_cutoff,
                            command_started_at,
                        )
                        journal.append_source_observation(
                            payload=b'{"partial":true}',
                            source_uri="https://example.invalid/partial-close",
                            source_type="TEST",
                            provider="fixture",
                            feed=None,
                            source_time=review_at,
                            retrieved_at=review_at,
                            provider_sequence=None,
                            delay_seconds=0,
                            health_result="OK",
                            details={"version": 1},
                        )
                        raise RuntimeError("provider transport failed")

                coordinator = (
                    provider_workflows_module.ActualCloseWorkflowCoordinator(
                        journal=journal,
                        report_archive_root=root,
                        source_collector=FailingCollector(),
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        plans=journal.phase1_signal_plan_resolver(),
                    )
                )
                with mock.patch(
                    "stock_monitor.reconciliation.replay_actual",
                    wraps=replay_actual,
                ) as replayed, self.assertRaisesRegex(
                    provider_workflows_module.CanonicalMaterialError,
                    "collection",
                ):
                    coordinator.close_material(
                        session_date,
                        review_at=review_at,
                        retrieved_at=retrieved_at,
                    )

                self.assertEqual(replayed.call_count, 2)
                self.assertEqual(
                    journal.count("source_observations"),
                    source_count + 1,
                )

    def test_issues_delayed_sip_mark_with_distinct_final_query_cutoff(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                source, plan_source, event_evidence, history_source = (
                    self._context_inputs(
                        journal,
                        signal_source,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                        iex_bid="999.00",
                        iex_ask="999.01",
                    )
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(source, context)

                self.assertEqual(source.review_at, review_at)
                self.assertEqual(source.query_cutoff, query_cutoff)
                self.assertEqual(source.observed_at, review_at - timedelta(minutes=16))
                self.assertEqual(mark.at, review_at)
                self.assertEqual(mark.price, Decimal("20.25"))
                self.assertEqual(context.position.entry, Decimal("20.43"))
                self.assertTrue(is_issued_actual_close_market_source(source))
                self.assertTrue(is_issued_actual_position_event_context(context))
                self.assertTrue(is_issued_market_mark(mark))

    def test_issued_close_decision_is_the_only_recommendation_write_boundary(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                source, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    source,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(source, context)

                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )
                self.assertEqual(decision.action, "HOLD")
                self.assertEqual(decision.reason_codes, ("PLAN_UNCHANGED",))
                self.assertTrue(
                    risk_module.is_issued_actual_close_decision_source(
                        decision
                    )
                )

                recommendation = journal.append_close_recommendation(decision)
                self.assertEqual(recommendation.action, "HOLD")
                self.assertEqual(
                    recommendation.recommended_stop_micros,
                    signal_source.recommended_stop_micros,
                )
                self.assertFalse(
                    risk_module.is_issued_actual_close_decision_source(
                        decision
                    )
                )
                self.assertFalse(is_issued_actual_close_market_source(source))
                self.assertFalse(
                    is_issued_actual_position_event_context(context)
                )
                self.assertFalse(is_issued_market_mark(mark))
                self.assertFalse(
                    risk_module.is_verified_latest_close_recommendation_source(
                        history
                    )
                )
                with self.assertRaises(TypeError):
                    journal.append_close_recommendation(
                        review_source=source.review_source,
                        position_plan_source=plan,
                        recommended_stop_micros=(
                            signal_source.recommended_stop_micros
                        ),
                        action="HOLD",
                        reason_codes=("PLAN_UNCHANGED",),
                        received_at=source.review_source.retrieved_at,
                    )

    def test_context_requires_the_reviews_exact_event_evidence_rows(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                self._persist_clear_event_evidence(
                    journal,
                    signal_source,
                    review_at,
                )
                market_receipts = self._append_market_receipts(
                    journal,
                    self._market_specs(review_at=review_at),
                )
                evidence = self._final_event_evidence(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                evidence_source = risk_module._phase1_bound_sources(
                    evidence
                )[0][0]
                self.assertGreater(
                    len(evidence_source.source_observation_row_ids),
                    1,
                )
                review = self._append_review_for_receipts(
                    journal,
                    receipts=market_receipts,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                    event_evidence=evidence,
                    event_evidence_row_ids=(
                        evidence_source.source_observation_row_ids[0],
                    ),
                )
                plan = self._final_plan(journal, query_cutoff)
                evidence = self._final_event_evidence(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                history = journal.read_latest_close_recommendation_source(
                    position_plan_source=plan,
                    query_cutoff=query_cutoff,
                )
                market = issue_actual_close_market_source(
                    review,
                    plan,
                    calendar_resolver=_calendar(),
                )

                with self.assertRaisesRegex(
                    RiskBlock,
                    "^ACTUAL_POSITION_EVENT_EVIDENCE_REVIEW_MISMATCH$",
                ):
                    issue_actual_position_event_context(
                        market,
                        plan,
                        evidence,
                        history,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

    def test_same_review_restart_replays_the_exact_durable_decision(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.sqlite3"
            with Journal.open(path) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                    sip_bid="21.00",
                    sip_ask="21.02",
                    iex_bid="20.99",
                    iex_ask="21.03",
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )
                self.assertEqual(decision.action, "TIGHTEN_STOP")
                stored = journal.append_close_recommendation(decision)
                review_id = stored.review_id
                recommendation_id = stored.recommendation_id
                recommendation_count = journal.count("close_recommendations")

            with Journal.open(path) as journal:
                review = journal.read_actual_close_review_source(
                    review_id,
                    query_cutoff=query_cutoff,
                )
                plan = self._final_plan(journal, query_cutoff)
                evidence = self._final_event_evidence(
                    journal,
                    plan.signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                history = journal.read_latest_close_recommendation_source(
                    position_plan_source=plan,
                    query_cutoff=query_cutoff,
                )
                market = issue_actual_close_market_source(
                    review,
                    plan,
                    calendar_resolver=_calendar(),
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                retry_decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )

                self.assertEqual(
                    retry_decision.position_action.status,
                    "RECONCILIATION_REQUIRED",
                )
                self.assertEqual(retry_decision.action, "TIGHTEN_STOP")
                retry = journal.append_close_recommendation(retry_decision)
                self.assertEqual(retry.recommendation_id, recommendation_id)
                self.assertEqual(
                    journal.count("close_recommendations"),
                    recommendation_count,
                )

    def test_decision_rejects_a_review_retrieved_after_its_query_cutoff(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                    review_retrieved_at=(
                        query_cutoff + timedelta(microseconds=1)
                    ),
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)

                with self.assertRaisesRegex(
                    RiskBlock,
                    "^ACTUAL_CLOSE_DECISION_REVIEW_NOT_SETTLED$",
                ):
                    risk_module.issue_actual_close_decision(
                        context,
                        mark,
                        policy,
                    )

    def test_append_rechecks_decision_after_sqlite_trace_callbacks(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )
                callback_fired = False

                def mutate_before_insert(statement: str) -> None:
                    nonlocal callback_fired
                    if (
                        not callback_fired
                        and "FROM close_recommendations WHERE session_date"
                        in statement
                    ):
                        callback_fired = True
                        object.__setattr__(decision, "action", "EXIT")

                journal._connection.set_trace_callback(mutate_before_insert)
                try:
                    with self.assertRaisesRegex(
                        InvalidJournalValue,
                        "changed during validation$",
                    ):
                        journal.append_close_recommendation(decision)
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(callback_fired)
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_append_rechecks_decision_after_insert_trace_callback(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )
                callback_fired = False

                def mutate_during_insert(statement: str) -> None:
                    nonlocal callback_fired
                    if (
                        not callback_fired
                        and statement.startswith(
                            "INSERT INTO close_recommendations"
                        )
                    ):
                        callback_fired = True
                        object.__setattr__(decision, "action", "EXIT")

                journal._connection.set_trace_callback(mutate_during_insert)
                try:
                    with self.assertRaisesRegex(
                        InvalidJournalValue,
                        "changed during write$",
                    ):
                        journal.append_close_recommendation(decision)
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(callback_fired)
                self.assertFalse(
                    risk_module.is_issued_actual_close_decision_source(decision)
                )
                self.assertEqual(journal.count("close_recommendations"), 0)

    def test_append_clears_trace_callback_before_commit(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )
                insert_seen = False
                commit_seen = False

                def mutate_during_commit(statement: str) -> None:
                    nonlocal insert_seen, commit_seen
                    if statement.startswith("INSERT INTO close_recommendations"):
                        insert_seen = True
                    if statement == "COMMIT":
                        commit_seen = True
                        object.__setattr__(decision, "action", "EXIT")

                journal._connection.set_trace_callback(mutate_during_commit)
                try:
                    stored = journal.append_close_recommendation(decision)
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(insert_seen)
                self.assertFalse(commit_seen)
                self.assertEqual(decision.action, "HOLD")
                self.assertEqual(stored.action, "HOLD")
                self.assertTrue(
                    journal.is_current_close_recommendation_source(stored)
                )
                self.assertEqual(journal.count("close_recommendations"), 1)

    def test_latest_iex_terminal_shapes_and_receipt_order_are_canonical(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        query_cutoff = review_at + timedelta(minutes=1)
        for latest_omits_next_page_token in (False, True):
            with self.subTest(
                latest_omits_next_page_token=latest_omits_next_page_token
            ):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        receipts = self._append_market_receipts(
                            journal,
                            self._market_specs(
                                review_at=review_at,
                                multipage_sip=True,
                                latest_omits_next_page_token=(
                                    latest_omits_next_page_token
                                ),
                            ),
                        )
                        ordered = self._issue_market_from_receipts(
                            journal,
                            receipts=receipts,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                        )
                        permuted = self._issue_market_from_receipts(
                            journal,
                            receipts=tuple(reversed(receipts)),
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                        )

                        self.assertEqual(permuted.source_digest, ordered.source_digest)
                        self.assertEqual(
                            tuple(
                                receipt.row_id
                                for receipt in permuted.observation_receipts
                            ),
                            tuple(
                                receipt.row_id
                                for receipt in ordered.observation_receipts
                            ),
                        )
                        self.assertTrue(
                            is_issued_actual_close_market_source(permuted)
                        )

    def test_normal_and_early_close_use_inclusive_delayed_sip_boundaries(self) -> None:
        cases = (
            (date(2026, 8, 14), "15:30"),
            (date(2026, 11, 27), "12:30"),
        )
        for session_day, clock in cases:
            for offset in (timedelta(minutes=-5), timedelta()):
                with self.subTest(session_day=session_day, offset=offset):
                    review_at = aware_et(session_day, clock)
                    with tempfile.TemporaryDirectory() as temporary_directory:
                        with Journal.open(
                            Path(temporary_directory) / "journal.sqlite3"
                        ) as journal:
                            source, _receipts = self._issue_source(
                                journal,
                                review_at=review_at,
                                query_cutoff=review_at + timedelta(minutes=1),
                                quote_offsets=(offset,),
                            )
                            self.assertEqual(
                                source.observed_at,
                                source.cutoff + offset,
                            )
                            self.assertEqual(
                                source.cutoff,
                                review_at - timedelta(minutes=16),
                            )
                            self.assertTrue(
                                is_issued_actual_close_market_source(source)
                            )

    def test_sip_or_iex_invalidity_fails_closed_without_price_fallback(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        invalid_cases = (
            {"include_daily": False},
            {"include_intraday": False},
            {"include_sip": False},
            {"sip_bid": "0", "sip_ask": "20.27"},
            {"sip_bid": "20.28", "sip_ask": "20.27"},
            {"quote_offsets": (timedelta(microseconds=1),)},
            {"quote_offsets": (timedelta(minutes=-5, microseconds=-1),)},
            {"quote_offsets": (timedelta(), timedelta())},
            {"include_iex": False},
            {"iex_offset": timedelta(minutes=-5, microseconds=-1)},
            {
                "iex_offset": timedelta(microseconds=1),
                "retrieved_offset": timedelta(seconds=1),
            },
            {"iex_bid": "20.28", "iex_ask": "20.27"},
        )
        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        receipts = self._append_market_receipts(
                            journal,
                            self._market_specs(
                                review_at=review_at,
                                **overrides,
                            ),
                        )
                        with self.assertRaises((RiskBlock, InvalidJournalValue)):
                            self._issue_market_from_receipts(
                                journal,
                                receipts=receipts,
                                review_at=review_at,
                                query_cutoff=review_at + timedelta(minutes=1),
                            )

    def test_terminal_multipage_sip_cohort_selects_latest_unique_bid(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                source, receipts = self._issue_source(
                    journal,
                    review_at=review_at,
                    query_cutoff=review_at + timedelta(minutes=2),
                    multipage_sip=True,
                )

                self.assertEqual(len(receipts), 5)
                self.assertEqual(source.observed_at, source.cutoff)
                self.assertEqual(source.sip_bid, Decimal("20.25"))
                self.assertTrue(is_issued_actual_close_market_source(source))

    def test_query_cutoff_and_economic_source_lookahead_fail_closed(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        cases = (
            (
                review_at - timedelta(microseconds=1),
                {},
            ),
            (
                review_at + timedelta(seconds=30),
                {"quote_offsets": (timedelta(seconds=1),)},
            ),
        )
        for query_cutoff, overrides in cases:
            with self.subTest(query_cutoff=query_cutoff, overrides=overrides):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with Journal.open(
                        Path(temporary_directory) / "journal.sqlite3"
                    ) as journal:
                        receipts = self._append_market_receipts(
                            journal,
                            self._market_specs(
                                review_at=review_at,
                                **overrides,
                            ),
                        )
                        with self.assertRaises((RiskBlock, InvalidJournalValue)):
                            self._issue_market_from_receipts(
                                journal,
                                receipts=receipts,
                                review_at=review_at,
                                query_cutoff=query_cutoff,
                            )

    def test_actual_close_review_rejects_receipt_after_terminal_collection(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        query_cutoff = aware_et(date(2026, 8, 14), "15:38")
        terminal_at = aware_et(date(2026, 8, 14), "15:39")
        after_terminal = terminal_at + timedelta(microseconds=1)

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                global_receipts = self._append_market_receipts(
                    journal,
                    self._global_close_specs(
                        review_at,
                        retrieved_at=after_terminal,
                        timestamp_source="UNAVAILABLE",
                    ),
                )

                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "chronology",
                ):
                    self._append_review_for_receipts(
                        journal,
                        receipts=(),
                        global_receipts=global_receipts,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                        retrieved_at=terminal_at,
                    )

    def test_wrong_provider_query_receipt_feed_or_health_fails_closed(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        for mode in (
            "PROVIDER",
            "QUERY",
            "REQUEST_LOOKAHEAD",
            "RECEIPT_FEED",
            "HEALTH",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    terminal_at = None
                    query_cutoff = review_at + timedelta(minutes=1)
                    specs = list(
                        self._market_specs(
                            review_at=review_at,
                            **(
                                {
                                    "retrieved_offset": timedelta(minutes=62),
                                    "iex_offset": timedelta(minutes=61),
                                }
                                if mode == "REQUEST_LOOKAHEAD"
                                else {}
                            ),
                        )
                    )
                    index = next(
                        ordinal
                        for ordinal, spec in enumerate(specs)
                        if spec["source_type"]
                        == (
                            "ALPACA_HISTORICAL_QUOTES"
                            if mode in {"QUERY", "REQUEST_LOOKAHEAD", "HEALTH"}
                            else "ALPACA_DAILY_BARS"
                        )
                    )
                    if mode == "PROVIDER":
                        specs[index] = self._rewrite_market_spec(
                            specs[index],
                            provider="not-alpaca",
                        )
                    elif mode == "QUERY":
                        specs[index] = self._rewrite_market_spec(
                            specs[index],
                            source_uri=str(specs[index]["source_uri"]).replace(
                                "feed=sip",
                                "feed=iex",
                            ),
                        )
                    elif mode == "REQUEST_LOOKAHEAD":
                        query_cutoff = review_at + timedelta(minutes=61)
                        terminal_at = review_at + timedelta(minutes=62)
                        mark_cutoff = review_at - timedelta(minutes=16)
                        exact_end = urlencode(
                            (("end", _utc_text(mark_cutoff)),)
                        )
                        lookahead_end = urlencode(
                            (
                                (
                                    "end",
                                    _utc_text(
                                        mark_cutoff
                                        + timedelta(microseconds=1)
                                    ),
                                ),
                            )
                        )
                        specs[index] = self._rewrite_market_spec(
                            specs[index],
                            source_uri=str(specs[index]["source_uri"]).replace(
                                exact_end,
                                lookahead_end,
                            ),
                        )
                    elif mode == "RECEIPT_FEED":
                        specs[index] = self._rewrite_market_spec(
                            specs[index],
                            feed="iex",
                        )
                    else:
                        specs[index] = self._rewrite_market_spec(
                            specs[index],
                            health_result="DEGRADED",
                        )
                    receipts = self._append_market_receipts(
                        journal,
                        tuple(specs),
                    )

                    with self.assertRaises((RiskBlock, InvalidJournalValue)):
                        self._issue_market_from_receipts(
                            journal,
                            receipts=receipts,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                            review_retrieved_at=terminal_at,
                        )

    def test_sip_pages_require_release_delay_and_retrieval_chronology(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        for source_type in (
            "ALPACA_DAILY_BARS",
            "ALPACA_INTRADAY_BARS",
            "ALPACA_HISTORICAL_QUOTES",
        ):
            with self.subTest(source_type=source_type), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "early.sqlite3") as journal:
                    specs = list(self._market_specs(review_at=review_at))
                    index = next(
                        ordinal
                        for ordinal, spec in enumerate(specs)
                        if spec["source_type"] == source_type
                    )
                    specs[index] = self._rewrite_market_spec(
                        specs[index],
                        retrieved_at=review_at - timedelta(microseconds=1),
                    )
                    receipts = self._append_market_receipts(
                        journal,
                        tuple(specs),
                    )
                    with self.assertRaises((RiskBlock, InvalidJournalValue)):
                        self._issue_market_from_receipts(
                            journal,
                            receipts=receipts,
                            review_at=review_at,
                            query_cutoff=review_at + timedelta(minutes=1),
                        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "order.sqlite3") as journal:
                specs = list(
                    self._market_specs(
                        review_at=review_at,
                        multipage_sip=True,
                    )
                )
                quote_indexes = tuple(
                    index
                    for index, spec in enumerate(specs)
                    if spec["source_type"] == "ALPACA_HISTORICAL_QUOTES"
                )
                self.assertEqual(len(quote_indexes), 2)
                specs[quote_indexes[0]] = self._rewrite_market_spec(
                    specs[quote_indexes[0]],
                    retrieved_at=review_at + timedelta(minutes=2),
                )
                specs[quote_indexes[1]] = self._rewrite_market_spec(
                    specs[quote_indexes[1]],
                    retrieved_at=review_at + timedelta(minutes=1),
                )
                receipts = self._append_market_receipts(journal, tuple(specs))

                with self.assertRaises((RiskBlock, InvalidJournalValue)):
                    self._issue_market_from_receipts(
                        journal,
                        receipts=receipts,
                        review_at=review_at,
                        query_cutoff=review_at + timedelta(minutes=3),
                    )

    def test_payload_feed_daily_volume_and_indicator_failures_fail_closed(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        invalid_overrides = (
            {
                "payload_feed_by_type": {
                    "ALPACA_DAILY_BARS": "iex",
                }
            },
            {
                "payload_feed_by_type": {
                    "ALPACA_INTRADAY_BARS": "iex",
                }
            },
            {
                "payload_feed_by_type": {
                    "ALPACA_HISTORICAL_QUOTES": "iex",
                }
            },
            {
                "payload_feed_by_type": {
                    "ALPACA_LATEST_QUOTES": "sip",
                }
            },
            {"daily_volume": 0},
            {"daily_bar_count": 13},
            {"daily_bar_count": 15},
        )
        for overrides in invalid_overrides:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    receipts = self._append_market_receipts(
                        journal,
                        self._market_specs(
                            review_at=review_at,
                            **overrides,
                        ),
                    )
                    with self.assertRaises((RiskBlock, InvalidJournalValue)):
                        self._issue_market_from_receipts(
                            journal,
                            receipts=receipts,
                            review_at=review_at,
                            query_cutoff=review_at + timedelta(minutes=1),
                        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "indicator.sqlite3"
            ) as journal:
                receipts = self._append_market_receipts(
                    journal,
                    self._market_specs(review_at=review_at),
                )
                with mock.patch(
                    "stock_monitor.indicators.wilder_atr",
                    side_effect=IndicatorError("forced indicator failure"),
                ), self.assertRaisesRegex(
                    RiskBlock,
                    "^ACTUAL_CLOSE_INDICATOR_INVALID$",
                ):
                    self._issue_market_from_receipts(
                        journal,
                        receipts=receipts,
                        review_at=review_at,
                        query_cutoff=review_at + timedelta(minutes=1),
                    )

    def test_zero_volume_intraday_bars_fail_closed_across_page_boundaries(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        cases = (
            ("single-first", (0, 45_000), False),
            ("single-terminal", (50_000, 0), False),
            ("paged-first", (0, 45_000), True),
            ("paged-terminal", (50_000, 0), True),
        )
        for name, intraday_volumes, multipage_intraday in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    receipts = self._append_market_receipts(
                        journal,
                        self._market_specs(
                            review_at=review_at,
                            intraday_volumes=intraday_volumes,
                            multipage_intraday=multipage_intraday,
                        ),
                    )

                    with self.assertRaisesRegex(
                        RiskBlock,
                        "^ACTUAL_CLOSE_INTRADAY_HISTORY_INCOMPLETE$",
                    ):
                        self._issue_market_from_receipts(
                            journal,
                            receipts=receipts,
                            review_at=review_at,
                            query_cutoff=review_at + timedelta(minutes=1),
                        )

    def test_copies_wrong_cutoff_and_later_write_fail_closed(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                source, plan_source, event_evidence, history_source = (
                    self._context_inputs(
                        journal,
                        signal_source,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                    )
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(source, context)
                decision = risk_module.issue_actual_close_decision(
                    context,
                    mark,
                    policy,
                )

                self.assertFalse(
                    is_issued_actual_close_market_source(replace(source))
                )
                self.assertFalse(
                    risk_module.is_issued_actual_close_decision_source(
                        replace(decision)
                    )
                )
                self.assertFalse(
                    risk_module.is_verified_latest_close_recommendation_source(
                        replace(history_source)
                    )
                )
                with tempfile.TemporaryDirectory() as second_directory:
                    with Journal.open(
                        Path(second_directory) / "journal.sqlite3"
                    ) as second:
                        with self.assertRaises(InvalidJournalValue):
                            second.append_close_recommendation(decision)
                with self.assertRaises(RiskBlock):
                    issue_actual_position_event_context(
                        source,
                        replace(plan_source),
                        event_evidence,
                        history_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                with self.assertRaises(RiskBlock):
                    issue_actual_position_event_context(
                        source,
                        plan_source,
                        replace(event_evidence),
                        history_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

                with self.assertRaises(TypeError):
                    issue_actual_position_event_context(
                        source,
                        plan_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                        event_exit_required=False,
                        thesis_invalidated=False,
                    )

                wrong_cutoff_evidence = self._final_event_evidence(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff - timedelta(microseconds=1),
                )
                with self.assertRaises(RiskBlock):
                    issue_actual_position_event_context(
                        source,
                        plan_source,
                        wrong_cutoff_evidence,
                        history_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )

                journal.append_source_observation(
                    payload=b'{"later":true}',
                    source_uri="https://example.invalid/later",
                    source_type="TEST",
                    provider="fixture",
                    feed=None,
                    source_time=review_at,
                    retrieved_at=review_at,
                    provider_sequence=None,
                    delay_seconds=0,
                    health_result="OK",
                    details={"version": 1},
                )
                self.assertFalse(is_issued_actual_close_market_source(source))
                self.assertFalse(is_issued_actual_position_event_context(context))
                self.assertFalse(is_issued_market_mark(mark))
                self.assertFalse(
                    risk_module.is_issued_actual_close_decision_source(
                        decision
                    )
                )
                self.assertFalse(
                    risk_module.is_verified_latest_close_recommendation_source(
                        history_source
                    )
                )

    def test_callback_mutation_cannot_issue_an_actual_position_context(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market_source, plan_source, event_evidence, history_source = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                original_verifier = (
                    risk_module.is_verified_actual_position_plan_source
                )
                callback_fired = False

                def mutate_after_plan_callback(source: object) -> bool:
                    nonlocal callback_fired
                    verified = original_verifier(source)
                    if not callback_fired:
                        callback_fired = True
                        object.__setattr__(
                            market_source,
                            "sip_bid",
                            market_source.sip_bid + Decimal("0.01"),
                        )
                    return verified

                with mock.patch.object(
                    risk_module,
                    "is_verified_actual_position_plan_source",
                    side_effect=mutate_after_plan_callback,
                ), self.assertRaises(RiskBlock):
                    issue_actual_position_event_context(
                        market_source,
                        plan_source,
                        event_evidence,
                        history_source,
                        calendar_resolver=_calendar(),
                        policy=policy_fixture(),
                    )
                self.assertTrue(callback_fired)

    def test_future_lot_mutation_unverifies_current_position_context(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market_source, plan_source, event_evidence, history_source = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                context = issue_actual_position_event_context(
                    market_source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertTrue(is_issued_actual_position_event_context(context))
                actual_position = next(
                    position
                    for position in plan_source.actual_position_state.positions
                    if position.symbol == _SYMBOL
                )

                object.__setattr__(
                    actual_position.lots[0],
                    "acquired_at",
                    review_at + timedelta(microseconds=1),
                )

                self.assertFalse(is_issued_actual_position_event_context(context))

    def test_cross_owner_and_wrong_signal_evidence_fail_closed(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with Journal.open(root / "position.sqlite3") as position_journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    position_journal
                )
                own_source, plan_source, event_evidence, history_source = (
                    self._context_inputs(
                        position_journal,
                        signal_source,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                    )
                )

                with Journal.open(root / "market.sqlite3") as market_journal:
                    cross_owner_source, _receipts = self._issue_source(
                        market_journal,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                    )
                    with self.assertRaises(RiskBlock):
                        issue_actual_position_event_context(
                            cross_owner_source,
                            plan_source,
                            event_evidence,
                            history_source,
                            calendar_resolver=_calendar(),
                            policy=policy_fixture(),
                        )

                with Journal.open(root / "wrong-signal.sqlite3") as wrong_journal:
                    qqq_candidate = next(
                        candidate
                        for candidate in _issued_candidates(2)
                        if candidate.symbol == "QQQ"
                    )
                    _publish(wrong_journal, candidates_override=(qqq_candidate,))
                    wrong_signal_source = (
                        wrong_journal._read_phase1_canonical_replay_source(
                            query_cutoff=aware_et(
                                date(2026, 8, 14),
                                "08:45",
                            ),
                        ).signal_sources[0]
                    )
                    self._persist_clear_event_evidence(
                        wrong_journal,
                        wrong_signal_source,
                        review_at,
                    )
                    wrong_signal_evidence = self._final_event_evidence(
                        wrong_journal,
                        wrong_signal_source,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                    )
                    with self.assertRaises(RiskBlock):
                        issue_actual_position_event_context(
                            own_source,
                            plan_source,
                            wrong_signal_evidence,
                            history_source,
                            calendar_resolver=_calendar(),
                            policy=policy_fixture(),
                        )

    def test_post_review_actual_entry_stop_or_partial_sale_cannot_shape_context(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        for mode in ("ENTRY", "STOP", "PARTIAL_SALE"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    signal_source, query_cutoff = self._seed_linked_open_position(
                        journal,
                        entry_clock=("15:31" if mode == "ENTRY" else "09:37"),
                    )
                    if mode != "ENTRY":
                        action_at = query_cutoff + timedelta(minutes=1)
                        text = (
                            f"STOP UPDATED {_SYMBOL} @ 20.05 AT 15:31 ET"
                            if mode == "STOP"
                            else f"SOLD {_SYMBOL} 1 shares @ 21 AT 15:31 ET"
                        )
                        ingest_confirmation(
                            journal,
                            ConfirmationEnvelope(
                                message_id=f"post-review-{mode.lower()}",
                                message_time=action_at,
                                received_at=action_at + timedelta(seconds=1),
                                text=text,
                                session_date=action_at.date(),
                            ),
                            plans=UnavailableSignalPlanResolver(),
                            calendar=_calendar(),
                            policy=policy_fixture(),
                            entry_authorities=(
                                UnavailableActualEntryAuthorityResolver()
                            ),
                        )
                        query_cutoff = action_at + timedelta(seconds=2)
                    market_source, plan_source, event_evidence, history_source = (
                        self._context_inputs(
                            journal,
                            signal_source,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                        )
                    )

                    with self.assertRaisesRegex(
                        RiskBlock,
                        "^ACTUAL_POSITION_ECONOMIC_LOOKAHEAD$",
                    ):
                        issue_actual_position_event_context(
                            market_source,
                            plan_source,
                            event_evidence,
                            history_source,
                            calendar_resolver=_calendar(),
                            policy=policy_fixture(),
                        )

    def test_context_uses_full_partial_fill_lineage_after_opener_is_consumed(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        lineage = position_plan_fixture_module.ActualPositionPlanSourceTests()
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                _publish(journal)
                signal = journal._read_phase1_canonical_replay_source(
                    query_cutoff=aware_et(date(2026, 8, 14), "08:45"),
                ).signal_sources[0]
                trigger_id, quote_id, completed_at = (
                    _append_completed_entry_observations(journal, signal)
                )
                parent_order_id = "robinhood:actual-close:partial-chain"
                first_action, first_recorded_at = (
                    lineage._partial_fill_action_source(
                        journal,
                        signal,
                        message_id="actual-close-partial-1",
                        shares=1,
                        parent_order_id=parent_order_id,
                        event_clock="10:14",
                        at=completed_at + timedelta(seconds=1),
                    )
                )
                journal.record_phase1_entry(
                    signal.signal_id,
                    confirmation_action_source=first_action,
                    trigger_observation_id=trigger_id,
                    quote_observation_id=quote_id,
                    calendar_resolver=_calendar(),
                    recorded_at=first_recorded_at,
                )
                second_action, second_recorded_at = (
                    lineage._partial_fill_action_source(
                        journal,
                        signal,
                        message_id="actual-close-partial-2",
                        shares=signal.planned_shares - 1,
                        parent_order_id=parent_order_id,
                        event_clock="10:16",
                        at=first_recorded_at + timedelta(seconds=2),
                    )
                )
                journal.record_phase1_entry(
                    signal.signal_id,
                    confirmation_action_source=second_action,
                    trigger_observation_id=None,
                    quote_observation_id=None,
                    calendar_resolver=_calendar(),
                    recorded_at=second_recorded_at,
                )
                sale_at = second_recorded_at + timedelta(seconds=1)
                lineage._ingest(
                    journal,
                    message_id="actual-close-consume-opener",
                    text=f"SOLD {signal.symbol} 1 shares @ 21 AT 15:00 ET",
                    at=sale_at,
                )
                query_cutoff = sale_at + timedelta(seconds=2)
                current_signal = journal._read_phase1_signal_source(
                    signal.signal_id,
                    query_cutoff=query_cutoff,
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    current_signal,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                positions = tuple(
                    position
                    for position in plan.actual_position_state.positions
                    if position.symbol == signal.symbol
                    and position.shares > 0
                )
                self.assertEqual(len(positions), 1)
                self.assertEqual(
                    tuple(
                        dict.fromkeys(
                            action.signal_id
                            for action in plan.matched_actions
                        )
                    ),
                    (positions[0].signal_id,),
                )

                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )

                self.assertEqual(
                    context.position.shares,
                    signal.planned_shares - 1,
                )
                self.assertEqual(
                    context.position_plan_digest,
                    plan.position_plan_digest,
                )

    def test_unresolved_event_evidence_only_allows_mechanical_full_exit(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        cases = (
            ("HOLD", "20.25", "20.27", "POSITION_UNVERIFIED", 0),
            ("TARGET", "21.50", "21.51", "POSITION_UNVERIFIED", 0),
            (
                "RECOMMENDED_STOP",
                "19.90",
                "19.91",
                "PROVISIONAL_EXIT",
                None,
            ),
        )
        for name, bid, ask, expected_status, expected_exit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                    signal_source, query_cutoff = self._seed_linked_open_position(
                        journal
                    )
                    market_source, plan_source, event_evidence, history_source = (
                        self._context_inputs(
                            journal,
                            signal_source,
                            review_at=review_at,
                            query_cutoff=query_cutoff,
                            binary_event_coverage="UNKNOWN",
                            sip_bid=bid,
                            sip_ask=ask,
                        )
                    )
                    policy = policy_fixture()
                    context = issue_actual_position_event_context(
                        market_source,
                        plan_source,
                        event_evidence,
                        history_source,
                        calendar_resolver=_calendar(),
                        policy=policy,
                    )
                    mark = issue_actual_close_mark(market_source, context)

                    action = risk_module.evaluate_position(
                        context.position,
                        mark,
                        policy,
                    )

                    self.assertEqual(mark.event_evidence_status, "UNRESOLVED")
                    self.assertEqual(action.status, expected_status)
                    self.assertEqual(
                        action.shares_to_exit,
                        (
                            context.position.shares
                            if expected_exit is None
                            else expected_exit
                        ),
                    )
                    if expected_status == "POSITION_UNVERIFIED":
                        self.assertIn(
                            "EVENT_EVIDENCE_UNRESOLVED",
                            action.reason_codes,
                        )
                        with self.assertRaisesRegex(
                            RiskBlock,
                            "^ACTUAL_CLOSE_DECISION_POSITION_UNVERIFIED$",
                        ):
                            risk_module.issue_actual_close_decision(
                                context,
                                mark,
                                policy,
                            )
                    else:
                        self.assertIn(
                            "RECOMMENDED_STOP_REACHED",
                            action.reason_codes,
                        )
                        decision = risk_module.issue_actual_close_decision(
                            context,
                            mark,
                            policy,
                        )
                        self.assertEqual(decision.action, "EXIT")

    def test_actual_close_mark_requires_exact_bound_position_and_one_verification(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market_source, plan_source, event_evidence, history_source = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                context = issue_actual_position_event_context(
                    market_source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                mark = issue_actual_close_mark(market_source, context)
                candidates = (
                    replace(context.position),
                    replace(
                        context.position,
                        profit_target_taken=(
                            not context.position.profit_target_taken
                        ),
                    ),
                )

                for candidate in candidates:
                    with self.subTest(candidate=candidate):
                        verifier = risk_module.is_issued_market_mark
                        with mock.patch.object(
                            risk_module,
                            "is_issued_market_mark",
                            wraps=verifier,
                        ) as issued:
                            action = risk_module.evaluate_position(
                                candidate,
                                mark,
                                policy_fixture(),
                            )

                        self.assertEqual(issued.call_count, 1)
                        self.assertEqual(action.status, "POSITION_UNVERIFIED")
                        self.assertEqual(
                            action.reason_codes,
                            ("POSITION_REVISION_MISMATCH",),
                        )
                        self.assertEqual(action.shares_to_exit, 0)

    def test_sqlite_trace_callback_cannot_mutate_unbound_position_into_exit(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market_source, plan_source, event_evidence, history_source = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                    binary_event_coverage="UNKNOWN",
                )
                context = issue_actual_position_event_context(
                    market_source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                mark = issue_actual_close_mark(market_source, context)
                unbound_position = replace(context.position)
                callback_fired = False

                def mutate_unbound_position(_statement: str) -> None:
                    nonlocal callback_fired
                    if not callback_fired:
                        callback_fired = True
                        object.__setattr__(
                            unbound_position,
                            "recommended_stop",
                            mark.price,
                        )

                journal._connection.set_trace_callback(mutate_unbound_position)
                try:
                    action = risk_module.evaluate_position(
                        unbound_position,
                        mark,
                        policy_fixture(),
                    )
                finally:
                    journal._connection.set_trace_callback(None)

                self.assertTrue(callback_fired)
                self.assertEqual(action.status, "POSITION_UNVERIFIED")
                self.assertEqual(
                    action.reason_codes,
                    ("POSITION_REVISION_MISMATCH",),
                )
                self.assertEqual(action.shares_to_exit, 0)

    def test_policy_validation_callback_cannot_use_mark_after_later_write(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market_source, plan_source, event_evidence, history_source = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                context = issue_actual_position_event_context(
                    market_source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                mark = issue_actual_close_mark(market_source, context)
                evaluation_policy = policy_fixture()
                original_validate = type(evaluation_policy).validate
                callback_fired = False

                def validate_then_write(candidate: object) -> None:
                    nonlocal callback_fired
                    original_validate(candidate)
                    if not callback_fired:
                        callback_fired = True
                        journal.append_source_observation(
                            payload=b'{"policy_callback":true}',
                            source_uri="https://example.invalid/policy-callback",
                            source_type="TEST",
                            provider="fixture",
                            feed=None,
                            source_time=review_at,
                            retrieved_at=review_at,
                            provider_sequence=None,
                            delay_seconds=0,
                            health_result="OK",
                            details={"version": 1},
                        )

                verifier = risk_module.is_issued_market_mark
                with mock.patch.object(
                    type(evaluation_policy),
                    "validate",
                    new=validate_then_write,
                ), mock.patch.object(
                    risk_module,
                    "is_issued_market_mark",
                    wraps=verifier,
                ) as issued:
                    action = risk_module.evaluate_position(
                        context.position,
                        mark,
                        evaluation_policy,
                    )

                self.assertTrue(callback_fired)
                self.assertEqual(issued.call_count, 1)
                self.assertEqual(action.status, "POSITION_UNVERIFIED")
                self.assertEqual(
                    action.reason_codes,
                    ("POSITION_CONTEXT_UNVERIFIED",),
                )
                self.assertEqual(action.shares_to_exit, 0)
                self.assertFalse(is_issued_market_mark(mark))

    def test_policy_callback_cannot_issue_or_persist_a_stale_decision(
        self,
    ) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(
                Path(temporary_directory) / "journal.sqlite3"
            ) as journal:
                signal_source, query_cutoff = self._seed_linked_open_position(
                    journal
                )
                market, plan, evidence, history = self._context_inputs(
                    journal,
                    signal_source,
                    review_at=review_at,
                    query_cutoff=query_cutoff,
                )
                policy = policy_fixture()
                context = issue_actual_position_event_context(
                    market,
                    plan,
                    evidence,
                    history,
                    calendar_resolver=_calendar(),
                    policy=policy,
                )
                mark = issue_actual_close_mark(market, context)
                original_validate = type(policy).validate
                callback_fired = False

                def validate_then_write(candidate: object) -> None:
                    nonlocal callback_fired
                    original_validate(candidate)
                    if not callback_fired:
                        callback_fired = True
                        journal.append_source_observation(
                            payload=b'{"decision_callback":true}',
                            source_uri=(
                                "https://example.invalid/decision-callback"
                            ),
                            source_type="TEST",
                            provider="fixture",
                            feed=None,
                            source_time=review_at,
                            retrieved_at=review_at,
                            provider_sequence=None,
                            delay_seconds=0,
                            health_result="OK",
                            details={"version": 1},
                        )

                with mock.patch.object(
                    type(policy),
                    "validate",
                    new=validate_then_write,
                ), self.assertRaises(RiskBlock):
                    risk_module.issue_actual_close_decision(
                        context,
                        mark,
                        policy,
                    )

                self.assertTrue(callback_fired)
                self.assertFalse(is_issued_actual_close_market_source(market))
                self.assertFalse(is_issued_actual_position_event_context(context))
                self.assertFalse(is_issued_market_mark(mark))
                self.assertFalse(
                    risk_module.is_verified_latest_close_recommendation_source(
                        history
                    )
                )

    def test_prior_actual_stop_is_carried_forward_without_widening(self) -> None:
        review_at = aware_et(date(2026, 8, 14), "15:30")
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Journal.open(Path(temporary_directory) / "journal.sqlite3") as journal:
                signal_source, original_cutoff = self._seed_linked_open_position(
                    journal
                )
                update_at = original_cutoff + timedelta(minutes=1)
                ingest_confirmation(
                    journal,
                    ConfirmationEnvelope(
                        message_id="actual-prior-stop",
                        message_time=update_at,
                        received_at=update_at + timedelta(seconds=1),
                        text="STOP UPDATED AAPL @ 20.05 AT 15:29 ET",
                        session_date=update_at.date(),
                    ),
                    plans=UnavailableSignalPlanResolver(),
                    calendar=_calendar(),
                    policy=policy_fixture(),
                    entry_authorities=UnavailableActualEntryAuthorityResolver(),
                )
                query_cutoff = update_at + timedelta(seconds=2)
                source, plan_source, event_evidence, history_source = (
                    self._context_inputs(
                        journal,
                        signal_source,
                        review_at=review_at,
                        query_cutoff=query_cutoff,
                    )
                )
                actual_positions = tuple(
                    position
                    for position in plan_source.actual_position_state.positions
                    if position.symbol == _SYMBOL and position.shares > 0
                )
                self.assertEqual(len(actual_positions), 1)
                self.assertEqual(
                    actual_positions[0].user_stop_micros,
                    20_050_000,
                )
                context = issue_actual_position_event_context(
                    source,
                    plan_source,
                    event_evidence,
                    history_source,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )

                self.assertEqual(
                    context.position.initial_stop,
                    Decimal("19.90"),
                )
                self.assertEqual(
                    context.position.recommended_stop,
                    Decimal("19.90"),
                )
                self.assertEqual(
                    context.position.user_confirmed_stop,
                    Decimal("20.05"),
                )


if __name__ == "__main__":
    unittest.main()
