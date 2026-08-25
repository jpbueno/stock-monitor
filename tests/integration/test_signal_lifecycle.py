from __future__ import annotations

import copy
import hashlib
import inspect
import json
import sqlite3
import tempfile
import unittest
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import stock_monitor.evidence as evidence_module
import stock_monitor.journal as journal_module
import stock_monitor.ledger as ledger_module
import stock_monitor.providers.alpaca as alpaca_module
import stock_monitor.risk as risk_module
import stock_monitor.screening as screening_module
from stock_monitor.journal import (
    IdempotencyConflict,
    InvalidJournalValue,
    Journal,
    MigrationCorruption,
    report_archive_relative_path,
    stable_report_id,
)
from stock_monitor.ledger import LedgerPair, LedgerSignal
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.risk import (
    ClosedTrade,
    EquityPoint,
    LongPlanRequest,
    RiskBlock,
    SessionCalendarResolver,
    plan_long,
)
from stock_monitor.providers.http import HttpResponse
import tests.unit._task5_fixtures as task5_fixture_module
from tests.support import aware_et, credentials, policy_fixture
from tests.unit._task5_fixtures import (
    TEST_UNIVERSE,
    evidence,
    universe_candidate_contexts,
)


_SESSION = date(2026, 8, 14)
_WINDOW_ID = "1" * 64
_PROVIDER_RAW_PAGES: dict[str, dict[str, object]] = {}


def _calendar() -> SessionCalendarResolver:
    return SessionCalendarResolver(
        (
            load_current_market_calendar(
                Path(__file__).resolve().parents[2],
                as_of=_SESSION,
            ),
        )
    )


def _signal(*, shares: int = 47) -> LedgerSignal:
    return LedgerSignal(
        signal_id="2026-08-14:AAPL",
        symbol="AAPL",
        role="PRIMARY",
        publication_session=_SESSION,
        maximum_entry=Decimal("20.43"),
        recommended_stop=Decimal("19.90"),
        target=Decimal("21.49"),
        planned_shares=shares,
        tick_size=Decimal("0.01"),
        trigger_price=Decimal("20.40"),
    )


def _start_window(journal: Journal):
    return journal.start_phase1_validation_window(  # type: ignore[attr-defined]
        window_id=_WINDOW_ID,
        started_session=date(2026, 8, 13),
        starting_capital=Decimal("5000"),
        started_at=aware_et(date(2026, 8, 13), "16:00"),
        received_at=aware_et(date(2026, 8, 13), "16:00"),
        calendar_resolver=_calendar(),
    )


def _canonical_instant(value) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _rehash_exit_replay_tamper(journal: Journal, mode: str) -> None:
    """Forge internally self-consistent exit rows for replay adversaries."""
    connection = journal._connection
    trigger_names = (
        "phase1_exit_reviews_no_update",
        "phase1_exit_review_facts_no_update",
        "phase1_signal_events_no_update",
        "phase1_signal_events_no_delete",
        "phase1_canonical_postings_no_update",
        "phase1_canonical_postings_no_delete",
        "phase1_closed_trades_no_update",
        "phase1_closed_trades_no_delete",
    )
    trigger_sql = {
        trigger: str(
            connection.execute(
                "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?",
                (trigger,),
            ).fetchone()[0]
        )
        for trigger in trigger_names
    }
    for trigger in trigger_names:
        connection.execute(f"DROP TRIGGER {trigger}")

    event_columns = journal_module._PHASE1_SIGNAL_EVENT_COLUMNS
    posting_columns = journal_module._PHASE1_CANONICAL_POSTING_COLUMNS
    close_columns = journal_module._PHASE1_CLOSED_TRADE_COLUMNS
    signal_columns = journal_module._PHASE1_SIGNAL_COLUMNS
    review_columns = journal_module._PHASE1_EXIT_REVIEW_COLUMNS
    fact_columns = journal_module._PHASE1_EXIT_REVIEW_FACT_COLUMNS
    signal_id = _signal().signal_id
    exit_rows = connection.execute(
        "SELECT " + ", ".join(event_columns) + " FROM phase1_signal_events "
        "WHERE signal_id = ? AND event_kind IN ('PARTIAL_EXIT', 'CLOSE') "
        "ORDER BY event_ordinal",
        (signal_id,),
    ).fetchall()
    if len(exit_rows) != 2:
        raise AssertionError("expected one two-step canonical exit")
    target_event_id = str(exit_rows[0][1])
    if mode == "suffix":
        suffix_event_id = str(exit_rows[-1][1])
        connection.execute(
            "DELETE FROM phase1_canonical_postings WHERE lifecycle_event_id = ?",
            (suffix_event_id,),
        )
        connection.execute(
            "DELETE FROM phase1_closed_trades WHERE signal_id = ?",
            (signal_id,),
        )
        connection.execute(
            "DELETE FROM phase1_signal_events WHERE lifecycle_event_id = ?",
            (suffix_event_id,),
        )
        exit_rows = exit_rows[:-1]

    if mode == "fact":
        fact_row = list(
            connection.execute(
                "SELECT "
                + ", ".join(fact_columns)
                + " FROM phase1_exit_review_facts WHERE fact_id = ?",
                (str(exit_rows[0][14]),),
            ).fetchone()
        )
        fact_values = json.loads(str(fact_row[18]))
        fact_values["low"] = "1"
        fact_row[17] = hashlib.sha256(b"forged-normalized-fields").hexdigest()
        fact_row[18] = journal_module._canonical_audit_json(fact_values)
        fact_semantic = tuple(fact_row[1:19])
        fact_row[19] = hashlib.sha256(
            journal_module._canonical_audit_json(
                {
                    "namespace": "stock-monitor/phase1-exit-fact/v1",
                    "values": fact_semantic,
                }
            ).encode()
        ).hexdigest()
        fact_row[20] = hashlib.sha256(
            journal_module._canonical_audit_json(
                dict(
                    zip(
                        fact_columns[1:-1],
                        (*fact_semantic, fact_row[19]),
                        strict=True,
                    )
                )
            ).encode()
        ).hexdigest()
        connection.execute(
            "UPDATE phase1_exit_review_facts SET normalized_fields_digest = ?, "
            "values_json = ?, source_digest = ?, record_sha256 = ? WHERE id = ?",
            (fact_row[17], fact_row[18], fact_row[19], fact_row[20], fact_row[0]),
        )

        review_id = str(fact_row[2])
        role_order_sql = (
            "CASE purpose WHEN 'DAILY_BAR' THEN 1 "
            "WHEN 'EXECUTION_BAR' THEN 2 WHEN 'QUOTE' THEN 3 END"
        )
        manifest_columns = journal_module._PHASE1_EXIT_REVIEW_MANIFEST_COLUMNS
        page_columns = journal_module._PHASE1_EXIT_REVIEW_PAGE_COLUMNS
        manifest_rows = connection.execute(
            "SELECT "
            + ", ".join(manifest_columns)
            + " FROM phase1_exit_review_manifests WHERE review_id = ? "
            + f"ORDER BY {role_order_sql}, id",
            (review_id,),
        ).fetchall()
        page_rows = connection.execute(
            "SELECT "
            + ", ".join(page_columns)
            + " FROM phase1_exit_review_pages WHERE review_id = ? "
            + f"ORDER BY {role_order_sql}, page_ordinal, id",
            (review_id,),
        ).fetchall()
        fact_rows = connection.execute(
            "SELECT "
            + ", ".join(fact_columns)
            + " FROM phase1_exit_review_facts WHERE review_id = ? "
            + f"ORDER BY {role_order_sql}, fact_ordinal, id",
            (review_id,),
        ).fetchall()
        core_references = []
        for page_row in page_rows:
            source_row, payload_row = journal._phase1_core_source_rows(
                source_observation_id=int(page_row[4])
            )
            core_references.extend(
                (
                    journal_module._journal_row_reference(
                        "source_observations",
                        journal_module._SOURCE_OBSERVATION_COLUMNS,
                        source_row,
                    ),
                    journal_module._journal_row_reference(
                        "phase1_source_payloads",
                        journal_module._PHASE1_SOURCE_PAYLOAD_COLUMNS,
                        payload_row,
                    ),
                )
            )
        review_row = list(
            connection.execute(
                "SELECT "
                + ", ".join(review_columns)
                + " FROM phase1_exit_reviews WHERE review_id = ?",
                (review_id,),
            ).fetchone()
        )
        review_row[10] = hashlib.sha256(
            journal_module._canonical_audit_json(
                {
                    "namespace": "stock-monitor/phase1-exit-review-source/v1",
                    "signal_id": str(review_row[2]),
                    "review_id": review_id,
                    "review_session": str(review_row[3]),
                    "calendar_digest": str(review_row[4]),
                    "query_cutoff": str(review_row[5]),
                    "manifest_records": [str(row[-1]) for row in manifest_rows],
                    "page_records": [str(row[-1]) for row in page_rows],
                    "fact_records": [str(row[-1]) for row in fact_rows],
                    "core_references": [
                        [reference.table, reference.row_id, reference.row_digest]
                        for reference in core_references
                    ],
                }
            ).encode()
        ).hexdigest()
        review_row[11] = hashlib.sha256(
            journal_module._canonical_audit_json(
                dict(
                    zip(
                        review_columns[1:-1],
                        tuple(review_row[1:-1]),
                        strict=True,
                    )
                )
            ).encode()
        ).hexdigest()
        connection.execute(
            "UPDATE phase1_exit_reviews SET source_digest = ?, "
            "record_sha256 = ? WHERE id = ?",
            (review_row[10], review_row[11], review_row[0]),
        )

    def rehash_event(row: Sequence[object]) -> tuple[object, ...]:
        event = list(row)
        details = json.loads(str(event[20]))
        if mode == "fact":
            review_digest = connection.execute(
                "SELECT source_digest FROM phase1_exit_reviews WHERE review_id = ?",
                (str(details["exit_review_id"]),),
            ).fetchone()[0]
            details["exit_review_source_digest"] = str(review_digest)
        if str(event[1]) == target_event_id:
            if mode == "fill":
                event[17] = int(event[17]) + 1_000
            elif mode == "reason":
                details["execution_reason"] = "STOP"
                details["execution_reason_codes"] = ["STOP_TRIGGERED"]
            elif mode == "batch":
                details["step_ordinal"] = 2
        signal_row = connection.execute(
            "SELECT " + ", ".join(signal_columns) + " FROM phase1_signals "
            "WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        review_row = connection.execute(
            "SELECT " + ", ".join(review_columns) + " FROM phase1_exit_reviews "
            "WHERE review_id = ?",
            (str(details["exit_review_id"]),),
        ).fetchone()
        execution_row = connection.execute(
            "SELECT " + ", ".join(fact_columns) + " FROM phase1_exit_review_facts "
            "WHERE fact_id = ?",
            (str(event[14]),),
        ).fetchone()
        quote_row = connection.execute(
            "SELECT " + ", ".join(fact_columns) + " FROM phase1_exit_review_facts "
            "WHERE fact_id = ?",
            (str(details["execution_quote_observation_id"]),),
        ).fetchone()
        prior_row = connection.execute(
            "SELECT " + ", ".join(event_columns) + " FROM phase1_signal_events "
            "WHERE signal_id = ? AND event_ordinal = ?",
            (signal_id, int(event[3]) - 1),
        ).fetchone()
        references = (
            journal_module._journal_row_reference(
                "phase1_signals", signal_columns, signal_row
            ),
            journal_module._journal_row_reference(
                "phase1_exit_reviews", review_columns, review_row
            ),
            journal_module._journal_row_reference(
                "phase1_exit_review_facts", fact_columns, execution_row
            ),
            journal_module._journal_row_reference(
                "phase1_exit_review_facts", fact_columns, quote_row
            ),
            journal_module._journal_row_reference(
                "phase1_signal_events", event_columns, prior_row
            ),
        )
        details_json = journal_module._canonical_audit_json(details)
        event[19] = journal_module._journal_bundle_digest(
            "stock-monitor/phase1-exit-lifecycle-event/v1",
            references,
            journal._phase1_exit_event_material(
                tuple(event[1:19]),
                details_json,
            ),
        )
        event[20] = details_json
        connection.execute(
            "UPDATE phase1_signal_events SET price_micros = ?, source_digest = ?, "
            "details_json = ? WHERE id = ?",
            (event[17], event[19], event[20], event[0]),
        )
        return tuple(event)

    for row in exit_rows:
        rehash_event(row)

    signal_row = connection.execute(
        "SELECT " + ", ".join(signal_columns) + " FROM phase1_signals "
        "WHERE signal_id = ?",
        (signal_id,),
    ).fetchone()
    signal_reference = journal_module._journal_row_reference(
        "phase1_signals", signal_columns, signal_row
    )
    posting_rows = connection.execute(
        "SELECT " + ", ".join(posting_columns) + " FROM phase1_canonical_postings "
        "WHERE signal_id = ? ORDER BY id",
        (signal_id,),
    ).fetchall()
    for original in posting_rows:
        posting = list(original)
        if str(posting[2]) not in {str(row[1]) for row in exit_rows}:
            continue
        event = connection.execute(
            "SELECT " + ", ".join(event_columns) + " FROM phase1_signal_events "
            "WHERE lifecycle_event_id = ?",
            (str(posting[2]),),
        ).fetchone()
        details = json.loads(str(posting[16]))
        details["lifecycle_event_source_digest"] = str(event[19])
        if mode == "fact":
            review_digest = connection.execute(
                "SELECT source_digest FROM phase1_exit_reviews WHERE review_id = ?",
                (str(details["exit_review_id"]),),
            ).fetchone()[0]
            details["exit_review_source_digest"] = str(review_digest)
        if str(posting[2]) == target_event_id:
            if mode == "fill" and str(posting[4]) == "SALE":
                posting[8] = int(event[17])
                posting[6] = -int(posting[7]) * int(posting[8])
            elif mode == "reason":
                details["execution_reason"] = "STOP"
            elif mode == "batch":
                details["step_ordinal"] = 2
            elif mode == "settlement" and str(posting[4]) == "SALE":
                stored_day = date.fromisoformat(str(posting[11]))
                posting[11] = (stored_day + timedelta(days=1)).isoformat()
                details["settlement_available_session"] = posting[11]
        review_row = connection.execute(
            "SELECT " + ", ".join(review_columns) + " FROM phase1_exit_reviews "
            "WHERE review_id = ?",
            (str(details["exit_review_id"]),),
        ).fetchone()
        execution_row = connection.execute(
            "SELECT " + ", ".join(fact_columns) + " FROM phase1_exit_review_facts "
            "WHERE fact_id = ?",
            (str(event[14]),),
        ).fetchone()
        quote_row = connection.execute(
            "SELECT " + ", ".join(fact_columns) + " FROM phase1_exit_review_facts "
            "WHERE fact_id = ?",
            (str(details["execution_quote_observation_id"]),),
        ).fetchone()
        references = (
            signal_reference,
            journal_module._journal_row_reference(
                "phase1_signal_events", event_columns, event
            ),
            journal_module._journal_row_reference(
                "phase1_exit_reviews", review_columns, review_row
            ),
            journal_module._journal_row_reference(
                "phase1_exit_review_facts", fact_columns, execution_row
            ),
            journal_module._journal_row_reference(
                "phase1_exit_review_facts", fact_columns, quote_row
            ),
        )
        details_json = journal_module._canonical_audit_json(details)
        posting[14] = journal_module._journal_bundle_digest(
            "stock-monitor/phase1-canonical-exit-posting/v1",
            references,
            {
                **dict(
                    zip(
                        posting_columns[1:14],
                        tuple(posting[1:14]),
                        strict=True,
                    )
                ),
                "details_json": details_json,
            },
        )
        posting[15] = hashlib.sha256(
            journal_module._canonical_audit_json(
                {
                    **dict(
                        zip(
                            posting_columns[1:15],
                            tuple(posting[1:15]),
                            strict=True,
                        )
                    ),
                    "details_json": details_json,
                }
            ).encode()
        ).hexdigest()
        posting[16] = details_json
        connection.execute(
            "UPDATE phase1_canonical_postings SET amount_micros = ?, "
            "unit_price_micros = ?, settlement_available_session = ?, "
            "source_digest = ?, record_sha256 = ?, details_json = ? WHERE id = ?",
            (
                posting[6],
                posting[8],
                posting[11],
                posting[14],
                posting[15],
                posting[16],
                posting[0],
            ),
        )

    if mode == "suffix":
        for trigger in trigger_names:
            connection.execute(trigger_sql[trigger])
        connection.commit()
        return

    close_row = connection.execute(
        "SELECT " + ", ".join(close_columns) + " FROM phase1_closed_trades "
        "WHERE signal_id = ?",
        (signal_id,),
    ).fetchone()
    if close_row is None:
        raise AssertionError("expected aggregate closed trade")
    close = list(close_row)
    current_postings = connection.execute(
        "SELECT " + ", ".join(posting_columns) + " FROM phase1_canonical_postings "
        "WHERE signal_id = ? ORDER BY id",
        (signal_id,),
    ).fetchall()
    entry_value = -sum(int(row[6]) for row in current_postings if row[4] == "BUY")
    exit_value = sum(int(row[6]) for row in current_postings if row[4] == "SALE")
    fee_value = -sum(int(row[6]) for row in current_postings if row[4] == "FEE")
    pnl = exit_value - entry_value - fee_value
    close[8], close[9], close[10], close[11], close[13] = (
        entry_value,
        exit_value,
        fee_value,
        pnl,
        pnl,
    )
    close_event = connection.execute(
        "SELECT " + ", ".join(event_columns) + " FROM phase1_signal_events "
        "WHERE lifecycle_event_id = ?",
        (str(close[5]),),
    ).fetchone()
    close[17] = journal_module._journal_bundle_digest(
        "stock-monitor/phase1-closed-trade/v1",
        (
            signal_reference,
            *(
                journal_module._journal_row_reference(
                    "phase1_canonical_postings", posting_columns, row
                )
                for row in current_postings
            ),
            journal_module._journal_row_reference(
                "phase1_signal_events", event_columns, close_event
            ),
        ),
        dict(
            zip(
                close_columns[1:17],
                tuple(close[1:17]),
                strict=True,
            )
        ),
    )
    connection.execute(
        "UPDATE phase1_closed_trades SET entry_value_micros = ?, "
        "exit_value_micros = ?, fee_micros = ?, pnl_micros = ?, "
        "net_r_numerator_micros = ?, source_digest = ? WHERE id = ?",
        (close[8], close[9], close[10], close[11], close[13], close[17], close[0]),
    )
    for trigger in trigger_names:
        connection.execute(trigger_sql[trigger])
    connection.commit()


class _CandidateAlpacaTransport:
    def __init__(self, contexts: tuple[object, ...]) -> None:
        self._bars = dict(contexts[0].bars_by_symbol)
        self._previous_quotes = {
            context.record.symbol: context.previous_session_quote
            for context in contexts
        }
        self._latest_quotes = {
            context.record.symbol: context.latest_iex_quote
            for context in contexts
        }
        self.bodies_by_url: dict[str, bytes] = {}

    def get(self, url: str, headers: object) -> HttpResponse:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        symbols = tuple(query["symbols"][0].split(","))
        if parsed.path == "/v2/stocks/bars":
            document = {
                "bars": {
                    symbol: [
                        {
                            "c": str(bar.close),
                            "h": str(bar.high),
                            "l": str(bar.low),
                            "o": str(bar.open),
                            "t": _canonical_instant(bar.timestamp),
                            "v": bar.volume,
                        }
                        for bar in self._bars[symbol]
                    ]
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes/latest":
            document = {
                "quotes": {
                    symbol: {
                        "ap": str(self._latest_quotes[symbol].ask),
                        "bp": str(self._latest_quotes[symbol].bid),
                        "i": self._latest_quotes[symbol].sequence,
                        "t": _canonical_instant(
                            self._latest_quotes[symbol].timestamp
                        ),
                    }
                    for symbol in symbols
                },
                "next_page_token": None,
            }
        elif parsed.path == "/v2/stocks/quotes":
            document = {
                "next_page_token": None,
                "quotes": {
                    symbol: [
                        {
                            "ap": str(self._previous_quotes[symbol].ask),
                            "bp": str(self._previous_quotes[symbol].bid),
                            "i": self._previous_quotes[symbol].sequence,
                            "t": _canonical_instant(
                                self._previous_quotes[symbol].timestamp
                            ),
                        }
                    ]
                    for symbol in symbols
                },
            }
        else:
            raise AssertionError(f"unexpected Alpaca fixture URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.bodies_by_url[url] = body
        return HttpResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            url=url,
        )


class _LifecycleAlpacaTransport:
    def __init__(
        self,
        *,
        trade_items: tuple[dict[str, object], ...] | None = None,
        quote_items: tuple[dict[str, object], ...] | None = None,
    ) -> None:
        self.bodies_by_url: dict[str, bytes] = {}
        self.trade_items = (
            (
                {
                    "i": 101,
                    "p": "20.41",
                    "s": 100,
                    "t": "2026-08-14T13:36:00Z",
                },
            )
            if trade_items is None
            else trade_items
        )
        self.quote_items = (
            (
                {
                    "ap": "20.42",
                    "bp": "20.41",
                    "i": 202,
                    "t": "2026-08-14T13:37:00Z",
                },
            )
            if quote_items is None
            else quote_items
        )

    def get(self, url: str, headers: object) -> HttpResponse:
        path = urlsplit(url).path
        if path == "/v2/stocks/trades":
            document = {
                "next_page_token": None,
                "trades": {"AAPL": list(self.trade_items)},
            }
        elif path == "/v2/stocks/quotes":
            document = {
                "next_page_token": None,
                "quotes": {"AAPL": list(self.quote_items)},
            }
        else:
            raise AssertionError(f"unexpected lifecycle Alpaca URL: {url}")
        body = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.bodies_by_url[url] = body
        return HttpResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            url=url,
        )


def _issued_lifecycle_cohorts(
    *,
    trade_items: tuple[dict[str, object], ...] | None = None,
    quote_items: tuple[dict[str, object], ...] | None = None,
    now_time: str = "16:20",
    window_start: str = "09:30",
    window_end: str = "16:00",
):
    transport = _LifecycleAlpacaTransport(
        trade_items=trade_items,
        quote_items=quote_items,
    )
    now = aware_et(_SESSION, now_time).astimezone(UTC)
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: now,
    )
    trade_cohort = client.historical_trades(
        ("AAPL",),
        alpaca_module.TimeWindow(
            aware_et(_SESSION, window_start).astimezone(UTC),
            aware_et(_SESSION, window_end).astimezone(UTC),
        ),
    )
    quote_cohort = client.historical_quotes(
        ("AAPL",),
        alpaca_module.TimeWindow(
            aware_et(_SESSION, window_start).astimezone(UTC),
            aware_et(_SESSION, window_end).astimezone(UTC),
        ),
    )
    return trade_cohort, quote_cohort, transport


def _pin_provider_cohort_pages(
    journal: Journal,
    cohort: object,
    transport: _LifecycleAlpacaTransport,
) -> tuple[int, ...]:
    manifest = alpaca_module._provider_fetch_cohort_manifest(cohort)
    bundle = alpaca_module.read_provider_fetch_bundle(cohort)
    row_ids: list[int] = []
    for page in manifest.pages:
        payload = transport.bodies_by_url[page.request_url]
        page_bundle = next(
            value
            for value in bundle.pages
            if value.page.page_ordinal == page.page_ordinal
        )
        metadata = alpaca_module.recompute_alpaca_page_metadata(
            payload=payload,
            request_url=page.request_url,
            source_type=page.source_type,
            retrieved_at=page_bundle.observation.retrieved_at,
        )
        row_id, _duplicate = journal.append_source_observation(
            payload=payload,
            source_uri=page.request_url,
            source_type=page.source_type,
            provider="alpaca",
            feed="SIP",
            source_time=metadata.source_time,
            retrieved_at=metadata.retrieved_at,
            provider_sequence=None,
            delay_seconds=metadata.delay_seconds,
            health_result="OK",
            details={"source_observation_id": page.source_observation_id},
        )
        row_ids.append(row_id)
    return tuple(row_ids)


def _provider_issued_contexts():
    raw_contexts = tuple(universe_candidate_contexts())
    transport = _CandidateAlpacaTransport(raw_contexts)
    now = raw_contexts[0].as_of.astimezone(UTC)
    client = alpaca_module.AlpacaMarketData(
        transport,
        credentials(),
        now=lambda: now,
    )
    raw_bars = tuple(
        bar
        for values in raw_contexts[0].bars_by_symbol.values()
        for bar in values
    )
    issued_bars = client.daily_bars(
        tuple(sorted(raw_contexts[0].bars_by_symbol)),
        alpaca_module.TimeWindow(
            min(bar.timestamp for bar in raw_bars),
            max(bar.timestamp for bar in raw_bars),
        ),
    )
    candidate_symbols = tuple(
        sorted(context.record.symbol for context in raw_contexts)
    )
    prior_times = tuple(
        context.previous_session_quote.timestamp for context in raw_contexts
    )
    issued_previous = client.historical_quotes(
        candidate_symbols,
        alpaca_module.TimeWindow(
            min(prior_times) - timedelta(minutes=1),
            max(prior_times) + timedelta(minutes=2),
        ),
    )
    issued_latest = client.latest_iex_quotes(candidate_symbols)

    _PROVIDER_RAW_PAGES.clear()
    facts = tuple(
        fact
        for values in issued_bars.values()
        for fact in values
    ) + tuple(
        value
        for values in issued_previous.values()
        for value in values
    ) + tuple(issued_latest.values())
    for fact in facts:
        source = alpaca_module._normalized_market_fact_source(fact)
        assert source is not None
        for page in source.fetch_manifest.pages:
            if page.source_observation_id in _PROVIDER_RAW_PAGES:
                continue
            body = transport.bodies_by_url[page.request_url]
            document = json.loads(body)
            collection = document[source.fetch_manifest.collection]
            timestamps = tuple(
                datetime.fromisoformat(str(item["t"]).replace("Z", "+00:00"))
                for values in collection.values()
                for item in (values if isinstance(values, list) else (values,))
            )
            _PROVIDER_RAW_PAGES[page.source_observation_id] = {
                "feed": source.feed.upper(),
                "payload": body,
                "retrieved_at": now,
                "source_time": max(timestamps),
                "source_type": page.source_type,
                "source_uri": page.request_url,
            }

    return tuple(
        replace(
            context,
            bars_by_symbol=issued_bars,
            previous_session_quote=issued_previous[context.record.symbol][-1],
            latest_iex_quote=issued_latest[context.record.symbol],
        )
        for context in raw_contexts
    )


def _issued_candidates(count: int):
    contexts = []
    for context in _provider_issued_contexts():
        is_etf = context.record.product_type == "etf"
        contexts.append(
            replace(
                context,
                evidence=evidence(
                    subject_kind="ETF" if is_etf else "STOCK",
                    symbol=context.record.symbol,
                    issuer_cik=None if is_etf else "0000000000",
                    age_days=5,
                    event_type=(
                        "fund sponsor notice" if is_etf else "material agreement"
                    ),
                    binary_event_coverage=(
                        "NOT_APPLICABLE" if is_etf else "CONFIRMED_CLEAR"
                    ),
                    etf_action_coverage=(
                        "CONFIRMED_CLEAR" if is_etf else "NOT_APPLICABLE"
                    ),
                ),
            )
        )
    cohort = screening_module.build_base_eligible_cohort(
        tuple(contexts),
        universe=TEST_UNIVERSE,
    )
    candidates = tuple(
        screening_module.to_scored_candidate(context)
        for context in cohort.contexts
        if screening_module.score_candidate(context).publishable
        and screening_module.detect_setup(context).eligible
    )
    ranked = screening_module.rank_candidates(candidates)
    if count == 1:
        return next(candidate for candidate in ranked if candidate.symbol == "AAPL"),
    return ranked[:count]


def _plan_manifest(plan) -> dict[str, object]:
    assert plan.plan is not None
    assert plan.request is not None
    assert plan.authority_digest is not None
    assert plan.as_of is not None
    return {
        "as_of": _canonical_instant(plan.as_of),
        "authority_digest": plan.authority_digest,
        "exposure_micros": risk_module.money_to_micros(plan.plan.exposure),
        "planned_risk_micros": risk_module.money_to_micros(
            plan.plan.planned_risk
        ),
        "quantity": plan.plan.quantity,
        "request": {
            "entry_micros": risk_module.money_to_micros(plan.request.entry),
            "published_target_micros": risk_module.money_to_micros(
                plan.request.published_target
            ),
            "session_date": plan.request.session_date.isoformat(),
            "stop_micros": risk_module.money_to_micros(plan.request.stop),
            "symbol": plan.request.symbol,
            "tick_size_micros": risk_module.money_to_micros(
                plan.request.tick_size
            ),
        },
        "target_micros": risk_module.money_to_micros(plan.target),
    }


def _publication_body(decision, plan, *, published_at) -> str:
    state_manifest = journal_module._phase1_publication_state_manifest(
        decision,
        plan,
    )
    return json.dumps(
        {
            "phase1_publication": state_manifest,
            "published_at": _canonical_instant(published_at),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _issued_publication(
    journal: Journal,
    *,
    candidate_count: int = 1,
    candidates_override=None,
    supplied_candidate_count: int | None = None,
    state_digest_override: str | None = None,
    pin_mode: str = "exact",
):
    cutoff = aware_et(_SESSION, "08:45")
    candidates = (
        _issued_candidates(candidate_count)
        if candidates_override is None
        else tuple(candidates_override)
    )
    published_at = aware_et(_SESSION, "08:45")
    with patch.object(journal_module, "_utc_now", return_value=published_at):
        claim = journal.claim_report(_SESSION, "MORNING")
    predecessor_source = None
    if claim.status == "ALREADY_FINALIZED":
        assert claim.report_id is not None
        predecessor_source = journal.read_phase1_publication_source(  # type: ignore[attr-defined]
            claim.report_id
        )

    def issue_current():
        replay_source = journal._read_phase1_canonical_replay_source(  # type: ignore[attr-defined]
            query_cutoff=cutoff,
            publication_predecessor=True,
            predecessor_publication_source=predecessor_source,
        )
        replay = ledger_module._issue_canonical_ledger_replay_from_phase1_source(
            replay_source
        )
        history_source = journal._read_phase1_breaker_history_source(  # type: ignore[attr-defined]
            ledger_name="CANONICAL",
            through_session=date(2026, 8, 13),
            query_cutoff=aware_et(date(2026, 8, 13), "16:00"),
        )
        history = risk_module._issue_breaker_history_from_phase1_source(
            history_source,
            calendar_resolver=_calendar(),
        )
        breaker = risk_module.evaluate_authorized_breakers(history)
        primary = screening_module.rank_candidates(candidates)[0]
        request = LongPlanRequest.from_scored_candidate(primary)
        policy = policy_fixture()
        portfolio_authority = risk_module._issue_portfolio_risk_authority(
            request=request,
            ledger_pair=replay.ledger_pair,
            ledger_name="CANONICAL",
            breaker_state=breaker,
            calendar_resolver=_calendar(),
            policy=policy,
            scope="CANONICAL_PUBLICATION",
            as_of=cutoff,
            phase1_canonical_replay=replay,
        )
        plan = plan_long(
            request,
            portfolio_authority.portfolio_state,
            policy,
            portfolio_authority=portfolio_authority,
        )
        report_decision = (
            screening_module._issue_portfolio_bound_publication_decision(
                candidates,
                primary_plan_decision=plan,
            )
        )
        decision = report_decision
        if supplied_candidate_count is not None:
            decision = screening_module._issue_portfolio_bound_publication_decision(
                screening_module.rank_candidates(candidates)[
                    :supplied_candidate_count
                ],
                primary_plan_decision=plan,
            )
        return (
            report_decision,
            decision,
            plan,
            (replay_source, history_source),
        )

    report_decision, decision, plan, lineage = issue_current()
    observation_manifest = screening_module._publication_observation_manifest(
        report_decision
    )
    publication_observation_ids = []
    source_time = aware_et(_SESSION, "08:30")
    for ordinal, external_id in enumerate(
        observation_manifest.source_observation_ids,
        start=1,
    ):
        provider_page = _PROVIDER_RAW_PAGES.get(external_id)
        raw_payload = (
            json.dumps(
                {
                    "raw_page": external_id,
                    "version": 1,
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if provider_page is None
            else provider_page["payload"]
        )
        assert isinstance(raw_payload, bytes)
        if pin_mode == "provider_payload_tamper" and provider_page is not None:
            raw_payload = raw_payload.replace(b'"v":7000000', b'"v":7000001', 1)
        raw_source_time = (
            source_time
            if provider_page is None
            else provider_page["source_time"]
        )
        raw_retrieved_at = (
            raw_source_time
            if provider_page is None
            else provider_page["retrieved_at"]
        )
        assert isinstance(raw_source_time, datetime)
        assert isinstance(raw_retrieved_at, datetime)
        raw_delay_seconds = int(
            (raw_retrieved_at - raw_source_time).total_seconds()
        )
        if provider_page is not None:
            if pin_mode == "provider_retrieved_at_backdated":
                raw_retrieved_at -= timedelta(microseconds=1)
                raw_delay_seconds = int(
                    (raw_retrieved_at - raw_source_time).total_seconds()
                )
            elif pin_mode == "provider_source_time_shifted":
                raw_source_time -= timedelta(microseconds=1)
                raw_delay_seconds = int(
                    (raw_retrieved_at - raw_source_time).total_seconds()
                )
            elif pin_mode == "provider_delay_shifted":
                raw_delay_seconds += 1
        row_id, _duplicate = journal.append_source_observation(
            payload=raw_payload,
            source_uri=(
                "https://phase1.invalid/task5/"
                + hashlib.sha256(external_id.encode("utf-8")).hexdigest()
                if provider_page is None
                else str(provider_page["source_uri"])
            ),
            source_type=(
                "TASK5_CANDIDATE_EVIDENCE"
                if provider_page is None
                else str(provider_page["source_type"])
            ),
            provider=(
                "TASK5_AUTHORITY" if provider_page is None else "alpaca"
            ),
            feed=(
                None if provider_page is None else str(provider_page["feed"])
            ),
            source_time=raw_source_time,
            retrieved_at=raw_retrieved_at,
            provider_sequence=ordinal,
            delay_seconds=raw_delay_seconds,
            health_result="OK",
            details={"source_observation_id": external_id},
        )
        publication_observation_ids.append(row_id)
    if pin_mode == "missing":
        publication_observation_ids = publication_observation_ids[:-1]
    elif pin_mode == "empty":
        publication_observation_ids = []
    elif pin_mode == "extra":
        extra_id, _duplicate = journal.append_source_observation(
            payload=b'{"raw_page":"unrelated","version":1}',
            source_uri="https://phase1.invalid/task5/unrelated",
            source_type="TASK5_CANDIDATE_EVIDENCE",
            provider="TASK5_AUTHORITY",
            feed=None,
            source_time=source_time,
            retrieved_at=source_time,
            provider_sequence=len(publication_observation_ids) + 1,
            delay_seconds=0,
            health_result="OK",
            details={"source_observation_id": "unrelated-source"},
        )
        publication_observation_ids.append(extra_id)
    elif pin_mode == "duplicate_external_id":
        duplicate_external_id = observation_manifest.source_observation_ids[0]
        duplicate_id, _duplicate = journal.append_source_observation(
            payload=b'{"raw_page":"conflicting-copy","version":1}',
            source_uri="https://phase1.invalid/task5/conflicting-copy",
            source_type="TASK5_CANDIDATE_EVIDENCE",
            provider="TASK5_AUTHORITY",
            feed=None,
            source_time=source_time,
            retrieved_at=source_time,
            provider_sequence=len(publication_observation_ids) + 1,
            delay_seconds=0,
            health_result="OK",
            details={"source_observation_id": duplicate_external_id},
        )
        publication_observation_ids.append(duplicate_id)
    elif pin_mode not in {
        "exact",
        "provider_payload_tamper",
        "provider_retrieved_at_backdated",
        "provider_source_time_shifted",
        "provider_delay_shifted",
    }:
        raise AssertionError(f"unsupported publication pin mode: {pin_mode}")

    # Source ingestion changes Journal authority generation. Reissue the exact
    # decision/plan before finalizing, then reissue once more after finalization.
    report_decision, decision, plan, lineage = issue_current()
    body = _publication_body(report_decision, plan, published_at=published_at)
    state_digest = (
        journal_module.phase1_publication_state_sha256(report_decision, plan)
        if state_digest_override is None
        else state_digest_override
    )
    if publication_observation_ids:
        placeholders = ", ".join("?" for _ in publication_observation_ids)
        rows = journal._connection.execute(  # type: ignore[attr-defined]
            "SELECT id, observation_sha256 FROM source_observations "
            f"WHERE id IN ({placeholders})",
            tuple(publication_observation_ids),
        ).fetchall()
        ordered_sha256s = tuple(
            value
            for _row_id, value in sorted(
                ((int(row[0]), str(row[1])) for row in rows),
                key=lambda item: (item[1], item[0]),
            )
        )
    else:
        ordered_sha256s = ()
    report_id = stable_report_id(
        "MORNING",
        _SESSION,
        ordered_sha256s,
        state_digest,
    )
    with patch.object(journal_module, "_utc_now", return_value=published_at):
        if claim.status == "ALREADY_FINALIZED":
            assert predecessor_source is not None
            return predecessor_source, decision, plan, lineage
        finalized = journal.finalize_report(
            claim_id=claim.claim_id,
            claim_token=claim.claim_token or "",
            body=body,
            state_sha256=state_digest,
            observation_ids=tuple(publication_observation_ids),
            archive_relative_path=report_archive_relative_path(
                "MORNING",
                _SESSION,
                report_id,
            ),
            created_at=published_at,
            outbox_destination="CODEX_TASK",
            outbox_payload="phase1 publication",
        )
    source = journal.read_phase1_publication_source(  # type: ignore[attr-defined]
        finalized.report_id
    )
    predecessor_source = source
    _report_decision, decision, plan, lineage = issue_current()
    return source, decision, plan, lineage


def _publish(journal: Journal, *, candidates_override=None):
    _start_window(journal)
    publication_source, decision, plan, _lineage = _issued_publication(
        journal,
        candidates_override=candidates_override,
    )
    return journal.publish_phase1_report(  # type: ignore[attr-defined]
        publication_source=publication_source,
        decision=decision,
        primary_plan_decision=plan,
        validation_window_id=_WINDOW_ID,
        calendar_resolver=_calendar(),
        received_at=aware_et(_SESSION, "08:45"),
    )


def _prepare_completed_entry(journal: Journal):
    _publish(journal)
    trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts()
    trade_rows = _pin_provider_cohort_pages(journal, trade_cohort, transport)
    quote_rows = _pin_provider_cohort_pages(journal, quote_cohort, transport)
    ingested = journal.ingest_phase1_session_cohorts(
        _signal().signal_id,
        (trade_cohort, quote_cohort),
        core_source_row_ids=(*trade_rows, *quote_rows),
        calendar_resolver=_calendar(),
    )
    completion_at = aware_et(_SESSION, "16:21")
    journal.complete_phase1_session(  # type: ignore[attr-defined]
        signal_id=_signal().signal_id,
        session_date=_SESSION,
        cohort_through_ordinal=len(ingested.observation_ids),
        expected_observation_count=len(ingested.observation_ids),
        received_through=aware_et(_SESSION, "16:20"),
        completed_at=completion_at,
        calendar_resolver=_calendar(),
    )
    return ingested.observation_ids, completion_at


def _seed_completed_fill(journal: Journal):
    observation_ids, completion_at = _prepare_completed_entry(journal)
    authority = journal.record_phase1_entry(
        _signal().signal_id,
        trigger_observation_id=observation_ids[0],
        quote_observation_id=observation_ids[1],
        calendar_resolver=_calendar(),
        recorded_at=completion_at,
    )
    return authority, completion_at


class SignalLifecyclePersistenceTests(unittest.TestCase):
    def test_active_validation_window_cannot_be_cherry_picked_or_restarted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                with self.assertRaises(IdempotencyConflict):
                    journal.start_phase1_validation_window(  # type: ignore[attr-defined]
                        window_id="2" * 64,
                        started_session=date(2026, 8, 14),
                        starting_capital=Decimal("5000"),
                        started_at=aware_et(date(2026, 8, 14), "16:00"),
                        received_at=aware_et(date(2026, 8, 14), "16:00"),
                        calendar_resolver=_calendar(),
                    )
                self.assertEqual(journal.count("phase1_validation_windows"), 1)

    def test_published_signal_and_initial_event_commit_atomically(self) -> None:
        self.assertTrue(
            hasattr(Journal, "publish_phase1_report"),
            "Task 8 must add the atomic full-report publication API",
        )
        self.assertFalse(hasattr(Journal, "publish_phase1_signal"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                result = _publish(journal)

                self.assertEqual(result.signal_ids, (_signal().signal_id,))
                self.assertEqual(result.initial_status, "PUBLISHED")
                self.assertFalse(result.duplicate)
                self.assertEqual(journal.count("phase1_signals"), 1)
                self.assertEqual(journal.count("phase1_signal_events"), 1)

    def test_publication_persists_and_reparses_exact_provider_fact_manifests(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                signal = journal._read_phase1_signal_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                source = signal.publication_source
                self.assertTrue(source.manifest_digest)
                self.assertTrue(source.normalized_market_fact_sources)
                self.assertTrue(source.provider_fetch_manifests)
                self.assertEqual(
                    {
                        item.source_observation_id
                        for item in source.normalized_market_fact_sources
                    },
                    set(_PROVIDER_RAW_PAGES),
                )

            with Journal.open(path) as reopened:
                restarted = reopened._read_phase1_signal_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "08:45"),
                ).publication_source
                self.assertEqual(restarted.manifest_digest, source.manifest_digest)
                self.assertEqual(
                    restarted.normalized_market_fact_sources,
                    source.normalized_market_fact_sources,
                )
                self.assertEqual(
                    restarted.provider_fetch_manifests,
                    source.provider_fetch_manifests,
                )

    def test_publication_rejects_same_external_id_with_tampered_provider_page(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                with self.assertRaisesRegex(
                    MigrationCorruption,
                    "Alpaca page metadata|provider|observation manifest",
                ):
                    _issued_publication(
                        journal,
                        pin_mode="provider_payload_tamper",
                    )
                self.assertEqual(journal.count("phase1_signals"), 0)
                self.assertEqual(journal.count("phase1_signal_events"), 0)

    def test_publication_recomputes_provider_page_identity_and_chronology(
        self,
    ) -> None:
        for pin_mode in (
            "provider_retrieved_at_backdated",
            "provider_source_time_shifted",
            "provider_delay_shifted",
        ):
            with self.subTest(pin_mode=pin_mode), tempfile.TemporaryDirectory() as temporary_directory:
                path = Path(temporary_directory) / "journal.db"
                with Journal.open(path) as journal:
                    _start_window(journal)
                    with self.assertRaisesRegex(
                        MigrationCorruption,
                        "Alpaca page metadata",
                    ):
                        _issued_publication(journal, pin_mode=pin_mode)
                    self.assertEqual(journal.count("phase1_signals"), 0)
                    self.assertEqual(journal.count("phase1_signal_events"), 0)

    def test_only_issued_terminal_provider_cohorts_mint_lifecycle_facts(
        self,
    ) -> None:
        self.assertFalse(hasattr(Journal, "append_phase1_observation"))
        self.assertFalse(hasattr(Journal, "ingest_phase1_session_cohort"))
        self.assertTrue(hasattr(Journal, "ingest_phase1_session_cohorts"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = (
                    _issued_lifecycle_cohorts()
                )
                trade_rows = _pin_provider_cohort_pages(
                    journal,
                    trade_cohort,
                    transport,
                )
                quote_rows = _pin_provider_cohort_pages(
                    journal,
                    quote_cohort,
                    transport,
                )

                result = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (quote_cohort, trade_cohort),
                    core_source_row_ids=(*quote_rows, *trade_rows),
                    calendar_resolver=_calendar(),
                )
                self.assertFalse(result.duplicate)
                self.assertEqual(len(result.manifest_ids), 2)
                self.assertEqual(len(result.observation_ids), 2)
                self.assertEqual(journal.count("phase1_observations"), 2)
                rows = journal._connection.execute(  # type: ignore[attr-defined]
                    "SELECT observation_kind, cohort_ordinal, fresh "
                    "FROM phase1_observations ORDER BY cohort_ordinal"
                ).fetchall()
                self.assertEqual(
                    [tuple(row) for row in rows],
                    [("TRADE", 1, 1), ("QUOTE", 2, 1)],
                )

                retry = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (trade_cohort, quote_cohort),
                    core_source_row_ids=(*trade_rows, *quote_rows),
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(retry.duplicate)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "requires live-fetch provider cohorts",
                ):
                    journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                        _signal().signal_id,
                        (copy.copy(trade_cohort), quote_cohort),
                        core_source_row_ids=(*trade_rows, *quote_rows),
                        calendar_resolver=_calendar(),
                    )
                self.assertEqual(journal.count("phase1_observations"), 2)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "internally authenticated observation plan",
                ):
                    journal._append_phase1_observation(  # type: ignore[attr-defined]
                        {
                            "signal_id": _signal().signal_id,
                            "observation_id": "forged",
                            "cohort_ordinal": 3,
                        }
                    )
                self.assertEqual(journal.count("phase1_observations"), 2)

            with Journal.open(path) as reopened:
                self.assertEqual(reopened.count("phase1_observations"), 2)
                self.assertEqual(
                    reopened.count("phase1_observation_fetch_manifests"),
                    2,
                )

    def test_lifecycle_cohort_set_rejects_cross_provider_owner_splice(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                first_trade, _first_quote, first_transport = (
                    _issued_lifecycle_cohorts()
                )
                _second_trade, second_quote, second_transport = (
                    _issued_lifecycle_cohorts()
                )
                row_ids = (
                    *_pin_provider_cohort_pages(
                        journal,
                        first_trade,
                        first_transport,
                    ),
                    *_pin_provider_cohort_pages(
                        journal,
                        second_quote,
                        second_transport,
                    ),
                )
                before = (
                    journal.count("phase1_observation_fetch_manifests"),
                    journal.count("phase1_observation_fetch_pages"),
                    journal.count("phase1_observations"),
                )

                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "one provider owner",
                ):
                    journal.ingest_phase1_session_cohorts(
                        _signal().signal_id,
                        (first_trade, second_quote),
                        core_source_row_ids=row_ids,
                        calendar_resolver=_calendar(),
                    )

                self.assertEqual(
                    (
                        journal.count("phase1_observation_fetch_manifests"),
                        journal.count("phase1_observation_fetch_pages"),
                        journal.count("phase1_observations"),
                    ),
                    before,
                )

    def test_lifecycle_completion_requires_full_session_request_coverage(
        self,
    ) -> None:
        trade_items = (
            {
                "i": 101,
                "p": "20.41",
                "s": 100,
                "t": "2026-08-14T14:02:00Z",
            },
        )
        quote_items = (
            {
                "ap": "20.42",
                "bp": "20.41",
                "i": 202,
                "t": "2026-08-14T14:02:00Z",
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts(
                    trade_items=trade_items,
                    quote_items=quote_items,
                    window_end="10:02",
                )
                row_ids = (
                    *_pin_provider_cohort_pages(journal, trade_cohort, transport),
                    *_pin_provider_cohort_pages(journal, quote_cohort, transport),
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "full release-calendar session",
                ):
                    journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                        _signal().signal_id,
                        (trade_cohort, quote_cohort),
                        core_source_row_ids=row_ids,
                        calendar_resolver=_calendar(),
                    )
                self.assertEqual(
                    journal.count("phase1_observation_fetch_manifests"), 0
                )
                self.assertEqual(journal.count("phase1_observations"), 0)

    def test_empty_terminal_cohort_set_is_required_and_restart_stable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "no enrolled provider fetch",
                ):
                    journal.complete_phase1_session(  # type: ignore[attr-defined]
                        signal_id=_signal().signal_id,
                        session_date=_SESSION,
                        cohort_through_ordinal=0,
                        expected_observation_count=0,
                        received_through=aware_et(_SESSION, "16:20"),
                        completed_at=aware_et(_SESSION, "16:21"),
                        calendar_resolver=_calendar(),
                    )

                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts(
                    trade_items=(),
                    quote_items=(),
                )
                row_ids = (
                    *_pin_provider_cohort_pages(journal, trade_cohort, transport),
                    *_pin_provider_cohort_pages(journal, quote_cohort, transport),
                )
                result = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (quote_cohort, trade_cohort),
                    core_source_row_ids=tuple(reversed(row_ids)),
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(len(result.manifest_ids), 2)
                self.assertEqual(result.observation_ids, ())
                journal.complete_phase1_session(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    cohort_through_ordinal=0,
                    expected_observation_count=0,
                    received_through=aware_et(_SESSION, "16:20"),
                    completed_at=aware_et(_SESSION, "16:21"),
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(journal.count("phase1_session_completions"), 1)

                manifest_count = journal.count(
                    "phase1_observation_fetch_manifests"
                )
                page_count = journal.count("phase1_observation_fetch_pages")
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "completed session rejects later fetch manifests",
                ):
                    journal._connection.execute(  # type: ignore[attr-defined]
                        "INSERT INTO phase1_observation_fetch_manifests("
                        "cohort_id, signal_id, session_date, purpose, "
                        "collection_name, requested_symbols_json, "
                        "request_digest, manifest_digest, "
                        "semantic_manifest_digest, terminal, request_start, "
                        "request_end, calendar_digest, received_through, "
                        "source_digest, record_sha256"
                        ") SELECT ?, signal_id, session_date, purpose, 'bars', "
                        "requested_symbols_json, ?, ?, ?, terminal, "
                        "request_start, request_end, calendar_digest, "
                        "received_through, ?, ? "
                        "FROM phase1_observation_fetch_manifests LIMIT 1",
                        tuple(character * 64 for character in "abcdef"),
                    )
                self.assertEqual(
                    journal.count("phase1_observation_fetch_manifests"),
                    manifest_count,
                )
                self.assertEqual(
                    journal.count("phase1_observation_fetch_pages"), page_count
                )

            with Journal.open(path) as reopened:
                source = reopened._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    query_cutoff=aware_et(_SESSION, "16:21"),
                )
                self.assertEqual(source.expected_observation_count, 0)
                self.assertGreater(len(source.row_references), 1)

    def test_semantic_retry_aliases_but_novel_late_cohort_invalidates(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts()
                row_ids = (
                    *_pin_provider_cohort_pages(journal, trade_cohort, transport),
                    *_pin_provider_cohort_pages(journal, quote_cohort, transport),
                )
                first = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (trade_cohort, quote_cohort),
                    core_source_row_ids=row_ids,
                    calendar_resolver=_calendar(),
                )
                journal.complete_phase1_session(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    cohort_through_ordinal=2,
                    expected_observation_count=2,
                    received_through=aware_et(_SESSION, "16:20"),
                    completed_at=aware_et(_SESSION, "16:21"),
                    calendar_resolver=_calendar(),
                )

                retry_trade, retry_quote, retry_transport = (
                    _issued_lifecycle_cohorts(now_time="16:21")
                )
                retry_rows = (
                    *_pin_provider_cohort_pages(
                        journal, retry_trade, retry_transport
                    ),
                    *_pin_provider_cohort_pages(
                        journal, retry_quote, retry_transport
                    ),
                )
                retry = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (retry_quote, retry_trade),
                    core_source_row_ids=tuple(reversed(retry_rows)),
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(retry.duplicate)
                self.assertFalse(retry.completion_invalidated)
                self.assertEqual(retry.manifest_ids, first.manifest_ids)
                self.assertEqual(retry.observation_ids, first.observation_ids)
                stable_completion = (
                    journal._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                        signal_id=_signal().signal_id,
                        session_date=_SESSION,
                        query_cutoff=aware_et(_SESSION, "16:21"),
                    )
                )
                self.assertEqual(stable_completion.expected_observation_count, 2)

                novel_trade, same_quote, novel_transport = (
                    _issued_lifecycle_cohorts(
                        trade_items=(
                            {
                                "i": 101,
                                "p": "20.41",
                                "s": 100,
                                "t": "2026-08-14T13:36:00Z",
                            },
                            {
                                "i": 102,
                                "p": "20.42",
                                "s": 50,
                                "t": "2026-08-14T13:40:00Z",
                            },
                        ),
                        now_time="16:20",
                    )
                )
                novel_rows = (
                    *_pin_provider_cohort_pages(
                        journal, novel_trade, novel_transport
                    ),
                    *_pin_provider_cohort_pages(
                        journal, same_quote, novel_transport
                    ),
                )
                before = (
                    journal.count("phase1_observation_fetch_manifests"),
                    journal.count("phase1_observation_fetch_pages"),
                    journal.count("phase1_observations"),
                )
                invalidation = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (same_quote, novel_trade),
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                    recorded_at=aware_et(_SESSION, "16:22"),
                )
                self.assertTrue(invalidation.completion_invalidated)
                self.assertFalse(invalidation.duplicate)
                self.assertEqual(
                    (
                        journal.count("phase1_observation_fetch_manifests"),
                        journal.count("phase1_observation_fetch_pages"),
                        journal.count("phase1_observations"),
                    ),
                    before,
                )
                self.assertEqual(journal.count("phase1_session_late_evidence"), 1)
                self.assertEqual(
                    journal.count("phase1_session_late_evidence_pages"), 1
                )
                late_retry = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (novel_trade, same_quote),
                    core_source_row_ids=tuple(reversed(novel_rows)),
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(late_retry.duplicate)
                self.assertTrue(late_retry.completion_invalidated)
                self.assertEqual(journal.count("phase1_session_late_evidence"), 1)
                historical = journal._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    query_cutoff=aware_et(_SESSION, "16:21"),
                )
                self.assertEqual(historical.expected_observation_count, 2)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "PHASE1_OBSERVATION_COHORT_LATE_FACT",
                ):
                    journal._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                        signal_id=_signal().signal_id,
                        session_date=_SESSION,
                        query_cutoff=aware_et(_SESSION, "16:22"),
                    )

            with Journal.open(path) as reopened:
                restarted_retry = reopened.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (same_quote, novel_trade),
                    core_source_row_ids=novel_rows,
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(restarted_retry.duplicate)
                self.assertTrue(restarted_retry.completion_invalidated)
                self.assertEqual(reopened.count("phase1_session_late_evidence"), 1)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "PHASE1_OBSERVATION_COHORT_LATE_FACT",
                ):
                    reopened._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                        signal_id=_signal().signal_id,
                        session_date=_SESSION,
                        query_cutoff=aware_et(_SESSION, "16:22"),
                    )

    def test_concurrent_exact_late_cohort_records_one_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts()
                baseline_rows = (
                    *_pin_provider_cohort_pages(journal, trade_cohort, transport),
                    *_pin_provider_cohort_pages(journal, quote_cohort, transport),
                )
                journal.ingest_phase1_session_cohorts(
                    _signal().signal_id,
                    (trade_cohort, quote_cohort),
                    core_source_row_ids=baseline_rows,
                    calendar_resolver=_calendar(),
                )
                journal.complete_phase1_session(
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    cohort_through_ordinal=2,
                    expected_observation_count=2,
                    received_through=aware_et(_SESSION, "16:20"),
                    completed_at=aware_et(_SESSION, "16:21"),
                    calendar_resolver=_calendar(),
                )
                novel_trade, same_quote, novel_transport = (
                    _issued_lifecycle_cohorts(
                        trade_items=(
                            {
                                "i": 101,
                                "p": "20.41",
                                "s": 100,
                                "t": "2026-08-14T13:36:00Z",
                            },
                            {
                                "i": 102,
                                "p": "20.42",
                                "s": 50,
                                "t": "2026-08-14T13:40:00Z",
                            },
                        ),
                        now_time="16:20",
                    )
                )
                novel_rows = (
                    *_pin_provider_cohort_pages(
                        journal, novel_trade, novel_transport
                    ),
                    *_pin_provider_cohort_pages(
                        journal, same_quote, novel_transport
                    ),
                )

            def invalidate(_: int) -> bool:
                with Journal.open(path) as journal:
                    return journal.ingest_phase1_session_cohorts(
                        _signal().signal_id,
                        (novel_trade, same_quote),
                        core_source_row_ids=novel_rows,
                        calendar_resolver=_calendar(),
                        recorded_at=aware_et(_SESSION, "16:22"),
                    ).duplicate

            with ThreadPoolExecutor(max_workers=2) as executor:
                duplicates = tuple(executor.map(invalidate, range(2)))
            self.assertEqual(sorted(duplicates), [False, True])
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("phase1_session_late_evidence"), 1)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "PHASE1_OBSERVATION_COHORT_LATE_FACT",
                ):
                    journal._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                        signal_id=_signal().signal_id,
                        session_date=_SESSION,
                        query_cutoff=aware_et(_SESSION, "16:22"),
                    )

    def test_equal_time_cross_stream_order_is_quote_then_trade_and_completes(
        self,
    ) -> None:
        trade_items = (
            {
                "i": 101,
                "p": "20.41",
                "s": 100,
                "t": "2026-08-14T20:00:00Z",
            },
        )
        quote_items = (
            {
                "ap": "20.42",
                "bp": "20.41",
                "i": 202,
                "t": "2026-08-14T20:00:00Z",
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts(
                    trade_items=trade_items,
                    quote_items=quote_items,
                )
                row_ids = (
                    *_pin_provider_cohort_pages(journal, quote_cohort, transport),
                    *_pin_provider_cohort_pages(journal, trade_cohort, transport),
                )
                result = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (trade_cohort, quote_cohort),
                    core_source_row_ids=row_ids,
                    calendar_resolver=_calendar(),
                )
                rows = journal._connection.execute(  # type: ignore[attr-defined]
                    "SELECT observation_kind, source_time, cohort_ordinal "
                    "FROM phase1_observations ORDER BY cohort_ordinal"
                ).fetchall()
                self.assertEqual(
                    [str(row[0]) for row in rows],
                    ["QUOTE", "TRADE"],
                )
                self.assertEqual(
                    tuple(int(row[2]) for row in rows),
                    (1, 2),
                )
                journal.complete_phase1_session(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    cohort_through_ordinal=2,
                    expected_observation_count=2,
                    received_through=aware_et(_SESSION, "16:20"),
                    completed_at=aware_et(_SESSION, "16:21"),
                    calendar_resolver=_calendar(),
                )
                self.assertEqual(journal.count("phase1_session_completions"), 1)
                self.assertEqual(len(result.observation_ids), 2)

            with Journal.open(path) as reopened:
                source = reopened._read_phase1_session_completion_source(  # type: ignore[attr-defined]
                    signal_id=_signal().signal_id,
                    session_date=_SESSION,
                    query_cutoff=aware_et(_SESSION, "16:21"),
                )
                self.assertEqual(source.expected_observation_count, 2)
                observations = tuple(
                    reopened.read_phase1_observation(  # type: ignore[attr-defined]
                        observation_id,
                        query_cutoff=aware_et(_SESSION, "16:21"),
                    )
                    for observation_id in result.observation_ids
                )
                diagnostic = ledger_module._phase1_entry_result_from_observations(
                    observations,
                    trigger=Decimal("20.40"),
                    limit=Decimal("20.43"),
                )
                self.assertEqual(diagnostic.status.value, "UNRESOLVED")
                self.assertEqual(
                    diagnostic.reason_codes,
                    ("MISSING_POST_TRIGGER_QUOTE",),
                )
                self.assertEqual(reopened.count("phase1_canonical_postings"), 0)
                self.assertEqual(
                    reopened.count("phase1_observation_fetch_pages"),
                    2,
                )

    def test_interleaved_cohort_set_order_is_canonical_and_retry_invariant(
        self,
    ) -> None:
        trade_items = (
            {
                "i": 101,
                "p": "20.41",
                "s": 100,
                "t": "2026-08-14T13:36:00Z",
            },
            {
                "i": 102,
                "p": "20.43",
                "s": 50,
                "t": "2026-08-14T14:00:00Z",
            },
        )
        quote_items = (
            {
                "ap": "20.42",
                "bp": "20.41",
                "i": 201,
                "t": "2026-08-14T13:37:00Z",
            },
            {
                "ap": "20.44",
                "bp": "20.43",
                "i": 202,
                "t": "2026-08-14T14:01:00Z",
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                trade_cohort, quote_cohort, transport = _issued_lifecycle_cohorts(
                    trade_items=trade_items,
                    quote_items=quote_items,
                )
                trade_rows = _pin_provider_cohort_pages(
                    journal, trade_cohort, transport
                )
                quote_rows = _pin_provider_cohort_pages(
                    journal, quote_cohort, transport
                )
                first = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (quote_cohort, trade_cohort),
                    core_source_row_ids=(*quote_rows, *trade_rows),
                    calendar_resolver=_calendar(),
                )
                rows = journal._connection.execute(  # type: ignore[attr-defined]
                    "SELECT observation_id, observation_kind, cohort_ordinal "
                    "FROM phase1_observations ORDER BY cohort_ordinal"
                ).fetchall()
                self.assertEqual(
                    [(str(row[1]), int(row[2])) for row in rows],
                    [("TRADE", 1), ("QUOTE", 2), ("TRADE", 3), ("QUOTE", 4)],
                )

                retry = journal.ingest_phase1_session_cohorts(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    (trade_cohort, quote_cohort),
                    core_source_row_ids=(*trade_rows, *quote_rows),
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(retry.duplicate)
                self.assertEqual(retry.observation_ids, first.observation_ids)
                self.assertEqual(
                    retry.observation_ids,
                    tuple(str(row[0]) for row in rows),
                )

    def test_publication_retry_is_exact_and_copied_source_is_rejected(self) -> None:
        self.assertTrue(hasattr(Journal, "publish_phase1_report"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                first = _publish(journal)
                duplicate = _publish(journal)
                self.assertEqual(duplicate.source_digest, first.source_digest)
                self.assertTrue(duplicate.duplicate)
                self.assertEqual(journal.count("phase1_signals"), 1)
                self.assertEqual(journal.count("phase1_signal_events"), 1)

    def test_generic_replay_is_inclusive_but_publication_plan_uses_predecessor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                inclusive = journal._read_phase1_canonical_replay_source(  # type: ignore[attr-defined]
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                self.assertEqual(
                    tuple(source.signal_id for source in inclusive.signal_sources),
                    (_signal().signal_id,),
                )
                self.assertEqual(
                    tuple(event.event_kind for event in inclusive.lifecycle_events),
                    ("PUBLISHED",),
                )
                signal_source = journal._read_phase1_signal_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "08:45"),
                )
                predecessor = journal._read_phase1_canonical_replay_source(  # type: ignore[attr-defined]
                    query_cutoff=aware_et(_SESSION, "08:45"),
                    publication_predecessor=True,
                    predecessor_publication_source=(
                        signal_source.publication_source
                    ),
                )
                self.assertEqual(predecessor.signal_sources, ())
                self.assertEqual(predecessor.lifecycle_events, ())

                source, decision, plan, _lineage = _issued_publication(journal)
                copied_source = copy.copy(source)
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "publication source",
                ):
                    journal.publish_phase1_report(  # type: ignore[attr-defined]
                        publication_source=copied_source,
                        decision=decision,
                        primary_plan_decision=plan,
                        validation_window_id=_WINDOW_ID,
                        calendar_resolver=_calendar(),
                        received_at=aware_et(_SESSION, "08:45"),
                    )
                self.assertEqual(journal.count("phase1_signals"), 1)
                self.assertEqual(journal.count("phase1_signal_events"), 1)

    def test_caller_transaction_rollback_removes_signal_and_initial_event(self) -> None:
        self.assertTrue(hasattr(Journal, "publish_phase1_report"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                publication_source, decision, plan, _lineage = (
                    _issued_publication(journal)
                )
                with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                    with journal.transaction() as transaction:
                        transaction.publish_phase1_report(  # type: ignore[attr-defined]
                            publication_source=publication_source,
                            decision=decision,
                            primary_plan_decision=plan,
                            validation_window_id=_WINDOW_ID,
                            calendar_resolver=_calendar(),
                            received_at=aware_et(_SESSION, "08:45"),
                        )
                        raise RuntimeError("synthetic crash")

                self.assertEqual(journal.count("phase1_signals"), 0)
                self.assertEqual(journal.count("phase1_signal_events"), 0)

    def test_concurrent_publication_has_one_stable_row_pair(self) -> None:
        self.assertTrue(hasattr(Journal, "publish_phase1_report"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            candidates = _issued_candidates(1)

            # Finalize the immutable report prerequisite before racing signal
            # publication. Concurrent report-claim recovery is covered by the
            # report journal tests; this assertion is specifically about the
            # atomic signal/event row pair.
            with Journal.open(path) as journal:
                _start_window(journal)
                _issued_publication(
                    journal,
                    candidates_override=candidates,
                )

            def publish_once(_: int):
                last_error: Exception | None = None
                for _attempt in range(5):
                    try:
                        with Journal.open(path) as journal:
                            return _publish(
                                journal,
                                candidates_override=candidates,
                            )
                    except (
                        IdempotencyConflict,
                        InvalidJournalValue,
                        RiskBlock,
                        screening_module.ScreeningError,
                    ) as error:
                        # Another publisher can commit between the owner-bound
                        # source read and write. Reissue from the new snapshot.
                        last_error = error
                assert last_error is not None
                raise last_error

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(publish_once, range(2)))

            self.assertEqual(sum(not result.duplicate for result in results), 1)
            self.assertEqual(sum(result.duplicate for result in results), 1)
            with Journal.open(path) as journal:
                self.assertEqual(journal.count("phase1_signals"), 1)
                self.assertEqual(journal.count("phase1_signal_events"), 1)

    def test_publication_requires_the_exact_report_candidate_manifest(self) -> None:
        self.assertTrue(hasattr(Journal, "publish_phase1_report"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                source, incomplete, plan, _lineage = _issued_publication(
                    journal,
                    candidate_count=3,
                    supplied_candidate_count=2,
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "manifest",
                ):
                    journal.publish_phase1_report(  # type: ignore[attr-defined]
                        publication_source=source,
                        decision=incomplete,
                        primary_plan_decision=plan,
                        validation_window_id=_WINDOW_ID,
                        calendar_resolver=_calendar(),
                        received_at=aware_et(_SESSION, "08:45"),
                    )
                self.assertEqual(journal.count("phase1_signals"), 0)
                self.assertEqual(journal.count("phase1_signal_events"), 0)

    def test_publication_rejects_missing_empty_and_extra_report_pins(self) -> None:
        for pin_mode in ("empty", "missing", "extra"):
            with self.subTest(pin_mode=pin_mode):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    path = Path(temporary_directory) / "journal.db"
                    with Journal.open(path) as journal:
                        _start_window(journal)
                        source, decision, plan, _lineage = _issued_publication(
                            journal,
                            pin_mode=pin_mode,
                        )
                        with self.assertRaisesRegex(
                            InvalidJournalValue,
                            "observation manifest",
                        ):
                            journal.publish_phase1_report(  # type: ignore[attr-defined]
                                publication_source=source,
                                decision=decision,
                                primary_plan_decision=plan,
                                validation_window_id=_WINDOW_ID,
                                calendar_resolver=_calendar(),
                                received_at=aware_et(_SESSION, "08:45"),
                            )
                        self.assertEqual(journal.count("phase1_signals"), 0)

    def test_publication_source_rejects_duplicate_external_source_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                with self.assertRaisesRegex(
                    MigrationCorruption,
                    "source identity",
                ):
                    _issued_publication(
                        journal,
                        pin_mode="duplicate_external_id",
                    )

    def test_publication_rejects_a_forged_structured_state_digest(self) -> None:
        self.assertTrue(hasattr(Journal, "publish_phase1_report"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _start_window(journal)
                source, decision, plan, _lineage = _issued_publication(
                    journal,
                    state_digest_override="f" * 64,
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "publication state manifest",
                ):
                    journal.publish_phase1_report(  # type: ignore[attr-defined]
                        publication_source=source,
                        decision=decision,
                        primary_plan_decision=plan,
                        validation_window_id=_WINDOW_ID,
                        calendar_resolver=_calendar(),
                        received_at=aware_et(_SESSION, "08:45"),
                    )
                self.assertEqual(journal.count("phase1_signals"), 0)
                self.assertEqual(journal.count("phase1_signal_events"), 0)

    def test_restart_reader_reissues_exact_signal_identity_only(self) -> None:
        self.assertTrue(hasattr(Journal, "read_phase1_signal"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)

            with Journal.open(path) as journal:
                issued = journal.read_phase1_signal(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:00"),
                )
                self.assertEqual(issued, _signal())
                self.assertTrue(ledger_module.is_issued_ledger_signal(issued))
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(replace(issued))
                )
                self.assertFalse(
                    ledger_module.is_issued_ledger_signal(copy.copy(issued))
                )
            self.assertFalse(ledger_module.is_issued_ledger_signal(issued))

    def test_persisted_plan_resolver_respects_symbol_session_and_cutoff(self) -> None:
        self.assertTrue(hasattr(Journal, "phase1_signal_plan_resolver"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                _publish(journal)
                persisted_source = journal._read_phase1_signal_source(  # type: ignore[attr-defined]
                    _signal().signal_id,
                    query_cutoff=aware_et(_SESSION, "10:15"),
                )
                resolver = journal.phase1_signal_plan_resolver()  # type: ignore[attr-defined]
                resolved = resolver.resolve(
                    symbol="AAPL",
                    economic_at=aware_et(_SESSION, "10:14"),
                    query_cutoff=aware_et(_SESSION, "10:15"),
                )
                self.assertIsNotNone(resolved)
                assert resolved is not None
                self.assertTrue(
                    ledger_module.is_issued_ledger_signal(resolved.signal)
                )
                self.assertEqual(
                    resolved.report_id,
                    persisted_source.publication_report_id,
                )
                self.assertEqual(resolved.publication_rank, 1)
                self.assertEqual(
                    resolved.publication_source_digest,
                    persisted_source.publication_source_digest,
                )
                self.assertIsNone(
                    resolver.resolve(
                        symbol="QQQ",
                        economic_at=aware_et(_SESSION, "10:14"),
                        query_cutoff=aware_et(_SESSION, "10:15"),
                    )
                )
                self.assertIsNone(
                    resolver.resolve(
                        symbol="AAPL",
                        economic_at=aware_et(_SESSION, "10:14"),
                        query_cutoff=aware_et(_SESSION, "08:44"),
                    )
                )

    def test_raw_phase1_registrars_are_not_public(self) -> None:
        self.assertFalse(hasattr(Journal, "append_phase1_observation"))
        self.assertFalse(hasattr(Journal, "record_phase1_paper_fill"))
        self.assertFalse(hasattr(Journal, "append_phase1_canonical_posting"))

    def test_typed_entry_retry_and_concurrent_retry_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                authority, completed_at = _seed_completed_fill(journal)
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                )
                retried = journal.record_phase1_entry(
                    authority.signal_id,
                    trigger_observation_id=authority.trigger_observation_id,
                    quote_observation_id=authority.quote_observation_id,
                    calendar_resolver=_calendar(),
                    recorded_at=completed_at,
                )
                self.assertEqual(retried, authority)
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                    ),
                    before,
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.record_phase1_entry(
                        authority.signal_id,
                        trigger_observation_id=authority.trigger_observation_id,
                        quote_observation_id="conflicting-quote",
                        calendar_resolver=_calendar(),
                        recorded_at=completed_at,
                    )
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                    ),
                    before,
                )

            def retry() -> object:
                with Journal.open(path) as connection:
                    return connection.record_phase1_entry(
                        authority.signal_id,
                        trigger_observation_id=authority.trigger_observation_id,
                        quote_observation_id=authority.quote_observation_id,
                        calendar_resolver=_calendar(),
                        recorded_at=completed_at,
                    )

            with ThreadPoolExecutor(max_workers=2) as executor:
                concurrent = tuple(executor.map(lambda _index: retry(), range(2)))
            self.assertEqual(concurrent[0], concurrent[1])
            with Journal.open(path) as restarted:
                self.assertEqual(restarted.count("phase1_signal_events"), 3)
                self.assertEqual(restarted.count("phase1_canonical_postings"), 1)

    def test_typed_entry_failure_after_lifecycle_rolls_back_the_buy_bundle(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                observation_ids, completed_at = _prepare_completed_entry(journal)
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                )
                with patch.object(
                    Journal,
                    "_insert_phase1_entry_buy",
                    side_effect=RuntimeError("injected BUY failure"),
                ), self.assertRaisesRegex(RuntimeError, "injected BUY failure"):
                    journal.record_phase1_entry(
                        _signal().signal_id,
                        trigger_observation_id=observation_ids[0],
                        quote_observation_id=observation_ids[1],
                        calendar_resolver=_calendar(),
                        recorded_at=completed_at,
                    )
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                    ),
                    before,
                )

    def test_raw_canonical_exit_tuple_is_not_public(self) -> None:
        self.assertEqual(
            tuple(
                inspect.signature(
                    Journal.record_phase1_canonical_exit
                ).parameters
            ),
            ("self", "exit_authority", "recorded_at", "calendar_resolver"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            with Journal.open(path) as journal:
                before = (
                    journal.count("phase1_signal_events"),
                    journal.count("phase1_canonical_postings"),
                    journal.count("phase1_closed_trades"),
                )
                with self.assertRaises((TypeError, InvalidJournalValue)):
                    journal.record_phase1_canonical_exit(  # type: ignore[call-arg]
                        signal_id="2026-08-14:AAPL",
                        shares=47,
                        price=Decimal("21.49"),
                        fee=Decimal("1.00"),
                        recorded_at=aware_et(_SESSION, "16:00"),
                        calendar_resolver=_calendar(),
                    )
                self.assertEqual(
                    (
                        journal.count("phase1_signal_events"),
                        journal.count("phase1_canonical_postings"),
                        journal.count("phase1_closed_trades"),
                    ),
                    before,
                )

    def test_exit_replay_rejects_fully_rehashed_semantic_tampering(self) -> None:
        from tests.integration.test_phase1_authorities import (
            _prepare_same_session_batch_exit,
            _seed_completed_authority_fill,
        )

        role, pair, authority = task5_fixture_module._test_coverage_authority(
            "AAPL",
            "0000000000",
        )
        scoped_patcher = patch.dict(
            evidence_module._SCOPED_REFERENCE_AUTHORITIES,
            {role: authority},
        )
        clear_patcher = patch.dict(
            evidence_module._CLEAR_COVERAGE_AUTHORITIES,
            {role: frozenset({pair})},
        )
        scoped_patcher.start()
        clear_patcher.start()
        self.addCleanup(clear_patcher.stop)
        self.addCleanup(scoped_patcher.stop)

        cases = (
            ("fact", "normalized fact integrity failed"),
            ("fill", "conservative simulation"),
            ("reason", "conservative simulation"),
            ("batch", "batch cardinality"),
            ("suffix", "batch cardinality"),
            ("settlement", "posting bundle"),
        )
        for mode, reason in cases:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "journal.db"
                with Journal.open(path) as journal:
                    _seed_completed_authority_fill(journal)
                    source, authority, query_cutoff = (
                        _prepare_same_session_batch_exit(journal)
                    )
                    journal.record_phase1_canonical_exit(
                        exit_authority=authority,
                        recorded_at=query_cutoff,
                        calendar_resolver=_calendar(),
                    )
                    self.assertFalse(
                        journal_module.is_verified_phase1_exit_review_source(
                            source
                        )
                    )
                    _rehash_exit_replay_tamper(journal, mode)

                with Journal.open(path) as restarted:
                    with self.assertRaisesRegex(MigrationCorruption, reason):
                        restarted.read_phase1_canonical_replay(
                            query_cutoff=query_cutoff,
                            calendar_resolver=_calendar(),
                            policy=policy_fixture(),
                        )

    def test_exit_replay_requires_authorities_and_holiday_t_plus_one(self) -> None:
        from tests.integration.test_phase1_authorities import (
            _record_provider_typed_exit,
            _seed_completed_authority_fill,
        )

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.db"
            exit_session = date(2026, 9, 4)
            expected_settlement = date(2026, 9, 8)
            with Journal.open(path) as journal:
                _seed_completed_authority_fill(journal)
                signal_source = journal._read_phase1_signal_source(
                    _signal().signal_id,
                    query_cutoff=aware_et(exit_session, "15:59")
                    + timedelta(seconds=59),
                )
                _source, authority, _stored, query_cutoff = (
                    _record_provider_typed_exit(
                        journal,
                        signal_source=signal_source,
                        session_date=exit_session,
                        bid=Decimal("21.00"),
                        ask=Decimal("21.01"),
                        previous_session_low=Decimal("20.80"),
                        execution_open=Decimal("21.00"),
                        execution_high=Decimal("21.05"),
                        execution_low=Decimal("20.95"),
                        execution_close=Decimal("21.00"),
                        adverse_evidence=False,
                        persist_evidence=False,
                    )
                )
                self.assertEqual(
                    tuple(step.event_kind for step in authority.steps),
                    ("CLOSE",),
                )
                self.assertEqual(
                    _calendar().add_sessions(exit_session, 1),
                    expected_settlement,
                )
                exit_postings = journal._connection.execute(
                    "SELECT entry_kind, settlement_available_session "
                    "FROM phase1_canonical_postings WHERE signal_id = ? "
                    "AND entry_kind IN ('SALE', 'FEE') ORDER BY id",
                    (_signal().signal_id,),
                ).fetchall()
                self.assertEqual(
                    tuple((str(row[0]), str(row[1])) for row in exit_postings),
                    (
                        ("SALE", expected_settlement.isoformat()),
                        ("FEE", exit_session.isoformat()),
                    ),
                )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "requires verified calendar and policy",
                ):
                    journal.read_phase1_canonical_replay(
                        query_cutoff=query_cutoff,
                        policy=policy_fixture(),
                    )
                with self.assertRaisesRegex(
                    InvalidJournalValue,
                    "requires verified calendar and policy",
                ):
                    journal.read_phase1_canonical_replay(
                        query_cutoff=query_cutoff,
                        calendar_resolver=_calendar(),
                    )
                before_settlement = journal.read_phase1_canonical_replay(
                    query_cutoff=query_cutoff,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(
                    before_settlement.settled_buying_power,
                    Decimal("4038.79"),
                )

            with Journal.open(path) as restarted:
                settled = restarted.read_phase1_canonical_replay(
                    query_cutoff=aware_et(expected_settlement, "09:30"),
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertEqual(
                    settled.settled_buying_power,
                    Decimal("5024.803000"),
                )

    def test_breaker_history_reader_binds_complete_rows_and_owner_lifetime(self) -> None:
        self.assertTrue(hasattr(Journal, "read_phase1_breaker_history"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            close_at = aware_et(_SESSION, "16:00")
            with Journal.open(path) as journal:
                _start_window(journal)
                history = journal.read_phase1_breaker_history(  # type: ignore[attr-defined]
                    ledger_name="CANONICAL",
                    through_session=date(2026, 8, 13),
                    query_cutoff=aware_et(date(2026, 8, 13), "16:00"),
                    calendar_resolver=_calendar(),
                )
                self.assertTrue(
                    risk_module.is_issued_breaker_history_authority(history)
                )
                self.assertFalse(
                    risk_module.is_issued_breaker_history_authority(
                        copy.copy(history)
                    )
                )
            self.assertFalse(
                risk_module.is_issued_breaker_history_authority(history)
            )

    def test_canonical_portfolio_authority_is_rebuilt_from_persisted_cohorts(self) -> None:
        self.assertTrue(
            hasattr(Journal, "read_phase1_canonical_portfolio_authority")
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal.db"
            prior_session = date(2026, 8, 13)
            as_of = aware_et(_SESSION, "08:45")
            request = LongPlanRequest(
                entry=Decimal("100"),
                stop=Decimal("97.50"),
                tick_size=Decimal("0.01"),
                session_date=_SESSION,
                symbol="SPY",
                published_target=Decimal("105"),
            )
            with Journal.open(path) as journal:
                _publish(journal)
                authority = journal.read_phase1_canonical_portfolio_authority(  # type: ignore[attr-defined]
                    request=request,
                    as_of=as_of,
                    calendar_resolver=_calendar(),
                    policy=policy_fixture(),
                )
                self.assertTrue(
                    risk_module.is_issued_portfolio_risk_authority(authority)
                )
                self.assertEqual(authority.portfolio_state.settled_cash, Decimal("5000"))
                self.assertFalse(
                    risk_module.is_issued_portfolio_risk_authority(
                        copy.copy(authority)
                    )
                )
            self.assertFalse(
                risk_module.is_issued_portfolio_risk_authority(authority)
            )

    def test_account_evidence_cannot_be_inserted_as_canonical_posting(self) -> None:
        self.assertFalse(hasattr(Journal, "append_phase1_canonical_posting"))

    def test_typed_closed_trade_round_trip_excludes_unlinked_rows(self) -> None:
        self.assertFalse(
            hasattr(Journal, "append_phase1_closed_trade"),
            "raw closed-trade registrars must not exist",
        )
        self.assertFalse(
            hasattr(Journal, "append_phase1_equity_point"),
            "raw equity registrars must not exist",
        )


if __name__ == "__main__":
    unittest.main()
