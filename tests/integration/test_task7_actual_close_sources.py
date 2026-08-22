from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

from stock_monitor import journal as journal_module
from stock_monitor import risk as risk_module
from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import (
    ActualCloseFailureBinding,
    ActualCloseReceiptBinding,
    InvalidJournalValue,
    Journal,
    MigrationCorruption,
    is_verified_actual_close_review_source,
    is_verified_close_recommendation_source,
    is_verified_latest_close_recommendation_source,
)
from stock_monitor.providers.alpaca import recompute_alpaca_page_metadata
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
    replay_actual,
)
from stock_monitor.risk import (
    issue_actual_close_mark,
    issue_actual_close_market_source,
    issue_actual_position_event_context,
)
from tests.integration import (
    test_actual_close_composition as actual_close_fixture_module,
)
from tests.integration.test_phase1_authorities import (
    _append_completed_entry_observations,
    _confirmation_action_source,
)
from tests.integration.test_signal_lifecycle import _calendar, _publish
from tests.support import aware_et, policy_fixture


_DAY = date(2026, 8, 14)
_SYMBOL = "AAPL"
_SYMBOL_ROLES = (
    "SIP_QUOTE",
    "SIP_MINUTE_BAR",
    "SIP_DAILY_BAR",
    "IEX_FRESHNESS",
    "EVENT_EVIDENCE",
)
_GLOBAL_ROLES = (
    "PRIMARY_HALT_FEED",
    "TRADER_ALERT_HALT",
    "OPERATIONAL_STATUS",
    "CROSS_CHECK_CALENDAR",
)
_DEFAULT_PAGE_TOKEN = object()


def _utc_text(value) -> str:
    return value.astimezone().astimezone(tz=None).isoformat()


class ActualCloseJournalSourceTests(unittest.TestCase):
    def _composition_fixture(
        self,
    ) -> actual_close_fixture_module.ActualCloseCompositionTests:
        fixture = actual_close_fixture_module.ActualCloseCompositionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def _issued_decision(
        self,
        fixture: actual_close_fixture_module.ActualCloseCompositionTests,
        journal: Journal,
        signal_source: object,
        *,
        review_at: datetime,
        query_cutoff: datetime,
        **market_overrides: object,
    ):
        market, plan, evidence, history = fixture._context_inputs(
            journal,
            signal_source,
            review_at=review_at,
            query_cutoff=query_cutoff,
            **market_overrides,
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
        return decision, plan, history

    def _reissued_decision(
        self,
        fixture: actual_close_fixture_module.ActualCloseCompositionTests,
        journal: Journal,
        recommendation_source: object,
        *,
        review_at: datetime,
        query_cutoff: datetime,
    ):
        review = journal.read_actual_close_review_source(
            recommendation_source.review_id,
            query_cutoff=query_cutoff,
        )
        plan = fixture._final_plan(journal, query_cutoff)
        evidence = fixture._final_event_evidence(
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
        return risk_module.issue_actual_close_decision(
            context,
            mark,
            policy,
        )

    @staticmethod
    def _payload(value: object) -> bytes:
        return json.dumps(
            value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def _market_receipt(
        self,
        journal: Journal,
        *,
        source_type: str,
        retrieved_at,
        page: int = 1,
        terminal: bool = True,
        item_index: int | None = None,
        window_start_delta: timedelta = timedelta(),
        request_page_token: object = _DEFAULT_PAGE_TOKEN,
        response_next_page_token: object = _DEFAULT_PAGE_TOKEN,
    ):
        mark_cutoff = aware_et(_DAY, "15:14")
        contracts = {
            "ALPACA_HISTORICAL_QUOTES": (
                "quotes",
                "/v2/stocks/quotes",
                "sip",
                {
                    "ap": "20.27",
                    "bp": "20.25",
                    "i": page if item_index is None else item_index,
                    "t": mark_cutoff.isoformat(),
                },
            ),
            "ALPACA_INTRADAY_BARS": (
                "bars",
                "/v2/stocks/bars",
                "sip",
                {
                    "c": "20.25",
                    "h": "20.30",
                    "l": "20.10",
                    "o": "20.20",
                    "t": mark_cutoff.isoformat(),
                    "v": 1000,
                },
            ),
            "ALPACA_DAILY_BARS": (
                "bars",
                "/v2/stocks/bars",
                "sip",
                {
                    "c": "20.00",
                    "h": "20.30",
                    "l": "19.80",
                    "o": "19.90",
                    "t": (mark_cutoff - timedelta(days=1)).isoformat(),
                    "v": 100000,
                },
            ),
            "ALPACA_LATEST_QUOTES": (
                "quotes",
                "/v2/stocks/quotes/latest",
                "iex",
                {
                    "ap": "20.28",
                    "bp": "20.24",
                    "i": page if item_index is None else item_index,
                    "t": (retrieved_at - timedelta(minutes=1)).isoformat(),
                },
            ),
        }
        collection, path, feed, item = contracts[source_type]
        values: object = [item] if source_type != "ALPACA_LATEST_QUOTES" else item
        next_page_token = (
            None if terminal else f"page-{page + 1}"
        ) if response_next_page_token is _DEFAULT_PAGE_TOKEN else (
            response_next_page_token
        )
        document = {
            collection: {_SYMBOL: values},
            "next_page_token": next_page_token,
        }
        query = [("symbols", _SYMBOL), ("feed", feed)]
        if source_type != "ALPACA_LATEST_QUOTES":
            query.extend(
                (
                    ("start", (mark_cutoff - timedelta(minutes=5)).isoformat()),
                    ("end", mark_cutoff.isoformat()),
                )
            )
            query[-2] = (
                "start",
                (
                    mark_cutoff
                    - timedelta(minutes=5)
                    + window_start_delta
                ).isoformat(),
            )
        if source_type == "ALPACA_INTRADAY_BARS":
            query.append(("timeframe", "1Min"))
        if source_type == "ALPACA_DAILY_BARS":
            query.append(("timeframe", "1Day"))
        page_token = (
            f"page-{page}" if page > 1 else None
        ) if request_page_token is _DEFAULT_PAGE_TOKEN else request_page_token
        if page_token is not None:
            query.append(("page_token", str(page_token)))
        source_uri = (
            "https://data.alpaca.markets" + path + "?" + urlencode(query)
        )
        payload = self._payload(document)
        metadata = recompute_alpaca_page_metadata(
            payload=payload,
            request_url=source_uri,
            source_type=source_type,
            retrieved_at=retrieved_at,
        )
        return journal.append_source_observation_receipt(
            payload=payload,
            source_uri=source_uri,
            source_type=source_type,
            provider="alpaca",
            feed=feed,
            source_time=metadata.source_time,
            retrieved_at=metadata.retrieved_at,
            provider_sequence=None,
            delay_seconds=metadata.delay_seconds,
            health_result="OK",
            details={"source_observation_id": metadata.source_observation_id},
        )

    @staticmethod
    def _required_failures(
        received_at,
        *,
        symbols: tuple[str, ...] = (_SYMBOL,),
        receipt_scopes: tuple[tuple[str | None, str], ...] = (),
        omitted_scopes: tuple[tuple[str | None, str], ...] = (),
    ) -> tuple[ActualCloseFailureBinding, ...]:
        receipt_scope_set = set(receipt_scopes)
        omitted_scope_set = set(omitted_scopes)
        scopes = (
            *((symbol, role) for role in _SYMBOL_ROLES for symbol in symbols),
            *((None, role) for role in _GLOBAL_ROLES),
        )
        return tuple(
            ActualCloseFailureBinding(
                symbol,
                role,
                "SOURCE_UNAVAILABLE",
                received_at,
            )
            for symbol, role in scopes
            if (symbol, role) not in receipt_scope_set
            and (symbol, role) not in omitted_scope_set
        )

    def _review(self, journal: Journal, *, day: date = _DAY, receipt=None):
        review_at = aware_et(day, "16:30")
        mark_cutoff = aware_et(day, "16:14")
        query_cutoff = aware_et(day, "16:31")
        if receipt is None:
            receipt = self._market_receipt(
                journal,
                source_type="ALPACA_HISTORICAL_QUOTES",
                retrieved_at=query_cutoff,
            )
        return journal.append_actual_close_review(
            session_date=day,
            review_at=review_at,
            mark_cutoff=mark_cutoff,
            query_cutoff=query_cutoff,
            retrieved_at=query_cutoff,
            receipt_bindings=(
                ActualCloseReceiptBinding(_SYMBOL, "SIP_QUOTE", receipt),
            ),
            failure_bindings=(
                *self._required_failures(
                    query_cutoff,
                    receipt_scopes=((_SYMBOL, "SIP_QUOTE"),),
                ),
            ),
        )

    @staticmethod
    def _actual_snapshot(journal: Journal, cutoff):
        with journal.transaction() as transaction:
            replay_source = transaction.read_actual_replay(query_cutoff=cutoff)
        state = replay_actual(
            replay_source,
            plans=UnavailableSignalPlanResolver(),
            calendar=_calendar(),
            policy=policy_fixture(),
        )
        return replay_source, state

    def _seed_position(self, journal: Journal):
        _publish(journal)
        signal_source = journal._read_phase1_canonical_replay_source(
            query_cutoff=aware_et(_DAY, "08:45"),
        ).signal_sources[0]
        trigger_id, quote_id, completed_at = (
            _append_completed_entry_observations(journal, signal_source)
        )
        action_source, recorded_at = _confirmation_action_source(
            journal,
            signal_source,
            event_kind="LIVE_CONFIRM",
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
        return signal_source

    def _plan(self, journal: Journal, cutoff):
        replay_source, state = self._actual_snapshot(journal, cutoff)
        resolution = journal.resolve_actual_position_plan_source(
            actual_replay_source=replay_source,
            actual_position_state=state,
            symbol=_SYMBOL,
            query_cutoff=cutoff,
        )
        self.assertEqual(
            resolution.status,
            "RESOLVED",
            resolution.reason_codes,
        )
        assert resolution.source is not None
        return resolution.source

    @staticmethod
    def _rewrite_recommendation_row(
        journal: Journal,
        source,
        *,
        recommendation_id: str,
        received_at,
    ) -> None:
        received_text = journal_module._canonical_timestamp(received_at)
        source_digest = journal._close_recommendation_source_digest(
            recommendation_id=recommendation_id,
            review_source_digest=source.review_source.source_digest,
            position_plan_digest=source.position_plan_digest,
            signal_id=source.signal_id,
            opening_actual_lifecycle_id=source.opening_actual_lifecycle_id,
            opening_actual_event_id=source.opening_actual_event_id,
            opening_actual_execution_event_id=(
                source.opening_actual_execution_event_id
            ),
            initial_stop_micros=source.initial_stop_micros,
            prior_recommended_stop_micros=(
                source.prior_recommended_stop_micros
            ),
            recommended_stop_micros=source.recommended_stop_micros,
            action=source.action,
            reason_codes=source.reason_codes,
            received_at=received_text,
        )
        record_material = {
            "action": source.action,
            "position_plan_digest": source.position_plan_digest,
            "reasons_json": journal_module._canonical_json(
                list(source.reason_codes)
            ),
            "received_at": received_text,
            "recommendation_id": recommendation_id,
            "recommended_stop_micros": source.recommended_stop_micros,
            "review_id": source.review_id,
            "session_date": source.session_date.isoformat(),
            "source_digest": source_digest,
            "symbol": source.symbol,
        }
        record_sha256 = hashlib.sha256(
            journal_module._canonical_audit_json(record_material).encode(
                "utf-8"
            )
        ).hexdigest()
        journal._connection.execute(
            "DROP TRIGGER close_recommendations_no_update"
        )
        journal._connection.execute(
            "UPDATE close_recommendations SET recommendation_id = ?, "
            "received_at = ?, source_digest = ?, record_sha256 = ? "
            "WHERE id = ?",
            (
                recommendation_id,
                received_text,
                source_digest,
                record_sha256,
                source.row_id,
            ),
        )

    def test_review_persists_deterministic_multipage_receipts_and_failure(
        self,
    ) -> None:
        review_at = aware_et(_DAY, "15:30")
        cutoff = aware_et(_DAY, "15:31")
        bundle_retrieved_at = cutoff + timedelta(seconds=5)
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                receipts = (
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_HISTORICAL_QUOTES",
                        retrieved_at=review_at + timedelta(seconds=30),
                        page=3,
                        request_page_token="a-page",
                    ),
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_HISTORICAL_QUOTES",
                        retrieved_at=review_at + timedelta(seconds=20),
                        page=2,
                        terminal=False,
                        request_page_token="z-page",
                        response_next_page_token="a-page",
                    ),
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_HISTORICAL_QUOTES",
                        retrieved_at=review_at + timedelta(seconds=10),
                        page=1,
                        terminal=False,
                        response_next_page_token="z-page",
                    ),
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_INTRADAY_BARS",
                        retrieved_at=review_at + timedelta(seconds=40),
                    ),
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_DAILY_BARS",
                        retrieved_at=review_at + timedelta(seconds=50),
                    ),
                    self._market_receipt(
                        journal,
                        source_type="ALPACA_LATEST_QUOTES",
                        retrieved_at=review_at + timedelta(seconds=55),
                    ),
                )
                receipts = journal.read_source_observation_receipts(
                    tuple(receipt.row_id for receipt in receipts)
                )
                source = journal.append_actual_close_review(
                    session_date=_DAY,
                    review_at=review_at,
                    mark_cutoff=aware_et(_DAY, "15:14"),
                    query_cutoff=cutoff,
                    retrieved_at=bundle_retrieved_at,
                    receipt_bindings=(
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "SIP_QUOTE",
                            receipts[0],
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "SIP_QUOTE",
                            receipts[1],
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "SIP_QUOTE",
                            receipts[2],
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "SIP_MINUTE_BAR",
                            receipts[3],
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "SIP_DAILY_BAR",
                            receipts[4],
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "IEX_FRESHNESS",
                            receipts[5],
                        ),
                    ),
                    failure_bindings=self._required_failures(
                        cutoff,
                        receipt_scopes=(
                            (_SYMBOL, "SIP_QUOTE"),
                            (_SYMBOL, "SIP_MINUTE_BAR"),
                            (_SYMBOL, "SIP_DAILY_BAR"),
                            (_SYMBOL, "IEX_FRESHNESS"),
                        ),
                    ),
                )

                self.assertTrue(is_verified_actual_close_review_source(source))
                self.assertTrue(journal.owns_actual_close_review_source(source))
                self.assertTrue(journal.is_current_actual_close_review_source(source))
                self.assertEqual(source.expected_binding_count, 11)
                self.assertEqual(
                    tuple(binding.binding_ordinal for binding in source.bindings),
                    tuple(range(1, 12)),
                )
                self.assertEqual(
                    [
                        binding.receipt.source_uri
                        for binding in source.bindings
                        if binding.source_role == "SIP_QUOTE"
                    ],
                    [
                        receipts[2].source_uri,
                        receipts[1].source_uri,
                        receipts[0].source_uri,
                    ],
                )
                self.assertEqual(
                    [
                        binding.received_at
                        for binding in source.bindings
                        if binding.source_role == "SIP_QUOTE"
                    ],
                    [
                        receipts[2].retrieved_at,
                        receipts[1].retrieved_at,
                        receipts[0].retrieved_at,
                    ],
                )
                self.assertTrue(
                    all(
                        binding.receipt is None
                        if binding.failure_code is not None
                        else binding.receipt is not None
                        for binding in source.bindings
                    )
                )
                self.assertTrue(source.row_references)
                self.assertEqual(len(source.review_id), 64)
                self.assertEqual(len(source.source_digest), 64)
                self.assertFalse(
                    any(
                        journal.owns_source_observation_receipt(receipt)
                        for receipt in receipts
                    )
                )

                reread = journal.read_actual_close_review_source(
                    source.review_id,
                    query_cutoff=cutoff,
                )
                self.assertEqual(reread.review_id, source.review_id)
                self.assertEqual(reread.source_digest, source.source_digest)
                self.assertTrue(is_verified_actual_close_review_source(source))
                self.assertTrue(is_verified_actual_close_review_source(reread))
                self.assertFalse(
                    is_verified_actual_close_review_source(replace(reread))
                )

    def test_review_requires_every_symbol_and_global_role_result(self) -> None:
        cutoff = aware_et(_DAY, "15:31")
        required_scopes = (
            *((_SYMBOL, role) for role in _SYMBOL_ROLES),
            *((None, role) for role in _GLOBAL_ROLES),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, missing_scope in enumerate(required_scopes):
                with self.subTest(missing_scope=missing_scope), Journal.open(
                    root / f"missing-{index}.sqlite3"
                ) as journal:
                    with self.assertRaises(InvalidJournalValue):
                        journal.append_actual_close_review(
                            session_date=_DAY,
                            review_at=aware_et(_DAY, "15:30"),
                            mark_cutoff=aware_et(_DAY, "15:14"),
                            query_cutoff=cutoff,
                            retrieved_at=cutoff,
                            receipt_bindings=(),
                            failure_bindings=self._required_failures(
                                cutoff,
                                omitted_scopes=(missing_scope,),
                            ),
                        )
            with Journal.open(root / "complete.sqlite3") as journal:
                complete = journal.append_actual_close_review(
                    session_date=_DAY,
                    review_at=aware_et(_DAY, "15:30"),
                    mark_cutoff=aware_et(_DAY, "15:14"),
                    query_cutoff=cutoff,
                    retrieved_at=cutoff,
                    receipt_bindings=(),
                    failure_bindings=self._required_failures(cutoff),
                )
                self.assertEqual(complete.expected_binding_count, 9)

    def test_review_rejects_disconnected_or_inconsistent_provider_pages(
        self,
    ) -> None:
        review_at = aware_et(_DAY, "15:30")
        cutoff = aware_et(_DAY, "15:31")
        scenarios = {
            "disconnected": (
                dict(page=1, terminal=False),
                dict(page=3),
            ),
            "window-mismatch": (
                dict(page=1, terminal=False),
                dict(page=2, window_start_delta=timedelta(seconds=1)),
            ),
            "retrieval-regression": (
                dict(
                    page=1,
                    terminal=False,
                    retrieved_at=review_at + timedelta(seconds=20),
                ),
                dict(
                    page=2,
                    retrieved_at=review_at + timedelta(seconds=10),
                ),
            ),
            "duplicate-terminal": (
                dict(page=1, item_index=1),
                dict(page=1, item_index=2),
            ),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, specifications in scenarios.items():
                with self.subTest(name=name), Journal.open(
                    root / f"{name}.sqlite3"
                ) as journal:
                    created = tuple(
                        self._market_receipt(
                            journal,
                            source_type="ALPACA_HISTORICAL_QUOTES",
                            retrieved_at=specification.get(
                                "retrieved_at",
                                cutoff,
                            ),
                            **{
                                key: value
                                for key, value in specification.items()
                                if key != "retrieved_at"
                            },
                        )
                        for index, specification in enumerate(specifications)
                    )
                    receipts = journal.read_source_observation_receipts(
                        tuple(receipt.row_id for receipt in created)
                    )
                    with self.assertRaises(InvalidJournalValue):
                        journal.append_actual_close_review(
                            session_date=_DAY,
                            review_at=review_at,
                            mark_cutoff=aware_et(_DAY, "15:14"),
                            query_cutoff=cutoff,
                            retrieved_at=cutoff,
                            receipt_bindings=tuple(
                                ActualCloseReceiptBinding(
                                    _SYMBOL,
                                    "SIP_QUOTE",
                                    receipt,
                                )
                                for receipt in receipts
                            ),
                            failure_bindings=self._required_failures(
                                cutoff,
                                receipt_scopes=((_SYMBOL, "SIP_QUOTE"),),
                            ),
                        )

    def test_review_rejects_post_review_reference_time_and_spoofed_sec_uri(
        self,
    ) -> None:
        review_at = aware_et(_DAY, "15:30")
        cutoff = aware_et(_DAY, "15:31")
        common_details = {
            "accession": None,
            "issuer_cik": None,
            "source_observation_id": "official-halt-post-review",
            "source_role": "PRIMARY_HALT_FEED",
            "symbol": None,
            "timestamp_source": "PRIMARY_METADATA",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with Journal.open(root / "post-review.sqlite3") as journal:
                receipt = journal.append_source_observation_receipt(
                    payload=b"<rss><channel /></rss>",
                    source_uri=(
                        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
                    ),
                    source_type="OFFICIAL_REFERENCE",
                    provider="Nasdaq",
                    feed="PRIMARY_METADATA",
                    source_time=review_at + timedelta(seconds=1),
                    retrieved_at=cutoff,
                    provider_sequence=None,
                    delay_seconds=59,
                    health_result="OK",
                    details=common_details,
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.append_actual_close_review(
                        session_date=_DAY,
                        review_at=review_at,
                        mark_cutoff=aware_et(_DAY, "15:14"),
                        query_cutoff=cutoff,
                        retrieved_at=cutoff,
                        receipt_bindings=(
                            ActualCloseReceiptBinding(
                                None,
                                "PRIMARY_HALT_FEED",
                                receipt,
                            ),
                        ),
                        failure_bindings=self._required_failures(
                            cutoff,
                            receipt_scopes=((None, "PRIMARY_HALT_FEED"),),
                        ),
                    )

            accession = "0000320193-26-000001"
            accession_path = accession.replace("-", "")
            path = (
                "/Archives/edgar/data/320193/"
                f"{accession_path}/aapl-20260814.htm"
            )
            spoofed_uris = (
                f"http://www.sec.gov{path}",
                f"https://attacker@www.sec.gov{path}",
                f"https://www.sec.gov:444{path}",
                f"https://www.sec.gov{path}?download=1",
                f"https://www.sec.gov{path}#fragment",
                f"https://www.sec.gov/evil{path}",
            )
            for index, source_uri in enumerate(spoofed_uris):
                with self.subTest(source_uri=source_uri), Journal.open(
                    root / f"sec-{index}.sqlite3"
                ) as journal:
                    receipt = journal.append_source_observation_receipt(
                        payload=b"<html>filing</html>",
                        source_uri=source_uri,
                        source_type="SEC_ARCHIVE",
                        provider="U.S. Securities and Exchange Commission",
                        feed="SEC_FILING_METADATA",
                        source_time=review_at,
                        retrieved_at=cutoff,
                        provider_sequence=None,
                        delay_seconds=60,
                        health_result="OK",
                        details={
                            "accession": accession,
                            "issuer_cik": "0000320193",
                            "source_observation_id": f"sec-archive-{index}",
                            "source_role": None,
                            "symbol": _SYMBOL,
                            "timestamp_source": "SEC_FILING_METADATA",
                        },
                    )
                    with self.assertRaises(InvalidJournalValue):
                        journal.append_actual_close_review(
                            session_date=_DAY,
                            review_at=review_at,
                            mark_cutoff=aware_et(_DAY, "15:14"),
                            query_cutoff=cutoff,
                            retrieved_at=cutoff,
                            receipt_bindings=(
                                ActualCloseReceiptBinding(
                                    _SYMBOL,
                                    "EVENT_EVIDENCE",
                                    receipt,
                                ),
                            ),
                            failure_bindings=self._required_failures(
                                cutoff,
                                receipt_scopes=(
                                    (_SYMBOL, "EVENT_EVIDENCE"),
                                ),
                            ),
                        )
            with Journal.open(root / "sec-valid.sqlite3") as journal:
                receipt = journal.append_source_observation_receipt(
                    payload=b"<html>filing</html>",
                    source_uri=f"https://www.sec.gov{path}",
                    source_type="SEC_ARCHIVE",
                    provider="U.S. Securities and Exchange Commission",
                    feed="SEC_FILING_METADATA",
                    source_time=review_at,
                    retrieved_at=cutoff,
                    provider_sequence=None,
                    delay_seconds=60,
                    health_result="OK",
                    details={
                        "accession": accession,
                        "issuer_cik": "0000320193",
                        "source_observation_id": "sec-archive-valid",
                        "source_role": None,
                        "symbol": _SYMBOL,
                        "timestamp_source": "SEC_FILING_METADATA",
                    },
                )
                source = journal.append_actual_close_review(
                    session_date=_DAY,
                    review_at=review_at,
                    mark_cutoff=aware_et(_DAY, "15:14"),
                    query_cutoff=cutoff,
                    retrieved_at=cutoff,
                    receipt_bindings=(
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "EVENT_EVIDENCE",
                            receipt,
                        ),
                    ),
                    failure_bindings=self._required_failures(
                        cutoff,
                        receipt_scopes=((_SYMBOL, "EVENT_EVIDENCE"),),
                    ),
                )
                self.assertEqual(
                    tuple(
                        binding.source_observation_id
                        for binding in source.bindings
                        if binding.source_role == "EVENT_EVIDENCE"
                    ),
                    (receipt.row_id,),
                )

    def test_review_rejects_role_scope_copy_cross_owner_and_lookahead(self) -> None:
        cutoff = aware_et(_DAY, "15:31")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with Journal.open(root / "first.sqlite3") as first, Journal.open(
                root / "second.sqlite3"
            ) as second:
                receipt = self._market_receipt(
                    first,
                    source_type="ALPACA_HISTORICAL_QUOTES",
                    retrieved_at=cutoff,
                )
                base = dict(
                    session_date=_DAY,
                    review_at=aware_et(_DAY, "15:30"),
                    mark_cutoff=aware_et(_DAY, "15:14"),
                    query_cutoff=cutoff,
                    retrieved_at=cutoff,
                    failure_bindings=(),
                )
                for binding in (
                    ActualCloseReceiptBinding(_SYMBOL, "IEX_FRESHNESS", receipt),
                    ActualCloseReceiptBinding(None, "SIP_QUOTE", receipt),
                    ActualCloseReceiptBinding(
                        _SYMBOL,
                        "SIP_QUOTE",
                        replace(receipt),
                    ),
                ):
                    with self.subTest(binding=binding), self.assertRaises(
                        InvalidJournalValue
                    ):
                        first.append_actual_close_review(
                            **base,
                            receipt_bindings=(binding,),
                        )
                with self.assertRaises(InvalidJournalValue):
                    second.append_actual_close_review(
                        **base,
                        receipt_bindings=(
                            ActualCloseReceiptBinding(
                                _SYMBOL,
                                "SIP_QUOTE",
                                receipt,
                            ),
                        ),
                    )
                with self.assertRaises(InvalidJournalValue):
                    first.append_actual_close_review(
                        **{**base, "query_cutoff": cutoff - timedelta(seconds=1)},
                        receipt_bindings=(
                            ActualCloseReceiptBinding(
                                _SYMBOL,
                                "SIP_QUOTE",
                                receipt,
                            ),
                        ),
                    )
                with self.assertRaises(InvalidJournalValue):
                    first.append_actual_close_review(
                        **{
                            **base,
                            "failure_bindings": (
                                ActualCloseFailureBinding(
                                    _SYMBOL,
                                    "SIP_QUOTE",
                                    "SOURCE_UNAVAILABLE",
                                    cutoff,
                                ),
                            ),
                        },
                        receipt_bindings=(
                            ActualCloseReceiptBinding(
                                _SYMBOL,
                                "SIP_QUOTE",
                                receipt,
                            ),
                        ),
                    )
                boundary_day = date(2026, 8, 15)
                with self.assertRaises(InvalidJournalValue):
                    first.append_actual_close_review(
                        session_date=boundary_day,
                        mark_cutoff=datetime(2026, 8, 15, 0, 10, tzinfo=UTC),
                        review_at=datetime(2026, 8, 15, 0, 20, tzinfo=UTC),
                        query_cutoff=datetime(2026, 8, 15, 0, 30, tzinfo=UTC),
                        retrieved_at=datetime(2026, 8, 15, 0, 30, tzinfo=UTC),
                        receipt_bindings=(),
                        failure_bindings=(
                            ActualCloseFailureBinding(
                                None,
                                "OPERATIONAL_STATUS",
                                "SOURCE_UNAVAILABLE",
                                datetime(2026, 8, 15, 0, 30, tzinfo=UTC),
                            ),
                        ),
                    )

    def test_review_binds_exact_global_reference_and_symbol_event_sources(
        self,
    ) -> None:
        cutoff = aware_et(_DAY, "15:31")
        source_time = cutoff - timedelta(minutes=1)
        common_details = {
            "accession": None,
            "issuer_cik": None,
            "source_observation_id": "official-halt-1",
            "source_role": "PRIMARY_HALT_FEED",
            "symbol": None,
            "timestamp_source": "PRIMARY_METADATA",
        }
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                official = journal.append_source_observation_receipt(
                    payload=b"<rss><channel /></rss>",
                    source_uri=(
                        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
                    ),
                    source_type="OFFICIAL_REFERENCE",
                    provider="Nasdaq",
                    feed="PRIMARY_METADATA",
                    source_time=source_time,
                    retrieved_at=cutoff,
                    provider_sequence=None,
                    delay_seconds=60,
                    health_result="OK",
                    details=common_details,
                )
                event = journal.append_source_observation_receipt(
                    payload=b'{"filings":[]}',
                    source_uri=(
                        "https://data.sec.gov/submissions/"
                        "CIK0000320193.json"
                    ),
                    source_type="SEC_SUBMISSIONS",
                    provider="U.S. Securities and Exchange Commission",
                    feed="SEC_SUBMISSIONS_METADATA",
                    source_time=source_time,
                    retrieved_at=cutoff,
                    provider_sequence=None,
                    delay_seconds=60,
                    health_result="OK",
                    details={
                        "accession": None,
                        "issuer_cik": "0000320193",
                        "source_observation_id": "sec-submissions-aapl-1",
                        "source_role": None,
                        "symbol": _SYMBOL,
                        "timestamp_source": "SEC_SUBMISSIONS_METADATA",
                    },
                )
                official, event = journal.read_source_observation_receipts(
                    (official.row_id, event.row_id)
                )
                source = journal.append_actual_close_review(
                    session_date=_DAY,
                    review_at=aware_et(_DAY, "15:30"),
                    mark_cutoff=aware_et(_DAY, "15:14"),
                    query_cutoff=cutoff,
                    retrieved_at=cutoff,
                    receipt_bindings=(
                        ActualCloseReceiptBinding(
                            None,
                            "PRIMARY_HALT_FEED",
                            official,
                        ),
                        ActualCloseReceiptBinding(
                            _SYMBOL,
                            "EVENT_EVIDENCE",
                            event,
                        ),
                    ),
                    failure_bindings=self._required_failures(
                        cutoff,
                        receipt_scopes=(
                            (_SYMBOL, "EVENT_EVIDENCE"),
                            (None, "PRIMARY_HALT_FEED"),
                        ),
                    ),
                )
                self.assertEqual(
                    tuple(
                        binding.source_role
                        for binding in source.bindings
                        if binding.receipt is not None
                    ),
                    ("EVENT_EVIDENCE", "PRIMARY_HALT_FEED"),
                )

                malformed = journal.append_source_observation_receipt(
                    payload=b"<rss><channel /></rss><bad />",
                    source_uri=(
                        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
                    ),
                    source_type="OFFICIAL_REFERENCE",
                    provider="Nasdaq",
                    feed="PRIMARY_METADATA",
                    source_time=source_time,
                    retrieved_at=cutoff,
                    provider_sequence=None,
                    delay_seconds=60,
                    health_result="OK",
                    details={**common_details, "symbol": _SYMBOL},
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.append_actual_close_review(
                        session_date=_DAY,
                        review_at=aware_et(_DAY, "15:30"),
                        mark_cutoff=aware_et(_DAY, "15:14"),
                        query_cutoff=cutoff,
                        retrieved_at=cutoff,
                        receipt_bindings=(
                            ActualCloseReceiptBinding(
                                None,
                                "PRIMARY_HALT_FEED",
                                malformed,
                            ),
                        ),
                        failure_bindings=(),
                    )

    def test_recommendations_derive_plan_identity_and_keep_stop_monotonic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Journal.open(Path(directory) / "journal.sqlite3") as journal:
                fixture = self._composition_fixture()
                signal, first_cutoff = fixture._seed_linked_open_position(
                    journal
                )
                initial = signal.recommended_stop_micros
                first_review_at = aware_et(_DAY, "15:30")
                first_decision, first_plan, _history = self._issued_decision(
                    fixture,
                    journal,
                    signal,
                    review_at=first_review_at,
                    query_cutoff=first_cutoff,
                    sip_bid="21.00",
                    sip_ask="21.02",
                    iex_bid="20.99",
                    iex_ask="21.03",
                )
                self.assertEqual(first_decision.action, "TIGHTEN_STOP")
                first = journal.append_close_recommendation(first_decision)
                self.assertTrue(is_verified_close_recommendation_source(first))
                self.assertEqual(first.symbol, _SYMBOL)
                self.assertEqual(
                    first.position_plan_digest,
                    first_plan.position_plan_digest,
                )
                self.assertEqual(first.signal_id, signal.signal_id)
                self.assertEqual(first.initial_stop_micros, initial)
                self.assertEqual(first.prior_recommended_stop_micros, None)
                recommendation_count = journal.count("close_recommendations")
                retry_decision = self._reissued_decision(
                    fixture,
                    journal,
                    first,
                    review_at=first_review_at,
                    query_cutoff=first_cutoff,
                )
                self.assertEqual(retry_decision.action, "TIGHTEN_STOP")
                retry = journal.append_close_recommendation(retry_decision)
                self.assertEqual(retry.recommendation_id, first.recommendation_id)
                self.assertEqual(
                    journal.count("close_recommendations"),
                    recommendation_count,
                )

                self.assertTrue(is_verified_close_recommendation_source(first))
                latest_plan = fixture._final_plan(journal, first_cutoff)
                history = journal.read_latest_close_recommendation_source(
                    position_plan_source=latest_plan,
                    query_cutoff=first_cutoff,
                )
                self.assertTrue(
                    is_verified_latest_close_recommendation_source(history)
                )
                self.assertEqual(
                    history.eligible_recommendation_ids,
                    (first.recommendation_id,),
                )
                latest = history.recommendation
                assert latest is not None
                self.assertEqual(latest.recommendation_id, first.recommendation_id)
                self.assertEqual(
                    latest.recommended_stop_micros,
                    first.recommended_stop_micros,
                )
                self.assertTrue(journal.owns_close_recommendation_source(latest))
                self.assertTrue(journal.is_current_close_recommendation_source(latest))
                self.assertFalse(
                    is_verified_close_recommendation_source(replace(latest))
                )

    def test_recommendation_rejects_unissued_inputs_and_as_of_lookahead(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with Journal.open(root / "first.sqlite3") as journal:
                fixture = self._composition_fixture()
                signal, query_cutoff = fixture._seed_linked_open_position(
                    journal
                )
                decision, _plan, _history = self._issued_decision(
                    fixture,
                    journal,
                    signal,
                    review_at=aware_et(_DAY, "15:30"),
                    query_cutoff=query_cutoff,
                )
                with self.assertRaises(InvalidJournalValue):
                    journal.append_close_recommendation(object())
                with self.assertRaises(InvalidJournalValue):
                    journal.append_close_recommendation(replace(decision))
                with Journal.open(root / "second.sqlite3") as second:
                    with self.assertRaises(InvalidJournalValue):
                        second.append_close_recommendation(decision)

                source = journal.append_close_recommendation(decision)
                before = source.received_at - timedelta(microseconds=1)
                before_plan = fixture._final_plan(journal, before)
                absent = journal.read_latest_close_recommendation_source(
                    position_plan_source=before_plan,
                    query_cutoff=before,
                )
                self.assertIsNone(absent.recommendation)
                self.assertEqual(absent.eligible_recommendation_ids, ())
                self.assertTrue(
                    is_verified_latest_close_recommendation_source(absent)
                )
                with Journal.open(root / "second.sqlite3") as second:
                    self.assertFalse(second.owns_close_recommendation_source(source))
                    self.assertFalse(
                        second.owns_latest_close_recommendation_source(absent)
                    )
                with self.assertRaises(InvalidJournalValue):
                    journal.read_close_recommendation_source(
                        source.recommendation_id,
                        query_cutoff=before,
                    )
                journal.append_source_observation(
                    payload=b'{"later":true}',
                    source_uri="https://example.invalid/later",
                    source_type="TEST",
                    provider="fixture",
                    feed=None,
                    source_time=source.received_at + timedelta(seconds=1),
                    retrieved_at=source.received_at + timedelta(seconds=1),
                    provider_sequence=None,
                    delay_seconds=0,
                    health_result="OK",
                    details={"kind": "later"},
                )
                self.assertFalse(is_verified_close_recommendation_source(source))
                self.assertFalse(
                    is_verified_latest_close_recommendation_source(absent)
                )

    def test_recommendation_readback_recomputes_identity_and_review_window(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = self._composition_fixture()
            with Journal.open(root / "forged-id.sqlite3") as journal:
                signal, query_cutoff = fixture._seed_linked_open_position(
                    journal
                )
                decision, _plan, _history = self._issued_decision(
                    fixture,
                    journal,
                    signal,
                    review_at=aware_et(_DAY, "15:30"),
                    query_cutoff=query_cutoff,
                )
                source = journal.append_close_recommendation(decision)
                forged_id = "f" * 64
                self.assertNotEqual(forged_id, source.recommendation_id)
                self._rewrite_recommendation_row(
                    journal,
                    source,
                    recommendation_id=forged_id,
                    received_at=source.received_at,
                )
                with self.assertRaises(MigrationCorruption):
                    journal.read_close_recommendation_source(
                        forged_id,
                        query_cutoff=source.query_cutoff,
                    )

            with Journal.open(root / "forged-time.sqlite3") as journal:
                signal, query_cutoff = fixture._seed_linked_open_position(
                    journal
                )
                decision, _plan, _history = self._issued_decision(
                    fixture,
                    journal,
                    signal,
                    review_at=aware_et(_DAY, "15:30"),
                    query_cutoff=query_cutoff,
                )
                source = journal.append_close_recommendation(decision)
                forged_received_at = (
                    source.review_source.review_at + timedelta(seconds=1)
                )
                identity_material = {
                    "action": source.action,
                    "position_plan_digest": source.position_plan_digest,
                    "reason_codes": list(source.reason_codes),
                    "received_at": journal_module._canonical_timestamp(
                        forged_received_at
                    ),
                    "recommended_stop_micros": source.recommended_stop_micros,
                    "review_id": source.review_id,
                    "session_date": source.session_date.isoformat(),
                    "symbol": source.symbol,
                }
                forged_id = journal_module._actual_close_semantic_digest(
                    "stock-monitor/close-recommendation-id/v1",
                    identity_material,
                )
                self._rewrite_recommendation_row(
                    journal,
                    source,
                    recommendation_id=forged_id,
                    received_at=forged_received_at,
                )
                with self.assertRaises(MigrationCorruption):
                    journal.read_close_recommendation_source(
                        forged_id,
                        query_cutoff=source.query_cutoff,
                    )

    def test_recommendation_api_only_accepts_an_issued_decision(self) -> None:
        documentation = inspect.getdoc(Journal.append_close_recommendation)
        assert documentation is not None
        self.assertIn("issued ACTUAL close decision authority", documentation)
        parameters = tuple(
            inspect.signature(Journal.append_close_recommendation).parameters
        )
        self.assertEqual(parameters, ("self", "decision_source"))


if __name__ == "__main__":
    unittest.main()
