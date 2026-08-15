from __future__ import annotations

import hashlib
import json
import pickle
import unittest
import tempfile
from copy import copy, deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from stock_monitor.providers import reference as reference_module
from stock_monitor.providers.cache import ContentCache, SourceDocument
from stock_monitor.providers.http import (
    EgressPolicy,
    HttpResponse,
    NetworkPolicyError,
    ProviderResponseError,
)
from stock_monitor.providers.reference import ReferenceClient, verify_exchange_status
from tests.support import FixtureTransport, reference_fixture


AS_OF = datetime(2026, 8, 14, 12, 45, tzinfo=UTC)


class StaticReferenceTransport:
    def __init__(
        self,
        body: bytes = b'{"status":"OPEN"}',
        content_type: str = "application/json",
    ) -> None:
        self.requested_urls: list[str] = []
        self.body = body
        self.content_type = content_type

    def get(self, url: str, headers) -> HttpResponse:
        self.requested_urls.append(url)
        return HttpResponse(
            200,
            (("Content-Type", self.content_type),),
            self.body,
            url,
        )


class RedirectReferenceTransport:
    def __init__(self, target: str) -> None:
        self.target = target
        self.requested_urls: list[str] = []

    def get(self, url: str, headers) -> HttpResponse:
        self.requested_urls.append(url)
        return HttpResponse(
            302,
            (("Location", self.target), ("Content-Type", "text/plain")),
            b"redirect",
            url,
        )


def status_observation(
    status: str,
    observation_id: str,
    *,
    retrieved_at: datetime = AS_OF,
    healthy: bool = True,
) -> dict[str, object]:
    return {
        "status": status,
        "healthy": healthy,
        "source_observation_id": observation_id,
        "retrieved_at": retrieved_at.isoformat().replace("+00:00", "Z"),
    }


def source_document(
    role: str,
    url: str,
    observation_id: str,
    *,
    retrieved_at: datetime = AS_OF,
) -> SourceDocument:
    body = f"{role}:{observation_id}".encode("ascii")
    return SourceDocument(
        url=url,
        published_at=None,
        retrieved_at=retrieved_at,
        content_hash=hashlib.sha256(body).hexdigest(),
        body=body,
        source_observation_id=observation_id,
        publisher="Official source",
        source_type="OFFICIAL_REFERENCE",
        timestamp_source="UNAVAILABLE",
        source_role=role,
    )


def structured_source_document(
    role: str,
    url: str,
    observation_id: str,
    payload: dict[str, object],
    *,
    retrieved_at: datetime = AS_OF,
) -> SourceDocument:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    return SourceDocument(
        url=url,
        published_at=None,
        retrieved_at=retrieved_at,
        content_hash=hashlib.sha256(body).hexdigest(),
        body=body,
        source_observation_id=observation_id,
        publisher="Official source",
        source_type="OFFICIAL_REFERENCE",
        timestamp_source="PRIMARY_METADATA",
        source_role=role,
    )


def bound_status_observation(
    role: str,
    status: str,
    url: str,
    observation_id: str,
    *,
    retrieved_at: datetime = AS_OF,
    healthy: bool = True,
) -> object:
    document = structured_source_document(
        role,
        url,
        observation_id,
        {
            "schema_version": 1,
            "kind": "REFERENCE_STATUS",
            "role": role,
            "status": status,
            "healthy": healthy,
            "source_observation_id": observation_id,
            "retrieved_at": retrieved_at.isoformat().replace("+00:00", "Z"),
        },
        retrieved_at=retrieved_at,
    )
    return reference_module.ReferenceStatusSnapshot(
        role=role,
        status=status,
        document=document,
        healthy=healthy,
    )


def parsed_status_observation(
    role: str,
    url: str,
    *,
    retrieved_at: datetime = AS_OF,
) -> object:
    if role == "TRADER_ALERT_HALT":
        body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<title>Nasdaq Equity Trader Alerts</title>"
            b"<ndaq:numItems>0</ndaq:numItems>"
            b"</channel></rss>"
        )
        content_type = "application/xml"
    elif role == "OPERATIONAL_STATUS":
        body = b"[]"
        content_type = "application/json"
    else:
        body = b"<html><body>Official HTML is not machine-normalized.</body></html>"
        content_type = "text/html"
    client = ReferenceClient(
        StaticReferenceTransport(body, content_type),
        EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
        allowed_urls={url},
        source_roles={url: role},
        now=lambda: retrieved_at,
    )
    return client.parse_status(client.fetch(url))


def complete_status_observations(
    *, primary_retrieved_at: datetime = AS_OF
) -> dict[str, object]:
    return {
        "primary": parsed_status_observation(
            "PRIMARY_CALENDAR",
            "https://www.nyse.com/trade/hours-calendars",
            retrieved_at=primary_retrieved_at,
        ),
        "cross_check": parsed_status_observation(
            "CROSS_CHECK_CALENDAR",
            "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        ),
        "operational_status": parsed_status_observation(
            "OPERATIONAL_STATUS",
            "https://www.nyse.com/api/notifications/public/alerts?2=3",
        ),
        "trader_alert": parsed_status_observation(
            "TRADER_ALERT_HALT",
            "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
        ),
    }


def configured_roles(*urls: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for url in urls:
        if "feed=tradehalts" in url:
            role = "PRIMARY_HALT_FEED"
        elif "feed=currentheadlines" in url:
            role = "TRADER_ALERT_HALT"
        elif "nasdaqtrader.com" in url:
            role = "CROSS_CHECK_CALENDAR"
        elif "/api/notifications/public/alerts" in url:
            role = "OPERATIONAL_STATUS"
        else:
            role = "PRIMARY_CALENDAR"
        result[url] = role
    return result


def halt_snapshot(
    *,
    observation_id: str,
    configured_url: str,
    origin: str,
    venue: str,
    scope: str = "ACTIVE_HALTS",
    coverage: str = "COMPLETE_ACTIVE_HALTS",
    halted_symbols: tuple[str, ...] = (),
    healthy: bool = True,
    pagination_complete: bool = True,
    retrieved_at: datetime = AS_OF - timedelta(minutes=1),
    valid_until: datetime = AS_OF + timedelta(minutes=4),
) -> object:
    if "feed=tradehalts" in configured_url:
        role = "PRIMARY_HALT_FEED"
    elif "feed=currentheadlines" in configured_url:
        role = "TRADER_ALERT_HALT"
    else:
        role = "OPERATIONAL_STATUS"
    document = structured_source_document(
        role,
        configured_url,
        observation_id,
        {
            "schema_version": 1,
            "kind": "HALT_FEED",
            "role": role,
            "venue": venue,
            "scope": scope,
            "source_observation_id": observation_id,
            "retrieved_at": retrieved_at.isoformat().replace("+00:00", "Z"),
            "valid_until": valid_until.isoformat().replace("+00:00", "Z"),
            "healthy": healthy,
            "pagination_complete": pagination_complete,
            "coverage": coverage,
            "halted_symbols": list(halted_symbols),
        },
        retrieved_at=retrieved_at,
    )
    return reference_module.HaltFeedSnapshot(
        configured_url=configured_url,
        observed_url=configured_url,
        origin=origin,
        venue=venue,
        scope=scope,
        source_observation_id=observation_id,
        retrieved_at=retrieved_at,
        valid_until=valid_until,
        healthy=healthy,
        pagination_complete=pagination_complete,
        coverage=coverage,
        halted_symbols=halted_symbols,
        document=document,
    )


def parsed_halt_snapshot(
    role: str,
    url: str,
    *,
    halted_symbols: tuple[str, ...] = (),
    operational_alert: bool = False,
    retrieved_at: datetime = AS_OF - timedelta(minutes=1),
) -> object:
    if role == "PRIMARY_HALT_FEED":
        items = b"".join(
            b"<item><ndaq:IssueSymbol>"
            + symbol.encode("ascii")
            + b"</ndaq:IssueSymbol></item>"
            for symbol in halted_symbols
        )
        body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            + f"<ndaq:numItems>{len(halted_symbols)}</ndaq:numItems>".encode("ascii")
            + items
            + b"</channel></rss>"
        )
        content_type = "application/xml"
    elif role == "TRADER_ALERT_HALT":
        items = b"".join(
            b"<item><title>ETA2026-"
            + f"{index:02d}".encode("ascii")
            + b" - Notice mentioning "
            + symbol.encode("ascii")
            + b"</title><pubDate>Mon, 10 Aug 2026 21:00:00 GMT</pubDate>"
            + b"<link>http://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2026-"
            + f"{index:02d}".encode("ascii")
            + b"</link><ndaq:NewsCategory>Equity Trader Alert</ndaq:NewsCategory>"
            + b"<ndaq:Alert>#2026-"
            + f"{index:03d}".encode("ascii")
            + b"</ndaq:Alert><ndaq:Markets>The Nasdaq Stock Market</ndaq:Markets>"
            + b"<ndaq:WhatYouNeedToKnow /><description>Reviewed notice.</description>"
            + b"</item>"
            for index, symbol in enumerate(halted_symbols, start=1)
        )
        body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            + b"<title>Nasdaq Equity Trader Alerts</title>"
            + f"<ndaq:numItems>{len(halted_symbols)}</ndaq:numItems>".encode("ascii")
            + items
            + b"</channel></rss>"
        )
        content_type = "application/xml"
    else:
        body = b"[2]" if operational_alert else b"[]"
        content_type = "application/json"
    client = ReferenceClient(
        StaticReferenceTransport(body, content_type),
        EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
        allowed_urls={url},
        source_roles={url: role},
        now=lambda: retrieved_at,
    )
    return client.parse_halt_feed(client.fetch(url))


def complete_halt_observations(
    *, halted_symbols: tuple[str, ...] = ()
) -> dict[str, object]:
    return {
        "primary_halt_feed": parsed_halt_snapshot(
            "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            halted_symbols=halted_symbols,
        ),
        "operational_status": parsed_halt_snapshot(
            "OPERATIONAL_STATUS",
            "https://www.nyse.com/api/notifications/public/alerts?2=3",
        ),
        "cross_check_halt_feed": parsed_halt_snapshot(
            "TRADER_ALERT_HALT",
            "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
        ),
    }


class ReferenceContractTests(unittest.TestCase):
    def test_direct_or_replaced_snapshots_cannot_authorize_status_or_clear(self) -> None:
        direct_status = {
            "primary": bound_status_observation(
                "PRIMARY_CALENDAR",
                "OPEN",
                "https://www.nyse.com/trade/hours-calendars",
                "direct-primary",
            ),
            "cross_check": bound_status_observation(
                "CROSS_CHECK_CALENDAR",
                "OPEN",
                "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
                "direct-cross",
            ),
            "operational_status": bound_status_observation(
                "OPERATIONAL_STATUS",
                "OPEN",
                "https://www.nyse.com/api/notifications/public/alerts?2=3",
                "direct-operational",
            ),
            "trader_alert": bound_status_observation(
                "TRADER_ALERT_HALT",
                "UNKNOWN",
                "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
                "direct-alert",
            ),
        }
        direct_exchange = verify_exchange_status(direct_status, as_of=AS_OF)
        self.assertEqual(
            direct_exchange.block_reason,
            "REFERENCE_SNAPSHOT_UNVERIFIED",
        )

        direct_halts = {
            "primary_halt_feed": halt_snapshot(
                observation_id="direct-halt-primary",
                configured_url="https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
                origin="https://www.nasdaqtrader.com",
                venue="ALL_US",
            ),
            "operational_status": halt_snapshot(
                observation_id="direct-halt-operational",
                configured_url="https://www.nyse.com/api/notifications/public/alerts?2=3",
                origin="https://www.nyse.com",
                venue="NYSE",
                scope="EXCHANGE_OPERATIONAL_STATUS",
            ),
            "cross_check_halt_feed": halt_snapshot(
                observation_id="direct-halt-cross",
                configured_url="https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
                origin="https://www.nasdaqtrader.com",
                venue="ALL_US",
            ),
        }
        direct_instrument = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            direct_halts,
            as_of=AS_OF,
        )
        self.assertEqual(direct_instrument.halt_status, "UNKNOWN")
        self.assertEqual(
            direct_instrument.block_reason,
            "HALT_SNAPSHOT_UNVERIFIED",
        )

        url = "https://www.nyse.com/trade/hours-calendars"
        client = ReferenceClient(
            StaticReferenceTransport(),
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles={url: "PRIMARY_CALENDAR"},
            now=lambda: AS_OF,
        )
        document = client.fetch(url)
        parsed = client.parse_status(document)
        self.assertEqual(parsed.status, "UNSUPPORTED")
        for copied in (
            replace(parsed),
            copy(parsed),
            deepcopy(parsed),
            pickle.loads(pickle.dumps(parsed)),
        ):
            observations = complete_status_observations()
            observations["primary"] = copied
            self.assertEqual(
                verify_exchange_status(observations, as_of=AS_OF).block_reason,
                "REFERENCE_SNAPSHOT_UNVERIFIED",
            )

        duplicate_status = {
            name: parsed
            for name in (
                "primary",
                "cross_check",
                "operational_status",
                "trader_alert",
            )
        }
        self.assertEqual(
            verify_exchange_status(duplicate_status, as_of=AS_OF).block_reason,
            "REFERENCE_SOURCE_IDENTITY_CONFLICT",
        )

        halt_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        halt_body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>SPY</ndaq:IssueSymbol></item>"
            b"</channel></rss>"
        )
        halt_client = ReferenceClient(
            StaticReferenceTransport(halt_body, "application/xml"),
            EgressPolicy({"www.nasdaqtrader.com"}),
            allowed_urls={halt_url},
            source_roles={halt_url: "PRIMARY_HALT_FEED"},
            now=lambda: AS_OF,
        )
        halt_parsed = halt_client.parse_halt_feed(halt_client.fetch(halt_url))
        for copied in (
            replace(halt_parsed),
            copy(halt_parsed),
            deepcopy(halt_parsed),
            pickle.loads(pickle.dumps(halt_parsed)),
        ):
            copied_halts = {
                "primary_halt_feed": copied,
                "operational_status": complete_halt_observations()[
                    "operational_status"
                ],
                "cross_check_halt_feed": complete_halt_observations()[
                    "cross_check_halt_feed"
                ],
            }
            copied_decision = reference_module.classify_instrument_status(
                "SPY",
                "NYSE",
                copied_halts,
                as_of=AS_OF,
            )
            self.assertEqual(copied_decision.halt_status, "UNKNOWN")
            self.assertEqual(
                copied_decision.block_reason,
                "HALT_SNAPSHOT_UNVERIFIED",
            )

    def test_recomputed_snapshot_digests_cannot_hide_in_place_tampering(self) -> None:
        status = parsed_status_observation(
            "PRIMARY_CALENDAR",
            "https://www.nyse.com/trade/hours-calendars",
        )
        object.__setattr__(status, "status", "OPEN")
        object.__setattr__(status, "healthy", True)
        object.__setattr__(status, "supported", True)
        object.__setattr__(
            status,
            "_snapshot_digest",
            reference_module._status_fingerprint(status),
        )
        status_observations = complete_status_observations()
        status_observations["primary"] = status
        self.assertEqual(
            verify_exchange_status(status_observations, as_of=AS_OF).block_reason,
            "REFERENCE_SNAPSHOT_UNVERIFIED",
        )

        halt = parsed_halt_snapshot(
            "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            halted_symbols=("SPY",),
        )
        object.__setattr__(halt, "halted_symbols", ())
        object.__setattr__(
            halt,
            "_snapshot_digest",
            reference_module._halt_fingerprint(halt),
        )
        halt_observations = complete_halt_observations()
        halt_observations["primary_halt_feed"] = halt
        decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            halt_observations,
            as_of=AS_OF,
        )
        self.assertEqual(decision.halt_status, "UNKNOWN")
        self.assertEqual(decision.block_reason, "HALT_SNAPSHOT_UNVERIFIED")

    def test_fetched_document_lineage_rejects_in_place_mutation_before_parsing(self) -> None:
        primary_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        alert_url = (
            "https://www.nasdaqtrader.com/"
            "rss.aspx?categorylist=2&feed=currentheadlines"
        )
        halted_body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>SPY</ndaq:IssueSymbol></item>"
            b"</channel></rss>"
        )
        empty_body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>0</ndaq:numItems></channel></rss>"
        )

        def fetched() -> tuple[ReferenceClient, SourceDocument]:
            client = ReferenceClient(
                StaticReferenceTransport(halted_body, "application/xml"),
                EgressPolicy({"www.nasdaqtrader.com"}),
                allowed_urls={primary_url, alert_url},
                source_roles={
                    primary_url: "PRIMARY_HALT_FEED",
                    alert_url: "TRADER_ALERT_HALT",
                },
                now=lambda: AS_OF,
            )
            return client, client.fetch(primary_url)

        mutation_sets = (
            {
                "body": empty_body,
                "content_hash": hashlib.sha256(empty_body).hexdigest(),
            },
            {
                "url": alert_url,
                "source_role": "TRADER_ALERT_HALT",
                "body": empty_body,
                "content_hash": hashlib.sha256(empty_body).hexdigest(),
            },
            {"source_type": "SEC_ARCHIVE"},
            {"retrieved_at": AS_OF - timedelta(minutes=1)},
        )
        for mutations in mutation_sets:
            client, document = fetched()
            for field_name, replacement in mutations.items():
                object.__setattr__(document, field_name, replacement)
            with self.subTest(fields=tuple(mutations)), self.assertRaises(ValueError):
                client.parse_halt_feed(document)

    def test_decision_outputs_are_sealed_against_copy_and_in_place_tampering(self) -> None:
        exchange = verify_exchange_status(complete_status_observations(), as_of=AS_OF)
        self.assertTrue(
            reference_module.is_reviewed_exchange_status_decision(exchange)
        )
        direct_exchange = reference_module.ExchangeStatusDecision(
            status=exchange.status,
            block_reason=exchange.block_reason,
            as_of=exchange.as_of,
            valid_until=exchange.valid_until,
            source_observation_ids=exchange.source_observation_ids,
            retrieved_at=exchange.retrieved_at,
        )
        self.assertFalse(
            reference_module.is_reviewed_exchange_status_decision(direct_exchange)
        )
        for copied in (
            replace(exchange),
            copy(exchange),
            deepcopy(exchange),
            pickle.loads(pickle.dumps(exchange)),
        ):
            self.assertFalse(
                reference_module.is_reviewed_exchange_status_decision(copied)
            )
        object.__setattr__(exchange, "status", "OPEN_CONFIRMED")
        object.__setattr__(exchange, "block_reason", None)
        object.__setattr__(
            exchange,
            "_decision_digest",
            reference_module._exchange_decision_fingerprint(exchange),
        )
        self.assertFalse(
            reference_module.is_reviewed_exchange_status_decision(exchange)
        )

        instrument = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            complete_halt_observations(halted_symbols=("SPY",)),
            as_of=AS_OF,
        )
        self.assertTrue(
            reference_module.is_reviewed_instrument_status_decision(instrument)
        )
        direct_instrument = reference_module.InstrumentStatusDecision(
            symbol=instrument.symbol,
            halt_status=instrument.halt_status,
            as_of=instrument.as_of,
            valid_until=instrument.valid_until,
            source_observation_ids=instrument.source_observation_ids,
            block_reason=instrument.block_reason,
        )
        self.assertFalse(
            reference_module.is_reviewed_instrument_status_decision(
                direct_instrument
            )
        )
        for copied in (
            replace(instrument),
            copy(instrument),
            deepcopy(instrument),
            pickle.loads(pickle.dumps(instrument)),
        ):
            self.assertFalse(
                reference_module.is_reviewed_instrument_status_decision(copied)
            )
        object.__setattr__(instrument, "halt_status", "CLEAR")
        object.__setattr__(instrument, "block_reason", None)
        object.__setattr__(
            instrument,
            "_decision_digest",
            reference_module._instrument_decision_fingerprint(instrument),
        )
        self.assertFalse(
            reference_module.is_reviewed_instrument_status_decision(instrument)
        )

    def test_trade_halt_rss_requires_exact_declared_unique_item_count(self) -> None:
        url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"

        def parse(body: bytes) -> object:
            client = ReferenceClient(
                StaticReferenceTransport(body, "application/xml"),
                EgressPolicy({"www.nasdaqtrader.com"}),
                allowed_urls={url},
                source_roles={url: "PRIMARY_HALT_FEED"},
                now=lambda: AS_OF,
            )
            return client.parse_halt_feed(client.fetch(url))

        valid = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item>"
            b"</channel></rss>"
        )
        parsed = parse(valid)
        self.assertTrue(parsed.pagination_complete)
        self.assertEqual(parsed.coverage, "COMPLETE_ACTIVE_HALTS")
        self.assertEqual(parsed.halted_symbols, ("QQQ",))

        invalid_bodies = (
            b'<rss version="2.0"><channel>'
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>-1</ndaq:numItems></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>one</ndaq:numItems></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>2</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems>"
            b"<ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>2</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol>"
            b"<Symbol>SPY</Symbol></item>"
            b"</channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b'<channel><ndaq:numItems bogus="yes">1</ndaq:numItems>'
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1<junk/>2</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol></item></channel></rss>",
        )
        for body in invalid_bodies:
            with self.subTest(body=body), self.assertRaises(ValueError):
                parse(body)

    def test_rss_namespaces_and_symbol_paths_are_exact(self) -> None:
        trade_halts_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        trader_alerts_url = (
            "https://www.nasdaqtrader.com/"
            "rss.aspx?categorylist=2&feed=currentheadlines"
        )

        def parse_trade_halts(body: bytes) -> object:
            client = ReferenceClient(
                StaticReferenceTransport(body, "application/xml"),
                EgressPolicy({"www.nasdaqtrader.com"}),
                allowed_urls={trade_halts_url},
                source_roles={trade_halts_url: "PRIMARY_HALT_FEED"},
                now=lambda: AS_OF,
            )
            return client.parse_halt_feed(client.fetch(trade_halts_url))

        def parse_trader_alerts(body: bytes) -> object:
            client = ReferenceClient(
                StaticReferenceTransport(body, "application/xml"),
                EgressPolicy({"www.nasdaqtrader.com"}),
                allowed_urls={trader_alerts_url},
                source_roles={trader_alerts_url: "TRADER_ALERT_HALT"},
                now=lambda: AS_OF,
            )
            return client.parse_halt_feed(client.fetch(trader_alerts_url))

        exact_trade_halt = (
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems><item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"</item></channel></rss>"
        )
        parsed = parse_trade_halts(exact_trade_halt)
        self.assertEqual(parsed.halted_symbols, ("SPY",))

        invalid_trade_halts = (
            b'<x:rss xmlns:x="urn:evil" version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><x:channel>'
            b"<ndaq:numItems>1</ndaq:numItems><x:item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"</x:item></x:channel></x:rss>",
            b'<rss version="2.0" xmlns:x="urn:evil" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><x:channel>'
            b"<ndaq:numItems>1</ndaq:numItems><item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"</item></x:channel></rss>",
            b'<rss version="2.0" xmlns:x="urn:evil" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems><x:item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"</x:item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems><item>"
            b"<IssueSymbol>SPY</IssueSymbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:x="urn:evil" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems><item>"
            b"<x:Symbol>SPY</x:Symbol></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems><item><wrapper>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"</wrapper></item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems><item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"<ndaq:IssueSymbol>QQQ</ndaq:IssueSymbol>"
            b"</item></channel></rss>",
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems>"
            b"<item><title>SPY</title></item></channel></rss>",
            b'<rss version="2.0" xmlns:x="urn:evil" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems><item>"
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>"
            b"<x:Symbol>SPY</x:Symbol></item></channel></rss>",
        )
        for body in invalid_trade_halts:
            with self.subTest(feed="trade-halts", body=body), self.assertRaises(
                ValueError
            ):
                parse_trade_halts(body)

        exact_alert_item = (
            b'<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
            b"<channel><ndaq:numItems>1</ndaq:numItems><item>"
            b"<title>ETA2026-45 - Reviewed headline</title>"
            b"<pubDate>Mon, 10 Aug 2026 21:00:00 GMT</pubDate>"
            b"<link>http://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2026-45</link>"
            b"<ndaq:NewsCategory>Equity Trader Alert</ndaq:NewsCategory>"
            b"<ndaq:Alert>#2026-045</ndaq:Alert>"
            b"<ndaq:Markets>The Nasdaq Stock Market</ndaq:Markets>"
            b"<ndaq:WhatYouNeedToKnow />"
            b"<description>Reviewed alert.</description>"
            b"</item></channel></rss>"
        )
        alert = parse_trader_alerts(exact_alert_item)
        self.assertEqual(alert.coverage, "PARTIAL")
        self.assertEqual(alert.halted_symbols, ())

        for injected in (
            b"<Symbol>SPY</Symbol>",
            b'<x:Symbol xmlns:x="urn:evil">SPY</x:Symbol>',
            b"<ndaq:IssueSymbol>SPY</ndaq:IssueSymbol>",
            b"<description><Symbol>SPY</Symbol></description>",
        ):
            body = exact_alert_item.replace(b"</item>", injected + b"</item>")
            with self.subTest(feed="trader-alerts", injected=injected), self.assertRaises(
                ValueError
            ):
                parse_trader_alerts(body)

    def test_only_exact_nasdaq_rss_is_parsed_and_nyse_html_is_unsupported(self) -> None:
        trade_halts = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        halt_body = (
            b'<?xml version="1.0"?><rss version="2.0" '
            b'xmlns:ndaq="http://www.nasdaqtrader.com/"><channel>'
            b"<ndaq:numItems>1</ndaq:numItems>"
            b"<item><ndaq:IssueSymbol>SPY</ndaq:IssueSymbol></item>"
            b"</channel></rss>"
        )
        halt_client = ReferenceClient(
            StaticReferenceTransport(halt_body, "application/xml"),
            EgressPolicy({"www.nasdaqtrader.com"}),
            allowed_urls={trade_halts},
            source_roles={trade_halts: "PRIMARY_HALT_FEED"},
            now=lambda: AS_OF,
        )
        halt_snapshot = halt_client.parse_halt_feed(
            halt_client.fetch(trade_halts)
        )
        self.assertEqual(halt_snapshot.coverage, "COMPLETE_ACTIVE_HALTS")
        self.assertEqual(halt_snapshot.halted_symbols, ("SPY",))

        operational_url = "https://www.nyse.com/api/notifications/public/alerts?2=3"
        operational_client = ReferenceClient(
            StaticReferenceTransport(b"<html><body>Open</body></html>", "text/html"),
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={operational_url},
            source_roles={operational_url: "OPERATIONAL_STATUS"},
            now=lambda: AS_OF,
        )
        with self.assertRaises(ProviderResponseError):
            operational_client.fetch(operational_url)

    def test_exact_nyse_current_alert_json_is_the_only_operational_clear_source(self) -> None:
        url = "https://www.nyse.com/api/notifications/public/alerts?2=3"
        clear_client = ReferenceClient(
            StaticReferenceTransport(b"[]", "application/json"),
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles={url: "OPERATIONAL_STATUS"},
            now=lambda: AS_OF,
        )
        clear_document = clear_client.fetch(url)
        clear = clear_client.parse_status(clear_document)
        self.assertEqual(clear.status, "OPERATIONAL")
        self.assertTrue(clear.healthy)
        self.assertTrue(clear.supported)
        self.assertEqual(
            clear.document.source_observation_id,
            clear_document.source_observation_id,
        )
        clear_halt = clear_client.parse_halt_feed(clear_document)
        self.assertEqual(clear_halt.scope, "EXCHANGE_OPERATIONAL_STATUS")
        self.assertEqual(clear_halt.coverage, "COMPLETE_ACTIVE_HALTS")
        self.assertTrue(clear_halt.healthy)
        self.assertEqual(
            clear_halt.source_observation_id,
            clear_document.source_observation_id,
        )

        alert_client = ReferenceClient(
            StaticReferenceTransport(b"[2]", "application/json"),
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles={url: "OPERATIONAL_STATUS"},
            now=lambda: AS_OF,
        )
        active_alert = alert_client.parse_status(alert_client.fetch(url))
        self.assertEqual(active_alert.status, "EMERGENCY_CLOSED")
        self.assertTrue(active_alert.healthy)
        active_halt = alert_client.parse_halt_feed(active_alert.document)
        self.assertEqual(active_halt.coverage, "UNKNOWN")

        for body in (b"{}", b"null", b"[", b'"[]"'):
            with self.subTest(body=body):
                client = ReferenceClient(
                    StaticReferenceTransport(body, "application/json"),
                    EgressPolicy({"www.nyse.com"}),
                    allowed_urls={url},
                    source_roles={url: "OPERATIONAL_STATUS"},
                    now=lambda: AS_OF,
                )
                with self.assertRaises(ValueError):
                    client.parse_status(client.fetch(url))

        for retrieved_at, expected in (
            (AS_OF - timedelta(hours=24, microseconds=1), "REFERENCE_SOURCE_STALE"),
            (AS_OF + timedelta(microseconds=1), "REFERENCE_TIMESTAMP_IN_FUTURE"),
        ):
            with self.subTest(expected=expected):
                observations = complete_status_observations()
                observations["operational_status"] = parsed_status_observation(
                    "OPERATIONAL_STATUS",
                    url,
                    retrieved_at=retrieved_at,
                )
                self.assertEqual(
                    verify_exchange_status(observations, as_of=AS_OF).block_reason,
                    expected,
                )

        for target in (
            "https://www.nyse.com/api/notifications/public/alerts?2=4",
            "https://attacker.example/alerts?2=3",
        ):
            with self.subTest(target=target):
                transport = RedirectReferenceTransport(target)
                client = ReferenceClient(
                    transport,
                    EgressPolicy({"www.nyse.com", "attacker.example"}),
                    allowed_urls={url},
                    source_roles={url: "OPERATIONAL_STATUS"},
                    now=lambda: AS_OF,
                )
                with self.assertRaises(NetworkPolicyError):
                    client.fetch(url)
                self.assertEqual(transport.requested_urls, [url])

    def test_cross_host_redirect_is_rejected_before_second_request(self) -> None:
        transport = FixtureTransport(
            "providers/reference/cross-host-redirect.json"
        )
        url = "https://www.nyse.com/trade/hours-calendars"
        client = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles=configured_roles(url),
        )
        with self.assertRaises(NetworkPolicyError):
            client.fetch(url)
        self.assertEqual(transport.requested_urls, [url])

    def test_uncertain_emergency_status_blocks_session(self) -> None:
        result = verify_exchange_status(
            complete_status_observations(),
            as_of=AS_OF,
        )
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.block_reason, "REFERENCE_SOURCE_UNSUPPORTED")

    def test_only_preconfigured_exact_reference_urls_can_be_fetched(self) -> None:
        configured = "https://www.nyse.com/trade/hours-calendars"
        transport = FixtureTransport(
            "providers/reference/cross-host-redirect.json"
        )
        client = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={configured},
            source_roles=configured_roles(configured),
        )
        for url in (
            "https://www.nyse.com/markets/hours-calendars",
            configured + "?unreviewed=1",
            "https://www.nyse.com/trade/hours-calendars/extra",
        ):
            with self.subTest(url=url), self.assertRaises(NetworkPolicyError):
                client.fetch(url)
        self.assertEqual(transport.requested_urls, [])

    def test_fetch_binds_configured_role_and_validates_metadata_before_io(self) -> None:
        url = "https://www.nyse.com/trade/hours-calendars"
        transport = StaticReferenceTransport()
        client = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles={url: "PRIMARY_CALENDAR"},
            now=lambda: AS_OF,
        )
        document = client.fetch(url, role="PRIMARY_CALENDAR")
        self.assertEqual(document.source_role, "PRIMARY_CALENDAR")
        self.assertEqual(document.source_type, "OFFICIAL_REFERENCE")
        self.assertEqual(transport.requested_urls, [url])

        for options in (
            {"role": "CROSS_CHECK_CALENDAR"},
            {"role": "PRIMARY_CALENDAR", "publisher": "\n"},
            {"role": "PRIMARY_CALENDAR", "source_type": "évidence"},
        ):
            with self.subTest(options=options), self.assertRaises(
                (ValueError, NetworkPolicyError)
            ):
                client.fetch(url, **options)
        self.assertEqual(transport.requested_urls, [url])

    def test_same_origin_redirect_also_requires_exact_reviewed_target(self) -> None:
        start = "https://www.nyse.com/trade/hours-calendars"
        target = "https://www.nyse.com/trade/hours-calendars-current"
        transport = FixtureTransport(
            "providers/reference/same-host-redirect.json"
        )
        client = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={start},
            source_roles=configured_roles(start),
        )

        with self.assertRaises(NetworkPolicyError):
            client.fetch(start)

        self.assertEqual(transport.requested_urls, [start])

        transport = FixtureTransport(
            "providers/reference/same-host-redirect.json"
        )
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(NetworkPolicyError):
                ReferenceClient(
                    transport,
                    EgressPolicy({"www.nyse.com"}),
                    allowed_urls={start, target},
                    source_roles={
                        start: "PRIMARY_CALENDAR",
                        target: "PRIMARY_CALENDAR",
                    },
                    cache=ContentCache(Path(directory)),
                    now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
                )
        self.assertEqual(transport.requested_urls, [])

    def test_exchange_consensus_fails_closed_for_missing_conflict_or_emergency(self) -> None:
        missing = complete_status_observations()
        missing.pop("cross_check")
        cases = {
            "REFERENCE_SOURCE_MISSING": missing,
            "REFERENCE_SOURCE_UNSUPPORTED": complete_status_observations(),
        }
        for expected, fixture in cases.items():
            with self.subTest(expected=expected):
                self.assertEqual(
                    verify_exchange_status(fixture, as_of=AS_OF).block_reason,
                    expected,
                )

    def test_complete_agreement_is_explicitly_safe_not_consensus_by_omission(self) -> None:
        result = verify_exchange_status(complete_status_observations(), as_of=AS_OF)
        self.assertEqual(result.block_reason, "REFERENCE_SOURCE_UNSUPPORTED")
        self.assertEqual(result.status, "BLOCKED")

    def test_missing_health_attestation_is_not_treated_as_healthy(self) -> None:
        document = source_document(
            "PRIMARY_CALENDAR",
            "https://www.nyse.com/trade/hours-calendars",
            "calendar-no-health",
        )
        with self.assertRaises(TypeError):
            reference_module.ReferenceStatusSnapshot(
                role="PRIMARY_CALENDAR",
                status="OPEN",
                document=document,
            )

    def test_exchange_status_requires_fresh_provenance_at_authoritative_as_of(self) -> None:
        fresh = complete_status_observations()
        result = verify_exchange_status(fresh, as_of=AS_OF)
        self.assertEqual(result.block_reason, "REFERENCE_SOURCE_UNSUPPORTED")
        self.assertEqual(len(result.source_observation_ids), 4)
        self.assertEqual(
            len(set(result.source_observation_ids)),
            len(result.source_observation_ids),
        )

        arbitrary = {
            "primary": status_observation("OPEN", "calendar-primary"),
            "cross_check": status_observation("OPEN", "calendar-cross-check"),
            "operational_status": status_observation("OPEN", "exchange-status"),
            "trader_alert": status_observation("CLEAR", "trader-alert"),
        }
        self.assertEqual(
            verify_exchange_status(arbitrary, as_of=AS_OF).block_reason,
            "REFERENCE_SNAPSHOT_UNVERIFIED",
        )

    def test_exchange_status_rejects_stale_and_future_observations(self) -> None:
        cases = (
            (
                AS_OF - timedelta(hours=24, microseconds=1),
                "REFERENCE_SOURCE_STALE",
            ),
            (
                AS_OF + timedelta(microseconds=1),
                "REFERENCE_TIMESTAMP_IN_FUTURE",
            ),
        )
        for retrieved_at, expected in cases:
            observations = complete_status_observations(
                primary_retrieved_at=retrieved_at
            )
            with self.subTest(expected=expected):
                self.assertEqual(
                    verify_exchange_status(observations, as_of=AS_OF).block_reason,
                    expected,
                )

    def test_status_snapshot_cannot_contradict_its_role_bound_document_bytes(self) -> None:
        url = "https://www.nyse.com/api/notifications/public/alerts?2=3"
        body = json.dumps(
            {
                "schema_version": 1,
                "kind": "REFERENCE_STATUS",
                "role": "OPERATIONAL_STATUS",
                "status": "OPEN",
                "healthy": True,
            },
            sort_keys=True,
        ).encode("ascii")
        client = ReferenceClient(
            StaticReferenceTransport(body, "application/json"),
            EgressPolicy({"www.nyse.com"}),
            allowed_urls={url},
            source_roles={url: "OPERATIONAL_STATUS"},
            now=lambda: AS_OF,
        )

        with self.assertRaises(ValueError):
            client.parse_status(client.fetch(url))

    def test_complete_active_halt_coverage_is_required_for_symbol_clear(self) -> None:
        observations = complete_halt_observations()
        decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            observations,
            as_of=AS_OF,
        )
        self.assertEqual(decision.symbol, "SPY")
        self.assertEqual(decision.halt_status, "CLEAR")
        self.assertIsNone(decision.block_reason)
        self.assertEqual(len(decision.source_observation_ids), 3)

        missing_cross_check = dict(observations)
        missing_cross_check.pop("cross_check_halt_feed")
        unknown = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            missing_cross_check,
            as_of=AS_OF,
        )
        self.assertEqual(unknown.halt_status, "UNKNOWN")
        self.assertIn(
            unknown.block_reason,
            {"HALT_SOURCE_MISSING", "HALT_COVERAGE_INCOMPLETE"},
        )

    def test_authoritative_complete_halts_and_operational_clear_allow_partial_cross_check(self) -> None:
        observations = complete_halt_observations()
        complete = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            observations,
            as_of=AS_OF,
        )
        self.assertEqual(complete.halt_status, "CLEAR")
        self.assertIsNone(complete.block_reason)
        self.assertEqual(
            observations["cross_check_halt_feed"].coverage,
            "PARTIAL",
        )

        partial_without_authority = {
            "operational_status": observations["operational_status"],
            "cross_check_halt_feed": observations["cross_check_halt_feed"],
        }
        partial = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            partial_without_authority,
            as_of=AS_OF,
        )
        self.assertEqual(partial.halt_status, "UNKNOWN")
        self.assertEqual(partial.block_reason, "HALT_SOURCE_MISSING")

        cross_check_notice = complete_halt_observations()
        cross_check_notice["cross_check_halt_feed"] = parsed_halt_snapshot(
            "TRADER_ALERT_HALT",
            "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
            halted_symbols=("SPY",),
            retrieved_at=AS_OF - timedelta(minutes=2),
        )
        notice_only = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            cross_check_notice,
            as_of=AS_OF,
        )
        self.assertEqual(
            cross_check_notice["cross_check_halt_feed"].halted_symbols,
            (),
        )
        self.assertEqual(notice_only.halt_status, "CLEAR")
        self.assertIsNone(notice_only.block_reason)

        operational_alarm = complete_halt_observations()
        operational_alarm["operational_status"] = parsed_halt_snapshot(
            "OPERATIONAL_STATUS",
            "https://www.nyse.com/api/notifications/public/alerts?2=3",
            operational_alert=True,
        )
        emergency = reference_module.classify_instrument_status(
            "SPY", "NYSE", operational_alarm, as_of=AS_OF
        )
        self.assertEqual(emergency.halt_status, "HALTED")
        self.assertEqual(emergency.block_reason, "EXCHANGE_OPERATIONAL_ALERT")

    def test_halt_sources_require_exact_key_role_url_and_distinct_observations(self) -> None:
        one_snapshot = complete_halt_observations()["primary_halt_feed"]
        reused = {name: one_snapshot for name in (
            "primary_halt_feed",
            "operational_status",
            "cross_check_halt_feed",
        )}
        reused_decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            reused,
            as_of=AS_OF,
        )
        self.assertEqual(reused_decision.halt_status, "UNKNOWN")
        self.assertEqual(
            reused_decision.block_reason,
            "HALT_SOURCE_IDENTITY_CONFLICT",
        )

        wrong_key = complete_halt_observations()
        wrong_key["cross_check_halt_feed"] = parsed_halt_snapshot(
            "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            retrieved_at=AS_OF - timedelta(minutes=2),
        )
        role_decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            wrong_key,
            as_of=AS_OF,
        )
        self.assertEqual(role_decision.halt_status, "UNKNOWN")
        self.assertEqual(role_decision.block_reason, "HALT_SOURCE_ROLE_CONFLICT")

    def test_explicit_halt_blocks_and_expired_or_wrong_scope_is_unknown(self) -> None:
        halted = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            complete_halt_observations(halted_symbols=("SPY",)),
            as_of=AS_OF,
        )
        self.assertEqual(halted.halt_status, "HALTED")
        self.assertEqual(halted.block_reason, "SYMBOL_HALTED")

        expired = complete_halt_observations()
        expired["primary_halt_feed"] = parsed_halt_snapshot(
            "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            retrieved_at=AS_OF - timedelta(minutes=5, microseconds=1),
        )
        expired_decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            expired,
            as_of=AS_OF,
        )
        self.assertEqual(expired_decision.halt_status, "UNKNOWN")
        self.assertEqual(expired_decision.block_reason, "HALT_SOURCE_STALE")

        wrong_scope = complete_halt_observations()
        wrong_scope["primary_halt_feed"] = replace(
            wrong_scope["primary_halt_feed"],
            venue="NASDAQ",
        )
        scope_decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            wrong_scope,
            as_of=AS_OF,
        )
        self.assertEqual(scope_decision.halt_status, "UNKNOWN")
        self.assertEqual(scope_decision.block_reason, "HALT_SNAPSHOT_UNVERIFIED")

    def test_halt_snapshot_cannot_hide_symbols_declared_in_document_bytes(self) -> None:
        parsed = parsed_halt_snapshot(
            "PRIMARY_HALT_FEED",
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
            halted_symbols=("SPY",),
        )
        self.assertEqual(parsed.halted_symbols, ("SPY",))
        tampered = replace(parsed, halted_symbols=())
        decision = reference_module.classify_instrument_status(
            "SPY",
            "NYSE",
            {
                "primary_halt_feed": tampered,
                "operational_status": complete_halt_observations()["operational_status"],
                "cross_check_halt_feed": complete_halt_observations()["cross_check_halt_feed"],
            },
            as_of=AS_OF,
        )
        self.assertEqual(decision.block_reason, "HALT_SNAPSHOT_UNVERIFIED")


if __name__ == "__main__":
    unittest.main()
