"""Adversarial foundation tests for canonical premarket source composition."""

from __future__ import annotations

import base64
import gc
import hashlib
import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock
from weakref import ref
from zoneinfo import ZoneInfo

import stock_monitor.market_calendar as market_calendar_module
import stock_monitor.provider_workflows as provider_workflows_module
from stock_monitor.evidence import EvidenceRegistryError, load_current_evidence_release
from stock_monitor.journal import Journal
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.provider_workflows import (
    CanonicalMaterialError,
    PremarketCollectionError,
    PremarketProviderCollection,
    PremarketRiskResolution,
    PremarketWorkflowCoordinator,
    PremarketSourceBinding,
    canonical_premarket_state_hash,
    is_issued_canonical_premarket_composition_authority,
    is_issued_canonical_premarket_source_binding_authority,
    issue_canonical_premarket_composition_authority,
    issue_canonical_premarket_source_binding_authority,
)
from stock_monitor.providers.alpaca import (
    AlpacaMarketData,
    Bar,
    ProviderFetchBundle,
    ProviderFetchCohort,
    ProviderFetchManifest,
    ProviderFetchPage,
    ProviderFetchPageBundle,
    Quote,
    read_provider_fetch_bundle,
)
from stock_monitor.providers.cache import SourceObservation
from stock_monitor.providers.http import EgressPolicy, HttpResponse
from stock_monitor.providers.reference import ReferenceClient
from stock_monitor.universe import load_current_universe
from stock_monitor.workflows import PremarketSnapshot, WorkflowDataError
from tests.support import FixtureTransport, credentials


ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 22)
DECISION_AT = datetime(2026, 8, 22, 8, 45, tzinfo=ET)
RETRIEVED_AT = datetime(2026, 8, 22, 8, 52, tzinfo=ET)


class CanonicalPremarketSourceBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.temporary_root = Path(temporary.name)
        self.archive_root = self.temporary_root / "reports"
        self.archive_root.mkdir()
        self.journal = Journal.open(self.temporary_root / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.calendar = load_current_market_calendar(ROOT, as_of=DAY)
        self.universe = load_current_universe(ROOT, as_of=DAY)
        self.evidence = load_current_evidence_release(
            ROOT,
            as_of=DECISION_AT,
            universe=self.universe,
        )

    @staticmethod
    def _reviewed_uri(role: str, payload: bytes) -> str:
        return (
            f"stock-monitor://reviewed/{role}/"
            f"{hashlib.sha256(payload).hexdigest()}"
        )

    def _reviewed_receipt(
        self,
        *,
        role: str,
        payload: bytes,
        source_time: datetime,
        journal: Journal | None = None,
    ):
        target = self.journal if journal is None else journal
        return target.append_source_observation_receipt(
            payload=payload,
            source_uri=self._reviewed_uri(role, payload),
            source_type="REVIEWED_ARTIFACT",
            provider="operator-reviewed",
            feed=None,
            source_time=source_time,
            retrieved_at=DECISION_AT,
            provider_sequence=None,
            delay_seconds=int((DECISION_AT - source_time).total_seconds()),
            health_result="REVIEWED",
            details={"role": role},
        )

    def _reviewed_bindings(self, *, journal: Journal | None = None):
        bindings: list[PremarketSourceBinding] = []
        calendar_payload = (ROOT / "data/calendars/2026.json").read_bytes()
        universe_payload = (ROOT / "data/universe/2026-08-22.json").read_bytes()
        release_payload = (ROOT / "data/evidence/current.json").read_bytes()
        reviewed = (
            (
                "calendar",
                calendar_payload,
                datetime.combine(self.calendar.reviewed_at, datetime.min.time(), ET),
                self.calendar,
            ),
            (
                "universe",
                universe_payload,
                datetime.combine(self.universe.reviewed_at, datetime.min.time(), ET),
                self.universe,
            ),
            (
                "evidence-release",
                release_payload,
                self.evidence.reviewed_at,
                self.evidence,
            ),
        )
        for role, payload, source_time, source in reviewed:
            bindings.append(
                PremarketSourceBinding(
                    receipt=self._reviewed_receipt(
                        role=role,
                        payload=payload,
                        source_time=source_time,
                        journal=journal,
                    ),
                    source=source,
                    decision_basis="ECONOMIC_INPUT",
                )
            )
        for symbol, bundle in self.evidence.by_symbol.items():
            payload = (ROOT / f"data/evidence/subjects/{symbol}.json").read_bytes()
            bindings.append(
                PremarketSourceBinding(
                    receipt=self._reviewed_receipt(
                        role=f"evidence-{symbol.lower()}",
                        payload=payload,
                        source_time=bundle.reviewed_at,
                        journal=journal,
                    ),
                    source=bundle,
                    decision_basis="ECONOMIC_INPUT",
                )
            )
            for source_binding in bundle.source_bindings:
                document = source_binding.document
                details = {
                    "accession": document.accession,
                    "issuer_cik": None,
                    "source_observation_id": document.source_observation_id,
                    "source_role": document.source_role,
                    "symbol": None,
                    "timestamp_source": document.timestamp_source,
                }
                receipt = (self.journal if journal is None else journal).append_source_observation_receipt(
                    payload=document.body,
                    source_uri=document.url,
                    source_type=document.source_type,
                    provider=document.publisher,
                    feed=document.timestamp_source,
                    source_time=document.published_at or document.retrieved_at,
                    retrieved_at=document.retrieved_at,
                    provider_sequence=None,
                    delay_seconds=0,
                    health_result="OK",
                    details=details,
                )
                bindings.append(
                    PremarketSourceBinding(
                        receipt=receipt,
                        source=document,
                        decision_basis="ECONOMIC_INPUT",
                    )
                )
        target = self.journal if journal is None else journal
        reread = target.read_source_observation_receipts(
            tuple(binding.receipt.row_id for binding in bindings)
        )
        return tuple(
            replace(binding, receipt=receipt)
            for binding, receipt in zip(bindings, reread, strict=True)
        )

    def _authority(self):
        return issue_canonical_premarket_source_binding_authority(
            journal=self.journal,
            decision_at=DECISION_AT,
            retrieved_at=RETRIEVED_AT,
            bindings=self._reviewed_bindings(),
        )

    def _provider_cohort_and_bundle(
        self,
        *,
        source_type: str,
        ordinal: int,
    ):
        symbols = tuple(sorted(record.symbol for record in self.universe.records))
        if source_type == "ALPACA_DAILY_BARS":
            collection = "bars"
            feed = "sip"
            query = (
                f"symbols={','.join(symbols)}&timeframe=1Day&"
                "start=2026-05-01T00%3A00%3A00Z&"
                "end=2026-08-21T20%3A00%3A00Z&"
                "adjustment=split&feed=sip&limit=10000"
            ).replace(",", "%2C")
            facts = tuple(
                (
                    symbol,
                    (
                        Bar(
                            symbol,
                            datetime(2026, 8, 21, 20, tzinfo=UTC),
                            Decimal("100"),
                            Decimal("101"),
                            Decimal("99"),
                            Decimal("100.5"),
                            5_000_000,
                            feed,
                            "split",
                            f"obs-provider-{ordinal}",
                        ),
                    ),
                )
                for symbol in symbols
            )
            source_time = datetime(2026, 8, 21, 20, tzinfo=UTC)
        else:
            collection = "quotes"
            feed = "iex" if source_type == "ALPACA_LATEST_QUOTES" else "sip"
            if source_type == "ALPACA_LATEST_QUOTES":
                query = f"symbols={','.join(symbols)}&feed=iex".replace(
                    ",", "%2C"
                )
                source_time = datetime(2026, 8, 22, 12, 50, tzinfo=UTC)
            else:
                query = (
                    f"symbols={','.join(symbols)}&"
                    "start=2026-08-21T19%3A55%3A00Z&"
                    "end=2026-08-21T20%3A00%3A00Z&feed=sip&limit=10000"
                ).replace(",", "%2C")
                source_time = datetime(2026, 8, 21, 20, tzinfo=UTC)
            facts = tuple(
                (
                    symbol,
                    (
                        Quote(
                            symbol,
                            source_time,
                            Decimal("100"),
                            Decimal("100.1"),
                            feed,
                            1,
                            0,
                            f"obs-provider-{ordinal}",
                        ),
                    ),
                )
                for symbol in symbols
            )
        path = (
            "/v2/stocks/bars"
            if source_type == "ALPACA_DAILY_BARS"
            else "/v2/stocks/quotes/latest"
            if source_type == "ALPACA_LATEST_QUOTES"
            else "/v2/stocks/quotes"
        )
        url = f"https://data.alpaca.markets{path}?{query}"
        payload = f'{{"page":{ordinal}}}'.encode()
        digest = hashlib.sha256(payload).hexdigest()
        page = ProviderFetchPage(
            page_ordinal=1,
            source_observation_id=f"obs-provider-{ordinal}",
            source_type=source_type,
            request_url=url,
            request_page_token=None,
            next_page_token=None,
            payload_sha256=digest,
        )
        manifest = ProviderFetchManifest(
            collection=collection,
            requested_symbols=symbols,
            request_digest=f"{ordinal:x}" * 64,
            pages=(page,),
            terminal=True,
            manifest_digest=f"{ordinal + 3:x}" * 64,
        )
        observation = SourceObservation(
            observation_id=page.source_observation_id,
            url=url,
            source_type=source_type,
            source_timestamp=source_time,
            retrieved_at=(
                datetime(2026, 8, 22, 12, 51, tzinfo=UTC)
                if feed == "iex"
                else datetime(2026, 8, 22, 12, 50, tzinfo=UTC)
            ),
            feed=feed,
            delay_seconds=int(
                (
                    (
                        datetime(2026, 8, 22, 12, 51, tzinfo=UTC)
                        if feed == "iex"
                        else datetime(2026, 8, 22, 12, 50, tzinfo=UTC)
                    )
                    - source_time
                ).total_seconds()
            ),
        )
        return ProviderFetchCohort(facts), ProviderFetchBundle(
            manifest,
            (ProviderFetchPageBundle(page, payload, observation),),
        )

    def _reference_documents_and_bindings(
        self,
        *,
        retrieved_at: datetime = RETRIEVED_AT,
    ):
        from tests.unit import _task5_fixtures as task5_fixtures

        urls = {
            "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts": (
                "PRIMARY_HALT_FEED",
                *task5_fixtures._HALT_SOURCE_RESPONSES[
                    "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
                ],
            ),
            "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines": (
                "TRADER_ALERT_HALT",
                *task5_fixtures._HALT_SOURCE_RESPONSES[
                    "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines"
                ],
            ),
            "https://www.nyse.com/api/notifications/public/alerts?2=3": (
                "OPERATIONAL_STATUS",
                *task5_fixtures._HALT_SOURCE_RESPONSES[
                    "https://www.nyse.com/api/notifications/public/alerts?2=3"
                ],
            ),
        }
        transport = mock.Mock()
        transport.get.side_effect = lambda url, _headers: HttpResponse(
            200,
            (("Content-Type", urls[url][2]),),
            urls[url][1],
            url,
        )
        owner = ReferenceClient(
            transport,
            EgressPolicy({"www.nyse.com", "www.nasdaqtrader.com"}),
            allowed_urls=urls,
            source_roles={url: values[0] for url, values in urls.items()},
            now=lambda: retrieved_at,
        )
        documents = tuple(owner.fetch(url) for url in urls)
        bindings = provider_workflows_module._persist_premarket_reference_bindings(
            journal=self.journal,
            owner=owner,
            documents=documents,
        )
        return owner, documents, bindings

    def _fresh_open_project(self):
        from tests.unit import _task5_fixtures as task5_fixtures

        session_date = date(2026, 8, 24)
        reviewed_at = datetime(2026, 8, 24, 12, 40, tzinfo=UTC)
        source_at = reviewed_at - timedelta(seconds=2)
        review_by = reviewed_at + timedelta(hours=23, minutes=59)
        project = self.temporary_root / "fresh-project"
        (project / "data/calendars").mkdir(parents=True)
        (project / "data/universe").mkdir(parents=True)
        (project / "data/evidence/subjects").mkdir(parents=True)
        (project / "data/evidence/sources").mkdir(parents=True)
        shutil.copy2(
            ROOT / "data/calendars/2026.json",
            project / "data/calendars/2026.json",
        )
        shutil.copy2(
            ROOT / "data/universe/2026-08-22.json",
            project / "data/universe/2026-08-22.json",
        )
        for source in (ROOT / "data/evidence/sources").iterdir():
            shutil.copy2(source, project / "data/evidence/sources" / source.name)

        calendar = load_current_market_calendar(ROOT, as_of=session_date)
        hold_end = calendar.add_sessions(session_date, 9)
        manifest = json.loads((ROOT / "data/evidence/current.json").read_bytes())
        manifest["release_id"] = "reviewed-evidence-2026-08-24-test"
        manifest["reviewed_at"] = reviewed_at.isoformat().replace("+00:00", "Z")
        manifest["review_by"] = review_by.isoformat().replace("+00:00", "Z")
        scoped_authorities = {}
        clear_authorities = {}
        scoped_sources = set()
        for subject in manifest["subjects"]:
            symbol = subject["symbol"]
            payload = json.loads(
                (ROOT / "data/evidence/subjects" / f"{symbol}.json").read_bytes()
            )
            payload["reviewed_at"] = manifest["reviewed_at"]
            role = f"CORPORATE_ACTION:{symbol}"
            pair = (
                f"https://reviewed.task10.invalid/coverage/{symbol.lower()}",
                "Reviewed Task 10 Coverage Authority",
            )
            issuer_cik = payload["subject"]["issuer_cik"]
            scoped_authorities[role] = (issuer_cik, frozenset({pair}))
            clear_authorities[role] = frozenset({pair})
            scoped_sources.add((role, issuer_cik, pair[0], pair[1]))
            for binding in payload["source_bindings"]:
                binding["primary_url"] = pair[0]
                binding["publisher"] = pair[1]
                binding["source_role"] = role
                binding["timestamp_source"] = "PRIMARY_METADATA"
                binding["published_at"] = source_at.isoformat().replace(
                    "+00:00", "Z"
                )
                binding["retrieved_at"] = source_at.isoformat().replace(
                    "+00:00", "Z"
                )
                binding["checked_at"] = manifest["reviewed_at"]
                binding["valid_until"] = review_by.isoformat().replace(
                    "+00:00", "Z"
                )
            if symbol == "AAPL":
                record = task5_fixtures._reviewed_record(
                    sequence=1,
                    subject_kind="STOCK",
                    symbol=symbol,
                    issuer_cik=issuer_cik,
                    event_type="material agreement",
                    published_at=reviewed_at - timedelta(days=5),
                    retrieved_at=source_at,
                )
                source_binding = task5_fixtures._source_binding(
                    record,
                    subject_kind="STOCK",
                    healthy=True,
                )
                payload["records"].append(
                    {
                        **task5_fixtures._record_document(record),
                        "content_hash": record.content_hash,
                    }
                )
                payload["source_bindings"].append(
                    task5_fixtures._binding_document(source_binding)
                )
                document = source_binding.document
                source_artifact = json.dumps(
                    {
                        "body": base64.b64encode(document.body).decode("ascii"),
                        "content_sha256": document.content_hash,
                        "encoding": "base64",
                        "kind": "RAW_SOURCE_ARTIFACT",
                        "schema_version": 1,
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                (
                    project
                    / "data/evidence/sources"
                    / f"{document.content_hash}.json"
                ).write_bytes(source_artifact)
            is_etf = payload["subject"]["subject_kind"] == "ETF"
            for coverage in payload["coverage_attestations"]:
                coverage["checked_at"] = manifest["reviewed_at"]
                coverage["valid_until"] = review_by.isoformat().replace(
                    "+00:00", "Z"
                )
                coverage["coverage_start"] = session_date.isoformat()
                coverage["coverage_end"] = hold_end.isoformat()
                coverage["complete"] = True
                coverage["coverage"] = (
                    "NOT_APPLICABLE"
                    if (coverage["coverage_kind"] == "BINARY_EVENT") == is_etf
                    else "CONFIRMED_CLEAR"
                )
            child = json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            (project / "data/evidence/subjects" / f"{symbol}.json").write_bytes(
                child
            )
            subject["sha256"] = hashlib.sha256(child).hexdigest()
        release_payload = json.dumps(
            manifest,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        (project / "data/evidence/current.json").write_bytes(release_payload)
        return (
            project,
            hashlib.sha256(release_payload).hexdigest(),
            scoped_authorities,
            clear_authorities,
            frozenset(scoped_sources),
        )

    def _complete_candidate_collection(self, **kwargs):
        import stock_monitor.evidence as evidence_module
        import stock_monitor.providers.alpaca as alpaca_module
        import stock_monitor.screening as screening_module
        from stock_monitor.providers.alpaca import TimeWindow
        from stock_monitor.providers.reference import classify_instrument_status
        from tests.integration.test_phase1_authorities import (
            _CandidateAlpacaTransport,
        )
        from tests.unit import _task5_fixtures as task5_fixtures

        session_date = kwargs["session_date"]
        decision_at = kwargs["decision_at"]
        retrieved_at = kwargs["retrieved_at"]
        calendar = kwargs["calendar"]
        universe = kwargs["universe"]
        evidence_release = kwargs["evidence_release"]
        previous_session = session_date - timedelta(days=1)
        while not calendar.is_open(previous_session):
            previous_session -= timedelta(days=1)
        history_dates: list[date] = []
        current = previous_session
        while len(history_dates) < 60:
            if calendar.is_open(current):
                history_dates.append(current)
            current -= timedelta(days=1)
        history_dates.reverse()
        hold_sessions = tuple(
            calendar.add_sessions(session_date, offset) for offset in range(10)
        )
        attestation = screening_module.build_market_session_attestation(
            calendar,
            session_date,
            decision_at,
        )
        templates = task5_fixtures.universe_candidate_contexts()
        template_by_symbol = {
            context.record.symbol: context for context in templates
        }
        template_bars = templates[0].bars_by_symbol
        bars_by_symbol = {
            symbol: tuple(
                replace(
                    bar,
                    timestamp=datetime(
                        day.year,
                        day.month,
                        day.day,
                        16,
                        tzinfo=ET,
                    ).astimezone(UTC),
                    source_observation_id=f"raw-bars-{symbol.lower()}",
                )
                for day, bar in zip(
                    history_dates,
                    template_bars[symbol],
                    strict=True,
                )
            )
            for symbol in kwargs["required_symbols"]
        }
        owner, documents, reference_bindings = (
            self._reference_documents_and_bindings(
                retrieved_at=retrieved_at,
            )
        )
        by_role = {document.source_role: document for document in documents}
        snapshots = {
            "primary_halt_feed": owner.parse_halt_feed(
                by_role["PRIMARY_HALT_FEED"]
            ),
            "operational_status": owner.parse_halt_feed(
                by_role["OPERATIONAL_STATUS"]
            ),
            "cross_check_halt_feed": owner.parse_halt_feed(
                by_role["TRADER_ALERT_HALT"]
            ),
        }
        raw_contexts = []
        for record in universe.records:
            bundle = evidence_release.by_symbol[record.symbol]
            evidence = evidence_module.classify_evidence(
                bundle.records,
                evidence_module.DateRange(
                    hold_sessions[0],
                    hold_sessions[-1],
                ),
                symbol=record.symbol,
                issuer_cik=record.issuer_cik,
                source_bindings=bundle.source_bindings,
                as_of=decision_at,
                subject_kind=bundle.subject_kind,
                coverage_attestations=bundle.coverage_attestations,
                reviewed_bundle=bundle,
            )
            status = classify_instrument_status(
                record.symbol,
                record.listing_venue,
                snapshots,
                as_of=retrieved_at,
            )
            template = template_by_symbol[record.symbol]
            raw_contexts.append(
                replace(
                    template,
                    record=record,
                    bars_by_symbol=bars_by_symbol,
                    previous_session_quote=replace(
                        template.previous_session_quote,
                        timestamp=datetime(
                            previous_session.year,
                            previous_session.month,
                            previous_session.day,
                            15,
                            58,
                            tzinfo=ET,
                        ),
                        source_observation_id=(
                            f"raw-previous-{record.symbol.lower()}"
                        ),
                    ),
                    latest_iex_quote=replace(
                        template.latest_iex_quote,
                        timestamp=retrieved_at - timedelta(minutes=2),
                        source_observation_id=(
                            f"raw-latest-{record.symbol.lower()}"
                        ),
                    ),
                    instrument_status=status,
                    evidence=evidence,
                    issuer_cik=record.issuer_cik,
                    initial_listing_date=record.initial_listing_date,
                    listing_date_status="VERIFIED",
                    session_date=session_date,
                    previous_session_date=previous_session,
                    as_of=decision_at,
                    operational_as_of=retrieved_at,
                    hold_sessions=hold_sessions,
                    session_attestation=attestation,
                    market_calendar=calendar,
                )
            )
        raw_contexts_tuple = tuple(raw_contexts)
        transport = _CandidateAlpacaTransport(raw_contexts_tuple)
        client = AlpacaMarketData(
            transport,
            credentials(),
            now=lambda: retrieved_at.astimezone(UTC),
        )
        raw_bars = tuple(
            bar for values in bars_by_symbol.values() for bar in values
        )
        provider_bars = client.daily_bars(
            kwargs["required_symbols"],
            TimeWindow(
                min(bar.timestamp for bar in raw_bars),
                max(bar.timestamp for bar in raw_bars),
            ),
        )
        previous_times = tuple(
            context.previous_session_quote.timestamp
            for context in raw_contexts_tuple
        )
        provider_previous = client.historical_quotes(
            kwargs["required_symbols"],
            TimeWindow(
                min(previous_times) - timedelta(minutes=1),
                max(previous_times) + timedelta(minutes=2),
            ),
        )
        provider_latest = client.latest_iex_quotes(
            kwargs["required_symbols"]
        )
        latest_manifest = alpaca_module._normalized_market_fact_source(
            next(iter(provider_latest.values()))
        ).fetch_manifest
        latest_page = latest_manifest.pages[0]
        latest_payload = transport.bodies_by_url[latest_page.request_url]
        latest_source_time = max(
            quote.timestamp for quote in provider_latest.values()
        )
        latest_page_bundle = ProviderFetchPageBundle(
            latest_page,
            latest_payload,
            SourceObservation(
                observation_id=latest_page.source_observation_id,
                url=latest_page.request_url,
                source_type=latest_page.source_type,
                source_timestamp=latest_source_time,
                retrieved_at=retrieved_at.astimezone(UTC),
                feed="iex",
                delay_seconds=int(
                    (
                        retrieved_at.astimezone(UTC) - latest_source_time
                    ).total_seconds()
                ),
            ),
        )
        latest_cohort = ProviderFetchCohort(
            tuple(
                (symbol, (quote,))
                for symbol, quote in sorted(provider_latest.items())
            )
        )
        contexts = tuple(
            replace(
                context,
                bars_by_symbol=provider_bars,
                previous_session_quote=provider_previous[
                    context.record.symbol
                ][-1],
                latest_iex_quote=provider_latest[context.record.symbol],
            )
            for context in raw_contexts_tuple
        )
        preview = screening_module.build_base_eligible_cohort(
            contexts,
            universe=universe,
        )
        self._candidate_diagnostics = (
            preview.status,
            preview.reason_codes,
            tuple(
                (
                    context.record.symbol,
                    screening_module.score_candidate(context).status,
                    screening_module.score_candidate(context).total,
                    screening_module.score_candidate(context).reason_codes,
                    screening_module.detect_setup(context).status,
                    screening_module.detect_setup(context).reason_codes,
                )
                for context in preview.contexts
            ),
        )
        cohorts = (provider_bars, provider_previous, latest_cohort)
        self._candidate_disclosures = {
            id(provider_bars): read_provider_fetch_bundle(provider_bars),
            id(provider_previous): read_provider_fetch_bundle(provider_previous),
            id(latest_cohort): ProviderFetchBundle(
                latest_manifest,
                (latest_page_bundle,),
            ),
        }
        provider_bindings = provider_workflows_module._persist_premarket_provider_bindings(
            journal=self.journal,
            cohorts=cohorts,
            required_symbols=kwargs["required_symbols"],
        )
        return PremarketProviderCollection(
            provider_cohorts=cohorts,
            reference_sources=documents,
            contexts=contexts,
            persisted_bindings=(*reference_bindings, *provider_bindings),
        )

    def test_market_closed_composes_real_reviewed_material_without_provider_calls(self) -> None:
        collector = mock.Mock()
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-market-closed",
        )

        material = coordinator.premarket_material(
            DAY,
            decision_at=DECISION_AT,
            retrieved_at=RETRIEVED_AT,
        )

        self.assertEqual(material.report.outcome, "NO TRADE")
        self.assertIn("`MARKET_CLOSED`", material.report.body)
        self.assertEqual(material.snapshot, PremarketSnapshot((), False))
        self.assertTrue(material.source_receipts)
        self.assertTrue(
            all(receipt.source_payload for receipt in material.source_receipts)
        )
        collector.collect.assert_not_called()

    def test_stale_reviewed_evidence_is_data_unavailable_with_zero_candidates(self) -> None:
        session_date = date(2026, 8, 24)
        decision_at = datetime(2026, 8, 24, 8, 45, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 24, 8, 52, tzinfo=ET)
        collector = mock.Mock()
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-stale-evidence",
        )

        material = coordinator.premarket_material(
            session_date,
            decision_at=decision_at,
            retrieved_at=retrieved_at,
        )

        self.assertEqual(
            material.report.outcome,
            "NO NEW TRADE - DATA UNAVAILABLE",
        )
        self.assertIn("`SOURCE_CHECK_FAILED`", material.report.body)
        self.assertEqual(material.snapshot.candidates, ())
        collector.collect.assert_not_called()

    def test_corrupt_subject_release_normalizes_to_source_check_failure(self) -> None:
        collector = mock.Mock()
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-corrupt-evidence",
        )

        with mock.patch(
            "stock_monitor.evidence.load_current_evidence_release",
            side_effect=EvidenceRegistryError(
                "reviewed evidence child checksum mismatch"
            ),
        ), self.assertRaisesRegex(
            WorkflowDataError,
            "^SOURCE_CHECK_FAILED$",
        ):
            coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        collector.collect.assert_not_called()

    def test_calendar_and_universe_failures_normalize_to_allowlisted_reasons(self) -> None:
        from stock_monitor.market_calendar import CalendarError
        from stock_monitor.universe import UniverseError

        cases = (
            (
                "stock_monitor.market_calendar.load_current_market_calendar",
                CalendarError("calendar checksum mismatch"),
                "STALE_CALENDAR",
            ),
            (
                "stock_monitor.universe.load_current_universe",
                UniverseError("universe checksum mismatch"),
                "STALE_UNIVERSE",
            ),
        )
        for target, error, reason in cases:
            with self.subTest(reason=reason):
                collector = mock.Mock()
                coordinator = PremarketWorkflowCoordinator(
                    journal=self.journal,
                    project_root=ROOT,
                    report_archive_root=self.archive_root,
                    collector=collector,
                    risk_resolver=None,
                    validation_window_id=f"task10-{reason.lower()}",
                )
                with mock.patch(target, side_effect=error), self.assertRaisesRegex(
                    WorkflowDataError,
                    f"^{reason}$",
                ):
                    coordinator.premarket_material(
                        DAY,
                        decision_at=DECISION_AT,
                        retrieved_at=RETRIEVED_AT,
                    )
                collector.collect.assert_not_called()

    def test_missing_subject_file_normalizes_to_source_check_failure(self) -> None:
        import stock_monitor.evidence as evidence_module

        project, evidence_sha256, *_unused = self._fresh_open_project()
        (project / "data/evidence/subjects/AAPL.json").unlink()
        collector = mock.Mock()
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=project,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-missing-subject-file",
        )

        with mock.patch.object(
            evidence_module,
            "CURRENT_EVIDENCE_RELEASE_SHA256",
            evidence_sha256,
        ), self.assertRaisesRegex(
            WorkflowDataError,
            "^SOURCE_CHECK_FAILED$",
        ):
            coordinator.premarket_material(
                date(2026, 8, 24),
                decision_at=datetime(2026, 8, 24, 8, 45, tzinfo=ET),
                retrieved_at=datetime(2026, 8, 24, 8, 52, tzinfo=ET),
            )

        collector.collect.assert_not_called()

    def test_expected_provider_exception_normalizes_to_provider_check_failure(self) -> None:
        from stock_monitor.providers.http import ProviderResponseError

        collector = mock.Mock()
        collector.collect.side_effect = ProviderResponseError(
            "reference source unavailable"
        )
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-provider-exception",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ), self.assertRaisesRegex(
            WorkflowDataError,
            "^PROVIDER_CHECK_FAILED$",
        ):
            coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

    def test_missing_subject_evidence_is_data_unavailable_after_collection(self) -> None:
        collector = mock.Mock()
        collector.collect.return_value = PremarketProviderCollection(
            failure_reason="SOURCE_CHECK_FAILED",
        )
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-missing-subject",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ):
            material = coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        self.assertEqual(
            material.report.outcome,
            "NO NEW TRADE - DATA UNAVAILABLE",
        )
        self.assertEqual(material.snapshot.candidates, ())
        self.assertIn("`SOURCE_CHECK_FAILED`", material.report.body)
        required_symbols = collector.collect.call_args.kwargs[
            "required_symbols"
        ]
        self.assertEqual(
            required_symbols,
            tuple(record.symbol for record in self.universe.records),
        )

    def test_complete_provider_pages_persist_and_reread_before_safe_failure(self) -> None:
        pairs = tuple(
            self._provider_cohort_and_bundle(
                source_type=source_type,
                ordinal=ordinal,
            )
            for ordinal, source_type in enumerate(
                (
                    "ALPACA_DAILY_BARS",
                    "ALPACA_HISTORICAL_QUOTES",
                    "ALPACA_LATEST_QUOTES",
                ),
                start=1,
            )
        )
        cohorts = tuple(pair[0] for pair in pairs)
        bundles = {id(pair[0]): pair[1] for pair in pairs}
        collector = mock.Mock()

        def collect_failure(**_kwargs):
            persisted = provider_workflows_module._persist_premarket_provider_bindings(
                journal=self.journal,
                cohorts=cohorts,
                required_symbols=tuple(
                    record.symbol for record in self.universe.records
                ),
            )
            return PremarketProviderCollection(
                provider_cohorts=cohorts,
                persisted_bindings=persisted,
                failure_reason="DATA_UNAVAILABLE",
            )

        collector.collect.side_effect = collect_failure
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-provider-pages",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.is_issued_provider_fetch_cohort",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.provider_fetch_cohorts_share_owner",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.read_provider_fetch_bundle",
            side_effect=lambda cohort: bundles[id(cohort)],
        ), mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ):
            material = coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        page_receipts = tuple(
            receipt
            for receipt in material.source_receipts
            if receipt.source_type.startswith("ALPACA_")
        )
        self.assertEqual(len(page_receipts), 3)
        composition = material.composition_authority
        self.assertIn(
            "OPERATIONAL_HEALTH_ONLY",
            tuple(item[2] for item in composition.decision_basis),
        )
        self.assertEqual(material.snapshot.candidates, ())

    def test_provider_exception_binds_page_persisted_before_failure(self) -> None:
        _cohort, disclosure = self._provider_cohort_and_bundle(
            source_type="ALPACA_DAILY_BARS",
            ordinal=1,
        )
        page_bundle = disclosure.pages[0]
        collector = mock.Mock()

        def collect_partial(**_kwargs):
            page = page_bundle.page
            observation = page_bundle.observation
            receipt = self.journal.append_source_observation_receipt(
                payload=page_bundle.payload,
                source_uri=page.request_url,
                source_type=page.source_type,
                provider="alpaca",
                feed=observation.feed,
                source_time=observation.source_timestamp,
                retrieved_at=observation.retrieved_at,
                provider_sequence=page.page_ordinal,
                delay_seconds=observation.delay_seconds,
                health_result="OK",
                details={},
            )
            raise PremarketCollectionError(
                PremarketProviderCollection(
                    persisted_bindings=(
                        PremarketSourceBinding(
                            receipt=receipt,
                            source=page_bundle,
                            decision_basis="ECONOMIC_INPUT",
                        ),
                    ),
                    failure_reason="PROVIDER_CHECK_FAILED",
                )
            )

        collector.collect.side_effect = collect_partial
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=None,
            validation_window_id="task10-partial-page",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ), mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            side_effect=lambda value: value is page_bundle,
        ):
            material = coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        self.assertEqual(
            material.report.outcome,
            "NO NEW TRADE - DATA UNAVAILABLE",
        )
        self.assertEqual(material.snapshot.candidates, ())
        self.assertIn("`PROVIDER_CHECK_FAILED`", material.report.body)
        self.assertTrue(
            any(
                receipt.source_type == "ALPACA_DAILY_BARS"
                for receipt in material.source_receipts
            )
        )

    def test_complete_collection_resolves_breaker_before_no_candidate_branch(self) -> None:
        events: list[tuple[str, tuple[int, ...]]] = []
        original_read_receipts = Journal.read_source_observation_receipts
        pairs = tuple(
            self._provider_cohort_and_bundle(
                source_type=source_type,
                ordinal=ordinal,
            )
            for ordinal, source_type in enumerate(
                (
                    "ALPACA_DAILY_BARS",
                    "ALPACA_HISTORICAL_QUOTES",
                    "ALPACA_LATEST_QUOTES",
                ),
                start=1,
            )
        )
        cohorts = tuple(pair[0] for pair in pairs)
        bundles = {id(pair[0]): pair[1] for pair in pairs}
        collector = mock.Mock()

        def collect_success(**_kwargs):
            _owner, documents, reference_bindings = (
                self._reference_documents_and_bindings()
            )
            persisted = provider_workflows_module._persist_premarket_provider_bindings(
                journal=self.journal,
                cohorts=cohorts,
                required_symbols=tuple(
                    record.symbol for record in self.universe.records
                ),
            )
            self._expected_collection_receipt_ids = tuple(
                binding.receipt.row_id
                for binding in (*persisted, *reference_bindings)
            )
            return PremarketProviderCollection(
                provider_cohorts=cohorts,
                reference_sources=documents,
                contexts=tuple(object() for _record in self.universe.records),
                persisted_bindings=(*persisted, *reference_bindings),
            )

        collector.collect.side_effect = collect_success
        breaker = object()
        resolver = mock.Mock()

        def resolve(**_kwargs):
            events.append(("resolve", ()))
            return PremarketRiskResolution(breaker_state=breaker)

        resolver.resolve.side_effect = resolve

        def read_receipts(journal, row_ids):
            identifiers = tuple(row_ids)
            events.append(("read", identifiers))
            return original_read_receipts(journal, identifiers)

        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=resolver,
            validation_window_id="task10-no-candidates",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.is_issued_provider_fetch_cohort",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.provider_fetch_cohorts_share_owner",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.read_provider_fetch_bundle",
            side_effect=lambda cohort: bundles[id(cohort)],
        ), mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), mock.patch.object(
            provider_workflows_module,
            "_rank_premarket_contexts",
            return_value=(),
            create=True,
        ), mock.patch(
            "stock_monitor.risk.is_issued_breaker_state",
            side_effect=lambda value: value is breaker,
        ), mock.patch(
            "stock_monitor.risk.breaker_pauses_entry",
            return_value=False,
        ), mock.patch.object(
            Journal,
            "read_source_observation_receipts",
            autospec=True,
            side_effect=read_receipts,
        ):
            material = coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        self.assertEqual(material.report.outcome, "NO TRADE")
        self.assertIn("`NO_CANDIDATES`", material.report.body)
        self.assertEqual(material.snapshot.candidates, ())
        resolver.resolve.assert_called_once()
        self.assertEqual(events[-1][0], "resolve")
        self.assertEqual(events[-2][0], "read")
        self.assertTrue(
            set(self._expected_collection_receipt_ids).issubset(events[-2][1])
        )

    def test_authoritative_no_trade_cohort_maps_to_no_candidates(self) -> None:
        from stock_monitor.screening import CohortDecision

        cohort = CohortDecision(
            status="NO_TRADE",
            contexts=(),
            eligibility=(),
            reason_codes=("NO_BASE_ELIGIBLE_CANDIDATES",),
        )

        self.assertEqual(
            provider_workflows_module._rank_premarket_cohort(cohort),
            (),
        )

    def test_active_breaker_precedes_data_unavailable_cohort(self) -> None:
        breaker = object()
        collector = mock.Mock()
        collector.collect.return_value = PremarketProviderCollection()
        resolver = mock.Mock()
        resolver.resolve.return_value = PremarketRiskResolution(
            breaker_state=breaker,
        )
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=ROOT,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=resolver,
            validation_window_id="task10-breaker-precedence",
        )

        with mock.patch(
            "stock_monitor.market_calendar.MarketCalendar.is_open",
            return_value=True,
        ), mock.patch.object(
            provider_workflows_module,
            "_require_complete_premarket_provider_handoff",
        ), mock.patch.object(
            provider_workflows_module,
            "_require_complete_premarket_reference_handoff",
        ), mock.patch.object(
            provider_workflows_module,
            "_rank_premarket_contexts",
            return_value=None,
        ), mock.patch(
            "stock_monitor.risk.is_issued_breaker_state",
            side_effect=lambda value: value is breaker,
        ), mock.patch(
            "stock_monitor.risk.breaker_pauses_entry",
            return_value=True,
        ):
            material = coordinator.premarket_material(
                DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
            )

        self.assertEqual(material.report.outcome, "NO TRADE")
        self.assertEqual(material.snapshot, PremarketSnapshot((), True))
        self.assertIn("`ACTIVE_BREAKER`", material.report.body)

    def test_complete_cohort_sizes_only_primary_with_real_authorities(self) -> None:
        import stock_monitor.evidence as evidence_module
        import stock_monitor.ledger as ledger_module
        import stock_monitor.risk as risk_module
        import stock_monitor.screening as screening_module
        from stock_monitor.risk import LongPlanRequest, SessionCalendarResolver
        from tests.support import policy_fixture

        session_date = date(2026, 8, 24)
        decision_at = datetime(2026, 8, 24, 8, 45, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 24, 8, 52, tzinfo=ET)
        (
            project,
            evidence_sha256,
            scoped_authorities,
            clear_authorities,
            scoped_sources,
        ) = self._fresh_open_project()
        calendar_resolver = SessionCalendarResolver(
            (load_current_market_calendar(project, as_of=session_date),)
        )
        window_id = "1" * 64
        self.journal.start_phase1_validation_window(
            window_id=window_id,
            started_session=date(2026, 8, 21),
            starting_capital=Decimal("5000"),
            started_at=datetime(2026, 8, 21, 16, tzinfo=ET),
            received_at=datetime(2026, 8, 21, 16, tzinfo=ET),
            calendar_resolver=calendar_resolver,
        )

        class Resolver:
            def resolve(inner_self, **kwargs):
                del inner_self
                resolver = SessionCalendarResolver((kwargs["calendar"],))
                replay_source = self.journal._read_phase1_canonical_replay_source(
                    query_cutoff=kwargs["decision_at"],
                )
                replay = ledger_module._issue_canonical_ledger_replay_from_phase1_source(
                    replay_source
                )
                previous = resolver.previous_session(kwargs["session_date"])
                history_source = self.journal._read_phase1_breaker_history_source(
                    ledger_name="CANONICAL",
                    through_session=previous,
                    query_cutoff=kwargs["decision_at"],
                )
                history = risk_module._issue_breaker_history_from_phase1_source(
                    history_source,
                    calendar_resolver=resolver,
                )
                breaker = risk_module.evaluate_authorized_breakers(history)
                ranked = kwargs["ranked_candidates"]
                if not ranked:
                    return PremarketRiskResolution(breaker_state=breaker)
                request = LongPlanRequest.from_scored_candidate(ranked[0])
                portfolio = risk_module._issue_portfolio_risk_authority(
                    request=request,
                    ledger_pair=replay.ledger_pair,
                    ledger_name="CANONICAL",
                    breaker_state=breaker,
                    calendar_resolver=resolver,
                    policy=policy_fixture(),
                    scope="CANONICAL_PUBLICATION",
                    as_of=kwargs["decision_at"],
                    phase1_canonical_replay=replay,
                )
                plan = risk_module.plan_long(
                    request,
                    portfolio.portfolio_state,
                    policy_fixture(),
                    portfolio_authority=portfolio,
                )
                decision = screening_module._issue_portfolio_bound_publication_decision(
                    ranked[:3],
                    primary_plan_decision=plan,
                )
                return PremarketRiskResolution(
                    breaker_state=breaker,
                    primary_plan=plan,
                    publication_decision=decision,
                )

        collector = mock.Mock()
        collector.collect.side_effect = self._complete_candidate_collection
        coordinator = PremarketWorkflowCoordinator(
            journal=self.journal,
            project_root=project,
            report_archive_root=self.archive_root,
            collector=collector,
            risk_resolver=Resolver(),
            validation_window_id=window_id,
        )

        with mock.patch.object(
            evidence_module,
            "CURRENT_EVIDENCE_RELEASE_SHA256",
            evidence_sha256,
        ), mock.patch.dict(
            evidence_module._SCOPED_REFERENCE_AUTHORITIES,
            scoped_authorities,
        ), mock.patch.dict(
            evidence_module._CLEAR_COVERAGE_AUTHORITIES,
            clear_authorities,
        ), mock.patch.object(
            provider_workflows_module,
            "_SCOPED_REFERENCE_SOURCES",
            scoped_sources,
        ), mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.is_issued_provider_fetch_cohort",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.provider_fetch_cohorts_share_owner",
            return_value=True,
        ), mock.patch(
            "stock_monitor.providers.alpaca.read_provider_fetch_bundle",
            side_effect=lambda cohort: self._candidate_disclosures[id(cohort)],
        ):
            material = coordinator.premarket_material(
                session_date,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
            )

        self.assertEqual(
            material.report.outcome,
            "CANDIDATES",
            (material.report.body, getattr(self, "_candidate_diagnostics", None)),
        )
        self.assertEqual(material.snapshot.candidates[0].role, "PRIMARY")
        self.assertGreater(material.snapshot.candidates[0].material.shares, 0)
        self.assertLessEqual(len(material.snapshot.candidates), 3)
        self.assertTrue(
            all(
                item.role == "WATCHLIST_SHADOW"
                and not hasattr(item.material, "shares")
                for item in material.snapshot.candidates[1:]
            )
        )

    def test_exact_reviewed_bytes_children_and_owner_issue_one_current_authority(self) -> None:
        original_calendar_predicate = (
            market_calendar_module.is_release_verified_market_calendar
        )
        with mock.patch.object(
            market_calendar_module,
            "is_release_verified_market_calendar",
            wraps=original_calendar_predicate,
        ) as calendar_predicate:
            authority = self._authority()
        self.assertEqual(calendar_predicate.call_count, 1)
        self.assertTrue(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )
        self.assertTrue(
            all(
                binding.receipt.health_result == "REVIEWED"
                and binding.receipt.source_payload
                for binding in provider_workflows_module._premarket_reviewed_bindings(
                    authority
                )
            )
        )
        dummy = b"{}"
        dummy_receipt = self._reviewed_receipt(
            role="calendar",
            payload=dummy,
            source_time=datetime.combine(
                self.calendar.reviewed_at,
                datetime.min.time(),
                ET,
            ),
        )
        with self.assertRaisesRegex(CanonicalMaterialError, "bytes or pin"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                bindings=(
                    PremarketSourceBinding(
                        receipt=dummy_receipt,
                        source=self.calendar,
                        decision_basis="ECONOMIC_INPUT",
                    ),
                ),
            )
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )

    def test_late_iex_is_health_only_and_details_id_has_no_authority(self) -> None:
        now = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
        client = AlpacaMarketData(
            FixtureTransport("providers/alpaca/latest-iex.json"),
            credentials(),
            now=lambda: now,
        )
        cohort = client.latest_iex_quotes(("QQQ", "SPY"))
        disclosure = read_provider_fetch_bundle(cohort["QQQ"])
        page = disclosure.pages[0]
        receipt = self.journal.append_source_observation_receipt(
            payload=page.payload,
            source_uri=page.page.request_url,
            source_type=page.page.source_type,
            provider="alpaca",
            feed=page.observation.feed,
            source_time=page.observation.source_timestamp,
            retrieved_at=page.observation.retrieved_at,
            provider_sequence=page.page.page_ordinal,
            delay_seconds=page.observation.delay_seconds,
            health_result="OK",
            details={"source_observation_id": "forged-but-ignored"},
        )
        binding = PremarketSourceBinding(
            receipt=receipt,
            source=page,
            disclosure=disclosure,
            decision_basis="OPERATIONAL_HEALTH_ONLY",
        )
        decision_at = datetime(2026, 8, 14, 8, 45, tzinfo=ET)
        retrieved_at = datetime(2026, 8, 14, 9, 0, tzinfo=ET)
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), self.assertRaisesRegex(
            CanonicalMaterialError,
            "caller-authored disclosures",
        ):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(binding,),
            )
        binding = replace(binding, disclosure=None)
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            side_effect=lambda value: value is page,
        ):
            authority = issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(binding,),
            )
        self.assertIn(
            (receipt.row_id, "ALPACA_LATEST_QUOTES", "OPERATIONAL_HEALTH_ONLY"),
            authority.decision_basis,
        )
        with mock.patch.object(
            provider_workflows_module,
            "_is_issued_provider_fetch_page_bundle",
            return_value=True,
        ), self.assertRaisesRegex(CanonicalMaterialError, "health-only"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=decision_at,
                retrieved_at=retrieved_at,
                bindings=(replace(binding, decision_basis="ECONOMIC_INPUT"),),
            )

    def test_cross_owner_copy_mutation_and_callback_fail_closed(self) -> None:
        authority = self._authority()
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                replace(authority),
                journal=self.journal,
            )
        )
        object.__setattr__(authority, "binding_digest", "f" * 64)
        self.assertFalse(
            is_issued_canonical_premarket_source_binding_authority(
                authority,
                journal=self.journal,
            )
        )

        with tempfile.TemporaryDirectory() as other_root:
            other = Journal.open(Path(other_root) / "other.sqlite3")
            self.addCleanup(other.close)
            with self.assertRaisesRegex(CanonicalMaterialError, "owner"):
                issue_canonical_premarket_source_binding_authority(
                    journal=self.journal,
                    decision_at=DECISION_AT,
                    retrieved_at=RETRIEVED_AT,
                    bindings=self._reviewed_bindings(journal=other),
                )

        bindings = self._reviewed_bindings()
        original = provider_workflows_module._verify_premarket_binding_source

        def mutate_owner(binding, **kwargs):
            result = original(binding, **kwargs)
            self.journal.append_source_observation(
                payload=b"callback",
                source_uri=self._reviewed_uri("callback", b"callback"),
                source_type="REVIEWED_ARTIFACT",
                provider="operator-reviewed",
                feed=None,
                source_time=DECISION_AT,
                retrieved_at=DECISION_AT,
                provider_sequence=None,
                delay_seconds=0,
                health_result="REVIEWED",
                details={"role": "callback"},
            )
            return result

        with mock.patch.object(
            provider_workflows_module,
            "_verify_premarket_binding_source",
            side_effect=mutate_owner,
        ), self.assertRaisesRegex(CanonicalMaterialError, "changed|current"):
            issue_canonical_premarket_source_binding_authority(
                journal=self.journal,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                bindings=bindings,
            )

    def test_composition_seals_binding_decision_basis_and_release_children(self) -> None:
        source_authority = self._authority()
        snapshot = PremarketSnapshot((), False)
        with self.assertRaisesRegex(
            CanonicalMaterialError,
            "conflict with reviewed evidence",
        ):
            issue_canonical_premarket_composition_authority(
                journal=self.journal,
                session_date=DAY,
                decision_at=DECISION_AT,
                retrieved_at=RETRIEVED_AT,
                validation_window_id="task10-foundation",
                source_binding_authority=source_authority,
                snapshot=snapshot,
                publication_decision=None,
                primary_plan=None,
                outcome="NO TRADE",
                reason_codes=("NO_CANDIDATES",),
                calendar=self.calendar,
                universe=self.universe,
                evidence_release=self.evidence,
                phase1_replay_children=(object(),),
            )
        composition = issue_canonical_premarket_composition_authority(
            journal=self.journal,
            session_date=DAY,
            decision_at=DECISION_AT,
            retrieved_at=RETRIEVED_AT,
            validation_window_id="task10-foundation",
            source_binding_authority=source_authority,
            snapshot=snapshot,
            publication_decision=None,
            primary_plan=None,
            outcome="NO TRADE",
            reason_codes=("NO_CANDIDATES",),
            calendar=self.calendar,
            universe=self.universe,
            evidence_release=self.evidence,
            phase1_replay_children=(),
        )
        self.assertEqual(
            composition.source_binding_digest,
            source_authority.binding_digest,
        )
        self.assertEqual(composition.decision_basis, source_authority.decision_basis)
        self.assertTrue(
            is_issued_canonical_premarket_composition_authority(composition)
        )
        source_candidate = provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS[
            id(source_authority)
        ]
        self.assertEqual(
            len(
                canonical_premarket_state_hash(
                    session_date=DAY,
                    decision_at=DECISION_AT,
                    retrieved_at=RETRIEVED_AT,
                    snapshot=snapshot,
                    source_receipts=tuple(
                        binding.receipt for binding in source_candidate.bindings
                    ),
                    publication_decision=None,
                    primary_plan=None,
                    validation_window_id="task10-foundation",
                    outcome="NO TRADE",
                    reason_codes=("NO_CANDIDATES",),
                    composition_authority=composition,
                )
            ),
            64,
        )
        self.assertFalse(
            is_issued_canonical_premarket_composition_authority(replace(composition))
        )

        identity = id(source_authority)
        reference = ref(source_authority)
        del source_authority
        gc.collect()
        self.assertIsNotNone(reference())
        self.assertIn(identity, provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS)

    def test_authority_registries_release_dead_capabilities(self) -> None:
        source_authority = self._authority()
        source_identity = id(source_authority)
        source_reference = ref(source_authority)
        del source_authority
        gc.collect()
        self.assertIsNone(source_reference())
        self.assertNotIn(
            source_identity,
            provider_workflows_module._ISSUED_PREMARKET_SOURCE_BINDINGS,
        )


if __name__ == "__main__":
    unittest.main()
