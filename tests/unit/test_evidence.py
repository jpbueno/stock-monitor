from __future__ import annotations

import unittest
import hashlib
import json
import pickle
import tempfile
from copy import copy, deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest import mock

from stock_monitor import evidence as evidence_module
from stock_monitor.evidence import (
    ADVERSE_TAGS,
    POSITIVE_EVENT_TYPES,
    DateRange,
    EvidenceRecord,
    classify_evidence,
)
from stock_monitor.providers.cache import SourceDocument


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISHED = datetime(2026, 8, 10, 14, 30, tzinfo=UTC)
RETRIEVED = datetime(2026, 8, 14, 12, 30, tzinfo=UTC)
AS_OF = datetime(2026, 8, 14, 12, 45, tzinfo=UTC)
SOURCE_BODY = b'{"reviewed":"not a canonical evidence source"}'
HOLD = DateRange(date(2026, 8, 14), date(2026, 8, 28))


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def sec_url(cik: str, accession: str, filename: str = "filing.htm") -> str:
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{filename}"
    )


def record_document(value: EvidenceRecord) -> dict[str, object]:
    return {
        "accession": value.accession,
        "adverse_tags": list(value.adverse_tags),
        "classification_ambiguous": value.classification_ambiguous,
        "conflicts": list(value.conflicts),
        "event_date": value.event_date.isoformat() if value.event_date else None,
        "event_kind": value.event_kind,
        "event_type": value.event_type,
        "fact": value.fact,
        "issuer_cik": value.issuer_cik,
        "primary_url": value.primary_url,
        "published_at": iso(value.published_at),
        "publisher": value.publisher,
        "record_id": value.record_id,
        "retrieved_at": iso(value.retrieved_at),
        "source_observation_ids": list(value.source_observation_ids),
        "symbol": value.symbol,
    }


def registry_record_document(value: EvidenceRecord) -> dict[str, object]:
    return {**record_document(value), "content_hash": value.content_hash}


def primary_body(value: EvidenceRecord, *, subject_kind: str) -> bytes:
    payload = {
        "kind": "REVIEWED_PRIMARY_EVIDENCE",
        "records": [record_document(value)],
        "schema_version": 1,
        "source_observation_id": value.source_observation_ids[0],
        "subject": {
            "issuer_cik": value.issuer_cik,
            "subject_kind": subject_kind,
            "symbol": value.symbol,
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def record(**overrides: object) -> EvidenceRecord:
    values: dict[str, object] = {
        "record_id": "evidence-001",
        "symbol": "EXM",
        "issuer_cik": "0000000001",
        "primary_url": sec_url("0000000001", "0000000001-26-000001"),
        "publisher": "U.S. Securities and Exchange Commission",
        "published_at": PUBLISHED,
        "retrieved_at": RETRIEVED,
        "event_type": "material agreement",
        "fact": "The issuer signed a material customer agreement.",
        "content_hash": "0" * 64,
        "source_observation_ids": ("source-001",),
        "accession": "0000000001-26-000001",
        "adverse_tags": (),
        "conflicts": (),
        "classification_ambiguous": False,
        "event_date": None,
        "event_kind": None,
    }
    values.update(overrides)
    if "issuer_cik" in overrides and "accession" not in overrides:
        issuer_cik = values["issuer_cik"]
        values["accession"] = (
            f"{issuer_cik}-26-000001" if issuer_cik is not None else None
        )
    if "issuer_cik" in overrides and "primary_url" not in overrides:
        if values["issuer_cik"] is None:
            values["primary_url"] = (
                "https://www.ssga.com/us/en/intermediary/etfs/funds/"
                "spdr-sp-500-etf-trust-spy"
            )
            values["publisher"] = "State Street Global Advisors"
        else:
            values["primary_url"] = sec_url(
                values["issuer_cik"],  # type: ignore[arg-type]
                values["accession"],  # type: ignore[arg-type]
            )
    if "source_observation_ids" not in overrides:
        values["source_observation_ids"] = (f"source-{values['record_id']}",)
    if "content_hash" not in overrides:
        provisional = EvidenceRecord(**values)  # type: ignore[arg-type]
        subject_kind = "STOCK" if provisional.issuer_cik is not None else "ETF"
        values["content_hash"] = hashlib.sha256(
            primary_body(provisional, subject_kind=subject_kind)
        ).hexdigest()
    return EvidenceRecord(**values)  # type: ignore[arg-type]


def etf_record(**overrides: object) -> EvidenceRecord:
    values = {
        "symbol": "SPY",
        "issuer_cik": None,
        "accession": None,
        **overrides,
    }
    return record(**values)


def source_binding(
    value: EvidenceRecord,
    *,
    identifier: str | None = None,
    healthy: bool = True,
    body: bytes | None = None,
) -> object:
    subject_kind = "STOCK" if value.issuer_cik is not None else "ETF"
    source_body = body if body is not None else primary_body(
        value,
        subject_kind=subject_kind,
    )
    if value.issuer_cik is None:
        source_type = "OFFICIAL_REFERENCE"
        timestamp_source = "PRIMARY_METADATA"
        accession = None
        source_role = (
            f"ISSUER_IR:{value.symbol}"
            if value.event_type == "fund sponsor notice"
            else f"CORPORATE_ACTION:{value.symbol}"
        )
    else:
        source_type = "SEC_ARCHIVE"
        timestamp_source = "SEC_FILING_METADATA"
        accession = value.accession
        source_role = None
    document = SourceDocument(
        url=value.primary_url,
        published_at=value.published_at,
        retrieved_at=value.retrieved_at,
        content_hash=hashlib.sha256(source_body).hexdigest(),
        body=source_body,
        source_observation_id=identifier or value.source_observation_ids[0],
        publisher=value.publisher,
        source_type=source_type,
        timestamp_source=timestamp_source,
        accession=accession,
        source_role=source_role,
    )
    return evidence_module.EvidenceSourceBinding.from_document(
        document,
        symbol=value.symbol,
        issuer_cik=value.issuer_cik,
        checked_at=value.retrieved_at,
        valid_until=value.retrieved_at + timedelta(hours=24),
        healthy=healthy,
    )


def coverage_binding(
    *,
    symbol: str = "EXM",
    issuer_cik: str | None = "0000000001",
    subject_kind: str = "STOCK",
    binary_event_coverage: str = "CONFIRMED_CLEAR",
    etf_action_coverage: str = "NOT_APPLICABLE",
    healthy: bool = True,
    complete: bool = True,
) -> tuple[object, tuple[object, ...]]:
    identifier = f"coverage-{symbol.lower()}"
    attestations = coverage_attestations(
        identifier,
        subject_kind=subject_kind,
        symbol=symbol,
        issuer_cik=issuer_cik,
        binary_event_coverage=binary_event_coverage,
        etf_action_coverage=etf_action_coverage,
        healthy=healthy,
        complete=complete,
    )
    payload = {
        "attestations": [coverage_document(value) for value in attestations],
        "kind": "REVIEWED_EVIDENCE_COVERAGE",
        "schema_version": 1,
        "source_observation_id": identifier,
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    document = SourceDocument(
        url="https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        published_at=PUBLISHED,
        retrieved_at=RETRIEVED,
        content_hash=hashlib.sha256(body).hexdigest(),
        body=body,
        source_observation_id=identifier,
        publisher="Nasdaq",
        source_type="OFFICIAL_REFERENCE",
        timestamp_source="PRIMARY_METADATA",
        source_role="CROSS_CHECK_CALENDAR",
    )
    binding = evidence_module.EvidenceSourceBinding.from_document(
        document,
        symbol=symbol,
        issuer_cik=issuer_cik,
        checked_at=RETRIEVED,
        valid_until=AS_OF + timedelta(hours=1),
        healthy=healthy,
    )
    return binding, attestations


def coverage_attestations(
    binding: object,
    *,
    subject_kind: str = "STOCK",
    symbol: str = "EXM",
    issuer_cik: str | None = "0000000001",
    binary_event_coverage: str = "CONFIRMED_CLEAR",
    etf_action_coverage: str = "NOT_APPLICABLE",
    healthy: bool = True,
    complete: bool = True,
) -> tuple[object, ...]:
    identifier = (
        binding
        if isinstance(binding, str)
        else binding.source_observation_id  # type: ignore[attr-defined]
    )
    checked_at = RETRIEVED
    valid_until = AS_OF + timedelta(hours=1)
    return tuple(
        evidence_module.EvidenceCoverageAttestation(
            subject_kind=subject_kind,
            symbol=symbol,
            issuer_cik=issuer_cik,
            coverage_kind=kind,
            coverage=coverage,
            source_observation_ids=(identifier,),
            checked_at=checked_at,
            valid_until=valid_until,
            healthy=healthy,
            complete=complete,
            conflicts=(),
        )
        for kind, coverage in (
            ("BINARY_EVENT", binary_event_coverage),
            ("ETF_ACTION", etf_action_coverage),
        )
    )


def coverage_document(value: object) -> dict[str, object]:
    return {
        "checked_at": iso(value.checked_at),  # type: ignore[attr-defined]
        "complete": value.complete,  # type: ignore[attr-defined]
        "conflicts": list(value.conflicts),  # type: ignore[attr-defined]
        "coverage": value.coverage,  # type: ignore[attr-defined]
        "coverage_kind": value.coverage_kind,  # type: ignore[attr-defined]
        "healthy": value.healthy,  # type: ignore[attr-defined]
        "issuer_cik": value.issuer_cik,  # type: ignore[attr-defined]
        "source_observation_ids": list(value.source_observation_ids),  # type: ignore[attr-defined]
        "subject_kind": value.subject_kind,  # type: ignore[attr-defined]
        "symbol": value.symbol,  # type: ignore[attr-defined]
        "valid_until": iso(value.valid_until),  # type: ignore[attr-defined]
    }


def binding_document(value: object) -> dict[str, object]:
    document = value.document  # type: ignore[attr-defined]
    return {
        "accession": document.accession,
        "checked_at": iso(value.checked_at),  # type: ignore[attr-defined]
        "content_hash": document.content_hash,
        "healthy": value.healthy,  # type: ignore[attr-defined]
        "issuer_cik": value.issuer_cik,  # type: ignore[attr-defined]
        "primary_url": document.url,
        "publisher": document.publisher,
        "retrieved_at": iso(document.retrieved_at),
        "source_observation_id": document.source_observation_id,
        "source_role": document.source_role,
        "source_type": document.source_type,
        "symbol": value.symbol,  # type: ignore[attr-defined]
        "timestamp_source": document.timestamp_source,
        "valid_until": iso(value.valid_until),  # type: ignore[attr-defined]
    }


def decision(
    records: object,
    *,
    subject_kind: str = "STOCK",
    symbol: str = "EXM",
    issuer_cik: str | None = "0000000001",
    binary_event_coverage: str = "CONFIRMED_CLEAR",
    etf_action_coverage: str = "NOT_APPLICABLE",
    source_healthy: bool = True,
    coverage_healthy: bool = True,
    coverage_complete: bool = True,
    return_context: bool = False,
    registry_reviewed_at: str = "2026-08-14T12:40:00Z",
    authority_as_of: datetime = AS_OF,
) -> object:
    values = tuple(records)  # type: ignore[arg-type]
    if subject_kind == "ETF":
        if symbol == "EXM":
            symbol = values[0].symbol if values else "SPY"
        if issuer_cik == "0000000001":
            issuer_cik = None
    by_identifier: dict[str, object] = {}
    for value in values:
        for identifier in value.source_observation_ids:
            by_identifier.setdefault(
                identifier,
                source_binding(
                    value,
                    identifier=identifier,
                    healthy=source_healthy,
                ),
            )
    coverage_source, attestations = coverage_binding(
        symbol=symbol,
        issuer_cik=issuer_cik,
        subject_kind=subject_kind,
        binary_event_coverage=binary_event_coverage,
        etf_action_coverage=etf_action_coverage,
        healthy=coverage_healthy,
        complete=coverage_complete,
    )
    by_identifier[coverage_source.source_observation_id] = coverage_source  # type: ignore[attr-defined]
    bindings = tuple(
        by_identifier[identifier] for identifier in sorted(by_identifier)
    )
    registry_document = {
        "coverage_attestations": [coverage_document(value) for value in attestations],
        "kind": "REVIEWED_EVIDENCE_BUNDLE",
        "records": [registry_record_document(value) for value in values],
        "registry_id": "test-reviewed-bundle",
        "reviewed_at": registry_reviewed_at,
        "schema_version": 2,
        "source_bindings": [binding_document(value) for value in bindings],
        "subject": {
            "issuer_cik": issuer_cik,
            "subject_kind": subject_kind,
            "symbol": symbol,
        },
    }
    payload = json.dumps(
        registry_document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256(payload).hexdigest()
    with tempfile.TemporaryDirectory() as directory:
        project_root = Path(directory)
        path = project_root / "data" / "evidence"
        path.mkdir(parents=True)
        (path / "current.json").write_bytes(payload)
        with mock.patch.object(
            evidence_module,
            "CURRENT_EVIDENCE_REGISTRY_SHA256",
            digest,
        ):
            authority = evidence_module.load_current_evidence_bundle(
                project_root,
                as_of=authority_as_of,
                source_documents={
                    value.source_observation_id: value.document  # type: ignore[attr-defined]
                    for value in bindings
                },
            )
    if return_context:
        return values, symbol, issuer_cik, bindings, attestations, authority
    return classify_evidence(
        values,
        HOLD,
        symbol=symbol,
        issuer_cik=issuer_cik,
        source_bindings=bindings,
        as_of=AS_OF,
        subject_kind=subject_kind,
        coverage_attestations=attestations,
        reviewed_bundle=authority,
    )


class EvidenceClassificationTests(unittest.TestCase):
    def test_taxonomy_is_exact_and_closed(self) -> None:
        self.assertEqual(
            POSITIVE_EVENT_TYPES,
            (
                "financial results/guidance",
                "material agreement",
                "product/regulatory milestone",
                "capital allocation",
                "management/governance",
                "acquisition/disposition",
            ),
        )
        self.assertEqual(
            ADVERSE_TAGS,
            (
                "lowered guidance",
                "restatement",
                "default/bankruptcy",
                "enforcement action",
                "product recall/regulatory rejection",
                "going-concern",
                "dilutive financing",
            ),
        )

    def test_each_exact_positive_category_remains_a_raw_qualifying_record(self) -> None:
        records = tuple(
            record(
                record_id=f"evidence-{index}",
                event_type=event_type,
                primary_url=sec_url(
                    "0000000001",
                    "0000000001-26-000001",
                    f"filing-{index}.htm",
                ),
            )
            for index, event_type in enumerate(POSITIVE_EVENT_TYPES, start=1)
        )
        result = decision(records)

        self.assertIsNone(result.block_reason)
        self.assertEqual(result.qualifying_records, records)
        self.assertEqual(result.adverse_tags, ())
        self.assertFalse(hasattr(result, "catalyst_points"))

    def test_every_exact_adverse_tag_blocks_and_cannot_qualify(self) -> None:
        for tag in ADVERSE_TAGS:
            with self.subTest(tag=tag):
                result = decision([record(adverse_tags=(tag,))])
                self.assertEqual(result.block_reason, "ADVERSE_EVENT")
                self.assertEqual(result.adverse_tags, (tag,))
                self.assertEqual(result.qualifying_records, ())

    def test_unknown_or_explicitly_ambiguous_classification_blocks(self) -> None:
        cases = (
            record(event_type="rumor"),
            record(classification_ambiguous=True),
            record(event_type=None),
        )
        for value in cases:
            with self.subTest(value=value):
                result = decision([value])
                self.assertEqual(
                    result.block_reason,
                    "AMBIGUOUS_EVIDENCE_CLASSIFICATION",
                )
                self.assertTrue(result.ambiguities)

    def test_conflicting_primary_facts_block(self) -> None:
        result = decision(
            [record(conflicts=("issuer date differs from SEC filing",))]
        )
        self.assertEqual(result.block_reason, "EVIDENCE_SOURCE_CONFLICT")
        self.assertEqual(
            result.conflicts,
            ("issuer date differs from SEC filing",),
        )

    def test_binary_and_etf_actions_inside_hold_are_preserved_and_block(self) -> None:
        for event_kind, expected in (
            ("BINARY_EVENT", "BINARY_EVENT_DURING_HOLD"),
            ("ETF_ACTION", "ETF_ACTION_DURING_HOLD"),
        ):
            value = (
                etf_record if event_kind == "ETF_ACTION" else record
            )(
                event_date=date(2026, 8, 20),
                event_kind=event_kind,
                event_type=(
                    "fund sponsor notice"
                    if event_kind == "ETF_ACTION"
                    else "material agreement"
                ),
            )
            with self.subTest(event_kind=event_kind):
                result = decision(
                    [value],
                    subject_kind=("ETF" if event_kind == "ETF_ACTION" else "STOCK"),
                    binary_event_coverage=(
                        "NOT_APPLICABLE"
                        if event_kind == "ETF_ACTION"
                        else "CONFIRMED_CLEAR"
                    ),
                    etf_action_coverage=(
                        "CONFIRMED_CLEAR"
                        if event_kind == "ETF_ACTION"
                        else "NOT_APPLICABLE"
                    ),
                )
                self.assertEqual(result.block_reason, expected)
                dates = (
                    result.binary_events
                    if event_kind == "BINARY_EVENT"
                    else result.etf_actions
                )
                self.assertEqual(dates, ((date(2026, 8, 20), value.event_type),))

    def test_confirmed_dated_events_outside_hold_remain_visible_without_blocking(self) -> None:
        binary = record(
            record_id="binary-later",
            event_date=date(2026, 9, 15),
            event_kind="BINARY_EVENT",
        )
        action = etf_record(
            record_id="etf-later",
            event_date=date(2026, 9, 20),
            event_kind="ETF_ACTION",
            event_type="fund sponsor notice",
        )
        result = decision([binary])
        etf_result = decision(
            [action],
            subject_kind="ETF",
            binary_event_coverage="NOT_APPLICABLE",
            etf_action_coverage="CONFIRMED_CLEAR",
        )
        self.assertIsNone(result.block_reason)
        self.assertIsNone(etf_result.block_reason)
        self.assertEqual(
            result.binary_events,
            ((date(2026, 9, 15), "material agreement"),),
        )
        self.assertEqual(
            etf_result.etf_actions,
            ((date(2026, 9, 20), "fund sponsor notice"),),
        )

    def test_missing_or_unhealthy_source_health_is_an_explicit_block(self) -> None:
        self.assertIsNone(decision([]).block_reason)
        result = decision([record()], source_healthy=False)
        self.assertEqual(result.block_reason, "EVIDENCE_SOURCE_UNAVAILABLE")
        self.assertEqual(result.health, "UNAVAILABLE")
        self.assertEqual(result.retrieved_at, RETRIEVED)

    def test_provenance_fields_are_required_and_validated(self) -> None:
        invalid = (
            {"primary_url": "http://example.com/event"},
            {"primary_url": "https://example.com/event?access_token=canary"},
            {"primary_url": "https://example.com/event?api-key=canary"},
            {"primary_url": "https://example.com/event?client.secret=canary"},
            {"publisher": ""},
            {"published_at": datetime(2026, 8, 10)},
            {"retrieved_at": datetime(2026, 8, 9, tzinfo=UTC)},
            {"fact": ""},
            {"fact": "First sentence.\nSecond sentence."},
            {"content_hash": "not-a-sha256"},
            {"source_observation_ids": ()},
            {"adverse_tags": ("negative sentiment",)},
        )
        for override in invalid:
            with self.subTest(override=override), self.assertRaises(ValueError):
                record(**override)

    def test_duplicate_conflict_descriptions_are_canonicalized(self) -> None:
        first = record(conflicts=("issuer and SEC dates differ",))
        second = record(
            record_id="evidence-002",
            primary_url=sec_url(
                "0000000001", "0000000001-26-000001", "second.htm"
            ),
            conflicts=("issuer and SEC dates differ",),
        )
        result = decision([second, first])
        self.assertEqual(result.conflicts, ("issuer and SEC dates differ",))

    def test_decision_order_is_deterministic_and_preserves_accession_and_sources(self) -> None:
        newer = record(
            record_id="evidence-newer",
            published_at=datetime(2026, 8, 12, tzinfo=UTC),
            primary_url=sec_url(
                "0000000001", "0000000001-26-000001", "new.htm"
            ),
        )
        older = record(
            record_id="evidence-older",
            published_at=datetime(2026, 8, 1, tzinfo=UTC),
            primary_url=sec_url(
                "0000000001", "0000000001-26-000001", "old.htm"
            ),
        )
        result = decision([newer, older])
        self.assertEqual(result.qualifying_records, (newer, older))
        self.assertEqual(result.qualifying_records[0].accession, newer.accession)
        self.assertEqual(
            result.qualifying_records[0].source_observation_ids,
            ("source-evidence-newer",),
        )

    def test_current_evidence_file_is_reviewed_empty_registry_not_live_cache(self) -> None:
        path = PROJECT_ROOT / "data" / "evidence" / "current.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(document["schema_version"], 2)
        self.assertEqual(document["kind"], "REVIEWED_EVIDENCE_BUNDLE")
        self.assertIsNone(document["subject"])
        self.assertEqual(document["records"], [])
        serialized = json.dumps(document).lower()
        for prohibited in (
            "authorization",
            "api_key",
            "secret",
            "raw_payload",
            "live_cache",
        ):
            self.assertNotIn(prohibited, serialized)

    def test_authoritative_as_of_rejects_future_and_stale_observations(self) -> None:
        future = record(
            published_at=AS_OF + timedelta(microseconds=1),
            retrieved_at=AS_OF + timedelta(microseconds=1),
        )
        values, symbol, issuer_cik, bindings, attestations, authority = decision(
            [future],
            binary_event_coverage="CONFIRMED_CLEAR",
            etf_action_coverage="NOT_APPLICABLE",
            return_context=True,
            registry_reviewed_at="2026-08-14T12:46:00Z",
            authority_as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
        )
        classified = classify_evidence(
            values,
            HOLD,
            symbol=symbol,
            issuer_cik=issuer_cik,
            source_bindings=bindings,
            as_of=AS_OF,
            subject_kind="STOCK",
            coverage_attestations=attestations,
            reviewed_bundle=authority,
        )
        self.assertEqual(classified.block_reason, "EVIDENCE_TIMESTAMP_IN_FUTURE")
        self.assertEqual(classified.as_of, AS_OF)

        stale = record(
            published_at=AS_OF - timedelta(days=5),
            retrieved_at=AS_OF - timedelta(hours=24, microseconds=1),
        )
        classified = decision(
            [stale],
            binary_event_coverage="CONFIRMED_CLEAR",
            etf_action_coverage="NOT_APPLICABLE",
        )
        self.assertEqual(classified.block_reason, "EVIDENCE_SOURCE_STALE")
        self.assertEqual(classified.as_of, AS_OF)

    def test_classifier_rejects_registry_reviewed_after_authoritative_as_of(self) -> None:
        values, symbol, issuer_cik, bindings, attestations, authority = decision(
            [],
            return_context=True,
            registry_reviewed_at="2026-08-14T13:00:00Z",
            authority_as_of=datetime(2026, 8, 14, 13, 1, tzinfo=UTC),
        )
        result = classify_evidence(
            values,
            HOLD,
            symbol=symbol,
            issuer_cik=issuer_cik,
            source_bindings=bindings,
            as_of=AS_OF,
            subject_kind="STOCK",
            coverage_attestations=attestations,
            reviewed_bundle=authority,
        )
        self.assertEqual(result.block_reason, "EVIDENCE_TIMESTAMP_IN_FUTURE")

    def test_registry_cannot_claim_review_before_its_source_observations(self) -> None:
        with self.assertRaises(evidence_module.EvidenceRegistryError):
            decision(
                [],
                registry_reviewed_at="2020-01-01T00:00:00Z",
            )

    def test_decision_exposes_explicit_hold_event_coverage(self) -> None:
        clear = decision(
            [record()],
            binary_event_coverage="CONFIRMED_CLEAR",
            etf_action_coverage="NOT_APPLICABLE",
        )
        self.assertIsNone(clear.block_reason)
        self.assertEqual(clear.binary_event_coverage, "CONFIRMED_CLEAR")
        self.assertEqual(clear.etf_action_coverage, "NOT_APPLICABLE")

        states = (
            ("STOCK", "UNKNOWN", "NOT_APPLICABLE", "BINARY_EVENT_STATUS_UNKNOWN"),
            ("ETF", "NOT_APPLICABLE", "UNKNOWN", "ETF_ACTION_STATUS_UNKNOWN"),
            (
                "STOCK",
                "CONFLICT",
                "NOT_APPLICABLE",
                "EVIDENCE_EVENT_COVERAGE_CONFLICT",
            ),
        )
        for subject_kind, binary_coverage, etf_coverage, expected in states:
            with self.subTest(expected=expected):
                result = decision(
                    [
                        (
                            etf_record if subject_kind == "ETF" else record
                        )(
                            event_type=(
                                "fund sponsor notice"
                                if subject_kind == "ETF"
                                else "material agreement"
                            )
                        )
                    ],
                    subject_kind=subject_kind,
                    binary_event_coverage=binary_coverage,
                    etf_action_coverage=etf_coverage,
                )
                self.assertEqual(result.block_reason, expected)

        outside_but_not_exhaustive = decision(
            [
                record(
                    event_kind="BINARY_EVENT",
                    event_date=date(2026, 9, 1),
                )
            ],
            binary_event_coverage="UNKNOWN",
            etf_action_coverage="NOT_APPLICABLE",
        )
        self.assertEqual(
            outside_but_not_exhaustive.binary_event_coverage,
            "UNKNOWN",
        )
        self.assertEqual(
            outside_but_not_exhaustive.block_reason,
            "BINARY_EVENT_STATUS_UNKNOWN",
        )

    def test_duplicate_or_conflicting_record_ids_are_rejected(self) -> None:
        first = record(record_id="duplicate-id")
        exact_duplicate = record(record_id="duplicate-id")
        conflict = record(
            record_id="duplicate-id",
            fact="The issuer announced a different material agreement.",
            primary_url=sec_url(
                "0000000001", "0000000001-26-000001", "other.htm"
            ),
        )
        for values in ((first, exact_duplicate), (first, conflict)):
            with self.subTest(values=values), self.assertRaises(
                evidence_module.EvidenceRegistryError
            ):
                decision(
                    values,
                    binary_event_coverage="CONFIRMED_CLEAR",
                    etf_action_coverage="NOT_APPLICABLE",
                )

    def test_source_health_has_no_implicit_healthy_default(self) -> None:
        binding = source_binding(record())
        with self.assertRaises(TypeError):
            evidence_module.EvidenceSourceBinding.from_document(
                binding.document,
                symbol="EXM",
                issuer_cik="0000000001",
                checked_at=RETRIEVED,
                valid_until=AS_OF + timedelta(hours=1),
            )

    def test_evidence_is_bound_to_subject_content_and_source_observation(self) -> None:
        value = record()
        binding = source_binding(value)
        result = decision([value])
        self.assertIsNone(result.block_reason)
        self.assertEqual(result.symbol, "EXM")
        self.assertEqual(result.issuer_cik, "0000000001")

        wrong_body = b"different verified source bytes"
        wrong_document = SourceDocument(
            url=value.primary_url,
            published_at=value.published_at,
            retrieved_at=value.retrieved_at,
            content_hash=hashlib.sha256(wrong_body).hexdigest(),
            body=wrong_body,
            source_observation_id=value.source_observation_ids[0],
            publisher=value.publisher,
            source_type="SEC_ARCHIVE",
            timestamp_source="SEC_FILING_METADATA",
            accession=value.accession,
        )
        wrong_hash = evidence_module.EvidenceSourceBinding.from_document(
            wrong_document,
            symbol=value.symbol,
            issuer_cik=value.issuer_cik,
            checked_at=value.retrieved_at,
            valid_until=value.retrieved_at + timedelta(hours=24),
            healthy=True,
        )
        with self.assertRaises(evidence_module.EvidenceUnavailableError):
            classify_evidence(
                [value],
                HOLD,
                symbol="EXM",
                issuer_cik="0000000001",
                source_bindings=(wrong_hash,),
                as_of=AS_OF,
                binary_event_coverage="CONFIRMED_CLEAR",
                etf_action_coverage="NOT_APPLICABLE",
            )

    def test_cross_issuer_or_unbound_evidence_fails_closed(self) -> None:
        value = record(symbol="OTHER", issuer_cik="0000000002")
        for bindings in ((source_binding(value),), ()):
            with self.subTest(bindings=bindings), self.assertRaises(
                evidence_module.EvidenceUnavailableError
            ):
                classify_evidence(
                    [value],
                    HOLD,
                    symbol="EXM",
                    issuer_cik="0000000001",
                    source_bindings=bindings,
                    as_of=AS_OF,
                    binary_event_coverage="CONFIRMED_CLEAR",
                    etf_action_coverage="NOT_APPLICABLE",
                )

    def test_reviewed_registry_loader_verifies_checksum_and_schema(self) -> None:
        path = PROJECT_ROOT / "data" / "evidence" / "current.json"
        payload = path.read_bytes()
        expected = evidence_module.CURRENT_EVIDENCE_REGISTRY_SHA256
        self.assertEqual(hashlib.sha256(payload).hexdigest(), expected)
        registry = evidence_module.load_evidence_registry(
            path,
            expected_sha256=expected,
            as_of=AS_OF,
        )
        self.assertEqual(registry.content_hash, expected)
        self.assertEqual(registry.records, ())
        self.assertEqual(registry.source_bindings, ())

        with tempfile.TemporaryDirectory() as directory:
            tampered = Path(directory) / "current.json"
            tampered.write_bytes(payload.replace(b'"records": []', b'"records": [{}]'))
            with self.assertRaises(evidence_module.EvidenceRegistryError):
                evidence_module.load_evidence_registry(
                    tampered,
                    expected_sha256=expected,
                    as_of=AS_OF,
                )

    def test_nonempty_registry_requires_verified_source_document_bytes(self) -> None:
        value = record()
        binding = source_binding(value)
        coverage_source, attestations = coverage_binding()
        bindings = (binding, coverage_source)
        registry_document = {
            "schema_version": 2,
            "kind": "REVIEWED_EVIDENCE_BUNDLE",
            "registry_id": "registry-with-record",
            "reviewed_at": "2026-08-14T12:40:00Z",
            "subject": {
                "subject_kind": "STOCK",
                "symbol": "EXM",
                "issuer_cik": "0000000001",
            },
            "records": [registry_record_document(value)],
            "source_bindings": [binding_document(item) for item in bindings],
            "coverage_attestations": [
                coverage_document(item) for item in attestations
            ],
        }
        payload = json.dumps(
            registry_document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_bytes(payload)
            with self.assertRaises(evidence_module.EvidenceRegistryError):
                evidence_module.load_evidence_registry(
                    path,
                    expected_sha256=digest,
                    as_of=AS_OF,
                )
            loaded = evidence_module.load_evidence_registry(
                path,
                expected_sha256=digest,
                as_of=AS_OF,
                source_documents={
                    item.source_observation_id: item.document  # type: ignore[attr-defined]
                    for item in bindings
                },
            )
        self.assertEqual(loaded.records, (value,))

    def test_bare_caller_coverage_strings_cannot_authorize_a_clear_decision(self) -> None:
        value = record()
        with self.assertRaises(evidence_module.EvidenceUnavailableError):
            classify_evidence(
                [value],
                HOLD,
                symbol="EXM",
                issuer_cik="0000000001",
                source_bindings=(source_binding(value),),
                as_of=AS_OF,
                binary_event_coverage="CONFIRMED_CLEAR",
                etf_action_coverage="NOT_APPLICABLE",
            )

    def test_etf_empty_catalysts_are_healthy_with_bound_complete_coverage(self) -> None:
        result = decision(
            [],
            symbol="SPY",
            issuer_cik=None,
            subject_kind="ETF",
            binary_event_coverage="NOT_APPLICABLE",
            etf_action_coverage="CONFIRMED_CLEAR",
        )
        self.assertEqual(result.health, "HEALTHY")
        self.assertIsNone(result.block_reason)
        self.assertEqual(result.qualifying_records, ())
        self.assertEqual(result.binary_event_coverage, "NOT_APPLICABLE")
        self.assertEqual(result.etf_action_coverage, "CONFIRMED_CLEAR")

    def test_etf_official_notice_categories_qualify_without_expanding_stock_taxonomy(self) -> None:
        self.assertEqual(
            evidence_module.ETF_POSITIVE_EVENT_TYPES,
            ("fund sponsor notice", "index provider notice"),
        )
        for event_type in evidence_module.ETF_POSITIVE_EVENT_TYPES:
            with self.subTest(event_type=event_type):
                self.assertNotIn(event_type, POSITIVE_EVENT_TYPES)
                value = etf_record(event_type=event_type)
                result = decision(
                    [value],
                    subject_kind="ETF",
                    binary_event_coverage="NOT_APPLICABLE",
                    etf_action_coverage="CONFIRMED_CLEAR",
                )
                self.assertIsNone(result.block_reason)
                self.assertEqual(result.qualifying_records, (value,))

    def test_decision_source_observation_ids_are_unique_and_sorted(self) -> None:
        first = record(record_id="shared-source-a")
        second = record(
            record_id="shared-source-b",
            fact="The issuer also authorized a capital allocation program.",
            event_type="capital allocation",
        )
        result = decision([second, first])
        self.assertEqual(
            result.source_observation_ids,
            (
                "coverage-exm",
                "source-shared-source-a",
                "source-shared-source-b",
            ),
        )

    def test_locked_two_argument_call_fails_typed_before_inspecting_records(self) -> None:
        class PoisonedRecords:
            def __init__(self) -> None:
                self.inspected = False

            def __iter__(self):
                self.inspected = True
                raise AssertionError("records must not be inspected without authority")

        values = PoisonedRecords()
        with self.assertRaises(RuntimeError) as raised:
            classify_evidence(values, HOLD)  # type: ignore[arg-type]
        self.assertEqual(type(raised.exception).__name__, "EvidenceUnavailableError")
        self.assertFalse(values.inspected)

    def test_each_required_classification_context_field_fails_typed_when_omitted(self) -> None:
        value = record()
        binding = source_binding(value)
        kwargs: dict[str, object] = {
            "symbol": value.symbol,
            "issuer_cik": value.issuer_cik,
            "source_bindings": (binding,),
            "as_of": AS_OF,
            "subject_kind": "STOCK",
            "coverage_attestations": coverage_attestations(binding),
        }
        for omitted in ("symbol", "issuer_cik", "source_bindings", "as_of"):
            options = dict(kwargs)
            options.pop(omitted)
            with self.subTest(omitted=omitted):
                with self.assertRaises(RuntimeError) as raised:
                    classify_evidence([object()], HOLD, **options)  # type: ignore[arg-type]
                self.assertEqual(
                    type(raised.exception).__name__,
                    "EvidenceUnavailableError",
                )

    def test_subject_kind_must_be_explicit_even_with_valid_reviewed_authority(self) -> None:
        values, symbol, issuer_cik, bindings, attestations, authority = decision(
            [record()],
            return_context=True,
        )
        with self.assertRaises(evidence_module.EvidenceUnavailableError):
            classify_evidence(
                values,
                HOLD,
                symbol=symbol,
                issuer_cik=issuer_cik,
                source_bindings=bindings,
                coverage_attestations=attestations,
                as_of=AS_OF,
                reviewed_bundle=authority,
            )

    def test_stock_requires_explicit_issuer_and_empty_binding_context_is_unavailable(self) -> None:
        with self.assertRaises(RuntimeError) as missing_cik:
            classify_evidence(
                [object()],
                HOLD,
                symbol="EXM",
                issuer_cik=None,
                source_bindings=(object(),),  # type: ignore[arg-type]
                as_of=AS_OF,
                subject_kind="STOCK",
            )
        self.assertEqual(type(missing_cik.exception).__name__, "EvidenceUnavailableError")

        class PoisonedRecords:
            inspected = False

            def __iter__(self):
                self.inspected = True
                raise AssertionError("records must not be inspected without bindings")

        values = PoisonedRecords()
        with self.assertRaises(RuntimeError) as empty_bindings:
            classify_evidence(
                values,  # type: ignore[arg-type]
                HOLD,
                symbol="EXM",
                issuer_cik="0000000001",
                source_bindings=(),
                as_of=AS_OF,
                subject_kind="STOCK",
            )
        self.assertEqual(
            type(empty_bindings.exception).__name__,
            "EvidenceUnavailableError",
        )
        self.assertFalse(values.inspected)

    def test_unbound_source_document_is_typed_evidence_unavailable(self) -> None:
        value = record()
        unrelated, unrelated_attestations = coverage_binding()
        with self.assertRaises(RuntimeError) as raised:
            classify_evidence(
                [value],
                HOLD,
                symbol=value.symbol,
                issuer_cik=value.issuer_cik,
                source_bindings=(unrelated,),  # type: ignore[arg-type]
                as_of=AS_OF,
                subject_kind="STOCK",
                coverage_attestations=unrelated_attestations,
            )
        self.assertEqual(type(raised.exception).__name__, "EvidenceUnavailableError")

    def test_freely_constructed_evidence_and_coverage_cannot_self_authorize(self) -> None:
        value = record(
            fact="This fact is not present in the alleged source bytes."
        )
        binding = source_binding(value, body=SOURCE_BODY)
        with self.assertRaises(RuntimeError) as raised:
            classify_evidence(
                [value],
                HOLD,
                symbol=value.symbol,
                issuer_cik=value.issuer_cik,
                source_bindings=(binding,),
                as_of=AS_OF,
                subject_kind="STOCK",
                coverage_attestations=coverage_attestations(binding),
            )
        self.assertEqual(type(raised.exception).__name__, "EvidenceUnavailableError")

    def test_source_binding_enforces_sec_identity_publisher_and_scoped_role(self) -> None:
        body = b'{"reviewed":"mismatched SEC evidence"}'
        sec_document = SourceDocument(
            url=(
                "https://www.sec.gov/Archives/edgar/data/2/"
                "000000000226000002/filing.htm"
            ),
            published_at=PUBLISHED,
            retrieved_at=RETRIEVED,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id="sec-wrong-subject",
            publisher="Untrusted publisher",
            source_type="SEC_ARCHIVE",
            timestamp_source="SEC_FILING_METADATA",
            accession="0000000002-26-000002",
        )
        with self.assertRaises(ValueError):
            evidence_module.EvidenceSourceBinding.from_document(
                sec_document,
                symbol="EXM",
                issuer_cik="0000000001",
                checked_at=RETRIEVED,
                valid_until=AS_OF + timedelta(hours=1),
                healthy=True,
            )

        reference_body = b'{"reviewed":"wrong issuer IR role"}'
        wrong_role = SourceDocument(
            url="https://ir.example.com/",
            published_at=PUBLISHED,
            retrieved_at=RETRIEVED,
            content_hash=hashlib.sha256(reference_body).hexdigest(),
            body=reference_body,
            source_observation_id="issuer-ir-wrong-role",
            publisher="Example Issuer",
            source_type="OFFICIAL_REFERENCE",
            timestamp_source="PRIMARY_METADATA",
            source_role="ISSUER_IR:OTHER",
        )
        with self.assertRaises(ValueError):
            evidence_module.EvidenceSourceBinding.from_document(
                wrong_role,
                symbol="EXM",
                issuer_cik="0000000001",
                checked_at=RETRIEVED,
                valid_until=AS_OF + timedelta(hours=1),
                healthy=True,
            )

    def test_registry_record_accession_must_match_bound_sec_document(self) -> None:
        value = record(accession="0000000001-26-000002")
        body = primary_body(value, subject_kind="STOCK")
        document = SourceDocument(
            url=sec_url("0000000001", "0000000001-26-000001"),
            published_at=value.published_at,
            retrieved_at=value.retrieved_at,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id=value.source_observation_ids[0],
            publisher="U.S. Securities and Exchange Commission",
            source_type="SEC_ARCHIVE",
            timestamp_source="SEC_FILING_METADATA",
            accession="0000000001-26-000001",
        )
        binding = evidence_module.EvidenceSourceBinding.from_document(
            document,
            symbol="EXM",
            issuer_cik="0000000001",
            checked_at=RETRIEVED,
            valid_until=AS_OF + timedelta(hours=1),
            healthy=True,
        )
        coverage_source, attestations = coverage_binding()
        bindings = (binding, coverage_source)
        registry_document = {
            "coverage_attestations": [
                coverage_document(item) for item in attestations
            ],
            "kind": "REVIEWED_EVIDENCE_BUNDLE",
            "records": [registry_record_document(value)],
            "registry_id": "mismatched-accession",
            "reviewed_at": "2026-08-14T12:40:00Z",
            "schema_version": 2,
            "source_bindings": [binding_document(item) for item in bindings],
            "subject": {
                "issuer_cik": "0000000001",
                "subject_kind": "STOCK",
                "symbol": "EXM",
            },
        }
        payload = json.dumps(
            registry_document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_bytes(payload)
            with self.assertRaises(evidence_module.EvidenceRegistryError):
                evidence_module.load_evidence_registry(
                    path,
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    as_of=AS_OF,
                    source_documents={
                        item.source_observation_id: item.document  # type: ignore[attr-defined]
                        for item in bindings
                    },
                )

    def test_etf_notice_cannot_qualify_from_an_unscoped_calendar_role(self) -> None:
        value = etf_record(
            event_type="fund sponsor notice",
            primary_url="https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
            publisher="Nasdaq",
        )
        body = primary_body(value, subject_kind="ETF")
        document = SourceDocument(
            url=value.primary_url,
            published_at=value.published_at,
            retrieved_at=value.retrieved_at,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id=value.source_observation_ids[0],
            publisher=value.publisher,
            source_type="OFFICIAL_REFERENCE",
            timestamp_source="PRIMARY_METADATA",
            source_role="CROSS_CHECK_CALENDAR",
        )
        binding = evidence_module.EvidenceSourceBinding.from_document(
            document,
            symbol="SPY",
            issuer_cik=None,
            checked_at=RETRIEVED,
            valid_until=AS_OF + timedelta(hours=1),
            healthy=True,
        )
        coverage_source, attestations = coverage_binding(
            symbol="SPY",
            issuer_cik=None,
            subject_kind="ETF",
            binary_event_coverage="NOT_APPLICABLE",
            etf_action_coverage="CONFIRMED_CLEAR",
        )
        bindings = (binding, coverage_source)
        registry_document = {
            "coverage_attestations": [
                coverage_document(item) for item in attestations
            ],
            "kind": "REVIEWED_EVIDENCE_BUNDLE",
            "records": [registry_record_document(value)],
            "registry_id": "wrong-etf-source-role",
            "reviewed_at": "2026-08-14T12:40:00Z",
            "schema_version": 2,
            "source_bindings": [binding_document(item) for item in bindings],
            "subject": {
                "issuer_cik": None,
                "subject_kind": "ETF",
                "symbol": "SPY",
            },
        }
        payload = json.dumps(
            registry_document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_bytes(payload)
            with self.assertRaises(evidence_module.EvidenceRegistryError):
                evidence_module.load_evidence_registry(
                    path,
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    as_of=AS_OF,
                    source_documents={
                        item.source_observation_id: item.document  # type: ignore[attr-defined]
                        for item in bindings
                    },
                )

    def test_one_sec_filing_cannot_self_attest_exhaustive_event_coverage(self) -> None:
        identifier = "sec-coverage-only"
        attestations = coverage_attestations(identifier)
        body = json.dumps(
            {
                "attestations": [
                    coverage_document(item) for item in attestations
                ],
                "kind": "REVIEWED_EVIDENCE_COVERAGE",
                "schema_version": 1,
                "source_observation_id": identifier,
                "subject": {
                    "issuer_cik": "0000000001",
                    "subject_kind": "STOCK",
                    "symbol": "EXM",
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        document = SourceDocument(
            url=sec_url("0000000001", "0000000001-26-000001"),
            published_at=PUBLISHED,
            retrieved_at=RETRIEVED,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id=identifier,
            publisher="U.S. Securities and Exchange Commission",
            source_type="SEC_ARCHIVE",
            timestamp_source="SEC_FILING_METADATA",
            accession="0000000001-26-000001",
        )
        binding = evidence_module.EvidenceSourceBinding.from_document(
            document,
            symbol="EXM",
            issuer_cik="0000000001",
            checked_at=RETRIEVED,
            valid_until=AS_OF + timedelta(hours=1),
            healthy=True,
        )
        registry_document = {
            "coverage_attestations": [
                coverage_document(item) for item in attestations
            ],
            "kind": "REVIEWED_EVIDENCE_BUNDLE",
            "records": [],
            "registry_id": "sec-self-coverage",
            "reviewed_at": "2026-08-14T12:40:00Z",
            "schema_version": 2,
            "source_bindings": [binding_document(binding)],
            "subject": {
                "issuer_cik": "0000000001",
                "subject_kind": "STOCK",
                "symbol": "EXM",
            },
        }
        payload = json.dumps(
            registry_document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_bytes(payload)
            with self.assertRaises(evidence_module.EvidenceRegistryError):
                evidence_module.load_evidence_registry(
                    path,
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    as_of=AS_OF,
                    source_documents={identifier: document},
                )

    def test_production_evidence_authority_uses_only_the_pinned_current_registry(self) -> None:
        authority = evidence_module.load_current_evidence_bundle(
            PROJECT_ROOT,
            as_of=AS_OF,
        )
        self.assertIsInstance(authority, evidence_module.ReviewedEvidenceBundle)
        self.assertEqual(
            authority.content_hash,
            evidence_module.CURRENT_EVIDENCE_REGISTRY_SHA256,
        )
        with self.assertRaises(TypeError):
            evidence_module.load_current_evidence_bundle(
                PROJECT_ROOT,
                as_of=AS_OF,
                expected_sha256="0" * 64,
            )

    def test_only_untampered_classifier_output_is_a_reviewed_decision(self) -> None:
        self.assertFalse(evidence_module.is_reviewed_evidence_decision(object()))
        reviewed = decision([])
        self.assertTrue(evidence_module.is_reviewed_evidence_decision(reviewed))
        self.assertEqual(reviewed.registry_id, "test-reviewed-bundle")
        self.assertRegex(reviewed.registry_content_hash, r"[0-9a-f]{64}\Z")

        tampered = replace(reviewed, block_reason="CALLER_OVERRIDE")
        self.assertFalse(evidence_module.is_reviewed_evidence_decision(tampered))

        class SpoofedRecords:
            def __iter__(self):
                return iter((record(record_id="attacker-record"),))

            def __repr__(self) -> str:
                return "()"

        repr_spoof = replace(reviewed, qualifying_records=SpoofedRecords())
        self.assertEqual(len(tuple(repr_spoof.qualifying_records)), 1)
        self.assertFalse(evidence_module.is_reviewed_evidence_decision(repr_spoof))

        for copied in (
            copy(reviewed),
            deepcopy(reviewed),
            pickle.loads(pickle.dumps(reviewed)),
        ):
            with self.subTest(copy_type=type(copied).__name__):
                self.assertFalse(
                    evidence_module.is_reviewed_evidence_decision(copied)
                )

    def test_recomputed_object_digest_cannot_hide_evidence_decision_tampering(self) -> None:
        reviewed = decision([record(adverse_tags=("restatement",))])
        self.assertEqual(reviewed.block_reason, "ADVERSE_EVENT")
        self.assertTrue(evidence_module.is_reviewed_evidence_decision(reviewed))

        object.__setattr__(reviewed, "adverse_tags", ())
        object.__setattr__(reviewed, "block_reason", None)
        object.__setattr__(
            reviewed,
            "_decision_digest",
            evidence_module._decision_fingerprint(reviewed),
        )

        self.assertFalse(evidence_module.is_reviewed_evidence_decision(reviewed))

    def test_dataclass_replace_cannot_preserve_reviewed_bundle_authority(self) -> None:
        values, symbol, issuer_cik, bindings, attestations, authority = decision(
            [record()],
            return_context=True,
        )
        forged = replace(authority, registry_id="attacker-registry")
        with self.assertRaises(evidence_module.EvidenceUnavailableError):
            classify_evidence(
                values,
                HOLD,
                symbol=symbol,
                issuer_cik=issuer_cik,
                source_bindings=bindings,
                as_of=AS_OF,
                subject_kind="STOCK",
                coverage_attestations=attestations,
                reviewed_bundle=forged,
            )

    def test_reviewed_bundle_authority_is_identity_and_external_fingerprint_bound(self) -> None:
        def authority() -> object:
            return decision([record()], return_context=True)[-1]

        authentic = authority()
        self.assertTrue(evidence_module._is_reviewed_bundle(authentic))
        for copied in (
            copy(authentic),
            deepcopy(authentic),
            pickle.loads(pickle.dumps(authentic)),
        ):
            with self.subTest(copy_type=type(copied).__name__):
                self.assertFalse(evidence_module._is_reviewed_bundle(copied))

        mutations = (
            ("reviewed_at", datetime(2020, 1, 1, tzinfo=UTC)),
            ("symbol", "ATTACKER"),
            ("records", ()),
        )
        for field_name, replacement in mutations:
            forged = authority()
            object.__setattr__(forged, field_name, replacement)
            object.__setattr__(
                forged,
                "_bundle_digest",
                evidence_module._bundle_fingerprint(forged),
            )
            with self.subTest(field_name=field_name):
                self.assertFalse(evidence_module._is_reviewed_bundle(forged))

    def test_coverage_subject_kind_enforces_stock_and_etf_cik_rules(self) -> None:
        binding, _ = coverage_binding()
        invalid = (
            ("STOCK", None),
            ("ETF", "0000000001"),
        )
        for subject_kind, issuer_cik in invalid:
            with self.subTest(subject_kind=subject_kind), self.assertRaises(ValueError):
                evidence_module.EvidenceCoverageAttestation(
                    subject_kind=subject_kind,
                    symbol="SPY",
                    issuer_cik=issuer_cik,
                    coverage_kind="BINARY_EVENT",
                    coverage=(
                        "CONFIRMED_CLEAR"
                        if subject_kind == "STOCK"
                        else "NOT_APPLICABLE"
                    ),
                    source_observation_ids=(binding.source_observation_id,),  # type: ignore[attr-defined]
                    checked_at=RETRIEVED,
                    valid_until=AS_OF + timedelta(hours=1),
                    healthy=True,
                    complete=True,
                    conflicts=(),
                )


if __name__ == "__main__":
    unittest.main()
