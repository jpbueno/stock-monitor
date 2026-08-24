from __future__ import annotations

import base64
import fcntl
import hashlib
import inspect
import json
import os
import stat
import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from stock_monitor import evidence as evidence_module
from stock_monitor import evidence_release_workflow as workflow_module
from stock_monitor import universe as universe_module
from stock_monitor.evidence_authorities import (
    EVIDENCE_AUTHORITIES,
    EvidenceAuthority,
)
from stock_monitor.evidence_release_workflow import (
    EvidenceProposalSummary,
    EvidenceWorkflowError,
    prepare_evidence_proposal,
)
from stock_monitor.providers.cache import SourceDocument
from stock_monitor.providers.evidence_sources import ProposalSourceObservation
from stock_monitor.universe import load_current_universe


PROJECT_ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 8, 24, 14, 0, tzinfo=UTC)
_PROPOSAL_KEYS = {
    "created_at",
    "kind",
    "observations",
    "parent_release_sha256",
    "reviewer_template_sha256",
    "schema_version",
    "source_failures",
    "subjects",
    "universe_sha256",
}
_OBSERVATION_KEYS = {
    "accession",
    "artifact_path",
    "content_sha256",
    "event_class",
    "issuer_cik",
    "observation_id",
    "published_at",
    "publisher",
    "retrieved_at",
    "role",
    "source_type",
    "symbol",
    "timestamp_source",
    "url",
}
_SAFE_REASON_CODES = frozenset(
    {
        "SEC_COLLECTOR_UNAVAILABLE",
        "SOURCE_COLLECTION_FAILED",
        "SOURCE_RESULT_INVALID",
    }
)


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _identifier(prefix: str, value: str) -> str:
    return f"{prefix}-{hashlib.sha256(value.encode('utf-8')).hexdigest()[:24]}"


class FixtureCollectors:
    def __init__(self) -> None:
        self.generic_calls: list[EvidenceAuthority] = []
        self.sec_calls: list[str] = []

    def generic_body(self, authority: EvidenceAuthority) -> bytes:
        return (
            f"official-reference:{authority.symbol}:{authority.requested_url}"
        ).encode("utf-8")

    def sec_body(self, issuer_cik: str) -> bytes:
        return f'{{"cik":"{issuer_cik}","filings":{{}}}}'.encode("ascii")

    def collect(self, authority: EvidenceAuthority) -> ProposalSourceObservation:
        if authority.role.startswith("SEC_SUBMISSIONS:"):
            raise AssertionError("SEC catalog entry reached generic collector")
        self.generic_calls.append(authority)
        body = self.generic_body(authority)
        return ProposalSourceObservation(
            observation_id=_identifier(
                "proposal",
                f"{authority.symbol}\0{authority.role}\0{authority.requested_url}",
            ),
            symbol=authority.symbol,
            issuer_cik=authority.issuer_cik,
            url=authority.requested_url,
            publisher=authority.publisher,
            role=authority.role,
            event_class=authority.event_class,
            retrieved_at=NOW,
            published_at=None,
            timestamp_source="UNAVAILABLE",
            content_sha256=hashlib.sha256(body).hexdigest(),
            body=body,
        )

    def collect_sec(self, issuer_cik: str) -> SourceDocument:
        self.sec_calls.append(issuer_cik)
        authority = next(
            item
            for item in EVIDENCE_AUTHORITIES
            if item.role.startswith("SEC_SUBMISSIONS:")
            and item.issuer_cik == issuer_cik
        )
        body = self.sec_body(issuer_cik)
        return SourceDocument(
            url=authority.requested_url,
            published_at=NOW - timedelta(minutes=30),
            retrieved_at=NOW,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id=_identifier(
                "obs",
                f"{authority.symbol}\0{authority.role}\0{authority.requested_url}",
            ),
            publisher=authority.publisher,
            source_type="SEC_SUBMISSIONS",
            timestamp_source="SEC_SUBMISSIONS_METADATA",
            accession=None,
        )


class SharedBodyCollectors(FixtureCollectors):
    def generic_body(self, authority: EvidenceAuthority) -> bytes:
        del authority
        return b"identical-official-reference-body"


class TimestampUnavailableSecCollectors(FixtureCollectors):
    def collect_sec(self, issuer_cik: str) -> SourceDocument:
        authority = next(
            item
            for item in EVIDENCE_AUTHORITIES
            if item.role.startswith("SEC_SUBMISSIONS:")
            and item.issuer_cik == issuer_cik
        )
        body = self.sec_body(issuer_cik)
        return SourceDocument(
            url=authority.requested_url,
            published_at=None,
            retrieved_at=NOW,
            content_hash=hashlib.sha256(body).hexdigest(),
            body=body,
            source_observation_id=_identifier(
                "obs",
                f"{authority.symbol}\0{authority.role}\0{authority.requested_url}",
            ),
            publisher=authority.publisher,
            source_type="SEC_SUBMISSIONS",
            timestamp_source="UNAVAILABLE",
            accession=None,
        )


@contextmanager
def _private_workspace():
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw).resolve()
        state_root = root / ".stock-monitor"
        state_root.mkdir(mode=0o700)
        yield root, state_root


def _copy_project(root: Path) -> Path:
    project_root = root / "project"
    project_root.mkdir(mode=0o700)
    for relative in (
        Path("data/universe/2026-08-22.json"),
        Path("data/evidence/current.json"),
    ):
        target = project_root / relative
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.write_bytes((PROJECT_ROOT / relative).read_bytes())
        target.chmod(0o600)
    for directory in (
        project_root / "data",
        project_root / "data/universe",
        project_root / "data/evidence",
    ):
        directory.chmod(0o700)
    return project_root


def _prepare(
    state_root: Path,
    *,
    collectors: FixtureCollectors | None = None,
    project_root: Path = PROJECT_ROOT,
    as_of: datetime = NOW,
    collect_sec: object = ...,
) -> EvidenceProposalSummary:
    fixture = collectors or FixtureCollectors()
    sec_callback = fixture.collect_sec if collect_sec is ... else collect_sec
    return prepare_evidence_proposal(
        project_root=project_root,
        state_root=state_root,
        as_of=as_of,
        collect=fixture.collect,
        collect_sec=sec_callback,  # type: ignore[arg-type]
    )


def _load_proposal(summary: EvidenceProposalSummary) -> dict[str, object]:
    value = json.loads(summary.proposal_path.read_bytes())
    assert isinstance(value, dict)
    return value


def _load_template(summary: EvidenceProposalSummary) -> dict[str, object]:
    value = json.loads(
        (summary.proposal_path.parent / "review-template.json").read_bytes()
    )
    assert isinstance(value, dict)
    return value


def _review_document(
    summary: EvidenceProposalSummary,
    *,
    reviewed_at: datetime = NOW + timedelta(minutes=5),
    review_by: datetime = NOW + timedelta(hours=23),
) -> dict[str, object]:
    proposal = _load_proposal(summary)
    raw_observations = proposal["observations"]
    raw_subjects = proposal["subjects"]
    assert isinstance(raw_observations, list)
    assert isinstance(raw_subjects, list)
    observations = [item for item in raw_observations if isinstance(item, dict)]
    subjects: list[dict[str, object]] = []
    for subject in raw_subjects:
        assert isinstance(subject, dict)
        symbol = subject["symbol"]
        assert isinstance(symbol, str)
        official = [
            item
            for item in observations
            if item["symbol"] == symbol
            and item["source_type"] == "OFFICIAL_REFERENCE"
        ]
        records: list[dict[str, object]] = []
        coverage_observation = official[0]
        if symbol == "NVDA":
            event_observation = next(
                item
                for item in official
                if item["url"]
                == "https://investor.nvidia.com/rss/Event.aspx?LanguageId=1"
            )
            coverage_observation = next(
                item for item in official if item is not event_observation
            )
            records.append(
                {
                    "adverse_tags": [],
                    "classification_ambiguous": False,
                    "conflicts": [],
                    "event_date": "2026-08-26",
                    "event_kind": "BINARY_EVENT",
                    "event_type": "financial results/guidance",
                    "fact": "NVIDIA financial results and guidance event is scheduled.",
                    "published_at": _utc_text(NOW - timedelta(minutes=15)),
                    "source_observation_ids": [
                        event_observation["observation_id"]
                    ],
                }
            )
        relevant = subject["event_class"]
        opposite = "ETF_ACTION" if relevant == "BINARY_EVENT" else "BINARY_EVENT"
        coverage_ids = [coverage_observation["observation_id"]]
        subjects.append(
            {
                "coverage_attestations": [
                    {
                        "complete": False,
                        "conflicts": [],
                        "coverage": "UNKNOWN",
                        "event_class": relevant,
                        "source_observation_ids": coverage_ids,
                    },
                    {
                        "complete": True,
                        "conflicts": [],
                        "coverage": "NOT_APPLICABLE",
                        "event_class": opposite,
                        "source_observation_ids": coverage_ids,
                    },
                ],
                "event_class": relevant,
                "issuer_cik": subject["issuer_cik"],
                "records": records,
                "subject_kind": subject["subject_kind"],
                "symbol": symbol,
            }
        )
    return {
        "coverage_end": "2026-09-04",
        "coverage_start": "2026-08-24",
        "kind": "EVIDENCE_REVIEW_INPUT",
        "proposal_sha256": summary.proposal_sha256,
        "review_by": _utc_text(review_by),
        "reviewed_at": _utc_text(reviewed_at),
        "schema_version": 1,
        "subjects": subjects,
        "universe_sha256": summary.universe_sha256,
    }


def _write_review_input(path: Path, document: dict[str, object]) -> None:
    path.write_bytes(_canonical_bytes(document))
    path.chmod(0o600)


def _review_subject(
    document: dict[str, object],
    symbol: str,
) -> dict[str, object]:
    subjects = document["subjects"]
    assert isinstance(subjects, list)
    value = next(item for item in subjects if item["symbol"] == symbol)
    assert isinstance(value, dict)
    return value


def _active_tree_snapshot(project_root: Path) -> tuple[tuple[object, ...], ...]:
    root = project_root / "data/evidence"
    values: list[tuple[object, ...]] = []
    for path in (root, *sorted(root.rglob("*"))):
        details = path.lstat()
        relative = path.relative_to(root).as_posix() if path != root else "."
        payload = path.read_bytes() if stat.S_ISREG(details.st_mode) else b""
        link = os.readlink(path) if stat.S_ISLNK(details.st_mode) else ""
        values.append(
            (
                relative,
                details.st_mode,
                details.st_nlink,
                details.st_size,
                details.st_mtime_ns,
                hashlib.sha256(payload).hexdigest(),
                link,
            )
        )
    return tuple(values)


def _assert_no_floats(test: unittest.TestCase, value: object) -> None:
    test.assertNotIsInstance(value, float)
    if isinstance(value, dict):
        for child in value.values():
            _assert_no_floats(test, child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_floats(test, child)


class EvidenceReleaseWorkflowApiTests(unittest.TestCase):
    def test_task_four_candidate_api_is_public_and_exact(self) -> None:
        summary_type = workflow_module.EvidenceCandidateSummary
        inspect_candidate = workflow_module.inspect_evidence_candidate

        self.assertEqual(
            tuple(item.name for item in fields(summary_type)),
            (
                "status",
                "candidate_sha256",
                "proposal_sha256",
                "review_input_sha256",
                "release_sha256",
                "universe_sha256",
                "reviewed_at",
                "review_by",
                "symbols",
                "coverage",
                "reason_codes",
                "candidate_path",
            ),
        )
        signature = inspect.signature(inspect_candidate).parameters
        self.assertEqual(
            tuple(signature),
            (
                "project_root",
                "state_root",
                "proposal_sha256",
                "review_input_path",
                "as_of",
            ),
        )
        self.assertTrue(
            all(
                value.kind is inspect.Parameter.KEYWORD_ONLY
                for value in signature.values()
            )
        )

    def test_task_three_public_api_is_importable(self) -> None:
        self.assertTrue(issubclass(EvidenceWorkflowError, RuntimeError))
        self.assertTrue(callable(prepare_evidence_proposal))
        self.assertEqual(EvidenceProposalSummary.__name__, "EvidenceProposalSummary")

    def test_public_types_and_signature_are_exact_and_immutable(self) -> None:
        self.assertEqual(
            tuple(item.name for item in fields(EvidenceProposalSummary)),
            (
                "status",
                "proposal_sha256",
                "universe_sha256",
                "parent_release_sha256",
                "symbols",
                "reason_codes",
                "proposal_path",
            ),
        )
        signature = inspect.signature(prepare_evidence_proposal).parameters
        self.assertEqual(
            tuple(signature),
            (
                "project_root",
                "state_root",
                "as_of",
                "collect",
                "collect_sec",
            ),
        )
        self.assertTrue(
            all(
                value.kind is inspect.Parameter.KEYWORD_ONLY
                for value in signature.values()
            )
        )
        self.assertIsNone(signature["collect_sec"].default)
        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root)
            self.assertFalse(hasattr(summary, "__dict__"))
            with self.assertRaises(FrozenInstanceError):
                summary.status = "CHANGED"  # type: ignore[misc]


class EvidenceProposalDocumentTests(unittest.TestCase):
    def test_success_prepares_exact_canonical_unreviewed_proposal(self) -> None:
        fixture = FixtureCollectors()
        current_bytes = (PROJECT_ROOT / "data/evidence/current.json").read_bytes()
        universe = load_current_universe(PROJECT_ROOT, as_of=NOW.date())
        eligible = tuple(sorted(universe.eligible_records(), key=lambda item: item.symbol))

        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root, collectors=fixture)
            proposal_bytes = summary.proposal_path.read_bytes()
            proposal = _load_proposal(summary)

            self.assertEqual(summary.status, "PREPARED")
            self.assertEqual(summary.reason_codes, ())
            self.assertRegex(summary.proposal_sha256, r"[0-9a-f]{64}\Z")
            self.assertEqual(
                hashlib.sha256(proposal_bytes).hexdigest(),
                summary.proposal_sha256,
            )
            self.assertEqual(summary.proposal_path.name, "proposal.json")
            self.assertEqual(summary.proposal_path.parent.name, summary.proposal_sha256)
            self.assertEqual(
                summary.proposal_path.parent.parent,
                state_root / "evidence-proposals",
            )
            self.assertFalse((state_root / ".stock-monitor").exists())
            self.assertEqual(proposal_bytes, _canonical_bytes(proposal))
            self.assertEqual(set(proposal), _PROPOSAL_KEYS)
            self.assertEqual(proposal["schema_version"], 1)
            self.assertIs(type(proposal["schema_version"]), int)
            self.assertEqual(proposal["kind"], "UNREVIEWED_EVIDENCE_PROPOSAL")
            self.assertEqual(proposal["created_at"], _utc_text(NOW))
            self.assertEqual(
                proposal["parent_release_sha256"],
                hashlib.sha256(current_bytes).hexdigest(),
            )
            self.assertEqual(proposal["universe_sha256"], universe._release_pin)
            self.assertEqual(summary.universe_sha256, universe._release_pin)
            self.assertEqual(
                summary.parent_release_sha256,
                hashlib.sha256(current_bytes).hexdigest(),
            )
            self.assertEqual(summary.symbols, tuple(item.symbol for item in eligible))
            self.assertEqual(proposal["source_failures"], [])
            self.assertEqual(
                proposal["subjects"],
                [
                    {
                        "event_class": (
                            "BINARY_EVENT"
                            if item.product_type == "common_stock"
                            else "ETF_ACTION"
                        ),
                        "issuer_cik": item.issuer_cik,
                        "subject_kind": (
                            "STOCK"
                            if item.product_type == "common_stock"
                            else "ETF"
                        ),
                        "symbol": item.symbol,
                    }
                    for item in eligible
                ],
            )
            observations = proposal["observations"]
            self.assertIsInstance(observations, list)
            assert isinstance(observations, list)
            self.assertEqual(len(observations), len(EVIDENCE_AUTHORITIES))
            self.assertEqual(
                [(item["symbol"], item["url"]) for item in observations],
                [
                    (authority.symbol, authority.requested_url)
                    for authority in EVIDENCE_AUTHORITIES
                ],
            )
            for item, authority in zip(
                observations,
                EVIDENCE_AUTHORITIES,
                strict=True,
            ):
                self.assertIsInstance(item, dict)
                assert isinstance(item, dict)
                self.assertEqual(set(item), _OBSERVATION_KEYS)
                self.assertEqual(item["symbol"], authority.symbol)
                self.assertEqual(item["issuer_cik"], authority.issuer_cik)
                self.assertEqual(item["publisher"], authority.publisher)
                self.assertEqual(item["role"], authority.role)
                self.assertEqual(item["event_class"], authority.event_class)
                self.assertEqual(
                    item["artifact_path"],
                    f"artifacts/{item['content_sha256']}.json",
                )
                expected_type = (
                    "SEC_SUBMISSIONS"
                    if authority.role.startswith("SEC_SUBMISSIONS:")
                    else "OFFICIAL_REFERENCE"
                )
                self.assertEqual(item["source_type"], expected_type)
                self.assertIsNone(item["accession"])
            self.assertEqual(
                tuple(fixture.generic_calls),
                tuple(
                    item
                    for item in EVIDENCE_AUTHORITIES
                    if not item.role.startswith("SEC_SUBMISSIONS:")
                ),
            )
            self.assertEqual(
                fixture.sec_calls,
                ["0000320193", "0000002488", "0001045810"],
            )
            self.assertNotIn(b"CONFIRMED_CLEAR", proposal_bytes)
            self.assertNotIn(b"reviewed_at", proposal_bytes)
            self.assertNotIn(b"review_by", proposal_bytes)
            _assert_no_floats(self, proposal)

    def test_raw_artifacts_are_canonical_content_addressed_envelopes(self) -> None:
        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root, collectors=SharedBodyCollectors())
            proposal = _load_proposal(summary)
            observations = proposal["observations"]
            assert isinstance(observations, list)
            generic = [
                item
                for item in observations
                if item["source_type"] == "OFFICIAL_REFERENCE"
            ]
            generic_paths = {item["artifact_path"] for item in generic}
            self.assertEqual(len(generic_paths), 1)
            expected_paths = {item["artifact_path"] for item in observations}
            actual_paths = {
                path.relative_to(summary.proposal_path.parent).as_posix()
                for path in (summary.proposal_path.parent / "artifacts").iterdir()
            }
            self.assertEqual(actual_paths, expected_paths)
            for item in observations:
                artifact_path = summary.proposal_path.parent / item["artifact_path"]
                payload = artifact_path.read_bytes()
                document = json.loads(payload)
                self.assertEqual(payload, _canonical_bytes(document))
                self.assertEqual(
                    set(document),
                    {
                        "body",
                        "content_sha256",
                        "encoding",
                        "kind",
                        "schema_version",
                    },
                )
                body = base64.b64decode(document["body"], validate=True)
                self.assertEqual(document["schema_version"], 1)
                self.assertEqual(document["kind"], "RAW_SOURCE_ARTIFACT")
                self.assertEqual(document["encoding"], "base64")
                self.assertEqual(
                    hashlib.sha256(body).hexdigest(),
                    document["content_sha256"],
                )
                self.assertEqual(document["content_sha256"], item["content_sha256"])

    def test_reviewer_template_is_closed_unknown_scaffold_without_approval(self) -> None:
        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root)
            proposal = _load_proposal(summary)
            template_path = summary.proposal_path.parent / "review-template.json"
            template_bytes = template_path.read_bytes()
            template = _load_template(summary)

            self.assertEqual(template_bytes, _canonical_bytes(template))
            self.assertEqual(
                hashlib.sha256(template_bytes).hexdigest(),
                proposal["reviewer_template_sha256"],
            )
            self.assertEqual(
                set(template),
                {"kind", "schema_version", "subjects"},
            )
            self.assertEqual(template["schema_version"], 1)
            self.assertEqual(template["kind"], "EVIDENCE_REVIEW_INPUT_TEMPLATE")
            self.assertNotIn(b"proposal_sha256", template_bytes)
            for forbidden in (
                b"CONFIRMED_CLEAR",
                b"reviewed_at",
                b"review_by",
                b"reviewer",
                b"approval",
            ):
                self.assertNotIn(forbidden, template_bytes)

            subjects = template["subjects"]
            assert isinstance(subjects, list)
            self.assertEqual(
                [item["symbol"] for item in subjects],
                list(summary.symbols),
            )
            for subject in subjects:
                relevant = (
                    "BINARY_EVENT"
                    if subject["subject_kind"] == "STOCK"
                    else "ETF_ACTION"
                )
                opposite = (
                    "ETF_ACTION"
                    if relevant == "BINARY_EVENT"
                    else "BINARY_EVENT"
                )
                coverage = subject["coverage_attestations"]
                self.assertEqual(
                    [(item["event_class"], item["coverage"], item["complete"]) for item in coverage],
                    [
                        (relevant, "UNKNOWN", False),
                        (opposite, "NOT_APPLICABLE", True),
                    ],
                )
                self.assertEqual(subject["records"], [])

    def test_equivalent_inputs_are_deterministic_and_idempotent(self) -> None:
        eastern = timezone(-timedelta(hours=4))
        equivalent = NOW.astimezone(eastern)
        with _private_workspace() as (_, first_state), _private_workspace() as (
            _,
            second_state,
        ):
            first = _prepare(first_state)
            first_tree = {
                path.relative_to(first.proposal_path.parent).as_posix(): path.read_bytes()
                for path in sorted(first.proposal_path.parent.rglob("*"))
                if path.is_file()
            }
            repeated = _prepare(first_state)
            second = _prepare(second_state, as_of=equivalent)
            repeated_tree = {
                path.relative_to(repeated.proposal_path.parent).as_posix(): path.read_bytes()
                for path in sorted(repeated.proposal_path.parent.rglob("*"))
                if path.is_file()
            }
            second_tree = {
                path.relative_to(second.proposal_path.parent).as_posix(): path.read_bytes()
                for path in sorted(second.proposal_path.parent.rglob("*"))
                if path.is_file()
            }

            self.assertEqual(first.proposal_sha256, repeated.proposal_sha256)
            self.assertEqual(first.proposal_sha256, second.proposal_sha256)
            self.assertEqual(first_tree, repeated_tree)
            self.assertEqual(first_tree, second_tree)
            self.assertEqual(
                [path.name for path in (first_state / "evidence-proposals").iterdir()],
                [first.proposal_sha256],
            )

    def test_parent_is_exact_current_bytes_and_universe_is_verified_pin(self) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            distinct_parent = b'{"unreviewed-parent":"exact regular bytes"}\n'
            current = project_root / "data/evidence/current.json"
            current.write_bytes(distinct_parent)
            current.chmod(0o600)

            summary = _prepare(state_root, project_root=project_root)
            proposal = _load_proposal(summary)

            self.assertEqual(
                summary.parent_release_sha256,
                hashlib.sha256(distinct_parent).hexdigest(),
            )
            self.assertEqual(
                proposal["parent_release_sha256"],
                hashlib.sha256(distinct_parent).hexdigest(),
            )
            self.assertEqual(
                summary.universe_sha256,
                universe_module.CURRENT_UNIVERSE_SHA256,
            )
            self.assertEqual(
                proposal["universe_sha256"],
                universe_module.CURRENT_UNIVERSE_SHA256,
            )


class EvidenceCandidateDocumentTests(unittest.TestCase):
    def test_canonical_review_compiles_loadable_immutable_candidate(self) -> None:
        inspected_at = NOW + timedelta(minutes=10)
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            active_before = _active_tree_snapshot(PROJECT_ROOT)

            summary = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=inspected_at,
            )

            self.assertEqual(summary.status, "AWAITING_DIGEST_APPROVAL")
            self.assertEqual(summary.proposal_sha256, proposal.proposal_sha256)
            self.assertEqual(summary.universe_sha256, proposal.universe_sha256)
            self.assertEqual(summary.reviewed_at, NOW + timedelta(minutes=5))
            self.assertEqual(summary.review_by, NOW + timedelta(hours=23))
            self.assertEqual(summary.symbols, proposal.symbols)
            self.assertEqual(summary.coverage, tuple(sorted(summary.coverage)))
            self.assertEqual(
                len(summary.coverage),
                2 * len(summary.symbols),
            )
            self.assertTrue(
                all(
                    state in {"UNKNOWN", "NOT_APPLICABLE"}
                    for _, _, state in summary.coverage
                )
            )
            self.assertEqual(
                summary.reason_codes,
                ("RELEVANT_COVERAGE_UNKNOWN",),
            )
            self.assertEqual(_active_tree_snapshot(PROJECT_ROOT), active_before)
            self.assertEqual(
                summary.candidate_path,
                state_root
                / "evidence-candidates"
                / summary.candidate_sha256
                / "candidate.json",
            )

            candidate_bytes = summary.candidate_path.read_bytes()
            candidate = json.loads(candidate_bytes)
            self.assertEqual(candidate_bytes, _canonical_bytes(candidate))
            self.assertEqual(
                hashlib.sha256(candidate_bytes).hexdigest(),
                summary.candidate_sha256,
            )
            self.assertEqual(
                set(candidate),
                {
                    "inventory",
                    "kind",
                    "parent_release_sha256",
                    "proposal_sha256",
                    "release_sha256",
                    "review_input_sha256",
                    "schema_version",
                    "universe_sha256",
                },
            )
            self.assertEqual(candidate["schema_version"], 1)
            self.assertEqual(candidate["kind"], "EVIDENCE_RELEASE_CANDIDATE")
            self.assertEqual(
                candidate["review_input_sha256"],
                hashlib.sha256(review_path.read_bytes()).hexdigest(),
            )
            release_root = summary.candidate_path.parent / "release"
            inventory = candidate["inventory"]
            self.assertEqual(
                inventory,
                sorted(inventory, key=lambda item: item["path"]),
            )
            actual_files = {
                path.relative_to(release_root).as_posix(): path.read_bytes()
                for path in sorted(release_root.rglob("*"))
                if path.is_file()
            }
            self.assertEqual(
                {item["path"] for item in inventory},
                set(actual_files),
            )
            for item in inventory:
                self.assertEqual(
                    item["sha256"],
                    hashlib.sha256(actual_files[item["path"]]).hexdigest(),
                )
            self.assertEqual(
                summary.release_sha256,
                hashlib.sha256(actual_files["current.json"]).hexdigest(),
            )
            for path in (summary.candidate_path.parent, *summary.candidate_path.parent.rglob("*")):
                details = path.lstat()
                if stat.S_ISDIR(details.st_mode):
                    self.assertEqual(stat.S_IMODE(details.st_mode), 0o700)
                else:
                    self.assertTrue(stat.S_ISREG(details.st_mode))
                    self.assertEqual(stat.S_IMODE(details.st_mode), 0o600)
                    self.assertEqual(details.st_nlink, 1)

            universe = load_current_universe(PROJECT_ROOT, as_of=inspected_at.date())
            release = evidence_module.load_evidence_release(
                release_root / "current.json",
                expected_sha256=summary.release_sha256,
                as_of=inspected_at,
                universe=universe,
            )
            nvda = release.by_symbol["NVDA"]
            decision = evidence_module.classify_evidence(
                nvda.records,
                evidence_module.DateRange(
                    datetime(2026, 8, 25).date(),
                    datetime(2026, 8, 27).date(),
                ),
                symbol="NVDA",
                issuer_cik="0001045810",
                source_bindings=nvda.source_bindings,
                as_of=inspected_at,
                subject_kind="STOCK",
                coverage_attestations=nvda.coverage_attestations,
                reviewed_bundle=nvda,
            )
            self.assertEqual(decision.block_reason, "BINARY_EVENT_DURING_HOLD")

    def test_adverse_only_record_may_be_undated_but_remains_closed_taxonomy(self) -> None:
        inspected_at = NOW + timedelta(minutes=10)
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review = _review_document(proposal)
            subjects = review["subjects"]
            assert isinstance(subjects, list)
            nvda = next(item for item in subjects if item["symbol"] == "NVDA")
            record = nvda["records"][0]
            record["event_type"] = None
            record["adverse_tags"] = ["lowered guidance"]
            record["event_date"] = None
            record["event_kind"] = None
            review_path = root / "review-input.json"
            _write_review_input(review_path, review)

            summary = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=inspected_at,
            )

            universe = load_current_universe(PROJECT_ROOT, as_of=inspected_at.date())
            release = evidence_module.load_evidence_release(
                summary.candidate_path.parent / "release/current.json",
                expected_sha256=summary.release_sha256,
                as_of=inspected_at,
                universe=universe,
            )
            compiled = release.by_symbol["NVDA"].records[0]
            self.assertIsNone(compiled.event_type)
            self.assertEqual(compiled.adverse_tags, ("lowered guidance",))
            self.assertIsNone(compiled.event_date)
            self.assertIsNone(compiled.event_kind)

    def test_unused_timestamp_unavailable_sec_observations_remain_compilable(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(
                state_root,
                collectors=TimestampUnavailableSecCollectors(),
            )
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))

            summary = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=NOW + timedelta(minutes=10),
            )

            self.assertEqual(summary.status, "AWAITING_DIGEST_APPROVAL")

    def test_existing_candidate_root_swap_is_detected_by_descriptor_identity(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            summary = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=NOW + timedelta(minutes=10),
            )
            real_stat = workflow_module.os.stat
            candidate_stats = 0

            def swapped_stat(path, *args, **kwargs):
                nonlocal candidate_stats
                details = real_stat(path, *args, **kwargs)
                if (
                    path == summary.candidate_sha256
                    and kwargs.get("dir_fd") is not None
                    and kwargs.get("follow_symlinks") is False
                ):
                    candidate_stats += 1
                    if candidate_stats == 2:
                        fields = list(details)
                        fields[1] += 1
                        return os.stat_result(fields)
                return details

            with mock.patch.object(workflow_module.os, "stat", swapped_stat):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            self.assertEqual(candidate_stats, 2)

    def test_valid_private_review_file_under_writable_ancestor_is_accepted(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            writable = root / "writable-ancestor"
            writable.mkdir(mode=0o700)
            writable.chmod(0o777)
            review_path = writable / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))

            summary = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=NOW + timedelta(minutes=10),
            )

            self.assertEqual(summary.status, "AWAITING_DIGEST_APPROVAL")

    def test_candidate_is_deterministic_idempotent_and_summary_is_safe(self) -> None:
        with _private_workspace() as (first_root, first_state), _private_workspace() as (
            second_root,
            second_state,
        ):
            first_proposal = _prepare(first_state)
            second_proposal = _prepare(second_state)
            first_review = first_root / "review-input.json"
            second_review = second_root / "review-input.json"
            _write_review_input(first_review, _review_document(first_proposal))
            _write_review_input(second_review, _review_document(second_proposal))

            first = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=first_state,
                proposal_sha256=first_proposal.proposal_sha256,
                review_input_path=first_review,
                as_of=NOW + timedelta(minutes=10),
            )
            repeated = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=first_state,
                proposal_sha256=first_proposal.proposal_sha256,
                review_input_path=first_review,
                as_of=NOW + timedelta(minutes=10),
            )
            second = workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=second_state,
                proposal_sha256=second_proposal.proposal_sha256,
                review_input_path=second_review,
                as_of=NOW + timedelta(minutes=10),
            )

            self.assertEqual(first.candidate_sha256, repeated.candidate_sha256)
            self.assertEqual(first.candidate_sha256, second.candidate_sha256)
            self.assertEqual(first.release_sha256, second.release_sha256)
            first_tree = {
                path.relative_to(first.candidate_path.parent).as_posix(): path.read_bytes()
                for path in sorted(first.candidate_path.parent.rglob("*"))
                if path.is_file()
            }
            second_tree = {
                path.relative_to(second.candidate_path.parent).as_posix(): path.read_bytes()
                for path in sorted(second.candidate_path.parent.rglob("*"))
                if path.is_file()
            }
            self.assertEqual(first_tree, second_tree)
            self.assertEqual(
                [path.name for path in (first_state / "evidence-candidates").iterdir()],
                [first.candidate_sha256],
            )
            self.assertFalse(hasattr(first, "__dict__"))
            with self.assertRaises(FrozenInstanceError):
                first.status = "CHANGED"  # type: ignore[misc]
            safe_repr = repr(first)
            for forbidden in (
                "NVIDIA financial results",
                "https://",
                "reviewer",
                "conflict",
                "RAW_SOURCE_ARTIFACT",
            ):
                self.assertNotIn(forbidden, safe_repr)


class EvidenceCandidateRejectionTests(unittest.TestCase):
    def test_unhashable_taxonomy_and_proposal_url_raise_workflow_error(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review = _review_document(proposal)
            _review_subject(review, "AAPL")["coverage_attestations"][0][
                "event_class"
            ] = ["BINARY_EVENT"]
            review_path = root / "review-input.json"
            _write_review_input(review_path, review)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review = _review_document(proposal)
            proposal_document = _load_proposal(proposal)
            proposal_document["observations"][0]["url"] = {
                "forged": "authority"
            }
            forged_payload = _canonical_bytes(proposal_document)
            forged_sha256 = hashlib.sha256(forged_payload).hexdigest()
            proposal.proposal_path.write_bytes(forged_payload)
            forged_root = proposal.proposal_path.parent.parent / forged_sha256
            proposal.proposal_path.parent.rename(forged_root)
            review["proposal_sha256"] = forged_sha256
            review_path = root / "review-input.json"
            _write_review_input(review_path, review)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=forged_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

    def test_duplicate_keys_extra_fields_and_authority_overrides_are_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            active_before = _active_tree_snapshot(PROJECT_ROOT)
            review_path = root / "review-input.json"

            canonical = _canonical_bytes(_review_document(proposal))
            duplicate = canonical.replace(
                b'{"coverage_end":',
                b'{"coverage_end":"2026-09-04","coverage_end":',
                1,
            )
            review_path.write_bytes(duplicate)
            review_path.chmod(0o600)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )
            review_path.write_bytes(canonical + b" ")
            review_path.chmod(0o600)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

            mutations: list[tuple[str, dict[str, object]]] = []
            extra_top = _review_document(proposal)
            extra_top["reviewer"] = "operator"
            mutations.append(("reviewer identity", extra_top))
            for field, value in (
                ("url", "https://example.invalid/override"),
                ("content_hash", "0" * 64),
                ("source_role", "ISSUER_IR:OVERRIDE"),
                ("healthy", False),
            ):
                override = _review_document(proposal)
                nvda = _review_subject(override, "NVDA")
                records = nvda["records"]
                assert isinstance(records, list)
                records[0][field] = value
                mutations.append((field, override))
            for name, document in mutations:
                with self.subTest(name=name):
                    _write_review_input(review_path, document)
                    with self.assertRaises(EvidenceWorkflowError):
                        workflow_module.inspect_evidence_candidate(
                            project_root=PROJECT_ROOT,
                            state_root=state_root,
                            proposal_sha256=proposal.proposal_sha256,
                            review_input_path=review_path,
                            as_of=NOW + timedelta(minutes=10),
                        )
                    self.assertEqual(
                        _active_tree_snapshot(PROJECT_ROOT),
                        active_before,
                    )

    def test_digest_subject_and_observation_mismatches_are_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            proposal_document = _load_proposal(proposal)
            observations = proposal_document["observations"]
            assert isinstance(observations, list)
            aapl_id = next(
                item["observation_id"]
                for item in observations
                if item["symbol"] == "AAPL"
                and item["source_type"] == "OFFICIAL_REFERENCE"
            )
            review_path = root / "review-input.json"
            active_before = _active_tree_snapshot(PROJECT_ROOT)
            mutations: list[tuple[str, dict[str, object]]] = []

            wrong_proposal = _review_document(proposal)
            wrong_proposal["proposal_sha256"] = "0" * 64
            mutations.append(("proposal digest", wrong_proposal))
            wrong_universe = _review_document(proposal)
            wrong_universe["universe_sha256"] = "0" * 64
            mutations.append(("universe digest", wrong_universe))
            wrong_symbol = _review_document(proposal)
            _review_subject(wrong_symbol, "AAPL")["symbol"] = "MSFT"
            mutations.append(("symbol", wrong_symbol))
            wrong_cik = _review_document(proposal)
            _review_subject(wrong_cik, "AAPL")["issuer_cik"] = "0000000001"
            mutations.append(("CIK", wrong_cik))
            unknown = _review_document(proposal)
            unknown_record = _review_subject(unknown, "NVDA")["records"][0]
            unknown_record["source_observation_ids"] = ["unknown-observation"]
            mutations.append(("unknown observation", unknown))
            foreign = _review_document(proposal)
            foreign_record = _review_subject(foreign, "NVDA")["records"][0]
            foreign_record["source_observation_ids"] = [aapl_id]
            mutations.append(("foreign observation", foreign))

            for name, document in mutations:
                with self.subTest(name=name):
                    _write_review_input(review_path, document)
                    with self.assertRaises(EvidenceWorkflowError):
                        workflow_module.inspect_evidence_candidate(
                            project_root=PROJECT_ROOT,
                            state_root=state_root,
                            proposal_sha256=proposal.proposal_sha256,
                            review_input_path=review_path,
                            as_of=NOW + timedelta(minutes=10),
                        )
                    self.assertEqual(
                        _active_tree_snapshot(PROJECT_ROOT),
                        active_before,
                    )

    def test_future_stale_expired_and_overlong_review_windows_are_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            active_before = _active_tree_snapshot(PROJECT_ROOT)
            mutations: list[tuple[str, dict[str, object]]] = []

            predates_proposal = _review_document(proposal)
            predates_proposal["reviewed_at"] = _utc_text(NOW - timedelta(seconds=1))
            mutations.append(("predates proposal", predates_proposal))
            future = _review_document(proposal)
            future["reviewed_at"] = _utc_text(NOW + timedelta(minutes=11))
            mutations.append(("future review", future))
            expired = _review_document(proposal)
            expired["review_by"] = _utc_text(NOW + timedelta(minutes=10))
            mutations.append(("expired review", expired))
            overlong = _review_document(proposal)
            overlong["review_by"] = _utc_text(NOW + timedelta(hours=25, minutes=5))
            mutations.append(("overlong review", overlong))
            stale_source = _review_document(proposal)
            stale_source["review_by"] = _utc_text(
                NOW + timedelta(hours=24, microseconds=1)
            )
            mutations.append(("stale source", stale_source))
            uncovered = _review_document(proposal)
            uncovered["coverage_start"] = "2026-08-25"
            mutations.append(("uncovered as_of", uncovered))
            noncanonical_time = _review_document(proposal)
            noncanonical_time["reviewed_at"] = "2026-08-24T14:05:00+00:00"
            mutations.append(("noncanonical UTC", noncanonical_time))

            for name, document in mutations:
                with self.subTest(name=name):
                    _write_review_input(review_path, document)
                    with self.assertRaises(EvidenceWorkflowError):
                        workflow_module.inspect_evidence_candidate(
                            project_root=PROJECT_ROOT,
                            state_root=state_root,
                            proposal_sha256=proposal.proposal_sha256,
                            review_input_path=review_path,
                            as_of=NOW + timedelta(minutes=10),
                        )
                    self.assertEqual(
                        _active_tree_snapshot(PROJECT_ROOT),
                        active_before,
                    )

    def test_clear_bypasses_unsafe_taxonomy_and_missing_coverage_are_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            active_before = _active_tree_snapshot(PROJECT_ROOT)
            mutations: list[tuple[str, dict[str, object]]] = []

            clear = _review_document(proposal)
            clear_coverage = _review_subject(clear, "AAPL")[
                "coverage_attestations"
            ][0]
            clear_coverage["coverage"] = "CONFIRMED_CLEAR"
            clear_coverage["complete"] = True
            mutations.append(("confirmed clear", clear))
            relevant_na = _review_document(proposal)
            relevant_na_coverage = _review_subject(relevant_na, "AAPL")[
                "coverage_attestations"
            ][0]
            relevant_na_coverage["coverage"] = "NOT_APPLICABLE"
            relevant_na_coverage["complete"] = True
            mutations.append(("relevant not applicable", relevant_na))
            overlap = _review_document(proposal)
            _review_subject(overlap, "AAPL")["coverage_attestations"][0][
                "coverage"
            ] = "OVERLAP"
            mutations.append(("reviewer overlap", overlap))
            conflict = _review_document(proposal)
            _review_subject(conflict, "AAPL")["coverage_attestations"][0][
                "conflicts"
            ] = ["reviewer conflict"]
            mutations.append(("reviewer conflict", conflict))
            conflict_state = _review_document(proposal)
            _review_subject(conflict_state, "AAPL")["coverage_attestations"][0][
                "coverage"
            ] = "CONFLICT"
            mutations.append(("reviewer conflict state", conflict_state))
            unsupported_class = _review_document(proposal)
            _review_subject(unsupported_class, "AAPL")["coverage_attestations"][0][
                "event_class"
            ] = "OTHER"
            mutations.append(("unsupported event class", unsupported_class))
            missing = _review_document(proposal)
            _review_subject(missing, "AAPL")["coverage_attestations"].pop()
            mutations.append(("missing coverage", missing))
            duplicate = _review_document(proposal)
            duplicate_coverage = _review_subject(duplicate, "AAPL")[
                "coverage_attestations"
            ]
            duplicate_coverage[1] = json.loads(
                _canonical_bytes(duplicate_coverage[0])
            )
            mutations.append(("duplicate coverage", duplicate))
            empty_sources = _review_document(proposal)
            _review_subject(empty_sources, "AAPL")["coverage_attestations"][0][
                "source_observation_ids"
            ] = []
            mutations.append(("empty coverage sources", empty_sources))
            duplicate_sources = _review_document(proposal)
            duplicate_source_values = _review_subject(
                duplicate_sources,
                "AAPL",
            )["coverage_attestations"][0]["source_observation_ids"]
            duplicate_source_values.append(duplicate_source_values[0])
            mutations.append(("duplicate coverage sources", duplicate_sources))
            sec_coverage = _review_document(proposal)
            proposal_observations = _load_proposal(proposal)["observations"]
            sec_id = next(
                item["observation_id"]
                for item in proposal_observations
                if item["symbol"] == "AAPL"
                and item["source_type"] == "SEC_SUBMISSIONS"
            )
            for value in _review_subject(sec_coverage, "AAPL")[
                "coverage_attestations"
            ]:
                value["source_observation_ids"] = [sec_id]
            mutations.append(("SEC coverage source", sec_coverage))
            unsupported_type = _review_document(proposal)
            _review_subject(unsupported_type, "NVDA")["records"][0][
                "event_type"
            ] = "rumor"
            mutations.append(("unsupported record type", unsupported_type))
            unsupported_tag = _review_document(proposal)
            _review_subject(unsupported_tag, "NVDA")["records"][0][
                "adverse_tags"
            ] = ["negative sentiment"]
            mutations.append(("unsupported adverse tag", unsupported_tag))
            wrong_kind = _review_document(proposal)
            _review_subject(wrong_kind, "NVDA")["records"][0][
                "event_kind"
            ] = "ETF_ACTION"
            mutations.append(("wrong event kind", wrong_kind))
            multiple_record_sources = _review_document(proposal)
            multiple_nvda = _review_subject(multiple_record_sources, "NVDA")
            multiple_nvda["records"][0]["source_observation_ids"].append(
                multiple_nvda["coverage_attestations"][0][
                    "source_observation_ids"
                ][0]
            )
            mutations.append(("multiple record sources", multiple_record_sources))
            future_publication = _review_document(proposal)
            _review_subject(future_publication, "NVDA")["records"][0][
                "published_at"
            ] = _utc_text(NOW + timedelta(microseconds=1))
            mutations.append(("future publication", future_publication))

            for name, document in mutations:
                with self.subTest(name=name):
                    _write_review_input(review_path, document)
                    with self.assertRaises(EvidenceWorkflowError):
                        workflow_module.inspect_evidence_candidate(
                            project_root=PROJECT_ROOT,
                            state_root=state_root,
                            proposal_sha256=proposal.proposal_sha256,
                            review_input_path=review_path,
                            as_of=NOW + timedelta(minutes=10),
                        )
                    self.assertEqual(
                        _active_tree_snapshot(PROJECT_ROOT),
                        active_before,
                    )

    def test_fact_and_coverage_sources_must_be_distinct_by_id_and_body(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            reused = _review_document(proposal)
            nvda = _review_subject(reused, "NVDA")
            fact_id = nvda["records"][0]["source_observation_ids"][0]
            for coverage in nvda["coverage_attestations"]:
                coverage["source_observation_ids"] = [fact_id]
            _write_review_input(review_path, reused)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root, collectors=SharedBodyCollectors())
            proposal_document = _load_proposal(proposal)
            observations = proposal_document["observations"]
            assert isinstance(observations, list)
            shared_url = (
                "https://www.ssga.com/us/en/intermediary/resources/"
                "authorized-participants"
            )
            spy_id = next(
                item["observation_id"]
                for item in observations
                if item["symbol"] == "SPY" and item["url"] == shared_url
            )
            xlk_id = next(
                item["observation_id"]
                for item in observations
                if item["symbol"] == "XLK" and item["url"] == shared_url
            )
            review = _review_document(proposal)
            spy = _review_subject(review, "SPY")
            spy["records"] = [
                {
                    "adverse_tags": [],
                    "classification_ambiguous": False,
                    "conflicts": [],
                    "event_date": "2026-08-26",
                    "event_kind": "ETF_ACTION",
                    "event_type": "fund sponsor notice",
                    "fact": "The fund sponsor published a dated notice.",
                    "published_at": _utc_text(NOW - timedelta(minutes=15)),
                    "source_observation_ids": [spy_id],
                }
            ]
            for coverage in _review_subject(review, "XLK")[
                "coverage_attestations"
            ]:
                coverage["source_observation_ids"] = [xlk_id]
            review_path = root / "review-input.json"
            _write_review_input(review_path, review)
            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

    def test_partial_proposal_is_not_candidate_input(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root, collect_sec=None)
            self.assertEqual(proposal.status, "PREPARED_BLOCKED")
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            active_before = _active_tree_snapshot(PROJECT_ROOT)

            with self.assertRaises(EvidenceWorkflowError):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )

            self.assertEqual(_active_tree_snapshot(PROJECT_ROOT), active_before)


class EvidenceCandidateFilesystemTests(unittest.TestCase):
    def test_invalid_proposal_observation_count_is_rejected_before_artifact_reads(
        self,
    ) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review = _review_document(proposal)
            proposal_document = _load_proposal(proposal)
            observations = proposal_document["observations"]
            assert isinstance(observations, list)
            observations.append(json.loads(_canonical_bytes(observations[0])))
            proposal_payload = _canonical_bytes(proposal_document)
            forged_sha256 = hashlib.sha256(proposal_payload).hexdigest()
            proposal.proposal_path.write_bytes(proposal_payload)
            forged_root = proposal.proposal_path.parent.parent / forged_sha256
            proposal.proposal_path.parent.rename(forged_root)
            review["proposal_sha256"] = forged_sha256
            review_path = root / "review-input.json"
            _write_review_input(review_path, review)

            real_read = workflow_module._read_private_bytes_at
            reads = 0

            def reject_artifact_read(*args, **kwargs):
                nonlocal reads
                reads += 1
                if reads > 2:
                    raise AssertionError("artifact read preceded count rejection")
                return real_read(*args, **kwargs)

            with mock.patch.object(
                workflow_module,
                "_read_private_bytes_at",
                reject_artifact_read,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=forged_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            self.assertEqual(reads, 2)

    def test_review_input_rejects_relative_symlink_hardlink_fifo_device_mode_and_size(
        self,
    ) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            payload = _canonical_bytes(_review_document(proposal))
            active_before = _active_tree_snapshot(PROJECT_ROOT)
            paths: list[tuple[str, Path]] = [("relative", Path("review-input.json"))]

            symlink_target = root / "symlink-target.json"
            symlink_target.write_bytes(payload)
            symlink_target.chmod(0o600)
            symlink_path = root / "symlink-review.json"
            symlink_path.symlink_to(symlink_target)
            paths.append(("symlink", symlink_path))

            hardlink_target = root / "hardlink-target.json"
            hardlink_target.write_bytes(payload)
            hardlink_target.chmod(0o600)
            hardlink_path = root / "hardlink-review.json"
            os.link(hardlink_target, hardlink_path)
            paths.append(("hardlink", hardlink_path))

            public_path = root / "public-review.json"
            public_path.write_bytes(payload)
            public_path.chmod(0o644)
            paths.append(("mode", public_path))

            fifo_path = root / "review.fifo"
            os.mkfifo(fifo_path, 0o600)
            fifo_path.chmod(0o600)
            paths.append(("fifo", fifo_path))

            oversized_path = root / "oversized-review.json"
            oversized_path.write_bytes(b"x" * (1_048_576 + 1))
            oversized_path.chmod(0o600)
            paths.append(("oversized", oversized_path))
            paths.append(("device", Path("/dev/null")))

            for name, review_path in paths:
                with self.subTest(name=name):
                    with self.assertRaises(EvidenceWorkflowError):
                        workflow_module.inspect_evidence_candidate(
                            project_root=PROJECT_ROOT,
                            state_root=state_root,
                            proposal_sha256=proposal.proposal_sha256,
                            review_input_path=review_path,
                            as_of=NOW + timedelta(minutes=10),
                        )
                    self.assertEqual(
                        _active_tree_snapshot(PROJECT_ROOT),
                        active_before,
                    )

    def test_proposal_and_artifact_tamper_or_unsafe_links_are_rejected(self) -> None:
        cases = ("proposal bytes", "artifact bytes", "artifact mode", "artifact link")
        for case in cases:
            with self.subTest(case=case), _private_workspace() as (root, state_root):
                proposal = _prepare(state_root)
                review_path = root / "review-input.json"
                _write_review_input(review_path, _review_document(proposal))
                proposal_document = _load_proposal(proposal)
                observations = proposal_document["observations"]
                assert isinstance(observations, list)
                artifact = proposal.proposal_path.parent / observations[0][
                    "artifact_path"
                ]
                if case == "proposal bytes":
                    proposal.proposal_path.write_bytes(
                        proposal.proposal_path.read_bytes() + b" "
                    )
                elif case == "artifact bytes":
                    artifact.write_bytes(artifact.read_bytes() + b" ")
                elif case == "artifact mode":
                    artifact.chmod(0o644)
                else:
                    extra = root / "artifact-hardlink.json"
                    os.link(artifact, extra)
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )

    def test_existing_candidate_rejects_content_mode_link_and_extra_collisions(
        self,
    ) -> None:
        cases = ("content", "mode", "hardlink", "extra")
        for case in cases:
            with self.subTest(case=case), _private_workspace() as (root, state_root):
                proposal = _prepare(state_root)
                review_path = root / "review-input.json"
                _write_review_input(review_path, _review_document(proposal))
                summary = workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )
                candidate_root = summary.candidate_path.parent
                current = candidate_root / "release/current.json"
                if case == "content":
                    current.write_bytes(current.read_bytes() + b" ")
                elif case == "mode":
                    current.chmod(0o644)
                elif case == "hardlink":
                    os.link(current, root / "candidate-hardlink.json")
                else:
                    extra = candidate_root / "release/extra.json"
                    extra.write_bytes(b"{}\n")
                    extra.chmod(0o600)
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )

    def test_candidate_publication_failure_cleans_temporary_and_addressed_trees(
        self,
    ) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            real_write = workflow_module._atomic_write_at
            candidate_writes = 0

            def fail_candidate_write(directory_descriptor, name, payload):
                nonlocal candidate_writes
                candidate_writes += 1
                if candidate_writes == 3:
                    raise OSError("injected candidate write failure")
                return real_write(directory_descriptor, name, payload)

            with mock.patch.object(
                workflow_module,
                "_atomic_write_at",
                fail_candidate_write,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            candidate_parent = state_root / "evidence-candidates"
            self.assertTrue(candidate_parent.is_dir())
            self.assertEqual(list(candidate_parent.iterdir()), [])

    def test_review_input_change_during_descriptor_read_is_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            target_identity = review_path.stat().st_ino
            real_read = workflow_module.os.read
            changed = False

            def changing_read(descriptor, count):
                nonlocal changed
                payload = real_read(descriptor, count)
                if not changed and os.fstat(descriptor).st_ino == target_identity:
                    changed = True
                    review_path.write_bytes(review_path.read_bytes() + b" ")
                    review_path.chmod(0o600)
                return payload

            with mock.patch.object(workflow_module.os, "read", changing_read):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            self.assertTrue(changed)

    def test_unrelated_sibling_creation_during_review_read_is_accepted(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            target_identity = review_path.stat().st_ino
            unrelated_path = root / "unrelated-directory"
            real_read = workflow_module.os.read
            changed = False

            def changing_read(descriptor, count):
                nonlocal changed
                payload = real_read(descriptor, count)
                if not changed and os.fstat(descriptor).st_ino == target_identity:
                    changed = True
                    unrelated_path.mkdir(mode=0o700)
                return payload

            with mock.patch.object(workflow_module.os, "read", changing_read):
                summary = workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )
            self.assertTrue(changed)
            self.assertEqual(summary.status, "AWAITING_DIGEST_APPROVAL")

    def test_review_input_ancestor_swap_during_read_is_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_directory = root / "reviews"
            review_directory.mkdir(mode=0o700)
            review_path = review_directory / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            detached_directory = root / "detached-reviews"
            replacement_payload = b'{"replacement":true}\n'
            target_identity = review_path.stat().st_ino
            real_read = workflow_module.os.read
            changed = False

            def changing_read(descriptor, count):
                nonlocal changed
                payload = real_read(descriptor, count)
                if not changed and os.fstat(descriptor).st_ino == target_identity:
                    changed = True
                    review_directory.rename(detached_directory)
                    review_directory.mkdir(mode=0o700)
                    replacement = review_directory / "review-input.json"
                    replacement.write_bytes(replacement_payload)
                    replacement.chmod(0o600)
                return payload

            with mock.patch.object(workflow_module.os, "read", changing_read):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            self.assertTrue(changed)
            self.assertEqual(review_path.read_bytes(), replacement_payload)

    def test_existing_candidate_collision_is_verified_only_while_locked(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            workflow_module.inspect_evidence_candidate(
                project_root=PROJECT_ROOT,
                state_root=state_root,
                proposal_sha256=proposal.proposal_sha256,
                review_input_path=review_path,
                as_of=NOW + timedelta(minutes=10),
            )
            real_flock = workflow_module.fcntl.flock
            real_verify = workflow_module._verify_candidate_tree
            exclusive_locked = False
            verified = False

            def tracked_flock(descriptor, operation):
                nonlocal exclusive_locked
                result = real_flock(descriptor, operation)
                if operation == workflow_module.fcntl.LOCK_EX:
                    exclusive_locked = True
                elif operation == workflow_module.fcntl.LOCK_UN and exclusive_locked:
                    exclusive_locked = False
                return result

            def locked_verify(*args, **kwargs):
                nonlocal verified
                self.assertTrue(exclusive_locked)
                verified = True
                return real_verify(*args, **kwargs)

            with mock.patch.object(workflow_module.fcntl, "flock", tracked_flock), mock.patch.object(
                workflow_module,
                "_verify_candidate_tree",
                locked_verify,
            ):
                workflow_module.inspect_evidence_candidate(
                    project_root=PROJECT_ROOT,
                    state_root=state_root,
                    proposal_sha256=proposal.proposal_sha256,
                    review_input_path=review_path,
                    as_of=NOW + timedelta(minutes=10),
                )
            self.assertTrue(verified)
            self.assertFalse(exclusive_locked)

    def test_candidate_nested_directory_open_failure_closes_descriptors_and_cleans(
        self,
    ) -> None:
        with _private_workspace() as (root, state_root):
            proposal = _prepare(state_root)
            review_path = root / "review-input.json"
            _write_review_input(review_path, _review_document(proposal))
            real_open = workflow_module.os.open
            subjects_descriptor: int | None = None

            def fail_sources_open(path, *args, **kwargs):
                nonlocal subjects_descriptor
                if path == "sources":
                    raise OSError("injected sources directory open failure")
                descriptor = real_open(path, *args, **kwargs)
                if path == "subjects":
                    subjects_descriptor = descriptor
                return descriptor

            with mock.patch.object(
                workflow_module,
                "_validate_compiled_release",
            ), mock.patch.object(workflow_module.os, "open", fail_sources_open):
                with self.assertRaises(EvidenceWorkflowError):
                    workflow_module.inspect_evidence_candidate(
                        project_root=PROJECT_ROOT,
                        state_root=state_root,
                        proposal_sha256=proposal.proposal_sha256,
                        review_input_path=review_path,
                        as_of=NOW + timedelta(minutes=10),
                    )
            self.assertIsNotNone(subjects_descriptor)
            assert subjects_descriptor is not None
            with self.assertRaises(OSError):
                os.fstat(subjects_descriptor)
            self.assertEqual(
                list((state_root / "evidence-candidates").iterdir()),
                [],
            )


class EvidenceProposalFailureTests(unittest.TestCase):
    def test_missing_sec_collector_persists_safe_partial_proposal(self) -> None:
        fixture = FixtureCollectors()
        with _private_workspace() as (_, state_root):
            summary = _prepare(
                state_root,
                collectors=fixture,
                collect_sec=None,
            )
            proposal_bytes = summary.proposal_path.read_bytes()
            proposal = _load_proposal(summary)

            self.assertEqual(summary.status, "PREPARED_BLOCKED")
            self.assertEqual(summary.reason_codes, ("SEC_COLLECTOR_UNAVAILABLE",))
            self.assertEqual(len(proposal["source_failures"]), 3)
            self.assertEqual(
                {item["reason_code"] for item in proposal["source_failures"]},
                {"SEC_COLLECTOR_UNAVAILABLE"},
            )
            self.assertEqual(fixture.sec_calls, [])
            self.assertEqual(len(fixture.generic_calls), 14)
            self.assertNotIn(b"data.sec.gov", proposal_bytes)
            self.assertTrue(set(summary.reason_codes) <= _SAFE_REASON_CODES)

    def test_collection_exceptions_and_invalid_results_are_safely_redacted(self) -> None:
        secret_exception = "secret-body https://bad.example/?token=secret-token"

        class FailingCollectors(FixtureCollectors):
            def collect(
                self,
                authority: EvidenceAuthority,
            ) -> ProposalSourceObservation:
                if authority.requested_url.endswith("default.aspx"):
                    raise RuntimeError(secret_exception)
                observation = super().collect(authority)
                if authority.requested_url.endswith("rss-feed.rss"):
                    return replace(observation, publisher="invalid-secret-publisher")
                return observation

            def collect_sec(self, issuer_cik: str) -> SourceDocument:
                if issuer_cik == "0000002488":
                    raise RuntimeError(secret_exception)
                document = super().collect_sec(issuer_cik)
                if issuer_cik == "0001045810":
                    return replace(document, source_type="OFFICIAL_REFERENCE")
                return document

        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root, collectors=FailingCollectors())
            proposal_bytes = summary.proposal_path.read_bytes()
            proposal = _load_proposal(summary)
            rendered_summary = repr(summary).encode("utf-8")

            self.assertEqual(summary.status, "PREPARED_BLOCKED")
            self.assertEqual(
                summary.reason_codes,
                ("SOURCE_COLLECTION_FAILED", "SOURCE_RESULT_INVALID"),
            )
            self.assertTrue(set(summary.reason_codes) <= _SAFE_REASON_CODES)
            failures = proposal["source_failures"]
            assert isinstance(failures, list)
            self.assertEqual(
                set().union(*(set(item) for item in failures)),
                {"reason_code", "source_key"},
            )
            self.assertEqual(
                [item["source_key"] for item in failures],
                sorted(item["source_key"] for item in failures),
            )
            for forbidden in (
                secret_exception.encode(),
                b"secret-token",
                b"invalid-secret-publisher",
            ):
                self.assertNotIn(forbidden, proposal_bytes)
                self.assertNotIn(forbidden, rendered_summary)

    def test_oversized_or_hash_inconsistent_body_is_a_closed_invalid_result(self) -> None:
        class InvalidBodyCollectors(FixtureCollectors):
            def collect(
                self,
                authority: EvidenceAuthority,
            ) -> ProposalSourceObservation:
                observation = super().collect(authority)
                if authority.requested_url.endswith("default.aspx"):
                    return replace(observation, content_sha256="0" * 64)
                if authority.requested_url.endswith("rss-feed.rss"):
                    body = b"x" * 4_194_305
                    return replace(
                        observation,
                        body=body,
                        content_sha256=hashlib.sha256(body).hexdigest(),
                    )
                return observation

        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root, collectors=InvalidBodyCollectors())
            proposal = _load_proposal(summary)
            self.assertEqual(summary.status, "PREPARED_BLOCKED")
            self.assertEqual(summary.reason_codes, ("SOURCE_RESULT_INVALID",))
            self.assertEqual(len(proposal["observations"]), len(EVIDENCE_AUTHORITIES) - 2)
            self.assertFalse(
                (summary.proposal_path.parent / "artifacts" / f"{'0' * 64}.json").exists()
            )


class EvidenceProposalFilesystemTests(unittest.TestCase):
    def test_prepare_does_not_change_active_evidence_tree_or_compiled_pin(self) -> None:
        before = _active_tree_snapshot(PROJECT_ROOT)
        pin_before = evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256
        with _private_workspace() as (_, state_root):
            _prepare(state_root)
        self.assertEqual(_active_tree_snapshot(PROJECT_ROOT), before)
        self.assertEqual(evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256, pin_before)

    def test_created_tree_is_private_and_contains_only_fixed_paths(self) -> None:
        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root)
            proposal_root = summary.proposal_path.parent
            directories = [
                state_root / "evidence-proposals",
                proposal_root,
                proposal_root / "artifacts",
            ]
            files = [
                summary.proposal_path,
                proposal_root / "review-template.json",
                *(proposal_root / "artifacts").iterdir(),
            ]
            for directory in directories:
                details = directory.lstat()
                self.assertTrue(stat.S_ISDIR(details.st_mode))
                self.assertEqual(stat.S_IMODE(details.st_mode), 0o700)
                self.assertEqual(details.st_uid, os.getuid())
            for path in files:
                details = path.lstat()
                self.assertTrue(stat.S_ISREG(details.st_mode))
                self.assertEqual(stat.S_IMODE(details.st_mode), 0o600)
                self.assertEqual(details.st_nlink, 1)
                self.assertEqual(details.st_uid, os.getuid())
            relative = {
                path.relative_to(proposal_root).as_posix()
                for path in proposal_root.rglob("*")
            }
            self.assertEqual(
                relative,
                {
                    "artifacts",
                    "proposal.json",
                    "review-template.json",
                    *{
                        item["artifact_path"]
                        for item in _load_proposal(summary)["observations"]
                    },
                },
            )

    def test_existing_addressed_tree_must_match_every_expected_byte(self) -> None:
        cases = ("tampered-template", "unexpected-file", "symlink-leaf")
        for case in cases:
            with self.subTest(case=case), _private_workspace() as (root, state_root):
                summary = _prepare(state_root)
                proposal_root = summary.proposal_path.parent
                if case == "tampered-template":
                    target = proposal_root / "review-template.json"
                    target.write_bytes(b"tampered-template-canary")
                    target.chmod(0o600)
                    protected = target
                    protected_bytes = target.read_bytes()
                elif case == "unexpected-file":
                    target = proposal_root / "unexpected"
                    target.write_bytes(b"unexpected-canary")
                    target.chmod(0o600)
                    protected = target
                    protected_bytes = target.read_bytes()
                else:
                    external = root / "external-canary"
                    external.write_bytes(b"external-canary")
                    external.chmod(0o600)
                    target = proposal_root / "review-template.json"
                    target.unlink()
                    target.symlink_to(external)
                    protected = external
                    protected_bytes = external.read_bytes()

                with self.assertRaises(EvidenceWorkflowError):
                    _prepare(state_root)

                self.assertEqual(protected.read_bytes(), protected_bytes)

    def test_rejects_relative_symlinked_and_insecure_roots_before_collection(self) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            fixture = FixtureCollectors()

            with self.assertRaises(EvidenceWorkflowError):
                prepare_evidence_proposal(
                    project_root=Path("relative-project"),
                    state_root=state_root,
                    as_of=NOW,
                    collect=fixture.collect,
                    collect_sec=fixture.collect_sec,
                )
            with self.assertRaises(EvidenceWorkflowError):
                prepare_evidence_proposal(
                    project_root=project_root,
                    state_root=Path("relative-state"),
                    as_of=NOW,
                    collect=fixture.collect,
                    collect_sec=fixture.collect_sec,
                )

            project_link = root / "project-link"
            project_link.symlink_to(project_root, target_is_directory=True)
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_root, collectors=fixture, project_root=project_link)

            state_link = root / "state-link"
            state_link.symlink_to(state_root, target_is_directory=True)
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_link, collectors=fixture, project_root=project_root)

            state_root.chmod(0o770)
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_root, collectors=fixture, project_root=project_root)
            state_root.chmod(0o700)
            self.assertEqual(fixture.generic_calls, [])
            self.assertEqual(fixture.sec_calls, [])

    def test_rejects_symlinked_workflow_parent_and_nonregular_current_release(self) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            external = root / "external-proposals"
            external.mkdir(mode=0o700)
            (state_root / "evidence-proposals").symlink_to(
                external,
                target_is_directory=True,
            )
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_root, project_root=project_root)
            self.assertEqual(tuple(external.iterdir()), ())

        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            current = project_root / "data/evidence/current.json"
            external = root / "external-current"
            external.write_bytes(current.read_bytes())
            external.chmod(0o600)
            current.unlink()
            current.symlink_to(external)
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_root, project_root=project_root)

        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            current = project_root / "data/evidence/current.json"
            hardlink = root / "current-hardlink"
            os.link(current, hardlink)
            with self.assertRaises(EvidenceWorkflowError):
                _prepare(state_root, project_root=project_root)

    def test_parent_manifest_rejects_an_intermediate_symlink_before_collection(
        self,
    ) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            evidence_root = project_root / "data/evidence"
            external = root / "external-evidence"
            evidence_root.rename(external)
            evidence_root.symlink_to(external, target_is_directory=True)
            fixture = FixtureCollectors()

            with self.assertRaises(EvidenceWorkflowError):
                _prepare(
                    state_root,
                    collectors=fixture,
                    project_root=project_root,
                )

            self.assertEqual(fixture.generic_calls, [])
            self.assertEqual(fixture.sec_calls, [])

    def test_parent_manifest_fifo_is_opened_nonblocking_and_rejected(self) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            current = project_root / "data/evidence/current.json"
            current.unlink()
            os.mkfifo(current, mode=0o600)
            observed_flags: list[int] = []
            original_open = os.open

            def force_safe_fifo_open(
                path: object,
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                if Path(os.fspath(path)).name == "current.json":
                    observed_flags.append(flags)
                    flags |= os.O_NONBLOCK
                return original_open(path, flags, mode, dir_fd=dir_fd)  # type: ignore[arg-type]

            with mock.patch.object(
                workflow_module.os,
                "open",
                side_effect=force_safe_fifo_open,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    _prepare(state_root, project_root=project_root)

            self.assertTrue(observed_flags)
            for flags in observed_flags:
                self.assertTrue(flags & os.O_NONBLOCK)
                self.assertTrue(flags & os.O_NOFOLLOW)

    def test_parent_manifest_rejects_descriptor_state_change_during_read(self) -> None:
        with _private_workspace() as (root, state_root):
            project_root = _copy_project(root)
            current = project_root / "data/evidence/current.json"
            current_inode = current.stat().st_ino
            original_fstat = os.fstat
            mutated = False

            def mutate_after_first_snapshot(descriptor: int):
                nonlocal mutated
                details = original_fstat(descriptor)
                if details.st_ino == current_inode and not mutated:
                    mutated = True
                    current.chmod(0o660)
                return details

            fixture = FixtureCollectors()
            with mock.patch.object(
                workflow_module.os,
                "fstat",
                side_effect=mutate_after_first_snapshot,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    _prepare(
                        state_root,
                        collectors=fixture,
                        project_root=project_root,
                    )

            self.assertTrue(mutated)
            self.assertEqual(fixture.generic_calls, [])
            self.assertEqual(fixture.sec_calls, [])

    def test_proposal_parent_swap_cannot_redirect_temporary_writes(self) -> None:
        with _private_workspace() as (root, state_root):
            proposal_parent = state_root / "evidence-proposals"
            proposal_parent.mkdir(mode=0o700)
            moved_parent = root / "moved-proposals"
            external = root / "external-proposals"
            external.mkdir(mode=0o700)
            original_mkdir = os.mkdir
            original_open = os.open
            external_write_opens: list[Path] = []
            swapped = False

            def swap_before_temporary_mkdir(
                path: object,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> None:
                nonlocal swapped
                name = Path(os.fspath(path)).name
                if not swapped and name.startswith("."):
                    proposal_parent.rename(moved_parent)
                    proposal_parent.symlink_to(external, target_is_directory=True)
                    swapped = True
                original_mkdir(path, mode, dir_fd=dir_fd)  # type: ignore[arg-type]

            def record_open(
                path: object,
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                candidate = Path(os.fspath(path))
                if (
                    dir_fd is None
                    and candidate.is_absolute()
                    and flags & os.O_ACCMODE != os.O_RDONLY
                ):
                    resolved = candidate.resolve(strict=False)
                    if resolved == external or external in resolved.parents:
                        external_write_opens.append(resolved)
                return original_open(path, flags, mode, dir_fd=dir_fd)  # type: ignore[arg-type]

            with mock.patch.object(
                workflow_module.os,
                "mkdir",
                side_effect=swap_before_temporary_mkdir,
            ), mock.patch.object(
                workflow_module.os,
                "open",
                side_effect=record_open,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    _prepare(state_root)

            self.assertTrue(swapped)
            self.assertEqual(external_write_opens, [])

    def test_existing_tree_is_locked_before_collision_verification(self) -> None:
        with _private_workspace() as (_, state_root):
            _prepare(state_root)
            lock_held = False
            verification_lock_states: list[bool] = []
            original_flock = fcntl.flock
            original_verify = workflow_module._verify_proposal_tree

            def record_flock(descriptor: int, operation: int) -> object:
                nonlocal lock_held
                if operation & fcntl.LOCK_EX:
                    result = original_flock(descriptor, operation)
                    lock_held = True
                    return result
                if operation & fcntl.LOCK_UN:
                    lock_held = False
                return original_flock(descriptor, operation)

            def record_verify(*args: object, **kwargs: object) -> object:
                verification_lock_states.append(lock_held)
                return original_verify(*args, **kwargs)

            with mock.patch.object(
                workflow_module.fcntl,
                "flock",
                side_effect=record_flock,
            ), mock.patch.object(
                workflow_module,
                "_verify_proposal_tree",
                side_effect=record_verify,
            ):
                _prepare(state_root)

            self.assertTrue(verification_lock_states)
            self.assertTrue(all(verification_lock_states))

    def test_existing_tree_rejects_file_mode_or_link_change_during_read(self) -> None:
        for case in ("mode", "hardlink"):
            with self.subTest(case=case), _private_workspace() as (root, state_root):
                summary = _prepare(state_root)
                target = summary.proposal_path
                target_inode = target.stat().st_ino
                original_fstat = os.fstat
                mutated = False

                def mutate_after_first_snapshot(descriptor: int):
                    nonlocal mutated
                    details = original_fstat(descriptor)
                    if details.st_ino == target_inode and not mutated:
                        mutated = True
                        if case == "mode":
                            target.chmod(0o640)
                        else:
                            os.link(target, root / "proposal-hardlink")
                    return details

                with mock.patch.object(
                    workflow_module.os,
                    "fstat",
                    side_effect=mutate_after_first_snapshot,
                ):
                    with self.assertRaises(EvidenceWorkflowError):
                        _prepare(state_root)

                self.assertTrue(mutated)

    def test_existing_tree_is_reenumerated_after_file_verification(self) -> None:
        with _private_workspace() as (_, state_root):
            summary = _prepare(state_root)
            target = summary.proposal_path.parent / "review-template.json"
            target_inode = target.stat().st_ino
            unexpected = summary.proposal_path.parent / "late-unexpected"
            original_fstat = os.fstat
            snapshots = 0

            def add_entry_after_final_file_snapshot(descriptor: int):
                nonlocal snapshots
                details = original_fstat(descriptor)
                if details.st_ino == target_inode:
                    snapshots += 1
                    if snapshots == 2:
                        unexpected.write_bytes(b"late-unexpected-canary")
                        unexpected.chmod(0o600)
                return details

            with mock.patch.object(
                workflow_module.os,
                "fstat",
                side_effect=add_entry_after_final_file_snapshot,
            ):
                with self.assertRaises(EvidenceWorkflowError):
                    _prepare(state_root)

            self.assertEqual(snapshots, 2)

    def test_atomic_write_failure_leaves_no_addressed_or_temporary_tree(self) -> None:
        with _private_workspace() as (_, state_root):
            original_replace = os.replace

            def fail_review_template(source: object, target: object) -> None:
                if Path(target).name == "review-template.json":
                    raise OSError("atomic-write-canary")
                original_replace(source, target)

            with mock.patch.object(
                workflow_module.os,
                "replace",
                side_effect=fail_review_template,
            ):
                with self.assertRaises(EvidenceWorkflowError) as raised:
                    _prepare(state_root)

            self.assertNotIn("atomic-write-canary", str(raised.exception))
            proposal_parent = state_root / "evidence-proposals"
            self.assertEqual(tuple(proposal_parent.iterdir()), ())

    def test_temporary_directory_open_failure_is_cleaned_without_leaking(self) -> None:
        with _private_workspace() as (_, state_root):
            original_open = os.open
            injected = False

            def fail_after_temporary_mkdir(
                path: object,
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                nonlocal injected
                if (
                    not injected
                    and dir_fd is not None
                    and Path(os.fspath(path)).name.startswith(".")
                    and flags & os.O_DIRECTORY
                ):
                    injected = True
                    raise OSError("temporary-open-canary")
                return original_open(path, flags, mode, dir_fd=dir_fd)  # type: ignore[arg-type]

            with mock.patch.object(
                workflow_module.os,
                "open",
                side_effect=fail_after_temporary_mkdir,
            ):
                with self.assertRaises(EvidenceWorkflowError) as raised:
                    _prepare(state_root)

            self.assertTrue(injected)
            self.assertNotIn("temporary-open-canary", str(raised.exception))
            proposal_parent = state_root / "evidence-proposals"
            self.assertEqual(tuple(proposal_parent.iterdir()), ())


if __name__ == "__main__":
    unittest.main()
