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
