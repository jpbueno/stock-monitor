# Evidence Authority Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a GET-only, digest-bound evidence proposal, inspection, and offline installation workflow without allowing public-source silence to mint broad clear coverage or scheduled authority.

**Architecture:** A compiled authority catalog feeds a dedicated read-only source adapter. Preparation writes unreviewed material only under the private state root; inspection compiles a candidate from a reviewer-authored closed-schema input; installation remains network-free and succeeds only after the intended release digest has independently become the compiled current pin. The existing evidence loader remains the final authority issuer.

**Tech Stack:** Python 3.13 standard library (`argparse`, `dataclasses`, `datetime`, `hashlib`, `json`, `os`, `pathlib`, `stat`, `tempfile`, `urllib` through the designated HTTP boundary), `unittest`, canonical JSON, SHA-256, Git/GitHub, Linear.

---

## File map

- Create `src/stock_monitor/evidence_authorities.py`: immutable exact-source policy and clear-capability decisions.
- Create `src/stock_monitor/providers/evidence_sources.py`: exact-catalog GET adapter returning unreviewed source observations.
- Create `src/stock_monitor/evidence_release_workflow.py`: proposal, review-input, candidate, secure-read/write, and offline-install services.
- Modify `src/stock_monitor/evidence.py`: import shared scoped policy, reject duplicate URL/content purpose reuse, retain compiled release pin.
- Modify `src/stock_monitor/providers/sec.py`: emit loader-compatible source-specific metadata labels.
- Modify `src/stock_monitor/cli.py`: add safe `evidence prepare|inspect|install` commands.
- Modify `docs/operations.md` and `docs/scheduled-prompts.md`: document the daily human gate and zero-schedule consequence.
- Create `tests/unit/test_evidence_authorities.py`.
- Create `tests/contract/test_evidence_sources.py`.
- Create `tests/unit/test_evidence_release_workflow.py`.
- Extend `tests/contract/test_sec.py`, `tests/unit/test_evidence.py`, `tests/e2e/test_provider_cli.py`, `tests/security/test_network_boundary.py`, and `tests/architecture/test_brokerage_boundary.py`.

### Task 1: Centralize exact evidence authority policy and align SEC metadata

**Files:**

- Create: `src/stock_monitor/evidence_authorities.py`
- Create: `tests/unit/test_evidence_authorities.py`
- Modify: `src/stock_monitor/evidence.py:74-181`
- Modify: `src/stock_monitor/providers/sec.py:430-500`
- Modify: `tests/contract/test_sec.py:180-210`

- [ ] **Step 1: Write failing authority-policy tests**

Add tests that express the immutable API before the module exists:

```python
from stock_monitor.evidence_authorities import (
    CLEAR_COVERAGE_AUTHORITY_BUNDLES,
    EVIDENCE_AUTHORITIES,
    EvidenceAuthority,
    authorities_for,
    scoped_reference_authorities,
)

class EvidenceAuthorityPolicyTests(unittest.TestCase):
    def test_every_source_is_exact_subject_scoped_and_fact_only(self) -> None:
        self.assertTrue(EVIDENCE_AUTHORITIES)
        self.assertEqual(CLEAR_COVERAGE_AUTHORITY_BUNDLES, {})
        for source in EVIDENCE_AUTHORITIES:
            self.assertIs(type(source), EvidenceAuthority)
            self.assertEqual(source.purpose, "FACT_DISCOVERY")
            self.assertFalse(source.clear_capable)
            self.assertTrue(source.requested_url.startswith("https://"))
            self.assertIn(source.requested_url, source.allowed_final_urls)
            self.assertEqual(source.role.rsplit(":", 1)[-1], source.symbol)

    def test_catalog_covers_exact_current_universe_without_generic_roles(self) -> None:
        self.assertEqual(
            {source.symbol for source in EVIDENCE_AUTHORITIES},
            {"AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"},
        )
        for symbol in {"AAPL", "AMD", "NVDA", "QQQ", "SPY", "VTI", "XLK"}:
            self.assertGreaterEqual(len(authorities_for(symbol)), 2)
        self.assertNotIn("ISSUER_IR:*", scoped_reference_authorities())
```

Extend the SEC contract to require `SEC_SUBMISSIONS_METADATA` for `get_submission()` and `SEC_FILING_METADATA` for `get_archive()`, then construct `EvidenceSourceBinding.from_document()` from each client-issued document.

- [ ] **Step 2: Run the tests and observe the intended RED failures**

Run:

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence_authorities tests.contract.test_sec -v
```

Expected: `test_evidence_authorities` fails because the module is absent; the new SEC assertions fail because the client still emits `SEC_ACCEPTANCE_METADATA`.

- [ ] **Step 3: Implement the minimal immutable catalog**

Define the closed policy object and helpers:

```python
@dataclass(frozen=True, slots=True)
class EvidenceAuthority:
    symbol: str
    issuer_cik: str | None
    requested_url: str
    allowed_final_urls: frozenset[str]
    publisher: str
    role: str
    event_class: str
    purpose: str = "FACT_DISCOVERY"
    timestamp_rule: str = "PRIMARY_ITEM_METADATA"
    clear_capable: bool = False

def authorities_for(symbol: str) -> tuple[EvidenceAuthority, ...]:
    return tuple(source for source in EVIDENCE_AUTHORITIES if source.symbol == symbol)

def scoped_reference_authorities() -> dict[str, tuple[str | None, frozenset[tuple[str, str]]]]:
    grouped: dict[str, set[tuple[str, str]]] = {}
    issuer_by_role: dict[str, str | None] = {}
    for source in EVIDENCE_AUTHORITIES:
        if not source.role.startswith(("ISSUER_IR:", "CORPORATE_ACTION:")):
            continue
        previous = issuer_by_role.setdefault(source.role, source.issuer_cik)
        if previous != source.issuer_cik:
            raise RuntimeError("evidence role has conflicting issuer identity")
        grouped.setdefault(source.role, set()).update(
            (url, source.publisher) for url in source.allowed_final_urls
        )
    return {
        role: (issuer_by_role[role], frozenset(values))
        for role, values in sorted(grouped.items())
    }

CLEAR_COVERAGE_AUTHORITY_BUNDLES: dict[
    tuple[str, str], frozenset[frozenset[tuple[str, str, str]]]
] = {}
```

Populate exact URLs and publishers from the design spec. Validate all entries at import time: stock CIKs are ten digits; ETF CIKs are `None`; roles are `ISSUER_IR:<symbol>`; event classes match product type; URLs are credential-free HTTPS; final URLs are non-empty and same-origin; no duplicate `(symbol, URL, purpose)` exists; all sources remain fact-only and not clear-capable.

In `evidence.py`, keep `_SCOPED_REFERENCE_AUTHORITIES` as a compatibility alias returned by `scoped_reference_authorities()` and keep `_CLEAR_COVERAGE_AUTHORITIES` empty. Do not alter `CURRENT_EVIDENCE_RELEASE_SHA256`.

Change only the two SEC metadata labels:

```python
timestamp_source=(
    "SEC_SUBMISSIONS_METADATA" if published_at is not None else "UNAVAILABLE"
)
# archive path
timestamp_source="SEC_FILING_METADATA"
```

- [ ] **Step 4: Run the focused tests and observe GREEN**

Run the Step 2 command. Expected: all authority and SEC contract tests pass with no warnings.

- [ ] **Step 5: Run regression boundaries**

Run:

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence tests.integration.test_canonical_workflow_foundation tests.architecture.test_brokerage_boundary -v
```

Expected: all tests pass; the compiled release digest is unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/stock_monitor/evidence_authorities.py src/stock_monitor/evidence.py src/stock_monitor/providers/sec.py tests/unit/test_evidence_authorities.py tests/contract/test_sec.py
git commit -m "feat: define evidence source authority policy"
```

### Task 2: Add the exact GET-only evidence-source adapter

**Files:**

- Create: `src/stock_monitor/providers/evidence_sources.py`
- Create: `tests/contract/test_evidence_sources.py`
- Modify: `tests/security/test_network_boundary.py`
- Modify: `tests/architecture/test_brokerage_boundary.py`

- [ ] **Step 1: Write failing adapter contracts**

Use the existing fixture transport style to require the wished-for API:

```python
client = EvidenceSourceClient(
    transport=transport,
    now=lambda: datetime(2026, 8, 24, 14, 0, tzinfo=UTC),
)
observation = client.fetch(authority)
self.assertEqual(observation.url, authority.requested_url)
self.assertEqual(observation.publisher, authority.publisher)
self.assertEqual(observation.role, authority.role)
self.assertEqual(observation.content_sha256, sha256(body).hexdigest())
self.assertIsNone(observation.published_at)
self.assertEqual(transport.calls, [(authority.requested_url, expected_headers)])
```

Add cases for a caller-constructed equal authority, off-catalog URL, cross-origin redirect, unlisted same-origin redirect, wrong content type, empty/oversized response, future clock, duplicate observation identity with changed bytes, and safe error redaction. Extend architecture/security tests to prove the new module has no direct `urllib`, proxy, subprocess, browser, broker, trading, or order surface.

- [ ] **Step 2: Run and observe RED**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.contract.test_evidence_sources tests.security.test_network_boundary tests.architecture.test_brokerage_boundary -v
```

Expected: import failure for `providers.evidence_sources` while existing security tests remain green.

- [ ] **Step 3: Implement the minimal adapter**

Expose immutable output only:

```python
@dataclass(frozen=True, slots=True)
class ProposalSourceObservation:
    observation_id: str
    symbol: str
    issuer_cik: str | None
    url: str
    publisher: str
    role: str
    event_class: str
    retrieved_at: datetime
    published_at: datetime | None
    timestamp_source: str
    content_sha256: str
    body: bytes = field(repr=False)

class EvidenceSourceClient:
    def fetch(self, authority: EvidenceAuthority) -> ProposalSourceObservation:
        if not any(authority is value for value in EVIDENCE_AUTHORITIES):
            raise NetworkPolicyError("evidence source is not the compiled authority")
        policy = EgressPolicy(
            {urlsplit(url).hostname or "" for url in authority.allowed_final_urls}
        )
        response = get_with_redirects(
            self._transport,
            policy,
            authority.requested_url,
            {"Accept": "application/json,application/xml,text/html,text/plain"},
            max_bytes=4_194_304,
            exact_url_validator=lambda target: self._require_exact_target(
                authority, target
            ),
        )
        retrieved_at = require_aware_timestamp(
            self._now(), "evidence retrieval time"
        ).astimezone(UTC)
        content_sha256 = hashlib.sha256(response.body).hexdigest()
        identity = hashlib.sha256(
            b"\0".join(
                (
                    authority.symbol.encode("ascii"),
                    authority.role.encode("ascii"),
                    response.url.encode("ascii"),
                    retrieved_at.isoformat(timespec="microseconds").encode("ascii"),
                    response.body,
                )
            )
        ).hexdigest()
        return ProposalSourceObservation(
            observation_id=f"proposal-{identity[:24]}",
            symbol=authority.symbol,
            issuer_cik=authority.issuer_cik,
            url=response.url,
            publisher=authority.publisher,
            role=authority.role,
            event_class=authority.event_class,
            retrieved_at=retrieved_at,
            published_at=None,
            timestamp_source="UNAVAILABLE",
            content_sha256=content_sha256,
            body=response.body,
        )
```

Use only `EgressPolicy`, `GetTransport`, and `get_with_redirects()` from `providers.http`. Publication time remains `None`/`UNAVAILABLE` for a whole page or feed; the adapter never accepts caller-supplied metadata.

- [ ] **Step 4: Run and observe GREEN**

Run the Step 2 command. Expected: all tests pass warning-strict.

- [ ] **Step 5: Commit**

```bash
git add src/stock_monitor/providers/evidence_sources.py tests/contract/test_evidence_sources.py tests/security/test_network_boundary.py tests/architecture/test_brokerage_boundary.py
git commit -m "feat: add exact evidence source retrieval"
```

### Task 3: Prepare immutable unreviewed proposals

**Files:**

- Create: `src/stock_monitor/evidence_release_workflow.py`
- Create: `tests/unit/test_evidence_release_workflow.py`

- [ ] **Step 1: Write failing proposal tests**

Create deterministic fixture collectors and assert this API:

```python
summary = prepare_evidence_proposal(
    project_root=project_root,
    state_root=state_root,
    as_of=NOW,
    collect=collector.collect,
)
self.assertEqual(summary.status, "PREPARED")
self.assertRegex(summary.proposal_sha256, r"[0-9a-f]{64}")
self.assertFalse((project_root / "data/evidence/current.json").read_bytes() != before)
self.assertNotIn(b"CONFIRMED_CLEAR", summary.proposal_path.read_bytes())
self.assertNotIn(b"reviewed_at", summary.proposal_path.read_bytes())
```

Add tests for canonical digest determinism, current-parent pin capture, exact universe ordering, template relevant `UNKNOWN`/incomplete and opposite `NOT_APPLICABLE`/complete, partial-source `PREPARED_BLOCKED`, content-address collision, private modes, atomic writes, symlinked roots, active-tree immutability, and safe summary fields.

- [ ] **Step 2: Run and observe RED**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence_release_workflow -v
```

Expected: import failure for the workflow module.

- [ ] **Step 3: Implement canonical proposal types and storage**

Define public result types and a single preparation entry point:

```python
@dataclass(frozen=True, slots=True)
class EvidenceProposalSummary:
    status: str
    proposal_sha256: str
    universe_sha256: str
    parent_release_sha256: str
    symbols: tuple[str, ...]
    reason_codes: tuple[str, ...]
    proposal_path: Path

def prepare_evidence_proposal(
    *, project_root: Path, state_root: Path, as_of: datetime,
    collect: Callable[[EvidenceAuthority], ProposalSourceObservation],
    collect_sec: Callable[[str], SourceDocument] | None = None,
) -> EvidenceProposalSummary:
    current = require_aware_timestamp(as_of, "proposal as_of").astimezone(UTC)
    universe = load_current_universe(project_root, as_of=current.date())
    parent_sha256 = _current_release_digest(project_root)
    observations, failures = _collect_proposal_sources(
        universe=universe,
        collect=collect,
        collect_sec=collect_sec,
    )
    proposal, template, artifacts = _build_proposal_documents(
        current=current,
        universe=universe,
        parent_sha256=parent_sha256,
        observations=observations,
        failures=failures,
    )
    proposal_sha256, proposal_path = _write_proposal_tree(
        state_root=state_root,
        proposal=proposal,
        review_template=template,
        artifacts=artifacts,
    )
    return EvidenceProposalSummary(
        status="PREPARED_BLOCKED" if failures else "PREPARED",
        proposal_sha256=proposal_sha256,
        universe_sha256=universe._release_pin,
        parent_release_sha256=parent_sha256,
        symbols=tuple(record.symbol for record in universe.eligible_records()),
        reason_codes=tuple(sorted(failures)),
        proposal_path=proposal_path,
    )
```

Implement the named private helpers in the same file with these exact contracts: `_current_release_digest()` reads and hashes only the regular `data/evidence/current.json`; `_collect_proposal_sources()` returns observations plus closed reason codes; `_build_proposal_documents()` returns strict proposal/template/artifact mappings; `_write_proposal_tree()` writes private canonical files and returns the digest plus canonical proposal path. Use the existing pinned universe loader and raw artifact envelope format. A failed source becomes an allowlisted reason code, never exception text. The function never imports the CLI, runtime workflows, journal, or scheduled modules.

- [ ] **Step 4: Run and observe GREEN**

Run the Step 2 command. Expected: all proposal tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/stock_monitor/evidence_release_workflow.py tests/unit/test_evidence_release_workflow.py
git commit -m "feat: prepare unreviewed evidence proposals"
```

### Task 4: Compile and inspect reviewer-authored candidates without authority

**Files:**

- Modify: `src/stock_monitor/evidence_release_workflow.py`
- Modify: `tests/unit/test_evidence_release_workflow.py`
- Modify: `src/stock_monitor/evidence.py`
- Modify: `tests/unit/test_evidence.py`

- [ ] **Step 1: Write failing review-input and candidate tests**

Exercise the closed interface:

```python
summary = inspect_evidence_candidate(
    project_root=project_root,
    state_root=state_root,
    proposal_sha256=proposal.proposal_sha256,
    review_input_path=review_input_path,
    as_of=NOW,
)
self.assertEqual(summary.status, "AWAITING_DIGEST_APPROVAL")
self.assertEqual(summary.proposal_sha256, proposal.proposal_sha256)
self.assertEqual(summary.release_sha256, sha256(current_bytes).hexdigest())
self.assertFalse((project_root / "data/evidence/current.json").read_bytes() != before)
```

Add one test per rejection: duplicate keys; extra fields; mismatched proposal/universe/symbol/CIK; future or over-24-hour review window; observation not in proposal; URL/hash/role override; relevant `NOT_APPLICABLE`; requested `CONFIRMED_CLEAR`; unsupported taxonomy; fact/coverage reuse by ID; fact/coverage reuse by identical URL+content hash; stale or unhealthy binding; missing relevant coverage; unsafe input path; and candidate directory collision.

Add a positive NVDA fixture with a dated August 26 event record and incomplete unknown coverage. Load the candidate with `load_evidence_release()` and assert classification resolves `BINARY_EVENT_DURING_HOLD`, not clear.

- [ ] **Step 2: Run and observe RED**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence_release_workflow tests.unit.test_evidence -v
```

Expected: missing `inspect_evidence_candidate`; the new identical-source purpose-reuse test fails against the current loader.

- [ ] **Step 3: Implement reviewer-input decoding and candidate compilation**

Expose:

```python
@dataclass(frozen=True, slots=True)
class EvidenceCandidateSummary:
    status: str
    candidate_sha256: str
    proposal_sha256: str
    review_input_sha256: str
    release_sha256: str
    universe_sha256: str
    reviewed_at: datetime
    review_by: datetime
    symbols: tuple[str, ...]
    coverage: tuple[tuple[str, str, str], ...]
    reason_codes: tuple[str, ...]
    candidate_path: Path

def inspect_evidence_candidate(
    *, project_root: Path, state_root: Path,
    proposal_sha256: str, review_input_path: Path, as_of: datetime,
) -> EvidenceCandidateSummary:
    current = require_aware_timestamp(as_of, "candidate as_of").astimezone(UTC)
    proposal = _load_proposal(state_root, proposal_sha256)
    review_input, review_input_sha256 = _load_review_input(review_input_path)
    release_tree = _compile_candidate_release(
        project_root=project_root,
        proposal=proposal,
        review_input=review_input,
        as_of=current,
    )
    release_sha256 = hashlib.sha256(release_tree["current.json"]).hexdigest()
    universe = load_current_universe(project_root, as_of=current.date())
    _validate_candidate_release(
        release_tree=release_tree,
        release_sha256=release_sha256,
        universe=universe,
        as_of=current,
    )
    candidate_sha256, candidate_path = _write_candidate_tree(
        state_root=state_root,
        proposal_sha256=proposal_sha256,
        review_input_sha256=review_input_sha256,
        release_sha256=release_sha256,
        release_tree=release_tree,
    )
    return _candidate_summary(
        candidate_sha256=candidate_sha256,
        candidate_path=candidate_path,
        proposal=proposal,
        review_input_sha256=review_input_sha256,
        release_sha256=release_sha256,
        release_tree=release_tree,
    )
```

Implement the named helpers in the same file. `_load_proposal()` and `_load_review_input()` use confined regular-file reads and duplicate-key rejection. `_compile_candidate_release()` accepts exact subject records, source metadata extracted from proposal bytes, and exactly two coverage attestations per subject, then returns a mapping from fixed relative paths to canonical bytes. `_validate_candidate_release()` materializes that mapping in a private temporary directory and calls `load_evidence_release()` with the candidate digest, current universe, and `as_of`. `_write_candidate_tree()` writes the wrapper/inventory and immutable release tree. `_candidate_summary()` reads only canonical candidate fields. Do not call `load_current_evidence_release()` or change its pin.

In `evidence.py`, extend purpose separation so record-source `(url, content_hash)` pairs cannot intersect coverage-source pairs even when observation IDs differ.

- [ ] **Step 4: Run and observe GREEN**

Run the Step 2 command. Expected: all tests pass and the compiled current pin remains unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/stock_monitor/evidence_release_workflow.py src/stock_monitor/evidence.py tests/unit/test_evidence_release_workflow.py tests/unit/test_evidence.py
git commit -m "feat: inspect digest-bound evidence candidates"
```

### Task 5: Install only independently pinned candidates

**Files:**

- Modify: `src/stock_monitor/evidence_release_workflow.py`
- Modify: `tests/unit/test_evidence_release_workflow.py`

- [ ] **Step 1: Write failing offline-install tests**

Use a copied project root and candidate fixture:

```python
summary = install_evidence_candidate(
    project_root=project_root,
    state_root=state_root,
    candidate_sha256=candidate.candidate_sha256,
    as_of=NOW,
)
self.assertEqual(summary.status, "INSTALLED")
self.assertEqual(
    sha256((project_root / "data/evidence/current.json").read_bytes()).hexdigest(),
    candidate.release_sha256,
)
```

Patch only the test process's compiled pin for the positive case. Add failures for unpinned digest, stale parent, changed universe, expired candidate, source/child/wrapper tamper, symlink/FIFO/hardlink/device/oversize, destination symlink, candidate-selected destination, and a simulated failure before manifest replacement. Assert the install path invokes no collector/provider and preserves all timestamps. Assert repeated same-release install returns `ALREADY_INSTALLED` after full readback.

- [ ] **Step 2: Run and observe RED**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence_release_workflow -v
```

Expected: missing `install_evidence_candidate`.

- [ ] **Step 3: Implement fixed-destination, manifest-last install**

Expose:

```python
@dataclass(frozen=True, slots=True)
class EvidenceInstallSummary:
    status: str
    candidate_sha256: str
    release_sha256: str
    installed_at: datetime
    symbols: tuple[str, ...]

def install_evidence_candidate(
    *, project_root: Path, state_root: Path,
    candidate_sha256: str, as_of: datetime,
) -> EvidenceInstallSummary:
    current = require_aware_timestamp(as_of, "installation as_of").astimezone(UTC)
    candidate = _load_candidate(state_root, candidate_sha256)
    release_sha256 = candidate["release_sha256"]
    if release_sha256 != evidence_module.CURRENT_EVIDENCE_RELEASE_SHA256:
        raise EvidenceWorkflowError("candidate release is not independently pinned")
    if _current_release_digest(project_root) != candidate["parent_release_sha256"]:
        if _current_release_digest(project_root) == release_sha256:
            return _read_installed_summary(
                project_root, candidate_sha256, release_sha256, current
            )
        raise EvidenceWorkflowError("candidate parent release changed")
    release_tree = _read_candidate_release_tree(candidate)
    universe = load_current_universe(project_root, as_of=current.date())
    _validate_candidate_release(
        release_tree=release_tree,
        release_sha256=release_sha256,
        universe=universe,
        as_of=current,
    )
    _install_release_tree_manifest_last(project_root, release_tree)
    return _read_installed_summary(
        project_root, candidate_sha256, release_sha256, current
    )
```

Implement the named helpers in the same file. `_load_candidate()` securely validates wrapper and inventory; `_read_candidate_release_tree()` permits only `sources/<sha>.json`, `subjects/<symbol>.json`, and `current.json`; `_install_release_tree_manifest_last()` writes artifacts and subjects through fixed mappings before the manifest; `_read_installed_summary()` re-reads active evidence with `load_current_evidence_release()` before returning success.

- [ ] **Step 4: Run and observe GREEN**

Run the Step 2 command. Expected: all workflow tests pass warning-strict.

- [ ] **Step 5: Commit**

```bash
git add src/stock_monitor/evidence_release_workflow.py tests/unit/test_evidence_release_workflow.py
git commit -m "feat: install independently pinned evidence"
```

### Task 6: Wire safe CLI commands and operator documentation

**Files:**

- Modify: `src/stock_monitor/cli.py`
- Modify: `tests/e2e/test_provider_cli.py`
- Modify: `docs/operations.md`
- Modify: `docs/scheduled-prompts.md`

- [ ] **Step 1: Write failing CLI tests**

Require parser grammar and allowlisted JSON for:

```text
stock-monitor evidence prepare --json
stock-monitor evidence inspect --proposal <64hex> --review-input /absolute/review.json --json
stock-monitor evidence install --candidate <64hex> --json
```

Mock the workflow boundary, not provider internals. Assert exact exit mapping, absolute review-input requirement, digest grammar, safe fields only, no exception/source-body/environment leakage, and that `inspect`/`install` never construct a network client.

- [ ] **Step 2: Run and observe RED**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_provider_cli tests.e2e.test_cli -v
```

Expected: parser rejects the top-level `evidence` command.

- [ ] **Step 3: Implement CLI construction and dispatch**

Add the grammar in `build_parser()` and dispatch through three shallow helpers. `prepare` constructs `HttpGetClient`, `EvidenceSourceClient`, `ContentCache`, `SecRateGovernor`, and `SecClient` from existing settings; `inspect` and `install` import no provider. Emit only summary safe fields. Map proposal-source incompleteness to a nonzero data exit without discarding its safe proposal digest.

Document this exact daily sequence:

```text
scripts/run_monitor.sh evidence prepare --json
scripts/run_monitor.sh evidence inspect --proposal <proposal_sha256> --review-input <absolute-path> --json
# Human reviews exact candidate output and separately updates CURRENT_EVIDENCE_RELEASE_SHA256.
scripts/run_monitor.sh evidence install --candidate <candidate_sha256> --json
scripts/run_monitor_unattended.sh verify evidence --json
```

State explicitly that current public sources leave relevant coverage unknown, the NVDA August 26 event overlaps the current hold window, and schedules remain zero until a separately approved authority/policy change clears every activation gate.

- [ ] **Step 4: Run and observe GREEN**

Run the Step 2 command. Expected: all CLI tests pass warning-strict.

- [ ] **Step 5: Commit**

```bash
git add src/stock_monitor/cli.py tests/e2e/test_provider_cli.py docs/operations.md docs/scheduled-prompts.md
git commit -m "feat: expose guarded evidence operations"
```

### Task 7: Full verification, independent review, and branch publication

**Files:**

- Review all changed files from `origin/main..HEAD`.

- [ ] **Step 1: Run focused warning-strict verification**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_evidence_authorities tests.contract.test_evidence_sources tests.unit.test_evidence_release_workflow tests.contract.test_sec tests.unit.test_evidence tests.e2e.test_provider_cli tests.e2e.test_cli tests.security.test_network_boundary tests.security.test_secret_redaction tests.security.test_unattended_environment tests.architecture.test_brokerage_boundary tests.integration.test_scheduled_authority_boundary tests.integration.test_scheduled_authority_adversarial -v
```

Expected: all tests pass with no warnings.

- [ ] **Step 2: Run the complete suite and compile check**

```bash
env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -W error -m unittest discover -s tests -v
.venv/bin/python -m compileall -q src tests
```

Expected: complete suite exits `0`; compile check exits `0`. If the complete suite is impractically slow, retain its output as incomplete and do not claim it passed; run the existing release/acceptance suites from `docs/superpowers/plans/2026-08-21-provider-backed-monitoring-implementation.md` as the bounded release gate.

- [ ] **Step 3: Run static boundary scans**

```bash
git diff --check origin/main...HEAD
rg -n --hidden --glob '!\.git/**' --glob '!\.venv/**' '(APCA_API_SECRET_KEY\s*=|SEC_USER_AGENT\s*=|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY)' .
rg -n 'https://[^" ]*(orders|trading)|robinhood' src scripts config
```

Expected: no whitespace errors, no credential values/private keys, and no prohibited production endpoint.

- [ ] **Step 4: Dispatch final spec-compliance and code-quality reviewers**

Provide reviewers the full spec, plan, and exact `origin/main..HEAD` range. Fix every Critical or Important issue test-first, then re-run the relevant verification and re-review until both approve.

- [ ] **Step 5: Verify the operational blocked result**

Run `evidence prepare --json` through the interactive launcher only. Do not inspect `.env` or print source bytes. Confirm the proposal is unreviewed and current public coverage cannot clear. Do not repin/install without the user's approval of an exact candidate digest. Query automations and confirm Stock Monitor schedule count remains zero.

- [ ] **Step 6: Update Linear with readback**

Update the existing Stock Monitor project status with commits, affected modules, exact test commands/results, official-source limitation, and the required policy/authority decision. Read the update back before claiming synchronization.

- [ ] **Step 7: Push the branch**

```bash
git status --short --branch
git push -u origin codex/evidence-authority-workflow
```

Expected: clean worktree and successful remote branch update. Do not merge or create schedules while the evidence gate remains unresolved.
