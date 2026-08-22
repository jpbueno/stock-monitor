# Provider-Backed Read-Only Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Activate read-only Alpaca market-data smoke, canonical non-fixture premarket and close reports, and gated Codex heartbeats without adding any brokerage, order, Robinhood, or live-options capability.

**Architecture:** A small provider-runtime factory creates the existing GET-only clients. Separate Journal-backed canonical coordinators compose reviewed releases, raw provider observations, screening/risk/reconciliation authorities, immutable report material, and a canonical publisher; fixture authority remains untouched. A literal private-env loader supports unattended invocation, while the existing scheduler retains durable claims and widens only its due window to 15 minutes.

**Tech Stack:** Python 3.13 standard library (`dataclasses`, `Decimal`, `zoneinfo`, `urllib`, `sqlite3`, `os`, `stat`, `json`, `hashlib`), `unittest`, SQLite migrations, TOML/JSON reviewed releases, shell-free Python launcher, Codex heartbeat automations, Git/GitHub, Linear.

---

## Locked implementation defaults

- Canonical means local non-fixture mode. It does not mean brokerage execution or production readiness.
- Only `https://data.alpaca.markets` is approved for Alpaca. Paper-account credentials are authentication material, not an asserted read-only key type.
- Provider readiness is exact aggregate `READY`: fresh IEX plus delayed historical SIP for a reviewed completed session.
- The current eligible/support cohort is derived from the checksum-verified universe; the initial release has seven symbols, but code never hard-codes seven.
- Premarket economic cutoff is exactly 08:45 ET. Close review cutoff is exactly 12:30 ET on reviewed early closes and 15:30 ET otherwise. Retrieval/publication use actual timestamps.
- Due windows are half-open: `[08:45,09:00)`, `[12:30,12:45)`, and `[15:30,15:45)` ET. There is no backfill.
- Delayed close mark is the latest valid SIP bid in the five minutes ending 16 minutes before nominal review. It has no IEX/trade/cache/prior-report fallback.
- Exit mapping remains `0` success/no-op, `2` configuration, `3` provider/source/evidence, `4` policy/risk/verification, `5` reconciliation/manual boundary, `10` internal.
- Every nonzero result has zero candidates and authorizes no action.
- Fixture and canonical publishers, contexts, adapters, and capabilities are distinct.
- `.env` is literal data. No `source`, `eval`, interpolation, or command substitution is permitted.
- Options and `option_chain()` remain unwired; no trading/account/order endpoint is added.

## File map

Create:

- `src/stock_monitor/provider_smoke.py` — exact Alpaca-only smoke construction and safe result projection.
- `src/stock_monitor/provider_workflows.py` — canonical material types and premarket/close coordinators.
- `src/stock_monitor/unattended.py` — race-resistant literal four-variable `.env` loader and isolated CLI entry.
- `scripts/run_monitor_unattended.sh` — tiny exact-reviewed environment-isolating launcher.
- `src/stock_monitor/sql/005_provider_monitoring.sql` — prior close recommendations and any required canonical publication state.
- `data/evidence/subjects/*.json` — subject-scoped reviewed evidence bundles.
- `data/evidence/sources/*.json` — strict base64 envelopes for exact content-addressed raw source bytes.
- `tests/security/test_unattended_environment.py`
- `tests/contract/test_provider_smoke.py`
- `tests/integration/test_provider_workflows.py`
- `tests/integration/test_canonical_publication.py`
- `tests/integration/test_actual_close_composition.py`
- `tests/e2e/test_provider_cli.py`

Modify:

- `src/stock_monitor/cli.py` — provider smoke, Phase 1 bootstrap, and canonical dispatch.
- `src/stock_monitor/market_calendar.py` — reviewed previous-session helper.
- `src/stock_monitor/universe.py` and `data/universe/2026-08-22.json` — newly reviewed stock CIK and all-instrument listing metadata; preserve `2026-08-14.json` as history.
- `src/stock_monitor/evidence.py` and `data/evidence/current.json` — multi-subject reviewed release.
- `src/stock_monitor/reports.py` — shadow and unverified-close projections.
- `src/stock_monitor/journal.py` — typed observation receipts, lineage selectors, recommendations, and canonical publication readback.
- `src/stock_monitor/risk.py` — public actual-close source/context/mark issuer.
- `src/stock_monitor/workflows.py` — explicit canonical protocols/functions and failure precedence.
- `src/stock_monitor/scheduled.py` — 15-minute window and canonical context dispatch.
- `src/stock_monitor/sql/__init__.py` — migration discovery only if required by the current loader.
- `tests/architecture/test_brokerage_boundary.py`
- `tests/security/test_secret_redaction.py`
- `tests/contract/test_alpaca.py`
- `tests/contract/test_reference.py`
- `tests/unit/test_evidence.py`
- `tests/unit/test_universe.py`
- `tests/unit/test_report_precedence.py`
- `tests/integration/test_journal.py`
- `tests/integration/test_scheduled.py`
- `tests/e2e/test_cli.py`
- `tests/e2e/test_acceptance.py`
- `README.md`, `.env.example`, `docs/operations.md`, and `docs/scheduled-prompts.md`.

## Execution batches

- Batch A in parallel: Tasks 1, 2, and 3.
- Batch B after A in parallel: Tasks 4, 5, and 6.
- Batch C sequential authority foundation: Tasks 7, 8, and 9.
- Batch D in parallel after C: Tasks 10 and 11.
- Batch E sequential integration/release: Tasks 12, 13, and 14.

### Task 1: Classify and activate exact read-only provider smoke

**Files:**
- Modify: `src/stock_monitor/providers/http.py`
- Modify: `src/stock_monitor/providers/alpaca.py`
- Create: `src/stock_monitor/provider_smoke.py`
- Modify: `src/stock_monitor/cli.py`
- Modify: `src/stock_monitor/market_calendar.py`
- Test: `tests/contract/test_provider_smoke.py`
- Test: `tests/e2e/test_provider_cli.py`

- [ ] **Step 1: Write failing typed-classification and CLI tests**

```python
def test_ready_requires_same_invocation_fresh_iex_and_delayed_sip(self):
    result = run_provider_smoke(self.settings, now=lambda: NOW)
    self.assertEqual((result.status, result.exit_code), ("READY", 0))
    self.assertEqual(set(result.safe_fields()), {"status", "exit_code", "observed_at", "checks", "reason_codes"})

def test_each_expected_failure_is_safe_exit_three(self):
    for failure, reason in self.failure_matrix():
        with self.subTest(reason=reason):
            result = self.run_failure(failure)
            self.assertEqual(result.exit_code, 3)
            self.assertIn(reason, result.reason_codes)

def test_provider_cli_unexpected_canary_exception_is_generic_exit_ten(self):
    with patch("stock_monitor.cli.run_provider_smoke", side_effect=RuntimeError(CANARY)):
        completed = run(["provider", "smoke", "--json"], environ=self.environment)
    self.assertEqual(completed, 10)
    self.assertNotIn(CANARY, self.stderr.getvalue())
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.contract.test_provider_smoke tests.e2e.test_provider_cli -v
```

Expected: missing `provider_smoke` module and the existing CLI exit `5`.

- [ ] **Step 3: Implement typed failures, completed-session selection, and safe CLI projection**

```python
@dataclass(frozen=True, slots=True)
class ProviderSmokeResult:
    status: str
    exit_code: int
    authentication_ok: bool
    historical_sip_ok: bool
    latest_iex_fresh: bool
    observed_at: datetime
    reason_codes: tuple[str, ...]

    @classmethod
    def from_entitlement(cls, smoke: EntitlementSmoke) -> "ProviderSmokeResult":
        ready = bool(
            smoke.status == "READY"
            and smoke.authentication_ok
            and smoke.historical_sip_ok
            and smoke.latest_iex_fresh
        )
        return cls(
            status="READY" if ready else smoke.status,
            exit_code=0 if ready else 3,
            authentication_ok=smoke.authentication_ok,
            historical_sip_ok=smoke.historical_sip_ok,
            latest_iex_fresh=smoke.latest_iex_fresh,
            observed_at=smoke.observed_at,
            reason_codes=smoke.failures,
        )

    def safe_fields(self) -> dict[str, object]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "observed_at": self.observed_at.isoformat(),
            "checks": {
                "authentication": self.authentication_ok,
                "historical_sip": self.historical_sip_ok,
                "latest_iex_fresh": self.latest_iex_fresh,
            },
            "reason_codes": list(self.reason_codes),
        }

def run_provider_smoke(settings: Settings, *, now: Callable[[], datetime]) -> ProviderSmokeResult:
    observed_at = now()
    calendar = load_current_market_calendar(settings.project_root, as_of=observed_at.astimezone(_ET).date())
    completed = latest_completed_session_window(calendar, observed_at=observed_at)
    policy = EgressPolicy(("data.alpaca.markets",))
    provider = AlpacaMarketData(
        HttpGetClient(policy),
        AlpacaCredentials(settings.alpaca_api_key_id, settings.alpaca_api_secret_key),
        base_url=settings.sources.alpaca_market_data_url,
        now=lambda: observed_at,
        cache=None,
    )
    return ProviderSmokeResult.from_entitlement(provider.smoke(completed_session=completed))
```

Add typed HTTP status, transport, stale, malformed, and incomplete-cohort exceptions so `.smoke()` never parses exception strings. Safe reasons distinguish authentication, connectivity, availability, malformed response, stale IEX, incomplete cohort, SIP entitlement, and completed-session release failure. Refactor CLI to `run(argv, *, environ)` so tests and the unattended wrapper inject only an explicit environment; `main()` delegates with `os.environ`.

- [ ] **Step 4: Run provider, CLI, config, and architecture tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.contract.test_alpaca tests.contract.test_provider_smoke tests.unit.test_config tests.e2e.test_provider_cli tests.e2e.test_cli tests.architecture.test_brokerage_boundary -v
```

Expected: PASS; smoke writes no Journal/cache/report and uses only the Alpaca market-data host.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/providers/http.py src/stock_monitor/providers/alpaca.py src/stock_monitor/provider_smoke.py src/stock_monitor/market_calendar.py src/stock_monitor/cli.py tests/contract/test_provider_smoke.py tests/e2e/test_provider_cli.py tests/e2e/test_cli.py
git commit -m "feat: classify and activate read-only provider smoke"
```

### Task 2: Add the isolated literal unattended wrapper

**Files:**
- Create: `src/stock_monitor/unattended.py`
- Create: `scripts/run_monitor_unattended.sh`
- Create: `tests/security/test_unattended_environment.py`
- Modify: `tests/architecture/test_brokerage_boundary.py`
- Modify: `tests/security/test_secret_redaction.py`

- [ ] **Step 1: Write descriptor, grammar, isolation, and canary tests**

```python
def test_command_substitution_and_backticks_remain_literal(self):
    path = self.private_file(self.valid_text(key_id="$(touch marker)`ignored`"))
    values = load_literal_environment(path)
    self.assertEqual(values["APCA_API_KEY_ID"], "$(touch marker)`ignored`")
    self.assertFalse(Path("marker").exists())

def test_unsafe_files_and_entries_are_rejected(self):
    for path in self.symlink_hardlink_mode_owner_duplicate_unknown_cases():
        with self.subTest(path=path), self.assertRaises(LiteralEnvironmentError):
            load_literal_environment(path)
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.security.test_unattended_environment -v
```

Expected: missing `stock_monitor.unattended`.

- [ ] **Step 3: Implement exact descriptor parsing and an empty-environment launcher**

```python
APPROVED_KEYS = frozenset({"APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "SEC_USER_AGENT", "STOCK_MONITOR_HOME"})

def read_bounded_descriptor(descriptor: int, *, maximum_bytes: int) -> bytes:
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, min(4096, maximum_bytes + 1 - len(payload)))
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise LiteralEnvironmentError("private environment is too large")

def parse_exact_literal_assignments(payload: bytes, *, approved: frozenset[str]) -> dict[str, str]:
    try:
        text = payload.decode("utf-8")
    except UnicodeError:
        raise LiteralEnvironmentError("private environment encoding is invalid") from None
    result: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator != "=" or key not in approved or key in result:
            raise LiteralEnvironmentError("private environment entry is invalid")
        if not value or any(ord(character) < 32 for character in value):
            raise LiteralEnvironmentError("private environment value is invalid")
        result[key] = value
    if result.keys() != approved:
        raise LiteralEnvironmentError("private environment is incomplete")
    return result

def load_literal_environment(path: Path) -> dict[str, str]:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        value = os.fstat(descriptor)
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid():
            raise LiteralEnvironmentError("private environment ownership is invalid")
        if value.st_nlink != 1 or stat.S_IMODE(value.st_mode) & 0o077:
            raise LiteralEnvironmentError("private environment permissions are invalid")
        payload = read_bounded_descriptor(descriptor, maximum_bytes=16384)
    finally:
        os.close(descriptor)
    return parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

def main(argv: Sequence[str] | None = None) -> int:
    root = Path(__file__).resolve(strict=True).parents[2]
    values = load_literal_environment(root / ".env")
    return cli.run(argv, environ=values)
```

Use these exact shell semantics:

```sh
#!/bin/sh
set -eu
set +x
umask 077
case $0 in
  /*) script_path=$0 ;;
  *) exit 2 ;;
esac
script_dir=${script_path%/*}
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
python=$repo_root/.venv/bin/python3
[ -x "$python" ] || exit 2
exec /usr/bin/env -i "$python" -I -m stock_monitor.unattended "$@"
```

It never sources/evaluates `.env`, inherits proxies/PYTHONPATH/credentials, or
falls back to PATH Python.

- [ ] **Step 4: Run wrapper, architecture, network, and canary tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.security.test_unattended_environment tests.architecture.test_brokerage_boundary tests.security.test_network_boundary tests.security.test_secret_redaction -v
```

Expected: PASS, including wrapper-byte tamper detection and no canary output.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/unattended.py scripts/run_monitor_unattended.sh tests/security/test_unattended_environment.py tests/architecture/test_brokerage_boundary.py tests/security/test_secret_redaction.py
git commit -m "feat: add isolated unattended environment wrapper"
```

### Task 3: Widen only the durable scheduler due window

**Files:**
- Modify: `src/stock_monitor/scheduled.py`
- Modify: `tests/integration/test_scheduled.py`
- Modify: `tests/integration/test_scheduled_authority_boundary.py`
- Modify: `tests/integration/test_scheduled_authority_adversarial.py`

- [ ] **Step 1: Write exact boundary tests**

```python
def test_premarket_due_window_is_half_open_fifteen_minutes(self):
    self.assertEqual(self.run_at("08:45:00").outcome, "CANDIDATES")
    self.assertEqual(self.run_at("08:59:59").outcome, "CANDIDATES")
    self.assertEqual(self.run_at("09:00:00").outcome, "MISSED_RUN_NOOP")

def test_close_uses_nominal_review_time_not_dispatch_time(self):
    result = self.run_close_at("15:44:59")
    self.assertEqual(result.outcome, "EMITTED")
    self.assertEqual(self.adapter.requested_review_at.time(), time(15, 30))
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_scheduled tests.integration.test_scheduled_authority_boundary tests.integration.test_scheduled_authority_adversarial -v
```

Expected: 08:59 is currently `MISSED_RUN_NOOP` because the limit is one minute.

- [ ] **Step 3: Implement one named constant and immutable nominal time**

```python
_DUE_WINDOW = timedelta(minutes=15)

if now_et >= intended_at + _DUE_WINDOW:
    missed = _noop("MISSED_RUN_NOOP", "MISSED_RUN", _execution_mode(context))
    complete_run(
        context.scheduler,
        authority=completion_authority,
        finished_at=datetime.now(timezone.utc),
        decision="MISSED_RUN",
        result=missed,
    )
    return missed

run_context = replace(context, now=now_et, intended_at=intended_at)
```

Add `intended_at: datetime | None = None` to the canonical context only; fixture context behavior remains compatible. Every economic cutoff reads `intended_at`, while retrieval reads `now`.

- [ ] **Step 4: Run all scheduled suites**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_scheduled tests.integration.test_scheduled_authority_boundary tests.integration.test_scheduled_authority_adversarial tests.integration.test_scheduled_result_envelope_migration -v
```

Expected: PASS with durable deduplication, truthful stored failures, and no backfill.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/scheduled.py tests/integration/test_scheduled.py tests/integration/test_scheduled_authority_boundary.py tests/integration/test_scheduled_authority_adversarial.py
git commit -m "feat: allow bounded scheduler dispatch latency"
```

### Task 4: Install reviewed instrument metadata and a multi-subject evidence release

**Files:**
- Modify: `src/stock_monitor/universe.py`
- Modify: `src/stock_monitor/evidence.py`
- Create: `data/universe/2026-08-22.json`
- Preserve unchanged: `data/universe/2026-08-14.json`
- Modify: `data/evidence/current.json`
- Create: `data/evidence/legacy/subjectless.json`
- Create: `data/evidence/subjects/*.json`
- Create: `data/evidence/sources/*.json`
- Modify: `tests/unit/test_universe.py`
- Modify: `tests/unit/test_evidence.py`

- [ ] **Step 1: Write failing schema, coverage, and expiry tests**

```python
def test_stock_universe_requires_cik_and_initial_listing_date(self):
    record = self.stock_record(issuer_cik=None)
    with self.assertRaises(UniverseError):
        UniverseRecord.from_mapping(record)

def test_evidence_release_covers_exact_verified_universe(self):
    release = load_current_evidence_release(ROOT, as_of=NOW, universe=self.universe)
    self.assertEqual(set(release.by_symbol), set(self.universe.eligible_symbols))

def test_expired_child_or_hash_mismatch_blocks_entire_release(self):
    with self.assertRaises(EvidenceRegistryError):
        load_evidence_release(self.expired_or_tampered_manifest, as_of=NOW, universe=self.universe)
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_universe tests.unit.test_evidence -v
```

Expected: missing fields/load function and current subjectless bundle failures.

- [ ] **Step 3: Implement exact release types and loaders**

```python
@dataclass(frozen=True, slots=True)
class ReviewedEvidenceRelease:
    release_id: str
    release_sha256: str
    universe_sha256: str
    reviewed_at: datetime
    review_by: datetime
    by_symbol: Mapping[str, ReviewedEvidenceBundle]

def load_current_evidence_release(
    project_root: Path,
    *,
    as_of: datetime,
    universe: UniverseSnapshot,
    source_documents: Mapping[str, SourceDocument] | None = None,
) -> ReviewedEvidenceRelease:
    return load_evidence_release(
        project_root / "data" / "evidence" / "current.json",
        expected_sha256=CURRENT_EVIDENCE_RELEASE_SHA256,
        as_of=as_of,
        universe=universe,
        source_documents=source_documents,
    )
```

The top-level JSON has exact keys `schema_version`, `kind`, `release_id`, `universe_sha256`, `reviewed_at`, `review_by`, and `subjects`. Each subject entry has `symbol`, `subject_kind`, `issuer_cik`, `path`, and `sha256`. Reject absolute/traversal/symlink paths, duplicates, extra/missing universe subjects, CIK mismatch, stale review, and child hash mismatch. Canonical children are schema version 3 with explicit `coverage_start`/`coverage_end` and binding `published_at`; schema version 2 is accepted only for the empty named legacy seed and never as a current child. Add explicit `issuer_cik` plus primary-source provenance for stocks and explicit initial-listing date, date kind, and provenance for every stock and ETF. Publish those facts in a newly reviewed `2026-08-22.json` universe while preserving the historical 2026-08-14 release unchanged.

When `source_documents` is omitted, load exact raw bytes from strict content-addressed `data/evidence/sources/<content_sha256>.json` envelopes with exact keys `{schema_version, kind, content_sha256, encoding, body}`, `kind="RAW_SOURCE_ARTIFACT"`, and canonical strict base64 encoding. Confined no-follow reads, decoded-size checks, and decoded SHA-256 checks are mandatory. The initial reviewed NYSE operational-status response has no primary publication timestamp and therefore binds `published_at: null`, `timestamp_source: "UNAVAILABLE"`, and only incomplete `UNKNOWN` coverage. It must never be converted to `CONFIRMED_CLEAR` or authorize unattended action.

- [ ] **Step 4: Recompute release constants mechanically and run warning-strict tests**

```sh
shasum -a 256 data/universe/2026-08-22.json data/evidence/current.json data/evidence/legacy/subjectless.json data/evidence/subjects/*.json data/evidence/sources/*.json
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_universe tests.unit.test_evidence tests.integration.test_phase1_authorities -v
```

Expected: PASS; an as-of time at or after `review_by` fails closed.

- [ ] **Step 5: Commit code/schema separately from reviewed data**

```sh
git add src/stock_monitor/universe.py src/stock_monitor/evidence.py tests/unit/test_universe.py tests/unit/test_evidence.py
git commit -m "feat: load reviewed subject evidence releases"
git add data/universe/2026-08-22.json data/evidence/current.json data/evidence/legacy/subjectless.json data/evidence/subjects data/evidence/sources
git commit -m "data: pin reviewed subject evidence release"
```

### Task 5: Add explicit idempotent Phase 1 bootstrap

**Files:**
- Create: `src/stock_monitor/phase1_bootstrap.py`
- Modify: `src/stock_monitor/cli.py`
- Modify: `src/stock_monitor/journal.py`
- Modify: `tests/e2e/test_cli.py`
- Modify: `tests/integration/test_journal.py`

- [ ] **Step 1: Write failing CLI and idempotency tests**

```python
def test_phase1_start_requires_reviewed_open_session_and_reads_back(self):
    completed = self.run_cli("phase1", "start", "--session", "2026-08-21", "--json")
    self.assertEqual(completed.returncode, 0)
    self.assertEqual(json.loads(completed.stdout)["starting_capital"], "5000.00")

def test_phase1_start_is_idempotent_but_conflicting_window_fails(self):
    first = self.start("2026-08-21")
    second = self.start("2026-08-21")
    self.assertEqual(first["window_id"], second["window_id"])
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_cli tests.integration.test_journal -v
```

Expected: parser rejects `phase1 start`.

- [ ] **Step 3: Implement deterministic bootstrap**

```python
def _phase1_start(settings: Settings, session_text: str, as_json: bool) -> int:
    session_date = date.fromisoformat(session_text)
    calendar = load_current_market_calendar(settings.project_root, as_of=session_date)
    session = calendar.session(session_date)
    resolver = SessionCalendarResolver((calendar,))
    started_at = datetime.combine(session_date, session.close_time, tzinfo=calendar.timezone)
    with Journal.open(settings.journal_path) as journal:
        stored = bootstrap_phase1(
            journal,
            session_date=session_date,
            calendar_resolver=resolver,
            received_at=datetime.now(timezone.utc),
        )
    _emit(stored.safe_fields(), as_json)
    return 0

def bootstrap_phase1(
    journal: Journal,
    *,
    session_date: date,
    calendar_resolver: SessionCalendarResolver,
    received_at: datetime,
) -> StoredPhase1ValidationWindow:
    session = calendar_resolver.session(session_date)
    started_at = datetime.combine(session_date, session.close_time, tzinfo=session.timezone)
    calendar_digest = hashlib.sha256(
        json.dumps(
            {
                "session_date": session_date.isoformat(),
                "open_time": session.open_time.isoformat(),
                "close_time": session.close_time.isoformat(),
                "review_time": session.review_time.isoformat(),
                "timezone": str(session.timezone),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    window_id = phase1_validation_window_id(
        session_date=session_date,
        calendar_digest=calendar_digest,
        starting_capital_micros=5_000_000_000,
    )
    existing = journal.read_phase1_validation_window(window_id)
    if existing is not None:
        return existing
    if received_at < started_at.astimezone(timezone.utc):
        raise InvalidJournalValue("Phase 1 genesis session is not complete")
    return journal.start_phase1_validation_window(
        window_id=window_id,
        started_session=session_date,
        starting_capital=Decimal("5000.00"),
        started_at=started_at,
        received_at=received_at,
        calendar_resolver=calendar_resolver,
    )
```

`start_or_read_phase1_validation_window()` reads before writing so a retry with
a later `received_at` returns the identical stored row for exact deterministic
material. It rejects a future/unclosed genesis session and raises
`IdempotencyConflict` for a conflicting active singleton. Phase 1 publication
requires the genesis session to be earlier than the first signal session, so a
window started after the 2026-08-21 close can first publish on the next reviewed
open session, 2026-08-24.

- [ ] **Step 4: Run bootstrap and authority tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_cli tests.integration.test_journal tests.integration.test_phase1_authorities -v
```

Expected: PASS; no implicit premarket bootstrap exists.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/phase1_bootstrap.py src/stock_monitor/cli.py src/stock_monitor/journal.py tests/e2e/test_cli.py tests/integration/test_journal.py
git commit -m "feat: add explicit phase one bootstrap"
```

### Task 6: Add role-safe report projections

**Files:**
- Modify: `src/stock_monitor/reports.py`
- Modify: `src/stock_monitor/workflows.py`
- Modify: `tests/unit/test_report_precedence.py`
- Modify: `tests/e2e/test_golden_reports.py`

- [ ] **Step 1: Write failing shadow and unverified-position tests**

```python
def test_watchlist_shadow_has_no_sizing_fields(self):
    shadow = PremarketShadow(symbol="AMD", role="WATCHLIST_SHADOW", score=Decimal("84"), setup="PULLBACK", trigger=Decimal("100"))
    body = render_premarket_report(self.state(candidates=(shadow,))).body
    self.assertIn("N/A - WATCHLIST ONLY", body)

def test_unverified_close_position_never_fabricates_mark_stop_or_target(self):
    position = UnverifiedClosePosition("AAPL", 4, Decimal("225.10"), "POSITION_UNVERIFIED", ("PLAN_LINEAGE_UNAVAILABLE",))
    body = render_close_report(self.close_state(positions=(position,))).body
    self.assertNotIn("$0.00", body)
    self.assertIn("PLAN LINEAGE UNAVAILABLE", body)
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_report_precedence tests.e2e.test_golden_reports -v
```

Expected: missing projection types or current positive-field validation failure.

- [ ] **Step 3: Implement discriminated projections**

```python
@dataclass(frozen=True, slots=True)
class PremarketShadow:
    symbol: str
    role: Literal["WATCHLIST_SHADOW"]
    score: Decimal
    setup: str
    trigger: Decimal

@dataclass(frozen=True, slots=True)
class UnverifiedClosePosition:
    symbol: str
    shares: int
    exact_cost_basis: Decimal
    status: Literal["POSITION_UNVERIFIED", "STOP_UNVERIFIED", "DATA_UNAVAILABLE", "RECONCILIATION_REQUIRED"]
    reason_codes: tuple[str, ...]
```

Restrict `PremarketCandidate.role` to `PRIMARY`. Extend verified `ClosePosition` with `action` and `reason_codes`. Update `CandidateSummary.material`, `CloseSnapshot`, renderers, stable hashing, and golden outputs using role-dependent branches rather than nullable fake numbers.

- [ ] **Step 4: Run report/workflow tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.unit.test_report_precedence tests.e2e.test_golden_reports tests.e2e.test_recorded_scenarios -v
```

Expected: PASS and unchanged fixture authority.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/reports.py src/stock_monitor/workflows.py tests/unit/test_report_precedence.py tests/e2e/test_golden_reports.py
git commit -m "feat: add role-safe monitor report projections"
```

### Task 7: Add typed Journal receipts, actual lineage, and durable close recommendations

**Files:**
- Create: `src/stock_monitor/sql/005_provider_monitoring.sql`
- Modify: `src/stock_monitor/journal.py`
- Modify: `tests/integration/test_journal.py`
- Modify: `tests/integration/test_actual_transitions.py`
- Modify: `tests/integration/test_journal_migrations.py`

- [ ] **Step 1: Write failing receipt, lineage, and recommendation tests**

```python
def test_source_observation_receipt_is_owner_bound_and_read_back(self):
    receipt = journal.append_source_observation_receipt(**self.observation())
    self.assertEqual(receipt, journal.read_source_observation_receipt(receipt.row_id))
    self.assertTrue(journal.owns_source_observation_receipt(receipt))

def test_actual_lot_maps_to_exact_confirmation_and_phase1_signal(self):
    source = journal.read_actual_position_plan_source(symbol="AAPL", query_cutoff=NOW)
    self.assertEqual(source.signal_source.signal_id, self.signal_id)

def test_prior_close_recommendation_never_widens(self):
    journal.append_close_recommendation(self.recommendation("220.00"))
    with self.assertRaises(InvalidJournalValue):
        journal.append_close_recommendation(self.recommendation("219.99"))
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_journal tests.integration.test_actual_transitions tests.integration.test_journal_migrations -v
```

Expected: missing typed APIs and migration.

- [ ] **Step 3: Implement immutable Journal sources and schema**

```python
@dataclass(frozen=True, slots=True, weakref_slot=True)
class SourceObservationReceipt:
    row_id: int
    observation_sha256: str
    payload_sha256: str
    source_uri: str

@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualPositionPlanSource:
    symbol: str
    query_cutoff: datetime
    lot_source_cursors: tuple[int, ...]
    confirmation_execution_event_ids: tuple[int, ...]
    signal_source: Phase1SignalSource
    source_digest: str
```

Migration `005_provider_monitoring.sql` adds append-only
`canonical_report_contexts`, `actual_close_reviews`,
`actual_close_source_bindings`, and `close_recommendations` tables.
`canonical_report_contexts` binds workflow kind, fixed economic/review time,
actual retrieval time, and material digest to a report row in the same SQLite
transaction as report/pins/outbox finalization. Actual-close review/binding rows
pin the session, review/mark/query cutoffs, cohort roles, observation receipt
rows, and safe collection failures. Recommendations are keyed by
session/symbol/recommendation ID with stop micros, action, reasons JSON, source
digest, received time, and record hash. Journal selectors require exact
source-cursor-to-confirmation-to-lifecycle-to-signal uniqueness at
`query_cutoff`; zero/multiple matches return a typed unavailable/ambiguous
resolution used by the report layer.

- [ ] **Step 4: Run migration, Journal, reconciliation, and authority tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_journal_migrations tests.integration.test_journal tests.integration.test_actual_transitions tests.integration.test_reconciliation tests.integration.test_phase1_authorities -v
```

Expected: PASS with structural fingerprint invalidation after every write.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/sql/005_provider_monitoring.sql src/stock_monitor/journal.py tests/integration/test_journal.py tests/integration/test_actual_transitions.py tests/integration/test_journal_migrations.py
git commit -m "feat: add actual close journal authorities"
```

### Task 8: Issue exact source-backed actual-close marks and position contexts

**Files:**
- Modify: `src/stock_monitor/risk.py`
- Create: `tests/integration/test_actual_close_composition.py`
- Modify: `tests/unit/test_position_management.py`

- [ ] **Step 1: Write failing delayed-SIP and context tests**

```python
def test_actual_close_mark_separates_review_and_observation_times(self):
    mark = issue_actual_close_mark(
        self.source(review_at=et("15:30"), observed_at=et("15:14"), bid="225.10"),
        self.context(),
    )
    self.assertEqual(mark.at, et("15:30"))
    self.assertEqual(mark.price, Decimal("225.10"))

def test_iex_only_crossed_stale_or_conflicting_sip_fails_closed(self):
    for source in self.invalid_sources():
        with self.subTest(source=source), self.assertRaises(RiskBlock):
            issue_actual_close_mark(source, self.context())
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_actual_close_composition tests.unit.test_position_management -v
```

Expected: missing actual-close issuers; existing `build_market_mark()` rejects the delayed observation time.

- [ ] **Step 3: Implement public owner-bound actual-close material**

```python
@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualCloseMarketSource:
    symbol: str
    review_at: datetime
    observed_at: datetime
    sip_bid: Decimal
    session_low: Decimal
    iex_observed_at: datetime
    observation_receipts: tuple[SourceObservationReceipt, ...]
    source_digest: str

@dataclass(frozen=True, slots=True, weakref_slot=True)
class ActualPositionEventContext:
    journal_owner: str
    position: Position
    review_at: datetime
    observed_at: datetime
    previous_session_low: Decimal
    session_low: Decimal
    atr14: Decimal
    iex_age: timedelta
    event_exit_required: bool
    thesis_invalidated: bool
    actual_close_source_digest: str
    context_digest: str

def issue_actual_close_mark(source: ActualCloseMarketSource, context: ActualPositionEventContext) -> MarketMark:
    if source.symbol != context.position.symbol or source.review_at != context.review_at:
        raise RiskBlock("ACTUAL_CLOSE_SOURCE_MISMATCH")
    cutoff = source.review_at - timedelta(minutes=16)
    if not cutoff - timedelta(minutes=5) <= source.observed_at <= cutoff:
        raise RiskBlock("ACTUAL_CLOSE_SIP_STALE")
    if source.sip_bid <= 0 or context.iex_age > timedelta(minutes=5):
        raise RiskBlock("ACTUAL_CLOSE_MARK_UNAVAILABLE")
    return _issue_actual_market_mark(context, source.sip_bid, source.session_low)
```

The private registry binds source/context/mark to the exact Journal, final replay, plan lineage, policy/calendar digest, and receipt fingerprints. Weighted long entry is `cost_basis_micros / shares / 1_000_000`, rounded with `ROUND_CEILING` to six decimal places for risk only.

- [ ] **Step 4: Run actual-close, risk, and adversarial authority tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_actual_close_composition tests.unit.test_position_management tests.integration.test_risk_journal_adapter tests.integration.test_task7_source_authorities -v
```

Expected: PASS; copied/cross-Journal/stale sources are rejected.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/risk.py tests/integration/test_actual_close_composition.py tests/unit/test_position_management.py
git commit -m "feat: add actual close market authority"
```

### Task 9: Add separate canonical workflow and publication capabilities

**Files:**
- Create: `src/stock_monitor/provider_workflows.py`
- Modify: `src/stock_monitor/workflows.py`
- Modify: `src/stock_monitor/journal.py`
- Create: `tests/integration/test_canonical_publication.py`

- [ ] **Step 1: Write failing exact-capability tests**

```python
def test_canonical_publisher_accepts_only_exact_issued_material(self):
    material, result = self.issued_premarket_material()
    published = self.publisher.publish(kind="PREMARKET", session_date=DAY, generated_at=NOW, result=result, material=material)
    self.assertIsNotNone(published.report_id)
    with self.assertRaises(WorkflowError):
        self.publisher.publish(kind="PREMARKET", session_date=DAY, generated_at=NOW, result=result, material=replace(material))

def test_fixture_and_canonical_publishers_cannot_cross(self):
    with self.assertRaises(WorkflowError):
        self.canonical_publisher.publish(
            kind="PREMARKET",
            session_date=DAY,
            generated_at=NOW,
            result=self.fixture_result,
            material=self.fixture_material,
        )
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_canonical_publication -v
```

Expected: missing protocols/material/publisher.

- [ ] **Step 3: Implement explicit material transport and phased publication**

```python
class CanonicalWorkflowAdapter(Protocol):
    def premarket_material(self, session_date: date, *, decision_at: datetime, retrieved_at: datetime) -> CanonicalPremarketMaterial:
        pass

    def close_material(self, session_date: date, *, review_at: datetime, retrieved_at: datetime) -> CanonicalCloseMaterial:
        pass

class CanonicalWorkflowPublisher(Protocol):
    def publish(self, *, kind: str, session_date: date, generated_at: datetime, result: WorkflowResult, material: CanonicalPremarketMaterial | CanonicalCloseMaterial) -> PublishedWorkflow:
        pass

@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalPremarketMaterial:
    decision_at: datetime
    retrieved_at: datetime
    snapshot: PremarketSnapshot
    report: Report
    source_receipts: tuple[SourceObservationReceipt, ...]
    publication_decision: object | None
    primary_plan: object | None
    state_hash: str

@dataclass(frozen=True, slots=True, weakref_slot=True)
class CanonicalCloseMaterial:
    review_at: datetime
    retrieved_at: datetime
    query_cutoff: datetime
    actual_state: object
    report: Report
    positions: tuple[ClosePosition | UnverifiedClosePosition, ...]
    source_receipts: tuple[SourceObservationReceipt, ...]
    state_hash: str
```

The canonical publisher verifies exact registered
identity/fingerprint/Journal owner, claims the report, finalizes SQLite
report/pin/outbox plus canonical timing-context rows, writes and verifies the
archive, then for `PREMARKET` rereads stored kind `MORNING`. The publication
source carries fixed `economic_at=decision_at` independently of actual
retrieval/publication time. The publisher reissues portfolio/plan/decision at
that exact economic cutoff and idempotently calls `publish_phase1_report()`.
`ALREADY_FINALIZED` heals the exact archive and Phase 1 publication before
returning success. Existing fixture publisher code and registry remain
unchanged.

- [ ] **Step 4: Run canonical, fixture, archive, and publication authority tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_canonical_publication tests.integration.test_report_archive tests.integration.test_phase1_authorities tests.e2e.test_recorded_scenarios -v
```

Expected: PASS; crash-gap retries never claim success early.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/provider_workflows.py src/stock_monitor/workflows.py src/stock_monitor/journal.py tests/integration/test_canonical_publication.py
git commit -m "feat: add canonical workflow publication authority"
```

### Task 10: Compose canonical premarket material

**Files:**
- Modify: `src/stock_monitor/provider_workflows.py`
- Modify: `src/stock_monitor/workflows.py`
- Modify: `tests/integration/test_provider_workflows.py`
- Modify: `tests/e2e/test_provider_cli.py`

- [ ] **Step 1: Write failing complete/no-candidate/breaker/data tests**

```python
def test_premarket_fetches_complete_release_cohort_and_sizes_only_primary(self):
    material = self.adapter.premarket_material(DAY, decision_at=et("08:45"), retrieved_at=et("08:52"))
    self.assertEqual(self.alpaca.daily_bar_symbols, self.universe.all_required_symbols)
    self.assertEqual(sum(item.role == "PRIMARY" for item in material.snapshot.candidates), 1)
    self.assertTrue(all(item.material.shares > 0 for item in material.snapshot.candidates if item.role == "PRIMARY"))

def test_missing_subject_evidence_is_exit_three_not_no_trade(self):
    result = self.run_with_missing_evidence()
    self.assertEqual((result.outcome, result.exit_code, result.candidates), ("DATA_UNAVAILABLE", 3, ()))

def test_active_breaker_is_exit_four(self):
    result = self.run_with_breaker()
    self.assertEqual((result.exit_code, result.reason_codes), (4, ("ACTIVE_BREAKER",)))
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_provider_workflows tests.e2e.test_provider_cli -v
```

Expected: adapter method is not implemented.

- [ ] **Step 3: Implement collection, persistence, stable reissue, and ranking**

```python
def premarket_material(self, session_date: date, *, decision_at: datetime, retrieved_at: datetime) -> CanonicalPremarketMaterial:
    calendar, universe, evidence = self._reviewed_releases(session_date, decision_at)
    symbols = universe.required_market_data_symbols()
    smoke = self._require_ready_smoke(calendar, retrieved_at)
    bars = self.alpaca.daily_bars(symbols, self._sixty_session_window(calendar, session_date))
    quotes = self.alpaca.historical_quotes(symbols, self._previous_close_quote_window(calendar, session_date))
    iex = self.alpaca.latest_iex_quotes(symbols)
    halts = self._halt_snapshots(retrieved_at)
    receipts = self._persist_all(smoke, bars, quotes, iex, halts, calendar, universe, evidence)
    contexts = self._candidate_contexts(calendar, universe, evidence, bars, quotes, iex, halts, decision_at)
    ranked = rank_candidates(tuple(to_scored_candidate(score_candidate(item)) for item in build_base_eligible_cohort(contexts)))
    return self._issue_stable_premarket_material(ranked[:3], receipts, decision_at, retrieved_at)
```

After `_persist_all`, reread/reissue every Journal-bound authority. Plan only rank one. Use real reviewed local/source receipts for market-closed/no-candidate/data-failure states. Normalize provider/reference/evidence/native errors into safe `WorkflowDataError` reason codes.

- [ ] **Step 4: Run screening, risk, workflow, and canonical publication tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_provider_workflows tests.integration.test_canonical_publication tests.unit.test_eligibility tests.unit.test_scoring tests.unit.test_ranking tests.unit.test_position_sizing tests.e2e.test_provider_cli -v
```

Expected: PASS with exact 08:45 economic cutoff and zero candidates on every nonzero branch.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/provider_workflows.py src/stock_monitor/workflows.py tests/integration/test_provider_workflows.py tests/e2e/test_provider_cli.py
git commit -m "feat: compose canonical premarket reports"
```

### Task 11: Compose canonical actual-close material

**Files:**
- Modify: `src/stock_monitor/provider_workflows.py`
- Modify: `src/stock_monitor/workflows.py`
- Modify: `tests/integration/test_actual_close_composition.py`
- Modify: `tests/integration/test_provider_workflows.py`
- Modify: `tests/e2e/test_provider_cli.py`

- [ ] **Step 1: Write failing two-pass and mixed-precedence tests**

```python
def test_reconciliation_remains_dominant_when_provider_fails(self):
    result = self.run_close(reconciliation=True, quote_failure=True)
    self.assertEqual(result.outcome, "RECONCILIATION_REQUIRED")
    self.assertEqual(result.exit_code, 5)
    self.assertIn("SIP_MARK_UNAVAILABLE", result.reason_codes)

def test_one_missing_mark_dominates_valid_exit_but_preserves_both_positions(self):
    result = self.run_two_positions(first="DATA_UNAVAILABLE", second="EXIT")
    self.assertEqual((result.outcome, result.exit_code), ("DATA_UNAVAILABLE", 3))
    self.assertEqual(len(result.report.positions), 2)

def test_journal_write_forces_final_replay_and_lineage_reread(self):
    material = self.adapter.close_material(DAY, review_at=et("15:30"), retrieved_at=et("15:38"))
    self.assertGreater(self.journal.final_replay_generation, self.journal.discovery_generation)
    self.assertEqual(material.query_cutoff, self.invocation_cutoff)
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_actual_close_composition tests.integration.test_provider_workflows tests.e2e.test_provider_cli -v
```

Expected: close coordinator absent/current workflow hides reconciliation behind provider failure.

- [ ] **Step 3: Implement discovery, fetch, final stable read, and decisions**

```python
def close_material(self, session_date: date, *, review_at: datetime, retrieved_at: datetime) -> CanonicalCloseMaterial:
    query_cutoff = retrieved_at.astimezone(timezone.utc)
    discovery = self.journal.read_actual_replay(query_cutoff=query_cutoff)
    symbols = tuple(sorted(position.symbol for position in replay_actual(discovery).positions))
    fetched = self._collect_close_sources(symbols, review_at, retrieved_at)
    receipts = self._persist_all(*fetched)
    final_replay = self.journal.read_actual_replay(query_cutoff=query_cutoff)
    actual = replay_actual(final_replay)
    positions = tuple(self._close_position(position, final_replay, receipts, review_at) for position in actual.positions)
    state = aggregate_close_state(positions, reconciliation=actual.reconciliation_required)
    return self._issue_stable_close_material(actual, positions, state, receipts, query_cutoff, review_at, retrieved_at)
```

If any write or collection error occurs after discovery, re-read the final replay before choosing reconciliation precedence. Resolve plans/lifecycle/evidence/recommendations only after all writes. No positions plus verified releases/provider health returns `HOLD`; unresolved/unrelated exposure uses `UnverifiedClosePosition` and remains visible.

- [ ] **Step 4: Run close, reconciliation, report, and scheduler tests**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.integration.test_actual_close_composition tests.integration.test_provider_workflows tests.integration.test_reconciliation tests.unit.test_position_management tests.unit.test_report_precedence tests.integration.test_scheduled -v
```

Expected: PASS with aggregate precedence `RECONCILIATION_REQUIRED`, `POSITION_UNVERIFIED`, `STOP_UNVERIFIED`, `DATA_UNAVAILABLE`, `EXIT`, `TIGHTEN_STOP`, `HOLD`.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/provider_workflows.py src/stock_monitor/workflows.py tests/integration/test_actual_close_composition.py tests/integration/test_provider_workflows.py tests/e2e/test_provider_cli.py
git commit -m "feat: compose canonical actual close reports"
```

### Task 12: Wire canonical CLI and scheduled contexts without fallback

**Files:**
- Modify: `src/stock_monitor/cli.py`
- Modify: `src/stock_monitor/scheduled.py`
- Modify: `tests/e2e/test_provider_cli.py`
- Modify: `tests/e2e/test_cli.py`
- Modify: `tests/integration/test_scheduled.py`

- [ ] **Step 1: Write failing dispatch and timestamp tests**

```python
def test_no_fixture_selects_only_canonical_adapter(self):
    with patch("stock_monitor.cli.ProviderWorkflowAdapter.open", return_value=self.adapter):
        completed = self.run_cli("run", "premarket", "--json", now=et("08:50"))
    self.assertEqual(completed.returncode, 0)
    self.assertEqual(json.loads(completed.stdout)["execution_mode"], "CANONICAL")

def test_fixture_never_falls_back_to_provider_and_provider_never_falls_back_to_fixture(self):
    self.assertEqual(self.run_bad_fixture().returncode, 3)
    self.assertEqual(self.run_bad_provider().returncode, 3)

def test_verify_evidence_projects_only_safe_release_metadata(self):
    completed = self.run_cli("verify", "evidence", "--json", now=et("08:40"))
    payload = json.loads(completed.stdout)
    self.assertEqual(
        set(payload),
        {"status", "release_sha256", "universe_checksum", "reviewed_at", "review_by", "symbols"},
    )
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_provider_cli tests.e2e.test_cli tests.integration.test_scheduled -v
```

Expected: no-fixture path still exits `5`.

- [ ] **Step 3: Implement explicit dispatch**

```python
if arguments.fixture is not None:
    return _run_recorded_workflow(settings, arguments)
return _run_canonical_workflow(settings, arguments)

def _run_canonical_workflow(settings: Settings, arguments: argparse.Namespace) -> int:
    now = datetime.now(_ET)
    with Journal.open(settings.journal_path) as journal:
        adapter = ProviderWorkflowAdapter.open(settings, journal, now=now)
        context = CanonicalWorkflowContext(
            adapter=adapter,
            publisher=CanonicalJournalWorkflowPublisher(journal, settings.reports_root),
            scheduler=JournalScheduledRunStore(journal) if arguments.scheduled else None,
            now=now,
        )
        kind = RunKind.CLOSE if arguments.run_command == "close" else RunKind.PREMARKET
        if arguments.scheduled:
            result = run_canonical_scheduled(kind, now, context)
        elif kind is RunKind.CLOSE:
            result = run_canonical_close(context)
        else:
            result = run_canonical_premarket(context)
    _emit(result.safe_fields(), arguments.json)
    return result.exit_code
```

Add `verify evidence --json` as a read-only canonical preflight. It loads the
reviewed release through the same production loader, verifies exact universe
coverage and digests, and emits only status, release/universe digests,
reviewed/review-by timestamps, and sorted symbols. It must never fetch a
provider, expose source-document text, or mutate the Journal.

Manual canonical runs enforce the same due windows as scheduled runs but do not create durable scheduled claims. Fixture roots and canonical roots remain separate.

- [ ] **Step 4: Run CLI, scheduler, fixture, and canary suites**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_provider_cli tests.e2e.test_cli tests.e2e.test_recorded_scenarios tests.integration.test_scheduled tests.security.test_secret_redaction -v
```

Expected: PASS with exact exits and no raw exception/credential output.

- [ ] **Step 5: Commit**

```sh
git add src/stock_monitor/cli.py src/stock_monitor/scheduled.py tests/e2e/test_provider_cli.py tests/e2e/test_cli.py tests/integration/test_scheduled.py
git commit -m "feat: wire canonical provider workflows"
```

### Task 13: Update operations, prompts, and final offline acceptance

**Files:**
- Modify: `README.md`
- Modify: `.env.example`
- Modify: `docs/operations.md`
- Modify: `docs/scheduled-prompts.md`
- Modify: `tests/e2e/test_acceptance.py`
- Modify: `tests/architecture/test_brokerage_boundary.py`
- Modify: `tests/security/test_secret_redaction.py`

- [ ] **Step 1: Write failing documentation and boundary assertions**

```python
def test_operations_names_exact_activation_gates_and_wrapper(self):
    text = Path("docs/operations.md").read_text()
    self.assertIn("run_monitor_unattended.sh", text)
    self.assertIn("PHASE1_BLOCKED_SCHEDULED_SMOKE", text)
    self.assertIn("08:45-09:00", text)

def test_activation_contains_no_order_or_option_capability(self):
    findings = scan_project_boundaries(ROOT)
    self.assertEqual(findings, ())
```

- [ ] **Step 2: Run tests and verify RED**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.e2e.test_acceptance tests.architecture.test_brokerage_boundary tests.security.test_secret_redaction -v
```

Expected: old one-minute/source-based instructions fail assertions.

- [ ] **Step 3: Update exact operator commands and prompts**

Document this unattended command shape without credential values:

```text
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' \
  run premarket --scheduled --json
```

Document Phase 1 bootstrap, evidence repin expiry, interactive/wrapper smoke, manual due-window gates, exact schedule creation/readback rules, safe nonzero handling, and `PHASE1_BLOCKED_SCHEDULED_SMOKE`. Do not call the key read-only or data real-time consolidated.

- [ ] **Step 4: Run focused and full offline verification**

```sh
PYTHONPATH=src .venv/bin/python -W error -m unittest tests.architecture.test_brokerage_boundary tests.security.test_network_boundary tests.security.test_secret_redaction tests.contract.test_alpaca tests.contract.test_sec tests.contract.test_reference tests.unit.test_config tests.e2e.test_cli tests.e2e.test_acceptance tests.integration.test_scheduled tests.integration.test_scheduled_authority_boundary tests.integration.test_scheduled_authority_adversarial -v
PYTHONPATH=src .venv/bin/python -W error -m unittest discover -s tests -v
.venv/bin/python -m compileall -q src tests
git diff --check
```

Expected: all commands exit `0`. Record exact counts/durations; do not substitute the earlier 132-test baseline for final verification.

- [ ] **Step 5: Commit**

```sh
git add README.md .env.example docs/operations.md docs/scheduled-prompts.md tests/e2e/test_acceptance.py tests/architecture/test_brokerage_boundary.py tests/security/test_secret_redaction.py
git commit -m "docs: add provider monitoring operations"
```

### Task 14: Run live gates, create automations only if eligible, and publish

**Files:**
- No tracked source edits unless a verified defect is first reproduced with a failing test.
- Update: Linear project `Stock Monitor` status record.
- External: Codex automation records and GitHub remote.

- [ ] **Step 1: Verify branch/main state and secret containment without printing values**

```sh
test "$(stat -f '%Lp' '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/.env')" = "600"
git -C '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor' check-ignore -v .env .venv/ .stock-monitor/ reports/
git status --short --branch
git -C '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor' status --short --branch
```

Expected: `.env` mode `600`; every runtime path ignored; the feature branch and
main checkout are clean.

- [ ] **Step 2: Fast-forward verified code into local main and reinstall editable package**

```sh
git -C '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor' merge --ff-only codex/provider-backed-monitoring
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/.venv/bin/python' -m pip install -e '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor'
git -C '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor' status --short --branch
```

Expected: local main advances to the final offline-verified feature commit and
remains clean. If a live run reveals a code defect, reproduce it with a failing
test on a new `codex/` fix branch, merge that verified fix, and rerun every
affected gate; do not commit directly on main.

- [ ] **Step 3: Run exact canonical activation commands from stable main**

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify universe --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify evidence --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify calendar --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' provider smoke --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' phase1 start --session 2026-08-21 --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --json
```

Expected: universe/calendar/provider exit `0`; the reviewed local calendar
confirms 2026-08-21 is a completed open genesis session and 2026-08-24 is its
next open session; bootstrap readback is exact; premarket and close run inside
their due windows and archive canonical reports. If implementation has passed
those dates, stop before bootstrap, select a completed genesis and later first
signal session only through the reviewed calendar API, update this dated plan
execution note, and record that change. If timing/evidence/provider blocks
either manual workflow, record the exact safe blocker and skip all schedule
creation.

- [ ] **Step 4: Create and read back Codex heartbeats only after every gate passes**

Create weekday America/New_York heartbeats at 08:45, 12:30, and 15:30 using the exact prompts in `docs/scheduled-prompts.md`, project ID `64a23f97-1147-4df5-a85e-ab2227370ec1`, and the stable main-checkout wrapper. Immediately read each record back and compare name, schedule, timezone, project, enabled state, and prompt. Never place secrets in automation records.

Expected: three exact enabled records, or zero newly created records when any prerequisite failed. Status remains `PHASE1_BLOCKED_SCHEDULED_SMOKE` until one unattended premarket and close report receive external human review.

- [ ] **Step 5: Perform final scans, update Linear, configure remote, and push**

```sh
cd '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor'
if git grep -nE 'paper-api\.alpaca\.markets|api\.alpaca\.markets|robinhood\.(com|net)|/v2/orders' -- src config scripts .env.example; then exit 1; fi
if git grep -nE '(APCA_API_SECRET_KEY|APCA_API_KEY_ID)=[^<]' -- ':!docs/superpowers/plans/*'; then exit 1; fi
git log --format='%H' --all | while read commit; do if git grep -nE 'AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{20,}' "$commit" -- . ':!tests/fixtures'; then exit 1; fi; done
git diff --check
if git remote get-url origin >/dev/null 2>&1; then test "$(git remote get-url origin)" = 'https://github.com/jpbueno/stock-monitor.git'; else git remote add origin 'https://github.com/jpbueno/stock-monitor.git'; fi
git push -u origin main
git ls-remote --heads origin main
```

Expected: no executable/config prohibited-host hits, no assigned credentials, no token-pattern hits, and no whitespace errors. Review any documentation/test mention manually rather than deleting safe boundary assertions.
Before the push, record changed modules, exact offline/live results, automation
IDs or activation blocker, and follow-ups in the existing Stock Monitor Linear
status. Expected final state: clean local `main`; remote `main` resolves to the
same commit; no secret/runtime file is tracked.
