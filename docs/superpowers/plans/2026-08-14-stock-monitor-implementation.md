# Stock Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, read-only, risk-controlled stock/ETF decision-support routine that produces auditable premarket and close reports, records manual Robinhood confirmations, validates the strategy prospectively, and never places brokerage orders.

**Architecture:** A Python 3.11+ standard-library package keeps pure screening/risk logic separate from constrained read-only providers and SQLite persistence. Append-only observations and execution events drive separate canonical-paper and actual-live projections; deterministic workflows render immutable Markdown reports and a CLI supplies the boundary used by Codex scheduled tasks.

**Tech Stack:** Python standard library (`argparse`, `dataclasses`, `Decimal`, `zoneinfo`, `urllib`, `sqlite3`, `tomllib`, `json`, `csv`, `hashlib`), `unittest`, TOML/JSON configuration, SQLite, shell launcher, Codex scheduled tasks.

---

## Locked implementation defaults

These resolve the remaining mechanical choices without changing the approved design:

- Money uses `Decimal` in Python and integer microdollars in SQLite; SQLite `REAL` is prohibited for money.
- All timestamps are timezone-aware ISO-8601. Session decisions use `America/New_York`; provider timestamps are normalized to UTC.
- Every universe/contract record carries a positive `tick_size` from reviewed instrument metadata. Trigger, entry, stop, target, and option-premium rounding use that value; missing or conflicting increments block the instrument. No global tick is assumed.
- Relative-strength percentiles use deterministic midrank: `(count(lower) + 0.5 * count(equal)) / cohort_size * 100`.
- The intended event-exclusion window is the full ten-session maximum hold; earlier policy exits remain valid.
- Previous-session score quotes must be positive and timestamped from 15:55:00 through 16:00:00 ET. Live IEX observations are freshness-only and must be no more than five minutes old.
- Historical SIP requests end at least sixteen minutes before retrieval. Missing pagination blocks the entire cohort. A comparable-close disagreement above 0.50% blocks the symbol.
- Completed historical observations remain content-addressed, but every candidate-producing run must complete a current provider-health/entitlement check and a complete cohort fetch; stale cache never substitutes for a failed run. SEC/issuer evidence health is refreshed within 24 hours for finalists.
- Universe review age is at most 31 calendar days. The current-year calendar must be present, source-verified, and internally consistent.
- Close-review precedence is `RECONCILIATION REQUIRED`, `POSITION UNVERIFIED`, `STOP UNVERIFIED`, `PROVISIONAL EXIT`, `PROVISIONAL TIGHTEN STOP`, then `PROVISIONAL HOLD`.
- Weekly equity windows start Monday ET; monthly windows use calendar months ET. A non-losing close resets the consecutive-loss count. High-water marks update after each ordered close or valid end-of-session mark.
- The canonical paper portfolio uses its own simulated `$5,000`; a same-session Robinhood account check is required only before an actual live entry.
- A missing Phase 2 daily mark fails the current prospective window. Prior records remain append-only; a new window starts from the next explicitly recorded Phase 2 start event.
- Runtime exits are `0=report/no-op success`, `2=configuration`, `3=data/source unavailable`, `4=policy/risk block`, `5=reconciliation required`, and `10=unexpected internal error`.
- Report paths are `reports/YYYY/MM/DD/<kind>-<session-date>-<report-id-prefix>.md`; report IDs and content hashes are stable.
- Allowed outbound hosts are exact HTTPS hostnames from configuration. IP literals, HTTP, userinfo, non-GET requests, and unapproved redirects are rejected. Alpaca order hosts and every Robinhood host are permanently absent.
- User replies enter through `stock-monitor confirm --message-id <ID> --message-time <ISO-8601> --text <MESSAGE>`. A repeated stable message ID is idempotent. Ambiguous text is retained without quantity mutation.

## File map

```text
pyproject.toml
README.md
.env.example
config/policy.toml
config/sources.toml
config/fees.json
data/universe/2026-08-14.json
data/calendars/2026.json
data/evidence/current.json
docs/operations.md
scripts/run_monitor.sh

src/stock_monitor/__init__.py
src/stock_monitor/__main__.py
src/stock_monitor/cli.py
src/stock_monitor/config.py
src/stock_monitor/domain.py
src/stock_monitor/policy.py
src/stock_monitor/market_calendar.py
src/stock_monitor/universe.py
src/stock_monitor/evidence.py
src/stock_monitor/indicators.py
src/stock_monitor/screening.py
src/stock_monitor/risk.py
src/stock_monitor/ledger.py
src/stock_monitor/confirmations.py
src/stock_monitor/reconciliation.py
src/stock_monitor/phase1.py
src/stock_monitor/validation.py
src/stock_monitor/replay.py
src/stock_monitor/options_paper.py
src/stock_monitor/journal.py
src/stock_monitor/exports.py
src/stock_monitor/reports.py
src/stock_monitor/workflows.py
src/stock_monitor/scheduled.py
src/stock_monitor/providers/__init__.py
src/stock_monitor/providers/http.py
src/stock_monitor/providers/cache.py
src/stock_monitor/providers/alpaca.py
src/stock_monitor/providers/sec.py
src/stock_monitor/providers/reference.py
src/stock_monitor/sql/001_core.sql
src/stock_monitor/sql/002_phase1.sql
src/stock_monitor/sql/003_phase2_paper.sql

tests/architecture/
tests/unit/
tests/integration/
tests/contract/
tests/e2e/
tests/security/
tests/fixtures/
```

### Test-support and fixture ownership

`tests/support.py` is created in Task 1 with only deterministic JSON loading, temporary-directory/environment helpers, timezone-aware timestamp construction, and a subprocess CLI runner. Each later task extends it in the same commit as the first test that uses the helper:

| Task | Helpers added | Exact fixture roots created |
|---|---|---|
| 1 | `policy_fixture` plus the initial helpers below | none |
| 2 | `calendar_fixture`, `universe_fixture` | `tests/fixtures/reference/` |
| 4 | `FixtureTransport`, `credentials`, `reference_fixture` | `tests/fixtures/providers/alpaca/`, `tests/fixtures/providers/reference/`, `tests/fixtures/providers/sec/`, `tests/fixtures/evidence/` |
| 5 | `fixture_bars`, `candidate_fixture` | `tests/fixtures/market/bars/`, `tests/fixtures/market/quotes/` |
| 6 | `seeded_ledgers`, `account_check`, `buy_event`, `cash_adjustment` | `tests/fixtures/portfolios/` |
| 7 | `journal_with_signal` | `tests/fixtures/messages/` |
| 8 | `trade`, `quote`, `validation_fixture` | `tests/fixtures/intraday/` |
| 9 | `option_chain_fixture`, `signal_fixture`, `option_window_fixture` | `tests/fixtures/options/`, `tests/fixtures/replay/` |
| 10 | `close_state`, `archived_text` | `tests/fixtures/golden_reports/` |
| 11 | `run_cli`, `run_close_wake` | `tests/fixtures/scenarios/` |
| 12 | `run_cli_with_canary_secrets`, `run_recorded_acceptance_matrix` | uses the immutable Task 11 scenarios |

The initial support module is concrete and dependency-free:

```python
FIXTURE_ROOT = Path(__file__).parent / "fixtures"

def load_json(relative: str) -> object:
    return json.loads((FIXTURE_ROOT / relative).read_text(encoding="utf-8"))

def aware_et(session_date: date, hhmm: str) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    return datetime.combine(session_date, time(hour, minute), ZoneInfo("America/New_York"))

def isolated_env(root: Path, overrides: Mapping[str, str] | None = None) -> dict[str, str]:
    result = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(Path.cwd() / "src"), "STOCK_MONITOR_HOME": str(root)}
    result.update(overrides or {})
    return result
```

### Required production interfaces

Names and signatures are fixed so later tasks and tests do not invent incompatible APIs:

| Owner | Required interface |
|---|---|
| Task 1 | `load_settings(project_root: Path, environ: Mapping[str, str]) -> Settings`; `money_to_micros(value: Decimal) -> int`; `money_from_micros(value: int) -> Decimal`; `Policy.from_toml(path: Path) -> Policy`; `Policy.validate() -> None` |
| Task 2 | `MarketCalendar.load(path: Path) -> MarketCalendar`; `session(day: date) -> MarketSession`; `is_open(day: date) -> bool`; `add_sessions(start: date, count: int) -> date`; `UniverseSnapshot.load(path: Path, as_of: date) -> UniverseSnapshot`; `eligible_records() -> tuple[UniverseRecord, ...]` |
| Task 3 | `Journal.open(path: Path) -> Journal`; `transaction() -> ContextManager[JournalTransaction]`; `migrate() -> None`; `append_raw_message(message_id: str, message_time: datetime, text: str) -> tuple[int, bool]`; `claim_report(session_date: date, kind: str) -> ReportClaim`; `count(table: str) -> int` |
| Task 4 | `EgressPolicy.validate_get(url: str) -> None`; `HttpGetClient.get(url: str, headers: Mapping[str, str]) -> HttpResponse`; `ContentCache.put(observation: SourceObservation, payload: bytes) -> str`; `AlpacaMarketData.daily_bars(symbols: Sequence[str], window: TimeWindow) -> Mapping[str, tuple[Bar, ...]]`; `historical_quotes(symbols: Sequence[str], window: TimeWindow) -> Mapping[str, tuple[Quote, ...]]`; `latest_iex_quotes(symbols: Sequence[str]) -> Mapping[str, Quote]`; `option_chain(underlying: str) -> tuple[OptionSnapshot, ...]`; `smoke() -> EntitlementSmoke`; `SecClient.get_submission(cik: str) -> SourceDocument`; `get_archive(path: str) -> SourceDocument`; `ReferenceClient.fetch(url: str) -> SourceDocument`; `classify_evidence(records: Sequence[EvidenceRecord], hold: DateRange) -> EvidenceDecision` |
| Task 5 | `sma(values: Sequence[Decimal], period: int) -> Decimal`; `ema(values: Sequence[Decimal], period: int) -> Decimal`; `wilder_atr(bars: Sequence[Bar], period: int) -> Decimal`; `evaluate_eligibility(context: CandidateContext) -> EligibilityDecision`; `detect_setup(context: CandidateContext) -> SetupDecision`; `midrank_percentile(value: Decimal, cohort: Sequence[Decimal]) -> Decimal`; `score_candidate(context: CandidateContext) -> ScoreCard`; `rank_candidates(candidates: Sequence[ScoredCandidate]) -> tuple[ScoredCandidate, ...]` |
| Task 6 | `size_long(entry: Decimal, stop: Decimal, settled_cash: Decimal, deployed: Decimal, open_risk: Decimal) -> PositionPlan`; `evaluate_position(position: Position, mark: MarketMark, policy: Policy) -> PositionAction`; `account_check_eligible(check: AccountCheck, buy: ExecutionEvent, intervening_events: Sequence[ExecutionEvent]) -> bool`; `evaluate_breakers(equity: Sequence[EquityPoint], closes: Sequence[ClosedTrade], calendar: MarketCalendar) -> BreakerState`; `LedgerPair.record_actual_buy(signal_id: str, price: Decimal, shares: int, at: datetime) -> ComplianceDecision`; `LedgerPair.record_canonical_fill(signal_id: str, price: Decimal, shares: int, at: datetime) -> None` |
| Task 7 | `parse_confirmation(text: str, session_date: date) -> ParsedConfirmation`; `parse_confirmation_or_pending(text: str, session_date: date) -> ParsedConfirmation | PendingConfirmation`; `ingest_confirmation(journal: Journal, message_id: str, message_time: datetime, text: str, session_date: date) -> IngestionResult` |
| Task 8 | `simulate_entry(trigger: Decimal, limit: Decimal, observations: Sequence[IntradayObservation]) -> PaperEntryResult`; `advance_signal(signal: Signal, event: SignalEvent) -> Signal`; `mark_equity(cash: Decimal, positions: Sequence[PaperPosition], marks: Mapping[str, MarketMark]) -> EquityPoint`; `evaluate_phase1(window: Phase1Window) -> PromotionDecision` |
| Task 9 | `replay_diagnostic(request: ReplayRequest) -> ReplayResult`; `replay_point_in_time(request: ReplayRequest) -> ReplayResult`; `rank_option_contracts(chain: Sequence[OptionContract], signal: Signal) -> tuple[OptionContract, ...]`; `record_option_mark(window: OptionWindow, mark: OptionMark) -> OptionWindow`; `evaluate_option_window(window: OptionWindow) -> OptionPromotionDecision`; `start_next_window(window: OptionWindow, start_event: OptionWindowStart | None) -> OptionWindow` |
| Task 10 | `render_premarket_report(state: PremarketState) -> Report`; `render_close_report(state: CloseState) -> Report`; `render_validation_report(state: ValidationState) -> Report`; `archive_report(report: Report, root: Path) -> ArchivedReport`; `export_tables(journal: Journal, destination: Path) -> tuple[Path, ...]` |
| Task 11 | `run_premarket(context: WorkflowContext) -> WorkflowResult`; `run_close(context: WorkflowContext) -> WorkflowResult`; `run_scheduled(kind: RunKind, now: datetime, context: WorkflowContext) -> WorkflowResult`; `build_parser() -> argparse.ArgumentParser`; `main(argv: Sequence[str] | None = None) -> int` |

Fixture JSON uses one canonical schema per record: bars `{symbol,t,o,h,l,c,v}`, quotes `{symbol,t,bp,ap,feed,sequence}`, trades `{symbol,t,p,feed,sequence}`, source documents `{url,published_at,retrieved_at,sha256,body}`, universe records `{symbol,product_type,benchmark,sector_etf,membership_sources,source_url,reviewed_at,free_float,float_source,tick_size}`, option contracts `{occ_symbol,expiration,strike,delta,bid,ask,open_interest,daily_volume,tick_size}`, and scenario manifests `{now,account,provider_fixture,evidence_fixture,expected_outcome}`. Decimal values are JSON strings; timestamps are ISO-8601 strings.

## Task 1: Bootstrap, configuration, domain, and policy

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `config/policy.toml`
- Create: `config/sources.toml`
- Create: `config/fees.json`
- Create: `src/stock_monitor/__init__.py`
- Create: `src/stock_monitor/__main__.py`
- Create: `src/stock_monitor/config.py`
- Create: `src/stock_monitor/domain.py`
- Create: `src/stock_monitor/policy.py`
- Create: `tests/support.py`
- Test: `tests/architecture/test_brokerage_boundary.py`
- Test: `tests/unit/test_config.py`
- Test: `tests/unit/test_domain.py`
- Test: `tests/unit/test_policy.py`

- [ ] **Step 1: Write the failing bootstrap and boundary tests**

```python
class BrokerageBoundaryTests(unittest.TestCase):
    def test_runtime_has_no_third_party_dependencies(self):
        data = tomllib.loads(Path("pyproject.toml").read_text())
        self.assertEqual(data["project"]["dependencies"], [])

    def test_source_configuration_excludes_order_and_robinhood_hosts(self):
        text = Path("config/sources.toml").read_text().lower()
        self.assertIn("data.alpaca.markets", text)
        self.assertNotIn("paper-api.alpaca.markets", text)
        self.assertNotIn("api.alpaca.markets", text)
        self.assertNotIn("robinhood", text)

class MoneyTests(unittest.TestCase):
    def test_microdollar_round_trip_is_exact(self):
        self.assertEqual(money_from_micros(money_to_micros(Decimal("25.01"))), Decimal("25.01"))

class TamperedPolicyTests(unittest.TestCase):
    def test_every_fixed_safety_limit_rejects_tampering(self):
        overrides = {
            "max_live_exposure": "1001", "max_position_risk": "26", "max_combined_risk": "51",
            "max_positions": 3, "max_entries_per_session": 2, "min_score": 79,
            "max_weekly_drawdown": "101", "max_monthly_drawdown": "251",
        }
        for name, value in overrides.items():
            with self.subTest(name=name), self.assertRaises(ConfigurationError):
                policy_fixture(**{name: value}).validate()
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.architecture.test_brokerage_boundary tests.unit.test_config tests.unit.test_domain tests.unit.test_policy -v`

Expected: import/file failures because the package and configuration do not exist.

- [ ] **Step 3: Implement the package boundary and validated policy**

```python
@dataclass(frozen=True)
class Policy:
    capital: Decimal
    max_live_exposure: Decimal
    max_position_risk: Decimal
    max_combined_risk: Decimal
    max_positions: int
    max_entries_per_session: int
    min_score: int
    max_monthly_drawdown: Decimal
    max_weekly_drawdown: Decimal
    universe_max_age_days: int
    live_quote_max_age_seconds: int
    disagreement_tolerance: Decimal

    def validate(self) -> None:
        if self.capital != Decimal("5000"):
            raise ConfigurationError("capital must remain 5000 during validation")
        if self.max_position_risk != Decimal("25") or self.max_combined_risk != Decimal("50"):
            raise ConfigurationError("validation risk caps are immutable")
        if self.max_live_exposure != Decimal("1000") or self.max_monthly_drawdown != Decimal("250"):
            raise ConfigurationError("validation exposure/drawdown caps are immutable")
        if self.max_positions != 2 or self.max_entries_per_session != 1 or self.min_score != 80:
            raise ConfigurationError("position, entry, and score gates are immutable")
        if self.max_weekly_drawdown != Decimal("100"):
            raise ConfigurationError("weekly drawdown gate is immutable")
```

`load_settings()` must read non-secret TOML from the project, read `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `SEC_USER_AGENT`, and the non-secret test/operator path override `STOCK_MONITOR_HOME` from the environment, never include secret values in `repr`, and create runtime paths only under the resolved `.stock-monitor/` root and `reports/`.

- [ ] **Step 4: Run Task 1 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.architecture.test_brokerage_boundary tests.unit.test_config tests.unit.test_domain tests.unit.test_policy -v`

Expected: all Task 1 tests pass with no warnings.

- [ ] **Step 5: Commit Task 1**

```bash
git add pyproject.toml .env.example config src/stock_monitor tests/support.py tests/architecture tests/unit
git commit -m "feat: establish stock monitor safety boundary"
```

## Task 2: Versioned market calendar and universe

**Files:**
- Create: `data/calendars/2026.json`
- Create: `data/universe/2026-08-14.json`
- Create: `src/stock_monitor/market_calendar.py`
- Create: `src/stock_monitor/universe.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_market_calendar.py`
- Test: `tests/unit/test_universe.py`

- [ ] **Step 1: Write failing calendar and universe tests**

```python
class CalendarTests(unittest.TestCase):
    def test_thanksgiving_early_close_routes_to_1230(self):
        calendar = MarketCalendar.load(Path("data/calendars/2026.json"))
        session = calendar.session(date(2026, 11, 27))
        self.assertEqual(session.close_time, time(13, 0))
        self.assertEqual(session.review_time, time(12, 30))

    def test_t_plus_one_skips_weekend(self):
        calendar = MarketCalendar.load(Path("data/calendars/2026.json"))
        self.assertEqual(calendar.add_sessions(date(2026, 8, 14), 1), date(2026, 8, 17))

class UniverseTests(unittest.TestCase):
    def test_corrupt_checksum_fails_closed(self):
        raw = json.loads(Path("tests/fixtures/reference/corrupt-universe.json").read_text())
        with self.assertRaises(UniverseError):
            UniverseSnapshot.from_mapping(raw, as_of=date(2026, 8, 14))
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_market_calendar tests.unit.test_universe -v`

Expected: missing-module failures.

- [ ] **Step 3: Implement exact session and manifest rules**

```python
def add_sessions(self, start: date, count: int) -> date:
    if count < 0:
        raise CalendarError("negative settlement offsets are unsupported")
    current = start
    remaining = count
    while remaining:
        current += timedelta(days=1)
        if self.is_open(current):
            remaining -= 1
    return current
```

The calendar manifest must carry primary NYSE and Nasdaq cross-check URLs, retrieval date, year, closures, early closes, and manual-disable dates. The universe manifest must carry effective/review dates, acquisition method, canonical payload checksum, benchmark/sector mapping, product type, leverage/inverse flags, positive reviewed tick size, issuer source URL, and stock free-float source/value when applicable. Seed a manually reviewed internal-use subset containing both S&P 500/Nasdaq-100 stocks and approved broad/sector ETFs. Every stock record requires primary index-membership evidence plus a primary SEC/issuer free-float derivation and remains blocked if either is unavailable; every ETF record requires its official sponsor source. The loader rejects missing/conflicting provenance, float, mapping, or tick metadata.

- [ ] **Step 4: Run Task 2 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_market_calendar tests.unit.test_universe -v`

Expected: all Task 2 tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add data/calendars data/universe src/stock_monitor/market_calendar.py src/stock_monitor/universe.py tests/support.py tests/unit/test_market_calendar.py tests/unit/test_universe.py tests/fixtures/reference
git commit -m "feat: validate calendar and trading universe"
```

## Task 3: Transactional journal and audit schema

**Files:**
- Create: `src/stock_monitor/journal.py`
- Create: `src/stock_monitor/sql/001_core.sql`
- Test: `tests/integration/test_journal.py`
- Test: `tests/integration/test_journal_migrations.py`

- [ ] **Step 1: Write failing append-only/idempotency tests**

```python
class JournalTests(unittest.TestCase):
    def test_duplicate_message_id_does_not_duplicate_event(self):
        journal = Journal.open(self.db_path)
        at = datetime(2026, 8, 14, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        first = journal.append_raw_message("msg-1", at, "SKIPPED SPY")
        second = journal.append_raw_message("msg-1", at, "SKIPPED SPY")
        self.assertEqual(first, second)
        self.assertEqual(journal.count("raw_messages"), 1)

    def test_money_columns_are_integer_backed(self):
        journal = Journal.open(self.db_path)
        columns = journal.table_info("execution_events")
        self.assertEqual(columns["price_micros"].upper(), "INTEGER")
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_journal tests.integration.test_journal_migrations -v`

Expected: missing journal/schema failures.

- [ ] **Step 3: Implement migrations and transactions**

```python
@contextmanager
def immediate_transaction(connection: sqlite3.Connection):
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
```

Enable foreign keys, WAL, and a bounded busy timeout. The core migration stores append-only raw messages, source observations, generic execution events, account checks, scheduled runs, reports, and outbox rows. Task 8 adds Phase 1 signal/position tables in migration 002; Task 9 adds option-paper tables in migration 003. Projections may update transactionally, but source/event rows may never be updated or deleted. Stable uniqueness constraints must cover message IDs, source observation hashes, session report keys, and event ordinals.

- [ ] **Step 4: Run Task 3 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_journal tests.integration.test_journal_migrations -v`

Expected: all Task 3 tests pass and migration replay is idempotent.

- [ ] **Step 5: Commit Task 3**

```bash
git add src/stock_monitor/journal.py src/stock_monitor/sql/001_core.sql tests/integration/test_journal.py tests/integration/test_journal_migrations.py
git commit -m "feat: add append-only audit journal"
```

## Task 4: Constrained HTTP, cache, Alpaca, SEC, and evidence

**Files:**
- Create: `data/evidence/current.json`
- Create: `src/stock_monitor/providers/__init__.py`
- Create: `src/stock_monitor/providers/http.py`
- Create: `src/stock_monitor/providers/cache.py`
- Create: `src/stock_monitor/providers/alpaca.py`
- Create: `src/stock_monitor/providers/sec.py`
- Create: `src/stock_monitor/providers/reference.py`
- Create: `src/stock_monitor/evidence.py`
- Modify: `tests/support.py`
- Test: `tests/security/test_network_boundary.py`
- Test: `tests/contract/test_alpaca.py`
- Test: `tests/contract/test_sec.py`
- Test: `tests/contract/test_reference.py`
- Test: `tests/unit/test_evidence.py`

- [ ] **Step 1: Write failing provider/security contract tests**

```python
class NetworkBoundaryTests(unittest.TestCase):
    def test_order_host_and_ip_literal_are_rejected(self):
        policy = EgressPolicy({"data.alpaca.markets", "www.sec.gov", "data.sec.gov"})
        for url in ("https://paper-api.alpaca.markets/v2/orders", "https://127.0.0.1/data"):
            with self.subTest(url=url), self.assertRaises(NetworkPolicyError):
                policy.validate_get(url)

class AlpacaContractTests(unittest.TestCase):
    def test_incomplete_pagination_rejects_entire_cohort(self):
        transport = FixtureTransport("tests/fixtures/providers/alpaca/partial-page.json")
        window = TimeWindow(datetime(2026, 5, 1, tzinfo=UTC), datetime(2026, 8, 13, tzinfo=UTC))
        with self.assertRaises(ProviderIncompleteError):
            AlpacaMarketData(transport, credentials()).daily_bars(["SPY", "QQQ"], window)

class ReferenceContractTests(unittest.TestCase):
    def test_cross_host_redirect_is_rejected_before_second_request(self):
        transport = FixtureTransport("tests/fixtures/providers/reference/cross-host-redirect.json")
        client = ReferenceClient(transport, EgressPolicy({"www.nyse.com"}))
        with self.assertRaises(NetworkPolicyError):
            client.fetch("https://www.nyse.com/trade/hours-calendars")
        self.assertEqual(transport.requested_urls, ["https://www.nyse.com/trade/hours-calendars"])

    def test_uncertain_emergency_status_blocks_session(self):
        result = verify_exchange_status(reference_fixture("emergency-status-unknown.json"))
        self.assertEqual(result.block_reason, "EMERGENCY_STATUS_UNCERTAIN")
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.security.test_network_boundary tests.contract.test_alpaca tests.contract.test_sec tests.contract.test_reference tests.unit.test_evidence -v`

Expected: missing provider/evidence failures.

- [ ] **Step 3: Implement read-only transports and source policy**

```python
def validate_get(self, url: str) -> None:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise NetworkPolicyError("only credential-free HTTPS GET URLs on port 443 are allowed")
    try:
        ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        pass
    else:
        raise NetworkPolicyError("IP literals are prohibited")
    if parsed.hostname not in self.allowed_hosts:
        raise NetworkPolicyError(f"host is not allowlisted: {parsed.hostname}")

class NoAutomaticRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
```

The transport installs `NoAutomaticRedirects`. For a 301/302/303/307/308 response, it resolves `Location`, validates the new URL with the same egress policy, strips all authorization headers, permits at most three redirects, and then performs a fresh GET. The Alpaca client must implement paginated `v2/stocks/bars`, `v2/stocks/quotes`, IEX latest freshness, and indicative `v1beta1/options/snapshots/{underlying}` reads. It must label feed/timestamps, request adjusted bars, and expose a smoke result that separately proves authentication, historical SIP entitlement, and IEX freshness without calling a trading host. The SEC client must use the configured identifying User-Agent, content cache, Archives for filing documents, and a cross-process lock/rate file capped at ten requests per second. `ReferenceClient` retrieves only configured issuer IR, NYSE/Nasdaq calendar, exchange operational-status, Trader Alert, and corporate-action URLs; it records timestamps/hashes, requires the primary/cross-check result, and returns an explicit uncertainty block on missing/conflicting/emergency state. Evidence classification stores primary URL, fact, timestamp, event type, hash, conflicts, and adverse/ambiguous state.

- [ ] **Step 4: Run Task 4 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.security.test_network_boundary tests.contract.test_alpaca tests.contract.test_sec tests.contract.test_reference tests.unit.test_evidence -v`

Expected: all provider/security/evidence fixture tests pass without network access.

- [ ] **Step 5: Commit Task 4**

```bash
git add data/evidence src/stock_monitor/providers src/stock_monitor/evidence.py tests/support.py tests/security tests/contract tests/unit/test_evidence.py tests/fixtures/providers tests/fixtures/evidence
git commit -m "feat: add constrained read-only data providers"
```

## Task 5: Indicators, eligibility, setup scoring, and deterministic ranking

**Files:**
- Create: `src/stock_monitor/indicators.py`
- Create: `src/stock_monitor/screening.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_indicators.py`
- Test: `tests/unit/test_eligibility.py`
- Test: `tests/unit/test_setups.py`
- Test: `tests/unit/test_scoring.py`
- Test: `tests/unit/test_ranking.py`

- [ ] **Step 1: Write failing formula and boundary tests**

```python
class IndicatorTests(unittest.TestCase):
    def test_wilder_atr_matches_hand_calculation(self):
        bars = fixture_bars("tests/fixtures/market/bars/atr-hand-calculated.json")
        self.assertEqual(wilder_atr(bars, 14), Decimal("2.187643"))

class ScoringTests(unittest.TestCase):
    def test_score_79_is_rejected_and_80_is_publishable(self):
        low = score_candidate(candidate_fixture(overrides={"catalyst_points": 0}))
        high = score_candidate(candidate_fixture(overrides={"catalyst_points": 5}))
        self.assertEqual(low.total, 79)
        self.assertFalse(low.publishable)
        self.assertEqual(high.total, 84)
        self.assertTrue(high.publishable)

    def test_midrank_percentile_is_deterministic_for_ties(self):
        self.assertEqual(midrank_percentile(Decimal("2"), [Decimal("1"), Decimal("2"), Decimal("2"), Decimal("4")]), Decimal("50"))
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_indicators tests.unit.test_eligibility tests.unit.test_setups tests.unit.test_scoring tests.unit.test_ranking -v`

Expected: missing analytics modules.

- [ ] **Step 3: Implement pure calculations from the design**

```python
def midrank_percentile(value: Decimal, cohort: Sequence[Decimal]) -> Decimal:
    if not cohort:
        raise ScreeningError("percentile cohort is empty")
    lower = sum(item < value for item in cohort)
    equal = sum(item == value for item in cohort)
    return (Decimal(lower) + Decimal(equal) / Decimal(2)) / Decimal(len(cohort)) * Decimal(100)

def rank_candidates(candidates: Sequence[ScoredCandidate]) -> tuple[ScoredCandidate, ...]:
    return tuple(sorted(candidates, key=lambda item: (-item.total_score, -item.relative_strength_percentile, -item.average_dollar_volume, item.symbol)))
```

Complete Step 3 as these independent RED/GREEN cycles; run the named module after each RED and again after its minimal production change:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 5A | `test_sma_ema_and_wilder_atr_hand_values`, insufficient-history rejection, split-adjusted-only rejection | Add `sma`, `ema`, `wilder_atr`, five/twenty-session returns, and volume aggregates in `indicators.py` using `Decimal` only |
| 5B | Boundary cases for price `$10`, dollar volume `$100M`, share volume `1M`, stock float `50M`, IPO age 90, ETF exemption, product flags, event overlap/unknown, 0.25% spread, rumor, and dual-index pause | Add `evaluate_eligibility`; return all reason codes and never short-circuit the audit list |
| 5C | Exact qualifying/nonqualifying pullback and breakout series, valid tick rounding, nonpositive stop distance, and no-lookahead mutation | Add `detect_setup`; slice only through completed session `t` and round with the record's `tick_size` |
| 5D | Every point boundary for six categories, catalyst days 10/11/30/31, volume groups, score 79/80, SPY/VTI/sector benchmarks, and tied midranks | Add the two-pass cohort builder plus `score_candidate`; use category caps `25/20/20/15/10/10` and reject incomplete cohorts |
| 5E | Four-way ordering ties, at most three results, one primary, two shadows, no secondary substitution, and no-capacity outcome | Add `rank_candidates` and `select_publication_roles` with key `(-score, -rs_percentile, -dollar_volume, symbol)` |

The exact per-cycle commands are `PYTHONPATH=src python3 -m unittest tests.unit.test_indicators -v`, then `test_eligibility`, `test_setups`, `test_scoring`, and `test_ranking`. Each new test must fail for the named missing behavior before its production function is written.

- [ ] **Step 4: Run Task 5 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_indicators tests.unit.test_eligibility tests.unit.test_setups tests.unit.test_scoring tests.unit.test_ranking -v`

Expected: all Task 5 tests pass.

- [ ] **Step 5: Commit Task 5**

```bash
git add src/stock_monitor/indicators.py src/stock_monitor/screening.py tests/support.py tests/unit/test_indicators.py tests/unit/test_eligibility.py tests/unit/test_setups.py tests/unit/test_scoring.py tests/unit/test_ranking.py tests/fixtures/market
git commit -m "feat: score and rank qualified candidates"
```

## Task 6: Risk engine, settlement, circuit breakers, and separate ledgers

**Files:**
- Create: `src/stock_monitor/risk.py`
- Create: `src/stock_monitor/ledger.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_position_sizing.py`
- Test: `tests/unit/test_settlement.py`
- Test: `tests/unit/test_position_management.py`
- Test: `tests/unit/test_circuit_breakers.py`
- Test: `tests/integration/test_separate_ledgers.py`

- [ ] **Step 1: Write failing sizing and ledger-separation tests**

```python
class SizingTests(unittest.TestCase):
    def test_quantity_respects_both_exposure_and_risk(self):
        plan = size_long(entry=Decimal("100"), stop=Decimal("97.50"), settled_cash=Decimal("5000"), deployed=Decimal("0"), open_risk=Decimal("0"))
        self.assertEqual(plan.quantity, 10)
        self.assertEqual(plan.exposure, Decimal("1000"))
        self.assertEqual(plan.planned_risk, Decimal("25.00"))

class SeparateLedgerTests(unittest.TestCase):
    def test_actual_above_limit_fill_does_not_mutate_canonical_fill(self):
        ledgers = seeded_ledgers(canonical_entry=Decimal("100"))
        ledgers.record_actual_buy(signal_id="sig-1", price=Decimal("101"), shares=5, at=aware_et(date(2026, 8, 14), "10:14"))
        self.assertEqual(ledgers.canonical.open_positions[0].entry, Decimal("100"))
        self.assertEqual(ledgers.actual.open_positions[0].entry, Decimal("101"))
        self.assertTrue(ledgers.actual.reconciliation_required)
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_position_sizing tests.unit.test_settlement tests.unit.test_position_management tests.unit.test_circuit_breakers tests.integration.test_separate_ledgers -v`

Expected: missing risk/ledger modules.

- [ ] **Step 3: Implement risk invariants and projections**

```python
def size_long(entry: Decimal, stop: Decimal, settled_cash: Decimal, deployed: Decimal, open_risk: Decimal) -> PositionPlan:
    distance = entry - stop
    if distance <= 0:
        raise RiskBlock("NON_POSITIVE_STOP_DISTANCE")
    remaining_exposure = min(settled_cash, Decimal("1000") - deployed)
    remaining_risk = min(Decimal("25"), Decimal("50") - open_risk)
    quantity = floor_decimal(min(remaining_exposure / entry, remaining_risk / distance))
    if quantity < 1:
        raise RiskBlock("QUANTITY_BELOW_ONE")
    return PositionPlan(quantity, entry * quantity, distance * quantity)
```

Complete Step 3 through these RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 6A | Cent/tick loops for quantity zero, nonpositive stop, exposure/risk caps, two positions, one entry/session, and target at 2R | Add tick-aware `size_long` and `PositionPlan`; whole shares only and all five rejection codes are explicit |
| 6B | Friday/holiday T+1, unsettled proceeds, check after buy, zero/nonzero pending/unlogged values, and an intervening adjustment | Add settlement postings and `account_check_eligible`; account-wide cash is eligibility evidence, never strategy P&L |
| 6C | Wider stop, averaging down, +1R, +2R for one/two/three shares, event exit, and ten-session exit | Add `evaluate_position`; recommended/user stops remain separate and stop recommendations are monotonic |
| 6D | Three losses, five-session pause, weekly `$100`, monthly `$250`, week/month boundaries, non-loss reset, and strictest actual/canonical pause | Add `evaluate_breakers`; live entries pause while canonical observations continue and record adherence |
| 6E | Actual above-limit fill, shadow fill, duplicate ticker/different signal, partial lot, and unreconciled exposure | Add disjoint `CanonicalLedger`, `ActualLedger`, and `LedgerPair`; no method shares mutable position objects or overwrites canonical fills |

Run the matching module after each RED and GREEN in this order: `PYTHONPATH=src python3 -m unittest tests.unit.test_position_sizing -v`, `PYTHONPATH=src python3 -m unittest tests.unit.test_settlement -v`, `PYTHONPATH=src python3 -m unittest tests.unit.test_position_management -v`, `PYTHONPATH=src python3 -m unittest tests.unit.test_circuit_breakers -v`, and `PYTHONPATH=src python3 -m unittest tests.integration.test_separate_ledgers -v`.

- [ ] **Step 4: Run Task 6 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_position_sizing tests.unit.test_settlement tests.unit.test_position_management tests.unit.test_circuit_breakers tests.integration.test_separate_ledgers -v`

Expected: all Task 6 tests pass, including deterministic boundary loops over cents and share counts.

- [ ] **Step 5: Commit Task 6**

```bash
git add src/stock_monitor/risk.py src/stock_monitor/ledger.py tests/support.py tests/unit/test_position_sizing.py tests/unit/test_settlement.py tests/unit/test_position_management.py tests/unit/test_circuit_breakers.py tests/integration/test_separate_ledgers.py tests/fixtures/portfolios
git commit -m "feat: enforce risk and settlement limits"
```

## Task 7: Confirmation grammar and transactional reconciliation

**Files:**
- Create: `src/stock_monitor/confirmations.py`
- Create: `src/stock_monitor/reconciliation.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_confirmations.py`
- Test: `tests/integration/test_reconciliation.py`
- Test: `tests/integration/test_confirmation_idempotency.py`

- [ ] **Step 1: Write failing table-driven parser tests**

```python
class ConfirmationTests(unittest.TestCase):
    def test_full_buy_message_is_parsed(self):
        event = parse_confirmation("BOUGHT spy 5 shares @ 100.25 AT 10:14 ET; BID 100.24 ASK 100.25; STOP SET @ 97.50", session_date=date(2026, 8, 14))
        self.assertEqual(event.symbol, "SPY")
        self.assertEqual(event.quantity, 5)
        self.assertEqual(event.price, Decimal("100.25"))
        self.assertEqual(event.stop, Decimal("97.50"))

    def test_clear_degraded_buy_is_retained_as_noncompliant(self):
        event = parse_confirmation("BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET", session_date=date(2026, 8, 14))
        self.assertEqual(event.kind, ConfirmationKind.BUY)
        self.assertEqual(event.missing_fields, frozenset({"bid", "ask", "stop"}))

class AccountCheckOrderingTests(unittest.TestCase):
    def test_check_after_buy_or_invalidated_by_intervening_event_is_not_eligible(self):
        late = account_check(at="10:15", settled_cash="5000")
        buy = buy_event(at="10:14", price="100", shares=5)
        self.assertFalse(account_check_eligible(late, buy, intervening_events=()))
        early = account_check(at="10:10", settled_cash="5000")
        adjustment = cash_adjustment(at="10:12", amount="-100")
        self.assertFalse(account_check_eligible(early, buy, intervening_events=(adjustment,)))
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_confirmations tests.integration.test_reconciliation tests.integration.test_confirmation_idempotency -v`

Expected: missing confirmation/reconciliation modules.

- [ ] **Step 3: Implement anchored grammars and one-transaction application**

```python
def ingest_confirmation(journal: Journal, message_id: str, message_time: datetime, text: str, session_date: date) -> IngestionResult:
    with journal.transaction() as tx:
        raw_id, duplicate = tx.append_raw_message(message_id, message_time, text)
        if duplicate:
            return tx.existing_ingestion_result(raw_id)
        parsed = parse_confirmation_or_pending(text, session_date)
        return tx.apply_parsed_confirmation(raw_id, parsed)
```

Support these exact anchored forms:

```text
ACCOUNT CHECK settled_cash <AMOUNT> pending_orders <COUNT> unlogged_positions <COUNT> AT <TIME>
BOUGHT <TICKER> <SHARES> shares @ <PRICE> AT <TIME>; BID <BID> ASK <ASK>; STOP SET @ <STOP>
BOUGHT <TICKER> <SHARES> shares @ <PRICE> AT <TIME>
STOP UPDATED <TICKER> @ <PRICE> AT <TIME>
STOP FILLED <TICKER> <SHARES> shares @ <PRICE> AT <TIME>
SOLD <TICKER> <SHARES> shares @ <PRICE> AT <TIME>
SKIPPED <TICKER>
OPTION PAPER WINDOW START AT <TIME>
OPTION PAPER OPEN <OCC> BID <BID> ASK <ASK> DELTA <DELTA> OI <OI> VOLUME <VOLUME> AT <TIME>
OPTION PAPER MARK <OCC> BID <BID> ASK <ASK> AT <TIME>
OPTION PAPER CLOSE <OCC> BID <BID> ASK <ASK> AT <TIME>
RECONCILE CASH <SIGNED_AMOUNT> REASON <TEXT> AT <TIME>
RECONCILE UNRELATED POSITION <TICKER> <SIGNED_SHARES> shares @ <PRICE> AT <TIME>
RECONCILE PENDING ORDERS <COUNT> AT <TIME>
FEE <ASSET_ID> <AMOUNT> AT <TIME>
PARTIAL FILL <TICKER> <SHARES> shares @ <PRICE> AT <TIME>
```

Full-string anchoring, positive numeric validation except explicitly signed adjustments, OCC validation, ET/ISO timestamp rules, stable message ID plus action ordinal, and raw-text retention are mandatory. An account check is eligible only when it precedes the buy, reports zero pending/unlogged items, and no intervening cash/position/pending-order/reconciliation event exists. Clear off-policy actions append real exposure and force reconciliation; ambiguous input appends a pending event without mutating positions.

Execute the implementation in three RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 7A | One valid and at least two invalid cases for every anchored form above; ticker case, signed-adjustment, OCC, ET/ISO, positive numeric, and degraded-buy cases | Add immutable parsed event dataclasses and `parse_confirmation`; full input must match exactly once |
| 7B | Clear off-policy/above-limit/shadow/wider-stop events, ambiguous text, over-sell, delayed partial fill, and missing stop/spread | Add state validation returning `COMPLIANT`, `NONCOMPLIANT_RECONCILIATION_REQUIRED`, or `PENDING_CLARIFICATION` without discarding raw input |
| 7C | Same message replay, same text with different IDs, multi-action ordinal, crash/rollback, account-check ordering, and intervening adjustment | Add `ingest_confirmation` as one immediate transaction that appends raw/event/posting/projection/outbox state and uses `(message_id, action_ordinal)` uniqueness |

Run `test_confirmations`, `test_reconciliation`, and `test_confirmation_idempotency` individually for each RED/GREEN pair before the combined Task 7 command.

- [ ] **Step 4: Run Task 7 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_confirmations tests.integration.test_reconciliation tests.integration.test_confirmation_idempotency -v`

Expected: all Task 7 tests pass.

- [ ] **Step 5: Commit Task 7**

```bash
git add src/stock_monitor/confirmations.py src/stock_monitor/reconciliation.py tests/support.py tests/unit/test_confirmations.py tests/integration/test_reconciliation.py tests/integration/test_confirmation_idempotency.py tests/fixtures/messages
git commit -m "feat: record and reconcile manual confirmations"
```

## Task 8: Phase 1 lifecycle and prospective validation

**Files:**
- Create: `src/stock_monitor/sql/002_phase1.sql`
- Create: `src/stock_monitor/phase1.py`
- Create: `src/stock_monitor/validation.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_paper_fills.py`
- Test: `tests/integration/test_signal_lifecycle.py`
- Test: `tests/unit/test_equity_curve.py`
- Test: `tests/unit/test_phase1_promotion.py`

- [ ] **Step 1: Write failing paper-fill and promotion tests**

```python
class PaperFillTests(unittest.TestCase):
    def test_gap_above_limit_without_return_is_not_filled(self):
        observations = [trade(sequence=1, at="09:36", price="102.00"), quote(sequence=2, at="09:36", bid="101.99", ask="102.01"), quote(sequence=3, at="15:59", bid="101.50", ask="101.52")]
        result = simulate_entry(trigger=Decimal("100"), limit=Decimal("100.10"), observations=observations)
        self.assertEqual(result.status, SignalStatus.NOT_FILLED_LIMIT)

    def test_trigger_then_return_to_limit_fills_conservatively_at_limit(self):
        observations = [trade(sequence=1, at="09:36", price="102.00"), quote(sequence=2, at="10:02", bid="100.08", ask="100.10")]
        result = simulate_entry(trigger=Decimal("100"), limit=Decimal("100.10"), observations=observations)
        self.assertEqual(result.fill_price, Decimal("100.10"))

    def test_same_sequence_or_stale_quote_remains_unresolved(self):
        same_sequence = [trade(sequence=1, at="09:36", price="102.00"), quote(sequence=1, at="09:36", bid="100.08", ask="100.10")]
        stale_quote = [trade(sequence=1, at="09:36", price="102.00"), quote(sequence=2, at="10:02", bid="100.08", ask="100.10", fresh=False)]
        self.assertEqual(simulate_entry(Decimal("100"), Decimal("100.10"), same_sequence).status, SignalStatus.UNRESOLVED)
        self.assertEqual(simulate_entry(Decimal("100"), Decimal("100.10"), stale_quote).status, SignalStatus.UNRESOLVED)

    def test_trigger_at_exactly_0935_is_ignored(self):
        observations = [trade(sequence=1, at="09:35", price="102.00"), quote(sequence=2, at="09:36", bid="100.08", ask="100.10")]
        self.assertEqual(simulate_entry(Decimal("100"), Decimal("100.10"), observations).status, SignalStatus.NOT_TRIGGERED)

    def test_trigger_without_any_later_quote_is_unresolved(self):
        observations = [trade(sequence=1, at="09:36", price="102.00")]
        self.assertEqual(simulate_entry(Decimal("100"), Decimal("100.10"), observations).status, SignalStatus.UNRESOLVED)

    def test_only_zero_missing_or_crossed_quotes_are_unresolved(self):
        observations = [
            trade(sequence=1, at="09:36", price="102.00"),
            quote(sequence=2, at="09:37", bid="0", ask="0"),
            quote(sequence=3, at="09:38", bid="100.20", ask="100.10"),
        ]
        self.assertEqual(simulate_entry(Decimal("100"), Decimal("100.10"), observations).status, SignalStatus.UNRESOLVED)

class PromotionTests(unittest.TestCase):
    def test_twenty_trades_before_four_weeks_does_not_pass(self):
        result = evaluate_phase1(validation_fixture(closed_trades=20, elapsed_days=27, mean_r=Decimal("0.1"), adherence=Decimal("0.95")))
        self.assertFalse(result.passed)
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_paper_fills tests.integration.test_signal_lifecycle tests.unit.test_equity_curve tests.unit.test_phase1_promotion -v`

Expected: missing Phase 1/validation modules.

- [ ] **Step 3: Implement lifecycle, marks, and gates**

```python
def simulate_entry(trigger: Decimal, limit: Decimal, observations: Sequence[IntradayObservation]) -> PaperEntryResult:
    if not observations or any(not item.fresh for item in observations):
        return PaperEntryResult(SignalStatus.UNRESOLVED, None, None)
    ordered = sorted(observations, key=lambda item: (item.timestamp, item.sequence))
    if len({item.sequence for item in ordered}) != len(ordered):
        return PaperEntryResult(SignalStatus.UNRESOLVED, None, None)
    trigger_sequence: int | None = None
    saw_valid_post_trigger_quote = False
    for observation in ordered:
        if observation.timestamp.astimezone(NEW_YORK).time() <= time(9, 35):
            continue
        if trigger_sequence is None and observation.kind == ObservationKind.TRADE and observation.trade_price >= trigger:
            trigger_sequence = observation.sequence
            continue
        if trigger_sequence is not None and observation.sequence > trigger_sequence and observation.kind == ObservationKind.QUOTE:
            if observation.bid is None or observation.ask is None or observation.bid <= 0 or observation.ask <= 0 or observation.ask < observation.bid:
                continue
            saw_valid_post_trigger_quote = True
            if observation.ask <= limit:
                return PaperEntryResult(SignalStatus.TRIGGERED_PAPER, limit, observation.timestamp)
    if trigger_sequence is None:
        return PaperEntryResult(SignalStatus.NOT_TRIGGERED, None, None)
    return PaperEntryResult(SignalStatus.NOT_FILLED_LIMIT if saw_valid_post_trigger_quote else SignalStatus.UNRESOLVED, None, None)
```

Complete four RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 8A | Every legal/illegal lifecycle transition, strictly-after-09:35 trigger, next-premarket finalization, session expiry, and invalidation | Add migration 002 plus `advance_signal` with an explicit transition table and immutable event history |
| 8B | Trigger then later quote, gap-over-limit/no-return, missing quote, stale quote, same-sequence ambiguity, skipped-live canonical entry, stop/target ambiguity, and overnight gap | Add `simulate_entry` and exit simulation; missing freshness/order evidence is `UNRESOLVED`, never a countable fill |
| 8C | Canonical/actual `$5,000` curves, idle cash, conservative bid/close marks, exact `$249.99/$250/$250.01` drawdowns, and external cash-flow exclusion | Add `mark_equity`, ordered high-water points, and separate drawdown projections |
| 8D | 19/20 trades, 27/28 days, zero/positive mean R, 89.99/90% adherence, incomplete signals/shadows, and each hard-breach override | Add the fixed adherence checklist and `evaluate_phase1`; shadows remain statistics, no secondary substitutes, and every published signal needs a disposition |

Run `test_signal_lifecycle`, `test_paper_fills`, `test_equity_curve`, and `test_phase1_promotion` individually through RED/GREEN before the combined Task 8 command.

- [ ] **Step 4: Run Task 8 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_paper_fills tests.integration.test_signal_lifecycle tests.unit.test_equity_curve tests.unit.test_phase1_promotion -v`

Expected: all Task 8 tests pass.

- [ ] **Step 5: Commit Task 8**

```bash
git add src/stock_monitor/sql/002_phase1.sql src/stock_monitor/phase1.py src/stock_monitor/validation.py tests/support.py tests/unit/test_paper_fills.py tests/integration/test_signal_lifecycle.py tests/unit/test_equity_curve.py tests/unit/test_phase1_promotion.py tests/fixtures/intraday
git commit -m "feat: track prospective phase one validation"
```

## Task 9: Historical replay and Phase 2 paper options

**Files:**
- Create: `src/stock_monitor/sql/003_phase2_paper.sql`
- Create: `src/stock_monitor/replay.py`
- Create: `src/stock_monitor/options_paper.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_replay.py`
- Test: `tests/unit/test_option_selection.py`
- Test: `tests/unit/test_option_accounting.py`
- Test: `tests/integration/test_phase2_gate.py`

- [ ] **Step 1: Write failing replay and option-ranking tests**

```python
class ReplayTests(unittest.TestCase):
    def test_daily_bar_with_stop_and_target_assumes_stop_first(self):
        result = replay_ambiguous_bar(entry=Decimal("100"), stop=Decimal("98"), target=Decimal("104"), high=Decimal("105"), low=Decimal("97"))
        self.assertEqual(result.exit_reason, "STOP_FIRST_CONSERVATIVE")

class OptionSelectionTests(unittest.TestCase):
    def test_ranking_uses_expiration_delta_spread_oi_volume_symbol(self):
        ranked = rank_option_contracts(option_chain_fixture("tests/fixtures/options/full-tie-ladder.json"), signal_fixture())
        self.assertEqual(ranked[0].occ_symbol, "SPY261002C00600000")

    def test_missing_daily_mark_fails_prospective_window(self):
        window = option_window_fixture(missing_mark=True)
        self.assertEqual(evaluate_option_window(window).status, "RESTART_REQUIRED")

    def test_twenty_closes_before_four_weeks_cannot_pass(self):
        window = option_window_fixture(closed_trades=20, elapsed_days=27, missing_mark=False)
        self.assertEqual(evaluate_option_window(window).status, "IN_PROGRESS")

    def test_new_window_requires_explicit_start_event(self):
        failed = option_window_fixture(missing_mark=True)
        self.assertEqual(start_next_window(failed, start_event=None).status, "RESTART_REQUIRED")
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_replay tests.unit.test_option_selection tests.unit.test_option_accounting tests.integration.test_phase2_gate -v`

Expected: missing replay/options modules.

- [ ] **Step 3: Implement replay labels and paper-only options**

```python
def option_sort_key(contract: OptionContract, target_date: date) -> tuple[object, ...]:
    dte_distance = abs(contract.dte - 45)
    later_tie_break = -contract.expiration.toordinal()
    delta_distance = abs(contract.delta - Decimal("0.35"))
    return (dte_distance, later_tie_break, delta_distance, contract.relative_spread, -contract.open_interest, -contract.daily_volume, contract.occ_symbol)
```

Complete four RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 9A | Current-list bias label, point-in-time source cutoff, exclusion count, daily-only unresolved, both-stop-target, entry-stop, and overnight gap | Add `replay_diagnostic` and `replay_point_in_time`; strict replay rejects future/missing evidence and exact mechanics require ordered intraday observations |
| 9B | Phase 1 not passed, put/bearish contract, every DTE/delta/OI/volume/spread/event/fee/one-position boundary, and the full tie ladder | Add migration 003, `eligible_option_contracts`, and `rank_option_contracts`; only bullish paper long calls can be returned |
| 9C | Provider/Robinhood mismatch, ask entry, bid exit, fee reserve, 15:29/15:30/15:55/15:56 marks, missing/nonpositive/crossed mark, and ordered high water | Add append-only option window/accounting functions; missing required mark records zero and fails the current window |
| 9D | 19/20 closes, 27/28 days, expectancy/adherence/drawdown gates, absent/present restart event, underlying stop/target, ten sessions, 21 DTE, exercise/roll/live verbs | Add `evaluate_option_window` and `start_next_window`; prior failed records persist and every executable/live verb raises `PaperOnlyBoundaryError` |

Run `test_replay`, `test_option_selection`, `test_option_accounting`, and `test_phase2_gate` individually through RED/GREEN before the combined Task 9 command. No function or dataclass may contain brokerage order fields.

- [ ] **Step 4: Run Task 9 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_replay tests.unit.test_option_selection tests.unit.test_option_accounting tests.integration.test_phase2_gate -v`

Expected: all Task 9 tests pass.

- [ ] **Step 5: Commit Task 9**

```bash
git add src/stock_monitor/sql/003_phase2_paper.sql src/stock_monitor/replay.py src/stock_monitor/options_paper.py tests/support.py tests/unit/test_replay.py tests/unit/test_option_selection.py tests/unit/test_option_accounting.py tests/integration/test_phase2_gate.py tests/fixtures/options tests/fixtures/replay
git commit -m "feat: add replay and paper option validation"
```

## Task 10: Reports, archives, and inspectable exports

**Files:**
- Create: `src/stock_monitor/reports.py`
- Create: `src/stock_monitor/exports.py`
- Modify: `tests/support.py`
- Test: `tests/unit/test_report_precedence.py`
- Test: `tests/integration/test_report_archive.py`
- Test: `tests/integration/test_exports.py`
- Test: `tests/e2e/test_golden_reports.py`

- [ ] **Step 1: Write failing golden and precedence tests**

```python
class ReportPrecedenceTests(unittest.TestCase):
    def test_reconciliation_precedes_exit_recommendation(self):
        state = close_state(reconciliation=True, exit_due=True, stop_verified=False)
        report = render_close_report(state)
        self.assertIn("RECONCILIATION REQUIRED", report.body)
        self.assertNotIn("PROVISIONAL EXIT", report.outcome)

class ArchiveTests(unittest.TestCase):
    def test_same_report_id_has_same_path_and_hash(self):
        first = archive_report(self.report, self.root)
        second = archive_report(self.report, self.root)
        self.assertEqual(first.path, second.path)
        self.assertEqual(first.sha256, second.sha256)
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_report_precedence tests.integration.test_report_archive tests.integration.test_exports tests.e2e.test_golden_reports -v`

Expected: missing reports/exports modules.

- [ ] **Step 3: Implement deterministic Markdown and CSV output**

```python
def stable_report_id(kind: str, session_date: date, observation_ids: Sequence[str], state_hash: str) -> str:
    canonical = json.dumps({"kind": kind, "session": session_date.isoformat(), "observations": sorted(observation_ids), "state": state_hash}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

Complete three RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 10A | Golden premarket candidate, `NO TRADE`, data failure, Phase 1, replay, and Phase 2 paper output; all required fields and plan-only wording | Add pure renderers with fixed section order and explicit outcome/reason codes; candidates show role, six score components, trigger/limit/stop/target/shares/risk, feed/time, invalidations, and source links |
| 10B | All six close outcomes and pairwise conflicting states | Add `render_close_report` using the locked precedence; recommended and user-confirmed stops are separate and every action is provisional pending Robinhood verification |
| 10C | Same report retry, conflicting content under same ID, failed rename, pinned-observation replay, CSV numeric exactness, and secret canary | Add `stable_report_id`, atomic archive, content-hash conflict rejection, and `export_tables`; exports use decimal strings and never secret-bearing configuration fields |

Run `test_golden_reports`, `test_report_precedence`, `test_report_archive`, and `test_exports` individually through RED/GREEN before the combined Task 10 command.

- [ ] **Step 4: Run Task 10 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_report_precedence tests.integration.test_report_archive tests.integration.test_exports tests.e2e.test_golden_reports -v`

Expected: all Task 10 tests pass and golden output contains no secret canaries.

- [ ] **Step 5: Commit Task 10**

```bash
git add src/stock_monitor/reports.py src/stock_monitor/exports.py tests/support.py tests/unit/test_report_precedence.py tests/integration/test_report_archive.py tests/integration/test_exports.py tests/e2e/test_golden_reports.py tests/fixtures/golden_reports
git commit -m "feat: render auditable monitor reports"
```

## Task 11: Workflows, scheduled deduplication, CLI, and launcher

**Files:**
- Create: `src/stock_monitor/workflows.py`
- Create: `src/stock_monitor/scheduled.py`
- Create: `src/stock_monitor/cli.py`
- Create: `scripts/run_monitor.sh`
- Modify: `tests/support.py`
- Test: `tests/integration/test_scheduled.py`
- Test: `tests/e2e/test_cli.py`
- Test: `tests/e2e/test_recorded_scenarios.py`

- [ ] **Step 1: Write failing CLI and scheduler tests**

```python
class CliTests(unittest.TestCase):
    def test_missing_credentials_returns_configuration_exit_without_candidate(self):
        completed = run_cli("run", "premarket", env={"STOCK_MONITOR_HOME": self.tempdir})
        self.assertEqual(completed.returncode, 2)
        self.assertIn("CONFIGURATION REQUIRED", completed.stdout)
        self.assertNotIn("PRIMARY", completed.stdout)

class ScheduledTests(unittest.TestCase):
    def test_early_close_emits_once_across_both_wakes(self):
        first = run_close_wake(date(2026, 11, 27), time(12, 30))
        second = run_close_wake(date(2026, 11, 27), time(15, 30))
        self.assertEqual(first.outcome, "EMITTED")
        self.assertEqual(second.outcome, "ALREADY_EMITTED_NOOP")
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_scheduled tests.e2e.test_cli tests.e2e.test_recorded_scenarios -v`

Expected: missing workflows/CLI failures.

- [ ] **Step 3: Implement use cases and stable CLI surface**

```text
stock-monitor db init
stock-monitor verify universe
stock-monitor verify calendar
stock-monitor provider smoke
stock-monitor run premarket [--fixture PATH]
stock-monitor run close [--fixture PATH]
stock-monitor confirm --message-id ID --message-time ISO --text TEXT
stock-monitor phase1 status
stock-monitor replay diagnostic|point-in-time --fixture PATH
stock-monitor option-paper start|rank|status --fixture PATH
stock-monitor export
```

Complete four RED/GREEN cycles:

| Cycle | Failing behavior added first | Minimal production change |
|---|---|---|
| 11A | Missing configuration, closed market, stale universe/calendar, provider/source failure, no candidates, candidates, active breaker, and fixture injection | Add `WorkflowContext` plus `run_premarket`; adapters collect/normalize, engines decide, journal persists, and no nonzero path emits a candidate |
| 11B | Normal/early close, already-emitted no-op, unverified/reconciled positions, failed provider check, and missed run | Add `run_close` and `run_scheduled`; claim `(session_date, CLOSE)` with `BEGIN IMMEDIATE`, use the calendar-selected wake, persist report/outbox atomically, and never backfill |
| 11C | Parser coverage for every command/subcommand, exact exit codes, JSON safe fields, duplicate confirmation, export, and paper-only command rejection | Add `build_parser` and `main`; catch only declared domain errors into exit `2/3/4/5`, leave unexpected errors as `10`, and never serialize secrets/raw provider payloads |
| 11D | Launcher invoked from another directory, paths containing spaces, absent `PYTHONPATH`, and canary environment values | Add executable `scripts/run_monitor.sh` using its own absolute repository path and `exec python3 -m stock_monitor`; it prints only CLI output |

Run `test_recorded_scenarios`, `test_scheduled`, and `test_cli` individually through RED/GREEN before the combined Task 11 command. Pure modules may not import provider, journal, CLI, or filesystem modules.

- [ ] **Step 4: Run Task 11 tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_scheduled tests.e2e.test_cli tests.e2e.test_recorded_scenarios -v`

Expected: all Task 11 tests pass for eligible, no-trade, early-close, normal-close, reconciliation, stale-data, and missing-configuration scenarios.

- [ ] **Step 5: Commit Task 11**

```bash
git add src/stock_monitor/workflows.py src/stock_monitor/scheduled.py src/stock_monitor/cli.py scripts/run_monitor.sh tests/support.py tests/integration/test_scheduled.py tests/e2e/test_cli.py tests/e2e/test_recorded_scenarios.py tests/fixtures/scenarios
git commit -m "feat: orchestrate safe monitor workflows"
```

## Task 12: Operator documentation, acceptance audit, and schedule prompts

**Files:**
- Create: `README.md`
- Create: `docs/operations.md`
- Create: `docs/scheduled-prompts.md`
- Modify: `tests/support.py`
- Test: `tests/security/test_secret_redaction.py`
- Test: `tests/architecture/test_module_boundaries.py`
- Test: `tests/e2e/test_acceptance.py`

- [ ] **Step 1: Write failing acceptance and secret-audit tests**

```python
class SecretRedactionTests(unittest.TestCase):
    def test_canary_secrets_never_appear_in_output_or_archive(self):
        completed = run_cli_with_canary_secrets("provider", "smoke")
        combined = completed.stdout + completed.stderr + archived_text(self.tempdir)
        self.assertNotIn("CANARY_KEY_123", combined)
        self.assertNotIn("CANARY_SECRET_456", combined)

class AcceptanceTests(unittest.TestCase):
    def test_recorded_fixture_acceptance_matrix(self):
        outcomes = run_recorded_acceptance_matrix()
        self.assertEqual(outcomes, {"eligible": 0, "no_trade": 0, "early_close": 0, "normal_close": 0, "reconciliation": 5, "data_failure": 3})
```

- [ ] **Step 2: Run and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.security.test_secret_redaction tests.architecture.test_module_boundaries tests.e2e.test_acceptance -v`

Expected: failures until documentation/configuration and the final acceptance harness exist.

- [ ] **Step 3: Finish operator-facing assets**

README and operations documentation must cover the educational/no-guarantee boundary, exact risk limits, cash/T+1 behavior, `.env` setup, database initialization, source-manifest review, provider smoke, fixture/manual runs, reply examples, reconciliation, exports, recovery, schedule behavior, and the external Phase 1/Phase 2 promotion gates. The scheduled prompts must instruct Codex to run the safe launcher in the local project, return the exact report outcome to this task, never invent candidates on a nonzero exit, never access Robinhood, and never place an order.

- [ ] **Step 4: Run the complete local verification suite**

Run: `PYTHONPATH=src python3 -m unittest discover -s tests -v`

Expected: every test passes with zero failures/errors.

Run: `python3 -m compileall -q src tests`

Expected: exit 0 with no output.

Run: `git grep -nE 'paper-api\\.alpaca\\.markets|api\\.alpaca\\.markets|robinhood\\.(com|net)|/v2/orders' -- src config scripts .env.example`

Expected: no matches in executable/configuration code. Human-facing documentation may mention Robinhood only as the manual broker boundary.

Run: `git status --short`

Expected: only intentional Task 12 files before the commit; runtime database/cache/report paths remain ignored.

- [ ] **Step 5: Commit Task 12**

```bash
git add README.md docs/operations.md docs/scheduled-prompts.md tests/support.py tests/security/test_secret_redaction.py tests/architecture/test_module_boundaries.py tests/e2e/test_acceptance.py
git commit -m "docs: add stock monitor operating runbook"
```

## Post-plan activation sequence

After all twelve tasks and both review stages pass:

1. Run `./scripts/run_monitor.sh db init`.
2. Run `./scripts/run_monitor.sh verify universe` and `./scripts/run_monitor.sh verify calendar`.
3. Run the six recorded acceptance scenarios and archive their outputs.
4. Run `./scripts/run_monitor.sh provider smoke`. If credentials are absent or historical SIP is not entitled, record `PHASE1_BLOCKED_CONFIGURATION_OR_ENTITLEMENT`, do not produce live candidates, and do not create the monitor automations.
5. After the provider smoke succeeds, run real non-fixture premarket and close commands manually. Both must complete their current data/source checks and archive reviewable reports; otherwise record `PHASE1_BLOCKED_MANUAL_RUN` and do not schedule.
6. Run the premarket and close prompts manually with recorded fixtures as regression checks, then merge the implementation branch into `main` after the final independent review.
7. Only after Step 5 succeeds, create three heartbeat automations in the current task: weekdays 08:45 ET premarket, weekdays 12:30 ET early-close check, and weekdays 15:30 ET normal-close check. The close deduplication key guarantees one close report per session.
8. Verify every automation record by readback, then require one successful real scheduled premarket run and one successful real scheduled close run from the unattended environment. Review both reports. Until those runs succeed, status remains `PHASE1_BLOCKED_SCHEDULED_SMOKE`; only then may the system report `PHASE1_READY_FOR_PROSPECTIVE_VALIDATION`.
9. Update the Linear Stock Monitor project with commits, test counts, commands, schedule IDs when created, proven boundaries, and each credential/network gate exactly as observed.
