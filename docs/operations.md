# Stock Monitor operations

## Safety and operating limits

This tool is educational decision support. It is not financial advice and
provides no guarantee of profit, monthly income, bill coverage, or protection
from loss. A report is a plan for review, never an instruction or an order.

The immutable default limits are:

- Cash-only, long-only stocks and approved ETFs; no margin, leverage, shorts,
  averaging down, or live options.
- An intended holding period of 2–10 trading days.
- At most two open positions and one new entry per trading session.
- $25 maximum planned risk per position, $50 maximum combined open risk, and
  $1,000 maximum actual live stocks/ETFs exposure.
- Pause new live entries after three consecutive losses, a $100 weekly
  drawdown, or a $250 monthly drawdown. Canonical paper observation continues
  during a pause.
- The canonical paper account starts at $5,000 and stays separate from the
  actual manual ledger.
- Manual brokerage verification and manual execution only. The application
  must never access Robinhood and must never place an order.

A cash-account sale settles T+1: proceeds become reusable on the next eligible
trading session, not immediately after the sale. Before any manual buy, verify
same-session settled cash, zero pending orders, and zero unlogged positions.
Unsettled proceeds do not count as buying power for this policy.

## Install and configure

Run from the repository root with Python 3.11 or newer:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e .
cp .env.example .env
chmod 600 .env
```

Edit `.env` without quoting or committing the secrets:

```dotenv
APCA_API_KEY_ID=<read-only-market-data-key>
APCA_API_SECRET_KEY=<read-only-market-data-secret>
SEC_USER_AGENT=Stock Monitor you@example.com
STOCK_MONITOR_HOME=<optional-absolute-operator-directory>
```

The launcher deliberately does not parse `.env`. Load it into each interactive
or unattended environment before invoking the launcher:

```sh
set -a
. ./.env
set +a
```

Do not print the environment, put secrets on a command line, place secrets in
fixtures, or copy `.env` into reports/backups. Runtime state is stored under
`$STOCK_MONITOR_HOME/.stock-monitor/` (or the repository root when the override
is empty); Markdown reports are under `$STOCK_MONITOR_HOME/reports/`.

## Initialize the database

```sh
./scripts/run_monitor.sh db init --json
```

Exit `0` with `INITIALIZED` confirms that all known SQLite migrations were
applied. Re-running is safe. A missing or invalid environment returns exit `2`.
Do not edit SQLite rows by hand: observations, confirmations, and execution
events are append-only audit evidence.

## Review and verify source manifests

Review the files before trusting a run:

- `config/policy.toml`: confirm the risk limits above are unchanged.
- `config/sources.toml`: confirm only reviewed HTTPS market-data, SEC, NYSE,
  and Nasdaq sources are present. No brokerage/order host is allowed.
- `config/fees.json`: confirm status, effective session, source checksum, and
  per-contract paper fee inputs.
- `data/calendars/<year>.json`: confirm primary NYSE provenance, Nasdaq
  cross-check, retrieval date, closures, early closes, and manual-disable days.
- `data/universe/<review-date>.json`: confirm review/effective dates, checksum,
  membership sources, product/leverage flags, benchmark and sector mapping,
  tick size, and issuer/free-float or official ETF sponsor evidence.
- `data/evidence/current.json`: confirm reviewed evidence provenance and age.

Then run the public validations:

```sh
./scripts/run_monitor.sh verify universe --json
./scripts/run_monitor.sh verify calendar --json
```

Both must exit `0`. An expired, incomplete, conflicting, or unverifiable
manifest is a data block; do not substitute an older cache.

## Provider smoke

```sh
./scripts/run_monitor.sh provider smoke --json
```

This must remain read-only and must verify current market-data health and the
required entitlement. Exit `0` is the only success. Exit `2`, `3`, `5`, or `10`
blocks non-fixture use. In a build where the provider adapter is not activated,
the command intentionally fails closed with the paper/manual boundary. Record
`PHASE1_BLOCKED_CONFIGURATION_OR_ENTITLEMENT`; do not schedule the monitor and
do not produce a live candidate.

## Recorded acceptance and manual runs

Fixtures never grant market or brokerage authority. Run the locked six-case
matrix from the repository root:

```sh
./scripts/run_monitor.sh run premarket --fixture tests/fixtures/scenarios/eligible.json --json
./scripts/run_monitor.sh run premarket --fixture tests/fixtures/scenarios/no-candidates.json --json
./scripts/run_monitor.sh run close --fixture tests/fixtures/scenarios/early-close.json --json
./scripts/run_monitor.sh run close --fixture tests/fixtures/scenarios/normal-close.json --json
./scripts/run_monitor.sh run close --fixture tests/fixtures/scenarios/reconciliation.json --json
./scripts/run_monitor.sh run premarket --fixture tests/fixtures/scenarios/provider-failure.json --json
```

The exact exit matrix is `eligible=0`, `no_trade=0`, `early_close=0`,
`normal_close=0`, `reconciliation=5`, and `data_failure=3`. For every nonzero
exit, `candidates` must be empty. Save the exact JSON and archived report; never
invent candidates or actions to fill a missing result.

Every recorded run is labeled `FIXTURE` in both JSON and Markdown. Its fixture
bytes select a content-addressed fixture root under
`.stock-monitor/fixtures/<sha256>/`; the journal, report claims, outbox, and
`reports/` archive all stay inside that root. Fixture runs never write the
canonical operator journal or canonical operator report tree, and two different
same-session fixtures cannot claim each other's report.

Only after the source checks and provider smoke exit `0`, run without
`--fixture`:

```sh
./scripts/run_monitor.sh run premarket --json
./scripts/run_monitor.sh run close --json
```

Both manual runs must complete current provider/source checks and create
reviewable reports. Otherwise record `PHASE1_BLOCKED_MANUAL_RUN` and do not
schedule. A candidate report remains paper-plan only and still requires a
separate same-session account check and manual decision.

## Manual confirmations

Give each source message a stable unique ID and its original offset-aware
message time. Examples:

```sh
./scripts/run_monitor.sh confirm \
  --message-id msg-account-20260814-1010 \
  --message-time 2026-08-14T10:10:00-04:00 \
  --text 'ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET' \
  --json

./scripts/run_monitor.sh confirm \
  --message-id msg-buy-aapl-20260814-1014 \
  --message-time 2026-08-14T10:14:00-04:00 \
  --text 'BOUGHT AAPL 4 shares @ 225.10 AT 10:14 ET; BID 225.09 ASK 225.10; STOP SET @ 220.00' \
  --json

./scripts/run_monitor.sh confirm \
  --message-id msg-skip-spy-20260814 \
  --message-time 2026-08-14T10:20:00-04:00 \
  --text 'SKIPPED SPY' \
  --json
```

Other exact forms include `STOP UPDATED AAPL @ 223.00 AT 15:32 ET`,
`STOP FILLED AAPL 4 shares @ 219.80 AT 10:01 ET`, and
`SOLD AAPL 4 shares @ 230.00 AT 15:31 ET`. Repeating the same ID and identical
content is idempotent. Reusing an ID with different content is a conflict.
Ambiguous text is retained for clarification and grants no quantity mutation.

## Reconciliation

`RECONCILIATION REQUIRED`, `POSITION UNVERIFIED`, and `STOP UNVERIFIED` all
block a position action. Compare the manual brokerage account with the journal,
then append the exact correction; never rewrite history. Supported examples
include:

```text
RECONCILE CASH -100.25 REASON unrelated withdrawal AT 11:00 ET
RECONCILE UNRELATED POSITION AAPL +4 shares @ 225.10 AT 11:01 ET
RECONCILE PENDING ORDERS 0 AT 11:02 ET
```

Submit the chosen line with `stock-monitor confirm`/the launcher, a new stable
message ID, and the original timestamp. Re-run the close review. Until the
report is clear, take no new action and do not infer state from Robinhood.

## Inspectable exports

```sh
./scripts/run_monitor.sh export --json
```

Exit `0` returns the CSV paths under `.stock-monitor/exports/`. Money is
rendered as exact decimal text. Secret-bearing columns and nested secret keys
are redacted. Treat exports as sensitive audit material even after redaction;
review permissions before sharing.

## Backup and recovery

Keep encrypted backups of `.stock-monitor/journal.sqlite3` and `reports/`
outside the repository. Use SQLite's online backup API or a filesystem snapshot
that preserves a consistent database; do not copy only the main database while
it is active in WAL mode. Exclude `.env` and provider credentials.

For recovery:

1. Stop every external schedule and preserve the damaged state read-only.
2. Restore the database and reports into a fresh `STOCK_MONITOR_HOME`.
3. Load `.env`, run `db init`, then verify universe and calendar.
4. Run all six recorded acceptance scenarios in a separate fresh test home.
5. Run `export` and compare report IDs, hashes, row counts, and the latest
   account/position facts with the last reviewed backup.
6. Resolve every difference through an append-only confirmation. Run a manual
   premarket and close smoke before re-enabling schedules.

If a verified backup is unavailable, initialize a new journal but do not
reconstruct or assume actual holdings. Record the account as reconciliation
required and rebuild truth from explicit operator evidence.

## Schedule behavior

Scheduling is external to the monitor. Activate it only after source review,
provider smoke, and both real manual runs succeed. The approved heartbeats are
weekdays at 08:45 ET for premarket, 12:30 ET for an early-close check, and
15:30 ET for normal close. The early-close heartbeat runs a close only when the
reviewed calendar identifies an early close. The close workflow is deduplicated
to one report per market session, and missed runs are never backfilled.

Every scheduled invocation must pass `--scheduled`; without it, `run premarket`
and `run close` remain explicit manual runs. Premarket is selected at exactly
08:45 America/New_York even when the caller timestamp has another UTC offset.
`verify calendar --json` returns the current session date, open status, open,
review, and close wall times, timezone, and the exact early-close flag.

Use the prompts in `docs/scheduled-prompts.md`. Read back every external
schedule record after creation. One successful unattended real premarket and
one successful unattended real close report must be reviewed before recording
`PHASE1_READY_FOR_PROSPECTIVE_VALIDATION`; otherwise the status is
`PHASE1_BLOCKED_SCHEDULED_SMOKE`.

Exit handling is exact: `0` report/no-op success, `2` configuration, `3`
data/source unavailable, `4` policy/risk block, `5` reconciliation or manual
boundary required, and `10` unexpected internal error. On any nonzero exit,
return the exact output, emit no candidate, and authorize no action.

## External promotion gates

Phase 1 is stocks/ETFs paper validation, not automatic permission. A window may
be considered only after all of these are true:

- At least 20 complete, closed `PRIMARY` trades and at least 28 elapsed calendar
  days (four weeks).
- Strictly positive mean net R and at least 90% adherence.
- Canonical and actual maximum drawdown are each no more than $250.
- Every published signal has a complete terminal disposition, every closed
  primary record is complete, and there is no hard risk-limit breach.

The persisted evidence, source digests, reports, and issued decision must then
be reviewed and approved by an external human gate. A diagnostic `PASSED`
value does not self-promote, enable automation, or authorize a trade.

Phase 2 starts only after that external Phase 1 approval and remains paper-only
long calls with a separate $5,000 paper account. Its external gate requires:

- At least 20 closed option-paper trades and at least 28 elapsed calendar days.
- Strictly positive mean net R, at least 90% adherence, and maximum drawdown no
  greater than $250.
- A complete prospective record with every required daily mark, exit review,
  and required close; no hard breach.

Any missing mark/review/required close makes the current window
`RESTART_REQUIRED`; a new window needs an explicit start event and prior records
remain append-only. Even a complete Phase 2 `PASSED` result is diagnostic until
an external human reviews the persisted evidence. Live options remain
prohibited; the tool contains no promotion path that can place or route orders.
