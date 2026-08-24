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
- Provider-backed workflows never request options data. Options endpoints and
  order/account/trading endpoints remain outside the application boundary.

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

Edit `.env` locally with exactly four literal, non-empty values, one assignment
each for `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `SEC_USER_AGENT`, and
`STOCK_MONITOR_HOME`. The right-hand sides are the real local values, but do not
show them in this runbook, a prompt, a command, chat, logs, screenshots,
reports, or backups. The parser accepts literal `NAME=value` data only; it does
not remove quotes, interpolate variable references, parse shell syntax, or
execute command-substitution text. Quotes would become part of the value and
should not be added. `SEC_USER_AGENT` must contain an application name and
contact email, and `STOCK_MONITOR_HOME` must be a non-empty absolute operator
directory.

The file must be owned by the current user, have one hard link, and have mode
0400 or 0600. `chmod 400 .env` and `chmod 600 .env` are the two supported
permission choices. Do not `source` or evaluate this file. The exact unattended
launcher opens it without following symlinks, validates it, and passes only the
four approved values to an isolated Python process. The interactive
`scripts/run_monitor.sh` launcher deliberately does not read `.env` and must
never be substituted into a scheduled prompt.

The Alpaca values are paper-account credentials used only by this
application's allow-listed market-data GET boundary. This is an application
restriction, not a provider-side read-only property of the credentials. The
application has no brokerage, account, trading, or order client. Runtime state
is stored under `$STOCK_MONITOR_HOME/.stock-monitor/`; Markdown reports are
under `$STOCK_MONITOR_HOME/reports/`.

## Initialize the database

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' db init --json
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

## Prepare, inspect, approve, and install evidence

Evidence renewal is a daily interactive human-review workflow. Run it from the
repository root by manually invoking the exact private-environment launcher;
never place these commands in an external schedule:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' evidence prepare --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' evidence inspect --proposal <proposal_sha256> --review-input <absolute-path> --json
# Human reviews the exact candidate and separately updates CURRENT_EVIDENCE_RELEASE_SHA256.
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' evidence install --candidate <candidate_sha256> --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify evidence --json
```

`prepare` performs credential-free, GET-only retrieval from the compiled exact
source catalog and writes the unreviewed proposal tree below
`$STOCK_MONITOR_HOME/.stock-monitor/evidence-proposals/<proposal_sha256>/`.
SEC retrieval may also populate the private content-addressed cache below
`$STOCK_MONITOR_HOME/.stock-monitor/cache/`. It never writes
`data/evidence/**` or changes the compiled release pin. Use the proposal's
`review-template.json` to create a separate reviewer input, make that input an
absolute-path regular file owned by the current user with mode 0600, and
inspect it. A partial collection returns one safe `PREPARED_BLOCKED` JSON
result and exit `3`; preserve its proposal digest for diagnosis, but do not
inspect or install it.

`inspect` is network-free. It verifies the proposal and reviewer input and
writes a digest-bound candidate below
`$STOCK_MONITOR_HOME/.stock-monitor/evidence-candidates/<candidate_sha256>/`.
Review the exact candidate digest, intended release digest, review window,
symbols, coverage states, reason codes, and candidate files. The command does
not approve or activate the candidate.

Approval is a separate human-reviewed source-control change to the exact
`CURRENT_EVIDENCE_RELEASE_SHA256` value. Neither `prepare`, `inspect`, nor
`install` may update that pin, infer approval from a reviewer-input file, or
self-repin. Only after that independent change is reviewed and present may the
network-free `install` command run. It returns exit `4` if the exact release is
not already pinned or if its compare-and-swap parent no longer matches. The
final `verify evidence` readback is required before any runtime use.

The current public catalog has no approved authority bundle that can turn
silence into broad clear coverage. Relevant silence therefore remains
`UNKNOWN`. NVIDIA's FY27 second-quarter financial-results event on August 26,
2026 overlaps the current 2–10 trading-day NVDA hold window and must remain a
`BINARY_EVENT_DURING_HOLD` block. Keep the external schedule count at zero
until a separately approved authority or policy change resolves the coverage
gap and every activation gate below has passed and been reviewed.

Then run the public validations:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify universe --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify evidence --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify calendar --json
```

All three must exit `0`. Verify the selected universe, calendar, and every
subject-evidence release are current now and remain current through the next
wake of every proposed external schedule. A release that is valid now but
expires before the next wake does not satisfy this gate. An expired,
incomplete, conflicting, or unverifiable manifest is a data block; do not
substitute an older cache.

## Provider smoke

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' provider smoke --json
```

This must stay within the application's GET-only market-data boundary and must
verify current market-data health and the required entitlement. Exit `0` with
overall `READY` is the only success. Exit `2`, `3`, `5`, or `10` blocks
non-fixture use. In a build where the provider adapter is not activated, the
command intentionally fails closed with the paper/manual boundary. Record
`PHASE1_BLOCKED_CONFIGURATION_OR_ENTITLEMENT`; do not schedule the monitor and
do not produce a live candidate.

## Recorded acceptance and manual runs

Fixtures never grant market or brokerage authority. Run the locked six-case
matrix from the repository root through the private environment launcher. This
avoids exporting or sourcing credentials; fixture mode still performs no
provider request:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --fixture tests/fixtures/scenarios/eligible.json --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --fixture tests/fixtures/scenarios/no-candidates.json --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --fixture tests/fixtures/scenarios/early-close.json --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --fixture tests/fixtures/scenarios/normal-close.json --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --fixture tests/fixtures/scenarios/reconciliation.json --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --fixture tests/fixtures/scenarios/provider-failure.json --json
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

Only after the current universe, calendar, and evidence checks and provider
smoke exit `0`, create or idempotently read back the explicit Phase 1 window.
Choose `YYYY-MM-DD` only from the reviewed calendar:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' phase1 start --session YYYY-MM-DD --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' phase1 status --json
```

Retain and review the exact `phase1 start` readback. `phase1 status` is useful
diagnostic output, but it does not replace the active Phase 1 validation
authority required by a canonical run. A missing, copied, stale, conflicting,
or inactive authority blocks activation.

Next perform a real manual premarket run and a real manual close run, without
`--scheduled` and without a fixture, inside their applicable due windows:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --json
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --json
```

Both runs must repeat current provider/source checks and create canonical,
reviewable reports. Review the exact output, archive, source lineage, and
terminal result from each. Otherwise record `PHASE1_BLOCKED_MANUAL_RUN` and do
not schedule. A candidate report remains paper-plan only and still requires a
separate same-session account check and manual decision.

## Manual confirmations

Give each source message a stable unique ID and its original offset-aware
message time. Examples:

```sh
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' confirm \
  --message-id msg-account-20260814-1010 \
  --message-time 2026-08-14T10:10:00-04:00 \
  --text 'ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET' \
  --json

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' confirm \
  --message-id msg-buy-aapl-20260814-1014 \
  --message-time 2026-08-14T10:14:00-04:00 \
  --text 'BOUGHT AAPL 4 shares @ 225.10 AT 10:14 ET; BID 225.09 ASK 225.10; STOP SET @ 220.00' \
  --json

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' confirm \
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
'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' export --json
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
3. Restore the literal `.env` at mode 0400 or 0600. Through the exact unattended
   launcher, run `db init`, then verify universe, evidence, and calendar.
4. Run all six recorded acceptance scenarios in a separate fresh test home.
5. Run `export` and compare report IDs, hashes, row counts, and the latest
   account/position facts with the last reviewed backup.
6. Resolve every difference through an append-only confirmation. Repeat every
   activation gate, including real manual and reviewed scheduled-mode
   premarket and close smoke, before re-enabling schedules.

If a verified backup is unavailable, initialize a new journal but do not
reconstruct or assume actual holdings. Record the account as reconciliation
required and rebuild truth from explicit operator evidence.

## Schedule behavior

Scheduling is external to the monitor and remains an all-or-none activation.
The exact half-open America/New_York due windows are:

- `08:45:00 <= start < 09:00:00 ET` for premarket;
- `12:30:00 <= start < 12:45:00 ET` for a verified early close; and
- `15:30:00 <= start < 15:45:00 ET` for a normal close.

Before the window the result is `NOT_DUE_NOOP`. At or after its end the result
is `MISSED_RUN_NOOP`. There is no backfill, no retry with later market data, and
no reuse of an earlier report or candidate. The nominal 08:45, 12:30, or 15:30
time remains the economic cutoff even if dispatch begins later inside the
window. The close path remains deduplicated to one report per market session.

Complete these gates in order before creating any external schedule:

1. Review the current universe, calendar, and subject evidence, run `verify
   universe --json`, `verify evidence --json`, and `verify calendar --json`,
   and confirm every selected release remains current through the next wake of
   each proposed heartbeat.
2. Run provider smoke through the exact unattended launcher and require exit
   `0` with overall `READY`.
3. Create or read back the reviewed Phase 1 window and confirm the canonical
   Journal has the active Phase 1 validation authority. Diagnostic status or a
   prior report is not a substitute for that authority.
4. Complete and review the real manual premarket and real manual close reports
   described above.
5. While there are still zero external schedules, invoke the unattended
   `--scheduled` path manually inside its due window. Review one scheduled-mode
   premarket result and one scheduled-mode close result (normal or verified
   early close), including exact exit, outcome, report, and source lineage:

   ```sh
   '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --scheduled --json
   '/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --scheduled --json
   ```

   These are the required reviewed scheduled-mode premarket smoke and reviewed
   scheduled-mode close smoke. Running the commands from a terminal exercises
   the scheduled code path without creating an external schedule.
6. Only after all five gates pass may the three weekday heartbeats be created:
   08:45 premarket, 12:30 early-close check, and 15:30 normal close. Use the
   exact prompts in `docs/scheduled-prompts.md`; immediately read back and
   compare every schedule's name, time, timezone, project, enabled state, and
   prompt. Never create or retain a partial set.

Immediately before creation, repeat the release-validity and authority checks.
If any gate is invalid, expired, missing, or unreviewed—or if there is no active
Phase 1 validation authority—record `PHASE1_BLOCKED_SCHEDULED_SMOKE` and leave
zero external schedules. Current evidence that expires before the next wake is
expired for this gate. Do not weaken a gate because a later wake could retry.

Every scheduled invocation must pass `--scheduled`; without it, `run premarket`
and `run close` remain explicit manual runs. The prompts use only the exact
absolute unattended launcher, disclose no environment value, never request
options data, never access Robinhood, and never place, route, modify, or cancel
an order. After the three schedule records are verified, the operator may
record `PHASE1_READY_FOR_PROSPECTIVE_VALIDATION`; scheduling still confers no
trade or promotion authority.

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
