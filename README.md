# Stock Monitor

Stock Monitor is a local, read-only decision-support tool for reviewing liquid
stocks and ETFs. It is educational, not financial advice, and offers no
guarantee of profit, income, or loss avoidance. It never connects to Robinhood
and never places an order. Any brokerage action is a separate, manual decision
by the operator.

The default operating model is deliberately conservative: cash only, long
stocks/ETFs only, a holding period of 2–10 trading days, at most two positions,
one new entry per session, $25 planned risk per position, $50 combined open
risk, and at most $1,000 live exposure. New entries pause after three
consecutive losses, a $100 weekly drawdown, or a $250 monthly drawdown. No
averaging down, leverage, shorting, or live options are permitted.

## Quick start

Use Python 3.11 or newer. From this repository:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e .
cp .env.example .env
chmod 600 .env
```

Edit `.env`, then load it into the current shell. The launcher does not read
the file automatically.

```sh
set -a
. ./.env
set +a
./scripts/run_monitor.sh db init --json
./scripts/run_monitor.sh verify universe --json
./scripts/run_monitor.sh verify calendar --json
./scripts/run_monitor.sh provider smoke --json
```

`APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` are for read-only Alpaca market
data. `SEC_USER_AGENT` must include an application name and contact email.
`STOCK_MONITOR_HOME` is optional; when set, it owns `.stock-monitor/` and
`reports/`. Keep `.env`, the database, exports, and reports out of version
control.

The provider smoke must return exit `0` before any non-fixture run. An absent
or unactivated provider adapter fails closed; that is a blocker, not permission
to make up data. Recorded fixtures are safe for local acceptance checks:

```sh
./scripts/run_monitor.sh run premarket \
  --fixture tests/fixtures/scenarios/eligible.json --json
./scripts/run_monitor.sh run close \
  --fixture tests/fixtures/scenarios/normal-close.json --json
```

Each recorded run is visibly labeled `FIXTURE` and uses a content-addressed
fixture state/report root. It never writes the canonical operator journal,
report claims, outbox, or report archive.

Every nonzero exit means no candidate and no authorized position action. Never
invent a candidate from stale output or from a previous report.

## Operator documentation

See [docs/operations.md](docs/operations.md) for setup, source review, fixture
and manual runs, confirmations, reconciliation, exports, recovery, scheduling,
and the external Phase 1 and Phase 2 promotion gates. Safe Codex prompt text is
in [docs/scheduled-prompts.md](docs/scheduled-prompts.md).

The locked exits are `0` report/no-op success, `2` configuration required, `3`
data/source unavailable, `4` policy/risk block, `5` reconciliation or manual
boundary required, and `10` unexpected internal error.
