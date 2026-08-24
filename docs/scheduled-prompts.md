# Safe scheduled prompts

These are prompt bodies for external Codex heartbeats. They do not create a
schedule and never authorize a trade. Every prompt must use only this exact
stable main-checkout unattended launcher:

```text
/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh
```

Never use the interactive launcher in a scheduled prompt. The unattended
launcher reads the private literal `.env` without sourcing it and emits no
credential value. Never copy environment values into a prompt, schedule,
command, result, report, or chat.

Evidence lifecycle commands are prohibited in every scheduled prompt. Never
run `evidence prepare`, `evidence inspect`, or `evidence install` from a
schedule; never read reviewer input, compile or activate an evidence candidate,
refresh or extend evidence timestamps, alter the compiled release pin, or
self-repin. Those steps belong to the interactive, separately reviewed human
workflow in `docs/operations.md`.

## Activation gate

Leave scheduling disabled until all of the following have been completed and
reviewed in order:

1. The current universe, evidence, and calendar releases pass `verify universe
   --json`, `verify evidence --json`, and `verify calendar --json`. Their
   reviewed validity must remain current through the next wake of every
   proposed schedule; being current only at creation time is insufficient.
2. `provider smoke --json` exits `0` with overall `READY` through the exact
   unattended environment.
3. The Journal has an active Phase 1 validation authority from the reviewed
   Phase 1 bootstrap. A diagnostic status or previous report is not authority.
4. A real manual premarket and a real manual close run complete inside their
   due windows, and both canonical reports are reviewed.
5. With no external schedule present, an operator manually exercises the
   unattended `--scheduled` path inside the due windows and reviews one
   scheduled-mode premarket smoke and one scheduled-mode close smoke.

If any gate is invalid, expired, missing, or unreviewed, or if no active Phase
1 validation authority exists, record `PHASE1_BLOCKED_SCHEDULED_SMOKE` and
leave zero external schedules. Do not create a partial set. Only after every
gate passes may all three schedule records be created, read back, and compared
with the intended name, time, America/New_York timezone, project, enabled
state, and exact prompt.

Current public-source silence is `UNKNOWN`, not clear coverage, because no
approved broad clear-capable authority bundle exists. The known NVIDIA FY27
second-quarter financial-results event on August 26, 2026 also overlaps the
current NVDA hold window. Therefore the schedule count remains zero until a
separately approved authority or policy change resolves the evidence gap and
all five gates above pass on fresh reviewed evidence.

## Timing and invariant behavior

The exact half-open due windows are:

- `08:45:00 <= start < 09:00:00 ET` for premarket;
- `12:30:00 <= start < 12:45:00 ET` for a verified early close; and
- `15:30:00 <= start < 15:45:00 ET` for a normal close.

There is no backfill. A late wake must preserve `MISSED_RUN_NOOP`; it must not
reuse an earlier report, rerun with later prices, or invent a candidate or
position action. Every prompt returns the exact process exit code, safe output,
and exact report outcome to this task. A nonzero exit is a blocked result, not
`NO TRADE`. The agent must never request options data, access Robinhood, or
place, route, modify, or cancel an order.

## Weekday 08:45 ET premarket

```text
Run the Stock Monitor premarket workflow exactly once for the current reviewed
session. Use no fixture and only this exact command:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run premarket --scheduled --json

Do not inspect or source .env. Capture stdout, stderr, and the process exit code
without exposing environment values or credentials. Return the exact safe JSON
outcome and exit code to this Codex task, plus the report path when supplied.
If the exit is nonzero, state that no candidate or action was produced; never
recover a candidate from earlier output. Never request options data, access
Robinhood, or place, route, modify, or cancel an order.
```

## Weekday 12:30 ET early-close check

```text
Use no fixture. First verify the reviewed local calendar with only this exact
command:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' verify calendar --json

Do not inspect or source .env. If verification is nonzero, return its exact safe
outcome and exit code to this Codex task and do not run close. If the verified
calendar does not identify today as an early-close session, return exactly
"NO RUN: NOT AN EARLY-CLOSE SESSION"; that is a heartbeat status, not a
fabricated report outcome. Only for a verified early-close session, run exactly:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --scheduled --json

Capture stdout, stderr, and the process exit code without exposing environment
values or credentials. Return the exact close outcome and exit code to this
task, plus the report path when supplied. On a nonzero exit, state that no
position action is authorized and never invent a substitute outcome. Never
request options data, access Robinhood, or place, route, modify, or cancel an
order.
```

## Weekday 15:30 ET normal close

```text
Run the Stock Monitor close workflow exactly once for the current reviewed
session. Use no fixture and only this exact command:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor_unattended.sh' run close --scheduled --json

Do not inspect or source .env. Capture stdout, stderr, and the process exit code
without exposing environment values or credentials. Return the exact safe JSON
close outcome and exit code to this Codex task, plus the report path when
supplied. If the exit is nonzero, state that no position action is authorized;
never invent an action from a prior report. Never request options data, access
Robinhood, or place, route, modify, or cancel an order.
```
