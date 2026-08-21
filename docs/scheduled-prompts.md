# Safe scheduled prompts

These are prompt bodies for external Codex heartbeats. They do not create a
schedule. Create schedules only after the activation gates in `operations.md`
have passed, then read every schedule back exactly.

The absolute launcher for this checkout is:

```text
/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor.sh
```

Every prompt must return the exact process exit code and exact report outcome
to this task. A nonzero exit is a blocked result: never invent candidates,
never reuse a previous candidate, and never convert a failure into `NO TRADE`.
The scheduled agent must never access Robinhood and must never place an order.

## Weekday 08:45 ET premarket

```text
Run the Stock Monitor premarket workflow for the current session using exactly
this absolute launcher and no fixture:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor.sh' run premarket --scheduled --json

Capture stdout, stderr, and the process exit code without exposing environment
variables or credentials. Return the exact JSON report outcome and exit code to
this Codex task, plus the report path when the command supplied one. If the exit
is nonzero, quote only the safe command output, state that no candidate or action
was produced, and never invent or recover a candidate from earlier output. Do
not access Robinhood. Never place, route, modify, or cancel an order.
```

## Weekday 12:30 ET early-close check

```text
Use the reviewed local calendar to decide whether today is an early-close
session. Use only the local Stock Monitor project and this absolute launcher:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor.sh' verify calendar --json

If calendar verification is nonzero, return its exact outcome and exit code to
this Codex task and do not run close. If the verified manifest does not identify
today as an early close, return exactly "NO RUN: NOT AN EARLY-CLOSE SESSION";
that is a heartbeat status, not a fabricated report outcome. Only for a verified
early-close session, run:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor.sh' run close --scheduled --json

Capture stdout, stderr, and the process exit code without exposing environment
variables or credentials. Return the exact close report outcome and exit code to
this task, plus the report path when supplied. On a nonzero exit, state that no
position action is authorized and never invent a substitute outcome. Never
access Robinhood and never place, route, modify, or cancel an order.
```

## Weekday 15:30 ET normal close

```text
Run the Stock Monitor close workflow for the current session using exactly this
absolute launcher and no fixture:

'/Users/jbuenosantan/Documents/ChatGPT/Stock Monitor/scripts/run_monitor.sh' run close --scheduled --json

Capture stdout, stderr, and the process exit code without exposing environment
variables or credentials. Return the exact JSON close report outcome and exit
code to this Codex task, plus the report path when the command supplied one. If
the exit is nonzero, quote only the safe command output, state that no position
action is authorized, and never invent an action from a prior report. Do not
access Robinhood. Never place, route, modify, or cancel an order.
```
