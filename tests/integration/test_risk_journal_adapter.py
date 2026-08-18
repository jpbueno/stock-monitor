from __future__ import annotations

import copy
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from stock_monitor.confirmations import ConfirmationEnvelope
from stock_monitor.journal import (
    Journal,
    JournalAccountCheckWindowSource,
    is_verified_journal_action_source,
    is_verified_journal_window_source,
)
from stock_monitor.market_calendar import load_current_market_calendar
from stock_monitor.reconciliation import (
    UnavailableActualEntryAuthorityResolver,
    UnavailableSignalPlanResolver,
    ingest_confirmation,
)
import stock_monitor.risk as risk_module
from stock_monitor.risk import (
    ExecutionEvent,
    RiskBlock,
    SessionCalendarResolver,
    evaluate_account_check_window,
    is_issued_confirmed_buy_action,
    is_issued_journal_event_window,
)
from tests.support import policy_fixture


ROOT = Path(__file__).resolve().parents[2]
SESSION = date(2026, 8, 14)


def envelope(
    message_id: str,
    text: str,
    *,
    message_time: str,
    received_at: str,
) -> ConfirmationEnvelope:
    return ConfirmationEnvelope(
        message_id=message_id,
        text=text,
        message_time=datetime.fromisoformat(message_time),
        received_at=datetime.fromisoformat(received_at),
        session_date=SESSION,
    )


class RiskJournalAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.journal = Journal.open(Path(temporary.name) / "journal.sqlite3")
        self.addCleanup(self.journal.close)
        self.calendar = SessionCalendarResolver(
            (load_current_market_calendar(ROOT, as_of=SESSION),)
        )

    def ingest(self, item: ConfirmationEnvelope):
        return ingest_confirmation(
            self.journal,
            item,
            plans=UnavailableSignalPlanResolver(),
            calendar=self.calendar,
            policy=policy_fixture(),
            entry_authorities=UnavailableActualEntryAuthorityResolver(),
        )

    def read_window(
        self,
        *,
        terminal_text: str = (
            "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET; "
            "BID 100.24 ASK 100.25; STOP SET @ 97.50"
        ),
        between_text: str | None = None,
    ) -> JournalAccountCheckWindowSource:
        check = self.ingest(
            envelope(
                "message:check",
                "ACCOUNT CHECK settled_cash 5000 pending_orders 0 "
                "unlogged_positions 0 AT 10:10 ET",
                message_time="2026-08-14T10:10:00-04:00",
                received_at="2026-08-14T10:10:01-04:00",
            )
        )
        if between_text is not None:
            self.ingest(
                envelope(
                    "message:between",
                    between_text,
                    message_time="2026-08-14T10:12:00-04:00",
                    received_at="2026-08-14T10:12:01-04:00",
                )
            )
        terminal = self.ingest(
            envelope(
                "message:terminal",
                terminal_text,
                message_time="2026-08-14T10:15:00-04:00",
                received_at="2026-08-14T10:15:01-04:00",
            )
        )
        with self.journal.transaction() as transaction:
            return transaction.read_account_check_window(
                account_check_event_id=check.actions[0].event_row_id,
                terminal_event_id=terminal.actions[0].event_row_id,
            )

    def test_verified_source_issues_exact_bound_account_buy_authority(self) -> None:
        source = self.read_window()

        check, action, window = risk_module._issue_account_buy_authority(source)

        self.assertEqual(check.settled_cash, Decimal("5000"))
        self.assertEqual(check.cursor, source.after_cursor)
        self.assertEqual(action.event_id, source.terminal_action.event_id)
        self.assertEqual(action.bid, Decimal("100.24"))
        self.assertEqual(action.ask, Decimal("100.25"))
        self.assertEqual(action.user_confirmed_stop, Decimal("97.50"))
        self.assertTrue(is_issued_confirmed_buy_action(action))
        self.assertTrue(is_issued_journal_event_window(window))
        self.assertIs(window.account_check, check)
        self.assertIs(window.terminal_action, action)
        self.assertEqual(window.events, ())
        self.assertTrue(
            evaluate_account_check_window(
                check,
                action.execution_event,
                window,
            ).eligible
        )
        object.__setattr__(action, "symbol", "QQQ")
        self.assertFalse(is_issued_confirmed_buy_action(action))
        self.assertFalse(is_issued_journal_event_window(window))

    def test_dirty_commit_revokes_window_and_every_derived_authority(self) -> None:
        source = self.read_window()
        _check, action, window = risk_module._issue_account_buy_authority(source)
        settlement = risk_module._issue_settlement_ledger_from_account_window(
            event_window=window,
            calendar_resolver=self.calendar,
        )
        expected_terminal = envelope(
            "message:terminal",
            (
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET; "
                "BID 100.24 ASK 100.25; STOP SET @ 97.50"
            ),
            message_time="2026-08-14T10:15:00-04:00",
            received_at="2026-08-14T10:15:01-04:00",
        )

        with self.journal.transaction():
            self.assertEqual(
                (
                    is_verified_journal_window_source(source),
                    is_verified_journal_action_source(source.terminal_action),
                    is_issued_confirmed_buy_action(action),
                    is_issued_journal_event_window(window),
                    settlement.source_verified,
                ),
                (True, True, True, True, True),
            )
        self.ingest(expected_terminal)
        self.assertEqual(
            (
                is_verified_journal_window_source(source),
                is_verified_journal_action_source(source.terminal_action),
                is_issued_confirmed_buy_action(action),
                is_issued_journal_event_window(window),
                settlement.source_verified,
            ),
            (True, True, True, True, True),
        )
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.journal.transaction() as transaction:
                transaction.append_raw_message(
                    "message:rolled-back-window",
                    datetime.fromisoformat("2026-08-14T10:15:30-04:00"),
                    "SKIPPED QQQ",
                )
                self.assertEqual(
                    (
                        is_verified_journal_window_source(source),
                        is_verified_journal_action_source(source.terminal_action),
                        is_issued_confirmed_buy_action(action),
                        is_issued_journal_event_window(window),
                        settlement.source_verified,
                    ),
                    (False, False, False, False, False),
                )
                raise RuntimeError("rollback")
        self.assertTrue(is_verified_journal_window_source(source))
        self.assertTrue(is_verified_journal_action_source(source.terminal_action))
        self.assertTrue(is_issued_confirmed_buy_action(action))
        self.assertTrue(is_issued_journal_event_window(window))
        self.assertTrue(settlement.source_verified)

        self.ingest(
            envelope(
                "message:new-window-generation",
                "FEE SPY 0.03 AT 10:16 ET",
                message_time="2026-08-14T10:16:30-04:00",
                received_at="2026-08-14T10:16:31-04:00",
            )
        )

        self.assertEqual(
            (
                is_verified_journal_window_source(source),
                is_verified_journal_action_source(source.terminal_action),
                is_issued_confirmed_buy_action(action),
                is_issued_journal_event_window(window),
                settlement.source_verified,
            ),
            (False, False, False, False, False),
        )

    def test_pending_clarification_between_check_and_buy_fails_closed(self) -> None:
        source = self.read_window(
            between_text="I may have bought QQQ around 10:12"
        )
        check, action, window = risk_module._issue_account_buy_authority(source)

        decision = evaluate_account_check_window(
            check,
            action.execution_event,
            window,
        )

        self.assertEqual(
            [event.kind for event in window.events],
            ["PENDING_CLARIFICATION"],
        )
        self.assertFalse(decision.eligible)
        self.assertIn("PENDING_ACCOUNT_EVENT", decision.reason_codes)
        self.assertIn("INTERVENING_ACCOUNT_EVENT", decision.reason_codes)

    def test_stop_update_between_check_and_buy_fails_closed(self) -> None:
        source = self.read_window(
            between_text="STOP UPDATED QQQ @ 90 AT 10:12 ET"
        )
        check, action, window = risk_module._issue_account_buy_authority(source)

        decision = evaluate_account_check_window(
            check,
            action.execution_event,
            window,
        )

        self.assertEqual([event.kind for event in window.events], ["STOP_UPDATED"])
        self.assertFalse(decision.eligible)
        self.assertIn("INTERVENING_ACCOUNT_EVENT", decision.reason_codes)

    def test_every_strict_between_row_is_rederived_in_cursor_order(self) -> None:
        source = self.read_window(
            between_text="RECONCILE CASH -1.25 REASON broker fee AT 10:12 ET"
        )

        check, action, window = risk_module._issue_account_buy_authority(source)

        self.assertEqual(len(window.events), 1)
        event = window.events[0]
        self.assertEqual(event.kind, "RECONCILE_CASH")
        self.assertEqual(event.amount, Decimal("-1.25"))
        self.assertEqual(event.cursor, source.between_actions[0].execution_event_id)
        self.assertEqual(event.message_time, source.between_actions[0].message_time)
        self.assertEqual(event.received_at, source.between_actions[0].received_at)
        decision = evaluate_account_check_window(
            check,
            action.execution_event,
            window,
        )
        self.assertFalse(decision.eligible)
        self.assertIn("INTERVENING_ACCOUNT_EVENT", decision.reason_codes)
        self.assertFalse(is_issued_confirmed_buy_action(replace(action)))
        self.assertFalse(is_issued_journal_event_window(replace(window)))
        object.__setattr__(event, "kind", "QUOTE")
        self.assertFalse(is_issued_journal_event_window(window))

    def test_copy_replace_and_nested_mutation_never_reissue_authority(self) -> None:
        source = self.read_window()

        for forged in (
            copy.copy(source),
            replace(source),
            replace(
                source,
                terminal_action=replace(source.terminal_action),
            ),
        ):
            with self.subTest(forged=forged), self.assertRaisesRegex(
                RiskBlock,
                "^JOURNAL_ACCOUNT_WINDOW_SOURCE_UNVERIFIED$",
            ):
                risk_module._issue_account_buy_authority(forged)

        object.__setattr__(
            source.terminal_action.row_references[0],
            "row_digest",
            "0" * 64,
        )
        with self.assertRaisesRegex(
            RiskBlock,
            "^JOURNAL_ACCOUNT_WINDOW_SOURCE_UNVERIFIED$",
        ):
            risk_module._issue_account_buy_authority(source)

    def test_degraded_buy_and_partial_fill_do_not_issue_authority(self) -> None:
        cases = (
            (
                "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
                "ACCOUNT_BUY_CONFIRMATION_INCOMPLETE",
            ),
            (
                "PARTIAL FILL SPY 2 shares @ 100.25 AT 10:14 ET; "
                "ORDER order-1 TOTAL 5 shares",
                "ACCOUNT_BUY_TERMINAL_ACTION_INVALID",
            ),
        )
        for terminal_text, reason_code in cases:
            with self.subTest(terminal_text=terminal_text):
                source = self.read_window(terminal_text=terminal_text)
                with self.assertRaisesRegex(RiskBlock, f"^{reason_code}$"):
                    risk_module._issue_account_buy_authority(source)

                self.journal.close()
                temporary = tempfile.TemporaryDirectory()
                self.addCleanup(temporary.cleanup)
                self.journal = Journal.open(
                    Path(temporary.name) / "journal.sqlite3"
                )
                self.addCleanup(self.journal.close)

    def test_account_check_is_defense_in_depth_invalidating_event(self) -> None:
        source = self.read_window()
        check, action, _ = risk_module._issue_account_buy_authority(source)
        later_check = ExecutionEvent(
            kind="ACCOUNT_CHECK",
            at=datetime.fromisoformat("2026-08-14T10:12:00-04:00"),
            cursor=source.after_cursor + 1,
        )

        self.assertFalse(
            risk_module.account_check_eligible(
                check,
                action.execution_event,
                (later_check,),
            )
        )


if __name__ == "__main__":
    unittest.main()
