from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_monitor.confirmations import (
    ConfirmationEnvelope,
    ConfirmationKind,
    ConfirmationParseError,
    ParsedConfirmation,
    PendingConfirmation,
    parse_confirmation,
    parse_confirmation_batch,
    parse_confirmation_batch_or_pending,
    parse_confirmation_or_pending,
)


SESSION = date(2026, 8, 14)
ET = ZoneInfo("America/New_York")


class ConfirmationGrammarTests(unittest.TestCase):
    def test_full_buy_is_normalized_and_immutable(self) -> None:
        action = parse_confirmation(
            "BOUGHT spy 5 shares @ 100.25 AT 10:14 ET; "
            "BID 100.24 ASK 100.25; STOP SET @ 97.50",
            session_date=SESSION,
        )

        self.assertEqual(action.kind, ConfirmationKind.BUY)
        self.assertEqual(
            action.raw_text,
            "BOUGHT spy 5 shares @ 100.25 AT 10:14 ET; "
            "BID 100.24 ASK 100.25; STOP SET @ 97.50",
        )
        self.assertEqual(action.symbol, "SPY")
        self.assertEqual(action.quantity, 5)
        self.assertEqual(action.price, Decimal("100.250000"))
        self.assertEqual(action.bid, Decimal("100.240000"))
        self.assertEqual(action.ask, Decimal("100.250000"))
        self.assertEqual(action.stop, Decimal("97.500000"))
        self.assertEqual(action.event_time, datetime(2026, 8, 14, 10, 14, tzinfo=ET))
        self.assertEqual(action.missing_fields, frozenset())
        with self.assertRaises(FrozenInstanceError):
            action.symbol = "QQQ"  # type: ignore[misc]

    def test_degraded_buy_is_clear_but_identifies_missing_fields(self) -> None:
        action = parse_confirmation(
            "BOUGHT SPY 5 shares @ 100.25 AT 10:14 ET",
            session_date=SESSION,
        )

        self.assertEqual(action.kind, ConfirmationKind.BUY)
        self.assertEqual(action.missing_fields, frozenset({"bid", "ask", "stop"}))

    def test_every_anchored_form_has_a_valid_case(self) -> None:
        cases = {
            "ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET": ConfirmationKind.ACCOUNT_CHECK,
            "STOP UPDATED spy @ 98.10 AT 15:32 ET": ConfirmationKind.STOP_UPDATED,
            "STOP FILLED spy 5 shares @ 97.45 AT 10:01 ET": ConfirmationKind.STOP_FILLED,
            "SOLD spy 5 shares @ 104.00 AT 15:31 ET": ConfirmationKind.SOLD,
            "SKIPPED spy": ConfirmationKind.SKIPPED,
            "OPTION PAPER WINDOW START AT 2026-08-14T14:00:00-04:00": ConfirmationKind.OPTION_WINDOW_START,
            "OPTION PAPER REVIEW AAPL260918C00150000 BID 2.10 ASK 2.20 DELTA 0.35 OI 1000 VOLUME 100 AT 10:19 ET": ConfirmationKind.OPTION_REVIEW,
            "OPTION PAPER OPEN AAPL260918C00150000 ASK 2.20 AT 10:20 ET": ConfirmationKind.OPTION_OPEN,
            "OPTION PAPER MARK AAPL260918C00150000 BID 2.20 ASK 2.30 AT 15:40 ET": ConfirmationKind.OPTION_MARK,
            "OPTION PAPER CLOSE AAPL260918C00150000 BID 2.40 ASK 2.50 AT 15:41 ET": ConfirmationKind.OPTION_CLOSE,
            "RECONCILE CASH -100.25 REASON unrelated withdrawal AT 11:00 ET": ConfirmationKind.RECONCILE_CASH,
            "RECONCILE UNRELATED POSITION spy +2 shares @ 100.25 AT 11:01 ET": ConfirmationKind.RECONCILE_UNRELATED_POSITION,
            "RECONCILE PENDING ORDERS 0 AT 11:02 ET": ConfirmationKind.RECONCILE_PENDING_ORDERS,
            "FEE SPY 0.03 AT 15:33 ET": ConfirmationKind.FEE,
            "PARTIAL FILL spy 2 shares @ 100.25 AT 10:14 ET": ConfirmationKind.PARTIAL_FILL,
            "PARTIAL FILL spy 2 shares @ 100.25 AT 10:14 ET; ORDER rh-order:42 TOTAL 5 shares": ConfirmationKind.PARTIAL_FILL,
        }

        for text, expected in cases.items():
            with self.subTest(text=text):
                action = parse_confirmation(text, session_date=SESSION)
                self.assertEqual(action.kind, expected)

    def test_skipped_is_explicitly_a_message_time_observation(self) -> None:
        action = parse_confirmation("SKIPPED SPY", session_date=SESSION)

        self.assertIsNone(action.event_time)
        self.assertEqual(action.event_time_basis, "MESSAGE_TIME_OBSERVATION")

    def test_equity_symbols_are_segmented_ascii_and_capped_at_fifteen(self) -> None:
        for text, expected in (
            ("SKIPPED brk.b", "BRK.B"),
            ("SKIPPED bf-b", "BF-B"),
            ("SKIPPED abcdefghijklmno", "ABCDEFGHIJKLMNO"),
        ):
            with self.subTest(text=text):
                self.assertEqual(
                    parse_confirmation(text, session_date=SESSION).symbol,
                    expected,
                )

        for symbol in (
            ".SPY",
            "-SPY",
            "SPY.",
            "SPY-",
            "SPY..A",
            "SPY--A",
            "SPY.-A",
            "SPY-.A",
            "ABCDEFGHIJKLMNOP",
            "F260918C00150000",
        ):
            with self.subTest(symbol=symbol), self.assertRaises(
                ConfirmationParseError
            ):
                parse_confirmation(f"SKIPPED {symbol}", session_date=SESSION)

    def test_partial_fill_requires_explicit_group_for_authority(self) -> None:
        plain = parse_confirmation(
            "PARTIAL FILL SPY 2 shares @ 100.25 AT 10:14 ET",
            session_date=SESSION,
        )
        grouped = parse_confirmation(
            "PARTIAL FILL SPY 2 shares @ 100.25 AT 10:14 ET; "
            "ORDER rh-order:42 TOTAL 5 shares",
            session_date=SESSION,
        )

        self.assertEqual(
            plain.missing_fields,
            frozenset({"parent_order_id", "fill_group_planned_shares"}),
        )
        self.assertEqual(grouped.parent_order_id, "rh-order:42")
        self.assertEqual(grouped.fill_group_planned_shares, 5)

    def test_iso_time_preserves_delayed_economic_time(self) -> None:
        action = parse_confirmation(
            "SOLD SPY 1 shares @ 101 AT 2026-08-13T15:59:30-04:00",
            session_date=SESSION,
        )
        self.assertEqual(
            action.event_time,
            datetime.fromisoformat("2026-08-13T15:59:30-04:00"),
        )

    def test_normalizes_exact_microdollar_money(self) -> None:
        action = parse_confirmation(
            "BOUGHT SPY 1 shares @ 100.250000 AT 10:14 ET",
            session_date=SESSION,
        )
        self.assertEqual(action.price.as_tuple().exponent, -6)

    def test_invalid_forms_are_rejected_with_full_consumption(self) -> None:
        invalid = (
            " BOUGHT SPY 5 shares @ 100 AT 10:14 ET",
            "BOUGHT SPY 5 shares @ 100 AT 10:14 ET trailing",
            "BOUGHT SPY 0 shares @ 100 AT 10:14 ET",
            "BOUGHT SPY -1 shares @ 100 AT 10:14 ET",
            "BOUGHT SPY 1 shares @ 0 AT 10:14 ET",
            "BOUGHT SPY 1 shares @ NaN AT 10:14 ET",
            "BOUGHT .SPY 1 shares @ 100 AT 10:14 ET",
            "BOUGHT SPY 1 shares @ 100 AT 24:00 ET",
            "BOUGHT SPY 1 shares @ 100 AT 2026-02-30T10:14:00-05:00",
            "BOUGHT SPY 1 shares @ 100 AT 2026-08-14T10:14:00",
            "SOLD SPY 1 shares @ 1 AT 2026-08-13T15:59:30+04:60",
            "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; BID 101 ASK 100; STOP SET @ 97",
            "BOUGHT SPY 1 shares @ 100 AT 10:14 ET; BID 99 ASK 100; STOP SET @ 0",
            "ACCOUNT CHECK settled_cash -1 pending_orders 0 unlogged_positions 0 AT 10:10 ET",
            "ACCOUNT CHECK settled_cash 1 pending_orders -1 unlogged_positions 0 AT 10:10 ET",
            "STOP UPDATED SPY @ 0 AT 10:14 ET",
            "STOP UPDATED .SPY @ 1 AT 10:14 ET",
            "STOP FILLED SPY 0 shares @ 1 AT 10:14 ET",
            "STOP FILLED SPY 1 shares @ 0 AT 10:14 ET",
            "SOLD SPY 1 shares @ -1 AT 10:14 ET",
            "SOLD SPY 0 shares @ 1 AT 10:14 ET",
            "SKIPPED SPY AT 10:14 ET",
            "SKIPPED .SPY",
            "OPTION PAPER WINDOW START AT 24:00 ET",
            "OPTION PAPER WINDOW START AT not-a-time",
            "OPTION PAPER REVIEW AAPL260918C00150000 BID 1 ASK 2 DELTA 0.3 OI 1 VOLUME 1",
            "OPTION PAPER REVIEW AAPL260918C00150000 BID 1 ASK 2 DELTA 0.3 OI 1 AT 10:14 ET",
            "OPTION PAPER REVIEW NOTOCC BID 1 ASK 2 DELTA 0.3 OI 1 VOLUME 1 AT 10:14 ET",
            "OPTION PAPER REVIEW AAPL260231C00150000 BID 1 ASK 2 DELTA 0.3 OI 1 VOLUME 1 AT 10:14 ET",
            "OPTION PAPER REVIEW AAPL260918C00150000 BID 1 ASK 2 DELTA 0 OI 1 VOLUME 1 AT 10:14 ET",
            "OPTION PAPER REVIEW AAPL260918C00150000 BID 1 ASK 2 DELTA 1 OI 1 VOLUME 1 AT 10:14 ET",
            "OPTION PAPER OPEN NOTOCC ASK 2 AT 10:14 ET",
            "OPTION PAPER OPEN AAPL260231C00150000 ASK 2 AT 10:14 ET",
            "OPTION PAPER OPEN AAPL260918C00150000 ASK 0 AT 10:14 ET",
            "OPTION PAPER OPEN AAPL260918C00150000 BID 1 ASK 2 DELTA 0.3 OI 1 VOLUME 1 AT 10:14 ET",
            "OPTION PAPER MARK AAPL260918C00150000 BID 2 ASK 1 AT 10:14 ET",
            "OPTION PAPER MARK AAPL260918C00150000 BID 0 ASK 1 AT 10:14 ET",
            "OPTION PAPER CLOSE AAPL260918C00150000 BID 2 ASK 1 AT 10:14 ET",
            "OPTION PAPER CLOSE NOTOCC BID 1 ASK 2 AT 10:14 ET",
            "RECONCILE CASH 100 REASON missing sign AT 10:14 ET",
            "RECONCILE CASH +0 REASON no-op AT 10:14 ET",
            "RECONCILE UNRELATED POSITION SPY 2 shares @ 1 AT 10:14 ET",
            "RECONCILE UNRELATED POSITION SPY +0 shares @ 1 AT 10:14 ET",
            "RECONCILE PENDING ORDERS -1 AT 10:14 ET",
            "RECONCILE PENDING ORDERS 1 AT 24:00 ET",
            "FEE SPY 0 AT 10:14 ET",
            "FEE .SPY 1 AT 10:14 ET",
            "FEE A260231C00150000 1 AT 10:14 ET",
            "FEE A260918C00000000 1 AT 10:14 ET",
            "PARTIAL FILL SPY 2 shares @ 1 AT 10:14 ET; ORDER id TOTAL 1 shares",
            "PARTIAL FILL SPY 2 shares @ 1 AT 10:14 ET; ORDER bad/id TOTAL 2 shares",
            "",
        )

        for text in invalid:
            with self.subTest(text=text), self.assertRaises(ConfirmationParseError):
                parse_confirmation(text, session_date=SESSION)

    def test_pending_wrapper_retains_ambiguous_raw_text(self) -> None:
        text = "I bought some SPY around one hundred"
        pending = parse_confirmation_or_pending(text, session_date=SESSION)

        self.assertIsInstance(pending, PendingConfirmation)
        self.assertEqual(pending.raw_text, text)
        self.assertEqual(pending.reason_code, "UNRECOGNIZED_CONFIRMATION")

    def test_pending_wrapper_returns_a_parsed_confirmation_for_valid_input(self) -> None:
        result = parse_confirmation_or_pending("SKIPPED SPY", session_date=SESSION)

        self.assertIsInstance(result, ParsedConfirmation)

    def test_batch_is_lf_only_and_atomic_on_ambiguous_input(self) -> None:
        text = "ACCOUNT CHECK settled_cash 5000 pending_orders 0 unlogged_positions 0 AT 10:10 ET\nSKIPPED SPY"
        actions = parse_confirmation_batch(text, session_date=SESSION)
        self.assertEqual(
            tuple(action.kind for action in actions),
            (ConfirmationKind.ACCOUNT_CHECK, ConfirmationKind.SKIPPED),
        )
        for invalid in (
            text.replace("\n", "\r\n"),
            text + "\n",
            text.replace("\n", "\n\n"),
            "SKIPPED S\u0420Y",  # Cyrillic er, not ASCII P.
            "SKIPPED SPY\x00",
        ):
            with self.subTest(invalid=repr(invalid)), self.assertRaises(
                ConfirmationParseError
            ):
                parse_confirmation_batch(invalid, session_date=SESSION)
        pending = parse_confirmation_or_pending(
            "SKIPPED SPY\nmaybe bought QQQ", session_date=SESSION
        )
        self.assertIsInstance(pending, PendingConfirmation)

    def test_confirmation_batch_has_a_strict_sixty_four_action_limit(self) -> None:
        supported = "\n".join("SKIPPED SPY" for _ in range(64))
        oversized = supported + "\nSKIPPED QQQ"

        self.assertEqual(
            len(parse_confirmation_batch(supported, session_date=SESSION)),
            64,
        )
        with self.assertRaisesRegex(
            ConfirmationParseError,
            "CONFIRMATION_BATCH_TOO_LARGE",
        ):
            parse_confirmation_batch(oversized, session_date=SESSION)
        with self.assertRaisesRegex(
            ConfirmationParseError,
            "CONFIRMATION_BATCH_TOO_LARGE",
        ):
            parse_confirmation_batch_or_pending(
                oversized,
                session_date=SESSION,
            )

    def test_envelope_enforces_distinct_aware_knowledge_times(self) -> None:
        message_time = datetime.fromisoformat("2026-08-14T10:20:00-04:00")
        received_at = datetime.fromisoformat("2026-08-14T10:20:01-04:00")
        envelope = ConfirmationEnvelope(
            message_id="message:1",
            message_time=message_time,
            received_at=received_at,
            text="SOLD SPY 1 shares @ 101 AT 10:14 ET",
            session_date=SESSION,
        )
        self.assertEqual(envelope.received_at, received_at)
        with self.assertRaises(ValueError):
            ConfirmationEnvelope(
                message_id="message:2",
                message_time=received_at,
                received_at=message_time,
                text="SKIPPED SPY",
                session_date=SESSION,
            )

    def test_envelope_binds_shorthand_to_the_message_et_session_date(self) -> None:
        with self.assertRaisesRegex(ValueError, "SESSION_DATE_MESSAGE_TIME_MISMATCH"):
            ConfirmationEnvelope(
                message_id="message:backdated-shorthand",
                message_time=datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                received_at=datetime.fromisoformat("2026-08-14T10:20:01-04:00"),
                text="SOLD SPY 1 shares @ 101 AT 10:14 ET",
                session_date=date(2026, 8, 13),
            )

    def test_envelope_rejects_explicit_economic_time_after_message_time(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "CONFIRMATION_SOURCE_TIME_OUT_OF_ORDER"
        ):
            ConfirmationEnvelope(
                message_id="message:future-event",
                message_time=datetime.fromisoformat("2026-08-14T10:20:00-04:00"),
                received_at=datetime.fromisoformat("2026-08-14T10:20:01-04:00"),
                text="SOLD SPY 1 shares @ 101 AT 2026-08-14T10:20:01-04:00",
                session_date=SESSION,
            )

    def test_unicode_is_preserved_only_in_reconcile_reason_text(self) -> None:
        action = parse_confirmation(
            "RECONCILE CASH -10 REASON depósito manual AT 11:00 ET",
            session_date=SESSION,
        )

        self.assertEqual(action.reason, "depósito manual")
        for text in (
            "SKIPPED SP\u0178",
            "RECONCILE CASH -10 REASON depósito\u200bmanual AT 11:00 ET",
        ):
            with self.subTest(text=text), self.assertRaises(ConfirmationParseError):
                parse_confirmation(text, session_date=SESSION)

    def test_session_date_is_validated_even_for_timeless_action(self) -> None:
        with self.assertRaises(ConfirmationParseError):
            parse_confirmation("SKIPPED SPY", session_date="bad")  # type: ignore[arg-type]

    def test_direct_fee_occ_must_be_semantically_valid_and_normalized(self) -> None:
        at = datetime(2026, 8, 14, 10, 14, tzinfo=ET)
        for asset_id in ("A260231C00150000", "A260918C00000000", "spy"):
            with self.subTest(asset_id=asset_id), self.assertRaises(ValueError):
                ParsedConfirmation(
                    kind=ConfirmationKind.FEE,
                    raw_text="FEE",
                    event_time=at,
                    amount=Decimal("1"),
                    asset_id=asset_id,
                )

    def test_option_delta_is_exactly_micro_precision_and_canonical(self) -> None:
        at = datetime(2026, 8, 14, 10, 14, tzinfo=ET)
        values = {
            "kind": ConfirmationKind.OPTION_REVIEW,
            "raw_text": "OPTION PAPER REVIEW",
            "event_time": at,
            "occ_symbol": "AAPL260918C00150000",
            "bid": Decimal("1"),
            "ask": Decimal("2"),
            "open_interest": 1,
            "volume": 1,
        }
        canonical = ParsedConfirmation(delta=Decimal("0.3500000"), **values)

        self.assertEqual(canonical.delta, Decimal("0.350000"))
        self.assertEqual(canonical.delta.as_tuple().exponent, -6)
        with self.assertRaisesRegex(ValueError, "INVALID_OPTION_DELTA"):
            ParsedConfirmation(delta=Decimal("0.1234567"), **values)
        with self.assertRaises(ConfirmationParseError):
            parse_confirmation(
                "OPTION PAPER REVIEW AAPL260918C00150000 BID 1 ASK 2 "
                "DELTA 0.1234567 OI 1 VOLUME 1 AT 10:14 ET",
                session_date=SESSION,
            )

    def test_parsed_type_rejects_direct_inconsistent_construction(self) -> None:
        with self.assertRaises(ValueError):
            ParsedConfirmation(
                kind=ConfirmationKind.BUY,
                raw_text="x",
                event_time=datetime(2026, 8, 14, 10, 14, tzinfo=ET),
                symbol="spy",
                quantity=1,
                price=Decimal("1"),
            )

    def test_every_kind_rejects_a_direct_incomplete_shape(self) -> None:
        for kind in ConfirmationKind:
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                ParsedConfirmation(
                    kind=kind,
                    raw_text="not the normalized action",
                    event_time=None,
                )


if __name__ == "__main__":
    unittest.main()
