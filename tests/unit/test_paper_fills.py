from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_monitor.phase1 import (
    ExitReason,
    IntradayObservation,
    ObservationKind,
    PaperEntryResult,
    PaperExitResult,
    Phase1Error,
    Signal,
    SignalEvent,
    SignalEventKind,
    SignalExpiryDeadlineEvidence,
    SignalStatus,
    advance_signal,
    simulate_entry,
    simulate_exit,
    simulate_forced_exit,
)


ET = ZoneInfo("America/New_York")
SESSION = date(2026, 8, 17)


def at(clock: str, *, day: date = SESSION) -> datetime:
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def trade(
    sequence: int | None,
    clock: str,
    price: str,
    *,
    fresh: bool = True,
    stream_id: str = "sip:SPY:2026-08-17",
) -> IntradayObservation:
    observed_at = at(clock)
    return IntradayObservation(
        observation_id=f"trade-{sequence}-{clock}",
        stream_id=stream_id,
        feed="SIP",
        kind=ObservationKind.TRADE,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=fresh,
        trade_price=Decimal(price),
    )


def quote(
    sequence: int | None,
    clock: str,
    bid: str | None,
    ask: str | None,
    *,
    fresh: bool = True,
    stream_id: str = "sip:SPY:2026-08-17",
) -> IntradayObservation:
    observed_at = at(clock)
    return IntradayObservation(
        observation_id=f"quote-{sequence}-{clock}",
        stream_id=stream_id,
        feed="SIP",
        kind=ObservationKind.QUOTE,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=fresh,
        bid=None if bid is None else Decimal(bid),
        ask=None if ask is None else Decimal(ask),
    )


def bar(
    sequence: int | None,
    clock: str,
    *,
    open_price: str,
    high: str,
    low: str,
    close: str,
    bid: str | None,
    ask: str | None,
    fresh: bool = True,
    session_open: bool = False,
) -> IntradayObservation:
    observed_at = at(clock)
    return IntradayObservation(
        observation_id=f"bar-{sequence}-{clock}",
        stream_id="sip:SPY:2026-08-17",
        feed="SIP",
        kind=ObservationKind.BAR,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=fresh,
        bid=None if bid is None else Decimal(bid),
        ask=None if ask is None else Decimal(ask),
        open_price=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close_price=Decimal(close),
        session_open=session_open,
    )


def signal() -> Signal:
    return Signal(
        signal_id="2026-08-17:SPY",
        symbol="SPY",
        role="PRIMARY",
        publication_session=SESSION,
        published_at=at("08:45"),
    )


def shadow_signal() -> Signal:
    return Signal(
        signal_id="2026-08-17:QQQ",
        symbol="QQQ",
        role="WATCHLIST_SHADOW",
        publication_session=SESSION,
        published_at=at("08:45"),
    )


def event(
    kind: SignalEventKind,
    clock: str,
    *,
    day: date = SESSION,
    suffix: str = "1",
    session_complete: bool = False,
    exit_observation_id: str | None = None,
    exit_authority_digest: str | None = None,
    shares: int | None = None,
    price: Decimal | None = None,
    recommended_stop: Decimal | None = None,
    expiry_evidence: SignalExpiryDeadlineEvidence | None = None,
) -> SignalEvent:
    return SignalEvent(
        event_id=f"{kind.value.lower()}-{suffix}",
        kind=kind,
        at=at(clock, day=day),
        source_id=f"source-{kind.value.lower()}-{suffix}",
        session_complete=session_complete,
        exit_observation_id=exit_observation_id,
        exit_authority_digest=exit_authority_digest,
        shares=shares,
        price=price,
        recommended_stop=recommended_stop,
        expiry_evidence=expiry_evidence,
    )


def expiry_evidence(
    *,
    signal_id: str = "2026-08-17:SPY",
    publication_session: date = SESSION,
    deadline_session: date = SESSION + timedelta(days=1),
    deadline_clock: str = "08:45",
    observed_clock: str = "08:45",
    source_id: str = "source-expire-1",
) -> SignalExpiryDeadlineEvidence:
    return SignalExpiryDeadlineEvidence(
        signal_id=signal_id,
        publication_session=publication_session,
        deadline_session=deadline_session,
        deadline_at=at(deadline_clock, day=deadline_session),
        observed_at=at(observed_clock, day=deadline_session),
        calendar_digest="c" * 64,
        source_id=source_id,
    )


class SignalLifecycleTests(unittest.TestCase):
    def test_non_finalizer_events_reject_session_complete_evidence(self) -> None:
        for kind in (
            SignalEventKind.TRIGGER_OBSERVED,
            SignalEventKind.PAPER_FILL,
            SignalEventKind.SHADOW_FILL,
            SignalEventKind.LIVE_CONFIRM,
            SignalEventKind.LIVE_SKIP,
            SignalEventKind.INVALIDATE,
            SignalEventKind.PARTIAL_EXIT,
            SignalEventKind.CLOSE,
        ):
            exit_values = (
                {
                    "exit_observation_id": "exit-observation",
                    "exit_authority_digest": "a" * 64,
                    "shares": 1,
                    "price": Decimal("101"),
                    "recommended_stop": (
                        Decimal("99")
                        if kind is SignalEventKind.PARTIAL_EXIT
                        else None
                    ),
                }
                if kind in {SignalEventKind.PARTIAL_EXIT, SignalEventKind.CLOSE}
                else {}
            )
            with self.subTest(kind=kind), self.assertRaisesRegex(
                Phase1Error,
                "UNEXPECTED_SESSION_COMPLETE_EVIDENCE",
            ):
                event(
                    kind,
                    "09:36",
                    session_complete=True,
                    **exit_values,
                )

    def test_partial_exit_carries_a_canonical_recommended_stop(self) -> None:
        lifecycle_event = SignalEvent(
            event_id="partial-with-stop",
            kind=SignalEventKind.PARTIAL_EXIT,
            at=at("10:00"),
            source_id="source-partial-with-stop",
            exit_observation_id="exit-observation",
            exit_authority_digest="a" * 64,
            shares=1,
            price=Decimal("101"),
            recommended_stop=Decimal("99"),
        )

        self.assertEqual(lifecycle_event.recommended_stop, Decimal("99.000000"))
        self.assertEqual(lifecycle_event.recommended_stop.as_tuple().exponent, -6)

    def test_partial_exit_requires_a_recommended_stop(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INCOMPLETE_RECOMMENDED_STOP",
        ):
            SignalEvent(
                event_id="partial-without-stop",
                kind=SignalEventKind.PARTIAL_EXIT,
                at=at("10:00"),
                source_id="source-partial-without-stop",
                exit_observation_id="exit-observation",
                exit_authority_digest="a" * 64,
                shares=1,
                price=Decimal("101"),
            )

    def test_partial_exit_recommended_stop_is_positive_canonical_money(
        self,
    ) -> None:
        invalid_stops = (
            99,
            Decimal("0"),
            Decimal("-0.000001"),
            Decimal("99.0000001"),
            Decimal("9223372036854.775808"),
            Decimal("NaN"),
        )
        for invalid in invalid_stops:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_RECOMMENDED_STOP",
            ):
                SignalEvent(
                    event_id="partial-invalid-stop",
                    kind=SignalEventKind.PARTIAL_EXIT,
                    at=at("10:00"),
                    source_id="source-partial-invalid-stop",
                    exit_observation_id="exit-observation",
                    exit_authority_digest="a" * 64,
                    shares=1,
                    price=Decimal("101"),
                    recommended_stop=invalid,  # type: ignore[arg-type]
                )

    def test_close_rejects_a_recommended_stop(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_RECOMMENDED_STOP",
        ):
            SignalEvent(
                event_id="close-with-stop",
                kind=SignalEventKind.CLOSE,
                at=at("15:30"),
                source_id="source-close-with-stop",
                exit_observation_id="exit-observation",
                exit_authority_digest="a" * 64,
                shares=1,
                price=Decimal("101"),
                recommended_stop=Decimal("99"),
            )

    def test_partial_exit_preserves_each_filled_open_status(self) -> None:
        awaiting = advance_signal(
            signal(),
            event(SignalEventKind.TRIGGER_OBSERVED, "09:36"),
        )
        open_signals = (
            advance_signal(
                awaiting,
                event(SignalEventKind.PAPER_FILL, "09:37", suffix="paper"),
            ),
            advance_signal(
                awaiting,
                event(SignalEventKind.LIVE_CONFIRM, "09:37", suffix="live"),
            ),
            advance_signal(
                awaiting,
                event(SignalEventKind.LIVE_SKIP, "09:37", suffix="skip"),
            ),
        )

        for index, current in enumerate(open_signals):
            with self.subTest(status=current.status):
                result = advance_signal(
                    current,
                    event(
                        SignalEventKind.PARTIAL_EXIT,
                        "10:00",
                        suffix=f"partial-{index}",
                        exit_observation_id=f"exit-observation-{index}",
                        exit_authority_digest="a" * 64,
                        shares=1,
                        price=Decimal("101"),
                        recommended_stop=Decimal("99"),
                    ),
                )
                self.assertEqual(result.status, current.status)
                self.assertEqual(
                    result.history[-1].kind,
                    SignalEventKind.PARTIAL_EXIT,
                )

    def test_partial_exit_rejects_unfilled_terminal_and_wrong_target_status(
        self,
    ) -> None:
        awaiting = advance_signal(
            signal(),
            event(SignalEventKind.TRIGGER_OBSERVED, "09:36"),
        )

        for index, current in enumerate((signal(), awaiting)):
            with self.subTest(before_fill=current.status), self.assertRaisesRegex(
                Phase1Error,
                "ILLEGAL_SIGNAL_TRANSITION",
            ):
                advance_signal(
                    current,
                    event(
                        SignalEventKind.PARTIAL_EXIT,
                        "10:00",
                        suffix=f"early-{index}",
                        exit_observation_id=f"exit-early-{index}",
                        exit_authority_digest="a" * 64,
                        shares=1,
                        price=Decimal("101"),
                        recommended_stop=Decimal("99"),
                    ),
                )

        paper = advance_signal(
            awaiting,
            event(SignalEventKind.PAPER_FILL, "09:37", suffix="paper"),
        )
        partial = advance_signal(
            paper,
            event(
                SignalEventKind.PARTIAL_EXIT,
                "10:00",
                suffix="partial",
                exit_observation_id="exit-partial",
                exit_authority_digest="b" * 64,
                shares=1,
                price=Decimal("101"),
                recommended_stop=Decimal("99"),
            ),
        )
        with self.assertRaisesRegex(
            Phase1Error,
            "SIGNAL_HISTORY_STATUS_MISMATCH",
        ):
            replace(partial, status=SignalStatus.CLOSED)

        closed = advance_signal(
            partial,
            event(
                SignalEventKind.CLOSE,
                "15:30",
                suffix="close",
                exit_observation_id="exit-close",
                exit_authority_digest="c" * 64,
                shares=1,
                price=Decimal("102"),
            ),
        )
        with self.assertRaisesRegex(Phase1Error, "ILLEGAL_SIGNAL_TRANSITION"):
            advance_signal(
                closed,
                event(
                    SignalEventKind.PARTIAL_EXIT,
                    "15:31",
                    suffix="after-close",
                    exit_observation_id="exit-after-close",
                    exit_authority_digest="d" * 64,
                    shares=1,
                    price=Decimal("103"),
                    recommended_stop=Decimal("100"),
                ),
            )

    def test_exit_events_require_the_complete_typed_evidence_quartet(
        self,
    ) -> None:
        valid = {
            "exit_observation_id": "exit-observation",
            "exit_authority_digest": "a" * 64,
            "shares": 1,
            "price": Decimal("101"),
        }
        for kind in (SignalEventKind.PARTIAL_EXIT, SignalEventKind.CLOSE):
            for missing in valid:
                values = dict(valid)
                values[missing] = None
                with self.subTest(
                    kind=kind,
                    missing=missing,
                ), self.assertRaisesRegex(
                    Phase1Error,
                    "INCOMPLETE_EXIT_EVENT_EVIDENCE",
                ):
                    event(kind, "10:00", **values)  # type: ignore[arg-type]

        invalid_values = (
            ("exit_observation_id", ""),
            ("exit_observation_id", 1),
            ("exit_authority_digest", "A" * 64),
            ("exit_authority_digest", "g" * 64),
            ("exit_authority_digest", "a" * 63),
            ("shares", True),
            ("shares", 0),
            ("shares", 9223372036854775808),
            ("price", 101),
            ("price", Decimal("0")),
            ("price", Decimal("101.0000001")),
            ("price", Decimal("9223372036854.775808")),
        )
        for field, invalid in invalid_values:
            values = dict(valid)
            values[field] = invalid
            with self.subTest(field=field, invalid=invalid), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_EXIT_EVENT_EVIDENCE",
            ):
                event(
                    SignalEventKind.PARTIAL_EXIT,
                    "10:00",
                    **values,  # type: ignore[arg-type]
                )

    def test_non_exit_event_rejects_exit_evidence(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_EXIT_EVENT_EVIDENCE",
        ):
            event(
                SignalEventKind.TRIGGER_OBSERVED,
                "09:36",
                exit_observation_id="exit-observation",
                exit_authority_digest="a" * 64,
                shares=1,
                price=Decimal("101"),
            )

    def test_non_exit_event_rejects_recommended_stop_alone(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_EXIT_EVENT_EVIDENCE",
        ):
            SignalEvent(
                event_id="trigger-with-stop",
                kind=SignalEventKind.TRIGGER_OBSERVED,
                at=at("09:36"),
                source_id="source-trigger-with-stop",
                recommended_stop=Decimal("99"),
            )

    def test_every_direct_legal_transition_is_explicit(self) -> None:
        next_day = SESSION + timedelta(days=1)
        direct = (
            (signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36"), SignalStatus.TRIGGERED_AWAITING_LIMIT),
            (signal(), event(SignalEventKind.FINALIZE_NOT_TRIGGERED, "08:45", day=next_day, session_complete=True), SignalStatus.NOT_TRIGGERED),
            (signal(), event(SignalEventKind.FINALIZE_UNRESOLVED, "08:45", day=next_day, session_complete=True), SignalStatus.UNRESOLVED),
            (
                signal(),
                event(
                    SignalEventKind.EXPIRE,
                    "08:45",
                    day=next_day,
                    expiry_evidence=expiry_evidence(),
                ),
                SignalStatus.EXPIRED,
            ),
            (signal(), event(SignalEventKind.INVALIDATE, "09:00"), SignalStatus.INVALIDATED),
        )
        awaiting = advance_signal(
            signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
        )
        direct += (
            (awaiting, event(SignalEventKind.PAPER_FILL, "09:37", suffix="2"), SignalStatus.TRIGGERED_PAPER),
            (awaiting, event(SignalEventKind.LIVE_CONFIRM, "09:37", suffix="2"), SignalStatus.LIVE_CONFIRMED),
            (awaiting, event(SignalEventKind.LIVE_SKIP, "09:37", suffix="2"), SignalStatus.SKIPPED_LIVE_TRACKED_PAPER),
            (awaiting, event(SignalEventKind.FINALIZE_NOT_FILLED, "08:45", day=next_day, suffix="2", session_complete=True), SignalStatus.NOT_FILLED_LIMIT),
            (awaiting, event(SignalEventKind.FINALIZE_UNRESOLVED, "08:45", day=next_day, suffix="2", session_complete=True), SignalStatus.UNRESOLVED),
            (
                awaiting,
                event(
                    SignalEventKind.EXPIRE,
                    "08:45",
                    day=next_day,
                    suffix="2",
                    expiry_evidence=expiry_evidence(
                        source_id="source-expire-2"
                    ),
                ),
                SignalStatus.EXPIRED,
            ),
            (awaiting, event(SignalEventKind.INVALIDATE, "09:37", suffix="2"), SignalStatus.INVALIDATED),
        )
        paper = advance_signal(
            awaiting, event(SignalEventKind.PAPER_FILL, "09:37", suffix="paper")
        )
        direct += (
            (paper, event(SignalEventKind.LIVE_CONFIRM, "09:38", suffix="3"), SignalStatus.LIVE_CONFIRMED),
            (paper, event(SignalEventKind.LIVE_SKIP, "09:38", suffix="3"), SignalStatus.SKIPPED_LIVE_TRACKED_PAPER),
            (
                paper,
                event(
                    SignalEventKind.CLOSE,
                    "15:30",
                    suffix="3",
                    exit_observation_id="exit-paper",
                    exit_authority_digest="a" * 64,
                    shares=1,
                    price=Decimal("101"),
                ),
                SignalStatus.CLOSED,
            ),
        )
        for entered_status, live_kind in (
            (SignalStatus.LIVE_CONFIRMED, SignalEventKind.LIVE_CONFIRM),
            (SignalStatus.SKIPPED_LIVE_TRACKED_PAPER, SignalEventKind.LIVE_SKIP),
        ):
            entered = advance_signal(
                awaiting, event(live_kind, "09:37", suffix=entered_status.value)
            )
            direct += (
                (
                    entered,
                    event(
                        SignalEventKind.CLOSE,
                        "15:30",
                        suffix=f"close-{entered_status.value}",
                        exit_observation_id=f"exit-{entered_status.value}",
                        exit_authority_digest="b" * 64,
                        shares=1,
                        price=Decimal("101"),
                    ),
                    SignalStatus.CLOSED,
                ),
            )

        for current, lifecycle_event, expected in direct:
            with self.subTest(current=current.status, event=lifecycle_event.kind):
                updated = advance_signal(current, lifecycle_event)
                self.assertEqual(updated.status, expected)
                self.assertEqual(updated.history[-1], lifecycle_event)
                self.assertNotEqual(id(updated), id(current))

    def test_illegal_and_terminal_transitions_fail_closed(self) -> None:
        illegal = (
            (signal(), event(SignalEventKind.PAPER_FILL, "09:36")),
            (
                signal(),
                event(
                    SignalEventKind.CLOSE,
                    "09:36",
                    exit_observation_id="exit-before-fill",
                    exit_authority_digest="c" * 64,
                    shares=1,
                    price=Decimal("101"),
                ),
            ),
            (
                advance_signal(signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")),
                event(SignalEventKind.FINALIZE_NOT_TRIGGERED, "08:45", day=SESSION + timedelta(days=1), suffix="2", session_complete=True),
            ),
        )
        closed = advance_signal(
            advance_signal(
                advance_signal(
                    signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
                ),
                event(SignalEventKind.PAPER_FILL, "09:37", suffix="2"),
            ),
            event(
                SignalEventKind.CLOSE,
                "15:30",
                suffix="3",
                exit_observation_id="exit-close",
                exit_authority_digest="d" * 64,
                shares=1,
                price=Decimal("101"),
            ),
        )
        illegal += ((closed, event(SignalEventKind.INVALIDATE, "15:31", suffix="4")),)

        for current, lifecycle_event in illegal:
            with self.subTest(current=current.status, event=lifecycle_event.kind):
                with self.assertRaisesRegex(Phase1Error, "ILLEGAL_SIGNAL_TRANSITION"):
                    advance_signal(current, lifecycle_event)

    def test_trigger_is_strictly_after_0935_and_on_publication_session(self) -> None:
        for lifecycle_event in (
            event(SignalEventKind.TRIGGER_OBSERVED, "09:35"),
            event(SignalEventKind.TRIGGER_OBSERVED, "16:01"),
            event(
                SignalEventKind.TRIGGER_OBSERVED,
                "09:36",
                day=SESSION + timedelta(days=1),
            ),
        ):
            with self.subTest(at=lifecycle_event.at):
                with self.assertRaisesRegex(Phase1Error, "INVALID_TRIGGER_TIME"):
                    advance_signal(signal(), lifecycle_event)

    def test_fill_is_bound_to_the_publication_regular_session(self) -> None:
        awaiting = advance_signal(
            signal(),
            event(SignalEventKind.TRIGGER_OBSERVED, "15:59"),
        )

        with self.assertRaisesRegex(Phase1Error, "INVALID_ENTRY_EVENT_TIME"):
            advance_signal(
                awaiting,
                event(
                    SignalEventKind.PAPER_FILL,
                    "09:30",
                    day=SESSION + timedelta(days=1),
                    suffix="next-session",
                ),
            )

    def test_session_finalization_must_happen_after_publication_date(self) -> None:
        with self.assertRaisesRegex(Phase1Error, "FINALIZATION_NOT_NEXT_PREMARKET"):
            advance_signal(
                signal(),
                event(
                    SignalEventKind.FINALIZE_NOT_TRIGGERED,
                    "16:01",
                    session_complete=True,
                ),
            )

    def test_session_finalization_must_be_premarket(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "FINALIZATION_NOT_NEXT_PREMARKET",
        ):
            advance_signal(
                signal(),
                event(
                    SignalEventKind.FINALIZE_NOT_TRIGGERED,
                    "09:36",
                    day=SESSION + timedelta(days=1),
                    session_complete=True,
                ),
            )

    def test_finalization_requires_session_complete_source_evidence(self) -> None:
        for kind in (
            SignalEventKind.FINALIZE_NOT_TRIGGERED,
            SignalEventKind.FINALIZE_UNRESOLVED,
        ):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(
                    Phase1Error, "SESSION_COMPLETE_EVIDENCE_REQUIRED"
                ):
                    advance_signal(
                        signal(),
                        event(
                            kind,
                            "08:45",
                            day=SESSION + timedelta(days=1),
                        ),
                    )

        awaiting = advance_signal(
            signal(),
            event(SignalEventKind.TRIGGER_OBSERVED, "09:36"),
        )
        with self.assertRaisesRegex(
            Phase1Error,
            "SESSION_COMPLETE_EVIDENCE_REQUIRED",
        ):
            advance_signal(
                awaiting,
                event(
                    SignalEventKind.FINALIZE_NOT_FILLED,
                    "08:45",
                    day=SESSION + timedelta(days=1),
                    suffix="not-filled",
                ),
            )

    def test_expiry_requires_deadline_evidence_not_fake_session_completion(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "EXPIRY_DEADLINE_EVIDENCE_REQUIRED",
        ):
            event(
                SignalEventKind.EXPIRE,
                "08:45",
                day=SESSION + timedelta(days=1),
            )

        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_SESSION_COMPLETE_EVIDENCE",
        ):
            event(
                SignalEventKind.EXPIRE,
                "08:45",
                day=SESSION + timedelta(days=1),
                session_complete=True,
                expiry_evidence=expiry_evidence(),
            )

        expired = advance_signal(
            signal(),
            event(
                SignalEventKind.EXPIRE,
                "08:45",
                day=SESSION + timedelta(days=1),
                expiry_evidence=expiry_evidence(),
            ),
        )
        self.assertEqual(expired.status, SignalStatus.EXPIRED)
        self.assertFalse(expired.history[-1].session_complete)
        self.assertEqual(
            expired.history[-1].expiry_evidence,
            expiry_evidence(),
        )

    def test_expiry_deadline_evidence_is_exactly_bound_to_signal_and_event(
        self,
    ) -> None:
        cases = (
            (
                replace(expiry_evidence(), signal_id="different-signal"),
                "08:45",
                "1",
                "EXPIRY_SIGNAL_MISMATCH",
            ),
            (
                replace(
                    expiry_evidence(),
                    publication_session=SESSION - timedelta(days=1),
                ),
                "08:45",
                "1",
                "EXPIRY_SESSION_MISMATCH",
            ),
            (
                expiry_evidence(source_id="different-source"),
                "08:45",
                "1",
                "EXPIRY_SOURCE_MISMATCH",
            ),
            (
                expiry_evidence(observed_clock="08:46"),
                "08:45",
                "1",
                "EXPIRY_EVENT_TIME_MISMATCH",
            ),
        )
        for evidence, clock, suffix, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(
                Phase1Error,
                reason,
            ):
                advance_signal(
                    signal(),
                    event(
                        SignalEventKind.EXPIRE,
                        clock,
                        day=SESSION + timedelta(days=1),
                        suffix=suffix,
                        expiry_evidence=evidence,
                    ),
                )

        for field, invalid in (
            ("deadline_clock", "08:44"),
            ("observed_clock", "08:44"),
            ("observed_clock", "09:36"),
        ):
            with self.subTest(field=field, invalid=invalid), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_EXPIRY_(DEADLINE|OBSERVED)_TIME",
            ):
                expiry_evidence(**{field: invalid})

    def test_non_expiry_events_reject_deadline_evidence(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_EXPIRY_DEADLINE_EVIDENCE",
        ):
            event(
                SignalEventKind.FINALIZE_NOT_TRIGGERED,
                "08:45",
                day=SESSION + timedelta(days=1),
                session_complete=True,
                expiry_evidence=expiry_evidence(
                    source_id="source-finalize_not_triggered-1"
                ),
            )

    def test_watchlist_shadow_rejects_every_canonical_entry_transition(self) -> None:
        awaiting = advance_signal(
            shadow_signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
        )
        for kind in (
            SignalEventKind.PAPER_FILL,
            SignalEventKind.LIVE_CONFIRM,
            SignalEventKind.LIVE_SKIP,
        ):
            with self.subTest(kind=kind), self.assertRaisesRegex(
                Phase1Error,
                "SHADOW_CANONICAL_FILL_PROHIBITED",
            ):
                advance_signal(
                    awaiting,
                    event(kind, "09:37", suffix="2"),
                )

    def test_watchlist_shadow_has_one_terminal_informational_fill(self) -> None:
        awaiting = advance_signal(
            shadow_signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
        )

        filled = advance_signal(
            awaiting,
            event(SignalEventKind.SHADOW_FILL, "09:37", suffix="shadow"),
        )

        self.assertEqual(
            filled.status,
            SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        )
        self.assertEqual(filled.history[-1].kind, SignalEventKind.SHADOW_FILL)
        for later_kind in (
            SignalEventKind.SHADOW_FILL,
            SignalEventKind.PARTIAL_EXIT,
            SignalEventKind.CLOSE,
        ):
            with self.subTest(later_kind=later_kind), self.assertRaisesRegex(
                Phase1Error,
                "ILLEGAL_SIGNAL_TRANSITION",
            ):
                advance_signal(
                    filled,
                    event(
                        later_kind,
                        "09:38",
                        suffix=f"later-{later_kind.value}",
                        **(
                            {
                                "exit_observation_id": "exit-shadow",
                                "exit_authority_digest": "a" * 64,
                                "shares": 1,
                                "price": Decimal("101"),
                                "recommended_stop": (
                                    Decimal("100")
                                    if later_kind is SignalEventKind.PARTIAL_EXIT
                                    else None
                                ),
                            }
                            if later_kind
                            in {SignalEventKind.PARTIAL_EXIT, SignalEventKind.CLOSE}
                            else {}
                        ),
                    ),
                )

    def test_primary_cannot_claim_an_informational_shadow_fill(self) -> None:
        awaiting = advance_signal(
            signal(), event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
        )

        with self.assertRaisesRegex(
            Phase1Error,
            "SHADOW_FILL_ROLE_MISMATCH",
        ):
            advance_signal(
                awaiting,
                event(SignalEventKind.SHADOW_FILL, "09:37", suffix="shadow"),
            )

    def test_advance_is_immutable_and_event_history_is_chronological(self) -> None:
        original = signal()
        triggered = advance_signal(
            original, event(SignalEventKind.TRIGGER_OBSERVED, "09:36")
        )
        self.assertEqual(original.status, SignalStatus.PUBLISHED)
        self.assertEqual(original.history, ())
        with self.assertRaises(FrozenInstanceError):
            triggered.status = SignalStatus.CLOSED  # type: ignore[misc]
        with self.assertRaisesRegex(Phase1Error, "EVENT_TIME_REGRESSION"):
            advance_signal(
                triggered,
                event(SignalEventKind.PAPER_FILL, "09:35", suffix="2"),
            )


class PaperEntryTests(unittest.TestCase):
    def test_entry_status_requires_the_signal_status_enum(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INVALID_PAPER_ENTRY_STATUS",
        ):
            PaperEntryResult(
                status="NOT_TRIGGERED",  # type: ignore[arg-type]
                fill_price=None,
                filled_at=None,
            )

    def test_entry_observation_ids_are_none_or_nonempty_strings(self) -> None:
        for field, invalid in (
            ("trigger_observation_id", ""),
            ("quote_observation_id", 1),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_PAPER_ENTRY_OBSERVATION_ID",
            ):
                PaperEntryResult(
                    status=SignalStatus.NOT_TRIGGERED,
                    fill_price=None,
                    filled_at=None,
                    **{field: invalid},
                )

    def test_not_triggered_entry_has_no_trigger_or_quote_metadata(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_PAPER_ENTRY_METADATA",
        ):
            PaperEntryResult(
                status=SignalStatus.NOT_TRIGGERED,
                fill_price=None,
                filled_at=None,
                trigger_at=at("09:36"),
                trigger_observation_id="trigger",
                quote_observation_id="quote",
            )

    def test_not_filled_entry_requires_paired_trigger_evidence(self) -> None:
        incomplete = (
            (None, None),
            (at("09:36"), None),
            (None, "trigger"),
        )
        for trigger_at, trigger_id in incomplete:
            with self.subTest(
                trigger_at=trigger_at,
                trigger_id=trigger_id,
            ), self.assertRaisesRegex(
                Phase1Error,
                "INCOMPLETE_PAPER_ENTRY_TRIGGER",
            ):
                PaperEntryResult(
                    status=SignalStatus.NOT_FILLED_LIMIT,
                    fill_price=None,
                    filled_at=None,
                    trigger_at=trigger_at,
                    trigger_observation_id=trigger_id,
                )

    def test_not_filled_entry_has_no_quote_metadata(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_PAPER_ENTRY_METADATA",
        ):
            PaperEntryResult(
                status=SignalStatus.NOT_FILLED_LIMIT,
                fill_price=None,
                filled_at=None,
                trigger_at=at("09:36"),
                trigger_observation_id="trigger",
                quote_observation_id="quote",
            )

    def test_unresolved_entry_requires_a_reason(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INCOMPLETE_PAPER_ENTRY_REASONS",
        ):
            PaperEntryResult(
                status=SignalStatus.UNRESOLVED,
                fill_price=None,
                filled_at=None,
            )

    def test_unresolved_entry_trigger_evidence_is_coherent(self) -> None:
        incoherent = (
            {"trigger_at": at("09:36")},
            {"trigger_observation_id": "trigger"},
            {"quote_observation_id": "quote"},
        )
        for metadata in incoherent:
            with self.subTest(metadata=metadata), self.assertRaisesRegex(
                Phase1Error,
                "INCOMPLETE_PAPER_ENTRY_TRIGGER",
            ):
                PaperEntryResult(
                    status=SignalStatus.UNRESOLVED,
                    fill_price=None,
                    filled_at=None,
                    reason_codes=("AMBIGUOUS_ENTRY",),
                    **metadata,
                )

    def test_triggered_paper_entry_has_no_failure_reasons(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_PAPER_ENTRY_REASONS",
        ):
            PaperEntryResult(
                status=SignalStatus.TRIGGERED_PAPER,
                fill_price=Decimal("100"),
                filled_at=at("09:37"),
                trigger_at=at("09:36"),
                trigger_observation_id="trigger",
                quote_observation_id="quote",
                reason_codes=("SHOULD_NOT_BE_PRESENT",),
            )

    def test_entry_reason_codes_are_an_exact_unique_string_tuple(self) -> None:
        invalid_reason_codes = (
            ["AMBIGUOUS_ENTRY"],
            ("AMBIGUOUS_ENTRY", "AMBIGUOUS_ENTRY"),
            ("",),
        )
        for reason_codes in invalid_reason_codes:
            with self.subTest(reason_codes=reason_codes), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_PAPER_ENTRY_REASONS",
            ):
                PaperEntryResult(
                    status=SignalStatus.UNRESOLVED,
                    fill_price=None,
                    filled_at=None,
                    reason_codes=reason_codes,  # type: ignore[arg-type]
                )

    def test_fill_cannot_precede_its_trigger(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INVALID_PAPER_ENTRY_TIME_ORDER",
        ):
            PaperEntryResult(
                status=SignalStatus.TRIGGERED_PAPER,
                fill_price=Decimal("100"),
                filled_at=at("09:36"),
                trigger_at=at("10:00"),
                trigger_observation_id="trigger",
                quote_observation_id="quote",
            )

    def test_gap_above_limit_without_return_is_not_filled(self) -> None:
        observations = (
            trade(1, "09:36", "102.00"),
            quote(2, "09:36", "101.99", "102.01"),
            quote(3, "15:59", "101.50", "101.52"),
        )

        result = simulate_entry(Decimal("100"), Decimal("100.10"), observations)

        self.assertEqual(result.status, SignalStatus.NOT_FILLED_LIMIT)
        self.assertIsNone(result.fill_price)

    def test_trigger_then_return_to_limit_fills_conservatively_at_limit(self) -> None:
        observations = (
            trade(1, "09:36", "102.00"),
            quote(2, "10:02", "100.07", "100.08"),
        )

        result = simulate_entry(Decimal("100"), Decimal("100.10"), observations)

        self.assertEqual(result.status, SignalStatus.TRIGGERED_PAPER)
        self.assertEqual(result.fill_price, Decimal("100.100000"))
        self.assertEqual(result.filled_at, at("10:02"))
        self.assertEqual(result.trigger_at, at("09:36"))

    def test_source_order_can_be_supplied_out_of_argument_order(self) -> None:
        observations = (
            quote(2, "10:02", "100.07", "100.08"),
            trade(1, "09:36", "102.00"),
        )

        result = simulate_entry(Decimal("100"), Decimal("100.10"), observations)

        self.assertEqual(result.status, SignalStatus.TRIGGERED_PAPER)

    def test_same_sequence_missing_sequence_or_conflicting_order_is_unresolved(self) -> None:
        cases = (
            (
                trade(1, "09:36", "102.00"),
                quote(1, "09:37", "100.08", "100.10"),
            ),
            (
                trade(None, "09:36", "102.00"),
                quote(2, "09:37", "100.08", "100.10"),
            ),
            (
                trade(2, "09:36", "102.00"),
                quote(1, "09:37", "100.08", "100.10"),
            ),
        )
        for observations in cases:
            with self.subTest(observations=observations):
                self.assertEqual(
                    simulate_entry(
                        Decimal("100"), Decimal("100.10"), observations
                    ).status,
                    SignalStatus.UNRESOLVED,
                )

    def test_normalized_cohort_ordinals_start_at_one_without_gaps(self) -> None:
        cases = (
            (
                trade(1, "09:36", "102.00"),
                quote(3, "09:37", "100.08", "100.10"),
            ),
            (
                trade(2, "09:36", "102.00"),
                quote(3, "09:37", "100.08", "100.10"),
            ),
        )
        for observations in cases:
            with self.subTest(observations=observations):
                result = simulate_entry(
                    Decimal("100"), Decimal("100.10"), observations
                )
                self.assertEqual(result.status, SignalStatus.UNRESOLVED)

    def test_duplicate_observation_identity_is_unresolved(self) -> None:
        trigger = trade(1, "09:36", "102.00")
        apparent_fill = quote(2, "09:37", "100.08", "100.10")
        apparent_fill = type(apparent_fill)(
            observation_id=trigger.observation_id,
            stream_id=apparent_fill.stream_id,
            feed=apparent_fill.feed,
            kind=apparent_fill.kind,
            at=apparent_fill.at,
            received_at=apparent_fill.received_at,
            sequence=apparent_fill.sequence,
            fresh=apparent_fill.fresh,
            bid=apparent_fill.bid,
            ask=apparent_fill.ask,
        )

        result = simulate_entry(
            Decimal("100"), Decimal("100.10"), (trigger, apparent_fill)
        )

        self.assertEqual(result.status, SignalStatus.UNRESOLVED)

    def test_observation_kind_rejects_fields_from_other_shapes(self) -> None:
        invalid_builders = (
            lambda: IntradayObservation(
                observation_id="trade-with-quote",
                stream_id="trades:SIP:SPY",
                feed="SIP",
                kind=ObservationKind.TRADE,
                at=at("09:36"),
                received_at=at("09:36") + timedelta(seconds=1),
                sequence=1,
                fresh=True,
                trade_price=Decimal("100"),
                bid=Decimal("99.99"),
            ),
            lambda: IntradayObservation(
                observation_id="quote-with-trade",
                stream_id="quotes:SIP:SPY",
                feed="SIP",
                kind=ObservationKind.QUOTE,
                at=at("09:36"),
                received_at=at("09:36") + timedelta(seconds=1),
                sequence=1,
                fresh=True,
                trade_price=Decimal("100"),
                bid=Decimal("99.99"),
                ask=Decimal("100.01"),
            ),
            lambda: IntradayObservation(
                observation_id="bar-with-trade",
                stream_id="bars:SIP:SPY",
                feed="SIP",
                kind=ObservationKind.BAR,
                at=at("15:30"),
                received_at=at("15:30") + timedelta(seconds=1),
                sequence=1,
                fresh=True,
                trade_price=Decimal("100"),
                open_price=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close_price=Decimal("100"),
            ),
        )
        for build in invalid_builders:
            with self.subTest(build=build):
                with self.assertRaisesRegex(
                    Phase1Error, "OBSERVATION_KIND_FIELD_MISMATCH"
                    ):
                    build()

    def test_non_bar_observation_cannot_claim_session_open(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INVALID_SESSION_OPEN_OBSERVATION",
        ):
            IntradayObservation(
                observation_id="trade-session-open",
                stream_id="trades:SIP:SPY",
                feed="SIP",
                kind=ObservationKind.TRADE,
                at=at("09:36"),
                received_at=at("09:36") + timedelta(seconds=1),
                sequence=1,
                fresh=True,
                trade_price=Decimal("100"),
                session_open=True,
            )

    def test_stale_observation_is_unresolved(self) -> None:
        observations = (
            trade(1, "09:36", "102.00"),
            quote(2, "10:02", "100.08", "100.10", fresh=False),
        )
        self.assertEqual(
            simulate_entry(
                Decimal("100"), Decimal("100.10"), observations
            ).status,
            SignalStatus.UNRESOLVED,
        )

    def test_delayed_historical_sip_receipt_does_not_make_source_stale(self) -> None:
        delayed_receipt = at("09:37") + timedelta(days=1)
        observations = (
            replace(
                trade(1, "09:36", "102.00"),
                received_at=delayed_receipt,
            ),
            replace(
                quote(2, "09:37", "100.08", "100.10"),
                received_at=delayed_receipt,
            ),
        )

        result = simulate_entry(
            Decimal("100"),
            Decimal("100.10"),
            observations,
        )

        self.assertEqual(result.status, SignalStatus.TRIGGERED_PAPER)
        self.assertEqual(result.quote_observation_id, observations[1].observation_id)

    def test_quote_older_than_its_trigger_is_unresolved_even_when_source_is_healthy(
        self,
    ) -> None:
        delayed_receipt = at("09:37") + timedelta(days=1)
        observations = (
            replace(
                trade(1, "09:36", "102.00"),
                received_at=delayed_receipt,
            ),
            replace(
                quote(2, "09:35", "100.08", "100.10"),
                received_at=delayed_receipt,
            ),
        )

        result = simulate_entry(
            Decimal("100"),
            Decimal("100.10"),
            observations,
        )

        self.assertEqual(result.status, SignalStatus.UNRESOLVED)
        self.assertEqual(
            result.reason_codes,
            ("NORMALIZED_SEQUENCE_TIME_CONFLICT",),
        )

    def test_normalized_cohort_order_can_join_trade_and_quote_streams(self) -> None:
        observations = (
            trade(1, "09:36", "102.00", stream_id="trades:SIP:SPY"),
            quote(
                2,
                "10:02",
                "100.08",
                "100.10",
                stream_id="quotes:SIP:SPY",
            ),
        )

        self.assertEqual(
            simulate_entry(
                Decimal("100"), Decimal("100.10"), observations
            ).status,
            SignalStatus.TRIGGERED_PAPER,
        )

    def test_trigger_at_exactly_0935_is_ignored(self) -> None:
        observations = (
            trade(1, "09:35", "102.00"),
            quote(2, "09:36", "100.08", "100.10"),
        )

        self.assertEqual(
            simulate_entry(
                Decimal("100"), Decimal("100.10"), observations
            ).status,
            SignalStatus.NOT_TRIGGERED,
        )

    def test_after_close_trade_and_quote_cannot_create_an_entry(self) -> None:
        observations = (
            trade(1, "16:01", "102.00"),
            quote(2, "16:02", "100.08", "100.10"),
        )

        self.assertEqual(
            simulate_entry(
                Decimal("100"),
                Decimal("100.10"),
                observations,
            ).status,
            SignalStatus.NOT_TRIGGERED,
        )

    def test_trigger_without_later_quote_is_unresolved(self) -> None:
        self.assertEqual(
            simulate_entry(
                Decimal("100"), Decimal("100.10"), (trade(1, "09:36", "102"),)
            ).status,
            SignalStatus.UNRESOLVED,
        )

    def test_only_zero_missing_or_crossed_post_trigger_quotes_are_unresolved(self) -> None:
        invalid_quotes = (
            quote(2, "09:37", "0", "0"),
            quote(2, "09:37", None, "100.10"),
            quote(2, "09:37", "100.20", "100.10"),
        )
        for invalid in invalid_quotes:
            with self.subTest(invalid=invalid):
                result = simulate_entry(
                    Decimal("100"),
                    Decimal("100.10"),
                    (trade(1, "09:36", "102"), invalid),
                )
                self.assertEqual(result.status, SignalStatus.UNRESOLVED)

    def test_invalid_quote_before_later_apparent_fill_stays_unresolved(self) -> None:
        result = simulate_entry(
            Decimal("100"),
            Decimal("100.10"),
            (
                trade(1, "09:36", "102"),
                quote(2, "09:37", None, None),
                quote(3, "09:38", "100.08", "100.10"),
            ),
        )

        self.assertEqual(result.status, SignalStatus.UNRESOLVED)

    def test_money_inputs_are_decimal_microdollars_and_bounded(self) -> None:
        max_money = Decimal("9223372036854.775807")
        with self.assertRaisesRegex(Phase1Error, "INVALID_ENTRY_PRICE"):
            simulate_entry(max_money + Decimal("0.000001"), max_money, ())
        with self.assertRaisesRegex(Phase1Error, "INVALID_ENTRY_PRICE"):
            simulate_entry(Decimal("100.0000001"), Decimal("100.10"), ())
        with self.assertRaisesRegex(Phase1Error, "INVALID_ENTRY_PRICE"):
            simulate_entry(Decimal("100"), Decimal("99.99"), ())


class PaperExitTests(unittest.TestCase):
    def test_forced_rule_exits_use_the_first_bar_at_or_after_the_rule_time(self) -> None:
        before_rule = bar(
            1,
            "15:49",
            open_price="20.90",
            high="21.05",
            low="20.85",
            close="21.00",
            bid="20.99",
            ask="21.01",
        )
        at_rule = bar(
            2,
            "15:50",
            open_price="20.95",
            high="21.10",
            low="20.90",
            close="21.00",
            bid="20.99",
            ask="21.01",
        )
        for exit_reason in (
            ExitReason.EVENT_EXIT_REQUIRED,
            ExitReason.THESIS_INVALIDATED,
            ExitReason.MAX_HOLD_SESSIONS_REACHED,
        ):
            with self.subTest(exit_reason=exit_reason):
                result = simulate_forced_exit(
                    Decimal("21.00"),
                    (before_rule, at_rule),
                    exit_reason=exit_reason,
                    triggered_at=at("15:50"),
                )

                self.assertEqual(result.exit_reason, exit_reason)
                self.assertEqual(result.fill_price, Decimal("20.979000"))
                self.assertEqual(result.exited_at, at_rule.at)
                self.assertEqual(result.observation_id, at_rule.observation_id)

    def test_forced_rule_exit_fails_closed_without_a_fresh_spread(self) -> None:
        stale = bar(
            1,
            "15:50",
            open_price="20.95",
            high="21.10",
            low="20.90",
            close="21.00",
            bid="20.99",
            ask="21.01",
            fresh=False,
        )

        result = simulate_forced_exit(
            Decimal("21.00"),
            (stale,),
            exit_reason=ExitReason.EVENT_EXIT_REQUIRED,
            triggered_at=at("15:50"),
        )

        self.assertEqual(result.exit_reason, ExitReason.UNRESOLVED)
        self.assertEqual(result.reason_codes, ("STALE_OBSERVATION",))

    def test_exit_bars_must_be_inside_the_regular_session(self) -> None:
        outside_session = bar(
            1,
            "16:01",
            open_price="100",
            high="105",
            low="97",
            close="101",
            bid="100.00",
            ask="100.02",
        )

        result = simulate_exit(
            Decimal("98"),
            Decimal("104"),
            (outside_session,),
        )

        self.assertEqual(result.exit_reason, ExitReason.UNRESOLVED)
        self.assertEqual(
            result.reason_codes,
            ("EXIT_BAR_OUTSIDE_REGULAR_SESSION",),
        )

    def test_session_open_exit_bar_must_be_the_opening_time_bar(self) -> None:
        mislabeled_open = bar(
            1,
            "09:31",
            open_price="95",
            high="101",
            low="94",
            close="100",
            bid="94.99",
            ask="95.01",
            session_open=True,
        )

        result = simulate_exit(
            Decimal("98"),
            Decimal("104"),
            (mislabeled_open,),
        )

        self.assertEqual(result.exit_reason, ExitReason.UNRESOLVED)
        self.assertEqual(
            result.reason_codes,
            ("INVALID_SESSION_OPEN_BAR_TIME",),
        )

    def test_exit_observation_id_is_none_or_a_nonempty_string(self) -> None:
        for invalid in ("", 1):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_PAPER_EXIT_OBSERVATION_ID",
            ):
                PaperExitResult(
                    ExitReason.NO_EXIT,
                    None,
                    None,
                    observation_id=invalid,  # type: ignore[arg-type]
                )

    def test_successful_exit_has_no_failure_reasons(self) -> None:
        for exit_reason in (
            ExitReason.STOP,
            ExitReason.TARGET,
            ExitReason.STOP_FIRST_CONSERVATIVE,
            ExitReason.GAP_STOP,
        ):
            with self.subTest(exit_reason=exit_reason), self.assertRaisesRegex(
                Phase1Error,
                "UNEXPECTED_PAPER_EXIT_REASONS",
            ):
                PaperExitResult(
                    exit_reason,
                    Decimal("100"),
                    at("15:30"),
                    observation_id="bar-1",
                    reason_codes=("SHOULD_NOT_BE_PRESENT",),
                )

    def test_unresolved_exit_requires_a_reason(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "INCOMPLETE_PAPER_EXIT_REASONS",
        ):
            PaperExitResult(ExitReason.UNRESOLVED, None, None)

    def test_unresolved_exit_has_no_observation_metadata(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_PAPER_EXIT",
        ):
            PaperExitResult(
                ExitReason.UNRESOLVED,
                None,
                None,
                observation_id="bar-1",
                reason_codes=("AMBIGUOUS_EXIT",),
            )

    def test_no_exit_has_no_failure_reasons(self) -> None:
        with self.assertRaisesRegex(
            Phase1Error,
            "UNEXPECTED_PAPER_EXIT_REASONS",
        ):
            PaperExitResult(
                ExitReason.NO_EXIT,
                None,
                None,
                reason_codes=("SHOULD_NOT_BE_PRESENT",),
            )

    def test_exit_reason_codes_are_immutable_nonempty_strings(self) -> None:
        invalid_reason_codes = (
            ["INVALID", 1],
            ("AMBIGUOUS_EXIT", "AMBIGUOUS_EXIT"),
            ("",),
        )
        for reason_codes in invalid_reason_codes:
            with self.subTest(reason_codes=reason_codes), self.assertRaisesRegex(
                Phase1Error,
                "INVALID_PAPER_EXIT_REASONS",
            ):
                PaperExitResult(
                    ExitReason.UNRESOLVED,
                    None,
                    None,
                    reason_codes=reason_codes,  # type: ignore[arg-type]
                )

    def test_same_bar_stop_and_target_assumes_stop_first_with_adverse_slippage(self) -> None:
        result = simulate_exit(
            Decimal("98"),
            Decimal("104"),
            (
                bar(
                    1,
                    "15:30",
                    open_price="100",
                    high="105",
                    low="97",
                    close="101",
                    bid="100.00",
                    ask="100.02",
                ),
            ),
        )

        self.assertEqual(result.exit_reason, ExitReason.STOP_FIRST_CONSERVATIVE)
        self.assertEqual(result.fill_price, Decimal("97.902000"))

    def test_overnight_gap_through_stop_uses_open_with_adverse_slippage(self) -> None:
        result = simulate_exit(
            Decimal("98"),
            Decimal("104"),
            (
                bar(
                    1,
                    "09:30",
                    open_price="95",
                    high="101",
                    low="94",
                    close="100",
                    bid="94.99",
                    ask="95.01",
                    session_open=True,
                ),
            ),
        )

        self.assertEqual(result.exit_reason, ExitReason.GAP_STOP)
        self.assertEqual(result.fill_price, Decimal("94.905000"))

    def test_missing_or_invalid_exit_spread_is_unresolved(self) -> None:
        for invalid in (
            bar(
                1,
                "15:30",
                open_price="100",
                high="105",
                low="99",
                close="104",
                bid=None,
                ask=None,
            ),
            bar(
                1,
                "15:30",
                open_price="100",
                high="105",
                low="99",
                close="104",
                bid="104.10",
                ask="104.00",
            ),
        ):
            with self.subTest(invalid=invalid):
                self.assertEqual(
                    simulate_exit(Decimal("98"), Decimal("104"), (invalid,)).exit_reason,
                    ExitReason.UNRESOLVED,
                )

    def test_no_boundary_touch_returns_no_exit(self) -> None:
        result = simulate_exit(
            Decimal("98"),
            Decimal("104"),
            (
                bar(
                    1,
                    "15:30",
                    open_price="100",
                    high="103",
                    low="99",
                    close="102",
                    bid="101.99",
                    ask="102.01",
                ),
            ),
        )

        self.assertEqual(result.exit_reason, ExitReason.NO_EXIT)
        self.assertIsNone(result.fill_price)


if __name__ == "__main__":
    unittest.main()
