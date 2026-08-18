from __future__ import annotations

import unittest
import inspect
from types import SimpleNamespace
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import stock_monitor.validation as validation_module
from stock_monitor.phase1 import EquityPoint, SignalStatus
from stock_monitor.validation import (
    AdherenceSummary,
    Phase1Trade,
    Phase1Window,
    PromotionDecision,
    PromotionStatus,
    PublishedSignalDisposition,
    evaluate_phase1,
)


ET = ZoneInfo("America/New_York")
START = date(2026, 7, 20)
_DEFAULT_TERMINAL_EVIDENCE = object()
_TERMINAL_EVIDENCE_KIND = {
    SignalStatus.NOT_TRIGGERED: "SESSION_COMPLETION",
    SignalStatus.NOT_FILLED_LIMIT: "SESSION_COMPLETION",
    SignalStatus.UNRESOLVED: "SESSION_COMPLETION",
    SignalStatus.EXPIRED: "EXPIRY_DEADLINE",
    SignalStatus.INVALIDATED: "SIGNAL_EVIDENCE",
    SignalStatus.SHADOW_FILLED_INFORMATIONAL: "SHADOW_FILL",
    SignalStatus.CLOSED: "CLOSED_TRADE",
}


def equity(
    day_offset: int,
    value: str,
    *,
    ledger_name: str,
    external_cash_flow: str = "0",
) -> EquityPoint:
    day = START + timedelta(days=day_offset)
    return EquityPoint(
        ledger_name=ledger_name,
        at=datetime(day.year, day.month, day.day, 16, 0, tzinfo=ET),
        cash=Decimal(value),
        positions_value=Decimal("0"),
        equity=Decimal(value),
        external_cash_flow=Decimal(external_cash_flow),
    )


def open_sessions(elapsed_days: int) -> tuple[date, ...]:
    return tuple(
        START + timedelta(days=offset)
        for offset in range(elapsed_days + 1)
        if (START + timedelta(days=offset)).weekday() < 5
    )


def curve(
    elapsed_days: int,
    terminal_drawdown: str,
    *,
    ledger_name: str,
) -> tuple[EquityPoint, ...]:
    sessions = open_sessions(elapsed_days)
    terminal = sessions[-1]
    return tuple(
        equity(
            (session - START).days,
            str(
                Decimal("5000") - Decimal(terminal_drawdown)
                if session == terminal
                else Decimal("5000")
            ),
            ledger_name=ledger_name,
        )
        for session in sessions
    )


def trade(
    index: int,
    *,
    role: str = "PRIMARY",
    net_r: str = "0.1",
    triggered: bool = True,
    filled: bool = True,
    closed: bool = True,
    record_complete: bool = True,
    fill_price: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
    filled_at: datetime | None | object = _DEFAULT_TERMINAL_EVIDENCE,
    source_id: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
    source_digest: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
) -> Phase1Trade:
    informational_shadow = role == "WATCHLIST_SHADOW" and filled and not closed
    if fill_price is _DEFAULT_TERMINAL_EVIDENCE:
        fill_price = "100.10" if informational_shadow else None
    if filled_at is _DEFAULT_TERMINAL_EVIDENCE:
        filled_at = (
            datetime(2026, 8, 14, 9, 37, tzinfo=ET)
            if informational_shadow
            else None
        )
    if source_id is _DEFAULT_TERMINAL_EVIDENCE:
        source_id = f"shadow-fill:{index}" if informational_shadow else None
    if source_digest is _DEFAULT_TERMINAL_EVIDENCE:
        source_digest = f"{index + 1:064x}" if informational_shadow else None
    return Phase1Trade(
        signal_id=f"sig-{index}",
        role=role,
        triggered=triggered,
        filled=filled,
        closed=closed,
        record_complete=record_complete,
        net_r=Decimal(net_r) if closed else None,
        fill_price=(
            Decimal(fill_price) if isinstance(fill_price, str) else fill_price
        ),
        filled_at=filled_at,
        source_id=source_id,
        source_digest=source_digest,
    )


def disposition(
    index: int,
    *,
    role: str = "PRIMARY",
    status: SignalStatus = SignalStatus.CLOSED,
    timestamps_complete: bool = True,
    source_evidence_complete: bool = True,
    session_complete: bool | None = None,
    terminal_evidence_kind: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
    terminal_evidence_digest: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
    terminal_source_id: str | None | object = _DEFAULT_TERMINAL_EVIDENCE,
) -> PublishedSignalDisposition:
    evidence_kind = _TERMINAL_EVIDENCE_KIND.get(status)
    if session_complete is None:
        session_complete = status not in {
            SignalStatus.EXPIRED,
            SignalStatus.INVALIDATED,
        }
    if terminal_evidence_kind is _DEFAULT_TERMINAL_EVIDENCE:
        terminal_evidence_kind = evidence_kind
    if terminal_evidence_digest is _DEFAULT_TERMINAL_EVIDENCE:
        terminal_evidence_digest = (
            f"{index + 1:064x}" if evidence_kind is not None else None
        )
    if terminal_source_id is _DEFAULT_TERMINAL_EVIDENCE:
        terminal_source_id = (
            f"{evidence_kind.lower()}:{index}"
            if evidence_kind is not None
            else None
        )
    return PublishedSignalDisposition(
        signal_id=f"sig-{index}",
        role=role,
        status=status,
        timestamps_complete=timestamps_complete,
        source_evidence_complete=source_evidence_complete,
        session_complete=session_complete,
        terminal_evidence_kind=terminal_evidence_kind,
        terminal_evidence_digest=terminal_evidence_digest,
        terminal_source_id=terminal_source_id,
    )


def window(
    *,
    closed_trades: int = 20,
    elapsed_days: int = 28,
    net_rs: tuple[str, ...] | None = None,
    adherence: AdherenceSummary | None = None,
    canonical_drawdown: str = "0",
    actual_drawdown: str = "0",
    extra_trades: tuple[Phase1Trade, ...] = (),
    extra_signals: tuple[PublishedSignalDisposition, ...] = (),
    hard_breach_codes: tuple[str, ...] = (),
) -> Phase1Window:
    values = net_rs or tuple("0.1" for _ in range(closed_trades))
    trades = tuple(
        trade(index, net_r=values[index]) for index in range(closed_trades)
    ) + extra_trades
    signals = tuple(disposition(index) for index in range(closed_trades)) + extra_signals
    return Phase1Window(
        started_on=START,
        as_of=START + timedelta(days=elapsed_days),
        expected_open_sessions=open_sessions(elapsed_days),
        trades=trades,
        published_signals=signals,
        canonical_equity=curve(
            elapsed_days,
            canonical_drawdown,
            ledger_name="CANONICAL",
        ),
        actual_equity=curve(
            elapsed_days,
            actual_drawdown,
            ledger_name="ACTUAL",
        ),
        adherence=adherence or AdherenceSummary(90, 100),
        hard_breach_codes=hard_breach_codes,
    )


class Phase1PromotionTests(unittest.TestCase):
    def test_adherence_authority_has_one_source_bound_no_selector_issuer(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(validation_module, "Phase1AdherenceCheckDecision")
        )
        self.assertTrue(hasattr(validation_module, "Phase1AdherenceAuthority"))
        self.assertTrue(
            hasattr(
                validation_module,
                "_issue_phase1_adherence_from_journal_source",
            )
        )
        self.assertTrue(
            hasattr(
                validation_module,
                "is_issued_phase1_adherence_authority",
            )
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    validation_module._issue_phase1_adherence_from_journal_source
                ).parameters
            ),
            ("source", "calendar_resolver", "policy"),
        )
        authority_fields = set(
            validation_module.Phase1AdherenceAuthority.__dataclass_fields__
        )
        self.assertTrue(
            {
                "validation_window_id",
                "signal_id",
                "role",
                "checks",
                "evaluated_at",
                "query_cutoff",
                "source_digest",
                "authority_digest",
            }.issubset(authority_fields)
        )
        self.assertFalse(
            hasattr(validation_module, "register_phase1_adherence_authority")
        )
        self.assertFalse(
            hasattr(validation_module, "_register_phase1_adherence_authority")
        )
        self.assertFalse(
            validation_module.is_issued_phase1_adherence_authority(
                SimpleNamespace()
            )
        )
        with self.assertRaisesRegex(
            validation_module.ValidationError,
            "PHASE1_ADHERENCE_REVIEW_SOURCE_UNVERIFIED",
        ):
            validation_module._issue_phase1_adherence_from_journal_source(
                SimpleNamespace(),
                calendar_resolver=SimpleNamespace(),
                policy=SimpleNamespace(),
            )

    def test_fixed_adherence_checklist_has_deterministic_applicability(self) -> None:
        self.assertTrue(
            hasattr(validation_module, "_PHASE1_ADHERENCE_CHECK_NAMES")
        )
        self.assertTrue(
            hasattr(validation_module, "_phase1_adherence_applicability")
        )
        expected_names = (
            "DATA_CALENDAR_UNIVERSE_FRESHNESS",
            "HARD_ELIGIBILITY_GATES",
            "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
            "VALID_TRIGGER_TIMING",
            "ENTRY_AND_SPREAD_COMPLIANCE",
            "POSITION_SIZE_EXPOSURE_AND_RISK",
            "STOP_STATE",
            "EXIT_RULE",
            "CIRCUIT_BREAKER_BEHAVIOR",
            "RECORD_COMPLETENESS",
        )
        self.assertEqual(
            validation_module._PHASE1_ADHERENCE_CHECK_NAMES,
            expected_names,
        )
        cases = (
            (
                "PRIMARY",
                False,
                False,
                False,
                {
                    "DATA_CALENDAR_UNIVERSE_FRESHNESS",
                    "HARD_ELIGIBILITY_GATES",
                    "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
                    "CIRCUIT_BREAKER_BEHAVIOR",
                    "RECORD_COMPLETENESS",
                },
            ),
            (
                "PRIMARY",
                True,
                False,
                False,
                {
                    "DATA_CALENDAR_UNIVERSE_FRESHNESS",
                    "HARD_ELIGIBILITY_GATES",
                    "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
                    "VALID_TRIGGER_TIMING",
                    "CIRCUIT_BREAKER_BEHAVIOR",
                    "RECORD_COMPLETENESS",
                },
            ),
            (
                "PRIMARY",
                True,
                True,
                False,
                {
                    "DATA_CALENDAR_UNIVERSE_FRESHNESS",
                    "HARD_ELIGIBILITY_GATES",
                    "SCORE_ARITHMETIC_AND_PRIMARY_SELECTION",
                    "VALID_TRIGGER_TIMING",
                    "ENTRY_AND_SPREAD_COMPLIANCE",
                    "POSITION_SIZE_EXPOSURE_AND_RISK",
                    "STOP_STATE",
                    "CIRCUIT_BREAKER_BEHAVIOR",
                    "RECORD_COMPLETENESS",
                },
            ),
            ("PRIMARY", True, True, True, set(expected_names)),
            ("WATCHLIST_SHADOW", True, True, False, set()),
        )
        for role, triggered, filled, closed, applicable in cases:
            with self.subTest(
                role=role,
                triggered=triggered,
                filled=filled,
                closed=closed,
            ):
                actual = validation_module._phase1_adherence_applicability(
                    role=role,
                    triggered=triggered,
                    filled=filled,
                    closed=closed,
                )
                self.assertEqual(
                    {name for name, value in actual if value},
                    applicable,
                )
                self.assertEqual(
                    tuple(name for name, _value in actual),
                    expected_names,
                )

    def test_nineteen_trades_fails_and_twenty_passes(self) -> None:
        nineteen = evaluate_phase1(window(closed_trades=19))
        twenty = evaluate_phase1(window(closed_trades=20))

        self.assertFalse(nineteen.passed)
        self.assertEqual(nineteen.status, PromotionStatus.IN_PROGRESS)
        self.assertIn("MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET", nineteen.reason_codes)
        self.assertTrue(twenty.passed)
        self.assertEqual(twenty.closed_primary_trades, 20)

    def test_twenty_trades_before_four_weeks_does_not_pass(self) -> None:
        day_27 = evaluate_phase1(window(elapsed_days=27))
        day_28 = evaluate_phase1(window(elapsed_days=28))

        self.assertFalse(day_27.passed)
        self.assertIn("MINIMUM_ELAPSED_DAYS_NOT_MET", day_27.reason_codes)
        self.assertTrue(day_28.passed)

    def test_mean_r_must_be_strictly_positive(self) -> None:
        zero = evaluate_phase1(
            window(net_rs=tuple("0" for _ in range(20)))
        )
        positive = evaluate_phase1(
            window(net_rs=("0.000001",) + tuple("0" for _ in range(19)))
        )

        self.assertFalse(zero.passed)
        self.assertIn("MEAN_NET_R_NOT_POSITIVE", zero.reason_codes)
        self.assertTrue(positive.passed)
        self.assertEqual(positive.mean_net_r, Decimal("0.00000005"))

    def test_net_r_is_bounded_before_mean_evaluation(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PHASE1_NET_R"):
            Phase1Trade(
                signal_id="sig-unbounded-r",
                role="PRIMARY",
                triggered=True,
                filled=True,
                closed=True,
                record_complete=True,
                net_r=Decimal("1e999999999"),
            )

    def test_adherence_8999_percent_fails_and_90_percent_passes(self) -> None:
        below = evaluate_phase1(
            window(adherence=AdherenceSummary(8999, 10000))
        )
        boundary = evaluate_phase1(
            window(adherence=AdherenceSummary(9, 10))
        )

        self.assertFalse(below.passed)
        self.assertEqual(below.adherence, Decimal("0.8999"))
        self.assertIn("ADHERENCE_BELOW_90_PERCENT", below.reason_codes)
        self.assertTrue(boundary.passed)
        self.assertEqual(boundary.adherence, Decimal("0.9"))

    def test_adherence_counts_are_bounded_before_ratio_evaluation(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_ADHERENCE_COUNTS"):
            AdherenceSummary(10**5000, 10**5000)

    def test_incomplete_primary_trades_and_shadows_do_not_count(self) -> None:
        incomplete = trade(19, record_complete=False)
        shadow = trade(
            20,
            role="WATCHLIST_SHADOW",
            triggered=True,
            filled=True,
            closed=False,
        )
        result = evaluate_phase1(
            window(
                closed_trades=19,
                extra_trades=(incomplete, shadow),
                extra_signals=(
                    disposition(19),
                    disposition(
                        20,
                        role="WATCHLIST_SHADOW",
                        status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
                    ),
                ),
            )
        )

        self.assertEqual(result.closed_primary_trades, 19)
        self.assertFalse(result.passed)

    def test_shadow_informational_fill_is_final_but_not_countable(self) -> None:
        shadow_trade = trade(
            20,
            role="WATCHLIST_SHADOW",
            triggered=True,
            filled=True,
            closed=False,
        )
        shadow_disposition = disposition(
            20,
            role="WATCHLIST_SHADOW",
            status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        )

        result = evaluate_phase1(
            window(
                extra_trades=(shadow_trade,),
                extra_signals=(shadow_disposition,),
            )
        )

        self.assertTrue(shadow_disposition.complete)
        self.assertEqual(shadow_trade.fill_price, Decimal("100.10"))
        self.assertEqual(
            shadow_trade.filled_at,
            datetime(2026, 8, 14, 9, 37, tzinfo=ET),
        )
        self.assertEqual(shadow_trade.source_id, "shadow-fill:20")
        self.assertRegex(shadow_trade.source_digest or "", r"[0-9a-f]{64}\Z")
        self.assertTrue(result.passed)
        self.assertEqual(result.closed_primary_trades, 20)
        self.assertEqual(result.mean_net_r, Decimal("0.1"))

    def test_shadow_informational_fill_accepts_only_triggered_filled_open(self) -> None:
        shadow_disposition = disposition(
            20,
            role="WATCHLIST_SHADOW",
            status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        )
        invalid_trades = (
            trade(
                20,
                role="WATCHLIST_SHADOW",
                triggered=False,
                filled=False,
                closed=False,
            ),
            trade(
                20,
                role="WATCHLIST_SHADOW",
                triggered=True,
                filled=False,
                closed=False,
            ),
            trade(
                20,
                role="WATCHLIST_SHADOW",
                triggered=True,
                filled=True,
                closed=True,
            ),
        )

        for invalid_trade in invalid_trades:
            with self.subTest(trade=invalid_trade), self.assertRaisesRegex(
                ValueError,
                "TRADE_SIGNAL_STATE_MISMATCH",
            ):
                window(
                    extra_trades=(invalid_trade,),
                    extra_signals=(shadow_disposition,),
                )

    def test_shadow_informational_fill_requires_complete_evidence(self) -> None:
        shadow_trade = trade(
            20,
            role="WATCHLIST_SHADOW",
            triggered=True,
            filled=True,
            closed=False,
        )
        incomplete_cases = (
            disposition(
                20,
                role="WATCHLIST_SHADOW",
                status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
                timestamps_complete=False,
            ),
            disposition(
                20,
                role="WATCHLIST_SHADOW",
                status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
                source_evidence_complete=False,
            ),
            disposition(
                20,
                role="WATCHLIST_SHADOW",
                status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
                session_complete=False,
            ),
        )

        for incomplete in incomplete_cases:
            with self.subTest(incomplete=incomplete):
                result = evaluate_phase1(
                    window(
                        extra_trades=(shadow_trade,),
                        extra_signals=(incomplete,),
                    )
                )

                self.assertFalse(incomplete.complete)
                self.assertFalse(result.passed)
                self.assertEqual(result.closed_primary_trades, 20)
                self.assertEqual(result.mean_net_r, Decimal("0.1"))
                self.assertIn(
                    "INCOMPLETE_SIGNAL_DISPOSITIONS",
                    result.reason_codes,
                )

    def test_shadow_terminal_requires_exactly_one_complete_source_fill_fact(
        self,
    ) -> None:
        shadow_disposition = disposition(
            20,
            role="WATCHLIST_SHADOW",
            status=SignalStatus.SHADOW_FILLED_INFORMATIONAL,
        )
        complete = trade(
            20,
            role="WATCHLIST_SHADOW",
            triggered=True,
            filled=True,
            closed=False,
        )

        with self.assertRaisesRegex(
            ValueError,
            "SHADOW_FILL_TRADE_RECORD_INCOMPLETE",
        ):
            window(extra_signals=(shadow_disposition,))
        with self.assertRaisesRegex(
            ValueError,
            "SHADOW_FILL_TRADE_RECORD_INCOMPLETE",
        ):
            window(
                extra_trades=(replace(complete, record_complete=False),),
                extra_signals=(shadow_disposition,),
            )
        with self.assertRaisesRegex(ValueError, "DUPLICATE_PHASE1_TRADE"):
            window(
                extra_trades=(complete, complete),
                extra_signals=(shadow_disposition,),
            )
        for missing_field in (
            "fill_price",
            "filled_at",
            "source_id",
            "source_digest",
        ):
            with self.subTest(missing_field=missing_field), self.assertRaisesRegex(
                ValueError,
                "INCOMPLETE_SHADOW_FILL_FACT",
            ):
                trade(
                    20,
                    role="WATCHLIST_SHADOW",
                    triggered=True,
                    filled=True,
                    closed=False,
                    **{missing_field: None},
                )

    def test_terminal_disposition_evidence_is_status_specific(self) -> None:
        expired = disposition(20, status=SignalStatus.EXPIRED)
        invalidated = disposition(21, status=SignalStatus.INVALIDATED)

        self.assertTrue(expired.complete)
        self.assertFalse(expired.session_complete)
        self.assertEqual(expired.terminal_evidence_kind, "EXPIRY_DEADLINE")
        self.assertTrue(invalidated.complete)
        self.assertFalse(invalidated.session_complete)
        self.assertEqual(invalidated.terminal_evidence_kind, "SIGNAL_EVIDENCE")

        invalid_cases = (
            lambda: disposition(
                20,
                status=SignalStatus.EXPIRED,
                session_complete=True,
            ),
            lambda: disposition(
                20,
                status=SignalStatus.INVALIDATED,
                session_complete=True,
            ),
            lambda: disposition(
                20,
                status=SignalStatus.EXPIRED,
                terminal_evidence_kind="SIGNAL_EVIDENCE",
            ),
            lambda: disposition(
                20,
                status=SignalStatus.INVALIDATED,
                terminal_evidence_kind="EXPIRY_DEADLINE",
            ),
            lambda: disposition(
                20,
                status=SignalStatus.EXPIRED,
                terminal_evidence_digest=None,
            ),
            lambda: disposition(
                20,
                status=SignalStatus.NOT_TRIGGERED,
                terminal_source_id=None,
            ),
        )
        for build_invalid in invalid_cases:
            with self.subTest(build_invalid=build_invalid), self.assertRaisesRegex(
                ValueError,
                "INVALID_SIGNAL_TERMINAL_EVIDENCE",
            ):
                build_invalid()

    def test_untriggered_and_unfilled_primaries_are_statistics_not_trades(self) -> None:
        extras = (
            disposition(20, status=SignalStatus.NOT_TRIGGERED),
            disposition(21, status=SignalStatus.NOT_FILLED_LIMIT),
            disposition(22, status=SignalStatus.EXPIRED),
            disposition(23, status=SignalStatus.INVALIDATED),
        )
        result = evaluate_phase1(window(extra_signals=extras))

        self.assertTrue(result.passed)
        self.assertEqual(result.closed_primary_trades, 20)

    def test_every_published_primary_and_shadow_needs_complete_disposition(self) -> None:
        incomplete_cases = (
            disposition(20, role="WATCHLIST_SHADOW", status=SignalStatus.PUBLISHED),
            disposition(20, timestamps_complete=False),
            disposition(20, source_evidence_complete=False),
            disposition(20, session_complete=False),
        )
        for incomplete in incomplete_cases:
            with self.subTest(incomplete=incomplete):
                result = evaluate_phase1(window(extra_signals=(incomplete,)))
                self.assertFalse(result.passed)
                self.assertIn(
                    "INCOMPLETE_SIGNAL_DISPOSITIONS", result.reason_codes
                )
                self.assertEqual(result.status, PromotionStatus.IN_PROGRESS)

    def test_every_closed_primary_has_one_complete_closed_trade(self) -> None:
        result = evaluate_phase1(
            window(extra_signals=(disposition(20),))
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.status, PromotionStatus.IN_PROGRESS)
        self.assertIn(
            "CLOSED_PRIMARY_TRADE_RECORD_INCOMPLETE", result.reason_codes
        )

    def test_trade_facts_must_match_the_signal_disposition(self) -> None:
        contradictory_trade = trade(
            20,
            triggered=True,
            filled=True,
            closed=False,
        )
        not_triggered = disposition(20, status=SignalStatus.NOT_TRIGGERED)

        with self.assertRaisesRegex(
            ValueError,
            "TRADE_SIGNAL_STATE_MISMATCH",
        ):
            window(
                extra_trades=(contradictory_trade,),
                extra_signals=(not_triggered,),
            )

    def test_both_curves_begin_at_fixed_5000_on_window_start(self) -> None:
        current = window()
        for ledger_name in ("CANONICAL", "ACTUAL"):
            current_curve = (
                current.canonical_equity
                if ledger_name == "CANONICAL"
                else current.actual_equity
            )
            replacement_curve = (
                equity(
                    0,
                    "4999.99",
                    ledger_name=ledger_name,
                ),
                *current_curve[1:],
            )
            with self.subTest(ledger=ledger_name):
                with self.assertRaisesRegex(
                    ValueError, "PHASE1_EQUITY_MUST_START_AT_5000"
                ):
                    replace(
                        current,
                        canonical_equity=replacement_curve
                        if ledger_name == "CANONICAL"
                        else current.canonical_equity,
                        actual_equity=replacement_curve
                        if ledger_name == "ACTUAL"
                        else current.actual_equity,
                    )

    def test_equity_points_outside_prospective_window_are_rejected(self) -> None:
        current = window()
        before_start = equity(-1, "5000", ledger_name="CANONICAL")
        after_end = equity(29, "5000", ledger_name="ACTUAL")
        cases = (
            lambda: replace(
                current,
                canonical_equity=(before_start, *current.canonical_equity[1:]),
            ),
            lambda: replace(
                current,
                actual_equity=(*current.actual_equity, after_end),
            ),
        )
        for build_invalid in cases:
            with self.subTest(build_invalid=build_invalid):
                with self.assertRaisesRegex(
                    ValueError, "EQUITY_POINT_OUTSIDE_PHASE1_WINDOW"
                ):
                    build_invalid()

    def test_equity_curves_cover_each_expected_open_session_exactly(self) -> None:
        current = window()
        missing_session_curve = (
            current.canonical_equity[:5] + current.canonical_equity[6:]
        )

        with self.assertRaisesRegex(
            ValueError,
            "PHASE1_EQUITY_SESSION_COVERAGE_INCOMPLETE",
        ):
            replace(current, canonical_equity=missing_session_curve)

    def test_canonical_and_actual_drawdown_boundaries_are_exact(self) -> None:
        for ledger_name in ("CANONICAL", "ACTUAL"):
            for drawdown, expected in (
                ("249.99", True),
                ("250", True),
                ("250.01", False),
            ):
                kwargs = {
                    "canonical_drawdown": drawdown
                    if ledger_name == "CANONICAL"
                    else "0",
                    "actual_drawdown": drawdown
                    if ledger_name == "ACTUAL"
                    else "0",
                }
                with self.subTest(ledger=ledger_name, drawdown=drawdown):
                    result = evaluate_phase1(window(**kwargs))
                    self.assertEqual(result.passed, expected)
                    if not expected:
                        self.assertIn(
                            f"{ledger_name}_DRAWDOWN_BREACH",
                            result.reason_codes,
                        )

    def test_external_cash_flow_is_excluded_from_promotion_drawdown(self) -> None:
        current = window()
        actual_equity = tuple(
            equity(
                (session - START).days,
                "5000"
                if (session - START).days < 14
                else "6000"
                if (session - START).days == 14
                else "5750",
                ledger_name="ACTUAL",
                external_cash_flow="1000"
                if (session - START).days == 14
                else "0",
            )
            for session in current.expected_open_sessions
        )
        current = replace(
            current,
            actual_equity=actual_equity,
        )

        result = evaluate_phase1(current)

        self.assertTrue(result.passed)
        self.assertEqual(result.actual_max_drawdown, Decimal("250.000000"))

    def test_any_hard_breach_overrides_otherwise_passing_metrics(self) -> None:
        for code in (
            "HARD_RISK_LIMIT_BREACH",
            "HIDDEN_EXPOSURE",
            "DISCARDED_SIGNAL",
        ):
            with self.subTest(code=code):
                result = evaluate_phase1(window(hard_breach_codes=(code,)))
                self.assertFalse(result.passed)
                self.assertEqual(result.status, PromotionStatus.FAILED)
                self.assertIn(code, result.reason_codes)

    def test_promotion_dtos_are_immutable_and_reject_non_decimal_r(self) -> None:
        decision = evaluate_phase1(window())
        with self.assertRaises(FrozenInstanceError):
            decision.passed = False  # type: ignore[misc]
        with self.assertRaises(ValueError):
            Phase1Trade(
                signal_id="sig",
                role="PRIMARY",
                triggered=True,
                filled=True,
                closed=True,
                record_complete=True,
                net_r=0.1,  # type: ignore[arg-type]
            )

    def test_passed_decision_cannot_contain_failure_reasons(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DECISION"):
            PromotionDecision(
                passed=True,
                status=PromotionStatus.PASSED,
                reason_codes=("MEAN_NET_R_NOT_POSITIVE",),
                closed_primary_trades=20,
                elapsed_days=28,
                mean_net_r=Decimal("0.1"),
                adherence=Decimal("0.9"),
                canonical_max_drawdown=Decimal("0"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_passed_decision_requires_twenty_closed_primary_trades(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DECISION"):
            PromotionDecision(
                passed=True,
                status=PromotionStatus.PASSED,
                reason_codes=(),
                closed_primary_trades=19,
                elapsed_days=28,
                mean_net_r=Decimal("0.1"),
                adherence=Decimal("0.9"),
                canonical_max_drawdown=Decimal("0"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_passed_decision_requires_twenty_eight_elapsed_days(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DECISION"):
            PromotionDecision(
                passed=True,
                status=PromotionStatus.PASSED,
                reason_codes=(),
                closed_primary_trades=20,
                elapsed_days=27,
                mean_net_r=Decimal("0.1"),
                adherence=Decimal("0.9"),
                canonical_max_drawdown=Decimal("0"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_passed_decision_requires_strictly_positive_mean_r(self) -> None:
        for invalid in (None, Decimal("0"), Decimal("-0.000001")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError,
                "INVALID_PROMOTION_DECISION",
            ):
                PromotionDecision(
                    passed=True,
                    status=PromotionStatus.PASSED,
                    reason_codes=(),
                    closed_primary_trades=20,
                    elapsed_days=28,
                    mean_net_r=invalid,
                    adherence=Decimal("0.9"),
                    canonical_max_drawdown=Decimal("0"),
                    actual_max_drawdown=Decimal("0"),
                )

    def test_passed_decision_requires_ninety_percent_adherence(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DECISION"):
            PromotionDecision(
                passed=True,
                status=PromotionStatus.PASSED,
                reason_codes=(),
                closed_primary_trades=20,
                elapsed_days=28,
                mean_net_r=Decimal("0.1"),
                adherence=Decimal("0.899999"),
                canonical_max_drawdown=Decimal("0"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_passed_decision_requires_both_drawdowns_at_most_250(self) -> None:
        for field in ("canonical_max_drawdown", "actual_max_drawdown"):
            values = {
                "canonical_max_drawdown": Decimal("0"),
                "actual_max_drawdown": Decimal("0"),
            }
            values[field] = Decimal("250.000001")
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError,
                "INVALID_PROMOTION_DECISION",
            ):
                PromotionDecision(
                    passed=True,
                    status=PromotionStatus.PASSED,
                    reason_codes=(),
                    closed_primary_trades=20,
                    elapsed_days=28,
                    mean_net_r=Decimal("0.1"),
                    adherence=Decimal("0.9"),
                    **values,
                )

    def test_promotion_drawdowns_are_bounded_decimals(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DRAWDOWN"):
            PromotionDecision(
                passed=False,
                status=PromotionStatus.IN_PROGRESS,
                reason_codes=("MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET",),
                closed_primary_trades=0,
                elapsed_days=0,
                mean_net_r=None,
                adherence=Decimal("0"),
                canonical_max_drawdown=Decimal("9223372036854.775808"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_promotion_drawdowns_use_canonical_microdollars(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_PROMOTION_DRAWDOWN"):
            PromotionDecision(
                passed=False,
                status=PromotionStatus.IN_PROGRESS,
                reason_codes=("MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET",),
                closed_primary_trades=0,
                elapsed_days=0,
                mean_net_r=None,
                adherence=Decimal("0"),
                canonical_max_drawdown=Decimal("0.0000001"),
                actual_max_drawdown=Decimal("0"),
            )

    def test_promotion_drawdowns_reject_nonfinite_values(self) -> None:
        for invalid in (Decimal("NaN"), Decimal("Infinity")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError,
                "INVALID_PROMOTION_DRAWDOWN",
            ):
                PromotionDecision(
                    passed=False,
                    status=PromotionStatus.IN_PROGRESS,
                    reason_codes=("MINIMUM_CLOSED_PRIMARY_TRADES_NOT_MET",),
                    closed_primary_trades=0,
                    elapsed_days=0,
                    mean_net_r=None,
                    adherence=Decimal("0"),
                    canonical_max_drawdown=invalid,
                    actual_max_drawdown=Decimal("0"),
                )

    def test_promotion_adherence_is_a_closed_unit_interval(self) -> None:
        for invalid in (Decimal("-0.000001"), Decimal("1.000001")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError,
                "INVALID_PROMOTION_ADHERENCE",
            ):
                PromotionDecision(
                    passed=True,
                    status=PromotionStatus.PASSED,
                    reason_codes=(),
                    closed_primary_trades=20,
                    elapsed_days=28,
                    mean_net_r=Decimal("0.1"),
                    adherence=invalid,
                    canonical_max_drawdown=Decimal("0"),
                    actual_max_drawdown=Decimal("0"),
                )


if __name__ == "__main__":
    unittest.main()
