from __future__ import annotations

import unittest
from dataclasses import dataclass, make_dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from stock_monitor.phase1 import IntradayObservation, ObservationKind

from stock_monitor.replay import (
    ReplayCase,
    ReplayDomainComponent,
    ReplayDomainResult,
    ReplayDomainStatus,
    ReplayEvaluationStatus,
    ReplayError,
    ReplayMechanicsEvidence,
    ReplayMechanicsResult,
    ReplayMechanicsStatus,
    ReplayRequest,
    ReplayResult,
    ReplayTier,
    replay_ambiguous_bar,
    replay_diagnostic,
    replay_ordered_intraday,
    replay_point_in_time,
    is_verified_historical_replay_source,
    _register_historical_replay_source,
)


ET = ZoneInfo("America/New_York")


def passing_domain_results(
    *,
    failed: ReplayDomainComponent | None = None,
) -> tuple[ReplayDomainResult, ...]:
    return tuple(
        ReplayDomainResult(
            component,
            (
                ReplayDomainStatus.FAILED
                if component is failed
                else ReplayDomainStatus.PASSED
            ),
            ("DOMAIN_CHECK_FAILED",) if component is failed else (),
        )
        for component in ReplayDomainComponent
    )


def intraday_bar(
    sequence: int,
    clock: str,
    *,
    open_price: str,
    high: str,
    low: str,
    close: str,
    bid: str,
    ask: str,
    session_open: bool = False,
) -> IntradayObservation:
    hour, minute = (int(part) for part in clock.split(":"))
    observed_at = datetime(2026, 8, 17, hour, minute, tzinfo=ET)
    return IntradayObservation(
        observation_id=f"bar-{sequence}",
        stream_id="sip:SPY:2026-08-17",
        feed="SIP",
        kind=ObservationKind.BAR,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=True,
        bid=Decimal(bid),
        ask=Decimal(ask),
        open_price=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close_price=Decimal(close),
        session_open=session_open,
    )


def intraday_trade(
    sequence: int,
    clock: str,
    price: str,
    *,
    stream_id: str = "sip:SPY:2026-08-17",
) -> IntradayObservation:
    hour, minute = (int(part) for part in clock.split(":"))
    observed_at = datetime(2026, 8, 17, hour, minute, tzinfo=ET)
    return IntradayObservation(
        observation_id=f"trade-{sequence}",
        stream_id=stream_id,
        feed="SIP",
        kind=ObservationKind.TRADE,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=True,
        trade_price=Decimal(price),
    )


def intraday_quote(
    sequence: int,
    clock: str,
    bid: str,
    ask: str,
    *,
    stream_id: str = "sip:SPY:2026-08-17",
) -> IntradayObservation:
    hour, minute = (int(part) for part in clock.split(":"))
    observed_at = datetime(2026, 8, 17, hour, minute, tzinfo=ET)
    return IntradayObservation(
        observation_id=f"quote-{sequence}",
        stream_id=stream_id,
        feed="SIP",
        kind=ObservationKind.QUOTE,
        at=observed_at,
        received_at=observed_at + timedelta(seconds=1),
        sequence=sequence,
        fresh=True,
        bid=Decimal(bid),
        ask=Decimal(ask),
    )


class ReplayLabelTests(unittest.TestCase):
    def test_diagnostic_uses_fixed_request_and_evaluates_domain_results(self) -> None:
        mechanics = replay_ambiguous_bar(
            entry=Decimal("100"),
            stop=Decimal("98"),
            target=Decimal("104"),
            high=Decimal("103"),
            low=Decimal("99"),
        )
        request = ReplayRequest(
            cases=(
                ReplayCase(
                    session_date=date(2026, 8, 17),
                    domain_results=passing_domain_results(
                        failed=ReplayDomainComponent.SIZING
                    ),
                    mechanics=mechanics,
                ),
            )
        )

        result = replay_diagnostic(request)

        self.assertIsInstance(result, ReplayResult)
        self.assertIs(result.tier, ReplayTier.DIAGNOSTIC)
        self.assertEqual(
            result.labels,
            (
                "CURRENT_LIST_SURVIVORSHIP_BIAS",
                "CURRENT_MEMBERSHIP_BIAS",
                "DIAGNOSTIC_ONLY_NOT_PERFORMANCE_VALIDATION",
            ),
        )
        self.assertEqual(result.total_dates, 1)
        self.assertEqual(result.included_dates, 1)
        self.assertIs(
            result.results[0].evaluation_status,
            ReplayEvaluationStatus.FAILED,
        )
        self.assertIs(result.results[0].mechanics, mechanics)
        self.assertEqual(
            result.evaluation_counts,
            (("FAILED", 1),),
        )

    def test_diagnostic_rejects_the_obsolete_date_sequence_api(self) -> None:
        with self.assertRaises(TypeError):
            replay_diagnostic((date(2026, 8, 17),))  # type: ignore[arg-type]


class PointInTimeAuthorityTests(unittest.TestCase):
    def test_strict_replay_requires_exact_one_to_one_date_coverage(self) -> None:
        session = date(2026, 8, 17)
        cutoff = datetime(2026, 8, 17, 16, 30, tzinfo=ET)
        requested_case = ReplayCase(
            session_date=session,
            domain_results=passing_domain_results(),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )
        matching = SimpleNamespace(
            session_date=session,
            evidence_sources=(),
            _rederive_historical_replay_case=lambda: requested_case,
        )
        extra = SimpleNamespace(
            session_date=date(2026, 8, 18),
            evidence_sources=(),
            _rederive_historical_replay_case=lambda: None,
        )
        variants = (
            ("extra", (matching, extra), 2),
            ("duplicate", (matching, matching), 2),
            ("missing", (), 1),
            ("bad_expected_count", (matching,), 2),
        )

        for name, date_sources, expected_date_count in variants:
            source = SimpleNamespace(
                tier="STRICT_POINT_IN_TIME",
                query_cutoff=cutoff,
                expected_date_count=expected_date_count,
                date_sources=date_sources,
            )
            with self.subTest(name=name), patch(
                "stock_monitor.replay.is_verified_historical_replay_source",
                return_value=True,
            ):
                result = replay_point_in_time(
                    ReplayRequest(
                        cases=(requested_case,),
                        historical_source=source,
                    )
                )
                self.assertEqual(result.included_dates, 0)
                self.assertEqual(
                    result.results[0].reason_codes,
                    ("HISTORICAL_REPLAY_DATE_COVERAGE_MISMATCH",),
                )

    def test_incomplete_source_counts_every_missing_evidence_role(self) -> None:
        session = date(2026, 8, 17)
        cutoff = datetime(2026, 8, 17, 16, 30, tzinfo=ET)
        requested_case = ReplayCase(
            session_date=session,
            domain_results=passing_domain_results(),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )
        date_source = SimpleNamespace(
            session_date=session,
            report_cutoff=cutoff,
            expected_role_count=3,
            expected_evidence_count=3,
            evidence_sources=(),
            _rederive_historical_replay_case=lambda: None,
        )
        source = SimpleNamespace(
            tier="STRICT_POINT_IN_TIME",
            query_cutoff=cutoff,
            expected_date_count=1,
            date_sources=(date_source,),
        )

        with patch(
            "stock_monitor.replay.is_verified_historical_replay_source",
            return_value=True,
        ):
            result = replay_point_in_time(
                ReplayRequest(
                    cases=(requested_case,),
                    historical_source=source,
                )
            )

        self.assertEqual(
            result.results[0].reason_codes,
            (
                "MISSING_UNIVERSE_MEMBERSHIP",
                "MISSING_EVENT_STATE",
                "MISSING_SOURCE_EVIDENCE",
            ),
        )
        self.assertEqual(
            result.exclusion_counts,
            (
                ("MISSING_EVENT_STATE", 1),
                ("MISSING_SOURCE_EVIDENCE", 1),
                ("MISSING_UNIVERSE_MEMBERSHIP", 1),
            ),
        )

    def test_strict_replay_uses_rederived_case_not_caller_outputs(self) -> None:
        session = date(2026, 8, 17)
        cutoff = datetime(2026, 8, 17, 16, 30, tzinfo=ET)
        caller_case = ReplayCase(
            session_date=session,
            domain_results=passing_domain_results(),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )
        rederived_case = ReplayCase(
            session_date=session,
            domain_results=passing_domain_results(
                failed=ReplayDomainComponent.DAILY_PRICE
            ),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("105"),
                low=Decimal("97"),
            ),
        )
        date_source = SimpleNamespace(
            session_date=session,
            report_cutoff=cutoff,
            expected_role_count=3,
            expected_evidence_count=3,
            evidence_sources=tuple(
                SimpleNamespace(
                    role=role,
                    effective_at=cutoff - timedelta(hours=1),
                    published_at=cutoff - timedelta(minutes=30),
                    retrieved_at=cutoff - timedelta(minutes=1),
                )
                for role in (
                    "UNIVERSE_MEMBERSHIP",
                    "EVENT_STATE",
                    "SOURCE_EVIDENCE",
                )
            ),
            case_digest="a" * 64,
            domain_input_digest="b" * 64,
            mechanics_digest="c" * 64,
            _rederive_historical_replay_case=lambda: rederived_case,
        )
        source = SimpleNamespace(
            tier="STRICT_POINT_IN_TIME",
            query_cutoff=cutoff,
            expected_date_count=1,
            date_sources=(date_source,),
        )

        with patch(
            "stock_monitor.replay.is_verified_historical_replay_source",
            return_value=True,
        ):
            result = replay_point_in_time(
                ReplayRequest(
                    cases=(caller_case,),
                    historical_source=source,
                )
            )

        self.assertEqual(
            result.results[0].domain_results,
            rederived_case.domain_results,
        )
        self.assertIs(result.results[0].mechanics, rederived_case.mechanics)
        self.assertIs(
            result.results[0].evaluation_status,
            ReplayEvaluationStatus.FAILED,
        )

    def test_failed_rederivation_excludes_without_caller_economics(self) -> None:
        session = date(2026, 8, 17)
        cutoff = datetime(2026, 8, 17, 16, 30, tzinfo=ET)
        caller_case = ReplayCase(
            session_date=session,
            domain_results=passing_domain_results(),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )
        date_source = SimpleNamespace(
            session_date=session,
            report_cutoff=cutoff,
            expected_role_count=3,
            expected_evidence_count=3,
            evidence_sources=tuple(
                SimpleNamespace(role=role)
                for role in (
                    "UNIVERSE_MEMBERSHIP",
                    "EVENT_STATE",
                    "SOURCE_EVIDENCE",
                )
            ),
            case_digest="a" * 64,
            domain_input_digest="b" * 64,
            mechanics_digest="c" * 64,
            _rederive_historical_replay_case=lambda: None,
        )
        source = SimpleNamespace(
            tier="STRICT_POINT_IN_TIME",
            query_cutoff=cutoff,
            expected_date_count=1,
            date_sources=(date_source,),
        )

        with patch(
            "stock_monitor.replay.is_verified_historical_replay_source",
            return_value=True,
        ):
            result = replay_point_in_time(
                ReplayRequest(
                    cases=(caller_case,),
                    historical_source=source,
                )
            )

        self.assertEqual(result.included_dates, 0)
        self.assertIn(
            "HISTORICAL_REPLAY_CASE_REDERIVATION_FAILED",
            result.results[0].reason_codes,
        )
        self.assertEqual(result.results[0].domain_results, ())
        self.assertIsNone(result.results[0].mechanics)

    def test_direct_registrar_cannot_mint_caller_built_authority(self) -> None:
        @dataclass(frozen=True, slots=True, weakref_slot=True)
        class CallerBuiltHistoricalReplaySource:
            run_id: str

            def _is_current_historical_replay_source(self) -> bool:
                return True

        source = CallerBuiltHistoricalReplaySource("run:caller-built")

        with self.assertRaisesRegex(
            ReplayError,
            "UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY",
        ):
            _register_historical_replay_source(source)
        self.assertFalse(is_verified_historical_replay_source(source))
        with self.assertRaises(TypeError):
            _register_historical_replay_source(
                source,
                current_check=lambda: True,  # type: ignore[call-arg]
            )

    def test_spoofed_journal_module_and_qualname_cannot_register(self) -> None:
        import stock_monitor.journal  # Ensure the neutral owner module is loaded.

        spoof_type = make_dataclass(
            "HistoricalReplaySource",
            (("run_id", str),),
            frozen=True,
            slots=True,
            weakref_slot=True,
            namespace={
                "_is_current_historical_replay_source": lambda self: True,
            },
        )
        spoof_type.__module__ = "stock_monitor.journal"
        spoof = spoof_type("run:spoof")

        with self.assertRaisesRegex(
            ReplayError,
            "UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY",
        ):
            _register_historical_replay_source(spoof)
        self.assertFalse(is_verified_historical_replay_source(spoof))

    def test_raw_historical_source_identity_is_never_authoritative(self) -> None:
        self.assertFalse(is_verified_historical_replay_source(object()))

    def test_missing_or_caller_built_authority_excludes_every_case(self) -> None:
        case = ReplayCase(
            session_date=date(2026, 8, 17),
            domain_results=passing_domain_results(),
            mechanics=replay_ambiguous_bar(
                entry=Decimal("100"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("103"),
                low=Decimal("99"),
            ),
        )
        request = ReplayRequest(cases=(case,))

        missing = replay_point_in_time(request)
        caller_built = replay_point_in_time(
            replace(request, historical_source=object())
        )

        self.assertIs(missing.tier, ReplayTier.POINT_IN_TIME)
        self.assertNotIn("STRICT_POINT_IN_TIME_EVIDENCE", missing.labels)
        self.assertEqual((missing.included_dates, missing.excluded_dates), (0, 1))
        self.assertEqual(
            missing.exclusion_counts,
            (("MISSING_HISTORICAL_REPLAY_AUTHORITY", 1),),
        )
        self.assertEqual(
            caller_built.exclusion_counts,
            (("UNVERIFIED_HISTORICAL_REPLAY_AUTHORITY", 1),),
        )


class DailyReplayMechanicsTests(unittest.TestCase):
    def test_daily_only_ambiguity_keeps_conservative_economics_non_exact(self) -> None:
        stop_and_target = replay_ambiguous_bar(
            entry=Decimal("100"),
            stop=Decimal("98"),
            target=Decimal("104"),
            high=Decimal("105"),
            low=Decimal("97"),
        )
        entry_and_stop = replay_ambiguous_bar(
            entry=Decimal("100"),
            stop=Decimal("98"),
            target=Decimal("104"),
            high=Decimal("101"),
            low=Decimal("97"),
        )

        self.assertIs(stop_and_target.status, ReplayMechanicsStatus.RESOLVED)
        self.assertEqual(stop_and_target.exit_reason, "STOP_FIRST_CONSERVATIVE")
        self.assertEqual(stop_and_target.fill_price, Decimal("97.902000"))
        self.assertIs(
            stop_and_target.evidence_kind,
            ReplayMechanicsEvidence.DAILY_ONLY,
        )
        self.assertFalse(stop_and_target.is_exact)
        self.assertEqual(
            stop_and_target.labels,
            (
                "DAILY_ONLY_NON_EXACT_EXECUTION",
                "CONSERVATIVE_SEQUENCE_ASSUMPTION",
            ),
        )
        self.assertIs(entry_and_stop.status, ReplayMechanicsStatus.RESOLVED)
        self.assertEqual(entry_and_stop.exit_reason, "ENTRY_THEN_STOP_CONSERVATIVE")
        self.assertEqual(entry_and_stop.entry_fill_price, Decimal("100.100000"))
        self.assertEqual(entry_and_stop.fill_price, Decimal("97.902000"))
        self.assertIs(entry_and_stop.evidence_kind, ReplayMechanicsEvidence.DAILY_ONLY)
        self.assertFalse(entry_and_stop.is_exact)

    def test_prices_are_canonical_microdollars_or_rejected(self) -> None:
        result = replay_ambiguous_bar(
            entry=Decimal("100.0"),
            stop=Decimal("98"),
            target=Decimal("104.00"),
            high=Decimal("105.000"),
            low=Decimal("97.0"),
        )

        self.assertEqual(result.reference_price.as_tuple().exponent, -6)
        self.assertEqual(result.fill_price.as_tuple().exponent, -6)
        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_BOUNDARY"):
            replay_ambiguous_bar(
                entry=Decimal("100.0000001"),
                stop=Decimal("98"),
                target=Decimal("104"),
                high=Decimal("105"),
                low=Decimal("97"),
            )


class ReplayResultInvariantTests(unittest.TestCase):
    def test_mechanics_result_rejects_incoherent_evidence_and_economics(self) -> None:
        with self.assertRaisesRegex(ReplayError, "INCOMPLETE_REPLAY_MECHANICS"):
            ReplayMechanicsResult(ReplayMechanicsStatus.RESOLVED)
        with self.assertRaisesRegex(ReplayError, "MISSING_REPLAY_MECHANICS_REASONS"):
            ReplayMechanicsResult(ReplayMechanicsStatus.UNRESOLVED)
        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_EXACTNESS"):
            ReplayMechanicsResult(
                ReplayMechanicsStatus.NO_ACTION,
                evidence_kind=ReplayMechanicsEvidence.DAILY_ONLY,
                is_exact=True,
                labels=("DAILY_ONLY_NON_EXACT_EXECUTION",),
            )

    def test_replay_result_authenticates_counts_and_required_labels(self) -> None:
        valid = replay_diagnostic(
            ReplayRequest(
                cases=(
                    ReplayCase(
                        session_date=date(2026, 8, 17),
                        domain_results=passing_domain_results(),
                        mechanics=replay_ambiguous_bar(
                            entry=Decimal("100"),
                            stop=Decimal("98"),
                            target=Decimal("104"),
                            high=Decimal("103"),
                            low=Decimal("99"),
                        ),
                    ),
                )
            )
        )

        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_COUNTS"):
            replace(valid, total_dates=2)
        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_LABELS"):
            replace(valid, labels=("CURRENT_LIST_SURVIVORSHIP_BIAS",))
        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_LABELS"):
            replace(valid, labels=(*valid.labels, "UNREVIEWED_EXTRA_LABEL"))
        with self.assertRaisesRegex(
            ReplayError,
            "INVALID_REPLAY_EVALUATION_COUNTS",
        ):
            replace(valid, evaluation_counts=())


class OrderedIntradayReplayTests(unittest.TestCase):
    def test_same_bar_stop_and_target_uses_stop_first_deterministically(self) -> None:
        first = intraday_bar(
            1,
            "10:00",
            open_price="100",
            high="103",
            low="99",
            close="102",
            bid="101.99",
            ask="102.01",
        )
        ambiguous = intraday_bar(
            2,
            "11:00",
            open_price="100",
            high="105",
            low="97",
            close="101",
            bid="100.00",
            ask="100.02",
        )

        ordered = replay_ordered_intraday(
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(first, ambiguous),
        )
        reversed_input = replay_ordered_intraday(
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(ambiguous, first),
        )

        self.assertEqual(ordered, reversed_input)
        self.assertIs(ordered.status, ReplayMechanicsStatus.RESOLVED)
        self.assertEqual(ordered.exit_reason, "STOP_FIRST_CONSERVATIVE")
        self.assertEqual(ordered.fill_price, Decimal("97.902000"))
        self.assertEqual(ordered.observation_ids, ("bar-2",))

    def test_entry_reuses_trigger_then_later_qualifying_quote_before_exit(self) -> None:
        trigger = intraday_trade(1, "10:00", "101")
        fill_quote = intraday_quote(2, "10:01", "100.08", "100.10")
        stop_bar = intraday_bar(
            3,
            "10:02",
            open_price="100",
            high="101",
            low="97",
            close="98",
            bid="97.99",
            ask="98.01",
        )

        result = replay_ordered_intraday(
            trigger=Decimal("100"),
            maximum_entry=Decimal("100.10"),
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(stop_bar, fill_quote, trigger),
        )

        self.assertIs(result.status, ReplayMechanicsStatus.RESOLVED)
        self.assertEqual(result.exit_reason, "STOP")
        self.assertEqual(result.entry_fill_price, Decimal("100.100000"))
        self.assertEqual(result.fill_price, Decimal("97.902000"))
        self.assertEqual(
            result.observation_ids,
            ("trade-1", "quote-2", "bar-3"),
        )

    def test_entry_rejects_early_mixed_and_impossible_evidence(self) -> None:
        early = replay_ordered_intraday(
            trigger=Decimal("100"),
            maximum_entry=Decimal("100.10"),
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(
                intraday_trade(1, "09:30", "101"),
                intraday_quote(2, "09:31", "100.08", "100.10"),
            ),
        )
        mixed_stream = replay_ordered_intraday(
            trigger=Decimal("100"),
            maximum_entry=Decimal("100.10"),
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(
                intraday_trade(1, "10:00", "101"),
                intraday_quote(
                    2,
                    "10:01",
                    "100.08",
                    "100.10",
                    stream_id="sip:QQQ:2026-08-17",
                ),
            ),
        )
        above_limit = replay_ordered_intraday(
            trigger=Decimal("100"),
            maximum_entry=Decimal("100.10"),
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(
                intraday_trade(1, "10:00", "101"),
                intraday_quote(2, "10:01", "100.09", "100.11"),
            ),
        )

        self.assertEqual(early.entry_status, "NOT_TRIGGERED")
        self.assertIs(early.status, ReplayMechanicsStatus.NO_ACTION)
        self.assertEqual(mixed_stream.reason_codes, ("MIXED_OBSERVATION_STREAM",))
        self.assertEqual(above_limit.entry_status, "NOT_FILLED_LIMIT")
        self.assertIs(above_limit.status, ReplayMechanicsStatus.NO_ACTION)
        with self.assertRaisesRegex(ReplayError, "INVALID_REPLAY_BOUNDARY"):
            replay_ordered_intraday(
                trigger=Decimal("100.11"),
                maximum_entry=Decimal("100.10"),
                stop=Decimal("98"),
                target=Decimal("104"),
                observations=(
                    intraday_trade(1, "10:00", "101"),
                    intraday_quote(2, "10:01", "100.08", "100.10"),
                ),
            )

    def test_overnight_stop_gap_uses_next_open_then_adverse_slippage(self) -> None:
        opening_bar = intraday_bar(
            1,
            "09:30",
            open_price="95",
            high="101",
            low="94",
            close="100",
            bid="94.99",
            ask="95.01",
            session_open=True,
        )

        result = replay_ordered_intraday(
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(opening_bar,),
        )

        self.assertEqual(result.exit_reason, "GAP_STOP")
        self.assertEqual(result.reference_price, Decimal("95.000000"))
        self.assertEqual(result.fill_price, Decimal("94.905000"))

    def test_exit_evaluation_begins_only_after_the_qualifying_quote(self) -> None:
        prefill_stop_bar = intraday_bar(
            1,
            "09:59",
            open_price="99",
            high="100",
            low="97",
            close="98",
            bid="97.99",
            ask="98.01",
        )
        stop_bar = intraday_bar(
            4,
            "10:02",
            open_price="99",
            high="100",
            low="97",
            close="98",
            bid="97.99",
            ask="98.01",
        )

        result = replay_ordered_intraday(
            trigger=Decimal("100"),
            maximum_entry=Decimal("100.10"),
            stop=Decimal("98"),
            target=Decimal("104"),
            observations=(
                stop_bar,
                intraday_quote(3, "10:01", "100.08", "100.10"),
                prefill_stop_bar,
                intraday_trade(2, "10:00", "101"),
            ),
        )

        self.assertEqual(result.exit_reason, "STOP")
        self.assertEqual(
            result.observation_ids,
            ("trade-2", "quote-3", "bar-4"),
        )
        self.assertEqual(result.fill_price, Decimal("97.902000"))


if __name__ == "__main__":
    unittest.main()
