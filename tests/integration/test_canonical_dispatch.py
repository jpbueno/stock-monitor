"""Canonical workflow dispatch keeps economic and retrieval time separate."""

from __future__ import annotations

import unittest
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from stock_monitor.workflows import (
    CanonicalWorkflowContext,
    PublishedWorkflow,
    SessionWindow,
    WorkflowResult,
    run_canonical_close,
    run_canonical_premarket,
)


ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 24)


class _RecordingAdapter:
    execution_mode = "CANONICAL"

    def __init__(self, *, review_time: time = time(15, 30), open_day: bool = True):
        self.review_time = review_time
        self.open_day = open_day
        self.calls: list[tuple[object, ...]] = []
        self.premarket = object()
        self.close = object()

    def market_session(self, day: date) -> SessionWindow | None:
        self.calls.append(("SESSION", day))
        if not self.open_day:
            return None
        return SessionWindow(day, self.review_time)

    def premarket_material(
        self,
        session_date: date,
        *,
        decision_at: datetime,
        retrieved_at: datetime,
    ) -> object:
        self.calls.append(
            ("PREMARKET", session_date, decision_at, retrieved_at)
        )
        return self.premarket

    def close_material(
        self,
        session_date: date,
        *,
        review_at: datetime,
        retrieved_at: datetime,
    ) -> object:
        self.calls.append(("CLOSE", session_date, review_at, retrieved_at))
        return self.close


class _RecordingPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def issue_result(self, *, material: object) -> WorkflowResult:
        self.calls.append(("ISSUE", material))
        return WorkflowResult(
            outcome="HOLD",
            message="MANUAL MONITORING ONLY",
            exit_code=0,
            reason_codes=("MANUAL_VERIFICATION_REQUIRED",),
            execution_mode="CANONICAL",
        )

    def publish(
        self,
        *,
        kind: str,
        session_date: date,
        generated_at: datetime,
        result: WorkflowResult,
        material: object,
    ) -> PublishedWorkflow:
        self.calls.append(
            ("PUBLISH", kind, session_date, generated_at, result, material)
        )
        return PublishedWorkflow("a" * 64, 7, "/tmp/canonical-report.md")


class CanonicalDispatchTests(unittest.TestCase):
    @staticmethod
    def _context(
        now: datetime,
        *,
        adapter: _RecordingAdapter | None = None,
        publisher: _RecordingPublisher | None = None,
    ) -> CanonicalWorkflowContext:
        return CanonicalWorkflowContext(
            adapter=_RecordingAdapter() if adapter is None else adapter,
            publisher=(
                _RecordingPublisher() if publisher is None else publisher
            ),
            scheduler=None,
            now=now,
        )

    def test_premarket_uses_fixed_economic_cutoff_and_actual_retrieval(self):
        now = datetime(2026, 8, 24, 8, 52, 31, tzinfo=ET)
        adapter = _RecordingAdapter()
        publisher = _RecordingPublisher()

        result = run_canonical_premarket(
            self._context(now, adapter=adapter, publisher=publisher)
        )

        self.assertEqual(
            adapter.calls,
            [
                ("SESSION", DAY),
                (
                    "PREMARKET",
                    DAY,
                    datetime(2026, 8, 24, 8, 45, tzinfo=ET),
                    now,
                ),
            ],
        )
        self.assertEqual(result.execution_mode, "CANONICAL")
        self.assertEqual(result.report_id, "a" * 64)
        self.assertEqual(result.report_row_id, 7)
        self.assertEqual(result.report_path, "/tmp/canonical-report.md")
        self.assertEqual(publisher.calls[0], ("ISSUE", adapter.premarket))
        self.assertEqual(publisher.calls[1][1:4], ("PREMARKET", DAY, now))

    def test_early_close_uses_review_time_and_actual_retrieval(self):
        now = datetime(2026, 11, 27, 12, 38, tzinfo=ET)
        adapter = _RecordingAdapter(review_time=time(12, 30))
        publisher = _RecordingPublisher()

        result = run_canonical_close(
            self._context(now, adapter=adapter, publisher=publisher)
        )

        self.assertEqual(
            adapter.calls,
            [
                ("SESSION", now.date()),
                (
                    "CLOSE",
                    now.date(),
                    datetime(2026, 11, 27, 12, 30, tzinfo=ET),
                    now,
                ),
            ],
        )
        self.assertEqual(result.report_id, "a" * 64)
        self.assertEqual(publisher.calls[1][1:4], ("CLOSE", now.date(), now))

    def test_manual_canonical_runs_never_backfill(self):
        cases = (
            (
                "before premarket",
                run_canonical_premarket,
                datetime(2026, 8, 24, 8, 44, 59, tzinfo=ET),
                "NOT_DUE_NOOP",
                "NOT_DUE",
            ),
            (
                "after premarket",
                run_canonical_premarket,
                datetime(2026, 8, 24, 9, 0, tzinfo=ET),
                "MISSED_RUN_NOOP",
                "MISSED_RUN",
            ),
            (
                "before close",
                run_canonical_close,
                datetime(2026, 8, 24, 15, 29, 59, tzinfo=ET),
                "NOT_DUE_NOOP",
                "NOT_DUE",
            ),
            (
                "after close",
                run_canonical_close,
                datetime(2026, 8, 24, 15, 45, tzinfo=ET),
                "MISSED_RUN_NOOP",
                "MISSED_RUN",
            ),
        )
        for name, runner, now, outcome, reason in cases:
            with self.subTest(case=name):
                adapter = _RecordingAdapter()
                publisher = _RecordingPublisher()
                result = runner(
                    self._context(
                        now,
                        adapter=adapter,
                        publisher=publisher,
                    )
                )
                self.assertEqual(
                    (result.outcome, result.reason_codes, result.exit_code),
                    (outcome, (reason,), 0),
                )
                self.assertEqual(adapter.calls, [("SESSION", now.date())])
                self.assertEqual(publisher.calls, [])

    def test_market_closed_is_a_read_only_noop(self):
        now = datetime(2026, 8, 23, 8, 52, tzinfo=ET)
        adapter = _RecordingAdapter(open_day=False)
        publisher = _RecordingPublisher()

        result = run_canonical_premarket(
            self._context(now, adapter=adapter, publisher=publisher)
        )

        self.assertEqual(
            (result.outcome, result.reason_codes, result.exit_code),
            ("MARKET_CLOSED_NOOP", ("MARKET_CLOSED",), 0),
        )
        self.assertEqual(publisher.calls, [])

    def test_in_progress_publication_never_claims_success(self):
        class InProgressPublisher(_RecordingPublisher):
            def publish(self, **values: object) -> PublishedWorkflow:
                self.calls.append(("PUBLISH", values))
                return PublishedWorkflow(None, None, None, "IN_PROGRESS")

        now = datetime(2026, 8, 24, 8, 52, tzinfo=ET)
        result = run_canonical_premarket(
            self._context(now, publisher=InProgressPublisher())
        )

        self.assertEqual(
            (result.outcome, result.exit_code, result.reason_codes),
            (
                "PUBLICATION_INCOMPLETE",
                10,
                ("PUBLICATION_INCOMPLETE",),
            ),
        )
        self.assertIsNone(result.report_id)


if __name__ == "__main__":
    unittest.main()
