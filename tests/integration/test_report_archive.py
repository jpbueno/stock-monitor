"""Crash-safe, content-exact report archive integration tests."""

from __future__ import annotations

import hashlib
import multiprocessing
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from stock_monitor.reports import (
    PremarketState,
    ReportArchiveConflict,
    ReportArchiveError,
    archive_report,
    render_premarket_report,
    stable_report_id,
)


ET = ZoneInfo("America/New_York")


def _archive_in_process(
    report,
    root: str,
    start,
    results,
) -> None:
    start.wait()
    try:
        archived = archive_report(report, Path(root))
        results.put(("published", archived.sha256))
    except ReportArchiveConflict:
        results.put(("conflict", None))
    except Exception as error:  # pragma: no cover - surfaced by the parent assertion
        results.put((type(error).__name__, str(error)))


def no_trade_report(*, observation_ids: tuple[str, ...] = ("obs-2", "obs-1")):
    return render_premarket_report(
        PremarketState(
            session_date=date(2026, 8, 14),
            generated_at=datetime(2026, 8, 14, 8, 45, tzinfo=ET),
            outcome="NO TRADE",
            reason_codes=("NO_CANDIDATE_PASSED_ALL_GATES",),
            observation_ids=observation_ids,
            state_hash="d" * 64,
        )
    )


class ArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.report = no_trade_report()

    def test_same_report_id_has_same_path_and_hash(self) -> None:
        first = archive_report(self.report, self.root)
        second = archive_report(self.report, self.root)

        self.assertEqual(first.path, second.path)
        self.assertEqual(first.sha256, second.sha256)
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(first.path.read_text(encoding="utf-8"), self.report.body)
        self.assertEqual(
            first.path.relative_to(self.root).as_posix(),
            self.report.archive_relative_path,
        )

    def test_observation_order_does_not_change_identity_or_archive(self) -> None:
        replayed = no_trade_report(observation_ids=("obs-1", "obs-2"))

        first = archive_report(self.report, self.root)
        second = archive_report(replayed, self.root)

        self.assertEqual(self.report.report_id, replayed.report_id)
        self.assertEqual(first.path, second.path)
        self.assertTrue(second.duplicate)
        self.assertEqual(
            stable_report_id(
                "PREMARKET",
                date(2026, 8, 14),
                ("obs-1", "obs-2"),
                "d" * 64,
            ),
            self.report.report_id,
        )

    def test_conflicting_content_under_the_same_id_is_rejected_exactly(self) -> None:
        archived = archive_report(self.report, self.root)
        conflicting_body = self.report.body + "unexpected mutation\n"
        conflicting = replace(
            self.report,
            body=conflicting_body,
            content_sha256=hashlib.sha256(
                conflicting_body.encode("utf-8")
            ).hexdigest(),
        )

        with self.assertRaisesRegex(
            ReportArchiveConflict,
            "report ID conflicts with archived content",
        ):
            archive_report(conflicting, self.root)

        self.assertEqual(archived.path.read_text(encoding="utf-8"), self.report.body)

    def test_tampered_report_hash_is_rejected_before_writing(self) -> None:
        tampered = replace(self.report, content_sha256="0" * 64)

        with self.assertRaisesRegex(ReportArchiveConflict, "content hash"):
            archive_report(tampered, self.root)

        self.assertFalse((self.root / self.report.archive_relative_path).exists())

    def test_path_traversal_kind_is_rejected_as_an_archive_conflict(self) -> None:
        traversal = replace(self.report, kind="../PREMARKET")

        with self.assertRaisesRegex(ReportArchiveConflict, "audit inputs"):
            archive_report(traversal, self.root)

        self.assertEqual(list(self.root.rglob("*.md")), [])

    def test_symbolic_link_ancestor_cannot_escape_the_archive_root(self) -> None:
        archive_root = self.root / "archive-root"
        outside = self.root / "outside"
        archive_root.mkdir()
        outside.mkdir()
        (archive_root / "reports").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(ReportArchiveConflict, "symbolic link"):
            archive_report(self.report, archive_root)

        self.assertEqual(list(outside.rglob("*.md")), [])

    def test_tampered_id_cannot_collide_with_an_existing_prefix_path(self) -> None:
        archived = archive_report(self.report, self.root)
        colliding_id = self.report.report_id[:12] + ("f" * 52)
        collision = replace(self.report, report_id=colliding_id)

        with self.assertRaisesRegex(ReportArchiveConflict, "audit inputs"):
            archive_report(collision, self.root)

        self.assertEqual(archived.path.read_text(encoding="utf-8"), self.report.body)
        self.assertEqual(list(self.root.rglob("*.md")), [archived.path])

    def test_failed_atomic_publish_leaves_no_partial_archive_or_temp_file(self) -> None:
        with patch(
            "stock_monitor.reports.os.link",
            side_effect=OSError("simulated publish failure"),
        ):
            with self.assertRaisesRegex(ReportArchiveError, "could not be published"):
                archive_report(self.report, self.root)

        target = self.root / self.report.archive_relative_path
        self.assertFalse(target.exists())
        self.assertEqual(
            [path for path in target.parent.iterdir() if path.name.endswith(".tmp")],
            [],
        )

    def test_conflicting_archive_is_atomic_across_processes(self) -> None:
        bodies = (
            self.report.body + ("A" * (8 * 1024 * 1024)),
            self.report.body + ("B" * (8 * 1024 * 1024)),
        )
        reports = tuple(
            replace(
                self.report,
                body=body,
                content_sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            )
            for body in bodies
        )
        context = multiprocessing.get_context("fork")
        start = context.Event()
        results = context.Queue()
        processes = [
            context.Process(
                target=_archive_in_process,
                args=(report, str(self.root), start, results),
            )
            for report in reports
        ]
        try:
            for process in processes:
                process.start()
            start.set()
            outcomes = [results.get(timeout=10) for _ in processes]
            for process in processes:
                process.join(timeout=10)
                self.assertEqual(process.exitcode, 0)
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            results.close()
            results.join_thread()

        self.assertEqual(
            sorted(status for status, _ in outcomes),
            ["conflict", "published"],
        )
        published_sha256 = next(
            value for status, value in outcomes if status == "published"
        )
        target = self.root / self.report.archive_relative_path
        self.assertEqual(
            hashlib.sha256(target.read_bytes()).hexdigest(),
            published_sha256,
        )


if __name__ == "__main__":
    unittest.main()
