"""Inspectable, exact, and secret-free journal CSV export tests."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from stock_monitor.domain import money_to_micros
from stock_monitor.exports import ExportError, export_tables
from stock_monitor.journal import Journal


class ExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.destination = self.root / "exports"
        self.journal = Journal.open(self.root / "journal.sqlite3")
        self.addCleanup(self.journal.close)

    def seed_execution(self) -> None:
        message_time = datetime(2026, 8, 14, 14, 5, tzinfo=timezone.utc)
        raw_message_id, _ = self.journal.append_raw_message(
            "message-export-1",
            message_time,
            "BOUGHT NVDA 1 shares @ 123.456789; "
            "APCA_API_SECRET_KEY=raw-secret-canary",
        )
        self.journal.append_execution_event(
            raw_message_id=raw_message_id,
            action_ordinal=0,
            parsed_action="BOUGHT",
            signal_id="sig-export-1",
            symbol="NVDA",
            shares=1,
            price_micros=money_to_micros(Decimal("123.456789")),
            event_time=message_time,
            compliance_result="COMPLIANT",
            reconciliation_state="CLEAR",
            details={
                "api_secret_key": "nested-secret-canary",
                "review": {"access_token": "token-secret-canary"},
                "review_history": [
                    {"password": "sequence-secret-canary"},
                    {"safe_result": "reviewed"},
                ],
                "safe_note": "exact decimal export fixture",
            },
        )

    def test_exports_every_journal_table_in_stable_name_order(self) -> None:
        self.seed_execution()

        paths = export_tables(self.journal, self.destination)

        self.assertEqual(paths, tuple(sorted(paths, key=lambda path: path.name)))
        self.assertIn(self.destination / "raw_messages.csv", paths)
        self.assertIn(self.destination / "execution_events.csv", paths)
        self.assertIn(self.destination / "reports.csv", paths)
        self.assertTrue(all(path.is_file() for path in paths))

    def test_microdollars_are_exported_as_exact_decimal_strings(self) -> None:
        self.seed_execution()

        export_tables(self.journal, self.destination)
        with (self.destination / "execution_events.csv").open(
            encoding="utf-8", newline=""
        ) as stream:
            rows = list(csv.DictReader(stream))

        self.assertEqual(len(rows), 1)
        self.assertNotIn("price_micros", rows[0])
        self.assertEqual(rows[0]["price_decimal"], "123.456789")
        self.assertEqual(rows[0]["shares"], "1")
        self.assertEqual(rows[0]["bid_decimal"], "")

    def test_secret_bearing_text_and_nested_json_are_redacted(self) -> None:
        self.seed_execution()

        paths = export_tables(self.journal, self.destination)
        combined = b"\n".join(path.read_bytes() for path in paths).decode("utf-8")

        self.assertNotIn("raw-secret-canary", combined)
        self.assertNotIn("nested-secret-canary", combined)
        self.assertNotIn("token-secret-canary", combined)
        self.assertNotIn("sequence-secret-canary", combined)
        self.assertIn("[REDACTED]", combined)
        self.assertIn("exact decimal export fixture", combined)
        with (self.destination / "execution_events.csv").open(
            encoding="utf-8", newline=""
        ) as stream:
            row = next(csv.DictReader(stream))
        details = json.loads(row["details_json"])
        self.assertEqual(details["api_secret_key"], "[REDACTED]")
        self.assertEqual(details["review"]["access_token"], "[REDACTED]")
        self.assertEqual(details["review_history"][0]["password"], "[REDACTED]")
        self.assertEqual(details["review_history"][1]["safe_result"], "reviewed")

    def test_authorization_header_credentials_are_fully_redacted_in_csv(self) -> None:
        message_time = datetime(2026, 8, 14, 14, 5, tzinfo=timezone.utc)
        headers = (
            "Authorization: Bearer bearer-secret-canary",
            "authorization=Basic basic-secret-canary",
        )
        for ordinal, header in enumerate(headers):
            self.journal.append_raw_message(
                f"authorization-export-{ordinal}",
                message_time,
                header,
            )

        export_tables(self.journal, self.destination)
        exported = (self.destination / "raw_messages.csv").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("bearer-secret-canary", exported)
        self.assertNotIn("basic-secret-canary", exported)
        self.assertNotIn("Bearer", exported)
        self.assertNotIn("Basic", exported)

    def test_cookie_header_is_fully_redacted_in_csv(self) -> None:
        self.journal.append_raw_message(
            "cookie-export",
            datetime(2026, 8, 14, 14, 5, tzinfo=timezone.utc),
            "Cookie: session=cookie-secret-one; csrf=cookie-secret-two",
        )

        export_tables(self.journal, self.destination)
        exported = (self.destination / "raw_messages.csv").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("cookie-secret-one", exported)
        self.assertNotIn("cookie-secret-two", exported)
        self.assertIn("Cookie: [REDACTED]", exported)

    def test_symbolic_link_ancestor_cannot_escape_the_export_root(self) -> None:
        outside_directory = tempfile.TemporaryDirectory()
        self.addCleanup(outside_directory.cleanup)
        outside = Path(outside_directory.name)
        (self.root / "link").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(ExportError, "symbolic link"):
            export_tables(self.journal, self.root / "link" / "exports")

        self.assertEqual(list(outside.rglob("*.csv")), [])

    def test_parent_segments_cannot_escape_the_journal_root(self) -> None:
        outside_directory = tempfile.TemporaryDirectory(dir=self.root.parent)
        self.addCleanup(outside_directory.cleanup)
        outside = Path(outside_directory.name)
        traversal = self.root / ".." / outside.name / "exports"

        with self.assertRaisesRegex(ExportError, "journal root"):
            export_tables(self.journal, traversal)

        self.assertEqual(list(outside.rglob("*.csv")), [])

    def test_retry_replaces_each_csv_with_identical_bytes(self) -> None:
        self.seed_execution()
        first_paths = export_tables(self.journal, self.destination)
        first = {path.name: path.read_bytes() for path in first_paths}

        second_paths = export_tables(self.journal, self.destination)
        second = {path.name: path.read_bytes() for path in second_paths}

        self.assertEqual(first_paths, second_paths)
        self.assertEqual(first, second)
        self.assertEqual(list(self.destination.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
