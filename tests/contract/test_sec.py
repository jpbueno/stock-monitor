from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from stock_monitor.evidence import EvidenceSourceBinding
from stock_monitor.providers.cache import ContentCache
from stock_monitor.providers.http import HttpResponse, NetworkPolicyError
from stock_monitor.providers.sec import (
    SecClient,
    SecMetadataError,
    SecRateGovernor,
    SecRateLimitError,
)
from tests.support import FixtureTransport


class RedirectingTransport:
    def __init__(self) -> None:
        self.requested_urls: list[str] = []

    def get(self, url: str, headers):
        from stock_monitor.providers.http import HttpResponse

        self.requested_urls.append(url)
        if len(self.requested_urls) == 1:
            return HttpResponse(
                302,
                (("Location", "https://data.sec.gov/not-submissions/file.json"),),
                b"",
                url,
            )
        return HttpResponse(
            200,
            (("Content-Type", "application/json"),),
            (
                b'{"cik":"0000320193","filings":{"recent":{'
                b'"accessionNumber":[],"acceptanceDateTime":[],"primaryDocument":[]}}}'
            ),
            url,
        )


class StaticJsonTransport:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.requested_urls: list[str] = []

    def get(self, url: str, headers) -> HttpResponse:
        self.requested_urls.append(url)
        return HttpResponse(
            200,
            (("Content-Type", "application/json"),),
            self.body,
            url,
        )


class SameOriginSwapTransport:
    def __init__(self, responses: list[tuple[int, tuple[tuple[str, str], ...], bytes]]) -> None:
        self.responses = list(responses)
        self.requested_urls: list[str] = []

    def get(self, url: str, headers) -> HttpResponse:
        self.requested_urls.append(url)
        if not self.responses:
            raise AssertionError(f"unexpected SEC request: {url}")
        status, response_headers, body = self.responses.pop(0)
        return HttpResponse(status, response_headers, body, url)


class FakeClock:
    def __init__(self, value: float) -> None:
        self.value = value
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class SecContractTests(unittest.TestCase):
    def test_corrupt_rate_state_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "sec-rate.state"
            state.write_text("not-a-timestamp", encoding="ascii")
            governor = SecRateGovernor(state)

            with self.assertRaises(SecRateLimitError):
                governor.wait()

    def test_two_governors_share_cross_process_state_with_110ms_minimum_cadence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "sec-rate.state"
            clock = FakeClock(1_000.0)
            first = SecRateGovernor(
                state,
                minimum_interval=0.110,
                wall_clock=clock,
                sleeper=clock.sleep,
            )
            second = SecRateGovernor(
                state,
                minimum_interval=0.110,
                wall_clock=clock,
                sleeper=clock.sleep,
            )

            first.wait()
            second.wait()

            self.assertEqual(len(clock.sleeps), 1)
            self.assertGreaterEqual(clock.sleeps[0], 0.110)
            persisted = float(state.read_text(encoding="ascii"))
            self.assertGreaterEqual(persisted, 1_000.110)

    def test_future_rate_state_fails_closed_without_sleeping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "sec-rate.state"
            state.write_text("2000.0\n", encoding="ascii")
            clock = FakeClock(1_000.0)
            governor = SecRateGovernor(
                state,
                wall_clock=clock,
                sleeper=clock.sleep,
            )
            with self.assertRaises(SecRateLimitError):
                governor.wait()
            self.assertEqual(clock.sleeps, [])

    def test_even_slightly_future_rate_state_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "sec-rate.state"
            state.write_text("1000.000000100\n", encoding="ascii")
            clock = FakeClock(1_000.0)
            with self.assertRaises(SecRateLimitError):
                SecRateGovernor(
                    state,
                    wall_clock=clock,
                    sleeper=clock.sleep,
                ).wait()
            self.assertEqual(clock.sleeps, [])

    def test_rate_state_rejects_symlinks_and_multiply_linked_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.state"
            target.write_text("", encoding="ascii")
            symlink = root / "symlink.state"
            symlink.symlink_to(target)
            with self.assertRaises(SecRateLimitError):
                SecRateGovernor(
                    symlink,
                    wall_clock=FakeClock(1000.0),
                    sleeper=lambda _: None,
                ).wait()

            linked = root / "linked.state"
            linked.hardlink_to(target)
            with self.assertRaises(SecRateLimitError):
                SecRateGovernor(
                    linked,
                    wall_clock=FakeClock(1000.0),
                    sleeper=lambda _: None,
                ).wait()

    def test_submissions_and_archive_use_exact_origins_identity_hash_and_sec_metadata_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transport = FixtureTransport("providers/sec/submission-and-archive.json")
            clock = FakeClock(1_000.0)
            client = SecClient(
                transport=transport,
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(
                    root / "sec-rate.state",
                    wall_clock=clock,
                    sleeper=clock.sleep,
                ),
                user_agent="Stock Monitor tests test@example.com",
                now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            )

            submission = client.get_submission("320193")
            archive = client.get_archive(
                "edgar/data/320193/000032019326000001/aapl-20260813.htm"
            )

            accepted = datetime(2026, 8, 13, 20, 1, 2, tzinfo=UTC)
            self.assertEqual(submission.published_at, accepted)
            self.assertEqual(archive.published_at, accepted)
            self.assertEqual(
                submission.timestamp_source,
                "SEC_SUBMISSIONS_METADATA",
            )
            self.assertEqual(archive.accession, "0000320193-26-000001")
            self.assertEqual(archive.timestamp_source, "SEC_FILING_METADATA")
            for document in (submission, archive):
                binding = EvidenceSourceBinding.from_document(
                    document,
                    symbol="AAPL",
                    issuer_cik="0000320193",
                    checked_at=document.retrieved_at,
                    valid_until=document.retrieved_at + timedelta(hours=1),
                    healthy=True,
                )
                self.assertIs(binding.document, document)
            self.assertNotEqual(
                archive.published_at,
                datetime(1999, 1, 1, tzinfo=UTC),
            )
            self.assertEqual(len(archive.content_hash), 64)
            self.assertTrue(archive.source_observation_id)
            for headers in transport.requested_headers:
                self.assertEqual(
                    headers["User-Agent"],
                    "Stock Monitor tests test@example.com",
                )
                self.assertEqual(headers["Accept-Encoding"], "identity")
            self.assertEqual(
                transport.requested_urls,
                [
                    "https://data.sec.gov/submissions/CIK0000320193.json",
                    "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/aapl-20260813.htm",
                ],
            )
            self.assertGreaterEqual(sum(clock.sleeps), 0.110)

    def test_archive_requires_prior_matching_sec_acceptance_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = SecClient(
                transport=FixtureTransport("providers/sec/archive-only.json"),
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
            )
            with self.assertRaises(SecMetadataError):
                client.get_archive(
                    "edgar/data/320193/000032019326000001/aapl-20260813.htm"
                )

    def test_accession_cik_prefix_must_match_requested_normalized_cik(self) -> None:
        body = (
            b'{"cik":"0000320193","filings":{"recent":{'
            b'"accessionNumber":["0000789019-26-000001"],'
            b'"acceptanceDateTime":["2026-08-13T20:01:02Z"],'
            b'"primaryDocument":["aapl-20260813.htm"]}}}'
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = SecClient(
                transport=StaticJsonTransport(body),
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
                now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            )

            with self.assertRaises(SecMetadataError):
                client.get_submission("320193")
            with self.assertRaises(SecMetadataError):
                client.get_archive(
                    "edgar/data/320193/000078901926000001/aapl-20260813.htm"
                )

    def test_primary_document_is_a_bounded_ascii_safe_filename(self) -> None:
        poisoned = (
            "filing.htm?download=1",
            "filing.htm#fragment",
            "filing\n.htm",
            "filing-é.htm",
            "%2e%2e",
            "filing%2fexhibit.htm",
            "a" * 256 + ".htm",
        )
        for primary_document in poisoned:
            body = json.dumps(
                {
                    "cik": "0000320193",
                    "filings": {
                        "recent": {
                            "accessionNumber": ["0000320193-26-000001"],
                            "acceptanceDateTime": ["2026-08-13T20:01:02Z"],
                            "primaryDocument": [primary_document],
                        }
                    },
                }
            ).encode("utf-8")
            with self.subTest(primary_document=primary_document):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    client = SecClient(
                        transport=StaticJsonTransport(body),
                        cache=ContentCache(root / "cache"),
                        governor=SecRateGovernor(root / "sec-rate.state"),
                        user_agent="Stock Monitor tests test@example.com",
                        now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
                    )
                    with self.assertRaises(SecMetadataError):
                        client.get_submission("320193")

    def test_known_accession_allows_matching_exhibit_without_fabricating_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transport = FixtureTransport("providers/sec/submission-and-exhibit.json")
            client = SecClient(
                transport=transport,
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
                now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            )
            client.get_submission("320193")
            exhibit = client.get_archive(
                "edgar/data/320193/000032019326000001/exhibit-991.htm"
            )
            self.assertEqual(exhibit.accession, "0000320193-26-000001")
            self.assertEqual(
                exhibit.published_at,
                datetime(2026, 8, 13, 20, 1, 2, tzinfo=UTC),
            )

    def test_sec_redirects_cannot_leave_the_pinned_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            submissions_transport = RedirectingTransport()
            client = SecClient(
                transport=submissions_transport,
                cache=ContentCache(root / "cache-a"),
                governor=SecRateGovernor(root / "sec-rate-a.state"),
                user_agent="Stock Monitor tests test@example.com",
            )
            with self.assertRaises(NetworkPolicyError):
                client.get_submission("320193")
            self.assertEqual(
                submissions_transport.requested_urls,
                ["https://data.sec.gov/submissions/CIK0000320193.json"],
            )

            archives_transport = FixtureTransport(
                "providers/sec/archive-redirect-outside-prefix.json"
            )
            client = SecClient(
                transport=archives_transport,
                cache=ContentCache(root / "cache-b"),
                governor=SecRateGovernor(root / "sec-rate-b.state"),
                user_agent="Stock Monitor tests test@example.com",
                now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            )
            client.get_submission("320193")
            with self.assertRaises(NetworkPolicyError):
                client.get_archive(
                    "edgar/data/320193/000032019326000001/aapl-20260813.htm"
                )
            self.assertEqual(len(archives_transport.requested_urls), 2)

    def test_same_origin_redirect_cannot_swap_requested_submission_cik(self) -> None:
        target = "https://data.sec.gov/submissions/CIK0000789019.json"
        transport = SameOriginSwapTransport(
            [
                (302, (("Location", target),), b""),
                (
                    200,
                    (("Content-Type", "application/json"),),
                    (
                        b'{"cik":"0000789019","filings":{"recent":{'
                        b'"accessionNumber":[],"acceptanceDateTime":[],'
                        b'"primaryDocument":[]}}}'
                    ),
                ),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = SecClient(
                transport=transport,
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
            )
            with self.assertRaises(NetworkPolicyError):
                client.get_submission("320193")

        self.assertEqual(
            transport.requested_urls,
            ["https://data.sec.gov/submissions/CIK0000320193.json"],
        )

    def test_same_origin_redirect_cannot_swap_archive_cik_or_accession(self) -> None:
        submission = (
            b'{"cik":"0000320193","filings":{"recent":{'
            b'"accessionNumber":["0000320193-26-000001"],'
            b'"acceptanceDateTime":["2026-08-13T20:01:02Z"],'
            b'"primaryDocument":["aapl-20260813.htm"]}}}'
        )
        target = (
            "https://www.sec.gov/Archives/edgar/data/789019/"
            "000078901926000777/other.htm"
        )
        transport = SameOriginSwapTransport(
            [
                (200, (("Content-Type", "application/json"),), submission),
                (302, (("Location", target),), b""),
                (200, (("Content-Type", "text/html"),), b"<html>other issuer</html>"),
            ]
        )
        requested_archive = (
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/aapl-20260813.htm"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = SecClient(
                transport=transport,
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
                now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            )
            client.get_submission("320193")
            with self.assertRaises(NetworkPolicyError):
                client.get_archive(
                    "edgar/data/320193/000032019326000001/aapl-20260813.htm"
                )

        self.assertEqual(
            transport.requested_urls,
            [
                "https://data.sec.gov/submissions/CIK0000320193.json",
                requested_archive,
            ],
        )

    def test_archive_path_cannot_escape_or_switch_origin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = SecClient(
                transport=FixtureTransport("providers/sec/archive-only.json"),
                cache=ContentCache(root / "cache"),
                governor=SecRateGovernor(root / "sec-rate.state"),
                user_agent="Stock Monitor tests test@example.com",
            )
            poisoned = (
                "../submissions/secret",
                "/Archives/edgar/data/file",
                "https://evil.example/file",
                "edgar/data/file?token=secret",
                "edgar/data/file#fragment",
            )
            for path in poisoned:
                with self.subTest(path=path), self.assertRaises(
                    (ValueError, NetworkPolicyError)
                ):
                    client.get_archive(path)

    def test_constructor_pins_official_sec_origins_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for field, value in (
                ("submissions_origin", "https://sec.example/submissions/"),
                ("submissions_origin", "https://data.sec.gov/other/"),
                ("archives_origin", "https://www.sec.gov/other/"),
                ("archives_origin", "https://data.sec.gov/Archives/"),
            ):
                kwargs = {field: value}
                with self.subTest(field=field, value=value), self.assertRaises(
                    NetworkPolicyError
                ):
                    SecClient(
                        transport=FixtureTransport("providers/sec/archive-only.json"),
                        cache=ContentCache(root / "cache"),
                        governor=SecRateGovernor(root / "sec-rate.state"),
                        user_agent="Stock Monitor tests test@example.com",
                        **kwargs,
                    )


if __name__ == "__main__":
    unittest.main()
