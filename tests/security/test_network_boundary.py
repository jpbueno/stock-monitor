from __future__ import annotations

import hashlib
import multiprocessing
import pickle
import tempfile
import threading
import unittest
from copy import copy, deepcopy
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from stock_monitor.providers import cache as cache_module
from stock_monitor.providers.cache import (
    CacheIntegrityError,
    CacheSourceUnavailableError,
    ContentCache,
    SourceObservation,
)
from stock_monitor.providers.http import (
    HttpResponse,
    NetworkPolicyError,
    ProviderResponseError,
    EgressPolicy,
    get_with_redirects,
)
from stock_monitor.providers.sec import SecClient, SecRateGovernor
from tests.support import FixtureTransport


def _process_cache_put(
    root: str,
    observation: SourceObservation,
    payload: bytes,
    start,
    results,
) -> None:
    start.wait()
    try:
        results.put(("ok", ContentCache(Path(root)).put(observation, payload)))
    except CacheIntegrityError:
        results.put(("error", None))


class SequenceTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = list(responses)
        self.requested_urls: list[str] = []
        self.requested_headers: list[dict[str, str]] = []

    def get(self, url: str, headers: dict[str, str]) -> HttpResponse:
        self.requested_urls.append(url)
        self.requested_headers.append(dict(headers))
        if not self.responses:
            raise AssertionError(f"unexpected request {url}")
        response = self.responses.pop(0)
        return HttpResponse(
            status=response.status,
            headers=response.headers,
            body=response.body,
            url=url,
        )


def response(
    status: int = 200,
    *,
    headers: tuple[tuple[str, str], ...] = (("Content-Type", "application/json"),),
    body: bytes = b"{}",
) -> HttpResponse:
    return HttpResponse(status=status, headers=headers, body=body, url="fixture")


class NetworkBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = EgressPolicy(
            {"data.alpaca.markets", "www.sec.gov", "data.sec.gov"}
        )

    def test_order_host_and_ip_literal_are_rejected(self) -> None:
        urls = (
            "https://paper-api.alpaca.markets/v2/orders",
            "https://api.alpaca.markets/v2/account",
            "https://127.0.0.1/data",
            "https://127.1/data",
            "https://0x7f.1/data",
            "https://[::1]/data",
        )
        for url in urls:
            with self.subTest(url=url), self.assertRaises(NetworkPolicyError):
                self.policy.validate_get(url)

    def test_only_exact_credential_free_https_default_port_urls_are_allowed(self) -> None:
        self.policy.validate_get("https://DATA.ALPACA.MARKETS:443/v2/stocks?feed=sip")
        poisoned = (
            "http://data.alpaca.markets/v2/stocks",
            "https://user:secret@data.alpaca.markets/v2/stocks",
            "https://data.alpaca.markets:444/v2/stocks",
            "https://data.alpaca.markets.evil.example/v2/stocks",
            "https://localhost/v2/stocks",
            "https://data.alpaca.markets/v2/stocks#fragment",
            "https://data.alpaca.markets\\@evil.example/v2/stocks",
            "https://data.alpaca.markets/v2/stocks\r\nX-Evil: yes",
            "https://data.alpaca.markets/v2/orders",
        )
        for url in poisoned:
            with self.subTest(url=url), self.assertRaises(NetworkPolicyError):
                self.policy.validate_get(url)

    def test_authentication_query_parameters_are_rejected_but_page_tokens_are_not(self) -> None:
        self.policy.validate_get(
            "https://data.alpaca.markets/v2/stocks?feed=sip&page_token=opaque"
        )
        for name in ("token", "access_token", "api_key", "key_id", "signature"):
            with self.subTest(name=name), self.assertRaises(NetworkPolicyError):
                self.policy.validate_get(
                    f"https://data.alpaca.markets/v2/stocks?{name}=canary"
                )

    def test_obfuscated_credential_query_names_are_rejected(self) -> None:
        poisoned = (
            "api-key",
            "x.api.key",
            "key-id",
            "client-secret",
            "auth-token",
            "request-signature",
        )
        for name in poisoned:
            with self.subTest(name=name), self.assertRaises(NetworkPolicyError):
                self.policy.validate_get(
                    f"https://data.alpaca.markets/v2/stocks?{name}=canary"
                )

    def test_redirects_are_same_origin_bounded_and_strip_sensitive_headers(self) -> None:
        transport = SequenceTransport(
            [
                response(
                    302,
                    headers=(("Location", "/final?cursor=two"),),
                    body=b"",
                ),
                response(body=b'{"ok":true}'),
            ]
        )
        headers = {
            "Authorization": "secret-auth",
            "Proxy-Authorization": "secret-proxy",
            "Cookie": "secret-cookie",
            "APCA-API-KEY-ID": "secret-key",
            "APCA-API-SECRET-KEY": "secret-value",
            "User-Agent": "Stock Monitor test@example.com",
        }

        result = get_with_redirects(
            transport,
            self.policy,
            "https://data.alpaca.markets/start",
            headers,
        )

        self.assertEqual(result.body, b'{"ok":true}')
        self.assertEqual(
            transport.requested_urls,
            [
                "https://data.alpaca.markets/start",
                "https://data.alpaca.markets/final?cursor=two",
            ],
        )
        self.assertEqual(
            transport.requested_headers[0]["Accept-Encoding"], "identity"
        )
        redirected = {name.lower(): value for name, value in transport.requested_headers[1].items()}
        for sensitive in (
            "authorization",
            "proxy-authorization",
            "cookie",
            "apca-api-key-id",
            "apca-api-secret-key",
        ):
            self.assertNotIn(sensitive, redirected)
        self.assertEqual(redirected["accept-encoding"], "identity")

    def test_cross_origin_redirect_is_rejected_before_second_request(self) -> None:
        transport = SequenceTransport(
            [
                response(
                    302,
                    headers=(("Location", "https://www.sec.gov/Archives/file"),),
                    body=b"",
                )
            ]
        )

        with self.assertRaises(NetworkPolicyError):
            get_with_redirects(
                transport,
                self.policy,
                "https://data.sec.gov/submissions/CIK0000320193.json",
                {},
            )

        self.assertEqual(
            transport.requested_urls,
            ["https://data.sec.gov/submissions/CIK0000320193.json"],
        )

    def test_redirect_loop_missing_duplicate_and_excessive_location_fail_closed(self) -> None:
        cases = {
            "loop": [
                response(302, headers=(("Location", "/b"),), body=b""),
                response(302, headers=(("Location", "/a"),), body=b""),
            ],
            "missing": [response(302, headers=(), body=b"")],
            "duplicate": [
                response(
                    302,
                    headers=(("Location", "/a"), ("location", "/b")),
                    body=b"",
                )
            ],
            "excessive": [
                response(302, headers=(("Location", f"/{index}"),), body=b"")
                for index in range(1, 5)
            ],
        }
        for case, responses in cases.items():
            with self.subTest(case=case):
                transport = SequenceTransport(responses)
                with self.assertRaises(ProviderResponseError):
                    get_with_redirects(
                        transport,
                        self.policy,
                        "https://data.alpaca.markets/a",
                        {},
                    )

    def test_empty_oversized_encoded_or_wrong_content_type_is_not_success(self) -> None:
        cases = (
            (response(body=b""), {"max_bytes": 20}),
            (response(body=b"x" * 21), {"max_bytes": 20}),
            (
                response(
                    headers=(
                        ("Content-Type", "application/json"),
                        ("Content-Encoding", "gzip"),
                    )
                ),
                {"max_bytes": 20},
            ),
            (
                response(headers=(("Content-Type", "image/png"),)),
                {"max_bytes": 20},
            ),
        )
        for fixture_response, options in cases:
            with self.subTest(response=fixture_response):
                with self.assertRaises(ProviderResponseError):
                    get_with_redirects(
                        SequenceTransport([fixture_response]),
                        self.policy,
                        "https://data.alpaca.markets/v2/stocks",
                        {},
                        **options,
                    )

    def test_transport_must_attest_the_exact_single_hop_url(self) -> None:
        transport = SequenceTransport([response(body=b'{"ok":true}')])
        original_get = transport.get

        def blank_url(url: str, headers: dict[str, str]) -> HttpResponse:
            value = original_get(url, headers)
            return HttpResponse(value.status, value.headers, value.body, "")

        transport.get = blank_url  # type: ignore[method-assign]
        with self.assertRaises(ProviderResponseError):
            get_with_redirects(
                transport,
                self.policy,
                "https://data.alpaca.markets/v2/stocks",
                {},
            )

    def test_every_redirect_error_and_transient_hop_attests_exact_url(self) -> None:
        requested = "https://data.alpaca.markets/v2/stocks"
        cases = (
            HttpResponse(
                302,
                (("Location", "/final"),),
                b"",
                "https://data.alpaca.markets/other",
            ),
            HttpResponse(
                401,
                (("Content-Type", "application/json"),),
                b"{}",
                "https://data.alpaca.markets/other",
            ),
            HttpResponse(
                503,
                (("Content-Type", "application/json"),),
                b"{}",
                "https://data.alpaca.markets/other",
            ),
        )

        class UnattestedTransport:
            def __init__(self, value: HttpResponse) -> None:
                self.value = value
                self.calls = 0

            def get(self, url: str, headers: dict[str, str]) -> HttpResponse:
                self.calls += 1
                return self.value

        for value in cases:
            with self.subTest(status=value.status):
                transport = UnattestedTransport(value)
                with self.assertRaises(ProviderResponseError):
                    get_with_redirects(
                        transport,
                        self.policy,
                        requested,
                        {},
                        sleeper=lambda _: None,
                    )
                self.assertEqual(transport.calls, 1)

    def test_redirect_limit_configuration_never_exceeds_three(self) -> None:
        with self.assertRaises(ValueError):
            get_with_redirects(
                SequenceTransport([response()]),
                self.policy,
                "https://data.alpaca.markets/v2/stocks",
                {},
                max_redirects=4,
            )

    def test_whitespace_only_body_is_not_success(self) -> None:
        with self.assertRaises(ProviderResponseError):
            get_with_redirects(
                SequenceTransport([response(body=b" \t\r\n")]),
                self.policy,
                "https://data.alpaca.markets/v2/stocks",
                {},
            )

    def test_only_safe_transient_statuses_are_retried_a_bounded_number(self) -> None:
        transient = SequenceTransport(
            [response(503), response(429), response(body=b'{"ok":true}')]
        )
        result = get_with_redirects(
            transient,
            self.policy,
            "https://data.alpaca.markets/v2/stocks",
            {},
            sleeper=lambda _: None,
        )
        self.assertEqual(result.status, 200)
        self.assertEqual(len(transient.requested_urls), 3)

        permanent = SequenceTransport([response(401), response(body=b"not reached")])
        with self.assertRaises(ProviderResponseError):
            get_with_redirects(
                permanent,
                self.policy,
                "https://data.alpaca.markets/v2/stocks",
                {"APCA-API-SECRET-KEY": "do-not-leak"},
                sleeper=lambda _: None,
            )
        self.assertEqual(len(permanent.requested_urls), 1)

    def test_retry_after_delta_is_honored_only_with_a_safe_two_second_cap(self) -> None:
        for raw_delay, expected in (("1", 1.0), ("999", 2.0), ("invalid", 0.1)):
            transport = SequenceTransport(
                [
                    response(
                        429,
                        headers=(
                            ("Content-Type", "application/json"),
                            ("Retry-After", raw_delay),
                        ),
                    ),
                    response(body=b'{"ok":true}'),
                ]
            )
            sleeps: list[float] = []
            with self.subTest(raw_delay=raw_delay):
                get_with_redirects(
                    transport,
                    self.policy,
                    "https://data.alpaca.markets/v2/stocks",
                    {},
                    sleeper=sleeps.append,
                )
                self.assertEqual(sleeps, [expected])

    def test_errors_redact_query_and_header_secrets(self) -> None:
        secret = "secret-canary-value"
        with self.assertRaises(ProviderResponseError) as raised:
            get_with_redirects(
                SequenceTransport([response(401)]),
                self.policy,
                f"https://data.alpaca.markets/v2/stocks?cursor={secret}",
                {"Authorization": secret, "APCA-API-SECRET-KEY": secret},
            )
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn("cursor=", str(raised.exception))


class ContentCacheTests(unittest.TestCase):
    def test_direct_or_replaced_health_attestations_cannot_authorize_cache(self) -> None:
        digest = self.cache.put(self.observation, b"direct-health-must-not-authorize")
        direct = cache_module.SourceHealthAttestation(
            source_observation_id=self.provider_health.source_observation_id,
            source_type=self.provider_health.source_type,
            origin=self.provider_health.origin,
            feed=self.provider_health.feed,
            entitlement_class=self.provider_health.entitlement_class,
            checked_at=self.provider_health.checked_at,
            valid_until=self.provider_health.valid_until,
            healthy=True,
            entitlement_ok=True,
        )
        for value in (
            direct,
            replace(self.provider_health),
            copy(self.provider_health),
            deepcopy(self.provider_health),
            pickle.loads(pickle.dumps(self.provider_health)),
        ):
            with self.subTest(value=value), self.assertRaises(
                CacheSourceUnavailableError
            ):
                self.cache.get(
                    digest,
                    health=value,
                    as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
                )

    def test_recomputed_object_digest_cannot_rebind_provider_health(self) -> None:
        forged_observation = replace(
            self.observation,
            observation_id="forged-observation",
        )
        digest = self.cache.put(forged_observation, b"forged-observation-body")
        object.__setattr__(
            self.provider_health,
            "source_observation_id",
            forged_observation.observation_id,
        )
        object.__setattr__(
            self.provider_health,
            "_attestation_digest",
            cache_module._health_fingerprint(self.provider_health),
        )

        with self.assertRaises(CacheSourceUnavailableError):
            self.cache.get(
                digest,
                health=self.provider_health,
                as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
            )

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cache = ContentCache(self.root)
        provider_temporary = tempfile.TemporaryDirectory()
        self.addCleanup(provider_temporary.cleanup)
        provider_root = Path(provider_temporary.name)
        provider_cache = ContentCache(provider_root / "cache")
        rate_now = [1.0]

        def rate_clock() -> float:
            return rate_now[0]

        def rate_sleep(delay: float) -> None:
            rate_now[0] += delay

        provider = SecClient(
            transport=FixtureTransport("providers/sec/submission-and-archive.json"),
            cache=provider_cache,
            governor=SecRateGovernor(
                provider_root / "rate-state",
                wall_clock=rate_clock,
                sleeper=rate_sleep,
            ),
            user_agent="Stock Monitor tests test@example.com",
            now=lambda: datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
        )
        self.provider = provider
        provider_document = provider.get_submission("320193")
        self.observation = SourceObservation(
            observation_id=provider_document.source_observation_id,
            url=provider_document.url,
            source_type=provider_document.source_type,
            source_timestamp=provider_document.published_at
            or provider_document.retrieved_at,
            retrieved_at=provider_document.retrieved_at,
            feed="sec",
            delay_seconds=int(
                (
                    provider_document.retrieved_at
                    - (
                        provider_document.published_at
                        or provider_document.retrieved_at
                    )
                ).total_seconds()
            ),
        )
        self.provider_health = provider.health_attestation(provider_document)

    def health(
        self,
        *,
        source_observation_id: str | None = None,
        checked_at: datetime = datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
        valid_until: datetime | None = None,
        healthy: bool = True,
        entitlement_ok: bool = True,
        source_type: str = "SEC_SUBMISSIONS",
        origin: str = "https://data.sec.gov",
        feed: str = "sec",
        entitlement_class: str = "PUBLIC_SEC",
    ) -> object:
        if (
            source_observation_id is None
            and checked_at == datetime(2026, 8, 14, 12, 45, tzinfo=UTC)
            and valid_until is None
            and healthy is True
            and entitlement_ok is True
            and source_type == "SEC_SUBMISSIONS"
            and origin == "https://data.sec.gov"
            and feed == "sec"
            and entitlement_class == "PUBLIC_SEC"
        ):
            return self.provider_health
        return cache_module.SourceHealthAttestation(
            source_observation_id=(
                source_observation_id or self.observation.observation_id
            ),
            source_type=source_type,
            origin=origin,
            feed=feed,
            entitlement_class=entitlement_class,
            checked_at=checked_at,
            valid_until=valid_until or checked_at + timedelta(minutes=5),
            healthy=healthy,
            entitlement_ok=entitlement_ok,
        )

    def test_put_is_content_addressed_and_read_verifies_bytes(self) -> None:
        digest = self.cache.put(self.observation, b'{"cik":"0000320193"}')
        restored_observation, payload = self.cache.get(
            digest,
            health=self.health(),
            as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
        )

        self.assertEqual(
            digest,
            "7823a5005ef7608b35a5af6fedf94bb9974b7e7b4d44fdc137d4be1b1052ee9d",
        )
        self.assertEqual(restored_observation, self.observation.with_content_hash(digest))
        self.assertEqual(payload, b'{"cik":"0000320193"}')
        self.assertEqual(list(self.root.rglob("*.tmp")), [])

        payload_path = next(self.root.rglob(f"{digest}.bin"))
        payload_path.write_bytes(b"tampered")
        with self.assertRaises(CacheIntegrityError):
            self.cache.get(
                digest,
                health=self.health(),
                as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
            )

    def test_cache_requires_fresh_provenanced_health_and_entitlement(self) -> None:
        digest = self.cache.put(self.observation, b"payload")
        as_of = datetime(2026, 8, 14, 12, 46, tzinfo=UTC)
        attestations = (
            self.health(healthy=False),
            self.health(entitlement_ok=False),
            self.health(checked_at=as_of - timedelta(minutes=6)),
            self.health(checked_at=as_of + timedelta(microseconds=1)),
        )
        for health in attestations:
            with self.subTest(health=health), self.assertRaises(
                CacheSourceUnavailableError
            ):
                self.cache.get(digest, health=health, as_of=as_of)
        with self.assertRaises((TypeError, CacheSourceUnavailableError)):
            self.cache.get(digest, source_healthy=True)  # type: ignore[call-arg]

        alpaca_health = self.health(
            source_type="ALPACA_LATEST_QUOTES",
            origin="https://data.alpaca.markets",
            feed="iex",
            entitlement_class="ALPACA_IEX",
        )
        with self.assertRaises(CacheSourceUnavailableError):
            self.cache.get(digest, health=alpaca_health, as_of=as_of)

    def test_observation_rejects_credentials_in_url_and_payload_hash_is_secret_independent(self) -> None:
        poisoned = (
            "https://user:secret@data.sec.gov/submissions/file.json",
            "https://data.sec.gov/submissions/file.json?api_key=secret",
            "https://data.sec.gov/submissions/file.json?token=secret",
            "https://data.sec.gov/submissions/file.json?key-id=secret",
            "https://data.sec.gov/submissions/file.json?client.secret=secret",
        )
        for url in poisoned:
            with self.subTest(url=url), self.assertRaises(ValueError):
                SourceObservation(
                    observation_id="obs-poisoned",
                    url=url,
                    source_type="SEC_SUBMISSIONS",
                    source_timestamp=datetime(2026, 8, 13, tzinfo=UTC),
                    retrieved_at=datetime(2026, 8, 14, tzinfo=UTC),
                    feed="sec",
                    delay_seconds=0,
                )

    def test_same_payload_preserves_each_observation_and_rejects_identity_collision(self) -> None:
        first_digest = self.cache.put(self.observation, b"same payload")
        archive_document = self.provider.get_archive(
            "edgar/data/320193/000032019326000001/aapl-20260813.htm"
        )
        second = SourceObservation(
            observation_id=archive_document.source_observation_id,
            url=archive_document.url,
            source_type=archive_document.source_type,
            source_timestamp=(
                archive_document.published_at or archive_document.retrieved_at
            ),
            retrieved_at=archive_document.retrieved_at,
            feed="sec",
            delay_seconds=int(
                (
                    archive_document.retrieved_at
                    - (archive_document.published_at or archive_document.retrieved_at)
                ).total_seconds()
            ),
        )
        second_health = self.provider.health_attestation(archive_document)
        second_digest = self.cache.put(second, b"same payload")

        self.assertEqual(second_digest, first_digest)
        observation_files = sorted((self.root / "observations").rglob("*.json"))
        self.assertEqual(len(observation_files), 2)
        serialized = "\n".join(path.read_text(encoding="utf-8") for path in observation_files)
        self.assertIn(self.observation.observation_id, serialized)
        self.assertIn(second.observation_id, serialized)
        restored_first, first_payload = self.cache.get_observation(
            self.observation.observation_id,
            health=self.health(),
            as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
        )
        restored_second, second_payload = self.cache.get_observation(
            second.observation_id,
            health=second_health,
            as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
        )
        self.assertEqual(restored_first.url, self.observation.url)
        self.assertEqual(restored_second.url, second.url)
        self.assertEqual(first_payload, second_payload)

        with self.assertRaises(CacheIntegrityError):
            self.cache.get(
                first_digest,
                health=self.health(),
                as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
            )

        content_metadata = next(
            path
            for path in self.root.rglob(f"{first_digest}.json")
            if "sha256" in path.parts
        ).read_text(encoding="utf-8")
        self.assertNotIn("observation_id", content_metadata)
        self.assertNotIn(self.observation.url, content_metadata)
        self.assertNotIn(second.url, content_metadata)

        collision = SourceObservation(
            observation_id=self.observation.observation_id,
            url=self.observation.url,
            source_type=self.observation.source_type,
            source_timestamp=self.observation.source_timestamp,
            retrieved_at=self.observation.retrieved_at,
            feed=self.observation.feed,
            delay_seconds=self.observation.delay_seconds,
        )
        with self.assertRaises(CacheIntegrityError):
            self.cache.put(collision, b"different payload")

        self.assertEqual(
            sorted(path.name for path in self.root.rglob("*.bin")),
            [f"{first_digest}.bin"],
        )

    def test_observation_collision_is_atomic_under_concurrency(self) -> None:
        collision = SourceObservation(
            observation_id=self.observation.observation_id,
            url=self.observation.url,
            source_type=self.observation.source_type,
            source_timestamp=self.observation.source_timestamp,
            retrieved_at=self.observation.retrieved_at,
            feed=self.observation.feed,
            delay_seconds=self.observation.delay_seconds,
        )
        barrier = threading.Barrier(2)
        original = ContentCache._atomic_write

        def synchronized(path: Path, payload: bytes) -> None:
            if "observations" in path.parts:
                try:
                    barrier.wait(timeout=0.05)
                except threading.BrokenBarrierError:
                    pass
            original(path, payload)

        def write(payload: bytes) -> tuple[str, object]:
            try:
                return "ok", self.cache.put(collision, payload)
            except CacheIntegrityError as error:
                return "error", error

        with patch.object(ContentCache, "_atomic_write", staticmethod(synchronized)):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = tuple(pool.map(write, (b"first", b"second")))

        self.assertEqual(sorted(status for status, _ in results), ["error", "ok"])
        successful_digest = next(value for status, value in results if status == "ok")
        restored, payload = self.cache.get_observation(
            collision.observation_id,
            health=self.health(),
            as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
        )
        self.assertEqual(restored.content_hash, successful_digest)
        self.assertEqual(
            payload,
            b"first" if successful_digest == hashlib.sha256(b"first").hexdigest() else b"second",
        )
        self.assertEqual(len(list(self.root.rglob("*.bin"))), 1)

    def test_put_verifies_existing_metadata_before_reporting_success(self) -> None:
        digest = self.cache.put(self.observation, b"payload")
        metadata_path = next(
            path
            for path in self.root.rglob(f"{digest}.json")
            if "sha256" in path.parts
        )
        metadata_path.write_text("{}", encoding="utf-8")
        with self.assertRaises(CacheIntegrityError):
            self.cache.put(self.observation, b"payload")

    def test_health_attestation_scope_and_validity_are_closed_and_bounded(self) -> None:
        with self.assertRaises(ValueError):
            self.health(valid_until=datetime(2100, 1, 1, tzinfo=UTC))
        with self.assertRaises(ValueError):
            self.health(
                source_type="UNREVIEWED_SOURCE",
                entitlement_class="UNREVIEWED_ENTITLEMENT",
            )

    def test_health_attestation_identity_must_match_the_cached_observation(self) -> None:
        digest = self.cache.put(self.observation, b"subject-a-payload")
        subject_b_health = self.health(source_observation_id="obs-subject-b")

        with self.assertRaises(CacheSourceUnavailableError):
            self.cache.get(
                digest,
                health=subject_b_health,
                as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
            )

    def test_health_attestation_accepts_only_closed_official_source_scopes(self) -> None:
        poisoned = (
            {
                "source_type": "SEC_SUBMISSIONS",
                "origin": "https://attacker.example",
                "feed": "sec",
                "entitlement_class": "PUBLIC_SEC",
            },
            {
                "source_type": "SEC_SUBMISSIONS",
                "origin": "https://data.sec.gov",
                "feed": "self-issued",
                "entitlement_class": "PUBLIC_SEC",
            },
            {
                "source_type": "OFFICIAL_REFERENCE",
                "origin": "https://attacker.example",
                "feed": "operational-status",
                "entitlement_class": "OFFICIAL_REFERENCE",
            },
        )
        for values in poisoned:
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.health(**values)

    def test_current_health_cannot_restore_an_expired_latest_quote_observation(self) -> None:
        old = SourceObservation(
            observation_id="obs-latest-2020",
            url="https://data.alpaca.markets/v2/stocks/quotes/latest?feed=iex",
            source_type="ALPACA_LATEST_QUOTES",
            source_timestamp=datetime(2020, 1, 2, 14, 30, tzinfo=UTC),
            retrieved_at=datetime(2020, 1, 2, 14, 31, tzinfo=UTC),
            feed="iex",
            delay_seconds=60,
        )
        digest = self.cache.put(old, b'{"quotes":{"SPY":{}}}')
        health = cache_module.SourceHealthAttestation(
            source_observation_id=old.observation_id,
            source_type=old.source_type,
            origin="https://data.alpaca.markets",
            feed=old.feed,
            entitlement_class="ALPACA_IEX",
            checked_at=datetime(2026, 8, 14, 12, 45, tzinfo=UTC),
            valid_until=datetime(2026, 8, 14, 12, 50, tzinfo=UTC),
            healthy=True,
            entitlement_ok=True,
        )

        with self.assertRaises(CacheSourceUnavailableError):
            self.cache.get(
                digest,
                health=health,
                as_of=datetime(2026, 8, 14, 12, 46, tzinfo=UTC),
            )

    def test_observation_collision_is_atomic_across_processes(self) -> None:
        context = multiprocessing.get_context("fork")
        start = context.Event()
        results = context.Queue()
        collision = SourceObservation(
            observation_id="obs-process-collision",
            url=self.observation.url,
            source_type=self.observation.source_type,
            source_timestamp=self.observation.source_timestamp,
            retrieved_at=self.observation.retrieved_at,
            feed=self.observation.feed,
            delay_seconds=self.observation.delay_seconds,
        )
        processes = [
            context.Process(
                target=_process_cache_put,
                args=(str(self.root), collision, payload, start, results),
            )
            for payload in (b"process-first", b"process-second")
        ]
        try:
            for process in processes:
                process.start()
            start.set()
            outcomes = [results.get(timeout=5) for _ in processes]
            for process in processes:
                process.join(timeout=5)
                self.assertEqual(process.exitcode, 0)
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            results.close()
            results.join_thread()

        self.assertEqual(sorted(status for status, _ in outcomes), ["error", "ok"])
        self.assertEqual(len(list(self.root.rglob("*.bin"))), 1)


if __name__ == "__main__":
    unittest.main()
