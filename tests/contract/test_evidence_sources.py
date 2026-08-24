from __future__ import annotations

import ast
import hashlib
import inspect
import unittest
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from stock_monitor.evidence_authorities import EVIDENCE_AUTHORITIES
from stock_monitor.providers import evidence_sources as evidence_sources_module
from stock_monitor.providers.evidence_sources import (
    EvidenceSourceClient,
    ProposalSourceObservation,
)
from stock_monitor.providers.http import HttpResponse, ProviderResponseError


_ACCEPT = "application/json,application/xml,text/html,text/plain"
_ALLOWED_CONTENT_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/json",
        "application/rss+xml",
        "application/xml",
        "text/html",
        "text/plain",
        "text/xml",
    }
)
_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "stock_monitor"
    / "providers"
    / "evidence_sources.py"
)


class SequenceTransport:
    def __init__(
        self,
        responses: list[tuple[int, tuple[tuple[str, str], ...], bytes]],
    ) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, headers) -> HttpResponse:
        self.calls.append((url, dict(headers)))
        if not self._responses:
            raise AssertionError("unexpected evidence-source request")
        status, response_headers, body = self._responses.pop(0)
        return HttpResponse(status, response_headers, body, url)


def success(
    body: bytes = b'{"ok":true}',
    content_type: str = "application/json",
) -> tuple[int, tuple[tuple[str, str], ...], bytes]:
    return 200, (("Content-Type", content_type),), body


def client_for(
    transport: SequenceTransport,
    when: object = datetime(2026, 8, 24, 14, 0, tzinfo=UTC),
) -> EvidenceSourceClient:
    return EvidenceSourceClient(transport=transport, now=lambda: when)  # type: ignore[arg-type,return-value]


class EvidenceSourceObservationContractTests(unittest.TestCase):
    def test_observation_has_exact_immutable_fields_and_hides_body_from_repr(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        body = b"body-repr-canary"
        observation = client_for(SequenceTransport([success(body)])).fetch(authority)

        self.assertEqual(
            tuple(item.name for item in fields(ProposalSourceObservation)),
            (
                "observation_id",
                "symbol",
                "issuer_cik",
                "url",
                "publisher",
                "role",
                "event_class",
                "retrieved_at",
                "published_at",
                "timestamp_source",
                "content_sha256",
                "body",
            ),
        )
        self.assertNotIn(body.decode("ascii"), repr(observation))
        with self.assertRaises(FrozenInstanceError):
            observation.body = b"changed"  # type: ignore[misc]
        self.assertFalse(hasattr(observation, "__dict__"))

    def test_public_api_accepts_only_transport_clock_and_catalog_authority(self) -> None:
        constructor = inspect.signature(EvidenceSourceClient.__init__).parameters
        self.assertEqual(tuple(constructor), ("self", "transport", "now"))
        self.assertIs(constructor["transport"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIs(constructor["now"].kind, inspect.Parameter.KEYWORD_ONLY)
        fetch = inspect.signature(EvidenceSourceClient.fetch).parameters
        self.assertEqual(tuple(fetch), ("self", "authority"))
        self.assertFalse(
            any(
                parameter.kind
                in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                for parameter in (*constructor.values(), *fetch.values())
            )
        )

    def test_fetch_uses_exact_request_headers_policy_limits_and_media_types(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        transport = SequenceTransport([success()])
        original = evidence_sources_module.get_with_redirects
        calls: list[tuple[object, object, str, dict[str, str], dict[str, object]]] = []

        def recording_get(transport_arg, policy, url, headers, **options):
            calls.append((transport_arg, policy, url, dict(headers), dict(options)))
            return original(transport_arg, policy, url, headers, **options)

        with patch.object(
            evidence_sources_module,
            "get_with_redirects",
            side_effect=recording_get,
        ):
            observation = client_for(transport).fetch(authority)

        self.assertEqual(observation.url, authority.requested_url)
        self.assertEqual(len(calls), 1)
        actual_transport, policy, url, headers, options = calls[0]
        self.assertIs(actual_transport, transport)
        self.assertEqual(url, authority.requested_url)
        self.assertEqual(headers, {"Accept": _ACCEPT})
        self.assertEqual(
            policy.allowed_hosts,
            frozenset(
                urlsplit(value).hostname for value in authority.allowed_final_urls
            ),
        )
        self.assertEqual(options["max_bytes"], 4_194_304)
        self.assertEqual(
            frozenset(options["allowed_content_types"]),  # type: ignore[arg-type]
            _ALLOWED_CONTENT_TYPES,
        )
        self.assertEqual(set(options), {"allowed_content_types", "max_bytes", "exact_url_validator"})
        self.assertEqual(
            transport.calls,
            [
                (
                    authority.requested_url,
                    {"Accept": _ACCEPT, "Accept-Encoding": "identity"},
                )
            ],
        )

    def test_observation_copies_only_catalog_metadata_and_marks_page_time_unavailable(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        body = b'{"catalog":"metadata"}'
        observation = client_for(SequenceTransport([success(body)])).fetch(authority)

        self.assertEqual(observation.symbol, authority.symbol)
        self.assertEqual(observation.issuer_cik, authority.issuer_cik)
        self.assertEqual(observation.publisher, authority.publisher)
        self.assertEqual(observation.role, authority.role)
        self.assertEqual(observation.event_class, authority.event_class)
        self.assertIsNone(observation.published_at)
        self.assertEqual(observation.timestamp_source, "UNAVAILABLE")
        self.assertEqual(observation.content_sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(observation.body, body)


class EvidenceSourceAuthorityAndResponseTests(unittest.TestCase):
    def test_equal_but_caller_constructed_authority_is_rejected_before_transport(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        clone = replace(authority)
        transport = SequenceTransport([success()])

        with self.assertRaises(ValueError):
            client_for(transport).fetch(clone)

        self.assertEqual(clone, authority)
        self.assertIsNot(clone, authority)
        self.assertEqual(transport.calls, [])

    def test_off_catalog_url_is_rejected_before_transport(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        poisoned_url = f"{authority.requested_url}?cursor=off-catalog-canary"
        poisoned = replace(
            authority,
            requested_url=poisoned_url,
            allowed_final_urls=frozenset({poisoned_url}),
        )
        transport = SequenceTransport([success()])

        with self.assertRaises(ValueError) as raised:
            client_for(transport).fetch(poisoned)

        self.assertEqual(transport.calls, [])
        self.assertNotIn("off-catalog-canary", str(raised.exception))

    def test_cross_origin_redirect_is_rejected_before_second_request(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        transport = SequenceTransport(
            [
                (
                    302,
                    (("Location", "https://attacker.example/stolen"),),
                    b"redirect-body-canary",
                )
            ]
        )

        with self.assertRaises(ValueError):
            client_for(transport).fetch(authority)

        self.assertEqual(len(transport.calls), 1)

    def test_unlisted_same_origin_redirect_is_rejected_before_second_request(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        transport = SequenceTransport(
            [(302, (("Location", "/unlisted?cursor=redirect-canary"),), b"")]
        )

        with self.assertRaises(ValueError) as raised:
            client_for(transport).fetch(authority)

        self.assertEqual(len(transport.calls), 1)
        self.assertNotIn("redirect-canary", str(raised.exception))
        self.assertNotIn("cursor=", str(raised.exception))

    def test_wrong_type_empty_and_oversized_responses_are_rejected(self) -> None:
        fixtures = (
            success(b"wrong-type-body-canary", "image/png"),
            success(b""),
            success(b"x" * 4_194_305),
        )
        for fixture in fixtures:
            with self.subTest(size=len(fixture[2]), content_type=fixture[1]):
                with self.assertRaises(ProviderResponseError) as raised:
                    client_for(SequenceTransport([fixture])).fetch(
                        EVIDENCE_AUTHORITIES[0]
                    )
                self.assertNotIn("wrong-type-body-canary", str(raised.exception))

    def test_every_permitted_content_type_is_accepted(self) -> None:
        for content_type in sorted(_ALLOWED_CONTENT_TYPES):
            with self.subTest(content_type=content_type):
                observation = client_for(
                    SequenceTransport([success(b"accepted", content_type)])
                ).fetch(EVIDENCE_AUTHORITIES[0])
                self.assertEqual(observation.body, b"accepted")

    def test_naive_and_non_datetime_clocks_are_rejected(self) -> None:
        invalid_values = (
            datetime(2026, 8, 24, 14, 0),
            "2026-08-24T14:00:00Z",
            None,
        )
        for value in invalid_values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                client_for(SequenceTransport([success()]), value).fetch(
                    EVIDENCE_AUTHORITIES[0]
                )

    def test_errors_do_not_echo_redirect_query_values_or_response_bodies(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        query_canary = "query-secret-8f4d"
        body_canary = "body-secret-71ca"
        transport = SequenceTransport(
            [
                (
                    302,
                    (("Location", f"/unlisted?cursor={query_canary}"),),
                    body_canary.encode("ascii"),
                )
            ]
        )

        with self.assertRaises(ValueError) as raised:
            client_for(transport).fetch(authority)

        message = str(raised.exception)
        self.assertNotIn(query_canary, message)
        self.assertNotIn("cursor=", message)
        self.assertNotIn(body_canary, message)


class EvidenceSourceIdentityTests(unittest.TestCase):
    def test_identity_is_deterministic_from_normalized_utc_time_and_exact_bytes(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        body = b'{"identity":"exact-bytes"}\n'
        retrieved = datetime(
            2026,
            8,
            24,
            10,
            0,
            0,
            123456,
            tzinfo=timezone(-timedelta(hours=4)),
        )
        expected_time = datetime(2026, 8, 24, 14, 0, 0, 123456, tzinfo=UTC)
        expected_digest = hashlib.sha256(
            b"\0".join(
                (
                    authority.symbol.encode("ascii"),
                    authority.role.encode("ascii"),
                    authority.requested_url.encode("ascii"),
                    expected_time.isoformat(timespec="microseconds").encode("ascii"),
                    body,
                )
            )
        ).hexdigest()

        first = client_for(SequenceTransport([success(body)]), retrieved).fetch(authority)
        second = client_for(SequenceTransport([success(body)]), expected_time).fetch(authority)

        self.assertEqual(first.retrieved_at, expected_time)
        self.assertEqual(first.observation_id, f"proposal-{expected_digest[:24]}")
        self.assertEqual(second.observation_id, first.observation_id)

    def test_same_generated_id_cannot_bind_changed_bytes(self) -> None:
        authority = EVIDENCE_AUTHORITIES[0]
        transport = SequenceTransport([success(b"first-bytes"), success(b"second-bytes")])
        client = client_for(transport)

        with patch.object(
            evidence_sources_module,
            "_identity_digest",
            return_value="a" * 64,
        ):
            first = client.fetch(authority)
            with self.assertRaises(ValueError) as raised:
                client.fetch(authority)

        self.assertEqual(first.observation_id, f"proposal-{'a' * 24}")
        self.assertNotIn("second-bytes", str(raised.exception))

    def test_same_generated_id_cannot_bind_changed_source_identity(self) -> None:
        first_authority = EVIDENCE_AUTHORITIES[0]
        second_authority = EVIDENCE_AUTHORITIES[1]
        transport = SequenceTransport([success(b"same"), success(b"same")])
        client = client_for(transport)

        with patch.object(
            evidence_sources_module,
            "_identity_digest",
            return_value="b" * 64,
        ):
            client.fetch(first_authority)
            with self.assertRaises(ValueError):
                client.fetch(second_authority)


class EvidenceSourceArchitectureTests(unittest.TestCase):
    def test_adapter_imports_only_reviewed_http_egress_primitives(self) -> None:
        tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
        http_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.level == 1
            and node.module == "http"
        ]

        self.assertEqual(len(http_imports), 1)
        self.assertEqual(
            {alias.name for alias in http_imports[0].names},
            {"EgressPolicy", "GetTransport", "get_with_redirects"},
        )

    def test_adapter_has_no_direct_network_proxy_process_browser_or_trade_surface(self) -> None:
        tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
        forbidden_import_roots = {
            "aiohttp",
            "http",
            "httpcore",
            "httpx",
            "requests",
            "socket",
            "subprocess",
            "urllib",
            "urllib3",
            "webbrowser",
        }
        imported_roots: set[str] = set()
        public_interface_tokens: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
            elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if not node.name.startswith("_"):
                    public_interface_tokens.update(node.name.casefold().split("_"))

        self.assertEqual(imported_roots.intersection(forbidden_import_roots), set())
        self.assertEqual(
            public_interface_tokens.intersection(
                {"broker", "brokerage", "order", "proxy", "trade", "trading"}
            ),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
