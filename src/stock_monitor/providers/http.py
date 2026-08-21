"""A redirect-disabled, proxy-free, GET-only HTTP boundary."""

from __future__ import annotations

import ipaddress
import re
import time
import urllib.error
import urllib.parse
import urllib.request as url_request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol


_APPROVED_ALPACA_DATA_HOST = "data.alpaca.markets"
_ALPACA_SUFFIX = ".alpaca.markets"
_SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "cookie2",
    }
)
_TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_IPV4_COMPONENT = re.compile(r"(?:[0-9]+|0x[0-9a-f]+)\Z")
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
_MAX_URL_LENGTH = 8_192
_DEFAULT_MAX_BYTES = 8 * 1024 * 1024
_SENSITIVE_QUERY_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "key_id",
        "password",
        "secret",
        "signature",
        "token",
    }
)
_SENSITIVE_QUERY_SUFFIXES = (
    "apikey",
    "authorization",
    "credential",
    "keyid",
    "password",
    "secret",
    "signature",
    "token",
)


class NetworkPolicyError(ValueError):
    """A request target or hop violates the permanent network boundary."""


class ProviderResponseError(RuntimeError):
    """A source response is unsafe, unavailable, or malformed."""


class HttpStatusError(ProviderResponseError):
    """A source returned one exact non-success HTTP status."""

    def __init__(self, status: int, target: str) -> None:
        if type(status) is not int or not 100 <= status <= 599:
            raise ValueError("HTTP error status must be an integer from 100 through 599")
        self.status = status
        super().__init__(f"source returned HTTP {status} for {target}")


class ProviderDataError(ProviderResponseError):
    """Provider data cannot be accepted without identifying one subtype."""


class ProviderMalformedError(ProviderDataError):
    """A provider response cannot be safely interpreted."""


class ProviderIncompleteError(ProviderResponseError):
    """A paginated or multi-symbol response is incomplete."""


class HttpTransportError(ProviderResponseError):
    """A single-hop GET could not be completed."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    url: str

    def __post_init__(self) -> None:
        if type(self.status) is not int or not 100 <= self.status <= 599:
            raise ValueError("HTTP status must be an integer from 100 through 599")
        if not isinstance(self.body, bytes):
            raise TypeError("HTTP response body must be bytes")
        if not isinstance(self.url, str):
            raise TypeError("HTTP response URL must be text")
        normalized: list[tuple[str, str]] = []
        for pair in self.headers:
            if (
                not isinstance(pair, tuple)
                or len(pair) != 2
                or not all(isinstance(value, str) for value in pair)
            ):
                raise TypeError("HTTP response headers must be string pairs")
            normalized.append(pair)
        object.__setattr__(self, "headers", tuple(normalized))

    def header_values(self, name: str) -> tuple[str, ...]:
        target = name.casefold()
        return tuple(value for key, value in self.headers if key.casefold() == target)


class GetTransport(Protocol):
    def get(self, url: str, headers: Mapping[str, str]) -> HttpResponse: ...


def _canonical_host(value: str) -> str:
    try:
        ascii_value = value.encode("ascii").decode("ascii").casefold()
    except UnicodeError:
        raise NetworkPolicyError("host must be an ASCII DNS name") from None
    host = ascii_value[:-1] if ascii_value.endswith(".") else ascii_value
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise NetworkPolicyError("IP literals are prohibited")
    labels = host.split(".")
    if labels and all(_IPV4_COMPONENT.fullmatch(label) for label in labels):
        raise NetworkPolicyError("IP literals are prohibited")
    if (
        len(host) > 253
        or len(labels) < 2
        or any(not _DNS_LABEL.fullmatch(label) for label in labels)
        or host == "localhost"
        or host.endswith(".localhost")
        or host.endswith(".local")
    ):
        raise NetworkPolicyError("host must be an approved public DNS name")
    if host.endswith(_ALPACA_SUFFIX) and host != _APPROVED_ALPACA_DATA_HOST:
        raise NetworkPolicyError("an Alpaca transactional origin is prohibited")
    if "robinhood" in host:
        raise NetworkPolicyError("a brokerage origin is prohibited")
    return host


def _parse_url(url: str) -> tuple[urllib.parse.SplitResult, str, int]:
    if (
        not isinstance(url, str)
        or not url
        or len(url) > _MAX_URL_LENGTH
        or "\\" in url
        or any(ord(character) <= 32 or ord(character) == 127 for character in url)
    ):
        raise NetworkPolicyError("GET URL is malformed")
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError:
        raise NetworkPolicyError("GET URL is malformed") from None
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
    ):
        raise NetworkPolicyError(
            "only credential-free HTTPS GET URLs on port 443 are allowed"
        )
    host = _canonical_host(parsed.hostname)
    segments = tuple(
        urllib.parse.unquote(segment).casefold()
        for segment in parsed.path.split("/")
        if segment
    )
    prohibited_segment = "".join(("ord", "ers"))
    if prohibited_segment in segments:
        raise NetworkPolicyError("a transactional resource is prohibited")
    try:
        query_pairs = urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError:
        raise NetworkPolicyError("GET query is malformed") from None
    for name, _ in query_pairs:
        folded = name.casefold()
        if folded == "page_token":
            continue
        compact = re.sub(r"[^a-z0-9]", "", folded)
        if folded in _SENSITIVE_QUERY_NAMES or any(
            compact.endswith(suffix) for suffix in _SENSITIVE_QUERY_SUFFIXES
        ):
            raise NetworkPolicyError("credentials in GET query parameters are prohibited")
    return parsed, host, 443


@dataclass(frozen=True, slots=True)
class EgressPolicy:
    allowed_hosts: frozenset[str]

    def __init__(self, allowed_hosts: Iterable[str]) -> None:
        canonical = frozenset(_canonical_host(host) for host in allowed_hosts)
        if not canonical:
            raise NetworkPolicyError("at least one exact egress host is required")
        object.__setattr__(self, "allowed_hosts", canonical)

    def validate_get(self, url: str) -> None:
        """Validate a credential-free HTTPS GET target against exact DNS names."""
        _, host, _ = _parse_url(url)
        if host not in self.allowed_hosts:
            raise NetworkPolicyError("GET host is not allowlisted")


class NoAutomaticRedirects(url_request.HTTPRedirectHandler):
    """Return redirect responses to the shared hop validator."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _safe_target(url: str) -> str:
    try:
        parsed, host, _ = _parse_url(url)
    except NetworkPolicyError:
        return "<rejected-target>"
    return urllib.parse.urlunsplit(("https", host, parsed.path, "", ""))


def _clean_headers(headers: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(headers, Mapping):
        raise NetworkPolicyError("GET headers must be a mapping")
    result: dict[str, str] = {}
    seen: set[str] = set()
    for name, value in headers.items():
        if (
            not isinstance(name, str)
            or not _HEADER_NAME.fullmatch(name)
            or not isinstance(value, str)
            or not value
            or not value.isascii()
            or not value.isprintable()
            or value != value.strip()
        ):
            raise NetworkPolicyError("GET headers are malformed")
        folded = name.casefold()
        if folded in seen or folded in {"host", "content-length", "transfer-encoding"}:
            raise NetworkPolicyError("GET headers contain a prohibited or duplicate name")
        seen.add(folded)
        if folded != "accept-encoding":
            result[name] = value
    result["Accept-Encoding"] = "identity"
    return result


def _redirect_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.casefold() not in _SENSITIVE_HEADERS
        and not name.casefold().startswith("apca-")
    }


def _origin(url: str) -> tuple[str, str, int]:
    parsed, host, port = _parse_url(url)
    return parsed.scheme.casefold(), host, port


def _validate_success_response(
    response: HttpResponse,
    *,
    requested_url: str,
    allowed_content_types: frozenset[str],
    max_bytes: int,
) -> None:
    if not 200 <= response.status <= 299:
        raise HttpStatusError(response.status, _safe_target(requested_url))
    if not response.body.strip():
        raise ProviderMalformedError(
            f"source returned an empty response for {_safe_target(requested_url)}"
        )
    if len(response.body) > max_bytes:
        raise ProviderMalformedError(
            f"source response exceeded the size limit for {_safe_target(requested_url)}"
        )
    encodings = response.header_values("Content-Encoding")
    if len(encodings) > 1 or (
        encodings and encodings[0].strip().casefold() not in {"", "identity"}
    ):
        raise ProviderMalformedError("source returned an unsupported content encoding")
    types = response.header_values("Content-Type")
    if len(types) != 1:
        raise ProviderMalformedError("source response needs one content type")
    media_type = types[0].split(";", 1)[0].strip().casefold()
    if media_type not in allowed_content_types:
        raise ProviderMalformedError("source returned an unsupported content type")


def _retry_delay(response: HttpResponse, attempt: int) -> float:
    values = response.header_values("Retry-After")
    if len(values) == 1:
        value = values[0].strip()
        if value.isascii() and value.isdigit() and len(value) <= 10:
            return min(float(int(value)), 2.0)
    return 0.1 * (2**attempt)


def get_with_redirects(
    transport: GetTransport,
    policy: EgressPolicy,
    url: str,
    headers: Mapping[str, str],
    *,
    allowed_content_types: Iterable[str] = (
        "application/json",
        "text/html",
        "text/plain",
        "application/xml",
        "text/xml",
    ),
    max_bytes: int = _DEFAULT_MAX_BYTES,
    max_redirects: int = 3,
    max_attempts: int = 3,
    sleeper: Callable[[float], None] = time.sleep,
    exact_url_validator: Callable[[str], None] | None = None,
) -> HttpResponse:
    """Perform bounded GET hops with identical policy for real and fixture transports."""
    if (
        type(max_bytes) is not int
        or max_bytes <= 0
        or type(max_redirects) is not int
        or not 0 <= max_redirects <= 3
        or type(max_attempts) is not int
        or not 1 <= max_attempts <= 3
    ):
        raise ValueError("GET limits are invalid")
    media_types = frozenset(value.casefold() for value in allowed_content_types)
    if not media_types:
        raise ValueError("at least one response content type is required")

    current_url = url
    current_headers = _clean_headers(headers)
    visited: set[str] = set()
    redirects = 0
    while True:
        policy.validate_get(current_url)
        if exact_url_validator is not None:
            exact_url_validator(current_url)
        if current_url in visited:
            raise ProviderMalformedError("source redirect loop detected")
        visited.add(current_url)

        response: HttpResponse | None = None
        for attempt in range(max_attempts):
            try:
                candidate = transport.get(current_url, current_headers)
            except HttpTransportError:
                if attempt + 1 == max_attempts:
                    raise
                sleeper(0.1 * (2**attempt))
                continue
            if candidate.url != current_url:
                raise ProviderMalformedError(
                    "single-hop transport changed the request URL"
                )
            if candidate.status in _TRANSIENT_STATUSES:
                if attempt + 1 == max_attempts:
                    response = candidate
                    break
                sleeper(_retry_delay(candidate, attempt))
                continue
            response = candidate
            break
        if response is None:
            raise ProviderMalformedError("source GET did not produce a response")

        if response.status in _REDIRECT_STATUSES:
            locations = response.header_values("Location")
            if len(locations) != 1 or not locations[0].strip():
                raise ProviderMalformedError("source redirect needs one Location header")
            if redirects >= max_redirects:
                raise ProviderMalformedError("source exceeded the redirect limit")
            next_url = urllib.parse.urljoin(current_url, locations[0].strip())
            policy.validate_get(next_url)
            if exact_url_validator is not None:
                exact_url_validator(next_url)
            if _origin(next_url) != _origin(current_url):
                raise NetworkPolicyError("cross-origin redirects are prohibited")
            if next_url in visited:
                raise ProviderMalformedError("source redirect loop detected")
            redirects += 1
            current_url = next_url
            current_headers = _redirect_headers(current_headers)
            current_headers = _clean_headers(current_headers)
            continue

        _validate_success_response(
            response,
            requested_url=current_url,
            allowed_content_types=media_types,
            max_bytes=max_bytes,
        )
        if response.url != current_url:
            raise ProviderMalformedError("single-hop transport changed the request URL")
        return response


class HttpGetClient:
    """One policy-checked, redirect-disabled GET hop with no environment proxies."""

    def __init__(
        self,
        policy: EgressPolicy,
        *,
        timeout_seconds: float = 15.0,
        max_bytes: int = _DEFAULT_MAX_BYTES,
    ) -> None:
        if timeout_seconds <= 0 or max_bytes <= 0:
            raise ValueError("HTTP transport limits must be positive")
        self._policy = policy
        self._timeout_seconds = float(timeout_seconds)
        self._max_bytes = int(max_bytes)
        self._opener = url_request.build_opener(
            url_request.ProxyHandler({}),
            NoAutomaticRedirects,
        )

    def get(self, url: str, headers: Mapping[str, str]) -> HttpResponse:
        """Perform exactly one credential-free GET hop; redirects are returned."""
        self._policy.validate_get(url)
        safe_headers = _clean_headers(headers)
        request = url_request.Request(
            url,
            data=None,
            headers=safe_headers,
            method="GET",
        )
        try:
            raw = self._opener.open(request, timeout=self._timeout_seconds)
        except urllib.error.HTTPError as error:
            raw = error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise HttpTransportError(
                f"network GET failed for {_safe_target(url)}: {type(error).__name__}"
            ) from None

        try:
            status = int(raw.getcode())
            final_url = str(raw.geturl())
            raw_headers = raw.headers
            if hasattr(raw_headers, "raw_items"):
                response_headers = tuple(
                    (str(name), str(value)) for name, value in raw_headers.raw_items()
                )
            else:
                response_headers = tuple(
                    (str(name), str(value)) for name, value in raw_headers.items()
                )
            body = raw.read(self._max_bytes + 1)
        except (OSError, ValueError, TypeError) as error:
            raise HttpTransportError(
                f"network GET response failed for {_safe_target(url)}: {type(error).__name__}"
            ) from None
        finally:
            raw.close()
        if len(body) > self._max_bytes:
            raise ProviderMalformedError(
                f"source response exceeded the size limit for {_safe_target(url)}"
            )
        if final_url != url:
            raise ProviderMalformedError(
                "single-hop transport followed an automatic redirect"
            )
        return HttpResponse(
            status=status,
            headers=response_headers,
            body=body,
            url=url,
        )


__all__ = [
    "EgressPolicy",
    "GetTransport",
    "HttpGetClient",
    "HttpResponse",
    "HttpStatusError",
    "HttpTransportError",
    "NetworkPolicyError",
    "NoAutomaticRedirects",
    "ProviderIncompleteError",
    "ProviderDataError",
    "ProviderMalformedError",
    "ProviderResponseError",
    "get_with_redirects",
]
