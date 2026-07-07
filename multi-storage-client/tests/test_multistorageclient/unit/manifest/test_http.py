# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Transport-contract tests for deterministic HTTP service chunks."""

from __future__ import annotations

import math
import ssl
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
import requests
import urllib3
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict

from multistorageclient.manifest.http import HTTPServiceRangeReader, normalize_http_base_url
from multistorageclient.manifest.models import QueryParameter
from multistorageclient.types import Range, RetryableError


@dataclass
class RecordingRaw:
    """A response body that records bounded reads without loading it eagerly."""

    body: bytes
    error: BaseException | None = None
    read_sizes: list[int] = field(default_factory=list)
    returned_sizes: list[int] = field(default_factory=list)
    _position: int = 0

    def read(self, amount: int | None = None, *args: Any, **kwargs: Any) -> bytes:
        if self.error is not None:
            raise self.error
        size = -1 if amount is None else amount
        self.read_sizes.append(size)
        if size < 0:
            size = len(self.body) - self._position
        result = self.body[self._position : self._position + size]
        self._position += len(result)
        self.returned_sizes.append(len(result))
        return result


class RecordingResponse:
    """Minimal requests-compatible response that rejects unbounded ``.content`` reads."""

    def __init__(
        self,
        status_code: int,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
        raw_error: BaseException | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = CaseInsensitiveDict(headers or {})
        self.raw = RecordingRaw(body, raw_error)
        self.reason = "fixture response"
        self.url = "https://service.example.test/v1/clips/file.bin"
        self.iter_chunk_sizes: list[int] = []
        self.closed = False

    @property
    def content(self) -> bytes:
        raise AssertionError("HTTP service reads must use a bounded streamed body read, not response.content")

    def iter_content(self, chunk_size: int = 1, decode_unicode: bool = False):
        self.iter_chunk_sizes.append(chunk_size)
        while chunk := self.raw.read(chunk_size):
            yield chunk

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"fixture status {self.status_code}", response=cast(requests.Response, self))

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "RecordingResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def _reader(**changes: Any) -> HTTPServiceRangeReader:
    options = {
        "base_url": "https://service.example.test/v1",
        "binding_identity": "https://service.example.test/v1",
        "allowed_path_prefixes": ("clips", "public"),
        "allowed_query_parameters": ("format", "tag"),
    }
    options.update(changes)
    return HTTPServiceRangeReader(**options)


def _valid_response(*, body: bytes = b"xyz", headers: Mapping[str, str] | None = None) -> RecordingResponse:
    response_headers = {
        "Content-Range": "bytes 5-7/10",
        "Content-Length": "3",
    }
    response_headers.update(headers or {})
    return RecordingResponse(206, response_headers, body)


def _patch_request(monkeypatch: pytest.MonkeyPatch, result: object) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def request(_session: requests.Session, method: str, url: str, **kwargs: Any) -> object:
        calls.append({"method": method, "url": url, "kwargs": kwargs})
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(requests.sessions.Session, "request", request)
    return calls


def _read(reader: HTTPServiceRangeReader) -> bytes:
    return reader.read(
        "clips/file.bin",
        (QueryParameter("format", "raw"),),
        Range(5, 3),
        total_size=10,
    )


@pytest.mark.parametrize("reserved_header", ["Range", "range", "Accept-Encoding", "HOST", "Content-Length"])
def test_http_service_reader_rejects_configured_protocol_control_headers(reserved_header: str) -> None:
    """Callers may configure credentials, but cannot override range-wire invariants."""
    with pytest.raises(ValueError):
        _reader(headers={reserved_header: "caller-controlled"})


@pytest.mark.parametrize("header_name", ["Range ", "Host\t", "Accept Encoding", "X\nInjected", ""])
def test_http_service_reader_rejects_non_token_header_names_before_reserved_name_matching(header_name: str) -> None:
    """Header names are RFC HTTP tokens, so whitespace lookalikes cannot evade protocol controls."""
    with pytest.raises(ValueError):
        _reader(headers={header_name: "caller-controlled"})


@pytest.mark.parametrize("header_value", ["line\rbreak", "line\nbreak", "nul\x00byte", "delete\x7fbyte"])
def test_http_service_reader_rejects_control_characters_in_header_values(header_value: str) -> None:
    """Configured header values cannot inject or corrupt HTTP wire framing."""
    with pytest.raises(ValueError, match="header values"):
        _reader(headers={"X-Configured": header_value})


def test_http_service_reader_rejects_case_insensitive_duplicate_header_names() -> None:
    """One semantic header has one configured value regardless of spelling case."""
    with pytest.raises(ValueError, match="duplicate"):
        _reader(headers={"X-Trace": "first", "x-trace": "second"})


@pytest.mark.parametrize(
    ("option", "value"),
    [
        pytest.param("verify_tls", 1, id="verify-integer"),
        pytest.param("verify_tls", "true", id="verify-string"),
        pytest.param("allow_insecure_http", 1, id="allow-insecure-integer"),
        pytest.param("allow_insecure_http", "false", id="allow-insecure-string"),
    ],
)
def test_http_service_reader_rejects_non_boolean_security_switches(option: str, value: object) -> None:
    """TLS policy switches require exact booleans rather than truthy configuration values."""
    with pytest.raises(ValueError, match="boolean"):
        _reader(**{option: value})


def test_http_base_url_normalizer_rejects_a_non_boolean_insecure_transport_switch() -> None:
    """Direct callers receive the same exact security-switch validation as the reader constructor."""
    with pytest.raises(ValueError, match="boolean"):
        normalize_http_base_url("https://service.example.test/v1", cast(Any, 1))


def test_http_service_reader_requires_https_unless_explicitly_opted_into_insecure_http() -> None:
    """Plain HTTP is opt-in so a service binding cannot silently downgrade TLS."""
    with pytest.raises(ValueError):
        _reader(base_url="http://service.example.test/v1")

    reader = _reader(base_url="http://service.example.test/v1", allow_insecure_http=True)
    assert reader.binding_identity == "https://service.example.test/v1"


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "service.example.test/v1",
        "ftp://service.example.test/v1",
        "https:///v1",
        "https://user:secret@service.example.test/v1",
        "https://service.example.test/v1?unexpected=query",
        "https://service.example.test/v1#fragment",
        "https://service.example.test/v1/../escape",
        "https://service.example.test/v1//double-segment",
        "https://service.example.test/v1%2Fencoded-segment",
        "https://service.example.test//v1",
        "https://service.example.test\\v1",
        "https://service.example.test%40evil.example/v1",
        "https://service.example.test%2Fevil.example/v1",
        "https://service.example.test:443@evil.example/v1",
        "https://service.example.test/v1\n",
        "https://service.example.test\t/v1",
    ],
)
def test_http_service_reader_rejects_malformed_or_ambiguous_base_urls(base_url: str) -> None:
    """The configured base is an origin plus canonical base path, never a request fragment."""
    with pytest.raises(ValueError):
        _reader(base_url=base_url)


def test_http_service_reader_rebuilds_the_transport_base_from_canonical_authority_components() -> None:
    """The request base cannot retain raw authority spelling that differs from its parsed origin."""
    reader = _reader(base_url="HTTPS://SERVICE.EXAMPLE.TEST:443/v1")

    assert reader._base_url == "https://service.example.test:443/v1"
    assert reader._build_url("clips/file.bin", ()) == "https://service.example.test:443/v1/clips/file.bin"


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("allowed_path_prefixes", ()),
        ("allowed_path_prefixes", ("",)),
        ("allowed_path_prefixes", ("/clips",)),
        ("allowed_path_prefixes", ("clips//nested",)),
        ("allowed_path_prefixes", ("clips//",)),
        ("allowed_path_prefixes", ("clips/../escape",)),
        ("allowed_path_prefixes", ("clips%2Fnested",)),
        ("allowed_path_prefixes", ("clips", "clips")),
        ("allowed_query_parameters", ("",)),
        ("allowed_query_parameters", ("format", "format")),
        ("allowed_query_parameters", ("format&other",)),
        ("allowed_query_parameters", ("format%20encoded",)),
    ],
)
def test_http_service_reader_rejects_malformed_allowlist_configuration(option: str, value: tuple[str, ...]) -> None:
    """Allowlist configuration is normalized and validated before it can be used for authorization."""
    with pytest.raises(ValueError):
        _reader(**{option: value})


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/clips/file.bin",
        "https://service.example.test/clips/file.bin",
        "clips%2Ffile.bin",
        "clips/file.bin?variant=preview",
        "clips/file.bin#fragment",
        "//authority/file.bin",
        "clips//file.bin",
        "clips\\file.bin",
        "clips\x00file.bin",
        "clips/",
        "./clips/file.bin",
        "../clips/file.bin",
        "clips/../file.bin",
        "clips2/file.bin",
        "publicish/file.bin",
        "clips/%2e%2e/public/file.bin",
        "clips%2f../public/file.bin",
    ],
)
def test_http_service_reader_rejects_invalid_or_outside_allowlist_paths(path: str) -> None:
    """Prefix matching uses complete POSIX segments, never a raw string prefix."""
    with pytest.raises(ValueError):
        _reader().validate(path, ())


def test_http_service_reader_accepts_allowed_prefixes_and_unicode_path_text() -> None:
    """The direct path is Unicode text; URL escaping happens exactly once at transport time."""
    reader = _reader()

    assert reader.validate("clips", ()) is None
    assert reader.validate("clips/日本語 file.bin", ()) is None
    assert reader.validate("public/shared.bin", ()) is None


@pytest.mark.parametrize(
    "query",
    [
        (QueryParameter("", "value"),),
        (QueryParameter("not-allowed", "value"),),
    ],
)
def test_http_service_reader_rejects_empty_or_unallowlisted_query_names(query: Sequence[QueryParameter]) -> None:
    """Query names are validated before any network request can be sent."""
    with pytest.raises(ValueError):
        _reader().validate("clips/file.bin", query)


def test_http_service_reader_allows_a_query_free_binding_to_validate_and_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty query allowlist is valid when a deterministic service accepts no query parameters."""
    response = _valid_response()
    calls = _patch_request(monkeypatch, response)
    reader = _reader(allowed_query_parameters=())

    assert reader.validate("clips/file.bin", ()) is None
    assert reader.read("clips/file.bin", (), Range(5, 3), total_size=10) == b"xyz"
    assert calls[0]["url"] == "https://service.example.test/v1/clips/file.bin"
    assert response.closed


def test_http_service_reader_preserves_duplicate_query_names_and_empty_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ordered duplicate query parameters are part of deterministic service identity."""
    response = _valid_response()
    calls = _patch_request(monkeypatch, response)
    reader = _reader(headers={"Authorization": "Bearer configured-secret", "X-Trace": "manifest"})

    result = reader.read(
        "clips/日本語 file.bin",
        (
            QueryParameter("format", "raw value"),
            QueryParameter("tag", ""),
            QueryParameter("format", "derived value"),
        ),
        Range(5, 3),
        total_size=10,
    )

    assert result == b"xyz"
    assert calls == [
        {
            "method": "GET",
            "url": (
                "https://service.example.test/v1/clips/"
                "%E6%97%A5%E6%9C%AC%E8%AA%9E%20file.bin?format=raw%20value&tag=&format=derived%20value"
            ),
            "kwargs": {
                "headers": {
                    "Authorization": "Bearer configured-secret",
                    "X-Trace": "manifest",
                    "Range": "bytes=5-7",
                    "Accept-Encoding": "identity",
                },
                "timeout": (60, 60),
                "verify": True,
                "allow_redirects": False,
                "stream": True,
            },
        }
    ]
    assert response.raw.read_sizes
    assert max(response.raw.read_sizes) <= 4
    assert sum(response.raw.returned_sizes) <= 4
    assert response.closed


def test_http_service_reader_passes_configured_timeouts_and_tls_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection and read bounds remain distinct, and TLS verification reaches requests."""
    response = _valid_response(headers={"Content-Encoding": "identity"})
    calls = _patch_request(monkeypatch, response)
    reader = _reader(connect_timeout_seconds=1.5, read_timeout_seconds=2.5, verify_tls=False)

    assert _read(reader) == b"xyz"
    assert calls[0]["kwargs"]["timeout"] == (1.5, 2.5)
    assert calls[0]["kwargs"]["verify"] is False
    assert response.closed


@pytest.mark.parametrize(
    ("option", "value"),
    [
        pytest.param("connect_timeout_seconds", True, id="connect-bool"),
        pytest.param("read_timeout_seconds", False, id="read-bool"),
        pytest.param("connect_timeout_seconds", math.nan, id="connect-nan"),
        pytest.param("read_timeout_seconds", math.inf, id="read-inf"),
        pytest.param("connect_timeout_seconds", -math.inf, id="connect-negative-inf"),
    ],
)
def test_http_service_reader_rejects_non_finite_or_boolean_timeouts(option: str, value: object) -> None:
    """Transport timeouts must be finite positive real durations, never truthy sentinels."""
    with pytest.raises(ValueError, match="finite positive"):
        _reader(**{option: value})


def test_http_service_reader_session_ignores_ambient_proxy_and_netrc_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """A manifest service binding is isolated from process-level proxy and netrc state."""
    netrc_path = tmp_path / "netrc"
    netrc_path.write_text("machine service.example.test login ambient password secret\n", encoding="utf-8")
    monkeypatch.setenv("HTTPS_PROXY", "http://ambient-proxy.example.test:8080")
    monkeypatch.setenv("NETRC", str(netrc_path))

    session = _reader()._session()

    assert session.trust_env is False
    assert session.merge_environment_settings("https://service.example.test/v1", {}, None, True, None)["proxies"] == {}
    prepared = session.prepare_request(requests.Request("GET", "https://service.example.test/v1"))
    assert "Authorization" not in prepared.headers


def test_http_service_reader_clears_response_cookies_before_the_next_service_call() -> None:
    """Set-Cookie state from one manifest read cannot influence the next request."""

    class CookieRecordingSession:
        def __init__(self) -> None:
            self.cookies = RequestsCookieJar()
            self.cookies_before_request: list[dict[str, str]] = []

        def request(self, *_args: Any, **_kwargs: Any) -> RecordingResponse:
            self.cookies_before_request.append(self.cookies.get_dict())
            if len(self.cookies_before_request) == 1:
                response = _valid_response(headers={"Set-Cookie": "session=ambient-service-state"})
                self.cookies.set("session", "ambient-service-state")
                return response
            return _valid_response()

    reader = _reader()
    session = CookieRecordingSession()
    reader._thread_local.session = session

    assert _read(reader) == b"xyz"
    assert _read(reader) == b"xyz"
    assert session.cookies_before_request == [{}, {}]
    assert session.cookies.get_dict() == {}


@pytest.mark.parametrize("byte_range", [Range(-1, 1), Range(0, -1), Range(0, 0), Range(9, 2)])
def test_http_service_reader_rejects_invalid_logical_ranges_without_transport(
    monkeypatch: pytest.MonkeyPatch, byte_range: Range
) -> None:
    """Service range requests must be nonempty and fully bounded by their declared output size."""
    calls = _patch_request(monkeypatch, _valid_response())

    with pytest.raises(ValueError):
        _reader().read("clips/file.bin", (), byte_range, total_size=10)

    assert calls == []


@pytest.mark.parametrize("total_size", [-1, 0])
def test_http_service_reader_rejects_nonpositive_declared_total_sizes(
    monkeypatch: pytest.MonkeyPatch, total_size: int
) -> None:
    """A response total must describe a nonempty service object before it can be ranged."""
    calls = _patch_request(monkeypatch, _valid_response())

    with pytest.raises(ValueError):
        _reader().read("clips/file.bin", (), Range(0, 1), total_size=total_size)

    assert calls == []


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
def test_http_service_reader_classifies_transient_statuses_as_retryable(
    monkeypatch: pytest.MonkeyPatch, status_code: int
) -> None:
    """The MSC retry layer receives only explicitly transient HTTP failures."""
    _patch_request(monkeypatch, RecordingResponse(status_code))

    with pytest.raises(RetryableError):
        _read(_reader())


@pytest.mark.parametrize("status_code", [200, 201, 204, 302, 400, 401, 403, 404, 416, 418, 501, 505])
def test_http_service_reader_classifies_redirects_and_nontransient_statuses_as_io_errors(
    monkeypatch: pytest.MonkeyPatch, status_code: int
) -> None:
    """A strict range contract never follows redirects or accepts a non-206 response."""
    _patch_request(monkeypatch, RecordingResponse(status_code))

    with pytest.raises(IOError):
        _read(_reader())


@pytest.mark.parametrize(
    "request_error",
    [
        requests.RequestException("unclassified request failure"),
        requests.exceptions.SSLError("certificate verification failed"),
    ],
)
def test_http_service_reader_classifies_generic_and_tls_request_failures_as_io_errors(
    monkeypatch: pytest.MonkeyPatch, request_error: requests.RequestException
) -> None:
    """Only explicit transient transport failures are retryable; TLS failures fail closed."""
    _patch_request(monkeypatch, request_error)

    with pytest.raises(IOError):
        _read(_reader())


@pytest.mark.parametrize(
    "request_error",
    [
        requests.ConnectTimeout("connection timed out"),
        requests.ReadTimeout("read timed out"),
        requests.ConnectionError("connection reset"),
    ],
)
def test_http_service_reader_classifies_requests_transport_failures_as_retryable(
    monkeypatch: pytest.MonkeyPatch, request_error: requests.RequestException
) -> None:
    """Connection and timeout exceptions retain retryability after requests wraps them."""
    _patch_request(monkeypatch, request_error)

    with pytest.raises(RetryableError):
        _read(_reader())


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Length": "3"},
        {"Content-Range": "bytes 5-7/10"},
        {"Content-Range": "bytes 5-8/10", "Content-Length": "3"},
        {"Content-Range": "bytes 4-6/10", "Content-Length": "3"},
        {"Content-Range": "bytes 5-7/11", "Content-Length": "3"},
        {"Content-Range": "bytes 5-7/*", "Content-Length": "3"},
        {"Content-Range": "not-a-content-range", "Content-Length": "3"},
        {"Content-Range": "bytes 5-7/10", "Content-Length": "not-an-integer"},
        {"Content-Range": "bytes 5-7/10", "Content-Length": "2"},
        {"Content-Range": "bytes 5-7/10", "Content-Length": "4"},
        {"Content-Range": "bytes 5-7/10", "Content-Length": "3", "Content-Encoding": "gzip"},
    ],
)
def test_http_service_reader_rejects_malformed_or_mismatched_response_metadata(
    monkeypatch: pytest.MonkeyPatch, headers: Mapping[str, str]
) -> None:
    """The 206 headers must exactly describe the requested bytes and declared total."""
    _patch_request(monkeypatch, RecordingResponse(206, headers, b"xyz"))

    with pytest.raises(IOError):
        _read(_reader())


@pytest.mark.parametrize(
    ("response", "error_type"),
    [
        (RecordingResponse(500), RetryableError),
        (RecordingResponse(206, {"Content-Range": "bytes 5-7/10", "Content-Length": "2"}, b"xy"), IOError),
    ],
)
def test_http_service_reader_closes_response_on_status_and_metadata_errors(
    monkeypatch: pytest.MonkeyPatch, response: RecordingResponse, error_type: type[Exception]
) -> None:
    """A response is closed on every error path after requests has handed it to the reader."""
    _patch_request(monkeypatch, response)

    with pytest.raises(error_type):
        _read(_reader())

    assert response.closed


@pytest.mark.parametrize("body", [b"xy", b"x" * 1024])
def test_http_service_reader_rejects_short_or_extra_response_bodies_with_a_bounded_read(
    monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    """Body length is verified without reading more than the requested bytes plus one sentinel byte."""
    response = _valid_response(body=body)
    _patch_request(monkeypatch, response)

    with pytest.raises(IOError):
        _read(_reader())

    assert response.raw.read_sizes
    assert max(response.raw.read_sizes) <= 4
    assert sum(response.raw.returned_sizes) <= 4
    assert response.closed


def test_http_service_reader_classifies_stream_connection_failures_as_retryable_and_closes_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connection loss while consuming a validated 206 body remains retryable."""
    response = RecordingResponse(
        206,
        {"Content-Range": "bytes 5-7/10", "Content-Length": "3"},
        raw_error=requests.ConnectionError("stream connection reset"),
    )
    _patch_request(monkeypatch, response)

    with pytest.raises(RetryableError):
        _read(_reader())

    assert response.closed


@pytest.mark.parametrize(
    ("name", "raw_error"),
    [
        pytest.param(
            "urllib3-read-timeout",
            urllib3.exceptions.ReadTimeoutError(
                cast(Any, None), "https://service.example.test/v1/clips/file.bin", "timed out"
            ),
            id="urllib3-read-timeout",
        ),
        pytest.param(
            "urllib3-protocol-timeout",
            urllib3.exceptions.ProtocolError("Connection aborted.", TimeoutError("timed out")),
            id="urllib3-protocol-timeout",
        ),
        pytest.param(
            "urllib3-protocol",
            urllib3.exceptions.ProtocolError("Connection aborted."),
            id="urllib3-protocol",
        ),
        pytest.param("raw-timeout", TimeoutError("timed out"), id="raw-timeout"),
    ],
)
def test_http_service_reader_classifies_real_stream_timeout_and_protocol_failures_as_retryable(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    raw_error: BaseException,
) -> None:
    """urllib3 exceptions raised by ``response.raw`` retain retryability across the HTTP boundary."""
    response = RecordingResponse(
        206,
        {"Content-Range": "bytes 5-7/10", "Content-Length": "3"},
        raw_error=raw_error,
    )
    _patch_request(monkeypatch, response)

    with pytest.raises(RetryableError):
        _read(_reader())

    assert response.closed, name


def test_http_service_reader_keeps_tls_failures_wrapped_by_urllib3_protocol_errors_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A protocol wrapper must not make a certificate or TLS failure retryable."""
    response = RecordingResponse(
        206,
        {"Content-Range": "bytes 5-7/10", "Content-Length": "3"},
        raw_error=urllib3.exceptions.ProtocolError("Connection aborted.", ssl.SSLError("certificate verify failed")),
    )
    _patch_request(monkeypatch, response)

    with pytest.raises(IOError):
        _read(_reader())

    assert response.closed
