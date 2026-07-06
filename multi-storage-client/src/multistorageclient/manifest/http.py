# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""HTTP byte-range service binding for virtual manifests."""

from __future__ import annotations

import math
import re
import ssl
import threading
from collections.abc import Mapping, Sequence
from numbers import Real
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

from ..types import Range, RetryableError
from .models import QueryParameter

_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_CONTENT_RANGE = re.compile(r"^bytes ([0-9]+)-([0-9]+)/([0-9]+)$")
_HTTP_TOKEN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_RESERVED_HEADERS = frozenset({"range", "accept-encoding", "host", "content-length"})


def _require_requests() -> Any:
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "Requests is required for virtual manifest HTTP services. "
            "Install it with: pip install multi-storage-client[virtual-manifest]"
        ) from exc
    return requests


def _validate_relative_path(path: str, *, description: str) -> None:
    if not isinstance(path, str) or not path:
        raise ValueError(f"{description} must be a non-empty relative path.")
    if path.startswith("/") or "\\" in path or "\x00" in path or _URI_SCHEME.match(path):
        raise ValueError(f"{description} must be a normalized relative POSIX path.")
    if any(character in path for character in ("%", "?", "#")):
        raise ValueError(f"{description} must be unescaped and query-free.")
    if any(segment in ("", ".", "..") for segment in path.split("/")):
        raise ValueError(f"{description} must be normalized.")


def _validate_query_name(name: str, *, description: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError(f"{description} must be a non-empty string.")
    if any(character in name for character in ("\x00", "%", "&", "=", "?", "#", "/", "\\")):
        raise ValueError(f"{description} contains a reserved URL character.")


def normalize_http_base_url(base_url: str, allow_insecure_http: bool) -> str:
    """Return one canonical HTTP origin and normalized base path for a service binding."""
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("base_url must be a non-empty HTTP(S) URL.")
    if "\\" in base_url or any(ord(character) < 32 or ord(character) == 127 for character in base_url):
        raise ValueError("base_url cannot contain control characters or backslashes.")
    try:
        parts = urlsplit(base_url)
        port = parts.port
    except ValueError as exc:
        raise ValueError("base_url has an invalid authority.") from exc
    if parts.scheme not in {"https", "http"} or not parts.netloc or not parts.hostname:
        raise ValueError("base_url must have a valid HTTP(S) authority.")
    if parts.scheme != "https" and not allow_insecure_http:
        raise ValueError("base_url must use HTTPS unless allow_insecure_http is enabled.")
    if parts.username is not None or parts.password is not None or parts.query or parts.fragment:
        raise ValueError("base_url cannot contain userinfo, query parameters, or fragments.")
    if "@" in parts.netloc or "%" in parts.netloc or parts.netloc.endswith(":"):
        raise ValueError("base_url has an ambiguous authority.")

    hostname = parts.hostname
    if hostname is None:
        raise ValueError("base_url must have a valid HTTP(S) authority.")
    try:
        canonical_host = hostname.lower() if ":" in hostname else hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("base_url has an invalid hostname.") from exc
    authority = f"[{canonical_host}]" if ":" in canonical_host else canonical_host
    if port is not None:
        authority = f"{authority}:{port}"

    raw_path = parts.path
    if raw_path.startswith("//") or raw_path.endswith("//"):
        raise ValueError("base_url path must be normalized.")
    path = raw_path.rstrip("/")
    if path:
        if not path.startswith("/"):
            raise ValueError("base_url path must be normalized.")
        try:
            _validate_relative_path(path[1:], description="base_url path")
        except ValueError as exc:
            raise ValueError("base_url path must be normalized.") from exc
        path = "/" + quote(path[1:], safe="/")
    return f"{parts.scheme}://{authority}{path}"


class HTTPServiceRangeReader:
    """Read deterministic service output through a strict HTTP range contract."""

    def __init__(
        self,
        *,
        base_url: str,
        binding_identity: str,
        allowed_path_prefixes: Sequence[str],
        allowed_query_parameters: Sequence[str],
        headers: Mapping[str, str] | None = None,
        connect_timeout_seconds: float = 60,
        read_timeout_seconds: float = 60,
        verify_tls: bool = True,
        allow_insecure_http: bool = False,
    ) -> None:
        self._base_url = self._normalize_base_url(base_url, allow_insecure_http)
        if not isinstance(binding_identity, str) or not binding_identity:
            raise ValueError("binding_identity must be a non-empty string.")
        self._allowed_path_prefixes = self._validate_path_prefixes(allowed_path_prefixes)
        self._allowed_query_parameters = self._validate_query_parameters(allowed_query_parameters)
        self._headers = self._validate_headers(headers)
        connect_timeout_seconds = self._validate_timeout(connect_timeout_seconds, "connect_timeout_seconds")
        read_timeout_seconds = self._validate_timeout(read_timeout_seconds, "read_timeout_seconds")
        self._binding_identity = binding_identity
        self._connect_timeout_seconds = connect_timeout_seconds
        self._read_timeout_seconds = read_timeout_seconds
        self._verify_tls = verify_tls
        self._allow_insecure_http = allow_insecure_http
        self._thread_local = threading.local()

    @staticmethod
    def _validate_timeout(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be a finite positive real number.")
        return float(value)

    @staticmethod
    def _normalize_base_url(base_url: str, allow_insecure_http: bool) -> str:
        return normalize_http_base_url(base_url, allow_insecure_http)

    @staticmethod
    def _validate_path_prefixes(prefixes: Sequence[str]) -> tuple[str, ...]:
        if not isinstance(prefixes, Sequence) or isinstance(prefixes, (str, bytes)) or not prefixes:
            raise ValueError("allowed_path_prefixes must be a non-empty sequence.")
        result = tuple(prefixes)
        if len(set(result)) != len(result):
            raise ValueError("allowed_path_prefixes must not contain duplicates.")
        for prefix in result:
            _validate_relative_path(prefix, description="allowed path prefix")
        return result

    @staticmethod
    def _validate_query_parameters(parameters: Sequence[str]) -> frozenset[str]:
        if not isinstance(parameters, Sequence) or isinstance(parameters, (str, bytes)):
            raise ValueError("allowed_query_parameters must be a sequence.")
        result = tuple(parameters)
        if len(set(result)) != len(result):
            raise ValueError("allowed_query_parameters must not contain duplicates.")
        for parameter in result:
            _validate_query_name(parameter, description="allowed query parameter")
        return frozenset(result)

    @staticmethod
    def _validate_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
        result = dict(headers or {})
        for name, value in result.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise ValueError("HTTP headers must have string names and values.")
            if _HTTP_TOKEN.fullmatch(name) is None:
                raise ValueError("HTTP header names must be RFC HTTP tokens.")
            if name.lower() in _RESERVED_HEADERS:
                raise ValueError(f"HTTP header {name!r} is controlled by the manifest range protocol.")
        return result

    def _session(self) -> Any:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = _require_requests().Session()
            session.trust_env = False
            self._thread_local.session = session
        return session

    def _build_url(self, path: str, query: Sequence[QueryParameter]) -> str:
        encoded_path = quote(path, safe="/")
        url = f"{self._base_url}/{encoded_path}" if self._base_url else encoded_path
        if query:
            url += "?" + urlencode([(item.name, item.value) for item in query], doseq=True, quote_via=quote, safe="")
        return url

    @staticmethod
    def _classify_request_exception(error: Exception, requests: Any) -> Exception:
        if isinstance(error, RetryableError):
            return error

        try:
            import urllib3
        except ImportError:  # pragma: no cover - requests requires urllib3 at runtime
            urllib3 = None

        exception_chain = HTTPServiceRangeReader._exception_chain(error)
        tls_error_types: tuple[type[BaseException], ...] = (requests.exceptions.SSLError, ssl.SSLError)
        if urllib3 is not None:
            tls_error_types += (urllib3.exceptions.SSLError,)
        if any(isinstance(exception, tls_error_types) for exception in exception_chain):
            return IOError("HTTP service TLS request failed")
        if isinstance(
            error,
            (
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
        ):
            return RetryableError("HTTP service transport request failed")
        retryable_body_error_types: tuple[type[BaseException], ...] = (TimeoutError,)
        if urllib3 is not None:
            retryable_body_error_types += (
                urllib3.exceptions.ProtocolError,
                urllib3.exceptions.ReadTimeoutError,
                urllib3.exceptions.TimeoutError,
            )
        if any(isinstance(exception, retryable_body_error_types) for exception in exception_chain):
            return RetryableError("HTTP service transport body read failed")
        if isinstance(error, requests.exceptions.RequestException):
            return IOError("HTTP service request failed")
        return error

    @staticmethod
    def _exception_chain(error: BaseException) -> tuple[BaseException, ...]:
        """Return exception causes and wrapped arguments without looping on self-references."""
        pending = [error]
        chain: list[BaseException] = []
        seen: set[int] = set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            chain.append(current)
            for nested in (current.__cause__, current.__context__, *current.args):
                if isinstance(nested, BaseException):
                    pending.append(nested)
        return tuple(chain)

    @property
    def binding_identity(self) -> str:
        """Return the non-secret service binding identity."""
        return self._binding_identity

    def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
        """Validate a service path and query without issuing a request."""
        _validate_relative_path(path, description="service path")
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in self._allowed_path_prefixes):
            raise ValueError(f"Service path {path!r} is not allowlisted.")
        if not isinstance(query, Sequence) or isinstance(query, (str, bytes)):
            raise ValueError("service query must be a sequence of query parameters.")
        for item in query:
            if not isinstance(item, QueryParameter):
                raise ValueError("service query items must be QueryParameter values.")
            _validate_query_name(item.name, description="service query name")
            if item.name not in self._allowed_query_parameters:
                raise ValueError(f"Service query parameter {item.name!r} is not allowlisted.")
            if not isinstance(item.value, str):
                raise ValueError("service query values must be strings.")

    def read(
        self,
        path: str,
        query: Sequence[QueryParameter],
        byte_range: Range,
        total_size: int,
    ) -> bytes:
        """Read an exact service byte range."""
        self.validate(path, query)
        if (
            not isinstance(byte_range.offset, int)
            or isinstance(byte_range.offset, bool)
            or not isinstance(byte_range.size, int)
            or isinstance(byte_range.size, bool)
            or byte_range.offset < 0
            or byte_range.size <= 0
        ):
            raise ValueError("HTTP service ranges must have non-negative offset and positive size.")
        if not isinstance(total_size, int) or isinstance(total_size, bool) or total_size <= 0:
            raise ValueError("HTTP service total_size must be positive.")
        if byte_range.offset > total_size - byte_range.size:
            raise ValueError("HTTP service range exceeds the declared total size.")

        requests = _require_requests()
        end = byte_range.offset + byte_range.size - 1
        headers = dict(self._headers)
        headers["Range"] = f"bytes={byte_range.offset}-{end}"
        headers["Accept-Encoding"] = "identity"
        response: Any = None
        session = self._session()
        try:
            session.cookies.clear()
            response = session.request(
                "GET",
                self._build_url(path, query),
                headers=headers,
                timeout=(self._connect_timeout_seconds, self._read_timeout_seconds),
                verify=self._verify_tls,
                allow_redirects=False,
                stream=True,
            )
            if response.status_code in {408, 429, 500, 502, 503, 504}:
                raise RetryableError(f"HTTP service returned retryable status {response.status_code}")
            if response.status_code != 206:
                raise IOError(f"HTTP service expected status 206, got {response.status_code}")

            content_range = response.headers.get("Content-Range")
            match = _CONTENT_RANGE.fullmatch(content_range or "")
            if match is None:
                raise IOError("HTTP service response has an invalid Content-Range header")
            response_start, response_end, response_total = (int(value) for value in match.groups())
            if (response_start, response_end, response_total) != (byte_range.offset, end, total_size):
                raise IOError("HTTP service response Content-Range does not match the request")

            content_length = response.headers.get("Content-Length")
            try:
                parsed_content_length = int(content_length) if content_length is not None else None
            except ValueError as exc:
                raise IOError("HTTP service response has an invalid Content-Length header") from exc
            if parsed_content_length != byte_range.size:
                raise IOError("HTTP service response Content-Length does not match the request")
            content_encoding = response.headers.get("Content-Encoding")
            if content_encoding is not None and content_encoding.lower() != "identity":
                raise IOError("HTTP service response Content-Encoding must be identity")

            body = response.raw.read(byte_range.size + 1)
            if not isinstance(body, bytes) or len(body) != byte_range.size:
                raise IOError("HTTP service response body length does not match the request")
            return body
        except Exception as exc:
            classified = self._classify_request_exception(exc, requests)
            if classified is exc:
                raise
            raise classified from exc
        finally:
            if response is not None:
                response.close()
            session.cookies.clear()
