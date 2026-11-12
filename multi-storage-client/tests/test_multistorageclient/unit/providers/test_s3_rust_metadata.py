# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import multiprocessing
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Barrier
from typing import ClassVar
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.ais import StaticAISCredentialProvider
from multistorageclient.providers.ais_s3 import AIStoreS3StorageProvider
from multistorageclient.providers.s3 import S3StorageProvider, StaticS3CredentialsProvider
from multistorageclient.types import AWARE_DATETIME_MIN, RetryableError

LEGACY_KEYS = ("a//b", "logs/./x", "a/../b", "a/\x01/b")


class MetadataHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    requests: ClassVar[list[tuple[str, str]]] = []
    required_authorization: ClassVar[str | None] = None

    def parse_request(self) -> bool:
        if not super().parse_request():
            return False
        if self.required_authorization and self.headers.get("Authorization") != self.required_authorization:
            self.send_error(403)
            return False
        return True

    def respond(self, head: bool) -> None:
        self.requests.append((self.command, unquote(self.path)))
        key = self.path.rsplit("/", 1)[-1]
        status = {"missing": 404, "denied": 403, "directory": 400}.get(key, 200)
        body = b"data" if key not in ("alias", "empty", "missing", "denied") else b""
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        if status == 200 and key != "minimal":
            self.send_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            self.send_header("ETag", '"test-etag"')
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("x-amz-storage-class", "STANDARD_IA")
            self.send_header("x-amz-meta-owner", "synthetic")
            if key == "alias":
                self.send_header("x-amz-meta-msc-symlink-target", "meta")
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def do_HEAD(self) -> None:
        self.respond(True)

    def do_GET(self) -> None:
        if "list-type=2" in self.path:
            self.requests.append(("LIST_BUCKET", urlsplit(self.path).path.lstrip("/")))
            query = parse_qs(urlsplit(self.path).query)
            if query.get("max-keys") != ["1"] or query.get("delimiter") != ["/"]:
                self.send_response(400)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            prefix = query.get("prefix", [""])[0]
            self.requests.append(("LIST", prefix))
            if prefix == "denied/":
                self.send_response(403)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            entry = ""
            if prefix in ("directory/", "marker/", ""):
                key = "marker/" if prefix == "marker/" else "directory/child"
                entry = f'<Contents><Key>{key}</Key><Size>0</Size><LastModified>2025-01-01T00:00:00Z</LastModified><ETag>"test"</ETag></Contents>'
            if prefix == "prefix-only/":
                entry = "<CommonPrefixes><Prefix>prefix-only/child/</Prefix></CommonPrefixes>"
            if prefix in ("dot-child/", "dot-dot-child/"):
                child = "." if prefix == "dot-child/" else ".."
                entry = f"<CommonPrefixes><Prefix>{prefix}{child}/</Prefix></CommonPrefixes>"
            if prefix.removesuffix("/") in (*LEGACY_KEYS, "/leading", "trailing/"):
                entry = "<CommonPrefixes><Prefix>child/</Prefix></CommonPrefixes>"
            body = f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>{entry}</ListBucketResult>'.encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.respond(False)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def endpoint_url():
    MetadataHandler.requests = []
    MetadataHandler.required_authorization = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), MetadataHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def provider(request, endpoint_url):
    base_path, rust_bucket = getattr(request, "param", ("test-bucket", "test-bucket"))
    rust_options: dict[str, object] = {"allow_http": True}
    if rust_bucket is not None:
        rust_options["bucket"] = rust_bucket
    return S3StorageProvider(
        base_path=base_path,
        region_name="us-east-1",
        endpoint_url=endpoint_url,
        credentials_provider=StaticS3CredentialsProvider("synthetic", "synthetic"),
        rust_client=rust_options,
    )


@pytest.mark.parametrize("authenticated", [False, True])
def test_aistore_metadata_preserves_authentication_and_directory_fallback(endpoint_url, authenticated):
    credentials_provider = None
    if authenticated:
        MetadataHandler.required_authorization = "Bearer synthetic-token"
        credentials_provider = StaticAISCredentialProvider(token="synthetic-token")
    provider = AIStoreS3StorageProvider(
        base_path="test-bucket",
        endpoint_url=endpoint_url,
        credentials_provider=credentials_provider,
        rust_client={"allow_http": True},
    )
    assert provider.get_object_metadata("meta").content_length == 4
    assert provider.get_object_metadata("directory/").type == "directory"
    assert provider.get_object_metadata("directory").type == "directory"
    assert ("HEAD", "/test-bucket/meta") in MetadataHandler.requests
    assert ("HEAD", "/test-bucket/directory") in MetadataHandler.requests
    assert MetadataHandler.requests.count(("LIST", "directory/")) == 2
    assert (provider._boto3_client is not None) == authenticated
    error_type = RuntimeError if authenticated else RetryableError
    with pytest.raises(error_type, match="status_code: 400"):
        provider.get_object_metadata("directory", strict=False)
    assert MetadataHandler.requests.count(("LIST", "directory/")) == 2


def test_directory_bucket_metadata_uses_boto3(endpoint_url):
    bucket = "test-bucket--use1-az1--x-s3"
    provider = S3StorageProvider(
        base_path=bucket,
        region_name="us-east-1",
        endpoint_url=endpoint_url,
        credentials_provider=StaticS3CredentialsProvider("synthetic", "synthetic"),
        rust_client={"allow_http": True},
    )
    boto_client = MagicMock()
    boto_client.head_object.return_value = {"ContentLength": 4}
    boto_client.list_objects_v2.return_value = {"CommonPrefixes": [{"Prefix": "directory/child/"}]}
    with patch.object(provider, "_create_s3_client", return_value=boto_client) as create_boto:
        assert provider.get_object_metadata("meta").content_length == 4
        assert provider.get_object_metadata("directory/").type == "directory"
        create_boto.assert_called_once()
    boto_client.head_object.assert_called_once_with(Bucket=bucket, Key="meta")
    boto_client.list_objects_v2.assert_called_once_with(Bucket=bucket, Prefix="directory/", MaxKeys=1, Delimiter="/")


def test_native_head_preserves_metadata(provider):
    metadata = provider.get_object_metadata("meta")
    assert metadata.content_length == 4
    assert metadata.last_modified == datetime(2025, 1, 1, tzinfo=UTC)
    assert metadata.etag == "test-etag"
    assert metadata.content_type == "application/octet-stream"
    assert metadata.storage_class == "STANDARD_IA"
    assert metadata.metadata == {"owner": "synthetic"}
    assert provider._boto3_client is None


def test_native_head_optional_headers(provider):
    metadata = provider.get_object_metadata("minimal")
    assert metadata.content_length == 4
    assert metadata.last_modified == AWARE_DATETIME_MIN
    assert metadata.etag is None
    assert metadata.content_type is None
    assert metadata.storage_class is None
    assert metadata.metadata == {}
    assert provider._boto3_client is None


@pytest.mark.parametrize(
    "provider,path,expected_bucket,use_rust",
    [
        (("", "configured-bucket"), "requested-bucket/meta", "requested-bucket", False),
        (("", "test-bucket"), "test-bucket/meta", "test-bucket", True),
        (("test-bucket", "configured-bucket"), "meta", "test-bucket", False),
        (("test-bucket", None), "meta", "test-bucket", True),
    ],
    indirect=["provider"],
)
def test_metadata_and_directory_use_requested_bucket(provider, path, expected_bucket, use_rust):
    metadata = provider.get_object_metadata(path, strict=False)
    assert metadata.key == path
    assert ("HEAD", f"/{expected_bucket}/meta") in MetadataHandler.requests

    directory_path = path.removesuffix("meta") + "directory/"
    assert provider.get_object_metadata(directory_path).type == "directory"
    assert ("LIST_BUCKET", expected_bucket) in MetadataHandler.requests
    assert (provider._boto3_client is None) == use_rust


@pytest.mark.parametrize("key", LEGACY_KEYS)
def test_metadata_probes_preserve_legacy_keys(provider, key):
    assert provider.get_object_metadata(key).content_length == 4
    assert provider.is_file(key)
    assert not provider.is_file(f"{key}/missing")
    assert ("HEAD", f"/test-bucket/{key}") in MetadataHandler.requests
    assert ("LIST", f"{key}/missing/") in MetadataHandler.requests
    assert provider._boto3_client is not None


@pytest.mark.parametrize("key", (*LEGACY_KEYS, "/leading", "trailing/"))
def test_directory_probes_preserve_legacy_prefixes(provider, key):
    assert provider._is_dir(f"test-bucket/{key}/")
    assert ("LIST", f"{key}/") in MetadataHandler.requests
    assert provider._boto3_client is not None


def test_metadata_probe_preserves_leading_key_delimiter(provider):
    assert provider._get_object_metadata("test-bucket//leading", strict=False).content_length == 4
    assert ("HEAD", "/test-bucket//leading") in MetadataHandler.requests
    assert provider._boto3_client is not None


@pytest.mark.parametrize("key", ["meta", "alias", "empty"])
def test_open_read_avoids_boto3(provider, key):
    client = StorageClient(StorageClientConfig(profile="test", storage_provider=provider))
    with client.open(key, "rb", prefetch_file=False) as stream:
        assert stream.read() == (b"" if key == "empty" else b"data")
    assert provider._boto3_client is None


@pytest.mark.parametrize("key,error", [("missing", FileNotFoundError), ("denied", PermissionError)])
def test_native_head_error_contract(provider, key, error):
    with pytest.raises(error):
        provider.get_object_metadata(key, strict=False)
    assert provider._boto3_client is None


def test_native_head_preserves_directory_fallback(provider):
    with patch.object(provider, "_is_dir", return_value=True) as is_dir:
        metadata = provider.get_object_metadata("missing")
    assert metadata.type == "directory"
    assert metadata.key == "missing/"
    is_dir.assert_called_once_with("test-bucket/missing/")


@pytest.mark.parametrize(
    "path,exists",
    [
        ("missing", False),
        ("directory/", True),
        ("marker/", True),
        ("prefix-only/", True),
        ("directory-extra/", False),
        ("", True),
    ],
)
def test_native_directory_lookup_avoids_boto3(provider, path, exists):
    assert provider._is_dir(f"test-bucket/{path}") == exists
    assert provider._boto3_client is None


def test_missing_file_probe_avoids_boto3(provider):
    assert not provider.is_file("missing")
    assert provider._boto3_client is None


def test_directory_lookup_preserves_permission_error(provider):
    with pytest.raises(PermissionError):
        provider._is_dir("test-bucket/denied/")
    assert provider._boto3_client is None


@pytest.mark.parametrize("path", ["dot-child/", "dot-dot-child/"])
def test_directory_probe_falls_back_for_unsupported_child_prefix(provider, path):
    assert provider.get_object_metadata(path).type == "directory"
    assert MetadataHandler.requests.count(("LIST", path)) == 2
    assert provider._boto3_client is not None


def test_rust_reads_do_not_create_boto3_client():
    with (
        patch.object(S3StorageProvider, "_create_s3_client") as create_boto,
        patch.object(S3StorageProvider, "_create_rust_client"),
        patch("multistorageclient.providers.s3.run_async_rust_client_method", return_value=b"data"),
    ):
        provider = S3StorageProvider(base_path="test-bucket", rust_client={})
        assert provider._get_object("test-bucket/key") == b"data"
        create_boto.assert_not_called()


def test_python_only_client_remains_eager():
    with patch.object(S3StorageProvider, "_create_s3_client") as create_boto:
        S3StorageProvider(base_path="test-bucket")
        create_boto.assert_called_once()


def test_fallback_creates_and_reuses_boto3_client():
    with (
        patch.object(S3StorageProvider, "_create_s3_client") as create_boto,
        patch.object(S3StorageProvider, "_create_rust_client"),
    ):
        provider = S3StorageProvider(base_path="test-bucket", rust_client={}, read_timeout=17)
        provider._delete_object("test-bucket/key")
        provider._delete_object("test-bucket/other")
        create_boto.assert_called_once()
        assert create_boto.call_args.kwargs["read_timeout"] == 17
        assert create_boto.return_value.delete_object.call_count == 2


def test_concurrent_first_use_creates_one_boto3_client():
    barrier = Barrier(8)
    boto_client = MagicMock()

    def create_client(**kwargs):
        time.sleep(0.02)
        return boto_client

    with (
        patch.object(S3StorageProvider, "_create_s3_client", side_effect=create_client) as create_boto,
        patch.object(S3StorageProvider, "_create_rust_client"),
    ):
        provider = S3StorageProvider(base_path="test-bucket", rust_client={})
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_delete_after_barrier, provider, barrier) for _ in range(8)]
            for future in futures:
                future.result()
        create_boto.assert_called_once()
        assert boto_client.delete_object.call_count == 8


def _delete_after_barrier(provider: S3StorageProvider, barrier: Barrier) -> None:
    barrier.wait()
    provider._delete_object("test-bucket/key")


def test_failed_initialization_can_retry():
    boto_client = MagicMock()
    with (
        patch.object(S3StorageProvider, "_create_s3_client", side_effect=[RuntimeError("unavailable"), boto_client]),
        patch.object(S3StorageProvider, "_create_rust_client"),
    ):
        provider = S3StorageProvider(base_path="test-bucket", rust_client={})
        with pytest.raises(RuntimeError, match="unavailable"):
            provider._delete_object("test-bucket/key")
        provider._delete_object("test-bucket/key")
        boto_client.delete_object.assert_called_once()


@pytest.mark.parametrize("verify", [False, "custom-ca.pem"])
def test_explicit_verify_preserves_boto3_metadata_routing(provider, verify):
    provider._verify = verify
    with patch(
        "multistorageclient.providers.s3.run_async_rust_client_method",
        side_effect=AssertionError("Rust transport used"),
    ):
        assert provider.get_object_metadata("meta").content_length == 4
        assert not provider.is_file("missing")
    assert provider._boto3_client is not None


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="fork is unavailable")
def test_lazy_client_resets_inherited_locked_mutex():
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    with patch.object(S3StorageProvider, "_create_s3_client"), patch.object(S3StorageProvider, "_create_rust_client"):
        provider = S3StorageProvider(base_path="test-bucket", rust_client={})
        process = context.Process(target=_access_lazy_client_after_fork, args=(provider, sender))
        provider._s3_client_lock.acquire()
        try:
            process.start()
            assert receiver.poll(5), "child blocked on the parent's inherited lock"
            assert receiver.recv()
            process.join(5)
            assert process.exitcode == 0
        finally:
            provider._s3_client_lock.release()
            if process.is_alive():
                process.terminate()
                process.join()
            receiver.close()
            sender.close()


def _access_lazy_client_after_fork(provider, sender):
    sender.send(provider._s3_client is not None)
    sender.close()
