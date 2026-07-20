# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""S3-over-RDMA (NVIDIA cuObject) storage provider.

Subclasses :py:class:`S3StorageProvider` and overrides only the object
GET/PUT data plane to move payloads over RDMA via cuObject, leaving the base
``s3`` provider (and all its metadata/list/credentials/error handling)
untouched. Kept separate so the RDMA data path -- which cannot be exercised
in CI without an RDMA NIC and an RDMA-capable endpoint -- can never affect the
base ``s3`` provider.

See https://docs.nvidia.com/gpudirect-storage/cuobject.
"""

import base64
import io
import os
import struct
from typing import IO, Any, Optional, Union

from ..types import Range
from ..utils import split_path, validate_attributes
from ._cuobj import CuObjEngine
from .s3 import EXPRESS_ONEZONE_STORAGE_CLASS, MiB, S3StorageProvider

PROVIDER = "s3_cuobject"

# cuObject transfers the whole registered buffer in a single shot, so the boto
# multipart threshold is raised past any practical object size and every
# transfer takes the single-shot path. Uploads larger than
# ``rdma.multipart_chunksize`` are split by the RDMA multipart path below.
RDMA_SINGLE_SHOT_THRESHOLD = 1 << 62

# Default RDMA multipart part size. Uploads larger than this are sent as an
# RDMA multipart upload (one registered buffer + token + CRC64NVME per part).
RDMA_MULTIPART_CHUNKSIZE = 512 * MiB


class S3CuObjectStorageProvider(S3StorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with
    S3 via NVIDIA cuObject (S3-over-RDMA).

    https://docs.nvidia.com/gpudirect-storage/cuobject
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "rust_client" in kwargs:
            raise ValueError("The 's3_cuobject' provider is mutually exclusive with 'rust_client'.")
        if kwargs.get("checksum_algorithm") is not None:
            raise ValueError("checksum_algorithm is not supported for the 's3_cuobject' provider.")

        self._rdma_options: dict[str, Any] = kwargs.get("rdma") or {}

        # Force the empty-body, unsigned-payload wire contract the RDMA endpoint
        # expects onto the boto client before it is constructed. Only the
        # empty-body contract is enforced; addressing style stays user-controlled
        # via the `s3` option.
        overrides = CuObjEngine.client_config_overrides()
        kwargs["request_checksum_calculation"] = overrides["request_checksum_calculation"]
        kwargs["response_checksum_validation"] = overrides["response_checksum_validation"]
        kwargs["s3"] = {**(kwargs.get("s3") or {}), **overrides["s3"]}

        super().__init__(*args, **kwargs)

        # Override the provider name from "s3".
        self._provider_name = PROVIDER

        self._checksum_algorithm = None
        self._multipart_threshold = RDMA_SINGLE_SHOT_THRESHOLD
        self._rdma_multipart_chunksize = int(self._rdma_options.get("multipart_chunksize", RDMA_MULTIPART_CHUNKSIZE))
        if self._rdma_multipart_chunksize < 1:
            raise ValueError(
                f"rdma.multipart_chunksize must be a positive integer, got {self._rdma_multipart_chunksize}"
            )

        self._rdma_engine = CuObjEngine()
        self._rdma_engine.install_hooks(self._s3_client)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)
        return self._rdma_get(path, bucket, key, byte_range)

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        bucket, key = split_path(path)

        def _invoke_api() -> int:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": body}
            if content_type:
                kwargs["ContentType"] = content_type
            if self._is_directory_bucket(bucket):
                kwargs["StorageClass"] = EXPRESS_ONEZONE_STORAGE_CLASS
            if if_match:
                kwargs["IfMatch"] = if_match
            if if_none_match:
                kwargs["IfNoneMatch"] = if_none_match
            validated_attributes = validate_attributes(attributes)
            if validated_attributes:
                kwargs["Metadata"] = validated_attributes
            return self._rdma_put(kwargs, body)

        return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

    def _upload_file(
        self,
        remote_path: str,
        f: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        return self._rdma_upload(remote_path, f, attributes, content_type)

    @staticmethod
    def _rdma_checksum(buffer) -> str:
        """Base64 CRC64NVME of ``buffer`` for the ``x-amz-checksum-crc64nvme`` header.

        CRC64NVME is hardware-accelerated and computed via ``awscrt`` (the same
        implementation botocore uses for this algorithm).
        """
        try:
            from awscrt import checksums
        except ImportError as error:
            raise RuntimeError(
                "RDMA PUT computes a CRC64NVME checksum and requires the 'awscrt' package (pip install awscrt)."
            ) from error
        return base64.b64encode(struct.pack(">Q", checksums.crc64nvme(buffer))).decode("ascii")

    def _rdma_put(self, kwargs: dict[str, Any], body: bytes) -> int:
        """Single-shot RDMA PUT: cuObject transfers the registered buffer; the HTTP body is empty.

        A CRC64NVME checksum of the payload is computed on the client and sent as
        ``x-amz-checksum-crc64nvme``. On an RDMA-capable endpoint it is validated
        against the bytes delivered over RDMA; on an endpoint that ignores the
        RDMA token it is validated against the (empty) HTTP body and fails with a
        checksum mismatch, so the upload is rejected rather than silently storing
        a 0-byte object.
        """
        engine = self._rdma_engine
        assert engine is not None
        # cuObject pins a writable region: reuse the caller's buffer when it is
        # writable, and copy only a read-only (immutable) body such as bytes.
        buffer = bytearray(body) if memoryview(body).readonly else body
        if len(buffer) == 0:
            self._s3_client.put_object(**kwargs)
            return 0
        kwargs = {**kwargs, "Body": b"", "ChecksumCRC64NVME": self._rdma_checksum(buffer)}
        with engine.transfer(buffer, is_put=True):
            response = self._s3_client.put_object(**kwargs)
            engine.check_reply(response)
        return len(buffer)

    def _rdma_get(self, path: str, bucket: str, key: str, byte_range: Optional[Range]) -> bytearray:
        """Single-shot RDMA GET into a registered buffer; returns the buffer."""
        engine = self._rdma_engine
        assert engine is not None
        if_match: Optional[str] = None
        if byte_range is not None:
            size = byte_range.size
            bytes_range: Optional[str] = f"bytes={byte_range.offset}-{byte_range.offset + byte_range.size - 1}"
        else:
            metadata = self._get_object_metadata(path)
            size = metadata.content_length
            bytes_range = None
            # Bind the GET to the object version the buffer was sized against; if
            # the object is replaced between the HEAD and the GET the endpoint
            # returns 412 instead of delivering bytes into a mismatched buffer.
            if_match = metadata.etag

        def _invoke_api() -> bytearray:
            buffer = bytearray(size)
            if size == 0:
                return buffer
            get_kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
            if bytes_range is not None:
                get_kwargs["Range"] = bytes_range
            if if_match:
                get_kwargs["IfMatch"] = if_match
            with engine.transfer(buffer, is_put=False):
                response = self._s3_client.get_object(**get_kwargs)
                response["Body"].read()
                engine.check_reply(response)
            return buffer

        return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _rdma_create_extra(
        self, bucket: str, attributes: Optional[dict[str, str]], content_type: Optional[str]
    ) -> dict[str, Any]:
        extra: dict[str, Any] = {}
        if content_type:
            extra["ContentType"] = content_type
        if self._is_directory_bucket(bucket):
            extra["StorageClass"] = EXPRESS_ONEZONE_STORAGE_CLASS
        validated = validate_attributes(attributes)
        if validated:
            extra["Metadata"] = validated
        return extra

    def _rdma_upload(
        self,
        remote_path: str,
        f: Union[str, IO],
        attributes: Optional[dict[str, str]],
        content_type: Optional[str],
    ) -> int:
        """RDMA upload entry point: single-shot below the part size, multipart above it."""
        bucket, key = split_path(remote_path)
        if isinstance(f, str):
            size = os.path.getsize(f)
            with open(f, "rb") as fp:
                if size > self._rdma_multipart_chunksize:
                    extra = self._rdma_create_extra(bucket, attributes, content_type)
                    return self._rdma_upload_multipart(bucket, key, fp, size, extra)
                return self._put_object(remote_path, fp.read(), attributes=attributes, content_type=content_type)

        f.seek(0, io.SEEK_END)
        size = f.tell()
        f.seek(0)
        if size > self._rdma_multipart_chunksize and not isinstance(f, io.StringIO):
            extra = self._rdma_create_extra(bucket, attributes, content_type)
            return self._rdma_upload_multipart(bucket, key, f, size, extra)
        data = f.read()
        if isinstance(data, str):
            data = data.encode("utf-8")
        return self._put_object(remote_path, data, attributes=attributes, content_type=content_type)

    def _rdma_upload_multipart(self, bucket: str, key: str, fp: IO, size: int, extra: dict[str, Any]) -> int:
        """RDMA multipart upload: each part is transferred as its own registered buffer.

        cuObject transfers one part-sized buffer per ``UploadPart`` (empty HTTP
        body + RDMA token), each carrying a CRC64NVME the endpoint validates
        against the RDMA-delivered bytes -- so the 0-byte-save guard applies per
        part. Multipart is required past the single-PutObject size limit and
        bounds the size of any one registered buffer / RDMA transfer.
        """
        engine = self._rdma_engine
        assert engine is not None
        part_size = self._rdma_multipart_chunksize

        def _invoke_api() -> int:
            upload_id = self._s3_client.create_multipart_upload(Bucket=bucket, Key=key, **extra)["UploadId"]
            parts: list[dict[str, Any]] = []
            try:
                part_number = 1
                remaining = size
                while remaining > 0:
                    n = min(part_size, remaining)
                    chunk = bytearray()
                    while len(chunk) < n:
                        data = fp.read(n - len(chunk))
                        if not data:
                            raise RuntimeError(f"unexpected end of input for {bucket}/{key} at part {part_number}")
                        chunk.extend(data)
                    checksum = self._rdma_checksum(chunk)
                    with engine.transfer(chunk, is_put=True):
                        response = self._s3_client.upload_part(
                            Bucket=bucket,
                            Key=key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=b"",
                            ChecksumCRC64NVME=checksum,
                        )
                        engine.check_reply(response)
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    remaining -= n
                    part_number += 1
                self._s3_client.complete_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
                )
            except BaseException:
                try:
                    self._s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
                except Exception:
                    pass
                raise
            return size

        return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)
