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

"""
End-to-end validation of the S3-over-RDMA (NVIDIA cuObject) data plane for the
``s3`` storage provider, mirroring the PyTorch cuObject checkpoint roundtrip.

This requires an RDMA-capable S3 endpoint and the cuObject runtime, so it is a
standalone script rather than a CI test. Run it on a cluster client (e.g. coe09)
against the RDMA MinIO endpoint (e.g. coe01:9000):

    # Build the shim once on a host with the cuObject SDK (see cuobj_shim.cpp):
    #   g++ -O2 -fPIC -shared cuobj_shim.cpp -o libmsc_cuobj.so -lcuobjclient -lcufile

    export MSC_CUOBJ_SHIM=/path/to/libmsc_cuobj.so
    export CUFILE_ENV_PATH_JSON=/path/to/cuobj.json   # rdma_dev_addr_list, use_pci_p2pdma, ...
    export LD_LIBRARY_PATH=/path/to/sdklib:$LD_LIBRARY_PATH   # version-matched libcufile/libcuobjclient
    export S3_ENDPOINT=http://coe01:9000
    export S3_BUCKET=rdma-test
    export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin

    python examples/rdma_roundtrip.py
"""

import os

import multistorageclient as msc

_ENDPOINT = os.environ["S3_ENDPOINT"]
_BUCKET = os.environ["S3_BUCKET"]
_PROFILE = "rdma-test"


def _config() -> dict:
    return {
        "profiles": {
            _PROFILE: {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "base_path": _BUCKET,
                        "endpoint_url": _ENDPOINT,
                        "region_name": "us-east-1",
                        # MinIO AIStor RDMA endpoints are path-addressed; the
                        # provider leaves addressing style to the user.
                        "s3": {"addressing_style": "path"},
                        # Presence of "rdma" enables the cuObject data plane and
                        # forces the empty-body/unsigned-payload wire contract.
                        "rdma": {},
                    },
                }
            }
        }
    }


def main() -> None:
    config = msc.StorageClientConfig.from_dict(_config(), profile=_PROFILE)
    client = msc.StorageClient(config)

    # A few sizes spanning the old multipart threshold (64 MiB); all must take
    # the single-shot RDMA path.
    for mib in (1, 64, 256):
        size = mib * 1024 * 1024
        payload = os.urandom(size)
        key = f"rdma_roundtrip/blob_{mib}mib.bin"

        client.write(key, payload)
        reloaded = bytes(client.read(key))

        assert len(reloaded) == size, f"{key}: size {len(reloaded)} != {size}"
        assert reloaded == payload, f"{key}: byte mismatch after RDMA roundtrip"
        print(f"OK  {key}  ({mib} MiB) byte-identical over RDMA")

    print("\nAll cuObject RDMA roundtrips passed.")


if __name__ == "__main__":
    main()
