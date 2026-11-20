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

"""Measure URL resolution without reading, signing or transferring an object.

Run in separate processes against the base and candidate packages using the same
interpreter. Put multi-storage-client/tests on PYTHONPATH to use --enable-resolver.
"""

import argparse
import json
import multiprocessing
import os
import statistics
import tempfile
from pathlib import Path
from time import perf_counter_ns

from multistorageclient import resolve_storage_client


def measure(root: str, samples: int) -> dict[str, float]:
    urls = [root + f"file-{index}" for index in range(257)]
    start = perf_counter_ns()
    resolve_storage_client(urls[0])
    first = perf_counter_ns() - start
    for url in urls:
        resolve_storage_client(url)

    timings = []
    for index in range(samples):
        url = urls[index % len(urls)]
        start = perf_counter_ns()
        resolve_storage_client(url)
        timings.append(perf_counter_ns() - start)
    timings.sort()
    return {
        "first_us": first / 1000,
        "p50_us": statistics.median(timings) / 1000,
        "p95_us": timings[int(len(timings) * 0.95)] / 1000,
    }


def measure_worker(root: str) -> dict[str, float]:
    from test_multistorageclient.unit.utils.catalog import CatalogResolver

    before = len(CatalogResolver.binds)
    timings = measure(root, 1000)
    assert len(CatalogResolver.binds) - before == 1
    return timings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable-resolver", action="store_true")
    parser.add_argument("--samples", type=int, default=20_000)
    args = parser.parse_args()
    if args.samples < 100:
        parser.error("--samples must be at least 100")

    with tempfile.TemporaryDirectory() as tmp:
        profile = {"storage_provider": {"type": "file", "options": {"base_path": tmp}}}
        mapping = "s3://msc-bench-mapped/"
        config = {"profiles": {"training": profile}, "path_mapping": {mapping: "msc://training/"}}
        routes = {"static": "msc://training/", "s3": "s3://msc-bench-implicit/", "local": tmp + "/", "mapped": mapping}
        if args.enable_resolver:
            config["url_resolvers"] = {
                "catalog": {
                    "type": "test_multistorageclient.unit.utils.catalog.CatalogResolver",
                    "options": {"profile": profile},
                }
            }
            routes["pinned"] = "catalog://ns/r1/files/"
        config_path = Path(tmp) / "config.json"
        config_path.write_text(json.dumps(config))
        os.environ.update(
            MSC_CONFIG=str(config_path),
            AWS_EC2_METADATA_DISABLED="true",
            AWS_ACCESS_KEY_ID="benchmark",
            AWS_SECRET_ACCESS_KEY="benchmark",
            AWS_DEFAULT_REGION="us-east-1",
        )
        results = {name: measure(url, args.samples) for name, url in routes.items()}
        if args.enable_resolver:
            from test_multistorageclient.unit.utils.catalog import CatalogResolver

            assert len(CatalogResolver.binds) == 1
            for revision in ("r1", "r2"):
                os.environ["MSC_TEST_REVISION"] = revision
                start = perf_counter_ns()
                client, _ = resolve_storage_client("catalog://alias/latest/files/")
                results[f"latest_{revision}"] = {"first_us": (perf_counter_ns() - start) / 1000}
                assert client.url() == f"catalog://ns/{revision}/files/"
            assert len(CatalogResolver.binds) == 3
            for method in ("spawn", "fork"):
                if method in multiprocessing.get_all_start_methods():
                    with multiprocessing.get_context(method).Pool(1) as pool:
                        results[f"worker_{method}"] = pool.apply(measure_worker, (routes["pinned"],))
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
