#!/usr/bin/env bash
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

set -euo pipefail
sandbox=$(mktemp -d "${TMPDIR:-/tmp}/msc-rust-benchmark.XXXXXX")
trap 'rm -rf "$sandbox"' EXIT
export MSC_POSIX_ROOT="$sandbox/objects"
mkdir -p "$MSC_POSIX_ROOT/benchmark" "$sandbox/files"
printf 'keep remote data' > "$MSC_POSIX_ROOT/benchmark/sentinel"
printf 'keep local data' > "$sandbox/files/sentinel"
cargo run --locked --bin msc-benchmark -- --profile local \
    --storage-config tests/fixtures/posix.yaml --config tests/fixtures/benchmark-smoke.json \
    --prefix benchmark --include-file-tests --file-tests-dir "$sandbox/files" \
    --format json > "$sandbox/result.json"
python3 - "$sandbox" <<'CHECK'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
report = json.loads((root / "result.json").read_text())
assert not report["cancelled"]
assert len(report["results"]) == 10
assert all(row["succeeded"] == 3 and row["failed"] == 0 for row in report["results"])
assert (root / "objects/benchmark/sentinel").read_text() == "keep remote data"
assert (root / "files/sentinel").read_text() == "keep local data"
assert len(list((root / "objects").rglob("*"))) >= 1
assert [path for path in (root / "objects").rglob("*") if path.is_file()] == [root / "objects/benchmark/sentinel"]
assert list((root / "files").iterdir()) == [root / "files/sentinel"]
CHECK
cat "$sandbox/result.json"
