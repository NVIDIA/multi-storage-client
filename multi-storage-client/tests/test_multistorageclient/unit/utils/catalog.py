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

"""A synthetic external catalog used by resolver, client and Path tests."""

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from urllib.parse import unquote

from multistorageclient import resolve_storage_client
from multistorageclient.references import BoundProfile, ParsedReference
from multistorageclient.types import (
    ObjectMetadata,
    ProviderBundleV2,
    Replica,
    ResolvedPath,
    ResolvedPathState,
    StorageBackend,
    StorageProviderConfig,
)
from test_multistorageclient.unit.utils.mocks import TestMetadataProvider as _Inventory


class CatalogResolver:
    binds: ClassVar[list[ParsedReference]] = []

    def __init__(self, profile: dict[str, Any] | None = None, namespace: str = "ns", ttl: int | None = 300):
        self.profile = profile or {"storage_provider": {"type": "file", "options": {"base_path": "/"}}}
        self.namespace = namespace
        self.ttl = ttl

    def parse(self, url: str) -> ParsedReference:
        root, files, key = url.partition("/files/")
        if not files or not root.startswith("catalog://"):
            raise ValueError(f"Invalid catalog reference: {url}")
        address, revision = root.removeprefix("catalog://").split("/")
        if address not in (self.namespace, "alias"):
            raise ValueError(f"Unknown catalog namespace: {address}")
        return ParsedReference(
            root + files,
            unquote(key),
            address == self.namespace and revision != "latest",
            os.environ.get("MSC_TEST_SCOPE", "alice") or None,
        )

    def bind(self, reference: ParsedReference) -> BoundProfile:
        self.binds.append(reference)
        if (
            reference.access_scope != (os.environ.get("MSC_TEST_SCOPE", "alice") or None)
            or reference.access_scope == "denied"
        ):
            raise PermissionError("Catalog access denied")
        revision = reference.root_url.split("/")[-3]
        if revision == "latest":
            revision = os.environ.get("MSC_TEST_REVISION", "r1")
        if revision == "missing":
            raise FileNotFoundError(revision)
        deadline = datetime.now(UTC) + timedelta(seconds=self.ttl) if self.ttl is not None else None
        return BoundProfile(self.profile, f"catalog://{self.namespace}/{revision}/files/", deadline)


ROOT = "catalog://ns/r1/files/"
LOGICAL = "./images/../café%?#"
ENCODED = "./images/../caf%C3%A9%25%3F%23"
PHYSICAL = "blob.001"


class Inventory(_Inventory):
    def __init__(self):
        super().__init__()
        self._files = {LOGICAL: ObjectMetadata(key=LOGICAL, content_length=5, last_modified=datetime.now(UTC))}

    def realpath(self, logical_path):
        if os.environ.get("MSC_TEST_REVOKED"):
            raise PermissionError("Inventory access revoked")
        if logical_path in self._files:
            return ResolvedPath(PHYSICAL, ResolvedPathState.EXISTS, profile="primary")
        return ResolvedPath(logical_path, ResolvedPathState.UNTRACKED, profile="primary")

    def get_object_metadata(self, path, include_pending=False):
        if not self.realpath(path).exists:
            raise FileNotFoundError(path)
        return self._files[path]


class InventoryBundle(ProviderBundleV2):
    def __init__(self, directory, multi):
        self._backends = {
            "primary": StorageBackend(
                StorageProviderConfig("file", {"base_path": directory + "/primary"}), replicas=[Replica("mirror", 1)]
            )
        }
        if multi:
            self._backends["other"] = StorageBackend(StorageProviderConfig("file", {"base_path": directory + "/other"}))

    @property
    def storage_backends(self):
        return self._backends

    @property
    def metadata_provider(self):
        return Inventory()


def configure_inventory(tmp_path, monkeypatch, multi=False, autocommit=None, replica_autocommit=None):
    (tmp_path / "primary").mkdir()
    (tmp_path / "mirror").mkdir()
    (tmp_path / "primary" / PHYSICAL).write_bytes(b"image")
    (tmp_path / "primary" / "untracked").write_bytes(b"private")
    profile = {
        "provider_bundle": {
            "type": f"{__name__}.InventoryBundle",
            "options": {"directory": str(tmp_path), "multi": multi},
        }
    }
    mirror = {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path / "mirror")}}}
    if autocommit:
        profile["autocommit"] = autocommit
    if replica_autocommit:
        mirror["autocommit"] = replica_autocommit
    config_file = tmp_path / "msc.json"
    config_file.write_text(
        json.dumps(
            {
                "profiles": {"mirror": mirror},
                "url_resolvers": {
                    "catalog": {
                        "type": f"{__name__}.CatalogResolver",
                        "options": {"profile": profile},
                    }
                },
            }
        )
    )
    monkeypatch.setenv("MSC_CONFIG", str(config_file))
    return resolve_storage_client("catalog://alias/latest/files/")[0]
