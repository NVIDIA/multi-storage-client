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

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

import multistorageclient as msc
from multistorageclient import config as msc_config
from multistorageclient import shortcuts
from multistorageclient.config import StorageClientConfig
from test_multistorageclient.unit.utils.catalog import RESOLVER, CatalogResolver

PINNED = "catalog://ns/r1/files/"
LATEST = "catalog://alias/latest/files/"


def test_pinned_hit_has_no_configuration_or_catalog_work(catalog_config, monkeypatch):
    catalog_config()
    client, key = msc.resolve_storage_client(PINNED + "x%25%3F%23/%E4%B8%8A/.././child/")
    assert key == "x%?#/上/.././child/"
    assert client.url(key) == PINNED + "x%25%3F%23/%E4%B8%8A/.././child/"

    def unexpected(*args, **kwargs):
        pytest.fail("Repeated cold initialization while resolving a pinned sibling")

    monkeypatch.setattr(StorageClientConfig, "read_msc_config", unexpected)
    monkeypatch.setattr(shortcuts, "validate_config", unexpected)
    monkeypatch.setattr(shortcuts, "import_class", unexpected)
    monkeypatch.setattr(CatalogResolver, "bind", unexpected)
    for i in range(20):
        assert msc.resolve_storage_client(PINNED + str(i)) == (client, str(i))


@pytest.mark.parametrize("enable_resolver", [False, True])
def test_warm_existing_routes_skip_configuration_and_imports(catalog_config, monkeypatch, tmp_path, enable_resolver):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    catalog_config(
        registrations={"unused": {"type": "missing_catalog.Resolver"}} if enable_resolver else {},
        profiles={"training": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}},
        path_mapping={"s3://mapped/": "msc://training/"},
    )
    routes = ("msc://training/", "s3://implicit/", str(tmp_path) + "/", "s3://mapped/")
    clients = [msc.resolve_storage_client(root)[0] for root in routes]

    def unexpected(*args, **kwargs):
        pytest.fail("A warm shortcut reinitialized its configuration or imported a plugin")

    monkeypatch.setattr(StorageClientConfig, "read_msc_config", unexpected)
    for module in (msc_config, shortcuts):
        monkeypatch.setattr(module, "validate_config", unexpected)
        monkeypatch.setattr(module, "import_class", unexpected)
    for index in range(20):
        for root, client in zip(routes, clients):
            assert msc.resolve_storage_client(root + str(index))[0] is client


def test_mutable_always_binds_and_seeds_its_pinned_revision(catalog_config, monkeypatch):
    catalog_config()
    r1, _ = msc.resolve_storage_client(LATEST)
    assert msc.resolve_storage_client(PINNED)[0] is r1
    monkeypatch.setenv("MSC_TEST_REVISION", "r2")
    r2, _ = msc.resolve_storage_client(LATEST)
    assert r2.url() == "catalog://ns/r2/files/"
    assert msc.resolve_storage_client(r2.url() + "sibling")[0] is r2
    assert msc.resolve_storage_client(PINNED)[0] is r1
    assert r1.profile != r2.profile
    assert [ref.root_url for ref in CatalogResolver.binds] == [LATEST, LATEST]


@pytest.mark.parametrize("ttl", [None, -1])
def test_ineligible_deadline_rebinds_with_same_scoped_identity(catalog_config, ttl):
    catalog_config(options={"ttl": ttl})
    first, _ = msc.resolve_storage_client(PINNED)
    second, _ = msc.resolve_storage_client(PINNED)
    assert first is not second
    assert first.profile == second.profile
    assert len(CatalogResolver.binds) == 2


def test_expiry_and_capacity_evict_grants(catalog_config, monkeypatch):
    catalog_config()
    monkeypatch.setattr(shortcuts, "_MAX_REFERENCE_BINDINGS", 2)
    first, _ = msc.resolve_storage_client(PINNED)
    for rev in ("r2", "r3"):
        msc.resolve_storage_client(f"catalog://ns/{rev}/files/")
    other, _ = msc.resolve_storage_client(PINNED)
    assert first is not other
    assert other.profile == first.profile
    now = shortcuts.time()
    monkeypatch.setattr(shortcuts, "time", lambda: now + 301)
    assert msc.resolve_storage_client(PINNED)[0] is not other
    _, resolvers = shortcuts._read_cached_routes()
    assert not resolvers["catalog"]._clients


def test_config_and_scope_isolation(catalog_config, monkeypatch, tmp_path):
    first_config = catalog_config()
    alice, _ = msc.resolve_storage_client(PINNED)
    monkeypatch.setenv("MSC_TEST_SCOPE", "bob")
    bob, _ = msc.resolve_storage_client(PINNED)
    assert alice.profile != bob.profile
    monkeypatch.setenv("MSC_TEST_SCOPE", "denied")
    with pytest.raises(PermissionError):
        msc.resolve_storage_client(PINNED)
    monkeypatch.setenv("MSC_TEST_SCOPE", "alice")
    catalog_config(options={"profile": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}})
    assert msc.resolve_storage_client(PINNED)[0].profile != alice.profile
    monkeypatch.setenv("MSC_CONFIG", str(first_config))
    assert msc.resolve_storage_client(PINNED)[0] is alice


def test_unscoped_binds_have_fresh_identity_and_no_content_cache(catalog_config, monkeypatch, tmp_path):
    monkeypatch.setenv("MSC_TEST_SCOPE", "")
    catalog_config(
        options={
            "profile": {"storage_provider": {"type": "file", "options": {"base_path": "/"}}, "caching_enabled": True}
        },
        cache={"location": str(tmp_path)},
    )
    a, _ = msc.resolve_storage_client(PINNED)
    b, _ = msc.resolve_storage_client(PINNED)
    assert a.profile != b.profile
    assert a._config.cache_manager is b._config.cache_manager is None
    assert len(CatalogResolver.binds) == 2


def test_mutable_unscoped_bind_does_not_infer_scope_from_pinned_root(catalog_config, monkeypatch):
    catalog_config()
    monkeypatch.setenv("MSC_TEST_SCOPE", "")
    original_parse = CatalogResolver.parse

    def parse(self, url):
        ref = original_parse(self, url)
        return replace(ref, access_scope="alice") if ref.is_canonical else ref

    monkeypatch.setattr(CatalogResolver, "parse", parse)
    first, _ = msc.resolve_storage_client(LATEST)
    assert first._config.access_scope is None
    monkeypatch.setenv("MSC_TEST_SCOPE", "alice")
    second, _ = msc.resolve_storage_client(PINNED)
    assert second.profile != first.profile
    assert len(CatalogResolver.binds) == 2


def test_cold_binds_are_independent_and_same_pinned_callers_share(catalog_config, monkeypatch):
    catalog_config()
    entered, release = threading.Event(), threading.Event()
    original = CatalogResolver.bind

    def block(self, ref):
        if ref.root_url == PINNED:
            entered.set()
            assert release.wait(10)
        return original(self, ref)

    monkeypatch.setattr(CatalogResolver, "bind", block)
    with ThreadPoolExecutor(4) as pool:
        blocked = pool.submit(msc.resolve_storage_client, PINNED)
        try:
            assert entered.wait(10)
            sibling = pool.submit(msc.resolve_storage_client, PINNED + "sibling")
            assert pool.submit(msc.resolve_storage_client, "/tmp/file").result(5)[1] == "/tmp/file"
            assert pool.submit(msc.resolve_storage_client, "catalog://ns/r2/files/y").result(5)[1] == "y"
            assert not blocked.done()
        finally:
            release.set()
        assert blocked.result()[0] is sibling.result()[0]
    assert len(CatalogResolver.binds) == 2


def test_explicit_profile_and_mapping_with_missing_plugin(catalog_config):
    catalog_config(
        registrations={"unused": {"type": "missing_catalog_does_not_exist.Resolver"}},
        profiles={"static": {"storage_provider": {"type": "file", "options": {"base_path": "/"}}}},
        path_mapping={"mapped://bucket/prefix/": "msc://static/"},
    )
    assert msc.resolve_storage_client("unused")[1] == str(Path.cwd() / "unused")
    static, key = msc.resolve_storage_client("msc://static/file")
    assert key == "file"
    assert msc.resolve_storage_client("mapped://bucket/prefix/file") == (static, key)
    with pytest.raises(ImportError):
        msc.resolve_storage_client("unused://revision/file")
    with pytest.raises(ValueError, match="Unknown URL"):
        msc.resolve_storage_client("unregistered://revision/file")


@pytest.mark.parametrize("scheme", ["msc", "s3", "gs", "ais", "file"])
def test_reserved_schemes_rejected(catalog_config, scheme):
    catalog_config(registrations={scheme: {"type": RESOLVER}})
    with pytest.raises(ValueError, match="reserved scheme"):
        msc.resolve_storage_client("/tmp/file")


def test_conflicting_ownership_in_include(catalog_config, tmp_path):
    included = tmp_path / "included.yaml"
    included.write_text("path_mapping: {'catalog://bucket/prefix/': 'msc://test/'}")
    catalog_config(include=[str(included)])
    with pytest.raises(ValueError, match="both registered"):
        msc.resolve_storage_client(PINNED)
    included.write_text(f"url_resolvers: {{catalog: {{type: {RESOLVER}}}}}")
    with pytest.raises(ValueError, match="Duplicate URL resolver"):
        msc.resolve_storage_client(PINNED)


@pytest.mark.parametrize("extension", ["json", "yaml"])
@pytest.mark.parametrize(
    "text, error",
    [
        ('{"url_resolvers":{"catalog":{"type":"one.R"},"catalog":{"type":"two.R"}}}', "Duplicate URL resolver"),
        ('{"url_resolvers":{},"url_resolvers":{}}', "Duplicate url_resolvers section"),
    ],
)
def test_duplicate_registration_in_one_file(tmp_path, monkeypatch, extension, text, error):
    config = tmp_path / f"config.{extension}"
    config.write_text(text)
    monkeypatch.setenv("MSC_CONFIG", str(config))
    with pytest.raises(ValueError, match=error):
        msc.resolve_storage_client(PINNED)


def test_json_resolver_config_uses_json_syntax_and_values(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(
        '{\n\t"url_resolvers": {"catalog": {"type": "example.Resolver", "options": {'
        '"url_resolvers": {"nested": 1, "nested": 2}, "values": [1e2, null, true, "on"]}}}}'
    )
    loaded, _ = StorageClientConfig.read_msc_config([str(config)])
    assert loaded == json.loads(config.read_text())
    with pytest.raises(json.JSONDecodeError):
        StorageClientConfig.from_json('{"url_resolvers": {},}')


@pytest.mark.parametrize(
    "input_url, returned",
    [
        (PINNED, "catalog://ns/r2/files/"),
        (PINNED, "catalog://other/r1/files/"),
        (LATEST, LATEST),
        (LATEST, "other://ns/r1/files/"),
        (LATEST, PINNED + "not-a-root"),
        (LATEST, "catalog://ns/r1?rev=/files/"),
        (LATEST, "catalog://ns/r1#rev=/files/"),
        (LATEST, "catalog://user@ns/r1/files/"),
    ],
)
def test_canonical_redirects_or_invalid_pins_fail_before_provider_build(
    catalog_config, monkeypatch, input_url, returned
):
    catalog_config()
    original = CatalogResolver.bind
    monkeypatch.setattr(
        CatalogResolver, "bind", lambda self, ref: replace(original(self, ref), canonical_root_url=returned)
    )
    monkeypatch.setattr(
        StorageClientConfig, "_from_bound_profile", lambda *a, **k: pytest.fail("constructed providers")
    )
    with pytest.raises(ValueError):
        msc.resolve_storage_client(input_url)


def test_worker_cannot_reopen_removed_or_changed_namespace(catalog_config):
    catalog_config()
    client, _ = msc.resolve_storage_client(PINNED)
    catalog_config(options={"namespace": "another"})
    with pytest.raises(ValueError, match="Unknown catalog namespace"):
        shortcuts._reopen_reference(client.url())
    catalog_config(registrations={}, path_mapping={"catalog://ns/r1/files/": "msc://test/"})
    with pytest.raises(ValueError, match="No URL resolver configured"):
        shortcuts._reopen_reference(client.url())


def test_bind_errors_are_not_cached(catalog_config, monkeypatch):
    catalog_config()
    with pytest.raises(FileNotFoundError):
        msc.resolve_storage_client("catalog://ns/missing/files/file")
    original = CatalogResolver.bind
    monkeypatch.setattr(
        CatalogResolver, "bind", lambda self, ref: replace(original(self, ref), reuse_until=datetime(2030, 1, 1))
    )
    with pytest.raises(ValueError, match="UTC"):
        msc.resolve_storage_client(PINNED)
    monkeypatch.setattr(
        CatalogResolver,
        "bind",
        lambda self, ref: replace(original(self, ref), profile_config={"not_json": Path("example")}),
    )
    with pytest.raises(TypeError):
        msc.resolve_storage_client(PINNED)
    monkeypatch.setattr(CatalogResolver, "bind", original)
    msc.resolve_storage_client(PINNED)
    assert len(CatalogResolver.binds) == 4
