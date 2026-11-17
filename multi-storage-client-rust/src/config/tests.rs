// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::{Value, json};
use tempfile::TempDir;

use super::{ConfigError, ConfigLoader, ResolvedConfig};

fn profile(bucket: &str) -> Value {
    json!({"storage_provider":{"type":"s3","options":{"base_path":bucket}}})
}

fn config(document: &Value) -> Result<ResolvedConfig, ConfigError> {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(document).unwrap()).unwrap();
    ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(BTreeMap::new())
        .load()
}

fn empty() -> ResolvedConfig {
    ConfigLoader::new()
        .with_explicit_paths(Vec::<String>::new())
        .with_environment(BTreeMap::new())
        .load()
        .unwrap()
}

#[test]
fn no_config_has_only_the_reserved_filesystem_profile() {
    let resolved = empty();
    assert_eq!(resolved.profiles.len(), 1);
    assert_eq!(
        resolved
            .profile("__filesystem__")
            .unwrap()
            .storage_provider
            .as_ref()
            .unwrap()
            .options["base_path"],
        "/"
    );
    assert!(resolved.sources.is_empty());
    assert!(resolved.profile("missing").is_err());
    assert_eq!(
        resolved.resolve("/tmp//file").unwrap().key.as_str(),
        "tmp/file"
    );
}

#[test]
fn optional_provider_nulls_round_trip_through_the_profile_document() {
    let resolved = config(&json!({"profiles":{"data":{
        "storage_provider":null,"credentials_provider":null
    }}}))
    .unwrap();
    let profile = resolved.profile("data").unwrap();
    assert!(profile.storage_provider.is_none());
    assert!(profile.credentials_provider.is_none());
    config(&json!({"profiles":{"data":profile.document()}})).unwrap();
}

#[test]
fn json_and_yaml_have_identical_effective_profiles() {
    let temp = TempDir::new().unwrap();
    let document = json!({"profiles":{"data":profile("bucket")}});
    let json_path = temp.path().join("config.json");
    let yaml_path = temp.path().join("config.yaml");
    std::fs::write(&json_path, serde_json::to_vec(&document).unwrap()).unwrap();
    std::fs::write(&yaml_path, serde_yaml::to_string(&document).unwrap()).unwrap();
    let load = |path| {
        ConfigLoader::new()
            .with_explicit_paths([path])
            .load()
            .unwrap()
    };
    assert_eq!(
        load(json_path).redacted_document(),
        load(yaml_path).redacted_document()
    );
}

#[test]
fn discovery_candidates_preserve_every_position_and_first_existing_wins() {
    let temp = TempDir::new().unwrap();
    let environment = BTreeMap::from([
        (
            "MSC_CONFIG".into(),
            temp.path().join("override.yaml").display().to_string(),
        ),
        (
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg-home").display().to_string(),
        ),
        (
            "HOME".into(),
            temp.path().join("home").display().to_string(),
        ),
        (
            "XDG_CONFIG_DIRS".into(),
            format!(
                "{}:{}",
                temp.path().join("system-a").display(),
                temp.path().join("system-b").display()
            ),
        ),
    ]);
    let paths = super::loader::discovery_paths(&environment);
    assert_eq!(paths.len(), 13);
    assert_eq!(paths[0], temp.path().join("override.yaml"));
    assert_eq!(paths[1], temp.path().join("xdg-home/msc/config.yaml"));
    assert_eq!(paths[2], temp.path().join("xdg-home/msc/config.json"));
    assert_eq!(paths[3], temp.path().join("home/.msc_config.yaml"));
    assert_eq!(paths[4], temp.path().join("home/.msc_config.json"));
    assert_eq!(paths[5], temp.path().join("home/.config/msc/config.yaml"));
    assert_eq!(paths[6], temp.path().join("home/.config/msc/config.json"));
    assert_eq!(paths[7], temp.path().join("system-a/msc/config.yaml"));
    assert_eq!(paths[8], temp.path().join("system-a/msc/config.json"));
    assert_eq!(paths[9], temp.path().join("system-b/msc/config.yaml"));
    assert_eq!(paths[10], temp.path().join("system-b/msc/config.json"));
    assert_eq!(paths[11], Path::new("/etc/msc_config.yaml"));
    assert_eq!(paths[12], Path::new("/etc/msc_config.json"));
    // Explicit candidates allow exercising system slots without writing /etc.
    for position in 0..paths.len() {
        let simulated: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(index, path)| {
                if path.starts_with("/etc/") {
                    temp.path().join(format!("system-{index}.json"))
                } else {
                    path.clone()
                }
            })
            .collect();
        for path in &simulated {
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        let path = &simulated[position];
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "{}").unwrap();
        let result = ConfigLoader::new()
            .with_explicit_paths(simulated.clone())
            .with_environment(environment.clone())
            .load()
            .unwrap();
        assert_eq!(result.sources, vec![path.clone()]);
    }
    let defaults = super::loader::discovery_paths(&BTreeMap::new());
    assert_eq!(defaults[0], Path::new("/etc/xdg/msc/config.yaml"));
}

#[test]
fn explicit_paths_suppress_environment_discovery_and_bad_first_file_is_final() {
    let temp = TempDir::new().unwrap();
    let bad = temp.path().join("bad.json");
    let good = temp.path().join("good.json");
    std::fs::write(&bad, "{broken").unwrap();
    std::fs::write(&good, "{}").unwrap();
    let environment = BTreeMap::from([("MSC_CONFIG".into(), bad.display().to_string())]);
    assert!(
        ConfigLoader::new()
            .with_environment(environment.clone())
            .load()
            .is_err()
    );
    assert!(
        ConfigLoader::new()
            .with_explicit_paths([&bad, &good])
            .with_environment(environment.clone())
            .load()
            .is_err()
    );
    let resolved = ConfigLoader::new()
        .with_explicit_paths([&good])
        .with_environment(environment)
        .load()
        .unwrap();
    assert_eq!(resolved.sources, [good]);
}

#[test]
fn unknown_extensions_are_rejected_even_when_content_is_valid() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.txt");
    std::fs::write(&path, "{}").unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([&path])
        .load()
        .unwrap_err();
    assert!(error.message.contains("extension"));
    assert_eq!(error.source, Some(path));
}

#[test]
fn relative_includes_are_ordered_and_identical_definitions_are_idempotent() {
    let temp = TempDir::new().unwrap();
    let primary = temp.path().join("main.json");
    let included = temp.path().join("child.yaml");
    std::fs::write(
        &primary,
        serde_json::to_vec(&json!({"include":["child.yaml"],"profiles":{"data":profile("a")}}))
            .unwrap(),
    )
    .unwrap();
    std::fs::write(
        &included,
        serde_yaml::to_string(&json!({"profiles":{"data":profile("a"),"other":profile("b")}}))
            .unwrap(),
    )
    .unwrap();
    let resolved = ConfigLoader::new()
        .with_explicit_paths([&primary])
        .load()
        .unwrap();
    assert_eq!(resolved.sources, [primary.clone(), included.clone()]);
    assert_eq!(resolved.profiles.len(), 3);
    assert!(resolved.redacted_document().get("include").is_none());
    assert_eq!(
        resolved.source_for("/profiles/other/storage_provider/options/base_path"),
        Some(included.as_path())
    );
    std::fs::write(
        &included,
        serde_yaml::to_string(&json!({"profiles":{"data":profile("different-secret-value")}}))
            .unwrap(),
    )
    .unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([&primary])
        .load()
        .unwrap_err();
    assert_eq!(error.field_path, "/profiles/data");
    assert_eq!(error.source, Some(included));
    assert!(error.message.contains(&primary.display().to_string()));
    assert!(!error.to_string().contains("different-secret-value"));
}

#[test]
fn missing_and_nested_includes_name_the_contributing_source() {
    let temp = TempDir::new().unwrap();
    let primary = temp.path().join("main.yaml");
    let child = temp.path().join("child.yaml");
    std::fs::write(&primary, "include: [child.yaml]\n").unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([&primary])
        .load()
        .unwrap_err();
    assert_eq!(error.source, Some(child.clone()));
    std::fs::write(&child, "include: []\n").unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([&primary])
        .load()
        .unwrap_err();
    assert_eq!(error.source, Some(child));
    assert!(error.message.contains("nested"));
}

#[test]
fn include_profiles_are_not_merged_field_by_field() {
    let temp = TempDir::new().unwrap();
    let primary = temp.path().join("main.json");
    let child = temp.path().join("child.json");
    std::fs::write(
        &primary,
        serde_json::to_vec(
            &json!({"include":["child.json"],"profiles":{"data":profile("bucket")}}),
        )
        .unwrap(),
    )
    .unwrap();
    std::fs::write(&child, r#"{"profiles":{"data":{"caching_enabled":false}}}"#).unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([primary])
        .load()
        .unwrap_err();
    assert_eq!(error.field_path, "/profiles/data");
}

#[test]
fn unknown_fields_are_rejected_at_msc_levels_and_provider_options_remain_open() {
    let documents = [
        json!({"profiles":{},"wrong":true}),
        json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket"}},"wrong":1}}}),
        json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket"},"wrong":1}}}}),
        json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket"}},"retry":{"wrong":1}}}}),
    ];
    for document in documents {
        assert!(
            config(&document)
                .unwrap_err()
                .field_path
                .ends_with("/wrong")
        );
    }
    let valid = config(&json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket","custom":{"arbitrary":true}}}}}})).unwrap();
    assert_eq!(
        valid
            .profile("data")
            .unwrap()
            .storage_provider
            .as_ref()
            .unwrap()
            .options["custom"]["arbitrary"],
        true
    );
}

#[test]
fn environment_values_expand_recursively_but_keys_do_not() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.json");
    let document = json!({"profiles":{"$NAME":{"storage_provider":{"type":"s3","options":{"base_path":"$BUCKET/prefix","nested":["${BUCKET}",{"value":"${SECRET}"}]}}}}});
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let environment = BTreeMap::from([
        ("BUCKET".into(), "bucket".into()),
        ("SECRET".into(), "config-test-secret-123".into()),
    ]);
    let resolved = ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(environment)
        .load()
        .unwrap();
    let provider = resolved
        .profile("$NAME")
        .unwrap()
        .storage_provider
        .as_ref()
        .unwrap();
    assert_eq!(provider.options["base_path"], "bucket/prefix");
    assert_eq!(
        provider.options["nested"][1]["value"],
        "config-test-secret-123"
    );
    for output in [
        format!("{resolved:?}"),
        resolved.to_string(),
        format!("{provider:?}"),
        format!("{:?}", resolved.profile("$NAME").unwrap()),
    ] {
        assert!(!output.contains("config-test-secret-123"));
    }
}

#[test]
fn missing_environment_values_and_noncoercion_fail_with_sources() {
    let error = config(&json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"$MISSING_BUCKET"}}}}})).unwrap_err();
    assert_eq!(
        error.field_path,
        "/profiles/data/storage_provider/options/base_path"
    );
    assert!(error.message.contains("MISSING_BUCKET"));
    assert!(error.source.is_some());
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket"}},"retry":{"jitter":"$FLAG"}}}})).unwrap()).unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(BTreeMap::from([("FLAG".into(), "true".into())]))
        .load()
        .unwrap_err();
    assert!(error.message.contains("not coerced"));
}

#[test]
fn config_display_and_parser_errors_never_print_literal_secrets() {
    let resolved = config(&json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{"base_path":"bucket"}},"credentials_provider":{"type":"S3Credentials","options":{"access_key":"access-secret-123","secret_key":"literal-secret-123"}}}}})).unwrap();
    assert!(!format!("{resolved:?} {resolved}").contains("literal-secret-123"));
    assert!(!format!("{resolved:?} {resolved}").contains("access-secret-123"));
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.yaml");
    std::fs::write(&path, "secret: [literal-secret-123\n").unwrap();
    let error = ConfigLoader::new()
        .with_explicit_paths([path])
        .load()
        .unwrap_err();
    assert!(!error.to_string().contains("literal-secret-123"));
}

#[test]
fn display_redacts_custom_credentials_and_embedded_url_authentication() {
    let resolved = config(&json!({"profiles":{"data":{
        "storage_provider":{"type":"s3","options":{"base_path":"bucket", "api_key":"custom-secret-123", "endpoint":"http://user:embedded-secret-123@localhost", "remote":"https://localhost/path?signature=query-secret-123"}},
        "credentials_provider":{"type":"custom","options":{"custom_field":"custom-credential-secret-123"}}
    }}})).unwrap();
    let output = format!("{resolved:?} {resolved}");
    for secret in [
        "custom-secret-123",
        "embedded-secret-123",
        "query-secret-123",
        "custom-credential-secret-123",
    ] {
        assert!(!output.contains(secret));
    }
    assert_eq!(
        resolved.redacted_document()["profiles"]["data"]["storage_provider"]["options"]["base_path"],
        "bucket"
    );
    let error = config(&json!({"profiles":{"data":profile("bucket")},"path_mapping":{"https://user:mapping-secret-123@localhost/prefix/":"msc://data/"}})).unwrap_err();
    assert!(!error.to_string().contains("mapping-secret-123"));
}

#[test]
fn provider_debug_redacts_expanded_tags_and_credential_scalar_values() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("config.json");
    let document = json!({"profiles":{"data":{
        "storage_provider":{"type":"s3","options":{"base_path":"bucket"}},
        "credentials_provider":{"type":"$CUSTOM_TYPE","options":{
            "pin":987_654_321, "label":"credential-label-123", "enabled":true,
            "nested":{"credentials_provider":{"type":"nested-secret-123","options":{}}}
        }}
    }}});
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let resolved = ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(BTreeMap::from([(
            "CUSTOM_TYPE".into(),
            "credential-tag-123".into(),
        )]))
        .load()
        .unwrap();
    let credentials = resolved
        .profile("data")
        .unwrap()
        .credentials_provider
        .as_ref()
        .unwrap();
    let output = format!("{resolved:?} {resolved} {credentials:?}");
    for secret in [
        "987654321",
        "credential-label-123",
        "credential-tag-123",
        "nested-secret-123",
    ] {
        assert!(!output.contains(secret), "credential material leaked");
    }
    let redacted =
        &resolved.redacted_document()["profiles"]["data"]["credentials_provider"]["options"];
    assert_eq!(redacted["enabled"], "[REDACTED]");
    assert_eq!(credentials.options["pin"], 987_654_321);
}

#[test]
fn urls_normalize_separators_and_keep_profile_and_key_separate() {
    let resolved = config(&json!({"profiles":{"data":profile("bucket")}})).unwrap();
    let url = resolved.resolve("msc://data///a//b/").unwrap();
    assert_eq!(url.profile, "data");
    assert_eq!(url.key.as_str(), "a/b");
    assert_eq!(resolved.resolve("msc://data").unwrap().key.as_str(), "");
    assert!(
        resolved
            .resolve("msc://unknown/key")
            .unwrap_err()
            .field_path
            .contains("profiles")
    );
    assert_eq!(
        resolved.resolve("file:///tmp//file").unwrap().key.as_str(),
        "tmp/file"
    );
    assert!(resolved.resolve("file://remote/path").is_err());
    let implicit = resolved
        .resolve("s3://some-bucket/path%20with%20spaces")
        .unwrap();
    assert_eq!(implicit.profile, "_s3-some-bucket");
    assert_eq!(implicit.key.as_str(), "path with spaces");
    assert_eq!(
        implicit.profile_config.storage_provider.unwrap().options["base_path"],
        "some-bucket"
    );
    assert!(resolved.resolve("gs://bucket/file").is_err());
    assert!(resolved.resolve("ais://bucket/file").is_err());
    assert!(resolved.resolve("relative/path").is_err());
    assert_eq!(
        resolved
            .resolve_with_base("file", Path::new("/tmp"))
            .unwrap()
            .key
            .as_str(),
        "tmp/file"
    );
    assert_eq!(
        resolved
            .resolve_with_base("./file", Path::new("/tmp"))
            .unwrap()
            .key
            .as_str(),
        "tmp/file"
    );
    assert!(
        resolved
            .resolve_with_base("../file", Path::new("/tmp"))
            .is_err()
    );
}

#[test]
fn longest_path_mapping_wins_before_implicit_synthesis() {
    let resolved = config(&json!({"profiles":{"wide":profile("a"),"narrow":profile("b")},"path_mapping":{"s3://bucket/":"msc://wide/","s3://bucket/nested/":"msc://narrow/","/mounted/":"msc://wide/"}})).unwrap();
    let mapped = resolved.resolve("s3://bucket/nested/dir/file").unwrap();
    assert_eq!(mapped.profile, "narrow");
    assert_eq!(mapped.key.as_str(), "dir/file");
    assert_eq!(
        resolved.resolve("s3://bucket/other").unwrap().profile,
        "wide"
    );
    assert_eq!(
        resolved.resolve("s3://bucket/nested").unwrap().key.as_str(),
        ""
    );
    assert_eq!(
        resolved.resolve("/mounted/a%2eb?#").unwrap().key.as_str(),
        "a%2eb?#"
    );
    assert!(config(&json!({"path_mapping":{"/tmp/":"msc://missing/"}})).is_err());
}

#[test]
fn urls_reject_encoded_traversal_before_normalization_or_mapping() {
    let resolved = config(&json!({"profiles":{"data":profile("bucket")},"path_mapping":{"s3://bucket/":"msc://data/"}})).unwrap();
    for url in [
        "msc://data/a/../b",
        "msc://data/%2e%2e/secret",
        "msc://data/a/%2E/b",
        "msc://data/%2f..%2fsecret",
        "s3://bucket/../secret",
        "file:///a/%2e%2e/secret",
        "msc://data/%FF",
        "msc://data/%zz",
        "msc://data/file?secret=hidden",
        "/a/../b",
    ] {
        assert!(resolved.resolve(url).is_err(), "accepted {url}");
    }
    assert_eq!(
        resolved
            .resolve("msc://data/%252e%252e")
            .unwrap()
            .key
            .as_str(),
        "%2e%2e"
    );
}

#[test]
fn invalid_profile_kinds_and_reserved_names_are_rejected() {
    for document in [
        json!({"profiles":{"_invalid":profile("a")}}),
        json!({"profiles":{"__filesystem__":profile("a")}}),
    ] {
        assert!(config(&document).is_err());
    }
    assert!(config(&json!({"profiles":{"__filesystem__":{"storage_provider":{"type":"file","options":{"base_path":"/"}}}}})).is_ok());
}

#[test]
fn retry_accepts_python_fields_and_validates_bounds() {
    let mut data = profile("a");
    data["retry"] = json!({"attempts":4,"delay":0.25,"backoff_multiplier":1.5});
    let resolved = config(&json!({"profiles":{"data":data}})).unwrap();
    assert_eq!(resolved.profile("data").unwrap().retry.max_attempts, 4);
    assert_eq!(
        resolved.profile("data").unwrap().retry.initial_delay_ms,
        250
    );
    assert!((resolved.profile("data").unwrap().retry.multiplier - 1.5).abs() < f64::EPSILON);
    data["retry"] = json!({"max_attempts":0});
    assert!(config(&json!({"profiles":{"data":data}})).is_err());
}

#[tokio::test]
async fn unsupported_profiles_load_but_cannot_be_selected_without_a_factory() {
    for tag in ["gcs_s3", "ais_s3", "my.module.CustomProvider", "gcs"] {
        let resolved = config(&json!({"profiles":{"data":{"storage_provider":{"type":tag,"options":{"base_path":"a"}}}}})).unwrap();
        let error = crate::ProviderRegistry::new()
            .build(resolved.profile("data").unwrap())
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error.kind,
            crate::ErrorKind::Unsupported | crate::ErrorKind::InvalidConfig
        ));
        assert!(!error.message.contains("Cargo feature"));
    }
    assert!(
        config(&json!({"profiles":{"data":{"storage_provider":{"type":"s3","options":{}}}}}))
            .is_err()
    );
    assert!(config(&json!({"profiles":{"data":profile("")}})).is_ok());
}

#[test]
fn python_s8k_profiles_keep_s3_options_credentials_and_url_resolution() {
    let resolved = config(&json!({"profiles": {
        "datasource": {
            "storage_provider": {"type": "s8k", "options": {
                "base_path": "bucket/project", "region_name": "us-west-2",
                "endpoint_url": "http://127.0.0.1:1"
            }},
            "credentials_provider": {"type": "S3Credentials", "options": {
                "access_key": "private-access", "secret_key": "private-secret"
            }}
        },
        "empty-base-path": {"storage_provider": {"type": "s8k", "options": {
            "base_path": ""
        }}}
    }}))
    .unwrap();
    let path = resolved
        .resolve("msc://datasource/nested/file.txt")
        .unwrap();
    assert_eq!(path.profile, "datasource");
    assert_eq!(path.key.as_str(), "nested/file.txt");
    let provider = path.profile_config.storage_provider.as_ref().unwrap();
    assert_eq!(provider.provider_type, "s8k");
    assert_eq!(provider.options["base_path"], "bucket/project");
    let redacted = resolved.redacted_document().to_string();
    assert!(!redacted.contains("private-access"));
    assert!(!redacted.contains("private-secret"));
}

#[test]
fn deferred_configuration_is_validated_only_when_profiles_are_selected() {
    for field in ["cache", "opentelemetry", "experimental_features", "posix"] {
        let mut document = json!({"profiles": {"data": profile("bucket")}});
        document[field] = json!({});
        config(&document)
            .unwrap()
            .profile("data")
            .unwrap()
            .validate_runtime_support()
            .unwrap();
    }
    for field in [
        "metadata_provider",
        "provider_bundle",
        "storage_provider_profiles",
        "replicas",
        "autocommit",
        "caching_enabled",
    ] {
        let mut document = json!({"profiles": {"data": profile("bucket")}});
        document["profiles"]["data"][field] = json!(true);
        let error = config(&document)
            .unwrap()
            .validate_runtime_support()
            .unwrap_err();
        assert_eq!(error.field_path, format!("/profiles/data/{field}"));
        document["profiles"]["data"][field] = json!(false);
        config(&document)
            .unwrap()
            .validate_runtime_support()
            .unwrap();
    }
    for tag in ["gcs", "azure", "dss"] {
        let mut document = json!({"profiles": {"data": profile("bucket")}});
        document["profiles"]["data"]["storage_provider"]["type"] = json!(tag);
        let error = config(&document)
            .unwrap()
            .validate_runtime_support()
            .unwrap_err();
        assert_eq!(error.field_path, "/profiles/data/storage_provider/type");
    }
}

#[tokio::test]
async fn shared_python_config_can_build_a_supported_profile() {
    let directory = tempfile::tempdir().unwrap();
    let resolved = config(&json!({"opentelemetry":{"enabled":true},"cache":{"size":"1GiB"},"profiles":{
        "local":{"storage_provider":{"type":"file","options":{"base_path":directory.path()}}},
        "future":{"metadata_provider":{"type":"python.module.Metadata"},"storage_provider":{"type":"gcs","options":{"base_path":"bucket"}}},
        "composite":{"storage_provider_profiles":["local"]}
    }})).unwrap();
    let client = crate::StorageClient::builder(resolved, "local")
        .build()
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

#[test]
fn cloned_and_renamed_profiles_keep_environment_secret_redaction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&json!({"profiles":{"original":{"storage_provider":{"type":"s3","options":{"base_path":"bucket","nested":[0,{"value":"${PRIVATE_TOKEN}"}]}}}}})).unwrap()).unwrap();
    let mut resolved = ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(BTreeMap::from([(
            "PRIVATE_TOKEN".into(),
            "expanded-private-value".into(),
        )]))
        .load()
        .unwrap();
    let profile = resolved.profiles.remove("original").unwrap();
    resolved.profiles.insert("renamed".into(), profile.clone());
    resolved.profiles.insert("cloned".into(), profile);
    assert!(
        !format!("{resolved:?} {resolved} {}", resolved.redacted_document())
            .contains("expanded-private-value")
    );
}

#[test]
fn runtime_validation_keeps_escaped_profile_paths_and_source_files() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&json!({"profiles": {"a~/b": profile("bucket")}})).unwrap(),
    )
    .unwrap();
    let mut resolved = ConfigLoader::new()
        .with_explicit_paths([&path])
        .load()
        .unwrap();
    resolved
        .profiles
        .get_mut("a~/b")
        .unwrap()
        .storage_provider
        .as_mut()
        .unwrap()
        .provider_type = "gcs".into();
    let error = resolved.validate_runtime_support().unwrap_err();
    assert_eq!(error.field_path, "/profiles/a~0~1b/storage_provider/type");
    assert_eq!(error.source.as_deref(), Some(path.as_path()));
}
