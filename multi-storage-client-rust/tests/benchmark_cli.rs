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

//! End-to-end benchmark configuration, process orchestration, metrics, and cleanup.
#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Output},
};

struct Fixture {
    directory: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir(root.join("objects")).unwrap();
        let storage = json!({"profiles": {"test": {"storage_provider": {
            "type": "file", "options": {"base_path": root.join("objects")}
        }}}});
        std::fs::write(
            root.join("storage.json"),
            serde_json::to_vec(&storage).unwrap(),
        )
        .unwrap();
        let fixture = Self { directory };
        fixture.config(&json!({"processes": [1], "threads": [1], "test_object_sizes": {"1KB": 2}}));
        fixture
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn config(&self, value: &Value) {
        std::fs::write(
            self.root().join("benchmark.json"),
            serde_json::to_vec(value).unwrap(),
        )
        .unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_msc-benchmark"));
        command.current_dir(self.root()).args([
            "--profile",
            "test",
            "--storage-config",
            "storage.json",
            "--config",
            "benchmark.json",
            "--prefix",
            "benchmark",
        ]);
        command
    }

    fn run(&self, arguments: &[&str]) -> Output {
        self.command().args(arguments).output().unwrap()
    }
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn help_and_required_arguments_follow_cli_exit_conventions() {
    for flag in ["--help", "--version"] {
        let output = Command::new(env!("CARGO_BIN_EXE_msc-benchmark"))
            .arg(flag)
            .output()
            .unwrap();
        success(&output);
        assert!(output.stderr.is_empty());
        assert!(String::from_utf8_lossy(&output.stdout).contains("msc-benchmark"));
    }
    let output = Command::new(env!("CARGO_BIN_EXE_msc-benchmark"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--profile"));
}

#[test]
fn multiprocess_matrix_covers_every_object_and_preserves_existing_files() {
    let fixture = Fixture::new();
    fixture.config(
        &json!({"processes": [1,2,8], "threads": [1,3], "test_object_sizes": {"1KB":3,"2KB":1}}),
    );
    let scratch = fixture.root().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    std::fs::write(scratch.join("test-1KB-0"), b"keep local data").unwrap();
    std::fs::create_dir(fixture.root().join("objects/benchmark")).unwrap();
    std::fs::write(
        fixture.root().join("objects/benchmark/test-1KB-0"),
        b"keep remote data",
    )
    .unwrap();
    let output = fixture.run(&[
        "--include-file-tests",
        "--file-tests-dir",
        "scratch",
        "--format",
        "json",
    ]);
    success(&output);
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    let results = summary["results"].as_array().unwrap();
    assert_eq!(results.len(), 60);
    for (index, report) in results.iter().enumerate() {
        let (count, size) = if report["object_size"] == "1KB" {
            (3, 1024)
        } else {
            (1, 2048)
        };
        assert_eq!(report["num_objects"], count);
        assert_eq!(report["processes"], [1, 2, 8][index / 10 % 3].min(count));
        assert_eq!(report["succeeded"], count);
        assert_eq!(report["failed"], 0);
        assert_eq!(
            report["total_bytes"],
            if report["operation"] == "delete" {
                0
            } else {
                count * size
            }
        );
        assert!(report["elapsed_seconds"].as_f64().unwrap() > 0.0);
        assert!(report["p50_response_ms"].as_f64().unwrap() >= 0.0);
    }
    assert_eq!(
        std::fs::read(scratch.join("test-1KB-0")).unwrap(),
        b"keep local data"
    );
    assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 1);
    let prefix = summary["prefix"].as_str().unwrap();
    assert_eq!(
        std::fs::read_dir(fixture.root().join("objects").join(prefix))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        std::fs::read(fixture.root().join("objects/benchmark/test-1KB-0")).unwrap(),
        b"keep remote data"
    );
}

#[test]
fn invalid_configuration_is_rejected_before_data_generation() {
    let fixture = Fixture::new();
    for config in [
        json!({"processes": [], "threads": [1], "test_object_sizes":{"1KB":1}}),
        json!({"processes": [0], "threads": [1], "test_object_sizes":{"1KB":1}}),
        json!({"processes": [1], "threads": [-1], "test_object_sizes":{"1KB":1}}),
        json!({"processes": [1], "threads": [1], "test_object_sizes":{}}),
        json!({"processes": [1], "threads": [1], "test_object_sizes":{"1KB":0}}),
        json!({"processes": [1], "threads": [1], "test_object_sizes":{"1mb":1}}),
        json!({"processes": [1], "threads": [1], "test_object_sizes":{"1KB":1}, "unknown":true}),
    ] {
        fixture.config(&config);
        let output = fixture.run(&["--include-file-tests", "--file-tests-dir", "scratch"]);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(!fixture.root().join("scratch").exists());
        assert_eq!(
            std::fs::read_dir(fixture.root().join("objects"))
                .unwrap()
                .count(),
            0
        );
    }
}

#[test]
fn traversal_unknown_profiles_and_missing_configs_fail_without_mutation() {
    let fixture = Fixture::new();
    for arguments in [
        &["--prefix", "../escape"][..],
        &["--prefix", "/absolute"],
        &["--profile", "missing"],
        &["--storage-config", "missing.yaml"],
        &["--config", "missing.json"],
    ] {
        // Supply overrides once, since clap rejects duplicate options.
        let mut command = Command::new(env!("CARGO_BIN_EXE_msc-benchmark"));
        command.current_dir(fixture.root());
        for (flag, value) in [
            ("--profile", "test"),
            ("--prefix", "benchmark"),
            ("--config", "benchmark.json"),
            ("--storage-config", "storage.json"),
        ] {
            if arguments[0] != flag {
                command.args([flag, value]);
            }
        }
        let output = command.args(arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{arguments:?}");
        assert_eq!(
            std::fs::read_dir(fixture.root().join("objects"))
                .unwrap()
                .count(),
            0
        );
    }
}

#[test]
fn msc_config_environment_discovery_and_text_reports_work() {
    let fixture = Fixture::new();
    let output = Command::new(env!("CARGO_BIN_EXE_msc-benchmark"))
        .current_dir(fixture.root())
        .env("MSC_CONFIG", fixture.root().join("storage.json"))
        .args([
            "--profile",
            "test",
            "--config",
            "benchmark.json",
            "--prefix",
            "benchmark",
        ])
        .output()
        .unwrap();
    success(&output);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("--- upload:"));
    assert!(text.contains("--- download:"));
    assert!(text.contains("--- delete:"));
    assert!(text.contains("Total data transferred: 2 KB"));
    assert!(text.contains("Response time percentiles:"));
}

#[test]
fn configured_object_sizes_can_exceed_the_client_default_read_limit() {
    let fixture = Fixture::new();
    fixture.config(&json!({"processes":[1],"threads":[1],"test_object_sizes":{"65MB":1}}));
    let output = fixture.run(&["--format", "json"]);
    success(&output);
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    let download = &summary["results"][1];
    assert_eq!(download["operation"], "download");
    assert_eq!(download["succeeded"], 1);
    assert_eq!(download["total_bytes"], 65 * 1024 * 1024);
}

#[test]
fn ctrl_c_finishes_the_phase_cleans_objects_and_exits_130() {
    use std::{
        io::{BufRead, BufReader, Read},
        process::Stdio,
    };

    let fixture = Fixture::new();
    fixture.config(&json!({"processes":[2],"threads":[2],"test_object_sizes":{"1KB":512}}));
    let mut child = fixture
        .command()
        .args(["--format", "json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut diagnostics = BufReader::new(child.stderr.take().unwrap());
    loop {
        let mut line = String::new();
        assert!(
            diagnostics.read_line(&mut line).unwrap() > 0,
            "benchmark exited before starting upload"
        );
        if line.contains("Running upload:") {
            break;
        }
    }
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let mut rest = String::new();
    diagnostics.read_to_string(&mut rest).unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130), "{rest}");
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(summary["cancelled"], true);
    let prefix = summary["prefix"].as_str().unwrap();
    assert_eq!(
        std::fs::read_dir(fixture.root().join("objects").join(prefix))
            .unwrap()
            .count(),
        0
    );
    assert!(
        summary["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|report| report["operation"] == "delete" && report["failed"] == 0)
    );
}

#[cfg(feature = "s3")]
#[test]
fn failed_transfers_exit_nonzero_and_report_no_successful_bytes_or_latency() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    let fixture = Fixture::new();
    fixture.config(&json!({"processes":[1],"threads":[1],"test_object_sizes":{"1KB":1}}));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&done);
    let server = std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            if let Ok((mut socket, _)) = listener.accept() {
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut buffer = [0; 8192];
                let _ = socket.read(&mut buffer);
                let _ = socket.write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            } else {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    });
    let storage = json!({"profiles":{"test":{
        "storage_provider":{"type":"s3","options":{"base_path":"bucket","endpoint_url":format!("http://{address}"),"path_style":true,"region_name":"us-east-1"}},
        "credentials_provider":{"type":"static","options":{"access_key_id":"test","secret_access_key":"test"}}
    }}});
    std::fs::write(
        fixture.root().join("storage.json"),
        serde_json::to_vec(&storage).unwrap(),
    )
    .unwrap();
    let output = fixture.run(&["--format", "json"]);
    done.store(true, Ordering::Relaxed);
    server.join().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    for report in summary["results"].as_array().unwrap() {
        assert_eq!(report["failed"], 1);
        assert_eq!(report["succeeded"], 0);
        assert_eq!(report["total_bytes"], 0);
        assert_eq!(report["throughput_bytes_per_second"], 0.0);
        assert!(report["p99_response_ms"].is_null());
        assert!(report["average_response_ms"].is_null());
    }
}

#[cfg(feature = "s3")]
#[test]
#[ignore = "requires local storage simulators; run just run-benchmarks"]
fn benchmark_s3_multiprocess_file_transfers_and_cleanup() {
    use futures::StreamExt;
    use multi_storage_client::{
        ConfigLoader, DeleteOptions, ListOptions, StorageClient, WriteOptions,
    };
    let fixture = Fixture::new();
    fixture.config(&json!({"processes":[1,2],"threads":[2],"test_object_sizes":{"8MB":3}}));
    let remote_prefix = format!(
        "{}/benchmark-{}",
        std::env::var("MSC_S3_TEST_BUCKET").unwrap(),
        uuid::Uuid::new_v4()
    );
    let storage = json!({"profiles":{"test":{
        "storage_provider":{"type":"s3","options":{
            "multipart_threshold":5_242_880,"part_size":5_242_880,"base_path":remote_prefix,"endpoint_url":std::env::var("MSC_S3_TEST_ENDPOINT").unwrap(),"path_style":true,"region_name":"us-east-1"
        }},
        "credentials_provider":{"type":"static","options":{
            "access_key_id":std::env::var("MSC_S3_TEST_ACCESS_KEY").unwrap(),
            "secret_access_key":std::env::var("MSC_S3_TEST_SECRET_KEY").unwrap()
        }}
    }}});
    std::fs::write(
        fixture.root().join("storage.json"),
        serde_json::to_vec(&storage).unwrap(),
    )
    .unwrap();
    let scratch = fixture.root().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    std::fs::write(scratch.join("sentinel"), b"keep local data").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let config = ConfigLoader::new()
            .with_explicit_paths([fixture.root().join("storage.json")])
            .load()
            .unwrap();
        let client = StorageClient::builder(config, "test")
            .build()
            .await
            .unwrap();
        client
            .write(
                "benchmark/sentinel",
                bytes::Bytes::from_static(b"keep remote data"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        client
    });
    let output = fixture.run(&[
        "--include-file-tests",
        "--file-tests-dir",
        "scratch",
        "--format",
        "json",
    ]);
    success(&output);
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(summary["results"].as_array().unwrap().len(), 10);
    for report in summary["results"].as_array().unwrap() {
        assert_eq!(report["succeeded"], 3);
        assert_eq!(report["failed"], 0);
    }
    assert_eq!(
        std::fs::read(scratch.join("sentinel")).unwrap(),
        b"keep local data"
    );
    assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 1);
    runtime.block_on(async {
        let mut objects = client.list_recursive(ListOptions::default()).await.unwrap();
        assert_eq!(
            objects.next().await.unwrap().unwrap().key.as_str(),
            "benchmark/sentinel"
        );
        assert!(objects.next().await.is_none());
        assert_eq!(
            client.read("benchmark/sentinel").await.unwrap(),
            "keep remote data"
        );
        client
            .delete("benchmark/sentinel", false, DeleteOptions::default())
            .await
            .unwrap();
        client.shutdown().await.unwrap();
    });
}
