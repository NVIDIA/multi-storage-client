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

//! Multiprocess storage transfer benchmark using the Rust MSC client.
mod config;
mod metrics;
mod processes;
mod worker;

use clap::{Parser, ValueEnum};
use multi_storage_client::{ConfigLoader, ObjectPath, ResolvedConfig};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use config::{Config, size_to_bytes};
use metrics::Report;
use processes::Workers;
use worker::{Job, Phase};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    name = "msc-benchmark",
    version,
    about = "Upload/download performance tests with Multi-Storage Client"
)]
struct Args {
    /// MSC profile to benchmark.
    #[arg(long, required_unless_present = "worker_job")]
    profile: Option<String>,
    /// Benchmark JSON configuration (processes, threads, `test_object_sizes`).
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
    /// MSC YAML/JSON configuration; otherwise use normal `MSC_CONFIG` discovery.
    #[arg(long, value_name = "FILE")]
    storage_config: Option<PathBuf>,
    /// Profile-relative prefix for a unique directory of test objects.
    #[arg(long, default_value = "")]
    prefix: String,
    /// Include `upload_from` and `download_to` tests.
    #[arg(long)]
    include_file_tests: bool,
    /// Parent for a temporary file-test directory; existing contents are preserved.
    #[arg(long, default_value = "/tmp/msc_benchmark", value_name = "DIR")]
    file_tests_dir: PathBuf,
    /// Print readable summaries or one JSON result document.
    #[arg(long, value_enum, default_value = "text")]
    format: Format,
    #[arg(long, hide = true)]
    worker_job: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

#[derive(Serialize)]
struct Summary {
    profile: String,
    prefix: String,
    results: Vec<Report>,
    cancelled: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let outcome = if let Some(path) = &args.worker_job {
        run_worker(path)
    } else {
        run_coordinator(args)
    };
    match outcome {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("msc-benchmark: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_worker(path: &Path) -> Result<u8> {
    let job: Job = serde_json::from_slice(&std::fs::read(path)?)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(job.threads)
        .enable_all()
        .build()?;
    runtime.block_on(worker::run(job))?;
    Ok(0)
}

fn run_coordinator(args: Args) -> Result<u8> {
    let config = Config::load(args.config.as_deref())?;
    let prefix = ObjectPath::new(&args.prefix)?.join(&ObjectPath::new(format!(
        "msc-benchmark-{}",
        uuid::Uuid::new_v4()
    ))?)?;
    let mut loader = ConfigLoader::new();
    if let Some(path) = &args.storage_config {
        if !path.is_file() {
            return Err(
                format!("MSC configuration file does not exist: {}", path.display()).into(),
            );
        }
        loader = loader.with_explicit_paths([path]);
    }
    let storage = loader.load()?;
    let profile = args.profile.as_deref().ok_or("--profile is required")?;
    storage.profile(profile)?.validate_runtime_support()?;
    eprintln!("Benchmark objects: msc://{profile}/{prefix}");
    if args.config.is_none() {
        eprintln!(
            "Using default configuration: 8 processes x 4 threads; 12800 x 4MB and 800 x 64MB"
        );
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(run(args, config, prefix.as_str(), &storage))
}

async fn run(args: Args, config: Config, prefix: &str, storage: &ResolvedConfig) -> Result<u8> {
    let cancellation = CancellationToken::new();
    let signal_token = cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("Interrupted; finishing the current phase and cleaning up test objects");
            signal_token.cancel();
        }
    });
    let mut summary = Summary {
        profile: args.profile.clone().ok_or("--profile is required")?,
        prefix: prefix.into(),
        results: Vec::new(),
        cancelled: false,
    };
    let result = run_matrix(&args, &config, &mut summary, &cancellation, storage).await;
    signal.abort();
    let _ = signal.await;
    summary.cancelled = cancellation.is_cancelled();
    let failed = summary.results.iter().any(|report| report.failed != 0);
    if matches!(args.format, Format::Json) {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    }
    if let Err(error) = result {
        if !summary.cancelled {
            return Err(error);
        }
        eprintln!("msc-benchmark: cleanup after interruption failed: {error}");
    }
    Ok(if summary.cancelled {
        130
    } else {
        u8::from(failed)
    })
}

async fn run_matrix(
    args: &Args,
    config: &Config,
    summary: &mut Summary,
    cancellation: &CancellationToken,
    storage: &ResolvedConfig,
) -> Result<()> {
    for (size, count) in &config.test_object_sizes {
        if cancellation.is_cancelled() {
            break;
        }
        let directory = prepare_files(args, config, storage, size, *count)?;
        let payload = directory.path().join("payload");
        for processes in &config.processes {
            for threads in &config.threads {
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                let job = Job {
                    profile: summary.profile.clone(),
                    storage_config: args.storage_config.clone(),
                    prefix: summary.prefix.clone(),
                    size: size.clone(),
                    size_bytes: size_to_bytes(size)?,
                    start: 0,
                    end: *count,
                    processes: *processes,
                    threads: *threads,
                    payload: payload.clone(),
                    files_directory: args
                        .include_file_tests
                        .then(|| directory.path().to_path_buf()),
                };
                let mut workers = Workers::start(&job, *count, directory.path()).await?;
                let tested = run_tests(args, &job, &mut workers, summary, cancellation).await;
                // Cleanup runs after transfer errors, and only touches this run's keys.
                let cleaned = if tested.is_ok() {
                    run_phase(args, &job, &mut workers, summary, Phase::Delete).await
                } else {
                    Ok(())
                };
                let interrupted = tested.is_err()
                    || cleaned.is_err()
                    || summary.results.last().is_some_and(|report| {
                        matches!(report.operation, Phase::Delete) && report.failed != 0
                    });
                let stopped = if interrupted {
                    workers.abort().await
                } else {
                    workers.stop().await
                };
                if interrupted || stopped.is_err() {
                    worker::cleanup(&job).await?;
                }
                tested.and(cleaned).and(stopped)?;
            }
        }
    }
    Ok(())
}

async fn run_tests(
    args: &Args,
    job: &Job,
    workers: &mut Workers,
    summary: &mut Summary,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut phases = vec![Phase::Upload, Phase::Download];
    if args.include_file_tests {
        phases.extend([Phase::UploadFile, Phase::DownloadFile]);
    }
    for phase in phases {
        if cancellation.is_cancelled() {
            break;
        }
        run_phase(args, job, workers, summary, phase).await?;
    }
    Ok(())
}

async fn run_phase(
    args: &Args,
    job: &Job,
    workers: &mut Workers,
    summary: &mut Summary,
    phase: Phase,
) -> Result<()> {
    eprintln!(
        "Running {phase}: {} x {}, {} processes x {} threads",
        job.end,
        job.size,
        workers.len(),
        job.threads
    );
    let (measurements, elapsed) = workers.phase(phase).await?;
    let report = Report::new(
        phase,
        &job.size,
        workers.len(),
        job.threads,
        elapsed,
        measurements,
    );
    if matches!(args.format, Format::Text) {
        report.print();
    }
    summary.results.push(report);
    Ok(())
}

fn prepare_files(
    args: &Args,
    config: &Config,
    storage: &ResolvedConfig,
    size: &str,
    count: usize,
) -> Result<TempDir> {
    let directory = if args.include_file_tests {
        std::fs::create_dir_all(&args.file_tests_dir)?;
        tempfile::Builder::new()
            .prefix("msc-benchmark-")
            .tempdir_in(&args.file_tests_dir)?
    } else {
        tempfile::Builder::new()
            .prefix("msc-benchmark-")
            .tempdir()?
    };
    let size_bytes = size_to_bytes(size)?;
    let count = u64::try_from(count)?;
    let concurrency = config
        .processes
        .iter()
        .max()
        .copied()
        .unwrap_or(1)
        .checked_mul(config.threads.iter().max().copied().unwrap_or(1))
        .ok_or("concurrency overflow")?;
    let active = count.min(u64::try_from(concurrency)?);
    let mut requirements = Vec::new();
    let local_copies = if args.include_file_tests {
        count.checked_add(active).ok_or("file count overflow")?
    } else {
        0
    };
    requirements.push((
        directory.path().to_path_buf(),
        size_bytes
            .checked_mul(local_copies.checked_add(1).ok_or("file count overflow")?)
            .ok_or("scratch space requirement overflow")?,
    ));
    let profile = storage.profile(args.profile.as_deref().ok_or("--profile is required")?)?;
    if let Some(provider) = &profile.storage_provider
        && provider.provider_type == "file"
    {
        let options: multi_storage_client::provider::posix::PosixOptions =
            serde_json::from_value(serde_json::to_value(&provider.options)?)?;
        let copies = count
            .checked_mul(if args.include_file_tests { 2 } else { 1 })
            .and_then(|copies| copies.checked_add(active))
            .ok_or("destination file count overflow")?;
        requirements.push((
            options.base_path,
            size_bytes
                .checked_mul(copies)
                .ok_or("destination space requirement overflow")?,
        ));
    }
    check_free_space(&requirements)?;
    let mut data = Vec::new();
    data.try_reserve_exact(usize::try_from(size_bytes)?)?;
    data.resize(usize::try_from(size_bytes)?, 0);
    getrandom::fill(&mut data)
        .map_err(|error| format!("cannot generate benchmark data: {error}"))?;
    std::fs::write(directory.path().join("payload"), &data)?;
    if args.include_file_tests {
        eprintln!(
            "Generating {count} x {size} test files in {}",
            directory.path().display()
        );
        for index in 0..count {
            std::fs::write(directory.path().join(format!("test-{size}-{index}")), &data)?;
        }
    }
    Ok(directory)
}

fn check_free_space(requirements: &[(PathBuf, u64)]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::{collections::BTreeMap, os::unix::fs::MetadataExt};
        let mut filesystems = BTreeMap::<u64, (&Path, u64)>::new();
        for (path, bytes) in requirements {
            let entry = filesystems
                .entry(std::fs::metadata(path)?.dev())
                .or_insert((path, 0));
            entry.1 = entry
                .1
                .checked_add(*bytes)
                .ok_or("filesystem space requirement overflow")?;
        }
        for (directory, required) in filesystems.into_values() {
            let status = rustix::fs::statvfs(directory)?;
            let free = u128::from(status.f_bavail) * u128::from(status.f_frsize);
            if u128::from(required) * 10 > free * 9 {
                return Err(format!("refusing to generate {required} bytes of test data: insufficient free space in {}", directory.display()).into());
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = requirements;
        return Err("benchmark scratch-space checks require Unix".into());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn preflight_aggregates_directories_on_the_same_filesystem() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let status = rustix::fs::statvfs(directory.path()).unwrap();
        let free =
            u64::try_from(u128::from(status.f_bavail) * u128::from(status.f_frsize)).unwrap();
        let bytes = free / 5 * 3;
        assert!(check_free_space(&[(directory.path().to_path_buf(), bytes)]).is_ok());
        assert!(
            check_free_space(&[(directory.path().to_path_buf(), bytes), (nested, bytes)]).is_err()
        );
    }
}
