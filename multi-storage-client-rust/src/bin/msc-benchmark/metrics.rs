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

use serde::{Deserialize, Serialize};

use super::worker::Phase;

#[derive(Default, Deserialize, Serialize)]
pub(super) struct Measurements {
    pub succeeded: usize,
    pub failed: usize,
    pub bytes: u64,
    pub latencies: Vec<f64>,
}

impl Measurements {
    pub fn merge(&mut self, other: Self) {
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.bytes += other.bytes;
        self.latencies.extend(other.latencies);
    }
}

#[derive(Serialize)]
pub(super) struct Report {
    pub operation: Phase,
    pub object_size: String,
    pub num_objects: usize,
    pub processes: usize,
    pub threads: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub total_bytes: u64,
    pub elapsed_seconds: f64,
    pub throughput_bytes_per_second: f64,
    pub operations_per_second: f64,
    pub average_response_ms: Option<f64>,
    pub p50_response_ms: Option<f64>,
    pub p90_response_ms: Option<f64>,
    pub p99_response_ms: Option<f64>,
}

impl Report {
    #[allow(clippy::cast_precision_loss)] // Benchmark values are approximate floating-point measurements.
    pub fn new(
        phase: Phase,
        size: &str,
        processes: usize,
        threads: usize,
        elapsed: f64,
        mut data: Measurements,
    ) -> Self {
        data.latencies.sort_unstable_by(f64::total_cmp);
        let average = if data.latencies.is_empty() {
            None
        } else {
            Some(data.latencies.iter().sum::<f64>() / data.latencies.len() as f64 * 1000.0)
        };
        Self {
            operation: phase,
            object_size: size.into(),
            num_objects: data.succeeded + data.failed,
            processes,
            threads,
            succeeded: data.succeeded,
            failed: data.failed,
            total_bytes: data.bytes,
            elapsed_seconds: elapsed,
            throughput_bytes_per_second: data.bytes as f64 / elapsed.max(f64::MIN_POSITIVE),
            operations_per_second: data.succeeded as f64 / elapsed.max(f64::MIN_POSITIVE),
            average_response_ms: average,
            p50_response_ms: median(&data.latencies).map(|v| v * 1000.0),
            p90_response_ms: percentile(&data.latencies, 90).map(|v| v * 1000.0),
            p99_response_ms: percentile(&data.latencies, 99).map(|v| v * 1000.0),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn print(&self) {
        println!(
            "--- {}: {} x {} objects, {} processes x {} threads ---",
            self.operation, self.num_objects, self.object_size, self.processes, self.threads
        );
        println!(
            "Successful operations: {}; failed operations: {}",
            self.succeeded, self.failed
        );
        println!(
            "Total data transferred: {}",
            pretty_bytes(self.total_bytes as f64)
        );
        println!("Total time: {:.2} seconds", self.elapsed_seconds);
        println!(
            "Throughput: {}/s",
            pretty_bytes(self.throughput_bytes_per_second)
        );
        if let (Some(avg), Some(p50), Some(p90), Some(p99)) = (
            self.average_response_ms,
            self.p50_response_ms,
            self.p90_response_ms,
            self.p99_response_ms,
        ) {
            println!("Average response time: {avg:.2} ms");
            println!(
                "Response time percentiles: 50% Median: {p50:.2} ms, 90%: {p90:.2} ms, 99%: {p99:.2} ms\n"
            );
        } else {
            println!("Response time percentiles: unavailable (no successful operations)\n");
        }
    }
}

fn percentile(sorted: &[f64], percentage: usize) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    // Nearest-rank percentiles also work for a single successful operation.
    let rank =
        (sorted.len() / 100 * percentage) + ((sorted.len() % 100 * percentage).div_ceil(100));
    Some(sorted[rank - 1])
}

fn median(sorted: &[f64]) -> Option<f64> {
    let middle = sorted.len() / 2;
    let upper = *sorted.get(middle)?;
    Some(if sorted.len().is_multiple_of(2) {
        sorted[middle - 1].midpoint(upper)
    } else {
        upper
    })
}

#[allow(clippy::cast_precision_loss)]
fn pretty_bytes(mut bytes: f64) -> String {
    let mut unit = "B";
    for candidate in ["KB", "MB", "GB", "TB", "PB"] {
        if bytes < 1024.0 {
            break;
        }
        bytes /= 1024.0;
        unit = candidate;
    }
    let number = format!("{bytes:.2}");
    format!(
        "{} {unit}",
        number.trim_end_matches('0').trim_end_matches('.')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_handle_empty_single_and_small_samples() {
        assert_eq!(percentile(&[], 99), None);
        assert_eq!(percentile(&[0.1], 99), Some(0.1));
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 50), Some(2.0));
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 90), Some(4.0));
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));
    }

    #[test]
    fn reports_count_failures_without_inventing_bytes_or_latency() {
        let data = Measurements {
            succeeded: 1,
            failed: 2,
            bytes: 1024,
            latencies: vec![0.1],
        };
        let report = Report::new(Phase::Upload, "1KB", 1, 2, 0.5, data);
        assert_eq!(report.num_objects, 3);
        assert!((report.throughput_bytes_per_second - 2048.0).abs() < f64::EPSILON);
        assert_eq!(report.average_response_ms, Some(100.0));
        assert_eq!(pretty_bytes(1024.0), "1 KB");
        assert_eq!(pretty_bytes(0.0), "0 B");
    }
}
