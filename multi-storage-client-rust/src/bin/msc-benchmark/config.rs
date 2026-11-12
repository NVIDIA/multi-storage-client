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

use serde::Deserialize;
use std::{collections::BTreeMap, path::Path};

use super::Result;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub processes: Vec<usize>,
    pub threads: Vec<usize>,
    pub test_object_sizes: BTreeMap<String, usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            processes: vec![8],
            threads: vec![4],
            test_object_sizes: BTreeMap::from([("4MB".into(), 12_800), ("64MB".into(), 800)]),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let config: Self = if let Some(path) = path {
            serde_json::from_slice(&std::fs::read(path)?)?
        } else {
            Self::default()
        };
        for (name, values) in [
            ("processes", &config.processes),
            ("threads", &config.threads),
        ] {
            if values.is_empty() || values.contains(&0) {
                return Err(format!("{name} must be a nonempty array of positive integers").into());
            }
        }
        if config.test_object_sizes.is_empty() {
            return Err("test_object_sizes must contain at least one size".into());
        }
        for (size, count) in &config.test_object_sizes {
            let bytes = size_to_bytes(size)?;
            if *count == 0 || bytes.checked_mul(u64::try_from(*count)?).is_none() {
                return Err(format!("invalid object count or total byte size for {size}").into());
            }
            usize::try_from(bytes)?;
        }
        Ok(config)
    }
}

pub(super) fn size_to_bytes(size: &str) -> Result<u64> {
    let split = size
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(size.len());
    let (number, unit) = size.split_at(split);
    let exponent = match unit {
        "KB" => 1,
        "MB" => 2,
        "GB" => 3,
        "TB" => 4,
        "PB" => 5,
        _ => return Err(format!("invalid object size {size:?}; expected e.g. 1KB or 64MB").into()),
    };
    number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(1024_u64.pow(exponent)))
        .ok_or_else(|| format!("invalid or overflowing object size {size:?}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_size_units_are_binary_and_invalid_sizes_are_rejected() {
        for (size, bytes) in [("1KB", 1024), ("4MB", 4_194_304), ("1PB", 1 << 50)] {
            assert_eq!(size_to_bytes(size).unwrap(), bytes);
        }
        for size in [
            "",
            "0MB",
            "MB",
            "1mb",
            "1.5MB",
            "-1KB",
            "1B",
            "18446744073709551615PB",
        ] {
            assert!(size_to_bytes(size).is_err(), "{size}");
        }
    }
}
