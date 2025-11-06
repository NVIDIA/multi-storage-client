// SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

use pyo3::prelude::*;

#[pyclass(get_all, set_all)]
#[derive(Clone, Debug, Default)]
pub struct ObjectMetadata {
    pub key: String,
    pub content_length: u64,
    pub last_modified: String,
    pub object_type: String,
    pub etag: Option<String>,
}

impl ObjectMetadata {
    pub fn new(
        key: String,
        content_length: u64,
        last_modified: String,
        object_type: String,
        etag: Option<String>,
    ) -> Self {
        Self {
            key,
            content_length,
            last_modified,
            object_type,
            etag,
        }
    }
}

#[pyclass(get_all, set_all)]
#[derive(Clone, Debug, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectMetadata>,
    pub prefixes: Vec<ObjectMetadata>,
}

impl ListResult {
    pub fn new(objects: Vec<ObjectMetadata>, prefixes: Vec<ObjectMetadata>) -> Self {
        Self { objects, prefixes }
    }
}
