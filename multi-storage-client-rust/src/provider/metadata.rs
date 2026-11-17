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

//! Bounded symlink resolution shared by the public provider and client.
use super::StorageProvider;
use crate::{ErrorKind, InfoOptions, MscError, ObjectKind, ObjectMetadata, ObjectPath, Result};

pub(crate) async fn resolve_metadata<P: StorageProvider + ?Sized>(
    provider: &P,
    path: &str,
    options: InfoOptions,
) -> Result<ObjectMetadata> {
    let mut key = ObjectPath::new(path)?;
    let mut visited = std::collections::BTreeSet::new();
    let mut path = path.to_owned();
    for hop in 0..=8 {
        if !visited.insert(key.clone()) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "symlink cycle detected",
            ));
        }
        let metadata = provider.get_object_metadata(&path, options).await?;
        if metadata.kind != ObjectKind::Symlink {
            return Ok(metadata);
        }
        if hop == 8 {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "symlink hop limit exceeded",
            ));
        }
        let target = metadata
            .symlink_target
            .ok_or_else(|| MscError::new(ErrorKind::Integrity, "symlink has no target"))?;
        key = key.resolve_link(&target)?;
        path = key.to_string();
    }
    unreachable!("bounded symlink resolution")
}
