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

use super::StorageClient;
use crate::{
    DeleteOptions, ErrorKind, InfoOptions, MetadataProvider, MscError, ObjectKind, ObjectPath,
    ResolvedPath, ResolvedPathState, Result, StorageProvider,
};
use std::borrow::Cow;

impl StorageClient {
    pub(super) async fn read_path<'a>(&self, path: &'a str) -> Result<Cow<'a, str>> {
        let Some(metadata) = &self.metadata_provider else {
            return Ok(Cow::Borrowed(path));
        };
        let resolved = metadata.realpath(path).await?;
        if resolved.state != ResolvedPathState::Exists {
            return Err(MscError::new(
                ErrorKind::NotFound,
                "logical object does not exist",
            ));
        }
        Ok(Cow::Owned(self.physical_path(resolved)?.into_string()))
    }

    pub(super) async fn write_path<'a>(&self, path: &'a str) -> Result<Cow<'a, str>> {
        let Some(metadata) = self.writable_metadata()? else {
            return Ok(Cow::Borrowed(path));
        };
        let overwrite = metadata.realpath(path).await?.state != ResolvedPathState::Untracked;
        if overwrite && !metadata.allow_overwrites() {
            return Err(MscError::new(
                ErrorKind::AlreadyExists,
                "logical object already exists",
            ));
        }
        let resolved = metadata.generate_physical_path(path, overwrite).await?;
        Ok(Cow::Owned(self.physical_path(resolved)?.into_string()))
    }

    fn physical_path(&self, resolved: ResolvedPath) -> Result<ObjectPath> {
        if resolved.profile.is_some() && resolved.profile != self.profile {
            return Err(MscError::unsupported("routing to another storage profile"));
        }
        Ok(resolved.physical_path)
    }

    pub(super) fn writable_metadata(&self) -> Result<Option<&dyn MetadataProvider>> {
        let metadata = self.metadata_provider.as_deref();
        if metadata.is_some_and(|provider| !provider.is_writable()) {
            return Err(MscError::unsupported(
                "mutation of a read-only metadata namespace",
            ));
        }
        Ok(metadata)
    }

    pub(super) async fn register_write(
        &self,
        logical: &str,
        physical: &str,
        attributes: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Result<()> {
        if let Some(metadata) = &self.metadata_provider {
            let mut entry = self
                .provider
                .get_object_metadata(physical, InfoOptions { strict: false })
                .await
                .map_err(|_| registration_failed("object metadata lookup", physical))?;
            entry.attributes.extend(attributes);
            metadata
                .add_file(logical, entry)
                .await
                .map_err(|_| registration_failed("logical metadata staging", physical))?;
        }
        Ok(())
    }

    pub(super) async fn delete_mapped(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
        options: DeleteOptions,
    ) -> Result<()> {
        let entry = metadata.get_object_metadata(path, false).await?;
        if entry.kind == ObjectKind::Directory {
            return Err(MscError::unsupported("logical directory deletion"));
        }
        if metadata.should_use_soft_delete() || entry.kind == ObjectKind::Symlink {
            if options.if_match.is_some()
                || options.if_none_match.is_some()
                || options.version.is_some()
            {
                return Err(MscError::unsupported("conditional metadata-only deletion"));
            }
        } else {
            let physical = self.read_path(path).await?;
            match self.provider.delete_object(&physical, options).await {
                Ok(()) => (),
                Err(error) if error.kind == ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
        }
        metadata.remove_file(path).await
    }
}

fn registration_failed(stage: &str, physical: &str) -> MscError {
    MscError::new(
        ErrorKind::AmbiguousOutcome,
        format!("physical object was written before {stage} failed"),
    )
    .context("register_write", "metadata", physical)
}
