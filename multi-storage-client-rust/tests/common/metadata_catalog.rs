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

//! A writable test catalog whose staged changes remain isolated until commit.

use multi_storage_client::{
    ErrorKind, ListOptions, MetadataProvider, MetadataStream, MscError, ObjectKind, ObjectMetadata,
    ObjectPath, ResolvedPath, ResolvedPathState, Result,
};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
pub(crate) struct WritableMetadata {
    state: Mutex<WritableState>,
    failed_removal: Option<String>,
}

#[derive(Default)]
struct WritableState {
    committed: BTreeMap<String, ObjectMetadata>,
    pending: BTreeMap<String, Option<ObjectMetadata>>,
}

impl WritableState {
    fn resolve(&self, path: &str) -> Result<(ObjectPath, Option<&ObjectMetadata>)> {
        let mut path = ObjectPath::new(path)?;
        if let Some(link) = self
            .committed
            .get(path.as_str())
            .and_then(|entry| entry.symlink_target.as_deref())
        {
            path = path.resolve_link(link)?;
        }
        let entry = self.committed.get(path.as_str());
        Ok((path, entry))
    }
}

fn missing() -> MscError {
    MscError::new(ErrorKind::NotFound, "test key missing")
}

fn resolved(entry: &ObjectMetadata) -> ResolvedPath {
    ResolvedPath {
        physical_path: entry.key.clone(),
        state: ResolvedPathState::Exists,
        profile: None,
    }
}

impl WritableMetadata {
    pub(crate) fn with_entries<'a>(
        entries: impl IntoIterator<Item = (&'a str, ObjectMetadata)>,
        failed_removal: Option<&str>,
    ) -> Self {
        Self {
            state: Mutex::new(WritableState {
                committed: entries
                    .into_iter()
                    .map(|(path, entry)| (path.into(), entry))
                    .collect(),
                ..WritableState::default()
            }),
            failed_removal: failed_removal.map(str::to_owned),
        }
    }
}

#[async_trait::async_trait]
impl MetadataProvider for WritableMetadata {
    async fn list_objects(&self, _: ListOptions) -> Result<MetadataStream> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn get_object_metadata(
        &self,
        path: &str,
        include_pending: bool,
    ) -> Result<ObjectMetadata> {
        let state = self.state.lock().unwrap();
        let entry = match state.pending.get(path).filter(|_| include_pending) {
            Some(entry) => entry.as_ref(),
            None => state.committed.get(path),
        };
        let mut entry = entry.cloned().ok_or_else(missing)?;
        entry.key = ObjectPath::new(path)?;
        Ok(entry)
    }

    async fn glob(&self, _: &str, _: Option<&str>) -> Result<Vec<ObjectPath>> {
        Ok(Vec::new())
    }

    async fn realpath(&self, path: &str) -> Result<ResolvedPath> {
        let state = self.state.lock().unwrap();
        let (path, entry) = state.resolve(path)?;
        Ok(match entry {
            Some(entry) => resolved(entry),
            None => ResolvedPath {
                physical_path: path,
                state: ResolvedPathState::Untracked,
                profile: None,
            },
        })
    }

    async fn resolve_copy_source(&self, path: &str) -> Result<(ResolvedPath, ObjectMetadata)> {
        let state = self.state.lock().unwrap();
        let (_, entry) = state.resolve(path)?;
        let entry = entry.ok_or_else(missing)?;
        let mut source = entry.clone();
        source
            .attributes
            .clone_from(&state.committed[path].attributes);
        Ok((resolved(entry), source))
    }

    async fn resolve_delete_target(&self, path: &str) -> Result<Option<ResolvedPath>> {
        let state = self.state.lock().unwrap();
        let entry = state.committed.get(path).ok_or_else(missing)?;
        match entry.kind {
            ObjectKind::Directory => {
                return Err(MscError::unsupported("logical directory deletion"));
            }
            ObjectKind::Symlink => return Ok(None),
            ObjectKind::File => (),
        }
        let shared = state
            .committed
            .iter()
            .chain(
                state
                    .pending
                    .iter()
                    .filter_map(|(key, entry)| entry.as_ref().map(|entry| (key, entry))),
            )
            .any(|(key, candidate)| {
                key != path && candidate.kind == ObjectKind::File && candidate.key == entry.key
            });
        Ok((!shared).then(|| resolved(entry)))
    }

    async fn generate_physical_path(&self, path: &str, _: bool) -> Result<ResolvedPath> {
        Ok(ResolvedPath {
            physical_path: ObjectPath::new(format!("physical/{path}"))?,
            state: ResolvedPathState::Untracked,
            profile: None,
        })
    }

    async fn add_file(&self, path: &str, metadata: ObjectMetadata) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .pending
            .insert(path.into(), Some(metadata));
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        if self.failed_removal.as_deref() == Some(path) {
            return Err(MscError::new(
                ErrorKind::NotFound,
                "metadata staging target missing",
            ));
        }
        self.state.lock().unwrap().pending.insert(path.into(), None);
        Ok(())
    }

    async fn commit_updates(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        for (path, entry) in std::mem::take(&mut state.pending) {
            if let Some(entry) = entry {
                state.committed.insert(path, entry);
            } else {
                state.committed.remove(&path);
            }
        }
        Ok(())
    }

    fn is_writable(&self) -> bool {
        true
    }
}
