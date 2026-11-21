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

//! Read-only metadata snapshots from Python-compatible JSON indexes and JSONL parts.
use super::{BaseStorageProvider, StorageBackend, StorageProvider, metadata::MAX_SYMLINK_HOPS};
use crate::filter::AttributeFilter;
use crate::{
    ErrorKind, ListOptions, MetadataProvider, MetadataStream, MscError, ObjectKind, ObjectMetadata,
    ObjectPath, ReadOptions, ResolvedPath, ResolvedPathState, Result, StorageClient,
};
use async_stream::try_stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use globset::GlobBuilder;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec, LinesCodecError},
    io::StreamReader,
};

const MANIFESTS: &str = ".msc_manifests";
const INDEX: &str = "msc_manifest_index.json";
const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
struct Index {
    version: String,
    format: Option<String>,
    parts: Vec<Part>,
}

#[derive(Deserialize)]
struct Part {
    path: ObjectPath,
}

#[derive(Deserialize)]
struct Row {
    key: ObjectPath,
    size_bytes: u64,
    last_modified: DateTime<Utc>,
    #[serde(rename = "type", default)]
    kind: ObjectKind,
    physical_path: Option<ObjectPath>,
    symlink_target: Option<String>,
    etag: Option<String>,
    content_type: Option<String>,
    storage_class: Option<String>,
    metadata: Option<BTreeMap<String, serde_json::Value>>,
}

struct Record {
    metadata: ObjectMetadata,
    physical_path: Option<ObjectPath>,
}

impl From<Row> for Record {
    fn from(row: Row) -> Self {
        let mut metadata = ObjectMetadata::file(row.key, row.size_bytes);
        metadata.kind = if row.symlink_target.is_some() {
            ObjectKind::Symlink
        } else {
            row.kind
        };
        metadata.last_modified = Some(row.last_modified);
        metadata.symlink_target = row.symlink_target;
        metadata.etag = row.etag;
        metadata.attributes = row.metadata.unwrap_or_default();
        for (name, value) in [
            ("content_type", row.content_type),
            ("storage_class", row.storage_class),
        ] {
            if let Some(value) = value {
                metadata.provider_fields.insert(name.into(), value);
            }
        }
        Self {
            metadata,
            physical_path: row.physical_path,
        }
    }
}

/// An immutable snapshot indexed by logical object key.
///
/// Metadata queries and listings use the in-memory index. The manifest and the
/// mapped data can live in different storage profiles.
#[derive(Clone)]
pub struct ManifestMetadataProvider {
    records: Arc<BTreeMap<ObjectPath, Record>>,
}

impl ManifestMetadataProvider {
    /// Load a concrete JSON index, a directory containing the index, or the newest
    /// snapshot below the directory's `.msc_manifests` tree. An explicit index pins
    /// the snapshot. Part paths are relative to the index's parent directory.
    ///
    /// # Errors
    /// Returns provider errors, `ManifestNotFound` on empty discovery, `Unsupported`
    /// for other formats or versions, and `Integrity` for malformed manifests.
    pub async fn load(storage: &StorageClient, path: impl AsRef<str>) -> Result<Self> {
        let storage = &storage.provider;
        let path = ObjectPath::new(path)?;
        let index = discover(storage, path).await?;
        let bytes = storage
            .get_object(index.as_str(), ReadOptions::default())
            .await?;
        let schema: Index = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(&index, &format!("invalid index at line {}", error.line())))?;
        if !matches!(schema.version.as_str(), "1" | "1.0") {
            return Err(MscError::unsupported("manifest index version"));
        }
        let mut records = BTreeMap::new();
        for part in schema.parts {
            if part.path.is_root() {
                return Err(invalid(&index, "empty manifest part path"));
            }
            if extension(&part.path)
                .or(schema.format.as_deref())
                .unwrap_or("jsonl")
                != "jsonl"
            {
                return Err(MscError::unsupported("manifest part format"));
            }
            let path = index.resolve_link(part.path.as_str())?;
            load_part(storage, &path, &mut records).await?;
        }
        Ok(Self {
            records: Arc::new(records),
        })
    }
}

async fn discover(storage: &dyn StorageProvider, path: ObjectPath) -> Result<ObjectPath> {
    if storage.is_file(path.as_str()).await? {
        if extension(&path) != Some("json") {
            return Err(MscError::unsupported("manifest index format"));
        }
        return Ok(path);
    }
    let index_name = ObjectPath::new(INDEX)?;
    let direct = path.join(&index_name)?;
    if storage.is_file(direct.as_str()).await? {
        return Ok(direct);
    }
    let snapshots = if path.as_str().split('/').any(|part| part == MANIFESTS) {
        path
    } else {
        path.join(&ObjectPath::new(MANIFESTS)?)?
    };
    let prefix = format!("{snapshots}/");
    let mut listing = storage
        .list_objects(ListOptions {
            prefix: snapshots,
            ..ListOptions::default()
        })
        .await?;
    let mut latest = None;
    while let Some(object) = listing.try_next().await? {
        if object
            .key
            .as_str()
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.split_once('/'))
            .is_some_and(|(_, name)| name == INDEX)
        {
            latest = Some(object.key);
        }
    }
    latest.ok_or_else(|| MscError::new(ErrorKind::ManifestNotFound, "no manifest index found"))
}

fn extension(path: &ObjectPath) -> Option<&str> {
    std::path::Path::new(path.as_str()).extension()?.to_str()
}

async fn load_part(
    storage: &BaseStorageProvider<dyn StorageBackend>,
    part: &ObjectPath,
    records: &mut BTreeMap<ObjectPath, Record>,
) -> Result<()> {
    let body = storage.open_manifest_part(part).await?;
    let reader = StreamReader::new(body.map_err(std::io::Error::other));
    let lines = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_RECORD_BYTES));
    let mut lines = lines.enumerate();
    while let Some((number, line)) = lines.next().await {
        let invalid_line = |message| invalid(part, &format!("{message} at line {}", number + 1));
        let line = line.map_err(|error| match error {
            LinesCodecError::Io(error) => match error.downcast::<MscError>() {
                Ok(error) => error,
                Err(_) => invalid_line("invalid UTF-8"),
            },
            LinesCodecError::MaxLineLengthExceeded => invalid_line("record exceeds 1 MiB"),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let row: Row = serde_json::from_str(&line).map_err(|_| invalid_line("invalid record"))?;
        if row.key.is_root() || row.physical_path.as_ref().is_some_and(ObjectPath::is_root) {
            return Err(invalid_line("empty object key"));
        }
        if records.insert(row.key.clone(), row.into()).is_some() {
            return Err(invalid_line("duplicate key"));
        }
    }
    Ok(())
}

fn invalid(path: &ObjectPath, message: &str) -> MscError {
    MscError::new(ErrorKind::Integrity, message).context("load_manifest", "manifest", path.as_str())
}

impl ManifestMetadataProvider {
    fn directory_times<'a>(
        &'a self,
        prefix: &str,
        options: &ListOptions,
        filter: Option<&AttributeFilter>,
    ) -> BTreeMap<&'a str, Option<DateTime<Utc>>> {
        let mut directories = BTreeMap::new();
        for (key, record) in self.records.range::<str, _>((Included(prefix), Unbounded)) {
            let Some(relative) = key.as_str().strip_prefix(prefix) else {
                break;
            };
            let metadata = &record.metadata;
            if !matches(metadata, filter) {
                continue;
            }
            let parents = relative
                .match_indices('/')
                .map(|(index, _)| prefix.len() + index);
            let own_key = (metadata.kind == ObjectKind::Directory).then_some(key.as_str().len());
            let limit = if options.recursive { usize::MAX } else { 1 };
            for end in parents.chain(own_key).take(limit) {
                let key = &key.as_str()[..end];
                let modified = directories.entry(key).or_insert(metadata.last_modified);
                *modified = (*modified).max(metadata.last_modified);
            }
        }
        directories
    }
}

fn matches(metadata: &ObjectMetadata, filter: Option<&AttributeFilter>) -> bool {
    filter.is_none_or(|filter| filter.matches(&metadata.attributes))
}

fn directory_prefix(key: &ObjectPath) -> String {
    if key.is_root() {
        String::new()
    } else {
        format!("{key}/")
    }
}

#[async_trait]
impl MetadataProvider for ManifestMetadataProvider {
    async fn list_objects(&self, options: ListOptions) -> Result<MetadataStream> {
        options.validate()?;
        let filter = AttributeFilter::compile(options.attribute_filter_expression.as_deref())?;
        let snapshot = self.clone();
        Ok(Box::pin(try_stream! {
            if let Some(record) = snapshot.records.get(&options.prefix)
                && record.metadata.kind != ObjectKind::Directory
            {
                if options.contains(&options.prefix) && matches(&record.metadata, filter.as_ref()) {
                    yield record.metadata.clone();
                }
                return;
            }

            let prefix = directory_prefix(&options.prefix);
            if options.include_directories {
                for (key, modified) in snapshot.directory_times(&prefix, &options, filter.as_ref()) {
                    let key = ObjectPath::new(key)?;
                    if !options.contains(&key) {
                        continue;
                    }
                    if let Some(record) = snapshot.records.get(&key)
                        && record.metadata.kind == ObjectKind::Directory
                        && matches(&record.metadata, filter.as_ref())
                    {
                        yield record.metadata.clone();
                    } else {
                        let mut directory = ObjectMetadata::directory(key);
                        directory.last_modified = modified;
                        yield directory;
                    }
                }
            }
            let start = match options.start_after.as_ref() {
                Some(start) if start.as_str() >= prefix.as_str() => Excluded(start.as_str()),
                _ => Included(prefix.as_str()),
            };
            for (key, record) in snapshot.records.range::<str, _>((start, Unbounded)) {
                let Some(relative) = key.as_str().strip_prefix(&prefix) else { break };
                if options.end_at.as_ref().is_some_and(|end| key > end) {
                    break;
                }
                if record.metadata.kind != ObjectKind::Directory
                    && (options.recursive || !relative.contains('/'))
                    && matches(&record.metadata, filter.as_ref())
                {
                    yield record.metadata.clone();
                }
            }
        }))
    }

    async fn get_object_metadata(&self, path: &str, _: bool) -> Result<ObjectMetadata> {
        let key = ObjectPath::new(if path == "/" { "" } else { path })?;
        let record = self.records.get(&key);
        if let Some(record) = record
            && (!path.ends_with('/') || record.metadata.kind == ObjectKind::Directory)
        {
            return Ok(record.metadata.clone());
        }
        let prefix = directory_prefix(&key);
        if self
            .records
            .range::<str, _>((Included(prefix.as_str()), Unbounded))
            .next()
            .is_some_and(|(candidate, _)| candidate.as_str().starts_with(&prefix))
        {
            return Ok(ObjectMetadata::directory(key));
        }
        Err(MscError::new(
            ErrorKind::NotFound,
            "logical object does not exist",
        ))
    }

    async fn glob(
        &self,
        pattern: &str,
        attribute_filter_expression: Option<&str>,
    ) -> Result<Vec<ObjectPath>> {
        let matcher = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|_| MscError::new(ErrorKind::InvalidPath, "invalid glob pattern"))?
            .compile_matcher();
        let filter = AttributeFilter::compile(attribute_filter_expression)?;
        Ok(self
            .records
            .iter()
            .filter(|(key, record)| {
                record.metadata.kind != ObjectKind::Directory
                    && matcher.is_match(key.as_str())
                    && matches(&record.metadata, filter.as_ref())
            })
            .map(|(key, _)| key.clone())
            .collect())
    }

    async fn realpath(&self, logical_path: &str) -> Result<ResolvedPath> {
        let mut key = ObjectPath::new(logical_path)?;
        let mut visited = Vec::new();
        for hop in 0..=MAX_SYMLINK_HOPS {
            let Some(record) = self.records.get(&key) else {
                return Ok(ResolvedPath {
                    physical_path: key,
                    state: ResolvedPathState::Untracked,
                    profile: None,
                });
            };
            if record.metadata.kind != ObjectKind::Symlink {
                return Ok(ResolvedPath {
                    physical_path: record.physical_path.clone().unwrap_or(key),
                    state: ResolvedPathState::Exists,
                    profile: None,
                });
            }
            if visited.contains(&key) {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "manifest symlink cycle detected",
                ));
            }
            if hop == MAX_SYMLINK_HOPS {
                break;
            }
            let target = record.metadata.symlink_target.as_deref().ok_or_else(|| {
                MscError::new(ErrorKind::Integrity, "manifest symlink has no target")
            })?;
            let next = key.resolve_link(target)?;
            visited.push(key);
            key = next;
        }
        Err(MscError::new(
            ErrorKind::Integrity,
            "manifest symlink hop limit exceeded",
        ))
    }

    async fn generate_physical_path(&self, logical_path: &str, _: bool) -> Result<ResolvedPath> {
        Ok(ResolvedPath {
            physical_path: ObjectPath::new(logical_path)?,
            state: ResolvedPathState::Untracked,
            profile: None,
        })
    }
}
