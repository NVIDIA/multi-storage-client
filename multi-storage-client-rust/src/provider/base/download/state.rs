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

//! Persistent chunk checkpoints adapted from the Python accelerator's download module.
use crate::{ErrorKind, MscError, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions, TryLockError},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const FORMAT: &str = "msc-download-v1";
const SUFFIX: &str = ".msc-dl";
const TEMP_ID_LENGTH: usize = 6;
const MAX_MARKER_SIZE: u64 = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct DownloadState {
    pub format: String,
    pub key: String,
    pub size: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub etag: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub version: Option<String>,
    pub chunk_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
}

impl DownloadState {
    pub(super) fn new(
        key: String,
        size: u64,
        etag: Option<String>,
        version: Option<String>,
        chunk_size: u64,
        backend: String,
    ) -> Self {
        Self {
            format: FORMAT.into(),
            key,
            size,
            etag,
            version,
            chunk_size,
            backend: Some(backend),
        }
    }

    fn same_object(&self, other: &Self) -> bool {
        self.key == other.key
            && self.size == other.size
            && self.etag == other.etag
            && self.version == other.version
            && self
                .backend
                .as_ref()
                .zip(other.backend.as_ref())
                .is_none_or(|(a, b)| a == b)
    }

    pub(super) fn length(&self, index: u64) -> u64 {
        self.chunk_size.min(self.size - index * self.chunk_size)
    }

    pub(super) fn count(&self) -> u64 {
        self.size.div_ceil(self.chunk_size)
    }
}

pub(super) struct Directory {
    pub path: PathBuf,
    pub state: DownloadState,
    lock: Option<same_file::Handle>,
    temporary: Option<tempfile::TempDir>,
}

impl Directory {
    pub(super) fn open(target: &Path, state: DownloadState) -> Result<Arc<Self>> {
        let parent = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut name = std::ffi::OsString::from(".");
        name.push(
            target
                .file_name()
                .ok_or_else(|| invalid_state("download destination requires a file name"))?,
        );
        name.push(SUFFIX);
        let path = parent.join(name);
        let (state, lock) = prepare_state(&path, state)?;
        if let Some(lock) = lock {
            return Ok(Arc::new(Self {
                path,
                state,
                lock: Some(lock),
                temporary: None,
            }));
        }
        let temporary = tempfile::Builder::new()
            .prefix(".msc-dl-")
            .suffix(SUFFIX)
            .tempdir_in(parent)?;
        Ok(Arc::new(Self {
            path: temporary.path().to_owned(),
            state,
            lock: None,
            temporary: Some(temporary),
        }))
    }

    fn verify_lock(&self) -> Result<()> {
        if let Some(lock) = &self.lock
            && (!fs::symlink_metadata(&self.path)?.is_dir()
                || !fs::symlink_metadata(self.path.join("lock"))?.is_file()
                || same_file::Handle::from_path(self.path.join("lock"))? != *lock)
        {
            return Err(invalid_state("download checkpoint lock entry was replaced"));
        }
        Ok(())
    }

    pub(super) fn has_chunk(&self, index: u64) -> Result<bool> {
        self.verify_lock()?;
        match fs::symlink_metadata(self.path.join(chunk_name(index))) {
            Ok(m) => Ok(m.is_file() && m.len() == self.state.length(index)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) fn merge(self: &Arc<Self>) -> Result<Merged> {
        self.verify_lock()?;
        let mut file = temporary(&self.path, "merged.")?;
        let mut length = 0_u64;
        for index in 0..self.state.count() {
            if !self.has_chunk(index)? {
                return Err(invalid_state(
                    "download chunk is missing or has changed length",
                ));
            }
            let input = fs::File::open(self.path.join(chunk_name(index)))?;
            let expected = self.state.length(index);
            let mut input = std::io::BufReader::with_capacity(
                1024 * 1024,
                input.take(expected.saturating_add(1)),
            );
            let count = std::io::copy(&mut input, file.as_file_mut())?;
            if count != expected {
                return Err(invalid_state(
                    "download chunk length differs from its range",
                ));
            }
            length += count;
        }
        if length != self.state.size {
            return Err(invalid_state(
                "merged download length differs from metadata",
            ));
        }
        file.as_file().sync_all()?;
        Ok(Merged {
            file,
            directory: self.clone(),
        })
    }

    pub(super) fn cleanup(&self) -> Result<()> {
        self.verify_lock()?;
        if let Some(temporary) = &self.temporary {
            fs::remove_dir_all(temporary.path())?;
        } else {
            remove_state(&self.path)?;
        }
        Ok(())
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if let Some(lock) = &self.lock {
            let _ = lock.as_file().unlock();
        }
    }
}

pub(super) struct Chunk {
    file: Mutex<Option<tempfile::NamedTempFile>>,
    directory: Arc<Directory>,
    index: u64,
}

impl Chunk {
    pub(super) fn create(directory: Arc<Directory>, index: u64) -> Result<Arc<Self>> {
        directory.verify_lock()?;
        let file = temporary(&directory.path, &format!("{}.", chunk_name(index)))?;
        Ok(Arc::new(Self {
            file: Mutex::new(Some(file)),
            directory,
            index,
        }))
    }

    pub(super) fn write(&self, bytes: &[u8]) -> Result<()> {
        let mut guard = self
            .file
            .lock()
            .map_err(|_| invalid_state("download chunk lock poisoned"))?;
        guard
            .as_mut()
            .ok_or_else(|| invalid_state("download chunk already published"))?
            .write_all(bytes)?;
        Ok(())
    }

    pub(super) fn publish(&self) -> Result<()> {
        self.directory.verify_lock()?;
        let mut guard = self
            .file
            .lock()
            .map_err(|_| invalid_state("download chunk lock poisoned"))?;
        let file = guard
            .take()
            .ok_or_else(|| invalid_state("download chunk already published"))?;
        if file.as_file().metadata()?.len() != self.directory.state.length(self.index) {
            return Err(invalid_state(
                "download chunk length differs from its range",
            ));
        }
        file.as_file().sync_all()?;
        verify_temporary(&file)?;
        file.persist(self.directory.path.join(chunk_name(self.index)))
            .map_err(|e| MscError::from(e.error))?;
        Ok(())
    }
}

pub(super) struct Merged {
    file: tempfile::NamedTempFile,
    pub directory: Arc<Directory>,
}

impl Merged {
    pub(super) fn publish(self, target: &Path) -> Result<Arc<Directory>> {
        self.directory.verify_lock()?;
        verify_temporary(&self.file)?;
        self.file
            .persist(target)
            .map_err(|e| MscError::from(e.error))?;
        Ok(self.directory)
    }
}

fn verify_temporary(file: &tempfile::NamedTempFile) -> Result<()> {
    if !fs::symlink_metadata(file.path())?.is_file()
        || same_file::Handle::from_file(file.as_file().try_clone()?)?
            != same_file::Handle::from_path(file.path())?
    {
        return Err(invalid_state("download staging entry was replaced"));
    }
    Ok(())
}

fn temporary(dir: &Path, prefix: &str) -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(prefix)
        .rand_bytes(TEMP_ID_LENGTH)
        .suffix(".tmp")
        .tempfile_in(dir)
}

fn chunk_name(index: u64) -> String {
    format!("{index:06}.chunk")
}

fn invalid_state(message: &str) -> MscError {
    MscError::new(ErrorKind::Integrity, message)
}

fn read_state(dir: &Path) -> Result<Option<DownloadState>> {
    match fs::symlink_metadata(dir) {
        Ok(m) if !m.is_dir() => {
            return Err(invalid_state(
                "download state directory conflicts with an existing path",
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let path = dir.join("state.json");
    match fs::symlink_metadata(&path) {
        Ok(m) if !m.is_file() => {
            return Err(invalid_state("download state marker is not a regular file"));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut encoded = Vec::new();
    file.take(MAX_MARKER_SIZE + 1).read_to_end(&mut encoded)?;
    if encoded.len() as u64 > MAX_MARKER_SIZE {
        return Err(invalid_state(
            "download state marker exceeds its size limit",
        ));
    }
    let state: DownloadState = serde_json::from_slice(&encoded)
        .map_err(|_| invalid_state("invalid download state marker"))?;
    if state.format != FORMAT || state.chunk_size == 0 {
        return Err(invalid_state("unrecognized download state marker"));
    }
    Ok(Some(state))
}

fn try_lock_state(dir: &Path) -> Result<Option<same_file::Handle>> {
    let path = dir.join("lock");
    match fs::symlink_metadata(&path) {
        Ok(m) if !m.is_file() => return Err(invalid_state("download lock is not a regular file")),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(e)) => return Err(e.into()),
    }
    let lock = same_file::Handle::from_file(file)?;
    if !fs::symlink_metadata(&path).is_ok_and(|m| m.is_file()) {
        return Ok(None);
    }
    match same_file::Handle::from_path(&path) {
        Ok(current) if current == lock => Ok(Some(lock)),
        Ok(_) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn prepare_state(
    dir: &Path,
    state: DownloadState,
) -> Result<(DownloadState, Option<same_file::Handle>)> {
    let mut builder = fs::DirBuilder::new();

    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            read_state(dir)?;
        }
        Err(e) => return Err(e.into()),
    }
    let Some(lock) = try_lock_state(dir)? else {
        return Ok((state, None));
    };
    if let Some(saved) = read_state(dir)?
        && saved.same_object(&state)
    {
        clear_download_files(dir, false)?;
        return Ok((saved, Some(lock)));
    }
    clear_download_files(dir, true)?;
    write_state(
        dir,
        &serde_json::to_vec(&state)
            .map_err(|_| invalid_state("unable to encode download state"))?,
    )?;
    Ok((state, Some(lock)))
}

fn write_state(dir: &Path, encoded: &[u8]) -> std::io::Result<()> {
    let mut file = temporary(dir, "state.json.")?;
    file.write_all(encoded)?;
    file.as_file().sync_all()?;
    file.persist(dir.join("state.json")).map_err(|e| e.error)?;
    Ok(())
}

fn clear_download_files(dir: &Path, completed: bool) -> std::io::Result<()> {
    let chunk = |name: &str| {
        name.strip_suffix(".chunk")
            .is_some_and(|index| index.len() >= 6 && index.bytes().all(|b| b.is_ascii_digit()))
    };
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let temporary = name
            .strip_suffix(".tmp")
            .and_then(|n| n.rsplit_once('.'))
            .is_some_and(|(prefix, id)| {
                (prefix == "merged" || prefix == "state.json" || chunk(prefix))
                    && id.len() == TEMP_ID_LENGTH
                    && id.bytes().all(|b| b.is_ascii_alphanumeric())
            });
        if (temporary || completed && chunk(name)) && entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn remove_state(dir: &Path) -> std::io::Result<()> {
    let encoded = fs::read(dir.join("state.json"))?;
    clear_download_files(dir, true)?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() != "lock" && entry.file_name() != "state.json" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::DirectoryNotEmpty,
                "unrelated checkpoint contents preserved",
            ));
        }
    }
    if !fs::symlink_metadata(dir.join("lock"))?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "download lock entry changed",
        ));
    }
    fs::remove_file(dir.join("state.json"))?;
    let cleanup = fs::remove_file(dir.join("lock")).and_then(|()| fs::remove_dir(dir));
    if cleanup.is_err() {
        // Reacquire the current lock before restoring state after unlinking the old one.
        if let Ok(Some(_lock)) = try_lock_state(dir)
            && matches!(read_state(dir), Ok(None))
        {
            let _ = write_state(dir, &encoded);
        }
    }
    cleanup
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> DownloadState {
        DownloadState::new(
            "model".into(),
            8,
            Some("revision-1".into()),
            None,
            4,
            "backend".into(),
        )
    }

    fn put(directory: &Arc<Directory>, index: u64, bytes: &[u8]) {
        let chunk = Chunk::create(directory.clone(), index).unwrap();
        chunk.write(bytes).unwrap();
        chunk.publish().unwrap();
    }

    #[test]
    fn saved_partition_and_complete_chunks_are_reused() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let directory = Directory::open(&target, state()).unwrap();
        put(&directory, 0, b"data");
        drop(directory);
        let directory = Directory::open(
            &target,
            DownloadState {
                chunk_size: 8,
                ..state()
            },
        )
        .unwrap();
        assert_eq!(directory.state.chunk_size, 4);
        assert!(directory.has_chunk(0).unwrap());
        assert!(!directory.has_chunk(1).unwrap());
        fs::write(directory.path.join("000001.chunk"), b"partial").unwrap();
        assert!(!directory.has_chunk(1).unwrap());
        put(&directory, 1, b"rest");
        let directory = directory.merge().unwrap().publish(&target).unwrap();
        directory.cleanup().unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"datarest");
        assert!(!directory.path.exists());
    }

    #[test]
    fn changed_source_discards_only_recognized_progress() {
        for change in 0..5 {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("model");
            let directory = Directory::open(&target, state()).unwrap();
            put(&directory, 0, b"data");
            fs::write(directory.path.join("keep.txt"), b"unrelated").unwrap();
            // A concurrent child process may inherit the descriptor until exec.
            let inherited = directory
                .lock
                .as_ref()
                .unwrap()
                .as_file()
                .try_clone()
                .unwrap();
            let path = directory.path.clone();
            drop(directory);
            let mut changed = state();
            match change {
                0 => changed.key = "different".into(),
                1 => changed.etag = Some("revision-2".into()),
                2 => changed.size = 9,
                3 => changed.version = Some("historical".into()),
                _ => changed.backend = Some("other".into()),
            }
            let directory = Directory::open(&target, changed).unwrap();
            assert_eq!(directory.path, path);
            assert!(!directory.has_chunk(0).unwrap());
            assert_eq!(
                fs::read(directory.path.join("keep.txt")).unwrap(),
                b"unrelated"
            );
            drop(inherited);
        }
    }

    #[test]
    fn missing_marker_and_interrupted_temporary_files_recover() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let directory = Directory::open(&target, state()).unwrap();
        put(&directory, 0, b"data");
        let path = directory.path.clone();
        drop(directory);
        fs::remove_file(path.join("state.json")).unwrap();
        let directory = Directory::open(&target, state()).unwrap();
        assert!(!directory.has_chunk(0).unwrap());
        put(&directory, 0, b"data");
        fs::write(path.join("000001.chunk.a1b2c3.tmp"), b"partial").unwrap();
        fs::write(path.join("000001.chunk.unsafe-id.tmp"), b"unrelated").unwrap();
        drop(directory);
        let directory = Directory::open(&target, state()).unwrap();
        assert!(directory.has_chunk(0).unwrap());
        assert!(!path.join("000001.chunk.a1b2c3.tmp").exists());
        assert!(path.join("000001.chunk.unsafe-id.tmp").exists());
    }

    #[test]
    fn incomplete_invalid_and_conflicting_markers_preserve_contents() {
        for field in ["etag", "version", "format", "size", "chunk_size"] {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("model");
            let directory = Directory::open(&target, state()).unwrap();
            put(&directory, 0, b"data");
            let path = directory.path.clone();
            drop(directory);
            let mut marker = serde_json::to_value(state()).unwrap();
            marker.as_object_mut().unwrap().remove(field);
            let bytes = serde_json::to_vec(&marker).unwrap();
            fs::write(path.join("state.json"), &bytes).unwrap();
            assert!(Directory::open(&target, state()).is_err());
            assert_eq!(fs::read(path.join("state.json")).unwrap(), bytes);
            assert_eq!(fs::read(path.join("000000.chunk")).unwrap(), b"data");
        }
    }

    #[test]
    fn contention_uses_independent_staging_and_keeps_original_progress() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let first = Directory::open(&target, state()).unwrap();
        put(&first, 0, b"data");
        let second = Directory::open(&target, state()).unwrap();
        assert!(second.temporary.is_some());
        assert_ne!(first.path, second.path);
        put(&second, 0, b"data");
        put(&second, 1, b"rest");
        let second = second.merge().unwrap().publish(&target).unwrap();
        second.cleanup().unwrap();
        assert!(first.has_chunk(0).unwrap());
        assert!(read_state(&first.path).unwrap().is_some());
    }

    #[test]
    fn merge_and_publication_failure_keep_chunks_and_destination() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        fs::write(&target, b"original").unwrap();
        let directory = Directory::open(&target, state()).unwrap();
        put(&directory, 0, b"data");
        assert!(directory.merge().is_err());
        assert_eq!(fs::read(&target).unwrap(), b"original");
        put(&directory, 1, b"rest");
        let invalid = parent.path().join("occupied");
        fs::create_dir(&invalid).unwrap();
        assert!(directory.merge().unwrap().publish(&invalid).is_err());
        assert!(directory.has_chunk(0).unwrap());
        assert!(directory.has_chunk(1).unwrap());
    }

    #[test]
    fn cleanup_preserves_unrelated_entries_and_valid_marker() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let directory = Directory::open(&target, state()).unwrap();
        put(&directory, 0, b"data");
        fs::write(directory.path.join("keep.txt"), b"unrelated").unwrap();
        assert!(directory.cleanup().is_err());
        assert!(read_state(&directory.path).unwrap().is_some());
        assert_eq!(
            fs::read(directory.path.join("keep.txt")).unwrap(),
            b"unrelated"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_directories_markers_and_chunks_are_not_followed() {
        use std::os::unix::fs::symlink;
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let outside = parent.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let path = parent.path().join(".model.msc-dl");
        symlink(&outside, &path).unwrap();
        assert!(Directory::open(&target, state()).is_err());
        fs::remove_file(&path).unwrap();
        let directory = Directory::open(&target, state()).unwrap();
        drop(directory);
        fs::write(outside.join("marker"), b"keep").unwrap();
        fs::remove_file(path.join("state.json")).unwrap();
        symlink(outside.join("marker"), path.join("state.json")).unwrap();
        assert!(Directory::open(&target, state()).is_err());
        assert_eq!(fs::read(outside.join("marker")).unwrap(), b"keep");
        fs::remove_file(path.join("state.json")).unwrap();
        let directory = Directory::open(&target, state()).unwrap();
        symlink(outside.join("marker"), path.join("000000.chunk")).unwrap();
        assert!(!directory.has_chunk(0).unwrap());
        put(&directory, 0, b"data");
        assert!(
            fs::symlink_metadata(path.join("000000.chunk"))
                .unwrap()
                .is_file()
        );
        assert_eq!(fs::read(outside.join("marker")).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn replaced_lock_and_temporary_entry_cannot_publish() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("model");
        let directory = Directory::open(&target, state()).unwrap();
        let chunk = Chunk::create(directory.clone(), 0).unwrap();
        chunk.write(b"data").unwrap();
        {
            let guard = chunk.file.lock().unwrap();
            let path = guard.as_ref().unwrap().path();
            fs::remove_file(path).unwrap();
            fs::write(path, b"evil").unwrap();
        }
        assert_eq!(chunk.publish().unwrap_err().kind, ErrorKind::Integrity);
        fs::rename(directory.path.join("lock"), directory.path.join("old-lock")).unwrap();
        fs::write(directory.path.join("lock"), b"new-lock").unwrap();
        assert!(directory.has_chunk(0).is_err());
    }

    #[test]
    fn cancelled_merge_retains_lock_until_blocking_work_finishes() {
        use std::{
            future::Future,
            task::{Context, Waker},
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("model");
            fs::write(&target, b"original").unwrap();
            let directory = Directory::open(&target, state()).unwrap();
            put(&directory, 0, b"data");
            put(&directory, 1, b"rest");
            let path = directory.path.clone();
            let (release, receiver) = std::sync::mpsc::channel();
            let (ready, started) = tokio::sync::oneshot::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                ready.send(()).unwrap();
                receiver.recv().unwrap();
            });
            started.await.unwrap();
            let mut merge = Box::pin(super::super::blocking(move || directory.merge()));
            assert!(
                merge
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(merge);
            assert!(try_lock_state(&path).unwrap().is_none());
            release.send(()).unwrap();
            occupied.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while try_lock_state(&path).unwrap().is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(fs::read(&target).unwrap(), b"original");
            assert!(path.join("000000.chunk").exists());
        });
    }
}
