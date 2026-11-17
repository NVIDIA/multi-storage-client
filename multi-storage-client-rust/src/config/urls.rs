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

use std::collections::BTreeMap;
use std::path::Path;

use super::schema::ProfileConfig;
use super::validation::pointer;
use super::{ConfigError, ResolvedConfig};

/// Separates routing from provider-relative keys, including implicit profiles.
#[derive(Clone, Debug)]
pub struct ResolvedUrl {
    /// Selected profile name; implicit cloud URLs may synthesize a new name.
    pub profile: String,
    /// Validated key relative to the selected profile's base path.
    pub key: crate::ObjectPath,
    /// Effective profile settings. For an implicit URL, insert this into the
    /// configuration's profile map before building the named client.
    pub profile_config: ProfileConfig,
}

impl ResolvedConfig {
    /// Resolves a user URL or absolute POSIX path without constructing a provider.
    ///
    /// # Errors
    /// Returns an error for missing profiles, unsupported schemes, relative paths,
    /// malformed encodings, or traversal segments.
    pub fn resolve(&self, input: &str) -> Result<ResolvedUrl, ConfigError> {
        let file_path = file_url_path(input)?;
        let input = file_path.as_deref().unwrap_or(input);
        if input.starts_with('/') {
            checked_key(input, false)?;
        } else if let Some((scheme, rest)) = input.split_once("://")
            && let Some((_, key)) = rest.split_once('/')
        {
            checked_key(&cloud_key(scheme, key), true)?;
        }
        let mut mapping: Option<(String, &String)> = None;
        for (source, target) in &self.path_mapping {
            let source = file_url_path(source)
                .ok()
                .flatten()
                .unwrap_or_else(|| source.clone());
            if (input.starts_with(&source) || input == source.trim_end_matches('/'))
                && mapping
                    .as_ref()
                    .is_none_or(|(previous, _)| source.len() > previous.len())
            {
                mapping = Some((source, target));
            }
        }
        if let Some((source, target)) = mapping {
            let suffix = input.strip_prefix(&source).unwrap_or("");
            let suffix = if source.starts_with('/') {
                suffix
                    .replace('%', "%25")
                    .replace('?', "%3F")
                    .replace('#', "%23")
            } else {
                cloud_key(
                    input.split_once("://").map_or("", |(scheme, _)| scheme),
                    suffix,
                )
            };
            return self.resolve_msc(&format!("{target}{suffix}"));
        }
        if input.starts_with('/') {
            return Ok(Self::resolved(
                "__filesystem__",
                checked_key(input, false)?,
                self.profile("__filesystem__")?.clone(),
            ));
        }
        let (scheme, rest) = input.split_once("://").ok_or_else(|| {
            ConfigError::new(
                "/url",
                "relative paths require an explicit absolute base directory",
            )
        })?;
        match scheme {
            "msc" => self.resolve_msc(input),
            "s3" => {
                let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
                validate_authority(bucket)?;
                let profile = format!("_{scheme}-{bucket}");
                let provider = "s3";
                Ok(Self::resolved(
                    &profile,
                    checked_key(&cloud_key(scheme, key), true)?,
                    ProfileConfig::implicit(provider, bucket),
                ))
            }
            _ => Err(ConfigError::new(
                "/url/scheme",
                "unsupported URL scheme; use msc, file, or s3",
            )),
        }
    }

    /// Resolves a relative path against an explicitly supplied absolute directory.
    ///
    /// # Errors
    /// Returns a diagnostic for a relative base, invalid UTF-8, or invalid routing.
    pub fn resolve_with_base(&self, input: &str, base: &Path) -> Result<ResolvedUrl, ConfigError> {
        if input.starts_with('/') || input.contains("://") {
            return self.resolve(input);
        }
        if !base.is_absolute() {
            return Err(ConfigError::new(
                "/url/base",
                "base directory must be absolute",
            ));
        }
        let path = base
            .join(input)
            .components()
            .filter(|part| !matches!(part, std::path::Component::CurDir))
            .collect::<std::path::PathBuf>();
        self.resolve(
            path.to_str()
                .ok_or_else(|| ConfigError::new("/url/key", "object paths must be UTF-8"))?,
        )
    }

    fn resolve_msc(&self, input: &str) -> Result<ResolvedUrl, ConfigError> {
        let rest = input
            .strip_prefix("msc://")
            .ok_or_else(|| ConfigError::new("/url", "path mapping target must be an MSC URL"))?;
        let (profile, key) = rest.split_once('/').unwrap_or((rest, ""));
        validate_authority(profile)?;
        Ok(Self::resolved(
            profile,
            checked_key(key, true)?,
            self.profile(profile)?.clone(),
        ))
    }

    fn resolved(
        profile: &str,
        key: crate::ObjectPath,
        profile_config: ProfileConfig,
    ) -> ResolvedUrl {
        ResolvedUrl {
            profile: profile.into(),
            key,
            profile_config,
        }
    }
}

fn cloud_key(scheme: &str, key: &str) -> String {
    if matches!(scheme, "s3" | "gs") {
        key.replace('?', "%3F").replace('#', "%23")
    } else {
        key.to_owned()
    }
}

fn validate_authority(authority: &str) -> Result<(), ConfigError> {
    if authority.is_empty()
        || authority.chars().any(|character| {
            character.is_whitespace()
                || character.is_control()
                || matches!(character, '@' | ':' | '?' | '#' | '%' | '\\')
        })
    {
        return Err(ConfigError::new(
            "/url/profile",
            "profile or bucket authority is missing or invalid",
        ));
    }
    Ok(())
}

fn file_url_path(input: &str) -> Result<Option<String>, ConfigError> {
    let Some(rest) = input.strip_prefix("file://") else {
        return Ok(None);
    };
    let (host, key) = rest.split_once('/').unwrap_or((rest, ""));
    if !matches!(host, "" | "localhost") {
        return Err(ConfigError::new(
            "/url/profile",
            "file URLs require an empty authority or localhost",
        ));
    }
    checked_key(key, true)?;
    Ok(Some(format!("/{}", percent_decode(key)?)))
}

fn checked_key(raw: &str, is_url: bool) -> Result<crate::ObjectPath, ConfigError> {
    if is_url && (raw.contains('?') || raw.contains('#')) {
        return Err(ConfigError::new(
            "/url/key",
            "URL query strings and fragments are unsupported; percent-encode literal key characters",
        ));
    }
    // Do not call Url::parse here: it removes dot segments before validation.
    let decoded = if is_url {
        percent_decode(raw)?
    } else {
        raw.to_owned()
    };
    if decoded.split('/').any(|part| matches!(part, "." | "..")) {
        return Err(ConfigError::new(
            "/url/key",
            "key contains a literal or encoded traversal segment",
        ));
    }
    crate::ObjectPath::new(decoded.trim_start_matches('/'))
        .map_err(|_| ConfigError::new("/url/key", "invalid provider-relative object key"))
}

fn percent_decode(raw: &str) -> Result<String, ConfigError> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut bytes = raw.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().and_then(hex);
            let low = bytes.next().and_then(hex);
            decoded.push(
                high.zip(low)
                    .map(|(high, low)| high * 16 + low)
                    .ok_or_else(|| ConfigError::new("/url/key", "malformed percent encoding"))?,
            );
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| ConfigError::new("/url/key", "percent-encoded key is not valid UTF-8"))
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn validate_mappings(
    mappings: &BTreeMap<String, String>,
    profiles: &BTreeMap<String, ProfileConfig>,
) -> Result<(), ConfigError> {
    for (source, target) in mappings {
        if let Ok(url) = url::Url::parse(source)
            && (!url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some())
        {
            return Err(ConfigError::new(
                "/path_mapping",
                "mapping URL prefixes cannot contain authentication, queries, or fragments",
            ));
        }
        let path = format!("/path_mapping/{}", pointer(source));
        if !source.ends_with('/')
            || !(source.starts_with('/')
                || source
                    .split_once("://")
                    .is_some_and(|(scheme, rest)| !scheme.is_empty() && !rest.is_empty()))
        {
            return Err(ConfigError::new(
                &path,
                "mapping source must be an absolute path or URL prefix ending in /",
            ));
        }
        let profile = target
            .strip_prefix("msc://")
            .and_then(|rest| rest.strip_suffix('/'))
            .filter(|profile| !profile.is_empty() && !profile.contains('/'))
            .ok_or_else(|| {
                ConfigError::new(&path, "mapping target must have the form msc://profile/")
            })?;
        validate_authority(profile)
            .map_err(|_| ConfigError::new(&path, "mapping target profile is invalid"))?;
        if !profiles.contains_key(profile) {
            return Err(ConfigError::new(
                &path,
                "mapping target profile does not exist",
            ));
        }
    }
    Ok(())
}
