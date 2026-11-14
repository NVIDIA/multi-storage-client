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

//! Safe printable configuration documents and secret-key classification.
use serde_json::Value;

pub(super) fn redact(value: &Value, all_values: bool) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| {
                    let secret = is_secret_key(key) && key != "credentials_provider";
                    (
                        key.clone(),
                        if key == "credentials_provider" && !all_values {
                            redact_credentials_provider(value)
                        } else if secret {
                            Value::String("[REDACTED]".into())
                        } else {
                            redact(value, all_values)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| redact(value, all_values))
                .collect(),
        ),
        _ if all_values => Value::String("[REDACTED]".into()),
        Value::String(string) if secret_url(string) => Value::String("[REDACTED]".into()),
        _ => value.clone(),
    }
}

fn redact_credentials_provider(value: &Value) -> Value {
    match value.as_object() {
        Some(provider) => Value::Object(
            provider
                .iter()
                .map(|(field, value)| (field.clone(), redact(value, field == "options")))
                .collect(),
        ),
        None => value.clone(),
    }
}

fn secret_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        !url.username().is_empty() || url.password().is_some() || url.query().is_some()
    })
}

pub(super) fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "secret",
        "password",
        "token",
        "credential",
        "access_key",
        "authorization",
        "headers",
        "client_key",
        "api_key",
        "private_key",
        "signing_key",
        "account_key",
        "connection",
        "passphrase",
        "passwd",
        "cookie",
        "signature",
    ]
    .iter()
    .any(|part| key.contains(part))
}

pub(super) fn redact_matching(value: &mut Value, secret: &Value) {
    if value == secret {
        *value = Value::String("[REDACTED]".into());
    } else {
        match value {
            Value::Object(fields) => fields
                .values_mut()
                .for_each(|value| redact_matching(value, secret)),
            Value::Array(values) => values
                .iter_mut()
                .for_each(|value| redact_matching(value, secret)),
            _ => (),
        }
    }
}
