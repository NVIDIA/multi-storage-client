// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use http::{HeaderValue, Method};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use object_store::{Attribute, ClientOptions, GetResult};

use crate::{
    types::{HeadResult, ObjectMetadata},
    StorageError,
};

#[derive(Debug)]
pub struct MetadataConnector;

impl HttpConnector for MetadataConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(MetadataService(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

#[derive(Debug)]
struct MetadataService(HttpClient);

#[derive(Clone, Debug)]
struct HeadHeaders {
    storage_class: Option<HeaderValue>,
    has_last_modified: bool,
}

#[async_trait]
impl HttpService for MetadataService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let is_head = request.method() == Method::HEAD;
        let mut response = self.0.execute(request).await?;
        // object_store 0.14 does not expose the S3 storage class in GetResult attributes.
        if is_head {
            let headers = HeadHeaders {
                storage_class: response.headers().get("x-amz-storage-class").cloned(),
                has_last_modified: response.headers().contains_key("last-modified"),
            };
            response.extensions_mut().insert(headers);
        }
        Ok(response)
    }
}

pub fn from_head(result: GetResult) -> Result<HeadResult, StorageError> {
    let headers = result.extensions.get::<HeadHeaders>();
    let storage_class = headers
        .and_then(|headers| headers.storage_class.as_ref())
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|error| StorageError::ObjectStoreError(error.to_string()))?;
    let metadata = result
        .attributes
        .iter()
        .filter_map(|(attribute, value)| {
            if let Attribute::Metadata(key) = attribute {
                Some((key.to_string(), value.as_ref().to_owned()))
            } else {
                None
            }
        })
        .collect();
    Ok(HeadResult {
        object_metadata: ObjectMetadata {
            key: result.meta.location.to_string(),
            content_length: result.meta.size,
            last_modified: if headers.is_some_and(|headers| !headers.has_last_modified) {
                "0001-01-01T00:00:00+00:00".to_string()
            } else {
                result.meta.last_modified.to_rfc3339()
            },
            object_type: "file".to_string(),
            etag: result.meta.e_tag,
        },
        content_type: result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_owned()),
        storage_class,
        metadata,
    })
}
