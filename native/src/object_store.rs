// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use url::Url;

use crate::proto_gen::object_store_registration::Backend;
use crate::proto_gen::ObjectStoreRegistration;
use crate::NativeResult;

#[cfg(feature = "object-store-aws")]
use crate::proto_gen::S3Options;
#[cfg(feature = "object-store-gcp")]
use crate::proto_gen::GcsOptions;
#[cfg(feature = "object-store-http")]
use crate::proto_gen::HttpOptions;

pub(crate) fn apply_registrations(
    ctx: &SessionContext,
    regs: &[ObjectStoreRegistration],
) -> NativeResult<()> {
    for reg in regs {
        let backend = reg
            .backend
            .as_ref()
            .ok_or("ObjectStoreRegistration.backend is required")?;
        let (url, store) = build_store(reg.url.as_deref(), backend)?;
        ctx.runtime_env().register_object_store(&url, store);
    }
    Ok(())
}

#[allow(unused_variables)]
fn build_store(
    url_override: Option<&str>,
    backend: &Backend,
) -> NativeResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    match backend {
        #[cfg(feature = "object-store-aws")]
        Backend::S3(opts) => build_s3(url_override, opts),
        #[cfg(not(feature = "object-store-aws"))]
        Backend::S3(_) => {
            Err("object-store-aws Cargo feature is not enabled in this build".into())
        }

        #[cfg(feature = "object-store-gcp")]
        Backend::Gcs(opts) => build_gcs(url_override, opts),
        #[cfg(not(feature = "object-store-gcp"))]
        Backend::Gcs(_) => {
            Err("object-store-gcp Cargo feature is not enabled in this build".into())
        }

        #[cfg(feature = "object-store-http")]
        Backend::Http(opts) => build_http(url_override, opts),
        #[cfg(not(feature = "object-store-http"))]
        Backend::Http(_) => {
            Err("object-store-http Cargo feature is not enabled in this build".into())
        }
    }
}

#[cfg(feature = "object-store-aws")]
fn build_s3(
    url_override: Option<&str>,
    opts: &S3Options,
) -> NativeResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

    if opts.bucket.is_empty() {
        return Err("S3Options.bucket is required".into());
    }

    let java_has_endpoint = opts.endpoint.is_some();
    let mut builder = AmazonS3Builder::default();
    for (os_key, os_value) in std::env::vars_os() {
        let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) else {
            continue;
        };
        if !key.starts_with("AWS_") {
            continue;
        }
        if java_has_endpoint
            && (key == "AWS_ENDPOINT" || key == "AWS_ENDPOINT_URL" || key == "AWS_ENDPOINT_URL_S3")
        {
            continue;
        }
        if key == "AWS_BUCKET" || key == "AWS_BUCKET_NAME" {
            continue;
        }
        if let Ok(config_key) = key.to_ascii_lowercase().parse() {
            builder = builder.with_config(config_key, value);
        }
    }

    builder = builder.with_bucket_name(&opts.bucket);
    if let Some(ref value) = opts.region {
        builder = builder.with_region(value);
    }
    if let Some(ref value) = opts.endpoint {
        builder = builder.with_endpoint(value);
    }
    if let Some(ref value) = opts.access_key_id {
        builder = builder.with_access_key_id(value);
    }
    if let Some(ref value) = opts.secret_access_key {
        builder = builder.with_secret_access_key(value);
    }
    if let Some(ref value) = opts.session_token {
        builder = builder.with_token(value);
    }
    if let Some(value) = opts.allow_http {
        builder = builder.with_allow_http(value);
    }
    if let Some(value) = opts.skip_signature {
        builder = builder.with_skip_signature(value);
    }
    if let Some(value) = opts.imdsv1_fallback {
        builder = builder.with_config(AmazonS3ConfigKey::ImdsV1Fallback, value.to_string());
    }

    let store = builder.build()?;
    let url = parse_url(url_override, format!("s3://{}", opts.bucket))?;
    Ok((url, Arc::new(store)))
}

#[cfg(feature = "object-store-gcp")]
fn build_gcs(
    url_override: Option<&str>,
    opts: &GcsOptions,
) -> NativeResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    if opts.bucket.is_empty() {
        return Err("GcsOptions.bucket is required".into());
    }
    if opts.service_account_key.is_some() && opts.service_account_path.is_some() {
        return Err("GcsOptions: service_account_key and service_account_path are mutually exclusive".into());
    }

    let java_has_credential = opts.service_account_key.is_some()
        || opts.service_account_path.is_some()
        || opts.application_credentials.is_some();
    let mut builder = GoogleCloudStorageBuilder::default();
    for (os_key, os_value) in std::env::vars_os() {
        let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) else {
            continue;
        };
        if !key.starts_with("GOOGLE_") {
            continue;
        }
        if java_has_credential
            && (key == "GOOGLE_SERVICE_ACCOUNT"
                || key == "GOOGLE_SERVICE_ACCOUNT_PATH"
                || key == "GOOGLE_SERVICE_ACCOUNT_KEY"
                || key == "GOOGLE_APPLICATION_CREDENTIALS")
        {
            continue;
        }
        if key == "GOOGLE_BUCKET" || key == "GOOGLE_BUCKET_NAME" {
            continue;
        }
        if let Ok(config_key) = key.to_ascii_lowercase().parse() {
            builder = builder.with_config(config_key, value);
        }
    }

    builder = builder.with_bucket_name(&opts.bucket);
    if let Some(ref value) = opts.service_account_key {
        builder = builder.with_service_account_key(value);
    }
    if let Some(ref value) = opts.service_account_path {
        builder = builder.with_service_account_path(value);
    }
    if let Some(ref value) = opts.application_credentials {
        builder = builder.with_application_credentials(value);
    }

    let store = builder.build()?;
    let url = parse_url(url_override, format!("gs://{}", opts.bucket))?;
    Ok((url, Arc::new(store)))
}

#[cfg(feature = "object-store-http")]
fn build_http(
    url_override: Option<&str>,
    opts: &HttpOptions,
) -> NativeResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::http::HttpBuilder;

    let listing = url_override.ok_or(
        "HttpOptions: ObjectStoreRegistration.url is required for the HTTP backend (no scheme-default)",
    )?;
    let listing_url =
        Url::parse(listing).map_err(|err| format!("invalid HTTP URL {listing:?}: {err}"))?;
    if !listing_url.path().is_empty() && listing_url.path() != "/" {
        return Err(format!(
            "HttpOptions: listing URL must be a host root (no path component); got {listing:?}"
        )
        .into());
    }

    let mut builder = HttpBuilder::new().with_url(listing);
    if let Some(value) = opts.allow_http {
        builder = builder.with_client_options(object_store::ClientOptions::new().with_allow_http(value));
    }

    let store = builder.build()?;
    Ok((listing_url, Arc::new(store)))
}

#[cfg(any(feature = "object-store-aws", feature = "object-store-gcp"))]
fn parse_url(url_override: Option<&str>, default: String) -> NativeResult<Url> {
    let value = url_override.unwrap_or(&default);
    Url::parse(value).map_err(|err| format!("invalid object store URL {value:?}: {err}").into())
}
