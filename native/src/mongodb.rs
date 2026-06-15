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

//! MongoDB table provider.
//!
//! A [`MongoDbTableFactoryHandle`] owns the connection pool once it is built,
//! so a single factory can register many collections without rebuilding a pool
//! per collection. Pool creation and per-collection provider resolution both
//! honour an optional cancellation token handle.

use std::os::raw::{c_char, c_int};

use datafusion::prelude::SessionContext;

use crate::{cstr, require_ptr, take_result, write_handle};

#[cfg(feature = "mongodb")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "mongodb")]
use datafusion::{error::DataFusionError, sql::TableReference};
#[cfg(feature = "mongodb")]
use datafusion_table_providers::{
    mongodb::{connection_pool::MongoDBConnectionPool, MongoDBTableFactory},
    util::secrets::to_secret_map,
};

#[cfg(feature = "mongodb")]
use crate::{
    cancellation::{resolve_token, run_cancellable},
    runtime,
};

/// Opaque handle returned to the managed side. Holds the pooled factory so
/// repeated registrations reuse one connection pool.
pub struct MongoDbTableFactoryHandle {
    #[cfg(feature = "mongodb")]
    factory: MongoDBTableFactory,
}

#[no_mangle]
pub extern "C" fn df_mongodb_table_factory_new(
    connection_string: *const c_char,
    token_handle: u64,
    out: *mut *mut MongoDbTableFactoryHandle,
) -> c_int {
    take_result(|| {
        let connection_string = cstr(connection_string, "connection_string")?;
        let handle = create_mongodb_table_factory(connection_string, token_handle)?;
        write_handle(out, handle)
    })
}

#[no_mangle]
pub extern "C" fn df_mongodb_table_factory_register(
    factory: *mut MongoDbTableFactoryHandle,
    context: *mut SessionContext,
    registration_name: *const c_char,
    collection_name: *const c_char,
    token_handle: u64,
) -> c_int {
    take_result(|| {
        let factory = unsafe { &*require_ptr(factory, "MongoDbTableFactory handle")? };
        let ctx = unsafe { &*require_ptr(context, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let collection_name = cstr(collection_name, "collection_name")?;

        register_mongodb_table(
            factory,
            ctx,
            registration_name,
            collection_name,
            token_handle,
        )
    })
}

#[no_mangle]
pub extern "C" fn df_mongodb_table_factory_free(factory: *mut MongoDbTableFactoryHandle) -> c_int {
    take_result(|| {
        if !factory.is_null() {
            unsafe {
                drop(Box::from_raw(factory));
            }
        }
        Ok(())
    })
}

#[cfg(feature = "mongodb")]
fn create_mongodb_table_factory(
    connection_string: String,
    token_handle: u64,
) -> crate::NativeResult<MongoDbTableFactoryHandle> {
    let token = resolve_token(token_handle);
    let params = to_secret_map(HashMap::from([(
        "connection_string".to_string(),
        connection_string,
    )]));
    let pool = runtime().block_on(run_cancellable(&token, async move {
        MongoDBConnectionPool::new(params)
            .await
            .map_err(|e| DataFusionError::External(e))
    }))?;
    Ok(MongoDbTableFactoryHandle {
        factory: MongoDBTableFactory::new(Arc::new(pool)),
    })
}

#[cfg(feature = "mongodb")]
fn register_mongodb_table(
    handle: &MongoDbTableFactoryHandle,
    ctx: &SessionContext,
    registration_name: String,
    collection_name: String,
    token_handle: u64,
) -> crate::NativeResult<()> {
    let token = resolve_token(token_handle);
    let provider = runtime().block_on(run_cancellable(&token, async move {
        handle
            .factory
            .table_provider(TableReference::bare(collection_name))
            .await
            .map_err(|e| DataFusionError::External(e))
    }))?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "mongodb"))]
fn create_mongodb_table_factory(
    _connection_string: String,
    _token_handle: u64,
) -> crate::NativeResult<MongoDbTableFactoryHandle> {
    Err(
        "datafusion_csharp_native was built without the `mongodb` Cargo feature; \
         rebuild the native crate with `--features mongodb` to enable MongoDB table providers"
            .into(),
    )
}

#[cfg(not(feature = "mongodb"))]
fn register_mongodb_table(
    _handle: &MongoDbTableFactoryHandle,
    _ctx: &SessionContext,
    _registration_name: String,
    _collection_name: String,
    _token_handle: u64,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `mongodb` Cargo feature; \
         rebuild the native crate with `--features mongodb` to enable MongoDB table providers"
            .into(),
    )
}
