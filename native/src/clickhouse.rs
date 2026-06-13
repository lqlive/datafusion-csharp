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

//! ClickHouse table provider.
//!
//! A [`ClickHouseTableFactoryHandle`] owns the connection pool once it is
//! built, so a single factory can register many tables without rebuilding a
//! pool per table. Pool creation and per-table provider resolution both honour
//! an optional cancellation token handle.

use std::os::raw::{c_char, c_int};

use datafusion::prelude::SessionContext;

use crate::{cstr, require_ptr, take_result, write_handle};

#[cfg(feature = "clickhouse")]
use std::collections::HashMap;

#[cfg(feature = "clickhouse")]
use datafusion::{error::DataFusionError, sql::TableReference};
#[cfg(feature = "clickhouse")]
use datafusion_table_providers::{
    clickhouse::ClickHouseTableFactory,
    sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool,
    util::secrets::to_secret_map,
};

#[cfg(feature = "clickhouse")]
use crate::{
    cancellation::{resolve_token, run_cancellable},
    runtime,
};

/// Opaque handle returned to the managed side. Holds the pooled factory so
/// repeated registrations reuse one connection pool.
pub struct ClickHouseTableFactoryHandle {
    #[cfg(feature = "clickhouse")]
    factory: ClickHouseTableFactory,
}

#[no_mangle]
pub extern "C" fn df_clickhouse_table_factory_new(
    url: *const c_char,
    database: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token_handle: u64,
    out: *mut *mut ClickHouseTableFactoryHandle,
) -> c_int {
    take_result(|| {
        let url = cstr(url, "url")?;
        let database = optional_cstr(database)?;
        let user = optional_cstr(user)?;
        let password = optional_cstr(password)?;
        let handle = create_clickhouse_table_factory(url, database, user, password, token_handle)?;
        write_handle(out, handle)
    })
}

#[no_mangle]
pub extern "C" fn df_clickhouse_table_factory_register(
    factory: *mut ClickHouseTableFactoryHandle,
    context: *mut SessionContext,
    registration_name: *const c_char,
    table_name: *const c_char,
    token_handle: u64,
) -> c_int {
    take_result(|| {
        let factory = unsafe { &*require_ptr(factory, "ClickHouseTableFactory handle")? };
        let ctx = unsafe { &*require_ptr(context, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let table_name = cstr(table_name, "table_name")?;

        register_clickhouse_table(factory, ctx, registration_name, table_name, token_handle)
    })
}

#[no_mangle]
pub extern "C" fn df_clickhouse_table_factory_free(
    factory: *mut ClickHouseTableFactoryHandle,
) -> c_int {
    take_result(|| {
        if !factory.is_null() {
            unsafe {
                drop(Box::from_raw(factory));
            }
        }
        Ok(())
    })
}

fn optional_cstr(ptr: *const c_char) -> crate::NativeResult<Option<String>> {
    if ptr.is_null() {
        Ok(None)
    } else {
        cstr(ptr, "optional string").map(Some)
    }
}

#[cfg(feature = "clickhouse")]
fn create_clickhouse_table_factory(
    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    token_handle: u64,
) -> crate::NativeResult<ClickHouseTableFactoryHandle> {
    let token = resolve_token(token_handle);
    let mut params = HashMap::from([("url".to_string(), url)]);
    if let Some(database) = database.filter(|value| !value.is_empty()) {
        params.insert("database".to_string(), database);
    }
    if let Some(user) = user.filter(|value| !value.is_empty()) {
        params.insert("user".to_string(), user);
    }
    if let Some(password) = password.filter(|value| !value.is_empty()) {
        params.insert("password".to_string(), password);
    }

    let pool = runtime().block_on(run_cancellable(&token, async move {
        ClickHouseConnectionPool::new(to_secret_map(params))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }))?;
    Ok(ClickHouseTableFactoryHandle {
        factory: ClickHouseTableFactory::new(pool),
    })
}

#[cfg(feature = "clickhouse")]
fn register_clickhouse_table(
    handle: &ClickHouseTableFactoryHandle,
    ctx: &SessionContext,
    registration_name: String,
    table_name: String,
    token_handle: u64,
) -> crate::NativeResult<()> {
    let token = resolve_token(token_handle);
    let provider = runtime().block_on(run_cancellable(&token, async move {
        handle
            .factory
            .table_provider(TableReference::bare(table_name), None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }))?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "clickhouse"))]
fn create_clickhouse_table_factory(
    _url: String,
    _database: Option<String>,
    _user: Option<String>,
    _password: Option<String>,
    _token_handle: u64,
) -> crate::NativeResult<ClickHouseTableFactoryHandle> {
    Err(
        "datafusion_csharp_native was built without the `clickhouse` Cargo feature; \
         rebuild the native crate with `--features clickhouse` to enable ClickHouse table providers"
            .into(),
    )
}

#[cfg(not(feature = "clickhouse"))]
fn register_clickhouse_table(
    _handle: &ClickHouseTableFactoryHandle,
    _ctx: &SessionContext,
    _registration_name: String,
    _table_name: String,
    _token_handle: u64,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `clickhouse` Cargo feature; \
         rebuild the native crate with `--features clickhouse` to enable ClickHouse table providers"
            .into(),
    )
}
