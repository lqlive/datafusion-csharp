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

//! SQLite table provider.
//!
//! A [`SqliteTableFactoryHandle`] owns the connection pool once it is built, so
//! a single factory can register many tables without rebuilding a pool per
//! table. Pool creation and per-table provider resolution both honour an
//! optional cancellation token handle.

use std::os::raw::{c_char, c_int, c_ulonglong};

use datafusion::prelude::SessionContext;

use crate::{cstr, require_ptr, take_result, write_handle};

#[cfg(feature = "sqlite")]
use std::{sync::Arc, time::Duration};

#[cfg(feature = "sqlite")]
use datafusion::{error::DataFusionError, sql::TableReference};
#[cfg(feature = "sqlite")]
use datafusion_table_providers::{
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
    sqlite::SqliteTableFactory,
};

#[cfg(feature = "sqlite")]
use crate::{
    cancellation::{resolve_token, run_cancellable},
    runtime,
};

/// Opaque handle returned to the managed side. Holds the pooled factory so
/// repeated registrations reuse one connection pool.
pub struct SqliteTableFactoryHandle {
    #[cfg(feature = "sqlite")]
    factory: SqliteTableFactory,
}

#[no_mangle]
pub extern "C" fn df_sqlite_table_factory_new(
    path: *const c_char,
    busy_timeout_ms: c_ulonglong,
    token_handle: u64,
    out: *mut *mut SqliteTableFactoryHandle,
) -> c_int {
    take_result(|| {
        let path = cstr(path, "path")?;
        let handle = create_sqlite_table_factory(path, busy_timeout_ms, token_handle)?;
        write_handle(out, handle)
    })
}

#[no_mangle]
pub extern "C" fn df_sqlite_table_factory_register(
    factory: *mut SqliteTableFactoryHandle,
    context: *mut SessionContext,
    registration_name: *const c_char,
    table_name: *const c_char,
    token_handle: u64,
) -> c_int {
    take_result(|| {
        let factory = unsafe { &*require_ptr(factory, "SqliteTableFactory handle")? };
        let ctx = unsafe { &*require_ptr(context, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let table_name = cstr(table_name, "table_name")?;

        register_sqlite_table(factory, ctx, registration_name, table_name, token_handle)
    })
}

#[no_mangle]
pub extern "C" fn df_sqlite_table_factory_free(factory: *mut SqliteTableFactoryHandle) -> c_int {
    take_result(|| {
        if !factory.is_null() {
            unsafe {
                drop(Box::from_raw(factory));
            }
        }
        Ok(())
    })
}

#[cfg(feature = "sqlite")]
fn create_sqlite_table_factory(
    path: String,
    busy_timeout_ms: c_ulonglong,
    token_handle: u64,
) -> crate::NativeResult<SqliteTableFactoryHandle> {
    let token = resolve_token(token_handle);
    let pool = runtime().block_on(run_cancellable(&token, async move {
        SqliteConnectionPoolFactory::new(&path, Mode::File, Duration::from_millis(busy_timeout_ms))
            .build()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }))?;
    Ok(SqliteTableFactoryHandle {
        factory: SqliteTableFactory::new(Arc::new(pool)),
    })
}

#[cfg(feature = "sqlite")]
fn register_sqlite_table(
    handle: &SqliteTableFactoryHandle,
    ctx: &SessionContext,
    registration_name: String,
    table_name: String,
    token_handle: u64,
) -> crate::NativeResult<()> {
    let token = resolve_token(token_handle);
    let provider = runtime().block_on(run_cancellable(&token, async move {
        handle
            .factory
            .table_provider(TableReference::bare(table_name))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }))?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "sqlite"))]
fn create_sqlite_table_factory(
    _path: String,
    _busy_timeout_ms: c_ulonglong,
    _token_handle: u64,
) -> crate::NativeResult<SqliteTableFactoryHandle> {
    Err(
        "datafusion_csharp_native was built without the `sqlite` Cargo feature; \
         rebuild the native crate with `--features sqlite` to enable SQLite table providers"
            .into(),
    )
}

#[cfg(not(feature = "sqlite"))]
fn register_sqlite_table(
    _handle: &SqliteTableFactoryHandle,
    _ctx: &SessionContext,
    _registration_name: String,
    _table_name: String,
    _token_handle: u64,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `sqlite` Cargo feature; \
         rebuild the native crate with `--features sqlite` to enable SQLite table providers"
            .into(),
    )
}
