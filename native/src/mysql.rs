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

//! MySQL table provider.
//!
//! A [`MySqlTableFactoryHandle`] owns the connection pool once it is built, so
//! a single factory can register many tables without re-establishing a pool
//! per table. Pool creation and per-table provider resolution both honour an
//! optional cancellation token handle.

use std::os::raw::{c_char, c_int};

use datafusion::prelude::SessionContext;

use crate::{cstr, require_ptr, take_result, write_handle};

#[cfg(feature = "mysql")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "mysql")]
use datafusion::{error::DataFusionError, sql::TableReference};
#[cfg(feature = "mysql")]
use datafusion_table_providers::{
    mysql::MySQLTableFactory, sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
    util::secrets::to_secret_map,
};

#[cfg(feature = "mysql")]
use crate::{
    cancellation::{resolve_token, run_cancellable},
    runtime,
};

/// Opaque handle returned to the managed side. Holds the pooled factory so
/// repeated registrations reuse one connection pool.
pub struct MySqlTableFactoryHandle {
    #[cfg(feature = "mysql")]
    factory: MySQLTableFactory,
}

#[no_mangle]
pub extern "C" fn df_mysql_table_factory_new(
    connection_string: *const c_char,
    token_handle: u64,
    out: *mut *mut MySqlTableFactoryHandle,
) -> c_int {
    take_result(|| {
        let connection_string = cstr(connection_string, "connection_string")?;
        let handle = create_mysql_table_factory(connection_string, token_handle)?;
        write_handle(out, handle)
    })
}

#[no_mangle]
pub extern "C" fn df_mysql_table_factory_register(
    factory: *mut MySqlTableFactoryHandle,
    context: *mut SessionContext,
    registration_name: *const c_char,
    schema_name: *const c_char,
    table_name: *const c_char,
    token_handle: u64,
) -> c_int {
    take_result(|| {
        let factory = unsafe { &*require_ptr(factory, "MySqlTableFactory handle")? };
        let ctx = unsafe { &*require_ptr(context, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let schema_name = optional_cstr(schema_name)?;
        let table_name = cstr(table_name, "table_name")?;

        register_mysql_table(
            factory,
            ctx,
            registration_name,
            schema_name,
            table_name,
            token_handle,
        )
    })
}

#[no_mangle]
pub extern "C" fn df_mysql_table_factory_free(factory: *mut MySqlTableFactoryHandle) -> c_int {
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
        cstr(ptr, "schema_name").map(Some)
    }
}

#[cfg(feature = "mysql")]
fn create_mysql_table_factory(
    connection_string: String,
    token_handle: u64,
) -> crate::NativeResult<MySqlTableFactoryHandle> {
    let token = resolve_token(token_handle);
    let params = to_secret_map(HashMap::from([(
        "connection_string".to_string(),
        connection_string,
    )]));
    let pool = runtime().block_on(run_cancellable(&token, async move {
        MySQLConnectionPool::new(params)
            .await
            .map_err(|e| DataFusionError::External(e))
    }))?;
    Ok(MySqlTableFactoryHandle {
        factory: MySQLTableFactory::new(Arc::new(pool)),
    })
}

#[cfg(feature = "mysql")]
fn register_mysql_table(
    handle: &MySqlTableFactoryHandle,
    ctx: &SessionContext,
    registration_name: String,
    schema_name: Option<String>,
    table_name: String,
    token_handle: u64,
) -> crate::NativeResult<()> {
    let token = resolve_token(token_handle);
    let table_reference = match schema_name {
        Some(schema) if !schema.is_empty() => TableReference::partial(schema, table_name),
        _ => TableReference::bare(table_name),
    };
    let provider = runtime().block_on(run_cancellable(&token, async move {
        handle
            .factory
            .table_provider(table_reference)
            .await
            .map_err(|e| DataFusionError::External(e))
    }))?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "mysql"))]
fn create_mysql_table_factory(
    _connection_string: String,
    _token_handle: u64,
) -> crate::NativeResult<MySqlTableFactoryHandle> {
    Err(
        "datafusion_csharp_native was built without the `mysql` Cargo feature; \
         rebuild the native crate with `--features mysql` to enable MySQL table providers"
            .into(),
    )
}

#[cfg(not(feature = "mysql"))]
fn register_mysql_table(
    _handle: &MySqlTableFactoryHandle,
    _ctx: &SessionContext,
    _registration_name: String,
    _schema_name: Option<String>,
    _table_name: String,
    _token_handle: u64,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `mysql` Cargo feature; \
         rebuild the native crate with `--features mysql` to enable MySQL table providers"
            .into(),
    )
}
