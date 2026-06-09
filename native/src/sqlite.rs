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

use std::os::raw::{c_char, c_int, c_ulonglong};

use crate::{cstr, require_ptr, take_result};

#[cfg(feature = "sqlite")]
use std::{sync::Arc, time::Duration};

#[cfg(feature = "sqlite")]
use datafusion::sql::TableReference;
#[cfg(feature = "sqlite")]
use datafusion_table_providers::{
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
    sqlite::SqliteTableFactory,
};

#[cfg(feature = "sqlite")]
use crate::runtime;

#[no_mangle]
pub extern "C" fn df_session_context_register_sqlite_table(
    handle: *mut datafusion::prelude::SessionContext,
    registration_name: *const c_char,
    path: *const c_char,
    table_name: *const c_char,
    busy_timeout_ms: c_ulonglong,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let path = cstr(path, "path")?;
        let table_name = cstr(table_name, "table_name")?;

        register_sqlite_table(ctx, registration_name, path, table_name, busy_timeout_ms)
    })
}

#[cfg(feature = "sqlite")]
fn register_sqlite_table(
    ctx: &datafusion::prelude::SessionContext,
    registration_name: String,
    path: String,
    table_name: String,
    busy_timeout_ms: c_ulonglong,
) -> crate::NativeResult<()> {
    let provider = runtime().block_on(async {
        let pool = Arc::new(
            SqliteConnectionPoolFactory::new(
                &path,
                Mode::File,
                Duration::from_millis(busy_timeout_ms),
            )
            .build()
            .await?,
        );
        let factory = SqliteTableFactory::new(pool);
        factory
            .table_provider(TableReference::bare(table_name))
            .await
    })?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "sqlite"))]
fn register_sqlite_table(
    _ctx: &datafusion::prelude::SessionContext,
    _registration_name: String,
    _path: String,
    _table_name: String,
    _busy_timeout_ms: c_ulonglong,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `sqlite` Cargo feature; \
         rebuild the native crate with `--features sqlite` to enable SQLite table providers"
            .into(),
    )
}
