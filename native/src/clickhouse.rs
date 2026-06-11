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

use std::os::raw::{c_char, c_int};

use crate::{cstr, require_ptr, take_result};

#[cfg(feature = "clickhouse")]
use std::collections::HashMap;

#[cfg(feature = "clickhouse")]
use datafusion::sql::TableReference;
#[cfg(feature = "clickhouse")]
use datafusion_table_providers::{
    clickhouse::ClickHouseTableFactory,
    sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool,
    util::secrets::to_secret_map,
};

#[cfg(feature = "clickhouse")]
use crate::runtime;

#[no_mangle]
pub extern "C" fn df_session_context_register_clickhouse_table(
    handle: *mut datafusion::prelude::SessionContext,
    registration_name: *const c_char,
    url: *const c_char,
    database: *const c_char,
    user: *const c_char,
    password: *const c_char,
    table_name: *const c_char,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let url = cstr(url, "url")?;
        let database = optional_cstr(database)?;
        let user = optional_cstr(user)?;
        let password = optional_cstr(password)?;
        let table_name = cstr(table_name, "table_name")?;

        register_clickhouse_table(
            ctx,
            registration_name,
            url,
            database,
            user,
            password,
            table_name,
        )
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
fn register_clickhouse_table(
    ctx: &datafusion::prelude::SessionContext,
    registration_name: String,
    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    table_name: String,
) -> crate::NativeResult<()> {
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

    let provider = runtime().block_on(async {
        let pool = ClickHouseConnectionPool::new(to_secret_map(params)).await?;
        let factory = ClickHouseTableFactory::new(pool);
        factory
            .table_provider(TableReference::bare(table_name), None)
            .await
    })?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "clickhouse"))]
fn register_clickhouse_table(
    _ctx: &datafusion::prelude::SessionContext,
    _registration_name: String,
    _url: String,
    _database: Option<String>,
    _user: Option<String>,
    _password: Option<String>,
    _table_name: String,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `clickhouse` Cargo feature; \
         rebuild the native crate with `--features clickhouse` to enable ClickHouse table providers"
            .into(),
    )
}
