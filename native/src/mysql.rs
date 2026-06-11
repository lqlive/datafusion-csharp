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

#[cfg(feature = "mysql")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "mysql")]
use datafusion::sql::TableReference;
#[cfg(feature = "mysql")]
use datafusion_table_providers::{
    mysql::MySQLTableFactory, sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
    util::secrets::to_secret_map,
};

#[cfg(feature = "mysql")]
use crate::runtime;

#[no_mangle]
pub extern "C" fn df_session_context_register_mysql_table(
    handle: *mut datafusion::prelude::SessionContext,
    registration_name: *const c_char,
    connection_string: *const c_char,
    schema_name: *const c_char,
    table_name: *const c_char,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let connection_string = cstr(connection_string, "connection_string")?;
        let schema_name = optional_cstr(schema_name)?;
        let table_name = cstr(table_name, "table_name")?;

        register_mysql_table(
            ctx,
            registration_name,
            connection_string,
            schema_name,
            table_name,
        )
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
fn register_mysql_table(
    ctx: &datafusion::prelude::SessionContext,
    registration_name: String,
    connection_string: String,
    schema_name: Option<String>,
    table_name: String,
) -> crate::NativeResult<()> {
    let table_reference = match schema_name {
        Some(schema) if !schema.is_empty() => TableReference::partial(schema, table_name),
        _ => TableReference::bare(table_name),
    };

    let params = to_secret_map(HashMap::from([(
        "connection_string".to_string(),
        connection_string,
    )]));
    let provider = runtime().block_on(async {
        let pool = Arc::new(MySQLConnectionPool::new(params).await?);
        let factory = MySQLTableFactory::new(pool);
        factory.table_provider(table_reference).await
    })?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "mysql"))]
fn register_mysql_table(
    _ctx: &datafusion::prelude::SessionContext,
    _registration_name: String,
    _connection_string: String,
    _schema_name: Option<String>,
    _table_name: String,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `mysql` Cargo feature; \
         rebuild the native crate with `--features mysql` to enable MySQL table providers"
            .into(),
    )
}
