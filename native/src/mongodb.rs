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

#[cfg(feature = "mongodb")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "mongodb")]
use datafusion::sql::TableReference;
#[cfg(feature = "mongodb")]
use datafusion_table_providers::{
    mongodb::{connection_pool::MongoDBConnectionPool, MongoDBTableFactory},
    util::secrets::to_secret_map,
};

#[cfg(feature = "mongodb")]
use crate::runtime;

#[no_mangle]
pub extern "C" fn df_session_context_register_mongodb_table(
    handle: *mut datafusion::prelude::SessionContext,
    registration_name: *const c_char,
    connection_string: *const c_char,
    collection_name: *const c_char,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let registration_name = cstr(registration_name, "registration_name")?;
        let connection_string = cstr(connection_string, "connection_string")?;
        let collection_name = cstr(collection_name, "collection_name")?;

        register_mongodb_table(ctx, registration_name, connection_string, collection_name)
    })
}

#[cfg(feature = "mongodb")]
fn register_mongodb_table(
    ctx: &datafusion::prelude::SessionContext,
    registration_name: String,
    connection_string: String,
    collection_name: String,
) -> crate::NativeResult<()> {
    let params = to_secret_map(HashMap::from([(
        "connection_string".to_string(),
        connection_string,
    )]));
    let provider = runtime().block_on(async {
        let pool = Arc::new(MongoDBConnectionPool::new(params).await?);
        let factory = MongoDBTableFactory::new(pool);
        factory
            .table_provider(TableReference::bare(collection_name))
            .await
    })?;

    ctx.register_table(registration_name, provider)?;
    Ok(())
}

#[cfg(not(feature = "mongodb"))]
fn register_mongodb_table(
    _ctx: &datafusion::prelude::SessionContext,
    _registration_name: String,
    _connection_string: String,
    _collection_name: String,
) -> crate::NativeResult<()> {
    Err(
        "datafusion_csharp_native was built without the `mongodb` Cargo feature; \
         rebuild the native crate with `--features mongodb` to enable MongoDB table providers"
            .into(),
    )
}
