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
use std::sync::Arc;

use datafusion::datasource::MemTable;

use crate::schema::read_ipc_batches;
use crate::{bytes, cstr, require_ptr, take_result};

#[no_mangle]
pub extern "C" fn df_session_context_register_table_ipc(
    handle: *mut datafusion::prelude::SessionContext,
    name: *const c_char,
    ipc_ptr: *const u8,
    ipc_len: usize,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let name = cstr(name, "name")?;
        let ipc = bytes(ipc_ptr, ipc_len)?;
        let (schema, batches) = read_ipc_batches(ipc)?;
        let table = MemTable::try_new(schema, vec![batches])?;
        ctx.register_table(name, Arc::new(table))?;
        Ok(())
    })
}
