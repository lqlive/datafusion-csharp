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

use std::ffi::c_void;
use std::os::raw::{c_char, c_int, c_uchar};
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};

use crate::{cstr, require_ptr, take_result, OK};

type DfScalarI64Callback =
    extern "C" fn(user_data: *mut c_void, out_value: *mut i64) -> c_int;

#[no_mangle]
pub extern "C" fn df_session_context_register_scalar_udf_i64(
    handle: *mut datafusion::prelude::SessionContext,
    name: *const c_char,
    volatility: c_uchar,
    callback: Option<DfScalarI64Callback>,
    user_data: *mut c_void,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let name = cstr(name, "name")?;
        let callback = callback.ok_or("scalar UDF callback is null")?;
        let user_data = user_data as usize;
        let volatility = match volatility {
            0 => Volatility::Immutable,
            1 => Volatility::Stable,
            2 => Volatility::Volatile,
            other => return Err(format!("unknown scalar UDF volatility: {other}").into()),
        };

        let udf = create_udf(
            &name,
            vec![],
            ::arrow::datatypes::DataType::Int64,
            volatility,
            Arc::new(move |_args: &[ColumnarValue]| {
                let mut value = 0_i64;
                let status = callback(user_data as *mut c_void, &mut value as *mut i64);
                if status != OK {
                    return Err(DataFusionError::Execution(
                        "managed scalar UDF callback failed".to_string(),
                    ));
                }

                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value))))
            }),
        );
        ctx.register_udf(udf);
        Ok(())
    })
}
