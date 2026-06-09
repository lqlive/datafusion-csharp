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

use std::os::raw::c_int;

use datafusion::dataframe::DataFrame;
use datafusion_proto::logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec};
use datafusion_proto::protobuf::LogicalPlanNode;
use prost::Message;

use crate::{bytes, require_ptr, runtime, take_result, write_handle};

#[no_mangle]
pub extern "C" fn df_session_context_from_proto(
    handle: *mut datafusion::prelude::SessionContext,
    plan_ptr: *const u8,
    plan_len: usize,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let node = LogicalPlanNode::decode(bytes(plan_ptr, plan_len)?)?;
        let codec = DefaultLogicalExtensionCodec {};
        let task_ctx = ctx.task_ctx();
        let plan = node.try_into_logical_plan(task_ctx.as_ref(), &codec)?;
        let df = runtime().block_on(ctx.execute_logical_plan(plan))?;
        write_handle(out, df)
    })
}

#[cfg(feature = "substrait")]
#[no_mangle]
pub extern "C" fn df_session_context_from_substrait(
    handle: *mut datafusion::prelude::SessionContext,
    plan_ptr: *const u8,
    plan_len: usize,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let plan_bytes = bytes(plan_ptr, plan_len)?.to_vec();
        let plan = runtime().block_on(async {
            let substrait_plan =
                datafusion_substrait::serializer::deserialize_bytes(plan_bytes).await?;
            datafusion_substrait::logical_plan::consumer::from_substrait_plan(
                &ctx.state(),
                &substrait_plan,
            )
            .await
        })?;
        let df = runtime().block_on(ctx.execute_logical_plan(plan))?;
        write_handle(out, df)
    })
}

#[cfg(not(feature = "substrait"))]
#[no_mangle]
pub extern "C" fn df_session_context_from_substrait(
    _handle: *mut datafusion::prelude::SessionContext,
    _plan_ptr: *const u8,
    _plan_len: usize,
    _out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        Err(
            "datafusion_csharp_native was built without the `substrait` Cargo feature; \
             rebuild the native crate with `--features substrait` to enable \
             SessionContext.FromSubstrait"
                .into(),
        )
    })
}
