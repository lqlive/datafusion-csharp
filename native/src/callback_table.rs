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

//! Callback-driven streaming table provider.
//!
//! The managed side owns the data source (for example an ADO.NET reader) and
//! exposes it as a [`StreamingTable`] partition. On every scan the native
//! adapter calls back into managed code, which exports a fresh
//! `CArrowArrayStream` over the Arrow C Data Interface; the adapter wraps that
//! stream as a DataFusion [`SendableRecordBatchStream`]. No native database
//! drivers are involved, so the crate stays free of their transitive TLS and
//! `openssl` dependencies.
//!
//! Ownership of the managed context handle is transferred to the adapter: the
//! `release` callback is invoked exactly once, either synchronously when
//! registration fails or when the table is finally dropped, so the managed side
//! never frees the handle itself.

use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;

use ::arrow::array::RecordBatch;
use ::arrow::datatypes::SchemaRef;
use ::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::stream;

use crate::schema::decode_optional_schema;
use crate::{cstr, require_ptr, take_result};

/// Managed scan callback: fills `out` with a fresh Arrow C Data Interface
/// stream and returns 0 on success or a non-zero status on failure.
type ScanCallback = extern "C" fn(context: *mut c_void, out: *mut FFI_ArrowArrayStream) -> c_int;

/// Managed release callback: frees the context handle. Invoked exactly once.
type ReleaseCallback = extern "C" fn(context: *mut c_void);

/// Owns the managed context handle and guarantees its release callback runs
/// exactly once when this guard is dropped, regardless of which code path
/// (success, registration failure, or panic) drops it.
struct ContextGuard {
    context: *mut c_void,
    release: Option<ReleaseCallback>,
}

// The raw context pointer is an opaque managed handle; the managed side keeps
// the target alive and tolerates calls from arbitrary native worker threads.
unsafe impl Send for ContextGuard {}
unsafe impl Sync for ContextGuard {}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(self.context);
        }
    }
}

struct ManagedScan {
    schema: SchemaRef,
    callback: ScanCallback,
    guard: ContextGuard,
}

impl std::fmt::Debug for ManagedScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedScan")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ManagedScan {
    fn invoke(&self) -> Result<ArrowArrayStreamReader, DataFusionError> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = (self.callback)(self.guard.context, &mut stream as *mut FFI_ArrowArrayStream);
        if status != 0 {
            return Err(DataFusionError::Execution(format!(
                "managed table provider scan callback returned status {status}"
            )));
        }
        ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl PartitionStream for ManagedScan {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        // Box the iterator so both the success and failure paths collapse to a
        // single concrete stream type. The reader is pulled lazily, one batch
        // at a time, as DataFusion polls the stream.
        let batches: Box<dyn Iterator<Item = Result<RecordBatch, DataFusionError>> + Send> =
            match self.invoke() {
                Ok(reader) => Box::new(reader.map(|batch| batch.map_err(DataFusionError::from))),
                Err(err) => Box::new(std::iter::once(Err(err))),
            };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
    }
}

#[no_mangle]
pub extern "C" fn df_session_context_register_callback_table(
    handle: *mut SessionContext,
    name: *const c_char,
    schema_ptr: *const u8,
    schema_len: usize,
    callback: ScanCallback,
    context: *mut c_void,
    release: ReleaseCallback,
) -> c_int {
    // Built before any fallible work so its Drop releases the managed handle on
    // every early-return path; on success it moves into the adapter and fires
    // when the table is dropped.
    let guard = ContextGuard {
        context,
        release: Some(release),
    };
    take_result(move || {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let name = cstr(name, "name")?;
        let schema: SchemaRef = Arc::new(
            decode_optional_schema(schema_ptr, schema_len)?
                .ok_or("callback table requires a non-empty schema")?,
        );
        let scan = Arc::new(ManagedScan {
            schema: schema.clone(),
            callback,
            guard,
        });
        let partitions: Vec<Arc<dyn PartitionStream>> = vec![scan];
        let table = StreamingTable::try_new(schema, partitions)?;
        ctx.register_table(name, Arc::new(table))?;
        Ok(())
    })
}
