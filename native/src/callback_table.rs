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

use std::any::Any;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;

use ::arrow::array::RecordBatch;
use ::arrow::datatypes::SchemaRef;
use ::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::catalog::{MemorySchemaProvider, Session, TableProvider};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::stream;

use crate::schema::decode_optional_schema;
use crate::{cstr, require_ptr, take_result};

#[repr(C)]
pub struct ManagedProjection {
    name: *const c_char,
}

#[repr(C)]
pub struct ManagedFilter {
    column: *const c_char,
    op: c_int,
    value_kind: c_int,
    value: *const c_char,
}

#[repr(C)]
pub struct ManagedScanRequest {
    has_projection: c_int,
    projections: *const ManagedProjection,
    projection_len: usize,
    filters: *const ManagedFilter,
    filter_len: usize,
    has_limit: c_int,
    limit: usize,
}

/// Managed scan callback: fills `out` with a fresh Arrow C Data Interface
/// stream and returns 0 on success or a non-zero status on failure.
type ScanCallback = extern "C" fn(
    context: *mut c_void,
    request: *const ManagedScanRequest,
    out: *mut FFI_ArrowArrayStream,
) -> c_int;

/// Managed release callback: frees the context handle. Invoked exactly once.
type ReleaseCallback = extern "C" fn(context: *mut c_void);

/// Owns the managed context handle and guarantees its release callback runs
/// exactly once when this guard is dropped, regardless of which code path
/// (success, registration failure, or panic) drops it.
struct ContextGuard {
    context: *mut c_void,
    release: Option<ReleaseCallback>,
}

impl std::fmt::Debug for ContextGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextGuard").finish_non_exhaustive()
    }
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
    context: Arc<ContextGuard>,
    has_projection: bool,
    projections: Vec<CString>,
    filters: Vec<ManagedFilterOwned>,
    limit: Option<usize>,
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
        let projections: Vec<ManagedProjection> = self
            .projections
            .iter()
            .map(|name| ManagedProjection {
                name: name.as_ptr(),
            })
            .collect();
        let filters: Vec<ManagedFilter> = self
            .filters
            .iter()
            .map(ManagedFilterOwned::as_ffi)
            .collect();
        let request = ManagedScanRequest {
            has_projection: self.has_projection as c_int,
            projections: projections.as_ptr(),
            projection_len: projections.len(),
            filters: filters.as_ptr(),
            filter_len: filters.len(),
            has_limit: self.limit.is_some() as c_int,
            limit: self.limit.unwrap_or_default(),
        };
        let status = (self.callback)(
            self.context.context,
            &request as *const ManagedScanRequest,
            &mut stream as *mut FFI_ArrowArrayStream,
        );
        if status != 0 {
            return Err(DataFusionError::Execution(format!(
                "managed table provider scan callback returned status {status}"
            )));
        }
        ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[derive(Debug)]
struct ManagedCallbackTable {
    schema: SchemaRef,
    callback: ScanCallback,
    context: Arc<ContextGuard>,
    supports_pushdown: bool,
}

#[async_trait::async_trait]
impl TableProvider for ManagedCallbackTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        if !self.supports_pushdown {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }

        Ok(filters
            .iter()
            .map(|filter| {
                if ManagedFilterOwned::try_from_expr(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let pushed_projection = if self.supports_pushdown {
            projection
        } else {
            None
        };
        let pushed_limit = if self.supports_pushdown { limit } else { None };
        let projected_schema = match pushed_projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => Arc::clone(&self.schema),
        };
        let projections = match pushed_projection {
            Some(projection) => projection
                .iter()
                .map(|idx| CString::new(self.schema.field(*idx).name().as_str()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            None => vec![],
        };
        let has_projection = pushed_projection.is_some();
        let filters = if self.supports_pushdown {
            filters
                .iter()
                .filter_map(ManagedFilterOwned::try_from_expr)
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let scan = Arc::new(ManagedScan {
            schema: Arc::clone(&projected_schema),
            callback: self.callback,
            context: Arc::clone(&self.context),
            has_projection,
            projections,
            filters,
            limit: pushed_limit,
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            projected_schema,
            vec![scan],
            if self.supports_pushdown {
                None
            } else {
                projection
            },
            LexOrdering::new(vec![]),
            false,
            if self.supports_pushdown { None } else { limit },
        )?))
    }
}

#[derive(Debug)]
struct ManagedFilterOwned {
    column: CString,
    op: c_int,
    value_kind: c_int,
    value: CString,
}

impl ManagedFilterOwned {
    fn as_ffi(&self) -> ManagedFilter {
        ManagedFilter {
            column: self.column.as_ptr(),
            op: self.op,
            value_kind: self.value_kind,
            value: self.value.as_ptr(),
        }
    }

    fn try_from_expr(expr: &Expr) -> Option<Self> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        if let Some(filter) = Self::try_from_parts(&binary.left, binary.op, &binary.right) {
            return Some(filter);
        }
        Self::try_from_parts(&binary.right, reverse_operator(binary.op)?, &binary.left)
    }

    fn try_from_parts(left: &Expr, op: Operator, right: &Expr) -> Option<Self> {
        let Expr::Column(column) = left else {
            return None;
        };
        let Expr::Literal(value, _) = right else {
            return None;
        };
        let op = operator_code(op)?;
        let (value_kind, value) = scalar_value(value)?;
        Some(Self {
            column: CString::new(column.name.as_str()).ok()?,
            op,
            value_kind,
            value: CString::new(value).ok()?,
        })
    }
}

fn operator_code(op: Operator) -> Option<c_int> {
    match op {
        Operator::Eq => Some(0),
        Operator::NotEq => Some(1),
        Operator::Lt => Some(2),
        Operator::LtEq => Some(3),
        Operator::Gt => Some(4),
        Operator::GtEq => Some(5),
        _ => None,
    }
}

fn reverse_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn scalar_value(value: &ScalarValue) -> Option<(c_int, String)> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some((0, v.to_string())),
        ScalarValue::Int8(Some(v)) => Some((1, v.to_string())),
        ScalarValue::Int16(Some(v)) => Some((1, v.to_string())),
        ScalarValue::Int32(Some(v)) => Some((1, v.to_string())),
        ScalarValue::Int64(Some(v)) => Some((1, v.to_string())),
        ScalarValue::UInt8(Some(v)) => Some((1, v.to_string())),
        ScalarValue::UInt16(Some(v)) => Some((1, v.to_string())),
        ScalarValue::UInt32(Some(v)) => Some((1, v.to_string())),
        ScalarValue::UInt64(Some(v)) => Some((1, v.to_string())),
        ScalarValue::Float32(Some(v)) => Some((2, v.to_string())),
        ScalarValue::Float64(Some(v)) => Some((2, v.to_string())),
        ScalarValue::Utf8(Some(v)) => Some((3, v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some((3, v.clone())),
        _ => None,
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
    supports_pushdown: c_int,
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
        register_callback_table(
            ctx,
            name,
            schema_ptr,
            schema_len,
            supports_pushdown,
            callback,
            guard,
        )
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_register_callback_table_in_schema(
    handle: *mut SessionContext,
    schema_name: *const c_char,
    table_name: *const c_char,
    schema_ptr: *const u8,
    schema_len: usize,
    supports_pushdown: c_int,
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
        let schema_name = cstr(schema_name, "schema_name")?;
        let table_name = cstr(table_name, "table_name")?;
        register_default_schema(ctx, &schema_name)?;
        register_callback_table(
            ctx,
            TableReference::partial(schema_name, table_name),
            schema_ptr,
            schema_len,
            supports_pushdown,
            callback,
            guard,
        )
    })
}

fn register_callback_table(
    ctx: &SessionContext,
    table_ref: impl Into<TableReference>,
    schema_ptr: *const u8,
    schema_len: usize,
    supports_pushdown: c_int,
    callback: ScanCallback,
    guard: ContextGuard,
) -> crate::NativeResult<()> {
        let schema: SchemaRef = Arc::new(
            decode_optional_schema(schema_ptr, schema_len)?
                .ok_or("callback table requires a non-empty schema")?,
        );
        let table = ManagedCallbackTable {
            schema,
            callback,
            context: Arc::new(guard),
            supports_pushdown: supports_pushdown != 0,
        };
        ctx.register_table(table_ref, Arc::new(table))?;
        Ok(())
}

fn register_default_schema(ctx: &SessionContext, schema_name: &str) -> crate::NativeResult<()> {
    let state = ctx.state();
    let default_catalog = state
        .config()
        .options()
        .catalog
        .default_catalog
        .as_str();
    let catalog = ctx
        .catalog(default_catalog)
        .ok_or_else(|| format!("failed to resolve catalog: {default_catalog}"))?;
    if catalog.schema(schema_name).is_none() {
        catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;
    }
    Ok(())
}
