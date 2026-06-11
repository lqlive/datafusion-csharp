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

use std::cell::RefCell;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

use ::arrow::datatypes::SchemaRef;
use ::arrow::error::ArrowError;
use datafusion::common::config::{CsvOptions, JsonOptions};
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{JoinType, UnnestOptions};
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{col, Expr, Partitioning, SortExpr};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use prost::Message;
use tokio::runtime::Runtime;

mod arrow;
mod avro;
mod cache_manager;
mod clickhouse;
mod csv;
mod json;
mod memory;
mod mongodb;
mod mysql;
mod object_store;
mod parquet;
mod postgres;
mod proto;
mod runtime_metrics;
mod schema;
mod sqlite;
mod table_provider;
mod udf;

mod proto_gen {
    include!(concat!(env!("OUT_DIR"), "/datafusion.rs"));
}

use crate::arrow::arrow_options;
use crate::avro::avro_options;
use crate::csv::csv_options;
use crate::json::json_options;
use crate::parquet::parquet_options;
use proto_gen::{
    CsvWriteOptionsProto, FileCompressionType as ProtoCompression, JsonWriteOptionsProto,
    SessionOptions,
};
use schema::{batches_ipc, schema_ipc};

pub(crate) type NativeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) const OK: c_int = 0;
const ERROR: c_int = 1;
const PANIC: c_int = 2;

const EXCEPTION_DATAFUSION: c_int = 0;
const EXCEPTION_PLAN: c_int = 1;
const EXCEPTION_EXECUTION: c_int = 2;
const EXCEPTION_RESOURCES_EXHAUSTED: c_int = 3;
const EXCEPTION_IO: c_int = 4;
const EXCEPTION_NOT_IMPLEMENTED: c_int = 5;
const EXCEPTION_CONFIGURATION: c_int = 6;

#[derive(Clone)]
struct LastError {
    kind: c_int,
    message: String,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<LastError>> = const { RefCell::new(None) };
}

#[repr(C)]
pub struct DfByteBuffer {
    ptr: *mut u8,
    len: usize,
}

#[repr(C)]
pub struct DfStringArray {
    ptr: *const *const c_char,
    len: usize,
}

pub(crate) fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        let rt = Runtime::new().expect("failed to create Tokio runtime");
        runtime_metrics::init(rt.handle());
        rt
    })
}

fn install_memory_tracker(builder: &mut RuntimeEnvBuilder) -> Arc<memory::TrackingMemoryPool> {
    let inner: Arc<dyn MemoryPool> = builder
        .memory_pool
        .take()
        .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));
    let tracker = Arc::new(memory::TrackingMemoryPool::new(inner));
    builder.memory_pool = Some(tracker.clone());
    tracker
}

fn set_error(kind: c_int, message: impl Into<String>) -> c_int {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(LastError {
            kind,
            message: message.into(),
        });
    });
    ERROR
}

fn set_panic(message: String) -> c_int {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(LastError {
            kind: EXCEPTION_DATAFUSION,
            message: format!("panic: {message}"),
        });
    });
    PANIC
}

pub(crate) fn take_result(f: impl FnOnce() -> NativeResult<()>) -> c_int {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => OK,
        Ok(Err(err)) => classify_boxed_error(err),
        Err(panic) => set_panic(panic_message(&panic)),
    }
}

fn classify_boxed_error(err: Box<dyn Error + Send + Sync>) -> c_int {
    match err.downcast::<DataFusionError>() {
        Ok(df_err) => set_error(classify_datafusion(df_err.find_root()), df_err.to_string()),
        Err(other) => set_error(EXCEPTION_DATAFUSION, other.to_string()),
    }
}

fn classify_datafusion(err: &DataFusionError) -> c_int {
    match err {
        DataFusionError::Plan(_)
        | DataFusionError::SQL(_, _)
        | DataFusionError::SchemaError(_, _) => EXCEPTION_PLAN,
        DataFusionError::Execution(_)
        | DataFusionError::ExecutionJoin(_)
        | DataFusionError::External(_)
        | DataFusionError::Ffi(_) => EXCEPTION_EXECUTION,
        DataFusionError::ResourcesExhausted(_) => EXCEPTION_RESOURCES_EXHAUSTED,
        DataFusionError::IoError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::ParquetError(_)
        | DataFusionError::AvroError(_) => EXCEPTION_IO,
        DataFusionError::ArrowError(arrow_err, _) => classify_arrow(arrow_err),
        DataFusionError::NotImplemented(_) => EXCEPTION_NOT_IMPLEMENTED,
        DataFusionError::Configuration(_) => EXCEPTION_CONFIGURATION,
        _ => EXCEPTION_DATAFUSION,
    }
}

fn classify_arrow(err: &ArrowError) -> c_int {
    match err {
        ArrowError::IoError(_, _) | ArrowError::IpcError(_) => EXCEPTION_IO,
        ArrowError::SchemaError(_) | ArrowError::ParseError(_) => EXCEPTION_PLAN,
        ArrowError::DivideByZero
        | ArrowError::ArithmeticOverflow(_)
        | ArrowError::ComputeError(_)
        | ArrowError::CastError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::MemoryError(_)
        | ArrowError::CsvError(_)
        | ArrowError::JsonError(_)
        | ArrowError::AvroError(_)
        | ArrowError::ParquetError(_)
        | ArrowError::ExternalError(_) => EXCEPTION_EXECUTION,
        ArrowError::NotYetImplemented(_) => EXCEPTION_NOT_IMPLEMENTED,
        _ => EXCEPTION_DATAFUSION,
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "rust panic with non-string payload".to_string()
    }
}

pub(crate) fn require_ptr<T>(ptr: *mut T, name: &str) -> NativeResult<*mut T> {
    if ptr.is_null() {
        Err(format!("{name} is null").into())
    } else {
        Ok(ptr)
    }
}

pub(crate) fn cstr(ptr: *const c_char, name: &str) -> NativeResult<String> {
    if ptr.is_null() {
        return Err(format!("{name} is null").into());
    }
    Ok(unsafe { CStr::from_ptr(ptr) }.to_str()?.to_owned())
}

pub(crate) fn bytes<'a>(ptr: *const u8, len: usize) -> NativeResult<&'a [u8]> {
    if ptr.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            Err("byte pointer is null with non-zero length".into())
        }
    } else {
        Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
    }
}

fn strings(array: DfStringArray) -> NativeResult<Vec<String>> {
    if array.ptr.is_null() && array.len != 0 {
        return Err("string array pointer is null with non-zero length".into());
    }
    let raw = unsafe { std::slice::from_raw_parts(array.ptr, array.len) };
    raw.iter()
        .map(|item| cstr(*item, "string array item"))
        .collect()
}

pub(crate) fn write_handle<T>(out: *mut *mut T, value: T) -> NativeResult<()> {
    if out.is_null() {
        return Err("output handle pointer is null".into());
    }
    unsafe {
        *out = Box::into_raw(Box::new(value));
    }
    Ok(())
}

pub(crate) fn write_buffer(out: *mut DfByteBuffer, data: Vec<u8>) -> NativeResult<()> {
    if out.is_null() {
        return Err("output buffer pointer is null".into());
    }
    let mut data = data.into_boxed_slice();
    let buffer = DfByteBuffer {
        ptr: data.as_mut_ptr(),
        len: data.len(),
    };
    std::mem::forget(data);
    unsafe {
        *out = buffer;
    }
    Ok(())
}

pub(crate) fn proto_compression(value: ProtoCompression) -> NativeResult<FileCompressionType> {
    match value {
        ProtoCompression::Unspecified => Err("file_compression_type is UNSPECIFIED".into()),
        ProtoCompression::Uncompressed => Ok(FileCompressionType::UNCOMPRESSED),
        ProtoCompression::Gzip => Ok(FileCompressionType::GZIP),
        ProtoCompression::Bzip2 => Ok(FileCompressionType::BZIP2),
        ProtoCompression::Xz => Ok(FileCompressionType::XZ),
        ProtoCompression::Zstd => Ok(FileCompressionType::ZSTD),
    }
}

fn proto_compression_variant(value: ProtoCompression) -> NativeResult<CompressionTypeVariant> {
    match value {
        ProtoCompression::Unspecified => Err("file_compression_type is UNSPECIFIED".into()),
        ProtoCompression::Uncompressed => Ok(CompressionTypeVariant::UNCOMPRESSED),
        ProtoCompression::Gzip => Ok(CompressionTypeVariant::GZIP),
        ProtoCompression::Bzip2 => Ok(CompressionTypeVariant::BZIP2),
        ProtoCompression::Xz => Ok(CompressionTypeVariant::XZ),
        ProtoCompression::Zstd => Ok(CompressionTypeVariant::ZSTD),
    }
}

fn join_type(code: c_uchar) -> NativeResult<JoinType> {
    match code {
        0 => Ok(JoinType::Inner),
        1 => Ok(JoinType::Left),
        2 => Ok(JoinType::Right),
        3 => Ok(JoinType::Full),
        4 => Ok(JoinType::LeftSemi),
        5 => Ok(JoinType::RightSemi),
        6 => Ok(JoinType::LeftAnti),
        7 => Ok(JoinType::RightAnti),
        8 => Ok(JoinType::LeftMark),
        9 => Ok(JoinType::RightMark),
        other => Err(format!("unknown join type code: {other}").into()),
    }
}

fn combine_schemas(
    left: &datafusion::common::DFSchema,
    right: &datafusion::common::DFSchema,
) -> datafusion::common::DFSchema {
    let mut combined = left.clone();
    combined.merge(right);
    combined
}

#[no_mangle]
pub extern "C" fn df_last_error_kind() -> c_int {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(EXCEPTION_DATAFUSION, |e| e.kind)
    })
}

#[no_mangle]
pub extern "C" fn df_last_error_message() -> *mut c_char {
    LAST_ERROR.with(|slot| {
        let message = slot
            .borrow()
            .as_ref()
            .map_or_else(String::new, |e| e.message.clone());
        CString::new(message)
            .unwrap_or_else(|_| {
                CString::new("error message contained nul byte").expect("literal is valid")
            })
            .into_raw()
    })
}

#[no_mangle]
pub extern "C" fn df_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

#[no_mangle]
pub extern "C" fn df_byte_buffer_free(buffer: DfByteBuffer) {
    if !buffer.ptr.is_null() {
        unsafe {
            drop(Box::from_raw(std::slice::from_raw_parts_mut(
                buffer.ptr, buffer.len,
            )));
        }
    }
}

#[no_mangle]
pub extern "C" fn df_session_context_new(
    out: *mut *mut datafusion::prelude::SessionContext,
) -> c_int {
    take_result(|| {
        let mut runtime_builder = RuntimeEnvBuilder::new();
        let tracker = install_memory_tracker(&mut runtime_builder);
        let runtime_env = runtime_builder.build()?;
        let ctx = datafusion::prelude::SessionContext::new_with_config_rt(
            SessionConfig::new(),
            Arc::new(runtime_env),
        );
        if out.is_null() {
            return Err("output handle pointer is null".into());
        }
        let handle = Box::into_raw(Box::new(ctx));
        memory::register(handle as usize, tracker);
        unsafe {
            *out = handle;
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_new_with_options(
    options_ptr: *const u8,
    options_len: usize,
    out: *mut *mut datafusion::prelude::SessionContext,
) -> c_int {
    take_result(|| {
        let opts = SessionOptions::decode(bytes(options_ptr, options_len)?)?;
        let mut config = SessionConfig::new();
        if let Some(v) = opts.batch_size {
            config = config.with_batch_size(v as usize);
        }
        if let Some(v) = opts.target_partitions {
            config = config.with_target_partitions(v as usize);
        }
        if let Some(v) = opts.collect_statistics {
            config = config.with_collect_statistics(v);
        }
        if let Some(v) = opts.information_schema {
            config = config.with_information_schema(v);
        }
        for opt in opts.options {
            if opt.key.starts_with("datafusion.runtime.") {
                return Err(format!(
                    "datafusion.runtime.* keys are not supported via SetOption yet: {}",
                    opt.key
                )
                .into());
            }
            config.options_mut().set(&opt.key, &opt.value)?;
        }

        let mut runtime_builder = RuntimeEnvBuilder::new();
        if let Some(mem) = opts.memory_limit {
            runtime_builder = runtime_builder
                .with_memory_limit(mem.max_memory_bytes as usize, mem.memory_fraction);
        }
        if let Some(dir) = opts.temp_directory {
            runtime_builder = runtime_builder.with_temp_file_path(dir);
        }
        if let Some(dm) = opts.disk_manager {
            if dm.disabled.unwrap_or(false) {
                let builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
                runtime_builder = runtime_builder.with_disk_manager_builder(builder);
            }
            if let Some(size) = dm.max_temp_directory_size {
                runtime_builder = runtime_builder.with_max_temp_directory_size(size);
            }
        }
        if let Some(cache_manager_options) = opts.cache_manager.as_ref() {
            if let Some(cache_manager_config) = cache_manager::build_config(cache_manager_options)?
            {
                runtime_builder = runtime_builder.with_cache_manager(cache_manager_config);
            }
        }
        let tracker = install_memory_tracker(&mut runtime_builder);
        let runtime_env = runtime_builder.build()?;
        let ctx =
            datafusion::prelude::SessionContext::new_with_config_rt(config, Arc::new(runtime_env));
        object_store::apply_registrations(&ctx, &opts.object_stores)?;
        if out.is_null() {
            return Err("output handle pointer is null".into());
        }
        let handle = Box::into_raw(Box::new(ctx));
        memory::register(handle as usize, tracker);
        unsafe {
            *out = handle;
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_free(
    handle: *mut datafusion::prelude::SessionContext,
) -> c_int {
    take_result(|| {
        if !handle.is_null() {
            memory::unregister(handle as usize);
            unsafe {
                drop(Box::from_raw(handle));
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_sql(
    handle: *mut datafusion::prelude::SessionContext,
    sql: *const c_char,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let sql = cstr(sql, "sql")?;
        let df = runtime().block_on(ctx.sql(&sql))?;
        write_handle(out, df)
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_table_schema_ipc(
    handle: *mut datafusion::prelude::SessionContext,
    table_name: *const c_char,
    out: *mut DfByteBuffer,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let table_name = cstr(table_name, "table_name")?;
        let table = runtime().block_on(ctx.table_provider(&table_name))?;
        write_buffer(out, schema_ipc(table.schema())?)
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_get_option(
    handle: *mut datafusion::prelude::SessionContext,
    key: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    take_result(|| {
        if out.is_null() {
            return Err("output string pointer is null".into());
        }
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let key = cstr(key, "key")?;
        if key.starts_with("datafusion.runtime.") {
            return Err(format!(
                "datafusion.runtime.* keys are not supported via GetOption yet; use SessionContextBuilder typed setters instead. Got: {key}"
            )
            .into());
        }

        let config = ctx.copied_config();
        for entry in config.options().entries() {
            if entry.key == key {
                let ptr = match entry.value {
                    Some(value) => CString::new(value)?.into_raw(),
                    None => std::ptr::null_mut(),
                };
                unsafe {
                    *out = ptr;
                }
                return Ok(());
            }
        }

        Err(format!("unknown DataFusion config key: {key}").into())
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_memory_usage(
    handle: *mut datafusion::prelude::SessionContext,
    out: *mut DfByteBuffer,
) -> c_int {
    take_result(|| {
        require_ptr(handle, "SessionContext handle")?;
        let tracker = memory::lookup(handle as usize)
            .ok_or("memory tracker not registered for this SessionContext")?;
        let (current, peak) = tracker.snapshot();
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&current.to_le_bytes());
        bytes.extend_from_slice(&peak.to_le_bytes());
        write_buffer(out, bytes)
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_runtime_stats(
    handle: *mut datafusion::prelude::SessionContext,
    out: *mut DfByteBuffer,
) -> c_int {
    take_result(|| {
        require_ptr(handle, "SessionContext handle")?;
        let stats = runtime_metrics::runtime_stats()?;
        let mut bytes = Vec::with_capacity(stats.len() * std::mem::size_of::<i64>());
        for value in stats {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        write_buffer(out, bytes)
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_register_parquet(
    handle: *mut datafusion::prelude::SessionContext,
    name: *const c_char,
    path: *const c_char,
    options_ptr: *const u8,
    options_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let name = cstr(name, "name")?;
        let path = cstr(path, "path")?;
        let options = parquet_options(options_ptr, options_len, schema_ptr, schema_len)?;
        runtime().block_on(ctx.register_parquet(&name, &path, options))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_session_context_read_parquet(
    handle: *mut datafusion::prelude::SessionContext,
    path: *const c_char,
    options_ptr: *const u8,
    options_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
        let path = cstr(path, "path")?;
        let options = parquet_options(options_ptr, options_len, schema_ptr, schema_len)?;
        let df = runtime().block_on(ctx.read_parquet(path, options))?;
        write_handle(out, df)
    })
}

macro_rules! read_register_format {
    ($register_fn:ident, $read_fn:ident, $options_fn:ident, $ctx_register:ident, $ctx_read:ident) => {
        #[no_mangle]
        pub extern "C" fn $register_fn(
            handle: *mut datafusion::prelude::SessionContext,
            name: *const c_char,
            path: *const c_char,
            options_ptr: *const u8,
            options_len: usize,
            schema_ptr: *const u8,
            schema_len: usize,
        ) -> c_int {
            take_result(|| {
                let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
                let name = cstr(name, "name")?;
                let path = cstr(path, "path")?;
                let options = $options_fn(options_ptr, options_len, schema_ptr, schema_len)?;
                runtime().block_on(ctx.$ctx_register(&name, &path, options))?;
                Ok(())
            })
        }

        #[no_mangle]
        pub extern "C" fn $read_fn(
            handle: *mut datafusion::prelude::SessionContext,
            path: *const c_char,
            options_ptr: *const u8,
            options_len: usize,
            schema_ptr: *const u8,
            schema_len: usize,
            out: *mut *mut DataFrame,
        ) -> c_int {
            take_result(|| {
                let ctx = unsafe { &*require_ptr(handle, "SessionContext handle")? };
                let path = cstr(path, "path")?;
                let options = $options_fn(options_ptr, options_len, schema_ptr, schema_len)?;
                let df = runtime().block_on(ctx.$ctx_read(path, options))?;
                write_handle(out, df)
            })
        }
    };
}

read_register_format!(
    df_session_context_register_csv,
    df_session_context_read_csv,
    csv_options,
    register_csv,
    read_csv
);
read_register_format!(
    df_session_context_register_json,
    df_session_context_read_json,
    json_options,
    register_json,
    read_json
);
read_register_format!(
    df_session_context_register_arrow,
    df_session_context_read_arrow,
    arrow_options,
    register_arrow,
    read_arrow
);
read_register_format!(
    df_session_context_register_avro,
    df_session_context_read_avro,
    avro_options,
    register_avro,
    read_avro
);

#[no_mangle]
pub extern "C" fn df_dataframe_free(handle: *mut DataFrame) -> c_int {
    take_result(|| {
        if !handle.is_null() {
            unsafe {
                drop(Box::from_raw(handle));
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_schema_ipc(handle: *mut DataFrame, out: *mut DfByteBuffer) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? };
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        write_buffer(out, schema_ipc(schema)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_collect_ipc(
    handle: *mut DataFrame,
    out: *mut DfByteBuffer,
) -> c_int {
    take_result(|| {
        let df = unsafe { *Box::from_raw(require_ptr(handle, "DataFrame handle")?) };
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let batches = runtime().block_on(df.collect())?;
        write_buffer(out, batches_ipc(schema, batches)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_execute_stream_ipc(
    handle: *mut DataFrame,
    out: *mut DfByteBuffer,
) -> c_int {
    take_result(|| {
        let df = unsafe { *Box::from_raw(require_ptr(handle, "DataFrame handle")?) };
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let mut stream = runtime().block_on(df.execute_stream())?;
        let batches = runtime().block_on(async {
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            Ok::<_, DataFusionError>(batches)
        })?;
        write_buffer(out, batches_ipc(schema, batches)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_count(handle: *mut DataFrame, out: *mut u64) -> c_int {
    take_result(|| {
        if out.is_null() {
            return Err("output count pointer is null".into());
        }
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let count = runtime().block_on(df.count())?;
        unsafe {
            *out = count as u64;
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_show(handle: *mut DataFrame, limit: i32) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        if limit < 0 {
            runtime().block_on(df.show())?;
        } else {
            runtime().block_on(df.show_limit(limit as usize))?;
        }
        Ok(())
    })
}

fn write_df(out: *mut *mut DataFrame, df: DataFrame) -> NativeResult<()> {
    write_handle(out, df)
}

#[no_mangle]
pub extern "C" fn df_dataframe_explain(
    handle: *mut DataFrame,
    verbose: bool,
    analyze: bool,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(out, df.explain(verbose, analyze)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_cache(handle: *mut DataFrame, out: *mut *mut DataFrame) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(out, runtime().block_on(df.cache())?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_describe(handle: *mut DataFrame, out: *mut *mut DataFrame) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(out, runtime().block_on(df.describe())?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_select(
    handle: *mut DataFrame,
    columns: DfStringArray,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let owned = strings(columns)?;
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        write_df(out, df.select_columns(&refs)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_filter(
    handle: *mut DataFrame,
    predicate: *const c_char,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let predicate = cstr(predicate, "predicate")?;
        let expr = df.parse_sql_expr(&predicate)?;
        write_df(out, df.filter(expr)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_limit(
    handle: *mut DataFrame,
    skip: usize,
    fetch: usize,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(out, df.limit(skip, Some(fetch))?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_distinct(handle: *mut DataFrame, out: *mut *mut DataFrame) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(out, df.distinct()?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_drop_columns(
    handle: *mut DataFrame,
    columns: DfStringArray,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let owned = strings(columns)?;
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        write_df(out, df.drop_columns(&refs)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_rename_column(
    handle: *mut DataFrame,
    old_name: *const c_char,
    new_name: *const c_char,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let old_name = cstr(old_name, "old_name")?;
        let new_name = cstr(new_name, "new_name")?;
        write_df(out, df.with_column_renamed(&old_name, &new_name)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_with_column(
    handle: *mut DataFrame,
    name: *const c_char,
    expr: *const c_char,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let name = cstr(name, "name")?;
        let expr = cstr(expr, "expr")?;
        let parsed = df.parse_sql_expr(&expr)?;
        write_df(out, df.with_column(&name, parsed)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_unnest_columns(
    handle: *mut DataFrame,
    columns: DfStringArray,
    preserve_nulls: bool,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let owned = strings(columns)?;
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        let opts = UnnestOptions::new().with_preserve_nulls(preserve_nulls);
        write_df(out, df.unnest_columns_with_options(&refs, opts)?)
    })
}

macro_rules! set_op {
    ($name:ident, $method:ident) => {
        #[no_mangle]
        pub extern "C" fn $name(
            left: *mut DataFrame,
            right: *mut DataFrame,
            out: *mut *mut DataFrame,
        ) -> c_int {
            take_result(|| {
                let left = unsafe { &*require_ptr(left, "left DataFrame handle")? }.clone();
                let right = unsafe { &*require_ptr(right, "right DataFrame handle")? }.clone();
                write_df(out, left.$method(right)?)
            })
        }
    };
}

set_op!(df_dataframe_union, union);
set_op!(df_dataframe_union_distinct, union_distinct);
set_op!(df_dataframe_union_by_name, union_by_name);
set_op!(df_dataframe_union_by_name_distinct, union_by_name_distinct);
set_op!(df_dataframe_intersect, intersect);
set_op!(df_dataframe_intersect_distinct, intersect_distinct);
set_op!(df_dataframe_except, except);
set_op!(df_dataframe_except_distinct, except_distinct);

#[no_mangle]
pub extern "C" fn df_dataframe_sort(
    handle: *mut DataFrame,
    columns: DfStringArray,
    ascending_ptr: *const bool,
    nulls_first_ptr: *const bool,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let names = strings(columns)?;
        if ascending_ptr.is_null() || nulls_first_ptr.is_null() {
            return Err("sort boolean array pointer is null".into());
        }
        let ascending = unsafe { std::slice::from_raw_parts(ascending_ptr, names.len()) };
        let nulls_first = unsafe { std::slice::from_raw_parts(nulls_first_ptr, names.len()) };
        let exprs: Vec<SortExpr> = names
            .into_iter()
            .zip(ascending.iter().zip(nulls_first.iter()))
            .map(|(name, (asc, nf))| SortExpr::new(col(&name), *asc, *nf))
            .collect();
        write_df(out, df.sort(exprs)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_repartition_round_robin(
    handle: *mut DataFrame,
    partitions: usize,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        write_df(
            out,
            df.repartition(Partitioning::RoundRobinBatch(partitions))?,
        )
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_repartition_hash(
    handle: *mut DataFrame,
    partitions: usize,
    columns: DfStringArray,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let owned = strings(columns)?;
        let exprs = owned.iter().map(|s| col(s.as_str())).collect();
        write_df(out, df.repartition(Partitioning::Hash(exprs, partitions))?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_join(
    left: *mut DataFrame,
    right: *mut DataFrame,
    join_type_code: c_uchar,
    left_columns: DfStringArray,
    right_columns: DfStringArray,
    filter: *const c_char,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let left = unsafe { &*require_ptr(left, "left DataFrame handle")? }.clone();
        let right = unsafe { &*require_ptr(right, "right DataFrame handle")? }.clone();
        let left_owned = strings(left_columns)?;
        let right_owned = strings(right_columns)?;
        let left_refs: Vec<&str> = left_owned.iter().map(String::as_str).collect();
        let right_refs: Vec<&str> = right_owned.iter().map(String::as_str).collect();
        let filter_expr: Option<Expr> = if filter.is_null() {
            None
        } else {
            let filter_sql = cstr(filter, "filter")?;
            let combined = combine_schemas(left.schema(), right.schema());
            let (state, _) = left.clone().into_parts();
            Some(state.create_logical_expr(&filter_sql, &combined)?)
        };
        write_df(
            out,
            left.join(
                right,
                join_type(join_type_code)?,
                &left_refs,
                &right_refs,
                filter_expr,
            )?,
        )
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_join_on(
    left: *mut DataFrame,
    right: *mut DataFrame,
    join_type_code: c_uchar,
    predicates: DfStringArray,
    out: *mut *mut DataFrame,
) -> c_int {
    take_result(|| {
        let left = unsafe { &*require_ptr(left, "left DataFrame handle")? }.clone();
        let right = unsafe { &*require_ptr(right, "right DataFrame handle")? }.clone();
        let predicate_strings = strings(predicates)?;
        let combined = combine_schemas(left.schema(), right.schema());
        let (state, _) = left.clone().into_parts();
        let exprs: Vec<Expr> = predicate_strings
            .iter()
            .map(|sql| state.create_logical_expr(sql, &combined))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        write_df(out, left.join_on(right, join_type(join_type_code)?, exprs)?)
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_write_parquet(
    handle: *mut DataFrame,
    path: *const c_char,
    compression: *const c_char,
    single_file_output: bool,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let path = cstr(path, "path")?;
        let write_opts = DataFrameWriteOptions::new().with_single_file_output(single_file_output);
        let writer_opts = if compression.is_null() {
            None
        } else {
            let mut opts = TableParquetOptions::default();
            opts.global.compression = Some(cstr(compression, "compression")?);
            Some(opts)
        };
        runtime().block_on(df.write_parquet(&path, write_opts, writer_opts))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_write_csv(
    handle: *mut DataFrame,
    path: *const c_char,
    options_ptr: *const u8,
    options_len: usize,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let path = cstr(path, "path")?;
        let p = CsvWriteOptionsProto::decode(bytes(options_ptr, options_len)?)?;
        let mut write_opts = DataFrameWriteOptions::new()
            .with_single_file_output(p.single_file_output.unwrap_or(false));
        if !p.partition_cols.is_empty() {
            write_opts = write_opts.with_partition_by(p.partition_cols.clone());
        }

        let compression = if p.file_compression_type.is_some() {
            Some(proto_compression_variant(p.file_compression_type())?)
        } else {
            None
        };
        let writer_opts = if p.has_header.is_some()
            || p.delimiter.is_some()
            || p.quote.is_some()
            || p.escape.is_some()
            || p.null_value.is_some()
            || compression.is_some()
        {
            let mut opts = CsvOptions::default();
            if let Some(v) = p.has_header {
                opts = opts.with_has_header(v);
            }
            if let Some(v) = p.delimiter {
                opts = opts.with_delimiter(v as u8);
            }
            if let Some(v) = p.quote {
                opts = opts.with_quote(v as u8);
            }
            if let Some(v) = p.escape {
                opts = opts.with_escape(Some(v as u8));
            }
            if let Some(v) = p.null_value {
                opts.null_value = Some(v);
            }
            if let Some(v) = compression {
                opts = opts.with_file_compression_type(v);
            }
            Some(opts)
        } else {
            None
        };

        runtime().block_on(df.write_csv(&path, write_opts, writer_opts))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_dataframe_write_json(
    handle: *mut DataFrame,
    path: *const c_char,
    options_ptr: *const u8,
    options_len: usize,
) -> c_int {
    take_result(|| {
        let df = unsafe { &*require_ptr(handle, "DataFrame handle")? }.clone();
        let path = cstr(path, "path")?;
        let p = JsonWriteOptionsProto::decode(bytes(options_ptr, options_len)?)?;
        let mut write_opts = DataFrameWriteOptions::new()
            .with_single_file_output(p.single_file_output.unwrap_or(false));
        if !p.partition_cols.is_empty() {
            write_opts = write_opts.with_partition_by(p.partition_cols.clone());
        }

        let writer_opts = if p.file_compression_type.is_some() {
            Some(JsonOptions {
                compression: proto_compression_variant(p.file_compression_type())?,
                ..JsonOptions::default()
            })
        } else {
            None
        };

        runtime().block_on(df.write_json(&path, write_opts, writer_opts))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_not_implemented(message: *const c_char) -> c_int {
    take_result(|| Err::<(), _>(DataFusionError::NotImplemented(cstr(message, "message")?).into()))
}
