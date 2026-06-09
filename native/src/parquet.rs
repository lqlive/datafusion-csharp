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

use datafusion::prelude::ParquetReadOptions;
use prost::Message;

use crate::proto_gen::ParquetReadOptionsProto;
use crate::schema::decode_optional_schema;
use crate::{bytes, NativeResult};

pub(crate) fn parquet_options(
    options_ptr: *const u8,
    options_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> NativeResult<ParquetReadOptions<'static>> {
    let p = ParquetReadOptionsProto::decode(bytes(options_ptr, options_len)?)?;
    let schema = decode_optional_schema(schema_ptr, schema_len)?;
    let ext: &'static str = Box::leak(p.file_extension.into_boxed_str());
    let mut opts = ParquetReadOptions::default().file_extension(ext);
    if let Some(v) = p.parquet_pruning {
        opts = opts.parquet_pruning(v);
    }
    if let Some(v) = p.skip_metadata {
        opts = opts.skip_metadata(v);
    }
    if let Some(v) = p.metadata_size_hint {
        opts = opts.metadata_size_hint(Some(v as usize));
    }
    if let Some(value) = schema {
        let leaked: &'static ::arrow::datatypes::Schema = Box::leak(Box::new(value));
        opts = opts.schema(leaked);
    }
    Ok(opts)
}
