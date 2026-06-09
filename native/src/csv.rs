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

use datafusion::prelude::CsvReadOptions;
use prost::Message;

use crate::proto_gen::CsvReadOptionsProto;
use crate::schema::decode_optional_schema;
use crate::{bytes, proto_compression, NativeResult};

pub(crate) fn csv_options(
    options_ptr: *const u8,
    options_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> NativeResult<CsvReadOptions<'static>> {
    let p = CsvReadOptionsProto::decode(bytes(options_ptr, options_len)?)?;
    let schema = decode_optional_schema(schema_ptr, schema_len)?;
    let compression = proto_compression(p.file_compression_type())?;
    let ext: &'static str = Box::leak(p.file_extension.into_boxed_str());
    let mut opts = CsvReadOptions::new()
        .has_header(p.has_header)
        .delimiter(p.delimiter as u8)
        .quote(p.quote as u8)
        .file_extension(ext)
        .file_compression_type(compression);
    if let Some(v) = p.terminator {
        opts = opts.terminator(Some(v as u8));
    }
    if let Some(v) = p.escape {
        opts = opts.escape(v as u8);
    }
    if let Some(v) = p.comment {
        opts = opts.comment(v as u8);
    }
    if let Some(v) = p.newlines_in_values {
        opts = opts.newlines_in_values(v);
    }
    if let Some(v) = p.schema_infer_max_records {
        opts = opts.schema_infer_max_records(v as usize);
    }
    if let Some(value) = schema {
        let leaked: &'static ::arrow::datatypes::Schema = Box::leak(Box::new(value));
        opts = opts.schema(leaked);
    }
    Ok(opts)
}
