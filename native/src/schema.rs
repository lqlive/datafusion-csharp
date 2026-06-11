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

use ::arrow::array::RecordBatch;
use ::arrow::datatypes::{Schema, SchemaRef};
use ::arrow::error::ArrowError;
use ::arrow::ipc::reader::StreamReader;
use ::arrow::ipc::writer::StreamWriter;
use ::arrow::record_batch::RecordBatchIterator;

use crate::{bytes, NativeResult};

pub(crate) fn schema_ipc(schema: SchemaRef) -> NativeResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
    writer.finish()?;
    Ok(buf)
}

pub(crate) fn decode_optional_schema(
    schema_ptr: *const u8,
    schema_len: usize,
) -> NativeResult<Option<Schema>> {
    let schema_bytes = bytes(schema_ptr, schema_len)?;
    if schema_bytes.is_empty() {
        return Ok(None);
    }

    let reader = StreamReader::try_new(std::io::Cursor::new(schema_bytes), None)?;
    Ok(Some((*reader.schema()).clone()))
}

pub(crate) fn batches_ipc(schema: SchemaRef, batches: Vec<RecordBatch>) -> NativeResult<Vec<u8>> {
    let mut buf = Vec::new();
    let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
    for batch in iter {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(buf)
}

pub(crate) fn read_ipc_batches(ipc: &[u8]) -> NativeResult<(SchemaRef, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((schema, batches))
}
