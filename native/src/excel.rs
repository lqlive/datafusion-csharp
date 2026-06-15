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
use ::arrow::datatypes::SchemaRef;
use prost::Message;

use crate::proto_gen::ExcelReadOptionsProto;
use crate::schema::decode_optional_schema;
use crate::{bytes, NativeResult};

pub(crate) struct ExcelOptions {
    pub has_header: bool,
    pub sheet_name: Option<String>,
    pub schema_infer_max_records: Option<usize>,
}

fn decode_options(options_ptr: *const u8, options_len: usize) -> NativeResult<ExcelOptions> {
    let p = ExcelReadOptionsProto::decode(bytes(options_ptr, options_len)?)?;
    Ok(ExcelOptions {
        has_header: p.has_header,
        sheet_name: p.sheet_name,
        schema_infer_max_records: p.schema_infer_max_records.map(|v| v as usize),
    })
}

pub(crate) fn read_excel(
    path: &str,
    options_ptr: *const u8,
    options_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> NativeResult<(SchemaRef, Vec<RecordBatch>)> {
    if decode_optional_schema(schema_ptr, schema_len)?.is_some() {
        return Err(
            "explicit schema override is not supported for Excel reads yet; \
                    column types are inferred from the sheet"
                .into(),
        );
    }
    read_excel_impl(path, decode_options(options_ptr, options_len)?)
}

#[cfg(feature = "excel")]
fn read_excel_impl(
    path: &str,
    options: ExcelOptions,
) -> NativeResult<(SchemaRef, Vec<RecordBatch>)> {
    imp::read_workbook(path, options)
}

#[cfg(not(feature = "excel"))]
fn read_excel_impl(
    _path: &str,
    _options: ExcelOptions,
) -> NativeResult<(SchemaRef, Vec<RecordBatch>)> {
    Err(
        "datafusion_csharp_native was built without the `excel` Cargo feature; \
         rebuild the native crate with `--features excel` (it is enabled by default) \
         to read spreadsheets"
            .into(),
    )
}

#[cfg(feature = "excel")]
mod imp {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ::arrow::array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    };
    use ::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use ::arrow::record_batch::RecordBatch;
    use calamine::{open_workbook_auto, Data, DataType as _, Reader};

    use super::ExcelOptions;
    use crate::NativeResult;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ColKind {
        Empty,
        Int,
        Float,
        Bool,
        DateTime,
        Text,
    }

    impl ColKind {
        fn of(cell: &Data) -> ColKind {
            match cell {
                Data::Empty => ColKind::Empty,
                Data::Int(_) => ColKind::Int,
                Data::Float(_) => ColKind::Float,
                Data::Bool(_) => ColKind::Bool,
                Data::DateTime(_) => ColKind::DateTime,
                _ => ColKind::Text,
            }
        }

        fn widen(self, next: ColKind) -> ColKind {
            match (self, next) {
                (ColKind::Empty, other) | (other, ColKind::Empty) => other,
                (a, b) if a == b => a,
                (ColKind::Int, ColKind::Float) | (ColKind::Float, ColKind::Int) => ColKind::Float,
                _ => ColKind::Text,
            }
        }

        fn data_type(self) -> DataType {
            match self {
                ColKind::Int => DataType::Int64,
                ColKind::Float => DataType::Float64,
                ColKind::Bool => DataType::Boolean,
                ColKind::DateTime => DataType::Timestamp(TimeUnit::Millisecond, None),
                ColKind::Empty | ColKind::Text => DataType::Utf8,
            }
        }

        fn builder(self) -> ColBuilder {
            match self {
                ColKind::Int => ColBuilder::Int(Int64Builder::new()),
                ColKind::Float => ColBuilder::Float(Float64Builder::new()),
                ColKind::Bool => ColBuilder::Bool(BooleanBuilder::new()),
                ColKind::DateTime => ColBuilder::Timestamp(TimestampMillisecondBuilder::new()),
                ColKind::Empty | ColKind::Text => ColBuilder::Text(StringBuilder::new()),
            }
        }
    }

    enum ColBuilder {
        Int(Int64Builder),
        Float(Float64Builder),
        Bool(BooleanBuilder),
        Timestamp(TimestampMillisecondBuilder),
        Text(StringBuilder),
    }

    impl ColBuilder {
        fn append(&mut self, cell: &Data) {
            match self {
                ColBuilder::Int(b) => b.append_option(cell_to_i64(cell)),
                ColBuilder::Float(b) => b.append_option(cell_to_f64(cell)),
                ColBuilder::Bool(b) => b.append_option(cell_to_bool(cell)),
                ColBuilder::Timestamp(b) => b.append_option(cell_to_millis(cell)),
                ColBuilder::Text(b) => b.append_option(cell_to_string(cell)),
            }
        }

        fn finish(self) -> ArrayRef {
            match self {
                ColBuilder::Int(mut b) => Arc::new(b.finish()),
                ColBuilder::Float(mut b) => Arc::new(b.finish()),
                ColBuilder::Bool(mut b) => Arc::new(b.finish()),
                ColBuilder::Timestamp(mut b) => Arc::new(b.finish()),
                ColBuilder::Text(mut b) => Arc::new(b.finish()),
            }
        }
    }

    fn cell_to_i64(cell: &Data) -> Option<i64> {
        match cell {
            Data::Int(v) => Some(*v),
            Data::Float(f) if f.fract() == 0.0 => Some(*f as i64),
            _ => None,
        }
    }

    fn cell_to_f64(cell: &Data) -> Option<f64> {
        match cell {
            Data::Float(v) => Some(*v),
            Data::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    fn cell_to_bool(cell: &Data) -> Option<bool> {
        match cell {
            Data::Bool(v) => Some(*v),
            _ => None,
        }
    }

    fn cell_to_string(cell: &Data) -> Option<String> {
        match cell {
            Data::Empty => None,
            Data::String(s) => Some(s.clone()),
            Data::DateTimeIso(s) | Data::DurationIso(s) => Some(s.clone()),
            Data::Int(i) => Some(i.to_string()),
            Data::Float(f) => Some(f.to_string()),
            Data::Bool(b) => Some(b.to_string()),
            Data::DateTime(_) => cell.as_f64().map(|f| f.to_string()),
            Data::Error(e) => Some(format!("{e:?}")),
        }
    }

    fn serial_to_millis(serial: f64) -> i64 {
        ((serial - 25_569.0) * 86_400_000.0).round() as i64
    }

    fn cell_to_millis(cell: &Data) -> Option<i64> {
        match cell {
            Data::Empty => None,
            _ => cell.as_f64().map(serial_to_millis),
        }
    }

    fn dedup_name(name: String, seen: &mut HashMap<String, usize>) -> String {
        let count = seen.entry(name.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            name
        } else {
            format!("{name}_{}", *count - 1)
        }
    }

    fn resolve_names(header_row: Option<&[Data]>, width: usize) -> Vec<String> {
        let mut seen = HashMap::new();
        (0..width)
            .map(|c| {
                let raw = header_row
                    .and_then(|h| h.get(c))
                    .and_then(cell_to_string)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("column_{}", c + 1));
                dedup_name(raw, &mut seen)
            })
            .collect()
    }

    fn infer_kinds(data_rows: &[&[Data]], width: usize, limit: usize) -> Vec<ColKind> {
        let mut kinds = vec![ColKind::Empty; width];
        for row in data_rows.iter().take(limit) {
            for (c, kind) in kinds.iter_mut().enumerate() {
                *kind = kind.widen(ColKind::of(row.get(c).unwrap_or(&Data::Empty)));
            }
        }
        kinds
    }

    fn build_columns(data_rows: &[&[Data]], kinds: &[ColKind]) -> Vec<ArrayRef> {
        let mut builders: Vec<ColBuilder> = kinds.iter().map(|k| k.builder()).collect();
        for row in data_rows {
            for (c, builder) in builders.iter_mut().enumerate() {
                builder.append(row.get(c).unwrap_or(&Data::Empty));
            }
        }
        builders.into_iter().map(ColBuilder::finish).collect()
    }

    pub(super) fn read_workbook(
        path: &str,
        options: ExcelOptions,
    ) -> NativeResult<(SchemaRef, Vec<RecordBatch>)> {
        let mut workbook = open_workbook_auto(path)
            .map_err(|e| format!("failed to open workbook {path:?}: {e}"))?;

        let sheet = match options.sheet_name {
            Some(name) => name,
            None => workbook
                .sheet_names()
                .first()
                .cloned()
                .ok_or("workbook contains no worksheets")?,
        };

        let range = workbook
            .worksheet_range(&sheet)
            .map_err(|e| format!("failed to read worksheet {sheet:?}: {e}"))?;

        let width = range.width();
        if width == 0 {
            let schema = Arc::new(Schema::empty());
            return Ok((schema.clone(), vec![RecordBatch::new_empty(schema)]));
        }

        let rows: Vec<&[Data]> = range.rows().collect();
        let (header_row, data_rows): (Option<&[Data]>, &[&[Data]]) =
            if options.has_header && !rows.is_empty() {
                (Some(rows[0]), &rows[1..])
            } else {
                (None, rows.as_slice())
            };

        let names = resolve_names(header_row, width);
        let limit = options.schema_infer_max_records.unwrap_or(usize::MAX);
        let kinds = infer_kinds(data_rows, width, limit);

        let fields: Vec<Field> = names
            .into_iter()
            .zip(&kinds)
            .map(|(name, kind)| Field::new(name, kind.data_type(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let columns = build_columns(data_rows, &kinds);
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        Ok((schema, vec![batch]))
    }
}
