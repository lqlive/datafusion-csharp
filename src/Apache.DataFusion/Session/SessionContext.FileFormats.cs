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

using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class SessionContext
{
    public void RegisterParquet(string schemaName, string tableName, string path, ParquetReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new ParquetReadOptions(), NativeMethods.df_session_context_register_parquet);

    public DataFrame ReadParquet(string schemaName, string tableName, string path, ParquetReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new ParquetReadOptions(), NativeMethods.df_session_context_read_parquet);

    public void RegisterCsv(string schemaName, string tableName, string path, CsvReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new CsvReadOptions(), NativeMethods.df_session_context_register_csv);

    public DataFrame ReadCsv(string schemaName, string tableName, string path, CsvReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new CsvReadOptions(), NativeMethods.df_session_context_read_csv);

    public void RegisterJson(string schemaName, string tableName, string path, JsonReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new JsonReadOptions(), NativeMethods.df_session_context_register_json);

    public DataFrame ReadJson(string schemaName, string tableName, string path, JsonReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new JsonReadOptions(), NativeMethods.df_session_context_read_json);

    public void RegisterArrow(string schemaName, string tableName, string path, ArrowReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new ArrowReadOptions(), NativeMethods.df_session_context_register_arrow);

    public DataFrame ReadArrow(string schemaName, string tableName, string path, ArrowReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new ArrowReadOptions(), NativeMethods.df_session_context_read_arrow);

    public void RegisterAvro(string schemaName, string tableName, string path, AvroReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new AvroReadOptions(), NativeMethods.df_session_context_register_avro);

    public DataFrame ReadAvro(string schemaName, string tableName, string path, AvroReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new AvroReadOptions(), NativeMethods.df_session_context_read_avro);

    public void RegisterExcel(string schemaName, string tableName, string path, ExcelReadOptions? options = null) =>
        Register(schemaName, tableName, path, options ?? new ExcelReadOptions(), NativeMethods.df_session_context_register_excel);

    public DataFrame ReadExcel(string schemaName, string tableName, string path, ExcelReadOptions? options = null) =>
        Read(schemaName, tableName, path, options ?? new ExcelReadOptions(), NativeMethods.df_session_context_read_excel);

    private delegate int RegisterNative(IntPtr context, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    private delegate int ReadNative(IntPtr context, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    private void Register(string schemaName, string tableName, string path, IReadOptions options, RegisterNative native)
    {
        using NativeUtf8String nativeSchemaName = new(schemaName);
        using NativeUtf8String nativeTableName = new(tableName);
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new(options.ToBytes());
        using NativeByteArray nativeSchema = new(options.SchemaIpc ?? []);
        NativeMethods.ThrowIfError(native(Handle, nativeSchemaName.Pointer, nativeTableName.Pointer, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length, nativeSchema.Pointer, nativeSchema.Length));
    }

    private DataFrame Read(string schemaName, string tableName, string path, IReadOptions options, ReadNative native)
    {
        using NativeUtf8String nativeSchemaName = new(schemaName);
        using NativeUtf8String nativeTableName = new(tableName);
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new(options.ToBytes());
        using NativeByteArray nativeSchema = new(options.SchemaIpc ?? []);
        NativeMethods.ThrowIfError(native(Handle, nativeSchemaName.Pointer, nativeTableName.Pointer, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length, nativeSchema.Pointer, nativeSchema.Length, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }
}
