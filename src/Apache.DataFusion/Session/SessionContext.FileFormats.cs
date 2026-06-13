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
    public void RegisterParquet(string name, string path, ParquetReadOptions? options = null) =>
        Register(name, path, options ?? new ParquetReadOptions(), NativeMethods.df_session_context_register_parquet);

    public DataFrame ReadParquet(string path, ParquetReadOptions? options = null) =>
        Read(path, options ?? new ParquetReadOptions(), NativeMethods.df_session_context_read_parquet);

    public void RegisterCsv(string name, string path, CsvReadOptions? options = null) =>
        Register(name, path, options ?? new CsvReadOptions(), NativeMethods.df_session_context_register_csv);

    public DataFrame ReadCsv(string path, CsvReadOptions? options = null) =>
        Read(path, options ?? new CsvReadOptions(), NativeMethods.df_session_context_read_csv);

    public void RegisterJson(string name, string path, JsonReadOptions? options = null) =>
        Register(name, path, options ?? new JsonReadOptions(), NativeMethods.df_session_context_register_json);

    public DataFrame ReadJson(string path, JsonReadOptions? options = null) =>
        Read(path, options ?? new JsonReadOptions(), NativeMethods.df_session_context_read_json);

    public void RegisterArrow(string name, string path, ArrowReadOptions? options = null) =>
        Register(name, path, options ?? new ArrowReadOptions(), NativeMethods.df_session_context_register_arrow);

    public DataFrame ReadArrow(string path, ArrowReadOptions? options = null) =>
        Read(path, options ?? new ArrowReadOptions(), NativeMethods.df_session_context_read_arrow);

    public void RegisterAvro(string name, string path, AvroReadOptions? options = null) =>
        Register(name, path, options ?? new AvroReadOptions(), NativeMethods.df_session_context_register_avro);

    public DataFrame ReadAvro(string path, AvroReadOptions? options = null) =>
        Read(path, options ?? new AvroReadOptions(), NativeMethods.df_session_context_read_avro);

    private delegate int RegisterNative(IntPtr context, IntPtr name, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    private delegate int ReadNative(IntPtr context, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    private void Register(string name, string path, IReadOptions options, RegisterNative native)
    {
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new(options.ToBytes());
        using NativeByteArray nativeSchema = new(options.SchemaIpc ?? []);
        NativeMethods.ThrowIfError(native(Handle, nativeName.Pointer, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length, nativeSchema.Pointer, nativeSchema.Length));
    }

    private DataFrame Read(string path, IReadOptions options, ReadNative native)
    {
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new(options.ToBytes());
        using NativeByteArray nativeSchema = new(options.SchemaIpc ?? []);
        NativeMethods.ThrowIfError(native(Handle, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length, nativeSchema.Pointer, nativeSchema.Length, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }
}
