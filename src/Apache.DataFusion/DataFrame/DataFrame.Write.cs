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

public sealed partial class DataFrame
{
    public void WriteParquet(string path, string? compression = null, bool singleFileOutput = false)
    {
        EnsureOpen();
        using NativeUtf8String nativePath = new(path);
        using NativeUtf8String? nativeCompression = compression is null ? null : new NativeUtf8String(compression);
        NativeMethods.Check(NativeMethods.df_dataframe_write_parquet(handle, nativePath.Pointer, nativeCompression?.Pointer ?? IntPtr.Zero, singleFileOutput));
    }

    public void WriteCsv(string path, CsvWriteOptions? options = null)
    {
        EnsureOpen();
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new((options ?? new CsvWriteOptions()).ToBytes());
        NativeMethods.Check(NativeMethods.df_dataframe_write_csv(handle, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length));
    }

    public void WriteJson(string path, JsonWriteOptions? options = null)
    {
        EnsureOpen();
        using NativeUtf8String nativePath = new(path);
        using NativeByteArray nativeOptions = new((options ?? new JsonWriteOptions()).ToBytes());
        NativeMethods.Check(NativeMethods.df_dataframe_write_json(handle, nativePath.Pointer, nativeOptions.Pointer, nativeOptions.Length));
    }
}
