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

using System.Runtime.InteropServices;
using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class SessionContext
{
    public byte[] TableSchemaIpc(string tableName)
    {
        using NativeUtf8String name = new(tableName);
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_table_schema_ipc(Handle, name.Pointer, out NativeMethods.ByteBuffer buffer));
        return NativeMethods.CopyAndFree(buffer);
    }

    public string? GetOption(string key)
    {
        using NativeUtf8String nativeKey = new(key);
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_get_option(Handle, nativeKey.Pointer, out IntPtr valuePtr));
        if (valuePtr == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            return Marshal.PtrToStringUTF8(valuePtr);
        }
        finally
        {
            NativeMethods.df_string_free(valuePtr);
        }
    }

    public MemoryUsage GetMemoryUsage()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_memory_usage(Handle, out NativeMethods.ByteBuffer buffer));
        ulong[] values = NativeMethods.CopyUInt64ArrayAndFree(buffer);
        if (values.Length != 2)
        {
            throw new DataFusionException($"Expected 2 memory usage values, got {values.Length}.");
        }

        return new MemoryUsage(values[0], values[1]);
    }

    [Obsolete("Use GetMemoryUsage instead.")]
    public MemoryUsage MemoryUsage() => GetMemoryUsage();

    public RuntimeStats GetRuntimeStats()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_runtime_stats(Handle, out NativeMethods.ByteBuffer buffer));
        long[] values = NativeMethods.CopyInt64ArrayAndFree(buffer);
        return Apache.DataFusion.RuntimeStats.FromNative(values);
    }

    [Obsolete("Use GetRuntimeStats instead.")]
    public RuntimeStats RuntimeStats() => GetRuntimeStats();
}
