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

namespace Apache.DataFusion.Interop;

internal static partial class NativeMethods
{
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_new(out IntPtr handle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_new_with_options(IntPtr optionsPtr, nuint optionsLen, out IntPtr handle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_free(IntPtr handle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_sql(IntPtr handle, IntPtr sql, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_from_proto(IntPtr handle, IntPtr planPtr, nuint planLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_from_substrait(IntPtr handle, IntPtr planPtr, nuint planLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_table_schema_ipc(IntPtr handle, IntPtr tableName, out ByteBuffer buffer);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_table_ipc(IntPtr handle, IntPtr name, IntPtr ipcPtr, nuint ipcLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_postgres_table(
        IntPtr handle,
        IntPtr registrationName,
        IntPtr connectionString,
        IntPtr schemaName,
        IntPtr tableName);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_mysql_table(
        IntPtr handle,
        IntPtr registrationName,
        IntPtr connectionString,
        IntPtr schemaName,
        IntPtr tableName);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_mongodb_table(
        IntPtr handle,
        IntPtr registrationName,
        IntPtr connectionString,
        IntPtr collectionName);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_clickhouse_table(
        IntPtr handle,
        IntPtr registrationName,
        IntPtr url,
        IntPtr database,
        IntPtr user,
        IntPtr password,
        IntPtr tableName);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_sqlite_table(
        IntPtr handle,
        IntPtr registrationName,
        IntPtr path,
        IntPtr tableName,
        ulong busyTimeoutMs);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate int ScalarI64Callback(IntPtr userData, out long value);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_scalar_udf_i64(IntPtr handle, IntPtr name, byte volatility, ScalarI64Callback callback, IntPtr userData);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_get_option(IntPtr handle, IntPtr key, out IntPtr value);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_memory_usage(IntPtr handle, out ByteBuffer buffer);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_runtime_stats(IntPtr handle, out ByteBuffer buffer);
}
