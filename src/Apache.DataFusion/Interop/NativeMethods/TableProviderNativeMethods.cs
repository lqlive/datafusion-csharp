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

// External-database table providers are exposed as factory handles: a factory
// builds its connection pool once and registers many tables from it. Each
// new/register call takes a cancellation token handle (0 = no token).
internal static partial class NativeMethods
{
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_postgres_table_factory_new(IntPtr connectionString, ulong tokenHandle, out IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_postgres_table_factory_register(
        IntPtr factory,
        IntPtr context,
        IntPtr registrationName,
        IntPtr schemaName,
        IntPtr tableName,
        ulong tokenHandle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_postgres_table_factory_free(IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mysql_table_factory_new(IntPtr connectionString, ulong tokenHandle, out IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mysql_table_factory_register(
        IntPtr factory,
        IntPtr context,
        IntPtr registrationName,
        IntPtr schemaName,
        IntPtr tableName,
        ulong tokenHandle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mysql_table_factory_free(IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_sqlite_table_factory_new(IntPtr path, ulong busyTimeoutMs, ulong tokenHandle, out IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_sqlite_table_factory_register(
        IntPtr factory,
        IntPtr context,
        IntPtr registrationName,
        IntPtr tableName,
        ulong tokenHandle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_sqlite_table_factory_free(IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_clickhouse_table_factory_new(
        IntPtr url,
        IntPtr database,
        IntPtr user,
        IntPtr password,
        ulong tokenHandle,
        out IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_clickhouse_table_factory_register(
        IntPtr factory,
        IntPtr context,
        IntPtr registrationName,
        IntPtr tableName,
        ulong tokenHandle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_clickhouse_table_factory_free(IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mongodb_table_factory_new(IntPtr connectionString, ulong tokenHandle, out IntPtr factory);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mongodb_table_factory_register(
        IntPtr factory,
        IntPtr context,
        IntPtr registrationName,
        IntPtr collectionName,
        ulong tokenHandle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_mongodb_table_factory_free(IntPtr factory);
}
