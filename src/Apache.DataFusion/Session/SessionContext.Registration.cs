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
    private readonly List<NativeMethods.ScalarI64Callback> scalarCallbacks = [];

    public void RegisterTable(string name, TableProvider tableProvider)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(tableProvider);
        using NativeUtf8String nativeName = new(name);
        using NativeByteArray ipc = new(tableProvider.ToIpcStream());
        NativeMethods.Check(NativeMethods.df_session_context_register_table_ipc(handle, nativeName.Pointer, ipc.Pointer, ipc.Length));
    }

    public void RegisterPostgres(string name, PostgresTableOptions options)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(options);
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeConnectionString = new(options.ConnectionString);
        using NativeUtf8String nativeTableName = new(options.TableName);
        using NativeUtf8String? nativeSchemaName = ToNativeOptional(options.SchemaName);
        NativeMethods.Check(NativeMethods.df_session_context_register_postgres_table(
            handle,
            nativeName.Pointer,
            nativeConnectionString.Pointer,
            nativeSchemaName?.Pointer ?? IntPtr.Zero,
            nativeTableName.Pointer));
    }

    public void RegisterMySql(string name, MySqlTableOptions options)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(options);
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeConnectionString = new(options.ConnectionString);
        using NativeUtf8String nativeTableName = new(options.TableName);
        using NativeUtf8String? nativeSchemaName = ToNativeOptional(options.SchemaName);
        NativeMethods.Check(NativeMethods.df_session_context_register_mysql_table(
            handle,
            nativeName.Pointer,
            nativeConnectionString.Pointer,
            nativeSchemaName?.Pointer ?? IntPtr.Zero,
            nativeTableName.Pointer));
    }

    public void RegisterMongoDb(string name, MongoDbTableOptions options)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(options);
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeConnectionString = new(options.ConnectionString);
        using NativeUtf8String nativeCollectionName = new(options.CollectionName);
        NativeMethods.Check(NativeMethods.df_session_context_register_mongodb_table(
            handle,
            nativeName.Pointer,
            nativeConnectionString.Pointer,
            nativeCollectionName.Pointer));
    }

    public void RegisterClickHouse(string name, ClickHouseTableOptions options)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(options);
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeUrl = new(options.Url);
        using NativeUtf8String? nativeDatabase = ToNativeOptional(options.Database);
        using NativeUtf8String? nativeUser = ToNativeOptional(options.User);
        using NativeUtf8String? nativePassword = ToNativeOptional(options.Password);
        using NativeUtf8String nativeTableName = new(options.TableName);
        NativeMethods.Check(NativeMethods.df_session_context_register_clickhouse_table(
            handle,
            nativeName.Pointer,
            nativeUrl.Pointer,
            nativeDatabase?.Pointer ?? IntPtr.Zero,
            nativeUser?.Pointer ?? IntPtr.Zero,
            nativePassword?.Pointer ?? IntPtr.Zero,
            nativeTableName.Pointer));
    }

    public void RegisterSqlite(string name, SqliteTableOptions options)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(options);
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativePath = new(options.Path);
        using NativeUtf8String nativeTableName = new(options.TableName);
        NativeMethods.Check(NativeMethods.df_session_context_register_sqlite_table(
            handle,
            nativeName.Pointer,
            nativePath.Pointer,
            nativeTableName.Pointer,
            checked((ulong)options.BusyTimeout.TotalMilliseconds)));
    }

    public void RegisterScalarUdf(ScalarUdf udf)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(udf);
        using NativeUtf8String nativeName = new(udf.Name);
        NativeMethods.ScalarI64Callback callback = (IntPtr _, out long value) =>
        {
            value = 0;
            try
            {
                ColumnarValue result = udf.Function(ScalarFunctionArgs.Empty);
                if (result is not ColumnarValue.Int64Scalar scalar || !scalar.Value.HasValue)
                {
                    return 1;
                }

                value = scalar.Value.Value;
                return 0;
            }
            catch
            {
                return 1;
            }
        };
        scalarCallbacks.Add(callback);
        NativeMethods.Check(NativeMethods.df_session_context_register_scalar_udf_i64(handle, nativeName.Pointer, (byte)udf.Volatility, callback, IntPtr.Zero));
    }

    private static NativeUtf8String? ToNativeOptional(string? value) =>
        string.IsNullOrEmpty(value) ? null : new NativeUtf8String(value);
}
