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

/// <summary>
/// Opens a ClickHouse connection pool once and reuses it to register any number
/// of tables into one or more <see cref="SessionContext"/> instances. Prefer
/// this over the one-shot <see cref="SessionContext.RegisterClickHouse(string, ClickHouseTableOptions, CancellationToken)"/>
/// when registering several tables from the same server, to avoid rebuilding a
/// pool per table. Dispose the factory to release the pool.
/// </summary>
public sealed class ClickHouseTableFactory : IDisposable
{
    private IntPtr handle;

    /// <summary>
    /// Open the connection pool. <paramref name="database"/>,
    /// <paramref name="user"/> and <paramref name="password"/> are optional.
    /// Pass a cancellable <paramref name="cancellationToken"/> to abort a slow
    /// connect.
    /// </summary>
    public ClickHouseTableFactory(string url, string? database = null, string? user = null, string? password = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException("ClickHouse URL cannot be empty.", nameof(url));
        }

        using NativeUtf8String nativeUrl = new(url);
        using NativeUtf8String? nativeDatabase = ToNativeOptional(database);
        using NativeUtf8String? nativeUser = ToNativeOptional(user);
        using NativeUtf8String? nativePassword = ToNativeOptional(password);
        handle = NativeCancellationBridge.Invoke(
            (ulong token, out IntPtr created) => NativeMethods.df_clickhouse_table_factory_new(
                nativeUrl.Pointer,
                nativeDatabase?.Pointer ?? IntPtr.Zero,
                nativeUser?.Pointer ?? IntPtr.Zero,
                nativePassword?.Pointer ?? IntPtr.Zero,
                token,
                out created),
            cancellationToken);
    }

    private IntPtr Handle
    {
        get
        {
            if (handle == IntPtr.Zero)
            {
                throw new ObjectDisposedException(nameof(ClickHouseTableFactory));
            }

            return handle;
        }
    }

    /// <summary>
    /// Register a table from the pooled connection into <paramref name="context"/>.
    /// Firing <paramref name="cancellationToken"/> aborts a slow registration and
    /// throws <see cref="OperationCanceledException"/>.
    /// </summary>
    public void Register(SessionContext context, string registrationName, string tableName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (string.IsNullOrWhiteSpace(registrationName))
        {
            throw new ArgumentException("Registration name cannot be empty.", nameof(registrationName));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("ClickHouse table name cannot be empty.", nameof(tableName));
        }

        IntPtr factoryHandle = Handle;
        IntPtr contextHandle = context.Handle;
        using NativeUtf8String nativeName = new(registrationName);
        using NativeUtf8String nativeTable = new(tableName);
        NativeCancellationBridge.Invoke(
            token => NativeMethods.df_clickhouse_table_factory_register(factoryHandle, contextHandle, nativeName.Pointer, nativeTable.Pointer, token),
            cancellationToken);
    }

    public void Dispose()
    {
        IntPtr current = handle;
        if (current == IntPtr.Zero)
        {
            return;
        }

        handle = IntPtr.Zero;
        NativeMethods.ThrowIfError(NativeMethods.df_clickhouse_table_factory_free(current));
    }

    private static NativeUtf8String? ToNativeOptional(string? value) =>
        string.IsNullOrEmpty(value) ? null : new NativeUtf8String(value);
}
