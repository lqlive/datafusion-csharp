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
/// Opens a SQLite connection pool once and reuses it to register any number of
/// tables into one or more <see cref="SessionContext"/> instances. Prefer this
/// over the one-shot <see cref="SessionContext.RegisterSqlite(string, SqliteTableOptions, CancellationToken)"/>
/// when registering several tables from the same database file, to avoid
/// rebuilding a pool per table. Dispose the factory to release the pool.
/// </summary>
public sealed class SqliteTableFactory : IDisposable
{
    private IntPtr handle;

    public SqliteTableFactory(string path, CancellationToken cancellationToken = default)
        : this(path, TimeSpan.FromSeconds(5), cancellationToken)
    {
    }

    /// <summary>
    /// Open the connection pool against <paramref name="path"/> with the given
    /// busy timeout. Pass a cancellable <paramref name="cancellationToken"/> to
    /// abort a slow open.
    /// </summary>
    public SqliteTableFactory(string path, TimeSpan busyTimeout, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException("SQLite database path cannot be empty.", nameof(path));
        }

        ulong busyTimeoutMs = checked((ulong)busyTimeout.TotalMilliseconds);
        using NativeUtf8String nativePath = new(path);
        handle = NativeCancellationBridge.Invoke(
            (ulong token, out IntPtr created) => NativeMethods.df_sqlite_table_factory_new(nativePath.Pointer, busyTimeoutMs, token, out created),
            cancellationToken);
    }

    private IntPtr Handle
    {
        get
        {
            if (handle == IntPtr.Zero)
            {
                throw new ObjectDisposedException(nameof(SqliteTableFactory));
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
            throw new ArgumentException("SQLite table name cannot be empty.", nameof(tableName));
        }

        IntPtr factoryHandle = Handle;
        IntPtr contextHandle = context.Handle;
        using NativeUtf8String nativeName = new(registrationName);
        using NativeUtf8String nativeTable = new(tableName);
        NativeCancellationBridge.Invoke(
            token => NativeMethods.df_sqlite_table_factory_register(factoryHandle, contextHandle, nativeName.Pointer, nativeTable.Pointer, token),
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
        NativeMethods.ThrowIfError(NativeMethods.df_sqlite_table_factory_free(current));
    }
}
