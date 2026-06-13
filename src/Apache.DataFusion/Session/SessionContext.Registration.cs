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
        ArgumentNullException.ThrowIfNull(tableProvider);
        using NativeUtf8String nativeName = new(name);
        using NativeByteArray ipc = new(tableProvider.ToIpcStream());
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_register_table_ipc(Handle, nativeName.Pointer, ipc.Pointer, ipc.Length));
    }

    /// <summary>
    /// Register a single PostgreSQL table. This builds a connection pool, uses
    /// it once and releases it; when registering several tables from the same
    /// database, reuse one <see cref="PostgresTableFactory"/> instead. Firing
    /// <paramref name="cancellationToken"/> aborts a slow connect/registration.
    /// </summary>
    public void RegisterPostgres(string name, PostgresTableOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        using PostgresTableFactory factory = new(options.ConnectionString, cancellationToken);
        factory.Register(this, name, options.TableName, options.SchemaName, cancellationToken);
    }

    /// <summary>
    /// Register a single MySQL table. This builds a connection pool, uses it
    /// once and releases it; when registering several tables from the same
    /// database, reuse one <see cref="MySqlTableFactory"/> instead. Firing
    /// <paramref name="cancellationToken"/> aborts a slow connect/registration.
    /// </summary>
    public void RegisterMySql(string name, MySqlTableOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        using MySqlTableFactory factory = new(options.ConnectionString, cancellationToken);
        factory.Register(this, name, options.TableName, options.SchemaName, cancellationToken);
    }

    /// <summary>
    /// Register a single MongoDB collection. This builds a connection pool, uses
    /// it once and releases it; when registering several collections from the
    /// same database, reuse one <see cref="MongoDbTableFactory"/> instead.
    /// Firing <paramref name="cancellationToken"/> aborts a slow
    /// connect/registration.
    /// </summary>
    public void RegisterMongoDb(string name, MongoDbTableOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        using MongoDbTableFactory factory = new(options.ConnectionString, cancellationToken);
        factory.Register(this, name, options.CollectionName, cancellationToken);
    }

    /// <summary>
    /// Register a single ClickHouse table. This builds a connection pool, uses
    /// it once and releases it; when registering several tables from the same
    /// server, reuse one <see cref="ClickHouseTableFactory"/> instead. Firing
    /// <paramref name="cancellationToken"/> aborts a slow connect/registration.
    /// </summary>
    public void RegisterClickHouse(string name, ClickHouseTableOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        using ClickHouseTableFactory factory = new(options.Url, options.Database, options.User, options.Password, cancellationToken);
        factory.Register(this, name, options.TableName, cancellationToken);
    }

    /// <summary>
    /// Register a single SQLite table. This opens a connection pool, uses it
    /// once and releases it; when registering several tables from the same
    /// database file, reuse one <see cref="SqliteTableFactory"/> instead. Firing
    /// <paramref name="cancellationToken"/> aborts a slow open/registration.
    /// </summary>
    public void RegisterSqlite(string name, SqliteTableOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        using SqliteTableFactory factory = new(options.Path, options.BusyTimeout, cancellationToken);
        factory.Register(this, name, options.TableName, cancellationToken);
    }

    public void RegisterScalarUdf(ScalarUdf udf)
    {
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
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_register_scalar_udf_i64(Handle, nativeName.Pointer, (byte)udf.Volatility, callback, IntPtr.Zero));
    }
}
