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

using System.Data.Common;
using ClickHouse.Driver.ADO;

namespace Apache.DataFusion.TableProviders.ClickHouse;

public static class SessionContextExtensions
{
    public static void RegisterClickHouse(this SessionContext context, string connectionString)
    {
        context.RegisterClickHouse(new ClickHouseDatabaseOptions
        {
            ConnectionString = connectionString,
        });
    }

    public static void RegisterClickHouse(this SessionContext context, ClickHouseDatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(options));
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        foreach (ClickHouseTable table in FetchTables(options.ConnectionString, options.DatabaseName, options.IncludeViews))
        {
            RegisterSourceTable(context, options.SourceName, table.TableName, new ClickHouseStreamingTableProvider(new ClickHouseTableOptions
            {
                ConnectionString = options.ConnectionString,
                DatabaseName = table.DatabaseName,
                TableName = table.TableName,
                BatchSize = options.BatchSize,
            }));
        }
    }

    public static void RegisterClickHouse(this SessionContext context, string name, ClickHouseTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        context.RegisterStreamingTable(name, new ClickHouseStreamingTableProvider(options));
    }

    public static void RegisterClickHouse(this SessionContext context, string name, string connectionString)
    {
        context.RegisterClickHouse(name, new ClickHouseTableOptions
        {
            ConnectionString = connectionString,
            TableName = name,
        });
    }

    private static IEnumerable<ClickHouseTable> FetchTables(
        string connectionString,
        string? databaseName,
        bool includeViews)
    {
        using ClickHouseConnection connection = new(connectionString);
        connection.Open();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT database, name
            FROM system.tables
            WHERE database = if({database:String} = '', currentDatabase(), {database:String})
              AND ({include_views:Bool} OR engine NOT IN ('View', 'MaterializedView'))
            ORDER BY name
            """;

        DbParameter databaseParameter = command.CreateParameter();
        databaseParameter.ParameterName = "database";
        databaseParameter.Value = databaseName ?? string.Empty;
        command.Parameters.Add(databaseParameter);

        DbParameter includeViewsParameter = command.CreateParameter();
        includeViewsParameter.ParameterName = "include_views";
        includeViewsParameter.Value = includeViews;
        command.Parameters.Add(includeViewsParameter);

        using DbDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            yield return new ClickHouseTable(reader.GetString(0), reader.GetString(1));
        }
    }

    private sealed record ClickHouseTable(string DatabaseName, string TableName);

    private static void RegisterSourceTable(SessionContext context, string? sourceName, string tableName, StreamingTableProvider provider)
    {
        if (string.IsNullOrWhiteSpace(sourceName))
        {
            context.RegisterStreamingTable(tableName, provider);
            return;
        }

        context.RegisterStreamingTable(sourceName, tableName, provider);
    }
}
