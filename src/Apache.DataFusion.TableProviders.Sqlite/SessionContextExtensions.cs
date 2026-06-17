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
using Microsoft.Data.Sqlite;

namespace Apache.DataFusion.TableProviders.Sqlite;

public static class SessionContextExtensions
{
    public static void RegisterSqlite(this SessionContext context, string connectionString)
    {
        context.RegisterSqlite(new SqliteDatabaseOptions
        {
            ConnectionString = connectionString,
        });
    }

    public static void RegisterSqlite(this SessionContext context, SqliteDatabaseOptions options)
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

        foreach (string tableName in FetchTables(options.ConnectionString, options.IncludeViews))
        {
            context.RegisterStreamingTable(tableName, new SqliteStreamingTableProvider(new SqliteTableOptions
            {
                ConnectionString = options.ConnectionString,
                TableName = tableName,
                BatchSize = options.BatchSize,
            }));
        }
    }

    public static void RegisterSqlite(this SessionContext context, string name, SqliteTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        context.RegisterStreamingTable(name, new SqliteStreamingTableProvider(options));
    }

    public static void RegisterSqlite(this SessionContext context, string name, string connectionString)
    {
        context.RegisterSqlite(name, new SqliteTableOptions
        {
            ConnectionString = connectionString,
            TableName = name,
        });
    }

    private static IEnumerable<string> FetchTables(string connectionString, bool includeViews)
    {
        using SqliteConnection connection = new(connectionString);
        connection.Open();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT name
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND (@include_views OR type = 'table')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """;

        DbParameter includeViewsParameter = command.CreateParameter();
        includeViewsParameter.ParameterName = "@include_views";
        includeViewsParameter.Value = includeViews;
        command.Parameters.Add(includeViewsParameter);

        using DbDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            yield return reader.GetString(0);
        }
    }
}
