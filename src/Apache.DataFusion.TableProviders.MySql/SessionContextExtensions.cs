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

using MySqlConnector;

namespace Apache.DataFusion.TableProviders.MySql;

public static class SessionContextExtensions
{
    public static void RegisterMySql(this SessionContext context, string connectionString)
    {
        context.RegisterMySql(new MySqlDatabaseOptions
        {
            ConnectionString = connectionString,
        });
    }

    public static void RegisterMySql(this SessionContext context, MySqlDatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        MySqlDataSource dataSource = options.DataSource ?? CreateDataSource(options.ConnectionString, nameof(options));

        foreach (MySqlTable table in FetchTables(dataSource, options.DatabaseName, options.IncludeViews))
        {
            RegisterSourceTable(context, options.SourceName, table.TableName, new MySqlStreamingTableProvider(new MySqlTableOptions
            {
                ConnectionString = options.ConnectionString,
                DataSource = dataSource,
                DatabaseName = table.DatabaseName,
                TableName = table.TableName,
                BatchSize = options.BatchSize,
            }));
        }
    }

    public static void RegisterMySql(this SessionContext context, string name, MySqlTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        context.RegisterStreamingTable(name, new MySqlStreamingTableProvider(options));
    }

    public static void RegisterMySql(this SessionContext context, string name, string connectionString)
    {
        context.RegisterMySql(name, new MySqlTableOptions
        {
            ConnectionString = connectionString,
            TableName = name,
        });
    }

    private static MySqlDataSource CreateDataSource(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace when a data source is not provided.", parameterName);
        }

        return new MySqlDataSourceBuilder(connectionString).Build();
    }

    private static IEnumerable<MySqlTable> FetchTables(
        MySqlDataSource dataSource,
        string? databaseName,
        bool includeViews)
    {
        using MySqlConnection connection = dataSource.OpenConnection();

        using MySqlCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = COALESCE(@database_name, DATABASE())
              AND table_type IN ('BASE TABLE', 'VIEW')
              AND (@include_views OR table_type = 'BASE TABLE')
            ORDER BY table_name
            """;
        command.Parameters.AddWithValue("@database_name", string.IsNullOrWhiteSpace(databaseName) ? DBNull.Value : databaseName);
        command.Parameters.AddWithValue("@include_views", includeViews);

        using MySqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            yield return new MySqlTable(reader.GetString(0), reader.GetString(1));
        }
    }

    private sealed record MySqlTable(string DatabaseName, string TableName);

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
