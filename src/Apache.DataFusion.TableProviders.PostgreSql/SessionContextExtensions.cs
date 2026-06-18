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

using System.Data;
using System.Data.Common;
using Npgsql;

namespace Apache.DataFusion.TableProviders.PostgreSql;

public static class SessionContextExtensions
{
    public static void RegisterPostgreSql(this SessionContext context, string connectionString)
    {
        context.RegisterPostgreSql(new PostgreSqlDatabaseOptions
        {
            ConnectionString = connectionString,
        });
    }

    public static void RegisterPostgreSql(this SessionContext context, PostgreSqlDatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        string[] schemas = options.Schemas
            .Where(static schema => !string.IsNullOrWhiteSpace(schema))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (schemas.Length == 0)
        {
            throw new ArgumentException("At least one schema must be provided.", nameof(options));
        }

        NpgsqlDataSource dataSource = options.DataSource ?? CreateDataSource(options.ConnectionString, nameof(options));

        foreach (PostgreSqlTable table in FetchTables(dataSource, schemas, options.IncludeViews))
        {
            string registrationName = RegistrationName(table, schemas);
            RegisterSourceTable(context, options.SourceName, registrationName, new PostgreSqlStreamingTableProvider(new PostgreSqlTableOptions
            {
                ConnectionString = options.ConnectionString,
                DataSource = dataSource,
                SchemaName = table.SchemaName,
                TableName = table.TableName,
                BatchSize = options.BatchSize,
            }));
        }
    }

    public static void RegisterPostgreSql(this SessionContext context, string name, PostgreSqlTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        context.RegisterStreamingTable(name, new PostgreSqlStreamingTableProvider(options));
    }

    public static void RegisterPostgreSql(this SessionContext context, string name, string connectionString)
    {
        context.RegisterPostgreSql(name, new PostgreSqlTableOptions
        {
            ConnectionString = connectionString,
            TableName = name,
        });
    }

    private static NpgsqlDataSource CreateDataSource(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace when a data source is not provided.", parameterName);
        }

        return NpgsqlDataSource.Create(connectionString);
    }

    private static IEnumerable<PostgreSqlTable> FetchTables(
        NpgsqlDataSource dataSource,
        IReadOnlyCollection<string> schemas,
        bool includeViews)
    {
        using NpgsqlConnection connection = dataSource.OpenConnection();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(@schemas)
              AND table_type = ANY(@table_types)
            ORDER BY table_schema, table_name
            """;

        DbParameter schemasParameter = command.CreateParameter();
        schemasParameter.ParameterName = "schemas";
        schemasParameter.Value = schemas.ToArray();
        command.Parameters.Add(schemasParameter);

        DbParameter tableTypesParameter = command.CreateParameter();
        tableTypesParameter.ParameterName = "table_types";
        tableTypesParameter.Value = includeViews
            ? new[] { "BASE TABLE", "VIEW" }
            : new[] { "BASE TABLE" };
        command.Parameters.Add(tableTypesParameter);

        using DbDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read())
        {
            yield return new PostgreSqlTable(reader.GetString(0), reader.GetString(1));
        }
    }

    private static string RegistrationName(PostgreSqlTable table, IReadOnlyCollection<string> schemas) =>
        schemas.Count == 1 && string.Equals(table.SchemaName, "public", StringComparison.OrdinalIgnoreCase)
            ? table.TableName
            : $"{table.SchemaName}_{table.TableName}";

    private sealed record PostgreSqlTable(string SchemaName, string TableName);

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
