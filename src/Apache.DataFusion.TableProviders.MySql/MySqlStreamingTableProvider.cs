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
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.DataFusion.TableProviders.MySql.Sql;
using MySqlConnector;

namespace Apache.DataFusion.TableProviders.MySql;

public sealed class MySqlStreamingTableProvider : StreamingTableProvider
{
    private readonly MySqlDataSource dataSource;
    private readonly string sourceSql;
    private readonly int batchSize;
    private readonly ColumnPlan[] columns;
    private readonly SqlQueryBuilder queryBuilder = new(MySqlDialect.Instance);

    public MySqlStreamingTableProvider(MySqlTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        dataSource = options.DataSource ?? CreateDataSource(options.ConnectionString, nameof(options));
        sourceSql = BuildSourceSql(options);
        batchSize = options.BatchSize;
        columns = FetchColumns(dataSource, sourceSql);
        Schema = BuildSchema(columns);
    }

    public override Schema Schema { get; }

    public override bool SupportsPushdown => true;

    public override IArrowArrayStream Scan() =>
        new MySqlArrowArrayStream(dataSource, sourceSql, Schema, columns, batchSize);

    public override IArrowArrayStream Scan(StreamingTableScanRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        PushedQuery pushedQuery = queryBuilder.Build(sourceSql, request);
        ColumnPlan[] projectedColumns = ProjectColumns(request);
        Schema projectedSchema = BuildSchema(projectedColumns);
        return new MySqlArrowArrayStream(
            dataSource,
            pushedQuery.Sql,
            projectedSchema,
            projectedColumns,
            batchSize,
            pushedQuery.Parameters);
    }

    private static MySqlDataSource CreateDataSource(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace when a data source is not provided.", parameterName);
        }

        return new MySqlDataSourceBuilder(connectionString).Build();
    }

    private static string BuildSourceSql(MySqlTableOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.TableName))
        {
            throw new ArgumentException("Table name must be provided.", nameof(options));
        }

        return $"SELECT * FROM {QuoteQualifiedTableName(options.DatabaseName, options.TableName)}";
    }

    private static string QuoteQualifiedTableName(string? databaseName, string tableName)
    {
        string quotedTable = MySqlDialect.Instance.QuoteIdentifier(tableName);
        return string.IsNullOrWhiteSpace(databaseName)
            ? quotedTable
            : $"{MySqlDialect.Instance.QuoteIdentifier(databaseName)}.{quotedTable}";
    }

    private static ColumnPlan[] FetchColumns(MySqlDataSource dataSource, string query)
    {
        using MySqlConnection connection = dataSource.OpenConnection();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = query;

        using DbDataReader reader = command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
        return reader.GetColumnSchema()
            .Select((column, ordinal) => ColumnPlan.From(column, ordinal))
            .ToArray();
    }

    private static Schema BuildSchema(IEnumerable<ColumnPlan> columns)
    {
        Schema.Builder builder = new();
        foreach (ColumnPlan column in columns)
        {
            builder.Field(field => field
                .Name(column.Name)
                .DataType(column.DataType)
                .Nullable(column.Nullable));
        }

        return builder.Build();
    }

    private ColumnPlan[] ProjectColumns(StreamingTableScanRequest request)
    {
        if (!request.HasProjection)
        {
            return columns;
        }

        Dictionary<string, ColumnPlan> columnLookup = columns.ToDictionary(
            static column => column.Name,
            StringComparer.OrdinalIgnoreCase);
        return request.Projection
            .Select((name, ordinal) => columnLookup.TryGetValue(name, out ColumnPlan? column)
                ? column with { Ordinal = ordinal }
                : throw new InvalidOperationException($"MySQL table provider does not contain column '{name}'."))
            .ToArray();
    }

}
