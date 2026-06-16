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
using MySqlConnector;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Apache.DataFusion.TableProviders.MySql;

public sealed class MySqlStreamingTableProvider : StreamingTableProvider
{
    private readonly string connectionString;
    private readonly string query;
    private readonly int batchSize;
    private readonly ColumnPlan[] columns;

    public MySqlStreamingTableProvider(MySqlTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.Query))
        {
            throw new ArgumentException("Query cannot be null or whitespace.", nameof(options));
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        connectionString = options.ConnectionString;
        query = options.Query;
        batchSize = options.BatchSize;
        columns = FetchColumns(connectionString, query);
        Schema = BuildSchema(columns);
    }

    public override Schema Schema { get; }

    public override IArrowArrayStream Scan() =>
        new MySqlArrowArrayStream(connectionString, query, Schema, columns, batchSize);

    private static ColumnPlan[] FetchColumns(string connectionString, string query)
    {
        using MySqlConnection connection = new(connectionString);
        connection.Open();

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

}
