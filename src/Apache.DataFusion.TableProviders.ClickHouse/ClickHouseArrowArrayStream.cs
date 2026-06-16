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
using Apache.DataFusion.TableProviders.ClickHouse.Sql;

using ClickHouse.Driver.ADO;

namespace Apache.DataFusion.TableProviders.ClickHouse;

internal sealed class ClickHouseArrowArrayStream : IArrowArrayStream
{
    private readonly Schema schema;
    private readonly ColumnPlan[] columns;
    private readonly int batchSize;
    private ClickHouseConnection? connection;
    private DbCommand? command;
    private DbDataReader? reader;
    private bool finished;

    public ClickHouseArrowArrayStream(
        string connectionString,
        string query,
        Schema schema,
        ColumnPlan[] columns,
        int batchSize,
        IReadOnlyList<ClickHouseQueryParameter>? parameters = null)
    {
        this.schema = schema;
        this.columns = columns;
        this.batchSize = batchSize;

        try
        {
            connection = new ClickHouseConnection(connectionString);
            connection.Open();

            command = connection.CreateCommand();
            command.CommandText = query;
            if (parameters is not null)
            {
                foreach (ClickHouseQueryParameter parameter in parameters)
                {
                    DbParameter dbParameter = command.CreateParameter();
                    dbParameter.ParameterName = parameter.Name;
                    dbParameter.Value = parameter.Value;
                    command.Parameters.Add(dbParameter);
                }
            }

            reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    public Schema Schema => schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (finished || reader is null)
        {
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        ColumnAppender[] appenders = columns.Select(static column => column.CreateAppender()).ToArray();
        int rowCount = 0;
        while (rowCount < batchSize && reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (ColumnAppender appender in appenders)
            {
                appender.Append(reader);
            }

            rowCount++;
        }

        if (rowCount == 0)
        {
            finished = true;
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        IArrowArray[] arrays = appenders.Select(static appender => appender.Build()).ToArray();
        RecordBatch batch = new(schema, arrays, rowCount);
        return new ValueTask<RecordBatch?>(batch);
    }

    public void Dispose()
    {
        reader?.Dispose();
        command?.Dispose();
        connection?.Dispose();
        reader = null;
        command = null;
        connection = null;
    }
}
