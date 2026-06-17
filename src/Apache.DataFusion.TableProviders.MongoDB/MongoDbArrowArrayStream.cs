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

using Apache.Arrow;
using Apache.Arrow.Ipc;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Apache.DataFusion.TableProviders.MongoDB;

internal sealed class MongoDbArrowArrayStream : IArrowArrayStream
{
    private readonly Schema schema;
    private readonly MongoDbColumnPlan[] columns;
    private readonly int batchSize;
    private readonly IAsyncCursor<BsonDocument> cursor;
    private IEnumerator<BsonDocument>? currentBatch;
    private bool finished;

    public MongoDbArrowArrayStream(
        IMongoCollection<BsonDocument> collection,
        MongoDbQuery query,
        Schema schema,
        MongoDbColumnPlan[] columns,
        int batchSize)
    {
        this.schema = schema;
        this.columns = columns;
        this.batchSize = batchSize;

        IFindFluent<BsonDocument, BsonDocument> find = collection.Find(query.Filter);
        if (query.Projection is not null)
        {
            find = find.Project<BsonDocument>(query.Projection);
        }

        if (query.Limit.HasValue)
        {
            if (query.Limit.Value > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(query), query.Limit, "MongoDB limit cannot exceed Int32.MaxValue.");
            }

            find = find.Limit((int)query.Limit.Value);
        }

        find.Options.BatchSize = batchSize;
        cursor = find.ToCursor();
    }

    public Schema Schema => schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (finished)
        {
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        MongoDbColumnAppender[] appenders = [.. columns.Select(static column => column.CreateAppender())];
        int rowCount = 0;
        while (rowCount < batchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TryReadNextDocument(out BsonDocument? document))
            {
                break;
            }

            foreach (MongoDbColumnAppender appender in appenders)
            {
                appender.Append(document);
            }

            rowCount++;
        }

        if (rowCount == 0)
        {
            finished = true;
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        IArrowArray[] arrays = [.. appenders.Select(static appender => appender.Build())];
        RecordBatch batch = new(schema, arrays, rowCount);
        return new ValueTask<RecordBatch?>(batch);
    }

    public void Dispose()
    {
        currentBatch?.Dispose();
        cursor.Dispose();
        currentBatch = null;
    }

    private bool TryReadNextDocument(out BsonDocument document)
    {
        while (true)
        {
            if (currentBatch is not null && currentBatch.MoveNext())
            {
                document = currentBatch.Current;
                return true;
            }

            currentBatch?.Dispose();
            currentBatch = null;
            if (!cursor.MoveNext())
            {
                document = [];
                return false;
            }

            currentBatch = cursor.Current.GetEnumerator();
        }
    }
}
