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

public sealed class MongoDbStreamingTableProvider : StreamingTableProvider
{
    private readonly IMongoCollection<BsonDocument> collection;
    private readonly int batchSize;
    private readonly MongoDbColumnPlan[] columns;
    private readonly MongoDbQueryBuilder queryBuilder = new();

    public MongoDbStreamingTableProvider(MongoDbTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.DatabaseName))
        {
            throw new ArgumentException("Database name cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.CollectionName))
        {
            throw new ArgumentException("Collection name cannot be null or whitespace.", nameof(options));
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        if (options.SchemaInferenceLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.SchemaInferenceLimit, "Schema inference limit must be greater than zero.");
        }

        batchSize = options.BatchSize;
        IMongoClient client = options.Client ?? CreateClient(options.ConnectionString, nameof(options));
        collection = client.GetDatabase(options.DatabaseName).GetCollection<BsonDocument>(options.CollectionName);
        columns = InferColumns(collection, options.SchemaInferenceLimit);
        Schema = BuildSchema(columns);
    }

    public override Schema Schema { get; }

    public override bool SupportsPushdown => true;

    public override IArrowArrayStream Scan() =>
        CreateStream(StreamingTableScanRequest.Empty);

    public override IArrowArrayStream Scan(StreamingTableScanRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return CreateStream(request);
    }

    private MongoDbArrowArrayStream CreateStream(StreamingTableScanRequest request)
    {
        MongoDbColumnPlan[] projectedColumns = ProjectColumns(request);
        MongoDbQuery query = queryBuilder.Build(request, projectedColumns);
        Schema projectedSchema = BuildSchema(projectedColumns);
        return new MongoDbArrowArrayStream(collection, query, projectedSchema, projectedColumns, batchSize);
    }

    private static IMongoClient CreateClient(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace when a MongoDB client is not provided.", parameterName);
        }

        return new MongoClient(connectionString);
    }

    private MongoDbColumnPlan[] ProjectColumns(StreamingTableScanRequest request)
    {
        if (!request.HasProjection)
        {
            return columns;
        }

        Dictionary<string, MongoDbColumnPlan> columnLookup = columns.ToDictionary(
            static column => column.Name,
            StringComparer.Ordinal);
        return [.. request.Projection
            .Select(name => columnLookup.TryGetValue(name, out MongoDbColumnPlan? column)
                ? column
                : throw new InvalidOperationException($"MongoDB table provider does not contain field '{name}'."))];
    }

    private static MongoDbColumnPlan[] InferColumns(IMongoCollection<BsonDocument> collection, int schemaInferenceLimit)
    {
        List<BsonDocument> documents = collection
            .Find(Builders<BsonDocument>.Filter.Empty)
            .Limit(schemaInferenceLimit)
            .ToList();
        return MongoDbColumnPlan.Infer(documents);
    }

    private static Schema BuildSchema(IEnumerable<MongoDbColumnPlan> columns)
    {
        Schema.Builder builder = new();
        foreach (MongoDbColumnPlan column in columns)
        {
            builder.Field(field => field
                .Name(column.Name)
                .DataType(column.DataType)
                .Nullable(column.Nullable));
        }

        return builder.Build();
    }
}
