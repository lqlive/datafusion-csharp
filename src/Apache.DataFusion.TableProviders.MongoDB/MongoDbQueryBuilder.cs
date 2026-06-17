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

using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Apache.DataFusion.TableProviders.MongoDB;

internal sealed class MongoDbQueryBuilder
{
    public MongoDbQuery Build(StreamingTableScanRequest request, IReadOnlyList<MongoDbColumnPlan> projectedColumns)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new MongoDbQuery(
            BuildFilter(request.Filters),
            BuildProjection(projectedColumns),
            request.Limit);
    }

    private static FilterDefinition<BsonDocument> BuildFilter(IReadOnlyList<StreamingTableFilter> filters)
    {
        FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
        if (filters.Count == 0)
        {
            return builder.Empty;
        }

        return builder.And(filters.Select(filter => BuildPredicate(builder, filter)));
    }

    private static FilterDefinition<BsonDocument> BuildPredicate(
        FilterDefinitionBuilder<BsonDocument> builder,
        StreamingTableFilter filter)
    {
        BsonValue value = ConvertFilterValue(filter);
        return filter.Operator switch
        {
            StreamingTableFilterOperator.Equal => builder.Eq(filter.Column, value),
            StreamingTableFilterOperator.NotEqual => builder.Ne(filter.Column, value),
            StreamingTableFilterOperator.LessThan => builder.Lt(filter.Column, value),
            StreamingTableFilterOperator.LessThanOrEqual => builder.Lte(filter.Column, value),
            StreamingTableFilterOperator.GreaterThan => builder.Gt(filter.Column, value),
            StreamingTableFilterOperator.GreaterThanOrEqual => builder.Gte(filter.Column, value),
            _ => throw new NotSupportedException($"Unsupported filter operator '{filter.Operator}'."),
        };
    }

    private static ProjectionDefinition<BsonDocument>? BuildProjection(IReadOnlyList<MongoDbColumnPlan> projectedColumns)
    {
        if (projectedColumns.Count == 0)
        {
            return Builders<BsonDocument>.Projection.Exclude("_id");
        }

        ProjectionDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Projection;
        ProjectionDefinition<BsonDocument> projection = builder.Combine(projectedColumns.Select(column => builder.Include(column.Name)));
        if (!projectedColumns.Any(static column => column.Name == "_id"))
        {
            projection = builder.Combine(projection, builder.Exclude("_id"));
        }

        return projection;
    }

    private static BsonValue ConvertFilterValue(StreamingTableFilter filter) => filter.ValueKind switch
    {
        StreamingTableFilterValueKind.Boolean => bool.Parse(filter.Value),
        StreamingTableFilterValueKind.Integer => long.Parse(filter.Value, CultureInfo.InvariantCulture),
        StreamingTableFilterValueKind.FloatingPoint => double.Parse(filter.Value, CultureInfo.InvariantCulture),
        StreamingTableFilterValueKind.String => filter.Value,
        _ => throw new NotSupportedException($"Unsupported filter value kind '{filter.ValueKind}'."),
    };
}
