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

using Apache.Arrow.Types;
using MongoDB.Bson;

namespace Apache.DataFusion.TableProviders.MongoDB;

internal sealed record MongoDbColumnPlan(
    string Name,
    IArrowType DataType,
    bool Nullable,
    MongoDbColumnKind Kind)
{
    public MongoDbColumnAppender CreateAppender() => Kind switch
    {
        MongoDbColumnKind.Boolean => new BooleanMongoDbColumnAppender(Name),
        MongoDbColumnKind.Int32 => new Int32MongoDbColumnAppender(Name),
        MongoDbColumnKind.Int64 => new Int64MongoDbColumnAppender(Name),
        MongoDbColumnKind.Double => new DoubleMongoDbColumnAppender(Name),
        _ => new StringMongoDbColumnAppender(Name),
    };

    public static MongoDbColumnPlan[] Infer(IReadOnlyList<BsonDocument> documents)
    {
        Dictionary<string, ColumnAccumulator> columns = new(StringComparer.Ordinal);
        for (int i = 0; i < documents.Count; i++)
        {
            foreach (BsonElement element in documents[i])
            {
                if (!columns.TryGetValue(element.Name, out ColumnAccumulator? accumulator))
                {
                    accumulator = new ColumnAccumulator();
                    columns.Add(element.Name, accumulator);
                }

                accumulator.Observe(element.Value);
            }
        }

        return [.. columns
            .Select(column => new MongoDbColumnPlan(
                column.Key,
                GetArrowType(column.Value.Kind),
                column.Value.Nullable || column.Value.ObservedCount < documents.Count,
                column.Value.Kind))];
    }

    private static IArrowType GetArrowType(MongoDbColumnKind kind) => kind switch
    {
        MongoDbColumnKind.Boolean => BooleanType.Default,
        MongoDbColumnKind.Int32 => Int32Type.Default,
        MongoDbColumnKind.Int64 => Int64Type.Default,
        MongoDbColumnKind.Double => DoubleType.Default,
        _ => StringType.Default,
    };

    private sealed class ColumnAccumulator
    {
        public MongoDbColumnKind Kind { get; private set; } = MongoDbColumnKind.String;

        public bool HasKind { get; private set; }

        public bool Nullable { get; private set; }

        public int ObservedCount { get; private set; }

        public void Observe(BsonValue value)
        {
            ObservedCount++;
            if (value.IsBsonNull || value.IsBsonUndefined)
            {
                Nullable = true;
                return;
            }

            MongoDbColumnKind valueKind = GetKind(value);
            Kind = HasKind ? Merge(Kind, valueKind) : valueKind;
            HasKind = true;
        }

        private static MongoDbColumnKind GetKind(BsonValue value) => value.BsonType switch
        {
            BsonType.Boolean => MongoDbColumnKind.Boolean,
            BsonType.Int32 => MongoDbColumnKind.Int32,
            BsonType.Int64 => MongoDbColumnKind.Int64,
            BsonType.Double => MongoDbColumnKind.Double,
            _ => MongoDbColumnKind.String,
        };

        private static MongoDbColumnKind Merge(MongoDbColumnKind left, MongoDbColumnKind right)
        {
            if (left == right)
            {
                return left;
            }

            if (IsInteger(left) && IsInteger(right))
            {
                return MongoDbColumnKind.Int64;
            }

            if (IsNumeric(left) && IsNumeric(right))
            {
                return MongoDbColumnKind.Double;
            }

            return MongoDbColumnKind.String;
        }

        private static bool IsInteger(MongoDbColumnKind kind) =>
            kind is MongoDbColumnKind.Int32 or MongoDbColumnKind.Int64;

        private static bool IsNumeric(MongoDbColumnKind kind) =>
            kind is MongoDbColumnKind.Int32 or MongoDbColumnKind.Int64 or MongoDbColumnKind.Double;
    }
}
