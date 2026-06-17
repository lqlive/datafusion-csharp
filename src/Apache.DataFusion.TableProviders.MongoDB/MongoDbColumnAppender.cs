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
using Apache.Arrow;
using MongoDB.Bson;

namespace Apache.DataFusion.TableProviders.MongoDB;

internal abstract class MongoDbColumnAppender(string name)
{
    protected string Name { get; } = name;

    public abstract void Append(BsonDocument document);

    public abstract IArrowArray Build();

    protected bool TryGetValue(BsonDocument document, out BsonValue value)
    {
        if (!document.TryGetValue(Name, out value) || value.IsBsonNull || value.IsBsonUndefined)
        {
            value = BsonNull.Value;
            return false;
        }

        return true;
    }

    protected static string FormatValue(BsonValue value) => value.BsonType switch
    {
        BsonType.String => value.AsString,
        BsonType.ObjectId => value.AsObjectId.ToString(),
        BsonType.DateTime => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        BsonType.Boolean => value.AsBoolean.ToString(CultureInfo.InvariantCulture),
        BsonType.Int32 => value.AsInt32.ToString(CultureInfo.InvariantCulture),
        BsonType.Int64 => value.AsInt64.ToString(CultureInfo.InvariantCulture),
        BsonType.Double => value.AsDouble.ToString(CultureInfo.InvariantCulture),
        _ => value.ToJson(),
    };

    protected static double ToDouble(BsonValue value) => value.BsonType switch
    {
        BsonType.Int32 => value.AsInt32,
        BsonType.Int64 => value.AsInt64,
        BsonType.Double => value.AsDouble,
        _ => Convert.ToDouble(FormatValue(value), CultureInfo.InvariantCulture),
    };
}

internal sealed class BooleanMongoDbColumnAppender(string name) : MongoDbColumnAppender(name)
{
    private readonly BooleanArray.Builder builder = new();

    public override void Append(BsonDocument document)
    {
        if (!TryGetValue(document, out BsonValue value))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(value.AsBoolean);
    }

    public override IArrowArray Build() => builder.Build();
}

internal sealed class Int32MongoDbColumnAppender(string name) : MongoDbColumnAppender(name)
{
    private readonly Int32Array.Builder builder = new();

    public override void Append(BsonDocument document)
    {
        if (!TryGetValue(document, out BsonValue value))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(value.ToInt32());
    }

    public override IArrowArray Build() => builder.Build();
}

internal sealed class Int64MongoDbColumnAppender(string name) : MongoDbColumnAppender(name)
{
    private readonly Int64Array.Builder builder = new();

    public override void Append(BsonDocument document)
    {
        if (!TryGetValue(document, out BsonValue value))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(value.ToInt64());
    }

    public override IArrowArray Build() => builder.Build();
}

internal sealed class DoubleMongoDbColumnAppender(string name) : MongoDbColumnAppender(name)
{
    private readonly DoubleArray.Builder builder = new();

    public override void Append(BsonDocument document)
    {
        if (!TryGetValue(document, out BsonValue value))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(ToDouble(value));
    }

    public override IArrowArray Build() => builder.Build();
}

internal sealed class StringMongoDbColumnAppender(string name) : MongoDbColumnAppender(name)
{
    private readonly StringArray.Builder builder = new();

    public override void Append(BsonDocument document)
    {
        if (!TryGetValue(document, out BsonValue value))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(FormatValue(value));
    }

    public override IArrowArray Build() => builder.Build();
}
