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

using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.DataFusion.TableProviders.PostgreSql;

internal sealed record ColumnPlan(
    int Ordinal,
    string Name,
    IArrowType DataType,
    bool Nullable,
    ColumnKind Kind)
{
    public static ColumnPlan From(DbColumn column, int ordinal)
    {
        Type? type = System.Nullable.GetUnderlyingType(column.DataType ?? typeof(string)) ?? column.DataType;
        ColumnKind kind = GetKind(type);
        return new ColumnPlan(
            ordinal,
            column.ColumnName ?? $"column_{ordinal}",
            GetArrowType(kind),
            column.AllowDBNull ?? true,
            kind);
    }

    public ColumnAppender CreateAppender() => Kind switch
    {
        ColumnKind.Boolean => new BooleanColumnAppender(Ordinal),
        ColumnKind.Int32 => new Int32ColumnAppender(Ordinal),
        ColumnKind.Int64 => new Int64ColumnAppender(Ordinal),
        ColumnKind.Float => new FloatColumnAppender(Ordinal),
        ColumnKind.Double => new DoubleColumnAppender(Ordinal),
        _ => new StringColumnAppender(Ordinal),
    };

    private static ColumnKind GetKind(Type? type)
    {
        if (type == typeof(bool))
        {
            return ColumnKind.Boolean;
        }

        if (type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort) || type == typeof(int))
        {
            return ColumnKind.Int32;
        }

        if (type == typeof(uint) || type == typeof(long) || type == typeof(ulong))
        {
            return ColumnKind.Int64;
        }

        if (type == typeof(float))
        {
            return ColumnKind.Float;
        }

        if (type == typeof(double) || type == typeof(decimal))
        {
            return ColumnKind.Double;
        }

        return ColumnKind.String;
    }

    private static IArrowType GetArrowType(ColumnKind kind) => kind switch
    {
        ColumnKind.Boolean => BooleanType.Default,
        ColumnKind.Int32 => Int32Type.Default,
        ColumnKind.Int64 => Int64Type.Default,
        ColumnKind.Float => FloatType.Default,
        ColumnKind.Double => DoubleType.Default,
        _ => StringType.Default,
    };
}
