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

namespace Apache.DataFusion.TableProviders.ClickHouse.Sql;

internal sealed class ClickHouseQueryBuilder(ClickHouseDialect dialect)
{
    public PushedQuery Build(string baseQuery, StreamingTableScanRequest request)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseQuery);
        ArgumentNullException.ThrowIfNull(request);

        List<ClickHouseQueryParameter> parameters = [];
        ClickHouseSelectStatement statement = new(
            BuildProjection(request),
            TrimTrailingSemicolon(baseQuery),
            BuildPredicates(request, parameters),
            request.Limit);

        return new PushedQuery(statement.ToSql(), parameters);
    }

    private string BuildProjection(StreamingTableScanRequest request)
    {
        if (!request.HasProjection)
        {
            return "*";
        }

        if (request.Projection.Count == 0)
        {
            return "1";
        }

        return string.Join(", ", request.Projection.Select(dialect.QuoteIdentifier));
    }

    private List<string> BuildPredicates(
        StreamingTableScanRequest request,
        List<ClickHouseQueryParameter> parameters)
    {
        List<string> predicates = [];
        for (int i = 0; i < request.Filters.Count; i++)
        {
            StreamingTableFilter filter = request.Filters[i];
            string parameterName = dialect.ParameterName(i);
            predicates.Add($"{dialect.QuoteIdentifier(filter.Column)} {dialect.OperatorSql(filter.Operator)} {parameterName}");
            parameters.Add(new ClickHouseQueryParameter(parameterName, ConvertFilterValue(filter)));
        }

        return predicates;
    }

    private static object ConvertFilterValue(StreamingTableFilter filter) => filter.ValueKind switch
    {
        StreamingTableFilterValueKind.Boolean => bool.Parse(filter.Value),
        StreamingTableFilterValueKind.Integer => long.Parse(filter.Value, System.Globalization.CultureInfo.InvariantCulture),
        StreamingTableFilterValueKind.FloatingPoint => double.Parse(filter.Value, System.Globalization.CultureInfo.InvariantCulture),
        StreamingTableFilterValueKind.String => filter.Value,
        _ => throw new NotSupportedException($"Unsupported filter value kind '{filter.ValueKind}'."),
    };

    private static string TrimTrailingSemicolon(string sql) =>
        sql.Trim().TrimEnd(';');
}
