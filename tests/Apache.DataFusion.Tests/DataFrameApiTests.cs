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

namespace Apache.DataFusion.Tests;

public sealed class DataFrameApiTests
{
    [Fact]
    public void TransformationsRemainLazyAndComposable()
    {
        using SessionContext context = new();
        using DataFrame source = context.Sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name");
        using DataFrame transformed = source
            .Filter("id > 1")
            .Select("name")
            .Limit(1);

        Assert.Equal(1UL, transformed.Count());
    }

    [Fact]
    public void SetOperationsReturnExpectedRows()
    {
        using SessionContext context = new();
        using DataFrame left = context.Sql("SELECT 1 AS id UNION ALL SELECT 2 AS id");
        using DataFrame right = context.Sql("SELECT 2 AS id UNION ALL SELECT 3 AS id");
        using DataFrame union = left.UnionDistinct(right);

        Assert.Equal(3UL, union.Count());
    }

    [Fact]
    public void CanWriteCsvAndJson()
    {
        using TempTestDirectory directory = new();
        string csvPath = directory.Child("csv");
        string jsonPath = directory.Child("json");
        using SessionContext context = new();
        using DataFrame dataFrame = context.Sql("SELECT 1 AS id UNION ALL SELECT 2 AS id");

        dataFrame.WriteCsv(csvPath);
        dataFrame.WriteJson(jsonPath);

        Assert.True(Directory.EnumerateFiles(csvPath).Any());
        Assert.True(Directory.EnumerateFiles(jsonPath).Any());
    }

    [Fact]
    public void PlanExchangeReportsDecodeAndFeatureErrors()
    {
        using SessionContext context = new();

        Assert.Throws<DataFusionException>(() => context.FromProto([]));
        DataFusionException exception = Assert.Throws<DataFusionException>(() => context.FromSubstrait([]));
        Assert.Contains("substrait", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SchemaReturnsStrongArrowSchema()
    {
        using SessionContext context = new();
        using DataFrame dataFrame = context.Sql("SELECT 1 AS id, 'alice' AS name");

        Apache.Arrow.Schema schema = dataFrame.Schema();

        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("id", schema.GetFieldByName("id")?.Name);
        Assert.Equal("name", schema.GetFieldByName("name")?.Name);
    }
}
