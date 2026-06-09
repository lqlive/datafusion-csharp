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

public sealed class PostgresTableOptionsTests
{
    [Fact]
    public void RequiresConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new PostgresTableOptions("", "companies"));
    }

    [Fact]
    public void RequiresTableName()
    {
        Assert.Throws<ArgumentException>(() => new PostgresTableOptions("host=localhost", ""));
    }

    [Fact]
    public void RegisterPostgresSurfacesFeatureGateWhenDisabled()
    {
        using SessionContext ctx = new();
        PostgresTableOptions options = new(
            "host=localhost port=5432 dbname=postgres user=postgres password=password sslmode=disable",
            "companies")
        {
            SchemaName = "public",
        };

        DataFusionException exception = Assert.Throws<DataFusionException>(() =>
            ctx.RegisterPostgres("companies", options));

        Assert.Contains("postgres", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("feature", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
