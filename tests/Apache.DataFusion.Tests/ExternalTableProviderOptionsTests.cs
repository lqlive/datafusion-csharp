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

public sealed class ExternalTableProviderOptionsTests
{
    [Fact]
    public void MySqlRequiresConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new MySqlTableOptions("", "companies"));
    }

    [Fact]
    public void MongoDbRequiresCollectionName()
    {
        Assert.Throws<ArgumentException>(() => new MongoDbTableOptions("mongodb://localhost:27017/db", ""));
    }

    [Fact]
    public void ClickHouseRequiresUrl()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTableOptions("", "Reports"));
    }

    [Fact]
    public void SqliteRequiresPath()
    {
        Assert.Throws<ArgumentException>(() => new SqliteTableOptions("", "companies"));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("mongodb")]
    [InlineData("clickhouse")]
    [InlineData("sqlite")]
    public void RegisterExternalProviderSurfacesFeatureGateWhenDisabled(string provider)
    {
        using SessionContext ctx = new();

        DataFusionException exception = Assert.Throws<DataFusionException>(() =>
        {
            switch (provider)
            {
                case "mysql":
                    ctx.RegisterMySql(
                        "companies",
                        new MySqlTableOptions("mysql://root:password@localhost:3306/mysql_db", "companies"));
                    break;
                case "mongodb":
                    ctx.RegisterMongoDb(
                        "companies",
                        new MongoDbTableOptions("mongodb://root:password@localhost:27017/mongo_db", "companies"));
                    break;
                case "clickhouse":
                    ctx.RegisterClickHouse(
                        "reports",
                        new ClickHouseTableOptions("http://localhost:8123", "Reports"));
                    break;
                case "sqlite":
                    ctx.RegisterSqlite(
                        "companies",
                        new SqliteTableOptions("example.db", "companies"));
                    break;
            }
        });

        Assert.Contains(provider, exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("feature", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
