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

public sealed class TableProviderFactoryTests
{
    [Fact]
    public void PostgresFactoryRequiresConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new PostgresTableFactory(""));
    }

    [Fact]
    public void MySqlFactoryRequiresConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new MySqlTableFactory(""));
    }

    [Fact]
    public void SqliteFactoryRequiresPath()
    {
        Assert.Throws<ArgumentException>(() => new SqliteTableFactory(""));
    }

    [Fact]
    public void ClickHouseFactoryRequiresUrl()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTableFactory(""));
    }

    [Fact]
    public void MongoDbFactoryRequiresConnectionString()
    {
        Assert.Throws<ArgumentException>(() => new MongoDbTableFactory(""));
    }

    // The default test build ships the native crate without any database
    // feature, so building a factory surfaces the feature gate with a message
    // naming both the provider and the missing Cargo feature.
    [Theory]
    [InlineData("postgres")]
    [InlineData("mysql")]
    [InlineData("clickhouse")]
    [InlineData("mongodb")]
    [InlineData("sqlite")]
    public void FactorySurfacesFeatureGateWhenDisabled(string provider)
    {
        DataFusionException exception = Assert.Throws<DataFusionException>(() =>
        {
            switch (provider)
            {
                case "postgres":
                    using (new PostgresTableFactory("host=localhost port=5432 dbname=postgres user=postgres password=password"))
                    {
                    }

                    break;
                case "mysql":
                    using (new MySqlTableFactory("mysql://root:password@localhost:3306/mysql_db"))
                    {
                    }

                    break;
                case "clickhouse":
                    using (new ClickHouseTableFactory("http://localhost:8123"))
                    {
                    }

                    break;
                case "mongodb":
                    using (new MongoDbTableFactory("mongodb://root:password@localhost:27017/mongo_db"))
                    {
                    }

                    break;
                case "sqlite":
                    using (new SqliteTableFactory("example.db"))
                    {
                    }

                    break;
            }
        });

        Assert.Contains(provider, exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("feature", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
