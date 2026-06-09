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

public sealed class TableProviderTests
{
    [Fact]
    public void SimpleTableProviderRegistersQueryableTable()
    {
        using SessionContext source = new();
        using DataFrame sourceData = source.Sql("SELECT 1 AS id UNION ALL SELECT 2 AS id");
        SimpleTableProvider provider = SimpleTableProvider.FromDataFrame(sourceData);

        using SessionContext target = new();
        target.RegisterTable("numbers", provider);
        using DataFrame result = target.Sql("SELECT COUNT(*) AS c FROM numbers");

        Assert.Equal(1UL, result.Count());
    }
}
