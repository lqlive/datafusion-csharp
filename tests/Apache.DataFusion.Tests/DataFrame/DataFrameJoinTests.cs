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

public sealed class DataFrameJoinTests
{
    [Fact]
    public void ColumnJoinReturnsExpectedRowCount()
    {
        using SessionContext context = new();
        using DataFrame left = context.Sql("SELECT 1 AS id UNION ALL SELECT 2 AS id");
        using DataFrame right = context.Sql("SELECT 2 AS rid UNION ALL SELECT 3 AS rid");
        using DataFrame joined = left.Join(right, JoinType.Inner, ["id"], ["rid"]);

        Assert.Equal(1UL, joined.Count());
    }
}
