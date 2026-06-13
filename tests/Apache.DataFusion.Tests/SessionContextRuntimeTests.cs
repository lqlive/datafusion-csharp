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

public sealed class SessionContextRuntimeTests
{
    [Fact]
    public void GetOptionReturnsConfiguredValue()
    {
        using SessionContext context = SessionContext
            .CreateBuilder()
            .SetOption("datafusion.execution.batch_size", "128")
            .Build();

        Assert.Equal("128", context.GetOption("datafusion.execution.batch_size"));
    }

    [Fact]
    public void MemoryUsageReturnsConsistentSnapshot()
    {
        using SessionContext context = new();

        MemoryUsage usage = context.GetMemoryUsage();

        Assert.True(usage.PeakBytes >= usage.CurrentBytes);
    }

    [Fact]
    public void RuntimeStatsReportsFeatureGateWhenUnavailable()
    {
        using SessionContext context = new();

        DataFusionException exception = Assert.Throws<DataFusionException>(() => context.GetRuntimeStats());
        Assert.Contains("runtime-metrics", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CacheManagerOptionsCanCreateContext()
    {
        CacheManagerOptions cacheManager = CacheManagerOptions
            .CreateBuilder()
            .FileMetadataCache(1024)
            .ListFilesCache(2048, TimeSpan.FromSeconds(5))
            .FileStatisticsCache(true)
            .Build();

        using SessionContext context = SessionContext
            .CreateBuilder()
            .CacheManager(cacheManager)
            .Build();
        using DataFrame dataFrame = context.Sql("SELECT 1 AS value");

        Assert.Equal(1UL, dataFrame.Count());
    }
}
