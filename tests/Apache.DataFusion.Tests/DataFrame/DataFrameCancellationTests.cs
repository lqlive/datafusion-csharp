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

using Apache.Arrow;

namespace Apache.DataFusion.Tests;

public sealed class DataFrameCancellationTests
{
    [Fact]
    public void PreCancelledTokenAbortsCollect()
    {
        using SessionContext context = new();
        using CancellationTokenSource cts = new();
        cts.Cancel();

        using DataFrame dataFrame = context.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        Assert.Throws<OperationCanceledException>(() => dataFrame.Collect(cts.Token));
    }

    [Fact]
    public void PreCancelledTokenAbortsExecuteStream()
    {
        using SessionContext context = new();
        using CancellationTokenSource cts = new();
        cts.Cancel();

        using DataFrame dataFrame = context.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        Assert.Throws<OperationCanceledException>(() => dataFrame.ExecuteStream(cts.Token));
    }

    [Fact]
    public async Task UnfiredTokenCollectsAllRows()
    {
        using SessionContext context = new();
        using CancellationTokenSource cts = new();

        using DataFrame dataFrame = context.Sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(x)");
        using ArrowBatchReader reader = dataFrame.Collect(cts.Token);

        Assert.Equal(4, await CountRowsAsync(reader));
    }

    [Fact]
    public async Task NoneTokenBehavesLikeParameterlessOverload()
    {
        using SessionContext context = new();

        using DataFrame collected = context.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");
        using ArrowBatchReader collectReader = collected.Collect(CancellationToken.None);
        Assert.Equal(3, await CountRowsAsync(collectReader));

        using DataFrame streamed = context.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");
        using ArrowBatchReader streamReader = streamed.ExecuteStream(CancellationToken.None);
        Assert.Equal(3, await CountRowsAsync(streamReader));
    }

    [Fact]
    public void CancelMidCollectAbortsBeforeCompletion()
    {
        ManualResetEventSlim firstInvocation = new(initialState: false);

        using SessionContext context = SessionContext.CreateBuilder().BatchSize(1).Build();
        context.RegisterScalarUdf(ScalarUdf.Int64(
            "sleep_identity",
            _ =>
            {
                firstInvocation.Set();
                Thread.Sleep(50);
                return ColumnarValue.Int64(1);
            },
            Volatility.Volatile));

        using CancellationTokenSource cts = new();

        // ~1000 * 50ms is far longer than the wait below, so the assertion only
        // passes if cancellation actually aborts the in-flight collect.
        using DataFrame dataFrame = context.Sql(
            "SELECT sleep_identity() AS y FROM generate_series(1, 1000)");

        Task<ArrowBatchReader> task = Task.Run(() => dataFrame.Collect(cts.Token));

        Assert.True(firstInvocation.Wait(TimeSpan.FromSeconds(10)), "UDF should have been invoked at least once");
        cts.Cancel();

        AggregateException aggregate = Assert.Throws<AggregateException>(() => task.Wait(TimeSpan.FromSeconds(20)));
        Assert.IsAssignableFrom<OperationCanceledException>(aggregate.InnerException);
    }

    private static async Task<long> CountRowsAsync(ArrowBatchReader reader)
    {
        long rows = 0;
        RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync()) is not null)
        {
            rows += batch.Length;
        }

        return rows;
    }
}
