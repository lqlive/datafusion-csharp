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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.DataFusion.Tests;

public class StreamingTableProviderTests
{
    private static Schema BuildSchema() => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
        .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
        .Build();

    [Fact]
    public void RegisterStreamingTable_QueriesRows()
    {
        InMemoryStreamingProvider provider =
            new(BuildSchema(), [1, 2, 3], ["Alice", "Bob", "Carol"]);

        using SessionContext context = new();
        context.RegisterStreamingTable("people", provider);

        using DataFrame all = context.Sql("SELECT * FROM people");
        Assert.Equal(3UL, all.Count());

        using DataFrame filtered = context.Sql("SELECT id FROM people WHERE id > 1 AND name = 'Bob'");
        Assert.Equal(1UL, filtered.Count());
    }

    [Fact]
    public void RegisterStreamingTable_InSchemaQueriesRows()
    {
        InMemoryStreamingProvider provider =
            new(BuildSchema(), [1, 2], ["Alice", "Bob"]);

        using SessionContext context = new();
        context.RegisterStreamingTable("sqlite1", "people", provider);

        using DataFrame result = context.Sql("SELECT name FROM sqlite1.people WHERE id = 2");
        Assert.Equal(1UL, result.Count());
    }

    [Fact]
    public void RegisterStreamingTable_ScansLazilyOnEachQuery()
    {
        InMemoryStreamingProvider provider =
            new(BuildSchema(), [10, 20], ["x", "y"]);

        using SessionContext context = new();
        context.RegisterStreamingTable("events", provider);

        using (DataFrame first = context.Sql("SELECT * FROM events"))
        {
            Assert.Equal(2UL, first.Count());
        }

        using (DataFrame second = context.Sql("SELECT * FROM events"))
        {
            Assert.Equal(2UL, second.Count());
        }

        // Each query triggers a fresh scan callback rather than reusing one
        // materialized batch set.
        Assert.True(provider.ScanCount >= 2);
    }

    private sealed class InMemoryStreamingProvider(Schema schema, int[] ids, string[] names) : StreamingTableProvider
    {
        public int ScanCount { get; private set; }

        public override Schema Schema { get; } = schema;

        public override IArrowArrayStream Scan()
        {
            ScanCount++;

            Int32Array.Builder idBuilder = new();
            foreach (int id in ids)
            {
                idBuilder.Append(id);
            }

            StringArray.Builder nameBuilder = new();
            foreach (string name in names)
            {
                nameBuilder.Append(name);
            }

            RecordBatch batch = new(
                Schema,
                [idBuilder.Build(), nameBuilder.Build()],
                ids.Length);

            return new ListArrayStream(Schema, batch);
        }
    }

    private sealed class ListArrayStream : IArrowArrayStream
    {
        private readonly Queue<RecordBatch> batches;

        public ListArrayStream(Schema schema, params RecordBatch[] batches)
        {
            Schema = schema;
            this.batches = new Queue<RecordBatch>(batches);
        }

        public Schema Schema { get; }

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
            new(batches.Count > 0 ? batches.Dequeue() : null);

        public void Dispose()
        {
            while (batches.Count > 0)
            {
                batches.Dequeue().Dispose();
            }
        }
    }
}
