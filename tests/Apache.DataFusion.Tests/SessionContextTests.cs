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

public sealed class SessionContextTests
{
    [Fact]
    public void SqlCountExecutesQuery()
    {
        using SessionContext context = new();
        using DataFrame dataFrame = context.Sql("SELECT 1 AS value");

        Assert.Equal(1UL, dataFrame.Count());
    }

    [Fact]
    public void UnknownTableThrowsPlanException()
    {
        using SessionContext context = new();

        Assert.Throws<DataFusionPlanException>(() =>
        {
            using DataFrame _ = context.Sql("SELECT * FROM missing_table");
        });
    }

    [Fact]
    public async Task ExecuteStreamReadsRecordBatch()
    {
        using SessionContext context = new();
        using DataFrame dataFrame = context.Sql("SELECT 1 AS value UNION ALL SELECT 2 AS value");
        using ArrowBatchReader reader = dataFrame.ExecuteStream();
        Assert.Equal(ArrowBatchReaderTransport.CDataInterface, reader.Transport);

        long rows = 0;
        Apache.Arrow.RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync()) is not null)
        {
            rows += batch.Length;
        }

        Assert.Equal(2, rows);
    }

    [Fact]
    public void CanWriteRegisterAndReadParquet()
    {
        using TempTestDirectory directory = new();
        using (SessionContext writerContext = new())
        using (DataFrame source = writerContext.Sql("SELECT 1 AS id UNION ALL SELECT 2 AS id"))
        {
            source.WriteParquet(directory.Path);
        }

        using SessionContext readerContext = new();
        readerContext.RegisterParquet("files", "items", directory.Path);
        using DataFrame dataFrame = readerContext.Sql("SELECT * FROM files.items");

        Assert.Equal(2UL, dataFrame.Count());
    }

    [Fact]
    public void ReadCsvRegistersNameForSubsequentQueries()
    {
        using TempTestDirectory directory = new();
        string primaryPath = directory.Child("primary.csv");
        string secondaryPath = directory.Child("secondary.csv");
        File.WriteAllText(
            primaryPath,
            """
            id,name
            1,first
            2,second
            """);
        File.WriteAllText(
            secondaryPath,
            """
            id,name
            3,third
            """);

        using SessionContext context = new();
        using DataFrame dataFrame = context.ReadCsv("primary_source", "items", primaryPath, new CsvReadOptions { HasHeader = true });
        using DataFrame secondary = context.ReadCsv("secondary_source", "items", secondaryPath, new CsvReadOptions { HasHeader = true });
        using DataFrame named = context.Sql("SELECT * FROM primary_source.items WHERE id = 2");

        Assert.Equal(2UL, dataFrame.Count());
        Assert.Equal(1UL, secondary.Count());
        Assert.Equal(1UL, named.Count());
    }

    [Fact]
    public void ReadJsonRegistersNameForSubsequentQueries()
    {
        using TempTestDirectory directory = new();
        string path = directory.Child("items.json");
        File.WriteAllText(
            path,
            """
            {"id":1,"name":"first"}
            {"id":2,"name":"second"}
            """);

        using SessionContext context = new();
        using DataFrame dataFrame = context.ReadJson("files", "json_items", path);
        using DataFrame named = context.Sql("SELECT * FROM files.json_items WHERE id = 2");

        Assert.Equal(2UL, dataFrame.Count());
        Assert.Equal(1UL, named.Count());
    }
}
