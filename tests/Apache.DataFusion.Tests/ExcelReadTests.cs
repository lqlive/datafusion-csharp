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

using Apache.DataFusion;

using MiniExcelLibs;

namespace Apache.DataFusion.Tests;

public class ExcelReadTests
{
    [Fact]
    public void RegisterExcel_WithHeader_QueriesByColumnName()
    {
        string path = WriteWorkbook(new[]
        {
            new { Id = 1, Name = "Alice", Amount = 100.5 },
            new { Id = 2, Name = "Bob", Amount = 250.0 },
            new { Id = 3, Name = "Carol", Amount = 75.25 },
        });

        try
        {
            using SessionContext context = new();
            context.RegisterExcel("people", path, new ExcelReadOptions { HasHeader = true });

            using DataFrame all = context.Sql("SELECT * FROM people");
            Assert.Equal(3UL, all.Count());

            using DataFrame filtered =
                context.Sql("SELECT \"Id\" FROM people WHERE \"Amount\" > 80 AND \"Name\" = 'Bob'");
            Assert.Equal(1UL, filtered.Count());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ReadExcel_ReturnsDataFrameWithExpectedRowCount()
    {
        string path = WriteWorkbook(new[]
        {
            new { Id = 1, Name = "Alice", Amount = 100.5 },
            new { Id = 2, Name = "Bob", Amount = 250.0 },
        });

        try
        {
            using SessionContext context = new();
            using DataFrame df = context.ReadExcel(path, new ExcelReadOptions { HasHeader = true });
            Assert.Equal(2UL, df.Count());
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static string WriteWorkbook<T>(T[] rows)
    {
        string path = Path.Combine(Path.GetTempPath(), $"datafusion-excel-test-{Guid.NewGuid():N}.xlsx");
        MiniExcel.SaveAs(path, rows, overwriteFile: true);
        return path;
    }
}
