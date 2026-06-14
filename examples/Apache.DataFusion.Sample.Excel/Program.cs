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

string xlsxPath = WritePeopleWorkbook();

using SessionContext context = new();

context.RegisterExcel("people", xlsxPath, new ExcelReadOptions { HasHeader = true });

Console.WriteLine("People earning more than 1000, highest first:");
using DataFrame df = context.Sql(
    "SELECT Id, Name, Amount FROM people WHERE Amount > 1000 ORDER BY Amount DESC");
df.Show();
Console.WriteLine($"Match count: {df.Count()}");

static string WritePeopleWorkbook()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "excel");
    Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, "people.xlsx");

    var rows = new[]
    {
        new { Id = 1, Name = "Alice", Amount = 1200.50, City = "Seattle" },
        new { Id = 2, Name = "Bob", Amount = 950.00, City = "Portland" },
        new { Id = 3, Name = "Carol", Amount = 2100.75, City = "Seattle" },
        new { Id = 4, Name = "Dave", Amount = 1800.00, City = "Austin" },
        new { Id = 5, Name = "Erin", Amount = 640.25, City = "Portland" },
    };
    MiniExcel.SaveAs(path, rows, overwriteFile: true);
    return path;
}
