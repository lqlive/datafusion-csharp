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
using Apache.DataFusion;

string csvPath = WritePeopleCsv();

using SessionContext context = new();
context.RegisterCsv("people", csvPath, new CsvReadOptions { HasHeader = true });

using DataFrame df = context.Sql("SELECT id, name, age FROM people ORDER BY id");

// Stream Arrow record batches and read typed column values.
using BatchReader reader = df.ExecuteStream();
int batchIndex = 0;
while (await reader.ReadNextRecordBatchAsync() is { } batch)
{
    Console.WriteLine($"Batch #{batchIndex++} ({batch.Length} rows):");
    Int64Array ids = (Int64Array)batch.Column(0);
    StringArray names = (StringArray)batch.Column(1);
    Int64Array ages = (Int64Array)batch.Column(2);
    for (int row = 0; row < batch.Length; row++)
    {
        Console.WriteLine($"  id={ids.GetValue(row)}, name={names.GetString(row)}, age={ages.GetValue(row)}");
    }
}

static string WritePeopleCsv()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "streaming");
    Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, "people.csv");
    File.WriteAllText(
        path,
        """
        id,name,age
        1,Alice,34
        2,Bob,29
        3,Carol,42
        """);
    return path;
}
