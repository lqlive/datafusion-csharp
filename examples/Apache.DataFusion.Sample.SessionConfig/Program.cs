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

string csvPath = WritePeopleCsv();

// Configure the session through the builder.
using SessionContext context = SessionContext.CreateBuilder()
    .BatchSize(2048)
    .TargetPartitions(4)
    .InformationSchema(true)
    .Build();

context.RegisterCsv("files", "people", csvPath, new CsvReadOptions { HasHeader = true });
using (DataFrame df = context.Sql("SELECT * FROM files.people"))
{
    df.Show();
}

Console.WriteLine($"batch_size        = {context.GetOption("datafusion.execution.batch_size")}");
Console.WriteLine($"target_partitions = {context.GetOption("datafusion.execution.target_partitions")}");

try
{
    Console.WriteLine($"memory  = {context.GetMemoryUsage()}");
}
catch (DataFusionException ex)
{
    Console.WriteLine($"memory usage unavailable: {ex.Message}");
}

try
{
    Console.WriteLine($"runtime = {context.GetRuntimeStats()}");
}
catch (DataFusionException ex)
{
    Console.WriteLine($"runtime stats unavailable: {ex.Message}");
}

static string WritePeopleCsv()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "session-config");
    Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, "people.csv");
    File.WriteAllText(
        path,
        """
        id,name,age,city
        1,Alice,34,Seattle
        2,Bob,29,Portland
        3,Carol,42,Seattle
        """);
    return path;
}
