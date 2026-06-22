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

using System.Diagnostics;
using Apache.DataFusion;

string parquetPath = args.Length > 0 ? args[0] : Path.Combine("benchmark-data", "large.parquet");
int iterations = args.Length > 1 ? int.Parse(args[1]) : 3;

parquetPath = ResolveParquetPath(parquetPath);
if (!File.Exists(parquetPath))
{
    Console.Error.WriteLine($"Parquet file not found: {parquetPath}");
    Console.Error.WriteLine("Generate one first:");
    Console.Error.WriteLine("  python scripts/generate_parquet.py --rows 10000000 --output benchmark-data/large.parquet");
    Environment.ExitCode = 1;
    return;
}

Console.WriteLine($"Parquet: {parquetPath}");
Console.WriteLine($"Iterations: {iterations}");

using SessionContext context = new();
context.RegisterParquet("files", "benchmark", parquetPath);

Measure("count", () =>
{
    using DataFrame df = context.Sql("SELECT * FROM files.benchmark");
    ulong rows = df.Count();
    Console.WriteLine($"  rows={rows:N0}");
});

const string aggregateSql = """
SELECT
  group_id,
  COUNT(*) AS row_count,
  SUM(quantity) AS total_quantity,
  AVG(amount) AS average_amount,
  SUM(amount) AS total_amount
FROM files.benchmark
WHERE is_active
GROUP BY group_id
ORDER BY total_amount DESC
LIMIT 10
""";

for (int iteration = 1; iteration <= iterations; iteration++)
{
    Measure($"aggregate #{iteration}", () =>
    {
        using DataFrame df = context.Sql(aggregateSql);
        df.Show();
    });
}

static void Measure(string name, Action action)
{
    Stopwatch stopwatch = Stopwatch.StartNew();
    action();
    stopwatch.Stop();
    Console.WriteLine($"{name}: {stopwatch.ElapsedMilliseconds:N0} ms");
}

static string ResolveParquetPath(string path)
{
    if (Path.IsPathRooted(path))
    {
        return Path.GetFullPath(path);
    }

    string? repositoryRoot = FindRepositoryRoot(AppContext.BaseDirectory)
        ?? FindRepositoryRoot(Directory.GetCurrentDirectory());

    return Path.GetFullPath(Path.Combine(repositoryRoot ?? Directory.GetCurrentDirectory(), path));
}

static string? FindRepositoryRoot(string startDirectory)
{
    DirectoryInfo? directory = new(startDirectory);
    while (directory is not null)
    {
        if (File.Exists(Path.Combine(directory.FullName, "datafusion-csharp.slnx")))
        {
            return directory.FullName;
        }

        directory = directory.Parent;
    }

    return null;
}
