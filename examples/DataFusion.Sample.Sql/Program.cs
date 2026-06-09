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

using SessionContext context = new();
context.RegisterCsv("people", csvPath, new CsvReadOptions { HasHeader = true });

Console.WriteLine("People aged 30 or older:");
using DataFrame df = context.Sql(
    "SELECT id, name, age, city FROM people WHERE age >= 30 ORDER BY id");
df.Show();
Console.WriteLine($"Match count: {df.Count()}");

static string WritePeopleCsv()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "sql");
    Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, "people.csv");
    File.WriteAllText(
        path,
        """
        id,name,age,city
        1,Alice,34,Seattle
        2,Bob,29,Portland
        3,Carol,42,Seattle
        4,Dave,38,Austin
        5,Erin,25,Portland
        """);
    return path;
}
