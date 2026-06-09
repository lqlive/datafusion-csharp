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

string jsonPath = WriteEventsJson();

using SessionContext context = new();
context.RegisterJson("events", jsonPath);

Console.WriteLine("Event counts and totals by type:");
using DataFrame df = context.Sql(
    "SELECT type, COUNT(*) AS n, SUM(value) AS total FROM events GROUP BY type ORDER BY total DESC");
df.Show();

static string WriteEventsJson()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "aggregate");
    Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, "events.json");
    File.WriteAllText(
        path,
        """
        {"id":1,"type":"click","value":3}
        {"id":2,"type":"view","value":1}
        {"id":3,"type":"click","value":5}
        {"id":4,"type":"purchase","value":20}
        {"id":5,"type":"click","value":2}
        """);
    return path;
}
