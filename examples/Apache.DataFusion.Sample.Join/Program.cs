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

string peoplePath = WritePeopleCsv();
string ordersPath = WriteOrdersCsv();

using SessionContext context = new();
using DataFrame people = context.ReadCsv("files", "people", peoplePath, new CsvReadOptions { HasHeader = true });
using DataFrame orders = context.ReadCsv("files", "orders", ordersPath, new CsvReadOptions { HasHeader = true });

Console.WriteLine("Inner join of people and their orders:");
using DataFrame joined = people.Join(orders, JoinType.Inner, ["id"], ["person_id"]);
joined.Show();

static string WritePeopleCsv()
{
    string path = Path.Combine(WorkDirectory(), "people.csv");
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

static string WriteOrdersCsv()
{
    string path = Path.Combine(WorkDirectory(), "orders.csv");
    File.WriteAllText(
        path,
        """
        order_id,person_id,amount
        100,1,42.50
        101,1,12.00
        102,3,99.99
        103,4,5.25
        104,3,15.00
        """);
    return path;
}

static string WorkDirectory()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "join");
    Directory.CreateDirectory(directory);
    return directory;
}
