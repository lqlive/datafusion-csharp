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
using Apache.DataFusion.TableProviders.Sqlite;
using Microsoft.Data.Sqlite;

string sqliteConnectionString = CreateSqliteOrders();
string customerRegionsPath = WriteCustomerRegionsCsv();

using SessionContext context = new();
context.RegisterSqlite(sqliteConnectionString);
context.RegisterCsv("customer_regions", customerRegionsPath, new CsvReadOptions { HasHeader = true });

Console.WriteLine("Federated query joining SQLite orders with CSV customer regions:");
using DataFrame df = context.Sql("""
    SELECT r.region, o.customer, SUM(o.total) AS spend
    FROM datafusion_orders AS o
    INNER JOIN customer_regions AS r
        ON o.customer = r.customer
    GROUP BY r.region, o.customer
    ORDER BY r.region, o.customer
    """);
df.Show();

static string CreateSqliteOrders()
{
    string directory = WorkDirectory();
    string databasePath = Path.Combine(directory, "orders.sqlite");
    string connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = databasePath,
    }.ToString();

    using SqliteConnection connection = new(connectionString);
    connection.Open();
    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = """
        CREATE TABLE IF NOT EXISTS datafusion_orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            total REAL NOT NULL
        );

        DELETE FROM datafusion_orders;

        INSERT INTO datafusion_orders (id, customer, total) VALUES
            (1, 'alice', 19.99),
            (2, 'bob', 7.50),
            (3, 'alice', 100.00),
            (4, 'carol', 42.25);
        """;
    command.ExecuteNonQuery();

    return connectionString;
}

static string WriteCustomerRegionsCsv()
{
    string path = Path.Combine(WorkDirectory(), "customer_regions.csv");
    File.WriteAllText(
        path,
        """
        customer,region
        alice,west
        bob,east
        carol,central
        """);
    return path;
}

static string WorkDirectory()
{
    string directory = Path.Combine(Path.GetTempPath(), "datafusion-csharp-examples", "federated-query");
    Directory.CreateDirectory(directory);
    return directory;
}
