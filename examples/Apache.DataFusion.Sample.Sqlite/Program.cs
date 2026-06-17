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

string connectionString = Environment.GetEnvironmentVariable("DATAFUSION_SQLITE_CONNECTION")
    ?? new SqliteConnectionStringBuilder
    {
        DataSource = Path.Combine(Path.GetTempPath(), "datafusion_sample.sqlite"),
    }.ToString();

using (SqliteConnection connection = new(connectionString))
{
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
}

using SessionContext context = new();
context.RegisterSqlite(connectionString);

Console.WriteLine("Customer spend from a SQLite-backed streaming table:");
using DataFrame df = context.Sql("""
    SELECT customer, SUM(total) AS spend
    FROM datafusion_orders
    GROUP BY customer
    ORDER BY customer
    """);
df.Show();
