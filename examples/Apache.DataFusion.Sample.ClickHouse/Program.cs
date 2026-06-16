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
using Apache.DataFusion.TableProviders.ClickHouse;

using ClickHouse.Client.ADO;

string? connectionString = Environment.GetEnvironmentVariable("DATAFUSION_CLICKHOUSE_CONNECTION");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("Set DATAFUSION_CLICKHOUSE_CONNECTION to run this sample.");
    Console.WriteLine("Example:");
    Console.WriteLine("  Host=localhost;Port=8123;Username=default;Password=;Database=default");
    return;
}

using (ClickHouseConnection connection = new(connectionString))
{
    connection.Open();
    Execute(connection, """
        CREATE TABLE IF NOT EXISTS datafusion_orders
        (
            id Int32,
            customer String,
            total Float64
        )
        ENGINE = Memory
        """);
    Execute(connection, "TRUNCATE TABLE datafusion_orders");
    Execute(connection, """
        INSERT INTO datafusion_orders (id, customer, total) VALUES
            (1, 'alice', 19.99),
            (2, 'bob', 7.50),
            (3, 'alice', 100.00),
            (4, 'carol', 42.25)
        """);
}

using SessionContext context = new();
context.RegisterClickHouse("orders", new ClickHouseTableOptions
{
    ConnectionString = connectionString,
    Query = """SELECT id, customer, total FROM datafusion_orders""",
});

Console.WriteLine("Customer spend from a ClickHouse-backed streaming table:");
using DataFrame df = context.Sql("""
    SELECT customer, SUM(total) AS spend
    FROM orders
    WHERE total >= 10
    GROUP BY customer
    ORDER BY customer
    """);
df.Show();

static void Execute(ClickHouseConnection connection, string sql)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    command.ExecuteNonQuery();
}
