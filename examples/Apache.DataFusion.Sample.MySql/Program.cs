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
using Apache.DataFusion.TableProviders.MySql;
using MySqlConnector;

string? connectionString = Environment.GetEnvironmentVariable("DATAFUSION_MYSQL_CONNECTION");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("Set DATAFUSION_MYSQL_CONNECTION to run this sample.");
    Console.WriteLine("Example:");
    Console.WriteLine("  Server=localhost;User ID=root;Password=pass;Database=datafusion_sample");
    return;
}

using (MySqlConnection connection = new(connectionString))
{
    connection.Open();
    using MySqlCommand command = connection.CreateCommand();
    command.CommandText = """
        CREATE TABLE IF NOT EXISTS datafusion_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(64) NOT NULL,
            total DOUBLE NOT NULL
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
context.RegisterMySql(connectionString);

Console.WriteLine("Customer spend from a MySQL-backed streaming table:");
using DataFrame df = context.Sql("""
    SELECT customer, SUM(total) AS spend
    FROM datafusion_orders
    GROUP BY customer
    ORDER BY customer
    """);
df.Show();
