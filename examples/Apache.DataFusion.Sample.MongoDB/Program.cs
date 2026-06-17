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
using Apache.DataFusion.TableProviders.MongoDB;

using MongoDB.Bson;
using MongoDB.Driver;

string? connectionString = Environment.GetEnvironmentVariable("DATAFUSION_MONGODB_CONNECTION");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("Set DATAFUSION_MONGODB_CONNECTION to run this sample.");
    Console.WriteLine("Example:");
    Console.WriteLine("  mongodb://localhost:27017");
    return;
}

const string databaseName = "datafusion_sample";
const string collectionName = "orders";

MongoClient client = new(connectionString);
IMongoCollection<BsonDocument> collection = client
    .GetDatabase(databaseName)
    .GetCollection<BsonDocument>(collectionName);

collection.DeleteMany(Builders<BsonDocument>.Filter.Empty);
collection.InsertMany(
[
    new BsonDocument
    {
        ["id"] = 1,
        ["customer"] = "alice",
        ["total"] = 19.99,
    },
    new BsonDocument
    {
        ["id"] = 2,
        ["customer"] = "bob",
        ["total"] = 7.50,
    },
    new BsonDocument
    {
        ["id"] = 3,
        ["customer"] = "alice",
        ["total"] = 100.00,
    },
    new BsonDocument
    {
        ["id"] = 4,
        ["customer"] = "carol",
        ["total"] = 42.25,
    },
]);

using SessionContext context = new();
context.RegisterMongoDb(new MongoDbDatabaseOptions
{
    ConnectionString = connectionString,
    DatabaseName = databaseName,
});

Console.WriteLine("Customer spend from a MongoDB-backed streaming table:");
using DataFrame df = context.Sql("""
    SELECT customer, SUM(total) AS spend
    FROM orders
    WHERE total >= 10
    GROUP BY customer
    ORDER BY customer
    """);
df.Show();
