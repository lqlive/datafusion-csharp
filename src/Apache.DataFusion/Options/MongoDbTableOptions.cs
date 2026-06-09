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

namespace Apache.DataFusion;

public sealed class MongoDbTableOptions
{
    public MongoDbTableOptions(string connectionString, string collectionName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("MongoDB connection string cannot be empty.", nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(collectionName))
        {
            throw new ArgumentException("MongoDB collection name cannot be empty.", nameof(collectionName));
        }

        ConnectionString = connectionString;
        CollectionName = collectionName;
    }

    public string ConnectionString { get; }

    public string CollectionName { get; }

    public static MongoDbTableOptions Create(string connectionString, string collectionName) =>
        new(connectionString, collectionName);
}
