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

using MongoDB.Driver;

namespace Apache.DataFusion.TableProviders.MongoDB;

public static class SessionContextExtensions
{
    public static void RegisterMongoDb(this SessionContext context, string connectionString)
    {
        context.RegisterMongoDb(new MongoDbDatabaseOptions
        {
            ConnectionString = connectionString,
        });
    }

    public static void RegisterMongoDb(this SessionContext context, MongoDbDatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be greater than zero.");
        }

        if (options.SchemaInferenceLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.SchemaInferenceLimit, "Schema inference limit must be greater than zero.");
        }

        if (options.DefaultLimit.HasValue && options.DefaultLimit.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.DefaultLimit, "Default limit must be greater than zero when specified.");
        }

        string databaseName = ResolveDatabaseName(options.ConnectionString, options.DatabaseName);
        IMongoClient client = options.Client ?? CreateClient(options.ConnectionString, nameof(options));
        IMongoDatabase database = client.GetDatabase(databaseName);
        foreach (string collectionName in database.ListCollectionNames().ToEnumerable())
        {
            RegisterSourceTable(context, options.SourceName, collectionName, new MongoDbStreamingTableProvider(new MongoDbTableOptions
            {
                ConnectionString = options.ConnectionString,
                Client = client,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                BatchSize = options.BatchSize,
                SchemaInferenceLimit = options.SchemaInferenceLimit,
                DefaultLimit = options.DefaultLimit,
            }));
        }
    }

    public static void RegisterMongoDb(this SessionContext context, string name, MongoDbTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);
        if (options.DefaultLimit.HasValue && options.DefaultLimit.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.DefaultLimit, "Default limit must be greater than zero when specified.");
        }

        context.RegisterStreamingTable(name, new MongoDbStreamingTableProvider(options));
    }

    public static void RegisterMongoDb(this SessionContext context, string name, string connectionString)
    {
        string databaseName = ResolveDatabaseName(connectionString, null);
        context.RegisterMongoDb(name, new MongoDbTableOptions
        {
            ConnectionString = connectionString,
            DatabaseName = databaseName,
            CollectionName = name,
        });
    }

    public static void RegisterMongoDb(
        this SessionContext context,
        string name,
        string connectionString,
        string databaseName,
        string collectionName)
    {
        context.RegisterMongoDb(name, new MongoDbTableOptions
        {
            ConnectionString = connectionString,
            DatabaseName = databaseName,
            CollectionName = collectionName,
        });
    }

    private static IMongoClient CreateClient(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace when a MongoDB client is not provided.", parameterName);
        }

        return new MongoClient(connectionString);
    }

    private static string ResolveDatabaseName(string? connectionString, string? databaseName)
    {
        if (!string.IsNullOrWhiteSpace(databaseName))
        {
            return databaseName;
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Database name must be provided in MongoDB options when a connection string is not provided.", nameof(databaseName));
        }

        string? connectionDatabaseName = MongoUrl.Create(connectionString).DatabaseName;
        if (!string.IsNullOrWhiteSpace(connectionDatabaseName))
        {
            return connectionDatabaseName;
        }

        throw new ArgumentException("Database name must be provided in MongoDB options or connection string.", nameof(connectionString));
    }

    private static void RegisterSourceTable(SessionContext context, string? sourceName, string tableName, StreamingTableProvider provider)
    {
        if (string.IsNullOrWhiteSpace(sourceName))
        {
            context.RegisterStreamingTable(tableName, provider);
            return;
        }

        context.RegisterStreamingTable(sourceName, tableName, provider);
    }
}
