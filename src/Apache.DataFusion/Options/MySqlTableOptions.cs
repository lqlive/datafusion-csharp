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

public sealed class MySqlTableOptions
{
    public MySqlTableOptions(string connectionString, string tableName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("MySQL connection string cannot be empty.", nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("MySQL table name cannot be empty.", nameof(tableName));
        }

        ConnectionString = connectionString;
        TableName = tableName;
    }

    public string ConnectionString { get; }

    public string? SchemaName { get; init; }

    public string TableName { get; }

    public static MySqlTableOptions Create(string connectionString, string tableName) =>
        new(connectionString, tableName);
}
