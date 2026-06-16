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

namespace Apache.DataFusion.TableProviders.ClickHouse;

public static class SessionContextExtensions
{
    public static void RegisterClickHouse(this SessionContext context, string name, ClickHouseTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);

        context.RegisterStreamingTable(name, new ClickHouseStreamingTableProvider(options));
    }

    public static void RegisterClickHouse(this SessionContext context, string name, string connectionString, string query)
    {
        context.RegisterClickHouse(name, new ClickHouseTableOptions
        {
            ConnectionString = connectionString,
            Query = query,
        });
    }
}
