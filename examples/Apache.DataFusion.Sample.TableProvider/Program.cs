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

using SessionContext context = new();

// Materialize an in-memory table from a DataFrame and register it as a provider.
using DataFrame source = context.Sql(
    "SELECT * FROM (VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')) AS t(id, fruit)");
SimpleTableProvider provider = SimpleTableProvider.FromDataFrame(source);
context.RegisterTable("fruits", provider);

Console.WriteLine("Querying the registered in-memory table:");
using DataFrame df = context.Sql("SELECT * FROM fruits WHERE id > 1 ORDER BY id");
df.Show();
