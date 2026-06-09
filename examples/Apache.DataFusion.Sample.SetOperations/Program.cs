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
using DataFrame left = context.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)");
using DataFrame right = context.Sql("SELECT * FROM (VALUES (2), (3), (4)) AS t(n)");

Console.WriteLine("UNION (distinct):");
using (DataFrame union = left.UnionDistinct(right))
{
    union.Show();
}

Console.WriteLine("INTERSECT:");
using (DataFrame intersect = left.Intersect(right))
{
    intersect.Show();
}

Console.WriteLine("EXCEPT:");
using (DataFrame except = left.Except(right))
{
    except.Show();
}
