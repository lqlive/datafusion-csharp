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

using Apache.Arrow;
using MySqlConnector;

namespace Apache.DataFusion.TableProviders.MySql;

internal sealed class StringColumnAppender(int ordinal) : ColumnAppender(ordinal)
{
    private readonly StringArray.Builder builder = new();

    public override void Append(MySqlDataReader reader)
    {
        if (reader.IsDBNull(Ordinal))
        {
            builder.AppendNull();
            return;
        }

        builder.Append(reader.GetString(Ordinal));
    }

    public override IArrowArray Build() => builder.Build();
}
