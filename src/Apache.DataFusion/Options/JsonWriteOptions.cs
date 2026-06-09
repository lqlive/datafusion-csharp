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

using Google.Protobuf;

namespace Apache.DataFusion;

public sealed class JsonWriteOptions
{
    public bool? SingleFileOutput { get; set; }

    public IList<string> PartitionColumns { get; } = new List<string>();

    public FileCompressionType? FileCompressionType { get; set; }

    internal byte[] ToBytes()
    {
        Proto.JsonWriteOptionsProto proto = new();
        if (SingleFileOutput.HasValue)
        {
            proto.SingleFileOutput = SingleFileOutput.Value;
        }

        proto.PartitionCols.AddRange(PartitionColumns);
        if (FileCompressionType.HasValue)
        {
            proto.FileCompressionType = FileCompressionTypeMapping.ToProto(FileCompressionType.Value);
        }

        return proto.ToByteArray();
    }
}
