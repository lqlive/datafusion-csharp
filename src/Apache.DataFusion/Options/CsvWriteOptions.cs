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

public sealed class CsvWriteOptions
{
    public bool? SingleFileOutput { get; set; }

    public IList<string> PartitionColumns { get; } = new List<string>();

    public bool? HasHeader { get; set; }

    public byte? Delimiter { get; set; }

    public byte? Quote { get; set; }

    public byte? Escape { get; set; }

    public string? NullValue { get; set; }

    public FileCompressionType? FileCompressionType { get; set; }

    internal byte[] ToBytes()
    {
        Proto.CsvWriteOptionsProto proto = new();
        if (SingleFileOutput.HasValue)
        {
            proto.SingleFileOutput = SingleFileOutput.Value;
        }

        proto.PartitionCols.AddRange(PartitionColumns);
        if (HasHeader.HasValue)
        {
            proto.HasHeader = HasHeader.Value;
        }

        if (Delimiter.HasValue)
        {
            proto.Delimiter = Delimiter.Value;
        }

        if (Quote.HasValue)
        {
            proto.Quote = Quote.Value;
        }

        if (Escape.HasValue)
        {
            proto.Escape = Escape.Value;
        }

        if (NullValue is not null)
        {
            proto.NullValue = NullValue;
        }

        if (FileCompressionType.HasValue)
        {
            proto.FileCompressionType = FileCompressionTypeMapping.ToProto(FileCompressionType.Value);
        }

        return proto.ToByteArray();
    }
}
