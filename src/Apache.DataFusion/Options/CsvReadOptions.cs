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

public sealed class CsvReadOptions : IReadOptions
{
    public byte[]? SchemaIpc { get; set; }

    public bool HasHeader { get; set; }

    public byte Delimiter { get; set; } = (byte)',';

    public byte Quote { get; set; } = (byte)'"';

    public byte? Terminator { get; set; }

    public byte? Escape { get; set; }

    public byte? Comment { get; set; }

    public bool? NewlinesInValues { get; set; }

    public ulong? SchemaInferMaxRecords { get; set; }

    public string FileExtension { get; set; } = ".csv";

    public FileCompressionType FileCompressionType { get; set; } = FileCompressionType.Uncompressed;

    public byte[] ToBytes()
    {
        Proto.CsvReadOptionsProto proto = new()
        {
            HasHeader = HasHeader,
            Delimiter = Delimiter,
            Quote = Quote,
            FileExtension = FileExtension,
            FileCompressionType = FileCompressionTypeMapping.ToProto(FileCompressionType),
        };
        if (Terminator.HasValue)
        {
            proto.Terminator = Terminator.Value;
        }

        if (Escape.HasValue)
        {
            proto.Escape = Escape.Value;
        }

        if (Comment.HasValue)
        {
            proto.Comment = Comment.Value;
        }

        if (NewlinesInValues.HasValue)
        {
            proto.NewlinesInValues = NewlinesInValues.Value;
        }

        if (SchemaInferMaxRecords.HasValue)
        {
            proto.SchemaInferMaxRecords = SchemaInferMaxRecords.Value;
        }

        return proto.ToByteArray();
    }
}
