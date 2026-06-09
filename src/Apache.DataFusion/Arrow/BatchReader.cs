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
using Apache.Arrow.Ipc;

namespace Apache.DataFusion;

public enum BatchReaderTransport
{
    ArrowIpcFallback,
}

public sealed class BatchReader : IDisposable
{
    private readonly MemoryStream stream;
    private readonly ArrowStreamReader reader;

    internal BatchReader(byte[] ipcBytes)
    {
        stream = new MemoryStream(ipcBytes, writable: false);
        reader = new ArrowStreamReader(stream);
    }

    public BatchReaderTransport Transport => BatchReaderTransport.ArrowIpcFallback;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
        reader.ReadNextRecordBatchAsync(cancellationToken);

    public void Dispose()
    {
        reader.Dispose();
        stream.Dispose();
    }
}
