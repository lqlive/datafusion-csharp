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

public enum ArrowBatchReaderTransport
{
    /// <summary>Batches are decoded from an Arrow IPC byte stream.</summary>
    ArrowIpcFallback,

    /// <summary>
    /// Batches are handed over through the Arrow C Data Interface, sharing the
    /// native buffers directly without an IPC serialize/deserialize round-trip.
    /// </summary>
    CDataInterface,
}

public sealed class ArrowBatchReader : IDisposable
{
    private readonly Func<CancellationToken, ValueTask<RecordBatch?>> readNext;
    private readonly Action dispose;
    private readonly ArrowBatchReaderTransport transport;

    internal ArrowBatchReader(byte[] ipcBytes)
    {
        MemoryStream stream = new(ipcBytes, writable: false);
        ArrowStreamReader reader = new(stream);
        readNext = reader.ReadNextRecordBatchAsync;
        dispose = () =>
        {
            reader.Dispose();
            stream.Dispose();
        };
        transport = ArrowBatchReaderTransport.ArrowIpcFallback;
    }

    internal ArrowBatchReader(IArrowArrayStream stream)
    {
        readNext = stream.ReadNextRecordBatchAsync;
        dispose = stream.Dispose;
        transport = ArrowBatchReaderTransport.CDataInterface;
    }

    public ArrowBatchReaderTransport Transport => transport;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
        readNext(cancellationToken);

    public void Dispose() => dispose();
}
