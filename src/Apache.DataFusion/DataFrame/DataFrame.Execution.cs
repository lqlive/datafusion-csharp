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
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class DataFrame
{
    public ArrowBatchReader Collect() => Collect(CancellationToken.None);

    /// <summary>
    /// Execute the plan with cooperative cancellation. Firing
    /// <paramref name="cancellationToken"/> from another thread aborts the
    /// in-flight query at its next poll point and throws
    /// <see cref="OperationCanceledException"/>. A token that cannot be
    /// cancelled (e.g. <see cref="CancellationToken.None"/>) behaves exactly
    /// like the parameterless overload.
    /// </summary>
    public ArrowBatchReader Collect(CancellationToken cancellationToken)
    {
        return ReadNativeArrowStream(TakeHandle(), cancellationToken, NativeMethods.df_dataframe_collect_cdata);
    }

    internal byte[] CollectIpcBytes() => CollectIpcBytes(CancellationToken.None);

    internal byte[] CollectIpcBytes(CancellationToken cancellationToken)
    {
        IntPtr current = TakeHandle();
        if (!cancellationToken.CanBeCanceled)
        {
            NativeMethods.ThrowIfError(NativeMethods.df_dataframe_collect_ipc(current, 0UL, out NativeMethods.ByteBuffer buffer));
            return NativeMethods.CopyAndFree(buffer);
        }

        using NativeCancellationToken token = NativeCancellationToken.Create();
        using (cancellationToken.Register(static state => ((NativeCancellationToken)state!).Cancel(), token))
        {
            int status = NativeMethods.df_dataframe_collect_ipc(current, token.Handle, out NativeMethods.ByteBuffer buffer);
            NativeMethods.ThrowIfError(status, cancellationToken);
            return NativeMethods.CopyAndFree(buffer);
        }
    }

    public ArrowBatchReader ExecuteStream() => ExecuteStream(CancellationToken.None);

    /// <summary>
    /// Execute the plan and materialize the result with cooperative
    /// cancellation. Firing <paramref name="cancellationToken"/> from another
    /// thread aborts the in-flight query at its next poll point and throws
    /// <see cref="OperationCanceledException"/>. A token that cannot be
    /// cancelled (e.g. <see cref="CancellationToken.None"/>) behaves exactly
    /// like the parameterless overload.
    /// </summary>
    public ArrowBatchReader ExecuteStream(CancellationToken cancellationToken)
    {
        return ReadNativeArrowStream(TakeHandle(), cancellationToken, NativeMethods.df_dataframe_execute_stream_cdata);
    }

    /// <summary>
    /// Run a native exporter that fills an Arrow C Data Interface stream and
    /// import it as a managed reader. The native call fully drives the query
    /// (honouring <paramref name="cancellationToken"/>), then hands the
    /// materialized batches over by sharing their buffers rather than copying
    /// them through Arrow IPC.
    /// </summary>
    private static unsafe ArrowBatchReader ReadNativeArrowStream(
        IntPtr handle,
        CancellationToken cancellationToken,
        Func<IntPtr, ulong, IntPtr, int> nativeExport)
    {
        CArrowArrayStream* cStream = CArrowArrayStream.Create();
        try
        {
            if (!cancellationToken.CanBeCanceled)
            {
                NativeMethods.ThrowIfError(nativeExport(handle, 0UL, (IntPtr)cStream));
            }
            else
            {
                using NativeCancellationToken token = NativeCancellationToken.Create();
                using (cancellationToken.Register(static state => ((NativeCancellationToken)state!).Cancel(), token))
                {
                    int status = nativeExport(handle, token.Handle, (IntPtr)cStream);
                    NativeMethods.ThrowIfError(status, cancellationToken);
                }
            }

            IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(cStream);
            return new ArrowBatchReader(stream);
        }
        finally
        {
            // On success the importer moved the stream's contents out and nulled
            // the source release callback, so this only frees the container.
            // On any failure the release callback (if the native side set one)
            // still runs here before the allocation is freed.
            CArrowArrayStream.Free(cStream);
        }
    }

    public byte[] SchemaIpc()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_schema_ipc(Handle, out NativeMethods.ByteBuffer buffer));
        return NativeMethods.CopyAndFree(buffer);
    }

    public Schema Schema() => SchemaConverter.FromIpc(SchemaIpc());

    public ulong Count()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_count(Handle, out ulong count));
        return count;
    }

    public void Show() => Show(-1);

    public void Show(int limit)
    {
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_show(Handle, limit));
    }
}
