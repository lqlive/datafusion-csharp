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

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class SessionContext
{
    private readonly List<NativeMethods.ScalarI64Callback> scalarCallbacks = [];

    // One function pointer per delegate is enough: the native side distinguishes
    // providers through the per-registration context handle, not the callback.
    // Holding the instances in static fields keeps them rooted for the lifetime
    // of any table that may invoke them from native worker threads.
    private static readonly NativeMethods.CallbackTableScan StreamingScanCallback = StreamingScanThunk;
    private static readonly NativeMethods.CallbackTableRelease StreamingReleaseCallback = StreamingReleaseThunk;

    public void RegisterTable(string name, TableProvider tableProvider)
    {
        ArgumentNullException.ThrowIfNull(tableProvider);
        using NativeUtf8String nativeName = new(name);
        using NativeByteArray ipc = new(tableProvider.ToIpcStream());
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_register_table_ipc(Handle, nativeName.Pointer, ipc.Pointer, ipc.Length));
    }

    /// <summary>
    /// Register a lazy, streaming table backed by managed code. DataFusion calls
    /// <see cref="StreamingTableProvider.Scan"/> on every scan and consumes the
    /// returned <see cref="IArrowArrayStream"/> over the Arrow C Data Interface,
    /// so data is produced on demand without a native database driver. The
    /// provider is kept alive until this context is disposed.
    /// </summary>
    public void RegisterStreamingTable(string name, StreamingTableProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Table name cannot be null or whitespace.", nameof(name));
        }

        byte[] schemaIpc = SerializeSchema(provider.Schema);
        GCHandle gcHandle = GCHandle.Alloc(provider);
        IntPtr context = GCHandle.ToIntPtr(gcHandle);
        using NativeUtf8String nativeName = new(name);
        using NativeByteArray schema = new(schemaIpc);

        // The native adapter takes ownership of the context handle and always
        // invokes the release callback exactly once - synchronously here on
        // failure, or when the table is dropped on success - so the handle is
        // freed in StreamingReleaseThunk and never here.
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_register_callback_table(
            Handle,
            nativeName.Pointer,
            schema.Pointer,
            schema.Length,
            StreamingScanCallback,
            context,
            StreamingReleaseCallback));
    }

    public void RegisterScalarUdf(ScalarUdf udf)
    {
        ArgumentNullException.ThrowIfNull(udf);
        using NativeUtf8String nativeName = new(udf.Name);
        NativeMethods.ScalarI64Callback callback = (IntPtr _, out long value) =>
        {
            value = 0;
            try
            {
                ColumnarValue result = udf.Function(ScalarFunctionArgs.Empty);
                if (result is not ColumnarValue.Int64Scalar scalar || !scalar.Value.HasValue)
                {
                    return 1;
                }

                value = scalar.Value.Value;
                return 0;
            }
            catch
            {
                return 1;
            }
        };
        scalarCallbacks.Add(callback);
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_register_scalar_udf_i64(Handle, nativeName.Pointer, (byte)udf.Volatility, callback, IntPtr.Zero));
    }

    private static unsafe int StreamingScanThunk(IntPtr context, IntPtr outStream)
    {
        try
        {
            if (GCHandle.FromIntPtr(context).Target is not StreamingTableProvider provider)
            {
                return 1;
            }

            IArrowArrayStream stream = provider.Scan();
            CArrowArrayStreamExporter.ExportArrayStream(stream, (CArrowArrayStream*)outStream);
            return 0;
        }
        catch
        {
            return 1;
        }
    }

    private static void StreamingReleaseThunk(IntPtr context)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(context);
            if (gcHandle.IsAllocated)
            {
                gcHandle.Free();
            }
        }
        catch
        {
            // Releasing the handle is best-effort; never let an exception cross
            // back into native code.
        }
    }

    private static byte[] SerializeSchema(Schema schema)
    {
        using MemoryStream stream = new();
        using (ArrowStreamWriter writer = new(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteEnd();
        }

        return stream.ToArray();
    }
}
