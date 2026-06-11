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
using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class DataFrame
{
    public ArrowBatchReader Collect()
    {
        return new ArrowBatchReader(CollectIpcBytes());
    }

    internal byte[] CollectIpcBytes()
    {
        IntPtr current = TakeHandle();
        NativeMethods.Check(NativeMethods.df_dataframe_collect_ipc(current, out NativeMethods.ByteBuffer buffer));
        return NativeMethods.CopyAndFree(buffer);
    }

    public ArrowBatchReader ExecuteStream()
    {
        IntPtr current = TakeHandle();
        NativeMethods.Check(NativeMethods.df_dataframe_execute_stream_ipc(current, out NativeMethods.ByteBuffer buffer));
        return new ArrowBatchReader(NativeMethods.CopyAndFree(buffer));
    }

    public byte[] SchemaIpc()
    {
        EnsureOpen();
        NativeMethods.Check(NativeMethods.df_dataframe_schema_ipc(handle, out NativeMethods.ByteBuffer buffer));
        return NativeMethods.CopyAndFree(buffer);
    }

    public Schema Schema() => SchemaConverter.FromIpc(SchemaIpc());

    public ulong Count()
    {
        EnsureOpen();
        NativeMethods.Check(NativeMethods.df_dataframe_count(handle, out ulong count));
        return count;
    }

    public void Show() => Show(-1);

    public void Show(int limit)
    {
        EnsureOpen();
        NativeMethods.Check(NativeMethods.df_dataframe_show(handle, limit));
    }
}
