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

namespace Apache.DataFusion;

public sealed class SimpleTableProvider : TableProvider
{
    private readonly byte[] ipcStream;

    private SimpleTableProvider(byte[] ipcStream)
    {
        this.ipcStream = ipcStream;
    }

    public static SimpleTableProvider FromIpcStream(byte[] ipcStream)
    {
        ArgumentNullException.ThrowIfNull(ipcStream);
        return new SimpleTableProvider((byte[])ipcStream.Clone());
    }

    public static SimpleTableProvider FromDataFrame(DataFrame dataFrame)
    {
        ArgumentNullException.ThrowIfNull(dataFrame);
        return new SimpleTableProvider(dataFrame.CollectIpcBytes());
    }

    internal override byte[] ToIpcStream() => (byte[])ipcStream.Clone();
}
