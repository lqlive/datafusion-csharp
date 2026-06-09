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

namespace Apache.DataFusion.Interop;

internal sealed class NativeByteArray : IDisposable
{
    public NativeByteArray(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        Length = (nuint)bytes.Length;
        if (bytes.Length == 0)
        {
            Pointer = IntPtr.Zero;
            return;
        }

        Pointer = Marshal.AllocCoTaskMem(bytes.Length);
        Marshal.Copy(bytes, 0, Pointer, bytes.Length);
    }

    public IntPtr Pointer { get; private set; }

    public nuint Length { get; }

    public void Dispose()
    {
        IntPtr ptr = Pointer;
        if (ptr != IntPtr.Zero)
        {
            Pointer = IntPtr.Zero;
            Marshal.FreeCoTaskMem(ptr);
        }
    }
}
