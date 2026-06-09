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

internal sealed class NativeStringArray : IDisposable
{
    private readonly NativeUtf8String[] strings;
    private readonly IntPtr pointerArray;

    public NativeStringArray(IEnumerable<string> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        strings = values.Select(value => new NativeUtf8String(value)).ToArray();

        IntPtr[] pointers = strings.Select(value => value.Pointer).ToArray();
        pointerArray = Marshal.AllocCoTaskMem(IntPtr.Size * pointers.Length);
        for (int i = 0; i < pointers.Length; i++)
        {
            Marshal.WriteIntPtr(pointerArray, i * IntPtr.Size, pointers[i]);
        }

        Native = new NativeMethods.StringArray(pointerArray, (nuint)pointers.Length);
    }

    public NativeMethods.StringArray Native { get; }

    public void Dispose()
    {
        Marshal.FreeCoTaskMem(pointerArray);
        foreach (NativeUtf8String value in strings)
        {
            value.Dispose();
        }
    }
}
