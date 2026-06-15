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

// Callback-driven streaming table providers: managed code owns the data source
// and hands DataFusion a fresh Arrow C Data Interface stream on every scan.
internal static partial class NativeMethods
{
    // Fills outStream with an FFI_ArrowArrayStream and returns 0 on success or a
    // non-zero status on failure.
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate int CallbackTableScan(IntPtr context, IntPtr outStream);

    // Frees the managed context handle. Invoked exactly once by the native side.
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CallbackTableRelease(IntPtr context);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_callback_table(
        IntPtr handle,
        IntPtr name,
        IntPtr schemaPtr,
        nuint schemaLen,
        CallbackTableScan scan,
        IntPtr context,
        CallbackTableRelease release);
}
