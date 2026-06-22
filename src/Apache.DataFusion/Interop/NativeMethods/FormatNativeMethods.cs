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

internal static partial class NativeMethods
{
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_parquet(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_parquet(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_csv(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_csv(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_json(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_json(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_arrow(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_arrow(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_avro(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_avro(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_register_excel(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_session_context_read_excel(IntPtr handle, IntPtr schemaName, IntPtr tableName, IntPtr path, IntPtr optionsPtr, nuint optionsLen, IntPtr schemaPtr, nuint schemaLen, out IntPtr dataFrame);
}
