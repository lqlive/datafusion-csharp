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
    internal static extern int df_dataframe_free(IntPtr handle);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_schema_ipc(IntPtr handle, out ByteBuffer buffer);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_collect_ipc(IntPtr handle, ulong tokenHandle, out ByteBuffer buffer);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_execute_stream_ipc(IntPtr handle, ulong tokenHandle, out ByteBuffer buffer);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_count(IntPtr handle, out ulong count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_show(IntPtr handle, int limit);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_explain(IntPtr handle, [MarshalAs(UnmanagedType.I1)] bool verbose, [MarshalAs(UnmanagedType.I1)] bool analyze, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_cache(IntPtr handle, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_describe(IntPtr handle, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_select(IntPtr handle, StringArray columns, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_filter(IntPtr handle, IntPtr predicate, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_limit(IntPtr handle, nuint skip, nuint fetch, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_distinct(IntPtr handle, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_drop_columns(IntPtr handle, StringArray columns, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_rename_column(IntPtr handle, IntPtr oldName, IntPtr newName, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_with_column(IntPtr handle, IntPtr name, IntPtr expression, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_unnest_columns(IntPtr handle, StringArray columns, [MarshalAs(UnmanagedType.I1)] bool preserveNulls, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_union(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_union_distinct(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_union_by_name(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_union_by_name_distinct(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_intersect(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_intersect_distinct(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_except(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_except_distinct(IntPtr left, IntPtr right, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_sort(IntPtr handle, StringArray columns, [In] bool[] ascending, [In] bool[] nullsFirst, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_repartition_round_robin(IntPtr handle, nuint partitions, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_repartition_hash(IntPtr handle, nuint partitions, StringArray columns, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_join(IntPtr left, IntPtr right, byte joinType, StringArray leftColumns, StringArray rightColumns, IntPtr filter, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_join_on(IntPtr left, IntPtr right, byte joinType, StringArray predicates, out IntPtr dataFrame);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_write_parquet(IntPtr handle, IntPtr path, IntPtr compression, [MarshalAs(UnmanagedType.I1)] bool singleFileOutput);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_write_csv(IntPtr handle, IntPtr path, IntPtr optionsPtr, nuint optionsLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_dataframe_write_json(IntPtr handle, IntPtr path, IntPtr optionsPtr, nuint optionsLen);
}
