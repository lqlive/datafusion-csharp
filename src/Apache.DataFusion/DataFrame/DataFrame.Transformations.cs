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

using Apache.DataFusion.Interop;

namespace Apache.DataFusion;

public sealed partial class DataFrame
{
    public DataFrame Explain(bool verbose = false, bool analyze = false) =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_explain(handle, verbose, analyze, out dataFrame));

    public DataFrame Cache() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_cache(handle, out dataFrame));

    public DataFrame Describe() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_describe(handle, out dataFrame));

    public DataFrame Select(params string[] columnNames)
    {
        EnsureOpen();
        using NativeStringArray columns = new(columnNames);
        NativeMethods.Check(NativeMethods.df_dataframe_select(handle, columns.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Filter(string predicate)
    {
        EnsureOpen();
        using NativeUtf8String nativePredicate = new(predicate);
        NativeMethods.Check(NativeMethods.df_dataframe_filter(handle, nativePredicate.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Limit(int fetch) => Limit(0, fetch);

    public DataFrame Limit(int skip, int fetch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfNegative(fetch);
        EnsureOpen();
        NativeMethods.Check(NativeMethods.df_dataframe_limit(handle, (nuint)skip, (nuint)fetch, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Distinct() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_distinct(handle, out dataFrame));

    public DataFrame DropColumns(params string[] columnNames)
    {
        EnsureOpen();
        using NativeStringArray columns = new(columnNames);
        NativeMethods.Check(NativeMethods.df_dataframe_drop_columns(handle, columns.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame WithColumnRenamed(string oldName, string newName)
    {
        EnsureOpen();
        using NativeUtf8String oldNative = new(oldName);
        using NativeUtf8String newNative = new(newName);
        NativeMethods.Check(NativeMethods.df_dataframe_rename_column(handle, oldNative.Pointer, newNative.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame WithColumn(string name, string expression)
    {
        EnsureOpen();
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeExpression = new(expression);
        NativeMethods.Check(NativeMethods.df_dataframe_with_column(handle, nativeName.Pointer, nativeExpression.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame UnnestColumns(IEnumerable<string> columns, bool preserveNulls = false)
    {
        EnsureOpen();
        using NativeStringArray nativeColumns = new(columns);
        NativeMethods.Check(NativeMethods.df_dataframe_unnest_columns(handle, nativeColumns.Native, preserveNulls, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Sort(params SortExpr[] expressions)
    {
        EnsureOpen();
        string[] names = expressions.Select(expr => expr.ColumnName).ToArray();
        bool[] ascending = expressions.Select(expr => expr.Ascending).ToArray();
        bool[] nullsFirst = expressions.Select(expr => expr.NullsFirst).ToArray();
        using NativeStringArray columns = new(names);
        NativeMethods.Check(NativeMethods.df_dataframe_sort(handle, columns.Native, ascending, nullsFirst, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    private DataFrame Unary(out IntPtr dataFrame, Func<int> native)
    {
        EnsureOpen();
        dataFrame = IntPtr.Zero;
        NativeMethods.Check(native());
        return new DataFrame(dataFrame);
    }
}
