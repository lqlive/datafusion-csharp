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
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_explain(Handle, verbose, analyze, out dataFrame));

    public DataFrame Cache() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_cache(Handle, out dataFrame));

    public DataFrame Describe() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_describe(Handle, out dataFrame));

    public DataFrame Select(params string[] columnNames)
    {
        using NativeStringArray columns = new(columnNames);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_select(Handle, columns.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Filter(string predicate)
    {
        using NativeUtf8String nativePredicate = new(predicate);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_filter(Handle, nativePredicate.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Limit(int fetch) => Limit(0, fetch);

    public DataFrame Limit(int skip, int fetch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfNegative(fetch);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_limit(Handle, (nuint)skip, (nuint)fetch, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Distinct() =>
        Unary(out IntPtr dataFrame, () => NativeMethods.df_dataframe_distinct(Handle, out dataFrame));

    public DataFrame DropColumns(params string[] columnNames)
    {
        using NativeStringArray columns = new(columnNames);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_drop_columns(Handle, columns.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame WithColumnRenamed(string oldName, string newName)
    {
        using NativeUtf8String oldNative = new(oldName);
        using NativeUtf8String newNative = new(newName);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_rename_column(Handle, oldNative.Pointer, newNative.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame WithColumn(string name, string expression)
    {
        using NativeUtf8String nativeName = new(name);
        using NativeUtf8String nativeExpression = new(expression);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_with_column(Handle, nativeName.Pointer, nativeExpression.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame UnnestColumns(IEnumerable<string> columns, bool preserveNulls = false)
    {
        using NativeStringArray nativeColumns = new(columns);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_unnest_columns(Handle, nativeColumns.Native, preserveNulls, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    /// <summary>
    /// Group the rows by zero or more grouping expressions and compute the
    /// supplied aggregate expressions. Each entry is a SQL expression evaluated
    /// against this frame's schema, e.g. grouping by <c>"category"</c> with
    /// aggregates <c>"sum(amount)"</c> and <c>"count(*)"</c>. Passing an empty
    /// <paramref name="groupExpressions"/> computes a single global aggregate.
    /// </summary>
    public DataFrame Aggregate(IEnumerable<string> groupExpressions, IEnumerable<string> aggregateExpressions)
    {
        ArgumentNullException.ThrowIfNull(groupExpressions);
        ArgumentNullException.ThrowIfNull(aggregateExpressions);
        using NativeStringArray nativeGroup = new(groupExpressions);
        using NativeStringArray nativeAggregate = new(aggregateExpressions);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_aggregate(Handle, nativeGroup.Native, nativeAggregate.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Sort(params SortExpr[] expressions)
    {
        string[] names = expressions.Select(expr => expr.ColumnName).ToArray();
        bool[] ascending = expressions.Select(expr => expr.Ascending).ToArray();
        bool[] nullsFirst = expressions.Select(expr => expr.NullsFirst).ToArray();
        using NativeStringArray columns = new(names);
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_sort(Handle, columns.Native, ascending, nullsFirst, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    private DataFrame Unary(out IntPtr dataFrame, Func<int> native)
    {
        dataFrame = IntPtr.Zero;
        NativeMethods.ThrowIfError(native());
        return new DataFrame(dataFrame);
    }
}
