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
    public DataFrame RepartitionRoundRobin(int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        EnsureOpen();
        NativeMethods.Check(NativeMethods.df_dataframe_repartition_round_robin(handle, (nuint)partitions, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame RepartitionHash(int partitions, params string[] columns)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        EnsureOpen();
        using NativeStringArray nativeColumns = new(columns);
        NativeMethods.Check(NativeMethods.df_dataframe_repartition_hash(handle, (nuint)partitions, nativeColumns.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame Join(DataFrame right, JoinType joinType, string[] leftColumns, string[] rightColumns, string? filter = null)
    {
        ArgumentNullException.ThrowIfNull(right);
        EnsureOpen();
        right.EnsureOpen();
        using NativeStringArray leftNative = new(leftColumns);
        using NativeStringArray rightNative = new(rightColumns);
        using NativeUtf8String? filterNative = filter is null ? null : new NativeUtf8String(filter);
        NativeMethods.Check(NativeMethods.df_dataframe_join(handle, right.handle, (byte)joinType, leftNative.Native, rightNative.Native, filterNative?.Pointer ?? IntPtr.Zero, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame JoinOn(DataFrame right, JoinType joinType, params string[] predicates)
    {
        ArgumentNullException.ThrowIfNull(right);
        EnsureOpen();
        right.EnsureOpen();
        using NativeStringArray predicateNative = new(predicates);
        NativeMethods.Check(NativeMethods.df_dataframe_join_on(handle, right.handle, (byte)joinType, predicateNative.Native, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }
}
