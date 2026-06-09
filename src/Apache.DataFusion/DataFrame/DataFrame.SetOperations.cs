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
    public DataFrame Union(DataFrame other) => Binary(other, NativeMethods.df_dataframe_union);

    public DataFrame UnionDistinct(DataFrame other) => Binary(other, NativeMethods.df_dataframe_union_distinct);

    public DataFrame UnionByName(DataFrame other) => Binary(other, NativeMethods.df_dataframe_union_by_name);

    public DataFrame UnionByNameDistinct(DataFrame other) => Binary(other, NativeMethods.df_dataframe_union_by_name_distinct);

    public DataFrame Intersect(DataFrame other) => Binary(other, NativeMethods.df_dataframe_intersect);

    public DataFrame IntersectDistinct(DataFrame other) => Binary(other, NativeMethods.df_dataframe_intersect_distinct);

    public DataFrame Except(DataFrame other) => Binary(other, NativeMethods.df_dataframe_except);

    public DataFrame ExceptDistinct(DataFrame other) => Binary(other, NativeMethods.df_dataframe_except_distinct);

    private delegate int BinaryNative(IntPtr left, IntPtr right, out IntPtr dataFrame);

    private DataFrame Binary(DataFrame other, BinaryNative native)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureOpen();
        other.EnsureOpen();
        NativeMethods.Check(native(handle, other.handle, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }
}
