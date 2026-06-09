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

public sealed partial class SessionContext
{
    public DataFrame Sql(string query)
    {
        EnsureOpen();
        using NativeUtf8String sql = new(query);
        NativeMethods.Check(NativeMethods.df_session_context_sql(handle, sql.Pointer, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame FromProto(byte[] planBytes)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(planBytes);
        using NativeByteArray plan = new(planBytes);
        NativeMethods.Check(NativeMethods.df_session_context_from_proto(handle, plan.Pointer, plan.Length, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }

    public DataFrame FromSubstrait(byte[] planBytes)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(planBytes);
        using NativeByteArray plan = new(planBytes);
        NativeMethods.Check(NativeMethods.df_session_context_from_substrait(handle, plan.Pointer, plan.Length, out IntPtr dataFrame));
        return new DataFrame(dataFrame);
    }
}
