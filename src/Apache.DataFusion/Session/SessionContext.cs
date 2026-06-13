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

public sealed partial class SessionContext : IDisposable
{
    private IntPtr handle;

    public SessionContext()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_new(out handle));
        EnsureNativeHandle();
    }

    internal SessionContext(byte[] optionsBytes)
    {
        using NativeByteArray options = new(optionsBytes);
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_new_with_options(options.Pointer, options.Length, out handle));
        EnsureNativeHandle();
    }

    public static SessionContextBuilder CreateBuilder() => new();

    [Obsolete("Use CreateBuilder instead.")]
    public static SessionContextBuilder Builder() => CreateBuilder();

    public void Dispose()
    {
        IntPtr current = handle;
        if (current == IntPtr.Zero)
        {
            return;
        }

        handle = IntPtr.Zero;
        NativeMethods.ThrowIfError(NativeMethods.df_session_context_free(current));
    }

    private void EnsureNativeHandle()
    {
        if (handle == IntPtr.Zero)
        {
            throw new DataFusionException("Native SessionContext handle was null.");
        }
    }

    // Validates on every access so call sites can use the handle directly
    // without a separate guard statement; throws once the context is disposed.
    // Visible to table-provider factories in this assembly so they can register
    // into the context.
    internal IntPtr Handle
    {
        get
        {
            if (handle == IntPtr.Zero)
            {
                throw new ObjectDisposedException(nameof(SessionContext));
            }

            return handle;
        }
    }
}
