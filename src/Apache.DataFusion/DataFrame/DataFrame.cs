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

public sealed partial class DataFrame : IDisposable
{
    private IntPtr handle;

    internal DataFrame(IntPtr handle)
    {
        if (handle == IntPtr.Zero)
        {
            throw new ArgumentException("DataFrame native handle is null.", nameof(handle));
        }

        this.handle = handle;
    }

    public void Dispose()
    {
        IntPtr current = handle;
        if (current == IntPtr.Zero)
        {
            return;
        }

        handle = IntPtr.Zero;
        NativeMethods.ThrowIfError(NativeMethods.df_dataframe_free(current));
    }

    // Validates on every access so call sites can use the handle directly
    // without a separate guard statement; throws once the DataFrame is closed
    // or has been consumed by a terminal operation.
    private IntPtr Handle
    {
        get
        {
            if (handle == IntPtr.Zero)
            {
                throw new ObjectDisposedException(nameof(DataFrame), "DataFrame is closed or already consumed.");
            }

            return handle;
        }
    }

    private IntPtr TakeHandle()
    {
        IntPtr current = Handle;
        handle = IntPtr.Zero;
        return current;
    }
}
