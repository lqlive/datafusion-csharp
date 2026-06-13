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

namespace Apache.DataFusion.Interop;

/// <summary>
/// Owns a native cancellation token registered in the process-global registry.
/// One instance bridges a managed <see cref="CancellationToken"/> to an
/// in-flight native query: when the managed token fires, <see cref="Cancel"/>
/// signals the native token so a query parked inside the blocking FFI call
/// aborts at its next poll point.
/// </summary>
internal sealed class NativeCancellationToken : IDisposable
{
    // Stored as long (not ulong) so Interlocked can guard the close/cancel race.
    // The native registry hands out monotonically increasing IDs starting at 1,
    // so the value never approaches the signed range and 0 is the freed sentinel.
    private long handle;

    private NativeCancellationToken(long handle)
    {
        this.handle = handle;
    }

    public static NativeCancellationToken Create()
    {
        NativeMethods.ThrowIfError(NativeMethods.df_cancellation_token_new(out ulong handle));
        return new NativeCancellationToken((long)handle);
    }

    public ulong Handle => (ulong)Interlocked.Read(ref handle);

    public void Cancel()
    {
        long current = Interlocked.Read(ref handle);
        if (current != 0)
        {
            NativeMethods.ThrowIfError(NativeMethods.df_cancellation_token_cancel((ulong)current));
        }
    }

    public void Dispose()
    {
        long current = Interlocked.Exchange(ref handle, 0);
        if (current != 0)
        {
            NativeMethods.ThrowIfError(NativeMethods.df_cancellation_token_free((ulong)current));
        }
    }
}
