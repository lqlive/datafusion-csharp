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

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Apache.DataFusion.Interop;

internal static partial class NativeMethods
{
    internal const string LibraryName = "datafusion_csharp_native";

    static NativeMethods()
    {
        NativeLibraryLoader.Load();
    }

    [StructLayout(LayoutKind.Sequential)]
    internal readonly struct ByteBuffer
    {
        public ByteBuffer(IntPtr ptr, UIntPtr len)
        {
            Ptr = ptr;
            Len = len;
        }

        public IntPtr Ptr { get; }

        public UIntPtr Len { get; }
    }

    [StructLayout(LayoutKind.Sequential)]
    internal readonly struct StringArray
    {
        public StringArray(IntPtr ptr, UIntPtr len)
        {
            Ptr = ptr;
            Len = len;
        }

        public IntPtr Ptr { get; }

        public UIntPtr Len { get; }
    }

    internal enum NativeExceptionKind
    {
        DataFusion = 0,
        Plan = 1,
        Execution = 2,
        ResourcesExhausted = 3,
        Io = 4,
        NotImplemented = 5,
        Configuration = 6,
    }

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int df_last_error_kind();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr df_last_error_message();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void df_string_free(IntPtr ptr);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void df_byte_buffer_free(ByteBuffer buffer);

    internal static void ThrowIfError(int status)
    {
        if (status == 0)
        {
            return;
        }

        IntPtr messagePtr = df_last_error_message();
        string message = Marshal.PtrToStringUTF8(messagePtr) ?? "Native DataFusion call failed";
        df_string_free(messagePtr);
        NativeExceptionKind kind = (NativeExceptionKind)df_last_error_kind();
        throw kind switch
        {
            NativeExceptionKind.Plan => new DataFusionPlanException(message),
            NativeExceptionKind.Execution => new DataFusionExecutionException(message),
            NativeExceptionKind.ResourcesExhausted => new DataFusionResourcesExhaustedException(message),
            NativeExceptionKind.Io => new DataFusionIoException(message),
            NativeExceptionKind.NotImplemented => new DataFusionNotImplementedException(message),
            NativeExceptionKind.Configuration => new DataFusionConfigurationException(message),
            _ => new DataFusionException(message),
        };
    }

    internal static byte[] CopyAndFree(ByteBuffer buffer)
    {
        try
        {
            int length = checked((int)buffer.Len);
            byte[] managed = new byte[length];
            if (length > 0)
            {
                Marshal.Copy(buffer.Ptr, managed, 0, length);
            }

            return managed;
        }
        finally
        {
            df_byte_buffer_free(buffer);
        }
    }

    internal static ulong[] CopyUInt64ArrayAndFree(ByteBuffer buffer)
    {
        byte[] bytes = CopyAndFree(buffer);
        if (bytes.Length % sizeof(ulong) != 0)
        {
            throw new DataFusionException($"Native buffer length {bytes.Length} is not a UInt64 array.");
        }

        ulong[] values = new ulong[bytes.Length / sizeof(ulong)];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(i * sizeof(ulong), sizeof(ulong)));
        }

        return values;
    }

    internal static long[] CopyInt64ArrayAndFree(ByteBuffer buffer)
    {
        byte[] bytes = CopyAndFree(buffer);
        if (bytes.Length % sizeof(long) != 0)
        {
            throw new DataFusionException($"Native buffer length {bytes.Length} is not an Int64 array.");
        }

        long[] values = new long[bytes.Length / sizeof(long)];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(i * sizeof(long), sizeof(long)));
        }

        return values;
    }
}
