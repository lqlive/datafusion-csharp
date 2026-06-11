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

using System.Reflection;
using System.Runtime.InteropServices;

namespace Apache.DataFusion;

public static class NativeLibraryLoader
{
    private const string LibraryName = "datafusion_csharp_native";
    private static readonly object SyncRoot = new();
    private static bool registered;

    public static void Load()
    {
        if (registered)
        {
            return;
        }

        lock (SyncRoot)
        {
            if (registered)
            {
                return;
            }

            // A resolver (rather than a bare NativeLibrary.Load by path) is required so
            // that the imported library name maps to the file we found. On Linux/macOS a
            // path-based pre-load does not satisfy a later P/Invoke by name the way it does
            // on Windows.
            NativeLibrary.SetDllImportResolver(typeof(NativeLibraryLoader).Assembly, Resolve);
            registered = true;
        }
    }

    private static IntPtr Resolve(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibraryName)
        {
            return IntPtr.Zero;
        }

        foreach (string candidate in CandidatePaths())
        {
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out IntPtr handle))
            {
                return handle;
            }
        }

        // Fall back to the default search so the runtime can probe the assembly's native
        // asset locations (runtimes/{rid}/native via deps.json, then system paths).
        if (NativeLibrary.TryLoad(libraryName, assembly, searchPath ?? DllImportSearchPath.SafeDirectories, out IntPtr fallback))
        {
            return fallback;
        }

        return IntPtr.Zero;
    }

    private static IEnumerable<string> CandidatePaths()
    {
        string baseDirectory = AppContext.BaseDirectory;
        string fileName = NativeLibraryFileName();

        // The native binary is copied next to the managed assembly: by the build for local
        // development (see Apache.DataFusion.csproj) and by the matching
        // Apache.DataFusion.Native.<rid> package under runtimes/{rid}/native for consumers.
        yield return Path.Combine(baseDirectory, fileName);

        foreach (string rid in RuntimeIdentifiers())
        {
            yield return Path.Combine(baseDirectory, "runtimes", rid, "native", fileName);
        }
    }

    private static string NativeLibraryFileName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return $"{LibraryName}.dll";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return $"lib{LibraryName}.dylib";
        }

        return $"lib{LibraryName}.so";
    }

    private static IEnumerable<string> RuntimeIdentifiers()
    {
        // The runtime's own RID is the most precise (e.g. linux-musl-x64 on Alpine),
        // so probe it before the portable fallback that matches our package folders.
        string preciseRid = RuntimeInformation.RuntimeIdentifier;
        if (!string.IsNullOrEmpty(preciseRid))
        {
            yield return preciseRid;
        }

        string? portableRid = PortableRuntimeIdentifier();
        if (portableRid is not null && portableRid != preciseRid)
        {
            yield return portableRid;
        }
    }

    private static string? PortableRuntimeIdentifier()
    {
        string? architecture = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => null,
        };

        if (architecture is null)
        {
            return null;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return $"win-{architecture}";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return $"osx-{architecture}";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return $"linux-{architecture}";
        }

        return null;
    }
}
