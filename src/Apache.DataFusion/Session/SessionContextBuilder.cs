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

using Google.Protobuf;

namespace Apache.DataFusion;

public sealed class SessionContextBuilder
{
    private ulong? batchSize;
    private ulong? targetPartitions;
    private bool? collectStatistics;
    private bool? informationSchema;
    private ulong? memoryLimitBytes;
    private double? memoryLimitFraction;
    private string? tempDirectory;
    private bool spillDisabled;
    private ulong? maxTempDirectorySize;
    private CacheManagerOptions? cacheManager;
    private readonly List<KeyValuePair<string, string>> options = [];
    private readonly List<ObjectStoreOptions> objectStores = [];

    internal SessionContextBuilder()
    {
    }

    public SessionContextBuilder BatchSize(int value)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
        batchSize = (ulong)value;
        return this;
    }

    public SessionContextBuilder TargetPartitions(int value)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
        targetPartitions = (ulong)value;
        return this;
    }

    public SessionContextBuilder CollectStatistics(bool value)
    {
        collectStatistics = value;
        return this;
    }

    public SessionContextBuilder InformationSchema(bool value)
    {
        informationSchema = value;
        return this;
    }

    public SessionContextBuilder MemoryLimit(long maxMemoryBytes, double fraction)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMemoryBytes);
        if (fraction <= 0.0 || fraction > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(fraction), fraction, "fraction must be in (0, 1].");
        }

        memoryLimitBytes = (ulong)maxMemoryBytes;
        memoryLimitFraction = fraction;
        return this;
    }

    public SessionContextBuilder TempDirectory(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        tempDirectory = path;
        return this;
    }

    public SessionContextBuilder DisableSpill()
    {
        spillDisabled = true;
        return this;
    }

    public SessionContextBuilder MaxTempDirectorySize(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        maxTempDirectorySize = (ulong)bytes;
        return this;
    }

    public SessionContextBuilder SetOption(string key, string value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        options.RemoveAll(item => item.Key == key);
        options.Add(KeyValuePair.Create(key, value));
        return this;
    }

    public SessionContextBuilder CacheManager(CacheManagerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        cacheManager = options;
        return this;
    }

    public SessionContextBuilder RegisterObjectStore(ObjectStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        objectStores.Add(options);
        return this;
    }

    public SessionContext Build() => new(ToBytes());

    internal byte[] ToBytes()
    {
        if (spillDisabled && tempDirectory is not null)
        {
            throw new InvalidOperationException("DisableSpill and TempDirectory are mutually exclusive.");
        }

        Proto.SessionOptions proto = new();
        if (batchSize.HasValue)
        {
            proto.BatchSize = batchSize.Value;
        }

        if (targetPartitions.HasValue)
        {
            proto.TargetPartitions = targetPartitions.Value;
        }

        if (collectStatistics.HasValue)
        {
            proto.CollectStatistics = collectStatistics.Value;
        }

        if (informationSchema.HasValue)
        {
            proto.InformationSchema = informationSchema.Value;
        }

        if (memoryLimitBytes.HasValue)
        {
            proto.MemoryLimit = new Proto.MemoryLimit
            {
                MaxMemoryBytes = memoryLimitBytes.Value,
                MemoryFraction = memoryLimitFraction ?? 1.0,
            };
        }

        if (tempDirectory is not null)
        {
            proto.TempDirectory = tempDirectory;
        }

        if (spillDisabled || maxTempDirectorySize.HasValue)
        {
            proto.DiskManager = new Proto.DiskManagerOptions();
            if (spillDisabled)
            {
                proto.DiskManager.Disabled = true;
            }

            if (maxTempDirectorySize.HasValue)
            {
                proto.DiskManager.MaxTempDirectorySize = maxTempDirectorySize.Value;
            }
        }

        if (cacheManager is not null)
        {
            proto.CacheManager = cacheManager.ToProto();
        }

        proto.ObjectStores.AddRange(objectStores.Select(store => store.ToRegistration()));

        proto.Options.AddRange(options.Select(option => new Proto.ConfigOption
        {
            Key = option.Key,
            Value = option.Value,
        }));

        return proto.ToByteArray();
    }
}
