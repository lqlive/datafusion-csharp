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

namespace Apache.DataFusion;

public sealed class CacheManagerOptions
{
    private readonly ulong? fileMetadataCacheMaxBytes;
    private readonly ListFilesCacheConfig? listFilesCache;
    private readonly bool? fileStatisticsCacheEnabled;

    private CacheManagerOptions(Builder builder)
    {
        fileMetadataCacheMaxBytes = builder.FileMetadataCacheMaxBytes;
        listFilesCache = builder.ListFilesCacheConfig;
        fileStatisticsCacheEnabled = builder.FileStatisticsCacheEnabled;
    }

    public static Builder CreateBuilder() => new();

    internal Proto.CacheManagerOptionsProto ToProto()
    {
        Proto.CacheManagerOptionsProto proto = new();
        if (fileMetadataCacheMaxBytes.HasValue)
        {
            proto.FileMetadataCacheMaxBytes = fileMetadataCacheMaxBytes.Value;
        }

        if (listFilesCache is not null)
        {
            proto.ListFilesCache = new Proto.ListFilesCacheOptionsProto();
            if (listFilesCache.MaxBytes.HasValue)
            {
                proto.ListFilesCache.MaxBytes = listFilesCache.MaxBytes.Value;
            }

            if (listFilesCache.Ttl.HasValue)
            {
                proto.ListFilesCache.TtlMillis = checked((ulong)listFilesCache.Ttl.Value.TotalMilliseconds);
            }
        }

        if (fileStatisticsCacheEnabled.HasValue)
        {
            proto.FileStatisticsCacheEnabled = fileStatisticsCacheEnabled.Value;
        }

        return proto;
    }

    internal sealed record ListFilesCacheConfig(ulong? MaxBytes, TimeSpan? Ttl);

    public sealed class Builder
    {
        internal ulong? FileMetadataCacheMaxBytes { get; private set; }

        internal ListFilesCacheConfig? ListFilesCacheConfig { get; private set; }

        internal bool? FileStatisticsCacheEnabled { get; private set; }

        internal Builder()
        {
        }

        public Builder FileMetadataCache(long maxBytes)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(maxBytes);
            FileMetadataCacheMaxBytes = (ulong)maxBytes;
            return this;
        }

        public Builder ListFilesCache(long maxBytes, TimeSpan? ttl = null)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(maxBytes);
            if (ttl.HasValue && ttl.Value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(ttl), ttl, "TTL must be non-negative.");
            }

            ListFilesCacheConfig = new ListFilesCacheConfig((ulong)maxBytes, ttl);
            return this;
        }

        public Builder FileStatisticsCache(bool enabled)
        {
            FileStatisticsCacheEnabled = enabled;
            return this;
        }

        public CacheManagerOptions Build() => new(this);
    }
}
