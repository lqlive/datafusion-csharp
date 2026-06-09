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

namespace Apache.DataFusion.Tests;

public sealed class ObjectStoreOptionsTests
{
    [Fact]
    public void S3RequiresBucket()
    {
        Assert.Throws<InvalidOperationException>(() => ObjectStoreOptions.S3().Build());
    }

    [Fact]
    public void GcsRejectsMutuallyExclusiveCredentials()
    {
        Assert.Throws<InvalidOperationException>(() => ObjectStoreOptions
            .Gcs()
            .WithBucket("bucket")
            .WithServiceAccountKey("{}")
            .WithServiceAccountPath("service-account.json")
            .Build());
    }

    [Fact]
    public void RegisterObjectStoreSurfacesFeatureGateWhenBackendIsDisabled()
    {
        ObjectStoreOptions.S3Options options = ObjectStoreOptions
            .S3()
            .WithBucket("bucket")
            .Build();

        DataFusionException exception = Assert.Throws<DataFusionException>(() => SessionContext
            .CreateBuilder()
            .RegisterObjectStore(options)
            .Build());

        Assert.Contains("object-store-aws", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
