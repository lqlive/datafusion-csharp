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

public abstract class ObjectStoreOptions
{
    private readonly string? url;

    private protected ObjectStoreOptions(string? url)
    {
        this.url = url;
    }

    public static S3Builder S3() => new();

    public static GcsBuilder Gcs() => new();

    public static HttpBuilder Http(string listingUrl)
    {
        ArgumentNullException.ThrowIfNull(listingUrl);
        return new HttpBuilder(listingUrl);
    }

    internal abstract Proto.ObjectStoreRegistration ToRegistration();

    protected void ApplyUrl(Proto.ObjectStoreRegistration registration)
    {
        if (url is not null)
        {
            registration.Url = url;
        }
    }

    public sealed class S3Options : ObjectStoreOptions
    {
        private readonly S3Builder builder;

        internal S3Options(S3Builder builder)
            : base(builder.Url)
        {
            this.builder = builder;
        }

        internal override Proto.ObjectStoreRegistration ToRegistration()
        {
            Proto.S3Options s3 = new() { Bucket = builder.Bucket! };
            if (builder.Region is not null) s3.Region = builder.Region;
            if (builder.Endpoint is not null) s3.Endpoint = builder.Endpoint;
            if (builder.AccessKeyId is not null) s3.AccessKeyId = builder.AccessKeyId;
            if (builder.SecretAccessKey is not null) s3.SecretAccessKey = builder.SecretAccessKey;
            if (builder.SessionToken is not null) s3.SessionToken = builder.SessionToken;
            if (builder.AllowHttp.HasValue) s3.AllowHttp = builder.AllowHttp.Value;
            if (builder.SkipSignature.HasValue) s3.SkipSignature = builder.SkipSignature.Value;
            if (builder.Imdsv1Fallback.HasValue) s3.Imdsv1Fallback = builder.Imdsv1Fallback.Value;
            Proto.ObjectStoreRegistration registration = new() { S3 = s3 };
            ApplyUrl(registration);
            return registration;
        }
    }

    public sealed class GcsOptions : ObjectStoreOptions
    {
        private readonly GcsBuilder builder;

        internal GcsOptions(GcsBuilder builder)
            : base(builder.Url)
        {
            this.builder = builder;
        }

        internal override Proto.ObjectStoreRegistration ToRegistration()
        {
            Proto.GcsOptions gcs = new() { Bucket = builder.Bucket! };
            if (builder.ServiceAccountKey is not null) gcs.ServiceAccountKey = builder.ServiceAccountKey;
            if (builder.ServiceAccountPath is not null) gcs.ServiceAccountPath = builder.ServiceAccountPath;
            if (builder.ApplicationCredentials is not null) gcs.ApplicationCredentials = builder.ApplicationCredentials;
            Proto.ObjectStoreRegistration registration = new() { Gcs = gcs };
            ApplyUrl(registration);
            return registration;
        }
    }

    public sealed class HttpOptions : ObjectStoreOptions
    {
        private readonly HttpBuilder builder;

        internal HttpOptions(HttpBuilder builder)
            : base(builder.Url)
        {
            this.builder = builder;
        }

        internal override Proto.ObjectStoreRegistration ToRegistration()
        {
            Proto.HttpOptions http = new();
            if (builder.AllowHttp.HasValue) http.AllowHttp = builder.AllowHttp.Value;
            Proto.ObjectStoreRegistration registration = new() { Http = http };
            ApplyUrl(registration);
            return registration;
        }
    }

    public sealed class S3Builder
    {
        internal string? Url { get; private set; }
        internal string? Bucket { get; private set; }
        internal string? Region { get; private set; }
        internal string? Endpoint { get; private set; }
        internal string? AccessKeyId { get; private set; }
        internal string? SecretAccessKey { get; private set; }
        internal string? SessionToken { get; private set; }
        internal bool? AllowHttp { get; private set; }
        internal bool? SkipSignature { get; private set; }
        internal bool? Imdsv1Fallback { get; private set; }

        internal S3Builder()
        {
        }

        public S3Builder WithUrl(string value) { Url = value; return this; }
        public S3Builder WithBucket(string value) { Bucket = value; return this; }
        public S3Builder WithRegion(string value) { Region = value; return this; }
        public S3Builder WithEndpoint(string value) { Endpoint = value; return this; }
        public S3Builder WithAccessKeyId(string value) { AccessKeyId = value; return this; }
        public S3Builder WithSecretAccessKey(string value) { SecretAccessKey = value; return this; }
        public S3Builder WithSessionToken(string value) { SessionToken = value; return this; }
        public S3Builder WithAllowHttp(bool value) { AllowHttp = value; return this; }
        public S3Builder WithSkipSignature(bool value) { SkipSignature = value; return this; }
        public S3Builder WithImdsv1Fallback(bool value) { Imdsv1Fallback = value; return this; }

        public S3Options Build()
        {
            if (string.IsNullOrEmpty(Bucket))
            {
                throw new InvalidOperationException("S3 ObjectStoreOptions requires a bucket.");
            }

            return new S3Options(this);
        }
    }

    public sealed class GcsBuilder
    {
        internal string? Url { get; private set; }
        internal string? Bucket { get; private set; }
        internal string? ServiceAccountKey { get; private set; }
        internal string? ServiceAccountPath { get; private set; }
        internal string? ApplicationCredentials { get; private set; }

        internal GcsBuilder()
        {
        }

        public GcsBuilder WithUrl(string value) { Url = value; return this; }
        public GcsBuilder WithBucket(string value) { Bucket = value; return this; }
        public GcsBuilder WithServiceAccountKey(string value) { ServiceAccountKey = value; return this; }
        public GcsBuilder WithServiceAccountPath(string value) { ServiceAccountPath = value; return this; }
        public GcsBuilder WithApplicationCredentials(string value) { ApplicationCredentials = value; return this; }

        public GcsOptions Build()
        {
            if (string.IsNullOrEmpty(Bucket))
            {
                throw new InvalidOperationException("GCS ObjectStoreOptions requires a bucket.");
            }
            if (ServiceAccountKey is not null && ServiceAccountPath is not null)
            {
                throw new InvalidOperationException("GCS service account key and path are mutually exclusive.");
            }

            return new GcsOptions(this);
        }
    }

    public sealed class HttpBuilder
    {
        internal string Url { get; }
        internal bool? AllowHttp { get; private set; }

        internal HttpBuilder(string url)
        {
            Url = url;
        }

        public HttpBuilder WithAllowHttp(bool value) { AllowHttp = value; return this; }

        public HttpOptions Build() => new(this);
    }
}
