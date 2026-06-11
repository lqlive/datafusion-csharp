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

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::{
    DefaultFileStatisticsCache, DefaultFilesMetadataCache,
};
use datafusion::execution::cache::DefaultListFilesCache;

use crate::proto_gen::CacheManagerOptionsProto;
use crate::NativeResult;

pub(crate) fn build_config(
    opts: &CacheManagerOptionsProto,
) -> NativeResult<Option<CacheManagerConfig>> {
    if opts.file_metadata_cache_max_bytes.is_none()
        && opts.list_files_cache.is_none()
        && opts.file_statistics_cache_enabled.is_none()
    {
        return Ok(None);
    }

    let mut config = CacheManagerConfig::default();

    if let Some(max_bytes) = opts.file_metadata_cache_max_bytes {
        let max = max_bytes as usize;
        config.file_metadata_cache = Some(Arc::new(DefaultFilesMetadataCache::new(max)));
        config.metadata_cache_limit = max;
    }

    if let Some(lfc) = &opts.list_files_cache {
        let default_limit = CacheManagerConfig::default().list_files_cache_limit;
        let max = lfc
            .max_bytes
            .map(|value| value as usize)
            .unwrap_or(default_limit);
        let ttl = lfc.ttl_millis.map(Duration::from_millis);

        config.list_files_cache = Some(Arc::new(DefaultListFilesCache::new(max, ttl)));
        config.list_files_cache_limit = max;
        config.list_files_cache_ttl = ttl;
    }

    if opts.file_statistics_cache_enabled.unwrap_or(false) {
        config.table_files_statistics_cache = Some(Arc::new(DefaultFileStatisticsCache::default()));
    }

    Ok(Some(config))
}
