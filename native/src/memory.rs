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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::common::Result;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};

#[derive(Debug)]
pub struct TrackingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
}

impl TrackingMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> (u64, u64) {
        let peak_observed = self.peak_bytes.load(Ordering::Relaxed);
        let current = self.current_bytes.load(Ordering::Relaxed);
        let clamped_peak = peak_observed.max(current);
        let stored_peak = self.peak_bytes.fetch_max(clamped_peak, Ordering::Relaxed);
        (current, stored_peak.max(clamped_peak))
    }

    fn record_grow(&self, additional: usize) {
        let now = self
            .current_bytes
            .fetch_add(additional as u64, Ordering::Relaxed)
            .saturating_add(additional as u64);
        self.peak_bytes.fetch_max(now, Ordering::Relaxed);
    }

    fn record_shrink(&self, shrink: usize) {
        let prev = self
            .current_bytes
            .fetch_sub(shrink as u64, Ordering::Relaxed);
        if prev < shrink as u64 {
            self.current_bytes.store(0, Ordering::Relaxed);
        }
    }
}

impl MemoryPool for TrackingMemoryPool {
    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_grow(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.record_shrink(shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn registry() -> &'static Mutex<HashMap<usize, Arc<TrackingMemoryPool>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<usize, Arc<TrackingMemoryPool>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(handle: usize, pool: Arc<TrackingMemoryPool>) {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .insert(handle, pool);
}

pub fn lookup(handle: usize) -> Option<Arc<TrackingMemoryPool>> {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .get(&handle)
        .cloned()
}

pub fn unregister(handle: usize) {
    registry()
        .lock()
        .expect("memory registry lock poisoned")
        .remove(&handle);
}
