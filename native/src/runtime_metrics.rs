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

pub const STATS_FIELD_COUNT: usize = 11;

#[cfg(feature = "runtime-metrics")]
mod imp {
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};

    use super::STATS_FIELD_COUNT;
    use crate::NativeResult;

    struct RuntimeAccumulator {
        intervals: RuntimeIntervals,
        elapsed: Duration,
        total_busy: Duration,
        total_park_count: u64,
        total_polls_count: u64,
        total_noop_count: u64,
        total_steal_count: u64,
        total_local_schedule_count: u64,
        total_overflow_count: u64,
    }

    static ACC: OnceLock<Mutex<RuntimeAccumulator>> = OnceLock::new();

    pub fn init(handle: &tokio::runtime::Handle) {
        ACC.get_or_init(|| {
            let monitor = RuntimeMonitor::new(handle);
            Mutex::new(RuntimeAccumulator {
                intervals: monitor.intervals(),
                elapsed: Duration::ZERO,
                total_busy: Duration::ZERO,
                total_park_count: 0,
                total_polls_count: 0,
                total_noop_count: 0,
                total_steal_count: 0,
                total_local_schedule_count: 0,
                total_overflow_count: 0,
            })
        });
    }

    pub fn runtime_stats() -> NativeResult<[i64; STATS_FIELD_COUNT]> {
        let mut acc = ACC
            .get_or_init(|| {
                let monitor = RuntimeMonitor::new(crate::runtime().handle());
                Mutex::new(RuntimeAccumulator {
                    intervals: monitor.intervals(),
                    elapsed: Duration::ZERO,
                    total_busy: Duration::ZERO,
                    total_park_count: 0,
                    total_polls_count: 0,
                    total_noop_count: 0,
                    total_steal_count: 0,
                    total_local_schedule_count: 0,
                    total_overflow_count: 0,
                })
            })
            .lock()
            .map_err(|_| "runtime-metrics accumulator lock poisoned")?;
        let delta = acc
            .intervals
            .next()
            .ok_or("tokio-metrics RuntimeMonitor returned no interval")?;

        acc.elapsed = acc.elapsed.saturating_add(delta.elapsed);
        acc.total_busy = acc.total_busy.saturating_add(delta.total_busy_duration);
        acc.total_park_count = acc.total_park_count.saturating_add(delta.total_park_count);
        acc.total_polls_count = acc
            .total_polls_count
            .saturating_add(delta.total_polls_count);
        acc.total_noop_count = acc.total_noop_count.saturating_add(delta.total_noop_count);
        acc.total_steal_count = acc
            .total_steal_count
            .saturating_add(delta.total_steal_count);
        acc.total_local_schedule_count = acc
            .total_local_schedule_count
            .saturating_add(delta.total_local_schedule_count);
        acc.total_overflow_count = acc
            .total_overflow_count
            .saturating_add(delta.total_overflow_count);

        Ok([
            delta.workers_count as i64,
            delta.live_tasks_count as i64,
            delta.global_queue_depth as i64,
            i128_to_i64_saturating(acc.elapsed.as_nanos() as i128),
            i128_to_i64_saturating(acc.total_busy.as_nanos() as i128),
            acc.total_park_count as i64,
            acc.total_polls_count as i64,
            acc.total_noop_count as i64,
            acc.total_steal_count as i64,
            acc.total_local_schedule_count as i64,
            acc.total_overflow_count as i64,
        ])
    }

    fn i128_to_i64_saturating(value: i128) -> i64 {
        value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }
}

#[cfg(feature = "runtime-metrics")]
pub use imp::{init, runtime_stats};

#[cfg(not(feature = "runtime-metrics"))]
pub fn init(_handle: &tokio::runtime::Handle) {}

#[cfg(not(feature = "runtime-metrics"))]
pub fn runtime_stats() -> crate::NativeResult<[i64; STATS_FIELD_COUNT]> {
    Err(
        "datafusion_csharp_native was built without the `runtime-metrics` Cargo feature; \
         rebuild with `RUSTFLAGS=\"--cfg tokio_unstable\" cargo build --features runtime-metrics` \
         to enable SessionContext.RuntimeStats"
            .into(),
    )
}
