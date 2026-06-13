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

//! Cancellation tokens for in-flight queries.
//!
//! Managed handles are opaque `u64` IDs, not raw pointers. A process-global
//! registry maps each live ID to its `Arc<CancellationToken>`. FFI handlers
//! look up by ID and clone the `Arc` out of the registry (under a lock) so a
//! concurrent free can never invalidate a borrow already in flight: the worst
//! a race produces is a token that no longer cancels anything, never a
//! use-after-free of a freed `Box`.
//!
//! The C# layer owns one token per cancellable `collect`/`execute_stream`
//! call and fires it from the thread that owns the managed `CancellationToken`
//! while the calling thread is parked inside `runtime().block_on(...)`.

use std::collections::HashMap;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::error::DataFusionError;
use tokio_util::sync::CancellationToken;

use crate::take_result;

/// Sentinel message used to round-trip a query-cancellation signal across the
/// FFI boundary. A cancelled `collect`/`execute_stream` returns a
/// `DataFusionError::Execution` carrying this message; the C# side maps it to
/// an `OperationCanceledException` whenever the managed token also reports a
/// cancellation request.
pub(crate) const CANCELLED_MESSAGE: &str = "datafusion-csharp: query cancelled";

fn registry() -> &'static Mutex<HashMap<u64, Arc<CancellationToken>>> {
    static REG: OnceLock<Mutex<HashMap<u64, Arc<CancellationToken>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_id() -> u64 {
    // Start at 1 so 0 stays reserved as the "no token" sentinel on the managed
    // side. Monotonic; 2^64 IDs is enough that reuse is never observed.
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Resolve a token handle into an `Arc<CancellationToken>`, or `None` when the
/// handle is the zero "no token" sentinel or refers to an already-freed entry.
/// The cloned `Arc` keeps the inner token alive for the borrow's lifetime, so a
/// concurrent `df_cancellation_token_free` removing the registry entry is safe.
pub(crate) fn token_arc(handle: u64) -> Option<Arc<CancellationToken>> {
    if handle == 0 {
        return None;
    }
    let guard = registry().lock().expect("cancellation registry poisoned");
    guard.get(&handle).cloned()
}

#[no_mangle]
pub extern "C" fn df_cancellation_token_new(out: *mut u64) -> c_int {
    take_result(|| {
        if out.is_null() {
            return Err("output token handle pointer is null".into());
        }
        let token: Arc<CancellationToken> = Arc::new(CancellationToken::new());
        let id = next_id();
        registry()
            .lock()
            .expect("cancellation registry poisoned")
            .insert(id, token);
        unsafe {
            *out = id;
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_cancellation_token_cancel(handle: u64) -> c_int {
    take_result(|| {
        // A missing entry (already freed or never registered) is a no-op: the
        // C# wrapper disposes its registration before freeing, so a real cancel
        // never loses a race, and a defensive no-op avoids spurious failures.
        if let Some(token) = token_arc(handle) {
            token.cancel();
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn df_cancellation_token_free(handle: u64) -> c_int {
    take_result(|| {
        if handle != 0 {
            // Remove the registry entry; the underlying `Arc` may still have
            // clones held by an in-flight collect/execute_stream future, which
            // keep the inner token alive until those futures finish.
            registry()
                .lock()
                .expect("cancellation registry poisoned")
                .remove(&handle);
        }
        Ok(())
    })
}

fn cancelled_error() -> DataFusionError {
    DataFusionError::Execution(CANCELLED_MESSAGE.to_string())
}

/// Drive `fut` to completion unless `token` fires first. A fired token yields a
/// `DataFusionError::Execution(CANCELLED_MESSAGE)` so the managed side can
/// surface a cancellation. `biased` polls the cancellation branch first so a
/// token that is already cancelled aborts before any query work runs.
pub(crate) async fn run_cancellable<F, T>(
    token: &Option<Arc<CancellationToken>>,
    fut: F,
) -> Result<T, DataFusionError>
where
    F: std::future::Future<Output = Result<T, DataFusionError>>,
{
    let Some(token) = token else {
        return fut.await;
    };
    tokio::select! {
        biased;
        _ = token.cancelled() => Err(cancelled_error()),
        result = fut => result,
    }
}

pub(crate) fn resolve_token(handle: u64) -> Option<Arc<CancellationToken>> {
    token_arc(handle)
}
