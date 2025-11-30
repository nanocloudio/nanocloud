//! Per-volume async locks used to serialize mutating CSI operations.
//! Locks are keyed by volume ID so unrelated volumes can proceed concurrently.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Default)]
pub struct VolumeLockRegistry {
    inner: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

pub struct VolumeLockGuard {
    _guard: OwnedMutexGuard<()>,
}

impl VolumeLockRegistry {
    pub fn global() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<VolumeLockRegistry>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| Arc::new(VolumeLockRegistry::default()))
            .clone()
    }

    pub async fn lock(&self, volume_id: &str) -> VolumeLockGuard {
        let mutex = {
            let mut map = self.inner.lock().await;
            map.entry(volume_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = mutex.lock_owned().await;
        VolumeLockGuard { _guard: guard }
    }
}
