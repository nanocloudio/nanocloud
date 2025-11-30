use std::sync::Arc;

pub trait CsiObserver: Send + Sync {
    fn on_operation_start(&self, _op: &str) {}
    fn on_operation_end(&self, _op: &str, _result: Result<(), String>) {}
    fn on_lock_wait(&self, _op: &str, _volume: &str, _wait_ms: u128) {}
    fn on_snapshot_complete(&self, _snapshot_id: &str, _bytes: u64, _duration_ms: u128) {}
}

#[derive(Clone, Default)]
pub struct NoopObserver;

impl CsiObserver for NoopObserver {}

impl NoopObserver {
    pub fn arc() -> Arc<dyn CsiObserver> {
        Arc::new(NoopObserver)
    }
}
