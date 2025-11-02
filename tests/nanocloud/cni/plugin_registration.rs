use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use nanocloud::nanocloud::cni::network::{CniReconciliationReport, CniResult};
use nanocloud::nanocloud::cni::provider::{cni_plugin, register_cni_plugin, CniPlugin};

static BRIDGE_CALLS: AtomicUsize = AtomicUsize::new(0);

struct TestCniPlugin;

impl CniPlugin for TestCniPlugin {
    fn reconcile_cni_artifacts(&self) -> Result<CniReconciliationReport, Box<dyn std::error::Error + Send + Sync>> {
        Ok(CniReconciliationReport {
            stale_containers: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn bridge(&self, _name: &str, _cidr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        BRIDGE_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn add(
        &self,
        _env: &HashMap<String, String>,
        _config: Vec<u8>,
    ) -> Result<CniResult, Box<dyn std::error::Error + Send + Sync>> {
        Ok(CniResult {
            cni_version: "1.0.0".into(),
            interfaces: Vec::new(),
            ips: Vec::new(),
            routes: Vec::new(),
        })
    }

    fn delete(&self, _env: &HashMap<String, String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[test]
fn register_cni_plugin_overrides_default_provider() {
    let plugin = Arc::new(TestCniPlugin);
    // Ignore failures if another test already registered a provider.
    let _ = register_cni_plugin(plugin);

    let plugin = cni_plugin();
    plugin
        .bridge("nanocloud-test", "10.1.0.1/24")
        .expect("bridge call succeeds");
    assert_eq!(BRIDGE_CALLS.load(Ordering::SeqCst), 1);
}
