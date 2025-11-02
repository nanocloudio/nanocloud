use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BundleFieldOwnership {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    entries: HashMap<String, String>,
}

impl BundleFieldOwnership {
    pub fn set_owner(&mut self, pointer: &str, manager: &str) {
        self.entries
            .insert(pointer.trim().to_string(), manager.trim().to_string());
    }

    pub fn manager_for(&self, pointer: &str) -> Option<&str> {
        self.entries.get(pointer.trim()).map(|value| value.as_str())
    }
}
