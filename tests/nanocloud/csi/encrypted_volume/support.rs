#![allow(dead_code)]

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;

use nanocloud::nanocloud::csi::StoredVolume;
use nanocloud::nanocloud::test_support::{keyspace_lock, test_output_dir};
use nanocloud::nanocloud::util::security::SecureAssets;
use nanocloud::nanocloud::util::Keyspace;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: &Path) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.as_ref() {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

struct GlobalPaths {
    base: PathBuf,
    csi_root: PathBuf,
    secure_dir: PathBuf,
    encrypted_root: PathBuf,
}

fn global_paths() -> &'static GlobalPaths {
    use std::sync::OnceLock;

    static PATHS: OnceLock<GlobalPaths> = OnceLock::new();
    PATHS.get_or_init(|| {
        let base = test_output_dir("encrypted-volume-suite");
        let csi_root = base.join("csi-root");
        let secure_dir = base.join("secure-assets");
        let encrypted_root = base.join("encrypted");

        fs::create_dir_all(&csi_root).expect("create csi root");
        fs::create_dir_all(&secure_dir).expect("create secure assets dir");
        fs::create_dir_all(&encrypted_root).expect("create encrypted root");

        SecureAssets::generate(&secure_dir, false).expect("generate secure assets");

        GlobalPaths {
            base,
            csi_root,
            secure_dir,
            encrypted_root,
        }
    })
}

pub struct EncryptedVolumeTestEnv {
    _lock: MutexGuard<'static, ()>,
    _keyspace_env: EnvGuard,
    _lock_env: EnvGuard,
    _secure_env: EnvGuard,
    _encrypted_env: EnvGuard,
    _csi_env: EnvGuard,
    _record_env: EnvGuard,
    record_path: PathBuf,
    secure_dir: PathBuf,
}

impl EncryptedVolumeTestEnv {
    pub fn new(test_name: &str) -> Self {
        let _lock = keyspace_lock().lock();
        let globals = global_paths();

        let keyspace_dir = globals.base.join("keyspace").join(test_name);
        fs::create_dir_all(&keyspace_dir).expect("create keyspace dir");

        let lock_path = globals.base.join("locks").join(format!("{test_name}.lock"));
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("create lock dir");
        }
        fs::File::create(&lock_path).expect("create lock file");

        let record_path = globals
            .base
            .join("records")
            .join(format!("{test_name}.log"));
        if let Some(parent) = record_path.parent() {
            fs::create_dir_all(parent).expect("create record dir");
        }

        let keyspace_env = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_env = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_path);
        let secure_env = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", &globals.secure_dir);
        let encrypted_env =
            EnvGuard::set_path("NANOCLOUD_ENCRYPTED_VOLUMES", &globals.encrypted_root);
        let csi_env = EnvGuard::set_path("NANOCLOUD_CSI_ROOT", &globals.csi_root);
        let record_env = EnvGuard::set_path("NANOCLOUD_ENCRYPTION_RECORD", &record_path);

        Self {
            _lock,
            _keyspace_env: keyspace_env,
            _lock_env: lock_env,
            _secure_env: secure_env,
            _encrypted_env: encrypted_env,
            _csi_env: csi_env,
            _record_env: record_env,
            record_path,
            secure_dir: globals.secure_dir.clone(),
        }
    }

    pub fn ensure_volume_key(&self, volume: &str) {
        SecureAssets::ensure_volume_keys(&self.secure_dir, &[volume.to_string()], false)
            .expect("generate volume key");
    }

    pub fn reset_log(&self) {
        fs::write(&self.record_path, "").expect("reset record log");
    }

    pub fn record_log(&self) -> String {
        fs::read_to_string(&self.record_path).unwrap_or_default()
    }

    pub fn stored_volume(&self, volume_id: &str) -> StoredVolume {
        let keyspace = Keyspace::new("csi");
        let key = format!("/volumes/{volume_id}");
        let raw = keyspace.get(&key).expect("load stored volume");
        serde_json::from_str(&raw).expect("deserialize stored volume")
    }

    pub fn stored_publications(&self, volume_id: &str) -> Vec<String> {
        self.stored_volume(volume_id).publications
    }

    pub fn write_volume(&self, volume_id: &str, stored: &StoredVolume) {
        let payload =
            serde_json::to_string(stored).expect("serialize updated stored volume record");
        let keyspace = Keyspace::new("csi");
        let key = format!("/volumes/{volume_id}");
        keyspace
            .put(&key, &payload)
            .expect("persist updated stored volume");
    }

    pub fn update_encryption_key(&self, volume_id: &str, key_name: &str) {
        let mut stored = self.stored_volume(volume_id);
        if let Some(encrypted) = stored.encrypted.as_mut() {
            encrypted.key_name = key_name.to_string();
        }
        stored
            .parameters
            .insert("encryption.key".to_string(), key_name.to_string());
        stored
            .volume
            .volume_context
            .insert("encrypted.keyName".to_string(), key_name.to_string());
        self.write_volume(volume_id, &stored);
    }

    pub fn set_volume_size(&self, volume_id: &str, new_size: u64) -> PathBuf {
        let mut stored = self.stored_volume(volume_id);
        let encrypted = stored
            .encrypted
            .as_mut()
            .expect("encrypted volume metadata must exist");
        encrypted.size_bytes = new_size;
        stored.volume.capacity_bytes = new_size;
        let backing_path = PathBuf::from(&encrypted.backing_path);
        self.write_volume(volume_id, &stored);

        let file = OpenOptions::new()
            .write(true)
            .open(&backing_path)
            .expect("open backing file for resize");
        file.set_len(new_size)
            .expect("resize encrypted backing file");
        backing_path
    }
}
