use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use nanocloud::nanocloud::util::security::volume::{
    cleanup_mount_dir, close_mapper, encrypted_host_mount, encrypted_mapper_name,
    ensure_encrypted_volume_root, ensure_luks_device, mount_mapper, open_mapper, read_volume_key,
    unmount_if_mounted,
};
use nanocloud::nanocloud::util::security::SecureAssets;
use nix::unistd::Uid;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: PathBuf) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, &value);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = &self.prev {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

struct LoopGuard {
    device: String,
}

impl Drop for LoopGuard {
    fn drop(&mut self) {
        let _ = Command::new("losetup").args(["-d", &self.device]).status();
    }
}

#[test]
fn encrypted_volume_unlock_and_lock_round_trip() {
    if !Uid::effective().is_root() {
        eprintln!("skipping encrypted volume test: requires root");
        return;
    }

    for tool in ["cryptsetup", "losetup", "mkfs"] {
        if Command::new(tool)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_err()
        {
            eprintln!("skipping encrypted volume test: missing {}", tool);
            return;
        }
    }

    let temp = TempDir::new().expect("tempdir");
    let secure_dir = temp.path().join("secure");
    let encrypted_dir = temp.path().join("encrypted");
    fs::create_dir_all(&secure_dir).expect("secure dir");
    fs::create_dir_all(&encrypted_dir).expect("encrypted dir");

    let _secure_env = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", secure_dir.clone());
    let _encrypted_env = EnvGuard::set("NANOCLOUD_ENCRYPTED_VOLUMES", encrypted_dir.clone());

    let volume_name = "ci-test";
    let _ = SecureAssets::ensure_volume_keys(&secure_dir, &[volume_name.to_string()], true)
        .expect("volume key");

    let backing = temp.path().join("backing.img");
    let backing_file = File::create(&backing).expect("backing file");
    backing_file
        .set_len(32 * 1024 * 1024)
        .expect("resize backing file");

    let losetup_output = Command::new("losetup")
        .arg("--find")
        .arg("--show")
        .arg(&backing)
        .output()
        .expect("losetup");
    assert!(losetup_output.status.success(), "losetup failed");
    let device = String::from_utf8_lossy(&losetup_output.stdout)
        .trim()
        .to_string();
    let _loop_guard = LoopGuard {
        device: device.clone(),
    };

    let container_id = "abc123";
    let mapper = encrypted_mapper_name(container_id, volume_name);
    let mount_path = encrypted_host_mount(container_id, volume_name);

    ensure_encrypted_volume_root().expect("encrypted root");
    let key = read_volume_key(volume_name).expect("read key");
    ensure_luks_device(&device, &key).expect("ensure luks");
    open_mapper(&device, &mapper, &key).expect("open mapper");
    fs::create_dir_all(&mount_path).expect("create mount dir");
    mount_mapper(&mapper, &mount_path, "ext4").expect("mount mapper");

    let mut data_file = File::create(mount_path.join("payload.txt")).expect("data file");
    data_file
        .write_all(b"encrypted payload")
        .expect("write payload");

    unmount_if_mounted(&mount_path).expect("unmount");
    cleanup_mount_dir(&mount_path);
    close_mapper(&mapper).expect("close mapper");

    assert!(
        !mount_path.exists(),
        "mount directory should be cleaned: {}",
        mount_path.display()
    );

    // Unlock again to verify the data is intact.
    open_mapper(&device, &mapper, &key).expect("reopen mapper");
    fs::create_dir_all(&mount_path).expect("recreate mount dir");
    mount_mapper(&mapper, &mount_path, "ext4").expect("remount mapper");

    let contents = fs::read_to_string(mount_path.join("payload.txt")).expect("read payload");
    assert_eq!(contents, "encrypted payload");

    // Final cleanup
    unmount_if_mounted(&mount_path).expect("final unmount");
    cleanup_mount_dir(&mount_path);
    close_mapper(&mapper).expect("final close");
}
