use nanocloud::nanocloud::test_support::keyspace_lock;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use nanocloud::nanocloud::cni::Network;
use nanocloud::nanocloud::kubelet::runtime::setup_container_files;
use nanocloud::nanocloud::oci::runtime::container_root_path;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set<K: Into<String>>(key: &'static str, value: K) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, value.into());
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.as_ref() {
            env::set_var(self.key, prev);
        } else {
            env::remove_var(self.key);
        }
    }
}

fn write_script(path: &Path, contents: &str) {
    fs::write(path, contents).expect("failed to write script");
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("set permissions");
}

#[test]
fn cni_add_delete_is_idempotent_with_netns_retained() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let bin_dir = base.join("bin");
    let keyspace_dir = base.join("keyspace");
    let lock_file = base.join("lockfile");
    let netns_dir = base.join("netns");
    let container_root = base.join("containers");
    let log_dir = base.join("logs");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir_all(&log_dir).unwrap();

    let ip_script = r#"#!/bin/sh
set -eu
LOG_DIR=${NANOCLOUD_TEST_LOG_DIR:-/tmp}
mkdir -p "$LOG_DIR"
printf '%s\n' "$@" >> "$LOG_DIR/ip.log"
NETNS_DIR=${NANOCLOUD_NETNS_DIR:-/var/run/netns}
if [ "$#" -ge 2 ] && [ "$1" = "link" ] && [ "$2" = "add" ]; then
    exit 0
fi
if [ "$#" -ge 2 ] && [ "$1" = "link" ] && [ "$2" = "set" ]; then
    exit 0
fi
if [ "$#" -ge 2 ] && [ "$1" = "link" ] && [ "$2" = "delete" ]; then
    exit 0
fi
if [ "$#" -ge 3 ] && [ "$1" = "netns" ] && [ "$2" = "add" ]; then
    mkdir -p "$NETNS_DIR"
    : > "$NETNS_DIR/$3"
    exit 0
fi
if [ "$#" -ge 4 ] && [ "$1" = "netns" ] && [ "$2" = "exec" ] && [ "$4" = "cat" ]; then
    echo "02:42:ac:11:00:02"
    exit 0
fi
if [ "$#" -ge 2 ] && [ "$1" = "netns" ] && [ "$2" = "exec" ]; then
    exit 0
fi
if [ "$#" -ge 3 ] && [ "$1" = "netns" ] && [ "$2" = "delete" ]; then
    rm -f "$NETNS_DIR/$3"
    exit 0
fi
if [ "$1" = "addr" ] || [ "$1" = "route" ]; then
    exit 0
fi
exit 0
"#;

    let nft_script = r#"#!/bin/sh
set -eu
LOG_DIR=${NANOCLOUD_TEST_LOG_DIR:-/tmp}
mkdir -p "$LOG_DIR"
printf '%s\n' "$@" >> "$LOG_DIR/nft.log"
if [ "$#" -ge 3 ] && [ "$1" = "-j" ] && [ "$2" = "list" ] && [ "$3" = "chain" ]; then
    echo '{"nftables":[]}'
    exit 0
fi
if [ "$#" -ge 2 ] && [ "$1" = "list" ] && [ "$2" = "table" ]; then
    echo ""
    exit 0
fi
exit 0
"#;

    write_script(&bin_dir.join("ip"), ip_script);
    write_script(&bin_dir.join("nft"), nft_script);

    let current_path = env::var("PATH").unwrap_or_default();
    let path_guard = EnvGuard::set("PATH", format!("{}:{}", bin_dir.display(), current_path));
    let _keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", keyspace_dir.to_string_lossy());
    let _lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", lock_file.to_string_lossy());
    let _netns_guard = EnvGuard::set("NANOCLOUD_NETNS_DIR", netns_dir.to_string_lossy());
    let _container_guard =
        EnvGuard::set("NANOCLOUD_CONTAINER_ROOT", container_root.to_string_lossy());
    let _log_guard = EnvGuard::set("NANOCLOUD_TEST_LOG_DIR", log_dir.to_string_lossy());
    let _unpriv_guard = EnvGuard::set("NANOCLOUD_CNI_ALLOW_UNPRIVILEGED", "1");

    let container_id = "test-container".to_string();
    let netns_path = PathBuf::from(env::var("NANOCLOUD_NETNS_DIR").unwrap()).join(&container_id);

    let mut env_map: HashMap<String, String> = HashMap::new();
    env_map.insert("CNI_COMMAND".to_string(), "ADD".to_string());
    env_map.insert("CNI_CONTAINERID".to_string(), container_id.clone());
    env_map.insert(
        "CNI_NETNS".to_string(),
        netns_path.to_string_lossy().to_string(),
    );
    env_map.insert("CNI_IFNAME".to_string(), "eth0".to_string());
    env_map.insert(
        "CNI_PATH".to_string(),
        bin_dir.to_string_lossy().to_string(),
    );

    let config = serde_json::json!({
        "cniVersion": "1.0.0",
        "name": "nanocloud-test",
        "type": "bridge",
        "bridge": "nanocloud0",
        "ipam": {
            "subnet": "10.10.0.0/24"
        }
    });
    let config_str = serde_json::to_string(&config).unwrap();

    let first = Network::add(&env_map, std::io::Cursor::new(config_str.clone())).unwrap();
    assert!(!first.ips.is_empty());
    let first_ip = first.ips[0].address.clone();
    let primary_ip = first_ip.split('/').next().expect("ip format").to_string();

    setup_container_files(&container_id, &primary_ip).expect("setup container files");
    let ip_path = container_root_path(&container_id)
        .join("network")
        .join("ip_address");
    let recorded_ip = fs::read_to_string(&ip_path).expect("read ip file");
    assert_eq!(recorded_ip.trim(), primary_ip);

    let mut del_env: HashMap<String, String> = HashMap::new();
    del_env.insert("CNI_COMMAND".to_string(), "DEL".to_string());
    del_env.insert("CNI_CONTAINERID".to_string(), container_id.clone());
    Network::delete(&del_env).expect("first delete succeeds");
    Network::delete(&del_env).expect("second delete is idempotent");

    let second = Network::add(&env_map, std::io::Cursor::new(config_str)).unwrap();
    assert_eq!(second.ips[0].address, first_ip);

    setup_container_files(&container_id, &primary_ip).expect("setup container files after restart");
    let reread_ip = fs::read_to_string(&ip_path).expect("read ip file after restart");
    assert_eq!(reread_ip.trim(), primary_ip);

    drop(path_guard);
}
