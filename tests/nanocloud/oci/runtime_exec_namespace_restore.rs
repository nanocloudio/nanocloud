use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;

use nanocloud::nanocloud::oci::runtime::{
    Linux, Mount, Namespace, OciConfig, Process, Root, Runtime, User,
};
use nix::sched::CloneFlags;
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{fork, pipe, read, write, ForkResult, Uid};
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: &PathBuf) -> Self {
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

#[test]
fn runtime_exec_enters_netns_via_pid_metadata_when_link_missing() {
    if !Uid::effective().is_root() {
        eprintln!("skipping runtime_exec_enters_netns...: requires root");
        return;
    }

    let container_root = TempDir::new().expect("container root tempdir");
    let netns_root = TempDir::new().expect("netns root tempdir");
    let container_root_path = container_root.path().to_path_buf();
    let netns_root_path = netns_root.path().to_path_buf();
    let _container_guard = EnvGuard::set_path("NANOCLOUD_CONTAINER_ROOT", &container_root_path);
    let _netns_guard = EnvGuard::set_path("NANOCLOUD_NETNS_DIR", &netns_root_path);

    let container_id = "execns-pid-fallback";
    let container_dir = container_root_path.join(container_id);
    fs::create_dir_all(&container_dir).expect("container dir");

    let (ready_r, ready_w) = pipe().expect("ready pipe");
    let (shutdown_r, shutdown_w) = pipe().expect("shutdown pipe");

    let child = match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            drop(ready_r);
            drop(shutdown_w);

            if let Err(err) = nix::sched::unshare(
                CloneFlags::CLONE_NEWNET
                    | CloneFlags::CLONE_NEWNS
                    | CloneFlags::CLONE_NEWUTS
                    | CloneFlags::CLONE_NEWIPC,
            ) {
                eprintln!("child unshare failed: {}", err);
                unsafe { libc::_exit(1) };
            }

            let _ = write(&ready_w, &[1]);
            drop(ready_w);

            let mut buf = [0u8; 1];
            let _ = read(&shutdown_r, &mut buf);
            drop(shutdown_r);
            unsafe { libc::_exit(0) };
        }
        Ok(ForkResult::Parent { child }) => child,
        Err(err) => panic!("fork failed: {}", err),
    };

    drop(ready_w);
    drop(shutdown_r);

    let mut ready_buf = [0u8; 1];
    read(&ready_r, &mut ready_buf).expect("read ready");
    drop(ready_r);

    let child_pid = child.as_raw() as i32;
    fs::write(container_dir.join("pid"), format!("{child_pid}\n")).expect("pid file");

    let config = OciConfig {
        oci_version: "1.0.2".to_string(),
        process: Process {
            terminal: false,
            user: User { uid: 0, gid: 0 },
            args: vec!["/bin/sh".to_string()],
            env: Vec::new(),
            cwd: "/".to_string(),
        },
        root: Root {
            path: "/".to_string(),
            readonly: false,
        },
        hostname: "exec-test".to_string(),
        mounts: vec![Mount {
            destination: "/proc".to_string(),
            mount_type: "proc".to_string(),
            source: "proc".to_string(),
            options: Some(vec![
                "nosuid".to_string(),
                "nodev".to_string(),
                "noexec".to_string(),
            ]),
        }],
        linux: Linux {
            namespaces: vec![
                Namespace {
                    ns_type: "pid".to_string(),
                    path: None,
                },
                Namespace {
                    ns_type: "uts".to_string(),
                    path: None,
                },
                Namespace {
                    ns_type: "ipc".to_string(),
                    path: None,
                },
                Namespace {
                    ns_type: "mount".to_string(),
                    path: None,
                },
                Namespace {
                    ns_type: "network".to_string(),
                    path: None,
                },
            ],
            resources: None,
        },
        encrypted_volumes: Vec::new(),
    };

    let mut config_file = File::create(container_dir.join("config.json")).expect("config file");
    serde_json::to_writer(&mut config_file, &config).expect("write config");
    config_file.flush().expect("flush config");

    let expected_link =
        std::fs::read_link(format!("/proc/{child_pid}/ns/net")).expect("expected netns link");

    let expected_first = expected_link.clone();
    Runtime::with_namespace(container_id, move || {
        let actual = std::fs::read_link("/proc/self/ns/net")
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
        if actual != expected_first {
            return Err(Box::new(io::Error::other(format!(
                "net namespace mismatch: expected {:?}, got {:?}",
                expected_first, actual
            ))));
        }
        Ok(())
    })
    .unwrap_or_else(|err| panic!("Runtime::exec failed: {}", err));

    assert_eq!(
        Runtime::take_exec_proc_mount_status(),
        Some(false),
        "expected proc guard to report existing /proc mount after first exec"
    );

    let expected_second = expected_link.clone();
    Runtime::with_namespace(container_id, move || {
        let actual = std::fs::read_link("/proc/self/ns/net")
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
        if actual != expected_second {
            return Err(Box::new(io::Error::other(format!(
                "net namespace mismatch on retry: expected {:?}, got {:?}",
                expected_second, actual
            ))));
        }
        Ok(())
    })
    .unwrap_or_else(|err| panic!("Runtime::exec retry failed: {}", err));

    assert_eq!(
        Runtime::take_exec_proc_mount_status(),
        Some(false),
        "expected proc guard to report existing /proc mount after second exec"
    );

    let _ = write(&shutdown_w, &[0]);
    drop(shutdown_w);

    match waitpid(child, None).expect("waitpid") {
        WaitStatus::Exited(_, 0) => {}
        WaitStatus::Exited(_, code) => panic!("child exited with {}", code),
        WaitStatus::Signaled(_, sig, _) => panic!("child signaled with {}", sig),
        other => panic!("unexpected child status: {:?}", other),
    }
}
