// Run with: cargo test runtime_exec_mounts_proc_when_missing -- --nocapture

use std::fs::File;
use std::io::{self, ErrorKind};

use nanocloud::nanocloud::oci::runtime::{
    ensure_proc_mounted_for_testing, Linux, Mount, OciConfig, Process, Root, User,
};
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::sched::{unshare, CloneFlags};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{fork, ForkResult, Uid};

#[test]
fn runtime_exec_mounts_proc_when_missing() {
    if !Uid::effective().is_root() {
        eprintln!("skipping runtime_exec_mounts_proc_when_missing: requires root");
        return;
    }

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            if let Err(err) = run_proc_guard_validation() {
                eprintln!("proc guard validation failed: {}", err);
                unsafe { libc::_exit(1) };
            }
            unsafe { libc::_exit(0) };
        }
        Ok(ForkResult::Parent { child }) => match waitpid(child, None) {
            Ok(WaitStatus::Exited(_, 0)) => {}
            Ok(WaitStatus::Exited(_, code)) => {
                panic!("proc guard child exited with status {}", code);
            }
            Ok(WaitStatus::Signaled(_, sig, _)) => {
                panic!("proc guard child received signal {}", sig);
            }
            Ok(other) => panic!("unexpected wait status: {:?}", other),
            Err(err) => panic!("waitpid failed: {}", err),
        },
        Err(err) => panic!("fork failed: {}", err),
    }
}

fn run_proc_guard_validation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    unshare(CloneFlags::CLONE_NEWNS)?;
    mount::<str, str, str, str>(None, "/", None, MsFlags::MS_REC | MsFlags::MS_PRIVATE, None)?;

    umount2("/proc", MntFlags::MNT_DETACH)?;

    match File::open("/proc/self/mountinfo") {
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(Box::new(err)),
        Ok(_) => {
            return Err(Box::new(io::Error::other(
                "mountinfo still accessible after unmounting /proc",
            )));
        }
    }

    let config = test_oci_config();
    let mounted = ensure_proc_mounted_for_testing(&config)?;
    if !mounted {
        return Err(Box::new(io::Error::other(
            "ensure_proc_mounted_for_testing reported /proc already present",
        )));
    }

    File::open("/proc/self/mountinfo").map_err(|err| {
        Box::new(io::Error::other(format!(
            "mountinfo unreadable after ensure_proc_mounted_for_testing: {}",
            err
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;

    let second = ensure_proc_mounted_for_testing(&config)?;
    if second {
        return Err(Box::new(io::Error::other(
            "ensure_proc_mounted_for_testing unexpectedly remounted /proc on second invocation",
        )));
    }

    Ok(())
}

fn test_oci_config() -> OciConfig {
    OciConfig {
        oci_version: "1.0.2".to_string(),
        process: Process {
            terminal: false,
            user: User { uid: 0, gid: 0 },
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
        },
        root: Root {
            path: "/".to_string(),
            readonly: false,
        },
        hostname: "proc-guard-test".to_string(),
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
            namespaces: Vec::new(),
            resources: None,
        },
        encrypted_volumes: Vec::new(),
    }
}
