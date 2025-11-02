use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use nanocloud::nanocloud::oci::runtime::{
    Linux, Mount, Namespace, OciConfig, Process, Root, Runtime, User,
};
use nix::unistd::Uid;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &Path) -> Self {
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

struct ExecFixture {
    container_id: String,
    container_dir: PathBuf,
    _root_dir: TempDir,
    _container_guard: EnvGuard,
}

impl ExecFixture {
    fn new(container_id: &str, terminal: bool, env_entries: &[&str]) -> io::Result<Self> {
        let root = TempDir::new()?;
        let guard = EnvGuard::set("NANOCLOUD_CONTAINER_ROOT", root.path());

        let container_dir = root.path().join(container_id);
        fs::create_dir_all(&container_dir)?;

        let pid_path = container_dir.join("pid");
        let pid = std::process::id();
        fs::write(&pid_path, format!("{pid}\n"))?;

        let mut config = base_config(terminal, env_entries);
        config.linux.namespaces = vec![
            Namespace {
                ns_type: "pid".into(),
                path: None,
            },
            Namespace {
                ns_type: "uts".into(),
                path: None,
            },
            Namespace {
                ns_type: "ipc".into(),
                path: None,
            },
            Namespace {
                ns_type: "mount".into(),
                path: None,
            },
        ];

        let mut config_file = fs::File::create(container_dir.join("config.json"))?;
        serde_json::to_writer(&mut config_file, &config)?;
        config_file.flush()?;

        Ok(Self {
            container_id: container_id.to_owned(),
            container_dir,
            _root_dir: root,
            _container_guard: guard,
        })
    }

    fn id(&self) -> &str {
        &self.container_id
    }

    fn artifact(&self, name: &str) -> PathBuf {
        self.container_dir.join(name)
    }
}

fn base_config(terminal: bool, env_entries: &[&str]) -> OciConfig {
    OciConfig {
        oci_version: "1.0.2".to_string(),
        process: Process {
            terminal,
            user: User { uid: 0, gid: 0 },
            args: vec!["/bin/sh".into()],
            env: env_entries
                .iter()
                .map(|entry| (*entry).to_string())
                .collect(),
            cwd: "/".into(),
        },
        root: Root {
            path: "/".into(),
            readonly: false,
        },
        hostname: "runtime-exec-test".into(),
        mounts: vec![Mount {
            destination: "/proc".into(),
            mount_type: "proc".into(),
            source: "proc".into(),
            options: Some(vec!["nosuid".into(), "nodev".into(), "noexec".into()]),
        }],
        linux: Linux {
            namespaces: Vec::new(),
            resources: None,
        },
        encrypted_volumes: Vec::new(),
    }
}

#[test]
fn runtime_exec_runs_non_interactive_closure() {
    if !Uid::effective().is_root() {
        eprintln!("skipping runtime_exec_runs_non_interactive_closure: requires root");
        return;
    }

    let fixture = ExecFixture::new("exec-non-interactive", false, &["FOO=bar", "TERM=xterm"])
        .expect("failed to prepare exec fixture");
    let output_path = fixture.artifact("non_interactive_output");

    let exec_output_path = output_path.clone();

    Runtime::with_namespace(fixture.id(), move || {
        let foo = std::env::var("FOO")?;
        let term = std::env::var("TERM")?;
        let mut file = fs::File::create(&exec_output_path)?;
        writeln!(file, "{foo}:{term}")?;
        Ok(())
    })
    .unwrap_or_else(|err| panic!("Runtime::exec failed: {err}"));

    let contents =
        fs::read_to_string(&output_path).expect("expected exec closure to write output file");
    assert_eq!(contents.trim(), "bar:xterm");
    assert!(
        Runtime::take_exec_proc_mount_status().is_some(),
        "exec run should record /proc mount status"
    );
}

#[test]
fn runtime_exec_respects_terminal_flag() {
    if !Uid::effective().is_root() {
        eprintln!("skipping runtime_exec_respects_terminal_flag: requires root");
        return;
    }

    let fixture = ExecFixture::new(
        "exec-interactive",
        true,
        &["SHELL=/bin/sh", "TERM=xterm-256color"],
    )
    .expect("failed to prepare exec fixture");
    let marker = fixture.artifact("interactive_marker");

    let exec_marker = marker.clone();

    Runtime::with_namespace(fixture.id(), move || {
        let term = std::env::var("TERM")?;
        if term != "xterm-256color" {
            return Err(Box::new(io::Error::other(format!(
                "unexpected TERM value: {term}"
            ))));
        }
        fs::write(&exec_marker, "interactive")?;
        Ok(())
    })
    .unwrap_or_else(|err| panic!("Runtime::exec failed for interactive case: {err}"));

    let marker_data = fs::read_to_string(&marker).expect("interactive exec marker missing");
    assert_eq!(marker_data, "interactive");
    assert!(
        Runtime::take_exec_proc_mount_status().is_some(),
        "interactive exec should still report /proc status"
    );
}

#[test]
fn runtime_exec_propagates_closure_failure() {
    if !Uid::effective().is_root() {
        eprintln!("skipping runtime_exec_propagates_closure_failure: requires root");
        return;
    }

    let fixture =
        ExecFixture::new("exec-failure", false, &["FOO=bar"]).expect("failed to prepare fixture");

    let error = Runtime::with_namespace(fixture.id(), move || {
        Err(Box::new(io::Error::other("exec failure path")))
    })
    .expect_err("Runtime::exec should surface closure failure");

    assert!(
        error.to_string().contains("exec failure path"),
        "expected propagated error message, got {error}"
    );
    assert!(
        Runtime::take_exec_proc_mount_status().is_some(),
        "failure path should still set /proc status indicator"
    );
}
