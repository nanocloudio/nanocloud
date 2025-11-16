use crate::oci_harness::{EnvGuard, FakeRegistryFixture, HarnessArtifacts};
use nanocloud::nanocloud::engine::log::write_docker_json_logs;
use nanocloud::nanocloud::engine::Image;
use nanocloud::nanocloud::kubelet::RestartBackoff;
use nanocloud::nanocloud::oci::runtime::{
    terminate_with_timeout, Linux, Mount, Namespace, OciConfig, Process, Root, Runtime,
    SecurityPolicy, User,
};
use nanocloud::nanocloud::oci::Registry;
use nanocloud::nanocloud::util::security::SecureAssets;
use nix::unistd::Uid;
use serial_test::serial;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{duplex, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::Instant as TokioInstant;

#[tokio::test]
#[serial]
async fn fake_registry_pull_reuses_cache_without_source_blobs() {
    let image_root = TempDir::new().expect("failed to create image root tempdir");
    let fake_registry = FakeRegistryFixture::new();
    let artifacts = HarnessArtifacts::new("image-pull-cache");
    let _image_guard = EnvGuard::set("NANOCLOUD_IMAGE_ROOT", image_root.path());
    let _registry_guard = fake_registry.activate();

    Registry::pull(fake_registry.image(), false)
        .await
        .expect("initial pull should succeed");

    fake_registry.remove_blob(fake_registry.config_digest());
    for digest in fake_registry.layer_digests() {
        fake_registry.remove_blob(&digest);
    }

    Registry::pull(fake_registry.image(), false)
        .await
        .expect("cached pull should succeed even when fake blobs removed");

    assert!(
        Registry::pull(fake_registry.image(), true).await.is_err(),
        "force update must fail when fake blobs are missing"
    );
    artifacts.write_text(
        "summary.txt",
        "Pulled fake-loop image twice; second run succeeded without blob sources. Forced update failed as expected.\n",
    );
}

#[tokio::test]
#[serial]
async fn macro_defaults_expand_expected_values() {
    let image_root = TempDir::new().expect("failed to create image root tempdir");
    let fake_registry = FakeRegistryFixture::new();
    let artifacts = HarnessArtifacts::new("macro-expansion");
    let _image_guard = EnvGuard::set("NANOCLOUD_IMAGE_ROOT", image_root.path());
    let _registry_guard = fake_registry.activate();
    let secure_dir = TempDir::new().expect("secure assets dir");
    SecureAssets::generate(secure_dir.path(), false).expect("generate secure assets");
    let _secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", secure_dir.path());

    let image = Image::load(None, "conformance/fake-loop", HashMap::new(), false)
        .await
        .expect("image load succeeds");

    let dns_value = image
        .config
        .options
        .get("macro_dns")
        .expect("macro_dns option present");
    assert!(
        !dns_value.is_empty(),
        "DNS macro should resolve to at least one server"
    );

    let rand_value = image
        .config
        .options
        .get("macro_rand")
        .expect("macro_rand present");
    assert_eq!(
        rand_value.len(),
        16,
        "rand hex macro should produce 16 characters for length 8"
    );
    assert!(
        rand_value.chars().all(|c| c.is_ascii_hexdigit()),
        "rand macro should emit hex characters"
    );

    let tls_value = image
        .config
        .options
        .get("macro_tls")
        .expect("macro_tls present");
    assert_eq!(
        tls_value, "/var/run/nanocloud.io/tls/cert.pem",
        "tls macro should map to cert path"
    );
    artifacts.write_text(
        "summary.txt",
        format!(
            "DNS macro resolved to {dns}; rand macro len {len}; tls macro -> {tls}\n",
            dns = dns_value,
            len = rand_value.len(),
            tls = tls_value
        ),
    );
}

#[tokio::test]
#[serial]
async fn signals_and_restart_backoff_behaviors() {
    let artifacts = HarnessArtifacts::new("signals-restart-policy");
    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn sleep helper");
    let pid = child
        .id()
        .expect("spawned helper must expose a PID")
        .to_string();

    terminate_with_timeout(&pid, Duration::from_millis(500))
        .await
        .expect("signal escalation must complete");
    child
        .wait()
        .await
        .expect("helper wait should succeed after signals");

    let mut backoff = RestartBackoff::default();
    let now = TokioInstant::now();
    backoff.on_failure(now, "first".into());
    assert!(
        !backoff.should_retry(now),
        "backoff should enforce delay after first failure"
    );
    assert!(
        backoff.should_retry(now + Duration::from_secs(1)),
        "first failure should unlock after one second"
    );

    backoff.on_failure(now, "second".into());
    assert!(
        backoff.should_retry(now + Duration::from_secs(2)),
        "second attempt should double retry window"
    );

    backoff.on_success(TokioInstant::now());
    assert!(
        backoff.should_retry(TokioInstant::now()),
        "successful restart should reset retry gate"
    );

    artifacts.write_text(
        "summary.txt",
        "Signals escalated from sleep helper and RestartBackoff enforced delays (1s, 2s) before resetting on success.\n",
    );
}

#[tokio::test]
#[serial]
async fn log_and_exec_flows_round_trip() {
    let artifacts = HarnessArtifacts::new("log-and-exec-flows");

    let temp_logs = TempDir::new().expect("log tempdir");
    let log_path = temp_logs.path().join("docker.json");

    let (mut stdout_writer, stdout_reader) = duplex(128);
    let (mut stderr_writer, stderr_reader) = duplex(64);

    tokio::spawn(async move {
        stdout_writer
            .write_all(b"stdout-line-1\nstdout-line-2\n")
            .await
            .expect("stdout write");
    });
    tokio::spawn(async move {
        stderr_writer
            .write_all(b"stderr-only\n")
            .await
            .expect("stderr write");
    });

    write_docker_json_logs(stdout_reader, stderr_reader, &log_path)
        .await
        .expect("docker log writer must succeed");

    let log_contents = std::fs::read_to_string(&log_path).expect("log file readable");
    assert!(log_contents.contains("stdout-line-1"));
    assert!(log_contents.contains("stderr-only"));

    if !Uid::effective().is_root() {
        eprintln!("skipping exec portion: requires root permissions");
        artifacts.write_text(
            "summary.txt",
            format!(
                "Log writer captured stdout/stderr at {}. Exec validation skipped (needs root).\n",
                log_path.display()
            ),
        );
        return;
    }

    let fixture = ExecFixture::new("exec-conformance", &[]).expect("exec fixture");
    let marker = fixture.artifact("exec-marker");
    let exec_path = marker.clone();
    Runtime::with_namespace(fixture.id(), move || {
        fs::write(&exec_path, "nanocloud-exec\n")
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    })
    .expect("exec helper should succeed");

    let exec_contents = fs::read_to_string(&marker).expect("exec marker readable");
    assert_eq!(exec_contents.trim(), "nanocloud-exec");

    artifacts.write_text(
        "summary.txt",
        format!(
            "Docker JSON logs captured at {} and Runtime::with_namespace wrote {}.\n",
            log_path.display(),
            marker.display()
        ),
    );
}

struct ExecFixture {
    container_id: String,
    container_dir: PathBuf,
    _root: TempDir,
    _guard: EnvGuard,
}

impl ExecFixture {
    fn new(container_id: &str, env_entries: &[&str]) -> io::Result<Self> {
        let root = TempDir::new()?;
        let guard = EnvGuard::set("NANOCLOUD_CONTAINER_ROOT", root.path());
        let container_dir = root.path().join(container_id);
        fs::create_dir_all(&container_dir)?;

        let pid_path = container_dir.join("pid");
        fs::write(&pid_path, format!("{}\n", std::process::id()))?;

        let mut config = base_config(env_entries);
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

        let mut config_file = File::create(container_dir.join("config.json"))?;
        serde_json::to_writer(&mut config_file, &config)?;
        config_file.flush()?;

        Ok(Self {
            container_id: container_id.to_string(),
            container_dir,
            _root: root,
            _guard: guard,
        })
    }

    fn id(&self) -> &str {
        &self.container_id
    }

    fn artifact(&self, name: &str) -> PathBuf {
        self.container_dir.join(name)
    }
}

fn base_config(env_entries: &[&str]) -> OciConfig {
    OciConfig {
        oci_version: "1.0.2".into(),
        process: Process {
            terminal: false,
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
        hostname: "exec-conformance".into(),
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
        security: SecurityPolicy::default(),
    }
}
