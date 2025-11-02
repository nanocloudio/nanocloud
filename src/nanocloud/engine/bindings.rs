use crate::nanocloud::oci::runtime::{container_root_path, netns_dir};
use crate::nanocloud::security::seccomp::SeccompFilter;
use crate::nanocloud::util::error::{new_error, with_context};
use humantime::parse_duration;
use nix::sched::{setns, CloneFlags};
use nix::unistd::{setgid, setgroups, setuid, Gid, Uid};
use std::env;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time;

type DynError = Box<dyn Error + Send + Sync>;
const DEFAULT_UID: u32 = 65_534;
const DEFAULT_GID: u32 = 65_534;
const DEFAULT_TIMEOUT_SECS: u64 = 90;
const DEFAULT_APPARMOR_PROFILE: &str = "nanocloud-bindings";
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin";
const NETNS_RETRY_LIMIT: usize = 50;
const NETNS_RETRY_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_SECCOMP_PROFILE: &str =
    include_str!("../../../assets/security/bindings-default.json");

fn isolation_enabled() -> bool {
    fn parse_flag(key: &str) -> Option<bool> {
        env::var(key).ok().map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
    }

    let skip = parse_flag("NANOCLOUD_BINDING_SKIP_ISOLATION")
        .or_else(|| parse_flag("NANOCLOUD_BINDING_SKIP_NAMESPACES"))
        .unwrap_or(false);
    !skip
}

enum SeccompProfileSource {
    Embedded,
    Path(PathBuf),
}

/// Runtime policy controlling how bindings are executed.
#[derive(Debug, Clone)]
pub struct BindingEnvelopePolicy {
    pub uid: u32,
    pub gid: u32,
    pub timeout: Duration,
    pub seccomp: Option<SeccompFilter>,
    pub apparmor_profile: Option<String>,
}

impl BindingEnvelopePolicy {
    pub fn from_environment() -> Result<Self, DynError> {
        let (uid, gid) = parse_identity()?;
        let timeout = parse_timeout().unwrap_or_else(|err| {
            log::warn!("Failed to parse NANOCLOUD_BINDING_TIMEOUT: {}", err);
            Duration::from_secs(DEFAULT_TIMEOUT_SECS)
        });
        let seccomp = load_seccomp_filter()?;
        Ok(Self {
            uid,
            gid,
            timeout,
            seccomp,
            apparmor_profile: parse_apparmor_profile(),
        })
    }
}

fn parse_identity() -> Result<(u32, u32), DynError> {
    let mut uid = DEFAULT_UID;
    let mut gid = DEFAULT_GID;

    if let Ok(raw) = env::var("NANOCLOUD_BINDING_UID") {
        if let Some((user, group)) = raw.split_once(':') {
            uid = user.trim().parse().map_err(|err| {
                new_error(format!(
                    "Failed to parse NANOCLOUD_BINDING_UID '{}': {}",
                    raw, err
                ))
            })?;
            gid = group.trim().parse().map_err(|err| {
                new_error(format!(
                    "Failed to parse gid from NANOCLOUD_BINDING_UID '{}': {}",
                    raw, err
                ))
            })?;
        } else {
            uid = raw.trim().parse().map_err(|err| {
                new_error(format!(
                    "Failed to parse NANOCLOUD_BINDING_UID '{}': {}",
                    raw, err
                ))
            })?;
        }
    }

    if let Ok(raw_gid) = env::var("NANOCLOUD_BINDING_GID") {
        gid = raw_gid.trim().parse().map_err(|err| {
            new_error(format!(
                "Failed to parse NANOCLOUD_BINDING_GID '{}': {}",
                raw_gid, err
            ))
        })?;
    }

    Ok((uid, gid))
}

fn parse_timeout() -> Result<Duration, DynError> {
    match env::var("NANOCLOUD_BINDING_TIMEOUT") {
        Ok(value) => parse_duration(value.trim())
            .map_err(|err| new_error(format!("Invalid NANOCLOUD_BINDING_TIMEOUT: {}", err))),
        Err(_) => Ok(Duration::from_secs(DEFAULT_TIMEOUT_SECS)),
    }
}

fn load_seccomp_filter() -> Result<Option<SeccompFilter>, DynError> {
    match parse_seccomp_source() {
        Some(SeccompProfileSource::Embedded) => {
            SeccompFilter::from_str(DEFAULT_SECCOMP_PROFILE).map(Some)
        }
        Some(SeccompProfileSource::Path(path)) => SeccompFilter::from_path(&path).map(Some),
        None => Ok(None),
    }
}

fn parse_seccomp_source() -> Option<SeccompProfileSource> {
    match env::var("NANOCLOUD_BINDING_SECCOMP") {
        Ok(raw) if raw.trim().eq_ignore_ascii_case("disable") => None,
        Ok(raw) if !raw.trim().is_empty() => {
            Some(SeccompProfileSource::Path(PathBuf::from(raw.trim())))
        }
        _ => Some(SeccompProfileSource::Embedded),
    }
}

fn parse_apparmor_profile() -> Option<String> {
    match env::var("NANOCLOUD_BINDING_APPARMOR") {
        Ok(raw) if raw.trim().eq_ignore_ascii_case("disable") => None,
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => Some(DEFAULT_APPARMOR_PROFILE.to_string()),
    }
}

/// Binding invocation metadata used by the runner.
#[derive(Debug, Clone)]
pub struct BindingInvocation {
    pub bundle: String,
    pub namespace: String,
    pub target_service: String,
    pub container_id: String,
    pub binding_id: Option<String>,
    pub command: Vec<String>,
}

/// Result returned after a binding command finishes.
#[derive(Debug)]
pub struct BindingResult {
    pub status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

/// Execute a binding command inside the configured envelope.
pub async fn run_binding(
    invocation: &BindingInvocation,
    policy: &BindingEnvelopePolicy,
) -> Result<BindingResult, DynError> {
    if invocation.command.is_empty() {
        return Err(new_error(format!(
            "Binding command list is empty for service '{}'",
            invocation.target_service
        )));
    }

    let isolation_enabled = isolation_enabled();
    let namespace = if isolation_enabled {
        Some(NamespaceConfig::new(&invocation.container_id)?)
    } else {
        None
    };
    let mut command = Command::new(&invocation.command[0]);
    if invocation.command.len() > 1 {
        command.args(&invocation.command[1..]);
    }

    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    configure_environment(&mut command, invocation);

    let uid = policy.uid;
    let gid = policy.gid;
    let seccomp_filter = policy.seccomp.clone();
    let apparmor_profile = policy.apparmor_profile.clone();
    let namespace_clone = namespace.clone();
    let isolation_for_exec = isolation_enabled;
    unsafe {
        command.pre_exec(move || {
            if isolation_for_exec {
                if let Some(ns) = namespace_clone.as_ref() {
                    ns.apply()
                        .map_err(|err| io::Error::other(err.to_string()))?;
                }
                apply_apparmor(apparmor_profile.as_deref())
                    .and_then(|_| drop_privileges(uid, gid))
                    .and_then(|_| apply_seccomp(seccomp_filter.as_ref()))
                    .map_err(|err| io::Error::other(err.to_string()))?;
            }
            Ok(())
        });
    }

    let started = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|err| with_context(err, "Failed to spawn binding command"))?;
    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let stdout_handle = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stdout_pipe {
            let _ = AsyncReadExt::read_to_end(&mut reader, &mut buf).await;
        }
        buf
    });
    let stderr_handle = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stderr_pipe {
            let _ = AsyncReadExt::read_to_end(&mut reader, &mut buf).await;
        }
        buf
    });
    let status = match time::timeout(policy.timeout, child.wait()).await {
        Ok(result) => {
            result.map_err(|err| with_context(err, "Failed to wait for binding command"))?
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_handle.await;
            let _ = stderr_handle.await;
            return Err(new_error(format!(
                "Binding command timed out after {:?}",
                policy.timeout
            )));
        }
    };
    let stdout_bytes = stdout_handle.await.unwrap_or_else(|_| Vec::new());
    let stderr_bytes = stderr_handle.await.unwrap_or_else(|_| Vec::new());

    Ok(BindingResult {
        status,
        stdout: decode_output(&stdout_bytes),
        stderr: decode_output(&stderr_bytes),
        duration: started.elapsed(),
    })
}

fn configure_environment(command: &mut Command, invocation: &BindingInvocation) {
    command.env_clear();
    command.env("PATH", DEFAULT_PATH);
    command.env("LANG", "C.UTF-8");
    command.env("NANOCLOUD_SERVICE", &invocation.target_service);
    command.env("NANOCLOUD_NAMESPACE", &invocation.namespace);
    command.env("NANOCLOUD_BUNDLE", &invocation.bundle);
    if let Some(id) = &invocation.binding_id {
        command.env("NANOCLOUD_BINDING_ID", id);
    }
}

fn decode_output(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes).trim().to_string();
    text
}

#[derive(Clone)]
struct NamespaceConfig {
    pid: i32,
    netns_path: PathBuf,
}

impl NamespaceConfig {
    fn new(container_id: &str) -> Result<Self, DynError> {
        let pid_path = container_root_path(container_id).join("pid");
        let pid_contents =
            fs::read_to_string(&pid_path).map_err(|err| with_context(err, "Failed to read PID"))?;
        let pid: i32 = pid_contents
            .trim()
            .parse()
            .map_err(|err| new_error(format!("Invalid PID value '{}': {}", pid_contents, err)))?;

        Ok(Self {
            pid,
            netns_path: netns_dir().join(container_id),
        })
    }

    fn apply(&self) -> Result<(), DynError> {
        enter_network_namespace(&self.netns_path, self.pid)?;
        for ns in ["pid", "uts", "ipc", "mnt"] {
            enter_namespace(&namespace_path(self.pid, ns))?;
        }
        Ok(())
    }
}

fn namespace_path(pid: i32, ns: &str) -> PathBuf {
    PathBuf::from(format!("/proc/{}/ns/{}", pid, ns))
}

fn enter_network_namespace(path: &Path, pid: i32) -> Result<(), DynError> {
    for attempt in 0..NETNS_RETRY_LIMIT {
        match FileHandle::open(path) {
            Ok(handle) => return handle.setns(),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if attempt + 1 == NETNS_RETRY_LIMIT {
                    break;
                }
                std::thread::sleep(NETNS_RETRY_DELAY);
            }
            Err(err) => return Err(with_context(err, "Failed to open network namespace")),
        }
    }

    enter_namespace(&namespace_path(pid, "net"))
}

fn enter_namespace(path: &Path) -> Result<(), DynError> {
    let handle = FileHandle::open(path)?;
    handle.setns()
}

struct FileHandle {
    file: std::fs::File,
    path: String,
}

impl FileHandle {
    fn open(path: &Path) -> Result<Self, io::Error> {
        Ok(Self {
            file: std::fs::File::open(path)?,
            path: path.display().to_string(),
        })
    }

    fn setns(&self) -> Result<(), DynError> {
        let fd = unsafe { BorrowedFd::borrow_raw(self.file.as_raw_fd()) };
        setns(fd, CloneFlags::empty())
            .map_err(|err| new_error(format!("Failed to enter namespace {}: {}", self.path, err)))
    }
}

fn drop_privileges(uid: u32, gid: u32) -> Result<(), DynError> {
    setgroups(&[]).map_err(|err| new_error(format!("Failed to clear groups: {}", err)))?;
    setgid(Gid::from_raw(gid)).map_err(|err| new_error(format!("Failed to set gid: {}", err)))?;
    setuid(Uid::from_raw(uid)).map_err(|err| new_error(format!("Failed to set uid: {}", err)))?;
    Ok(())
}

fn apply_apparmor(profile: Option<&str>) -> Result<(), DynError> {
    if let Some(name) = profile {
        match OpenOptions::new().write(true).open("/proc/self/attr/exec") {
            Ok(mut file) => file.write_all(name.as_bytes()).map_err(|err| {
                new_error(format!(
                    "Failed to apply AppArmor profile '{}': {}",
                    name, err
                ))
            })?,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(());
            }
            Err(err) => {
                return Err(new_error(format!(
                    "Failed to open AppArmor exec attribute: {}",
                    err
                )))
            }
        }
    }
    Ok(())
}

fn apply_seccomp(filter: Option<&SeccompFilter>) -> Result<(), DynError> {
    if let Some(seccomp) = filter {
        seccomp.apply()?;
    }
    Ok(())
}
