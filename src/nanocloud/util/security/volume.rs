use std::env;
use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use nix::errno::Errno;
use nix::mount::umount2;
use nix::unistd::Uid;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;

use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::Config;

pub fn encrypted_volume_root() -> PathBuf {
    Config::EncryptedVolumes.get_path()
}

fn record_encryption_event(entry: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let path = match env::var("NANOCLOUD_ENCRYPTION_RECORD") {
        Ok(path) if !path.is_empty() => path,
        _ => return Ok(false),
    };
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| with_context(e, format!("Failed to open encryption record '{}'", path)))?;
    writeln!(file, "{entry}")
        .map_err(|e| with_context(e, format!("Failed to write encryption record '{}'", path)))?;
    Ok(true)
}

pub fn sanitize_identifier(value: &str, fallback: &str) -> String {
    let mut sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    let trimmed = sanitized.trim_matches('-').to_string();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed
    }
}

pub fn encrypted_mapper_name(container_id: &str, volume_name: &str) -> String {
    let container_component: String = sanitize_identifier(container_id, "ctr")
        .chars()
        .take(16)
        .collect();
    let volume_component = sanitize_identifier(volume_name, "volume");
    format!("ncld-{}-{}", container_component, volume_component)
}

pub fn encrypted_host_mount(container_id: &str, volume_name: &str) -> PathBuf {
    let container_component = sanitize_identifier(container_id, "ctr");
    let volume_component = sanitize_identifier(volume_name, "volume");
    encrypted_volume_root()
        .join(container_component)
        .join(volume_component)
}

pub fn read_volume_key(key_name: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let key_path = Config::SecureAssets
        .get_path()
        .join("volumes")
        .join(format!("{}.key", key_name));
    let encoded = fs::read_to_string(&key_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to read encrypted volume key '{}'",
                key_path.display()
            ),
        )
    })?;
    BASE64
        .decode(encoded.trim().as_bytes())
        .map_err(|e| with_context(e, "Failed to decode encrypted volume key"))
}

pub fn ensure_luks_device(device: &str, key: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!("ensure_luks {}", device))? {
        return Ok(());
    }

    let status = Command::new("cryptsetup")
        .arg("isLuks")
        .arg(device)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to invoke cryptsetup isLuks for {}", device),
            )
        })?;
    if status.success() {
        return Ok(());
    }

    let output = run_cryptsetup_with_key(
        &[
            "luksFormat",
            "--type",
            "luks2",
            "--batch-mode",
            "--key-file",
            "-",
            device,
        ],
        key,
    )?;

    if output.status.success() {
        Ok(())
    } else {
        Err(new_error(format!(
            "cryptsetup luksFormat failed for {}: {}",
            device,
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

pub fn open_mapper(
    device: &str,
    mapper: &str,
    key: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!("open_mapper {} {}", device, mapper))? {
        return Ok(());
    }

    let mapper_path = Path::new("/dev/mapper").join(mapper);
    if mapper_path.exists() {
        return Ok(());
    }

    let output = run_cryptsetup_with_key(
        &[
            "luksOpen",
            "--type",
            "luks2",
            "--key-file",
            "-",
            device,
            mapper,
        ],
        key,
    )?;

    if output.status.success() {
        Ok(())
    } else {
        Err(new_error(format!(
            "cryptsetup luksOpen failed for {} as {}: {}",
            device,
            mapper,
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

pub fn mount_mapper(
    mapper: &str,
    target: &Path,
    filesystem: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!(
        "mount_mapper {} {} {}",
        mapper,
        target.display(),
        filesystem
    ))? {
        return Ok(());
    }

    let device_path = format!("/dev/mapper/{}", mapper);
    let source =
        CString::new(device_path.clone()).map_err(|e| with_context(e, "Invalid mapper path"))?;
    let target_bytes = target.as_os_str().as_bytes().to_vec();
    let target_cstr =
        CString::new(target_bytes).map_err(|e| with_context(e, "Invalid mount path"))?;
    let fs_cstr =
        CString::new(filesystem).map_err(|e| with_context(e, "Invalid filesystem name"))?;

    if let Err(err) = nix::mount::mount(
        Some(source.as_c_str()),
        target_cstr.as_c_str(),
        Some(fs_cstr.as_c_str()),
        nix::mount::MsFlags::MS_RELATIME,
        None::<&str>,
    ) {
        if err == Errno::EINVAL {
            mkfs_device(mapper, filesystem)?;
            nix::mount::mount(
                Some(source.as_c_str()),
                target_cstr.as_c_str(),
                Some(fs_cstr.as_c_str()),
                nix::mount::MsFlags::MS_RELATIME,
                None::<&str>,
            )
            .map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to mount filesystem {} on {} after formatting",
                        filesystem,
                        target.display()
                    ),
                )
            })?
        } else {
            return Err(with_context(
                err,
                format!("Failed to mount mapper {} on {}", mapper, target.display()),
            ));
        }
    }

    Ok(())
}

pub fn unmount_if_mounted(path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!("unmount {}", path.display()))? {
        return Ok(());
    }

    match umount2(path, nix::mount::MntFlags::MNT_DETACH) {
        Ok(_) => Ok(()),
        Err(err) if err == Errno::EINVAL || err == Errno::ENOENT => Ok(()),
        Err(err) => Err(with_context(
            err,
            format!("Failed to unmount {}", path.display()),
        )),
    }
}

pub fn close_mapper(mapper: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!("close_mapper {}", mapper))? {
        return Ok(());
    }

    let mapper_path = Path::new("/dev/mapper").join(mapper);
    if !mapper_path.exists() {
        return Ok(());
    }

    let output = Command::new("cryptsetup")
        .arg("close")
        .arg(mapper)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to invoke cryptsetup close for {}", mapper),
            )
        })?;

    if output.status.success() {
        Ok(())
    } else {
        Err(new_error(format!(
            "cryptsetup close failed for {}: {}",
            mapper,
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

pub fn cleanup_mount_dir(path: &Path) {
    if let Err(err) = fs::remove_dir_all(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            log::warn!(
                "Failed to remove encrypted mount directory {}: {}",
                path.display(),
                err
            );
        }
    }

    let mut current = path.parent();
    let root = encrypted_volume_root();
    while let Some(parent) = current {
        if parent == root {
            break;
        }
        match fs::remove_dir(parent) {
            Ok(_) => current = parent.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => {
                log::warn!(
                    "Failed to remove encrypted mount parent {}: {}",
                    parent.display(),
                    err
                );
                break;
            }
        }
    }
}

fn run_cryptsetup_with_key(
    args: &[&str],
    key: &[u8],
) -> Result<std::process::Output, Box<dyn Error + Send + Sync>> {
    let mut command = Command::new("cryptsetup");
    command.args(args);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| with_context(e, "Failed to spawn cryptsetup"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(key)?;
    }
    child
        .wait_with_output()
        .map_err(|e| with_context(e, "cryptsetup execution failed"))
}

pub(crate) fn mkfs_device(
    mapper: &str,
    filesystem: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if record_encryption_event(&format!("mkfs {} {}", mapper, filesystem))? {
        return Ok(());
    }

    let device_path = format!("/dev/mapper/{}", mapper);
    let output = Command::new("mkfs")
        .args(["-t", filesystem, "-F", &device_path])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| with_context(e, format!("Failed to invoke mkfs for {}", device_path)))?;

    if output.status.success() {
        Ok(())
    } else {
        Err(new_error(format!(
            "mkfs failed for {}: {}",
            device_path,
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

/// Ensure encrypted volume root directory exists with appropriate ownership.
pub fn ensure_encrypted_volume_root() -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let root = encrypted_volume_root();
    fs::create_dir_all(&root).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create encrypted volume root '{}'",
                root.display()
            ),
        )
    })?;
    if let Ok(metadata) = fs::metadata(&root) {
        if metadata.uid() == 0 && Uid::effective().is_root() {
            // leave ownership as root:root
        }
    }
    Ok(root)
}
