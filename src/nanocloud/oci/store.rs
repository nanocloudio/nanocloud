use crate::nanocloud::util::error::{new_error, with_context};
use std::error::Error;
use std::path::{Component, Path, PathBuf};

const DEFAULT_IMAGE_ROOT: &str = "/var/lib/nanocloud.io/image";
const ENV_IMAGE_ROOT: &str = "NANOCLOUD_IMAGE_ROOT";
const ENV_FAKE_REGISTRY: &str = "NANOCLOUD_FAKE_REGISTRY";

/// Returns the root directory for OCI image data with normalization and validation.
///
/// The resulting path is absolute, contains no parent directory components, and
/// is validated to avoid obviously unwritable or invalid inputs. The returned
/// directory is expected to contain `refs`, `blobs/sha256`, and `overlay` subtrees.
///
/// ```
/// # use std::fs;
/// use nanocloud::nanocloud::oci::image_store_root;
/// let temp = std::env::temp_dir().join("nanocloud-store-example");
/// fs::create_dir_all(&temp).unwrap();
/// std::env::set_var("NANOCLOUD_IMAGE_ROOT", &temp);
/// let root = image_store_root().unwrap();
/// assert!(root.ends_with("nanocloud-store-example"));
/// std::env::remove_var("NANOCLOUD_IMAGE_ROOT");
/// # let _ = fs::remove_dir_all(&temp);
/// ```
pub fn image_store_root() -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let requested = std::env::var(ENV_IMAGE_ROOT).unwrap_or_else(|_| DEFAULT_IMAGE_ROOT.to_owned());
    let path = normalize_root(&requested, "image store root")?;
    validate_writeable_dir(&path, "image store root")?;
    Ok(path)
}

/// Returns the path to a fake registry dataset when running in tests.
pub fn fake_registry_root() -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
    match std::env::var(ENV_FAKE_REGISTRY) {
        Ok(value) => {
            if value.trim().is_empty() {
                return Err(new_error("NANOCLOUD_FAKE_REGISTRY cannot be empty"));
            }
            Ok(Some(normalize_root(&value, "fake registry root")?))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(with_context(err, "Failed to read NANOCLOUD_FAKE_REGISTRY")),
    }
}

fn normalize_root(raw: &str, label: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    if raw.trim().is_empty() {
        return Err(new_error(format!("{label} cannot be empty")));
    }

    let candidate = Path::new(raw);
    let absolute = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(candidate))
            .map_err(|e| {
                with_context(e, format!("Failed to resolve {label} to an absolute path"))
            })?
    };

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => continue,
            Component::ParentDir => {
                return Err(new_error(format!(
                    "{label} cannot contain parent directory components"
                )))
            }
            other => normalized.push(other),
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(new_error(format!("{label} resolved to an empty path")));
    }

    Ok(normalized)
}

fn validate_writeable_dir(path: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok(metadata) = std::fs::metadata(path) {
        if !metadata.is_dir() {
            return Err(new_error(format!(
                "{label} must be a directory (got {})",
                path.display()
            )));
        }
        if metadata.permissions().readonly() {
            return Err(new_error(format!(
                "{label} at {} is not writable",
                path.display()
            )));
        }
    } else if let Some(parent) = path.parent() {
        if let Ok(metadata) = std::fs::metadata(parent) {
            if metadata.permissions().readonly() {
                return Err(new_error(format!(
                    "{label} parent {} is not writable",
                    parent.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn uses_default_image_root_when_env_missing() {
        std::env::remove_var(ENV_IMAGE_ROOT);
        let root = image_store_root().expect("default root");
        assert!(root.is_absolute());
        assert!(root.ends_with("var/lib/nanocloud.io/image"));
    }

    #[test]
    #[serial]
    fn rejects_parent_components_in_image_root() {
        std::env::set_var(ENV_IMAGE_ROOT, "../bad");
        let result = image_store_root();
        std::env::remove_var(ENV_IMAGE_ROOT);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn resolves_relative_paths_to_absolute() {
        let relative = PathBuf::from("store");
        std::env::set_var(ENV_IMAGE_ROOT, &relative);
        let root = image_store_root().expect("resolved root");
        std::env::remove_var(ENV_IMAGE_ROOT);
        assert!(root.is_absolute());
        assert!(root.ends_with("store"));
    }

    #[test]
    #[serial]
    fn rejects_empty_fake_registry_root() {
        std::env::set_var(ENV_FAKE_REGISTRY, " ");
        let result = fake_registry_root();
        std::env::remove_var(ENV_FAKE_REGISTRY);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn accepts_valid_fake_registry_root() {
        let temp = std::env::temp_dir().join("nanocloud-fake-registry");
        std::fs::create_dir_all(&temp).expect("temp registry dir");
        std::env::set_var(ENV_FAKE_REGISTRY, &temp);
        let root = fake_registry_root()
            .expect("fake registry root")
            .expect("present");
        std::env::remove_var(ENV_FAKE_REGISTRY);
        assert!(root.is_absolute());
        assert!(root.ends_with("nanocloud-fake-registry"));
    }
}
