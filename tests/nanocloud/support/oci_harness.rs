use nanocloud::nanocloud::oci::OciManifest;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use walkdir::WalkDir;

const FIXTURE_IMAGE: &str = "registry.nanocloud.io/conformance/fake-loop:latest";

#[derive(Debug)]
pub struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    pub fn set(key: &'static str, value: &Path) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, value);
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

pub struct FakeRegistryFixture {
    root: TempDir,
    manifest: OciManifest,
}

impl FakeRegistryFixture {
    pub fn new() -> Self {
        let root = TempDir::new().expect("failed to create fake registry tempdir");
        copy_tree(Path::new("tests/data/fake_registry"), root.path())
            .expect("failed to copy fake registry dataset");
        let manifest_path = root
            .path()
            .join("manifests/registry.nanocloud.io/conformance/fake-loop/latest.json");
        let manifest_file =
            fs::File::open(&manifest_path).expect("fake manifest missing after copy");
        let manifest: OciManifest =
            serde_json::from_reader(manifest_file).expect("failed to parse fake manifest");
        Self { root, manifest }
    }

    pub fn image(&self) -> &'static str {
        FIXTURE_IMAGE
    }

    pub fn activate(&self) -> EnvGuard {
        EnvGuard::set("NANOCLOUD_FAKE_REGISTRY", self.root.path())
    }

    pub fn config_digest(&self) -> &str {
        &self.manifest.config.digest
    }

    pub fn layer_digests(&self) -> Vec<String> {
        self.manifest
            .layers
            .iter()
            .map(|layer| layer.digest.clone())
            .collect()
    }

    pub fn remove_blob(&self, digest: &str) {
        let path = self
            .root
            .path()
            .join("blobs/sha256")
            .join(digest_suffix(digest));
        fs::remove_file(&path)
            .unwrap_or_else(|err| panic!("failed to remove fake blob {}: {err}", path.display()));
    }
}

fn copy_tree(src: &Path, dst: &Path) -> io::Result<()> {
    for entry in WalkDir::new(src) {
        let entry = entry?;
        let rel = entry.path().strip_prefix(src).unwrap();
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn digest_suffix(digest: &str) -> &str {
    digest.strip_prefix("sha256:").unwrap_or(digest)
}

pub struct HarnessArtifacts {
    root: PathBuf,
}

impl HarnessArtifacts {
    pub fn new(case: &str) -> Self {
        let base = env::var("OCI_CONFORMANCE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let target = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
                PathBuf::from(target).join("oci-conformance")
            });
        let root = base.join(case);
        fs::create_dir_all(&root).unwrap_or_else(|err| {
            panic!("failed to create artifact dir {}: {err}", root.display())
        });
        Self { root }
    }

    pub fn write_text(&self, name: &str, contents: impl AsRef<[u8]>) {
        fs::write(self.root.join(name), contents)
            .unwrap_or_else(|err| panic!("failed to write artifact {name}: {err}"));
    }
}
