/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fmt;

use crate::nanocloud::cli::args::TokenArgs;
use crate::nanocloud::cli::commands::token;
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::cni::cni_plugin;
use crate::nanocloud::k8s::configmap::ConfigMap;
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::store;
use crate::nanocloud::util::security::SecureAssets;
use crate::nanocloud::Config;

const BANNER: &str = r#"
################################################################
                                _                 _   _         
   _ __   __ _ _ __   ___   ___| | ___  _   _  __| | (_) ___    
  | '_ \ / _` | '_ \ / _ \ / __| |/ _ \| | | |/ _` | | |/ _ \   
  | | | | (_| | | | | (_) | (__| | (_) | |_| | (_| |_| | (_) |  
  |_| |_|\__,_|_| |_|\___/ \___|_|\___/ \__,_|\__,_(_)_|\___/   
                                                                
################################################################

"#;

const BACKUP_CONFIG_NAMESPACE: &str = "kube-system";
const BACKUP_CONFIG_NAME: &str = "nanocloud.io";
const BACKUP_RETENTION_KEY: &str = "backup.retentionCount";
const DEFAULT_BACKUP_RETENTION: usize = 3;

pub struct Setup;

#[derive(Debug, Clone, Copy)]
pub enum SetupStage {
    SecureAssetsVerify,
    SecureAssetsGenerate,
    NetworkBridge,
    BackupConfig,
    Token,
}

#[derive(Debug)]
pub struct SetupError {
    stage: SetupStage,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl SetupError {
    fn new(stage: SetupStage, source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { stage, source }
    }
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "setup step '{}' failed: {}", self.stage, self.source)
    }
}

impl std::error::Error for SetupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl fmt::Display for SetupStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            SetupStage::SecureAssetsVerify => "secure-assets-verify",
            SetupStage::SecureAssetsGenerate => "secure-assets-generate",
            SetupStage::NetworkBridge => "network-bridge",
            SetupStage::BackupConfig => "backup-configmap",
            SetupStage::Token => "token-qr",
        };
        write!(f, "{label}")
    }
}

impl Setup {
    /// Execute the setup pipeline (secure assets, bridge, backup config, onboarding token) with stage-labeled logging.
    /// Each failure is wrapped with the stage name so operators can see which phase needs attention; usable for install or repair flows.
    pub fn run(repair: bool) -> Result<(), SetupError> {
        let mode = if repair { "repair" } else { "install" };

        Terminal::stdout(format_args!("{}", BANNER.trim_end_matches('\n')));
        Terminal::stdout(format_args!("[setup::{mode}] Starting Nanocloud setup"));

        let secure_assets = Config::SecureAssets.get_path();
        let secure_assets_display = secure_assets.display().to_string();

        Terminal::stdout(format_args!(
            "[setup::{mode}] Verifying secure assets at {}",
            secure_assets_display
        ));
        run_step(SetupStage::SecureAssetsVerify, || {
            Config::SecureAssets
                .verify(None, !repair)
                .map(|_| ())
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err })?;
            Ok(())
        })?;

        Terminal::stdout(format_args!(
            "[setup::{mode}] Generating secure assets at {}",
            secure_assets_display
        ));
        run_step(SetupStage::SecureAssetsGenerate, || {
            SecureAssets::generate(&secure_assets, repair)
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err })?;
            Ok(())
        })?;
        Terminal::stdout(format_args!("[setup::{mode}] Secure assets ready"));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Ensuring network bridge nanocloud0 (172.20.0.1/16)"
        ));
        run_step(SetupStage::NetworkBridge, || {
            cni_plugin()
                .bridge("nanocloud0", "172.20.0.1/16")
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err })?;
            Ok(())
        })?;
        Terminal::stdout(format_args!("[setup::{mode}] Network bridge available"));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Ensuring backup retention configmap {}/{}",
            BACKUP_CONFIG_NAMESPACE, BACKUP_CONFIG_NAME
        ));
        run_step(SetupStage::BackupConfig, ensure_backup_configmap)?;
        Terminal::stdout(format_args!(
            "[setup::{mode}] Backup retention config ready"
        ));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Generating onboarding token (nanocloud token --qr)"
        ));
        run_step(SetupStage::Token, || {
            token::handle_token(&TokenArgs {
                user: "admin".to_string(),
                cluster: None,
                curl: false,
                qr: true,
            })
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err })?;
            Ok(())
        })?;

        Terminal::stdout(format_args!("[setup::{mode}] Nanocloud setup complete"));
        Ok(())
    }
}

fn ensure_backup_configmap() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let existing = store::load_config_map(Some(BACKUP_CONFIG_NAMESPACE), BACKUP_CONFIG_NAME)?;
    let mut config = existing.unwrap_or_else(|| {
        ConfigMap::new(ObjectMeta {
            name: Some(BACKUP_CONFIG_NAME.to_string()),
            namespace: Some(BACKUP_CONFIG_NAMESPACE.to_string()),
            ..Default::default()
        })
    });

    let needs_update = !matches!(
        config.data.get(BACKUP_RETENTION_KEY),
        Some(value) if !value.trim().is_empty()
    );

    if needs_update {
        config.data.insert(
            BACKUP_RETENTION_KEY.to_string(),
            DEFAULT_BACKUP_RETENTION.to_string(),
        );
        store::save_config_map(Some(BACKUP_CONFIG_NAMESPACE), BACKUP_CONFIG_NAME, &config)?;
    }

    Ok(())
}

fn run_step<F>(stage: SetupStage, action: F) -> Result<(), SetupError>
where
    F: FnOnce() -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    action().map_err(|err| SetupError::new(stage, err))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_stage_display_matches_labels() {
        assert_eq!(
            SetupStage::SecureAssetsVerify.to_string(),
            "secure-assets-verify"
        );
        assert_eq!(
            SetupStage::SecureAssetsGenerate.to_string(),
            "secure-assets-generate"
        );
        assert_eq!(SetupStage::NetworkBridge.to_string(), "network-bridge");
        assert_eq!(SetupStage::BackupConfig.to_string(), "backup-configmap");
        assert_eq!(SetupStage::Token.to_string(), "token-qr");
    }

    #[test]
    fn run_step_wraps_errors_with_stage() {
        let err = run_step(SetupStage::NetworkBridge, || {
            Err(Box::new(std::io::Error::other("boom")))
        })
        .expect_err("should fail");

        assert!(format!("{err}").contains("network-bridge"));
        assert!(format!("{err}").contains("boom"));
    }
}
