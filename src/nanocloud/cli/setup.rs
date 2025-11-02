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

use std::process;

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

impl Setup {
    /// Execute the setup steps for a new Nanocloud
    pub fn run(repair: bool) {
        let mode = if repair { "repair" } else { "install" };
        let exit_on_error =
            |step: &str, result: Result<(), Box<dyn std::error::Error + Send + Sync>>| {
                if let Err(err) = result {
                    Terminal::error(format_args!("[setup::{mode}] step '{step}' failed: {err}"));
                    process::exit(1);
                }
            };

        Terminal::stdout(format_args!("{}", BANNER.trim_end_matches('\n')));
        Terminal::stdout(format_args!("[setup::{mode}] Starting Nanocloud setup"));

        let secure_assets = Config::SecureAssets.get_path();
        let secure_assets_display = secure_assets.display().to_string();

        Terminal::stdout(format_args!(
            "[setup::{mode}] Verifying secure assets at {}",
            secure_assets_display
        ));
        exit_on_error(
            "secure-assets-verify",
            Config::SecureAssets.verify(None, !repair).map(|_| ()),
        );

        Terminal::stdout(format_args!(
            "[setup::{mode}] Generating secure assets at {}",
            secure_assets_display
        ));
        exit_on_error(
            "secure-assets-generate",
            SecureAssets::generate(&secure_assets, repair),
        );
        Terminal::stdout(format_args!("[setup::{mode}] Secure assets ready"));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Ensuring network bridge nanocloud0 (172.20.0.1/16)"
        ));
        exit_on_error(
            "network-bridge",
            cni_plugin().bridge("nanocloud0", "172.20.0.1/16"),
        );
        Terminal::stdout(format_args!("[setup::{mode}] Network bridge available"));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Ensuring backup retention configmap {}/{}",
            BACKUP_CONFIG_NAMESPACE, BACKUP_CONFIG_NAME
        ));
        exit_on_error("backup-configmap", ensure_backup_configmap());
        Terminal::stdout(format_args!(
            "[setup::{mode}] Backup retention config ready"
        ));

        Terminal::stdout(format_args!(
            "[setup::{mode}] Generating onboarding token (nanocloud token --qr)"
        ));
        exit_on_error(
            "token-qr",
            token::handle_token(&TokenArgs {
                user: "admin".to_string(),
                cluster: None,
                curl: false,
                qr: true,
            }),
        );

        Terminal::stdout(format_args!("[setup::{mode}] Nanocloud setup complete"));
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
