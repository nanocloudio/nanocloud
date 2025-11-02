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

use std::error::Error;
use std::fs;

use serde::Serialize;

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::args::{
    DeviceArgs, DeviceCommands, DeviceCreateArgs, DeviceDeleteArgs, DeviceGetArgs,
    DeviceIssueCertificateArgs, DeviceListArgs,
};
use crate::nanocloud::cli::Terminal;

pub(super) async fn handle_devices(
    client: &NanocloudClient,
    args: &DeviceArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match &args.command {
        DeviceCommands::List(list_args) => handle_list(client, list_args).await,
        DeviceCommands::Get(get_args) => handle_get(client, get_args).await,
        DeviceCommands::Create(create_args) => handle_create(client, create_args).await,
        DeviceCommands::Delete(delete_args) => handle_delete(client, delete_args).await,
        DeviceCommands::IssueCertificate(issue_args) => {
            handle_issue_certificate(client, issue_args).await
        }
    }
}

async fn handle_list(
    client: &NanocloudClient,
    args: &DeviceListArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = namespace_option(&args.namespace);
    let list = client.list_devices(namespace).await?;
    print_json(&list)
}

async fn handle_get(
    client: &NanocloudClient,
    args: &DeviceGetArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = namespace_or_default(&args.namespace);
    let device = client.get_device(namespace, &args.name).await?;
    print_json(&device)
}

async fn handle_create(
    client: &NanocloudClient,
    args: &DeviceCreateArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = namespace_option(&args.namespace);
    let description = args
        .description
        .as_deref()
        .filter(|value| !value.is_empty());
    let device = client
        .create_device(namespace, &args.hash, description)
        .await?;
    print_json(&device)
}

async fn handle_delete(
    client: &NanocloudClient,
    args: &DeviceDeleteArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = namespace_or_default(&args.namespace);
    let device = client.delete_device(namespace, &args.name).await?;
    print_json(&device)
}

async fn handle_issue_certificate(
    client: &NanocloudClient,
    args: &DeviceIssueCertificateArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let csr = fs::read_to_string(&args.csr_path)?;
    let namespace = namespace_or_default(&args.namespace);
    let response = client
        .issue_device_certificate(namespace, csr.as_str())
        .await?;
    print_json(&response)
}

fn namespace_option(namespace: &Option<String>) -> Option<&str> {
    namespace.as_deref().filter(|value| !value.is_empty())
}

fn namespace_or_default(namespace: &Option<String>) -> &str {
    namespace_option(namespace).unwrap_or("default")
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn Error + Send + Sync>> {
    let body = serde_json::to_string_pretty(value)?;
    Terminal::stdout(format_args!("{body}"));
    Ok(())
}
