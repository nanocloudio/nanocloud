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

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::args::StatusArgs;
use crate::nanocloud::cli::curl::print_curl_request;
use crate::nanocloud::cli::output::{print_pod_state, print_pod_table, service_display_name};
use crate::nanocloud::cli::Terminal;

pub(super) async fn handle_status(
    client: &NanocloudClient,
    args: &StatusArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = args.namespace.as_deref();
    let namespace = namespace.filter(|value| !value.is_empty());
    if args.pods {
        if let Some(service) = args.service.as_deref() {
            return Err(format!(
                "cannot combine --pods with service '{}'; specify a namespace instead",
                service
            )
            .into());
        }
        if args.curl {
            let segments = NanocloudClient::pod_collection_segments(namespace);
            let mut url = client.url_from_segments(&segments)?;
            url.query_pairs_mut().append_pair("format", "table");
            print_curl_request(client, "GET", url.as_ref(), None)?;
            return Ok(());
        }
        let table = client.list_pods_table(namespace).await?;
        if table.rows.is_empty() {
            if let Some(ns) = namespace {
                Terminal::stdout(format_args!("No pods found in namespace '{}'.", ns));
            } else {
                Terminal::stdout(format_args!("No pods found."));
            }
            return Ok(());
        }
        print_pod_table(&table);
        return Ok(());
    }
    if args.curl {
        if let Some(service) = args.service.as_deref() {
            let pod_name = service_display_name(namespace, service);
            let segments = NanocloudClient::pod_segments(namespace, &pod_name);
            let url = client.url_from_segments(&segments)?.to_string();
            print_curl_request(client, "GET", &url, None)?;
        } else {
            let segments = NanocloudClient::pod_collection_segments(namespace);
            let mut url = client.url_from_segments(&segments)?;
            url.query_pairs_mut().append_pair("format", "table");
            print_curl_request(client, "GET", url.as_ref(), None)?;
        }
        return Ok(());
    }

    if let Some(service) = args.service.as_deref() {
        let pod_name = service_display_name(namespace, service);
        match client.get_pod(namespace, &pod_name).await? {
            Some(pod) => print_pod_state(namespace, service, &pod),
            None => {
                let target = service_display_name(namespace, service);
                Terminal::stderr(format_args!("Pod '{}' not found", target));
            }
        }
        return Ok(());
    }

    let table = client.list_pods_table(namespace).await?;
    if table.rows.is_empty() {
        if let Some(ns) = namespace {
            Terminal::stdout(format_args!("No pods found in namespace '{}'.", ns));
        } else {
            Terminal::stdout(format_args!("No pods found."));
        }
        return Ok(());
    }
    print_pod_table(&table);
    Ok(())
}
