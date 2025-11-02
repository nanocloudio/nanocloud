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
use crate::nanocloud::cli::args::StartArgs;
use crate::nanocloud::cli::curl::print_curl_request;
use crate::nanocloud::cli::output::service_display_name;
use crate::nanocloud::cli::Terminal;

#[derive(Clone, Copy)]
pub(super) enum ServiceAction {
    Start,
    Stop,
    Restart,
}

pub(super) async fn handle_simple_action(
    client: &NanocloudClient,
    args: &StartArgs,
    action: ServiceAction,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = args.namespace.as_deref();
    let namespace = namespace.filter(|value| !value.is_empty());
    if args.curl {
        let ns = namespace.unwrap_or("default");
        let action_suffix = match action {
            ServiceAction::Start => "start",
            ServiceAction::Stop => "stop",
            ServiceAction::Restart => "restart",
        };
        let action_segment = format!("{}:{}", args.service, action_suffix);
        let url = client
            .url_from_segments(&[
                "apis",
                "nanocloud.io",
                "v1",
                "namespaces",
                ns,
                "bundles",
                action_segment.as_str(),
            ])?
            .to_string();
        print_curl_request(client, "POST", &url, None)?;
        return Ok(());
    }

    let display = service_display_name(namespace, &args.service);

    let response = match action {
        ServiceAction::Start => client.start_bundle(namespace, &args.service).await?,
        ServiceAction::Stop => client.stop_bundle(namespace, &args.service).await?,
        ServiceAction::Restart => client.restart_bundle(namespace, &args.service).await?,
    };

    Terminal::stdout(format_args!(
        "{} request accepted for {}",
        response.action.to_uppercase(),
        display
    ));

    Ok(())
}
