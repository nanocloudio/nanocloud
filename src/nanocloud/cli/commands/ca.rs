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
use crate::nanocloud::api::types::CaRequest;
use crate::nanocloud::cli::args::CaArgs;
use crate::nanocloud::cli::curl::print_curl_request;
use crate::nanocloud::cli::Terminal;

pub(super) async fn handle_ca(
    client: &NanocloudClient,
    args: &CaArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let additional = args.additional.as_ref().map(|value| {
        value
            .split(',')
            .map(|item| item.trim().to_string())
            .collect::<Vec<_>>()
    });
    let additional = additional.filter(|items| !items.is_empty());

    if args.curl {
        let request = CaRequest {
            common_name: args.common_name.clone(),
            additional: additional.clone(),
        };
        let body = serde_json::to_string_pretty(&request)?;
        let url = client.url_from_segments(&["v1", "ca"])?.to_string();
        print_curl_request(client, "POST", &url, Some(&body))?;
        return Ok(());
    }

    let tls_info = client
        .issue_certificate(&args.common_name, additional)
        .await?;
    Terminal::stdout(format_args!("{}", serde_json::to_string_pretty(&tls_info)?));
    Ok(())
}
