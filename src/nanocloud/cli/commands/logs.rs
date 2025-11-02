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
use crate::nanocloud::cli::args::LogsArgs;
use crate::nanocloud::cli::curl::print_curl_request;
use tokio_util::sync::CancellationToken;

use super::install::stream_logs_to_terminal;

pub(super) async fn handle_logs(
    client: &NanocloudClient,
    args: &LogsArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.curl {
        let namespace = args.namespace.as_deref();
        let log_segments = NanocloudClient::logs_segments(namespace, &args.service);
        let mut url = client.url_from_segments(&log_segments)?;
        if args.follow {
            url.query_pairs_mut().append_pair("follow", "true");
        }
        let url_string = url.to_string();
        print_curl_request(client, "GET", &url_string, None)?;
        return Ok(());
    }

    stream_logs_to_terminal(
        client.clone(),
        args.namespace.clone(),
        args.service.clone(),
        args.follow,
        CancellationToken::new(),
    )
    .await
}
