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

use std::env;
use std::error::Error;
use std::time::Duration;

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::args::LogsArgs;
use crate::nanocloud::cli::commands::{stream_logs_to_terminal, LogStreamConfig};
use crate::nanocloud::cli::curl::print_curl_request;
use tokio::signal;
use tokio_util::sync::CancellationToken;

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

    let cancel = CancellationToken::new();
    let cancel_token = cancel.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        cancel_token.cancel();
    });

    stream_logs_to_terminal(
        client.clone(),
        args.namespace.clone(),
        args.service.clone(),
        args.follow,
        cancel,
        log_stream_config_from_env(),
    )
    .await
}

fn log_stream_config_from_env() -> LogStreamConfig {
    let mut config = LogStreamConfig::default();
    if let Ok(value) = env::var("NANOCLOUD_LOG_BACKOFF_MS") {
        if let Ok(ms) = value.parse::<u64>() {
            config.retry_backoff = Duration::from_millis(ms.max(1));
        }
    }
    if let Ok(value) = env::var("NANOCLOUD_LOG_MAX_BACKOFF_MS") {
        if let Ok(ms) = value.parse::<u64>() {
            config.max_backoff = Duration::from_millis(ms.max(1));
        }
    }
    if let Ok(value) = env::var("NANOCLOUD_LOG_MAX_RETRIES") {
        if let Ok(limit) = value.parse::<u32>() {
            config.max_retries = Some(limit);
        }
    }
    if let Ok(value) = env::var("NANOCLOUD_LOG_DRAIN_MS") {
        if let Ok(ms) = value.parse::<u64>() {
            config.drain_timeout = Duration::from_millis(ms);
        }
    }
    if let Ok(value) = env::var("NANOCLOUD_LOG_FLUSH_MS") {
        if let Ok(ms) = value.parse::<u64>() {
            config.flush_interval = Duration::from_millis(ms.max(1));
        }
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn config_reads_env_with_defaults() {
        env::set_var("NANOCLOUD_LOG_BACKOFF_MS", "150");
        env::set_var("NANOCLOUD_LOG_MAX_BACKOFF_MS", "4000");
        env::set_var("NANOCLOUD_LOG_MAX_RETRIES", "3");
        env::set_var("NANOCLOUD_LOG_DRAIN_MS", "0");
        env::set_var("NANOCLOUD_LOG_FLUSH_MS", "50");

        let config = log_stream_config_from_env();
        assert_eq!(config.retry_backoff, Duration::from_millis(150));
        assert_eq!(config.max_backoff, Duration::from_millis(4000));
        assert_eq!(config.max_retries, Some(3));
        assert_eq!(config.drain_timeout, Duration::from_millis(0));
        assert_eq!(config.flush_interval, Duration::from_millis(50));

        env::remove_var("NANOCLOUD_LOG_BACKOFF_MS");
        env::remove_var("NANOCLOUD_LOG_MAX_BACKOFF_MS");
        env::remove_var("NANOCLOUD_LOG_MAX_RETRIES");
        env::remove_var("NANOCLOUD_LOG_DRAIN_MS");
        env::remove_var("NANOCLOUD_LOG_FLUSH_MS");
    }
}
