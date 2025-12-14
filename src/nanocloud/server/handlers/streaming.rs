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

//! Shared streaming helper utilities for HTTP streaming endpoints.
//!
//! This module provides common utilities for streaming responses including:
//! - Backpressure handling via bounded channels
//! - Client disconnect detection
//! - Graceful shutdown coordination
//!
//! # Example
//!
//! ```ignore
//! use crate::nanocloud::server::handlers::streaming::{
//!     StreamConfig, StreamController, create_stream_channel
//! };
//!
//! // Create a streaming channel with default config
//! let (controller, body) = create_stream_channel(StreamConfig::default());
//!
//! // Spawn a producer task
//! tokio::spawn(async move {
//!     while let Some(data) = get_data().await {
//!         if controller.send(data).await.is_err() {
//!             break; // Client disconnected
//!         }
//!     }
//! });
//!
//! // Return the response body
//! Response::builder().body(body)?
//! ```

use std::io;
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;

use crate::nanocloud::logger::{log_debug, log_warn};

/// Default channel buffer size for streaming responses.
pub const DEFAULT_CHANNEL_BUFFER: usize = 64;

/// Default timeout for send operations when backpressure is applied.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for streaming channels.
///
/// This configuration provides fine-grained control over streaming endpoint
/// behavior including timeouts, backpressure, and duration limits.
///
/// # Configuration Knobs
///
/// - `buffer_size`: Controls how many items can queue before backpressure applies
/// - `send_timeout`: Maximum time to wait when the channel buffer is full
///
/// # Example
///
/// ```ignore
/// let config = StreamConfig::new("logs")
///     .with_buffer_size(128)
///     .with_send_timeout(Duration::from_secs(10));
/// ```
#[derive(Clone, Debug)]
pub struct StreamConfig {
    /// Size of the channel buffer. Larger buffers allow more data to queue
    /// before backpressure is applied.
    pub buffer_size: usize,
    /// Maximum time to wait when the channel is full before giving up.
    pub send_timeout: Duration,
    /// Log target for debugging messages.
    pub log_target: &'static str,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_CHANNEL_BUFFER,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            log_target: "streaming",
        }
    }
}

impl StreamConfig {
    /// Create a new config for a specific streaming endpoint.
    pub fn new(log_target: &'static str) -> Self {
        Self {
            log_target,
            ..Default::default()
        }
    }

    /// Set the buffer size.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the send timeout.
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }
}

/// Controller for sending data to a streaming response.
///
/// This provides a higher-level API over mpsc channels with:
/// - Timeout-based backpressure handling
/// - Client disconnect detection
/// - Logging for debugging
pub struct StreamController {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    config: StreamConfig,
    bytes_sent: u64,
}

impl StreamController {
    /// Create a new controller with the given sender and config.
    fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>, config: StreamConfig) -> Self {
        Self {
            sender,
            config,
            bytes_sent: 0,
        }
    }

    /// Send data to the client.
    ///
    /// Returns `Ok(())` if the data was sent successfully, or `Err(SendError)`
    /// if the client disconnected or the send timed out.
    pub async fn send(&mut self, data: Bytes) -> Result<(), SendError> {
        let len = data.len();
        match timeout(self.config.send_timeout, self.sender.send(Ok(data))).await {
            Ok(Ok(())) => {
                self.bytes_sent += len as u64;
                Ok(())
            }
            Ok(Err(_)) => {
                // Channel closed - client disconnected
                log_debug(
                    self.config.log_target,
                    "Stream client disconnected",
                    &[("bytes_sent", &self.bytes_sent.to_string())],
                );
                Err(SendError::ClientDisconnected)
            }
            Err(_) => {
                // Timeout - backpressure exceeded
                log_warn(
                    self.config.log_target,
                    "Stream send timeout (backpressure)",
                    &[
                        ("bytes_sent", &self.bytes_sent.to_string()),
                        (
                            "timeout_secs",
                            &self.config.send_timeout.as_secs().to_string(),
                        ),
                    ],
                );
                Err(SendError::Timeout)
            }
        }
    }

    /// Check if the client is still connected without sending data.
    pub fn is_connected(&self) -> bool {
        !self.sender.is_closed()
    }

    /// Returns the total bytes sent so far.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }
}

/// Error type for stream send operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// Client disconnected before the data could be sent.
    ClientDisconnected,
    /// Send operation timed out due to backpressure.
    Timeout,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::ClientDisconnected => write!(f, "client disconnected"),
            SendError::Timeout => write!(f, "send timeout"),
        }
    }
}

impl std::error::Error for SendError {}

/// Create a streaming channel with the given configuration.
///
/// Returns a controller for sending data and a Body for the HTTP response.
pub fn create_stream_channel(config: StreamConfig) -> (StreamController, Body) {
    let (tx, rx) = mpsc::channel(config.buffer_size);
    let controller = StreamController::new(tx, config);
    let body = Body::from_stream(ReceiverStream::new(rx));
    (controller, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_config_defaults() {
        let config = StreamConfig::default();
        assert_eq!(config.buffer_size, DEFAULT_CHANNEL_BUFFER);
        assert_eq!(config.send_timeout, DEFAULT_SEND_TIMEOUT);
        assert_eq!(config.log_target, "streaming");
    }

    #[test]
    fn stream_config_builder() {
        let config = StreamConfig::new("test")
            .with_buffer_size(128)
            .with_send_timeout(Duration::from_secs(60));

        assert_eq!(config.buffer_size, 128);
        assert_eq!(config.send_timeout, Duration::from_secs(60));
        assert_eq!(config.log_target, "test");
    }

    #[tokio::test]
    async fn stream_controller_sends_data() {
        let (mut controller, _body) = create_stream_channel(StreamConfig::default());

        let result = controller.send(Bytes::from("hello")).await;
        assert!(result.is_ok());
        assert_eq!(controller.bytes_sent(), 5);
    }

    #[tokio::test]
    async fn stream_controller_detects_disconnect() {
        let config = StreamConfig::new("test").with_buffer_size(1);
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let mut controller = StreamController::new(tx, config);

        drop(rx);

        let result = controller.send(Bytes::from("hello")).await;
        assert_eq!(result, Err(SendError::ClientDisconnected));
    }

    #[tokio::test]
    async fn stream_controller_is_connected() {
        let config = StreamConfig::new("test");
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let controller = StreamController::new(tx, config);

        assert!(controller.is_connected());

        drop(rx);

        assert!(!controller.is_connected());
    }

    #[tokio::test]
    async fn stream_controller_timeout_on_backpressure() {
        let config = StreamConfig::new("test")
            .with_buffer_size(1)
            .with_send_timeout(Duration::from_millis(10));
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let mut controller = StreamController::new(tx, config);

        controller.send(Bytes::from("1")).await.unwrap();

        let result = controller.send(Bytes::from("2")).await;
        assert_eq!(result, Err(SendError::Timeout));

        drop(rx);
    }

    #[test]
    fn send_error_display() {
        assert_eq!(
            SendError::ClientDisconnected.to_string(),
            "client disconnected"
        );
        assert_eq!(SendError::Timeout.to_string(), "send timeout");
    }

    #[tokio::test]
    async fn stream_controller_bytes_sent_accumulates() {
        let (mut controller, _body) = create_stream_channel(StreamConfig::default());

        controller.send(Bytes::from("hello")).await.unwrap();
        assert_eq!(controller.bytes_sent(), 5);

        controller.send(Bytes::from(" world")).await.unwrap();
        assert_eq!(controller.bytes_sent(), 11);
    }
}
