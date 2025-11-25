#![allow(dead_code)]

use super::exec::{
    parse_status_code, ExecChannelMultiplexer, CHANNEL_CLOSE, CHANNEL_RESIZE, CHANNEL_STATUS,
    CHANNEL_STDERR, CHANNEL_STDIN, CHANNEL_STDOUT,
};
use super::{ExecRequest, NanocloudClient};
use std::error::Error;
use std::io;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_tungstenite::WebSocketStream;

pub fn test_client() -> NanocloudClient {
    NanocloudClient::with_bearer("https://nanocloud.test", "test-token".into())
        .expect("construct test Nanocloud client")
}

pub fn build_exec_request(container: Option<&str>) -> ExecRequest {
    ExecRequest {
        namespace: "demo".to_string(),
        pod: "web-0".to_string(),
        container: container.map(|value| value.to_string()),
        command: vec!["sh".into(), "-c".into(), "echo hi".into()],
        stdin: true,
        stdout: true,
        stderr: false,
        tty: true,
    }
}

pub const CHANNEL_STDIN_ID: u8 = CHANNEL_STDIN;
pub const CHANNEL_STDOUT_ID: u8 = CHANNEL_STDOUT;
pub const CHANNEL_STDERR_ID: u8 = CHANNEL_STDERR;
pub const CHANNEL_STATUS_ID: u8 = CHANNEL_STATUS;
pub const CHANNEL_RESIZE_ID: u8 = CHANNEL_RESIZE;
pub const CHANNEL_CLOSE_ID: u8 = CHANNEL_CLOSE;

pub fn parse_status(payload: &[u8]) -> io::Result<Option<i32>> {
    parse_status_code(payload)
}

pub fn extract_exec_protocol_label(
    headers: &HeaderMap,
) -> Result<&'static str, Box<dyn Error + Send + Sync>> {
    super::exec::extract_exec_protocol_label(headers)
}

pub struct MultiplexerHarness {
    multiplexer: ExecChannelMultiplexer,
}

impl MultiplexerHarness {
    pub fn new(stream: WebSocketStream<DuplexStream>) -> Self {
        Self {
            multiplexer: ExecChannelMultiplexer::new_for_tests(stream),
        }
    }

    pub fn stdin_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.multiplexer.stdin_sender()
    }

    pub fn resize_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.multiplexer.resize_sender()
    }

    pub fn take_stdout(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.multiplexer.take_stdout()
    }

    pub fn take_stderr(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.multiplexer.take_stderr()
    }

    pub fn take_status(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.multiplexer.take_status()
    }

    pub fn take_close(&mut self) -> Option<mpsc::Receiver<()>> {
        self.multiplexer.take_close()
    }

    pub async fn shutdown(self) {
        self.multiplexer.shutdown().await;
    }
}
