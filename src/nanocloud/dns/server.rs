/*
 * Copyright (C) 2025 The Nanocloud Authors
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

use super::config::DnsConfig;
use super::resolver::{DnsQuestion, DnsRecord, DnsResolver, QueryType, Resolution, ResponseCode};
use super::DnsService;
use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::observability::{metrics, tracing as obs_tracing};
use crate::nanocloud::util::error::with_context;
use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    watch, Mutex, OwnedSemaphorePermit, Semaphore,
};
use tokio::task::JoinHandle;
use tokio::time::{error::Elapsed, sleep, timeout, Duration, Instant};
use trust_dns_proto::op::{Edns, Message, MessageType, OpCode, Query};
use trust_dns_proto::rr::rdata;
use trust_dns_proto::rr::{Name, RData, Record, RecordType};
use trust_dns_proto::serialize::binary::{BinDecodable, BinEncodable, BinEncoder};

const DNS_COMPONENT: &str = "dns";
const MAX_LISTENER_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct RateLimiter {
    limit_per_second: Option<u32>,
    burst: u32,
    buckets: Arc<Mutex<HashMap<IpAddr, TokenBucket>>>,
    max_entries: usize,
}

#[derive(Clone, Debug)]
struct TokenBucket {
    tokens: f32,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(limit_per_second: Option<u32>, burst: u32) -> Self {
        Self {
            limit_per_second,
            burst: burst.max(1),
            buckets: Arc::new(Mutex::new(HashMap::new())),
            max_entries: 1024,
        }
    }

    async fn allow(&self, addr: IpAddr) -> bool {
        let Some(limit) = self.limit_per_second else {
            return true;
        };
        let mut guard = self.buckets.lock().await;
        if guard.len() > self.max_entries {
            guard.clear();
        }
        let bucket = guard.entry(addr).or_insert_with(|| TokenBucket {
            tokens: self.burst as f32,
            last_refill: Instant::now(),
        });
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let refilled = (elapsed * limit as f64) as f32;
            bucket.tokens = (bucket.tokens + refilled).min(self.burst as f32);
            bucket.last_refill = now;
        }
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct BufferPool {
    tx: UnboundedSender<Vec<u8>>,
    rx: Arc<Mutex<UnboundedReceiver<Vec<u8>>>>,
    max_len: usize,
}

impl BufferPool {
    fn new(capacity: usize, max_len: usize) -> Self {
        let (tx, rx) = unbounded_channel();
        for _ in 0..capacity {
            let buf = vec![0; max_len];
            let _ = tx.send(buf);
        }
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            max_len,
        }
    }

    async fn lease(&self) -> BufferLease {
        let mut rx = self.rx.lock().await;
        let mut buf = rx.try_recv().unwrap_or_else(|_| vec![0; self.max_len]);
        buf.truncate(self.max_len);
        if buf.len() < self.max_len {
            buf.resize(self.max_len, 0);
        }
        BufferLease {
            buf: Some(buf),
            pool: self.clone(),
        }
    }

    fn recycle(&self, mut buf: Vec<u8>) {
        buf.truncate(self.max_len);
        if buf.len() < self.max_len {
            buf.resize(self.max_len, 0);
        }
        let _ = self.tx.send(buf);
    }
}

struct BufferLease {
    buf: Option<Vec<u8>>,
    pool: BufferPool,
}

impl BufferLease {
    fn truncate(&mut self, len: usize) {
        if let Some(buf) = self.buf.as_mut() {
            buf.truncate(len);
        }
    }
}

impl Deref for BufferLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf.as_deref().unwrap_or_default()
    }
}

impl DerefMut for BufferLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_deref_mut().unwrap_or(&mut [])
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.recycle(buf);
        }
    }
}

#[derive(Clone)]
struct Backoff {
    current: Duration,
    initial: Duration,
}

impl Backoff {
    fn new(initial_ms: u64) -> Self {
        let initial = Duration::from_millis(initial_ms.max(1));
        Self {
            current: initial,
            initial,
        }
    }

    async fn delay(&mut self) {
        sleep(self.current).await;
        self.current = (self.current * 2).min(MAX_LISTENER_BACKOFF);
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

fn mark_ready(ready_tx: &watch::Sender<bool>, state: &Arc<AtomicUsize>, bit: usize) {
    let prev = state.fetch_or(bit, Ordering::SeqCst);
    let combined = prev | bit;
    if combined == 0b11 {
        let _ = ready_tx.send(true);
    }
}

type ServerResult = Result<(), Box<dyn Error + Send + Sync>>;

#[allow(dead_code)]
pub struct DnsServerHandle {
    shutdown_tx: watch::Sender<bool>,
    ready_rx: watch::Receiver<bool>,
    udp: JoinHandle<ServerResult>,
    tcp: JoinHandle<ServerResult>,
}

#[allow(dead_code)]
impl DnsServerHandle {
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn wait_ready(&mut self) {
        if *self.ready_rx.borrow() {
            return;
        }
        let _ = self.ready_rx.changed().await;
    }

    pub async fn wait(self) -> ServerResult {
        let _ = self.shutdown_tx.send(true);
        let udp = self
            .udp
            .await
            .map_err(|e| with_context(e, "DNS UDP task join failed"))?;
        let tcp = self
            .tcp
            .await
            .map_err(|e| with_context(e, "DNS TCP task join failed"))?;
        udp?;
        tcp?;
        Ok(())
    }
}

pub async fn start(
    service: Arc<DnsService>,
) -> Result<DnsServerHandle, Box<dyn Error + Send + Sync>> {
    let config = service.config().clone();
    let resolver = service.resolver();
    let bind_addr = SocketAddr::new(config.listen_address, config.listen_port);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = watch::channel(false);
    let ready_state = Arc::new(AtomicUsize::new(0));
    let handler_limit = if config.handler_concurrency > 0 {
        Some(Arc::new(Semaphore::new(config.handler_concurrency)))
    } else {
        None
    };
    let rate_limiter = Arc::new(RateLimiter::new(
        config.rate_limit_per_second,
        config.rate_limit_burst,
    ));
    let buffer_pool = Arc::new(BufferPool::new(
        config.buffer_pool_size,
        config.max_udp_payload_size as usize,
    ));

    log_info(
        DNS_COMPONENT,
        "DNS listeners starting",
        &[
            ("addr", bind_addr.to_string().as_str()),
            ("cluster_domain", config.cluster_domain.as_str()),
        ],
    );

    let udp_handle = tokio::spawn(run_udp(
        bind_addr,
        shutdown_rx.clone(),
        config.clone(),
        resolver.clone(),
        handler_limit.clone(),
        rate_limiter.clone(),
        buffer_pool,
        ready_tx.clone(),
        Arc::clone(&ready_state),
    ));
    let tcp_handle = tokio::spawn(run_tcp(
        bind_addr,
        shutdown_rx,
        config,
        resolver,
        handler_limit,
        rate_limiter,
        ready_tx,
        ready_state,
    ));

    Ok(DnsServerHandle {
        shutdown_tx,
        ready_rx,
        udp: udp_handle,
        tcp: tcp_handle,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_udp(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    config: DnsConfig,
    resolver: DnsResolver,
    handler_limit: Option<Arc<Semaphore>>,
    rate_limiter: Arc<RateLimiter>,
    buffer_pool: Arc<BufferPool>,
    ready_tx: watch::Sender<bool>,
    ready_state: Arc<AtomicUsize>,
) -> ServerResult {
    let mut backoff = Backoff::new(config.listener_backoff_ms);
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        match UdpSocket::bind(bind_addr).await {
            Ok(socket) => {
                log_info(
                    DNS_COMPONENT,
                    "UDP listener bound",
                    &[("addr", bind_addr.to_string().as_str())],
                );
                mark_ready(&ready_tx, &ready_state, 0b01);
                backoff.reset();
                let socket = Arc::new(socket);
                let result = run_udp_loop(
                    socket,
                    &mut shutdown_rx,
                    &config,
                    resolver.clone(),
                    handler_limit.clone(),
                    rate_limiter.clone(),
                    buffer_pool.clone(),
                )
                .await;
                if *shutdown_rx.borrow() {
                    break;
                }
                if let Err(err) = result {
                    log_error(
                        DNS_COMPONENT,
                        "UDP listener failed",
                        &[("error", err.to_string().as_str())],
                    );
                    backoff.delay().await;
                    continue;
                }
            }
            Err(err) => {
                log_error(
                    DNS_COMPONENT,
                    "UDP bind failed",
                    &[("error", err.to_string().as_str())],
                );
                backoff.delay().await;
            }
        }
    }
    Ok(())
}

async fn run_udp_loop(
    socket: Arc<UdpSocket>,
    shutdown_rx: &mut watch::Receiver<bool>,
    config: &DnsConfig,
    resolver: DnsResolver,
    handler_limit: Option<Arc<Semaphore>>,
    rate_limiter: Arc<RateLimiter>,
    buffer_pool: Arc<BufferPool>,
) -> ServerResult {
    let max_len = config.max_udp_payload_size as usize;
    loop {
        let mut lease = buffer_pool.lease().await;
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            recv = socket.recv_from(&mut lease) => {
                match recv {
                    Ok((len, peer)) => {
                        let truncated = len.min(max_len);
                        lease.truncate(truncated);
                        if truncated < config.min_dns_packet_len {
                            metrics::record_dns_response("FORMERR");
                            metrics::record_dns_drop("too_short");
                            log_warn(
                                DNS_COMPONENT,
                                "Dropping undersized DNS packet",
                                &[("len", truncated.to_string().as_str())],
                            );
                            continue;
                        }
                        if !rate_limiter.allow(peer.ip()).await {
                            metrics::record_dns_drop("rate_limit");
                            log_warn(
                                DNS_COMPONENT,
                                "DNS query rate limited",
                                &[("peer", peer.ip().to_string().as_str())],
                            );
                            continue;
                        }
                        let permit = handler_limit
                            .as_ref()
                            .and_then(|limit| limit.clone().try_acquire_owned().ok());
                        if handler_limit.is_some() && permit.is_none() {
                            metrics::record_dns_drop("saturated");
                            log_warn(
                                DNS_COMPONENT,
                                "Dropping DNS query due to handler saturation",
                                &[("peer", peer.ip().to_string().as_str())],
                            );
                            continue;
                        }
                        let socket = Arc::clone(&socket);
                        let config = config.clone();
                        let resolver = resolver.clone();
                        tokio::spawn(async move {
                            let _permit: Option<OwnedSemaphorePermit> = permit;
                            if let Err(err) =
                                handle_udp_query(socket, lease, peer, config, resolver).await
                            {
                                log_warn(
                                    DNS_COMPONENT,
                                    "Failed processing UDP query",
                                    &[("error", err.to_string().as_str())],
                                );
                            }
                        });
                    }
                    Err(err) => {
                        return Err(with_context(err, "UDP listener receive failed"));
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_tcp(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    config: DnsConfig,
    resolver: DnsResolver,
    handler_limit: Option<Arc<Semaphore>>,
    rate_limiter: Arc<RateLimiter>,
    ready_tx: watch::Sender<bool>,
    ready_state: Arc<AtomicUsize>,
) -> ServerResult {
    let mut backoff = Backoff::new(config.listener_backoff_ms);
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        match TcpListener::bind(bind_addr).await {
            Ok(listener) => {
                log_info(
                    DNS_COMPONENT,
                    "TCP listener bound",
                    &[("addr", bind_addr.to_string().as_str())],
                );
                mark_ready(&ready_tx, &ready_state, 0b10);
                backoff.reset();
                if let Err(err) = run_tcp_loop(
                    listener,
                    &mut shutdown_rx,
                    &config,
                    resolver.clone(),
                    handler_limit.clone(),
                    rate_limiter.clone(),
                )
                .await
                {
                    log_error(
                        DNS_COMPONENT,
                        "TCP listener failed",
                        &[("error", err.to_string().as_str())],
                    );
                    backoff.delay().await;
                }
            }
            Err(err) => {
                log_error(
                    DNS_COMPONENT,
                    "TCP bind failed",
                    &[("error", err.to_string().as_str())],
                );
                backoff.delay().await;
            }
        }
    }
    Ok(())
}

async fn run_tcp_loop(
    listener: TcpListener,
    shutdown_rx: &mut watch::Receiver<bool>,
    config: &DnsConfig,
    resolver: DnsResolver,
    handler_limit: Option<Arc<Semaphore>>,
    rate_limiter: Arc<RateLimiter>,
) -> ServerResult {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        if !rate_limiter.allow(peer.ip()).await {
                            log_warn(
                                DNS_COMPONENT,
                                "DNS TCP peer rate limited",
                                &[("peer", peer.ip().to_string().as_str())],
                            );
                            continue;
                        }
                        let permit = handler_limit
                            .as_ref()
                            .and_then(|limit| limit.clone().try_acquire_owned().ok());
                        if handler_limit.is_some() && permit.is_none() {
                            metrics::record_dns_drop("saturated");
                            log_warn(
                                DNS_COMPONENT,
                                "Dropping DNS TCP session due to handler saturation",
                                &[("peer", peer.ip().to_string().as_str())],
                            );
                            continue;
                        }
                        let resolver = resolver.clone();
                        let config = config.clone();
                        let mut shutdown = shutdown_rx.clone();
                        let limiter = rate_limiter.clone();
                        tokio::spawn(async move {
                            let _permit: Option<OwnedSemaphorePermit> = permit;
                            if let Err(err) = handle_tcp_stream(
                                stream,
                                peer,
                                &mut shutdown,
                                limiter,
                                config,
                                resolver,
                            )
                            .await
                            {
                                log_warn(
                                    DNS_COMPONENT,
                                    "Failed processing TCP query",
                                    &[("error", err.to_string().as_str())],
                                );
                            }
                        });
                    }
                    Err(err) => {
                        return Err(with_context(err, "TCP listener accept failed"));
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_udp_query(
    socket: Arc<UdpSocket>,
    data: BufferLease,
    peer: SocketAddr,
    config: DnsConfig,
    resolver: DnsResolver,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let response = obs_tracing::with_span(DNS_COMPONENT, "udp_query", async {
        process_query(&data, &config, &resolver, Transport::Udp).await
    })
    .await?;
    if !response.is_empty() {
        let _ = socket.send_to(&response, peer).await;
    }
    Ok(())
}

async fn handle_tcp_stream(
    mut stream: TcpStream,
    peer: SocketAddr,
    shutdown_rx: &mut watch::Receiver<bool>,
    rate_limiter: Arc<RateLimiter>,
    config: DnsConfig,
    resolver: DnsResolver,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            res = stream.readable() => {
                if res.is_err() {
                    break;
                }
                let mut len_buf = [0u8; 2];
                if let Err(err) = stream.try_read(&mut len_buf) {
                    if err.kind() == std::io::ErrorKind::WouldBlock {
                        continue;
                    }
                    break;
                }
                let expected = u16::from_be_bytes(len_buf) as usize;
                if expected < config.min_dns_packet_len {
                    metrics::record_dns_response("FORMERR");
                    metrics::record_dns_drop("too_short");
                    log_warn(
                        DNS_COMPONENT,
                        "Dropping undersized TCP DNS packet",
                        &[("len", expected.to_string().as_str())],
                    );
                    continue;
                }
                if !rate_limiter.allow(peer.ip()).await {
                    metrics::record_dns_drop("rate_limit");
                    log_warn(
                        DNS_COMPONENT,
                        "DNS TCP peer rate limited",
                        &[("peer", peer.ip().to_string().as_str())],
                    );
                    continue;
                }
                let mut buf = vec![0u8; expected];
                stream.read_exact(&mut buf).await?;
                let response = obs_tracing::with_span(DNS_COMPONENT, "tcp_query", async {
                    process_query(&buf, &config, &resolver, Transport::Tcp).await
                })
                .await?;
                if !response.is_empty() {
                    let len = (response.len() as u16).to_be_bytes();
                    stream.write_all(&len).await?;
                    stream.write_all(&response).await?;
                }
            }
        }
    }
    log_info(
        DNS_COMPONENT,
        "Closed DNS TCP session",
        &[("peer", peer.to_string().as_str())],
    );
    Ok(())
}

#[derive(Copy, Clone)]
enum Transport {
    Udp,
    Tcp,
}

async fn process_query(
    data: &[u8],
    config: &DnsConfig,
    resolver: &DnsResolver,
    transport: Transport,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    if data.len() < config.min_dns_packet_len {
        metrics::record_dns_response("FORMERR");
        metrics::record_dns_drop("too_short");
        log_warn(
            DNS_COMPONENT,
            "Received DNS packet below minimum length",
            &[("len", data.len().to_string().as_str())],
        );
        return Ok(Vec::new());
    }

    let message = match Message::from_bytes(data) {
        Ok(msg) => msg,
        Err(err) => {
            metrics::record_dns_response("FORMERR");
            metrics::record_dns_drop("malformed");
            log_warn(
                DNS_COMPONENT,
                "Received malformed DNS message",
                &[("error", err.to_string().as_str())],
            );
            return Ok(vec![]);
        }
    };

    let id = message.id();
    let opcode = message.op_code();
    let question = match message.queries().first() {
        Some(q) => q,
        None => {
            metrics::record_dns_response("FORMERR");
            metrics::record_dns_drop("malformed");
            return Ok(Vec::new());
        }
    };

    let qtype_text = question.query_type().to_string();
    metrics::record_dns_query(&qtype_text);
    let name = question.name().to_ascii();
    let query = DnsQuestion {
        name: name.clone(),
        qtype: map_query_type(question.query_type()),
    };

    let resolution = resolver.resolve(&query);
    match resolution {
        Resolution::Answer(answer) => {
            metrics::record_dns_response(map_response_code(&answer.code));
            build_response(
                id,
                opcode,
                question,
                &answer.code,
                &answer.answers,
                &answer.authorities,
                &answer.additionals,
                config,
            )
        }
        Resolution::Forward(upstreams) => {
            let forwarded = forward_query(upstreams, data, transport, config).await?;
            if forwarded.is_empty() {
                metrics::record_dns_response("SERVFAIL");
                build_response(
                    id,
                    opcode,
                    question,
                    &ResponseCode::ServFail,
                    &[],
                    &[],
                    &[],
                    config,
                )
            } else {
                Ok(forwarded)
            }
        }
    }
}

async fn forward_query(
    upstreams: Vec<SocketAddr>,
    data: &[u8],
    transport: Transport,
    config: &DnsConfig,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let attempts = config.upstream_retries.saturating_add(1);
    for upstream in upstreams {
        let mut backoff = Backoff::new(config.listener_backoff_ms);
        for attempt in 0..attempts {
            let result = match transport {
                Transport::Udp => forward_udp(upstream, data, config.upstream_timeout).await,
                Transport::Tcp => forward_tcp(upstream, data, config.upstream_timeout).await,
            };
            match result {
                Ok(buf) if !buf.is_empty() => {
                    metrics::record_dns_upstream_attempt("success");
                    if let Ok(msg) = Message::from_bytes(&buf) {
                        let code = msg.response_code().to_string();
                        metrics::record_dns_response(code.as_str());
                    }
                    return Ok(buf);
                }
                Ok(_) => {
                    metrics::record_dns_upstream_attempt("empty");
                    log_warn(
                        DNS_COMPONENT,
                        "Upstream returned empty DNS response",
                        &[("upstream", upstream.to_string().as_str())],
                    );
                }
                Err(err) => {
                    let outcome = if err.downcast_ref::<Elapsed>().is_some() {
                        "timeout"
                    } else {
                        "error"
                    };
                    metrics::record_dns_upstream_attempt(outcome);
                    log_warn(
                        DNS_COMPONENT,
                        "Upstream DNS query failed",
                        &[
                            ("upstream", upstream.to_string().as_str()),
                            ("attempt", (attempt + 1).to_string().as_str()),
                            ("error", err.to_string().as_str()),
                        ],
                    );
                }
            }
            if attempt + 1 < attempts {
                backoff.delay().await;
            }
        }
    }
    metrics::record_dns_response("SERVFAIL");
    Ok(Vec::new())
}

async fn forward_udp(
    upstream: SocketAddr,
    data: &[u8],
    timeout_duration: Duration,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let bind_addr = SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0);
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.send_to(data, upstream).await?;
    let mut buf = vec![0u8; 4096];
    let recv_result = timeout(timeout_duration, socket.recv_from(&mut buf)).await?;
    let (len, _) = recv_result?;
    buf.truncate(len);
    Ok(buf)
}

async fn forward_tcp(
    upstream: SocketAddr,
    data: &[u8],
    timeout_duration: Duration,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let stream = timeout(timeout_duration, TcpStream::connect(upstream)).await?;
    let mut stream = stream?;
    let len = (data.len() as u16).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data).await?;
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let expected = u16::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; expected];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[allow(clippy::too_many_arguments)]
fn build_response(
    id: u16,
    opcode: OpCode,
    query: &Query,
    code: &ResponseCode,
    answers: &[DnsRecord],
    authorities: &[DnsRecord],
    additionals: &[DnsRecord],
    config: &DnsConfig,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut message = Message::new();
    message
        .set_id(id)
        .set_message_type(MessageType::Response)
        .set_op_code(opcode)
        .set_response_code(map_proto_response_code(code))
        .set_authoritative(true)
        .set_recursion_available(false)
        .add_query(query.clone());

    for record in answers {
        message.add_answer(to_proto_record(record)?);
    }
    for record in authorities {
        message.add_name_server(to_proto_record(record)?);
    }
    for record in additionals {
        message.add_additional(to_proto_record(record)?);
    }

    let mut edns = Edns::new();
    edns.set_max_payload(config.max_udp_payload_size);
    message.set_edns(edns);

    let mut bytes = Vec::new();
    let mut encoder = BinEncoder::new(&mut bytes);
    message.emit(&mut encoder)?;
    Ok(bytes)
}

fn to_proto_record(record: &DnsRecord) -> Result<Record, Box<dyn Error + Send + Sync>> {
    match record {
        DnsRecord::A { name, address, ttl } => Ok(Record::from_rdata(
            Name::from_ascii(name)?,
            *ttl,
            RData::A(rdata::A::from(*address)),
        )),
        DnsRecord::AAAA { name, address, ttl } => Ok(Record::from_rdata(
            Name::from_ascii(name)?,
            *ttl,
            RData::AAAA(rdata::AAAA::from(*address)),
        )),
        DnsRecord::Srv {
            name,
            priority,
            weight,
            port,
            target,
            ttl,
        } => Ok(Record::from_rdata(
            Name::from_ascii(name)?,
            *ttl,
            RData::SRV(trust_dns_proto::rr::rdata::srv::SRV::new(
                *priority,
                *weight,
                *port,
                Name::from_ascii(target)?,
            )),
        )),
        DnsRecord::Ns { name, host, ttl } => Ok(Record::from_rdata(
            Name::from_ascii(name)?,
            *ttl,
            RData::NS(rdata::NS(Name::from_ascii(host)?)),
        )),
        DnsRecord::Soa {
            name,
            mname,
            rname,
            serial,
            refresh,
            retry,
            expire,
            minimum,
            ttl,
        } => Ok(Record::from_rdata(
            Name::from_ascii(name)?,
            *ttl,
            RData::SOA(rdata::SOA::new(
                Name::from_ascii(mname)?,
                Name::from_ascii(rname)?,
                *serial,
                i32::try_from(*refresh).unwrap_or(i32::MAX),
                i32::try_from(*retry).unwrap_or(i32::MAX),
                i32::try_from(*expire).unwrap_or(i32::MAX),
                *minimum,
            )),
        )),
    }
}

fn map_query_type(record_type: RecordType) -> QueryType {
    match record_type {
        RecordType::A => QueryType::A,
        RecordType::AAAA => QueryType::AAAA,
        RecordType::SRV => QueryType::SRV,
        RecordType::NS => QueryType::NS,
        RecordType::SOA => QueryType::SOA,
        other => QueryType::Other(other.into()),
    }
}

fn map_response_code(code: &ResponseCode) -> &'static str {
    match code {
        ResponseCode::NoError => "NOERROR",
        ResponseCode::NxDomain => "NXDOMAIN",
        ResponseCode::Refused => "REFUSED",
        ResponseCode::ServFail => "SERVFAIL",
    }
}

fn map_proto_response_code(code: &ResponseCode) -> trust_dns_proto::op::ResponseCode {
    match code {
        ResponseCode::NoError => trust_dns_proto::op::ResponseCode::NoError,
        ResponseCode::NxDomain => trust_dns_proto::op::ResponseCode::NXDomain,
        ResponseCode::Refused => trust_dns_proto::op::ResponseCode::Refused,
        ResponseCode::ServFail => trust_dns_proto::op::ResponseCode::ServFail,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    use crate::nanocloud::dns::registry::DnsRegistry;
    use crate::nanocloud::observability::metrics;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn build_query_bytes(name: &str) -> Vec<u8> {
        let mut message = Message::new();
        message.add_query(Query::query(
            Name::from_ascii(name).expect("query name"),
            RecordType::A,
        ));
        let mut bytes = Vec::new();
        let mut encoder = BinEncoder::new(&mut bytes);
        message.emit(&mut encoder).expect("encode dns query");
        bytes
    }

    #[tokio::test]
    async fn rate_limit_records_drop_metric() {
        let mut config = DnsConfig::default();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.listen_port = 0;
        config.rate_limit_per_second = Some(1);
        config.rate_limit_burst = 1;

        let resolver = DnsResolver::new(config.clone(), Arc::new(DnsRegistry::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut shutdown_rx = shutdown_rx;
        let socket = Arc::new(
            UdpSocket::bind((config.listen_address, 0))
                .await
                .expect("bind udp listener"),
        );
        let buffer_pool = Arc::new(BufferPool::new(
            config.buffer_pool_size,
            config.max_udp_payload_size as usize,
        ));
        let rate_limiter = Arc::new(RateLimiter::new(
            config.rate_limit_per_second,
            config.rate_limit_burst,
        ));

        let server_addr = socket.local_addr().expect("addr");
        let udp_task = {
            let socket = Arc::clone(&socket);
            let config = config.clone();
            let resolver = resolver.clone();
            let rate_limiter = rate_limiter.clone();
            let buffer_pool = buffer_pool.clone();
            tokio::spawn(async move {
                run_udp_loop(
                    socket,
                    &mut shutdown_rx,
                    &config,
                    resolver,
                    None,
                    rate_limiter,
                    buffer_pool,
                )
                .await
            })
        };

        let client = UdpSocket::bind((config.listen_address, 0))
            .await
            .expect("bind client");
        let query = build_query_bytes("example.com.");

        client
            .send_to(&query, server_addr)
            .await
            .expect("send first");
        client
            .send_to(&query, server_addr)
            .await
            .expect("send second");
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = shutdown_tx.send(true);
        udp_task.await.expect("udp loop join").expect("udp loop ok");

        let metrics_text =
            String::from_utf8(metrics::gather().expect("metrics")).expect("utf8 metrics");
        assert!(
            metrics_text.contains("nanocloud_dns_drops_total")
                && metrics_text.contains("reason=\"rate_limit\""),
            "expected rate_limit drop metric, got:\n{}",
            metrics_text
        );
    }

    #[tokio::test]
    async fn upstream_retry_records_attempts() {
        let upstream_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind upstream");
        let upstream_addr = upstream_socket.local_addr().expect("upstream addr");
        let received = Arc::new(AtomicUsize::new(0));
        let listener = {
            let received = Arc::clone(&received);
            tokio::spawn(async move {
                loop {
                    let mut buf = [0u8; 512];
                    let Ok((len, peer)) = upstream_socket.recv_from(&mut buf).await else {
                        break;
                    };
                    let count = received.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 2 {
                        let mut message =
                            Message::from_bytes(&buf[..len]).expect("decode upstream query");
                        message.set_message_type(MessageType::Response);
                        message.set_response_code(trust_dns_proto::op::ResponseCode::NoError);
                        let mut bytes = Vec::new();
                        let mut encoder = BinEncoder::new(&mut bytes);
                        message
                            .emit(&mut encoder)
                            .expect("encode upstream response");
                        let _ = upstream_socket.send_to(&bytes, peer).await;
                        break;
                    }
                }
            })
        };

        let mut config = DnsConfig::default();
        config.upstream_servers = vec![upstream_addr];
        config.upstream_timeout = Duration::from_millis(50);
        config.upstream_retries = 1;
        config.listener_backoff_ms = 10;
        let query = build_query_bytes("example.com.");

        let response = forward_query(vec![upstream_addr], &query, Transport::Udp, &config)
            .await
            .unwrap();
        listener.await.expect("upstream task");
        assert!(
            !response.is_empty(),
            "expected upstream retry to succeed on second attempt"
        );

        let metrics_text =
            String::from_utf8(metrics::gather().expect("metrics")).expect("utf8 metrics");
        assert!(
            metrics_text.contains("nanocloud_dns_upstream_attempts_total")
                && metrics_text.contains("outcome=\"timeout\"")
                && metrics_text.contains("outcome=\"success\""),
            "expected upstream attempt metrics, got:\n{}",
            metrics_text
        );
    }

    #[tokio::test]
    async fn udp_and_tcp_queries_succeed() {
        let mut config = DnsConfig::default();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.listen_port = 0;
        let registry = Arc::new(DnsRegistry::new());
        registry
            .register_service(crate::nanocloud::dns::registry::ServiceDescription {
                name: "svc".to_string(),
                namespace: "default".to_string(),
                cluster_ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42))),
                ports: Vec::new(),
                ttl_seconds: Some(30),
            })
            .unwrap();
        let resolver = DnsResolver::new(config.clone(), Arc::clone(&registry));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let udp_socket = Arc::new(
            UdpSocket::bind((config.listen_address, 0))
                .await
                .expect("bind udp listener"),
        );
        let port = udp_socket.local_addr().unwrap().port();
        let tcp_listener = TcpListener::bind((config.listen_address, port))
            .await
            .expect("bind tcp listener");
        let buffer_pool = Arc::new(BufferPool::new(
            config.buffer_pool_size,
            config.max_udp_payload_size as usize,
        ));
        let rate_limiter = Arc::new(RateLimiter::new(
            config.rate_limit_per_second,
            config.rate_limit_burst,
        ));
        let listen_addr = config.listen_address;

        let udp_task = {
            let socket = Arc::clone(&udp_socket);
            let config = config.clone();
            let resolver = resolver.clone();
            let rate_limiter = rate_limiter.clone();
            let buffer_pool = buffer_pool.clone();
            let mut shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                run_udp_loop(
                    socket,
                    &mut shutdown,
                    &config,
                    resolver,
                    None,
                    rate_limiter,
                    buffer_pool,
                )
                .await
            })
        };

        let tcp_task = {
            let resolver = resolver.clone();
            let rate_limiter = rate_limiter.clone();
            let mut shutdown = shutdown_rx.clone();
            let config = config.clone();
            tokio::spawn(async move {
                run_tcp_loop(
                    tcp_listener,
                    &mut shutdown,
                    &config,
                    resolver,
                    None,
                    rate_limiter,
                )
                .await
            })
        };

        let udp_query = build_query_bytes("svc.default.svc.cluster.local.");
        let mut udp_client = vec![0u8; 512];
        let client = UdpSocket::bind((listen_addr, 0))
            .await
            .expect("bind udp client");
        client
            .send_to(&udp_query, (listen_addr, port))
            .await
            .expect("send udp query");
        let (len, _) = client.recv_from(&mut udp_client).await.expect("recv udp");
        let udp_resp = Message::from_bytes(&udp_client[..len]).expect("decode udp");
        match udp_resp.answers()[0].data() {
            Some(RData::A(a)) => assert_eq!(a.0, Ipv4Addr::new(10, 0, 0, 42)),
            other => panic!("unexpected UDP answer: {:?}", other),
        }

        let mut tcp_stream = TcpStream::connect((config.listen_address, port))
            .await
            .expect("connect tcp");
        let tcp_query = build_query_bytes("svc.default.svc.cluster.local.");
        let len_bytes = (tcp_query.len() as u16).to_be_bytes();
        tcp_stream.write_all(&len_bytes).await.expect("write len");
        tcp_stream.write_all(&tcp_query).await.expect("write query");
        let mut len_buf = [0u8; 2];
        tcp_stream.read_exact(&mut len_buf).await.expect("read len");
        let expected = u16::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; expected];
        tcp_stream.read_exact(&mut buf).await.expect("read resp");
        let tcp_resp = Message::from_bytes(&buf).expect("decode tcp");
        match tcp_resp.answers()[0].data() {
            Some(RData::A(a)) => assert_eq!(a.0, Ipv4Addr::new(10, 0, 0, 42)),
            other => panic!("unexpected TCP answer: {:?}", other),
        }

        let _ = shutdown_tx.send(true);
        udp_task.await.expect("udp loop").expect("udp ok");
        tcp_task.await.expect("tcp loop").expect("tcp ok");
    }

    #[tokio::test]
    async fn malformed_packet_records_drop() {
        let mut config = DnsConfig::default();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.listen_port = 0;
        let resolver = DnsResolver::new(config.clone(), Arc::new(DnsRegistry::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let socket = Arc::new(
            UdpSocket::bind((config.listen_address, 0))
                .await
                .expect("bind udp listener"),
        );
        let buffer_pool = Arc::new(BufferPool::new(
            config.buffer_pool_size,
            config.max_udp_payload_size as usize,
        ));
        let rate_limiter = Arc::new(RateLimiter::new(
            config.rate_limit_per_second,
            config.rate_limit_burst,
        ));
        let udp_task = {
            let socket = Arc::clone(&socket);
            let config = config.clone();
            let resolver = resolver.clone();
            let rate_limiter = rate_limiter.clone();
            let buffer_pool = buffer_pool.clone();
            let mut shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                run_udp_loop(
                    socket,
                    &mut shutdown,
                    &config,
                    resolver,
                    None,
                    rate_limiter,
                    buffer_pool,
                )
                .await
            })
        };

        let peer = socket.local_addr().unwrap();
        let _ = UdpSocket::bind((config.listen_address, 0))
            .await
            .unwrap()
            .send_to(&[0u8; 4], peer)
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = shutdown_tx.send(true);
        udp_task.await.expect("udp task").expect("udp ok");

        let metrics_text =
            String::from_utf8(metrics::gather().expect("metrics")).expect("utf8 metrics");
        assert!(
            metrics_text.contains("nanocloud_dns_drops_total")
                && metrics_text.contains("reason=\"too_short\""),
            "expected too_short drop metric, got:\n{}",
            metrics_text
        );
    }

    #[tokio::test]
    async fn upstream_nxdomain_and_timeout() {
        let upstream_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind upstream");
        let upstream_addr = upstream_socket.local_addr().expect("addr");
        let listener = tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let Ok((len, peer)) = upstream_socket.recv_from(&mut buf).await else {
                return;
            };
            let mut message = Message::from_bytes(&buf[..len]).expect("decode");
            message.set_message_type(MessageType::Response);
            message.set_response_code(trust_dns_proto::op::ResponseCode::NXDomain);
            let mut bytes = Vec::new();
            let mut encoder = BinEncoder::new(&mut bytes);
            message.emit(&mut encoder).expect("encode");
            let _ = upstream_socket.send_to(&bytes, peer).await;
        });

        let mut config = DnsConfig::default();
        config.upstream_servers = vec![upstream_addr];
        config.upstream_timeout = Duration::from_millis(20);
        let query = build_query_bytes("example.com.");
        let response = forward_query(vec![upstream_addr], &query, Transport::Udp, &config)
            .await
            .unwrap();
        listener.await.expect("upstream join");
        let parsed = Message::from_bytes(&response).expect("decode response");
        assert_eq!(
            parsed.response_code(),
            trust_dns_proto::op::ResponseCode::NXDomain
        );

        let mut config_fail = config.clone();
        config_fail.upstream_timeout = Duration::from_millis(5);
        let empty = forward_query(vec![upstream_addr], &query, Transport::Udp, &config_fail)
            .await
            .unwrap();
        assert!(empty.is_empty(), "expected SERVFAIL on timeout");
    }

    #[tokio::test]
    async fn server_shutdown_and_restart() {
        let mut config = DnsConfig::default();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.listen_port = 0;
        let service = Arc::new(DnsService::new(config.clone()));

        let mut first = start(Arc::clone(&service)).await.expect("start server");
        first.wait_ready().await;
        first.shutdown();
        first.wait().await.expect("shutdown first");

        let mut second = start(service).await.expect("restart server");
        second.wait_ready().await;
        second.shutdown();
        second.wait().await.expect("shutdown second");
    }
}
