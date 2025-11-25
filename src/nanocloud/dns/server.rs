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
use crate::nanocloud::observability::metrics;
use crate::nanocloud::util::error::with_context;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use trust_dns_proto::op::{Edns, Message, MessageType, OpCode, Query};
use trust_dns_proto::rr::rdata;
use trust_dns_proto::rr::{Name, RData, Record, RecordType};
use trust_dns_proto::serialize::binary::{BinDecodable, BinEncodable, BinEncoder};

const DNS_COMPONENT: &str = "dns";
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(2);

pub struct DnsServerHandle {
    _udp: JoinHandle<()>,
    _tcp: JoinHandle<()>,
}

pub async fn start(
    service: Arc<DnsService>,
) -> Result<DnsServerHandle, Box<dyn Error + Send + Sync>> {
    let config = service.config().clone();
    let resolver = service.resolver();
    let bind_addr = SocketAddr::new(config.listen_address, config.listen_port);

    let udp_socket = UdpSocket::bind(bind_addr)
        .await
        .map_err(|e| with_context(e, format!("Failed to bind DNS UDP socket at {bind_addr}")))?;
    let tcp_listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| with_context(e, format!("Failed to bind DNS TCP listener at {bind_addr}")))?;

    log_info(
        DNS_COMPONENT,
        "DNS listeners started",
        &[
            ("addr", bind_addr.to_string().as_str()),
            ("cluster_domain", config.cluster_domain.as_str()),
        ],
    );

    let udp_handle = tokio::spawn(run_udp(udp_socket, config.clone(), resolver.clone()));
    let tcp_handle = tokio::spawn(run_tcp(tcp_listener, config, resolver));

    Ok(DnsServerHandle {
        _udp: udp_handle,
        _tcp: tcp_handle,
    })
}

async fn run_udp(socket: UdpSocket, config: DnsConfig, resolver: DnsResolver) {
    let max_len = config.max_udp_payload_size as usize;
    let socket = Arc::new(socket);
    let mut buf = vec![0u8; max_len];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, peer)) => {
                let data = buf[..len.min(max_len)].to_vec();
                let socket = Arc::clone(&socket);
                let config = config.clone();
                let resolver = resolver.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_udp_query(socket, data, peer, config, resolver).await {
                        log_warn(
                            DNS_COMPONENT,
                            "Failed processing UDP query",
                            &[("error", err.to_string().as_str())],
                        );
                    }
                });
            }
            Err(err) => {
                log_error(
                    DNS_COMPONENT,
                    "UDP listener failed",
                    &[("error", err.to_string().as_str())],
                );
                break;
            }
        }
    }
}

async fn run_tcp(listener: TcpListener, config: DnsConfig, resolver: DnsResolver) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let resolver = resolver.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_tcp_stream(stream, peer, config, resolver).await {
                        log_warn(
                            DNS_COMPONENT,
                            "Failed processing TCP query",
                            &[("error", err.to_string().as_str())],
                        );
                    }
                });
            }
            Err(err) => {
                log_error(
                    DNS_COMPONENT,
                    "TCP listener failed",
                    &[("error", err.to_string().as_str())],
                );
                break;
            }
        }
    }
}

async fn handle_udp_query(
    socket: Arc<UdpSocket>,
    data: Vec<u8>,
    peer: SocketAddr,
    config: DnsConfig,
    resolver: DnsResolver,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let response = process_query(&data, &config, &resolver, Transport::Udp).await?;
    if !response.is_empty() {
        let _ = socket.send_to(&response, peer).await;
    }
    Ok(())
}

async fn handle_tcp_stream(
    mut stream: TcpStream,
    peer: SocketAddr,
    config: DnsConfig,
    resolver: DnsResolver,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        let mut len_buf = [0u8; 2];
        if stream.readable().await.is_err() {
            break;
        }
        if let Err(err) = stream.try_read(&mut len_buf) {
            if err.kind() == std::io::ErrorKind::WouldBlock {
                continue;
            }
            break;
        }
        let expected = u16::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; expected];
        stream.read_exact(&mut buf).await?;
        let response = process_query(&buf, &config, &resolver, Transport::Tcp).await?;
        if !response.is_empty() {
            let len = (response.len() as u16).to_be_bytes();
            stream.write_all(&len).await?;
            stream.write_all(&response).await?;
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
    let message = match Message::from_bytes(data) {
        Ok(msg) => msg,
        Err(err) => {
            metrics::record_dns_response("FORMERR");
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
            let forwarded = forward_query(upstreams, data, transport).await?;
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
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    for upstream in upstreams {
        match transport {
            Transport::Udp => {
                let bind_addr = SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0);
                if let Ok(socket) = UdpSocket::bind(bind_addr).await {
                    let _ = socket.send_to(data, upstream).await;
                    let mut buf = vec![0u8; 4096];
                    if let Ok(Ok((len, _))) =
                        timeout(DEFAULT_FORWARD_TIMEOUT, socket.recv_from(&mut buf)).await
                    {
                        buf.truncate(len);
                        if let Ok(msg) = Message::from_bytes(&buf) {
                            let code = msg.response_code().to_string();
                            metrics::record_dns_response(code.as_str());
                        }
                        return Ok(buf);
                    }
                }
            }
            Transport::Tcp => {
                if let Ok(stream) =
                    timeout(DEFAULT_FORWARD_TIMEOUT, TcpStream::connect(upstream)).await
                {
                    let mut stream = stream?;
                    let len = (data.len() as u16).to_be_bytes();
                    stream.write_all(&len).await?;
                    stream.write_all(data).await?;
                    let mut len_buf = [0u8; 2];
                    stream.read_exact(&mut len_buf).await?;
                    let expected = u16::from_be_bytes(len_buf) as usize;
                    let mut buf = vec![0u8; expected];
                    stream.read_exact(&mut buf).await?;
                    if let Ok(msg) = Message::from_bytes(&buf) {
                        let code = msg.response_code().to_string();
                        metrics::record_dns_response(code.as_str());
                    }
                    return Ok(buf);
                }
            }
        }
    }
    metrics::record_dns_response("SERVFAIL");
    Ok(Vec::new())
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
