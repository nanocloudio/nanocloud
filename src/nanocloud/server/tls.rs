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

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;

use openssl::pkey::PKey;
use openssl::ssl::{
    select_next_proto, AlpnError, Ssl, SslAcceptor as OpenSslAcceptor, SslMethod, SslVerifyMode,
};
use openssl::stack::Stack;
use openssl::x509::{X509Name, X509};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::security::TlsInfo;

pub(super) fn build_tls_acceptor(
    addr: &SocketAddr,
    require_client_certificate: bool,
) -> Result<OpenSslAcceptor, Box<dyn Error + Send + Sync>> {
    let mut san = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];

    match addr.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => san.push(ip.to_string()),
        IpAddr::V6(ip) if !ip.is_unspecified() => san.push(ip.to_string()),
        _ => {}
    }

    san.sort();
    san.dedup();

    let tls = TlsInfo::create("nanocloud-server", Some(&san))
        .map_err(|e| with_context(e, "Failed to create server TLS assets"))?;
    let server_cert = X509::from_pem(&tls.cert)
        .map_err(|e| with_context(e, "Failed to parse server certificate PEM"))?;
    let server_key = PKey::private_key_from_pem(&tls.key)
        .map_err(|e| with_context(e, "Failed to parse server private key PEM"))?;
    let ca_cert = X509::from_pem(&tls.ca)
        .map_err(|e| with_context(e, "Failed to parse cluster CA certificate PEM"))?;

    let mut builder = OpenSslAcceptor::mozilla_modern(SslMethod::tls())
        .map_err(|e| with_context(e, "Failed to initialize TLS acceptor builder"))?;
    builder
        .set_private_key(&server_key)
        .map_err(|e| with_context(e, "Failed to attach server private key"))?;
    builder
        .set_certificate(&server_cert)
        .map_err(|e| with_context(e, "Failed to attach server certificate"))?;
    builder
        .check_private_key()
        .map_err(|e| with_context(e, "Server certificate and key mismatch"))?;
    builder
        .cert_store_mut()
        .add_cert(ca_cert.clone())
        .map_err(|e| with_context(e, "Failed to add cluster CA to certificate store"))?;
    let mut verify_mode = SslVerifyMode::PEER;
    if require_client_certificate {
        verify_mode |= SslVerifyMode::FAIL_IF_NO_PEER_CERT;
    }
    builder.set_verify(verify_mode);

    let mut name_stack = Stack::<X509Name>::new()
        .map_err(|e| with_context(e, "Failed to prepare client CA stack"))?;
    name_stack
        .push(
            ca_cert
                .subject_name()
                .to_owned()
                .map_err(|e| with_context(e, "Failed to copy cluster CA subject"))?,
        )
        .map_err(|e| with_context(e, "Failed to register client CA subject"))?;
    builder.set_client_ca_list(name_stack);
    builder.set_verify_callback(SslVerifyMode::PEER, |_, _| true);

    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    let hash = hasher.finish();
    builder
        .set_session_id_context(&hash.to_be_bytes())
        .map_err(|e| with_context(e, "Failed to set TLS session context"))?;

    const ALPN_PROTO_LIST: &[u8] = b"\x08http/1.1";
    builder
        .set_alpn_protos(ALPN_PROTO_LIST)
        .map_err(|e| with_context(e, "Failed to configure ALPN protocols"))?;
    builder.set_alpn_select_callback(|_, client| {
        select_next_proto(client, ALPN_PROTO_LIST).ok_or(AlpnError::NOACK)
    });

    Ok(builder.build())
}

pub(super) async fn accept_with_tls(
    acceptor: &OpenSslAcceptor,
    stream: TcpStream,
) -> Result<SslStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    let ssl = Ssl::new(acceptor.context())
        .map_err(|e| with_context(e, "Failed to initialize TLS session"))?;
    let mut tls_stream = SslStream::new(ssl, stream)
        .map_err(|e| with_context(e, "Failed to bind TLS stream to socket"))?;
    Pin::new(&mut tls_stream)
        .accept()
        .await
        .map_err(|e| with_context(e, "TLS handshake failed"))?;
    Ok(tls_stream)
}
