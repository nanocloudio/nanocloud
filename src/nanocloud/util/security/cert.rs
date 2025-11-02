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

use openssl::asn1::Asn1Time;
use openssl::ec::{EcGroup, EcKey};
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::x509::extension::{
    AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
    SubjectKeyIdentifier,
};
use openssl::x509::{X509Extension, X509Name, X509Req, X509v3Context, X509};
use std::iter;

pub(super) fn gen_ecc_key() -> Result<PKey<Private>, ErrorStack> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
    let ec_key = EcKey::generate(&group)?;
    PKey::from_ec_key(ec_key)
}

pub(super) fn gen_rsa_key() -> Result<PKey<Private>, ErrorStack> {
    let rsa_key = Rsa::generate(2048)?;
    PKey::from_rsa(rsa_key)
}

pub(super) fn gen_x509_cert(
    common_name: &str,
    key: &PKey<Private>,
    ca: Option<(&X509, &PKey<Private>)>,
    san_list: Option<&Vec<String>>,
) -> Result<X509, ErrorStack> {
    let name = gen_x509_name(common_name)?;
    let mut builder = X509::builder()?;
    builder.set_version(2)?;
    builder.set_subject_name(&name)?;
    builder.set_issuer_name(ca.map(|x509| x509.0.subject_name()).unwrap_or(&name))?;
    builder.set_pubkey(key)?;
    builder.set_not_before(openssl::asn1::Asn1Time::days_from_now(0)?.as_ref())?;
    builder.set_not_after(openssl::asn1::Asn1Time::days_from_now(36525)?.as_ref())?;

    builder.append_extension(
        SubjectKeyIdentifier::new().build(&builder.x509v3_context(None, None))?,
    )?;

    if let Some((ca_cert, ca_key)) = ca {
        builder.append_extension(
            AuthorityKeyIdentifier::new()
                .keyid(true)
                .issuer(true)
                .build(&builder.x509v3_context(Some(ca_cert), None))?,
        )?;
        builder.append_extension(BasicConstraints::new().critical().build()?)?;
        builder.append_extension(
            KeyUsage::new()
                .critical()
                .digital_signature()
                .key_encipherment()
                .build()?,
        )?;
        builder.append_extension(
            ExtendedKeyUsage::new()
                .server_auth()
                .client_auth()
                .build()?,
        )?;

        if let Some(san_list) = san_list {
            let san_list = iter::once(common_name.to_string())
                .chain(san_list.iter().cloned())
                .collect();
            builder.append_extension(gen_san_extension(
                &san_list,
                &builder.x509v3_context(Some(ca_cert), None),
            )?)?;
        }

        builder.sign(ca_key, MessageDigest::sha256())?;
    } else {
        builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
        builder.append_extension(
            KeyUsage::new()
                .critical()
                .digital_signature()
                .key_encipherment()
                .key_cert_sign()
                .build()?,
        )?;
        builder.sign(key, MessageDigest::sha256())?;
    }

    Ok(builder.build())
}

fn gen_x509_name(common_name: &str) -> Result<X509Name, ErrorStack> {
    let mut name_builder = X509Name::builder()?;
    name_builder.append_entry_by_nid(Nid::COMMONNAME, common_name)?;
    Ok(name_builder.build())
}

fn gen_san_extension(
    san_list: &Vec<String>,
    context: &X509v3Context,
) -> Result<X509Extension, ErrorStack> {
    let mut dns_list: Vec<String> = Vec::new();
    let mut ip_list: Vec<String> = Vec::new();

    for name in san_list {
        if is_ipv4_literal(name) {
            ip_list.push(name.to_string());
        } else {
            dns_list.push(name.to_string());
        }
    }

    let mut san_builder = SubjectAlternativeName::new();
    for dns in &dns_list {
        let _ = san_builder.dns(dns);
    }
    for ip in &ip_list {
        let _ = san_builder.ip(ip);
    }

    san_builder.build(context)
}

fn is_ipv4_literal(candidate: &str) -> bool {
    let mut count = 0;
    for part in candidate.split('.') {
        if part.is_empty() || part.len() > 3 || !part.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        if let Ok(value) = part.parse::<u8>() {
            if part.len() > 1 && part.starts_with('0') && value != 0 {
                // Reject leading zeros except for zero itself.
                return false;
            }
        } else {
            return false;
        }
        count += 1;
    }
    count == 4
}

pub(crate) fn sign_csr(
    csr: &X509Req,
    ca_cert: &X509,
    ca_key: &PKey<Private>,
    ttl_days: u32,
) -> Result<X509, ErrorStack> {
    let ttl_days = ttl_days.max(1);
    let mut builder = X509::builder()?;
    builder.set_version(2)?;
    builder.set_subject_name(csr.subject_name())?;
    builder.set_issuer_name(ca_cert.subject_name())?;
    let public_key = csr.public_key()?;
    builder.set_pubkey(&public_key)?;
    builder.set_not_before(Asn1Time::days_from_now(0)?.as_ref())?;
    builder.set_not_after(Asn1Time::days_from_now(ttl_days)?.as_ref())?;

    builder.append_extension(
        SubjectKeyIdentifier::new().build(&builder.x509v3_context(None, None))?,
    )?;
    builder.append_extension(
        AuthorityKeyIdentifier::new()
            .keyid(true)
            .issuer(true)
            .build(&builder.x509v3_context(Some(ca_cert), None))?,
    )?;
    builder.append_extension(BasicConstraints::new().critical().build()?)?;
    builder.append_extension(
        KeyUsage::new()
            .critical()
            .digital_signature()
            .key_encipherment()
            .build()?,
    )?;
    builder.append_extension(ExtendedKeyUsage::new().client_auth().build()?)?;

    builder.sign(ca_key, MessageDigest::sha256())?;
    Ok(builder.build())
}
