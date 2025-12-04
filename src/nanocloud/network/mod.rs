//! Networking helpers for Nanocloud.
//!
//! The `policy` module owns Kubernetes NetworkPolicy translation into nftables
//! chains, while `proxy` is responsible for programming ClusterIP-style service
//! routing with iptables DNAT rules. `policy` decides which traffic is allowed
//! or dropped for each pod, and `proxy` ensures allowed traffic reaches healthy
//! endpoints.
//!
//! Typical usage:
//! ```rust,no_run
//! use nanocloud::nanocloud::network::policy::{
//!     PolicyChain, PolicyDirection, PolicyProgrammer, PolicyRule,
//! };
//!
//! // Build policy chains and apply them atomically.
//! let chains = vec![PolicyChain::new(
//!     "default",
//!     "web-0",
//!     "10.0.0.12",
//!     PolicyDirection::Ingress,
//!     vec![PolicyRule {
//!         cidr: Some("10.0.0.0/24".into()),
//!         protocol: Some("tcp".into()),
//!         port: Some(80),
//!     }],
//! )];
//! PolicyProgrammer::shared()?.sync(&chains)?;
//! # Ok::<(), nanocloud::nanocloud::network::policy::PolicyError>(())
//! ```
//!
//! ```rust,no_run
//! use nanocloud::nanocloud::network::proxy;
//! # use nanocloud::nanocloud::k8s::service::{Service, ServicePort, ServiceSpec, ServiceStatus};
//! # use nanocloud::nanocloud::k8s::endpoints::{EndpointAddress, EndpointSubset, Endpoints};
//! # use nanocloud::nanocloud::k8s::pod::ObjectMeta;
//! #
//! # let service = Service {
//! #     metadata: ObjectMeta {
//! #         name: Some("svc".into()),
//! #         namespace: Some("default".into()),
//! #         ..Default::default()
//! #     },
//! #     spec: ServiceSpec {
//! #         ports: vec![ServicePort {
//! #             name: Some("http".into()),
//! #             port: 80,
//! #             target_port: Some(8080),
//! #             protocol: Some("TCP".into()),
//! #         }],
//! #         ..Default::default()
//! #     },
//! #     status: Some(ServiceStatus {
//! #         cluster_ip: Some("10.203.0.12".into()),
//! #     }),
//! #     ..Default::default()
//! # };
//! # let endpoints = Endpoints {
//! #     metadata: ObjectMeta {
//! #         name: Some("svc".into()),
//! #         namespace: Some("default".into()),
//! #         ..Default::default()
//! #     },
//! #     subsets: vec![EndpointSubset {
//! #         addresses: vec![EndpointAddress {
//! #             ip: "10.1.0.30".into(),
//! #         }],
//! #         ports: Vec::new(),
//! #     }],
//! #     ..Default::default()
//! # };
//! // Program proxy rules for a service and remove them when no longer needed.
//! proxy::program_service(&service, &endpoints)?;
//! proxy::remove_service(&service)?;
//! # Ok::<(), nanocloud::nanocloud::network::proxy::ProxyError>(())
//! ```
//!
//! ## Guardrails for future growth
//! - Prefer small, purpose-built submodules over growing `policy.rs`/`proxy.rs`.
//! - Add shared helpers (configuration, validation, instrumentation) under
//!   `network::config` to avoid duplication.
//! - Keep public APIs documented with runnable examples; add doc tests alongside
//!   new entry points to catch drift.

pub mod config;
pub mod policy;
pub mod proxy;
