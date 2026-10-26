// Wormhole's traffic cores with its Phase 7–9 lists beside them
// (`cores.rs`, then `continuity_cores.rs`, `portfolio_cores.rs` and
// `region_cores.rs`), mounted from the published `wormhole-common` tree for
// the parts of the projection that speak Wormhole's portfolio and region
// records: the failure-domain preflight and placement (`fd_ctl.v1`),
// tenant usage records, the signed `service_dns` projection and the
// explain tool. A superset of `wormhole_traffic.rs`; an includer takes one
// or the other, never both.
//
// What the later lists name beside the cores, mounted from the trees this
// project already stages, not restated: Clustor's registries and wire
// (every namespace the cores build commands for), Fluxor's net-identity
// contract and windowed counter reservation, and Kagi's operation scopes
// (the controllers' scope gate is in `region_cores.rs`). `#[path]`
// resolves beside the includer's crate root, three directories below the
// project root.
#[rustfmt::skip]
#[allow(
    dead_code,
    unfulfilled_lint_expectations,
    clippy::all,
    reason = "Clustor's wire and registries are mounted whole for the cores that name them"
)]
#[path = "../../../target/fluxor/clustor-common"]
mod wh_clustor {
    pub mod attachment_registry;
    pub mod failover_registry;
    pub mod operation_registry;
    pub mod region_registry;
    pub mod reservation_registry;
    pub mod session_registry;
    pub mod slot_index;
    pub mod timing;
    pub mod transport_registry;
    pub mod vip_registry;
    pub mod wire;
}
#[allow(
    dead_code,
    clippy::all,
    reason = "Kagi's scope codec is mounted whole for the controllers' scope gate"
)]
#[path = "../../../target/fluxor/kagi-common/operation_scope.rs"]
mod wh_kscp;
#[allow(
    dead_code,
    clippy::all,
    reason = "the identity contract is mounted whole for the cores that name it"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/net/identity.rs"]
mod wh_netid;
#[allow(
    dead_code,
    clippy::all,
    reason = "the reservation core is mounted whole for the datagram anchor"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/cores/nonce_reservation.rs"]
mod wh_nonce_res;

// Fluxor's TRANSPORT_CONTINUITY contract, which Wormhole's continuity cores
// re-export (`fx_contract.rs`): mounted from this project's staged SDK.
#[allow(
    dead_code,
    unused_imports,
    clippy::all,
    reason = "the contract is mounted whole for Wormhole's continuity cores"
)]
mod wh_fxtc {
    include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
    include!("../../../target/fluxor/fluxor-abi/sdk/contracts/net/transport_continuity.rs");
}

#[allow(
    dead_code,
    unused_imports,
    clippy::all,
    reason = "Wormhole's cores are mounted whole; the projection uses its portfolio and region records"
)]
mod traffic {
    pub use super::{sha256, Sha256};
    use super::{
        wh_clustor as clustor, wh_fxtc as fxtc, wh_kscp as kscp, wh_netid as netid,
        wh_nonce_res as nonce_res,
    };
    include!("../../../target/fluxor/wormhole-common/traffic/cores.rs");
    include!("../../../target/fluxor/wormhole-common/traffic/continuity_cores.rs");
    include!("../../../target/fluxor/wormhole-common/traffic/portfolio_cores.rs");
    include!("../../../target/fluxor/wormhole-common/traffic/region_cores.rs");
}
