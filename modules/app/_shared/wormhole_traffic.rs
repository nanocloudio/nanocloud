// Wormhole's traffic cores (rfc_lb_vip Phase 0), mounted from the published
// `wormhole-common` tree (`target/fluxor/wormhole-common/traffic/`): its
// canonical hashing, selection, the v1 routing records and their strict
// decoders, health, VIP and profile rules. `include!`d by `wormhole_emit`,
// `wormhole_status` and the harness suite that tests them, so the record
// layouts nanocloud writes are Wormhole's own code, not a restatement.
//
// Why not `#[path] mod traffic` of Wormhole's `traffic/mod.rs`: that file
// mounts the SDK's SHA-256 by a path relative to Wormhole's own tree, which
// resolves to `target/target/fluxor/...` from a consumer's staged copy. So
// this shim includes `traffic/cores.rs` — Wormhole's list of its cores, in
// order — with the SHA-256 the includer already holds at its crate root.

// What the cores name beside them (`cores.rs`): Clustor's registries and
// wire (`clustor::*`, from the staged `clustor-common`) and Fluxor's
// net-identity address-control contract (`netid`). Mounted from the trees
// this project already stages, not restated; the projection itself uses
// none of them, but the VIP cores in the same list do. `#[path]` resolves
// beside the includer's crate root, which every includer keeps three
// directories below the project root.
#[rustfmt::skip]
#[allow(
    dead_code,
    unfulfilled_lint_expectations,
    reason = "Clustor's wire and registries are mounted whole for the cores that name them; an `expect` Clustor's own lint configuration fulfils (its `type_complexity` threshold) need not be fulfilled under this project's"
)]
#[path = "../../../target/fluxor/clustor-common"]
mod wh_clustor {
    pub mod reservation_registry;
    pub mod slot_index;
    pub mod transport_registry;
    pub mod vip_registry;
    pub mod wire;
}
#[allow(
    dead_code,
    clippy::all,
    reason = "the identity contract is mounted whole for the cores that name it"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/net/identity.rs"]
mod wh_netid;

#[allow(
    dead_code,
    unused_imports,
    clippy::all,
    reason = "Wormhole's cores are mounted whole; the projection uses the records, their ids and the VIP binding"
)]
mod traffic {
    pub use super::{sha256, Sha256};
    use super::{wh_clustor as clustor, wh_netid as netid};
    include!("../../../target/fluxor/wormhole-common/traffic/cores.rs");
}
