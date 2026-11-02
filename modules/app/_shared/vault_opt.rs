// A graph Pod that keeps its OWN keys: Fluxor's workload vault (the CREATE
// option `vault`/`store`, `<dir>\0<64 hex seal key>`, fluxor
// sdk/contracts/workload.rs `vault`), stated by the Pod's container through
// two names of its environment — so the seal key is resolved as every other
// env value is (`valueFrom.secretKeyRef`, on the plane, by pod_resolve) and
// rides the chain the environment rides:
//
//   FLUXOR_VAULT_DIR   where the keys are kept, as the workload names it: at
//                      or beneath the mountPath of one of its WRITABLE
//                      volumes (Fluxor refuses any other), absolute
//   FLUXOR_VAULT_KEY   the 32-byte seal key, 64 hex digits (a Secret's value;
//                      one trailing newline, as `echo` leaves one, is not
//                      part of it)
//
// Both, or neither: one without the other is a vault that cannot be stated,
// and the workload is refused rather than run with its keys in the node's
// store. Neither name is handed to the workload as an environment entry —
// the seal key is the runtime's, not a module param's. `include!`d by
// workload_effect (with `argv.rs`, whose `argv_decode` reads an escaped env
// value), and by the harness suite that proves it.

/// The env name of the vault's directory.
const VAULT_DIR_ENV: &[u8] = b"FLUXOR_VAULT_DIR";
/// The env name of the vault's seal key.
const VAULT_KEY_ENV: &[u8] = b"FLUXOR_VAULT_KEY";
/// The option's namespace and key (fluxor `contracts::workload::vault`).
const VAULT_OPT_NS: &[u8] = b"vault";
const VAULT_OPT_KEY: &[u8] = b"store";
/// A Tier-2 entry the backend must realize, or refuse (`ENOSYS`).
const VAULT_OPT_REQUIRED: u8 = 0x01;
/// The seal key's bytes (64 hex digits in the option).
const VAULT_SEAL_LEN: usize = 32;
/// The longest directory a vault names.
const VAULT_DIR_MAX: usize = 255;

/// Why a Pod's vault cannot be stated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VaultErr {
    /// One of the two names without the other.
    Half,
    /// The directory is empty, not absolute, longer than [`VAULT_DIR_MAX`],
    /// or holds a NUL or a `..` segment.
    Dir,
    /// The key is not 64 hex digits.
    Key,
    /// The option does not fit `out`.
    Room,
}

/// The escaped env entry's name (before its first `=`).
fn vault_env_name(e: &[u8]) -> Option<&[u8]> {
    let i = e.iter().position(|&c| c == b'=')?;
    (i > 0).then(|| e.get(..i)).flatten()
}

/// Whether the escaped env entry `e` is one the vault takes: never handed
/// to the workload as its environment.
fn vault_env(e: &[u8]) -> bool {
    matches!(vault_env_name(e), Some(n) if n == VAULT_DIR_ENV || n == VAULT_KEY_ENV)
}

/// The escaped value of the LAST entry named `name` in `env` (`NAME=VALUE,…`,
/// a later entry of a name replacing an earlier, as the environment's).
fn vault_env_value<'a>(env: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    env.rsplit(|&c| c == b',')
        .find(|e| vault_env_name(e) == Some(name))
        .map(|e| &e[name.len() + 1..])
}

fn vault_hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// The Tier-2 `vault`/`store` entry `env` states, written at `out`: `Ok(0)`
/// when it names neither, `Ok(n)` the entry's `n` bytes — `[ns_len][ns]
/// [key_len][key][REQUIRED][val_len u16 LE]<dir>\0<64 lowercase hex>` — and
/// an error when the vault cannot be stated (a workload then refused, never
/// run without it).
#[allow(
    clippy::manual_contains,
    reason = "`<[u8]>::contains` lowers to memchr, which the PIC link cannot resolve"
)]
fn vault_option(env: &[u8], out: &mut [u8]) -> Result<usize, VaultErr> {
    let dir_e = vault_env_value(env, VAULT_DIR_ENV);
    let key_e = vault_env_value(env, VAULT_KEY_ENV);
    let (dir_e, key_e) = match (dir_e, key_e) {
        (None, None) => return Ok(0),
        (Some(d), Some(k)) => (d, k),
        _ => return Err(VaultErr::Half),
    };
    let mut dir = [0u8; VAULT_DIR_MAX];
    let dl = argv_decode(dir_e, &mut dir).ok_or(VaultErr::Dir)?;
    let dir = &dir[..dl];
    if dl == 0
        || dir[0] != b'/'
        || dir.iter().any(|&c| c == 0)
        || dir.split(|&c| c == b'/').any(|s| s == b"..")
    {
        return Err(VaultErr::Dir);
    }
    let mut key = [0u8; 2 * VAULT_SEAL_LEN + 2];
    let mut kl = argv_decode(key_e, &mut key).ok_or(VaultErr::Key)?;
    if kl > 0 && key[kl - 1] == b'\n' {
        kl -= 1;
    }
    if kl != 2 * VAULT_SEAL_LEN || !key[..kl].iter().all(|&c| vault_hex(c).is_some()) {
        return Err(VaultErr::Key);
    }
    let vl = dl + 1 + kl;
    let head = 1 + VAULT_OPT_NS.len() + 1 + VAULT_OPT_KEY.len() + 1 + 2;
    let o = out.get_mut(..head + vl).ok_or(VaultErr::Room)?;
    o[0] = VAULT_OPT_NS.len() as u8;
    o[1..1 + VAULT_OPT_NS.len()].copy_from_slice(VAULT_OPT_NS);
    let k = 1 + VAULT_OPT_NS.len();
    o[k] = VAULT_OPT_KEY.len() as u8;
    o[k + 1..k + 1 + VAULT_OPT_KEY.len()].copy_from_slice(VAULT_OPT_KEY);
    let f = k + 1 + VAULT_OPT_KEY.len();
    o[f] = VAULT_OPT_REQUIRED;
    o[f + 1..f + 3].copy_from_slice(&(vl as u16).to_le_bytes());
    let v = &mut o[head..];
    v[..dl].copy_from_slice(dir);
    v[dl] = 0;
    for (d, &c) in v[dl + 1..].iter_mut().zip(&key[..kl]) {
        *d = c.to_ascii_lowercase();
    }
    Ok(head + vl)
}
