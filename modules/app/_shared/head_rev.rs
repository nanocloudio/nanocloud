// A key's store revision, by HEAD — its own fragment because both store
// halves need it: `store.rs` (the connectors' conditional writes) and
// `store_source` (the revision a verdict is conditional on), which carries its
// own store plumbing and does not include `store.rs`. The including module
// supplies `MAX_KEY`.

/// storage.object HEAD — the key's current revision (the etag's first eight
/// bytes), or None when the key is absent. The opcode is local because only a
/// conditional writer needs it, and the fragment's contract is that an
/// including module names only the opcodes every module shares.
unsafe fn head_rev(sys: &SyscallTable, key: &[u8]) -> Option<u64> {
    const OBJ_HEAD: u32 = 0x1422;
    let mut arg = [0u8; MAX_KEY + 32];
    if 2 + key.len() + 8 + 4 + 8 + 2 > arg.len() {
        return None;
    }
    // `[size:u64][mtime:u64][ct_len:u8][ct][etag_len:u8][etag]` — sized for a
    // content type as long as the u8 prefix allows, so a provider that tags
    // its objects cannot push the etag off the end.
    let mut rec = [0u8; 8 + 8 + 1 + 255 + 1 + 32];
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p..p + 8].copy_from_slice(&(rec.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(rec.len() as u32).to_le_bytes());
    p += 4;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, OBJ_HEAD, arg.as_mut_ptr(), p);
    if n < 18 {
        return None;
    }
    let n = n as usize;
    let ct = rec[16] as usize;
    let at = 17 + ct;
    if at + 1 > n || (rec[at] as usize) < 8 || at + 1 + 8 > n {
        return None;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&rec[at + 1..at + 9]);
    Some(u64::from_le_bytes(b))
}
