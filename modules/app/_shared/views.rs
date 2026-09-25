// A key's marks — `include!`d by the connectors that answer the API.
//
// A view of an object (`…/scale`, `…/log`) is not an object of its own: it is
// the SAME stored object, read or written through a view. The request names
// it after a `#` in the key (`/deployments/ns/web#scale`), with the writer's
// marks after it; a key never holds a `#` otherwise. Storage always uses the
// part before it. What a view reads and writes is the schema's
// (docschema.rs `view.*`/`write.*`).

/// `<key>#<view>` → (`<key>`, `<view>`); a key without `#` has no view.
fn view_split(key: &[u8]) -> (&[u8], &[u8]) {
    match key.iter().position(|&b| b == b'#') {
        Some(i) => (&key[..i], &key[i + 1..]),
        None => (key, &[]),
    }
}

/// A view's text after `#`, split: `<view>[@[+]<manager>][!]` → (view,
/// manager, force, dry run). A write names its field manager after `@` (the
/// request's `fieldManager`, else its User-Agent's product), `+` when an apply
/// forces conflicts; `!` last marks a dry run. A key's name never holds `@`.
fn view_args(v: &[u8]) -> (&[u8], &[u8], bool, bool) {
    let dry = v.last() == Some(&b'!');
    let v = if dry { &v[..v.len() - 1] } else { v };
    match v.iter().position(|&b| b == b'@') {
        Some(i) => {
            let m = &v[i + 1..];
            let force = m.first() == Some(&b'+');
            (&v[..i], if force { &m[1..] } else { m }, force, dry)
        }
        None => (v, &[], false, dry),
    }
}
