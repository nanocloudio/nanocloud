// The `;`-separated `tag=value` reader — the STRICT form: `;` is the only
// delimiter, and `seg.len() > tag.len()` means an EMPTY value reads as absent.
//
// Included by the modules that want exactly this rule. The other variants in
// this repo are DELIBERATE and stay in the modules that need them:
//
//   * `,` as an additional delimiter — for records like `sel=k=v,k=v`
//     where a comma separates entries rather than ending one.
//
// Flattening these into one shared default would change parsing silently, in
// states the E2Es do not all reach. One file, one meaning; divergence stays
// visible at its call site.

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() > tag.len() && &seg[..tag.len()] == tag {
            return Some(&seg[tag.len()..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}
