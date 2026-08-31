// Kubernetes REST path parsing — the ONE parser.
//
// `include!`d by `kube_decode`, which projects a request into a Chronicle
// record frame. It lives in `_shared` rather than in that module because the
// grammar is the API surface's, not one consumer's: a second copy would drift
// the moment a group or a sub-resource is added, and the drift would be a
// silent mis-route rather than a build failure.

/// Parse `/api/v1/namespaces/<ns>/<resource>[/<name>]`,
/// `/api/v1/<resource>` (cluster/all-namespace), and the apps/batch groups into
/// `(resource, namespace, name)`. Returns None if not a resource path.
fn parse_rest_path(path: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    // Split into non-empty segments.
    let mut segs: [&[u8]; 12] = [&[]; 12];
    let mut n = 0;
    let mut start = 0;
    while start < path.len() && n < segs.len() {
        if path[start] == b'/' {
            start += 1;
            continue;
        }
        let end = path[start..]
            .iter()
            .position(|&b| b == b'/')
            .map(|i| start + i)
            .unwrap_or(path.len());
        segs[n] = &path[start..end];
        n += 1;
        start = end;
    }
    let segs = &segs[..n];
    // /api/v1/...           (core group)
    // /apis/<group>/v1/...  (named groups)
    let tail: &[&[u8]] = if segs.len() >= 2 && segs[0] == b"api" && segs[1] == b"v1" {
        &segs[2..]
    } else if segs.len() >= 3 && segs[0] == b"apis" {
        &segs[3..]
    } else {
        return None;
    };
    // tail forms, by length:
    //   1: [resource]                        collection (all namespaces / cluster)
    //   2: [resource, name]                  cluster-scoped object, incl. a namespace
    //   3: [namespaces, ns, resource]        namespaced collection
    //   4: [namespaces, ns, resource, name]  namespaced object
    let ns = b"namespaces";
    match tail.len() {
        1 => Some((tail[0], b"", b"")),
        2 => Some((tail[0], b"", tail[1])),
        3 if tail[0] == ns => Some((tail[2], tail[1], b"")),
        4 if tail[0] == ns => Some((tail[2], tail[1], tail[3])),
        _ => None,
    }
}
