// Shared no_std core: where a change-feed consumer that hands revisions to
// its OWN clients stands — included via `include!("../_shared/feedpos.rs")`.
//
// `storage.namespace` CHANGES answers one bounded page per call. A page may
// cut INSIDE a revision (one command can touch many keys under one revision,
// and a page is bounded by entries and bytes, not by revisions), so the page
// carries `safe_rev`: the highest revision it delivered WHOLE — the cut's
// revision at a boundary, one less inside one; 0 on a relist page with more
// to come (nothing is whole until the relist ends); the final page's (`more`
// clear) is the revision its token names.
//
// A consumer that hands resume points to its clients — store_watch, whose
// event resourceVersion a Kubernetes client resumes from — must never hand
// out a revision past what it has delivered whole, or a client resuming from
// it skips the rest of that revision. So:
//
//   * an event is stamped `min(record.mod_rev, page.safe_rev)`
//     ([`feed_event_rev`]);
//   * the consumer's own cursor follows `safe_rev` ([`feed_cursor_after`]),
//     never the store's opaque continuation, which it keeps only while a
//     read pages ([`feed_token`]);
//   * a BOOKMARK is sent only at rest, at the cursor, and never while a
//     relist is mid-way ([`feed_bookmark`]).
//
// A client resuming from any revision so handed out misses nothing: every
// change it was not yet sent has a revision past it.
//
// The including module must have `abi` (the SDK root) in scope.

/// The revision an event read from a CHANGES page is stamped with: the
/// record's own, never past the page's `safe_rev`. On a relist page with
/// more to come that is 0: a client resuming from it relists.
fn feed_event_rev(mod_rev: u64, safe_rev: u64) -> u64 {
    mod_rev.min(safe_rev)
}

/// The consumer's cursor once a page is served: it follows `safe_rev`, and
/// never moves back (a relist page's 0 leaves it).
fn feed_cursor_after(cursor: u64, safe_rev: u64) -> u64 {
    cursor.max(safe_rev)
}

/// The token the next read resumes from, into `out`; its length. Mid-read
/// (`cont` non-empty) the store's continuation, echoed verbatim; at rest the
/// 8-byte revision of the cursor; with no revision at all (cursor 0) the
/// empty token — a relist.
fn feed_token(
    cont: &[u8],
    cursor: u64,
    out: &mut [u8; abi::contracts::storage::namespace::changes::TOKEN_MAX],
) -> usize {
    use abi::contracts::storage::namespace::changes;
    if !cont.is_empty() {
        let n = cont.len().min(out.len());
        for (o, &c) in out.iter_mut().zip(cont.iter().take(n)) {
            *o = c;
        }
        return n;
    }
    if cursor == 0 {
        return 0;
    }
    for (o, c) in out.iter_mut().zip(changes::revision_token(cursor)) {
        *o = c;
    }
    changes::REVISION_TOKEN_LEN
}

/// The revision a BOOKMARK may carry after a page with `more` as given was
/// served and the cursor moved: the cursor, at rest only, and never 0 (a
/// relist not yet whole).
fn feed_bookmark(cursor: u64, more: bool) -> Option<u64> {
    (!more && cursor != 0).then_some(cursor)
}
