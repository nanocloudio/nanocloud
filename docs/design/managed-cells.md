# Managed cells: enrollment and authority epochs

A Nanocloud cell runs standalone, or under a **management plane** — a fabric
controller that places cells, owns their policy and acts in them through
their public API. This document is the cell's side of that relationship: how
a management plane enrolls with a cell, what credential it holds there, and
how the cell stops a management writer that has lost its authority.

Nothing here is a second runtime. The management plane is an ordinary API
client of the cell; the cell's state stays the cell's; a cell keeps running
its accepted workloads when the management plane is unreachable.

## The bootstrap envelope, from the cell's side

A management plane that creates a cell hands it a short-lived **bootstrap
envelope**: the cell's UID, the management plane's endpoints and public
certificate chain, a one-time **enrollment grant**, the authority epoch it
acts under, and nothing secret beyond the grant (no root key, no private
signing key, no administrator credential). Both sides hold the envelope; the
cell computes its digest from the bytes it received.

The cell records one management plane as one **ManagementBinding**:

```yaml
apiVersion: nanocloud.io/v1
kind: ManagementBinding            # cluster-scoped
metadata:
  name: fabric-a                   # a DNS label
spec:
  cellUID: 5f0c2a8e-3d41-4b7a-9c1e-8a2b6d4f0e17
  envelopeDigest: sha256:9b7e…     # of the envelope this cell received
  grant: g7Qm2xV9pL4sT8wZ…         # the envelope's one-time grant; no "/"
  authorityEpoch: 3                # the epoch the envelope carries (default 0)
  authority:                       # the management plane's public identity,
    name: fabric-a                 #   recorded as given
    issuer: https://fabric-a.example
    certificate: <base64 DER chain>
  rules:                           # what its credential may do in this cell
    - apiGroups: [""]
      resources: ["configmaps"]
      verbs: ["get", "list", "create", "patch", "delete"]
```

It is written by the cell's administrator through the API, or by a managed
cell's installer from the envelope it was booted with: the CLI applet writes
the store offline, before the service starts (`nanocloud bundle apply`, one
`/managementbindings.nanocloud.io/<name>⇥<json>` line — the JSON with its own
`metadata.uid`, which the API would otherwise stamp; a binding without one
is offered nothing). It is never written by the management plane: it holds no credential until it
enrolls. `spec.grant` is readable by whoever may read ManagementBindings —
the cell's administrators, as kubeadm's bootstrap-token Secrets are — and is
spent by the first enrollment.

From it the cell derives (`management.plan`, the `mb_` chain):

| Object | What |
| --- | --- |
| Namespace `nanocloud-management` | created if absent |
| ServiceAccount `nanocloud-management/<binding>` | the management plane's identity in the cell |
| ClusterRole `nanocloud-management:<binding>` | the binding's own rules — `get` its ManagementBinding, `create` its `managementbindings/epoch` — then `spec.rules`, spliced from the stored binding; rewritten when they change |
| ClusterRoleBinding `nanocloud-management:<binding>` | the two; owned by the binding, and collected when the binding is deleted — which revokes everything the management plane could do |
| `/authority-epochs/system:serviceaccount:nanocloud-management:<binding>/<authorityEpoch>` | the envelope's epoch, committed (create-only) |
| `/enrollment-offers/<binding>/<grant>/<cellUID>/<envelopeDigest>` | the enrollment offer — made ONCE per grant, ever: the grant's offers (`/enrollment-offers/<binding>/<grant>/`, live or spent) are counted, and the offer is CREATED only when there are none. One write: a crash before it leaves no offer, and the next pass makes it; after it, the offer. An enrollment CLAIMS it — answered and left spent in the same store step — so however often the binding is read again (a restart, an edit) a spent grant is never offered again |

When the binding changes, its offers are checked against it (`management_sweep.plan`,
the `mb_wd` chain): an offer for a grant, cell UID or digest the binding no
longer states — or of a binding being deleted, or gone — is CLAIMED: withdrawn,
and spent like an enrolled one. A new grant therefore supersedes the earlier
one at once: the earlier grant enrolls nothing, and stated again later it is
not offered again. When the binding is deleted, its ClusterRoleBinding,
ClusterRole and ServiceAccount are collected (`mb_gc*`, by their first
ownerReference).

What a deleted binding leaves, on purpose: its committed epochs and its spent
offers. A credential the cell minted names a SUBJECT
(`system:serviceaccount:nanocloud-management:<binding>`), not the binding
object, and stays valid until it expires (an hour). A binding made again under
the same name (a new uid) re-derives the ClusterRoleBinding for that subject;
the kept epochs are what then refuses the predecessor's credentials (`409`), and
the kept claims are what keeps a spent grant from enrolling again. So a binding
made again under a name must state an `authorityEpoch` greater than the
greatest its predecessor committed — the fence admits an equal epoch — and a
grant of its own.

A part that is empty or holds `/`, `"` or `\` is offered nothing: the grant,
UID and digest are key parts.

Writing a ManagementBinding through the API is writing that ClusterRole and
its ClusterRoleBinding at once, so it takes upstream's escalation rule for
both (`apiplane_esc.plan`, `_shared/rbac_cover.rs`): the writer must already
hold, cluster-wide, every permission `spec.rules` grants and the binding's own
two (`get` the binding, `create` its `managementbindings/epoch`, by name) — or
hold both `escalate` and `bind` on the ClusterRole
`nanocloud-management:<binding>`. A create, a replace and every patch (on its
result) are checked; a refusal is upstream's 403 naming the first permission
not held. The cell's administrator (cluster-admin) holds everything; the
installer's offline `nanocloud bundle apply` writes the store directly and is
not an API write.

## Enrollment

```
POST /apis/nanocloud.io/v1/managementbindings/<binding>/enroll
Authorization: Bearer <grant>
Content-Type: application/json

{"apiVersion":"nanocloud.io/v1","kind":"Enrollment",
 "spec":{"cellUID":"<cell UID>","envelopeDigest":"sha256:<hex>"}}
```

The bearer credential is the grant, not a token: it is never handed to the
token verifier. The request names exactly one offer key — the binding, the
grant, the cell UID and the envelope digest — and the cell CLAIMS that key
(answered and left spent in one store step, conditional on the revision read:
the record is replaced by the claim, which every later take answers as absent
and which a create of the key finds there).
Registration is complete only when both sides prove the same cell UID and
envelope digest (spec §9): the management plane proves them by naming them,
and the cell's `201` is its proof that it holds the same pair — it answered
from an offer keyed by exactly those values, over the TLS connection the
envelope's certificate chain authenticates.

| Answer | When |
| --- | --- |
| `201` an `authentication.k8s.io/v1` TokenRequest for `nanocloud-management/<binding>` (`status.token`, `status.expirationTimestamp`; audience `nanocloud`, 3600 s) | the offer was there: the grant is now spent |
| `403 Forbidden` "enrollment refused: …" | a wrong grant, cell UID or digest, a spent grant, a re-enrollment under another cell UID, no such binding — one answer for all of them |
| `503` | the store could not decide; the grant is NOT spent: present it again |

The body is read inline (at most 512 bytes). Restart does not repeat
enrollment: the management plane keeps its credential and renews it (below);
a lost credential is a new grant written by the cell's administrator.

## Authority epochs

The management plane acts under an **authority epoch**: a number its own
consensus advances whenever authority over the cell moves (a new leader, a
failover, a restored backup). A writer that has lost authority but not yet
noticed must not change the cell. The management plane fences its own writes
(compare-and-swap); the cell fences them too, so a stale writer is stopped
even when it ignores its own fence.

Every **write** (POST, PUT, PATCH, DELETE) by a ManagementBinding's
credential names its epoch:

```
Nanocloud-Authority-Epoch: <decimal, at most 18 digits>
```

The request path's `fence` stage (before authorization, `apiplane_fence.plan`)
compares it with the epochs the cell has committed for that credential:

| Write | Answer |
| --- | --- |
| no header | `403 Forbidden` (the message names the header) |
| not a decimal number | `400 BadRequest` |
| older than the newest committed epoch | `409 Conflict`, "stale authority epoch N: this cell has committed epoch M" |
| equal or newer | admitted, to RBAC and admission as any request — a newer header commits nothing |
| the committed epochs cannot be read | `503` |

Reads are not fenced. A caller with no committed epoch (any identity that is
not a ManagementBinding's) is not fenced; its header is ignored.

### Committing an epoch, and renewing the credential

```
POST /apis/nanocloud.io/v1/managementbindings/<binding>/epoch
Authorization: Bearer <the binding's token>
Nanocloud-Authority-Epoch: <E>

{}
```

commits `E` for the binding and answers `201` with a fresh TokenRequest for
its ServiceAccount: the management plane renews its credential under the
epoch it holds, before the token expires. The path is fenced by the
binding's own committed epochs whoever calls it, so an `E` behind the
committed epoch is the `409` above, and an `E` equal to it is idempotent.
Only the binding's own credential (its ClusterRole's resource-named rule) —
or an administrator holding `managementbindings/epoch` — may call it; one
binding cannot move another's epoch.

The committed epochs are keys, one per epoch, and the committed epoch is the
greatest of them: a set that only grows, so two advances in flight can never
take it back, and nothing — not an edit of `spec.authorityEpoch`, not a
replayed request — lowers it.

## What the management plane does

1. Make the envelope; boot the cell with it (its installer writes the
   ManagementBinding).
2. `POST …/managementbindings/<b>/enroll` with the grant and the cell UID and
   envelope digest it holds; keep the TokenRequest's token.
3. On every write, send `Nanocloud-Authority-Epoch: <its current epoch>`;
   treat a `409` from the fence as "my authority is stale": stop, re-read its
   own state, do not retry under the same epoch.
4. When its epoch advances, `POST …/managementbindings/<b>/epoch` with the new
   one — before any write under it — and use the token it answers.
5. Renew by re-posting the current epoch to the same path.

## Proven by

`scripts/management-binding-e2e.sh` (the shipped control plane, live):
the derived objects; enrollment refused for a wrong grant, cell UID, digest
or binding, admitted for the right three, the grant spent, re-enrollment for
another cell UID refused; the credential under its rules; a managed write
without a header, with a malformed one, and with a stale epoch (create,
update, patch, delete) refused, an equal or newer one admitted; an epoch
committed and the credential renewed, the stale writer then refused, an
advance backwards refused, one binding unable to move another's epoch; a
grant replaced before it was spent superseding the earlier (claimed, 403; the
new one enrolls; the earlier stated again not offered again); the binding
deleted, its ClusterRoleBinding, ClusterRole and ServiceAccount collected and
the credential's writes refused, its epochs and spent offer kept — made again
at a greater epoch, the old credential is fenced (409) and the spent grant
refused; a binding written just before the plane is killed offered on the
next boot.
`tests/harness/tests/management_fence.rs` drives the stages' compiled
programs on Chronicle's pinned engine, the fence over a modelled COUNT.

## Limits

- A deleted binding's committed epochs and spent offers are kept (above): a
  few bytes per binding name ever used.
- Superseding a grant is eventually consistent: until the sweep has run on the
  binding's change (milliseconds), the earlier, unspent offer can still enroll.
- `spec.rules` is the administrator's grant to the management plane. Rules
  that let it write ManagementBindings (or RBAC objects) do not let it widen
  its own: what it writes may grant only what it holds (above), unless it is
  also granted `escalate` / `bind`.
