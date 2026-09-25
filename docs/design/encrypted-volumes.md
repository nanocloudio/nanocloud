# Encrypted volumes: claims, the mount effect and the lifecycle pipelines

An encrypted volume is a Loam volume whose blocks `crypt_block` seals under a
volume key only a node's KEY_VAULT ever holds. Nanocloud provisions it,
attaches it to the node a pod runs on, keeps the attachment alive, and takes it
away again. It also changes who may attach it, rotates its key, snapshots and
restores it, recovers it through the recovery authority, and erases it. Each of these is a Chronicle pipeline over one store record, with
every side effect a typed request and a typed answer. Chronicle carries phases,
identifiers, fence tokens and deadlines. It never carries block data, key
material, share envelopes or attachment bundles.

| Source | Holds |
|---|---|
| `modules/app/_chronicle/volume_provision.uproc` | PVC + StorageClass → provision record → provision steps |
| `modules/app/_chronicle/volume_attach.uproc` | attach steps, renewal |
| `modules/app/_chronicle/volume_attach_epoch.uproc` | an attachment's second key epoch; the clone device guard |
| `modules/app/_chronicle/volume_detach.uproc` | ordered teardown: detach, crypto-erase, and every failure cleanup |
| `modules/app/_chronicle/volume_effect.uproc` | the typed-effect seam of provision, attach and detach: `act` (phase → request) and `record` (outcome → record) |
| `modules/app/_chronicle/volume_key_effect.uproc` | the seam of grant, rotation, the second epoch and crypto-erase |
| `modules/app/_chronicle/volume_data_effect.uproc` | the seam of snapshot, recovery and deletion |
| `modules/app/_chronicle/volume_grant.uproc` | grant rotation, device replacement, revocation |
| `modules/app/_chronicle/volume_rotate.uproc` | data-key rotation: `vt_prepare`, `vt_migrate` |
| `modules/app/_chronicle/volume_snapshot.uproc` | snapshot, retention and restore into a clone |
| `modules/app/_chronicle/volume_recovery.uproc` | recovery-authority use: `vr_recover`, `vr_unwind` |
| `modules/app/_chronicle/volume_delete.uproc` | secure deletion (`ve_step`) and retention expiry (`ve_retain`) |
| `modules/app/_chronicle/volume_claim.uproc` | the device claim form and the mount effect |
| `modules/app/_chronicle/volume.uproc` | unchanged: a claim's `path=` → `/volume-binds/<uid>/<i>` |

## Claims

`/volumes/<ns>/<pvc>` has two forms:

| Form | Written by | Meaning |
|---|---|---|
| `path=<host dir>` | an operator | bind this directory |
| `device=<path>;fs=<type>[;mkfs=1]` | the attach pipeline only | mount this published block device |

`mkfs=1` appears only while the volume's identity says it has never held a
filesystem. The mount effect adds its own fields to a device claim:

| Field | Value |
|---|---|
| `mount` | `mounting` · `mounted` · `fsck_failed` · `format_failed` · `mount_failed` · `denied` · `invalid` · `refused` · `unmount` · `unmounting` · `unmounted` · `busy` |
| `dir` | where it is mounted: `<mount root>/<ns>/<pvc>` |
| `path` | the same directory, present only while mounted |
| `code`, `at` | the last exit status; when the state was entered |

`volume.uproc` reads `path` alone, so the bind step never learns whether a
claim is a directory or an encrypted device. A device claim without `path`
yields no bind row, and the pod's sandbox waits.

Validation, in order:

| Claim | Result |
|---|---|
| no `device`, no `fs` | a path claim: left to `volume.uproc` |
| `fs` without `device` | `invalid` |
| `fs` other than `ext4` or `xfs` | `invalid` |
| `device` ≠ the `dev` the attachment record published | `refused`, looked at again once they match |
| `device` with a `path` it did not get from the mount effect | `invalid` |
| otherwise | mounted |

The mount effect records each state it enters with a `REPLACE` conditional on
the revision it read, so a stale emission cannot recreate a claim that detach
has withdrawn. Its outcomes are MERGEd, and detach never withdraws a claim
while a mount or unmount is in flight.

## The mount effect

`vc_claim` records the next state, and `vc_act` issues the request only if the
store accepted that write. The actuator answers raw facts, and `vc_done` maps
them to a typed outcome:

| Request | Actuator runs | Answers |
|---|---|---|
| `MOUNT` (50): dev, fs, mkfs | with `mkfs=1` and no filesystem signature: `mkfs -t <fs>`; otherwise `fsck -p`; then `mount -t <fs> -o nodev,nosuid` at `<mount root>/<claim>` | `priv`, `mkfs`, `fsck`, `code`, `dir` |
| `UNMOUNT` (51): dir | `umount` of `<mount root>/<claim>`, when it is a mount point | `priv`, `was`, `code` |

| Raw result | Typed outcome |
|---|---|
| `priv = 0` on MOUNT | `denied` (nothing ran) |
| `mkfs` > 0 | `format_failed` |
| `fsck` ≥ 4 | `fsck_failed`: errors left uncorrected, or an operational error |
| `fsck` 0, 1, 2 | clean or repaired; the mount proceeds. fsck(8)'s status 2 asks for a reboot only when the root filesystem was repaired. |
| mount `code` ≠ 0 | `mount_failed` |
| mounted | `mounted`, with `dir=` and `path=` in the same write, and `mkfs` cleared |
| UNMOUNT, `was = 0` | `unmounted` (nothing was mounted) |
| UNMOUNT, `priv = 0` or `code` ≠ 0 | `busy`: the filesystem is still mounted |

**Privilege.** Mounting needs `CAP_SYS_ADMIN`. The actuator checks it before
running anything and answers `priv = 0` when it lacks it. Without the
capability, `mount(8)` itself exits 32, the same status as any other mount
failure, so the exit status cannot report a missing privilege. Two gates stand
in front of the actuator:

- A device reaches it only when the attachment record for the same claim
  published exactly that device. Nothing but the attach pipeline can point it
  at a block device.
- An unmount the actuator could not perform is `busy`, never `unmounted`.
  Detach never goes past a filesystem that may still be mounted.

The decision half runs without root. The mapping above is exercised from
recorded actuator results: exit statuses as fsck(8), mkfs(8), mount(8) and
umount(8) report them, and `priv = 0` for an unprivileged actuator.

## Records

| Record | Opened by | Holds |
|---|---|---|
| `/volume-provisions/<ns>/<pvc>` | `vp_open`, when the StorageClass's provisioner is `encrypted.loam.nanocloud.io` | `phase out at tries wake`; the class policy `custody fs tier lifetime size access`; `device` (the first consumer's node); results `vol set grant vek`; `reason` |
| `/volume-attachments/<ns>/<pvc>` | `VOLUME_PERSIST`, the provision pipeline's identity step; `CLONE_PERSIST` for a clone | the identity `vol fs grant access set fresh`; the scheduler's `desired device`; `phase out at tries wake`; `fence lease_exp exp inst dev ready`; `reason note`; `erase` (secure deletion); `retain` (a retention deadline) |
| `/volume-keys/<ns>/<pvc>` | `VOLUME_PERSIST` (`kvol=<vol>;epoch=1`), `CLONE_PERSIST`, rotation's `KEY_EPOCHS` | `kvol`, the Kagi key resource (a clone's is its source's); `epoch`, the key epoch the container is sealed under; `next`, the epoch a rotation is introducing |
| `/volumes/<ns>/<pvc>` | `CLAIM_PUT` (attach) | the claim, as above |
| `/volume-grants/<ns>/<pvc>` | an operator | `op` rotate · replace · revoke, `custody tier lifetime`, `new`, `evict`, `actor` |
| `/volume-rotations/<ns>/<pvc>` | an operator, or `ROTATION_REQUEST` (eviction) | `why actor`, `from to set vek`, progress `cstate cursor`, `pins` |
| `/volume-snapshots/<ns>/<pvc>/<snap>` | an operator or the VolumeSnapshot controller | `desired retain restore restored`, `root elo ehi kvol clone` |
| `/volume-recoveries/<ns>/<pvc>` | an operator | `actor approver device ttl`, `ticket inst fence ready custodians` |
| `/volume-deletions/<ns>/<pvc>` | an operator, the PV's reclaim, `ve_retain` | `why actor pins erased` |

Every record except the claim also carries `phase out at tries wake reason
note`. Every step is write-ahead. The step decision MERGEs the next `phase`, clears
`out` and stamps `at` and `wake`. Only the store's acceptance of that MERGE
lets `act` issue the phase's effect. `record` writes the answer back as
`out=<phase>:<code>` with that phase's results. A result field is written only
by the phase that produces it. Every effect is idempotent for (claim, phase),
so a level-triggered re-emission or a re-drive never doubles one. An
unanswered step is asked again after 30 s, at most five times, and then
abandoned with `reason=exhausted:<phase>`.

## Provision

| Step | Effect | Result |
|---|---|---|
| validate | — | custody `possession` or `hardware`, fs `ext4` or `xfs`, a lifetime of 1 s to 24 h, a size, and access `ReadWriteOnce` or `ReadWriteOncePod`. Anything else fails with `policy_invalid`. |
| `p_create` | `LOAM_CREATE` | the Loam volume, `vol` |
| `p_authorise` | `KAGI_CREATE` | Kagi authorises the key policy (epoch 1) |
| `p_split` | `VAULT_SPLIT` | the provisioning vault generates the volume key and seals one recovery share to each of three custodians |
| `p_recordset` | `KAGI_RECORD_SET` | Kagi records the recovery set, `set` |
| `p_vekdrop` | `VAULT_DESTROY` | the provisioning vault destroys its key handle |
| (wait) | — | WaitForFirstConsumer: until `device` names the node a consumer was scheduled to |
| `p_grant` | `KAGI_GRANT` | the grant to that device, `grant` |
| `p_persist` | `VOLUME_PERSIST` | the volume identity opens the attachment record |
| `p_bind` | `PV_BIND` | the PersistentVolume, bound to the claim |

No step creates a durable recipient envelope. A node receives the key only as
a fresh two-share bundle, once per attachment. If a later step fails while the
provisioning vault still holds the key, `p_vekdrop` runs first, retried until
it answers. The record then goes to `failed` with its reason.

## Attach

The pipeline is active while `desired=attached` and no `reason` is set. Every
Kagi release and renewal names the key record's epoch and key resource. A
clone never moves its source's grant: scheduled to another device than the
grant names, it is refused `shared_key_device` before anything is acquired.

| Phase | Effect | Success |
|---|---|---|
| (start) | — | `device` and `vol` present; access is single-writer (otherwise `reason=access_mode`) |
| `granting` | `KAGI_REPLACE` | only when the grant names another device |
| `leasing` | `LEASE_ACQUIRE` | `fence`, `lease_exp`. Another holder answers `held`. |
| `instantiating` | `GRAPH_START` | `inst`: loam_volume under that fence, and crypt_block with `attach = 1` (`format = 1` only when `fresh`), in a quiesced graph that nothing consumes yet |
| `recipient` | `RECIPIENT` | crypt_block announced its fresh FXRK recipient key |
| `releasing` | `KAGI_RELEASE` | Kagi's attachment release, against the node's proof over recipient, volume, fence and nonce; `exp` is the handle expiry. The FXSB bundle stays with the effector. |
| `unlocking` | `BUNDLE_DELIVER` | the bundle on crypt_block's `bundle` port; `ready` once it rebuilt its key. A bundle for another recipient or volume is `eacces`; one for an epoch the container has already retired is `stale`. |
| `e_recipient` · `e_releasing` · `e_unlocking` | `CB_RECIPIENT` · `KAGI_RELEASE` · `CB_BUNDLE` | only with `next` set (`volume_attach_epoch.uproc`): the second epoch's recipient, release and bundle, so the container holds every key its units use before anything is published |
| `publishing` | `NBD_START` | nbd_serve over crypt_block, attached as `dev` |
| `claiming` | `CLAIM_PUT` | `device=<dev>;fs=<fs>[;mkfs=1]` |
| `mounting` | the mount effect | `mounted`; any failed state becomes `reason=mount:<state>` |
| `renew_lease` | `LEASE_RENEW` | the lease confirmed, `lease_exp` |
| `attached` | — | served; renewing |

**Renewal** is a state machine over two deadlines. The lease is renewed 20 s
before `lease_exp`, and the grant is re-proven (`KAGI_RENEW`, same device and
fence) 60 s before `exp`. Lease renewal takes priority, so a Kagi outage cannot
starve it. An unavailable service is retried every 5 s on the grace that is
left. The attachment is forced into teardown when:

| Condition | Reason |
|---|---|
| `lease_exp` less than 3 s away | `lease_expiring`: the device is quiesced while the lease is still this node's |
| the lease passed on | `lease_lost` |
| `exp` reached | `attachment_expired` |
| Kagi refuses the renewal | `grant_revoked` (final) |
| Kagi: the lease fence changed (29), the attachment expired (30), a newer link superseded it (19) | `renew_grant:fence_changed` · `:expired` · `:conflict`: detach, then attach again |
| the claim is no longer mounted | `mount_lost` |

## Detach

The pipeline is active when `desired` is not `attached` or a `reason` is set;
attach is inactive exactly then. It waits for an in-flight mount-effect step
to answer, and for an in-flight attach step up to the step timeout. It then
releases **only what the record says is
held, in this order**:

| # | Phase | Effect | Held when |
|---|---|---|---|
| 1 | `d_unmount` | `UNMOUNT_REQ`: the mount effect withdraws `path=`, then unmounts | a claim not yet settled as not-mounted |
| 2 | `d_flush` | `FLUSH`: nbd_serve, crypt_block's journal and homes, the Loam commit | `ready` |
| 3 | `d_unclaim` | `CLAIM_DELETE` | the claim exists |
| 4 | `d_unpublish` | `NBD_STOP` | `dev` |
| 4b | `d_erase` | `CB_ERASE`: FXER — every master destroyed, both superblocks zeroed | `erase=1` (secure deletion) and `inst` |
| 5 | `d_destroy` | `GRAPH_DESTROY`: crypt_block's recipient and volume-key handles die with it | `inst` |
| 6 | `d_release` | `LEASE_RELEASE` | `fence` |
| 7 | — | `detached` when asked; `idle` for a retryable reason (attached again after 5 s); `failed` otherwise | — |

Releasing the writer lease before the device is quiesced would permit two
writers. Destroying the handles before the flush would make recovery
ambiguous. Two steps can fail without blocking the order:

- A flush that fails five times is recorded (`note=flush_failed`) and passed.
  The committed root is intact, and what was staged since the last commit is
  lost.
- A lease that Loam cannot be reached to release is recorded
  (`note=lease_lapses`) and lapses at its TTL.

An unmount is never passed. While it stays `busy` the lease is no longer
renewed, and loam_volume refuses every write once the lease lapses. Retryable
reasons are `leasing:held`, `lease_lost`, `lease_expiring`,
`attachment_expired`, `renew_grant:fence_changed` · `:expired` · `:conflict`,
`device_replaced` and `exhausted:<attach phase>`. A volume being deleted
(`reason=deleting`) always ends `failed` with its reason kept, so nothing
attaches it again.

## Grant rotation and device replacement

`volume_grant.uproc`. None of these rewrites data: the key stays, and only
which device may be issued a fresh bundle changes.

| op | Steps |
|---|---|
| rotate | `g_rotate` `KAGI_ROTATE_GRANT`: same device, new policy (Kagi op 8). The attachment carries on; its next renewal is decided under the new policy. |
| replace | `g_wait` until the scheduler no longer asks for the old device; `g_detach` `ATT_DETACH` (`device_replaced`) only if attached there; `g_quiesce` until nothing is attached; `g_replace` `KAGI_REPLACE` (op 7); `g_grant` `ATT_GRANT`, so attach does not move it back |
| revoke | `g_revoke` `KAGI_REVOKE` first (op 6: no bundle or renewal from here); `g_detach` (`grant_revoked`, final); `g_quiesce` |
| `evict=1` | then `g_evict` `ROTATION_REQUEST`: a device that may have kept key material is evicted only by a new key |

Revocation cannot reach back into a node that already holds a handle, so it
is Kagi first, then the detach order. A clone is refused `shared_key`.

## Data-key rotation

`volume_rotate.uproc`, one record through the design's persisted state
machine. `vt_prepare` owns the phases up to PREPARED and `vt_migrate` the
rest; the two never both write.

| Phase | Effect | State |
|---|---|---|
| `r_kagi` | `KAGI_ROTATE_KEY` (op 9): epoch `to`; Kagi keeps admitting `from` | |
| `r_split` | `VAULT_SPLIT`: the provisioning vault makes the `to` key and seals a share to each custodian | |
| `r_recordset` | `KAGI_RECORD_SET`: the new recovery set | |
| `r_vekdrop` | `VAULT_DESTROY` | PREPARED |
| `r_mark` | `KEY_EPOCHS` `next=to`, before crypt_block hears of `to` | |
| `r_wait` | — until attached: the migration runs in the attached crypt_block | |
| `r_probe` | `CB_STATUS`: state toward `to` and cursor, every 5 s, recorded as `cstate cursor` | |
| `r_recipient` · `r_release` · `r_deliver` | `CB_RECIPIENT` · `KAGI_RELEASE` · `CB_BUNDLE`, when the probe says `none` | ACTIVE, MIGRATING, RETIRING (crypt_block, resumably) |
| `r_steady` | `KEY_EPOCHS` `epoch=to`, `next` cleared: attachments now need and renew only `to` | steady |
| `r_pins` · `r_pinned` | `EPOCH_PINS`: retained snapshots and live clones needing `from`; while any, rescan every 30 s | |
| `r_retire` | `KAGI_RETIRE_EPOCH` (op 10): `from`'s custody set destroyed, audited | RETIRED |

Retirement needs both proofs: crypt_block's that no live unit needs `from`
(steady), and this pipeline's that no retained snapshot or clone does.

**Mixed epochs.** `next` is recorded before crypt_block sees the new epoch,
and cleared only after crypt_block is steady. Any attachment in between —
including the re-attach after a node crash at any rotation state — takes
both keys (`e_*` above) before it publishes, and crypt_block resumes from its
superblock. A crash after steady but before `r_steady` delivers `from` first;
crypt_block answers `stale`, and `to` follows.

**Failure.** Before Kagi rotates, a refusal ends the record with nothing held.
After it, a failure is retried, never abandoned: a lost answer is re-driven, a
failed split is split again, a set Kagi will not record costs the new key
(`r_vekdrop`) and a fresh split. A key the provisioning vault holds when the
record fails is destroyed first. A clone cannot rotate its shared key
(`shared_key`), and a rotation waits for the last to retire
(`rotation_in_progress`: Kagi refuses a rotation while the current epoch's
recovery set is unrecorded).

## Snapshot and restore

`volume_snapshot.uproc`, one record per snapshot, keyed
`<ns>/<pvc>/<snap>`; related records are read at the key's parent.

| Phase | Effect | |
|---|---|---|
| (open) | — | the pin, write-ahead: `elo..ehi` = the key record's epochs (both during a rotation), before any capture |
| `s_flush` | `FLUSH` | only if attached: the committed root holds what was acknowledged |
| `s_capture` | `LOAM_SNAPSHOT` | one bind of the committed root under the snapshot root; answers the exact epochs that root's units use, which replace the pin |
| `retained` | — | until `desired=deleted` or `retain` passes |
| `s_clone` · `s_identity` | `LOAM_RESTORE` · `CLONE_PERSIST` | `restore=<ns>/<pvc>`: a clone (no bytes move), with the source's identity and a key record `kvol=<source> epoch=elo next=ehi`; the attach pipeline attaches it under exactly those epochs |
| `s_drop` | `LOAM_SNAP_DELETE` | the pin is released only when Loam answers |

A retained snapshot keeps every epoch in `elo..ehi` from being retired and its
volume's keys from being erased. A clone shares its source's key resource and
pins the same way until it is deleted. A volume being deleted takes no new
snapshot. Snapshots are crash-consistent: the filesystem is not frozen.

## Recovery authority

`volume_recovery.uproc`. Admission, before anything runs: an `actor` and a
different `approver`; the volume itself, not a clone; no rotation in flight
(a ticket names one epoch); nothing attached; a `device` other than the one
the grant names; a `ttl` of 1 … 3600 s.

| Phase | Effect |
|---|---|
| `rc_audit` | `AUDIT`: the authorisation, before anything else |
| `rc_ticket` | `KAGI_RECOVERY_AUTHORISE` (op 11): one ticket for this epoch and device |
| `rc_lease` · `rc_start` · `rc_recipient` | the lease, a quiesced instance on the recovering node and its recipient, held as `rc:<claim>` |
| `rc_recover` | `KAGI_RECOVER` (op 12): Kagi spends the ticket and two custodians unwrap to that recipient. Exactly two, or `rc_recover:custodians`. |
| `rc_unlock` | `BUNDLE_DELIVER`: crypt_block rebuilds the key and verifies the superblock under it |
| `rc_regrant` | `KAGI_REGRANT`: the grant names the recovered node, replacing the device it names (op 7), or granted afresh under its own policy when revoked (op 4) |
| `vr_unwind` | `rc_flush` · `rc_destroy` · `rc_release` in the detach order, then `rc_grant` `ATT_GRANT` on success; the ordinary attach serves it from there |

Any refusal unwinds the same way and ends `failed`. A spent ticket is never
accepted again.

## Secure deletion and retention

`volume_delete.uproc`.

| Phase | Effect |
|---|---|
| `x_pins` · `x_blocked` | `EPOCH_PINS`: any retained snapshot or clone needing the keys refuses the deletion, rescanned every 30 s (`note=snapshot_retained`) |
| `x_quiesce` · `x_wait` | `ATT_DETACH` `reason=deleting;erase=1`: detach runs its order with `d_erase` (FXER) before the instance goes; wait until nothing is held |
| `x_kagi` | `KAGI_ERASE` (Kagi op 14): the grant replaced by an erasure record, every recovery set deleted, the custodians ordered to refuse the resource; the body names the epoch the pin scan covered and the pins found (not for a clone) |
| `x_loam` | `LOAM_DELETE`: unbound under a lease; bodies go to the orphan GC |
| `x_forget` | `VOLUME_FORGET`: identity, key record, PV |

Nothing is destroyed before the device is quiesced; keys go before ciphertext;
keys go only when nothing needs them. Once begun, a deletion is never
abandoned: every step is asked again until it answers. `erased=keys` claims
crypto-erasure; a clone's deletion is `erased=shared_key`.

`ve_retain` reads the attachments with their own `retain` deadline and
CREATEs a deletion (`why=retention`) for an unattached volume past it. A
snapshot's retention is the snapshot pipeline's.

## The typed-effect seam

The request is `1 cid · 2 op · 3 key · 4..8 a1..a5 · 9 n1 · 10 n2 · 254 carry`:

| op | Effect | Arguments |
|---|---|---|
| 1 / 2 / 3 | `LEASE_ACQUIRE` / `LEASE_RENEW` / `LEASE_RELEASE` | vol; fence |
| 10 | `GRAPH_START` | vol, fresh; fence |
| 11 / 12 / 13 | `RECIPIENT` / `BUNDLE_DELIVER` / `NBD_START` | inst |
| 14 / 15 / 16 | `FLUSH` / `NBD_STOP` / `GRAPH_DESTROY` | inst (NBD_STOP also dev) |
| 20 / 21 | `KAGI_GRANT` / `KAGI_REPLACE` | vol, device, custody; tier, lifetime / old device, new device, vol |
| 22 / 23 | `KAGI_RELEASE` / `KAGI_RENEW` | inst, vol, device, epoch, key resource; fence |
| 24 / 25 | `KAGI_CREATE` / `KAGI_RECORD_SET` | vol, tenant, custody; tier, lifetime / vol |
| 30 / 31 / 32 | `LOAM_CREATE` / `VAULT_SPLIT` / `VAULT_DESTROY` | tenant, pvc, size / vol / vol |
| 40 / 41 / 42 | `CLAIM_PUT` / `CLAIM_DELETE` / `UNMOUNT_REQ` | dev, fs, fresh / — / — |
| 43 / 44 | `VOLUME_PERSIST` / `PV_BIND` | vol, fs, device, access, set / tenant, pvc, vol, size, access |

The outcome is `1 cid · 2 op · 3 key · 4 code · 5 fence · 6 lease_exp · 7 exp
· 8 inst · 9 dev · 10 vol · 11 set · 12 grant · 254 carry`. Codes are `ok`,
`ready`, `held`, `lost`, `refused`, `eacces`, `failed` and `unavailable`.

The key and data seams add these (`volume_key_effect.uproc`,
`volume_data_effect.uproc`); their outcome appends `13 n · 14 r1 · 15 r2 ·
16 r3`, and codes add `stale`, `custodians`, `fence_changed`, `expired` and
`conflict`:

| op | Effect | Arguments → result |
|---|---|---|
| 60 / 61 | `KAGI_ROTATE_GRANT` / `KAGI_REVOKE` | vol, device, custody, actor; tier, lifetime / vol, actor |
| 62 / 63 / 64 | `ATT_DETACH` / `ATT_GRANT` / `ROTATION_REQUEST` | reason, erase / device / why, actor |
| 65 | `KAGI_ROTATE_KEY` | vol, from, actor → r1 the new epoch |
| 66 | `KEY_EPOCHS` | epoch, next |
| 67 / 68 / 69 | `CB_STATUS` / `CB_RECIPIENT` / `CB_BUNDLE` | inst, epoch → r1 state, n cursor / — / `ready` · `stale` · `eacces` |
| 70 / 71 / 72 | `EPOCH_PINS` / `KAGI_RETIRE_EPOCH` / `CB_ERASE` | key resource, epoch (`0`: any), claim → n / vol, epoch, actor / inst |
| 73 / 74 / 75 / 76 | `LOAM_SNAPSHOT` / `LOAM_RESTORE` / `CLONE_PERSIST` / `LOAM_SNAP_DELETE` | vol, snapshot → r1 manifest, r2 r3 epochs / manifest, target → r1 clone / clone, key resource, elo, ehi, snapshot / snapshot |
| 77 / 78 / 79 / 80 | `AUDIT` / `KAGI_RECOVERY_AUTHORISE` / `KAGI_RECOVER` / `KAGI_REGRANT` | event, actor, approver, device / vol, epoch, device, actor; ttl → r1 ticket / inst, vol, device, epoch, ticket; fence → n custodians / vol, device, actor, the device the grant names |
| 81 / 82 / 83 | `KAGI_ERASE` / `LOAM_DELETE` / `VOLUME_FORGET` | vol, actor, the key epoch the pin scan covered / vol → `ok` · `held` / — |

`VAULT_SPLIT`, `VAULT_DESTROY` and `KAGI_RECORD_SET` take the epoch as a2.
Lease and graph requests with a5 `recovery` are held by the owner
`rc:<claim>`. Kagi op numbers are the storage-key service's
(`kagi/docs/architecture/storage-key.md`).

The effector has four obligations:

- It answers every request, with all twelve fields present.
- An effect that fails holds nothing new, because the effector undoes its own
  partial work.
- It runs one effect per claim at a time. A later request for a claim
  supersedes an unanswered one, which the effector abandons and undoes.
- The recipient key and the bundle never leave it.

## Graph shape

Each chain is the usual store seam:

```text
store_source (flat, now=1, due=wake, related …)
  → store_effect (step decision, MERGE into its own prefix)
  → decision (vx_act)
  → the effector
  → store_effect (vx_record, MERGE)
```

The attach, second-epoch and detach steps read the same
`/volume-attachments/` source, with `related:
"/volumes/!flat:mount,device;/volume-keys/!flat:kvol,epoch,next"`, and their
reply feeds `vx_act` and `vk_act`. The grant and rotation chains feed `vk_act`;
the snapshot, recovery and deletion chains `vl_act`. A snapshot's related
records are its key's parent (`/volume-attachments/^1`, …); rotation reads the
attachment's `fence#` as an integer. The record effects (ops 40-44, 62-64,
66, 70, 75, 77, 83) are not an effector module but a chain on the effect bus,
`volume_records.uproc`:

```text
effect → store_effect vw_s1 → store_effect vw_s2 → store_effect vw_s3
       → decision vw_answer → the routes
```

Each stage's fused decision builds its store call from the record before it.
A merge into a record is conditional on the revision the stage before read,
so a merge never recreates a record another chain removed. `EPOCH_PINS` is
two COUNTs whose `count_where` predicates (`vw_pin_snap`, `vw_pin_key`) run
over each snapshot and key record. A child that cannot be read or decided
fails the count rather than shortening it. `AUDIT` keys its record by the wall
clock store_effect hands the stage (`wall`) and the effect's cid. Only this
chain uses string builtins: it concatenates the PersistentVolume's name, and
rewrites a claim's `/` as `-`. The claim chain is:

```text
/volumes/ source (related /volume-attachments/!flat:dev)
  → store_effect vc_claim
  → decision vc_act
  → mount actuator
  → store_effect vc_done
```

## Limits

| Name | Value |
|---|---|
| step timeout, retries | 30 s, 5 |
| teardown retry | 5 s |
| lease renew ahead / grace | 20 s / 3 s |
| grant renew ahead | 60 s |
| mount timeout (fsck) | 15 min |
| provision lifetime | 1 s … 86 400 s |
| rotation progress probe | 5 s |
| pin rescan (rotation, deletion) | 30 s |
| recovery ticket | 1 s … 3600 s |
| previous epochs Kagi admits | up to 8, each retired by name |

## What the services must provide

These pipelines stop at the typed-effect boundary. What they ask of the other
projects, and what does not exist there yet:

| Needed | Owner | Today |
|---|---|---|
| FXER on an attach-mode instance zeroes the superblocks under the live lease | Fluxor `crypt_block` | FXER exists; attach-mode behaviour as stated in its README |
| The key epochs a committed root's units use, readable without the key | Loam + `crypt_block` (container metadata in the root page) | not exposed; the pin stays at the key record's epochs until it is |
| `KAGI_REGRANT` over a revoked grant | Kagi | op 4 re-grants over a revoked grant; the effector picks op 4 or 7 |
| A clone's own key resource | Kagi | one grant per resource: a clone shares its source's grant device and pins its epochs until deleted |
| Filesystem freeze before a snapshot | the mount actuator | none; snapshots are crash-consistent |

Provided by Fluxor `crypt_block` (fluxor 60d8256, 82f2a2c): attach-mode
rotation (FXRT announces the next epoch's recipient; its FXSB bundle on
`bundle`), and the FXST status record on the `status` output that
`CB_STATUS` reads — state, rotation state, epoch, previous epoch, the
epoch a bundle is awaited for, and the last errno. A bundle for an epoch
the container no longer needs is refused without failing the mount; the
awaited epoch names the one to offer.
