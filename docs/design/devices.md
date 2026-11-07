# Devices: claim tokens and the device signer

A customer's `Device` (`nanocloud.io/v1`, namespaced) is a microcontroller the
customer adopts into a namespace: it gets an identity, a certificate for its own
key, and a place on the namespace's MQTT broker. This is the contract Acorn's
managed cells expose (Acorn `docs/design/nanocloud-cells.md`); `device_pki`
(`modules/app/device_pki`) implements it, and its manifest is the wire contract.

```yaml
apiVersion: nanocloud.io/v1
kind: Device
metadata: {name: sensor-1, namespace: workshop}
spec:
  class: pico2w                      # a customer Device: adopted by claim
  wifi: {secretRef: {name: workshop-wifi}}   # Secret type nanocloud.io/wifi
  certificate:
    lifetime: 24h                    # the default; clamped by the cell
    maxOffline: 720h                 # optional; unset = re-enroll however late
  revoked: false                     # true: no redemption, no re-enrollment
status:
  phase: Enrolled                    # Claimable, Enrolled, Revoked
  identity:
    spiffeId: spiffe://<td>/device/workshop/sensor-1
    keyFingerprint: sha256:<hex>     # of the 65-byte P-256 point
    certificateNotAfter: "2026-12-07T18:00:30Z"
    certificateLifetime: 24h         # what the certificate issued lasts
  mqtt:
    broker: <device_pki's `broker` param>
    topicPrefix: t/<16 hex>/devices/sensor-1/
  conditions: [{type: Claimed, status: "True"}, {type: CertificateValid, status: "True"}]
```

The status never carries the token, the private key or the certificate itself.
`<16 hex>` is the start of the SHA-256 of the namespace's `metadata.uid`: unique
across namespaces and cells, naming neither.

## Adoption

1. **Claim.** Within a second of a Device with a `spec.class` appearing,
   `device_pki` mints a claim token `<id>.<secret>` (the bootstrap-token shape,
   `[a-z0-9]{6}.[a-z0-9]{16}`, from the CSPRNG), good for 24 hours. The offer it
   is checked against, `/device-claims/<id>`, holds the secret only as a hash and
   names the Device's namespace, name and uid. The Device reads `Claimable`.
2. **Firmware.** `GET …/devices/<name>/firmware` is to return the class's signed
   image and a config block: the WiFi settings from the Secret `spec.wifi`
   names, the token, the enrollment URL, the cell's CA and the broker. Today it
   answers **501**, saying why (below).
3. **Redeem.** On first boot the device makes a P-256 key and sends one request:

   ```http
   POST /apis/certificates.k8s.io/v1/certificatesigningrequests
   Authorization: Bearer <id>.<secret>

   {"apiVersion":"certificates.k8s.io/v1","kind":"CertificateSigningRequest",
    "metadata":{"name":"device-<id>"},
    "spec":{"signerName":"nanocloud.io/device","request":"<base64 PEM PKCS#10>"}}
   ```

   The 201 carries `status.certificate` (base64 PEM) and an `Approved`
   condition. The identity comes from the offer, never from the request; the
   request's own signature proves the key. The first key to redeem a token
   binds it: another key under it is 403.
4. **Retry.** The same request again is 409 `AlreadyExists`, and
   `GET …/certificatesigningrequests/device-<id>` under the same token reads the
   CSR back. A device that lost the answer does that.
5. The Device reads `Enrolled` and the token is emptied from the plane.

A Device deleted and created again under its name is a new uid: it is minted a
new token and the earlier one is withdrawn (401).

## Lifetime, and never stranded

A certificate lasts the Device's `spec.certificate.lifetime` (a duration:
`24h`, `720h`, `90m`), a day when unset, clamped to the cell's `device_pki`
`min_lifetime_s`/`max_lifetime_s` (an hour and 90 days by default), and asked of
the authority per request (kagi's `lifetime_s`, itself clamped by the
authority's own bounds). `status.identity.certificateLifetime` reports what the
certificate issued actually lasts, read from it. Industrial users want it short;
a consumer device may sit unpowered for weeks.

So expiry never strands a device. A device **re-enrolls** with one request
signed by the key it enrolled, and needs neither a token nor a valid
certificate:

```http
POST /apis/certificates.k8s.io/v1/certificatesigningrequests

{"metadata":{"name":"device-<first 16 hex of SHA-256(point)>"},
 "spec":{"signerName":"nanocloud.io/device","request":"<base64 PEM PKCS#10, same key>"}}
```

It is answered 201 with a fresh certificate when the request's signature proves
the key, the key is the one the Device records (`/device-keys/<fp>` names the
Device, its claim names the key), the Device is the same uid, and it is not
revoked (`spec.revoked`). With `spec.certificate.maxOffline` set, it is refused
(403) once that long has passed since the last certificate expired: the
industrial choice, where a device gone that long is to be adopted again. Unset,
a device re-enrolls however late. Nothing is kept per re-enrollment but the
claim's facts, so a lost answer is the same request sent again.

**Renewal** is the same request while the certificate is valid, and a device
may present that certificate to the API's `tls` as it sends it (the listener
asks for one and requires none, `verify_peer: 2`): verified against the cell's
authority, it is accepted beside the request's own proof of the key, which is
what decides. An expired certificate is refused at the handshake, and the device
re-enrolls without one.

**Why this is safe.** Whoever holds the key could renew while the certificate was
valid anyway: a certificate adds nothing to the proof the key itself gives, so
requiring an unexpired one would only strand honest devices, not stop a thief.
What bounds a lost or revoked device is elsewhere. Revocation (`spec.revoked`,
or deleting the Device) stops every re-enrollment at once. The broker checks only
the CA and the expiry and is told of no revocation, so a revoked device keeps
broker access until its current certificate expires, and no longer: the
lifetime is that bound, and a short one makes it tight. `maxOffline` bounds the
other case, a key found in a device that was shelved: past it, the key buys
nothing.

| Answer | When |
| --- | --- |
| 201 | issued |
| 400 | `spec.request` is not a P-256 PKCS#10, or its signature does not verify |
| 401 | not a claim token, unknown or withdrawn, a wrong secret, or expired before it was bound |
| 403 | another key under a bound token; a signer other than `nanocloud.io/device`; a name other than `device-<id>` (re-enrollment: `device-<fp16>`); the Device recreated or revoked; another CSR read under the token; a re-enrollment by a key the Device does not record, or past `maxOffline` |
| 409 | issued already under this token and key |
| 503 | the store or the authority did not answer; the same request again finishes it |

A node's own link-end Devices (namespace `nanocloud-peers`, written by
`node_pki`) keep the older path: naming `spec.publicKey` is enough there, because
only the join's ServiceAccount may write them. Anywhere else a named key is
certified by nothing (`device.plan`).

`scripts/device-adoption-e2e.sh` proves all of the above on the packaged control
plane with openssl as the device, the lifetime by the certificates issued (a
day by default, the floor and the ceiling), `maxOffline` by setting the claim's
recorded `notAfter` in the past, and expiry for real: with the floors lowered in
its graph, a 90 s certificate renewed under itself over mTLS, then refused at the
handshake once expired, and its key re-enrolled. `scripts/device-e2e.sh` proves
the `nanocloud-peers` path.

## Not yet

- **Firmware (501).** Fluxor now gives a pico2w a durable vault (`vault_store`
  on the flash data store), a network clock (`sntp` on RP2350) and an MQTT client
  over TLS with a vault-held identity (`examples/mqtt_publisher/pico2w_tls.yaml`).
  What it does not give is the proof this signer takes: `tls` asks for its
  identity's certificate with the bare public key (`identity_req`), and a vault
  key is its module type's alone, so nothing on the device can sign the PKCS#10.
  The image waits on `tls` emitting a certificate request signed by that key
  (or the vault lending the key's signature to a named module). Then: a device
  graph with an adoption client (the claim token as Bearer, the CSR, the answer
  on `identity_cert`), its config in the RP parameter store, and the
  subresource writing that sector's UF2 blocks beside the class's image. It is
  a read with no side effects, under its own RBAC resource `devices/firmware`.
- **The token sealed.** `/device-claim-of/<ns>/<name>` keeps the token in the
  control-plane store until redeemed, as an enrollment grant is kept, readable
  by the plane's own graph alone; it is to be KEK-sealed when the firmware
  subresource that reads it exists.
- **Cleanup.** A deleted Device's offer, claim and key index are not swept (a
  recreated one is a new uid, which they do not match).
- **kubectl.** `/apis` lists `certificates.k8s.io`, so stock kubectl maps the
  kind: `kubectl --token <claim token> create -f csr.json -o
  jsonpath='{.status.certificate}'` redeems (proved in the e2e).
