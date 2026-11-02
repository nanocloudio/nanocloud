# Decoding `http_chain`'s request envelope into the API plane's record — the
# byte half of what kube_decode does; kube.plan `route` is the other half (the
# path's shape: resource, namespace, name, key).
#
#   [cid u32][method u8][flags u8][target_len u16][hdr_len u16]
#   [body_len u16] target headers body [ peer trailer: [len u16][peer] ]
#
# Out: 1 request id (the cid), 2 method, 6 body,
# 8 bearer credential (scheme stripped; empty for any other scheme), 11 the
# Content-Type (`-` without one: a PATCH's type is its meaning), 15 "1" when
# the Accept header asks for a server-side Table (kubectl get does), 16 the
# writer's field manager, 18 the Nanocloud-Authority-Epoch header, 19 an
# enrollment's `<cellUID>/<envelopeDigest>` (its body), 9 path
# (the target without its query), 10 query, 12 `peer:<peer hex>` (empty
# without a verified peer — the trailer is there only then, past
# `body_len`, where no client can reach), 41..48 the path's segments,
# 51..56 the store-key candidates `route` picks from, 20 the path's group
# between commas (`,<group>,`: what `route` looks up in its list of built-in
# groups, so no decision concatenates for it).
int 4 le ; set 1
int 1 ; set 2
int 1 ; store 7                  # flags (the trailer is present only when set)
int 2 le ; store 0
int 2 le ; store 1
int 2 le ; store 2
load 0 ; taken ; store 3         # the raw request target
load 1 ; taken ; store 4         # the header block
load 2 ; taken ; store 5         # the body
load 5 ; set 6
load 3 ; before "?" ; store 6    # the path
load 6 ; set 9
load 3 ; after "?" ; set 10
load 4 ; hdr "authorization" ; after "Bearer " ; set 8
load 4 ; hdr "content-type" ; str "-" ; coalesce ; set 11
str "1" ; load 4 ; hdr "accept" ; after "as=Table" ; gate ; str "" ; coalesce ; set 15
# The writer's field manager: `fieldManager` from the query, else the
# User-Agent's product (`kubectl/v1.34 …` → kubectl), as upstream names one.
load 3 ; after "?" ; after "fieldManager=" ; before "&" ; load 4 ; hdr "user-agent" ; before "/" ; coalesce ; set 16
# The management plane's authority epoch (`Nanocloud-Authority-Epoch`): the
# fence stage (apiplane_fence.plan) holds a managed credential's writes to it.
load 4 ; hdr "nanocloud-authority-epoch" ; str "" ; coalesce ; set 18
# An enrollment's proof (`managementbindings/<b>/enroll`): its body's
# spec.cellUID and spec.envelopeDigest, `<uid>/<digest>` — the tail of the
# offer key the grant was made under (apiplane_admit.plan ENROLL).
load 5 ; str "spec.cellUID" ; jsonat ; str "" ; coalesce ; str "/" ; load 5 ; str "spec.envelopeDigest" ; jsonat ; str "" ; coalesce ; cat 3 ; set 19
rest ; drop 2 ; store 7          # the verified peer's identity, or nothing
str "peer:" ; load 7 ; hex ; cat 2 ; load 7 ; gate ; set 12
load 6 ; part '/' 1 ; set 41
load 6 ; part '/' 2 ; set 42
load 6 ; part '/' 3 ; set 43
load 6 ; part '/' 4 ; set 44
load 6 ; part '/' 5 ; set 45
load 6 ; part '/' 6 ; set 46
load 6 ; part '/' 7 ; set 47
load 6 ; part '/' 8 ; set 48
# Key candidates, `/<resource>/[<ns>/]<name>`; a create names its object in
# the BODY, so an empty path name falls back to metadata.name (the path wins).
#   /api/v1/<res>[/<name>]                      51
#   /api/v1/namespaces/<ns>/<res>[/<name>]      52
#   /apis/<g>/<v>/<res>[/<name>]                53
#   /apis/<g>/<v>/namespaces/<ns>/<res>[/<name>] 54
# and a grouped resource the store keys by `<res>.<group>` (kube.plan route):
#   /apis/<g>/<v>/<res>[/<name>]                55  /<res>.<g>/<name>
#   /apis/<g>/<v>/namespaces/<ns>/<res>[/<name>] 56  /<res>.<g>/<ns>/<name>
str "/" ; load 6 ; part '/' 3 ; str "/" ; load 6 ; part '/' 4 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 4 ; set 51
str "/" ; load 6 ; part '/' 5 ; str "/" ; load 6 ; part '/' 4 ; str "/" ; load 6 ; part '/' 6 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 6 ; set 52
str "/" ; load 6 ; part '/' 4 ; str "/" ; load 6 ; part '/' 5 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 4 ; set 53
str "/" ; load 6 ; part '/' 6 ; str "/" ; load 6 ; part '/' 5 ; str "/" ; load 6 ; part '/' 7 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 6 ; set 54
str "/" ; load 6 ; part '/' 4 ; str "." ; load 6 ; part '/' 2 ; str "/" ; load 6 ; part '/' 5 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 6 ; set 55
str "/" ; load 6 ; part '/' 6 ; str "." ; load 6 ; part '/' 2 ; str "/" ; load 6 ; part '/' 5 ; str "/" ; load 6 ; part '/' 7 ; load 5 ; str "metadata.name" ; jsonat ; coalesce ; cat 8 ; set 56
str "," ; load 6 ; part '/' 2 ; str "," ; cat 3 ; set 20
finish
