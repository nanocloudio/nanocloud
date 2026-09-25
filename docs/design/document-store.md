# Document store provider

Status: specification for T1.7.2 (E1/S1.7). Owner: **open** (lean: lattice;
alternatives: fluxor as a storage-layer service, or a new sibling).

## Why

`store_effect` and `store_source` hold two things that have different owners:

| Part | What it is | Owner |
|---|---|---|
| Storage adapter | A Chronicle record ↔ fluxor's `storage.object` / `storage.namespace` contracts: key and value parts, the carried context (30..39, 254), GET/PUT/CAS/LIST/WATCH/APPEND/COUNT | Chronicle (the record format's owner) |
| Document engine | JSON documents over that storage: patches, keyed merge, field ownership, stamping, guarded delete, views, framed list/watch, tables | this provider |

Kubernetes is neither. It is **data**: one `schema` param per connector node
(below) and the `patch_keys` / `table` params. Since T1.7.1 (549a2b0) the
engine sources hold no Kubernetes name.

## Operations

A request is a Chronicle record (field map in `modules/app/store_effect/manifest.toml`).

| Op | Code | Meaning |
|---|---|---|
| `CREATE` | 8 | Stamp (`create`, `named`, `generate`) and write if absent; 409 if present |
| `REPLACE` | 9 | Keep (`keep`, `named`), write; conditional on the body's revision at `rv_path` when it states one; 404 if absent |
| `PATCH_MERGE` | 13 | RFC 7386 merge patch, read–patch–conditional-write, retried on a lost race |
| `PATCH_JSON` | 14 | RFC 6902 JSON Patch, same loop |
| `PATCH_SMP` | 15 | Keyed merge over `patch_keys`, with the `smp.*` directives |
| `APPLY` | 16 | Create when absent; otherwise keyed merge with field ownership (conflicts, force, removal) |
| `DELETE` | 4 | Delete, or, when `guard` is non-empty, stamp `deleting` and keep. The object is deleted once its guard empties. |

Every write through the engine is recorded in the ownership list when the
schema states the ownership dialect. The engine answers with the object as
stored (revision stamped) when it fits a record. Otherwise the answer is a GET
registration for the watch side.

**Key marks.** After the key, a `#` introduces `<view>[@[+]<manager>][!]`:
- the view the request reads or writes through;
- the writer's manager, with `+` when an apply forces;
- `!` for a dry run, which computes and answers but stores nothing.

A key's name never holds `#` or `@`.

**The watch side** is the `store_watch` node, reached through registrations
(`/watch-reg/<cid>`):
- LIST streams items between the registration's head and the schema's `list.tail`;
- GET answers one object, through its view;
- WATCH streams the change feed as `event` lines, with `bookmark`s, and ends with `expired` when its cursor falls out of history.

Any of these can instead answer a table (`table` spec, `table.*` envelope).

## Schema directives

`name=value;…`, one `str_chunked` param. Paths are dotted, and `[…]` quotes a
segment that holds dots. Generators are `uuid`, `now`, `name`, or `#<json>`.
Templates have holes: `{{path|default}}` for object values, `{{name}}` and
`{{?name:text with $}}` for values the engine supplies.

| Directive | Used by | Meaning |
|---|---|---|
| `identity=<path>,…` | writer | A patch may not change these paths |
| `create=<path>:<gen>,…` | writer | Stamped on create |
| `generate=<path>` | writer | Name prefix when the key names no object (plus 5 random chars) |
| `keep=<path>,…` | writer | Carried from the stored object across a replace |
| `named=<prefix>\|<path>:<gen>` | writer | For keys `<prefix><name>`, stamped and kept |
| `guard=<path>` | writer | A non-empty list here holds a delete |
| `deleting=<path>:<gen>,…` | writer | What a held delete stamps; the first path is the mark |
| `view.<v>=tpl:<template>` | both | Read through view `v` as a template over the object |
| `view.<v>=text:<prefix>\|<path>` | watch | Read as the text at `<prefix><value at path>` (tail/limit honoured) |
| `write.<v>=<path>,…` | writer | A write through view `v` changes only these paths |
| `rv=<path>` | watch | Where every answered object carries its revision |
| `missing=<json>` | watch | The answer to a GET of nothing |
| `event=<tpl>` | watch | One change: `{{type}}`, `{{object}}` |
| `expired=<json>` | watch | What a watch past its history ends with |
| `bookmark=<tpl>` | watch | A bookmark's object: `{{kind}}`, `{{av}}`, `{{rv}}` |
| `list.tail=<tpl>` | watch | A list's close: `{{rv}}`, `{{continue}}` |
| `table.head` / `table.col` / `table.row` / `table.end` | watch | The table envelope: `{{cols}}`; `{{name}}`, `{{format}}`; `{{cells}}` plus object holes; a non-list close |
| `owners=<path>` | writer | Where the ownership entries live |
| `owners.keys=<m>,<op>,<api>,<time>,<sub>,<fields>` | writer | An entry's member names |
| `owners.ops=<apply>,<update>` | writer | The operation names |
| `owners.api=<path>` | writer | The object's version, recorded per entry |
| `owners.entry=<tpl>` | writer | An entry: `{{manager}} {{op}} {{api}} {{time}} {{sub}} {{fields}}` |
| `owners.marks=<f>,<k>,<v>,<dot>` | writer | Path-element marks: member, keyed element, set element, the element itself |
| `unowned=<path>,…` | writer | Fields no manager owns |
| `conflict` / `conflict.item` / `conflict.cause` | writer | An apply's refusal: `{{count}} {{many}} {{list}} {{causes}}`; per conflict `{{manager}} {{api}} {{field}}` |
| `smp.patch` / `smp.retain` / `smp.delete` / `smp.prefix` | writer | The keyed-merge directives' names |

Table cells (`table` spec, `Kind=Header[/format]:<cell>,…;*=…`) are
`path|default`, or literal text mixed with these functions:

| Function | Result |
|---|---|
| `@v(path\|default)` | The value at path |
| `@n(path)` | How many elements the list at path has |
| `@n(path?k=v)` | How many elements have member k equal to v |
| `@sum(path:k)` | The sum of member k over the elements |
| `@age(path)` | Time since the RFC 3339 timestamp at path |

The Kubernetes statement of all of this is in
`packaging/debian/fluxor-apiplane.yaml` (`se_probe`, `ws_evt`).

## Bounds

| Bound | Value | Past it |
|---|---|---|
| Object | 3 MiB | Refused |
| Tracked object | 512 KiB | managedFields carried as stored |
| Managers per object | 16 | Same |
| Field arena | 48 KiB | Same |
| Schema | 2560 bytes (writer), 2048 bytes (watch) | Refused whole |
| Table spec | 2048 bytes | Refused whole |

A keyed element names one schema key; upstream names a container port by
`containerPort` and `protocol`.

## What moving it takes (T1.7.3)

1. The owner takes `_shared/{json,jsonpatch,docschema,fieldset,table,views}.rs`
   and the engine parts of `store_effect` / `store_source`. It proves them
   standalone in its own repo, as connectors.md asks of every provider:
   patches, apply conflicts, views, list/watch framing and tables, against a
   schema of its own choosing.
2. Chronicle takes the storage adapter (the rest of `store_*`), with the same
   module names and wire.
3. nanocloud repins both. Its graphs keep the Kubernetes schema, and its E2Es
   (`kubectl-patch`, `apiplane*`, `kubectl-watch`) stay the acceptance suite.
