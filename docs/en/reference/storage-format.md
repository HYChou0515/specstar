# Storage format & on-disk contract

This page documents the **persistence layout** that SpecStar writes — the
SQL schemas, S3 key patterns, on-disk directory shape, and the framed
dump-archive format. It is intended as a forward-compatibility contract:
once a project depends on these shapes (for backups, external readers,
migrations, or operational tooling), SpecStar tries hard not to break
them silently.

> **Stability scope.** This page is the *contract*. Anything described
> here is meant to evolve only through documented, version-gated
> migrations. Anything **not** on this page (internal cache layouts,
> private helper tables, runtime structs that never round-trip to
> storage) is **not** part of the contract and may change without notice.
>
> SpecStar is still pre-1.0. Breaking changes *can* happen, but when
> they do they will:
>
> 1. ship with a runtime migration path (e.g. an `ALTER TABLE` in
>    `_init_postgres_table` or a one-shot loader);
> 2. be called out in `CHANGELOG.md` under a "Storage" heading; and
> 3. be reflected here.
>
> If something here turns out to be wrong, that is a doc bug — please
> file an issue.

---

## Three storage layers

SpecStar splits persistence into three orthogonal stores. Each has a
defined interface and one or more concrete backends:

| Layer | Interface | Holds | Indexed for |
|-------|-----------|-------|-------------|
| Meta store | `IMetaStore` | Current `ResourceMeta` rows (one per resource) | Resource list / search / sort |
| Resource store | `IResourceStore` | All `RevisionInfo` + raw payload bytes (one per revision × schema_version) | Revision history, point reads |
| Blob store | `IBlobStore` | Binary `Binary.data` content keyed by content hash (`file_id`) | Streaming download |

The contract is **per-layer**: a backup created from a Postgres meta
store + S3 resource store + S3 blob store can be loaded into a Disk
meta store + Disk resource store + Memory blob store without surprise.

---

## Common payload encoding

All structured records (`ResourceMeta`, `RevisionInfo`, the user's
resource `data`) are serialised by `MsgspecSerializer` with one of two
encodings, selected per-resource via `add_model(..., encoding=...)` or
the global `configure(encoding=...)`:

* `Encoding.json` *(default)* — `msgspec.json.Encoder(order="deterministic")`.
* `Encoding.msgpack` — `msgspec.msgpack.Encoder(order="deterministic")`.

Both encoders use **deterministic key ordering**: the same input always
produces byte-identical output. This is load-bearing for `data_hash`
and same-content dedup.

A field's bytes-level shape comes from the user's `msgspec.Struct`
definition; `UNSET` fields are omitted, `None` is encoded as JSON
`null` / msgpack `nil`.

---

## `ResourceMeta` — per-resource current row

`ResourceMeta` (defined in `specstar.types`) is the canonical shape;
every meta-store backend serialises **the whole `ResourceMeta` struct**
into a `data` blob (`BYTEA` / `BLOB`), plus a fixed set of *promoted*
columns for indexing and filtering.

Backends agree on the promoted-column set; only types differ
(`TIMESTAMP` vs `REAL`, `JSONB` vs `TEXT`).

### Postgres (`PostgresMetaStore`)

```sql
CREATE TABLE "{table_name}" (
    resource_id        TEXT PRIMARY KEY,
    data               BYTEA NOT NULL,        -- msgspec-encoded ResourceMeta
    created_time       TIMESTAMP NOT NULL,
    updated_time       TIMESTAMP NOT NULL,
    created_by         TEXT NOT NULL,
    updated_by         TEXT NOT NULL,
    is_deleted         BOOLEAN NOT NULL,
    schema_version     TEXT,                  -- nullable: unversioned resources
    indexed_data       JSONB,                 -- @Indexed-marked fields, mirrored for search
    rev_status         TEXT,                  -- mirrors RevisionInfo.status of current revision
    rev_created_by     TEXT,
    rev_updated_by     TEXT,
    rev_created_time   TIMESTAMP,
    rev_updated_time   TIMESTAMP
);

CREATE INDEX "idx_{table_name}_created_time"     ON "{table_name}"(created_time);
CREATE INDEX "idx_{table_name}_updated_time"     ON "{table_name}"(updated_time);
CREATE INDEX "idx_{table_name}_created_by"       ON "{table_name}"(created_by);
CREATE INDEX "idx_{table_name}_updated_by"       ON "{table_name}"(updated_by);
CREATE INDEX "idx_{table_name}_is_deleted"       ON "{table_name}"(is_deleted);
CREATE INDEX "idx_{table_name}_rev_*"            ON "{table_name}"(rev_*);
CREATE INDEX "idx_{table_name}_indexed_data_gin" ON "{table_name}" USING GIN (indexed_data);
-- plus pgvector vector(N) + HNSW indices for Vector fields (created on demand)
```

Each name carries its table (`base_index_name`; past Postgres's 63 bytes the
table part is cut and a digest of the full name keeps it distinct). Up to
0.13.0a4 the names were fixed (`idx_indexed_data_gin`, `idx_is_deleted`, …) —
and an index name shares its schema's relation namespace with every table and
index in it, so in a schema with several tables only the first got them: every
other table's `==` / `in_` filters scanned every row. On boot an existing table
now gets what it lacks: the first table's fixed-name indexes are renamed, the
others' are built with `CREATE INDEX CONCURRENTLY` (the table keeps taking
writes; a failed build is logged, never raised). On a large table that build is
long and runs on the boot path, so build them before upgrading — one statement at
a time, as the table's owner:

```python
import psycopg2
from specstar.resource_manager.meta_store.postgres import base_index_statements

conn = psycopg2.connect(dsn)
conn.autocommit = True  # CONCURRENTLY refuses a transaction
with conn.cursor() as cur:
    for sql in base_index_statements("my_table_meta"):
        cur.execute(sql)
```

The upgraded boot then builds nothing; on the table that held the fixed names it
drops the fixed-name copies the statements were built beside. A build killed midway leaves
an INVALID index under its name, which the boot drops and builds again.

`table_name` defaults to `resource_meta` and is customisable per
manager. **`data` is the source of truth**: if you read `data` and
decode it back to `ResourceMeta`, the promoted columns must agree
(when present) — they exist purely so SQL-level filtering / sorting
doesn't need to decode every row.

### SQLite / disk (`SqliteMetaStore`, `FileSqliteMetaStore`)

Identical column set, SQLite-native types:

```sql
CREATE TABLE resource_meta (
    resource_id        TEXT PRIMARY KEY,
    data               BLOB NOT NULL,
    created_time       REAL NOT NULL,        -- unix timestamp
    updated_time       REAL NOT NULL,
    created_by         TEXT NOT NULL,
    updated_by         TEXT NOT NULL,
    is_deleted         INTEGER NOT NULL,
    schema_version     TEXT,
    indexed_data       TEXT,                 -- JSON
    rev_status         TEXT,
    rev_created_by     TEXT,
    rev_updated_by     TEXT,
    rev_created_time   REAL,
    rev_updated_time   REAL
);
```

Indices match the Postgres set (minus GIN/pgvector).

### Memory / df / redis

`MemoryMetaStore` and `RedisMetaStore` carry the same logical record
(`ResourceMeta` struct serialised to bytes); they do not expose a
schema. A migration *from* memory *to* SQL is just an iteration over
`storage.dump_meta()` followed by `meta_store[rid] = meta` writes.

### Forward-compat rules for meta

* **New columns are additive.** Backends `ALTER TABLE … ADD COLUMN` on
  first connect. Reading an old row that lacks the column yields
  `UNSET` (e.g. `rev_status` on resources created before that field
  existed; backfill via `ResourceManager.backfill_revision_meta()`).
* **Removed columns:** never. If a field becomes obsolete, the
  promoted column stays.
* **Renames:** never (would silently break SQL clients).
* **Type changes:** treated as a hard migration; documented in the
  changelog.

---

## `RevisionInfo` + payload — per-revision row

A *resource* has many *revisions*. For each revision SpecStar stores:

1. `RevisionInfo` — the metadata struct (uid, parent, schema_version,
   data_hash, timestamps, status, …).
2. The **raw payload bytes** of the user's `Struct` (encoded via the
   same `MsgspecSerializer`).

These are stored together (one logical record per revision) and are
addressable by either:

* `uid` (UUID of this exact revision row), or
* `(resource_id, revision_id, schema_version)` triple, where
  `schema_version` may be `None`.

### Postgres (`PostgresResourceStore`)

Two-table layout — a content table keyed by `uid`, plus an index that
maps the human triple to `uid`:

```sql
CREATE TABLE "{prefix}resource_data" (
    uid   TEXT PRIMARY KEY,
    data  BYTEA NOT NULL,         -- msgspec-encoded user payload
    info  BYTEA NOT NULL          -- msgspec-encoded RevisionInfo
);

CREATE TABLE "{prefix}resource_index" (
    resource_id     TEXT NOT NULL,
    revision_id     TEXT NOT NULL,
    schema_version  TEXT,                          -- NULL → stored as '__none__' literal
    uid             TEXT NOT NULL REFERENCES "{prefix}resource_data"(uid),
    PRIMARY KEY (resource_id, revision_id, schema_version)
);

CREATE INDEX ON "{prefix}resource_index" (resource_id);
```

The `__none__` sentinel is an implementation detail of the composite
PK; readers must use `_unsv()` (or treat literal `'__none__'` as NULL)
when reading directly.

### Disk (`DiskResourceStore`)

Two-tier directory layout — a content store keyed by `uid`, plus
"symlinks" (directory entries) keyed by the triple:

```
{rootdir}/
├── store/
│   └── {uid}/
│       ├── data        # raw payload bytes
│       └── info        # msgspec-encoded RevisionInfo
└── resource/
    └── {resource_id}/
        └── {revision_id}/
            └── {p_schema_version}/   # "v_v1", "v_v2", … or "no_ver" for None
                └── uid               # text file containing the uid
```

`p_schema_version` encoding: `"v_{schema_version}"` for a non-null
version, `"no_ver"` when `schema_version is None`.

### S3 (`S3ResourceStore`)

Same logical layout, S3 keys (no fan-out):

```
{prefix}resources/
├── store/{uid}/data
├── store/{uid}/info
└── resource/{resource_id}/{revision_id}/{p_schema_version}/uid
```

### Forward-compat rules for revisions

* `RevisionInfo` evolves additively (new optional fields encoded as
  `UNSET` on old data).
* The triple `(resource_id, revision_id, schema_version)` is stable —
  it appears in URLs and is what `switch` consumes.
* `data` payload format is the user's `Struct` shape, evolved through
  the `Schema` migration graph (see [migrations
  howto](../howto/migrations.md)). Multi-version coexistence is the
  whole point — old payloads stay on disk at their original
  `schema_version` until explicitly migrated.

---

## `data_hash` — content addressing

`RevisionInfo.data_hash` is `"xxh3_128:<32-hex-chars>"` —
`xxhash.xxh3_128_hexdigest(encoded_bytes)`. It's a content fingerprint
of the deterministically-encoded payload bytes, **not** a cryptographic
hash. Same payload → same hash, so two writes of identical content
share a `data_hash`. This drives the same-content dedup behavior
described in [API conventions § Revisions and
mutability](../howto/api-conventions.md#revisions-and-mutability).

---

## Blob store — content-addressed binaries

`Binary` fields are extracted at write time, the bytes are stored
keyed by `file_id` (xxh3-128 hex of the bytes), and the resource
payload retains only `{file_id, size, content_type}`.

### S3 (`S3BlobStore`) keys

```
{prefix}{file_id}                  # the actual blob bytes
{prefix}_sessions/{upload_id}      # upload-session metadata (sharable across pods)
{prefix}_uploads/{upload_id}       # buffered bytes for "proxy" upload mode
```

### Disk / memory

Disk: `{rootdir}/{file_id}`. Memory: dict keyed by `file_id`. Both are
content-addressed and idempotent — re-uploading the same bytes returns
the same `file_id` and does not produce a duplicate object.

### Forward-compat rules for blobs

* `file_id` is xxh3-128 hex — fixed.
* Upload-session keys are a private implementation detail; they live
  under a known prefix (`_sessions/` on S3) so admins can sweep stale
  uploads, but they are not part of the cross-backend contract.

---

## Backup archive — `.acbak`

`ExportRouteTemplate` / `ImportRouteTemplate` (and the global
`/_backup/{export,import}` endpoints) read and write the same framed
msgpack archive format. Each frame is:

```
[4-byte big-endian length: uint32][msgpack payload]
```

The payload is a tagged `msgspec.Struct` (discriminator field `t`),
decoded against this union (`specstar.resource_manager.dump_format`):

```python
DumpRecord = Union[
    HeaderRecord,         # version: int = 2
    ModelStartRecord,     # model_name: str
    MetaRecord,           # data: bytes  (encoded ResourceMeta)
    RevisionRecord,       # data: bytes  (encoded RawResource = RevisionInfo + raw payload)
    BlobRecord,           # file_id, blob_data, size, content_type
    ModelEndRecord,       # model_name: str
    EofRecord,
]
```

Record order in a valid archive:

```
HeaderRecord
ModelStartRecord("issue")
    MetaRecord*               # 0..N resource metas
    RevisionRecord*           # 0..N revisions
    BlobRecord*               # 0..N referenced blobs
ModelEndRecord("issue")
ModelStartRecord("user")
    …
ModelEndRecord("user")
EofRecord
```

The archive is **self-contained**: each `MetaRecord` is a complete
`ResourceMeta`, each `RevisionRecord` carries its own `RevisionInfo`,
and blobs are inlined. Import is duplicate-tolerant via the
`on_duplicate=` strategy (`overwrite` / `skip` / `raise_error`).

### Forward-compat rules for archives

* `HeaderRecord.version` is the format version. Readers must accept
  the same major version and reject newer majors.
* Record types are open for extension: adding a new tag is additive,
  unknown tags must be ignored by tolerant readers. **Removing** a tag
  is a major version bump.
* Frame framing (`uint32-BE length`) is stable.
* Encoder is `msgspec.msgpack.Encoder(order="deterministic")` — the
  same logical archive serialises to the same bytes.

The current version is **`2`**.

---

## What is *not* contract

The following are internal and may change between minor releases:

* Cache layouts (`CachedS3ResourceStore` shadow files, ETag bookkeeping).
* Message-queue table / queue names beyond what `add_route_template`
  documents.
* `fast_slow` meta store's tier-promotion heuristics.
* Internal counters, `__none__` literal in
  `resource_index.schema_version` (use `list_schema_versions` to
  iterate instead of querying SQL directly).
* Any unit-test fixture layout under `tests/`.

If you find yourself depending on something not in this page, please
open an issue so we can promote it to the contract or design an
alternative.

---

## Cross-references

* [How-to: backup & restore](../howto/backup-restore.md) — operational
  use of the archive format.
* [How-to: migrations](../howto/migrations.md) — how payload schema
  evolves across `RevisionInfo.schema_version`.
* [How-to: relationships](../howto/relationships.md) — how `Ref(...)`
  fields surface in `indexed_data` for cross-resource queries.
* [Reference: backend configuration](./backend-configuration.md) —
  picking which backends to mount at which layer.
