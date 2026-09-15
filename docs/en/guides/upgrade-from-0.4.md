# Upgrading from autocrud 0.4.x (data migration)

`autocrud` 0.4.x and today's `specstar` do not share a storage format — the
on-disk layout, the meta records and the backup archive all changed between
0.4 and 0.9. A specstar process pointed at a 0.4.x data directory sees
nothing. The supported way across is **export on 0.4, import on specstar**:

1. `autocrud==0.4.6` — a patch release of the old line whose only job is a
   working `AutoCRUD.dump()` that writes specstar's `.acbak` archive.
2. `specstar` — `SpecStar.load()` imports that archive as-is.

Nothing in your 0.4.x data directory is modified or deleted by this
procedure; it stays as the rollback point until you remove it yourself.

> **Why a 0.4.6 at all?** `AutoCRUD.dump()` raised
> `TypeError: Missing required argument 'result'` on every 0.4.x release,
> so there was no way to get data out. 0.4.6 fixes that and targets the
> current archive format directly, so no intermediate versions are needed.

---

## Quick checklist

- [ ] Back up the 0.4.x data directory (`cp -r`)
- [ ] `pip install autocrud==0.4.6` **in the old environment**, run `crud.dump()`
- [ ] Create the specstar project: new imports (`MIGRATION.md`), **new empty storage**
- [ ] `spec.load()` the archive, check the returned `LoadStats`
- [ ] Verify counts / revisions / soft-deletes, then switch traffic
      (big store? full export first, delta at cutover — see *Large deployments*)
- [ ] Keep the old directory until you are sure

---

## 1. Export on the old side

Work in the environment that runs your 0.4.x app today. Your application
code does not change; only the package version does.

```bash
cp -r ./data ./data.bak-0.4        # whatever DiskStorageFactory("./data") points at
pip install autocrud==0.4.6
```

```python
# export.py — same models, same storage as your app
from myapp import crud             # your AutoCRUD instance, models already add_model()'d

with open("backup.acbak", "wb") as f:
    crud.dump(f)
```

`dump()` walks every registered model and writes every resource — soft-deleted
ones included — with its complete revision history. The archive is
self-contained; nothing else from the old deployment is needed.

!!! note "Payload encoding"
    `crud.dump(f, encoding="msgpack")` is available if the specstar side
    stores data as msgpack (`add_model(..., encoding="msgpack")`). The
    default, JSON, matches specstar's default. Pick the one the *importing*
    side uses; `data_hash` is recomputed over the emitted bytes either way.

!!! warning "`migration=` on 0.4.x"
    Revisions are read through the normal 0.4.x store path. If a model was
    registered with `migration=`, revisions stored at an older
    `schema_version` are migrated on read **and rewritten on disk** — the
    same thing a plain `get()` on 0.4.x does. This is why the backup copy
    comes first.

---

## 2. Set up the specstar side

```bash
pip install -U specstar
```

Apply the rename from [MIGRATION.md](https://github.com/HYChou0515/specstar/blob/master/MIGRATION.md):
`from autocrud import …` → `from specstar import …`, `AutoCRUD()` →
`SpecStar()`, the global `crud` → `spec`.

Two things to get right before importing:

- **Point storage at a fresh location.** `DiskStorageFactory("./data-specstar")`,
  a new Postgres schema, an empty bucket prefix — anything but the 0.4.x
  directory. specstar's `DiskStorageFactory` writes a different layout and
  must not be aimed at the old tree.
- **Register the same models under the same names.** The archive is keyed by
  resource name (`ticket`, `user-profile`, …). 0.4.x and specstar derive
  the name the same way (kebab-case of the class name by default), so if you
  did not pass `name=` or change `model_naming`, nothing to do. If you did,
  mirror it. `load()` refuses an archive that names a model the target has
  not registered.

Field shapes must decode: the payload bytes are exactly what 0.4.x wrote
for your `Struct`, so if the class is unchanged, the data is unchanged. If
you want to evolve the model as part of the move, import first, then
register the new shape with a `Schema(...)` whose first step has source
`None` — imported revisions carry `schema_version=None` (0.4.x without
`migration=` never set one). See
[Migrating from unversioned data](../howto/migrations.md#migrating-from-unversioned-schema_versionnone-data).

---

## 3. Import and verify

```python
from myapp import spec             # your SpecStar instance

with open("backup.acbak", "rb") as f:
    stats = spec.load(f)
print(stats)
# {'ticket': LoadStats(loaded=3, skipped=0, total=3), 'note': LoadStats(loaded=2, skipped=0, total=2)}
```

`load()` defaults to `on_duplicate=OnDuplicate.overwrite`; pass
`OnDuplicate.skip` or `OnDuplicate.raise_error` if the target is not empty.
The `/_backup/import` route accepts the same file if you would rather upload
it (see [Backup and restore](../howto/backup-restore.md)).

Then check, per model:

```python
from specstar.query_types import ResourceMetaSearchQuery

mgr = spec.get_resource_manager("ticket")
mgr.count_resources()                                             # every resource, deleted ones too
mgr.count_resources(ResourceMetaSearchQuery(is_deleted=False))    # live ones
mgr.list_revisions("ticket:1")                                    # full history is there
mgr.get("ticket:1").data                                          # decodes with your Struct
mgr.get_meta("ticket:3").current_revision_id                      # a switch() on 0.4 is honoured
```

What the archive carries, and what you should therefore see:

| On 0.4.x | After import |
| --- | --- |
| Every revision of every resource | `list_revisions()` identical, `parent_revision_id` chain intact |
| `switch()`-ed current revision | `current_revision_id` as it was, later revisions still readable |
| Soft-deleted resources | Still deleted: `get()` raises `ResourceIsDeletedError`, `get_meta(include_deleted=True)` works |
| `created_by` / timestamps | Unchanged, on metas and revisions |
| `indexed_fields` values | Present in `ResourceMeta.indexed_data` |
| `data_hash` | Identical when the encoding matches (JSON in, JSON out) |
| *(didn't exist)* `rev_*` mirror fields | Filled by the exporter — **no `backfill_revision_meta()` needed** |

New writes continue the imported history (`update()` on an imported
resource gets `parent_revision_id` = the imported current revision and
bumps `total_revision_count`).

---

## Large deployments

Both sides stream. `dump()` writes one record at a time, and `load()`
reads one frame at a time and writes in batches (`batch_size=1000`
records / `batch_bytes=64 MiB` by default), so neither needs the archive —
or the largest model — to fit in memory. What changes at scale is the
*downtime*: a full export of a big store takes long enough that the old
service keeps taking writes meanwhile. Do it in two steps:

```python
# Step 1 — full export while 0.4.x keeps serving. Note the START time.
import datetime as dt
from autocrud.types import ResourceMetaSearchQuery

t0 = dt.datetime.now()   # same kind (naive / aware) as the `now` your app passes to meta_provide()
with open("full.acbak", "wb") as f:
    crud.dump(f)
```

Import `full.acbak` into specstar and verify at leisure (§3). Then, at
cutover, stop writes on 0.4.x and export only what moved since `t0`:

```python
# Step 2 — delta: resources touched since t0, each with its whole history
with open("delta.acbak", "wb") as f:
    crud.dump(f, query=ResourceMetaSearchQuery(updated_time_start=t0))
```

```python
spec.load(open("delta.acbak", "rb"))           # default on_duplicate=overwrite
```

- `query` selects **resources**, and a selected resource is exported with
  **all** its revisions, so re-importing it over the full import is an
  idempotent overwrite — `total_revision_count`, `current_revision_id`
  and the history always agree.
- `updated_time` moves on `update`, `patch`, `switch`, `delete` and
  `restore`, so a resource that was only soft-deleted or switched after
  `t0` is in the delta too.
- Take `t0` from *before* the full export started, not after it finished:
  a write that lands while the export is running may or may not be in
  `full.acbak`, and the overlap makes that irrelevant.
- `query` accepts every `ResourceMetaSearchQuery` field
  (`created_time_*`, `created_bys`, `is_deleted`, …); `limit`/`offset`
  are ignored.

Two more knobs:

- **Compression.** Both ends only need `write()` / `read(n)`, so
  `crud.dump(gzip.open("full.acbak.gz", "wb"))` and
  `spec.load(gzip.open("full.acbak.gz", "rb"))` work as-is; JSON payloads
  shrink a lot.
- **HTTP vs Python.** `POST /_backup/import` streams the upload from its
  spooled temp file, but for multi-GB archives prefer the Python API on
  the host — no request timeouts, no proxy body limits.

---

## 4. Cut over

Switch traffic to the specstar process. Leave `data.bak-0.4` and
`backup.acbak` in place until you have run long enough to trust the new
deployment; deleting them is the only irreversible step in this guide.

---

## Which 0.4.x setups are covered

`dump()` goes through the `IStorage` interface, so any backend combination
should export. Two are exercised by tests against the real 0.4.6 package:

- `DiskStorageFactory` — the setup the 0.4.x docs recommended.
- A hand-composed **local SQLite meta store + disk revisions**
  (`SimpleStorage(FileSqliteMetaStore(...), DiskResourceStore(...))`), the
  setup behind #448. The delta query (`updated_time_start=`) runs as SQL
  on the SQLite side; the archive it produces is identical in shape.

Other hand-composed 0.4.x backends (`PostgresMetaStore`, `RedisMetaStore`,
`S3ResourceStore`) are untested — please
[open an issue](https://github.com/HYChou0515/specstar/issues) with the
combination if you hit a problem.

On the **specstar side** the archive is backend-agnostic; the import is
tested into memory, disk, SQLite-meta and a live `PostgresStorageFactory`
(Postgres meta + Postgres revisions, `tests/test_postgres_legacy_load.py`).
`PostgresStorageFactory` defaults its *store* encoding to msgpack — that
governs how it serialises its own meta / info rows and is independent of
the payload encoding, which follows `SpecStar(encoding=...)` (JSON by
default, matching `crud.dump()`'s default).

## Not covered
- **0.5 – 0.8.2 tar archives**: those versions had a working tar-based
  `dump()`; specstar does not read it. Upgrade to 0.8.3+ first (framed
  archive) or open an issue.

## See also

- [MIGRATION.md](https://github.com/HYChou0515/specstar/blob/master/MIGRATION.md) — the package rename
- [Backup and restore](../howto/backup-restore.md) — `.acbak` archives and the import routes
- [Storage format](../reference/storage-format.md) — the archive contract `load()` reads
- [Schema migration](../howto/migrations.md) — evolving the `Struct` shape after import
