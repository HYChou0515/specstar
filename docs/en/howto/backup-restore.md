# Backup and Restore

SpecStar includes export and import workflows so you can move data between environments, preserve demo data, or prepare a disaster recovery process.

This guide covers the practical side of using those features.

---

## Two backup paths

SpecStar supports two main approaches:

### Per-model export and import

Use this when you only want selected resource types.

- `GET /{model_name}/export`
- `POST /{model_name}/import`

This is useful for:

- moving one resource family between environments
- migrating a subset of data
- debugging or reproducing specific datasets

### Global backup and restore

Use this when you want to move or restore a larger environment snapshot.

Typical routes include:

- `GET /_backup/export`
- `POST /_backup/import`

This is useful for:

- full-environment migration
- restoring a test or staging environment
- disaster recovery drills

---

## Per-model export

Per-model export streams an `.acbak` archive.

Because the export path supports the same filtering ideas as list and search, you can narrow the exported dataset before moving it.

That means you can export:

- all resources of a model
- only active resources
- only resources matching a query
- only a time window of changes

---

## Per-model import

Import accepts an `.acbak` archive and loads records back into the datastore.

> **Upload format.** `POST /{model}/import` accepts the archive **either**
> as `multipart/form-data` with a single `file` form field **or** as a raw
> `application/octet-stream` body — so the bytes from
> `GET /{model}/export` round-trip directly.
>
> ```bash
> # export → raw bytes on stdout, save to disk
> curl -o dump.acbak http://localhost:8000/issue/export
>
> # import → multipart
> curl -X POST -F 'file=@dump.acbak' \
>      'http://localhost:8000/issue/import?on_duplicate=overwrite'
>
> # import → raw body (round-trips with export)
> curl -X POST --data-binary @dump.acbak \
>      -H 'Content-Type: application/octet-stream' \
>      'http://localhost:8000/issue/import?on_duplicate=overwrite'
> ```
>
> The **global** route is stricter: `POST /_backup/import` takes only the
> multipart form field, and a raw body returns `422` with
> `body.file: Field required`.

The important behavior control is `on_duplicate`, which defines how existing records should be handled.

Typical options are:

- `overwrite`
- `skip`
- `raise_error`

Choose the strategy based on whether the target environment should treat the archive as authoritative or only as an additive load.

---

## Knowing the archive is whole

A backup whose failures are quiet is worse than no backup, because the
discovery happens on restore day. Two guards make that impossible:

**A dump refuses to produce a short archive.** `dump()` runs in
`strict=True` mode by default: a referenced blob the store will not return,
or a revision whose payload will not decode (and which therefore
contributes none of the blob ids it references), raises
`DumpIncompleteError` naming what it could not read. Both used to be
skipped in silence.

If exporting what *is* readable is the right call — salvaging from a
damaged store, say — pass `strict=False` and read the stats:

```python notest
stats = spec.dump(open("backup.acbak", "wb"), strict=False)
for model, s in stats.items():
    if not s.complete:
        print(model, "missing blobs:", s.skipped_blobs)
        print(model, "undecodable revisions:", s.undecodable_revisions)
```

`DumpStats` also carries `metas`, `revisions` and `blobs` counts.
`complete` is the one question a backup script should ask.

**A load refuses a truncated archive.** Every archive ends with an
end-of-stream record. If the bytes run out before it — a dump that died,
a transfer that stopped, a strict failure part-way — the archive is
refused: a cut at a record boundary raises `ArchiveTruncatedError`, a cut
in the middle of a record is caught by the frame reader itself
(`ValueError`), and both are a `400` on the import routes. Loading is not
transactional, so batches already written stay written; the
`ArchiveTruncatedError` carries the per-model counts that *were* applied,
rather than pretending to undo them.

This is what makes a strict dump safe to keep: the partial file it leaves
behind cannot later be restored as though it were whole.

---

## Who may run a backup

`dump` and `load` are actions like any other — `ResourceAction.dump`,
`ResourceAction.load`, grouped as `ResourceAction.backup` — checked by
your `permission_checker`. All four backup routes resolve the request's
user through the same `DependencyProvider` as every other generated
route, so the check is against the caller and a refusal is a `403`.

Two things follow that are easy to get wrong:

- **The default `permission_checker` is `AllowAll()`.** Out of the box the
  backup routes are as open as every other route. `configure(admin=...)`
  or a custom `IPermissionChecker` closes them; gate on
  `ResourceAction.backup` to allow or deny the whole path at once.
- **`access_scope` does not fence a backup.** It restricts reads and
  request-writes, but `dump` is a privileged, whole-model operation that
  reads through storage directly. A user allowed to `dump` gets every row
  of that model, not the rows their scope would show them. Grant the
  backup actions to operators, not to end users.

Calling the library directly, pass the user the same way:

```python notest
stats = spec.dump(open("backup.acbak", "wb"), user="operator")
```

---

## Typical migration workflow

A common promotion or migration flow looks like this:

1. keep the source environment running
2. export the required data
3. start the target environment with the correct storage backend
4. import the archive
5. verify references, revision history, and blob accessibility

This works well when moving from:

- in-memory demo storage to disk
- local development to staging
- simple deployment to a production-grade backend

---

## What to verify after restore

After loading a backup, confirm that:

- the expected resource counts are present
- references still resolve correctly
- binary attachments remain available if blobs are involved
- search results still behave as expected
- revision history is intact where relevant

Treat restore validation as part of the process, not as an optional extra step.

---

## Operational advice

- large archives stream on both ends — `dump()` writes record by record,
  both export routes send frames as they are produced, and `load()`
  writes in batches (`batch_size` / `batch_bytes`), so memory is bounded
  by the batch, not by the archive. `SpecStar.iter_dump()` is the same
  archive as an iterator of byte chunks (it is what `GET /_backup/export`
  returns); use it when the destination is a pipe or an uploader rather
  than a file
- one blob is still one record, so peak memory is at least the size of the
  largest single attachment; for multi-GB files prefer
  `spec.load(open(...))` on the host over an HTTP upload
- test restore regularly instead of assuming the archive is enough
- choose `overwrite` carefully in shared environments
- use per-model export when you want a safer, narrower migration scope
- use full backup routes when you need environment-level recovery
- pair backup strategy with your blob storage and persistent backend choices

---

## Relationship to production rollout

Backup and restore are especially important when you move beyond in-memory development setups.

If your deployment needs durable storage, background jobs, and binary uploads, backup procedures should be part of the operational checklist from the start.

---

## Related pages

- [From demo to production](/specstar/guides/from-demo-to-production)
- [Routes generation](/specstar/howto/routes)
- [Binary data](/specstar/howto/binary-data)
- [Examples](/specstar/examples/)
