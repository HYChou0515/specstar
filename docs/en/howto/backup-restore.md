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

> **Upload format asymmetry.** `GET /{model}/export` *returns* the archive
> as `application/octet-stream` (so the response body is the raw bytes),
> but `POST /{model}/import` *accepts* the archive as
> **`multipart/form-data`** with a single `file` form field — not as a raw
> body. Sending the export bytes directly as the request body returns
> `422` because the `file` form field is missing.
>
> Correct upload with `curl`:
>
> ```bash
> # export → raw bytes on stdout, save to disk
> curl -o dump.acbak http://localhost:8000/issue/export
>
> # import → multipart, use -F file=@...
> curl -X POST -F 'file=@dump.acbak' -F 'on_duplicate=overwrite' \
>      http://localhost:8000/issue/import
> ```

The important behavior control is `on_duplicate`, which defines how existing records should be handled.

Typical options are:

- `overwrite`
- `skip`
- `raise_error`

Choose the strategy based on whether the target environment should treat the archive as authoritative or only as an additive load.

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

- large archives stream on both ends — `dump()` writes record by record and
  `load()` writes in batches (`batch_size` / `batch_bytes`), so memory is
  bounded by the batch, not by the archive; for multi-GB files prefer
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
