# Counter-intuitive behaviors & gotchas

Quick reference for things that **look like a bug but aren't**. Each entry
explains the "intuitive guess" first, then what SpecStar actually does, and
links to the relevant guide.

If a behavior here genuinely bites you, please open an issue — most of
these are 0.x trade-offs that may sharpen up before 1.0.

---

## Reads / response shape

### `GET /{model}/{id}` returns an envelope, not the bare resource

| Intuitive guess | Actual behavior |
|-----------------|-----------------|
| `GET /user/123` → `{name: "...", email: "..."}` | `GET /user/123` → `{"data": {...}, "revision_info": {...}, "meta": {...}}` |

The envelope is intentional: the same endpoint can return data, revision
info, and metadata together. To get the bare data **object**:

* `GET /user/123/data`, or
* `GET /user/123?returns=only-data` — the `only-*` selector unwraps the
  section. (Plain `?returns=data` still wraps it as `{"data": {...}}`.)

To change the default shape for every GET, set `default_get_returns`
(e.g. `spec.configure(default_get_returns="only-data")`).

Note the asymmetry: `POST` / `PUT` return a **flat** `RevisionInfo` (top-level
`resource_id`, `revision_id`, `created_by`, …) — a different shape from this GET
envelope. A client reads the new id from the flat write response, then reads the
resource back through the envelope.

See [API conventions](./api-conventions.md#canonical-read-api-get-modelresource_id).

### `?partial=true` on a list returns `data: {}` for every row

`partial` is a **field selector** (slash-prefixed paths), not a boolean.
`?partial=true` is normalised to the path `/true` which matches no field in
your struct, so the projector strips data down to an empty object (a
boolean-looking `partial` value also emits a `SpecStarWarning`). Use
slash-prefixed field names instead:

```
GET /user?partial=/name&partial=/email
```

See [API conventions § `partial`](./api-conventions.md#field-projection-partial-partial).

### Three id fields in every response

| Field | Where | Use for |
|-------|-------|---------|
| `resource_id` | `meta.resource_id` | URL paths, stable across revisions |
| `revision_id` | `revision_info.revision_id` | `switch`, revision-targeted reads |
| `uid` | `meta.uid` | **Internal — do not put in URLs** (differs per revision) |

---

## Lists & filters

### Bare `?field=value` query params are silently ignored

| Intuitive guess | Actual behavior |
|-----------------|-----------------|
| `GET /task?name=Alice` returns Alice's tasks | Returns *all* tasks; `name` is silently dropped |

Filtering goes through the structured `qb` DSL or `data_conditions` /
`conditions`. See [Query Builder](./query-builder.md).

### Default page size is effectively unlimited until you set it

`SPECSTAR_DEFAULT_QUERY_LIMIT` defaults to `2**32 - 1`. Set a sane limit
(e.g. `1000`) for production. Per-request `?limit=` overrides it.

### `limit=0` returns zero rows, not "all rows"

`limit` is the literal page size. `0` means "give me nothing." For all
rows, omit `limit` (or set it to a large finite number).

---

## Writes

### Unknown fields are silently dropped (until you opt in)

By default, `POST` / `PUT` with extra keys returns `200` and the extras
are gone. A typo'd field = lost data. Opt into strict mode:

```python
SpecStar(forbid_unknown_fields=True)
# or
spec.configure(forbid_unknown_fields=True)
```

With it on, unknown fields return `422`. See [API
conventions § Strictness](./api-conventions.md#strictness-unknown-fields-on-write).

### `resource_id` cannot be set through the request body

```
POST /note  {"title": "a", "resource_id": "my-id"}   # 422
PUT /note/<id>  {"title": "a", "resource_id": "x"}   # 422
PATCH /note/<id>  [{"op":"replace","path":"/resource_id","value":"x"}]  # 422
```

`resource_id` is server-generated at creation and immutable afterwards, so
it never belongs in `POST` / `PUT` / `PATCH` bodies. SpecStar rejects it with
`422` rather than silently dropping it (the previous behavior — see
[CHANGELOG](https://github.com/HYChou0515/specstar/blob/master/CHANGELOG.md)).
To customise how ids are generated, pass `id_generator=` to
`spec.add_model(...)`.

The one exception: if your Struct *legitimately* declares a field named
`resource_id`, the guard steps aside and treats it as ordinary data.

A **custom** id supplied programmatically (`rm.create(data, resource_id=...)`
or a custom `id_generator`) must be safe in file paths **and** URLs: ids with
`/`, `\`, or control characters are rejected with a `ValidationError`. A `/`
would otherwise break disk storage (the id is used as a filename) and make the
resource unreachable over HTTP — `/{model}/{resource_id}` is a single path
segment, so `GET /model/a/b` (and even `a%2Fb`) returns 404. Stick to letters,
digits, `:`, `-`, `_`.

### `PUT` is full-replace, not upsert

| Intuitive guess | Actual behavior |
|-----------------|-----------------|
| `PUT /user/unknown-id` creates the user | Returns `404` — `PUT` requires the resource to exist. Use `POST /user` to create. |

### `PATCH` accepts two flavors — pick by shape (array = ops, object = merge)

`PATCH` understands **both** REST patch standards on the same endpoint:

```
PATCH /user/123  [{"op":"replace","path":"/name","value":"new"}]   # RFC 6902 JSON Patch (array)
PATCH /user/123  {"name": "new"}                                   # RFC 7386 Merge Patch (object)
```

A JSON **array** is treated as RFC 6902 operations; a JSON **object** is a
RFC 7386 merge patch (partial update; `null` deletes a field). Set
`Content-Type: application/json-patch+json` or `application/merge-patch+json`
to be explicit. See [API conventions § PATCH](./api-conventions.md#patch-two-flavors).

### Same-content writes are de-duplicated

A `PUT` that produces byte-identical content as the current revision
returns `200` but **does not create a new revision**. Compare the
returned `revision_id` to the prior one to detect a dedup. See [API
conventions § Revisions and mutability](./api-conventions.md#revisions-and-mutability).

### `DELETE` returns `200 + meta body`, not `204 No Content`

Useful when callers want the post-delete metadata (e.g. to confirm soft
delete, capture timestamps). If you only care about success, just check
`status_code < 300`.

### Soft-deleted resources return `410 Gone`, not `404`

Distinct from "never existed" `404`. Pass `?include_deleted=true` to
read them through the same endpoints.

### Programmatic `list_resources()` includes soft-deleted rows (HTTP excludes them)

The two layers disagree on the default:

| Call | Soft-deleted included? |
|------|------------------------|
| `rm.list_resources()` / `count_resources()` / `iter_all()` (bare) | **Yes** — the query's `is_deleted` is `UNSET`, i.e. no filter |
| `GET /{model}` (no `is_deleted` param) | **No** — the route param defaults to `False` |

To exclude them in a single programmatic call, pass it explicitly:

```python
from specstar.query_types import ResourceMetaSearchQuery
rm.list_resources(ResourceMetaSearchQuery(is_deleted=False))   # live only
```

Or set a default for *all* programmatic list/count/iter calls (non-breaking —
defaults to `None` = include both):

```python
spec.configure(default_is_deleted=False)   # exclude soft-deleted by default
# None = include both (default) · False = exclude · True = only deleted
# An explicit is_deleted in the query always wins.
```

### `Unique()` ignores soft-deleted rows

A `Unique()` constraint only considers **live** resources. After you soft-delete
a row, its value (SKU, employee id, …) can be reused by a new resource. This is
often desirable, but surprising if you expected the value to stay reserved.
Permanently delete (or keep the row) if you need the value held.

---

## Blobs / attachments

### Deleting a resource does **not** delete its blobs

Blobs (`Binary` / `file_id`) are **not** reference-counted or garbage-collected
on delete, so deleting a resource leaves its uploaded files behind. Do **not**
naively delete a resource's blobs in an on-delete handler: the same `file_id`
can be referenced by **other resources, other fields, or older revisions** of
the same resource (and soft-deleted resources can still be restored). Deleting
it would corrupt those references.

Safe cleanup is a refcount-aware sweep, not an on-delete hook. Recipe:

```python
# Run as a periodic / manual GC, never inline on delete.
# A blob is an orphan only if NO live resource/revision references it.
def collect_orphan_blobs(manager, *, dry_run=True):
    referenced: set[str] = set()
    # 1. gather every file_id referenced by any live revision of any resource
    for meta in manager.iter_all():                  # all live resources
        for rev_id in manager.list_revisions(meta.resource_id):
            data = manager.get_resource_revision(meta.resource_id, rev_id).data
            referenced |= file_ids_in(data)          # your model-specific extractor
    # 2. delete blobs that nothing references
    orphans = [bid for bid in manager.blob_store.list_blobs() if bid not in referenced]
    if not dry_run:
        for bid in orphans:
            manager.blob_store.delete(bid)
    return orphans
```

Adapt `file_ids_in(...)` and the blob-store listing to your storage backend;
both a full revision scan and blob enumeration are required to be correct.

---

## Versioning / migration

### Resources registered with bare `add_model(Model)` store `schema_version=None`

Later upgrading to `Schema(Model, "v2").step("v1", ...)` then fails with
"No migration path from version None to 'v2'". Two recoveries:

1. **Recommended from day one:** always use `Schema(Model, "v1")` even
   before you have migrations.
2. **Already have unversioned data:** register a `step(None, ...)`
   migration. See [Migrations howto](./migrations.md#migrating-from-unversioned-schema_versionnone-data).

### `switch` needs the full `revision_id` *or* a bare revision number

Both forms work; anything else returns `400` with a hint:

```
POST /user/{rid}/switch/3                  # OK — normalised to {rid}:3
POST /user/{rid}/switch/{rid}:3            # OK — explicit
POST /user/{rid}/switch/whatever           # 400 — invalid format
```

### `switch` to an older revision may raise `RevisionNotMigratedError`

After `migrate(resource_id)` bumps `meta.schema_version` to the new
target, older revisions still sit at their original `schema_version`.
`switch` to one of those raises `RevisionNotMigratedError` (400). Fix
by migrating that specific revision first:

```python
rm.migrate(resource_id, revision_id=old_revision_id)
rm.switch(resource_id, old_revision_id)
```

---

## Configuration

### `configure(admin=...)` sets the RBAC root **user name**, not a URL path

```python
spec.configure(admin="alice")   # username "alice" gets full access
spec.configure(admin="/admin")  # username "/admin" — almost certainly NOT what you wanted
```

The web admin UI is the separate TypeScript app under `wizard/`.

### Programmatic `mgr.create/update/...` use `anonymous` + `now()` if you don't set a user

By default (non-strict) a programmatic write with no operation context records
the same values an unauthenticated HTTP request would — `created_by="anonymous"`
and the current UTC time — so it "just works". To attribute writes to a real
user, set a default or wrap the call:

```python
spec.configure(default_user="me", default_now=datetime.utcnow)
# or, per-call
with mgr.using(user="me", now=datetime.utcnow()):
    mgr.create(...)
```

If you'd rather a missing context be a hard error (no silent `anonymous`), opt
into strict mode — then `create`/`update`/`migrate`/`switch` without a context
raise `MissingOperationContextError`:

```python
spec.configure(strict_operation_context=True)
```

### `MigrateRouteTemplate` is opt-in

`/{model}/migrate/...` endpoints are **not** registered by default.
Add them explicitly before `add_model()`:

```python
from specstar.crud.route_templates.migrate import MigrateRouteTemplate
spec.add_route_template(MigrateRouteTemplate())
```

The mounted paths follow `model_naming` and are singular —
`/issue/migrate/execute`, not `/issues/migrate/execute`.

---

## Backup / restore

### `/import` accepts both a `multipart` `file` field and a raw body

`GET /{model}/export` streams `application/octet-stream`.
`POST /{model}/import` accepts **either** `multipart/form-data` with a `file`
field **or** the archive as a raw `application/octet-stream` body — so the
export bytes round-trip directly (`--data-binary @dump.acbak`). See [Backup &
Restore](./backup-restore.md#per-model-import) for `curl` recipes.

The **global** route does not: `POST /_backup/import` takes the multipart
form field only, and a raw body is a `422` (`body.file: Field required`).

### `dump()` raises on an unreadable blob — by default

`dump()` runs `strict=True`: content the store will not give up raises
`DumpIncompleteError` instead of being left out of the archive. An unreadable
blob used to be skipped in silence while the dump reported success, so a short
archive was indistinguishable from a complete one; a resource whose revision
data would not read used to die on a bare `KeyError` instead. Pass
`strict=False` to export what is readable and check the returned per-model
`DumpStats` — `complete` is the question to ask.

A revision stored at an **older schema version** does not decode under the
current model, which is a supported state. For a model with no `Binary` field
nothing is decoded at all and nothing is lost. For one that carries
attachments it *is* a loss — that revision contributes no blob ids, so its
attachment never enters the archive — and strict mode refuses, naming it.

Both export routes accept `?strict=false`, but a streamed response has nowhere
to return the stats: the archive comes back well-formed whatever was skipped
and `load` accepts it. The server logs a warning; that is the only record.

### A truncated archive is refused, not partially loaded

Cutting an archive at a record boundary leaves whole, decodable records, so
nothing downstream used to notice: `load()` returned `loaded=0` without
raising. It now requires the end-of-stream record and now requires the end-of-stream
record and raises `ArchiveTruncatedError` if the stream ends early — at a
record boundary or inside a record, on both import routes, always a `400` and
always carrying the counts that were applied.
Loading is not transactional — batches already written stay written, and the
error carries the counts that were applied.

### `access_scope` does not fence a backup

The backup routes are permission-checked against the request's user like every
other route (`ResourceAction.dump` / `load`, grouped as
`ResourceAction.backup`); a refusal is a `403`. Two caveats: the default
checker is `AllowAll()`, so out of the box they are as open as everything else;
and `access_scope` restricts reads and request-writes only — a user allowed to
`dump` gets **every** row of that model, not the rows their scope would show.
Grant the backup actions to operators.

---

## GraphQL is opt-in

The README lists GraphQL as a feature; that means *supported*, not
*on by default*. To use it:

```bash
pip install specstar[graphql]
```

```python
from specstar.crud.route_templates.graphql import GraphQLRouteTemplate
spec.add_route_template(GraphQLRouteTemplate())
```

---

## Naming / paths

### Multi-word class names are kebab-cased, not snake-cased

`BlogPost → /blog-post`, `XMLNode → /xml-node` (acronyms are treated as one
word, not split letter-by-letter). The default
`model_naming="kebab"` is lowercase with hyphens; see [Routes howto §
`model_naming` reference](./routes.md#model_naming-reference) for a full
matrix and an example of supplying a callable (e.g. to pluralise).

### Path params are always `resource_id`, not `id`

`GET /user/{resource_id}` — there is no `/user/{id}`. Same for every
generated route.
