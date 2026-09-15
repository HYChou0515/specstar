# Changelog

All notable changes to SpecStar (formerly `autocrud`).

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---
## [Unreleased]


### Added

- Data migration path from `autocrud` 0.4.x (#448): `autocrud==0.4.6` is a
  patch of the old line whose `AutoCRUD.dump()` — broken on 0.4.0–0.4.5 —
  now writes the current `.acbak` v2 archive, which `SpecStar.load()`
  imports unchanged (revision history, soft-deletes, `switch()`-ed
  currents, `indexed_data`, and pre-filled `rev_*` mirror fields). Pinned
  by a fixture generated with the real 0.4.6 package
  (`tests/test_legacy_load.py`). 0.4.6's `dump(query=...)` exports only the
  resources a `ResourceMetaSearchQuery` selects (each with its full
  history), so a large store moves as a full export plus a delta at cutover.
- `SpecStar.load(bio, *, batch_size=1000, batch_bytes=64 MiB)`: records are
  written in batches as the archive is read instead of buffering a whole
  model section first. Peak memory drops from ~2.3x the section to a
  fraction of the batch; `on_duplicate=skip` still skips a resource's
  revisions when they arrive in a later batch. `POST /_backup/import` and
  `POST /{model}/import` stream a multipart upload from its spooled file
  instead of reading it whole.


### Documentation

- New guide *Upgrading from autocrud 0.4.x*; `MIGRATION.md` §8 now points
  0.4 users there instead of at the schema-migration page.

## [0.13.0a3] — 2026-07-28


### Added

- Release alpha|beta|rc|final, and repair the broken bumps
- P1 resolve app branding from rc, OpenAPI title, then defaults
- P2 emit resolved branding into the generated app
- P3 give the app a real logo and its own name


### Documentation

- P3 — the union name-length limit and its add_model(name=...) fix
- P5 document how an app gets its own name and logo


### Fixed

- P1 — shorten schema names past PyYAML's 1024-char key limit
- P2 — refuse an over-long derived resource name; pass add_model(name=...)
- P4 size the logo where it is actually legible


### Performance

- Stop paying for a transaction and a cursor a bounded read cannot use
- Read a revision's info and data in one round-trip
- Fetch a listing's page in one query instead of once per row
- Stop an update doing work nothing consumes
- Ask whether a resource exists once, not twice
- A page-sized search does not need a server-side cursor
- Export every resource in one statement, not one per resource
- A limited aggregation does not need a server-side cursor
- Drop orphaned data rows in one statement, not one ask per uid
- Read every revision's info at once when pruning
- Dump one resource in one read too
- Read the row once when migrating it

## [0.13.0a2] — 2026-07-21


### Added

- patch_many — apply one patch to every row a query selects, batching the
  reads and writes a per-row loop cannot (#434)
- IResourceStore.read_many — bulk revision read under a byte budget, with
  native size probes on Postgres (octet_length) and S3 (head_object)
- IMetaStore.get_many — bulk meta read, one ANY(...) on Postgres

### Fixed

- A revision deleted between selection and read no longer fails a whole
  bulk read batch

## [0.13.0a1] — 2026-07-20


### Added

- Python fallback for .fuzzy()/.similarity() when pg_trgm is absent
- Composite group-by — by accepts a list of Fields (#430)

## [0.12.3] — 2026-07-16


### Fixed

- Dispatch fuzzy/vector conditions when nested inside a group

## [0.12.2] — 2026-07-16


### Added

- P1 — .any()/.all() element quantifier for list fields (pure-Python reference)
- P2 — push .any()/.all() down to SQLite and Postgres
- P3 — reject bare scalar string ops on a list field, direct to .any()
- P1 — TrigramIndex opt-in annotation for substring/fuzzy search
- P2 — build the pg_trgm GIN for a TrigramIndex field on Postgres
- P5 — .fuzzy() pg_trgm word_similarity search (Postgres-native)
- P6 — .similarity().desc() trigram ranking sort (Postgres-native)
- P7 — accept .fuzzy() / .similarity() over the HTTP ?qb= surface
- P9 — .fuzzy()/.similarity() on every backend via a Python port


### Documentation

- P5 — document the .any()/.all() element quantifier
- P8 — document .fuzzy() / .similarity() and TrigramIndex


### Performance

- P3 — route .any().eq() to the GIN-probeable @> membership
- P4 — index-back .any().contains() with a coarse LIKE + recheck

## [0.12.1] — 2026-07-16


### Fixed

- Cap the in_list GIN fan-out so long lists stop being quadratic
- A failed sort-index build must not take the process down


### Performance

- Keep Vector fields out of the indexed_data JSONB

## [0.12.0] — 2026-07-16


### Added

- SortIndex — opt-in btree over (indexed_data->'field') for range/ORDER BY (#421)


### Fixed

- Populate the SetIndex column before enabling the && fast path (#420)
- SetIndex/SortIndex over the whole lifecycle, and when pods boot together (#422)


### Performance

- Probe indexed_data with @> so the GIN it already maintains is used (#419)
- Push count_resources down to SQL COUNT(*) instead of decoding every row

## [0.11.15] — 2026-07-14


### Added

- Group-level order_by + limit/offset + exp_count_groups (in-process ref)
- SQLite pushdown of group-level ORDER BY / LIMIT / OFFSET
- Postgres pushdown of group-level ORDER BY / LIMIT / OFFSET + parity


### Documentation

- Design plan — group-level order_by + limit/offset + exp_count_groups
- Plan status → all phases done + DoD checked
- How-to page for aggregation + group-level ordering/pagination

## [Unreleased]


### Added

- Group-level pagination for `exp_aggregate_by`: keyword-only `order_by` (an
  aggregate result-name or the `"key"` sentinel, with the `+`/`-` direction
  convention), `limit`, and `offset` page the distinct GROUPS — NULL
  order-values and NULL keys sort last regardless of direction, and the group
  key is the stable ascending tiebreak so pages never straddle a tie. Adds
  `exp_count_groups(by, query=)` for the pager total. The
  `ORDER BY … LIMIT … OFFSET` pushes down to the SQLite and Postgres engines
  when the order target is engine-orderable (the group key, or a single-column
  Count/Sum/Min/Max including datetime Min/Max); an Avg / ForeignAggregate
  target falls back to the in-process reference, which every backend still
  matches byte-for-byte. (#412)

## [0.11.14] — 2026-07-03


### Fixed

- Stale/killed jobs now go through the retry budget instead of failing
  outright. `recover_stale_jobs` spends one retry — requeuing the job while
  budget remains and marking it FAILED only once exhausted — rather than
  terminally failing a stuck PROCESSING job with zero retries. On RabbitMQ a
  new consume-time guard classifies each (re)delivery by the job's persisted
  status, so a job that outlives `consumer_timeout` is counted and eventually
  dead-lettered instead of re-running forever, and a duplicate delivery of a
  COMPLETED job (lost ack) is dropped instead of re-run. (#409)

## [0.11.13] — 2026-07-01


### Added

- Push Sum down to IMetaWithAgg stores (Phase 1)
- Push Min/Max down for numeric fields (Phase 2)
- Push Avg down via Sum + non-null Count (Phase 3)
- Push Min/Max over meta datetime columns (Phase 4)

## [0.11.12] — 2026-06-30


### Fixed

- Authorize once at request boundary (#402)
- Stamp last_heartbeat_at at claim so recover_stale_jobs can't false-positive a fresh job (#404)

## [Unreleased]


### Changed

- **Behavior change:** permission is now authorized once per request, at the
  outermost operation (request-boundary model, matching access-scope). A single
  PATCH no longer re-runs the permission checker for the internal `get`/`update`
  it performs, and a single GET no longer re-checks the nested
  `get_meta`/`get_resource_revision`. Consequence: granting only `patch` is now
  sufficient — a policy that *denied* `read`/`update` but *allowed* `patch`
  previously blocked the patch and now lets it through. Express "can't see →
  can't write" via `access_scope` instead of a deny-read rule. (#402)


### Fixed

- Permission checker ran 3× per HTTP PATCH (patch→get→update cascade) (#402)

## [0.11.11] — 2026-06-27


### Added

- Prune old revisions to reclaim storage (#377)
- Per-model read access-scope predicate (#398 part A)
- Resource-aware write authorization via current_resource (#398)
- Gate writes by access_scope + lifecycle resource-aware checks (#398)


### Documentation

- Document prune_revisions revision pruning (#396)

## [0.11.10] — 2026-06-26


### Added

- Ref-count GC to reclaim unreferenced blobs (#370)


### Fixed

- Contains on list-typed indexed fields uses element membership (#378)
- Honor partition_key/idempotency_key on all queue backends (#384)
- Shard disk stores into fanout subdirs to avoid NAS inode limits (#387)

## [0.11.9] — 2026-06-24


### Added

- Push exp_aggregate_by Count group-by down to SQLite
- Push exp_aggregate_by Count group-by down to PostgreSQL
- Contains_any list-overlap operator + SetIndex Postgres acceleration


### Fixed

- Contains on list-typed indexed fields uses JSONB @> (#362)
- Contains on list-typed indexed fields uses json_each membership (#362)
- Auto-register list indexed fields from model annotations (#378)
- Share one connection pool per DSN (#380)

## [Unreleased]


### Added

- **Resource-level access control (#398).** A per-model `access_scope`
  predicate (`user -> ConditionBuilder | None | UNRESTRICTED`) for row-level
  security. It is ANDed into every request-originated read (`list`/`search`/
  `count` and all single-resource GET variants) **and** enforced as a
  precondition for every request-originated write (`update`/`modify`/`patch`/
  `delete`/`permanently_delete`/`switch`/`restore`, plus the batch
  delete/restore endpoints). A resource outside the caller's scope is hidden as
  **404** on both reads and writes — evaluated *before* the permission checker,
  so existence never leaks via a 403. `None` denies all (fail-closed),
  `UNRESTRICTED` bypasses, and a predicate over an unindexed field raises rather
  than silently widening visibility. Internal `ResourceManager` calls stay
  unscoped; only routes entered with `apply_access_scope=True` are gated. See the
  [access-scope how-to](docs/en/howto/access-scope.md). (#399, #400)
- Write-phase permission checks can read the current stored resource via
  `PermissionContext.current_resource`, gated per action by
  `IPermissionChecker.required_resource_parts` / `@requires_resource_parts` so a
  checker pays only for the slices it declares. Available across the full write
  lifecycle — `update`/`modify`/`patch`/`delete` **and** `switch`/
  `permanently_delete`/`restore` — so `owner_self` and embedded-field write ACLs
  work end-to-end. (#398)


### Fixed

- `owner_self` no longer always-denies write actions: it read a `context.meta`
  that never existed on before-contexts (deny-all). It now reads
  `current_resource.meta.created_by` and works on the whole write lifecycle.
  ⚠️ behavior change: a setup that mapped `owner_self` to a write/lifecycle
  action was effectively deny-all and will now correctly allow the owner. (#398)

- `partition_key` / `idempotency_key` are now honored by **every** message
  queue backend, not just `SimpleMessageQueue` (#384). RabbitMQ and Celery
  previously accepted the tags on `Job` but silently ignored them, so
  same-partition jobs ran concurrently and idempotent enqueue could
  duplicate. Both backends now check for a PROCESSING peer before claiming a
  job and defer it through a short delay (configurable via
  `partition_retry_delay_seconds`) if the partition is busy; a deferred job
  is not counted as a retry. Enforcement is **best-effort** on multi-worker
  backends (the check-then-claim is not atomic across workers) and strict
  only on the single-consumer `SimpleMessageQueue` — see `Job.partition_key`.
  `enqueue()` is now part of the `IMessageQueue` contract, and the queue
  auto-registers `partition_key` / `idempotency_key` as indexed fields so
  the lookups don't silently degrade on SQL backends (cf. #378).
- Postgres stores sharing a DSN now share one process-global connection
  pool, so connection count scales with the number of distinct DSNs instead
  of `models × 2 × replicas` — no more boot-time `too many clients` storms
  (#380). Pools are lazy (`minconn=0`) by default and capped at
  `maxconn=16` per process per DSN; both are tunable via the postgres
  connection `options`. **Note:** `maxconn` is now a per-process, per-DSN
  ceiling shared across every model and store role, not a per-store limit —
  raise it for high-concurrency deployments.

## [0.11.8] — 2026-06-10


### Fixed

- Bounded ESTALE retry on disk stores (#352)
- Atomic writes + TOCTOU translation on disk stores (#352)

## [0.11.7] — 2026-06-09


### Added

- Exp_aggregate_by — group-by + Count aggregate (v1, experimental)
- Exp_aggregate_by — add Sum / Min / Max / Avg
- Exp_aggregate_by handles cross-RM (one method, ForeignAggregate)
- Field.source — make QB[] vs QB.foo() a real dispatch (aggregate API breaks str)


### Fixed

- Snapshot before iterating to survive concurrent writes
- Make create() crash-safe — atomic meta commit + typed not-found (#340)

## [0.11.6] — 2026-06-07


### Added

- Lease-based distributed lock (#342 #2)
- Partition_key serialization + idempotent enqueue (#342 #3/#4)
- Optimistic concurrency (#342 part 1) — expected_revision_id, if_not_exists
- Expected_etag CAS — detect concurrent in-place modify()
- Wire CAS through If-Match / If-None-Match + ETag (#342 part 2)

## [0.11.5] — 2026-05-26


### Added

- Default_is_deleted option for programmatic list/count/iter


### Fixed

- Reject path/URL-unsafe custom resource_id

## [0.11.4] — 2026-05-24


### Added

- GET only-* returns + default_get_returns, raw-body import, partial nudge


### Documentation

- Note id/timestamps/author/version are built-in metadata
- Fix verified doc bugs (event-handler imports, resource_id, kebab)

## [0.11.3] — 2026-05-24


### Added

- Configurable on_decode_error policy (skip/error/raw)
- On_unindexed_query policy (warn/error) for non-indexed filters
- No-arg search_resources / count_resources (match-all, = QB.all())


### Documentation

- Audit-user how-to + list-truncation & schema-divergence notes
- Surface that on_delete defaults to dangling


### Fixed

- Warn when count/list diverge on undecodable rows
- Apply registered migrations lazily on the read path
- Accept do(...) builders inside event_handlers=[...]
- Discoverable get_resource_manager error + no-arg list_resources

## [0.11.2] — 2026-05-24


### Added

- Production-hardening pass — audit fields, PATCH 7386, safety nets
- Programmatic writes without a context fall back to anonymous + now()


## [0.11.1] — 2026-05-23

### Added

- **`ResourceManager.get(..., include_deleted=True)`** reads a revision of a
  soft-deleted resource instead of raising `ResourceIsDeletedError`, mirroring
  `get_meta(include_deleted=...)`. Defaults to `False`, so existing behavior is
  unchanged. Fixes the Data Versioning quickstart, which showed inspecting an
  old revision after a soft delete.

### Changed

- **BREAKING — `resource_id` is rejected in request bodies (was silently
  dropped).** Sending `resource_id` in a `POST` / `PUT` body, or targeting
  `/resource_id` in a `PATCH` op, now returns **`422`** instead of `200`.
  Previously the key was silently ignored and the server-generated id was
  returned anyway, so clients believed they had set an id they hadn't.
  `resource_id` is server-generated at creation and immutable thereafter; to
  customise id generation pass `id_generator=` to `add_model(...)`. The guard
  steps aside only when the resource Struct legitimately declares its own
  `resource_id` field.

---

## [0.10.0] — 2026-05-01

The package is renamed from `autocrud` to **`specstar`**. No public method
signatures or behaviors changed; this is a brand and identifier rename.

### Renamed

- **PyPI distribution**: `autocrud` → **`specstar`**.
- **Top-level Python package**: `autocrud` → **`specstar`**.
- **Class**: `AutoCRUD` → **`SpecStar`**. Importable as
  `from specstar.crud.core import SpecStar` or `from specstar import SpecStar`.
- **Global instance**: the singleton exported from the top-level package was
  renamed `crud` → **`spec`**. Use `from specstar import spec`.
- **Warning class**: `AutoCRUDWarning` → **`SpecStarWarning`**.
- **Environment variable**: `AUTOCRUD_DEFAULT_QUERY_LIMIT` →
  **`SPECSTAR_DEFAULT_QUERY_LIMIT`**. The legacy name still works during
  the migration window with a one-shot `DeprecationWarning`.
- **Repository / docs URL**: `github.com/HYChou0515/autocrud` →
  `github.com/HYChou0515/specstar`. The old URL redirects.

### Added

- **Deprecation shim**: a new `autocrud==0.10.0` is published on PyPI as a
  thin wrapper around `specstar==0.10.0`. It installs an `importlib`
  meta-path finder that redirects every `autocrud[.X]` import to the
  matching `specstar[.X]` module at runtime, emitting a `DeprecationWarning`
  once per import path. This is the last release of `autocrud`; future
  releases ship as `specstar` only.
- **Migration guide**: see [MIGRATION.md](MIGRATION.md) for the find /
  replace table and shim details.
- **Logo**: text and mark variants in `docs/assets/`.

### Fixed

- **GraphQL resolvers** silently swallowed exceptions when `ResourceMeta`
  carried newly-added `rev_*` fields, when `indexed_data` was `UNSET`, or
  when `RevisionInfo.uid` (a `UUID`) was passed to a `str`-typed strawberry
  field. Resolvers now project only the fields the GraphQL type declares,
  map `UNSET → None`, and stringify UUIDs.
- **`SpecStar.apply()`** returned `app.router` (the FastAPI internal
  `APIRouter`) instead of the documented "supplied `router` if any, else
  `app`". The return value now matches the docstring.

### Pre-existing on `autocrud<=0.9.0`

The two `Fixed` items above were latent bugs on the 0.9.0 line. If you're
upgrading directly from `autocrud==0.9.0` you'll get the fixes
transparently — no code change required.

---

## [0.9.0] — 2026-04-29

Last release under the `autocrud` name. Breaking changes are documented in
detail in [docs/en/guides/upgrade-0.9.md](docs/en/guides/upgrade-0.9.md).

### Removed

- `PostgreSQLStorageFactory` and `PostgreSQLDiskS3StorageFactory` (deprecated
  with a runtime warning since 0.8). Replace with `PostgreSQLS3StorageFactory`
  or compose the meta / resource / blob stores directly.
- `specstar.permission.basic` (deprecated since 0.8). Use the unified APIs
  in `specstar.permission`.

### Changed

- `IResourceStore` now requires `save_many()` and `dump_all_revisions()` on
  every backend. Custom stores must implement both.
- `dump()` returns a generator of `(meta, revisions)` tuples rather than a
  flat record stream. Callers that built a list need to flatten or pivot.
- `IResourceManager` subclasses must implement `load_records_bulk()`.
- `start_consume(block=False)` returns the worker handle instead of `None`.

### Added

- **`ResourceMeta` carries the current-revision fields** (`rev_status`,
  `rev_created_by`, `rev_updated_by`, `rev_created_time`, `rev_updated_time`).
  Run `ResourceManager.backfill_revision_meta()` once after deploying to
  populate them on resources created on earlier versions; until you do, the
  fields are `UNSET`. (#274)

---

## Earlier releases

For releases before 0.9.0, see the
[GitHub Releases page](https://github.com/HYChou0515/specstar/releases) and
the per-version notes embedded in
[docs/en/guides/](docs/en/guides/).
