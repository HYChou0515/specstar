# Migrating from `autocrud` to `specstar`

`autocrud` was renamed to **`specstar`** in v0.10.0. Source code, types,
behavior, and the FastAPI surface area are unchanged — only the package name
and a handful of public identifiers were renamed.

This guide covers everything you need to update.

> **TL;DR** — `pip install -U specstar`, then run the [find / replace
> table](#3-find--replace-table) and you're done. If you can't change code
> right away, install `autocrud==0.10.0` instead and you'll get a
> deprecation-warned shim that still works.

---

## 1. Why the rename

`autocrud` started as "automatic CRUD for FastAPI", but the project grew well
beyond that — versioning, GraphQL, search, permissions, background jobs, an
admin UI generator. `SpecStar` better captures the actual scope: you write
the **spec** (a `msgspec.Struct`) and SpecStar generates the **rest of the
star**. The original name was also taken on PyPI in some ecosystems and
confusable with unrelated projects.

The rename is mechanical. No public method signatures or behaviors changed.

---

## 2. Pick a migration path

You have two options. Pick whichever fits your team's risk appetite.

### A. Migrate the imports (recommended)

```bash
pip install -U specstar
pip uninstall autocrud   # optional — see "Keep both?" below
```

Then apply the [find / replace table](#3-find--replace-table) to your code.
This is the path that gets you off the deprecation warnings and onto the
canonical name.

### B. Pin the shim (no code changes)

```bash
pip install -U "autocrud>=0.10.0"
```

`autocrud==0.10.0` no longer ships any code of its own — it's a thin shim
that depends on `specstar==0.10.0` and uses an `importlib` meta-path finder
to redirect every `autocrud[.X]` import to the matching `specstar[.X]`
module at runtime. Your existing imports continue to work, but each one
emits a `DeprecationWarning` once per process to remind you to migrate.

The shim is a **migration runway, not a long-term home.** No further releases
of `autocrud` will be published beyond 0.10.0; future fixes and features
ship under `specstar` only.

### Keep both installed?

Don't. `autocrud==0.10.0` already depends on `specstar==0.10.0`, so installing
the shim gives you both transitively. Installing `autocrud<0.10.0` *and*
`specstar` side-by-side would give you two separate copies of the codebase
that won't share state — confusing and pointless.

---

## 3. Find / replace table

| Before                                                | After                                                  |
| ----------------------------------------------------- | ------------------------------------------------------ |
| `pip install autocrud`                                | `pip install specstar`                                 |
| `from autocrud import ...`                            | `from specstar import ...`                             |
| `from autocrud.crud.core import AutoCRUD`             | `from specstar.crud.core import SpecStar`              |
| `from autocrud import crud`                           | `from specstar import spec`                            |
| `AutoCRUD()`                                          | `SpecStar()`                                           |
| `AutoCRUDWarning`                                     | `SpecStarWarning`                                      |
| `crud.add_model(...)`, `crud.apply(...)`              | `spec.add_model(...)`, `spec.apply(...)`               |
| `AUTOCRUD_DEFAULT_QUERY_LIMIT`                        | `SPECSTAR_DEFAULT_QUERY_LIMIT`                         |

The `crud → spec` rename only applies to the **global instance** exported
from `specstar/__init__.py`. Local variables you named `crud` in your own
code can stay; they were never part of the public API. Submodule paths that
contain `crud` (e.g. `specstar.crud.core`, `specstar.crud.route_templates`)
are also unchanged — `crud` is a meaningful technical term in those paths,
not a brand string.

---

## 4. Environment variables

`AUTOCRUD_DEFAULT_QUERY_LIMIT` was renamed to **`SPECSTAR_DEFAULT_QUERY_LIMIT`**.
The legacy name is still read at startup and emits a `DeprecationWarning`
pointing at the new one. If both are set, `SPECSTAR_*` wins.

```bash
# Old
export AUTOCRUD_DEFAULT_QUERY_LIMIT=1000

# New
export SPECSTAR_DEFAULT_QUERY_LIMIT=1000
```

---

## 5. Generated code (Starter Wizard)

Projects scaffolded by the [Starter Wizard](https://hychou0515.github.io/specstar/wizard/)
prior to v0.10.0 emit `from autocrud import crud`. Re-running the wizard
against the same configuration on v0.10.0 will produce `from specstar import
spec` — there are no other content changes. If you'd rather hand-edit, the
file you need to update is typically `main.py` and the import lines.

---

## 6. CI and pre-commit

If you have CI greps or pre-commit hooks that block the string `autocrud`,
update them. If you've pinned `autocrud<X.Y` in `requirements.txt`,
`Pipfile`, `pyproject.toml`, or `uv.lock`, replace the dependency with
`specstar` at the corresponding version.

---

## 7. Verifying the migration

After updating, this should run cleanly with **no deprecation warnings**:

```python
import warnings
warnings.simplefilter("error", DeprecationWarning)

from specstar import SpecStar, spec  # noqa: F401
```

If a warning fires, the message names the import path that still goes
through the shim — fix that import and re-run.

---

## 8. Data migrations are covered elsewhere

This document only covers the package-name rename. If you arrived here for
*data*:

* **Coming from `autocrud` 0.4.x** — the storage format changed underneath
  you and specstar cannot read a 0.4 data directory. Export with
  `autocrud==0.4.6` and import with `SpecStar.load()`; the walkthrough is
  [Upgrading from autocrud 0.4.x](https://hychou0515.github.io/specstar/guides/upgrade-from-0.4/).
* **Evolving the `msgspec.Struct` shape** across versions of your own
  application — see the
  [Schema Migration guide](https://hychou0515.github.io/specstar/howto/migrations/).

---

## 9. Reporting issues

Please file rename-related issues at
<https://github.com/HYChou0515/specstar/issues>. Include the import path
that broke and the deprecation warning text if there is one.
