"""The base meta indexes exist on EVERY table, not only the first one in a schema.

``_init_postgres_table`` created its eleven base indexes — the ``indexed_data``
GIN that every ``==`` / ``in_`` / list ``.contains`` probe relies on (#416), the
``is_deleted`` btree every listing filters on, and the ``created_*`` /
``updated_*`` / ``rev_*`` btrees — under FIXED names (``idx_indexed_data_gin``,
``idx_is_deleted``, ...). An index name shares its schema's relation namespace
with every table and index in it, and the DDL said ``IF NOT EXISTS``: so in a
schema holding several
SpecStar tables the first table took every name and each later table got a
``NOTICE: relation "idx_indexed_data_gin" already exists, skipping`` and no
index at all. Its equality filters scanned every row.

Reproduced on a fresh database with JASMINE's 22 tables: all eleven names on
``jasmine_app_meta``, the other 21 tables with none — a 2-row answer taking 45 s
in production.

Every test runs in its own fresh database: the shared test database's ``public``
schema already has an ``idx_indexed_data_gin`` on some table — exactly the state
being reproduced — and a test that rewinds tables to the old names takes those
names for the whole database, so no two tests may share one.
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from datetime import UTC, datetime

import psycopg2
import pytest

from specstar.query import QB
from specstar.resource_manager.meta_store.postgres import (
    BASE_INDEXES,
    PostgresMetaStore,
    base_index_name,
    base_index_statements,
)
from specstar.types import ResourceMeta

ADMIN_DSN = "postgresql://admin:password@localhost:5432/your_database"

# What each base index is, by definition: (access method, indexed expression).
# Identity by DEFINITION, never by name — the bug is that names lied.
EXPECTED = {
    ("btree", "created_time"),
    ("btree", "updated_time"),
    ("btree", "created_by"),
    ("btree", "updated_by"),
    ("btree", "is_deleted"),
    ("btree", "rev_status"),
    ("btree", "rev_created_by"),
    ("btree", "rev_updated_by"),
    ("btree", "rev_created_time"),
    ("btree", "rev_updated_time"),
    ("gin", "indexed_data"),
}
_DEF = re.compile(r"USING (\w+) \((.+)\)$")


@pytest.fixture
def dsn():
    name = "specstar_idxnames_" + uuid.uuid4().hex[:8]
    admin = psycopg2.connect(ADMIN_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    yield ADMIN_DSN.rsplit("/", 1)[0] + "/" + name
    from specstar.resource_manager import _pg_pool

    _pg_pool.close_all_pools()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.close()


def _sql(dsn: str, sql: str, args=None) -> list[tuple]:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def _indexes(dsn: str, table: str) -> list[tuple[str, bool, tuple[str, str]]]:
    """(name, valid, (method, expression)) of every non-primary index on ``table``."""
    rows = _sql(
        dsn,
        "SELECT c.relname, i.indisvalid, pg_get_indexdef(i.indexrelid) "
        "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE i.indrelid = to_regclass(%s) AND NOT i.indisprimary",
        [f'"{table}"'],
    )
    out = []
    for name, valid, ddl in rows:
        m = _DEF.search(ddl)
        assert m, ddl
        out.append((name, valid, (m.group(1), m.group(2))))
    return out


def _base(dsn: str, table: str) -> list[tuple[str, str]]:
    return sorted(d for _, _, d in _indexes(dsn, table) if d in EXPECTED)


def _table(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _meta(rid: str, color: str) -> ResourceMeta:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return ResourceMeta(
        current_revision_id=f"rev_{rid}",
        resource_id=rid,
        total_revision_count=1,
        created_time=base,
        updated_time=base,
        created_by="t",
        updated_by="t",
        is_deleted=False,
        indexed_data={"color": color},
    )


def _as_built_by_0_13_0a4(dsn: str, owner: str, others: list[str]) -> None:
    """Rewind tables a fresh store just built to the state the old DDL left:
    ``owner`` holds every fixed name, ``others`` hold no base index at all."""
    for t in [owner, *others]:
        for name, _, d in _indexes(dsn, t):
            if d in EXPECTED:
                _sql(dsn, f'DROP INDEX "{name}"')
    old = {
        "created_time": "(created_time)",
        "updated_time": "(updated_time)",
        "created_by": "(created_by)",
        "updated_by": "(updated_by)",
        "is_deleted": "(is_deleted)",
        "rev_status": "(rev_status)",
        "rev_created_by": "(rev_created_by)",
        "rev_updated_by": "(rev_updated_by)",
        "rev_created_time": "(rev_created_time)",
        "rev_updated_time": "(rev_updated_time)",
    }
    for col, expr in old.items():
        _sql(dsn, f'CREATE INDEX idx_{col} ON "{owner}"{expr}')
    _sql(
        dsn, f'CREATE INDEX idx_indexed_data_gin ON "{owner}" USING GIN (indexed_data)'
    )


# ---- a schema with several tables ---------------------------------------------


def test_every_table_in_one_schema_gets_all_base_indexes(dsn):
    tables = [_table() for _ in range(3)]
    for t in tables:
        PostgresMetaStore(pg_dsn=dsn, table_name=t)
    for t in tables:
        assert _base(dsn, t) == sorted(EXPECTED), t


def test_an_equality_filter_on_a_later_table_is_served_by_its_own_gin(dsn):
    first, later = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=first)
    store = PostgresMetaStore(pg_dsn=dsn, table_name=later)
    for i in range(50):
        store[f"r{i}"] = _meta(f"r{i}", "red" if i % 10 == 0 else "blue")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SET enable_seqscan = off")  # a small table would scan anyway
            cur.execute(
                f'EXPLAIN SELECT data FROM "{later}" WHERE indexed_data @> %s::jsonb',
                ['{"color": "red"}'],
            )
            plan = "\n".join(r[0] for r in cur.fetchall())
    finally:
        conn.close()
    assert base_index_name(later, "indexed_data_gin") in plan, plan
    hits = sorted(
        m.resource_id for m in store.iter_search((QB["color"] == "red").build())
    )
    assert hits == [f"r{i}" for i in range(0, 50, 10)]


# ---- tables the old DDL built ---------------------------------------------------


def test_a_table_that_holds_the_old_names_keeps_one_of_each_not_two(dsn):
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)  # the upgraded process boots
    assert _base(dsn, owner) == sorted(EXPECTED)  # renamed, not built a second time
    names = {n for n, _, d in _indexes(dsn, owner) if d in EXPECTED}
    assert (
        "idx_indexed_data_gin" not in names
    )  # the name is free for nobody to take again
    assert base_index_name(owner, "indexed_data_gin") in names


def test_a_table_the_old_ddl_left_without_indexes_gets_them_with_its_rows(dsn):
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    store = PostgresMetaStore(pg_dsn=dsn, table_name=other)
    for i in range(200):
        store[f"r{i}"] = _meta(f"r{i}", "red")
    _as_built_by_0_13_0a4(dsn, owner, [other])
    assert _base(dsn, other) == []
    PostgresMetaStore(pg_dsn=dsn, table_name=other)  # the upgraded process boots
    found = [(v, d) for _, v, d in _indexes(dsn, other) if d in EXPECTED]
    assert sorted(d for _, d in found) == sorted(EXPECTED)
    assert all(v for v, _ in found)


def test_an_invalid_index_under_the_new_name_is_rebuilt(dsn):
    t = _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    gin = base_index_name(t, "indexed_data_gin")
    # What a CONCURRENTLY build killed mid-flight leaves: the name, marked invalid.
    _sql(
        dsn,
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(%s)",
        [f'"{gin}"'],
    )
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    assert [v for n, v, _ in _indexes(dsn, t) if n == gin] == [True]


def test_several_processes_upgrading_at_once_all_boot(dsn, caplog):
    # Postgres runs one concurrent build per table at a time: pods racing to build
    # deadlock each other (#422's measurement: 4 of 6 died). The guard would turn
    # that into warnings, so the property is that NO pod has a failed build to warn
    # about — the losers skip, the winner builds.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    seeded = PostgresMetaStore(pg_dsn=dsn, table_name=other)
    for i in range(3000):  # enough rows that the builds overlap
        seeded[f"r{i}"] = _meta(f"r{i}", "red")
    _as_built_by_0_13_0a4(dsn, owner, [other])
    errors: list[BaseException] = []

    def pod(table: str) -> None:
        try:
            PostgresMetaStore(pg_dsn=dsn, table_name=table)
        except BaseException as e:  # noqa: BLE001 — collected and asserted below
            errors.append(e)

    pods = [threading.Thread(target=pod, args=(t,)) for t in [owner] * 2 + [other] * 6]
    with caplog.at_level(logging.WARNING):
        for p in pods:
            p.start()
        for p in pods:
            p.join(timeout=120)
    assert errors == []
    assert not [r for r in caplog.records if "base indexes" in r.getMessage()], (
        caplog.text
    )
    # A pod that lost the race skips the build; one more boot finishes any it left.
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    for t in (owner, other):
        assert _base(dsn, t) == sorted(EXPECTED), t


def test_a_build_that_fails_does_not_fail_the_boot(dsn, monkeypatch, caplog):
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    real_connect = psycopg2.connect

    def timing_out_connect(*args, **kwargs):
        # Only the index build's own connection (``psycopg2.connect(dsn)``): the
        # pool's (``connect(dsn=..., options=...)``) runs CREATE TABLE, which must
        # keep working for the build to be the thing that fails.
        conn = real_connect(*args, **kwargs)
        if args and not kwargs:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1ms'")
        return conn

    monkeypatch.setattr(psycopg2, "connect", timing_out_connect)
    with caplog.at_level(logging.WARNING):
        PostgresMetaStore(pg_dsn=dsn, table_name=other)  # must not raise
    assert any(other in r.getMessage() for r in caplog.records), caplog.text


# ---- names ----------------------------------------------------------------------


def test_long_table_names_sharing_a_prefix_get_distinct_index_names(dsn):
    # Each table name fits Postgres's 63 bytes (so they are two tables), but
    # ``idx_{table}_indexed_data_gin`` does not, and a plain cut at 63 bytes would
    # give both the same index names: they differ only after the cut.
    stem = "a_table_name_long_enough_to_push_index_names_"
    tables = [stem + uuid.uuid4().hex[:8] for _ in range(2)]
    assert all(len(t) <= 63 for t in tables)
    assert all(len(f"idx_{t}_is_deleted") > 63 for t in tables)
    for t in tables:
        PostgresMetaStore(pg_dsn=dsn, table_name=t)
    names = [n for t in tables for n, _, d in _indexes(dsn, t) if d in EXPECTED]
    assert len(names) == len(set(names)) == 2 * len(EXPECTED)
    assert all(len(n.encode()) <= 63 for n in names)
    for t in tables:
        assert _base(dsn, t) == sorted(EXPECTED), t


def test_the_published_statements_build_what_a_boot_would(dsn):
    # An operator pre-builds the indexes with these before upgrading, so the boot
    # finds every one and builds nothing: the statements and the boot must agree.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    booted = {n for n, _, d in _indexes(dsn, other) if d in EXPECTED}
    _as_built_by_0_13_0a4(dsn, owner, [other])
    for statement in base_index_statements(other):
        _sql(dsn, statement)  # one by one: CONCURRENTLY refuses a transaction
    assert {n for n, _, d in _indexes(dsn, other) if d in EXPECTED} == booted
    assert len(BASE_INDEXES) == len(EXPECTED)


def test_an_invalid_index_under_the_old_name_is_replaced_not_renamed(dsn):
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    _sql(
        dsn,
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(%s)",
        ["idx_indexed_data_gin"],
    )
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    gins = [(n, v) for n, v, d in _indexes(dsn, owner) if d == ("gin", "indexed_data")]
    assert gins == [(base_index_name(owner, "indexed_data_gin"), True)]


def _legacy_names(dsn: str, table: str) -> set[str]:
    legacy = {old for _, old, *_ in BASE_INDEXES}
    return {n for n, _, _ in _indexes(dsn, table)} & legacy


def test_prebuilding_the_table_that_holds_the_old_names_leaves_one_of_each(dsn):
    # The pre-build statements cannot rename: on the table holding the old names
    # they build a second copy of every index. The boot must drop the old copies.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    for statement in base_index_statements(owner):
        _sql(dsn, statement)
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    assert _base(dsn, owner) == sorted(EXPECTED)
    assert _legacy_names(dsn, owner) == set()


def test_old_names_an_old_version_puts_back_mid_rollout_are_dropped(dsn):
    # Once renamed, the old names are free again: an old pod restarting during the
    # rollout (or a rollback) builds all eleven again on the first table it opens.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [])  # `other` already has its new names
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)  # a new pod: renamed
    _as_built_by_0_13_0a4(dsn, other, [])  # an old pod: the old names, on `other`
    for _, old, ddl, _ in BASE_INDEXES:  # ...as a second copy beside the new ones
        _sql(dsn, f'CREATE INDEX IF NOT EXISTS "{old}" ON "{owner}" {ddl}')
    for t in (owner, other):
        PostgresMetaStore(pg_dsn=dsn, table_name=t)  # the next new boot
        assert _base(dsn, t) == sorted(EXPECTED), t
        assert _legacy_names(dsn, t) == set(), t


def test_an_index_of_ours_by_name_only_is_neither_renamed_nor_dropped(dsn):
    # Someone's own index that happens to be called idx_created_time, on another
    # column: not SpecStar's, so it stays, and the real one is built beside it.
    t = _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    _as_built_by_0_13_0a4(dsn, t, [])
    _sql(dsn, 'DROP INDEX "idx_created_time"')
    _sql(dsn, f'CREATE INDEX "idx_created_time" ON "{t}" (updated_by)')
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    mine = [d for n, _, d in _indexes(dsn, t) if n == "idx_created_time"]
    assert mine == [("btree", "updated_by")]
    assert ("btree", "created_time") in _base(dsn, t)


def test_the_old_names_are_renamed_in_place_not_built_again(dsn):
    # A rename is a catalog change; a rebuild of the GIN on a large table is not.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    oid = "SELECT to_regclass(%s)::oid"
    before = _sql(dsn, oid, ["idx_indexed_data_gin"])[0][0]
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    after = _sql(dsn, oid, [f'"{base_index_name(owner, "indexed_data_gin")}"'])[0][0]
    assert after == before


def test_one_index_that_cannot_be_built_does_not_cost_the_others(dsn, monkeypatch):
    # Another pod's CREATE INDEX CONCURRENTLY on the same table (a SortIndex, a
    # TrigramIndex) can deadlock with ours and pick our statement as the victim.
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    real_connect = psycopg2.connect

    class Failing:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, *args):
            if "_is_deleted" in str(sql) and "CREATE INDEX" in str(sql):
                raise psycopg2.errors.DeadlockDetected("deadlock detected")
            return self._cur.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._cur, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._cur.__exit__(*exc)

    class Conn:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self, *a, **k):
            return Failing(self._conn.cursor(*a, **k).__enter__())

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def __setattr__(self, name, value):
            if name == "_conn":
                object.__setattr__(self, name, value)
            else:
                setattr(self._conn, name, value)

    def connect(*args, **kwargs):  # only the build's own connection (see above)
        conn = real_connect(*args, **kwargs)
        return Conn(conn) if args and not kwargs else conn

    monkeypatch.setattr(psycopg2, "connect", connect)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    built = set(_base(dsn, other))
    # is_deleted is fifth of eleven: the six after it must not be skipped.
    assert built == EXPECTED - {("btree", "is_deleted")}
    monkeypatch.undo()
    PostgresMetaStore(pg_dsn=dsn, table_name=other)  # the next boot finishes it
    assert _base(dsn, other) == sorted(EXPECTED)


def test_a_unique_index_under_an_old_name_is_not_ours(dsn):
    t = _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    _as_built_by_0_13_0a4(dsn, t, [])
    _sql(dsn, 'DROP INDEX "idx_created_time"')
    _sql(dsn, f'CREATE UNIQUE INDEX "idx_created_time" ON "{t}" (created_time)')
    PostgresMetaStore(pg_dsn=dsn, table_name=t)
    unique = _sql(
        dsn,
        "SELECT i.indisunique FROM pg_index i WHERE i.indexrelid = to_regclass(%s)",
        ["idx_created_time"],
    )
    assert unique == [(True,)]  # still there, still unique
    assert ("btree", "created_time") in _base(dsn, t)


def test_an_invalid_new_name_beside_a_valid_old_one_is_replaced_by_a_rename(dsn):
    owner, other = _table(), _table()
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    PostgresMetaStore(pg_dsn=dsn, table_name=other)
    _as_built_by_0_13_0a4(dsn, owner, [other])
    gin = base_index_name(owner, "indexed_data_gin")
    _sql(dsn, f'CREATE INDEX "{gin}" ON "{owner}" USING GIN (indexed_data)')
    _sql(
        dsn,
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(%s)",
        [f'"{gin}"'],
    )
    before = _sql(dsn, "SELECT to_regclass(%s)::oid", ["idx_indexed_data_gin"])[0][0]
    PostgresMetaStore(pg_dsn=dsn, table_name=owner)
    gins = [(n, v) for n, v, d in _indexes(dsn, owner) if d == ("gin", "indexed_data")]
    assert gins == [(gin, True)]
    assert _sql(dsn, "SELECT to_regclass(%s)::oid", [f'"{gin}"'])[0][0] == before
