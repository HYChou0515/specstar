"""PostgreSQL-based IResourceStore implementation.

Stores versioned resource data (raw bytes) and revision info in PostgreSQL
tables.  The schema uses two tables:

* ``<table_prefix>resource_data`` – UID-keyed raw data + serialised
  :class:`RevisionInfo`.
* ``<table_prefix>resource_index`` – maps ``(resource_id, revision_id,
  schema_version)`` → UID.

The design mirrors the S3 resource store's UID-based deduplication: if two
revisions share the same UID the underlying bytes are stored only once.
"""

from __future__ import annotations

import io
import time
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from typing import IO, Any

from specstar.resource_manager import _pg_pool
from specstar.resource_manager.basic import (
    Encoding,
    IResourceStore,
    MsgspecSerializer,
)
from specstar.types import RevisionInfo

try:
    import psycopg2
    import psycopg2.pool
    from psycopg2.extras import execute_batch
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]  # ty:ignore[invalid-assignment]
    execute_batch = None  # type: ignore[assignment]  # ty:ignore[invalid-assignment]


class PostgresResourceStore(IResourceStore):
    """PostgreSQL-backed resource store.

    Args:
        pg_dsn: PostgreSQL connection string
            (e.g. ``"postgresql://user:pass@host:port/db"``).
        encoding: Serialisation format for :class:`RevisionInfo`
            (default ``Encoding.json``).
        table_prefix: Optional prefix for table names, useful to isolate
            multiple tenants / test runs within the same database.
        minconn: Minimum connections kept in the shared per-DSN pool
            (default ``0`` — lazy, opens nothing until first use).
        maxconn: Maximum connections in the shared per-DSN pool (default
            ``16``). This is a per-process, per-DSN ceiling shared across
            every store on the same DSN (#380), not a per-store limit.
    """

    def __init__(
        self,
        pg_dsn: str,
        encoding: Encoding = Encoding.json,
        *,
        table_prefix: str = "",
        minconn: int = _pg_pool.DEFAULT_MINCONN,
        maxconn: int = _pg_pool.DEFAULT_MAXCONN,
    ):
        self._info_serializer = MsgspecSerializer(
            encoding=encoding,
            resource_type=RevisionInfo,
        )

        # Share one process-global pool per DSN (#380). The pool is owned by
        # the registry for the process lifetime; this store never closes it.
        self._conn_pool = _pg_pool.get_pool(pg_dsn, minconn=minconn, maxconn=maxconn)

        self._data_table = f"{table_prefix}resource_data"
        self._index_table = f"{table_prefix}resource_index"

        self._init_tables()

    # ------------------------------------------------------------------
    # Connection helpers (same pattern as PostgresMetaStore)
    #
    # The connection pool is shared across all stores on this DSN and owned
    # by the process-global registry (#380); a store must not close it on
    # garbage collection. Use ``_pg_pool.close_all_pools()`` for explicit
    # shutdown / test teardown.
    # ------------------------------------------------------------------

    def _get_conn(self) -> Any:
        retry = 5
        last_error: Exception | None = None
        for attempt in range(retry):
            try:
                conn = self._conn_pool.getconn()
            except psycopg2.pool.PoolError:
                last_error = psycopg2.pool.PoolError("connection pool exhausted")
                time.sleep(min(0.5 * (attempt + 1), 3))
                continue

            # Health-check OUTSIDE a transaction. With autocommit off the
            # probe's own `SELECT 1` opens one, and psycopg2 then refuses
            # any later `autocommit` change on this connection — which is
            # what a read-only caller needs to do to skip BEGIN/COMMIT.
            conn.autocommit = True
            if self._test_query(conn):
                conn.autocommit = False  # writers are the default
                return conn

            try:
                self._conn_pool.putconn(conn, close=True)
            except Exception:
                pass
            last_error = ConnectionError("Failed to get a valid PostgreSQL connection.")
            time.sleep(1)

        if isinstance(last_error, psycopg2.pool.PoolError):
            raise last_error
        raise ConnectionError("Failed to get a valid PostgreSQL connection.")

    def _put_conn(self, conn: Any) -> None:
        self._conn_pool.putconn(conn)

    @staticmethod
    def _test_query(conn: Any) -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
        except Exception:
            return False

    @contextmanager
    def _transaction(self) -> Generator[Any, None, None]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._put_conn(conn)

    @contextmanager
    def _read(self) -> Generator[Any, None, None]:
        """A read that runs outside a transaction.

        `BEGIN` and `COMMIT` are two round-trips, and a single-statement SELECT
        has nothing to make atomic. `_get_conn` turns autocommit off because most
        callers write; a read turns it back on for its own duration and restores
        it before the connection returns to the pool.
        """
        conn = self._get_conn()
        previous = conn.autocommit
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                yield cur
        finally:
            conn.autocommit = previous
            self._put_conn(conn)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _init_tables(self) -> None:
        with self._transaction() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self._data_table}" (
                    uid TEXT PRIMARY KEY,
                    data BYTEA NOT NULL,
                    info BYTEA NOT NULL
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self._index_table}" (
                    resource_id TEXT NOT NULL,
                    revision_id TEXT NOT NULL,
                    schema_version TEXT,
                    uid TEXT NOT NULL REFERENCES "{self._data_table}"(uid),
                    PRIMARY KEY (resource_id, revision_id, schema_version)
                )
            """)
            # Index for fast resource listing / revision listing
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._index_table}_resource
                ON "{self._index_table}" (resource_id)
            """)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sv(schema_version: str | None) -> str:
        """Normalise schema_version for the composite PK (NULL → '__none__')."""
        return "__none__" if schema_version is None else schema_version

    @staticmethod
    def _unsv(sv: str) -> str | None:
        """Reverse of ``_sv``."""
        return None if sv == "__none__" else sv

    # ------------------------------------------------------------------
    # IResourceStore implementation
    # ------------------------------------------------------------------

    def list_resources(self) -> Generator[str]:
        with self._read() as cur:
            cur.execute(f'SELECT DISTINCT resource_id FROM "{self._index_table}"')
            for row in cur.fetchall():
                yield row[0]

    def list_revisions(self, resource_id: str) -> Generator[str]:
        with self._read() as cur:
            cur.execute(
                f'SELECT DISTINCT revision_id FROM "{self._index_table}" '
                f"WHERE resource_id = %s",
                (resource_id,),
            )
            for row in cur.fetchall():
                yield row[0]

    def list_schema_versions(
        self, resource_id: str, revision_id: str
    ) -> Generator[str | None]:
        with self._read() as cur:
            cur.execute(
                f'SELECT schema_version FROM "{self._index_table}" '
                f"WHERE resource_id = %s AND revision_id = %s",
                (resource_id, revision_id),
            )
            for row in cur.fetchall():
                yield self._unsv(row[0])

    def exists(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> bool:
        with self._read() as cur:
            cur.execute(
                f'SELECT 1 FROM "{self._index_table}" '
                f"WHERE resource_id = %s AND revision_id = %s "
                f"AND schema_version = %s",
                (resource_id, revision_id, self._sv(schema_version)),
            )
            return cur.fetchone() is not None

    @contextmanager
    def get_data_bytes(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> Generator[IO[bytes], None, None]:
        with self._read() as cur:
            cur.execute(
                f'SELECT d.data FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE i.resource_id = %s AND i.revision_id = %s "
                f"AND i.schema_version = %s",
                (resource_id, revision_id, self._sv(schema_version)),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(
                    f"Resource not found: {resource_id}/{revision_id}/{schema_version}"
                )
            yield io.BytesIO(bytes(row[0]))

    def get_revision_bundle(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> "tuple[RevisionInfo, bytes]":
        """Both columns of the same joined row, in one round-trip.

        `info` and `data` live side by side in the data table and were fetched by
        two identical queries differing only in the column list — two connection
        checkouts, each with its own health check, for one row.
        """
        with self._read() as cur:
            cur.execute(
                f'SELECT d.info, d.data FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE i.resource_id = %s AND i.revision_id = %s "
                f"AND i.schema_version = %s",
                (resource_id, revision_id, self._sv(schema_version)),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(
                    f"Resource not found: {resource_id}/{revision_id}/{schema_version}"
                )
            return self._info_serializer.decode(bytes(row[0])), bytes(row[1])

    def dump_all_revisions(
        self,
        *,
        resource_ids: "frozenset[str] | None" = None,
        max_workers: int = 10,
    ) -> "dict[str, list[tuple[RevisionInfo, bytes]]] | None":
        """Every revision of every requested resource, in one statement.

        `dump` already prefers a bulk path and falls back to reading each
        resource in turn when the store returns None — which Postgres did, so an
        export cost statements proportional to the number of resources (measured:
        322 for 40). The per-resource query is a join on the same two tables, so
        the whole export is that join without the per-resource `WHERE`.

        `max_workers` is part of the interface for stores that fetch payloads
        over a network (S3); a single SQL statement has nothing to parallelise.
        """
        del max_workers
        with self._read() as cur:
            if resource_ids is None:
                cur.execute(
                    f"SELECT i.resource_id, d.info, d.data "
                    f'FROM "{self._data_table}" d '
                    f'JOIN "{self._index_table}" i ON d.uid = i.uid'
                )
            else:
                if not resource_ids:
                    return {}
                cur.execute(
                    f"SELECT i.resource_id, d.info, d.data "
                    f'FROM "{self._data_table}" d '
                    f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                    f"WHERE i.resource_id = ANY(%s)",
                    (list(resource_ids),),
                )
            rows = cur.fetchall()
        out: dict[str, list[tuple[RevisionInfo, bytes]]] = {}
        for resource_id, info, data in rows:
            out.setdefault(resource_id, []).append(
                (self._info_serializer.decode(bytes(info)), bytes(data))
            )
        return out

    def get_all_revision_infos(
        self,
        resource_id: str,
    ) -> "dict[str, RevisionInfo]":
        """All of a resource's revision infos in one statement.

        The per-revision version of this was two queries each — resolve the
        schema version, then read the info — so pruning cost statements
        proportional to the revision count.
        """
        with self._read() as cur:
            cur.execute(
                f"SELECT i.revision_id, d.info "
                f'FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE i.resource_id = %s",
                (resource_id,),
            )
            rows = cur.fetchall()
        out: dict[str, RevisionInfo] = {}
        for revision_id, info in rows:
            # A revision can be stored at several schema versions; their
            # created_time / parent_revision_id agree, so the first wins.
            out.setdefault(revision_id, self._info_serializer.decode(bytes(info)))
        return out

    def get_revision_bundles(
        self,
        keys: "Sequence[tuple[str, str, str | None]]",
    ) -> "dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]]":
        """A whole page in one statement.

        The per-row query is already a join keyed on the same three columns, so
        the page is one `IN` over their tuples — the page's cost stops growing
        with its size.
        """
        wanted = [(r, v, self._sv(sv)) for r, v, sv in keys]
        if not wanted:
            return {}
        with self._read() as cur:
            cur.execute(
                f"SELECT i.resource_id, i.revision_id, i.schema_version, d.info, d.data "
                f'FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE (i.resource_id, i.revision_id, i.schema_version) IN %s",
                (tuple(wanted),),
            )
            rows = cur.fetchall()
        by_normalised = {
            (r, v, sv): (self._info_serializer.decode(bytes(info)), bytes(data))
            for r, v, sv, info, data in rows
        }
        # Hand back the caller's own key shape, not the normalised one.
        out: dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]] = {}
        for key in keys:
            hit = by_normalised.get((key[0], key[1], self._sv(key[2])))
            if hit is not None:
                out[key] = hit
        return out

    def get_revision_info(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> RevisionInfo:
        with self._read() as cur:
            cur.execute(
                f'SELECT d.info FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE i.resource_id = %s AND i.revision_id = %s "
                f"AND i.schema_version = %s",
                (resource_id, revision_id, self._sv(schema_version)),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(
                    f"Resource not found: {resource_id}/{revision_id}/{schema_version}"
                )
            return self._info_serializer.decode(bytes(row[0]))

    def save(self, info: RevisionInfo, data: IO[bytes]) -> None:
        uid = str(info.uid)
        raw_data = data.read()
        raw_info = self._info_serializer.encode(info)

        with self._transaction() as cur:
            # Upsert data row
            cur.execute(
                f'INSERT INTO "{self._data_table}" (uid, data, info) '
                f"VALUES (%s, %s, %s) "
                f"ON CONFLICT (uid) DO UPDATE SET data = EXCLUDED.data, info = EXCLUDED.info",
                (uid, raw_data, raw_info),
            )
            # Upsert index row
            cur.execute(
                f'INSERT INTO "{self._index_table}" '
                f"(resource_id, revision_id, schema_version, uid) "
                f"VALUES (%s, %s, %s, %s) "
                f"ON CONFLICT (resource_id, revision_id, schema_version) "
                f"DO UPDATE SET uid = EXCLUDED.uid",
                (
                    info.resource_id,
                    info.revision_id,
                    self._sv(info.schema_version),
                    uid,
                ),
            )

    # -- Bulk read (#434) -------------------------------------------------

    def _key_tuples(
        self, items: "Sequence[tuple[str, str, str | None]]"
    ) -> tuple[tuple[str, str, str], ...]:
        return tuple((rid, rev, self._sv(sv)) for rid, rev, sv in items)

    def payload_sizes(
        self, items: "Sequence[tuple[str, str, str | None]]"
    ) -> list[int]:
        """Size the batch with ``octet_length`` — one query, no bytes moved.

        This is why the memory budget can be honoured *exactly* here rather
        than overshooting: Postgres reports how big each payload is without
        sending it, so the packing decision is made before the transfer.

        Rows that are absent size as 0; ``read_payloads`` then omits them.
        """
        if not items:
            return []
        keys = self._key_tuples(items)
        with self._read() as cur:
            cur.execute(
                f"SELECT i.resource_id, i.revision_id, i.schema_version, "
                f"octet_length(d.data) "
                f'FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE (i.resource_id, i.revision_id, i.schema_version) IN %s",
                (keys,),
            )
            found = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
        return [found.get(key, 0) for key in keys]

    def read_payloads(
        self, items: "Sequence[tuple[str, str, str | None]]"
    ) -> dict[str, bytes]:
        """Fetch the whole batch in one query instead of one per row."""
        if not items:
            return {}
        keys = self._key_tuples(items)
        with self._read() as cur:
            cur.execute(
                f"SELECT i.resource_id, d.data "
                f'FROM "{self._data_table}" d '
                f'JOIN "{self._index_table}" i ON d.uid = i.uid '
                f"WHERE (i.resource_id, i.revision_id, i.schema_version) IN %s",
                (keys,),
            )
            return {row[0]: bytes(row[1]) for row in cur.fetchall()}

    def save_many(
        self, items: Iterable[tuple[RevisionInfo, bytes | IO[bytes]]]
    ) -> None:
        """Bulk save multiple revisions in a single transaction."""
        item_list = list(items)
        if not item_list:
            return

        with self._transaction() as cur:
            data_rows: list[tuple[str, bytes, bytes]] = []
            index_rows: list[tuple[str, str, str, str]] = []
            for info, data in item_list:
                raw = data.read() if hasattr(data, "read") else data  # ty:ignore[call-non-callable]
                uid = str(info.uid)
                raw_info = self._info_serializer.encode(info)
                data_rows.append((uid, raw, raw_info))
                index_rows.append(
                    (
                        info.resource_id,
                        info.revision_id,
                        self._sv(info.schema_version),
                        uid,
                    )
                )

            execute_batch(
                cur,
                f'INSERT INTO "{self._data_table}" (uid, data, info) '
                f"VALUES (%s, %s, %s) "
                f"ON CONFLICT (uid) DO UPDATE SET data = EXCLUDED.data, info = EXCLUDED.info",
                data_rows,
            )
            execute_batch(
                cur,
                f'INSERT INTO "{self._index_table}" '
                f"(resource_id, revision_id, schema_version, uid) "
                f"VALUES (%s, %s, %s, %s) "
                f"ON CONFLICT (resource_id, revision_id, schema_version) "
                f"DO UPDATE SET uid = EXCLUDED.uid",
                index_rows,
            )

    def purge_resource(self, resource_id: str) -> None:
        """Hard-delete all revision data for a resource."""
        with self._read() as cur:
            # Collect UIDs that belong exclusively to this resource
            # (in case of UID deduplication across resources we must not
            # remove data rows still referenced by other resources)
            cur.execute(
                f'SELECT DISTINCT uid FROM "{self._index_table}" '
                f"WHERE resource_id = %s",
                (resource_id,),
            )
            uids = [row[0] for row in cur.fetchall()]

            # Remove index rows first (FK constraint)
            cur.execute(
                f'DELETE FROM "{self._index_table}" WHERE resource_id = %s',
                (resource_id,),
            )

            # Remove data rows only if no other index references them. As one
            # statement: per-uid this was two round-trips each (ask, then maybe
            # delete), so the cost grew with the revision count — and it raced,
            # since another writer can add a reference between the ask and the
            # delete. NOT EXISTS decides and deletes in the same breath.
            if uids:
                cur.execute(
                    f'DELETE FROM "{self._data_table}" d '
                    f"WHERE d.uid = ANY(%s) AND NOT EXISTS ("
                    f'SELECT 1 FROM "{self._index_table}" i WHERE i.uid = d.uid)',
                    (list(uids),),
                )

    def delete_revisions(self, resource_id: str, revision_ids: list[str]) -> None:
        """Hard-delete specific revisions, ref-counting shared uids.

        Mirrors ``purge_resource`` but scoped to *revision_ids* (all of their
        schema versions). Idempotent: revision ids without index rows are
        no-ops. A data row is removed only once no surviving index row — for
        any resource — still references its uid.
        """
        if not revision_ids:
            return
        rev_list = list(revision_ids)
        with self._read() as cur:
            # UIDs referenced by the revisions we're about to drop.
            cur.execute(
                f'SELECT DISTINCT uid FROM "{self._index_table}" '
                f"WHERE resource_id = %s AND revision_id = ANY(%s)",
                (resource_id, rev_list),
            )
            uids = [row[0] for row in cur.fetchall()]
            if not uids:
                return

            # Remove index rows first (FK constraint).
            cur.execute(
                f'DELETE FROM "{self._index_table}" '
                f"WHERE resource_id = %s AND revision_id = ANY(%s)",
                (resource_id, rev_list),
            )

            # Remove data rows only if no other index references them. As one
            # statement: per-uid this was two round-trips each (ask, then maybe
            # delete), so the cost grew with the revision count — and it raced,
            # since another writer can add a reference between the ask and the
            # delete. NOT EXISTS decides and deletes in the same breath.
            if uids:
                cur.execute(
                    f'DELETE FROM "{self._data_table}" d '
                    f"WHERE d.uid = ANY(%s) AND NOT EXISTS ("
                    f'SELECT 1 FROM "{self._index_table}" i WHERE i.uid = d.uid)',
                    (list(uids),),
                )

    def cleanup(self) -> None:
        """Remove all data (both tables). Useful for test teardown."""
        with self._transaction() as cur:
            cur.execute(f'DELETE FROM "{self._index_table}"')
            cur.execute(f'DELETE FROM "{self._data_table}"')

    def drop_tables(self) -> None:
        """Drop both tables. Useful for test teardown."""
        with self._transaction() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{self._index_table}" CASCADE')
            cur.execute(f'DROP TABLE IF EXISTS "{self._data_table}" CASCADE')
