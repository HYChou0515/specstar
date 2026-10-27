import logging
import time
import warnings
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from enum import Enum as EnumType
from typing import Any

import msgspec
from msgspec import UNSET

from specstar.errors import SpecStarWarning
from specstar.query_types import (
    AggKeyRef,
    AggSpec,
    DataSearchFilter,
    DataSearchGroup,
    DataSearchLogicOperator,
    DataSearchOperator,
    DataSearchQuantifier,
    FieldTransform,
    ResourceMetaSearchQuery,
    ResourceMetaSearchSort,
    ResourceMetaSortDirection,
    TrigramFuzzyCondition,
    TrigramSimilaritySort,
    VectorDistanceCondition,
    VectorDistanceSort,
)
from specstar.resource_manager import _pg_pool
from specstar.resource_manager.basic import (
    _LIST_SCALAR_ONLY_OPS,
    Encoding,
    IMetaWithAgg,
    IMetaWithCount,
    ISlowMetaStore,
    MsgspecSerializer,
    bad_list_string_op,
    get_sort_fn,
    is_match_query,
)
from specstar.types import ResourceMeta

_RANGE_OPS = {
    DataSearchOperator.greater_than: ">",
    DataSearchOperator.greater_than_or_equal: ">=",
    DataSearchOperator.less_than: "<",
    DataSearchOperator.less_than_or_equal: "<=",
}


def _gin_probeable(condition: "DataSearchFilter", value: Any) -> bool:
    """Can *value* be matched by a top-level ``indexed_data @> …`` probe?

    Two cases must keep the old ``->>`` path (#416):

    * a ``transform`` compares a DERIVED value (``length`` etc.), while ``@>``
      only ever matches what is actually stored;
    * ``None`` — the reference matcher returns Unknown for ``field == None``
      (``basic.py``), and so does ``->>' f' = 'None'``. ``@> '{"f": null}'``
      would instead MATCH rows storing a JSON null, which is a behaviour
      change, not a speed-up.
    """
    return getattr(condition, "transform", None) is None and value is not None


def _containment(field_path: str, value: Any) -> tuple[str, list]:
    """A top-level containment probe, served by the table's ``indexed_data`` GIN.

    The value is encoded with :func:`json.dumps` — symmetric with the write path
    (``_meta_row_values``) — so its JSON type survives. This matters: ``@>`` is
    type-strict and silently returns ZERO rows on a mismatch, so the ``str(value)``
    the old path used would turn a perf fix into wrong results. Type-strictness
    also brings Postgres in line with the reference matcher, which compares with
    a plain Python ``==``.
    """
    import json

    return "indexed_data @> %s::jsonb", [
        json.dumps({field_path: value}, ensure_ascii=False)
    ]


def _like_escape(value: str) -> str:
    """Escape the LIKE metacharacters so a needle is matched literally.

    Backslash first (it is the default escape char), then ``%`` / ``_``.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _json_text_safe(value: str) -> bool:
    """Does *value* appear verbatim inside a jsonb array's ``->>`` text?

    The coarse prefilter runs over ``indexed_data->>'field'``, which for a list
    is the SERIALISED array text (``["mol", "capping"]``) — there ``"`` and ``\\``
    and control chars are JSON-escaped, so a needle containing one of them would
    NOT be a substring of the serialised text even when an element contains it,
    and the coarse would wrongly DROP that row (a false negative the recheck
    cannot repair). A needle free of those characters appears unchanged inside the
    serialised text, so the coarse stays a correct superset. Anything else falls
    back to the exact EXISTS scan — correct, just unaccelerated.
    """
    return not any(c in '"\\' or ord(c) < 0x20 for c in value)


# The ``.any()`` substring family whose coarse ``LIKE '%v%'`` prefilter over the
# serialised-array text is a correct SUPERSET: "some element contains / starts
# with / ends with v" all imply "v occurs in the array text". Anchored regex
# does NOT (``^v$`` binds to a single element), so it is deliberately excluded.
_TRGM_COARSE_OPS = frozenset(
    {
        DataSearchOperator.contains,
        DataSearchOperator.starts_with,
        DataSearchOperator.ends_with,
    }
)


logger = logging.getLogger(__name__)

#: The base indexes every meta table carries: (name suffix, the fixed name
#: 0.13.0a4 and earlier gave it, what follows ``ON "table"``).
#:
#: Their names used to be those fixed names. Postgres index names are unique per
#: SCHEMA, not per table, and the DDL said ``IF NOT EXISTS`` — so in a schema
#: holding several SpecStar tables the first table took every name and each later
#: table got a NOTICE and no index: its ``==`` / ``in_`` probes, which only this
#: GIN serves (#416), scanned every row. A name is now per table
#: (:func:`base_index_name`).
BASE_INDEXES: tuple[tuple[str, str, str], ...] = (
    ("created_time", "idx_created_time", "(created_time)"),
    ("updated_time", "idx_updated_time", "(updated_time)"),
    ("created_by", "idx_created_by", "(created_by)"),
    ("updated_by", "idx_updated_by", "(updated_by)"),
    ("is_deleted", "idx_is_deleted", "(is_deleted)"),
    ("rev_status", "idx_rev_status", "(rev_status)"),
    ("rev_created_by", "idx_rev_created_by", "(rev_created_by)"),
    ("rev_updated_by", "idx_rev_updated_by", "(rev_updated_by)"),
    ("rev_created_time", "idx_rev_created_time", "(rev_created_time)"),
    ("rev_updated_time", "idx_rev_updated_time", "(rev_updated_time)"),
    ("indexed_data_gin", "idx_indexed_data_gin", "USING GIN (indexed_data)"),
)

#: Postgres truncates a longer identifier SILENTLY, which would make two long
#: table names sharing their first 63 bytes collide again.
_PG_IDENTIFIER_BYTES = 63


def base_index_name(table_name: str, suffix: str) -> str:
    """The name of ``table_name``'s base index ``suffix`` (see :data:`BASE_INDEXES`).

    ``idx_{table}_{suffix}``; when that is longer than Postgres's 63 bytes, the
    table part is cut and an 8-hex digest of the WHOLE table name keeps it
    distinct. Deterministic, so pods agree and :func:`base_index_statements`
    builds exactly what a boot looks for.
    """
    name = f"idx_{table_name}_{suffix}"
    if len(name.encode()) <= _PG_IDENTIFIER_BYTES:
        return name
    import hashlib

    tail = (
        f"_{hashlib.blake2b(table_name.encode(), digest_size=4).hexdigest()}_{suffix}"
    )
    room = _PG_IDENTIFIER_BYTES - len("idx_") - len(tail.encode())
    head = table_name.encode()[: max(room, 0)].decode("utf-8", "ignore")
    return f"idx_{head}{tail}"


def base_index_statements(table_name: str) -> list[str]:
    """``CREATE INDEX CONCURRENTLY`` for each of ``table_name``'s base indexes.

    For building them ahead of an upgrade, while the old version serves: run them
    one at a time (``CONCURRENTLY`` refuses a transaction) as the table's owner,
    and the upgraded boot finds every index and builds nothing. A build killed
    midway leaves an INVALID index under its name; the boot drops and rebuilds
    those.
    """
    return [
        f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{base_index_name(table_name, suffix)}" '
        f'ON "{table_name}" {ddl}'
        for suffix, _, ddl in BASE_INDEXES
    ]


def _condition_has_trigram(cond: object) -> bool:
    """Whether *cond* is (or, for a group, contains) a ``TrigramFuzzyCondition``."""
    if isinstance(cond, TrigramFuzzyCondition):
        return True
    if isinstance(cond, DataSearchGroup):
        return any(_condition_has_trigram(sub) for sub in cond.conditions)
    return False


def _query_uses_trigram(query: ResourceMetaSearchQuery) -> bool:
    """Whether *query* filters with ``.fuzzy()`` or sorts by ``.similarity()`` —
    the pg_trgm-backed operators. Used to route to the Python fallback when the
    Postgres extension is absent."""
    conds = query.conditions if query.conditions is not UNSET else []
    if any(_condition_has_trigram(c) for c in conds):
        return True
    sorts = query.sorts if query.sorts is not UNSET else []
    return any(isinstance(s, TrigramSimilaritySort) for s in sorts)


#: Longest ``in_list`` still expanded into one GIN probe per value.
#:
#: The probes are BitmapOr'd across the single GIN, which is a large win while the
#: list is short and a severe loss once it is not: Postgres plans and OR's one
#: branch PER VALUE, so the cost grows superlinearly, whereas the flat predicate
#: below is a Seq Scan whose cost is independent of the list. Measured, 60k rows:
#:
#:     N        probes (plan+exec)     flat
#:     1              0.15 ms          ~7 ms
#:     100            5.95 ms          ~8 ms    <- probes still ahead
#:     200           14.33 ms          ~8 ms    <- crossover passed
#:     1000         168.25 ms          ~8 ms
#:     2000         977.00 ms         ~15 ms    <- 64x slower
#:
#: 64 sits comfortably below the ~150 crossover. It is deliberately not tuned to
#: the crossover itself: the probes' advantage over a scan is small near it (6 ms
#: vs 8 ms at N=100) while the loss past it is unbounded, so the risk is one-sided.
#: The true crossover also moves with table size — a scan costs more on a bigger
#: table, which buys the probes room — so a fixed limit is a heuristic, chosen to
#: keep the common short-list case fast without ever letting the tail blow up.
IN_LIST_PROBE_LIMIT = 64


try:
    import psycopg2 as pg
    import psycopg2.pool
    from psycopg2.extras import DictCursor, execute_batch
except ImportError:  # pragma: no cover
    pg = None  # type: ignore[assignment]  # ty:ignore[invalid-assignment]
    psycopg2 = None  # type: ignore[assignment]  # noqa: F811  # ty:ignore[invalid-assignment]
    DictCursor = None  # type: ignore[assignment,misc]  # ty:ignore[invalid-assignment]
    execute_batch = None  # type: ignore[assignment]  # ty:ignore[invalid-assignment]


# psycopg2 fetches a named cursor in batches of this many rows (its `itersize`
# default). A result that fits in one batch is materialised whole regardless of
# which cursor asked for it.
_STREAM_BATCH_ROWS = 2000


class PostgresMetaStore(IMetaWithAgg, IMetaWithCount, ISlowMetaStore):
    """PostgreSQL-backed metadata store.

    All stores resolving to the same DSN share one process-global
    connection pool (#380), so connection count scales with the number of
    distinct DSNs rather than the number of models. ``minconn`` defaults to
    ``0`` (lazy) and ``maxconn`` to ``16`` — a per-process, per-DSN ceiling
    shared across every store on that DSN, tunable for high concurrency.
    """

    def __init__(
        self,
        pg_dsn: str,
        encoding: Encoding = Encoding.json,
        *,
        table_name: str = "resource_meta",
        minconn: int = _pg_pool.DEFAULT_MINCONN,
        maxconn: int = _pg_pool.DEFAULT_MAXCONN,
    ):
        self._serializer = MsgspecSerializer(
            encoding=encoding,
            resource_type=ResourceMeta,
        )

        # Share one process-global pool per DSN (#380). The pool is owned by
        # the registry for the process lifetime; this store never closes it.
        self._pg_dsn = pg_dsn
        self._conn_pool = _pg_pool.get_pool(pg_dsn, minconn=minconn, maxconn=maxconn)
        self.table_name = table_name
        # field_path → (indexed_dim, distance) for pgvector columns
        self._vec_columns: dict[str, tuple[int, str]] = {}
        # Field paths registered as list-typed. ``contains`` on these uses
        # JSONB ``@>`` instead of the substring-based ``LIKE``. See #362.
        self._list_fields: set[str] = set()
        # field_path → pg element type (e.g. "text") for SetIndex array columns
        self._set_columns: dict[str, str] = {}
        # Field paths carrying a btree expression index over (indexed_data->'f').
        # Ranges on these use the indexable jsonb form rather than a per-row
        # numeric cast. See #418.
        self._sort_indexes: set[str] = set()
        # Field paths carrying a pg_trgm GIN over (indexed_data->>'f'). Substring
        # LIKE / fuzzy word_similarity on these become index-served; for a list
        # field the array text is coarse-filtered before an exact recheck.
        self._trigram_indexes: set[str] = set()

        # 初始化 PostgreSQL 表
        self._init_postgres_table()
        self._has_pgvector = self._detect_pgvector()
        self._has_pg_trgm = self._detect_pg_trgm()

    def _detect_pgvector(self) -> bool:
        # Use a dedicated raw connection to avoid touching the pool — pool
        # mock-based tests rely on a fixed iteration count.
        try:
            conn = psycopg2.connect(self._pg_dsn)
        except BaseException:
            return False
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_extension WHERE extname = 'vector' LIMIT 1"
                )
                return cur.fetchone() is not None
        except BaseException:
            return False
        finally:
            try:
                conn.close()
            except BaseException:
                pass

    @property
    def supports_native_vector_search(self) -> bool:
        return self._has_pgvector

    def _detect_pg_trgm(self) -> bool:
        # Dedicated raw connection, same as _detect_pgvector: never touch the pool.
        try:
            conn = psycopg2.connect(self._pg_dsn)
        except BaseException:
            return False
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm' LIMIT 1"
                )
                return cur.fetchone() is not None
        except BaseException:
            return False
        finally:
            try:
                conn.close()
            except BaseException:
                pass

    @property
    def supports_native_trigram_search(self) -> bool:
        return self._has_pg_trgm

    def register_list_field(self, field_path: str) -> None:
        """Declare *field_path* as a list-typed indexed field.

        Routes ``DataSearchOperator.contains`` on this field through JSONB
        ``@>`` (true element containment) instead of the default ``LIKE``
        substring match. Idempotent — safe to call from ``add_model`` on
        every registration.

        See #362.
        """
        self._list_fields.add(field_path)

    # ------------------------------------------------------------------
    # Vector / pgvector DDL helpers
    # ------------------------------------------------------------------

    # pgvector HNSW upper bound for indexable dim.
    HNSW_MAX_DIM = 2000

    _DISTANCE_OPS = {
        "cosine": ("vector_cosine_ops", "<=>"),
        "l2": ("vector_l2_ops", "<->"),
        "ip": ("vector_ip_ops", "<#>"),
    }

    def _vec_col_name(self, field_path: str) -> str:
        # JSON paths use ".", which is invalid in column identifiers
        return "vec_" + field_path.replace(".", "_")

    def ensure_vector_column(
        self,
        field_path: str,
        *,
        dim: int,
        distance: str = "cosine",
    ) -> None:
        """Create a pgvector column + HNSW index for *field_path*.

        Idempotent: skips when the column / index already exists.  When
        ``dim`` exceeds :pyattr:`HNSW_MAX_DIM`, only the first
        ``HNSW_MAX_DIM`` dimensions are indexed (Matryoshka prefix
        strategy).
        """
        if not self._has_pgvector:
            raise RuntimeError(
                "PostgresMetaStore: pgvector extension is required for "
                "vector fields. Run `CREATE EXTENSION vector;` as superuser."
            )

        ops_class, _op = self._DISTANCE_OPS.get(distance, self._DISTANCE_OPS["cosine"])
        col = self._vec_col_name(field_path)
        indexed_dim = min(dim, self.HNSW_MAX_DIM)
        idx_name = f"idx_{self.table_name}_{col}_hnsw"

        # ALTER TABLE / CREATE INDEX CONCURRENTLY cannot run inside a
        # transaction.  Pool connections are managed with autocommit=False,
        # so open a dedicated raw connection for DDL and close it after.
        conn = psycopg2.connect(self._pg_dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{self.table_name}" '
                    f'ADD COLUMN IF NOT EXISTS "{col}" vector({indexed_dim})'
                )
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                    f'ON "{self.table_name}" '
                    f'USING hnsw ("{col}" {ops_class})'
                )
        finally:
            conn.close()

        self._vec_columns[field_path] = (indexed_dim, distance)

    # ------------------------------------------------------------------
    # SetIndex (array set-overlap) DDL helpers
    # ------------------------------------------------------------------

    # Python element type → Postgres array element type.
    _SET_ELEM_TYPES: dict[type, str] = {
        str: "text",
        int: "bigint",
        float: "double precision",
    }

    def _set_col_name(self, field_path: str) -> str:
        return "set_" + field_path.replace(".", "_")

    def ensure_set_column(self, field_path: str, elem_type: type = str) -> None:
        """Create a dedicated array column + GIN for *field_path* so
        ``contains_any`` runs as one indexed ``&&`` overlap instead of a
        per-candidate ``@>`` fan-out on the shared index.

        Idempotent; mirrors :meth:`ensure_vector_column` (pgvector). Other
        backends serve ``contains_any`` from the shared path and need no
        shadow column, so this is Postgres-only native acceleration.

        Called from ``add_model``, i.e. on every process start. The column is
        reconciled against ``indexed_data`` BEFORE the field is registered —
        registering is what switches ``contains_any`` onto the ``&&`` fast path,
        and that fast path over a column which is NULL (never filled) or stale
        (filled, then the row changed while the annotation was absent) silently
        returns wrong rows rather than failing (#417). Reconciling, rather than
        filling the blanks, is what makes re-adding a removed annotation safe;
        it is also idempotent, so a restart on a clean table costs zero writes.
        """
        pg_elem = self._SET_ELEM_TYPES.get(elem_type, "text")
        col = self._set_col_name(field_path)
        idx_name = f"idx_{self.table_name}_{col}_gin"
        # ALTER TABLE / CREATE INDEX can't run inside the pool's txn; use a
        # dedicated autocommit connection for DDL (same as ensure_vector_column).
        conn = psycopg2.connect(self._pg_dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{self.table_name}" '
                    f'ADD COLUMN IF NOT EXISTS "{col}" {pg_elem}[]'
                )
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                    f'ON "{self.table_name}" USING GIN ("{col}")'
                )
        finally:
            conn.close()
        # Order matters: populate first, register second. Mirrors the startup
        # backfill _migrate_existing_records already does for indexed_data.
        self._run_set_backfill(field_path, pg_elem)
        self._set_columns[field_path] = pg_elem

    def _sort_idx_name(self, field_path: str) -> str:
        # JSON paths use ".", which is invalid in an identifier
        return f"idx_{self.table_name}_sort_{field_path.replace('.', '_')}"

    def ensure_sort_index(self, field_path: str) -> None:
        """Create a btree index over ``(indexed_data->'field_path')`` (#418).

        Unlike :meth:`ensure_set_column` there is no column, no write-path change
        and nothing to backfill — the index is a pure derivation of live
        ``indexed_data``, maintained by Postgres, so it can never be stale and has
        no window where queries are wrong. ``CONCURRENTLY`` keeps the build online
        (it cannot run inside a transaction, hence the dedicated connection —
        same as :meth:`ensure_vector_column`).

        The extract is indexed WITHOUT a cast. Indexing
        ``((indexed_data->>'f')::numeric)`` would be faster and estimate better,
        but it makes the index a write-time constraint: any row whose value is not
        castable is rejected outright (``invalid input syntax for type numeric``),
        so declaring the annotation could start rejecting writes that previously
        succeeded, and the build itself fails on legacy data. jsonb extraction is
        total, and jsonb orders numbers numerically, so this form cannot fail.

        Idempotent; safe to call from ``add_model`` on every process start, and
        from several pods at once (see :meth:`_build_sort_index_ddl`).
        """
        # Register FIRST, and never conditionally. The emitted SQL must follow the
        # ANNOTATION — identical code on every pod — not the outcome of the DDL
        # below. If registration could be skipped by a lost race or a raised DDL,
        # pods would emit different SQL for the same query; on a string field the
        # two forms do not merely differ in speed, the numeric-cast one raises.
        # Whether the index exists only ever decides Seq Scan vs Index Scan.
        self._sort_indexes.add(field_path)
        self._build_sort_index_ddl(field_path)

    def _build_sort_index_ddl(self, field_path: str) -> None:
        """Best-effort: get the index built, without ever failing the caller.

        "Best-effort" is load-bearing and was previously only half-true: the pods
        that LOST the race skipped safely, but the one that won propagated any DDL
        error straight out of ``add_model`` — the boot path. Nothing about this
        index is worth a failed boot, so every failure below is swallowed and
        logged.

        Postgres permits only ONE concurrent index build per table at a time, so
        N pods reaching ``CREATE INDEX CONCURRENTLY`` together deadlock each other
        (measured: 4 of 6 pods died with DeadlockDetected — a CrashLoopBackOff on
        the first rollout after the annotation ships). An advisory lock elects one
        builder; the losers simply skip, which is safe precisely because a missing
        index costs speed and nothing else. The index lives in Postgres, not in
        the pod, so once the winner is done every pod's next query uses it — no
        restart, no reconnect.

        A build killed mid-flight leaves an INVALID index behind, and
        ``IF NOT EXISTS`` matches BY NAME, so it would skip the repair forever
        while the planner ignores the index — the annotation silently dead. Drop
        it first when we find one.
        """
        try:
            self._try_build_sort_index(field_path)
        except Exception:
            # "Best-effort" has to mean it. This runs from add_model, on every
            # process's boot path, and CREATE INDEX CONCURRENTLY fails for reasons
            # that have nothing to do with the caller: a statement_timeout, a
            # cancelled backend, the pod killed mid-rollout, disk pressure. The
            # index is DERIVED metadata — present it is fast, absent it is slow —
            # so a failed build must degrade to slow, never take the process down.
            # Raising here also made the damage self-sustaining: a killed build
            # leaves an INVALID index, and the repair for it lives at the top of
            # this very function, i.e. on the next boot — which is the thing that
            # was crashing. That is a crash loop caused by an index whose entire
            # job is to make one sort faster.
            logger.warning(
                "sort index build for %r on %r failed; queries stay correct but "
                "unaccelerated until a later boot rebuilds it",
                field_path,
                self.table_name,
                exc_info=True,
            )

    def _try_build_sort_index(self, field_path: str) -> None:
        """The DDL itself. Raises; :meth:`_build_sort_index_ddl` is the guard."""
        idx_name = self._sort_idx_name(field_path)
        # Stable across pods, distinct per (table, field), and fits in bigint.
        lock_key = self._advisory_key(f"sort_index:{self.table_name}:{field_path}")
        conn = psycopg2.connect(self._pg_dsn)
        conn.autocommit = True  # CONCURRENTLY cannot run inside a transaction
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
                if not cur.fetchone()[0]:
                    return  # another pod is on it; ours would only deadlock
                try:
                    cur.execute(
                        "SELECT indisvalid FROM pg_index "
                        "WHERE indexrelid = to_regclass(%s)",
                        [idx_name],
                    )
                    row = cur.fetchone()
                    if row is not None and not row[0]:
                        cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{idx_name}"')
                    cur.execute(
                        f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{idx_name}" '
                        f"ON \"{self.table_name}\" ((indexed_data->'{field_path}'))"
                    )
                finally:
                    # Best-effort too: if the backend was cancelled the connection
                    # may be unusable, and raising HERE would mask the real error
                    # with a confusing one. Postgres releases session advisory
                    # locks when the connection drops, which conn.close() below
                    # guarantees — so the lock is never stranded either way.
                    try:
                        cur.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                    except Exception:
                        logger.debug(
                            "advisory unlock for %r failed; the closing connection "
                            "releases it",
                            field_path,
                            exc_info=True,
                        )
        finally:
            conn.close()

    @staticmethod
    def _advisory_key(name: str) -> int:
        """A stable signed-bigint key for pg_advisory_lock.

        Must not depend on PYTHONHASHSEED: pods have to agree.
        """
        import hashlib

        digest = hashlib.blake2b(name.encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big", signed=True)

    def _trigram_idx_name(self, field_path: str) -> str:
        # JSON paths use ".", which is invalid in an identifier
        return f"idx_{self.table_name}_trgm_{field_path.replace('.', '_')}"

    def ensure_trigram_index(self, field_path: str) -> None:
        """Create a pg_trgm GIN over ``(indexed_data->>'field_path')`` (TrigramIndex).

        Like :meth:`ensure_sort_index` there is no column, no write-path change and
        nothing to backfill — the index is a pure derivation of live
        ``indexed_data``, maintained by Postgres, so it can never be stale and has
        no window where queries are wrong. The GIN serves substring ``LIKE`` and
        ``word_similarity`` over the text extract, which is exactly what a scalar
        ``.contains`` / ``.fuzzy`` already emits; for a ``list[str]`` field the same
        extract yields the serialised-array text, which the ``.any()`` rewrite
        coarse-filters before an exact per-element recheck.

        Idempotent; safe to call from ``add_model`` on every process start, and
        from several pods at once (see :meth:`_build_trigram_index_ddl`).
        """
        # Register FIRST and unconditionally — the emitted SQL follows the
        # ANNOTATION, identical on every pod, never the outcome of the DDL below.
        # The coarse ``ILIKE`` prefilter the ``.any()`` rewrite adds is a correct
        # superset with or without the index; the index only decides Seq Scan vs
        # Bitmap. Same discipline as :meth:`ensure_sort_index`.
        self._trigram_indexes.add(field_path)
        self._build_trigram_index_ddl(field_path)

    def _build_trigram_index_ddl(self, field_path: str) -> None:
        """Best-effort: get the GIN built, without ever failing the caller.

        Runs from ``add_model`` on every boot. ``CREATE EXTENSION`` needs a
        privilege the app role may not have, and ``CREATE INDEX CONCURRENTLY``
        fails for reasons unrelated to the caller (statement_timeout, a cancelled
        backend, a pod killed mid-rollout). The index is DERIVED metadata — present
        it is fast, absent it is slow — so every failure degrades to a scan, never
        takes the process down. See :meth:`_build_sort_index_ddl` for the full
        rationale (advisory-lock election, INVALID-index repair, crash-loop trap).
        """
        try:
            self._try_build_trigram_index(field_path)
        except Exception:
            logger.warning(
                "trigram index build for %r on %r failed; queries stay correct but "
                "unaccelerated until a later boot rebuilds it",
                field_path,
                self.table_name,
                exc_info=True,
            )

    def _try_build_trigram_index(self, field_path: str) -> None:
        """The DDL itself. Raises; :meth:`_build_trigram_index_ddl` is the guard."""
        idx_name = self._trigram_idx_name(field_path)
        lock_key = self._advisory_key(f"trigram_index:{self.table_name}:{field_path}")
        conn = psycopg2.connect(self._pg_dsn)
        conn.autocommit = True  # CONCURRENTLY cannot run inside a transaction
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
                if not cur.fetchone()[0]:
                    return  # another pod is on it; ours would only deadlock
                try:
                    # gin_trgm_ops lives in pg_trgm; ensure it before the opclass is
                    # referenced. Best-effort like the rest — if the role can't create
                    # it, the CREATE INDEX below raises and the guard degrades to scan.
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                    cur.execute(
                        "SELECT indisvalid FROM pg_index "
                        "WHERE indexrelid = to_regclass(%s)",
                        [idx_name],
                    )
                    row = cur.fetchone()
                    if row is not None and not row[0]:
                        cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{idx_name}"')
                    cur.execute(
                        f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{idx_name}" '
                        f'ON "{self.table_name}" USING GIN '
                        f"((indexed_data->>'{field_path}') gin_trgm_ops)"
                    )
                finally:
                    try:
                        cur.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                    except Exception:
                        logger.debug(
                            "advisory unlock for %r failed; the closing connection "
                            "releases it",
                            field_path,
                            exc_info=True,
                        )
        finally:
            conn.close()

    def _extract_set_value(self, meta: ResourceMeta, field_path: str) -> "list | None":
        """The list value for a SetIndex column, copied from indexed_data."""
        if meta.indexed_data is UNSET or meta.indexed_data is None:
            return None
        v = meta.indexed_data.get(field_path)
        return v if isinstance(v, list) else None

    def _run_set_backfill(self, field_path: str, pg_elem: str) -> int:
        """Reconcile the shadow column with ``indexed_data``. Returns rows fixed.

        Kept separate from :meth:`backfill_set_column` so ``ensure_set_column``
        can run it BEFORE registering the field, i.e. before the ``&&`` fast path
        can read the column (#417).

        The predicate is ``IS DISTINCT FROM``, not ``IS NULL``. "Has this row been
        filled in?" is the wrong question — writes populate the column from
        ``_set_columns``, so a row written while the annotation was absent keeps
        whatever the column held before (``ON CONFLICT DO UPDATE`` never names it).
        Such a row is non-NULL and WRONG, and an ``IS NULL`` predicate skips it: the
        fast path then answers from stale data, which unlike a NULL column also
        returns rows that do not match. The right question is "does this row still
        agree with the source of truth?", which also makes the pass idempotent —
        a clean table costs zero writes, so running it on every boot is cheap.
        """
        col = self._set_col_name(field_path)
        extract = "jsonb_array_elements_text(indexed_data->%s)"
        arr = (
            f"ARRAY(SELECT {extract})"
            if pg_elem == "text"
            else f"ARRAY(SELECT ({extract})::{pg_elem})"
        )
        sql = (
            f'UPDATE "{self.table_name}" SET "{col}" = {arr} '
            f"WHERE jsonb_typeof(indexed_data->%s) = 'array' "
            f'AND "{col}" IS DISTINCT FROM {arr}'
        )
        with self.transaction() as cur:
            cur.execute(sql, [field_path, field_path, field_path])
            return cur.rowcount

    def backfill_set_column(self, field_path: str) -> int:
        """Reconcile the SetIndex shadow column for *field_path* against
        ``indexed_data``. One pushed-down ``UPDATE``; returns rows fixed. No-op
        when the field has no shadow column.

        Analogous to :func:`backfill_vectors` for Vector fields, but the value
        is a pure derivation of ``indexed_data`` (no re-encoding needed), so it
        runs entirely in SQL.

        Since #417 ``ensure_set_column`` reconciles on its own, so this is not
        required to make a newly-declared SetIndex field correct. It remains as
        an explicit repair for a column suspected of drift.
        """
        if field_path not in self._set_columns:
            return 0
        return self._run_set_backfill(field_path, self._set_columns[field_path])

    def _build_vector_condition(
        self, condition: "VectorDistanceCondition"
    ) -> tuple[str, list]:
        if condition.field_path not in self._vec_columns:
            return "", []
        if not isinstance(condition.query_vector, list):
            return "", []  # str query_vector must be resolved upstream
        indexed_dim, default_distance = self._vec_columns[condition.field_path]
        metric = condition.distance or default_distance or "cosine"
        _ops, op_symbol = self._DISTANCE_OPS[metric]
        col = self._vec_col_name(condition.field_path)
        clipped = condition.query_vector[:indexed_dim]
        vec_literal = self._format_vec_literal(clipped)
        op_map = {
            DataSearchOperator.less_than: "<",
            DataSearchOperator.less_than_or_equal: "<=",
            DataSearchOperator.greater_than: ">",
            DataSearchOperator.greater_than_or_equal: ">=",
        }
        sql_op = op_map.get(condition.operator)
        if sql_op is None:
            return "", []
        return (
            f'"{col}" {op_symbol} %s::vector {sql_op} %s',
            [vec_literal, float(condition.threshold)],
        )

    def _build_fuzzy_condition(
        self, condition: "TrigramFuzzyCondition"
    ) -> tuple[str, list]:
        """pg_trgm ``word_similarity`` over the field's ``->>`` text.

        No ``threshold`` → the ``%s <% (indexed_data->>'f')`` operator, which the
        gin_trgm_ops GIN serves at the server's ``pg_trgm.word_similarity_threshold``
        (0.6 by default — permissive enough that a fragment like "mol" matches
        "molecular"). A per-call ``threshold`` pins the cut-off exactly with the
        ``word_similarity(a, b) >= t`` function form; ``<%`` can't (it only reads
        the session GUC), so that form runs as a scan in v1.
        """
        text = f"(indexed_data->>'{condition.field_path}')"
        if condition.threshold is None:
            # ``<%%`` not ``<%``: psycopg2 does %-substitution on the SQL, so the
            # literal ``%`` in the ``<%`` operator has to be doubled or it raises
            # "unsupported format character".
            return f"%s <%% {text}", [condition.query]
        return f"word_similarity(%s, {text}) >= %s", [
            condition.query,
            float(condition.threshold),
        ]

    def _build_trigram_similarity_order(
        self, sort: "TrigramSimilaritySort"
    ) -> tuple[str, list]:
        """``ORDER BY word_similarity(query, indexed_data->>'f')``.

        A ranking sort, not a filter: the gin_trgm_ops GIN accelerates ``<%`` but
        cannot ORDER (only a GiST ``<->`` KNN index could), so this is a
        compute-then-sort. Pair it with a ``.fuzzy()`` filter to keep the set the
        sort runs over small. ``descending`` = most-similar first.
        """
        text = f"(indexed_data->>'{sort.field_path}')"
        direction = (
            "ASC" if sort.direction == ResourceMetaSortDirection.ascending else "DESC"
        )
        return f"word_similarity(%s, {text}) {direction}", [sort.query]

    def _build_vector_order(self, sort: "VectorDistanceSort") -> tuple[str, list]:
        if sort.field_path not in self._vec_columns:
            return "", []
        if not isinstance(sort.query_vector, list):
            return "", []
        indexed_dim, default_distance = self._vec_columns[sort.field_path]
        metric = sort.distance or default_distance or "cosine"
        _ops, op_symbol = self._DISTANCE_OPS[metric]
        col = self._vec_col_name(sort.field_path)
        clipped = sort.query_vector[:indexed_dim]
        vec_literal = self._format_vec_literal(clipped)
        direction = (
            "ASC" if sort.direction == ResourceMetaSortDirection.ascending else "DESC"
        )
        return f'"{col}" {op_symbol} %s::vector {direction}', [vec_literal]

    @staticmethod
    def _format_vec_literal(vec: list[float]) -> str:
        """pgvector accepts vector input as the string '[v1,v2,...]'."""
        return "[" + ",".join(str(float(v)) for v in vec) + "]"

    def _extract_vec_value(
        self, meta: ResourceMeta, field_path: str, indexed_dim: int
    ) -> str | None:
        """Pull the vector value out of meta.indexed_data and clip to indexed_dim."""
        if meta.indexed_data is UNSET or meta.indexed_data is None:
            return None
        # Support dotted paths like "summary.vector"
        cur: object = meta.indexed_data
        for part in field_path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        if not isinstance(cur, list):
            return None
        clipped = cur[:indexed_dim]
        if len(clipped) != indexed_dim:
            return None  # under-dim vector — leave NULL
        return self._format_vec_literal(clipped)

    # The connection pool is shared across all stores on this DSN and owned
    # by the process-global registry (#380); a store must not close it on
    # garbage collection. Use ``_pg_pool.close_all_pools()`` for explicit
    # shutdown / test teardown.

    def get_conn(self) -> Any:
        retry = 5
        last_error: Exception | None = None
        for attempt in range(retry):
            try:
                conn = self._conn_pool.getconn()
            except psycopg2.pool.PoolError:
                # Pool 耗盡，等待後重試（其他 thread 可能會歸還連線）
                last_error = psycopg2.pool.PoolError("connection pool exhausted")
                time.sleep(min(0.5 * (attempt + 1), 3))
                continue

            # Health-check OUTSIDE a transaction. With autocommit off the
            # probe's own `SELECT 1` opens one, and psycopg2 then refuses any
            # later `autocommit` change on this connection — which is what a
            # read-only caller needs to do to skip BEGIN/COMMIT.
            conn.autocommit = True
            if self.test_query(conn):
                conn.autocommit = False  # writers are the default
                return conn

            # test_query 失敗：歸還壞連線並標記關閉，避免洩漏
            try:
                self._conn_pool.putconn(conn, close=True)
            except Exception:
                pass
            last_error = ConnectionError("Failed to get a valid PostgreSQL connection.")
            time.sleep(1)

        if isinstance(last_error, psycopg2.pool.PoolError):
            raise last_error
        raise ConnectionError("Failed to get a valid PostgreSQL connection.")

    def put_conn(self, conn):
        self._conn_pool.putconn(conn)

    def test_query(self, conn) -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
        except Exception:
            return False

    @contextmanager
    def transaction(self):
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.put_conn(conn)

    @contextmanager
    def stream_cursor(self):
        """A server-side (named) cursor, for reads whose result set is unbounded.

        Naming the cursor is what lets postgres stream instead of materialising
        the whole answer, and it costs `DECLARE` / `FETCH` / `CLOSE` — three
        round-trips where an ordinary cursor needs one. Worth it per table scan;
        pure loss per row. A caller that has already bounded its result (a point
        read, a COUNT, a fixed set of keys) wants `row_cursor` instead.
        """
        conn = self.get_conn()
        try:
            # 建立 server-side cursor (named cursor)
            with conn.cursor(
                name="PostgresMetaStore",
                cursor_factory=DictCursor,
            ) as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.put_conn(conn)

    @contextmanager
    def row_cursor(self):
        """An ordinary client-side cursor with dict rows, outside a transaction.

        `stream_cursor` without the `DECLARE`/`FETCH`/`CLOSE` a bounded read
        cannot use, and without the `BEGIN`/`COMMIT` it has nothing to make
        atomic either. `get_conn` turns autocommit off because most callers
        write; a single-statement read turns it back on for its own duration and
        restores it before the connection returns to the pool, so a writer that
        checks the same connection out next is unaffected.
        """
        conn = self.get_conn()
        previous = conn.autocommit
        conn.autocommit = True
        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        finally:
            conn.autocommit = previous
            self.put_conn(conn)

    def _init_postgres_table(self):
        """初始化 PostgreSQL 表結構"""
        with self.transaction() as cur:
            cur.execute("SELECT to_regclass(%s)", [f'"{self.table_name}"'])
            existed = cur.fetchone()[0] is not None
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    resource_id TEXT PRIMARY KEY,
                    data BYTEA NOT NULL,
                    created_time TIMESTAMP NOT NULL,
                    updated_time TIMESTAMP NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    is_deleted BOOLEAN NOT NULL,
                    schema_version TEXT,
                    indexed_data JSONB,  -- JSON 格式的索引數據
                    rev_status TEXT,
                    rev_created_by TEXT,
                    rev_updated_by TEXT,
                    rev_created_time TIMESTAMP,
                    rev_updated_time TIMESTAMP
                )
            """)

            # 檢查是否需要添加 indexed_data 欄位（用於向後兼容）
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{self.table_name}' AND column_name = 'indexed_data'
            """)
            if not cur.fetchone():
                cur.execute(
                    f'ALTER TABLE "{self.table_name}" ADD COLUMN indexed_data JSONB'
                )

            # 檢查是否需要添加 schema_version 欄位（用於向後兼容）
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{self.table_name}' AND column_name = 'schema_version'
            """)
            if not cur.fetchone():
                cur.execute(
                    f'ALTER TABLE "{self.table_name}" ADD COLUMN schema_version TEXT'
                )

            # 為 rev_* 欄位逐一檢查並補欄（向後兼容舊表）
            for col_name, col_type in (
                ("rev_status", "TEXT"),
                ("rev_created_by", "TEXT"),
                ("rev_updated_by", "TEXT"),
                ("rev_created_time", "TIMESTAMP"),
                ("rev_updated_time", "TIMESTAMP"),
            ):
                cur.execute(
                    f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{self.table_name}' AND column_name = '{col_name}'
                    """
                )
                if not cur.fetchone():
                    cur.execute(
                        f'ALTER TABLE "{self.table_name}" ADD COLUMN {col_name} {col_type}'
                    )

            if not existed:
                # An empty table: building them here costs nothing, and a table
                # created by this process has them before its first row.
                for suffix, _, ddl in BASE_INDEXES:
                    name = base_index_name(self.table_name, suffix)
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS "{name}" ON "{self.table_name}" {ddl}'
                    )

            # 遷移已存在的記錄，填充 indexed_data
            self._migrate_existing_data(cur)
        if existed:
            self._ensure_base_indexes()

    def _ensure_base_indexes(self) -> None:
        """Best-effort: give an existing table every base index it lacks.

        A table built by 0.13.0a4 or earlier either holds the fixed names (the
        first table in its schema) or has no base index at all (every other one).
        The first are RENAMED — instant, and no second copy of each index; the
        rest are built ``CONCURRENTLY``, so the table keeps taking writes. Like
        :meth:`_build_sort_index_ddl`: an advisory lock elects one builder among
        pods booting together (concurrent builds on one table deadlock), and no
        failure leaves this method — a missing index costs speed, never
        correctness, so it must never cost a boot. A build killed midway leaves an
        INVALID index under the new name; it is dropped and built again.
        """
        try:
            self._try_ensure_base_indexes()
        except Exception:
            logger.warning(
                "base indexes on %r could not all be built; queries stay correct "
                "but unaccelerated until a later boot completes them "
                "(base_index_statements() builds them by hand)",
                self.table_name,
                exc_info=True,
            )

    def _try_ensure_base_indexes(self) -> None:
        """The DDL itself. Raises; :meth:`_ensure_base_indexes` is the guard."""
        lock_key = self._advisory_key(f"base_indexes:{self.table_name}")
        conn = psycopg2.connect(self._pg_dsn)
        conn.autocommit = True  # CONCURRENTLY cannot run inside a transaction
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
                if not cur.fetchone()[0]:
                    return  # another pod is on it; ours would only deadlock
                try:
                    self._complete_base_indexes(cur)
                finally:
                    try:
                        cur.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                    except Exception:
                        logger.debug(
                            "advisory unlock for the base indexes of %r failed; the "
                            "closing connection releases it",
                            self.table_name,
                            exc_info=True,
                        )
        finally:
            conn.close()

    def _complete_base_indexes(self, cur) -> None:
        cur.execute(
            "SELECT c.relname, i.indisvalid FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE i.indrelid = to_regclass(%s)",
            [f'"{self.table_name}"'],
        )
        on_this_table = dict(cur.fetchall())  # name -> valid
        for suffix, legacy, ddl in BASE_INDEXES:
            name = base_index_name(self.table_name, suffix)
            valid = on_this_table.get(name)
            if valid:
                continue
            if valid is False:
                cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{name}"')
            elif on_this_table.get(legacy):
                # This table took the fixed name: the same index, under a name
                # that no longer blocks every other table in the schema.
                cur.execute(f'ALTER INDEX "{legacy}" RENAME TO "{name}"')
                continue
            elif legacy in on_this_table:  # the fixed name, but invalid
                cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{legacy}"')
            cur.execute(
                f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" '
                f'ON "{self.table_name}" {ddl}'
            )

    def _migrate_existing_data(self, cur):
        """為已存在但沒有 indexed_data 的記錄填充索引數據"""
        import json

        cur.execute(f"""
            SELECT resource_id, data FROM "{self.table_name}" 
            WHERE indexed_data IS NULL
        """)

        for resource_id, data_blob in cur.fetchall():
            try:
                data = self._serializer.decode(data_blob)
                indexed_data_json = (
                    json.dumps(data.indexed_data, ensure_ascii=False)
                    if data.indexed_data is not UNSET
                    else None
                )
                cur.execute(
                    f"""
                    UPDATE "{self.table_name}" SET indexed_data = %s WHERE resource_id = %s
                """,
                    (indexed_data_json, resource_id),
                )
            except Exception:
                # 如果解析失敗，設置為空 JSON 對象
                cur.execute(
                    f"""
                    UPDATE "{self.table_name}" SET indexed_data = %s WHERE resource_id = %s
                """,
                    ("{}", resource_id),
                )

    def _searchable_indexed_data(self, meta: ResourceMeta) -> Any:
        """``meta.indexed_data`` minus the fields that have a pgvector column.

        ``add_model`` puts every Vector field into ``indexed_fields`` on purpose:
        brute-force backends have no vector column and answer similarity straight
        out of ``indexed_data``, and we ourselves read that copy on the way in (see
        ``_extract_vec_value``, called from ``_meta_row_values`` BEFORE this — off
        the in-memory meta, so the column is populated either way).

        On this backend the copy has no reader once the column is written:
        ``_build_vector_condition`` and ``_build_vector_order`` both query
        ``_vec_col_name(...)``, and ``iter_search`` returns metas by decoding the
        ``data`` BYTEA — which still holds the whole ResourceMeta, vector included.
        So nothing observable changes; what goes away is dead weight, and it is not
        the cheap kind. The GIN over ``indexed_data`` indexes every ELEMENT of the
        array, so one 4096-dim embedding costs 4096 index entries per row, and
        every ``@>`` probe rechecks against the fat jsonb. Measured on 5000 rows
        carrying a 4096-dim embedding, against the same rows without it:

            GIN size          43 MB   ->   312 kB     (138x)
            total relation   280 MB   ->   840 kB     (333x)
            @> probe        490.8 ms  ->   1.2 ms     (400x)

        That is what made a real deployment's document list take 15 seconds. The
        bloat predates the #416 rewrite, but nothing USED the GIN while ``->>``
        forced a Seq Scan, so #416 is what woke it up.

        Only a REGISTERED column licenses the strip: ``_vec_columns`` is empty
        unless ``ensure_vector_column`` ran, and without a column ``indexed_data``
        is the only queryable surface left, so the copy stays.

        This strips the JSONB column only, NOT the BYTEA — which is exactly why
        readers cannot tell, and equally why the storage is not reclaimed. Doing
        that changes what readers see and is a separate decision.
        """
        data = meta.indexed_data
        if data is UNSET or not isinstance(data, dict) or not self._vec_columns:
            return data
        # `ensure_vector_column` is registered under the short alias, which IS the
        # indexed_data key (crud.core passes `vinfo.name`) — so a plain key drop.
        return {k: v for k, v in data.items() if k not in self._vec_columns}

    def _meta_row_values(self, meta: ResourceMeta) -> tuple:
        """Pack a ResourceMeta into the column-order tuple used by INSERT/UPSERT."""
        import json

        base = (
            meta.resource_id,
            self._serializer.encode(meta),
            meta.created_time,
            meta.updated_time,
            meta.created_by,
            meta.updated_by,
            meta.is_deleted,
            meta.schema_version,
            (
                json.dumps(self._searchable_indexed_data(meta), ensure_ascii=False)
                if meta.indexed_data is not UNSET
                else None
            ),
            meta.rev_status if meta.rev_status is not UNSET else None,
            meta.rev_created_by if meta.rev_created_by is not UNSET else None,
            meta.rev_updated_by if meta.rev_updated_by is not UNSET else None,
            meta.rev_created_time if meta.rev_created_time is not UNSET else None,
            meta.rev_updated_time if meta.rev_updated_time is not UNSET else None,
        )
        # Append registered vec column values in deterministic order
        for field_path, (indexed_dim, _) in self._vec_columns.items():
            base = base + (self._extract_vec_value(meta, field_path, indexed_dim),)
        # Then SetIndex array column values, same deterministic order.
        for field_path in self._set_columns:
            base = base + (self._extract_set_value(meta, field_path),)
        return base

    def _upsert_sql(self) -> str:
        vec_cols = [self._vec_col_name(fp) for fp in self._vec_columns]
        set_cols = [self._set_col_name(fp) for fp in self._set_columns]
        base_cols = [
            "resource_id",
            "data",
            "created_time",
            "updated_time",
            "created_by",
            "updated_by",
            "is_deleted",
            "schema_version",
            "indexed_data",
            "rev_status",
            "rev_created_by",
            "rev_updated_by",
            "rev_created_time",
            "rev_updated_time",
        ]
        all_cols = base_cols + vec_cols + set_cols
        col_list = ", ".join(f'"{c}"' for c in all_cols)
        # vec values are passed as strings cast to vector; set values as lists
        # cast to the column's array type (psycopg2 adapts a list → array).
        placeholders = ", ".join(
            ["%s"] * len(base_cols)
            + ["%s::vector"] * len(vec_cols)
            + [f"%s::{self._set_columns[fp]}[]" for fp in self._set_columns]
        )
        update_clause = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in all_cols if c != "resource_id"
        )
        return (
            f'INSERT INTO "{self.table_name}" ({col_list}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (resource_id) DO UPDATE SET {update_clause}"
        )

    def save_many(self, metas: Iterable[ResourceMeta]) -> None:
        """批量保存元数据到 PostgreSQL（ISlowMetaStore 接口方法）"""
        metas_list = list(metas)
        if not metas_list:
            return

        with self.transaction() as cur:
            execute_batch(
                cur,
                self._upsert_sql(),
                [self._meta_row_values(m) for m in metas_list],
            )

    def values(self):
        """Streaming bulk read — single ``SELECT data`` instead of N+1 queries."""
        with self.row_cursor() as cur:
            cur.execute(f'SELECT data FROM "{self.table_name}"')
            for row in cur:
                yield self._serializer.decode(row["data"])

    def __getitem__(self, pk: str) -> ResourceMeta:
        # 直接從 PostgreSQL 查詢
        with self.row_cursor() as cur:
            cur.execute(
                f'SELECT data FROM "{self.table_name}" WHERE resource_id = %s', (pk,)
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(pk)
            return self._serializer.decode(row["data"])

    def get_many(self, pks: "Sequence[str]") -> "dict[str, ResourceMeta]":
        """Bulk read in one ``WHERE resource_id = ANY(...)`` (#434).

        The inherited default loops ``__getitem__``, which on a remote
        database is one round-trip per row — the very cost a fan-out batches
        to avoid. Missing ids are simply absent from the result.
        """
        pk_list = list(pks)
        if not pk_list:
            return {}
        with self.row_cursor() as cur:
            cur.execute(
                f'SELECT resource_id, data FROM "{self.table_name}" '
                f"WHERE resource_id = ANY(%s)",
                (pk_list,),
            )
            return {
                row["resource_id"]: self._serializer.decode(row["data"])
                for row in cur.fetchall()
            }

    def __setitem__(self, pk: str, meta: ResourceMeta) -> None:  # ty:ignore[invalid-method-override]
        # 直接寫入 PostgreSQL — honour caller-supplied ``pk`` even if it
        # differs from ``meta.resource_id`` for compat with dict-style usage.
        row = (pk, *self._meta_row_values(meta)[1:])
        with self.transaction() as cur:
            cur.execute(self._upsert_sql(), row)

    def __delitem__(self, pk: str) -> None:
        # 從 PostgreSQL 刪除
        with self.transaction() as cur:
            cur.execute(
                f'DELETE FROM "{self.table_name}" WHERE resource_id = %s', (pk,)
            )
            if cur.rowcount == 0:
                raise KeyError(pk)

    def __iter__(self) -> Generator[str]:
        # 從 PostgreSQL 查询所有 resource_id
        with self.row_cursor() as cur:
            cur.execute(f'SELECT resource_id FROM "{self.table_name}"')
            for row in cur:
                yield row["resource_id"]

    def __len__(self) -> int:
        # 從 PostgreSQL 计算总数
        with self.row_cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            return cur.fetchone()[0]

    def _build_where(self, query: ResourceMetaSearchQuery) -> tuple[str, list]:
        """Translate a search query into a SQL ``WHERE`` clause + params.

        Shared by :meth:`iter_search` and :meth:`aggregate_by` so a pushed-down
        aggregate filters exactly like a search — meta columns + ``indexed_data``
        JSONB conditions + vector conditions, built ONE way. Returns the
        ``"WHERE ..."`` string (``""`` when unfiltered) and its params;
        ordering / paging stay with the caller.
        """
        conditions = []
        params = []

        if query.is_deleted is not UNSET:
            conditions.append("is_deleted = %s")
            params.append(query.is_deleted)

        if query.created_time_start is not UNSET:
            conditions.append("created_time >= %s")
            params.append(query.created_time_start)

        if query.created_time_end is not UNSET:
            conditions.append("created_time <= %s")
            params.append(query.created_time_end)

        if query.updated_time_start is not UNSET:
            conditions.append("updated_time >= %s")
            params.append(query.updated_time_start)

        if query.updated_time_end is not UNSET:
            conditions.append("updated_time <= %s")
            params.append(query.updated_time_end)

        if query.created_bys is not UNSET:
            placeholders = ",".join(["%s"] * len(query.created_bys))
            conditions.append(f"created_by IN ({placeholders})")
            params.extend(query.created_bys)

        if query.updated_bys is not UNSET:
            placeholders = ",".join(["%s"] * len(query.updated_bys))
            conditions.append(f"updated_by IN ({placeholders})")
            params.extend(query.updated_bys)

        if query.rev_statuses is not UNSET:
            placeholders = ",".join(["%s"] * len(query.rev_statuses))
            conditions.append(f"rev_status IN ({placeholders})")
            params.extend(query.rev_statuses)

        if query.rev_created_bys is not UNSET:
            placeholders = ",".join(["%s"] * len(query.rev_created_bys))
            conditions.append(f"rev_created_by IN ({placeholders})")
            params.extend(query.rev_created_bys)

        if query.rev_updated_bys is not UNSET:
            placeholders = ",".join(["%s"] * len(query.rev_updated_bys))
            conditions.append(f"rev_updated_by IN ({placeholders})")
            params.extend(query.rev_updated_bys)

        if query.rev_created_time_start is not UNSET:
            conditions.append("rev_created_time >= %s")
            params.append(query.rev_created_time_start)
        if query.rev_created_time_end is not UNSET:
            conditions.append("rev_created_time <= %s")
            params.append(query.rev_created_time_end)
        if query.rev_updated_time_start is not UNSET:
            conditions.append("rev_updated_time >= %s")
            params.append(query.rev_updated_time_start)
        if query.rev_updated_time_end is not UNSET:
            conditions.append("rev_updated_time <= %s")
            params.append(query.rev_updated_time_end)

        # 處理 data_conditions - 在 PostgreSQL 層面過濾
        if query.data_conditions is not UNSET:
            for condition in query.data_conditions:
                json_condition, json_params = self._build_condition(condition)
                if json_condition:
                    conditions.append(json_condition)
                    params.extend(json_params)

        # 處理 conditions - 在 PostgreSQL 層面過濾
        if query.conditions is not UNSET:
            for condition in query.conditions:
                if isinstance(condition, VectorDistanceCondition):
                    sql_cond, vec_params = self._build_vector_condition(condition)
                    if sql_cond:
                        conditions.append(sql_cond)
                        params.extend(vec_params)
                    continue
                if isinstance(condition, TrigramFuzzyCondition):
                    sql_cond, fz_params = self._build_fuzzy_condition(condition)
                    if sql_cond:
                        conditions.append(sql_cond)
                        params.extend(fz_params)
                    continue
                json_condition, json_params = self._build_condition(condition)
                if json_condition:
                    conditions.append(json_condition)
                    params.extend(json_params)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, params

    def _warn_no_pg_trgm(self) -> None:
        """Warn once (per store) that a trigram query is running unaccelerated."""
        if getattr(self, "_warned_no_pg_trgm", False):
            return
        self._warned_no_pg_trgm = True
        warnings.warn(
            "pg_trgm is not installed, so .fuzzy() / .similarity() are computed in "
            "Python (correct, but a full scan — no GIN). Run "
            "`CREATE EXTENSION pg_trgm;` (or annotate a field with TrigramIndex) to "
            "accelerate them on Postgres.",
            SpecStarWarning,
            stacklevel=3,
        )

    def _trigram_fallback_search(
        self, query: ResourceMetaSearchQuery
    ) -> Generator[ResourceMeta]:
        """Evaluate a ``.fuzzy()`` / ``.similarity()`` query WITHOUT pg_trgm.

        Push every trigram-FREE top-level condition down to SQL (they are ANDed, so
        a subset is a correct pre-filter that bounds the candidate set), then apply
        the FULL predicate + sorts + pagination in Python with the same reference
        helpers the memory backend uses — so the answer is identical to native
        pg_trgm, only slower.
        """
        self._warn_no_pg_trgm()
        conds = query.conditions if query.conditions is not UNSET else []
        pushable = [c for c in conds if not _condition_has_trigram(c)]
        # Fetch candidates: trigram-free conditions only, no sorts, unbounded — the
        # trigram filter/sort/pagination all happen in Python below. With no trigram
        # left, this recursion takes the ordinary SQL path (no re-entry here).
        candidate_query = msgspec.structs.replace(
            query,
            conditions=pushable,
            sorts=UNSET,
            limit=2**63 - 1,
            offset=0,
        )
        candidates = list(self.iter_search(candidate_query))
        survivors = [m for m in candidates if is_match_query(m, query)]
        survivors.sort(key=get_sort_fn([] if query.sorts is UNSET else query.sorts))
        # offset/offset+limit — same pagination the memory backend applies; a query
        # reaching a store always carries both (QB.build() fills the defaults).
        yield from survivors[query.offset : query.offset + query.limit]

    def iter_search(self, query: ResourceMetaSearchQuery) -> Generator[ResourceMeta]:
        if not self._has_pg_trgm and _query_uses_trigram(query):
            yield from self._trigram_fallback_search(query)
            return

        where_clause, params = self._build_where(query)

        # 构建排序子句
        order_clause = ""
        order_params: list = []
        if query.sorts is not UNSET and query.sorts:
            order_parts = []
            for sort in query.sorts:
                if isinstance(sort, VectorDistanceSort):
                    sql_part, sort_params = self._build_vector_order(sort)
                    if sql_part:
                        order_parts.append(sql_part)
                        order_params.extend(sort_params)
                    continue
                if isinstance(sort, TrigramSimilaritySort):
                    sql_part, sort_params = self._build_trigram_similarity_order(sort)
                    order_parts.append(sql_part)
                    order_params.extend(sort_params)
                    continue
                if isinstance(sort, ResourceMetaSearchSort):
                    direction = (
                        "ASC"
                        if sort.direction == ResourceMetaSortDirection.ascending
                        else "DESC"
                    )
                    order_parts.append(f"{sort.key} {direction}")
                else:
                    # ResourceDataSearchSort - 處理 indexed_data 欄位排序
                    direction = (
                        "ASC"
                        if sort.direction == ResourceMetaSortDirection.ascending
                        else "DESC"
                    )
                    jsonb_extract = f"indexed_data->'{sort.field_path}'"
                    order_parts.append(f"{jsonb_extract} {direction}")
            order_clause = "ORDER BY " + ", ".join(order_parts)
        params.extend(order_params)

        sql = f'SELECT data FROM "{self.table_name}" {where_clause} {order_clause} LIMIT %s OFFSET %s'
        params.append(query.limit)
        params.append(query.offset)

        # A named cursor is what keeps an unbounded search from pulling the whole
        # table into memory, and psycopg2 drains one in batches of `itersize`. So
        # a search whose LIMIT is no larger than a single batch would be
        # materialised in one FETCH either way: the cursor buys nothing there and
        # costs DECLARE + a trailing empty FETCH + CLOSE, inside a transaction.
        #
        # The threshold is the batch size rather than a number picked to look
        # safe — above it the cursor is load-bearing. Note the DEFAULT limit is a
        # sentinel (~4.29e9), not a page size, so the default search still
        # streams.
        if query.limit is not UNSET and query.limit <= _STREAM_BATCH_ROWS:
            with self.row_cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            for row in rows:
                yield self._serializer.decode(row["data"])
            return
        with self.stream_cursor() as cur:
            cur.execute(sql, params)
            for row in cur:
                yield self._serializer.decode(row["data"])

    def count(self, query: ResourceMetaSearchQuery) -> int:
        """#414: ``COUNT(*)`` in the engine — no row data crosses the wire.

        The ``IMetaWithCount`` contract is "identical to
        ``ilen(iter_search(query))``", so the LIMIT/OFFSET window is preserved
        by counting over a sub-select. ``ORDER BY`` is deliberately dropped: the
        SIZE of a limited window does not depend on the order its rows come back
        in, so sorting for a count is pure waste (``iter_search`` pays it today).
        """
        if not self._has_pg_trgm and _query_uses_trigram(query):
            # No pushdown for the fuzzy count — honour the contract by counting the
            # Python-fallback window directly.
            return sum(1 for _ in self.iter_search(query))
        where_clause, params = self._build_where(query)
        sql = (
            f'SELECT COUNT(*) AS n FROM (SELECT 1 FROM "{self.table_name}" '
            f"{where_clause} LIMIT %s OFFSET %s) AS sub"
        )
        params.append(query.limit)
        params.append(query.offset)

        with self.row_cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["n"])

    #: Postgres pushes the #412 group-level ORDER BY / LIMIT / OFFSET (below).
    supports_group_paging = True

    def aggregate_by(
        self,
        query: ResourceMetaSearchQuery,
        by: AggKeyRef,
        aggregates: list[AggSpec],
        *,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[tuple[object, dict[str, object]]]:
        """Push a ``Count`` group-by down to PostgreSQL — ``GROUP BY`` over the
        filtered set, never materialising one row per match.

        The group key is a real column for ``source="meta"`` or
        ``indexed_data->>'field'`` (JSONB text) for ``source="data"`` (a missing
        field yields SQL ``NULL`` → ``None``, matching the Python path). The
        ``WHERE`` is built by the SAME :meth:`_build_where` as
        :meth:`iter_search`, and NO row-level ``LIMIT``/``OFFSET`` is applied —
        an aggregate spans the whole filtered set.

        When ``order_by`` is given (#412), the engine also orders and pages the
        GROUPS: NULL order-values LAST (direction-free), then the group key
        ascending (NULLs last) as the stable tiebreak, then ``LIMIT``/``OFFSET``
        over those ordered groups — byte-for-byte the ResourceManager's
        ``_order_and_page_groups`` reference (and identical to SQLite's push).
        """
        # Push-down ops: Count + numeric Sum/Min/Max (Avg is decomposed by the
        # ResourceManager into Sum+Count and never reaches a store). The RM's
        # dispatch predicate only sends eligible specs, so the assert is a
        # defensive guard, not control flow.
        assert all(a.op in ("count", "sum", "min", "max") for a in aggregates), (
            "PostgresMetaStore.aggregate_by supports count/sum/min/max"
        )
        if by.source == "meta":
            key_expr = by.name  # a real column
        else:
            key_expr = f"indexed_data->>'{by.name}'"
        where_clause, params = self._build_where(query)
        select_aggs = ", ".join(
            f"{self._agg_expr(a)} AS a{i}" for i, a in enumerate(aggregates)
        )
        sql = (
            f"SELECT {key_expr} AS k, {select_aggs} "
            f'FROM "{self.table_name}" {where_clause} GROUP BY {key_expr}'
        )
        if order_by is not None:
            # Shared NULLS-LAST + key-tiebreak ORDER BY (IMetaWithAgg) — the same
            # builder SQLite uses, fed this store's key + per-aggregate SQL, so
            # the two backends order byte-for-byte identically.
            sql += self._group_order_clause(
                order_by, key_expr, aggregates, self._agg_expr
            )
            if limit is not None or offset:
                # Postgres: ``LIMIT NULL`` = no limit (offset still applies).
                sql += " LIMIT %s OFFSET %s"
                params = [*params, limit, offset]

        # Same rule as `iter_search`, for the same reason. A GROUP BY is not
        # inherently small — grouping by a high-cardinality key returns a row per
        # group, which can be a row per resource — so what bounds it is the
        # caller's LIMIT, not the fact that it aggregates.
        def _rows(cur):
            return [
                (row[0], {a.result_name: row[i + 1] for i, a in enumerate(aggregates)})
                for row in cur
            ]

        if limit is not None and limit <= _STREAM_BATCH_ROWS:
            with self.row_cursor() as cur:
                cur.execute(sql, params)
                return _rows(cur.fetchall())
        with self.stream_cursor() as cur:
            cur.execute(sql, params)
            return _rows(cur)

    def _agg_expr(self, a: AggSpec) -> str:
        """SQL for one aggregate's value (not the GROUP BY key). A field-less
        ``count`` is ``COUNT(*)``; a ``count`` WITH a field counts that field's
        non-null values (the denominator for a decomposed ``Avg``) — the raw
        JSONB text, no numeric cast. A numeric value reducer reads a
        ``resource_meta`` column (``source="meta"``) or casts the JSONB text
        ``(indexed_data->>'f')::numeric`` (``source="data"``) so ``SUM``/``MIN``/
        ``MAX`` reduce as numbers (the ResourceManager coerces the result back to
        the field's declared ``int``/``float``)."""
        if a.op == "count" and a.field is None:
            return "COUNT(*)"
        ref = a.field
        assert ref is not None, "value reducer requires a field"
        if a.op == "count":
            base = (
                f'"{ref.name}"'
                if ref.source == "meta"
                else f"(indexed_data->>'{ref.name}')"
            )
            return f"COUNT({base})"
        if a.value_type == "datetime":
            # Reduce a TIMESTAMP meta column and return an absolute Unix epoch:
            # interpret the naive column AS UTC (writes go through a UTC session —
            # see _pg_pool) so the ResourceManager rebuilds the same tz-aware UTC
            # datetime the Python path holds.
            return (
                f"EXTRACT(EPOCH FROM {a.op.upper()}(\"{ref.name}\") AT TIME ZONE 'UTC')"
            )
        if ref.source == "meta":
            base = f'"{ref.name}"'
        else:
            base = f"(indexed_data->>'{ref.name}')::numeric"
        return f"{a.op.upper()}({base})"

    def _scalar_element_sql(
        self, operator: DataSearchOperator, value: Any
    ) -> tuple[str | None, list]:
        """Per-element scalar predicate over a ``jsonb_array_elements_text``
        alias ``val``.

        Case-sensitive (matching the pure-Python reference) and injection-free —
        ``strpos``/``starts_with``/``right`` instead of splicing user text into a
        ``LIKE`` pattern. Returns ``(None, [])`` for an operator the quantifier
        never emits.
        """
        if operator == DataSearchOperator.equals:
            return "val = %s", [value]
        if operator == DataSearchOperator.not_equals:
            return "val <> %s", [value]
        if operator == DataSearchOperator.contains:
            return "strpos(val, %s) > 0", [value]
        if operator == DataSearchOperator.starts_with:
            return "starts_with(val, %s)", [value]
        if operator == DataSearchOperator.ends_with:
            return "right(val, length(%s)) = %s", [value, value]
        if operator == DataSearchOperator.regex:
            return "val ~ %s", [value]
        return None, []

    def _build_quantified(
        self,
        field_path: str,
        operator: DataSearchOperator,
        value: Any,
        quantifier: DataSearchQuantifier,
    ) -> tuple[str, list]:
        """``any``/``all`` over a list field via ``EXISTS
        (jsonb_array_elements_text …)``.

        The ``CASE WHEN jsonb_typeof = 'array'`` around the extraction keeps a
        scalar / missing value from raising "cannot extract elements from a
        scalar" (it degrades to an empty array). ``any`` then never matches such
        a row; ``all`` additionally gates on the outer ``= 'array'`` so a
        non-array is a non-match rather than vacuously true (a present empty
        array still satisfies ``all``) — aligning every edge with the reference.
        """
        pred, params = self._scalar_element_sql(operator, value)
        if pred is None:
            return "", []
        ptr = f"indexed_data->'{field_path}'"
        arr = f"CASE WHEN jsonb_typeof({ptr}) = 'array' THEN {ptr} ELSE '[]'::jsonb END"
        elems = f"jsonb_array_elements_text({arr}) AS e(val)"
        if quantifier == DataSearchQuantifier.any:
            exists = f"EXISTS (SELECT 1 FROM {elems} WHERE {pred})"
            # When the field carries a TrigramIndex, AND in a coarse
            # ``(indexed_data->>'f') LIKE '%v%'`` over the serialised-array text.
            # That expression is what the gin_trgm_ops GIN is built on, so the
            # planner bitmap-scans candidate rows instead of unnesting every row;
            # the EXISTS then rechecks exact per-element membership. The coarse is
            # a correct SUPERSET only for the substring family and only for
            # ``any`` — an empty array yields "[]" text that the coarse rejects,
            # which is exactly what ``any`` wants (``all`` would need it kept).
            if (
                operator in _TRGM_COARSE_OPS
                and isinstance(value, str)
                and _json_text_safe(value)
                and field_path in self._trigram_indexes
            ):
                coarse = f"(indexed_data->>'{field_path}') LIKE %s"
                return f"({coarse} AND {exists})", [f"%{_like_escape(value)}%", *params]
            return exists, params
        return (
            f"(jsonb_typeof({ptr}) = 'array' AND "
            f"NOT EXISTS (SELECT 1 FROM {elems} WHERE NOT ({pred})))",
            params,
        )

    def _build_condition(self, condition: DataSearchFilter) -> tuple[str, list]:
        """構建 PostgreSQL 查詢條件 (支援 Meta 欄位與 JSONB 欄位)"""
        # Vector / fuzzy conditions carry no ``operator``/``value``, so they must be
        # dispatched to their own builders BEFORE the scalar path — not only at the
        # top level (``_build_where``) but also when reached recursively INSIDE a
        # group, i.e. AND/OR-combined with another filter.
        if isinstance(condition, VectorDistanceCondition):
            return self._build_vector_condition(condition)
        if isinstance(condition, TrigramFuzzyCondition):
            return self._build_fuzzy_condition(condition)

        if isinstance(condition, DataSearchGroup):
            sub_conditions = []
            sub_params = []
            for sub_cond in condition.conditions:
                c_str, c_params = self._build_condition(sub_cond)
                if c_str:
                    sub_conditions.append(c_str)
                    sub_params.extend(c_params)

            if not sub_conditions:
                return "", []

            if condition.operator == DataSearchLogicOperator.and_op:
                return f"({' AND '.join(sub_conditions)})", sub_params
            if condition.operator == DataSearchLogicOperator.or_op:
                return f"({' OR '.join(sub_conditions)})", sub_params
            if condition.operator == DataSearchLogicOperator.not_op:
                # NOT (AND(conditions))
                return f"NOT ({' AND '.join(sub_conditions)})", sub_params
            return "", []

        field_path = condition.field_path
        operator = condition.operator
        value = condition.value

        # Normalize Enum to value (indexed data stores Enum as value string)
        if isinstance(value, EnumType):
            value = value.value
        elif isinstance(value, (list, tuple, set)):
            # Handle Enum in lists (for in_list/not_in_list operators)
            value = [v.value if isinstance(v, EnumType) else v for v in value]

        # Element quantifier (``QB[...].any()/.all()``): apply the operator to
        # each array element and fold existentially / universally.
        if condition.quantifier is not None:
            # ``.any().eq(v)`` is exactly element membership, so it is the same
            # question as a bare list ``.contains(v)`` — route it to the top-level
            # ``indexed_data @> {"f": [v]}`` probe served by the indexed_data GIN
            # instead of a per-row EXISTS(jsonb_array_elements_text) scan. The
            # containment form matches the reference on every edge (a scalar /
            # missing / empty-array field all fail it, just like ``any`` does).
            if (
                condition.quantifier == DataSearchQuantifier.any
                and operator == DataSearchOperator.equals
                and _gin_probeable(condition, value)
            ):
                return _containment(field_path, [value])
            return self._build_quantified(
                field_path, operator, value, condition.quantifier
            )

        # 判斷是否為 Meta 欄位
        meta_fields = {
            "resource_id",
            "created_time",
            "updated_time",
            "created_by",
            "updated_by",
            "is_deleted",
            "schema_version",
            "rev_status",
            "rev_created_by",
            "rev_updated_by",
            "rev_created_time",
            "rev_updated_time",
        }

        if field_path in meta_fields:
            column_name = field_path

            # Special handling for boolean fields (is_deleted)
            is_boolean_field = field_path == "is_deleted"

            if operator == DataSearchOperator.equals:
                # Handle list/dict comparison for meta fields
                if isinstance(value, (list, dict)):
                    # Cannot compare meta fields to list/dict, skip this condition
                    return "", []
                # Handle incompatible type comparison for boolean fields
                if (
                    is_boolean_field
                    and isinstance(value, (int, float, str))
                    and value not in (True, False)
                ):
                    # PostgreSQL boolean cannot be compared with non-boolean types, skip this condition
                    return "", []
                return f"{column_name} = %s", [value]
            if operator == DataSearchOperator.not_equals:
                # Handle list/dict comparison for meta fields
                if isinstance(value, (list, dict)):
                    # Meta field is never equal to list/dict, so skip this condition (no-op)
                    # Return empty condition to be filtered out by parent AND/OR group
                    return "", []
                # Handle incompatible type comparison for boolean fields
                # PostgreSQL boolean cannot be compared with non-boolean types (int, float, str except boolean values)
                # IMPORTANT: Check bool FIRST because bool is subclass of int in Python!
                if is_boolean_field and not isinstance(value, bool):
                    if isinstance(value, (int, float, str)):
                        # Boolean is never equal to non-boolean values, skip this condition
                        return "", []
                return f"{column_name} != %s", [value]
            if operator == DataSearchOperator.greater_than:
                return f"{column_name} > %s", [value]
            if operator == DataSearchOperator.greater_than_or_equal:
                return f"{column_name} >= %s", [value]
            if operator == DataSearchOperator.less_than:
                return f"{column_name} < %s", [value]
            if operator == DataSearchOperator.less_than_or_equal:
                return f"{column_name} <= %s", [value]
            if operator == DataSearchOperator.contains:
                return f"{column_name} LIKE %s", [f"%{value}%"]
            if operator == DataSearchOperator.starts_with:
                return f"{column_name} LIKE %s", [f"{value}%"]
            if operator == DataSearchOperator.ends_with:
                return f"{column_name} LIKE %s", [f"%{value}"]
            if operator == DataSearchOperator.regex:
                return f"{column_name} ~ %s", [value]
            if operator == DataSearchOperator.in_list:
                if isinstance(value, (list, tuple, set)):
                    placeholders = ",".join(["%s"] * len(value))
                    return f"{column_name} IN ({placeholders})", list(value)
            elif operator == DataSearchOperator.not_in_list:
                if isinstance(value, (list, tuple, set)):
                    placeholders = ",".join(["%s"] * len(value))
                    return f"{column_name} NOT IN ({placeholders})", list(value)
            if operator == DataSearchOperator.is_null:
                if value:
                    return f"{column_name} IS NULL", []
                else:
                    return f"{column_name} IS NOT NULL", []
            if operator == DataSearchOperator.exists:
                if value:
                    return "TRUE", []
                else:
                    return "FALSE", []
            if operator == DataSearchOperator.isna:
                if value:
                    return f"{column_name} IS NULL", []
                else:
                    return f"{column_name} IS NOT NULL", []

            return "", []

        # A bare scalar string op on a registered list field would run against
        # the serialised array — reject it, directing to .any()/.all(). (The
        # quantified form already returned above; ``contains`` stays @> membership.)
        if field_path in self._list_fields and operator in _LIST_SCALAR_ONLY_OPS:
            raise bad_list_string_op(field_path, operator)

        # PostgreSQL JSONB 提取語法: indexed_data->>'field_path'
        # 對於數字比較，使用 (indexed_data->>'field_path')::numeric
        jsonb_text_extract = f"indexed_data->>'{field_path}'"
        jsonb_numeric_extract = f"(indexed_data->>'{field_path}')::numeric"

        # Apply field transformation if specified
        if condition.transform is not None:
            if condition.transform == FieldTransform.length:
                # Get length of JSONB value
                # For arrays: jsonb_array_length()
                # For strings: length(text_value)
                # Use jsonb_typeof to determine type
                jsonb_type = f"jsonb_typeof(indexed_data->'{field_path}')"
                jsonb_text_extract = f"""CASE 
                    WHEN {jsonb_type} = 'array' THEN jsonb_array_length(indexed_data->'{field_path}')
                    WHEN {jsonb_type} = 'string' THEN length(indexed_data->>'{field_path}')
                    ELSE NULL
                END"""
                jsonb_numeric_extract = jsonb_text_extract  # Use same for both

        if operator == DataSearchOperator.equals:
            if isinstance(value, (list, dict)):
                # Equality on a composite value must stay EXACT: containment
                # would make ``{"f": [1, 2, 3]}`` match a query for ``[1]``.
                import json

                return f"indexed_data->'{field_path}' = %s::jsonb", [json.dumps(value)]
            if _gin_probeable(condition, value):
                return _containment(field_path, value)
            if isinstance(value, bool):
                return f"{jsonb_text_extract} = %s", ["true" if value else "false"]
            return f"{jsonb_text_extract} = %s", [str(value)]
        if operator == DataSearchOperator.not_equals:
            if isinstance(value, (list, dict)):
                # For list/dict, use JSONB comparison with NULL safe
                import json

                return (
                    f"({jsonb_text_extract} IS NULL OR indexed_data->'{field_path}' != %s::jsonb)",
                    [json.dumps(value)],
                )
            if isinstance(value, bool):
                return f"{jsonb_text_extract} != %s", ["true" if value else "false"]
            return f"{jsonb_text_extract} != %s", [str(value)]
        if operator in _RANGE_OPS:
            sql_op = _RANGE_OPS[operator]
            if field_path in self._sort_indexes and _gin_probeable(condition, value):
                # Matches the indexed expression exactly, so the btree from
                # ensure_sort_index serves it (#418). Only for declared fields:
                # WITHOUT an index the jsonb form is SLOWER than the cast, which
                # Postgres parallelises, so rewriting globally would be a pessimism.
                import json

                return f"indexed_data->'{field_path}' {sql_op} %s::jsonb", [
                    json.dumps(value, ensure_ascii=False)
                ]
            return f"{jsonb_numeric_extract} {sql_op} %s", [value]
        if operator == DataSearchOperator.contains:
            # List-typed fields use JSONB ``@>`` so ``contains`` is true
            # element containment, not a substring on the serialised JSON
            # (which produced false-positives like ``"c1"`` matching
            # ``["c10"]``). The default keeps ``LIKE`` for string fields.
            # See #362.
            if field_path in self._list_fields:
                import json

                if _gin_probeable(condition, value):
                    # Probe at the TOP level: ``indexed_data->'f' @> …`` applies
                    # containment to the extract, which the GIN on
                    # ``indexed_data`` cannot serve any more than ``->>`` can.
                    # Element membership stays exact either way (#362/#378).
                    return _containment(field_path, [value])
                return (
                    f"indexed_data->'{field_path}' @> %s::jsonb",
                    [json.dumps(value)],
                )
            return f"{jsonb_text_extract} LIKE %s", [f"%{value}%"]
        if operator == DataSearchOperator.starts_with:
            return f"{jsonb_text_extract} LIKE %s", [f"{value}%"]
        if operator == DataSearchOperator.ends_with:
            return f"{jsonb_text_extract} LIKE %s", [f"%{value}"]
        if operator == DataSearchOperator.regex:
            return f"{jsonb_text_extract} ~ %s", [value]
        if operator == DataSearchOperator.in_list:
            if isinstance(value, (list, tuple, set)):
                vals = list(value)
                if not vals:
                    return "FALSE", []  # ``IN ()`` is not valid SQL
                if all(_gin_probeable(condition, v) for v in vals):
                    import json

                    if len(vals) <= IN_LIST_PROBE_LIMIT:
                        # Postgres BitmapOr's the probes across the one GIN. Only
                        # worth it while the list is short — see the constant.
                        ors = " OR ".join(["indexed_data @> %s::jsonb"] * len(vals))
                        return f"({ors})", [
                            json.dumps({field_path: v}, ensure_ascii=False)
                            for v in vals
                        ]
                    # Long list: ONE flat predicate whose cost does not grow with
                    # the list. Still jsonb equality, so it agrees value-for-value
                    # with the probes above — the length picks the plan, never the
                    # answer.
                    return f"indexed_data->'{field_path}' = ANY(%s::jsonb[])", [
                        [json.dumps(v, ensure_ascii=False) for v in vals]
                    ]
                placeholders = ",".join(["%s"] * len(vals))
                return f"{jsonb_text_extract} IN ({placeholders})", [
                    str(v) for v in vals
                ]
        elif operator == DataSearchOperator.not_in_list:
            if isinstance(value, (list, tuple, set)):
                placeholders = ",".join(["%s"] * len(value))
                return f"{jsonb_text_extract} NOT IN ({placeholders})", [
                    str(v) for v in value
                ]
        if operator == DataSearchOperator.contains_any:
            vals = list(value) if isinstance(value, (list, tuple, set)) else [value]
            if not vals:
                return "FALSE", []  # empty candidate set matches nothing
            if field_path in self._set_columns:
                # Fast path: ONE indexed array-overlap on the dedicated SetIndex
                # column (its own GIN), instead of the per-candidate @> fan-out.
                # psycopg2 adapts the Python list to an array literal.
                col = self._set_col_name(field_path)
                pg_elem = self._set_columns[field_path]
                return f'"{col}" && %s::{pg_elem}[]', [list(vals)]
            # Fallback (field not SetIndex-declared): OR one ``@>`` containment
            # probe per candidate, served by the SINGLE GIN on the whole
            # ``indexed_data`` (jsonb_ops supports ``@>``). We deliberately do NOT
            # use ``indexed_data->'field' ?| ...`` — that extraction can't use the
            # index — nor a per-field expression index here, which would force
            # CREATE/DROP DDL when indexed fields change and break specstar's "the
            # user never manages the DB / schema can vary freely" guarantee.
            # ``@>`` also scopes to THIS field (a candidate living in another
            # field won't false-match), and the planner BitmapOr's the probes.
            import json

            terms = " OR ".join(["indexed_data @> %s::jsonb"] * len(vals))
            params = [json.dumps({field_path: [v]}, ensure_ascii=False) for v in vals]
            return f"({terms})", params
        if operator == DataSearchOperator.is_null:
            if value:
                # Strict is_null: Must exist AND be null
                return f"(indexed_data ? %s) AND ({jsonb_text_extract} IS NULL)", [
                    field_path
                ]
            else:
                # Strict is_null=False: Must exist AND be NOT null
                # (If it doesn't exist, it's False. If it exists and is null, it's False.)
                # Wait, if is_null=False, we want "Not (Exists and Null)".
                # But user said "if field_path does not exist... return false".
                # So if missing, is_null=False should return False?
                # "is_null(False)" means "is NOT null".
                # If missing, is it "not null"? Yes.
                # But user said "all value related comparisons should return false".
                # If is_null is a value comparison, then is_null(False) on missing should be False.
                # This means "It must exist AND NOT be null".
                return f"(indexed_data ? %s) AND ({jsonb_text_extract} IS NOT NULL)", [
                    field_path
                ]
        if operator == DataSearchOperator.exists:
            if value:
                return "indexed_data ? %s", [field_path]
            else:
                return "NOT (indexed_data ? %s)", [field_path]
        if operator == DataSearchOperator.isna:
            if value:
                return f"{jsonb_text_extract} IS NULL", []
            else:
                return f"{jsonb_text_extract} IS NOT NULL", []

        # 如果不支持的操作，返回空條件
        return "", []
