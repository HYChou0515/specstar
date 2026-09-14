"""The 0.4.6 archive imports into a Postgres-only deployment (#448).

Same fixture and assertions as ``test_legacy_load.py``, run against
``PostgresStorageFactory`` (Postgres meta store + Postgres resource store —
the target setup behind #448) on a live database. Integration-marked via
the ``test_postgres_*`` glob in ``tests/conftest.py``.
"""

from __future__ import annotations

import json

import psycopg2
import pytest

from specstar.crud.core import SpecStar
from specstar.resource_manager.storage_factory import PostgresStorageFactory
from tests.test_legacy_load import (
    FIXTURE,
    Note,
    Ticket,
    _load,
    _manifest_state,
    _spec_state,
)

PG_DSN = "postgresql://admin:password@localhost:5432/your_database"
PREFIX = "legacy448_"


@pytest.fixture
def spec() -> SpecStar:
    try:
        conn = psycopg2.connect(PG_DSN)
    except Exception as e:  # pragma: no cover - environment
        pytest.skip(f"PostgreSQL not available: {e}")
    with conn, conn.cursor() as cur:
        for model in ("ticket", "note"):
            for suffix in ("meta", "resource_index", "resource_data"):
                cur.execute(f'DROP TABLE IF EXISTS "{PREFIX}{model}_{suffix}" CASCADE')
    conn.close()

    s = SpecStar(storage_factory=PostgresStorageFactory(PG_DSN, table_prefix=PREFIX))
    s.add_model(Ticket)
    s.add_model(Note)
    return s


def test_full_import_into_postgres_matches_the_0_4_x_manifest(spec):
    manifest = json.loads((FIXTURE / "manifest.json").read_text())
    stats = _load(spec)
    assert {m: (s.loaded, s.total) for m, s in stats.items()} == {
        "ticket": (3, 3),
        "note": (2, 2),
    }
    assert _spec_state(spec, manifest) == _manifest_state(manifest)

    tickets = spec.get_resource_manager("ticket")
    first = tickets.get_resource_revision("ticket:1", "ticket:1:1")
    assert first.data.title == "A first" and first.data.payload == b"\x00\x01\x02binary"
    assert (
        first.info.data_hash
        == manifest["ticket"]["ticket:1"]["revisions"]["ticket:1:1"]["data_hash"]
    )
    meta = tickets.get_meta("ticket:1")
    assert meta.rev_created_by == "bob"  # rev_* landed in the promoted columns too
    assert meta.indexed_data == manifest["ticket"]["ticket:1"]["indexed_data"]


def test_full_then_delta_into_postgres_equals_the_source_after_cutover(spec):
    after = json.loads((FIXTURE / "manifest_after_delta.json").read_text())
    _load(spec)
    with (FIXTURE / "delta.acbak").open("rb") as f:
        stats = spec.load(f)
    assert stats["ticket"].loaded == 4 and stats["note"].loaded == 1
    assert _spec_state(spec, after) == _manifest_state(after)
    tickets = spec.get_resource_manager("ticket")
    assert tickets.get("ticket:1").data.title == "A fourth"
    assert tickets.get("ticket:2").data.title == "B gone"  # restored after T
    assert tickets.get_meta("ticket:3", include_deleted=True).is_deleted is True
