"""``SpecStar.load()`` imports an archive written by ``autocrud==0.4.6`` (#448).

The fixture in ``tests/fixtures/legacy/0_4_x/`` was produced by the real
0.4.6 package against a 0.4.x ``DiskStorageFactory`` deployment (see
``gen_0_4_x.py`` there); ``manifest.json`` is what that deployment held,
read back through the 0.4.x API. This test asserts the import against the
manifest, not against the archive.
"""

from __future__ import annotations

import datetime as dt
import enum
import json
from pathlib import Path
from uuid import UUID

import msgspec
import pytest
from msgspec import Struct

from specstar.crud.core import SpecStar
from specstar.query_types import ResourceMetaSearchQuery
from specstar.resource_manager.core import SimpleStorage
from specstar.resource_manager.meta_store.sqlite3 import FileSqliteMetaStore
from specstar.resource_manager.resource_store.simple import DiskResourceStore
from specstar.resource_manager.storage_factory import (
    DiskStorageFactory,
    IStorageFactory,
    MemoryStorageFactory,
)
from specstar.types import DuplicateResourceError, OnDuplicate, ResourceIsDeletedError

FIXTURE = Path(__file__).parent / "fixtures" / "legacy" / "0_4_x"


# The same models the 0.4.x deployment was built with (gen_0_4_x.py).
class Priority(enum.StrEnum):
    low = "low"
    high = "high"


class Address(Struct):
    city: str
    zip: str | None = None


class Ticket(Struct):
    title: str
    priority: Priority = Priority.low
    tags: list[str] = []
    due: dt.datetime | None = None
    address: Address | None = None
    score: float = 0.0
    ref: UUID | None = None
    payload: bytes = b""


class Note(Struct):
    body: str
    count: int = 0


@pytest.fixture
def manifest() -> dict:
    return json.loads((FIXTURE / "manifest.json").read_text())


class _SqliteStorageFactory(IStorageFactory):
    """SQLite meta rows (promoted columns + JSON blob) with disk revisions."""

    def __init__(self, rootdir: Path):
        self.rootdir = rootdir

    def build(self, model_name: str):
        return SimpleStorage(
            FileSqliteMetaStore(db_filepath=self.rootdir / f"{model_name}.sqlite3"),
            DiskResourceStore(rootdir=self.rootdir / model_name),
        )


@pytest.fixture(params=["memory", "disk", "sqlite"])
def spec(request, tmp_path) -> SpecStar:
    tmp_path.mkdir(exist_ok=True)
    factory = {
        "memory": lambda: MemoryStorageFactory(),
        "disk": lambda: DiskStorageFactory(tmp_path / "data"),
        "sqlite": lambda: _SqliteStorageFactory(tmp_path),
    }[request.param]()
    s = SpecStar(storage_factory=factory)
    s.add_model(Ticket)
    s.add_model(Note)
    return s


def _load(spec: SpecStar, **kw):
    with (FIXTURE / "backup.acbak").open("rb") as f:
        return spec.load(f, **kw)


def test_every_resource_and_revision_from_0_4_x_is_readable(spec, manifest):
    stats = _load(spec)
    assert {m: (s.loaded, s.total) for m, s in stats.items()} == {
        "ticket": (3, 3),
        "note": (2, 2),
    }

    for model_name, resources in manifest.items():
        mgr = spec.get_resource_manager(model_name)
        for rid, expected in resources.items():
            meta = mgr.get_meta(rid, include_deleted=True)
            assert meta.current_revision_id == expected["current_revision_id"]
            assert meta.total_revision_count == expected["total_revision_count"]
            assert meta.is_deleted is expected["is_deleted"]
            assert meta.created_by == expected["created_by"]
            assert meta.updated_by == expected["updated_by"]
            assert meta.created_time.isoformat() == expected["created_time"]
            assert meta.updated_time.isoformat() == expected["updated_time"]
            assert meta.schema_version is None
            if expected["indexed_data"]:
                assert meta.indexed_data == expected["indexed_data"]

            assert set(mgr.list_revisions(rid)) == set(expected["revisions"])
            for rev_id, rev in expected["revisions"].items():
                res = mgr.get_resource_revision(rid, rev_id)
                assert msgspec.to_builtins(res.data) == rev["data"]
                assert str(res.info.uid) == rev["uid"]
                assert res.info.parent_revision_id == rev["parent_revision_id"]
                assert res.info.status == rev["status"]
                assert res.info.data_hash == rev["data_hash"]
                assert res.info.created_by == rev["created_by"]
                assert res.info.created_time.isoformat() == rev["created_time"]
                assert res.info.schema_version is None


def test_current_revision_mirror_fields_arrive_filled(spec, manifest):
    """0.4.x never had ``rev_*``; the 0.4.6 exporter fills them so no
    ``backfill_revision_meta()`` pass is needed after import."""
    _load(spec)
    for model_name, resources in manifest.items():
        mgr = spec.get_resource_manager(model_name)
        for rid, expected in resources.items():
            meta = mgr.get_meta(rid, include_deleted=True)
            current = expected["revisions"][expected["current_revision_id"]]
            assert meta.rev_status == current["status"]
            assert meta.rev_created_by == current["created_by"]
            assert meta.rev_created_time.isoformat() == current["created_time"]
    assert mgr.backfill_revision_meta() == 0


def test_soft_deleted_and_switched_resources_keep_their_state(spec):
    _load(spec)
    tickets = spec.get_resource_manager("ticket")

    # ticket:2 was soft-deleted on 0.4.x
    assert tickets.get_meta("ticket:2", include_deleted=True).is_deleted is True
    live = tickets.search_resources(ResourceMetaSearchQuery(is_deleted=False))
    assert {m.resource_id for m in live} == {"ticket:1", "ticket:3"}
    with pytest.raises(ResourceIsDeletedError):
        tickets.get("ticket:2")

    # ticket:3 was switched back to its first revision; the second still exists
    meta = tickets.get_meta("ticket:3")
    assert meta.current_revision_id == "ticket:3:1"
    assert tickets.get("ticket:3").data.title == "C v1"
    assert tickets.get_resource_revision("ticket:3", "ticket:3:2").data.title == "C v2"


def test_rich_payload_types_round_trip_exactly(spec):
    _load(spec)
    first = spec.get_resource_manager("ticket").get_resource_revision(
        "ticket:1", "ticket:1:1"
    )
    assert first.data == Ticket(
        title="A first",
        tags=["x"],
        due=dt.datetime(2025, 10, 1, 8, 30),
        address=Address(city="Taipei", zip="100"),
        score=1.5,
        ref=UUID("12345678-1234-5678-1234-567812345678"),
        payload=b"\x00\x01\x02binary",
    )


def test_writes_continue_the_imported_history(spec):
    _load(spec)
    tickets = spec.get_resource_manager("ticket")
    with tickets.meta_provide("erin", dt.datetime(2026, 1, 1)):
        info = tickets.update("ticket:1", Ticket(title="A fourth"))
    assert info.parent_revision_id == "ticket:1:3"
    meta = tickets.get_meta("ticket:1")
    assert meta.total_revision_count == 4
    assert meta.current_revision_id == info.revision_id
    assert tickets.get("ticket:1").data.title == "A fourth"


def test_on_duplicate_governs_a_second_import(spec):
    _load(spec)
    with pytest.raises(DuplicateResourceError):
        _load(spec, on_duplicate=OnDuplicate.raise_error)
    stats = _load(spec, on_duplicate=OnDuplicate.skip)
    assert (stats["ticket"].skipped, stats["note"].skipped) == (3, 2)
    stats = _load(spec, on_duplicate=OnDuplicate.overwrite)
    assert (stats["ticket"].loaded, stats["note"].loaded) == (3, 2)


def test_archive_names_a_model_the_target_does_not_register(tmp_path):
    only_notes = SpecStar(storage_factory=MemoryStorageFactory())
    only_notes.add_model(Note)
    with pytest.raises(ValueError, match="'ticket'"):
        _load(only_notes)
