"""Regenerate the ``autocrud==0.4.6`` fixture used by ``tests/test_legacy_load.py``.

Run this with the *old* package installed, never with ``specstar``::

    uv venv /tmp/ac046 && uv pip install --python /tmp/ac046/bin/python autocrud==0.4.6
    /tmp/ac046/bin/python tests/fixtures/legacy/gen_0_4_x.py

It builds a 0.4.x ``DiskStorageFactory`` deployment in a temp dir (the
layout 0.4.0–0.4.5 wrote; 0.4.6 changes nothing on disk) and writes, next
to itself:

* ``0_4_x/backup.acbak``  – ``AutoCRUD.dump()`` output, i.e. what a 0.4.x
  user hands to ``SpecStar.load()``
* ``0_4_x/manifest.json`` – what the data *is*, read back through the 0.4.x
  API, so the test asserts against the source of truth rather than the
  archive
"""

from __future__ import annotations

import datetime as dt
import enum
import itertools
import json
import shutil
import tempfile
from pathlib import Path
from uuid import UUID

import msgspec
from autocrud.crud.core import AutoCRUD
from autocrud.resource_manager.storage_factory import DiskStorageFactory
from msgspec import Struct

OUT = Path(__file__).parent / "0_4_x"


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


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    disk = Path(tempfile.mkdtemp(prefix="autocrud-0.4.x-"))
    crud = AutoCRUD(storage_factory=DiskStorageFactory(disk))
    tick = itertools.count(1)
    note = itertools.count(1)
    crud.add_model(
        Ticket,
        id_generator=lambda: f"ticket:{next(tick)}",
        indexed_fields=[("title", str), ("priority", str)],
    )
    crud.add_model(Note, id_generator=lambda: f"note:{next(note)}")

    tickets = crud.resource_managers["ticket"]
    notes = crud.resource_managers["note"]
    t0 = dt.datetime(2025, 9, 1, 12, 0, 0)

    # A: three revisions, current = latest
    with tickets.meta_provide("alice", t0):
        a = tickets.create(
            Ticket(
                title="A first",
                tags=["x"],
                due=dt.datetime(2025, 10, 1, 8, 30),
                address=Address(city="Taipei", zip="100"),
                score=1.5,
                ref=UUID("12345678-1234-5678-1234-567812345678"),
                payload=b"\x00\x01\x02binary",
            )
        )
    with tickets.meta_provide("bob", t0 + dt.timedelta(hours=1)):
        tickets.update(
            a.resource_id,
            Ticket(title="A second", priority=Priority.high, tags=["x", "y"]),
        )
    with tickets.meta_provide("bob", t0 + dt.timedelta(hours=2)):
        tickets.update(a.resource_id, Ticket(title="A third", score=-2.25))

    # B: created then soft-deleted
    with tickets.meta_provide("alice", t0):
        b = tickets.create(Ticket(title="B gone"))
    with tickets.meta_provide("carol", t0 + dt.timedelta(days=1)):
        tickets.delete(b.resource_id)

    # C: two revisions, switched back to the first
    with tickets.meta_provide("alice", t0):
        c = tickets.create(Ticket(title="C v1"))
    with tickets.meta_provide("alice", t0 + dt.timedelta(minutes=5)):
        tickets.update(c.resource_id, Ticket(title="C v2"))
    with tickets.meta_provide("alice", t0 + dt.timedelta(minutes=10)):
        tickets.switch(c.resource_id, c.revision_id)

    with notes.meta_provide("dave", t0):
        notes.create(Note(body="hello", count=1))
        notes.create(Note(body="世界", count=2))

    with (OUT / "backup.acbak").open("wb") as f:
        crud.dump(f)

    manifest: dict = {}
    for name, mgr in crud.resource_managers.items():
        model = {}
        for meta in mgr.storage.dump_meta():
            revs = {}
            for rev_id in mgr.storage.list_revisions(meta.resource_id):
                res = mgr.storage.get_resource_revision(meta.resource_id, rev_id)
                revs[rev_id] = {
                    "uid": str(res.info.uid),
                    "parent_revision_id": res.info.parent_revision_id
                    if res.info.parent_revision_id is not msgspec.UNSET
                    else None,
                    "status": res.info.status.value,
                    "data_hash": res.info.data_hash,
                    "created_by": res.info.created_by,
                    "created_time": res.info.created_time.isoformat(),
                    "data": msgspec.to_builtins(res.data),
                }
            model[meta.resource_id] = {
                "current_revision_id": meta.current_revision_id,
                "total_revision_count": meta.total_revision_count,
                "is_deleted": meta.is_deleted,
                "created_by": meta.created_by,
                "updated_by": meta.updated_by,
                "created_time": meta.created_time.isoformat(),
                "updated_time": meta.updated_time.isoformat(),
                "indexed_data": None
                if meta.indexed_data is msgspec.UNSET
                else meta.indexed_data,
                "revisions": revs,
            }
        manifest[name] = model
    (OUT / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    main()
