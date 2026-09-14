"""``SpecStar.load()`` streams an archive in bounded batches (#448 follow-up).

A model section is no longer buffered whole before it is written: records are
flushed every ``batch_size`` records / ``batch_bytes`` bytes, so peak memory
is a function of the batch, not of the largest model in the archive — and the
``on_duplicate=skip`` contract (a skipped resource's revisions are skipped
too) holds across those flush boundaries.
"""

from __future__ import annotations

import datetime as dt
import io
import tracemalloc

from msgspec import Struct

from specstar.crud.core import SpecStar
from specstar.resource_manager.storage_factory import DiskStorageFactory
from specstar.types import OnDuplicate


class Item(Struct):
    name: str
    blob: str = ""


def _spec(root) -> SpecStar:
    s = SpecStar(storage_factory=DiskStorageFactory(root))
    s.add_model(Item)
    return s


def _dump(spec: SpecStar) -> bytes:
    bio = io.BytesIO()
    spec.dump(bio)
    return bio.getvalue()


def test_skip_holds_across_flush_boundaries(tmp_path):
    src = _spec(tmp_path / "src")
    items = src.get_resource_manager(Item)
    with items.meta_provide("u", dt.datetime(2025, 1, 1)):
        ids = [items.create(Item(name=f"i{n}")).resource_id for n in range(3)]

    dst = _spec(tmp_path / "dst")
    dst.load(io.BytesIO(_dump(src)))  # target now holds all three, 1 revision each

    with items.meta_provide("u", dt.datetime(2025, 1, 2)):
        for rid in ids:
            items.update(rid, Item(name="changed"))  # source: 2 revisions each

    # batch_size=1 puts every meta and every revision in its own flush, so the
    # "this resource was skipped" decision must survive between flushes.
    stats = dst.load(
        io.BytesIO(_dump(src)), on_duplicate=OnDuplicate.skip, batch_size=1
    )
    assert (stats["item"].skipped, stats["item"].loaded) == (3, 0)
    got = dst.get_resource_manager(Item)
    for rid in ids:
        assert len(got.list_revisions(rid)) == 1  # the new revisions were not written
        assert got.get(rid).data.name != "changed"


def test_peak_memory_is_bounded_by_the_batch_not_the_archive(tmp_path):
    src = _spec(tmp_path / "src")
    items = src.get_resource_manager(Item)
    payload = "x" * 4096
    with items.meta_provide("u", dt.datetime(2025, 1, 1)):
        for n in range(2000):
            items.create(Item(name=f"i{n}", blob=payload))
    archive = _dump(src)
    assert len(archive) > 8 * 1024 * 1024  # a model section that is not small

    dst = _spec(tmp_path / "dst")
    bio = io.BytesIO(archive)
    tracemalloc.start()
    try:
        stats = dst.load(bio, batch_size=100)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert stats["item"].loaded == 2000
    # Buffering the whole section peaks at ~2.3x the archive (measured); a
    # 100-record batch of 4 KiB payloads peaks at ~0.25x. Assert the midpoint.
    assert peak < len(archive) / 2, (
        f"peak {peak} bytes for a {len(archive)}-byte archive"
    )
