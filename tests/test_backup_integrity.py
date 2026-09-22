"""Integrity of the ``.acbak`` backup path (issue #450).

A backup is only worth having if the failures on the way in and out are
loud.  These tests pin the failure modes that used to be silent:

* an archive whose stream ends before its ``EofRecord`` is **truncated**,
  and loading it used to report ``loaded=0`` without raising;
* a blob the dump could not read used to be dropped from the archive;
* a revision whose payload would not decode used to contribute no blob
  ids, so its attachments went missing too.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import warnings

import msgspec
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from msgspec import Struct

from specstar.crud.core import SpecStar
from specstar.errors import ArchiveTruncatedError, DumpIncompleteError
from specstar.resource_manager.dump_format import (
    DumpStreamReader,
    DumpStreamWriter,
    MetaRecord,
)
from specstar.types import Binary


class Item(Struct):
    payload: str


class Doc(msgspec.Struct):
    title: str
    file: Binary | None = None


def _doc_spec() -> SpecStar:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        spec = SpecStar(default_user="tester", default_now=dt.datetime.now)
        spec.add_model(Doc, name="doc")
    return spec


def _spec_with_orphaned_blob() -> tuple[SpecStar, str]:
    """A resource whose attachment is no longer in the blob store.

    This is what a backup meets in the wild: the row still references the
    file, the bytes are gone. Returns the spec and the missing ``file_id``.
    """
    from xxhash import xxh3_128_hexdigest

    spec = _doc_spec()
    mgr = spec.resource_managers["doc"]
    with mgr.using(user="tester", now=dt.datetime.now()) as ops:
        ops.create(Doc(title="t", file=Binary(data=b"payload")))
    file_id = xxh3_128_hexdigest(b"payload")
    spec.blob_store.delete(file_id)
    return spec, file_id


def _spec() -> SpecStar:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        spec = SpecStar(default_user="tester", default_now=dt.datetime.now)
        spec.add_model(Item, name="item")
    return spec


def _seeded(n: int = 5) -> SpecStar:
    spec = _spec()
    mgr = spec.get_resource_manager(Item)
    with mgr.using(user="tester", now=dt.datetime.now()):
        for i in range(n):
            mgr.create(Item(payload=f"row-{i}"))
    return spec


def _client(spec: SpecStar) -> TestClient:
    app = FastAPI()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        spec.apply(app)
    return TestClient(app, raise_server_exceptions=False)


def _archive(spec: SpecStar) -> bytes:
    bio = io.BytesIO()
    spec.dump(bio)
    return bio.getvalue()


def _truncate_after_last_revision(archive: bytes) -> bytes:
    """Re-frame *archive* without its trailing ``ModelEnd`` / ``Eof``.

    Cutting at a frame boundary is the realistic shape: a dump killed
    part-way through, or a transfer that stopped, leaves whole records
    followed by nothing.  A mid-frame cut is already detected by the
    reader; this one was not.
    """
    records = list(DumpStreamReader(io.BytesIO(archive)))
    assert type(records[-1]).__name__ == "EofRecord"
    assert type(records[-2]).__name__ == "ModelEndRecord"
    out = io.BytesIO()
    writer = DumpStreamWriter(out)
    for record in records[:-2]:
        writer.write(record)
    return out.getvalue()


class TestTruncatedArchive:
    def test_load_raises_when_the_stream_ends_without_eof(self):
        truncated = _truncate_after_last_revision(_archive(_seeded()))

        with pytest.raises(ArchiveTruncatedError):
            _spec().load(io.BytesIO(truncated))

    def test_load_raises_with_stats_when_the_cut_is_mid_record(self):
        """The realistic cut, and the one the guard first missed.

        A killed process or a dropped transfer lands on a frame boundary
        only by luck. Cut inside a frame, the frame reader raised a plain
        ``ValueError`` that escaped the loop, so the final flush never ran
        and the error carried no stats — the buffered batch was dropped
        exactly as before, which is what this guard exists to stop.
        """
        archive = _archive(_seeded(20))
        cut = archive[: len(archive) // 2]
        dst = _spec()

        with pytest.raises(ArchiveTruncatedError) as exc_info:
            dst.load(io.BytesIO(cut), batch_size=1000)

        assert exc_info.value.stats["item"].loaded > 0
        # The batch really was written, not just counted. A cut mid-record
        # can leave a meta whose revision data never arrived, so re-dump
        # non-strict and let it report that rather than refuse.
        bio = io.BytesIO()
        dst.dump(bio, strict=False)
        restored = list(DumpStreamReader(io.BytesIO(bio.getvalue())))
        assert sum(isinstance(r, MetaRecord) for r in restored) > 0

    def test_truncated_load_keeps_the_records_it_did_read(self):
        """The whole records before the cut are applied, and the error says so.

        The buffered batch used to be discarded when the loop ran out of
        records, so a truncated archive both failed silently and loaded
        nothing.  Flushing first makes the report true.
        """
        truncated = _truncate_after_last_revision(_archive(_seeded(5)))
        dst = _spec()

        with pytest.raises(ArchiveTruncatedError) as exc_info:
            dst.load(io.BytesIO(truncated))

        assert exc_info.value.stats["item"].loaded == 5
        restored = list(DumpStreamReader(io.BytesIO(_archive(dst))))
        assert sum(isinstance(r, MetaRecord) for r in restored) == 5


class TestTruncatedArchiveOverHttp:
    """A truncated upload is a bad request, not a quiet success."""

    def test_per_model_import_rejects_a_truncated_archive(self):
        truncated = _truncate_after_last_revision(_archive(_seeded(5)))
        client = _client(_spec())

        response = client.post("/item/import", files={"file": ("x.acbak", truncated)})

        assert response.status_code == 400
        assert "truncated" in response.json()["detail"].lower()

    def test_per_model_import_rejects_an_archive_cut_mid_record(self):
        """The ordinary "upload stopped early" shape.

        A cut at a frame boundary is caught by the EofRecord check; a cut
        inside a frame is caught by the reader, which raises a plain
        ``ValueError``. The per-model route wrapped only ``StopIteration``
        around its first read, so that one escaped as a 500 while the
        global route already answered 400 for the same bytes.
        """
        archive = _archive(_seeded(5))
        cut = archive[: len(archive) - 7]

        for path in ("/item/import", "/_backup/import"):
            response = _client(_spec()).post(path, files={"file": ("x.acbak", cut)})

            assert response.status_code == 400, path
            # Both cut shapes, on both routes, report the same way: as a
            # truncation carrying the counts already applied. The per-model
            # route used to hand back the frame reader's bare message.
            detail = response.json()["detail"]
            assert "truncated" in detail.lower(), (path, detail)
            assert "loaded=" in detail, (path, detail)

    def test_global_import_rejects_a_truncated_archive(self):
        truncated = _truncate_after_last_revision(_archive(_seeded(5)))
        client = _client(_spec())

        response = client.post(
            "/_backup/import", files={"file": ("x.acbak", truncated)}
        )

        assert response.status_code == 400
        assert "truncated" in response.json()["detail"].lower()


class TestUnreadableBlob:
    """A blob the dump cannot read must not vanish from the archive.

    `resource_manager/core.py` wrapped the blob fetch in a bare
    ``except Exception: pass``, so an unreadable attachment was dropped
    and the dump still finished normally. For a backup that is the one
    failure that must never be quiet — it is discovered on restore day.
    """

    def test_dump_raises_when_a_referenced_blob_cannot_be_read(self):
        spec, file_id = _spec_with_orphaned_blob()

        with pytest.raises(DumpIncompleteError) as exc_info:
            spec.dump(io.BytesIO())

        assert file_id in str(exc_info.value)

    def test_non_strict_dump_reports_the_blob_it_skipped(self):
        """Opting out of strict must still hand back the evidence.

        "Dump what you can" is a legitimate choice; "dump what you can and
        say nothing" is not. The caller gets per-model stats and can ask
        ``complete``.
        """
        spec, file_id = _spec_with_orphaned_blob()
        bio = io.BytesIO()

        stats = spec.dump(bio, strict=False)

        assert stats["doc"].skipped_blobs == [file_id]
        assert stats["doc"].complete is False
        assert stats["doc"].metas == 1
        assert stats["doc"].revisions == 1
        assert stats["doc"].blobs == 0

    def test_a_healthy_dump_reports_complete(self):
        spec = _seeded(3)
        bio = io.BytesIO()

        stats = spec.dump(bio)

        assert stats["item"].complete is True
        assert stats["item"].metas == 3
        assert stats["item"].revisions == 3

    def test_a_strict_failure_leaves_an_archive_load_refuses(self):
        """The two guards meet: a dump that died writes a file load rejects.

        Strict mode raises part-way through, so the bytes on disk have no
        ``EofRecord``. Someone who keeps that file and tries to restore it
        later gets ``ArchiveTruncatedError`` rather than a quiet partial
        restore — neither guard has to know about the other.
        """
        spec, _ = _spec_with_orphaned_blob()
        bio = io.BytesIO()

        with pytest.raises(DumpIncompleteError):
            spec.dump(bio)

        with pytest.raises(ArchiveTruncatedError):
            _doc_spec().load(io.BytesIO(bio.getvalue()))


def _asgi_body_chunks(app, path: str, watch) -> list[int]:
    """GET *path* straight through the ASGI app, sampling *watch* per chunk.

    ``TestClient`` collects the whole response body before handing back a
    response object, so it reports a buffered archive and a streamed one
    identically — it cannot see this difference at all. Driving the app
    directly can: each ``http.response.body`` message is one chunk the
    server would have flushed, and *watch* is sampled as it goes out.

    Returns the ``watch()`` reading at each non-empty body chunk.
    """
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("test", 1),
        "server": ("test", 80),
    }
    samples: list[int] = []
    statuses: list[int] = []
    first_receive = True

    async def receive():
        nonlocal first_receive
        if first_receive:
            first_receive = False
            return {"type": "http.request", "body": b"", "more_body": False}
        # StreamingResponse races the body against a disconnect listener;
        # never disconnect, and let the body finishing end the exchange.
        await asyncio.Event().wait()

    async def send(message):
        if message["type"] == "http.response.start":
            statuses.append(message["status"])
        elif message["type"] == "http.response.body" and message.get("body"):
            samples.append(watch())

    async def run():
        await asyncio.wait_for(app(scope, receive, send), timeout=30)

    asyncio.run(run())
    assert statuses == [200], statuses
    return samples


class TestStreamingExport:
    """The export routes must not rebuild the whole archive in memory.

    ``ResourceManager.dump`` genuinely streams, but both routes consumed
    it into a ``BytesIO`` and handed *that* to ``StreamingResponse`` — so
    the endpoint's peak memory was the size of the archive, exactly what
    the docs promised it was not.
    """

    N = 50

    def _counting_app(self):
        """An app over ``N`` resources that counts each resource read."""
        spec = _seeded(self.N)
        storage = spec.resource_managers["item"].storage
        reads: list[str] = []
        original = storage.dump_resource

        def counting(resource_id: str):
            reads.append(resource_id)
            return original(resource_id)

        storage.dump_resource = counting
        app = FastAPI()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            spec.apply(app)
        return app, (lambda: len(reads))

    def test_global_export_sends_bytes_before_reading_every_resource(self):
        app, reads_so_far = self._counting_app()

        samples = _asgi_body_chunks(app, "/_backup/export", reads_so_far)

        assert samples[0] < self.N
        assert len(samples) > 1, "a buffered archive goes out as one chunk"
        assert samples[-1] == self.N

    def test_per_model_export_sends_bytes_before_reading_every_resource(self):
        app, reads_so_far = self._counting_app()

        samples = _asgi_body_chunks(app, "/item/export", reads_so_far)

        assert samples[0] < self.N
        assert len(samples) > 1, "a buffered archive goes out as one chunk"
        assert samples[-1] == self.N


class ItemV1(msgspec.Struct):
    name: str
    qty: int


class ItemV2(msgspec.Struct):
    name: str
    quantity: int
    sku: str


class TestStrictAndOldRevisions:
    """Strict mode must not refuse to back up a supported state.

    Revisions stored at an older schema version are first-class here —
    reads migrate them lazily and `migrate()` is explicitly optional. They
    do not decode under the current serializer, and `dump` decodes every
    payload only to harvest blob ids. Making that fatal turned "this model
    has un-migrated rows" into "this model cannot be backed up".
    """

    @staticmethod
    def _store_with_a_v1_revision(tmp_path):
        from specstar import Schema
        from specstar.backend import DiskStorageFactory

        def to_v2(old: ItemV1) -> ItemV2:
            return ItemV2(name=old.name, quantity=old.qty, sku=f"AUTO-{old.name}")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            writer = SpecStar()
            writer.configure(
                storage_factory=DiskStorageFactory(str(tmp_path)), default_user="t"
            )
            writer.add_model(Schema(ItemV1, "v1"), name="item")
            mgr = writer.get_resource_manager(ItemV1)
            with mgr.using(user="t", now=dt.datetime.now()) as ops:
                ops.create(ItemV1(name="widget", qty=7))

            reader = SpecStar()
            reader.configure(
                storage_factory=DiskStorageFactory(str(tmp_path)), default_user="t"
            )
            reader.add_model(
                Schema(ItemV2, "v2").step("v1", to_v2, source_type=ItemV1),
                name="item",
            )
        return reader

    def test_a_model_with_no_blobs_dumps_despite_an_old_revision(self, tmp_path):
        spec = self._store_with_a_v1_revision(tmp_path)
        bio = io.BytesIO()

        stats = spec.dump(bio)

        assert stats["item"].revisions == 1
        # The model cannot carry a `Binary`, so its payloads are never
        # decoded and there is nothing a failed decode could have cost.
        assert stats["item"].complete is True
        assert stats["item"].undecodable_revisions == []
        assert stats["item"].skipped_blobs == []
        records = list(DumpStreamReader(io.BytesIO(bio.getvalue())))
        assert type(records[-1]).__name__ == "EofRecord"

    def test_the_archive_round_trips(self, tmp_path):
        spec = self._store_with_a_v1_revision(tmp_path)
        bio = io.BytesIO()
        spec.dump(bio)

        assert b"widget" in bio.getvalue()


class TestPartiallyRestoredStore:
    """Re-dumping after a partial restore must not be a bare `KeyError`.

    A truncated load is reported, not rolled back, so a resource can end
    up with a meta and no revision data. `dump` read revisions
    unguarded, so that store could not be backed up at all — one partial
    restore became permanent, with an exception no caller could act on.
    """

    @staticmethod
    def _store_missing_a_revision() -> SpecStar:
        archive = _archive(_seeded(3))
        records = list(DumpStreamReader(io.BytesIO(archive)))
        # Keep the final MetaRecord, drop the RevisionRecord that follows.
        kept = [r for i, r in enumerate(records) if i != len(records) - 3]
        out = io.BytesIO()
        writer = DumpStreamWriter(out)
        for record in kept:
            writer.write(record)
        dst = _spec()
        dst.load(io.BytesIO(out.getvalue()))
        return dst

    def test_strict_names_the_resource_it_cannot_read(self):
        dst = self._store_missing_a_revision()

        with pytest.raises(DumpIncompleteError) as exc_info:
            dst.dump(io.BytesIO())

        assert "revisions of" in str(exc_info.value)

    def test_non_strict_reports_it_and_keeps_going(self):
        dst = self._store_missing_a_revision()
        bio = io.BytesIO()

        stats = dst.dump(bio, strict=False)

        assert len(stats["item"].unreadable_resources) == 1
        assert stats["item"].complete is False
        # The other two resources are still exported, and the archive is
        # terminated — a reportable gap, not a dead backup.
        assert stats["item"].revisions == 2
        records = list(DumpStreamReader(io.BytesIO(bio.getvalue())))
        assert type(records[-1]).__name__ == "EofRecord"


class TestStrictOverHttp:
    """The export routes need the same escape hatch the library has.

    With no way to say `strict=false` over HTTP, a single unreadable blob
    made the model unexportable through the API: the route answers 200 and
    then aborts mid-body, so the caller gets a short archive and no error.
    """

    def test_strict_is_the_default_and_truncates_a_damaged_export(self):
        spec, _ = _spec_with_orphaned_blob()
        client = _client(spec)

        body = client.get("/doc/export").content

        records = list(DumpStreamReader(io.BytesIO(body)))
        assert not any(type(r).__name__ == "EofRecord" for r in records)

    def test_strict_false_exports_what_is_readable(self):
        spec, _ = _spec_with_orphaned_blob()
        client = _client(spec)

        body = client.get("/doc/export?strict=false").content

        records = list(DumpStreamReader(io.BytesIO(body)))
        assert type(records[-1]).__name__ == "EofRecord"
        assert any(type(r).__name__ == "MetaRecord" for r in records)


class TestUnMigratedRevisionWithAttachments:
    """The other half of the same question, and the opposite answer.

    For a model that CAN carry a `Binary`, a revision that will not decode
    contributes no file ids — so its attachment is never written and the
    archive comes out short while looking whole. Restoring it gives a
    resource whose `Binary` points at a blob that is not there. That is
    the exact failure #450 exists to kill, so here it is fatal.
    """

    @staticmethod
    def _store(tmp_path):
        from specstar import Schema
        from specstar.backend import DiskStorageFactory

        class DocV1(msgspec.Struct):
            title: str
            file: Binary | None = None

        class DocV2(msgspec.Struct):
            title: str
            owner: str
            file: Binary | None = None

        def to_v2(old: DocV1) -> DocV2:
            return DocV2(title=old.title, owner="auto", file=old.file)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            writer = SpecStar()
            writer.configure(
                storage_factory=DiskStorageFactory(str(tmp_path)), default_user="t"
            )
            writer.add_model(Schema(DocV1, "v1"), name="doc")
            mgr = writer.get_resource_manager(DocV1)
            with mgr.using(user="t", now=dt.datetime.now()) as ops:
                ops.create(DocV1(title="contract", file=Binary(data=b"attachment")))

            reader = SpecStar()
            reader.configure(
                storage_factory=DiskStorageFactory(str(tmp_path)), default_user="t"
            )
            reader.add_model(
                Schema(DocV2, "v2").step("v1", to_v2, source_type=DocV1), name="doc"
            )
        return reader

    def test_strict_refuses_rather_than_lose_the_attachment(self, tmp_path):
        spec = self._store(tmp_path)

        with pytest.raises(DumpIncompleteError) as exc_info:
            spec.dump(io.BytesIO())

        assert "blob references" in str(exc_info.value)
        assert "migrate()" in str(exc_info.value)

    def test_non_strict_says_the_archive_is_not_complete(self, tmp_path):
        spec = self._store(tmp_path)
        bio = io.BytesIO()

        stats = spec.dump(bio, strict=False)

        assert stats["doc"].complete is False
        assert len(stats["doc"].undecodable_revisions) == 1
        # The proof it matters: the attachment is not in the archive.
        assert b"attachment" not in bio.getvalue()
