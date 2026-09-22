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
