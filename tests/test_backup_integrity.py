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

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from msgspec import Struct

from specstar.crud.core import SpecStar
from specstar.errors import ArchiveTruncatedError
from specstar.resource_manager.dump_format import (
    DumpStreamReader,
    DumpStreamWriter,
    MetaRecord,
)


class Item(Struct):
    payload: str


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
