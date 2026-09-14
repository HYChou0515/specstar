"""``AutoCRUD.dump()`` writes the specstar ``.acbak`` v2 stream; ``load()`` reads it back."""

import datetime as dt
import io

import msgspec
import pytest
from msgspec import UNSET, Struct, UnsetType
from xxhash import xxh3_128_hexdigest

from autocrud.crud.core import AutoCRUD
from autocrud.resource_manager.dump_format import DumpStreamReader
from autocrud.resource_manager.storage_factory import DiskStorageFactory
from autocrud.types import RawResource, ResourceIsDeletedError


class User(Struct):
    name: str
    age: int


T0 = dt.datetime(2025, 9, 1, 12, 0, 0)


def _disk_crud(root, *models) -> AutoCRUD:
    crud = AutoCRUD(storage_factory=DiskStorageFactory(root))
    for m in models:
        crud.add_model(m)
    return crud


def _round_trip(src: AutoCRUD, dst: AutoCRUD) -> bytes:
    bio = io.BytesIO()
    src.dump(bio)
    raw = bio.getvalue()
    dst.load(io.BytesIO(raw))
    return raw


def test_dump_then_load_round_trips_one_resource(tmp_path):
    src = _disk_crud(tmp_path / "src", User)
    users = src.get_resource_manager(User)
    with users.meta_provide("alice", T0):
        info = users.create(User(name="a", age=1))

    dst = _disk_crud(tmp_path / "dst", User)
    _round_trip(src, dst)

    loaded = dst.get_resource_manager(User)
    got = loaded.get(info.resource_id)
    assert got.data == User(name="a", age=1)
    assert got.info == info
    assert loaded.get_meta(info.resource_id) == users.get_meta(info.resource_id)


def test_history_soft_delete_and_switch_survive_round_trip(tmp_path):
    src = _disk_crud(tmp_path / "src", User)
    users = src.get_resource_manager(User)
    with users.meta_provide("alice", T0):
        r1 = users.create(User(name="v1", age=1))
    with users.meta_provide("bob", T0 + dt.timedelta(hours=1)):
        r2 = users.update(r1.resource_id, User(name="v2", age=2))
    with users.meta_provide("bob", T0 + dt.timedelta(hours=2)):
        users.switch(r1.resource_id, r1.revision_id)  # current is v1 again
    with users.meta_provide("carol", T0):
        gone = users.create(User(name="gone", age=0))
    with users.meta_provide("carol", T0 + dt.timedelta(days=1)):
        users.delete(gone.resource_id)

    dst = _disk_crud(tmp_path / "dst", User)
    _round_trip(src, dst)
    loaded = dst.get_resource_manager(User)

    meta = loaded.get_meta(r1.resource_id)
    assert meta.current_revision_id == r1.revision_id
    assert meta.total_revision_count == 2
    assert set(loaded.list_revisions(r1.resource_id)) == {
        r1.revision_id,
        r2.revision_id,
    }
    assert loaded.get(r1.resource_id).data == User(name="v1", age=1)
    v2 = loaded.get_resource_revision(r1.resource_id, r2.revision_id)
    assert v2.data == User(name="v2", age=2)
    assert v2.info.parent_revision_id == r1.revision_id

    with pytest.raises(ResourceIsDeletedError):
        loaded.get(gone.resource_id)
    assert loaded.storage.get_meta(gone.resource_id).is_deleted is True


# What ``specstar.SpecStar.load()`` decodes a MetaRecord into (its ResourceMeta),
# spelled out here so the archive contract is checked against the *consumer*.
class SpecstarResourceMeta(Struct, kw_only=True):
    current_revision_id: str
    resource_id: str
    schema_version: str | None = None
    total_revision_count: int
    created_time: dt.datetime
    updated_time: dt.datetime
    created_by: str
    updated_by: str
    is_deleted: bool = False
    indexed_data: dict | UnsetType = UNSET
    rev_status: str | UnsetType = UNSET
    rev_created_by: str | UnsetType = UNSET
    rev_updated_by: str | UnsetType = UNSET
    rev_created_time: dt.datetime | UnsetType = UNSET
    rev_updated_time: dt.datetime | UnsetType = UNSET


def _records(raw: bytes) -> list:
    return list(DumpStreamReader(io.BytesIO(raw)))


def test_archive_is_specstar_v2_with_current_revision_fields_on_meta(tmp_path):
    src = _disk_crud(tmp_path / "src", User)
    users = src.get_resource_manager(User)
    with users.meta_provide("alice", T0):
        r1 = users.create(User(name="v1", age=1))
    with users.meta_provide("bob", T0 + dt.timedelta(hours=1)):
        r2 = users.update(r1.resource_id, User(name="v2", age=2))

    bio = io.BytesIO()
    src.dump(bio)
    records = _records(bio.getvalue())

    assert [type(r).__name__ for r in records] == [
        "HeaderRecord",
        "ModelStartRecord",
        "MetaRecord",
        "RevisionRecord",
        "RevisionRecord",
        "ModelEndRecord",
        "EofRecord",
    ]
    assert records[0].version == 2
    assert records[1].model_name == "user" == records[5].model_name

    meta = msgspec.msgpack.decode(records[2].data, type=SpecstarResourceMeta)
    assert meta.resource_id == r1.resource_id
    assert meta.current_revision_id == r2.revision_id
    assert meta.schema_version is None
    # current-revision mirror fields, so specstar needs no backfill_revision_meta()
    assert meta.rev_status == "stable"
    assert meta.rev_created_by == "bob"
    assert meta.rev_updated_by == "bob"
    assert meta.rev_created_time == T0 + dt.timedelta(hours=1)
    assert meta.rev_updated_time == T0 + dt.timedelta(hours=1)

    by_rev = {}
    for rec in records[3:5]:
        raw = msgspec.msgpack.decode(rec.data, type=RawResource)
        by_rev[raw.info.revision_id] = raw
    assert set(by_rev) == {r1.revision_id, r2.revision_id}
    for rev_id, raw in by_rev.items():
        on_disk = (
            tmp_path / "src" / "user" / "data" / r1.resource_id / f"{rev_id}.data"
        ).read_bytes()
        assert raw.raw_data == on_disk  # byte-identical: JSON in, JSON out
        assert raw.info.data_hash == f"xxh3_128:{xxh3_128_hexdigest(raw.raw_data)}"
    assert by_rev[r2.revision_id].info.parent_revision_id == r1.revision_id


def test_dump_encoding_msgpack_transcodes_payload_and_rehashes(tmp_path):
    src = _disk_crud(tmp_path / "src", User)
    users = src.get_resource_manager(User)
    with users.meta_provide("alice", T0):
        r1 = users.create(User(name="v1", age=1))

    bio = io.BytesIO()
    src.dump(bio, encoding="msgpack")
    records = _records(bio.getvalue())
    raw = msgspec.msgpack.decode(records[3].data, type=RawResource)

    assert msgspec.msgpack.decode(raw.raw_data, type=User) == User(name="v1", age=1)
    assert raw.raw_data != users.storage.encode_data(
        User(name="v1", age=1)
    )  # not the JSON bytes
    assert raw.info.data_hash == f"xxh3_128:{xxh3_128_hexdigest(raw.raw_data)}"
    assert raw.info.data_hash != r1.data_hash  # the hash follows the bytes

    # and the archive still restores (payload encoding is sniffed per record)
    dst = _disk_crud(tmp_path / "dst", User)
    dst.load(io.BytesIO(bio.getvalue()))
    assert dst.get_resource_manager(User).get(r1.resource_id).data == User(
        name="v1", age=1
    )


class Note(Struct):
    body: str


def test_every_model_gets_its_own_section_and_unknown_models_are_refused(tmp_path):
    src = _disk_crud(tmp_path / "src", User, Note)
    with src.get_resource_manager(User).meta_provide("alice", T0):
        u = src.get_resource_manager(User).create(User(name="u", age=1))
    with src.get_resource_manager(Note).meta_provide("alice", T0):
        n = src.get_resource_manager(Note).create(Note(body="n"))
    bio = io.BytesIO()
    src.dump(bio)

    sections = [
        r.model_name
        for r in _records(bio.getvalue())
        if type(r).__name__ == "ModelStartRecord"
    ]
    assert sections == ["user", "note"]

    dst = _disk_crud(tmp_path / "dst", User, Note)
    dst.load(io.BytesIO(bio.getvalue()))
    assert dst.get_resource_manager(User).get(u.resource_id).data == User(
        name="u", age=1
    )
    assert dst.get_resource_manager(Note).get(n.resource_id).data == Note(body="n")

    only_user = _disk_crud(tmp_path / "only_user", User)
    with pytest.raises(ValueError, match="'note'.*registered: user"):
        only_user.load(io.BytesIO(bio.getvalue()))


def test_load_rejects_a_stream_that_is_not_an_archive(tmp_path):
    crud = _disk_crud(tmp_path / "x", User)
    with pytest.raises(ValueError, match="missing header record"):
        crud.load(io.BytesIO(b""))
    with pytest.raises(ValueError, match="missing header record"):
        crud.load(io.BytesIO(b"\x00\x00\x00\x01\x90"))  # a valid frame, wrong record
