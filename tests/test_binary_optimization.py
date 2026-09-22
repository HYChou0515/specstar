import datetime as dt
from typing import Any, Dict, Generic, List, Optional, TypeVar

import pytest
from msgspec import UNSET, Struct

from specstar.resource_manager.binary_processor import BinaryProcessor
from specstar.resource_manager.core import ResourceManager, SimpleStorage
from specstar.resource_manager.meta_store.simple import MemoryMetaStore
from specstar.resource_manager.resource_store.simple import MemoryResourceStore
from specstar.types import (
    Binary,
    Job,
)


class BinaryData(Struct):
    content: Binary
    name: str


class Menu(Struct):
    name: str
    icon: Optional[Binary] = None
    sub_menus: List["Menu"] = []


class UserWithBinary(Struct):
    id: str
    avatar: BinaryData
    files: List[BinaryData]
    metadata: Dict[str, BinaryData]
    optional_binary: Optional[BinaryData] = None


class MockBlobStore:
    def __init__(self):
        self.puts = []

    def put(self, data: bytes, *, content_type: Any = UNSET) -> Binary:
        self.puts.append(data)
        return Binary(file_id="mock_file_id", size=len(data), content_type=content_type)

    def get(self, file_id: str) -> Binary:
        return Binary(data=b"")


@pytest.fixture
def storage():
    resource_store = MemoryResourceStore(encoding="json")  # ty:ignore[invalid-argument-type]
    meta_store = MemoryMetaStore()
    return SimpleStorage(resource_store=resource_store, meta_store=meta_store)


def test_binary_traversal_optimization(storage):
    tracker_store = MockBlobStore()
    manager = ResourceManager(
        resource_type=UserWithBinary,
        storage=storage,
        blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
    )

    assert manager._binary_processor is not None, "Processor should be compiled"

    raw_content = b"testdata"

    # Test Dict Input
    input_dict = {
        "id": "1",
        "avatar": {"content": Binary(data=raw_content), "name": "avatar.png"},
        "files": [
            {"content": Binary(data=raw_content), "name": "file1.txt"},
            {"content": Binary(data=raw_content), "name": "file2.txt"},
        ],
        "metadata": {
            "key1": {"content": Binary(data=raw_content), "name": "meta1.dat"}
        },
    }

    manager._binary_processor.process(input_dict, tracker_store)  # ty:ignore[invalid-argument-type]
    assert len(tracker_store.puts) == 4

    # Test Struct Input
    tracker_store.puts = []
    input_struct = UserWithBinary(
        id="2",
        avatar=BinaryData(content=Binary(data=raw_content), name="avatar.png"),
        files=[
            BinaryData(content=Binary(data=raw_content), name="file1.txt"),
            BinaryData(content=Binary(data=raw_content), name="file2.txt"),
        ],
        metadata={
            "key1": BinaryData(content=Binary(data=raw_content), name="meta1.dat")
        },
    )

    manager._binary_processor.process(input_struct, tracker_store)  # ty:ignore[invalid-argument-type]
    assert len(tracker_store.puts) == 4


def test_recursive_struct_compilation(storage):
    tracker_store = MockBlobStore()
    # This should not raise RecursionError during init
    try:
        manager = ResourceManager(
            resource_type=Menu,
            storage=storage,
            blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
        )
    except RecursionError:
        pytest.fail(
            "ResourceManager init validation hit recursion error on recursive type"
        )

    assert manager._binary_processor is not None

    raw_content = b"icon"

    # Test recursive processing
    menu = Menu(
        name="root",
        icon=Binary(data=raw_content),
        sub_menus=[Menu(name="child", icon=Binary(data=raw_content))],
    )

    manager._binary_processor.process(menu, tracker_store)  # ty:ignore[invalid-argument-type]
    assert len(tracker_store.puts) == 2


def test_recursion_broken_structure(storage):
    # Test that we can handle recursive dicts without infinite loop if they are finite
    tracker_store = MockBlobStore()
    manager = ResourceManager(
        resource_type=Menu,
        storage=storage,
        blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
    )

    raw_content = b"icon"

    # Dict structure simulating recursive menu
    menu_dict = {
        "name": "root",
        "icon": Binary(data=raw_content),
        "sub_menus": [
            {"name": "child", "icon": Binary(data=raw_content), "sub_menus": []}
        ],
    }

    manager._binary_processor.process(menu_dict, tracker_store)  # ty:ignore[invalid-argument-type]
    assert len(tracker_store.puts) == 2


class LooseStruct(Struct):
    payload: Any


def test_binary_generic_coverage(storage):
    tracker_store = MockBlobStore()
    manager = ResourceManager(
        resource_type=LooseStruct,
        storage=storage,
        blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
    )

    # 1. Test Generic List (hits _process_binary_generic list branch)
    tracker_store.puts = []
    data_list = LooseStruct(payload=[Binary(data=b"1"), Binary(data=b"2")])
    processed_list = manager._binary_processor.process(data_list, tracker_store)  # ty:ignore[invalid-argument-type]

    assert len(tracker_store.puts) == 2
    assert processed_list.payload[0].file_id == "mock_file_id"
    assert processed_list.payload[0].data is UNSET

    # 2. Test Generic Dict (hits _process_binary_generic dict branch)
    tracker_store.puts = []
    data_dict = LooseStruct(payload={"k1": Binary(data=b"3"), "k2": Binary(data=b"4")})
    processed_dict = manager._binary_processor.process(data_dict, tracker_store)  # ty:ignore[invalid-argument-type]

    assert len(tracker_store.puts) == 2
    assert processed_dict.payload["k1"].file_id == "mock_file_id"

    # 3. Test Generic Struct (hits _process_binary_generic Struct branch)
    tracker_store.puts = []
    d = BinaryData(content=Binary(data=b"5"), name="n")
    data_struct = LooseStruct(payload=d)
    processed_struct = manager._binary_processor.process(data_struct, tracker_store)  # ty:ignore[invalid-argument-type]

    assert len(tracker_store.puts) == 1
    assert processed_struct.payload.content.file_id == "mock_file_id"

    # 4. Test No Changes (coverage for 'return data' paths)
    tracker_store.puts = []

    # List no change
    l_no_change = LooseStruct(payload=["a", "b"])
    res_l = manager._binary_processor.process(l_no_change, tracker_store)  # ty:ignore[invalid-argument-type]
    # Checks that original object is returned when no changes
    assert res_l.payload is l_no_change.payload

    # Dict no change
    d_no_change = LooseStruct(payload={"k": "v"})
    res_d = manager._binary_processor.process(d_no_change, tracker_store)  # ty:ignore[invalid-argument-type]
    assert res_d.payload is d_no_change.payload

    # Struct no change
    class Simple(Struct):
        x: int

    simple = Simple(x=1)
    s_wrapper = LooseStruct(payload=simple)
    res_s = manager._binary_processor.process(s_wrapper, tracker_store)  # ty:ignore[invalid-argument-type]
    assert res_s.payload is s_wrapper.payload


def test_public_api_binary_handling(storage):
    tracker_store = MockBlobStore()
    manager = ResourceManager(
        resource_type=UserWithBinary,
        storage=storage,
        blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
    )

    raw_content = b"public_api_content"

    # Test Create
    input_struct = UserWithBinary(
        id="create_test",
        avatar=BinaryData(content=Binary(data=raw_content), name="avatar.png"),
        files=[],
        metadata={},
    )

    # Should trigger binary processing via public create()
    # Need to provide meta context (user, now)
    with manager.meta_provide(user="test_user", now=dt.datetime.now(dt.timezone.utc)):
        info = manager.create(input_struct)

        # 1. Check side effect: blob stored
        assert len(tracker_store.puts) == 1, "Blob should be stored upon create"
        assert tracker_store.puts[0] == raw_content

        # 2. Check stored data via get() - file_id should be set, data should be None
        resource = manager.get(info.resource_id)
        saved_data = resource.data

        assert saved_data.avatar.content.file_id == "mock_file_id"
        assert saved_data.avatar.content.data is UNSET

        # Test Update
        new_content = b"updated_content"
        input_struct_update = UserWithBinary(
            id="create_test",
            avatar=BinaryData(content=Binary(data=new_content), name="avatar_v2.png"),
            files=[],
            metadata={},
        )

        manager.update(info.resource_id, input_struct_update)

        assert len(tracker_store.puts) == 2, "Blob should be stored upon update"
        assert tracker_store.puts[1] == new_content

        resource_updated = manager.get(info.resource_id)
        assert resource_updated.data.avatar.content.file_id == "mock_file_id"
        assert resource_updated.data.avatar.content.data is UNSET
        assert resource_updated.data.avatar.name == "avatar_v2.png"


def test_binary_restore(storage):
    class InMemoryBlobStore:
        def __init__(self):
            self.blobs = {}

        def put(self, data: bytes, *, content_type: Any = UNSET) -> Binary:
            file_id = f"hash-{len(self.blobs)}"
            self.blobs[file_id] = Binary(
                file_id=file_id, size=len(data), data=data, content_type=content_type
            )
            return Binary(file_id=file_id, size=len(data), content_type=content_type)

        def get(self, file_id: str) -> Binary:
            return self.blobs[file_id]

        def exists(self, file_id: str) -> bool:
            return file_id in self.blobs

    blob_store = InMemoryBlobStore()
    manager = ResourceManager(
        resource_type=UserWithBinary,
        storage=storage,
        blob_store=blob_store,  # ty:ignore[invalid-argument-type]
    )

    raw_content = b"testdata-restore"

    # create object
    input_struct = UserWithBinary(
        id="restore-test",
        avatar=BinaryData(content=Binary(data=raw_content), name="avatar.png"),
        files=[],
        metadata={},
    )

    # Process converts data to file_id reference
    processed = manager._binary_processor.process(input_struct, blob_store)  # ty:ignore[invalid-argument-type]

    # Check it is processed (data is UNSET, file_id is set)
    assert isinstance(processed.avatar.content.data, type(UNSET))
    assert processed.avatar.content.file_id is not UNSET

    # Now restore
    restored = manager.restore_binary(processed)

    # Check it is restored
    assert restored.avatar.content.data == raw_content  # ty:ignore[unresolved-attribute]
    assert restored.avatar.content.file_id == processed.avatar.content.file_id  # ty:ignore[unresolved-attribute]


def test_binary_generic_restore(storage):
    # Tests _restore_generic by using LooseStruct where payload is Any
    class InMemoryBlobStore:
        def __init__(self):
            self.blobs = {}
            # Pre-populate a blob
            self.blobs["generic-id"] = Binary(
                file_id="generic-id", data=b"generic-restored"
            )

        def put(self, data: bytes, *, content_type: Any = UNSET) -> Binary:  # ty:ignore[empty-body]
            pass

        def get(self, file_id: str) -> Binary:
            return self.blobs[file_id]

    blob_store = InMemoryBlobStore()
    manager = ResourceManager(
        resource_type=LooseStruct,
        storage=storage,
        blob_store=blob_store,  # ty:ignore[invalid-argument-type]
    )

    # 1. Test Generic List restore
    # Payload is a list containing a Binary with only file_id
    stored_binary = Binary(file_id="generic-id", data=UNSET)
    data_list = LooseStruct(payload=[stored_binary])

    restored_list = manager.restore_binary(data_list)
    assert restored_list.payload[0].data == b"generic-restored"  # ty:ignore[unresolved-attribute]

    # 2. Test Generic Dict restore
    data_dict = LooseStruct(payload={"key": stored_binary})
    restored_dict = manager.restore_binary(data_dict)
    assert restored_dict.payload["key"].data == b"generic-restored"  # ty:ignore[unresolved-attribute]

    # 3. Test Generic Struct restore (nested inside Any)
    # Since it's inside 'Any', compiled processor uses _restore_generic which checks isinstance(data, Struct)
    struct_data = BinaryData(content=stored_binary, name="test")
    data_struct_wrapper = LooseStruct(payload=struct_data)

    restored_struct = manager.restore_binary(data_struct_wrapper)
    assert restored_struct.payload.content.data == b"generic-restored"  # ty:ignore[unresolved-attribute]


# ── Generic Struct (Job[T]) tests ──────────────────────────────────────


class ZipFile(Struct):
    content: Binary


class MyPayload(Struct):
    filezip: ZipFile
    age: int


class MyJob(Job[MyPayload]):
    pass


class TestGenericStructBinaryProcess:
    """Tests for Binary fields nested inside Generic Struct types (e.g. Job[T])."""

    def test_concrete_subclass_process(self, storage):
        """MyJob(Job[MyPayload]) should process nested Binary in payload."""
        tracker_store = MockBlobStore()
        manager = ResourceManager(
            resource_type=MyJob,
            storage=storage,
            blob_store=tracker_store,  # ty:ignore[invalid-argument-type]
        )

        data = MyJob(
            payload=MyPayload(filezip=ZipFile(content=Binary(data=b"hello")), age=12)
        )
        processed = manager._binary_processor.process(data, tracker_store)  # ty:ignore[invalid-argument-type]

        assert len(tracker_store.puts) == 1
        assert tracker_store.puts[0] == b"hello"
        assert processed.payload.filezip.content.file_id == "mock_file_id"
        assert processed.payload.filezip.content.data is UNSET

    def test_concrete_subclass_restore(self, storage):
        """MyJob(Job[MyPayload]) should restore nested Binary from blob store."""

        class InMemBlobStore:
            def __init__(self):
                self.blobs = {}

            def put(self, data: bytes, *, content_type: Any = UNSET) -> Binary:
                fid = f"hash-{len(self.blobs)}"
                self.blobs[fid] = Binary(
                    file_id=fid, size=len(data), data=data, content_type=content_type
                )
                return Binary(file_id=fid, size=len(data), content_type=content_type)

            def get(self, file_id: str) -> Binary:
                return self.blobs[file_id]

        blob_store = InMemBlobStore()
        manager = ResourceManager(
            resource_type=MyJob,
            storage=storage,
            blob_store=blob_store,  # ty:ignore[invalid-argument-type]
        )

        data = MyJob(
            payload=MyPayload(
                filezip=ZipFile(content=Binary(data=b"restore-me")), age=5
            )
        )
        processed = manager._binary_processor.process(data, blob_store)  # ty:ignore[invalid-argument-type]
        assert processed.payload.filezip.content.data is UNSET

        restored = manager.restore_binary(processed)
        assert restored.payload.filezip.content.data == b"restore-me"  # ty:ignore[unresolved-attribute]

    def test_generic_alias_process(self):
        """Job[MyPayload] (generic alias, not subclass) should process nested Binary."""
        tracker_store = MockBlobStore()
        processor = BinaryProcessor(Job[MyPayload])

        data = Job(
            payload=MyPayload(filezip=ZipFile(content=Binary(data=b"alias")), age=99)
        )
        processed = processor.process(data, tracker_store)  # ty:ignore[invalid-argument-type]

        assert len(tracker_store.puts) == 1
        assert tracker_store.puts[0] == b"alias"
        assert processed.payload.filezip.content.file_id == "mock_file_id"
        assert processed.payload.filezip.content.data is UNSET

    def test_generic_alias_dict_input(self):
        """Job[MyPayload] should process dict input with nested Binary."""
        tracker_store = MockBlobStore()
        processor = BinaryProcessor(Job[MyPayload])

        data = {
            "payload": {
                "filezip": {"content": Binary(data=b"dict-input")},
                "age": 7,
            },
            "status": "pending",
        }
        processed = processor.process(data, tracker_store)  # ty:ignore[invalid-argument-type]

        assert len(tracker_store.puts) == 1
        assert processed["payload"]["filezip"]["content"].file_id == "mock_file_id"

    def test_generic_alias_restore(self):
        """Job[MyPayload] (generic alias) should restore nested Binary."""

        class InMemBlobStore:
            def __init__(self):
                self.blobs = {"fid-0": Binary(file_id="fid-0", data=b"restored-alias")}

            def put(self, data, *, content_type=UNSET):
                pass

            def get(self, file_id):
                return self.blobs[file_id]

        blob_store = InMemBlobStore()
        processor = BinaryProcessor(Job[MyPayload])

        data = Job(
            payload=MyPayload(
                filezip=ZipFile(content=Binary(file_id="fid-0", data=UNSET)), age=1
            )
        )
        restored = processor.restore(data, blob_store)  # ty:ignore[invalid-argument-type]
        assert restored.payload.filezip.content.data == b"restored-alias"

    def test_custom_generic_struct_process(self):
        """Custom Generic[T] Struct (not Job) with nested Binary should work."""
        U = TypeVar("U")

        class Wrapper(Struct, Generic[U]):
            inner: U
            label: str

        tracker_store = MockBlobStore()
        processor = BinaryProcessor(Wrapper[ZipFile])

        data = Wrapper(inner=ZipFile(content=Binary(data=b"custom")), label="test")
        processed = processor.process(data, tracker_store)  # ty:ignore[invalid-argument-type]

        assert len(tracker_store.puts) == 1
        assert processed.inner.content.file_id == "mock_file_id"
        assert processed.inner.content.data is UNSET


# ======================================================================
# The compiled-processor cache must not claim a type needs processing
# ======================================================================


class _Plain(Struct):
    a: str = ""
    b: str = ""


class _Leaf(Struct):
    f: Optional[Binary] = None


class _Nested(Struct):
    x: Optional[_Leaf] = None


class _SameTypeTwiceThenBlob(Struct):
    p1: Optional[_Plain] = None
    p2: Optional[_Plain] = None
    leaf: Optional[_Leaf] = None


class _Recursive(Struct):
    child: Optional["_Recursive"] = None
    f: Optional[Binary] = None


class _ListOfBlob(Struct):
    items: List[Binary] = []


class _DictOfBlob(Struct):
    m: Dict[str, Binary] = {}


class TestCollectorGate:
    """``_collector is not None`` must mean "this type can carry a Binary".

    ``_compile`` pre-registers a truthy stub in its cache so a recursive
    type can refer to itself mid-compilation, and used to leave the stub
    behind when the compilation concluded that nothing needed processing.
    The *second* lookup of that type then returned the stub, so any
    struct with two fields of the same type claimed it needed processing.
    Behaviour was unaffected — the stub is the identity function — but
    every truthiness test on the result was wrong, and a dump uses one to
    decide whether a model can hold an attachment at all.
    """

    @pytest.mark.parametrize(
        "type_hint,can_hold_a_blob",
        [
            (_Plain, False),
            (_Leaf, True),
            (_Nested, True),
            (_SameTypeTwiceThenBlob, True),
            (_Recursive, True),
            (_ListOfBlob, True),
            (_DictOfBlob, True),
            (Optional[_Leaf], True),
        ],
    )
    def test_the_gate_answers_the_question_it_is_asked(
        self, type_hint: Any, can_hold_a_blob: bool
    ):
        collector = BinaryProcessor(type_hint)._collector

        assert (collector is not None) is can_hold_a_blob

    def test_a_repeated_blob_free_field_does_not_flip_the_answer(self):
        """The exact shape that used to break it: one type, seen twice."""
        assert BinaryProcessor(_Plain)._collector is None
        assert BinaryProcessor(_SameTypeTwiceThenBlob)._collector is not None
