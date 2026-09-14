import datetime as dt
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager, suppress
from functools import cached_property, wraps
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    NamedTuple,
    TypeVar,
    get_args,
    get_origin,
)
from uuid import uuid4
import inspect
import msgspec
from jsonpatch import JsonPatch
from msgspec import UNSET, Struct, UnsetType
from xxhash import xxh3_128_hexdigest
from autocrud.types import PermissionDeniedError

from autocrud.types import (
    AfterCreate,
    AfterDelete,
    AfterDump,
    AfterGet,
    AfterGetMeta,
    AfterGetResourceRevision,
    AfterListRevisions,
    AfterLoad,
    AfterPatch,
    AfterRestore,
    AfterSearchResources,
    AfterSwitch,
    AfterUpdate,
    BeforeCreate,
    BeforeDelete,
    BeforeDump,
    BeforeGet,
    BeforeGetMeta,
    BeforeGetResourceRevision,
    BeforeListRevisions,
    BeforeLoad,
    BeforePatch,
    BeforeRestore,
    BeforeSearchResources,
    BeforeSwitch,
    BeforeUpdate,
    EventContext,
    IMigration,
    IResourceManager,
    IndexableField,
    OnFailureCreate,
    OnFailureDelete,
    OnFailureDump,
    OnFailureGet,
    OnFailureGetMeta,
    OnFailureGetResourceRevision,
    OnFailureListRevisions,
    OnFailureLoad,
    OnFailurePatch,
    OnFailureRestore,
    OnFailureSearchResources,
    OnFailureSwitch,
    OnFailureUpdate,
    OnSuccessCreate,
    OnSuccessDelete,
    OnSuccessDump,
    OnSuccessGet,
    OnSuccessGetMeta,
    OnSuccessGetResourceRevision,
    OnSuccessListRevisions,
    OnSuccessLoad,
    OnSuccessPatch,
    OnSuccessRestore,
    OnSuccessSearchResources,
    OnSuccessSwitch,
    OnSuccessUpdate,
    RawResource,
    Resource,
    ResourceAction,
    ResourceIDNotFoundError,
    ResourceIsDeletedError,
    ResourceMeta,
    ResourceMetaSearchQuery,
    RevisionIDNotFoundError,
    RevisionInfo,
    RevisionStatus,
    SpecialIndex,
)
from autocrud.types import IEventHandler

if TYPE_CHECKING:
    from autocrud.types import IPermissionChecker


from autocrud.types import PermissionResult
from autocrud.resource_manager.basic import (
    Ctx,
    Encoding,
    IMetaStore,
    IResourceStore,
    IStorage,
    MsgspecSerializer,
)
from autocrud.resource_manager.data_converter import DataConverter
from autocrud.resource_manager.dump_format import MetaRecord, RevisionRecord
from autocrud.util.naming import NameConverter, NamingFormat

T = TypeVar("T")


def _get_type_name(resource_type) -> str:
    """取得類型名稱，處理 Union 類型"""
    if hasattr(resource_type, "__name__"):
        return resource_type.__name__

    # 處理 Union 類型
    origin = get_origin(resource_type)
    if origin is not None:
        args = get_args(resource_type)
        if args:
            # 使用第一個類型的名稱，或者創建一個組合名稱
            first_type = args[0]
            if hasattr(first_type, "__name__"):
                return f"{first_type.__name__}Union"
        return "UnionType"

    # 後備方案
    return str(resource_type).replace(" ", "").replace("|", "Or")


class _ExportResourceMeta(ResourceMeta, kw_only=True):
    """``ResourceMeta`` plus the current-revision mirror fields specstar stores.

    Dump-only: the runtime ``ResourceMeta`` is untouched, so nothing on disk
    changes shape. All five default to ``UNSET`` (omitted when encoded).
    """

    rev_status: RevisionStatus | UnsetType = UNSET
    rev_created_by: str | UnsetType = UNSET
    rev_updated_by: str | UnsetType = UNSET
    rev_created_time: dt.datetime | UnsetType = UNSET
    rev_updated_time: dt.datetime | UnsetType = UNSET


class SimpleStorage(IStorage[T]):
    def __init__(self, meta_store: IMetaStore, resource_store: IResourceStore[T]):
        self._meta_store = meta_store
        self._resource_store = resource_store

    def exists(self, resource_id: str) -> bool:
        return resource_id in self._meta_store

    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        return self.exists(resource_id) and self._resource_store.exists(
            resource_id,
            revision_id,
        )

    def get_meta(self, resource_id: str) -> ResourceMeta:
        return self._meta_store[resource_id]

    def save_meta(self, meta: ResourceMeta) -> None:
        self._meta_store[meta.resource_id] = meta

    def list_revisions(self, resource_id: str) -> list[str]:
        return list(self._resource_store.list_revisions(resource_id))

    def get_resource_revision(self, resource_id: str, revision_id: str) -> Resource[T]:
        return self._resource_store.get(resource_id, revision_id)

    def get_resource_revision_info(
        self,
        resource_id: str,
        revision_id: str,
    ) -> RevisionInfo:
        return self._resource_store.get_revision_info(resource_id, revision_id)

    def save_resource_revision(self, resource: Resource[T]) -> None:
        self._resource_store.save(resource)

    def search(self, query: ResourceMetaSearchQuery) -> list[ResourceMeta]:
        return list(self._meta_store.iter_search(query))

    def encode_data(self, data: T) -> bytes:
        return self._resource_store.encode(data)

    def dump_meta(self) -> Generator[ResourceMeta]:
        yield from self._meta_store.values()

    def dump_resource(self) -> Generator[Resource[T]]:
        for resource_id in self._resource_store.list_resources():
            for revision_id in self._resource_store.list_revisions(resource_id):
                yield self._resource_store.get(resource_id, revision_id)


class _BuildRevMetaCreate(Struct):
    data: T


class _BuildRevInfoUpdate(Struct):
    prev_res_meta: ResourceMeta
    data: T


class _BuildResMetaCreate(Struct):
    rev_info: RevisionInfo
    data: T


class _BuildResMetaUpdate(Struct):
    prev_res_meta: ResourceMeta
    rev_info: RevisionInfo
    data: T


class _Contexts(NamedTuple):
    before: EventContext
    after: EventContext
    on_success: EventContext
    on_failure: EventContext


class PermissionEventHandler(IEventHandler):
    def __init__(self, permission_checker: "IPermissionChecker"):
        self.permission_checker = permission_checker

    def is_supported(self, context: EventContext) -> bool:
        with suppress(AttributeError):
            return context.action in ResourceAction and context.phase == "before"
        return False

    def handle_event(self, context: EventContext) -> None:
        result = self.permission_checker.check_permission(context)
        if result != PermissionResult.allow:
            raise PermissionDeniedError(
                f"Permission denied for user '{context.user}' "
                f"to perform '{context.action}' on '{context.resource_name}'",
            )


def execute_with_events(
    contexts: _Contexts,
    result: str | Callable[[Any], dict[str, Any]],
    *,
    inputs: dict[str, str | UnsetType] | None = None,
):
    contexts = _Contexts(*contexts)
    if isinstance(result, str):

        def _build_result(x):
            return {result: x}

    else:
        _build_result = result

    def wrapper(func):
        sig = inspect.signature(func)

        @wraps(func)
        def wrapped(self: "ResourceManager", *args, **kwargs):
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()  # 應用默認值

            func_inputs = dict(bound_args.arguments)
            del func_inputs["self"]

            inputs_ = func_inputs | {
                "user": self.user_or_unset,
                "now": self.now_or_unset,
                "resource_name": self.resource_name,
            }
            if inputs:

                def get_from_path(d, path: str):
                    parts = path.split(".")
                    current = d
                    for part in parts:
                        if hasattr(current, part):
                            current = getattr(current, part)
                        else:
                            current = current[part]
                    return current

                for k, v in inputs.items():
                    if v is UNSET:
                        del inputs_[k]
                    else:
                        inputs_[k] = get_from_path(func_inputs, v)
            self._handle_event(contexts.before(**inputs_))
            try:
                result = func(self, *args, **kwargs)
                built_result = _build_result(result)
                self._handle_event(contexts.on_success(**inputs_, **built_result))
                return result
            except Exception as e:
                self._handle_event(
                    contexts.on_failure(
                        **inputs_,
                        error=str(e),
                        stack_trace=traceback.format_exc(),
                    )
                )
                raise
            finally:
                self._handle_event(contexts.after(**inputs_))

        return wrapped

    return wrapper


class ResourceManager(IResourceManager[T], Generic[T]):
    def __init__(
        self,
        resource_type: type[T],
        *,
        storage: IStorage[T],
        id_generator: Callable[[], str] | None = None,
        migration: IMigration | None = None,
        indexed_fields: list[IndexableField] | None = None,
        permission_checker: "IPermissionChecker | None" = None,
        name: str | NamingFormat = NamingFormat.SNAKE,
        event_handlers: Sequence[IEventHandler] | None = None,
    ):
        self.user_ctx = Ctx("user_ctx", strict_type=str)
        self.now_ctx = Ctx("now_ctx", strict_type=dt.datetime)
        self.id_ctx = Ctx[str | UnsetType]("id_ctx")
        self._resource_type = resource_type
        self.storage = storage
        self.data_converter = DataConverter(self.resource_type)
        schema_version = migration.schema_version if migration else None
        self.schema_version = UNSET if schema_version is None else schema_version
        self._indexed_fields = indexed_fields or []

        if isinstance(name, NamingFormat):
            self._resource_name = NameConverter(_get_type_name(resource_type)).to(
                NamingFormat.SNAKE,
            )
        else:
            self._resource_name = name

        def default_id_generator():
            return f"{self._resource_name}:{uuid4()}"

        self.id_generator = (
            default_id_generator if id_generator is None else id_generator
        )
        self.event_handlers = list(event_handlers) if event_handlers else []
        # 設定權限檢查器
        if permission_checker is not None:
            self.event_handlers.append(
                PermissionEventHandler(permission_checker),
            )

    @property
    def user(self) -> str:
        return self.user_ctx.get()

    @property
    def now(self) -> dt.datetime:
        return self.now_ctx.get()

    @property
    def user_or_unset(self) -> str | UnsetType:
        try:
            return self.user_ctx.get()
        except LookupError:
            return UNSET

    @property
    def now_or_unset(self) -> dt.datetime | UnsetType:
        try:
            return self.now_ctx.get()
        except LookupError:
            return UNSET

    @property
    def resource_type(self):
        return self._resource_type

    @property
    def resource_name(self):
        return self._resource_name

    @property
    def indexed_fields(self) -> list[IndexableField]:
        """取得被索引的 data 欄位列表"""
        return self._indexed_fields

    def _extract_indexed_values(self, data: T) -> dict[str, Any]:
        """從 data 中提取需要索引的值"""
        indexed_data = {}
        for field in self._indexed_fields:
            try:
                if field.field_type == SpecialIndex.msgspec_tag:
                    value = msgspec.inspect.type_info(type(data)).tag
                else:
                    # 使用 JSON path 提取值
                    value = self._extract_by_path(data, field.field_path)
                indexed_data[field.field_path] = value
            except Exception:
                # 如果提取失敗，跳過該字段
                continue

        return indexed_data

    def _extract_by_path(self, data: T, field_path: str) -> Any:
        """使用 JSON path 從 data 中提取值"""
        # 簡單的點分隔路徑解析 (e.g., "user.email")
        parts = field_path.split(".")
        current = data

        for part in parts:
            if hasattr(current, part):
                current = getattr(current, part)
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        if current is UNSET:
            return None
        return current

    @contextmanager
    def meta_provide(
        self,
        user: str,
        now: dt.datetime,
        *,
        resource_id: str | UnsetType = UNSET,
    ):
        with (
            self.user_ctx.ctx(user),
            self.now_ctx.ctx(now),
            self.id_ctx.ctx(resource_id),
        ):
            yield

    def _res_meta(
        self,
        mode: _BuildResMetaCreate | _BuildResMetaUpdate,
    ) -> ResourceMeta:
        if isinstance(mode, _BuildResMetaCreate):
            current_revision_id = mode.rev_info.revision_id
            resource_id = mode.rev_info.resource_id
            total_revision_count = 1
            created_time = self.now_ctx.get()
            created_by = self.user_ctx.get()
        elif isinstance(mode, _BuildResMetaUpdate):
            current_revision_id = mode.rev_info.revision_id
            resource_id = mode.prev_res_meta.resource_id
            total_revision_count = mode.prev_res_meta.total_revision_count + 1
            created_time = mode.prev_res_meta.created_time
            created_by = mode.prev_res_meta.created_by

        # 提取索引數據
        indexed_data = {}
        if self._indexed_fields:
            extracted = self._extract_indexed_values(mode.data)
            if extracted:
                indexed_data = extracted

        return ResourceMeta(
            current_revision_id=current_revision_id,
            resource_id=resource_id,
            schema_version=self.schema_version,
            total_revision_count=total_revision_count,
            created_time=created_time,
            updated_time=self.now_ctx.get(),
            created_by=created_by,
            updated_by=self.user_ctx.get(),
            indexed_data=indexed_data,
        )

    def get_data_hash(self, data: T) -> str:
        b = self.storage.encode_data(data)
        data_hash = f"xxh3_128:{xxh3_128_hexdigest(b)}"
        return data_hash

    def _rev_info(
        self,
        mode: _BuildRevMetaCreate | _BuildRevInfoUpdate,
    ) -> RevisionInfo:
        uid = uuid4()
        if isinstance(mode, _BuildRevMetaCreate):
            _id = self.id_ctx.get()
            if _id is UNSET:
                resource_id = self.id_generator()
            else:
                resource_id = _id
            revision_id = f"{resource_id}:1"
            last_revision_id = UNSET
        elif isinstance(mode, _BuildRevInfoUpdate):
            prev_res_meta = mode.prev_res_meta
            resource_id = prev_res_meta.resource_id
            revision_id = f"{resource_id}:{prev_res_meta.total_revision_count + 1}"
            last_revision_id = prev_res_meta.current_revision_id

        data_hash = self.get_data_hash(mode.data)

        info = RevisionInfo(
            uid=uid,
            resource_id=resource_id,
            revision_id=revision_id,
            parent_revision_id=last_revision_id,
            schema_version=self.schema_version,
            data_hash=data_hash,
            status=RevisionStatus.stable,
            created_time=self.now_ctx.get(),
            updated_time=self.now_ctx.get(),
            created_by=self.user_ctx.get(),
            updated_by=self.user_ctx.get(),
        )
        return info

    def _handle_event(self, context: EventContext) -> None:
        for eh in self.event_handlers:
            if eh.is_supported(context):
                eh.handle_event(context)

    def _get_meta_no_check_is_deleted(self, resource_id: str) -> ResourceMeta:
        if not self.storage.exists(resource_id):
            raise ResourceIDNotFoundError(resource_id)
        meta = self.storage.get_meta(resource_id)
        return meta

    @execute_with_events(
        (
            BeforeGetMeta,
            AfterGetMeta,
            OnSuccessGetMeta,
            OnFailureGetMeta,
        ),
        "meta",
    )
    def get_meta(self, resource_id: str) -> ResourceMeta:
        meta = self._get_meta_no_check_is_deleted(resource_id)
        if meta.is_deleted:
            raise ResourceIsDeletedError(resource_id)
        return meta

    @execute_with_events(
        (
            BeforeSearchResources,
            AfterSearchResources,
            OnSuccessSearchResources,
            OnFailureSearchResources,
        ),
        "results",
    )
    def search_resources(self, query: ResourceMetaSearchQuery) -> list[ResourceMeta]:
        return self.storage.search(query)

    @execute_with_events(
        (BeforeCreate, AfterCreate, OnSuccessCreate, OnFailureCreate),
        "info",
    )
    def create(self, data: T) -> RevisionInfo:
        info = self._rev_info(_BuildRevMetaCreate(data))
        resource = Resource(
            info=info,
            data=data,
        )
        self.storage.save_resource_revision(resource)
        self.storage.save_meta(self._res_meta(_BuildResMetaCreate(info, data)))
        return info

    @execute_with_events(
        (BeforeGet, AfterGet, OnSuccessGet, OnFailureGet),
        "resource",
    )
    def get(self, resource_id: str) -> Resource[T]:
        meta = self.get_meta(resource_id)
        return self.get_resource_revision(resource_id, meta.current_revision_id)

    @execute_with_events(
        (
            BeforeGetResourceRevision,
            AfterGetResourceRevision,
            OnSuccessGetResourceRevision,
            OnFailureGetResourceRevision,
        ),
        "resource",
    )
    def get_resource_revision(self, resource_id: str, revision_id: str) -> Resource[T]:
        obj = self.storage.get_resource_revision(resource_id, revision_id)
        return obj

    @execute_with_events(
        (
            BeforeListRevisions,
            AfterListRevisions,
            OnSuccessListRevisions,
            OnFailureListRevisions,
        ),
        "revisions",
    )
    def list_revisions(self, resource_id: str) -> list[str]:
        return self.storage.list_revisions(resource_id)

    @execute_with_events(
        (BeforeUpdate, AfterUpdate, OnSuccessUpdate, OnFailureUpdate),
        "revision_info",
    )
    def update(self, resource_id: str, data: T) -> RevisionInfo:
        prev_res_meta = self.get_meta(resource_id)
        prev_info = self.storage.get_resource_revision_info(
            resource_id,
            prev_res_meta.current_revision_id,
        )
        cur_data_hash = self.get_data_hash(data)
        if prev_info.data_hash == cur_data_hash:
            return prev_info
        rev_info = self._rev_info(_BuildRevInfoUpdate(prev_res_meta, data))
        res_meta = self._res_meta(_BuildResMetaUpdate(prev_res_meta, rev_info, data))
        resource = Resource(
            info=rev_info,
            data=data,
        )
        self.storage.save_resource_revision(resource)
        self.storage.save_meta(res_meta)
        return rev_info

    def create_or_update(self, resource_id, data):
        try:
            return self.update(resource_id, data)
        except ResourceIDNotFoundError:
            return self.create(data)

    @execute_with_events(
        (BeforePatch, AfterPatch, OnSuccessPatch, OnFailurePatch),
        "revision_info",
        inputs={"patch_data": "patch_data.patch"},
    )
    def patch(self, resource_id: str, patch_data: JsonPatch) -> RevisionInfo:
        data = self.get(resource_id).data
        d = self.data_converter.data_to_builtins(data)
        patch_data.apply(d, in_place=True)
        data = self.data_converter.builtins_to_data(d)
        return self.update(resource_id, data)

    @execute_with_events(
        (BeforeSwitch, AfterSwitch, OnSuccessSwitch, OnFailureSwitch),
        "meta",
    )
    def switch(self, resource_id: str, revision_id: str) -> ResourceMeta:
        meta = self.get_meta(resource_id)
        if meta.current_revision_id == revision_id:
            return meta
        if not self.storage.revision_exists(resource_id, revision_id):
            raise RevisionIDNotFoundError(resource_id, revision_id)

        # 切換到指定版本時，需要更新索引數據
        if self._indexed_fields:
            resource = self.storage.get_resource_revision(resource_id, revision_id)
            indexed_data = self._extract_indexed_values(resource.data)
            meta.indexed_data = indexed_data or UNSET

        meta.updated_by = self.user_ctx.get()
        meta.updated_time = self.now_ctx.get()
        meta.current_revision_id = revision_id
        self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (BeforeDelete, AfterDelete, OnSuccessDelete, OnFailureDelete),
        "meta",
    )
    def delete(self, resource_id: str) -> ResourceMeta:
        meta = self.get_meta(resource_id)
        meta.is_deleted = True
        meta.updated_by = self.user_ctx.get()
        meta.updated_time = self.now_ctx.get()
        self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (BeforeRestore, AfterRestore, OnSuccessRestore, OnFailureRestore),
        "meta",
    )
    def restore(self, resource_id: str) -> ResourceMeta:
        meta = self._get_meta_no_check_is_deleted(resource_id)
        if meta.is_deleted:
            meta.is_deleted = False
            meta.updated_by = self.user_ctx.get()
            meta.updated_time = self.now_ctx.get()
            self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (BeforeDump, AfterDump, OnSuccessDump, OnFailureDump),
        "result",
        inputs={"encoding": UNSET, "query": UNSET},
    )
    def dump(
        self,
        *,
        encoding: Encoding = Encoding.json,
        query: ResourceMetaSearchQuery | None = None,
    ) -> Generator[MetaRecord | RevisionRecord]:
        payload_encoder = MsgspecSerializer(
            encoding=encoding, resource_type=self.resource_type
        )
        if query is None:
            metas = self.storage.dump_meta()
            resources = self.storage.dump_resource()
        else:
            # A query selects *resources*; each hit is exported with its whole
            # revision history so the meta and the history stay consistent
            # (and re-importing it over an earlier full import is idempotent).
            # limit/offset are paging concerns of the search API, not of an
            # export, so they are overridden.
            hits = self.storage.search(
                msgspec.structs.replace(query, limit=2**31 - 1, offset=0)
            )
            metas = iter(hits)
            resources = (
                self.storage.get_resource_revision(meta.resource_id, revision_id)
                for meta in hits
                for revision_id in self.storage.list_revisions(meta.resource_id)
            )
        for meta in metas:
            yield MetaRecord(data=self.meta_serializer.encode(self._export_meta(meta)))
        for resource in resources:
            raw_data = payload_encoder.encode(resource.data)
            info = resource.info
            # The hash is defined over the stored bytes; re-deriving it keeps it
            # true whether the target encoding matches the store's or not (same
            # encoding -> same deterministic bytes -> same hash as before).
            info.data_hash = f"xxh3_128:{xxh3_128_hexdigest(raw_data)}"
            yield RevisionRecord(
                data=self.resource_serializer.encode(
                    RawResource(info=info, raw_data=raw_data)
                )
            )

    @execute_with_events(
        (BeforeLoad, AfterLoad, OnSuccessLoad, OnFailureLoad),
        lambda _: {},
        inputs={"record": UNSET, "key": "record.__class__.__name__"},
    )
    def load(self, record: MetaRecord | RevisionRecord) -> None:
        if isinstance(record, MetaRecord):
            self.storage.save_meta(self.meta_serializer.decode(record.data))
        elif isinstance(record, RevisionRecord):
            raw = self.resource_serializer.decode(record.data)
            self.storage.save_resource_revision(
                Resource(info=raw.info, data=self._decode_payload(raw.raw_data)),
            )

    def _export_meta(self, meta: ResourceMeta) -> "_ExportResourceMeta":
        """Mirror the current revision's info onto the meta (specstar >= 0.9
        keeps these ``rev_*`` fields on ``ResourceMeta``; filling them here
        means the importer needs no backfill pass)."""
        export = _ExportResourceMeta(**msgspec.structs.asdict(meta))
        try:
            info = self.storage.get_resource_revision_info(
                meta.resource_id, meta.current_revision_id
            )
        except Exception:
            return export  # unreadable current revision: leave UNSET
        export.rev_status = info.status
        export.rev_created_by = info.created_by
        export.rev_updated_by = info.updated_by
        export.rev_created_time = info.created_time
        export.rev_updated_time = info.updated_time
        return export

    def _decode_payload(self, raw_data: bytes) -> T:
        # An archive carries the payload in whichever encoding it was dumped
        # with; a JSON object always starts with ``{`` (msgspec emits no
        # leading whitespace), a msgpack map never does.
        if raw_data[:1] == b"{":
            return self._json_payload_decoder.decode(raw_data)
        return self._msgpack_payload_decoder.decode(raw_data)

    @cached_property
    def _json_payload_decoder(self):
        return msgspec.json.Decoder(self.resource_type)

    @cached_property
    def _msgpack_payload_decoder(self):
        return msgspec.msgpack.Decoder(self.resource_type)

    @cached_property
    def meta_serializer(self):
        # Decodes plain ResourceMeta; encodes _ExportResourceMeta too (a subclass).
        return MsgspecSerializer(encoding=Encoding.msgpack, resource_type=ResourceMeta)

    @cached_property
    def resource_serializer(self):
        return MsgspecSerializer(encoding=Encoding.msgpack, resource_type=RawResource)
