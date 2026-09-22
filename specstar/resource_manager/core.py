import base64
import concurrent.futures
import datetime as dt
import inspect
import io
import json
import logging
import threading
import traceback
import warnings
from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager, suppress
from functools import cached_property, cmp_to_key, wraps
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NamedTuple,
    TypedDict,
    TypeVar,
    cast,
)
from uuid import uuid4

import more_itertools as mit
import msgspec
from jsonpatch import JsonPatch
from jsonpointer import JsonPointer
from msgspec import UNSET, Struct, UnsetType
from xxhash import xxh3_128_hexdigest

from specstar.events import (
    AfterCreate,
    AfterDelete,
    AfterDump,
    AfterGet,
    AfterGetMeta,
    AfterGetResourceRevision,
    AfterListRevisions,
    AfterLoad,
    AfterMigrate,
    AfterModify,
    AfterPatch,
    AfterPermanentlyDelete,
    AfterPrune,
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
    BeforeMigrate,
    BeforeModify,
    BeforePatch,
    BeforePermanentlyDelete,
    BeforePrune,
    BeforeRestore,
    BeforeSearchResources,
    BeforeSwitch,
    BeforeUpdate,
    EventContext,
    IEventHandler,
    OnFailureCreate,
    OnFailureDelete,
    OnFailureDump,
    OnFailureGet,
    OnFailureGetMeta,
    OnFailureGetResourceRevision,
    OnFailureListRevisions,
    OnFailureLoad,
    OnFailureMigrate,
    OnFailureModify,
    OnFailurePatch,
    OnFailurePermanentlyDelete,
    OnFailurePrune,
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
    OnSuccessMigrate,
    OnSuccessModify,
    OnSuccessPatch,
    OnSuccessPermanentlyDelete,
    OnSuccessPrune,
    OnSuccessRestore,
    OnSuccessSearchResources,
    OnSuccessSwitch,
    OnSuccessUpdate,
)
from specstar.query_types import AggSpec, ResourceMetaSearchQuery
from specstar.resource_manager.partial import (
    classify_partial_fields,
    create_partial_type,
    filter_struct_partial,
    prune_object,
)
from specstar.resource_manager.pydantic_converter import (  # noqa: E402
    build_validator,
    pydantic_to_dict,
)
from specstar.types import (
    Binary,
    CannotModifyResourceError,
    DumpIncompleteError,
    DuplicateResourceError,
    IConstraintChecker,
    IMessageQueue,
    IMigration,
    IndexableField,
    IResourceManager,
    IValidator,
    MergePatch,
    OnDecodeError,
    OnDuplicate,
    OnUnindexedQuery,
    PatchManyResult,
    PermissionDeniedError,
    RawResource,
    Resource,
    ResourceAction,
    ResourceDecodeError,
    ResourceIDNotFoundError,
    ResourceIsDeletedError,
    ResourceMeta,
    RevisionIDNotFoundError,
    RevisionInfo,
    RevisionNotMigratedError,
    RevisionStatus,
    SearchedResource,
    SpecialIndex,
    SpecStarWarning,
    UndecodableData,
    UnindexedQueryError,
    ValidationError,
)

if TYPE_CHECKING:
    from specstar.permission.access_scope import AccessScope
    from specstar.permission.checker import IPermissionChecker
    from specstar.schema import Schema

from specstar.permission.access_scope import UNRESTRICTED, _Unrestricted
from specstar.permission.checker import PermissionResult, ResourcePart
from specstar.query import QB, ConditionBuilder, Query
from specstar.resource_manager.basic import (
    Ctx,
    Encoding,
    IBlobStore,
    IMetaStore,
    IMetaWithAgg,
    IMetaWithCount,
    IResourceStore,
    IStorage,
    MsgspecSerializer,
)
from specstar.resource_manager.binary_processor import BinaryProcessor
from specstar.resource_manager.dump_format import (
    BlobRecord,
    DumpStats,
    MetaRecord,
    RevisionRecord,
)
from specstar.util.naming import NameConverter, NamingFormat
from specstar.util.type_utils import (
    get_type_display_name,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _validate_resource_id(resource_id: str) -> None:
    """Reject a caller-supplied ``resource_id`` that isn't safe in file paths
    and URLs.

    Path separators break disk storage (the id is used as a filename), and any
    ``/`` makes the resource unaddressable over HTTP (``/{model}/{resource_id}``
    is a single path segment — even ``%2F`` 404s). We fail fast and
    backend-agnostically rather than crash later or create a REST-invisible row.
    Auto-generated ids (``model:uuid``) are unaffected.
    """
    if not isinstance(resource_id, str) or not resource_id.strip():
        raise ValidationError("resource_id must be a non-empty string.")
    if (
        "/" in resource_id
        or "\\" in resource_id
        or any(ord(c) < 32 for c in resource_id)
    ):
        raise ValidationError(
            f"resource_id {resource_id!r} contains characters that are not "
            "allowed: path separators ('/', '\\') or control characters. A "
            "resource_id must be safe in both file paths and URLs — use a flat "
            "id of letters, digits, ':', '-', or '_'."
        )


class IndexedValueExtractor:
    """從資源資料中提取需要索引的欄位值（Enum 會自動轉換為 value）"""

    def __init__(self, indexed_fields: list[IndexableField]):
        self.indexed_fields = indexed_fields

    def _convert_enum_to_value(self, value: Any) -> Any:
        """將 Enum 轉換為其值，但保留其他類型（如 datetime）"""
        from enum import Enum

        if isinstance(value, Enum):
            return value.value
        elif isinstance(value, list):
            return [self._convert_enum_to_value(v) for v in value]
        elif isinstance(value, dict):
            return {k: self._convert_enum_to_value(v) for k, v in value.items()}
        else:
            # 保留其他類型（datetime, int, str, etc.）
            return value

    def extract_indexed_values(self, data: Any) -> dict[str, Any]:
        """從 data 中提取需要索引的值，並將 Enum 轉換為其值"""

        indexed_data = {}
        for field in self.indexed_fields:
            value = UNSET
            if field.field_type == SpecialIndex.msgspec_tag:
                with suppress(Exception):
                    value = getattr(msgspec.inspect.type_info(type(data)), "tag", UNSET)
            else:
                # 使用 JSON path 提取值
                with suppress(Exception):
                    value = self._extract_by_path(data, field.field_path)

            if value is not UNSET:
                # 將 Enum 轉換為其值（但保留其他類型如 datetime）
                key = field.index_key or field.field_path
                indexed_data[key] = self._convert_enum_to_value(value)

        return indexed_data

    def _extract_by_path(self, data: Any, field_path: str) -> Any:
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


class SimpleStorage(IStorage):
    def __init__(self, meta_store: IMetaStore, resource_store: IResourceStore):
        self._meta_store = meta_store
        self._resource_store = resource_store

    @property
    def meta_store(self) -> IMetaStore:
        """Expose the underlying meta store so the ResourceManager can detect
        capabilities like :class:`IMetaWithAgg` (aggregate push-down)."""
        return self._meta_store

    def exists(self, resource_id: str) -> bool:
        return resource_id in self._meta_store

    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        # `get_meta` raises for an unknown id, so reaching the next line already
        # proves the resource exists — the `exists()` that used to guard it asked
        # the meta store the same question a second time, over its own connection.
        meta = self.get_meta(resource_id)
        return self._resource_store.exists(
            resource_id,
            revision_id,
            meta.schema_version,
        )

    def find_revision_schema_version(
        self, resource_id: str, revision_id: str
    ) -> str | None | UnsetType:
        """Find the most recent schema version stored for a revision.

        Returns the highest schema version if the revision exists, or
        ``UNSET`` if the revision was never stored.  Note that ``None``
        is a valid schema version (meaning "no schema configured").
        """
        try:
            versions = list(
                self._resource_store.list_schema_versions(resource_id, revision_id)
            )
        except KeyError:
            return UNSET
        if not versions:
            return UNSET
        # Return the highest (newest) schema version.
        # ``None`` means "no schema" and is always the lowest.
        non_none = [v for v in versions if v is not None]
        if non_none:
            return max(non_none)
        return None  # only None-keyed entry exists

    def get_meta(self, resource_id: str) -> ResourceMeta:
        return self._meta_store[resource_id]

    def save_meta(self, meta: ResourceMeta) -> None:
        self._meta_store[meta.resource_id] = meta

    def list_revisions(self, resource_id: str) -> list[str]:
        return list(self._resource_store.list_revisions(resource_id))

    def get_data_bytes(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> AbstractContextManager[IO[bytes]]:
        if schema_version is UNSET:
            meta = self.get_meta(resource_id)
            schema_version = meta.schema_version
        return self._resource_store.get_data_bytes(
            resource_id, revision_id, schema_version
        )

    def get_resource_revision_info(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> RevisionInfo:
        if schema_version is UNSET:
            meta = self.get_meta(resource_id)
            schema_version = meta.schema_version
        return self._resource_store.get_revision_info(
            resource_id, revision_id, schema_version
        )

    def get_revision_bundle(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> "tuple[RevisionInfo, bytes]":
        """Info and data together — see `IResourceStore.get_revision_bundle`."""
        if schema_version is UNSET:
            meta = self.get_meta(resource_id)
            schema_version = meta.schema_version
        return self._resource_store.get_revision_bundle(
            resource_id, revision_id, schema_version
        )

    def get_revision_bundles(
        self,
        keys: "Sequence[tuple[str, str, str | None]]",
    ) -> "dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]]":
        """A page's worth at once — see `IResourceStore.get_revision_bundles`."""
        return self._resource_store.get_revision_bundles(keys)

    def get_all_revision_infos(self, resource_id: str) -> "dict[str, RevisionInfo]":
        """Every revision's info — see `IResourceStore.get_all_revision_infos`."""
        return self._resource_store.get_all_revision_infos(resource_id)

    def save_revision(self, info: RevisionInfo, data: IO[bytes]) -> None:
        self._resource_store.save(info, data)

    def search(self, query: ResourceMetaSearchQuery) -> list[ResourceMeta]:
        return list(self._meta_store.iter_search(query))

    def iter_search(self, query: ResourceMetaSearchQuery) -> Generator[ResourceMeta]:
        yield from self._meta_store.iter_search(query)

    def count(self, query: ResourceMetaSearchQuery) -> int:
        # #414: push the count into the store's engine when it can do it
        # (SQL COUNT(*)). The fallback below streams EVERY matching row's meta
        # blob over the wire and decodes it into a ResourceMeta just to discard
        # it — an O(n) count. `IMetaWithCount` implementations are contracted to
        # return exactly what this fallback returns (tests/meta_store/test_count.py).
        if isinstance(self._meta_store, IMetaWithCount):
            return self._meta_store.count(query)
        return mit.ilen(self._meta_store.iter_search(query))

    def purge_meta(self, resource_id: str) -> None:
        """Hard-delete metadata for a resource (no soft-delete, no event hooks).

        Used as a compensating action for unique-constraint race-condition
        rollback.  Removes the entry from the meta store directly.
        """
        del self._meta_store[resource_id]

    def purge_resource(self, resource_id: str) -> None:
        """Hard-delete metadata and all revision data for a resource.

        Used by ``permanently_delete`` to physically remove all traces of a
        resource from storage.
        """
        del self._meta_store[resource_id]
        self._resource_store.purge_resource(resource_id)

    def delete_revisions(self, resource_id: str, revision_ids: list[str]) -> None:
        """Hard-delete a set of revisions, leaving metadata untouched.

        Delegates to the resource store; the meta store (current revision,
        ``total_revision_count``) is intentionally not modified.
        """
        self._resource_store.delete_revisions(resource_id, revision_ids)

    def dump_meta(
        self, resource_ids: frozenset[str] | None = None
    ) -> Generator[ResourceMeta]:
        if resource_ids is None:
            yield from self._meta_store.values()
        else:
            for rid in resource_ids:
                if rid in self._meta_store:
                    yield self._meta_store[rid]

    def dump_resource(
        self, resource_id: str
    ) -> Generator[tuple[RevisionInfo, IO[bytes]]]:
        # One read for the whole resource where the store offers it. Walking
        # revisions and schema versions to fetch each info and payload
        # separately made a single-resource dump cost statements proportional to
        # its history — six per revision, measured. A store without a bulk read
        # returns None and the per-revision walk below still serves it.
        bulk = self._resource_store.dump_all_revisions(
            resource_ids=frozenset({resource_id})
        )
        if bulk is not None:
            for info, raw in bulk.get(resource_id, []):
                yield info, io.BytesIO(raw)
            return
        for revision_id in self._resource_store.list_revisions(resource_id):
            for schema_version in self._resource_store.list_schema_versions(
                resource_id, revision_id
            ):
                info = self._resource_store.get_revision_info(
                    resource_id, revision_id, schema_version
                )
                with self._resource_store.get_data_bytes(
                    resource_id, revision_id, schema_version
                ) as data:
                    yield info, data

    @property
    def supports_bulk_dump(self) -> bool:
        """Whether :meth:`dump_resources_bulk` can do anything for us.

        Asked before a dump decides to collect resource ids, so a store
        without a bulk path never pays for the id set it would not use.
        """
        return getattr(self._resource_store, "supports_bulk_dump", False)

    def dump_resources_bulk(
        self, resource_ids: frozenset[str] | None = None
    ) -> dict[str, list[tuple[RevisionInfo, bytes]]] | None:
        """Bulk pre-fetch all revisions (concurrent when supported).

        Returns ``None`` when the underlying resource store does not
        provide a bulk dump method, signalling the caller to fall back
        to per-resource streaming via :meth:`dump_resource`.
        """
        return self._resource_store.dump_all_revisions(resource_ids=resource_ids)

    # ------------------------------------------------------------------
    # Bulk load helpers
    # ------------------------------------------------------------------

    def save_metas_bulk(self, metas: list["ResourceMeta"]) -> None:
        """Bulk save multiple metas.

        Uses the underlying meta store's ``save_many`` when available
        (e.g. PostgreSQL ``execute_batch``); otherwise falls back to
        sequential ``__setitem__`` calls.
        """
        if not metas:
            return
        self._meta_store.save_many(metas)

    def save_revisions_bulk(self, items: list[tuple[RevisionInfo, bytes]]) -> None:
        """Bulk save multiple revisions.

        Uses the underlying resource store's ``save_many`` when
        available (e.g. concurrent S3 PUTs); otherwise falls back to
        sequential :meth:`save` calls.
        """
        if not items:
            return
        self._resource_store.save_many(items)

    def read_metas_bulk(
        self, resource_ids: "Sequence[str]"
    ) -> "dict[str, ResourceMeta]":
        """Bulk-read metas (#434) — the read-side twin of
        :meth:`save_metas_bulk`. Ids with no row are omitted."""
        if not resource_ids:
            return {}
        return self._meta_store.get_many(resource_ids)

    def read_revisions_bulk(
        self,
        items: "Sequence[tuple[str, str, str | None]]",
        *,
        max_bytes: int,
    ) -> "tuple[dict[str, bytes], int]":
        """Bulk-read revision payloads under a memory budget (#434).

        The read-side counterpart of :meth:`save_revisions_bulk`. Returns
        ``(payloads_by_resource_id, consumed)``; the caller drains a large set
        with ``items = items[consumed:]``. See
        :meth:`IResourceStore.read_many` for the budget contract — bytes not
        rows, at least one row always consumed, missing rows omitted.
        """
        if not items:
            return {}, 0
        return self._resource_store.read_many(items, max_bytes=max_bytes)

    def collect_orphans(self) -> int:
        """Reclaim resource/blob data left by interrupted ``create()`` calls.

        The meta store is the source of truth for "what resources exist"; its
        keys are the finalised commit markers. Any resource/blob data the
        resource store holds that no finalised meta points at is garbage from
        an aborted multi-file write and is removed.

        Returns the number of orphaned directories removed (``0`` when the
        resource store does not support reclamation, e.g. in-memory).
        """
        collect = getattr(self._resource_store, "collect_orphans", None)
        if collect is None:
            return 0
        return collect(set(self._meta_store))


class _BlobEntry(Struct, kw_only=True):
    """Internal struct for serialising blob data in dump streams."""

    file_id: str
    data: bytes
    size: int
    content_type: str


class _BuildRevInfoCreate(Struct, Generic[T]):
    data: T
    status: RevisionStatus = RevisionStatus.stable


#: Default memory ceiling for one ``patch_many`` batch (#434). A library
#: default has to be safe on a small machine, so it is deliberately well below
#: what a big host could afford; a caller that knows it has the headroom raises
#: it per call.
DEFAULT_PATCH_MANY_MAX_BYTES = 256 * 1024 * 1024


class _BuildRevInfoUpdate(Struct, Generic[T]):
    prev_res_meta: ResourceMeta
    data: T
    status: RevisionStatus = RevisionStatus.stable


class _BuildRevInfoModify(Struct, Generic[T]):
    prev_res_meta: ResourceMeta
    prev_info: RevisionInfo
    data: T | UnsetType
    status: RevisionStatus | UnsetType = RevisionStatus.stable


class _BuildResMetaCreate(Struct, Generic[T]):
    rev_info: RevisionInfo
    data: T


class _BuildResMetaUpdate(Struct, Generic[T]):
    prev_res_meta: ResourceMeta
    rev_info: RevisionInfo
    data: T


class _BuildResMetaModify(Struct, Generic[T]):
    prev_res_meta: ResourceMeta
    rev_info: RevisionInfo
    data: T | UnsetType


class _Contexts(NamedTuple):
    # Each entry is one of the 64 event-context Struct *classes*; we
    # construct via ``contexts.before(**inputs_)`` etc. ty can't
    # statically validate the dict-spread against the specific
    # constructor signature, so we widen to ``Any``.
    before: Any
    after: Any
    on_success: Any
    on_failure: Any


class _LoadCtxKw(TypedDict):
    """Common kwargs shared by every Load event context constructor."""

    user: str | UnsetType
    now: dt.datetime | UnsetType
    resource_name: str
    record_type: str


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


def _rfc7386_merge(target, patch):
    """Apply an RFC 7386 JSON Merge Patch to ``target`` and return the result.

    A non-object patch replaces the target wholesale; ``null`` values delete the
    corresponding key; nested objects merge recursively.
    """
    if not isinstance(patch, dict):
        return patch
    result = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict):
            result[key] = _rfc7386_merge(result.get(key), value)
        else:
            result[key] = value
    return result


def coerce_data_to_resource_type(func):
    """Decorator that coerces the ``data`` argument to the resource Struct type
    **before** the wrapped function (and therefore before any event handlers)
    executes.  Applied as the *outermost* decorator so that
    ``@execute_with_events`` passes already-coerced data to event contexts.
    """

    @wraps(func)
    def wrapper(self: "ResourceManager", *args, **kwargs):
        sig = inspect.signature(func)
        bound = sig.bind(self, *args, **kwargs)
        bound.apply_defaults()
        data_arg = bound.arguments.get("data")
        if data_arg is not None and data_arg is not UNSET:
            bound.arguments["data"] = self._coerce_data(data_arg)
        # Re-pack into positional + keyword
        new_args = tuple(bound.args[1:])  # strip self
        return func(self, *new_args, **bound.kwargs)

    return wrapper


# Write verbs that target an existing resource by id and are subject to the
# read access-scope as a *precondition*: a request-originated write of a
# resource outside the caller's scope is hidden as 404 before the permission
# checker runs (#398 follow-up). ``create`` is excluded (no existing resource);
# maintenance verbs (migrate/prune/dump/load) run under a privileged/unscoped
# context and are intentionally not gated here.
_SCOPED_WRITE_ACTIONS = frozenset(
    {
        ResourceAction.update,
        ResourceAction.modify,
        ResourceAction.patch,
        ResourceAction.delete,
        ResourceAction.permanently_delete,
        ResourceAction.switch,
        ResourceAction.restore,
    }
)


def execute_with_events(
    contexts: "_Contexts | tuple[Any, Any, Any, Any]",
    result: str | Callable[[Any], dict[str, Any]],
    *,
    inputs: dict[str, str | UnsetType] | None = None,
    context_aware: bool = False,
):
    contexts = _Contexts(*contexts)
    if isinstance(result, str):

        def _build_result(x):
            return {result: x}

    else:
        _build_result = result

    # Names of the context kwargs that context_aware methods accept.
    _CTX_KWARGS = ("user", "now", "resource_id")

    def wrapper(func):
        sig = inspect.signature(func)

        @wraps(func)
        def wrapped(self: "ResourceManager", *args, **kwargs):
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()  # 應用默認值

            func_inputs = dict(bound_args.arguments)
            del func_inputs["self"]

            # ``stack`` always exists so the permission re-entrancy guard (#402)
            # can be armed even for non-context_aware ops (e.g. ``get``, whose
            # nested ``get_meta``/``get_resource_revision`` must skip re-checking).
            stack = ExitStack()
            # ── context_aware: push explicit kwargs into ContextVar ──
            if context_aware:
                ctx_user = func_inputs.get("user", UNSET)
                ctx_now = func_inputs.get("now", UNSET)
                ctx_rid = func_inputs.get("resource_id", UNSET)
                if ctx_user is not UNSET:
                    stack.enter_context(self.user_ctx.ctx(ctx_user))
                if ctx_now is not UNSET:
                    stack.enter_context(self.now_ctx.ctx(ctx_now))
                if ctx_rid is not UNSET:
                    stack.enter_context(self.id_ctx.ctx(ctx_rid))

            # Strip context-only kwargs from func_inputs so they don't
            # leak into event contexts as raw UNSET values.  A kwarg is
            # "context-only" when its default is UNSET in the function
            # signature (user, now, and — for create — resource_id).
            # Positional parameters with the same name (e.g. resource_id
            # in update/delete) must be preserved for event payloads.
            if context_aware:
                for name in ("user", "now", "resource_id"):
                    param = sig.parameters.get(name)
                    if param is not None and param.default is UNSET:
                        func_inputs.pop(name, None)

            try:
                stack.__enter__()

                # Strict mode validation (after ContextVar push)
                if context_aware and self._strict_operation_context:
                    self._validate_write_context(func.__name__)

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
                before_ctx = contexts.before(**inputs_)
                self._authorize_and_announce(before_ctx, stack)
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
            finally:
                stack.__exit__(None, None, None)

        return wrapped

    return wrapper


class ResourceOps(Generic[T]):
    """Context-capturing proxy returned by :meth:`ResourceManager.using`.

    ``ResourceOps`` captures ``user``, ``now``, and ``resource_id`` at
    creation time.  Each method call **re-applies** these values via the
    manager's context system, ensuring that multiple ``ResourceOps``
    instances created from the same manager do not interfere with each
    other.

    This enables the *multiple using()* pattern::

        with mgr.using(user="u1") as op1, mgr.using(user="u2") as op2:
            op1.create(data1)  # created_by = "u1"
            op2.create(data2)  # created_by = "u2"

    After the ``with`` block exits, or if an exception propagates, the
    proxy is **deactivated** and any subsequent method call raises
    :class:`RuntimeError`.

    Note:
        Calling ``op.using(...)`` or ``op.meta_provide(...)`` is
        forbidden and raises :class:`RuntimeError`.
    """

    __slots__ = (
        "_mgr",
        "_user",
        "_now",
        "_resource_id",
        "_apply_access_scope",
        "_active",
    )

    def __init__(
        self,
        mgr: "IResourceManager[T]",
        user: "str | UnsetType",
        now: "dt.datetime | UnsetType",
        resource_id: "str | UnsetType",
        apply_access_scope: bool = False,
    ) -> None:
        object.__setattr__(self, "_mgr", mgr)
        object.__setattr__(self, "_user", user)
        object.__setattr__(self, "_now", now)
        object.__setattr__(self, "_resource_id", resource_id)
        object.__setattr__(self, "_apply_access_scope", apply_access_scope)
        object.__setattr__(self, "_active", True)

    def _deactivate(self) -> None:
        """Mark this proxy as inactive (called automatically on context exit)."""
        object.__setattr__(self, "_active", False)

    def __getattr__(self, name: str) -> Any:
        if name in ("using", "meta_provide"):
            raise RuntimeError("Cannot rebind context through ResourceOps")
        if not self._active:
            raise RuntimeError("ResourceOps is no longer active")
        attr = getattr(self._mgr, name)
        if inspect.ismethod(attr):

            @wraps(attr)
            def _wrapper(*args: Any, **kwargs: Any) -> Any:
                if not self._active:
                    raise RuntimeError("ResourceOps is no longer active")
                with self._mgr._apply_context(
                    self._user,
                    self._now,
                    resource_id=self._resource_id,
                    apply_access_scope=self._apply_access_scope,
                ):
                    return attr(*args, **kwargs)

            return _wrapper
        return attr

    if TYPE_CHECKING:
        # -- Stubs for IDE auto-complete / type checking --
        def create(
            self,
            data: T,
            *,
            status: RevisionStatus | UnsetType = ...,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
            resource_id: str | UnsetType = ...,
        ) -> RevisionInfo: ...
        def update(
            self,
            resource_id: str,
            data: T,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> RevisionInfo: ...
        def create_or_update(
            self,
            resource_id: str,
            data: T,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> RevisionInfo: ...
        def get(
            self,
            resource_id: str,
            *,
            revision_id: str | UnsetType = ...,
            schema_version: str | None | UnsetType = ...,
        ) -> Resource[T]: ...
        def get_partial(
            self,
            resource_id: str,
            revision_id: str,
            partial: Iterable[str | JsonPointer],
        ) -> Struct: ...
        def get_revision_info(
            self,
            resource_id: str,
            revision_id: str | UnsetType = ...,
        ) -> RevisionInfo: ...
        def get_resource_revision(
            self,
            resource_id: str,
            revision_id: str,
            schema_version: str | None | UnsetType = ...,
        ) -> Resource[T]: ...
        def list_revisions(self, resource_id: str) -> list[str]: ...
        def get_meta(
            self, resource_id: str, include_deleted: bool = ...
        ) -> ResourceMeta: ...
        def exists(self, resource_id: str) -> bool: ...
        def revision_exists(self, resource_id: str, revision_id: str) -> bool: ...
        def count_resources(
            self, query: ResourceMetaSearchQuery | None = ...
        ) -> int: ...
        def search_resources(
            self, query: ResourceMetaSearchQuery | None = ...
        ) -> list[ResourceMeta]: ...
        def list_resources(
            self,
            query: ResourceMetaSearchQuery | None = ...,
            *,
            returns: list[str] | None = ...,
            partial: list[str] | None = ...,
        ) -> list[SearchedResource[T]]: ...
        def modify(
            self,
            resource_id: str,
            data: T | JsonPatch | UnsetType = ...,
            status: RevisionStatus | UnsetType = ...,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> RevisionInfo: ...
        def patch(
            self,
            resource_id: str,
            patch_data: JsonPatch,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> RevisionInfo: ...
        def switch(
            self,
            resource_id: str,
            revision_id: str,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> ResourceMeta: ...
        def delete(
            self,
            resource_id: str,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> ResourceMeta: ...
        def restore(
            self,
            resource_id: str,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> ResourceMeta: ...
        def permanently_delete(
            self,
            resource_id: str,
            *,
            user: str | UnsetType = ...,
            now: dt.datetime | UnsetType = ...,
        ) -> ResourceMeta: ...
        def migrate(
            self,
            resource_id: str,
            *,
            revision_id: str | UnsetType = ...,
        ) -> ResourceMeta: ...
        def get_blob(self, file_id: str) -> Binary: ...
        def get_blob_url(self, file_id: str) -> str | None: ...


class ResourceManager(IResourceManager[T], Generic[T]):
    def __init__(
        self,
        resource_type: type[T],
        *,
        storage: IStorage,
        blob_store: IBlobStore | None = None,
        message_queue: Callable[["IResourceManager[T]"], IMessageQueue] | None = None,
        id_generator: Callable[[], str] | None = None,
        migration: "IMigration[T] | Schema[T] | None" = None,
        indexed_fields: list[IndexableField] | None = None,
        permission_checker: "IPermissionChecker | None" = None,
        access_scope: "AccessScope | None" = None,
        name: str | NamingFormat = NamingFormat.SNAKE,
        event_handlers: Sequence[IEventHandler] | None = None,
        encoding: Encoding = Encoding.json,
        default_status: RevisionStatus = RevisionStatus.stable,
        default_user: str | Callable[[], str] | UnsetType = UNSET,
        default_now: Callable[[], dt.datetime] | UnsetType = UNSET,
        validator: "Callable[[T], None] | IValidator | type | None" = None,
        pydantic_type: type | None = None,
        constraint_checkers: "Sequence[IConstraintChecker | Callable[[ResourceManager], IConstraintChecker]] | None" = None,
        strict_operation_context: bool = False,
        forbid_unknown_fields: bool = False,
        on_decode_error: OnDecodeError = OnDecodeError.skip,
        on_unindexed_query: OnUnindexedQuery = OnUnindexedQuery.warn,
        default_get_returns: "str | list[str]" = "data,revision_info,meta",
        default_is_deleted: bool | None = None,
        encoder_registry: "Any | None" = None,
        vector_encoders: "dict[str, str | Callable] | None" = None,
    ):
        """Initialize a ResourceManager.


        Args:
        strict_operation_context (bool):
            Whether strict operation context validation is enabled.

            When ``True``, write operations (create, update, delete, etc.) will
            raise :class:`MissingOperationContextError` if required context
            fields (``user``, ``now``) are not fully resolved from any source
            (explicit kwargs, ``using()`` scope, or manager defaults).
        forbid_unknown_fields (bool):
            When ``True``, ``create()`` / ``update()`` / ``modify()`` reject
            dict inputs (or Pydantic instances) carrying keys that are not
            declared on the resource ``Struct``, raising
            :class:`specstar.types.ValidationError`. Defaults to ``False`` —
            unknown keys are silently dropped, matching msgspec's default.
        """
        self._pydantic_type = pydantic_type
        self._strict_operation_context = strict_operation_context
        self._forbid_unknown_fields = forbid_unknown_fields
        self._on_decode_error = OnDecodeError(on_decode_error)
        self._on_unindexed_query = OnUnindexedQuery(on_unindexed_query)
        self._default_get_returns = (
            ",".join(default_get_returns)
            if isinstance(default_get_returns, (list, tuple))
            else default_get_returns
        )
        self._default_is_deleted = default_is_deleted
        self._warned_lazy_migration = False

        # ── Resolve Schema vs legacy migration/validator ──────────────
        from specstar.schema import Schema as _Schema

        if isinstance(migration, _Schema):
            self._schema = migration
            # Extract validator from Schema if present and no explicit validator
            if migration.has_validator and validator is None:
                validator = migration._validator  # already normalized callable
            # Use Schema as migration if it has migration steps
            migration_obj = migration if migration.has_migration else None
        elif isinstance(migration, IMigration):
            self._schema = _Schema.from_legacy(migration)
            migration_obj = self._schema
        elif migration is None:
            self._schema = None
            migration_obj = None
        else:
            raise TypeError(
                f"migration must be Schema, IMigration, or None, got {type(migration).__name__}"
            )

        # When no default is configured we fall back to the same values an
        # unauthenticated HTTP request records ("anonymous" + UTC now), so a
        # programmatic ``create``/``update`` without a ``using()`` context
        # behaves like the HTTP path instead of leaking a raw ``LookupError``.
        # In strict mode we leave the context defaultless so a missing context
        # raises ``MissingOperationContextError`` (see _validate_write_context).
        self.user_ctx: Ctx[str]
        self.now_ctx: Ctx[dt.datetime]
        if default_user is UNSET:
            if strict_operation_context:
                self.user_ctx = Ctx("user_ctx", strict_type=str)
            else:
                self.user_ctx = Ctx("user_ctx", strict_type=str, default="anonymous")
        elif isinstance(default_user, str):
            self.user_ctx = Ctx("user_ctx", strict_type=str, default=default_user)
        else:
            self.user_ctx = Ctx(
                "user_ctx",
                strict_type=str,
                default_factory=default_user,
            )
        if default_now is not UNSET:
            self.now_ctx = Ctx(
                "now_ctx", strict_type=dt.datetime, default_factory=default_now
            )
        elif strict_operation_context:
            self.now_ctx = Ctx("now_ctx", strict_type=dt.datetime)
        else:
            self.now_ctx = Ctx(
                "now_ctx",
                strict_type=dt.datetime,
                default_factory=lambda: dt.datetime.now(dt.timezone.utc),
            )
        self.id_ctx = Ctx[str | UnsetType]("id_ctx", default=UNSET)
        # Per-model read access-scope predicate (issue #398). Only consulted
        # when ``apply_scope_ctx`` is True — i.e. for request-originated reads
        # entered via ``using(..., apply_access_scope=True)`` from the generated
        # read-route templates. Internal reads default to unscoped.
        self._access_scope = access_scope
        self.apply_scope_ctx = Ctx[bool]("apply_scope_ctx", default=False)
        # Re-entrancy guard for permission checking (issue #402). A single
        # request-level operation (e.g. ``patch`` → internal ``get`` + ``update``;
        # ``get`` → internal ``get_meta`` + ``get_resource_revision``) used to run
        # the permission checker once per nested op. Authorization is now applied
        # once, at the outermost operation: the first event-emitting op in a call
        # stack arms this flag for its whole extent, and any nested op observing it
        # skips ONLY the permission check (every other event handler still fires).
        # Mirrors ``apply_scope_ctx``'s request-boundary model.
        self._perm_checked_ctx = Ctx[bool]("perm_checked_ctx", default=False)
        self._resource_type = resource_type
        self.storage = storage
        self.blob_store = blob_store

        # Set resource_name early because message_queue initialization may need it
        if isinstance(name, NamingFormat):
            self._resource_name = NameConverter(
                get_type_display_name(resource_type)
            ).to(
                NamingFormat.SNAKE,
            )
        else:
            self._resource_name = name

        schema_version = (
            self._schema.schema_version if self._schema is not None else None
        )
        self._schema_version = schema_version
        self._indexed_fields = indexed_fields or []
        self._indexed_value_extractor = IndexedValueExtractor(self._indexed_fields)
        self._migration = self._schema if self._schema is not None else migration_obj
        self._encoding = encoding
        # Sync encoding to Schema so multi-step migrations re-encode
        # intermediate results in the correct format (json vs msgpack).
        if self._schema is not None:
            self._schema.set_encoding(encoding)
        self._data_serializer = MsgspecSerializer(
            encoding=encoding,
            resource_type=resource_type,
        )
        self.default_status = default_status

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

        # Constraint checkers（放在最後，在 PermissionEventHandler 之後執行）
        from specstar.resource_manager.constraint_lifecycle import (
            build_constraint_handler,
        )

        self._constraint_handler = build_constraint_handler(self, constraint_checkers)
        if self._constraint_handler is not None:
            self.event_handlers.append(self._constraint_handler)

        self._binary_processor = BinaryProcessor(resource_type)

        # ── Vector / Embedding ────────────────────────────────────────────
        from specstar.resource_manager.embedding_processor import EmbeddingProcessor
        from specstar.resource_manager.encoder_registry import EncoderRegistry

        self._encoder_registry = encoder_registry or EncoderRegistry()
        self._vector_encoders = vector_encoders or {}
        self._embedding_processor = EmbeddingProcessor(
            resource_type,
            self._encoder_registry,
            model_overrides=self._vector_encoders,
        )

        # Set up validator
        self._validator = build_validator(validator)

        # Message queue is provided as a factory callable
        if message_queue is not None:
            self.message_queue = message_queue(self)
        else:
            self.message_queue = None

        # Reverse mapping filled by SpecStar._register_async_job_models().
        # Maps job resource name → job ResourceManager for async create actions
        # that target this resource.
        self._async_create_job_rms: dict[str, "IResourceManager"] = {}

        # Reverse mapping filled by SpecStar._register_async_update_job_models().
        # Maps job resource name → job ResourceManager for async update actions
        # that target this resource.
        self._async_update_job_rms: dict[str, "IResourceManager"] = {}

    def register_async_create_job(
        self, job_resource_name: str, job_rm: "IResourceManager"
    ) -> None:
        """Register an async create-job ResourceManager for this resource.

        Called by :meth:`SpecStar._register_async_job_models` to populate the
        reverse mapping so that :meth:`start_consume` can locate child job
        consumers via ``custom_creation``.

        Args:
            job_resource_name: The registered name of the Job resource.
            job_rm: The :class:`ResourceManager` instance for the Job resource.

        Raises:
            ValueError: If *job_resource_name* is already registered.
        """
        if job_resource_name in self._async_create_job_rms:
            raise ValueError(
                f"Async create-job '{job_resource_name}' is already registered "
                f"on resource '{self.resource_name}'."
            )
        self._async_create_job_rms[job_resource_name] = job_rm

    @property
    def async_create_job_names(self) -> list[str]:
        """Return the names of all registered async create-job resources.

        Returns:
            A list of job resource names registered via
            :meth:`register_async_create_job`.
        """
        return list(self._async_create_job_rms.keys())

    def register_async_update_job(
        self, job_resource_name: str, job_rm: "ResourceManager"
    ) -> None:
        """Register an async update-job ResourceManager for this resource.

        Called by :meth:`SpecStar._register_async_update_job_models` to
        populate the reverse mapping so that consumers can locate child
        job resources.

        Args:
            job_resource_name: The registered name of the Job resource.
            job_rm: The :class:`ResourceManager` instance for the Job resource.

        Raises:
            ValueError: If *job_resource_name* is already registered.
        """
        if job_resource_name in self._async_update_job_rms:
            raise ValueError(
                f"Async update-job '{job_resource_name}' is already registered "
                f"on resource '{self.resource_name}'."
            )
        self._async_update_job_rms[job_resource_name] = job_rm

    @property
    def async_update_job_names(self) -> list[str]:
        """Return the names of all registered async update-job resources.

        Returns:
            A list of job resource names registered via
            :meth:`register_async_update_job`.
        """
        return list(self._async_update_job_rms.keys())

    def encode(self, data: T) -> bytes:
        return self._data_serializer.encode(data)

    def decode(self, data: bytes) -> T:
        return self._data_serializer.decode(data)

    def _decode_and_validate(self, data: bytes) -> None:
        return self._data_serializer.decode_and_validate(data)

    def _run_validator(self, data: T) -> None:
        """Run the custom validator on the data if one is configured."""
        if self._validator is not None:
            try:
                self._validator(data)
            except ValidationError:
                raise
            except Exception as e:
                raise ValidationError(str(e)) from e

    def _coerce_data(self, data: Any) -> T:
        """Coerce data to the resource Struct type.

        Accepts:
        - msgspec Struct instance → returned as-is
        - dict → converted via msgspec.convert
        - Pydantic BaseModel instance (when pydantic_type is set)
          → model_dump() → msgspec.convert

        When ``forbid_unknown_fields`` is set on the manager, dict / Pydantic
        inputs carrying keys outside the resource ``Struct``'s declared
        top-level fields raise :class:`ValidationError` instead of having
        those keys silently dropped by ``msgspec.convert``.

        This allows Pydantic users to pass native Pydantic instances
        or plain dicts without knowing about msgspec.
        """
        if data is UNSET or type(data) is JsonPatch or type(data) is MergePatch:
            return data
        if isinstance(data, Struct):
            return data
        if isinstance(data, dict):
            if self._forbid_unknown_fields:
                self._check_no_unknown_fields(data)
            return msgspec.convert(data, self._resource_type)
        # Accept Pydantic instance when RM was configured with pydantic_type
        if self._pydantic_type is not None and isinstance(data, self._pydantic_type):
            as_dict = pydantic_to_dict(data)
            if self._forbid_unknown_fields:
                self._check_no_unknown_fields(as_dict)
            return msgspec.convert(as_dict, self._resource_type)
        return data

    def _check_no_unknown_fields(self, data: dict) -> None:
        """Raise :class:`ValidationError` if ``data`` carries keys outside the
        resource ``Struct``'s top-level fields.

        Top-level only — nested ``Struct`` fields still rely on msgspec's
        default permissive behavior. This catches the common typo case
        without imposing deep-walk overhead on every write.
        """
        try:
            allowed = {f.name for f in msgspec.structs.fields(self._resource_type)}
        except TypeError:
            return
        extra = [k for k in data.keys() if k not in allowed]
        if extra:
            raise ValidationError(
                f"Unknown field(s) for {self._resource_type.__name__}: "
                f"{sorted(extra)}. Allowed fields: {sorted(allowed)}"
            )

    @property
    def pydantic_type(self) -> type | None:
        """The original Pydantic model class, if this RM was created from one."""
        return self._pydantic_type

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
    def default_get_returns(self) -> str:
        """Default value for the GET ``returns`` query param (the response shape
        when the client omits ``?returns=``). See ``OnUnindexedQuery`` siblings."""
        return self._default_get_returns

    @property
    def schema_version(self) -> str:
        if self._schema_version is None:
            raise ValueError("Schema version is not set for this resource manager")
        return self._schema_version

    @execute_with_events(
        (
            BeforeMigrate,
            AfterMigrate,
            OnSuccessMigrate,
            OnFailureMigrate,
        ),
        "meta",
    )
    def migrate(
        self,
        resource_id: str,
        *,
        revision_id: str | UnsetType = UNSET,
    ) -> ResourceMeta:
        """
        Migrate a resource to the latest schema version.

        When *revision_id* is ``UNSET`` (the default), the **current**
        revision (``meta.current_revision_id``) is migrated and
        ``meta.schema_version`` is updated accordingly.

        When *revision_id* is provided, only **that specific revision**
        is migrated.  ``meta.schema_version`` is **not** changed (it
        should already be at the latest version from migrating the
        current revision first).

        Arguments:
            resource_id (str): The ID of the resource to migrate.
            revision_id (str | UnsetType): Optional. A specific revision
                to migrate. If ``UNSET``, migrates the current revision.

        Returns:
            meta (ResourceMeta): The (possibly updated) metadata after migration.

        Raises:
            ValueError: If migration logic is not configured.
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If *revision_id* does not exist.
        """
        if self._migration is None:
            raise ValueError("Migration is not set for this resource manager")

        meta = self._get_meta_no_check_is_deleted(resource_id)

        if revision_id is UNSET:
            # --- Migrate the current revision (original behaviour) ---
            target_rev = meta.current_revision_id
            actual_sv = meta.schema_version
        else:
            # --- Migrate a specific (possibly non-current) revision ---
            target_rev = revision_id
            actual_sv = self.storage.find_revision_schema_version(
                resource_id, target_rev
            )
            if actual_sv is UNSET:
                raise RevisionIDNotFoundError(resource_id, target_rev)

        # Info and payload are two columns of the same joined row, and a
        # migration that proceeds needs both. Fetching them separately cost an
        # extra query and an extra connection checkout PER RESOURCE — and a
        # migration runs once per resource in the store.
        info, raw = self.storage.get_revision_bundle(
            resource_id, target_rev, schema_version=actual_sv
        )

        # 檢查是否需要遷移
        if info.schema_version == self._migration.schema_version:
            return meta

        # 執行數據遷移
        migrated_data = self._migration.migrate(io.BytesIO(raw), info.schema_version)

        # 更新 resource info 的 schema_version
        info.parent_schema_version = info.schema_version
        info.schema_version = self._migration.schema_version

        if revision_id is UNSET:
            # Only update meta-level schema_version when migrating *current* revision
            meta.schema_version = self._migration.schema_version
            meta.indexed_data = self._extract_indexed_values(migrated_data)
            self.storage.save_meta(meta)

        self.storage.save_revision(info, io.BytesIO(self.encode(migrated_data)))

        return meta

    def backfill_revision_meta(self) -> int:
        """Populate ``ResourceMeta.rev_*`` for resources created before this feature.

        Iterates every resource currently in the meta store; for each one
        whose ``rev_status`` is still ``UNSET`` (i.e. the resource was
        created on a release that did not yet embed current-revision info),
        loads the current ``RevisionInfo`` from the resource store and
        copies its ``status`` / ``created_by`` / ``updated_by`` /
        ``created_time`` / ``updated_time`` into the meta, then saves it.

        Returns:
            int: The number of resources that were updated.

        Notes:
            This is opt-in (not auto-run on startup) because for very
            large stores it can be expensive — every backfilled resource
            triggers a read against ``IResourceStore``.  Run it once
            after upgrading.
        """
        updated = 0
        # Snapshot the metas first so an update half-way through doesn't
        # disturb the iterator (some meta stores are dict-backed).
        for meta in list(self.storage.dump_meta()):
            if meta.rev_status is not UNSET:
                continue
            try:
                info = self.storage.get_resource_revision_info(
                    meta.resource_id,
                    meta.current_revision_id,
                    schema_version=meta.schema_version,
                )
            except Exception:
                # Skip resources whose current revision cannot be read —
                # the caller can investigate them separately.
                continue
            meta.rev_status = info.status
            meta.rev_created_by = info.created_by
            meta.rev_updated_by = info.updated_by
            meta.rev_created_time = info.created_time
            meta.rev_updated_time = info.updated_time
            self.storage.save_meta(meta)
            updated += 1
        return updated

    @property
    def resource_name(self):
        return self._resource_name

    @property
    def strict_operation_context(self) -> bool:
        """Whether strict operation context validation is enabled."""
        return self._strict_operation_context

    @property
    def indexed_fields(self) -> list[IndexableField]:
        """取得被索引的 data 欄位列表"""
        return self._indexed_fields

    def add_indexed_field(self, field: IndexableField) -> None:
        """新增一個索引欄位並重建 extractor。

        如果該欄位已存在（依 ``field_path`` 判斷），則不重複新增。
        """
        existing = {f.field_path for f in self._indexed_fields}
        if field.field_path not in existing:
            self._indexed_fields.append(field)
            self._indexed_value_extractor = IndexedValueExtractor(self._indexed_fields)

    def _extract_indexed_values(self, data: T) -> dict[str, Any]:
        """從 data 中提取需要索引的值（保留原始類型，Enum 會在序列化時轉換）"""
        return self._indexed_value_extractor.extract_indexed_values(data)

    def _load_revision_data(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> T:
        """Load and decode data for a specific revision."""
        with self.storage.get_data_bytes(
            resource_id, revision_id, schema_version=schema_version
        ) as data_io:
            return self.decode(data_io.read())

    @contextmanager
    def using(
        self,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        *,
        resource_id: str | UnsetType = UNSET,
        apply_access_scope: bool = False,
    ) -> Generator[ResourceOps[T], None, None]:
        """Context manager to provide operation context for write operations.

        Returns a :class:`ResourceOps` proxy that **captures** the supplied
        ``user``, ``now``, and ``resource_id`` values.  Each method call on
        the proxy re-applies its captured context, so multiple proxies
        created from the same manager do not interfere with each other.

        Resolution order (highest to lowest priority):
            1. Explicit keyword arguments on the method call
            2. Active ``using()`` scope (via the :class:`ResourceOps` proxy)
            3. Manager defaults (``default_user``, ``default_now``)

        Args:
            user: The user performing the action.
            now: The current timestamp.
            resource_id: Specific resource ID to use for ``create()``.

        Yields:
            ResourceOps[T]: A context-capturing proxy.  Calling methods
                on it is equivalent to calling them on the manager with
                the captured context.

        Example::

            # Single context
            with mgr.using(user="alice", now=datetime.now()) as op:
                op.create(data1)
                op.update(rid, data2)

            # Multiple contexts (safe — each proxy has its own capture)
            with (
                mgr.using(user="u1", now=now) as op1,
                mgr.using(user="u2", now=now) as op2,
            ):
                op1.create(data1)  # created_by = "u1"
                op2.create(data2)  # created_by = "u2"
        """
        ops = ResourceOps(self, user, now, resource_id, apply_access_scope)
        with self._apply_context(
            user=user,
            now=now,
            resource_id=resource_id,
            apply_access_scope=apply_access_scope,
        ):
            try:
                yield ops
            finally:
                ops._deactivate()

    @contextmanager
    def meta_provide(
        self,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        *,
        resource_id: str | UnsetType = UNSET,
    ):
        """Context manager to provide metadata context (user, time, resource_id).

        .. deprecated::
            Use :meth:`using` instead.  ``meta_provide`` will be removed
            in a future release.

        Arguments:
            user (str, optional): The user performing the action.
            now (datetime, optional): The current timestamp.
            resource_id (str, optional): Specific resource ID to use.
        """
        warnings.warn(
            "meta_provide() is deprecated, use using() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        with self.using(user, now, resource_id=resource_id):
            yield

    @contextmanager
    def _apply_context(
        self,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        *,
        resource_id: str | UnsetType = UNSET,
        apply_access_scope: bool = False,
    ):
        """Internal context manager — same as using() but without yielding self."""
        with (
            self.user_ctx.ctx(user) if user is not UNSET else suppress(),
            self.now_ctx.ctx(now) if now is not UNSET else suppress(),
            self.id_ctx.ctx(resource_id) if resource_id is not UNSET else suppress(),
            self.apply_scope_ctx.ctx(True) if apply_access_scope else suppress(),
        ):
            yield

    def _validate_write_context(self, method_name: str | None = None) -> None:
        """Validate that required context fields are available for write ops."""
        from specstar.types import MissingOperationContextError

        missing: list[str] = []
        if self.user_or_unset is UNSET:
            missing.append("user")
        if self.now_or_unset is UNSET:
            missing.append("now")
        if missing:
            raise MissingOperationContextError(missing, method_name)

    def _res_meta(
        self,
        mode: _BuildResMetaCreate | _BuildResMetaUpdate | _BuildResMetaModify,
    ) -> ResourceMeta:
        if isinstance(mode, _BuildResMetaCreate):
            current_revision_id = mode.rev_info.revision_id
            resource_id = mode.rev_info.resource_id
            total_revision_count = 1
            created_time = self.now_ctx.get()
            created_by = self.user_ctx.get()
            indexed_data = self._extract_indexed_values(mode.data)
        elif isinstance(mode, _BuildResMetaUpdate):
            current_revision_id = mode.rev_info.revision_id
            resource_id = mode.prev_res_meta.resource_id
            total_revision_count = mode.prev_res_meta.total_revision_count + 1
            created_time = mode.prev_res_meta.created_time
            created_by = mode.prev_res_meta.created_by
            indexed_data = self._extract_indexed_values(mode.data)
        elif isinstance(mode, _BuildResMetaModify):
            current_revision_id = mode.rev_info.revision_id
            resource_id = mode.prev_res_meta.resource_id
            total_revision_count = mode.prev_res_meta.total_revision_count
            created_time = mode.prev_res_meta.created_time
            created_by = mode.prev_res_meta.created_by
            if mode.data is UNSET:
                indexed_data = mode.prev_res_meta.indexed_data
            else:
                indexed_data = self._extract_indexed_values(mode.data)

        # rev_* fields mirror the now-current ``RevisionInfo`` so that
        # search/sort by *current revision* attributes does not require an
        # extra read against ``IResourceStore``.
        rev_info = mode.rev_info
        return ResourceMeta(
            current_revision_id=current_revision_id,
            resource_id=resource_id,
            schema_version=self._schema_version,
            total_revision_count=total_revision_count,
            created_time=created_time,
            updated_time=self.now_ctx.get(),
            created_by=created_by,
            updated_by=self.user_ctx.get(),
            indexed_data=indexed_data,
            rev_status=rev_info.status,
            rev_created_by=rev_info.created_by,
            rev_updated_by=rev_info.updated_by,
            rev_created_time=rev_info.created_time,
            rev_updated_time=rev_info.updated_time,
        )

    def get_data_hash(self, data: T) -> str:
        b = self.encode(data)
        self._decode_and_validate(b)  # 確保可解碼
        self._run_validator(data)  # 執行自訂驗證
        data_hash = f"xxh3_128:{xxh3_128_hexdigest(b)}"
        return data_hash

    def _process_binary_fields(self, data: Any) -> Any:
        result = self._binary_processor.process(data, self.blob_store)
        store = self.blob_store
        if store is not None:
            # Phase-2 ref accounting (issue #370): once per (revision, file_id).
            # Best-effort — a hiccup here must never break the write; the
            # reconcile pass is the authoritative source of truth.
            for file_id in self._binary_processor.collect_file_ids(result):
                try:
                    store.restore_from_quarantine(file_id)  # resurrection
                    store.incref(file_id)
                    store.touch(file_id)
                except Exception:
                    pass
        return result

    def restore_binary(self, data: T) -> T:
        """
        還原 data 中的 binary.data (如果是從 blob store 讀取).
        這對於需要讀取 Binary 原始資料時很有用.
        """
        return self._binary_processor.restore(data, self.blob_store)

    def _rev_info(
        self,
        mode: _BuildRevInfoCreate | _BuildRevInfoUpdate | _BuildRevInfoModify,
    ) -> RevisionInfo:
        uid = uuid4()
        if isinstance(mode, _BuildRevInfoCreate):
            _id = self.id_ctx.get()
            if _id is UNSET:
                resource_id = self.id_generator()
            else:
                _validate_resource_id(_id)  # caller-supplied id must be path/URL-safe
                resource_id = _id
            revision_id = f"{resource_id}:1"
            last_revision_id = None
            created_time = self.now_ctx.get()
            created_by = self.user_ctx.get()
            status = mode.status
            data_hash = self.get_data_hash(mode.data)

        elif isinstance(mode, _BuildRevInfoUpdate):
            prev_res_meta = mode.prev_res_meta
            resource_id = prev_res_meta.resource_id
            revision_id = f"{resource_id}:{prev_res_meta.total_revision_count + 1}"
            last_revision_id = prev_res_meta.current_revision_id
            created_time = self.now_ctx.get()
            created_by = self.user_ctx.get()
            status = mode.status
            data_hash = self.get_data_hash(mode.data)

        elif isinstance(mode, _BuildRevInfoModify):
            prev_info = mode.prev_info
            prev_res_meta = mode.prev_res_meta
            resource_id = prev_res_meta.resource_id
            revision_id = prev_res_meta.current_revision_id
            created_time = prev_info.created_time
            last_revision_id = prev_info.parent_revision_id
            created_by = prev_info.created_by
            status = mode.status
            if mode.status is UNSET:
                status = prev_info.status
            else:
                status = mode.status
            if mode.data is UNSET:
                data_hash = prev_info.data_hash
            else:
                data_hash = self.get_data_hash(mode.data)

        info = RevisionInfo(
            uid=uid,
            resource_id=resource_id,
            revision_id=revision_id,
            parent_revision_id=last_revision_id,
            schema_version=self._schema_version,
            data_hash=data_hash,
            status=status,
            created_time=created_time,
            updated_time=self.now_ctx.get(),
            created_by=created_by,
            updated_by=self.user_ctx.get(),
        )
        return info

    def _handle_event(
        self, context: EventContext, *, check_permission: bool = True
    ) -> None:
        # ``check_permission=False`` for a nested op's before-context (#402):
        # the outermost op already authorized this request, so suppress the
        # permission checker while keeping every other handler (audit,
        # constraints, on_success, …) running. Non-before contexts never trigger
        # the permission checker (it is before-phase only), so the flag is a
        # no-op for them and defaults to True for all other callers.
        for eh in self.event_handlers:
            if not check_permission and isinstance(eh, PermissionEventHandler):
                continue
            if eh.is_supported(context):
                eh.handle_event(context)

    def _get_meta_no_check_is_deleted(self, resource_id: str) -> ResourceMeta:
        # Read access-scope gate: a request-scoped read of a resource outside
        # the caller's scope is hidden as a 404. No-op for internal reads.
        # This is the single choke point every single-resource read route leads
        # with (get_meta, or get()->get_meta), so all GET variants inherit it.
        self._assert_in_scope(resource_id)
        # Read straight from the store and treat a missing key as not-found.
        # Going through get_meta (rather than an exists()-then-read pair) closes
        # the TOCTOU window where a concurrent purge / interrupted create can
        # delete the meta between the check and the read — that must surface as
        # the typed ResourceIDNotFoundError, never a raw OSError/KeyError.
        try:
            return self.storage.get_meta(resource_id)
        except KeyError:
            raise ResourceIDNotFoundError(resource_id) from None

    def _required_resource_parts(
        self, action: ResourceAction
    ) -> frozenset[ResourcePart]:
        """Union of resource slices every registered permission checker needs
        loaded into ``current_resource`` for a before-phase **write** check of
        ``action``. Empty when no checker opts in — keeping writes free of any
        extra read for models without resource-aware ACLs.
        """
        parts: frozenset[ResourcePart] = frozenset()
        for eh in self.event_handlers:
            if isinstance(eh, PermissionEventHandler):
                parts |= eh.permission_checker.required_resource_parts(action)
        return parts

    def _load_current_resource(
        self, resource_id: str, parts: frozenset[ResourcePart]
    ) -> SearchedResource:
        """Assemble a ``SearchedResource`` carrying only ``parts`` of the
        current (stored) resource.

        Uses NON-event reads (``_get_meta_no_check_is_deleted`` + raw storage)
        so it never fires a nested before-event / permission check — going
        through the event-emitting ``get`` / ``get_meta`` would recurse. A
        missing resource raises ``ResourceIDNotFoundError`` here, i.e. before
        the write permission verdict, which is the intended behaviour.
        """
        current: SearchedResource = SearchedResource()
        meta = self._get_meta_no_check_is_deleted(resource_id)
        if ResourcePart.META in parts:
            current.meta = meta
        if ResourcePart.INFO in parts:
            current.info = self.storage.get_resource_revision_info(
                resource_id, meta.current_revision_id, meta.schema_version
            )
        if ResourcePart.DATA in parts:
            with self.storage.get_data_bytes(
                resource_id, meta.current_revision_id, meta.schema_version
            ) as fh:
                current.data = self._data_serializer.decode(fh.read())
        return current

    def _authorize_and_announce(self, before_ctx: Any, stack: ExitStack) -> None:
        """Authorize one operation and fire its before-context.

        This is *the* permission gate, in one place. ``execute_with_events``
        calls it for the operation it wraps; a bulk operation that has to
        authorize row by row (:meth:`patch_many`) calls it once per row. It
        stays a single function deliberately — a second copy of this sequence
        would drift from the decorator, and what would drift is the
        authorization path, which fails silently and open.

        *stack* must stay open for the rest of the operation: it holds the
        re-entrancy guard and the unscoped window opened by the write-scope
        probe.

        Permission re-entrancy guard (#402): authorize once, at the outermost
        operation. The first event-emitting op in this call stack arms the flag
        for its whole extent; any nested op (a patch's internal get/update, a
        get's internal get_meta, …) observes it and skips ONLY its permission
        check — every other event handler still fires.

        Read access-scope (#398) is a precondition for request-originated
        writes: a resource outside the caller's scope is hidden as not-found
        (→ 404) *before* the permission checker (→ 403) and before any
        current_resource load, so writes never leak existence. The remainder
        then runs unscoped — the resource is confirmed in-scope, and the
        operation's own (and nested get/get_meta) reads must not re-probe.
        """
        check_permission = not self._perm_checked_ctx.get()
        if check_permission:
            stack.enter_context(self._perm_checked_ctx.ctx(True))
        if self._maybe_assert_write_scope(before_ctx):
            stack.enter_context(self.apply_scope_ctx.ctx(False))
        # current_resource is loaded solely for the permission check, so skip
        # the extra storage read when this nested op won't check.
        if check_permission:
            self._maybe_load_current_resource(before_ctx)
        self._handle_event(before_ctx, check_permission=check_permission)

    def _maybe_load_current_resource(self, before_ctx: Any) -> None:
        """Populate ``before_ctx.current_resource`` for a write before-context
        when a permission checker declares it needs it.

        No-op for every non-write context (only the update/modify/patch/delete
        ``Before*`` structs carry the field — presence is the gate) and when no
        registered checker opts in, so the overhead is paid only where an
        actual resource-aware write ACL is wired.
        """
        if not hasattr(before_ctx, "current_resource"):
            return
        parts = self._required_resource_parts(before_ctx.action)
        if not parts:
            return
        before_ctx.current_resource = self._load_current_resource(
            before_ctx.resource_id, parts
        )

    def _maybe_assert_write_scope(self, before_ctx: Any) -> bool:
        """Enforce the read access-scope as a precondition for a
        request-originated write: a resource outside the caller's scope raises
        not-found (→ 404) *before* the permission checker runs, so writes never
        leak existence (uniformly with reads).

        Returns ``True`` when this is a scoped write that was probed — the
        caller then runs the remainder of the operation unscoped (the resource
        is confirmed in-scope; its own and nested reads must not re-probe).
        Returns ``False`` (a no-op) for internal/unscoped writes — every
        ``ResourceManager`` write that isn't entered via
        ``using(..., apply_access_scope=True)`` keeps its full pre-#398
        behaviour, paying nothing.
        """
        if not self.apply_scope_ctx.get():
            return False
        if getattr(before_ctx, "action", None) not in _SCOPED_WRITE_ACTIONS:
            return False
        rid = getattr(before_ctx, "resource_id", UNSET)
        if rid is UNSET or rid is None:
            return False
        self._assert_in_scope(rid)
        return True

    def exists(self, resource_id: str) -> bool:
        """
        Check if a resource exists.

        Arguments:
            resource_id (str): The ID of the resource to check.

        Returns:
            bool: True if the resource exists, False otherwise.
        """
        return self.storage.exists(resource_id)

    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        """
        Check if a specific revision of a resource exists.

        Arguments:
            resource_id (str): The ID of the resource.
            revision_id (str): The revision ID to check.

        Returns:
            bool: True if the revision exists, False otherwise.
        """
        return self.storage.revision_exists(resource_id, revision_id)

    def collect_orphans(self) -> int:
        """Reclaim storage left behind by interrupted ``create()`` calls.

        A ``create()`` writes revision data before the meta commit marker, so a
        process killed mid-write leaves durable resource/blob data with no
        finalised meta. Such data is already invisible to reads and listings
        (the meta store is the source of truth); this call physically removes
        it to reclaim disk space.

        Safe to run at boot. Returns the number of orphaned directories
        removed (``0`` for backends without reclaimable on-disk garbage).
        """
        collect = getattr(self.storage, "collect_orphans", None)
        removed = collect() if collect is not None else 0
        if removed:
            logger.info("collect_orphans: reclaimed %d orphaned director(ies)", removed)
        return removed

    @execute_with_events(
        (
            BeforeGetMeta,
            AfterGetMeta,
            OnSuccessGetMeta,
            OnFailureGetMeta,
        ),
        "meta",
        inputs={"include_deleted": UNSET},
    )
    def get_meta(self, resource_id: str, include_deleted: bool = False) -> ResourceMeta:
        """
        Get the metadata of a resource.

        Arguments:
            resource_id (str): The ID of the resource.
            include_deleted (bool): If True, return metadata even for
                soft-deleted resources. Defaults to False.

        Returns:
            meta (ResourceMeta): The metadata object.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            ResourceIsDeletedError: If the resource has been soft-deleted
                and *include_deleted* is False.
        """
        meta = self._get_meta_no_check_is_deleted(resource_id)
        if meta.is_deleted and not include_deleted:
            raise ResourceIsDeletedError(resource_id)
        return meta

    def get_blob(self, file_id: str) -> Binary:
        if self.blob_store is None:
            raise NotImplementedError("Blob store is not configured")
        return self.blob_store.get(file_id)

    def get_blob_url(self, file_id: str) -> str | None:
        if self.blob_store is None:
            raise NotImplementedError("Blob store is not configured")
        return self.blob_store.get_url(file_id)

    def get_blob_stream(self, file_id: str):
        """Return a streaming iterator for blob content, or ``None``."""
        if self.blob_store is None:
            raise NotImplementedError("Blob store is not configured")
        get_stream = getattr(self.blob_store, "get_stream", None)
        if get_stream is None:
            return None
        return get_stream(file_id)

    def get_blob_response(self, file_id: str):
        """Return the blob store's preferred download response."""
        from specstar.types import BlobResponse

        if self.blob_store is None:
            raise NotImplementedError("Blob store is not configured")
        response = self.blob_store.get_response(file_id)
        if response is not None:
            return response
        # Fallback for blob stores that don't implement get_response:
        # use the RM's own get_blob (which may be overridden in subclasses)
        blob = self.get_blob(file_id)
        return BlobResponse("data", blob=blob)

    def start_consume(
        self,
        *,
        block: bool = True,
        custom_creation: "Literal['all'] | list[str] | None" = None,
        custom_update: "Literal['all'] | list[str] | None" = None,
    ) -> "threading.Thread | None":
        """Start consuming jobs from the message queue.

        When both *custom_creation* and *custom_update* are ``None``
        (default) the resource’s own message-queue consumer is started.

        When *custom_creation* is ``"all"``, all async-create-job consumers
        that target this resource are started (but **not** this resource’s
        own consumer).

        When *custom_creation* is a list of job resource names, only those
        specific create-job consumers are started.

        When *custom_update* is ``"all"``, all async-update-job consumers
        that target this resource are started.

        When *custom_update* is a list of job resource names, only those
        specific update-job consumers are started.

        Both *custom_creation* and *custom_update* can be used together
        in the same call.

        Args:
            block: If ``True`` (default), block until the consumer thread(s)
                finish.  ``False`` returns immediately after launching the
                daemon thread(s).
            custom_creation: Which async-create-job consumers to start.
                ``None`` → ignored (no create-job consumers started).
                ``"all"`` → every registered async create-job consumer.
                ``["name", ...]`` → only the listed job resource names.
            custom_update: Which async-update-job consumers to start.
                ``None`` → ignored (no update-job consumers started).
                ``"all"`` → every registered async update-job consumer.
                ``["name", ...]`` → only the listed job resource names.

        Raises:
            NotImplementedError: If both *custom_creation* and *custom_update*
                are ``None`` and no message queue is configured on this
                resource.
            ValueError: If a name in *custom_creation* or *custom_update*
                is not a registered async job for this resource.
        """
        if custom_creation is None and custom_update is None:
            # Original behaviour: start this RM’s own MQ consumer.
            if self.message_queue is None:
                raise NotImplementedError("Message queue is not configured")
            worker_thread = threading.Thread(
                target=self.message_queue.start_consume, daemon=True
            )
            worker_thread.start()
            if block:
                worker_thread.join()
            return worker_thread

        threads = []

        # --- custom_creation mode ---
        if custom_creation is not None:
            if custom_creation == "all":
                create_targets = list(self._async_create_job_rms.values())
            else:
                create_targets = []
                for name in custom_creation:
                    job_rm = self._async_create_job_rms.get(name)
                    if job_rm is None:
                        raise ValueError(
                            f"'{name}' is not a registered async create-job "
                            f"for resource '{self.resource_name}'. "
                            f"Available: {list(self._async_create_job_rms.keys())}"
                        )
                    create_targets.append(job_rm)

            for job_rm in create_targets:
                t = job_rm.start_consume(block=False)
                threads.append(t)

        # --- custom_update mode ---
        if custom_update is not None:
            if custom_update == "all":
                update_targets = list(self._async_update_job_rms.values())
            else:
                update_targets = []
                for name in custom_update:
                    job_rm = self._async_update_job_rms.get(name)
                    if job_rm is None:
                        raise ValueError(
                            f"'{name}' is not a registered async update-job "
                            f"for resource '{self.resource_name}'. "
                            f"Available: {list(self._async_update_job_rms.keys())}"
                        )
                    update_targets.append(job_rm)

            for job_rm in update_targets:
                t = job_rm.start_consume(block=False)
                threads.append(t)

        if block:
            for t in threads:
                t.join()

    def _queryable_field_names(self) -> set[str]:
        """Field paths a search condition can actually match against: the
        ``ResourceMeta`` attributes plus the configured indexed keys (an
        indexed field is queried under ``index_key or field_path``)."""
        names = set(ResourceMeta.__struct_fields__)
        names.discard("indexed_data")
        for f in self._indexed_fields:
            names.add(f.index_key or f.field_path)
        return names

    def _collect_condition_fields(self, query: ResourceMetaSearchQuery) -> list[str]:
        """Field paths referenced by scalar data conditions, recursing through
        groups. Vector conditions have their own validation, so they are
        skipped here."""
        from specstar.query_types import DataSearchCondition, DataSearchGroup

        found: list[str] = []

        def walk(cond: object) -> None:
            if isinstance(cond, DataSearchCondition):
                found.append(cond.field_path)
            elif isinstance(cond, DataSearchGroup):
                for sub in cond.conditions:
                    walk(sub)

        for bucket in (query.conditions, query.data_conditions):
            if bucket is not UNSET:
                for cond in bucket:
                    walk(cond)
        return found

    def _validate_query_fields(self, query: ResourceMetaSearchQuery) -> None:
        """Surface conditions that filter on a field which is neither indexed
        nor a ``ResourceMeta`` attribute — such a condition matches nothing, so
        the query silently under-returns. Behaviour set by ``on_unindexed_query``
        (``warn`` by default, ``error`` to raise)."""
        refs = self._collect_condition_fields(query)
        if not refs:
            return
        valid = self._queryable_field_names()
        seen: set[str] = set()
        missing: list[str] = []
        for f in refs:
            if f not in valid and f not in seen:
                seen.add(f)
                missing.append(f)
        if not missing:
            return
        indexed = sorted(f.index_key or f.field_path for f in self._indexed_fields)
        if self._on_unindexed_query == OnUnindexedQuery.error:
            raise UnindexedQueryError(missing, indexed)
        warnings.warn(
            f"Search on {self._resource_name!r} filters on non-indexed "
            f"field(s) {missing!r}; these conditions match nothing, so the "
            f"query will under-return. Indexed fields: {indexed!r}. Add them "
            f"via indexed_fields=/add_indexed_field(), or filter on a "
            f"ResourceMeta attribute.",
            SpecStarWarning,
            stacklevel=3,
        )

    def _apply_default_is_deleted(
        self, query: ResourceMetaSearchQuery
    ) -> ResourceMetaSearchQuery:
        """Inject the configured ``default_is_deleted`` when the query didn't
        specify one. ``None`` leaves it UNSET (include both live and
        soft-deleted — the default); an explicit ``is_deleted`` always wins."""
        if self._default_is_deleted is not None and query.is_deleted is UNSET:
            return msgspec.structs.replace(query, is_deleted=self._default_is_deleted)
        return query

    # ── Read access-scope (issue #398, part A) ───────────────────────────
    def _resolve_active_scope(
        self,
    ) -> "ConditionBuilder | None | _Unrestricted | UnsetType":
        """Resolve the read access-scope decision for the *current* read.

        Returns ``UNSET`` when scoping is inactive — either no ``access_scope``
        is registered, or this read did not opt in. Only the generated
        read-route templates enter ``using(..., apply_access_scope=True)``;
        every internal ``ResourceManager`` read stays unscoped. Otherwise
        returns the predicate's result: a :class:`ConditionBuilder` (restrict),
        ``None`` (deny all, fail-closed), or :data:`UNRESTRICTED` (no limit)."""
        if self._access_scope is None or not self.apply_scope_ctx.get():
            return UNSET
        return self._access_scope(self.user)

    def _validate_scope_fields(self, scope_query: ResourceMetaSearchQuery) -> None:
        """Strictly reject an access-scope predicate that filters on a field
        which is neither a ``ResourceMeta`` attribute nor a configured indexed
        field. Unlike ``_validate_query_fields`` (whose leniency is governed by
        ``on_unindexed_query``), a scope predicate must **never** silently
        degrade — a dropped/unmatched condition would widen visibility, a
        security hole. Always raises on an unknown field."""
        refs = self._collect_condition_fields(scope_query)
        if not refs:
            return
        valid = self._queryable_field_names()
        missing = [f for f in dict.fromkeys(refs) if f not in valid]
        if missing:
            indexed = sorted(f.index_key or f.field_path for f in self._indexed_fields)
            raise UnindexedQueryError(missing, indexed)

    def _and_scope_into(
        self, query: ResourceMetaSearchQuery, scope: "ConditionBuilder"
    ) -> ResourceMetaSearchQuery:
        """AND a scope predicate into ``query.conditions`` (list entries are
        ANDed), after strictly validating the scope's fields."""
        scope_query = scope.build()
        if scope_query.conditions is UNSET or not scope_query.conditions:
            return query  # an empty predicate carries no restriction
        self._validate_scope_fields(scope_query)
        existing = list(query.conditions) if query.conditions is not UNSET else []
        return msgspec.structs.replace(
            query, conditions=existing + list(scope_query.conditions)
        )

    @contextmanager
    def _scoped_read(self, query: ResourceMetaSearchQuery):
        """For a request-scoped list/count, yield ``(query, deny)`` and clear
        the scope flag for the duration so nested/downstream reads (events,
        per-row fetches) run unscoped — scope is applied exactly once, at the
        outermost read. ``deny`` True → caller returns the empty result without
        touching storage (the ``None`` deny-all, fail-closed case)."""
        scope = self._resolve_active_scope()
        if scope is UNSET or scope is UNRESTRICTED:
            yield query, False
            return
        with self.apply_scope_ctx.ctx(False):
            if scope is None:
                yield query, True
            else:
                yield self._and_scope_into(query, scope), False

    def _assert_in_scope(self, resource_id: str) -> None:
        """Enforce the read access-scope for a single-resource read: a resource
        outside the caller's scope is treated as non-existent
        (``ResourceIDNotFoundError`` → HTTP 404), hiding its existence
        uniformly with list/search. No-op for unscoped (internal) reads."""
        scope = self._resolve_active_scope()
        if scope is UNSET or scope is UNRESTRICTED:
            return
        with self.apply_scope_ctx.ctx(False):
            if scope is None:
                raise ResourceIDNotFoundError(resource_id)
            probe = self._and_scope_into(
                ResourceMetaSearchQuery(),
                scope & QB["resource_id"].eq(resource_id),
            )
            probe = msgspec.structs.replace(probe, limit=1)
            if self.storage.count(probe) == 0:
                raise ResourceIDNotFoundError(resource_id)

    def count_resources(
        self, query: "ResourceMetaSearchQuery | Query | None" = None
    ) -> int:
        """
        Count the number of resources matching the query.

        Arguments:
            query (ResourceMetaSearchQuery | Query | None): The search query
                object or Query builder. ``None`` (the default) counts all
                resources — equivalent to passing ``QB.all()``.

        Returns:
            count (int): The number of matching resources.
        """
        if query is None:
            query = ResourceMetaSearchQuery()
        elif isinstance(query, Query):
            query = query.build()
        with self._scoped_read(query) as (query, deny):
            if deny:
                return 0
            query = self._apply_default_is_deleted(query)
            self._validate_query_fields(query)
            query = self._resolve_str_query_vectors(query)
            return self.storage.count(query)

    def _resolve_str_query_vectors(
        self, query: ResourceMetaSearchQuery
    ) -> ResourceMetaSearchQuery:
        """Convert any ``str`` ``query_vector`` in conditions/sorts to a
        ``list[float]`` by invoking the field's registered encoder.

        Sync only: raises if a required encoder is async.
        """
        import inspect

        import msgspec

        from specstar.query_types import (
            DataSearchGroup as _DSG,
        )
        from specstar.query_types import (
            VectorDistanceCondition as _VC,
        )
        from specstar.query_types import (
            VectorDistanceSort as _VS,
        )
        from specstar.resource_manager.encoder_registry import lookup_encoder
        from specstar.types import extract_vector_field_infos

        # Build per-field encoder cache once
        infos = {i.name: i for i in extract_vector_field_infos(self._resource_type)}

        # Embedding fields' field_path in query is the parent name (e.g. "summary"),
        # which corresponds to info.name; raw vector fields map directly too.

        def _encode(field_path: str, text: str) -> list[float]:
            top = field_path.split(".")[0]
            info = infos.get(top)
            if info is None:
                raise ValueError(
                    f"Vector field {field_path!r} not registered on "
                    f"{self._resource_type.__name__}; cannot encode str query_vector."
                )
            encoder = lookup_encoder(
                self._encoder_registry,
                field_info=info,
                model_overrides=self._vector_encoders,
            )
            if encoder is None:
                raise ValueError(
                    f"No encoder configured for field {field_path!r}; "
                    f"cannot encode str query_vector."
                )
            vec = encoder(text)
            if inspect.isawaitable(vec):
                # Close the unawaited coroutine to avoid a RuntimeWarning
                try:
                    vec.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
                raise RuntimeError(
                    f"Encoder for field {field_path!r} is async but search was "
                    f"invoked synchronously. Register a sync encoder or use the "
                    f"async query path."
                )
            return vec

        def _maybe_resolve_cond(c):
            if isinstance(c, _VC) and isinstance(c.query_vector, str):
                return msgspec.structs.replace(
                    c, query_vector=_encode(c.field_path, c.query_vector)
                )
            if isinstance(c, _DSG):
                new_sub = [_maybe_resolve_cond(sub) for sub in c.conditions]
                if any(a is not b for a, b in zip(new_sub, c.conditions)):
                    return msgspec.structs.replace(c, conditions=new_sub)
            return c

        def _maybe_resolve_sort(s):
            if isinstance(s, _VS) and isinstance(s.query_vector, str):
                return msgspec.structs.replace(
                    s, query_vector=_encode(s.field_path, s.query_vector)
                )
            return s

        changes: dict = {}
        if query.conditions is not UNSET:
            new_conds = [_maybe_resolve_cond(c) for c in query.conditions]
            if any(a is not b for a, b in zip(new_conds, query.conditions)):
                changes["conditions"] = new_conds
        if query.sorts is not UNSET:
            new_sorts = [_maybe_resolve_sort(s) for s in query.sorts]
            if any(a is not b for a, b in zip(new_sorts, query.sorts)):
                changes["sorts"] = new_sorts
        if not changes:
            return query
        return msgspec.structs.replace(query, **changes)

    def search_resources(
        self, query: "ResourceMetaSearchQuery | Query | None" = None
    ) -> list[ResourceMeta]:
        """
        Search resources based on the provided query.

        Arguments:
            query (ResourceMetaSearchQuery | Query | None): The search query
                object or Query builder. ``None`` (the default) lists all
                resources — equivalent to passing ``QB.all()``.

        Returns:
            results (list[ResourceMeta]): A list of ResourceMeta objects matching the query.
        """
        # Normalise "no query" → match-all *before* the event-emitting method,
        # so the SearchResources event contexts always carry a real query
        # (matching how list_resources / iter_all already work).
        if query is None:
            query = ResourceMetaSearchQuery()
        elif isinstance(query, Query):
            query = query.build()
        with self._scoped_read(query) as (query, deny):
            if deny:
                return []
            return self._search_resources(query)

    @execute_with_events(
        (
            BeforeSearchResources,
            AfterSearchResources,
            OnSuccessSearchResources,
            OnFailureSearchResources,
        ),
        "results",
    )
    def _search_resources(
        self, query: ResourceMetaSearchQuery | Query
    ) -> list[ResourceMeta]:
        if isinstance(query, Query):
            query = query.build()
        query = self._apply_default_is_deleted(query)
        self._validate_query_fields(query)
        query = self._resolve_str_query_vectors(query)
        return self.storage.search(query)

    def iter_all(
        self,
        query: "ResourceMetaSearchQuery | Query | None" = None,
        *,
        batch_size: int = 1000,
    ) -> Generator[ResourceMeta]:
        """Yield *every* resource matching ``query``, paging internally.

        Unlike :meth:`search_resources` — which is bounded by the query's
        ``limit`` and can silently truncate — this walks the full result set
        in ``batch_size`` chunks. Use it whenever you genuinely want "all
        rows" so a forgotten ``limit`` can't drop data. ``query`` defaults to
        "all resources".

        Arguments:
            query: search query (or ``Query`` builder); ``None`` matches all.
            batch_size: page size used internally for the scan.

        Yields:
            ResourceMeta: one per matching resource, oldest page first.
        """
        if query is None:
            query = ResourceMetaSearchQuery()
        elif isinstance(query, Query):
            query = query.build()
        offset = 0
        while True:
            page = self.search_resources(
                msgspec.structs.replace(query, limit=batch_size, offset=offset)
            )
            if not page:
                return
            yield from page
            if len(page) < batch_size:
                return
            offset += batch_size

    def _default_worker_num(self, nr_work: int) -> int:
        """Calculate the number of worker threads for parallel fetch."""
        if nr_work <= 10:
            return 1
        return max(1, min(16, nr_work // 3))

    def _undecodable_data(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: "str | None | UnsetType",
        error: str,
    ) -> UndecodableData:
        """Build an :class:`UndecodableData` for a row whose data can't be
        decoded into the model: best-effort parsed dict, else raw bytes
        (base64). Used by the ``raw`` decode-error policy."""
        raw = b""
        try:
            with self.storage.get_data_bytes(
                resource_id, revision_id, schema_version
            ) as fh:
                raw = fh.read()
        except Exception:
            pass
        try:
            parsed = self._data_serializer.decode_to_builtins(raw)
            if isinstance(parsed, dict):
                return UndecodableData(decode_error=error, data=parsed)
        except Exception:
            pass
        return UndecodableData(
            decode_error=error,
            raw_base64=base64.b64encode(raw).decode("ascii"),
        )

    def list_resources(
        self,
        query: "ResourceMetaSearchQuery | Query | None" = None,
        *,
        returns: list[str] | None = None,
        partial: list[str] | None = None,
    ) -> list[SearchedResource[T]]:
        """Search for resources and fetch their data in one call.

        Internally calls ``search_resources(query)`` (which triggers
        Before/After/OnSuccess/OnFailure SearchResources events), then
        fetches the requested sections for each matching resource.

        Arguments:
            query (ResourceMetaSearchQuery | Query): The search query.
            returns: sections to include per item.  Allowed values are
                ``"data"``, ``"info"``, ``"meta"``.  ``None`` means all three.
            partial: optional list of field paths to retrieve.  Paths may
                be prefixed with ``data/``, ``meta/``, or ``info/`` to
                target a specific section; unprefixed paths default to
                ``"data"``.

        Returns:
            resources (list[SearchedResource[T]]): one item per matched resource.
        """
        # 1. Search — triggers SearchResources events. ``None`` lists all.
        if query is None:
            query = ResourceMetaSearchQuery()
        metas = self.search_resources(query)
        if not metas:
            return []

        # 2. Normalise returns
        if returns is None:
            returns = ["data", "info", "meta"]

        # 3. Classify partial fields
        spec = classify_partial_fields(partial, default_category="data")

        # 3b. Fetch the whole page's rows in one go. Per-item reads made a
        # listing cost grow with its length — the same query repeated once per
        # row, each on its own pooled connection. Only the full-data path
        # qualifies: a partial read projects columns per item, and a listing
        # that wants no data has nothing to prefetch. Backends without a bulk
        # read fall back to the same per-row loop inside `get_revision_bundles`,
        # so this is a speed-up where it exists and a no-op where it does not.
        prefetched: dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]] = {}
        if "data" in returns and not spec.data_fields:
            prefetched = self.storage.get_revision_bundles(
                [
                    (m.resource_id, m.current_revision_id, m.schema_version)
                    for m in metas
                ]
            )

        # 4. Define per-item fetch function
        def _fetch_one(meta: ResourceMeta) -> SearchedResource[T] | None:
            try:
                data = UNSET
                info = UNSET

                if "data" in returns:
                    if spec.data_fields:
                        data = self.get_partial(
                            meta.resource_id,
                            meta.current_revision_id,
                            spec.data_fields,
                            schema_version=meta.schema_version,
                        )
                    else:
                        # low-level read (no policy) — list applies the
                        # on_decode_error policy itself in the except below.
                        pre = prefetched.get(
                            (
                                meta.resource_id,
                                meta.current_revision_id,
                                meta.schema_version,
                            )
                        )
                        resource = (
                            self._resource_from_bundle(*pre)
                            if pre is not None
                            else self.get_resource_revision(
                                meta.resource_id,
                                meta.current_revision_id,
                                schema_version=meta.schema_version,
                            )
                        )
                        data = resource.data
                        if "info" in returns:
                            info = resource.info

                if "info" in returns and info is UNSET:
                    info = self.get_revision_info(
                        meta.resource_id,
                        meta.current_revision_id,
                        schema_version=meta.schema_version,
                    )

                if "meta" in returns:
                    meta_out = meta
                else:
                    meta_out = UNSET

                # Apply partial filtering on meta and info
                if spec.meta_fields and meta_out is not UNSET:
                    meta_out = filter_struct_partial(meta_out, spec.meta_fields)
                if spec.info_fields and info is not UNSET:
                    info = filter_struct_partial(info, spec.info_fields)

                return SearchedResource(data=data, info=info, meta=meta_out)
            except Exception as e:
                if self._on_decode_error == OnDecodeError.error:
                    raise ResourceDecodeError(meta.resource_id, str(e)) from e
                if self._on_decode_error == OnDecodeError.raw:
                    info_out: Any = UNSET
                    if "info" in returns:
                        try:
                            info_out = self.get_revision_info(
                                meta.resource_id,
                                meta.current_revision_id,
                                schema_version=meta.schema_version,
                            )
                        except Exception:
                            info_out = UNSET
                    return SearchedResource(
                        data=self._undecodable_data(
                            meta.resource_id,
                            meta.current_revision_id,
                            meta.schema_version,
                            str(e),
                        )
                        if "data" in returns
                        else UNSET,
                        info=info_out,
                        meta=meta if "meta" in returns else UNSET,
                    )
                return None  # skip

        # 5. Execute — single-threaded or parallel
        worker_num = self._default_worker_num(len(metas))
        results: list[SearchedResource[T]] = []
        skipped_ids: list[str] = []

        # The page rows were already scoped by ``search_resources`` above, so
        # the per-row data/info fetches must NOT re-apply the access scope —
        # clear the flag for the fetch phase to keep it from re-probing each row
        # (a single-resource re-check per row = N+1). The parallel branch runs
        # in worker threads where ContextVars don't propagate anyway; this makes
        # the single-threaded branch consistent with it.
        with self.apply_scope_ctx.ctx(False):
            if worker_num <= 1:
                for meta in metas:
                    item = _fetch_one(meta)
                    if item is not None:
                        results.append(item)
                    else:
                        skipped_ids.append(meta.resource_id)
            else:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=worker_num,
                ) as executor:
                    futures = {
                        executor.submit(_fetch_one, meta): meta for meta in metas
                    }
                    for future in futures:
                        item = future.result()
                        if item is not None:
                            results.append(item)
                        else:
                            skipped_ids.append(futures[future].resource_id)

        # A matched resource whose data can't be fetched/decoded is skipped here
        # (defensive — one bad row shouldn't 500 the whole page), but ``count``
        # still counts it, so the two endpoints diverge. Make that divergence
        # non-silent: most often it means an incompatible schema change at the
        # same version. See the schema-migration guide.
        if skipped_ids:
            logger.warning(
                "%s.list: skipped %d undecodable resource(s) that /count still "
                "counts (e.g. %s). This usually means an incompatible schema "
                "change without a version bump; migrate or bump the schema "
                "version. See docs/en/quickstart/schema-migration.md.",
                self.resource_name,
                len(skipped_ids),
                skipped_ids[:5],
            )

        return results

    def exp_aggregate_by(
        self,
        by: "object",
        aggregates: "dict[str, object]",
        query: "ResourceMetaSearchQuery | Query | None" = None,
        *,
        order_by: "str | None" = None,
        limit: "int | None" = None,
        offset: int = 0,
    ) -> "list[object]":
        """Group rows by ``by`` and reduce with the named ``aggregates``.

        **Experimental** (``exp_`` prefix) — the API may still adjust before
        stabilising as ``aggregate_by``. Handles both single-RM group-by and
        cross-RM annotation in one method.

        **Push-down:** a single-key, ``Count``-only group-by is pushed to the
        store's engine when the meta store implements
        :class:`~specstar.resource_manager.basic.IMetaWithAgg` (SQLite today) —
        a real ``GROUP BY`` instead of streaming every matching row into
        Python. Results are identical to the in-process path (enforced
        cross-backend); it's scan-fast, not O(1) — for a genuinely hot,
        high-fan-out count prefer a denormalised counter maintained on write.
        Sum/Min/Max/Avg and ForeignAggregate still reduce in Python.

        Self aggregates (over this RM's rows in each group):
        :class:`~specstar.aggregates.Count` and
        :class:`~specstar.aggregates.Sum` / :class:`~specstar.aggregates.Min`
        / :class:`~specstar.aggregates.Max` / :class:`~specstar.aggregates.Avg`
        (each takes the field to reduce). SQL ``None`` semantics: ``None`` /
        UNSET values are skipped, and a group whose values are all ``None``
        reduces to ``None`` for Sum / Min / Max / Avg.

        Foreign aggregates (over *another* RM's rows linked to each group's
        key value): :class:`~specstar.aggregates.ForeignAggregate` ``(rm,
        link, aggregate)`` — internally one extra aggregate query per foreign,
        scoped to just the keys this call actually surfaces.

        Cross-RM "list parents with children stats": pass
        ``by=QB.resource_id()`` — each group is then exactly one row of *this*
        RM, and the returned ``GroupRow.resource`` carries that row's
        :class:`SearchedResource` so you can read ``r.resource.data.name``
        alongside ``r.chunk_count``. For any other ``by``, ``.resource`` is
        ``None``.

        Args:
            by: a QB :class:`~specstar.query.Field` — ``QB["foo"]`` for an
                indexed data field, ``QB.created_by()`` (and friends) for a
                ``ResourceMeta`` attribute. The Field's ``source`` is honoured
                exactly (no meta-vs-data guessing), and the same
                ``on_unindexed_query`` policy as filter conditions applies
                (default ``warn``). Passing a plain string raises ``TypeError``.
            aggregates: ``{result_name: Aggregate | ForeignAggregate}``, e.g.
                ``{"count": Count(), "total": Sum(QB["size"]),
                "chunks": ForeignAggregate(chrm, QB["source_doc_id"], Count())}``.
                Each ``result_name`` becomes an attribute on the returned rows.
            query: optional filter (same shape as ``search_resources``); rows
                matching the query are what we group over. ``None`` = all.

        Returns:
            ``list[GroupRow]`` — one row per distinct ``by`` value (missing /
            UNSET values group under ``key=None``). Each row has ``.key``, the
            named aggregates as both attributes and items, and ``.resource``
            (only populated when ``by`` is ``QB.resource_id()``).
        """
        from specstar.aggregates import (
            Aggregate,
            Avg,
            Count,
            ForeignAggregate,
            GroupRow,
            Max,
            Min,
            Sum,
        )
        from specstar.query import Field as _Field

        # 0) ``by`` must be a QB Field, or a non-empty list/tuple of Fields for a
        #    COMPOSITE group-by — no string-by-convention guessing. A composite
        #    ``by`` yields a TUPLE group key (in field order); a single Field keeps
        #    the scalar key (backward compatible).
        by_fields = list(by) if isinstance(by, (list, tuple)) else [by]
        if not by_fields or not all(isinstance(f, _Field) for f in by_fields):
            raise TypeError(
                "exp_aggregate_by: ``by`` must be a QB Field, or a non-empty "
                "list of QB Fields for a composite group-by "
                '(e.g. QB["source_doc_id"], or [QB["metric"], QB["period"]]); '
                f"got {type(by).__name__}."
            )
        is_composite = len(by_fields) > 1
        by = by_fields[0]  # single-field paths (per-row mode / push) use this

        # 0b) Parse the #412 group order + guard the page bounds up front. The
        #     negative-bound check MUST be here: on the pushed path a negative
        #     ``limit`` would otherwise reach the store, where e.g. SQLite reads
        #     ``LIMIT -1`` as "no limit" instead of raising. An invalid
        #     ``order_by`` TARGET needs no up-front check — it is never pushable,
        #     so it always falls through to ``_order_and_page_groups`` below,
        #     which raises (the single source of that error, covered on every
        #     backend).
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError("exp_aggregate_by: offset/limit must be non-negative")
        order_target: "str | None" = None
        order_descending = False
        if order_by is not None:
            order_target = order_by
            if order_target[:1] in ("+", "-"):
                order_descending = order_target[0] == "-"
                order_target = order_target[1:]

        # 1) Split self-aggregates from foreign-aggregates; validate.
        self_aggs: dict[str, Aggregate] = {}
        foreign_aggs: dict[str, ForeignAggregate] = {}
        for name, agg in aggregates.items():
            if isinstance(agg, Aggregate):
                self_aggs[name] = agg
            elif isinstance(agg, ForeignAggregate):
                foreign_aggs[name] = agg
            else:
                raise TypeError(
                    f"aggregates[{name!r}] must be an Aggregate or "
                    f"ForeignAggregate; got {type(agg).__name__}."
                )

        # 2) Validate group-by + every self-aggregated Field is queryable for
        #    its declared source (foreign fields are checked by the foreign rm).
        #    Reuse the same on_unindexed_query policy as filter conditions.
        agg_field_specs: list[_Field] = [
            a.field for a in self_aggs.values() if isinstance(a, (Sum, Min, Max, Avg))
        ]
        bad = [
            f.name
            for f in (*by_fields, *agg_field_specs)
            if not self._is_field_queryable(f)
        ]
        if bad:
            from specstar.types import OnUnindexedQuery, UnindexedQueryError

            if self._on_unindexed_query == OnUnindexedQuery.error:
                raise UnindexedQueryError(
                    bad,
                    sorted(f.index_key or f.field_path for f in self._indexed_fields),
                )
            warnings.warn(
                f"exp_aggregate_by on {self._resource_name!r}: field(s) {bad!r} "
                "aren't in indexed_data and aren't ResourceMeta attributes — "
                "they'll be treated as None. Index them, or group / aggregate on "
                "a ResourceMeta attribute.",
                SpecStarWarning,
                stacklevel=2,
            )

        # Per-aggregate state machines.
        def _init(agg: Aggregate):
            if isinstance(agg, Count):
                return 0
            if isinstance(agg, Avg):
                return [0.0, 0]  # [running_sum, n]
            return None  # Sum / Min / Max

        def _update(agg: Aggregate, state, val):
            if isinstance(agg, Count):
                return state + 1
            if val is None:
                return state
            if isinstance(agg, (Sum, Avg)) and not isinstance(val, (int, float)):
                raise TypeError(
                    f"{type(agg).__name__}({agg.field!r}) requires a numeric "
                    f"value; got {type(val).__name__} ({val!r})."
                )
            if isinstance(agg, Sum):
                return val if state is None else state + val
            if isinstance(agg, Min):
                return val if state is None else min(state, val)
            if isinstance(agg, Max):
                return val if state is None else max(state, val)
            # Avg
            state[0] += val
            state[1] += 1
            return state

        def _finalize(agg: Aggregate, state):
            if isinstance(agg, Avg):
                return state[0] / state[1] if state[1] > 0 else None
            return state

        # 3) Reduce self-aggregates.
        #    When grouping by ``QB.resource_id()``, each group is exactly one
        #    row of *this* rm — fetch via list_resources so we can attach
        #    ``.resource`` to the result (the cross-RM "list parents with
        #    children stats" case). For any other ``by``, walk iter_all and
        #    bucket per key.
        groups: dict[object, dict[str, object]] = {}
        resource_by_key: dict[object, object] = {}
        per_row_mode = (
            not is_composite and by.name == "resource_id" and by.source == "meta"
        )
        # Set only when the store's engine did the group ORDER BY / LIMIT /
        # OFFSET (#412) — then step 6 returns ``out`` as-is instead of
        # re-ordering + re-slicing it (which would double-apply the offset).
        did_page = False

        if per_row_mode:
            parents = self.list_resources(query)
            for p in parents:
                key = p.meta.resource_id
                resource_by_key[key] = p
                state = groups[key] = {n: _init(a) for n, a in self_aggs.items()}
                for n, a in self_aggs.items():
                    val = (
                        None
                        if isinstance(a, Count)
                        else self._read_field(p.meta, a.field)
                    )
                    state[n] = _update(a, state[n], val)
        else:
            # Push the group-by down to the metastore engine when it advertises
            # the capability (typed ``isinstance``, not ``hasattr``) AND every
            # self-aggregate is push-eligible. Each aggregate contributes a
            # push-down *plan* (store AggSpecs + a finalizer); an ineligible one
            # (str/undeclared field, …) drops the WHOLE call to the Python path
            # below so a partial/type-mismatched result is never pushed. A
            # pushed result MUST equal the Python reduction (enforced
            # cross-backend by tests/meta_store/test_aggregate_by.py).
            ms = self.storage.meta_store
            pushable = bool(
                self_aggs
                and not foreign_aggs
                and not is_composite  # composite group-by: Python reduce (pushdown TODO)
                and by.source in ("meta", "data")
                and isinstance(ms, IMetaWithAgg)
            )
            store_specs: list[AggSpec] = []
            to_state: dict[str, "Callable[[dict[str, object]], object]"] = {}
            # Result-names an engine ORDER BY can target: those whose push-down
            # plan is exactly ONE store column named after the caller name
            # (Count / Sum / Min / Max — incl. datetime Min/Max). Avg decomposes
            # into two columns, so ordering by it stays on the Python path.
            single_col_aggs: set[str] = set()
            if pushable:
                for n, a in self_aggs.items():
                    plan = self._agg_pushdown_plan(n, a)
                    if plan is None:
                        pushable = False
                        break
                    specs, make_state = plan
                    store_specs.extend(specs)
                    to_state[n] = make_state
                    if len(specs) == 1 and specs[0].result_name == n:
                        single_col_aggs.add(n)
            if pushable:
                # ``pushable`` already required ``isinstance(ms, IMetaWithAgg)``;
                # re-assert so the type checker narrows ``ms`` for aggregate_by.
                assert isinstance(ms, IMetaWithAgg)
                from specstar.query_types import AggKeyRef
                from specstar.query_types import ResourceMetaSearchQuery as _RMSQ

                if query is None:
                    q = _RMSQ()
                elif isinstance(query, Query):
                    q = query.build()
                else:
                    q = query
                # #412: push the group-level ORDER BY / LIMIT / OFFSET too, but
                # only when the store advertises the capability AND the order
                # target is engine-orderable (the group key, or a single-column
                # aggregate). Anything else — an Avg / ForeignAggregate / non-
                # paging store — keeps the in-process reorder over all groups.
                store_order: "str | None" = None
                if order_target is not None and getattr(
                    ms, "supports_group_paging", False
                ):
                    if order_target == "key" or order_target in single_col_aggs:
                        store_order = ("-" if order_descending else "") + order_target
                did_page = store_order is not None
                rows = ms.aggregate_by(
                    q,
                    # ``by.source`` is narrowed to meta/data by the guard above;
                    # Field.source is typed ``str`` so spell the Literal out.
                    AggKeyRef(
                        source=cast(Literal["meta", "data"], by.source), name=by.name
                    ),
                    store_specs,
                    order_by=store_order,
                    limit=limit if did_page else None,
                    offset=offset if did_page else 0,
                )
                groups = {
                    key: {n: to_state[n](state) for n in self_aggs}
                    for key, state in rows
                }
            else:
                for meta in self.iter_all(query):
                    key = (
                        tuple(self._read_field(meta, f) for f in by_fields)
                        if is_composite
                        else self._read_field(meta, by)
                    )
                    state = groups.get(key)
                    if state is None:
                        state = groups[key] = {
                            n: _init(a) for n, a in self_aggs.items()
                        }
                    for n, a in self_aggs.items():
                        val = (
                            None
                            if isinstance(a, Count)
                            else self._read_field(meta, a.field)
                        )
                        state[n] = _update(a, state[n], val)

        # 4) Foreign aggregates: one query per foreign, scoped to the keys we
        #    actually have, then zip onto each group.
        foreign_results: dict[str, dict[object, object]] = {n: {} for n in foreign_aggs}
        if groups and foreign_aggs:
            from specstar.query import QB as _QB

            keys = list(groups.keys())
            for name, fa in foreign_aggs.items():
                rows = fa.rm.exp_aggregate_by(
                    fa.link,
                    {name: fa.aggregate},
                    query=(_QB[fa.link.name] << keys).build(),
                )
                foreign_results[name] = {r.key: r[name] for r in rows}

        def _foreign_zero(fa: ForeignAggregate):
            return 0 if isinstance(fa.aggregate, Count) else None

        # 5) Build GroupRow per group, with finalised self-aggregates + foreign
        #    lookups + optional .resource (per-row mode only).
        out: list[GroupRow] = []
        for key, state in groups.items():
            row_kwargs = {n: _finalize(self_aggs[n], state[n]) for n in self_aggs}
            for n, fa in foreign_aggs.items():
                row_kwargs[n] = foreign_results[n].get(key, _foreign_zero(fa))
            out.append(
                GroupRow(key=key, resource=resource_by_key.get(key), **row_kwargs)
            )

        # 6) Group-level ordering + pagination (#412). ``order_by`` targets an
        #    aggregate result-name or the sentinel ``"key"`` (the group key), with
        #    the ``"-name"`` / ``"+name"`` direction convention from ``Query.sort()``
        #    (asc default, ``-`` = desc). ``limit`` / ``offset`` page the GROUPS —
        #    distinct from the row-level ``query.limit/offset`` (still ignored for
        #    aggregates). When the store's engine already did the ORDER BY / LIMIT /
        #    OFFSET (``did_page``), ``out`` IS the requested page — return it as-is.
        #    Otherwise this in-process reference orders + slices the materialized
        #    ``GroupRow`` list, and every pushed-down path MUST match it.
        if did_page:
            return out
        return self._order_and_page_groups(out, aggregates, order_by, limit, offset)

    def exp_count_groups(
        self,
        by: "object",
        query: "ResourceMetaSearchQuery | Query | None" = None,
    ) -> int:
        """The number of DISTINCT groups :meth:`exp_aggregate_by` would return for
        the same ``by`` + ``query`` — the total a pager needs, independent of any
        ``limit`` / ``offset``. A missing / UNSET / NULL ``by`` value counts as one
        group (``key=None``), matching :meth:`exp_aggregate_by`."""
        from specstar.aggregates import Count

        return len(self.exp_aggregate_by(by, {"__n": Count()}, query=query))

    def _order_and_page_groups(
        self,
        out: "list[Any]",
        aggregates: "dict[str, object]",
        order_by: "str | None",
        limit: "int | None",
        offset: int,
    ) -> "list[Any]":
        """#412 in-process reference for group-level ``order_by`` + ``limit`` /
        ``offset``: sort the materialized ``GroupRow``\\ s and slice. Every backend's
        pushed ``ORDER BY … LIMIT … OFFSET`` MUST return the SAME result. NULL
        order-values AND NULL keys sort LAST regardless of direction; the group key
        is the deterministic secondary sort so pages never straddle on ties.

        ``offset`` / ``limit`` are already guaranteed non-negative by the caller
        (``exp_aggregate_by`` validates the page bounds up front, for the pushed
        path too), so this reference only orders + slices."""
        if order_by is not None:
            name, descending = order_by, False
            if name[:1] in ("+", "-"):
                descending = name[0] == "-"
                name = name[1:]
            if name != "key" and name not in aggregates:
                raise ValueError(
                    f"exp_aggregate_by: order_by target {name!r} is not an "
                    "aggregate result-name or 'key'"
                )

            def _val(r: "Any", _n: str = name) -> "object":
                return r.key if _n == "key" else r[_n]

            def _cmp(a: "Any", b: "Any") -> int:
                # Primary: order value by direction, NULLs LAST (direction-free).
                av, bv = _val(a), _val(b)
                if av is None or bv is None:
                    if av is not bv:  # exactly one None → it sorts last
                        return 1 if av is None else -1
                elif av != bv:
                    return (-1 if av < bv else 1) * (-1 if descending else 1)
                # Secondary: group key ascending, NULLs LAST (stable tiebreak).
                ak, bk = a.key, b.key
                if ak is None or bk is None:
                    return 0 if ak is bk else (1 if ak is None else -1)
                return 0 if ak == bk else (-1 if ak < bk else 1)

            out.sort(key=cmp_to_key(_cmp))
        if offset or limit is not None:
            out = out[offset : None if limit is None else offset + limit]
        return out

    #: ResourceMeta datetime attributes eligible for Min/Max push-down. They are
    #: stored as absolute instants (SQLite REAL epoch / Postgres TIMESTAMP read
    #: as UTC), so a pushed Min/Max round-trips exactly to the Python-path value.
    _META_TIME_COLUMNS = frozenset(
        {"created_time", "updated_time", "rev_created_time", "rev_updated_time"}
    )

    def _declared_data_field_type(self, name: str) -> "type | None":
        """The declared Python type of a data-source indexed field (matched by
        ``index_key or field_path``), or ``None`` when the field is unindexed
        or was registered without a concrete ``field_type`` (``field_type`` may
        be ``UNSET`` or a ``SpecialIndex``). Used to decide push-down
        eligibility for value reducers and to coerce a pushed result back to the
        exact type the Python reduction produces."""
        for f in self._indexed_fields:
            if (f.index_key or f.field_path) == name:
                ft = f.field_type
                return ft if isinstance(ft, type) else None
        return None

    def _agg_pushdown_plan(self, result_name: str, agg: "object"):
        """A push-down *plan* for one self-aggregate, or ``None`` to keep the
        Python reduction.

        Returns ``(store_specs, to_state)``: ``store_specs`` are the
        :class:`~specstar.query_types.AggSpec`\\ s to send to the store, and
        ``to_state(store_dict)`` maps the store's per-group dict (keyed by those
        specs' ``result_name``) to the SAME per-group *state* the Python
        reduction accumulates — which step 5's ``_finalize`` then turns into the
        final value. For ``Count``/``Sum``/``Min``/``Max`` the state IS the
        (declared-type-coerced) value and ``_finalize`` is identity; ``Avg`` is
        decomposed into a pushed ``Sum`` + a pushed non-null ``Count`` and
        returns the ``[running_sum, n]`` state so ``_finalize`` divides it —
        byte-parity with the reference path and no backend ``AVG`` float
        divergence.

        v1 (#406): ``Count`` (any group); ``Sum``/``Min``/``Max``/``Avg`` over a
        **data-source** field with a declared numeric (``int``/``float``) type.
        Everything else — ``str``/undeclared fields, meta columns — returns
        ``None``."""
        from specstar.aggregates import Avg, Count, Max, Min, Sum
        from specstar.query_types import AggKeyRef, AggSpec

        if isinstance(agg, Count):
            return (
                [AggSpec(result_name=result_name, op="count")],
                lambda st, _n=result_name: int(st[_n]),
            )

        op = {Sum: "sum", Min: "min", Max: "max"}.get(type(agg))
        is_avg = isinstance(agg, Avg)
        if op is None and not is_avg:
            return None
        # op is not None or is_avg ⇒ a _FieldAggregate carrying ``.field``.
        field = agg.field  # ty: ignore[unresolved-attribute]

        # Min/Max over a meta time column (created_time / updated_time / rev_*):
        # push it as an absolute Unix epoch and rebuild the tz-aware UTC datetime
        # in Python — SQLite stores those columns as REAL epochs; Postgres
        # EXTRACTs the epoch (see the store _agg_expr). The float epoch
        # round-trips to the microsecond, so it equals the Python-path datetime.
        if (
            op in ("min", "max")
            and field.source == "meta"
            and (field.name in self._META_TIME_COLUMNS)
        ):
            return (
                [
                    AggSpec(
                        result_name=result_name,
                        op=op,  # ty: ignore[invalid-argument-type]
                        field=AggKeyRef(source="meta", name=field.name),
                        value_type="datetime",
                    )
                ],
                lambda st, _n=result_name: (
                    None
                    if st[_n] is None
                    else dt.datetime.fromtimestamp(float(st[_n]), dt.timezone.utc)
                ),
            )

        # Numeric value reducers + Avg need a data-source field with a declared
        # numeric type; anything else keeps the Python path.
        ftype = self._declared_data_field_type(field.name)
        if field.source != "data" or ftype not in (int, float):
            return None
        ref = AggKeyRef(source="data", name=field.name)

        if op is not None:
            coerce = int if ftype is int else float
            return (
                [
                    AggSpec(
                        result_name=result_name,
                        op=op,  # ty: ignore[invalid-argument-type]
                        field=ref,
                        value_type="numeric",
                    )
                ],
                lambda st, _n=result_name, _c=coerce: (
                    None if st[_n] is None else _c(st[_n])
                ),
            )

        # Avg → pushed Sum + non-null Count, returned as the reference
        # ``[running_sum, n]`` state for ``_finalize`` to divide. Postgres SUM is
        # Decimal → float() so the divided result is a float; a group with no
        # non-null values has ``n == 0`` (and NULL sum) ⇒ ``_finalize`` → None.
        s_key, c_key = f"{result_name}__sum", f"{result_name}__count"
        return (
            [
                AggSpec(result_name=s_key, op="sum", field=ref, value_type="numeric"),
                AggSpec(result_name=c_key, op="count", field=ref),
            ],
            lambda st, _s=s_key, _c=c_key: [
                0.0 if st[_s] is None else float(st[_s]),
                st[_c],
            ],
        )

    def _read_field(self, meta: ResourceMeta, field: "object") -> object:
        """Read a ``Field``'s value from a meta, honouring ``Field.source``.

        - ``meta``: look up the ``ResourceMeta`` struct attribute.
        - ``data``: look up ``ResourceMeta.indexed_data[name]``.
        - ``auto`` (legacy): try meta first, then ``indexed_data``.

        Missing / UNSET → ``None``.
        """
        name = field.name
        source = field.source
        if source == "meta":
            if hasattr(meta, name) and name != "indexed_data":
                return getattr(meta, name)
            return None
        if source == "data":
            if meta.indexed_data is not UNSET and name in meta.indexed_data:
                return meta.indexed_data[name]
            return None
        # auto
        if hasattr(meta, name) and name != "indexed_data":
            return getattr(meta, name)
        if meta.indexed_data is not UNSET and name in meta.indexed_data:
            return meta.indexed_data[name]
        return None

    def _is_field_queryable(self, field: "object") -> bool:
        """Whether ``field.name`` is reachable for ``field.source``."""
        meta_names = set(ResourceMeta.__struct_fields__) - {"indexed_data"}
        data_names = {f.index_key or f.field_path for f in self._indexed_fields}
        if field.source == "meta":
            return field.name in meta_names
        if field.source == "data":
            return field.name in data_names
        return field.name in (meta_names | data_names)

    @coerce_data_to_resource_type
    @execute_with_events(
        (BeforeCreate, AfterCreate, OnSuccessCreate, OnFailureCreate),
        "info",
        context_aware=True,
    )
    def create(
        self,
        data: T,
        *,
        status: RevisionStatus | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        resource_id: str | UnsetType = UNSET,
        if_not_exists: bool | UnsetType = UNSET,
    ) -> RevisionInfo:
        """
        Create a new resource.

        Arguments:
            data (T): The resource data object.
            status (RevisionStatus | UnsetType): The initial status of the resource (default: stable).
            user (str | UnsetType): The user performing the action.
                Overrides any active ``using()`` scope or manager default.
            now (datetime | UnsetType): The current timestamp.
                Overrides any active ``using()`` scope or manager default.
            resource_id (str | UnsetType): Specific resource ID to use
                instead of auto-generating one.
            if_not_exists (bool | UnsetType): When ``True`` and ``resource_id``
                is supplied, the call only proceeds if no resource with that id
                exists; otherwise :class:`DuplicateResourceError` is raised.
                This turns ``create`` into an atomic create-only that can be
                used as a cross-worker first-wins mutex / lock primitive.

        Returns:
            info (RevisionInfo): The revision info of the created resource.

        Raises:
            UniqueConstraintError: If a field annotated with :class:`Unique` already
                has the same value on another non-deleted resource.
            DuplicateResourceError: If ``if_not_exists=True`` and the supplied
                ``resource_id`` already exists.
        """
        status = self.default_status if status is UNSET else status
        if if_not_exists is True and resource_id is not UNSET:
            if self.storage.exists(resource_id):
                from specstar.types import DuplicateResourceError as _DRE

                raise _DRE(resource_id=resource_id)
        data = self._process_binary_fields(data)
        data = self._embedding_processor.process_sync(data)
        info = self._rev_info(_BuildRevInfoCreate(data, status))
        self.storage.save_revision(info, io.BytesIO(self.encode(data)))
        self.storage.save_meta(self._res_meta(_BuildResMetaCreate(info, data)))
        if self.message_queue is not None:
            self.message_queue.put(info.resource_id)
        return info

    @execute_with_events(
        (BeforeGet, AfterGet, OnSuccessGet, OnFailureGet),
        "resource",
    )
    def get(
        self,
        resource_id: str,
        *,
        revision_id: str | UnsetType = UNSET,
        schema_version: str | None | UnsetType = UNSET,
        include_deleted: bool = False,
    ) -> Resource[T]:
        """
        Get a resource by its ID.

        Arguments:
            resource_id (str): The ID of the resource to retrieve.
            revision_id (str | UnsetType): (Optional) The specific revision ID to retrieve. If not set, retrieves the latest revision.
            schema_version (str | None | UnsetType): (Optional) The schema version of the resource.
            include_deleted (bool): If True, return the requested revision even
                for a soft-deleted resource instead of raising
                ``ResourceIsDeletedError``. Mirrors ``get_meta()``. Defaults to
                ``False`` so existing behavior is unchanged.

        Returns:
            resource (Resource[T]): The resource object containing both data and metadata.

        Raises:
            ResourceIDNotFoundError: If the resource ID or revision ID does not exist.
            ResourceIsDeletedError: If the resource has been soft-deleted and
                *include_deleted* is ``False``.
        """
        if revision_id is UNSET or schema_version is UNSET:
            meta = self.get_meta(resource_id, include_deleted=include_deleted)
            if revision_id is UNSET:
                revision_id = meta.current_revision_id
            if schema_version is UNSET:
                schema_version = meta.schema_version
        try:
            return self.get_resource_revision(
                resource_id, revision_id, schema_version=schema_version
            )
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            # Honour on_decode_error. For a single get there is nothing to
            # "skip", so skip degrades to error.
            if self._on_decode_error == OnDecodeError.raw:
                info = self.get_revision_info(
                    resource_id, revision_id, schema_version=schema_version
                )
                return Resource(
                    info=info,
                    data=self._undecodable_data(
                        resource_id, revision_id, schema_version, str(e)
                    ),
                )
            raise ResourceDecodeError(resource_id, str(e)) from e

    def get_partial(
        self,
        resource_id: str,
        revision_id: str,
        partial: Iterable[str | JsonPointer],
        *,
        schema_version: str | None | UnsetType = UNSET,
    ) -> Struct:
        """
        Get a partial view of a resource (only specified fields).

        Arguments:
            resource_id (str): The ID of the resource.
            revision_id (str): The revision ID of the resource.
            partial (Iterable[str | JsonPointer]): A list of fields or JSON pointers to retrieve.

        Returns:
            partial_data (Struct): A struct containing only the requested fields.
        """
        with self.storage.get_data_bytes(
            resource_id, revision_id, schema_version=schema_version
        ) as data_io:
            PartialType = create_partial_type(self._resource_type, partial)
            s = MsgspecSerializer(
                encoding=self._encoding,
                resource_type=PartialType,
            )
            decoded = s.decode(data_io.read())
            return prune_object(decoded, partial)

    def get_revision_info(
        self,
        resource_id: str,
        revision_id: str | UnsetType = UNSET,
        *,
        schema_version: str | None | UnsetType = UNSET,
    ) -> RevisionInfo:
        """
        Get the metadata (RevisionInfo) of a specific revision.

        Arguments:
            resource_id (str): The ID of the resource.
            revision_id (str | UnsetType): The revision ID. If not set, returns the latest revision info.

        Returns:
            info (RevisionInfo): The metadata of the revision.
        """
        # Access-scope gate: this is the one single-resource read route that can
        # reach storage with an explicit ``revision_id`` *without* leading with
        # get_meta (``GET /{id}/revision-info?revision_id=...``), so enforce
        # scope here too. Clear the flag for the rest so the UNSET-branch
        # get_meta below doesn't re-probe (the resource is already authorized).
        self._assert_in_scope(resource_id)
        with self.apply_scope_ctx.ctx(False):
            if revision_id is UNSET:
                meta = self.get_meta(resource_id)
                revision_id = meta.current_revision_id
                if schema_version is UNSET:
                    schema_version = meta.schema_version

            return self.storage.get_resource_revision_info(
                resource_id, revision_id, schema_version=schema_version
            )

    @execute_with_events(
        (
            BeforeGetResourceRevision,
            AfterGetResourceRevision,
            OnSuccessGetResourceRevision,
            OnFailureGetResourceRevision,
        ),
        "resource",
    )
    def get_resource_revision(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> Resource[T]:
        """
        Get a specific revision of a resource.

        Arguments:
            resource_id (str): The ID of the resource.
            revision_id (str): The specific revision ID.

        Returns:
            resource (Resource[T]): The resource object.
        """
        # One store round-trip: `info` and `data` are two columns of the same
        # joined row, and reading a resource always needs both.
        info, raw = self.storage.get_revision_bundle(
            resource_id, revision_id, schema_version
        )
        return self._resource_from_bundle(info, raw)

    def _resource_from_bundle(self, info: RevisionInfo, raw: bytes) -> Resource[T]:
        """Turn a fetched row into a `Resource` — decode, lazy migration, and the
        warning that goes with it.

        Split out so a caller that already holds the row (a listing that fetched
        its whole page in one query) reuses this instead of restating it beside a
        loop. It is not a parameter on `get_resource_revision` because that method
        is wrapped by the event machinery, which validates its keyword arguments.
        """
        if self._migration is not None and info.schema_version != self._schema_version:
            # Lazy read-time migration: the row is stored at an older version,
            # so apply the registered migration to present it as the current
            # model. Storage is NOT rewritten — ``info.schema_version`` keeps
            # the stored version (honest about what's persisted); run an
            # explicit migrate() to persist the upgrade. Warn once so the
            # "looks migrated but isn't persisted" gap is visible.
            data = self._migration.migrate(io.BytesIO(raw), info.schema_version)
            if not self._warned_lazy_migration:
                self._warned_lazy_migration = True
                logger.warning(
                    "%s: applying registered migration on read (%s -> %s) — "
                    "storage is NOT rewritten and schema_version stays at the "
                    "stored value; run migrate() to persist the upgrade.",
                    self.resource_name,
                    info.schema_version,
                    self._schema_version,
                )
        else:
            data = self.decode(raw)
        return Resource(info=info, data=data)

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
        """
        List all revision IDs for a given resource.

        Arguments:
            resource_id (str): The ID of the resource.

        Returns:
            revisions (list[str]): A list of revision IDs.
        """
        return self.storage.list_revisions(resource_id)

    @coerce_data_to_resource_type
    @execute_with_events(
        (BeforeUpdate, AfterUpdate, OnSuccessUpdate, OnFailureUpdate),
        "revision_info",
        context_aware=True,
    )
    def update(
        self,
        resource_id: str,
        data: T,
        *,
        expected_revision_id: str | UnsetType = UNSET,
        expected_etag: str | UnsetType = UNSET,
        status: RevisionStatus | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """
        Update an existing resource with new data (creates a new revision).

        Arguments:
            resource_id (str): The ID of the resource to update.
            data (T): The new resource data.
            expected_revision_id (str | UnsetType): Optimistic concurrency
                guard. When set, the call only proceeds if the resource's
                current revision matches; otherwise it raises
                :class:`PreconditionFailedError` without writing. Lets
                edit-and-retry loops survive concurrent writers safely.
            status (RevisionStatus | UnsetType): The status of the new revision (default: stable).
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            info (RevisionInfo): The revision info of the updated resource.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            PreconditionFailedError: If ``expected_revision_id`` is set and
                the resource's current revision has moved on.
        """
        status = self.default_status if status is UNSET else status
        data = self._process_binary_fields(data)
        prev_res_meta = self.get_meta(resource_id)
        if (
            expected_revision_id is not UNSET
            and prev_res_meta.current_revision_id != expected_revision_id
        ):
            from specstar.types import PreconditionFailedError as _PFE

            raise _PFE(
                resource_id=resource_id,
                expected_revision_id=expected_revision_id,
                actual_revision_id=prev_res_meta.current_revision_id,
            )
        # Pass the schema version we already hold. Omitting it makes the storage
        # facade re-read this very meta to resolve it — the same
        # `SELECT ... WHERE resource_id = ...`, plus its own connection checkout,
        # from a caller standing on the answer.
        prev_info = self.storage.get_resource_revision_info(
            resource_id,
            prev_res_meta.current_revision_id,
            schema_version=prev_res_meta.schema_version,
        )
        if expected_etag is not UNSET and prev_info.etag != expected_etag:
            from specstar.types import PreconditionFailedError as _PFE

            raise _PFE(
                resource_id=resource_id,
                expected_revision_id=expected_etag,
                actual_revision_id=prev_info.etag,
            )
        # Pass previous data for Embedding cache reuse (bypass events: raw decode).
        # Only worth fetching when something will actually read it: a model with
        # no Embedding field makes `process_sync` an empty loop, and this read —
        # plus the decode of the whole previous row — would produce an argument
        # nobody looks at, on every update.
        prev_data = None
        if self._embedding_processor.reuses_previous:
            try:
                with self.storage.get_data_bytes(
                    resource_id,
                    prev_res_meta.current_revision_id,
                    prev_res_meta.schema_version,
                ) as fh:
                    prev_data = self._data_serializer.decode(fh.read())
            except Exception:
                pass
        data = self._embedding_processor.process_sync(data, previous=prev_data)
        rev_info = self._rev_info(_BuildRevInfoUpdate(prev_res_meta, data, status))
        if prev_info.data_hash == rev_info.data_hash:
            return prev_info
        res_meta = self._res_meta(_BuildResMetaUpdate(prev_res_meta, rev_info, data))
        self.storage.save_revision(rev_info, io.BytesIO(self.encode(data)))
        self.storage.save_meta(res_meta)
        return rev_info

    def create_or_update(
        self,
        resource_id,
        data,
        *,
        status: RevisionStatus | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ):
        """
        Create a new resource or update if it already exists.

        Arguments:
            resource_id (str): The ID of the resource.
            data (T): The resource data.
            status (RevisionStatus | UnsetType): The status (default: stable).
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            info (RevisionInfo): The revision info.
        """
        with self._apply_context(user=user, now=now, resource_id=resource_id):
            try:
                return self.update(resource_id, data, status=status)
            except ResourceIDNotFoundError:
                return self.create(data, status=status)

    @coerce_data_to_resource_type
    @execute_with_events(
        (BeforeModify, AfterModify, OnSuccessModify, OnFailureModify),
        "revision_info",
        context_aware=True,
    )
    def modify(
        self,
        resource_id: str,
        data: "T | JsonPatch | MergePatch | UnsetType" = UNSET,
        status: RevisionStatus | UnsetType = UNSET,
        *,
        expected_revision_id: str | UnsetType = UNSET,
        expected_etag: str | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """
        Modify a resource without creating a new revision (only for DRAFT status).

        Arguments:
            resource_id (str): The ID of the resource.
            data (T | JsonPatch | UnsetType): The new data or JSON patch to apply.
            status (RevisionStatus | UnsetType): The new status.
            expected_revision_id (str | UnsetType): Optimistic concurrency
                guard — see :meth:`update`.
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            info (RevisionInfo): The updated revision info.

        Raises:
            CannotModifyResourceError: If the resource is not in DRAFT status.
            PreconditionFailedError: If ``expected_revision_id`` is set and
                the resource's current revision has moved on.
        """
        if data is UNSET and status is not UNSET:
            return self._modify_status(resource_id, status)

        prev_res_meta = self.get_meta(resource_id)
        if (
            expected_revision_id is not UNSET
            and prev_res_meta.current_revision_id != expected_revision_id
        ):
            from specstar.types import PreconditionFailedError as _PFE

            raise _PFE(
                resource_id=resource_id,
                expected_revision_id=expected_revision_id,
                actual_revision_id=prev_res_meta.current_revision_id,
            )
        prev_info = self.storage.get_resource_revision_info(
            resource_id,
            prev_res_meta.current_revision_id,
        )
        if expected_etag is not UNSET and prev_info.etag != expected_etag:
            from specstar.types import PreconditionFailedError as _PFE

            raise _PFE(
                resource_id=resource_id,
                expected_revision_id=expected_etag,
                actual_revision_id=prev_info.etag,
            )
        if data is UNSET and status is UNSET:
            return prev_info
        if (
            prev_info.status != RevisionStatus.draft
            and status is not RevisionStatus.draft
        ):
            raise CannotModifyResourceError(resource_id)
        if type(data) is JsonPatch:
            data = self._apply_patch(resource_id, data)
        elif type(data) is MergePatch:
            data = self._apply_merge_patch(resource_id, data)

        if data is not UNSET:
            data = self._process_binary_fields(data)
            prev_data = None
            try:
                with self.storage.get_data_bytes(
                    resource_id, prev_res_meta.current_revision_id
                ) as fh:
                    prev_data = self._data_serializer.decode(fh.read())
            except Exception:
                pass
            data = self._embedding_processor.process_sync(data, previous=prev_data)

        rev_info = self._rev_info(
            _BuildRevInfoModify(prev_res_meta, prev_info, data, status=status)
        )
        if prev_info.data_hash == rev_info.data_hash:
            return prev_info
        res_meta = self._res_meta(_BuildResMetaModify(prev_res_meta, rev_info, data))
        self.storage.save_revision(rev_info, io.BytesIO(self.encode(data)))
        self.storage.save_meta(res_meta)
        return rev_info

    def _modify_status(self, resource_id: str, status: RevisionStatus) -> RevisionInfo:
        prev_res_meta = self.get_meta(resource_id)
        prev_info = self.storage.get_resource_revision_info(
            resource_id,
            prev_res_meta.current_revision_id,
        )
        if prev_info.status == status:
            return prev_info
        rev_info = self._rev_info(
            _BuildRevInfoModify(prev_res_meta, prev_info, UNSET, status=status)
        )
        res_meta = self._res_meta(_BuildResMetaModify(prev_res_meta, rev_info, UNSET))
        with self.storage.get_data_bytes(
            resource_id, prev_res_meta.current_revision_id
        ) as data_io:
            self.storage.save_revision(rev_info, data_io)
        self.storage.save_meta(res_meta)
        return rev_info

    @execute_with_events(
        (BeforePatch, AfterPatch, OnSuccessPatch, OnFailurePatch),
        "revision_info",
        inputs={"patch_data": "patch_data.patch"},
        context_aware=True,
    )
    def patch(
        self,
        resource_id: str,
        patch_data: "JsonPatch | MergePatch",
        *,
        expected_revision_id: str | UnsetType = UNSET,
        expected_etag: str | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """
        Apply an RFC 6902 JSON Patch or RFC 7386 Merge Patch to the resource.

        Arguments:
            resource_id (str): the id of the resource to patch.
            patch_data (JsonPatch | MergePatch): RFC 6902 operations
                (``JsonPatch``) or an RFC 7386 merge patch (``MergePatch``).
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            info (RevisionInfo): the metadata of the newly created revision.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted.
        """
        if type(patch_data) is MergePatch:
            data = self._apply_merge_patch(resource_id, patch_data)
        else:
            data = self._apply_patch(resource_id, patch_data)
        return self.update(
            resource_id,
            data,
            expected_revision_id=expected_revision_id,
            expected_etag=expected_etag,
        )

    def patch_many(
        self,
        query: "ResourceMetaSearchQuery | Query | None",
        patch_data: "JsonPatch | MergePatch",
        *,
        max_bytes: int = DEFAULT_PATCH_MANY_MAX_BYTES,
        status: RevisionStatus | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> PatchManyResult:
        """Apply one patch to every resource *query* selects (#434).

        This is the shape a fan-out actually needs — pushing a derived mirror
        onto every row of a collection, say. The caller describes the fields
        that change and never touches the data, which is the point: once the
        read belongs to specstar, specstar can batch it. Handing back
        ``(resource_id, full_data)`` pairs instead would force the caller to
        read and re-encode every row itself, one round-trip at a time.

        Semantically this is **N patches whose reads and writes go through the
        bulk path**, not a second set of rules:

        * Events fire **per row**, including the permission check. Write ACLs
          are per-row policies, so a batch-level check would authorize the
          batch and not the rows — a way to write rows the caller may not
          write. This is why ``patch_many`` is not itself wrapped in
          ``execute_with_events``: that would arm the re-entrancy guard and
          turn every row's check into a no-op.
        * A row whose current revision moved since it was selected is reported
          as a **conflict** and left alone. A patch rewrites the whole body, so
          writing anyway would discard a concurrent edit entirely, not merely
          the mirrored fields. This is the same guarantee ``patch`` gives with
          ``expected_revision_id`` — no stronger: the compare and the write are
          not atomic, so it narrows the window rather than closing it.
        * A no-op patch creates no revision, so a caller need not pre-filter
          rows that already hold the target value, and re-running is cheap.

        Failures are **collected, not raised**: one unwritable row must not
        strand the rest of a collection.

        Memory is bounded by *max_bytes*, not by a row count — row size varies
        by orders of magnitude between models, so rows are the wrong unit. Rows
        are read in batches that fit the budget; a row larger than the whole
        budget is still processed on its own.

        Arguments:
            query: selects the rows to patch. ``None`` selects everything.
            patch_data: an RFC 6902 ``JsonPatch`` or RFC 7386 ``MergePatch``,
                applied to every selected row.
            max_bytes: ceiling on how much row data is held in memory at once.
            status: status for the new revisions (default: stable).
            user: the user performing the fan-out; recorded as ``updated_by``
                on every row it writes, while each row keeps its own
                ``created_by``.
            now: timestamp for the write.

        Returns:
            result (PatchManyResult): counts plus the ids that conflicted or
            failed.
        """
        status = self.default_status if status is UNSET else status
        result = PatchManyResult()
        with self._apply_context(user=user, now=now):
            metas = self.search_resources(query)
            pending = [
                (m.resource_id, m.current_revision_id, m.schema_version) for m in metas
            ]
            while pending:
                payloads, consumed = self.storage.read_revisions_bulk(
                    pending, max_bytes=max_bytes
                )
                batch, pending = pending[:consumed], pending[consumed:]
                self._patch_batch(batch, payloads, patch_data, status, result)
        return result

    def _patch_batch(
        self,
        batch: "list[tuple[str, str, str | None]]",
        payloads: dict[str, bytes],
        patch_data: "JsonPatch | MergePatch",
        status: RevisionStatus,
        result: PatchManyResult,
    ) -> None:
        """Authorize, patch and write one budget-sized batch of rows.

        Ordering matters and is the whole reason this is not a loop over
        ``patch()``: every row is authorized and announced **before** anything
        is written, the batch is then written in one go, and only rows that
        actually landed get their success event. Firing success first and
        writing after would tell handlers a row was committed that might not
        be.
        """
        fresh = self.storage.read_metas_bulk([rid for rid, _, _ in batch])
        revisions: list[tuple[RevisionInfo, bytes]] = []
        new_metas: list[ResourceMeta] = []
        prepared: list[tuple[str, dict[str, Any], RevisionInfo]] = []

        for resource_id, selected_revision_id, _schema_version in batch:
            ctx_kw: dict[str, Any] = {
                "user": self.user_or_unset,
                "now": self.now_or_unset,
                "resource_name": self.resource_name,
                "resource_id": resource_id,
                "patch_data": patch_data.patch,
            }
            try:
                with ExitStack() as stack:
                    stack.enter_context(self.id_ctx.ctx(resource_id))
                    self._authorize_and_announce(BeforePatch(**ctx_kw), stack)

                    raw = payloads.get(resource_id)
                    prev_meta = fresh.get(resource_id)
                    if raw is None or prev_meta is None:
                        raise ResourceIDNotFoundError(resource_id)
                    if prev_meta.current_revision_id != selected_revision_id:
                        result.conflicts.append(resource_id)
                        continue

                    prev_data = self._data_serializer.decode(raw)
                    data = self._patched(prev_data, patch_data)
                    data = self._process_binary_fields(data)
                    data = self._embedding_processor.process_sync(
                        data, previous=prev_data
                    )
                    encoded = self.encode(data)
                    # Both sides go through this encoder, so a difference in
                    # how the row was stored (an older schema, say) cancels
                    # out and only a real change counts. Comparing here also
                    # saves a per-row read of the previous RevisionInfo just
                    # to reach its hash.
                    if encoded == self.encode(prev_data):
                        result.unchanged += 1
                        self._handle_event(AfterPatch(**ctx_kw))
                        continue

                    rev_info = self._rev_info(
                        _BuildRevInfoUpdate(prev_meta, data, status)
                    )
                    revisions.append((rev_info, encoded))
                    new_metas.append(
                        self._res_meta(_BuildResMetaUpdate(prev_meta, rev_info, data))
                    )
                    prepared.append((resource_id, ctx_kw, rev_info))
            except Exception as e:
                self._announce_patch_failure(ctx_kw, e)
                result.failures.append((resource_id, str(e)))

        if not revisions:
            return
        try:
            self.storage.save_revisions_bulk(revisions)
            self.storage.save_metas_bulk(new_metas)
        except Exception as e:
            # The batch is written as a unit, so nothing here can be reported
            # as committed. Every prepared row failed together.
            for resource_id, ctx_kw, _ in prepared:
                self._announce_patch_failure(ctx_kw, e)
                result.failures.append((resource_id, str(e)))
            return

        for resource_id, ctx_kw, rev_info in prepared:
            self._handle_event(OnSuccessPatch(**ctx_kw, revision_info=rev_info))
            self._handle_event(AfterPatch(**ctx_kw))
            result.patched += 1

    def _announce_patch_failure(self, ctx_kw: dict[str, Any], error: Exception) -> None:
        self._handle_event(
            OnFailurePatch(
                **ctx_kw,
                error=str(error),
                stack_trace="".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                ),
            )
        )
        self._handle_event(AfterPatch(**ctx_kw))

    def _patched(self, data: T, patch_data: "JsonPatch | MergePatch") -> T:
        """Apply either patch flavour to data that is already decoded.

        Split out of :meth:`_apply_patch` / :meth:`_apply_merge_patch` so a
        bulk caller that has already read the row (:meth:`patch_many`) applies
        the patch through the very same code the single-row path uses. Two
        implementations of "what a patch means" would be one too many — they
        would agree right up until the day they didn't.
        """
        if isinstance(data, msgspec.Raw):
            d = json.loads(bytes(data))
        else:
            d = msgspec.to_builtins(data)
        if isinstance(patch_data, MergePatch):
            return msgspec.convert(_rfc7386_merge(d, patch_data), self.resource_type)
        patch_data.apply(d, in_place=True)
        return msgspec.convert(d, self.resource_type)

    def _apply_patch(self, resource_id: str, patch_data: JsonPatch) -> T:
        return self._patched(self.get(resource_id).data, patch_data)

    def _apply_merge_patch(self, resource_id: str, merge_patch: dict) -> T:
        """RFC 7386 analogue of :meth:`_apply_patch`: merge ``merge_patch`` into
        the current data and return the resulting full resource."""
        return self._patched(self.get(resource_id).data, MergePatch(merge_patch))

    @execute_with_events(
        (BeforeSwitch, AfterSwitch, OnSuccessSwitch, OnFailureSwitch),
        "meta",
        context_aware=True,
    )
    def switch(
        self,
        resource_id: str,
        revision_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """
        Switch specific resource to another revision.

        Arguments:
            resource_id (str): The ID of the resource.
            revision_id (str): The revision ID to switch to.
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            meta (ResourceMeta): The updated metadata.

        Raises:
            RevisionIDNotFoundError: If the revision ID does not exist.
            RevisionNotMigratedError: If the revision has a different
                schema version than the resource's current schema version
                and migration is configured.  The revision must be
                migrated first via ``migrate(resource_id, revision_id=...)``.
        """
        meta = self.get_meta(resource_id)
        if meta.current_revision_id == revision_id:
            return meta

        # When migration is configured, use find_revision_schema_version
        # to locate the revision regardless of its schema_version key.
        if self._migration is not None:
            actual_sv = self.storage.find_revision_schema_version(
                resource_id, revision_id
            )
            if actual_sv is UNSET:
                raise RevisionIDNotFoundError(resource_id, revision_id)
            if actual_sv != meta.schema_version:
                raise RevisionNotMigratedError(
                    resource_id, revision_id, actual_sv, meta.schema_version
                )
        else:
            # No migration configured — use the original check
            if not self.storage.revision_exists(resource_id, revision_id):
                raise RevisionIDNotFoundError(resource_id, revision_id)

        # 切換到指定版本時，需要更新索引數據
        if self._indexed_fields:
            data = self._load_revision_data(resource_id, revision_id)
            meta.indexed_data = self._extract_indexed_values(data)

        # Refresh the embedded ``rev_*`` mirror to point at the new
        # current revision so search-by-revision-attrs stays consistent.
        target_info = self.storage.get_resource_revision_info(
            resource_id, revision_id, schema_version=meta.schema_version
        )
        meta.rev_status = target_info.status
        meta.rev_created_by = target_info.created_by
        meta.rev_updated_by = target_info.updated_by
        meta.rev_created_time = target_info.created_time
        meta.rev_updated_time = target_info.updated_time

        meta.updated_by = self.user_ctx.get()
        meta.updated_time = self.now_ctx.get()
        meta.current_revision_id = revision_id
        self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (BeforeDelete, AfterDelete, OnSuccessDelete, OnFailureDelete),
        "meta",
        context_aware=True,
    )
    def delete(
        self,
        resource_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """
        Soft delete a resource.

        Arguments:
            resource_id (str): The ID of the resource to delete.
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            meta (ResourceMeta): The updated metadata (is_deleted=True).

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
        """
        meta = self.get_meta(resource_id)
        meta.is_deleted = True
        meta.updated_by = self.user_ctx.get()
        meta.updated_time = self.now_ctx.get()
        self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (BeforeRestore, AfterRestore, OnSuccessRestore, OnFailureRestore),
        "meta",
        context_aware=True,
    )
    def restore(
        self,
        resource_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """
        Restore a soft-deleted resource.

        Arguments:
            resource_id (str): The ID of the resource to restore.
            user (str | UnsetType): The user performing the action.
            now (datetime | UnsetType): The current timestamp.

        Returns:
            meta (ResourceMeta): The updated metadata (is_deleted=False).

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
        """
        meta = self._get_meta_no_check_is_deleted(resource_id)
        if meta.is_deleted:
            meta.is_deleted = False
            meta.updated_by = self.user_ctx.get()
            meta.updated_time = self.now_ctx.get()
            self.storage.save_meta(meta)
        return meta

    @execute_with_events(
        (
            BeforePermanentlyDelete,
            AfterPermanentlyDelete,
            OnSuccessPermanentlyDelete,
            OnFailurePermanentlyDelete,
        ),
        "meta",
        context_aware=True,
    )
    def permanently_delete(
        self,
        resource_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """Permanently delete a resource and all its revision data.

        This is an irreversible operation that removes the resource metadata
        and all associated revision data from storage.

        Args:
            resource_id: The ID of the resource to permanently delete.
            user: The user performing the action.  Overrides any active
                ``using()`` scope or manager default.
            now: The current timestamp.  Overrides any active ``using()``
                scope or manager default.

        Returns:
            The metadata of the resource before deletion.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
        """
        meta = self._get_meta_no_check_is_deleted(resource_id)
        # Phase-2 ref accounting (issue #370): decref this resource's blobs
        # before purging its revisions, so blobs that hit zero feed the
        # incremental GC's candidate set. Best-effort; reconcile is authoritative.
        self._decref_resource_blobs(resource_id)
        self.storage.purge_resource(resource_id)
        return meta

    @execute_with_events(
        (BeforePrune, AfterPrune, OnSuccessPrune, OnFailurePrune),
        "pruned",
        context_aware=True,
    )
    def prune_revisions(
        self,
        resource_id: str,
        *,
        keep_last_n: int | UnsetType = UNSET,
        before: dt.datetime | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> list[str]:
        """Prune old revisions of a resource to reclaim storage space.

        See :meth:`IResourceManager.prune_revisions`.  The current revision is
        always preserved; ``total_revision_count`` is left unchanged (it seeds
        new revision ids).  Pruned revisions' blobs are ``decref``'d so the
        #370 garbage collector can reclaim them — no blob is deleted here.
        """
        if keep_last_n is UNSET and before is UNSET:
            raise ValueError(
                "prune_revisions requires at least one of keep_last_n or before"
            )
        if keep_last_n is not UNSET and keep_last_n < 1:
            raise ValueError("keep_last_n must be >= 1")

        meta = self._get_meta_no_check_is_deleted(resource_id)
        current = meta.current_revision_id

        # Gather revision info for every revision that has retrievable data.
        # created_time / parent_revision_id are identical across a revision's
        # schema versions, so any stored version's info will do.
        # One read for all of them: resolving the schema version and reading the
        # info per revision made pruning cost statements proportional to the
        # revision count — the thing being pruned.
        infos: dict[str, RevisionInfo] = self.storage.get_all_revision_infos(
            resource_id
        )
        if len(infos) <= 1:
            return []

        # Lineage = the current revision's ancestry chain (its "直系"/lineal
        # revisions). Walk parent_revision_id from current to the root.
        lineage: set[str] = set()
        cursor: str | None = current
        while cursor is not None and cursor in infos and cursor not in lineage:
            lineage.add(cursor)
            cursor = infos[cursor].parent_revision_id

        # Sort key: lineal first, then newer first. ``created_time`` is the
        # primary recency signal; the revision sequence number (the trailing
        # ``:N`` of the default ``<resource_id>:N`` scheme) is a deterministic
        # tie-breaker so revisions written at the same ``now`` still order by
        # authoring sequence — otherwise keep_last_n would be non-deterministic
        # on batch-created history. The current revision is always lineal and
        # the newest of its own lineage, so it ranks #1.
        def _seq(revision_id: str) -> int:
            tail = revision_id.rsplit(":", 1)[-1]
            return int(tail) if tail.isdigit() else -1

        ordered = sorted(
            infos,
            key=lambda r: (r in lineage, infos[r].created_time, _seq(r)),
            reverse=True,
        )

        # Union of the two retention floors (conservative): keep the top
        # ``keep_last_n`` and/or anything newer than ``before``; the current
        # revision is always kept (the keep_last_n>=1 floor).
        kept: set[str] = {current}
        if keep_last_n is not UNSET:
            kept.update(ordered[:keep_last_n])
        if before is not UNSET:
            kept.update(r for r in infos if infos[r].created_time >= before)

        prune_set = [r for r in ordered if r not in kept]

        # Defensive concurrency guard: re-read the current revision right
        # before deleting and never prune it — a concurrent switch() could
        # have made an about-to-be-pruned revision the current one.
        current_now = self._get_meta_no_check_is_deleted(
            resource_id
        ).current_revision_id
        prune_set = [r for r in prune_set if r != current_now]
        if not prune_set:
            return []

        # Keep #370 blob refcounts correct before the data disappears.
        self._decref_resource_blobs(resource_id, only_revisions=set(prune_set))
        self.storage.delete_revisions(resource_id, prune_set)
        return prune_set

    def _decref_resource_blobs(
        self, resource_id: str, *, only_revisions: "set[str] | None" = None
    ) -> None:
        """Best-effort ``decref`` of every blob referenced by *resource_id*'s
        revisions, once per (revision, file_id).

        ``dump_resource`` yields once per (revision, schema_version), so a
        migrated revision appears multiple times; we dedupe by ``revision_id``
        so the decref count matches ``incref`` (which fires once per logical
        revision write).

        When *only_revisions* is given, only those revisions are decref'd —
        used by :meth:`prune_revisions` so pruning a subset of a resource's
        history keeps the #370 blob refcounts correct (the pruned revisions'
        ``incref`` would otherwise never be matched and their blobs leak)."""
        store = self.blob_store
        if store is None or self._binary_processor._collector is None:
            return
        decode = self._data_serializer.decode
        collect = self._binary_processor.collect_file_ids
        decremented: dict[str, set[str]] = {}
        try:
            for info, data_io in self.storage.dump_resource(resource_id):
                if (
                    only_revisions is not None
                    and info.revision_id not in only_revisions
                ):
                    continue
                try:
                    file_ids = collect(decode(data_io.read()))
                except Exception:
                    continue
                seen = decremented.setdefault(info.revision_id, set())
                for file_id in file_ids:
                    if file_id in seen:
                        continue
                    seen.add(file_id)
                    store.decref(file_id)
        except Exception:
            pass

    @execute_with_events(
        (BeforeDump, AfterDump, OnSuccessDump, OnFailureDump),
        "result",
        inputs={"query": UNSET, "strict": UNSET, "stats": UNSET},
    )
    def dump(
        self,
        query: Query | ResourceMetaSearchQuery | None = None,
        *,
        strict: bool = True,
        stats: DumpStats | None = None,
    ) -> Generator[MetaRecord | RevisionRecord | BlobRecord]:
        """Dump metadata, revision data, and blobs as Record objects.

        Args:
            query: Optional QB/search query.  When given, only matching
                resources are exported.  ``None`` exports everything.
            strict: When *True* (the default), a referenced blob that
                cannot be read and a revision whose payload cannot be
                decoded each raise :class:`DumpIncompleteError` instead of
                being skipped.  Both used to be swallowed, so a short
                archive looked like a successful backup.  Pass *False* to
                dump what is readable and inspect *stats* afterwards.
            stats: Optional :class:`DumpStats` to fill in as records are
                yielded.  Counts are only final once the generator is
                exhausted.

        Yields:
            :class:`MetaRecord`, :class:`RevisionRecord`, and
            :class:`BlobRecord` instances.  For each resource the meta
            record is yielded first, immediately followed by all its
            revision records — no intermediate id collection needed.
            Blob records are emitted at the end, in ``file_id`` order so
            two dumps of the same data produce the same bytes.

        Raises:
            DumpIncompleteError: In ``strict`` mode, on the first blob or
                revision that cannot be read.  The archive written so far
                is partial and has no ``EofRecord``, so a later ``load``
                also refuses it (:class:`ArchiveTruncatedError`).
        """
        record_stats = stats if stats is not None else DumpStats()

        # Pre-hoist encoders
        meta_encode = self.meta_serializer.encode
        res_encode = self.resource_serializer.encode
        has_blobs = self.blob_store is not None
        collect = self._binary_processor.collect_file_ids if has_blobs else None
        data_decode = self._data_serializer.decode if has_blobs else None
        blob_file_ids: set[str] = set()

        # Build meta iterator
        if query is not None:
            if isinstance(query, Query):
                query = query.build()
            q = msgspec.structs.replace(query, limit=2**31 - 1, offset=0)
            metas = self.storage.iter_search(q)
        else:
            metas = self.storage.dump_meta(None)

        # --- helpers shared by both fast/slow paths ---
        def _make_rev_record(info, raw_data: bytes):
            return RevisionRecord(
                data=res_encode(RawResource(info=info, raw_data=raw_data))
            )

        def _collect_blobs(raw_data: bytes, info):
            if collect is None or data_decode is None:
                return
            try:
                blob_file_ids.update(collect(data_decode(raw_data)))
            except Exception as e:
                # A revision we cannot decode may reference blobs we will
                # therefore never see, so its attachments quietly stayed
                # out of the archive. Same shape as the blob fetch below:
                # the caller has to be able to find out.
                if strict:
                    raise DumpIncompleteError(
                        self.resource_name,
                        f"revision {info.revision_id}",
                        str(e),
                    ) from e
                record_stats.undecodable_revisions.append(info.revision_id)

        # Bulk pre-fetch (concurrent S3 downloads) needs the whole id set
        # up front, so taking it costs one list of every meta. Ask first:
        # disk and memory have no bulk path and used to pay for that list
        # anyway, which on a large export is the one allocation that scales
        # with the dataset instead of with the batch.
        if getattr(self.storage, "supports_bulk_dump", False):
            metas_list = list(metas)
            rid_set = frozenset(m.resource_id for m in metas_list)
            bulk = self.storage.dump_resources_bulk(resource_ids=rid_set)
        else:
            metas_list = metas
            bulk = None

        if bulk is not None:
            for meta in metas_list:
                yield MetaRecord(data=meta_encode(meta))
                record_stats.metas += 1
                for info, raw_data in bulk.get(meta.resource_id, []):
                    yield _make_rev_record(info, raw_data)
                    record_stats.revisions += 1
                    _collect_blobs(raw_data, info)
        else:
            # Slow path: stream one resource at a time
            dump_resource = self.storage.dump_resource
            for meta in metas_list:
                yield MetaRecord(data=meta_encode(meta))
                record_stats.metas += 1
                for info, data_io in dump_resource(meta.resource_id):
                    raw_data = data_io.read()
                    yield _make_rev_record(info, raw_data)
                    record_stats.revisions += 1
                    _collect_blobs(raw_data, info)

        # Blobs (must come after all revisions so file_ids are fully collected)
        if has_blobs and blob_file_ids:
            blob_store = self.blob_store
            # Sorted so the same data dumps to the same bytes twice;
            # ``blob_file_ids`` is a set and its order is not stable.
            for file_id in sorted(blob_file_ids):
                try:
                    blob = blob_store.get(file_id)
                except Exception as e:
                    if strict:
                        raise DumpIncompleteError(
                            self.resource_name, f"blob {file_id}", str(e)
                        ) from e
                    record_stats.skipped_blobs.append(file_id)
                    continue
                if blob.data is UNSET:
                    # The store answered without bytes — as invisible a
                    # loss as a raise, and previously just as quiet.
                    if strict:
                        raise DumpIncompleteError(
                            self.resource_name,
                            f"blob {file_id}",
                            "store returned no payload",
                        )
                    record_stats.skipped_blobs.append(file_id)
                    continue
                yield BlobRecord(
                    file_id=file_id,
                    blob_data=blob.data,
                    size=blob.size if blob.size is not UNSET else len(blob.data),
                    content_type=blob.content_type
                    if blob.content_type is not UNSET
                    else "",
                )
                record_stats.blobs += 1

    def collect_all_referenced_file_ids(self) -> tuple[set[str], bool]:
        """Scan every revision of every resource for referenced blob file_ids.

        Returns ``(referenced_file_ids, complete)`` (issue #370):

        * ``referenced_file_ids`` — every blob ``file_id`` referenced by any
          stored revision (any resource, including soft-deleted ones, any
          schema version).  This is the authoritative "live" set used by the
          garbage collector's reconcile pass.
        * ``complete`` — ``False`` if any revision failed to decode.  When
          incomplete the caller must **not** treat a missing file_id as proof
          that a blob is unreferenced (it must skip irreversible deletion),
          because an un-decodable revision may reference blobs we could not
          see.

        Returns ``(set(), True)`` when the model has no blob store or no
        ``Binary`` fields.
        """
        if self.blob_store is None or self._binary_processor._collector is None:
            return set(), True
        collect = self._binary_processor.collect_file_ids
        data_decode = self._data_serializer.decode
        result: set[str] = set()
        complete = True

        metas_list = list(self.storage.dump_meta(None))
        rid_set = frozenset(m.resource_id for m in metas_list)
        bulk = self.storage.dump_resources_bulk(resource_ids=rid_set)

        def _accumulate(raw_data: bytes) -> None:
            nonlocal complete
            try:
                result.update(collect(data_decode(raw_data)))
            except Exception:
                complete = False

        if bulk is not None:
            for meta in metas_list:
                for _info, raw_data in bulk.get(meta.resource_id, []):
                    _accumulate(raw_data)
        else:
            for meta in metas_list:
                for _info, data_io in self.storage.dump_resource(meta.resource_id):
                    _accumulate(data_io.read())

        return result, complete

    @execute_with_events(
        (BeforeLoad, AfterLoad, OnSuccessLoad, OnFailureLoad),
        lambda _: {},
        inputs={"bio": UNSET},
    )
    def load(self, record_type: str, bio: IO[bytes]) -> None:
        """Legacy load interface — load a single keyed item.

        .. deprecated::
            Use :meth:`load_record` with :class:`DumpRecord` objects instead.
        """
        if record_type.startswith("meta/"):
            self.storage.save_meta(self.meta_serializer.decode(bio.read()))
        elif record_type.startswith("data/"):
            raw_res = self.resource_serializer.decode(bio.read())
            self.storage.save_revision(raw_res.info, io.BytesIO(raw_res.raw_data))
        elif record_type.startswith("blob/"):
            blob_entry = self._blob_serializer.decode(bio.read())
            if self.blob_store is not None:
                self.blob_store.put(
                    blob_entry.data, content_type=blob_entry.content_type
                )

    def load_record(
        self,
        record: "MetaRecord | RevisionRecord | BlobRecord",
        on_duplicate: "OnDuplicate" = OnDuplicate.raise_error,
    ) -> bool:
        """Load a single :class:`DumpRecord` into storage.

        Args:
            record: A :class:`MetaRecord`, :class:`RevisionRecord`, or
                :class:`BlobRecord` instance (typically produced by
                :meth:`dump`).
            on_duplicate: Strategy when a resource with the same ID already
                exists.  Only meaningful for :class:`MetaRecord`; revision
                and blob records are always written.

        Returns:
            ``True`` if the record was stored, ``False`` if it was skipped
            (only possible when *on_duplicate* is :attr:`OnDuplicate.skip`).

        Raises:
            DuplicateResourceError: When the resource already exists and
                *on_duplicate* is :attr:`OnDuplicate.raise_error`.
        """
        record_type = type(record).__name__
        ctx_kw: _LoadCtxKw = {
            "user": self.user_or_unset,
            "now": self.now_or_unset,
            "resource_name": self.resource_name,
            "record_type": record_type,
        }
        self._handle_event(BeforeLoad(**ctx_kw))
        try:
            result = self._load_record_impl(record, on_duplicate)
            self._handle_event(OnSuccessLoad(**ctx_kw))
            return result
        except Exception as e:
            self._handle_event(
                OnFailureLoad(
                    **ctx_kw,
                    error=str(e),
                    stack_trace=traceback.format_exc(),
                )
            )
            raise
        finally:
            self._handle_event(AfterLoad(**ctx_kw))

    def _load_record_impl(
        self,
        record: "MetaRecord | RevisionRecord | BlobRecord",
        on_duplicate: "OnDuplicate",
    ) -> bool:
        """Internal implementation of load_record."""
        if isinstance(record, MetaRecord):
            meta = self.meta_serializer.decode(record.data)
            exists = self.storage.exists(meta.resource_id)
            if exists:
                if on_duplicate is OnDuplicate.skip:
                    return False
                if on_duplicate is OnDuplicate.raise_error:
                    raise DuplicateResourceError(meta.resource_id)
                # OnDuplicate.overwrite — fall through and save
            self.storage.save_meta(meta)
            return True

        if isinstance(record, RevisionRecord):
            raw_res = self.resource_serializer.decode(record.data)
            self.storage.save_revision(raw_res.info, io.BytesIO(raw_res.raw_data))
            return True

        if isinstance(record, BlobRecord):
            if self.blob_store is not None:
                self.blob_store.put(
                    record.blob_data,
                    content_type=record.content_type,
                )
            return True

        return True

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    def load_records_bulk(
        self,
        meta_records: "list[MetaRecord]",
        revision_records: "list[RevisionRecord]",
        blob_records: "list[BlobRecord]",
        on_duplicate: "OnDuplicate" = OnDuplicate.raise_error,
        *,
        skipped_ids: "set[str] | None" = None,
    ):
        """Batch-load multiple dump records into storage.

        Instead of writing each record one-by-one (which incurs per-call
        overhead especially on remote backends like S3 / PostgreSQL), this
        method:

        1. Decodes **all** meta records, applies the *on_duplicate*
           strategy, and bulk-saves them via
           :meth:`SimpleStorage.save_metas_bulk`.
        2. Decodes **all** revision records and bulk-saves them via
           :meth:`SimpleStorage.save_revisions_bulk` (which may use
           concurrent I/O on S3).
        3. Writes blob records sequentially (typically few in number).

        Events (Before/After/OnSuccess/OnFailure Load) are fired **once
        per batch**, not per record.

        Args:
            skipped_ids: Resource ids already skipped by earlier batches of
                the same archive section. Their revisions in this batch are
                dropped, and ids skipped here are added to the set, so a
                caller that splits one section over several calls keeps the
                ``OnDuplicate.skip`` contract intact. ``None`` (the default)
                scopes the set to this call.

        Returns:
            A :class:`LoadStats` instance with *loaded*, *skipped* and
            *total* counts (counted per distinct ``resource_id``).
        """
        from specstar.crud.core import LoadStats as _LoadStats

        stats = _LoadStats()

        # --- fire BeforeLoad once for the batch --------------------------
        ctx_kw: _LoadCtxKw = {
            "user": self.user_or_unset,
            "now": self.now_or_unset,
            "resource_name": self.resource_name,
            "record_type": "bulk",
        }
        self._handle_event(BeforeLoad(**ctx_kw))

        try:
            # --- 1. decode + deduplicate metas ----------------------------
            metas_to_save: list[ResourceMeta] = []
            if skipped_ids is None:
                skipped_ids = set()

            for rec in meta_records:
                meta = self.meta_serializer.decode(rec.data)
                stats.total += 1
                exists = self.storage.exists(meta.resource_id)
                if exists:
                    if on_duplicate is OnDuplicate.skip:
                        stats.skipped += 1
                        skipped_ids.add(meta.resource_id)
                        continue
                    if on_duplicate is OnDuplicate.raise_error:
                        raise DuplicateResourceError(meta.resource_id)
                metas_to_save.append(meta)
                stats.loaded += 1

            # bulk write metas
            self.storage.save_metas_bulk(metas_to_save)

            # --- 2. decode + bulk write revisions -------------------------
            revisions_to_save: list[tuple[RevisionInfo, bytes]] = []
            for rec in revision_records:
                raw_res = self.resource_serializer.decode(rec.data)
                if raw_res.info.resource_id in skipped_ids:
                    continue
                revisions_to_save.append((raw_res.info, raw_res.raw_data))

            self.storage.save_revisions_bulk(revisions_to_save)

            # --- 3. blob records (sequential — usually few) ---------------
            if self.blob_store is not None:
                for rec in blob_records:
                    self.blob_store.put(
                        rec.blob_data,
                        content_type=rec.content_type,
                    )

            self._handle_event(OnSuccessLoad(**ctx_kw))
        except Exception as e:
            self._handle_event(
                OnFailureLoad(
                    **ctx_kw,
                    error=str(e),
                    stack_trace=traceback.format_exc(),
                )
            )
            raise
        finally:
            self._handle_event(AfterLoad(**ctx_kw))

        return stats

    @cached_property
    def meta_serializer(self):
        return MsgspecSerializer(encoding=Encoding.msgpack, resource_type=ResourceMeta)

    @cached_property
    def resource_serializer(self):
        return MsgspecSerializer(
            encoding=Encoding.msgpack,
            resource_type=RawResource,
        )

    @cached_property
    def _blob_serializer(self):
        return MsgspecSerializer(encoding=Encoding.msgpack, resource_type=_BlobEntry)

    @cached_property
    def _binary_processor(self) -> BinaryProcessor:
        return BinaryProcessor(self._resource_type)
