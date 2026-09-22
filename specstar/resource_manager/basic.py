import datetime as dt
import functools
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Iterable, MutableMapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from enum import Flag, StrEnum
from typing import IO, Any, Generic, TypeVar

import msgspec
from msgspec import UNSET, Struct, UnsetType

from specstar.query_types import (
    AggKeyRef,
    AggSpec,
    DataSearchCondition,
    DataSearchGroup,
    DataSearchLogicOperator,
    DataSearchOperator,
    DataSearchQuantifier,
    ResourceDataSearchSort,
    ResourceMetaSearchQuery,
    ResourceMetaSearchSort,
    ResourceMetaSortDirection,
    TrigramFuzzyCondition,
    TrigramSimilaritySort,
    VectorDistanceCondition,
    VectorDistanceSort,
)
from specstar.query_types import ResourceMetaSortKey as ResourceMetaSortKey
from specstar.types import (
    Binary,
    BlobResponse,
    BlobStreamInfo,
    BlobUploadSession,
    ResourceMeta,
    RevisionInfo,
)
from specstar.util.datetime_utils import ensure_aware

T = TypeVar("T")


class NoDefaultType:
    pass


NO_DEFAULT = NoDefaultType()


class Ctx(Generic[T]):
    def __init__(
        self,
        name: str,
        *,
        strict_type: type[T] | UnsetType = UNSET,
        default: T | NoDefaultType = NO_DEFAULT,
        default_factory: Callable[[], T] | NoDefaultType = NO_DEFAULT,
    ):
        self.strict_type = strict_type
        self.v = ContextVar[T](name)
        self.default = default
        self.default_factory = default_factory

    @contextmanager
    def ctx(self, value: T | UnsetType):
        if self.strict_type is not UNSET and not isinstance(value, self.strict_type):
            raise TypeError(f"Context value must be of type {self.strict_type}")
        token = self.v.set(value)  # ty:ignore[invalid-argument-type]
        try:
            yield
        finally:
            self.v.reset(token)

    def get(self) -> T:
        if self.default is NO_DEFAULT and self.default_factory is NO_DEFAULT:
            return self.v.get()
        if self.default_factory is not NO_DEFAULT:
            return self.v.get(self.default_factory())  # ty:ignore[call-non-callable]
        return self.v.get(self.default)  # ty:ignore[invalid-return-type]


class Encoding(StrEnum):
    json = "json"
    msgpack = "msgpack"


def is_match_query(meta: ResourceMeta, query: ResourceMetaSearchQuery) -> bool:
    if query.is_deleted is not UNSET and meta.is_deleted != query.is_deleted:
        return False

    if (
        query.created_time_start is not UNSET
        and meta.created_time < query.created_time_start
    ):
        return False
    if (
        query.created_time_end is not UNSET
        and meta.created_time > query.created_time_end
    ):
        return False
    if (
        query.updated_time_start is not UNSET
        and meta.updated_time < query.updated_time_start
    ):
        return False
    if (
        query.updated_time_end is not UNSET
        and meta.updated_time > query.updated_time_end
    ):
        return False

    if query.created_bys is not UNSET and meta.created_by not in query.created_bys:
        return False
    if query.updated_bys is not UNSET and meta.updated_by not in query.updated_bys:
        return False

    # rev_* filters: a meta whose rev_* field is UNSET (legacy / not
    # backfilled) cannot satisfy the filter — treat as no-match so that
    # callers don't get spurious hits with missing data.
    if query.rev_statuses is not UNSET:
        if meta.rev_status is UNSET or meta.rev_status not in query.rev_statuses:
            return False
    if query.rev_created_bys is not UNSET:
        if (
            meta.rev_created_by is UNSET
            or meta.rev_created_by not in query.rev_created_bys
        ):
            return False
    if query.rev_updated_bys is not UNSET:
        if (
            meta.rev_updated_by is UNSET
            or meta.rev_updated_by not in query.rev_updated_bys
        ):
            return False
    if query.rev_created_time_start is not UNSET:
        if (
            meta.rev_created_time is UNSET
            or meta.rev_created_time < query.rev_created_time_start
        ):
            return False
    if query.rev_created_time_end is not UNSET:
        if (
            meta.rev_created_time is UNSET
            or meta.rev_created_time > query.rev_created_time_end
        ):
            return False
    if query.rev_updated_time_start is not UNSET:
        if (
            meta.rev_updated_time is UNSET
            or meta.rev_updated_time < query.rev_updated_time_start
        ):
            return False
    if query.rev_updated_time_end is not UNSET:
        if (
            meta.rev_updated_time is UNSET
            or meta.rev_updated_time > query.rev_updated_time_end
        ):
            return False

    if query.conditions is not UNSET:
        for condition in query.conditions:
            if isinstance(condition, VectorDistanceCondition):
                if not _match_vector_condition(meta, condition):
                    return False
                continue
            if isinstance(condition, TrigramFuzzyCondition):
                if not _match_fuzzy_condition(meta, condition):
                    return False
                continue
            if not _match_condition(meta, condition):
                return False

    if query.data_conditions is not UNSET and meta.indexed_data is not UNSET:
        for condition in query.data_conditions:
            if not _match_data_condition(meta.indexed_data, condition):
                return False
    elif query.data_conditions is not UNSET:
        # 如果有 data 條件但沒有索引資料，不匹配
        return False

    return True


def _match_condition(
    meta: ResourceMeta,
    condition: DataSearchCondition | DataSearchGroup,
) -> bool:
    """檢查 meta 是否匹配條件"""
    result = _evaluate_trivalent(meta, condition)
    return result is True


def _match_vector_condition(
    meta: ResourceMeta,
    condition: VectorDistanceCondition,
) -> bool:
    """Brute-force evaluation of a VectorDistanceCondition against indexed_data."""
    from specstar.util.vector_distance import distance

    if meta.indexed_data is UNSET:
        return False
    vec = meta.indexed_data.get(condition.field_path)
    if not isinstance(vec, list):
        return False
    if not isinstance(condition.query_vector, list):
        # str query_vector must have been resolved to a vector before reaching here
        return False
    metric = condition.distance or "cosine"
    d = distance(vec, condition.query_vector, metric)
    op = condition.operator
    if op == DataSearchOperator.less_than:
        return d < condition.threshold
    if op == DataSearchOperator.less_than_or_equal:
        return d <= condition.threshold
    if op == DataSearchOperator.greater_than:
        return d > condition.threshold
    if op == DataSearchOperator.greater_than_or_equal:
        return d >= condition.threshold
    return False


def _fuzzy_field_text(value: Any) -> str | None:
    """The text pg_trgm's ``->>`` would expose for a field, for trigram matching.

    A scalar string is itself; a list joins its elements so each is a separate
    word — the same words Postgres' serialised-array text (``["a", "b"]``) splits
    into, since every quote / bracket / comma is a trigram boundary. Anything else
    (not a text or ``text[]`` field) is ``None`` — no fuzzy match.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(str(el) for el in value)
    return None


def _match_fuzzy_condition(
    meta: ResourceMeta,
    condition: TrigramFuzzyCondition,
) -> bool:
    """Brute-force evaluation of a :class:`TrigramFuzzyCondition` (``.fuzzy()``).

    Mirrors Postgres' ``word_similarity(query, indexed_data->>'f') >= threshold``
    (default :data:`~specstar.util.trigram.WORD_SIMILARITY_THRESHOLD`). The
    :mod:`specstar.util.trigram` port is bit-for-bit identical to pg_trgm, so an
    empty / missing / non-text field never reaches the threshold and is excluded.
    """
    from specstar.util.trigram import WORD_SIMILARITY_THRESHOLD, word_similarity

    if meta.indexed_data is UNSET:
        return False
    text = _fuzzy_field_text(meta.indexed_data.get(condition.field_path))
    if text is None:
        return False
    threshold = (
        WORD_SIMILARITY_THRESHOLD
        if condition.threshold is None
        else condition.threshold
    )
    return word_similarity(condition.query, text) >= threshold


def _match_data_condition(
    indexed_data: dict[str, Any],
    condition: DataSearchCondition | DataSearchGroup,
) -> bool:
    """檢查索引資料是否匹配 data 條件"""
    result = _evaluate_trivalent(indexed_data, condition)
    return result is True


def _scalar_op(op: DataSearchOperator, element: Any, compare_value: Any) -> bool:
    """Apply a *scalar* string/comparison operator to a single list element.

    This is the per-element predicate behind the ``any``/``all`` quantifier: the
    element is a scalar, so ``contains`` is substring (not the whole-array
    membership that :data:`DataSearchOperator.contains` means on a list) and
    ``regex`` anchors ``^``/``$`` to this one element. It mirrors the scalar
    branches of :func:`_evaluate_trivalent`, kept in sync with them.
    """
    if op == DataSearchOperator.equals:
        return element == compare_value
    if op == DataSearchOperator.not_equals:
        return element != compare_value
    if op == DataSearchOperator.contains:
        return str(compare_value) in str(element)
    if op == DataSearchOperator.starts_with:
        return str(element).startswith(str(compare_value))
    if op == DataSearchOperator.ends_with:
        return str(element).endswith(str(compare_value))
    if op == DataSearchOperator.regex:
        return re.search(str(compare_value), str(element)) is not None
    # ElementQuantifier never emits any other operator; be conservative.
    return False


def _match_quantified(
    quantifier: DataSearchQuantifier,
    op: DataSearchOperator,
    field_value: Any,
    compare_value: Any,
) -> bool | None:
    """Fold a per-element scalar predicate with an ``any``/``all`` quantifier.

    ``any`` is existential (empty list -> ``False``); ``all`` is universal
    (empty list -> vacuously ``True``). A non-list value is Unknown (``None``)
    — quantifying over a scalar is a category error.
    """
    if not isinstance(field_value, list):
        return None
    results = [_scalar_op(op, el, compare_value) for el in field_value]
    if quantifier == DataSearchQuantifier.any:
        return any(results)
    return all(results)


# Bare scalar-string operators that are meaningless on a list field: without a
# quantifier they would run against the whole serialised array (a cross-element,
# backend-divergent, index-blind footgun), so they must go through .any()/.all().
# ``contains`` stays valid on a list (exact element membership, #362) and is not
# listed here.
_LIST_SCALAR_ONLY_OPS = frozenset(
    {
        DataSearchOperator.regex,
        DataSearchOperator.starts_with,
        DataSearchOperator.ends_with,
    }
)


def bad_list_string_op(field_path: str, operator: DataSearchOperator) -> ValueError:
    """The error raised when a bare scalar string op targets a list field."""
    return ValueError(
        f"{operator.value!r} on the list field {field_path!r} is not allowed: a "
        f"scalar string operator would run against the whole serialised array. "
        f"Quantify over the elements instead, e.g. "
        f"QB[{field_path!r}].any().{operator.value}(...) "
        f"(or .all(), or .icontains/.istarts_with/.iends_with through .any())."
    )


def reject_unquantified_list_string_ops(
    query: ResourceMetaSearchQuery, list_fields: Iterable[str]
) -> None:
    """Raise :func:`bad_list_string_op` if *query* applies a bare scalar string
    operator to a registered list field without ``.any()``/``.all()``.

    A side-effect-free validation the reference (pure-Python) backends run once
    per query, before iterating — so an empty store rejects a bad query just
    like the SQL backends, which raise the same error from ``_build_condition``.
    """
    fields = set(list_fields)
    if not fields:
        return

    def _walk(cond: object) -> None:
        if isinstance(cond, DataSearchGroup):
            for sub in cond.conditions:
                _walk(sub)
        elif isinstance(cond, DataSearchCondition):
            if (
                cond.quantifier is None
                and cond.operator in _LIST_SCALAR_ONLY_OPS
                and cond.field_path in fields
            ):
                raise bad_list_string_op(cond.field_path, cond.operator)

    for bucket in (query.conditions, query.data_conditions):
        if bucket is not UNSET:
            for c in bucket:
                _walk(c)


def _evaluate_trivalent(
    data: dict[str, Any] | ResourceMeta,
    condition: DataSearchCondition
    | DataSearchGroup
    | VectorDistanceCondition
    | TrigramFuzzyCondition,
) -> bool | None:
    """
    Evaluate condition using SQL-like trivalent logic (True, False, Unknown/None).
    Unknown is returned for operations on missing keys or NULL values (except is_null/exists/isna).
    """
    if isinstance(condition, VectorDistanceCondition):
        # Vector conditions only make sense against full ResourceMeta (with
        # indexed_data); when nested inside a data-condition group we
        # synthesize a ResourceMeta-like wrapper.
        if isinstance(data, ResourceMeta):
            return _match_vector_condition(data, condition)
        if isinstance(data, dict):
            # Treat as indexed_data dict
            class _Wrap:
                pass

            wrapper = _Wrap()
            wrapper.indexed_data = data  # type: ignore[attr-defined]
            return _match_vector_condition(wrapper, condition)  # type: ignore[arg-type]
        return None

    if isinstance(condition, TrigramFuzzyCondition):
        # Like vector conditions, a fuzzy condition can be AND/OR-combined with
        # other filters (``QB[...] == v) & QB[...].fuzzy(q)``), so it must be
        # evaluated when reached INSIDE a group too — not only at the top level.
        # It reads the same indexed_data; wrap a bare dict when nested.
        if isinstance(data, ResourceMeta):
            return _match_fuzzy_condition(data, condition)
        if isinstance(data, dict):

            class _Wrap:
                pass

            wrapper = _Wrap()
            wrapper.indexed_data = data  # type: ignore[attr-defined]
            return _match_fuzzy_condition(wrapper, condition)  # type: ignore[arg-type]
        return None

    if isinstance(condition, DataSearchGroup):
        results = [
            _evaluate_trivalent(data, sub_cond) for sub_cond in condition.conditions
        ]

        if condition.operator == DataSearchLogicOperator.and_op:
            # AND: False if any False. Unknown if any Unknown (and no False). True if all True.
            if any(r is False for r in results):
                return False
            if any(r is None for r in results):
                return None
            return True

        if condition.operator == DataSearchLogicOperator.or_op:
            # OR: True if any True. Unknown if any Unknown (and no True). False if all False.
            if any(r is True for r in results):
                return True
            if any(r is None for r in results):
                return None
            return False

        if condition.operator == DataSearchLogicOperator.not_op:
            # NOT: True->False, False->True, Unknown->Unknown
            # Implicitly ANDs the conditions if multiple
            if any(r is False for r in results):
                return True
            if any(r is None for r in results):
                return None
            return False

        return None

    # Leaf Condition

    # Helper to check existence and get value
    val = UNSET
    has_key = False
    actual_field_path = condition.field_path

    if isinstance(data, dict):
        if actual_field_path in data:
            has_key = True
            val = data[actual_field_path]  # ty:ignore[invalid-argument-type]
    elif isinstance(data, ResourceMeta):
        if hasattr(data, actual_field_path) and actual_field_path != "indexed_data":
            has_key = True
            val = getattr(data, actual_field_path)
        elif data.indexed_data is not UNSET and actual_field_path in data.indexed_data:
            has_key = True
            val = data.indexed_data[actual_field_path]

    # Apply field transformation if specified
    field_value = val
    if has_key and val is not None and condition.transform is not None:
        from specstar.query_types import FieldTransform

        if condition.transform == FieldTransform.length:
            try:
                field_value = len(val)  # ty:ignore[invalid-argument-type]
            except TypeError:
                # Value doesn't support len(), treat as Unknown
                return None
    elif has_key:
        field_value = val
    else:
        field_value = UNSET

    # 1. Handle operators that work on missing keys or don't care about value
    if condition.operator == DataSearchOperator.exists:
        return has_key if condition.value else not has_key

    if condition.operator == DataSearchOperator.isna:
        # isna = not exist or is null
        if not has_key:
            return condition.value  # True if checking isna=True
        is_na = val is None
        return is_na == condition.value

    # 2. Handle missing keys for other operators -> Unknown
    if not has_key:
        return None

    # 3. Handle NULL values for other operators
    if field_value is None:
        # is_null is the only operator that handles NULL value gracefully (besides exists/isna)
        if condition.operator == DataSearchOperator.is_null:
            return condition.value  # True if checking is_null=True

        # All other comparisons with NULL return Unknown
        return None

    # 4. Handle standard operators on present, non-null values
    if condition.operator == DataSearchOperator.is_null:
        # Value is not None, so is_null is False
        return not condition.value

    # Normalize Enum to value for comparison (indexed data stores Enum as value string/int)
    from enum import Enum as EnumType

    if isinstance(condition.value, EnumType):
        compare_value = condition.value.value
    elif isinstance(condition.value, (list, tuple, set)):
        # Handle Enum in lists (for in_list/not_in_list operators)
        compare_value = type(condition.value)(
            v.value if isinstance(v, EnumType) else v for v in condition.value
        )
    else:
        compare_value = condition.value

    # Element-wise quantifier (``QB[...].any().<op>(...)``): apply the operator
    # to each list element as a scalar and fold with any/all.
    if condition.quantifier is not None:
        return _match_quantified(
            condition.quantifier, condition.operator, field_value, compare_value
        )

    if condition.operator == DataSearchOperator.equals:
        return field_value == compare_value
    if condition.operator == DataSearchOperator.not_equals:
        return field_value != compare_value
    if condition.operator == DataSearchOperator.greater_than:
        return field_value > compare_value
    if condition.operator == DataSearchOperator.greater_than_or_equal:
        return field_value >= compare_value
    if condition.operator == DataSearchOperator.less_than:
        return field_value < compare_value
    if condition.operator == DataSearchOperator.less_than_or_equal:
        return field_value <= compare_value
    if condition.operator == DataSearchOperator.contains:
        # 特殊處理：如果 field_value 是列表，檢查 condition.value 是否在列表中
        if isinstance(field_value, list):
            return compare_value in field_value
        if isinstance(condition.value, Flag) and isinstance(field_value, int):
            return (condition.value.value & field_value) == condition.value.value
        # 標準字符串包含檢查
        return str(compare_value) in str(field_value)
    if condition.operator == DataSearchOperator.contains_any:
        # list field shares any element with the candidate values (set overlap);
        # a scalar field degrades to membership (field in candidates).
        cands = (
            compare_value
            if isinstance(compare_value, (list, tuple, set))
            else [compare_value]
        )
        if isinstance(field_value, list):
            return any(v in field_value for v in cands)
        return field_value in cands
    if condition.operator == DataSearchOperator.starts_with:
        return str(field_value).startswith(
            str(compare_value),
        )
    if condition.operator == DataSearchOperator.ends_with:
        return str(field_value).endswith(
            str(compare_value),
        )
    if condition.operator == DataSearchOperator.regex:
        return re.search(str(compare_value), str(field_value)) is not None
    if condition.operator == DataSearchOperator.in_list:
        return (
            field_value in compare_value
            if isinstance(compare_value, (list, tuple, set))
            else False
        )
    if condition.operator == DataSearchOperator.not_in_list:
        return (
            field_value not in compare_value
            if isinstance(compare_value, (list, tuple, set))
            else True
        )

    return None


def bool_to_sign(b: bool) -> int:
    return 1 if b else -1


def _compare_sort_values(v1: Any, v2: Any) -> int:
    """Compare sort values with stable handling for missing and datetime values."""
    missing1 = v1 is UNSET or v1 is None
    missing2 = v2 is UNSET or v2 is None

    if missing1 and missing2:
        return 0
    if missing1:
        return -1
    if missing2:
        return 1

    if isinstance(v1, dt.datetime) and isinstance(v2, dt.datetime):
        v1 = ensure_aware(v1)
        v2 = ensure_aware(v2)

    if v1 == v2:
        return 0

    return bool_to_sign(v1 > v2)


def get_sort_fn(qsorts: list[ResourceMetaSearchSort | ResourceDataSearchSort]):
    """Return a ``functools.cmp_to_key`` comparator for *qsorts*.

    Datetime fields are normalised to timezone-aware UTC values via
    :func:`~specstar.util.datetime_utils.ensure_aware` so that mixed
    naive/aware ``created_time`` or ``updated_time`` values never cause
    a ``TypeError``.
    """

    def compare(meta1: ResourceMeta, meta2: ResourceMeta) -> int:
        for sort in qsorts:
            if isinstance(sort, ResourceMetaSearchSort):
                v1 = getattr(meta1, sort.key.value)
                v2 = getattr(meta2, sort.key.value)
            elif isinstance(sort, VectorDistanceSort):
                from specstar.util.vector_distance import distance

                metric = sort.distance or "cosine"
                if not isinstance(sort.query_vector, list):
                    return 0  # unresolved str query_vector — punt
                v1_raw = (
                    meta1.indexed_data.get(sort.field_path)
                    if meta1.indexed_data is not UNSET
                    else None
                )
                v2_raw = (
                    meta2.indexed_data.get(sort.field_path)
                    if meta2.indexed_data is not UNSET
                    else None
                )
                v1 = (
                    distance(v1_raw, sort.query_vector, metric)
                    if isinstance(v1_raw, list)
                    else UNSET
                )
                v2 = (
                    distance(v2_raw, sort.query_vector, metric)
                    if isinstance(v2_raw, list)
                    else UNSET
                )
            elif isinstance(sort, TrigramSimilaritySort):
                from specstar.util.trigram import word_similarity

                text1 = _fuzzy_field_text(
                    meta1.indexed_data.get(sort.field_path)
                    if meta1.indexed_data is not UNSET
                    else None
                )
                text2 = _fuzzy_field_text(
                    meta2.indexed_data.get(sort.field_path)
                    if meta2.indexed_data is not UNSET
                    else None
                )
                v1 = word_similarity(sort.query, text1) if text1 is not None else UNSET
                v2 = word_similarity(sort.query, text2) if text2 is not None else UNSET
            else:
                v1 = (
                    meta1.indexed_data.get(sort.field_path)
                    if meta1.indexed_data is not UNSET
                    else UNSET
                )
                v2 = (
                    meta2.indexed_data.get(sort.field_path)
                    if meta2.indexed_data is not UNSET
                    else UNSET
                )

            cmp = _compare_sort_values(v1, v2)
            if cmp != 0:
                return cmp * (
                    1 if sort.direction == ResourceMetaSortDirection.ascending else -1
                )
        return 0

    return functools.cmp_to_key(compare)


class MsgspecSerializer(Generic[T]):
    def __init__(self, encoding: Encoding, resource_type: type[T]):
        self.encoding = encoding
        if self.encoding == "msgpack":
            self.encoder = msgspec.msgpack.Encoder(order="deterministic")
            self.decoder = msgspec.msgpack.Decoder(resource_type)
        else:
            self.encoder = msgspec.json.Encoder(order="deterministic")
            self.decoder = msgspec.json.Decoder(resource_type)

    def encode(self, obj: T) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, b: bytes) -> T:
        return self.decoder.decode(b)

    def decode_to_builtins(self, b: bytes):
        """Decode to plain Python builtins (no target type), using the
        configured codec. Best-effort recovery when the typed :meth:`decode`
        fails (e.g. an incompatible schema change)."""
        if self.encoding == "msgpack":
            return msgspec.msgpack.decode(b)
        return msgspec.json.decode(b)

    def decode_and_validate(self, b: bytes) -> None:
        """Decode and validate the data by re-encoding it and comparing hashes."""
        self.decode(b)


class IMetaStore(MutableMapping[str, ResourceMeta]):
    """Interface for a metadata store that manages resource metadata.

    This interface provides a dictionary-like interface for storing and retrieving
    resource metadata, with additional search capabilities. It serves as the primary
    storage mechanism for ResourceMeta objects in the SpecStar system.

    The store can be used like a standard Python dictionary, with resource IDs as keys
    and ResourceMeta objects as values. It extends the MutableMapping interface with
    search functionality to support complex queries.

    See: https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping
    """

    def register_list_field(self, field_path: str) -> None:
        """Declare *field_path* as a list-typed indexed field (see #362).

        Base default: record the name so a bare scalar string operator on it is
        rejected in favour of ``.any()``/``.all()`` (see
        :func:`reject_unquantified_list_string_ops`). SQL stores override this to
        *also* route ``contains`` through element membership. Idempotent — safe
        to call from ``add_model`` on every registration.
        """
        try:
            self._list_fields.add(field_path)  # ty: ignore[unresolved-attribute]
        except AttributeError:
            self._list_fields = {field_path}

    def _registered_list_fields(self) -> "set[str] | frozenset[str]":
        """Names declared list-typed via :meth:`register_list_field` (empty when
        none were, e.g. a store built directly in a test)."""
        return getattr(self, "_list_fields", frozenset())

    @abstractmethod
    def __getitem__(self, pk: str) -> ResourceMeta:
        """Get resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to retrieve metadata for.

        Returns:
            ResourceMeta: The metadata object for the specified resource.

        Raises:
            KeyError: If the resource ID does not exist in the store.
        """

    @abstractmethod
    def __setitem__(self, pk: str, b: ResourceMeta) -> None:
        """Store resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to store metadata under.
            b (ResourceMeta): The metadata object to store.

        ---
        This method stores or updates the metadata for a resource. If the resource ID
        already exists, the metadata will be replaced. The implementation should ensure
        the metadata is persisted according to the store's persistence strategy.
        """

    @abstractmethod
    def __delitem__(self, pk: str) -> None:
        """Delete resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to delete metadata for.

        Raises:
            KeyError: If the resource ID does not exist in the store.

        ---
        This method permanently removes the metadata for a resource from the store.
        Note that this is different from soft deletion - this completely removes
        the metadata record from storage.
        """

    @abstractmethod
    def __iter__(self) -> Generator[str]:
        """Iterate over all resource IDs in the store.

        Returns:
            Generator[str]: A generator yielding all resource IDs in the store.

        ---
        This method allows iteration over all resource IDs currently stored in the
        metadata store. The order of iteration may vary depending on the implementation.
        """

    @abstractmethod
    def __len__(self) -> int:
        """Return the total number of resources in the store.

        Returns:
            int: The count of all resource metadata records in the store.

        ---
        This method provides the total count of all resource metadata records,
        including both active and soft-deleted resources.
        """

    @abstractmethod
    def iter_search(self, query: ResourceMetaSearchQuery) -> Generator[ResourceMeta]:
        """Search for resource metadata based on query criteria.

        Arguments:
            query (ResourceMetaSearchQuery): The search criteria including filters,
                sorting, and pagination options.

        Returns:
            Generator[ResourceMeta]: A generator yielding ResourceMeta objects that
                match the query criteria.

        ---
        This method performs a search across all resource metadata using the provided
        query parameters. The query supports:
        - Filtering by deletion status, timestamps, and user information
        - Data content filtering based on indexed_data fields
        - Sorting by metadata fields or data content fields
        - Pagination with limit and offset

        The results are yielded in the order specified by the sort criteria and
        limited by the pagination parameters.
        """

    def save_many(self, metas: Iterable[ResourceMeta]) -> None:
        """Optional bulk save. Default loops and uses ``__setitem__``.

        ``ISlowMetaStore`` requires this to be overridden for batch
        durability semantics; the default here lets callers invoke
        ``store.save_many(...)`` on any meta store without checking
        ``hasattr``.
        """
        for m in metas:
            self[m.resource_id] = m

    def get_many(self, pks: "Sequence[str]") -> "dict[str, ResourceMeta]":
        """Optional bulk read — the read-side twin of :meth:`save_many` (#434).

        A fan-out re-reads every row's meta right before writing it, both to
        build the new revision and to notice a concurrent writer. Without a
        bulk path that is one round-trip per row, which is exactly the cost
        batching set out to remove.

        Missing keys are **omitted** rather than raised: the caller is
        reconciling a set and needs to see which members are gone, not to be
        stopped by the first one.
        """
        out: dict[str, ResourceMeta] = {}
        for pk in pks:
            try:
                out[pk] = self[pk]
            except KeyError:
                continue
        return out

    @property
    def supports_native_vector_search(self) -> bool:
        """Whether this meta store can run vector searches natively (vs brute-force).

        Default is ``False``: subclasses that have native vector indexing
        (e.g. PostgresMetaStore with pgvector) override to return ``True``.
        """
        return False


class IFastMetaStore(IMetaStore):
    """Interface for a fast, temporary metadata store with bulk operations.

    This interface extends IMetaStore with additional capabilities for high-performance
    temporary storage. It's designed for scenarios where metadata needs to be quickly
    stored and then bulk-transferred to a slower, more persistent store.

    Fast meta stores are typically implemented using in-memory storage (like Redis or
    memory-based stores) and provide optimized performance for frequent read/write
    operations at the cost of potential data loss if the store goes down.

    The key feature is the get_then_delete operation which enables efficient bulk
    synchronization with slower storage systems while maintaining data consistency.
    """

    @abstractmethod
    @contextmanager
    def get_then_delete(self) -> Generator[Iterable[ResourceMeta]]:
        """Atomically retrieve all metadata and mark it for deletion.

        Returns:
            Generator[Iterable[ResourceMeta]]: A context manager that yields an
                iterable of all ResourceMeta objects currently in the store.

        ---
        This method provides an atomic operation that:
        1. Retrieves all metadata currently stored in the fast store
        2. Marks that metadata for deletion
        3. Actually deletes it only if the context exits successfully

        If an exception occurs within the context, the metadata will NOT be deleted,
        ensuring data consistency during bulk transfer operations.

        Use Cases:
        - Bulk synchronization from fast storage to slow storage
        - Batch processing of accumulated metadata
        - Atomic transfer operations between storage tiers

        Example:
            ```python
            with fast_store.get_then_delete() as metas:
                slow_store.save_many(metas)
                # Metadata is deleted from fast store only if save_many succeeds
            ```

        The deletion occurs only after the context manager exits successfully,
        providing transactional semantics for bulk operations.
        """


class ISlowMetaStore(IMetaStore):
    """Interface for a persistent, durable metadata store with batch operations.

    This interface extends IMetaStore with capabilities optimized for persistent
    storage systems. Slow meta stores prioritize data durability and consistency
    over raw performance, making them suitable for long-term metadata storage.

    These stores are typically implemented using persistent storage systems like
    databases (PostgreSQL, SQLite) or distributed storage systems, providing
    guarantees about data persistence even in the event of system failures.

    The key feature is the save_many operation which enables efficient bulk
    insertion and update operations, optimizing performance for batch scenarios
    while maintaining the durability guarantees of persistent storage.
    """

    @abstractmethod
    def save_many(self, metas: Iterable[ResourceMeta]) -> None:
        """Bulk save operation for multiple resource metadata objects.

        Arguments:
            metas (Iterable[ResourceMeta]): An iterable of ResourceMeta objects
                to be saved to persistent storage.

        ---
        This method provides an optimized bulk save operation that can efficiently
        handle multiple metadata objects in a single transaction or batch operation.
        It's designed to minimize the overhead of individual save operations when
        dealing with large numbers of metadata objects.

        Behavior:
        - All metadata objects are saved atomically where possible
        - Existing metadata with the same resource_id will be updated
        - New metadata objects will be inserted
        - The operation should be optimized for the underlying storage system

        Use Cases:
        - Bulk synchronization from fast storage to persistent storage
        - Initial data loading and migration operations
        - Batch processing scenarios with multiple metadata updates
        - Periodic bulk backup operations

        Performance Considerations:
        - Implementation should use batch operations where available
        - Transaction boundaries should be optimized for the storage system
        - Error handling should provide partial success information where possible

        The method may raise storage-specific exceptions if the bulk operation fails,
        and implementations should provide appropriate error handling and rollback
        mechanisms where supported by the underlying storage system.
        """


class IMetaWithCount(IMetaStore, ABC):
    """Meta stores that can push a row COUNT down to their engine.

    Mixed into a concrete store ALONGSIDE its base — e.g.
    ``class SqliteMetaStore(IMetaWithAgg, IMetaWithCount, ISlowMetaStore)``.
    The abstract ``count`` forces an implementation, so ``isinstance(ms,
    IMetaWithCount)`` is a typed, typo-proof capability check (no ``hasattr``
    sniffing) — the same shape as :class:`IMetaWithAgg`.
    :meth:`~specstar.resource_manager.core.SimpleStorage.count` uses it to
    decide whether to push the count down or fall back to counting decoded
    rows in Python.

    Why it exists (#414): the fallback is
    ``more_itertools.ilen(store.iter_search(query))``, which streams EVERY
    matching row's meta blob over the wire and decodes it into a
    :class:`ResourceMeta` purely to discard it — an O(n) count. A store whose
    engine can answer ``SELECT COUNT(*)`` should.

    Contract: an implementation MUST return exactly what that ``ilen``
    reference path returns for the same query — including honouring
    ``query.limit`` / ``query.offset`` (the size of the paged window, i.e.
    ``clamp(matching - offset, 0, limit)``) and returning ``0`` when nothing
    matches. Enforced cross-backend by ``tests/meta_store/test_count.py``.

    ``query.sorts`` is irrelevant to a count and MAY be ignored: the size of a
    ``LIMIT``-ed window does not depend on the order its rows come back in, so
    a pushed-down count should skip the sort entirely.
    """

    @abstractmethod
    def count(self, query: ResourceMetaSearchQuery) -> int:
        """Count the rows matching ``query`` in the store's engine.

        Arguments:
            query (ResourceMetaSearchQuery): the same filter ``iter_search``
                would apply, including ``limit`` / ``offset``.

        Returns:
            int: the number of matching rows in the ``limit`` / ``offset``
            window.
        """


class IMetaWithAgg(IMetaStore, ABC):
    """Meta stores that can push a group-by aggregate down to their engine.

    Mixed into a concrete store ALONGSIDE its base — e.g.
    ``class SqliteMetaStore(IMetaWithAgg, ISlowMetaStore)``. The abstract
    ``aggregate_by`` forces an implementation, so ``isinstance(ms,
    IMetaWithAgg)`` is a typed, typo-proof capability check (no ``hasattr``
    sniffing): :meth:`ResourceManager.exp_aggregate_by` uses it to decide
    whether to push the aggregation down or fall back to its in-process Python
    reduction.

    Contract: every implementation MUST return results identical to that
    Python fallback — enforced cross-backend by
    ``tests/meta_store/test_aggregate_by.py``.
    """

    #: Whether this store can push the GROUP-level ``order_by`` + ``limit`` /
    #: ``offset`` (#412) down to its engine. When ``False`` (the default), the
    #: :class:`ResourceManager` never passes those args — it materialises all
    #: groups and orders / pages them with its in-process reference reduction.
    #: A store that implements the engine-side ``ORDER BY … LIMIT … OFFSET``
    #: overrides this to ``True``; results MUST still equal the reference.
    supports_group_paging: bool = False

    @abstractmethod
    def aggregate_by(
        self,
        query: ResourceMetaSearchQuery,
        by: AggKeyRef,
        aggregates: list[AggSpec],
        *,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[tuple[object, dict[str, object]]]:
        """Group rows matching ``query`` by ``by`` and reduce each aggregate
        per group, pushed down to the store's engine.

        Aggregates over the WHOLE filtered set — ``query``'s ``limit`` /
        ``offset`` are ignored (paging an aggregate is meaningless), matching
        the ResourceManager's ``iter_all`` semantics. Returns one
        ``(key, {result_name: value})`` per distinct ``by`` value; a missing /
        NULL key groups under ``key=None`` to match the Python reference path.

        **Group-level paging (#412)** — only used when
        :attr:`supports_group_paging` is ``True`` (else the RM leaves these at
        their defaults and orders / pages the groups itself):

        * ``order_by`` — a store ``AggSpec.result_name`` or the sentinel
          ``"key"`` (the group key), with the ``"-name"`` / ``"+name"``
          direction convention (asc default, ``-`` = desc). The engine MUST
          order NULL order-values LAST (direction-free) and break ties by the
          group key ascending (NULLs last) — byte-for-byte the
          ResourceManager's ``_order_and_page_groups`` reference.
        * ``limit`` / ``offset`` — page the GROUPS themselves (distinct from the
          ignored row-level ``query.limit/offset``). Only meaningful with a
          deterministic ``order_by``.
        """
        ...

    @staticmethod
    def _group_order_clause(
        order_by: str,
        key_expr: str,
        aggregates: "list[AggSpec]",
        agg_expr: "Callable[[AggSpec], str]",
    ) -> str:
        """The #412 group-level ``ORDER BY`` shared by every push-down store.

        ``order_by`` is a store ``AggSpec.result_name`` or the ``"key"`` sentinel
        with the ``+``/``-`` direction convention. NULLS-LAST is spelled with the
        version-agnostic ``(expr IS NULL) ASC`` prefix (not native ``NULLS
        LAST``) so SQLite, Postgres, and the in-process reference agree
        byte-for-byte; the group key is always the ascending secondary sort so
        pages are stable and never straddle a tie. ``key_expr`` and ``agg_expr``
        are the engine-specific SQL a concrete store supplies (its group-key
        expression and its per-aggregate value expression)."""
        name, descending = order_by, False
        if name[:1] in ("+", "-"):
            descending = name[0] == "-"
            name = name[1:]
        if name == "key":
            order_expr = key_expr
        else:
            order_expr = agg_expr(next(a for a in aggregates if a.result_name == name))
        direction = "DESC" if descending else "ASC"
        return (
            f" ORDER BY (({order_expr}) IS NULL) ASC, ({order_expr}) {direction}, "
            f"(({key_expr}) IS NULL) ASC, ({key_expr}) ASC"
        )


class IBlobStore(ABC):
    """Interface for storing and retrieving binary blobs.

    Implementations use content-hash addressing by default: the ``file_id``
    is derived from an xxh3-128 digest so identical payloads are
    automatically deduplicated.

    In addition to the basic ``put``/``get``/``exists`` operations, every
    implementation **must** provide the five *upload session* methods
    (``create_upload_session``, ``get_upload_session``,
    ``upload_to_session``, ``finalize_upload_session``,
    ``abort_upload_session``) that allow chunked or client-direct uploads.

    Exception conventions for session methods:

    * ``FileNotFoundError`` — session does not exist.
    * ``ValueError`` — invalid status transition (e.g. finalizing a
      pending session or uploading to an already-finalized session).
    """

    @abstractmethod
    def put(
        self,
        data: bytes,
        *,
        key: str | None = None,
        content_type: str | UnsetType = UNSET,
    ) -> Binary:
        """Store binary data and return a :class:`Binary` descriptor.

        Args:
            data: Raw bytes to store.
            key: Optional caller-specified storage key.  When ``None``
                (the default), the key is derived from a content hash.
                When a ``str`` is given, it is used as the ``file_id``
                directly and a subsequent ``put`` with the same key
                **overwrites** the previous content.
            content_type: MIME type hint.  ``UNSET`` lets the
                implementation guess (if available).

        Returns:
            A :class:`Binary` instance with ``file_id``, ``size``, and
            ``content_type`` populated.
        """
        pass

    @abstractmethod
    def get(self, file_id: str) -> Binary:
        """Retrieve binary data by ID."""
        pass

    @abstractmethod
    def exists(self, file_id: str) -> bool:
        """Check if blob exists."""
        pass

    @abstractmethod
    def delete(self, file_id: str) -> None:
        """Permanently remove a blob by ``file_id``.

        This is the **irreversible** primitive used by garbage collection
        (issue #370) once a blob has been confirmed unreferenced.  It is
        **idempotent**: deleting a ``file_id`` that does not exist is a
        no-op, never an error, so a crashed/restarted GC can safely retry.

        Removes the blob whether it lives in the active area or in
        quarantine.
        """
        pass

    @abstractmethod
    def quarantine(self, file_id: str, *, now: dt.datetime) -> None:
        """Move a blob from the active area into a reversible quarantine area.

        ``now`` is recorded as the blob's quarantine **entry time** so that
        :meth:`iter_quarantined` can later select blobs that have dwelt long
        enough (the GC's ``T2`` grace period).  Quarantining is **reversible**
        via :meth:`restore_from_quarantine`, and a quarantined blob remains
        readable through :meth:`get` / :meth:`exists` (active-miss
        fall-through).  No-op if ``file_id`` is not currently active.
        """
        pass

    @abstractmethod
    def restore_from_quarantine(self, file_id: str) -> None:
        """Move a blob back from quarantine to the active area (resurrection).

        No-op if ``file_id`` is not currently quarantined.
        """
        pass

    @abstractmethod
    def iter_quarantined(self, *, entered_before: dt.datetime) -> Generator[str]:
        """Yield ``file_id``s quarantined strictly before ``entered_before``.

        Used by the reconcile pass to find quarantined blobs old enough to be
        considered for the irreversible :meth:`delete`.
        """
        pass

    @abstractmethod
    def incref(self, file_id: str) -> int:
        """Increment the blob's approximate reference count, returning the new
        value.

        The count is a **best-effort, non-atomic** hint (issue #370): it is
        only used to cheaply nominate orphan candidates, never as the sole
        authority for deletion.  The reconcile pass recomputes exact counts.
        """
        pass

    @abstractmethod
    def decref(self, file_id: str) -> int:
        """Decrement the blob's approximate reference count, returning the new
        value.  Clamps at zero (never negative).  Best-effort / non-atomic."""
        pass

    @abstractmethod
    def iter_active(self) -> Generator[str]:
        """Yield the ``file_id`` of every blob in the **active** area
        (excluding quarantined blobs).

        Used by the reconcile pass to discover orphans (active blobs that no
        live revision references).
        """
        pass

    @abstractmethod
    def get_mtime(self, file_id: str) -> "dt.datetime | None":
        """Return the active blob's last-write time, or ``None`` if absent.

        This is the blob's *age* source (issue #370): the GC protects blobs
        younger than the ``T1`` grace period from being quarantined, covering
        the "uploaded-but-not-yet-referenced" window.  Implementations use the
        backend's native modification time (filesystem ``mtime`` / S3
        ``LastModified`` / in-memory put timestamp).  Returns a timezone-aware
        UTC datetime.
        """
        pass

    @abstractmethod
    def touch(self, file_id: str, *, now: "dt.datetime | None" = None) -> None:
        """Refresh an active blob's last-write time to ``now`` (default: the
        current UTC time).

        Called on the reference hot path (issue #370): re-referencing a blob
        keeps its *age* fresh so the ``T1`` grace covers the gap between
        "someone referenced it" and "the count caught up".  No-op if the blob
        is not active.  (S3 stamps the server's current time regardless of
        ``now``, since object ``LastModified`` is server-controlled.)
        """
        pass

    @abstractmethod
    def iter_orphan_candidates(self, *, modified_before: dt.datetime) -> Generator[str]:
        """Yield active ``file_id``s whose approximate count has dropped to
        ``<= 0`` (orphan suspects) and whose last-write time precedes
        ``modified_before`` (the ``T1`` grace cutoff).

        This is the cheap, scan-free candidate feed for the incremental GC
        pass — populated as a side effect of :meth:`decref`.  Best-effort:
        the authoritative truth is always the reconcile scan.
        """
        pass

    @abstractmethod
    def get_url(self, file_id: str) -> str | None:
        """
        Get a direct download URL for the blob if supported.
        Returns None if not supported (e.g. local storage without a public server).
        """
        pass

    @abstractmethod
    def get_stream(self, file_id: str) -> BlobStreamInfo | None:
        """Return a streaming iterator for blob content.

        Returns a :class:`BlobStreamInfo` containing a chunk iterator,
        file size, and content type — suitable for
        :class:`~starlette.responses.StreamingResponse`.

        Returns ``None`` when the implementation does not support
        streaming (e.g. :class:`MemoryBlobStore` where data is already
        in memory).  The caller should fall back to :meth:`get` in that
        case.

        Raises:
            FileNotFoundError: If the blob does not exist.
        """
        pass

    @abstractmethod
    def get_response(self, file_id: str) -> BlobResponse:
        """Return the preferred download response for a blob.

        The blob store decides its own download strategy.  The default
        order is:

        1. **stream** — :meth:`get_stream` (constant memory, works
           through proxies).
        2. **data** — :meth:`get` (full body in memory).
        3. **redirect** — :meth:`get_url` (presigned URL / CDN).

        Subclasses may override this to change the priority (e.g.
        :class:`S3BlobStore` can put ``get_url`` first when
        ``prefer_presigned_url=True``).

        Raises:
            FileNotFoundError: If the blob does not exist.
        """
        pass

    # ------------------------------------------------------------------
    # Upload session API
    # ------------------------------------------------------------------

    @abstractmethod
    def create_upload_session(
        self,
        *,
        key: str | None = None,
        content_type: str | UnsetType = UNSET,
        size: int | None = None,
        total_parts: int | None = None,
    ) -> BlobUploadSession:
        """Create an upload session.

        Returns session metadata including ``upload_url`` for the client
        to deliver bytes.

        Args:
            key: Optional caller-specified storage key.  When ``None``
                the key is derived from a content hash at finalize time.
            content_type: MIME type hint.
            size: Expected size of the content in bytes (``None`` if unknown).
            total_parts: Expected number of parts for parallel chunked upload
                (``None`` if unknown).  When set, :meth:`finalize_upload_session`
                validates that exactly this many parts have been received.

        Returns:
            A :class:`BlobUploadSession` with ``upload_id``, ``upload_method``,
            and ``upload_url`` populated.
        """

    @abstractmethod
    def get_upload_session(self, upload_id: str) -> BlobUploadSession:
        """Return current state of an upload session.

        Args:
            upload_id: The upload session identifier.

        Returns:
            A :class:`BlobUploadSession` reflecting the current state.

        Raises:
            FileNotFoundError: If the session does not exist.
        """

    @abstractmethod
    def upload_to_session(
        self, upload_id: str, data: bytes, *, part_number: int
    ) -> None:
        """Upload a numbered part for a proxy upload session.

        May be called **multiple times** with different ``part_number``
        values — potentially **in parallel** from the client.  Parts
        may arrive out of order.

        Implementations use an *eager-merge* strategy where possible:
        parts that arrive in sequence are immediately appended to the
        final data buffer, while out-of-order parts are buffered
        temporarily and flushed as soon as the gap is filled.

        The session transitions from ``"pending"`` to ``"uploading"``
        on the first call and stays in ``"uploading"`` for subsequent
        calls.

        This does **not** commit data to the blob store; call
        :meth:`finalize_upload_session` to persist.

        Args:
            upload_id: The upload session identifier.
            data: Raw bytes for this part.
            part_number: 1-based part index.  Must be ≥ 1.
                Duplicate ``part_number`` values are idempotent: if the
                part has already been merged it is silently ignored; if
                it is still buffered it is overwritten (retry-safe).

        Raises:
            FileNotFoundError: If the session does not exist.
            ValueError: If the session status does not allow uploading
                (e.g. already finalized or aborted), or if
                ``part_number < 1``.
        """

    @abstractmethod
    def finalize_upload_session(self, upload_id: str) -> Binary:
        """Finalize session: commit bytes to the blob store.

        Accepts sessions in ``"uploaded"`` (single upload, backwards
        compatible) or ``"uploading"`` (chunked upload) status.

        Returns a :class:`Binary` descriptor (without raw ``data``).

        Args:
            upload_id: The upload session identifier.

        Returns:
            A :class:`Binary` with ``file_id``, ``size``, and
            ``content_type`` populated (``data`` is ``UNSET``).

        Raises:
            FileNotFoundError: If the session does not exist.
            ValueError: If the session status does not allow finalizing
                (e.g. still pending, already finalized, or aborted).
        """

    @abstractmethod
    def abort_upload_session(self, upload_id: str) -> None:
        """Abort session and discard any buffered bytes.

        Args:
            upload_id: The upload session identifier.

        Raises:
            FileNotFoundError: If the session does not exist.
            ValueError: If the session has already been finalized.
        """


class IResourceStore(ABC):
    """Interface for storing and retrieving versioned resource data.

    This interface manages the storage of actual resource data and their revision
    information. Unlike metadata stores that handle ResourceMeta objects, resource
    stores manage the complete Resource[T] objects including both data content and
    revision information.

    The store provides version control capabilities by maintaining all revisions
    of each resource, allowing for complete history tracking and point-in-time
    recovery. Each resource can have multiple revisions, and each revision
    contains both the data at that point in time and metadata about the revision.

    Type Parameters:
        T: The type of data stored in resources. This allows the store to be
           type-safe for specific resource data structures.
    """

    @abstractmethod
    def list_resources(self) -> Generator[str]:
        """Iterate over all resource IDs in the store.

        Returns:
            Generator[str]: A generator yielding all unique resource IDs that
                have at least one revision stored in the system.

        ---
        This method provides access to all resource identifiers currently stored
        in the system, regardless of their deletion status or number of revisions.
        Each resource ID represents a unique resource that may have one or more
        revisions stored.

        The iteration order is implementation-dependent and may vary between
        different storage backends. For consistent ordering, use appropriate
        sorting mechanisms in the calling code.
        """

    @abstractmethod
    def list_revisions(self, resource_id: str) -> Generator[str]:
        """Iterate over all revision IDs for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource to list
                revisions for.

        Returns:
            Generator[str]: A generator yielding all revision IDs for the
                specified resource.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist in the store.

        ---
        This method provides access to all revision identifiers for a specific
        resource, enabling complete history traversal and revision management.
        The revisions represent the complete change history of the resource.

        The iteration order is typically chronological (oldest to newest) but
        may vary depending on the implementation. For guaranteed ordering,
        consider using revision timestamps or sequence numbers.
        """

    @abstractmethod
    def list_schema_versions(
        self, resource_id: str, revision_id: str
    ) -> Generator[str | None]:
        """Retrieve a list of migrated revisions for a specific resource and revision."""

    @abstractmethod
    def exists(
        self, resource_id: str, revision_id: str, schema_version: str | None
    ) -> bool:
        """Check if a specific revision exists for a given resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to check.

        Returns:
            bool: True if the specified revision exists, False otherwise.

        ---
        This method provides a fast existence check without retrieving the actual
        data. It's useful for validation and conditional logic before attempting
        to retrieve or operate on specific revisions.

        The method returns False if either the resource doesn't exist or the
        specific revision doesn't exist for that resource.
        """

    @abstractmethod
    def get_data_bytes(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> AbstractContextManager[IO[bytes]]:
        """Retrieve raw data bytes for a specific revision.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to retrieve.

        Returns:
            IO[bytes]: A byte stream containing the raw encoded resource data.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If the revision ID does not exist for the resource.

        ---
        This method provides direct access to the raw encoded bytes of resource data
        without automatic decoding. This enables migration scenarios where the
        ResourceManager needs to handle decoding and transformation at a higher level.

        The returned stream should be positioned at the beginning of the data and
        remain valid until closed or the method is called again.
        """

    def get_revision_bundle(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> "tuple[RevisionInfo, bytes]":
        """A revision's info AND its data.

        Reading a resource needs both, and every caller asked for them
        separately — two store round-trips for one row. Concrete because the
        answer is derivable: this default is exactly the two calls it replaces,
        so a store gains nothing and loses nothing by ignoring it. A backend that
        can return both in one round-trip should override (the SQL stores select
        two columns of the same joined row).
        """
        info = self.get_revision_info(resource_id, revision_id, schema_version)
        with self.get_data_bytes(resource_id, revision_id, schema_version) as fh:
            return info, fh.read()

    def get_all_revision_infos(
        self,
        resource_id: str,
    ) -> "dict[str, RevisionInfo]":
        """Every stored revision's info for one resource, keyed by revision id.

        Pruning has to see all of them to decide what to keep, and did so one
        revision at a time — a schema-version lookup and an info read each. The
        default here IS that loop, so a store without a bulk read is unchanged.

        Only `info` is needed: the payload is what pruning is about to delete.
        """
        out: dict[str, RevisionInfo] = {}
        for revision_id in self.list_revisions(resource_id):
            for schema_version in self.list_schema_versions(resource_id, revision_id):
                try:
                    out[revision_id] = self.get_revision_info(
                        resource_id, revision_id, schema_version
                    )
                except (KeyError, FileNotFoundError):
                    continue
                break
        return out

    def get_revision_bundles(
        self,
        keys: "Sequence[tuple[str, str, str | None]]",
    ) -> "dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]]":
        """Many revisions' info+data at once, keyed by ``(resource_id,
        revision_id, schema_version)``.

        A listing needs every row on the page, and fetching them one at a time
        makes the page cost proportional to its size. Concrete for the same
        reason as `get_revision_bundle`: this default IS the loop it replaces, so
        a store without a bulk read keeps working unchanged. Missing keys are
        simply absent from the result — the caller decides whether that is an
        error, exactly as it does today.
        """
        out: dict[tuple[str, str, str | None], tuple[RevisionInfo, bytes]] = {}
        for key in keys:
            try:
                out[key] = self.get_revision_bundle(*key)
            except KeyError:
                continue
        return out

    @abstractmethod
    def get_revision_info(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> RevisionInfo:
        """Retrieve revision metadata without the resource data.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            RevisionInfo: The revision metadata including timestamps, user info,
                parent revision references, and other revision-specific information.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If the revision ID does not exist for the resource.

        ---
        This method provides access to revision metadata without loading the
        potentially large resource data. It's useful for revision browsing,
        audit trails, and operations that only need revision metadata.

        The RevisionInfo includes information such as:
        - Creation and update timestamps
        - User information (who created/updated)
        - Parent revision relationships
        - Revision status and schema version
        - Data integrity hashes
        """

    @abstractmethod
    def save(self, info: RevisionInfo, data: IO[bytes]) -> None:
        """Save a new revision."""

    def save_many(
        self, items: Iterable[tuple[RevisionInfo, "bytes | IO[bytes]"]]
    ) -> None:
        """Optional bulk save. Default loops and calls :meth:`save`.

        Subclasses with bulk-optimised paths (e.g. concurrent S3 PUTs)
        override this. The default exists so callers can always invoke
        ``store.save_many(...)`` without checking ``hasattr``.
        """
        import io as _io

        for info, raw in items:
            stream = _io.BytesIO(raw) if isinstance(raw, (bytes, bytearray)) else raw
            self.save(info, stream)

    # -- Bulk read (#434) -------------------------------------------------
    #
    # ``dump_all_revisions`` below is an *export* primitive: it pulls every
    # revision of every resource. A read-modify-write fan-out needs the
    # opposite shape — the *current* revision only, for a named set of
    # resources, bounded by how much memory the caller can spare. That is
    # what ``read_many`` is for.

    #: How a store signals "this payload is gone". Backends disagree
    #: (``KeyError`` for the keyed stores, ``FileNotFoundError`` for the
    #: filesystem one) and a bulk read has to treat them alike: a row can be
    #: deleted between being selected and being read, and one dead row must
    #: not take the whole batch down. See :meth:`read_many`.
    MISSING_PAYLOAD: "tuple[type[BaseException], ...]" = (KeyError, FileNotFoundError)

    def payload_sizes(
        self, items: "Sequence[tuple[str, str, str | None]]"
    ) -> "list[int] | None":
        """Byte size of each item's payload, *without* transferring it.

        Returns ``None`` when the backend cannot answer cheaply, which makes
        :meth:`read_many` fall back to measuring payloads as it reads them.

        Size is deliberately **derived, never stored**. A ``size`` field on
        :class:`RevisionInfo` would need a backfill, and every revision
        written before it existed would read as ``UNSET`` — a budget that
        silently mis-packs exactly the oldest data. ``stat`` /
        ``octet_length`` / ``head_object`` cost a little time and are always
        right; a missing column costs correctness and says nothing.
        """
        return None

    def read_payloads(
        self, items: "Sequence[tuple[str, str, str | None]]"
    ) -> "dict[str, bytes]":
        """Fetch every item's raw payload, keyed by ``resource_id``.

        *items* holds ``(resource_id, revision_id, schema_version)`` triples,
        at most one per ``resource_id``. The default reads them one by one;
        backends with concurrent or set-based fetches override it.

        Items whose payload has gone missing are **omitted** rather than
        raised — see :attr:`MISSING_PAYLOAD`.
        """
        out: dict[str, bytes] = {}
        for resource_id, revision_id, schema_version in items:
            try:
                with self.get_data_bytes(
                    resource_id, revision_id, schema_version
                ) as fh:
                    out[resource_id] = fh.read()
            except self.MISSING_PAYLOAD:
                continue
        return out

    def read_many(
        self,
        items: "Sequence[tuple[str, str, str | None]]",
        *,
        max_bytes: int,
    ) -> "tuple[dict[str, bytes], int]":
        """Read a prefix of *items* that fits in *max_bytes*.

        Returns ``(payloads_by_resource_id, consumed)`` where *consumed* is
        how many leading entries of *items* were read, so a caller drains the
        set with ``items = items[consumed:]``.

        The budget is in **bytes rather than rows** because row size varies by
        orders of magnitude between models — a batch size tuned for a flat
        derived table will exhaust memory on documents carrying their whole
        extracted text, and vice versa.

        **At least one entry is always consumed**, even if it alone exceeds
        *max_bytes*. A row bigger than the budget must still make progress;
        refusing it would leave the caller asking for the same prefix forever.

        When :meth:`payload_sizes` cannot size cheaply the fallback measures
        payloads as it reads them, so it may overshoot the budget by at most
        one entry — the one that crossed the line has already been fetched.

        Entries whose payload vanished between selection and read are counted
        as consumed but absent from the returned mapping; a long fan-out has
        to survive a row being deleted underneath it.
        """
        items = list(items)
        if not items:
            return {}, 0

        sizes = self.payload_sizes(items)
        if sizes is not None:
            total = 0
            consumed = 0
            for size in sizes:
                if consumed and total + size > max_bytes:
                    break
                total += size
                consumed += 1
            return self.read_payloads(items[:consumed]), consumed

        # No cheap size probe: read until the running total reaches the
        # budget. Bounded overshoot of one entry — see the docstring.
        out: dict[str, bytes] = {}
        total = 0
        consumed = 0
        for resource_id, revision_id, schema_version in items:
            consumed += 1
            try:
                with self.get_data_bytes(
                    resource_id, revision_id, schema_version
                ) as fh:
                    raw = fh.read()
            except self.MISSING_PAYLOAD:
                continue
            out[resource_id] = raw
            total += len(raw)
            if total >= max_bytes:
                break
        return out, consumed

    def dump_all_revisions(
        self, *, resource_ids: "frozenset[str] | None" = None
    ) -> "dict[str, list[tuple[RevisionInfo, bytes]]] | None":
        """Optional bulk pre-fetch. Default returns ``None``.

        ``None`` signals to the caller to fall back to per-resource
        streaming. Subclasses with bulk-fetch capability (e.g. S3
        list+get pipelines) override this to return a dict keyed by
        ``resource_id``.
        """
        return None

    def purge_resource(self, resource_id: str) -> None:
        """Hard-delete all revision data for a resource.

        Removes all revision data (all revisions, all schema versions) for the
        given resource.  This is an **irreversible** operation used by
        ``permanently_delete`` to remove resource data after metadata has been
        removed.

        The base implementation raises :exc:`NotImplementedError`.  Storage
        backends that support permanent deletion must override this method.

        Arguments:
            resource_id (str): The unique identifier of the resource whose
                revision data should be permanently removed.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement purge_resource(). "
            "Override this method to support permanent deletion."
        )

    def delete_revisions(self, resource_id: str, revision_ids: list[str]) -> None:
        """Hard-delete a specific set of revisions for a resource.

        Removes the listed revisions (all of their schema versions) and the
        underlying uid payload once no surviving revision still references it
        — the same reference-counted cleanup ``purge_resource`` performs, but
        scoped to *revision_ids* instead of the whole resource.  This is the
        physical primitive behind :meth:`IResourceManager.prune_revisions`.

        Implementations must be idempotent: revision ids that do not exist are
        silently skipped, so a concurrent prune / delete cannot raise here.
        Callers are responsible for never asking to delete a resource's
        current revision; this method does not consult metadata.

        The base implementation raises :exc:`NotImplementedError`.  Storage
        backends that support revision pruning must override this method.

        Arguments:
            resource_id (str): The resource whose revisions to delete.
            revision_ids (list[str]): The revision ids to remove.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement delete_revisions(). "
            "Override this method to support revision pruning."
        )


class IStorage(ABC):
    """Interface for unified storage management combining metadata and resource data.

    This interface provides a high-level abstraction that combines both metadata
    storage (IMetaStore) and resource data storage (IResourceStore) into a single
    unified interface. It serves as the primary storage abstraction for the
    ResourceManager and handles the coordination between metadata and data storage.

    The storage interface manages the complete lifecycle of resources including:
    - Resource and revision existence checking
    - Metadata and data storage coordination
    - Search and query operations across metadata
    - Bulk data export and import operations
    - Data encoding and serialization

    Type Parameters:
        T: The type of data stored in resources. This ensures type safety
           for resource data operations throughout the storage layer.

    This interface is typically implemented by storage systems that coordinate
    between separate metadata and resource stores, providing a unified view
    while optimizing for different access patterns and performance requirements.
    """

    @property
    def meta_store(self) -> "IMetaStore | None":
        """The single underlying metadata store, when this storage has one.

        ``SimpleStorage`` returns its meta store; composite / cached storages
        may return ``None``. Lets the ResourceManager reach a capability like
        :class:`IMetaWithAgg` via ``isinstance`` without sniffing attributes.
        Default ``None`` so existing storages need no change.
        """
        return None

    @abstractmethod
    def exists(self, resource_id: str) -> bool:
        """Check if a resource exists in the storage system.

        Arguments:
            resource_id (str): The unique identifier of the resource to check.

        Returns:
            bool: True if the resource exists (has metadata), False otherwise.

        ---
        This method checks for resource existence at the metadata level. A resource
        is considered to exist if it has associated metadata, regardless of its
        deletion status or the number of revisions it has.

        This is a lightweight operation that only checks metadata presence and
        does not verify the existence of specific revisions or data integrity.
        """

    @abstractmethod
    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        """Check if a specific revision exists for a given resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to check.

        Returns:
            bool: True if the specified revision exists, False otherwise.

        ---
        This method verifies the existence of a specific revision within the
        resource's history. It checks both that the resource exists and that
        the particular revision is available in the storage system.

        This operation may involve checking both metadata consistency and
        data availability depending on the storage implementation.
        """

    @abstractmethod
    def find_revision_schema_version(
        self, resource_id: str, revision_id: str
    ) -> str | None | UnsetType:
        """Find the most recent schema version stored for a revision.

        Searches all schema versions under which *revision_id* has been
        saved and returns the **latest** (highest) one.  If the revision
        does not exist under any schema version, returns ``UNSET``.

        Note that ``None`` is a valid return value meaning the revision
        exists but was stored without a schema version.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            str | None | UnsetType: The schema version string, ``None``
                if stored without a schema, or ``UNSET`` if the revision
                does not exist at all.
        """

    @abstractmethod
    def get_meta(self, resource_id: str) -> ResourceMeta:
        """Retrieve metadata for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.

        Returns:
            ResourceMeta: The complete metadata object for the resource.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.

        ---
        This method retrieves the complete metadata for a resource, including
        current revision information, timestamps, user data, deletion status,
        and indexed data for search operations.

        The metadata provides essential information about the resource without
        requiring access to the potentially large resource data content.
        """

    @abstractmethod
    def save_meta(self, meta: ResourceMeta) -> None:
        """Store or update metadata for a resource.

        Arguments:
            meta (ResourceMeta): The metadata object to store.

        ---
        This method stores or updates the metadata for a resource. If metadata
        for the resource already exists, it will be replaced. The operation
        should be atomic and ensure consistency between the metadata and any
        associated indexes.

        The method handles persistence of all metadata fields including indexed
        data that may be used for search operations.
        """

    @abstractmethod
    def list_revisions(self, resource_id: str) -> list[str]:
        """List all revision IDs for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.

        Returns:
            list[str]: A list of all revision IDs for the resource, typically
                ordered chronologically from oldest to newest.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.

        ---
        This method provides a complete list of all revisions available for a
        resource, enabling full history traversal and revision management
        operations. The ordering facilitates understanding the evolution of
        the resource over time.
        """

    @abstractmethod
    def get_data_bytes(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> AbstractContextManager[IO[bytes]]:
        """Retrieve raw data bytes for a specific resource revision.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            IO[bytes]: A byte stream containing the raw encoded resource data.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.
            RevisionIDNotFoundError: If the revision does not exist.

        ---
        This method provides direct access to the raw encoded bytes of resource data
        without automatic decoding. This enables migration scenarios where the
        ResourceManager needs to handle decoding and transformation at a higher level.

        The returned stream should be positioned at the beginning of the data and
        remain valid until closed or the method is called again.
        """

    @abstractmethod
    def get_resource_revision_info(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> RevisionInfo:
        """Retrieve revision information without the resource data.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            RevisionInfo: The revision metadata including creation info,
                parent relationships, and status information.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.
            RevisionIDNotFoundError: If the revision does not exist.

        ---
        This method provides access to revision metadata without loading the
        potentially large resource data. It's optimized for operations that
        only need revision information such as audit trails, version browsing,
        and revision relationship analysis.
        """

    @abstractmethod
    def save_revision(self, info: RevisionInfo, data: IO[bytes]) -> None:
        """Store raw data bytes for a resource revision.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.
            data (IO[bytes]): A byte stream containing the encoded resource data.

        ---
        This method stores raw encoded bytes for a resource revision without
        handling the decoding or interpretation of the data. This enables the
        ResourceManager to handle encoding/decoding at a higher level while
        keeping the storage layer focused on pure byte operations.

        The implementation should read all data from the stream and store it
        persistently. The stream position should be reset to the beginning
        before reading if necessary.
        """

    @abstractmethod
    def count(self, query: ResourceMetaSearchQuery) -> int: ...

    def purge_meta(self, resource_id: str) -> None:
        """Hard-delete the metadata for a resource (bypasses soft-delete).

        This is an internal compensating operation used by the unique-constraint
        enforcement mechanism to roll back a just-created resource when a race
        condition is detected.  It removes the resource metadata from the store
        entirely so that the resource becomes invisible to all future queries.

        Unlike :meth:`ResourceManager.delete`, this method does **not** set
        ``is_deleted=True``; it physically removes the record.  Any revision
        data that was already saved in the resource store becomes orphaned but
        is not accessible through normal API paths.

        The base implementation raises :exc:`NotImplementedError`.  Storage
        backends that need to support unique-constraint rollback must override
        this method.

        Arguments:
            resource_id (str): The unique identifier of the resource whose
                metadata should be permanently removed.

        Raises:
            NotImplementedError: If the storage backend does not implement
                hard deletion.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement purge_meta(). "
            "Override this method to support unique-constraint rollback."
        )

    def purge_resource(self, resource_id: str) -> None:
        """Hard-delete all revision data for a resource.

        Removes both the metadata and all revision data (all revisions, all
        schema versions) for the given resource.  This is an **irreversible**
        operation used by ``permanently_delete``.

        The base implementation raises :exc:`NotImplementedError`.  Storage
        backends that support permanent deletion must override this method.

        Arguments:
            resource_id (str): The unique identifier of the resource to
                permanently remove.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement purge_resource(). "
            "Override this method to support permanent deletion."
        )

    def delete_revisions(self, resource_id: str, revision_ids: list[str]) -> None:
        """Hard-delete a specific set of revisions for a resource.

        Delegates to the underlying resource store's
        :meth:`IResourceStore.delete_revisions`.  Unlike ``purge_resource``
        this leaves the resource metadata untouched (the current revision and
        ``total_revision_count`` survive); it only removes the listed
        revisions' payloads.  Idempotent: unknown revision ids are skipped.

        The base implementation raises :exc:`NotImplementedError`.  Storage
        backends that support revision pruning must override this method.

        Arguments:
            resource_id (str): The resource whose revisions to delete.
            revision_ids (list[str]): The revision ids to remove.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement delete_revisions(). "
            "Override this method to support revision pruning."
        )

    @abstractmethod
    def search(self, query: ResourceMetaSearchQuery) -> list[ResourceMeta]:
        """Search for resources based on metadata and data criteria.

        Arguments:
            query (ResourceMetaSearchQuery): The search criteria including
                filters, sorting, and pagination parameters.

        Returns:
            list[ResourceMeta]: A list of metadata objects for resources
                that match the search criteria.

        ---
        This method provides comprehensive search capabilities across both
        metadata fields and indexed resource data. It supports complex
        filtering, sorting, and pagination to enable efficient resource
        discovery and management operations.

        The search operates on the metadata level but can filter based on
        indexed data content, providing powerful query capabilities without
        requiring full data loading for each resource.
        """

    def iter_search(self, query: ResourceMetaSearchQuery) -> Generator[ResourceMeta]:
        """Streaming variant of :meth:`search`.

        Yields one :class:`ResourceMeta` at a time so callers can avoid
        materialising the full result list in memory.  The default
        implementation simply delegates to :meth:`search`; backends that
        support true streaming should override this method.
        """
        yield from self.search(query)

    @abstractmethod
    def dump_meta(
        self, resource_ids: frozenset[str] | None = None
    ) -> Generator[ResourceMeta]:
        """Export resource metadata for backup or migration.

        Args:
            resource_ids: If provided, only yield metadata for these resource IDs.
                If ``None``, yield all metadata.

        Returns:
            Generator[ResourceMeta]: A generator yielding metadata objects.
        """

    @abstractmethod
    def dump_resource(
        self,
        resource_id: str,
    ) -> Generator[tuple[RevisionInfo, IO[bytes]]]:
        """Export all revisions for a single resource.

        Args:
            resource_id: The resource ID whose revisions to export.

        Returns:
            Generator[tuple[RevisionInfo, IO[bytes]]]: A generator that yields
            tuples of (RevisionInfo, data bytes).
        """

    def dump_resources_bulk(
        self, *, resource_ids: "frozenset[str] | None" = None
    ) -> "dict[str, list[tuple[RevisionInfo, bytes]]] | None":
        """Optional bulk pre-fetch across multiple resources.

        Returns ``None`` when the underlying resource store has no
        bulk-fetch path; callers should fall back to per-resource
        streaming via :meth:`dump_resource`. Implementations that wrap
        a bulk-capable resource store override this.
        """
        return None

    @abstractmethod
    def save_metas_bulk(self, metas: "list[ResourceMeta]") -> None:
        """Bulk save resource metadata.

        Implementations should delegate to the underlying meta store's
        :meth:`IMetaStore.save_many` for batch durability.
        """

    @abstractmethod
    def save_revisions_bulk(self, items: "list[tuple[RevisionInfo, bytes]]") -> None:
        """Bulk save revision records.

        Implementations should delegate to the underlying resource
        store's :meth:`IResourceStore.save_many`.
        """

    @abstractmethod
    def read_metas_bulk(
        self, resource_ids: "Sequence[str]"
    ) -> "dict[str, ResourceMeta]":
        """Bulk read resource metadata (#434).

        The read-side counterpart of :meth:`save_metas_bulk`; implementations
        should delegate to :meth:`IMetaStore.get_many`. Ids with no row are
        omitted rather than raising — a bulk caller needs to see which members
        of its set have gone.
        """

    @abstractmethod
    def read_revisions_bulk(
        self,
        items: "Sequence[tuple[str, str, str | None]]",
        *,
        max_bytes: int,
    ) -> "tuple[dict[str, bytes], int]":
        """Bulk read revision payloads under a memory budget (#434).

        The read-side counterpart of :meth:`save_revisions_bulk`; implementations
        should delegate to :meth:`IResourceStore.read_many`, whose docstring
        carries the budget contract — bytes rather than rows, at least one row
        always consumed, missing rows omitted.
        """


# Data Search Related Classes


class UnifiedSortKey(StrEnum):
    # Meta 欄位
    created_time = "created_time"
    updated_time = "updated_time"
    resource_id = "resource_id"

    # Data 欄位（用前綴區分）
    data_prefix = "data."  # 實際使用時會是 "data.name", "data.user.email" 等


class UnifiedSearchSort(Struct, kw_only=True):
    direction: ResourceMetaSortDirection = ResourceMetaSortDirection.ascending
    key: str  # 可以是 meta 欄位名或 "data.field_path"


class IndexEntry(Struct, kw_only=True):
    resource_id: str
    revision_id: str
    field_path: str
    field_value: Any
    field_type: str  # Store type name as string
