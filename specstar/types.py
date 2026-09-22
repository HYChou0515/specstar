from __future__ import annotations

import datetime as dt
import threading
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager
from enum import Enum, Flag, StrEnum, auto
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    TypeVar,
)
from uuid import UUID

from jsonpatch import JsonPatch
from jsonpointer import JsonPointer
from msgspec import UNSET, Struct, UnsetType
from msgspec import field as _msgspec_field
from typing_extensions import Literal
from typing_extensions import TypeVar as TypeVarExt

from specstar.query_types import ResourceMetaSearchQuery

if TYPE_CHECKING:
    from specstar.crud.core import LoadStats
    from specstar.query import Query
    from specstar.resource_manager.dump_format import (
        BlobRecord,
        MetaRecord,
        RevisionRecord,
    )

T = TypeVar("T")
D = TypeVarExt("D", default=None)


# ---------------------------------------------------------------------------
# Resource References (Ref / RefRevision)
# ---------------------------------------------------------------------------


class OnDelete(StrEnum):
    """Defines the referential action when the referenced resource is deleted."""

    dangling = "dangling"
    """No action taken. The reference becomes dangling. (default)"""

    set_null = "set_null"
    """Set the referencing field to null. Requires the field to be Optional."""

    cascade = "cascade"
    """Delete the referencing resource as well."""

    restrict = "restrict"
    """Refuse to delete the referenced resource while any other resource
    still references it. The :meth:`ResourceManager.delete` call raises
    :class:`specstar.types.ResourceConflictError` (HTTP 409 on routes).

    Use this when you want to force the caller to clean up references
    explicitly instead of letting the referencing rows become dangling
    (``dangling``) or wholesale-cascaded (``cascade``)."""


class RefType(StrEnum):
    """Defines the type of reference a field holds."""

    resource_id = "resource_id"
    """The field stores a resource_id. The reference targets the resource as
    a whole and participates in referential integrity (on_delete), auto-indexing,
    and referrers queries."""

    revision_id = "revision_id"
    """The field stores a version-aware reference: either a revision_id
    (pinned to a specific revision) or a resource_id (meaning *latest*).
    Revision refs are always ``on_delete=dangling``, are not auto-indexed,
    and are excluded from referrers queries."""


class OnDecodeError(StrEnum):
    """What to do when a stored row can't be decoded into the current model
    (e.g. an incompatible schema change without a version bump).

    The policy is honoured by the ``ResourceManager`` read paths, so HTTP routes
    (which delegate to the manager) behave identically to programmatic calls.
    """

    skip = "skip"
    """Omit the undecodable row from list results and log a warning. ``count``
    still counts it, so the two can differ — but no longer silently. For a
    single ``get`` (nothing to skip), this degrades to ``error``. (default)"""

    error = "error"
    """Raise :class:`ResourceDecodeError` (HTTP 422) on the undecodable row."""

    raw = "raw"
    """Return the row in the normal envelope but with ``data`` set to an
    :class:`UndecodableData` (best-effort dict, else raw bytes) instead of the
    model type — i.e. ``Resource[UndecodableData]`` / ``SearchedResource[...]``."""


class OnUnindexedQuery(StrEnum):
    """What to do when a search filters on a field that is neither an indexed
    field nor a ``ResourceMeta`` attribute.

    Such a field is never written to ``indexed_data``, so the condition can
    never match — the query silently under-returns (often "returns nothing").
    This policy makes that footgun visible. Honoured by the ``ResourceManager``
    search paths, so HTTP routes behave identically to programmatic calls.
    """

    warn = "warn"
    """Emit a :class:`SpecStarWarning` naming the offending field(s) and run the
    query anyway (it will under-return). Suppress with the standard ``warnings``
    machinery. (default)"""

    error = "error"
    """Raise :class:`UnindexedQueryError` naming the offending field(s) instead
    of running a query that can only under-return."""


class OnDuplicate(StrEnum):
    """Strategy for handling duplicate resource IDs during incremental load."""

    overwrite = "overwrite"
    """Overwrite existing resources with loaded data."""

    skip = "skip"
    """Skip resources that already exist."""

    raise_error = "raise_error"
    """Raise DuplicateResourceError when a duplicate is found."""


class Ref:
    """Metadata marker for a field that references another SpecStar resource.

    Use with ``Annotated`` to annotate a ``str`` field that holds a reference
    to another SpecStar resource.

    By default ``ref_type`` is ``RefType.resource_id``, meaning the field
    stores a ``resource_id`` and participates in referential integrity.

    Set ``ref_type=RefType.revision_id`` for version-aware references where
    the field may store either a ``revision_id`` (pinned) or a ``resource_id``
    (meaning *latest*).  Revision refs are always ``on_delete=dangling``.

    Example::

        class Monster(Struct):
            zone_id: Annotated[str, Ref("zone")]
            guild_id: Annotated[
                str | None, Ref("guild", on_delete=OnDelete.set_null)
            ] = None
            owner_id: Annotated[str, Ref("character", on_delete=OnDelete.cascade)]
            zone_snapshot_id: Annotated[str, Ref("zone", ref_type=RefType.revision_id)]
    """

    __slots__ = ("resource", "on_delete", "ref_type")

    def __init__(
        self,
        resource: str,
        *,
        on_delete: OnDelete = OnDelete.dangling,
        ref_type: RefType = RefType.resource_id,
    ) -> None:
        self.resource = resource
        self.on_delete = OnDelete(on_delete)
        self.ref_type = RefType(ref_type)
        if self.ref_type != RefType.resource_id and self.on_delete != OnDelete.dangling:
            raise ValueError(
                f"Ref({resource!r}) with ref_type={self.ref_type!r} "
                f"requires on_delete=OnDelete.dangling, "
                f"got on_delete={self.on_delete!r}."
            )

    def __repr__(self) -> str:
        parts = [repr(self.resource), f"on_delete={self.on_delete!r}"]
        if self.ref_type != RefType.resource_id:
            parts.append(f"ref_type={self.ref_type!r}")
        return f"Ref({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Ref):
            return NotImplemented
        return (
            self.resource == other.resource
            and self.on_delete == other.on_delete
            and self.ref_type == other.ref_type
        )

    def __hash__(self) -> int:
        return hash((self.resource, self.on_delete, self.ref_type))


class RefRevision:
    """Metadata marker for a field that references another resource's revision_id.

    .. deprecated:: 0.9.0
        Use ``Ref(resource, ref_type=RefType.revision_id)`` instead.

    Example::

        class Monster(Struct):
            zone_revision_id: Annotated[str, RefRevision("zone")]
    """

    __slots__ = ("resource",)

    def __init__(self, resource: str) -> None:
        import warnings

        warnings.warn(
            "RefRevision is deprecated. "
            "Use Ref(resource, ref_type=RefType.revision_id) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.resource = resource

    def __repr__(self) -> str:
        return f"RefRevision({self.resource!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RefRevision):
            return NotImplemented
        return self.resource == other.resource

    def __hash__(self) -> int:
        return hash(self.resource)


class _RefInfo(Struct, frozen=True, kw_only=True):
    """Describes a single reference relationship discovered from type annotations."""

    source: str
    """Name of the resource that contains the reference."""
    source_field: str
    """Field name on the source resource."""
    target: str
    """Name of the referenced resource."""
    ref_type: str
    """Either ``'resource_id'`` or ``'revision_id'``."""
    on_delete: OnDelete
    """Referential action on delete."""
    nullable: bool
    """Whether the field is nullable (Optional)."""
    is_list: bool = False
    """Whether the field is a list of references (e.g. ``list[Annotated[str, Ref(...)]]``)."""


def extract_refs(struct_type: type, source_name: str) -> list[_RefInfo]:
    """Scan a Struct's annotated fields and extract all Ref / RefRevision markers.

    Handles both direct ``Annotated[str, Ref(...)]`` and nullable
    ``Annotated[str | None, Ref(...)]`` forms.
    """
    from specstar.util.type_utils import get_hints

    refs: list[_RefInfo] = []
    try:
        hints = get_hints(struct_type)
    except Exception:
        return refs

    for field_name, hint in hints.items():
        _extract_from_hint(hint, field_name, source_name, refs)
    return refs


def _extract_from_hint(
    hint: Any,
    field_name: str,
    source_name: str,
    out: list[_RefInfo],
    *,
    is_list: bool = False,
) -> None:
    from specstar.util.type_utils import (
        get_inner_types,
        is_annotated_type,
        is_list_type,
        is_nullable_type,
        unwrap_annotated,
    )

    if is_annotated_type(hint):
        inner_type, metadata = unwrap_annotated(hint)
        nullable = is_nullable_type(inner_type)
        for meta in metadata:
            if isinstance(meta, Ref):
                out.append(
                    _RefInfo(
                        source=source_name,
                        source_field=field_name,
                        target=meta.resource,
                        ref_type=meta.ref_type.value,
                        on_delete=meta.on_delete,
                        nullable=nullable,
                        is_list=is_list,
                    )
                )
            elif isinstance(meta, RefRevision):
                out.append(
                    _RefInfo(
                        source=source_name,
                        source_field=field_name,
                        target=meta.resource,
                        ref_type="revision_id",
                        on_delete=OnDelete.dangling,
                        nullable=nullable,
                        is_list=is_list,
                    )
                )
        return

    # Fallback: unwrap Union/Optional and recurse into each arg
    # Detect list origin to propagate is_list flag
    if is_list_type(hint):
        is_list = True
    args = get_inner_types(hint)
    if args:
        for arg in args:
            _extract_from_hint(arg, field_name, source_name, out, is_list=is_list)


class DisplayName:
    """Annotation marker designating a ``str`` field as the display name.

    Usage::

        class Character(Struct):
            name: Annotated[str, DisplayName()]  # ← this field is the display name
            level: int = 1

    The SpecStar framework will inject ``x-display-name-field`` into the
    OpenAPI schema so the web frontend can show a friendly name instead of
    just the resource ID.
    """

    __slots__ = ("label",)

    def __init__(self, label: str | None = None) -> None:
        self.label = label

    def __repr__(self) -> str:
        if self.label is None:
            return "DisplayName()"
        return f"DisplayName({self.label!r})"


def extract_display_name(struct_type: type) -> str | None:
    """Return the field name annotated with :class:`DisplayName`, or ``None``."""
    from specstar.util.type_utils import find_annotated_fields

    fields = find_annotated_fields(struct_type, DisplayName)
    return fields[0] if fields else None


# ---------------------------------------------------------------------------
# Unique Constraint
# ---------------------------------------------------------------------------


class Unique:
    """Annotation marker that enforces uniqueness of a field.

    Use with ``Annotated`` to mark a field as unique among **non-deleted**
    resources of the same type.

    Semantics:
    - Soft-deleted resources are ignored.
    - ``None`` values are ignored (``None`` may repeat).

    SpecStar ensures the field is indexed and checks uniqueness on write
    operations (create/update/modify/patch) when the unique-relevant value
    changes.

    Usage::

        class User(Struct):
            username: Annotated[str, Unique()]
            email: Annotated[str, Unique()]
            nickname: Annotated[str | None, Unique()] = None  # None can repeat

    Raises:
        :exc:`UniqueConstraintError`: When a duplicate non-None value is detected.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "Unique()"


# ---------------------------------------------------------------------------
# Vector / Embedding
# ---------------------------------------------------------------------------


class Vector:
    """Annotation marker for a vector-typed field.

    Use with ``Annotated`` to declare a field that stores a numeric vector
    (``list[float]``) or an :class:`Embedding` instance.  Carries the
    indexing and encoding configuration for the field.

    Args:
        dim: The dimensionality of the stored vector.
        distance: Distance metric to use for similarity comparisons.  When
            ``None`` (default), the framework falls back to the query-time
            metric and finally to ``"cosine"``.
        encoder: Name of a registered encoder.  Used when querying with a
            ``str`` and (for :class:`Embedding` fields) to auto-compute the
            vector at write time.  Resolution: field-level overrides
            model-level overrides global registration.

    Example::

        class Doc(Struct):
            embedding: Annotated[list[float], Vector(dim=1536, distance="cosine")]
    """

    __slots__ = ("dim", "distance", "encoder")

    def __init__(
        self,
        dim: int,
        *,
        distance: "Literal['cosine', 'l2', 'ip'] | None" = None,
        encoder: str | None = None,
    ) -> None:
        if dim <= 0:
            raise ValueError(f"Vector dim must be positive, got {dim}")
        if distance is not None and distance not in ("cosine", "l2", "ip"):
            raise ValueError(
                f"Vector distance must be one of 'cosine', 'l2', 'ip' or None, "
                f"got {distance!r}"
            )
        self.dim = dim
        self.distance = distance
        self.encoder = encoder

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Vector):
            return NotImplemented
        return (
            self.dim == other.dim
            and self.distance == other.distance
            and self.encoder == other.encoder
        )

    def __hash__(self) -> int:
        return hash((self.dim, self.distance, self.encoder))


class Embedding(Struct, kw_only=True):
    """A piece of content paired with its vector embedding.

    Analogous to :class:`Binary` for file blobs.  At write time, when only
    ``content`` is supplied, the framework computes ``vector`` via the
    field's registered encoder and fills ``content_hash`` (xxh3_128 of
    content) and ``encoder_id`` so that subsequent writes with unchanged
    content+encoder can skip the encoder call.
    """

    content: str
    vector: list[float] | UnsetType = UNSET
    content_hash: str | UnsetType = UNSET
    encoder_id: str | UnsetType = UNSET


class VectorFieldInfo(Struct, frozen=True, kw_only=True):
    """Per-field info about a Vector-annotated field, returned by
    :func:`extract_vector_field_infos`.
    """

    name: str
    marker: Vector
    is_embedding: bool
    nullable: bool


def extract_vector_field_infos(struct_type: type) -> "list[VectorFieldInfo]":
    """Return rich info for every ``Vector``-annotated field in *struct_type*.

    Returned list preserves definition order.  Each entry tells callers
    whether the inner type is :class:`Embedding` (vs raw ``list[float]``)
    and whether the field is nullable.
    """
    from specstar.util.type_utils import (
        get_hints,
        get_non_none_args,
        is_annotated_type,
        is_nullable_type,
        unwrap_annotated,
    )

    out: list[VectorFieldInfo] = []
    try:
        hints = get_hints(struct_type)
    except (TypeError, NameError):
        return out
    for field_name, hint in hints.items():
        if not is_annotated_type(hint):
            continue
        inner, metadata = unwrap_annotated(hint)
        marker: Vector | None = None
        for meta in metadata:
            if isinstance(meta, Vector):
                marker = meta
                break
        if marker is None:
            continue
        nullable = is_nullable_type(inner)
        # peel Optional/Union to find the value type
        value_type = inner
        if nullable:
            non_none = get_non_none_args(inner)
            if non_none:
                value_type = non_none[0]
        is_embedding = isinstance(value_type, type) and issubclass(
            value_type, Embedding
        )
        out.append(
            VectorFieldInfo(
                name=field_name,
                marker=marker,
                is_embedding=is_embedding,
                nullable=nullable,
            )
        )
    return out


class SetIndex:
    """Annotation marker for a list field that should get a dedicated
    set-overlap index.

    Use with ``Annotated`` on a homogeneous ``list[...]`` field. On Postgres the
    framework auto-creates a dedicated array column + GIN (mirroring how
    :class:`Vector` fields auto-create pgvector columns), so
    ``QB[field].contains_any([...])`` runs as one indexed ``&&`` overlap instead
    of a per-candidate containment fan-out. Other backends serve ``contains_any``
    from the shared path and need no shadow column. The element type is inferred
    from the annotation (``list[str]`` → text, ``list[int]`` → bigint,
    ``list[float]`` → double precision).

    Example::

        class Foo(Struct):
            keys: Annotated[list[str], SetIndex()]
    """

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SetIndex)

    def __hash__(self) -> int:
        return hash(SetIndex)


class SetIndexFieldInfo(Struct, frozen=True, kw_only=True):
    """Per-field info about a :class:`SetIndex`-annotated field, returned by
    :func:`extract_set_index_field_infos`."""

    name: str
    elem_type: type


def extract_set_index_field_infos(struct_type: type) -> "list[SetIndexFieldInfo]":
    """Return info for every ``SetIndex``-annotated list field, with the list
    element type inferred from the annotation (``list[str]`` → ``str``)."""
    from typing import get_args

    from specstar.util.type_utils import (
        get_hints,
        get_non_none_args,
        is_annotated_type,
        is_nullable_type,
        unwrap_annotated,
    )

    out: list[SetIndexFieldInfo] = []
    try:
        hints = get_hints(struct_type)
    except (TypeError, NameError):
        return out
    for field_name, hint in hints.items():
        if not is_annotated_type(hint):
            continue
        inner, metadata = unwrap_annotated(hint)
        if not any(isinstance(meta, SetIndex) for meta in metadata):
            continue
        # Peel Optional/Union to the list type, then read its element type.
        value_type = inner
        if is_nullable_type(inner):
            non_none = get_non_none_args(inner)
            if non_none:
                value_type = non_none[0]
        args = get_args(value_type)
        elem_type = args[0] if args else str
        out.append(SetIndexFieldInfo(name=field_name, elem_type=elem_type))
    return out


class SortIndex:
    """Annotation marker for a scalar field that should get a btree index.

    The shared ``indexed_data`` GIN is a value->row inverted index with no
    ordering: it answers ``@>``/``?``/``?|``/``?&`` and nothing else. Ranges and
    ``ORDER BY`` therefore cannot be served by it at any index size, and stay
    full scans / full sorts. Declaring ``SortIndex`` makes Postgres build a btree
    over the extract ``(indexed_data->'field')``, which serves range, ``ORDER BY``
    and equality from ONE index.

    Opt-in and index-only: no column, no backfill, no write-path change.
    ``indexed_data`` remains the single source of truth and the index is a pure
    derivation of it, so adding or removing the annotation costs a
    ``CREATE``/``DROP INDEX`` and nothing else — the index's absence can only cost
    speed, never correctness. That is the point: declare it once a field has
    STOPPED changing shape, and drop it freely if it starts again.

    Postgres-only native acceleration (like :class:`SetIndex` / :class:`Vector`);
    other backends ignore it. Use :class:`SetIndex`, not this, for list fields — a
    btree over a jsonb array would order by the whole array.

    Example::

        class Doc(Struct):
            collection_id: str  # JSONB only, zero DDL
            quality_score: Annotated[int, SortIndex()]  # stabilised → promote
    """

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SortIndex)

    def __hash__(self) -> int:
        return hash(SortIndex)


class SortIndexFieldInfo(Struct, frozen=True, kw_only=True):
    """Per-field info about a :class:`SortIndex`-annotated field, returned by
    :func:`extract_sort_index_field_infos`."""

    name: str
    value_type: type


def extract_sort_index_field_infos(struct_type: type) -> "list[SortIndexFieldInfo]":
    """Return info for every ``SortIndex``-annotated scalar field.

    ``Optional[T]`` is peeled to ``T``; a JSON null sorts into its own jsonb band,
    which is the desired SQL semantics (never matches a range, sorts to one end).

    Raises ``TypeError`` for a list field — that is :class:`SetIndex`'s job.
    """
    from specstar.util.type_utils import (
        get_hints,
        get_non_none_args,
        is_annotated_type,
        is_nullable_type,
        unwrap_annotated,
    )

    out: list[SortIndexFieldInfo] = []
    try:
        hints = get_hints(struct_type)
    except (TypeError, NameError):
        return out
    for field_name, hint in hints.items():
        if not is_annotated_type(hint):
            continue
        inner, metadata = unwrap_annotated(hint)
        if not any(isinstance(meta, SortIndex) for meta in metadata):
            continue
        value_type = inner
        if is_nullable_type(inner):
            non_none = get_non_none_args(inner)
            if non_none:
                value_type = non_none[0]
        if _is_list_like(value_type):
            raise TypeError(
                f"{struct_type.__name__}.{field_name}: SortIndex cannot be used on a "
                f"list field — a btree over a jsonb array orders by the whole array. "
                f"Use SetIndex for element membership."
            )
        out.append(SortIndexFieldInfo(name=field_name, value_type=value_type))
    return out


def _is_list_like(tp: type) -> bool:
    from typing import get_origin

    return tp is list or get_origin(tp) is list


class TrigramIndex:
    """Annotation marker for a text or ``list[str]`` field that should get a
    pg_trgm GIN index for substring and fuzzy search.

    The shared ``indexed_data`` GIN (jsonb_ops) serves only ``@>``/``?`` and can
    never accelerate a substring ``LIKE`` or a trigram similarity. Declaring
    ``TrigramIndex`` makes Postgres build a ``gin_trgm_ops`` GIN over the text
    extract ``(indexed_data->>'field')``, which serves ``ILIKE '%x%'`` (exact
    substring) and ``word_similarity`` (typo/partial fuzzy) — the primitives
    behind ``.contains`` / ``.any().contains`` and ``.fuzzy`` / ``.similarity``.

    Opt-in and index-only: no column, no backfill, no write-path change.
    ``indexed_data`` remains the single source of truth and the index is a pure
    derivation of it, so adding or removing the annotation costs a
    ``CREATE``/``DROP INDEX`` and nothing else — the index's absence can only cost
    speed, never correctness.

    Accepts a scalar ``str`` field (the GIN goes over the value) OR a
    ``list[str]`` field (the GIN goes over the serialised-array text, which
    coarse-filters ``.any().contains()`` before an exact per-element recheck).
    Text only: a numeric field should use :class:`SortIndex`.

    Postgres-only native acceleration (like :class:`SortIndex` / :class:`SetIndex`
    / :class:`Vector`); other backends ignore it and serve the query by scan.

    Example::

        class Card(Struct):
            title: Annotated[str, TrigramIndex()]
            norm_keys: Annotated[list[str], TrigramIndex()]
    """

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TrigramIndex)

    def __hash__(self) -> int:
        return hash(TrigramIndex)


class TrigramIndexFieldInfo(Struct, frozen=True, kw_only=True):
    """Per-field info about a :class:`TrigramIndex`-annotated field, returned by
    :func:`extract_trigram_index_field_infos`. ``is_list`` is ``True`` for a
    ``list[str]`` field (the query rewrite then coarse-filters + rechecks)."""

    name: str
    is_list: bool


def extract_trigram_index_field_infos(
    struct_type: type,
) -> "list[TrigramIndexFieldInfo]":
    """Return info for every ``TrigramIndex``-annotated field.

    Accepts a scalar ``str`` field or a ``list[str]`` field; ``Optional[T]`` is
    peeled first. Raises ``TypeError`` for a non-text field (a trigram index over
    a number is meaningless — use :class:`SortIndex`).
    """
    from typing import get_args

    from specstar.util.type_utils import (
        get_hints,
        get_non_none_args,
        is_annotated_type,
        is_nullable_type,
        unwrap_annotated,
    )

    out: list[TrigramIndexFieldInfo] = []
    try:
        hints = get_hints(struct_type)
    except (TypeError, NameError):
        return out
    for field_name, hint in hints.items():
        if not is_annotated_type(hint):
            continue
        inner, metadata = unwrap_annotated(hint)
        if not any(isinstance(meta, TrigramIndex) for meta in metadata):
            continue
        value_type = inner
        if is_nullable_type(inner):
            non_none = get_non_none_args(inner)
            if non_none:
                value_type = non_none[0]
        is_list = _is_list_like(value_type)
        elem_type = (get_args(value_type) or (None,))[0] if is_list else value_type
        if elem_type is not str:
            raise TypeError(
                f"{struct_type.__name__}.{field_name}: TrigramIndex is a text index "
                f"and accepts only a str or list[str] field. Use SortIndex for a "
                f"numeric field."
            )
        out.append(TrigramIndexFieldInfo(name=field_name, is_list=is_list))
    return out


def extract_vectors(struct_type: type) -> "list[tuple[str, Vector]]":
    """Return ``(field_name, Vector)`` pairs for Vector-annotated fields."""
    from specstar.util.type_utils import (
        get_hints,
        is_annotated_type,
        unwrap_annotated,
    )

    result: list[tuple[str, Vector]] = []
    hints = get_hints(struct_type)
    for field_name, hint in hints.items():
        if is_annotated_type(hint):
            _inner, metadata = unwrap_annotated(hint)
            for meta in metadata:
                if isinstance(meta, Vector):
                    result.append((field_name, meta))
                    break
    return result


def extract_unique_fields(struct_type: type) -> list[str]:
    """Return all field names annotated with :class:`Unique`.

    Arguments:
        struct_type: A ``msgspec.Struct`` subclass or any class whose fields
            may carry ``Unique()`` annotations.

    Returns:
        list[str]: Field names that carry a :class:`Unique` annotation, in
        definition order.  Returns an empty list if none are found or if
        type hints cannot be resolved.
    """
    from specstar.util.type_utils import find_annotated_fields

    return find_annotated_fields(struct_type, Unique)


class RevisionStatus(StrEnum):
    draft = "draft"
    stable = "stable"


class RevisionInfo(Struct, kw_only=True):
    """Metadata about a specific revision of a resource.

    This class contains essential information about a particular revision,
    """

    uid: UUID
    """The unique identifier for this revision.
    
    This is a UUID that uniquely identifies this specific revision of the resource.
    You don't need this value for most operations; use `resource_id` and `revision_id` instead.
    """
    resource_id: str
    """The ID of this revision of the resource."""
    revision_id: str
    """The unique identifier for the resource."""

    parent_revision_id: str | None = None
    """The ID of the parent revision, if any."""
    parent_schema_version: str | None | UnsetType = UNSET
    """The schema version of the parent revision, if any.
    
    This field is UNSET if the parent revision does not exist.
    """
    schema_version: str | None = None
    """The schema version of this revision.
    
    None is a valid schema version, indicating the default version.
    """
    data_hash: str | UnsetType = UNSET
    """The hash of the data for this revision.
    
    If UNSET, the hash has not been computed.
    """

    status: RevisionStatus
    """The status of this revision."""

    created_time: dt.datetime
    """The time when this revision was created."""
    updated_time: dt.datetime
    """The time when this revision was last updated.
    
    Note that this may only be different from created_time if the revision was 
    modified without creating a new revision (e.g., patching a draft).
    """
    created_by: str
    """The user who created this revision."""
    updated_by: str
    """The user who last updated this revision.

    Note that this may only be different from created_by if the revision was
    modified without creating a new revision (e.g., patching a draft).
    """

    @property
    def etag(self) -> str:
        """Concurrency token: ``<revision_id>@<data_hash>``.

        Bumps on every write — including in-place ``modify()`` that keeps the
        same ``revision_id`` but recomputes ``data_hash``. Pass back via
        ``expected_etag=`` on ``update`` / ``modify`` for compare-and-swap that
        catches concurrent in-place edits too, not just cross-revision moves.

        Falls back to ``revision_id`` when ``data_hash`` is UNSET (legacy rows).
        """
        if self.data_hash is UNSET:
            return self.revision_id
        return f"{self.revision_id}@{self.data_hash}"


class Resource(Struct, Generic[T]):
    info: RevisionInfo
    data: T


class UndecodableData(Struct):
    """Stand-in for a resource's ``data`` that couldn't be decoded into the
    model type, used when ``on_decode_error=OnDecodeError.raw``.

    It fills the ``data`` slot of the normal envelope — i.e. you get a
    ``Resource[UndecodableData]`` / ``SearchedResource[UndecodableData]`` whose
    ``meta`` / ``revision_info`` (including ``schema_version``) are still valid.
    Best-effort: ``data`` holds the parsed dict when the bytes are still
    parseable (the common incompatible-schema case); if even that fails (e.g.
    the encoding was changed), ``data`` is ``None`` and ``raw_base64`` preserves
    the original bytes losslessly.
    """

    decode_error: str
    data: dict | None = None
    raw_base64: str | None = None


class Binary(Struct):
    """A wrapper for binary data that handles storage optimization.

    When creating a resource, you can populate the `data` field with bytes.
    The system will automatically extract it, store it in the blob store,
    and populate `file_id` (which is the hash of the content) and `size`.
    The `data` field will be cleared in the stored resource.
    """

    file_id: str | UnsetType = UNSET
    """The unique identifier of the stored blob (hash of the content)."""

    size: int | UnsetType = UNSET
    """Size of the binary content in bytes."""

    content_type: str | UnsetType = UNSET
    """MIME type of the content."""

    data: bytes | UnsetType = UNSET
    """Binary content. Used for input or specific retrieval, usually None in storage."""


class BlobStreamInfo:
    """Metadata + chunk iterator for streaming blob downloads.

    Returned by :meth:`IBlobStore.get_stream` to enable
    :class:`~starlette.responses.StreamingResponse` without loading
    the entire blob into memory.
    """

    __slots__ = ("iterator", "size", "content_type", "file_id")

    def __init__(
        self,
        iterator: "Iterator[bytes]",
        *,
        size: int,
        content_type: "str | UnsetType" = UNSET,
        file_id: str = "",
    ):
        self.iterator = iterator
        self.size = size
        self.content_type = content_type
        self.file_id = file_id


class BlobResponse:
    """Encapsulates the blob store's preferred download strategy.

    Each blob store decides its own response order via
    :meth:`IBlobStore.get_response`.  The route handler converts
    this object into the appropriate Starlette response.

    Attributes:
        kind: One of ``"stream"``, ``"redirect"``, or ``"data"``.
        stream: A :class:`BlobStreamInfo` when *kind* is ``"stream"``.
        url: A redirect URL string when *kind* is ``"redirect"``.
        blob: A :class:`Binary` when *kind* is ``"data"``.
    """

    __slots__ = ("kind", "stream", "url", "blob")

    def __init__(
        self,
        kind: str,
        *,
        stream: "BlobStreamInfo | None" = None,
        url: "str | None" = None,
        blob: "Binary | None" = None,
    ):
        self.kind = kind
        self.stream = stream
        self.url = url
        self.blob = blob


class BlobUploadSession(Struct, kw_only=True):
    """Represents an active or completed blob upload session.

    Lifecycle (single upload)::

        pending → uploaded → finalized  (or → aborted)

    Lifecycle (chunked upload)::

        pending → uploading → … → uploading → finalized  (or → aborted)

    ``upload_method`` indicates how the client should deliver file bytes:

    - ``"proxy"``: PUT bytes to ``/blobs/upload-sessions/{upload_id}/content``.
      May be called multiple times for chunked uploads.
    - ``"single_put"``: upload directly to ``upload_url`` (e.g. S3 presigned PUT)
    """

    upload_id: str
    """Unique identifier for this upload session."""

    file_id: str
    """Pre-allocated file ID (may be a placeholder until finalize)."""

    status: Literal["pending", "uploading", "uploaded", "finalized", "aborted"] = (
        "pending"
    )
    """Current lifecycle state of the session."""

    upload_method: Literal["proxy", "single_put"] = "proxy"
    """How the client should deliver file bytes."""

    upload_url: str = ""
    """URL for the client to upload bytes (only relevant for ``single_put``)."""

    content_type: str | UnsetType = UNSET
    """MIME type of the content being uploaded."""

    size: int | None = None
    """Expected size of the content in bytes (``None`` if unknown)."""

    uploaded_size: int = 0
    """Number of bytes already uploaded (useful for progress tracking)."""

    total_parts: int | None = None
    """Expected number of parts for parallel chunked upload (``None`` if unknown)."""

    parts_received: list[int] = []
    """Sorted list of 1-based part numbers that have been received so far."""

    expires_at: dt.datetime | None = None
    """When this upload session expires (``None`` for no expiry)."""


class RawResource(Struct):
    info: RevisionInfo
    raw_data: bytes


class ResourceMeta(Struct, kw_only=True):
    """Metadata about a resource, including its current revision and status.

    This class provides essential information about a resource without including
    the full data content.
    """

    current_revision_id: str
    """The ID of the current revision of the resource."""
    resource_id: str
    """The unique identifier for the resource."""
    schema_version: str | None = None
    """The schema version of the resource.
    
    This indicates the version of the data schema that the resource conforms to.
    """

    total_revision_count: int
    """The total number of revisions for the resource."""

    created_time: dt.datetime
    """The time when the resource was created."""
    updated_time: dt.datetime
    """The time when the resource was last updated."""
    created_by: str
    """The user who created the resource."""
    updated_by: str
    """The user who last updated the resource."""

    is_deleted: bool = False
    """Indicates whether the resource has been deleted."""

    indexed_data: dict[str, Any] | UnsetType = UNSET
    """A dictionary of indexed fields for the resource, used for searching and sorting."""

    rev_status: RevisionStatus | UnsetType = UNSET
    """Status of the current revision (mirrors ``RevisionInfo.status``).

    Embedded so that callers can filter / sort resources by the status of
    their *current* revision without an extra read against ``IResourceStore``.

    ``UNSET`` indicates the resource was created before this field existed
    and has not yet been backfilled (see ``ResourceManager.backfill_revision_meta``).
    """

    rev_created_by: str | UnsetType = UNSET
    """User who created the current revision (mirrors ``RevisionInfo.created_by``)."""

    rev_updated_by: str | UnsetType = UNSET
    """User who last updated the current revision (mirrors ``RevisionInfo.updated_by``)."""

    rev_created_time: dt.datetime | UnsetType = UNSET
    """Creation time of the current revision (mirrors ``RevisionInfo.created_time``)."""

    rev_updated_time: dt.datetime | UnsetType = UNSET
    """Last update time of the current revision (mirrors ``RevisionInfo.updated_time``)."""


class SearchedResource(Struct, Generic[T]):
    """A resource item returned by list_resources.

    Each field may be the full type, a partial Struct (when partial fields
    are requested), or UNSET (when excluded via the *returns* parameter).
    """

    data: T | Struct | UnsetType = UNSET
    info: RevisionInfo | Struct | UnsetType = UNSET
    meta: ResourceMeta | Struct | UnsetType = UNSET


class ResourceAction(Flag):
    create = auto()
    get = auto()
    get_resource_revision = auto()
    list_revisions = auto()
    get_meta = auto()
    search_resources = auto()
    update = auto()
    patch = auto()
    switch = auto()
    delete = auto()
    permanently_delete = auto()
    restore = auto()
    dump = auto()
    load = auto()
    migrate = auto()
    modify = auto()
    prune = auto()

    create_or_update = create | update | modify

    read = get | get_meta | get_resource_revision | list_revisions
    read_list = search_resources
    write = create | update | modify | patch
    lifecycle = switch | delete | permanently_delete | restore
    backup = dump | load | migrate
    full = read | read_list | write | lifecycle | backup
    owner = read | patch | update | modify | lifecycle


class IMigration(ABC, Generic[T]):
    """Interface for handling data migration between different schema versions.

    This interface defines the contract for migrating resource data when schema
    versions change. Implementations should handle the transformation of data
    from older schema versions to the current version.
    """

    @abstractmethod
    def migrate(self, data: IO[bytes], schema_version: str | None) -> T:
        """Migrate resource data from an older schema version to the current version.

        Args:
            data: Binary stream containing the serialized resource data
            schema_version: The schema version of the input data, or UNSET if unknown

        Returns:
            T: The migrated data object in the current schema format

        Raises:
            ValueError: If the schema version is not supported
        """
        ...

    @property
    @abstractmethod
    def schema_version(self) -> str | None:
        """The target schema version for this migration.

        Returns:
            str | None: The schema version that this migration targets,
            or None if no specific version is targeted.
        """
        ...


class MergePatch(dict):
    """An RFC 7386 JSON Merge Patch payload.

    A thin ``dict`` marker so :meth:`IResourceManager.patch` /
    :meth:`IResourceManager.modify` can tell a *merge patch* (partial update;
    ``null`` deletes a field) apart from full replacement data — mirroring how
    ``jsonpatch.JsonPatch`` marks an RFC 6902 operation list. Construct it
    around the changed fields::

        mgr.patch(rid, MergePatch({"qty": 50}))  # merge, keep other fields
    """

    @property
    def patch(self) -> dict:
        """The merge-patch payload.

        Mirrors ``jsonpatch.JsonPatch.patch`` so the Patch event payload (which
        reads ``patch_data.patch``) resolves uniformly for both flavors.
        """
        return dict(self)


class PatchManyResult(Struct):
    """Outcome of a :meth:`IResourceManager.patch_many` fan-out (#434).

    Counts, not a list of ``RevisionInfo``: a fan-out can cover a hundred
    thousand rows, and returning one struct per row would undo the memory
    ceiling the batching exists to hold. Only rows that went wrong are named,
    so the report stays proportional to the trouble rather than to the work.

    ``patched`` and ``unchanged`` are separate because a *re-run* of a mirror
    fan-out is expected to be almost entirely ``unchanged`` — identical data
    hashes to the same revision and is never written twice — while the caller
    still wants to report how many rows it actually moved.
    """

    patched: int = 0
    """Rows that were written; a new revision exists."""

    unchanged: int = 0
    """Rows the patch was a no-op for. No revision was created."""

    conflicts: list[str] = _msgspec_field(default_factory=list)
    """Rows whose current revision moved between selection and write.

    Left untouched. Re-running the same call picks them up: a patch is
    idempotent and a no-op costs no revision.
    """

    failures: list[tuple[str, str]] = _msgspec_field(default_factory=list)
    """``(resource_id, reason)`` for rows that could not be patched — denied by
    a permission checker, deleted mid-fan-out, or failed to encode."""

    @property
    def total(self) -> int:
        """How many rows the query selected."""
        return self.patched + self.unchanged + len(self.conflicts) + len(self.failures)


class IResourceManager(ABC, Generic[T]):
    """Interface for managing versioned resources with full lifecycle support.

    This is the core interface for SpecStar's resource management system.
    It provides CRUD operations, versioning, search, permissions, and more.
    """

    @property
    @abstractmethod
    def user(self) -> str:
        """Get the current user from context.

        Returns:
            str: The current user identifier.

        Raises:
            LookupError: If no user is set in the context.
        """

    @property
    @abstractmethod
    def now(self) -> dt.datetime:
        """Get the current timestamp from context.

        Returns:
            datetime: The current timestamp.

        Raises:
            LookupError: If no timestamp is set in the context.
        """

    @property
    @abstractmethod
    def user_or_unset(self) -> str | UnsetType:
        """Get the current user from context, or UNSET if not available.

        Returns:
            str | UnsetType: The current user identifier or UNSET.
        """

    @property
    @abstractmethod
    def now_or_unset(self) -> dt.datetime | UnsetType:
        """Get the current timestamp from context, or UNSET if not available.

        Returns:
            datetime | UnsetType: The current timestamp or UNSET.
        """

    @property
    @abstractmethod
    def resource_type(self) -> type[T]:
        """Get the resource data type managed by this manager.

        Returns:
            type[T]: The resource type class.
        """

    @abstractmethod
    def migrate(
        self,
        resource_id: str,
        *,
        revision_id: str | UnsetType = UNSET,
    ) -> ResourceMeta:
        """Migrate a resource to the latest schema version.

        When *revision_id* is ``UNSET`` (the default), the **current**
        revision (``meta.current_revision_id``) is migrated and
        ``meta.schema_version`` is updated accordingly.

        When *revision_id* is provided, only **that specific revision**
        is migrated.  ``meta.schema_version`` is **not** changed (it
        should already have been updated by a prior call that migrated
        the current revision).

        Args:
            resource_id: The ID of the resource to migrate.
            revision_id: Optional specific revision to migrate.  If
                ``UNSET``, the current revision is migrated.

        Returns:
            ResourceMeta: The (possibly updated) metadata.

        Raises:
            ValueError: If migration logic is not configured.
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If *revision_id* does not exist.
        """

    @property
    @abstractmethod
    def schema_version(self) -> str:
        """Get the current schema version for this resource type.

        Returns:
            str: The schema version identifier.

        Raises:
            ValueError: If schema version is not configured.
        """

    @property
    @abstractmethod
    def resource_name(self) -> str:
        """Get the name of this resource type.

        Returns:
            str: The resource name.
        """

    @abstractmethod
    def using(
        self,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        *,
        resource_id: str | UnsetType = UNSET,
    ) -> AbstractContextManager[Any]:
        """Context manager to provide operation context for write operations.

        Returns a context-capturing proxy (``ResourceOps``) that records
        the supplied ``user``, ``now``, and ``resource_id`` values.  Each
        method call on the proxy re-applies its captured context, so
        multiple proxies created from the same manager do not interfere
        with each other.

        Resolution order (highest to lowest priority):
            1. Explicit keyword arguments on the method call
            2. Active ``using()`` scope (via the proxy)
            3. Manager defaults (``default_user``, ``default_now``)

        Args:
            user: The user performing the action.
            now: The current timestamp.
            resource_id: Specific resource ID to use for ``create()``.

        Yields:
            ResourceOps: A context-capturing proxy.
        """

    @abstractmethod
    def meta_provide(
        self,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        *,
        resource_id: str | UnsetType = UNSET,
    ) -> AbstractContextManager:
        """Context manager to provide metadata context (user, time, resource_id).

        .. deprecated::
            Use :meth:`using` instead.  ``meta_provide`` will be removed
            in a future release.

        Args:
            user: The user performing the action.
            now: The current timestamp.
            resource_id: Specific resource ID to use.

        Yields:
            None: Use this as a context manager with 'with' statement.
        """

    @abstractmethod
    def create(
        self,
        data: T,
        *,
        status: RevisionStatus | UnsetType = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
        resource_id: str | UnsetType = UNSET,
    ) -> RevisionInfo:
        """Create a new resource.

        This method creates a new resource with an initial revision. The resource
        will be assigned a unique resource_id and revision_id.

        Args:
            data: The resource data object conforming to the resource type.
            status: The initial status of the resource (default: stable).
            user: The user performing the action.  Overrides any active
                ``using()`` scope or manager default.
            now: The current timestamp.  Overrides any active ``using()``
                scope or manager default.
            resource_id: Specific resource ID to use instead of
                auto-generating one.

        Returns:
            RevisionInfo: Metadata about the created revision including
                resource_id, revision_id, created_time, created_by, and status.

        Note:
            Binary fields (Binary type) will be automatically extracted and stored
            in the blob store if configured.
        """

    @abstractmethod
    def exists(self, resource_id: str) -> bool:
        """Check if a resource exists.

        Args:
            resource_id: The ID of the resource to check.

        Returns:
            bool: True if the resource exists, False otherwise.
        """

    @abstractmethod
    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        """Check if a specific revision of a resource exists.

        Args:
            resource_id: The ID of the resource.
            revision_id: The revision ID to check.

        Returns:
            bool: True if the revision exists, False otherwise.
        """

    @abstractmethod
    def get(
        self,
        resource_id: str,
        *,
        revision_id: str | UnsetType = UNSET,
        schema_version: str | None | UnsetType = UNSET,
        include_deleted: bool = False,
    ) -> Resource[T]:
        """Get the current revision of the resource.

        Args:
            resource_id (str): the id of the resource to get.
            revision_id (str | UnsetType): the id of a specific revision to get.
              If UNSET, the current revision is returned.
            include_deleted (bool): if True, return the requested revision even
              for a soft-deleted resource instead of raising
              ``ResourceIsDeletedError``. Mirrors ``get_meta()``. Defaults to
              ``False``.
        Returns:
            resource (Resource[T]): the resource with its data and revision info.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted and
              *include_deleted* is ``False``.

        ---

        Returns the current revision of the specified resource. The current revision
        is determined by the `current_revision_id` field in ResourceMeta.

        This method will raise different exceptions based on the resource state:
        - ResourceIDNotFoundError: The resource ID does not exist in storage
        - ResourceIsDeletedError: The resource exists but is marked as deleted
          (is_deleted=True) and *include_deleted* is False

        To read a soft-deleted resource's revision pass ``include_deleted=True``,
        or use ``restore()`` first to make it accessible again.
        """

    @abstractmethod
    def get_partial(
        self,
        resource_id: str,
        revision_id: str,
        partial: Iterable[str | JsonPointer],
        *,
        schema_version: "str | None | UnsetType" = UNSET,
    ) -> Struct:
        """Get a partial view of the resource data for a specific revision.

        Args:
            resource_id (str): the id of the resource.
            revision_id (str): the id of the specific revision to retrieve.
            partial (Iterable[str | JsonPointer]): list of field paths to include in the result.
        Returns:
            partial_data (Struct): a Struct containing only the requested fields.
        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            RevisionIDNotFoundError: if revision id does not exist for this resource.

        Retrieves a subset of the resource's data for the specified revision,
        based on the provided list of field paths. This allows clients to fetch
        only the data they need, reducing bandwidth and processing overhead.
        This method does NOT check the is_deleted status of the resource metadata,
        allowing access to revisions of soft-deleted resources for audit and
        recovery purposes.
        The returned Struct contains only the fields specified in the `partial`
        argument, preserving the original data structure for those fields.
        """

    @abstractmethod
    def get_revision_info(
        self,
        resource_id: str,
        revision_id: str | UnsetType = UNSET,
        *,
        schema_version: "str | None | UnsetType" = UNSET,
    ) -> RevisionInfo:
        """Get the RevisionInfo for a specific revision of the resource.

        Args:
            resource_id (str): the id of the resource.
            revision_id (str | UnsetType): the id of the specific revision to retrieve.
              If UNSET, the current revision is returned.
        Returns:
            info (RevisionInfo): the metadata of the specified revision.
        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            RevisionIDNotFoundError: if revision id does not exist for this resource.

        Retrieves the RevisionInfo metadata for a specific revision of the resource.
        If revision_id is UNSET, the current revision's info is returned. This method does NOT
        check the is_deleted status of the resource metadata, allowing access to revisions of
        soft-deleted resources for audit and recovery purposes.
        """

    @abstractmethod
    def get_resource_revision(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None | UnsetType = UNSET,
    ) -> Resource[T]:
        """Get a specific revision of the resource.

        Args:
            resource_id (str): the id of the resource.
            revision_id (str): the id of the specific revision to retrieve.

        Returns:
            resource (Resource[T]): the resource with its data and revision info for the specified revision.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            RevisionIDNotFoundError: if revision id does not exist for this resource.

        ---

        Retrieves a specific historical revision of the resource identified by both
        resource_id and revision_id. Unlike get() which returns the current revision,
        this method allows access to any revision in the resource's history.

        This method does NOT check the is_deleted status of the resource metadata,
        allowing access to revisions of soft-deleted resources for audit and
        recovery purposes.

        The returned Resource contains both the data as it existed at that revision
        and the RevisionInfo with metadata about that specific revision.
        """

    @abstractmethod
    def list_revisions(self, resource_id: str) -> list[str]:
        """Get a list of all revision IDs for the resource.

        Args:
            resource_id (str): the id of the resource.

        Returns:
            list[str]: list of revision IDs for the resource, typically ordered chronologically.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.

        ---

        Returns all revision IDs that exist for the specified resource, providing
        a complete history of all revisions. This is useful for:
        - Browsing the complete revision history
        - Selecting specific revisions for comparison
        - Audit trails and compliance reporting
        - Determining available restore points

        The revision IDs are typically returned in chronological order (oldest to newest),
        but the exact ordering may depend on the implementation.

        This method does NOT check the is_deleted status of the resource, allowing
        access to revision lists for soft-deleted resources.
        """

    @abstractmethod
    def get_meta(self, resource_id: str, include_deleted: bool = False) -> ResourceMeta:
        """Get the metadata of the resource.

        Args:
            resource_id (str): the id of the resource to get metadata for.
            include_deleted (bool): if True, return metadata even for
                soft-deleted resources instead of raising
                ``ResourceIsDeletedError``.  Defaults to ``False``.

        Returns:
            meta (ResourceMeta): the metadata of the resource.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted and
                *include_deleted* is ``False``.

        ---

        Returns the metadata of the specified resource, including its current revision,
        total revision count, creation and update timestamps, and user information.
        When *include_deleted* is False (the default), this method raises
        ``ResourceIsDeletedError`` for soft-deleted resources.
        """

    @abstractmethod
    def get_blob(self, file_id: str) -> Binary:
        """Get the binary content of a blob by its file ID."""
        pass

    @abstractmethod
    def get_blob_url(self, file_id: str) -> str | None:
        """Get the direct download URL for a blob by its file ID, if available."""
        pass

    @abstractmethod
    def get_blob_stream(self, file_id: str) -> "BlobStreamInfo | None":
        """Get a streaming iterator for blob content, if supported."""
        pass

    @abstractmethod
    def get_blob_response(self, file_id: str) -> "BlobResponse":
        """Get the blob store's preferred download response for a blob.

        Delegates to the underlying :meth:`IBlobStore.get_response` so
        that the blob store itself decides the download strategy.
        """
        pass

    @abstractmethod
    def count_resources(self, query: ResourceMetaSearchQuery | None = None) -> int:
        """Count resources matching ``query``; ``None`` counts all."""

    @abstractmethod
    def search_resources(
        self, query: ResourceMetaSearchQuery | None = None
    ) -> list[ResourceMeta]:
        """Search for resources based on a query.

        Args:
            query (ResourceMetaSearchQuery): the search criteria and options.

        Returns:
            list[ResourceMeta]: list of resource metadata matching the query criteria.

        ---

        This method allows searching for resources based on various criteria defined
        in the ResourceMetaSearchQuery. The query supports filtering by:
        - Deletion status (is_deleted)
        - Time ranges (created_time_start/end, updated_time_start/end)
        - User filters (created_bys, updated_bys)
        - Pagination (limit, offset)
        - Sorting (sorts with direction and key)

        The results are returned as a list of resource metadata that match the specified
        criteria, ordered according to the sort parameters and limited by the
        pagination settings.
        """

    @abstractmethod
    def list_resources(
        self,
        query: ResourceMetaSearchQuery | None = None,
        *,
        returns: list[str] | None = None,
        partial: list[str] | None = None,
    ) -> list["SearchedResource[T]"]:
        """Search for resources and fetch their data in one call.

        Internally calls ``search_resources(query)`` (which triggers
        Before/After/OnSuccess/OnFailure SearchResources events), then
        fetches the requested sections for each matching resource.

        Args:
            query: search criteria (same as ``search_resources``).
            returns: sections to include per item.  Allowed values are
                ``"data"``, ``"info"``, ``"meta"``.  ``None`` means all three.
            partial: optional list of field paths to retrieve.  Paths may
                be prefixed with ``data/``, ``meta/``, or ``info/`` to
                target a specific section; unprefixed paths default to
                ``"data"``.

        Returns:
            list[SearchedResource[T]]: one item per matched resource.
                Fields excluded by *returns* are ``UNSET``.  Fields
                narrowed by *partial* are partial ``Struct`` instances.
        """

    @abstractmethod
    def update(
        self,
        resource_id: str,
        data: T,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """Update the data of the resource by creating a new revision.

        Args:
            resource_id (str): the id of the resource to update.
            data (T): the data to replace the current one.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            info (RevisionInfo): the metadata of the newly created revision.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted.

        ---

        Creates a new revision with the provided data and updates the resource's
        current_revision_id to point to this new revision. The new revision's
        parent_revision_id will be set to the previous current_revision_id.

        This operation will fail if the resource is soft-deleted. Use restore()
        first to make soft-deleted resources accessible for updates.

        For partial updates, use patch() instead of update().
        """

    @abstractmethod
    def create_or_update(
        self,
        resource_id: str,
        data: T,
        *,
        status: "RevisionStatus | UnsetType" = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        pass

    @abstractmethod
    def modify(
        self,
        resource_id: str,
        data: "T | JsonPatch | MergePatch | UnsetType" = UNSET,
        status: RevisionStatus | UnsetType = UNSET,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """Modify the data of the resource by update the current revision.

        Args:
            resource_id (str): the id of the resource to modify.
            data (T): the data to replace the current one.
        Returns:
            info (RevisionInfo): the metadata of the modified revision.
        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted.
            CannotModifyResourceError: if resource is not in draft status.
        """

    @abstractmethod
    def patch(
        self,
        resource_id: str,
        patch_data: "JsonPatch | MergePatch",
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> RevisionInfo:
        """Apply an RFC 6902 JSON Patch or RFC 7386 Merge Patch to the resource.

        Args:
            resource_id (str): the id of the resource to patch.
            patch_data (JsonPatch): RFC 6902 JSON Patch operations to apply.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            info (RevisionInfo): the metadata of the newly created revision.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted.

        ---

        Applies the provided JSON Patch operations to the current revision data
        and creates a new revision with the modified data. The patch operations
        follow RFC 6902 standard.

        This method internally:
        1. Gets the current revision data
        2. Applies the patch operations in-place
        3. Creates a new revision via update()

        This operation will fail if the resource is soft-deleted. Use restore()
        first to make soft-deleted resources accessible for patching.
        """

    @abstractmethod
    def patch_many(
        self,
        query: "Any",
        patch_data: "JsonPatch | MergePatch",
        *,
        max_bytes: int = ...,
        status: "RevisionStatus | UnsetType" = UNSET,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> PatchManyResult:
        """Apply one patch to every resource *query* selects (#434).

        The bulk shape of :meth:`patch`, for fan-outs that push a derived value
        onto many rows. The caller names the fields that change and never
        handles the data — which is the point, because once the read belongs to
        specstar it can be batched.

        Semantically **N patches whose reads and writes go through the bulk
        path**, not a second set of rules. Events (and therefore permission
        checks) fire per row; a row whose revision moved since it was selected
        is reported as a conflict instead of being overwritten; a no-op patch
        creates no revision. Failures are collected, not raised, so one
        unwritable row cannot strand the rest.

        The selection is exactly what *query* returns — **including its
        ``limit``**, which carries a process-wide default that a deployment can
        configure. :attr:`PatchManyResult.total` reports how many rows were
        selected so a truncated fan-out is visible rather than silent.

        Args:
            query: selects the rows to patch; ``None`` selects everything.
            patch_data: an RFC 6902 ``JsonPatch`` or RFC 7386 ``MergePatch``.
            max_bytes: ceiling on how much row data is held in memory at once.
                Bytes rather than a row count because row size varies by orders
                of magnitude between models. A row larger than the whole budget
                is still processed, on its own.
            status: status for the new revisions (default: stable).
            user: recorded as ``updated_by`` on every row written; each row
                keeps its own ``created_by``.
            now: the timestamp for the write.

        Returns:
            result (PatchManyResult): counts, plus the ids that conflicted or
            failed.
        """

    @abstractmethod
    def switch(
        self,
        resource_id: str,
        revision_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """Switch the current revision to a specific revision.

        Args:
            resource_id (str): the id of the resource.
            revision_id (str): the id of the revision to switch to.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            meta (ResourceMeta): the metadata of the resource after switching.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is soft-deleted.
            RevisionIDNotFoundError: if revision id does not exist.

        ---

        Changes the current_revision_id in ResourceMeta to point to the specified
        revision. This allows you to make any historical revision the current one
        without deleting any revisions. All historical revisions remain accessible.

        Behavior:
        - If switching to the same revision (current_revision_id == revision_id),
          returns the current metadata without any changes
        - Otherwise, updates current_revision_id, updated_time, and updated_by
        - Subsequent update/patch operations will use the new current revision as parent

        This operation will fail if the resource is soft-deleted. The revision_id
        must exist in the resource's revision history.
        """

    @abstractmethod
    def delete(
        self,
        resource_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """Mark the resource as deleted (soft delete).

        Args:
            resource_id (str): the id of the resource to delete.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            meta (ResourceMeta): the updated metadata with is_deleted=True.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ResourceIsDeletedError: if resource is already soft-deleted.

        ---

        This operation performs a soft delete by setting the `is_deleted` flag to True
        in the ResourceMeta. The resource and all its revisions remain in storage
        and can be recovered later.

        Behavior:
        - Sets `is_deleted = True` in ResourceMeta
        - Updates `updated_time` and `updated_by` to record the deletion
        - All revision data and metadata are preserved
        - Resource can be restored using restore()

        This operation will fail if the resource is already soft-deleted.
        This is a reversible operation that maintains data integrity while
        marking the resource as logically deleted.
        """

    @abstractmethod
    def restore(
        self,
        resource_id: str,
        *,
        user: str | UnsetType = UNSET,
        now: dt.datetime | UnsetType = UNSET,
    ) -> ResourceMeta:
        """Restore a previously deleted resource (undo soft delete).

        Args:
            resource_id (str): the id of the resource to restore.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            meta (ResourceMeta): the updated metadata with is_deleted=False.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.

        ---

        This operation restores a previously soft-deleted resource by setting
        the `is_deleted` flag back to False in the ResourceMeta. This undoes
        the soft delete operation.

        Behavior:
        - If resource is deleted (is_deleted=True):
          - Sets `is_deleted = False` in ResourceMeta
          - Updates `updated_time` and `updated_by` to record the restoration
          - Saves the updated metadata to storage
        - If resource is not deleted (is_deleted=False):
          - Returns the current metadata without any changes
          - No timestamps are updated

        All revision data and metadata remain unchanged. The resource becomes
        accessible again through normal operations only if it was previously deleted.

        Note: This method pairs with delete() to provide reversible
        soft delete functionality.
        """

    @abstractmethod
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
            resource_id (str): the id of the resource to permanently delete.
            user: The user performing the action.  Overrides any active
                ``using()`` scope or manager default.
            now: The current timestamp.  Overrides any active ``using()``
                scope or manager default.

        Returns:
            meta (ResourceMeta): the metadata of the resource before deletion.

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.

        ---

        Unlike soft delete (delete()), this operation physically removes:
        - All revision data for the resource
        - The resource metadata record

        The resource cannot be restored after this operation.
        This method does NOT require the resource to be soft-deleted first;
        it can permanently delete both active and soft-deleted resources.
        """

    @abstractmethod
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

        Hard-deletes historical revisions that are no longer worth keeping
        while always preserving the resource's current revision.  The set to
        delete is selected by two optional, composable knobs:

        Args:
            resource_id (str): the id of the resource whose revisions to prune.
            keep_last_n: keep the *n* most important revisions and prune the
                rest.  Importance is ranked by the sort key
                ``(is_lineal, created_time)`` descending — revisions on the
                current revision's ancestry chain (``parent_revision_id``) rank
                above collateral branches, and newer above older.  The current
                revision is always rank 1, so it is never pruned.  Must be
                ``>= 1``.
            before: prune revisions created strictly before this timestamp.
            user: The user performing the action.
            now: The current timestamp.

        Returns:
            list[str]: the revision ids that were pruned (empty when nothing
                matched).

        Raises:
            ResourceIDNotFoundError: if resource id does not exist.
            ValueError: if neither ``keep_last_n`` nor ``before`` is given, or
                if ``keep_last_n < 1``.

        ---

        When both knobs are given the kept set is their **union** (the
        conservative choice): a revision is pruned only when it is *both*
        beyond ``keep_last_n`` *and* older than ``before``.  The current
        revision is always retained regardless of the knobs.

        Like ``permanently_delete``, this only removes revision payload data
        (and underlying uid data once no surviving revision references it); it
        never touches blob-store contents — orphaned blobs are reclaimed
        separately (see #370).  ``total_revision_count`` is intentionally left
        unchanged because it seeds new ``revision_id`` values and must stay
        monotonic.  Works on soft-deleted resources too.
        """

    @abstractmethod
    def dump(
        self,
        query: Query | ResourceMetaSearchQuery | None = None,
    ) -> "Generator[MetaRecord | RevisionRecord | BlobRecord]":
        """Dump all resource data as a series of tar archive entries.

        Returns:
            Generator[tuple[str, IO[bytes]]]: generator yielding (filename, fileobj) pairs for each resource.

        ---

        Exports all resources in the manager as a series of tar archive entries.
        Each entry represents one resource and contains both its metadata and
        all revision data in a structured format.

        The generator yields tuples where:
        - filename: A unique identifier for the resource (typically the resource_id)
        - fileobj: An IO[bytes] object containing the tar archive data for that resource

        This method is designed for:
        - Complete data backup and export operations
        - Migrating resources between different systems
        - Creating portable resource archives
        - Bulk data transfer scenarios

        The tar archive format ensures that all resource information including
        metadata, revision history, and data content is preserved in a
        standardized, portable format.

        Note: This method does not filter by deletion status, so both active
        and soft-deleted resources will be included in the dump.
        """

    @abstractmethod
    def load(self, key: str, bio: IO[bytes]) -> None:
        """Load resource data from a tar archive entry.

        Args:
            key (str): the unique identifier for the resource being loaded.
            bio (IO[bytes]): the tar archive containing the resource data.

        ---

        Imports a single resource from a tar archive entry, typically created
        by the dump() method. The tar archive should contain both metadata
        and all revision data for the resource.

        The key parameter serves as the resource identifier and should match
        the filename used when the resource was dumped. The bio parameter
        contains the complete tar archive data for that specific resource.

        This method handles:
        - Extracting metadata and revision information from the archive
        - Restoring all historical revisions with proper parent-child relationships
        - Maintaining data integrity and revision ordering
        - Preserving timestamps, user information, and other metadata

        Use Cases:
        - Restoring resources from backup archives
        - Importing resources from external systems
        - Migrating data between different SpecStar instances
        - Bulk resource restoration operations

        Behavior:
        - If a resource with the same key already exists, the behavior depends on implementation
        - All revision history and metadata from the archive will be restored
        - The resource's deletion status and other flags are preserved as archived

        Note: This method should be used in conjunction with dump() for
        complete backup and restore workflows.
        """

    @abstractmethod
    def load_record(
        self,
        record: "MetaRecord | RevisionRecord | BlobRecord",
        on_duplicate: "OnDuplicate" = OnDuplicate.raise_error,
    ) -> bool:
        """Load a single dump record into storage.

        Args:
            record: A ``MetaRecord``, ``RevisionRecord``, or ``BlobRecord``
                instance (typically produced by :meth:`dump`).
            on_duplicate: Strategy when a resource with the same ID already
                exists.  Only meaningful for ``MetaRecord``; revision and
                blob records are always written.

        Returns:
            ``True`` if the record was stored, ``False`` if it was skipped
            (only possible when *on_duplicate* is :attr:`OnDuplicate.skip`).

        Raises:
            DuplicateResourceError: When the resource already exists and
                *on_duplicate* is :attr:`OnDuplicate.raise_error`.
        """

    @abstractmethod
    def load_records_bulk(
        self,
        meta_records: "list[MetaRecord]",
        revision_records: "list[RevisionRecord]",
        blob_records: "list[BlobRecord]",
        on_duplicate: "OnDuplicate" = OnDuplicate.raise_error,
    ) -> "LoadStats":
        """Batch-load multiple dump records into storage.

        Optimised counterpart to :meth:`load_record` for the bulk path.
        """

    @abstractmethod
    def restore_binary(self, data: T) -> T:
        """
        還原 data 中的 binary.data (如果是從 blob store 讀取).
        這對於需要讀取 Binary 原始資料時很有用.
        """

    @abstractmethod
    def register_async_create_job(
        self, job_resource_name: str, job_rm: "IResourceManager"
    ) -> None:
        """Register an async create-job ResourceManager for this resource.

        Args:
            job_resource_name: The registered name of the Job resource.
            job_rm: The ResourceManager instance for the Job resource.

        Raises:
            ValueError: If *job_resource_name* is already registered.
        """

    @property
    @abstractmethod
    def async_create_job_names(self) -> list[str]:
        """Return the names of all registered async create-job resources."""

    @abstractmethod
    def start_consume(
        self,
        *,
        block: bool = True,
        custom_creation: "Literal['all'] | list[str] | None" = None,
        custom_update: "Literal['all'] | list[str] | None" = None,
    ) -> "threading.Thread | None":
        """Start consuming jobs from the message queue.

        When both *custom_creation* and *custom_update* are ``None``
        (default), starts this resource’s own message-queue consumer.

        When *custom_creation* is ``"all"``, starts all async-create-job
        consumers that target this resource.

        When *custom_creation* is a list of job resource names, starts only
        those specific create-job consumers.

        When *custom_update* is ``"all"``, starts all async-update-job
        consumers that target this resource.

        When *custom_update* is a list of job resource names, starts only
        those specific update-job consumers.

        Both *custom_creation* and *custom_update* can be used together
        in the same call.

        Args:
            block: If ``True`` (default), block until the consumer thread(s)
                finish.  ``False`` returns immediately.
            custom_creation: Which async-create-job consumers to start.
            custom_update: Which async-update-job consumers to start.

        Raises:
            NotImplementedError: if message queue is not configured
                (when both *custom_creation* and *custom_update* are
                ``None``).
            ValueError: if a name in *custom_creation* or *custom_update*
                is not registered.
        """


class PermissionDeniedError(Exception):
    pass


class ResourceDecodeError(Exception):
    """A stored row could not be decoded into the current model type.

    Raised by read paths when ``on_decode_error=OnDecodeError.error``. Usually
    means an incompatible schema change without a version bump (the persisted
    bytes no longer fit the Struct). Maps to HTTP 422.
    """

    def __init__(self, resource_id: str, reason: str = ""):
        self.resource_id = resource_id
        self.reason = reason
        super().__init__(f"Cannot decode resource {resource_id!r}: {reason}")


class UnindexedQueryError(Exception):
    """A search filtered on a field that is neither indexed nor a
    ``ResourceMeta`` attribute, so the condition can never match.

    Raised by search paths when ``on_unindexed_query=OnUnindexedQuery.error``.
    Maps to HTTP 400 (the query references a field that cannot be filtered).
    """

    def __init__(self, fields: list[str], indexed: list[str]):
        self.fields = list(fields)
        self.indexed = list(indexed)
        super().__init__(
            f"Query filters on non-indexed field(s) {self.fields!r}; such "
            f"conditions match nothing. Indexed fields: {self.indexed!r}. Add "
            f"them via indexed_fields=/add_indexed_field(), or filter on a "
            f"ResourceMeta attribute."
        )


class SpecStarWarning(UserWarning):
    """Category for SpecStar advisory warnings (e.g. permissive defaults).

    Suppress with the standard ``warnings`` machinery, e.g.
    ``warnings.filterwarnings("ignore", category=SpecStarWarning)``.
    """


class ResourceNotFoundError(Exception):
    """Base class for resource/revision not found errors."""


class RevisionNotFoundError(ResourceNotFoundError):
    pass


class RevisionIDNotFoundError(RevisionNotFoundError):
    def __init__(self, resource_id: str, revision_id: str):
        super().__init__(
            f"Revision '{revision_id}' of Resource '{resource_id}' not found.",
        )
        self.resource_id = resource_id
        self.revision_id = revision_id


class ResourceIsDeletedError(ResourceNotFoundError):
    def __init__(self, resource_id: str):
        super().__init__(f"Resource '{resource_id}' is deleted.")
        self.resource_id = resource_id


class ResourceIDNotFoundError(ResourceNotFoundError):
    def __init__(self, resource_id: str):
        super().__init__(f"Resource '{resource_id}' not found.")
        self.resource_id = resource_id


class ResourceConflictError(Exception):
    pass


class SchemaConflictError(ResourceConflictError):
    pass


class RevisionNotMigratedError(SchemaConflictError):
    """Raised when switching to a revision whose schema version differs from
    the resource's current schema version.

    The revision must be migrated first via
    ``resource_manager.migrate(resource_id, revision_id=...)``.

    Attributes:
        resource_id: The resource that was being switched.
        revision_id: The target revision that is not yet migrated.
        revision_schema_version: The schema version stored on the target revision.
        current_schema_version: The resource-level (latest) schema version.
    """

    def __init__(
        self,
        resource_id: str,
        revision_id: str,
        revision_schema_version: str | None,
        current_schema_version: str | None,
    ) -> None:
        super().__init__(
            f"Revision '{revision_id}' of resource '{resource_id}' is at "
            f"schema version '{revision_schema_version}' but the resource is at "
            f"'{current_schema_version}'. Migrate the revision first with "
            f"migrate('{resource_id}', revision_id='{revision_id}')."
        )
        self.resource_id = resource_id
        self.revision_id = revision_id
        self.revision_schema_version = revision_schema_version
        self.current_schema_version = current_schema_version


class CannotModifyResourceError(ResourceConflictError):
    def __init__(self, resource_id: str):
        super().__init__(f"Resource '{resource_id}' cannot be modified.")
        self.resource_id = resource_id


class UniqueConstraintError(ResourceConflictError):
    """Raised when a field annotated with :class:`Unique` already has the given value
    on another (non-deleted) resource.

    Attributes:
        field: The name of the unique-constrained field.
        value: The duplicate value that caused the conflict.
        conflicting_resource_id: The ``resource_id`` that already holds the value.
    """

    def __init__(self, field: str, value: Any, conflicting_resource_id: str) -> None:
        super().__init__(
            f"Unique constraint violated: field '{field}' value {value!r} "
            f"already exists on resource '{conflicting_resource_id}'."
        )
        self.field = field
        self.value = value
        self.conflicting_resource_id = conflicting_resource_id


class DuplicateResourceError(ResourceConflictError):
    """Raised when loading a resource with an ID that already exists
    and *on_duplicate* is set to :attr:`OnDuplicate.raise_error`."""

    def __init__(self, resource_id: str) -> None:
        super().__init__(
            f"Duplicate resource '{resource_id}' already exists during load."
        )
        self.resource_id = resource_id


class PreconditionFailedError(Exception):
    """Raised when an optimistic-concurrency precondition fails.

    Use case: a client passes ``If-Match: <revision_id>`` (or
    ``expected_revision_id=`` on the manager call) to assert that they
    are updating *the revision they saw*; if the resource has moved on
    in the meantime, the write is refused. Maps to HTTP **412
    Precondition Failed**.

    Attributes:
        resource_id: The resource the precondition was checked on.
        expected_revision_id: The revision id the client asserted.
        actual_revision_id: The current revision id at check time.
    """

    def __init__(
        self,
        resource_id: str,
        expected_revision_id: str,
        actual_revision_id: str,
    ) -> None:
        super().__init__(
            f"Precondition failed for '{resource_id}': expected current "
            f"revision '{expected_revision_id}', got '{actual_revision_id}'."
        )
        self.resource_id = resource_id
        self.expected_revision_id = expected_revision_id
        self.actual_revision_id = actual_revision_id


class MissingOperationContextError(Exception):
    """Raised when a write operation is missing required context fields.

    This error occurs when a mutating method (create, update, delete, etc.)
    is called without the required ``user`` and/or ``now`` context.  Context
    can be supplied via:

    1. Explicit keyword arguments: ``mgr.create(data, user=..., now=...)``
    2. A ``using()`` scope: ``with mgr.using(user=..., now=...): ...``
    3. Manager defaults: ``ResourceManager(..., default_user=..., default_now=...)``

    Attributes:
        missing_fields: Names of the context fields that were not resolved.
        method_name: The name of the method that required the context.
    """

    def __init__(
        self,
        missing_fields: list[str],
        method_name: str | None = None,
    ) -> None:
        self.missing_fields = missing_fields
        self.method_name = method_name
        fields_str = ", ".join(missing_fields)
        if method_name:
            msg = (
                f"Missing required operation context for '{method_name}': "
                f"{fields_str}. "
                f"Provide via explicit kwargs, using() scope, or manager defaults."
            )
        else:
            msg = (
                f"Missing required operation context: {fields_str}. "
                f"Provide via explicit kwargs, using() scope, or manager defaults."
            )
        super().__init__(msg)


class DumpIncompleteError(Exception):
    """A dump could not read something the archive needed (issue #450).

    Raised by ``ResourceManager.dump`` in its default ``strict=True`` mode
    when a referenced blob will not load, or when a revision's payload will
    not decode (and so contributes none of the blob ids it references).
    Both used to be ``except Exception: pass``: the archive came out short
    and the dump reported success, which is the one failure mode a backup
    must never have, because it surfaces on restore day.

    Pass ``strict=False`` to dump anyway and read
    :class:`~specstar.resource_manager.dump_format.DumpStats` for what was
    skipped.
    """

    def __init__(self, resource_name: str, what: str, reason: str = ""):
        self.resource_name = resource_name
        self.what = what
        self.reason = reason
        super().__init__(
            f"Dump of {resource_name!r} is incomplete: could not read {what}"
            + (f" ({reason})" if reason else "")
            + ". Pass strict=False to dump anyway and read DumpStats."
        )


class ArchiveTruncatedError(ValueError):
    """A ``.acbak`` stream ended before its :class:`EofRecord` (issue #450).

    The archive is incomplete — a dump that died part-way through, or a
    transfer that stopped.  Cutting at a frame boundary leaves whole,
    decodable records behind, so nothing downstream notices: the load used
    to finish quietly and report ``loaded=0``, which is the one failure a
    backup must never have.

    Loading is **not** transactional.  Every batch written before the
    stream ran out is already persisted, so this reports what was applied
    rather than undoing it; ``stats`` carries the per-model counts as of
    the moment the truncation was found.

    Subclasses :class:`ValueError` so the archive-format handling that
    already exists (``SpecStar.load`` documents ``ValueError``; the import
    routes map it to ``400``) covers it without a second except clause.
    """

    def __init__(self, stats: Mapping[str, "LoadStats"] | None = None):
        self.stats = dict(stats) if stats else {}
        applied = ", ".join(
            f"{model}: loaded={s.loaded} skipped={s.skipped}"
            for model, s in sorted(self.stats.items())
        )
        super().__init__(
            "Archive ended without an EofRecord, so it is truncated. "
            "Records already written were NOT rolled back"
            + (f" ({applied})." if applied else ".")
        )


class ValidationError(ValueError):
    """Raised when data fails custom validation.

    Inherits from ValueError so it can be caught broadly.
    This is distinct from msgspec.ValidationError which handles
    type-level validation.
    """

    pass


class IValidator(ABC):
    """Interface for custom data validators.

    Implement this to create reusable validators that can be attached via
    `add_model(validator=...)` or `Schema(..., validator=...)`.

    Example::

        class PriceValidator(IValidator):
            def validate(self, data) -> None:
                if data.price < 0:
                    raise ValueError("Price must be non-negative")


        spec.add_model(Item, validator=PriceValidator())
    """

    @abstractmethod
    def validate(self, data: Any) -> None:
        """Validate the data.

        Raises:
            ValidationError:
                If validation fails. Raising `ValueError` is allowed and will be
                wrapped as `ValidationError` by SpecStar.
        """


class SpecialIndex(Enum):
    msgspec_tag = "msgspec_tag"


class IndexableField(Struct):
    """Defines a field that should be indexed for searching."""

    field_path: str  # JSON path to the field, e.g., "name", "user.email"
    field_type: type | SpecialIndex | UnsetType = (
        UNSET  # The type of the field (str, int, float, bool, datetime)
    )
    index_key: str | None = None
    """Optional override for the key under which the extracted value is
    stored in ``indexed_data``.  When ``None`` (default) the ``field_path``
    is used.  Useful when you want to extract from a nested path
    (e.g. ``summary.vector``) but expose the value under a short alias
    (e.g. ``summary``) for query convenience."""


class IConstraintChecker(ABC):
    """Interface for custom constraint checkers.

    Implement this to define reusable data constraints that are automatically
    enforced during create, update, modify, switch and restore operations.
    The framework handles all event lifecycle (before / on_success) and
    compensation (rollback) logic — you only need to implement the check.

    Example::

        class NoDuplicateEmailChecker(IConstraintChecker):
            def __init__(self, rm: ResourceManager) -> None:
                self.rm = rm

            def check(
                self, data: Any, *, exclude_resource_id: str | None = None
            ) -> None:
                email = getattr(data, "email", None)
                if email and self._email_exists(email, exclude_resource_id):
                    raise ValueError(f"Email {email!r} already in use")


        # Pass a factory callable (receives ResourceManager):
        spec.add_model(User, constraint_checkers=[NoDuplicateEmailChecker])
        # Or a lambda factory:
        spec.add_model(
            User, constraint_checkers=[lambda rm: NoDuplicateEmailChecker(rm)]
        )
    """

    @abstractmethod
    def check(self, data: Any, *, exclude_resource_id: str | None = None) -> None:
        """Validate that *data* satisfies this constraint.

        Args:
            data: The resource data (msgspec Struct instance).
            exclude_resource_id: When updating an existing resource, pass its
                ID so the checker can allow the resource to keep its own values.

        Raises:
            Exception: Raised by the checker to signal a constraint violation.
                The framework catches it, executes compensation, and re-raises
                it.
        """
        ...

    def data_relevant_changed(self, current_data: Any, new_data: Any) -> bool:
        """Return whether the fields relevant to this constraint changed.

        Called during *modify* to skip unnecessary checks when the
        constrained fields are unchanged.  The default implementation
        returns ``True`` (always re-check).  Override for optimisation.
        """
        return True


class TaskStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class JobRedirectInfo(Struct, kw_only=True):
    """Response body returned by async create actions (HTTP 202).

    When a custom create action uses ``async_mode='job'``, the endpoint
    returns this struct instead of ``RevisionInfo`` so the client can
    navigate to the auto-generated Job resource to track progress.

    Attributes:
        job_resource_name: The registered name of the auto-generated Job resource.
        job_resource_id: The resource ID of the newly created Job instance.
        redirect_url: A URL path to the Job detail endpoint.
    """

    job_resource_name: str
    """The registered name of the auto-generated Job resource."""

    job_resource_id: str
    """The resource ID of the newly created Job instance."""

    redirect_url: str
    """A URL path to the Job detail endpoint."""


class BackgroundTaskAccepted(Struct, kw_only=True):
    """Response body returned by background create actions (HTTP 202).

    When a custom create action uses ``async_mode='background'``, the
    endpoint returns this struct immediately while the handler continues
    executing in a FastAPI ``BackgroundTasks`` worker.  Unlike
    ``async_mode='job'``, no Job model is created and the task cannot be
    tracked from the frontend.

    Attributes:
        message: A human-readable acceptance message.
    """

    message: str
    """A human-readable acceptance message."""


class Job(Struct, Generic[T, D]):
    """A job wrapping a payload ``T`` with optional artifact type ``D``.

    The second type parameter ``D`` defaults to ``None`` so existing
    ``Job[T]`` usage is fully backward-compatible.

    Attributes:
        payload: The input data for the job.
        status: Current processing status.
        errmsg: Error or result message after processing.
        artifact: Optional typed output produced by the job handler.
        retries: Number of times the job has been retried.
    """

    payload: T
    """The actual job data/resource."""

    status: TaskStatus = TaskStatus.PENDING
    """Current status of the job."""

    errmsg: str | None = None
    """Result or error message after processing."""

    artifact: D | None = None
    """Optional typed output produced by the job handler.

    This field stores the result/artifact of job execution.
    The type ``D`` defaults to ``None``, so ``Job[T]`` is equivalent
    to ``Job[T, None]`` and ``artifact`` is simply ``None``.
    """

    retries: int = 0
    """Number of times the job has been retried."""

    max_retries: int | None = None
    """Per-job maximum retry count. If ``None`` (default), the queue-level
    ``max_retries`` setting is used. When set, this value takes precedence
    over the queue default.  For Celery queues the effective value is
    ``min(job.max_retries, queue.max_retries)`` because the Celery task
    decorator imposes a hard upper bound."""

    periodic_interval_seconds: int | None = None
    """If set, the job will be re-enqueued every N seconds after completion."""

    periodic_max_runs: int | None = None
    """Maximum number of times to run the periodic job. None means run indefinitely."""

    periodic_runs: int = 0
    """Number of times this periodic job has been executed."""

    periodic_initial_delay_seconds: int | None = None
    """Delay in seconds before the first execution. If None, executes immediately."""

    last_heartbeat_at: dt.datetime | None = None
    """Timestamp of the last heartbeat. Used to detect dead workers."""

    partition_key: str | None = None
    """Per-key serialization tag (``#342 #3``).

    Two jobs sharing the same ``partition_key`` are not run concurrently:
    the consumer holds back a pending job whose partition already has a
    PROCESSING peer and re-schedules it after a short delay. ``None``
    (default) means no serialization (every pending job is independently
    eligible, matching legacy behavior). Typical use: a per-tenant or
    per-aggregate write workflow where inflight reruns would trample each
    other.

    Enforcement strength depends on the backend (#384):

    * :class:`~specstar.message_queue.simple.SimpleMessageQueue` — **strict**.
      Its single consumer checks and claims in one thread, so two
      same-partition jobs never overlap.
    * :class:`~specstar.message_queue.rabbitmq.RabbitMQMessageQueue` and
      :class:`~specstar.message_queue.celery_queue.CeleryMessageQueue` —
      **best-effort**. Each worker checks for a PROCESSING peer before
      claiming and defers if it finds one, but the check-then-claim is not
      atomic across workers: two same-partition jobs that arrive at
      different workers simultaneously can briefly overlap. Order within a
      partition is **not** guaranteed.

    For strictly-serialized partitions on a multi-worker deployment, run a
    single consumer (``SimpleMessageQueue``); a future release may add an
    atomic cross-worker lock backend for strict guarantees.
    """

    idempotency_key: str | None = None
    """Exactly-once enqueue tag (``#342 #4``).

    When supplied to :meth:`~specstar.types.IMessageQueue.enqueue`, a second
    call with the same ``idempotency_key`` returns the *original* job instead
    of creating a new one — including after the original ran to completion.
    This is the standard contract for retry-prone client paths (mobile,
    webhook receivers, public APIs). Re-using a key with a *different*
    payload is rejected as a programming error. ``None`` (default) disables
    dedup.

    ``enqueue`` (and therefore this dedup) is available on every backend
    (#384). The lookup is best-effort across workers: two enqueues racing
    on the same key from different processes can briefly both miss and
    create two jobs, since the find-then-create is not atomic. For a single
    enqueue producer the dedup is exact.
    """


class IMessageQueue(ABC, Generic[T]):
    """Interface for a message queue that manages jobs as resources."""

    @abstractmethod
    def put(self, resource_id: str) -> Resource[Job[T]]:
        """Enqueue a job that has already been created.

        Args:
            resource_id: The ID of the job resource that was already created.

        Returns:
            The job resource.
        """
        ...

    @abstractmethod
    def pop(self) -> Resource[Job[T]] | None:
        """Dequeue the next pending job."""
        ...

    @abstractmethod
    def enqueue(
        self,
        payload: T,
        *,
        partition_key: str | None = None,
        idempotency_key: str | None = None,
    ) -> Resource[Job[T]]:
        """Create-and-enqueue a job in one step, honoring optional dedup and
        per-key serialization tags.

        This is the user-facing companion to :meth:`put` (which enqueues an
        already-created resource). It is implemented uniformly by every
        backend (#384); see :attr:`Job.partition_key` and
        :attr:`Job.idempotency_key` for the per-backend enforcement strength.

        Args:
            payload: The job payload.
            partition_key: When set, jobs sharing this key are not run
                concurrently (strict on a single consumer, best-effort on
                multi-worker backends). ``None`` = no serialization.
            idempotency_key: When set, a prior enqueue with the same key
                returns the *original* job. Re-using a key with a different
                payload raises :class:`ValueError`. ``None`` = no dedup.

        Returns:
            The job resource (the existing one on dedup, a fresh one
            otherwise).
        """
        ...

    @abstractmethod
    def complete(self, resource_id: str, result: str | None = None) -> Resource[Job[T]]:
        """Mark a job as completed."""
        ...

    @abstractmethod
    def fail(self, resource_id: str, error: str) -> Resource[Job[T]]:
        """Mark a job as failed."""
        ...

    @abstractmethod
    def recover_stale_jobs(self, heartbeat_timeout_seconds: float) -> list[str]:
        """Recover jobs stuck in PROCESSING status.

        This handles cases where a worker was killed (e.g. OOM) and left jobs
        in PROCESSING status. A recovered job spends one unit of its retry
        budget (``retries`` is incremented): while budget remains it is
        requeued for another attempt, and only once the budget is exhausted is
        it marked FAILED (#409). Store-backed queues requeue by moving the job
        back to PENDING; broker-backed queues instead leave it FAILED and rely
        on the broker to re-deliver the message.

        Args:
            heartbeat_timeout_seconds: Only recover jobs whose
                ``last_heartbeat_at`` is older than this many seconds (or is
                ``None``).  A value of 0 means recover ALL PROCESSING jobs
                regardless of heartbeat (use with caution in multi-worker
                setups).

        Returns:
            List of resource IDs that were recovered.
        """
        ...

    @abstractmethod
    def start_consume(self) -> None:
        """Start consuming jobs from the queue.

        Uses the callback function that was provided during construction.
        """
        ...

    @abstractmethod
    def stop_consuming(self) -> None:
        """Stop consuming jobs from the queue."""
        ...

    def get_logs(self, resource_id: str) -> str | None:
        """Retrieve the execution log text for a job.

        Args:
            resource_id: The job's resource ID.

        Returns:
            The log text as a string, or ``None`` if no logs exist
            (e.g. no blob store configured or the job was never executed).
        """
        return None


class IMessageQueueFactory(ABC):
    """Factory interface for creating message queues."""

    @abstractmethod
    def build(
        self, do: "Callable[[Resource[Job[T]]], None]"
    ) -> "Callable[[IResourceManager[Job[T]]], IMessageQueue[T]]":
        """Build a message queue factory function.

        Args:
            do: Callback function to process each job.

        Returns:
            A callable that accepts an IResourceManager and returns an IMessageQueue instance.
            The ResourceManager will inject itself when calling this function.
        """
        ...


__all__ = [
    "BackgroundTaskAccepted",
    "Binary",
    "BlobResponse",
    "BlobStreamInfo",
    "BlobUploadSession",
    "CannotModifyResourceError",
    "DisplayName",
    "DuplicateResourceError",
    "IConstraintChecker",
    "IMessageQueue",
    "IMessageQueueFactory",
    "IMigration",
    "IResourceManager",
    "IValidator",
    "IndexableField",
    "Job",
    "JobRedirectInfo",
    "MissingOperationContextError",
    "OnDelete",
    "OnDuplicate",
    "PermissionDeniedError",
    "PreconditionFailedError",
    "RawResource",
    "Ref",
    "RefRevision",
    "RefType",
    "Resource",
    "ResourceAction",
    "ResourceConflictError",
    "ResourceIDNotFoundError",
    "ResourceIsDeletedError",
    "ResourceMeta",
    "ResourceNotFoundError",
    "RevisionIDNotFoundError",
    "RevisionInfo",
    "RevisionNotFoundError",
    "RevisionNotMigratedError",
    "RevisionStatus",
    "SchemaConflictError",
    "SearchedResource",
    "SpecialIndex",
    "TaskStatus",
    "Unique",
    "UniqueConstraintError",
    "ValidationError",
    "extract_display_name",
    "extract_unique_fields",
]
