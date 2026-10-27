"""``TrigramIndex`` — opt-in pg_trgm GIN for substring / fuzzy search on a
text or ``list[str]`` field.

Like :class:`SortIndex`, the marker is opt-in and index-only: no column, no
backfill, no write-path change. ``indexed_data`` stays the single source of
truth, so adding or removing the annotation costs a ``CREATE`` / ``DROP INDEX``
and nothing else — an index's absence can only cost speed, never correctness.

Unlike ``SortIndex`` (scalar-only) it *also* accepts a ``list[str]`` field: the
GIN goes over the serialised-array text ``indexed_data->>'field'`` and coarse-
filters ``.any().contains()`` before an exact per-element recheck.

Service-free by design: ``tests/meta_store/`` is auto-marked ``integration`` and
never runs in CI. Live-Postgres behaviour is pinned by
``tests/meta_store/test_trigram_index.py``.
"""

import json
from typing import Annotated, Optional

import msgspec
import pytest

from specstar.query_types import (
    DataSearchCondition,
    DataSearchOperator,
    DataSearchQuantifier,
    ResourceMetaSortDirection,
    TrigramFuzzyCondition,
    TrigramSimilaritySort,
)
from specstar.resource_manager.meta_store.postgres import PostgresMetaStore
from specstar.types import TrigramIndex, extract_trigram_index_field_infos


def _builder(list_fields=(), trigram_indexes=()) -> PostgresMetaStore:
    """A store whose SQL builder runs without a live server (see #418's helper)."""
    store = object.__new__(PostgresMetaStore)
    store._list_fields = set(list_fields)
    store._set_columns = {}
    store._sort_indexes = set()
    store._trigram_indexes = set(trigram_indexes)
    return store


def _quant(op, value, quantifier, field_path="keys") -> DataSearchCondition:
    return DataSearchCondition(
        field_path=field_path, operator=op, value=value, quantifier=quantifier
    )


def test_extract_finds_a_scalar_text_field():
    class Foo(msgspec.Struct):
        title: Annotated[str, TrigramIndex()]
        plain: str  # not declared → ignored

    infos = extract_trigram_index_field_infos(Foo)
    assert {i.name: i.is_list for i in infos} == {"title": False}


def test_extract_marks_a_list_str_field_as_list():
    class Foo(msgspec.Struct):
        norm_keys: Annotated[list[str], TrigramIndex()]

    infos = extract_trigram_index_field_infos(Foo)
    assert {i.name: i.is_list for i in infos} == {"norm_keys": True}


def test_extract_rejects_a_numeric_scalar_field():
    class Foo(msgspec.Struct):
        score: Annotated[int, TrigramIndex()]

    with pytest.raises(TypeError, match="text"):
        extract_trigram_index_field_infos(Foo)


def test_extract_rejects_a_non_text_list_field():
    class Foo(msgspec.Struct):
        ids: Annotated[list[int], TrigramIndex()]

    with pytest.raises(TypeError, match="text"):
        extract_trigram_index_field_infos(Foo)


def test_optional_is_peeled_before_the_text_check():
    class Foo(msgspec.Struct):
        title: Annotated[Optional[str], TrigramIndex()] = None
        keys: Annotated[Optional[list[str]], TrigramIndex()] = None

    infos = extract_trigram_index_field_infos(Foo)
    assert {i.name: i.is_list for i in infos} == {"title": False, "keys": True}


def test_marker_nested_in_a_substruct_is_not_extracted():
    """Same reach as SortIndex / SetIndex / Vector: only top-level fields scan."""

    class Inner(msgspec.Struct):
        title: Annotated[str, TrigramIndex()]

    class Outer(msgspec.Struct):
        inner: Inner

    assert extract_trigram_index_field_infos(Outer) == []


def test_marker_is_exported_from_the_top_level_package():
    """Users annotate with ``from specstar import TrigramIndex`` like SortIndex."""
    import specstar

    assert specstar.TrigramIndex is TrigramIndex


# --- .any().eq() membership → GIN-probeable @> (no TrigramIndex needed) ------


def test_any_eq_routes_to_the_top_level_containment_probe():
    """``.any().eq(v)`` is exact element membership, so it can go through the
    table's ``indexed_data`` GIN (``indexed_data @> {"f": [v]}``) instead of a
    per-row ``EXISTS(jsonb_array_elements_text …)`` scan."""
    sql, params = _builder(list_fields=["keys"])._build_condition(
        _quant(DataSearchOperator.equals, "mol", DataSearchQuantifier.any)
    )
    assert sql == "indexed_data @> %s::jsonb"
    assert json.loads(params[0]) == {"keys": ["mol"]}


def test_all_eq_is_not_membership_and_keeps_the_universal_exists():
    """``.all().eq(v)`` means EVERY element equals v — not membership — so the
    ``@>`` short-circuit must not swallow it."""
    sql, _ = _builder()._build_condition(
        _quant(DataSearchOperator.equals, "mol", DataSearchQuantifier.all)
    )
    assert "@>" not in sql
    assert "NOT EXISTS" in sql


def test_any_contains_is_substring_not_membership_and_keeps_exists():
    """``.any().contains(v)`` is substring, which ``@>`` (membership) cannot do."""
    sql, _ = _builder()._build_condition(
        _quant(DataSearchOperator.contains, "ol", DataSearchQuantifier.any)
    )
    assert "@>" not in sql
    assert "strpos(val" in sql


def test_any_eq_none_keeps_exists_not_a_containment_probe():
    """``@> {"f": [null]}`` would MATCH a stored JSON null; the reference returns
    Unknown for ``== None``. ``None`` must stay on the EXISTS path (see
    ``_gin_probeable``)."""
    sql, _ = _builder()._build_condition(
        _quant(DataSearchOperator.equals, None, DataSearchQuantifier.any)
    )
    assert "@>" not in sql


# --- .any().contains() on a TrigramIndex field: coarse LIKE + EXISTS recheck --


def test_any_contains_on_a_trigram_field_prepends_the_coarse_like():
    """The coarse ``(indexed_data->>'keys') LIKE %s`` rides the gin_trgm_ops GIN;
    the EXISTS then rechecks per-element exactness. Substring stays exact."""
    sql, params = _builder(
        list_fields=["keys"], trigram_indexes=["keys"]
    )._build_condition(
        _quant(DataSearchOperator.contains, "ol", DataSearchQuantifier.any)
    )
    assert sql == (
        "((indexed_data->>'keys') LIKE %s AND "
        "EXISTS (SELECT 1 FROM jsonb_array_elements_text("
        "CASE WHEN jsonb_typeof(indexed_data->'keys') = 'array' "
        "THEN indexed_data->'keys' ELSE '[]'::jsonb END) AS e(val) "
        "WHERE strpos(val, %s) > 0))"
    )
    assert params == ["%ol%", "ol"]  # coarse param first, then the recheck param


def test_any_contains_without_a_trigram_index_stays_a_plain_exists():
    """Opt-in like SortIndex: the coarse LIKE is dead weight without the GIN, so
    an unannotated field keeps the exact EXISTS it had before."""
    sql, params = _builder(list_fields=["keys"])._build_condition(
        _quant(DataSearchOperator.contains, "ol", DataSearchQuantifier.any)
    )
    assert sql.startswith("EXISTS (")
    assert "LIKE" not in sql
    assert params == ["ol"]


def test_any_contains_escapes_like_metacharacters_in_the_coarse():
    """``%`` / ``_`` are LIKE wildcards; the coarse must match them literally
    (the recheck is literal, so an unescaped coarse would still be a superset —
    but escaping keeps it tight and the GIN's trigrams accurate)."""
    _sql, params = _builder(
        list_fields=["keys"], trigram_indexes=["keys"]
    )._build_condition(
        _quant(DataSearchOperator.contains, "a%b_c", DataSearchQuantifier.any)
    )
    assert params[0] == r"%a\%b\_c%"


@pytest.mark.parametrize("needle", ["a\\b", 'a"b', "a\x01b"])
def test_any_contains_with_json_unsafe_needle_falls_back_to_exists(needle):
    """The coarse runs over the SERIALISED array text, where ``"`` / ``\\`` /
    control chars are JSON-escaped — so such a needle can't be coarse-matched
    without a false negative. It must drop to the exact EXISTS scan."""
    sql, params = _builder(
        list_fields=["keys"], trigram_indexes=["keys"]
    )._build_condition(
        _quant(DataSearchOperator.contains, needle, DataSearchQuantifier.any)
    )
    assert sql.startswith("EXISTS (")
    assert "LIKE" not in sql
    assert params == [needle]


@pytest.mark.parametrize(
    "op", [DataSearchOperator.starts_with, DataSearchOperator.ends_with]
)
def test_any_starts_ends_with_share_the_same_coarse_like(op):
    """``starts_with`` / ``ends_with`` also imply "v occurs in the array text",
    so they ride the same coarse ``%v%`` and let the recheck pin the position."""
    sql, params = _builder(
        list_fields=["keys"], trigram_indexes=["keys"]
    )._build_condition(_quant(op, "m", DataSearchQuantifier.any))
    assert sql.startswith("((indexed_data->>'keys') LIKE %s AND EXISTS (")
    assert params[0] == "%m%"


def test_all_contains_never_gets_a_coarse_like():
    """An empty array matches ``all`` vacuously but its "[]" text fails the coarse
    ``%v%`` — so the coarse would wrongly drop it. ``all`` must stay pure EXISTS."""
    sql, _ = _builder(list_fields=["keys"], trigram_indexes=["keys"])._build_condition(
        _quant(DataSearchOperator.contains, "ol", DataSearchQuantifier.all)
    )
    assert "LIKE" not in sql
    assert "NOT EXISTS" in sql


# --- .fuzzy(): pg_trgm word_similarity, index-accelerated by default ----------


def test_fuzzy_without_a_threshold_uses_the_indexable_word_similarity_operator():
    """``%s <% (indexed_data->>'title')`` is what the gin_trgm_ops GIN serves, at
    the server's default word_similarity_threshold (0.6)."""
    sql, params = _builder(trigram_indexes=["title"])._build_fuzzy_condition(
        TrigramFuzzyCondition(field_path="title", query="mol")
    )
    # ``<%%`` — the ``%`` in the ``<%`` operator is doubled for psycopg2.
    assert sql == "%s <%% (indexed_data->>'title')"
    assert params == ["mol"]


def test_fuzzy_with_a_threshold_uses_the_exact_word_similarity_function():
    """A per-call threshold pins the cut-off exactly (the ``<%`` operator only
    reads the session GUC), at the cost of the index — v1 runs it as a scan."""
    sql, params = _builder(trigram_indexes=["title"])._build_fuzzy_condition(
        TrigramFuzzyCondition(field_path="title", query="mol", threshold=0.3)
    )
    assert sql == "word_similarity(%s, (indexed_data->>'title')) >= %s"
    assert params == ["mol", 0.3]


# --- .similarity().desc() ranking sort (compute-then-sort; no index ordering) -


def test_similarity_sort_desc_orders_by_word_similarity():
    sql, params = _builder(trigram_indexes=["title"])._build_trigram_similarity_order(
        TrigramSimilaritySort(
            field_path="title",
            query="mol",
            direction=ResourceMetaSortDirection.descending,
        )
    )
    assert sql == "word_similarity(%s, (indexed_data->>'title')) DESC"
    assert params == ["mol"]


def test_similarity_sort_asc_flips_the_direction():
    sql, params = _builder(trigram_indexes=["title"])._build_trigram_similarity_order(
        TrigramSimilaritySort(
            field_path="title",
            query="mol",
            direction=ResourceMetaSortDirection.ascending,
        )
    )
    assert sql == "word_similarity(%s, (indexed_data->>'title')) ASC"
    assert params == ["mol"]
