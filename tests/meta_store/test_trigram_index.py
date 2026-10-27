"""TrigramIndex against a live Postgres.

Two things must hold, and the second matters more than the first:

* the pg_trgm GIN is actually USED for a substring ``.contains`` (otherwise the
  annotation is a lie);
* declaring or dropping it NEVER changes an answer — the index is a pure
  derivation of live ``indexed_data``, so its absence can only cost speed.

A scalar ``.contains`` already compiles to ``indexed_data->>'field' LIKE %s``,
the exact expression the GIN is built on, so it becomes index-served the moment
the index exists — no query rewrite (that lands for the ``.any()`` list case in a
later phase). The SQL shape is pinned service-free in
``tests/test_trigram_index_marker.py``.
"""

import uuid
from datetime import UTC, datetime

import pytest

from specstar.query import QB
from specstar.query_types import (
    DataSearchCondition,
    DataSearchOperator,
    DataSearchQuantifier,
)
from specstar.resource_manager.meta_store.postgres import base_index_name
from specstar.types import ResourceMeta

from .common import get_meta_store

# id -> title (scalar text) ; keys (list[str])
ROWS = {
    "1": ("molecular biology", ["mol", "capping"]),
    "2": ("polymer chains", ["m4", "m40"]),
    "3": ("small molecule", ["ol"]),
    "4": ("unrelated", []),
}


def _meta(rid: str, title: str, keys: list[str]) -> ResourceMeta:
    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    return ResourceMeta(
        current_revision_id=f"rev_{rid}",
        resource_id=str(uuid.uuid4()),
        total_revision_count=1,
        created_time=base,
        updated_time=base,
        created_by="t",
        updated_by="t",
        is_deleted=False,
        indexed_data={"id": rid, "title": title, "keys": keys},
    )


def _seed(store, rows=ROWS):
    for rid, (title, keys) in rows.items():
        m = _meta(rid, title, keys)
        store[m.resource_id] = m
    return store


def _ids(store, cond) -> list[str]:
    return sorted(m.indexed_data["id"] for m in store.iter_search(cond.build()))


def _explain(store, sql: str) -> str:
    with store.transaction() as cur:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN " + sql.format(t=f'"{store.table_name}"'))
        return "\n".join(r[0] for r in cur.fetchall())


@pytest.fixture
def store():
    return _seed(get_meta_store("postgres"))


def test_the_index_is_applicable_to_a_scalar_contains(store):
    """The point of the annotation: a substring ``.contains`` uses the GIN.

    ``enable_seqscan=off`` asks whether the index MATCHES the query, not whether
    the planner prefers it on four rows (it never would).
    """
    store.ensure_trigram_index("title")
    plan = _explain(
        store,
        "SELECT count(*) FROM {t} WHERE indexed_data->>'title' LIKE '%mole%'",
    )
    assert store._trigram_idx_name("title") in plan, plan


def test_declaring_the_index_does_not_change_contains_results(store):
    """The invariant: index in, index out, same answers."""
    before = _ids(store, QB["title"].contains("mole"))
    assert before == ["1", "3"]  # "molecular biology", "small molecule"

    store.ensure_trigram_index("title")

    assert _ids(store, QB["title"].contains("mole")) == before


def test_detect_reports_pg_trgm_available(store):
    """The capability flag mirrors supports_native_vector_search."""
    assert store.supports_native_trigram_search is True


def test_ensure_trigram_index_is_idempotent_across_restarts(store):
    """It runs from add_model, i.e. on every process start."""
    store.ensure_trigram_index("title")
    store.ensure_trigram_index("title")
    store.ensure_trigram_index("title")
    assert _ids(store, QB["title"].contains("mole")) == ["1", "3"]


def test_dropping_the_index_leaves_results_identical(store):
    """Rollback is a pure perf change and cannot be wrong: same SQL either way."""
    store.ensure_trigram_index("title")
    with_index = _ids(store, QB["title"].contains("mole"))

    with store.transaction() as cur:
        cur.execute(f'DROP INDEX "{store._trigram_idx_name("title")}"')
    store._trigram_indexes.discard("title")

    assert _ids(store, QB["title"].contains("mole")) == with_index


def test_the_index_builds_over_rows_that_already_exist(store):
    """No backfill exists or is needed: the GIN is derived from live rows."""
    store.ensure_trigram_index("title")  # rows were seeded BEFORE this
    assert _ids(store, QB["title"].contains("mole")) == ["1", "3"]


def _index_exists(store, idx_name: str) -> bool:
    with store.transaction() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", [idx_name])
        return cur.fetchone()[0]


def test_a_list_field_gets_a_gin_and_any_contains_stays_correct(store):
    """The DDL builds for a list field too (the ``.any()`` rewrite that USES it
    lands in a later phase). Meanwhile ``.any().contains`` stays correct by scan."""
    store.ensure_trigram_index("keys")
    assert _index_exists(store, store._trigram_idx_name("keys"))
    # substring over elements: "ol" ⊂ "mol" (row 1) and "ol" itself (row 3).
    assert _ids(store, QB["keys"].any().contains("ol")) == ["1", "3"]


def test_any_eq_membership_uses_the_shared_jsonb_gin(store):
    """``.any().eq(v)`` needs no TrigramIndex — as exact membership it rides the
    always-present shared jsonb GIN via ``indexed_data @> {"keys": [v]}``, not a
    per-row unnest scan."""
    assert _ids(store, QB["keys"].any().eq("mol")) == ["1"]  # only the literal "mol"
    plan = _explain(
        store,
        'SELECT count(*) FROM {t} WHERE indexed_data @> \'{{"keys": ["mol"]}}\'',
    )
    assert base_index_name(store.table_name, "indexed_data_gin") in plan, plan


def test_any_eq_membership_matches_the_reference_answer(store):
    """Membership, not substring: "mol" hits only row 1, never "small molecule"
    (row 3's title) or the "m4"/"m40" keys."""
    assert _ids(store, QB["keys"].any().eq("mol")) == ["1"]
    assert _ids(store, QB["keys"].any().eq("m4")) == ["2"]
    assert _ids(store, QB["keys"].any().eq("nope")) == []


def _explain_cond(store, cond) -> str:
    """EXPLAIN the ACTUAL SQL specstar builds for *cond* (a raw condition)."""
    sql, params = store._build_condition(cond)
    with store.transaction() as cur:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute(
            f'EXPLAIN SELECT count(*) FROM "{store.table_name}" WHERE {sql}', params
        )
        return "\n".join(r[0] for r in cur.fetchall())


def _any_contains(field, value) -> DataSearchCondition:
    return DataSearchCondition(
        field_path=field,
        operator=DataSearchOperator.contains,
        value=value,
        quantifier=DataSearchQuantifier.any,
    )


def test_any_contains_on_a_trigram_list_field_uses_the_gin(store):
    """The whole point of P4: ``.any().contains`` stops being a per-row unnest.
    The coarse LIKE over the array text lets the planner bitmap-scan the GIN."""
    store.ensure_trigram_index("keys")
    plan = _explain_cond(store, _any_contains("keys", "mol"))
    assert store._trigram_idx_name("keys") in plan, plan


def test_any_contains_coarse_recheck_matches_the_reference(store):
    """The coarse is a superset; the EXISTS recheck restores exactness."""
    store.ensure_trigram_index("keys")
    # substring within an element: "ol" ⊂ "mol" (row 1), "ol" itself (row 3)
    assert _ids(store, QB["keys"].any().contains("ol")) == ["1", "3"]
    # "4" ⊂ "m4"/"m40" (row 2) but is no element on its own
    assert _ids(store, QB["keys"].any().contains("4")) == ["2"]
    assert _ids(store, QB["keys"].any().contains("zzz")) == []


def test_recheck_eliminates_a_structural_false_positive(store):
    """The coarse can match the ``", "`` join between elements; the recheck must
    drop such a row because no single element contains it."""
    m = _meta("9", "pair", ["ab", "cd"])
    store[m.resource_id] = m
    store.ensure_trigram_index("keys")
    # ", " is present in the serialised '["ab", "cd"]' but in no element
    assert _ids(store, QB["keys"].any().contains(", ")) == []
    # a bracket is structural too
    assert _ids(store, QB["keys"].any().contains("[")) == []
    # sanity: a real element substring still hits
    assert _ids(store, QB["keys"].any().contains("ab")) == ["9"]


def test_declaring_the_index_does_not_change_any_contains_results(store):
    """Index in, index out — same rows. The coarse+recheck must equal the plain
    EXISTS it replaces."""
    before = _ids(store, QB["keys"].any().contains("ol"))
    store.ensure_trigram_index("keys")
    assert _ids(store, QB["keys"].any().contains("ol")) == before == ["1", "3"]


def test_json_unsafe_needle_still_correct_via_the_exists_fallback(store):
    """A needle with a backslash cannot ride the coarse (the array text escapes
    it) — it drops to the exact EXISTS scan and must still find its row."""
    m = _meta("9", "escaped", ["a\\b", "plain"])  # element is raw a-backslash-b
    store[m.resource_id] = m
    store.ensure_trigram_index("keys")
    assert _ids(store, QB["keys"].any().contains("a\\b")) == ["9"]
    assert _ids(store, QB["keys"].any().contains("a\\z")) == []


# --- .fuzzy(): pg_trgm word_similarity search --------------------------------


def test_fuzzy_matches_a_fragment_of_a_word(store):
    """A short query matches a longer, similar word: "molecular" hits both the
    "molecular biology" and "small molecule" titles, "polymer" only its own."""
    assert _ids(store, QB["title"].fuzzy("molecular")) == ["1", "3"]
    assert _ids(store, QB["title"].fuzzy("polymer")) == ["2"]
    assert _ids(store, QB["title"].fuzzy("zzznope")) == []


def test_fuzzy_is_index_accelerated_but_works_without_the_index(store):
    """``<%`` needs no TrigramIndex to be correct — the index only turns the scan
    into a bitmap. Same rows before and after; the GIN serves the operator."""
    before = _ids(store, QB["title"].fuzzy("molecular"))
    store.ensure_trigram_index("title")
    assert _ids(store, QB["title"].fuzzy("molecular")) == before == ["1", "3"]
    plan = _explain(
        store,
        "SELECT count(*) FROM {t} WHERE 'molecular' <% (indexed_data->>'title')",
    )
    assert store._trigram_idx_name("title") in plan, plan


def test_fuzzy_on_a_list_field_matches_any_similar_element(store):
    """The array text word-splits on the quotes/commas, so ``.fuzzy`` on a list
    matches when any element is similar: "capp" ~ the "capping" key."""
    store.ensure_trigram_index("keys")
    assert _ids(store, QB["keys"].fuzzy("capp")) == ["1"]
    assert _ids(store, QB["keys"].fuzzy("mol")) == ["1"]


def test_fuzzy_threshold_tightens_the_cutoff(store):
    """A per-call threshold pins the minimum similarity exactly."""
    assert _ids(store, QB["title"].fuzzy("molec", threshold=0.5)) == ["1", "3"]
    assert _ids(store, QB["title"].fuzzy("molec", threshold=0.99)) == []


def _ordered_ids(store, builder) -> list[str]:
    """Iteration order preserved (unlike ``_ids``, which sorts)."""
    return [m.indexed_data["id"] for m in store.iter_search(builder.build())]


def test_similarity_sort_ranks_the_best_match_first(store):
    """The recommended pairing: ``.fuzzy()`` narrows via the GIN, then
    ``.similarity().desc()`` ranks the survivors. "molecular" is an exact word of
    row 1 (1.0) and only partial in row 3 (0.7), so desc = [1, 3], asc = [3, 1]."""
    store.ensure_trigram_index("title")
    desc = (
        QB["title"].fuzzy("molecular").sort(QB["title"].similarity("molecular").desc())
    )
    assert _ordered_ids(store, desc) == ["1", "3"]
    asc = QB["title"].fuzzy("molecular").sort(QB["title"].similarity("molecular").asc())
    assert _ordered_ids(store, asc) == ["3", "1"]


def test_fuzzy_combined_with_a_filter_in_a_group(store):
    """Regression: ``.fuzzy()`` AND/OR-combined with another filter nests inside a
    DataSearchGroup, so the group SQL builder must dispatch it to word_similarity —
    not the scalar path, which reads ``.operator`` (absent on a fuzzy condition)."""
    store.ensure_trigram_index("title")
    # AND: title contains "biology" AND fuzzy-matches "molecular" -> only row 1
    # ("molecular biology"), NOT row 3 ("small molecule") which fuzzy alone matches.
    q_and = QB["title"].contains("biology") & QB["title"].fuzzy("molecular")
    assert _ids(store, q_and) == ["1"]
    # OR: title contains "polymer" OR fuzzy-matches "molecular" -> rows 1, 2, 3
    q_or = QB["title"].contains("polymer") | QB["title"].fuzzy("molecular")
    assert _ids(store, q_or) == ["1", "2", "3"]


# --- Reference-backend parity: the Python port must match live Postgres --------


# (query, target) spanning fragments, typos, the ``->>`` serialised-array text,
# and non-ASCII — the port is what lets memory / disk / sqlite serve .fuzzy().
_PARITY_PAIRS = [
    ("mol", "molecular biology"),
    ("mole", "small molecule"),
    ("molec", "molecular"),
    ("capor", "capping protein"),
    ("mu", "must-have"),
    ("xyz", "molecular"),
    ("quick", "the quick brown fox"),
    ("cap", '["capping", "protein"]'),  # the text ``->>`` exposes for a list
    ("phys", '["quantum", "physics"]'),
    ("分子", "分子生物學"),
    ("中文", "這是中文測試"),
]


@pytest.mark.parametrize(("query", "target"), _PARITY_PAIRS)
def test_reference_matches_live_postgres(store, query, target):
    """The standing guard against pg_trgm drift: :mod:`specstar.util.trigram` must
    equal live Postgres ``word_similarity`` / ``similarity`` for the same inputs.
    This is *why* the reference backends can return the same rows as production."""
    from specstar.util import trigram

    with store.transaction() as cur:
        cur.execute(
            "SELECT word_similarity(%s, %s), similarity(%s, %s)",
            [query, target, query, target],
        )
        pg_ws, pg_sim = cur.fetchone()
    assert trigram.word_similarity(query, target) == pytest.approx(
        float(pg_ws), abs=1e-6
    )
    assert trigram.similarity(query, target) == pytest.approx(float(pg_sim), abs=1e-6)


@pytest.mark.parametrize("store_type", ["memory", "disk", "sql3-mem"])
def test_fuzzy_answers_match_postgres_on_every_backend(store, store_type, tmp_path):
    """End-to-end: ``.fuzzy()`` / ``.similarity()`` over the reference backends
    return the SAME ids (and ranking) as the live-Postgres ``store`` fixture."""
    ref = _seed(get_meta_store(store_type, tmp_path))
    for cond in [
        QB["title"].fuzzy("molecular"),
        QB["title"].fuzzy("polymer"),
        QB["title"].fuzzy("zzznope"),
        QB["keys"].fuzzy("capp"),
        QB["title"].fuzzy("molec", threshold=0.5),
        QB["title"].fuzzy("molec", threshold=0.99),
    ]:
        assert _ids(ref, cond) == _ids(store, cond)

    # ranking parity too (distinct similarities → deterministic order everywhere)
    desc = (
        QB["title"].fuzzy("molecular").sort(QB["title"].similarity("molecular").desc())
    )
    assert _ordered_ids(ref, desc) == _ordered_ids(store, desc) == ["1", "3"]
