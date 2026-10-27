"""``base_index_name``: per table, distinct, and within Postgres's 63-byte limit.

Postgres silently TRUNCATES an identifier longer than 63 bytes, so two long table
names sharing their first 63 bytes would collide again — the very bug the
per-table names fix. Pure function; no database (named to stay out of the
integration glob, so CI runs it).
"""

from specstar.resource_manager.meta_store.postgres import base_index_name


def test_a_short_name_is_the_table_and_the_index():
    assert base_index_name("jasmine_app_meta", "indexed_data_gin") == (
        "idx_jasmine_app_meta_indexed_data_gin"
    )


def test_a_long_name_stays_within_63_bytes_and_keeps_its_suffix():
    name = base_index_name("t" * 80, "rev_created_time")
    assert len(name.encode()) <= 63
    assert name.startswith("idx_") and name.endswith("_rev_created_time")


def test_long_names_that_share_63_bytes_do_not_collide():
    a, b = "x" * 70 + "_a", "x" * 70 + "_b"
    assert base_index_name(a, "is_deleted") != base_index_name(b, "is_deleted")


def test_the_same_table_always_gets_the_same_name():
    # Pods must agree, and the pre-build statements must match the boot.
    assert base_index_name("y" * 90, "is_deleted") == base_index_name(
        "y" * 90, "is_deleted"
    )


def test_a_multibyte_table_name_is_measured_in_bytes():
    name = base_index_name("表" * 30, "indexed_data_gin")
    assert len(name.encode()) <= 63
