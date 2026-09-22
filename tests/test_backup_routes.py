"""Tests for backup / restore HTTP endpoints.

Covers:
* Per-model export (``GET /{model}/export``)
* Per-model import (``POST /{model}/import``)
* Global export   (``GET /_backup/export``)
* Global import   (``POST /_backup/import``)
"""

from __future__ import annotations

import datetime as dt
import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from msgspec import Struct

from specstar.crud.core import SpecStar
from specstar.resource_manager.dump_format import (
    DumpStreamReader,
    DumpStreamWriter,
    EofRecord,
    HeaderRecord,
    MetaRecord,
    ModelStartRecord,
)

# ── models ────────────────────────────────────────────────────────────


class Item(Struct):
    name: str
    price: int


class Tag(Struct):
    label: str


# ── helpers ───────────────────────────────────────────────────────────


def _make_app(*models, **kw) -> tuple[SpecStar, TestClient]:
    """Build an SpecStar + FastAPI app with *models* registered and
    return ``(spec, client)``."""
    spec = SpecStar(
        default_user="tester",
        default_now=dt.datetime.now,
        **kw,
    )
    names = {}
    for m in models:
        name = m.__name__.lower()
        spec.add_model(m, name=name)
        names[m] = name
    app = FastAPI()
    spec.apply(app)
    return spec, TestClient(app)


def _seed(spec: SpecStar, model_name: str, items: list) -> list[str]:
    """Create several resources, return their resource_ids."""
    rm = spec.resource_managers[model_name]
    ids = []
    for item in items:
        meta = rm.create(item)
        ids.append(meta.resource_id)
    return ids


# ======================================================================
# Per-model export
# ======================================================================


class TestPerModelExport:
    """``GET /{model}/export``"""

    def test_export_empty_model(self):
        spec, client = _make_app(Item)
        resp = client.get("/item/export")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/octet-stream"

        # Parse archive — should contain header + model start/end + eof
        reader = DumpStreamReader(io.BytesIO(resp.content))
        records = list(reader)
        types = [type(r).__name__ for r in records]
        assert types == [
            "HeaderRecord",
            "ModelStartRecord",
            "ModelEndRecord",
            "EofRecord",
        ]

    def test_export_with_data(self):
        spec, client = _make_app(Item)
        _seed(spec, "item", [Item(name="A", price=10), Item(name="B", price=20)])

        resp = client.get("/item/export")
        assert resp.status_code == 200

        reader = DumpStreamReader(io.BytesIO(resp.content))
        records = list(reader)
        meta_count = sum(1 for r in records if isinstance(r, MetaRecord))
        assert meta_count == 2

    def test_export_filename(self):
        spec, client = _make_app(Item)
        resp = client.get("/item/export")
        assert "item.acbak" in resp.headers.get("content-disposition", "")

    def test_export_with_qb_filter(self):
        spec, client = _make_app(
            Item,
        )
        spec.add_model(Item, name="item2", indexed_fields=[("price", int)])
        app2 = FastAPI()
        # Rebuild with indexed fields
        crud2 = SpecStar(default_user="t", default_now=dt.datetime.now)
        crud2.add_model(Item, name="item", indexed_fields=[("price", int)])
        app2 = FastAPI()
        crud2.apply(app2)
        client2 = TestClient(app2)

        _seed(crud2, "item", [Item(name="A", price=10), Item(name="B", price=99)])

        # Filter by price == 10 using QB
        resp = client2.get("/item/export", params={"qb": "QB['price'] == 10"})
        assert resp.status_code == 200

        reader = DumpStreamReader(io.BytesIO(resp.content))
        records = list(reader)
        meta_count = sum(1 for r in records if isinstance(r, MetaRecord))
        assert meta_count == 1


# ======================================================================
# Per-model import
# ======================================================================


class TestPerModelImport:
    """``POST /{model}/import``"""

    @pytest.fixture
    def crud_client(self):
        return _make_app(Item)

    def _make_archive(self, spec: SpecStar, model_name: str) -> bytes:
        """Export model to bytes via Python API."""
        buf = io.BytesIO()
        spec.dump(buf, model_queries={model_name: None})
        return buf.getvalue()

    # --- happy path ---

    def test_import_new_data(self, crud_client):
        spec, client = crud_client
        _seed(spec, "item", [Item(name="X", price=5)])
        archive = self._make_archive(spec, "item")

        # Create a fresh instance to import into
        crud2, client2 = _make_app(Item)
        resp = client2.post(
            "/item/import",
            files={"file": ("backup.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["loaded"] == 1
        assert data["skipped"] == 0
        assert data["total"] == 1

    def test_import_overwrite(self, crud_client):
        spec, client = crud_client
        ids = _seed(spec, "item", [Item(name="Original", price=1)])
        archive = self._make_archive(spec, "item")

        # Modify the resource
        rm = spec.resource_managers["item"]
        rm.update(ids[0], Item(name="Modified", price=999))

        # Import with overwrite (default)
        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["loaded"] == 1

    def test_import_skip(self, crud_client):
        spec, client = crud_client
        _seed(spec, "item", [Item(name="Existing", price=1)])
        archive = self._make_archive(spec, "item")

        # Import with skip
        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
            params={"on_duplicate": "skip"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["loaded"] == 0
        assert data["skipped"] == 1

    def test_import_raise_error(self, crud_client):
        spec, client = crud_client
        _seed(spec, "item", [Item(name="Existing", price=1)])
        archive = self._make_archive(spec, "item")

        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
            params={"on_duplicate": "raise_error"},
        )
        assert resp.status_code == 409

    # --- validation ---

    def test_import_invalid_on_duplicate(self, crud_client):
        _, client = crud_client
        buf = io.BytesIO()
        w = DumpStreamWriter(buf)
        w.write(HeaderRecord())
        w.write(EofRecord())
        archive = buf.getvalue()

        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
            params={"on_duplicate": "invalid"},
        )
        assert resp.status_code == 400
        assert "Invalid on_duplicate" in resp.json()["detail"]

    def test_import_empty_file(self, crud_client):
        _, client = crud_client
        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", b"", "application/octet-stream")},
        )
        assert resp.status_code == 400

    def test_import_bad_header(self, crud_client):
        _, client = crud_client
        buf = io.BytesIO()
        w = DumpStreamWriter(buf)
        w.write(ModelStartRecord(model_name="item"))
        archive = buf.getvalue()

        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 400
        assert "header" in resp.json()["detail"].lower()

    def test_import_wrong_model_skipped(self, crud_client):
        """Archive with a different model section is ignored."""
        spec, client = crud_client
        # Build archive for model "other" (no such section for "item")
        crud_other = SpecStar(default_user="t", default_now=dt.datetime.now)
        crud_other.add_model(Tag, name="tag")
        rm = crud_other.resource_managers["tag"]
        rm.create(Tag(label="test"))
        buf = io.BytesIO()
        crud_other.dump(buf, model_queries={"tag": None})
        archive = buf.getvalue()

        resp = client.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0  # No "item" section was found


# ======================================================================
# Global export
# ======================================================================


class TestGlobalExport:
    """``GET /_backup/export``"""

    def test_export_all_models(self):
        spec, client = _make_app(Item, Tag)
        _seed(spec, "item", [Item(name="A", price=1)])
        _seed(spec, "tag", [Tag(label="x")])

        resp = client.get("/_backup/export")
        assert resp.status_code == 200

        reader = DumpStreamReader(io.BytesIO(resp.content))
        records = list(reader)
        model_starts = [
            r.model_name for r in records if isinstance(r, ModelStartRecord)
        ]
        assert "item" in model_starts
        assert "tag" in model_starts

    def test_export_selected_models(self):
        spec, client = _make_app(Item, Tag)
        _seed(spec, "item", [Item(name="A", price=1)])
        _seed(spec, "tag", [Tag(label="x")])

        resp = client.get("/_backup/export", params={"models": ["item"]})
        assert resp.status_code == 200

        reader = DumpStreamReader(io.BytesIO(resp.content))
        records = list(reader)
        model_starts = [
            r.model_name for r in records if isinstance(r, ModelStartRecord)
        ]
        assert model_starts == ["item"]

    def test_export_unknown_model(self):
        _, client = _make_app(Item)
        resp = client.get("/_backup/export", params={"models": ["nonexist"]})
        assert resp.status_code == 400
        assert "nonexist" in resp.json()["detail"]

    def test_export_filename(self):
        _, client = _make_app(Item)
        resp = client.get("/_backup/export")
        assert "backup.acbak" in resp.headers.get("content-disposition", "")


# ======================================================================
# Global import
# ======================================================================


class TestGlobalImport:
    """``POST /_backup/import``"""

    def test_import_all_models(self):
        spec, client = _make_app(Item, Tag)
        _seed(spec, "item", [Item(name="A", price=1), Item(name="B", price=2)])
        _seed(spec, "tag", [Tag(label="x")])

        buf = io.BytesIO()
        spec.dump(buf)
        archive = buf.getvalue()

        # Fresh instance
        crud2, client2 = _make_app(Item, Tag)
        resp = client2.post(
            "/_backup/import",
            files={"file": ("backup.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["item"]["loaded"] == 2
        assert data["tag"]["loaded"] == 1

    def test_import_skip_strategy(self):
        spec, client = _make_app(Item)
        ids = _seed(spec, "item", [Item(name="A", price=1)])
        buf = io.BytesIO()
        spec.dump(buf)
        archive = buf.getvalue()

        resp = client.post(
            "/_backup/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
            params={"on_duplicate": "skip"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["item"]["skipped"] == 1

    def test_import_invalid_on_duplicate(self):
        _, client = _make_app(Item)
        buf = io.BytesIO()
        w = DumpStreamWriter(buf)
        w.write(HeaderRecord())
        w.write(EofRecord())

        resp = client.post(
            "/_backup/import",
            files={"file": ("b.acbak", buf.getvalue(), "application/octet-stream")},
            params={"on_duplicate": "bad"},
        )
        assert resp.status_code == 400

    def test_import_bad_archive(self):
        _, client = _make_app(Item)
        resp = client.post(
            "/_backup/import",
            files={"file": ("b.acbak", b"garbage", "application/octet-stream")},
        )
        assert resp.status_code == 400


# ======================================================================
# Roundtrip: export → import
# ======================================================================


class TestRoundtrip:
    """End-to-end: export data via HTTP, import into clean instance."""

    def test_per_model_roundtrip(self):
        """Export items via GET, import into fresh app, verify data."""
        crud1, client1 = _make_app(Item)
        _seed(crud1, "item", [Item(name="Widget", price=42)])

        # Export
        resp = client1.get("/item/export")
        assert resp.status_code == 200
        archive = resp.content

        # Import into fresh instance
        crud2, client2 = _make_app(Item)
        resp = client2.post(
            "/item/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        assert resp.json()["loaded"] == 1

        # Verify data via API
        resp = client2.get("/item")
        assert resp.status_code == 200
        results = resp.json()
        assert len(results) == 1
        assert results[0]["data"]["name"] == "Widget"
        assert results[0]["data"]["price"] == 42

    def test_global_roundtrip(self):
        """Full backup/restore across multiple models."""
        crud1, client1 = _make_app(Item, Tag)
        _seed(crud1, "item", [Item(name="A", price=1)])
        _seed(crud1, "tag", [Tag(label="alpha"), Tag(label="beta")])

        # Global export
        resp = client1.get("/_backup/export")
        assert resp.status_code == 200
        archive = resp.content

        # Import into clean instance
        crud2, client2 = _make_app(Item, Tag)
        resp = client2.post(
            "/_backup/import",
            files={"file": ("b.acbak", archive, "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["item"]["loaded"] == 1
        assert data["tag"]["loaded"] == 2

        # Verify item
        resp = client2.get("/item")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

        # Verify tags
        resp = client2.get("/tag")
        assert resp.status_code == 200
        assert len(resp.json()) == 2


# ======================================================================
# OpenAPI schema generation
# ======================================================================


class TestOpenAPISchemaGeneration:
    """Ensure the backup endpoints don't break OpenAPI schema generation.

    Regression: ``from __future__ import annotations`` in core.py turns
    ``-> StreamingResponse`` into a ForwardRef that Pydantic cannot
    resolve, causing ``PydanticUserError`` when generating the schema.
    """

    def test_openapi_schema_generation_succeeds(self):
        """GET /openapi.json must succeed when backup routes are enabled."""
        spec, client = _make_app(Item)
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        # Verify backup endpoints appear in schema paths
        paths = schema.get("paths", {})
        assert "/_backup/export" in paths
        assert "/_backup/import" in paths


# ======================================================================
# Authorization (issue #450)
# ======================================================================


def _locked_app() -> tuple[SpecStar, TestClient, bytes]:
    """An app whose only allowed user is ``root``, driven as ``anon``.

    Returns the spec, a client that surfaces a 500 instead of raising, and
    a valid archive produced by a permissive twin — so the import tests
    exercise the permission check rather than a parse failure.
    """
    from specstar.permission.simple import RootOnly

    donor = SpecStar(default_user="root", default_now=dt.datetime.now)
    donor.add_model(Item, name="item")
    _seed(donor, "item", [Item(name="secret", price=1)])
    buf = io.BytesIO()
    donor.dump(buf)

    spec = SpecStar(
        default_user="anon",
        default_now=dt.datetime.now,
        permission_checker=RootOnly(root_user="root"),
    )
    spec.add_model(Item, name="item")
    # Seed as root: the point is to deny the *route*, not the fixture.
    with spec.resource_managers["item"].using(user="root", now=dt.datetime.now()):
        spec.resource_managers["item"].create(Item(name="TOP-SECRET-XYZ", price=99))
    app = FastAPI()
    spec.apply(app)
    return spec, TestClient(app, raise_server_exceptions=False), buf.getvalue()


class TestBackupAuthorization:
    """The backup doors answer 403 when the checker denies.

    Permission *is* enforced — ``ResourceManager.dump`` / ``load`` are
    wrapped by ``execute_with_events``, so a denied user never reaches the
    data. But the four routes did not translate the refusal: global
    export/import surfaced it as a 500 and per-model import as a 409,
    which reads as "the library is broken" or "there was a conflict"
    rather than "you may not do this" — and made the whole path look
    unguarded from the outside.
    """

    def test_global_export_is_forbidden_not_a_server_error(self):
        _, client, _ = _locked_app()

        resp = client.get("/_backup/export")

        assert resp.status_code == 403
        assert b"TOP-SECRET-XYZ" not in resp.content

    def test_per_model_export_is_forbidden_not_a_server_error(self):
        _, client, _ = _locked_app()

        resp = client.get("/item/export")

        assert resp.status_code == 403
        assert b"TOP-SECRET-XYZ" not in resp.content

    def test_global_import_is_forbidden_not_a_server_error(self):
        _, client, archive = _locked_app()

        resp = client.post("/_backup/import", files={"file": ("x.acbak", archive)})

        assert resp.status_code == 403

    def test_per_model_import_is_forbidden_not_a_conflict(self):
        _, client, archive = _locked_app()

        resp = client.post("/item/import", files={"file": ("x.acbak", archive)})

        assert resp.status_code == 403
