"""Shared exception-to-HTTP mapping for route templates.

Provides a single :func:`to_http_exception` helper that maps internal
domain exceptions to consistent :class:`~fastapi.HTTPException` responses,
plus :func:`install_structured_error_handlers` which (when enabled via
``SpecStar(structured_errors=True)``) registers handlers that emit a
uniform ``{message, code, ...}`` error envelope across HTTPException and
FastAPI's ``RequestValidationError``.

Every route handler should use the same pattern::

    except Exception as e:
        raise to_http_exception(e)

Mapping table:

==================================  ====  ============================
Exception                           HTTP  Structured ``code``
==================================  ====  ============================
``HTTPException``                   (re-raised as-is)  —
``msgspec.ValidationError``         422   ``VALIDATION_ERROR``
``specstar.types.ValidationError``  422   ``VALIDATION_ERROR``
``PermissionDeniedError``           403   ``PERMISSION_DENIED``
``ResourceIsDeletedError``          410   ``RESOURCE_GONE``
``ResourceNotFoundError`` family    404   ``RESOURCE_NOT_FOUND``
``FileNotFoundError``               404   ``RESOURCE_NOT_FOUND``
``UniqueConstraintError``           409   ``UNIQUE_CONSTRAINT``
``ResourceConflictError`` family    409   ``RESOURCE_CONFLICT``
``NotImplementedError``             501   ``NOT_IMPLEMENTED``
fallback                            400   ``BAD_REQUEST``
==================================  ====  ============================
"""

from typing import Any

import msgspec
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from specstar.types import (
    DumpIncompleteError,
    PermissionDeniedError,
    PreconditionFailedError,
    ResourceConflictError,
    ResourceDecodeError,
    ResourceIsDeletedError,
    ResourceNotFoundError,
    UnindexedQueryError,
    UniqueConstraintError,
    ValidationError,
)


def to_http_exception(e: Exception) -> HTTPException:
    """Convert a domain exception to an appropriate :class:`HTTPException`.

    This centralises the error→HTTP-status mapping so that every route
    template returns consistent status codes for the same logical error.

    If *e* is already an :class:`HTTPException` it is returned unchanged
    so that inline ``raise HTTPException(...)`` inside a ``try`` block
    passes through transparently.

    Args:
        e: The caught exception.

    Returns:
        An :class:`HTTPException` with the mapped status code and the
        original exception message as ``detail``.

    Examples:
        >>> from specstar.types import ResourceIDNotFoundError
        >>> exc = to_http_exception(ResourceIDNotFoundError("abc"))
        >>> exc.status_code
        404

        >>> exc = to_http_exception(ValueError("bad input"))
        >>> exc.status_code
        400
    """
    # Already an HTTPException — pass through unchanged
    if isinstance(e, HTTPException):
        return e

    # Validation errors (both msgspec type-level and specstar custom)
    if isinstance(e, (msgspec.ValidationError, ValidationError)):
        return HTTPException(status_code=422, detail=str(e))

    # Permission denied
    if isinstance(e, PermissionDeniedError):
        return HTTPException(status_code=403, detail=str(e))

    # Optimistic-concurrency precondition (If-Match / expected_revision)
    if isinstance(e, PreconditionFailedError):
        return HTTPException(
            status_code=412,
            detail={
                "message": str(e),
                "code": "PRECONDITION_FAILED",
                "expected_revision_id": e.expected_revision_id,
                "actual_revision_id": e.actual_revision_id,
            },
        )

    # Soft-deleted: 410 Gone, distinct from "never existed" 404.
    # Must be checked before ResourceNotFoundError since it is a subclass.
    if isinstance(e, ResourceIsDeletedError):
        return HTTPException(status_code=410, detail=str(e))

    # Resource / revision not found
    if isinstance(e, ResourceNotFoundError):
        return HTTPException(status_code=404, detail=str(e))

    # Stored data can't be decoded into the current model: the stored entity is
    # unprocessable into the current schema. Map to 422 (same status the raw
    # msgspec.ValidationError produced before this was a typed error).
    if isinstance(e, ResourceDecodeError):
        return HTTPException(status_code=422, detail=str(e))

    # Query filters on a field that isn't indexed (and isn't a ResourceMeta
    # attribute): the request references something that can't be filtered → 400.
    if isinstance(e, UnindexedQueryError):
        return HTTPException(status_code=400, detail=str(e))

    # File not found (e.g. blob storage)
    if isinstance(e, FileNotFoundError):
        return HTTPException(status_code=404, detail=str(e) or "Not found")

    # Unique constraint violations get structured detail
    if isinstance(e, UniqueConstraintError):
        return HTTPException(
            status_code=409,
            detail={
                "message": str(e),
                "code": "UNIQUE_CONSTRAINT",
                "field": e.field,
                "conflicting_resource_id": e.conflicting_resource_id,
            },
        )

    # Other conflict errors (DuplicateResourceError, SchemaConflictError, etc.)
    if isinstance(e, ResourceConflictError):
        return HTTPException(status_code=409, detail=str(e))

    # A strict dump found a blob / revision it could not read. Nothing is
    # wrong with the request — the stored data is short — so this is a 500,
    # not the 400 the fallback would have given it.
    if isinstance(e, DumpIncompleteError):
        return HTTPException(status_code=500, detail=str(e))

    # Not implemented (e.g. blob store not configured)
    if isinstance(e, NotImplementedError):
        return HTTPException(status_code=501, detail=str(e) or "Not implemented")

    # Fallback — generic bad request
    return HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# Structured error envelope (opt-in)
# ---------------------------------------------------------------------------


_STATUS_TO_CODE: dict[int, str] = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "PERMISSION_DENIED",
    404: "RESOURCE_NOT_FOUND",
    409: "RESOURCE_CONFLICT",
    410: "RESOURCE_GONE",
    412: "PRECONDITION_FAILED",
    422: "VALIDATION_ERROR",
    501: "NOT_IMPLEMENTED",
}


def _normalize_detail(status_code: int, detail: Any) -> dict[str, Any]:
    """Coerce any HTTPException ``detail`` shape into a uniform envelope.

    Output: ``{"message": str, "code": str, **extras}``.

    * Plain strings → ``{"message": detail, "code": <by status>}``.
    * Dicts that already carry ``message`` → keep, ensure ``code``.
    * Other (lists / arbitrary objects) → preserve under ``extra`` and use
      a generic message; this keeps the envelope predictable even when
      the underlying detail is unusual.
    """
    code = _STATUS_TO_CODE.get(status_code, "ERROR")
    if isinstance(detail, dict):
        out = {"code": code, **detail}
        out.setdefault("message", str(detail))
        # In case caller already supplied a code, prefer theirs.
        if "code" in detail:
            out["code"] = detail["code"]
        return out
    if isinstance(detail, str):
        return {"message": detail, "code": code}
    return {"message": "error", "code": code, "extra": detail}


def _structured_http_exception_handler(_request: Request, exc: HTTPException):
    payload = _normalize_detail(exc.status_code, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": payload},
        headers=exc.headers,
    )


def _structured_validation_error_handler(
    _request: Request, exc: RequestValidationError
):
    # FastAPI's RequestValidationError carries a list of pydantic-style
    # errors. Promote them under ``errors`` so clients still get the per-
    # field info, but expose a uniform top-level shape.
    return JSONResponse(
        status_code=422,
        content={
            "detail": {
                "message": "Request validation failed",
                "code": "VALIDATION_ERROR",
                "errors": exc.errors(),
            }
        },
    )


def install_structured_error_handlers(app: FastAPI) -> None:
    """Register the structured error handlers on *app*.

    Idempotent: calling twice replaces the previous handlers. Intended to
    be called from ``SpecStar.apply()`` only when
    ``structured_errors=True``.
    """
    app.add_exception_handler(HTTPException, _structured_http_exception_handler)
    app.add_exception_handler(
        RequestValidationError, _structured_validation_error_handler
    )
