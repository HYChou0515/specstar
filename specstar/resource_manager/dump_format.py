"""Streaming msgpack dump/load format for SpecStar backup.

Each record is framed as ``[4-byte big-endian length][msgpack payload]``.
The payload is a tagged ``msgspec.Struct`` so the reader can decode each
record polymorphically via a single ``Union`` type.

Format overview (in order)::

    HeaderRecord          – format version
    ModelStartRecord      – begin a model section
      MetaRecord*         – zero or more resource metadata entries
      RevisionRecord*     – zero or more revision data entries
      BlobRecord*         – zero or more binary blob entries
    ModelEndRecord        – end of model section
    ...                   – repeat for additional models
    EofRecord             – end of stream
"""

from __future__ import annotations

import struct
from typing import IO, Union

import msgspec
from msgspec import Struct

# ---------------------------------------------------------------------------
# Record types (tagged union via ``tag_field="t"``)
# ---------------------------------------------------------------------------


class HeaderRecord(Struct, tag=True, tag_field="t"):
    """First record — carries format version."""

    version: int = 2


class ModelStartRecord(Struct, tag=True, tag_field="t"):
    """Marks the beginning of a model section."""

    model_name: str


class MetaRecord(Struct, tag=True, tag_field="t"):
    """Serialised ``ResourceMeta`` bytes."""

    data: bytes


class RevisionRecord(Struct, tag=True, tag_field="t"):
    """Serialised ``RawResource`` bytes (RevisionInfo + raw data)."""

    data: bytes


class BlobRecord(Struct, tag=True, tag_field="t"):
    """Complete binary blob entry."""

    file_id: str
    blob_data: bytes
    size: int
    content_type: str


class ModelEndRecord(Struct, tag=True, tag_field="t"):
    """Marks the end of a model section."""

    model_name: str


class EofRecord(Struct, tag=True, tag_field="t"):
    """Last record — signals end of stream."""

    pass


class DumpStats:
    """What one model's dump actually managed to read (issue #450).

    A dump used to be a bare generator: it swallowed an unreadable blob
    and an undecodable revision alike, and finished normally either way,
    so a caller could not tell a complete archive from one missing its
    attachments. With ``strict=False`` the failures land here instead of
    being lost.

    ``complete`` is the one question a backup script should ask.
    """

    __slots__ = (
        "metas",
        "revisions",
        "blobs",
        "skipped_blobs",
        "unreadable_resources",
        "undecodable_revisions",
    )

    def __init__(self) -> None:
        self.metas = 0
        self.revisions = 0
        self.blobs = 0
        self.skipped_blobs: list[str] = []
        self.unreadable_resources: list[str] = []
        self.undecodable_revisions: list[str] = []

    @property
    def complete(self) -> bool:
        """True when no bytes the archive should hold are known missing.

        This is the question a backup script should ask. It answers
        ``skipped_blobs`` only: an entry there is an attachment the store
        would not give up, which is data lost from the archive.

        It deliberately ignores ``undecodable_revisions``. A revision
        stored at an older schema version is a supported state — reads
        migrate lazily and ``migrate()`` is optional — and such a payload
        is written to the archive verbatim either way. Only the blob ids
        it *might* reference could not be read back, so it is a gap in
        verification, not in content. Folding it in here would flip
        ``complete`` to False for the ordinary un-migrated store and teach
        operators to ignore the flag.
        """
        return not self.skipped_blobs and not self.unreadable_resources

    @property
    def fully_verified(self) -> bool:
        """True when ``complete`` holds *and* every revision decoded.

        The stricter question: not only is nothing known to be missing,
        every revision's blob references were actually checked. Ask this
        when the model carries attachments and you need certainty rather
        than the absence of known loss.
        """
        return self.complete and not self.undecodable_revisions

    def __repr__(self) -> str:
        return (
            f"DumpStats(metas={self.metas}, revisions={self.revisions}, "
            f"blobs={self.blobs}, skipped_blobs={self.skipped_blobs!r}, "
            f"unreadable_resources={self.unreadable_resources!r}, "
            f"undecodable_revisions={self.undecodable_revisions!r})"
        )


# The discriminated union used for decoding.
DumpRecord = Union[
    HeaderRecord,
    ModelStartRecord,
    MetaRecord,
    RevisionRecord,
    BlobRecord,
    ModelEndRecord,
    EofRecord,
]

# ---------------------------------------------------------------------------
# Encoder / Decoder singletons
# ---------------------------------------------------------------------------

_encoder = msgspec.msgpack.Encoder(order="deterministic")
_decoder = msgspec.msgpack.Decoder(DumpRecord)

# Frame header: 4-byte unsigned big-endian length.
_FRAME_FMT = ">I"
_FRAME_SIZE = struct.calcsize(_FRAME_FMT)


# ---------------------------------------------------------------------------
# Writer / Reader
# ---------------------------------------------------------------------------


def encode_frame(record: DumpRecord) -> bytes:
    """Encode one record as its length-prefixed frame.

    Exposed separately from :class:`DumpStreamWriter` so an archive can be
    produced as an iterator of byte chunks — which is what lets an HTTP
    export stream instead of being assembled in a buffer first.
    """
    payload = _encoder.encode(record)
    return struct.pack(_FRAME_FMT, len(payload)) + payload


class DumpStreamWriter:
    """Write ``DumpRecord`` objects to a binary stream with length-prefix framing."""

    __slots__ = ("_bio",)

    def __init__(self, bio: IO[bytes]) -> None:
        self._bio = bio

    def write(self, record: DumpRecord) -> None:
        self._bio.write(encode_frame(record))


class DumpStreamReader:
    """Iterate ``DumpRecord`` objects from a binary stream."""

    __slots__ = ("_bio",)

    def __init__(self, bio: IO[bytes]) -> None:
        self._bio = bio

    def __iter__(self):
        return self

    def __next__(self) -> DumpRecord:
        header = self._bio.read(_FRAME_SIZE)
        if not header:
            raise StopIteration
        if len(header) < _FRAME_SIZE:
            raise ValueError("Truncated frame header in dump stream.")
        (length,) = struct.unpack(_FRAME_FMT, header)
        payload = self._bio.read(length)
        if len(payload) < length:
            raise ValueError("Truncated frame payload in dump stream.")
        return _decoder.decode(payload)
