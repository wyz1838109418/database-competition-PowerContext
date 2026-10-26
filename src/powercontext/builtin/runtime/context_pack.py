# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass

PACK_SCHEMA = "powercontext.context-pack.v1"
DELTA_SCHEMA = "powercontext.context-pack-delta.v1"


class ContextPackError(ValueError):
    """An invalid context-pack value or wire message."""


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContextPackError("duplicate JSON key")
        result[key] = value
    return result


def _decode(data: bytes, schema: str, fields: set[str]) -> dict:
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_object)
    except (ValueError, UnicodeError, AttributeError) as error:
        raise ContextPackError("invalid UTF-8 JSON") from error
    if not isinstance(value, dict) or set(value) != fields or value["schema"] != schema:
        raise ContextPackError("invalid wire schema")
    return value


def _items(items: tuple[bytes, ...]) -> None:
    if not isinstance(items, tuple) or any(not isinstance(item, bytes) for item in items):
        raise ContextPackError("chunks must be a tuple of bytes")
    try:
        for item in items:
            item.decode("utf-8")
    except UnicodeError as error:
        raise ContextPackError("chunks must contain valid UTF-8") from error


def _encoded(items: tuple[bytes, ...]) -> list[str]:
    return [base64.b64encode(item).decode("ascii") for item in items]


def _decoded(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContextPackError("wire chunks must be Base64 strings")
    try:
        result = tuple(base64.b64decode(item, validate=True) for item in value)
    except (ValueError, binascii.Error) as error:
        raise ContextPackError("invalid Base64") from error
    if _encoded(result) != value:
        raise ContextPackError("noncanonical Base64")
    _items(result)
    return result


def _digest(value: object) -> None:
    if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ContextPackError("invalid digest")


@dataclass(frozen=True, slots=True)
class ContextPack:
    items: tuple[bytes, ...]

    def __post_init__(self):
        _items(self.items)

    @classmethod
    def build(cls, items: tuple[bytes, ...]) -> ContextPack:
        return cls(items)

    @classmethod
    def from_content(cls, content: str | None, *, chunk_bytes: int = 512) -> ContextPack:
        if type(chunk_bytes) is not int or chunk_bytes <= 0:
            raise ContextPackError("chunk_bytes must be a positive integer")
        if content is None:
            content = ""
        if not isinstance(content, str):
            raise ContextPackError("content must be text")
        chunks = []
        current = bytearray()
        try:
            for char in content:
                encoded = char.encode("utf-8")
                if len(encoded) > chunk_bytes:
                    raise ContextPackError("a code point exceeds the chunk limit")
                if len(current) + len(encoded) > chunk_bytes:
                    chunks.append(bytes(current))
                    current.clear()
                current.extend(encoded)
        except UnicodeError as error:
            raise ContextPackError("invalid Unicode content") from error
        if current:
            chunks.append(bytes(current))
        return cls.build(tuple(chunks))

    @property
    def digest(self) -> str:
        unsigned = {"schema": PACK_SCHEMA, "items": _encoded(self.items)}
        return "sha256:" + hashlib.sha256(_json(unsigned)).hexdigest()

    def to_bytes(self) -> bytes:
        payload = {"schema": PACK_SCHEMA, "items": _encoded(self.items), "digest": self.digest, "byte_count": 0}
        # The decimal byte_count includes its own digits. Iterate to the fixed point.
        while True:
            data = _json(payload)
            if len(data) == payload["byte_count"]:
                return data
            payload["byte_count"] = len(data)

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPack:
        value = _decode(data, PACK_SCHEMA, {"schema", "items", "digest", "byte_count"})
        pack = cls.build(_decoded(value["items"]))
        if (
            value["digest"] != pack.digest
            or type(value["byte_count"]) is not int
            or value["byte_count"] != pack.byte_count
        ):
            raise ContextPackError("pack checksum or byte count mismatch")
        return pack


@dataclass(frozen=True, slots=True)
class ContextPackDelta:
    base_digest: str
    target_digest: str
    prefix_items: int
    appended: tuple[bytes, ...]

    def __post_init__(self):
        _digest(self.base_digest)
        _digest(self.target_digest)
        if type(self.prefix_items) is not int or self.prefix_items < 0:
            raise ContextPackError("invalid prefix count")
        _items(self.appended)

    @classmethod
    def between(cls, base: ContextPack, target: ContextPack) -> ContextPackDelta:
        prefix = 0
        for left, right in zip(base.items, target.items, strict=False):
            if left != right:
                break
            prefix += 1
        return cls(base.digest, target.digest, prefix, target.items[prefix:])

    def to_bytes(self) -> bytes:
        return _json({
            "schema": DELTA_SCHEMA,
            "base_digest": self.base_digest,
            "target_digest": self.target_digest,
            "prefix_items": self.prefix_items,
            "appended": _encoded(self.appended),
        })

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPackDelta:
        value = _decode(data, DELTA_SCHEMA, {"schema", "base_digest", "target_digest", "prefix_items", "appended"})
        return cls(value["base_digest"], value["target_digest"], value["prefix_items"], _decoded(value["appended"]))


def apply_context_pack_delta(base: ContextPack, delta: ContextPackDelta) -> ContextPack:
    if base.digest != delta.base_digest or delta.prefix_items > len(base.items):
        raise ContextPackError("base digest or prefix bounds mismatch")
    target = ContextPack.build(base.items[: delta.prefix_items] + delta.appended)
    if target.digest != delta.target_digest:
        raise ContextPackError("target digest mismatch")
    return target
