"""Additional contract boundaries independent of the published acceptance cases."""

import json

import httpx
import pytest
from powercontext.builtin.artifacts.memory.http_reranker import HTTPMemoryReranker
from powercontext.builtin.runtime.context_pack import (
    ContextPack,
    ContextPackDelta,
    ContextPackError,
    apply_context_pack_delta,
)
from powercontext.sources.csv_rows import CSVDocumentInput, parse_csv_document
from powercontext.sources.pi_session import PiSessionInput, parse_pi_session


def test_csv_strict_quoting_and_blank_primary_keys():
    for data in [b'id,name\n1,"bad"suffix\n', b"id,name\n   ,blank\n"]:
        with pytest.raises(ValueError):
            parse_csv_document(CSVDocumentInput(document_id="d", primary_key="id", content=data))


def test_pi_json_text_unicode_line_separator_is_not_a_physical_record_boundary():
    rows = [
        {"type": "session", "version": 3, "id": "s"},
        {"type": "message", "id": "a", "parentId": None, "message": {"role": "user", "content": "before\u2028after"}},
    ]
    raw = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows).encode()
    assert parse_pi_session(PiSessionInput(content=raw))[0].content == "user: before\u2028after"


def test_context_wire_rejects_ambiguous_fields_and_non_utf8_chunks():
    pack = ContextPack.build((b"a",))
    with pytest.raises(ContextPackError):
        ContextPack.from_bytes(pack.to_bytes().replace(b"{", b'{"schema":"other",', 1))
    with pytest.raises(ContextPackError):
        ContextPack.build((b"\xff",))
    with pytest.raises(ContextPackError):
        ContextPack.from_content("abc", chunk_bytes=True)


@pytest.mark.parametrize("size", [1, 2, 3, 4, 7, 512])
def test_unicode_chunk_limits_and_serialized_delta_reconstruction(size):
    content = "中文🙂 café\n" * 80
    if size < 4:
        with pytest.raises(ContextPackError):
            ContextPack.from_content(content, chunk_bytes=size)
        return
    pack = ContextPack.from_content(content, chunk_bytes=size)
    assert all(len(item) <= size and item.decode("utf-8") for item in pack.items)
    assert b"".join(pack.items).decode() == content
    target = ContextPack.from_content(content + "尾部", chunk_bytes=size)
    rebuilt = apply_context_pack_delta(
        ContextPack.from_bytes(pack.to_bytes()),
        ContextPackDelta.from_bytes(ContextPackDelta.between(pack, target).to_bytes()),
    )
    assert rebuilt == target


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [{"scores": [{"rank": True, "score": 0.5}]}, {"scores": [{"rank": 1, "score": True}]}]
)
async def test_http_boolean_ranks_and_scores_are_not_numbers(payload):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
        from types import SimpleNamespace

        with pytest.raises(ValueError):
            await HTTPMemoryReranker("https://local.invalid", client=client).rerank(
                "q", (SimpleNamespace(text="x"),), 1
            )
