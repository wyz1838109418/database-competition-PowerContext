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

import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, JsonValue

from powercontext.sources.connectors import ConnectorRunCompletion, ConnectorRunSession, ConnectorRunStatus
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence


class PiSessionInput(BaseModel):
    content: bytes
    leaf_id: str | None = None


class PiEventInput(BaseModel):
    model_config = ConfigDict(frozen=True)
    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]


class PiEventSource(Source, PiEventInput):
    source_id: str


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _identity(session_id: str, event_id: str) -> str:
    return "pi:" + hashlib.sha256(_json([session_id, event_id]).encode("utf-8")).hexdigest()


def _invalid_constant(value: str):
    raise ValueError("non-finite JSON number")


def _archive(value: PiSessionInput):
    try:
        records = [
            json.loads(line, parse_constant=_invalid_constant)
            for line in value.content.decode("utf-8-sig").split("\n")
            if line.strip()
        ]
    except (UnicodeError, ValueError) as error:
        raise ValueError("invalid UTF-8 Pi JSONL") from error
    if not records or not isinstance(records[0], dict):
        raise ValueError("missing Pi session header")
    header = records[0]
    if header.get("type") != "session" or type(header.get("version")) is not int or header["version"] != 3:
        raise ValueError("Pi session version 3 is required")
    session_id = header.get("id")
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("invalid session identity")
    by_id = {}
    for record in records[1:]:
        if not isinstance(record, dict):
            raise ValueError("Pi nodes must be objects")
        event_id = record.get("id")
        if not isinstance(event_id, str) or not event_id.strip() or event_id in by_id:
            raise ValueError("invalid or duplicate Pi node identity")
        if "parentId" not in record:
            raise ValueError("Pi node is missing its parent")
        parent = record["parentId"]
        if parent is not None and (not isinstance(parent, str) or parent not in by_id):
            raise ValueError("Pi parent must reference an earlier node")
        by_id[event_id] = record
    leaf = value.leaf_id if value.leaf_id is not None else next(reversed(by_id), None)
    if leaf is not None and leaf not in by_id:
        raise ValueError("selected Pi leaf does not exist")
    chain = []
    while leaf is not None:
        record = by_id[leaf]
        chain.append(record)
        leaf = record["parentId"]
    return session_id, by_id, tuple(reversed(chain))


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
        elif block.get("type") == "toolCall":
            name, call_id = block.get("name", ""), block.get("id", "")
            parts.append(f"[tool_call {name} {call_id}] {_json(block.get('arguments', {}))}")
    return "\n".join(parts)


def _events(session_id: str, chain: tuple[dict, ...]) -> tuple[PiEventInput, ...]:
    events = []
    for record in chain:
        event_type = record.get("type")
        if event_type == "message":
            message = record.get("message")
            if not isinstance(message, dict):
                raise ValueError("Pi message must be an object")
            role = message.get("role")
            if not isinstance(role, str) or not role:
                raise ValueError("Pi message role is required")
            body = _text(message.get("content"))
            if role == "toolResult" and body:
                body = f"[tool_result {message.get('toolName', '')} {message.get('toolCallId', '')}] {body}"
            elif role == "bashExecution":
                command, output = message.get("command", ""), message.get("output", "")
                if not isinstance(command, str) or not isinstance(output, str):
                    raise ValueError("invalid bash evidence")
                body = f"$ {command}\n{output}" if command or output else ""
        elif event_type in ("compaction", "branch_summary"):
            role, body = event_type, record.get("summary", "")
        elif event_type == "custom_message":
            role, body = "custom", _text(record.get("content"))
        else:
            continue
        if not isinstance(body, str):
            raise ValueError("visible evidence must be text")
        if body.strip():
            events.append(
                PiEventInput(
                    session_id=session_id,
                    event_id=record["id"],
                    parent_id=record["parentId"],
                    event_type=event_type,
                    role=role,
                    content=f"{role}: {body}",
                    record=record,
                )
            )
    return tuple(events)


def parse_pi_session(value: PiSessionInput) -> tuple[PiEventInput, ...]:
    session_id, _, chain = _archive(value)
    return _events(session_id, chain)


class PiEventAdapter:
    input_class = PiEventInput
    source_class = PiEventSource
    name = "pi-event"

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        identity = _identity(value.session_id, value.event_id)
        return PiEventSource(
            name=identity, source_id=identity, materialization=SourceMaterialization.CAPTURED, **value.model_dump()
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return PiEventInput.model_validate(source.model_dump(include=set(PiEventInput.model_fields)))


class PiEventProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PiEventSource
    output_class = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        return TextEvidence(
            source_type="pi-event",
            source_id=source.source_id,
            content=source.content,
            metadata={
                key: getattr(source, key) for key in ("session_id", "event_id", "parent_id", "event_type", "role")
            },
        )


PI_EVENT_SOURCE_DEFINITION = AdapterSourceDefinition(PiEventAdapter(), projections=(PiEventProjection(),))


class PiSessionConnector:
    name = "pi-session-history"
    version = "1"
    source_definitions = frozenset({"pi-event"})

    def __init__(self, path: str | Path, leaf_id: str | None = None):
        self.path, self.leaf_id = Path(path), leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        session_id, records, chain = _archive(PiSessionInput(content=self.path.read_bytes(), leaf_id=self.leaf_id))
        events = _events(session_id, chain)
        previous = session.checkpoint
        seen = {}
        if previous is not None:
            if not isinstance(previous, dict) or previous.get("session_id") != session_id:
                raise ValueError("checkpoint belongs to another Pi session")
            stored = previous.get("records")
            if not isinstance(stored, dict):
                raise ValueError("invalid Pi checkpoint")
            seen = dict(stored)
            # Check every previously imported record, including other branches, before any write.
            for event_id, digest in seen.items():
                if event_id not in records or _record_digest(records[event_id]) != digest:
                    raise ValueError("an imported Pi event was rewritten or removed")
        for event in events:
            if event.event_id not in seen:
                await session.submit(_identity(session_id, event.event_id), "pi-event", event)
                seen[event.event_id] = _record_digest(event.record)
        return ConnectorRunCompletion(
            status=ConnectorRunStatus.COMPLETE, checkpoint={"session_id": session_id, "records": seen}
        )


def _record_digest(record: dict) -> str:
    return hashlib.sha256(_json(record).encode("utf-8")).hexdigest()
