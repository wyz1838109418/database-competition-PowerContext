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

import csv
import io
import json
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, model_validator

from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence


class CSVDocumentInput(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    document_id: str
    primary_key: str
    content: bytes


def csv_row_source_id(document_id: str, primary_key_value: str) -> str:
    if not document_id or document_id != document_id.strip():
        raise ValueError("document_id must be nonempty and trimmed")
    if not primary_key_value.strip():
        raise ValueError("primary key value must not be blank")
    identity = f"csv:{quote(document_id, safe='')}:{quote(primary_key_value, safe='')}"
    if len(identity) > 256:
        raise ValueError("CSV row identity exceeds 256 characters")
    return identity


class CSVRowInput(BaseModel):
    model_config = ConfigDict(frozen=True)
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_row(self):
        csv_row_source_id(self.document_id, self.primary_key_value)
        if not self.columns or any(not name.strip() for name in self.columns):
            raise ValueError("CSV columns must not be blank")
        if len(set(self.columns)) != len(self.columns) or len(self.columns) != len(self.values):
            raise ValueError("CSV columns must be unique and match the row width")
        if not self.primary_key or self.primary_key not in self.columns:
            raise ValueError("primary key must exactly match a column")
        if self.values[self.columns.index(self.primary_key)] != self.primary_key_value:
            raise ValueError("primary key value does not match the row")
        return self


class CSVRowSource(Source, CSVRowInput):
    source_id: str


def parse_csv_document(value: CSVDocumentInput) -> tuple[CSVRowInput, ...]:
    if not value.document_id or value.document_id != value.document_id.strip() or not value.primary_key:
        raise ValueError("invalid document identity or primary key")
    try:
        reader = csv.reader(io.StringIO(value.content.decode("utf-8-sig"), newline=""), strict=True)
        columns = tuple(next(reader))
        if not columns or any(not name.strip() for name in columns) or len(set(columns)) != len(columns):
            raise ValueError("invalid CSV header")
        if value.primary_key not in columns:
            raise ValueError("primary key must exactly match a header")
        position = columns.index(value.primary_key)
        rows = []
        seen = set()
        for fields in reader:
            if not fields:
                continue
            if len(fields) != len(columns):
                raise ValueError("CSV row width does not match header")
            key = fields[position]
            if key in seen:
                raise ValueError("duplicate CSV primary key")
            seen.add(key)
            rows.append(
                CSVRowInput(
                    document_id=value.document_id,
                    primary_key=value.primary_key,
                    primary_key_value=key,
                    columns=columns,
                    values=tuple(fields),
                )
            )
        return tuple(rows)
    except (UnicodeError, csv.Error, StopIteration) as error:
        raise ValueError("invalid UTF-8 CSV document") from error


class CSVRowAdapter:
    input_class = CSVRowInput
    source_class = CSVRowSource
    name = "csv-row"

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        identity = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=identity, source_id=identity, materialization=SourceMaterialization.CAPTURED, **value.model_dump()
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput.model_validate(source.model_dump(include=set(CSVRowInput.model_fields)))


class CSVRowProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        row = dict(zip(source.columns, source.values, strict=True))
        return TextEvidence(
            source_type="csv-row",
            source_id=source.source_id,
            content=json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": row,
            },
        )


CSV_ROW_SOURCE_DEFINITION = AdapterSourceDefinition(CSVRowAdapter(), projections=(CSVRowProjection(),))
