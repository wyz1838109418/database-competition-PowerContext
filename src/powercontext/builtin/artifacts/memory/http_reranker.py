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

import math

import httpx

from powercontext.builtin.artifacts.memory.models import MemoryHit
from powercontext.builtin.artifacts.memory.reranking import MemoryRerankDecision
from powercontext.builtin.inference import InferenceUsage


class HTTPMemoryReranker:
    """Map strictly validated HTTP scores back to the original global ranks."""

    def __init__(
        self,
        endpoint: str,
        *,
        client: httpx.AsyncClient,
        batch_size: int = 32,
        policy_id: str = "http-cross-encoder-v1",
    ) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.endpoint, self.client, self.batch_size, self.policy_id = endpoint, client, batch_size, policy_id

    async def rerank(self, query: str, candidates: tuple[MemoryHit, ...], limit: int, /) -> MemoryRerankDecision:
        if not candidates or type(limit) is not int or not 1 <= limit <= len(candidates):
            raise ValueError("invalid candidate count or limit")
        scores = {}
        requests = 0
        for start in range(0, len(candidates), self.batch_size):
            batch = [
                {"rank": i + 1, "text": candidate.text}
                for i, candidate in enumerate(candidates[start : start + self.batch_size], start)
            ]
            response = await self.client.post(
                self.endpoint, json={"query": query, "candidates": batch}, follow_redirects=False
            )
            requests += 1
            response.raise_for_status()
            try:
                payload = response.json()
            except (ValueError, UnicodeError) as error:
                raise ValueError("invalid reranking JSON") from error
            if not isinstance(payload, dict) or set(payload) != {"scores"} or not isinstance(payload["scores"], list):
                raise ValueError("invalid reranking response schema")
            expected = {item["rank"] for item in batch}
            observed = set()
            for item in payload["scores"]:
                if not isinstance(item, dict) or set(item) != {"rank", "score"}:
                    raise ValueError("invalid reranking score schema")
                rank, score = item["rank"], item["score"]
                if type(rank) is not int or rank not in expected or rank in observed:
                    raise ValueError("invalid or repeated reranking rank")
                if type(score) not in (int, float):
                    raise ValueError("reranking scores must be finite numbers")
                try:
                    finite = math.isfinite(score)
                except OverflowError:
                    finite = False
                if not finite:
                    raise ValueError("reranking scores must be finite numbers")
                observed.add(rank)
                scores[rank] = score
            if observed != expected:
                raise ValueError("reranking response omitted ranks")
        selected = tuple(sorted(scores, key=lambda rank: (-scores[rank], rank))[:limit])
        return MemoryRerankDecision(selected_ranks=selected, usage=InferenceUsage(requests=requests))
