from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from openai import AsyncOpenAI
from pydantic import BaseModel

from temporal_graph.doc_processing.client import DocProcessingClient
from temporal_graph.doc_processing.types import CompletionRequest, EmbeddingRequest
from temporal_graph.settings import Settings

logger = logging.getLogger(__name__)


def load_llm_config(path: Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


class LLMRouter:
    """Routes logical roles to doc-processing HTTP API or direct OpenAI."""

    def __init__(
        self,
        settings: Settings | None = None,
        doc_client: DocProcessingClient | None = None,
    ) -> None:
        self._settings = settings or Settings()
        self._cfg = load_llm_config(self._settings.llm_config_path)
        self._roles: dict[str, Any] = self._cfg.get("roles", {})
        self._doc = doc_client or DocProcessingClient(self._settings)
        self._openai: AsyncOpenAI | None = None
        if self._settings.openai_api_key:
            self._openai = AsyncOpenAI(api_key=self._settings.openai_api_key)

    async def aclose(self) -> None:
        await self._doc.aclose()

    def _role_cfg(self, role: str) -> dict[str, Any]:
        if role not in self._roles:
            raise KeyError(f"Unknown LLM role '{role}'. Define it in llm_config.yml")
        return self._roles[role]

    async def complete_json_schema(
        self,
        role: str,
        system_prompt: str,
        user_prompt: str,
        response_model: type[BaseModel],
        schema_name: str | None = None,
    ) -> BaseModel:
        """Structured completion: doc-processing response_format json_schema or OpenAI parse."""
        rc = self._role_cfg(role)
        backend = rc.get("backend", "doc_processing")
        schema = response_model.model_json_schema()
        name = schema_name or response_model.__name__
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": name, "schema": schema, "strict": False},
        }
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        if backend == "doc_processing":
            req = CompletionRequest(
                provider=rc["provider"],
                messages=messages,
                model=rc.get("model"),
                reasoning_effort=rc.get("reasoning_effort"),
                response_format=response_format,
            )
            resp = await self._doc.complete(req)
            if resp.parsed is not None:
                return response_model.model_validate(resp.parsed)
            return response_model.model_validate_json(resp.content)

        if backend == "openai_direct":
            if not self._openai:
                raise RuntimeError("openai_direct backend requires OPENAI_API_KEY")
            model = rc.get("model") or "gpt-4.1-mini"
            combined = f"{system_prompt}\n\n---\n\n{user_prompt}"
            parsed = await self._openai.responses.parse(
                model=model,
                temperature=0,
                input=combined,
                text_format=response_model,
            )
            return parsed.output_parsed  # type: ignore[no-any-return]

        raise ValueError(f"Unsupported backend: {backend}")

    async def complete_text(self, role: str, system_prompt: str, user_prompt: str) -> str:
        rc = self._role_cfg(role)
        backend = rc.get("backend", "doc_processing")
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        if backend == "doc_processing":
            req = CompletionRequest(
                provider=rc["provider"],
                messages=messages,
                model=rc.get("model"),
                reasoning_effort=rc.get("reasoning_effort"),
            )
            resp = await self._doc.complete(req)
            return resp.content
        if backend == "openai_direct":
            if not self._openai:
                raise RuntimeError("openai_direct backend requires OPENAI_API_KEY")
            model = rc.get("model") or "gpt-4.1-mini"
            r = await self._openai.chat.completions.create(
                model=model,
                temperature=0,
                messages=messages,
            )
            return r.choices[0].message.content or ""
        raise ValueError(f"Unsupported backend: {backend}")

    async def embed(self, role: str, text: str) -> list[float]:
        rc = self._role_cfg(role)
        backend = rc.get("backend", "doc_processing")
        model = rc.get("model", "text-embedding-3-large")
        dimensions = rc.get("embedding_dimensions")

        if backend == "doc_processing":
            req = EmbeddingRequest(
                provider=rc.get("provider", "openai"),
                input=text,
                model=model,
                dimensions=dimensions,
            )
            emb = await self._doc.embeddings(req)
            vec = emb.data[0].embedding
            if isinstance(vec, str):
                raise TypeError("Expected float embedding list from doc_processing")
            return vec

        if backend == "openai_direct":
            if not self._openai:
                raise RuntimeError("openai_direct backend requires OPENAI_API_KEY")
            kwargs: dict[str, Any] = {"model": model, "input": text}
            if dimensions:
                kwargs["dimensions"] = dimensions
            r = await self._openai.embeddings.create(**kwargs)
            return list(r.data[0].embedding)

        raise ValueError(f"Unsupported backend: {backend}")
