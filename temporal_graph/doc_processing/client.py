from __future__ import annotations

import logging
from typing import Any

import httpx
from tenacity import retry, retry_any, retry_if_exception, retry_if_exception_type, stop_after_attempt, wait_exponential

from temporal_graph.doc_processing.types import (
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ModelsResponse,
)
from temporal_graph.settings import Settings

logger = logging.getLogger(__name__)


class DocProcessingClient:
    """HTTP client for doc-processing service OpenAPI (LLM + embeddings)."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._base = self._settings.llm_service_base_url.rstrip("/")
        timeout = httpx.Timeout(
            connect=self._settings.doc_processing_connect_timeout_seconds,
            read=self._settings.doc_processing_timeout_seconds,
            write=self._settings.doc_processing_timeout_seconds,
            pool=self._settings.doc_processing_connect_timeout_seconds,
        )
        self._client = httpx.AsyncClient(base_url=self._base, timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _retry(self) -> retry:
        def _5xx(exc: BaseException) -> bool:
            return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500

        return retry(
            stop=stop_after_attempt(self._settings.doc_processing_max_retries),
            wait=wait_exponential(
                multiplier=self._settings.doc_processing_retry_backoff_seconds,
                min=self._settings.doc_processing_retry_backoff_seconds,
                max=30,
            ),
            retry=retry_any(
                retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, httpx.ReadError)),
                retry_if_exception(_5xx),
            ),
            reraise=True,
        )

    async def complete(self, body: CompletionRequest) -> CompletionResponse:
        @self._retry()
        async def _post() -> CompletionResponse:
            r = await self._client.post("/llm/complete", json=body.model_dump(exclude_none=True))
            r.raise_for_status()
            return CompletionResponse.model_validate(r.json())

        try:
            return await _post()
        except httpx.HTTPStatusError as e:
            logger.warning("doc_processing /llm/complete failed: %s %s", e.response.status_code, e.response.text)
            raise

    async def embeddings(self, body: EmbeddingRequest) -> EmbeddingResponse:
        @self._retry()
        async def _post() -> EmbeddingResponse:
            r = await self._client.post("/llm/embeddings", json=body.model_dump(exclude_none=True))
            r.raise_for_status()
            return EmbeddingResponse.model_validate(r.json())

        return await _post()

    async def models(self) -> ModelsResponse:
        r = await self._client.get("/llm/models")
        r.raise_for_status()
        return ModelsResponse.model_validate(r.json())

    async def health(self) -> dict[str, Any]:
        r = await self._client.get("/health")
        r.raise_for_status()
        return r.json()
