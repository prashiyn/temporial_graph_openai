from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Literal

import httpx
import redis.asyncio as aioredis

from temporal_graph.doc_processing.client import DocProcessingClient
from temporal_graph.llm.router import LLMRouter
from temporal_graph.models.api import IngestJobStatus, IngestPayload, JobState
from temporal_graph.neo4j.driver import get_driver
from temporal_graph.neo4j.repository import GraphRepository
from temporal_graph.ontology.loader import load_ontology
from temporal_graph.pipeline.extraction import TemporalIngestionPipeline
from temporal_graph.predicates import load_predicates
from temporal_graph.settings import Settings, get_settings
from temporal_graph.wiring.collection_ns import strip_wire_from_json

logger = logging.getLogger(__name__)

JobBackend = Literal["memory", "redis"]


@dataclass
class JobRecord:
    job_id: str
    state: JobState = JobState.pending
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error: str | None = None
    result_summary: dict[str, Any] | None = None
    payload: IngestPayload | None = None
    webhook_url: str | None = None
    events: asyncio.Queue[dict[str, Any]] = field(default_factory=asyncio.Queue)


class JobManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._jobs: dict[str, JobRecord] = {}
        self._lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(max(1, self._settings.ingest_max_concurrent_jobs))
        self._tasks: set[asyncio.Task[Any]] = set()
        self._redis: Any = None
        if self.backend == "redis":
            if not (self._settings.redis_url or "").strip():
                raise ValueError("JOB_BACKEND=redis requires REDIS_URL")
            self._redis = aioredis.from_url(
                self._settings.redis_url,
                decode_responses=True,
            )

    @property
    def backend(self) -> JobBackend:
        v = (self._settings.job_backend or "memory").strip().lower()
        return "redis" if v == "redis" else "memory"

    def _job_hash_key(self, job_id: str) -> str:
        return f"{self._settings.redis_job_key_prefix}:{job_id}"

    def _job_events_key(self, job_id: str) -> str:
        return f"{self._settings.redis_job_key_prefix}:{job_id}:events"

    async def create_job(self, payload: IngestPayload) -> JobRecord:
        jid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        if self._redis:
            await self._redis.hset(
                self._job_hash_key(jid),
                mapping={
                    "state": JobState.pending.value,
                    "created_at": now,
                    "updated_at": now,
                    "error": "",
                    "result_summary": "",
                    "payload": payload.model_dump_json(),
                    "webhook_url": payload.webhook_url or "",
                },
            )
            await self._redis.rpush(self._settings.redis_job_queue_key, jid)
            return JobRecord(job_id=jid, payload=payload, webhook_url=payload.webhook_url)
        rec = JobRecord(
            job_id=jid,
            payload=payload,
            webhook_url=payload.webhook_url,
        )
        async with self._lock:
            self._jobs[jid] = rec
        return rec

    async def get(self, job_id: str) -> JobRecord | None:
        if self._redis:
            d = await self._redis.hgetall(self._job_hash_key(job_id))
            if not d:
                return None
            return _redis_to_record(job_id, d)
        async with self._lock:
            return self._jobs.get(job_id)

    def status_model(self, rec: JobRecord) -> IngestJobStatus:
        return IngestJobStatus(
            job_id=rec.job_id,
            state=rec.state,
            created_at=rec.created_at,
            updated_at=rec.updated_at,
            error=rec.error,
            result_summary=rec.result_summary,
        )

    async def _notify(self, rec: JobRecord, typ: str, data: dict[str, Any]) -> None:
        msg = {"type": typ, "data": data, "ts": datetime.now(timezone.utc).isoformat()}
        if self._redis:
            await self._redis.rpush(self._job_events_key(rec.job_id), json.dumps(msg, default=str))
            return
        await rec.events.put(msg)

    async def _save_redis_state(self, job_id: str, **fields: Any) -> None:
        if not self._redis:
            return
        m: dict[str, str] = {}
        if "state" in fields and fields["state"] is not None:
            m["state"] = (
                fields["state"].value if isinstance(fields["state"], JobState) else str(fields["state"])
            )
        if "error" in fields:
            m["error"] = fields["error"] or ""
        if "result_summary" in fields and fields["result_summary"] is not None:
            m["result_summary"] = json.dumps(fields["result_summary"], default=str)
        m["updated_at"] = datetime.now(timezone.utc).isoformat()
        if m:
            await self._redis.hset(self._job_hash_key(job_id), mapping=m)

    async def _emit_pipeline(self, rec: JobRecord):
        async def emit(typ: str, data: dict[str, Any]) -> None:
            await self._notify(rec, typ, data)

        return emit

    async def _run_ingest(self, rec: JobRecord) -> None:
        assert rec.payload is not None
        async with self._sem:
            rec.state = JobState.running
            rec.updated_at = datetime.now(timezone.utc)
            if self._redis:
                await self._save_redis_state(rec.job_id, state=rec.state)
            await self._notify(rec, "state", {"state": rec.state.value})
            doc_client = DocProcessingClient(self._settings)
            router = LLMRouter(self._settings, doc_client)
            try:
                driver = get_driver(self._settings)
                repo = GraphRepository(driver, self._settings)
                ontology = load_ontology(self._settings.ontologies_dir, rec.payload.ontology_id)
                predicates = load_predicates(self._settings.predicates_path)
                emit = await self._emit_pipeline(rec)
                pipe = TemporalIngestionPipeline(router, repo, predicates, self._settings, emit=emit)
                summary = await pipe.ingest(rec.payload, ontology)
                rec.result_summary = summary
                rec.state = JobState.completed
            except Exception as e:
                logger.exception("ingest job %s failed", rec.job_id)
                rec.error = str(e)
                rec.state = JobState.failed
            finally:
                await router.aclose()
                await doc_client.aclose()
                rec.updated_at = datetime.now(timezone.utc)
                if self._redis:
                    await self._save_redis_state(
                        rec.job_id,
                        state=rec.state,
                        error=rec.error,
                        result_summary=rec.result_summary,
                    )
                await self._notify(
                    rec, "state", {"state": rec.state.value, "error": rec.error}
                )
                if rec.webhook_url:
                    await self._post_webhook(rec)

    async def run_job_by_id(self, job_id: str) -> None:
        """Worker entry: load job from Redis and execute."""
        if not self._redis:
            return
        d = await self._redis.hgetall(self._job_hash_key(job_id))
        if not d:
            logger.warning("missing redis job %s", job_id)
            return
        rec = _redis_to_record(job_id, d)
        if rec.payload is None:
            logger.warning("bad payload for job %s", job_id)
            return
        await self._run_ingest(rec)

    async def _post_webhook(self, rec: JobRecord) -> None:
        if not rec.webhook_url:
            return
        body = strip_wire_from_json(self.status_model(rec).model_dump(mode="json"))
        raw = json.dumps(body, default=str).encode()
        headers = {"Content-Type": "application/json"}
        secret = self._settings.job_webhook_signing_secret or ""
        if secret:
            sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
            headers["X-Temporal-Graph-Signature"] = f"sha256={sig}"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                r = await client.post(rec.webhook_url, content=raw, headers=headers)
                r.raise_for_status()
        except Exception as e:
            logger.warning("webhook post failed for job %s: %s", rec.job_id, e)

    def spawn(self, rec: JobRecord) -> None:
        if self._redis:
            return
        task = asyncio.create_task(self._run_ingest(rec))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def iter_sse_events(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        if self._redis:
            if not await self._redis.exists(self._job_hash_key(job_id)):
                return
            last = 0
            terminal = {JobState.completed.value, JobState.failed.value}
            while True:
                items = await self._redis.lrange(self._job_events_key(job_id), last, -1)
                for raw in items:
                    last += 1
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                rec = await self.get(job_id)
                if rec and rec.state.value in terminal:
                    break
                await asyncio.sleep(0.25)
            return

        rec = await self.get(job_id)
        if not rec:
            return
        terminal = {JobState.completed.value, JobState.failed.value}
        while True:
            msg = await rec.events.get()
            yield msg
            if msg.get("type") == "state" and msg.get("data", {}).get("state") in terminal:
                break

    async def aclose(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _redis_to_record(job_id: str, d: dict[str, str]) -> JobRecord:
    try:
        st = JobState(d.get("state", "pending"))
    except ValueError:
        st = JobState.pending
    now = datetime.now(timezone.utc).isoformat()
    created_at = _parse_iso(d.get("created_at", now))
    updated_at = _parse_iso(d.get("updated_at", d.get("created_at", now)))
    err = d.get("error") or None
    rs_raw = d.get("result_summary") or ""
    rs = json.loads(rs_raw) if rs_raw else None
    payload_raw = d.get("payload") or "{}"
    payload = IngestPayload.model_validate_json(payload_raw)
    wh = d.get("webhook_url") or None
    return JobRecord(
        job_id=job_id,
        state=st,
        created_at=created_at,
        updated_at=updated_at,
        error=err,
        result_summary=rs,
        payload=payload,
        webhook_url=wh,
    )


async def ingest_worker_loop(app: Any, stop: asyncio.Event) -> None:
    settings: Settings = app.state.settings
    mgr: JobManager = app.state.job_manager
    if not mgr._redis:
        return
    r = mgr._redis
    key = settings.redis_job_queue_key
    logger.info("ingest redis worker listening on %s", key)
    while not stop.is_set():
        try:
            item = await r.brpop(key, timeout=5)
        except asyncio.CancelledError:
            break
        if not item:
            continue
        _, jid = item
        await mgr.run_job_by_id(jid)
    logger.info("ingest redis worker stopped")
