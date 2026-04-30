from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from temporal_graph.api.collection_routes import router as collection_router
from temporal_graph.api.ingest_routes import router as ingest_router
from temporal_graph.api.retrieve_routes import router as retrieve_router
from temporal_graph.jobs.manager import JobManager, ingest_worker_loop
from temporal_graph.neo4j.bootstrap import bootstrap_graph
from temporal_graph.neo4j.driver import close_driver, get_driver
from temporal_graph.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.job_manager = JobManager(settings)
    app.state.worker_stop = asyncio.Event()
    app.state.worker_task = None
    if (
        (settings.job_backend or "").strip().lower() == "redis"
        and settings.ingest_start_redis_worker
        and settings.redis_url
    ):
        app.state.worker_task = asyncio.create_task(ingest_worker_loop(app, app.state.worker_stop))
        logger.info("Started Redis ingest worker (BRPOP %s)", settings.redis_job_queue_key)
    driver = get_driver(settings)
    try:
        await bootstrap_graph(driver, settings)
        logger.info("Neo4j schema ensured for %s", settings.neo4j_uri)
    except Exception as e:
        logger.warning("Neo4j bootstrap failed (API will start; ingestion may fail): %s", e)
    yield
    app.state.worker_stop.set()
    if app.state.worker_task:
        app.state.worker_task.cancel()
        try:
            await app.state.worker_task
        except asyncio.CancelledError:
            pass
    await app.state.job_manager.aclose()
    await close_driver()


app = FastAPI(
    title="Temporal Graph RAG",
    description="Neo4j-backed temporal graph ingestion and retrieval (doc-processing LLM proxy).",
    lifespan=lifespan,
)

app.include_router(ingest_router)
app.include_router(retrieve_router)
app.include_router(collection_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
