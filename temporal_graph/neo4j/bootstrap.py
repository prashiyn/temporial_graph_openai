from __future__ import annotations

from neo4j import AsyncDriver

from temporal_graph.neo4j.schema import ensure_schema
from temporal_graph.settings import Settings


async def bootstrap_graph(driver: AsyncDriver, settings: Settings) -> None:
    async with driver.session(database=settings.neo4j_database) as session:
        await ensure_schema(session)
