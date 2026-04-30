from __future__ import annotations

import logging
from neo4j import AsyncDriver, AsyncGraphDatabase

from temporal_graph.settings import Settings, get_settings

logger = logging.getLogger(__name__)

_driver: AsyncDriver | None = None


def get_driver(settings: Settings | None = None) -> AsyncDriver:
    global _driver
    if _driver is None:
        s = settings or get_settings()
        _driver = AsyncGraphDatabase.driver(
            s.neo4j_uri,
            auth=(s.neo4j_user, s.neo4j_password),
        )
    return _driver


async def close_driver() -> None:
    global _driver
    if _driver is not None:
        await _driver.close()
        _driver = None


async def verify_connectivity(driver: AsyncDriver) -> None:
    await driver.verify_connectivity()
