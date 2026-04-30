"""Neo4j constraints and indexes for temporal graph RAG."""

CONSTRAINTS_AND_INDEXES = [
    "CREATE CONSTRAINT collection_id_unique IF NOT EXISTS FOR (c:Collection) REQUIRE c.collection_id IS UNIQUE",
    "CREATE INDEX document_collection IF NOT EXISTS FOR (d:Document) ON (d.collection_id)",
    # Document identity: grouping key doc_id + publish_date (ISO date string)
    """CREATE CONSTRAINT document_doc_publish_unique IF NOT EXISTS
       FOR (d:Document) REQUIRE (d.doc_id, d.publish_date) IS UNIQUE""",
    "CREATE CONSTRAINT chunk_id_unique IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE",
    "CREATE CONSTRAINT statement_event_id IF NOT EXISTS FOR (e:StatementEvent) REQUIRE e.id IS UNIQUE",
    "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
    "CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)",
]


async def ensure_schema(session: object) -> None:
    for cypher in CONSTRAINTS_AND_INDEXES:
        await session.run(cypher)
