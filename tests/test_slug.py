from temporal_graph.utils.slug import slugify_collection_id


def test_slugify_basic() -> None:
    assert slugify_collection_id("Pricing Research") == "pricing_research"


def test_slugify_fallback() -> None:
    assert slugify_collection_id("!!!") == "collection"
