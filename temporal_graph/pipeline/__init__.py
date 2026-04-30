from __future__ import annotations

from typing import Any

__all__ = ["TemporalIngestionPipeline"]


def __getattr__(name: str) -> Any:
    if name == "TemporalIngestionPipeline":
        from temporal_graph.pipeline.extraction import TemporalIngestionPipeline

        return TemporalIngestionPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
