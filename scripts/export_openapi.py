#!/usr/bin/env python3
"""Write repo-root openapi.json from the FastAPI app (run after route changes)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> None:
    from fastapi.openapi.utils import get_openapi

    from temporal_graph.api.main import app

    schema = get_openapi(
        title=app.title,
        version="0.1.0",
        openapi_version=app.openapi_version,
        description=app.description or "",
        routes=app.routes,
    )
    out = _ROOT / "openapi.json"
    out.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
