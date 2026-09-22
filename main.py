"""Local entry point: ``uv run uvicorn main:app``."""

from edge_gateway.app import app

__all__ = ["app"]
