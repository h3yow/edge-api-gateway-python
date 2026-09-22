"""边缘网关启动入口（兼容原骨架的 main:app）。

直接运行需先启动模拟上游：

    uv run python -m sim.local_run

若只想启动网关本体（上游自行准备）：

    uv run uvicorn main:app --host 127.0.0.1 --port 8080
"""

from gateway.app import app  # noqa: F401

__all__ = ["app"]
