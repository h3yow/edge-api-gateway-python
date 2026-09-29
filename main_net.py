"""Local network-lab entry point:

    uv run uvicorn main_net:app --port 8000
"""

from edge_gateway.lab.netapp import create_network_app

app = create_network_app()
