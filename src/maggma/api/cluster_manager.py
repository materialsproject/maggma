import uvicorn
from fastapi import FastAPI
from typing import Dict
from monty.json import MSONable
from maggma.api.endpoint_cluster import EndpointCluster


class ClusterManager(MSONable):
    def __init__(self, endpoints: Dict[str, EndpointCluster]):
        self.endpoints = endpoints

    def run(self, ip: str = "127.0.0.1", port: int = 8000, log_level: str = "info"):
        """
        Runs the Cluster Manager locally

        Args:
            ip: Local IP to listen on
            port: Local port to listen on
            log_level: Logging level for the webserver

        Returns:
            None
        """
        app = FastAPI()
        assert len(self.endpoints) > 0, "ERROR: There are no endpoints provided"

        for prefix, endpoint in self.endpoints.items():
            app.include_router(endpoint.router, prefix=prefix)
        uvicorn.run(app, host=ip, port=port, log_level=log_level, reload=False)

    def __setitem__(self, key, item):
        self.endpoints[key] = item

    def __getitem__(self, key):
        return self.endpoints[key]

    def __len__(self):
        return len(self.endpoints)

    def keys(self):
        return self.endpoints.keys()

    def __contains__(self, item):
        return item in self.endpoints
