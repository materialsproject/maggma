import uvicorn
from fastapi import FastAPI
from typing import Dict
from monty.json import MSONable
from maggma.api.endpoint_cluster import EndpointCluster
from inspect import isclass
from maggma.utils import dynamic_import


class ClusterManager(MSONable):
    def __init__(self, endpoints: Dict[str, EndpointCluster]):
        self.endpoints = endpoints

    @property
    def app(self):
        """
        App server for the cluster manager
        """
        app = FastAPI()
        if len(self) == 0:
            raise RuntimeError("ERROR: There are no endpoints provided")

        for prefix, endpoint in self.endpoints.items():
            app.include_router(endpoint.router, prefix=f"/{prefix}")
        return app

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

        uvicorn.run(self.app, host=ip, port=port, log_level=log_level, reload=False)

    def load(self, endpoint, prefix: str = "/"):
        """
        loads an endpoint dynamically. The endpoint can be either a path to an EndpointCluster instance,
        or a EndpointCluster instance Args: endpoint:

        Returns:
            None

        Raises:
            ValueError -- if the endpoint is not a path to an EndpointCluster or it is not an EndpointCluster
        """
        if isinstance((endpoint, str)):
            module_path = ".".join(endpoint.split(".")[:-1])
            class_name = endpoint.split(".")[-1]
            new_endpoint = dynamic_import(module_path, class_name)
            self.__setitem__(prefix, new_endpoint)
            pass
        elif isclass(endpoint) and issubclass(endpoint, EndpointCluster):
            self.__setitem__(prefix, endpoint)
        else:
            raise ValueError(
                "endpont has to be a EndpointCluster instance or a path to EndpointCluster instance"
            )

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
