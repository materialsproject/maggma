from fastapi import FastAPI
from endpoint_cluster import EndpointCluster
import uvicorn
from monty.json import MSONable


class ClusterManager(MSONable):
    def __init__(self):
        self.endpoints = dict()
        self.app = FastAPI()

    def addEndpoint(self, endpoint: EndpointCluster):
        """
        add a endpoint to the cluster manager
        Args:
            endpoint: the new endpoint to add in

        Returns:
            None
        """
        assert endpoint.prefix not in self.endpoints, "ERR: endpoint [{}] already exist, please modify the endpoint " \
                                                      "in-place".format(endpoint.prefix)

        self.endpoints[endpoint.prefix] = endpoint

    def runAllEndpoints(self):
        """
        Must of AT LEAST one endpoint in the list
        initialize and run all endpoints with their respective parameters

        Returns:
            None
        """
        assert len(self.endpoints) > 0, "ERROR: There are no endpoints provided"
        # print(self.endpoints)
        for prefix, endpoint in self.endpoints.items():
            self.app.include_router(
                endpoint.router,
                prefix=prefix
            )
        uvicorn.run(self.app, host="127.0.0.1", port=8000, log_level="info", reload=False)

    def getEndpoints(self):
        """

        Returns:
            a list of existing endpoints
        """
        return self.endpoints.values()

    def getEndpoint(self, key:str):
        """

        Args:
            key: the given key

        Returns:
            return the endpoint if the key is in self.endpoints, otherwise, raise error
        """
        return self.endpoints[key]
