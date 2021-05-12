from datetime import datetime
from typing import Dict

import uvicorn
from fastapi import FastAPI
from monty.json import MSONable
from starlette.responses import RedirectResponse

from maggma import __version__
from maggma.api.resource import Resource


class API(MSONable):
    """
    Basic API manager to tie together various resources
    """

    def __init__(
        self,
        resources: Dict[str, Resource],
        title="Generic API",
        version="v0.0.0",
        debug=False,
    ):
        """
        Args:
            resources: dictionary of resource objects and http prefix they live in
            title: a string title for this API
            version: the version for this API
            debug: turns debug on in FastAPI
        """
        self.title = title
        self.version = version
        self.debug = debug

        if len(resources) == 0:
            raise RuntimeError("ERROR: There are no endpoints provided")

        self.resources = resources

    def on_startup(self):
        """
        Basic startup that runs the resource startup functions
        """
        for resource in self.resources.values():
            resource.on_startup()

    @property
    def app(self):
        """
        App server for the cluster manager
        """
        app = FastAPI(
            title=self.title,
            version=self.version,
            on_startup=[self.on_startup],
            debug=self.debug,
        )
        for prefix, resource in self.resources.items():
            app.include_router(resource.router, prefix=f"/{prefix}")

        @app.get("/heartbeat", include_in_schema=False)
        def heartbeat():
            """ API Heartbeat for Load Balancing """

            return {"status": "OK", "time": datetime.utcnow()}

        @app.get("/", include_in_schema=False)
        def redirect_docs():
            """ Redirects the root end point to the docs """
            return RedirectResponse(url=app.docs_url, status_code=301)

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
