from fastapi import FastAPI, APIRouter, Path, HTTPException
from monty.json import MSONable
import pydantic

# CITATION: https://gist.github.com/bl4de/3086cf26081110383631
# Table mapping response codes to messages; entries have the
default_responses = {
    100: {"description": 'Continue'},
    101: {"description": 'Switching Protocols'},

    200: {"description": 'OK'},
    201: {"description": 'Created'},
    202: {"description": 'Accepted'},
    203: {"description": 'Non-Authoritative Information'},
    204: {"description": 'No Content'},
    205: {"description": 'Reset Content'},
    206: {"description": 'Partial Content'},

    300: {"description": 'Multiple Choices'},
    301: {"description": 'Moved Permanently'},
    302: {"description": 'Found'},
    303: {"description": 'See Other'},
    304: {"description": 'Not Modified'},
    305: {"description": 'Use Proxy'},
    307: {"description": 'Temporary Redirect'},

    400: {"description": 'Bad Request'},
    401: {"description": 'Unauthorized'},
    402: {"description": 'Payment Required'},
    403: {"description": 'Forbidden'},
    404: {"description": 'Not Found'},
    405: {"description": 'Method Not Allowed'},
    406: {"description": 'Not Acceptable'},
    407: {"description": 'Proxy Authentication Required'},
    408: {"description": 'Request Timeout'},
    409: {"description": 'Conflict'},
    410: {"description": 'Gone'},
    411: {"description": 'Length Required'},
    412: {"description": 'Precondition Failed'},
    413: {"description": 'Request Entity Too Large'},
    414: {"description": 'Request-URI Too Long'},
    415: {"description": 'Unsupported Media Type'},
    416: {"description": 'Requested Range Not Satisfiable'},
    417: {"description": 'Expectation Failed'},

    500: {"description": 'Internal Server Error'},
    501: {"description": 'Not Implemented'},
    502: {"description": 'Bad Gateway'},
    503: {"description": 'Service Unavailable'},
    504: {"description": 'Gateway Timeout'},
    505: {"description": 'HTTP Version Not Supported'}
}


class EndpointCluster(MSONable):
    def __init__(self, db_source, model: pydantic.BaseModel, prefix: str = "", tags: list = [], responses: dict = {}):
        self.db_source = db_source
        self.router = APIRouter()
        # self.app = app # no need, include router in manager
        self.prefix = prefix
        self.model = model
        self.tags = tags
        self.responses = default_responses.copy()

        self.responses.update(responses)

        self.router.post("/simple_post")(self.simple_post)
        self.router.get("/",
                        tags=self.tags,
                        responses=self.responses)(self.root)
        self.router.get("/task_id/{task_id}",
                        response_description="Get Task ID",
                        response_model=self.model,
                        tags=self.tags,
                        responses=self.responses
                        ) \
            (self.get_on_task_id)
        # self.prepareEndpoint()

    async def root(self):
        """
        Default root response

        Returns:
            Default response
        """
        return {"response": "At root level"}

    async def get_on_task_id(self, task_id: str = Path(..., title="The task_id of the item to get")):
        """
        http://127.0.0.1:8000/task_id/mp-30933
        Args:
            task_id: in the format of mp-NUMBER

        Returns:
            a single material that satisfy the Material Model
        """
        cursor = self.db_source.query(criteria={"task_id": task_id})
        material = cursor[0] if cursor.count() > 0 else None
        if material:
            material = self.model(**material)
            return material
        else:
            raise HTTPException(status_code=404, detail="Item with Task_id = {} not found".format(task_id))

    async def simple_post(self, data: str):
        # https://www.errietta.me/blog/python-fastapi-intro/
        # https://fastapi.tiangolo.com/tutorial/request-forms/
        return {"response": "posting " + data}

    @property
    def app(self):
        app = FastAPI()
        app.include_router(
            self.router,
            prefix="")
        # NOTE: per documentation of uvicorn, unable to attach reload=True attribute
        # https://www.uvicorn.org/deployment/#running-programmatically
        return app
