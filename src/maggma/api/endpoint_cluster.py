from fastapi import FastAPI, APIRouter, Path, HTTPException
from monty.json import MSONable
import pydantic


class EndpointCluster(MSONable):
    def __init__(
        self,
        db_source,
        model: pydantic.BaseModel,
        prefix: str = "",
        tags: list = [],
        responses: dict = {},
    ):
        self.db_source = db_source
        self.router = APIRouter()
        # self.app = app # no need, include router in manager
        self.prefix = prefix
        self.model = model
        self.tags = tags
        self.responses = default_responses.copy()

        self.responses.update(responses)

        self.router.post("/simple_post")(self.simple_post)
        self.router.get("/", tags=self.tags, responses=self.responses)(self.root)
        self.router.get(
            "/task_id/{task_id}",
            response_description="Get Task ID",
            response_model=self.model,
            tags=self.tags,
            responses=self.responses,
        )(self.get_on_task_id)
        # self.prepareEndpoint()

    async def root(self):
        """
        Default root response

        Returns:
            Default response
        """
        return {"response": "At root level"}

    async def get_on_task_id(
        self, task_id: str = Path(..., title="The task_id of the item to get")
    ):
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
            raise HTTPException(
                status_code=404,
                detail="Item with Task_id = {} not found".format(task_id),
            )

    async def simple_post(self, data: str):
        # https://www.errietta.me/blog/python-fastapi-intro/
        # https://fastapi.tiangolo.com/tutorial/request-forms/
        return {"response": "posting " + data}

    @property
    def app(self):
        app = FastAPI()
        app.include_router(self.router, prefix="")
        # NOTE: per documentation of uvicorn, unable to attach reload=True attribute
        # https://www.uvicorn.org/deployment/#running-programmatically
        return app
