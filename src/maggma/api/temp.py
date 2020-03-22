from random import randint
from pydantic import BaseModel, Field
from maggma.stores import MemoryStore
from maggma.api.resource import Resource
from fastapi import FastAPI
import uvicorn
from maggma.api.APIManager import APIManager


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: float = Field(None, title="Owner's weight")


owners = (
    [
        Owner(name=f"Person{i}", age=randint(10, 100), weight=float(randint(100, 200)))
        for i in list(range(10)[1:])
    ]
    + [Owner(name="PersonAge9", age=9, weight=float(randint(100, 200)))]
    + [Owner(name="PersonWeight150", age=randint(10, 15), weight=float(150))]
)

owner_store = MemoryStore("owners", key="name")
owner_store.connect()
owners = [d.dict() for d in owners]  # type:ignore
owner_store.update(owners)  # type:ignore

endpoint = Resource(owner_store, Owner, alias={"weight": "mass"})  # type:ignore
app = FastAPI()
app.include_router(endpoint.router)
uvicorn.run(app)

# owner_endpoint = Resource(owner_store, Owner)  # type:ignore
# manager = APIManager({"owners": owner_endpoint})
# manager.run()
