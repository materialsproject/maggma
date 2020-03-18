from random import randint
from pydantic import BaseModel, Field
from maggma.stores import MemoryStore
from maggma.api.resource import Resource
from maggma.api.cluster_manager import ClusterManager


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

owner_endpoint = Resource(owner_store, Owner)  # type:ignore
manager = ClusterManager({"owners": owner_endpoint})
manager.run()
