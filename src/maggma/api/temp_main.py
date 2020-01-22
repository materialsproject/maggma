from random import randint
from pydantic import BaseModel, Field
from maggma.stores import MemoryStore
from endpoint_cluster import EndpointCluster
from cluster_manager import ClusterManager


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(None, title="Owne'r Age")
    weight: int = Field(None, title="Owner's weight")


def owners():
    return (
        [
            Owner(name=f"Person{i}", age=randint(10, 100), weight=randint(100, 200))
            for i in list(range(10)[1:])
        ]
        + [Owner(name="PersonAge12", age=12, weight=randint(100, 200))]
        + [Owner(name="PersonWeight150", age=randint(10, 15), weight=150)]
    )


def owner_store(owners):
    store = MemoryStore("owners", key="name")
    store.connect()
    owners = [d.dict() for d in owners]
    store.update(owners)
    return store


o_s = owner_store(owners())

endpoint_main = EndpointCluster(o_s, Owner, description="main")
endpoint_main_temp = EndpointCluster(o_s, Owner, description="main_temp")
endpoint_temp = EndpointCluster(o_s, Owner, description="temp")
# endpoint.run()
manager = ClusterManager(
    {"/temp": endpoint_temp, "/main": endpoint_main, "/main/temp": endpoint_main_temp}
)

manager.pprint()
