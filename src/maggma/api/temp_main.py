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


#
o_s = owner_store(owners())
#
# endpoint_main = EndpointCluster(o_s, Owner, description="main")
# endpoint_main_temp = EndpointCluster(o_s, Owner, description="main_temp")
# endpoint_temp = EndpointCluster(o_s, Owner, description="temp")
# endpoint_main_a_temp = EndpointCluster(o_s, Owner, description="main_a_temp")
# # endpoint.run()
# manager = ClusterManager(
#     {
#         "/temp": endpoint_temp,
#         "/main": endpoint_main,
#         "/main/temp": endpoint_main_temp,
#         "/main/a/temp": endpoint_main_a_temp,
#     }
# )
#
# manager.pprint()

owner_model = Owner(name="PersonAge12", age=12, weight=randint(100, 200))
endpoint_model_extractor = EndpointCluster(o_s, Owner)
endpoint_model_extractor.run()

# from fastapi import Query
#
# params = dict()
# for name, model_field in owner_model.__fields__.items():
#     if model_field.type_ == str:
#         params[name] = {f"{model_field.name}_eq": Query(...),
#                         f"{model_field.name}_not_eq": Query(...)}
#     elif model_field.type_ == int:
#         params[name] = {f"{model_field.name}_lt": Query(...),
#                         f"{model_field.name}_gt": Query(...),
#                         f"{model_field.name}_eq": Query(...),
#                         f"{model_field.name}_not_eq": Query(...)}
#     elif model_field.type_ == float:
#         params[name] = {f"{model_field.name}_lt": Query(...),
#                         f"{model_field.name}_gt": Query(...),
#                         f"{model_field.name}_eq": Query(...),
#                         f"{model_field.name}_not_eq": Query(...)}
#     else:
#         print(model_field.type_)
#
#
# def dynamic_call(x):
#     pass
#
# import inspect
# param = inspect.Parameter("name", kind= inspect.Parameter.POSITIONAL_OR_KEYWORD, default="name")
# print("new_parap ->", param)
