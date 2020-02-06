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
# import inspect
# from fastapi import Query
#
# class DynamicQueryMetaClass(type):
#     def __new__(cls, model: BaseModel, additional_signature_fields=None):
#         if additional_signature_fields is None:
#             additional_signature_fields = dict()
#         cls.params = dict()
#         # construct fields
#         for name, model_field in model.__fields__.items():
#             if model_field.type_ == str:
#                 cls.params[f"{model_field.name}_eq"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#                 cls.params[f"{model_field.name}_not_eq"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#             elif model_field.type_ == int or model_field == float:
#                 cls.params[f"{model_field.name}_lt"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#                 cls.params[f"{model_field.name}_gt"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#                 cls.params[f"{model_field.name}_eq"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#                 cls.params[f"{model_field.name}_not_eq"] = [
#                     model_field.type_,
#                     Query(model_field.default),
#                 ]
#             else:
#                 raise NotImplementedError(
#                     f"Field name {model_field.name} with {model_field.type_} not implemented"
#                 )
#             cls.params.update(additional_signature_fields)
#         signatures = []
#         signatures.extend(
#             inspect.Parameter(
#                 param,
#                 inspect.Parameter.POSITIONAL_OR_KEYWORD,
#                 default=query[1],
#                 annotation=query[0],
#             )
#             for param, query in cls.params.items()
#         )
#         # dynamic_call.__signature__ = inspect.Signature(signatures)
#
#         cls.__init__.__signature__ = inspect.Signature(signatures)
#
# dynamic_query_meta_class = DynamicQueryMetaClass(owner_model)
# print(dynamic_query_meta_class)
