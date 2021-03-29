import pytest

from maggma.api.utils import merge_queries, api_sanitize
from monty.json import MSONable

from pydantic import BaseModel, Field
from datetime import datetime


class Pet(MSONable):
    def __init__(self, name, age):
        self.name = name
        self.age = age


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(..., title="Owne'r Age")
    weight: float = Field(..., title="Owner's weight")
    last_updated: datetime = Field(..., title="Last updated date for this record")
    pet: Pet = Field(..., title="Owner's Pet")


def test_api_sanitize():

    with pytest.raises(ValueError):
        Owner()

    new_owner = api_sanitize(Owner)

    # This will fail if non-optional fields are not turned off
    new_owner()

    new_owner2 = api_sanitize(Owner, fields_to_leave=["name"])
    with pytest.raises(ValueError):
        new_owner2()

    temp_pet_dict = Pet(name="fido", age=3).as_dict()
    with pytest.raises(ValueError):
        new_owner2(pet=temp_pet_dict)

    new_owner3 = api_sanitize(Owner, allow_dict_msonable=True)

    new_owner3(pet=temp_pet_dict)
