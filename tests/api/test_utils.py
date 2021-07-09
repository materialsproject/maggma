from datetime import datetime
from enum import Enum

import pytest
from monty.json import MSONable
from pydantic import BaseModel, Field

from maggma.api.utils import api_sanitize, object_id_serilaization_helper
from typing import Union

from bson import ObjectId


class SomeEnum(Enum):
    A = 1
    B = 2
    C = 3


class Pet(MSONable):
    def __init__(self, name, age):
        self.name = name
        self.age = age


class AnotherPet(MSONable):
    def __init__(self, name, age):
        self.name = name
        self.age = age


class AnotherOwner(BaseModel):
    name: str = Field(..., description="Ower name")
    weight_or_pet: Union[float, AnotherPet] = Field(..., title="Owners weight or Pet")


class Owner(BaseModel):
    name: str = Field(..., title="Owner's name")
    age: int = Field(..., title="Owne'r Age")
    weight: float = Field(..., title="Owner's weight")
    last_updated: datetime = Field(..., title="Last updated date for this record")
    pet: Pet = Field(..., title="Owner's Pet")
    other: SomeEnum = Field(..., title="A enum?")


def test_api_sanitize():

    # Ensure model validation fails
    with pytest.raises(ValueError):
        Owner()

    # This should still fail validation
    new_owner = api_sanitize(Owner, fields_to_leave=["Owner.name"])
    with pytest.raises(ValueError):
        new_owner()

    new_owner(name="owner")

    # These will fail if non-optional fields are not turned off
    new_owner2 = api_sanitize(Owner)
    new_owner()  # api_sanitize is in-place
    new_owner2()
    Owner()

    # This should fail type validation for pet
    with pytest.raises(Exception):
        Owner(pet="fido")

    temp_pet_dict = Pet(name="fido", age=3).as_dict()
    bad_pet_dict = dict(temp_pet_dict)
    del bad_pet_dict["@module"]
    del bad_pet_dict["@class"]

    # This should fail because of bad data type
    with pytest.raises(Exception):
        Owner(pet=bad_pet_dict)

    assert isinstance(Owner(pet=temp_pet_dict).pet, Pet)

    api_sanitize(Owner, allow_dict_msonable=True)

    # This should still fail because of bad data type
    with pytest.raises(Exception):
        Owner(pet=bad_pet_dict)

    # This should work
    assert isinstance(Owner(pet=temp_pet_dict).pet, dict)

    # This should work evne though AnotherPet is inside the Union type
    api_sanitize(AnotherOwner, allow_dict_msonable=True)
    temp_pet_dict = AnotherPet(name="fido", age=3).as_dict()

    assert isinstance(AnotherPet.validate_monty(temp_pet_dict), dict)


def test_object_id_serilaization_helper():
    oid = ObjectId("60b7d47bb671aa7b01a2adf6")
    assert object_id_serilaization_helper(oid) == "60b7d47bb671aa7b01a2adf6"


@pytest.mark.xfail
def test_object_id_serilaization_helper_xfail():
    oid = "test"
    object_id_serilaization_helper(oid)
