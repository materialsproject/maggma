from typing import Dict, List, Optional
from pydantic import BaseModel, Schema, validator
import pydantic
from enum import Enum
from datetime import datetime
from typing_extensions import Literal
from monty.json import MSONable
import graphene
from graphene_pydantic import PydanticObjectType


class Time(BaseModel):
    string: str


class LatticeModel(pydantic.BaseModel):
    a: float = Schema(..., title="*a* lattice parameter")
    alpha: int = Schema(..., title="Angle between a and b lattice vectors")
    b: float = Schema(..., title="b lattice parameter")
    beta: int = Schema(..., title="angle between a and c lattice vectors")
    c: float = Schema(..., title="c lattice parameter")
    gamma: int = Schema(..., title="angle between b and c lattice vectors")
    volume: float = Schema(..., title="lattice volume")
    matrix: List[List[int]] = Schema(..., title="matrix representation of this lattice")


class SpecieModel(pydantic.BaseModel):
    element: str = Schema(..., title="element")
    occu: float = Schema(..., title="site occupancy")


class SiteModelPropertiesModel(pydantic.BaseModel):
    magmom: int = Schema(..., title="arbitrary property list")


class SiteModel(pydantic.BaseModel):
    abc: List[float] = Schema(..., title="fractional coordinates")
    label: str = None
    species: List[SpecieModel] = Schema(..., title="species occupying this site")
    xyz: List[float] = Schema(..., title="cartesian coordinates")
    # properties: Dict[str, int] = Schema(..., title="arbitrary property list") # this will cause graphene to crash
    properties: SiteModelPropertiesModel


class StructureModel(pydantic.BaseModel):
    charge: Optional[float] = Schema(None, title="site wide charge")
    lattice: LatticeModel
    sites: List[SiteModel]


class CrystalSystem(str, Enum):
    tetragonal = "tetragonal"
    triclinic = "triclinic"
    orthorhombic = "orthorhombic"
    monoclinic = "monoclinic"
    hexagonal = "hexagonal"
    cubic = "cubic"
    trigonal = "trigonal"
    types = [tetragonal, triclinic, orthorhombic, monoclinic, hexagonal, cubic, trigonal]


class SymmetryModel(pydantic.BaseModel):
    source: str
    symbol: str
    number: int
    point_group: str
    # crystal_system: CrystalSystem # this will cause graphene pydantic to crash
    crystal_system: str
    hall: str

    @validator('crystal_system')
    def check_crystal_system(cls, cs):
        if cs in CrystalSystem.types:
            return cs
        else:
            raise ValueError(f'Unknown Crystal System')


class CompositionModel(pydantic.BaseModel):
    # this is not going to work because name != elem, for instance, Ti != elem
    elem: int = Schema(None, title="composition as a dictionary of elements and their amount")


class CompositionReducedModel(pydantic.BaseModel):
    # this is not going to work because name != elem, for instance, Ti != elem
    elem: int = Schema(None, title="reduced composition as a dictionary of elements and their amount")


class MaterialModel(pydantic.BaseModel):
    chemsys: str = Schema(
        ...,
        title="chemical system as a string of elements in alphabetical order delineated by dashes",
    )
    # composition: CompositionModel
    # composition_reduced: CompositionReducedModel
    # Please see https://pypi.org/project/graphene-pydantic/
    # dictionaries
    composition: Dict[str, int] = Schema(
        None, title="composition as a dictionary of elements and their amount"
    )
    composition_reduced: Dict[str, int] = Schema(
        None, title="reduced composition as a dictionary of elements and their amount"
    )
    created_at: str = Schema(
        None,
        title="creation time for this material defined by when the first structure optimization calculation was run",
    )
    errors: List[str] = Schema(None, title="List of errors")
    warnings: List[str] = Schema(None, title="List of warnings")
    density: float = Schema(..., title="mass density")
    elements: List[str] = Schema(..., title="list of elements")
    formula_anonymous: str = Schema(..., title="formula using anonymized elements")
    formula_pretty: str = Schema(..., title="clean representation of the formula")
    last_updated: datetime = Schema(..., title="timestamp for the most recent calculation")
    nelements: int = Schema(..., title="number of elements")
    nsites: int = Schema(..., title="number of sites")
    structure: StructureModel = Schema(..., title="the structure object")
    symmetry: SymmetryModel = Schema(..., title="symmetry data for this")
    task_id: str = Schema(
        ..., title="task id for this material. Also called the material id"
    )
    task_ids: List[str] = Schema(None, title="List of task-ids that this material is associated with") ## TODO complete
    volume: float = Schema(..., title="")
    _id: str


class CommonPaginationParams:
    def __init__(self, skip: int = 0, limit: int = 10):
        self.skip = skip
        self.limit = limit


class Lattice(PydanticObjectType):
    class Meta:
        model = LatticeModel


class Specie(PydanticObjectType):
    class Meta:
        model = SpecieModel


class SiteModelProperties(PydanticObjectType):
    class Meta:
        model = SiteModelPropertiesModel


class Site(PydanticObjectType):
    class Meta:
        model = SiteModel


class Structure(PydanticObjectType):
    class Meta:
        model = StructureModel


class Symmetry(PydanticObjectType):
    class Meta:
        model = SymmetryModel


class Composition(PydanticObjectType):
    class Meta:
        model = CompositionModel


class CompositionReduced(PydanticObjectType):
    class Meta:
        model = CompositionReducedModel


class Material(PydanticObjectType):
    class Meta:
        model = MaterialModel
        exclude_fields = ("_id",
                          "composition",
                          "composition_reduced")
