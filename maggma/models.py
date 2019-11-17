from typing import Dict, List, Optional
from pydantic import BaseModel, Schema
from enum import Enum
from datetime import datetime
from typing_extensions import Literal
from monty.json import MSONable


class Time(BaseModel):
    string: str


class Lattice(BaseModel):
    a: float = Schema(..., title="*a* lattice parameter")
    alpha: int = Schema(..., title="Angle between a and b lattice vectors")
    b: float = Schema(..., title="b lattice parameter")
    beta: int = Schema(..., title="angle between a and c lattice vectors")
    c: float = Schema(..., title="c lattice parameter")
    gamma: int = Schema(..., title="angle between b and c lattice vectors")
    volume: float = Schema(..., title="lattice volume")
    matrix: List[List[int]] = Schema(..., title="matrix representation of this lattice")


class Specie(BaseModel):
    element: str = Schema(..., title="element")
    occu: float = Schema(..., title="site occupancy")


class Site(BaseModel):
    abc: List[float] = Schema(..., title="fractional coordinates")
    label: str = None
    species: List[Specie] = Schema(..., title="species occupying this site")
    xyz: List[float] = Schema(..., title="cartesian coordinates")
    properties: Dict[str, int] = Schema(..., title="arbitrary property list")


class Structure(BaseModel):
    charge: Optional[float] = Schema(None, title="site wide charge")
    lattice: Lattice
    sites: List[Site]


class CrystalSystem(str, Enum):
    tetragonal = "tetragonal"
    triclinic = "triclinic"
    orthorhombic = "orthorhombic"
    monoclinic = "monoclinic"
    hexagonal = "hexagonal"
    cubic = "cubic"
    trigonal = "trigonal"


class Symmetry(BaseModel):
    source: str
    symbol: str
    number: int
    point_group: str
    crystal_system: CrystalSystem
    hall: str


class Material(BaseModel):
    chemsys: str = Schema(
        ...,
        title="chemical system as a string of elements in alphabetical order delineated by dashes",
    )
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
    structure: Structure = Schema(..., title="the structure object")
    symmetry: Symmetry = Schema(..., title="symmetry data for this")
    task_id: str = Schema(
        ..., title="task id for this material. Also called the material id"
    )
    volume: float = Schema(..., title="")


class CommonPaginationParams:
    def __init__(self, skip: int = 0, limit: int = 10):
        self.skip = skip
        self.limit = limit

