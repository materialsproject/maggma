# import testing modules

# set module path
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import files
from fastapi import Depends, FastAPI, Path, APIRouter

from tests.models import CommonPaginationParams
from pymatgen.core.composition import Composition, CompositionError
from typing import List

from endpoint_cluster import EndpointCluster
from maggma.stores import JSONStore
from tests.models import Material


class MaterialEndPoint(EndpointCluster):
    def __init__(self, db_source):
        super().__init__(db_source, Material)
        self.material_router = APIRouter()
        self.material_router.get("/chemsys/{chemsys}",
                        response_description="Get all the materials that matches the chemsys field",
                        response_model=List[self.Model])(self.get_on_chemsys)


    async def get_on_chemsys(self, chemsys: str = Path(..., title="The task_id of the item to get"),
                             paginationParam: CommonPaginationParams = Depends()):
        skip, limit = paginationParam.skip, paginationParam.limit
        cursor = None
        elements = chemsys.split("-")
        unique_elements = set(elements) - {"*"}
        crit = dict()
        crit["elements"] = {"$all": list(unique_elements)}
        crit["nelements"] = len(elements)
        cursor = self.db_source.query(criteria=crit)
        raw_result = [c for c in cursor]
        for r in raw_result:
            material = Material(**r)
        return raw_result[skip:skip + limit]

    def is_chemsys(self, query: str):
        if "-" in query:
            query = query.split("-")
            for q in query:
                try:
                    Composition(q)
                except CompositionError as e:
                    return False
            return True
        return False

    def is_formula(self, query):
        try:
            Composition(query)
            return True
        except Exception:
            return False

    def is_task_id(self, query):
        if "-" in query:
            splits = query.split("-")
            if len(splits) == 2 and splits[1].isdigit():
                return True
        return False

    def run(self):
        app = FastAPI()
        app.include_router(self.material_router, prefix="/materials")
        super(MaterialEndPoint, self).run(app)


store = JSONStore("./more_mats.json")
store.connect()
materialEndpointCluster = MaterialEndPoint(store)

materialEndpointCluster.run()
