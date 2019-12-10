# import testing modules

# set module path
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import files
from fastapi import Depends, Path, APIRouter

from examples.models import CommonPaginationParams
from pymatgen.core.composition import Composition, CompositionError
from typing import List

from endpoint_cluster import EndpointCluster
from maggma.stores import JSONStore
from examples.models import Material


class MaterialEndpointCluster(EndpointCluster):
    def __init__(self, db_source):
        super().__init__(db_source, Material)
        # initialize routes
        self.materials_router = APIRouter()
        self.materials_router.get("/chemsys/{chemsys}",
                                  response_description="Get all the materials that matches the chemsys field",
                                  response_model=List[self.Model])(self.get_on_chemsys)
        self.materials_router.get("/formula/{formula}",
                                  response_model=List[self.Model],
                                  response_description="Get all the materials that matches the formula field") \
            (self.get_on_formula)
        self.materials_router.get("/")(self.get_on_materials)
        # add route the app
        self.app.include_router(self.materials_router, prefix="/materials")

    async def get_on_materials(self):
        return {"result": "At MaterialsEndpointCluster level"}

    async def get_on_chemsys(self, chemsys: str = Path(..., title="The task_id of the item to get"),
                             paginationParam: CommonPaginationParams = Depends()):
        '''
        Ex: http://127.0.0.1:8080/materials/chemsys/B-La
        :param chemsys:
        :param paginationParam:
        :return:
        '''
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

    async def get_on_formula(self, formula: str = Path(..., title="The formula of the item to get"),
                             paginationParam: CommonPaginationParams = Depends()):
        '''
        Ex: http://127.0.0.1:8080/materials/formula/LaB4
        :param formula:
        :param paginationParam:
        :return:
        '''
        skip, limit = paginationParam.skip, paginationParam.limit
        cursor = None
        if "*" in formula:
            nstars = formula.count("*")
            dummies = 'ADEGJLMQRXZ'
            formula_dummies = formula.replace("*", "{}").format(*dummies[:nstars])
            try:
                comp = Composition(formula_dummies).reduced_composition
                crit = dict()
                crit["formula_anonymous"] = comp.anonymized_formula
                real_elts = [str(e) for e in comp.elements
                             if not e.as_dict().get("element", "A") in dummies]

                # Paranoia below about floating-point "equality"
                for el, n in comp.to_reduced_dict.items():
                    if el in real_elts:
                        crit['composition_reduced.{}'.format(el)] = {"$gt": .99 * n, "$lt": 1.01 * n}

                # pretty_formula = comp.reduced_formula
                cursor = self.db_source.query(criteria=crit)
                result = [c for c in cursor]
                return result[skip:skip + limit]
            except Exception as e:
                raise e
        else:
            cursor = self.db_source.query(criteria={"formula_pretty": formula})
            result = [] if cursor is None else [i for i in cursor]
            return result[skip:skip + limit]

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


if __name__ == "__main__":
    store = JSONStore("./more_mats.json")
    store.connect()
    materialEndpointCluster = MaterialEndpointCluster(store)

    materialEndpointCluster.run()
