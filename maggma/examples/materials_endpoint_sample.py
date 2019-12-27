# import testing modules

# set module path
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster_manager import ClusterManager
# import files
from models import MaterialModel
from fastapi import HTTPException, Depends
from fastapi import Path

from models import CommonPaginationParams, SpecieModel
from pymatgen.core.composition import Composition, CompositionError
from typing import List
from starlette.responses import RedirectResponse
from endpoint_cluster import EndpointCluster
from maggma.stores import JSONStore
import uvicorn
import starlette.status


class MaterialEndpointCluster(EndpointCluster):
    def __init__(self, db_source: JSONStore, prefix: str = "/materials", tags: list = []):
        super().__init__(db_source, MaterialModel, prefix, tags)
        # initialize routes
        # self.model = MaterialModel in this case
        self.router.get("/task_id/{task_id}",
                        response_description="Get Task ID",
                        response_model=self.model,
                        tags=self.tags,
                        status_code=starlette.status.HTTP_200_OK,
                        ) \
            (self.get_on_task_id)
        self.router.get("/chemsys/{chemsys}",
                        response_description="Get all the materials that matches the chemsys field",
                        response_model=List[self.model],
                        tags=self.tags
                        )(self.get_on_chemsys)
        self.router.get("/formula/{formula}",
                        response_model=List[self.model],
                        response_description="Get all the materials that matches the formula field",
                        tags=self.tags) \
            (self.get_on_formula)
        self.router.get("/{query}",
                        tags=self.tags)(self.get_on_materials)
        self.router.get("/distinct/",
                        tags=self.tags)(self.get_distinct_choices)

    async def get_on_chemsys(self, chemsys: str = Path(..., title="The task_id of the item to get"),
                             paginationParam: CommonPaginationParams = Depends()):
        """
        Ex: http://127.0.0.1:8000/chemsys/B-La
        Args:
            chemsys: the user input of chemical system
            paginationParam: optional skip and limit parameter

        Returns:
            list of materials that satisfy the input chemsys, limited by the paginationParam
        """
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
            material = MaterialModel(**r)
        return raw_result[skip:skip + limit]

    async def get_on_formula(self, formula: str = Path(..., title="The formula of the item to get"),
                             paginationParam: CommonPaginationParams = Depends()):
        """
        Return the materials that matches the input formula
        - **formula**: The formula of the item to get
        - **paginationParam**:

        Ex: http://127.0.0.1:8000/formula/LaB4
        Args:
            formula: the user input of formula anonymous
            paginationParam: optional skip and limit parameter

        Returns:
            list of materials that satisfy the user input formula, limited by the paginationParam
        """
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

    async def get_on_materials(self, query: str = Path(...)):
        """
        To re-route to the correct route
        Args:
            query: the input query, can be in any string format

        Returns:
            RedirectResponse to the correct route
        """
        if self.is_task_id(query):
            return RedirectResponse("/task_id/{}".format(query))
        elif self.is_chemsys(query):
            return RedirectResponse("/chemsys/{}".format(query))
        elif self.is_formula(query):
            return RedirectResponse("/formula/{}".format(query))
        else:
            return HTTPException(status_code=404,
                                 detail="WARNING: Query <{}> does not match any of the endpoint features".format(query))

    async def get_distinct_choices(self,
                                   paginationParam: CommonPaginationParams = Depends()):
        """

        Args:
            paginationParam:

        Returns:
            List of distint values of each field
        """
        skip, limit = paginationParam.skip, paginationParam.limit
        data = self.db_source.query_one()
        keys = data.keys()
        result = dict()
        for k in keys:
            result[k] = self.db_source.distinct(k)[skip:skip + limit]
        return result

    async def get_on_task_id(self, task_id: str = Path(..., title="The task_id of the item to get")):
        """
        http://127.0.0.1:8000/task_id/mp-30933
        Args:
            task_id: in the format of mp-NUMBER

        Returns:
            a single material that satisfy the Material Model
        """
        cursor = self.db_source.query(criteria={"task_id": task_id})
        material = cursor[0] if cursor.count() > 0 else None
        if material:
            material = self.model(**material)
            return material
        else:
            raise HTTPException(status_code=404, detail="Item with Task_id = {} not found".format(task_id))

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
    json_store = JSONStore("./data/more_mats.json")
    json_store.connect()

    ## initialize endpoints
    mp_endpoint1 = MaterialEndpointCluster(db_source=json_store, prefix="/materials1", tags=["material", "1"])
    mp_endpoint2 = MaterialEndpointCluster(db_source=json_store, prefix="/materials2", tags=["material", "2"])
    general_endpoint = EndpointCluster(db_source=json_store, model=SpecieModel)

    clusterManager = ClusterManager()
    clusterManager.addEndpoint(mp_endpoint1)
    clusterManager.addEndpoint(mp_endpoint2)
    clusterManager.addEndpoint(general_endpoint)

    clusterManager.runAllEndpoints()
