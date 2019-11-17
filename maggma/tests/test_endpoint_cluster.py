# import testing modules
import unittest
import warnings

# set module path
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import files
from fastapi import FastAPI
from maggma.stores import JSONStore
from starlette.testclient import TestClient

from endpoint_cluster import EndpointCluster
from models import Material


class TestEndpointCluster(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', category=DeprecationWarning)

        self.store = JSONStore("../more_mats.json")
        self.store.connect()
        cluster = EndpointCluster(self.store, Material)

        self.app = FastAPI()
        self.app.include_router(
            cluster.router,
            prefix="/materials",
            responses={404: {"description": "Not found"}},
        )

        self.client = TestClient(self.app)

    def test_materials_root(self):
        response = self.client.get("/materials")
        assert response.status_code == 200
        assert response.json() == {"result": "At example root level"}

    def test_materials_task_id(self):
        task_id = "mp-7283"
        response = self.client.get("/materials/task_id/" + task_id)
        response_json = response.json()

        actual_json = self.store.query_one(criteria={"task_id": task_id})

        assert response.status_code == 200
        # assert response_json == actual_json
        assert response_json["chemsys"] == actual_json["chemsys"]
        assert response_json["density"] == actual_json["density"]

    def test_materials_get_on_chemsys(self):
        chemsys = "B-La"
        response = self.client.get("/materials/chemsys/" + chemsys)
        actual_cursor = self.store.query(criteria={"chemsys": chemsys})
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json) == actual_cursor.count()
        assert response_json[0]["chemsys"] == actual_cursor[0]["chemsys"]
        assert response_json[0]["composition"] == actual_cursor[0]["composition"]
        assert response_json[0]["nelements"] == actual_cursor[0]["nelements"]
        assert response_json[0]["task_id"] == actual_cursor[0]["task_id"]

        chemsys = "B-*"
        response = self.client.get("/materials/chemsys/" + chemsys)
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json) == 2
        assert response_json[0]["chemsys"] == "B-La"
        assert response_json[0]["composition"] == {"B": 16, "La": 4}
        assert response_json[0]["task_id"] == "mp-7283"
        assert response_json[1]["chemsys"] == "B-Dy"
        assert response_json[1]["composition"] == {"B": 16, "Dy": 4}
        assert response_json[1]["task_id"] == "mp-2719"

    def test_materials_get_on_materials(self):
        chemsys = "B-La"
        response = self.client.get("/materials/" + chemsys)
        response_json = response.json()
        assert response_json[0]["chemsys"] == "B-La"
        assert response_json[0]["task_id"] == "mp-7283"

        formula = "LaB4"
        response = self.client.get("/materials/" + formula)
        response_json = response.json()
        assert response_json[0]["chemsys"] == "B-La"
        assert response_json[0]["task_id"] == "mp-7283"

        task_id = "mp-7283"
        response = self.client.get("/materials/" + task_id)
        response_json = response.json()
        assert response_json["chemsys"] == "B-La"
        assert response_json["task_id"] == "mp-7283"

    def test_materials_get_on_formula(self):
        formula = "LaB4"
        response = self.client.get("/materials/formula/" + formula)
        response_json = response.json()

        assert response_json[0]["chemsys"] == "B-La"
        assert response_json[0]["task_id"] == "mp-7283"

        formula = "Ho*"
        response = self.client.get("/materials/formula/" + formula)
        response_json = response.json()

        assert response_json[0]["chemsys"] == "Ho-Rh"
        assert response_json[0]["task_id"] == "mp-2163"




if __name__ == '__main__':
    unittest.main()
