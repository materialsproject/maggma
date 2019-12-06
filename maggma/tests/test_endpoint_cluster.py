import unittest
import warnings
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from maggma.stores import JSONStore
from examples.materials_endpoint import MaterialEndpointCluster

from starlette.testclient import TestClient


class TestMaterialsEndpoint(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', category=DeprecationWarning)
        self.store = JSONStore("../examples/more_mats.json")
        self.store.connect()
        materials_endpoint = MaterialEndpointCluster(self.store)
        self.client = TestClient(materials_endpoint.app)

    def test_materials_root(self):
        response = self.client.get("/materials")
        assert response.status_code == 200
        assert response.json() == {"result": "At MaterialsEndpointCluster level"}

    def test_root(self):
        response = self.client.get("/")
        print(response.status_code)
        assert response.status_code == 200
        assert response.json() == {"result": "At EndpointCluster root level"}

    def test_root_task_id(self):
        response = self.client.get("/task_id/mp-7283")
        print(response.status_code)
        assert response.status_code == 200

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


if __name__ == '__main__':
    unittest.main()
