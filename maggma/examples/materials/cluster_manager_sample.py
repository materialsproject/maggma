import os, sys
from maggma.stores import JSONStore

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cluster_manager import ClusterManager
from endpoint_cluster import EndpointCluster
from materials.materials_endpoint import MaterialEndpointCluster
from materials.models import SpecieModel

if __name__ == "__main__":
    json_store = JSONStore("./materials/data/more_mats.json")
    json_store.connect()
    custom_responses = {
        204: {"description": "CUSTOM_DESCRIPTION: No content not found"},
        302: {"description": "CUSTOM_DESCRIPTION: The item was moved"},
        404: {"description": "CUSTOM_DESCRIPTION: NOT FOUND"},
    }

    mp_endpoint1 = MaterialEndpointCluster(db_source=json_store,
                                           prefix="/materials1",
                                           tags=["material", "1"],
                                           responses=custom_responses)
    mp_endpoint2 = MaterialEndpointCluster(db_source=json_store,
                                           prefix="/materials2",
                                           tags=["material", "2"])
    general_endpoint = EndpointCluster(db_source=json_store,
                                       model=SpecieModel)

    clusterManager = ClusterManager()
    clusterManager.addEndpoint(mp_endpoint1)
    clusterManager.addEndpoint(mp_endpoint2)
    clusterManager.addEndpoint(general_endpoint)

    clusterManager.runAllEndpoints()
