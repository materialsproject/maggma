# Usage:
# with multiprocessing:
#    python runner_sample.py
# with mpi(need mpi4py pacakge):
#    mpiexec -n 5 python runner_sample.py

from maggma.stores import MemoryStore
from maggma.builder import Builder
from maggma.runner import Runner
import random
import time


class MyDumbBuilder(Builder):

    def get_items(self):
        for i in range(3):
            yield i

    def process_item(self, item):
        print("processing item: {}".format(item))
        time.sleep(random.randint(0,5))
        return {item: "processed"}

    def update_targets(self, items):
        print("updating targets. Input recieved: {}".format(items))

    def finalize(self):
        print("Finalizing...")
        print("DONE!")

        
if __name__ == '__main__':        
    stores = [MemoryStore(str(i)) for i in range(7)]
    sources = [stores[0], stores[1], stores[3]]
    targets = [stores[3], stores[6]]
    mdb = MyDumbBuilder(sources, targets)
    builders = [mdb]    
    runner = Runner(builders)
    runner.run()
