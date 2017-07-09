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

    def __init__(self, N,  sources, targets, get_chunk_size, process_chunk_size=1):
        super(MyDumbBuilder, self).__init__(sources, targets, get_chunk_size, process_chunk_size)
        self.N = N
    

    def get_items(self):
        for i in range(self.N):
            yield i

    def process_item(self, item):
        print("processing item: {}".format(item))
        #time.sleep(random.randint(0,5))
        return {item: "processed"}

    def update_targets(self, items):
        print("Updating targets ...")        
        print("Received {} processed items".format(len(items)))
        print("Processed items: {}".format(items))

    def finalize(self):
        print("Finalizing ...")
        print("DONE!")

        
if __name__ == '__main__':
    N=10
    get_chunk_size=3
    stores = [MemoryStore(str(i)) for i in range(7)]
    sources = [stores[0], stores[1], stores[3]]
    targets = [stores[3], stores[6]]
    mdb = MyDumbBuilder(N, sources, targets, get_chunk_size=get_chunk_size)
    builders = [mdb]    
    runner = Runner(builders)
    runner.run()
