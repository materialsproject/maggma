import six
import abc

@six.add_metaclass(abc.ABCMeta)
class Builder:

	def __init__(self,sources,targets,get_chunk_size=1000, process_chunk_size = 1):
		self.sources = sources
		self.targets = targets
		self.process_chunk_size = process_chunk_size
		self.get_chunk_size = get_chunk_size

	@abc.abstractmethod
	def get_items(self):
		pass

	@abc.abstractmethod
	def process_item(self,item):
		"""
		Itended to be purely functional, with no DB access or update
		"""
		pass

	@abstractmethod
	def update_targets(self,items):
		"""

		"""
		pass

	@abc.abstractmethod
	def finalize(self)
		pass

	def run(self):
		pass

	def run_multiprocessing(self):
		pass

	def run_mpi(self):
		pass

