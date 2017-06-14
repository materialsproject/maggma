import six
import abc
import mongomock

@six.add_metaclass(abc.ABCMeta)
class Store:

	def __init__(self,lu_field='_lu'):
		self.lu_field = lu_field

	@property
	@abc.abstractmethod
	def collection(self):
		pass

	@abc.abstractmethod last_updated(self):
		pass

	@classmethod
	def lu_fiter(cls,targets):
		"""
		Assuming targets is a list of stores
		"""
		if isinstance(targets,Store) or isinstance(targets,Date):
			targets = [targets]

		targets = [t.last_updated if isinstance(t,Store) else t]
		return  {self.lu_field: {"$gte": max(targets)}}
		

class MongoStore(Store):

	@TODO: 
	def __init__(self, collection, lu_field='_lu'):
		self._collection = collection
		super(lu_field)

	def collection(self):
		return _collection

	def last_updated(self):
		return next(_collection.find({},{"_id":0,self.lu_field: 1}).sort([(lu_field,pymongo.DESCENDING)]))[lu_field]


class JSONStore(MongoStore):

	def __init__(self,path, lu_field='_lu'):
		_collection = mongomock.MongoCient().db.collection

		with open(path) as f:
			objects = list(json.load(f))
			objects = [objects] if not isinstance(objects,list) else objects
			_collection.insert_many(objects)

		super(_collection,lu_field)
