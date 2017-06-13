import json
from pymongo import MongoClient

def database(path):
    """Connect to a database given a path to a credential file.

    Args:
        path (str): Path to a database credential file.

    Returns:
        pymongo.database.Database: The database object.
    """
    with open(path) as f:
        cred = json.load(f)
        conn = MongoClient(
            cred.get('host', 'localhost'),
            cred.get('port', 27017))
        db = conn[cred['database']]
        if cred.get('username'):
            d.authenticate(cred['username'], cred['password'])
    return db
