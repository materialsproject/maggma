# Setting up MongoDB

Many users find MongoDB to best suit their data storage needs. While MongoDB [can be installed locally](https://www.mongodb.com/docs/manual/installation/), the easiest route is often to create a Mongo database via a cloud storage solution called [MongoDB Atlas](https://www.mongodb.com/atlas), which has a free tier. The setup instructions for using Maggma with MongoDB Atlas are described below:

1. Sign up for a free account on [MongoDB Atlas](https://www.mongodb.com/atlas).
2. Once logged in, select the "Create a Project" option and give your project a name (e.g. "MyProject"). Add your email address as the Project Owner.
3. Click the "Build a Database" button under the "Deployment > Database" section and choose the free (i.e. M0) option. Give your cluster a unique name (e.g. "MyCluster").
4. Select "Create" and enter your desired login credentials that you will use to access your database. You are probably best off not using special characters here since it will be URL-encoded. You should also use different credentials than your usual, since it's not uncommon to share credentials with trusted colleagues. Select "Finish and Close" when done.
5. Go to the "Collections" tab of your cluster, which is where you will create a database (e.g. "my_database") and corresponding data collection (e.g. "my_collection") by clicking the "Add My Own Data" button.
6. Under the "Security > Network Access" section, edit the IP Access List to allow access from anywhere for maximum flexibility.
7. Finally, retrieve your MongoDB URI, which is the address of your MongoDB cluster. You can find your database's URI by clicking the "Database" section in the sidebar and then selecting "Connect > Compass" and copying the link of the form `mongodb+srv://<username>:<password>@<host>`.

To test that you can connect to your database, run the following code:

```python
from maggma.stores import MongoURIStore

# Define your database credentials
store = MongoURIStore(
    "mongodb+srv://<username>:<password>@<host>",
    "my_collection",
    database="my_database",
)

# Query the database
with store:
    print(store.count())
```

!!! Note

    If you are using a self-hosted Mongo database, you will probably want to use a [`MongoStore`](https://materialsproject.github.io/maggma/reference/stores/#maggma.stores.mongolike.MongoStore) instead of the [`MongoURIStore`](https://materialsproject.github.io/maggma/reference/stores/#maggma.stores.mongolike.MongoURIStore), which takes slightly different arguments.
