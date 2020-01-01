=================
Introduction
=================

.. highlight:: console

Maggma is designed to provide a consistent interface to a variety of data sources using a standard Mongo-style collections and methods. Many of the internal of Maggma leverage pyMongo explicitly or pass out cursors and other pyMongo objects. Several advanced Maggma interfaces simplify the the process of connecting to data sources such as Mongo's internal GridFS or AWS block storage. A mongo database is still required. Other interfaces provide implicit aggregation, sandboxing, or aliasing to enable complex data access patterns.

An important aspect of Maggma is to provide a framework to describe data manipulation, particuarly in the context of complex data pipelines such as those employed in Material Science. Maggma enables these pipelines to scale by providing a "Builder" interface that can be used to construct a data operation between Maggma Stores. The "Runner" class in Maggma enables deploying these "Builder"s to various resources whether it is simple serial processing or multiprocessing across cores on a single machine or multiprocessing across computers using MPI.

Finally, everything in Maggma is designed to serializable to json. The monty MSONable pattern is employed so that stores, builders, and runners can be easily coverted to python dictionaries and back or even dumped quickly to file and loaded back. This ensures that pipelines can be easily saved and rerun at later times, ensuring explicit documentation of a data manipulation process.
