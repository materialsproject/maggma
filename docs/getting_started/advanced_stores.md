# Configurations and Usage of Advanced `store`'s

## S3Store

### Configuration

The S3Store interfaces with S3 object storage via [boto3](https://pypi.org/project/boto3/).
For this to work properly, you have to set your basic configuration in `~/.aws/config`
```buildoutcfg
[default]
source_profile = default
```

Then, you have to set up your credentials in `~/.aws/credentials`
```buildoutcfg
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

For more information on the configuration please see the following [documentation](https://docs.aws.amazon.com/credref/latest/refdocs/settings-global.html).
Note that while these configurations are in the `~/.aws` folder, they are shared by other similar services like the self-hosted [minio](https://min.io/) service.

### Basic Usage

MongoDB is not designed to handle large object storage.
As such, we created an abstract object that combines the large object storage capabilities of Amazon S3 and the easy, python-friendly query language of MongoDB.
These `S3Store`s all include an `index` store that only stores specific queryable data and the object key for retrieving the data from an S3 bucket using the `key` attribute (called `'fs_id'` by default).

An entry of in the `index` may look something like this:
```
{
    fs_id : "5fc6b87e99071dfdf04ca871"
    task_id : "mp-12345"
}
```
Please note that since we are giving users the ability to reconstruct the index store using the object metadata, the object size in the `index` is limited by the metadata and not MongoDB.
Different S3 services might have different rules, but the limit is typically smaller: 8 KB for [aws](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)

The `S3Store` should be constructed as follows:

```python
from maggma.stores import MongograntStore, S3Store
index = MongograntStore("ro:mongodb03/js_cathodes",
                        "atomate_aeccar0_fs_index",
                        key="fs_id")
s3store = S3Store(index=index,
        bucket="<<BUCKET_NAME>>",
        s3_profile="<<S3_PROFILE_NAME>>",
        compress= True,
        endpoint_url= "<<S3_URL>>",
        sub_dir= "atomate_aeccar0_fs",
        s3_workers=4
       )
```

The `subdir` field creates subdirectories in the bucket to help the user organize their data.

### Parallelism

Once you start working with large quantities of data, the speed at which you process this data will often be limited by database I/O.
For the most time-consuming upload part of the process, we have implemented thread-level parallelism in the `update` member function.
The `update` function received an entire chunk of processed data as defined by `chunk_size`,
however since `Store.update` is typically called in the `update_targets` part of a builder, where builder execution is not longer multi-threaded.
As such, we multithread the execution inside of `update` using `s3_workers` threads to perform the database write operation.
As a general rule of thumb, if you notice that your update step is taking too long, you should change the `s3_worker` field which is
optimized differently based on server-side resources.
