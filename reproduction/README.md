# Reproduction

Reproduction of https://github.com/NVIDIA/multi-storage-client/issues/19.

```shell
$ just run
# Prepare the virtual environment.
uv sync --all-extras --python python
Resolved 218 packages in 4ms
Audited 32 packages in 1ms
# Run the reproduction.
MSC_CONFIG=msc_bug.yaml uv run msc_bug.py
Resolving MSC client for msc://s3-my-bucket/my_file.txt...
s3
https://my-endpoint.s3.com
my-bucket

Resolving MSC client for s3://my-bucket/my_file.txt...
s3

my-bucket
Traceback (most recent call last):
  File "/Volumes/workplace/multi-storage-client/reproduction/msc_bug.py", line 16, in <module>
    main()
  File "/Volumes/workplace/multi-storage-client/reproduction/msc_bug.py", line 12, in main
    assert client._storage_provider._endpoint_url == "https://my-endpoint.s3.com"
AssertionError
error: Recipe `run` failed on line 22 with exit code 1
```
