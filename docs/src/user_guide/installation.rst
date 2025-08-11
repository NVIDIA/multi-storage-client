############
Installation
############

MSC is vended as the ``multi-storage-client`` package on PyPI.

The base :term:`client` supports POSIX file systems by default, but there are extras for each :term:`storage service`
which provide the necessary package dependencies for its corresponding storage provider.

While MSC can be installed with minimal dependencies, we strongly recommend installing the ``observability-otel``
extra dependencies to enable observability features. Without observability dependencies, you will have limited visibility
into MSC's operations and performance, making it harder to debug issues and optimize your application.

.. code-block:: shell
   :caption: Install MSC with observability dependencies.

   pip install multi-storage-client[observability-otel]


.. code-block:: shell
   :caption: Install MSC with storage provider dependencies.

   # POSIX file systems.
   pip install multi-storage-client

   # NVIDIA AIStore.
   pip install "multi-storage-client[aistore]"

   # Azure Blob Storage.
   pip install "multi-storage-client[azure-storage-blob]"

   # AWS S3 and S3-compatible object stores.
   pip install "multi-storage-client[boto3]"

   # Google Cloud Storage (GCS).
   pip install "multi-storage-client[google-cloud-storage]"

   # Oracle Cloud Infrastructure (OCI) Object Storage.
   pip install "multi-storage-client[oci]"

MSC also implements adapters to let higher-level libraries like fsspec or PyTorch work wth the MSC.
Likewise, there are extras for each higher level library.

.. code-block:: shell
   :caption: Install MSC with higher-level library adapter dependencies.

   # fsspec.
   pip install "multi-storage-client[fsspec]"

   # Hydra
   pip install "multi-storage-client[hydra-core]"

   # PyTorch.
   pip install "multi-storage-client[torch]"

   # Xarray.
   pip install "multi-storage-client[xarray]"

   # Zarr.
   pip install "multi-storage-client[zarr]"

   # Ray
   pip install "multi-storage-client[ray]"
