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

   # HuggingFace
   pip install "multi-storage-client[huggingface]"

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

.. note::

   The ``zarr`` extra is unavailable on Python 3.14, so ``msc.zarr`` and ``msc.xarray`` don't work
   there. Zarr 2.x can't run on Python 3.14 — it requires ``numcodecs<0.16``, and ``numcodecs``
   only ships Python 3.14 wheels from 0.16.4 on. Zarr 3.x does support Python 3.14 but is a
   breaking API change, and it requires Python 3.11 or higher, so it can't cover MSC's Python 3.10
   support. MSC will migrate to Zarr 3.x once Python 3.10 reaches end of life in October 2026.

   On Python 3.14, ``pip install "multi-storage-client[zarr]"`` succeeds but installs nothing.
   Use Python 3.13 or older for Zarr workloads, or install Zarr 3.x yourself and reach MSC through
   the fsspec integration with ``msc://`` URLs.

MSC also provides an optional Model Context Protocol (MCP) server that enables conversational interaction with storage through AI assistants.

.. code-block:: shell
   :caption: Install MSC with MCP Server support.

   # MCP Server only.
   pip install "multi-storage-client[mcp]"

   # Verify installation.
   msc mcp-server --help

.. note::

   The MCP Server requires Python 3.10 or higher. For complete MCP Server documentation, see :doc:`/user_guide/mcp_server`.
