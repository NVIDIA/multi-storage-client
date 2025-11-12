##########################
Multi-Storage Client (MSC)
##########################

The Multi-Storage Client (MSC) is a unified high-performance Python client for object and file stores such as AWS S3, Azure Blob Storage, Google Cloud Storage (GCS), NVIDIA AIStore, Oracle Cloud Infrastructure (OCI) Object Storage, POSIX file systems, and more.

It provides a generic interface to interact with objects and files across various storage services. This lets you spend less time learning each storage service's unique interface and lets you change where data is stored without having to change how your code accesses it.

.. toctree::
   :maxdepth: 2
   :caption: Introduction

   user_guide/concepts
   user_guide/installation

.. toctree::
   :maxdepth: 2
   :caption: Basic Usage

   user_guide/quickstart
   user_guide/telemetry
   user_guide/manifests
   user_guide/cache
   user_guide/libraries
   user_guide/cli

.. toctree::
   :maxdepth: 2
   :caption: Advanced Usage

   user_guide/replicas
   user_guide/rust
   user_guide/multi_storage_file_system
   user_guide/mcp_server

.. toctree::
   :maxdepth: 2
   :caption: References

   references/configuration
   references/api

.. toctree::
   :maxdepth: 2
   :caption: Indices

   /genindex
   /modindex
