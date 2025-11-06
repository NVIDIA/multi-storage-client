********
Concepts
********

MSC has 3 main concepts:

.. glossary::

   storage service
      A service that stores objects/files such as AWS S3, Azure Blob Storage, Google Cloud Storage (GCS),
      NVIDIA AIStore, Oracle Cloud Infrastructure (OCI) Object Storage, POSIX file systems, HuggingFace, and more.

   provider
      A provider implements generic object/file operations such as create, read, update, delete, and list
      or supply credentials for a specific :term:`storage service`.

      Providers are further subdivided into storage providers, metadata providers, and credentials providers.

      Storage providers operate on a storage service directly.

      Metadata providers operate on manifest files to accelerate object/file enumeration and metadata retrieval.

      Credentials providers supply credentials for accessing objects/files.

   client
      The client exposes generic object and file operations such as create, read, update, delete, and list.
      It does validation and path translation before calling a :term:`provider`. A client may bundle several
      providers together.