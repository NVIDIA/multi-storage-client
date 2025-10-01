#############
API Reference
#############

**************
Storage Client
**************

.. autoclass:: multistorageclient.StorageClient
   :members:

.. autoclass:: multistorageclient.StorageClientConfig
   :members:

*******
Pathlib
*******

.. automodule:: multistorageclient.pathlib
   :members:
   :undoc-members:

*********
Shortcuts
*********

.. automodule:: multistorageclient.shortcuts
   :members:
   :undoc-members:

*****
Types
*****

.. automodule:: multistorageclient.types
   :members:
   :undoc-members:

*********
Providers
*********

.. automodule:: multistorageclient.providers.posix_file
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.manifest_metadata
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.ais
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.azure
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.gcs
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.gcs_s3
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.oci
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.s3
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.s8k
   :members:
   :undoc-members:

.. automodule:: multistorageclient.providers.huggingface
   :members:
   :undoc-members:

*********
Telemetry
*********

.. autoclass:: multistorageclient.telemetry.Telemetry

.. autoclass:: multistorageclient.telemetry.TelemetryManager

.. automodule:: multistorageclient.telemetry
   :members: TelemetryMode, init
   :undoc-members:

Attributes
==========

.. automodule:: multistorageclient.telemetry.attributes.base
   :members: AttributesProvider
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.environment_variables
   :members:
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.host
   :members:
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.msc_config
   :members:
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.process
   :members:
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.static
   :members:
   :undoc-members:

.. automodule:: multistorageclient.telemetry.attributes.thread
   :members:
   :undoc-members:

Metrics
=======

Readers
-------

.. automodule:: multistorageclient.telemetry.metrics.readers.diperiodic_exporting
   :members:
   :undoc-members:

**********
Generators
**********

.. automodule:: multistorageclient.generators
   :members:
   :undoc-members:

**********************
Higher-Level Libraries
**********************

fsspec
======

.. automodule:: multistorageclient.contrib.async_fs
   :members:
   :undoc-members:

Hydra
=====

.. automodule:: multistorageclient.contrib.hydra
   :members:
   :undoc-members:

NumPy
=====

.. automodule:: multistorageclient.contrib.numpy
   :members:
   :undoc-members:

PyTorch
=======

.. automodule:: multistorageclient.contrib.torch
   :members:
   :undoc-members:

Xarray
======

.. automodule:: multistorageclient.contrib.xarray
   :members:
   :undoc-members:

Zarr
====

.. automodule:: multistorageclient.contrib.zarr
   :members:
   :undoc-members:
