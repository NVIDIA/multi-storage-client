######################
Higher-Level Libraries
######################

The MSC adapters for higher-level libraries use shortcuts under the hood.

******
fsspec
******

:py:mod:`multistorageclient.async_fs` aliases the :py:mod:`multistorageclient.contrib.async_fs` module.

This module provides the :py:class:`multistorageclient.contrib.async_fs.MultiStorageAsyncFileSystem` class which implements fsspec's ``AsyncFileSystem`` class.

.. note:: The ``msc://`` protocol is automatically registered when ``pip install multi-storage-client`` is run.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create an MSC-based AsyncFileSystem instance.
   fs = msc.async_fs.MultiStorageAsyncFileSystem()

   # Create a client for the data-s3-iad profile and open a file.
   file = fs.open("msc://data-s3-iad/animal-photos/red-panda.png")

   # Reuse the client for the data-s3-iad profile and download a file.
   fs.get_file(
       rpath="msc://data-s3-iad/animal-photos/red-panda.png",
       lpath="/tmp/animal-photos/red-panda.png"
   )

*****
Hydra
*****

The MSC Hydra plugin enables loading Hydra configurations directly from object storage using ``msc://`` URLs.

.. note:: The plugin is automatically registered by Hydra when both ``multistorageclient`` and ``hydra-core`` are installed.

.. code-block:: python
   :caption: Programmatic usage.

   import hydra
   import multistorageclient as msc
   from omegaconf import DictConfig

   # Load config directly from object storage
   @hydra.main(version_base=None, config_path="msc://profile/configs", config_name="training")
   def your_app(cfg: DictConfig) -> None:
       print(f"Loaded config: {cfg}")

.. code-block:: shell
   :caption: Shell usage.

   python your_app.py --config-path="msc://profile/configs" --config-name=training

*****
NumPy
*****

:py:mod:`multistorageclient.numpy` aliases the :py:mod:`multistorageclient.contrib.numpy` module.

This module provides ``load``, ``memmap``, and ``save`` methods for loading and saving NumPy arrays.

.. code-block:: python
   :linenos:

   import multistorageclient as msc
   import numpy

   # Create a client for the data-s3-iad profile and load an array.
   array = msc.numpy.load("msc://data-s3-iad/numpy-arrays/ndarray-1.npz")

   # Reuse the client for the data-s3-iad profile and load a memory-mapped array.
   mmarray = msc.numpy.memmap("msc://data-s3-iad/numpy-arrays/ndarray-1.bin")

   # Reuse the client for the data-s3-iad profile and save an array.
   msc.numpy.save(
       numpy.array([1, 2, 3, 4, 5], dtype=numpy.int32),
       "msc://data-s3-iad/numpy-arrays/ndarray-2.npz"
   )

*******
PyTorch
*******

:py:mod:`multistorageclient.torch` aliases the :py:mod:`multistorageclient.contrib.torch` module.

This module provides ``load`` and ``save`` methods for loading and saving PyTorch data.

.. code-block:: python
   :linenos:

   import multistorageclient as msc
   import torch

   # Create a client for the data-s3-iad profile and load a tensor.
   tensor = msc.torch.load("msc://data-s3-iad/pytorch-tensors/tensor-1.pt")

   # Reuse the client for the data-s3-iad profile and save a tensor.
   msc.torch.save(
       torch.tensor([1, 2, 3, 4]),
       "msc://data-s3-iad/pytorch-tensors/tensor-2.pt"
   )

In addition to the ``load`` and ``save`` methods, the ``torch`` module provides the ``MultiStorageFileSystemReader`` and ``MultiStorageFileSystemWriter`` classes for reading and writing PyTorch objects to multiple storage backends.

.. code-block:: python
   :linenos:

   import multistorageclient as msc
   import torch
   import torch.distributed.checkpoint as dcp

   # Create a MultiStorageFileSystemWriter for the data-s3-iad profile.
   writer = msc.torch.MultiStorageFileSystemWriter("msc://data-s3-iad/checkpoint/1")
   dcp.save(
       state_dict=state_dict,
       storage_writer=writer,
   )

   # Create a MultiStorageFileSystemReader for the data-s3-iad profile.
   reader = msc.torch.MultiStorageFileSystemReader("msc://data-s3-iad/checkpoint/1")
   dcp.load(
       state_dict=loaded_state_dict,
       storage_reader=reader,
   )

******
Xarray
******

:py:mod:`multistorageclient.xz` aliases the :py:mod:`multistorageclient.contrib.xarray` module.

This module provides ``open_zarr`` for reading Xarray datasets from Zarr files/objects.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a client for the data-s3-iad profile and load a Zarr array into an Xarray dataset.
   xarray_dataset = msc.xz.open_zarr("msc://data-s3-iad/abc.zarr")

Note: ``Xarray`` supports fsspec URLs natively, so you can use Xarray standard interface with ``msc://`` URLs.

.. code-block:: python
   :linenos:

   import xarray

   # Use Xarray native interface to load a Zarr array into an Xarray dataset.
   xarray_dataset = xarray.open_zarr("msc://data-s3-iad/abc.zarr")

****
Zarr
****

:py:mod:`multistorageclient.zarr` aliases the :py:mod:`multistorageclient.contrib.zarr` module.

This module provides ``open_consolidated`` for reading Zarr groups from files/objects.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a client for the data-s3-iad profile and load a Zarr array.
   z = msc.zarr.open_consolidated("msc://data-s3-iad/abc.zarr")

.. note:: ``Zarr`` supports fsspec URLs natively, so you can use Zarr standard interface with ``msc://`` URLs.

.. code-block:: python
   :linenos:

   import zarr

   # Use Zarr native interface to load a Zarr array.
   z = zarr.open("msc://data-s3-iad/abc.zarr")

****
Path
****

:py:class:`multistorageclient.Path` aliases the :py:class:`multistorageclient.pathlib.MultiStoragePath` class.

This module provides the ``Path`` class for working with paths in a way similar to ``pathlib.Path``.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a Path object for a file in the data-s3-iad profile
   path = msc.Path("msc://data-s3-iad/data/file.txt")

   # Get parent directory
   parent = path.parent  # msc://data-s3-iad/data

   # Get file name
   name = path.name  # file.txt

   # Join paths
   new_path = path.parent / "other.txt"  # msc://data-s3-iad/data/other.txt

   # Check if path exists
   exists = path.exists()

   # List contents of a directory
   for child in msc.Path("msc://data-s3-iad/data").iterdir():
       print(child)

   # Find files matching a pattern
   for matched in msc.Path("msc://data-s3-iad/data").glob("*.txt"):
       print(matched)

.. note:: The ``Path`` class implements much of the same interface as ``pathlib.Path``, making it familiar to use while working with remote storage.
