##########
Quickstart
##########

*************
Configuration
*************

The MSC can be used without any configuration to access POSIX paths. When no configuration is provided, it uses a default profile that enables basic file system operations:

.. code-block:: python

   # Using full imports
   from multistorageclient import StorageClient, StorageClientConfig

   # Create a client with default POSIX profile
   client = StorageClient(StorageClientConfig.from_file())

   # Access local files
   client.read("/path/to/file.txt")

   # Using msc shortcut
   import multistorageclient as msc

   # Access local files
   with msc.open("/path/to/file.txt", "r") as f:
       print(f.read())

For more advanced usage, you'll want to create an MSC configuration. This configuration defines profiles which define
:term:`provider` configurations for different storage backends like S3, GCS, etc.

.. note::
   When using POSIX paths, only absolute paths (starting with "/") are supported.

.. _file-based:

File-Based
==========

File-based configurations can be written in YAML or JSON format. The MSC will search for configuration files in the following locations (in order):

#. Path specified by ``MSC_CONFIG`` environment variable
#. ``${XDG_CONFIG_HOME}/msc/config.yaml`` or ``${XDG_CONFIG_HOME}/msc/config.json``
#. ``${HOME}/.msc_config.yaml`` or ``${HOME}/.msc_config.json``
#. ``${HOME}/.config/msc/config.yaml`` or ``${HOME}/.config/msc/config.json``
#. ``${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.yaml`` or ``${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.json``
#. ``/etc/msc_config.yaml`` or ``/etc/msc_config.json``

You can explicitly specify a configuration file by setting the ``MSC_CONFIG`` environment variable:

.. code-block:: bash

   export MSC_CONFIG=/path/to/my/config.yaml

.. code-block:: yaml
   :caption: YAML-based configuration.
   :linenos:

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       metadata_provider:
         type: manifest
         options:
           manifest_path: .msc_manifests

.. code-block:: json
   :caption: JSON-based configuration.
   :linenos:

   {
     "profiles": {
       "my-profile": {
         "storage_provider": {
           "type": "s3",
           "options": {
             "base_path": "my-bucket"
           }
         },
         "metadata_provider": {
           "type": "manifest",
           "options": {
             "manifest_path": ".msc_manifests"
           }
         }
       }
     }
   }

Each profile object configures the providers and options for a storage client. The schema includes provider types, options, and optional provider bundles.

See :doc:`/references/configuration` for the complete configuration schema.

Dictionary-Based
================

.. note::

   This option can only be used if you create :py:class:`multistorageclient.StorageClient` instances directly.
   See :ref:`operations` for the different ways to interact with MSC.

Dictionary-based configurations use Python dictionaries with :py:meth:`multistorageclient.StorageClientConfig.from_dict`.

The schema is the same as file-based configurations.

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   config = StorageClientConfig.from_dict(
       config_dict={
           "profiles": {
               "my-profile": {
                   "storage_provider": {
                       "type": "s3",
                       "options": {
                           "base_path": "my-bucket"
                       }
                   }
               }
           }
       }
   )

   client = StorageClient(config=config)

Rclone-Based
============

MSC also supports using an rclone configuration file as the source for MSC profiles. This is particularly useful if you already have an rclone configuration file and want to leverage the same profiles for MSC.

In an rclone configuration file, profiles are defined as INI sections, and the keys follow rclone's naming conventions. MSC will parse these files to create the corresponding provider configurations.

.. code-block:: INI
   :caption: Rclone-based configuration.
   :linenos:

   [my-profile]
   type = s3
   base_path = my-bucket
   access_key_id = my-access-key-id
   secret_key_id = my-secret-key-id
   endpoint = https://my-endpoint
   region = us-east-1

MSC checks for rclone-based configurations with the following priority:

#. The same directory as the ``rclone`` executable (if found on ``PATH``).
#. ``${XDG_CONFIG_HOME}/rclone/rclone.conf`` (if ``XDG_CONFIG_HOME`` is set).
#. ``/etc/rclone.conf``
#. ``~/.config/rclone/rclone.conf``
#. ``~/.rclone.conf``

.. note::

   MSC :ref:`file-based` configuration uses different configuration keys than rclone. For example, MSC uses ``endpoint_url`` for :py:class:`multistorageclient.StorageClient.S3StorageProvider` but rclone expects ``endpoint``. MSC aligns with rclone defaults so that if you have a rclone configuration, you can use it with MSC without any modifications on existing keys.

.. note::

   Rclone configuration primarily focus on storage access. Some MSC features such as caching and observability cannot be enabled with a rclone configuration. Therefore, MSC allows to use a rclone-based configuration for storage acceess alongside with a built-in :ref:`file-based` configuration for additional features. You can also use the built-in file-based configuration to add extra parameters to an individual profile such as ``metadata_provider``.

.. _operations:

**********************
Object/File Operations
**********************

There's 3 ways to interact with MSC:

* Shortcut functions in the :py:mod:`multistorageclient` module.
* The :py:class:`multistorageclient.StorageClient` class.
* :doc:`/user_guide/libraries`.

Shortcuts
=========

Shortcuts automatically create and manage :py:class:`multistorageclient.StorageClient` instances for you.
They only support file-based configuration.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a client for the data-s3-iad profile and open a file.
   file = msc.open(url="msc://data-s3-iad/animal-photos/giant-panda.png")

   # Reuse the client for the data-s3-iad profile and download a file.
   msc.download_file(
       url="msc://data-s3-iad/animal-photos/red-panda.png",
       local_path="/tmp/animal-photos/red-panda.png"
   )

Shortcuts use ``msc://{profile name}/{file/object path relative to the storage provider's base path}``
URLs for file/object paths.

See :py:mod:`multistorageclient` for all shortcut methods.

Clients
=======

There may be times when you want to create and manage clients by yourself for programmatic configuration or
manual lifecycle control instead of using shortcuts.

You can create :py:class:`multistorageclient.StorageClientConfig` and :py:class:`multistorageclient.StorageClient`
instances directly.

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Use a file-based configuration.
   config = StorageClientConfig.from_file(
       # Optional. See the API reference for more details.
       config_file_paths=[
           "path/to/msc_config.yaml",
           "path/to/msc_config.json"
       ],
       profile="my-profile"
   )

   # Use a dictionary-based configuration.
   config = StorageClientConfig.from_dict(
       config_dict={
           "profiles": {
               "my-profile": {
                   "storage_provider": {
                       "type": "s3",
                       "options": {
                           "base_path": "my-bucket"
                       }
                   }
               }
           }
       },
       profile="my-profile",
   )

   # Create a client
   client = StorageClient(config=config)

   # Open a file
   file = client.open("animal-photos/red-panda.png")

Clients use file/object paths relative to the storage provider's base path.

Syncing Files
=============

MSC supports concurrent copying of multiple files between different storage backends using the :py:meth:`multistorageclient.StorageClient.sync_from` method.

:py:meth:`multistorageclient.StorageClient.sync_from`
-----------------------------------------------------

Use :py:meth:`multistorageclient.StorageClient.sync_from` to copy files from one :py:class:`multistorageclient.StorageClient` to another. This is useful for duplicating data across storage backends or syncing environments (e.g., staging to production). This function uses multiple processes and threads to improve performance, which can be controlled by environment variables ``MSC_NUM_PROCESSES`` and ``MSC_NUM_THREADS_PER_PROCESS``.

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Load two clients with different profiles
   src_config = StorageClientConfig.from_file(profile="staging-data")
   dst_config = StorageClientConfig.from_file(profile="prod-data")
   src_client = StorageClient(config=src_config)
   dst_client = StorageClient(config=dst_config)

   # Sync files in images/ from the staging bucket to the prod bucket
   dst_client.sync_from(
       source_client=src_client,
       source_path="images/",
       target_path="images/",
       delete_unmatched_files=False
   )

If ``delete_unmatched_files`` is ``True``, any files in the target not found in the source will be deleted

:py:meth:`multistorageclient.sync`
----------------------------------

The :py:meth:`multistorageclient.sync` shortcut provides a quick way to sync files using MSC URLs:

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Sync data from one profile to another
   msc.sync(
       source_url="msc://staging-data/images/",
       target_url="msc://prod-data/images/",
       delete_unmatched_files=True
   )
