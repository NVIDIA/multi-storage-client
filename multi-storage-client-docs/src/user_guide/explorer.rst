#################################
Web User Interface (MSC Explorer)
#################################

The Multi-Storage Client (MSC) Explorer is a web-based user interface that provides a graphical way to browse and view files across multiple storage backends. It complements the CLI and Python SDK by offering a visual, read-only interface for users who prefer point-and-click interactions to explore their data.

.. note::

   The Web User Interface is currently an **experimental feature**. Functionality and interfaces may change in future releases.

********
Overview
********

The MSC Explorer runs locally on your machine and connects to your configured storage profiles. It provides a unified, read-only view of your data, regardless of whether it resides on AWS S3, Google Cloud Storage, Azure Blob Storage, or a local filesystem.

**Key Features:**

* **Unified File Browsing**: Navigate through directories and files across different cloud providers using a single interface.
* **File Download**: Download files from remote storage to your local machine.

************
Installation
************

The MSC Explorer is an optional component of the Multi-Storage Client. To use it, you must install the ``explorer`` extra:

.. code-block:: bash

   pip install multi-storage-client[explorer]

*********************
Starting the Explorer
*********************

Once installed, you can start the web interface using the ``msc`` CLI:

.. code-block:: bash

   # Start the explorer on the default port (8888)
   msc explorer

When the server starts, it will display the URL where the interface is accessible (defaulting to ``http://127.0.0.1:8888``). Open this URL in your web browser to access the MSC Explorer.

*****
Usage
*****

Navigation
==========

The main interface displays a file browser view. You can:

* **Select a Profile**: Use the profile selector to switch between your configured storage backends (e.g., ``s3-prod``, ``gcs-backup``, ``local-data``).
* **Browse Directories**: Click on folders to navigate into them. Use the breadcrumb bar to navigate back to parent directories.
* **Refresh**: Click the refresh button to reload the current directory listing.

File Operations
===============

The MSC Explorer provides read-only access to your storage backends with the following operations:

* **View Metadata**: See file information including size, type, and last modified date in an organized table view.
* **Download**: Download files from remote storage to your local machine.

Configuration
=============

The MSC Explorer shares the same configuration as the core Multi-Storage Client. Any profiles defined there, after being uploaded into the explorer, will be available in the web UI.

See :doc:`/references/configuration` for details on how to configure storage profiles.

.. seealso::

   * :doc:`/user_guide/cli` - MSC Command Line Interface
   * :doc:`/user_guide/quickstart` - Getting started with MSC configuration
   * :doc:`/references/configuration` - Complete configuration reference

