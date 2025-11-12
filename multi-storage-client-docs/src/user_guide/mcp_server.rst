###################################
Model Context Protocol (MCP) Server
###################################

The Multi-Storage Client MCP Server enables conversational interaction with MSC through AI assistants and LLM clients using natural language, removing the need to memorize API methods or CLI commands.

********
Overview
********

Model Context Protocol (MCP) is an open protocol that standardizes how applications provide context to Large Language Models (LLMs). The MSC MCP Server implements this protocol, allowing AI assistants like Cursor, Claude Desktop, and others to interact with your storage backends through conversational interfaces.

**Key Benefits:**

* **Natural Language Interface**: Ask "Show me all files under prefix /project1/part2/" instead of learning the ``list()`` method parameters or ``msc ls`` command syntax

* **Pattern Matching**: Request "Find all .log files from last week" instead of constructing complex glob patterns

* **Simplified Operations**: Say "Copy all model checkpoints from local to S3" instead of writing sync scripts

* **Interactive Workflows**: Manage storage with conversational confirmation and guidance

The MCP Server leverages MSC's existing authentication and configuration system, so your credentials and storage profiles work seamlessly without additional setup.

************
Requirements
************

* **Python 3.10 or higher** - MCP Server uses features only available in Python 3.10+
* **Multi-Storage Client** with the ``mcp`` extra and storage provider extras for each backend you intend to use
* **An MCP-compatible client** such as Cursor, Claude Desktop, or other MCP clients

See the :doc:`/user_guide/installation` guide for complete installation instructions.

.. important::

   The ``msc`` CLI must be accessible from your shell's PATH. If ``msc mcp-server --help`` doesn't work:
   
   * **Verify msc is in PATH**: Run ``which msc`` (macOS/Linux) or ``where msc`` (Windows)
   * **macOS/Homebrew**: Ensure ``/opt/homebrew/bin`` or your virtualenv's ``bin`` directory is in PATH
   * **Windows**: Ensure your Python installation's ``Scripts`` directory is in PATH
   * **Virtual environment**: Activate your Python virtual environment if you installed MSC in one

********************
Starting the Server
********************

The MCP Server runs locally on your machine and is started automatically by your MCP client (e.g., Cursor) when configured. See :ref:`cursor-configuration` for setup instructions.

You can also start the server manually for testing or debugging:

.. code-block:: bash

   # Start with default configuration discovery
   msc mcp-server start

   # Start with a specific configuration file
   msc mcp-server start --config /path/to/config.yaml

   # Start with verbose logging for debugging
   msc mcp-server start --config /path/to/config.yaml --verbose

.. _cursor-configuration:

*************
Configuration
*************

Understanding stdio Communication
==================================

The MCP Server communicates with AI assistants using the **stdio protocol** (standard input/output), which sends and receives JSON-RPC 2.0 messages over stdout and stdin. **Any output to stdout will interfere with the MCP protocol and cause communication failures.**

MSC progress bars are written to stderr (not stdout), so no special configuration is needed for MSC's own output. However, if you use optional extras (e.g., ``boto3``, ``google-cloud-storage``, ``azure-storage-blob``, ``huggingface``), some third-party dependencies may write to stdout. You are responsible for configuring these dependencies to suppress stdout output through environment variables (e.g., ``HF_HUB_DISABLE_PROGRESS_BARS`` for HuggingFace Hub).

Cursor Configuration
====================

To use the MSC MCP Server with Cursor, add the following configuration to your Cursor settings file:

* **macOS/Linux**: ``~/.cursor/mcp.json``
* **Windows**: ``%APPDATA%\Cursor\mcp.json``
* **Or via Cursor UI**: Settings → Features → Model Context Protocol

.. code-block:: json
   :caption: Basic Cursor configuration for MSC MCP Server.
   :linenos:

   {
     "mcpServers": {
       "Multi-Storage Client": {
         "command": "msc",
         "args": [
           "mcp-server",
           "start"
         ]
       }
     }
   }

.. code-block:: json
   :caption: Advanced Cursor configuration with custom config file.
   :linenos:

   {
     "mcpServers": {
       "Multi-Storage Client": {
         "command": "msc",
         "args": [
           "mcp-server",
           "start",
           "--config", "/path/to/msc_config.yaml"
         ]
       }
     }
   }

**Configuration Parameters:**

* ``command``: The ``msc`` CLI command (must be installed and accessible in your PATH)

* ``args``: Arguments to pass to the MCP server

  * ``mcp-server start``: Command to start the MCP server
  * ``--config``: Optional path to MSC configuration file

* ``env``: Optional environment variables (e.g., to suppress stdout output from third-party dependencies)

.. note::

   If you don't specify a ``--config`` path, MSC will use its standard configuration discovery mechanism, checking locations like ``~/.msc_config.yaml``, ``MSC_CONFIG`` environment variable, etc. See :doc:`/user_guide/quickstart` for details.

Other MCP Clients
=================

The MSC MCP Server follows the standard MCP protocol and can be used with any MCP-compatible client. Configuration will be similar to the Cursor example above, though the exact format may vary by client. Refer to your MCP client's documentation for specific configuration instructions.

MSC Configuration
=================

The MCP Server uses your existing MSC configuration file to determine which storage profiles and backends are available. No special MCP-specific configuration is needed in your MSC config.

.. code-block:: yaml
   :caption: Example MSC configuration that works with MCP Server.
   :linenos:

   profiles:
     my-s3-bucket:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1

     my-gcs-bucket:
       storage_provider:
         type: gcs
         options:
           base_path: my-gcs-bucket

     local-storage:
       storage_provider:
         type: file
         options:
           base_path: /mnt/data

***************
Available Tools
***************

The MCP Server provides the following tools for storage operations. Your AI assistant will automatically use these tools when you make requests in natural language.

msc_list
========

Lists files and directories from a storage location.

**Parameters:**

* ``url`` (required): Storage URL to list (e.g., ``msc://profile/path/``, ``s3://bucket/prefix/``)

  URLs can use either ``msc://profile/path/`` format (using profiles from your MSC config) or provider-native URLs like ``s3://bucket/prefix/``. A trailing slash typically indicates a directory/prefix.

* ``start_after``: Start listing after this key (exclusive, for pagination)

* ``end_at``: End listing at this key (inclusive, for pagination)

  Listings are lexicographically ordered by key. ``start_after`` and ``end_at`` apply to this ordering.

* ``include_directories``: Whether to include directories in results (default: false)

* ``attribute_filter_expression``: Filter results by attributes

* ``show_attributes``: Include attributes in the response (default: false)

* ``limit``: Maximum number of objects to return

**Example prompts:**

* "List all files in my **production-s3** profile under the data/ prefix"

* "Show me the first 10 files in **my-profile** under datasets/"

* "List directories in my **backup-storage** profile"

msc_info
========

Retrieves detailed metadata about a specific file or directory.

**Parameters:**

* ``url`` (required): Full path to the object (e.g., ``msc://profile/path/file.txt``)

**Example prompts:**

* "Get information about model.pkl in my **training-data** profile"

* "What's the size of dataset.csv in **my-s3-bucket**?"

* "When was model.pkl in the **production** profile last modified?"

msc_upload_file
===============

Uploads a file from the local filesystem to storage.

**Parameters:**

* ``url`` (required): Destination URL where file will be stored

* ``local_path`` (required): Local path of the file to upload

* ``attributes``: Optional metadata attributes to attach to the file

**Example prompts:**

* "Upload my local model.pkl to the **production-s3** profile"

* "Copy /tmp/data.csv to datasets/ in my **backup-bucket** profile"

msc_download_file
=================

Downloads a file from storage to the local filesystem.

**Parameters:**

* ``url`` (required): URL of the file to download

* ``local_path`` (required): Local path where file should be saved

**Example prompts:**

* "Download checkpoint.pkl from my **training** profile to /tmp/"

* "Get model.pkl from the **production-s3** profile and save it to /tmp/"

msc_delete
==========

Deletes files or directories from storage.

**Parameters:**

* ``url`` (required): URL of the object to delete

* ``recursive``: Whether to delete directories recursively (default: false)

**Example prompts:**

* "Delete old_checkpoint.pkl from my **training** profile"

* "Remove all files in the temp/ directory in **staging-storage**"

.. warning::

   Deletion operations are permanent. The AI assistant will typically ask for confirmation before executing delete operations.

msc_copy
========

Copies a file within the same storage profile.

**Parameters:**

* ``source_url`` (required): Source file URL

* ``target_url`` (required): Target file URL

**Example prompts:**

* "Copy model.pkl to model_backup.pkl in my **production** profile"

* "Duplicate checkpoint.pkl within the **training-data** profile"

.. note::

   ``msc_copy`` is designed for copying files **within the same storage profile** (e.g., copying ``msc://my-s3-bucket/file.txt`` to ``msc://my-s3-bucket/file_backup.txt``). 
   
   To copy files **between different profiles** (e.g., from ``local-storage`` to ``production-s3``), use ``msc_sync`` or download then upload.

msc_sync
========

Synchronizes files between different storage locations or profiles.

**Parameters:**

* ``source_url`` (required): Source storage URL

* ``target_url`` (required): Target storage URL  

* ``delete_unmatched_files``: Delete files at target not present at source (default: false)

* ``preserve_source_attributes``: Preserve source file metadata (default: false)

**Example prompts:**

* "Sync data/ from my **local-storage** profile to **production-s3**"

* "Copy all model checkpoints from **my-gcs** profile to **local-storage**"

* "Mirror datasets/ from **primary-bucket** to **backup-bucket** profile"

msc_sync_replicas
=================

Synchronizes files from primary storage to configured replica storage locations. See :doc:`/user_guide/replicas` for more information about replica configuration.

**Parameters:**

* ``source_url`` (required): Source storage URL

* ``replica_indices``: Optional list of specific replica indices to sync (0-based)

* ``delete_unmatched_files``: Delete files at replicas not present at source (default: false)

**Example prompts:**

* "Sync datasets/ from **primary-storage** profile to all configured replicas"

* "Mirror data/ from **production** to replica 0 only"

* "Update replicas with latest files from the **main-bucket** profile"

msc_is_file
===========

Checks whether a URL points to a file rather than a directory.

**Parameters:**

* ``url`` (required): URL to check

**Example prompts:**

* "Is data/model.pkl in my **training** profile a file or directory?"

* "Check if model.pkl exists as a file in the **production-s3** profile"

msc_is_empty
============

Checks whether a storage location contains any objects.

**Parameters:**

* ``url`` (required): URL to check

**Example prompts:**

* "Is the temp/ directory empty in my **staging** profile?"

* "Check if the **backup-bucket** profile has any files"

*****************
Available Prompts
*****************

The MCP Server provides a single prompt to help AI assistants understand MSC functionality and guide users effectively.

msc_help
========

Provides comprehensive help information about MSC configuration, usage, and available MCP tools. This prompt gives your AI assistant context on:

* Supported storage backends (S3, GCS, Azure, local filesystem, etc.)

* Available MCP tools and their parameters

* Configuration file locations and formats

* URL formats for accessing storage

* General usage guidance

**How it works:**

The ``msc_help`` tool provides context to your AI assistant automatically—you don't need to invoke it directly. When you ask general questions about MSC, the AI assistant uses this information to provide accurate answers.

For example, you can ask your assistant:

* "How do I use MSC with the MCP Server?"

* "What can I do with Multi-Storage Client?"

* "Help me understand MSC configuration"

* "What MCP tools are available?"

The AI will automatically consult the ``msc_help`` context to formulate its response.

********
Examples
********

Conversational File Operations
===============================

**Instead of writing:**

.. code-block:: python

   from multistorageclient import StorageClient, StorageClientConfig
   
   client = StorageClient(StorageClientConfig.from_file(profile="my-s3"))
   for obj in client.list(path="datasets/images/", include_directories=False):
       print(obj.name, obj.size)

**You can simply ask:**

   "Show me all files in my **my-s3** profile under datasets/images/"

Or:

   "List files in the datasets/images folder using the **my-s3-bucket** profile"

Pattern Matching and Filtering
===============================

**Instead of using glob patterns:**

.. code-block:: python

   import multistorageclient as msc
   
   for file in msc.glob("msc://my-profile/logs/*.log"):
       if file.last_modified > some_date:
           print(file)

**You can simply ask:**

   "Find all .log files from last week in my **my-profile** logs folder"

Or:

   "Show me .log files modified in the last 7 days in the **production-storage** profile"

Bulk Operations
===============

**Instead of writing sync scripts:**

.. code-block:: python

   from multistorageclient import StorageClient, StorageClientConfig
   
   source = StorageClient(StorageClientConfig.from_file(profile="local"))
   target = StorageClient(StorageClientConfig.from_file(profile="s3"))
   
   target.sync_from(source, "checkpoints/", "checkpoints/")

**You can simply ask:**

   "Sync all model checkpoints from my **local-storage** profile to **my-s3-bucket** profile"

Or:

   "Copy checkpoints/ from **local** to **production-s3**"

Interactive Management
=======================

**Instead of carefully constructing delete commands:**

.. code-block:: bash

   msc rm msc://my-bucket/temp/ --recursive

**You can simply ask:**

   "Delete files in my **my-bucket** profile temp directory older than 30 days"

Or:

   "Remove old files from temp/ in **staging-storage**"

The AI assistant can help you identify the right files, confirm the operation, and execute it safely.

.. seealso::

   * :doc:`/user_guide/quickstart` - MSC configuration basics

   * :doc:`/user_guide/replicas` - Configure replicas for use with MCP Server

   * :doc:`/user_guide/rust` - Enable Rust client for better MCP Server performance

   * :doc:`/references/configuration` - Complete configuration reference

   * `Model Context Protocol <https://modelcontextprotocol.io/>`_ - Official MCP documentation

