# Multi-Storage FUSE Daemon

The POSIX Multi-Storage Client enables easy adoption of object storage
by applications currently accessing their storage via POSIX. While the
Python variant of the Multi-Storage Client is designed to enable easy
adoption of object storage by Python applications, some appication users
prefer (or are required to) not make such modifications. For that matter,
some applications might not be able to invoke the Python variant as they
are implemented in a different language.

The tool described here utilizes FUSE to provide this POSIX access path
thus enabling easy adoption of object storage while providing a common
set of mechanisms to the Python variant.

## FUSE Daemon Configuration

There are two mechanisms for configuring the POSIX Multi-Storage Client.
As with the Python Multi-Storage Client, there is a `file-based` approach
that will search an ordered sequence of configuration file as described
[here](https://nvidia.github.io/multi-storage-client/user_guide/quickstart.html#file-based).
Alternatively, the POSIX Multi-Storage Client may be invoked with a single
argument that explicitly specifies the path to the configuration file to
be used. In either case, the configuration file may be in `YAML` or `JSON`
format (as indicated by the file's extension (i.e. `.yaml`, `.yml`, or `.json`).
The complete reference documentation for the configuration file's contents is described
[here](https://nvidia.github.io/multi-storage-client/references/configuration.html).

As may be desireable, such configuration files may prefer to reference
environment variables. Hence, a string setting may contain `$VAR` and/or
`${VAR}` references to such values whereupon evaluation of the setting
will ultimately substitute the environment variable `VAR`'s current value.

As FUSE details often require more fine grained and detailed control,
a MSFS-specific (`MSFS` being an acronym for "Multi-Storage-File-System")
configuration language is also available. This configuration mode is selected
by supplying a top-level key `msfs_version` with a supported version number
(see below).

**Environment Variable Integration:**

When using the mount helper (`mount -t msfs <config> <mountpoint>`),
the `MSFS_MOUNTPOINT` environment variable is automatically set and takes precedence over the
`mountpoint` setting in the configuration file. This allows the same configuration file to be
mounted at different locations. The `MSC_CONFIG` environment variable is similarly set with the
path to the configuration file being used.

The MSFS-specific global (i.e. "top-level") settings are described in the following table:

| Setting                                           | Units                |                  Default | Description                                                                                                                                                                                                         |
| :------------------------------------------------ | :------------------- | -----------------------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| msfs_version                                      | decimal              |                        0 | If == 0, the configuration is assumed to follow the [Multi-Storage Client specification](https://nvidia.github.io/multi-storage-client/references/configuration.html); otherwise, must == 1 & the following applies |
| mountname                                         | string               |                   "msfs" | Filesystem `name` as it would appear in e.g. `df`                                                                                                                                                                   |
| mountpoint                                        | string               | ${MSFS_MOUNTPOINT:-/mnt} | Filesystem `path` where POSIX representation will appear                                                                                                                                                            |
| fuse_workers                                      | decimal              |                        0 | Number of FUSE device file readers to keep ready; if == 0, lets the underlying FUSE library use its default: runtime.NumCPU()                                                                                       |
| fuse_fd_per_worker                                | boolean              |                    false | If true, each FUSE worker will have a unique cloned file descriptor                                                                                                                                                 |
| uid                                               | decimal              |           (current euid) | UserID of the filesystem root directory                                                                                                                                                                             |
| gid                                               | decimal              |           (current egid) | GroupID of the filesystem root directory                                                                                                                                                                            |
| dir_perm                                          | string (in octal)    |                    "555" | Permission (Mode) Bits (in 3-digit octal form) of the file system root directory                                                                                                                                    |
| allow_other                                       | boolean              |                     true | If true, Permission (Mode) Bits determine who may have access; otherwise only owner and `root` have access                                                                                                          |
| max_write                                         | decimal bytes        |           131072 (128Ki) | Maximum write size Linux VFS will send to FUSE implementatino                                                                                                                                                       |
| entry_attr_ttl                                    | decimal milliseconds |                    10000 | Amount of time Linux VFS is allowed to cache returned metadata (including potentially temporary inode numbers)                                                                                                      |
| evictable_inode_ttl                               | decimal milliseconds |                  1000000 | Amount of time an auto-generated inode will be minimally maintained (should be at least entry_attr_ttl)                                                                                                             |
| virtual_dir_ttl                                   | decimal milliseconds |                  1000000 | Amount of time a created but still empty directory should be maintained (should be at least evictable_inode_ttl)                                                                                                    |
| virtual_file_ttl                                  | decimal milliseconds |                  1000000 | Amount of time a created but still not flushed file should be maintained (should be at least evictable_inode_ttl)                                                                                                   |
| ttl_check_interval                                | decimal milliseconds |                      250 | Amount of time between checking for evictions and cache pruning                                                                                                                                                     |
| cache_storage                                     | string               |            "mapped-file" | Where each cache line is stored: "ram" (anonymous mmap; RAM only), "mapped-file" (single shared memory-mapped file; default), or "per-inode-file" (per-inode contiguous files under <cache_dir>/cachelines served via pread, with FOPEN_DIRECT_IO dropped; evicted lines reclaimed via fallocate(PUNCH_HOLE) on Linux) |
| mapped_cache                                      | boolean              |                     true | DEPRECATED — use cache_storage. true → "mapped-file", false → "ram"                                                                                                                                                 |
| cache_backend                                     | string               |                 "memory" | DEPRECATED — use cache_storage. "disk" → "per-inode-file"; "memory" → "mapped-file" or "ram" (per mapped_cache)                                                                                                      |
| cache_line_size                                   | decimal bytes        |          10485760 (10Mi) | Granularity of caching layer for both file read and write traffic                                                                                                                                                   |
| cache_lines                                       | decimal              |                      128 | Number of cache lines provisioned                                                                                                                                                                                   |
| cache_lines_to_prefetch                           | decimal              |                        4 | Maximum number of cache lines to prefetch while fetching a cache line to satisfy a read operation                                                                                                                   |
| dirty_cache_lines_flush_trigger                   | decimal              |       80% of cache_lines | If readonly false, background flushes triggered at this threshold                                                                                                                                                   |
| dirty_cache_lines_max                             | decimal              |       90% of cache_lines | If readonly false, flushes will block writes until below this threshold                                                                                                                                             |
| cache_dir_path                                    | string               |       (default temp dir) | Path to containing directory where a metadata overflow directory will be placed                                                                                                                                     |
| metadata_cache_paging_mode                        | string               |                 "pebble" | Paging mode for metadata overflow (either "file" or "pebble")                                                                                                                                                       |
| pebble_cache_size                                 | decimal              |          33554432 (32Mi) | If metadata_cache_paging_mode == "pebble", sets cache size for uncompressed blocks from SSTables                                                                                                                    |
| pebble_l0_compaction_file_threshold               | decimal              |                        4 | If metadata_cache_paging_mode == "pebble", sets the read amplification trigger point for L0 compaction                                                                                                              |
| pebble_l0_stop_writes_threshold                   | decimal              |                       12 | If metadata_cache_paging_mode == "pebble", sets the hard limit on on L0 read amplification                                                                                                                          |
| pebble_mem_table_size                             | decimal              |            8388608 (8Mi) | If metadata_cache_paging_mode == "pebble", sets the MemTable size                                                                                                                                                   |
| inode_map_keys_per_page_max                       | decimal              |                      400 | Max number of inodes per page in the B+Tree mapping inode number to the corresponding inode (must be >3 and a a multiple of 2)                                                                                      |
| inode_map_page_evict_low_limit                    | decimal              |                      100 | Paging out of inode map B+Tree pages will cease once the number of in memory pages reaches this number                                                                                                              |
| inode_map_page_evict_high_limit                   | decimal              |                      104 | Paging out of inode map B+Tree pages will begin once the number of in memory pages reaches this number                                                                                                              |
| inode_map_page_dirty_flush_trigger                | decimal              |                       50 | Flushing of the inode map B+Tree will be triggered once the number of dirty pages reaches this number                                                                                                               |
| inode_map_flushes_per_gc                          | decimal              |                       10 | If != 0, number of flushes of the inode map B+tree between each garbage collection trigger                                                                                                                          |
| inode_eviction_queue_keys_per_page_max            | decimal              |                      300 | Max number of inode eviction queue entries per page in the B+Tree indicating when inodes expire (must be >3 and a a multiple of 2)                                                                                  |
| inode_eviction_queue_page_evict_low_limit         | decimal              |                      100 | Paging out of inode eviction queue B+Tree pages will cease once the number of in memory pages reaches this number                                                                                                   |
| inode_eviction_queue_page_evict_high_limit        | decimal              |                      104 | Paging out of inode eviction queue B+Tree pages will begin once the number of in memory pages reaches this number                                                                                                   |
| inode_eviction_queue_page_dirty_flush_trigger     | decimal              |                       50 | Flushing of the inode eviction queueB+Tree will be triggered once the number of dirty pages reaches this number                                                                                                     |
| inode_eviction_queue_flushes_per_gc               | decimal              |                       10 | If != 0, number of flushes of the inode eviction queue B+Tree between each garbage collection trigger                                                                                                               |
| phys_child_dir_entry_map_keys_per_page_max        | decimal              |                      250 | Max number of physical directory entries per page in the B+Tree mapping child inode basenames to inode numbers (must be >3 and a a multiple of 2)                                                                   |
| phys_child_dir_entry_map_page_evict_low_limit     | decimal              |                      100 | Paging out of physical directory entry B+Tree pages will cease once the number of in memory pages reaches this number                                                                                               |
| phys_child_dir_entry_map_page_evict_high_limit    | decimal              |                      104 | Paging out of physical directory entry B+Tree pages will begin once the number of in memory pages reaches this number                                                                                               |
| phys_child_dir_entry_map_page_dirty_flush_trigger | decimal              |                       50 | Flushing of the physical directory entry B+Tree will be triggered once the number of dirty pages reaches this number                                                                                                |
| phys_child_dir_entry_map_flushes_per_gc           | decimal              |                       10 | If != 0, number of flushes of the physical directory entry B+Tree between each garbage collection trigger                                                                                                           |
| virt_child_dir_entry_map_keys_per_page_max        | decimal              |                      250 | Max number of virtual directory entries per page in the B+Tree mapping child inode basenames to inode numbers (must be >3 and a a multiple of 2)                                                                    |
| virt_child_dir_entry_map_page_evict_low_limit     | decimal              |                      100 | Paging out of virtual directory entry B+Tree pages will cease once the number of in memory pages reaches this number                                                                                                |
| virt_child_dir_entry_map_page_evict_high_limit    | decunak              |                      104 | Paging out of virtual directory entry B+Tree pages will begin once the number of in memory pages reaches this number                                                                                                |
| virt_child_dir_entry_map_page_dirty_flush_trigger | decimal              |                       50 | Flushing of the virtual directory entry B+Tree will be triggered once the number of dirty pages reaches this number                                                                                                 |
| virt_child_dir_entry_map_flushes_per_gc           | decimal              |                       10 | If != 0, number of flushes of the virtual directory entry B+Tree between each garbage collection trigger                                                                                                            |
| process_memory_limit                              | decimal bytes        |         4294967296 (4Gi) | If != 0, sets the limit on the amount of memory for the entire process (including cache lines and the evict high limits on metadata pages)                                                                          |
| auto_sighup_interval                              | decimal seconds      |                        0 | If != 0, schedules SIGHUP processing                                                                                                                                                                                |
| endpoint                                          | string               |                       "" | If != "", enables a RESTful service endpoint (including the "http:// or "https://" scheme though "https://" is not currently supported)                                                                             |
| backends                                          | array                |                          | An array of each object store backend to be presented as a pseudo-directory underneath the `mountpoint1                                                                                                             |

As noted in the above table, the `backends` setting defines an array of object
store backends to be presented as pseudo-directories underneath the `mountpoint`.
While existing `backends` may not be modified, they can be removed and/or others
added. Changes to the configuration file will be read if a SIGHUP is received.
It is also possible to configure a periodic check for changes to the configuration
file as well. In any event, each `backend` is described in an array element of
the `backends` array as described by settings in the following table:

| Setting                         | Units                | Default             | Description                                                                                                              |
| :------------------------------ | :------------------- | ------------------: | :----------------------------------------------------------------------------------------------------------------------- |
| dir_name                        | string               |                     | Name of the pseudo-direcory underneath `mountpoint` where this backend's files will appear                               |
| readonly                        | boolean              |                true | If true, the entire pseudo-directory for this backend will be read only                                                  |
| flush_on_close                  | boolean              |                true | If true, last close of a modified file will trigger a synchronous flush                                                  |
| uid                             | decimal              |      (current euid) | UserID of this backend's top-level directory and every element underneath it                                             |
| gid                             | decimal              |      (current egid) | GroupID of this backend's top-level directory and every element underneath it                                            |
| dir_perm                        | string (in octal)    | "555"(ro)/"777"(rw) | Permission (Mode) Bits (in 3-digit octal form) of this backend's top-level directory and all directories below it        |
| file_perm                       | string (in octal)    | "444"(ro)/"666"(rw) | Permission (Mode) Bits (in 3-digit octal form) of files underneath this backend's top level directory                    |
| directory_page_size             | decimal              |                   0 | Maximum number of directory elements fetched at a time; if == 0, object store endpoint default is used                   |
| multipart_cache_line_threshold  | decimal              |                 512 | Files that fit in this many cache lines will be uploaded in a single PUT; otherwise, Multi-Part Upload will be performed |
| upload_part_cache_lines         | decimal              |                  32 | Consecutive cache lines that make up each Multi-Part Upload `part`                                                       |
| upload_part_concurrency         | decimal              |                  32 | Number of Multi-Part Uploads simultaneously employed for a single file                                                   |
| bucket_container_name           | string               |                     | Name of `bucket` (a.k.a. `container`) to present via POSIX                                                               |
| prefix                          | string               |                  "" | Subdirectory inside `bucket_container_name` to narrow what to present via POSIX; if !="", should end with "/"            |
| trace_level                     | decimal              |                   0 | If == 0, no tracing; if >= 1, errors traced; if >= 2, successes traced; if > 2, success details traced                   |
| backend_type                    | string               |                     | One of the supported object store backends (i.e. `AIStore`, `GCS`, `PSEUDO`, `RAM`, or `S3`)                             |
| <backend_type_specific>         | (sub-field section)  |         (see below) | A section containing `backend-type`-specific settings                                                                    |

Note that precisely one section (specific content appropriate for the
specified `backup_type`) must be present. The following sub-sections
describe the `backup_type`-specific settings.

### AIStore Backend

If `backend_type` is specificd as "AIStore", a sub-section of the `backend`
configuration (whose name is `AIStore`) may be provided. The AIStore-specific
settings must be provided (or the defaults accepted) as described in
the following table:

| Setting                     | Units                | Default                                                 | Description                                                            |
| :-------------------------- | :------------------- | ------------------------------------------------------: | :--------------------------------------------------------------------- |
| endpoint                    | string               |                                       "${AIS_ENDPOINT}" | AIStore Endpoint (including the "http:// or "https://" scheme)         |
| skip_tls_certificate_verify | boolean              |                                                   false | If true & using HTTPS (TLS), TLS Certificate Verification skipped      |
| authnToken                  | string               |                                    "${AIS_AUTHN_TOKEN}" | If != "", specifies AUTHN Token                                        |
| authnTokenFile              | string               | "${AIS_AUTHN_TOKEN_FILE:=~/.config/ais/cli/auth.token}" | If != "", specifies location of AUTHN Token file                       |
| provider                    | string               |                                                    "s3" | IF != "ais", specifies the backend of which bucket contents are cached |
| timeout                     | decimal milliseconds |                                                   30000 | Limit on allowed duration of requests (including retries)              |
| manifest_gen_backend        | string               |                                                      "" | IF != "", `dir_name` of another (non-AIStore) backend used for LIST/STAT-DIR (manifest generation, readdir); object reads still go via AIS. Lets listing hit the underlying store (e.g. S3) directly while reads benefit from AIS caching. The referenced backend should target the same bucket/prefix; a mismatch is logged as a warning (not rejected). |

### GCS Backend Configuration

If `backend_type` is specified as "GCS", a sub-section of the `backend`
configuration (whose name is `GCS`) must be provided. The GCS-specific
settings must be provided (or the defaults accepted) as described in
the following table:

| Setting                      | Units                | Default | Description                                                                         |
| :--------------------------- | :------------------- | ------: | :---------------------------------------------------------------------------------- |
| api_key                      |                      |      "" | If empty, no authentication is performed                                            |
| endpoint                     | string               |      "" | GCS Endpoint (including the "http://", "grpc://", "https://", or "grpcs://" scheme) |
| skip_tls_certificate_verify  | boolean              |   false | If true & using HTTPS/GRPCS, TLS Certificate Verification skipped                   |
| retry_base_delay             | decimal milliseconds |      10 | Delay between failure response and first retry                                      |
| retry_next_delay_multiplier  | float                |     2.0 | Must be >= 1.0; used to compute delay between prior failure and next retry          |
| retry_max_delay              | decimal milliseconds |    2000 | Stops retries if next delay would exceed this limit                                 |

### PSEUDO Backend Configuration

If `backend_type` is specified as "PSEUDO", a sub-section of the `backend`
configuration (whose name is `PSEUDO`) may be provided if any non-defaults
are are needed. The PSEUDO-specific settings must be provided (or the
defaults accepted) as described in the following table:

| Setting                    | Units                | Default      | Description                                                                                                   |
| :------------------------- | :------------------- | -----------: | :------------------------------------------------------------------------------------------------------------ |
| dir_name_format            | string               |   "dir_%08X" | Format specifier for the naming pattern of subdirectories (must lexigraphically sort before file_name_format) |
| file_name_format           | string               |  "file_%08X" | Format specifier for the naming pattern of files (must lexigraphically sort after dir_name_format)            |
| dir_starting_number        | decimal              |            0 | Number in dir_name_format of the first subdirectory                                                           |
| file_starting_number       | decimal              |            0 | Number in file_name_format of the first file                                                                  |
| file_size                  | decimal              |            0 | Size (in bytes) of each file (content all zeroes)                                                             |
| files_at_depth_0           | decimal              |            0 | Number of files at depth 0 (i.e. in top-most directory)                                                       |
| files_at_depth_1           | decimal              |            0 | Number of files at depth 1 (subdirectories_at_depth_0 must be >0)                                             |
| files_at_depth_2           | decimal              |            0 | Number of files at depth 2 (subdirectories_at_depth_{0\|1} must be >0)                                        |
| files_at_depth_3           | decimal              |            0 | Number of files at depth 3 (subdirectories_at_depth_{0\|1\|2} must be >0)                                     |
| max_list_page_size         | decimal              |         1000 | Cap on the number of List{Directory\|Objects} returned subdirectories+files or objects                        |
| min_latency_delete_file    | decimal_milliseconds |            0 | Minimum latency for a call to .deleteFile                                                                     |
| min_latency_list_directory | decimal_milliseconds |            0 | Minimum latency for a call to .listDirectory                                                                  |
| min_latency_list_objects   | decimal_milliseconds |            0 | Minimum latency for a call to .listObjects call                                                               |
| min_latency_read_file      | decimal_milliseconds |            0 | Minimum latency for a call to .readFile call                                                                  |
| min_latency_stat_directory | decimal_milliseconds |            0 | Minimum latency for a call to .statDirectory call                                                             |
| min_latency_stat_file      | decimal_milliseconds |            0 | Minimum latency for a call to .statFile call                                                                  |
| subdirectories_at_depth_0  | decimal              |            0 | Number of subdirectories at depth 0 (i.e. in top-most directory)                                              |
| subdirectories_at_depth_1  | decimal              |            0 | Number of subdirectories at depth 1 (subdirectories_at_depth_0 must be >0)                                    |
| subdirectories_at_depth_2  | decimal              |            0 | Number of subdirectories at depth 2 (subdirectories_at_depth_{0\|1} must be >0)                               |

### RAM Backend Configuration

If `backend_type` is specified as "RAM", a sub-section of the `backend`
configuration (whose name is `RAM`) may be provided if any non-defaults
are are needed. The RAM-specific settings must be provided (or the
defaults accepted) as described in the following table:

| Setting                | Units   | Default         | Description                                                                            |
| :--------------------- | :------ | --------------: | :------------------------------------------------------------------------------------- |
| max_list_page_size     | decimal |            1000 | Cap on the number of List{Directory\|Objects} returned subdirectories+files or objects |
| max_total_objects      | decimal |           10000 | Cap on the number of objects to support                                                |
| max_total_object_space | decimal | 1073741824(1Gi) | Cap on the sum of all the object sizes to support                                      |

### S3 Backend Configuration

If `backend_type` is specified as "S3", a sub-section of the `backend`
configuration (whose name is `S3`) must be provided. The S3-specific
settings must be provided (or the defaults accepted) as described in
the following table:

| Setting                      | Units                | Default                                                     | Description                                                                                       |
| :--------------------------- | :------------------- | ----------------------------------------------------------: | :------------------------------------------------------------------------------------------------ |
| config_credentials_profile   | string               |                                   "${AWS_PROFILE:-default}" | If use_{config\|credentials}_env == true, optionally specifies {config\|credentials} file profile |
| use_config_env               | boolean              |                                                       false | If true, use config file instead of access_key_id and secret_access_key                           |
| config_file_path             | string               |                  "${AWS_CONFIG_FILE:-\${HOME}/.aws/config}" | If use_config_env == true, optionally specifies location of config file                           |
| region                       | string               |                                  "${AWS_REGION:-us-east-1}" | S3 Region                                                                                         |
| endpoint                     | string               |                                           "${AWS_ENDPOINT}" | S3 Endpoint (including the "http://" or "https://" scheme)                                        |
| use_credentials_env          | boolean              |                                                       false | If true, use credentials file instead of access_key_id and secret_access_key                      |
| credentials_file_path        | string               | "${AWS_SHARED_CREDENTIALS_FILE:-\${HOME}/.aws/credentials}" | If use_credentials_env == true, optionally specifies location of credentials file                 |
| access_key_id                | string               |                                      "${AWS_ACCESS_KEY_ID}" | If use_credentials_env == false, specifies S3 Access Key                                          |
| secret_access_key            | string               |                                  "${AWS_SECRET_ACCESS_KEY}" | If use_credentials_env == false, specifies S3 Secret Key                                          |
| skip_tls_certificate_verify  | boolean              |                                                       false | If true & using HTTPS (TLS), TLS Certificate Verification skipped                                 |
| virtual_hosted_style_request | boolean              |                                                       false | If false, uses "path style" URLs                                                                  |
| unsigned_payload             | boolean              |                                                       false | If true, skips the "signing" of payloads                                                          |
| retry_base_delay             | decimal milliseconds |                                                          10 | If == 0, retry is disabled ; delay between failure response and first retry                       |
| retry_next_delay_multiplier  | float                |                                                         2.0 | Must be >= 1.0; used to compute delay between prior failure and next retry                        |
| retry_max_delay              | decimal milliseconds |                                                        2000 | Stops retries if next delay would exceed this limit                                               |

### Configuration Example

Here is an eample (taken from `./msfs_config_dev.yaml`) YAML-formatted configuration file:
```
msfs_version: 1
backends: [
  {
    dir_name: minio,
    bucket_container_name: dev,
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
  {
    dir_name: ais,
    bucket_container_name: dev,
    backend_type: S3,
    S3: {
      use_config_env: true,
      use_credentials_env: true,
    },
  },
]
```

Notice the following:
* The internal configuration format is selected by setting `msfs_version` to `1`
* The `mountpoint` is not specified, so will be the non-empty value of MSFS_MOUNTPOINT ENV or simply `/mnt`
* There are two backends: `minio` and `ais`
  * These will appear as subdirectories under the mountpoint (e.g. `/mnt/minio` and `/mnt/ais`)
  * Each maps to an S3 bucket named `dev`
  * The `minio` backend explicitly specifies the `region` and `endpoint`:
    * The `region` is set to `us-east-1`
    * The `endpoint` is set to `http://minio:9000`
  * The   minio  backend explicitly specifies the   access_key_id` and `secret_access_key`:
    * The `access_key_id` is set to `minioadmin` (the default MinIO `AWS_ACCESS_KEY`)
    * The `secret_access_key` is set to `minioadmin` (the default MinIO `AWS_SECRET_ACCESS_KEY`)
  * The `ais` backend enables fetching the `region` and `endpoint` values from the environment:
    * This mode is triggered by setting `use_config_env` to `true`
    * The values for `region` and `endpoint` are fetched from the `${HOME}/.aws/config` file
    * The location of the `config` file could have been adjusted by setting AWS_CONFIG_FILE ENV
    * Since `config_credentials_profile` was not specified, those values come from the `[default]` profile
  * The `ais` backend enables fetching the `access_key_id` and `secret_access_key` values from the environment:
    * This mode is triggered by setting `use_credentials_env` to `true`
    * The values for `access_key_id` and `secret_access_key` are fetched from the `${HOME}/.aws/credentials` file
    * The location of the `credentials` file could have been adjusted by setting AWS_SHARED_CREDENTIALS_FILE ENV
    * Since `config_credentials_profile` was not specified, those values come from the `[default]` profile
* All other settings utilized the various defaults specified above

## Deployment Model

MSFS is not a centralized storage or caching service. It is a FUSE process that
runs on each compute node, started either directly (`mount -t msfs`) or by the
CSI node plugin on behalf of a pod. Nothing needs to be provisioned outside the
compute node, so MSFS is deployable without dedicated storage appliances, but
the consequence is that **every MSFS process owns an independent cache**. Two
mounts on the same node, or the same mount on two nodes, share nothing.

| Capability | Status |
| :--------- | :----- |
| POSIX read access to S3, AIStore, and GCS backends | Supported |
| Multiple buckets or bucket+prefixes as sibling subdirectories of one mount | Supported |
| Adding and removing backends without unmounting (SIGHUP) | Supported |
| Manifest-based bootstrap for large namespaces | Supported |
| Node-local read cache with read-ahead, capacity bound, and LRU eviction | Supported |
| Cache backed by RAM, a shared mapped file, or per-inode files | Supported |
| OpenTelemetry metrics and a Prometheus endpoint | Supported |
| Kubernetes deployment via the CSI node plugin | Supported |
| Cache shared or coordinated across mounts or nodes | Not implemented |
| Cache surviving unmount | Not implemented |
| Explicit data pre-warm API | Not implemented |
| Per-user (UID/GID) authorization within a mount | Not implemented |
| Modifying an existing backend in place via SIGHUP | Not implemented |

Access is granted per mount, not per user: whoever can read the mount point can
read everything the backend credentials can reach. Deployments needing tenant
isolation must separate tenants by mount.

## Caching and Pre-warming

The cache is line-based. `cache_line_size` (default 10 MiB) is the fetch and
residency granularity, and `cache_lines` (default 128) is how many lines are
provisioned, so **the default capacity is about 1.25 GiB**. The read benchmarks
below used `cache_lines: 10000`, or roughly 100 GiB. Capacity is a hard bound
and eviction is LRU, so a dataset larger than the cache holds only its active
working set; for multi-PiB datasets, sizing follows the working set rather than
the dataset.

`cache_storage` selects where lines live: `ram` (anonymous mmap), `mapped-file`
(one shared memory-mapped file, the default), or `per-inode-file` (per-inode
contiguous files served with `pread`). All three are node-local.

### Cache lifetime

Each mount creates its own private cache directory under `cache_dir_path` and
removes it on unmount. The cache does not survive the mount, and a remount
starts cold. Files left behind by a crash are not discovered or reused.

Pointing `cache_dir_path` at a shared filesystem such as Lustre places those
private directories on shared storage but does not produce a shared cache. The
catalog that makes cached bytes findable — the object-to-line index, cache-line
state and ETags, in-flight fetch tracking, LRU order, and capacity accounting —
lives in the memory of a single MSFS process. Two MSFS processes over the same
Lustre path therefore still use different directories, fetch the same object
twice, cannot see each other's entries, and cannot coordinate fills,
invalidation, or eviction. Lustre guarantees consistency for shared files; it
does not supply object-cache semantics such as key/version lookup, single-flight
fetches, or ETag invalidation.

### Pre-warming

There is no pre-warm API. Reading files warms the cache of the MSFS process that
served the reads, so a CPU job can warm a mount that a later job reuses, but
only if that MSFS process is still running and the working set still fits in
cache. A pre-warm job that unmounts, or a later job that creates its own mount,
starts cold.

Manifest generation is a separate mechanism and warms only namespace metadata.
It makes directory traversal and attribute lookups fast without per-object S3
calls; it does not fetch file contents.

## Measured Scale and Performance

These are single-node measurements against same-region storage. They describe
what has been measured, not a supported configuration limit; see
[Qualified scale boundary](#qualified-scale-boundary).

### Namespace scale: 100M objects

EC2 `c5a.12xlarge` (48 vCPU, 96 GiB), `us-west-2`, S3 bucket in the same region.
Dataset: 100,237,498 objects across 101,339 directories.

| Phase | Elapsed | Throughput | Peak RSS |
| :---- | ------: | ---------: | -------: |
| Manifest generation (parallel BFS listing, 200 workers) | 1m 45s | 954,680 obj/s | |
| Manifest ingest (per-directory TSV into sharded B+Tree/PebbleDB) | 16m 41s | 100,129 obj/s | ~7.4 GiB |
| Total bootstrap | ~18m 26s | | |

The mount is **browsable when generation finishes**, at about 105 seconds, not
when ingest finishes. During ingest, metadata is served from the manifest while
the optimized index is built in the background, so traversal and enumeration —
enough to compute dataset splits and begin streaming — work well before the
18m 26s mark. Generation held at ~1m 43s and ingest at ~16m 50s across repeated
runs.

Set `process_memory_limit` generously for a run this size. The 4 GiB default sits
below the working set of a 100M-object ingest, which drives continuous garbage
collection and collapses throughput.

These figures assume a hierarchical layout. Both phases degrade sharply when a
single directory holds the whole namespace, because generation finds one key
prefix to split across ~20 range workers instead of ~200 directory workers, and
every directory entry lands in one B+Tree shard:

| 100M objects | Generation | Ingest | Peak RSS |
| :----------- | ---------: | -----: | -------: |
| 101,339 directories | 1m 43s (977K obj/s) | 16m 29s (101K obj/s) | ~6.5 GiB |
| 1 directory | 30m 35s (~55K obj/s) | 44m 19s (37.8K obj/s) | ~21 GiB |

The penalty is super-linear in directory width: 10M objects in one directory
generate in 1m 41s and ingest in 2m 5s, so the same flat shape is far cheaper an
order of magnitude smaller.

### Read throughput: 24-cell matrix

EC2 `c5n.18xlarge` (72 vCPU, 184 GiB), `us-west-2`, same-region S3, ~100 GiB
cache. Dataset ~88 GiB: 8,192 x 1 MiB plus 80 x 1 GiB. Six workload families
(4 KiB and 64 KiB request sizes; small-file and large-file; sequential and
random) at 1, 2, 4, and 8 application threads, each with a cold and a
cache-resident pass, driven by `elbencho -r --direct`. Compared against
s3fs-fuse 1.93 with a local disk cache.

Result: **18 wins, 3 ties, 3 losses** across the 24 cells.

| Workload family | Cache-resident MSFS vs s3fs |
| :-------------- | :-------------------------- |
| Small files, 4 KiB sequential | 20x - 52x |
| Small files, 64 KiB sequential | 6.5x - 27x |
| Large files, 4 KiB sequential | 0.69x - 1.04x |
| Large files, 64 KiB sequential | 0.45x - 1.05x |
| Large files, 4 KiB random | 3.9x - 6.9x |
| Large files, 64 KiB random | 5.4x - 14x |

Cold reads scale with thread count because each reader issues concurrent ranged
GETs: large-file 64 KiB sequential moves 92 MiB/s at one thread and 192 MiB/s at
eight. s3fs cold is flat near 125 MiB/s on the same families. Cache-resident
reads reach 4,536 MiB/s on that family at eight threads. At eight threads MSFS
wins or ties every family.

Two caveats keep the losses honest. The large-file 64 KiB losses at one and two
threads are mostly a page-cache artifact: on a 184 GiB host, s3fs serves its
warm reads from the Linux page cache over its own cache files, and after
`drop_caches` it falls to about 148 MiB/s on the same data. The large-file 4 KiB
results are a real per-operation FUSE ceiling — with one or two threads there
are only one or two FUSE operations in flight, so neither extra readers nor
cache geometry help. `--direct` forces strict 4 KiB operations with no kernel
read-ahead, which is a deliberately pessimistic operating point; workloads that
do not use `O_DIRECT` get page-cache assistance and reach roughly 2.6 GiB/s on
the same data.

### Reproducing these numbers

Defaults are not benchmark settings. The values below are what the results above
were measured with; a run that differs on any of them is not comparable.

| Setting | Value | Why |
| :------ | :---- | :-- |
| `fuse_fd_per_worker` | `false` | Shared `/dev/fuse` descriptor. Cloned per-worker descriptors measured 18-25% slower. |
| `fuse_workers` | `50` | On a 72-vCPU host. The default of `0` uses `runtime.NumCPU()`, which is too many readers on large hosts. |
| `cache_line_size` | `10485760` | 10 MiB fetch and residency granularity. |
| `cache_lines` | sized to the working set | `10000` held the whole 88 GiB dataset. |
| `cache_lines_to_prefetch` | `4` | Read-ahead depth. Higher values can help backends with a different latency knee. |
| `process_memory_limit` | `68719476736` | The 4 GiB default causes sustained garbage collection on large ingests. |
| `GOMAXPROCS` | unset on a 72-vCPU host | Left unset so Go uses every vCPU. On a 256-vCPU host, pinning it to 72 matched the EC2 result; leaving it at 32 throttled the read and GC path. |

Two settings live outside the configuration file and must be re-applied after
every mount, because the FUSE connection id changes:

```bash
ulimit -n 131072

sudo sh -c 'for d in /sys/fs/fuse/connections/*/; do
    echo 144 > "${d}max_background"
    echo 108 > "${d}congestion_threshold"
done'
```

The kernel defaults, `max_background=12` and `congestion_threshold=9`, throttle
in-flight background and read-ahead requests and cap read concurrency well below
what the thread count suggests. Leaving them at their defaults is the most common
reason a run appears to stop scaling after a few threads.

### Qualified scale boundary

What the results above establish, and what they do not:

| Dimension | Measured | Not established |
| :-------- | :------- | :-------------- |
| Clients | 1 MSFS process per test | Many concurrent clients against one backend, including request fan-out and cache-hit behavior under aggregate load |
| Application threads | 1 - 8 | Higher concurrency per mount |
| Objects | ~100M in one namespace | Substantially larger namespaces |
| Working set | ~88 GiB, cache-resident | Multi-PiB datasets, where the cache holds a small fraction of the data |
| Latency | Mean throughput per cell | Tail-latency targets |
| Failure handling | Clean runs | Backend failure and recovery under load |

The companion run in which the dataset deliberately exceeded cache capacity, so
that reads had to keep returning to the backend, stopped after 18 of 24 cases and
was never completed. Sustained-eviction behavior is therefore not characterized.

## Authentication

Direct mounts take credentials from the configuration file, or from the standard
AWS configuration and credentials files via `use_config_env` and
`use_credentials_env`. Environment variable references such as
`${AWS_ACCESS_KEY_ID}` keep literal secrets out of the configuration file.

Under Kubernetes, the CSI node plugin additionally supports a static Secret
referenced by `nodePublishSecretRef`, a driver-level workload identity (IRSA on
EKS), and a per-workload role assumed from `volumeAttributes.roleArn`. See
[csi/deploy/README.md](csi/deploy/README.md).

Credential rotation and multi-tenant isolation have not been qualified
end-to-end. Since authorization is per mount rather than per user, a mount
exposes everything its credentials can reach to every reader of the mount point.

## Docker Development Environment

To facillitate a common developer and testing experience, a Docker Container
environment is provided via a `Dockerfile`. As it is also useful to utilize
a controlled environment for holding the objects to be presented via POSIX,
a `docker-compose.yaml` is also provided that launches a Docker Container
running a Minio S3 object server (`minio`) along with the Development (`dev`)
Docker Container.

A typical development sequence is depicted in the following:

| Host Commands                    | `dev` Container Commands                                        | Description                                                                                                 |
| :------------------------------- | :-------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------- |
| $ docker pull minio/minio:latest |                                                                 | Ensures the latest version of `minio` Docker Container Image is used (optional)                             |
| $ docker-compose build           |                                                                 | Builds the `dev` Docker Container Image (optionally append `--no-cache` to ensure it is built from scratch) |
| $ docker-compose up -d dev       |                                                                 | Launches both the `minio` and the `dev` Docker Containers                                                   |
| $ docker-compose exec dev bash   |                                                                 | Enters a `bash` shell inside the `dev` Docker Container                                                     |
|                                  | # ./dev_setup.sh {\|ais\|aisMinio\|garage\|gcs\|minio\|versity} | Creates and populates a `dev` bucket/container, populated with the source tree (defaults to `minio`)        |
|                                  | # make                                                          | Builds (if necessary) the FUSE program                                                                      |
|                                  | # ./msfs &                                                      | Runs the FUSE program in the background configured by what's in ${MSC_CONFIG} (`./msfs_config_dev.yaml`)    |
|                                  | ^M                                                              | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # mount | grep fuse                                             | Shows that the `dev` bucket is mounted via FUSE at `/mnt`                                                   |
|                                  | # df -h /mnt                                                    | Shows the "stats" for the FUSE-mounted filesystem                                                           |
|                                  | # ls -ailR /mnt                                                 | Recursively lists the files (backed by the "dev" bucket objects) via POSIX                                  |
|                                  | # kill -SIGHUP \`pidof ./msfs\`                                 | Sends a SIGHUP to the FUSE program telling it to re-parse the configuration file (here `dev.json`)          |
|                                  | ^M                                                              | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # kill -SIGINT \`pidof ./msfs\`                                 | Sends a SIGINT to the FUSE program telling it to cleanly exit                                               |
|                                  | ^M                                                              | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # exit                                                          | Exits the `bash` shell running inside the `dev` Docker Container                                            |
| $ docker-compose down            |                                                                 | Terminates the `minio` and `dev` Docker Containers                                                          |

## Mount Helpers

After installation (`sudo make install`), use standard Unix `mount` and `umount` commands:

### Mounting

```bash
# Mount MSFS filesystem with config file and mountpoint
mount -t msfs /path/to/config.yaml /mnt/msfs1

# Mount multiple instances with different configs or mountpoints
mount -t msfs /path/to/config1.yaml /mnt/msfs1
mount -t msfs /path/to/config2.json /mnt/msfs2
```

### Unmounting

```bash
# Unmount specific mountpoint
umount /mnt/msfs1

# Unmount another mountpoint
umount /mnt/msfs2
```

### How It Works

The `mount` command uses a standard Unix convention: when you specify `-t <type>`, it looks for a helper script at `/usr/sbin/mount.<type>`. For MSFS:

- `mount -t msfs <config> <mountpoint>` → automatically calls `/usr/sbin/mount.msfs`
- The mount helper sets environment variables and launches the `msfs` daemon

**Important: Standard `mount` Command Behavior**

The `mount` command behaves differently depending on the arguments provided:

- **`mount`** (no args) → Lists all currently mounted filesystems
- **`mount -t msfs`** (type only) → Lists all currently mounted MSFS filesystems (does NOT call `mount.msfs`)
- **`mount -t msfs <config> <mountpoint>`** → Calls `/usr/sbin/mount.msfs` to perform the mount

The mount helper (`mount.msfs`) is **only invoked when you provide both the config file and mountpoint**. This is standard Unix `mount` behavior, not a limitation. The helper validates that both arguments are provided before attempting to launch `msfs`

This is the same mechanism used by other filesystems like NFS (`mount.nfs`), CIFS (`mount.cifs`), and FUSE (`mount.fuse`).

**Mount Helper (`mount.msfs`):**
- Exports `MSC_CONFIG` environment variable from the config file argument
- Exports `MSFS_MOUNTPOINT` environment variable from the mountpoint argument
- Creates log directory if needed (`/var/log/msfs/`)
- Launches `msfs` daemon in the background using `setsid` for proper process management
- Stores process ID and mountpoint in `/var/log/msfs/msfs_*.pid` for tracking
- Returns once the daemon is running

**Environment Variables:**
- `MSC_CONFIG`: Path to the configuration file (set by mount command)
- `MSFS_MOUNTPOINT`: Mount point path (set by mount command, overrides config file)
- `MSFS_BINARY`: Path to msfs binary (default: `/usr/local/bin/msfs`)
- `MSFS_LOG_DIR`: Log directory (default: `/var/log/msfs`)

**Unmount Helper (`umount.msfs`):**
- Finds all running msfs processes
- Terminates each process with SIGTERM (waits up to 10 seconds)
- If still running, sends SIGKILL
- Handles zombie processes gracefully (accepts as success)
- Cleans up all PID files in `/var/log/msfs/`
- Note: Unmounts **all** MSFS filesystems, regardless of how many are mounted

### Environment Variables

- **`MSC_CONFIG`**: Path to MSFS configuration file (YAML or JSON)
  - Automatically set by mount helper from the first argument to `mount -t msfs`
  - Passed to the `msfs` binary for configuration loading
- **`MSFS_MOUNTPOINT`**: Mount point path
  - Automatically set by mount helper from the second argument to `mount -t msfs`
  - Overrides the `mountpoint` setting in the configuration file
- **`MSFS_BINARY`**: Path to msfs binary (default: `/usr/local/bin/msfs`)
- **`MSFS_LOG_DIR`**: Directory for logs and PID files (default: `/var/log/msfs`)

### Automatic Mounting with /etc/fstab

MSFS filesystems can be automatically mounted at boot time by adding entries to `/etc/fstab`:

```fstab
# MSFS filesystem with S3 backend
/etc/msfs/s3-config.yaml  /mnt/s3-data  msfs  defaults,_netdev  0  0

# MSFS filesystem with local config
/home/user/msfs.json      /mnt/storage  msfs  defaults,noauto   0  0
```

**fstab field explanation:**
- **Field 1**: Path to MSFS configuration file (YAML or JSON)
- **Field 2**: Mount point directory
- **Field 3**: Filesystem type (`msfs`)
- **Field 4**: Mount options (comma-separated)
  - `defaults`: Standard mount options
  - `_netdev`: Wait for network before mounting (for remote storage)
  - `noauto`: Don't mount automatically at boot (mount manually with `mount /mnt/storage`)
  - `user`: Allow non-root users to mount (requires `allow_other` in config)
- **Field 5**: Dump frequency (usually `0`)
- **Field 6**: fsck pass number (usually `0`)

After editing `/etc/fstab`, test the configuration with:
```bash
sudo mount -a  # Mount all filesystems in fstab
```

### Configuration

The mountpoint is defined in the configuration file's `mountpoint` setting (default: `/mnt`). The filesystem name displayed in `df` and `mount` output is controlled by the `mountname` setting (default: `msfs`).

### Publication

Inside the `dev` container, one may type the following to produce `.deb` and `.rpm`
packages for both AMD64 and ARM64 architectures:

```
make deb-packages rpm-packages
```

To actually create .tar.gz and .zip assets:

```
make assets
```

Those assets include `msfs_install.sh` and `msfs_uninstall.sh` scripts automating the process for installing and uninstalling the appropriate package for the platform.

## Use-Case Suggestions

### Fronting a remote bucket with a fast tier (AIStore)

MSFS caching is node-local and bounded (see
[Caching and Pre-warming](#caching-and-pre-warming)), so the cost of a first
touch is paid per node and again after any remount. Where that cost dominates —
many nodes reading the same dataset once, or working sets too large to stay
cache-resident — a shared cache tier in front of the remote bucket can absorb it.
MSFS does not implement such a tier, but it can read one as a backend.

We measured this with AIStore. The 24-cell read matrix was re-run with MSFS
pointed at an AIStore cluster (3 proxies, 3 targets) fronting the same S3 bucket,
with the 88 GiB dataset prefetched into the cluster, from a 256 vCPU / 1.5 TiB
client:

- **First-touch reads were 2.2x to 37x faster** than MSFS reading S3 directly,
  peaking near 1.7 GiB/s. The gain was largest on small files (23x - 37x at
  64 KiB, 8.7x - 19x at 4 KiB) and smallest on large files (2.2x - 6.9x),
  because a first touch lands on in-datacenter targets instead of crossing the
  network to the remote bucket.
- **Cache-resident reads were unchanged**, which is the expected result:
  `backend_read_file_successes_total` stayed flat across the cache-resident pass
  on all 24 cells, so those reads never reached any backend. Cache-resident
  throughput is a property of the MSFS cache and the host, not of what sits
  behind it. A fast tier improves the first touch, not the cached steady state.
- The 100M-object namespace on the same cluster generated in 1m 27.7s and
  ingested in about 3m 19s, but only with listing delegated to the underlying
  store through `manifest_gen_backend`. Listing the fronted bucket through
  AIStore timed out at this scale, while listing the store directly did not.
  Object reads still went through AIStore.

The practical reading: a fast tier is worth evaluating when first-touch cost
dominates, and `manifest_gen_backend` should point at whichever backend lists
the namespace fastest, which is not necessarily the one serving reads. This was
one client at 1-8 threads; it does not establish behavior for many concurrent
clients (see [Qualified scale boundary](#qualified-scale-boundary)).
