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
be used. In either case, the configuration file may be in `.yaml` or `.json`
format. The complete reference documentation for the configuration file's
contents is described
[here](https://nvidia.github.io/multi-storage-client/references/configuration.html).

As may be desireable, such configuration files may prefer to reference
environment variables. Hence, a string setting may contain `$VAR` and/or
`${VAR}` references to such values whereupon evaluation of the setting
will ultimately substitute the environment variable `VAR`'s current value.

As FUSE details often require more fine grained and detailed control,
a MSCP-specific (`MSCP` being an acronym for "Multi-Storage Client POSIX")
configuration language is also available. This configuration mode is selected
by supplying a top-level key `mscp_version` with a supported version number
(see below).

The MSCP-specific global (i.e. "top-level") settings are described in the following table:

| Setting                         | Units                | Default             | Description                                                                                                                                                                                                         |
| :------------------------------ | :------------------- | ------------------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| mscp_version                    | decimal              |                   0 | If == 0, the configuration is assumed to follow the [Multi-Storage Client specification](https://nvidia.github.io/multi-storage-client/references/configuration.html); otherwise, must == 1 & the following applies |
| mountname                       | string               |         "msc-posix" | Filesystem `name` as it would appear in e.g. `df`                                                                                                                                                                   |
| mountpoint                      | string               |              "/mnt" | Filesystem `path` were POSIX representation will appear                                                                                                                                                             |
| uid                             | decimal              |      (current euid) | UserID of the filesystem root directory                                                                                                                                                                             |
| gid                             | decimal              |      (current egid) | GroupID of the filesystem root directory                                                                                                                                                                            |
| dir_perm                        | string (in octal)    |               "555" | Permission (Mode) Bits (in 3-digit octal form) of the file system root directory                                                                                                                                    |
| allow_other                     | boolean              |                true | If true, Permission (Mode) Bits determine who may have access; otherwise only owner and `root` have access                                                                                                          |
| max_write                       | decimal bytes        |      131072 (128Ki) | Maximum write size Linux VFS will send to FUSE implementatino                                                                                                                                                       |
| entry_attr_ttl                  | decimal milliseconds |               10000 | Amount of time Linux VFS is allowed to cache returned metadata (including potentially temporary inode numbers)                                                                                                      |
| evictable_inode_ttl             | decimal milliseconds |             1000000 | Amount of time an auto-generated inode will be minimally maintained (should be at least entry_attr_ttl)                                                                                                             |
| cache_line_size                 | decimal bytes        |       1048576 (1Mi) | Granularity of caching layer for both file read and write traffic                                                                                                                                                   |
| cache_lines                     | decimal              |                4096 | Number of cache lines provisioned                                                                                                                                                                                   |
| dirty_cache_lines_flush_trigger | decimal              |  80% of cache_lines | If readonly false, background flushes triggered at this threshold                                                                                                                                                   |
| dirty_cache_lines_max           | decimal              |  90% of cache_lines | If readonly false, flushes will block writes until below this threshold                                                                                                                                             |
| auto_sighup_interval            | decimal seconds      |                   0 | If != 0, schedules SIGHUP processing                                                                                                                                                                                |
| backends                        | array                |                     | An array of each object store backend to be presented as a pseudo-directory underneath the `mountpoint1                                                                                                             |

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
| backend_type                    | string               |                     | One of the supported object store backends (i.e. `Azure`, `GCP`, 'OCI`, or `S3` though only `S3` is currently supported) |
| <backend_type_specific>         | (sub-field section)  |         (see below) | A section containing `backend-type`-specific settings                                                                    |

Note that precisely one section (specific content appropriate for the
specified `backup_type`) must be present. The following sub-sections
describe the `backup_type`-specific settings.

### S3 Backend Configuration

If `backend_type` is specified as "S3", a sub-section of the `backend`
configuration (whose name is `S3`) must be provided. The S3-specific
settings must beprovided (or the defaults accepted) as described in
the following table:

| Setting                      | Units                | Default     | Description                                                                 |
| :--------------------------- | :------------------- | ----------: | :-------------------------------------------------------------------------- |
| access_key_id                | string               |             | S3 Access Key                                                               |
| secret_access_key            | string               |             | S3 Secret Key                                                               |
| region                       | string               |             | S3 Region                                                                   |
| endpoint                     | string               |             | S3 Endpoint                                                                 |
| allow_http                   | boolean              |       false | If false, requires HTTPS (TLS)                                              |
| skip_tls_certificate_verify  | boolean              |        true | If true & using HTTPS (TLS), TLS Certificate Verification skipped           |
| virtual_hosted_style_request | boolean              |       false | If false, uses "path style" URLs                                            |
| unsigned_payload             | boolean              |       false | If true, skips the "signing" of payloads                                    |
| retry_base_delay             | decimal milliseconds |          10 | If == 0, retry is disabled ; delay between failure response and first retry |
| retry_next_delay_multiplier  | float                |         2.0 | Must be >= 1.0; used to compute delay between prior failure and next retry  |
| retry_max_delay              | decimal milliseconds |        2000 | Stops retries if next delay would exceed this limit                         |

## Docker Development Environment

To facillitate a common developer and testing experience, a Docker Container
environment is provided via a `Dockerfile`. As it is also useful to utilize
a controlled environment for holding the objects to be presented via POSIX,
a `docker-compose.yaml` is also provided that launches a Docker Container
running a Minio S3 object server (`minio`) along with the Development (`dev`)
Docker Container.

A typical development sequence is depicted in the following:

| Host Commands                    | `dev` Container Commands                                                       | Description                                                                                                 |
| :------------------------------- | :----------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------- |
| $ docker pull minio/minio:latest |                                                                                | Ensures the latest version of `minio` Docker Container Image is used (optional)                             |
| $ docker-compose build           |                                                                                | Builds the `dev` Docker Container Image (optionally append `--no-cache` to ensure it is built from scratch) |
| $ docker-compose up -d dev       |                                                                                | Launches both the `minio` and the `dev` Docker Containers                                                   |
| $ docker-compose exec dev bash   |                                                                                | Enters a `bash` shell inside the `dev` Docker Container                                                     |
|                                  | # ./dev_setup.sh {ais\|minio}                                                  | Creates and populates a `dev` bucket/container, populated with the source tree, in either `ais` or `minio`  |
|                                  | # make                                                                         | Builds (if necessary) the FUSE program                                                                      |
|                                  | # ./mscp &                                                                     | Runs the FUSE program in the background configured by what's in ${MSC_CONFIG} (`./mscp_config_dev.yaml`)    |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # mount | grep fuse                                                            | Shows that the `dev` bucket is mounted via FUSE at `/mnt`                                                   |
|                                  | # df -h /mnt                                                                   | Shows the "stats" for the FUSE-mounted filesystem                                                           |
|                                  | # ls -ailR /mnt                                                                | Recursively lists the files (backed by the "dev" bucket objects) via POSIX                                  |
|                                  | # kill -SIGHUP \`pidof ./mscp\`                                                | Sends a SIGHUP to the FUSE program telling it to re-parse the configuration file (here `dev.json`)          |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # kill -SIGINT \`pidof ./mscp\`                                                | Sends a SIGINT to the FUSE program telling it to cleanly exit                                               |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # exit                                                                         | Exits the `bash` shell running inside the `dev` Docker Container                                            |
| $ docker-compose down            |                                                                                | Terminates the `minio` and `dev` Docker Containers                                                          |

## Deployment Aids

A mechanism for distributing the binary executable version of mscp is via a Docker Container
Image is described here. These steps have been automated by Makefile rule `publish` with the
resultant extracted binaries named for their target OS and CPU.

### Creation of a Scratch-based Docker Container Image having only /mscp

`(cd ../../.. && docker build --file posix/fuse/mscp/Dockerfile --target built --tag mscp_built:$(git describe --tags --always --dirty) .)`

### Extraction of Linuxon-AMD64 mscp from the Scratch-based Docker Container Image

`docker create --name mscp_built mscp_built:<tag> --entrypoint /mscp && docker cp mscp_built:/msc-posix-linux-amd64 ./mscp && docker rm mscp_built`

### Extraction of Linuxon-ARM64 mscp from the Scratch-based Docker Container Image

`docker create --name mscp_built mscp_built:<tag> --entrypoint /mscp && docker cp mscp_built:/msc-posix-linux-arm64 ./mscp && docker rm mscp_built`
