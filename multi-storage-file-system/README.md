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

| Setting                         | Units                |                  Default | Description                                                                                                                                                                                                         |
| :------------------------------ | :------------------- | -----------------------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| msfs_version                    | decimal              |                        0 | If == 0, the configuration is assumed to follow the [Multi-Storage Client specification](https://nvidia.github.io/multi-storage-client/references/configuration.html); otherwise, must == 1 & the following applies |
| mountname                       | string               |                   "msfs" | Filesystem `name` as it would appear in e.g. `df`                                                                                                                                                                   |
| mountpoint                      | string               | ${MSFS_MOUNTPOINT:-/mnt} | Filesystem `path` where POSIX representation will appear                                                                                                                                                            |
| uid                             | decimal              |           (current euid) | UserID of the filesystem root directory                                                                                                                                                                             |
| gid                             | decimal              |           (current egid) | GroupID of the filesystem root directory                                                                                                                                                                            |
| dir_perm                        | string (in octal)    |                    "555" | Permission (Mode) Bits (in 3-digit octal form) of the file system root directory                                                                                                                                    |
| allow_other                     | boolean              |                     true | If true, Permission (Mode) Bits determine who may have access; otherwise only owner and `root` have access                                                                                                          |
| max_write                       | decimal bytes        |           131072 (128Ki) | Maximum write size Linux VFS will send to FUSE implementatino                                                                                                                                                       |
| entry_attr_ttl                  | decimal milliseconds |                    10000 | Amount of time Linux VFS is allowed to cache returned metadata (including potentially temporary inode numbers)                                                                                                      |
| evictable_inode_ttl             | decimal milliseconds |                  1000000 | Amount of time an auto-generated inode will be minimally maintained (should be at least entry_attr_ttl)                                                                                                             |
| cache_line_size                 | decimal bytes        |            1048576 (1Mi) | Granularity of caching layer for both file read and write traffic                                                                                                                                                   |
| cache_lines                     | decimal              |                     4096 | Number of cache lines provisioned                                                                                                                                                                                   |
| dirty_cache_lines_flush_trigger | decimal              |       80% of cache_lines | If readonly false, background flushes triggered at this threshold                                                                                                                                                   |
| dirty_cache_lines_max           | decimal              |       90% of cache_lines | If readonly false, flushes will block writes until below this threshold                                                                                                                                             |
| auto_sighup_interval            | decimal seconds      |                        0 | If != 0, schedules SIGHUP processing                                                                                                                                                                                |
| endpoint                        | string               |                       "" | If != "", enables a RESTful service endpoint (including the "http:// or "https://" scheme though "https://" is not currently supported)                                                                             |
| backends                        | array                |                          | An array of each object store backend to be presented as a pseudo-directory underneath the `mountpoint1                                                                                                             |

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
| backend_type                    | string               |                     | One of the supported object store backends (i.e. `AIStore`, `RAM`, or `S3`                                               |
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
| skip_tls_certificate_verify | boolean              |                                                    true | If true & using HTTPS (TLS), TLS Certificate Verification skipped      |
| authnToken                  | string               |                                    "${AIS_AUTHN_TOKEN}" | If != "", specifies AUTHN Token                                        |
| authnTokenFile              | string               | "${AIS_AUTHN_TOKEN_FILE:=~/.config/ais/cli/auth.token}" | If != "", specifies location of AUTHN Token file                       |
| provider                    | string               |                                                    "s3" | IF != "ais", specifies the backend of which bucket contents are cached |
| timeout                     | decimal milliseconds |                                                   30000 | Limit on allowed duration of requests (including retries)              |

### RAM Backend Configuration

If `backend_type` is specified as "RAM", a sub-section of the `backend`
configuration (whose name is `RAM`) may be provided if any non-defaults
are are needed. The RAM-specific settings must be provided (or the
defaults accepted) as described in the following table:

| Setting                 | Units   | Default         | Description                                                          |
| :---------------------- | :------ | --------------: | :------------------------------------------------------------------- |
| max_total_objects       | decimal |           10000 | Cap on the number of objets to support                               |
| max_total_object_space  | decimal | 1073741824(1Gi) | Cap on the sum of all the object sizes to support                    |
| max_directory_page_size | decimal |             100 | Cap on the number of ListDirectory returned subdirectories and files |

### S3 Backend Configuration

If `backend_type` is specified as "S3", a sub-section of the `backend`
configuration (whose name is `S3`) must be provided. The S3-specific
settings must be provided (or the defaults accepted) as described in
the following table:

| Setting                      | Units                | Default                                                     | Description                                                                                       |
| :--------------------------- | :------------------- | ----------------------------------------------------------: | :------------------------------------------------------------------------------------------------ |
| config_credentials_profile   | string               |                                   "${AWS_PROFILE:-default}" | If use_{config\|credentials}_env == true, optionally specifies {config\|credentials} file profile |
| use_config_env               | boolean              |                                                       false | If true, use cconfig file instead of access_key_id and secret_access_key                          |
| config_file_path             | string               |                  "${AWS_CONFIG_FILE:-\${HOME}/.aws/config}" | If use_config_env == true, optionally specifies location of config file                           |
| region                       | string               |                                  "${AWS_REGION:-us-east-1}" | S3 Region                                                                                         |
| endpoint                     | string               |                                           "${AWS_ENDPOINT}" | S3 Endpoint (including the "http://" or "https://" scheme)                                        |
| use_credentials_env          | boolean              |                                                       false | If true, use credentials file instead of access_key_id and secret_access_key                      |
| credentials_file_path        | string               | "${AWS_SHARED_CREDENTIALS_FILE:-\${HOME}/.aws/credentials}" | If use_credentials_env == true, optionally specifies location of credentials file                 |
| access_key_id                | string               |                                      "${AWS_ACCESS_KEY_ID}" | If use_credentials_env == false, specifies S3 Access Key                                          |
| secret_access_key            | string               |                                  "${AWS_SECRET_ACCESS_KEY}" | If use_credentials_env == false, specifies S3 Secret Key                                          |
| skip_tls_certificate_verify  | boolean              |                                                        true | If true & using HTTPS (TLS), TLS Certificate Verification skipped                                 |
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
|                                  | # ./dev_setup.sh {ais\|aisMinio\|minio}                                        | Creates and populates a `dev` bucket/container, populated with the source tree, in `ais` and/or `minio`     |
|                                  | # make                                                                         | Builds (if necessary) the FUSE program                                                                      |
|                                  | # ./msfs &                                                                     | Runs the FUSE program in the background configured by what's in ${MSC_CONFIG} (`./msfs_config_dev.yaml`)    |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # mount | grep fuse                                                            | Shows that the `dev` bucket is mounted via FUSE at `/mnt`                                                   |
|                                  | # df -h /mnt                                                                   | Shows the "stats" for the FUSE-mounted filesystem                                                           |
|                                  | # ls -ailR /mnt                                                                | Recursively lists the files (backed by the "dev" bucket objects) via POSIX                                  |
|                                  | # kill -SIGHUP \`pidof ./msfs\`                                                | Sends a SIGHUP to the FUSE program telling it to re-parse the configuration file (here `dev.json`)          |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # kill -SIGINT \`pidof ./msfs\`                                                | Sends a SIGINT to the FUSE program telling it to cleanly exit                                               |
|                                  | ^M                                                                             | Hitting `ENTER` will get us a `#` prompt                                                                    |
|                                  | # exit                                                                         | Exits the `bash` shell running inside the `dev` Docker Container                                            |
| $ docker-compose down            |                                                                                | Terminates the `minio` and `dev` Docker Containers                                                          |

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
