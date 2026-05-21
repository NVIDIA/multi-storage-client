# Multi-Storage File System (Go)

Go-based FUSE filesystem that exposes object storage as a POSIX mount. See the root `AGENTS.md` for overall architecture and cross-language sync points.

## Structure

```text
main.go              Entry point — signal handling, config reload loop (SIGHUP)
fs.go                FUSE filesystem operations (read, write, readdir, lookup, etc.)
globals.go           Global state, configuration, and initialization
globals_lock.go      Lock helpers for global state
config.go            Configuration parsing (YAML/JSON, env var substitution)
backend.go           Storage backend interface
backend_s3.go        AWS S3 backend
backend_gcs.go       GCS backend
backend_aistore.go   AIStore backend
backend_pseudo.go    Pseudo backend (testing)
backend_ram.go       RAM backend (testing)
bptree.go            B+ tree for manifest/metadata indexing (with paging)
cache.go             Local file cache (cache-line based read/write)
fission.go           Block-level deduplication
llrb.go              Left-leaning red-black tree
http.go              HTTP utilities
metrics.go           Prometheus/OpenTelemetry metrics
utils.go             Shared helpers
mount.msfs           Mount helper script (called by `mount -t msfs`)
Dockerfile.csi       Multi-stage Docker build for the CSI driver image (builds both msfs and msfs-csi-driver)
csi/                 Kubernetes CSI driver — mounts S3 buckets as POSIX volumes inside pods (separate Go module; see csi/README.md)
telemetry/           OpenTelemetry setup, auth, attributes, periodic metrics
tools/lockgen/       Code generator for typed lock wrappers
packaging/           Debian and RPM packaging specs
*_test.go            Tests (co-located with source)
```

## Configuration

MSFS supports two config modes:

1. **MSC-compatible** (`msfs_version` absent or `0`) — uses the standard [MSC config format](https://nvidia.github.io/multi-storage-client/references/configuration.html). Sample: `msc_config_sample.yaml`.
2. **MSFS-native** (`msfs_version: 1`) — extended format with fine-grained FUSE tuning (cache lines, B+ tree paging, memory limits). Sample: `msfs_config_dev.yaml`.

Config files support `$VAR` / `${VAR}` environment variable substitution. See `config.go` for the full schema and defaults.

## Running / Mounting

**Direct (dev):**
```bash
cd multi-storage-file-system && just compile
MSC_CONFIG=./msfs_config_dev.yaml ./build/generic/amd64-linux/bin/msfs
```

**Mount helper (after `sudo make install`):**
```bash
mount -t msfs /path/to/config.yaml /mnt/msfs1
umount /mnt/msfs1
```

**Docker dev environment:**
```bash
docker-compose build
docker-compose up -d dev
docker-compose exec dev bash
# Inside container:
./dev_setup.sh minio   # creates and populates a dev bucket
make                   # builds the FUSE binary
./msfs &               # mounts at /mnt
```

**Key environment variables:**
- `MSC_CONFIG` — path to config file
- `MSFS_MOUNTPOINT` — overrides `mountpoint` in config
- `MSFS_BINARY` — path to msfs binary (default: `/usr/local/bin/msfs`)

## Commands

| Task | Command |
|---|---|
| Run tests | `cd multi-storage-file-system && go test ./...` |
| Lint & format | `cd multi-storage-file-system && just analyze` |
| Build binaries | `cd multi-storage-file-system && just compile` |
| Full release build | `just multi-storage-file-system/build` |
| Build Debian/RPM packages | `cd multi-storage-file-system && just package` |
| Build CSI driver (Go only) | `cd multi-storage-file-system/csi && go build ./...` |
| Build CSI driver image | `cd multi-storage-file-system && docker build --platform linux/amd64 -f Dockerfile.csi -t <your-registry>/msfs-csi:latest .` |
| Deploy CSI driver | See `multi-storage-file-system/csi/deploy/commands-runbook.sh` |

## Testing

- Tests are co-located with source as `*_test.go` files.
- Use Go's standard `testing` package.
- Run with `go test ./...` from this directory.

## Tooling

- **Build:** `go build` (cross-compiled for amd64/arm64 linux)
- **Linter:** golangci-lint
- **Formatter:** `go fmt`
- **Module management:** `go mod tidy`

## Conventions

- MSFS version tracks the MSC version (pulled from `multi-storage-client/pyproject.toml` via uv).
- Config is YAML/JSON (see `config.go` for schema and defaults).
- Storage backends implement the interface in `backend.go`.
- SIGHUP triggers config reload (add/remove backends without restart).
- `auto_sighup_interval` in config enables periodic config reload.

## Code Style

- Follow standard Go conventions (`go fmt`, `golangci-lint`).
- Error handling: return `error` values, do not panic in library code.
- New backends: implement `backendContextIf` from `backend.go`, add a `backend_<name>.go` file.
- Config parsing: add new fields to `config.go` with `parseXxx()` helpers and defaults in `globals.go`.

## Boundaries

- **Always:** run `go test ./...` and `just analyze` before submitting changes.
- **Ask first:** changes to the B+ tree paging logic (`bptree.go`), FUSE ops (`fs.go`), or cache eviction (`cache.go`) — these are performance-critical and subtle.
- **Never:** modify `packaging/` without verifying both Debian and RPM builds; change config defaults without updating `README.md`.
