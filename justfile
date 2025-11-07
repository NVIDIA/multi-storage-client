#
# Just configuration.
#
# https://just.systems/man/en
#

# Default to the first Python binary on `PATH`.
python-binary := "python"

# List recipes.
help:
    just --list

# Build nix.
nix:
    just nix/build

# Build multi-storage-explorer.
multi-storage-explorer:
    just multi-storage-explorer/build

# Build multi-storage-client.
multi-storage-client: multi-storage-explorer
    just python-binary={{python-binary}} multi-storage-client/build

# Build multi-storage-client-docs.
multi-storage-client-docs: multi-storage-client
    just python-binary={{python-binary}} multi-storage-client-docs/build

# Build multi-storage-client-scripts.
multi-storage-client-scripts:
    just python-binary={{python-binary}} multi-storage-client-scripts/build

# Build multi-storage-file-system.
multi-storage-file-system:
    just multi-storage-file-system/build

# Release build.
build: nix multi-storage-explorer multi-storage-client multi-storage-client-docs multi-storage-client-scripts multi-storage-file-system
