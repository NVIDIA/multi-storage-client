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

# Build multi-storage-client.
multi-storage-client:
    just python-binary={{python-binary}} multi-storage-client/build

# Build multi-storage-client-docs.
multi-storage-client-docs: multi-storage-client
    just python-binary={{python-binary}} multi-storage-client-docs/build

# Release build.
build: multi-storage-client multi-storage-client-docs
