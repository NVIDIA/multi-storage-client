# AIStore Local Cluster

Self-contained setup for running a local 2-node AIStore cluster (1 proxy + 1 target) for testing.

## Files

- `prepare_sandbox.sh` - Generates AIStore configuration files (adapted from [AIStore v1.4.0](https://github.com/NVIDIA/aistore/blob/v1.4.0/deploy/dev/local/aisnode_config.sh))
- `sandbox/` - Runtime directory (logs, configs, data)

## Key Differences from Upstream

- **Self-contained**: No external dependencies (`utils.sh`, environment variables)
- **Fixed ports**: 51080 (proxy) and 51081 (target) for test compatibility
- **Multi-node**: Generates both proxy and target configs in one run
- **Isolated paths**: All data under `PROJECT_ROOT/.aistore/sandbox/`
