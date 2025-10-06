# AIStore

AIStore configurations. See [AIStore's local configuration script](https://github.com/NVIDIA/aistore/blob/v1.3.31/deploy/dev/local/aisnode_config.sh) for more information.

Differences between `prepare_sandbox.sh` and the script:

- `ais.json`
    - `net.http.idle_conns_per_host` + `net.http.idle_conns` set to 0 (for macOS).
- `ais_local.json`
    - `host_net.port` set to 51080 (like the AIStore Docker container).
    - Directories are under `sandbox`.
