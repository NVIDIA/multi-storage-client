# Security Policy: Multi-Storage Client

## Reporting a Vulnerability

If you discover a potential security vulnerability, please **do not open a public issue, discussion, or pull request**.

- **Web (preferred):** [NVIDIA Vulnerability Disclosure Program](https://www.nvidia.com/en-us/security/)
- **E-mail:** [psirt@nvidia.com](mailto:psirt@nvidia.com)
  - For secure communication, use the [NVIDIA public PGP key](https://www.nvidia.com/en-us/security/pgp-key).
- **GitHub:** Use this repository's **Security** tab and select **Report a vulnerability**.

Please include:

- The affected project version, branch, or commit
- The affected component and vulnerability type
- Reproduction steps
- Proof-of-concept code, if available
- The expected and observed behavior
- An assessment of potential impact

Detailed reports help NVIDIA evaluate and address issues faster. NVIDIA's Product Security Incident Response Team will acknowledge the report, validate its severity, coordinate remediation, and publish a security bulletin when appropriate.

## Security Architecture & Context

Multi-Storage Client is an open-source storage access toolkit consisting of:

- A Python library and command-line interface that provide a unified API for object and file storage
- A Rust extension, exposed through PyO3, for performance-sensitive storage operations
- A Go FUSE filesystem and Kubernetes CSI integration that expose object storage through POSIX interfaces
- A local web explorer and an MCP server that provide interactive access to storage operations
- Telemetry integrations for metrics and tracing

The project operates at the library, application, CLI, and filesystem-integration levels. Its primary security responsibilities are protecting storage credentials, preserving authorization boundaries established by storage providers and the host environment, and maintaining the confidentiality and integrity of transferred data.

**Repository Exposure Classification:** Public.
Basis: the origin repository is the publicly accessible `NVIDIA/multi-storage-client` GitHub repository.

**Service Exposure Classification:** External / Regulated (high confidence).
Basis: the project is External (Open Source), is distributed for use outside a controlled internal environment, handles storage credentials and data paths, and includes FUSE and Kubernetes CSI deployment modes.

The main security boundaries are:

- `multi-storage-client/src/multistorageclient/config.py` loads YAML configuration, included files, environment-variable substitutions, credential-provider settings, and provider endpoints. Configuration files and their owners are trusted inputs.
- Python and Rust storage providers cross a network trust boundary when communicating with S3-compatible services, Azure Blob Storage, Google Cloud Storage, AIStore, OCI Object Storage, and other configured endpoints.
- `multi-storage-client/rust/src/credentials.rs` transfers refreshed credentials from Python credential providers into the Rust extension. The Python process and PyO3 boundary share the same trust domain.
- `multi-storage-file-system` crosses the kernel/userspace boundary through FUSE. Its CSI deployment also crosses Kubernetes workload, kubelet, node, and cloud-identity boundaries.
- The Explorer REST API and MCP tools can perform read, upload, download, copy, synchronization, and deletion operations using the privileges of the selected storage profile.
- Telemetry configuration can select process, host, environment, and Multi-Storage Client configuration attributes for export to an external collector.

### Threat Model

The following scenarios represent the project's primary security concerns:

1. **Credential disclosure or credential redirection through configuration:** `config.py`, file-based credential providers, and the Rust credential bridge process access keys, tokens, and provider endpoints. An untrusted configuration file, included file, environment value, or custom endpoint could disclose credentials or redirect authenticated requests.

2. **Unauthorized storage operations through Explorer or MCP:** `multi-storage-client/src/multistorageclient/explorer/api/server.py` exposes configuration upload and storage mutation APIs, while `multi-storage-client/src/multistorageclient/mcp/tools.py` exposes similar operations to an MCP host. These interfaces rely on their local binding, process boundary, and hosting environment for access control; exposing them to untrusted callers could permit data access or deletion with the configured profile's privileges.

3. **Exposure of unauthenticated filesystem diagnostic endpoints:** `multi-storage-file-system/http.go` provides `/backends`, `/dump`, `/drain`, `/hang`, `/locks`, and `/metrics` without application-layer authentication. A broadly bound or externally routed endpoint could reveal filesystem state, backend names, or operational details, and `/drain` can modify cache state.

4. **Privilege or tenant-boundary violations in FUSE and CSI deployments:** The FUSE configuration supports `allow_other`, configurable UID/GID values, and writable mount permissions. The CSI node component requires privileged access, FUSE device access, mount propagation, and storage credentials or workload-identity tokens. Incorrect mount scoping, permissions, Kubernetes RBAC, or identity mapping could expose another workload's data or expand the impact of a compromised node component.

5. **Transport downgrade or endpoint spoofing:** Provider configurations support custom endpoints and development options such as disabled certificate verification or HTTP transport. Telemetry exporters can also be configured with external endpoints. Using these options outside a trusted development environment could expose credentials, bearer tokens, telemetry, or storage data to interception.

6. **Sensitive-data leakage through telemetry or error handling:** Telemetry attribute providers can select environment variables and values from Multi-Storage Client configuration. Explorer and provider error paths can include resource paths or provider messages. Unsafe attribute selectors or externally accessible logs and collectors could disclose credentials, object names, tenant identifiers, or filesystem layout.

7. **Data integrity failures caused by object-store and POSIX consistency differences:** Manifests, local caches, and FUSE metadata may become stale when multiple clients or out-of-band writers update the same objects. Object stores do not provide all POSIX locking and atomicity guarantees. Applications that assume globally coherent caches, atomic multi-step operations, or current manifests could read stale data or overwrite concurrent changes.

### Critical Security Assumptions

- Configuration files, included files, credential files, and relevant environment variables are controlled by a trusted operator and protected with restrictive filesystem permissions.
- Production deployments use short-lived, automatically refreshed credentials where available. Credentials are scoped to the minimum required storage operations, buckets, containers, and prefixes.
- Cloud providers, identity providers, the host operating system, and Kubernetes enforce authentication, authorization, process isolation, and secret-file permissions correctly.
- Custom storage and telemetry endpoints are operator-approved. Production traffic uses authenticated endpoints with certificate verification enabled; HTTP and certificate-verification bypasses are restricted to isolated development environments.
- The Explorer remains bound to a trusted local interface unless an authenticated, encrypted, and access-controlled proxy is placed in front of it.
- MCP is run over its intended local process transport by a trusted host. The host applies user confirmation, sandboxing, and filesystem restrictions before invoking tools with storage or local-path side effects.
- The Multi-Storage File System HTTP endpoint is disabled, bound to a trusted local or pod-only interface, or protected by network policy and an authenticated proxy.
- FUSE and CSI operators deliberately configure `allow_other`, UID/GID mappings, mount permissions, mount propagation, Kubernetes RBAC, and per-workload identities. Tenants that do not fully trust one another receive separate mount and credential boundaries.
- Applications coordinate concurrent writes and treat manifests and caches as performance metadata rather than authorization authorities. They use provider-supported conditional writes, checksums, or external coordination when consistency is required.
- Telemetry selectors, exporters, and log destinations are reviewed so that credentials, tokens, sensitive configuration values, and private object paths are not exported.
- The Python and Rust components run in the same trusted process boundary, and third-party packages and release artifacts are obtained from trusted, integrity-verified sources.
