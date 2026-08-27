# MSFS CSI Driver

A Kubernetes CSI (Container Storage Interface) node plugin that mounts S3 object storage as a local FUSE filesystem inside application pods, powered by MSFS.

## Why CSI instead of a sidecar

The sidecar pattern requires `Bidirectional` mount propagation from the MSFS container to the app container. Many Kubernetes clusters (including EKS) do not support this reliably. With a CSI driver, **kubelet** manages the FUSE mount directly and bind-mounts it into the pod — no mount propagation, no privileged app pods, no sidecar needed.

## Architecture

**In one sentence:** the driver runs as a single `DaemonSet` (one pod per worker node) that mounts an S3 bucket as a FUSE filesystem and lets the kubelet bind-mount it into your application pod — no controller plugin, nothing on the control plane, and the app pod stays unprivileged.

```text
+--------------------------------------------------------------------+
|                           Worker node                              |
|                                                                    |
|    +---------+                                                     |
|    | kubelet |                                                     |
|    +----+----+                                                     |
|         |                                                          |
|         |  (1) NodePublishVolume gRPC                              |
|         |      over /csi/csi.sock                                  |
|         v                                                          |
|    +----------------------------------------------------+          |
|    |    msfs-csi-node DaemonSet pod   (privileged)      |          |
|    |                                                    |          |
|    |    +---------------------+                         |          |
|    |    |  msfs-csi-driver    |                         |          |
|    |    +----------+----------+                         |          |
|    |               |  (2) exec(msfs <config>)           |          |
|    |               v                                    |          |
|    |    +---------------------+                         |          |
|    |    |  msfs  (FUSE)       |                         |          |
|    |    +----------+----------+                         |          |
|    +---------------|------------------------------------+          |
|                    |                                               |
|                    |  (3) FUSE mount appears at kubelet's          |
|                    |      targetPath on the host                   |
|                    v                                               |
|    +----------------------------------------------------+          |
|    |    Application pod   (unprivileged, no SYS_ADMIN)  |          |
|    |                                                    |          |
|    |    +---------------------+                         |          |
|    |    |  app container      |                         |          |
|    |    |                     |  (4) kubelet bind-      |          |
|    |    |  /mnt/storage  <----+      mounts targetPath  |          |
|    |    |                     |      into the container |          |
|    |    +----------+----------+                         |          |
|    +---------------|------------------------------------+          |
|                    |  (5) read / write                             |
|                    v                                               |
|         (kernel routes I/O through FUSE back to msfs above)        |
|                    |                                               |
+--------------------|-----------------------------------------------+
                     |  (6) HTTPS — GetObject / PutObject /
                     |              ListObjectsV2 scoped by prefix
                     v
            +---------------------+
            |  AWS S3             |
            |  (or any MSC        |
            |   backend)          |
            +---------------------+
```

### Where it runs

| Component | Workload type | Lives on | Notes |
|---|---|---|---|
| `CSIDriver` object | Cluster-scoped K8s resource | API server | Declarative registration; not a running process |
| `msfs-csi-node` pod | `DaemonSet` (`msfs` namespace) | Every worker node | Two containers: `msfs-csi-driver` + `node-driver-registrar` sidecar |
| `msfs` (FUSE) | Child process of the driver container | Same worker node | One per mounted volume; spawned on `NodePublishVolume`, killed on `NodeUnpublishVolume` |
| App pod | Your workload | Any worker node | Just references the PVC or inline CSI volume |

There is **no controller `Deployment` or `StatefulSet`**. The `CSIDriver` object sets `attachRequired: false` because object storage has no attach/detach phase — any node can talk to S3 over HTTPS in parallel, so there's no cluster-wide work for a controller to do. On managed Kubernetes (EKS, GKE, AKS) nothing runs on the control plane.

### Per-node flow when a pod starts

1. The kubelet on the chosen worker node sees a Pod that needs an MSFS volume.
2. It calls `NodePublishVolume` on the local `msfs-csi-driver` over a UNIX socket at `/csi/csi.sock` (registered with the kubelet by the `node-driver-registrar` sidecar).
3. The driver writes a temporary `msfs.yaml`, sets AWS env vars from the K8s `Secret`, and execs the `msfs` binary, which creates a FUSE mount at the kubelet-managed `targetPath`.
4. The kubelet bind-mounts `targetPath` into the app pod at the requested mount path (e.g. `/mnt/storage/s3/`).
5. On pod deletion, `NodeUnpublishVolume` stops the `msfs` process and cleans up the mount.

## Quick start

### 1. Build and push the image

From `multi-storage-file-system/`:

```bash
docker build --platform linux/amd64 -f Dockerfile.csi -t <your-registry>/msfs-csi:latest .
docker push <your-registry>/msfs-csi:latest
```

Then update `image:` in `csi/deploy/daemonset.yaml` to point to your pushed image.

### 2. Install CSI components

```bash
kubectl apply -f csi/deploy/csi-driver.yaml
kubectl apply -f csi/deploy/rbac.yaml
kubectl apply -f csi/deploy/daemonset.yaml
```

### 3. Configure credentials (choose one mode)

The driver supports static, IRSA, and no-credentials modes via `volumeAttributes.authType`. Default is `auto`: static when a secret is provided, otherwise IRSA. Use `none` (alias `anonymous`) to connect with no credentials at all — S3 sends unsigned requests and AIStore uses an empty token (e.g. a local AIS cluster or a public bucket).

**Recommended on EKS — IRSA / workload identity (no Secret needed):**

```bash
kubectl annotate serviceaccount msfs-csi-node \
  --namespace msfs \
  eks.amazonaws.com/role-arn='arn:aws:iam::<account-id>:role/<msfs-csi-role>' \
  --overwrite
```

The IAM role's trust policy must allow `AssumeRoleWithWebIdentity` from the cluster's OIDC provider for the `system:serviceaccount:msfs:msfs-csi-node` SA. The role needs least-privilege S3 access (e.g. `s3:ListBucket` + `s3:GetObject` scoped to the bucket and prefix). See the [Helm chart README's IRSA walkthrough](charts/msfs-csi/README.md#aws-side-for-irsa-one-time-manual--by-design) for an exact trust policy and bucket policy.

**Fallback — static AWS access keys in a Secret:**

```bash
kubectl create secret generic msfs-s3-credentials \
  --namespace msfs \
  --from-literal=access_key_id='<your-key>' \
  --from-literal=secret_access_key='<your-secret>'
```

### 4. Use in your pod

**Option A: PV/PVC (recommended for shared/production use)**

Admin creates a StorageClass, PV, and PVC once per bucket:

```bash
kubectl apply -f csi/deploy/storageclass.yaml
kubectl apply -f csi/deploy/example-pv-pvc.yaml
```

App pod references just the PVC — no S3 details:

```yaml
containers:
  - name: app
    image: my-app:latest
    volumeMounts:
      - name: data
        mountPath: /mnt/storage
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: msfs-s3-claim
```

**Option B: Inline ephemeral (quick testing)**

S3 details specified directly in the pod spec. Static-secret variant:

```yaml
containers:
  - name: app
    image: my-app:latest
    volumeMounts:
      - name: s3-data
        mountPath: /mnt/storage
volumes:
  - name: s3-data
    csi:
      driver: msfs.csi.nvidia.com
      volumeAttributes:
        bucketName: my-bucket
        region: us-west-2
      nodePublishSecretRef:
        name: msfs-s3-credentials
```

IRSA variant — no `nodePublishSecretRef`:

```yaml
volumes:
  - name: s3-data
    csi:
      driver: msfs.csi.nvidia.com
      volumeAttributes:
        authType: irsa
        bucketName: my-bucket
        region: us-west-2
```

No `privileged`, no `SYS_ADMIN`, no mount propagation in the app pod with either option.

### 5. Verify

```bash
kubectl exec <pod-name> -- ls /mnt/storage/s3/
```

## Project structure

```
csi/
  cmd/msfs-csi-driver/
    main.go                     Entry point (flags, signal handling)
  pkg/driver/
    driver.go                   Driver struct, gRPC wiring
    server.go                   Unix socket listener, logging interceptor
    identity.go                 CSI Identity service (3 RPCs)
    node.go                     CSI Node service (Publish/Unpublish + config gen)
    controller.go               CSI Controller service — registered but inert (see below)
  deploy/
    csi-driver.yaml             CSIDriver K8s object
    daemonset.yaml              Node plugin DaemonSet + registrar
    rbac.yaml                   ServiceAccount + ClusterRole
    storageclass.yaml           Default StorageClass
    example-pod.yaml            Static-secret inline pod example
    example-pod-irsa.yaml       IRSA / workload-identity inline pod example
    example-pv-pvc.yaml         Static-secret PV/PVC example
    example-pv-pvc-irsa.yaml    IRSA / workload-identity PV/PVC example
    commands-runbook.sh         Full command reference
    README.md                   Deploy instructions
  charts/msfs-csi/              Helm chart (single-command install)
  go.mod / go.sum               Go module (CSI spec + gRPC deps)
```

The Dockerfile is at `multi-storage-file-system/Dockerfile.csi` (build context needs the msfs source).

**On `controller.go`.** The Controller service is registered on the same socket
(`driver.go`), but it does not provision anything and nothing calls it in any
documented flow: `CSIDriver.attachRequired` is `false`, so there is no
`external-provisioner` or `external-attacher`, and no controller Deployment is
installed. `CreateVolume` echoes the request name and parameters back without
touching S3; `DeleteVolume` is an explicit no-op and never deletes a bucket.
Buckets must exist before you mount them. Dynamic provisioning is future scope
— do not read the presence of `CreateVolume` as support for it.

## How NodePublishVolume works

1. Kubelet calls `NodePublishVolume` with `targetPath`, `volumeAttributes`, and `secrets`.
2. The plugin resolves the credential mode from `volumeAttributes.authType` (`auto` / `static` / `irsa` / `none`).
3. The plugin writes a temporary `msfs.yaml` config from `volumeAttributes` (bucket, region, prefix, etc.). In `static` mode the config includes `${AWS_ACCESS_KEY_ID}` / `${AWS_SECRET_ACCESS_KEY}` placeholders; in `irsa` mode they are omitted so the AWS SDK falls through to its credential chain (projected SA token); in `none` mode an S3 backend gets `anonymous: true` (unsigned requests) and an AIStore backend is left with no token.
4. Credentials are supplied to msfs according to the mode:
   - **`static`** — AWS keys from the K8s Secret (`nodePublishSecretRef`) are exported as env vars on the msfs process.
   - **`none` (alias `anonymous`)** — no Secret, no IRSA, and no AWS env vars are injected. For S3 the generated config sets `anonymous: true` so the SDK skips request signing; for AIStore the token is left empty (anonymous access). Useful for public buckets or no-auth endpoints such as a local AIS cluster.
   - **`irsa` (driver-SA, default)** — no AWS env vars are injected; the EKS-set `AWS_ROLE_ARN` / `AWS_WEB_IDENTITY_TOKEN_FILE` of the **driver** pod reach msfs unchanged, so every mount shares the driver's IAM role.
   - **`irsa` (per-workload)** — when the chart sets `auth.perWorkloadIrsa.enabled=true` (CSIDriver `tokenRequests`), the kubelet passes the **workload** pod's projected token in `volume_context`. The plugin writes it to `<config-dir>/aws-web-identity-token` (mode `0600`) and overrides `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` (from `volumeAttributes.roleArn`) so the mount assumes the workload's own role. With `requiresRepublish`, the kubelet re-publishes periodically and the plugin rewrites the token file in place — msfs is not restarted.
5. The plugin execs `msfs <config-path>`. MSFS creates a FUSE mount at `targetPath`.
6. The plugin waits up to **30 seconds** for the mount to appear. If it does not, publish fails and the pod stays in `ContainerCreating`.
7. Kubelet bind-mounts `targetPath` into the app pod.

**Per-workload IRSA needs `tokenRequests` on the `CSIDriver` object.** The Helm
chart renders it when `auth.perWorkloadIrsa.enabled=true` (default `false`). The
raw manifests in `deploy/` ship with that block **commented out**, so a cluster
installed from `deploy/csi-driver.yaml` gets driver-SA IRSA only — the kubelet
never sends a workload token and `volumeAttributes.roleArn` is ignored. Uncomment
it there, or install via the chart.

## How NodeUnpublishVolume works

1. Kubelet calls `NodeUnpublishVolume` with `targetPath`.
2. The plugin sends `SIGTERM` to the msfs process, then `SIGKILL` after a 5-second grace period.
3. Runs `fusermount -u` on the target path, falling back to `umount` if that fails. Unmount is bounded at 10 seconds.
4. Cleans up the temporary config directory and mount point.

## volumeAttributes reference

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `bucketName` | Conditional | - | Bucket / AIStore bucket name. Required for a single-backend volume; omit when using `backendsJson`. |
| `backendsJson` | No | - | JSON array of backend objects to expose **multiple** backends in one volume (multi-bucket / multi-backend). Each object: `dirName`, `backendType`, `bucketName` (required), `prefix`, `readonly`, plus S3 (`region`, `endpoint`) and AIStore (`aisEndpoint`, `aisProvider`, `aisAuthnToken`, `aisAuthnTokenFile`, `aisSkipTLSCertificateVerify`, `aisTimeout`, `aisManifestGenBackend`) fields, plus the per-backend tuning fields (`manifestPath`, `manifestGenWorkers`, `flatDirConfirmationPages`, `traceLevel`, `directoryPageSize`, `uid`, `gid`, `dirPerm`, `filePerm`, `flushOnClose`, `multipartCacheLineThreshold`, `multipartUploadThresholdBytes`, `uploadPartCacheLines`, `uploadPartConcurrency`) — numeric values passed as strings (e.g. `"uid": "1000"`). When set, the single-backend attributes below are ignored. Credentials (`authType` / Secret) are shared by all backends. |
| `backendType` | No | `S3` | MSFS backend emitted by the CSI driver: `S3` or `AIStore` |
| `dirName` | No | `s3` / `ais` | Directory name exposed under the MSFS mount for the generated backend |
| `authType` | No | `auto` | Credential mode: `auto` (static if Secret provided, else IRSA), `static`, `irsa` (alias `wif`), `none` (alias `anonymous`; no credentials — unsigned S3 / empty AIStore token) |
| `roleArn` | No | - | IAM role ARN the mount assumes under **per-workload IRSA** (`auth.perWorkloadIrsa.enabled=true`). Required in that mode; ignored otherwise. |
| `region` | No | `us-east-1` | AWS region (`backendType=S3`) |
| `endpoint` | No | `https://s3.<region>.amazonaws.com` | S3 endpoint URL (`backendType=S3`) |
| `prefix` | No | `""` | Object key prefix (with trailing `/` if non-empty) |
| `readonly` | No | `true` | Mount as read-only |
| `manifestPath` | No | - | Path for manifest generation output |
| `manifestGenWorkers` | No | - | Number of parallel listing workers |
| `flatDirConfirmationPages` | No | - | Flat directory confirmation pages |
| *(tuning keys)* | No | - | Cache sizing, write behaviour, ownership and permissions are listed separately under [Performance tuning reference](#performance-tuning-reference) |
| `aisEndpoint` | No | `${AIS_ENDPOINT}` | Native AIStore endpoint (`backendType=AIStore`) |
| `aisProvider` | No | `s3` | AIStore bucket provider (`ais`, `aws`, `gcp`, `azure`, etc.) |
| `aisAuthnTokenFile` | No | `${AIS_AUTHN_TOKEN_FILE:-${HOME}/.config/ais/cli/auth.token}` | AIStore auth token file read by MSFS at mount setup |
| `aisAuthnToken` | No | `${AIS_AUTHN_TOKEN}` | Inline AIStore auth token; prefer `aisAuthnTokenFile` or env/secret projection |
| `aisSkipTLSCertificateVerify` | No | `false` | Skip AIStore endpoint TLS verification |
| `aisTimeout` | No | `30000` | AIStore client timeout in milliseconds |
| `aisManifestGenBackend` | No | - | Existing backend name used by MSFS for AIStore LIST/STAT-DIR delegation |

## Performance tuning reference

The attributes above select *what* to mount. The ones below control *how well*
it performs, and every one of them is optional. Defaults are MSFS defaults, not
CSI-specific.

### Mount-wide tuning

These apply to the whole volume rather than to one backend, so they are always
flat `volumeAttributes` keys even when `backendsJson` is used.

| Key | Default | Description |
|-----|---------|-------------|
| `cacheLineSize` | `10485760` (10 MiB) | Fetch and residency granularity. Larger suits sequential access; smaller suits random access. |
| `cacheLines` | `128` | Number of cache lines provisioned. **With the defaults this is only ~1.25 GiB of cache.** Size it to your working set. |
| `cacheLinesToPrefetch` | `4` | Read-ahead depth for sequential reads. |
| `dirtyCacheLinesFlushTrigger` | `80` | Dirty cache lines that trigger a background flush. |
| `dirtyCacheLinesMax` | `90` | Hard ceiling on dirty cache lines. |
| `writeCommitWorkers` | `32` | Small objects committed concurrently. File-level concurrency, separate from `uploadPartConcurrency`. |
| `writeCommitQueueDepth` | `256` | Detached commits allowed to queue before release applies backpressure. |
| `writeCachePromotion` | `false` | Admit bytes retained from a successful write into the read cache, with no extra GET. |
| `allowOther` | `true` | **Omitting this leaves the mount readable by other local users on the node.** MSFS defaults `allow_other` to `true` and the driver emits nothing when the attribute is absent, so the MSFS default applies. Set `"false"` to restrict the mount to the mounting user. |

**Cache sizing is the highest-impact knob.** The default of 128 x 10 MiB is
about 1.25 GiB; the read benchmarks in the
[MSFS user guide](https://nvidia.github.io/multi-storage-client/user_guide/multi_storage_file_system.html)
used `cacheLines: 10000`, roughly 100 GiB. Capacity is a hard bound and eviction
is LRU, so a dataset larger than the cache holds only its active working set.

`cacheLineSize x cacheLines` is reserved **per mounted volume, on every node**,
and which resource it consumes depends on `cache_storage` — which CSI cannot
set, so the MSFS default of `mapped-file` always applies to a CSI mount:

| `cache_storage` | Backed by | Budget on the DaemonSet |
|---|---|---|
| `mapped-file` (MSFS default, and what CSI always gets) | One memory-mapped file under `cache_dir_path` | **Ephemeral storage**, not a memory limit. Page-cache pressure still shows up as node memory use. |
| `per-inode-file` | Per-inode files served with `pread` | Ephemeral storage. |
| `ram` | Anonymous `mmap`, outside the Go heap | Memory. Pages commit on first touch, and it is not counted against `process_memory_limit`. |

So on CSI, size the pod's `ephemeral-storage` request rather than its memory
limit. `cache_dir_path` also cannot be set through CSI and defaults to empty,
which resolves to `os.TempDir()` — `$TMPDIR`, or `/tmp` — **inside the driver
container**. Unless a volume is mounted there, the cache consumes the container's
writable layer, and a large `cacheLines` can fill the node's disk or trip the
pod's ephemeral-storage limit. The directory is created per mount and removed on
unmount, so nothing persists across a remount.

### Per-backend tuning

Valid as flat keys for a single-backend volume, and as fields inside each
`backendsJson` entry. Numeric values are strings in `backendsJson`
(e.g. `"uid": "1000"`).

| Key | Default | Description |
|-----|---------|-------------|
| `uid` | **`0` under CSI** | Owner UID reported for every file and directory. MSFS defaults to the euid of the process that mounts, and under CSI that process is the driver container — which is privileged with no `runAsUser`, so the euid is `0`. Files therefore appear **root-owned inside your unprivileged application pod** unless you set this. |
| `gid` | **`0` under CSI** | Owner GID, same reasoning as `uid`. |
| `dirPerm` | `555` ro / `777` rw | Directory permission bits, 3-digit octal. |
| `filePerm` | `444` ro / `666` rw | File permission bits, 3-digit octal. Objects carry no mode, so this is backend-wide policy and `chmod` is accepted but not applied. |

Ownership and permissions interact in a way worth checking before you deploy.
None of the shipped examples in `deploy/` set `uid` or `gid`, so a mount is
root-owned by default. Reads still work because `filePerm` defaults to `444`
(world-readable), and a writable mount works because the defaults become `777`
and `666` — but that also means **a writable mount is world-writable inside the
pod**. If your application checks ownership, or you want tighter bits, set
`uid`, `gid`, `dirPerm` and `filePerm` explicitly to match the pod's
`securityContext`.
| `flushOnClose` | `false` | When `"true"`, `close()` waits for the backend commit. When false, the commit is asynchronous and `fsync()` is the durability barrier. |
| `multipartUploadThresholdBytes` | `67108864` (64 MiB) | Buffered size at which a new object is promoted from a single `PutObject` to a streaming multipart upload. `0` disables deferral. |
| `multipartCacheLineThreshold` | `512` | Files fitting in this many cache lines upload in a single PUT. |
| `uploadPartCacheLines` | `32` | Cache lines per multipart part, so part size is this times `cacheLineSize`. |
| `uploadPartConcurrency` | `32` | Parts uploaded in parallel for one file. |
| `directoryPageSize` | `0` | Directory entries fetched per page; `0` uses the endpoint default. |
| `traceLevel` | `0` | `1` traces errors, `2` adds successes, `>2` adds detail. |

Writes require `readonly: "false"`, which is **not** the default. See the write
durability semantics in
[`multi-storage-file-system/README.md`](../README.md#write-durability-semantics).

### Settings that cannot be set through CSI

The driver generates `msfs.yaml` from `volumeAttributes` and only emits the keys
above. These MSFS settings have no `volumeAttributes` equivalent today:

| Setting | Why it matters |
|---|---|
| `process_memory_limit` | Go soft-memory limit, **defaults to 4 GiB**. A large manifest ingest needs more, and below its working set the heap sits permanently above the limit and Go garbage-collects continuously, collapsing throughput. This is the most likely one to bite a large deployment. |
| `fuse_workers`, `fuse_fd_per_worker` | FUSE concurrency. The default worker count is `runtime.NumCPU()`, which is too many on large hosts. |
| `cache_storage`, `cache_dir_path` | Where cache lines live (`ram`, `mapped-file`, `per-inode-file`) and on which filesystem. Inside a DaemonSet pod this decides whether the cache consumes pod memory or node disk. |
| `write_deferral_max_bytes` | Global ceiling on memory held by deferred writes across all backends. |
| `auto_sighup_interval` | Periodic config reload. |

If you need one of these, mount MSFS directly rather than through CSI, or file
an issue — adding a key is a small, additive change to `writeConfig` in
`pkg/driver/node.go`.

### Worked example

A writable, cache-heavy mount for a training workload:

```yaml
    volumeAttributes:
      authType: irsa
      bucketName: my-training-data
      region: us-west-2
      readonly: "false"
      # ~40 GiB of cache on each node
      cacheLineSize: "10485760"
      cacheLines: "4096"
      cacheLinesToPrefetch: "4"
      # write path
      flushOnClose: "false"
      multipartUploadThresholdBytes: "67108864"
      writeCommitWorkers: "32"
      writeCachePromotion: "true"
```

Remember to raise the DaemonSet's `ephemeral-storage` request to match the cache
you asked for — 4096 x 10 MiB is about 40 GiB per volume per node, and with the
default `mapped-file` storage that lands on disk rather than in the memory limit.

## Multiple backends in one volume

By default a volume exposes a single backend built from the flat attributes above. To expose **several** backends under one mount (e.g. two S3 buckets, or S3 + native AIStore), set `volumeAttributes.backendsJson` to a JSON array — each entry becomes one MSFS backend, mounted under its own `dirName` subdirectory. The single-backend attributes (`bucketName`, `backendType`, `region`, …) are ignored when `backendsJson` is present.

```yaml
    volumeAttributes:
      authType: irsa            # credentials are shared by all backends
      backendsJson: |
        [
          {"dirName": "images",  "backendType": "S3", "bucketName": "my-images", "prefix": "train/", "region": "us-west-2"},
          {"dirName": "labels",  "backendType": "S3", "bucketName": "my-labels"},
          {"dirName": "cache",   "backendType": "AIStore", "bucketName": "ds", "aisEndpoint": "http://ais:51080", "aisProvider": "ais"}
        ]
```

This mounts `images/`, `labels/`, and `cache/` under the volume. `dirName` values must be unique; an unsupported `backendType`, a missing `bucketName`, or a duplicate `manifestPath` across entries is rejected. Credentials are volume-level: every S3 backend shares the one `authType` / `nodePublishSecretRef` (per-backend distinct AWS identities are out of scope). Per-backend tuning fields (`manifestPath`, `manifestGenWorkers`, `uid`, perms, …) are supported per entry (numeric values passed as strings).

**`manifestPath` under CSI:** the `msfs` process runs inside the CSI node DaemonSet pod, so `manifestPath` resolves in that pod's filesystem (not the application pod) and is **not** persisted by the driver — the generated config dir is removed on `NodeUnpublishVolume`. As a result the manifest is **regenerated on every (re)mount**, but only for `readonly: true` backends: MSFS restricts mount-time generation to read-only backends, so a `readonly: false` volume with `manifestPath` logs `skipping generation` and mounts without manifest metadata unless the manifest was generated out of band into a path the mount can read. Manifest ingest itself runs for writable backends too, so a persisted `manifestPath` also replays the append-only delta log written by an earlier mount. Persisting it across mounts requires a DaemonSet-mounted volume and is tracked as a follow-up (NGCDP-9116). Note that manifest generation does a `RemoveAll` on `manifestPath`, which is why two backends in one volume may not share one.

## Credential resolution

`resolveCredentialMode` decides per mount, so one driver instance serves mixed
static and IRSA volumes simultaneously — useful during a migration.

| `authType` | Secret with both keys? | Resolved mode | Generated `msfs.yaml` | Environment given to `msfs` |
|---|---|---|---|---|
| unset or `auto` | yes | `static` | `${AWS_ACCESS_KEY_ID}` / `${AWS_SECRET_ACCESS_KEY}` placeholders | keys exported from the Secret |
| unset or `auto` | no, or only one of the two | `irsa` | placeholders omitted | host env passed through unchanged |
| `static` | yes | `static` | placeholders present | keys exported from the Secret |
| `static` | no | **error** | - | `InvalidArgument`: `authType=static requires both access_key_id and secret_access_key` |
| `irsa` or `wif` | ignored | `irsa` | placeholders omitted | host env passed through unchanged |
| `none` or `anonymous` | ignored | `none` | S3 gets `anonymous: true`; AIStore token left empty | no AWS variables injected |
| anything else | - | **error** | - | `InvalidArgument` listing the valid values |

**Why the placeholders are omitted in `irsa` mode.** If `msfs.yaml` kept
`${AWS_ACCESS_KEY_ID}` while the variable was unset, the AWS SDK would treat it
as an intended static credential and fail with a missing-credentials error
rather than falling through to the projected-token chain. The symptom is an auth
failure on a cluster where IRSA is configured correctly. This is asserted by
`TestWriteConfig_WorkloadIdentityModeOmitsStaticCredentialPlaceholders` in
`node_test.go`; do not "fix" the generated config by adding them back.

Note that `auto` resolving to `irsa` is not a check that IRSA works — it only
means no usable Secret was supplied. On a cluster without IRSA the mount is
created and the failure appears at first S3 request instead.

**Per-workload IRSA** reads the workload pod's token from
`volume_context["csi.storage.k8s.io/serviceAccount.tokens"]`, matching on the
audience configured in `auth.perWorkloadIrsa.audience` (default
`sts.amazonaws.com`). It writes the token to `aws-web-identity-token` in the
per-mount config dir with mode `0600` and points `AWS_WEB_IDENTITY_TOKEN_FILE`
at it, with `AWS_ROLE_ARN` taken from `volumeAttributes.roleArn`. If that
`volume_context` key is absent — older kubelet, or `tokenRequests` not set on
the `CSIDriver` — the driver falls back to driver-SA IRSA rather than failing.
That fallback is why a misconfigured cluster looks like "IRSA works but every
mount uses the wrong role".

Cluster-side IAM role and OIDC trust setup are AWS account operations. Neither
the chart nor the driver provisions them; see the
[chart README](charts/msfs-csi/README.md#aws-side-for-irsa-one-time-manual--by-design)
for copy-paste trust and bucket policies.

## Secret keys reference

Only required when `authType=static` (or `auto` with a Secret provided). In `irsa` mode the Secret is unused.

The K8s Secret referenced by `nodePublishSecretRef` should contain:

| Key | Required | Description |
|-----|----------|-------------|
| `access_key_id` | Yes | AWS access key ID |
| `secret_access_key` | Yes | AWS secret access key |
| `session_token` | No | AWS session token (for temporary credentials) |

## Extending the driver

Where each behaviour lives, so a change lands in the right place:

| To change | Edit | Notes |
|---|---|---|
| Add a `volumeAttributes` key that maps to a **mount-wide** MSFS setting | `writeConfig` in `pkg/driver/node.go` | Add one `optionalGlobalStr("yourKey", "your_msfs_key")` line, then document it under [Mount-wide tuning](#mount-wide-tuning). |
| Add a **per-backend** MSFS setting | `renderMSFSBackend` in `pkg/driver/node.go` | Use `optionalStr` for numerics/bools, `optionalQuoted` for strings that need YAML quoting. Also add the field to `volumeBackendJSON` and its `backendFromJSON` mapping so `backendsJson` accepts it. |
| Add a credential mode | `resolveCredentialMode` + `buildEnv` in `pkg/driver/node.go` | Covered by `node_test.go`; add a case there. |
| Change mount/unmount behaviour | `NodePublishVolume` / `NodeUnpublishVolume` in `pkg/driver/node.go` | Timeouts are the `unmountTimeout`, `killGrace` and publish-wait constants at the top of the file. |
| Change what the chart installs | `charts/msfs-csi/templates/` and `values.yaml` | Keep `deploy/*.yaml` in sync; they are the no-Helm path and drift silently. |

**Docs and code must agree in both directions.** Every key in the tables above
should be one the driver reads, and every key the driver reads should appear in
a table. To check:

```bash
cd multi-storage-file-system/csi

# keys the driver reads. Three access patterns, all three are needed:
#   optional*      -> the tuning keys
#   volCtx["..."]  -> inline lookups (allowOther, authType, bucketName, ...)
#   valOrDefault   -> keys with a default (dirName, region, endpoint)
{ rg -o 'optional(GlobalStr|Str|Quoted)\("([a-zA-Z]+)"' -r '$2' pkg/driver/node.go
  rg -o 'volCtx\["([a-zA-Z]+)"\]' -r '$1' pkg/driver/node.go
  rg -o 'valOrDefault\(volCtx, "([a-zA-Z]+)"' -r '$1' pkg/driver/node.go
} | sort -u

# keys the README documents (one key per table cell, or this misses them)
rg -o '^\| `([a-zA-Z]+)`' -r '$1' README.md | sort -u
```

Keep one key per table cell. Writing ``| `uid` / `gid` |`` in a single cell
hides `gid` from the second command and the drift goes unnoticed.

Build and test:

```bash
cd multi-storage-file-system/csi
go build ./... && go vet ./... && go test ./...
helm lint ../charts/msfs-csi 2>/dev/null || helm lint charts/msfs-csi
helm template charts/msfs-csi          # render without installing

# image (build context is multi-storage-file-system/, needs the msfs source)
cd .. && docker build --platform linux/amd64 -f Dockerfile.csi -t <registry>/msfs-csi:dev .
```

The driver execs the `msfs` binary rather than linking it, so MSFS behaviour
changes do not require a driver change — but the generated `msfs.yaml` uses
`msfs_version: 1`, the MSFS-native schema in
[`config.go`](../config.go), so a renamed or removed MSFS config key will break
mounts silently at runtime rather than at build time.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pod stuck in `ContainerCreating` | CSI driver not running on that node | Check `kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi-node` |
| `exec format error` in CSI pod | Image built for wrong architecture | Rebuild with `--platform linux/amd64` |
| `ImagePullBackOff` | Missing imagePullSecret | Check `kubectl get secrets -n msfs` |
| Mount timeout | msfs failed to start within the 30s publish window (bad config or credentials) | Check CSI driver logs: `kubectl logs -n msfs <csi-pod> -c msfs-csi-driver` |
| Missing-credentials error although IRSA is configured | Under `authType: static` a complete Secret is required, and the config keeps `${AWS_ACCESS_KEY_ID}` placeholders. A *partial* Secret under `auto` resolves to `irsa` instead and never uses the Secret at all, so credentials silently come from the driver's role rather than the keys you supplied | Set `authType: irsa` explicitly for IRSA, or supply both `access_key_id` and `secret_access_key`. See [Credential resolution](#credential-resolution) |
| Every mount uses the same IAM role under per-workload IRSA | `tokenRequests` is absent from the `CSIDriver`, so the driver silently fell back to driver-SA IRSA | Install via the chart with `auth.perWorkloadIrsa.enabled=true`, or uncomment the block in `deploy/csi-driver.yaml` |
| Mount is read-only unexpectedly | `readonly` defaults to `true`, and a read-only publish is a hard floor a backend cannot override | Set `volumeAttributes.readonly: "false"` **and** ensure the PV has `accessModes: [ReadWriteMany]` rather than `ReadOnlyMany` |
| Slow first access on a large bucket | `manifestPath` is not persisted, so the manifest regenerates on every remount | Known limitation (NGCDP-9116); omit `manifestPath` if regeneration costs more than it saves |
| Throughput collapses on a large bucket | `process_memory_limit` defaults to 4 GiB and cannot be set through CSI | Mount MSFS directly, or reduce the working set. See [Settings that cannot be set through CSI](#settings-that-cannot-be-set-through-csi) |
| Empty mount in app pod | MSFS started but bucket is empty or prefix wrong | Verify `volumeAttributes` in pod spec |
| `fusermount: bad mount point` on cleanup | Mount already gone | Safe to ignore; cleanup continues |

See [deploy/commands-runbook.sh](deploy/commands-runbook.sh) for the full command reference.
