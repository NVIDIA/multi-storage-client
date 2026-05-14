# MSFS CSI Driver

A Kubernetes CSI (Container Storage Interface) node plugin that mounts S3 object storage as a local FUSE filesystem inside application pods, powered by MSFS.

## Why CSI instead of a sidecar

The sidecar pattern requires `Bidirectional` mount propagation from the MSFS container to the app container. Many Kubernetes clusters (including EKS) do not support this reliably. With a CSI driver, **kubelet** manages the FUSE mount directly and bind-mounts it into the pod — no mount propagation, no privileged app pods, no sidecar needed.

## Architecture

![MSFS CSI Driver Architecture](docs/architecture.png)

**In one sentence:** the driver runs as a single `DaemonSet` (one pod per worker node) that mounts an S3 bucket as a FUSE filesystem and lets the kubelet bind-mount it into your application pod — no controller plugin, nothing on the control plane, and the app pod stays unprivileged.

### Where it runs

| Component | Workload type | Lives on | Notes |
|---|---|---|---|
| `CSIDriver` object | Cluster-scoped K8s resource | API server | Declarative registration; not a running process |
| `msfs-csi-node` pod | `DaemonSet` (`msfs` namespace) | Every worker node | Two containers: `msfs-csi-driver` + `node-driver-registrar` sidecar |
| `msfs` (FUSE) | Child process of the driver container | Same worker node | One per mounted volume; spawned on `NodePublishVolume`, killed on `NodeUnpublishVolume` |
| App pod | Your workload | Any worker node | Just references the PVC or inline CSI volume |

There is **no controller `Deployment` or `StatefulSet`**. The `CSIDriver` object sets `attachRequired: false` because object storage has no attach/detach phase — any node can talk to S3 over HTTPS in parallel, so there's no cluster-wide work for a controller to do. On managed Kubernetes (EKS, GKE, AKS) nothing runs on the control plane.

### Per-node flow when a pod starts

1. The kubelet on the chosen worker node sees a Pod that needs an MSC volume.
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

The driver supports two credential modes via `volumeAttributes.authType`. Default is `auto`: static when a secret is provided, otherwise IRSA.

**Recommended on EKS — IRSA / workload identity (no Secret needed):**

```bash
kubectl annotate serviceaccount msfs-csi-node \
  --namespace msfs \
  eks.amazonaws.com/role-arn='arn:aws:iam::<account-id>:role/<msfs-csi-role>' \
  --overwrite
```

The IAM role's trust policy must allow `AssumeRoleWithWebIdentity` from the cluster's OIDC provider for the `system:serviceaccount:msfs:msfs-csi-node` SA. The role needs least-privilege S3 access (e.g. `s3:ListBucket` + `s3:GetObject` scoped to the bucket and prefix). See `deploy/EKS_CSI_CREDENTIALS_A_B_C_GUIDE.md` for an exact policy.

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
    controller.go               CSI Controller service (CreateVolume, DeleteVolume, ValidateVolumeCapabilities)
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
    EKS_CSI_CREDENTIALS_A_B_C_GUIDE.md
                                Phased credentials migration plan
    README.md                   Deploy instructions
  charts/msfs-csi/              Helm chart (single-command install)
  go.mod / go.sum               Go module (CSI spec + gRPC deps)
```

The Dockerfile is at `multi-storage-file-system/Dockerfile.csi` (build context needs the msfs source).

## How NodePublishVolume works

1. Kubelet calls `NodePublishVolume` with `targetPath`, `volumeAttributes`, and `secrets`.
2. The plugin resolves the credential mode from `volumeAttributes.authType` (`auto` / `static` / `irsa`).
3. The plugin writes a temporary `msfs.yaml` config from `volumeAttributes` (bucket, region, prefix, etc.). In `static` mode the config includes `${AWS_ACCESS_KEY_ID}` / `${AWS_SECRET_ACCESS_KEY}` placeholders; in `irsa` mode they are omitted so the AWS SDK falls through to its credential chain (projected SA token).
4. In `static` mode, AWS credentials from the K8s Secret (`nodePublishSecretRef`) are exported as env vars on the msfs process. In `irsa` mode no AWS env vars are injected — EKS-set `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` reach msfs unchanged.
5. The plugin execs `msfs <config-path>`. MSFS creates a FUSE mount at `targetPath`.
6. Kubelet bind-mounts `targetPath` into the app pod.

## How NodeUnpublishVolume works

1. Kubelet calls `NodeUnpublishVolume` with `targetPath`.
2. The plugin sends SIGTERM to the msfs process (SIGKILL after 5s if needed).
3. Runs `fusermount -u` on the target path.
4. Cleans up the temporary config directory and mount point.

## volumeAttributes reference

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `bucketName` | Yes | - | S3 bucket name |
| `authType` | No | `auto` | Credential mode: `auto` (static if Secret provided, else IRSA), `static`, `irsa` (alias `wif`) |
| `region` | No | `us-east-1` | AWS region |
| `endpoint` | No | `https://s3.<region>.amazonaws.com` | S3 endpoint URL |
| `prefix` | No | `""` | Object key prefix (with trailing `/` if non-empty) |
| `readonly` | No | `true` | Mount as read-only |
| `manifestPath` | No | - | Path for manifest generation output |
| `manifestGenWorkers` | No | - | Number of parallel listing workers |
| `flatDirConfirmationPages` | No | - | Flat directory confirmation pages |

## Secret keys reference

Only required when `authType=static` (or `auto` with a Secret provided). In `irsa` mode the Secret is unused.

The K8s Secret referenced by `nodePublishSecretRef` should contain:

| Key | Required | Description |
|-----|----------|-------------|
| `access_key_id` | Yes | AWS access key ID |
| `secret_access_key` | Yes | AWS secret access key |
| `session_token` | No | AWS session token (for temporary credentials) |

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pod stuck in `ContainerCreating` | CSI driver not running on that node | Check `kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi-node` |
| `exec format error` in CSI pod | Image built for wrong architecture | Rebuild with `--platform linux/amd64` |
| `ImagePullBackOff` | Missing imagePullSecret | Check `kubectl get secrets -n msfs` |
| Mount timeout | msfs failed to start (bad config or credentials) | Check CSI driver logs: `kubectl logs -n msfs <csi-pod> -c msfs-csi-driver` |
| Empty mount in app pod | MSFS started but bucket is empty or prefix wrong | Verify `volumeAttributes` in pod spec |
| `fusermount: bad mount point` on cleanup | Mount already gone | Safe to ignore; cleanup continues |

See [deploy/commands-runbook.sh](deploy/commands-runbook.sh) for the full command reference.
