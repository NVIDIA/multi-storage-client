# MSFS CSI Driver

A Kubernetes CSI (Container Storage Interface) node plugin that mounts S3 object storage as a local FUSE filesystem inside application pods, powered by MSFS.

## Why CSI instead of a sidecar

The sidecar pattern requires `Bidirectional` mount propagation from the MSFS container to the app container. Many Kubernetes clusters (including EKS) do not support this reliably. With a CSI driver, **kubelet** manages the FUSE mount directly and bind-mounts it into the pod — no mount propagation, no privileged app pods, no sidecar needed.

## Architecture

```
┌─── Every EC2 Node ─────────────────────────────────────────┐
│                                                             │
│  ┌─────────────────────┐     ┌──────────────────────────┐  │
│  │ CSI Node Plugin     │     │ node-driver-registrar    │  │
│  │ (DaemonSet)         │     │ (registers socket with   │  │
│  │                     │     │  kubelet)                 │  │
│  │ gRPC server on      │     └──────────────────────────┘  │
│  │ /csi/csi.sock       │                                    │
│  └──────────┬──────────┘                                    │
│             │                                               │
│   NodePublishVolume                                         │
│             │                                               │
│             ▼                                               │
│  ┌─────────────────────┐                                    │
│  │ msfs process        │──── FUSE mount at targetPath       │
│  │ (child of plugin)   │                                    │
│  └─────────────────────┘                                    │
│             │                                               │
│        kubelet bind-mounts targetPath into pod              │
│             │                                               │
│             ▼                                               │
│  ┌─────────────────────┐                                    │
│  │ App Pod             │                                    │
│  │ /mnt/storage/s3/... │ ◄── S3 bucket contents visible     │
│  └─────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

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

### 3. Create an AWS credentials Secret

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

S3 details specified directly in the pod spec:

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
    example-pod.yaml            Test pod using the CSI volume
    commands-runbook.sh          Full command reference
    README.md                   Deploy instructions
  go.mod / go.sum               Go module (CSI spec + gRPC deps)
```

The Dockerfile is at `multi-storage-file-system/Dockerfile.csi` (build context needs the msfs source).

## How NodePublishVolume works

1. Kubelet calls `NodePublishVolume` with `targetPath`, `volumeAttributes`, and `secrets`.
2. The plugin writes a temporary `msfs.yaml` config from `volumeAttributes` (bucket, region, prefix, etc.).
3. AWS credentials from the K8s Secret (`nodePublishSecretRef`) are set as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables on the msfs process.
4. The plugin execs `msfs <config-path>`. MSFS creates a FUSE mount at `targetPath`.
5. Kubelet bind-mounts `targetPath` into the app pod.

## How NodeUnpublishVolume works

1. Kubelet calls `NodeUnpublishVolume` with `targetPath`.
2. The plugin sends SIGTERM to the msfs process (SIGKILL after 5s if needed).
3. Runs `fusermount -u` on the target path.
4. Cleans up the temporary config directory and mount point.

## volumeAttributes reference

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `bucketName` | Yes | - | S3 bucket name |
| `region` | No | `us-east-1` | AWS region |
| `endpoint` | No | `https://s3.<region>.amazonaws.com` | S3 endpoint URL |
| `prefix` | No | `""` | Object key prefix (with trailing `/` if non-empty) |
| `readonly` | No | `true` | Mount as read-only |
| `manifestPath` | No | - | Path for manifest generation output |
| `manifestGenWorkers` | No | - | Number of parallel listing workers |
| `flatDirConfirmationPages` | No | - | Flat directory confirmation pages |

## Secret keys reference

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
