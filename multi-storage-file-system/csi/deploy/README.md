# MSFS CSI Driver Deployment

## Prerequisites

- Kubernetes 1.28+
- EC2 nodes with `fuse` kernel module loaded
- MSFS CSI image pushed to a registry accessible from the cluster
- AWS credentials Secret in the target namespace

## Deploy

```bash
# 1. Install the CSIDriver object (cluster-wide, once)
kubectl apply -f csi-driver.yaml

# 2. Create the namespace used by the RBAC, DaemonSet, and Secret resources
kubectl create namespace msfs

# 3. Create RBAC (ServiceAccount lives in the msfs namespace)
kubectl apply -f rbac.yaml

# 4. Deploy the node plugin DaemonSet
kubectl apply -f daemonset.yaml

# 5. Verify CSI pods are running on all nodes
kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi-node

# 6. Create the AWS credentials Secret (if not already present)
kubectl create secret generic msfs-s3-credentials \
  --namespace msfs \
  --from-literal=access_key_id='<your-access-key>' \
  --from-literal=secret_access_key='<your-secret-key>'

# 7. Deploy a test pod
kubectl apply -f example-pod.yaml

# 8. Verify the mount
kubectl exec -n msfs msfs-test-app -- ls /mnt/storage/s3/
```

## Usage patterns

### Option A: PV/PVC with StorageClass (recommended)

```bash
# Create StorageClass (once per cluster)
kubectl apply -f storageclass.yaml

# Create PV + PVC + test pod
kubectl apply -f example-pv-pvc.yaml
```

The PV holds S3 details (bucket, region, credentials ref). The PVC binds to it. The pod just references the PVC name — no S3 details in the pod spec.

### Option B: Inline ephemeral volume (quick testing)

```bash
kubectl apply -f example-pod.yaml
```

S3 details are specified directly in the pod spec via `csi.volumeAttributes`.

### How it works (both options)

When the pod is scheduled:
1. Kubelet calls the MSFS CSI node plugin's `NodePublishVolume` gRPC.
2. The plugin writes a temporary `msfs.yaml`, sets AWS env vars from the Secret, and execs `msfs`.
3. MSFS creates a FUSE mount at the kubelet-managed target path.
4. Kubelet bind-mounts that path into the pod. The app sees files at `/mnt/storage/`.
5. On pod deletion, kubelet calls `NodeUnpublishVolume`. The plugin stops msfs and cleans up.

No privileged pods, no SYS_ADMIN, no mount propagation needed in the app pod.

## volumeAttributes reference

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `bucketName` | Yes | - | S3 bucket name |
| `region` | No | `us-east-1` | AWS region |
| `endpoint` | No | `https://s3.<region>.amazonaws.com` | S3 endpoint URL |
| `prefix` | No | `""` | Object key prefix |
| `readonly` | No | `true` | Mount as read-only |
| `manifestPath` | No | - | Path for manifest generation output |
| `manifestGenWorkers` | No | - | Number of manifest generation workers |
| `flatDirConfirmationPages` | No | - | Flat directory confirmation pages |
