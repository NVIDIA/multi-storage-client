# MSFS CSI Driver Deployment

> For an architecture overview (what runs where, why there's no controller plugin), see the [main CSI README](../README.md#architecture).

## Prerequisites

- Kubernetes 1.28+
- EC2 nodes with `fuse` kernel module loaded
- MSFS CSI image pushed to a registry accessible from the cluster
- One of the following auth modes:
  - **Recommended on EKS:** IRSA wired to the `msfs-csi-node` ServiceAccount (no Secret needed). See the [Helm chart README's IRSA walkthrough](../charts/msfs-csi/README.md#aws-side-for-irsa-one-time-manual--by-design) for the IAM role + OIDC trust setup.
  - **Multi-tenant (per-workload IRSA):** each workload pod assumes its own IAM role via `volumeAttributes.roleArn`. Opt in by uncommenting the `tokenRequests` / `requiresRepublish` block in `csi-driver.yaml` (the Helm chart renders it from `auth.perWorkloadIrsa.enabled=true`). See [Per-workload IRSA](../charts/msfs-csi/README.md#per-workload-irsa-each-workload-its-own-role) and `example-pv-pvc-per-workload-irsa.yaml`.
  - **Fallback:** AWS credentials Secret in the target namespace, referenced by `nodePublishSecretRef`.

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

# 6a. EKS IRSA (recommended): annotate the SA with the IAM role ARN.
#     The role's trust policy must permit AssumeRoleWithWebIdentity for the
#     EKS OIDC provider and the system:serviceaccount:msfs:msfs-csi-node SA.
kubectl annotate serviceaccount msfs-csi-node \
  --namespace msfs \
  eks.amazonaws.com/role-arn='arn:aws:iam::<account-id>:role/<msfs-csi-role>' \
  --overwrite

# 6b. Static-secret fallback (only if not using IRSA)
# kubectl create secret generic msfs-s3-credentials \
#   --namespace msfs \
#   --from-literal=access_key_id='<your-access-key>' \
#   --from-literal=secret_access_key='<your-secret-key>'

# 7. Deploy a test pod (IRSA example by default)
kubectl apply -f example-pod-irsa.yaml
# OR the static-secret variant:
# kubectl apply -f example-pod.yaml

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
| `authType` | No | `auto` | Credential mode: `auto`, `static`, `irsa` (alias `wif`) |
| `region` | No | `us-east-1` | AWS region |
| `endpoint` | No | `https://s3.<region>.amazonaws.com` | S3 endpoint URL |
| `prefix` | No | `""` | Object key prefix |
| `readonly` | No | `true` | Mount as read-only |
| `manifestPath` | No | - | Path for manifest generation output |
| `manifestGenWorkers` | No | - | Number of manifest generation workers |
| `flatDirConfirmationPages` | No | - | Flat directory confirmation pages |

> Multiple backends in one volume (multi-bucket / multi-backend via `backendsJson`) and the full per-backend tuning field set are documented in the [CSI driver README](../README.md#multiple-backends-in-one-volume). Note: under CSI a `manifestPath` manifest is regenerated on every (re)mount — the driver does not persist it (follow-up: NGCDP-9116).

## Caching and cache lifetime under CSI

The node plugin starts one MSFS process per published volume, so **the cache is
scoped to that volume on that node** — it is not shared with other pods, other
nodes, or other volumes. MSFS caching, capacity sizing, and pre-warming behavior
are documented in the [MSFS README](../../README.md#caching-and-pre-warming);
two consequences matter specifically for CSI:

- **The cache does not outlive the pod.** `NodeUnpublishVolume` stops the MSFS
  process, which removes its cache directory. A pod that reads a dataset warms
  only its own mount, so a pre-warm pod followed by a separate training pod
  starts cold. Warming across pods requires a long-lived mount outside the pod
  lifecycle, which this driver does not provide.
- **The default cache is small.** `cache_lines` defaults to 128 lines of 10 MiB,
  about 1.25 GiB. Read-heavy workloads normally need this raised through the
  volume's tuning fields; the benchmark configuration used roughly 100 GiB.

The kernel FUSE tunables that gate read concurrency (`max_background`,
`congestion_threshold`) are per-connection and reset on every mount, so under CSI
they must be applied per node rather than once per cluster. See
[Reproducing these numbers](../../README.md#reproducing-these-numbers).

## Measured scale and what is not yet qualified

Published MSFS measurements — a 100M-object namespace browsable in about 105
seconds, and a 24-cell read matrix over an ~88 GiB working set — are documented
in [Measured Scale and Performance](../../README.md#measured-scale-and-performance).

Those runs used a **single MSFS client with 1-8 application threads on one host**.
They do not characterize many concurrent clients against one backend, namespaces
substantially larger than 100M objects, datasets far larger than cache, or
tail-latency targets. A multi-node CSI deployment is exactly the case that has
not been qualified, so treat aggregate backend request fan-out and cache-hit
behavior across a DaemonSet as unmeasured. See
[Qualified scale boundary](../../README.md#qualified-scale-boundary).

The three credential modes above are implemented, but credential rotation and
multi-tenant isolation have not been qualified end-to-end. Because MSFS grants
access per mount rather than per UID/GID, every process that can read a pod's
mount can read everything the volume's credentials can reach.
