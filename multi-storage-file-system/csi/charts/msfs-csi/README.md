# msfs-csi Helm chart

Single-command install of the MSFS CSI driver, which mounts S3 object storage as a POSIX FUSE filesystem inside Kubernetes pods.

This chart **defaults to IRSA / workload-identity** auth — the recommended path on EKS. Static AWS access keys remain a supported fallback.

## TL;DR

```bash
# 1. Build & push the driver image (once per release)
cd multi-storage-file-system
docker build --platform linux/amd64 -f Dockerfile.csi \
  -t <your-registry>/msfs-csi:v0.1.0 .
docker push <your-registry>/msfs-csi:v0.1.0

# 2. Install the chart (recommended IRSA flow)
helm install msfs-csi ./csi/charts/msfs-csi \
  --namespace msfs --create-namespace \
  --set image.repository=<your-registry>/msfs-csi \
  --set image.tag=v0.1.0 \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<acct>:role/<role>

# 3. Mount a bucket
kubectl apply -f ./csi/deploy/example-pv-pvc-irsa.yaml
```

## What the chart installs

| Resource | When | Notes |
|---|---|---|
| `CSIDriver/msfs.csi.nvidia.com` | Always | Cluster-scoped registration (`attachRequired: false`) |
| `Namespace` | When `namespace.create=true` | Off by default; use `--create-namespace` instead |
| `ServiceAccount/msfs-csi-node` | When `serviceAccount.create=true` (default) | IRSA annotation goes here |
| `ClusterRole` + `ClusterRoleBinding` | Always | Minimum perms: `get nodes` |
| `DaemonSet/msfs-csi-node` | Always | Privileged, hostPath FUSE access; one pod per worker node |
| `StorageClass/msfs-s3` | When `storageClass.create=true` (default) | Provisioner = the CSI driver |

There is no Controller Deployment / StatefulSet — see `../../README.md#architecture` for why.

## Auth modes

The driver supports four credential modes, selected per-volume via `volumeAttributes.authType`:

| Mode | Behavior | Needs Secret? |
|---|---|---|
| `auto` (default) | Static when a `nodePublishSecretRef` is provided, otherwise IRSA | Optional |
| `static` | Read keys from `nodePublishSecretRef`; reject if incomplete | **Required** |
| `irsa` (alias `wif`) | Skip secret injection; let the AWS SDK use the projected SA token | No |
| `none` (alias `anonymous`) | Connect with no credentials at all: S3 issues unsigned requests (`anonymous: true`), AIStore connects with an empty token (e.g. a local AIS cluster or a public bucket) | No |

The chart's `auth.mode` value only sets the default emitted in chart-rendered guidance — your PV/inline volume specs can still mix modes per-volume.

**Per-workload IRSA (opt-in).** By default an `irsa` mount uses the *driver* ServiceAccount's IAM role, shared by every workload. Set `auth.perWorkloadIrsa.enabled=true` to instead give each workload pod its **own** role — see [Per-workload IRSA](#per-workload-irsa-each-workload-its-own-role) below.

## Prerequisites

### Cluster-side (required for the chart itself)

- Kubernetes 1.28+
- Worker nodes with the `fuse` kernel module loaded (EC2 nodes have this; Fargate does not)
- Cluster admission policy that allows the DaemonSet container to run `privileged: true` with `SYS_ADMIN`
- Network egress from worker nodes to S3 (NAT or S3 VPC endpoint)
- The driver image (`msfs-csi`) pushed to a registry the cluster can pull from

### AWS-side for IRSA (one-time, manual — by design)

These four steps are AWS account operations that no Helm chart can do for you. They mirror the prerequisites for `aws-load-balancer-controller`, `aws-ebs-csi-driver`, etc.

#### 1. Get the cluster's OIDC provider URL

```bash
aws eks describe-cluster --name <cluster-name> \
  --query "cluster.identity.oidc.issuer" --output text
# → https://oidc.eks.<region>.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE
```

If your cluster doesn't already have an IAM OIDC provider associated with it:

```bash
eksctl utils associate-iam-oidc-provider --cluster <cluster-name> --approve
```

#### 2. Create the IAM trust policy

Save as `trust-policy.json`, replacing `<account-id>`, `<region>`, and the OIDC ID:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<account-id>:oidc-provider/oidc.eks.<region>.amazonaws.com/id/<OIDC-ID>"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.<region>.amazonaws.com/id/<OIDC-ID>:aud": "sts.amazonaws.com",
          "oidc.eks.<region>.amazonaws.com/id/<OIDC-ID>:sub": "system:serviceaccount:msfs:msfs-csi-node"
        }
      }
    }
  ]
}
```

The `:sub` claim must match `system:serviceaccount:<release-namespace>:<sa-name>`. If you change `serviceAccount.name` or install into a non-`msfs` namespace, update this.

#### 3. Create the least-privilege bucket policy

Save as `s3-policy.json`, scoped to the bucket and prefix you want to expose:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::<bucket-name>",
      "Condition": {
        "StringLike": { "s3:prefix": ["<prefix>/*", "<prefix>"] }
      }
    },
    {
      "Sid": "ReadObjectsPrefix",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::<bucket-name>/<prefix>/*"
    }
  ]
}
```

For read-write workloads, add `s3:PutObject` and `s3:DeleteObject` with the same prefix scoping.

#### 4. Create the role and attach the policy

```bash
aws iam create-role --role-name msfs-csi \
  --assume-role-policy-document file://trust-policy.json

aws iam put-role-policy --role-name msfs-csi \
  --policy-name msfs-csi-s3 \
  --policy-document file://s3-policy.json

aws iam get-role --role-name msfs-csi --query 'Role.Arn' --output text
# → arn:aws:iam::<account-id>:role/msfs-csi
```

Pass that ARN to `helm install` via `serviceAccount.annotations."eks\.amazonaws\.com/role-arn"`.

## Per-workload IRSA (each workload its own role)

By default every IRSA mount shares the `msfs-csi-node` driver role. On multi-tenant clusters where each workload should be scoped to its own bucket/prefix, enable per-workload IRSA so each pod assumes its own IAM role:

```bash
helm install msfs-csi ./csi/charts/msfs-csi \
  --namespace msfs --create-namespace \
  --set image.repository=<registry>/msfs-csi --set image.tag=v0.1.0 \
  --set auth.perWorkloadIrsa.enabled=true
```

This makes the `CSIDriver` declare `tokenRequests` (audience `sts.amazonaws.com`) and `requiresRepublish: true`. At mount time the kubelet mints a projected token for the **workload** pod's ServiceAccount, and the driver assumes the role named in that PV's `volumeAttributes.roleArn`.

What changes vs. driver-SA IRSA:

- **Trust per workload.** Each IAM role's trust policy `:sub` claim must match the *workload* pod's ServiceAccount (e.g. `system:serviceaccount:msfs:team-a`), not `msfs-csi-node`. The `:aud` stays `sts.amazonaws.com`.
- **PV carries the role.** Each PV / inline volume sets `volumeAttributes.roleArn=<workload-role-arn>` — required in this mode.
- **Backward compatible.** Default is `false`. Existing driver-SA IRSA and static mounts are unaffected; only mounts whose PV sets `roleArn` and whose kubelet delivers a workload token use the new path.

See [`deploy/example-pv-pvc-per-workload-irsa.yaml`](../../deploy/example-pv-pvc-per-workload-irsa.yaml) for a complete workload SA + PV/PVC + pod example.

Tunables: `auth.perWorkloadIrsa.audience` (default `sts.amazonaws.com` — leave as-is for AWS) and `auth.perWorkloadIrsa.tokenExpirationSeconds` (default `3600`; the kubelet republishes to refresh before expiry).

## Static-secret fallback (when IRSA isn't an option)

If you can't use IRSA (non-EKS cluster, no OIDC provider, IAM constraints), the chart still works in static mode. **The chart never inlines credential values** — you must create the Secret out of band:

```bash
kubectl create secret generic msfs-s3-credentials \
  --namespace msfs \
  --from-literal=access_key_id='<AKIA...>' \
  --from-literal=secret_access_key='<your-secret>'
```

Then reference it from your PV / inline volume via `nodePublishSecretRef`. See [`deploy/example-pv-pvc.yaml`](../../deploy/example-pv-pvc.yaml).

For "creds in cluster but not in Git" patterns, layer one of these on top — the chart is agnostic to which:

- Sealed Secrets (kubeseal) — encrypted blob committed to Git, decrypted in-cluster
- SOPS + helm-secrets — encrypted YAML, decrypted at deploy time
- Vault + External Secrets Operator — reference a Vault path, ESO syncs to a `Secret`
- Kubernetes Secrets Store CSI driver — projected from any provider
- CI pipeline secret store — pipeline runs `kubectl create secret` at deploy

## Values reference

| Key | Default | Description |
|---|---|---|
| `image.repository` | `msfs-csi` | Driver image repo |
| `image.tag` | `""` (uses `Chart.AppVersion`) | Driver image tag |
| `image.pullPolicy` | `IfNotPresent` | |
| `nodeDriverRegistrar.image.repository` | `registry.k8s.io/sig-storage/csi-node-driver-registrar` | Sidecar |
| `nodeDriverRegistrar.image.tag` | `v2.12.0` | |
| `imagePullSecrets` | `[]` | Pull secrets for private registries |
| `namespace.create` | `false` | Create the release namespace as part of the chart |
| `namespace.name` | `msfs` | Used only when `namespace.create=true` |
| `serviceAccount.create` | `true` | |
| `serviceAccount.name` | `msfs-csi-node` | |
| `serviceAccount.annotations` | `{}` | **Set `eks.amazonaws.com/role-arn` for IRSA** |
| `serviceAccount.labels` | `{}` | |
| `nodePlugin.driverName` | `msfs.csi.nvidia.com` | |
| `nodePlugin.hostPluginPath` | `/var/lib/kubelet/plugins/msfs.csi.nvidia.com` | |
| `nodePlugin.hostKubeletPath` | `/var/lib/kubelet/pods` | |
| `nodePlugin.hostRegistrationPath` | `/var/lib/kubelet/plugins_registry` | |
| `nodePlugin.hostFuseDevice` | `/dev/fuse` | |
| `nodePlugin.resources` | `{}` | CPU/memory requests + limits for the driver container |
| `nodePlugin.nodeSelector` | `{}` | |
| `nodePlugin.tolerations` | `[]` | |
| `nodePlugin.affinity` | `{}` | |
| `nodePlugin.podAnnotations` | `{}` | |
| `nodePlugin.podLabels` | `{}` | |
| `nodePlugin.extraArgs` | `[]` | Appended after `--endpoint`, `--v=4` |
| `storageClass.create` | `true` | |
| `storageClass.name` | `msfs-s3` | |
| `storageClass.default` | `false` | Mark as cluster default |
| `storageClass.reclaimPolicy` | `Retain` | |
| `storageClass.volumeBindingMode` | `Immediate` | |
| `staticCredentials.existingSecretName` | `""` | Reference-only; chart never inlines values |
| `staticCredentials.existingSecretNamespace` | `""` | Defaults to release namespace |
| `auth.mode` | `irsa` | Default mode advertised in NOTES.txt |
| `auth.perWorkloadIrsa.enabled` | `false` | Opt-in: `CSIDriver` gets `tokenRequests` + `requiresRepublish`; each mount assumes its PV's `roleArn` |
| `auth.perWorkloadIrsa.audience` | `sts.amazonaws.com` | Token audience (leave as-is for AWS STS) |
| `auth.perWorkloadIrsa.tokenExpirationSeconds` | `3600` | Projected token lifetime; kubelet republishes to refresh |
| `commonLabels` | `{}` | Added to every rendered resource |

## Native AIStore Backend

By default the CSI driver emits an MSFS `S3` backend. To use the native AIStore API instead, set these per-volume attributes in your PV or inline CSI volume:

```yaml
volumeAttributes:
  backendType: AIStore
  bucketName: my-ais-bucket
  aisEndpoint: https://ais.example.com
  aisProvider: ais
  aisAuthnTokenFile: /var/run/secrets/ais/token
```

The driver writes these into the generated `msfs.yaml` as `backend_type: AIStore`. AWS-specific `authType` / IRSA settings are only needed for S3-backed mounts.

## Multiple backends in one volume

To expose several backends under one mount (multi-bucket, or mixed S3 + AIStore), set `volumeAttributes.backendsJson` to a JSON array instead of the single-backend attributes — each entry is mounted under its own `dirName` subdirectory:

```yaml
volumeAttributes:
  authType: irsa            # shared by all backends
  backendsJson: |
    [
      {"dirName": "images", "backendType": "S3", "bucketName": "my-images", "prefix": "train/", "region": "us-west-2"},
      {"dirName": "cache",  "backendType": "AIStore", "bucketName": "ds", "aisEndpoint": "http://ais:51080", "aisProvider": "ais"}
    ]
```

`dirName` values must be unique; a missing `bucketName`, unsupported `backendType`, or duplicate `manifestPath` is rejected. Credentials are volume-level (one `authType` / Secret shared by all S3 backends). Per-backend tuning fields (`manifestPath`, `manifestGenWorkers`, `uid`, perms, …) may be set per entry (numeric values as strings); under CSI a `manifestPath` manifest is regenerated on every (re)mount (not persisted by the driver — see NGCDP-9116). See the [CSI README](../../README.md#multiple-backends-in-one-volume) for the full field list.

## Verification after install

```bash
# All node pods up
kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi -o wide

# Driver registered with kubelet
kubectl get csidriver msfs.csi.nvidia.com

# Mount a test PV (IRSA flow)
kubectl apply -f ../../deploy/example-pv-pvc-irsa.yaml
kubectl exec -n msfs msfs-pvc-test-app-irsa -- ls /mnt/storage/s3/
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Pod stuck `ContainerCreating` | Driver pod not on that node, or mount timeout | `kubectl describe pod`; check driver logs on that node |
| `WebIdentityErr: ... no identity-based policy allows` | IAM role / trust policy mismatch | Re-check the `:sub` claim matches the SA |
| `MissingRegion` | Region not set anywhere | Add `region` to `volumeAttributes` or `AWS_REGION` env to driver |
| `403 Forbidden` from S3 | Bucket policy blocks the role | Add the role ARN to the bucket policy or use a path the role policy allows |
| `roleArn is required for per-workload IRSA` (`InvalidArgument`) | `auth.perWorkloadIrsa.enabled=true` but the PV has no `volumeAttributes.roleArn` | Add `roleArn` to the PV, or disable `auth.perWorkloadIrsa.enabled` |
| `ImagePullBackOff` | Registry auth | Set `imagePullSecrets` |
| `exec format error` | Image built for wrong CPU arch | Rebuild with `--platform linux/amd64` |

For the "is FUSE the latency bottleneck?" question, see the architecture overview in `../../README.md#architecture`.

## Cross-references

- [Driver source and architecture](../../README.md)
- [Deploy manifests (raw kubectl flow)](../../deploy/README.md)

## Versioning

This chart is independently versioned from the driver image:

- `Chart.yaml.version` — chart-only version (bump for chart changes)
- `Chart.yaml.appVersion` — default driver image tag (bump when shipping a new driver release)

Set `image.tag` explicitly to override.
