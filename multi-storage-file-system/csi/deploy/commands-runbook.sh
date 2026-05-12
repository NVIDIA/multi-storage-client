#!/usr/bin/env bash
#
# MSFS CSI driver deployment runbook.
#
# Override the variables below for your environment, either by editing this file
# or by exporting them before running the commands, e.g.:
#
#   export REGISTRY=ghcr.io/your-org
#   export REGISTRY_HOST=ghcr.io
#   export REGISTRY_USER=your-user
#   export REGISTRY_TOKEN=your-token
#   export CLUSTER_CONTEXT=your-kubectl-context
#

REGISTRY="${REGISTRY:-your-registry}"
REGISTRY_HOST="${REGISTRY_HOST:-${REGISTRY%%/*}}"
REGISTRY_USER="${REGISTRY_USER:-your-registry-user}"
REGISTRY_TOKEN="${REGISTRY_TOKEN:-your-registry-token}"
CLUSTER_CONTEXT="${CLUSTER_CONTEXT:-your-kubectl-context}"
IMAGE="${IMAGE:-${REGISTRY}/msfs-csi:latest}"

# ============================================================
# 1. BUILD & PUSH THE CSI DRIVER IMAGE
# ============================================================

# Prune Docker build cache if needed (frees disk for large Go builds)
docker builder prune -af
docker system prune -f

# Build for linux/amd64 (required when building on Apple Silicon for x86 nodes)
# Build context is multi-storage-file-system/ (not csi/)
cd multi-storage-file-system
docker build --platform linux/amd64 -f Dockerfile.csi -t "${IMAGE}" .

# Login to your container registry
printf '%s' "${REGISTRY_TOKEN}" | docker login "${REGISTRY_HOST}" -u "${REGISTRY_USER}" --password-stdin

# Push the image
docker push "${IMAGE}"

# Verify local image architecture
docker inspect --format='{{.Architecture}}' "${IMAGE}"

# ============================================================
# 2. CONNECT TO THE KUBERNETES CLUSTER
# ============================================================

# Switch kubectl to the target cluster context
kubectl config use-context "${CLUSTER_CONTEXT}"

# Confirm access
kubectl get nodes

# ============================================================
# 3. INSTALL CSI DRIVER (cluster-wide, once)
# ============================================================

# Register the CSIDriver object with Kubernetes
kubectl apply -f csi/deploy/csi-driver.yaml

# ============================================================
# 4. CREATE NAMESPACE, RBAC, AND PULL SECRET
# ============================================================

# Namespace (if not already created)
kubectl create namespace msfs

# RBAC for the CSI node plugin
kubectl apply -f csi/deploy/rbac.yaml

# imagePullSecret (only needed if pulling from a private registry)
# kubectl create secret docker-registry your-image-pull-secret \
#   --namespace msfs \
#   --docker-server="${REGISTRY_HOST}" \
#   --docker-username="${REGISTRY_USER}" \
#   --docker-password="${REGISTRY_TOKEN}"

# ============================================================
# 5. CREATE AWS CREDENTIALS SECRET
# ============================================================

kubectl create secret generic msfs-s3-credentials \
  --namespace msfs \
  --from-literal=access_key_id="${AWS_ACCESS_KEY_ID:-your-access-key}" \
  --from-literal=secret_access_key="${AWS_SECRET_ACCESS_KEY:-your-secret-key}"

# ============================================================
# 6. DEPLOY CSI NODE PLUGIN DAEMONSET
# ============================================================

kubectl apply -f csi/deploy/daemonset.yaml

# ============================================================
# 7. VERIFY CSI NODE PLUGIN IS RUNNING
# ============================================================

# Check all CSI node pods are 2/2 Running (driver + registrar)
kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi-node -o wide

# Count CSI pods
kubectl get pods -n msfs -l app.kubernetes.io/name=msfs-csi-node --no-headers | wc -l

# Check CSIDriver is registered
kubectl get csidriver msfs.csi.nvidia.com

# ============================================================
# 8. DEPLOY A TEST APP POD WITH CSI VOLUME
# ============================================================

kubectl apply -f csi/deploy/example-pod.yaml

# Wait for pod to be Running
kubectl get pod msfs-test-app -n msfs -w

# ============================================================
# 9. VERIFY THE MOUNT WORKS
# ============================================================

# Check the app container can see the FUSE mount
kubectl exec -n msfs msfs-test-app -- ls /mnt/storage/

# List S3 bucket contents through the mount
kubectl exec -n msfs msfs-test-app -- ls /mnt/storage/s3/

# Count visible directories
kubectl exec -n msfs msfs-test-app -- ls /mnt/storage/s3/ | wc -l

# ============================================================
# 10. TEARDOWN
# ============================================================

# Delete test pod
kubectl delete pod msfs-test-app -n msfs

# Delete CSI node plugin DaemonSet
kubectl delete daemonset msfs-csi-node -n msfs

# Delete RBAC
kubectl delete -f csi/deploy/rbac.yaml

# Delete CSIDriver object
kubectl delete csidriver msfs.csi.nvidia.com

# Delete secrets
kubectl delete secret msfs-s3-credentials -n msfs

# ============================================================
# 11. TROUBLESHOOTING
# ============================================================

# Check CSI driver logs on a specific node
# kubectl logs -n msfs CSI_NODE_POD_NAME -c msfs-csi-driver

# Check node-driver-registrar logs
# kubectl logs -n msfs CSI_NODE_POD_NAME -c node-driver-registrar

# Check if CSI driver socket is registered
# kubectl exec -n msfs CSI_NODE_POD_NAME -c msfs-csi-driver -- ls -la /csi/csi.sock

# "exec format error" — image built for wrong arch; rebuild with --platform linux/amd64
# "no space left on device" during build — run docker builder prune -af
# "ImagePullBackOff" — imagePullSecret missing or wrong; check kubectl get secrets -n msfs
# "denied" on docker push — check the registry path and that you're logged in
# Pod stuck in ContainerCreating — check CSI driver logs and events:
#   kubectl describe pod POD_NAME -n msfs
#   kubectl logs -n msfs CSI_NODE_POD -c msfs-csi-driver
