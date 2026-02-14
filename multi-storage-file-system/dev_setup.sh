#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"

usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [{ais|aisMinio|garage|gcs|minio|versity}]

Populate the "dev" bucket in the specified object store (defaults to minio) with contents of the current directory tree.

Examples:
    ${SCRIPT_NAME} -h          # Show this help message
    ${SCRIPT_NAME} ais         # Populates the AIStore object store "dev" bucket
    ${SCRIPT_NAME} aisMinio    # Populates the MinIO object store "dev" bucket cached by the AIStore object store "dev" bucket
    ${SCRIPT_NAME} garage      # Populates the Garage object store "dev" bucket
    ${SCRIPT_NAME} gcs         # Populates the fake-gcs-server object store "dev" bucket
    ${SCRIPT_NAME} minio       # Populates the MinIO object store "dev" bucket
    ${SCRIPT_NAME} versity     # Populates the Versity object store "dev" bucket
    ${SCRIPT_NAME}             # Populates the (default) MinIO object store "dev" bucket
EOF
}

waitForAIStore() {
    aisCount="0"
    while [ "$aisCount" -ne 2 ]; do
        sleep 1

        (ais show cluster smap > /tmp/show_cluster_smap.out) || true
        aisCount=$(grep -c "http://ais:" /tmp/show_cluster_smap.out)
    done
}

waitForFakeGCS() {
    gcsCount="0"
    while [ "$gcsCount" -ne 1 ]; do
        sleep 1
        (curl -s -v http://fake-gcs:4443/storage/v1/b >/dev/null 2> /tmp/curl_fake_gcs_server_list_buckets.out) || true
        gcsCount=$(grep -c "200 OK" /tmp/curl_fake_gcs_server_list_buckets.out)
    done
}

waitForGarage() {
    garageCount="0"
    while [ "$garageCount" -ne 1 ]; do
        sleep 1

        (curl -s -v -H "Authorization: Bearer test_admin_token" http://garage:3903/health >/dev/null 2> /tmp/curl_garage_health.out) || true
        garageCount=$(grep -c "200 OK" /tmp/curl_garage_health.out)
    done
}

waitForMinIO() {
    minioCount="0"
    while [ "$minioCount" -ne 1 ]; do
        sleep 1

        (curl -s -I http://minio:9000/minio/health/live > /tmp/curl_minio_health_live.out) || true
        minioCount=$(grep -c "200 OK" /tmp/curl_minio_health_live.out)
    done
}

waitForVersity() {
    until s3cmd --config=${SCRIPT_DIR}/versity.s3cfg ls -r s3:// > /dev/null 2>&1; do
        sleep 1
    done
}

if [ $# -gt 1 ]; then
    usage
    exit
fi

if [ $# -eq 0 ]; then
    target_bucket=minio
else
    target_bucket=$1
fi

case "$target_bucket" in
    ais)
        waitForAIStore
        ais create ais://dev
        find . -type f | sed 's/^..//' | xargs -I {} ais put {} ais://dev/{}
        ais ls ais://dev
        ;;
    aisMinio)
        waitForMinIO
        s3cmd --config=${SCRIPT_DIR}/minio.s3cfg mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd --config=${SCRIPT_DIR}/minio.s3cfg put {} s3://dev/{}
        s3cmd --config=${SCRIPT_DIR}/minio.s3cfg ls -r s3://dev
        waitForAIStore
        ais create s3://dev --skip-lookup
        ais bucket props set s3://dev features S3-Use-Path-Style
        # ais create ais://dev
        # ais bucket props set ais://dev backend_bck=s3://dev
        # ais ls ais://dev --all
        ais ls s3://dev --all
        ;;
    garage)
        waitForGarage
        s3cmd --config=${SCRIPT_DIR}/garage.s3cfg mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd --config=${SCRIPT_DIR}/garage.s3cfg put {} s3://dev/{}
        s3cmd --config=${SCRIPT_DIR}/garage.s3cfg ls -r s3://dev
        ;;
    gcs)
        waitForFakeGCS
        curl -X POST http://fake-gcs:4443/storage/v1/b -H 'Content-Type: application/json' -d '{"name": "dev"}'
        find . -type f | sed 's/^..//' | xargs -I {} curl -X POST http://fake-gcs:4443/upload/storage/v1/b/dev/o?uploadType=media\&name={} --data-binary @{}
        curl -s http://fake-gcs:4443/storage/v1/b/dev/o | jq -r '.items[] | [("            " + (.size|tostring))[-12:], (.name)] | join(" ")'
        ;;
    minio)
        waitForMinIO
        s3cmd --config=${SCRIPT_DIR}/minio.s3cfg mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd --config=${SCRIPT_DIR}/minio.s3cfg put {} s3://dev/{}
        s3cmd --config=${SCRIPT_DIR}/minio.s3cfg ls -r s3://dev
        ;;
    versity)
        waitForVersity
        s3cmd --config=${SCRIPT_DIR}/versity.s3cfg mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd --config=${SCRIPT_DIR}/versity.s3cfg put {} s3://dev/{}
        s3cmd --config=${SCRIPT_DIR}/versity.s3cfg ls -r s3://dev
        ;;
    *)
        usage
esac
