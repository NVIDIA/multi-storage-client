
#!/bin/bash

function usage() {
    echo "Usage: $0 {ais|minio} # \$1 indicates where source-of-truth objects reside"
    exit 1
}

function waitForAIStore() {
    aisCount="0"
    while [ "$aisCount" -ne 2 ]; do
        sleep 1
        (ais show cluster smap > /tmp/show_cluster_smap.out) || true
        aisCount=$(grep -c "http://ais:" /tmp/show_cluster_smap.out)
    done
}

function waitForMinio() {
    minioCount="0"
    while [ "$minioCount" -ne 1 ]; do
        sleep 1
        (curl -s -I http://minio:9000/minio/health/live > /tmp/curl_minio_health_live.out) || true
        minioCount=$(grep -c "200 OK" /tmp/curl_minio_health_live.out)
    done
}

if [ $# -ne 1 ]; then
    usage
fi

case "$1" in
    ais)
        waitForAIStore
        ais create ais://dev
        find . -type f | sed 's/^..//' | xargs -I {} ais put {} ais://dev/{}
        ais ls ais://dev
        ;;
    minio)
        waitForMinio
        s3cmd mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd put {} s3://dev/{}
        s3cmd ls -r s3://dev
        waitForAIStore
        ais create s3://dev --skip-lookup
        ais bucket props set s3://dev features S3-Use-Path-Style
        ais ls s3://dev --all
        ;;
    *)
        usage
esac
