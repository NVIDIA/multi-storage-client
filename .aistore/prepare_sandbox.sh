#! /usr/bin/env bash

# Dynamically generate AIStore configurations to resolve relative paths.

set -e

SANDBOX_ABSOLUTE_PATH=$(realpath .)/sandbox

mkdir --parents ${SANDBOX_ABSOLUTE_PATH}/{data,test}

cat > ${SANDBOX_ABSOLUTE_PATH}/ais.json << END_OF_FILE
{
    "backend": {
        "aws": {},
        "azure": {},
        "gcp": {},
        "ht": {},
        "oci": {}
    },
    "mirror": {
        "copies": 2,
        "burst_buffer": 128,
        "enabled": false
    },
    "ec": {
        "objsize_limit": 262144,
        "compression": "never",
        "bundle_multiplier": 2,
        "data_slices": 1,
        "parity_slices": 1,
        "enabled": false,
        "disk_only": false
    },
    "chunks": {
        "objsize_limit": "0",
        "chunk_size": "1GiB",
        "checkpoint_every": 0,
        "flags": 0
    },
    "log": {
        "level": "3",
        "max_size": "10mb",
        "max_total": "256mb",
        "flush_time": "60s",
        "stats_time": "60s"
    },
    "periodic": {
        "stats_time": "10s",
        "notif_time": "30s",
        "retry_sync_time": "2s"
    },
    "timeout": {
        "cplane_operation": "2s",
        "max_keepalive": "4s",
        "cold_get_conflict": "5s",
        "max_host_busy": "20s",
        "startup_time": "1m",
        "join_startup_time": "3m",
        "send_file_time": "5m",
        "ec_streams_time": "10m",
        "object_md": "2h"
    },
    "client": {
        "client_timeout": "10s",
        "client_long_timeout": "10m",
        "list_timeout": "1m"
    },
    "proxy": {
        "primary_url": "",
        "original_url": "",
        "discovery_url": "",
        "non_electable": false
    },
    "space": {
        "cleanupwm": 65,
        "lowwm": 75,
        "highwm": 90,
        "out_of_space": 95,
        "batch_size": 32768,
        "dont_cleanup_time": "120m"
    },
    "lru": {
        "dont_evict_time": "120m",
        "capacity_upd_time": "10m",
        "enabled": true
    },
    "disk": {
        "iostat_time_long": "2s",
        "iostat_time_short": "100ms",
        "iostat_time_smooth": "8s",
        "disk_util_low_wm": 20,
        "disk_util_high_wm": 80,
        "disk_util_max_wm": 95
    },
    "rebalance": {
        "dest_retry_time": "2m",
        "compression": "never",
        "bundle_multiplier": 2,
        "enabled": true
    },
    "resilver": {
        "enabled": true
    },
    "checksum": {
        "type": "xxhash2",
        "validate_cold_get": false,
        "validate_warm_get": false,
        "validate_obj_move": false,
        "enable_read_range": false
    },
    "transport": {
        "max_header": 4096,
        "burst_buffer": 512,
        "idle_teardown": "4s",
        "quiescent": "10s",
        "lz4_block": "256kb",
        "lz4_frame_checksum": false
    },
    "memsys": {
        "min_free": "2gb",
        "default_buf": "32kb",
        "to_gc": "4gb",
        "hk_time": "3m",
        "min_pct_total": 0,
        "min_pct_free": 0
    },
    "versioning": {
        "enabled": true,
        "validate_warm_get": false
    },
    "net": {
        "l4": {
            "proto": "tcp",
            "sndrcv_buf_size": 131072
        },
        "http": {
            "use_https": false,
            "server_crt": "server.crt",
            "server_key": "server.key",
            "domain_tls": "",
            "client_ca_tls": "",
            "client_auth_tls": 0,
            "idle_conn_time": "6s",
            "idle_conns_per_host": 0,
            "idle_conns": 0,
            "write_buffer_size": 0,
            "read_buffer_size": 0,
            "chunked_transfer": true,
            "skip_verify": false
        }
    },
    "fshc": {
        "test_files": 4,
        "error_limit": 2,
        "io_err_limit": 10,
        "io_err_time": "10s",
        "enabled": true
    },
    "auth": {
        "secret": "",
        "enabled": false
    },
    "keepalivetracker": {
        "proxy": {
            "interval": "10s",
            "name": "heartbeat",
            "factor": 3
        },
        "target": {
            "interval": "10s",
            "name": "heartbeat",
            "factor": 3
        },
        "num_retries": 3,
        "retry_factor": 4
    },
    "downloader": {
        "timeout": "1h"
    },
    "distributed_sort": {
        "duplicated_records": "ignore",
        "missing_shards": "ignore",
        "ekm_malformed_line": "abort",
        "ekm_missing_key": "abort",
        "default_max_mem_usage": "80%",
        "call_timeout": "10m",
        "dsorter_mem_threshold": "100GB",
        "compression": "never",
        "bundle_multiplier": 4
    },
    "tcb": {
        "compression": "never",
        "bundle_multiplier": 2
    },
    "tco": {
        "compression": "never",
        "bundle_multiplier": 2
    },
    "arch": {
        "compression": "never",
        "bundle_multiplier": 2
    },
    "write_policy": {
        "data": "",
        "md": ""
    },
    "rate_limit": {
        "backend": {
            "num_retries": 3,
            "interval": "1m",
            "per_op_max_tokens": "",
            "max_tokens": 1000,
            "enabled": false
        },
        "frontend": {
            "burst_size": 375,
            "interval": "1m",
            "per_op_max_tokens": "",
            "max_tokens": 1000,
            "enabled": false
        }
    },
    "features": "0"
}
END_OF_FILE

cat > ${SANDBOX_ABSOLUTE_PATH}/ais_local.json << END_OF_FILE
{
    "confdir": ".",
    "log_dir": "log",
    "host_net": {
        "hostname": "",
        "hostname_intra_control": "",
        "hostname_intra_data": "",
        "port": "51080",
        "port_intra_control": "9080",
        "port_intra_data": "10080"
    },
    "fspaths": {
        "${SANDBOX_ABSOLUTE_PATH}/data": ""
    },
    "test_fspaths": {
        "root": "${SANDBOX_ABSOLUTE_PATH}/test",
        "count": 0,
        "instance": 0
    }
}
END_OF_FILE
