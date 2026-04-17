#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FIO_TEMPLATE="${SCRIPT_DIR}/benchmark.fio"

if [[ ! -f "$FIO_TEMPLATE" ]]; then
    echo "ERROR: template file not found: ${FIO_TEMPLATE}" >&2
    exit 1
fi

usage() {
    cat <<EOF
Usage: $0 <SECTION> <DIRECTORY>

Run an FIO benchmark section against a directory of pre-existing files.

  SECTION    One of: P1.1 P1.2 P1.4 P1.8
                     P2.1 P2.2 P2.4 P2.8
                     P3.1 P3.2 P3.4 P3.8
                     P4.1 P4.2 P4.4 P4.8
                     P5.1 P5.2 P5.4 P5.8
                     P6.1 P6.2 P6.4 P6.8

  DIRECTORY  Path containing the data files.
             P1/P2 sections expect file_00000000 .. file_00001FFF (1 MiB each).
             P3-P6 sections expect file_00000000 .. file_0000004F (1 GiB each).

Examples:
  $0 P1.1 /mnt/pseudo8192x1MiB
  $0 P3.4 /mnt/pseudo80x1GiB
EOF
    exit 1
}

[[ $# -eq 2 ]] || usage

SECTION="$1"
DIRECTORY="$2"

if [[ ! -d "$DIRECTORY" ]]; then
    echo "ERROR: directory not found or not accessible: ${DIRECTORY}" >&2
    exit 1
fi

prefix="${SECTION%%.*}"

case "$prefix" in
    P1|P2)
        NFILES=8192
        ;;
    P3|P4|P5|P6)
        NFILES=80
        ;;
    *)
        echo "ERROR: unknown section prefix '${prefix}' (expected P1..P6)" >&2
        exit 1
        ;;
esac

# --- Build a temporary .fio job file from the template ---

TMPFIO=$(mktemp "/tmp/fio-${SECTION}-XXXXXXXXXXXX.fio")
trap 'rm -f "$TMPFIO"' EXIT

# Extract [global] block (everything from [global] up to, but not including,
# the next section header).
awk '/^\[global\]/{found=1} found{if(/^\[/ && !/^\[global\]/) exit; print}' \
    "$FIO_TEMPLATE" > "$TMPFIO"

# Extract the selected [SECTION] block.
awk -v sec="$SECTION" \
    'BEGIN{pat="^\\[" sec "\\]"} $0~pat{found=1} found{if(/^\[/ && !($0~pat)) exit; print}' \
    "$FIO_TEMPLATE" >> "$TMPFIO"

# Append one filename= line per file so fio knows the exact file set
# without trying to create/provision anything (the FS is read-only).
seq 0 $((NFILES - 1)) | awk '{printf "filename=file_%08X\n", $0}' >> "$TMPFIO"

echo "=== FIO benchmark: section=${SECTION}  directory=${DIRECTORY}  files=${NFILES} ==="
echo "=== temp job file: ${TMPFIO} ==="

fio "$TMPFIO" --directory="$DIRECTORY"
