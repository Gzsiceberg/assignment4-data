#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_parallel_wget_warc.sh <url_list_file> [parallel_jobs]
# Example: ./run_parallel_wget_warc.sh subsample_urls_h100.txt 16

URL_FILE="${1:-}"
PARALLEL_JOBS="${2:-16}"

if [[ -z "${URL_FILE}" ]]; then
  echo "Usage: $0 <url_list_file> [parallel_jobs]"
  exit 1
fi

if [[ ! -f "${URL_FILE}" ]]; then
  echo "Error: input file not found: ${URL_FILE}"
  exit 1
fi

# Check required commands
need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; exit 1; }; }
need_cmd split
need_cmd parallel
need_cmd wget
need_cmd wc
need_cmd mkdir
need_cmd basename

# Output directories
WARCS_DIR="warcs"
LISTS_DIR="tmp_lists"
mkdir -p "${WARCS_DIR}" "${LISTS_DIR}"

# Count lines and compute lines per part (ceil division)
TOTAL_LINES=$(wc -l < "${URL_FILE}")
if [[ "${TOTAL_LINES}" -eq 0 ]]; then
  echo "Input file is empty: ${URL_FILE}"
  exit 1
fi

LINES_PER_PART=$(( (TOTAL_LINES + PARALLEL_JOBS - 1) / PARALLEL_JOBS ))
if [[ "${LINES_PER_PART}" -lt 1 ]]; then
  LINES_PER_PART=1
fi

echo "Total lines: ${TOTAL_LINES}"
echo "Parallel jobs: ${PARALLEL_JOBS}"
echo "Lines per part: ${LINES_PER_PART}"

# Clean previous parts (if any)
rm -f "${LISTS_DIR}/part_"*.list 2>/dev/null || true

# Split into list parts
split -l "${LINES_PER_PART}" -d --additional-suffix=".list" "${URL_FILE}" "${LISTS_DIR}/part_"

PARTS=( "${LISTS_DIR}"/part_*.list )
PART_COUNT=${#PARTS[@]}
if [[ "${PART_COUNT}" -eq 0 ]]; then
  echo "Split failed: no sub-list files generated."
  exit 1
fi

echo "Generated parts: ${PART_COUNT}"

# Run parallel fetch: one wget process per part, one WARC per process
# Notes:
# - --timeout=5 covers DNS/connection/read timeouts (you can also set granular ones)
# - --tries=1 for speed; increase if you need resilience
# - -q for quiet operation; remove it to debug
# - --warc-compression=on: some wget 1.x builds want a boolean; if your wget errors on this, remove the line
parallel -j "${PARALLEL_JOBS}" --lb --halt now,fail=1 '
  base=$(basename {} .list);
  echo "[START] {#} -> ${WARCS_DIR}/warc_{#}_${base}.warc.gz"
  wget \
    --timeout=5 \
    --warc-file="${WARCS_DIR}/warc_{#}_${base}" \
    -q -i {} -O /dev/null
  status=$?
  if [ "$status" -eq 0 ]; then
    echo "[DONE ] {#} -> ${WARCS_DIR}/warc_{#}_${base}.warc.gz"
  else
    echo "[FAIL ] {#} (exit $status) for {}" >&2
  fi
  exit $status
' ::: "${PARTS[@]}"

echo "All tasks finished. WARC files are in: ${WARCS_DIR}/"

# Uncomment to clean sub-lists after completion
# rm -rf "${LISTS_DIR}"