#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_parallel_wget_warc.sh <url_list_file> [parallel_jobs]
# Example: ./run_parallel_wget_warc.sh subsample_urls_h100.txt 10

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

# Minimal dependency check
need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; exit 1; }; }
need_cmd parallel
need_cmd wget
need_cmd split
need_cmd basename
need_cmd wc
need_cmd mkdir

# Config
WARCS_DIR="warcs"
LISTS_DIR="tmp_lists"
TRIES="${TRIES:-1}"  # override via env: TRIES=2 ...

mkdir -p "${WARCS_DIR}" "${LISTS_DIR}"

TOTAL_LINES=$(wc -l < "${URL_FILE}")
if [[ "${TOTAL_LINES}" -eq 0 ]]; then
  echo "Input file is empty: ${URL_FILE}"
  exit 1
fi

# Compute lines per part (ceil division)
LINES_PER_PART=$(( (TOTAL_LINES + PARALLEL_JOBS - 1) / PARALLEL_JOBS ))
if [[ "${LINES_PER_PART}" -lt 1 ]]; then LINES_PER_PART=1; fi

echo "Total lines: ${TOTAL_LINES}"
echo "Parallel jobs: ${PARALLEL_JOBS}"
echo "Lines per part: ${LINES_PER_PART}"

# Split input into parts
rm -f "${LISTS_DIR}/part_"*.list 2>/dev/null || true
split -l "${LINES_PER_PART}" -d --additional-suffix=".list" "${URL_FILE}" "${LISTS_DIR}/part_"

PARTS=( "${LISTS_DIR}"/part_*.list )
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  echo "Split failed: no parts generated."
  exit 1
fi
echo "Generated parts: ${#PARTS[@]}"

# Run one wget per part in parallel; keep going even if some jobs fail
TIMEOUT_FLAGS="--dns-timeout=3 --connect-timeout=3 --read-timeout=5"

set +e
parallel --will-cite -j "${PARALLEL_JOBS}" --lb '
  base=$(basename {} .list)
  out="'"${WARCS_DIR}"'/warc_{#}_${base}"
  echo "[START] {#} -> ${out}.warc.gz"
  wget '"${TIMEOUT_FLAGS}"' --tries='"${TRIES}"' \
       --warc-file="${out}" -q -i "{}" -O /dev/null
  status=$?
  if [ "$status" -eq 0 ]; then
    echo "[DONE ] {#} -> ${out}.warc.gz"
  else
    echo "[FAIL ] {#} (exit $status) for {} -> ${out}.warc.gz" >&2
  fi
  exit $status
' ::: "${PARTS[@]}"
PAR_RC=$?
set -e

if [ "$PAR_RC" -ne 0 ]; then
  echo "Some jobs failed (exit ${PAR_RC}). Check the FAIL lines above for details."
else
  echo "All jobs completed successfully."
fi

echo "WARC files are in: ${WARCS_DIR}/"