#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_parallel_wget_warc.sh <url_list_file> [parallel_jobs] [--debug]
# Example: ./run_parallel_wget_warc.sh subsample_urls_h100.txt 10 --debug

URL_FILE="${1:-}"
PARALLEL_JOBS="${2:-10}"
DEBUG_FLAG="${3:-}"

if [[ -z "${URL_FILE}" ]]; then
  echo "Usage: $0 <url_list_file> [parallel_jobs] [--debug]"
  exit 1
fi
if [[ ! -f "${URL_FILE}" ]]; then
  echo "Error: input file not found: ${URL_FILE}"
  exit 1
fi

# Required commands
need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; exit 1; }; }
need_cmd split
need_cmd parallel
need_cmd wget
need_cmd wc
need_cmd mkdir
need_cmd basename
need_cmd head
need_cmd tail

# Output directories (make sure they exist)
WARCS_DIR="warcs"
LISTS_DIR="tmp_lists"
LOGS_DIR="logs"
mkdir -p "${WARCS_DIR}" "${LISTS_DIR}" "${LOGS_DIR}"

# Optional: print versions
echo "wget: $(wget --version | head -n 1)"
echo "parallel: $(parallel --version | head -n 1)"

# Determine debug/verbosity
DEBUG=0
if [[ "${DEBUG_FLAG}" == "--debug" ]]; then
  DEBUG=1
fi

# Configure wget flags
# You can override retries by exporting TRIES, e.g., TRIES=2 ./run_parallel_wget_warc.sh ...
TRIES="${TRIES:-1}"
if [[ "${DEBUG}" -eq 1 ]]; then
  # Show server responses and minimal output; log will capture details
  WGET_FLAGS="-S -nv"
else
  WGET_FLAGS="-q"
fi

# Timeouts (tune as needed)
TIMEOUT_FLAGS="--dns-timeout=3 --connect-timeout=3 --read-timeout=5"

# Compression option: prefer enabling compression if supported.
if wget --help 2>&1 | grep -qE -- '--warc-compression'; then
  WARC_COMP_ARG="--warc-compression=on"
else
  WARC_COMP_ARG=""
fi

TOTAL_LINES=$(wc -l < "${URL_FILE}")
if [[ "${TOTAL_LINES}" -eq 0 ]]; then
  echo "Input file is empty: ${URL_FILE}"
  exit 1
fi

# Split into roughly equal parts
LINES_PER_PART=$(( (TOTAL_LINES + PARALLEL_JOBS - 1) / PARALLEL_JOBS ))
if [[ "${LINES_PER_PART}" -lt 1 ]]; then LINES_PER_PART=1; fi

echo "Total lines: ${TOTAL_LINES}"
echo "Parallel jobs: ${PARALLEL_JOBS}"
echo "Lines per part: ${LINES_PER_PART}"

# Clean previous parts and split
rm -f "${LISTS_DIR}/part_"*.list 2>/dev/null || true
split -l "${LINES_PER_PART}" -d --additional-suffix=".list" "${URL_FILE}" "${LISTS_DIR}/part_"

PARTS=( "${LISTS_DIR}"/part_*.list )
PART_COUNT=${#PARTS[@]}
if [[ "${PART_COUNT}" -eq 0 ]]; then
  echo "Split failed: no parts generated."
  exit 1
fi
echo "Generated parts: ${PART_COUNT}"

# Prepare a joblog to summarize
JOBLOG_FILE="parallel_joblog.tsv"

# Run in parallel. We embed WARCS_DIR/LOGS_DIR/etc. values into the block to avoid env expansion issues.
# We do not halt on failures; each job logs to its own file and prints a tail on failure.
set +e
parallel -j "${PARALLEL_JOBS}" --lb --keep-order --joblog "${JOBLOG_FILE}" '
  base=$(basename {} .list)
  out="'"${WARCS_DIR}"'/warc_{#}_${base}"
  log="'"${LOGS_DIR}"'/job_{#}_${base}.log"
  echo "[START] {#} part={} (urls=$(wc -l < {})) -> ${out}.warc.gz"
  {
    echo "== Job {#} =="
    echo "Part file: {}"
    echo "Output WARC prefix: ${out}"
    echo "Wget flags: '"${WGET_FLAGS}"'"
    echo "Timeout flags: '"${TIMEOUT_FLAGS}"'"
    echo "WARC compression arg: '"${WARC_COMP_ARG:-<none>}"'"
    echo "Retries: '"${TRIES}"'"
    echo "First 3 URLs:"
    head -n 3 "{}"
    echo "---- wget begin ----"
  } > "${log}"

  wget '"${TIMEOUT_FLAGS}"' --tries='"${TRIES}"' \
       --warc-file="${out}" '"${WARC_COMP_ARG}"' \
       '"${WGET_FLAGS}"' -i "{}" -O /dev/null >> "${log}" 2>&1
  status=$?

  if [ "$status" -eq 0 ]; then
    echo "[DONE ] {#} -> ${out}.warc.gz"
  else
    echo "[FAIL ] {#} (exit $status) for {} -> ${out}.warc.gz; see ${log}" >&2
    echo "---- wget end (exit $status) ----" >> "${log}"
    # Show last lines to help debug
    tail -n 50 "${log}" >&2
  fi
  # Do not propagate error to GNU parallel, keep other jobs running
  exit 0
' ::: "${PARTS[@]}"
PARALLEL_RC=$?
set -e

echo "All jobs launched. Logs: ${LOGS_DIR}/"
echo "Summary joblog: ${JOBLOG_FILE}"
echo "WARC files: ${WARCS_DIR}/"

# Optional hint if failures occurred
if grep -q "\[FAIL \]" "${JOBLOG_FILE}" 2>/dev/null; then
  echo "Note: Some jobs reported failures; inspect the corresponding logs under ${LOGS_DIR}/"
fi