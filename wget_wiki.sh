#!/bin/bash
input_file="subsample_urls_h100.txt"
output_warc="subsampled_positive_urls.warc.gz"

# get input file from command line argument if provided
if [ $# -ge 1 ]; then
    input_file="$1"
fi

# get output warc file from command line argument if provided
if [ $# -ge 2 ]; then
    output_warc="$2"
fi

wget2 --input-file="$input_file" \
    --warc-file="$output_warc" \
    --warc-compression=gzip \
    --http2 \
    --max-threads=32 \
    --max-threads-per-host=4 \
    --tries=1 \
    --dns-timeout=3 \
    --connect-timeout=3 \
    --read-timeout=5 \
    -O /dev/null \
    -q