#!/usr/bin/env bash

INPUT_FILE="enwiki-20240420-extracted_urls.txt"
OUTPUT_FILE="subsampled_urls.txt"
P=0.001
if [ -n "$1" ]; then
    INPUT_FILE="$1"
fi
if [ -n "$2" ]; then
    OUTPUT_FILE="$2"
fi
if [ -n "$3" ]; then
    P="$3"
fi
awk -v p="$P" 'BEGIN{srand()} rand()<p' "$INPUT_FILE" > "$OUTPUT_FILE"