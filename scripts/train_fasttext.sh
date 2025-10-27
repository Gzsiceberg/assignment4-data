#!/usr/bin/env bash
INPUT="data/filter_CC/qc_fasttext_tr.txt"
OUTPUT="model/qc_model"
VAL_DATA="data/filter_CC/qc_fasttext_val.txt"
./fasttext supervised -input $INPUT -output $OUTPUT -autotune-validation $VAL_DATA