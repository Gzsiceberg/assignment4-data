#!/usr/bin/env bash

POS_WARC="data/t.warc"
POS_DATA="data/wiki_ft_pos.dat"
# check POS_DATA exists
if [ ! -f "$POS_DATA" ]; then
    echo "Positive data file $POS_DATA does not exist. Generating..."
    uv run cs336_data/gen_fasttext.py $POS_WARC $POS_DATA
    pos_count=$(wc -l < "$POS_DATA")
else
    pos_count=$(wc -l < "$POS_DATA")
    echo "Positive data file $POS_DATA already exists with $pos_count lines. Skipping generation."
fi

NEG_WARC="data/CC-MAIN-20250417135010-20250417165010-00065.warc.gz"
NEG_DATA="data/wiki_ft_neg.dat"
# check NEG_DATA exists
if [ ! -f "$NEG_DATA" ]; then
    echo "Negative data file $NEG_DATA does not exist. Generating..."
    # count lines in NEG_WARC
    uv run cs336_data/gen_fasttext.py $NEG_WARC $NEG_DATA -n $pos_count
    neg_count=$(wc -l < "$NEG_DATA")
else
    neg_count=$(wc -l < "$NEG_DATA")
    echo "Negative data file $NEG_DATA already exists with $neg_count lines. Skipping generation."
fi

if [ "$pos_count" -ne "$neg_count" ]; then
    echo "Warning: Positive data has $pos_count lines, but negative data has $neg_count lines."
    head -n $neg_count "$POS_DATA" > "data/wiki_ft_pos_trimmed.dat"
    mv "data/wiki_ft_pos_trimmed.dat" "$POS_DATA"
    echo "Trimmed positive data to $neg_count lines."
fi

merge_file="data/wiki_ft_input.txt"
echo "Merging positive and negative data into $merge_file"
cat "$POS_DATA" "$NEG_DATA" | shuf > "$merge_file"
echo "Merged data has $(wc -l < "$merge_file") lines."

# split into train and test
train_file="data/wiki_ft_train.txt"
test_file="data/wiki_ft_test.txt"
train_ratio=0.8
total_lines=$(wc -l < "$merge_file")
train_lines=$(printf "%.0f" "$(echo "$total_lines * $train_ratio" | bc -l)")
test_lines=$((total_lines - train_lines))
echo "Splitting data into train ($train_lines lines) and test ($test_lines lines)"
head -n "$train_lines" "$merge_file" > "$train_file"
tail -n "$test_lines" "$merge_file" > "$test_file"
echo "Training data saved to $train_file with $(wc -l < "$train_file") lines."
echo "Testing data saved to $test_file with $(wc -l < "$test_file") lines."