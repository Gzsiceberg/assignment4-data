from transformers import AutoTokenizer
from datasets import load_dataset
import numpy as np
import datasets
import os
import gc
from rich.progress import track
from rich import print
from fastwarc.warc import ArchiveIterator, WarcRecordType

tokenizer = AutoTokenizer.from_pretrained("gpt2")


def tokenize_line_and_add_eos(line):
    # 添加 verbose=False 来禁用长度警告，因为我们只是tokenize，不会输入模型
    return tokenizer.encode(line, verbose=False) + [tokenizer.eos_token_id]

def tokenize_lines(lines):
    all_tokens = []
    for line in lines:
        tokens = tokenize_line_and_add_eos(line)
        all_tokens.extend(tokens)
    return all_tokens


def gen_c4_100():
    dataset = load_dataset("allenai/paloma", "c4_100_domains")
    valid_dataset: datasets.Dataset = dataset["val"]  # type: ignore

    sum_tokens = 0
    count = 0
    max_samples = 100
    for t, data in track(enumerate(valid_dataset), total=max_samples):
        text: str = data["text"] # type: ignore
        lines = text.splitlines(keepends=True)
        tokens = tokenize_lines(lines)
        sum_tokens += len(tokens)
        count += 1
        if t >= max_samples - 1:
            break
    estimated_tokens = int(sum_tokens * (len(valid_dataset) / count) * 1.2)
    print(f"Estimated tokens: {estimated_tokens:,}")

    output_file = "data/filter_CC/gen_tokens/c4_100_val.npy"
    dir_output = os.path.dirname(output_file)
    os.makedirs(dir_output, exist_ok=True)
    mm = np.memmap(output_file, dtype=np.uint16, mode="w+", shape=(estimated_tokens,))

    idx: int = 0
    for data in track(valid_dataset, total=len(valid_dataset)):
        text = data["text"] # type: ignore
        lines = text.splitlines(keepends=True)
        tokens = tokenize_lines(lines)
        mm[idx : idx + len(tokens)] = tokens
        idx += len(tokens)
    print(f"Total tokens written: {idx:,}")

    mm.flush()
    del mm
    gc.collect()
    token_count: int = idx

    pre_size = os.path.getsize(output_file)
    # Truncate the file to the real size (2 bytes per token)
    with open(output_file, "r+b") as f:
        f.truncate(token_count * 2)
    after_size = os.path.getsize(output_file)
    print(f"Truncated file from {pre_size:,} bytes to {after_size:,} bytes.")


def process_single_wet_file(input_path: str):
    from cs336_data.filter_CC.filter_01 import decode_content
    with open(input_path, "rb") as infile:
        for record in ArchiveIterator(infile):
            if record.record_type != WarcRecordType.conversion:
                continue
            record_id = record.record_id
            content_bytes = record.reader.read()
            text = decode_content(content_bytes)
            lines = text.splitlines(keepends=True)
            yield record_id, lines


def gen_cc(input_path: str):
    import glob
    import multiprocessing
    wet_filepaths = glob.glob(f"{input_path}/*.warc.wet.gz")
    output_file = "data/filter_CC/gen_tokens/cc_tokens.npy"
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)

    sample_file_count = min(10, len(wet_filepaths))
    sum_tokens = 0
    for wet_filepath in wet_filepaths[:sample_file_count]:
        for record_id, lines in process_single_wet_file(wet_filepath):
            tokens = tokenize_lines(lines)
            sum_tokens += len(tokens)
    average_tokens_per_file = sum_tokens / sample_file_count
    estimated_tokens = int(average_tokens_per_file * len(wet_filepaths) * 1.2)
    print(f"Estimated tokens: {estimated_tokens:,}")

    dir_output = os.path.dirname(output_file)
    os.makedirs(dir_output, exist_ok=True)
    mm = np.memmap(output_file, dtype=np.uint16, mode="w+", shape=(estimated_tokens,))
    # pool = multiprocessing.Pool(multiprocessing.cpu_count())

    idx = 0
    for wet_filepath in wet_filepaths:
        for record_id, lines in process_single_wet_file(wet_filepath):
            tokens = tokenize_lines(lines)
            mm[idx : idx + len(tokens)] = tokens
            idx += len(tokens)
    print(f"Total tokens written: {idx:,}")

    mm.flush()
    del mm
    gc.collect()
    token_count: int = idx

    pre_size = os.path.getsize(output_file)
    # Truncate the file to the real size (2 bytes per token)
    with open(output_file, "r+b") as f:
        f.truncate(token_count * 2)
    after_size = os.path.getsize(output_file)
    print(f"Truncated file from {pre_size:,} bytes to {after_size:,} bytes.")


if __name__ == "__main__":
    import argparse

    argparse = argparse.ArgumentParser()
    argparse.add_argument("--c4", action='store_true')
    argparse.add_argument("--cc", action='store_true')
    args = argparse.parse_args()

    if args.c4:
        gen_c4_100()
    
    if args.cc:
        gen_cc("data/filtered_01_by_model/")





