import concurrent.futures
import gc
import os
import pathlib
import random
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse import encoding
from collections import defaultdict
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from dataclasses import dataclass
import numpy as np
from multiprocessing import shared_memory
import multiprocessing


@dataclass
class Record:
    url: str
    recoder_id: str
    content: str


def decode_content(content_bytes: bytes) -> str:
    encode = encoding.detect_encoding(content_bytes)
    try:
        text = content_bytes.decode(encode)
    except (UnicodeDecodeError, LookupError):
        if __file__ == "__main__":
            print(
                f"Failed to decode with {encode}, falling back to utf-8 with replacement."
            )
        text = content_bytes.decode("utf-8", errors="replace")
    return text


def write_record(writer: WARCWriter, record: Record):
    from io import BytesIO

    content_bytes = record.content.encode("utf-8")
    r = writer.create_warc_record(
        record.url, record_type="conversion", payload=BytesIO(content_bytes)
    )
    writer.write_record(r)


def process_single_wet_file(input_path: str, output_path: str):
    from cs336_data.language_identification import detect_language
    from cs336_data.mask_pii import mask_email, mask_phone_numbers, mask_ip_addresses
    from cs336_data.harmful_content import classify_nsfw, classify_toxicity
    from cs336_data.quality_filters import gopher_quality_filter

    filter_counter = defaultdict(int)
    with open(input_path, "rb") as infile, open(output_path, "wb") as warc_stream:
        writer = WARCWriter(warc_stream, gzip=True)
        for i, record in enumerate(ArchiveIterator(infile)):
            if record.record_type != WarcRecordType.conversion:
                continue
            record_id = record.record_id
            content_bytes = record.reader.read()
            text = decode_content(content_bytes)

            filter_counter["01_total"] += 1
            lang, confidence = detect_language(text)
            if lang != "en" or confidence < 0.8:
                filter_counter["02_language"] += 1
                continue

            """
            do we need to mask PII when I only care about validation loss?
            """
            # text, _ = mask_email(text)
            # text, _ = mask_phone_numbers(text)
            # text, _ = mask_ip_addresses(text)

            """
            do we need to filter harmful content when I only care about validation loss?
            """
            # nsfw, conf = classify_nsfw(text)
            # if nsfw == "nsfw" and conf > 0.8:
            #     filter_counter["nsfw"] += 1
            #     continue

            # toxicity, conf = classify_toxicity(text)
            # if toxicity == "toxic" and conf > 0.8:
            #     filter_counter["toxic"] += 1
            #     continue

            passed, reason = gopher_quality_filter(text)
            if not passed:
                filter_counter[f"03_quality"] += 1
                continue

            filter_counter["04_filter_passed"] += 1

            url: str = record.headers.get("WARC-Target-URI", "unknown")  # type: ignore
            rec = Record(
                url=url,
                recoder_id=record_id,
                content=text,
            )
            write_record(writer, rec)

    return output_path, filter_counter


def exact_line_dedup_preprocess_shared(
    input_path: str, max_lines: int, shm_name: str
) -> int:
    stats_count: int = 0
    
    # 连接到已存在的共享内存
    shm = shared_memory.SharedMemory(name=shm_name)
    # 创建numpy数组视图，指向共享内存
    hash_counter = np.ndarray((max_lines,), dtype=np.int8, buffer=shm.buf)
    
    with open(input_path, "rb") as file:
        for record in ArchiveIterator(file):
            if record.record_type != WarcRecordType.conversion:
                continue
            content_bytes = record.reader.read()
            text = decode_content(content_bytes)
            lines = text.splitlines()
            
            for line in lines:
                hash_index = hash(line) % max_lines
                current_val = hash_counter[hash_index]
                if current_val < 10:
                    hash_counter[hash_index] = current_val + 1
                stats_count += 1
    shm.close()
    return stats_count


def exact_line_deduplication_single_file(
    input_path: str, output_path: str, shm_name: str, max_lines: int
):
    filter_counter = defaultdict(int)
    
    # 连接到共享内存
    shm = shared_memory.SharedMemory(name=shm_name)
    hash_counter = np.ndarray((max_lines,), dtype=np.int8, buffer=shm.buf)
    
    try:
        with open(input_path, "rb") as infile, open(output_path, "wb") as outfile:
            writer = WARCWriter(outfile, gzip=True)
            for record in ArchiveIterator(infile):
                if record.record_type != WarcRecordType.conversion:
                    continue
                content_bytes = record.reader.read()
                text = decode_content(content_bytes)
                lines = text.splitlines()
                filter_counter["dedup_total"] += 1

                deduped_lines = []
                for line in lines:
                    hash_index = hash(line) % max_lines
                    if hash_counter[hash_index] == 1:
                        deduped_lines.append(line)
                deduped_text = "\n".join(deduped_lines)

                if not deduped_text.strip():
                    filter_counter["dedup_filtered"] += 1
                    continue
                filter_counter["dedup_passed"] += 1

                record_id = record.record_id
                url: str = record.headers.get("WARC-Target-URI", "unknown")  # type: ignore
                rec = Record(
                    url=url,
                    recoder_id=record_id,
                    content=deduped_text,
                )
                write_record(writer, rec)
    except Exception as e:
        print(f"Error processing {input_path}: {e}")
    finally:
        shm.close()
    
    return filter_counter


def filter(
    wet_filepaths: list[str],
    executor: concurrent.futures.ProcessPoolExecutor,
    output_path: str,
):
    futures = []
    for wet_filepath in wet_filepaths:
        # For each warc.wet.gz filepath, submit a job to the executor and get a future back
        wet_filename = os.path.basename(wet_filepath)
        future = executor.submit(
            process_single_wet_file,
            wet_filepath,
            os.path.join(output_path, wet_filename),
        )
        # Store the futures
        futures.append(future)

    # Iterate over the completed futures as they finish, using a progress bar
    # to keep track of progress.
    filter_counter: dict[str, int] = defaultdict(int)
    for future in tqdm(
        concurrent.futures.as_completed(futures),
        total=len(wet_filepaths),
    ):
        output_file, future_filter_counter = future.result()
        for key, value in future_filter_counter.items():
            filter_counter[key] += value

    print("Final filter counts:")
    total = max(filter_counter.values())
    for key, value in filter_counter.items():
        print(f"{key}: {value} ({value/total:.2%})")


def dedup(
    executor: concurrent.futures.ProcessPoolExecutor,
    input_path: str,
    output_path: str,
    limit: int = 10000,
):
    all_input_files = glob.glob(os.path.join(input_path, "*.warc.wet.gz"))[:limit]
    
    print("Aggregating line counts for deduplication...")
    max_lines = 1000_000_000
    
    nbytes = max_lines * np.dtype(np.int8).itemsize
    print(f"Creating shared memory of size {nbytes / (1024**3):.2f} GB...")
    shm = shared_memory.SharedMemory(create=True, size=nbytes)
    
    try:
        hash_counter = np.ndarray((max_lines,), dtype=np.int8, buffer=shm.buf)
        hash_counter[:] = 0  # 初始化为0
        
        futures = []
        for file_path in all_input_files:
            future = executor.submit(
                exact_line_dedup_preprocess_shared, file_path, max_lines, shm.name
            )
            futures.append(future)

        total = len(all_input_files)
        total_lines = 0
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=total,
            desc="Phase 1: Counting lines",
        ):
            total_lines += future.result()

        print(f"Total lines processed: {total_lines:,}")
        
        # 打印统计信息
        count_zero = np.sum(hash_counter == 0)
        count_one = np.sum(hash_counter == 1)
        count_more = np.sum(hash_counter > 1)
        total_counts = len(hash_counter)
        print(f"Hash counts distribution:")
        print(f"  Zero count: {count_zero:,} ({count_zero/total_counts:.2%})")
        print(f"  One count: {count_one:,} ({count_one/total_counts:.2%})")
        print(f"  More than one count: {count_more:,} ({count_more/total_counts:.2%})")

        # 第二阶段：使用共享内存进行去重
        print("Starting deduplication phase...")
        futures = []
        os.makedirs(output_path, exist_ok=True)
        for file_path in all_input_files:
            wet_filename = os.path.basename(file_path)
            deduped_output_path = os.path.join(output_path, wet_filename)
            future = executor.submit(
                exact_line_deduplication_single_file,
                file_path,
                deduped_output_path,
                shm.name,  # 传递共享内存名称
                max_lines,
            )
            futures.append(future)

        filter_counter: dict[str, int] = defaultdict(int)
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(all_input_files),
            desc="Phase 2: Deduplicating",
        ):
            counter = future.result()
            for key, value in counter.items():
                filter_counter[key] += value

        print("Final dedup counts:")
        total = max(filter_counter.values())
        for key, value in filter_counter.items():
            print(f"{key}: {value:,} ({value/total:.2%})")
    except Exception as e:
        print(f"Error during deduplication: {e}")
    finally:
        # 清理共享内存
        shm.close()
        shm.unlink()
        print("Shared memory cleaned up.")


global_model = None


def predict_c4_like(text: str) -> tuple[str, float]:
    from cs336_data.gen_fasttext import preprocess_text
    import fasttext

    text = preprocess_text(text)
    global global_model
    if global_model is None:
        dir_path = os.path.dirname(os.path.abspath(__file__))
        global_model = fasttext.load_model(
            os.path.join(dir_path, "../../model/qc_model.bin")
        )
    model = global_model
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")  # type: ignore
    confidence = output[1][0]
    return "c4" if label == "positive" else "cc", confidence


def process_single_wet_file_by_model(input_path: str, output_path: str):
    filter_counter = defaultdict(int)
    with open(input_path, "rb") as infile, open(output_path, "wb") as warc_stream:
        writer = WARCWriter(warc_stream, gzip=True)
        for i, record in enumerate(ArchiveIterator(infile)):
            if record.record_type != WarcRecordType.conversion:
                continue
            record_id = record.record_id
            content_bytes = record.reader.read()
            text = decode_content(content_bytes)

            filter_counter["01_total"] += 1
            pred_label, confidence = predict_c4_like(text)
            if pred_label != "c4":
                filter_counter["02_filtered_out"] += 1
                continue

            filter_counter["03_passed"] += 1

            url: str = record.headers.get("WARC-Target-URI", "unknown")  # type: ignore
            rec = Record(
                url=url,
                recoder_id=record_id,
                content=text,
            )
            write_record(writer, rec)
    return filter_counter


def filter_by_model(
    input_deduped: str,
    executor: concurrent.futures.ProcessPoolExecutor,
    output_path: str,
    limit: int = 10000,
):
    wet_filepaths = glob.glob(f"{input_deduped}/*.warc.wet.gz")[:limit]
    os.makedirs(output_path, exist_ok=True)
    futures = []
    for wet_filepath in wet_filepaths:
        wet_filename = str(pathlib.Path(wet_filepath).name)
        future = executor.submit(
            process_single_wet_file_by_model,
            wet_filepath,
            os.path.join(output_path, wet_filename),
        )
        futures.append(future)

    filter_counter = defaultdict(int)
    for future in tqdm(
        concurrent.futures.as_completed(futures),
        total=len(wet_filepaths),
    ):
        future_filter_counter = future.result()
        for key, value in future_filter_counter.items():
            filter_counter[key] += value

    print("Final filter counts:")
    total = max(filter_counter.values())
    for key, value in filter_counter.items():
        print(f"{key}: {value} ({value/total:.2%})")


if __name__ == "__main__":
    import glob
    from rich import print
    import argparse
    import time

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--filter", action="store_true", help="Whether to apply filtering"
    )
    arg_parser.add_argument(
        "--dedup", action="store_true", help="Whether to apply deduplication"
    )
    arg_parser.add_argument(
        "--by_model", action="store_true", help="Whether to apply filtering by model"
    )
    arg_parser.add_argument(
        "-m", "--max_workers", type=int, default=32, help="Maximum number of worker processes"
    )
    arg_parser.add_argument(
        "--limit",
        type=int,
        default=100000,
        help="Limit the number of WET files to process",
    )
    args = arg_parser.parse_args()

    random.seed(42)
    wet_filepaths = glob.glob("data/warc_wets/*.warc.wet.gz")
    random.shuffle(wet_filepaths)
    wet_filepaths = wet_filepaths[: args.limit]
    num_cpus = min(len(os.sched_getaffinity(0)), int(len(wet_filepaths) / 2))
    # Set up the executor
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_cpus)
    output_directory_path = "data/filtered_01/"
    output_directory_path_dedup = "data/filtered_01_deduped/"
    output_directory_path_by_model = "data/filtered_01_by_model/"
    print(f"Processing {len(wet_filepaths)} WET files using {num_cpus} CPUs.")
    os.makedirs(output_directory_path, exist_ok=True)

    if args.filter:
        start_time = time.time()
        filter(wet_filepaths, executor, output_directory_path)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(
            f"Filtering took {elapsed_time:.2f} seconds. Throughput: {len(wet_filepaths)/elapsed_time:.2f} WET files/second."
        )
        """
        Final filter counts:
        01_total: 2348614 (100.00%)
        02_language: 1888850 (80.42%)
        04_filter_passed: 343384 (14.62%)
        03_quality: 116380 (4.96%)
        Filtering took 310.87 seconds. Throughput: 0.32 WET files/second.
        """

    if args.dedup:
        start_time = time.time()
        dedup(
            executor,
            output_directory_path,
            output_directory_path_dedup,
            limit=args.limit,
        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(
            f"Deduplication took {elapsed_time:.2f} seconds. Throughput: {len(wet_filepaths)/elapsed_time:.2f} WET files/second."
        )
        """
        Creating shared memory of size 0.93 GB...
        Phase 1: Counting lines: 100%|________________________________________________________________________________________________________| 1000/1000 [01:36<00:00, 10.39it/s]
        Total lines processed: 476,482,657
        Hash counts distribution:
        Zero count: 919,807,499 (91.98%)
        One count: 62,684,168 (6.27%)
        More than one count: 17,508,333 (1.75%)
        Starting deduplication phase...
        Phase 2: Deduplicating: 100%|_________________________________________________________________________________________________________| 1000/1000 [02:44<00:00,  6.08it/s]
        Final dedup counts:
        dedup_total: 3,435,127 (100.00%)
        dedup_passed: 3,118,104 (90.77%)
        dedup_filtered: 317,023 (9.23%)
        Shared memory cleaned up.
        Deduplication took 263.37 seconds. Throughput: 5.32 WET files/second.
        """

    if args.by_model:
        start_time = time.time()
        filter_by_model(
            output_directory_path_dedup,
            executor,
            output_directory_path_by_model,
            limit=args.limit,
        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(
            f"Filtering by model took {elapsed_time:.2f} seconds. Throughput: {len(wet_filepaths)/elapsed_time:.2f} WET files/second."
        )
