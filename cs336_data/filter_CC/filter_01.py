import concurrent.futures
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
                filter_counter[f"03_quality_{str(reason)}"] += 1
                continue

            filter_counter["04_passed"] += 1

            url: str = record.headers.get("WARC-Target-URI", "unknown")  # type: ignore
            rec = Record(
                url=url,
                recoder_id=record_id,
                content=text,
            )
            write_record(writer, rec)

    return output_path, filter_counter


if __name__ == "__main__":
    import glob
    from rich import print

    # Set up the executor
    num_cpus = len(os.sched_getaffinity(0))
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_cpus)
    wet_filepaths = glob.glob("data/warc_wets/*.warc.wet.gz")
    random.seed(42)
    random.shuffle(wet_filepaths)
    wet_filepaths = wet_filepaths[:100]  # For testing, limit to first 100 files
    output_directory_path = "data/filtered_01/"
    print(f"Processing {len(wet_filepaths)} WET files using {num_cpus} CPUs.")
    os.makedirs(output_directory_path, exist_ok=True)

    futures = []
    for wet_filepath in wet_filepaths:
        # For each warc.wet.gz filepath, submit a job to the executor and get a future back
        wet_filename = str(pathlib.Path(wet_filepath).name)
        future = executor.submit(
            process_single_wet_file,
            wet_filepath,
            os.path.join(output_directory_path, wet_filename),
        )
        # Store the futures
        futures.append(future)

    # Iterate over the completed futures as they finish, using a progress bar
    # to keep track of progress.
    filter_counter = defaultdict(int)
    for future in tqdm(
        concurrent.futures.as_completed(futures),
        total=len(wet_filepaths),
    ):
        output_file, future_filter_counter = future.result()
        print(f"Output file written: {output_file}")
        for key, value in future_filter_counter.items():
            filter_counter[key] += value

    print("Final filter counts:")
    total = filter_counter["01_total"]
    for key, value in filter_counter.items():
        print(f"{key}: {value} ({value/total:.2%})")
