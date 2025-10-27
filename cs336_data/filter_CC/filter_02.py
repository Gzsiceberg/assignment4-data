import concurrent.futures
import os
import pathlib
import random
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse import encoding
from collections import defaultdict
from warcio.warcwriter import WARCWriter
from dataclasses import dataclass


@dataclass
class Record:
    url: str
    recoder_id: str
    content: str


global_model = None

def predict_c4_like(text: str) -> tuple[str, float]:
    from cs336_data.gen_fasttext import preprocess_text
    import fasttext
    text = preprocess_text(text)
    global global_model
    if global_model is None:
        global_model = fasttext.load_model("model/qc_model.bin")
    model = global_model
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return "c4" if label == "positive" else "cc", confidence


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

    return output_path, filter_counter


if __name__ == "__main__":
    import glob
    from rich import print

    wet_filepaths = glob.glob("data/filtered_01/*.warc.wet.gz")
    num_cpus = min(len(os.sched_getaffinity(0)), int(len(wet_filepaths) / 2))
    # Set up the executor
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_cpus)
    random.seed(42)
    random.shuffle(wet_filepaths)
    output_directory_path = "data/filtered_02/"
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
