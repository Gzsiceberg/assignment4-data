from datasets import load_dataset
import datasets
from fastwarc import ArchiveIterator, WarcRecordType
from cs336_data.gen_fasttext import preprocess_text
from cs336_data.filter_CC.filter_01 import decode_content
import os
from cs336_data.language_identification import detect_language
from cs336_data.quality_filters import gopher_quality_filter

def gen_fasttext_pos_data(data):
    text = data["text"]
    text = preprocess_text(text)
    label_text = f"__label__positive {text}"
    return {"label_text": label_text}

def gen_fasttext_neg_data(warc_path: str, line_count: int, prob: float) -> list[str]:
    from rich.progress import Progress
    import random
    write_count = 0
    neg_sameples: list[str] = []
    random.seed(42)
    with open(warc_path, "rb") as f:
        with Progress() as progress: 
            task_id = progress.add_task("Starting...", total=line_count)
            for record in ArchiveIterator(f):
                progress.update(task_id, description=f"Generating {write_count} samples out of {line_count}...", advance=1)
                if not record.record_type == WarcRecordType.conversion:
                    continue

                # downsample
                if random.random() > prob:
                    continue

                content_bytes = record.reader.read()
                text = decode_content(content_bytes)
                text = preprocess_text(text)
                label_text = f"__label__negative {text}"
                neg_sameples.append(label_text)
                write_count += 1
    return neg_sameples

if __name__ == "__main__":
    import glob
    dataset = load_dataset("allenai/paloma", "c4_100_domains")
    valid_dataset: datasets.Dataset = dataset["val"] # type: ignore
    valid_dataset = valid_dataset.map(gen_fasttext_pos_data)
    pos_count = len(valid_dataset)
    print(f"Positive samples count: {pos_count}")

    # depend on filter_01 output
    warc_wets = glob.glob("data/filtered_01/*.warc.wet.gz")
    first_one = warc_wets[0]
    line_count = 0
    with open(first_one, "rb") as f:
        for record in ArchiveIterator(f):
            line_count += 1
    estimated_line_count = int(line_count * 1.05)

    prob = pos_count * 2 / estimated_line_count
    prob /= len(warc_wets)
    print(f"estimated line count per file: {estimated_line_count}, prob: {prob}")

    neg_samples: list[str] = []
    for warc_wet in warc_wets:
        temp_neg_samples = gen_fasttext_neg_data(warc_wet, line_count=estimated_line_count, prob=prob)
        neg_samples.extend(temp_neg_samples)
    total_samples = pos_count + len(neg_samples)
    ratio = len(neg_samples) / pos_count
    print(f"samples count: {total_samples} (pos: {pos_count}, neg: {len(neg_samples)}) ratio: {ratio:.4f}")

    # output_path = "data/filter_CC/cc_pos.txt"
    # dir_path = os.path.dirname(output_path)
    # os.makedirs(dir_path, exist_ok=True)
    # with open(output_path, "w", encoding="utf-8") as out_f:
    #     for item in valid_dataset:
    #         out_f.write(item["label_text"] + "\n")
    # print(f"Finished writing {len(valid_dataset)} samples to {output_path}.")