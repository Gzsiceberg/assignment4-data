from fastwarc.warc import ArchiveIterator, WarcRecordType
from cs336_data.extract_text import extract_warc
from rich.progress import track, Progress
from language_identification import detect_language
from quality_filters import gopher_quality_filter
import regex as re


def preprocess_text(text: str) -> str:
    text = text.replace("\n", " ").strip()
    text = re.sub(r"([.\!\?,'/()])", r"\1", text)
    text = text.lower()
    return text


def generate_fasttext_pos_data(
    warc_path: str, output_path: str, line_count: int, neg_count: int
) -> None:
    write_count = 0
    is_positive = neg_count == 0
    with open(warc_path, "rb") as f, open(output_path, "w", encoding="utf-8") as out_f:
        with Progress() as progress: 
            task_id = progress.add_task("Starting...", total=line_count)
            for record in ArchiveIterator(f):
                if not record.record_type == WarcRecordType.response:
                    continue
                payload = record.reader.read()
                text = extract_warc(payload)
                passed, _ = gopher_quality_filter(text)
                if not passed:
                    continue
                lang, confidence = detect_language(text)
                if lang != "en" or confidence < 0.8:
                    continue
                text = preprocess_text(text)
                label_text = f"{'__label__positive' if is_positive else '__label__negative'} {text}\n"
                out_f.write(label_text)
                write_count += 1
                progress.update(task_id, description=f"Generating {write_count} samples out of {line_count}...")
                if not is_positive and write_count >= neg_count:
                    break
    print(f"Finished writing {write_count} samples to {output_path}.")


if __name__ == "__main__":
    import argparse
    import sys

    argument_parser = argparse.ArgumentParser(
        description="Extract text from WARC file."
    )
    argument_parser.add_argument(
        "warc_path", type=str, help="Path to the input WARC file."
    )
    argument_parser.add_argument(
        "output_path", type=str, help="Path to the output text file."
    )
    argument_parser.add_argument("-n", "--negative", type=int, default=0, help="Generate negative samples.")
    args = argument_parser.parse_args(sys.argv[1:])
    warc_path = args.warc_path
    output_path = args.output_path
    is_positive = not bool(args.negative)

    with open(warc_path, "rb") as f:
        line_count: int = sum(1 for _ in ArchiveIterator(f))

    print(f"Total {"positive" if is_positive else "negative"} records in WARC: {line_count} ")
    generate_fasttext_pos_data(warc_path, output_path, line_count, args.negative)
