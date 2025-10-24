from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse import encoding
from resiliparse.extract.html2text import extract_plain_text
from rich.progress import track


def extract_warc(content_bytes: bytes) -> str:
    encode = encoding.detect_encoding(content_bytes)
    try:
        content = content_bytes.decode(encode)
    except (UnicodeDecodeError, LookupError):
        if __file__ == "__main__":
            print(f"Failed to decode with {encode}, falling back to utf-8 with replacement.")
        content = content_bytes.decode("utf-8", errors="replace")
    text = extract_plain_text(content)
    return text


if __name__ == "__main__":
    import argparse
    import sys
    args = argparse.ArgumentParser(description="Extract text from WARC file.")
    args.add_argument("warc_path", type=str, help="Path to the input WARC file.")
    args.add_argument("output_path", type=str, help="Path to the output text file.")
    parsed_args = args.parse_args(sys.argv[1:])
    warc_path = parsed_args.warc_path
    output_path = parsed_args.output_path

    with open(warc_path, "rb") as f:
        line_count: int = sum(1 for _ in ArchiveIterator(f))
    print(f"Total WARC records: {line_count}")
    

