from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse import encoding
from resiliparse.extract.html2text import extract_plain_text
from rich.progress import track


def extract_warc(content_bytes: bytes) -> str:
    encode = encoding.detect_encoding(content_bytes)
    try:
        content = content_bytes.decode(encode)
    except (UnicodeDecodeError, LookupError):
        print(f"Failed to decode with {encode}, falling back to utf-8 with replacement.")
        content = content_bytes.decode("utf-8", errors="replace")
    text = extract_plain_text(content)
    return text



if __name__ == "__main__":
    import sys

    warc_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(warc_path, "rb") as f:
        with open(output_path, "w", encoding="utf-8") as out_f:
            for i, record in enumerate(track(ArchiveIterator(f))):
                if record.record_type == WarcRecordType.request:
                    out_f.write(f"Date: {record.record_date}\n")
                    out_f.write(f"RecordID: {record.record_id}\n")
                if record.record_type == WarcRecordType.response:
                    payload = record.reader.read()
                    text = extract_warc(payload)
                    out_f.write("\n")
                    out_f.write(text)
                    if i > 10000:
                        break

