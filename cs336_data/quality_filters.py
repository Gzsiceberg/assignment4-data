from nltk import word_tokenize
import regex as re
import enum

ELLIPSIS_LINE_RE = re.compile(r"(?m)^[^\n\r]*?(?:\s*(?:\.\.\.|…))\s*$")
LINE_START_RE = re.compile(r"(?m)^")


class Reason(enum.Enum):
    TOO_SHORT = "Too Short"
    TOO_LONG = "Too Long"
    LOW_ALPHABETIC_CONTENT = "Low Alphabetic Content"
    AVG_WORD_LENGTH_OUT_OF_BOUNDS = "Average Word Length Out of Bounds"
    TOO_MANY_ELLIPSIS_LINES = "Too Many Ellipsis Lines"
    Ok = "Ok"



def more_than_30_percent_ellipsis_lines_regex(s: str) -> bool:
    if not s:
        return False

    total_lines = len(LINE_START_RE.findall(s))
    if total_lines == 0:
        return False

    ellipsis_lines = len(ELLIPSIS_LINE_RE.findall(s))
    return ellipsis_lines > 0.3 * total_lines


def gopher_quality_filter(text: str, word_limit: int = 50) -> tuple[bool, Reason]:
    tokens = word_tokenize(text)
    total_tokens = len(tokens)
    if total_tokens < word_limit:
        return False, Reason.TOO_SHORT
    if total_tokens > 100_000:
        return False, Reason.TOO_LONG
    
    regex = re.compile(r"[a-zA-Z]")
    count = 0
    word_length_sum = 0
    for token in tokens:
        if regex.search(token):
            count += 1
        word_length_sum += len(token)
    if count / total_tokens < 0.8:
        return False, Reason.LOW_ALPHABETIC_CONTENT
    avg_word_length = word_length_sum / total_tokens
    if avg_word_length < 3 or avg_word_length > 10:
        return False, Reason.AVG_WORD_LENGTH_OUT_OF_BOUNDS
    
    if more_than_30_percent_ellipsis_lines_regex(text):
        return False, Reason.TOO_MANY_ELLIPSIS_LINES
    
    return True, Reason.Ok
    


if __name__ == "__main__":
    from rich import print

    texts = [
        "This is a normal text with enough content and no issues.",
        "...\n...\n...\nThis text has too many ellipsis lines.\n...\n...\n...",
        "Short text.",
        "A" * 200_000,
        "这是一些中文文本，没有足够的英文内容。",
        "the be " * 100
    ]
    for i, text in enumerate(texts):
        passed, _ = gopher_quality_filter(text, word_limit=5)
        print(f"Text {i+1}: {'Passes' if passed else 'Fails'} the quality filter.")
        from fastwarc.warc import ArchiveIterator, WarcRecordType

    from rich.progress import track
    import random
    from extract_text import extract_warc
    warc_path = "data/CC-MAIN-20250417135010-20250417165010-00065.warc.gz"
    low_quality_count = 0
    total_count = 0
    print_count = 0
    with open(warc_path, "rb") as f:
        for i, record in enumerate(track(ArchiveIterator(f))):
            if record.record_type != WarcRecordType.response:
                continue
            total_count += 1
            payload = record.reader.read()
            text = extract_warc(payload)
            passed, reason = gopher_quality_filter(text)
            if not passed:
                low_quality_count += 1
                if print_count < 20 and reason == Reason.TOO_MANY_ELLIPSIS_LINES:
                    print("-" * 80)
                    print(f"RecordID: {record.record_id} - Low Quality: {reason}")
                    text = text.replace("\n", " ")
                    print(text[:10000])
                    print_count += 1
            if total_count >= 10000:
                break
    print(f"Processed {total_count} records, low_quality_count={low_quality_count}  percent={low_quality_count / total_count * 100:.5f}%")

