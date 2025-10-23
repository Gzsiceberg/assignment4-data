from nltk import word_tokenize
import regex as re

ELLIPSIS_LINE_RE = re.compile(r"(?m)^[^\n\r]*?(?:\s*(?:\.\.\.|…))\s*$")
LINE_START_RE = re.compile(r"(?m)^")


def more_than_30_percent_ellipsis_lines_regex(s: str) -> bool:
    if not s:
        return False

    total_lines = len(LINE_START_RE.findall(s))
    if total_lines == 0:
        return False

    ellipsis_lines = len(ELLIPSIS_LINE_RE.findall(s))
    return ellipsis_lines > 0.3 * total_lines


def gopher_quality_filter(text: str, word_limit: int = 50) -> bool:
    tokens = word_tokenize(text)
    if len(tokens) < word_limit:
        return False
    if len(tokens) > 100_000:
        return False
    
    regex = re.compile(r"[a-zA-Z]")
    count = 0
    word_length_sum = 0
    for token in tokens:
        if regex.search(token):
            count += 1
        word_length_sum += len(token)
    if count / len(tokens) < 0.8:
        return False
    avg_word_length = word_length_sum / len(tokens)
    if avg_word_length < 3 or avg_word_length > 10:
        return False
    
    if more_than_30_percent_ellipsis_lines_regex(text):
        return False
    
    return True
    


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
        result = gopher_quality_filter(text, word_limit=5)
        print(f"Text {i+1}: {'Passes' if result else 'Fails'} the quality filter.")

