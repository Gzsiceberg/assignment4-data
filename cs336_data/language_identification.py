import fasttext

from cs336_data.extract_text import extract_warc
def detect_language(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = fasttext.load_model("model/lid.176.bin")
    output = model.predict(text)
    lang = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return lang, confidence


if __name__ == "__main__":
    sample_text = "Bonjour tout le monde"
    lang, confidence = detect_language(sample_text)
    print(f"Detected language: {lang} with confidence {confidence}")

    from fastwarc.warc import ArchiveIterator, WarcRecordType
    from rich.progress import track
    warc_path = "data/CC-MAIN-20250417135010-20250417165010-00065.warc.gz"
    count = 0
    is_english = 0
    total_records = 0
    with open(warc_path, "rb") as f:
        for i, record in enumerate(track(ArchiveIterator(f))):
            if record.record_type != WarcRecordType.response:
                continue
            payload = record.reader.read()
            text = extract_warc(payload)
            lang, confidence = detect_language(text)
            total_records += 1
            if lang == "en":
                is_english += 1
            if count < 20:
                print("-" * 80)
                print(f"RecordID: {record.record_id} - Detected language: {lang} with confidence {confidence}")
                text = text.replace("\n", " ")
                print(text[:200])
                count += 1
            if total_records >= 10000:
                break
    print(f"Total records: {total_records}, English records: {is_english} percentage: {is_english / total_records:.2%}")