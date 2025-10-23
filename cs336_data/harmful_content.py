import fasttext

global_models: dict = {}

def _get_model(model_name: str):
    global global_models
    if model_name not in global_models:
        if model_name == "nsfw":
            global_models[model_name] = fasttext.load_model("model/jigsaw_fasttext_bigrams_nsfw_final.bin")
        elif model_name == "toxicity":
            global_models[model_name] = fasttext.load_model("model/jigsaw_fasttext_bigrams_hatespeech_final.bin")
        else:
            raise ValueError(f"Unknown model name: {model_name}")
    return global_models[model_name]

def classify_nsfw(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = _get_model("nsfw")
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return label, confidence


def classify_toxicity(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = _get_model("toxicity")
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return label, confidence


if __name__ == "__main__":
    from fastwarc.warc import ArchiveIterator, WarcRecordType
    from rich.progress import track
    import random
    from extract_text import extract_warc
    warc_path = "data/CC-MAIN-20250417135010-20250417165010-00065.warc.gz"
    count = 0
    harmful_count = 0
    total_count = 0
    with open(warc_path, "rb") as f:
        for i, record in enumerate(track(ArchiveIterator(f))):
            if record.record_type != WarcRecordType.response:
                continue
            total_count += 1
            payload = record.reader.read()
            text = extract_warc(payload)
            label, conf = classify_nsfw(text)
            if label == "nsfw" and conf > 0.8:
                harmful_count += 1
            if label == "nsfw" and count < 20:
                print("-" * 80)
                print(f"RecordID: {record.record_id} - NSFW Confidence: {conf:.4f}")
                text = text.replace("\n", " ")
                print(text[:500])
                count += 1
                continue
            label, conf = classify_toxicity(text)
            if label == "toxic" and conf > 0.8:
                harmful_count += 1
            if label == "toxic" and count < 20:
                print("-" * 80)
                print(f"RecordID: {record.record_id} - Toxic Confidence: {conf:.4f}")
                text = text.replace("\n", " ")
                print(text[:500])
                count += 1
            if total_count >= 10000:
                break
    print(f"Processed {total_count} records, harmful_count={harmful_count}  percent={harmful_count / total_count * 100:.5f}%")