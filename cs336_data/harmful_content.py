import fasttext

def classify_nsfw(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = fasttext.load_model("model/jigsaw_fasttext_bigrams_nsfw_final.bin")
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return label, confidence


def classify_toxicity(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = fasttext.load_model("model/jigsaw_fasttext_bigrams_hatespeech_final.bin")
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return label, confidence