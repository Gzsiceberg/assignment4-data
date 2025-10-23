import fasttext
def detect_language(text: str) -> tuple[str, float]:
    text = text.replace("\n", " ").strip()
    model = fasttext.load_model("model/lid.176.bin")
    output = model.predict(text)
    lang = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return lang, confidence


if __name__ == "__main__":
    # sample_text = "This is a sample English text."
    # lang, confidence = detect_language(sample_text)
    # print(f"Detected language: {lang} (Confidence: {confidence})")

    smaple_text = 'Herman Melville - Moby-Dick\n\nAvailing himself of the mild, summer-cool weather that now reigned in these latitudes,...airing, or new shaping their various weapons and boat furniture.\n\nThis is a test paragraph with a link.\n\n  • Novel'
    lang, confidence = detect_language(smaple_text)
    print(f"Detected language: {lang} (Confidence: {confidence})")
