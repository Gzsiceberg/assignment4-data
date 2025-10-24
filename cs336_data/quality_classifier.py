import fasttext
from cs336_data.gen_fasttext import preprocess_text

global_model = None

def predict_wiki_like(text: str) -> tuple[str, float]:
    text = preprocess_text(text)
    global global_model
    if global_model is None:
        global_model = fasttext.load_model("data/wiki_model.bin")
    model = global_model
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return "wiki" if label == "positive" else "cc", confidence
