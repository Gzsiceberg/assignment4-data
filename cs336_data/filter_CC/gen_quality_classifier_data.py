from datasets import load_dataset
from cs336_data.gen_fasttext import preprocess_text
import os

def create_fasttext_pos_data(data):
    text = data["text"]
    text = preprocess_text(text)
    label_text = f"{'__label__positive'} {text}"
    return {"label_text": label_text}


if __name__ == "__main__":
    dataset = load_dataset("allenai/paloma", "c4_100_domains")
    valid_dataset = dataset["val"]
    valid_dataset = valid_dataset.map(create_fasttext_pos_data)
    output_path = "data/filter_CC/cc_pos.txt"
    dir_path = os.path.dirname(output_path)
    os.makedirs(dir_path, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as out_f:
        for item in valid_dataset:
            out_f.write(item["label_text"] + "\n")
    print(f"Finished writing {len(valid_dataset)} samples to {output_path}.")