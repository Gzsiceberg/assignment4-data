import datasets
from datasets import load_dataset

if __name__ == "__main__":
    dataset = load_dataset("allenai/paloma", "c4_100_domains")
    valid_dataset: datasets.Dataset = dataset["val"] # type: ignore

    with open("data/filter_CC/c4.txt", "w", encoding="utf-8") as out_f:
        for data in valid_dataset:
            text = data["text"]
            out_f.write(text + "\n\n\n")
        