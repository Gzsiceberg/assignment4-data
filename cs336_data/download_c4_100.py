from datasets import load_dataset


if __name__ == "__main__":
    dataset = load_dataset("allenai/paloma", "c4_100_domains")
    print(dataset)