import fasttext
from cs336_data.gen_fasttext import preprocess_text

global_model = None

def predict_wiki_like(text: str) -> tuple[str, float]:
    text = preprocess_text(text)
    global global_model
    if global_model is None:
        global_model = fasttext.load_model("model/wiki_model.bin")
    model = global_model
    output = model.predict(text)
    label = output[0][0].replace("__label__", "")
    confidence = output[1][0]
    return "wiki" if label == "positive" else "cc", confidence

def roc_auc_score(all_labels, all_preds):
    # Calculate the ROC AUC score
    pos_count = sum(all_labels)
    neg_count = len(all_labels) - pos_count
    if pos_count == 0 or neg_count == 0:
        return 0.0
    
    # Sort by predictions in descending order, along with their labels
    sorted_pairs = sorted(zip(all_preds, all_labels), reverse=True)
    
    # Count how many negative examples are ranked lower than each positive example
    num_correct_pairs = 0
    
    for i, (pred_i, label_i) in enumerate(sorted_pairs):
        if label_i == 1:  # For each positive example
            # Count how many negatives come after it (have lower rank/score)
            for j in range(i + 1, len(sorted_pairs)):
                _, label_j = sorted_pairs[j]
                if label_j == 0:  # Negative example ranked lower
                    num_correct_pairs += 1
    
    auc = num_correct_pairs / (pos_count * neg_count)
    return auc

if __name__ == "__main__":
    import sys

    file_path = sys.argv[1]
    all_labels = []
    all_preds = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            label, text = line.split(" ", 1)
            if label == "__label__positive":
                label = 1
            else:
                label = 0
            pred_label, confidence = predict_wiki_like(text)
            all_labels.append(label)
            all_preds.append(confidence if pred_label == "wiki" else 1 - confidence)
    auc = roc_auc_score(all_labels, all_preds)
    print(f"AUC: {auc:.4f}")


