import os
from typing import Self
import mmh3
import regex as re
import unicodedata
from nltk import word_tokenize

def remove_all_punct(s: str) -> str:
    return "".join(ch for ch in s if not unicodedata.category(ch).startswith("P"))

def preprocess(text: str) -> str:
    # lowercase
    text = text.lower()
    
    # normalize whitespace
    text = re.sub(r"\s+", " ", text)

    # Remove all punctuation
    text = remove_all_punct(text)

    # Normalize unicode characters
    text = unicodedata.normalize("NFD", text)

    # Remove accents
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")

    return text

def minhash(ngrams: set[str], seed: int) -> int:
    return min(mmh3.hash(t, seed) for t in ngrams)

def compute_minhash_signature(text: str, num_hashes: int, ngram_size: int) -> tuple[list[int], set[str]]:
    tokens = word_tokenize(text)
    ngrams: set[str] = {" ".join(tokens[i:i + ngram_size]) for i in range(len(tokens) - ngram_size + 1)}
    signature = []
    for seed in range(num_hashes):
        signature.append(minhash(ngrams, seed))
    return signature, ngrams


class Document:
    def __init__(self, path: os.PathLike, ngrams: set[str]):
        self.path = path
        self.ngrams = ngrams
        self.parent: Document = self
    
    def is_root(self) -> bool:
        return self.parent == self
    
    def jaccard_similarity(self, other: Self) -> float:
        intersection = len(self.ngrams.intersection(other.ngrams))
        union = len(self.ngrams.union(other.ngrams))
        return intersection / union if union > 0 else 0.0
    
    def find_root(self) -> Self:
        if self.is_root():
            return self
        parent = self.parent.find_root()
        self.parent = parent
        return parent # type: ignore


def deuplicate_documents(docs: list[Document], jaccard_threshold: float):
    for i in range(len(docs)):
        for j in range(i + 1, len(docs)):
            doc1 = docs[i].find_root()
            doc2 = docs[j].find_root()
            if doc1 == doc2:
                continue
            similarity = doc1.jaccard_similarity(doc2)
            if similarity >= jaccard_threshold:
                doc2.parent = doc1


def minhash_deduplicate(input_files: list[os.PathLike], num_hashes: int, num_bands: int,
                        ngram_size: int, jaccard_threshold: float, output_dir: os.PathLike):

    band_buckets = [{} for _ in range(num_bands)]
    rows_per_band = num_hashes // num_bands
    for path in input_files:
        with open(path, 'r') as f:
            text = f.read()
            text = preprocess(text)
            signature, ngrams = compute_minhash_signature(text, num_hashes, ngram_size)
            doc: Document = Document(path, ngrams)
            for band in range(num_bands):
                start = band * rows_per_band
                end = start + rows_per_band
                band_signature = tuple(signature[start:end])
                bucket: list[Document] = band_buckets[band].setdefault(band_signature, [])
                bucket.append(doc)

    # Deduplicate documents within each bucket
    for band in range(num_bands):
        buckets = band_buckets[band] 
        for signature in buckets:
            docs = buckets[signature]
            deuplicate_documents(docs, jaccard_threshold)
    
    output_docs: dict[os.PathLike, Document] = {}
    for band in range(num_bands):
        buckets = band_buckets[band] 
        for signature in buckets:
            docs: list[Document] = buckets[signature]
            for doc in docs:
                root: Document = doc.find_root()
                if root.path not in output_docs:
                    output_docs[root.path] = root
    
    os.makedirs(output_dir, exist_ok=True)
    for root_path in output_docs:
        output_path = os.path.join(output_dir, os.path.basename(root_path))
        with open(root_path, 'r') as src_file, open(output_path, 'w') as dst_file:
            dst_file.write(src_file.read())
            


if __name__ == "__main__":
    content = """This is a sample text! It includes punctuation, accents like café, and    irregular   spacing.
    Let's see how preprocessing works."""
    preprocessed = preprocess(content)
    print("Preprocessed Text:")
    print(preprocessed)

    signature = compute_minhash_signature(content, num_hashes=100, ngram_size=3)
    print("MinHash Signature:")
    print(signature)