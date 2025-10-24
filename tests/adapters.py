from __future__ import annotations

import os
from typing import Any



def run_extract_text_from_html_bytes(html_bytes: bytes) -> str | None:
    from cs336_data.extract_text import extract_warc
    return extract_warc(html_bytes)


def run_identify_language(text: str) -> tuple[Any, float]:
    from cs336_data.language_identification import detect_language
    return detect_language(text)


def run_mask_emails(text: str) -> tuple[str, int]:
    from cs336_data.mask_pii import mask_email
    return mask_email(text)


def run_mask_phone_numbers(text: str) -> tuple[str, int]:
    from cs336_data.mask_pii import mask_phone_numbers
    return mask_phone_numbers(text)


def run_mask_ips(text: str) -> tuple[str, int]:
    from cs336_data.mask_pii import mask_ip_addresses
    return mask_ip_addresses(text)


def run_classify_nsfw(text: str) -> tuple[Any, float]:
    from cs336_data.harmful_content import classify_nsfw
    return classify_nsfw(text)


def run_classify_toxic_speech(text: str) -> tuple[Any, float]:
    from cs336_data.harmful_content import classify_toxicity
    return classify_toxicity(text)


def run_classify_quality(text: str) -> tuple[Any, float]:
    from cs336_data.quality_classifier import predict_wiki_like
    return predict_wiki_like(text)


def run_gopher_quality_filter(text: str) -> bool:
    from cs336_data.quality_filters import gopher_quality_filter
    return gopher_quality_filter(text)[0]


def run_exact_line_deduplication(
    input_files: list[os.PathLike], output_directory: os.PathLike
):
    raise NotImplementedError


def run_minhash_deduplication(
    input_files: list[os.PathLike],
    num_hashes: int,
    num_bands: int,
    ngrams: int,
    jaccard_threshold: float,
    output_directory: os.PathLike,
):
    raise NotImplementedError
