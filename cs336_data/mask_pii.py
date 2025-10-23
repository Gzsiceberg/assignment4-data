import re

def mask_email(text) -> tuple[str, int]:
    """Mask email addresses in the given text."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.subn(email_pattern, '|||EMAIL_ADDRESS|||', text)


if __name__ == "__main__":
    sample_text = "Contact us at forking@gmail.com"
    masked_text, count = mask_email(sample_text)
    print(f"Masked Text: {masked_text}")

    sample_text_multiple = "Emails: forking-vr@gmail.com, get_vr@gmail.com"
    masked_text_multiple, count_multiple = mask_email(sample_text_multiple)
    print(f"Masked Text: {masked_text_multiple}, Count: {count_multiple}")

    sample_text = "Feel free to contact me at test@gmail.com if you have any questions."
    masked_text, num_masked = mask_email(sample_text)
    print(f"Masked Text: {masked_text}, Number Masked: {num_masked}")