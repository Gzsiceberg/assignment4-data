import re

from cs336_data.extract_text import extract_warc

def mask_email(text) -> tuple[str, int]:
    """Mask email addresses in the given text."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.subn(email_pattern, '|||EMAIL_ADDRESS|||', text)

def mask_phone_numbers(text) -> tuple[str, int]:
    """Mask phone numbers in the given text."""
    total_count = 0
    phone_pattern = r'(?<!\d)(?:\d{10}|\(\d{3}\)[ -]?\d{3}[ -]?\d{4}|\d{3}[ -]?\d{3}[ -]?\d{4})(?!\d)'
    text, count01 = re.subn(phone_pattern, '|||PHONE_NUMBER|||', text)
    total_count += count01

    phone_pattern_international = r'(?<!\d)(?:(?:\+\d{1,3}|\(\+\d{1,3}\))[ -]?)?\d{3}[ -]?\d{4}[ -]?\d{4}(?!\d)'
    text, count02 = re.subn(phone_pattern_international, '|||PHONE_NUMBER|||', text)
    total_count += count02

    return text, total_count

def mask_ip_addresses(text) -> tuple[str, int]:
    """Mask IP addresses in the given text."""
    ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    return re.subn(ip_pattern, '|||IP_ADDRESS|||', text)


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

    sample_text_phone = "Call me at (123) 456-7890 or 987-654-3210."
    masked_text_phone, num_masked_phone = mask_phone_numbers(sample_text_phone)
    print(f"Masked Text: {masked_text_phone}, Number Masked: {num_masked_phone}")

    sample_text_phone2 = "My number is 2831823829."
    masked_text_phone2, num_masked_phone2 = mask_phone_numbers(sample_text_phone2)
    print(f"Masked Text: {masked_text_phone2}, Number Masked: {num_masked_phone2}")

    sample_text_phone3 = "Reach me at (283)-182-3829."
    masked_text_phone3, num_masked_phone3 = mask_phone_numbers(sample_text_phone3)
    print(f"Masked Text: {masked_text_phone3}, Number Masked: {num_masked_phone3}")

    sample_text_phone4 = "You can dial (+33) 18155704487 or +53 181-5570-4487 for info."
    masked_text_phone4, num_masked_phone4 = mask_phone_numbers(sample_text_phone4)
    print(f"Masked Text: {masked_text_phone4}, Number Masked: {num_masked_phone4}")

    ip_text = "The server IPs are 192.168.1.1 and 10.0.0.1."
    masked_text_ip, num_masked_ip = mask_ip_addresses(ip_text)
    print(f"Masked Text: {masked_text_ip}, Number Masked: {num_masked_ip}")


    from fastwarc.warc import ArchiveIterator, WarcRecordType
    from rich.progress import track
    from rich import print
    import random
    warc_path = "data/CC-MAIN-20250417135010-20250417165010-00065.warc.gz"
    count = 0
    is_english = 0
    is_chinese = 0
    total_records = 0
    with open(warc_path, "rb") as f:
        for i, record in enumerate(track(ArchiveIterator(f))):
            if record.record_type != WarcRecordType.response:
                continue
            if random.random() > 0.05:
                continue
            payload = record.reader.read()
            text = extract_warc(payload)
            text, email_count = mask_email(text)
            text, phone_count = mask_phone_numbers(text)
            text, ip_count = mask_ip_addresses(text)
            if email_count + phone_count + ip_count > 0 and count < 20:
                print("-" * 80)
                print(f"RecordID: {record.record_id} - Masked {email_count} emails, {phone_count} phone numbers, {ip_count} IP addresses")
                text = text.replace("\n", " ")
                print(text)
                count += 1
                if count >= 20:
                    break
