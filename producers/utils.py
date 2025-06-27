from bs4 import BeautifulSoup
import emoji
import re
import unicodedata

def clean_text(html_content:str):
    """Takes in raw html content from posts and returns clean text without emojis or special symbols"""
    # Strip html
    soup = BeautifulSoup(html_content, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)

    # replace unicode punctuation
    text = unicodedata.normalize("NFKD", text)
    unicode_replacements = {
        '\u2018': "'", '\u2019': "'",  # Single quotes
        '\u201C': '"', '\u201D': '"',  # Double quotes
        '\u2013': '-', '\u2014': '-',  # Dashes
        '\u00A0': ' ',                # Non-breaking space
        '\u2026': '...',              # Ellipsis
    }
    for uni_char, replacement in unicode_replacements.items():
        text = text.replace(uni_char, replacement)
    
    # replace emojis with text description
    text = emoji.demojize(text, delimiters=(" ", " "))

    # collapse whitespace & lowercase
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text