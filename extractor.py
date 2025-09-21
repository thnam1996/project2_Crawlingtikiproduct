import re, html

def normalize_description_multiline(raw_html):
    if not raw_html or not isinstance(raw_html, str):
        return ""
    s = html.unescape(raw_html)
    s = re.sub(r'(?i)<\s*br\s*/?\s*>', '\n', s)
    s = re.sub(r'(?i)</\s*p\s*>', '\n', s)
    s = re.sub(r'(?i)</\s*li\s*>', '\n', s)
    s = re.sub(r'(?s)<[^>]+>', '', s)
    s = re.sub(r'[ \t\r\f]+', ' ', s)
    s = re.sub(r'\n{2,}', '\n', s)
    return s.strip()

def extract_item(data: dict) -> dict:
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": normalize_description_multiline(data.get("description")),
        "images": data.get("images"),
    }
