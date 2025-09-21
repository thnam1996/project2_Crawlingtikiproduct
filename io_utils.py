import csv, json
from pathlib import Path
from typing import Iterable, List
from config import OUT_DIR

def iter_ids(list_product, batch_size: int = 1000):
    buf = []

    for row in list_product:
        if row:
            buf.append(row.strip())
            if len(buf) >= batch_size:
                yield buf
                buf = []
    if buf:
        yield buf

def next_batch_index(prefix: str = "product_batch") -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(OUT_DIR.glob(f"{prefix}_*.json"))
    return len(existing) + 1

def save_batch_unique(items: list, idx: int, prefix: str = "product_batch"):
    """Unique theo id trước khi ghi."""
    uniq = {}
    for it in items:
        pid = it.get("id")
        if pid is not None and pid not in uniq:
            uniq[pid] = it
    out = list(uniq.values())
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{prefix}_{idx:05d}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[BATCH DONE] Saved {len(out)} items -> {path}")


