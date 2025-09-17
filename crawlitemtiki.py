#!/usr/bin/env python3
import asyncio, aiohttp, csv, json, re, html, random
from pathlib import Path
import pandas as pd



# Gán biến ban đầu====================================
PRODUCT_ID_CSV   = "productid2.csv" #Locate file productid
OUT_DIR          = Path("./data")
CHECKPOINT_FILE  = OUT_DIR / "checkpoint.csv"

API              = "https://api.tiki.vn/product-detail/api/v1/products/{}"

BATCH_READ_IDS   = 20000     # đọc CSV theo 20k product id
SAVE_BATCH_SIZE  = 1000      # 1000 sản phẩm / 1 file JSON
CONCURRENCY      = 50        # số request đồng thời
RETRIES          = 3
RETRY_DELAY      = 0.5
PROGRESS_STEP    = 100       # in progress mỗi 100 items
HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0 Safari/537.36",
    "referer": "https://tiki.vn/",
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ---------- UTILS ----------
def normalize_description_multiline(raw_html: str) -> str:
    if not raw_html:
        return ""
    s = html.unescape(raw_html)
    s = re.sub(r'(?i)<\s*br\s*/?\s*>', '\n', s)
    s = re.sub(r'(?i)</\s*p\s*>', '\n', s)
    s = re.sub(r'(?i)</\s*li\s*>', '\n', s)
    s = re.sub(r'(?s)<[^>]+>', '', s)
    s = re.sub(r'[ \t\r\f]+', ' ', s)
    s = re.sub(r'\n{2,}', '\n', s)
    return s.strip()

data=pd.read_csv("./producttest.csv")
dataset=data["id"].tolist()

def iter_ids(path: str, batch_size: int = 10):
    buf = []
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if row and row[0].strip():
                buf.append(row[0].strip())
                if len(buf) >= batch_size:
                    yield buf
                    buf = []
    if buf:
        yield buf
        
def load_checkpoint_ids() -> set:
    if not CHECKPOINT_FILE.exists():
        return set()
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        return {row[0].strip() for row in csv.reader(f)
                if row and row[0].strip() and row[0] != "id"}

def append_checkpoint(pid: str, status: str, reason: str = ""):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not CHECKPOINT_FILE.exists()
    with open(CHECKPOINT_FILE, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["id", "status", "reason"])
        w.writerow([pid, status, reason])

def next_batch_index() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(OUT_DIR.glob("product_batch_*.json"))
    return len(existing) + 1

def save_batch(items: list, idx: int):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"product_batch_{idx:05d}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"[BATCH DONE] Saved {len(items)} items to {path}")

def extract_item(data: dict):
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": normalize_description_multiline(data.get("description")),
        "images": data.get("images")
    }

# Function gọi API và format data----------
async def fetch_one(session: aiohttp.ClientSession, pid: str, sem: asyncio.Semaphore):
    url = API.format(pid)
    async with sem:
        for attempt in range(RETRIES + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {"type": "ok", "pid": pid, "data": extract_item(data)}
                    elif resp.status == 404:
                        return {"type": "fail", "pid": pid, "reason": "not_found"}
                    elif resp.status == 429:
                        if attempt < RETRIES:
                            delay = (2 ** attempt) + random.random()
                            await asyncio.sleep(delay)
                            continue
                        else:
                            return {"type": "fail", "pid": pid, "reason": "http_429"}
                    else:
                        if attempt == RETRIES:
                            return {"type": "fail", "pid": pid, "reason": f"http_{resp.status}"}
            except Exception as e:
                if attempt == RETRIES:
                    return {"type": "fail", "pid": pid, "reason": f"exception:{type(e).__name__}"}
            if attempt < RETRIES:
                await asyncio.sleep(RETRY_DELAY)

# Function MAin gọi API 200K sản phẩm----------
async def main():
    timeout = aiohttp.ClientTimeout(total=20, sock_connect=5, sock_read=15)
    connector = aiohttp.TCPConnector(limit=80)
    sem = asyncio.Semaphore(CONCURRENCY)

    processed_set = load_checkpoint_ids()
    out_batch, out_idx = [], next_batch_index()

    total = success = fail = 0
    fail_404 = fail_429 = fail_exc = fail_http_other = 0
    last_pid = None

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        for ids_chunk in iter_ids(PRODUCT_ID_CSV, batch_size=BATCH_READ_IDS):
            ids = [pid for pid in ids_chunk if pid not in processed_set]
            if not ids:
                continue

            tasks = [fetch_one(session, pid, sem) for pid in ids]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                pid = res["pid"]
                last_pid = pid  # lưu ID xử lý gần nhất

                if res["type"] == "ok":
                    out_batch.append(res["data"])
                    append_checkpoint(pid, "done")
                    processed_set.add(pid)
                    success += 1
                else:
                    reason = res["reason"]
                    append_checkpoint(pid, "fail", reason)
                    processed_set.add(pid)
                    fail += 1
                    if reason == "not_found":
                        fail_404 += 1
                    elif reason == "http_429":
                        fail_429 += 1
                    elif reason.startswith("exception"):
                        fail_exc += 1
                    else:
                        fail_http_other += 1

                total += 1

                # progress mỗi 100 items
                if total % PROGRESS_STEP == 0:
                    print(f"[PROGRESS] total={total} success={success} fail={fail} last_pid={last_pid}")

                # ghi batch khi đủ số lượng
                if len(out_batch) >= SAVE_BATCH_SIZE:
                    save_batch(out_batch, out_idx)
                    out_batch, out_idx = [], out_idx + 1

    if out_batch:
        save_batch(out_batch, out_idx)

    # summary kết quả
    print("=" * 60)
    print(f"[SUMMARY] total={total} success={success} fail={fail}")
    print(f"          404={fail_404}, 429={fail_429}, exception={fail_exc}, http_other={fail_http_other}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
