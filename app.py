import asyncio, aiohttp, pandas as pd
from config import (PRODUCT_ID_CSV, HEADERS, CONCURRENCY, BATCH_READ_IDS,
                    SAVE_BATCH_SIZE, PROGRESS_STEP, TIMEOUT_TOTAL, TIMEOUT_CONNECT,
                    TIMEOUT_READ, TCP_LIMIT)
from io_utils import iter_ids, next_batch_index, save_batch_unique
from checkpoint import load_checkpoint_ids, append_checkpoint
from fetcher import fetch_one
from retry_job import retry_failed_items
import sys

async def main():
    # load id list (đã lọc trùng trong CSV bằng pandas)
    raw = pd.read_csv(PRODUCT_ID_CSV)
    list_product = raw['id'].dropna().drop_duplicates().astype(str).tolist()


    timeout   = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL, sock_connect=TIMEOUT_CONNECT, sock_read=TIMEOUT_READ)
    connector = aiohttp.TCPConnector(limit=TCP_LIMIT)
    sem       = asyncio.Semaphore(CONCURRENCY)

    processed_set, last_pid = load_checkpoint_ids()
    out_batch, out_idx = [], next_batch_index()

    total = success = fail = 0
    fail_404 = fail_429 = fail_exc = fail_http_other = 0

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        for ids_chunk in iter_ids(list_product, batch_size=BATCH_READ_IDS):
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
                    save_batch_unique(out_batch, out_idx)
                    out_batch, out_idx = [], out_idx + 1

    if out_batch:
        save_batch_unique(out_batch, out_idx)

    # summary kết quả
    print("=" * 60)
    print(f"[SUMMARY] total={total} success={success} fail={fail}")
    print(f"          404={fail_404}, 429={fail_429}, exception={fail_exc}, http_other={fail_http_other}")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
    # vét lại tất cả ID đang fail
    asyncio.run(retry_failed_items())

sys.exit(0)   
