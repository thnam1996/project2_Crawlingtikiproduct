import asyncio, aiohttp
from config import (HEADERS, CONCURRENCY, TIMEOUT_TOTAL, TIMEOUT_CONNECT,
                    TIMEOUT_READ, TCP_LIMIT, SAVE_BATCH_SIZE, PROGRESS_STEP)
from io_utils import next_batch_index, save_batch_unique
from checkpoint import get_failed_ids, rewrite_checkpoint
from fetcher import fetch_one

async def retry_failed_items():
    retry_ids = get_failed_ids()
    if not retry_ids:
        print("[RETRY] Không có ID fail để chạy lại.")
        return

    print(f"[RETRY] Bắt đầu chạy lại {len(retry_ids)} ID fail...")
    timeout   = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL, sock_connect=TIMEOUT_CONNECT, sock_read=TIMEOUT_READ)
    connector = aiohttp.TCPConnector(limit=TCP_LIMIT)
    sem       = asyncio.Semaphore(CONCURRENCY)

    updates = {}
    out_batch, out_idx = [], next_batch_index("retry_batch")

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        tasks = [fetch_one(session, pid, sem) for pid in retry_ids]
        done = 0
        for coro in asyncio.as_completed(tasks):
            res = await coro
            pid = res["pid"]

            if res["type"] == "ok":
                out_batch.append(res["data"])
                updates[pid] = ("done", "retry_ok")
            else:
                updates[pid] = ("fail", res["reason"])

            done += 1
            if done % PROGRESS_STEP == 0:
                print(f"[RETRY PROGRESS] done={done}/{len(retry_ids)} last_pid={pid}")

            if len(out_batch) >= SAVE_BATCH_SIZE:
                save_batch_unique(out_batch, out_idx, prefix="retry_batch")
                out_batch, out_idx = [], out_idx + 1

    if out_batch:
        save_batch_unique(out_batch, out_idx, prefix="retry_batch")

    stats = rewrite_checkpoint(updates)
