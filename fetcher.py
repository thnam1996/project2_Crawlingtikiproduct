import asyncio, aiohttp, random
from config import API, RETRIES, RETRY_DELAY
from extractor import extract_item

async def fetch_one(session: aiohttp.ClientSession, pid: str, sem: asyncio.Semaphore):
    url = API.format(pid)
    async with sem:
        for attempt in range(RETRIES + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        d = await resp.json()
                        return {"type": "ok", "pid": pid, "data": extract_item(d)}
                    elif resp.status == 404:
                        return {"type": "fail", "pid": pid, "reason": "not_found"}
                    elif resp.status == 429:
                        if attempt < RETRIES:
                            await asyncio.sleep((2 ** attempt) + random.random())
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
