from pathlib import Path

# ====== INPUT / OUTPUT ======
PRODUCT_ID_CSV = "raw/productid2.csv"
OUT_DIR = Path("./data")
CHECKPOINT_FILE = OUT_DIR / "checkpoint.csv"

# ====== API / HEADERS ======
API = "https://api.tiki.vn/product-detail/api/v1/products/{}"
HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0 Safari/537.36",
    "referer": "https://tiki.vn/",
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ====== RUNTIME ======
BATCH_READ_IDS  = 20000     # đọc theo lô
SAVE_BATCH_SIZE = 1000      # 1000 sp / file
CONCURRENCY     = 50
RETRIES         = 3
RETRY_DELAY     = 0.5
PROGRESS_STEP   = 100
TIMEOUT_TOTAL   = 20
TIMEOUT_CONNECT = 5
TIMEOUT_READ    = 15
TCP_LIMIT       = 80
