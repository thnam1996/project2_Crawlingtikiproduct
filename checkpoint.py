import csv
from typing import Dict, Tuple, List
from config import CHECKPOINT_FILE, OUT_DIR

def load_checkpoint_ids() -> Tuple[set, str]:
    """Trả về (processed_set, last_pid)."""
    if not CHECKPOINT_FILE.exists():
        return set(), None
    processed_set, last_pid = set(), None
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or row[0] == "id":
                continue
            pid = row[0].strip()
            if pid:
                processed_set.add(pid)
                last_pid = pid
    return processed_set, last_pid

def append_checkpoint(pid: str, status: str, reason: str = ""):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not CHECKPOINT_FILE.exists()
    with open(CHECKPOINT_FILE, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["id", "status", "reason"])
        w.writerow([pid, status, reason])

def get_failed_ids() -> List[str]:
    ids = []
    if not CHECKPOINT_FILE.exists():
        return ids
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or row[0] == "id":
                continue
            if len(row) > 1 and row[1] == "fail":
                ids.append(row[0].strip())
    # bỏ trùng nếu có
    return list(dict.fromkeys(ids))

def rewrite_checkpoint(updates: Dict[str, tuple]):
    """
    updates: { pid: ("done", "retry_ok") or ("fail", "<reason>") }
    Trả về stats để log.
    """
    if not CHECKPOINT_FILE.exists():
        print("[REWRITE] checkpoint not found.")
        return {"applied_total": 0, "applied_done": 0, "applied_fail": 0,
                "file_done": 0, "file_fail": 0, "file_total": 0}

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    applied_done = applied_fail = applied_total = 0
    new_rows = []
    for row in rows:
        if not row or row[0] == "id":
            new_rows.append(row)
            continue
        pid = row[0]
        if pid in updates:
            st, rsn = updates[pid]
            new_rows.append([pid, st, rsn])
            applied_total += 1
            if st == "done":
                applied_done += 1
            elif st == "fail":
                applied_fail += 1
        else:
            new_rows.append(row)

    with open(CHECKPOINT_FILE, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(new_rows)

    file_done = file_fail = file_total = 0
    for r in new_rows:
        if not r or r[0] == "id":
            continue
        file_total += 1
        if len(r) > 1 and r[1] == "done":
            file_done += 1
        elif len(r) > 1 and r[1] == "fail":
            file_fail += 1

    print("=" * 60)
    print(f"[REWRITE] rerun_fail_total={applied_total}  rerun_fail_done={applied_done}  rerun_fail_fail={applied_fail}")
    print(f"[REWRITE] file_total={file_total}  file_done={file_done}  file_fail={file_fail}")
    print("=" * 60)

    return {"applied_total": applied_total, "applied_done": applied_done, "applied_fail": applied_fail,
            "file_total": file_total, "file_done": file_done, "file_fail": file_fail}
