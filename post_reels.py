#!/usr/bin/env python3
"""Har run par queue se agli 2 pending reels ko saare accounts par post karta hai.
Cron se har 12 ghante chalao. Dobara chalane par jo post ho chuka wo skip hota hai."""
import csv, json, os, random, time, logging
from pathlib import Path
import requests

BASE = Path(__file__).parent
API = "https://graph.facebook.com/v21.0"
REELS_PER_RUN = 2
GAP_BETWEEN_POSTS = (int(os.getenv('GAP_MIN', 120)), int(os.getenv('GAP_MAX', 300)))  # seconds, accounts ke beech random gap
PUBLISH_TIMEOUT = 600            # video process hone ka max wait (sec)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(BASE / "poster.log"), logging.StreamHandler()],
)
log = logging.getLogger()


def load_queue():
    with open(BASE / "queue.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_queue(rows):
    with open(BASE / "queue.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["video_url", "caption", "status"])
        w.writeheader()
        w.writerows(rows)


def load_state():
    p = BASE / "state.json"
    return set(json.loads(p.read_text())) if p.exists() else set()


def save_state(done):
    (BASE / "state.json").write_text(json.dumps(sorted(done)))


def post_reel(acc, video_url, caption):
    uid, token = acc["ig_user_id"], acc["access_token"]
    r = requests.post(f"{API}/{uid}/media", data={
        "media_type": "REELS", "video_url": video_url,
        "caption": caption, "access_token": token}, timeout=60)
    r.raise_for_status()
    cid = r.json()["id"]

    start = time.time()
    while time.time() - start < PUBLISH_TIMEOUT:
        s = requests.get(f"{API}/{cid}", params={
            "fields": "status_code", "access_token": token}, timeout=30).json()
        code = s.get("status_code")
        if code == "FINISHED":
            break
        if code in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"Container {code}: {s}")
        time.sleep(15)
    else:
        raise TimeoutError("Video process hone mein bahut time laga")

    r = requests.post(f"{API}/{uid}/media_publish", data={
        "creation_id": cid, "access_token": token}, timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def main():
    # GitHub Actions mein ACCOUNTS_JSON secret se, warna local accounts.json se
    env_acc = os.getenv("ACCOUNTS_JSON")
    accounts = json.loads(env_acc) if env_acc else json.loads((BASE / "accounts.json").read_text())
    rows = load_queue()
    done = load_state()

    pending = [i for i, r in enumerate(rows) if r["status"].strip().lower() != "done"]
    batch = pending[:REELS_PER_RUN]
    if not batch:
        log.info("Queue khaali hai, kuch post karne ko nahi.")
        return

    for i in batch:
        row = rows[i]
        all_ok = True
        for acc in accounts:
            key = f"{i}:{row['video_url']}:{acc['name']}"
            if key in done:
                continue
            caption = row["caption"] + acc.get("caption_suffix", "")
            try:
                media_id = post_reel(acc, row["video_url"], caption)
                log.info("OK   %s -> reel %s (%s)", acc["name"], i + 1, media_id)
                done.add(key)
                save_state(done)
            except Exception as e:
                all_ok = False
                log.error("FAIL %s -> reel %s: %s", acc["name"], i + 1, e)
            time.sleep(random.randint(*GAP_BETWEEN_POSTS))
        if all_ok:
            rows[i]["status"] = "done"
            save_queue(rows)
        else:
            log.warning("Reel %s kuch accounts par fail hui; agli run mein retry hogi.", i + 1)


if __name__ == "__main__":
    main()
