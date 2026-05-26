"""
Transcription worker — run one or more of these alongside app.py.
Each worker atomically claims queued jobs from the shared SQLite DB,
processes them with faster-whisper, and writes results back.
Multiple workers run fully in parallel; no coordination beyond the DB is needed.
"""
import json
import os
import platform
import signal
import sqlite3
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from transcribe import get_duration, load_model, to_srt, to_vtt, to_txt

WORKER_ID   = os.getenv("WORKER_ID",   str(uuid.uuid4())[:8])
WORKER_HOST = os.getenv("HOSTNAME",    platform.node() or "worker")
DB_PATH     = Path(os.getenv("DB_PATH",    "jobs.db"))
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", "outputs"))
MODELS_DIR  = Path(os.getenv("MODELS_DIR", "models"))
UPLOAD_DIR  = Path(os.getenv("UPLOAD_DIR", "uploads"))
POLL_SECS   = int(os.getenv("POLL_INTERVAL", "2"))

OUTPUT_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)

_running = True


def _handle_signal(sig, _frame):
    global _running
    print(f"\n[{WORKER_ID}] shutting down…")
    _running = False


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# DB helpers (plain sqlite3, no FastAPI dependency)
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _update(job_id: str, **fields):
    clause = ", ".join(f"{k} = ?" for k in fields)
    now = datetime.utcnow().isoformat()
    c = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    try:
        c.execute(f"UPDATE jobs SET {clause} WHERE id = ?", [*fields.values(), job_id])
        # Piggyback heartbeat so app.py sees the worker as ONLINE during transcription.
        c.execute("""
            INSERT INTO workers (id, host, last_seen, current_job)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                host = excluded.host,
                last_seen = excluded.last_seen,
                current_job = excluded.current_job
        """, (WORKER_ID, WORKER_HOST, now, job_id))
        c.commit()
    finally:
        c.close()


def _heartbeat(current_job: Optional[str] = None):
    try:
        c = sqlite3.connect(str(DB_PATH), timeout=10, check_same_thread=False)
        c.execute("""
            INSERT INTO workers (id, host, last_seen, current_job)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_seen   = excluded.last_seen,
                current_job = excluded.current_job
        """, (WORKER_ID, WORKER_HOST, datetime.utcnow().isoformat(), current_job))
        c.commit()
        c.close()
    except Exception as exc:
        print(f"[{WORKER_ID}] heartbeat error: {exc}", flush=True)



def _claim() -> Optional[dict]:
    """Atomically claim a queued job assigned to this worker (or any unassigned job)."""
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        c.execute("BEGIN IMMEDIATE")
        # Prefer jobs explicitly assigned to this worker, then unassigned (any-worker) jobs.
        # Skip jobs assigned to a different worker.
        row = c.execute("""
            SELECT * FROM jobs
            WHERE status = 'queued'
              AND (preferred_worker IS NULL OR preferred_worker = ?)
            ORDER BY
              CASE WHEN preferred_worker = ? THEN 0 ELSE 1 END,
              created_at
            LIMIT 1
        """, (WORKER_ID, WORKER_ID)).fetchone()
        if row:
            c.execute(
                "UPDATE jobs SET status = 'processing', worker_id = ? WHERE id = ?",
                (WORKER_ID, row["id"]),
            )
            c.commit()
            return dict(row)
        c.rollback()
        return None
    except Exception:
        try: c.rollback()
        except Exception: pass
        return None
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Settings & webhooks
# ---------------------------------------------------------------------------

def _get_setting(key: str) -> Optional[str]:
    try:
        c = sqlite3.connect(str(DB_PATH), timeout=5, check_same_thread=False)
        row = c.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        c.close()
        return row[0] if row else None
    except Exception:
        return None


def _fire_webhooks(job: dict):
    """Fire Jellyfin refresh and/or generic webhook after a job completes."""
    jf_url = _get_setting("jellyfin_url")
    jf_key = _get_setting("jellyfin_api_key")
    wh_url = _get_setting("webhook_url")

    # Jellyfin — trigger full library refresh
    if jf_url and jf_key:
        try:
            url = f"{jf_url.rstrip('/')}/Library/Refresh"
            req = urllib.request.Request(url, method="POST")
            req.add_header("X-Emby-Token", jf_key)
            req.add_header("Content-Length", "0")
            urllib.request.urlopen(req, timeout=10)
            print(f"[{WORKER_ID}] Jellyfin library refresh triggered", flush=True)
        except Exception as exc:
            print(f"[{WORKER_ID}] Jellyfin webhook error: {exc}", flush=True)

    # Generic webhook — POST JSON payload
    if wh_url:
        try:
            payload = json.dumps({
                "job_id":   job["id"],
                "filename": job["filename"],
                "status":   "done",
                "language": job.get("language"),
                "task":     job.get("task") or "transcribe",
            }).encode()
            req = urllib.request.Request(wh_url, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            urllib.request.urlopen(req, timeout=10)
            print(f"[{WORKER_ID}] Webhook fired → {wh_url}", flush=True)
        except Exception as exc:
            print(f"[{WORKER_ID}] Webhook error: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------

def _process(job: dict):
    job_id = job["id"]
    out_format = job.get("out_format") or "srt"
    model_size = job.get("model_size") or "base"
    language   = job.get("language") or "auto"
    task       = job.get("task") or "transcribe"
    if task not in ("transcribe", "translate"):
        task = "transcribe"

    # Resolve source file — library jobs have source_path; uploads are in UPLOAD_DIR
    source_path = job.get("source_path")
    if source_path:
        file_path   = Path(source_path)
        delete_after = False
    else:
        candidates = list(UPLOAD_DIR.glob(f"{job_id}_*"))
        if not candidates:
            _update(job_id, status="failed", error="Source file not found")
            return
        file_path    = candidates[0]
        delete_after = True

    if not file_path.exists():
        _update(job_id, status="failed", error=f"File missing: {file_path}")
        return

    print(f"[{WORKER_ID}] ▶ {job['filename']}  ({job_id[:8]})")
    _update(job_id, status="processing", progress=0)

    try:
        duration   = get_duration(file_path)
        lang_arg   = language if language != "auto" else None
        # Model loading may take minutes on first run; app.py shows BUSY while status='processing'
        model      = load_model(model_size, str(MODELS_DIR))

        segments_gen, info = model.transcribe(
            str(file_path), language=lang_arg,
            task=task,
            vad_filter=True, word_timestamps=False,
        )
        detected  = info.language
        all_segs  = []
        started   = time.monotonic()

        for seg in segments_gen:
            all_segs.append(seg)
            pct     = min(99.0, seg.end / duration * 100) if duration else -1
            elapsed = time.monotonic() - started
            speed   = round(seg.end / elapsed, 2) if elapsed > 1 and seg.end > 0 else None
            eta     = max(0, round((duration - seg.end) / speed)) if (speed and duration and pct >= 0) else None
            _update(job_id, progress=pct, language=detected, speed=speed, eta=eta)

        ext      = "txt" if out_format == "txt" else out_format
        out_path = OUTPUT_DIR / f"{job_id}.{ext}"

        if out_format == "srt":
            out_path.write_text(to_srt(all_segs), encoding="utf-8")
        elif out_format == "vtt":
            out_path.write_text(to_vtt(all_segs), encoding="utf-8")
        else:
            out_path.write_text(to_txt(all_segs), encoding="utf-8")

        _update(job_id, status="done", progress=100,
                completed_at=datetime.utcnow().isoformat(),
                output_file=str(out_path), language=detected, speed=None, eta=None)
        print(f"[{WORKER_ID}] ✓ {job['filename']}", flush=True)
        _fire_webhooks(job)

    except Exception as exc:
        _update(job_id, status="failed", error=str(exc))
        print(f"[{WORKER_ID}] ✗ {job['filename']}: {exc}")
    finally:
        if delete_after and file_path.exists():
            try: file_path.unlink()
            except Exception: pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    # Attempt WAL mode (may silently fall back to DELETE on some filesystems).
    with _conn() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")

    print(f"[{WORKER_ID}] worker ready  host={WORKER_HOST}  db={DB_PATH}")

    _heartbeat()
    while _running:
        job = _claim()
        if job:
            _process(job)
        else:
            _heartbeat()
            time.sleep(POLL_SECS)

    with _conn() as c:
        c.execute("DELETE FROM workers WHERE id = ?", (WORKER_ID,))
        c.commit()
    print(f"[{WORKER_ID}] offline")


if __name__ == "__main__":
    main()
