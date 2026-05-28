"""
whisper.cpp/Vulkan worker for AMD GPUs such as the RX 580.

This worker uses the same SQLite queue contract as worker.py, but only claims
jobs whose backend is "whispercpp_vulkan". It shells out to whisper-cli so the
GPU path stays isolated from the faster-whisper/PyTorch worker.
"""
import json
import os
import platform
import re
import signal
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

WORKER_ID      = os.getenv("WORKER_ID", str(uuid.uuid4())[:8])
WORKER_HOST    = os.getenv("HOSTNAME", platform.node() or "worker")
WORKER_BACKEND = os.getenv("WORKER_BACKEND", "whispercpp_vulkan")
WORKER_DEVICE  = os.getenv("WORKER_DEVICE", "RX 580 / Vulkan")
DB_PATH        = Path(os.getenv("DB_PATH", "jobs.db"))
OUTPUT_DIR     = Path(os.getenv("OUTPUT_DIR", "outputs"))
UPLOAD_DIR     = Path(os.getenv("UPLOAD_DIR", "uploads"))
MODEL_DIR      = Path(os.getenv("WHISPER_CPP_MODEL_DIR", "models"))
WHISPER_BIN    = Path(os.getenv("WHISPER_CPP_BIN", "/opt/whisper.cpp/build/bin/whisper-cli"))
EXTRA_ARGS     = os.getenv("WHISPER_CPP_EXTRA_ARGS", "")
POLL_SECS      = int(os.getenv("POLL_INTERVAL", "2"))

OUTPUT_DIR.mkdir(exist_ok=True)
MODEL_DIR.mkdir(exist_ok=True)

_running = True
_progress_re = re.compile(r"(\d{1,3})%")
_timestamp_re = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?\s+-->")


def _handle_signal(_sig, _frame):
    global _running
    print(f"\n[{WORKER_ID}] shutting down...", flush=True)
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _heartbeat(current_job: Optional[str] = None):
    now = datetime.utcnow().isoformat()
    try:
        with _conn() as c:
            c.execute("""
                INSERT INTO workers (id, host, last_seen, current_job, backend, device)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    current_job = excluded.current_job,
                    backend = excluded.backend,
                    device = excluded.device
            """, (WORKER_ID, WORKER_HOST, now, current_job, WORKER_BACKEND, WORKER_DEVICE))
            c.commit()
    except Exception as exc:
        print(f"[{WORKER_ID}] heartbeat error: {exc}", flush=True)


def _update(job_id: str, **fields):
    clause = ", ".join(f"{k} = ?" for k in fields)
    now = datetime.utcnow().isoformat()
    with _conn() as c:
        c.execute(f"UPDATE jobs SET {clause} WHERE id = ?", [*fields.values(), job_id])
        c.execute(
            "UPDATE workers SET last_seen = ?, current_job = ?, backend = ?, device = ? WHERE id = ?",
            (now, job_id, WORKER_BACKEND, WORKER_DEVICE, WORKER_ID),
        )
        c.commit()


def _status(job_id: str) -> Optional[str]:
    try:
        with _conn() as c:
            row = c.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return row["status"] if row else None
    except Exception:
        return None


def _claim() -> Optional[dict]:
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("""
            SELECT * FROM jobs
            WHERE status = 'queued'
              AND COALESCE(backend, 'faster_whisper') = ?
              AND (preferred_worker IS NULL OR preferred_worker = ?)
            ORDER BY
              CASE WHEN preferred_worker = ? THEN 0 ELSE 1 END,
              created_at
            LIMIT 1
        """, (WORKER_BACKEND, WORKER_ID, WORKER_ID)).fetchone()
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
        try:
            c.rollback()
        except Exception:
            pass
        return None
    finally:
        c.close()


def _get_setting(key: str) -> Optional[str]:
    try:
        with _conn() as c:
            row = c.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row[0] if row else None
    except Exception:
        return None


def _fire_webhooks(job: dict):
    jf_url = _get_setting("jellyfin_url")
    jf_key = _get_setting("jellyfin_api_key")
    wh_url = _get_setting("webhook_url")

    if jf_url and jf_key:
        try:
            req = urllib.request.Request(f"{jf_url.rstrip('/')}/Library/Refresh", method="POST")
            req.add_header("X-Emby-Token", jf_key)
            req.add_header("Content-Length", "0")
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            print(f"[{WORKER_ID}] Jellyfin webhook error: {exc}", flush=True)

    if wh_url:
        try:
            payload = json.dumps({
                "job_id": job["id"],
                "filename": job["filename"],
                "status": "done",
                "language": job.get("language"),
                "task": job.get("task") or "transcribe",
                "backend": WORKER_BACKEND,
            }).encode()
            req = urllib.request.Request(wh_url, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            print(f"[{WORKER_ID}] webhook error: {exc}", flush=True)


def _duration(path: Path) -> Optional[float]:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        return float(json.loads(r.stdout)["format"]["duration"])
    except Exception:
        return None


def _timestamp_seconds(line: str) -> Optional[float]:
    match = _timestamp_re.search(line)
    if not match:
        return None
    hours, minutes, seconds = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def _model_path(model_size: str) -> Path:
    override = os.getenv("WHISPER_CPP_MODEL")
    if override:
        return Path(override)
    return MODEL_DIR / f"ggml-{model_size}.bin"


def _output_flag(out_format: str) -> str:
    return {"srt": "-osrt", "vtt": "-ovtt", "txt": "-otxt"}.get(out_format, "-osrt")


def _resolve_source(job: dict) -> tuple[Optional[Path], bool]:
    if job.get("source_path"):
        return Path(job["source_path"]), False
    candidates = list(UPLOAD_DIR.glob(f"{job['id']}_*"))
    return (candidates[0], True) if candidates else (None, False)


def _process(job: dict):
    job_id = job["id"]
    out_format = job.get("out_format") or "srt"
    model_size = job.get("model_size") or "base"
    language = job.get("language") or "auto"
    task = job.get("task") or "transcribe"
    if task not in ("transcribe", "translate"):
        task = "transcribe"
    file_path, delete_after = _resolve_source(job)

    if not file_path or not file_path.exists():
        _update(job_id, status="failed", error="Source file not found")
        return
    if not WHISPER_BIN.exists():
        _update(job_id, status="failed", error=f"whisper-cli not found: {WHISPER_BIN}")
        return

    model_path = _model_path(model_size)
    if not model_path.exists():
        _update(job_id, status="failed", error=f"Model missing: {model_path}")
        return

    print(f"[{WORKER_ID}] Vulkan -> {job['filename']} ({job_id[:8]})", flush=True)
    _update(job_id, status="processing", progress=-1, speed=None, eta=None)

    out_ext = "txt" if out_format == "txt" else out_format
    if job.get("source_path"):
        output_language = "en" if task == "translate" else (language if language != "auto" else "auto")
        prefix = file_path.with_name(f"{file_path.stem}.{output_language}")
    else:
        prefix = OUTPUT_DIR / job_id
    out_path = prefix.with_suffix(f".{out_ext}")
    duration = _duration(file_path)
    started = time.monotonic()
    paused = False

    try:
        with tempfile.TemporaryDirectory(prefix=f"subgen-{job_id}-") as tmpdir:
            audio_path = Path(tmpdir) / "audio.wav"
            ffmpeg_cmd = [
                "ffmpeg",
                "-hide_banner", "-loglevel", "error",
                "-y",
                "-i", str(file_path),
                "-vn",
                "-ac", "1",
                "-ar", "16000",
                "-c:a", "pcm_s16le",
                str(audio_path),
            ]
            subprocess.run(ffmpeg_cmd, check=True)

            cmd = [
                str(WHISPER_BIN),
                "-m", str(model_path),
                "-f", str(audio_path),
                "-of", str(prefix),
                _output_flag(out_format),
            ]
            if language != "auto":
                cmd.extend(["-l", language])
            if task == "translate":
                cmd.append("-tr")
            if EXTRA_ARGS:
                cmd.extend(EXTRA_ARGS.split())

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            last_pct = -1
            assert proc.stdout is not None
            for line in proc.stdout:
                print(f"[{WORKER_ID}] {line.rstrip()}", flush=True)
                if _status(job_id) == "pause_requested":
                    paused = True
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    _update(job_id, status="paused", worker_id=None, speed=None, eta=None)
                    _heartbeat(None)
                    print(f"[{WORKER_ID}] paused {job['filename']}", flush=True)
                    return
                pct = None
                match = _progress_re.search(line)
                if match:
                    pct = min(99, max(0, int(match.group(1))))
                elif duration:
                    timestamp = _timestamp_seconds(line)
                    if timestamp is not None:
                        pct = min(99, max(0, int((timestamp / duration) * 100)))

                if pct is None:
                    _heartbeat(job_id)
                    continue
                if pct == last_pct:
                    continue
                last_pct = pct
                elapsed = max(1, time.monotonic() - started)
                processed = (duration or 0) * (pct / 100)
                speed = round(processed / elapsed, 2) if processed > 0 else None
                eta = round(((duration or 0) - processed) / speed) if speed else None
                _update(job_id, progress=pct, speed=speed, eta=eta)

            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"whisper-cli exited with code {rc}")
            if not out_path.exists():
                raise RuntimeError(f"Expected output was not created: {out_path}")

        _update(
            job_id,
            status="done",
            progress=100,
            completed_at=datetime.utcnow().isoformat(),
            output_file=str(out_path),
            language=None if language == "auto" else language,
            speed=None,
            eta=None,
        )
        print(f"[{WORKER_ID}] done -> {job['filename']}", flush=True)
        _fire_webhooks(job)
    except Exception as exc:
        _update(job_id, status="failed", error=str(exc), speed=None, eta=None)
        print(f"[{WORKER_ID}] failed: {exc}", flush=True)
    finally:
        if delete_after and not paused and file_path.exists():
            try:
                file_path.unlink()
            except Exception:
                pass


def main():
    with _conn() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    print(f"[{WORKER_ID}] Vulkan worker ready host={WORKER_HOST} db={DB_PATH}", flush=True)
    _heartbeat(None)

    while _running:
        job = _claim()
        if job:
            _process(job)
        else:
            _heartbeat(None)
            time.sleep(POLL_SECS)

    with _conn() as c:
        c.execute("DELETE FROM workers WHERE id = ?", (WORKER_ID,))
        c.commit()
    print(f"[{WORKER_ID}] offline", flush=True)


if __name__ == "__main__":
    main()
