"""
Subgen Web UI — coordinator process.
Handles HTTP, job creation, SSE progress (DB-poll), library scanning, worker management.
Transcription is handled by worker.py — run one or more workers alongside this.
"""
import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiofiles
from fastapi import BackgroundTasks, Body, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from ruamel.yaml import YAML

log = logging.getLogger(__name__)

UPLOAD_DIR   = Path("uploads")
OUTPUT_DIR   = Path("outputs")
DB_PATH      = Path(os.getenv("DB_PATH", "jobs.db"))
COMPOSE_PATH = Path("docker-compose.yml")
MEDIA_SETTLE_SECONDS = int(os.getenv("MEDIA_SETTLE_SECONDS", "600"))

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".m4v", ".wmv", ".mpg", ".mpeg",
    ".ts", ".m2ts", ".webm", ".flv", ".3gp",
    ".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus",
}
TRANSIENT_MEDIA_RE = re.compile(r"\.(?:f\d+|temp)\.[^.]+$", re.IGNORECASE)

for d in [UPLOAD_DIR, OUTPUT_DIR]:
    d.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    with _db() as conn:
        # Enable WAL mode once at startup so multiple processes can read/write concurrently
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id                TEXT PRIMARY KEY,
                filename          TEXT NOT NULL,
                source_path       TEXT,
                library_id        TEXT,
                worker_id         TEXT,
                preferred_worker  TEXT,
                backend           TEXT NOT NULL DEFAULT 'faster_whisper',
                status            TEXT NOT NULL DEFAULT 'queued',
                progress          REAL NOT NULL DEFAULT 0,
                created_at        TEXT NOT NULL,
                completed_at      TEXT,
                output_file       TEXT,
                error             TEXT,
                language          TEXT,
                task              TEXT NOT NULL DEFAULT 'transcribe',
                out_format        TEXT NOT NULL DEFAULT 'srt',
                model_size        TEXT NOT NULL DEFAULT 'base',
                speed             REAL,
                eta               INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS libraries (
                id               TEXT PRIMARY KEY,
                name             TEXT NOT NULL,
                path             TEXT NOT NULL,
                model_size       TEXT NOT NULL DEFAULT 'base',
                language         TEXT NOT NULL DEFAULT 'auto',
                task             TEXT NOT NULL DEFAULT 'transcribe',
                out_format       TEXT NOT NULL DEFAULT 'srt',
                preferred_worker TEXT,
                backend          TEXT NOT NULL DEFAULT 'faster_whisper',
                enabled          INTEGER NOT NULL DEFAULT 1,
                created_at       TEXT NOT NULL,
                last_scan        TEXT,
                last_scan_error  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                host        TEXT,
                last_seen   TEXT,
                current_job TEXT,
                backend     TEXT NOT NULL DEFAULT 'faster_whisper',
                device      TEXT,
                configured  INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # Migrate older schemas
        migrations = [
            ("jobs",      "source_path",      "TEXT"),
            ("jobs",      "library_id",        "TEXT"),
            ("jobs",      "worker_id",         "TEXT"),
            ("jobs",      "preferred_worker",  "TEXT"),
            ("jobs",      "backend",           "TEXT NOT NULL DEFAULT 'faster_whisper'"),
            ("jobs",      "speed",             "REAL"),
            ("jobs",      "eta",               "INTEGER"),
            ("jobs",      "log_cleared",       "INTEGER NOT NULL DEFAULT 0"),
            ("jobs",      "task",              "TEXT NOT NULL DEFAULT 'transcribe'"),
            ("workers",   "name",              "TEXT"),
            ("workers",   "backend",           "TEXT NOT NULL DEFAULT 'faster_whisper'"),
            ("workers",   "device",            "TEXT"),
            ("workers",   "configured",        "INTEGER NOT NULL DEFAULT 0"),
            ("libraries", "preferred_worker",  "TEXT"),
            ("libraries", "backend",           "TEXT NOT NULL DEFAULT 'faster_whisper'"),
            ("libraries", "last_scan_error",   "TEXT"),
            ("libraries", "schedule_time",     "TEXT"),
            ("libraries", "last_schedule_date", "TEXT"),
            ("libraries", "task",              "TEXT NOT NULL DEFAULT 'transcribe'"),
        ]
        for table, col, defn in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.commit()


_init_db()


def _get_job(job_id: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


def _update_job(job_id: str, **fields):
    if not fields:
        return
    clause = ", ".join(f"{k} = ?" for k in fields)
    with _db() as conn:
        conn.execute(f"UPDATE jobs SET {clause} WHERE id = ?", [*fields.values(), job_id])
        conn.commit()


def _delete_job_files(job: dict):
    if job.get("output_file"):
        Path(job["output_file"]).unlink(missing_ok=True)
    if not job.get("source_path"):
        for candidate in UPLOAD_DIR.glob(f"{job['id']}_*"):
            candidate.unlink(missing_ok=True)


def _job_is_attached_to_worker(conn: sqlite3.Connection, job_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM workers WHERE current_job = ? LIMIT 1",
        (job_id,),
    ).fetchone()
    return row is not None


def _get_job_status(job_id: str) -> Optional[str]:
    with _db() as conn:
        row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return row["status"] if row else None


def _get_library(lib_id: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute("SELECT * FROM libraries WHERE id = ?", (lib_id,)).fetchone()
        return dict(row) if row else None


def _update_library(lib_id: str, **fields):
    if not fields:
        return
    clause = ", ".join(f"{k} = ?" for k in fields)
    with _db() as conn:
        conn.execute(f"UPDATE libraries SET {clause} WHERE id = ?", [*fields.values(), lib_id])
        conn.commit()


def _is_file_queued_or_done(source_path: str, library_id: str, task: str) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT id FROM jobs WHERE source_path = ? AND library_id = ?"
            " AND COALESCE(task, 'transcribe') = ? AND status != 'failed'",
            (source_path, library_id, task),
        ).fetchone()
        return row is not None


def _has_subtitle(video_path: Path) -> bool:
    """Return True if a subtitle file already exists next to the video."""
    for ext in (".srt", ".vtt", ".txt"):
        if video_path.with_suffix(ext).exists():
            return True
    return False


def _is_transient_media_file(video_path: Path) -> bool:
    """Skip downloader fragments and temporary files until the final media exists."""
    return TRANSIENT_MEDIA_RE.search(video_path.name) is not None


def _is_media_still_changing(video_path: Path) -> bool:
    try:
        return time.time() - video_path.stat().st_mtime < MEDIA_SETTLE_SECONDS
    except FileNotFoundError:
        return True


# ---------------------------------------------------------------------------
# Library scanner
# ---------------------------------------------------------------------------

def _scan_library(lib: dict):
    lib_path = Path(lib["path"])
    task = lib.get("task") or "transcribe"
    now = datetime.utcnow().isoformat()
    if not lib_path.is_dir():
        _update_library(lib["id"], last_scan=now,
                        last_scan_error=f"Path not found or not a directory: {lib['path']}")
        print(f"[scan] library '{lib['name']}': path not found → {lib['path']}")
        return
    queued = 0
    for f in lib_path.rglob("*"):
        if not f.is_file() or f.suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        if _is_transient_media_file(f):
            continue
        if _is_media_still_changing(f):
            continue
        if task == "transcribe" and _has_subtitle(f):
            continue
        if _is_file_queued_or_done(str(f), lib["id"], task):
            continue
        job_id = str(uuid.uuid4())
        with _db() as conn:
            inserted = conn.execute(
                "INSERT INTO jobs (id, filename, source_path, library_id, preferred_worker,"
                " backend, status, progress, created_at, language, task, out_format, model_size)"
                " SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM libraries"
                " WHERE id = ? AND enabled = 1",
                (job_id, f.name, str(f), lib["id"],
                 lib.get("preferred_worker") or None,
                 lib.get("backend") or "faster_whisper",
                 "queued", 0, now,
                 lib["language"], task, lib["out_format"], lib["model_size"], lib["id"]),
            )
            conn.commit()
        queued += inserted.rowcount
    _update_library(lib["id"], last_scan=now, last_scan_error=None)
    print(f"[scan] library '{lib['name']}': queued {queued} new file(s)")


async def _library_watcher():
    while True:
        await asyncio.sleep(30)
        try:
            local_now = datetime.now()
            schedule_time = local_now.strftime("%H:%M")
            schedule_date = local_now.date().isoformat()
            with _db() as conn:
                libs = [dict(r) for r in
                        conn.execute(
                            "SELECT * FROM libraries WHERE enabled = 1"
                            " AND schedule_time = ?"
                            " AND COALESCE(last_schedule_date, '') != ?",
                            (schedule_time, schedule_date),
                        ).fetchall()]
            for lib in libs:
                _update_library(lib["id"], last_schedule_date=schedule_date)
                await asyncio.to_thread(_scan_library, lib)
        except Exception as e:
            print(f"[watcher] {e}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    asyncio.create_task(_library_watcher())
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Subgen Web UI", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return (Path(__file__).parent / "templates" / "index.html").read_text()


# --- Jobs ---

@app.post("/jobs", status_code=201)
async def create_job(
    file: UploadFile = File(...),
    language: str = Form("auto"),
    out_format: str = Form("srt"),
    model_size: str = Form("base"),
    preferred_worker: str = Form(""),
    task: str = Form("transcribe"),
    backend: str = Form("faster_whisper"),
):
    if task not in ("transcribe", "translate"):
        raise HTTPException(400, "Task must be transcribe or translate")
    job_id      = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{job_id}_{file.filename}"

    async with aiofiles.open(upload_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            await f.write(chunk)

    pw = preferred_worker.strip() or None
    be = backend.strip() or "faster_whisper"
    if be not in {"faster_whisper", "whispercpp_vulkan"}:
        raise HTTPException(400, "Unsupported backend")
    with _db() as conn:
        conn.execute(
            "INSERT INTO jobs (id, filename, preferred_worker, backend, status, progress,"
            " created_at, language, task, out_format, model_size) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, file.filename, pw, be, "queued", 0,
             datetime.utcnow().isoformat(), language, task, out_format, model_size),
        )
        conn.commit()

    return {"id": job_id}


@app.get("/jobs")
async def list_jobs():
    with _db() as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


@app.get("/jobs/current")
async def list_current_jobs():
    with _db() as conn:
        rows = conn.execute("""
            SELECT * FROM jobs
            WHERE status IN ('queued', 'processing', 'paused', 'pause_requested')
            ORDER BY
              CASE status
                WHEN 'processing' THEN 0
                WHEN 'pause_requested' THEN 1
                WHEN 'queued' THEN 2
                WHEN 'paused' THEN 3
                ELSE 4
              END,
              created_at
        """).fetchall()
        return [dict(r) for r in rows]


@app.post("/jobs/bulk-delete")
async def bulk_delete_jobs(ids: list[str] = Body(..., embed=True)):
    if not ids:
        return {"deleted": 0}
    placeholders = ",".join("?" for _ in ids)
    with _db() as conn:
        rows = conn.execute(f"SELECT * FROM jobs WHERE id IN ({placeholders})", ids).fetchall()
        jobs = [dict(r) for r in rows]
        deletable = [
            j for j in jobs
            if j["status"] != "processing"
            and not (j["status"] == "pause_requested" and _job_is_attached_to_worker(conn, j["id"]))
        ]
        for job in deletable:
            _delete_job_files(job)
        if deletable:
            delete_ids = [j["id"] for j in deletable]
            delete_placeholders = ",".join("?" for _ in delete_ids)
            conn.execute(f"DELETE FROM jobs WHERE id IN ({delete_placeholders})", delete_ids)
            conn.commit()
    return {"deleted": len(deletable), "skipped": len(jobs) - len(deletable)}


@app.get("/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.patch("/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    status = job["status"]
    if status == "queued":
        _update_job(job_id, status="paused", speed=None, eta=None)
    elif status == "processing":
        _update_job(job_id, status="pause_requested", speed=None, eta=None)
    elif status in {"paused", "pause_requested"}:
        return {"ok": True, "status": status}
    else:
        raise HTTPException(400, f"Cannot pause a {status} job")
    return {"ok": True, "status": _get_job_status(job_id)}


@app.patch("/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] == "pause_requested":
        raise HTTPException(409, "Worker is still pausing this job")
    if job["status"] != "paused":
        raise HTTPException(400, f"Cannot resume a {job['status']} job")
    _update_job(job_id, status="queued", worker_id=None, progress=0, speed=None, eta=None, error=None)
    return {"ok": True, "status": "queued"}


@app.get("/jobs/{job_id}/progress")
async def progress_stream(job_id: str):
    if not _get_job(job_id):
        raise HTTPException(404, "Job not found")

    async def stream() -> AsyncGenerator[str, None]:
        last_key = None
        while True:
            job = _get_job(job_id)
            if not job:
                break
            key = (job["status"], round(job["progress"] or 0), job.get("speed"), job.get("eta"))
            if key != last_key:
                yield f"data: {json.dumps(job)}\n\n"
                last_key = key
            if job["status"] in ("done", "failed", "paused"):
                break
            await asyncio.sleep(1)
        yield ": done\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/jobs/{job_id}/download")
async def download_job(job_id: str):
    job = _get_job(job_id)
    if not job or job["status"] != "done":
        raise HTTPException(404, "Output not ready")
    out_path = Path(job["output_file"])
    if not out_path.exists():
        raise HTTPException(404, "Output file missing")
    stem = Path(job["filename"]).stem
    return FileResponse(out_path, filename=f"{stem}.{job['out_format']}", media_type="text/plain")


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    with _db() as conn:
        if job["status"] == "processing" or (
            job["status"] == "pause_requested" and _job_is_attached_to_worker(conn, job_id)
        ):
            raise HTTPException(409, "Pause active jobs before deleting them")
        _delete_job_files(job)
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
    return {"ok": True}


@app.post("/jobs/{job_id}/queue/move")
async def move_queued_job(
    job_id: str, direction: str = Form(...), queue_worker: str = Form("")
):
    """Move a waiting job within the FIFO queue without affecting active work."""
    if direction not in ("up", "down"):
        raise HTTPException(400, "Direction must be 'up' or 'down'")

    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        worker_filter = ""
        params = []
        if queue_worker == "__unassigned__":
            worker_filter = " AND preferred_worker IS NULL"
        elif queue_worker:
            worker_filter = " AND preferred_worker = ?"
            params.append(queue_worker)
        rows = conn.execute(
            "SELECT id, created_at FROM jobs WHERE status = 'queued'"
            f"{worker_filter} ORDER BY created_at, id",
            params,
        ).fetchall()
        ids = [row["id"] for row in rows]
        if job_id not in ids:
            row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                raise HTTPException(404, "Job not found")
            raise HTTPException(409, "Only waiting jobs can be reordered")

        index = ids.index(job_id)
        target = index - 1 if direction == "up" else index + 1
        if 0 <= target < len(rows):
            conn.execute(
                "UPDATE jobs SET created_at = ? WHERE id = ?",
                (rows[target]["created_at"], rows[index]["id"]),
            )
            conn.execute(
                "UPDATE jobs SET created_at = ? WHERE id = ?",
                (rows[index]["created_at"], rows[target]["id"]),
            )
        conn.commit()
    return {"ok": True}


@app.get("/logs")
async def list_logs():
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'done'"
            " AND COALESCE(log_cleared, 0) = 0"
            " ORDER BY completed_at DESC, created_at DESC"
        ).fetchall()
    return [dict(row) for row in rows]


@app.delete("/logs")
async def clear_logs():
    with _db() as conn:
        result = conn.execute(
            "UPDATE jobs SET log_cleared = 1"
            " WHERE status = 'done' AND COALESCE(log_cleared, 0) = 0"
        )
        conn.commit()
    return {"ok": True, "cleared": result.rowcount}


@app.delete("/jobs/{job_id}/queue")
async def delete_queued_job(job_id: str):
    """Remove waiting work safely and keep dismissed library media out of scans."""
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status, source_path FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Job not found")
        if row["status"] != "queued":
            raise HTTPException(409, "This job has already started processing")
        if row["source_path"]:
            conn.execute(
                "UPDATE jobs SET status = 'cancelled', worker_id = NULL,"
                " progress = 0, speed = NULL, eta = NULL WHERE id = ?",
                (job_id,),
            )
        else:
            conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()

    if not row["source_path"]:
        for path in UPLOAD_DIR.glob(f"{job_id}_*"):
            path.unlink(missing_ok=True)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Docker Compose sync
# ---------------------------------------------------------------------------

def _load_compose():
    """Load docker-compose.yml, returning (yaml_instance, data) or (None, None)."""
    if not COMPOSE_PATH.exists():
        return None, None
    try:
        yml = YAML()
        yml.preserve_quotes = True
        with COMPOSE_PATH.open() as f:
            data = yml.load(f)
        return yml, data
    except Exception as exc:
        log.warning("compose: could not parse %s: %s", COMPOSE_PATH, exc)
        return None, None


def _save_compose(yml, data):
    try:
        with COMPOSE_PATH.open("w") as f:
            yml.dump(data, f)
    except Exception as exc:
        log.warning("compose: could not write %s: %s", COMPOSE_PATH, exc)


def _compose_service_for_worker(worker_id: str) -> Optional[str]:
    """Return the service name whose WORKER_ID env var matches worker_id, or None."""
    _, data = _load_compose()
    if not data:
        return None
    for svc_name, svc in (data.get("services") or {}).items():
        env = svc.get("environment") or []
        if isinstance(env, dict):
            if env.get("WORKER_ID") == worker_id:
                return svc_name
        else:  # list form: ["KEY=val", ...]
            for e in env:
                if str(e) == f"WORKER_ID={worker_id}":
                    return svc_name
    return None


def _compose_add_worker(worker_id: str):
    """Append a new worker service to docker-compose.yml if it isn't there yet."""
    yml, data = _load_compose()
    if data is None:
        return

    services = data.setdefault("services", {})
    volumes  = data.get("volumes") or {}

    # Use the worker_id as the service name (e.g. "worker-3")
    svc_name = worker_id
    if svc_name in services:
        return  # already present

    # Mirror command and volumes from an existing worker service.
    # Fall back to sensible defaults if no worker exists yet.
    ref_command = "python worker.py"
    ref_volumes = None
    for svc in services.values():
        env = svc.get("environment") or []
        has_worker_id = (
            (isinstance(env, dict) and "WORKER_ID" in env) or
            (isinstance(env, list) and any(str(e).startswith("WORKER_ID=") for e in env))
        )
        if has_worker_id:
            ref_command = svc.get("command", ref_command)
            ref_volumes = svc.get("volumes")
            break
    if ref_volumes is None:
        ref_volumes = [
            "uploads:/app/uploads",
            "outputs:/app/outputs",
            "models:/app/models",
            "db:/app",
        ]

    from ruamel.yaml.comments import CommentedMap, CommentedSeq
    new_svc = CommentedMap({
        "build": ".",
        "command": ref_command,
        "environment": [f"WORKER_ID={worker_id}", f"HOSTNAME={worker_id}"],
        "volumes": list(ref_volumes),
        "restart": "unless-stopped",
    })
    services[svc_name] = new_svc

    # Add to web's depends_on if the web service exists
    web = services.get("web")
    if web is not None:
        deps = web.get("depends_on")
        if deps is None:
            web["depends_on"] = CommentedSeq([svc_name])
        elif isinstance(deps, list):
            if svc_name not in deps:
                deps.append(svc_name)
        elif isinstance(deps, dict):
            if svc_name not in deps:
                deps[svc_name] = CommentedMap({"condition": "service_started"})

    _save_compose(yml, data)
    log.info("compose: added service '%s'", svc_name)


def _compose_add_vulkan_worker(worker_id: str):
    """Append a whisper.cpp/Vulkan GPU worker service to docker-compose.yml."""
    yml, data = _load_compose()
    if data is None:
        return

    services = data.setdefault("services", {})
    svc_name = worker_id
    if svc_name in services:
        return

    from ruamel.yaml.comments import CommentedMap
    new_svc = CommentedMap({
        "build": CommentedMap({
            "context": ".",
            "dockerfile": "Dockerfile.vulkan",
        }),
        "command": "python worker_whispercpp.py",
        "environment": [
            f"WORKER_ID={worker_id}",
            f"HOSTNAME={worker_id}",
            "WORKER_BACKEND=whispercpp_vulkan",
            "WORKER_DEVICE=RX 580 / Vulkan",
            "DB_PATH=/data/jobs.db",
            "WHISPER_CPP_BIN=/opt/whisper.cpp/build/bin/whisper-cli",
            "WHISPER_CPP_MODEL_DIR=/models",
        ],
        "devices": ["/dev/dri:/dev/dri"],
        "group_add": ["video", "render"],
        "volumes": [
            "uploads:/app/uploads",
            "outputs:/app/outputs",
            "models:/models",
            "db:/data",
            "/mnt/media:/mnt/media",
        ],
        "restart": "unless-stopped",
    })
    services[svc_name] = new_svc

    web = services.get("web")
    if web is not None:
        deps = web.get("depends_on")
        if isinstance(deps, list) and svc_name not in deps:
            deps.append(svc_name)

    _save_compose(yml, data)
    log.info("compose: added Vulkan service '%s'", svc_name)


def _compose_remove_worker(worker_id: str):
    """Remove the worker service for worker_id from docker-compose.yml."""
    yml, data = _load_compose()
    if data is None:
        return

    svc_name = _compose_service_for_worker(worker_id)
    if svc_name is None:
        return  # nothing to remove

    services = data.get("services") or {}
    services.pop(svc_name, None)

    # Remove from web's depends_on
    web = services.get("web")
    if web is not None:
        deps = web.get("depends_on")
        if isinstance(deps, list) and svc_name in deps:
            deps.remove(svc_name)
        elif isinstance(deps, dict) and svc_name in deps:
            del deps[svc_name]

    _save_compose(yml, data)
    log.info("compose: removed service '%s'", svc_name)


# --- Workers ---

def _worker_status(w: dict) -> str:
    if not w.get("last_seen"):
        return "offline"
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM workers WHERE id = ?"
            " AND datetime(last_seen) > datetime('now', '-15 seconds')",
            (w["id"],),
        ).fetchone()
    return ("busy" if w.get("current_job") else "idle") if row else "offline"


@app.get("/workers")
async def list_workers():
    with _db() as conn:
        rows = conn.execute("""
            SELECT w.id, w.name, w.host, w.last_seen, w.current_job,
                   w.backend, w.device, w.configured,
                   j.filename  AS job_filename,
                   j.progress  AS job_progress,
                   j.speed     AS job_speed,
                   j.eta       AS job_eta,
                   CASE
                     -- If the worker's current_job is actively processing, always show BUSY.
                     -- The heartbeat can lag during model download (ctranslate2 holds the GIL
                     -- for minutes), so we never rely on heartbeat freshness for busy workers.
                     WHEN j.status IN ('processing', 'pause_requested') THEN 'busy'
                     WHEN w.last_seen IS NULL THEN 'offline'
                     WHEN datetime(w.last_seen) > datetime('now', '-30 seconds')
                          AND w.current_job IS NOT NULL THEN 'busy'
                     WHEN datetime(w.last_seen) > datetime('now', '-30 seconds') THEN 'idle'
                     ELSE 'offline'
                   END AS status
            FROM workers w
            LEFT JOIN jobs j ON j.id = w.current_job
            ORDER BY w.configured DESC, w.name, w.id
        """).fetchall()
        return [dict(r) for r in rows]


@app.post("/workers", status_code=201)
async def add_worker(
    name: str = Form(...),
    worker_id: str = Form(...),
    backend: str = Form("faster_whisper"),
):
    wid = worker_id.strip()
    if not wid:
        raise HTTPException(400, "worker_id required")
    be = backend.strip() or "faster_whisper"
    if be not in {"faster_whisper", "whispercpp_vulkan"}:
        raise HTTPException(400, "Unsupported backend")
    device = "RX 580 / Vulkan" if be == "whispercpp_vulkan" else "CPU"
    with _db() as conn:
        conn.execute("""
            INSERT INTO workers (id, name, backend, device, configured)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                backend = excluded.backend,
                device = excluded.device,
                configured = 1
        """, (wid, name.strip(), be, device))
        conn.commit()
    if be == "whispercpp_vulkan":
        await asyncio.to_thread(_compose_add_vulkan_worker, wid)
    else:
        await asyncio.to_thread(_compose_add_worker, wid)
    return {"id": wid}


@app.delete("/workers/{worker_id}")
async def remove_worker(worker_id: str):
    with _db() as conn:
        conn.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
        conn.commit()
    await asyncio.to_thread(_compose_remove_worker, worker_id)
    return {"ok": True}


# --- Libraries ---

@app.get("/libraries")
async def list_libraries():
    with _db() as conn:
        rows = conn.execute("SELECT * FROM libraries ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


@app.post("/libraries", status_code=201)
async def create_library(
    name: str = Form(...), path: str = Form(...),
    model_size: str = Form("base"), language: str = Form("auto"),
    out_format: str = Form("srt"), preferred_worker: str = Form(""),
    schedule_time: str = Form(""),
    task: str = Form("transcribe"),
    backend: str = Form("faster_whisper"),
):
    if task not in ("transcribe", "translate"):
        raise HTTPException(400, "Task must be transcribe or translate")
    lib_id = str(uuid.uuid4())
    be = backend.strip() or "faster_whisper"
    if be not in {"faster_whisper", "whispercpp_vulkan"}:
        raise HTTPException(400, "Unsupported backend")
    with _db() as conn:
        conn.execute(
            "INSERT INTO libraries (id, name, path, model_size, language, out_format,"
            " preferred_worker, backend, enabled, created_at, schedule_time, task)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (lib_id, name, path, model_size, language, out_format,
             preferred_worker.strip() or None, be, 1, datetime.utcnow().isoformat(),
             schedule_time.strip() or None, task),
        )
        conn.commit()
    return {"id": lib_id}


@app.put("/libraries/{lib_id}")
async def update_library_endpoint(
    lib_id: str,
    name: str = Form(...), path: str = Form(...),
    model_size: str = Form("base"), language: str = Form("auto"),
    out_format: str = Form("srt"), preferred_worker: str = Form(""),
    backend: str = Form("faster_whisper"),
    enabled: int = Form(1),
    schedule_time: str = Form(""),
    task: str = Form("transcribe"),
):
    if task not in ("transcribe", "translate"):
        raise HTTPException(400, "Task must be transcribe or translate")
    if not _get_library(lib_id):
        raise HTTPException(404, "Library not found")
    be = backend.strip() or "faster_whisper"
    if be not in {"faster_whisper", "whispercpp_vulkan"}:
        raise HTTPException(400, "Unsupported backend")
    _update_library(lib_id, name=name, path=path, model_size=model_size,
                    language=language, task=task, out_format=out_format,
                    preferred_worker=preferred_worker.strip() or None,
                    backend=be,
                    enabled=enabled,
                    schedule_time=schedule_time.strip() or None,
                    last_schedule_date=None)
    return {"ok": True}


@app.patch("/libraries/{lib_id}/toggle")
async def toggle_library(lib_id: str, enabled: int = Form(...)):
    if not _get_library(lib_id):
        raise HTTPException(404, "Library not found")
    _update_library(lib_id, enabled=enabled)
    return {"ok": True}


@app.post("/libraries/{lib_id}/scan")
async def scan_library_now(lib_id: str, background_tasks: BackgroundTasks):
    lib = _get_library(lib_id)
    if not lib:
        raise HTTPException(404, "Library not found")
    background_tasks.add_task(_scan_library, lib)
    return {"ok": True}


@app.delete("/libraries/{lib_id}")
async def delete_library(lib_id: str):
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if not conn.execute("SELECT 1 FROM libraries WHERE id = ?", (lib_id,)).fetchone():
            raise HTTPException(404, "Library not found")
        conn.execute(
            "DELETE FROM jobs WHERE library_id = ? AND status = 'queued'",
            (lib_id,),
        )
        conn.execute("DELETE FROM libraries WHERE id = ?", (lib_id,))
        conn.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@app.get("/settings")
async def get_settings():
    with _db() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


@app.post("/settings")
async def save_settings(
    jellyfin_url:     str = Form(""),
    jellyfin_api_key: str = Form(""),
    webhook_url:      str = Form(""),
):
    pairs = [
        ("jellyfin_url",     jellyfin_url.strip()),
        ("jellyfin_api_key", jellyfin_api_key.strip()),
        ("webhook_url",      webhook_url.strip()),
    ]
    with _db() as conn:
        for key, value in pairs:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        conn.commit()
    return {"ok": True}
