"""
Subgen Web UI — coordinator process.
Handles HTTP, job creation, SSE progress (DB-poll), library scanning, worker management.
Transcription is handled by worker.py — run one or more workers alongside this.
"""
import asyncio
import json
import logging
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiofiles
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from ruamel.yaml import YAML

log = logging.getLogger(__name__)

UPLOAD_DIR   = Path("uploads")
OUTPUT_DIR   = Path("outputs")
DB_PATH      = Path("jobs.db")
COMPOSE_PATH = Path("docker-compose.yml")

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".m4v", ".wmv", ".mpg", ".mpeg",
    ".ts", ".m2ts", ".webm", ".flv", ".3gp",
    ".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus",
}

for d in [UPLOAD_DIR, OUTPUT_DIR]:
    d.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_db():
    with _db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id                TEXT PRIMARY KEY,
                filename          TEXT NOT NULL,
                source_path       TEXT,
                library_id        TEXT,
                worker_id         TEXT,
                preferred_worker  TEXT,
                status            TEXT NOT NULL DEFAULT 'queued',
                progress          REAL NOT NULL DEFAULT 0,
                created_at        TEXT NOT NULL,
                completed_at      TEXT,
                output_file       TEXT,
                error             TEXT,
                language          TEXT,
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
                out_format       TEXT NOT NULL DEFAULT 'srt',
                preferred_worker TEXT,
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
                configured  INTEGER NOT NULL DEFAULT 0
            )
        """)
        # Migrate older schemas
        migrations = [
            ("jobs",      "source_path",      "TEXT"),
            ("jobs",      "library_id",        "TEXT"),
            ("jobs",      "worker_id",         "TEXT"),
            ("jobs",      "preferred_worker",  "TEXT"),
            ("jobs",      "speed",             "REAL"),
            ("jobs",      "eta",               "INTEGER"),
            ("workers",   "name",              "TEXT"),
            ("workers",   "configured",        "INTEGER NOT NULL DEFAULT 0"),
            ("libraries", "preferred_worker",  "TEXT"),
            ("libraries", "last_scan_error",   "TEXT"),
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


def _is_file_queued_or_done(source_path: str) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT id FROM jobs WHERE source_path = ? AND status != 'failed'",
            (source_path,),
        ).fetchone()
        return row is not None


# ---------------------------------------------------------------------------
# Library scanner
# ---------------------------------------------------------------------------

def _scan_library(lib: dict):
    lib_path = Path(lib["path"])
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
        if _is_file_queued_or_done(str(f)):
            continue
        job_id = str(uuid.uuid4())
        with _db() as conn:
            conn.execute(
                "INSERT INTO jobs (id, filename, source_path, library_id, preferred_worker,"
                " status, progress, created_at, out_format, model_size)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (job_id, f.name, str(f), lib["id"],
                 lib.get("preferred_worker") or None,
                 "queued", 0, now,
                 lib["out_format"], lib["model_size"]),
            )
            conn.commit()
        queued += 1
    _update_library(lib["id"], last_scan=now, last_scan_error=None)
    print(f"[scan] library '{lib['name']}': queued {queued} new file(s)")


async def _library_watcher():
    while True:
        await asyncio.sleep(60)
        try:
            with _db() as conn:
                libs = [dict(r) for r in
                        conn.execute("SELECT * FROM libraries WHERE enabled = 1").fetchall()]
            for lib in libs:
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
):
    job_id      = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{job_id}_{file.filename}"

    async with aiofiles.open(upload_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            await f.write(chunk)

    pw = preferred_worker.strip() or None
    with _db() as conn:
        conn.execute(
            "INSERT INTO jobs (id, filename, preferred_worker, status, progress,"
            " created_at, out_format, model_size) VALUES (?,?,?,?,?,?,?,?)",
            (job_id, file.filename, pw, "queued", 0,
             datetime.utcnow().isoformat(), out_format, model_size),
        )
        conn.commit()

    return {"id": job_id}


@app.get("/jobs")
async def list_jobs():
    with _db() as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


@app.get("/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


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
            if job["status"] in ("done", "failed"):
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
    if job.get("output_file"):
        Path(job["output_file"]).unlink(missing_ok=True)
    with _db() as conn:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
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
            SELECT w.id, w.name, w.host, w.last_seen, w.current_job, w.configured,
                   j.filename  AS job_filename,
                   j.progress  AS job_progress,
                   j.speed     AS job_speed,
                   j.eta       AS job_eta,
                   CASE
                     WHEN w.last_seen IS NULL THEN 'offline'
                     WHEN datetime(w.last_seen) > datetime('now', '-15 seconds')
                          AND w.current_job IS NOT NULL THEN 'busy'
                     WHEN datetime(w.last_seen) > datetime('now', '-15 seconds') THEN 'idle'
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
):
    wid = worker_id.strip()
    if not wid:
        raise HTTPException(400, "worker_id required")
    with _db() as conn:
        conn.execute("""
            INSERT INTO workers (id, name, configured)
            VALUES (?, ?, 1)
            ON CONFLICT(id) DO UPDATE SET name = excluded.name, configured = 1
        """, (wid, name.strip()))
        conn.commit()
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
):
    lib_id = str(uuid.uuid4())
    with _db() as conn:
        conn.execute(
            "INSERT INTO libraries (id, name, path, model_size, language, out_format,"
            " preferred_worker, enabled, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (lib_id, name, path, model_size, language, out_format,
             preferred_worker.strip() or None, 1, datetime.utcnow().isoformat()),
        )
        conn.commit()
    return {"id": lib_id}


@app.put("/libraries/{lib_id}")
async def update_library_endpoint(
    lib_id: str,
    name: str = Form(...), path: str = Form(...),
    model_size: str = Form("base"), language: str = Form("auto"),
    out_format: str = Form("srt"), preferred_worker: str = Form(""),
    enabled: int = Form(1),
):
    if not _get_library(lib_id):
        raise HTTPException(404, "Library not found")
    _update_library(lib_id, name=name, path=path, model_size=model_size,
                    language=language, out_format=out_format,
                    preferred_worker=preferred_worker.strip() or None,
                    enabled=enabled)
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
    if not _get_library(lib_id):
        raise HTTPException(404, "Library not found")
    with _db() as conn:
        conn.execute("DELETE FROM libraries WHERE id = ?", (lib_id,))
        conn.commit()
    return {"ok": True}
