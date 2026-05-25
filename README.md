# Subgen Web UI

A self-hosted web interface for generating subtitles using [faster-whisper](https://github.com/SYSTRAN/faster-whisper). Upload a video or audio file, pick a model, and get an SRT/VTT/TXT subtitle file back — all from the browser.

![Workers tab showing live queue with progress, speed, and ETA](https://placehold.co/800x400/161616/6366f1?text=Subgen+Web+UI)

## Features

- **Drag-and-drop upload** — MP4, MKV, AVI, MOV, MP3, WAV, FLAC, and more
- **Live progress tracking** — real-time speed (×), ETA, and progress bar per job
- **Multiple workers** — run as many transcription workers as you have resources for, each processes one job at a time
- **Worker tab** — see every worker's status (idle/busy/offline), live queue position, speed and ETA for each active job
- **Library scanning** — point Subgen at a folder and it auto-queues new files every 60 seconds
- **Docker Compose sync** — adding or removing a worker in the UI automatically updates `docker-compose.yml`
- **Preferred worker** — pin a job or library to a specific worker (e.g. a GPU machine)
- **Output formats** — SRT, WebVTT, plain text
- **Models** — tiny · base · small · medium · large-v2 · large-v3

---

## Quick Start

### Docker Compose (recommended)

```bash
git clone https://github.com/S1mple32/subgen-webui.git
cd subgen-webui
docker compose up -d
```

Then open **http://localhost:8000**.

The default `docker-compose.yml` starts one web server and two workers. To add more workers, use the **Workers** tab in the UI — the compose file is updated automatically.

### Mount your media folders

Edit `docker-compose.yml` and add volume mounts for your media directories:

```yaml
services:
  web:
    volumes:
      - /path/to/movies:/media/movies
      - /path/to/shows:/media/shows
  worker-1:
    volumes:
      - /path/to/movies:/media/movies
      - /path/to/shows:/media/shows
```

Then add them as Libraries in the UI.

---

## Running Locally (without Docker)

**Requirements:** Python 3.9+, `ffmpeg` installed and on your PATH.

```bash
git clone https://github.com/S1mple32/subgen-webui.git
cd subgen-webui

pip install -r requirements.txt

# Terminal 1 — web server
uvicorn app:app --host 0.0.0.0 --port 8000

# Terminal 2 — worker (add more terminals for more workers)
WORKER_ID=worker-1 python worker.py
```

---

## Configuration

All settings are environment variables:

| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | random 8-char ID | Unique ID for this worker |
| `HOSTNAME` | system hostname | Display name in the UI |
| `DB_PATH` | `jobs.db` | Path to the SQLite database |
| `UPLOAD_DIR` | `uploads/` | Where uploaded files are stored |
| `OUTPUT_DIR` | `outputs/` | Where subtitle files are written |
| `MODELS_DIR` | `models/` | Where Whisper models are cached |
| `POLL_INTERVAL` | `2` | Seconds between job queue checks |

---

## Architecture

```
Browser  ──HTTP──▶  app.py (FastAPI)  ──SQLite──▶  worker.py × N
                        │                               │
                    uploads/                        outputs/
```

- **`app.py`** — HTTP server, file uploads, SSE progress streaming, library watcher
- **`worker.py`** — polls SQLite for queued jobs, runs faster-whisper, writes output files
- **`transcribe.py`** — shared utilities (model loading, SRT/VTT/TXT formatting)
- **`jobs.db`** — SQLite database shared between the web server and all workers

Workers claim jobs atomically using `BEGIN IMMEDIATE` transactions — no coordination layer needed, just add more workers and they share the queue automatically.

---

## Models

| Model | Size | Speed | Quality |
|---|---|---|---|
| tiny | ~75 MB | Fastest | Low |
| base | ~145 MB | Fast | OK |
| small | ~460 MB | Medium | Good |
| medium | ~1.5 GB | Slow | Great |
| large-v2 | ~3 GB | Slowest | Best |
| large-v3 | ~3 GB | Slowest | Best |

Models are downloaded automatically on first use and cached in the `models/` directory.

---

## License

MIT
