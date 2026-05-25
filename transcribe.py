"""Shared transcription utilities imported by both app.py and worker.py."""
import json
import subprocess
from pathlib import Path
from typing import Optional

from faster_whisper import WhisperModel

_model_cache: dict[str, WhisperModel] = {}


def get_duration(path: Path) -> Optional[float]:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        return float(json.loads(r.stdout)["format"]["duration"])
    except Exception:
        return None


def load_model(model_size: str, models_dir: str = "models") -> WhisperModel:
    if model_size not in _model_cache:
        _model_cache[model_size] = WhisperModel(
            model_size, device="auto", compute_type="int8",
            download_root=models_dir,
        )
    return _model_cache[model_size]


def _ts(seconds: float, vtt: bool = False) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    sep = "." if vtt else ","
    return f"{h:02d}:{m:02d}:{s:02d}{sep}{ms:03d}"


def to_srt(segments) -> str:
    blocks = []
    for i, seg in enumerate(segments, 1):
        blocks.append(f"{i}\n{_ts(seg.start)} --> {_ts(seg.end)}\n{seg.text.strip()}")
    return "\n\n".join(blocks)


def to_vtt(segments) -> str:
    blocks = ["WEBVTT"]
    for seg in segments:
        blocks.append(f"{_ts(seg.start, vtt=True)} --> {_ts(seg.end, vtt=True)}\n{seg.text.strip()}")
    return "\n\n".join(blocks)


def to_txt(segments) -> str:
    return "\n".join(seg.text.strip() for seg in segments)
