#!/usr/bin/env python3
"""auto-clip — hands-off live/VOD clipper for ClippyMe.

One link in, finished vertical clips out:

  auto-clip submit <youtube|twitch|kick url>

A long-lived worker probes the link, waits for scheduled streams, records live
ones with streamlink+ffmpeg, feeds 20-minute MP4 chunks to the local ClippyMe
backend, collects the rendered shorts in ``library/`` and optionally uploads
them with ``youtube-kit``.  All state lives in SQLite, so the service survives
reboots and is restarted by systemd.

Stdlib only.  No Docker, no root, no sudo.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import logging
import logging.handlers
import os
import re
import shutil
import signal
import socket
import sqlite3
import ssl
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

VERSION = "1.0.0"

BASE = Path(os.environ.get("AUTO_CLIP_HOME") or Path(__file__).resolve().parent).resolve()
PROJECT = BASE.parent
CLIPPYME = PROJECT / "clippyme"
STATE_DIR = BASE / "state"
CAPTURE_DIR = BASE / "captures"
LIBRARY_DIR = BASE / "library"
LOG_DIR = BASE / "logs"
DB_PATH = STATE_DIR / "auto_clip.db"
LOCK_PATH = STATE_DIR / "daemon.lock"
PID_PATH = STATE_DIR / "daemon.pid"
CONFIG_PATH = Path(os.environ.get("AUTO_CLIP_CONFIG") or (BASE / "config.json"))

TERMINAL_ITEMS = ("completed", "failed", "cancelled")
ACTIVE_CHUNK_STATES = ("submitting", "submitted", "processing")
TERMINAL_CHUNK_STATES = ("done", "failed", "skipped", "cancelled")

SOURCE_HOSTS = frozenset({
    "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com",
    "youtu.be", "youtube-nocookie.com", "www.youtube-nocookie.com",
    "twitch.tv", "www.twitch.tv", "m.twitch.tv", "clips.twitch.tv",
    "kick.com", "www.kick.com",
})

DEFAULT_CONFIG = {
    "tools": {
        "ffmpeg": "vendor/ffmpeg/bin/ffmpeg",
        "ffprobe": "vendor/ffmpeg/bin/ffprobe",
        "lib_dir": "vendor/ffmpeg/lib",
        "streamlink": "",
    },
    "clippyme": {
        "base_url": "http://127.0.0.1:8000",
        "output_dir": "../clippyme/output",
        "python": "../clippyme/.venv/bin/python",
        "cookies": "../clippyme/data/cookies.txt",
        "poll_seconds": 8,
    },
    "capture": {
        "segment_seconds": 60,
        "min_chunk_seconds": 15,
        "max_height": 720,
        "stall_timeout_seconds": 180,
        "retry_backoff_seconds": 45,
        "max_session_failures": 8,
        "max_concurrent_streams": 4,
        "disk_guard_free_gb": 8,
        "prelive_skip_seconds": 0,
        "live_edge": 2,
        "keep_source_seconds": 1800,
    },
    "chat": {
        "enabled": False,
        "window_seconds": 10,
        "baseline_windows": 12,
        "min_history": 3,
        "min_baseline": 2.0,
        "spike_multiplier": 3.0,
        "spike_rise_fraction": 0.75,
        "spike_min_rise": 6.0,
        "spike_min_messages": 8,
        "cooldown_seconds": 90,
        "pre_seconds": 20,
        "post_seconds": 10,
        "poll_seconds": 3,
        "max_clip_seconds": 59,
        "compose_subtitles": True,
    },
    "probe": {
        "timeout_seconds": 180,
        "upcoming_poll_seconds": 240,
        "retry_seconds": 120,
        "max_attempts": 30,
    },
    "analysis": {
        "aspect": "9:16",
        "reframe_mode": "auto",
        "language": "en",
        "instructions": (
            "Pick the most viral, entertaining moments. Prefer complete moments "
            "with a clear hook in the first two seconds, strong reactions, funny "
            "lines, or high-energy gameplay. Avoid intros, dead air, filler and "
            "unfinished sentences. Keep each clip self-contained."
        ),
    },
    "publish": {
        "enabled": True,
        "privacy": "private",
        "channel": "",
        "category_id": "20",
        "language": "en-US",
        "audio_language": "en-US",
        "tags": ["gta 6", "gta vi", "gta 6 gameplay", "gta 6 clips", "shorts"],
        "hashtags": "#GTA6 #GTAVI #Gaming #Shorts",
        "title_suffix": " #Shorts",
        "min_gap_minutes": 15,
        "max_per_day": 6,
        "compose_subtitles": True,
        "upload_caption_tracks": True,
        "caption_language": "en-US",
        "ytk": "../youtube-kit/ytk",
        "upload_timeout_seconds": 7200,
    },
    "watchlist": {
        "require_gta_title": True,
        "channels": [],
    },
    "retention": {
        "delete_chunks": True,
        "delete_job_output": True,
    },
}

log = logging.getLogger("auto-clip")

# Resolved by init_runtime().
CFG: dict = {}
DB_READY = False
OUTPUT_DIR = CLIPPYME / "output"
PYTHON = str(CLIPPYME / ".venv" / "bin" / "python")
FFMPEG = "ffmpeg"
FFPROBE = "ffprobe"
STREAMLINK = "streamlink"
CURL = "curl"
YTK = str(PROJECT / "youtube-kit" / "ytk")


class AutoClipError(Exception):
    """Base error for expected, user-facing failures."""


class ProbeError(AutoClipError):
    def __init__(self, message: str, *, permanent: bool = False, not_live: bool = False):
        super().__init__(message)
        self.permanent = permanent
        self.not_live = not_live


class ApiError(AutoClipError):
    def __init__(self, message: str, *, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


# --------------------------------------------------------------------------
# Config / runtime
# --------------------------------------------------------------------------

def deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config() -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if CONFIG_PATH.exists():
        try:
            user_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise AutoClipError(f"cannot read {CONFIG_PATH}: {exc}") from exc
        if not isinstance(user_cfg, dict):
            raise AutoClipError(f"{CONFIG_PATH} must contain a JSON object")
        cfg = deep_merge(cfg, user_cfg)
    return cfg


def project_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = BASE / path
    return path.resolve()


def executable_path(value: str) -> Path:
    """Like project_path but keeps venv symlinks intact."""
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = BASE / path
    return Path(os.path.abspath(path))


def ensure_dirs() -> None:
    for directory in (STATE_DIR, CAPTURE_DIR, LIBRARY_DIR, LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def find_exe(name: str, *extra: str) -> str:
    for candidate in (shutil.which(name), *extra):
        if candidate and Path(candidate).exists():
            return str(candidate)
    return name


def init_runtime(*, with_db: bool = True) -> dict:
    global CFG, DB_READY, OUTPUT_DIR, PYTHON, FFMPEG, FFPROBE, STREAMLINK, CURL, YTK
    CFG = load_config()
    ensure_dirs()

    OUTPUT_DIR = project_path(CFG["clippyme"]["output_dir"])
    PYTHON = str(executable_path(CFG["clippyme"]["python"]))
    YTK = str(executable_path(CFG["publish"]["ytk"]))

    tools = CFG.get("tools") or {}
    lib_dir = project_path(tools["lib_dir"]) if tools.get("lib_dir") else None
    if lib_dir and lib_dir.exists():
        current = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = str(lib_dir) + (f":{current}" if current else "")
    configured_ffmpeg = executable_path(tools["ffmpeg"]) if tools.get("ffmpeg") else None
    if configured_ffmpeg and configured_ffmpeg.exists():
        FFMPEG = str(configured_ffmpeg)
    else:
        FFMPEG = find_exe("ffmpeg", str(Path.home() / ".local" / "bin" / "ffmpeg"))
    configured_ffprobe = executable_path(tools["ffprobe"]) if tools.get("ffprobe") else None
    if configured_ffprobe and configured_ffprobe.exists():
        FFPROBE = str(configured_ffprobe)
    elif Path(FFMPEG).with_name("ffprobe").exists():
        FFPROBE = str(Path(FFMPEG).with_name("ffprobe"))
    else:
        FFPROBE = find_exe("ffprobe", str(Path.home() / ".local" / "bin" / "ffprobe"))
    configured_streamlink = executable_path(tools["streamlink"]) if tools.get("streamlink") else None
    STREAMLINK = (str(configured_streamlink) if configured_streamlink and configured_streamlink.exists()
                  else find_exe("streamlink", str(Path.home() / ".local" / "bin" / "streamlink")))
    CURL = find_exe("curl")

    if with_db and not DB_READY:
        init_db()
        DB_READY = True
    return CFG


def setup_logging(*, daemon: bool = False, verbose: bool = False) -> None:
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s")
    if daemon:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            LOG_DIR / "auto-clip.log", maxBytes=10_000_000, backupCount=3, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    else:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)
        root.setLevel(logging.DEBUG if verbose else logging.WARNING)


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL,
    title TEXT,
    source_id TEXT,
    duration REAL,
    work_dir TEXT,
    job_id TEXT,
    publish INTEGER NOT NULL DEFAULT 1,
    instructions TEXT,
    error TEXT,
    note TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    next_action_at REAL NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    meta TEXT NOT NULL DEFAULT '{}'
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_active_url
    ON items(normalized_url)
    WHERE status NOT IN ('completed', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status, next_action_at, created_at);

CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    ts_path TEXT,
    mp4_path TEXT,
    duration REAL,
    size INTEGER,
    status TEXT NOT NULL,
    job_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    error TEXT,
    started_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    meta TEXT NOT NULL DEFAULT '{}',
    UNIQUE (item_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_chunks_status ON chunks(status, created_at);

CREATE TABLE IF NOT EXISTS spikes (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    detected_at REAL NOT NULL,
    bucket_start REAL NOT NULL,
    bucket_end REAL NOT NULL,
    window_start REAL NOT NULL,
    window_end REAL NOT NULL,
    count INTEGER NOT NULL,
    baseline REAL NOT NULL,
    status TEXT NOT NULL,
    cut_path TEXT,
    job_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (item_id, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_spikes_status ON spikes(status, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS clips (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL,
    clip_index INTEGER NOT NULL,
    spike_id TEXT,
    source_path TEXT,
    library_path TEXT,
    cover_path TEXT,
    title TEXT,
    description TEXT,
    score REAL,
    status TEXT NOT NULL,
    youtube_video_id TEXT,
    privacy TEXT,
    published_at REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    error TEXT,
    meta TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (job_id, clip_index)
);
CREATE INDEX IF NOT EXISTS idx_clips_status ON clips(status, next_attempt_at, published_at);

CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def init_db() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        for table, column, decl in (
            ("chunks", "started_at", "REAL"),
            ("clips", "spike_id", "TEXT"),
        ):
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if column not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        conn.execute(
            "UPDATE chunks SET started_at = created_at - COALESCE(duration, 0)"
            " WHERE started_at IS NULL"
        )
        conn.execute(
            "INSERT OR IGNORE INTO kv(key, value) VALUES('schema_version', '2')"
        )
        conn.commit()
    finally:
        conn.close()


@contextlib.contextmanager
def db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def row_dict(row) -> dict | None:
    return dict(row) if row is not None else None


def get_item(item_id: str) -> dict | None:
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone())


def get_chunk(chunk_id: str) -> dict | None:
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM chunks WHERE id=?", (chunk_id,)).fetchone())


def get_clip(clip_id: str) -> dict | None:
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM clips WHERE id=?", (clip_id,)).fetchone())


def update_item(item_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{key}=?" for key in fields)
    with db() as conn:
        conn.execute(f"UPDATE items SET {assignments} WHERE id=?", (*fields.values(), item_id))


def update_chunk(chunk_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{key}=?" for key in fields)
    with db() as conn:
        conn.execute(f"UPDATE chunks SET {assignments} WHERE id=?", (*fields.values(), chunk_id))


def update_clip(clip_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{key}=?" for key in fields)
    with db() as conn:
        conn.execute(f"UPDATE clips SET {assignments} WHERE id=?", (*fields.values(), clip_id))


def item_meta(item: dict) -> dict:
    try:
        value = json.loads(item.get("meta") or "{}")
        return value if isinstance(value, dict) else {}
    except ValueError:
        return {}


def merge_item_meta(item_id: str, **updates) -> None:
    item = get_item(item_id)
    if not item:
        return
    meta = item_meta(item)
    meta.update(updates)
    update_item(item_id, meta=json.dumps(meta, ensure_ascii=False))


def find_item(prefix: str) -> dict:
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM items WHERE id LIKE ? ORDER BY created_at DESC LIMIT 2",
            (prefix + "%",),
        ).fetchall()
    if not rows:
        raise AutoClipError(f"no item matches {prefix!r}")
    if len(rows) > 1:
        raise AutoClipError(f"{prefix!r} is ambiguous; use more characters")
    return dict(rows[0])


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def now() -> float:
    return time.time()


def ts_text(value: float | None) -> str:
    if not value:
        return "-"
    return dt.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")


def age_text(value: float | None) -> str:
    if not value:
        return "-"
    seconds = max(0, int(time.time() - value))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    return f"{seconds // 86400}d"


def slugify(value: str, limit: int = 60) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value or "").strip("-._")
    return (cleaned[:limit].strip("-._") or "clip")


def clean_text(value: str | None, limit: int = 0) -> str:
    text = (value or "").replace("\x00", " ")
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if limit and len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def disk_free_gb(path: Path) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return 0.0


def terminate_group(proc: subprocess.Popen | None, *, grace: float = 8.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=grace)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def run(cmd: list[str], *, timeout: float | None = None, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout
    )
    if check and result.returncode != 0:
        raise AutoClipError(
            f"command failed ({result.returncode}): {' '.join(cmd[:4])}\n{result.stderr[-1200:]}"
        )
    return result


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)

# --------------------------------------------------------------------------
# URLs
# --------------------------------------------------------------------------

def validate_source_url(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        raise AutoClipError("empty URL")
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme not in ("http", "https"):
        raise AutoClipError("URL must start with http:// or https://")
    host = (parsed.hostname or "").lower()
    if host not in SOURCE_HOSTS:
        raise AutoClipError(
            f"unsupported host {host!r}; expected YouTube, Twitch or Kick"
        )
    return normalize_url(text)


def normalize_url(raw: str) -> str:
    parsed = urllib.parse.urlsplit(raw.strip())
    host = (parsed.hostname or "").lower()
    path = parsed.path or "/"

    if host == "youtu.be":
        video_id = path.strip("/").split("/")[0]
        if not video_id:
            raise AutoClipError("missing video id in youtu.be URL")
        return f"https://www.youtube.com/watch?v={video_id}"

    if host.endswith("youtube.com") or host.endswith("youtube-nocookie.com"):
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("v"):
            return f"https://www.youtube.com/watch?v={query['v'][0]}"
        match = re.match(r"^/(?:shorts|live|embed)/([A-Za-z0-9_-]{6,})", path)
        if match:
            return f"https://www.youtube.com/watch?v={match.group(1)}"
        clean_path = path.rstrip("/")
        # A bare channel link means "that channel's live stream", not its VOD
        # catalogue.  Resolving to /live lets the same one-link contract wait
        # for an upcoming stream instead of downloading the latest upload.
        if re.match(
            r"^/(?:@[^/]+|channel/[^/]+|c/[^/]+|user/[^/]+)(?:/streams)?$",
            clean_path,
        ):
            return "https://www.youtube.com" + clean_path.removesuffix("/streams") + "/live"
        return "https://www.youtube.com" + clean_path

    if host in ("clips.twitch.tv",):
        return "https://clips.twitch.tv" + path.rstrip("/")

    if host in ("twitch.tv", "www.twitch.tv", "m.twitch.tv"):
        return "https://www.twitch.tv" + path.rstrip("/")

    if host in ("kick.com", "www.kick.com"):
        return "https://kick.com" + path.rstrip("/")

    return f"https://{host}{path.rstrip('/')}"


def is_supported_url(raw: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(raw)
        return parsed.scheme in ("http", "https") and (parsed.hostname or "").lower() in SOURCE_HOSTS
    except ValueError:
        return False


# --------------------------------------------------------------------------
# ClippyMe API
# --------------------------------------------------------------------------

def api_base() -> str:
    return str(CFG["clippyme"]["base_url"]).rstrip("/")


def api_json(method: str, path: str, payload: dict | None = None, *, timeout: float = 60):
    url = api_base() + path
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise ApiError(f"HTTP {exc.code} on {path}: {body[:500]}", status=exc.code, body=body) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ApiError(f"backend unreachable ({path}): {exc}") from exc
    if not body:
        return {}
    try:
        return json.loads(body)
    except ValueError as exc:
        raise ApiError(f"invalid JSON from {path}: {body[:300]}") from exc


def backend_healthy() -> bool:
    try:
        api_json("GET", "/api/health", timeout=5)
        return True
    except ApiError:
        return False


def wait_for_backend(stop: threading.Event, *, log_every: float = 30) -> bool:
    last = 0.0
    while not stop.is_set():
        if backend_healthy():
            return True
        if time.time() - last >= log_every:
            log.warning("waiting for ClippyMe backend at %s", api_base())
            last = time.time()
        stop.wait(5)
    return False


def submit_url_job(url: str, instructions: str | None) -> str:
    payload = {
        "url": url,
        "aspect": CFG["analysis"]["aspect"],
        "reframe_mode": CFG["analysis"]["reframe_mode"],
        "language": CFG["analysis"]["language"],
        "instructions": instructions or None,
    }
    response = api_json("POST", "/api/process", payload, timeout=120)
    job_id = response.get("job_id")
    if not job_id:
        raise AutoClipError(f"backend returned no job_id: {response}")
    return job_id


def submit_file_job(
    path: Path,
    instructions: str | None,
    *,
    clip_start: float | None = None,
    clip_end: float | None = None,
) -> str:
    """Multipart upload through curl (streams from disk, survives large chunks)."""
    cmd = [
        CURL, "-sS", "--fail-with-body", "--connect-timeout", "30",
        "--speed-time", "180", "--speed-limit", "2048",
        "-X", "POST", api_base() + "/api/process",
        "-F", f"file=@{path};type=video/mp4",
        "--form-string", f"aspect={CFG['analysis']['aspect']}",
        "--form-string", f"reframe_mode={CFG['analysis']['reframe_mode']}",
        "--form-string", f"language={CFG['analysis']['language']}",
    ]
    if instructions:
        cmd += ["--form-string", f"instructions={instructions}"]
    if clip_start is not None and clip_end is not None:
        cmd += [
            "--form-string", f"clip_start={float(clip_start):.3f}",
            "--form-string", f"clip_end={float(clip_end):.3f}",
        ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    body = (result.stdout or "").strip()
    if result.returncode != 0:
        detail = body or (result.stderr or "").strip()[-800:]
        raise AutoClipError(f"upload failed (curl {result.returncode}): {detail}")
    try:
        response = json.loads(body)
    except ValueError as exc:
        raise AutoClipError(f"upload returned invalid JSON: {body[:500]}") from exc
    job_id = response.get("job_id")
    if not job_id:
        raise AutoClipError(f"backend returned no job_id: {response}")
    return job_id


def job_status(job_id: str) -> dict:
    return api_json("GET", f"/api/status/{urllib.parse.quote(job_id)}", timeout=60)


def compose_clip(
    job_id: str,
    clip_index: int,
    toggles: dict | None = None,
    *,
    include_caption_file: bool = False,
) -> Path:
    """Burn the configured overlays (subtitles) on a finished clip."""
    response = api_json(
        "POST",
        f"/api/compose/{urllib.parse.quote(job_id)}/{int(clip_index)}",
        {
            "toggles": toggles or {"subtitles": True},
            "include_caption_file": include_caption_file,
        },
        timeout=3600,
    )
    composed_url = str(response.get("composed_url") or "")
    if not composed_url:
        raise AutoClipError(f"compose returned no file: {response}")
    relative = urllib.parse.unquote(composed_url)
    prefix = "/videos/"
    if relative.startswith(prefix):
        relative = relative[len(prefix):]
    path = OUTPUT_DIR / relative
    if not path.exists() or path.stat().st_size == 0:
        raise AutoClipError(f"composed clip is missing: {path}")
    if include_caption_file:
        caption_srt = response.get("caption_srt")
        if not isinstance(caption_srt, str) or not caption_srt.strip():
            raise AutoClipError("compose returned no caption sidecar")
        sidecar = path.with_suffix(".en.srt")
        atomic_write(sidecar, caption_srt)
    return path


def cancel_remote_job(job_id: str) -> None:
    with contextlib.suppress(ApiError):
        api_json("POST", f"/api/cancel/{urllib.parse.quote(job_id)}", {}, timeout=30)


# --------------------------------------------------------------------------
# yt-dlp probing
# --------------------------------------------------------------------------

def ytdlp_base() -> list[str]:
    python = PYTHON if Path(PYTHON).exists() else sys.executable
    cmd = [
        python, "-m", "yt_dlp", "--no-warnings", "--no-playlist",
        "--socket-timeout", "20",
    ]
    cookies = project_path(CFG["clippyme"]["cookies"])
    if cookies.exists() and cookies.stat().st_size > 0:
        cmd += ["--cookies", str(cookies)]
    return cmd


def ytdlp_command(url: str) -> list[str]:
    return ytdlp_base() + ["-J", url]


def is_youtube_url(raw: str) -> bool:
    try:
        host = (urllib.parse.urlsplit(raw).hostname or "").lower()
    except ValueError:
        return False
    return host in ("youtu.be", "youtube.com", "www.youtube.com", "m.youtube.com",
                    "music.youtube.com", "youtube-nocookie.com",
                    "www.youtube-nocookie.com")


def is_twitch_url(raw: str) -> bool:
    try:
        host = (urllib.parse.urlsplit(raw).hostname or "").lower()
    except ValueError:
        return False
    return host in ("twitch.tv", "www.twitch.tv", "m.twitch.tv")


def twitch_channel(raw: str) -> str | None:
    """Extract the channel login from a Twitch live URL."""
    try:
        path = urllib.parse.urlsplit(raw).path
    except ValueError:
        return None
    parts = [part for part in path.split("/") if part]
    if not parts:
        return None
    name = parts[0].lower()
    if name in {
        "videos", "clip", "clips", "directory", "search", "p", "downloads",
        "subscriptions", "settings", "wallet", "drops", "friends", "inventory",
        "turbo", "store", "legal", "login", "signup", "jobs", "privacy",
    }:
        return None
    if not re.fullmatch(r"[a-z0-9_]{1,25}", name):
        return None
    return name


def resolve_stream_urls(url: str, max_height: int | None) -> tuple[str, str | None]:
    """Resolve direct HLS/DASH manifest URLs for a YouTube live stream.

    YouTube now answers streamlink's extractor with ``LOGIN_REQUIRED``, while
    yt-dlp keeps working.  YouTube serves video-only and audio-only manifests,
    and merging to stdout is unsupported, so return both and let ffmpeg take
    two inputs.
    """
    timeout = float(CFG["probe"]["timeout_seconds"])

    def first_url(selector: str) -> str | None:
        result = run(ytdlp_base() + ["-g", "-f", selector, url], timeout=timeout)
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise ProbeError(message[-1200:] or f"yt-dlp failed for {selector}")
        for line in (result.stdout or "").splitlines():
            line = line.strip()
            if line.startswith("http"):
                return line
        return None

    if max_height:
        video = first_url(f"bv*[height<={int(max_height)}]/bv*/b")
    else:
        video = first_url("bv*/b")
    if not video:
        raise ProbeError("yt-dlp found no live video manifest", permanent=True)
    audio: str | None
    try:
        audio = first_url("ba")
    except ProbeError as exc:
        log.warning("no separate audio manifest (%s); continuing video-only",
                    str(exc).splitlines()[-1][:200])
        audio = None
    if audio == video:
        audio = None
    return video, audio


PERMANENT_PROBE_MARKERS = (
    "unsupported url",
    "video unavailable",
    "private video",
    "members-only",
    "this video is not available",
    "is not a valid url",
    "requested format is not available",
    "does not exist",
    "this channel does not have a live stream",
    "not currently live",
    "has not started",
    "live event will begin",
    "sign in to confirm you",
    "not a bot",
    "age-restricted",
    "join this channel",
    "copyright",
)


def probe(url: str) -> dict:
    timeout = float(CFG["probe"]["timeout_seconds"])
    try:
        result = run(ytdlp_command(url), timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise ProbeError(f"yt-dlp timed out after {timeout:.0f}s", permanent=False) from exc
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        lower = message.lower()
        not_live = any(
            marker in lower
            for marker in (
                "not currently live",
                "does not have a live stream",
                "no live stream",
                "live event will begin",
                "has not started",
                "is not live",
                "channel is offline",
                "premieres in",
            )
        )
        permanent = any(marker in lower for marker in PERMANENT_PROBE_MARKERS)
        # "not live yet" is a waiting condition, even when the same message also
        # matches a permanent marker such as "does not have a live stream".
        if not_live:
            permanent = False
        hint = ""
        if "sign in to confirm" in lower or "not a bot" in lower:
            hint = " (add cookies in clippyme/data/cookies.txt)"
        raise ProbeError(message[-1500:] + hint, permanent=permanent, not_live=not_live)

    text = (result.stdout or "").strip()
    try:
        info = json.loads(text)
    except ValueError:
        info = None
        for line in reversed(text.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                with contextlib.suppress(ValueError):
                    info = json.loads(line)
                    break
        if info is None:
            raise ProbeError(f"yt-dlp returned no JSON: {text[:400]}", permanent=True)
    if not isinstance(info, dict):
        raise ProbeError("yt-dlp returned unexpected data", permanent=True)
    if info.get("_type") in ("playlist", "multi_video"):
        raise ProbeError(
            "that URL is a playlist/channel, not a single video or live stream", permanent=True
        )
    if not info.get("id"):
        raise ProbeError("yt-dlp found no video id", permanent=True)
    return info


def classify_info(info: dict) -> str:
    live_status = str(info.get("live_status") or "")
    if info.get("is_live") or live_status == "is_live":
        return "live"
    if info.get("is_upcoming") or live_status == "is_upcoming":
        return "upcoming"
    if live_status == "post_live":
        return "post_live"
    return "vod"


def canonical_stream_url(info: dict, original: str) -> str:
    extractor = f"{info.get('extractor_key') or ''} {info.get('extractor') or ''}".lower()
    if "youtube" in extractor and info.get("id"):
        return f"https://www.youtube.com/watch?v={info['id']}"
    webpage = info.get("webpage_url")
    if webpage and is_supported_url(webpage):
        return normalize_url(webpage)
    return original


def upcoming_at(info: dict) -> float:
    stamp = info.get("release_timestamp") or info.get("timestamp")
    interval = float(CFG["probe"]["upcoming_poll_seconds"])
    if isinstance(stamp, (int, float)) and stamp > 0:
        return max(now() + 30, float(stamp) - 30)
    return now() + interval


# --------------------------------------------------------------------------
# YouTube live chat
# --------------------------------------------------------------------------

CHAT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CHAT_CLIENT_VERSION = "2.20241101.00.00"


class ChatUnavailable(AutoClipError):
    """Chat is disabled, ended, or not a live chat."""


def _cookie_header() -> str:
    path = project_path(CFG["clippyme"]["cookies"])
    if not path.exists() or path.stat().st_size == 0:
        return ""
    pairs: list[str] = []
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) >= 7 and "youtube.com" in fields[0]:
                pairs.append(f"{fields[5]}={fields[6]}")
    except OSError:
        return ""
    return "; ".join(pairs)


def _http_text(url: str, *, timeout: float = 30) -> str:
    headers = {
        "User-Agent": CHAT_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    cookie = _cookie_header()
    if cookie:
        headers["Cookie"] = cookie
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "replace")


def _http_json(url: str, payload: dict, *, timeout: float = 30) -> dict:
    headers = {
        "User-Agent": CHAT_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://www.youtube.com",
        "Referer": "https://www.youtube.com/",
    }
    cookie = _cookie_header()
    if cookie:
        headers["Cookie"] = cookie
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _json_after(text: str, marker: str) -> dict | None:
    index = text.find(marker)
    if index < 0:
        return None
    start = text.find("{", index)
    if start < 0:
        return None
    try:
        value, _ = json.JSONDecoder().raw_decode(text[start:])
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _chat_continuation(chat: dict) -> tuple[str, int] | None:
    for entry in chat.get("continuations") or []:
        for key in (
            "invalidationContinuationData",
            "timedContinuationData",
            "reloadContinuationData",
        ):
            value = entry.get(key) if isinstance(entry, dict) else None
            if isinstance(value, dict) and value.get("continuation"):
                timeout = int(value.get("timeoutMs") or 10000)
                return str(value["continuation"]), timeout
    return None


def open_live_chat(video_id: str) -> tuple[str, str, int]:
    html = _http_text(
        f"https://www.youtube.com/live_chat?is_popout=1&v={urllib.parse.quote(video_id)}"
    )
    key_match = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
    if not key_match:
        raise ChatUnavailable("could not find the Innertube API key on the public live-chat page")
    api_key = key_match.group(1)
    initial = _json_after(html, "ytInitialData")
    if not initial:
        raise ChatUnavailable("could not parse the live chat page")
    chat = (initial.get("contents") or {}).get("liveChatRenderer")
    if not isinstance(chat, dict):
        raise ChatUnavailable("this stream has no live chat (or it has ended)")
    continuation = _chat_continuation(chat)
    if not continuation:
        raise ChatUnavailable("live chat has no continuation token")
    token, timeout = continuation
    return api_key, token, timeout


def parse_chat_actions(chat: dict) -> list[dict]:
    messages: list[dict] = []
    for action in chat.get("actions") or []:
        item = (action.get("addChatItemAction") or {}).get("item")
        if not isinstance(item, dict):
            continue
        for kind in (
            "liveChatTextMessageRenderer",
            "liveChatPaidMessageRenderer",
            "liveChatPaidStickerRenderer",
        ):
            renderer = item.get(kind)
            if not isinstance(renderer, dict):
                continue
            text = ""
            for run in (renderer.get("message") or {}).get("runs") or []:
                if isinstance(run, dict):
                    text += str(run.get("text") or "")
            raw_ts = renderer.get("timestampUsec")
            try:
                timestamp = int(raw_ts) / 1_000_000 if raw_ts else now()
            except (TypeError, ValueError):
                timestamp = now()
            messages.append({"ts": timestamp, "kind": kind, "text": text})
    return messages


def iter_live_chat(video_id: str, stop: threading.Event):
    """Yield batches of live chat messages until chat ends or stop is set."""
    backoff = 5
    while not stop.is_set():
        try:
            api_key, token, timeout_ms = open_live_chat(video_id)
            backoff = 5
            log.info("live chat connected for %s", video_id)
        except ChatUnavailable as exc:
            log.warning("live chat unavailable: %s", exc)
            return
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            log.warning("live chat connect failed (%s); retrying in %ss", exc, backoff)
            stop.wait(backoff)
            backoff = min(60, backoff * 2)
            continue

        url = f"https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?key={api_key}"
        body = {
            "context": {"client": {
                "clientName": "WEB",
                "clientVersion": CHAT_CLIENT_VERSION,
                "hl": "en",
                "gl": "US",
            }},
            "continuation": token,
            "isInvalidationTimeoutRequest": False,
        }
        while not stop.is_set():
            try:
                response = _http_json(url, body, timeout=20)
            except urllib.error.HTTPError as exc:
                if exc.code in (403, 404, 410):
                    log.info("live chat ended (HTTP %s)", exc.code)
                    return
                log.warning("live chat poll HTTP %s; reconnecting", exc.code)
                break
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                log.warning("live chat poll failed (%s); reconnecting", exc)
                break

            chat = (response.get("continuationContents") or {}).get(
                "liveChatContinuation"
            )
            if not isinstance(chat, dict):
                log.info("live chat stream ended")
                return
            messages = parse_chat_actions(chat)
            if messages:
                yield messages
            continuation = _chat_continuation(chat)
            if not continuation:
                log.info("live chat has no further continuation")
                return
            token, timeout_ms = continuation
            body["continuation"] = token
            body["isInvalidationTimeoutRequest"] = False
            wait = max(1.0, min(float(CFG["chat"]["poll_seconds"]), timeout_ms / 1000))
            stop.wait(wait)

        if not stop.is_set():
            stop.wait(backoff)
            backoff = min(60, backoff * 2)


TWITCH_IRC_HOST = "irc.chat.twitch.tv"
TWITCH_IRC_PORT = 6697


def _twitch_send(sock: ssl.SSLSocket, text: str) -> None:
    sock.sendall((text + "\r\n").encode("utf-8"))


def parse_twitch_privmsg(line: str) -> dict | None:
    """Parse one Twitch IRC line into the shared chat-message shape."""
    tags: dict[str, str] = {}
    rest = line
    if rest.startswith("@"):
        raw_tags, _, rest = rest.partition(" ")
        for pair in raw_tags[1:].split(";"):
            key, _, value = pair.partition("=")
            tags[key] = value
    parts = rest.split(" ", 3)
    if len(parts) < 4 or parts[1] != "PRIVMSG":
        return None
    text = parts[3]
    if text.startswith(":"):
        text = text[1:]
    if not text.strip():
        return None
    stamp = tags.get("tmi-sent-ts")
    try:
        timestamp = int(stamp) / 1000 if stamp else now()
    except (TypeError, ValueError):
        timestamp = now()
    return {"ts": timestamp, "kind": "twitchPrivmsg", "text": text[:1000]}


def iter_twitch_chat(channel: str, stop: threading.Event):
    """Yield batches of Twitch chat messages using anonymous read-only IRC."""
    backoff = 5
    while not stop.is_set():
        sock = None
        try:
            context = ssl.create_default_context()
            sock = context.wrap_socket(
                socket.create_connection((TWITCH_IRC_HOST, TWITCH_IRC_PORT), timeout=30),
                server_hostname=TWITCH_IRC_HOST,
            )
            sock.settimeout(1.0)
            nick = f"justinfan{int(time.time() * 1000) % 100_000_000}"
            _twitch_send(sock, "CAP REQ :twitch.tv/tags twitch.tv/commands")
            _twitch_send(sock, f"NICK {nick}")
            _twitch_send(sock, f"JOIN #{channel}")
            log.info("Twitch chat connected for #%s", channel)
            backoff = 5
            buffer = b""
            pending: list[dict] = []
            last_batch = time.monotonic()
            last_ping = time.monotonic()
            while not stop.is_set():
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    chunk = b""
                if chunk:
                    buffer += chunk
                    if len(buffer) > 1_000_000:
                        buffer = b""
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        text = line.decode("utf-8", "replace").rstrip("\r")
                        if text.startswith("PING"):
                            _twitch_send(sock, "PONG :tmi.twitch.tv")
                            continue
                        if " RECONNECT" in text:
                            raise ConnectionError("Twitch requested reconnect")
                        message = parse_twitch_privmsg(text)
                        if message:
                            pending.append(message)
                moment = time.monotonic()
                if pending and (moment - last_batch >= 0.5 or len(pending) >= 100):
                    yield pending
                    pending = []
                    last_batch = moment
                if moment - last_ping >= 120:
                    _twitch_send(sock, "PING :tmi.twitch.tv")
                    last_ping = moment
                if not chunk:
                    stop.wait(0.2)
            if pending:
                yield pending
        except ChatUnavailable as exc:
            log.warning("Twitch chat unavailable: %s", exc)
            return
        except (OSError, ConnectionError) as exc:
            if stop.is_set():
                return
            log.warning("Twitch chat connection lost (%s); retrying in %ss", exc, backoff)
            stop.wait(backoff)
            backoff = min(60, backoff * 2)
        finally:
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.close()


def _insert_spike(
    item: dict,
    *,
    bucket_start: float,
    bucket_end: float,
    count: int,
    baseline: float,
) -> bool:
    pre = float(CFG["chat"]["pre_seconds"])
    post = float(CFG["chat"]["post_seconds"])
    max_seconds = float(CFG["chat"]["max_clip_seconds"])
    window_start = bucket_start - pre
    window_end = bucket_end + post
    if window_end - window_start > max_seconds:
        window_end = window_start + max_seconds
    now_value = now()
    spike_id = str(uuid.uuid4())
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO spikes(id, item_id, detected_at, bucket_start, bucket_end,"
                " window_start, window_end, count, baseline, status, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,'pending',?,?)",
                (spike_id, item["id"], now_value, bucket_start, bucket_end,
                 window_start, window_end, count, baseline, now_value, now_value),
            )
    except sqlite3.IntegrityError:
        return False
    log.info(
        "CHAT SPIKE: %d messages in %.0fs (baseline %.1f) → clip %s..%s",
        count, bucket_end - bucket_start, baseline,
        ts_text(window_start), ts_text(window_end),
    )
    update_item(item["id"], note=f"chat spike: {count} messages/{int(bucket_end - bucket_start)}s")
    return True


def chat_monitor_loop(item_id: str, stop: threading.Event) -> None:
    cfg = CFG["chat"]
    if not cfg["enabled"]:
        log.info("live chat monitoring is disabled")
        return
    item = get_item(item_id)
    if not item:
        return
    if is_youtube_url(item["url"]):
        if not item.get("source_id"):
            log.warning("cannot monitor chat: item has no source id")
            return
        chat_stream = iter_live_chat(str(item["source_id"]), stop)
    elif is_twitch_url(item["url"]):
        channel = twitch_channel(item["url"])
        if not channel:
            log.warning("cannot monitor Twitch chat: no channel in %s", item["url"])
            return
        chat_stream = iter_twitch_chat(channel, stop)
    else:
        log.warning(
            "live chat spike detection currently supports YouTube and Twitch"
        )
        return

    window = float(cfg["window_seconds"])
    baseline_windows = int(cfg["baseline_windows"])
    min_history = int(cfg["min_history"])
    min_baseline = float(cfg["min_baseline"])
    multiplier = float(cfg["spike_multiplier"])
    rise_fraction = float(cfg.get("spike_rise_fraction", 0.75))
    min_rise = float(cfg.get("spike_min_rise", 6.0))
    min_messages = int(cfg["spike_min_messages"])
    cooldown = float(cfg["cooldown_seconds"])

    recording_start = float(item_meta(item).get("recording_started_at") or item["created_at"])
    counts: dict[int, int] = {}
    history: list[int] = []
    last_spike = 0.0
    last_finalized = int(recording_start // window) - 1
    with contextlib.suppress(Exception):
        with db() as conn:
            row = conn.execute(
                "SELECT MAX(detected_at) FROM spikes WHERE item_id=?", (item_id,)
            ).fetchone()
            last_spike = float(row[0] or 0)

    def finalize_bucket(index: int) -> None:
        nonlocal last_spike
        count = counts.pop(index, 0)
        bucket_start = index * window
        bucket_end = bucket_start + window
        history.append(count)
        if len(history) > baseline_windows + 2:
            del history[:-baseline_windows - 2]
        if len(history) < min_history:
            log.debug("chat bucket %s: %d msgs (building baseline %d/%d)",
                      ts_text(bucket_start), count, len(history), min_history)
            return
        baseline_slice = history[-baseline_windows - 1:-1] or [0]
        baseline = max(min_baseline, statistics.median(baseline_slice))
        # A fixed multiple is unreachable on high-volume chats (a 100 msg/10s
        # stream would need 300).  Cap the required jump at a proportional
        # surge so the same rule works for 5 msgs/10s and 500 msgs/10s.
        surge = baseline + max(min_rise, baseline * rise_fraction)
        threshold = max(float(min_messages), min(baseline * multiplier, surge))
        log.debug("chat bucket %s: %d msgs (baseline %.1f, threshold %.1f)",
                  ts_text(bucket_start), count, baseline, threshold)
        if count < threshold:
            return
        if now() - last_spike < cooldown:
            log.debug("chat spike suppressed by cooldown (%d msgs)", count)
            return
        if _insert_spike(
            item, bucket_start=bucket_start, bucket_end=bucket_end,
            count=count, baseline=baseline,
        ):
            last_spike = now()

    try:
        for batch in chat_stream:
            current_time = now()
            for message in batch:
                timestamp = min(max(float(message["ts"]), recording_start - 5), current_time + 2)
                if timestamp < recording_start - 5:
                    continue
                index = int(timestamp // window)
                if index <= last_finalized:
                    continue
                counts[index] = counts.get(index, 0) + 1
            latest_complete = int(current_time // window) - 1
            while last_finalized < latest_complete:
                last_finalized += 1
                finalize_bucket(last_finalized)
    except Exception:
        log.exception("chat monitor crashed for %s", item_id[:8])
    log.info("chat monitor stopped for %s", item_id[:8])



def chunk_paths(work: Path) -> list[Path]:
    """Closed capture chunks. Live capture writes MP4 directly.

    MPEG-TS segments from this ffmpeg build are not reliably demuxable, while
    MP4 segments are; writing MP4 also skips a remux pass.
    """
    return sorted(work.glob("chunk_*.mp4"), key=lambda p: p.name)


def chunk_seq(path: Path) -> int:
    match = re.search(r"chunk_(\d+)\.(?:mp4|ts)$", path.name)
    return int(match.group(1)) if match else -1


def next_chunk_seq(work: Path) -> int:
    seqs = [chunk_seq(path) for path in work.glob("chunk_*.ts")]
    seqs += [chunk_seq(path) for path in work.glob("chunk_*.mp4")]
    seqs = [seq for seq in seqs if seq >= 0]
    return (max(seqs) + 1) if seqs else 0


def probe_duration(path: Path) -> float | None:
    try:
        result = run(
            [FFPROBE, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", str(path)],
            timeout=60,
        )
        if result.returncode != 0:
            return None
        return float((result.stdout or "").strip())
    except (ValueError, subprocess.TimeoutExpired, OSError):
        return None


def finalize_chunks(
    item_id: str,
    work: Path,
    *,
    include_last: bool,
    min_seconds: float | None = None,
) -> list[str]:
    """Register newly closed chunk files in the database.

    While ffmpeg is running the highest-numbered file is still open, so only the
    files before it are final.  After the process exits ``include_last=True``
    also closes the last one.
    """
    minimum = float(CFG["capture"]["min_chunk_seconds"] if min_seconds is None else min_seconds)
    files = chunk_paths(work)
    if not files:
        return []
    if not include_last:
        files = files[:-1]
    if not files:
        return []

    with db() as conn:
        existing = {
            int(row["seq"])
            for row in conn.execute("SELECT seq FROM chunks WHERE item_id=?", (item_id,))
        }

    inserted: list[str] = []
    for index, path in enumerate(files):
        seq = chunk_seq(path)
        if seq < 0 or seq in existing:
            continue
        is_last = index == len(files) - 1
        duration = probe_duration(path)
        size = path.stat().st_size if path.exists() else 0

        if duration is None:
            if include_last and is_last:
                log.warning("dropping unreadable final chunk %s", path.name)
                with contextlib.suppress(OSError):
                    path.unlink()
                continue
            chunk_id = str(uuid.uuid4())
            with db() as conn:
                conn.execute(
                    "INSERT INTO chunks(id, item_id, seq, mp4_path, size, status, error,"
                    " created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (chunk_id, item_id, seq, str(path), size, "failed",
                     "unreadable chunk", now(), now()),
                )
            inserted.append(chunk_id)
            log.error("chunk %s is unreadable", path.name)
            continue

        if duration < minimum:
            log.info("discarding short chunk %s (%.1fs)", path.name, duration)
            with contextlib.suppress(OSError):
                path.unlink()
            continue

        chunk_id = str(uuid.uuid4())
        # The capture writes one file per segment; its mtime is the moment the
        # segment closed, so the first frame belongs at mtime - duration.
        try:
            closed_at = path.stat().st_mtime
        except OSError:
            closed_at = now()
        started_at = closed_at - duration
        with db() as conn:
            conn.execute(
                "INSERT INTO chunks(id, item_id, seq, mp4_path, duration, size, status,"
                " started_at, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (chunk_id, item_id, seq, str(path), duration, size, "captured",
                 started_at, now(), now()),
            )
        inserted.append(chunk_id)
        log.info("captured chunk #%d (%.1f min, %.1f MB)",
                 seq, duration / 60, size / (1024 * 1024))
    return inserted


def capture_command(
    url: str,
    work: Path,
    seq: int,
    *,
    segment_seconds: float,
    max_height: int | None,
    no_quality_filter: bool = False,
) -> tuple[list[str] | None, list[str]]:
    """Build the capture pipeline for ``url``.

    Returns ``(feeder_cmd, ffmpeg_cmd)``.  A feeder (streamlink) writes media to
    stdout which ffmpeg reads from ``pipe:0``.  For YouTube there is no feeder:
    ffmpeg reads the two resolved manifests directly.
    """
    output = [
        "-c", "copy", "-f", "segment",
        "-segment_time", str(float(segment_seconds)),
        "-reset_timestamps", "1",
        "-segment_format", "mp4",
        "-segment_start_number", str(seq),
        str(work / "chunk_%05d.mp4"),
    ]

    if is_youtube_url(url):
        video, audio = resolve_stream_urls(
            url, None if no_quality_filter else max_height
        )
        ffmpeg_cmd = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostdin",
                      "-i", video]
        if audio:
            ffmpeg_cmd += ["-i", audio]
        ffmpeg_cmd += ["-map", "0:v:0"]
        ffmpeg_cmd += ["-map", "1:a:0"] if audio else ["-map", "0:a:0?"]
        ffmpeg_cmd += output
        return None, ffmpeg_cmd

    streamlink_cmd = [STREAMLINK]
    if not no_quality_filter and max_height:
        # Twitch labels its 60 fps variant "720p60"; comparing against "720p"
        # treats it as higher and silently drops to 480p.  Excluding ">720p60"
        # keeps both 720p60 and plain 720p while dropping every 1080p variant.
        streamlink_cmd += [
            "--stream-sorting-excludes",
            f">{int(max_height)}p60",
        ]
    streamlink_cmd += ["--hls-live-edge", str(CFG["capture"]["live_edge"])]
    streamlink_cmd += [url, "best", "-O"]

    ffmpeg_cmd = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostdin",
                  "-i", "pipe:0"] + output
    return streamlink_cmd, ffmpeg_cmd


def run_capture_session(
    item: dict,
    work: Path,
    stop: threading.Event,
    *,
    segment_seconds: float | None = None,
    max_height: int | None = None,
    deadline: float | None = None,
    no_quality_filter: bool = False,
    check_item: bool = True,
    min_chunk_seconds: float | None = None,
) -> tuple[int | None, str]:
    """Record one continuous segment of a live stream.

    Returns ``(ffmpeg_exit_code, reason)`` where reason is one of ``ended``,
    ``stop``, ``cancelled``, ``disk``, ``stall``, ``deadline`` or ``error``.
    """
    work.mkdir(parents=True, exist_ok=True)
    seq = next_chunk_seq(work)
    try:
        feeder_cmd, ffmpeg_cmd = capture_command(
            item["url"], work, seq,
            segment_seconds=float(segment_seconds or CFG["capture"]["segment_seconds"]),
            max_height=CFG["capture"]["max_height"] if max_height is None else max_height,
            no_quality_filter=no_quality_filter,
        )
    except ProbeError as exc:
        log.error("could not resolve live stream: %s", str(exc).splitlines()[-1][:400])
        return None, "error"
    sl_log_path = work / "streamlink.log"
    ff_log_path = work / "ffmpeg.log"
    sl_log = open(sl_log_path, "ab")
    ff_log = open(ff_log_path, "ab")
    sl = ff = None
    reason = "ended"
    try:
        sl_log.write(f"\n--- {ts_text(now())} session start ---\n".encode())
        sl_log.flush()
        try:
            stdin = subprocess.DEVNULL
            if feeder_cmd:
                sl = subprocess.Popen(
                    feeder_cmd, stdout=subprocess.PIPE, stderr=sl_log,
                    start_new_session=True,
                )
                stdin = sl.stdout
            ff = subprocess.Popen(
                ffmpeg_cmd, stdin=stdin, stderr=ff_log, start_new_session=True,
            )
            if sl is not None and sl.stdout:
                sl.stdout.close()
        except OSError as exc:
            log.error("could not start capture processes: %s", exc)
            terminate_group(sl)
            return None, "error"

        last_progress = now()
        last_signature: tuple[int, float] | None = None
        last_disk_check = 0.0
        last_prune = 0.0
        while True:
            if stop.is_set():
                reason = "stop"
                break
            if check_item:
                current = get_item(item["id"])
                if not current or current["status"] == "cancelled":
                    reason = "cancelled"
                    break
            if deadline is not None and now() >= deadline:
                reason = "deadline"
                break
            if ff.poll() is not None:
                reason = "ended"
                break

            if check_item:
                finalize_chunks(
                    item["id"], work, include_last=False,
                    min_seconds=min_chunk_seconds,
                )
                if now() - last_prune > 60:
                    last_prune = now()
                    prune_source_chunks(item["id"])

            files = chunk_paths(work)
            if files:
                newest = files[-1]
                try:
                    stat = newest.stat()
                    signature = (stat.st_size, stat.st_mtime)
                except OSError:
                    signature = None
                if signature != last_signature:
                    last_signature = signature
                    last_progress = now()
                elif now() - last_progress > float(CFG["capture"]["stall_timeout_seconds"]):
                    log.warning("capture stalled for %.0fs; restarting stream",
                                now() - last_progress)
                    reason = "stall"
                    break
            elif now() - last_progress > float(CFG["capture"]["stall_timeout_seconds"]):
                log.warning("no chunk data after %.0fs; restarting stream",
                            now() - last_progress)
                reason = "stall"
                break

            if now() - last_disk_check > 15:
                last_disk_check = now()
                if disk_free_gb(work) < float(CFG["capture"]["disk_guard_free_gb"]):
                    log.error("free disk below %.1f GB; pausing capture",
                              float(CFG["capture"]["disk_guard_free_gb"]))
                    reason = "disk"
                    break
            stop.wait(2)
    finally:
        terminate_group(ff)
        terminate_group(sl)
        if ff is not None:
            with contextlib.suppress(subprocess.TimeoutExpired):
                ff.wait(timeout=5)
        if sl is not None:
            with contextlib.suppress(subprocess.TimeoutExpired):
                sl.wait(timeout=5)
        try:
            if check_item:
                finalize_chunks(
                    item["id"], work, include_last=True,
                    min_seconds=min_chunk_seconds,
                )
        finally:
            sl_log.close()
            ff_log.close()

    rc = ff.returncode if ff is not None else None
    if reason not in ("ended", "deadline") or (rc not in (None, 0)):
        tail = read_tail(ff_log_path, 600)
        if tail:
            log.warning("ffmpeg tail: %s", tail.replace("\n", " | "))
        sl_tail = read_tail(sl_log_path, 400)
        if reason in ("error", "stall") and sl_tail:
            log.warning("streamlink tail: %s", sl_tail.replace("\n", " | "))
    return rc, reason


def read_tail(path: Path, limit: int = 800) -> str:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - limit))
            return handle.read().decode("utf-8", "replace").strip()
    except OSError:
        return ""


def probe_after_capture(item: dict, stop: threading.Event, attempts: int = 3) -> dict | None:
    for attempt in range(attempts):
        if stop.is_set():
            return None
        try:
            return probe(item["url"])
        except ProbeError as exc:
            log.warning("post-capture probe failed (%d/%d): %s",
                        attempt + 1, attempts, str(exc).splitlines()[-1][:300])
            if exc.not_live or exc.permanent:
                return None
            if attempt + 1 < attempts:
                stop.wait(20)
    return None


def capture_item(item_id: str, stop: threading.Event) -> None:
    """Record a live item until the stream ends or the item is cancelled."""
    item = get_item(item_id)
    if not item or item["status"] in TERMINAL_ITEMS:
        return
    work = CAPTURE_DIR / item_id
    work.mkdir(parents=True, exist_ok=True)
    update_item(item_id, status="recording", work_dir=str(work), error=None)
    merge_item_meta(item_id, recording_started_at=now())

    # On daemon restart there may be leftover chunks from a killed process.
    finalize_chunks(item_id, work, include_last=True)

    # Comment-spike detection runs next to capture. It reads timestamps from
    # the chat itself, so this stays aligned even if capture restarts.
    chat_stop = threading.Event()
    chat_thread = threading.Thread(
        target=chat_monitor_loop, args=(item_id, chat_stop),
        name=f"chat-{item_id[:8]}", daemon=True,
    )
    chat_thread.start()

    try:
        prelive = float(CFG["capture"]["prelive_skip_seconds"])
        if prelive > 0:
            log.info("skipping first %.0fs of the stream", prelive)
            deadline = now() + prelive
            while not stop.is_set() and now() < deadline:
                stop.wait(min(30, deadline - now()))

        failures = 0
        no_filter = False
        while not stop.is_set():
            item = get_item(item_id)
            if not item or item["status"] == "cancelled":
                return

            free_gb = disk_free_gb(work)
            guard = float(CFG["capture"]["disk_guard_free_gb"])
            if free_gb < guard:
                update_item(item_id, note=f"waiting for disk space ({free_gb:.1f} GB free)")
                log.error("only %.1f GB free (guard %.1f GB); waiting", free_gb, guard)
                while not stop.is_set() and disk_free_gb(work) < guard:
                    stop.wait(30)
                continue

            started = now()
            rc, reason = run_capture_session(
                item, work, stop, no_quality_filter=no_filter
            )
            short_session = now() - started < 60

            if reason == "stop":
                log.info("daemon stopping; capture left resumable")
                return
            if reason == "cancelled":
                return
            if reason == "error" and not no_filter and CFG["capture"]["max_height"]:
                no_filter = True
                log.warning("retrying capture without the resolution cap")
                stop.wait(5)
                continue

            info = probe_after_capture(item, stop)
            still_live = False
            if info is not None:
                kind = classify_info(info)
                same_stream = (not item.get("source_id")) or info.get("id") == item["source_id"]
                if same_stream and kind == "live":
                    still_live = True
                elif item.get("source_id") and not same_stream:
                    log.info("a different stream is live now; ending capture")
            if still_live:
                if short_session:
                    failures += 1
                else:
                    failures = 0
                if failures >= int(CFG["capture"]["max_session_failures"]):
                    update_item(
                        item_id, status="finalizing", finished_at=now(),
                        error=f"capture failed {failures} times in a row",
                    )
                    return
                backoff = min(300, float(CFG["capture"]["retry_backoff_seconds"]) * max(1, failures))
                log.warning("stream ended prematurely (%s); retrying in %.0fs", reason, backoff)
                stop.wait(backoff)
                continue

            update_item(item_id, status="finalizing", finished_at=now(), note="stream ended")
            log.info("capture finished for %s", item_id[:8])
            return
    finally:
        chat_stop.set()
        chat_thread.join(timeout=30)
        prune_source_chunks(item_id)
        log.info("capture stopped by shutdown for %s", item_id[:8])

# --------------------------------------------------------------------------
# Recorder: probe links, wait for streams, capture them
# --------------------------------------------------------------------------

def next_recorder_item(exclude_ids: set[str] | None = None) -> dict | None:
    exclude_ids = exclude_ids or set()
    exclusion = ""
    params: list = [now()]
    if exclude_ids:
        exclusion = " AND id NOT IN (" + ",".join("?" for _ in exclude_ids) + ")"
        params.extend(sorted(exclude_ids))
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM items WHERE status IN ('queued','waiting','probing','recording')"
            " AND next_action_at <= ?" + exclusion +
            " ORDER BY CASE status WHEN 'recording' THEN 0 WHEN 'probing' THEN 1 ELSE 2 END,"
            " next_action_at, created_at LIMIT 1",
            params,
        ).fetchone()
    return row_dict(row)


def handle_probe(item: dict, stop: threading.Event) -> None:
    item_id = item["id"]
    update_item(item_id, status="probing", updated_at=now())
    try:
        info = probe(item["url"])
    except ProbeError as exc:
        message = str(exc).strip().splitlines()[-1][:1500] if str(exc).strip() else "probe failed"
        attempts = int(item["attempts"]) + 1
        if exc.permanent:
            update_item(item_id, status="failed", error=message, finished_at=now())
            log.error("cannot process %s: %s", item["url"], message)
            return
        if exc.not_live:
            next_at = now() + float(CFG["probe"]["upcoming_poll_seconds"])
            update_item(
                item_id, status="waiting", kind="upcoming", attempts=attempts,
                next_action_at=next_at, error=message,
            )
            log.info("stream not live yet; checking again in %.0f min",
                     (next_at - now()) / 60)
            return
        if attempts > int(CFG["probe"]["max_attempts"]):
            update_item(item_id, status="failed", error=message, finished_at=now())
            log.error("giving up on %s after %d probes: %s", item["url"], attempts, message)
            return
        backoff = min(1800, float(CFG["probe"]["retry_seconds"]) * attempts)
        update_item(
            item_id, status="waiting", attempts=attempts,
            next_action_at=now() + backoff, error=message,
        )
        log.warning("probe failed for %s (%s); retrying in %.0fs",
                    item["url"], message, backoff)
        return

    kind = classify_info(info)
    title = info.get("title") or item.get("title") or item["url"]
    source_id = info.get("id")
    source_url = canonical_stream_url(info, item["url"])
    duration = info.get("duration")
    common = dict(
        kind=kind, title=title, source_id=source_id, url=source_url,
        duration=duration, attempts=0, error=None, next_action_at=0,
    )

    if kind == "live":
        update_item(item_id, status="recording", **common)
        log.info("LIVE: %s — recording with %s", title, source_url)
        capture_item(item_id, stop)
        return

    if kind == "upcoming":
        next_at = upcoming_at(info)
        update_item(item_id, status="waiting", next_action_at=next_at, **common)
        log.info("UPCOMING: %s — waiting until %s", title, ts_text(next_at))
        return

    if kind == "post_live":
        next_at = now() + max(600, float(CFG["probe"]["upcoming_poll_seconds"]))
        update_item(item_id, status="waiting", next_action_at=next_at, **common)
        log.info("POST-LIVE: %s — YouTube is still processing the archive", title)
        return

    update_item(item_id, status="ready", **common)
    log.info("VOD: %s (%.0fs) — queued for processing", title, duration or 0)


def recorder_loop(stop: threading.Event) -> None:
    log.info("recorder started")
    captures: dict[str, threading.Thread] = {}
    while not stop.is_set():
        try:
            captures = {item_id: thread for item_id, thread in captures.items()
                        if thread.is_alive()}
            max_streams = max(1, int(CFG["capture"].get("max_concurrent_streams", 8)))
            if len(captures) >= max_streams:
                stop.wait(2)
                continue
            item = next_recorder_item(set(captures))
            if not item:
                stop.wait(5)
                continue
            if item["status"] == "recording":
                thread = threading.Thread(
                    name=f"capture-{item['id'][:8]}",
                    target=capture_item,
                    args=(item["id"], stop),
                    daemon=True,
                )
                captures[item["id"]] = thread
                thread.start()
            elif item["status"] == "probing":
                update_item(item["id"], status="queued", next_action_at=0)
            else:
                handle_probe(item, stop)
        except Exception:
            log.exception("recorder iteration failed")
            stop.wait(10)
    deadline = time.monotonic() + 25
    for thread in captures.values():
        thread.join(max(0, deadline - time.monotonic()))
    log.info("recorder stopped")


# --------------------------------------------------------------------------
# Comment-spike clipping
# --------------------------------------------------------------------------

CAPTURE_LOCK = threading.Lock()
SPIKE_ACTIVE = ("cutting", "submitting", "processing")


def get_spike(spike_id: str) -> dict | None:
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM spikes WHERE id=?", (spike_id,)).fetchone())


def update_spike(spike_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    assignments = ", ".join(f"{key}=?" for key in fields)
    with db() as conn:
        conn.execute(f"UPDATE spikes SET {assignments} WHERE id=?",
                     (*fields.values(), spike_id))


def chunk_ranges(item_id: str) -> list[dict]:
    with db() as conn:
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM chunks WHERE item_id=? AND mp4_path IS NOT NULL"
            " ORDER BY seq", (item_id,)).fetchall()]
    result = []
    for row in rows:
        path = Path(row["mp4_path"])
        if not path.exists() or path.stat().st_size == 0:
            continue
        duration = float(row.get("duration") or 0)
        if duration <= 0:
            continue
        start = row.get("started_at")
        if start is None:
            start = float(row["created_at"]) - duration
        start = float(start)
        result.append({
            "id": row["id"], "path": path, "start": start,
            "end": start + duration, "duration": duration,
        })
    return result


def spike_covered(item_id: str, start: float, end: float) -> bool:
    ranges = chunk_ranges(item_id)
    if not ranges:
        return False
    if start < ranges[0]["start"] + 0.1:
        start = ranges[0]["start"] + 0.1
    if end > ranges[-1]["end"] + 0.5:
        return False
    return any(r["start"] < end - 0.2 and r["end"] > start + 0.2 for r in ranges)


def cut_spike_window(item: dict, spike: dict) -> Path | None:
    """Cut [window_start, window_end] from closed capture chunks.

    The window is re-encoded so its first frame is exactly the requested one;
    stream-copy cuts would snap to the previous keyframe.
    """
    start = float(spike["window_start"])
    end = float(spike["window_end"])
    if end - start < 5:
        end = start + 5
    with CAPTURE_LOCK:
        ranges = chunk_ranges(item["id"])
        if not ranges:
            return None
        start = max(start, ranges[0]["start"] + 0.05)
        if end > ranges[-1]["end"] + 0.5:
            return None
        end = min(end, ranges[-1]["end"] - 0.02)
        if end - start < 3:
            return None

        work = CAPTURE_DIR / item["id"] / "cuts"
        work.mkdir(parents=True, exist_ok=True)
        parts: list[Path] = []
        covered_until = start
        for part_index, chunk in enumerate(ranges):
            overlap_start = max(start, chunk["start"], covered_until)
            overlap_end = min(end, chunk["end"])
            if overlap_end - overlap_start < 0.25:
                continue
            offset = overlap_start - chunk["start"]
            length = overlap_end - overlap_start
            part = work / f"{spike['id']}_{part_index:02d}.mp4"
            result = run(
                [FFMPEG, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
                 "-ss", f"{offset:.3f}", "-i", str(chunk["path"]),
                 "-t", f"{length:.3f}",
                 "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                 "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "160k",
                 "-movflags", "+faststart", str(part)],
                timeout=600,
            )
            if result.returncode != 0 or not part.exists() or part.stat().st_size == 0:
                for created in parts:
                    with contextlib.suppress(OSError):
                        created.unlink()
                with contextlib.suppress(OSError):
                    part.unlink()
                raise AutoClipError(
                    f"spike cut failed: {(result.stderr or '').strip()[-800:]}"
                )
            parts.append(part)
            covered_until = max(covered_until, overlap_end)

        if len(parts) == 1:
            destination = work / f"{spike['id']}.mp4"
            os.replace(parts[0], destination)
        else:
            list_path = work / f"{spike['id']}.txt"
            list_path.write_text(
                "".join(f"file '{part.as_posix()}'\n" for part in parts),
                encoding="utf-8",
            )
            destination = work / f"{spike['id']}.mp4"
            result = run(
                [FFMPEG, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
                 "-f", "concat", "-safe", "0", "-i", str(list_path),
                 "-c", "copy", "-movflags", "+faststart", str(destination)],
                timeout=600,
            )
            if result.returncode != 0 or not destination.exists():
                result = run(
                    [FFMPEG, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
                     "-f", "concat", "-safe", "0", "-i", str(list_path),
                     "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                     "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "160k",
                     "-movflags", "+faststart", str(destination)],
                    timeout=600,
                )
            for part in parts:
                with contextlib.suppress(OSError):
                    part.unlink()
            with contextlib.suppress(OSError):
                list_path.unlink()
            if result.returncode != 0 or not destination.exists():
                raise AutoClipError(
                    f"spike concat failed: {(result.stderr or '').strip()[-800:]}"
                )

    duration = probe_duration(destination)
    if not duration or duration < 3:
        with contextlib.suppress(OSError):
            destination.unlink()
        raise AutoClipError("spike cut produced no usable video")
    update_spike(
        spike["id"], status="cut", cut_path=str(destination), error=None,
    )
    log.info("cut spike %s (%.1fs, %.1f MB)", spike["id"][:8], duration,
             destination.stat().st_size / (1024 * 1024))
    return destination


def prune_source_chunks(item_id: str) -> None:
    keep = float(CFG["capture"].get("keep_source_seconds") or 1800)
    cutoff = now() - keep
    with CAPTURE_LOCK:
        with db() as conn:
            active = [dict(row) for row in conn.execute(
                "SELECT window_start, window_end FROM spikes WHERE item_id=?"
                " AND status IN ('pending','cutting','submitting','processing')",
                (item_id,),
            ).fetchall()]
            chunks = [dict(row) for row in conn.execute(
                "SELECT * FROM chunks WHERE item_id=?", (item_id,),
            ).fetchall()]
        for chunk in chunks:
            duration = float(chunk.get("duration") or 0)
            start = chunk.get("started_at")
            if start is None:
                start = float(chunk["created_at"]) - duration
            start = float(start)
            end = start + duration
            if end > cutoff:
                continue
            if any(float(s["window_start"]) < end and float(s["window_end"]) > start
                   for s in active):
                continue
            path = chunk.get("mp4_path")
            if path:
                with contextlib.suppress(OSError):
                    Path(path).unlink()
            with db() as conn:
                conn.execute("DELETE FROM chunks WHERE id=?", (chunk["id"],))


def next_ready_spike() -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT s.*, i.title AS item_title, i.url AS item_url, i.status AS item_status"
            " FROM spikes s JOIN items i ON i.id=s.item_id"
            " WHERE s.status IN ('pending','cut') AND s.next_attempt_at <= ?"
            " AND i.status IN ('recording','finalizing')"
            " ORDER BY s.detected_at LIMIT 1", (now(),),
        ).fetchone()
    return row_dict(row)


def start_spike(spike: dict) -> None:
    item = get_item(spike["item_id"])
    if not item or item["status"] == "cancelled":
        update_spike(spike["id"], status="failed", error="item cancelled")
        return
    try:
        if spike["status"] == "pending":
            cut_path = cut_spike_window(item, spike)
            if cut_path is None:
                # The capture chunks that cover the window are not closed yet.
                update_spike(spike["id"], next_attempt_at=now() + 15)
                return
            spike = get_spike(spike["id"]) or spike
        cut_path = Path(spike.get("cut_path") or "")
        if not (cut_path.exists() and cut_path.stat().st_size > 0):
            raise AutoClipError(f"spike cut file is missing: {cut_path}")
        duration = probe_duration(cut_path) or 0
        if duration <= 0:
            raise AutoClipError("spike cut has no readable duration")
        update_spike(spike["id"], status="submitting", error=None)
        instructions = item.get("instructions") or CFG["analysis"]["instructions"]
        job_id = submit_file_job(
            cut_path, instructions, clip_start=0.0, clip_end=duration
        )
        attempts = int(spike["attempts"]) + 1
        update_spike(
            spike["id"], status="processing", job_id=job_id, attempts=attempts,
            next_attempt_at=0, error=None,
        )
        log.info("submitted spike %s (%.1fs) to ClippyMe job %s",
                 spike["id"][:8], duration, job_id[:8])
    except Exception as exc:
        attempts = int(spike.get("attempts") or 0) + 1
        message = str(exc)[:1500]
        if attempts >= 3:
            update_spike(spike["id"], status="failed", attempts=attempts, error=message)
            log.error("spike %s failed for good: %s", spike["id"][:8], message)
        else:
            update_spike(
                spike["id"], status="cut" if spike.get("cut_path") else "pending",
                attempts=attempts, next_attempt_at=now() + 90 * attempts, error=message,
            )
            log.warning("spike %s submission failed, will retry: %s",
                        spike["id"][:8], message)


def finish_spike(spike: dict, status: dict) -> None:
    state = str(status.get("status") or "failed")
    item = get_item(spike["item_id"])
    if item is None or item["status"] == "cancelled":
        update_spike(spike["id"], status="failed", error="item cancelled")
        return
    if state in ("completed", "stopped"):
        count = collect_clips(
            item, spike["job_id"], status.get("result") or {},
            spike_id=spike["id"],
        )
        update_spike(
            spike["id"], status="done", error=None,
            next_attempt_at=0,
        )
        log.info("spike %s finished with %d clip(s)", spike["id"][:8], count)
        if count and CFG["retention"].get("delete_chunks"):
            cut_path = spike.get("cut_path")
            if cut_path:
                with contextlib.suppress(OSError):
                    Path(cut_path).unlink()
        if count and CFG["retention"].get("delete_job_output"):
            # The library copy is the durable artifact; ClippyMe's per-job
            # working directory is only needed while rendering/composing.
            shutil.rmtree(OUTPUT_DIR / spike["job_id"], ignore_errors=True)
        if not count:
            update_spike(spike["id"], error="ClippyMe produced no clip")
    elif state == "cancelled":
        update_spike(spike["id"], status="failed", error="cancelled in ClippyMe")
        log.warning("spike job %s was cancelled in ClippyMe", spike["job_id"][:8])
    else:
        attempts = int(spike.get("attempts") or 0) + 1
        message = job_error_text(status)
        if attempts >= 3:
            update_spike(spike["id"], status="failed", attempts=attempts, error=message)
            log.error("spike job %s failed: %s", spike["job_id"][:8], message[-300:])
        else:
            update_spike(
                spike["id"], status="cut" if spike.get("cut_path") else "pending",
                attempts=attempts, next_attempt_at=now() + 120 * attempts,
                job_id=None, error=message,
            )
            log.warning("spike job %s failed; will retry (%d)", spike["job_id"][:8], attempts)
    prune_source_chunks(spike["item_id"])


def find_spike_for_job(job_id: str) -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM spikes WHERE job_id=?", (job_id,)
        ).fetchone()
    return row_dict(row)


def find_chunk_for_job(job_id: str) -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM chunks WHERE job_id=?", (job_id,)
        ).fetchone()
    return row_dict(row)




def find_active_work() -> tuple[str, dict] | None:
    with db() as conn:
        spike = conn.execute(
            "SELECT * FROM spikes WHERE status IN ('submitting','processing')"
            " ORDER BY created_at LIMIT 1"
        ).fetchone()
        if spike:
            return "spike", dict(spike)
        # Legacy in-flight chunk jobs (pre-spike pipeline) still get drained.
        chunk = conn.execute(
            "SELECT * FROM chunks WHERE status IN ('submitting','submitted','processing')"
            " ORDER BY created_at LIMIT 1"
        ).fetchone()
        if chunk:
            return "chunk", dict(chunk)
        vod = conn.execute(
            "SELECT * FROM items WHERE status='processing' AND job_id IS NOT NULL"
            " ORDER BY created_at LIMIT 1"
        ).fetchone()
        if vod:
            return "vod", dict(vod)
    return None


def next_startable_chunk() -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT c.* FROM chunks c JOIN items i ON i.id=c.item_id"
            " WHERE c.status IN ('captured','remuxed') AND c.next_attempt_at <= ?"
            " AND i.status IN ('recording','finalizing')"
            " ORDER BY c.created_at LIMIT 1",
            (now(),),
        ).fetchone()
    return row_dict(row)


def next_startable_chunk() -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT c.* FROM chunks c JOIN items i ON i.id=c.item_id"
            " WHERE c.status IN ('captured','remuxed') AND c.next_attempt_at <= ?"
            " AND i.status IN ('recording','finalizing')"
            " ORDER BY c.created_at LIMIT 1",
            (now(),),
        ).fetchone()
    return row_dict(row)


def next_vod_ready() -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM items WHERE kind='vod' AND status='ready'"
            " AND next_action_at <= ? ORDER BY created_at LIMIT 1",
            (now(),),
        ).fetchone()
    return row_dict(row)


def remux_chunk(chunk: dict) -> Path:
    ts_path = Path(chunk["ts_path"])
    if not ts_path.exists():
        raise AutoClipError(f"source chunk is gone: {ts_path}")
    mp4_path = ts_path.with_suffix(".mp4")
    update_chunk(chunk["id"], status="remuxing", error=None)
    result = run(
        [FFMPEG, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
         "-i", str(ts_path), "-c", "copy", "-movflags", "+faststart", str(mp4_path)],
        timeout=900,
    )
    if result.returncode != 0 or not mp4_path.exists() or mp4_path.stat().st_size == 0:
        with contextlib.suppress(OSError):
            mp4_path.unlink()
        message = (result.stderr or "ffmpeg produced no output").strip()[-1200:]
        update_chunk(chunk["id"], status="failed", error=f"remux failed: {message}")
        raise AutoClipError(f"remux failed for {ts_path.name}")
    update_chunk(chunk["id"], status="remuxed", mp4_path=str(mp4_path), error=None)
    log.info("remuxed %s (%.1f MB)", mp4_path.name, mp4_path.stat().st_size / (1024 * 1024))
    return mp4_path


def start_chunk(chunk: dict) -> None:
    item = get_item(chunk["item_id"])
    if not item or item["status"] == "cancelled":
        update_chunk(chunk["id"], status="skipped", error="item cancelled")
        return
    try:
        if chunk["status"] == "captured":
            mp4_path = Path(chunk.get("mp4_path") or "")
            if not (mp4_path.exists() and mp4_path.stat().st_size > 0):
                # Legacy TS chunk from before live capture wrote MP4 directly.
                if chunk.get("ts_path"):
                    remux_chunk(chunk)
                    chunk = get_chunk(chunk["id"]) or chunk
                else:
                    raise AutoClipError("capture chunk file is missing")
        if chunk["status"] not in ("captured", "remuxed"):
            return
        mp4_path = Path(chunk["mp4_path"])
        if not mp4_path.exists():
            raise AutoClipError(f"chunk file is missing: {mp4_path}")
        update_chunk(chunk["id"], status="submitting", error=None)
        instructions = item.get("instructions") or CFG["analysis"]["instructions"]
        job_id = submit_file_job(mp4_path, instructions)
        attempts = int(chunk["attempts"]) + 1
        update_chunk(
            chunk["id"], status="processing", job_id=job_id, attempts=attempts,
            error=None,
        )
        log.info("submitted chunk %s to ClippyMe job %s", mp4_path.name, job_id[:8])
    except Exception as exc:
        attempts = int(chunk.get("attempts") or 0) + 1
        message = str(exc)[:1500]
        if attempts >= 3:
            update_chunk(chunk["id"], status="failed", attempts=attempts, error=message)
            log.error("chunk %s failed for good: %s", Path(chunk.get("ts_path") or "?").name, message)
        else:
            update_chunk(
                chunk["id"], status="remuxed", attempts=attempts,
                next_attempt_at=now() + 60 * attempts, error=message,
            )
            log.warning("chunk submission failed, will retry: %s", message)


def start_vod(item: dict) -> None:
    try:
        instructions = item.get("instructions") or CFG["analysis"]["instructions"]
        job_id = submit_url_job(item["url"], instructions)
        attempts = int(item["attempts"]) + 1
        update_item(
            item["id"], status="processing", job_id=job_id, attempts=attempts,
            next_action_at=0, error=None,
        )
        log.info("submitted VOD %s to ClippyMe job %s", item["url"], job_id[:8])
    except Exception as exc:
        attempts = int(item["attempts"]) + 1
        message = str(exc)[:1500]
        if attempts >= 5:
            update_item(item["id"], status="failed", attempts=attempts,
                        error=message, finished_at=now())
            log.error("VOD submission failed for good: %s", message)
        else:
            update_item(
                item["id"], status="ready", attempts=attempts,
                next_action_at=now() + 120 * attempts, error=message,
            )
            log.warning("VOD submission failed, will retry: %s", message)


def job_error_text(status: dict) -> str:
    logs = status.get("logs") or []
    text = " | ".join(str(line) for line in logs[-8:])
    return (text or "job failed")[-1800:]


def finish_chunk(chunk: dict, status: dict) -> None:
    state = str(status.get("status") or "failed")
    if state in ("completed", "stopped"):
        item = get_item(chunk["item_id"])
        count = 0
        if item:
            count = collect_clips(item, chunk["job_id"], status.get("result") or {})
        update_chunk(chunk["id"], status="done", error=None,
                     meta=json.dumps({"job_status": state}, ensure_ascii=False))
        log.info("chunk job %s finished with %d clip(s)", chunk["job_id"][:8], count)
        if CFG["retention"]["delete_chunks"]:
            for key in ("mp4_path", "ts_path"):
                path = chunk.get(key)
                if path:
                    with contextlib.suppress(OSError):
                        Path(path).unlink()
    elif state == "cancelled":
        update_chunk(chunk["id"], status="failed", error="cancelled in ClippyMe")
        log.warning("chunk job %s was cancelled in ClippyMe", chunk["job_id"][:8])
    else:
        update_chunk(chunk["id"], status="failed", error=job_error_text(status))
        log.error("chunk job %s failed: %s", chunk["job_id"][:8], job_error_text(status)[-300:])


def finish_vod(item: dict, status: dict) -> None:
    state = str(status.get("status") or "failed")
    if state in ("completed", "stopped"):
        count = collect_clips(item, item["job_id"], status.get("result") or {})
        note = f"{count} clip(s) selected" if count else "no clips selected"
        update_item(
            item["id"], status="completed", finished_at=now(), note=note,
            error=None if count else "analysis selected no clips",
        )
        write_item_manifest(item["id"])
        log.info("VOD %s completed with %d clip(s)", item["id"][:8], count)
    elif state == "cancelled":
        update_item(item["id"], status="failed", finished_at=now(),
                    error="cancelled in ClippyMe")
    else:
        update_item(item["id"], status="failed", finished_at=now(),
                    error=job_error_text(status))
        log.error("VOD job %s failed", item["id"][:8])


def poll_work(kind: str, row: dict) -> None:
    job_id = row.get("job_id")
    if not job_id:
        if kind == "spike":
            update_spike(row["id"], status="cut" if row.get("cut_path") else "pending",
                         next_attempt_at=now() + 60)
        elif kind == "chunk":
            update_chunk(row["id"], status="remuxed", attempts=int(row["attempts"]) + 1)
        else:
            update_item(row["id"], status="ready", next_action_at=now() + 120)
        return
    try:
        status = job_status(job_id)
    except ApiError as exc:
        if exc.status == 404:
            if now() - float(row["updated_at"]) < 300:
                return
            if kind == "spike":
                if int(row["attempts"]) >= 2:
                    update_spike(row["id"], status="failed",
                                 error="ClippyMe forgot the job (backend restart)")
                else:
                    log.warning("spike job %s vanished; resubmitting", job_id[:8])
                    update_spike(row["id"], status="cut", job_id=None,
                                 next_attempt_at=now() + 30)
            elif kind == "chunk":
                if int(row["attempts"]) >= 2:
                    update_chunk(row["id"], status="failed",
                                 error="ClippyMe forgot the job (backend restart)")
                else:
                    fallback = row.get("mp4_path") or row.get("ts_path")
                    state = "remuxed" if row.get("mp4_path") else "captured"
                    log.warning("job %s vanished; resubmitting %s", job_id[:8], Path(fallback or "?").name)
                    update_chunk(row["id"], status=state, job_id=None)
            else:
                if int(row["attempts"]) >= 2:
                    update_item(row["id"], status="failed",
                                error="ClippyMe forgot the job (backend restart)")
                else:
                    update_item(row["id"], status="ready", job_id=None,
                                next_action_at=now() + 30)
            return
        log.warning("status poll failed for %s: %s", job_id[:8], exc)
        return

    state = str(status.get("status") or "").lower()
    if state in ("queued", "running", "processing", "paused", "pending"):
        return
    if kind == "spike":
        finish_spike(row, status)
    elif kind == "chunk":
        finish_chunk(row, status)
    else:
        finish_vod(row, status)
    reconcile_items()


def reconcile_items() -> None:
    with db() as conn:
        items = [dict(row) for row in conn.execute(
            "SELECT * FROM items WHERE status='finalizing'"
        ).fetchall()]
    for item in items:
        # A spike whose post-roll was never recorded cannot be cut anymore.
        ranges = chunk_ranges(item["id"])
        last_end = ranges[-1]["end"] if ranges else 0.0
        with db() as conn:
            pending_spikes = [dict(row) for row in conn.execute(
                "SELECT * FROM spikes WHERE item_id=? AND status IN ('pending','cut')",
                (item["id"],),
            ).fetchall()]
        for spike in pending_spikes:
            if last_end and float(spike["window_end"]) > last_end + 0.5:
                update_spike(
                    spike["id"], status="failed",
                    error="clip window was not captured (stream ended)",
                )
                log.warning("spike %s dropped: window ends after the last chunk",
                            spike["id"][:8])

        with db() as conn:
            counts = conn.execute(
                "SELECT"
                " SUM(CASE WHEN status IN ('submitting','submitted','processing','remuxing')"
                " THEN 1 ELSE 0 END),"
                " SUM(CASE WHEN status='done' THEN 1 ELSE 0 END),"
                " SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),"
                " COUNT(*) FROM chunks WHERE item_id=?",
                (item["id"],),
            ).fetchone()
            spike_counts = conn.execute(
                "SELECT"
                " SUM(CASE WHEN status NOT IN ('done','failed') THEN 1 ELSE 0 END),"
                " SUM(CASE WHEN status='done' THEN 1 ELSE 0 END),"
                " SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),"
                " COUNT(*) FROM spikes WHERE item_id=?",
                (item["id"],),
            ).fetchone()
            clip_count = conn.execute(
                "SELECT COUNT(*) FROM clips WHERE item_id=?", (item["id"],)
            ).fetchone()[0]
        active, done, failed, total = (int(x or 0) for x in counts)
        s_active, s_done, s_failed, s_total = (int(x or 0) for x in spike_counts)
        if active or s_active:
            continue
        if clip_count:
            update_item(item["id"], status="completed", finished_at=now(),
                        note=f"{clip_count} clip(s) selected", error=None)
            write_item_manifest(item["id"])
            log.info("item %s completed with %d clip(s)", item["id"][:8], clip_count)
        elif s_total and s_failed == s_total:
            update_item(item["id"], status="failed", finished_at=now(),
                        error=item.get("error") or "every chat-spike clip failed")
            log.error("item %s failed: every spike failed", item["id"][:8])
        elif total and failed == total and not s_total:
            update_item(item["id"], status="failed", finished_at=now(),
                        error=item.get("error") or "all captured chunks failed")
            log.error("item %s failed: every chunk failed", item["id"][:8])
        else:
            update_item(item["id"], status="completed", finished_at=now(),
                        note="stream ended without usable content",
                        error="no usable content")
            write_item_manifest(item["id"])
            log.info("item %s finished with no usable content", item["id"][:8])


def processor_loop(stop: threading.Event) -> None:
    log.info("processor started")
    while not stop.is_set():
        try:
            if not backend_healthy():
                if not wait_for_backend(stop):
                    break
            active = find_active_work()
            if active:
                poll_work(*active)
                stop.wait(float(CFG["clippyme"]["poll_seconds"]))
                continue
            reconcile_items()
            spike = next_ready_spike()
            if spike:
                start_spike(spike)
                stop.wait(2)
                continue
            item = next_vod_ready()
            if item:
                start_vod(item)
                stop.wait(2)
                continue
            stop.wait(5)
        except Exception:
            log.exception("processor iteration failed")
            stop.wait(10)
    log.info("processor stopped")


# --------------------------------------------------------------------------
# Clip collection + manifests
# --------------------------------------------------------------------------

def clip_title(clip: dict, item_title: str, index: int) -> str:
    raw = (clip.get("video_title_for_youtube_short") or clip.get("title")
           or f"{item_title} clip {index + 1}")
    return clean_text(raw, 95)


def spike_clip_title(clip: dict, item: dict, spike: dict, index: int) -> str:
    title = clip_title(clip, item.get("title") or "Live clip", index)
    # A forced-window job on an empty/quiet source falls back to the upload
    # filename (a UUID). Never publish that as the video title.
    looks_like_id = (
        not title.strip()
        or title.strip() in {str(spike.get("job_id") or ""), str(spike.get("id") or "")}
        or (len(title) == 36 and title.count("-") == 4)
    )
    if looks_like_id:
        stamp = time.strftime("%H:%M", time.localtime(float(spike["bucket_start"])))
        base = clean_text(item.get("title") or "Live clip", 70)
        title = f"{base} — chat spike {stamp}"
    return clean_text(title, 95)


def clip_description(clip: dict) -> str:
    raw = (clip.get("video_description_for_youtube_short")
           or clip.get("video_description_for_tiktok")
           or clip.get("video_description_for_instagram") or "")
    text = raw.replace("\x00", " ").strip()
    if not text:
        title = clean_text(clip.get("video_title_for_youtube_short") or clip.get("title") or "GTA VI clip", 180)
        text = (
            f"{title}. Watch more GTA VI gaming moments, live-stream reactions, "
            "and GTA 6 Shorts on GTA VI Paglu. Subscribe for new clips."
        )
    hashtags = str(CFG["publish"]["hashtags"] or "").strip()
    if hashtags and hashtags.lower() not in text.lower():
        text = (text + "\n\n" + hashtags).strip()
    return text[:4900]


def find_clip_file(output: Path, clip: dict, index: int) -> Path | None:
    name = os.path.basename(str(clip.get("clip_filename") or ""))
    if name:
        candidate = output / name
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    matches = sorted(output.glob(f"*_clip_{index + 1}.mp4"))
    matches += sorted(output.glob(f"*clip_{index + 1}.mp4"))
    for candidate in matches:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return None


def collect_clips(
    item: dict, job_id: str, result: dict, *, spike_id: str | None = None
) -> int:
    clips = result.get("clips") or []
    if not clips:
        write_global_manifest()
        return 0
    output = OUTPUT_DIR / job_id
    dest_dir = LIBRARY_DIR / item["id"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    publish_enabled = bool(item.get("publish")) and bool(CFG["publish"]["enabled"])
    collected = 0
    spike_sequence = 0
    if spike_id:
        with db() as conn:
            spike_sequence = int(conn.execute(
                "SELECT COUNT(*) FROM clips WHERE item_id=?", (item["id"],)
            ).fetchone()[0])

    for position, clip in enumerate(clips):
        index = int(clip.get("original_index", position))
        with db() as conn:
            already = conn.execute(
                "SELECT 1 FROM clips WHERE job_id=? AND clip_index=?",
                (job_id, index),
            ).fetchone()
        if already:
            log.debug("clip %s/%d already collected", job_id[:8], index)
            continue
        source = find_clip_file(output, clip, index)
        if source is None:
            log.warning("clip %d of job %s has no file on disk", index + 1, job_id[:8])
            continue
        library_source = source
        if spike_id and CFG["publish"].get("compose_subtitles"):
            try:
                library_source = compose_clip(
                    job_id, index, {"subtitles": True},
                    include_caption_file=bool(
                        CFG["publish"].get("upload_caption_tracks", True)
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("subtitle compose failed for job %s; using raw render: %s",
                            job_id[:8], str(exc)[:300])
        title = clip_title(clip, item.get("title") or "clip", index)
        if spike_id:
            spike = get_spike(spike_id) or {}
            if spike:
                title = spike_clip_title(clip, item, spike, index)
        sequence = (spike_sequence + position + 1) if spike_id else (index + 1)
        destination = dest_dir / f"{sequence:03d}_{slugify(title)}.mp4"
        if not destination.exists() or destination.stat().st_mtime < library_source.stat().st_mtime:
            shutil.copy2(library_source, destination)
        source_srt = library_source.with_suffix(".en.srt")
        if source_srt.is_file():
            shutil.copy2(source_srt, destination.with_suffix(".en.srt"))
        elif spike_id and CFG["publish"].get("upload_caption_tracks", True):
            log.warning("caption sidecar missing for %s", destination.name)
        cover_source = source.with_name(source.stem + "_cover.jpg")
        if not cover_source.exists():
            cover_source = next(iter(output.glob(f"*_clip_{index + 1}_cover.jpg")), None)
        cover_dest = None
        if cover_source and Path(cover_source).exists():
            cover_dest = destination.with_name(destination.stem + "_cover.jpg")
            if not cover_dest.exists():
                shutil.copy2(cover_source, cover_dest)

        clip_id = str(uuid.uuid4())
        status = "collected" if publish_enabled else "skipped"
        error = None if publish_enabled else (
            "publishing disabled" if not CFG["publish"]["enabled"] else "publish disabled for item"
        )
        with db() as conn:
            try:
                conn.execute(
                    "INSERT INTO clips(id, item_id, job_id, clip_index, spike_id, source_path,"
                    " library_path, cover_path, title, description, score, status, error,"
                    " created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (clip_id, item["id"], job_id, index, spike_id, str(source), str(destination),
                     str(cover_dest) if cover_dest else None, title,
                     clip_description(clip), float(clip.get("viral_score") or 0),
                     status, error, now(), now()),
                )
                collected += 1
            except sqlite3.IntegrityError:
                log.debug("clip %s/%d already collected", job_id[:8], index)
        log.info("collected clip %s (%s)", destination.name, status)

    merge_item_meta(item_id=item["id"], source_info=result.get("source_info"))
    write_item_manifest(item["id"])
    write_global_manifest()
    return collected


def write_item_manifest(item_id: str) -> None:
    item = get_item(item_id)
    if not item:
        return
    with db() as conn:
        chunks = [dict(row) for row in conn.execute(
            "SELECT seq, duration, size, status, job_id, error FROM chunks"
            " WHERE item_id=? ORDER BY seq", (item_id,)).fetchall()]
        clips = [dict(row) for row in conn.execute(
            "SELECT * FROM clips WHERE item_id=? ORDER BY clip_index", (item_id,)).fetchall()]
    data = {
        "item": {
            "id": item["id"], "url": item["url"], "kind": item["kind"],
            "status": item["status"], "title": item["title"],
            "source_id": item["source_id"], "created_at": item["created_at"],
            "finished_at": item.get("finished_at"), "note": item.get("note"),
        },
        "clips": [{
            "index": clip["clip_index"], "title": clip["title"],
            "description": clip["description"], "score": clip["score"],
            "status": clip["status"], "file": clip["library_path"],
            "cover": clip["cover_path"], "youtube_video_id": clip["youtube_video_id"],
            "privacy": clip["privacy"], "published_at": clip["published_at"],
            "error": clip["error"],
        } for clip in clips],
        "chunks": chunks,
        "source_info": item_meta(item).get("source_info"),
        "updated_at": now(),
    }
    atomic_write(
        LIBRARY_DIR / item_id / "manifest.json",
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
    )


def write_global_manifest() -> None:
    with db() as conn:
        items = [dict(row) for row in conn.execute(
            "SELECT * FROM items WHERE id IN (SELECT DISTINCT item_id FROM clips)"
            " ORDER BY created_at DESC LIMIT 200"
        ).fetchall()]
        payload = []
        for item in items:
            clips = [dict(row) for row in conn.execute(
                "SELECT clip_index, title, library_path, status, youtube_video_id,"
                " privacy, published_at FROM clips WHERE item_id=? ORDER BY clip_index",
                (item["id"],)).fetchall()]
            if not clips:
                continue
            payload.append({
                "id": item["id"], "url": item["url"], "title": item["title"],
                "kind": item["kind"], "created_at": item["created_at"],
                "clips": clips,
            })
    atomic_write(
        LIBRARY_DIR / "manifest.json",
        json.dumps({"generated_at": now(), "items": payload}, indent=2,
                   ensure_ascii=False, default=str),
    )


# --------------------------------------------------------------------------
# Publisher
# --------------------------------------------------------------------------

NO_SUFFIX_TITLE_LIMIT = 100


def next_publishable_clip() -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT c.*, i.title AS item_title, i.url AS item_url FROM clips c"
            " JOIN items i ON i.id=c.item_id"
            " WHERE c.status='collected' AND i.publish=1 AND c.next_attempt_at <= ?"
            " AND i.status!='cancelled'"
            " ORDER BY i.created_at, c.score DESC, c.clip_index LIMIT 1",
            (now(),),
        ).fetchone()
    return row_dict(row)


def start_of_today() -> float:
    return dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def next_midnight() -> float:
    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    return tomorrow.replace(hour=0, minute=5, second=0, microsecond=0).timestamp()


def publish_slot() -> tuple[bool, str, float]:
    with db() as conn:
        published_today = conn.execute(
            "SELECT COUNT(*) FROM clips WHERE status='published' AND published_at >= ?",
            (start_of_today(),),
        ).fetchone()[0]
        last = conn.execute(
            "SELECT MAX(published_at) FROM clips WHERE status='published'"
        ).fetchone()[0]
    limit = int(CFG["publish"]["max_per_day"])
    if limit and published_today >= limit:
        wait = max(60, next_midnight() - now())
        return False, f"daily upload limit reached ({published_today}/{limit})", wait
    gap = float(CFG["publish"]["min_gap_minutes"]) * 60
    if last and gap and now() - float(last) < gap:
        return False, "waiting for the publish gap", gap - (now() - float(last))
    return True, "", 0


def publish_command(clip: dict) -> list[str]:
    title = clean_text(clip.get("title") or "Clip", NO_SUFFIX_TITLE_LIMIT)
    suffix = clean_text(CFG["publish"]["title_suffix"], 20)
    if suffix and suffix.lower() not in title.lower():
        title = (title[: NO_SUFFIX_TITLE_LIMIT - len(suffix)] + suffix).strip()
    description = clip.get("description") or title
    if not description:
        description = title

    cmd = [
        YTK, "video", "upload", clip["library_path"],
        "--title", title,
        "--description", description[:4900],
        "--privacy", str(CFG["publish"]["privacy"]),
        "--category-id", str(CFG["publish"]["category_id"]),
        "--language", str(CFG["publish"]["language"]),
        "--audio-language", str(CFG["publish"]["audio_language"]),
    ]
    channel = str(CFG["publish"]["channel"] or "").strip()
    if channel:
        cmd += ["--my-channel", channel]
    tags: list[str] = []
    for tag in CFG["publish"]["tags"] + ["shorts"]:
        tag = clean_text(str(tag), 60)
        if tag and tag.lower() not in {t.lower() for t in tags}:
            tags.append(tag)
    total = 0
    for tag in tags:
        if total + len(tag) + 1 > 450:
            break
        cmd += ["--tags", tag]
        total += len(tag) + 1
    return cmd


def upload_caption_track(video_id: str, caption_path: Path) -> dict:
    if not caption_path.is_file() or caption_path.stat().st_size == 0:
        raise AutoClipError(f"caption sidecar missing or empty: {caption_path}")
    cmd = [
        YTK, "caption", "upload", video_id, str(caption_path),
        "--language", str(CFG["publish"].get("caption_language") or "en-US"),
        "--name", "English captions",
    ]
    channel = str(CFG["publish"].get("channel") or "").strip()
    if channel:
        cmd += ["--my-channel", channel]
    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=300,
    )
    if result.returncode:
        raise AutoClipError(parse_ytk_error(result.stderr).get("message") or result.stderr[-1200:])
    try:
        return json.loads((result.stdout or "").strip())
    except ValueError as exc:
        raise AutoClipError(f"caption upload returned invalid JSON: {result.stdout[-800:]}") from exc


def parse_ytk_error(stderr: str) -> dict:
    for line in reversed((stderr or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        with contextlib.suppress(ValueError):
            data = json.loads(line)
            if isinstance(data, dict) and "error" in data:
                return data["error"]
    return {"type": "unknown", "message": (stderr or "").strip()[-1200:]}


def handle_publish_error(clip: dict, error: dict) -> None:
    kind = str(error.get("type") or "unknown")
    message = str(error.get("message") or "")[:1500]
    lower = message.lower()
    attempts = int(clip["attempts"]) + 1

    if "quota" in kind or "quota" in lower or "upload limit" in lower:
        wait = max(600, next_midnight() - now())
        update_clip(clip["id"], status="collected", error=message,
                    attempts=attempts, next_attempt_at=now() + wait)
        log.warning("YouTube quota reached; pausing uploads until %s", ts_text(now() + wait))
        return

    if kind in ("auth", "forbidden", "protected"):
        update_clip(clip["id"], status="collected", error=message, attempts=attempts,
                    next_attempt_at=now() + 3600)
        log.error("YouTube rejected the upload (%s): %s", kind, message)
        return

    if kind in ("network", "timeout"):
        if attempts >= 6:
            update_clip(clip["id"], status="failed", error=message, attempts=attempts)
            log.error("giving up on upload after %d network errors", attempts)
        else:
            wait = min(21600, 300 * (2 ** attempts))
            update_clip(clip["id"], status="collected", error=message,
                        attempts=attempts, next_attempt_at=now() + wait)
            log.warning("upload network error; retrying in %.0f min", wait / 60)
        return

    if kind in ("usage", "invalid_request", "not_found"):
        update_clip(clip["id"], status="failed", error=f"{kind}: {message}",
                    attempts=attempts)
        log.error("upload rejected permanently (%s): %s", kind, message)
        return

    if attempts >= 5:
        update_clip(clip["id"], status="failed", error=message, attempts=attempts)
        log.error("giving up on upload after %d errors: %s", attempts, message)
    else:
        wait = min(21600, 900 * (2 ** attempts))
        update_clip(clip["id"], status="collected", error=message,
                    attempts=attempts, next_attempt_at=now() + wait)
        log.warning("upload failed; retrying in %.0f min: %s", wait / 60, message)


def publish_clip(clip: dict) -> None:
    update_clip(clip["id"], status="publishing", error=None)
    cmd = publish_command(clip)
    log.info("uploading %s (%s)", Path(clip["library_path"]).name,
             CFG["publish"]["privacy"])
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=float(CFG["publish"]["upload_timeout_seconds"]),
        )
    except subprocess.TimeoutExpired:
        update_clip(clip["id"], status="collected", attempts=int(clip["attempts"]) + 1,
                    next_attempt_at=now() + 1800, error="upload timed out")
        log.error("upload timed out")
        return

    if result.returncode != 0:
        error = parse_ytk_error(result.stderr)
        handle_publish_error(clip, error)
        return
    try:
        response = json.loads((result.stdout or "").strip())
    except ValueError:
        update_clip(clip["id"], status="publish_unknown",
                    error="upload succeeded but output was not JSON; check YouTube Studio")
        log.error("upload output was not JSON; not retrying to avoid duplicates")
        return

    video_id = response.get("id")
    if not video_id:
        update_clip(clip["id"], status="publish_unknown",
                    error="upload returned no video id; check YouTube Studio")
        log.error("upload returned no video id; not retrying to avoid duplicates")
        return
    update_clip(
        clip["id"], status="published", youtube_video_id=video_id,
        privacy=str(CFG["publish"]["privacy"]), published_at=now(),
        attempts=int(clip["attempts"]) + 1, error=None,
        meta=json.dumps({"upload": response}, ensure_ascii=False, default=str),
    )
    log.info("published %s as %s (%s)", Path(clip["library_path"]).name,
             response.get("url") or video_id, CFG["publish"]["privacy"])
    if CFG["publish"].get("upload_caption_tracks", True):
        caption_path = Path(clip["library_path"]).with_suffix(".en.srt")
        try:
            caption_result = upload_caption_track(video_id, caption_path)
            meta = {"upload": response, "caption_upload": caption_result}
            update_clip(clip["id"], meta=json.dumps(meta, ensure_ascii=False, default=str))
            log.info("uploaded YouTube caption track for %s: %s",
                     video_id, caption_result.get("status", "accepted"))
        except Exception as exc:  # video already published; never retry the video upload
            message = f"caption upload failed: {str(exc)[:1000]}"
            update_clip(clip["id"], error=message)
            log.exception("%s (video remains published)", message)
    write_item_manifest(clip["item_id"])
    write_global_manifest()


def publisher_loop(stop: threading.Event) -> None:
    if not CFG["publish"]["enabled"]:
        log.warning("publishing is disabled in config; clips will stay in library/")
        return
    log.info("publisher started (privacy=%s, max %s/day)",
             CFG["publish"]["privacy"], CFG["publish"]["max_per_day"])
    while not stop.is_set():
        try:
            ok, reason, wait = publish_slot()
            if not ok:
                stop.wait(min(300, max(10, wait)))
                continue
            clip = next_publishable_clip()
            if not clip:
                stop.wait(15)
                continue
            publish_clip(clip)
            stop.wait(5)
        except Exception:
            log.exception("publisher iteration failed")
            stop.wait(30)
    log.info("publisher stopped")

# --------------------------------------------------------------------------
# Daemon lifecycle
# --------------------------------------------------------------------------

def recover_state() -> None:
    """Put crash-interrupted rows back into a runnable state."""
    with db() as conn:
        conn.execute("UPDATE items SET status='queued' WHERE status='probing'")
        conn.execute(
            "UPDATE items SET status='ready' WHERE status='processing'"
            " AND job_id IS NULL AND kind='vod'"
        )
        conn.execute("UPDATE chunks SET status='captured' WHERE status='remuxing'")
        conn.execute("UPDATE chunks SET status='remuxed' WHERE status='submitting'")
        conn.execute(
            "UPDATE spikes SET status='cut' WHERE status='submitting'"
            " AND cut_path IS NOT NULL"
        )
        conn.execute(
            "UPDATE spikes SET status='pending' WHERE status='submitting'"
            " AND cut_path IS NULL"
        )
        conn.execute(
            "UPDATE spikes SET status='cut' WHERE status='processing'"
            " AND job_id IS NULL AND cut_path IS NOT NULL"
        )
        conn.execute(
            "UPDATE spikes SET status='pending' WHERE status='processing'"
            " AND job_id IS NULL AND cut_path IS NULL"
        )
        conn.execute(
            "UPDATE chunks SET status='remuxed' WHERE status='processing'"
            " AND job_id IS NULL AND mp4_path IS NOT NULL"
        )
        conn.execute(
            "UPDATE chunks SET status='captured' WHERE status='processing'"
            " AND job_id IS NULL AND mp4_path IS NULL"
        )
        conn.execute(
            "UPDATE clips SET status='publish_unknown',"
            " error='daemon restarted during upload; check YouTube Studio'"
            " WHERE status='publishing'"
        )


def daemon_running() -> bool:
    try:
        with open(LOCK_PATH, "a+") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle, fcntl.LOCK_UN)
    except OSError:
        pass
    return False


def daemon_pid() -> int | None:
    try:
        return int(PID_PATH.read_text().strip())
    except (OSError, ValueError):
        return None


def guarded(name: str, target, stop: threading.Event) -> None:
    try:
        target(stop)
    except Exception:
        log.exception("%s worker crashed", name)
        stop.set()


def run_daemon() -> int:
    init_runtime()
    setup_logging(daemon=True)

    lock_handle = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error("another auto-clip daemon already holds %s", LOCK_PATH)
        lock_handle.close()
        return 1

    PID_PATH.write_text(str(os.getpid()))
    stop = threading.Event()

    def on_signal(signum, _frame):
        log.info("received signal %s; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    try:
        recover_state()
        log.info("auto-clip %s starting (state=%s)", VERSION, BASE)
        log.info("publish=%s privacy=%s library=%s",
                 CFG["publish"]["enabled"], CFG["publish"]["privacy"], LIBRARY_DIR)
        threads = [
            threading.Thread(name="recorder", target=guarded,
                             args=("recorder", recorder_loop, stop), daemon=True),
            threading.Thread(name="processor", target=guarded,
                             args=("processor", processor_loop, stop), daemon=True),
            threading.Thread(name="publisher", target=guarded,
                             args=("publisher", publisher_loop, stop), daemon=True),
        ]
        for thread in threads:
            thread.start()
        while not stop.is_set():
            stop.wait(1)
        for thread in threads:
            thread.join(timeout=20)
        log.info("auto-clip stopped")
    finally:
        with contextlib.suppress(OSError):
            PID_PATH.unlink()
        with contextlib.suppress(OSError):
            fcntl.flock(lock_handle, fcntl.LOCK_UN)
        lock_handle.close()
    return 0


# --------------------------------------------------------------------------
# Doctor
# --------------------------------------------------------------------------

def check_line(name: str, ok: bool | None, detail: str = "") -> bool:
    if ok is True:
        print(f"[ ok ] {name}" + (f": {detail}" if detail else ""))
    elif ok is None:
        print(f"[warn] {name}" + (f": {detail}" if detail else ""))
    else:
        print(f"[FAIL] {name}" + (f": {detail}" if detail else ""))
    return ok is not False


def cmd_doctor(_args) -> int:
    init_runtime()
    setup_logging(verbose=True)
    print(f"auto-clip {VERSION}  ({BASE})")
    print(f"config: {CONFIG_PATH}")
    print()

    critical_ok = True
    critical_ok &= check_line("config file", True,
                              str(CONFIG_PATH) if CONFIG_PATH.exists() else "using built-in defaults")
    critical_ok &= check_line("backend", backend_healthy(), api_base())

    python_ok = Path(PYTHON).exists()
    critical_ok &= check_line("clippyme python", python_ok, PYTHON)
    if python_ok:
        result = run([PYTHON, "-m", "yt_dlp", "--version"], timeout=60)
        ytdlp_ok = result.returncode == 0
        critical_ok &= check_line("yt-dlp", ytdlp_ok, (result.stdout or result.stderr).strip()[:80])
    else:
        critical_ok &= check_line("yt-dlp", False, "python missing")

    for label, path in (("ffmpeg", FFMPEG), ("ffprobe", FFPROBE),
                        ("streamlink", STREAMLINK), ("curl", CURL)):
        result = run([path, "--version"], timeout=30)
        output = (result.stdout or "") + (result.stderr or "")
        detail = output.splitlines()[0][:90] if output.strip() else ""
        critical_ok &= check_line(label, bool(output.strip()), detail)

    ytk_ok = Path(YTK).exists()
    critical_ok &= check_line("youtube-kit", ytk_ok, YTK)
    if ytk_ok:
        result = run([YTK, "auth", "status"], timeout=60)
        channels = 0
        with contextlib.suppress(ValueError):
            data = json.loads((result.stdout or "").strip())
            channels = len(data.get("channels") or data.get("accounts") or [])
        check_line("YouTube login", result.returncode == 0 and channels > 0,
                   f"{channels} channel(s)")

    free_gb = disk_free_gb(BASE)
    guard = float(CFG["capture"]["disk_guard_free_gb"])
    check_line("disk space", free_gb >= guard, f"{free_gb:.1f} GB free (guard {guard:.0f} GB)")

    privacy = str(CFG["publish"]["privacy"])
    privacy_ok = privacy in ("private", "unlisted", "public")
    critical_ok &= check_line("privacy", privacy_ok, privacy)
    if privacy == "public":
        check_line("public uploads", None, "new clips will be published publicly")
    check_line("publishing", True,
               "enabled" if CFG["publish"]["enabled"] else "disabled (clips stay local)")
    if CFG["publish"]["enabled"]:
        check_line("upload budget", None,
                   f"{CFG['publish']['max_per_day']}/day, {CFG['publish']['min_gap_minutes']} min gap")

    cookies = project_path(CFG["clippyme"]["cookies"])
    check_line("cookies", None,
               f"{cookies} ({cookies.stat().st_size} bytes)" if cookies.exists() else "none (public sources only)")

    running = daemon_running()
    check_line("daemon", True if running else None,
               f"pid {daemon_pid()}" if running else "not running (systemctl --user start auto-clip.service)")
    for unit in ("clippyme-backend.service", "auto-clip.service"):
        result = run(["systemctl", "--user", "is-active", unit], timeout=10)
        state = (result.stdout or "").strip()
        check_line(unit, True if state == "active" else None, state or "unknown")

    with db() as conn:
        counts = {row[0]: row[1] for row in conn.execute(
            "SELECT status, COUNT(*) FROM items GROUP BY status")}
        clips = {row[0]: row[1] for row in conn.execute(
            "SELECT status, COUNT(*) FROM clips GROUP BY status")}
    print()
    print(f"items: {counts or 'none'}")
    print(f"clips: {clips or 'none'}")
    return 0 if critical_ok else 1


# --------------------------------------------------------------------------
# CLI commands
# --------------------------------------------------------------------------

def cmd_submit(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    normalized = validate_source_url(args.url)

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM items WHERE normalized_url=?"
            " AND status NOT IN ('completed','failed','cancelled')"
            " ORDER BY created_at DESC LIMIT 1", (normalized,),
        ).fetchone()
        if existing:
            item = dict(existing)
            message = f"already {item['status']} as {item['id'][:8]}"
            if args.json:
                print(json.dumps({"id": item["id"], "status": item["status"],
                                  "duplicate": True, "url": item["url"]}))
            else:
                print(message)
            return 0

        publish = 1 if (CFG["publish"]["enabled"] and not args.no_publish) else 0
        item_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO items(id, url, normalized_url, kind, status, publish,"
            " instructions, created_at, updated_at, next_action_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (item_id, normalized, normalized, "unknown", "queued", publish,
             args.instructions, now(), now(), 0),
        )
        item = dict(conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone())

    if args.start:
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            run(["systemctl", "--user", "start", "auto-clip.service"], timeout=60)
    running = daemon_running()
    if args.json:
        print(json.dumps({
            "id": item["id"], "status": item["status"], "url": item["url"],
            "publish": bool(item["publish"]), "daemon_running": running,
        }, ensure_ascii=False))
    else:
        print(f"queued {item['id'][:8]}  {item['url']}")
        if item["publish"]:
            print(f"publish: {CFG['publish']['privacy']} via @{str(CFG['publish']['channel']).lstrip('@')}")
        else:
            print("publish: no (clips stay in library/)")
        if running:
            print("daemon: running")
        else:
            print("daemon: NOT running — start it with: systemctl --user start auto-clip.service")
        print(f"follow: auto-clip status {item['id'][:8]}")
    return 0


def cmd_scan_live(args) -> int:
    """Check configured gaming channels and queue currently live GTA streams."""
    init_runtime()
    setup_logging(verbose=args.verbose)
    outcomes = []
    gta_pattern = re.compile(r"\b(?:gta\s*(?:6|vi)|grand theft auto\s*(?:6|vi))\b", re.I)
    for channel in CFG.get("watchlist", {}).get("channels", []):
        name = str(channel.get("name") or channel.get("url") or "channel")
        url = str(channel.get("url") or "").strip()
        try:
            info = probe(url)
            title = str(info.get("title") or "")
            if classify_info(info) != "live":
                outcomes.append({"channel": name, "status": "not_live", "title": title})
                continue
            source_id = str(info.get("id") or "").strip()
            if CFG.get("watchlist", {}).get("require_gta_title", True) and not gta_pattern.search(title):
                outcomes.append({"channel": name, "status": "live_skipped_non_gta", "title": title})
                continue
            if not source_id:
                outcomes.append({"channel": name, "status": "error", "error": "live probe returned no video id"})
                continue
            video_url = f"https://www.youtube.com/watch?v={source_id}"
            normalized = validate_source_url(video_url)
            if args.dry_run:
                outcomes.append({"channel": name, "status": "would_queue",
                                 "publish": bool(CFG["publish"].get("enabled")),
                                 "video_id": source_id, "title": title, "url": video_url})
                continue
            with db() as conn:
                existing = conn.execute(
                    "SELECT id,status FROM items WHERE (normalized_url=? OR source_id=?)"
                    " AND status NOT IN ('completed','failed','cancelled')"
                    " ORDER BY created_at DESC LIMIT 1", (normalized, source_id),
                ).fetchone()
                if existing:
                    outcomes.append({"channel": name, "status": "already_queued",
                                     "item_id": existing["id"], "item_status": existing["status"],
                                     "video_id": source_id, "title": title})
                    continue
                item_id = str(uuid.uuid4())
                publish = int(bool(CFG["publish"].get("enabled")))
                conn.execute(
                    "INSERT INTO items(id,url,normalized_url,kind,status,publish,instructions,"
                    "created_at,updated_at,next_action_at,source_id,title)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (item_id, video_url, normalized, "unknown", "queued", publish,
                     None, now(), now(), 0, source_id, title),
                )
            outcomes.append({"channel": name, "status": "queued", "item_id": item_id,
                             "publish": bool(publish), "video_id": source_id, "title": title,
                             "url": video_url})
        except ProbeError as exc:
            outcomes.append({"channel": name,
                             "status": "not_live" if exc.not_live else "probe_error",
                             "error": str(exc).strip().splitlines()[-1][:220]})
        except Exception as exc:  # one inaccessible channel must not block the rest
            outcomes.append({"channel": name, "status": "probe_error", "error": str(exc)[:220]})
    if any(x.get("status") == "queued" for x in outcomes) and not daemon_running():
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            run(["systemctl", "--user", "start", "auto-clip.service"], timeout=60)
    if args.json:
        print(json.dumps(outcomes, ensure_ascii=False))
    else:
        for result in outcomes:
            print(f"{result['channel']}: {result['status']} — "
                  f"{result.get('title') or result.get('error') or result.get('video_id','')}")
        print(f"queued {sum(x.get('status') == 'queued' for x in outcomes)} stream(s); "
              f"publishing cap {CFG['publish']['max_per_day']}/day, gap {CFG['publish']['min_gap_minutes']} min")
    return 0


def cmd_list(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    with db() as conn:
        items = [dict(row) for row in conn.execute(
            "SELECT i.*,"
            " (SELECT COUNT(*) FROM clips c WHERE c.item_id=i.id) AS clip_count,"
            " (SELECT COUNT(*) FROM clips c WHERE c.item_id=i.id AND c.status='published') AS published_count"
            " FROM items i ORDER BY i.created_at DESC LIMIT ?", (args.limit,),
        ).fetchall()]
    if args.json:
        print(json.dumps(items, ensure_ascii=False, default=str))
        return 0
    if not items:
        print("no items yet — run: auto-clip submit <url>")
        return 0
    print(f"daemon: {'running' if daemon_running() else 'stopped'}")
    print(f"{'ID':8}  {'STATUS':10} {'KIND':8} {'AGE':>5}  {'CLIPS':>5} {'PUB':>3}  TITLE")
    for item in items:
        print(f"{item['id'][:8]}  {item['status'][:10]:10} {item['kind'][:8]:8}"
              f" {age_text(item['created_at']):>5}  {item['clip_count']:>5}"
              f" {item['published_count']:>3}  {clean_text(item['title'] or item['url'], 60)}")
    return 0


def item_counts(item_id: str) -> tuple[dict, dict]:
    with db() as conn:
        chunks = {row[0]: row[1] for row in conn.execute(
            "SELECT status, COUNT(*) FROM chunks WHERE item_id=? GROUP BY status",
            (item_id,))}
        clips = {row[0]: row[1] for row in conn.execute(
            "SELECT status, COUNT(*) FROM clips WHERE item_id=? GROUP BY status",
            (item_id,))}
    return chunks, clips


def cmd_status(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    if not args.item:
        if args.json:
            with db() as conn:
                counts = {row[0]: row[1] for row in conn.execute(
                    "SELECT status, COUNT(*) FROM items GROUP BY status")}
            print(json.dumps({"daemon_running": daemon_running(),
                              "pid": daemon_pid(), "items": counts}))
        else:
            print(f"daemon: {'running' if daemon_running() else 'stopped'}"
                  + (f" (pid {daemon_pid()})" if daemon_running() else ""))
            with db() as conn:
                counts = {row[0]: row[1] for row in conn.execute(
                    "SELECT status, COUNT(*) FROM items GROUP BY status")}
            print("items: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none"))
        return 0

    item = find_item(args.item)
    with db() as conn:
        chunks = [dict(row) for row in conn.execute(
            "SELECT * FROM chunks WHERE item_id=? ORDER BY seq", (item["id"],))]
        clips = [dict(row) for row in conn.execute(
            "SELECT * FROM clips WHERE item_id=? ORDER BY clip_index", (item["id"],))]
    if args.json:
        print(json.dumps({"item": item, "chunks": chunks, "clips": clips},
                         ensure_ascii=False, default=str))
        return 0

    print(f"{item['id']}  {item['status']}  ({item['kind']})")
    print(f"  url:     {item['url']}")
    print(f"  title:   {clean_text(item['title'] or '-', 100)}")
    print(f"  created: {ts_text(item['created_at'])}  updated: {ts_text(item['updated_at'])}")
    if item.get("next_action_at"):
        print(f"  next:    {ts_text(item['next_action_at'])}")
    if item.get("note"):
        print(f"  note:    {item['note']}")
    if item.get("error"):
        print(f"  error:   {clean_text(item['error'], 300)}")
    print(f"  publish: {'yes' if item['publish'] else 'no'}")
    if chunks:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(item_counts(item["id"])[0].items()))
        print(f"  chunks:  {summary}")
        for chunk in chunks:
            print(f"    #{chunk['seq']:03d} {chunk['status']:10} "
                  f"{(chunk['duration'] or 0) / 60:5.1f}m  {chunk.get('job_id') or '-'}"
                  + (f"  {clean_text(chunk.get('error'), 80)}" if chunk.get("error") else ""))
    if clips:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(item_counts(item["id"])[1].items()))
        print(f"  clips:   {summary}")
        for clip in clips:
            extra = ""
            if clip.get("youtube_video_id"):
                extra = f"  https://youtu.be/{clip['youtube_video_id']} ({clip.get('privacy')})"
            elif clip.get("error"):
                extra = f"  {clean_text(clip['error'], 80)}"
            print(f"    {clip['clip_index'] + 1:03d} {clip['status']:10} "
                  f"{clip.get('score') or 0:5.0f}  {clean_text(clip.get('title'), 60)}{extra}")
    return 0


def cmd_cancel(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    item = find_item(args.item)
    if item["status"] in TERMINAL_ITEMS:
        print(f"already {item['status']}")
        return 0
    update_item(item["id"], status="cancelled", finished_at=now(), note="cancelled by user")
    if item.get("job_id"):
        cancel_remote_job(item["job_id"])
    with db() as conn:
        active = [dict(row) for row in conn.execute(
            "SELECT * FROM chunks WHERE item_id=? AND status IN"
            " ('submitting','submitted','processing')", (item["id"],)).fetchall()]
        active_spikes = [dict(row) for row in conn.execute(
            "SELECT * FROM spikes WHERE item_id=? AND status IN"
            " ('submitting','processing')", (item["id"],)).fetchall()]
        conn.execute(
            "UPDATE chunks SET status='skipped', updated_at=? WHERE item_id=?"
            " AND status NOT IN ('done','failed','skipped','cancelled')",
            (now(), item["id"]),
        )
        conn.execute(
            "UPDATE spikes SET status='cancelled', updated_at=? WHERE item_id=?"
            " AND status NOT IN ('done','failed','cancelled')",
            (now(), item["id"]),
        )
        conn.execute(
            "UPDATE clips SET status='skipped', updated_at=? WHERE item_id=?"
            " AND status='collected'", (now(), item["id"]),
        )
    for chunk in active:
        if chunk.get("job_id"):
            cancel_remote_job(chunk["job_id"])
    for spike in active_spikes:
        if spike.get("job_id"):
            cancel_remote_job(spike["job_id"])
    print(f"cancelled {item['id'][:8]}")
    return 0


def cmd_retry(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    item = find_item(args.item)
    changed = 0
    with db() as conn:
        if args.publish:
            conn.execute("UPDATE items SET publish=1, updated_at=? WHERE id=?",
                         (now(), item["id"]))
            changed += conn.execute(
                "UPDATE clips SET status='collected', error=NULL, next_attempt_at=0,"
                " updated_at=? WHERE item_id=? AND status='skipped'",
                (now(), item["id"]),
            ).rowcount
        else:
            if item["status"] == "failed":
                conn.execute(
                    "UPDATE items SET status='queued', error=NULL, attempts=0,"
                    " job_id=NULL, next_action_at=0, finished_at=NULL, updated_at=?"
                    " WHERE id=?", (now(), item["id"]),
                )
                changed += 1
            for chunk in conn.execute(
                "SELECT * FROM chunks WHERE item_id=? AND status='failed'", (item["id"],)
            ).fetchall():
                if chunk["mp4_path"] and Path(chunk["mp4_path"]).exists():
                    state = "remuxed"
                elif chunk["ts_path"] and Path(chunk["ts_path"]).exists():
                    state = "captured"
                else:
                    continue
                conn.execute(
                    "UPDATE chunks SET status=?, error=NULL, job_id=NULL, attempts=0,"
                    " next_attempt_at=0, updated_at=? WHERE id=?",
                    (state, now(), chunk["id"]),
                )
                changed += 1
            changed += conn.execute(
                "UPDATE clips SET status='collected', error=NULL, attempts=0,"
                " next_attempt_at=0, updated_at=? WHERE item_id=?"
                " AND status IN ('failed','publish_unknown')",
                (now(), item["id"]),
            ).rowcount
    print(f"retried {changed} row(s) for {item['id'][:8]}")
    if changed and not daemon_running():
        print("daemon is stopped; run: systemctl --user start auto-clip.service")
    return 0


def cmd_publish(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    item = find_item(args.item)
    with db() as conn:
        conn.execute("UPDATE items SET publish=1, updated_at=? WHERE id=?",
                     (now(), item["id"]))
        changed = conn.execute(
            "UPDATE clips SET status='collected', error=NULL, attempts=0,"
            " next_attempt_at=0, updated_at=? WHERE item_id=? AND status='skipped'",
            (now(), item["id"]),
        ).rowcount
    print(f"publishing enabled for {item['id'][:8]} ({changed} clip(s) queued)")
    return 0


def cmd_probe(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    info = probe(args.url)
    payload = {
        "kind": classify_info(info),
        "id": info.get("id"),
        "title": info.get("title"),
        "duration": info.get("duration"),
        "is_live": info.get("is_live"),
        "live_status": info.get("live_status"),
        "url": canonical_stream_url(info, args.url),
        "extractor": info.get("extractor_key") or info.get("extractor"),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_gc(args) -> int:
    init_runtime()
    setup_logging(verbose=args.verbose)
    cutoff = now() - float(args.days) * 86400
    freed = 0
    removed = 0
    with db() as conn:
        items = {row[0]: dict(row) for row in conn.execute(
            "SELECT id, status, updated_at FROM items").fetchall()}
    for directory in CAPTURE_DIR.iterdir() if CAPTURE_DIR.exists() else []:
        if not directory.is_dir():
            continue
        item = items.get(directory.name)
        if item and item["status"] not in TERMINAL_ITEMS:
            continue
        if item and float(item["updated_at"]) > cutoff:
            continue
        for path in directory.rglob("*"):
            if path.is_file():
                freed += path.stat().st_size
        shutil.rmtree(directory, ignore_errors=True)
        removed += 1
    print(f"removed {removed} capture dir(s), freed {freed / (1024 ** 2):.1f} MB")
    return 0


def cmd_selftest_live(args) -> int:
    init_runtime()
    setup_logging(verbose=True)
    info = probe(args.url)
    if classify_info(info) != "live":
        print(f"not live right now ({classify_info(info)}); try an active stream")
        return 1
    url = canonical_stream_url(info, args.url)
    work = STATE_DIR / "selftest-live"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    print(f"recording {url} for {args.seconds}s in {args.segment_seconds}s segments")
    fake = {"id": "__selftest__", "url": url}
    stop = threading.Event()
    deadline = now() + float(args.seconds)
    rc, reason = run_capture_session(
        fake, work, stop,
        segment_seconds=float(args.segment_seconds),
        max_height=args.max_height,
        deadline=deadline,
        check_item=False,
        min_chunk_seconds=5,
    )
    print(f"session ended: reason={reason} rc={rc}")
    files = chunk_paths(work)
    if not files:
        print("no chunks were written")
        return 1
    print(f"{'CHUNK':16} {'SECONDS':>8} {'MB':>7}  PROBE")
    ok = 0
    for path in files:
        duration = probe_duration(path)
        if duration is not None and duration > 0:
            ok += 1
        print(f"{path.name:16} {(duration or 0):8.1f} {path.stat().st_size / (1024 ** 2):7.1f}"
              f"  {'ok' if duration and duration > 0 else 'FAILED'}")
    print(f"{ok}/{len(files)} chunk(s) readable")
    print(f"files kept in {work}")
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="auto-clip",
        description="Hands-off live/VOD clipper on top of ClippyMe.",
    )
    parser.add_argument("--version", action="version", version=f"auto-clip {VERSION}")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit", help="queue a YouTube/Twitch/Kick link")
    submit.add_argument("url")
    submit.add_argument("--no-publish", action="store_true", help="keep clips local")
    submit.add_argument("--instructions", help="override the analysis instructions")
    submit.add_argument("--start", action="store_true", help="start the user service if needed")
    submit.add_argument("--json", action="store_true")
    submit.set_defaults(func=cmd_submit)

    listing = sub.add_parser("list", help="list queued and finished items")
    listing.add_argument("--limit", type=int, default=30)
    listing.add_argument("--json", action="store_true")
    listing.set_defaults(func=cmd_list)

    status = sub.add_parser("status", help="show daemon and item status")
    status.add_argument("item", nargs="?")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)

    cancel = sub.add_parser("cancel", help="cancel an item and its running capture")
    cancel.add_argument("item")
    cancel.set_defaults(func=cmd_cancel)

    retry = sub.add_parser("retry", help="retry failed chunks or uploads")
    retry.add_argument("item")
    retry.add_argument("--publish", action="store_true", help="queue skipped clips for upload")
    retry.set_defaults(func=cmd_retry)

    publish = sub.add_parser("publish", help="enable publishing for a finished item")
    publish.add_argument("item")
    publish.set_defaults(func=cmd_publish)

    scan_live = sub.add_parser(
        "scan-live", help="check the configured gaming channels and queue live GTA streams"
    )
    scan_live.add_argument("--json", action="store_true")
    scan_live.add_argument("--dry-run", action="store_true", help="probe and report without queueing")
    scan_live.set_defaults(func=cmd_scan_live)

    probe_cmd = sub.add_parser("probe", help="classify a link without queueing it")
    probe_cmd.add_argument("url")
    probe_cmd.set_defaults(func=cmd_probe)

    doctor = sub.add_parser("doctor", help="check dependencies, backend and YouTube auth")
    doctor.set_defaults(func=cmd_doctor)

    run_cmd = sub.add_parser("run", help="run the worker in the foreground (systemd uses this)")
    run_cmd.set_defaults(func=lambda _args: run_daemon())

    gc = sub.add_parser("gc", help="delete old finished capture directories")
    gc.add_argument("--days", type=float, default=7)
    gc.set_defaults(func=cmd_gc)

    selftest = sub.add_parser("selftest-live", help="record a few test segments from a live URL")
    selftest.add_argument("url")
    selftest.add_argument("--seconds", type=float, default=50)
    selftest.add_argument("--segment-seconds", type=float, default=20)
    selftest.add_argument("--max-height", type=int, default=720)
    selftest.set_defaults(func=cmd_selftest_live)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except AutoClipError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
