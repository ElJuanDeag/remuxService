import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .scanner import scan_folders, VideoFile
from .encoder import encode_file, pick_encoder, cancel_encode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/app/logs/remux.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

DEFAULT_FOLDERS   = ["/media/movies"]
DEFAULT_THRESHOLD = 2.0
DEFAULT_DEPTH     = 2
DEFAULT_CRF       = 23
DEFAULT_ENCODER   = "cpu"
DEFAULT_NAMING    = "replace"

_scan_results: dict[str, VideoFile] = {}
_queue: list[str] = []
_status: dict[str, dict] = {}
_encode_lock = asyncio.Lock()

app = FastAPI(title="Remux Service")

static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


class ScanRequest(BaseModel):
    folders: list[str] = DEFAULT_FOLDERS
    threshold_gb_hr: float = DEFAULT_THRESHOLD
    depth: int = DEFAULT_DEPTH

class QueueAddRequest(BaseModel):
    paths: list[str]

class EncodeStartRequest(BaseModel):
    crf: int = DEFAULT_CRF
    encoder: str = DEFAULT_ENCODER
    naming: str = DEFAULT_NAMING


@app.get("/", response_class=HTMLResponse)
async def index():
    html = (static_dir / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/scan")
async def scan(req: ScanRequest):
    global _scan_results, _queue, _status
    log.info("Scan requested: folders=%s threshold=%.1f depth=%d",
             req.folders, req.threshold_gb_hr, req.depth)

    files = await scan_folders(req.folders, req.threshold_gb_hr, req.depth)

    _scan_results = {f.path: f for f in files}
    _queue = [f.path for f in files]
    _status = {f.path: {"status": "queued", "percent": 0, "saved_gb": None} for f in files}

    log.info("Scan complete: %d file(s) above threshold", len(files))
    return {
        "count": len(files),
        "files": [f.to_dict() | {"status": "queued"} for f in files],
    }


@app.get("/queue")
async def get_queue():
    out = []
    for path in _queue:
        vf = _scan_results.get(path)
        st = _status.get(path, {})
        if vf:
            out.append(vf.to_dict() | st)
    return {"queue": out}


@app.post("/queue/add")
async def queue_add(req: QueueAddRequest):
    added = []
    for p in req.paths:
        if p in _scan_results and p not in _queue:
            _queue.append(p)
            _status[p] = {"status": "queued", "percent": 0, "saved_gb": None}
            added.append(p)
    return {"added": len(added)}


@app.delete("/queue/{path:path}")
async def queue_remove(path: str):
    full = "/" + path
    if full in _queue:
        _queue.remove(full)
        _status[full] = {"status": "skipped", "percent": 0, "saved_gb": None}
        return {"removed": full}
    raise HTTPException(404, "Not in queue")


@app.post("/encode/cancel")
async def encode_cancel():
    await cancel_encode()
    return {"cancelled": True}

@app.get("/status")
async def get_status():
    return {"encoding": _encode_lock.locked()}

@app.get("/encode/stream")
async def encode_stream(
    crf: int = DEFAULT_CRF,
    encoder: str = DEFAULT_ENCODER,
    naming: str = DEFAULT_NAMING,
    workers: int = 1,
):
    if _encode_lock.locked():
        raise HTTPException(409, "Encode already running")

    enc, enc_args = await pick_encoder(encoder)
    workers = max(1, min(workers, 8))

    async def event_generator():
        async with _encode_lock:
            total_saved = 0.0
            event_queue: asyncio.Queue = asyncio.Queue()
            pending = [p for p in _queue if _status.get(p, {}).get("status") == "queued"]

            if not pending:
                yield _sse({"type": "queue_done", "total_saved_gb": 0})
                return

            sem = asyncio.Semaphore(workers)
            active_tasks: set[asyncio.Task] = set()

            async def encode_one(path: str):
                vf = _scan_results.get(path)
                if not vf:
                    return
                async with sem:
                    _status[path]["status"] = "encoding"
                    async for event in encode_file(vf, enc, enc_args, crf, naming):
                        if event["type"] == "progress":
                            _status[path]["percent"] = event.get("percent", 0)
                        elif event["type"] == "file_done":
                            _status[path]["status"] = "done"
                            _status[path]["percent"] = 100
                            _status[path]["saved_gb"] = event.get("saved_gb", 0)
                        elif event["type"] == "error":
                            _status[path]["status"] = "error"
                        await event_queue.put(event)

            for path in pending:
                task = asyncio.create_task(encode_one(path))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            while active_tasks or not event_queue.empty():
                try:
                    event = await asyncio.wait_for(event_queue.get(), timeout=0.2)
                    if event["type"] == "file_done":
                        total_saved += event.get("saved_gb", 0)
                    yield _sse(event)
                except asyncio.TimeoutError:
                    continue

            yield _sse({"type": "queue_done", "total_saved_gb": round(total_saved, 2)})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


class ProbeRawRequest(BaseModel):
    path: str

@app.post("/probe/raw")
async def probe_raw(req: ProbeRawRequest):
    probe_cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_streams", "-show_format",
        req.path,
    ]
    probe_proc = await asyncio.create_subprocess_exec(
        *probe_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    probe_out, _ = await probe_proc.communicate()
    try:
        probe_data = json.loads(probe_out)
    except Exception:
        probe_data = {}

    ffmpeg_cmd = [
        "ffmpeg", "-hide_banner", "-y",
        "-i", req.path,
        "-map", "0", "-c", "copy",
        "-t", "3", "-f", "null", "-",
    ]
    ffmpeg_proc = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, ffmpeg_err = await asyncio.wait_for(ffmpeg_proc.communicate(), timeout=30)
        ffmpeg_stderr = ffmpeg_err.decode("utf-8", errors="ignore")
    except asyncio.TimeoutError:
        ffmpeg_stderr = "timed out"

    streams_summary = [
        {
            "index": s.get("index"),
            "type": s.get("codec_type"),
            "codec": s.get("codec_name"),
            "lang": s.get("tags", {}).get("language", ""),
            "title": s.get("tags", {}).get("title", ""),
        }
        for s in probe_data.get("streams", [])
    ]

    return {
        "path": req.path,
        "streams": streams_summary,
        "ffmpeg_stderr": ffmpeg_stderr,
    }

