# Remux Service — Full Build Spec

Use this file to regenerate the entire project from scratch.
Paste it into a new Claude conversation with the prompt:
**"Build this project exactly as specified:"**

---

## What it is

A self-hosted, Dockerized web app that scans media folders for video files
above a bitrate threshold (GB/hr), shows them in a UI for review, then
re-encodes them to HEVC using ffmpeg — with live per-file progress streaming.

---

## Stack

| Layer | Choice | Reason |
|---|---|---|
| Backend | FastAPI (Python) | Async, SSE support, easy subprocess streaming |
| Frontend | Vanilla HTML/JS | No build step, served by FastAPI as static |
| Container | Docker + Compose | Single service, media volume mount |
| Encoding | ffmpeg subprocess | hevc_nvenc (GPU) → libx265 (CPU) fallback |
| Progress | SSE (text/event-stream) | Real-time per-file % without websocket complexity |

---

## File structure

```
remux-service/
├── SPEC.md                  ← this file
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── app/
    ├── main.py              ← FastAPI app, routes, SSE endpoint
    ├── scanner.py           ← ffprobe scan logic
    ├── encoder.py           ← ffmpeg encode + progress parsing
    └── static/
        └── index.html       ← full dashboard UI (vanilla JS)
```

---

## API routes

| Method | Path | Description |
|---|---|---|
| GET | `/` | Serve `index.html` |
| POST | `/scan` | Body: `{folders, threshold_gb_hr, depth}` → returns file list JSON |
| GET | `/queue` | Returns current encode queue state |
| POST | `/queue/add` | Body: `{files: [path, ...]}` — add files to queue |
| DELETE | `/queue/{path}` | Remove a file from queue |
| GET | `/encode/stream` | SSE stream — starts encoding queue, streams progress events |
| POST | `/encode/cancel` | Kill active ffmpeg subprocess |
| GET | `/health` | `{status: ok}` |

---

## SSE event schema

Every event is JSON on the `data:` field.

```json
{ "type": "progress",  "path": "/media/movies/Film.mkv", "percent": 42, "fps": 24.3, "eta_seconds": 180 }
{ "type": "file_done", "path": "/media/movies/Film.mkv", "saved_gb": 12.4, "ratio": 38.2 }
{ "type": "queue_done","total_saved_gb": 34.1 }
{ "type": "error",     "path": "/media/movies/Film.mkv", "message": "ffmpeg exit 1" }
```

---

## scanner.py logic

```
probe_file(path) → VideoFile | None
  - subprocess: ffprobe -v quiet -print_format json -show_format -show_streams <path>
  - extract: duration (seconds), size (bytes), video codec name, all stream metadata
  - compute: bitrate_gb_per_hour = (size_bytes / 1024^3) / (duration_seconds / 3600)
  - skip if duration < 60s (clips/trailers)
  - return VideoFile dataclass

scan_folders(folders, threshold, max_depth) → list[VideoFile]
  - os.walk each folder, prune at max_depth
  - filter VIDEO_EXTENSIONS = {.mkv .mp4 .avi .mov .ts .m2ts .wmv .flv .webm}
  - skip files ending in ._remux_tmp (in-progress guard)
  - call probe_file on each match
  - return only files where bitrate_gb_per_hour > threshold
```

---

## encoder.py logic

```
pick_encoder(preference) → (encoder_name, extra_args)
  - "auto": run `ffmpeg -encoders`, check for hevc_nvenc → use if present
  - "nvidia": force hevc_nvenc
  - "cpu": force libx265
  - hevc_nvenc args: ["-rc", "vbr", "-cq", "<CRF>", "-preset", "p4"]
  - libx265 args:   ["-crf", "<CRF>", "-preset", "medium"]

encode_file(vf, encoder, encoder_args, crf, naming) → AsyncGenerator[dict]
  - build ffmpeg cmd:
      ffmpeg -hide_banner -y -i <source>
        -map 0
        -c:v <encoder> <encoder_args>
        -c:a copy -c:s copy -c:d copy
        -progress pipe:1          ← key: structured progress to stdout
        <tmp_dest>
  - parse stdout lines for "out_time_ms=" and "total_size=" to compute percent
  - yield SSE-compatible dicts as progress events
  - on completion: ffprobe verify (duration > 60s)
  - if replace mode: original → .orig_bak → move tmp → delete bak
  - yield file_done event with saved_gb

Progress parsing from ffmpeg -progress pipe:1:
  ffmpeg emits key=value lines. Relevant keys:
    out_time_ms   → microseconds encoded so far
    total_size    → bytes written so far
    progress=end  → signals completion
  Percent = (out_time_ms / 1000000) / source_duration_seconds * 100
```

---

## Dockerfile

```dockerfile
FROM python:3.12-slim
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
EXPOSE 7755
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7755"]
```

---

## docker-compose.yml

```yaml
services:
  remux:
    build: .
    container_name: remux-service
    ports:
      - "7755:7755"
    volumes:
      - /mnt/pve/data0/media:/media:rw
      - ./logs:/app/logs
    environment:
      - MEDIA_ROOT=/media
    restart: unless-stopped
```

Path mapping: host `/mnt/pve/data0/media` → container `/media`.
UI folder inputs should use `/media/movies`, `/media/tv` etc.

---

## index.html UI spec

Single-page dashboard. Sections in order:

1. **Stats bar** — 4 metric cards: Files scanned / Above threshold / Remuxed / Space saved
2. **Scan config card**
   - Folder list (add/remove paths, pre-filled with `/media/movies`)
   - Threshold (GB/hr), depth, encoder, CRF slider, output naming
   - "Scan" button → POST /scan → populates file table
3. **File table** (shows only above-threshold files after scan)
   - Columns: filename, size (GB), bitrate (GB/hr), duration, codec, status, action
   - Status badges: queued / encoding / done / error / skipped
   - Per-row skip button; "Queue all" button
   - Inline progress bar on encoding row (driven by SSE)
4. **Encode controls**
   - "Start encoding" → opens GET /encode/stream as EventSource
   - "Cancel" → POST /encode/cancel
5. **Log panel** — monospace, auto-scroll, shows SSE events as human-readable lines

SSE handling in JS:
```js
const es = new EventSource('/encode/stream');
es.onmessage = e => {
  const ev = JSON.parse(e.data);
  if (ev.type === 'progress') updateRow(ev.path, ev.percent);
  if (ev.type === 'file_done') markDone(ev.path, ev.saved_gb);
  if (ev.type === 'queue_done') es.close();
};
```

---

## Deployment on Proxmox Ubuntu VM

```bash
# on the Ubuntu 24.04 VM (not Proxmox host)
git clone <your-repo> ~/remux-service
cd ~/remux-service
docker compose up -d --build
# access at http://<VM-LAN-IP>:7755
```

No Nginx proxy needed for LAN-only use.
For public access add an NPM proxy host: requests.braje.sh → VM:7755.

---

## Config defaults (top of main.py)

```python
DEFAULT_FOLDERS   = ["/media/movies"]
DEFAULT_THRESHOLD = 2.0   # GB/hr
DEFAULT_DEPTH     = 2
DEFAULT_CRF       = 23
DEFAULT_CODEC     = "hevc"
DEFAULT_ENCODER   = "auto"
DEFAULT_NAMING    = "replace"
```
