import os
import json
import asyncio
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

VIDEO_EXTENSIONS: set[str] = {
    ".mkv", ".mp4", ".avi", ".mov", ".ts", ".m2ts", ".wmv", ".flv", ".webm"
}


@dataclass
class VideoFile:
    path: str
    name: str
    size_bytes: int
    duration_seconds: float
    video_codec: str
    bitrate_gb_per_hour: float
    streams: list[dict] = field(default_factory=list)

    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)

    @property
    def duration_minutes(self) -> float:
        return self.duration_seconds / 60

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "name": self.name,
            "size_gb": round(self.size_gb, 2),
            "size_bytes": self.size_bytes,
            "duration_minutes": round(self.duration_minutes, 1),
            "duration_seconds": self.duration_seconds,
            "video_codec": self.video_codec,
            "bitrate_gb_per_hour": round(self.bitrate_gb_per_hour, 2),
        }


async def probe_file(path: Path) -> Optional[VideoFile]:
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format", "-show_streams",
        str(path),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0:
            log.debug("ffprobe failed for %s", path.name)
            return None

        data = json.loads(stdout)
    except (asyncio.TimeoutError, json.JSONDecodeError, FileNotFoundError) as exc:
        log.debug("Could not probe %s: %s", path.name, exc)
        return None

    fmt = data.get("format", {})
    duration = float(fmt.get("duration", 0))
    if duration < 60:
        return None

    streams = data.get("streams", [])
    video_stream = next(
        (s for s in streams if s.get("codec_type") == "video"), None
    )
    if not video_stream:
        return None

    size_bytes = path.stat().st_size
    duration_hours = duration / 3600
    bitrate = (size_bytes / (1024 ** 3)) / duration_hours if duration_hours > 0 else 0

    return VideoFile(
        path=str(path),
        name=path.name,
        size_bytes=size_bytes,
        duration_seconds=duration,
        video_codec=video_stream.get("codec_name", "unknown"),
        bitrate_gb_per_hour=bitrate,
        streams=streams,
    )


async def scan_folders(
    folders: list[str],
    threshold: float,
    max_depth: int,
) -> list[VideoFile]:
    results: list[VideoFile] = []

    for folder_str in folders:
        folder = Path(folder_str)
        if not folder.is_dir():
            log.warning("Folder not found: %s", folder)
            continue

        base_depth = len(folder.parts)

        for dirpath, dirnames, filenames in os.walk(folder):
            current_depth = len(Path(dirpath).parts) - base_depth
            if current_depth >= max_depth:
                dirnames.clear()

            for fname in filenames:
                fpath = Path(dirpath) / fname
                if fpath.suffix.lower() not in VIDEO_EXTENSIONS:
                    continue
                if "._remux_tmp" in fpath.name:
                    continue

                vf = await probe_file(fpath)
                if vf is None:
                    continue

                if vf.bitrate_gb_per_hour > threshold:
                    results.append(vf)

    results.sort(key=lambda f: f.bitrate_gb_per_hour, reverse=True)
    return results
