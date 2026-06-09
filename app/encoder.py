import asyncio
import logging
import shutil
from pathlib import Path
from typing import AsyncGenerator, Optional

from .scanner import VideoFile, probe_file

log = logging.getLogger(__name__)

# Active ffmpeg process — kept module-level so cancel can reach it
_active_proc: Optional[asyncio.subprocess.Process] = None


async def detect_nvenc() -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-encoders",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        return b"hevc_nvenc" in stdout
    except Exception:
        return False


async def pick_encoder(preference: str) -> tuple[str, list[str]]:
    if preference == "nvidia":
        return "hevc_nvenc", ["-rc", "vbr", "-cq", "0"]
    if preference == "cpu":
        return "libx265", []

    # auto
    if await detect_nvenc():
        log.info("GPU encoder detected: hevc_nvenc")
        return "hevc_nvenc", ["-rc", "vbr", "-cq", "0"]
    log.info("No GPU encoder, falling back to libx265")
    return "libx265", []


def _tmp_path(source: Path) -> Path:
    return source.with_name(source.stem + "._remux_tmp" + source.suffix)


def _output_path(source: Path, naming: str) -> Path:
    if naming == "replace":
        return source
    elif naming == "suffix":
        return source.with_name(source.stem + ".remux" + source.suffix)
    elif naming == "subdir":
        out_dir = source.parent / "remux"
        out_dir.mkdir(exist_ok=True)
        return out_dir / source.name
    raise ValueError(f"Unknown naming: {naming}")


def _build_stream_args(streams: list[dict]) -> list[str]:
    """
    Build explicit per-stream codec args from probe data.
    - Video: will be set separately by the caller
    - Audio: copy all tracks
    - Subtitles: copy if MKV-safe (subrip/ass/ssa/hdmv_pgs), else convert to srt, drop dvb_teletext
    - Data/attachments: copy
    MKV-safe subtitle codecs that copy cleanly:
      subrip (srt), ass, ssa, hdmv_pgs_subtitle, dvd_subtitle, webvtt
    """
    SAFE_SUB_COPY = {"subrip", "ass", "ssa", "hdmv_pgs_subtitle", "dvd_subtitle", "webvtt", "mov_text"}
    DROP_SUB = {"dvb_teletext", "dvb_subtitle"}

    args = ["-map", "0:v:0"]   # first video stream only

    # audio: map all, copy
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    for i in range(len(audio_streams)):
        args += [f"-map", f"0:a:{i}"]
    if audio_streams:
        args += ["-c:a", "copy"]

    # subtitles: map selectively
    sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
    mapped_subs = 0
    sub_codec_args = []
    for i, s in enumerate(sub_streams):
        codec = s.get("codec_name", "").lower()
        if codec in DROP_SUB:
            log.debug("Dropping subtitle stream %d (%s)", i, codec)
            continue
        args += ["-map", f"0:s:{i}"]
        if codec in SAFE_SUB_COPY:
            sub_codec_args += [f"-c:s:{mapped_subs}", "copy"]
        else:
            log.debug("Converting subtitle stream %d (%s) → srt", i, codec)
            sub_codec_args += [f"-c:s:{mapped_subs}", "srt"]
        mapped_subs += 1

    args += sub_codec_args

    # attachments / data
    data_streams = [s for s in streams if s.get("codec_type") in ("data", "attachment")]
    for i in range(len(data_streams)):
        args += ["-map", f"0:d:{i}"]
    if data_streams:
        args += ["-c:d", "copy"]

    return args


async def encode_file(
    vf: VideoFile,
    encoder: str,
    encoder_extra: list[str],
    crf: int,
    naming: str,
) -> AsyncGenerator[dict, None]:
    global _active_proc

    source = Path(vf.path)
    tmp = _tmp_path(source)
    final = _output_path(source, naming)

    stream_args = _build_stream_args(vf.streams)

    cmd = [
        "ffmpeg", "-hide_banner", "-y",
        "-i", str(source),
    ]

    cmd += stream_args
    cmd += ["-c:v", encoder]

    if encoder == "hevc_nvenc":
        cmd += encoder_extra + ["-cq", str(crf)]
    else:
        cmd += ["-crf", str(crf), "-preset", "medium"]

    cmd += [
        "-progress", "pipe:1",
        "-nostats",
        str(tmp),
    ]

    log.info("Encoding: %s  encoder=%s  crf=%d", source.name, encoder, crf)
    log.debug("Command: %s", " ".join(cmd))

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _active_proc = proc
    except FileNotFoundError:
        yield {"type": "error", "path": vf.path, "message": "ffmpeg not found"}
        return

    duration = vf.duration_seconds
    out_time_us = 0
    stderr_lines: list[str] = []

    # Drain stderr concurrently so it never blocks stdout
    async def drain_stderr():
        async for raw in proc.stderr:
            line = raw.decode("utf-8", errors="ignore").rstrip()
            stderr_lines.append(line)

    stderr_task = asyncio.create_task(drain_stderr())

    # Read ffmpeg -progress pipe:1 output (key=value lines)
    try:
        async for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if "=" not in line:
                continue
            key, _, val = line.partition("=")

            if key == "out_time_us":
                try:
                    out_time_us = int(val)
                except ValueError:
                    pass
                percent = min(int(out_time_us / 1_000_000 / duration * 100), 99) if duration else 0
                yield {
                    "type": "progress",
                    "path": vf.path,
                    "name": vf.name,
                    "percent": percent,
                }

            elif key == "progress" and val == "end":
                break

    except asyncio.CancelledError:
        proc.terminate()
        await proc.wait()
        await stderr_task
        tmp.unlink(missing_ok=True)
        yield {"type": "error", "path": vf.path, "message": "cancelled"}
        return

    await proc.wait()
    await stderr_task
    _active_proc = None

    if proc.returncode not in (0, None):
        tmp.unlink(missing_ok=True)
        # Surface the last meaningful ffmpeg error line
        error_detail = next(
            (l for l in reversed(stderr_lines) if l.strip() and not l.startswith("frame=")),
            f"ffmpeg exit {proc.returncode}"
        )
        log.error("ffmpeg failed for %s:\n%s", source.name, "\n".join(stderr_lines[-20:]))
        yield {"type": "error", "path": vf.path, "message": error_detail}
        return

    # Verify output
    verified = await probe_file(tmp)
    if verified is None or verified.duration_seconds < 60:
        tmp.unlink(missing_ok=True)
        yield {"type": "error", "path": vf.path, "message": "output verification failed"}
        return

    # Commit
    new_size = tmp.stat().st_size
    saved_gb = round((vf.size_bytes - new_size) / (1024 ** 3), 2)
    ratio = round((1 - new_size / vf.size_bytes) * 100, 1) if vf.size_bytes else 0

    if naming == "replace":
        backup = source.with_suffix(".orig_bak" + source.suffix)
        source.rename(backup)
        tmp.rename(final)
        backup.unlink(missing_ok=True)
    else:
        tmp.rename(final)

    log.info("Done: %s  saved=%.2f GB  ratio=%.1f%%", source.name, saved_gb, ratio)

    yield {
        "type": "file_done",
        "path": vf.path,
        "name": vf.name,
        "saved_gb": saved_gb,
        "ratio": ratio,
        "percent": 100,
    }


async def cancel_encode():
    global _active_proc
    if _active_proc and _active_proc.returncode is None:
        _active_proc.terminate()
        try:
            await asyncio.wait_for(_active_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            _active_proc.kill()
        _active_proc = None
        log.info("Encode cancelled.")
