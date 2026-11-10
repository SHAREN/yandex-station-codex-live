"""One-shot, explicitly armed, privacy-bounded audio latency flight recorder.

No microphone is opened: this passively observes only the existing Station ->
VPS PCM stream and already-decoded GPT voice during ONE intentionally armed call.
Never active unless a root/user-controlled local file is present. No public API.
Audio & timing data live in a private tmpfs directory and are not published.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import time
from typing import Optional

TRACE_ROOT = Path("/dev/shm/room-gpt-latency")
ARM_FILE = TRACE_ROOT / "arm.json"
MAX_SECONDS = 45
MAX_MIC_BYTES = MAX_SECONDS * 32000  # PCM16 mono 16000
MAX_GPT_BYTES = MAX_SECONDS * 96000   # PCM16 mono 48000
MAX_EVENT_BYTES = 512 * 1024  # no unbounded JSONL events, even for multi-hour calls


def _utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def _ensure_root() -> None:
    TRACE_ROOT.mkdir(parents=True, exist_ok=True)
    TRACE_ROOT.chmod(0o700)


class AudioLatencyTrace:
    """All recording operations are best-effort and cannot block a voice call."""

    def __init__(self, label: str) -> None:
        self.enabled = False
        self.start_ns = time.monotonic_ns()
        self.id = label
        self._mic_bytes = 0
        self._gpt_bytes = 0
        self._event_bytes = 0
        self._mic = None
        self._gpt = None
        self._events = None
        self._closed = False
        try:
            _ensure_root()
            if not ARM_FILE.is_file():
                return
            data = json.loads(ARM_FILE.read_text(encoding="utf-8"))
            expiry = float(data.get("expires_unix", 0))
            if not (time.time() < expiry <= time.time() + 180):
                ARM_FILE.unlink(missing_ok=True)
                return
            # Claim exactly one voice session, atomically. Future calls
            # cannot record until the user explicitly re-arms.
            claimed = TRACE_ROOT / f".arm-claimed-{os.getpid()}-{label}"
            try:
                ARM_FILE.rename(claimed)
            except FileNotFoundError:
                return
            with contextlib.suppress(OSError):
                claimed.unlink()
            session_path = TRACE_ROOT / f"session-{int(time.time())}-{label}"
            session_path.mkdir(mode=0o700)
            self.folder = session_path
            self._mic = (session_path / "station-mic.s16le").open("xb")
            self._gpt = (session_path / "gpt-voice.s16le").open("xb")
            self._events = (session_path / "trace.jsonl").open("x", encoding="utf-8")
            for file in (self._mic, self._gpt, self._events):
                with contextlib.suppress(OSError):
                    os.chmod(file.name, 0o600)
            self.enabled = True
            self.event("session_started", {
                "mic_format": "s16le/16000/mono",
                "gpt_format": "s16le/48000/mono",
                "max_recording_seconds": MAX_SECONDS,
                "session_id": label,
            })
        except Exception:
            self.close()

    def event(self, kind: str, fields: Optional[dict] = None) -> None:
        if not self.enabled or self._closed or self._events is None:
            return
        if self._event_bytes >= MAX_EVENT_BYTES:
            return
        try:
            row = {
                "t_ns": time.monotonic_ns(),
                "utc": _utc(),
                "event": kind,
                "fields": fields or {},
            }
            line = json.dumps(row, separators=(",", ":")) + "\n"
            if self._event_bytes + len(line.encode("utf-8")) > MAX_EVENT_BYTES:
                return
            self._events.write(line)
            self._event_bytes += len(line.encode("utf-8"))
            self._events.flush()
        except (OSError, ValueError):
            self.enabled = False

    def audio(self, source: str, pcm: bytes) -> None:
        if not self.enabled or self._closed or not pcm:
            return
        if time.monotonic_ns() - self.start_ns >= MAX_SECONDS * 1_000_000_000:
            self.close()
            return
        try:
            now_ns = time.monotonic_ns()
            if source == "mic":
                file = self._mic
                current = self._mic_bytes
                cap = MAX_MIC_BYTES
                event = "mic_chunk"
                sample_rate = 16000
            elif source == "gpt":
                file = self._gpt
                current = self._gpt_bytes
                cap = MAX_GPT_BYTES
                event = "gpt_chunk"
                sample_rate = 48000
            else:
                return
            if file is None or current >= cap or len(pcm) % 2:
                return
            n = min(len(pcm), cap - current)
            n -= n % 2
            if n <= 0:
                return
            file.write(pcm[:n])
            file.flush()  # Ensure a just-finished test can be collected while WS stays open
            sample_offset = current // 2
            if source == "mic":
                self._mic_bytes += n
            else:
                self._gpt_bytes += n
            if self._events is not None:
                row = {
                    "t_ns": now_ns,
                    "event": event,
                    "sample_offset": sample_offset,
                    "samples": n // 2,
                    "sample_rate": sample_rate,
                }
                line = json.dumps(row, separators=(",", ":")) + "\n"
                nbytes = len(line.encode("utf-8"))
                if self._event_bytes + nbytes <= MAX_EVENT_BYTES:
                    self._events.write(line)
                    self._event_bytes += nbytes
                    self._events.flush()
        except (OSError, ValueError):
            self.enabled = False

    def close(self) -> None:
        if self._closed:
            return
        if self.enabled:
            self.event("session_finished", {
                "mic_bytes": self._mic_bytes, "gpt_bytes": self._gpt_bytes,
                "duration_ms": (time.monotonic_ns() - self.start_ns) // 1_000_000,
            })
        self._closed = True
        self.enabled = False
        for file in (self._mic, self._gpt, self._events):
            if file is not None:
                with contextlib.suppress(OSError):
                    file.flush()
                    file.close()
