#!/usr/bin/env python3
"""VPS endpoint: Station PCM16/16k <-> ChatGPT Codex OAuth GPT-Live WebRTC.
OAuth is never read by this service: installed CLIProxyAPI creates the call.
Only localhost nginx may access this HTTP listener; the public nginx location
uses Basic authentication and limits the single websocket path.
"""
from __future__ import annotations

import asyncio
import audioop
import contextlib
import json
import logging
import os
import time
from collections import deque
from fractions import Fraction

import aiohttp
import av
from aiohttp import web
from aiortc import (
    RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription, MediaStreamTrack,
)
from opus_stream import StreamEncoder
from latency_trace import AudioLatencyTrace

HOST = "127.0.0.1"
PORT = int(os.environ.get("ROOM_GPT_LIVE_PORT", "18771"))
CALL_URL = os.getenv("CODEX_PROXY_CALL_URL", "http://127.0.0.1:8317/v1/realtime/calls")
MODEL = "gpt-live-1-codex"
VOICE = "cove"
DELEGATION_MODE = os.getenv("ROOM_GPT_DELEGATION_MODE", "client").strip().lower()
BACKEND_MODEL = "gpt-6.1-terra"
TERRA_CLIENT_ENABLED = os.getenv("ROOM_GPT_TERRA_CLIENT", "0") == "1"
BACKEND_URL = os.getenv("CODEX_PROXY_RESPONSES_URL", "http://127.0.0.1:8317/v1/responses")
if DELEGATION_MODE not in ("client", "responses"):
    raise RuntimeError("ROOM_GPT_DELEGATION_MODE must be client or responses")
MAX_RECEIVED_PCM = 65536
MAX_BUFFER = 192000  # 2 seconds after first voiced RTP frame, for low-latency ongoing turns
MAX_BUFFER_STARTUP = 960000  # retain initial spoken phrase through variable RTC startup
PCM_FRAME_BYTES = 1920  # 20 ms at 48kHz, mono, signed 16-bit
SILENCE_TRIM_TARGET_FRAMES = 16  # 320 ms, including speech safety padding
SILENCE_PREROLL_FRAMES = 4  # never trim the final 80 ms before speech
SILENCE_RMS_THRESHOLD = 12  # ONLY near-zero/gated frames, not quiet consonants
# Station vqe_0 PCM16 is extremely quiet on the rooted Lite 1 (observed peak 167).
# Apply the existing 32x native-station input amplification only at this GPT relay.
STATION_INPUT_GAIN = float(os.getenv("ROOM_GPT_INPUT_GAIN", "32"))
STATION_RAW_NOISE_GATE_RMS = int(os.getenv("ROOM_GPT_RAW_GATE_RMS", "3"))
SILENCE_FRAME = bytes(960 * 2)
LOG = logging.getLogger("room-gpt-live")


class StationPCMTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self, trace: AudioLatencyTrace | None = None) -> None:
        super().__init__()
        self.trace = trace
        self._rate_state = None
        self._buffer = bytearray()
        self._pending: deque[bytes] = deque()
        self._buffer_lock = asyncio.Lock()
        self.sent_frames = 0
        self.incoming_bytes = 0
        self.nonzero_frames = 0
        self.max_rms = 0
        self.output_bytes_local = 0
        self.input_nonzero_chunks = 0
        self.dropped_bytes = 0
        self.peak_buffer_bytes = 0
        self.skipped_silent_frames = 0
        self.silence_trim_events = 0
        self._next_frame_at = None
        self._ticks = 0
        self._trace_rx_ns = 0
        self._trace_tx_ns = 0
        self.first_voice_input_at = None
        self.last_voice_input_at = None
        self.first_voice_rtp_at = None
        self.last_voice_rtp_at = None

    async def push(self, pcm16: bytes) -> None:
        if not pcm16 or len(pcm16) % 2 or len(pcm16) > MAX_RECEIVED_PCM:
            return
        if self.trace is not None:
            self.trace.audio("mic", pcm16)
        # Stock VQE capture can be very quiet; the bridge applies configurable input gain.
        # Without it the real Station produces mostly silence to the GPT VAD.
        raw_rms = audioop.rms(pcm16, 2)
        gained = (
            audioop.mul(pcm16, 2, STATION_INPUT_GAIN)
            if raw_rms >= STATION_RAW_NOISE_GATE_RMS
            else bytes(len(pcm16))
        )
        pcm48, self._rate_state = audioop.ratecv(
            gained, 2, 1, 16000, 48000, self._rate_state
        )
        if raw_rms > 3:
            now = time.monotonic()
            self.input_nonzero_chunks += 1
            if self.first_voice_input_at is None:
                self.first_voice_input_at = now
            self.last_voice_input_at = now
        async with self._buffer_lock:
            self._buffer.extend(pcm48)
            self.peak_buffer_bytes = max(self.peak_buffer_bytes, len(self._buffer))
            self.incoming_bytes += len(pcm16)
            self._trim_stale_leading_silence()
            # While ICE/RTP sender warms up, protect the first spoken utterance.
            # After the first voiced RTP frame, cap latency aggressively.
            max_buffer = MAX_BUFFER if self.first_voice_rtp_at is not None else MAX_BUFFER_STARTUP
            if len(self._buffer) > max_buffer:
                # Realtime priority: keep recent speech rather than growing stale queues.
                excess = len(self._buffer) - max_buffer
                excess += excess % 2
                del self._buffer[:excess]
                self.dropped_bytes += excess
            # Diagnostic-only sampling: queue age, trimming and loss, no speech content.
            if self.trace is not None and self.trace.enabled:
                sample_ns = time.monotonic_ns()
                if sample_ns - self._trace_rx_ns >= 250_000_000:
                    self._trace_rx_ns = sample_ns
                    self.trace.event("input_queue_sample", {
                        "queued_ms": len(self._buffer) // 96,
                        "raw_rms": raw_rms,
                        "pcm16_bytes": len(pcm16),
                        "trimmed_silence_ms_total": self.skipped_silent_frames * 20,
                        "dropped_audio_ms_total": self.dropped_bytes // 96,
                    })

    def _trim_stale_leading_silence(self) -> None:
        """Remove only *gated-zero* frames at the head of a stale queue.

        Do not drop voiced data: audio captured during the initial WebRTC
        handshake may contain the user's first words. Keep 80 ms before any
        voiced frame and the latest 320 ms during prolonged silence.
        """
        frames = len(self._buffer) // PCM_FRAME_BYTES
        if frames <= SILENCE_TRIM_TARGET_FRAMES:
            return
        can_scan = frames - SILENCE_TRIM_TARGET_FRAMES
        quiet_frames = 0
        for n in range(can_scan):
            start = n * PCM_FRAME_BYTES
            end = start + PCM_FRAME_BYTES
            if audioop.rms(self._buffer[start:end], 2) > SILENCE_RMS_THRESHOLD:
                break
            quiet_frames += 1
        drop = max(0, quiet_frames - SILENCE_PREROLL_FRAMES)
        if drop:
            del self._buffer[:drop * PCM_FRAME_BYTES]
            self.skipped_silent_frames += drop
            self.silence_trim_events += 1

    async def recv(self) -> av.AudioFrame:
        # Pace from an absolute clock, NOT sleep(20ms) per recv. The old
        # implementation accumulated timer/scheduler overhead every frame,
        # making the outgoing RTP track slower than realtime and creating
        # multi-second stale audio backlogs on long Station calls.
        now = time.monotonic()
        if self._next_frame_at is None:
            self._next_frame_at = now
        delay = self._next_frame_at - now
        if delay > 0:
            await asyncio.sleep(delay)
        now = time.monotonic()
        self._next_frame_at = max(self._next_frame_at + 0.020, now - 0.040)
        async with self._buffer_lock:
            payload = bytes(self._buffer[:1920])
            del self._buffer[: len(payload)]
            queue_ms = len(self._buffer) // 96
        pad_ms = (PCM_FRAME_BYTES - len(payload)) // 96
        payload = payload.ljust(1920, b"\0")
        rms = audioop.rms(payload, 2)
        if self.trace is not None and self.trace.enabled:
            sample_ns = time.monotonic_ns()
            if sample_ns - self._trace_tx_ns >= 250_000_000:
                self._trace_tx_ns = sample_ns
                self.trace.event("rtp_queue_sample", {
                    "queued_ms": queue_ms,
                    "rms": rms,
                    "silence_pad_ms": pad_ms,
                    "sent_frames": self.sent_frames,
                })
        if rms > 100:
            self.nonzero_frames += 1
            if self.first_voice_rtp_at is None:
                self.first_voice_rtp_at = time.monotonic()
            self.last_voice_rtp_at = time.monotonic()
        self.max_rms = max(self.max_rms, rms)
        self.output_bytes_local += len(payload)
        frame = av.AudioFrame(format="s16", layout="mono", samples=960)
        frame.planes[0].update(payload)
        frame.pts = self._ticks
        frame.time_base = Fraction(1, 48000)
        frame.sample_rate = 48000
        self.sent_frames += 1
        self._ticks += 960
        return frame


class GptLiveSession:
    def __init__(self, socket: web.WebSocketResponse, label: str) -> None:
        self.socket = socket
        self.label = label
        # This VPS has a public IPv4 and previously negotiated a working
        # host/public candidate directly. Waiting for external Google STUN
        # adds ~5 seconds to aiortc ICE gathering on every new voice call.
        self.peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        self.trace = AudioLatencyTrace(label)
        self.in_track = StationPCMTrack(self.trace)
        self.peer.addTrack(self.in_track)
        self.dc = self.peer.createDataChannel("oai-events", ordered=True)
        self.audio_task: asyncio.Task | None = None
        self.finish_task: asyncio.Task | None = None
        self.out_encoder: StreamEncoder | None = None
        self.out_rate_converter = av.AudioResampler(format="s16", layout="mono", rate=48000)
        self.last_out_at = 0.0
        self.created_at = time.monotonic()
        self.first_output_at = None
        self._voice_event_log_counts = {}
        self.out_frames = 0
        self.out_bytes = 0
        self.control_messages = 0
        self.event_counts = {}
        # Session-only transcript fragments, bounded; never written to logs.
        self._user_transcripts = deque(maxlen=600)
        self._terra_tasks = set()
        self._delegation_ids = set()
        self._send_lock = asyncio.Lock()
        self._closed = False
        self._first_audio_logged = False
        self._first_ogg_headers_logged = False
        self._first_ogg_audio_logged = False
        self._first_input_logged = False
        self._setup_handlers()

    async def send(self, payload: bytes | str) -> None:
        if self.socket.closed or self._closed:
            return
        async with self._send_lock:
            try:
                if isinstance(payload, bytes):
                    if self.trace.enabled:
                        self.trace.event("station_ws_send_start", {"bytes": len(payload)})
                    await self.socket.send_bytes(payload)
                    self.out_bytes += len(payload)
                else:
                    await self.socket.send_str(payload)
            except (ConnectionError, RuntimeError):
                self._closed = True

    def _send_terra_context(self, delegation_id: str, kind: str, content: str) -> bool:
        """Return facts to Live, never impersonating an executed tool."""
        if self._closed or self.dc.readyState != "open":
            return False
        event = {
            "type": kind,
            "event_id": "terra_" + str(time.monotonic_ns()),
            "delegation_id": delegation_id,
            "content": content,
        }
        try:
            self.dc.send(json.dumps(event, ensure_ascii=False))
            return True
        except (RuntimeError, ValueError, OSError) as exc:
            LOG.warning("terra_context_send_failed session=%s kind=%s error=%s",
                        self.label, kind, type(exc).__name__)
            return False

    async def _run_terra_delegation(self, delegation_id: str, offset_ms: int) -> None:
        """Bounded, text-only backend. The Terra endpoint has no tools."""
        started = time.monotonic()
        # A silent update suppresses the automatic "I'll do it" acknowledgement,
        # leaving the actual response to arrive only after Terra returns.
        self._send_terra_context(delegation_id, "session.thinking.append", "")
        await asyncio.sleep(0.35)  # allow last transcription fragments to arrive
        recent = []
        for start_ms, end_ms, fragment in self._user_transcripts:
            if not offset_ms or start_ms <= offset_ms + 600:
                recent.append(fragment)
        query = "".join(recent).strip()[-6500:]
        if len(query) < 3:
            self._send_terra_context(
                delegation_id, "session.commentary.append",
                "Не удалось получить текст вопроса. Пожалуйста, повтори запрос."
            )
            LOG.warning("terra_delegation_no_transcript session=%s", self.label)
            return
        payload = {
            "model": BACKEND_MODEL,
            "input": query,
            "max_output_tokens": 700,
            "reasoning": {"effort": "medium"},
        }
        try:
            timeout = aiohttp.ClientTimeout(total=38, connect=6, sock_read=36)
            async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as client:
                async with client.post(BACKEND_URL, json=payload) as res:
                    if res.status != 200:
                        LOG.warning("terra_backend_http_error session=%s status=%d",
                                    self.label, res.status)
                        raise RuntimeError("Terra backend unavailable")
                    obj = await res.json()
            if obj.get("model") != BACKEND_MODEL:
                raise RuntimeError("unexpected Terra model")
            status = obj.get("status", "completed")
            if status not in ("completed", None):
                raise RuntimeError("incomplete Terra response")
            pieces = []
            for item in obj.get("output", []):
                if not isinstance(item, dict):
                    continue
                for part in item.get("content", []):
                    if isinstance(part, dict) and part.get("type") in ("output_text", "text"):
                        pieces.append(part.get("text", ""))
            answer = "".join(pieces).strip()
            if not answer:
                raise RuntimeError("empty Terra response")
            # The speech-facing message must fit the GPT-Live append limit.
            if len(answer) > 800:
                answer = answer[:795].rsplit(" ", 1)[0] + "…"
            delivered = self._send_terra_context(delegation_id, "session.commentary.append", answer)
            LOG.info("terra_backend_result session=%s success=%s transcript_chars=%d answer_chars=%d duration_ms=%d",
                     self.label, delivered, len(query), len(answer),
                     int((time.monotonic() - started) * 1000))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("terra_backend_failed session=%s error=%s duration_ms=%d",
                        self.label, type(exc).__name__,
                        int((time.monotonic() - started) * 1000))
            self._send_terra_context(
                delegation_id, "session.commentary.append",
                "Terra не смогла сейчас обработать вопрос. Попробуй ещё раз чуть позже."
            )

    def _setup_handlers(self):
        @self.dc.on("message")
        def event_received(message):
            if not isinstance(message, str):
                return
            try:
                data = json.loads(message)
            except (ValueError, TypeError):
                return
            kind = str(data.get("type") or "")[:80] if isinstance(data, dict) else ""
            self.control_messages += 1
            if self.trace.enabled and (
                kind in {
                    "input_transcript.added", "turn.created", "turn.delta", "turn.done",
                    "output_transcript.added", "session.started", "session.updated",
                    "error", "response.created", "response.done",
                }
                or kind.startswith(("input_audio_buffer.", "response.output_audio.",
                                    "response.audio.", "conversation.item.input_audio_transcription."))
            ):
                # Event type and queue depth only. No transcripts or model content.
                self.trace.event("model_control", {
                    "type": kind,
                    "rtp_queue_ms": len(self.in_track._buffer) // 96,
                })
            self.event_counts[kind] = self.event_counts.get(kind, 0) + 1
            if TERRA_CLIENT_ENABLED and kind in (
                "input_transcript.added", "session.input_transcript.delta"
            ):
                item = data.get("item") if isinstance(data, dict) else None
                fragment = (
                    item.get("text") if isinstance(item, dict)
                    else data.get("delta") if isinstance(data, dict) else None
                )
                if isinstance(fragment, str) and fragment:
                    self._user_transcripts.append((
                        int(data.get("start_ms") or 0),
                        int(data.get("end_ms") or 0),
                        fragment[:250],
                    ))
            if TERRA_CLIENT_ENABLED and kind == "session.delegation.created":
                item = data.get("delegation") if isinstance(data, dict) else None
                if isinstance(item, dict) and item.get("target") == "client":
                    delegation_id = item.get("id")
                    if isinstance(delegation_id, str) and delegation_id not in self._delegation_ids:
                        self._delegation_ids.add(delegation_id)
                        offset_ms = int(data.get("offset_ms") or 0)
                        task = asyncio.create_task(
                            self._run_terra_delegation(delegation_id, offset_ms),
                            name="terra-" + self.label,
                        )
                        self._terra_tasks.add(task)
                        task.add_done_callback(self._terra_tasks.discard)
                        LOG.info("terra_delegation_started session=%s input_parts=%d",
                                 self.label, len(self._user_transcripts))
            if os.getenv("ROOM_GPT_PROBE_DEBUG") == "1" and kind in (
                "session.started", "input_transcript.added", "session.input_transcript.delta",
                "session.delegation.created", "response.event", "error",
            ):
                # Debug is enabled ONLY for the separate synthetic test unit.
                LOG.info("probe_event session=%s kind=%s fields=%s payload=%s",
                         self.label, kind, list(data.keys())[:15],
                         json.dumps(data, ensure_ascii=False)[:1800])
            if kind in ("session.started", "session.updated"):
                cfg = data.get("session") if isinstance(data, dict) else None
                delegation = cfg.get("delegation") if isinstance(cfg, dict) else None
                if isinstance(delegation, dict):
                    responses = delegation.get("responses")
                    backend = responses.get("model") if isinstance(responses, dict) else None
                    LOG.info("live_session_config session=%s delegation=%s backend=%s",
                             self.label, str(delegation.get("type"))[:24], str(backend)[:55])
            if kind == "session.delegation.created":
                item = data.get("delegation") if isinstance(data, dict) else None
                LOG.info("live_delegation_created session=%s target=%s",
                         self.label, (item.get("target") if isinstance(item, dict) else None))
            if kind == "response.event":
                event = data.get("event") if isinstance(data, dict) else None
                name = str(event.get("type") or "")[:80] if isinstance(event, dict) else ""
                self.event_counts["response.event:" + name] = self.event_counts.get("response.event:" + name, 0) + 1
                if name in ("response.created", "response.completed", "response.failed"):
                    LOG.info("live_backend_event session=%s kind=%s", self.label, name)
            if kind in ("input_transcript.added", "turn.created", "turn.done", "output_transcript.added"):
                count = self._voice_event_log_counts.get(kind, 0)
                if count < 12:
                    now = time.monotonic()
                    last_voice = self.in_track.last_voice_rtp_at
                    LOG.info(
                        "voice_timing session=%s kind=%s count=%d session_age_ms=%d "
                        "since_last_voice_rtp_ms=%d queued_audio_ms=%d",
                        self.label, kind, count + 1, int((now-self.created_at)*1000),
                        int((now-last_voice)*1000) if last_voice else -1,
                        int(len(self.in_track._buffer)/96),
                    )
                self._voice_event_log_counts[kind] = count + 1
            if kind == "input_transcript.added" and self.out_encoder is not None:
                asyncio.create_task(self.interrupt())
            elif kind in {"turn.done", "response.done"}:
                self.schedule_finish()
            elif kind == "error":
                error = data.get("error") or {}
                code = str(error.get("code", "") if isinstance(error, dict) else "")[:70]
                LOG.warning("GPT-Live control error code=%s session=%s", code, self.label)

        @self.peer.on("track")
        def new_track(track):
            if track.kind == "audio" and self.audio_task is None:
                LOG.info("remote_audio_track session=%s", self.label)
                self.audio_task = asyncio.create_task(
                    self.stream_remote_audio(track), name=f"room-gpt-output-{self.label}"
                )

        @self.peer.on("connectionstatechange")
        async def state_change():
            LOG.info("webrtc_state session=%s state=%s", self.label, self.peer.connectionState)

    def schedule_finish(self):
        if self.out_encoder is None or (self.finish_task and not self.finish_task.done()):
            return

        async def _delayed_finish():
            try:
                while self.out_encoder is not None:
                    age = time.monotonic() - self.last_out_at
                    if age >= .55:
                        break
                    await asyncio.sleep(min(.25, .55 - age))
                await self.finish_turn()
            except asyncio.CancelledError:
                pass

        self.finish_task = asyncio.create_task(_delayed_finish())

    async def finish_turn(self):
        encoder = self.out_encoder
        self.out_encoder = None
        if encoder is not None:
            try:
                tail = encoder.finish()
                if tail:
                    await self.send(tail)
            finally:
                encoder.close()
        # One-shot diagnostics need only the first complete audible response.
        # Close/flushed trace immediately; the voice call itself keeps working.
        if self.trace.enabled and self.out_frames > 0:
            self.trace.event("first_response_complete")
            self.trace.close()

    async def interrupt(self):
        if self.out_encoder is not None:
            self.out_encoder.close()
            self.out_encoder = None
            if self.finish_task and not self.finish_task.done():
                self.finish_task.cancel()
            await self.send('{"type":"interrupt"}')
            LOG.info("gpt_live_barge_in session=%s", self.label)

    async def stream_remote_audio(self, track):
        LOG.info("audio_receive_started session=%s", self.label)
        try:
            while not self._closed:
                frame = await track.recv()
                for out in self.out_rate_converter.resample(frame):
                    pcm = out.to_ndarray().tobytes()
                    if not pcm or len(pcm) % 2:
                        continue
                    if self.trace.enabled:
                        self.trace.audio("gpt", pcm)
                    if self.out_encoder is None and audioop.rms(pcm, 2) < 80:
                        continue
                    if self.out_encoder is None:
                        self.out_encoder = StreamEncoder()
                        if not self._first_audio_logged:
                            self._first_audio_logged = True
                            self.first_output_at = time.monotonic()
                            if self.trace.enabled:
                                self.trace.event("gpt_first_audible_output")
                            LOG.info(
                                "first_audio_response session=%s age_ms=%d since_last_voice_rtp_ms=%d",
                                self.label, int((self.first_output_at-self.created_at)*1000),
                                int((self.first_output_at-self.in_track.last_voice_rtp_at)*1000)
                                if self.in_track.last_voice_rtp_at else -1,
                            )
                    self.last_out_at = time.monotonic()
                    data = self.out_encoder.push(pcm)
                    self.out_frames += 1
                    if data:
                        if self.trace.enabled and self.out_encoder.frames == 1 and not self._first_ogg_headers_logged:
                            self._first_ogg_headers_logged = True
                            self.trace.event("ogg_headers_ready")
                        first_audio_page = self.out_encoder.frames >= 2 and not self._first_ogg_audio_logged
                        if first_audio_page and self.trace.enabled:
                            self.trace.event("ogg_first_audio_page_ready")
                        await self.send(data)
                        if first_audio_page:
                            self._first_ogg_audio_logged = True
                            if self.trace.enabled:
                                self.trace.event("ogg_first_audio_page_ws_sent")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("outgoing_audio_finished session=%s error=%s", self.label, type(exc).__name__)
        finally:
            await self.finish_turn()

    async def start(self) -> None:
        start_at = time.monotonic()
        if self.trace.enabled:
            self.trace.event("webrtc_offer_start")
        offer = await self.peer.createOffer()
        offer_at = time.monotonic()
        if self.trace.enabled:
            self.trace.event("webrtc_offer_created")
        await asyncio.wait_for(self.peer.setLocalDescription(offer), timeout=14)
        gathered_at = time.monotonic()
        if self.trace.enabled:
            self.trace.event("webrtc_local_sdp_ready")
        LOG.info(
            "rtc_offer_timing session=%s create_ms=%d ice_gather_ms=%d total_ms=%d",
            self.label, int((offer_at-start_at)*1000),
            int((gathered_at-offer_at)*1000), int((gathered_at-start_at)*1000),
        )
        sdp = self.peer.localDescription.sdp
        if "a=ice-pwd:" not in sdp or "a=fingerprint:" not in sdp:
            raise RuntimeError("WebRTC SDP is incomplete")
        backend_delegation = (
            {"type": "responses",
             "responses": {
                 "model": BACKEND_MODEL,
                 "reasoning": {"effort": "medium"},
                 "tool_choice": "none",
             }}
            if DELEGATION_MODE == "responses"
            else {"type": "client"}
        )
        session = {
            "model": MODEL,
            "audio": {"output": {"voice": VOICE}},
            "delegation": backend_delegation,
            "instructions": (
                "Ты голосовой помощник в комнате. Отвечай естественно, по-русски, кратко. "
                "Не говори сам первым при тишине. На команды умного дома отвечай, "
                "что для действий доступна штатная Алиса."
                + (
                    " Делегирование: помощник Terra умеет рассуждать, объяснять, "
                    "анализировать вопросы и давать рекомендации, но не имеет доступа "
                    "к телефону, экрану и устройствам, не запускает код. "
                    "Generate <|SpawnThinking|> when просьба требует обстоятельного "
                    "анализа или многошагового решения. Для простых разговоров "
                    "отвечай сама, не делегируй."
                    if DELEGATION_MODE == "responses" else (
                        " Делегирование: Terra 6.1 может объяснять, рассуждать и "
                        "анализировать многошаговые вопросы. Настоящих инструментов "
                        "управления телефоном, экраном, браузером или VPS у неё нет. "
                        "Generate <|SpawnThinking|> when просьба требует "
                        "сложного анализа или решения. На простые вопросы "
                        "отвечай сама."
                        if TERRA_CLIENT_ENABLED else ""
                    )
                )
            ),
        }
        timeout = aiohttp.ClientTimeout(total=22, connect=7)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as client:
            if self.trace.enabled:
                self.trace.event("gpt_sdp_post_start")
            async with client.post(
                CALL_URL,
                json={"sdp": sdp, "session": session},
                headers={"OpenAI-Alpha": "quicksilver=v2"},
            ) as result:
                status = result.status
                raw = await result.text()
                if status not in (200, 201) or not raw.startswith("v=0"):
                    code = ""
                    try:
                        parsed = json.loads(raw)
                        error = parsed.get("error") or {}
                        code = str(error.get("code", ""))[:50] if isinstance(error, dict) else ""
                    except (ValueError, TypeError):
                        pass
                    raise RuntimeError(f"GPT Live call rejected status={status} code={code}")
                if self.trace.enabled:
                    self.trace.event("gpt_call_created", {"http": status})
                LOG.info(
                    "gpt_live_call_created session=%s http=%s post_ms=%d age_ms=%d",
                    self.label, status, int((time.monotonic()-gathered_at)*1000),
                    int((time.monotonic()-self.created_at)*1000),
                )
        await asyncio.wait_for(
            self.peer.setRemoteDescription(RTCSessionDescription(sdp=raw, type="answer")),
            timeout=10,
        )
        if self.trace.enabled:
            self.trace.event("webrtc_remote_sdp_applied")
        for i in range(56):
            if self.peer.connectionState == "connected" and self.dc.readyState == "open":
                if self.trace.enabled:
                    self.trace.event("webrtc_ready")
                LOG.info(
                    "gpt_live_webrtc_ready session=%s age_ms=%d",
                    self.label, int((time.monotonic()-self.created_at)*1000),
                )
                return
            if self.peer.connectionState in ("closed", "failed"):
                break
            await asyncio.sleep(.25)
        raise RuntimeError(f"GPT Live ICE/datachannel not ready peer={self.peer.connectionState}")

    async def close(self):
        self._closed = True
        for task in list(self._terra_tasks):
            task.cancel()
        if self._terra_tasks:
            await asyncio.gather(*self._terra_tasks, return_exceptions=True)
        if self.finish_task is not None and not self.finish_task.done():
            self.finish_task.cancel()
        if self.audio_task is not None and not self.audio_task.done():
            self.audio_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.audio_task
        if self.out_encoder is not None:
            self.out_encoder.close()
            self.out_encoder = None
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.peer.close(), timeout=5)
        if self.trace.enabled:
            self.trace.event("webrtc_closed")
        self.trace.close()
        LOG.info("gpt_live_event_counts label=%s types=%s", self.label, self.event_counts)
        LOG.info(
            "latency_summary session=%s first_voice_input_ms=%d first_voice_rtp_ms=%d "
            "first_output_ms=%d real_input_secs=%.2f sent_rtp_secs=%.2f stale_audio_ms=%d",
            self.label,
            int((self.in_track.first_voice_input_at-self.created_at)*1000)
            if self.in_track.first_voice_input_at else -1,
            int((self.in_track.first_voice_rtp_at-self.created_at)*1000)
            if self.in_track.first_voice_rtp_at else -1,
            int((self.first_output_at-self.created_at)*1000) if self.first_output_at else -1,
            self.in_track.incoming_bytes/32000, self.in_track.sent_frames/50,
            int(len(self.in_track._buffer)/96),
        )
        LOG.info(
            "audio_tx_stats label=%s nonzero_frames=%d max_rms=%d input_nonzero_chunks=%d "
            "peak_buffer_bytes=%d dropped_bytes=%d skipped_silent_ms=%d "
            "trim_events=%d remain=%d",
            self.label, self.in_track.nonzero_frames, self.in_track.max_rms,
            self.in_track.input_nonzero_chunks, self.in_track.peak_buffer_bytes,
            self.in_track.dropped_bytes, self.in_track.skipped_silent_frames * 20,
            self.in_track.silence_trim_events, len(self.in_track._buffer),
        )
        LOG.info(
            "session_closed label=%s input_bytes=%d input_frames=%d output_frames=%d output_bytes=%d events=%d",
            self.label, self.in_track.incoming_bytes, self.in_track.sent_frames,
            self.out_frames, self.out_bytes, self.control_messages,
        )


async def live_websocket(request: web.Request) -> web.WebSocketResponse:
    if request.remote not in ("127.0.0.1", "::1"):
        raise web.HTTPForbidden()
    ws = web.WebSocketResponse(heartbeat=18, receive_timeout=None, max_msg_size=MAX_RECEIVED_PCM)
    await ws.prepare(request)
    label = hex(int(time.monotonic() * 1000))[2:][-10:]
    session = GptLiveSession(ws, label)
    try:
        await asyncio.wait_for(session.start(), timeout=34)
        if session.trace.enabled:
            session.trace.event("station_relay_ready")
        await session.send('{"type":"ready","provider":"gpt-live"}')
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                if 0 < len(msg.data) <= MAX_RECEIVED_PCM:
                    await session.in_track.push(msg.data)
                    if not session._first_input_logged:
                        session._first_input_logged = True
                        if session.trace.enabled:
                            session.trace.event("station_first_pcm_received", {"bytes": len(msg.data)})
                        LOG.info("first_station_pcm session=%s bytes=%d", label, len(msg.data))
            elif msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == '{"type":"end"}':
                    break
            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                break
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        LOG.warning("gpt_live_session_failed label=%s error=%s message=%s", label, type(exc).__name__, str(exc)[:150])
        with contextlib.suppress(Exception):
            await session.send('{"type":"end"}')
    finally:
        await session.close()
        with contextlib.suppress(Exception):
            await ws.close()
    return ws


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    app = web.Application(client_max_size=256 * 1024)
    app.add_routes([
        web.get("/ws", live_websocket),
        web.get("/healthz", lambda _: web.json_response({"status": "ok", "model": MODEL})),
    ])
    web.run_app(app, host=HOST, port=PORT, access_log=None)


if __name__ == "__main__":
    main()
