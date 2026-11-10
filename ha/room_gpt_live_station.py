"""HA room-only WS relay to Codex OAuth GPT-Live through VPS nginx.

Never reads OAuth secrets, sends Basic auth only over certificate-verified WSS.
Does not change the Gemini pipeline, Home Assistant default flow or P610.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress

import aiohttp
import httpx
from loguru import logger
from starlette.websockets import WebSocket, WebSocketDisconnect

SELECTOR = "input_select.komnata_assistent"
USER = os.getenv("ROOM_GPT_LIVE_BASIC_USER", "codex-voice")
MAX_AUDIO = 65536


async def is_selected(*, supervisor: str, token: str) -> bool:
    """Fail closed on Supervisor errors: Gemini is the default."""
    if not token:
        return False
    url = supervisor.rstrip("/") + "/core/api/states/" + SELECTOR
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=1.5) as client:
            result = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            result.raise_for_status()
            data = result.json()
            return isinstance(data, dict) and str(data.get("state", "")).strip().casefold() == "gpt live"
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Room GPT Live mode read failed {}; default Gemini", type(exc).__name__)
        return False


async def relay_station(
    station: WebSocket,
    *,
    url: str,
    password: str,
    timeout_seconds: float = 14,
) -> bool:
    """Return False if upstream never became ready; caller may run Gemini fallback.

    On established connection, transfer binary PCM uplink and Ogg/Opus downlink
    unchanged. Only allow simple control tokens (no text/audio transcripts).
    """
    if not password or not url.startswith("wss://") or not url.endswith("/room-gpt-live/ws"):
        logger.error("Room GPT Live upstream not configured; keeping Gemini fallback")
        return False

    headers = {"Cache-Control": "no-store"}
    auth = aiohttp.BasicAuth(USER, password)
    client_timeout = aiohttp.ClientTimeout(total=None, sock_connect=6)
    try:
        session = aiohttp.ClientSession(timeout=client_timeout, trust_env=False)
        try:
            upstream = await session.ws_connect(
                url, auth=auth, heartbeat=20, receive_timeout=None,
                timeout=aiohttp.ClientWSTimeout(ws_receive=None),
                max_msg_size=MAX_AUDIO,
                headers=headers,
            )
        except Exception:
            await session.close()
            raise
    except Exception as exc:
        logger.warning("Room GPT Live WSS refused {}; Gemini fallback", type(exc).__name__)
        return False

    async with session, upstream:
        # GPT backend sends ready only after CLIProxyAPI+WebRTC session authenticated.
        ready = False
        first_packets: list[bytes] = []
        try:
            async with asyncio.timeout(timeout_seconds):
                while not ready:
                    response = await upstream.receive()
                    if response.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(response.data)
                        except (ValueError, TypeError):
                            data = {}
                        if data.get("type") == "ready" and data.get("provider") == "gpt-live":
                            ready = True
                            break
                        if data.get("type") == "end":
                            break
                    elif response.type == aiohttp.WSMsgType.BINARY:
                        first_packets.append(response.data)
                    else:
                        break
        except (TimeoutError, aiohttp.ClientError) as exc:
            logger.warning("Room GPT Live upstream readiness failed {}; Gemini fallback", type(exc).__name__)
        if not ready:
            return False

        logger.info("Room GPT Live verified upstream ready; activating audio relay")
        for packet in first_packets:
            await station.send_bytes(packet)

        async def uplink():
            packets = 0
            while True:
                try:
                    msg = await station.receive()
                except (WebSocketDisconnect, RuntimeError):
                    return
                if msg.get("type") == "websocket.disconnect":
                    return
                data = msg.get("bytes")
                if isinstance(data, bytes) and 0 < len(data) <= MAX_AUDIO:
                    await upstream.send_bytes(data)
                    packets += 1
                elif msg.get("text") == '{"type":"end"}':
                    await upstream.send_str('{"type":"end"}')
                    return

        async def downlink():
            while True:
                event = await upstream.receive()
                if event.type == aiohttp.WSMsgType.BINARY:
                    await station.send_bytes(event.data)
                elif event.type == aiohttp.WSMsgType.TEXT:
                    try:
                        message = json.loads(event.data)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(message, dict) and message.get("type") in ("interrupt", "end"):
                        await station.send_text(json.dumps({"type": message["type"]}))
                        if message["type"] == "end":
                            return
                else:
                    return

        up = asyncio.create_task(uplink(), name="room-gpt-live-uplink")
        down = asyncio.create_task(downlink(), name="room-gpt-live-downlink")
        try:
            done, pending = await asyncio.wait([up, down], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                with suppress(asyncio.CancelledError):
                    task.result()
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        except Exception as exc:
            logger.warning("Room GPT Live relay stopped {}", type(exc).__name__)
        finally:
            with suppress(Exception):
                await upstream.close()
            logger.info("Room GPT Live relay finished")
        return True
