#!/usr/bin/env python3
"""Synthetic PCM and Ogg output test for the VPS localhost gpt-live bridge."""
import asyncio
import aiohttp
import time
import os
from pathlib import Path

PATH=os.getenv("ROOM_GPT_TEST_AUDIO_FILE", "/tmp/room_gpt_live_synthetic_question_20261108.pcm")
URL=os.getenv("ROOM_GPT_TEST_URL", "http://127.0.0.1:18771/ws")

async def test():
    raw=Path(PATH).read_bytes()
    if not 60000<len(raw)<1100000:
        raise RuntimeError("synthetic sample missing")
    async with aiohttp.ClientSession(trust_env=False,timeout=aiohttp.ClientTimeout(total=42)) as client:
      async with client.ws_connect(URL,heartbeat=20,timeout=30) as ws:
        t=time.monotonic()
        received=bytearray()
        counts={"binary":0,"text":0,"first":None,"finished":False,"ready":False}
        async def sender():
            await asyncio.sleep(1.0)
            for off in range(0,len(raw),640):
                await ws.send_bytes(raw[off:off+640])
                await asyncio.sleep(.02)
            print("SYNTH_INPUT_SENT",len(raw),"frames",(len(raw)+639)//640,flush=True)
        async def receiver():
            while time.monotonic()-t<float(os.getenv("ROOM_GPT_TEST_DURATION", "29")):
                try:m=await asyncio.wait_for(ws.receive(),timeout=1)
                except asyncio.TimeoutError:continue
                if m.type==aiohttp.WSMsgType.BINARY:
                    if counts["first"] is None:counts["first"]=round(time.monotonic()-t,2)
                    received.extend(m.data)
                    counts["binary"]+=1
                elif m.type==aiohttp.WSMsgType.TEXT:
                    counts["text"]+=1
                    if '"type":"ready"' in m.data:counts["ready"]=True
                    if '"type":"end"' in m.data:print("UPSTREAM_END_MARKER",flush=True);break
                else:print("UPSTREAM_DISCONNECTED",m.type.name,flush=True);break
        await asyncio.gather(sender(),receiver())
        print("RESULT",{"ready":counts["ready"],"binary_chunks":counts["binary"],"bytes":len(received),
                        "first_audio_sec":counts["first"],"ogg":received.startswith(b"OggS"),
                        "OpusHead":received.find(b"OpusHead"),"controls":counts["text"]},flush=True)

asyncio.run(test())
