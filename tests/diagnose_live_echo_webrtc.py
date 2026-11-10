#!/usr/bin/env python3
"""Compare GPT-Live with same synthetic replay of its own speech (no Station).

Bridge tracks remain untouched. Ogg/Opus from model is decoded in-memory and
played back at ~40% of model output amplitude into the virtual station mic.
"""
from __future__ import annotations
import asyncio
import audioop
import json
import subprocess
import time
from pathlib import Path
from aiohttp import ClientSession, ClientTimeout, WSMsgType

WS="ws://127.0.0.1:18771/ws"
PCM=Path("/tmp/room_gpt_live_synthetic_question_20261108.pcm")
GAIN=32.0
ECHO_LEVEL=0.4

def ogg_eos(data:bytes):
    pos=0
    while pos+27<=len(data):
        if data[pos:pos+4]!=b"OggS":return False
        h=data[pos+26]
        if pos+27+h>len(data):return False
        page_end=pos+27+h+sum(data[pos+27:pos+27+h])
        if page_end>len(data):return False
        if data[pos+5]&4:return True
        pos=page_end
    return False

def decode_to_16k(ogg:bytes):
    cp=subprocess.run(["ffmpeg","-nostdin","-hide_banner","-loglevel","error",
      "-f","ogg","-i","pipe:0","-ac","1","-ar","16000","-f","s16le","pipe:1"],
      input=ogg,capture_output=True,timeout=8,check=False)
    if cp.returncode!=0 or len(cp.stdout)<3200:
        raise RuntimeError("Opus decode failed "+str(cp.stderr[:300]))
    return cp.stdout

async def run():
    pcm=PCM.read_bytes()
    assert len(pcm)>90000 and audioop.rms(pcm,2)>300
    result={"model":"gpt-live-1-codex","upstream":"WebRTC","echo_fraction":ECHO_LEVEL}
    original=bytearray()
    followup=bytearray()
    ready=asyncio.Event()
    first_audio_ready=asyncio.Event()
    phase="original"
    async with ClientSession(timeout=ClientTimeout(total=65),trust_env=False) as session:
        async with session.ws_connect(WS,heartbeat=14,max_msg_size=256*1024) as ws:
            async def receive():
                nonlocal phase
                deadline=time.monotonic()+50
                while time.monotonic()<deadline:
                    try:msg=await asyncio.wait_for(ws.receive(),timeout=1)
                    except asyncio.TimeoutError:continue
                    if msg.type==WSMsgType.TEXT:
                        obj=json.loads(msg.data)
                        if obj.get("type")=="ready":
                            result["ready_provider"]=obj.get("provider")
                            ready.set()
                        elif obj.get("type")=="interrupt":
                            result["interrupts"]=result.get("interrupts",0)+1
                        elif obj.get("type")=="end":
                            result["ended"]=True
                            break
                    elif msg.type==WSMsgType.BINARY:
                        if phase=="original":
                            original.extend(msg.data)
                            if len(original)>=9000:first_audio_ready.set()
                        else:followup.extend(msg.data)
                    else:break
            rx=asyncio.create_task(receive())
            async def send_bytes(pcm):
                for pos in range(0,len(pcm),640):
                    await ws.send_bytes(pcm[pos:pos+640].ljust(640,b"\0"))
                    await asyncio.sleep(.02)
            try:
                await asyncio.wait_for(ready.wait(),timeout=18)
                await send_bytes(pcm)
                await asyncio.wait_for(first_audio_ready.wait(),timeout=25)
                decoded=decode_to_16k(bytes(original))
                # VPS bridge will multiply this by gain 32 before upstream RTP.
                echo=audioop.mul(decoded,2,ECHO_LEVEL/GAIN)
                result["model_audio_ogg_bytes"]=len(original)
                result["replayed_model_audio_pcm_16k_bytes"]=len(echo)
                result["model_audio_rms"]=audioop.rms(decoded,2)
                result["echo_raw_rms"]=audioop.rms(echo,2)
                phase="echo"
                await send_bytes(echo)
                await send_bytes(bytes(16000))
                await asyncio.sleep(5)
                result["second_output_ogg_bytes"]=len(followup)
            finally:
                rx.cancel()
                try:await rx
                except asyncio.CancelledError:pass
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))

if __name__=="__main__":
    asyncio.run(run())
