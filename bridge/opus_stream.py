"""VPS Ogg/Opus streaming for the rooted Yandex Lite speech path."""
import os
import ctypes
import ctypes.util
import struct

_OPUS_APPLICATION_VOIP = 2048
_OPUS_FRAME_SAMPLES = 960
_OPUS_FRAME_BYTES = _OPUS_FRAME_SAMPLES * 2
_OPUS_MAX_PACKET = 4000
_OPUS_PRE_SKIP = 312

def _ogg_crc(page: bytes | bytearray) -> int:
    crc = 0
    for index, value in enumerate(page):
        if 22 <= index < 26:
            value = 0
        crc ^= value << 24
        for _ in range(8):
            crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF if crc & 0x80000000 else (crc << 1) & 0xFFFFFFFF
    return crc


class _OggOpusStream:
    """Minimal low-latency Ogg/Opus packetizer for one assistant speech turn."""

    def __init__(self, *, pre_skip: int = _OPUS_PRE_SKIP) -> None:
        self._serial = int.from_bytes(os.urandom(4), "little")
        self._sequence = 0
        self._granule = 0
        self._pending_packet: bytes | None = None
        self._started = False
        self._pre_skip = max(0, min(65535, int(pre_skip)))

    def _page(self, packet: bytes, *, flags: int, granule: int) -> bytes:
        count = len(packet) // 255 + 1
        lacing = bytearray([255] * (count - 1) + [len(packet) % 255])
        page = bytearray(27 + count + len(packet))
        page[:4] = b"OggS"
        page[4] = 0
        page[5] = flags & 0xFF
        struct.pack_into("<Q", page, 6, max(0, int(granule)))
        struct.pack_into("<I", page, 14, self._serial)
        struct.pack_into("<I", page, 18, self._sequence)
        self._sequence += 1
        page[26] = count
        page[27 : 27 + count] = lacing
        page[27 + count :] = packet
        struct.pack_into("<I", page, 22, _ogg_crc(page))
        return bytes(page)

    def _headers(self) -> bytes:
        head = b"OpusHead" + bytes((1, 1)) + struct.pack("<H", self._pre_skip) + struct.pack("<I", 48000) + struct.pack("<h", 0) + bytes((0,))
        vendor = b"Yandex Lite GPT Live"
        tags = b"OpusTags" + struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", 0)
        self._started = True
        return self._page(head, flags=0x02, granule=0) + self._page(tags, flags=0, granule=0)

    def push_packet(self, packet: bytes) -> bytes:
        if not packet:
            return b""
        out = bytearray()
        if not self._started:
            out += self._headers()
        if self._pending_packet is not None:
            self._granule += _OPUS_FRAME_SAMPLES
            out += self._page(self._pending_packet, flags=0, granule=self._granule)
        self._pending_packet = bytes(packet)
        return bytes(out)

    def finish(self) -> bytes:
        if self._pending_packet is None:
            return b""
        out = bytearray()
        if not self._started:
            out += self._headers()
        self._granule += _OPUS_FRAME_SAMPLES
        out += self._page(self._pending_packet, flags=0x04, granule=self._granule)
        self._pending_packet = None
        return bytes(out)


class _OpusEncoder:
    """Small ctypes wrapper around the system libopus encoder."""

    def __init__(self) -> None:
        name = ctypes.util.find_library("opus") or "libopus.so.0"
        self._lib = ctypes.CDLL(name)
        self._lib.opus_encoder_create.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        self._lib.opus_encoder_create.restype = ctypes.c_void_p
        self._lib.opus_encode.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int16), ctypes.c_int, ctypes.POINTER(ctypes.c_ubyte), ctypes.c_int32]
        self._lib.opus_encode.restype = ctypes.c_int
        self._lib.opus_encoder_destroy.argtypes = [ctypes.c_void_p]
        self._lib.opus_encoder_destroy.restype = None
        error = ctypes.c_int(0)
        self._encoder = self._lib.opus_encoder_create(48000, 1, _OPUS_APPLICATION_VOIP, ctypes.byref(error))
        if not self._encoder or error.value != 0:
            raise RuntimeError(f"libopus encoder create failed: {error.value}")
        self._closed = False

    def encode(self, pcm: bytes) -> bytes:
        if self._closed or not self._encoder:
            raise RuntimeError("libopus encoder is closed")
        if len(pcm) != _OPUS_FRAME_BYTES:
            raise ValueError(f"expected {_OPUS_FRAME_BYTES} PCM bytes, got {len(pcm)}")
        samples = (ctypes.c_int16 * _OPUS_FRAME_SAMPLES).from_buffer_copy(pcm)
        output = (ctypes.c_ubyte * _OPUS_MAX_PACKET)()
        size = self._lib.opus_encode(self._encoder, samples, _OPUS_FRAME_SAMPLES, output, _OPUS_MAX_PACKET)
        if size < 0:
            raise RuntimeError(f"libopus encode failed: {size}")
        return bytes(output[:size])

    def close(self) -> None:
        if not self._closed and self._encoder:
            self._lib.opus_encoder_destroy(self._encoder)
            self._encoder = None
        self._closed = True



class StreamEncoder:
    def __init__(self):
        self.encoder = _OpusEncoder()
        self.ogg = _OggOpusStream()
        self.pending = bytearray()
        self.frames = 0
    def push(self, pcm: bytes) -> bytes:
        self.pending.extend(pcm)
        result = bytearray()
        while len(self.pending) >= _OPUS_FRAME_BYTES:
            chunk = bytes(self.pending[:_OPUS_FRAME_BYTES])
            del self.pending[:_OPUS_FRAME_BYTES]
            result.extend(self.ogg.push_packet(self.encoder.encode(chunk)))
            self.frames += 1
        return bytes(result)
    def finish(self) -> bytes:
        result = bytearray()
        if self.pending:
            padded = bytes(self.pending) + b"\0" * (_OPUS_FRAME_BYTES - len(self.pending))
            self.pending.clear()
            result.extend(self.ogg.push_packet(self.encoder.encode(padded)))
        result.extend(self.ogg.finish())
        self.encoder.close()
        return bytes(result)
    def close(self):
        self.encoder.close()
