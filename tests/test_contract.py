"""Offline checks for the Codex-only voice relay contract.

No microphone, tokens, internet requests, Home Assistant or station required.
"""
from __future__ import annotations
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"bridge"))

from opus_stream import StreamEncoder

class OggOpusContract(unittest.TestCase):
    def test_streaming_frames_and_headers(self):
        enc=StreamEncoder()
        # 960 samples / 20ms at 48k mono signed PCM, two frames.
        try:
            data=enc.push(bytes(960*2*2))
            data+=enc.finish()
        finally:
            enc.close()
        self.assertIn(b"OggS",data)
        self.assertIn(b"OpusHead",data)
        self.assertIn(b"OpusTags",data)
        self.assertGreater(len(data),80)

    def test_model_is_codex_live_not_an_api_key_provider(self):
        # Import-free code scan avoids connecting to external API at test time.
        code=(ROOT/"bridge"/"app.py").read_text(encoding="utf-8")
        self.assertIn('MODEL = "gpt-live-1-codex"',code)
        self.assertIn('CODEX_PROXY_CALL_URL',code)
        self.assertNotIn('OPENAI_API_KEY',code)
        self.assertNotIn('api.openai.com',code)

    def test_station_selection_is_opt_in(self):
        code=(ROOT/"station"/"main.go").read_text(encoding="utf-8")
        self.assertIn('case "codex", "gpt live"',code)
        self.assertIn('return "quasar", true',code)
        self.assertIn('runSession(ctx, client, nativeVAD)',code)


class RepositoryPrivacyChecks(unittest.TestCase):
    def test_auditor_recognizes_actual_private_network_and_secrets(self):
        import importlib.util
        source = ROOT / "tools" / "check_repository_safety.py"
        spec = importlib.util.spec_from_file_location("repository_safety", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        examples = [
            "192." + "168.0.19",
            "100." + "68.233.33",
            "7c:" + "11:22:33:44:55",
            "private." + "duckdns.org",
            "gho_" + "a" * 35,
        ]
        for value in examples:
            with self.subTest(category=value[:3]):
                self.assertTrue(module.check_text(value, "example_source.py"))
        self.assertFalse(module.check_text("127.0.0.1", "example_source.py"))

if __name__=="__main__":
    unittest.main()
