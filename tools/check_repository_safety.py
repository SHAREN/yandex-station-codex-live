#!/usr/bin/env python3
"""Check every Git-tracked file for secrets and private network identifiers.

This is defense in depth, NOT a guarantee. Run it before publishing; a new
commit does not erase secrets already contained in earlier Git history.
"""
from __future__ import annotations

import ipaddress
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

PATTERNS = {
    "OpenAI API key": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
    "GitHub OAuth/access token": re.compile(r"\bgh[opusr]_[A-Za-z0-9_]{20,}"),
    "GitLab token": re.compile(r"\bglpat-[A-Za-z0-9_-]{18,}"),
    "Generic bearer token": re.compile(r"\bBearer\s+[A-Za-z0-9_-]{32,}", re.I),
    "Private key PEM": re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"),
    "SSH public key": re.compile(r"\bssh-(?:rsa|ed25519)\s+[A-Za-z0-9+/]{60,}"),
    "MAC address": re.compile(r"(?<![0-9a-f])(?:[0-9a-f]{2}:){5}[0-9a-f]{2}(?![0-9a-f])", re.I),
    "Home/VPS domain": re.compile(r"\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:duckdns\.org|no-ip\.org|dynu\.net|hopto\.org|ddns\.net)\b", re.I),
    "Tailscale hostname": re.compile(r"\b[a-z0-9-]+\.tail[a-f0-9]+\.ts\.net\b", re.I),
    "Tailscale IPv6 address": re.compile(r"\bfd7a:115c:a1e0:[0-9a-f:]+\b", re.I),
    "Likely private user path": re.compile(r"(?:/[Hh]ome/[^/\s]+/|[A-Za-z]:\\Users\\[^\\\s]+\\)"),
    "Hardcoded auth secret assignment": re.compile(r"(?im)^\s*(?:password|api_key|access_token|client_secret)\s*=\s*['\"][^'\"]{10,}['\"]\s*$"),
}
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
BAD_SUFFIX = {".pcm", ".ogg", ".wav", ".opus", ".mp3", ".apk", ".bin", ".pem", ".key", ".p12", ".pfx"}
MAX_BYTES = 512_000
MAX_FINDINGS = 100


def tracked_files() -> list[pathlib.Path]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [ROOT / raw.decode("utf-8", "replace") for raw in output.split(b"\0") if raw]


def check_text(text: str, path: str) -> list[str]:
    findings = []
    for name, regex in PATTERNS.items():
        # The scanner naturally contains the signatures being checked.
        # Skip its own regex declarations, but do scan all other source lines.
        if path == "tools/check_repository_safety.py":
            lines = text.splitlines()
            relevant = "\n".join(line for line in lines if not line.lstrip().startswith(
                ('"OpenAI API key":', '"GitHub OAuth/access token":', '"GitLab token":',
                 '"Generic bearer token":', '"Private key PEM":', '"SSH public key":',
                 '"MAC address":', '"Home/VPS domain":', '"Tailscale hostname":',
                 '"Tailscale IPv6 address":', '"Likely private user path":',
                 '"Hardcoded auth secret assignment":', "IPV4 =")
            ))
        else:
            relevant = text
        if regex.search(relevant):
            findings.append(f"{name} in {path}")
    for match in IPV4.finditer(text):
        try:
            ip = ipaddress.ip_address(match.group())
        except ValueError:
            continue
        # 127.0.0.1 and 0.0.0.0 are standard portable example binds.
        # All other numeric IPs, including LAN, Tailscale and public VPS,
        # should be supplied through env and not hard-coded into source.
        if str(ip) not in {"127.0.0.1", "0.0.0.0"}:
            findings.append(f"Hardcoded non-loopback IPv4 address in {path}")
            break
    return findings


def scan() -> list[str]:
    findings: list[str] = []
    for path in tracked_files():
        relative = path.relative_to(ROOT).as_posix()
        if not path.is_file():
            continue
        if path.suffix.lower() in BAD_SUFFIX:
            findings.append(f"Binary/audio/key file accidentally tracked: {relative}")
        blob = path.read_bytes()
        if len(blob) > MAX_BYTES:
            findings.append(f"Review large tracked file {relative}: {len(blob)} bytes")
        try:
            decoded = blob.decode("utf-8")
        except UnicodeDecodeError:
            findings.append(f"Non-UTF8 tracked file: {relative}")
            continue
        findings.extend(check_text(decoded, relative))
        if len(findings) >= MAX_FINDINGS:
            return findings
    return findings


def main() -> int:
    findings = scan()
    for finding in findings:
        print("ERROR:", finding, file=sys.stderr)
    if findings:
        return 1
    print(f"PASS: {len(tracked_files())} tracked files scanned; no known private network or secret patterns")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
