"""Conservative pattern scan of publishable sources; review findings manually."""
from pathlib import Path
import re
import socket
import sys

ROOT = Path(__file__).resolve().parents[1]
patterns = {
    "personal_absolute_path": re.compile(r"/(?:home|Users)/[^/\s]+/"),
    "private_ipv4": re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"),
    "internal_dns": re.compile(r"\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:lan|local|internal|ts\.net)\b", re.I),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "common_secret_prefix": re.compile(r"\b(?:ghp_|github_pat_|AKIA[0-9A-Z]{16}|sk-[A-Za-z0-9]{20,})"),
}
local_hostname = socket.gethostname()
if len(local_hostname) >= 4:
    patterns["current_hostname"] = re.compile(r"(?<![A-Za-z0-9])" + re.escape(local_hostname) + r"(?![A-Za-z0-9])", re.I)
findings = []
for path in ROOT.rglob("*"):
    if path == Path(__file__).resolve():  # Detector patterns necessarily appear in its own source.
        continue
    if not path.is_file() or any(part in {".git", "__pycache__", ".venv"} for part in path.parts):
        continue
    if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".pdf", ".zip", ".sqlite", ".db"}:
        continue
    try:
        data = path.read_text(encoding="utf-8")
    except UnicodeError:
        continue
    for label, regex in patterns.items():
        matches = list(regex.finditer(data))
        # This fixed mount is the public, synthetic Docker account, not a host user.
        # Review every match so an allowed mount cannot hide another personal path.
        if label == "personal_absolute_path":
            matches = [match for match in matches if not data[match.start():].startswith(
                "/home/emulator/.aws/emulator")]
        if matches:
            findings.append((path.relative_to(ROOT), label))
for path, category in findings:
    print(f"REVIEW {path}: {category}")
print(f"Pattern findings: {len(findings)} (does not cover unknown aliases or binary metadata)")
sys.exit(bool(findings))
