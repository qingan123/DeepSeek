#!/usr/bin/env python3
"""Create a clean deployment ZIP without runtime data, archives, or secrets."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
INCLUDE_DIRS = ("app", "scripts", "deployment", "hermes_patches", "docs")
INCLUDE_FILES = ("README.md", "HANDOFF.md", ".env.example", ".gitignore", "requirements.txt")
EXCLUDED_PARTS = {"__pycache__", ".pytest_cache", ".ruff_cache", ".venv", "venv", "dist", "archive"}
EXCLUDED_NAMES = {".env", "app.db", "dspow_native"}
TEXT_SUFFIXES = {".py", ".js", ".json", ".c", ".h", ".sh", ".ps1", ".bat", ".md", ".txt", ".example"}
SUSPICIOUS = (
    re.compile(r"(?i)Bearer\s+[A-Za-z0-9+/=_-]{24,}"),
    re.compile(r"(?im)^\s*(?:token|user_token)\s*=\s*['\"][A-Za-z0-9+/=_-]{24,}['\"]"),
)

def allowed(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if any(part in EXCLUDED_PARTS for part in relative.parts):
        return False
    if path.name in EXCLUDED_NAMES:
        return False
    if ".bak" in path.name or ".before-" in path.name or path.name.endswith(".candidate"):
        return False
    if path.suffix.lower() in {".db", ".sqlite", ".log", ".pyc"}:
        return False
    return path.is_file()

def collect() -> list[Path]:
    files: list[Path] = []
    for directory in INCLUDE_DIRS:
        base = ROOT / directory
        if base.exists():
            files.extend(path for path in base.rglob("*") if allowed(path))
    files.extend(ROOT / name for name in INCLUDE_FILES if (ROOT / name).is_file())
    return sorted(set(files), key=lambda path: path.as_posix())

def scan(files: list[Path]) -> None:
    findings: list[str] = []
    for path in files:
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name != ".env.example":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if any(pattern.search(text) for pattern in SUSPICIOUS):
            findings.append(str(path.relative_to(ROOT)))
    if findings:
        raise SystemExit("Refusing to package possible secrets in: " + ", ".join(findings))

def main() -> None:
    files = collect()
    scan(files)
    DIST.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    output = DIST / f"project_60089-release-{stamp}.zip"
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in files:
            archive.write(path, Path("project_60089") / path.relative_to(ROOT))
    print(f"created: {output}")
    print(f"files: {len(files)}")

if __name__ == "__main__":
    main()
