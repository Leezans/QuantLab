from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET_DIRS = [ROOT / "src" / "ui" / "pages", ROOT / "src" / "ui" / "views"]
FORBIDDEN_PATTERNS = [
    re.compile(r"^\s*from\s+cLab\b"),
    re.compile(r"^\s*import\s+cLab\b"),
]


def main() -> int:
    violations: list[str] = []
    for target in TARGET_DIRS:
        for path in target.rglob("*.py"):
            lines = path.read_text(encoding="utf-8").splitlines()
            for line_no, line in enumerate(lines, start=1):
                if any(pattern.search(line) for pattern in FORBIDDEN_PATTERNS):
                    violations.append(f"{path}:{line_no}: {line.strip()}")

    if violations:
        print("Architecture check failed. UI page/view cannot import cLab directly.")
        for item in violations:
            print(f"  - {item}")
        return 1

    print("Architecture check passed. No cLab import found in ui/pages or ui/views.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

