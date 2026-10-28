#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

"""Validate the GitHub Markdown documentation and its local links."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = chr(96) * 3
FENCE_LINE = re.compile(r"^(\x60{3,}|~{3,})(.*)$")
LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
HTML_LINK = re.compile(r"(?:href|src)=\"([^\"]+)\"")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
RST = re.compile(r"^\s*\.\.\s+[a-zA-Z0-9_-]+\s*:{1,2}(?:\s|$)|:[a-zA-Z][a-zA-Z0-9_-]*:`")


def exact_case_path(path: Path) -> bool:
    """Check path spelling explicitly because macOS filesystems may ignore case."""
    current = Path(path.anchor) if path.is_absolute() else Path()
    for part in path.parts[1:] if path.is_absolute() else path.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            current = current.parent
            continue
        if not current.exists() or not any(child.name == part for child in current.iterdir()):
            return False
        current = current / part
    return current.exists()


def github_slug(heading: str) -> str:
    heading = re.sub(r"<[^>]+>", "", heading)
    heading = heading.replace(chr(96), "")
    heading = re.sub(r"\s+", "-", heading.strip().lower())
    return re.sub(r"[^\w-]", "", heading)


def heading_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    counts: dict[str, int] = {}
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith(FENCE):
            fenced = not fenced
            continue
        if fenced:
            continue
        match = HEADING.match(line)
        if not match:
            continue
        base = github_slug(match.group(1))
        index = counts.get(base, 0)
        counts[base] = index + 1
        ids.add(base if index == 0 else f"{base}-{index}")
    return ids


def local_target(source: Path, url: str) -> tuple[Path | None, str | None]:
    url = url.strip().split()[0].strip("<>\"'")
    parsed = urlsplit(unquote(url))
    if parsed.scheme or parsed.netloc or url.startswith("//"):
        return None, None
    fragment = parsed.fragment or None
    raw_path = parsed.path
    if not raw_path:
        target = source
    else:
        target = (source.parent / raw_path).resolve()
    if target.is_dir() and (target / "README.md").exists():
        target = target / "README.md"
    elif not target.suffix:
        if target.with_suffix(".md").exists():
            target = target.with_suffix(".md")
        elif (target / "README.md").exists():
            target = target / "README.md"
    return target, fragment


def main() -> int:
    files = sorted({ROOT / "README.md", *DOCS.rglob("*.md")})
    errors: list[str] = []
    for path in files:
        rel = path.relative_to(ROOT)
        text = path.read_text(encoding="utf-8")
        fenced = False
        fence_char = ""
        fence_width = 0
        h1 = 0
        fence_language = None
        for line_number, line in enumerate(text.splitlines(), 1):
            quoted = line.lstrip().startswith(">")
            normalized = line.lstrip()
            while normalized.startswith(">"):
                normalized = normalized[1:]
                if normalized.startswith(" "):
                    normalized = normalized[1:]
            fence_match = FENCE_LINE.match(normalized.lstrip())
            if fence_match and not fenced:
                fence_char = fence_match.group(1)[0]
                fence_width = len(fence_match.group(1))
                fence_language = fence_match.group(2).strip().lower()
                if not fence_language:
                    errors.append(f"{rel}:{line_number}: opening code fence needs a language label")
                fenced = True
                code_lines: list[str] = []
                code_start = line_number + 1
                continue
            elif fence_match and fenced and fence_match.group(1)[0] == fence_char and len(fence_match.group(1)) >= fence_width:
                if fence_language == "python":
                    try:
                        ast.parse("\n".join(code_lines), filename=f"{rel}:{code_start}")
                    except SyntaxError as error:
                        errors.append(f"{rel}:{error.lineno or code_start}: invalid Python fence: {error.msg}")
                fenced = False
                fence_language = None
                fence_char = ""
                fence_width = 0
                continue
            if fenced:
                if fence_language == "python":
                    code_lines.append(normalized if quoted else line)
                continue
            heading = HEADING.match(line)
            if heading and line.startswith("# "):
                h1 += 1
            if re.match(r"^<h1(?:\s|>)", line.strip(), re.I):
                h1 += 1
            if RST.search(line):
                errors.append(f"{rel}:{line_number}: residual Sphinx/reStructuredText markup")
        if fenced:
            errors.append(f"{rel}: unclosed code fence")
        if h1 != 1:
            errors.append(f"{rel}: expected one H1, found {h1}")

        urls = [match.group(1) for match in LINK.finditer(text)]
        urls.extend(match.group(1) for match in HTML_LINK.finditer(text))
        for url in urls:
            target, fragment = local_target(path, url)
            if target is None:
                continue
            if not exact_case_path(target):
                errors.append(f"{rel}: broken or case-mismatched link: {url}")
                continue
            if fragment and target.suffix.lower() == ".md" and fragment not in heading_ids(target):
                errors.append(f"{rel}: missing heading anchor in {target.relative_to(ROOT)}: #{fragment}")

    rst_files = list(DOCS.rglob("*.rst"))
    if rst_files:
        errors.append(f"docs/: {len(rst_files)} active .rst files remain")
    if errors:
        for error in errors[:250]:
            print(error)
        if len(errors) > 250:
            print(f"... and {len(errors) - 250} more errors")
        print(f"Markdown validation failed with {len(errors)} issue(s).")
        return 1
    print(f"Validated {len(files)} Markdown pages, local links, fragments, and Python code fences.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
