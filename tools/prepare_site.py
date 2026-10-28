#!/usr/bin/env python3
"""Stage docs and normalize legacy duplicate-heading links for Zensical."""

from __future__ import annotations

import re
import shutil
from html.parser import HTMLParser
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs"
STAGED = ROOT / ".build" / "docs"
MANIFEST = STAGED / "api" / "manifest.md"
MANIFEST_SECTION_SIZE = 120
MAX_TABLE_SECTION_BYTES = 24_000
LEGACY_TOC_LINK = re.compile(
    r"\]\(#(?P<slug>[a-z0-9][a-z0-9_-]*)-(?P<index>[1-9][0-9]*)\)"
)


class HeadingIdParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            for name, value in attrs:
                if name == "id" and value:
                    self.ids.add(value)


def heading_ids(source: str) -> set[str]:
    rendered = markdown.markdown(source, extensions=["toc"])
    parser = HeadingIdParser()
    parser.feed(rendered)
    return parser.ids


def normalize_repeated_heading_links(path: Path) -> int:
    original = path.read_text(encoding="utf-8")
    if not LEGACY_TOC_LINK.search(original):
        return 0

    ids = heading_ids(original)

    changed = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal changed
        legacy = f"{match.group('slug')}-{match.group('index')}"
        canonical = f"{match.group('slug')}_{match.group('index')}"
        if legacy not in ids and canonical in ids:
            changed += 1
            return match.group(0).replace(legacy, canonical)
        return match.group(0)

    normalized = LEGACY_TOC_LINK.sub(replace, original)
    if changed:
        path.write_text(normalized, encoding="utf-8")
    return changed


def split_manifest_for_search(path: Path) -> int:
    """Split the generated symbol table into search-sized Markdown sections."""
    lines = path.read_text(encoding="utf-8").splitlines()
    table_header = next(
        (index for index, line in enumerate(lines) if line == "| Qualified symbol | Reference |"),
        None,
    )
    if table_header is None or table_header + 1 >= len(lines):
        return 0

    rows = [line for line in lines[table_header + 2 :] if line.startswith("| `pyrogram.")]
    if len(rows) <= MANIFEST_SECTION_SIZE:
        return 0

    content = lines[:table_header]
    content.extend(["## Symbol index", ""])
    for offset in range(0, len(rows), MANIFEST_SECTION_SIZE):
        section = rows[offset : offset + MANIFEST_SECTION_SIZE]
        first = offset + 1
        last = offset + len(section)
        content.extend(
            [
                f"### Entries {first}-{last}",
                "",
                "| Qualified symbol | Reference |",
                "| --- | --- |",
                *section,
                "",
            ]
        )

    path.write_text("\n".join(content).rstrip() + "\n", encoding="utf-8")
    return len(rows)


def split_oversized_tables(root: Path) -> tuple[int, int]:
    """Add headings around large tables so each search section stays bounded."""
    tables_split = 0
    rows_split = 0

    for path in root.rglob("*.md"):
        lines = path.read_text(encoding="utf-8").splitlines()
        output: list[str] = []
        cursor = 0
        heading_level = 0
        fence_char: str | None = None
        fence_size = 0
        index = 0

        while index < len(lines):
            fence = re.match(r"^\s*(`{3,}|~{3,})", lines[index])
            if fence:
                marker = fence.group(1)
                if fence_char is None:
                    fence_char = marker[0]
                    fence_size = len(marker)
                elif marker[0] == fence_char and len(marker) >= fence_size:
                    fence_char = None
                    fence_size = 0
                index += 1
                continue
            if fence_char is not None:
                index += 1
                continue

            heading = re.match(r"^(#{1,6})\s+", lines[index])
            if heading:
                heading_level = len(heading.group(1))

            is_table_header = (
                lines[index].startswith("|")
                and index + 1 < len(lines)
                and lines[index + 1].startswith("|")
                and re.search(r"\|\s*:?-{3,}", lines[index + 1])
            )
            if not is_table_header:
                index += 1
                continue

            end = index + 2
            while end < len(lines) and lines[end].startswith("|"):
                end += 1

            rows = lines[index + 2 : end]
            table_size = sum(len(row.encode("utf-8")) + 1 for row in rows)
            if table_size <= MAX_TABLE_SECTION_BYTES or len(rows) < 2:
                index = end
                continue

            output.extend(lines[cursor:index])
            if output and output[-1].strip():
                output.append("")

            chunk_rows: list[str] = []
            chunk_bytes = 0
            chunks: list[list[str]] = []
            for row in rows:
                row_bytes = len(row.encode("utf-8")) + 1
                if chunk_rows and chunk_bytes + row_bytes > MAX_TABLE_SECTION_BYTES:
                    chunks.append(chunk_rows)
                    chunk_rows = []
                    chunk_bytes = 0
                chunk_rows.append(row)
                chunk_bytes += row_bytes
            if chunk_rows:
                chunks.append(chunk_rows)

            section_level = "#" * min(max(heading_level + 1, 2), 6)
            first_row = 1
            for chunk in chunks:
                last_row = first_row + len(chunk) - 1
                output.extend(
                    [
                        f"{section_level} Entries {first_row}-{last_row}",
                        "",
                        lines[index],
                        lines[index + 1],
                        *chunk,
                        "",
                    ]
                )
                first_row = last_row + 1

            tables_split += 1
            rows_split += len(rows)
            cursor = end
            index = end

        output.extend(lines[cursor:])
        if output != lines:
            path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")

    return tables_split, rows_split


def main() -> None:
    if not SOURCE.is_dir():
        raise SystemExit(f"Documentation source directory not found: {SOURCE}")

    STAGED.parent.mkdir(parents=True, exist_ok=True)
    if STAGED.exists():
        shutil.rmtree(STAGED)
    shutil.copytree(SOURCE, STAGED, ignore=shutil.ignore_patterns(".DS_Store"))

    manifest_rows = split_manifest_for_search(MANIFEST) if MANIFEST.is_file() else 0
    split_tables, split_rows = split_oversized_tables(STAGED)

    adjusted = 0
    for path in STAGED.rglob("*.md"):
        adjusted += normalize_repeated_heading_links(path)

    print(
        f"Staged {sum(1 for _ in STAGED.rglob('*.md'))} Markdown pages; "
        f"split {manifest_rows} manifest symbols and {split_rows} rows across "
        f"{split_tables} large tables for search; "
        f"normalized {adjusted} legacy repeated-heading links."
    )


if __name__ == "__main__":
    main()
