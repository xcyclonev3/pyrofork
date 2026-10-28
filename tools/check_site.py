#!/usr/bin/env python3
"""Smoke-check the generated documentation site artifact."""

from __future__ import annotations

import json
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
SITE = ROOT / "site"
SITE_URL = "https://xcyclonev3.github.io/pyrofork/"


def output_for(source: Path) -> Path:
    relative = source.relative_to(DOCS)
    if relative.name == "README.md":
        return SITE / relative.parent / "index.html"
    return SITE / relative.with_suffix(".html")


def main() -> None:
    pages = sorted(DOCS.rglob("*.md"))
    missing = [str(path.relative_to(DOCS)) for path in pages if not output_for(path).is_file()]
    if missing:
        raise SystemExit(f"Missing built pages ({len(missing)}):\n" + "\n".join(missing[:30]))

    home = SITE / "index.html"
    if not home.is_file():
        raise SystemExit("The root README did not produce site/index.html.")
    home_html = home.read_text(encoding="utf-8")
    for expected in ("Pyrofork", "Quick start", "API Reference"):
        if expected.casefold() not in home_html.casefold():
            raise SystemExit(f"Homepage is missing expected content: {expected}")

    expected_pages = (
        DOCS / "api" / "methods" / "messages" / "send_message.md",
        DOCS / "api" / "client.md",
        DOCS / "api" / "decorators" / "on_ton_wallet_state.md",
        DOCS / "api" / "raw" / "functions" / "help-g.md",
        next((DOCS / "api" / "raw" / "types").glob("*.md")),
    )
    for page in expected_pages:
        if not output_for(page).is_file():
            raise SystemExit(f"Expected API page was not built: {page.relative_to(DOCS)}")

    sitemap = SITE / "sitemap.xml"
    if not sitemap.is_file():
        raise SystemExit("sitemap.xml was not generated.")
    sitemap_root = ElementTree.parse(sitemap).getroot()
    sitemap_locations = {
        element.text
        for element in sitemap_root.iter()
        if element.tag.endswith("}loc") and element.text
    }
    if not sitemap_locations or any(not url.startswith(SITE_URL) for url in sitemap_locations):
        raise SystemExit(f"Sitemap does not use the configured site URL: {SITE_URL}")
    for page in expected_pages:
        relative = output_for(page).relative_to(SITE).as_posix()
        expected_url = f"{SITE_URL}{relative}"
        if expected_url not in sitemap_locations:
            raise SystemExit(f"Sitemap is missing a generated API page: {relative}")

    search_index = SITE / "search.json"
    if not search_index.is_file():
        raise SystemExit("No client-side search index was generated at site/search.json.")

    search_data = json.loads(search_index.read_text(encoding="utf-8"))
    search_config = search_data.get("config", {})
    separator = search_config.get("separator", "")
    if "_" not in separator or r"\.(?!\d)" not in separator or "(?=[A-Z][a-z])" not in separator:
        raise SystemExit("Search tokenization must split underscores, dotted names, and CamelCase.")

    search_items = search_data.get("items", [])
    search_text = json.dumps(search_items, ensure_ascii=False).casefold()
    for term in (
        "send_message",
        "pyrogram.client.on_ton_wallet_state",
        "pyrogram.raw.functions.help.getappconfig",
        "getappconfig",
    ):
        if term.casefold() not in search_text:
            raise SystemExit(f"Search index is missing an API symbol term: {term}")

    for route in (
        "api/decorators/on_ton_wallet_state.html",
        "api/raw/functions/help-g.html",
    ):
        if not any(item.get("location", "").startswith(route) for item in search_items):
            raise SystemExit(f"Search index is missing a hidden API page: {route}")

    for page in (home, output_for(DOCS / "api" / "decorators" / "on_ton_wallet_state.md")):
        html = page.read_text(encoding="utf-8")
        if f'<link rel="canonical" href="{SITE_URL}' not in html:
            raise SystemExit(f"Page canonical URL is missing the /pyrofork/ base path: {page.relative_to(SITE)}")

    if len(sitemap_locations) < len(pages):
        raise SystemExit(
            f"Sitemap has {len(sitemap_locations)} pages for {len(pages)} Markdown sources."
        )

    print(
        f"Site smoke checks passed: {len(pages)} Markdown pages, "
        f"{len(search_items)} searchable sections, and {len(sitemap_locations)} sitemap URLs."
    )


if __name__ == "__main__":
    main()
