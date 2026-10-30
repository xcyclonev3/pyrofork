#  Pyrogram - Telegram MTProto API Client Library for Python
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

"""Links in Markdown: what they find, how long it takes, and what happens to the ones that aren't links."""

import asyncio
import random
import re
import time

import pytest

from pyrogram import raw
from pyrogram.parser.markdown import Markdown, find_links

LINK_RE = re.compile(r"(!?)\[(.+?)\]\((.+?)\)")


def regex_links(text):
    return [(m.start(), m.group(1), m.group(2), m.group(3), m.group(0)) for m in LINK_RE.finditer(text)]


def parse(text, strict=False):
    return asyncio.run(Markdown(None).parse(text, strict))


@pytest.mark.parametrize("text", [
    "",
    "[",
    "[]()",
    "[](x)",
    "[a]()",
    "[a](b)",
    "![a](b)",
    "!![a](b)",
    "! [a](b)",
    "[]](x)",
    "[[a]](b)",
    "[a][b](c)",
    "[a](b)c)",
    "[a](b) [c](d)",
    "[a](b)![c](d)",
    "[a\n](b)",
    "[a](\nb)",
    "[a](b\n)",
    "x[a](\n[b](c)",
    "[a]\n[b](c)",
    "[a](b\r)",
    "[😀](🎉)",
])
def test_find_links_finds_what_the_pattern_matches(text):
    assert list(find_links(text)) == regex_links(text)


def test_find_links_finds_what_the_pattern_matches_in_random_text():
    rng = random.Random(1234)

    for _ in range(20000):
        text = "".join(rng.choice("[[]]](()))!!\na ") for _ in range(rng.randrange(40)))
        assert list(find_links(text)) == regex_links(text), repr(text)


@pytest.mark.parametrize("text", [
    "[](" * 1365,
    "[a](" * 1024,
    "![a](" * 819,
    "[" * 4096,
    "[]" * 2048,
    "](" * 2048,
    "[a]" * 1365,
    "[a](b" * 819,
    "[a](b)" * 682,
    ("[](" * 100 + "\n") * 13,
])
@pytest.mark.parametrize("strict", [False, True], ids=["default", "markdown"])
def test_links_in_hostile_text_do_not_block_the_event_loop(text, strict):
    start = time.perf_counter()

    parse(text, strict)

    # The pattern took 6 seconds with the first one
    assert time.perf_counter() - start < 0.5


@pytest.mark.parametrize("text", [
    "![logo](https://example.org/logo.png)",
    "see ![logo](https://example.org/logo.png) here",
    "![a](b)",
    "![a](tg://emoji?id=x)",
    "![a](tg://emoji?id=)",
    f"![a](tg://emoji?id={2 ** 63})",
    "![a](tg://user?id=5)",
    "![fig](123)",
    "![a](tg://emoji?id=1_000)",
])
@pytest.mark.parametrize("strict", [False, True], ids=["default", "markdown"])
def test_what_looks_like_an_image_stays_as_it_was_written(text, strict):
    assert parse(text, strict) == {"message": text, "entities": None}


def test_custom_emojis_keep_working():
    parsed = parse("x ![😀](tg://emoji?id=123) y")

    [emoji] = parsed["entities"]
    assert parsed["message"] == "x 😀 y"
    assert isinstance(emoji, raw.types.MessageEntityCustomEmoji) and emoji.document_id == 123


def test_links_keep_working_with_what_looks_like_an_image_beside_them():
    parsed = parse("![i](https://a.b/c.png) [t](https://a.b/)")

    [link] = parsed["entities"]
    assert parsed["message"] == "![i](https://a.b/c.png) t"
    assert link.url == "https://a.b/"
