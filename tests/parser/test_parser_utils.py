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

"""The helpers of the parsers do what their slower forms did."""

import random
import re
import time
from struct import unpack

import pytest

from pyrogram.parser import utils
from pyrogram.types.messages_and_media.message import Str

SMP_RE = re.compile(r"[\U00010000-\U0010FFFF]")
TEXTS = [
    "",
    "plain",
    "café",
    "😀",
    "a😀b👨‍👩‍👧c",
    "\U00010000\U0010ffff",
    "\ufeffa",
    "a\x00b",
    "\ud83d\ude00",  # A surrogate pair given as two characters
]


def old_add_surrogates(text):
    return SMP_RE.sub(lambda match: "".join(chr(i) for i in unpack("<HH", match.group().encode("utf-16le"))), text)


def old_remove_surrogates(text):
    return text.encode("utf-16", "surrogatepass").decode("utf-16")


def random_texts(alphabet, count=20000, longest=30, seed=1234):
    rng = random.Random(seed)

    for _ in range(count):
        yield "".join(rng.choice(alphabet) for _ in range(rng.randrange(longest)))


@pytest.mark.parametrize("text", TEXTS)
def test_add_surrogates_is_unchanged(text):
    assert utils.add_surrogates(text) == old_add_surrogates(text)


@pytest.mark.parametrize("text", TEXTS)
def test_remove_surrogates_is_unchanged(text):
    assert utils.remove_surrogates(text) == old_remove_surrogates(text)


def test_surrogates_are_unchanged_in_random_text():
    for text in random_texts("ab é€\ud83d\ude00😀🎉\U00010000\U0010ffff"):
        added = utils.add_surrogates(text)
        assert added == old_add_surrogates(text)

        try:
            expected = old_remove_surrogates(text)
        except UnicodeDecodeError:
            with pytest.raises(UnicodeDecodeError):
                utils.remove_surrogates(text)
        else:
            assert utils.remove_surrogates(text) == expected


@pytest.mark.parametrize("function", [utils.add_surrogates, utils.remove_surrogates])
def test_surrogate_helpers_give_plain_strings(function):
    """Slicing a Str calls them: what they give must not be a Str again."""
    assert type(function(Str("ascii"))) is str
    assert type(function(Str("😀"))) is str


@pytest.mark.parametrize("function, old", [(utils.add_surrogates, old_add_surrogates), (utils.remove_surrogates, old_remove_surrogates)])
@pytest.mark.parametrize("text", ["ascii", "😀"])
def test_surrogate_helpers_give_the_text_of_a_subclass_with_its_own_str(function, old, text):
    class Named(str):
        def __str__(self):
            return "another text"

    assert function(Named(text)) == old(Named(text)) != "another text"


def test_slicing_a_str_counts_utf16_code_units():
    text = Str("😀abc").init([])

    assert text[:2] == "😀" and text[2:] == "abc" and type(text[2:]) is str


@pytest.mark.parametrize("function, error", [
    (utils.add_surrogates, TypeError),
    (utils.remove_surrogates, AttributeError),
])
@pytest.mark.parametrize("value", [None, 1, b"ascii"])
def test_surrogate_helpers_fail_as_before_on_what_isnt_text(function, error, value):
    with pytest.raises(error):
        function(value)


def test_utf16_length_counts_what_add_surrogates_gives():
    for text in [*TEXTS, *random_texts("ab é€\ud83d\ude00😀🎉\U00010000\U0010ffff")]:
        assert utils.utf16_length(text) == len(old_add_surrogates(text))


CLOSING_TAGS = re.compile(r"\s*(</[\w</>]*>)\s*$")


@pytest.mark.parametrize("text", [
    "",
    ">",
    "</",
    "</>",
    "a </b>",
    "a </b> ",
    "a\n\n</b>\n",
    "a </b> </i>",
    "a </b></i> \n ",
    "a </b> x",
    "a </b> x </i>",
    "</b>",
    "  </b>  ",
    "<b> </b>",
    "a b>",
    "a</b",
    "a<//>",
    "a </é>",
    "a </_>",
    "a </b c>",
    "a\xa0</b>\u2003",
    "a\x1c</b>\x1f",
    "a </b>\n\n",
])
def test_strip_closing_tags_does_what_the_pattern_does(text):
    assert utils.strip_closing_tags(text) == CLOSING_TAGS.sub(r"\1", text)


def test_strip_closing_tags_does_what_the_pattern_does_in_random_text():
    for text in random_texts(" \n\t\xa0<>/<>/ab_é1"):
        assert utils.strip_closing_tags(text) == CLOSING_TAGS.sub(r"\1", text), repr(text)


@pytest.mark.parametrize("text", [
    "a" + " " * 65536 + "b<",
    "a" + " " * 65536 + "</b>",
    " " * 65536 + "x</b> y>",
    "</" * 32768 + ">",
    "</>" * 21845 + " x",
    "a" * 65536 + ">",
])
def test_strip_closing_tags_takes_linear_time(text):
    start = time.perf_counter()

    utils.strip_closing_tags(text)

    # The pattern took seconds with the first ones
    assert time.perf_counter() - start < 0.2
