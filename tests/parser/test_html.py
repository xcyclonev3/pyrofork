#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
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

import asyncio

import pytest

import pyrogram
from pyrogram.parser.html import HTML, Parser
from pyrogram.parser.markdown import Markdown


# expected: the expected unparsed HTML
# text: original text without entities
# entities: message entities coming from the server

def test_html_unparse_bold():
    expected = "<b>bold</b>"
    text = "bold"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.BOLD, offset=0, length=4)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_italic():
    expected = "<i>italic</i>"
    text = "italic"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.ITALIC, offset=0, length=6)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_underline():
    expected = "<u>underline</u>"
    text = "underline"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.UNDERLINE, offset=0, length=9)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_strike():
    expected = "<s>strike</s>"
    text = "strike"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.STRIKETHROUGH, offset=0, length=6)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_spoiler():
    expected = "<spoiler>spoiler</spoiler>"
    text = "spoiler"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.SPOILER, offset=0, length=7)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_url():
    expected = '<a href="https://pyrogram.org/">URL</a>'
    text = "URL"
    entities = pyrogram.types.List([pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.TEXT_LINK,
                                                                 offset=0, length=3, url='https://pyrogram.org/')])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_code():
    expected = '<code>code</code>'
    text = "code"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.CODE, offset=0, length=4)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_pre():
    expected = """<pre language="python">for i in range(10):
    print(i)</pre>"""

    text = """for i in range(10):
    print(i)"""

    entities = pyrogram.types.List([pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.PRE, offset=0,
                                                                 length=32, language='python')])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_mixed():
    expected = "<b>aaaaaaa<i>aaa<u>bbbb</u></i></b><u><i>bbbbbbccc</i></u><u>ccccccc<s>ddd</s></u><s>ddddd<spoiler>dd" \
               "eee</spoiler></s><spoiler>eeeeeeefff</spoiler>ffff<code>fffggggggg</code>ggghhhhhhhhhh"
    text = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhh"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.BOLD, offset=0, length=14),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.ITALIC, offset=7, length=7),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.UNDERLINE, offset=10, length=4),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.UNDERLINE, offset=14, length=9),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.ITALIC, offset=14, length=9),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.UNDERLINE, offset=23, length=10),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.STRIKETHROUGH, offset=30, length=3),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.STRIKETHROUGH, offset=33, length=10),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.SPOILER, offset=38, length=5),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.SPOILER, offset=43, length=10),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.CODE, offset=57, length=10)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_escaped():
    expected = "<b>&lt;b&gt;bold&lt;/b&gt;</b>"
    text = "<b>bold</b>"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.BOLD, offset=0, length=11)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_escaped_nested():
    expected = "<b>&lt;b&gt;bold <u>&lt;u&gt;underline&lt;/u&gt;</u> bold&lt;/b&gt;</b>"
    text = "<b>bold <u>underline</u> bold</b>"
    entities = pyrogram.types.List(
        [pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.BOLD, offset=0, length=33),
         pyrogram.types.MessageEntity(type=pyrogram.enums.MessageEntityType.UNDERLINE, offset=8, length=16)])

    assert HTML.unparse(text=text, entities=entities) == expected


def test_html_unparse_no_entities():
    expected = "text"
    text = "text"
    entities = []

    assert HTML.unparse(text=text, entities=entities) == expected


def entity(entity_type, offset, length, **kwargs):
    return pyrogram.types.MessageEntity(type=entity_type, offset=offset, length=length, **kwargs)


def bold(offset, length):
    return entity(pyrogram.enums.MessageEntityType.BOLD, offset, length)


def parse(text):
    return asyncio.run(HTML(None).parse(text))


@pytest.mark.parametrize("text, expected", [
    ("&amp;copy=1", "&copy=1"),
    ("&amp;amp;", "&amp;"),
    ("&amp;lt;b&amp;gt;", "&lt;b&gt;"),
    ("&lt;b&gt;", "<b>"),
    ("a &copy; b", "a \u00a9 b"),
    ("<b>&amp;copy=1</b>", "&copy=1"),
    ("<style>a &amp; b</style> c", "a & b c"),
    ("<script>a &amp;copy=1</script>", "a &copy=1"),
    ("<title>a &amp;copy=1</title>", "a &copy=1"),
    ("<style><b>a</b></style>", "a"),
])
def test_html_parse_unescapes_once(text, expected):
    assert parse(text)["message"] == expected


def test_html_parse_counts_the_unescaped_text():
    assert parse("<b>&amp;copy=1</b>")["entities"][0].length == len("&copy=1")


def test_markdown_strict_keeps_the_text():
    assert asyncio.run(Markdown(None).parse("&amp;copy=1", strict=True))["message"] == "&amp;copy=1"


def test_parser_keeps_its_interface():
    parser = Parser(None)
    parser.feed("<b>a</b> &amp;")
    parser.close()

    assert parser.text == "a &"


def test_html_unparse_escapes_the_text_around_the_entities():
    assert HTML.unparse(text="a < b & c > d", entities=[bold(6, 1)]) == "a &lt; b <b>&amp;</b> c &gt; d"


def test_html_unparse_escapes_a_text_without_entities():
    assert HTML.unparse(text="a < b & c", entities=[]) == "a &lt; b &amp; c"


def test_html_unparse_escapes_the_attributes():
    url = 'https://example.com/?a=1&copy=2&b="c"'
    entities = [entity(pyrogram.enums.MessageEntityType.TEXT_LINK, 0, 4, url=url)]

    assert HTML.unparse(text="link", entities=entities) == (
        '<a href="https://example.com/?a=1&amp;copy=2&amp;b=&quot;c&quot;">link</a>'
    )
    assert parse(HTML.unparse(text="link", entities=entities))["entities"][0].url == url

    entities = [entity(pyrogram.enums.MessageEntityType.PRE, 0, 4, language='a"b')]

    assert HTML.unparse(text="code", entities=entities) == '<pre language="a&quot;b">code</pre>'


@pytest.mark.parametrize("text", [
    "promo <x",
    "promo <!--",
    'promo <a href="',
    "x<y sekarang",
    "tag <toko> resmi",
    "a & b &copy=2 &lt;b&gt;",
    "<emoji>",
    "it's \"quoted\"",
    "\U0001f600 <b>x</b> \U0001f600",
])
@pytest.mark.parametrize("with_entity", [True, False])
def test_html_unparse_round_trip(text, with_entity):
    # The bold text comes after the text under test, so the offset counts it in UTF-16 code units
    offset = len(text.encode("utf-16-le")) // 2 + 2
    full_text, entities = (f"{text}\n\nend", [bold(offset, 3)]) if with_entity else (text, [])

    parsed = parse(HTML.unparse(text=full_text, entities=entities))

    assert parsed["message"] == full_text
    assert [(e.offset, e.length) for e in parsed["entities"] or []] == [(e.offset, e.length) for e in entities]


def test_html_parse_counts_character_references_to_emojis():
    parsed = parse("&#128512; <b>x</b>")

    assert parsed["message"] == "\U0001f600 x"
    assert [(e.offset, e.length) for e in parsed["entities"]] == [(3, 1)]


def test_html_parse_keeps_emojis_in_attributes():
    parsed = parse('<a href="https://example.com/\U0001f600">\U0001f600</a>')

    assert parsed["entities"][0].url == "https://example.com/\U0001f600"
    assert (parsed["entities"][0].offset, parsed["entities"][0].length) == (0, 2)
    assert parsed["entities"][0].write()


def test_parser_on_its_own_does_not_produce_surrogates():
    parser = Parser(None)
    parser.feed("\U0001f600 &#128512;")
    parser.close()

    assert parser.text == "\U0001f600 \U0001f600"


def test_unparse_of_a_mention_without_its_user_keeps_the_text():
    entities = [entity(pyrogram.enums.MessageEntityType.TEXT_MENTION, 0, 4), bold(5, 4)]

    assert HTML.unparse(text="user bold", entities=list(entities)) == "user <b>bold</b>"
    assert Markdown.unparse(text="user bold", entities=list(entities)) == "user **bold**"


@pytest.mark.parametrize("text, expected", [
    ("<emoji>x</emoji>", "x"),
    ("<emoji id>x</emoji>", "x"),
    ('<emoji id="x">y</emoji>', "y"),
    ('<emoji id="">y</emoji>', "y"),
    (f'<emoji id="{2 ** 63}">y</emoji>', "y"),
    (f'<emoji id="{-2 ** 63 - 1}">y</emoji>', "y"),
    ('<emoji id="0">y</emoji>', "y"),
    ('<emoji id="1_000">y</emoji>', "y"),
    ('<emoji id=" 7 ">y</emoji>', "y"),
    ('<emoji id="٣">y</emoji>', "y"),
    ("<a>x</a>", "x"),
    ("<a href>x</a>", "x"),
    ('<a href="">x</a>', "x"),
])
def test_html_parse_takes_tags_without_a_valid_value_for_plain_text(text, expected):
    assert parse(text) == {"message": expected, "entities": None}


@pytest.mark.parametrize("text, offset, length", [
    ('<a href="https://x.y">foo<a>bar</a>baz</a>', 0, 9),
    ('<a href="https://x.y">foo<a href>bar</a>baz</a>', 0, 9),
    ('<emoji id="1">foo<emoji>bar</emoji>baz</emoji>', 0, 9),
    ('x<a>y<a href="https://x.y">z</a></a>', 2, 1),
])
def test_html_parse_keeps_the_tag_around_one_without_a_valid_value(text, offset, length):
    [entity] = parse(text)["entities"]

    assert (entity.offset, entity.length) == (offset, length)


@pytest.mark.parametrize("value", [2 ** 63 - 1, -2 ** 63, 42])
def test_html_parse_keeps_valid_custom_emojis(value):
    [emoji] = parse(f'<emoji id="{value}">x</emoji>')["entities"]

    assert emoji.document_id == value
    emoji.write()


def test_html_parse_gives_a_language_that_can_be_sent():
    [pre] = parse("<pre language>x</pre>")["entities"]

    assert pre.language == ""
    pre.write()


@pytest.mark.parametrize("digits", [20, 5000])
def test_html_parse_takes_a_mention_of_an_impossible_user_for_a_link(digits):
    url = "tg://user?id=" + "9" * digits

    [link] = parse(f'<a href="{url}">x</a>')["entities"]

    assert link.url == url


@pytest.mark.parametrize("text", ["a<![foo[b", "a<![foo[b]]>c", "<![a](b)", "a<![", "a<!"])
def test_html_parse_does_not_fail_on_marked_sections(text):
    assert parse(text)["message"].startswith(text.split("<")[0])


def test_older_pythons_do_not_fail_on_unknown_marked_sections(monkeypatch):
    from _markupbase import ParserBase

    def older_python(self, i, report=1):
        raise AssertionError("unknown status keyword")

    monkeypatch.setattr(ParserBase, "parse_marked_section", older_python)
    parser = Parser(None)
    parser.rawdata = "<![foo[b]>c"

    assert parser.parse_marked_section(0) == len("<![foo[b]>")


class MentionsClient:
    """Knows the users in *known*, and Telegram knows the ones in *telegram* or answers with *error*."""

    def __init__(self, known=(), telegram=(), error=None):
        self.known = set(known)
        self.telegram = set(telegram)
        self.error = error
        self.requests = []
        self.storage = self
        self.is_connected = True

    async def get_peer_by_id(self, peer_id):
        if peer_id not in self.known:
            raise KeyError(peer_id)

        return pyrogram.raw.types.InputPeerUser(user_id=peer_id, access_hash=1)

    resolve_peer = get_peer_by_id

    async def invoke(self, query):
        self.requests.append(sorted(i.user_id for i in query.id))

        if self.error:
            raise self.error

        return [pyrogram.raw.types.User(id=i.user_id, access_hash=1) for i in query.id if i.user_id in self.telegram]

    async def fetch_peers(self, users):
        self.known |= {user.id for user in users}


def mentions(*user_ids):
    return "".join(f'<a href="tg://user?id={user_id}">m</a>' for user_id in user_ids)


def parse_with(client, text):
    return asyncio.run(HTML(client).parse(text))


def test_html_parse_asks_for_unknown_users_once():
    client = MentionsClient(known=[1], telegram=[2])

    parsed = parse_with(client, mentions(1, 1, 2, 2, 3, 3))

    assert client.requests == [[2, 3]]
    assert [e.user_id.user_id for e in parsed["entities"]] == [1, 1, 2, 2]
    assert parsed["message"] == "m" * 6
    [e.write() for e in parsed["entities"]]


def test_html_parse_does_not_ask_for_known_users():
    client = MentionsClient(known=[1])

    assert len(parse_with(client, mentions(1))["entities"]) == 1 and client.requests == []


@pytest.mark.parametrize("error", [pyrogram.errors.PeerIdInvalid, pyrogram.errors.UserIdInvalid])
def test_html_parse_leaves_out_the_mentions_telegram_refuses(error):
    client = MentionsClient(known=[1], error=error())

    parsed = parse_with(client, mentions(1, 2))

    assert parsed["message"] == "mm" and [e.user_id.user_id for e in parsed["entities"]] == [1]
    assert client.requests == [[2]]


class RefusesOneUser(MentionsClient):
    """Telegram refuses whole requests with the user 2, and knows all the others."""

    async def invoke(self, query):
        ids = sorted(i.user_id for i in query.id)
        self.requests.append(ids)

        if 2 in ids:
            raise pyrogram.errors.UserIdInvalid

        return [pyrogram.raw.types.User(id=i, access_hash=1) for i in ids]


def test_html_parse_asks_for_a_few_users_one_by_one_when_telegram_refuses_them():
    client = RefusesOneUser()

    parsed = parse_with(client, mentions(1, 2, 3))

    assert [e.user_id.user_id for e in parsed["entities"]] == [1, 3]
    assert client.requests[0] == [1, 2, 3] and sorted(client.requests[1:]) == [[1], [2], [3]]


def test_html_parse_does_not_ask_for_many_users_one_by_one():
    client = RefusesOneUser()

    parsed = parse_with(client, mentions(*range(1, 8)))

    assert parsed["entities"] is None and client.requests == [list(range(1, 8))]


def test_html_parse_does_not_hide_other_errors_of_a_mention():
    with pytest.raises(pyrogram.errors.FloodWait):
        parse_with(MentionsClient(error=pyrogram.errors.FloodWait(value=5)), mentions(2))


def test_html_parse_needs_a_connection_only_for_mentions():
    client = MentionsClient()
    client.is_connected = False

    assert parse_with(client, "<b>x</b>")["message"] == "x"

    with pytest.raises(ConnectionError):
        parse_with(client, mentions(1))


@pytest.mark.parametrize("user_id", [0, 2 ** 63, 10 ** 19 - 1])
def test_html_parse_does_not_ask_for_impossible_users(user_id):
    client = MentionsClient()

    assert parse_with(client, mentions(user_id)) == {"message": "m", "entities": None}
    assert client.requests == []
