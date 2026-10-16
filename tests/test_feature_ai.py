#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrofork.
#
#  Pyrofork is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrofork is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrofork.  If not, see <http://www.gnu.org/licenses/>.

"""Offline tests of the AI text features: text composition styles, AI compose, summaries and translation tones."""

import pytest

from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, PEERS


def text(value, entities=None):
    return raw.types.TextWithEntities(text=value, entities=entities or [])


RAW_AUTHOR = raw.types.User(id=7, first_name="Ann", access_hash=2)

DEFAULT_TONE = raw.types.AiComposeToneDefault(tone="formal", emoji_id=5001, title="Formal")


def custom_tone(**kwargs):
    fields = dict(
        id=11,
        access_hash=22,
        slug="PirateTone1",
        title="Pirate",
        creator=True,
        emoji_id=5002,
        prompt="Talk like a pirate",
        installs_count=3,
        author_id=7,
        example_english=raw.types.AiComposeToneExample(
            from_peer=text("Hello"),
            to=text("Ahoy matey", [raw.types.MessageEntityBold(offset=0, length=4)])
        )
    )
    fields.update(kwargs)
    return raw.types.AiComposeTone(**fields)


def tones(*items, users=(RAW_AUTHOR,)):
    return raw.types.aicompose.Tones(hash=1, tones=list(items), users=list(users))


def tone_client(respond=lambda query: True):
    """A client whose aicompose.getTones returns the default and the custom tone."""
    def dispatch(query):
        if isinstance(query, raw.functions.aicompose.GetTones):
            return tones(DEFAULT_TONE, custom_tone())
        return respond(query)

    return offline_client(PEERS, dispatch)


# Types

def test_formatted_text_parse():
    parsed = types.FormattedText._parse(None, roundtrip(text("Hi there", [
        raw.types.MessageEntityBold(offset=0, length=2),
        raw.types.MessageEntityMentionName(offset=3, length=5, user_id=7),
    ])), {7: roundtrip(RAW_AUTHOR)})

    assert parsed.text == "Hi there"
    assert [e.type for e in parsed.entities] == [enums.MessageEntityType.BOLD, enums.MessageEntityType.TEXT_MENTION]
    assert parsed.entities[1].user.id == 7 and parsed.entities[1].user.first_name == "Ann"
    assert parsed.text.html == '<b>Hi</b> <a href="tg://user?id=7">there</a>'

    plain = types.FormattedText._parse(None, roundtrip(text("")))
    assert plain.text == "" and plain.entities is None
    assert types.FormattedText._parse(None, None) is None


def test_formatted_text_keeps_mentioned_users_without_users():
    # Texts returned by the AI methods come without users
    parsed = types.FormattedText._parse(None, roundtrip(text("Hi Bob", [
        raw.types.MessageEntityMentionName(offset=3, length=3, user_id=42),
    ])))

    mention = parsed.entities[0]
    assert mention.type == enums.MessageEntityType.TEXT_MENTION and mention.user.id == 42
    assert parsed.text.html == 'Hi <a href="tg://user?id=42">Bob</a>'


def test_text_composition_style_example_parse():
    example = types.TextCompositionStyleExample._parse(None, roundtrip(raw.types.AiComposeToneExample(
        from_peer=text("Hello"),
        to=text("Ahoy matey", [raw.types.MessageEntityItalic(offset=5, length=5)])
    )))

    assert example.source_text.text == "Hello" and example.source_text.entities is None
    assert example.result_text.text == "Ahoy matey"
    assert example.result_text.entities[0].type == enums.MessageEntityType.ITALIC
    assert example.result_text.text.markdown == "Ahoy __matey__"


def test_text_composition_style_parse_default():
    style = types.TextCompositionStyle._parse(None, roundtrip(DEFAULT_TONE))

    assert (style.name, style.title, style.custom_emoji_id) == ("formal", "Formal", 5001)
    assert style.is_custom is False and style.is_creator is None and style.creator is None
    assert style.link is None
    assert isinstance(style.write(), raw.types.InputAiComposeToneDefault) and style.write().tone == "formal"


def test_text_composition_style_parse_custom():
    style = types.TextCompositionStyle._parse(None, roundtrip(custom_tone()), {7: roundtrip(RAW_AUTHOR)})

    assert (style.name, style.title, style.custom_emoji_id) == ("PirateTone1", "Pirate", 5002)
    assert style.is_custom is True and style.is_creator is True
    assert style.install_count == 3 and style.prompt == "Talk like a pirate"
    assert style.creator.id == 7 and style.creator.first_name == "Ann"
    assert style.english_example.source_text.text == "Hello"
    assert style.english_example.result_text.entities[0].type == enums.MessageEntityType.BOLD
    assert style.link == "https://t.me/addstyle/PirateTone1"
    assert isinstance(style.write(), raw.types.InputAiComposeToneSlug) and style.write().slug == "PirateTone1"


def test_text_composition_style_parse_custom_without_optional_fields():
    style = types.TextCompositionStyle._parse(None, roundtrip(raw.types.AiComposeTone(
        id=11, access_hash=22, slug="SomeSlug1", title="Some"
    )))

    assert style.is_custom is True and style.is_creator is False
    assert style.custom_emoji_id is None and style.install_count is None and style.prompt is None
    assert style.creator is None and style.english_example is None


def test_text_composition_style_input():
    client = tone_client()

    def get_input(style, **kwargs):
        return call(lambda: types.TextCompositionStyle._get_input(client, style, **kwargs))

    # A custom prompt wins over the style, like in TDLib
    single_use = get_input("formal", custom_prompt="Rhyme everything")
    assert isinstance(single_use, raw.types.InputAiComposeToneSingleUse)
    assert single_use.custom_prompt == "Rhyme everything"

    assert get_input(None, optional=True) is None
    assert get_input("", optional=True) is None
    with pytest.raises(ValueError):
        get_input(None)
    assert client.sent == []

    # Style objects need no request
    style = types.TextCompositionStyle(name="formal", title="Formal", is_custom=False)
    assert isinstance(get_input(style), raw.types.InputAiComposeToneDefault)
    assert client.sent == []

    # Names are looked up in the styles of the user, otherwise used as slugs
    default = get_input("formal")
    assert isinstance(default, raw.types.InputAiComposeToneDefault) and default.tone == "formal"
    custom = get_input("PirateTone1")
    assert isinstance(custom, raw.types.InputAiComposeToneID) and (custom.id, custom.access_hash) == (11, 22)
    slug = get_input("Shared_Style-2")
    assert isinstance(slug, raw.types.InputAiComposeToneSlug) and slug.slug == "Shared_Style-2"
    with pytest.raises(ValueError):
        get_input("not a slug")

    assert len(client.sent) == 4
    assert all(isinstance(q, raw.functions.aicompose.GetTones) and q.hash == 0 for q in client.sent)


# Methods

def test_get_text_composition_styles():
    client = offline_client(PEERS, lambda q: tones(DEFAULT_TONE, custom_tone()))
    styles = call(lambda: client.get_text_composition_styles())

    request = client.sent[-1]
    assert isinstance(request, raw.functions.aicompose.GetTones) and request.hash == 0
    assert [(s.name, s.is_custom) for s in styles] == [("formal", False), ("PirateTone1", True)]
    assert styles[1].creator.first_name == "Ann"

    client = offline_client(PEERS, lambda q: raw.types.aicompose.TonesNotModified())
    assert call(lambda: client.get_text_composition_styles()) == []


def test_create_text_composition_style():
    client = offline_client(PEERS, lambda q: custom_tone(installs_count=None, example_english=None))
    client.me = types.User(id=7, first_name="Me")
    style = call(lambda: client.create_text_composition_style("Pirate", 5002, "Talk like a pirate", show_creator=True))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.aicompose.CreateTone)
    assert (request.title, request.emoji_id, request.prompt) == ("Pirate", 5002, "Talk like a pirate")
    assert request.display_author is True
    assert style.name == "PirateTone1" and style.is_creator is True
    assert style.creator is client.me  # createTone returns no users


def test_edit_text_composition_style():
    client = offline_client(PEERS, lambda q: custom_tone(title="Captain"))
    style = types.TextCompositionStyle(name="PirateTone1", title="Pirate", is_custom=True)
    edited = call(lambda: client.edit_text_composition_style(style, title="Captain", show_creator=False))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.aicompose.UpdateTone)
    assert isinstance(request.tone, raw.types.InputAiComposeToneSlug) and request.tone.slug == "PirateTone1"
    assert request.title == "Captain" and request.display_author is False
    assert request.emoji_id is None and request.prompt is None  # Only the passed fields are changed
    assert edited.title == "Captain"


def test_delete_text_composition_style():
    client = tone_client()
    assert call(lambda: client.delete_text_composition_style("PirateTone1")) is True

    get_tones, request = client.sent
    assert isinstance(get_tones, raw.functions.aicompose.GetTones)
    assert isinstance(request, raw.functions.aicompose.DeleteTone)
    assert isinstance(request.tone, raw.types.InputAiComposeToneID) and request.tone.id == 11


def test_search_text_composition_style():
    found = custom_tone(creator=False, prompt=None)
    client = tone_client(lambda q: tones(found))
    style = call(lambda: client.search_text_composition_style("SharedStyle1"))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.aicompose.GetTone)
    assert isinstance(request.tone, raw.types.InputAiComposeToneSlug) and request.tone.slug == "SharedStyle1"
    assert style.name == "PirateTone1" and style.is_creator is False and style.creator.id == 7

    client = tone_client(lambda q: raw.types.aicompose.TonesNotModified())
    assert call(lambda: client.search_text_composition_style("SharedStyle1")) is None


def test_get_text_composition_style_example():
    client = tone_client(lambda q: raw.types.AiComposeToneExample(from_peer=text("Hi"), to=text("Hello there")))
    example = call(lambda: client.get_text_composition_style_example("formal", 2))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.aicompose.GetToneExample) and request.num == 2
    assert isinstance(request.tone, raw.types.InputAiComposeToneDefault) and request.tone.tone == "formal"
    assert example.source_text.text == "Hi" and example.result_text.text == "Hello there"


@pytest.mark.parametrize("method, unsave", [
    ("add_text_composition_style", False),
    ("remove_text_composition_style", True),
])
def test_add_and_remove_text_composition_style(method, unsave):
    client = tone_client()
    style = types.TextCompositionStyle(name="SharedStyle1", title="Shared", is_custom=True)
    assert call(lambda: getattr(client, method)(style)) is True

    request = client.sent[-1]
    assert len(client.sent) == 1
    assert isinstance(request, raw.functions.aicompose.SaveTone) and request.unsave is unsave
    assert isinstance(request.tone, raw.types.InputAiComposeToneSlug) and request.tone.slug == "SharedStyle1"


# AI compose

def composed(result, diff=None):
    return raw.types.messages.ComposedMessageWithAI(result_text=result, diff_text=diff)


# "Its a a test" fixed: the diff keeps the deleted text and marks the replaced, deleted and inserted parts
FIXED = composed(
    text("It's a test."),
    text("It's a a test.", [
        raw.types.MessageEntityDiffReplace(offset=0, length=4, old_text="Its"),
        raw.types.MessageEntityDiffDelete(offset=5, length=2),
        raw.types.MessageEntityDiffInsert(offset=13, length=1),
    ])
)


def test_fixed_text_parse():
    fixed = types.FixedText._parse(None, roundtrip(FIXED))

    assert fixed.text.text == "It's a test." and fixed.text.entities is None
    assert fixed.diff_text.text == "It's a a test."
    assert [(e.type, e.offset, e.length, e.old_text) for e in fixed.diff_text.entities] == [
        (enums.MessageEntityType.DIFF_REPLACE, 0, 4, "Its"),
        (enums.MessageEntityType.DIFF_DELETE, 5, 2, None),
        (enums.MessageEntityType.DIFF_INSERT, 13, 1, None),
    ]

    without_diff = types.FixedText._parse(None, roundtrip(composed(text("Fine."))))
    assert without_diff.text.text == "Fine." and without_diff.diff_text is None


def test_compose_text_with_ai():
    client = tone_client(lambda q: composed(text("Hola, ¿seguimos?", [raw.types.MessageEntityBold(offset=0, length=4)])))
    result = call(lambda: client.compose_text_with_ai(
        "**hey**, are we on?", translate_to_language_code="es", style="formal", add_emojis=True
    ))

    get_tones, request = client.sent
    assert isinstance(get_tones, raw.functions.aicompose.GetTones)
    assert isinstance(request, raw.functions.messages.ComposeMessageWithAI)
    assert request.text.text == "hey, are we on?"
    assert [type(e) for e in request.text.entities] == [raw.types.MessageEntityBold]
    assert request.translate_to_lang == "es" and request.emojify is True and request.proofread is False
    assert isinstance(request.tone, raw.types.InputAiComposeToneDefault) and request.tone.tone == "formal"
    assert isinstance(result, types.FormattedText)
    assert result.text == "Hola, ¿seguimos?" and result.entities[0].type == enums.MessageEntityType.BOLD


def test_compose_text_with_ai_without_style():
    client = offline_client(PEERS, lambda q: composed(text("I love pizza 🍕")))
    style = types.TextCompositionStyle(name="PirateTone1", title="Pirate", is_custom=True)
    result = call(lambda: client.compose_text_with_ai("I love *pizza*", style=style, parse_mode=enums.ParseMode.DISABLED))

    request, = client.sent  # A style object needs no request to find it
    assert request.text.text == "I love *pizza*" and request.text.entities == []
    assert isinstance(request.tone, raw.types.InputAiComposeToneSlug) and request.tone.slug == "PirateTone1"
    assert request.translate_to_lang is None and request.emojify is False
    assert result.text == "I love pizza 🍕"

    client = offline_client(PEERS, lambda q: composed(text("I love pizza 🍕")))
    call(lambda: client.compose_text_with_ai("I love pizza", add_emojis=True))

    request, = client.sent
    assert request.tone is None and request.emojify is True


def test_fix_text_with_ai():
    client = offline_client(PEERS, lambda q: FIXED)
    fixed = call(lambda: client.fix_text_with_ai(
        "Its a a test", entities=[types.MessageEntity(type=enums.MessageEntityType.ITALIC, offset=0, length=3)]
    ))

    request, = client.sent
    assert isinstance(request, raw.functions.messages.ComposeMessageWithAI)
    assert request.proofread is True and request.emojify is False
    assert request.tone is None and request.translate_to_lang is None
    assert request.text.text == "Its a a test"
    assert [(type(e), e.offset, e.length) for e in request.text.entities] == [(raw.types.MessageEntityItalic, 0, 3)]
    assert isinstance(fixed, types.FixedText)
    assert fixed.text.text == "It's a test."
    assert [e.type for e in fixed.diff_text.entities] == [
        enums.MessageEntityType.DIFF_REPLACE, enums.MessageEntityType.DIFF_DELETE, enums.MessageEntityType.DIFF_INSERT
    ]
