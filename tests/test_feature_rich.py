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

"""Rich messages (Layer 226-230): received blocks and texts, sending, editing, drafts, translation and AI."""

import pyrogram
from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import roundtrip

HA = enums.PageBlockHorizontalAlignment
VA = enums.PageBlockVerticalAlignment

PHOTO_ID, VIDEO_ID, ANIMATION_ID, AUDIO_ID, VOICE_ID, DOCUMENT_ID, ICON_ID = 101, 102, 103, 104, 105, 106, 107


def plain(text):
    return raw.types.TextPlain(text=text)


def raw_photo(photo_id):
    return raw.types.Photo(
        id=photo_id, access_hash=1, file_reference=b"r", date=1, dc_id=2,
        sizes=[raw.types.PhotoSize(type="m", w=320, h=240, size=1000)]
    )


def raw_document(document_id, attributes, mime_type="application/octet-stream"):
    return raw.types.Document(
        id=document_id, access_hash=2, file_reference=b"r", date=1, mime_type=mime_type, size=10, dc_id=2,
        attributes=attributes
    )


RAW_DOCUMENTS = [
    raw_document(VIDEO_ID, [raw.types.DocumentAttributeVideo(duration=3, w=640, h=360)], "video/mp4"),
    raw_document(ANIMATION_ID, [raw.types.DocumentAttributeVideo(duration=1, w=320, h=180),
                                raw.types.DocumentAttributeAnimated()], "video/mp4"),
    raw_document(AUDIO_ID, [raw.types.DocumentAttributeAudio(duration=60, title="Song", performer="Band"),
                            raw.types.DocumentAttributeFilename(file_name="song.mp3")], "audio/mpeg"),
    raw_document(VOICE_ID, [raw.types.DocumentAttributeAudio(voice=True, duration=5)], "audio/ogg"),
    raw_document(DOCUMENT_ID, [raw.types.DocumentAttributeFilename(file_name="report.pdf")], "application/pdf"),
    raw_document(ICON_ID, [raw.types.DocumentAttributeFilename(file_name="icon.png")], "image/png"),
]


def empty_caption():
    return raw.types.PageCaption(text=raw.types.TextEmpty(), credit=raw.types.TextEmpty())


def caption(text, credit=None):
    return raw.types.PageCaption(text=plain(text), credit=plain(credit) if credit else raw.types.TextEmpty())


def offline_parser_client():
    client = pyrogram.Client("rich", api_id=1, api_hash="x", in_memory=True)
    client.me = None
    return client


def parse_rich_message(rich_message):
    return types.RichMessage._parse(offline_parser_client(), roundtrip(rich_message))


RICH_PARAGRAPH = raw.types.TextConcat(texts=[
    plain("Hi "),
    raw.types.TextBold(text=raw.types.TextItalic(text=plain("you"))),
    raw.types.TextUnderline(text=plain("u")),
    raw.types.TextStrike(text=plain("s")),
    raw.types.TextFixed(text=plain("f")),
    raw.types.TextSubscript(text=plain("2")),
    raw.types.TextSuperscript(text=plain("3")),
    raw.types.TextMarked(text=plain("m")),
    raw.types.TextSpoiler(text=plain("hidden")),
    raw.types.TextMention(text=plain("@durov")),
    raw.types.TextHashtag(text=plain("#news")),
    raw.types.TextCashtag(text=plain("$USD")),
    raw.types.TextBotCommand(text=plain("/start")),
    raw.types.TextAutoUrl(text=plain("t.me/pyrofork")),
    raw.types.TextAutoEmail(text=plain("a@b.c")),
    raw.types.TextAutoPhone(text=plain("+123456")),
    raw.types.TextBankCard(text=plain("4242424242424242")),
    raw.types.TextTonAddress(text=plain("EQabc")),
    raw.types.TextMentionName(text=plain("Pavel"), user_id=7),
    raw.types.TextDate(text=plain("tomorrow"), date=1647531900, long_date=True, day_of_week=True),
    raw.types.TextDiff(text=plain("new"), old_text=plain("old")),
    raw.types.TextMath(source="x^2"),
    raw.types.TextCustomEmoji(document_id=5, alt="\U0001f600"),
    raw.types.TextUrl(text=plain("site"), url="https://telegram.org", webpage_id=9),
    raw.types.TextEmail(text=plain("mail"), email="me@example.org"),
    raw.types.TextPhone(text=plain("call"), phone="+100"),
    raw.types.TextUrl(text=plain("section"), url="#sec", webpage_id=0),
    raw.types.TextUrl(text=plain("see note"), url="#note", webpage_id=0),
    raw.types.TextUrl(text=plain("top"), url="#", webpage_id=0),
    raw.types.TextAnchor(text=plain("the note"), name="note"),
    raw.types.TextAnchor(text=raw.types.TextEmpty(), name="here"),
    raw.types.TextImage(document_id=ICON_ID, w=16, h=16),
    raw.types.TextButton(text=plain("Open"), type=raw.types.InlineButtonTypeUrl(url="https://example.org"),
                         style=raw.types.RichButtonStyle(link=True)),
])


def test_received_rich_message_with_many_blocks_and_texts():
    rich = parse_rich_message(raw.types.RichMessage(
        rtl=True,
        part=True,
        blocks=[
            raw.types.PageBlockHeading1(text=plain("Title")),
            raw.types.PageBlockHeading6(text=raw.types.TextBold(text=plain("Small"))),
            raw.types.PageBlockParagraph(text=RICH_PARAGRAPH),
            raw.types.PageBlockAnchor(name="sec"),
            raw.types.PageBlockMath(source="e=mc^2"),
            raw.types.PageBlockThinking(text=plain("Thinking...")),
            raw.types.PageBlockPreformatted(text=plain("print(1)"), language="python"),
            raw.types.PageBlockFooter(text=plain("bye")),
            raw.types.PageBlockDivider(),
            raw.types.PageBlockList(items=[
                raw.types.PageListItemText(checkbox=True, checked=True, text=plain("done")),
                raw.types.PageListItemBlocks(checked=True, blocks=[]),
            ]),
            raw.types.PageBlockOrderedList(start=3, type="a", items=[
                raw.types.PageListOrderedItemText(text=plain("c")),
                raw.types.PageListOrderedItemBlocks(num="7", blocks=[raw.types.PageBlockParagraph(text=plain("x"))]),
                raw.types.PageListOrderedItemText(text=plain("roman"), value=14, type="I"),
                raw.types.PageListOrderedItemText(text=plain("next")),
            ]),
            raw.types.PageBlockOrderedList(reversed=True, items=[
                raw.types.PageListOrderedItemText(text=plain("one"), value=2),
                raw.types.PageListOrderedItemText(text=plain("zero")),
            ]),
            raw.types.PageBlockBlockquote(text=plain("quote"), caption=raw.types.TextEmpty()),
            raw.types.PageBlockBlockquote(collapsed=True, text=plain("long quote"), caption=plain("Author")),
            raw.types.PageBlockBlockquoteBlocks(blocks=[raw.types.PageBlockParagraph(text=plain("q"))],
                                                caption=plain("Credit")),
            raw.types.PageBlockPullquote(text=plain("pull"), caption=raw.types.TextEmpty()),
            raw.types.PageBlockPhoto(spoiler=True, photo_id=PHOTO_ID, caption=caption("A photo", "me")),
            raw.types.PageBlockVideo(autoplay=True, loop=True, video_id=VIDEO_ID, caption=empty_caption()),
            raw.types.PageBlockVideo(autoplay=True, loop=True, spoiler=True, video_id=ANIMATION_ID,
                                     caption=empty_caption()),
            raw.types.PageBlockAudio(audio_id=AUDIO_ID, caption=caption("A song")),
            raw.types.PageBlockAudio(audio_id=VOICE_ID, caption=empty_caption()),
            raw.types.PageBlockDocument(document_id=DOCUMENT_ID, caption=caption("A file")),
            raw.types.PageBlockDocument(document_id=999, caption=empty_caption()),
            raw.types.PageBlockCollage(items=[raw.types.PageBlockPhoto(photo_id=PHOTO_ID, caption=empty_caption())],
                                       caption=empty_caption()),
            raw.types.PageBlockTable(bordered=True, compact=True, title=plain("Table"), rows=[
                raw.types.PageTableRow(cells=[
                    raw.types.PageTableCell(header=True, align_center=True, valign_bottom=True, text=plain("h"),
                                            colspan=2),
                    raw.types.PageTableCell(),
                ]),
            ]),
            raw.types.PageBlockDetails(open=True, blocks=[raw.types.PageBlockParagraph(text=plain("in"))],
                                       title=plain("More")),
            raw.types.PageBlockMap(geo=raw.types.GeoPoint(long=1.5, lat=2.5, access_hash=0), zoom=10, w=300, h=200,
                                   caption=empty_caption()),
            raw.types.PageBlockButtonRow(align_center=True, buttons=[
                raw.types.PageButton(text=plain("Data"), type=raw.types.InlineButtonTypeCallback(data=b"cb"),
                                     style=raw.types.RichButtonStyle(bg_primary=True)),
                raw.types.PageButton(text=raw.types.TextConcat(texts=[
                    raw.types.TextCustomEmoji(document_id=8, alt="*"), plain("Copy")
                ]), type=raw.types.InlineButtonTypeCopy(copy_text="secret")),
                raw.types.PageButton(text=plain("Me"), type=raw.types.InlineButtonTypeUserProfile(user_id=7)),
            ]),
            raw.types.PageBlockUnsupported(),
            raw.types.InputPageBlockMap(geo=raw.types.InputGeoPoint(lat=1, long=2), zoom=1, w=1, h=1,
                                        caption=empty_caption()),
        ],
        photos=[raw_photo(PHOTO_ID)],
        documents=RAW_DOCUMENTS,
    ))

    assert isinstance(rich, types.RichMessage) and rich.is_rtl and not rich.is_full
    (
        heading1, heading6, paragraph, anchor, math, thinking, pre, footer, divider, bullets, ordered, reversed_list,
        quote, expandable, quote_blocks, pull, photo, video, animation, audio, voice, document, missing_document,
        collage, table, details, map_block, button_row, unsupported, input_map
    ) = rich.blocks

    assert isinstance(heading1, types.PageBlockSectionHeading) and heading1.size == 1
    assert heading1.text.text == "Title"
    assert heading6.size == 6 and isinstance(heading6.text, types.RichTextBold)
    assert isinstance(anchor, types.PageBlockAnchor) and anchor.name == "sec"
    assert isinstance(math, types.PageBlockMathematicalExpression) and math.expression == "e=mc^2"
    assert isinstance(thinking, types.PageBlockThinking) and thinking.text.text == "Thinking..."
    assert isinstance(pre, types.PageBlockPreformatted) and pre.language == "python"
    assert isinstance(footer, types.PageBlockFooter) and footer.footer.text == "bye"
    assert isinstance(divider, types.PageBlockDivider)

    texts = paragraph.text.texts
    assert isinstance(paragraph.text, types.RichTexts) and len(texts) == len(RICH_PARAGRAPH.texts)
    (
        hi, bold, underline, strike, fixed, sub, sup, marked, spoiler, mention, hashtag, cashtag, command, auto_url,
        auto_email, auto_phone, card, ton, mention_name, date, diff, formula, emoji, url, email, phone, anchor_link,
        reference_link, top_link, reference, inline_anchor, icon, button
    ) = texts
    assert isinstance(hi, types.RichTextPlain) and hi.text == "Hi "
    assert isinstance(bold, types.RichTextBold) and isinstance(bold.text, types.RichTextItalic)
    assert bold.text.text.text == "you"
    for text, cls in [(underline, types.RichTextUnderline), (strike, types.RichTextStrikethrough),
                      (fixed, types.RichTextFixed), (sub, types.RichTextSubscript), (sup, types.RichTextSuperscript),
                      (marked, types.RichTextMarked), (spoiler, types.RichTextSpoiler)]:
        assert type(text) is cls and isinstance(text.text, types.RichTextPlain)
    assert isinstance(mention, types.RichTextMention) and mention.username == "durov"
    assert isinstance(hashtag, types.RichTextHashtag) and hashtag.hashtag == "news"
    assert isinstance(cashtag, types.RichTextCashtag) and cashtag.cashtag == "USD"
    assert isinstance(command, types.RichTextBotCommand) and command.bot_command == "start"
    assert isinstance(auto_url, types.RichTextUrl) and auto_url.url == "t.me/pyrofork" and not auto_url.is_cached
    assert isinstance(auto_email, types.RichTextEmailAddress) and auto_email.email_address == "a@b.c"
    assert isinstance(auto_phone, types.RichTextPhoneNumber) and auto_phone.phone_number == "+123456"
    assert isinstance(card, types.RichTextBankCardNumber) and card.bank_card_number == "4242424242424242"
    assert isinstance(ton, types.RichTextTonAddress) and ton.address == "EQabc"
    assert isinstance(mention_name, types.RichTextMentionName) and mention_name.user_id == 7
    assert isinstance(date, types.RichTextDateTime) and date.date.timestamp() == 1647531900
    assert date.long_date and date.day_of_week and not date.relative and date.text.text == "tomorrow"
    assert isinstance(diff, types.RichTextDiff) and (diff.text.text, diff.old_text.text) == ("new", "old")
    assert isinstance(formula, types.RichTextMathematicalExpression) and formula.expression == "x^2"
    assert isinstance(emoji, types.RichTextCustomEmoji)
    assert (emoji.custom_emoji_id, emoji.alternative_text) == (5, "\U0001f600")
    assert isinstance(url, types.RichTextUrl) and url.url == "https://telegram.org" and url.is_cached
    assert isinstance(email, types.RichTextEmailAddress) and email.email_address == "me@example.org"
    assert isinstance(phone, types.RichTextPhoneNumber) and phone.phone_number == "+100"
    assert isinstance(anchor_link, types.RichTextAnchorLink) and anchor_link.anchor_name == "sec"
    assert anchor_link.url == "#sec"
    assert isinstance(reference_link, types.RichTextReferenceLink) and reference_link.reference_name == "note"
    assert isinstance(top_link, types.RichTextAnchorLink) and top_link.anchor_name == ""
    assert isinstance(reference, types.RichTextReference) and reference.name == "note"
    assert reference.text.text == "the note"
    assert isinstance(inline_anchor, types.RichTextAnchor) and inline_anchor.name == "here"
    assert isinstance(icon, types.RichTextIcon) and (icon.width, icon.height) == (16, 16)
    assert isinstance(icon.document, types.Document) and icon.document.file_name == "icon.png"
    assert isinstance(button, types.RichTextButton) and isinstance(button.button, types.InlineButton)
    assert button.button.url == "https://example.org" and button.button.style == enums.ButtonStyle.LINK
    assert button.button.text.text == "Open"

    assert isinstance(bullets, types.PageBlockList)
    done, empty_item = bullets.items
    assert done.label == "•" and done.has_checkbox and done.is_checked and done.value == 0 and done.type == ""
    assert done.blocks[0].text.text == "done"
    # "checked" without "checkbox" isn't a checked item, and an empty item gets an empty paragraph
    assert not empty_item.has_checkbox and not empty_item.is_checked
    assert isinstance(empty_item.blocks[0], types.PageBlockParagraph) and empty_item.blocks[0].text.text == ""

    assert [(i.label, i.value, i.type) for i in ordered.items] == [
        ("c.", 3, "a"), ("7.", 4, "a"), ("XIV.", 14, "I"), ("o.", 15, "a")
    ]
    assert ordered.items[1].blocks[0].text.text == "x"
    assert [(i.label, i.value, i.type) for i in reversed_list.items] == [("2.", 2, "1"), ("1.", 1, "1")]

    assert isinstance(quote, types.PageBlockBlockQuote) and quote.credit is None
    assert quote.blocks[0].text.text == "quote"
    assert isinstance(expandable, types.PageBlockExpandableBlockQuote) and expandable.credit.text == "Author"
    assert isinstance(quote_blocks, types.PageBlockBlockQuote) and quote_blocks.credit.text == "Credit"
    assert quote_blocks.blocks[0].text.text == "q"
    assert isinstance(pull, types.PageBlockPullQuote) and pull.credit is None

    assert isinstance(photo, types.PageBlockPhoto) and photo.has_spoiler and photo.url is None
    assert isinstance(photo.photo, types.Photo) and photo.photo.width == 320
    assert photo.caption.text.text == "A photo" and photo.caption.credit.text == "me"
    assert isinstance(video, types.PageBlockVideo) and video.need_autoplay and video.is_looped
    assert isinstance(video.video, types.Video) and video.video.duration == 3 and video.caption is None
    assert isinstance(animation, types.PageBlockAnimation) and animation.need_autoplay and animation.has_spoiler
    assert isinstance(animation.animation, types.Animation)
    assert isinstance(audio, types.PageBlockAudio) and audio.audio.title == "Song" and audio.audio.performer == "Band"
    assert audio.caption.text.text == "A song" and audio.caption.credit is None
    assert isinstance(voice, types.PageBlockVoiceNote) and isinstance(voice.voice_note, types.Voice)
    assert voice.voice_note.duration == 5
    assert isinstance(document, types.PageBlockDocument) and document.document.file_name == "report.pdf"
    assert isinstance(missing_document, types.PageBlockUnsupported)
    assert isinstance(collage, types.PageBlockCollage) and isinstance(collage.blocks[0].photo, types.Photo)

    assert isinstance(table, types.PageBlockTable) and table.is_bordered and table.is_compact and not table.is_striped
    assert table.caption.text == "Table"
    header, hidden = table.cells[0]
    assert header.is_header and header.text.text == "h" and (header.colspan, header.rowspan) == (2, 1)
    assert (header.align, header.valign) == (HA.CENTER, VA.BOTTOM)
    assert hidden.text is None and (hidden.align, hidden.valign) == (HA.LEFT, VA.TOP)

    assert isinstance(details, types.PageBlockDetails) and details.is_open and details.header.text == "More"
    assert details.blocks[0].text.text == "in"

    assert isinstance(map_block, types.PageBlockMap) and map_block.caption is None
    assert (map_block.location.latitude, map_block.location.longitude) == (2.5, 1.5)
    assert (map_block.zoom, map_block.width, map_block.height) == (10, 300, 200)

    assert isinstance(button_row, types.PageBlockButtonRow) and button_row.align == HA.CENTER
    data, copy, profile = button_row.buttons
    assert data.callback_data == "cb" and data.style == enums.ButtonStyle.PRIMARY and data.text.text == "Data"
    assert copy.copy_text == "secret" and copy.style is None
    assert isinstance(copy.text, types.RichTexts) and isinstance(copy.text.texts[0], types.RichTextCustomEmoji)
    assert profile.user_id == 7

    assert isinstance(unsupported, types.PageBlockUnsupported)
    assert isinstance(input_map, types.PageBlockUnsupported)


def test_received_instant_view_blocks():
    channel = raw.types.Channel(id=123, access_hash=1, title="News", username="news", photo=raw.types.ChatPhotoEmpty(),
                                date=0, broadcast=True)
    rich = parse_rich_message(raw.types.RichMessage(
        blocks=[
            raw.types.PageBlockTitle(text=plain("T")),
            raw.types.PageBlockSubtitle(text=plain("S")),
            raw.types.PageBlockAuthorDate(author=plain("A"), published_date=1700000000),
            raw.types.PageBlockHeader(text=plain("H")),
            raw.types.PageBlockSubheader(text=plain("SH")),
            raw.types.PageBlockKicker(text=plain("K")),
            raw.types.PageBlockCover(cover=raw.types.PageBlockPhoto(photo_id=PHOTO_ID, caption=empty_caption(),
                                                                    url="https://t.me/x", webpage_id=1)),
            raw.types.PageBlockEmbed(full_width=True, url="https://e.org", poster_photo_id=PHOTO_ID, w=10, h=20,
                                     caption=caption("embed")),
            raw.types.PageBlockEmbedPost(url="https://p.org", webpage_id=0, author_photo_id=0, author="Bob", date=0,
                                         blocks=[raw.types.PageBlockParagraph(text=plain("post"))],
                                         caption=empty_caption()),
            raw.types.PageBlockSlideshow(items=[], caption=empty_caption()),
            raw.types.PageBlockChannel(channel=channel),
            raw.types.PageBlockRelatedArticles(title=plain("Related"), articles=[
                raw.types.PageRelatedArticle(url="https://r.org", webpage_id=0, title="R", photo_id=PHOTO_ID,
                                             published_date=1700000000),
            ]),
        ],
        photos=[raw_photo(PHOTO_ID)],
        documents=[],
    ))

    title, subtitle, author_date, header, subheader, kicker, cover, embed, post, slideshow, chat_link, related = rich.blocks
    assert rich.is_full and not rich.is_rtl
    assert title.title.text == "T" and subtitle.subtitle.text == "S" and author_date.author.text == "A"
    assert author_date.publish_date.timestamp() == 1700000000
    assert header.header.text == "H" and subheader.subheader.text == "SH" and kicker.kicker.text == "K"
    assert isinstance(cover.cover, types.PageBlockPhoto) and cover.cover.url == "https://t.me/x"
    assert isinstance(embed, types.PageBlockEmbedded) and embed.is_full_width and not embed.allow_scrolling
    assert isinstance(embed.poster_photo, types.Photo) and (embed.width, embed.height) == (10, 20)
    assert embed.caption.text.text == "embed"
    assert isinstance(post, types.PageBlockEmbeddedPost) and post.author == "Bob" and post.author_photo is None
    assert post.date is None and post.blocks[0].text.text == "post"
    assert isinstance(slideshow, types.PageBlockSlideshow) and slideshow.blocks == []
    assert isinstance(chat_link, types.PageBlockChatLink) and chat_link.chat.title == "News"
    assert chat_link.chat.id == pyrogram.utils.get_channel_id(123)
    [article] = related.articles
    assert related.header.text == "Related" and article.title == "R" and isinstance(article.photo, types.Photo)
    assert article.publish_date.timestamp() == 1700000000 and article.description is None


def test_received_message_rich_message_field():
    """messages carry the rich message in message.rich_message (flags2.13)."""
    message = roundtrip(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), date=1, message="",
        rich_message=raw.types.RichMessage(blocks=[raw.types.PageBlockParagraph(text=plain("hi"))],
                                           photos=[], documents=[])
    ))
    rich = types.RichMessage._parse(offline_parser_client(), message.rich_message)
    assert rich.blocks[0].text.text == "hi" and rich.is_full
    assert types.RichMessage._parse(None, None) is None
