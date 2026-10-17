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

import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from urllib.parse import quote, unquote

import pyrogram
from pyrogram import raw, types, utils
from pyrogram.raw.core import TLObject
from ..object import Object

log = logging.getLogger(__name__)


class RichMessageParseContext:
    """Media and anchors of a rich message, shared by all its blocks and texts while they are parsed.

    Not a public type: :meth:`~pyrogram.types.RichMessage._parse` builds it.
    """

    def __init__(
        self,
        client: "pyrogram.Client" = None,
        photos: Dict[int, "raw.types.Photo"] = None,
        documents: Dict[int, "raw.types.Document"] = None,
        anchors: Dict[str, bool] = None
    ):
        self.client = client
        self.photos = photos or {}
        self.documents = documents or {}
        # anchor name -> True if the anchor has a text (a reference), False otherwise
        self.anchors = anchors or {}


def is_empty_raw_rich_text(rich_text: "raw.base.RichText") -> bool:
    """Whether a raw rich text is empty (TDLib's RichText::empty())."""
    return (
        rich_text is None
        or isinstance(rich_text, raw.types.TextEmpty)
        or (isinstance(rich_text, raw.types.TextPlain) and not rich_text.text)
    )


def get_raw_full_text(rich_text: "raw.base.RichText") -> str:
    """The plain text of a raw rich text, as TDLib's RichText::get_full_text() computes it."""
    if isinstance(rich_text, raw.types.TextPlain):
        return rich_text.text
    if isinstance(rich_text, raw.types.TextMath):
        return rich_text.source
    if isinstance(rich_text, raw.types.TextCustomEmoji):
        return rich_text.alt
    if isinstance(rich_text, raw.types.TextConcat):
        return "".join(get_raw_full_text(text) for text in rich_text.texts)

    return "".join(
        get_raw_full_text(getattr(rich_text, name))
        for name in ("text", "old_text")
        if isinstance(getattr(rich_text, name, None), TLObject)
    )


def trim_first(text: str, char: str) -> str:
    return text[1:] if text.startswith(char) else text


def collect_anchors(obj, anchors: Dict[str, bool]):
    """Collect the anchors of raw page blocks and rich texts (pageBlockAnchor and textAnchor)."""
    if isinstance(obj, list):
        for item in obj:
            collect_anchors(item, anchors)
        return

    if not isinstance(obj, TLObject):
        return

    if isinstance(obj, raw.types.PageBlockAnchor):
        anchors.setdefault(obj.name, False)
    elif isinstance(obj, raw.types.TextAnchor):
        anchors.setdefault(obj.name, not is_empty_raw_rich_text(obj.text))

    for name in obj.__slots__:
        collect_anchors(getattr(obj, name, None), anchors)


async def write_rich_text(writer, text: Optional[Union["RichText", str]]) -> "raw.base.RichText":
    """Write an optional rich text; a ``str`` is written as :obj:`~pyrogram.types.RichTextPlain`."""
    if text is None:
        return raw.types.TextEmpty()

    if isinstance(text, str):
        text = RichTextPlain(text)

    return await text.write(writer)


async def write_optional_rich_text(writer, text: Optional[Union["RichText", str]]) -> Optional["raw.base.RichText"]:
    return None if text is None else await write_rich_text(writer, text)


class RichText(Object):
    """A formatted text of a rich message.

    It can be one of:

    - :obj:`~pyrogram.types.RichTextPlain`
    - :obj:`~pyrogram.types.RichTextBold`
    - :obj:`~pyrogram.types.RichTextItalic`
    - :obj:`~pyrogram.types.RichTextUnderline`
    - :obj:`~pyrogram.types.RichTextStrikethrough`
    - :obj:`~pyrogram.types.RichTextSpoiler`
    - :obj:`~pyrogram.types.RichTextSubscript`
    - :obj:`~pyrogram.types.RichTextSuperscript`
    - :obj:`~pyrogram.types.RichTextMarked`
    - :obj:`~pyrogram.types.RichTextDateTime`
    - :obj:`~pyrogram.types.RichTextMention`
    - :obj:`~pyrogram.types.RichTextHashtag`
    - :obj:`~pyrogram.types.RichTextCashtag`
    - :obj:`~pyrogram.types.RichTextBankCardNumber`
    - :obj:`~pyrogram.types.RichTextTonAddress`
    - :obj:`~pyrogram.types.RichTextBotCommand`
    - :obj:`~pyrogram.types.RichTextFixed`
    - :obj:`~pyrogram.types.RichTextMentionName`
    - :obj:`~pyrogram.types.RichTextUrl`
    - :obj:`~pyrogram.types.RichTextEmailAddress`
    - :obj:`~pyrogram.types.RichTextPhoneNumber`
    - :obj:`~pyrogram.types.RichTextCustomEmoji`
    - :obj:`~pyrogram.types.RichTextIcon`
    - :obj:`~pyrogram.types.RichTextMathematicalExpression`
    - :obj:`~pyrogram.types.RichTextButton`
    - :obj:`~pyrogram.types.RichTextDiff`
    - :obj:`~pyrogram.types.RichTextReference`
    - :obj:`~pyrogram.types.RichTextReferenceLink`
    - :obj:`~pyrogram.types.RichTextAnchor`
    - :obj:`~pyrogram.types.RichTextAnchorLink`
    - :obj:`~pyrogram.types.RichTexts`

    Wherever a rich text is expected when sending a rich message, a ``str`` can be passed instead of
    :obj:`~pyrogram.types.RichTextPlain`.
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        rich_text: "raw.base.RichText",
        context: RichMessageParseContext = None
    ) -> Optional["RichText"]:
        if rich_text is None:
            return None

        if context is None:
            context = RichMessageParseContext(client)

        def parse(text):
            return RichText._parse(client, text, context)

        if isinstance(rich_text, raw.types.TextEmpty):
            return RichTextPlain("")

        if isinstance(rich_text, raw.types.TextPlain):
            return RichTextPlain(rich_text.text)

        simple = {
            raw.types.TextBold: RichTextBold,
            raw.types.TextItalic: RichTextItalic,
            raw.types.TextUnderline: RichTextUnderline,
            raw.types.TextStrike: RichTextStrikethrough,
            raw.types.TextSpoiler: RichTextSpoiler,
            raw.types.TextSubscript: RichTextSubscript,
            raw.types.TextSuperscript: RichTextSuperscript,
            raw.types.TextMarked: RichTextMarked,
            raw.types.TextFixed: RichTextFixed,
        }.get(type(rich_text))

        if simple is not None:
            return simple(parse(rich_text.text))

        if isinstance(rich_text, raw.types.TextConcat):
            return RichTexts([parse(text) for text in rich_text.texts])

        if isinstance(rich_text, raw.types.TextUrl):
            url = rich_text.url

            if url.startswith("#"):
                anchor = url[1:]

                for name in (anchor, unquote(anchor)):
                    if name in context.anchors:
                        if context.anchors[name]:
                            return RichTextReferenceLink(parse(rich_text.text), reference_name=name, url=url)

                        return RichTextAnchorLink(parse(rich_text.text), anchor_name=name, url=url)

            return RichTextUrl(parse(rich_text.text), url=url, is_cached=bool(rich_text.webpage_id))

        if isinstance(rich_text, raw.types.TextEmail):
            return RichTextEmailAddress(parse(rich_text.text), email_address=rich_text.email)

        if isinstance(rich_text, raw.types.TextPhone):
            return RichTextPhoneNumber(parse(rich_text.text), phone_number=rich_text.phone)

        if isinstance(rich_text, raw.types.TextImage):
            document = context.documents.get(rich_text.document_id)

            return RichTextIcon(
                document=types.Document._parse(client, document, get_file_name(document)) if document is not None else None,
                width=rich_text.w,
                height=rich_text.h
            )

        if isinstance(rich_text, raw.types.TextAnchor):
            if is_empty_raw_rich_text(rich_text.text):
                return RichTextAnchor(name=rich_text.name)

            return RichTextReference(name=rich_text.name, text=parse(rich_text.text))

        if isinstance(rich_text, raw.types.TextMath):
            return RichTextMathematicalExpression(expression=rich_text.source)

        if isinstance(rich_text, raw.types.TextCustomEmoji):
            return RichTextCustomEmoji(custom_emoji_id=rich_text.document_id, alternative_text=rich_text.alt)

        full_text = get_raw_full_text(getattr(rich_text, "text", None))

        if isinstance(rich_text, raw.types.TextMention):
            return RichTextMention(parse(rich_text.text), username=trim_first(full_text, "@"))

        if isinstance(rich_text, raw.types.TextHashtag):
            return RichTextHashtag(parse(rich_text.text), hashtag=trim_first(full_text, "#"))

        if isinstance(rich_text, raw.types.TextCashtag):
            return RichTextCashtag(parse(rich_text.text), cashtag=trim_first(full_text, "$"))

        if isinstance(rich_text, raw.types.TextBotCommand):
            return RichTextBotCommand(parse(rich_text.text), bot_command=trim_first(full_text, "/"))

        if isinstance(rich_text, raw.types.TextAutoUrl):
            return RichTextUrl(parse(rich_text.text), url=full_text, is_cached=False)

        if isinstance(rich_text, raw.types.TextAutoEmail):
            return RichTextEmailAddress(parse(rich_text.text), email_address=full_text)

        if isinstance(rich_text, raw.types.TextAutoPhone):
            return RichTextPhoneNumber(parse(rich_text.text), phone_number=full_text)

        if isinstance(rich_text, raw.types.TextBankCard):
            return RichTextBankCardNumber(parse(rich_text.text), bank_card_number=full_text)

        if isinstance(rich_text, raw.types.TextTonAddress):
            return RichTextTonAddress(parse(rich_text.text), address=full_text)

        if isinstance(rich_text, raw.types.TextMentionName):
            return RichTextMentionName(parse(rich_text.text), user_id=rich_text.user_id)

        if isinstance(rich_text, raw.types.TextDate):
            return RichTextDateTime(
                parse(rich_text.text),
                date=utils.timestamp_to_datetime(rich_text.date),
                relative=rich_text.relative,
                short_time=rich_text.short_time,
                long_time=rich_text.long_time,
                short_date=rich_text.short_date,
                long_date=rich_text.long_date,
                day_of_week=rich_text.day_of_week
            )

        if isinstance(rich_text, raw.types.TextDiff):
            return RichTextDiff(parse(rich_text.text), old_text=parse(rich_text.old_text))

        if isinstance(rich_text, raw.types.TextButton):
            return RichTextButton(types.InlineButton._parse(client, rich_text, context))

        log.warning("Unknown rich text type: %s", type(rich_text).__name__)

        return RichTextPlain(full_text)

    async def write(self, writer) -> "raw.base.RichText":
        raise NotImplementedError


def get_file_name(document: "raw.types.Document") -> Optional[str]:
    for attribute in getattr(document, "attributes", []):
        if isinstance(attribute, raw.types.DocumentAttributeFilename):
            return attribute.file_name

    return None


class RichTextPlain(RichText):
    """A plain text.

    Parameters:
        text (``str``):
            Text.
    """

    def __init__(self, text: str):
        super().__init__()

        self.text = text

    async def write(self, writer) -> "raw.base.RichText":
        if not self.text:
            return raw.types.TextEmpty()

        return raw.types.TextPlain(text=self.text)


class _StyledRichText(RichText):
    RAW_TYPE = None

    def __init__(self, text: Union["RichText", str]):
        super().__init__()

        self.text = text

    async def write(self, writer) -> "raw.base.RichText":
        return self.RAW_TYPE(text=await write_rich_text(writer, self.text))


class RichTextBold(_StyledRichText):
    """A bold rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextBold


class RichTextItalic(_StyledRichText):
    """An italicized rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextItalic


class RichTextUnderline(_StyledRichText):
    """An underlined rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextUnderline


class RichTextStrikethrough(_StyledRichText):
    """A strikethrough rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextStrike


class RichTextSpoiler(_StyledRichText):
    """A spoilered rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextSpoiler


class RichTextSubscript(_StyledRichText):
    """A subscript rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextSubscript


class RichTextSuperscript(_StyledRichText):
    """A superscript rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextSuperscript


class RichTextMarked(_StyledRichText):
    """A marked rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextMarked


class RichTextFixed(_StyledRichText):
    """A fixed-width rich text.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.
    """

    RAW_TYPE = raw.types.TextFixed


class RichTextDateTime(RichText):
    """A date and time.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Original text.

        date (:py:obj:`~datetime.datetime`):
            Point in time represented by the text.

        relative (``bool``, *optional*):
            True, if the time must be shown relative to the current time.

        short_time (``bool``, *optional*):
            True, if the time must be shown in a short way (22:45).

        long_time (``bool``, *optional*):
            True, if the time must be shown in a long way (22:45:00).

        short_date (``bool``, *optional*):
            True, if the date must be shown in a short way (17.03.22).

        long_date (``bool``, *optional*):
            True, if the date must be shown in a long way (March 17, 2022).

        day_of_week (``bool``, *optional*):
            True, if the day of week must be shown.

            If none of the formatting fields is set, the original text must not be changed.
    """

    def __init__(
        self,
        text: Union["RichText", str],
        date: datetime,
        relative: bool = None,
        short_time: bool = None,
        long_time: bool = None,
        short_date: bool = None,
        long_date: bool = None,
        day_of_week: bool = None
    ):
        super().__init__()

        self.text = text
        self.date = date
        self.relative = relative
        self.short_time = short_time
        self.long_time = long_time
        self.short_date = short_date
        self.long_date = long_date
        self.day_of_week = day_of_week

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextDate(
            text=await write_rich_text(writer, self.text),
            date=utils.datetime_to_timestamp(self.date),
            relative=self.relative,
            short_time=self.short_time,
            long_time=self.long_time,
            short_date=self.short_date,
            long_date=self.long_date,
            day_of_week=self.day_of_week
        )


class _DetectedRichText(RichText):
    """A text detected by the server; only its inner text is sent (TDLib does the same)."""

    def __init__(self, text: Union["RichText", str]):
        super().__init__()

        self.text = text

    async def write(self, writer) -> "raw.base.RichText":
        return await write_rich_text(writer, self.text)


class RichTextMention(_DetectedRichText):
    """A mention of a Telegram user or chat by a username.

    Only the text is sent with a rich message: mentions are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        username (``str``):
            The username.
    """

    def __init__(self, text: Union["RichText", str], username: str = None):
        super().__init__(text)

        self.username = username


class RichTextHashtag(_DetectedRichText):
    """A hashtag.

    Only the text is sent with a rich message: hashtags are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        hashtag (``str``):
            The hashtag.
    """

    def __init__(self, text: Union["RichText", str], hashtag: str = None):
        super().__init__(text)

        self.hashtag = hashtag


class RichTextCashtag(_DetectedRichText):
    """A cashtag.

    Only the text is sent with a rich message: cashtags are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        cashtag (``str``):
            The cashtag.
    """

    def __init__(self, text: Union["RichText", str], cashtag: str = None):
        super().__init__(text)

        self.cashtag = cashtag


class RichTextBankCardNumber(_DetectedRichText):
    """A bank card number.

    Only the text is sent with a rich message: bank card numbers are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        bank_card_number (``str``):
            The number of the bank card.
    """

    def __init__(self, text: Union["RichText", str], bank_card_number: str = None):
        super().__init__(text)

        self.bank_card_number = bank_card_number


class RichTextTonAddress(_DetectedRichText):
    """An address in the TON blockchain.

    Only the text is sent with a rich message: addresses are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        address (``str``):
            The address.
    """

    def __init__(self, text: Union["RichText", str], address: str = None):
        super().__init__(text)

        self.address = address


class RichTextBotCommand(_DetectedRichText):
    """A bot command.

    Only the text is sent with a rich message: bot commands are detected by Telegram.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        bot_command (``str``):
            The bot command, without the leading "/".
    """

    def __init__(self, text: Union["RichText", str], bot_command: str = None):
        super().__init__(text)

        self.bot_command = bot_command


class RichTextMentionName(RichText):
    """A rich text that serves as a mention of a user.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        user_id (``int``):
            Identifier of the mentioned user.
    """

    def __init__(self, text: Union["RichText", str], user_id: int):
        super().__init__()

        self.text = text
        self.user_id = user_id

    async def write(self, writer) -> "raw.base.RichText":
        await writer.add_user(self.user_id)

        return raw.types.TextMentionName(text=await write_rich_text(writer, self.text), user_id=self.user_id)


class RichTextUrl(RichText):
    """A rich text URL link.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        url (``str``):
            URL.

        is_cached (``bool``, *optional*):
            True, if the URL has cached instant view server-side.
    """

    def __init__(self, text: Union["RichText", str], url: str, is_cached: bool = None):
        super().__init__()

        self.text = text
        self.url = url
        self.is_cached = is_cached

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextUrl(text=await write_rich_text(writer, self.text), url=self.url, webpage_id=0)


class RichTextEmailAddress(RichText):
    """A rich text email address.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        email_address (``str``):
            Email address.
    """

    def __init__(self, text: Union["RichText", str], email_address: str):
        super().__init__()

        self.text = text
        self.email_address = email_address

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextEmail(text=await write_rich_text(writer, self.text), email=self.email_address)


class RichTextPhoneNumber(RichText):
    """A rich text phone number.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        phone_number (``str``):
            Phone number.
    """

    def __init__(self, text: Union["RichText", str], phone_number: str):
        super().__init__()

        self.text = text
        self.phone_number = phone_number

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextPhone(text=await write_rich_text(writer, self.text), phone=self.phone_number)


class RichTextCustomEmoji(RichText):
    """A custom emoji.

    Parameters:
        custom_emoji_id (``int``):
            Unique identifier of the custom emoji.

        alternative_text (``str``):
            Alternative text for the custom emoji.
    """

    def __init__(self, custom_emoji_id: int, alternative_text: str):
        super().__init__()

        self.custom_emoji_id = custom_emoji_id
        self.alternative_text = alternative_text

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextCustomEmoji(document_id=self.custom_emoji_id, alt=self.alternative_text)


class RichTextIcon(RichText):
    """A small image inside the text; can't be sent.

    Parameters:
        document (:obj:`~pyrogram.types.Document`):
            The image represented as a document. The image can be in GIF, JPEG or PNG format.

        width (``int``):
            Width of a bounding box in which the image must be shown; 0 if unknown.

        height (``int``):
            Height of a bounding box in which the image must be shown; 0 if unknown.
    """

    def __init__(self, document: "types.Document", width: int, height: int):
        super().__init__()

        self.document = document
        self.width = width
        self.height = height

    async def write(self, writer) -> "raw.base.RichText":
        raise ValueError("RichTextIcon can't be sent in a rich message")


class RichTextMathematicalExpression(RichText):
    """A mathematical expression.

    Parameters:
        expression (``str``):
            The expression in LaTeX format.
    """

    def __init__(self, expression: str):
        super().__init__()

        self.expression = expression

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextMath(source=self.expression)


class RichTextButton(RichText):
    """A button.

    Parameters:
        button (:obj:`~pyrogram.types.InlineButton`):
            The button.
    """

    def __init__(self, button: "types.InlineButton"):
        super().__init__()

        self.button = button

    async def write(self, writer) -> "raw.base.RichText":
        text, button_type, style = await self.button.write(writer)

        return raw.types.TextButton(text=text, type=button_type, style=style)


class RichTextDiff(RichText):
    """A rich text replacing another rich text; can't be sent.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text.

        old_text (:obj:`~pyrogram.types.RichText`):
            The old text.
    """

    def __init__(self, text: Union["RichText", str], old_text: Union["RichText", str]):
        super().__init__()

        self.text = text
        self.old_text = old_text

    async def write(self, writer) -> "raw.base.RichText":
        raise ValueError("RichTextDiff can't be sent in a rich message")


class RichTextReference(RichText):
    """A reference.

    Parameters:
        name (``str``):
            Reference name.

        text (:obj:`~pyrogram.types.RichText`):
            Text of the reference.
    """

    def __init__(self, name: str, text: Union["RichText", str]):
        super().__init__()

        self.name = name
        self.text = text

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextAnchor(text=await write_rich_text(writer, self.text), name=self.name)


class RichTextReferenceLink(RichText):
    """A link to a reference in the same message.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            The link text.

        reference_name (``str``):
            The reference name.

        url (``str``, *optional*):
            A URL that opens the reference. Ignored when sending.
    """

    def __init__(self, text: Union["RichText", str], reference_name: str, url: str = None):
        super().__init__()

        self.text = text
        self.reference_name = reference_name
        self.url = url

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextUrl(
            text=await write_rich_text(writer, self.text),
            url="#" + quote(self.reference_name, safe=""),
            webpage_id=0
        )


class RichTextAnchor(RichText):
    """An anchor.

    Parameters:
        name (``str``):
            Anchor name.
    """

    def __init__(self, name: str):
        super().__init__()

        self.name = name

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextAnchor(text=raw.types.TextEmpty(), name=self.name)


class RichTextAnchorLink(RichText):
    """A link to an anchor in the same message.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            The link text.

        anchor_name (``str``):
            The anchor name. If the name is empty, the link must bring back to top.

        url (``str``, *optional*):
            A URL that opens the anchor. Ignored when sending.
    """

    def __init__(self, text: Union["RichText", str], anchor_name: str, url: str = None):
        super().__init__()

        self.text = text
        self.anchor_name = anchor_name
        self.url = url

    async def write(self, writer) -> "raw.base.RichText":
        return raw.types.TextUrl(
            text=await write_rich_text(writer, self.text),
            url="#" + quote(self.anchor_name, safe=""),
            webpage_id=0
        )


class RichTexts(RichText):
    """A concatenation of rich texts.

    Parameters:
        texts (List of :obj:`~pyrogram.types.RichText`):
            Texts.
    """

    def __init__(self, texts: List[Union["RichText", str]]):
        super().__init__()

        self.texts = texts

    async def write(self, writer) -> "raw.base.RichText":
        if not self.texts:
            return raw.types.TextEmpty()

        return raw.types.TextConcat(texts=[await write_rich_text(writer, text) for text in self.texts])
