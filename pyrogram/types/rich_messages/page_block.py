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
from typing import List, Optional, Union

import pyrogram
from pyrogram import enums, raw, types, utils
from ..object import Object
from .page_block_caption import PageBlockCaption, parse_optional_rich_text
from .page_block_list_item import PageBlockListItem
from .page_block_related_article import PageBlockRelatedArticle
from .page_block_table_cell import PageBlockTableCell
from .rich_text import RichMessageParseContext, RichText, RichTextPlain, get_file_name

log = logging.getLogger(__name__)


def parse_rich_message_document(
    client: "pyrogram.Client",
    document: "raw.types.Document"
) -> Optional[Union["types.Animation", "types.Audio", "types.Document", "types.Video", "types.Voice"]]:
    """Parse a document of a rich message into the media type its attributes describe."""
    if not isinstance(document, raw.types.Document):
        return None

    attributes = {type(i): i for i in document.attributes}
    file_name = get_file_name(document)

    if raw.types.DocumentAttributeAnimated in attributes:
        return types.Animation._parse(client, document, attributes.get(raw.types.DocumentAttributeVideo), file_name)

    if raw.types.DocumentAttributeVideo in attributes:
        return types.Video._parse(client, document, attributes[raw.types.DocumentAttributeVideo], file_name)

    audio_attributes = attributes.get(raw.types.DocumentAttributeAudio)

    if audio_attributes is not None:
        if audio_attributes.voice:
            return types.Voice._parse(client, document, audio_attributes)

        return types.Audio._parse(client, document, audio_attributes, file_name)

    return types.Document._parse(client, document, file_name)


def get_ordered_list_type(list_type: Optional[str]) -> str:
    return list_type if list_type and len(list_type) == 1 and list_type in "aAiI1" else "1"


def get_ordered_list_label(number: int, list_type: str) -> str:
    """The label of an item of an ordered list, as TDLib computes it (get_ordered_list_label)."""
    if list_type in ("a", "A") and number > 0:
        result = ""

        while number > 0:
            number -= 1
            result = chr(ord("A") + number % 26) + result
            number //= 26

        return (result.lower() if list_type == "a" else result) + "."

    if list_type in ("i", "I") and 0 < number < 4000:
        result = ""

        for value, numeral in (
            (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"), (90, "XC"),
            (50, "L"), (40, "XL"), (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")
        ):
            while number >= value:
                result += numeral
                number -= value

        return (result.lower() if list_type == "i" else result) + "."

    return f"{number}."


class PageBlock(Object):
    """A block of a rich message.

    It can be one of:

    - :obj:`~pyrogram.types.PageBlockTitle`
    - :obj:`~pyrogram.types.PageBlockSubtitle`
    - :obj:`~pyrogram.types.PageBlockAuthorDate`
    - :obj:`~pyrogram.types.PageBlockHeader`
    - :obj:`~pyrogram.types.PageBlockSubheader`
    - :obj:`~pyrogram.types.PageBlockSectionHeading`
    - :obj:`~pyrogram.types.PageBlockKicker`
    - :obj:`~pyrogram.types.PageBlockParagraph`
    - :obj:`~pyrogram.types.PageBlockPreformatted`
    - :obj:`~pyrogram.types.PageBlockFooter`
    - :obj:`~pyrogram.types.PageBlockThinking`
    - :obj:`~pyrogram.types.PageBlockDivider`
    - :obj:`~pyrogram.types.PageBlockMathematicalExpression`
    - :obj:`~pyrogram.types.PageBlockAnchor`
    - :obj:`~pyrogram.types.PageBlockList`
    - :obj:`~pyrogram.types.PageBlockBlockQuote`
    - :obj:`~pyrogram.types.PageBlockExpandableBlockQuote`
    - :obj:`~pyrogram.types.PageBlockPullQuote`
    - :obj:`~pyrogram.types.PageBlockAnimation`
    - :obj:`~pyrogram.types.PageBlockAudio`
    - :obj:`~pyrogram.types.PageBlockDocument`
    - :obj:`~pyrogram.types.PageBlockPhoto`
    - :obj:`~pyrogram.types.PageBlockVideo`
    - :obj:`~pyrogram.types.PageBlockVoiceNote`
    - :obj:`~pyrogram.types.PageBlockCover`
    - :obj:`~pyrogram.types.PageBlockEmbedded`
    - :obj:`~pyrogram.types.PageBlockEmbeddedPost`
    - :obj:`~pyrogram.types.PageBlockCollage`
    - :obj:`~pyrogram.types.PageBlockSlideshow`
    - :obj:`~pyrogram.types.PageBlockChatLink`
    - :obj:`~pyrogram.types.PageBlockTable`
    - :obj:`~pyrogram.types.PageBlockDetails`
    - :obj:`~pyrogram.types.PageBlockRelatedArticles`
    - :obj:`~pyrogram.types.PageBlockMap`
    - :obj:`~pyrogram.types.PageBlockButtonRow`
    - :obj:`~pyrogram.types.PageBlockUnsupported`

    To send blocks, use the :obj:`~pyrogram.types.InputPageBlock` types.
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def _parse_blocks(
        client: "pyrogram.Client",
        blocks: List["raw.base.PageBlock"],
        context: RichMessageParseContext = None
    ) -> List["PageBlock"]:
        return [PageBlock._parse(client, block, context) for block in blocks or []]

    @staticmethod
    def _parse(
        client: "pyrogram.Client",
        block: "raw.base.PageBlock",
        context: RichMessageParseContext = None
    ) -> "PageBlock":
        if context is None:
            context = RichMessageParseContext(client)

        def text(rich_text):
            return RichText._parse(client, rich_text, context)

        def caption(page_caption):
            return PageBlockCaption._parse(client, page_caption, context)

        def credit(rich_text):
            return parse_optional_rich_text(client, rich_text, context)

        def blocks(page_blocks):
            return PageBlock._parse_blocks(client, page_blocks, context)

        def photo(photo_id):
            raw_photo = context.photos.get(photo_id) if photo_id else None
            return types.Photo._parse(client, raw_photo) if raw_photo else None

        def document(document_id):
            return parse_rich_message_document(client, context.documents.get(document_id))

        def date(timestamp):
            return utils.timestamp_to_datetime(timestamp) if timestamp else None

        single_text = {
            raw.types.PageBlockTitle: lambda b: PageBlockTitle(title=text(b.text)),
            raw.types.PageBlockSubtitle: lambda b: PageBlockSubtitle(subtitle=text(b.text)),
            raw.types.PageBlockHeader: lambda b: PageBlockHeader(header=text(b.text)),
            raw.types.PageBlockSubheader: lambda b: PageBlockSubheader(subheader=text(b.text)),
            raw.types.PageBlockKicker: lambda b: PageBlockKicker(kicker=text(b.text)),
            raw.types.PageBlockParagraph: lambda b: PageBlockParagraph(text=text(b.text)),
            raw.types.PageBlockFooter: lambda b: PageBlockFooter(footer=text(b.text)),
            raw.types.PageBlockThinking: lambda b: PageBlockThinking(text=text(b.text)),
            raw.types.PageBlockHeading1: lambda b: PageBlockSectionHeading(text=text(b.text), size=1),
            raw.types.PageBlockHeading2: lambda b: PageBlockSectionHeading(text=text(b.text), size=2),
            raw.types.PageBlockHeading3: lambda b: PageBlockSectionHeading(text=text(b.text), size=3),
            raw.types.PageBlockHeading4: lambda b: PageBlockSectionHeading(text=text(b.text), size=4),
            raw.types.PageBlockHeading5: lambda b: PageBlockSectionHeading(text=text(b.text), size=5),
            raw.types.PageBlockHeading6: lambda b: PageBlockSectionHeading(text=text(b.text), size=6),
        }.get(type(block))

        if single_text is not None:
            return single_text(block)

        if isinstance(block, raw.types.PageBlockAuthorDate):
            return PageBlockAuthorDate(author=text(block.author), publish_date=date(block.published_date))

        if isinstance(block, raw.types.PageBlockPreformatted):
            return PageBlockPreformatted(text=text(block.text), language=block.language)

        if isinstance(block, raw.types.PageBlockDivider):
            return PageBlockDivider()

        if isinstance(block, raw.types.PageBlockMath):
            return PageBlockMathematicalExpression(expression=block.source)

        if isinstance(block, raw.types.PageBlockAnchor):
            return PageBlockAnchor(name=block.name)

        if isinstance(block, raw.types.PageBlockList):
            items = []

            for item in block.items:
                item_blocks = (
                    [PageBlockParagraph(text=text(item.text))]
                    if isinstance(item, raw.types.PageListItemText)
                    else blocks(item.blocks)
                ) or [PageBlockParagraph(text=RichTextPlain(""))]

                items.append(PageBlockListItem(
                    label="•",
                    blocks=item_blocks,
                    has_checkbox=bool(item.checkbox),
                    is_checked=bool(item.checkbox and item.checked)
                ))

            return PageBlockList(items=items)

        if isinstance(block, raw.types.PageBlockOrderedList):
            next_value = block.start if block.start is not None else 1
            base_type = get_ordered_list_type(block.type)
            items = []

            for item in block.items:
                item_blocks = (
                    [PageBlockParagraph(text=text(item.text))]
                    if isinstance(item, raw.types.PageListOrderedItemText)
                    else blocks(item.blocks)
                ) or [PageBlockParagraph(text=RichTextPlain(""))]
                value = item.value if item.value is not None else next_value
                item_type = get_ordered_list_type(item.type) if item.type else base_type

                items.append(PageBlockListItem(
                    label=item.num + "." if item.num else get_ordered_list_label(value, item_type),
                    blocks=item_blocks,
                    has_checkbox=bool(item.checkbox),
                    is_checked=bool(item.checkbox and item.checked),
                    value=value,
                    type=item_type
                ))

                next_value = value + (-1 if block.reversed else 1)

            return PageBlockList(items=items)

        if isinstance(block, raw.types.PageBlockBlockquote):
            if block.collapsed:
                return PageBlockExpandableBlockQuote(text=text(block.text), credit=credit(block.caption))

            return PageBlockBlockQuote(blocks=[PageBlockParagraph(text=text(block.text))], credit=credit(block.caption))

        if isinstance(block, raw.types.PageBlockBlockquoteBlocks):
            return PageBlockBlockQuote(blocks=blocks(block.blocks), credit=credit(block.caption))

        if isinstance(block, raw.types.PageBlockPullquote):
            return PageBlockPullQuote(text=text(block.text), credit=credit(block.caption))

        if isinstance(block, raw.types.PageBlockPhoto):
            return PageBlockPhoto(
                photo=photo(block.photo_id),
                caption=caption(block.caption),
                url=block.url,
                has_spoiler=block.spoiler
            )

        if isinstance(block, raw.types.PageBlockVideo):
            media = document(block.video_id)

            if isinstance(media, types.Animation):
                return PageBlockAnimation(
                    animation=media,
                    caption=caption(block.caption),
                    need_autoplay=block.autoplay,
                    has_spoiler=block.spoiler
                )

            return PageBlockVideo(
                video=media if isinstance(media, types.Video) else None,
                caption=caption(block.caption),
                need_autoplay=block.autoplay,
                is_looped=block.loop,
                has_spoiler=block.spoiler
            )

        if isinstance(block, raw.types.PageBlockAudio):
            media = document(block.audio_id)

            if isinstance(media, types.Voice):
                return PageBlockVoiceNote(voice_note=media, caption=caption(block.caption))

            return PageBlockAudio(
                audio=media if isinstance(media, types.Audio) else None,
                caption=caption(block.caption)
            )

        if isinstance(block, raw.types.PageBlockDocument):
            raw_document = context.documents.get(block.document_id)

            if not isinstance(raw_document, raw.types.Document):
                return PageBlockUnsupported()

            return PageBlockDocument(
                document=types.Document._parse(client, raw_document, get_file_name(raw_document)),
                caption=caption(block.caption)
            )

        if isinstance(block, raw.types.PageBlockCover):
            return PageBlockCover(cover=PageBlock._parse(client, block.cover, context))

        if isinstance(block, raw.types.PageBlockEmbed):
            return PageBlockEmbedded(
                url=block.url,
                html=block.html,
                poster_photo=photo(block.poster_photo_id),
                width=block.w,
                height=block.h,
                caption=caption(block.caption),
                is_full_width=block.full_width,
                allow_scrolling=block.allow_scrolling
            )

        if isinstance(block, raw.types.PageBlockEmbedPost):
            return PageBlockEmbeddedPost(
                url=block.url,
                author=block.author,
                author_photo=photo(block.author_photo_id),
                date=date(block.date),
                blocks=blocks(block.blocks),
                caption=caption(block.caption)
            )

        if isinstance(block, raw.types.PageBlockCollage):
            return PageBlockCollage(blocks=blocks(block.items), caption=caption(block.caption))

        if isinstance(block, raw.types.PageBlockSlideshow):
            return PageBlockSlideshow(blocks=blocks(block.items), caption=caption(block.caption))

        if isinstance(block, raw.types.PageBlockChannel):
            return PageBlockChatLink(chat=types.Chat._parse_chat(client, block.channel))

        if isinstance(block, raw.types.PageBlockTable):
            return PageBlockTable(
                caption=credit(block.title),
                cells=[[PageBlockTableCell._parse(client, cell, context) for cell in row.cells] for row in block.rows],
                is_bordered=block.bordered,
                is_striped=block.striped,
                is_compact=block.compact
            )

        if isinstance(block, raw.types.PageBlockDetails):
            return PageBlockDetails(header=text(block.title), blocks=blocks(block.blocks), is_open=block.open)

        if isinstance(block, raw.types.PageBlockRelatedArticles):
            return PageBlockRelatedArticles(
                header=text(block.title),
                articles=[PageBlockRelatedArticle._parse(client, article, context) for article in block.articles]
            )

        if isinstance(block, raw.types.PageBlockMap):
            return PageBlockMap(
                location=types.Location._parse(client, block.geo),
                zoom=block.zoom,
                width=block.w,
                height=block.h,
                caption=caption(block.caption)
            )

        if isinstance(block, raw.types.PageBlockButtonRow):
            if block.align_left:
                align = enums.PageBlockHorizontalAlignment.LEFT
            elif block.align_center:
                align = enums.PageBlockHorizontalAlignment.CENTER
            elif block.align_right:
                align = enums.PageBlockHorizontalAlignment.RIGHT
            else:
                align = None

            return PageBlockButtonRow(
                buttons=[types.InlineButton._parse(client, button, context) for button in block.buttons],
                align=align
            )

        if not isinstance(block, raw.types.PageBlockUnsupported):
            log.warning("Unknown page block type: %s", type(block).__name__)

        return PageBlockUnsupported()


class PageBlockTitle(PageBlock):
    """The title of a page; instant view only.

    Parameters:
        title (:obj:`~pyrogram.types.RichText`):
            Title.
    """

    def __init__(self, *, title: "RichText"):
        super().__init__()

        self.title = title


class PageBlockSubtitle(PageBlock):
    """The subtitle of a page; instant view only.

    Parameters:
        subtitle (:obj:`~pyrogram.types.RichText`):
            Subtitle.
    """

    def __init__(self, *, subtitle: "RichText"):
        super().__init__()

        self.subtitle = subtitle


class PageBlockAuthorDate(PageBlock):
    """The author and publishing date of a page; instant view only.

    Parameters:
        author (:obj:`~pyrogram.types.RichText`):
            Author.

        publish_date (:py:obj:`~datetime.datetime`, *optional*):
            Date the article was published.
    """

    def __init__(self, *, author: "RichText", publish_date: datetime = None):
        super().__init__()

        self.author = author
        self.publish_date = publish_date


class PageBlockHeader(PageBlock):
    """A header; instant view only.

    Parameters:
        header (:obj:`~pyrogram.types.RichText`):
            Header.
    """

    def __init__(self, *, header: "RichText"):
        super().__init__()

        self.header = header


class PageBlockSubheader(PageBlock):
    """A subheader; instant view only.

    Parameters:
        subheader (:obj:`~pyrogram.types.RichText`):
            Subheader.
    """

    def __init__(self, *, subheader: "RichText"):
        super().__init__()

        self.subheader = subheader


class PageBlockSectionHeading(PageBlock):
    """A section heading.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text of the section heading.

        size (``int``):
            Relative size of the text font; 1-6, 1 is the largest, 6 is the smallest.
    """

    def __init__(self, *, text: "RichText", size: int):
        super().__init__()

        self.text = text
        self.size = size


class PageBlockKicker(PageBlock):
    """A kicker; instant view only.

    Parameters:
        kicker (:obj:`~pyrogram.types.RichText`):
            Kicker.
    """

    def __init__(self, *, kicker: "RichText"):
        super().__init__()

        self.kicker = kicker


class PageBlockParagraph(PageBlock):
    """A text paragraph.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Paragraph text.
    """

    def __init__(self, *, text: "RichText"):
        super().__init__()

        self.text = text


class PageBlockPreformatted(PageBlock):
    """A preformatted text paragraph.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Paragraph text.

        language (``str``):
            Programming language for which the text needs to be formatted.
    """

    def __init__(self, *, text: "RichText", language: str):
        super().__init__()

        self.text = text
        self.language = language


class PageBlockFooter(PageBlock):
    """The footer of a page.

    Parameters:
        footer (:obj:`~pyrogram.types.RichText`):
            Footer.
    """

    def __init__(self, *, footer: "RichText"):
        super().__init__()

        self.footer = footer


class PageBlockThinking(PageBlock):
    """A "Thinking..." placeholder; for pending rich messages only.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text of the placeholder.
    """

    def __init__(self, *, text: "RichText"):
        super().__init__()

        self.text = text


class PageBlockDivider(PageBlock):
    """An empty block separating a page."""

    def __init__(self):
        super().__init__()


class PageBlockMathematicalExpression(PageBlock):
    """A mathematical expression.

    Parameters:
        expression (``str``):
            The expression in LaTeX format.
    """

    def __init__(self, *, expression: str):
        super().__init__()

        self.expression = expression


class PageBlockAnchor(PageBlock):
    """An invisible anchor on a page, which can be used in a URL to open the page from the specified anchor.

    Parameters:
        name (``str``):
            Name of the anchor.
    """

    def __init__(self, *, name: str):
        super().__init__()

        self.name = name


class PageBlockList(PageBlock):
    """A list of data blocks.

    Parameters:
        items (List of :obj:`~pyrogram.types.PageBlockListItem`):
            The items of the list.
    """

    def __init__(self, *, items: List["PageBlockListItem"]):
        super().__init__()

        self.items = items


class PageBlockBlockQuote(PageBlock):
    """A block quote.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Quote blocks.

        credit (:obj:`~pyrogram.types.RichText`, *optional*):
            Quote credit.
    """

    def __init__(self, *, blocks: List["PageBlock"], credit: "RichText" = None):
        super().__init__()

        self.blocks = blocks
        self.credit = credit


class PageBlockExpandableBlockQuote(PageBlock):
    """An expandable block quote.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Text of the quote.

        credit (:obj:`~pyrogram.types.RichText`, *optional*):
            Quote credit.
    """

    def __init__(self, *, text: "RichText", credit: "RichText" = None):
        super().__init__()

        self.text = text
        self.credit = credit


class PageBlockPullQuote(PageBlock):
    """A pull quote.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Quote text.

        credit (:obj:`~pyrogram.types.RichText`, *optional*):
            Quote credit.
    """

    def __init__(self, *, text: "RichText", credit: "RichText" = None):
        super().__init__()

        self.text = text
        self.credit = credit


class PageBlockAnimation(PageBlock):
    """An animation.

    Parameters:
        animation (:obj:`~pyrogram.types.Animation`, *optional*):
            Animation file.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Animation caption.

        need_autoplay (``bool``, *optional*):
            True, if the animation must be played automatically.

        has_spoiler (``bool``, *optional*):
            True, if the animation preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        *,
        animation: "types.Animation" = None,
        caption: "PageBlockCaption" = None,
        need_autoplay: bool = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.animation = animation
        self.caption = caption
        self.need_autoplay = need_autoplay
        self.has_spoiler = has_spoiler


class PageBlockAudio(PageBlock):
    """An audio file.

    Parameters:
        audio (:obj:`~pyrogram.types.Audio`, *optional*):
            Audio file.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Audio file caption.
    """

    def __init__(self, *, audio: "types.Audio" = None, caption: "PageBlockCaption" = None):
        super().__init__()

        self.audio = audio
        self.caption = caption


class PageBlockDocument(PageBlock):
    """A general file.

    Parameters:
        document (:obj:`~pyrogram.types.Document`):
            The file.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            File caption.
    """

    def __init__(self, *, document: "types.Document", caption: "PageBlockCaption" = None):
        super().__init__()

        self.document = document
        self.caption = caption


class PageBlockPhoto(PageBlock):
    """A photo.

    Parameters:
        photo (:obj:`~pyrogram.types.Photo`, *optional*):
            Photo file.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Photo caption.

        url (``str``, *optional*):
            URL that needs to be opened when the photo is clicked; instant view only.

        has_spoiler (``bool``, *optional*):
            True, if the photo preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        *,
        photo: "types.Photo" = None,
        caption: "PageBlockCaption" = None,
        url: str = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.photo = photo
        self.caption = caption
        self.url = url
        self.has_spoiler = has_spoiler


class PageBlockVideo(PageBlock):
    """A video.

    Parameters:
        video (:obj:`~pyrogram.types.Video`, *optional*):
            Video file.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Video caption.

        need_autoplay (``bool``, *optional*):
            True, if the video must be played automatically.

        is_looped (``bool``, *optional*):
            True, if the video must be looped.

        has_spoiler (``bool``, *optional*):
            True, if the video preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        *,
        video: "types.Video" = None,
        caption: "PageBlockCaption" = None,
        need_autoplay: bool = None,
        is_looped: bool = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.video = video
        self.caption = caption
        self.need_autoplay = need_autoplay
        self.is_looped = is_looped
        self.has_spoiler = has_spoiler


class PageBlockVoiceNote(PageBlock):
    """A voice note.

    Parameters:
        voice_note (:obj:`~pyrogram.types.Voice`):
            Voice note.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Voice note caption.
    """

    def __init__(self, *, voice_note: "types.Voice", caption: "PageBlockCaption" = None):
        super().__init__()

        self.voice_note = voice_note
        self.caption = caption


class PageBlockCover(PageBlock):
    """A page cover; instant view only.

    Parameters:
        cover (:obj:`~pyrogram.types.PageBlock`):
            Cover.
    """

    def __init__(self, *, cover: "PageBlock"):
        super().__init__()

        self.cover = cover


class PageBlockEmbedded(PageBlock):
    """An embedded web page; instant view only.

    Parameters:
        url (``str``, *optional*):
            URL of the embedded page.

        html (``str``, *optional*):
            HTML-markup of the embedded page.

        poster_photo (:obj:`~pyrogram.types.Photo`, *optional*):
            Poster photo.

        width (``int``, *optional*):
            Block width.

        height (``int``, *optional*):
            Block height.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Block caption.

        is_full_width (``bool``, *optional*):
            True, if the block must be full width.

        allow_scrolling (``bool``, *optional*):
            True, if scrolling needs to be allowed.
    """

    def __init__(
        self,
        *,
        url: str = None,
        html: str = None,
        poster_photo: "types.Photo" = None,
        width: int = None,
        height: int = None,
        caption: "PageBlockCaption" = None,
        is_full_width: bool = None,
        allow_scrolling: bool = None
    ):
        super().__init__()

        self.url = url
        self.html = html
        self.poster_photo = poster_photo
        self.width = width
        self.height = height
        self.caption = caption
        self.is_full_width = is_full_width
        self.allow_scrolling = allow_scrolling


class PageBlockEmbeddedPost(PageBlock):
    """An embedded post; instant view only.

    Parameters:
        url (``str``):
            URL of the embedded post.

        author (``str``):
            Post author.

        author_photo (:obj:`~pyrogram.types.Photo`, *optional*):
            Post author photo.

        date (:py:obj:`~datetime.datetime`, *optional*):
            Date the post was created.

        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Post content.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Post caption.
    """

    def __init__(
        self,
        *,
        url: str,
        author: str,
        author_photo: "types.Photo" = None,
        date: datetime = None,
        blocks: List["PageBlock"],
        caption: "PageBlockCaption" = None
    ):
        super().__init__()

        self.url = url
        self.author = author
        self.author_photo = author_photo
        self.date = date
        self.blocks = blocks
        self.caption = caption


class PageBlockCollage(PageBlock):
    """A collage.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Collage item contents.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Block caption.
    """

    def __init__(self, *, blocks: List["PageBlock"], caption: "PageBlockCaption" = None):
        super().__init__()

        self.blocks = blocks
        self.caption = caption


class PageBlockSlideshow(PageBlock):
    """A slideshow.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Slideshow item contents.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Block caption.
    """

    def __init__(self, *, blocks: List["PageBlock"], caption: "PageBlockCaption" = None):
        super().__init__()

        self.blocks = blocks
        self.caption = caption


class PageBlockChatLink(PageBlock):
    """A link to a chat; instant view only.

    Parameters:
        chat (:obj:`~pyrogram.types.Chat`):
            The chat.
    """

    def __init__(self, *, chat: "types.Chat"):
        super().__init__()

        self.chat = chat


class PageBlockTable(PageBlock):
    """A table.

    Parameters:
        caption (:obj:`~pyrogram.types.RichText`, *optional*):
            Table caption.

        cells (List of List of :obj:`~pyrogram.types.PageBlockTableCell`):
            Table cells, row by row.

        is_bordered (``bool``, *optional*):
            True, if the table is bordered.

        is_striped (``bool``, *optional*):
            True, if the table is striped.

        is_compact (``bool``, *optional*):
            True, if table cells must have smaller indents.
    """

    def __init__(
        self,
        *,
        caption: "RichText" = None,
        cells: List[List["PageBlockTableCell"]],
        is_bordered: bool = None,
        is_striped: bool = None,
        is_compact: bool = None
    ):
        super().__init__()

        self.caption = caption
        self.cells = cells
        self.is_bordered = is_bordered
        self.is_striped = is_striped
        self.is_compact = is_compact


class PageBlockDetails(PageBlock):
    """A collapsible block.

    Parameters:
        header (:obj:`~pyrogram.types.RichText`):
            Always visible heading for the block.

        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Block contents.

        is_open (``bool``, *optional*):
            True, if the block is open by default.
    """

    def __init__(self, *, header: "RichText", blocks: List["PageBlock"], is_open: bool = None):
        super().__init__()

        self.header = header
        self.blocks = blocks
        self.is_open = is_open


class PageBlockRelatedArticles(PageBlock):
    """Related articles; instant view only.

    Parameters:
        header (:obj:`~pyrogram.types.RichText`):
            Block header.

        articles (List of :obj:`~pyrogram.types.PageBlockRelatedArticle`):
            List of related articles.
    """

    def __init__(self, *, header: "RichText", articles: List["PageBlockRelatedArticle"]):
        super().__init__()

        self.header = header
        self.articles = articles


class PageBlockMap(PageBlock):
    """A map.

    Parameters:
        location (:obj:`~pyrogram.types.Location`):
            Location of the map center.

        zoom (``int``):
            Map zoom level.

        width (``int``):
            Map width.

        height (``int``):
            Map height.

        caption (:obj:`~pyrogram.types.PageBlockCaption`, *optional*):
            Block caption.
    """

    def __init__(
        self,
        *,
        location: "types.Location",
        zoom: int,
        width: int,
        height: int,
        caption: "PageBlockCaption" = None
    ):
        super().__init__()

        self.location = location
        self.zoom = zoom
        self.width = width
        self.height = height
        self.caption = caption


class PageBlockButtonRow(PageBlock):
    """A list of buttons shown in a row.

    Parameters:
        buttons (List of :obj:`~pyrogram.types.InlineButton`):
            The buttons.

        align (:obj:`~pyrogram.enums.PageBlockHorizontalAlignment`, *optional*):
            Horizontal alignment of the buttons; None if the buttons must be shown full-width.
    """

    def __init__(
        self,
        *,
        buttons: List["types.InlineButton"],
        align: "enums.PageBlockHorizontalAlignment" = None
    ):
        super().__init__()

        self.buttons = buttons
        self.align = align


class PageBlockUnsupported(PageBlock):
    """A block unsupported by the current version of Pyrofork."""

    def __init__(self):
        super().__init__()
