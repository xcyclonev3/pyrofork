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

from typing import BinaryIO, List, Union

from pyrogram import enums, raw, types
from ..object import Object
from .page_block import get_ordered_list_label
from .page_block_caption import PageBlockCaption
from .rich_text import RichText, write_rich_text

HEADINGS = {
    1: raw.types.PageBlockHeading1,
    2: raw.types.PageBlockHeading2,
    3: raw.types.PageBlockHeading3,
    4: raw.types.PageBlockHeading4,
    5: raw.types.PageBlockHeading5,
    6: raw.types.PageBlockHeading6,
}


async def write_caption(writer, caption: Union["PageBlockCaption", "RichText", str, None]) -> "raw.types.PageCaption":
    if caption is not None and not isinstance(caption, PageBlockCaption):
        caption = PageBlockCaption(caption)

    return await PageBlockCaption._write(writer, caption)


async def write_blocks(writer, blocks: List["InputPageBlock"]) -> List["raw.base.PageBlock"]:
    return [await block.write(writer) for block in blocks]


class InputPageBlock(Object):
    """A block of a rich message to send.

    It can be one of:

    - :obj:`~pyrogram.types.InputPageBlockSectionHeading`
    - :obj:`~pyrogram.types.InputPageBlockParagraph`
    - :obj:`~pyrogram.types.InputPageBlockPreformatted`
    - :obj:`~pyrogram.types.InputPageBlockFooter`
    - :obj:`~pyrogram.types.InputPageBlockThinking`
    - :obj:`~pyrogram.types.InputPageBlockDivider`
    - :obj:`~pyrogram.types.InputPageBlockMathematicalExpression`
    - :obj:`~pyrogram.types.InputPageBlockAnchor`
    - :obj:`~pyrogram.types.InputPageBlockList`
    - :obj:`~pyrogram.types.InputPageBlockBlockQuote`
    - :obj:`~pyrogram.types.InputPageBlockExpandableBlockQuote`
    - :obj:`~pyrogram.types.InputPageBlockPullQuote`
    - :obj:`~pyrogram.types.InputPageBlockAnimation`
    - :obj:`~pyrogram.types.InputPageBlockAudio`
    - :obj:`~pyrogram.types.InputPageBlockDocument`
    - :obj:`~pyrogram.types.InputPageBlockPhoto`
    - :obj:`~pyrogram.types.InputPageBlockVideo`
    - :obj:`~pyrogram.types.InputPageBlockVoiceNote`
    - :obj:`~pyrogram.types.InputPageBlockCollage`
    - :obj:`~pyrogram.types.InputPageBlockSlideshow`
    - :obj:`~pyrogram.types.InputPageBlockTable`
    - :obj:`~pyrogram.types.InputPageBlockDetails`
    - :obj:`~pyrogram.types.InputPageBlockMap`
    - :obj:`~pyrogram.types.InputPageBlockButtonRow`

    Wherever a rich text is expected, a ``str`` can be passed instead of :obj:`~pyrogram.types.RichTextPlain`.
    Media can be passed as a file path, a file-like object, an HTTP URL or a file id, or as the matching
    :obj:`~pyrogram.types.InputMedia` type to set its attributes (the caption of the input media is ignored).
    """

    def __init__(self):
        super().__init__()

    async def write(self, writer) -> "raw.base.PageBlock":
        raise NotImplementedError


class InputPageBlockSectionHeading(InputPageBlock):
    """A section heading.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Text of the section heading.

        size (``int``):
            Relative size of the text font; 1-6, 1 is the largest, 6 is the smallest.
    """

    def __init__(self, text: Union["RichText", str], size: int):
        super().__init__()

        self.text = text
        self.size = size

    async def write(self, writer) -> "raw.base.PageBlock":
        if self.size not in HEADINGS:
            raise ValueError("Invalid section heading size specified: must be 1-6")

        return HEADINGS[self.size](text=await write_rich_text(writer, self.text))


class InputPageBlockParagraph(InputPageBlock):
    """A text paragraph.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Paragraph text.
    """

    def __init__(self, text: Union["RichText", str]):
        super().__init__()

        self.text = text

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockParagraph(text=await write_rich_text(writer, self.text))


class InputPageBlockPreformatted(InputPageBlock):
    """A preformatted text paragraph.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Paragraph text.

        language (``str``, *optional*):
            Programming language for which the text needs to be formatted.
    """

    def __init__(self, text: Union["RichText", str], language: str = ""):
        super().__init__()

        self.text = text
        self.language = language

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockPreformatted(text=await write_rich_text(writer, self.text), language=self.language or "")


class InputPageBlockFooter(InputPageBlock):
    """The footer of the message.

    Parameters:
        footer (:obj:`~pyrogram.types.RichText` | ``str``):
            Footer.
    """

    def __init__(self, footer: Union["RichText", str]):
        super().__init__()

        self.footer = footer

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockFooter(text=await write_rich_text(writer, self.footer))


class InputPageBlockThinking(InputPageBlock):
    """A "Thinking..." placeholder; for pending rich messages only; for bots only.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Text of the placeholder.
    """

    def __init__(self, text: Union["RichText", str]):
        super().__init__()

        self.text = text

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockThinking(text=await write_rich_text(writer, self.text))


class InputPageBlockDivider(InputPageBlock):
    """An empty block separating the message."""

    def __init__(self):
        super().__init__()

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockDivider()


class InputPageBlockMathematicalExpression(InputPageBlock):
    """A mathematical expression.

    Parameters:
        expression (``str``):
            The expression in LaTeX format.
    """

    def __init__(self, expression: str):
        super().__init__()

        self.expression = expression

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockMath(source=self.expression)


class InputPageBlockAnchor(InputPageBlock):
    """An invisible anchor.

    Parameters:
        name (``str``):
            Name of the anchor.
    """

    def __init__(self, name: str):
        super().__init__()

        self.name = name

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockAnchor(name=self.name)


class InputPageBlockListItem(Object):
    """An item of an :obj:`~pyrogram.types.InputPageBlockList`.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Item blocks.

        has_checkbox (``bool``, *optional*):
            True, if the item has a checkbox.

        is_checked (``bool``, *optional*):
            True, if the item is checked.

        value (``int``, *optional*):
            Value of the item, used as its number; 0 for unordered lists.

        type (``str``, *optional*):
            Type of the item numbering: "a" for lowercase letters, "A" for uppercase letters, "i" for lowercase Roman
            numerals, "I" for uppercase Roman numerals, "1" for decimal numbers, or empty for unordered lists.
            All the items of a list must be either ordered or unordered.
    """

    def __init__(
        self,
        blocks: List["InputPageBlock"],
        has_checkbox: bool = None,
        is_checked: bool = None,
        value: int = 0,
        type: str = ""
    ):
        super().__init__()

        self.blocks = blocks
        self.has_checkbox = has_checkbox
        self.is_checked = is_checked
        self.value = value
        self.type = type

    async def write(self, writer) -> Union["raw.types.PageListItemBlocks", "raw.types.PageListOrderedItemBlocks"]:
        blocks = await write_blocks(writer, self.blocks)
        checked = bool(self.has_checkbox and self.is_checked) or None

        if not self.type:
            return raw.types.PageListItemBlocks(checkbox=self.has_checkbox, checked=checked, blocks=blocks)

        value = self.value or 0

        return raw.types.PageListOrderedItemBlocks(
            checkbox=self.has_checkbox,
            checked=checked,
            num=get_ordered_list_label(value, self.type),
            blocks=blocks,
            value=value,
            type=self.type
        )


class InputPageBlockList(InputPageBlock):
    """A list of data blocks.

    Parameters:
        items (List of :obj:`~pyrogram.types.InputPageBlockListItem`):
            The items of the list.
    """

    def __init__(self, items: List["InputPageBlockListItem"]):
        super().__init__()

        self.items = items

    async def write(self, writer) -> "raw.base.PageBlock":
        if not self.items:
            raise ValueError("List must be non-empty")

        for item in self.items:
            if item.type and (len(item.type) != 1 or item.type not in "aAiI1"):
                raise ValueError(f"Invalid list item type {item.type!r}")

        is_ordered = bool(self.items[0].type)

        if any(bool(item.type) != is_ordered for item in self.items):
            raise ValueError("List must be either ordered or unordered")

        items = [await item.write(writer) for item in self.items]

        if is_ordered:
            return raw.types.PageBlockOrderedList(items=items, start=0, type="")

        return raw.types.PageBlockList(items=items)


class InputPageBlockBlockQuote(InputPageBlock):
    """A block quote.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Quote blocks.

        credit (:obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Quote credit.
    """

    def __init__(self, blocks: List["InputPageBlock"], credit: Union["RichText", str] = None):
        super().__init__()

        self.blocks = blocks
        self.credit = credit

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockBlockquoteBlocks(
            blocks=await write_blocks(writer, self.blocks),
            caption=await write_rich_text(writer, self.credit)
        )


class InputPageBlockExpandableBlockQuote(InputPageBlock):
    """An expandable block quote.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Quote text.

        credit (:obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Quote credit.
    """

    def __init__(self, text: Union["RichText", str], credit: Union["RichText", str] = None):
        super().__init__()

        self.text = text
        self.credit = credit

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockBlockquote(
            collapsed=True,
            text=await write_rich_text(writer, self.text),
            caption=await write_rich_text(writer, self.credit)
        )


class InputPageBlockPullQuote(InputPageBlock):
    """A pull quote.

    Parameters:
        text (:obj:`~pyrogram.types.RichText` | ``str``):
            Quote text.

        credit (:obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Quote credit.
    """

    def __init__(self, text: Union["RichText", str], credit: Union["RichText", str] = None):
        super().__init__()

        self.text = text
        self.credit = credit

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockPullquote(
            text=await write_rich_text(writer, self.text),
            caption=await write_rich_text(writer, self.credit)
        )


class InputPageBlockAnimation(InputPageBlock):
    """An animation, played automatically and looped.

    Parameters:
        animation (``str`` | ``BinaryIO`` | :obj:`~pyrogram.types.InputMediaAnimation`):
            The animation to be sent.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Animation caption.

        has_spoiler (``bool``, *optional*):
            True, if the animation preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        animation: Union[str, BinaryIO, "types.InputMediaAnimation"],
        caption: Union["PageBlockCaption", "RichText", str] = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.animation = animation
        self.caption = caption
        self.has_spoiler = has_spoiler

    async def write(self, writer) -> "raw.base.PageBlock":
        document = await writer.add_document("animation", self.animation)

        return raw.types.PageBlockVideo(
            autoplay=True,
            loop=True,
            spoiler=self.has_spoiler,
            video_id=document.id,
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockAudio(InputPageBlock):
    """An audio file.

    Parameters:
        audio (``str`` | ``BinaryIO`` | :obj:`~pyrogram.types.InputMediaAudio`):
            The audio to be sent.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Audio file caption.
    """

    def __init__(
        self,
        audio: Union[str, BinaryIO, "types.InputMediaAudio"],
        caption: Union["PageBlockCaption", "RichText", str] = None
    ):
        super().__init__()

        self.audio = audio
        self.caption = caption

    async def write(self, writer) -> "raw.base.PageBlock":
        document = await writer.add_document("audio", self.audio)

        return raw.types.PageBlockAudio(audio_id=document.id, caption=await write_caption(writer, self.caption))


class InputPageBlockDocument(InputPageBlock):
    """A general file.

    Parameters:
        document (``str`` | ``BinaryIO`` | :obj:`~pyrogram.types.InputMediaDocument`):
            The file to be sent.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            File caption.
    """

    def __init__(
        self,
        document: Union[str, BinaryIO, "types.InputMediaDocument"],
        caption: Union["PageBlockCaption", "RichText", str] = None
    ):
        super().__init__()

        self.document = document
        self.caption = caption

    async def write(self, writer) -> "raw.base.PageBlock":
        document = await writer.add_document("document", self.document)

        return raw.types.PageBlockDocument(document_id=document.id, caption=await write_caption(writer, self.caption))


class InputPageBlockPhoto(InputPageBlock):
    """A photo.

    Parameters:
        photo (``str`` | ``BinaryIO`` | :obj:`~pyrogram.types.InputMediaPhoto`):
            The photo to be sent.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Photo caption.

        has_spoiler (``bool``, *optional*):
            True, if the photo preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        photo: Union[str, BinaryIO, "types.InputMediaPhoto"],
        caption: Union["PageBlockCaption", "RichText", str] = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.photo = photo
        self.caption = caption
        self.has_spoiler = has_spoiler

    async def write(self, writer) -> "raw.base.PageBlock":
        photo = await writer.add_photo(self.photo)

        return raw.types.PageBlockPhoto(
            spoiler=self.has_spoiler,
            photo_id=photo.id,
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockVideo(InputPageBlock):
    """A video.

    Parameters:
        video (``str`` | ``BinaryIO`` | :obj:`~pyrogram.types.InputMediaVideo`):
            The video to be sent.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Video caption.

        has_spoiler (``bool``, *optional*):
            True, if the video preview must be covered by a spoiler animation.
    """

    def __init__(
        self,
        video: Union[str, BinaryIO, "types.InputMediaVideo"],
        caption: Union["PageBlockCaption", "RichText", str] = None,
        has_spoiler: bool = None
    ):
        super().__init__()

        self.video = video
        self.caption = caption
        self.has_spoiler = has_spoiler

    async def write(self, writer) -> "raw.base.PageBlock":
        document = await writer.add_document("video", self.video)

        return raw.types.PageBlockVideo(
            spoiler=self.has_spoiler,
            video_id=document.id,
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockVoiceNote(InputPageBlock):
    """A voice note.

    Parameters:
        voice_note (``str`` | ``BinaryIO``):
            The voice note to be sent: an OGG file encoded with OPUS, a file id or an HTTP URL.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Voice note caption.

        duration (``int``, *optional*):
            Duration of the voice note, in seconds.
    """

    def __init__(
        self,
        voice_note: Union[str, BinaryIO],
        caption: Union["PageBlockCaption", "RichText", str] = None,
        duration: int = 0
    ):
        super().__init__()

        self.voice_note = voice_note
        self.caption = caption
        self.duration = duration

    async def write(self, writer) -> "raw.base.PageBlock":
        document = await writer.add_document("voice", self.voice_note, duration=self.duration)

        return raw.types.PageBlockAudio(audio_id=document.id, caption=await write_caption(writer, self.caption))


class InputPageBlockCollage(InputPageBlock):
    """A collage.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Collage item contents.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Block caption.
    """

    def __init__(self, blocks: List["InputPageBlock"], caption: Union["PageBlockCaption", "RichText", str] = None):
        super().__init__()

        self.blocks = blocks
        self.caption = caption

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockCollage(
            items=await write_blocks(writer, self.blocks),
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockSlideshow(InputPageBlock):
    """A slideshow.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Slideshow item contents.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Block caption.
    """

    def __init__(self, blocks: List["InputPageBlock"], caption: Union["PageBlockCaption", "RichText", str] = None):
        super().__init__()

        self.blocks = blocks
        self.caption = caption

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockSlideshow(
            items=await write_blocks(writer, self.blocks),
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockTable(InputPageBlock):
    """A table.

    Parameters:
        cells (List of List of :obj:`~pyrogram.types.PageBlockTableCell`):
            Table cells, row by row.

        caption (:obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Table caption.

        is_bordered (``bool``, *optional*):
            Pass True if the table is bordered.

        is_striped (``bool``, *optional*):
            Pass True if the table is striped.

        is_compact (``bool``, *optional*):
            Pass True if table cells must have smaller indents.
    """

    def __init__(
        self,
        cells: List[List["types.PageBlockTableCell"]],
        caption: Union["RichText", str] = None,
        is_bordered: bool = None,
        is_striped: bool = None,
        is_compact: bool = None
    ):
        super().__init__()

        self.cells = cells
        self.caption = caption
        self.is_bordered = is_bordered
        self.is_striped = is_striped
        self.is_compact = is_compact

    async def write(self, writer) -> "raw.base.PageBlock":
        rows = []

        for row in self.cells:
            rows.append(raw.types.PageTableRow(cells=[await cell.write(writer) for cell in row]))

        return raw.types.PageBlockTable(
            bordered=self.is_bordered,
            striped=self.is_striped,
            compact=self.is_compact,
            title=await write_rich_text(writer, self.caption),
            rows=rows
        )


class InputPageBlockDetails(InputPageBlock):
    """A collapsible block.

    Parameters:
        header (:obj:`~pyrogram.types.RichText` | ``str``):
            Always visible heading for the block.

        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Block contents.

        is_open (``bool``, *optional*):
            Pass True if the block is open by default.
    """

    def __init__(self, header: Union["RichText", str], blocks: List["InputPageBlock"], is_open: bool = None):
        super().__init__()

        self.header = header
        self.blocks = blocks
        self.is_open = is_open

    async def write(self, writer) -> "raw.base.PageBlock":
        return raw.types.PageBlockDetails(
            open=self.is_open,
            blocks=await write_blocks(writer, self.blocks),
            title=await write_rich_text(writer, self.header)
        )


class InputPageBlockMap(InputPageBlock):
    """A map. The map's width and height must not exceed 10000 in total, and width and height ratio must be at most 20.

    Parameters:
        location (:obj:`~pyrogram.types.Location`):
            Location of the map center.

        zoom (``int``):
            Map zoom level; 0-24.

        width (``int``):
            Map width; 0-10000.

        height (``int``):
            Map height; 0-10000.

        caption (:obj:`~pyrogram.types.PageBlockCaption` | :obj:`~pyrogram.types.RichText` | ``str``, *optional*):
            Block caption.
    """

    def __init__(
        self,
        location: "types.Location",
        zoom: int,
        width: int,
        height: int,
        caption: Union["PageBlockCaption", "RichText", str] = None
    ):
        super().__init__()

        self.location = location
        self.zoom = zoom
        self.width = width
        self.height = height
        self.caption = caption

    async def write(self, writer) -> "raw.base.PageBlock":
        zoom, width, height = self.zoom, self.width, self.height

        if (
            not 0 <= zoom <= 24 or not 0 <= width <= 10000 or not 0 <= height <= 10000 or width + height > 10000
            or width > height * 20 or height > width * 20
        ):
            raise ValueError("Invalid map properties specified")

        return raw.types.InputPageBlockMap(
            geo=raw.types.InputGeoPoint(lat=self.location.latitude, long=self.location.longitude),
            zoom=zoom,
            w=width,
            h=height,
            caption=await write_caption(writer, self.caption)
        )


class InputPageBlockButtonRow(InputPageBlock):
    """A list of buttons shown in a row.

    Parameters:
        buttons (List of :obj:`~pyrogram.types.InlineButton`):
            The buttons.

        align (:obj:`~pyrogram.enums.PageBlockHorizontalAlignment`, *optional*):
            Horizontal alignment of the buttons; pass None if the buttons must be shown full-width.
    """

    def __init__(self, buttons: List["types.InlineButton"], align: "enums.PageBlockHorizontalAlignment" = None):
        super().__init__()

        self.buttons = buttons
        self.align = align

    async def write(self, writer) -> "raw.base.PageBlock":
        if not self.buttons:
            raise ValueError("Button row must be non-empty")

        buttons = []

        for button in self.buttons:
            text, button_type, style = await button.write(writer)
            buttons.append(raw.types.PageButton(text=text, type=button_type, style=style))

        return raw.types.PageBlockButtonRow(
            align_left=self.align == enums.PageBlockHorizontalAlignment.LEFT,
            align_center=self.align == enums.PageBlockHorizontalAlignment.CENTER,
            align_right=self.align == enums.PageBlockHorizontalAlignment.RIGHT,
            buttons=buttons
        )
