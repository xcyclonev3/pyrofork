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

from typing import List, Optional

from pyrogram import raw, types
from ..object import Object


class RichMessageSource(Object):
    """The source of a rich message to send, used in :obj:`~pyrogram.types.InputRichMessage`.

    It can be one of:

    - :obj:`~pyrogram.types.RichMessageSourceBlocks`
    - :obj:`~pyrogram.types.RichMessageSourceMarkdown`
    - :obj:`~pyrogram.types.RichMessageSourceHtml`
    """

    def __init__(self):
        super().__init__()

    async def write(self, writer, rtl: Optional[bool], noautolink: Optional[bool]) -> "raw.base.InputRichMessage":
        raise NotImplementedError


class RichMessageSourceBlocks(RichMessageSource):
    """A rich message defined by blocks.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.InputPageBlock`):
            Content of the message.
    """

    def __init__(self, blocks: List["types.InputPageBlock"]):
        super().__init__()

        self.blocks = blocks

    async def write(self, writer, rtl: Optional[bool], noautolink: Optional[bool]) -> "raw.base.InputRichMessage":
        blocks = [await block.write(writer) for block in self.blocks]

        return raw.types.InputRichMessage(
            rtl=rtl,
            noautolink=noautolink,
            blocks=blocks,
            photos=writer.photos or None,
            documents=writer.documents or None,
            users=writer.users or None
        )


class RichMessageSourceMarkdown(RichMessageSource):
    """A Markdown-formatted rich message, parsed by Telegram; for bots only.

    Parameters:
        text (``str``):
            Markdown-formatted text of the message.

        media (List of :obj:`~pyrogram.types.InputRichMessageMedia`, *optional*):
            Media used in the message, referred to by their identifiers.
    """

    def __init__(self, text: str, media: List["types.InputRichMessageMedia"] = None):
        super().__init__()

        self.text = text
        self.media = media

    async def write(self, writer, rtl: Optional[bool], noautolink: Optional[bool]) -> "raw.base.InputRichMessage":
        files = [await media.write(writer) for media in self.media or []]

        return raw.types.InputRichMessageMarkdown(rtl=rtl, noautolink=noautolink, markdown=self.text, files=files or None)


class RichMessageSourceHtml(RichMessageSource):
    """An HTML-formatted rich message, parsed by Telegram; for bots only.

    Parameters:
        text (``str``):
            HTML-formatted text of the message.

        media (List of :obj:`~pyrogram.types.InputRichMessageMedia`, *optional*):
            Media used in the message, referred to by their identifiers.
    """

    def __init__(self, text: str, media: List["types.InputRichMessageMedia"] = None):
        super().__init__()

        self.text = text
        self.media = media

    async def write(self, writer, rtl: Optional[bool], noautolink: Optional[bool]) -> "raw.base.InputRichMessage":
        files = [await media.write(writer) for media in self.media or []]

        return raw.types.InputRichMessageHTML(rtl=rtl, noautolink=noautolink, html=self.text, files=files or None)
