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

from typing import Optional, Union

import pyrogram
from pyrogram import raw
from ..object import Object
from .rich_text import RichText, is_empty_raw_rich_text, write_rich_text


class PageBlockCaption(Object):
    """A caption of a page block.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`):
            Content of the caption.

        credit (:obj:`~pyrogram.types.RichText`, *optional*):
            Block credit (like HTML tag <cite>).
    """

    def __init__(self, text: Union["RichText", str], credit: Union["RichText", str] = None):
        super().__init__()

        self.text = text
        self.credit = credit

    @staticmethod
    def _parse(client: "pyrogram.Client", caption: "raw.types.PageCaption", context=None) -> Optional["PageBlockCaption"]:
        if caption is None or (is_empty_raw_rich_text(caption.text) and is_empty_raw_rich_text(caption.credit)):
            return None

        return PageBlockCaption(
            text=RichText._parse(client, caption.text, context),
            credit=parse_optional_rich_text(client, caption.credit, context)
        )

    @staticmethod
    async def _write(writer, caption: Optional["PageBlockCaption"]) -> "raw.types.PageCaption":
        """The caption to send; an empty one for no caption, because pageCaption isn't optional."""
        if caption is None:
            return raw.types.PageCaption(text=raw.types.TextEmpty(), credit=raw.types.TextEmpty())

        return raw.types.PageCaption(
            text=await write_rich_text(writer, caption.text),
            credit=await write_rich_text(writer, caption.credit)
        )


def parse_optional_rich_text(client, rich_text: "raw.base.RichText", context=None) -> Optional["RichText"]:
    """Parse a rich text that is shown only when it isn't empty (credits, table captions)."""
    if is_empty_raw_rich_text(rich_text):
        return None

    return RichText._parse(client, rich_text, context)
