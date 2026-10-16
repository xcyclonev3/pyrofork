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

import pyrogram
from pyrogram import raw, types
from ..object import Object
from .page_block import PageBlock
from .rich_text import RichMessageParseContext, collect_anchors


class RichMessage(Object):
    """A message with rich formatting: headings, lists, tables, quotes, media, formulas, buttons and more.

    Parameters:
        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Content of the message.

        is_rtl (``bool``):
            True, if the message must be shown from right to left.

        is_full (``bool``):
            True, if the object contains the full message. Otherwise, use
            :meth:`~pyrogram.Client.get_full_rich_message` to get the full message.
    """

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        blocks: List["types.PageBlock"],
        is_rtl: bool = False,
        is_full: bool = True
    ):
        super().__init__(client)

        self.blocks = blocks
        self.is_rtl = is_rtl
        self.is_full = is_full

    @staticmethod
    def _parse(client: "pyrogram.Client", rich_message: "raw.types.RichMessage") -> Optional["RichMessage"]:
        if not isinstance(rich_message, raw.types.RichMessage):
            return None

        # Links to "#name" open the anchor or the reference with that name; "#" brings back to top
        anchors = {"": False}
        collect_anchors(rich_message.blocks, anchors)

        context = RichMessageParseContext(
            client,
            photos={photo.id: photo for photo in rich_message.photos if isinstance(photo, raw.types.Photo)},
            documents={
                document.id: document
                for document in rich_message.documents
                if isinstance(document, raw.types.Document)
            },
            anchors=anchors
        )

        return RichMessage(
            client=client,
            blocks=PageBlock._parse_blocks(client, rich_message.blocks, context),
            is_rtl=bool(rich_message.rtl),
            is_full=not rich_message.part
        )
