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

from typing import Union

import pyrogram
from pyrogram import enums, raw
from ..object import Object
from .page_block_caption import parse_optional_rich_text
from .rich_text import RichText, write_rich_text


class PageBlockTableCell(Object):
    """A cell of a :obj:`~pyrogram.types.PageBlockTable`.

    Parameters:
        text (:obj:`~pyrogram.types.RichText`, *optional*):
            Cell text. If the text is None, then the cell must be invisible.

        is_header (``bool``, *optional*):
            True, if it is a header cell.

        colspan (``int``, *optional*):
            The number of columns the cell spans; defaults to 1.

        rowspan (``int``, *optional*):
            The number of rows the cell spans; defaults to 1.

        align (:obj:`~pyrogram.enums.PageBlockHorizontalAlignment`, *optional*):
            Horizontal cell content alignment; defaults to :obj:`~pyrogram.enums.PageBlockHorizontalAlignment.LEFT`.

        valign (:obj:`~pyrogram.enums.PageBlockVerticalAlignment`, *optional*):
            Vertical cell content alignment; when sending, defaults to
            :obj:`~pyrogram.enums.PageBlockVerticalAlignment.MIDDLE`.
    """

    def __init__(
        self,
        text: Union["RichText", str] = None,
        is_header: bool = None,
        colspan: int = 1,
        rowspan: int = 1,
        align: "enums.PageBlockHorizontalAlignment" = None,
        valign: "enums.PageBlockVerticalAlignment" = None
    ):
        super().__init__()

        self.text = text
        self.is_header = is_header
        self.colspan = colspan
        self.rowspan = rowspan
        self.align = align
        self.valign = valign

    @staticmethod
    def _parse(client: "pyrogram.Client", cell: "raw.types.PageTableCell", context=None) -> "PageBlockTableCell":
        if cell.align_center:
            align = enums.PageBlockHorizontalAlignment.CENTER
        elif cell.align_right:
            align = enums.PageBlockHorizontalAlignment.RIGHT
        else:
            align = enums.PageBlockHorizontalAlignment.LEFT

        if cell.valign_middle:
            valign = enums.PageBlockVerticalAlignment.MIDDLE
        elif cell.valign_bottom:
            valign = enums.PageBlockVerticalAlignment.BOTTOM
        else:
            valign = enums.PageBlockVerticalAlignment.TOP

        return PageBlockTableCell(
            text=parse_optional_rich_text(client, cell.text, context),
            is_header=cell.header,
            colspan=max(cell.colspan or 0, 1),
            rowspan=max(cell.rowspan or 0, 1),
            align=align,
            valign=valign
        )

    async def write(self, writer) -> "raw.types.PageTableCell":
        if (self.colspan is not None and self.colspan < 0) or (self.rowspan is not None and self.rowspan < 0):
            raise ValueError("Invalid table cell colspan or rowspan specified")

        colspan = max(self.colspan or 0, 1)
        rowspan = max(self.rowspan or 0, 1)
        valign = self.valign or enums.PageBlockVerticalAlignment.MIDDLE
        text = None if self.text is None else await write_rich_text(writer, self.text)

        return raw.types.PageTableCell(
            header=self.is_header,
            align_center=self.align == enums.PageBlockHorizontalAlignment.CENTER,
            align_right=self.align == enums.PageBlockHorizontalAlignment.RIGHT,
            valign_middle=valign == enums.PageBlockVerticalAlignment.MIDDLE,
            valign_bottom=valign == enums.PageBlockVerticalAlignment.BOTTOM,
            text=None if isinstance(text, raw.types.TextEmpty) else text,
            colspan=colspan if colspan != 1 else None,
            rowspan=rowspan if rowspan != 1 else None
        )
