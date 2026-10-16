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

from typing import List

from pyrogram import types
from ..object import Object


class PageBlockListItem(Object):
    """An item of a :obj:`~pyrogram.types.PageBlockList`.

    Parameters:
        label (``str``):
            Item label: "•" for unordered lists, the number of the item followed by a dot (like "1.", "b.", "IV.")
            for ordered lists.

        blocks (List of :obj:`~pyrogram.types.PageBlock`):
            Item blocks.

        has_checkbox (``bool``):
            True, if the item has a checkbox.

        is_checked (``bool``):
            True, if the item is checked.

        value (``int``):
            Value of the item; 0 for unordered lists.

        type (``str``):
            Type of the item numbering: "a" for lowercase letters, "A" for uppercase letters, "i" for lowercase Roman
            numerals, "I" for uppercase Roman numerals, "1" for decimal numbers, or empty for unordered lists.
    """

    def __init__(
        self,
        *,
        label: str,
        blocks: List["types.PageBlock"],
        has_checkbox: bool = False,
        is_checked: bool = False,
        value: int = 0,
        type: str = ""
    ):
        super().__init__()

        self.label = label
        self.blocks = blocks
        self.has_checkbox = has_checkbox
        self.is_checked = is_checked
        self.value = value
        self.type = type
