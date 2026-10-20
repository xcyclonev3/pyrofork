#  Pyrogram - Telegram MTProto API Client Library for Python
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

from enum import auto

from .auto_name import AutoName


class PageBlockHorizontalAlignment(AutoName):
    """Horizontal alignment of a table cell content or of a row of buttons, used in
    :obj:`~pyrogram.types.PageBlockTableCell` and :obj:`~pyrogram.types.PageBlockButtonRow`."""

    LEFT = auto()
    "The content must be left-aligned"

    CENTER = auto()
    "The content must be center-aligned"

    RIGHT = auto()
    "The content must be right-aligned"
