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

from enum import auto

from .auto_name import AutoName


class CraftGiftResultType(AutoName):
    """Type of the result of gift crafting enumeration used in :obj:`~pyrogram.types.CraftGiftResult`."""

    SUCCESS = auto()
    "Crafting was successful"

    TOO_EARLY = auto()
    "Crafting isn't possible because one of the gifts can't be used for crafting yet"

    INVALID_GIFT = auto()
    "Crafting isn't possible because one of the gifts isn't suitable for crafting"

    FAIL = auto()
    "Crafting has failed: no gift was crafted and the gifts used for crafting were burned"
