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

from pyrogram import raw
from .auto_name import AutoName


class GiftAttributeRarityType(AutoName):
    """Rarity type of an upgraded gift attribute enumeration used in :obj:`~pyrogram.types.GiftAttribute`."""

    PER_MILLE = raw.types.StarGiftAttributeRarity
    "The rarity is the frequency of the attribute in permilles, see :obj:`~pyrogram.types.GiftAttribute` *rarity*"

    UNCOMMON = raw.types.StarGiftAttributeRarityUncommon
    "The attribute is uncommon"

    RARE = raw.types.StarGiftAttributeRarityRare
    "The attribute is rare"

    EPIC = raw.types.StarGiftAttributeRarityEpic
    "The attribute is epic"

    LEGENDARY = raw.types.StarGiftAttributeRarityLegendary
    "The attribute is legendary"
