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

"""Offline tests for gift crafting, rarity tiers, currency exchange rates and stake dice (Layer 222-230)."""
import asyncio

import pytest

from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS

STICKER = raw.types.Document(
    id=11, access_hash=12, file_reference=b"ref", date=1, mime_type="application/x-tgsticker", size=10, dc_id=2,
    attributes=[raw.types.DocumentAttributeSticker(alt="🎁", stickerset=raw.types.InputStickerSetEmpty())]
)

RT = enums.GiftAttributeRarityType


def parse_attribute(attr):
    return asyncio.run(types.GiftAttribute._parse(None, roundtrip(attr), {}))


@pytest.mark.parametrize("rarity, permille, rarity_type", [
    (raw.types.StarGiftAttributeRarity(permille=15), 15, RT.PER_MILLE),
    (raw.types.StarGiftAttributeRarityUncommon(), None, RT.UNCOMMON),
    (raw.types.StarGiftAttributeRarityRare(), None, RT.RARE),
    (raw.types.StarGiftAttributeRarityEpic(), None, RT.EPIC),
    (raw.types.StarGiftAttributeRarityLegendary(), None, RT.LEGENDARY),
])
def test_gift_attribute_rarity_type(rarity, permille, rarity_type):
    parsed = parse_attribute(raw.types.StarGiftAttributeBackdrop(
        name="Night", backdrop_id=1, center_color=1, edge_color=2, pattern_color=3, text_color=4, rarity=rarity
    ))
    assert parsed.type == enums.GiftAttributeType.BACKDROP
    assert parsed.rarity == permille and parsed.rarity_type == rarity_type


@pytest.mark.parametrize("crafted", [True, False])
def test_gift_attribute_crafted_model(crafted):
    parsed = parse_attribute(raw.types.StarGiftAttributeModel(
        name="Gold", document=STICKER, rarity=raw.types.StarGiftAttributeRarityEpic(), crafted=crafted
    ))
    assert parsed.type == enums.GiftAttributeType.MODEL and parsed.name == "Gold"
    assert parsed.is_crafted is crafted and parsed.rarity_type == RT.EPIC and parsed.rarity is None
    assert parsed.sticker.emoji == "🎁"

    pattern = parse_attribute(raw.types.StarGiftAttributePattern(
        name="Stars", document=STICKER, rarity=raw.types.StarGiftAttributeRarity(permille=200)
    ))
    assert pattern.type == enums.GiftAttributeType.SYMBOL and pattern.is_crafted is None
    assert (pattern.rarity, pattern.rarity_type) == (200, RT.PER_MILLE)


def test_gift_attribute_original_details_has_no_rarity():
    parsed = parse_attribute(raw.types.StarGiftAttributeOriginalDetails(
        recipient_id=raw.types.PeerUser(user_id=7), date=1
    ))
    assert parsed.type == enums.GiftAttributeType.ORIGINAL_DETAILS
    assert parsed.rarity is None and parsed.rarity_type is None and parsed.is_crafted is None
