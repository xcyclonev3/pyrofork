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

import pyrogram
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


def unique_gift(**kwargs):
    return raw.types.StarGiftUnique(
        id=1, gift_id=2, title="Helmet", slug="Helmet-1", num=7, availability_issued=1, availability_total=10,
        attributes=[raw.types.StarGiftAttributeModel(
            name="Gold", document=STICKER, rarity=raw.types.StarGiftAttributeRarityLegendary(), crafted=True
        )],
        **kwargs
    )


def test_unique_gift_crafting_fields():
    gift = asyncio.run(types.Gift._parse(None, roundtrip(unique_gift(burned=True, craft_chance_permille=125)), {}, {}))
    assert gift.name == "Helmet-1" and gift.regular_gift_id == 2
    assert gift.is_burned is True and gift.is_crafted is False and gift.craft_probability_per_mille == 125
    assert gift.attributes[0].is_crafted and gift.attributes[0].rarity_type == RT.LEGENDARY

    gift = asyncio.run(types.Gift._parse(None, roundtrip(unique_gift(crafted=True)), {}, {}))
    assert gift.is_crafted is True and gift.is_burned is False and gift.craft_probability_per_mille is None


def test_saved_gift_can_craft_at():
    saved = roundtrip(raw.types.SavedStarGift(date=1, gift=unique_gift(craft_chance_permille=80), msg_id=5,
                                              can_craft_at=1700000000))
    gift = asyncio.run(types.Gift._parse(None, saved, {}, {}))
    assert gift.can_craft_at == pyrogram.utils.timestamp_to_datetime(1700000000)
    assert gift.message_id == 5 and gift.craft_probability_per_mille == 80

    saved = roundtrip(raw.types.SavedStarGift(date=1, gift=unique_gift(), msg_id=5))
    assert asyncio.run(types.Gift._parse(None, saved, {}, {})).can_craft_at is None


def craft_message(message_id=10, gift_crafted=True, **kwargs):
    return raw.types.MessageService(
        id=message_id, peer_id=raw.types.PeerUser(user_id=7), date=1,
        action=raw.types.MessageActionStarGiftUnique(gift=unique_gift(crafted=gift_crafted), **kwargs)
    )


@pytest.mark.parametrize("craft, gift_crafted", [(True, True), (True, None), (None, True)])
def test_unique_gift_action_crafting_fields(craft, gift_crafted):
    message = roundtrip(craft_message(gift_crafted=gift_crafted, craft=craft, can_craft_at=1700000000))
    gift = asyncio.run(types.Gift._parse_action(None, message, {}, {}))
    assert gift.is_crafted is True and gift.upgrade_message_id == 10
    assert gift.can_craft_at == pyrogram.utils.timestamp_to_datetime(1700000000)


def test_upgraded_gift_action_is_not_crafted():
    message = roundtrip(raw.types.MessageService(
        id=10, peer_id=raw.types.PeerUser(user_id=7), date=1,
        action=raw.types.MessageActionStarGiftUnique(gift=unique_gift(), upgrade=True)
    ))
    gift = asyncio.run(types.Gift._parse_action(None, message, {}, {}))
    assert gift.is_crafted is False and gift.is_upgraded is True and gift.can_craft_at is None
