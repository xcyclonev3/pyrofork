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
from pyrogram import enums, errors, raw, types
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


def collect(make_generator):
    async def main():
        return [item async for item in make_generator()]
    return asyncio.run(main())


@pytest.mark.parametrize("kwargs, for_craft, stars_only", [
    ({}, False, False),
    ({"for_crafting": True}, True, False),
    ({"for_stars": True}, False, True),
    ({"for_crafting": True, "for_stars": True}, True, True),
])
def test_search_gifts_for_resale_crafting_filters(kwargs, for_craft, stars_only):
    client = offline_client(PEERS, lambda q: raw.types.payments.ResaleStarGifts(
        count=1, gifts=[unique_gift(craft_chance_permille=250)], chats=[], users=[]
    ))
    gifts = collect(lambda: client.search_gifts_for_resale(2, **kwargs))

    [request] = client.sent
    assert isinstance(request, raw.functions.payments.GetResaleStarGifts) and request.gift_id == 2
    assert (request.for_craft, request.stars_only) == (for_craft, stars_only)
    assert [g.name for g in gifts] == ["Helmet-1"] and gifts[0].craft_probability_per_mille == 250


def saved_gifts(*msg_ids, next_offset=None):
    return raw.types.payments.SavedStarGifts(
        count=3, next_offset=next_offset, chats=[], users=[],
        gifts=[raw.types.SavedStarGift(date=1, gift=unique_gift(craft_chance_permille=100), msg_id=i,
                                       can_craft_at=1700000000) for i in msg_ids]
    )


@pytest.mark.parametrize("limit, expected", [(0, [5, 6, 7]), (2, [5, 6])])
def test_get_gifts_for_crafting(limit, expected):
    client = offline_client(PEERS, lambda q: saved_gifts(5, 6, next_offset="6") if not q.offset else saved_gifts(7))
    gifts = collect(lambda: client.get_gifts_for_crafting(2, limit=limit))

    assert [g.message_id for g in gifts] == expected
    assert all(g.regular_gift_id == 2 and g.craft_probability_per_mille == 100 for g in gifts)
    assert gifts[0].can_craft_at == pyrogram.utils.timestamp_to_datetime(1700000000)
    assert all(isinstance(r, raw.functions.payments.GetCraftStarGifts) and r.gift_id == 2 for r in client.sent)
    assert [r.offset for r in client.sent] == (["", "6"] if limit == 0 else [""])
    assert client.sent[0].limit == (100 if limit == 0 else limit)


CR = enums.CraftGiftResultType


def test_craft_gift_success():
    client = offline_client(PEERS, lambda q: empty_updates(updates=[
        raw.types.UpdateNewMessage(message=craft_message(message_id=42, craft=True), pts=1, pts_count=1)
    ]))
    result = call(lambda: client.craft_gift([123, "456", "https://t.me/nft/Helmet-1", "-100123_789"]))

    [request] = client.sent
    assert isinstance(request, raw.functions.payments.CraftStarGift)
    by_id, by_str, by_slug, by_chat = request.stargift
    assert isinstance(by_id, raw.types.InputSavedStarGiftUser) and by_id.msg_id == 123
    assert isinstance(by_str, raw.types.InputSavedStarGiftUser) and by_str.msg_id == 456
    assert isinstance(by_slug, raw.types.InputSavedStarGiftSlug) and by_slug.slug == "Helmet-1"
    assert isinstance(by_chat, raw.types.InputSavedStarGiftChat) and by_chat.saved_id == 789
    assert by_chat.peer.write() == PEERS[-100123].write()

    assert isinstance(result, types.CraftGiftResult) and result.type == CR.SUCCESS
    assert result.owned_gift_id == "42" and result.retry_after is None
    assert isinstance(result.gift, types.Gift) and result.gift.name == "Helmet-1" and result.gift.is_crafted


@pytest.mark.parametrize("response", [
    empty_updates(updates=[raw.types.UpdateStarGiftCraftFail()]),
    raw.types.UpdateShort(update=raw.types.UpdateStarGiftCraftFail(), date=1),
])
def test_craft_gift_fail(response):
    client = offline_client(PEERS, lambda q: response)
    result = call(lambda: client.craft_gift(["123"]))
    assert result.type == CR.FAIL and result.gift is None and result.owned_gift_id is None


def rpc_error(code, message):
    try:
        errors.RPCError.raise_it(raw.types.RpcError(error_code=code, error_message=message),
                                 raw.functions.payments.CraftStarGift)
    except errors.RPCError as e:
        return e


class StargiftCraftTooEarly(errors.BadRequest):  # stands for the class the error table may get one day
    ID = "STARGIFT_CRAFT_TOO_EARLY_X"
    MESSAGE = "The gift can't be used for crafting yet"


@pytest.mark.parametrize("make_error, result_type, retry_after", [
    (lambda: rpc_error(400, "STARGIFT_CRAFT_TOO_EARLY_3600"), CR.TOO_EARLY, 3600),
    (lambda: StargiftCraftTooEarly(value="3600"), CR.TOO_EARLY, 3600),
    (lambda: rpc_error(400, "STARGIFT_CRAFT_UNAVAILABLE"), CR.INVALID_GIFT, None),
    (lambda: rpc_error(400, "STARGIFT_ALREADY_CRAFTED"), CR.INVALID_GIFT, None),
])
def test_craft_gift_errors_are_results(monkeypatch, tmp_path, make_error, result_type, retry_after):
    monkeypatch.chdir(tmp_path)  # unknown errors are logged to ./unknown_errors.txt
    error = make_error()

    def respond(query):
        raise error

    client = offline_client(PEERS, respond)
    result = call(lambda: client.craft_gift(["123"]))
    assert result.type == result_type and result.retry_after == retry_after and result.gift is None


def test_craft_gift_other_errors_are_raised(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    error = rpc_error(400, "STARGIFT_INVALID")

    def respond(query):
        raise error

    client = offline_client(PEERS, respond)
    with pytest.raises(errors.StargiftInvalid):
        call(lambda: client.craft_gift(["123"]))


def test_currency_exchange_rate_type():
    rate = types.CurrencyExchangeRate._parse(None, roundtrip(raw.types.CurrencyRate(currency="EUR", rate=1.25)))
    assert isinstance(rate, types.CurrencyExchangeRate) and (rate.currency, rate.rate) == ("EUR", 1.25)


def test_get_currency_exchange_rates():
    client = offline_client(PEERS, lambda q: raw.types.payments.CurrencyRates(rates=[
        raw.types.CurrencyRate(currency="EUR", rate=1.25),
        raw.types.CurrencyRate(currency="TON", rate=3.5),
    ]))
    rates = call(lambda: client.get_currency_exchange_rates())

    [request] = client.sent
    assert isinstance(request, raw.functions.payments.GetCurrencyRates)
    assert all(isinstance(r, types.CurrencyExchangeRate) for r in rates)
    assert [(r.currency, r.rate) for r in rates] == [("EUR", 1.25), ("TON", 3.5)]


PRIZES = [0, 0, 500, 1000, 1500, 2000, 10000]


def dice_info(**kwargs):
    return raw.types.messages.EmojiGameDiceInfo(
        game_hash="hash", prev_stake=100000000, current_streak=2, params=PRIZES, **kwargs
    )


@pytest.mark.parametrize("plays_left", [3, None])
def test_stake_dice_state_type(plays_left):
    state = types.StakeDiceState._parse(None, roundtrip(dice_info(plays_left=plays_left)))
    assert isinstance(state, types.StakeDiceState)
    assert (state.state_hash, state.stake_gram_amount, state.current_streak) == ("hash", 100000000, 2)
    assert state.prize_per_mille == PRIZES[:6] and state.streak_prize_per_mille == 10000
    assert state.plays_left == plays_left

    unavailable = types.StakeDiceState._parse(None, roundtrip(raw.types.messages.EmojiGameUnavailable()))
    assert isinstance(unavailable, types.StakeDiceState) and unavailable.state_hash is None
    assert unavailable.prize_per_mille is None and unavailable.stake_gram_amount is None


def test_get_stake_dice_state():
    client = offline_client(PEERS, lambda q: dice_info())
    state = call(lambda: client.get_stake_dice_state())

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.GetEmojiGameInfo)
    assert state.state_hash == "hash" and state.prize_per_mille == PRIZES[:6] and state.plays_left is None
