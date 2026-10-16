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

"""Offline tests for the Layer 221-230 bot features: managed bots, prepared keyboard buttons,
guard bot answers, guest queries and business connected bots."""

import pytest

from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, PEERS, USER

BOT = raw.types.InputPeerUser(user_id=99, access_hash=3)
MANAGER_BOT = raw.types.InputPeerUser(user_id=98, access_hash=4)
BOT_PEERS = {**PEERS, 99: BOT, 98: MANAGER_BOT, "managerbot": MANAGER_BOT}

# bot and bot_info_version share flags.14: the server sends both
RAW_MANAGED_BOT = raw.types.User(
    id=99, bot=True, bot_info_version=1, first_name="My Bot", username="my_managed_bot", access_hash=3
)
RAW_MANAGER_BOT = raw.types.User(id=98, bot=True, bot_info_version=1, first_name="Manager", access_hash=4)
RAW_USER = raw.types.User(id=7, first_name="Alice", access_hash=2)


# Managed bots

def test_create_bot():
    client = offline_client(BOT_PEERS, lambda q: RAW_MANAGED_BOT)
    cached = []

    async def fetch_peers(peers):
        cached.extend(peers)
        return False

    client.fetch_peers = fetch_peers
    bot = call(lambda: client.create_bot("managerbot", "My Bot", "my_managed_bot", via_link=True))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.CreateBot)
    assert request.name == "My Bot" and request.username == "my_managed_bot" and request.via_deeplink
    assert request.manager_id.write() == MANAGER_BOT.write()
    assert isinstance(bot, types.User) and bot.id == 99 and bot.is_bot and bot.username == "my_managed_bot"
    assert [peer.id for peer in cached] == [99]


@pytest.mark.parametrize("revoke", [False, True])
def test_get_managed_bot_token(revoke):
    client = offline_client(BOT_PEERS, lambda q: raw.types.bots.ExportedBotToken(token="99:secret"))
    assert call(lambda: client.get_managed_bot_token(99, revoke=revoke)) == "99:secret"

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.ExportBotToken)
    assert request.bot.write() == BOT.write() and request.revoke is revoke


def test_get_managed_bot_access_settings():
    client = offline_client(BOT_PEERS, lambda q: raw.types.bots.AccessSettings(restricted=True, add_users=[RAW_USER]))
    settings = call(lambda: client.get_managed_bot_access_settings(99))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.GetAccessSettings) and request.bot.write() == BOT.write()
    assert isinstance(settings, types.BotAccessSettings) and settings.is_restricted is True
    assert [user.id for user in settings.added_users] == [7] and settings.added_users[0].first_name == "Alice"


def test_bot_access_settings_type_without_users():
    settings = types.BotAccessSettings._parse(None, roundtrip(raw.types.bots.AccessSettings()))
    assert settings.is_restricted is False and settings.added_users is None


@pytest.mark.parametrize("is_restricted, user_ids", [(True, [7]), (True, None), (False, None)])
def test_set_managed_bot_access_settings(is_restricted, user_ids):
    client = offline_client(BOT_PEERS, lambda q: True)
    assert call(lambda: client.set_managed_bot_access_settings(99, is_restricted, user_ids)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.EditAccessSettings) and request.bot.write() == BOT.write()
    assert request.restricted is is_restricted
    assert [user.write() for user in request.add_users] == [USER.write() for _ in user_ids or []]


# Guard bots

@pytest.mark.parametrize("result, url, expected", [
    (enums.ChatJoinResultType.APPROVED, None, raw.types.JoinChatBotResultApproved),
    (enums.ChatJoinResultType.DECLINED, None, raw.types.JoinChatBotResultDeclined),
    (enums.ChatJoinResultType.QUEUED, None, raw.types.JoinChatBotResultQueued),
    (enums.ChatJoinResultType.WEB_APP, "https://example.org/check", raw.types.JoinChatBotResultWebView),
])
def test_answer_chat_join_request_query(result, url, expected):
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.answer_chat_join_request_query("555", result, url=url)) is True

    request = client.sent[-1]
    assert isinstance(request, raw.functions.bots.SetJoinChatResults) and request.query_id == 555
    assert isinstance(request.result, expected)
    assert getattr(request.result, "url", None) == url


@pytest.mark.parametrize("result, url", [
    (enums.ChatJoinResultType.WEB_APP, None),
    (enums.ChatJoinResultType.APPROVED, "https://example.org/check"),
])
def test_answer_chat_join_request_query_checks_url(result, url):
    client = offline_client(PEERS, lambda q: True)
    with pytest.raises(ValueError):
        call(lambda: client.answer_chat_join_request_query(555, result, url=url))
    assert client.sent == []
