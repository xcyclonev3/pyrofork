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

import asyncio
import json

import pytest

import pyrogram
from pyrogram import enums, raw, types
from tests.test_layer_roundtrip import PEERS, RAW_CHANNEL, call, offline_client, roundtrip


def test_get_guard_bot_web_app_url():
    client = offline_client(PEERS, lambda q: raw.types.WebViewResultUrl(url="https://guard.example/app", query_id=5))
    url = call(lambda: client.get_guard_bot_web_app_url(555, theme_params={"bg_color": "#000000"}))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.RequestChatJoinWebView)
    assert request.query_id == 555 and request.platform == "unknown"
    assert json.loads(request.theme_params.data) == {"bg_color": "#000000"}
    assert url == "https://guard.example/app"


@pytest.mark.parametrize("result, result_type, url", [
    (raw.types.JoinChatBotResultApproved(), enums.ChatJoinResultType.APPROVED, None),
    (raw.types.JoinChatBotResultDeclined(), enums.ChatJoinResultType.DECLINED, None),
    (raw.types.JoinChatBotResultQueued(), enums.ChatJoinResultType.QUEUED, None),
    (raw.types.JoinChatBotResultWebView(url="https://guard.example/x"), enums.ChatJoinResultType.WEB_APP,
     "https://guard.example/x"),
])
def test_chat_join_result_updates(result, result_type, url):
    client = pyrogram.Client("layer-roundtrip", api_id=1, api_hash="x", in_memory=True)
    update = roundtrip(raw.types.UpdateJoinChatWebViewDecision(
        peer=raw.types.PeerChannel(channel_id=123), query_id=555, result=result))
    parser = client.dispatcher.update_parsers[type(update)]

    parsed, handler_type = asyncio.run(parser(update, {}, {123: roundtrip(RAW_CHANNEL)}))

    assert handler_type is pyrogram.handlers.ChatJoinResultHandler
    assert isinstance(parsed, types.ChatJoinResult) and parsed.chat.title == "Test"
    assert (parsed.query_id, parsed.type, parsed.url) == (555, result_type, url)
    assert callable(client.on_chat_join_result)
