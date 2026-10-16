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

from pyrogram import enums, raw
from tests.test_layer_roundtrip import offline_client, call, PEERS


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
