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

"""Offline tests for the misc Layer 221-230 additions and the official apps' internal APIs (worker E)."""

import pytest

from pyrogram import raw, types
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS

CHANNEL = PEERS[-100123]
BASIC_GROUP = PEERS[-45]
USER = PEERS[7]


def raw_user(user_id=7, **kwargs):
    return raw.types.User(id=user_id, access_hash=2, first_name="A", **kwargs)


def storage_client(respond, peers=PEERS):
    """An offline client with an opened in-memory storage, for methods that cache peers or the session."""
    client = offline_client(peers, respond)
    call(lambda: client.storage.open())
    return client


# get_chat_owner_after_leaving

@pytest.mark.parametrize("chat_id, peer", [(-100123, CHANNEL), (-45, BASIC_GROUP)])
def test_get_chat_owner_after_leaving(chat_id, peer):
    client = storage_client(lambda q: raw_user(8))
    user = call(lambda: client.get_chat_owner_after_leaving(chat_id))

    request = client.sent[-1]
    assert isinstance(request, raw.functions.messages.GetFutureChatCreatorAfterLeave)
    assert request.peer.write() == peer.write()
    assert isinstance(user, types.User) and user.id == 8 and user.first_name == "A"


def test_get_chat_owner_after_leaving_without_candidate():
    client = storage_client(lambda q: raw.types.UserEmpty(id=0))
    assert call(lambda: client.get_chat_owner_after_leaving(-100123)) is None


def test_get_chat_owner_after_leaving_rejects_private_chats():
    client = offline_client(PEERS, lambda q: raw_user())
    with pytest.raises(ValueError):
        call(lambda: client.get_chat_owner_after_leaving(7))
    assert client.sent == []
