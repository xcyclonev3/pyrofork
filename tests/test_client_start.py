#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
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

"""What Client.start asks Telegram once connected, offline."""

import asyncio

import pytest

import pyrogram
from pyrogram import raw, types
from pyrogram.errors import FloodWait

DELAY = 0.05  # Every request takes this long


def start(failing=None):
    """Starts a client whose requests take DELAY each: returns the calls made, the client and the error start()
    raised, if any."""
    calls = []

    async def main():
        client = pyrogram.Client("start", api_id=1, api_hash="x", in_memory=True)

        async def connect():
            await client.storage.open()
            await client.storage.is_bot(False)
            client.is_connected = True
            return True  # Authorized

        async def invoke(query, *args, **kwargs):
            calls.append(("begin", type(query).__name__))
            await asyncio.sleep(DELAY)
            calls.append(("end", type(query).__name__))
            if failing == "GetState":
                raise FloodWait(5)
            return raw.types.updates.State(pts=1, qts=0, date=0, seq=0, unread_count=0)

        async def get_me():
            calls.append(("begin", "get_me"))
            await asyncio.sleep(DELAY)
            calls.append(("end", "get_me"))
            if failing == "get_me":
                raise FloodWait(7)
            return types.User(id=7, is_self=True)

        async def initialize():
            calls.append(("initialize",))

        async def disconnect():
            calls.append(("disconnect",))
            client.is_connected = False

        client.connect, client.invoke, client.get_me = connect, invoke, get_me
        client.initialize, client.disconnect = initialize, disconnect
        error = None
        try:
            await client.start()
        except FloodWait as e:
            error = e
        await client.storage.close()
        return client, error

    client, error = asyncio.run(main())
    return calls, client, error


def test_state_and_account_are_asked_together():
    calls, client, error = start()
    # GetState then get_me, one after the other, took two round trips: both must be on their way before either ends
    assert {calls[0], calls[1]} == {("begin", "GetState"), ("begin", "get_me")}
    assert {calls[2], calls[3]} == {("end", "GetState"), ("end", "get_me")}
    assert calls[-1] == ("initialize",)
    assert client.me.id == 7 and error is None


@pytest.mark.parametrize("failing", ["GetState", "get_me"])
def test_a_failing_request_disconnects(failing):
    calls, client, error = start(failing)
    # get_me used to fail after the try: the client stayed connected, and a second start() said "already connected"
    assert error is not None and calls[-1] == ("disconnect",)
    assert ("initialize",) not in calls
    assert client.me is None
    assert len(calls) == 5  # both requests ended before the disconnection: nothing left running
