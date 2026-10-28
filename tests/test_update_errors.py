#  Pyrogram - Telegram MTProto API Client Library for Python
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

"""Requests that fail while handling updates, or in the updates watchdog, when the connection stays down."""

import asyncio
import logging
from datetime import datetime, timedelta

import pytest

import pyrogram
from pyrogram import raw, utils
from pyrogram.errors import ChannelInvalid

CHANNEL_ID = 1234567
CLOSED = OSError("unable to perform operation on <TCPTransport closed=True reading=False>; the handler is closed")


def channel_updates(min_channel=True):
    """A new message of a channel known only as min, and a second update after it in the same container."""
    channel = raw.types.Channel(
        id=CHANNEL_ID, title="channel", photo=raw.types.ChatPhotoEmpty(), date=0, min=min_channel, access_hash=7
    )
    message = raw.types.Message(id=10, peer_id=raw.types.PeerChannel(channel_id=CHANNEL_ID), date=0, message="hi")
    return raw.types.Updates(
        updates=[
            raw.types.UpdateNewChannelMessage(message=message, pts=20, pts_count=1),
            raw.types.UpdateUserStatus(user_id=5, status=raw.types.UserStatusRecently()),
        ],
        users=[],
        chats=[channel],
        date=0,
        seq=0,
    )


def handle(updates, failing, known_channel=False):
    """Client.handle_updates with every request failing: -> the requests asked and the queued updates."""

    async def main():
        client = pyrogram.Client("updates", api_id=1, api_hash="x", in_memory=True)
        await client.storage.open()
        client.is_connected = True
        asked = []

        if known_channel:  # resolve_peer finds it, so the request that fails is GetChannelDifference itself
            await client.storage.update_peers([(utils.get_channel_id(CHANNEL_ID), 7, "channel", None, None)])

        async def invoke(query, *args, **kwargs):
            asked.append(type(query).__name__)
            raise failing

        client.invoke = invoke
        await client.handle_updates(updates)  # Used to raise, in a task nobody awaits, and drop the container
        queued = []

        while not client.dispatcher.updates_queue.empty():
            queued.append(type(client.dispatcher.updates_queue.get_nowait()[0]).__name__)

        await client.storage.close()
        return asked, queued

    return asyncio.run(main())


@pytest.mark.parametrize("failing", [CLOSED, TimeoutError("Request timed out"), ChannelInvalid()])
@pytest.mark.parametrize("known_channel", [False, True])
def test_a_channel_message_whose_full_peers_cant_be_fetched_is_still_handled(failing, known_channel):
    asked, queued = handle(channel_updates(), failing, known_channel)
    assert asked == ["GetChannelDifference" if known_channel else "GetChannels"]
    assert queued == ["UpdateNewChannelMessage", "UpdateUserStatus"]


def test_a_short_message_whose_difference_cant_be_fetched_is_logged(caplog):
    short = raw.types.UpdateShortMessage(id=3, user_id=5, message="hi", pts=8, pts_count=1, date=0)

    with caplog.at_level(logging.WARNING, logger="pyrogram.client"):
        asked, queued = handle(short, CLOSED)

    assert asked == ["GetDifference"] and queued == []
    assert "Unable to get the message of a short update" in caplog.text


def test_the_watchdog_outlives_a_failed_get_state(monkeypatch, caplog):
    monkeypatch.setattr(pyrogram.Client, "UPDATES_WATCHDOG_INTERVAL", 0.02)

    async def main():
        client = pyrogram.Client("watchdog", api_id=1, api_hash="x", in_memory=True)
        client.last_update_time = datetime.now() - timedelta(hours=1)
        asked = []

        async def invoke(query, *args, **kwargs):
            asked.append(type(query).__name__)
            raise CLOSED

        client.invoke = invoke
        task = asyncio.create_task(client.updates_watchdog())
        await asyncio.sleep(0.2)
        running = not task.done()

        # What terminate() does: it awaited a watchdog that had ended with the error and raised it again, so stop()
        # never got to disconnect the client
        client.updates_watchdog_event.set()
        await task
        return asked, running

    with caplog.at_level(logging.WARNING, logger="pyrogram.client"):
        asked, running = asyncio.run(main())

    assert running and len(asked) > 1 and set(asked) == {"GetState"}
    assert "Unable to get the update state" in caplog.text
