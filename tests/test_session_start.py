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

"""What a session sends when it starts, and a request sent again after a bad msg notification."""

import asyncio

import pytest

import pyrogram
from pyrogram import raw
from pyrogram.connection.transport import TCPAbridged
from pyrogram.session import Session
from pyrogram.session.internals import MsgId

AUTH_KEY = bytes(range(256))


class SilentTCP(TCPAbridged):
    """Connects at once, then receives nothing until closed."""

    async def connect(self, address):
        self.reader = asyncio.StreamReader()

    async def close(self):
        if self.reader is not None:
            self.reader.feed_eof()


@pytest.mark.parametrize("is_media, is_cdn", [(False, False), (True, False), (True, True)])
def test_first_request_of_a_session(is_media, is_cdn):
    async def main():
        client = pyrogram.Client("start", api_id=1, api_hash="x", in_memory=True)
        client.protocol_factory = SilentTCP
        await client.storage.open()
        session = Session(client, 2, AUTH_KEY, False, is_media=is_media, is_cdn=is_cdn)
        sent = []

        async def send(data, *args, **kwargs):  # No server: every request succeeds at once
            sent.append(type(data))

        session.send = send
        await session.start()
        await session.stop()
        await client.storage.close()
        return sent

    # The first request learns the server salt (BadServerSalt, then sent again): a Ping before InitConnection only
    # added a round trip. CDN sessions send no InitConnection and keep the Ping.
    assert asyncio.run(main()) == ([raw.functions.Ping] if is_cdn else [raw.functions.InvokeWithLayer])


class AnsweringConnection:
    """Answers the request just sent: a bad msg notification first, then a pong."""

    def __init__(self, session):
        self.session = session
        self.requests = 0

    async def send(self, payload):
        msg_id = list(self.session.results)[-1]
        self.requests += 1
        answer = (
            raw.types.BadMsgNotification(bad_msg_id=msg_id, bad_msg_seqno=0, error_code=16)
            if self.requests == 1
            else raw.types.Pong(msg_id=msg_id, ping_id=7)
        )

        def deliver():
            self.session.results[msg_id].value = answer
            self.session.results[msg_id].event.set()

        asyncio.get_running_loop().call_soon(deliver)


def test_request_sent_again_after_a_bad_msg_notification_returns_its_answer():
    async def main():
        session = Session(None, 2, AUTH_KEY, False)
        session.connection = AnsweringConnection(session)
        session.stored_msg_ids.append(MsgId())  # The notification itself was a received message
        result = await session.send(raw.functions.Ping(ping_id=7))
        return result, session.connection.requests

    result, requests = asyncio.run(main())
    # The request went again, but its answer was dropped and the notification returned instead
    assert requests == 2
    assert isinstance(result, raw.types.Pong) and result.ping_id == 7
