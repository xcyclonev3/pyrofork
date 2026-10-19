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

"""What a session sends when it starts."""

import asyncio

import pytest

import pyrogram
from pyrogram import raw
from pyrogram.connection.transport import TCPAbridged
from pyrogram.session import Session

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
