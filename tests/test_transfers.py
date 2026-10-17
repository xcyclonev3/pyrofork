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

"""Sessions used for file transfers, and the transfers themselves."""

import asyncio
import os

from pyrogram import raw
from pyrogram.session import Session


class SilentConnection:
    """A connection that sends everything and never receives an answer."""

    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


def test_cancelled_request_leaves_no_result_slot():
    async def main():
        session = Session(None, 2, os.urandom(256), False)
        session.connection = SilentConnection()
        session.is_started.set()

        query = raw.functions.payments.GetOnrampBaseCurrencies(provider="p", crypto_currency="TON")
        task = asyncio.get_running_loop().create_task(session.invoke(query))
        while not session.connection.sent:
            await asyncio.sleep(0)

        assert len(session.results) == 1 and len(session.result_vectors) == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return session

    session = asyncio.run(main())
    assert session.results == {} and session.result_vectors == {}

