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

"""Session.send without waiting for answers: message creation, packing and handing the payload to the connection."""
import asyncio
import os
import time

from common import AUTH_KEY

from pyrogram import raw
from pyrogram.session import Session


class Sink:
    async def send(self, payload):
        pass


async def main():
    session = Session(None, 2, AUTH_KEY, False)
    session.connection = Sink()
    for name, query, n in (
        ("ping", raw.functions.Ping(ping_id=1), 5000),
        ("sendMessage 300 chars", raw.functions.messages.SendMessage(
            peer=raw.types.InputPeerSelf(), message="x" * 300, random_id=1), 5000),
        ("saveFilePart 512 KiB", raw.functions.upload.SaveFilePart(file_id=1, file_part=0, bytes=os.urandom(524288)), 200),
        ("saveBigFilePart 1 MiB", raw.functions.upload.SaveBigFilePart(
            file_id=1, file_part=0, file_total_parts=9, bytes=os.urandom(1048576)), 120),
    ):
        best = 1e9
        for _ in range(3):
            start, cpu = time.perf_counter(), time.process_time()
            for _ in range(n):
                await session.send(query, wait_response=False)
            best = min(best, (time.perf_counter() - start) / n)
        print(f"{name:<24} {best * 1e6:9.1f} us per send")

asyncio.run(main())
