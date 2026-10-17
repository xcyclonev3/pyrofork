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

"""Session.handle_packet for update packets of several sizes, as recv_worker runs it (a task per packet)."""
import asyncio
import time
from hashlib import sha256

from common import AUTH_KEY, AUTH_KEY_ID

from pyrogram import raw
from pyrogram.crypto import aes
from pyrogram.crypto.mtproto import kdf
from pyrogram.raw.core import Long, Message
from pyrogram.session import Session


class Client:
    async def handle_updates(self, update):
        pass


class Sink:
    async def send(self, payload):
        pass


def packet(session, body, msg_id):
    data = Long(1) + session.session_id + Message(body, msg_id, 0, len(body.write())).write()
    padding = bytes(-(len(data) + 12) % 16 + 12)
    msg_key = sha256(AUTH_KEY[96:128] + data + padding).digest()[8:24]
    key, iv = kdf(AUTH_KEY, msg_key, False)
    return AUTH_KEY_ID + msg_key + aes.ige256_encrypt(data + padding, key, iv)


async def main():
    for size, count in ((100, 4000), (4000, 3000), (16000, 1500), (60000, 600), (200000, 200)):
        best = 1e9
        for _ in range(3):
            session = Session(Client(), 2, AUTH_KEY, False)
            session.connection = Sink()
            base = (int(time.time()) << 32) | 1
            packets = [packet(session, raw.types.UpdateShortMessage(
                id=i, user_id=7, message="x" * size, pts=i, pts_count=1, date=1), base + 4 * i) for i in range(count)]
            start = time.perf_counter()
            tasks = [session.loop.create_task(session.handle_packet(p)) for p in packets]
            await asyncio.gather(*tasks)
            best = min(best, (time.perf_counter() - start) / count)
            await asyncio.sleep(0)
        print(f"update packet {size:>7} B: {best * 1e6:8.1f} us per packet")

asyncio.run(main())
