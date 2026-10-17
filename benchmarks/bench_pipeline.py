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

"""Updates from packets to a message handler: session, handle_updates, storage, dispatcher, parsing, handler."""
import asyncio
import cProfile
import os
import pstats
import tempfile
import time
from hashlib import sha256

from common import AUTH_KEY, AUTH_KEY_ID

import pyrogram
from pyrogram import raw, types
from pyrogram.crypto import aes
from pyrogram.crypto.mtproto import kdf
from pyrogram.handlers import MessageHandler
from pyrogram.raw.core import Long, Message
from pyrogram.session import Session
from tl_samples import channel, message, user


class Sink:
    async def send(self, payload):
        pass


def packet(session, body, msg_id):
    data = Long(1) + session.session_id + Message(body, msg_id, 0, len(body.write())).write()
    padding = bytes(-(len(data) + 12) % 16 + 12)
    msg_key = sha256(AUTH_KEY[96:128] + data + padding).digest()[8:24]
    key, iv = kdf(AUTH_KEY, msg_key, False)
    return AUTH_KEY_ID + msg_key + aes.ige256_encrypt(data + padding, key, iv)


async def run(count, profile=False):
    if os.environ.get("SESSION") == "file":  # a session file, as user accounts and named bots have by default
        client = pyrogram.Client("bench", api_id=1, api_hash="x", workdir=tempfile.mkdtemp(), workers=4)
    else:
        client = pyrogram.Client("bench", api_id=1, api_hash="x", in_memory=True, workers=4)
    await client.storage.open()
    client.me = types.User(id=999, is_self=True)
    done = asyncio.Event()
    handled = 0

    async def on_message(c, m):
        nonlocal handled
        handled += 1
        if handled == count:
            done.set()

    client.add_handler(MessageHandler(on_message))
    await client.dispatcher.start()
    session = Session(client, 2, AUTH_KEY, False)
    session.connection = Sink()
    base = (int(time.time()) << 32) | 1

    def offline_message(i):
        m = message(i)
        m.reply_to = None  # replies are fetched from Telegram
        return m

    packets = [packet(session, raw.types.Updates(
        updates=[raw.types.UpdateNewChannelMessage(message=offline_message(i), pts=i, pts_count=1)],
        users=[user((i % 20) + 1)], chats=[channel(777)], date=0, seq=0), base + 4 * i) for i in range(count)]
    profiler = cProfile.Profile() if profile else None
    start = time.perf_counter()
    if profiler:
        profiler.enable()
    for p in packets:
        session.loop.create_task(session.handle_packet(p))
        await asyncio.sleep(0)
    await asyncio.wait_for(done.wait(), 60)
    elapsed = time.perf_counter() - start
    if profiler:
        profiler.disable()
        pstats.Stats(profiler).sort_stats("tottime").print_stats(14)
    await client.dispatcher.stop()
    return elapsed / count


async def main():
    best = min([await run(2000) for _ in range(3)])
    print(f"update -> handler: {best * 1e6:.1f} us per update ({1 / best:,.0f} updates/s)")
    if os.environ.get("PROFILE"):
        await run(2000, profile=True)

asyncio.run(main())
