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

"""Simulated downloads and uploads over a network with RTT, handshakes and shared bandwidth (no Telegram)."""
import asyncio
import io
import os
import time

import common  # noqa: F401
import fake_network
from fake_network import Network, STATS, make_client
from pyrogram.file_id import FileId, FileType

fake_network.install()


def doc(dc_id=2, media_id=1):
    return FileId(file_type=FileType.DOCUMENT, dc_id=dc_id, media_id=media_id, access_hash=1, file_reference=b"r")


async def download(client, size, dc_id=2):
    media_id = 1 if size == 32 << 20 else size
    Network.sizes[media_id] = size
    got = 0
    async for chunk in client.get_file(doc(dc_id, media_id), size):
        got += len(chunk)
    assert got == size, (got, size)


async def scenario(name, coro_factory, rtt):
    Network.rtt = rtt
    Network.down.lock = Network.up.lock = None
    for k in STATS:
        STATS[k] = 0
    client = await make_client()
    t0, c0 = time.perf_counter(), time.process_time()
    await coro_factory(client)
    wall, cpu = time.perf_counter() - t0, time.process_time() - c0
    if hasattr(client, "media_sessions"):
        for s in list(client.media_sessions.values()):
            await s.stop()
    print(f"  {name:<44} rtt {int(rtt * 1000):>3} ms: {wall:7.2f} s  cpu {cpu:5.2f} s  "
          f"starts {STATS['session_starts']:>3}  dh {STATS['auth_creates']:>3}  max_in_flight {STATS['max_in_flight']}")
    return wall


async def main():
    for rtt in (0.05, 0.1, 0.15):
        size = 32 << 20
        w = await scenario("download 32 MB", lambda c: download(c, size), rtt)
        print(f"{'':48}-> {size / w / 1e6:6.1f} MB/s")
    await scenario("20 photos x 200 KB, same DC, one by one",
                   lambda c: _seq(c, [lambda c=c: download(c, 200_000)] * 20), 0.1)
    await scenario("20 photos x 200 KB, other DC, one by one",
                   lambda c: _seq(c, [lambda c=c: download(c, 200_000, dc_id=4)] * 20), 0.1)
    for size in (8 << 20, 40 << 20):
        data = os.urandom(size)
        w = await scenario(f"upload {size >> 20} MB", lambda c, data=data: c.save_file(io.BytesIO(data)), 0.1)
        print(f"{'':48}-> {size / w / 1e6:6.1f} MB/s")
    await scenario("20 uploads x 300 KB, one by one",
                   lambda c: _seq(c, [lambda c=c: c.save_file(io.BytesIO(os.urandom(300_000)))] * 20), 0.1)


async def _seq(client, factories):
    for f in factories:
        await f()

asyncio.run(main())
