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

"""A fake Telegram network for offline transfer benchmarks: sessions with RTT, handshakes and shared bandwidth.

It replaces Session and Auth in every loaded pyrogram module, so it works for the old and the new code.
"""
import asyncio
import os
import sys

import pyrogram
from pyrogram import raw
from pyrogram.session import Session as RealSession
from pyrogram.session.auth import Auth as RealAuth

STATS = {"session_starts": 0, "auth_creates": 0, "requests": 0, "max_in_flight": 0}


class Link:
    def __init__(self, bandwidth):
        self.bandwidth = bandwidth
        self.lock = None

    async def transfer(self, size):
        if self.lock is None:
            self.lock = asyncio.Lock()
        async with self.lock:  # serialization delay on the shared link
            await asyncio.sleep(size / self.bandwidth)


class Network:
    rtt = 0.1
    down = Link(100e6)
    up = Link(50e6)
    server_time = 0.005
    file = os.urandom(32 << 20)
    sizes = {}  # media_id -> file size (default: the whole 32 MB)
    in_flight = 0


class FakeSession:
    def __init__(self, client, dc_id, auth_key, test_mode, is_media=False, is_cdn=False):
        self.client, self.dc_id, self.is_media = client, dc_id, is_media
        self.is_started = asyncio.Event()

    async def start(self):
        STATS["session_starts"] += 1
        await asyncio.sleep(2 * Network.rtt)  # TCP connect + initConnection
        self.is_started.set()

    async def stop(self):
        self.is_started.clear()

    async def invoke(self, query, *args, **kwargs):
        STATS["requests"] += 1
        Network.in_flight += 1
        STATS["max_in_flight"] = max(STATS["max_in_flight"], Network.in_flight)
        try:
            if isinstance(query, raw.functions.upload.GetFile):
                await asyncio.sleep(Network.rtt / 2 + Network.server_time)
                size = Network.sizes.get(query.location.id, len(Network.file))
                data = Network.file[query.offset:min(query.offset + query.limit, size)]
                await Network.down.transfer(len(data))
                await asyncio.sleep(Network.rtt / 2)
                return raw.types.upload.File(type=raw.types.storage.FilePartial(), mtime=0, bytes=data)
            if isinstance(query, (raw.functions.upload.SaveFilePart, raw.functions.upload.SaveBigFilePart)):
                await Network.up.transfer(len(query.bytes))
                await asyncio.sleep(Network.rtt + Network.server_time)
                return True
            await asyncio.sleep(Network.rtt)
            return True
        finally:
            Network.in_flight -= 1

    async def send(self, *args, **kwargs):
        return await self.invoke(*args, **kwargs)


class FakeAuth:
    def __init__(self, client, dc_id, test_mode):
        pass

    async def create(self):
        STATS["auth_creates"] += 1
        await asyncio.sleep(4 * Network.rtt)  # DH key exchange
        return os.urandom(256)


def install():
    for module in list(sys.modules.values()):
        if module is None or not getattr(module, "__name__", "").startswith("pyrogram"):
            continue
        if getattr(module, "Session", None) is RealSession:
            module.Session = FakeSession
        if getattr(module, "Auth", None) is RealAuth:
            module.Auth = FakeAuth


async def make_client():
    client = pyrogram.Client("bench", api_id=1, api_hash="x", in_memory=True, no_updates=True)
    await client.storage.open()
    await client.storage.dc_id(2)
    await client.storage.auth_key(os.urandom(256))
    await client.storage.test_mode(False)
    client.me = pyrogram.types.User(id=999, is_self=True, is_premium=False)
    client.is_connected = True

    async def invoke(query, *args, **kwargs):
        await asyncio.sleep(Network.rtt)
        if isinstance(query, raw.functions.auth.ExportAuthorization):
            return raw.types.auth.ExportedAuthorization(id=1, bytes=b"x")
        return True

    client.invoke = invoke
    return client
