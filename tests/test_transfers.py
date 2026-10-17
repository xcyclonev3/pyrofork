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


# Fake media sessions serving files from memory

import io  # noqa: E402

import pytest  # noqa: E402

import pyrogram  # noqa: E402
from pyrogram import errors, types  # noqa: E402
from pyrogram.file_id import FileId, FileType  # noqa: E402
from pyrogram.methods.advanced import save_file as save_file_module  # noqa: E402
from pyrogram.methods.messages import inline_session  # noqa: E402

CHUNK = 1024 * 1024


class FakeServer:
    def __init__(self):
        self.files = {}  # media_id -> bytes
        self.failures = {}  # (media_id, offset) -> exception to raise
        self.delays = {}  # offset -> seconds before answering
        self.requested = []  # (media_id, offset) in request order
        self.parts = {}  # file_part -> bytes
        self.starts = 0
        self.auth_keys = 0
        self.in_flight = 0
        self.max_in_flight = 0


SERVER = FakeServer()


class FakeSession:
    def __init__(self, client, dc_id, auth_key, test_mode, is_media=False, is_cdn=False):
        self.dc_id, self.is_media = dc_id, is_media
        self.stopped = False

    async def start(self):
        SERVER.starts += 1

    async def stop(self):
        self.stopped = True

    async def invoke(self, query, *args, **kwargs):
        SERVER.in_flight += 1
        SERVER.max_in_flight = max(SERVER.max_in_flight, SERVER.in_flight)
        try:
            await asyncio.sleep(0)
            if isinstance(query, raw.functions.upload.GetFile):
                key = (query.location.id, query.offset)
                SERVER.requested.append(key)
                await asyncio.sleep(SERVER.delays.get(query.offset, 0))
                if key in SERVER.failures:
                    raise SERVER.failures[key]
                data = SERVER.files[query.location.id][query.offset:query.offset + query.limit]
                return raw.types.upload.File(type=raw.types.storage.FilePartial(), mtime=0, bytes=data)
            if isinstance(query, (raw.functions.upload.SaveFilePart, raw.functions.upload.SaveBigFilePart)):
                SERVER.parts[query.file_part] = query.bytes
                return True
            return True
        finally:
            SERVER.in_flight -= 1


class FakeAuth:
    def __init__(self, client, dc_id, test_mode):
        pass

    async def create(self):
        SERVER.auth_keys += 1
        return os.urandom(256)


@pytest.fixture
def transfer_client(monkeypatch):
    global SERVER
    SERVER = FakeServer()
    # Wherever a module creates sessions: no test ever opens a real connection
    for module in (inline_session, pyrogram.client, save_file_module):
        for name, fake in (("Session", FakeSession), ("Auth", FakeAuth)):
            if hasattr(module, name):
                monkeypatch.setattr(module, name, fake)
    clients = []

    def make():
        # Created in the running loop: the client runs its transfer tasks on client.loop
        client = pyrogram.Client("transfers", api_id=1, api_hash="x", in_memory=True)
        client.me = types.User(id=7, is_self=True, is_premium=False)

        async def invoke(query, *args, **kwargs):
            if isinstance(query, raw.functions.auth.ExportAuthorization):
                return raw.types.auth.ExportedAuthorization(id=1, bytes=b"x")
            return True

        client.invoke = invoke
        clients.append(client)
        return client

    yield make
    for client in clients:
        if not client.loop.is_closed():
            client.loop.close()
    asyncio.set_event_loop(None)


async def open_storage(client, dc_id=2):
    await client.storage.open()
    await client.storage.dc_id(dc_id)
    await client.storage.auth_key(os.urandom(256))
    await client.storage.test_mode(False)


def document(media_id, dc_id=2):
    return FileId(file_type=FileType.DOCUMENT, dc_id=dc_id, media_id=media_id, access_hash=1, file_reference=b"r")


async def download(client, file_id, file_size=0, limit=0, offset=0, progress=None):
    return [chunk async for chunk in client.get_file(file_id, file_size, limit, offset, progress)]


@pytest.mark.parametrize("dc_id, auth_keys", [(2, 0), (4, 1)])
def test_downloads_reuse_the_media_session(transfer_client, dc_id, auth_keys):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = os.urandom(300_000)
        for _ in range(3):
            assert b"".join(await download(client, document(1, dc_id), 300_000)) == SERVER.files[1]
        return client.media_sessions

    sessions = asyncio.run(main())
    # One session started (and, on another data center, one authorization key) for all the downloads
    assert SERVER.starts == 1 and SERVER.auth_keys == auth_keys
    assert list(sessions) == [dc_id] and not sessions[dc_id].stopped


def test_uploads_reuse_the_media_session(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        for _ in range(3):
            assert isinstance(await client.save_file(io.BytesIO(os.urandom(300_000))), raw.types.InputFile)
        await client.storage.close()

    asyncio.run(main())
    assert SERVER.starts == 1


def test_transfer_sessions_are_stopped_on_terminate(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = os.urandom(1000)
        await download(client, document(1), 1000)
        session = client.media_sessions[2]
        client.is_initialized = True
        client.dispatcher.stop = lambda: asyncio.sleep(0)
        await client.terminate()
        return session

    session = asyncio.run(main())
    assert session.stopped
