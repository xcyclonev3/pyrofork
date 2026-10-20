#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
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
        self.parts = {}  # file_part -> (request type, bytes)
        self.upload_delay = 0
        self.start_delay = 0
        self.sessions = []
        self.starts = 0
        self.auth_keys = 0
        self.in_flight = 0
        self.max_in_flight = 0


SERVER = FakeServer()


class FakeSession:
    def __init__(self, client, dc_id, auth_key, test_mode, is_media=False, is_cdn=False):
        self.dc_id, self.is_media = dc_id, is_media
        self.stopped = False
        SERVER.sessions.append(self)

    async def start(self):
        SERVER.starts += 1
        await asyncio.sleep(SERVER.start_delay)

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
                await asyncio.sleep(SERVER.upload_delay)
                SERVER.parts[query.file_part] = (type(query), query.bytes)
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


def test_transfer_cancelled_while_its_session_starts(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = os.urandom(1000)
        SERVER.start_delay = 10
        task = asyncio.get_running_loop().create_task(download(client, document(1), 1000))
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return client.media_sessions

    # The half started session is stopped, and not kept for the next transfers
    assert asyncio.run(main()) == {}
    [session] = SERVER.sessions
    assert session.stopped


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


# Downloads request the next parts ahead, and behave like a part by part download

DATA = os.urandom(7 * CHUNK + 123)


def sequential_download(data, file_size, limit, offset):
    """What the part by part download did: the parts, the progress values and the offsets it requested."""
    chunks, progress, requested = [], [], []
    total = abs(limit) or (1 << 31) - 1
    offset_bytes = abs(offset) * CHUNK
    current = 0

    while True:
        requested.append(offset_bytes)
        chunk = data[offset_bytes:offset_bytes + CHUNK]
        chunks.append(chunk)
        current += 1
        offset_bytes += CHUNK
        progress.append(min(offset_bytes, file_size) if file_size != 0 else offset_bytes)

        if len(chunk) < CHUNK or current >= total:
            return chunks, progress, requested


SIZES = [1000, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 5 * CHUNK + 123, 7 * CHUNK]


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("hint", ["exact", "unknown", "smaller", "bigger"])
@pytest.mark.parametrize("limit, offset", [(0, 0), (1, 0), (3, 0), (0, 2), (2, 1)])
def test_download_matches_the_part_by_part_download(transfer_client, size, hint, limit, offset):
    file_size = {"exact": size, "unknown": 0, "smaller": max(size // 3, 1), "bigger": size + 3 * CHUNK}[hint]
    expected_chunks, expected_progress, expected_requests = sequential_download(DATA[:size], file_size, limit, offset)

    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = DATA[:size]
        progress = []

        async def on_progress(current, total):
            assert total == file_size
            progress.append(current)

        chunks = await download(client, document(1), file_size, limit, offset, on_progress)
        await asyncio.sleep(0)
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []  # nothing left running
        return chunks, progress

    chunks, progress = asyncio.run(main())
    assert chunks == expected_chunks and progress == expected_progress
    requested = [offset for _, offset in SERVER.requested]
    assert requested[:len(expected_requests)] == expected_requests
    if hint == "exact":
        # The known size keeps the requests to the parts the part by part download asks for
        assert requested == expected_requests
    else:
        # Without a (right) size, at most the parts in flight are requested past the end
        assert len(requested) <= len(expected_requests) + pyrogram.Client.DOWNLOAD_PARTS_IN_FLIGHT
    assert SERVER.max_in_flight <= pyrogram.Client.DOWNLOAD_PARTS_IN_FLIGHT


def test_download_hands_out_parts_in_order(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = DATA
        # The first parts are the slowest to come
        SERVER.delays = {CHUNK: 0.05, 2 * CHUNK: 0.03, 3 * CHUNK: 0.01}
        return await download(client, document(1), len(DATA))

    assert b"".join(asyncio.run(main())) == DATA
    assert SERVER.max_in_flight == pyrogram.Client.DOWNLOAD_PARTS_IN_FLIGHT


@pytest.mark.parametrize("error, raised", [
    (errors.FileReferenceExpired(), None),  # logged and the download stops, as before
    (errors.FloodWait(value=60), errors.FloodWait),
])
def test_download_error_on_a_part(transfer_client, error, raised):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = DATA
        SERVER.failures[(1, 2 * CHUNK)] = error
        chunks = []
        try:
            async for chunk in client.get_file(document(1), len(DATA)):
                chunks.append(chunk)
        except Exception as e:
            assert raised is not None and isinstance(e, raised)
        else:
            assert raised is None
        await asyncio.sleep(0)
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []
        return chunks

    # The parts before the failing one are handed out, like the part by part download did
    assert asyncio.run(main()) == [DATA[:CHUNK], DATA[CHUNK:2 * CHUNK]]


def test_download_stopped_early_cancels_the_parts_requested_ahead(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = DATA
        SERVER.delays = {offset: 10 for offset in range(CHUNK, len(DATA), CHUNK)}  # never come in time
        file = client.get_file(document(1), len(DATA))
        first = await file.__anext__()
        await file.aclose()
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []
        return first

    assert asyncio.run(main()) == DATA[:CHUNK]


def test_download_stop_transmission(transfer_client):
    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.files[1] = DATA

        async def on_progress(current, total):
            if current >= 2 * CHUNK:
                client.stop_transmission()

        with pytest.raises(pyrogram.StopTransmission):
            await download(client, document(1), len(DATA), progress=on_progress)
        await asyncio.sleep(0)
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []

    asyncio.run(main())


# Uploads send several parts at the same time, small files included

from hashlib import md5  # noqa: E402

PART = 512 * 1024


@pytest.mark.parametrize("size, big", [(1000, False), (3 * PART + 7, False), (6 * PART, False), (21 * PART + 1, True)])
def test_upload_sends_parts_at_the_same_time(transfer_client, size, big):
    data = os.urandom(size)

    async def main():
        client = transfer_client()
        await open_storage(client)
        SERVER.upload_delay = 0.01
        progress = []

        async def on_progress(current, total):
            progress.append((current, total))

        uploaded = await client.save_file(io.BytesIO(data), progress=on_progress)
        await asyncio.sleep(0)
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []
        return uploaded, progress

    uploaded, progress = asyncio.run(main())
    parts = (size + PART - 1) // PART
    assert sorted(SERVER.parts) == list(range(parts))
    assert b"".join(SERVER.parts[i][1] for i in range(parts)) == data
    request = raw.functions.upload.SaveBigFilePart if big else raw.functions.upload.SaveFilePart
    assert {t for t, _ in SERVER.parts.values()} == {request}
    assert SERVER.max_in_flight == min(parts, pyrogram.Client.UPLOAD_PARTS_IN_FLIGHT)
    if big:
        assert isinstance(uploaded, raw.types.InputFileBig) and uploaded.parts == parts
    else:
        assert isinstance(uploaded, raw.types.InputFile) and uploaded.parts == parts
        assert uploaded.md5_checksum == md5(data).hexdigest()
    # The same progress as the part by part upload: after each part is read, in order
    assert progress == [(min((i + 1) * PART, size), size) for i in range(parts)]
