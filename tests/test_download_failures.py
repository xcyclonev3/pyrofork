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

"""Downloads that fail don't pass for complete files, and a failing progress callback doesn't stop them."""

import asyncio
import os
from hashlib import sha256

import pytest

import pyrogram
from pyrogram import errors, raw, types
from pyrogram.crypto import aes
from pyrogram.methods.messages import inline_session
from tests import test_transfers as transfers
from tests.test_transfers import CHUNK, DATA, document, open_storage, transfer_client  # noqa: F401 (a fixture)

PARTS = len(DATA) // CHUNK + 1

# Files a CDN serves: the file's data center redirects the download, and gives the hashes of the parts

CDN_KEY, CDN_IV = os.urandom(32), os.urandom(16)


class RedirectingSession(transfers.FakeSession):
    async def invoke(self, query, *args, **kwargs):
        if isinstance(query, raw.functions.upload.GetFile):
            return raw.types.upload.FileCdnRedirect(
                dc_id=203, file_token=b"t", encryption_key=CDN_KEY, encryption_iv=CDN_IV, file_hashes=[]
            )
        if isinstance(query, raw.functions.upload.GetCdnFileHashes):
            part = transfers.SERVER.files[1][query.offset:query.offset + CHUNK]
            return [raw.types.FileHash(offset=query.offset, limit=CHUNK, hash=sha256(part).digest())]
        return await super().invoke(query, *args, **kwargs)


class CdnSession(transfers.FakeSession):
    async def invoke(self, query, *args, **kwargs):
        if isinstance(query, raw.functions.upload.GetCdnFile):
            key = (1, query.offset)
            transfers.SERVER.requested.append(key)
            if key in transfers.SERVER.failures:
                raise transfers.SERVER.failures[key]
            part = transfers.SERVER.files[1][query.offset:query.offset + query.limit]
            iv = bytearray(CDN_IV[:-4] + (query.offset // 16).to_bytes(4, "big"))
            return raw.types.upload.CdnFile(bytes=aes.ctr256_encrypt(part, CDN_KEY, iv))
        return await super().invoke(query, *args, **kwargs)


@pytest.fixture(params=["main-dc", "cdn"])
def make_client(request, transfer_client, monkeypatch):
    """Clients downloading the files of the fake server from its data center, or from a CDN it redirects to."""
    if request.param == "cdn":
        monkeypatch.setattr(inline_session, "Session", RedirectingSession)
        monkeypatch.setattr(pyrogram.client, "Session", CdnSession)
    return transfer_client


async def started(make_client):
    client = make_client()
    await open_storage(client)
    transfers.SERVER.files[1] = DATA
    return client


def media():
    """The file 1 of the fake server, as a document with its size."""
    return types.Document(file_id=document(1).encode(), file_unique_id="u", file_size=len(DATA))


def nothing_left_running():
    return [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []


def logged_errors(caplog):
    """The exceptions logged with their traceback."""
    return [record.exc_info[1] for record in caplog.records if record.exc_info]


# A failing progress callback doesn't stop the download

@pytest.mark.parametrize("is_async, error_type", [
    pytest.param(True, RuntimeError, id="async"),
    pytest.param(False, RuntimeError, id="sync"),
    pytest.param(True, errors.MessageNotModified, id="message-not-modified"),  # a message edited with the same text
])
def test_progress_callback_errors_dont_stop_the_download(make_client, tmp_path, caplog, is_async, error_type):
    reported = []

    def on_progress(current, total):
        reported.append(current)
        if len(reported) in (2, 3, 5):
            raise error_type()

    async def on_progress_async(current, total):
        on_progress(current, total)

    async def main():
        client = await started(make_client)
        path = await client.download_media(
            media(), file_name=str(tmp_path / "file.bin"), progress=on_progress_async if is_async else on_progress
        )
        await asyncio.sleep(0)
        assert nothing_left_running()
        client.executor.shutdown()
        return path

    path = asyncio.run(main())
    assert path == str(tmp_path / "file.bin")
    with open(path, "rb") as f:
        assert f.read() == DATA
    assert os.listdir(tmp_path) == ["file.bin"]
    # Still called for every part
    assert reported == [min((i + 1) * CHUNK, len(DATA)) for i in range(PARTS)]
    # Logged once, with its traceback
    [error] = logged_errors(caplog)
    assert isinstance(error, error_type) and error.__traceback__ is not None


@pytest.mark.parametrize("in_memory", [False, True], ids=["file", "in-memory"])
def test_stop_transmission_in_the_progress_callback_returns_none(make_client, tmp_path, caplog, in_memory):
    async def main():
        client = await started(make_client)

        async def on_progress(current, total):
            if current >= 2 * CHUNK:
                client.stop_transmission()

        result = await client.download_media(
            media(), file_name=str(tmp_path / "file.bin"), in_memory=in_memory, progress=on_progress
        )
        await asyncio.sleep(0)
        assert nothing_left_running()
        return result

    assert asyncio.run(main()) is None
    assert os.listdir(tmp_path) == []
    # The download stopped there, without an error logged
    assert len(transfers.SERVER.requested) < PARTS
    assert logged_errors(caplog) == []


def test_flood_wait_in_the_progress_callback_is_raised(make_client, tmp_path):
    async def main():
        client = await started(make_client)

        async def on_progress(current, total):
            raise errors.FloodWait(value=60)

        with pytest.raises(errors.FloodWait):
            await client.download_media(media(), file_name=str(tmp_path / "file.bin"), progress=on_progress)
        await asyncio.sleep(0)
        assert nothing_left_running()

    asyncio.run(main())
    assert os.listdir(tmp_path) == []


# A download failing on a part returns None and leaves no file; a stream of the file just ends there, as before

FAILURES = [
    pytest.param(0, errors.FileReferenceExpired, id="first-part-reference-expired"),
    pytest.param(2 * CHUNK, errors.FileReferenceExpired, id="third-part-reference-expired"),
    pytest.param(2 * CHUNK, OSError, id="third-part-connection-lost"),  # what a session raises after its retries
]


@pytest.mark.parametrize("in_memory", [False, True], ids=["file", "in-memory"])
@pytest.mark.parametrize("offset, error_type", FAILURES)
def test_download_failing_on_a_part_returns_none(make_client, tmp_path, caplog, offset, error_type, in_memory):
    async def main():
        client = await started(make_client)
        transfers.SERVER.failures[(1, offset)] = error_type()
        result = await client.download_media(media(), file_name=str(tmp_path / "file.bin"), in_memory=in_memory)
        await asyncio.sleep(0)
        assert nothing_left_running()
        return result

    assert asyncio.run(main()) is None
    # Neither the file nor its .temp
    assert os.listdir(tmp_path) == []
    [error] = logged_errors(caplog)
    assert isinstance(error, error_type)


@pytest.mark.parametrize("offset, error_type", FAILURES)
def test_stream_media_failing_on_a_part_ends_early(make_client, caplog, offset, error_type):
    async def main():
        client = await started(make_client)
        transfers.SERVER.failures[(1, offset)] = error_type()
        chunks = [chunk async for chunk in client.stream_media(media())]
        await asyncio.sleep(0)
        assert nothing_left_running()
        return chunks

    # The parts before the failing one, then the end of the stream without an exception
    assert asyncio.run(main()) == [DATA[i:i + CHUNK] for i in range(0, offset, CHUNK)]
    [error] = logged_errors(caplog)
    assert isinstance(error, error_type)


@pytest.mark.parametrize("in_memory", [False, True], ids=["file", "in-memory"])
def test_download_flood_wait_on_a_part_is_raised(make_client, tmp_path, in_memory):
    async def main():
        client = await started(make_client)
        transfers.SERVER.failures[(1, 2 * CHUNK)] = errors.FloodWait(value=60)
        with pytest.raises(errors.FloodWait):
            await client.download_media(media(), file_name=str(tmp_path / "file.bin"), in_memory=in_memory)
        await asyncio.sleep(0)
        assert nothing_left_running()

    asyncio.run(main())
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("in_memory", [False, True], ids=["file", "in-memory"])
def test_download_success(make_client, request, tmp_path, caplog, in_memory):
    reported = []

    async def main():
        client = await started(make_client)

        async def on_progress(current, total, *args):
            reported.append((current, total, args))

        return await client.download_media(
            media(), file_name=str(tmp_path / "file.bin"), in_memory=in_memory,
            progress=on_progress, progress_args=("arg",)
        )

    result = asyncio.run(main())
    if in_memory:
        assert result.name == "file.bin" and result.getvalue() == DATA
        assert os.listdir(tmp_path) == []
    else:
        assert result == str(tmp_path / "file.bin")
        with open(result, "rb") as f:
            assert f.read() == DATA
        assert os.listdir(tmp_path) == ["file.bin"]
    assert reported == [(min((i + 1) * CHUNK, len(DATA)), len(DATA), ("arg",)) for i in range(PARTS)]
    assert logged_errors(caplog) == []
    # From a CDN, the parts come through a CDN session, stopped once done
    cdn_sessions = [session for session in transfers.SERVER.sessions if isinstance(session, CdnSession)]
    from_cdn = request.node.callspec.params["make_client"] == "cdn"
    assert [session.stopped for session in cdn_sessions] == ([True] if from_cdn else [])


def test_get_file_overridden_with_the_previous_signature(tmp_path):
    class Overriding(pyrogram.Client):
        async def get_file(self, file_id, file_size=0, limit=0, offset=0, progress=None, progress_args=()):
            yield b"abc"
            yield b"def"

    async def main():
        client = Overriding("overriding", api_id=1, api_hash="x", in_memory=True)
        return await client.handle_download((None, str(tmp_path), "file.bin", False, 6, None, ()))

    path = asyncio.run(main())
    # Such overrides don't take _raise_errors: downloads must keep working with them
    assert path is not None and open(path, "rb").read() == b"abcdef"
