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

"""Live photos, story music and audio listening reports (Layer 221-230)."""

import pytest

from pyrogram import raw
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from tests.test_layer_roundtrip import offline_client, call, PEERS


def document_file_id(file_type, media_id, access_hash=55, file_reference=b"ref"):
    return FileId(
        file_type=file_type, dc_id=2, media_id=media_id, access_hash=access_hash, file_reference=file_reference
    ).encode()


def photo_file_id(media_id, access_hash=66, file_reference=b"pref"):
    return FileId(
        file_type=FileType.PHOTO, dc_id=2, media_id=media_id, access_hash=access_hash, file_reference=file_reference,
        thumbnail_source=ThumbnailSource.THUMBNAIL, thumbnail_file_type=FileType.PHOTO, thumbnail_size="y",
        volume_id=0, local_id=0
    ).encode()


AUDIO_ID = document_file_id(FileType.AUDIO, 1001)
VOICE_ID = document_file_id(FileType.VOICE, 1002)


# listen_to_audio -> messages.reportMusicListen (TDLib listenToAudio)

def test_listen_to_audio_reports_music_listen():
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.listen_to_audio(AUDIO_ID, 42)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.ReportMusicListen)
    assert request.listened_duration == 42
    assert isinstance(request.id, raw.types.InputDocument)
    assert (request.id.id, request.id.access_hash, request.id.file_reference) == (1001, 55, b"ref")


@pytest.mark.parametrize("duration", [0, 2])
def test_listen_to_audio_skips_short_listens(duration):
    client = offline_client(PEERS, lambda q: pytest.fail("short listens must not be reported"))
    assert call(lambda: client.listen_to_audio(AUDIO_ID, duration)) is True
    assert client.sent == []


@pytest.mark.parametrize("file_id, duration", [(AUDIO_ID, -1), (VOICE_ID, 10), ("not a file id", 10)])
def test_listen_to_audio_rejects_invalid_arguments(file_id, duration):
    client = offline_client(PEERS, lambda q: True)
    with pytest.raises(ValueError):
        call(lambda: client.listen_to_audio(file_id, duration))
    assert client.sent == []
