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

import asyncio

import pytest

from pyrogram import raw, types
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS


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

RAW_USER = raw.types.User(id=7, access_hash=2, first_name="A")


def raw_photo(photo_id=2001):
    return raw.types.Photo(
        id=photo_id, access_hash=66, file_reference=b"pref", date=1, dc_id=2,
        sizes=[raw.types.PhotoSize(type="y", w=800, h=600, size=1000)]
    )


def raw_document(document_id, mime_type, attributes):
    return raw.types.Document(
        id=document_id, access_hash=55, file_reference=b"ref", date=1, mime_type=mime_type, size=4096, dc_id=2,
        attributes=attributes
    )


RAW_SONG = raw_document(1001, "audio/mpeg", [
    raw.types.DocumentAttributeAudio(duration=180, title="Song", performer="Band"),
    raw.types.DocumentAttributeFilename(file_name="song.mp3"),
])


def decoded(file_id):
    d = FileId.decode(file_id)
    return d.file_type, d.media_id, d.access_hash, d.file_reference


def assert_input_document(input_document, media_id, access_hash=55, file_reference=b"ref"):
    assert isinstance(input_document, raw.types.InputDocument)
    assert (input_document.id, input_document.access_hash, input_document.file_reference) == (
        media_id, access_hash, file_reference)


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


# Story music: stories.sendStory/editStory.music, storyItem.music (not in TDLib's td_api yet)

def story_item(music=None):
    return raw.types.StoryItem(
        id=5, date=1, expire_date=86401, media=raw.types.MessageMediaPhoto(photo=raw_photo()), music=music
    )


def story_client(item):
    def respond(query):
        if isinstance(query, raw.functions.users.GetUsers):
            return [roundtrip(RAW_USER)]  # a Vector result is not a TLObject: offline_client won't round trip it
        return empty_updates(updates=[raw.types.UpdateStory(peer=raw.types.PeerUser(user_id=7), story=item)])

    return offline_client(PEERS, respond)


def parse_story(client, item):
    return asyncio.run(types.Story._parse(client, roundtrip(item), raw.types.PeerUser(user_id=7)))


def sent(client, request_type):
    [request] = [q for q in client.sent if isinstance(q, request_type)]
    return request


def assert_song(audio):
    assert isinstance(audio, types.Audio)
    assert decoded(audio.file_id) == (FileType.AUDIO, 1001, 55, b"ref")
    assert (audio.title, audio.performer, audio.duration, audio.file_name) == ("Song", "Band", 180, "song.mp3")
    assert audio.mime_type == "audio/mpeg" and audio.file_size == 4096


def test_story_music_is_parsed():
    client = story_client(None)
    story = parse_story(client, story_item(music=RAW_SONG))
    assert_song(story.music)
    assert story.photo is not None and story.from_user.id == 7

    assert parse_story(client, story_item()).music is None


def test_send_story_with_music():
    client = story_client(story_item(music=RAW_SONG))
    story = call(lambda: client.send_story(photo=photo_file_id(2001), music=AUDIO_ID))

    request = sent(client, raw.functions.stories.SendStory)
    assert isinstance(request.peer, raw.types.InputPeerSelf)
    assert isinstance(request.media, raw.types.InputMediaPhoto) and request.media.id.id == 2001
    assert_input_document(request.music, 1001)
    assert story.id == 5
    assert_song(story.music)


def test_send_story_without_music():
    client = story_client(story_item())
    story = call(lambda: client.send_story(photo=photo_file_id(2001)))

    assert sent(client, raw.functions.stories.SendStory).music is None
    assert story.music is None


def test_edit_story_music():
    client = story_client(story_item(music=RAW_SONG))
    story = call(lambda: client.edit_story(story_id=5, music=AUDIO_ID))

    request = sent(client, raw.functions.stories.EditStory)
    assert request.id == 5 and isinstance(request.peer, raw.types.InputPeerSelf)
    assert request.media is None and request.caption is None
    assert request.media_areas == [] and request.privacy_rules == []  # absent flag vectors are read back as []
    assert_input_document(request.music, 1001)
    assert_song(story.music)


def test_story_edit_bound_method_passes_music():
    client = story_client(story_item(music=RAW_SONG))
    story = parse_story(client, story_item())
    edited = call(lambda: story.edit(music=AUDIO_ID))

    assert_input_document(sent(client, raw.functions.stories.EditStory).music, 1001)
    assert_song(edited.music)


@pytest.mark.parametrize("method, kwargs", [
    ("send_story", {"photo": photo_file_id(2001)}),
    ("edit_story", {"story_id": 5}),
])
def test_story_music_must_be_an_audio(method, kwargs):
    client = story_client(story_item())
    with pytest.raises(ValueError):
        call(lambda: getattr(client, method)(music=VOICE_ID, **kwargs))
    assert client.sent == []
